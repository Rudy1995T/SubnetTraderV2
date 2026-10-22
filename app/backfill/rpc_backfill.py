"""Archive-RPC hourly cross-sections (spec 17 §2.3): eras ``gap``, ``ledger``, ``pre_ledger``.

Per bucket (one UTC hour ``H``): locate a block inside ``[H, H+step)`` via ``Timestamp.Now`` (never an interpolated
block), read ``SubnetInfoRuntimeApi_get_all_dynamic_info`` raw, decode with the cached runtime, price per §2.3.1
(``auto``: ``AlphaSqrtPrice²`` while the Swap map has keys, else the reserve ratio — verified 2026-09-24: the map
exists at block 8,443,757 and is gone by 9,000,000, where the SDK price equals the reserve ratio), convert, and write
``pool_history`` + ``rpc_owner_history`` + ``block_times`` + ``backfill_buckets`` in ONE transaction.

``--emission-daily`` (spec 20 N2 item 3): one cross-section per UTC day at the first-hour block → ``rpc_subnet_daily``
(``emission_share`` = 100 · ``tao_in_emission`` / Σ over netuids ≠ 0, ``moving_price``) for every occupant alive that
day, dead ones included; resumable from its own rows (``backfill_buckets.era`` has no value for it).
"""
from __future__ import annotations

import asyncio
import bisect
import importlib.util
import json
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from app.backfill import common
from app.backfill.rpc_client import (DecodeError, Endpoint, NoRuntimeApi, RateLimited, RpcError, RpcGateway,
                                     endpoints_from_settings, le_int, netuid_from_key, storage_key, storage_prefix)
from app.logging.logger import logger
from app.util.time import now_ts, to_iso

DTAO_LAUNCH_TS = 1739482884            # 2025-02-13T21:41:24Z (block 4,920,351)
LEDGER_START_TS = 1758585588           # first PC ledger row (block 6,502,594)
LEDGER_END_TS_PC = 1781904274          # last PC ledger row (block 8,443,757); used only when no ledger is imported
BLOCK_SEC = 12.0
GOLDEN_ANCHORS: tuple[tuple[int, int], ...] = ((4920351, 1739482884), (6502594, 1758585588), (8443757, 1781904264))
ERAS = ("gap", "ledger", "pre_ledger")
SWAP_SQRT_PREFIX = storage_prefix("Swap", "AlphaSqrtPrice")
DECODE_STOP_AFTER = 10
NOT_ENDPOINT_ERRORS = ("block_search", "DecodeError", "no_runtime_api", "write:")   # never trigger failover


class BlockSearchFailed(RuntimeError):
    pass


@dataclass(frozen=True, order=True)
class Bucket:
    era: str
    bucket_ts: int


@dataclass
class BucketResult:
    bucket: Bucket
    status: str                                   # done | failed | skipped
    endpoint: str = ""
    block_number: int | None = None
    block_ts: int | None = None
    spec_version: int | None = None
    n_subnets: int | None = None
    rows: list[dict] = field(default_factory=list)
    owners: list[dict] = field(default_factory=list)
    block_times: list[tuple[int, int]] = field(default_factory=list)
    price_method: str | None = None
    attempts: int = 1
    error: str | None = None
    counts: dict = field(default_factory=dict)


# ── planning ─────────────────────────────────────────────────────────────────────────────────────────────────
def ledger_span(con: sqlite3.Connection) -> tuple[int, int, bool]:
    """(min_ts, max_ts, imported) of ``ledger_*`` rows; falls back to the verified PC span when none exist."""
    r = con.execute("SELECT MIN(timestamp), MAX(timestamp) FROM pool_history WHERE source LIKE 'ledger_%'").fetchone()
    if r and r[0] is not None:
        return int(r[0]), int(r[1]), True
    return LEDGER_START_TS, LEDGER_END_TS_PC, False


def era_range(con: sqlite3.Connection, era: str, *, step: int, now: int) -> tuple[int, int]:
    """[from_ts, to_ts) of bucket starts for ``era`` (aligned to ``step``)."""
    lo, hi, _ = ledger_span(con)
    if era == "pre_ledger":
        a, b = (DTAO_LAUNCH_TS // step) * step, (lo // step) * step
    elif era == "ledger":
        a, b = (lo // step) * step, (hi // step) * step + step
    elif era == "gap":
        a, b = (hi // step) * step + step, (now // 3600 - 1) * 3600
        b = (b // step) * step
    else:
        raise ValueError(f"unknown era {era!r}")
    return a, max(a, b)


def plan_buckets(con: sqlite3.Connection, era: str, *, step: int, now: int, min_netuids: int,
                 from_ts: int | None = None, to_ts: int | None = None) -> list[Bucket]:
    """Every bucket of the era not yet ``done``/``skipped``; the ``ledger`` era (hourly) keeps only hours with
    < ``min_netuids`` netuids or no non-NULL ``moving_price`` (17 §2.3 table)."""
    a, b = era_range(con, era, step=step, now=now)
    if from_ts is not None:
        a = max(a, (from_ts // step) * step)
    if to_ts is not None:
        b = min(b, to_ts)
    finished = {r[0] for r in con.execute(
        "SELECT bucket_ts FROM backfill_buckets WHERE era = ? AND status IN ('done','skipped') AND bucket_ts >= ? "
        "AND bucket_ts < ?", (era, a, b))}
    complete: set[int] = set()
    if era == "ledger" and step == 3600:
        for h, n, mp in con.execute(
                "SELECT timestamp / 3600, COUNT(DISTINCT netuid), SUM(moving_price IS NOT NULL) FROM pool_history "
                "WHERE timestamp >= ? AND timestamp < ? GROUP BY 1", (a, b)):
            if n >= min_netuids and mp:
                complete.add(int(h) * 3600)
    return [Bucket(era, t) for t in range(a, b, step) if t not in finished and t not in complete]


# ── block ↔ time ─────────────────────────────────────────────────────────────────────────────────────────────
class BlockSampler:
    """Piecewise-linear block ↔ time over anchors; 12.0 s/block outside them (17 §2.3 step 1)."""

    def __init__(self, anchors: Iterable[tuple[int, int]]) -> None:
        self._b: list[int] = []
        self._t: list[int] = []
        for b, t in sorted(set((int(b), int(t)) for b, t in anchors)):
            self.add(b, t)

    def __len__(self) -> int:
        return len(self._b)

    def add(self, block: int, ts: int) -> None:
        i = bisect.bisect_left(self._b, block)
        if i < len(self._b) and self._b[i] == block:
            return
        self._b.insert(i, block)
        self._t.insert(i, ts)

    def predict(self, target_ts: int) -> int:
        if not self._b:
            raise ValueError("BlockSampler has no anchors")
        i = bisect.bisect_left(self._t, target_ts)
        if i <= 0:
            return max(0, self._b[0] + round((target_ts - self._t[0]) / BLOCK_SEC))
        if i >= len(self._t):
            return self._b[-1] + round((target_ts - self._t[-1]) / BLOCK_SEC)
        b0, t0, b1, t1 = self._b[i - 1], self._t[i - 1], self._b[i], self._t[i]
        if t1 == t0:
            return b0
        return b0 + round((target_ts - t0) * (b1 - b0) / (t1 - t0))

    def ts_at(self, block: int) -> int:
        if not self._b:
            raise ValueError("BlockSampler has no anchors")
        i = bisect.bisect_left(self._b, block)
        if i <= 0:
            return self._t[0] + round((block - self._b[0]) * BLOCK_SEC)
        if i >= len(self._b):
            return self._t[-1] + round((block - self._b[-1]) * BLOCK_SEC)
        b0, t0, b1, t1 = self._b[i - 1], self._t[i - 1], self._b[i], self._t[i]
        return t0 + round((block - b0) * (t1 - t0) / (b1 - b0))


def load_anchors(con: sqlite3.Connection, live_from_ts: int) -> list[tuple[int, int]]:
    out = [(int(b), int(ms) // 1000) for b, ms in con.execute("SELECT block_number, ts_ms FROM block_times")]
    for b, t in con.execute("SELECT MIN(block_number), MIN(timestamp) FROM pool_history WHERE source LIKE 'ledger_%' "
                            "GROUP BY timestamp / 3600"):
        out.append((int(b), int(t) - 10 if t >= live_from_ts else int(t)))
    out.extend(GOLDEN_ANCHORS)
    return out


async def locate_block(rpc: RpcGateway, sampler: BlockSampler, bucket_ts: int, *, step: int, offset: int,
                       seen: list[tuple[int, int]] | None = None, max_corrections: int = 3,
                       max_bisect: int = 24) -> tuple[int, str, int] | None:
    """(block, hash, ts_ms) of a block inside ``[bucket_ts, bucket_ts+step)`` near ``bucket_ts+step−offset``.

    Up to ``max_corrections`` delta steps ``round((target − ts)/12)`` while unbracketed; once a block before and a
    block after the window are known it bisects. ``None`` = no block in the window (chain stall: the last block
    before it and the first block after it are adjacent); ``BlockSearchFailed`` otherwise."""
    target = bucket_ts + step - offset
    b = sampler.predict(target)
    lo: int | None = None   # highest block known to be before the window
    hi: int | None = None   # lowest block known to be after it
    corrections = bisects = 0
    while True:
        h = await rpc.block_hash(b)
        ts_ms = await rpc.timestamp_ms(h)
        ts = ts_ms // 1000
        sampler.add(b, ts)
        if seen is not None:
            seen.append((b, ts_ms))
        if bucket_ts <= ts < bucket_ts + step:
            return b, h, ts_ms
        if ts < bucket_ts:
            lo = b if lo is None else max(lo, b)
        else:
            hi = b if hi is None else min(hi, b)
        if lo is not None and hi is not None:
            if hi - lo <= 1:
                return None
            bisects += 1
            if bisects > max_bisect:
                break
            b = (lo + hi) // 2
            continue
        corrections += 1
        if corrections > max_corrections:
            break
        delta = round((target - ts) / BLOCK_SEC)
        if delta == 0:
            delta = 1 if ts < bucket_ts else -1
        b = max(0, b + delta)
    raise BlockSearchFailed(f"no block found in [{bucket_ts}, {bucket_ts + step}) after {corrections} corrections")


# ── conversion ───────────────────────────────────────────────────────────────────────────────────────────────
def _ss58(v: Any) -> str | None:
    from scalecodec.utils.ss58 import ss58_encode
    if v is None:
        return None
    if isinstance(v, str):
        if v.startswith("0x") and len(v) == 66:
            return ss58_encode(bytes.fromhex(v[2:]), 42)
        return v  # already SS58
    if isinstance(v, (bytes, bytearray)):
        return ss58_encode(bytes(v), 42)
    if isinstance(v, (list, tuple)):
        if len(v) == 1 and isinstance(v[0], (list, tuple)):
            v = v[0]
        return ss58_encode(bytes(int(x) for x in v), 42)
    return str(v)


def _text(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, str):
        return v
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).decode("utf-8", errors="replace")
    if isinstance(v, (list, tuple)):
        return bytes(int(x) & 0xFF for x in v).decode("utf-8", errors="replace")
    return str(v)


def _bits(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, dict):
        v = v.get("bits")
    return int(v) if v is not None else None


def convert_entry(entry: dict, *, ts: int, block: int, observed_at: int, source: str, price: float | None,
                  price_method: str, run_id: str) -> dict | None:
    """DynamicInfo → ``pool_history`` row (17 §2.3 conversion table); ``None`` when price ≤ 0 or tao_in ≤ 0."""
    netuid = int(entry["netuid"])
    tao_in = int(entry["tao_in"]) / 1e9
    alpha_in = int(entry["alpha_in"]) / 1e9
    if price is None:
        price = (tao_in / alpha_in) if alpha_in > 0 else 0.0
        price_method = "reserve_ratio"
    if price <= 0 or tao_in <= 0:
        return None
    mp_bits = _bits(entry.get("moving_price"))
    sv = entry.get("subnet_volume")
    reg = int(entry["network_registered_at"])
    return {"netuid": netuid, "block_number": int(block), "timestamp": int(ts), "price": float(price),
            "tao_in_pool": tao_in, "alpha_in_pool": alpha_in,
            "moving_price": (mp_bits / 2 ** 32) if mp_bits is not None else None,
            "network_registered_at": reg, "subnet_volume": (int(sv) / 1e9) if sv is not None else None,
            "occupant_key": f"{netuid}:{reg}", "source": source, "event_at": int(ts), "observed_at": int(observed_at),
            "price_method": price_method, "enriched_at": None, "run_id": run_id}


def owner_fields(entry: dict) -> dict:
    return {"owner_coldkey": _ss58(entry.get("owner_coldkey")), "owner_hotkey": _ss58(entry.get("owner_hotkey")),
            "subnet_name": _text(entry.get("subnet_name")), "token_symbol": _text(entry.get("token_symbol"))}


def api_price_method(entry: dict, api_price: float | None, tol: float = 1e-6) -> str:
    """Label a chain-API price by the mechanism it matches: the reserve ratio (constant product) or the Swap
    pallet's sqrt price. The API rounds to the rao, hence the tolerance."""
    if api_price is None:
        return "reserve_ratio"
    alpha = int(entry["alpha_in"])
    if alpha <= 0 or api_price <= 0:
        return "reserve_ratio"
    ratio = int(entry["tao_in"]) / alpha
    return "reserve_ratio" if abs(ratio - api_price) / api_price <= tol or abs(ratio - api_price) <= 1.5e-9 \
        else "swap_sqrt"


# ── swap price ───────────────────────────────────────────────────────────────────────────────────────────────
@dataclass
class SwapKeyCache:
    keys: list[str]
    built_at_block: int


async def prices_swap_sqrt(rpc: RpcGateway, block_hash: str, block: int, spec_version: int,
                           cache: dict[int, SwapKeyCache], regs: dict[int, int]) -> dict[int, float]:
    """``AlphaSqrtPrice`` (U64F64 LE) → ``v²`` per netuid; {} when the map is empty at that runtime.

    The key list is cached per ``specVersion`` and rebuilt when a netuid registered after the cache was built has no
    key (17 §2.3.1)."""
    kc = cache.get(spec_version)
    if kc is None:
        kc = cache[spec_version] = SwapKeyCache(await rpc.keys_paged(SWAP_SQRT_PREFIX, block_hash), block)
    if not kc.keys:
        return {}
    have = {netuid_from_key(k) for k in kc.keys}
    if any(n != 0 and n not in have and reg > kc.built_at_block for n, reg in regs.items()):
        keys = await rpc.keys_paged(SWAP_SQRT_PREFIX, block_hash)
        if block >= kc.built_at_block or len(keys) >= len(kc.keys):
            kc = cache[spec_version] = SwapKeyCache(sorted(set(kc.keys) | set(keys)), max(block, kc.built_at_block))
    vals = await rpc.query_storage_at(kc.keys, block_hash)
    out: dict[int, float] = {}
    for k, v in vals.items():
        x = le_int(v)
        if x is not None:
            s = x / 2 ** 64
            out[netuid_from_key(k)] = s * s
    return out


# ── one bucket ───────────────────────────────────────────────────────────────────────────────────────────────
async def process_bucket(rpc: RpcGateway, sampler: BlockSampler, bucket: Bucket, *, step: int, offset: int,
                         price_method: str, key_cache: dict[int, SwapKeyCache], run_id: str) -> BucketResult:
    res = BucketResult(bucket=bucket, status="failed", endpoint=rpc.endpoint.label)
    try:
        loc = await locate_block(rpc, sampler, bucket.bucket_ts, step=step, offset=offset, seen=res.block_times)
    except BlockSearchFailed as exc:
        res.error = f"block_search: {exc}"
        return res
    if loc is None:
        res.status, res.error = "skipped", "no_block_in_hour"
        return res
    b, h, ts_ms = loc
    ts = ts_ms // 1000
    res.block_number, res.block_ts = b, ts
    spec = await rpc.runtime_version(h)
    res.spec_version = spec
    try:
        entries = await rpc.dynamic_info(h, spec, meta_hash_fn=lambda: rpc.block_hash(b + 1))
    except NoRuntimeApi as exc:
        res.status, res.error = "skipped", f"no_runtime_api: {str(exc)[:120]}"
        return res
    none_entries = sum(e is None for e in entries)
    entries = [e for e in entries if e is not None]
    regs = {int(e["netuid"]): int(e["network_registered_at"]) for e in entries}
    prices: dict[int, float] = {}
    api_prices: dict[int, float] | None = None
    ep = rpc.endpoint
    if price_method == "auto":
        # the chain's own price where the runtime exposes it (BUILD-LOG Phase 3 #17: AlphaSqrtPrice went stale
        # between runtimes 419 and 424 while the storage lingered until 445)
        api_prices = await rpc.alpha_prices(h, spec, meta_hash_fn=lambda: rpc.block_hash(b + 1))
    if api_prices is None and (price_method == "swap_sqrt" or (price_method == "auto" and ep.kind != "archive")):
        try:
            prices = await prices_swap_sqrt(rpc, h, b, spec, key_cache, regs)
        except (RpcError, ValueError) as exc:
            if price_method == "swap_sqrt":
                raise
            logger.warning("swap_sqrt price failed; reserve_ratio", data={"block": b, "error": str(exc)[:160]})
            prices = {}
    observed = now_ts()
    skipped = 0
    methods: set[str] = set()
    for e in entries:
        n = int(e["netuid"])
        if api_prices is not None and n != 0:     # root: the API says 1.0; keep the reserve ratio (ledger parity)
            p = api_prices.get(n)
            m = api_price_method(e, p)
        elif api_prices is not None:
            p, m = None, "reserve_ratio"
        else:
            p = prices.get(n)
            m = "swap_sqrt" if p is not None else "reserve_ratio"
        row = convert_entry(e, ts=ts, block=b, observed_at=observed, source=ep.source, price=p, price_method=m,
                            run_id=run_id)
        if row is None:
            skipped += 1
            continue
        methods.add(row["price_method"])
        res.rows.append(row)
        res.owners.append({"netuid": n, "block_number": b, "occupant_key": row["occupant_key"], "event_at": ts,
                           "observed_at": observed, "source": ep.source, "run_id": run_id, **owner_fields(e)})
    res.status = "done"
    res.n_subnets = len(entries)
    res.price_method = "swap_sqrt" if "swap_sqrt" in methods else "reserve_ratio"
    res.counts = {"none_entries": none_entries, "skipped_nonpositive": skipped, "price_api": api_prices is not None,
                  "swap_sqrt_rows": sum(r["price_method"] == "swap_sqrt" for r in res.rows)}
    return res


# ── write ────────────────────────────────────────────────────────────────────────────────────────────────────
_POOL_COLS = ("netuid", "block_number", "timestamp", "price", "tao_in_pool", "alpha_in_pool", "moving_price",
              "network_registered_at", "subnet_volume", "occupant_key", "source", "event_at", "observed_at",
              "price_method", "enriched_at", "run_id")
_OWNER_FIELDS = ("owner_coldkey", "owner_hotkey", "subnet_name", "token_symbol")


def _bucket_row(con: sqlite3.Connection, res: BucketResult, run_id: str) -> None:
    con.execute(
        "INSERT INTO backfill_buckets (era, bucket_ts, status, block_number, block_ts, n_subnets, rows_written, "
        "price_method, endpoint, attempts, error, observed_at, run_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT (era, bucket_ts) DO UPDATE SET status=excluded.status, block_number=excluded.block_number, "
        "block_ts=excluded.block_ts, n_subnets=excluded.n_subnets, rows_written=excluded.rows_written, "
        "price_method=excluded.price_method, endpoint=excluded.endpoint, "
        "attempts=backfill_buckets.attempts + excluded.attempts, error=excluded.error, "
        "observed_at=excluded.observed_at, run_id=excluded.run_id WHERE backfill_buckets.status <> 'done'",
        (res.bucket.era, res.bucket.bucket_ts, res.status, res.block_number, res.block_ts, res.n_subnets,
         res.counts.get("rows_written"), res.price_method if res.status == "done" else None, res.endpoint,
         res.attempts, res.error, now_ts(), run_id))


def write_bucket(con: sqlite3.Connection, res: BucketResult, *, run_id: str) -> dict:
    """ONE transaction: pool_history (INSERT OR IGNORE) → rpc_owner_history (change-only) → block_times →
    backfill_buckets. A failed/skipped bucket writes only its block_times + bucket row."""
    out = {"pool_rows": 0, "owner_rows": 0}
    with common.tx(con):
        now = now_ts()
        for b, ms in res.block_times:
            con.execute("INSERT OR IGNORE INTO block_times (block_number, ts_ms, endpoint, observed_at, run_id) "
                        "VALUES (?,?,?,?,?)", (b, ms, res.endpoint, now, run_id))
        if res.status == "done":
            ph = ",".join("?" * len(_POOL_COLS))
            for r in res.rows:
                cur = con.execute(f"INSERT OR IGNORE INTO pool_history ({','.join(_POOL_COLS)}) VALUES ({ph})",
                                  [r[c] for c in _POOL_COLS])
                out["pool_rows"] += max(0, cur.rowcount)
            for o in res.owners:
                prev = con.execute(
                    "SELECT owner_coldkey, owner_hotkey, subnet_name, token_symbol FROM rpc_owner_history "
                    "WHERE occupant_key = ? AND block_number < ? ORDER BY block_number DESC LIMIT 1",
                    (o["occupant_key"], o["block_number"])).fetchone()
                if prev is None:
                    change = "first"
                else:
                    diff = [f for i, f in enumerate(_OWNER_FIELDS) if prev[i] != o[f]]
                    if not diff:
                        continue
                    change = ",".join(diff)
                cur = con.execute(
                    "INSERT OR IGNORE INTO rpc_owner_history (netuid, block_number, occupant_key, event_at, "
                    "observed_at, source, run_id, owner_coldkey, owner_hotkey, subnet_name, token_symbol, change) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (o["netuid"], o["block_number"], o["occupant_key"], o["event_at"], o["observed_at"], o["source"],
                     run_id, o["owner_coldkey"], o["owner_hotkey"], o["subnet_name"], o["token_symbol"], change))
                out["owner_rows"] += max(0, cur.rowcount)
        res.counts["rows_written"] = out["pool_rows"]
        _bucket_row(con, res, run_id)
    return out


# ── probe ────────────────────────────────────────────────────────────────────────────────────────────────────
GOLDEN_SN64 = {"tao_in_pool": 187212.134573233, "alpha_in_pool": 2897413.298870939,
               "moving_price": 0.06478730659000576, "network_registered_at": 4531295,
               "subnet_volume": 4657484.168541687, "price": 0.064653121}


def _rel(a: float | None, b: float | None) -> float | None:
    if a is None or b is None:
        return None
    return abs(a - b) / max(abs(b), 1e-18)


def probe_path() -> Path:
    return common.backfill_dir() / "rpc_probe.json"


def load_probe() -> dict:
    try:
        return json.loads(probe_path().read_text())
    except (OSError, ValueError):
        return {}


def probe_passed(endpoint_label: str, mode: str) -> bool:
    p = load_probe().get("by_endpoint", {}).get(endpoint_label) or {}
    return bool(p.get("pass")) and p.get("mode", "runtime_api") == mode


async def _cross_section(rpc: RpcGateway, block: int, cache: dict[int, SwapKeyCache]) -> dict:
    h = await rpc.block_hash(block)
    ts_ms = await rpc.timestamp_ms(h)
    spec = await rpc.runtime_version(h)
    entries = [e for e in await rpc.dynamic_info(h, spec, meta_hash_fn=lambda: rpc.block_hash(block + 1)) if e]
    regs = {int(e["netuid"]): int(e["network_registered_at"]) for e in entries}
    prices = await prices_swap_sqrt(rpc, h, block, spec, cache, regs)
    return {"hash": h, "ts_ms": ts_ms, "spec": spec, "entries": entries, "prices": prices}


async def probe(*, endpoint_override: str | None = None, db_path: Path | None = None) -> dict:
    """17 §2.3 ``--probe`` → ``rpc_probe.json`` (per endpoint). Pure reads; writes only ``block_times``."""
    from app.ratelimit import ProviderLimiter
    s = common.settings()
    eps = endpoints_from_settings(s, endpoint_override)
    rpc = RpcGateway(eps[:1], ProviderLimiter.from_settings(), timeout=s.BACKFILL_RPC_TIMEOUT_SEC,
                     max_attempts=s.BACKFILL_RPC_MAX_ATTEMPTS, backoff_max=s.ARCHIVE_BACKOFF_MAX_SEC,
                     public_max_rps=s.BACKFILL_RPC_PUBLIC_MAX_RPS, spend=common.SpendBook(db_path))
    checks: dict[str, Any] = {}
    ok = True
    con = common.open_db(db_path)
    cache: dict[int, SwapKeyCache] = {}
    try:
        await rpc.connect()
        # golden block 8,443,757
        g = await _cross_section(rpc, 8443757, cache)
        led = {r["netuid"]: dict(r) for r in con.execute(
            "SELECT * FROM pool_history WHERE block_number = 8443757 AND source LIKE 'ledger_%'")}
        by_n = {int(e["netuid"]): e for e in g["entries"]}
        rows = {n: convert_entry(e, ts=g["ts_ms"] // 1000, block=8443757, observed_at=now_ts(), source="probe",
                                 price=g["prices"].get(n), price_method="swap_sqrt", run_id="probe")
                for n, e in by_n.items()}
        worst: dict[str, float] = {}
        ref = led or {64: GOLDEN_SN64}
        for n, want in ref.items():
            got = rows.get(n)
            if got is None:
                worst["missing"] = worst.get("missing", 0) + 1
                continue
            for f in ("tao_in_pool", "alpha_in_pool", "subnet_volume", "moving_price"):
                e = _rel(got[f], want.get(f))
                if e is not None:
                    worst[f] = max(worst.get(f, 0.0), e)
            if got["network_registered_at"] != want.get("network_registered_at"):
                worst["network_registered_at_mismatch"] = worst.get("network_registered_at_mismatch", 0) + 1
            if n != 0:
                e = _rel(got["price"], want.get("price"))
                if e is not None:
                    worst["price_swap_sqrt"] = max(worst.get("price_swap_sqrt", 0.0), e)
                e = _rel(got["tao_in_pool"] / got["alpha_in_pool"], want.get("price"))
                if e is not None:
                    worst["price_reserve_ratio"] = max(worst.get("price_reserve_ratio", 0.0), e)
        raw64 = le_int(await rpc.storage(storage_key("SubtensorModule", "SubnetTAO", 64), g["hash"]))
        g_ok = (g["ts_ms"] // 1000 == 1781904264 and len(g["entries"]) == 129
                and all(worst.get(f, 0.0) <= 1e-9 for f in ("tao_in_pool", "alpha_in_pool", "subnet_volume",
                                                              "moving_price"))
                and not worst.get("network_registered_at_mismatch") and not worst.get("missing")
                and raw64 is not None and abs(raw64 / 1e9 - 187212.13) <= 0.01)
        price_match = ("swap_sqrt" if worst.get("price_swap_sqrt", 1) <= 1e-6 else
                       "reserve_ratio" if worst.get("price_reserve_ratio", 1) <= 1e-6 else None)
        g_ok = g_ok and price_match is not None
        checks["golden_8443757"] = {"pass": g_ok, "ts_ms": g["ts_ms"], "entries": len(g["entries"]),
                                    "spec_version": g["spec"], "compared_to": "ledger" if led else "spec_sn64",
                                    "max_rel_err": worst, "subnet_tao_64": raw64 / 1e9 if raw64 else None}
        ok &= g_ok
        # 6,502,594 and 4,920,351 timestamps
        h1 = await rpc.block_hash(6502594)
        t1 = await rpc.timestamp_ms(h1)
        checks["ledger_start_6502594"] = {"pass": t1 // 1000 == 1758585588, "ts_ms": t1}
        ok &= checks["ledger_start_6502594"]["pass"]
        l0 = await _cross_section(rpc, 4920351, cache)
        s64 = le_int(await rpc.storage(storage_key("SubtensorModule", "SubnetTAO", 64), l0["hash"]))
        l0_ok = l0["ts_ms"] // 1000 == DTAO_LAUNCH_TS and len(l0["entries"]) == 65 and s64 is not None \
            and abs(s64 / 1e9 - 0.709) < 0.01
        checks["dtao_launch_4920351"] = {"pass": l0_ok, "ts_ms": l0["ts_ms"], "entries": len(l0["entries"]),
                                         "spec_version": l0["spec"], "subnet_tao_64": s64 / 1e9 if s64 else None,
                                         "swap_sqrt_keys": len(l0["prices"])}
        ok &= l0_ok
        # 4,913,000: pre-dTAO → no runtime API
        h2 = await rpc.block_hash(4913000)
        try:
            spec2 = await rpc.runtime_version(h2)
            await rpc.dynamic_info(h2, spec2, meta_hash_fn=lambda: rpc.block_hash(4913001))
            checks["pre_dtao_4913000"] = {"pass": False, "result": "decoded (expected no_runtime_api)"}
        except NoRuntimeApi as exc:
            checks["pre_dtao_4913000"] = {"pass": True, "result": "no_runtime_api", "detail": str(exc)[:120]}
        ok &= checks["pre_dtao_4913000"]["pass"]
        # head
        head_b, _ = await rpc.head()
        hd = await _cross_section(rpc, head_b - 5, cache)
        checks["head"] = {"pass": len(hd["entries"]) >= 128, "block": head_b - 5, "entries": len(hd["entries"]),
                          "spec_version": hd["spec"], "swap_sqrt_keys": len(hd["prices"])}
        ok &= checks["head"]["pass"]
        # per-bucket request accounting on a second bucket of the golden runtime (runtime already cached)
        sampler = BlockSampler(list(GOLDEN_ANCHORS) + [(8443757, g["ts_ms"] // 1000)])
        bkt = Bucket("gap", ((g["ts_ms"] // 1000) // 3600 - 2) * 3600)
        before_req, before_tok = rpc.requests_sent, rpc.tokens_acquired
        br = await process_bucket(rpc, sampler, bkt, step=3600, offset=s.BACKFILL_RPC_SAMPLE_OFFSET_SEC,
                                  price_method="auto", key_cache=cache, run_id="probe")
        dreq, dtok = rpc.requests_sent - before_req, rpc.tokens_acquired - before_tok
        corrections = max(0, len(br.block_times) - 1)
        expected = (5 if (br.price_method == "swap_sqrt" or br.counts.get("price_api")) else 4) + 2 * corrections
        checks["requests_per_bucket"] = {"pass": br.status == "done" and dreq == dtok and dreq <= expected,
                                         "socket_requests": dreq, "tokens": dtok, "corrections": corrections,
                                         "expected_max": expected, "price_method": br.price_method}
        ok &= checks["requests_per_bucket"]["pass"]
        # block_times from the probe are exact anchors
        with common.tx(con):
            for b, ms in ((8443757, g["ts_ms"]), (6502594, t1), (4920351, l0["ts_ms"]), *br.block_times):
                con.execute("INSERT OR IGNORE INTO block_times (block_number, ts_ms, endpoint, observed_at, run_id) "
                            "VALUES (?,?,?,?,?)", (b, ms, rpc.endpoint.label, now_ts(), "probe"))
    except Exception as exc:
        ok = False
        checks["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
    finally:
        await rpc.close()
        con.close()
    try:
        import async_substrate_interface as asi
        from importlib.metadata import version
        asi_version = version("async-substrate-interface")
    except Exception:  # pragma: no cover
        asi, asi_version = None, None
    res = {"pass": bool(ok), "at": now_ts(), "at_iso": to_iso(now_ts()), "endpoint": rpc.endpoint.label,
           "mode": s.BACKFILL_RPC_MODE, "price_method_match": (checks.get("golden_8443757", {}) and price_match)
           if "golden_8443757" in checks else None, "asi_version": asi_version,
           "bittensor_wallet_installed": importlib.util.find_spec("bittensor_wallet") is not None,
           "checks": checks, "stats": rpc.stats(), "throttled": rpc.throttled > 0}
    doc = load_probe()
    doc.setdefault("by_endpoint", {})[rpc.endpoint.label] = res
    doc["latest"] = res
    doc["pass"], doc["endpoint"] = res["pass"], res["endpoint"]
    doc["price_method_match"] = res["price_method_match"]
    from app.util.fs import write_atomic
    write_atomic(probe_path(), doc)
    return res


# ── era runner ───────────────────────────────────────────────────────────────────────────────────────────────
@dataclass
class EraReport:
    era: str
    planned: int = 0
    done: int = 0
    failed: int = 0
    skipped: int = 0
    rows: int = 0
    owner_rows: int = 0
    stopped: str | None = None
    exit_code: int = 0
    seconds: float = 0.0
    from_ts: int | None = None
    to_ts: int | None = None


async def run_era(era: str, *, step: int | None = None, force_unprobed: bool = False,
                  endpoint_override: str | None = None, price_method: str | None = None,
                  from_block: int | None = None, to_block: int | None = None, limit: int | None = None,
                  db_path: Path | None = None, run_id: str | None = None,
                  gateway: RpcGateway | None = None) -> EraReport:
    """Plan → fetch (bounded concurrency) → write per bucket. Occupancy/stamp/bars run in the script afterwards."""
    from app.ratelimit import ProviderLimiter
    s = common.settings()
    step = step or s.BACKFILL_RPC_STEP_SEC
    pm = price_method or s.BACKFILL_RPC_PRICE_METHOD
    rep = EraReport(era=era)
    t0 = time.monotonic()
    run_id = run_id or f"script:backfill_rpc:{era}:{now_ts()}"
    con = common.open_db(db_path)
    try:
        eps = endpoints_from_settings(s, endpoint_override)
        rpc = gateway or RpcGateway(eps, ProviderLimiter.from_settings(), timeout=s.BACKFILL_RPC_TIMEOUT_SEC,
                                    max_attempts=s.BACKFILL_RPC_MAX_ATTEMPTS, backoff_max=s.ARCHIVE_BACKOFF_MAX_SEC,
                                    cooldown_sec=s.SUBTENSOR_ENDPOINT_COOLDOWN_SEC,
                                    public_max_rps=s.BACKFILL_RPC_PUBLIC_MAX_RPS,
                                    spend=common.SpendBook(db_path))
        if not force_unprobed and not probe_passed(rpc.endpoint.label, s.BACKFILL_RPC_MODE):
            raise common.PreconditionError(
                f"no passing probe for endpoint {rpc.endpoint.label!r} (run --probe, or --force-unprobed)",
                code=common.EXIT_VERIFY)
        sampler = BlockSampler(load_anchors(con, s.LEDGER_LIVE_FROM_TS))
        from_ts = sampler.ts_at(from_block) if from_block is not None else None
        to_ts = sampler.ts_at(to_block) if to_block is not None else None
        buckets = plan_buckets(con, era, step=step, now=now_ts(), min_netuids=s.BACKFILL_RPC_MIN_NETUIDS_PER_HOUR,
                               from_ts=from_ts, to_ts=to_ts)
        if limit is not None:
            buckets = buckets[:limit]
        rep.planned = len(buckets)
        if buckets:
            rep.from_ts, rep.to_ts = buckets[0].bucket_ts, buckets[-1].bucket_ts + step
        logger.info("backfill_rpc era planned", data={"era": era, "buckets": len(buckets), "step": step,
                                                      "endpoint": rpc.endpoint.label,
                                                      "from": to_iso(rep.from_ts), "to": to_iso(rep.to_ts)})
        if not buckets:
            return rep
        conc = s.BACKFILL_RPC_CONCURRENCY or (4 if rpc.endpoint.kind in ("blockmachine", "onfinality") else 1)
        key_cache: dict[int, SwapKeyCache] = {}
        write_lock = asyncio.Lock()
        queue: asyncio.Queue[Bucket] = asyncio.Queue()
        for b in buckets:
            queue.put_nowait(b)
        state = {"consec_failed": 0, "decode_fail": {}, "stop": None, "last_log": time.monotonic()}

        async def worker() -> None:
            while not queue.empty():
                if state["stop"] is not None:
                    return
                if common.stop_requested():
                    state["stop"] = "signal" if common.signalled() else "pause_all"
                    return
                bkt = queue.get_nowait()
                try:
                    if rpc.endpoint.kind == "archive":
                        await asyncio.sleep(s.ARCHIVE_MIN_BUCKET_INTERVAL_SEC)
                    res = await process_bucket(rpc, sampler, bkt, step=step, offset=s.BACKFILL_RPC_SAMPLE_OFFSET_SEC,
                                               price_method=pm, key_cache=key_cache, run_id=run_id)
                except asyncio.CancelledError:
                    t = asyncio.current_task()
                    if t is not None and t.cancelling():
                        raise
                    res = BucketResult(bucket=bkt, status="failed", endpoint=rpc.endpoint.label,
                                       error="CancelledError: leaked from the transport")
                except Exception as exc:  # one bad bucket never aborts the era (RpcError, DecodeError, KeyError…)
                    if not isinstance(exc, (RateLimited, RpcError, DecodeError, asyncio.TimeoutError)):
                        logger.error("bucket crashed", data={"bucket": bkt.bucket_ts, "error": repr(exc)[:300]})
                    res = BucketResult(bucket=bkt, status="failed", endpoint=rpc.endpoint.label,
                                       error=f"{type(exc).__name__}: {str(exc)[:300]}")
                async with write_lock:
                    try:
                        w = await asyncio.to_thread(write_bucket, con, res, run_id=run_id)
                    except sqlite3.DatabaseError as exc:
                        logger.error("bucket write aborted", data={"bucket": bkt.bucket_ts, "error": str(exc)})
                        res.status, res.error = "failed", f"write: {exc}"
                        state["trigger_aborts"] = state.get("trigger_aborts", 0) + 1
                        with common.tx(con):
                            _bucket_row(con, res, run_id)
                        w = {"pool_rows": 0, "owner_rows": 0}
                    rep.rows += w["pool_rows"]
                    rep.owner_rows += w["owner_rows"]
                    if res.status == "done":
                        rep.done += 1
                        state["consec_failed"] = 0
                        if res.spec_version is not None:
                            state["decode_fail"][res.spec_version] = 0
                    elif res.status == "skipped":
                        rep.skipped += 1
                    else:
                        rep.failed += 1
                        # only endpoint trouble counts toward failover (NOT_ENDPOINT_ERRORS)
                        if not (res.error or "").startswith(NOT_ENDPOINT_ERRORS):
                            state["consec_failed"] += 1
                        logger.warning("bucket failed", data={"era": era, "bucket": to_iso(bkt.bucket_ts),
                                                              "error": (res.error or "")[:200]})
                        if res.error and res.error.startswith("DecodeError") and res.spec_version is not None:
                            n = state["decode_fail"].get(res.spec_version, 0) + 1
                            state["decode_fail"][res.spec_version] = n
                            if n >= DECODE_STOP_AFTER:
                                state["stop"] = f"decode:{res.spec_version}"
                                rep.exit_code = common.EXIT_DECODE
                        if state["consec_failed"] >= 3 and len(rpc.endpoints) > 1:
                            state["consec_failed"] = 0
                            await rpc.fail_over(res.error or "3 failed buckets", from_label=res.endpoint)
                    if time.monotonic() - state["last_log"] > 30:
                        state["last_log"] = time.monotonic()
                        logger.info("backfill_rpc progress", data={
                            "era": era, "done": rep.done, "failed": rep.failed, "skipped": rep.skipped,
                            "left": queue.qsize(), "rows": rep.rows, "endpoint": rpc.endpoint.label,
                            "rate_buckets_per_s": round((rep.done + rep.failed + rep.skipped)
                                                        / max(1.0, time.monotonic() - t0), 2)})

        try:
            await asyncio.gather(*(worker() for _ in range(conc)))
        finally:
            if gateway is None:
                await rpc.close()
        rep.stopped = state["stop"]
        if state["stop"] in ("signal",):
            rep.exit_code = common.EXIT_INTERRUPTED
        if state.get("trigger_aborts"):
            rep.exit_code = rep.exit_code or common.EXIT_VERIFY
        logger.info("backfill_rpc era finished", data={**rep.__dict__, "stats": rpc.stats()})
        return rep
    finally:
        rep.seconds = round(time.monotonic() - t0, 1)
        con.close()


# ── daily emission share (spec 20 N2 item 3) ─────────────────────────────────────────────────────────────────
EMISSION_SOURCE_LIVE = "rpc_storage"     # chain_rpc_hourly's rows; never counted as this pass's work


def emission_shares(entries: Iterable[dict]) -> dict[int, float | None]:
    """``100 · tao_in_emission / Σ`` over netuids ≠ 0 (TMC ``emission_percent`` scale); ``None`` when Σ = 0."""
    vals = {int(e["netuid"]): int(e.get("tao_in_emission") or 0) for e in entries if int(e["netuid"]) != 0}
    tot = sum(vals.values())
    return {n: (100.0 * v / tot if tot > 0 else None) for n, v in vals.items()}


def plan_emission_days(con: sqlite3.Connection, *, now: int, from_ts: int | None = None,
                       to_ts: int | None = None) -> list[int]:
    """Day starts from the first full day after dTAO launch (the launch day's first hour predates the runtime API)
    whose first hour closed ≥ 1 h ago and that hold no row of this pass."""
    a = (DTAO_LAUNCH_TS // 86400 + 1) * 86400
    b = ((now - 7200) // 86400) * 86400 + 86400
    if from_ts is not None:
        a = max(a, (from_ts // 86400) * 86400)
    if to_ts is not None:
        b = min(b, to_ts)
    have = {r[0] for r in con.execute("SELECT DISTINCT as_of FROM rpc_subnet_daily WHERE source <> ? AND as_of >= ? "
                                      "AND as_of < ?", (EMISSION_SOURCE_LIVE, a, b))}
    return [d for d in range(a, b, 86400) if d not in have]


@dataclass
class EmissionDay:
    day_ts: int
    status: str = "failed"
    block_number: int | None = None
    block_ts: int | None = None
    endpoint: str | None = None
    rows: list[dict] = field(default_factory=list)
    block_times: list[tuple[int, int]] = field(default_factory=list)
    spec_version: int | None = None
    error: str | None = None


async def process_emission_day(rpc: RpcGateway, sampler: BlockSampler, day_ts: int, *, offset: int,
                               run_id: str) -> EmissionDay:
    """The block nearest ``hour(day_ts) + 3600 − offset`` inside that hour (17 §2.3 search), then DynamicInfo."""
    res = EmissionDay(day_ts=day_ts, endpoint=rpc.endpoint.label)
    hour = (day_ts // 3600) * 3600
    try:
        loc = await locate_block(rpc, sampler, hour, step=3600, offset=offset, seen=res.block_times)
    except BlockSearchFailed as exc:
        res.error = f"block_search: {exc}"
        return res
    if loc is None:
        res.status, res.error = "skipped", "no_block_in_hour"
        return res
    b, h, ts_ms = loc
    ts = ts_ms // 1000
    res.block_number, res.block_ts = b, ts
    spec = await rpc.runtime_version(h)
    res.spec_version = spec
    try:
        entries = await rpc.dynamic_info(h, spec, meta_hash_fn=lambda: rpc.block_hash(b + 1))
    except NoRuntimeApi as exc:
        res.status, res.error = "skipped", f"no_runtime_api: {str(exc)[:120]}"
        return res
    entries = [e for e in entries if e is not None]
    shares = emission_shares(entries)
    observed = now_ts()
    as_of = (day_ts // 86400) * 86400
    for e in entries:
        n = int(e["netuid"])
        if n == 0:
            continue
        mp = _bits(e.get("moving_price"))
        res.rows.append({"netuid": n, "as_of": as_of, "occupant_key": f"{n}:{int(e['network_registered_at'])}",
                         "block_number": b, "observed_at": observed, "event_at": ts, "source": rpc.endpoint.source,
                         "run_id": run_id, "moving_price": (mp / 2 ** 32) if mp is not None else None,
                         "endpoint": rpc.endpoint.label, "emission_share": shares.get(n)})
    res.status = "done"
    return res


_EMISSION_COLS = ("netuid", "as_of", "occupant_key", "block_number", "observed_at", "event_at", "source", "run_id",
                  "moving_price", "endpoint", "emission_share")


def write_emission_day(con: sqlite3.Connection, res: EmissionDay, *, run_id: str) -> int:
    """ONE transaction: block_times + ``rpc_subnet_daily`` (INSERT OR IGNORE: append-only, first success wins)."""
    n = 0
    with common.tx(con):
        now = now_ts()
        for b, ms in res.block_times:
            con.execute("INSERT OR IGNORE INTO block_times (block_number, ts_ms, endpoint, observed_at, run_id) "
                        "VALUES (?,?,?,?,?)", (b, ms, res.endpoint, now, run_id))
        if res.status == "done":
            ph = ",".join("?" * len(_EMISSION_COLS))
            for r in res.rows:
                cur = con.execute(f"INSERT OR IGNORE INTO rpc_subnet_daily ({','.join(_EMISSION_COLS)}) "
                                  f"VALUES ({ph})", [r[c] for c in _EMISSION_COLS])
                n += max(0, cur.rowcount)
    return n


async def run_emission_daily(*, force_unprobed: bool = False, endpoint_override: str | None = None,
                             from_block: int | None = None, to_block: int | None = None, limit: int | None = None,
                             db_path: Path | None = None, run_id: str | None = None,
                             gateway: RpcGateway | None = None) -> EraReport:
    """Plan → fetch (bounded concurrency) → write per day. ``failed`` and ``skipped`` days are retried next run."""
    from app.ratelimit import ProviderLimiter
    s = common.settings()
    rep = EraReport(era="emission_daily")
    t0 = time.monotonic()
    run_id = run_id or f"script:backfill_rpc:emission_daily:{now_ts()}"
    con = common.open_db(db_path)
    try:
        rpc = gateway or RpcGateway(endpoints_from_settings(s, endpoint_override), ProviderLimiter.from_settings(),
                                    timeout=s.BACKFILL_RPC_TIMEOUT_SEC, max_attempts=s.BACKFILL_RPC_MAX_ATTEMPTS,
                                    backoff_max=s.ARCHIVE_BACKOFF_MAX_SEC,
                                    cooldown_sec=s.SUBTENSOR_ENDPOINT_COOLDOWN_SEC,
                                    public_max_rps=s.BACKFILL_RPC_PUBLIC_MAX_RPS, spend=common.SpendBook(db_path))
        if not force_unprobed and not probe_passed(rpc.endpoint.label, s.BACKFILL_RPC_MODE):
            raise common.PreconditionError(
                f"no passing probe for endpoint {rpc.endpoint.label!r} (run --probe, or --force-unprobed)",
                code=common.EXIT_VERIFY)
        sampler = BlockSampler(load_anchors(con, s.LEDGER_LIVE_FROM_TS))
        from_ts = sampler.ts_at(from_block) if from_block is not None else None
        to_ts = sampler.ts_at(to_block) if to_block is not None else None
        days = plan_emission_days(con, now=now_ts(), from_ts=from_ts, to_ts=to_ts)
        if limit is not None:
            days = days[:limit]
        rep.planned = len(days)
        if days:
            rep.from_ts, rep.to_ts = days[0], days[-1] + 86400
        logger.info("backfill_rpc emission_daily planned", data={"days": len(days), "endpoint": rpc.endpoint.label,
                                                                 "from": to_iso(rep.from_ts), "to": to_iso(rep.to_ts)})
        if not days:
            return rep
        conc = s.BACKFILL_RPC_CONCURRENCY or (4 if rpc.endpoint.kind in ("blockmachine", "onfinality") else 1)
        write_lock = asyncio.Lock()
        queue: asyncio.Queue[int] = asyncio.Queue()
        for d in days:
            queue.put_nowait(d)
        state = {"consec_failed": 0, "stop": None, "last_log": time.monotonic()}

        async def worker() -> None:
            while not queue.empty():
                if state["stop"] is not None:
                    return
                if common.stop_requested():
                    state["stop"] = "signal" if common.signalled() else "pause_all"
                    return
                d = queue.get_nowait()
                try:
                    if rpc.endpoint.kind == "archive":
                        await asyncio.sleep(s.ARCHIVE_MIN_BUCKET_INTERVAL_SEC)
                    res = await process_emission_day(rpc, sampler, d, offset=s.BACKFILL_RPC_SAMPLE_OFFSET_SEC,
                                                     run_id=run_id)
                except asyncio.CancelledError:
                    t = asyncio.current_task()
                    if t is not None and t.cancelling():
                        raise
                    res = EmissionDay(day_ts=d, endpoint=rpc.endpoint.label,
                                      error="CancelledError: leaked from the transport")
                except Exception as exc:  # one bad day never aborts the pass
                    if not isinstance(exc, (RateLimited, RpcError, DecodeError, asyncio.TimeoutError)):
                        logger.error("emission day crashed", data={"day": d, "error": repr(exc)[:300]})
                    res = EmissionDay(day_ts=d, endpoint=rpc.endpoint.label,
                                      error=f"{type(exc).__name__}: {str(exc)[:300]}")
                async with write_lock:
                    try:
                        rep.rows += await asyncio.to_thread(write_emission_day, con, res, run_id=run_id)
                    except sqlite3.DatabaseError as exc:
                        logger.error("emission day write aborted", data={"day": d, "error": str(exc)})
                        res.status, res.error = "failed", f"write: {exc}"
                    if res.status == "done":
                        rep.done += 1
                        state["consec_failed"] = 0
                    elif res.status == "skipped":
                        rep.skipped += 1
                    else:
                        rep.failed += 1
                        if not (res.error or "").startswith(NOT_ENDPOINT_ERRORS):
                            state["consec_failed"] += 1
                        logger.warning("emission day failed", data={"day": to_iso(d), "error": (res.error or "")[:200]})
                        if state["consec_failed"] >= 3 and len(rpc.endpoints) > 1:
                            state["consec_failed"] = 0
                            await rpc.fail_over(res.error or "3 failed days", from_label=res.endpoint)
                    if time.monotonic() - state["last_log"] > 30:
                        state["last_log"] = time.monotonic()
                        logger.info("backfill_rpc progress", data={
                            "era": "emission_daily", "done": rep.done, "failed": rep.failed, "skipped": rep.skipped,
                            "left": queue.qsize(), "rows": rep.rows, "endpoint": rpc.endpoint.label})

        try:
            await asyncio.gather(*(worker() for _ in range(conc)))
        finally:
            if gateway is None:
                await rpc.close()
        rep.stopped = state["stop"]
        if state["stop"] == "signal":
            rep.exit_code = common.EXIT_INTERRUPTED
        logger.info("backfill_rpc era finished", data={**rep.__dict__, "stats": rpc.stats()})
        return rep
    finally:
        rep.seconds = round(time.monotonic() - t0, 1)
        con.close()
