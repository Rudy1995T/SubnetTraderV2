"""Owner data sources C1–C8 (spec 11 §2.3; live shapes §2.3.1, probed 2026-10-18). Fetchers never raise on HTTP
errors — they return ``(data | None, status)``; ``BudgetExhausted`` propagates (the run ends ``paused:budget``).
Every TMC call goes through spec 05's ``TmcClient`` (02 §G2), wrapped in ``PacedTmc`` for this job's own pacing."""
from __future__ import annotations

import asyncio
import hashlib
import html as _html
import json
import re
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from app.integrity.models import OwnerKeys, RufusSubnet
from app.util.time import parse_iso, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.collectors.chain_tmc import TmcClient, TmcResponse
    from app.http import Http
    from app.storage.db import Database

RAO = 1e9
_SS58 = re.compile(r"^5[1-9A-HJ-NP-Za-km-z]{47}$")
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
FIELD_MAP_PATH = "data/owner_field_map.json"


# ── units / small helpers ────────────────────────────────────────────────────────────────────────────────────
def num(x: Any) -> float | None:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def rao(x: Any) -> float | None:
    v = num(x)
    return None if v is None else v / RAO


def iint(x: Any) -> int | None:
    v = num(x)
    return None if v is None else int(v)


def ts_of(x: Any) -> int | None:
    if x is None or x == "":
        return None
    if isinstance(x, (int, float)):
        return int(x / 1000) if x > 1e11 else int(x)
    try:
        return parse_iso(str(x))
    except (ValueError, TypeError):
        return None


def ss58_encode(pubkey_hex: str, prefix: int = 42) -> str | None:
    """0x + 32-byte public key → SS58 (subnetradar shows new owners as hex, §2.3.1)."""
    h = (pubkey_hex or "").lower().removeprefix("0x")
    if not re.fullmatch(r"[0-9a-f]{64}", h):
        return None
    payload = bytes([prefix]) + bytes.fromhex(h)
    chk = hashlib.blake2b(b"SS58PRE" + payload, digest_size=64).digest()[:2]
    raw = payload + chk
    n = int.from_bytes(raw, "big")
    out = ""
    while n:
        n, r = divmod(n, 58)
        out = _B58[r] + out
    pad = len(raw) - len(raw.lstrip(b"\0"))
    return "1" * pad + out


def is_ss58(s: Any) -> bool:
    return isinstance(s, str) and bool(_SS58.match(s))


def load_field_map(path: str | Path | None = None) -> dict:
    """``data/owner_field_map.json`` written by ``scripts/owner_probe.py``; the verified defaults apply without it."""
    import app.config as app_config
    p = app_config.settings.path(path or FIELD_MAP_PATH)
    try:
        return json.loads(Path(p).read_text())
    except (OSError, ValueError):
        return {}


def activity_time_format(setting: str) -> str:
    if setting != "auto":
        return setting
    return load_field_map().get("activity_time_format", "iso")


def fmt_time(ts: int, fmt: str) -> Any:
    if fmt == "s":
        return int(ts)
    if fmt == "ms":
        return int(ts) * 1000
    return to_iso(int(ts))


# ── pacing ───────────────────────────────────────────────────────────────────────────────────────────────────
class RunBudgetSpent(Exception):
    """This run's own TMC ceiling (``OWNER_TMC_MAX_PER_RUN``) or the hard stop time was reached."""


class PacedTmc:
    """Job-local ceiling on top of 01's limiter: ≤ ``per_min`` requests in any 60 s and ≤ ``per_run`` per run;
    no request after ``stop_at`` (unix s). ``clock``/``sleep`` are injectable for tests."""

    def __init__(self, tmc: "TmcClient", *, per_min: int, per_run: int, stop_at: float | None = None,
                 clock: Callable[[], float] = time.time, sleep: Callable[[float], Any] = asyncio.sleep) -> None:
        self.tmc = tmc
        self.per_min = max(1, int(per_min))
        self.per_run = int(per_run)
        self.stop_at = stop_at
        self.clock = clock
        self.sleep = sleep
        self.n = 0
        self._recent: deque[float] = deque()
        self.calls: list[str] = []

    @property
    def remaining(self) -> int:
        return max(0, self.per_run - self.n)

    async def _gate(self) -> None:
        if self.n >= self.per_run:
            raise RunBudgetSpent("per_run")
        while True:
            now = self.clock()
            if self.stop_at is not None and now >= self.stop_at:
                raise RunBudgetSpent("stop_at")
            while self._recent and now - self._recent[0] >= 60.0:
                self._recent.popleft()
            if len(self._recent) < self.per_min:
                break
            await self.sleep(60.0 - (now - self._recent[0]) + 0.01)
        self._recent.append(self.clock())
        self.n += 1

    async def get(self, path: str, params: dict | None = None) -> "TmcResponse":
        await self._gate()
        self.calls.append(path)
        return await self.tmc.get(path, params)


# ── C1 owner keys ────────────────────────────────────────────────────────────────────────────────────────────
async def list_snapshot(db: "Database", *, now: int, max_age_s: int = 12 * 3600) -> tuple[dict[int, dict], int | None]:
    """04's newest ``/subnets/`` snapshot (≤ 12 h) → {netuid: row}; 0 requests (02 §G2)."""
    r = await db.fetchone("SELECT observed_at, payload_json FROM chain_rest_snapshots WHERE endpoint = '/subnets/' "
                          "AND payload_json IS NOT NULL AND observed_at <= ? ORDER BY observed_at DESC LIMIT 1", (now,),
                          writer=True)
    if not r or now - int(r["observed_at"]) > max_age_s:
        return {}, None
    try:
        body = json.loads(r["payload_json"])
    except ValueError:
        return {}, None
    rows = body.get("results") if isinstance(body, dict) else body
    out = {}
    for row in rows or []:
        n = iint(row.get("netuid"))
        if n is not None:
            out[n] = row
    return out, int(r["observed_at"])


def map_subnet_row(row: dict, *, source: str) -> OwnerKeys:
    """One ``/subnets/`` list row or a ``/subnets/{n}/`` answer → ``OwnerKeys`` (α units)."""
    ls = row.get("latest_snapshot") or {}

    def g(*keys: str) -> Any:
        for k in keys:
            if ls.get(k) not in (None, ""):
                return ls.get(k)
            if row.get(k) not in (None, ""):
                return row.get(k)
        return None
    poc = rao(g("pending_owner_cut"))
    since = iint(g("blocks_since_last_step"))
    aoe = rao(g("subnet_alpha_out_emission"))
    cut = poc / since if (poc is not None and since) else None
    enabled = None
    if poc is not None and since and aoe and aoe > 0:
        enabled = poc > 0
    stake = rao(row.get("owner_personal_stake"))
    lists_zero = source == "tmc_list"                         # the list carries 0 for the owner lock fields
    return OwnerKeys(
        coldkey=g("subnet_owner") if is_ss58(g("subnet_owner")) else None,
        hotkey=g("subnet_owner_hotkey") if is_ss58(g("subnet_owner_hotkey")) else None,
        source=source, block_number=iint(g("block_number")), network_registered_at=iint(g("network_registered_at")),
        first_emission_block=iint(g("first_emission_block_number")), owner_cut_per_block=cut, pending_owner_cut=poc,
        owner_cut_enabled=enabled,
        personal_stake=None if lists_zero else stake,
        locked=None if lists_zero else rao(row.get("owner_personal_locked_alpha")),
        perpetual=None if lists_zero else rao(row.get("owner_personal_perpetual_alpha")),
        decaying=None if lists_zero else rao(row.get("owner_personal_decaying_alpha")),
        raw={k: g(k) for k in ("subnet_owner", "subnet_owner_hotkey", "pending_owner_cut", "blocks_since_last_step",
                               "subnet_alpha_out_emission", "first_emission_block_number", "network_registered_at",
                               "block_number")})


async def fetch_owner_keys(tmc: PacedTmc | None, netuid: int, *, list_rows: dict[int, dict],
                           source_mode: str) -> tuple[OwnerKeys | None, str]:
    row = list_rows.get(int(netuid))
    if source_mode != "detail" and row is not None:
        k = map_subnet_row(row, source="tmc_list")
        if k.coldkey and k.hotkey:
            return k, "ok:list"
        if source_mode == "list":
            return k, "ok:no_keys"
    if source_mode == "list" or tmc is None:
        return None, "skip:no_list"
    r = await tmc.get(f"/subnets/{int(netuid)}/")
    if not r.ok or not isinstance(r.json, dict):
        return None, f"fail:{r.status or r.error}"
    k = map_subnet_row(r.json, source="tmc_detail")
    return k, "ok" if (k.coldkey and k.hotkey) else "ok:no_keys"


# ── C2 coldkey stakes ────────────────────────────────────────────────────────────────────────────────────────
async def fetch_coldkey_stakes(tmc: PacedTmc, coldkey: str) -> tuple[dict | None, str]:
    r = await tmc.get(f"/accounts/coldkey-stakes/{coldkey}/")
    if r.ok and isinstance(r.json, (dict, list)):
        return (r.json if isinstance(r.json, dict) else {"subnets": r.json}), "ok"
    if r.status == 404:
        return None, "fail:404"
    return None, f"fail:{r.status or r.error}"


def map_coldkey_stakes(payload: dict | None, netuid: int) -> dict | None:
    """{total, locked, available, hotkeys[], is_subnet_owner, rows} for netuid n; an answer without a row for n is a
    real zero (TMC says no stakes)."""
    if payload is None:
        return None
    rows = payload.get("subnets") or payload.get("results") or []
    mine = [r for r in rows if iint(r.get("subnet", r.get("netuid", r.get("subnet_id")))) == int(netuid)]
    total = sum(rao(r.get("balance_alpha")) or 0.0 for r in mine)
    locked = sum(rao(r.get("locked_alpha")) or 0.0 for r in mine)
    hks = [{"hotkey": h.get("address"), "stake": rao(h.get("stake")), "locked": rao(h.get("locked_alpha")),
            "is_subnet_owner": h.get("is_subnet_owner"), "role": h.get("role")}
           for r in mine for h in (r.get("hotkeys") or [])]
    return {"total": total, "locked": locked, "available": max(0.0, total - locked), "hotkeys": hks,
            "is_subnet_owner": any(h.get("is_subnet_owner") for h in hks),
            "rows": [{k: v for k, v in r.items() if k not in ("usd", "eur", "hotkeys")} for r in mine]}


# ── C3 owner-hotkey series ───────────────────────────────────────────────────────────────────────────────────
async def fetch_neuron_series(tmc: PacedTmc, netuid: int, hotkey: str, span: str) -> tuple[list[dict] | None, str]:
    r = await tmc.get(f"/analytics/neuron/{int(netuid)}/{hotkey}/", {"span": span})
    if not r.ok:
        return None, f"fail:{r.status or r.error}"
    body = r.json
    rows = body if isinstance(body, list) else (body.get("results") or body.get("data") or []) if isinstance(body, dict) else []
    return list(rows), "ok"


def map_neuron_rows(rows: list[dict]) -> list[dict]:
    out = []
    for r in rows:
        ts = ts_of(r.get("ts") or r.get("timestamp"))
        if ts is None:
            continue
        out.append({"hour_ts": ts - ts % 3600, "event_at": ts, "block_number": iint(r.get("block_number")),
                    "alpha_stake": rao(r.get("alpha_stake")), "conviction": rao(r.get("conviction")),
                    "conviction_locked_alpha": rao(r.get("conviction_locked_alpha")),
                    "conviction_perpetual_alpha": rao(r.get("conviction_perpetual_alpha")),
                    "conviction_decaying_alpha": rao(r.get("conviction_decaying_alpha")),
                    "hotkey": r.get("hotkey")})
    return out


# ── C4 staking activity ──────────────────────────────────────────────────────────────────────────────────────
def map_activity_row(r: dict) -> dict | None:
    ts = ts_of(r.get("timestamp"))
    if ts is None:
        return None
    return {"id": r.get("id"), "extrinsic": r.get("extrinsic"), "block_number": iint(r.get("block_number")),
            "event_at": ts, "function": r.get("function"), "success": r.get("success", True),
            "is_proxy": bool(r.get("is_proxy")), "signer": r.get("signer_coldkey"),
            "from_coldkey": r.get("from_coldkey"), "to_coldkey": r.get("to_coldkey"),
            "from_hotkey": r.get("from_hotkey"), "to_hotkey": r.get("to_hotkey"),
            "from_netuid": iint(r.get("from_netuid")), "to_netuid": iint(r.get("to_netuid")),
            "from_alpha": rao(r.get("from_alpha_amount")), "to_alpha": rao(r.get("to_alpha_amount")),
            "from_tao": rao(r.get("from_tao_amount")), "to_tao": rao(r.get("to_tao_amount")),
            "price": num(r.get("from_alpha_price") or r.get("to_alpha_price")),
            "enabled": r.get("enabled"), "raw": r}


async def fetch_activity(tmc: PacedTmc, coldkey: str, from_ts: int, to_ts: int, *, page_limit: int, max_pages: int,
                         time_format: str = "iso") -> tuple[list[dict], str, bool]:
    """Ascending pages (``ordering=block_number``) of ``FROM``…``TO``; returns (rows, status, truncated)."""
    out: list[dict] = []
    for page in range(max_pages):
        params = {"coldkey": coldkey, "FROM": fmt_time(from_ts, time_format), "TO": fmt_time(to_ts, time_format),
                  "ordering": "block_number", "limit": page_limit}
        if page:
            params["offset"] = page * page_limit
        r = await tmc.get("/extrinsics/staking-activity/", params)
        if not r.ok:
            return out, ("fail:" + str(r.status or r.error)) if page == 0 else f"partial:{r.status or r.error}", True
        body = r.json
        rows = body.get("results", []) if isinstance(body, dict) else (body or [])
        out.extend(rows)
        if len(rows) < page_limit or (isinstance(body, dict) and not body.get("next")):
            return out, "ok", False
    return out, "ok", True


# ── C6 Rufus ─────────────────────────────────────────────────────────────────────────────────────────────────
def _ci(d: dict, key: str) -> Any:
    if key in d:
        return d[key]
    low = key.lower()
    for k, v in d.items():
        if isinstance(k, str) and k.lower() == low:
            return v
    return None


def _pct(x: Any) -> float | None:
    v = num(x)
    if v is None:
        return None
    return v / 100.0 if v > 1 else v


def parse_rufus(payload: Any) -> dict[int, RufusSubnet]:
    """Tolerant: ``{data: {"<netuid>": {...}}}`` (live), a bare list of subnet objects, or a dict keyed by netuid."""
    if isinstance(payload, dict) and isinstance(_ci(payload, "data"), dict):
        items = list(_ci(payload, "data").items())
    elif isinstance(payload, dict):
        items = [(k, v) for k, v in payload.items() if isinstance(v, dict)]
    elif isinstance(payload, list):
        items = [(str(_ci(v, "netuid")), v) for v in payload if isinstance(v, dict)]
    else:
        return {}
    out: dict[int, RufusSubnet] = {}
    for k, v in items:
        meta = _ci(v, "meta") or {}
        n = iint(_ci(meta, "netuid")) if _ci(meta, "netuid") is not None else iint(_ci(v, "netuid"))
        if n is None:
            n = iint(k)
        if n is None:
            continue
        owner = _ci(v, "owner") or {}
        to_owner = _ci(v, "to_owner") or {}
        chal = _ci(v, "challenger") or {}
        rows = _ci(v, "rows") or []
        orow = next((r for r in rows if isinstance(r, dict) and _ci(r, "bucket") == "owner"), None)
        al = _ci(v, "auto_lock_enabled")
        out[n] = RufusSubnet(
            netuid=n, owner_coldkey=_ci(meta, "owner_coldkey"), owner_hotkey=_ci(meta, "owner_hotkey"),
            registration_block=iint(_ci(meta, "registration_block")),
            auto_lock_enabled=None if al is None else bool(al),
            owner_perp=num(_ci(owner, "locked_perp")) if owner else None,
            owner_decay=num(_ci(owner, "locked_decay")) if owner else None,
            owner_pct=_pct(_ci(owner, "pct")) if owner else None,
            owner_perpetual_flag=None if orow is None else bool(_ci(orow, "perpetual")),
            to_owner_perp=num(_ci(to_owner, "locked_perp")), to_owner_lockers=iint(_ci(to_owner, "count")),
            challenger_max_pct=_pct(_ci(chal, "max_pct")),
            challenger_coldkeys=tuple(sorted({_ci(r, "coldkey") for r in rows if isinstance(r, dict)
                                              and _ci(r, "bucket") == "challenger" and _ci(r, "coldkey")})),
            days_to_king=num(_ci(v, "days_to_king")))
    return out


async def _get(http: "Http", provider: str, url: str, *, timeout: float, accept: str | None = None):
    from app.http import ProviderError
    try:
        return await http.request(provider, "GET", url, timeout=timeout, follow_redirects=True,
                                  headers={"Accept": accept} if accept else None), None
    except ProviderError as exc:
        return None, f"fail:{exc.status or exc}"


async def fetch_rufus(http: "Http", *, table_url: str, baked_url: str,
                      last_baked_at: int | None) -> tuple[Any | None, str, int | None, dict]:
    """→ (table payload | None, status, baked_at, raw {name: body}). ``table.json`` is fetched only when
    ``baked_at_utc`` changed; status ``unchanged`` lets the caller reuse its stored table."""
    raw: dict = {}
    r, e = await _get(http, "rufus", baked_url, timeout=60)
    if r is None:
        return None, e or "fail", None, raw
    try:
        baked = r.json()
    except ValueError:
        return None, "fail:baked_json", None, raw
    raw["rufus/baked-at.json"] = baked
    baked_at = ts_of(baked.get("baked_at_utc")) if isinstance(baked, dict) else None
    if baked_at is not None and last_baked_at is not None and baked_at == last_baked_at:
        return None, "unchanged", baked_at, raw
    r, e = await _get(http, "rufus", table_url, timeout=90)
    if r is None:
        return None, e or "fail", baked_at, raw
    try:
        table = r.json()
    except ValueError:
        return None, "fail:table_json", baked_at, raw
    raw["rufus/table.json"] = table
    return table, "ok", baked_at, raw


# ── C7 subnetradar ───────────────────────────────────────────────────────────────────────────────────────────
_TR = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
_TITLE_KEY = re.compile(r'title="(5[1-9A-HJ-NP-Za-km-z]{47}|0x[0-9a-fA-F]{64})"')
_DATE = re.compile(r"\b([A-Z][a-z]{2} \d{1,2}, \d{4})\b")
_BLOCK = re.compile(r"taostats\.io/block/(\d+)")


def _key(s: str) -> str | None:
    return s if is_ss58(s) else ss58_encode(s)


def parse_subnetradar(body: str, content_type: str = "text/html") -> list[dict]:
    """→ [{netuid | None (all subnets), old_coldkey, new_coldkey, block, date_ts, type}] — never raises."""
    try:
        if "json" in (content_type or "") or body.lstrip()[:1] in ("[", "{"):
            data = json.loads(body)
            rows = data if isinstance(data, list) else data.get("swaps") or data.get("results") or []
            return [{"netuid": iint(r.get("netuid")), "old_coldkey": r.get("old_coldkey") or r.get("previous_owner"),
                     "new_coldkey": r.get("new_coldkey") or r.get("new_owner"), "block": iint(r.get("block")),
                     "date_ts": ts_of(r.get("date") or r.get("timestamp")), "type": r.get("type") or "coldkey_swap"}
                    for r in rows if isinstance(r, dict)]
        out = []
        for tr in _TR.findall(body or ""):
            keys = _TITLE_KEY.findall(tr)
            if len(keys) < 2:
                continue
            nets = [int(n) for n in re.findall(r'href="/subnet/(\d{1,4})"', tr)]
            all_subnets = "All subnets" in tr
            dm = _DATE.search(_html.unescape(re.sub(r"<[^>]+>", " ", tr)))
            date_ts = None
            if dm:
                try:
                    date_ts = int(datetime.strptime(dm.group(1), "%b %d, %Y").replace(tzinfo=timezone.utc).timestamp())
                except ValueError:
                    date_ts = None
            bm = _BLOCK.search(tr)
            base = {"old_coldkey": _key(keys[0]), "new_coldkey": _key(keys[-1]),
                    "block": int(bm.group(1)) if bm else None, "date_ts": date_ts, "type": "coldkey_swap"}
            if nets:
                out += [dict(base, netuid=n) for n in nets]
            elif all_subnets or not nets:
                out.append(dict(base, netuid=None))
        return out
    except Exception:
        return []


async def fetch_subnetradar(http: "Http", url: str) -> tuple[list[dict], str, str | None]:
    r, e = await _get(http, "subnetradar", url, timeout=60)
    if r is None:
        return [], e or "fail", None
    body = r.text
    rows = parse_subnetradar(body, r.headers.get("content-type", ""))
    return rows, "ok" if rows else "parse_empty", body
