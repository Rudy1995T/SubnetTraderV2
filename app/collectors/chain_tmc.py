"""TaoMarketCap access (spec 05 §8, 02 §G2): ``TmcClient`` — the only TMC caller in the process — plus the
``chain_rest_snapshots`` helpers and the SSE line parser. The SSE recorder (``TableState``, ``ChainSseTask``) lives
further down this module.

``TmcClient`` sits on 01's ``Http.request('tmc', …)`` / ``ProviderLimiter['tmc']`` (8/min, burst 2, 500/h,
90k/month). Error contract: 200 → ``TmcResponse(ok=True)``; 404/401/403 and ``ProviderError`` → ``ok=False``;
``BudgetExhausted`` propagates.
"""
from __future__ import annotations

import json
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, AsyncIterator, Callable
from urllib.parse import urlencode

import app.config as app_config
from app.http import ProviderError
from app.logging.logger import logger
from app.util.time import now_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    import httpx

    from app.config import Settings
    from app.http import Http
    from app.storage.db import Database


@dataclass(slots=True)
class TmcResponse:
    ok: bool
    status: int | None
    json: Any
    headers: dict[str, str] = field(default_factory=dict)
    error: str | None = None
    elapsed_ms: int = 0
    url: str = ""


def canonical_params(params: dict | None) -> str:
    """Sorted query string without ``offset`` (the snapshot key, 03 ``chain_rest_snapshots.params``)."""
    if not params:
        return ""
    items = sorted((k, v) for k, v in params.items() if k != "offset" and v is not None)
    return urlencode(items)


class TmcClient:
    def __init__(self, http: "Http", settings_getter: Callable[[], "Settings"] | None = None) -> None:
        self.http = http
        self._s = settings_getter or (lambda: app_config.settings)

    def _url(self, path: str) -> str:
        return self._s().TMC_BASE_URL.rstrip("/") + path

    async def get(self, path: str, params: dict | None = None, *, timeout: float | None = None) -> TmcResponse:
        s = self._s()
        url = self._url(path)
        t0 = time.monotonic()
        try:
            resp = await self.http.request("tmc", "GET", url, params=params,
                                           timeout=timeout if timeout is not None else s.TMC_REQUEST_TIMEOUT_SEC,
                                           retries=s.TMC_MAX_RETRIES)
        except ProviderError as exc:
            return TmcResponse(False, exc.status, None, {}, str(exc), int((time.monotonic() - t0) * 1000), path)
        ms = int((time.monotonic() - t0) * 1000)
        headers = {k.lower(): v for k, v in resp.headers.items()}
        if resp.status_code != 200:
            return TmcResponse(False, resp.status_code, None, headers, f"HTTP {resp.status_code}", ms, path)
        try:
            body = resp.json()
        except ValueError:
            return TmcResponse(False, 200, None, headers, "invalid_json", ms, path)
        return TmcResponse(True, 200, body, headers, None, ms, path)

    async def get_paginated(self, path: str, params: dict | None = None, *, page_limit: int,
                            max_pages: int = 10) -> TmcResponse:
        """``limit``/``offset`` paging; accepts a bare list or ``{count, next, results}`` (never follows ``next``)."""
        base = dict(params or {})
        results: list = []
        count: int | None = None
        last: TmcResponse | None = None
        for page in range(max_pages):
            q = {**base, "limit": page_limit}
            if page:
                q["offset"] = page * page_limit
            r = await self.get(path, q)
            last = r
            if not r.ok:
                if page == 0:
                    return r
                break
            body = r.json
            if isinstance(body, list):
                if page == 0:
                    return TmcResponse(True, 200, {"count": len(body), "results": body}, r.headers, None,
                                       r.elapsed_ms, path)
                results.extend(body)
                break
            rows = body.get("results") or []
            results.extend(rows)
            count = body.get("count", count)
            if len(rows) < page_limit or (count is not None and len(results) >= count):
                break
        assert last is not None
        return TmcResponse(True, 200, {"count": count if count is not None else len(results), "results": results},
                           last.headers, None, last.elapsed_ms, path)

    # ── named endpoints (G2 callers) ────────────────────────────────────────────────────────────────────────
    async def fetch_subnets(self) -> TmcResponse:
        s = self._s()
        return await self.get_paginated("/subnets/", page_limit=s.TMC_SUBNETS_PAGE_LIMIT, max_pages=5)

    async def fetch_subnets_table(self) -> TmcResponse:
        return await self.get("/subnets/table/")

    async def fetch_coldkey_identities(self) -> TmcResponse:
        r = await self.get("/accounts/coldkey-identities/")
        if r.ok and isinstance(r.json, dict) and "results" in r.json and r.json.get("next"):
            return await self.get_paginated("/accounts/coldkey-identities/", page_limit=1000, max_pages=5)
        return r

    async def fetch_analytics(self, netuid: int, span: str, fields: str | None = None) -> TmcResponse:
        params: dict[str, Any] = {"span": span}
        if fields:
            params["fields"] = fields
        return await self.get(f"/analytics/subnet/{int(netuid)}/", params)

    async def fetch_candles(self, netuid: int, period: str, from_ts: int, to_ts: int, *,
                            timeout: float | None = None) -> TmcResponse:
        return await self.get(f"/subnets/{int(netuid)}/candle-chart-compact/",
                              {"period": period, "from_timestamp": to_iso(from_ts), "to_timestamp": to_iso(to_ts)},
                              timeout=timeout)

    async def fetch_holders(self, netuid: int) -> TmcResponse:
        s = self._s()
        return await self.get_paginated(f"/accounts/subnet-holders/{int(netuid)}/", page_limit=s.TMC_HOLDERS_PAGE_LIMIT,
                                        max_pages=s.TMC_HOLDERS_MAX_PAGES)

    async def fetch_pressure(self, side: str, from_ts: int, to_ts: int, *, netuid: int | None = None,
                             bucket: str = "1d") -> TmcResponse:
        params: dict[str, Any] = {"FROM": to_iso(from_ts), "TO": to_iso(to_ts), "side": side, "bucket": bucket}
        if netuid is not None:
            params["subnet"] = netuid
        return await self.get("/extrinsics/staking-activity/pressure/", params)

    async def fetch_trending(self, limit: int = 50) -> TmcResponse:
        return await self.get("/analytics/trending/subnets/", {"limit": limit})

    async def fetch_market(self) -> TmcResponse:
        return await self.get("/market/market-data/")

    @asynccontextmanager
    async def stream_sse(self, url: str) -> AsyncIterator["httpx.Response"]:
        """Open the SSE stream on the ``tmc_sse`` client, holding one of the ``TMC_SSE_MAX_CONNECTIONS`` slots."""
        import httpx
        s = self._s()
        limiter = self.http.limiter
        if not limiter.open_connection("tmc_sse"):
            raise RuntimeError("tmc_sse connection slots exhausted")
        try:
            client = self.http.client("tmc_sse")
            timeout = httpx.Timeout(10.0, read=float(s.TMC_SSE_READ_TIMEOUT_SEC))
            async with client.stream("GET", url, headers={"Accept": "text/event-stream"}, timeout=timeout) as r:
                yield r
        finally:
            limiter.close_connection("tmc_sse")


# ── chain_rest_snapshots (03 key: endpoint, params, observed_at) ────────────────────────────────────────────────
def _subnets_params() -> str:
    return canonical_params({"limit": app_config.settings.TMC_SUBNETS_PAGE_LIMIT})


REST_SNAPSHOT_KINDS: dict[str, tuple[str, Callable[[], str]]] = {
    "subnets": ("/subnets/", _subnets_params),
    "subnets_table": ("/subnets/table/", lambda: ""),
    "coldkey_identities": ("/accounts/coldkey-identities/", lambda: ""),
    "burn": ("/subnets/burn/", lambda: ""),
    "network_lock_cost": ("/subnets/network-lock-cost/", lambda: ""),
    "validators": ("/subnets/validators/", lambda: ""),
    "market_data": ("/market/market-data/", lambda: ""),
    "neurons": ("/subnets/neurons/{n}/", lambda: ""),
    "analytics_subnet": ("/analytics/subnet/{n}/", lambda: ""),
}


@dataclass(slots=True)
class RestSnapshot:
    endpoint: str
    params: str
    observed_at: int
    run_id: str
    http_status: int | None
    payload: Any


def _kind_key(kind: str, path_args: dict | None, params: str | None) -> tuple[str, str]:
    endpoint, pfn = REST_SNAPSHOT_KINDS[kind]
    if path_args:
        endpoint = endpoint.format(**path_args)
    return endpoint, (params if params is not None else pfn())


async def write_rest_snapshot(db: "Database", kind: str, resp: TmcResponse, *, run_id: str,
                              path_args: dict | None = None, params: str | None = None,
                              observed_at: int | None = None) -> None:
    endpoint, p = _kind_key(kind, path_args, params)
    await db.execute(
        "INSERT OR IGNORE INTO chain_rest_snapshots (endpoint, params, observed_at, run_id, http_status, payload_json, "
        "event_at, source) VALUES (?,?,?,?,?,?,NULL,'tmc')",
        (endpoint, p, int(observed_at if observed_at is not None else now_ts()), run_id, resp.status,
         json.dumps(resp.json, separators=(",", ":"))))


async def latest_rest_snapshot(db: "Database", kind: str, *, before: int | None = None, max_age_s: int | None = None,
                               path_args: dict | None = None) -> RestSnapshot | None:
    endpoint, _ = _kind_key(kind, path_args, None)
    t = int(before if before is not None else now_ts())
    q = "SELECT * FROM chain_rest_snapshots WHERE endpoint = ? AND observed_at <= ?"
    args: list[Any] = [endpoint, t]
    if max_age_s is not None:
        q += " AND observed_at >= ?"
        args.append(t - int(max_age_s))
    row = await db.fetchone(q + " ORDER BY observed_at DESC LIMIT 1", args)
    if not row:
        return None
    try:
        payload = json.loads(row["payload_json"]) if row["payload_json"] else None
    except ValueError:
        payload = None
    return RestSnapshot(row["endpoint"], row["params"], row["observed_at"], row["run_id"], row["http_status"], payload)


# ── SSE parsing ──────────────────────────────────────────────────────────────────────────────────────────────
@dataclass(slots=True)
class SseEvent:
    event: str | None
    data: str
    id: str | None


async def parse_sse(lines: AsyncIterator[str]) -> AsyncIterator[SseEvent]:
    """``data:`` lines are joined, a blank line ends an event, ``:`` lines are keepalive comments.
    A truncated final event (no terminating blank line) is dropped."""
    data: list[str] = []
    ev: str | None = None
    eid: str | None = None
    async for raw in lines:
        line = raw.rstrip("\r")
        if line == "":
            if data:
                yield SseEvent(ev, "\n".join(data), eid)
            data, ev, eid = [], None, None
            continue
        if line.startswith(":"):
            continue
        name, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if name == "data":
            data.append(value)
        elif name == "event":
            ev = value
        elif name == "id":
            eid = value


def log_once(key: str, data: Any = None, _seen: set = set()) -> None:  # noqa: B006
    if key in _seen:
        return
    _seen.add(key)
    logger.info(key, data=data)


# ══ SSE recorder (spec 05 §2.1–§2.2, §2.8) ════════════════════════════════════════════════════════════════════
import asyncio  # noqa: E402
from pathlib import Path  # noqa: E402

from app.jobs import LongTask  # noqa: E402
from app.storage.precedence import precedence_upsert_sql  # noqa: E402
from app.util.time import floor_hour  # noqa: E402

HOT_FIELDS = ("price", "subnet_moving_price", "emission", "emission_enabled", "immune", "deregistration_risk",
              "excess_tao", "subnet_ema_tao_flow", "owner_personal_locked_alpha", "total_locked_alpha",
              "total_subnet_stake", "tao_liquidity")
FLAG_FIELDS = ("emission_enabled", "immune", "deregistration_risk", "network_registered_at", "is_active")
HOURLY_COLUMNS = (
    "name", "symbol", "logo_url", "is_active", "emission_enabled", "immune", "deregistration_risk", "price",
    "price_after", "price_difference_hour", "price_difference_day", "price_difference_week", "price_difference_month",
    "subnet_moving_price", "emission", "excess_tao", "subnet_ema_tao_flow", "owner_personal_stake",
    "owner_personal_locked_alpha", "owner_personal_perpetual_alpha", "owner_personal_decaying_alpha",
    "total_locked_alpha", "total_subnet_stake", "tao_liquidity", "alpha_liquidity", "circulating_supply", "marketcap",
    "marketcap_rank", "fdv", "volume", "miners_tao_per_day", "neuron_regs_burned_24h", "root_prop", "adr",
    "network_registered_at", "subnet_owner", "commits_7d", "commits_30d", "commits_90d", "chain_buys_per_block",
    "subsidized_breakpoint", "is_subsidized")
_INT_COLS = {"is_active", "emission_enabled", "immune", "deregistration_risk", "marketcap_rank",
             "network_registered_at", "commits_7d", "commits_30d", "commits_90d", "is_subsidized", "block_number"}
_TEXT_COLS = {"name", "symbol", "logo_url", "subnet_owner"}
_KNOWN_FIELDS = frozenset(HOURLY_COLUMNS) | {"subnet", "block_number"}


def coerce(col: str, v: Any) -> Any:
    """Store as delivered, but never let a wrong type abort a STRICT batch (05 §7 partial rows)."""
    if v is None:
        return None
    if col in _TEXT_COLS:
        return str(v)
    if col in _INT_COLS:
        if isinstance(v, bool):
            return int(v)
        try:
            f = float(v)
        except (TypeError, ValueError):
            return None
        return int(f) if f == int(f) else None
    if isinstance(v, bool):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def immune_local(block: int | None, network_registered_at: int | None, period: int) -> bool | None:
    if block is None or network_registered_at is None:
        return None
    return (int(block) - int(network_registered_at)) < int(period)


def occupant_of(row: dict) -> str | None:
    reg = coerce("network_registered_at", row.get("network_registered_at"))
    n = row.get("subnet")
    return f"{int(n)}:{reg}" if isinstance(n, int) and reg is not None else None


class TableState:
    def __init__(self) -> None:
        self._rows: dict[int, dict] = {}
        self._changed: dict[int, set[str]] = {}
        self.last_event_at: int | None = None
        self.last_block: int | None = None

    def replace(self, rows: list[dict]) -> None:
        new = {}
        for r in rows:
            n = r.get("subnet")
            if isinstance(n, int):
                new[n] = dict(r)
        self._changed = {}
        for n, r in new.items():
            old = self._rows.get(n)
            if old is not None:
                ch = {f for f in FLAG_FIELDS if old.get(f) != r.get(f)}
                if ch:
                    self._changed[n] = ch
        self._rows = new

    def patch(self, changes: dict[str, dict], added: list[dict]) -> set[int]:
        self._changed = {}
        touched: set[int] = set()
        for k, fields in (changes or {}).items():
            try:
                n = int(k)
            except (TypeError, ValueError):
                continue
            row = self._rows.setdefault(n, {"subnet": n})
            ch = {f for f in FLAG_FIELDS if f in fields and row.get(f) != fields[f]}
            row.update(fields)
            if ch:
                self._changed[n] = ch
            touched.add(n)
        for r in added or []:
            n = r.get("subnet")
            if isinstance(n, int):
                self._rows[n] = dict(r)
                self._changed.setdefault(n, set()).add("added")
                touched.add(n)
        return touched

    def block_number(self) -> int | None:
        bs = [coerce("block_number", r.get("block_number")) for r in self._rows.values()]
        bs = [b for b in bs if b is not None]
        return max(bs) if bs else None

    def rows(self) -> dict[int, dict]:
        return self._rows

    def changed_flags(self, netuid: int) -> set[str]:
        return self._changed.get(netuid, set())

    def field_set(self) -> frozenset[str]:
        out: set[str] = set()
        for r in self._rows.values():
            out |= set(r)
        return frozenset(out)

    def mark_event(self, block: int | None, ts: int) -> None:
        self.last_event_at = ts
        if block is not None:
            self.last_block = block


BLOCK_SECONDS = 12


class StaleStream(RuntimeError):
    pass


class ChainSseTask(LongTask):
    """LongTask ``chain_sse``: TMC SSE table stream → chain_ticks / chain_hourly / price_bars / chain_network_hourly."""

    def __init__(self, rt, spec) -> None:
        super().__init__(rt, spec)
        s = app_config.settings
        self.backoff_min_sec = float(s.TMC_SSE_BACKOFF_MIN_SEC)
        self.backoff_max_sec = float(s.TMC_SSE_BACKOFF_MAX_SEC)
        self.tmc = TmcClient(rt.http) if rt.http is not None else None
        self.state = "connecting"
        self.tbl = TableState()
        self.last_block = -1
        self.last_block_change_at: int | None = None
        self.last_tick_block: dict[int, int] = {}
        self.current_hour: int | None = None
        self.hour_last_event_at: int | None = None
        self.hour_source = "sse"
        self.occ_keys: dict[int, str] = {}
        self.unauthorized_until = 0
        self.fallback_task: asyncio.Task | None = None
        self.fallback_state: str | None = None
        self.fallback_since: int | None = None
        self.down_alerted_since: int | None = None
        self.bad_events = 0
        self.field_nulls = 0
        self.ticks_written = 0
        self.hourly_rows_last_flush = 0
        self.event_at_source = "arrival"
        self.block_source: str | None = None
        self.run_id = "sse:0"
        self._first_flush_done = False
        self._drift_seen: set[str] = set()
        self.anchor: tuple[int, int] | None = None

    # ── connection lifetime ────────────────────────────────────────────────────────────────────────────────
    async def run_once(self, stop: asyncio.Event) -> None:
        s = app_config.settings
        now = now_ts()
        if self.tbl.last_event_at and now - self.tbl.last_event_at > s.TMC_SSE_SILENCE_SEC:
            self.start_fallback("silent")
        if now < self.unauthorized_until:
            await self._wait(stop, 15)
            return
        self.state = "connecting"
        connected_at = now_ts()
        self.run_id = f"sse:{connected_at}"
        got_full = False
        try:
            async with self.tmc.stream_sse(s.TMC_SSE_URL) as r:
                if r.status_code == 429:
                    ra = r.headers.get("retry-after")
                    await self._wait(stop, max(float(ra) if ra and ra.isdigit() else 60.0, 60.0))
                    return
                if r.status_code in (401, 403):
                    self.state = "unauthorized"
                    self.unauthorized_until = now_ts() + s.TMC_SSE_UNAUTHORIZED_RETRY_SEC
                    self.start_fallback("unauthorized")
                    return
                if r.status_code != 200:
                    raise RuntimeError(f"SSE HTTP {r.status_code}")
                self.state = "connected"
                self.mark_connected(True)
                self.last_block_change_at = now_ts()
                async for ev in parse_sse(r.aiter_lines()):
                    if stop.is_set():
                        return
                    try:
                        msg = json.loads(ev.data)
                    except ValueError:
                        self.bad_events += 1
                        continue
                    t = msg.get("type") if isinstance(msg, dict) else None
                    if t == "heartbeat":
                        self.beat()
                        self._check_stale()
                        continue
                    if t == "full":
                        rows = msg.get("data")
                        if not isinstance(rows, list) or len(rows) < 100:
                            self.bad_events += 1
                            continue
                        got_full = True
                        await self._on_event("full", rows, None, source="sse")
                        await self.on_full()
                    elif t == "delta":
                        if not got_full:
                            continue
                        await self._on_event("delta", None, msg, source="sse")
                    else:
                        log_once(f"sse_unknown_type:{t}", {"type": t})
                        continue
                    self._check_stale()
        finally:
            self.mark_connected(False)
            if self.state == "connected":
                self.state = "connecting"
            if stop.is_set() and self.current_hour is not None and self.tbl.rows():
                try:
                    await self._write_hourly(self.current_hour)
                except Exception as exc:  # pragma: no cover
                    logger.warning("partial hour flush failed", data={"error": str(exc)})

    def _check_stale(self) -> None:
        s = app_config.settings
        if self.last_block_change_at and now_ts() - self.last_block_change_at > s.TMC_SSE_SILENCE_SEC:
            self.state = "silent"
            self.start_fallback("stale_block")
            raise StaleStream("block number has not advanced")

    async def _on_event(self, kind: str, rows: list | None, msg: dict | None, *, source: str) -> None:
        now = now_ts()
        hour = floor_hour(now)
        if self.current_hour is not None and hour > self.current_hour and self.tbl.rows():
            await self._flush_hour(self.current_hour)
        if kind == "full":
            fields_before = self.tbl.field_set()
            self.tbl.replace(rows or [])
            if not fields_before:
                await self._schema_check()
        else:
            self.tbl.patch(msg.get("changes") or {}, msg.get("added") or [])
        self.beat()
        row_block = self.tbl.block_number()
        if kind == "full" or self.anchor is None:
            self.anchor = (row_block, now) if row_block is not None else None
        # Live deltas carry no block number (per row or envelope; verified 2026-09-24) and /sse/blocks/ stays
        # silent, so between fulls the block advances with arrival time at 12 s/block (BUILD-LOG).
        block = row_block
        if self.anchor is not None and kind == "delta":
            block = max(row_block or 0, self.anchor[0] + (now - self.anchor[1]) // BLOCK_SECONDS)
            self.block_source = "row+estimated"
        elif self.block_source is None:
            self.block_source = "row"
        self.tbl.mark_event(block, now)
        if self.current_hour is None or hour > self.current_hour:
            self.current_hour = hour
        self.hour_last_event_at = now
        self.hour_source = source
        if block is None or block <= self.last_block:
            return
        self.last_block_change_at = now
        await self._write_ticks(block, source)
        self.last_block = block

    async def _schema_check(self) -> None:
        s = app_config.settings
        fields = sorted(self.tbl.field_set())
        path = s.path(s.DATA_DIR) / "tmc_table_fields.json"
        try:
            if not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps({"captured_at": now_ts(), "fields": fields, "block_source": "row",
                                            "event_at_source": "arrival"}, indent=1))
                frozen = set(fields)
            else:
                frozen = set(json.loads(path.read_text()).get("fields", []))
        except (OSError, ValueError):
            frozen = set(_KNOWN_FIELDS)
        added = sorted(set(fields) - frozen)
        removed = sorted(frozen - set(fields))
        if added or removed:
            import hashlib
            h = hashlib.sha256(json.dumps([added, removed]).encode()).hexdigest()[:16]
            if h not in self._drift_seen:
                self._drift_seen.add(h)
                await self._emit("tmc_schema_drift", None, None, {"added": added, "removed": removed}, h, "info")

    async def _emit(self, et: str, occ: str | None, netuid: int | None, payload: dict, dedupe: str | None,
                    severity: str) -> None:
        from app.storage.events import emit_event
        try:
            now = now_ts()
            await emit_event(self.rt.db, event_type=et, occupant_key=occ, netuid=netuid if occ else None,
                             severity=severity, event_at=now, observed_at=now, payload=payload, dedupe_key=dedupe,
                             source="chain", run_id=self.run_id)
        except Exception as exc:
            logger.warning("chain event write failed", data={"event_type": et, "error": str(exc)})

    # ── ticks ──────────────────────────────────────────────────────────────────────────────────────────────
    async def _write_ticks(self, block: int, source: str, *, closing_at: int | None = None) -> None:
        """Stride/flag ticks; ``closing_at`` = hour flush: one tick of the hour's last state for every netuid whose
        last tick is older (so the 5m/1h bars close on the hour's final state, spec 05 §9 hour-flush test)."""
        s = app_config.settings
        stride = max(1, int(s.CHAIN_TICKS_BLOCK_STRIDE))
        now = closing_at if closing_at is not None else now_ts()
        out = []
        flips = []
        for n, r in self.tbl.rows().items():
            key = occupant_of(r)
            if key is None:
                continue
            prev_key = self.occ_keys.get(n)
            if prev_key and prev_key != key:
                logger.info("sse_reoccupation_seen", data={"netuid": n, "old_key": prev_key, "new_key": key,
                                                           "block_number": block})
            self.occ_keys[n] = key
            flags = self.tbl.changed_flags(n)
            last = self.last_tick_block.get(n, -10**9)
            new_window = block // stride > last // stride
            if closing_at is not None:
                if last >= block:
                    continue
            elif not (new_window or flags):
                continue
            row = {"netuid": n, "block_number": block, "occupant_key": key, "event_at": now,
                   "observed_at": now_ts() if closing_at is not None else now,
                   "source": source, "run_id": self.run_id}
            for f in HOT_FIELDS:
                v = coerce(f, r.get(f))
                if v is None and r.get(f) is not None:
                    self.field_nulls += 1
                row[f] = v
            out.append(row)
            self.last_tick_block[n] = block
            if closing_at is None and "emission_enabled" in flags and n != 0 and r.get("is_active"):
                flips.append((n, key, r.get("emission_enabled")))
        if not out:
            return
        cols = list(out[0])
        ph = ",".join("?" * len(cols))
        await self.rt.db.executemany(f"INSERT OR IGNORE INTO chain_ticks ({','.join(cols)}) VALUES ({ph})",
                                     [[r[c] for c in cols] for r in out])
        self.ticks_written += len(out)
        for n, key, now_flag in flips:
            et = "emission_reenabled" if now_flag else "emission_disabled"
            await self._emit(et, key, n, {"netuid": n, "block_number": block, "prev": int(not now_flag),
                                          "now": int(bool(now_flag))}, f"{key}|{block}",
                             "info" if now_flag else "critical")

    # ── hourly flush and rollups ───────────────────────────────────────────────────────────────────────────
    async def _flush_hour(self, hour_ts: int) -> None:
        if self.tbl.last_block is not None and self.hour_last_event_at is not None:
            await self._write_ticks(self.tbl.last_block, self.hour_source, closing_at=self.hour_last_event_at)
        await self._write_hourly(hour_ts)
        await self._rollup_5m(hour_ts)
        await self._rollup_1h(hour_ts)
        if not self._first_flush_done:
            self._first_flush_done = True
            for h in range(hour_ts - 23 * 3600, hour_ts, 3600):
                if not await self.rt.db.fetchval("SELECT 1 FROM price_bars WHERE period = '1h' AND source = "
                                                 "'sse_rollup' AND ts = ? LIMIT 1", (h,)):
                    await self._rollup_5m(h)
                    await self._rollup_1h(h)
        await self._network_row(hour_ts)

    async def _write_hourly(self, hour_ts: int) -> None:
        s = app_config.settings
        now = now_ts()
        rows = []
        for n, r in self.tbl.rows().items():
            key = occupant_of(r)
            if key is None:
                continue
            d: dict[str, Any] = {"netuid": n, "hour_ts": hour_ts, "occupant_key": key,
                                 "block_number": max(coerce("block_number", r.get("block_number")) or 0,
                                                     self.tbl.last_block or 0) or None,
                                 "event_at": self.hour_last_event_at or now, "observed_at": now,
                                 "source": self.hour_source, "run_id": self.run_id, "backfilled": 0}
            for c in HOURLY_COLUMNS:
                d[c] = coerce(c, r.get(c))
            il = immune_local(d["block_number"], d["network_registered_at"], s.NETWORK_IMMUNITY_PERIOD_BLOCKS)
            d["immune_local"] = None if il is None else int(il)
            extras = {k: v for k, v in r.items() if k not in _KNOWN_FIELDS}
            d["extras_json"] = json.dumps(extras, separators=(",", ":")) if extras else None
            rows.append(d)
        if not rows:
            return
        cols = list(rows[0])
        sql = precedence_upsert_sql("chain_hourly", cols, ("netuid", "hour_ts"))
        await self.rt.db.executemany(sql, rows)
        self.hourly_rows_last_flush = len(rows)

    async def _tick_groups(self, start: int, end: int) -> dict[int, list[dict]]:
        rows = await self.rt.db.fetchall(
            "SELECT netuid, occupant_key, block_number, event_at, observed_at, price, subnet_moving_price, tao_liquidity "
            "FROM chain_ticks WHERE event_at >= ? AND event_at < ? AND price > 0 ORDER BY netuid, block_number",
            (start, end), writer=True)
        out: dict[int, list[dict]] = {}
        for r in rows:
            out.setdefault(r["netuid"], []).append(r)
        return out

    async def _upsert_bars(self, bars: list[dict]) -> None:
        if not bars:
            return
        cols = list(bars[0])
        sql = precedence_upsert_sql("price_bars", cols, ("netuid", "period", "ts"),
                                    coalesce=("tao_in_pool", "alpha_in_pool", "moving_price", "block_number",
                                              "occupant_key", "tao_price_usd"))
        await self.rt.db.executemany(sql, bars)

    def _bar(self, netuid: int, period: str, ts: int, ticks: list[dict]) -> dict | None:
        close = ticks[-1]
        ticks = [t for t in ticks if t["occupant_key"] == close["occupant_key"]]
        prices = [t["price"] for t in ticks]
        if not prices or close["price"] is None or close["price"] <= 0:
            return None
        return {"netuid": netuid, "period": period, "ts": ts, "occupant_key": close["occupant_key"],
                "open": ticks[0]["price"], "high": max(prices), "low": min(prices), "close": close["price"],
                "volume": None, "ts_last": close["event_at"],
                "tao_in_pool": close["tao_liquidity"] / 1e9 if close["tao_liquidity"] is not None else None,
                "alpha_in_pool": None, "moving_price": close["subnet_moving_price"],
                "block_number": close["block_number"], "source": "sse_rollup", "event_at": close["event_at"],
                "observed_at": now_ts(), "backfilled": 0, "run_id": self.run_id}

    async def _rollup_5m(self, hour_ts: int) -> None:
        bars = []
        for n, ticks in (await self._tick_groups(hour_ts, hour_ts + 3600)).items():
            buckets: dict[int, list[dict]] = {}
            for t in ticks:
                buckets.setdefault(t["event_at"] // 300 * 300, []).append(t)
            for b, tk in sorted(buckets.items()):
                bar = self._bar(n, "5m", b, tk)
                if bar:
                    bars.append(bar)
        await self._upsert_bars(bars)

    async def _rollup_1h(self, hour_ts: int) -> None:
        bars = []
        for n, ticks in (await self._tick_groups(hour_ts, hour_ts + 3600)).items():
            bar = self._bar(n, "1h", hour_ts, ticks)
            if bar:
                bars.append(bar)
        await self._upsert_bars(bars)

    async def _network_row(self, hour_ts: int) -> None:
        rows = self.tbl.rows().values()
        active = [r for r in rows if r.get("subnet") != 0 and r.get("is_active")]
        prices = [coerce("price", r.get("price")) for r in active]
        tao = [coerce("tao_liquidity", r.get("tao_liquidity")) for r in rows]
        tao_usd, src = None, None
        snap = await latest_rest_snapshot(self.rt.db, "market_data", max_age_s=3 * 3600)
        if snap and isinstance(snap.payload, dict):
            tao_usd = market_price_usd(snap.payload)
            src = "market_data" if tao_usd is not None else None
        await self.rt.db.execute(
            "INSERT OR IGNORE INTO chain_network_hourly (hour_ts, block_number, sum_subnet_prices, n_active, "
            "total_subnet_tao, tao_usd, tao_usd_source, event_at, observed_at, source, run_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (hour_ts, self.tbl.block_number(), sum(p for p in prices if p is not None), len(active),
             sum(t for t in tao if t is not None) / 1e9, tao_usd, src, self.hour_last_event_at, now_ts(),
             self.hour_source, self.run_id))

    # ── REST fallback ──────────────────────────────────────────────────────────────────────────────────────
    def start_fallback(self, reason: str) -> None:
        if not app_config.settings.TMC_TABLE_FALLBACK_ENABLED:
            return
        if self.fallback_task is None or self.fallback_task.done():
            self.fallback_state = reason
            self.fallback_since = self.fallback_since or now_ts()
            self.fallback_task = asyncio.create_task(self._fallback_loop())

    async def _fallback_loop(self) -> None:
        from app.ratelimit import BudgetExhausted
        s = app_config.settings
        while True:
            now = now_ts()
            if self.fallback_since and now - self.fallback_since >= s.TMC_SSE_ALERT_AFTER_SEC \
                    and self.down_alerted_since is None:
                self.down_alerted_since = self.fallback_since
                await self._emit("chain_sse_down", None, None,
                                 {"since": self.fallback_since, "reconnects": self.reconnects,
                                  "last_block": self.tbl.last_block}, str(self.fallback_since), "warning")
            self.attempt()
            try:
                r = await self.tmc.fetch_subnets_table()
                if r.ok and isinstance(r.json, list) and len(r.json) >= 100:
                    self.fallback_state = "polling"
                    await self._on_event("full", r.json, None, source="rest_table")
            except BudgetExhausted:
                self.fallback_state = "budget"
            except Exception as exc:
                self.last_error = f"fallback: {type(exc).__name__}: {exc}"[:300]
            if self.state == "connected":
                break
            await asyncio.sleep(s.TMC_TABLE_FALLBACK_SEC)

    async def on_full(self) -> None:
        if self.fallback_task is not None and not self.fallback_task.done():
            self.fallback_task.cancel()
        self.fallback_task = None
        self.fallback_state = None
        if self.down_alerted_since is not None:
            since = self.down_alerted_since
            await self._emit("chain_sse_recovered", None, None,
                             {"since": since, "reconnects": self.reconnects, "last_block": self.tbl.last_block,
                              "down_seconds": now_ts() - since}, str(since), "info")
        self.down_alerted_since = None
        self.fallback_since = None

    def status(self) -> dict:
        out = super().status()
        out.update(state=self.state, last_event_at=to_iso(self.tbl.last_event_at), last_block=self.tbl.last_block,
                   event_at_source=self.event_at_source, block_source=self.block_source,
                   fallback_state=self.fallback_state, bad_events=self.bad_events, field_nulls=self.field_nulls,
                   ticks_written=self.ticks_written, hourly_rows_last_flush=self.hourly_rows_last_flush)
        return out


def market_price_usd(payload: dict) -> float | None:
    for path in (("current_price",), ("usd_quote", "price"), ("price_usd",), ("price",), ("quote", "USD", "price"),
                 ("tao_price_usd",)):
        v: Any = payload
        for k in path:
            v = v.get(k) if isinstance(v, dict) else None
        if isinstance(v, (int, float)) and v > 0:
            return float(v)
    return None


_ = Path  # (re-exported for tests)
