"""TMC REST jobs (spec 05 §6.1): ``tmc_hourly`` (trending + market), ``tmc_daily`` (constants, burn/lock cost,
1W analytics → ``chain_daily`` + ``chain_hourly`` gap repair, pressure, trending rollup) and ``tmc_weekly`` (1/7 of
the netuids: holders, 1h candles, 1Y analytics; validators on Mondays).

Every 128-loop runs in ascending netuid with per-netuid isolation; ``BudgetExhausted`` propagates after the netuids
already done are committed (G1). ≥ 25 % netuid failures → ``ChainPartialFailure`` after the commits (§7).
"""
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.collectors.chain_tmc import TmcClient, canonical_params, write_rest_snapshot  # noqa: F401 (re-export, 05 §8)
from app.features.holders import concentration
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.ratelimit import BudgetExhausted
from app.storage.precedence import precedence_upsert_sql
from app.util.time import floor_day, floor_hour, now_ts, parse_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

ANALYTICS_1W_FIELDS = None  # all fields (the 1W payload is ≈ 80 KB per netuid; no `fields=` filter verified)

# analytics → chain_hourly (§3.1): renamed + same-name columns; everything else NULL
_HOURLY_RENAME = {"emission_percent": "emission", "subnet_tao": "tao_liquidity", "chain_buys": "excess_tao",
                  "total_locked": "total_locked_alpha"}
_HOURLY_SAME = ("owner_personal_locked_alpha", "owner_personal_decaying_alpha", "total_tao_buy_24h",
                "total_tao_sell_24h", "volume_24h", "tx_count_24h", "neuron_registrations_24h", "rao_recycled_24h",
                "tao_price_usd")
_DAILY_COLS = ("subnet_tao", "total_supply", "circulation_supply", "emission_percent", "daily_emission",
               "daily_inflation", "total_locked", "owner_personal_locked_alpha", "owner_personal_decaying_alpha",
               "total_decaying", "total_tao_buy", "total_tao_sell", "total_tao_buy_24h", "total_tao_sell_24h",
               "volume_24h", "tx_count_24h", "tx_amount_24h", "neuron_registrations_24h", "rao_recycled_24h",
               "chain_buys", "tao_price_usd", "tao_price_eur")
_DAILY_INT = {"tx_count_24h", "neuron_registrations_24h"}
_ANALYTICS_META = {"ts", "block_number", "timestamp"}


class ChainPartialFailure(RuntimeError):
    def __init__(self, failed: int, total: int) -> None:
        super().__init__(f"{failed}/{total} netuids failed")
        self.failed, self.total = failed, total


def _num(v: Any, as_int: bool = False) -> Any:
    if v is None or isinstance(v, bool):
        return None if v is None else int(v)
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if as_int:
        return int(f) if f == int(f) else None
    return f


def _row_ts(r: dict) -> int | None:
    t = r.get("ts", r.get("timestamp"))
    if t is None:
        return None
    if isinstance(t, (int, float)):
        return int(t / 1000) if t > 1e11 else int(t)
    try:
        return parse_ts(str(t))
    except (ValueError, TypeError):
        return None


def analytics_rows(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for k in ("results", "data"):
            if isinstance(payload.get(k), list):
                return [r for r in payload[k] if isinstance(r, dict)]
    return []


# ── J1 helpers (05 + 17) ──────────────────────────────────────────────────────────────────────────────────────
async def upsert_hourly_from_analytics(db: "Database", rows: list[dict], *, netuid: int, occupant_key: str,
                                       source: str, run_id: str, observed_at: int, backfilled: int = 1) -> int:
    """One occupant segment → ``chain_hourly`` (precedence: only missing ``(netuid, hour_ts)`` land)."""
    out = []
    for r in rows:
        ts = _row_ts(r)
        if ts is None:
            continue
        d: dict[str, Any] = {"netuid": netuid, "hour_ts": floor_hour(ts), "occupant_key": occupant_key,
                             "block_number": _num(r.get("block_number"), True), "event_at": ts,
                             "observed_at": observed_at, "source": source, "run_id": run_id, "backfilled": backfilled}
        for a, c in _HOURLY_RENAME.items():
            d[c] = _num(r.get(a))
        for c in _HOURLY_SAME:
            d[c] = _num(r.get(c), c in _DAILY_INT)
        out.append(d)
    if not out:
        return 0
    sql = precedence_upsert_sql("chain_hourly", list(out[0]), ("netuid", "hour_ts"))
    n = 0
    for d in out:
        res = await db.execute(sql, d)
        n += max(0, res.rowcount or 0)
    return n


async def upsert_daily_from_analytics(db: "Database", rows: list[dict], *, netuid: int, source: str, run_id: str,
                                      observed_at: int, occupant_key: str | None = None,
                                      backfilled: int = 0) -> int:
    """``chain_daily`` rows (J1). 1W hourly points map to ``day_ts`` only from the day's first hour (§3.1)."""
    from app.registry import occupant_at
    by_day: dict[int, dict] = {}
    for r in rows:
        ts = _row_ts(r)
        if ts is None:
            continue
        day = floor_day(ts)
        if source == "analytics_1w" and not (day <= ts < day + 3600):
            continue
        if day not in by_day or ts < _row_ts(by_day[day]):
            by_day[day] = r
    written = skipped = 0
    sql = None
    for day, r in sorted(by_day.items()):
        key = r.get("occupant_key") or occupant_key
        blk = _num(r.get("block_number"), True)
        if key is None and blk is not None:
            occ = await occupant_at(db, netuid, blk)
            key = occ.occupant_key if occ else None
        if key is None:
            skipped += 1
            continue
        d: dict[str, Any] = {"netuid": netuid, "day_ts": day, "occupant_key": key, "block_number": blk,
                             "event_at": _row_ts(r), "observed_at": observed_at, "source": source, "run_id": run_id,
                             "backfilled": backfilled}
        for c in _DAILY_COLS:
            d[c] = _num(r.get(c), c in _DAILY_INT)
        extras = {k: v for k, v in r.items() if k not in _DAILY_COLS and k not in _ANALYTICS_META
                  and k != "occupant_key"}
        d["extras_json"] = json.dumps(extras, separators=(",", ":")) if extras else None
        sql = sql or precedence_upsert_sql("chain_daily", list(d), ("netuid", "day_ts"), keep=("occupant_key",))
        res = await db.execute(sql, d)
        written += max(0, res.rowcount or 0)
    if skipped:
        logger.info("chain_daily rows without an occupant skipped", data={"netuid": netuid, "skipped": skipped})
    return written


async def write_trending_daily(db: "Database", day_ts: int, *, run_id: str) -> int:
    """Roll one UTC day's last successful ``chain_trending`` pull into ``chain_trending_daily`` (§3.1)."""
    pulls = await db.fetchall("SELECT DISTINCT observed_at FROM chain_trending WHERE observed_at >= ? AND "
                              "observed_at < ? ORDER BY observed_at", (day_ts, day_ts + 86400), writer=True)
    if not pulls:
        return 0
    last = pulls[-1]["observed_at"]
    listed = {r["netuid"]: r for r in await db.fetchall(
        "SELECT netuid, views, visitors FROM chain_trending WHERE observed_at = ?", (last,), writer=True)}
    occs = await db.fetchall(
        "SELECT occupant_key, netuid FROM occupants WHERE netuid != 0 AND (status IN ('active','pre_launch') OR "
        "(deregistered_at IS NOT NULL AND deregistered_at > ?)) AND first_seen_at < ?",
        (day_ts + 86400, day_ts + 86400), writer=True)
    rows = []
    for o in occs:
        t = listed.get(o["netuid"])
        rows.append((day_ts, o["occupant_key"], o["netuid"], int(t["views"] or 0) if t else 0,
                     t["visitors"] if t else None, len(pulls), last, run_id))
    await db.executemany("INSERT OR IGNORE INTO chain_trending_daily (day_ts, occupant_key, netuid, views, visitors, "
                         "n_pulls_ok, observed_at, run_id) VALUES (?,?,?,?,?,?,?,?)", rows)
    return len(rows)


# ── shared bits ───────────────────────────────────────────────────────────────────────────────────────────────
async def _budget_event(run: RunContext, exc: BudgetExhausted) -> None:
    if exc.window != "month":
        return
    from app.storage.events import emit_event
    from app.util.time import to_iso
    now = now_ts()
    lim = run.rt.http.limiter if run.rt.http is not None else None
    used = lim.month_requests("tmc") if lim is not None else None
    try:
        await emit_event(run.rt.db, event_type="tmc_budget_degraded", occupant_key=None, netuid=None,
                         event_at=now, observed_at=now, payload={"requests_month": used,
                                                                 "cap": run.settings.TMC_MONTHLY_CAP},
                         dedupe_key=(to_iso(now) or "")[:7], source="chain", run_id=run.run_id)
    except Exception as exc2:  # pragma: no cover
        logger.warning("tmc_budget_degraded write failed", data={"error": str(exc2)})


async def _netuid_occupants(db: "Database") -> list[dict]:
    return await db.fetchall("SELECT o.netuid, o.occupant_key, o.network_registered_at FROM subnets s "
                             "JOIN occupants o USING (occupant_key) WHERE o.status IN ('active','pre_launch') "
                             "ORDER BY o.netuid", writer=True)


class _TmcJob(CollectorJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        self.tmc = TmcClient(run.rt.http)
        try:
            return await self._collect(run)
        except BudgetExhausted as exc:
            await _budget_event(run, exc)
            raise

    async def _collect(self, run: RunContext) -> JobOutcome:  # pragma: no cover - abstract
        raise NotImplementedError


# ── tmc_hourly ─────────────────────────────────────────────────────────────────────────────────────────────────
class TmcHourlyJob(_TmcJob):
    async def _collect(self, run: RunContext) -> JobOutcome:
        s = run.settings
        db = run.rt.db
        now = run.started_at
        notes, rows = [], 0
        if s.TMC_TRENDING_ENABLED:
            r = await self.tmc.fetch_trending(50)
            data = r.json.get("data") if r.ok and isinstance(r.json, dict) else (r.json if r.ok else None)
            if isinstance(data, list) and data:
                current = {o["netuid"]: o["occupant_key"] for o in await _netuid_occupants(db)}
                out, unknown = [], 0
                for pos, e in enumerate(data, 1):
                    try:
                        n = int(e.get("entity_id"))
                    except (TypeError, ValueError):
                        unknown += 1
                        continue
                    if n not in current:
                        unknown += 1
                        continue
                    out.append((now, n, current[n], pos, _num(e.get("views"), True), _num(e.get("visitors"), True),
                                _num(e.get("percentage_change")), now, "tmc_trending", run.run_id))
                await db.executemany("INSERT OR IGNORE INTO chain_trending (observed_at, netuid, occupant_key, rank, "
                                     "views, visitors, percentage_change, event_at, source, run_id) "
                                     "VALUES (?,?,?,?,?,?,?,?,?,?)", out)
                rows += len(out)
                notes.append(f"trending={len(out)}" + (f" unknown={unknown}" if unknown else ""))
            else:
                notes.append(f"trending failed ({r.error or 'empty'})")
        m = await self.tmc.fetch_market() if s.TMC_MARKET_ENABLED else None
        if m is None:
            pass
        elif m.ok:
            await write_rest_snapshot(db, "market_data", m, run_id=run.run_id, observed_at=now)
            rows += 1
            notes.append("market ok")
        else:
            notes.append(f"market failed ({m.error})")
        return JobOutcome(rows=rows, note=" ".join(notes))


# ── tmc_daily ──────────────────────────────────────────────────────────────────────────────────────────────────
def _flatten_constants(payload: Any) -> dict[str, str]:
    mod = payload.get("subtensor_module") if isinstance(payload, dict) else None
    if not isinstance(mod, dict):
        return {}
    return {k: (v if isinstance(v, str) else json.dumps(v)) for k, v in mod.items() if v is not None}


class TmcDailyJob(_TmcJob):
    async def _collect(self, run: RunContext) -> JobOutcome:
        s = run.settings
        db = run.rt.db
        now = run.started_at
        notes: list[str] = []
        # 1. constants (runtime defaults only; never read NetworkImmunityPeriod from here, §2.4)
        c = await self.tmc.get("/general/constants/", {"type": "subtensor_module"})
        if c.ok:
            consts = _flatten_constants(c.json)
            blk = _num(c.json.get("block_number"), True) if isinstance(c.json, dict) else None
            await db.executemany("INSERT OR IGNORE INTO chain_constants (name, source, observed_at, value, block_number,"
                                 " endpoint, event_at, run_id) VALUES (?, 'tmc_constants', ?, ?, ?, NULL, NULL, ?)",
                                 [(k, now, v, blk, run.run_id) for k, v in consts.items()])
            notes.append(f"constants={len(consts)}")
        # 2. burn / network lock cost snapshots
        for kind, path in (("burn", "/subnets/burn/"), ("network_lock_cost", "/subnets/network-lock-cost/")):
            r = await self.tmc.get(path)
            if r.ok:
                await write_rest_snapshot(db, kind, r, run_id=run.run_id, observed_at=now)
        # 3. 1W analytics per occupant: chain_daily rows + chain_hourly gap repair
        occs = await _netuid_occupants(db)
        ok = failed = hourly = daily = 0
        for o in occs:
            n = o["netuid"]
            if n == 0:
                continue
            try:
                r = await self.tmc.fetch_analytics(n, "1W", ANALYTICS_1W_FIELDS)
                if not r.ok:
                    failed += 1
                    continue
                rows = [x for x in analytics_rows(r.json)
                        if (_num(x.get("block_number"), True) or 0) >= o["network_registered_at"]]
                hourly += await upsert_hourly_from_analytics(db, rows, netuid=n, occupant_key=o["occupant_key"],
                                                             source="analytics_1w", run_id=run.run_id,
                                                             observed_at=now, backfilled=1)
                daily += await upsert_daily_from_analytics(db, rows, netuid=n, source="analytics_1w",
                                                           run_id=run.run_id, observed_at=now,
                                                           occupant_key=o["occupant_key"], backfilled=0)
                ok += 1
            except BudgetExhausted:
                raise
            except Exception as exc:
                failed += 1
                logger.warning("tmc_daily analytics failed", data={"netuid": n, "error": str(exc)})
        notes.append(f"analytics ok={ok} failed={failed} hourly_repaired={hourly} daily={daily}")
        # 4. pressure (network-wide, last 7 closed UTC days)
        if s.TMC_PRESSURE_ENABLED:
            end = floor_day(now)
            np_ = 0
            for side in ("buy", "sell"):
                r = await self.tmc.fetch_pressure(side, end - 7 * 86400, end - 1)
                pts = r.json if r.ok and isinstance(r.json, list) else (
                    r.json.get("results") if r.ok and isinstance(r.json, dict) else None)
                if not isinstance(pts, list):
                    continue
                vals = []
                for p in pts:
                    ts = _row_ts(p)
                    if ts is None:
                        continue
                    vals.append((floor_day(ts), side, -1, _num(p.get("value")), _num(p.get("ema_price")), ts, now,
                                 run.run_id))
                await db.executemany("INSERT OR IGNORE INTO chain_pressure (day_ts, side, netuid, value_rao, "
                                     "ema_price, event_at, observed_at, source, run_id) "
                                     "VALUES (?,?,?,?,?,?,?,'tmc_pressure',?)", vals)
                np_ += len(vals)
            notes.append(f"pressure={np_}")
        # 5. trending rollup: yesterday + any of the last 7 days still missing
        today = floor_day(now)
        tr = 0
        for d in range(today - 7 * 86400, today, 86400):
            have = await db.fetchval("SELECT 1 FROM chain_trending_daily WHERE day_ts = ? LIMIT 1", (d,), writer=True)
            if not have:
                tr += await write_trending_daily(db, d, run_id=run.run_id)
        notes.append(f"trending_daily={tr}")
        total = ok + failed
        if total and failed / total >= 0.25:
            raise ChainPartialFailure(failed, total)
        return JobOutcome(rows=hourly + daily + tr, note=" ".join(notes),
                          meta={"analytics_ok": ok, "analytics_failed": failed})


# ── tmc_weekly ─────────────────────────────────────────────────────────────────────────────────────────────────
def candle_bars(payload: Any, netuid: int, occupant_for_ts, *, run_id: str, observed_at: int) -> list[dict]:
    """Compact candles ``[ts, tmc_block, o, h, l, c, volume, start_volume, tao_price_usd, …]`` → 1h bars."""
    rows = payload if isinstance(payload, list) else (payload.get("results") or payload.get("data") or []) \
        if isinstance(payload, dict) else []
    out = []
    for c in rows:
        if isinstance(c, dict):
            vals = [c.get(k) for k in ("timestamp", "block_number", "open", "high", "low", "close", "volume",
                                       "start_volume", "tao_price_usd")]
        elif isinstance(c, list) and len(c) >= 6:
            vals = (list(c) + [None] * 9)[:9]
        else:
            continue
        ts = _num(vals[0], True)
        close = _num(vals[5])
        if ts is None or close is None or close <= 0:
            continue
        ts = ts // 1000 if ts > 10**11 else ts
        out.append({"netuid": netuid, "period": "1h", "ts": floor_hour(ts), "occupant_key": occupant_for_ts(ts),
                    "open": _num(vals[2]), "high": _num(vals[3]), "low": _num(vals[4]), "close": close,
                    "volume": _num(vals[6]), "start_volume": _num(vals[7]), "tao_price_usd": _num(vals[8]),
                    "ts_last": None, "tmc_block_number": _num(vals[1], True), "block_number": None,
                    "source": "tmc_candle", "event_at": floor_hour(ts), "observed_at": observed_at, "backfilled": 0,
                    "run_id": run_id})
    return out


class TmcWeeklyJob(_TmcJob):
    async def _collect(self, run: RunContext) -> JobOutcome:
        from app.util.time import parse_dt
        s = run.settings
        db = run.rt.db
        now = run.started_at
        wd = parse_dt(now).weekday()
        occs = [o for o in await _netuid_occupants(db) if o["netuid"] != 0 and o["netuid"] % 7 == wd]
        reg_ts = {r["occupant_key"]: r["registered_ts"] or r["registered_at_est"] for r in await db.fetchall(
            "SELECT occupant_key, registered_ts, registered_at_est FROM occupants", writer=True)}
        ok = failed = 0
        counts = {"holders": 0, "candles": 0, "daily": 0}
        for o in occs:
            n, key = o["netuid"], o["occupant_key"]
            try:
                if s.TMC_HOLDERS_ENABLED:
                    r = await self.tmc.fetch_holders(n)
                    if r.ok and isinstance(r.json, dict):
                        res = r.json.get("results") or []
                        st = concentration(res, _num(r.json.get("count"), True))
                        slim = [{k: x.get(k) for k in ("rank", "coldkey", "percentage", "total_alpha")} for x in res]
                        await db.execute(
                            "INSERT OR IGNORE INTO holders_snapshots (netuid, observed_at, occupant_key, run_id, "
                            "source, event_at, holders_count, n_fetched, top1_share, top10_share, hhi_topn, gini_topn,"
                            " rows_json) VALUES (?,?,?,?,'tmc_holders',NULL,?,?,?,?,?,?,?)",
                            (n, now, key, run.run_id, st.holders_count, st.n_fetched, st.top1_share, st.top10_share,
                             st.hhi_topn, st.gini_topn, json.dumps(slim, separators=(",", ":"))))
                        counts["holders"] += 1
                rt = reg_ts.get(key)
                c = await self.tmc.fetch_candles(n, "1h", now - 7 * 86400, now)
                if c.ok:
                    bars = candle_bars(c.json, n, lambda ts, k=key, t0=rt: k if (t0 is None or ts >= t0) else None,
                                       run_id=run.run_id, observed_at=now)
                    if bars:
                        sql = precedence_upsert_sql("price_bars", list(bars[0]), ("netuid", "period", "ts"))
                        await db.executemany(sql, bars)
                        counts["candles"] += len(bars)
                a = await self.tmc.fetch_analytics(n, "1Y")
                if a.ok:
                    rows = [x for x in analytics_rows(a.json)
                            if (_num(x.get("block_number"), True) or 0) >= o["network_registered_at"]]
                    counts["daily"] += await upsert_daily_from_analytics(
                        db, rows, netuid=n, source="analytics_1y", run_id=run.run_id, observed_at=now,
                        occupant_key=key, backfilled=0)
                if s.TMC_NEURONS_ENABLED:
                    nr = await self.tmc.get(f"/subnets/neurons/{n}/")
                    if nr.ok:
                        await write_rest_snapshot(db, "neurons", nr, run_id=run.run_id, path_args={"n": n},
                                                  observed_at=now)
                ok += 1
            except BudgetExhausted:
                raise
            except Exception as exc:
                failed += 1
                logger.warning("tmc_weekly netuid failed", data={"netuid": n, "error": str(exc)})
        if wd == 0:
            v = await self.tmc.get("/subnets/validators/")
            if v.ok:
                await write_rest_snapshot(db, "validators", v, run_id=run.run_id, observed_at=now)
        total = ok + failed
        if total and failed / total >= 0.25:
            raise ChainPartialFailure(failed, total)
        return JobOutcome(rows=sum(counts.values()),
                          note=f"slice={wd} netuids={total} failed={failed} " +
                               " ".join(f"{k}={v}" for k, v in counts.items()))
