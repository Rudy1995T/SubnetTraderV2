"""In-service TMC history backfill, job ``backfill_tmc`` (spec 17 §2.4; manual trigger, shared ``tmc`` bucket).

Tasks: ``analytics1y`` → ``chain_daily`` (``analytics_1y``, backfilled=1), ``analytics1m`` → ``chain_hourly``
(``analytics_1m``, once per occupant segment), ``restamp`` (re-parse stored payloads, 0 requests), ``candles5m`` →
``price_bars`` 5m + local 1h/1d aggregates, ``positions`` (Pi coldkey, raw only). Rows whose occupant is unknown are
never written (``unattributed`` → ``partial``); the raw payload is kept in ``chain_rest_snapshots`` for ``restamp``.
"""
from __future__ import annotations

import asyncio
import json
import statistics
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from app.backfill import common
from app.collectors.chain_tmc import TmcClient, write_rest_snapshot
from app.collectors.chain_tmc_rest import (_num, _row_ts, analytics_rows, upsert_daily_from_analytics,
                                           upsert_hourly_from_analytics)
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.ratelimit import BudgetExhausted
from app.util.fs import write_atomic
from app.util.time import in_utc_windows, now_ts, to_iso

DEFAULT_TASKS = ["analytics1y", "analytics1m", "restamp", "candles5m"]
ALL_TASKS = ("analytics1y", "analytics1m", "restamp", "candles5m", "positions")
SPAN = {"analytics1y": "1Y", "analytics1m": "1M"}


class ParseError(ValueError):
    pass


# ── pacing ───────────────────────────────────────────────────────────────────────────────────────────────────
class HourCeiling:
    """Job-local sliding ceiling (≤ ``max_per_hour`` in any trailing hour) + UTC quiet windows (02 §F2)."""

    def __init__(self, max_per_hour: int, quiet_utc: str, clock: Callable[[], float] = time.time,
                 sleep: Callable[[float], Any] = asyncio.sleep) -> None:
        self.max = int(max_per_hour)
        self.quiet = quiet_utc or ""
        self._clock = clock
        self._sleep = sleep
        self._q: deque[float] = deque()

    def quiet_until(self) -> int | None:
        if not self.quiet.strip():
            return None
        inside, end = in_utc_windows(int(self._clock()), self.quiet)
        return end if inside else None

    async def wait_quiet(self) -> None:
        while True:
            end = self.quiet_until()
            if end is None:
                return
            await self._sleep(max(1.0, end - self._clock()))

    async def acquire(self) -> None:
        while True:
            await self.wait_quiet()
            now = self._clock()
            while self._q and self._q[0] <= now - 3600:
                self._q.popleft()
            if len(self._q) < self.max:
                self._q.append(now)
                return
            await self._sleep(max(1.0, self._q[0] + 3600 - now))


# ── candles ──────────────────────────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Candle:
    ts: int
    block: int | None
    open: float | None
    high: float | None
    low: float | None
    close: float
    volume: float | None
    start_volume: float | None
    tao_usd: float | None


def _ts(v: Any) -> int:
    if isinstance(v, (int, float)):
        return int(v / 1000) if v > 1e11 else int(v)
    from app.util.time import parse_ts
    return parse_ts(str(v))


def parse_compact_candles(payload: Any) -> list[Candle]:
    """``[ts, block, o, h, l, c, v, start_volume, tao_usd(, tao_eur)]`` — 9 or 10 elements, else ``ParseError``."""
    rows = payload if isinstance(payload, list) else (
        (payload.get("results") or payload.get("data") or payload.get("candles") or [])
        if isinstance(payload, dict) else [])
    out: list[Candle] = []
    for r in rows:
        if not isinstance(r, (list, tuple)) or len(r) not in (9, 10):
            raise ParseError(f"candle row arity {len(r) if isinstance(r, (list, tuple)) else type(r).__name__}")
        close = _num(r[5])
        if close is None or close <= 0:
            continue
        out.append(Candle(_ts(r[0]), _num(r[1], True), _num(r[2]), _num(r[3]), _num(r[4]), close, _num(r[6]),
                          _num(r[7]), _num(r[8])))
    out.sort(key=lambda c: c.ts)
    return out


def detect_label(candles: list[Candle], samples: list[tuple[int, float]]) -> tuple[str, float | None]:
    """Compare each chain sample with the candle *containing* it under the start / end label hypotheses."""
    by_ts = {c.ts: c.close for c in candles}
    errs = {"start": [], "end": []}
    for ts, px in samples:
        if px <= 0:
            continue
        s0 = (ts // 300) * 300              # start-label: bucket [s0, s0+300) is labelled s0
        if s0 in by_ts:
            errs["start"].append(abs(by_ts[s0] - px) / px)
        e0 = s0 + 300                       # end-label: the same bucket is labelled by its end
        if e0 in by_ts:
            errs["end"].append(abs(by_ts[e0] - px) / px)
    med = {k: statistics.median(v) if v else None for k, v in errs.items()}
    cands = [(v, k) for k, v in med.items() if v is not None]
    if not cands:
        return "start", None
    best_err, best = min(cands)
    if best_err >= 0.005:
        return "start", best_err
    return best, best_err


def aggregate(candles: list[Candle], period: int, min_count: int) -> list[dict]:
    groups: dict[int, list[Candle]] = {}
    for c in candles:
        groups.setdefault((c.ts // period) * period, []).append(c)
    out = []
    for b, cs in sorted(groups.items()):
        if len(cs) < min_count:
            continue
        cs.sort(key=lambda c: c.ts)
        highs = [c.high for c in cs if c.high is not None]
        lows = [c.low for c in cs if c.low is not None]
        vols = [c.volume for c in cs if c.volume is not None]
        out.append({"ts": b, "open": cs[0].open, "high": max(highs) if highs else None,
                    "low": min(lows) if lows else None, "close": cs[-1].close,
                    "volume": sum(vols) if vols else None, "start_volume": cs[0].start_volume,
                    "tao_price_usd": cs[-1].tao_usd, "tmc_block_number": cs[-1].block, "n": len(cs)})
    return out


# ── helpers ──────────────────────────────────────────────────────────────────────────────────────────────────
def _sidecar() -> common.Sidecar:
    return common.Sidecar(common.backfill_dir() / "tmc_progress.json", {"version": 1, "tasks": {}})


def read_request() -> dict:
    p = common.backfill_dir() / "tmc_request.json"
    try:
        req = json.loads(p.read_text())
    except (OSError, ValueError):
        req = {}
    tasks = [t for t in (req.get("tasks") or DEFAULT_TASKS) if t in ALL_TASKS]
    return {"tasks": tasks, "netuids": req.get("netuids"), "coldkey": req.get("coldkey"),
            "redo": bool(req.get("redo")), "requested_at": req.get("requested_at")}


def write_request(tasks: list[str], *, netuids: list[int] | None, coldkey: str | None, redo: bool) -> Path:
    p = common.backfill_dir() / "tmc_request.json"
    write_atomic(p, {"tasks": tasks, "netuids": netuids, "coldkey": coldkey, "redo": redo,
                     "requested_at": now_ts()})
    return p


def _segments(rows: list[dict]) -> list[tuple[str, list[dict]]]:
    segs: list[tuple[str, list[dict]]] = []
    for r in sorted(rows, key=lambda x: _row_ts(x) or 0):
        k = r["occupant_key"]
        if segs and segs[-1][0] == k:
            segs[-1][1].append(r)
        else:
            segs.append((k, [r]))
    return segs


def _annotate(rows: list[dict], netuid: int, occ) -> tuple[list[dict], int]:
    """Attach ``occupant_key`` by the row's block (TMC analytics block ≈ chain ± 1 min); drop unknowns."""
    keep, unknown = [], 0
    for r in rows:
        blk = _num(r.get("block_number"), True)
        key = occ.occupant_at_block(netuid, blk) if blk is not None else None
        if key is None:
            unknown += 1
            continue
        keep.append({**r, "occupant_key": key})
    return keep, unknown


# ── the job ──────────────────────────────────────────────────────────────────────────────────────────────────
class BackfillTmcJob(CollectorJob):
    id = "backfill_tmc"
    phase = 3
    timeout_sec = 14400
    kind = "manual"
    paid = False

    def health_extra(self) -> dict | None:
        sc = _sidecar().data.get("tasks", {})
        prog = {}
        for task, units in sc.items():
            st = [u.get("status") for u in units.values()]
            prog[task] = {"done": st.count("done"), "partial": st.count("partial"), "failed": st.count("failed"),
                          "total": len(st)}
        return {"progress": prog} if prog else None

    async def collect(self, run: RunContext) -> JobOutcome:
        s = run.settings
        db = run.rt.db
        req = read_request()
        tmc = TmcClient(run.rt.http)
        ceiling = HourCeiling(s.BACKFILL_TMC_MAX_PER_HOUR, s.BACKFILL_TMC_QUIET_UTC)
        side = _sidecar()
        deadline = time.monotonic() + self.spec.timeout_sec - 300
        con = await asyncio.to_thread(common.open_db, None)
        try:
            from app.backfill.occupancy import OccupancyIndex
            occ = await asyncio.to_thread(OccupancyIndex.load, con)
            maxn = await db.fetchval("SELECT MAX(netuid) FROM subnets") or 128
            netuids = [int(n) for n in (req["netuids"] or range(1, int(maxn) + 1))]
            rows_total = 0
            pending = 0
            notes: list[str] = []
            t0 = time.monotonic()
            for task in req["tasks"]:
                if task == "positions":
                    if req["coldkey"] or s.PI_TRADER_COLDKEY:
                        await ceiling.acquire()
                        rows_total += await task_positions(run, tmc, req["coldkey"] or s.PI_TRADER_COLDKEY, ceiling)
                    continue
                if task == "candles5m":
                    netuids_c = sorted(netuids, key=lambda n: (n not in (64, 1), n))
                else:
                    netuids_c = netuids
                ctx: dict[str, Any] = {}
                for n in netuids_c:
                    unit = side.unit(task, str(n))
                    if unit.get("status") == "done" and not req["redo"]:
                        continue
                    if task == "restamp" and unit.get("status") is None and not self._restamp_needed(side, n):
                        continue
                    reason = self.paused_reason()
                    if reason:
                        side.save()
                        return JobOutcome(rows=rows_total, note=f"paused:{reason}")
                    if time.monotonic() > deadline:
                        pending += 1
                        continue
                    try:
                        if task in SPAN:
                            await ceiling.acquire()
                            r = await task_analytics(run, tmc, n, task, occ)
                        elif task == "restamp":
                            r = await task_restamp(run, n, occ)
                        else:
                            r = await task_candles5m(run, tmc, n, con, occ, side, ceiling, ctx)
                    except BudgetExhausted:
                        side.save()
                        raise
                    except Exception as exc:
                        r = {"status": "failed", "rows": 0, "last_error": f"{type(exc).__name__}: {str(exc)[:200]}"}
                        logger.warning("backfill_tmc unit failed", data={"task": task, "netuid": n,
                                                                        "error": r["last_error"]})
                    attempts = int(unit.get("attempts", 0)) + (0 if task == "restamp" else 1)
                    side.mark(task, str(n), attempts=attempts, **r)
                    side.save()
                    rows_total += int(r.get("rows") or 0)
                    if r.get("status") != "done":
                        pending += 1
                done = sum(1 for u in side.data.get("tasks", {}).get(task, {}).values() if u.get("status") == "done")
                notes.append(f"{task}:{done}/{len(netuids_c)}")
            side.save()
            note = " ".join(notes)
            if pending and time.monotonic() > deadline:
                return JobOutcome(rows=rows_total, note=f"partial: {pending} units left {note}")
            ev_payload = {"rows": rows_total, "seconds": round(time.monotonic() - t0, 1)}
            if not pending:
                await common.emit_step_event(db, "backfill_done", job="backfill_tmc", era=None, run_id=run.run_id,
                                             **ev_payload)
            return JobOutcome(rows=rows_total, note=(note + (f" pending={pending}" if pending else "")))
        except BudgetExhausted:
            raise
        except Exception as exc:
            try:
                await common.emit_step_event(db, "backfill_failed", job="backfill_tmc", era=None, run_id=run.run_id,
                                             error=f"{type(exc).__name__}: {str(exc)[:300]}",
                                             resume_hint="python -m scripts.backfill_tmc")
            except Exception:  # pragma: no cover
                pass
            raise
        finally:
            con.close()

    @staticmethod
    def _restamp_needed(side: common.Sidecar, n: int) -> bool:
        return any(side.data.get("tasks", {}).get(t, {}).get(str(n), {}).get("status") == "partial"
                   for t in SPAN)


# ── tasks ────────────────────────────────────────────────────────────────────────────────────────────────────
async def _store_analytics(run: RunContext, n: int, task: str, payload: Any, occ, *, observed_at: int) -> dict:
    rows = analytics_rows(payload)
    keep, unknown = _annotate(rows, n, occ)
    db = run.rt.db
    written = 0
    if task == "analytics1y":
        if keep:
            written = await upsert_daily_from_analytics(db, keep, netuid=n, source="analytics_1y", run_id=run.run_id,
                                                        observed_at=observed_at, backfilled=1)
    else:
        for key, seg in _segments(keep):
            written += await upsert_hourly_from_analytics(db, seg, netuid=n, occupant_key=key, source="analytics_1m",
                                                          run_id=run.run_id, observed_at=observed_at, backfilled=1)
    ts = [t for t in (_row_ts(r) for r in rows) if t is not None]
    return {"status": "done" if unknown == 0 else "partial", "rows": written, "unattributed": unknown,
            "points": len(rows), "range": [min(ts), max(ts)] if ts else None, "last_error": None}


async def task_analytics(run: RunContext, tmc: TmcClient, n: int, task: str, occ) -> dict:
    span = SPAN[task]
    resp = await tmc.fetch_analytics(n, span)
    if not resp.ok:
        return {"status": "failed", "rows": 0, "last_error": resp.error or f"HTTP {resp.status}"}
    obs = now_ts()
    await write_rest_snapshot(run.rt.db, "analytics_subnet", resp, run_id=run.run_id, path_args={"n": n},
                              params=f"span={span}", observed_at=obs)
    return await _store_analytics(run, n, task, resp.json, occ, observed_at=obs)


async def task_restamp(run: RunContext, n: int, occ) -> dict:
    """Re-parse the newest stored ``span=1Y``/``1M`` payload (0 requests) → ``done`` when all rows attribute."""
    db = run.rt.db
    total = {"status": "done", "rows": 0, "unattributed": 0}
    for task, span in SPAN.items():
        row = await db.fetchone("SELECT payload_json, observed_at FROM chain_rest_snapshots WHERE endpoint = ? AND "
                                "params = ? ORDER BY observed_at DESC LIMIT 1",
                                (f"/analytics/subnet/{n}/", f"span={span}"))
        if not row or not row["payload_json"]:
            continue
        r = await _store_analytics(run, n, task, json.loads(row["payload_json"]), occ,
                                   observed_at=int(row["observed_at"]))
        total["rows"] += r["rows"]
        total["unattributed"] += r["unattributed"]
        if r["status"] != "done":
            total["status"] = "partial"
    return total


def _gap(con) -> tuple[int, int]:
    from app.backfill.rpc_backfill import ledger_span
    _, hi, _ = ledger_span(con)
    return (hi // 300) * 300 - 3600, (now_ts() // 300) * 300 - 600


async def task_candles5m(run: RunContext, tmc: TmcClient, n: int, con, occ, side: common.Sidecar,
                         ceiling: HourCeiling, ctx: dict) -> dict:
    s = run.settings
    from app.backfill import bars
    gap_start, gap_end = _gap(con)
    side.data["gap_start_ts"], side.data["gap_end_ts"] = gap_start, gap_end
    await ceiling.acquire()
    resp = await tmc.fetch_candles(n, "5m", gap_start, gap_end, timeout=s.BACKFILL_TMC_CANDLE_TIMEOUT_SEC)
    candles: list[Candle] = []
    if resp.ok:
        candles = parse_compact_candles(resp.json)
    else:
        step = s.BACKFILL_TMC_CANDLE_CHUNK_DAYS * 86400
        a = gap_start
        while a < gap_end:
            await ceiling.acquire()
            r = await tmc.fetch_candles(n, "5m", a, min(a + step, gap_end), timeout=s.BACKFILL_TMC_CANDLE_TIMEOUT_SEC)
            if not r.ok:
                return {"status": "failed", "rows": 0, "last_error": r.error or f"HTTP {r.status}"}
            candles.extend(parse_compact_candles(r.json))
            a += step
        candles = sorted({c.ts: c for c in candles}.values(), key=lambda c: c.ts)
    if not candles:
        return {"status": "done", "rows": 0, "range": [gap_start, gap_end], "last_error": None}
    # label detection: SN64, then SN1 (processed first) against chain samples; a stored label is reused on resume
    provisional = None
    if "label" not in ctx and side.data.get("candle_label") in ("start", "end"):
        ctx["label"] = side.data["candle_label"]
    if "label" not in ctx:
        samples = [(int(t), float(p)) for t, p in con.execute(
            "SELECT timestamp, price FROM pool_history WHERE netuid = ? AND timestamp >= ? AND timestamp < ? "
            "ORDER BY timestamp LIMIT 48", (n, candles[0].ts, candles[-1].ts + 300))]
        lab, err = detect_label(candles, samples)
        det = ctx.setdefault("detections", [])
        det.append({"netuid": n, "label": lab, "median_err": err, "samples": len(samples)})
        good = err is not None and err < 0.005
        provisional = lab if good else None
        if good or n not in (64, 1) or len(det) >= 2:
            ctx["label"] = lab if good else "start"
            side.data["candle_label"] = lab if good else "unverified"
            side.data["candle_label_detail"] = det
    label = ctx.get("label") or provisional or "start"
    shift = -300 if label == "end" else 0
    obs = now_ts()
    rows5: list[dict] = []
    unknown = 0
    for c in candles:
        t = c.ts + shift
        key = occ.occupant_at_ts(n, t)
        if key is None:
            unknown += 1
        rows5.append({"netuid": n, "ts": t, "occupant_key": key, "open": c.open, "high": c.high, "low": c.low,
                      "close": c.close, "volume": c.volume, "start_volume": c.start_volume, "tao_price_usd": c.tao_usd,
                      "tmc_block_number": c.block, "source": "tmc_candle", "event_at": t, "observed_at": obs,
                      "ts_last": None, "tao_in_pool": None, "alpha_in_pool": None, "moving_price": None,
                      "block_number": None, "backfilled": 1, "run_id": run.run_id})
    shifted = [Candle(c.ts + shift, c.block, c.open, c.high, c.low, c.close, c.volume, c.start_volume, c.tao_usd)
               for c in candles]
    written = 0

    def _write() -> int:
        w = bars.upsert_bars(con, rows5, "5m")["written"]
        for per, sec, need in (("1h", 3600, s.BACKFILL_TMC_MIN_5M_PER_1H), ("1d", 86400, s.BACKFILL_TMC_MIN_5M_PER_1D)):
            agg = []
            for a in aggregate(shifted, sec, need):
                agg.append({"netuid": n, "ts": a["ts"], "occupant_key": occ.occupant_at_ts(n, a["ts"]),
                            "open": a["open"], "high": a["high"], "low": a["low"], "close": a["close"],
                            "volume": a["volume"], "start_volume": a["start_volume"],
                            "tao_price_usd": a["tao_price_usd"], "tmc_block_number": a["tmc_block_number"],
                            "source": "tmc_candle", "event_at": a["ts"], "observed_at": obs, "ts_last": None,
                            "tao_in_pool": None, "alpha_in_pool": None, "moving_price": None, "block_number": None,
                            "backfilled": 1, "run_id": run.run_id})
            w += bars.upsert_bars(con, agg, per)["written"]
            bars.enrich_reserves(con, n, per, gap_start, gap_end + 86400, now=obs)
        return w
    written = await asyncio.to_thread(_write)
    return {"status": "done", "rows": written, "unattributed": unknown, "range": [candles[0].ts, candles[-1].ts],
            "last_error": None}


RAO = 1_000_000_000


def parse_coldkey_stakes(payload: Any) -> list[dict]:
    """Rows from ``subnets|results|data|items|<root list>``; normalised field names.

    Verified shape (2026-10-13): ``{"nominator_coldkey": ss58, "subnets": [{"subnet", "balance_alpha", "balance_tao",
    "hotkeys": [{"address", "stake", "tao_staked", …}], …}]}`` with integer **rao** amounts (``realized`` etc. too) →
    ``alpha``/``tao_value`` are converted to α/τ; the hotkey is the first ``hotkeys[].address``. Other shapes keep
    their numbers as given (shape UNVERIFIED)."""
    if isinstance(payload, list):
        rows = payload
    else:
        rows = next((payload[k] for k in ("subnets", "results", "data", "items") if isinstance(payload, dict)
                     and isinstance(payload.get(k), list)), [])
    rao = isinstance(payload, dict) and "nominator_coldkey" in payload
    out = []
    for r in rows:
        if not isinstance(r, dict):
            continue

        def pick(*ks: str) -> Any:
            return next((r[k] for k in ks if k in r and r[k] is not None), None)
        hot = pick("hotkey", "hotkey_ss58")
        if hot is None and isinstance(r.get("hotkeys"), list) and r["hotkeys"] and isinstance(r["hotkeys"][0], dict):
            hot = r["hotkeys"][0].get("address")
        alpha = _num(pick("balance_alpha", "alpha", "stake", "balance"))
        tao = _num(pick("balance_tao", "tao_value", "value_tao"))
        if rao:
            alpha = alpha / RAO if alpha is not None else None
            tao = tao / RAO if tao is not None else None
        out.append({"netuid": _num(pick("netuid", "subnet_id", "subnet"), True), "hotkey": hot,
                    "alpha": alpha, "tao_value": tao})
    return out


async def task_positions(run: RunContext, tmc: TmcClient, coldkey: str, ceiling: HourCeiling) -> int:
    db = run.rt.db
    n = 0
    for path in (f"/accounts/coldkey-stakes/{coldkey}/", f"/accounts/coldkey-subnet-chart/{coldkey}/"):
        r = await tmc.get(path)
        if r.ok:
            await db.execute("INSERT OR IGNORE INTO chain_rest_snapshots (endpoint, params, observed_at, run_id, "
                             "http_status, payload_json, event_at, source) VALUES (?,?,?,?,?,?,NULL,'tmc')",
                             (path, "", now_ts(), run.run_id, r.status, json.dumps(r.json, separators=(",", ":"))))
            n += 1
        else:
            logger.info("positions endpoint not ok", data={"path": path.replace(coldkey, "<coldkey>"),
                                                            "status": r.status})
        await ceiling.acquire()
    return n
