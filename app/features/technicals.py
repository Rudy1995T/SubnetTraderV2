"""Workbench technicals (spec 19 §2.2–§2.3): EMA72, the dispersion band around it, band position, distance, drawdown, realised
vol and trend age as weight-0 evidence ``metric_values`` rows (``source='chain'``, 02 §K3/§K4).

Pure functions (``ema``, ``series_stats``, ``overlay``) are shared with the chart route and the study, so the plot,
the stored rows and the study use one definition. Only complete bars are read (``ts + period ≤ as_of``).

CLI: ``python -m app.features.technicals run [--as-of YYYY-MM-DDTHH]``.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Sequence

import app.config as app_config
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.storage.pit import put_metric_values
from app.util.mathutil import _average_ranks
from app.util.time import floor_day, floor_hour, now_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

SPEC_VERSION = "ts-1.0"
DEF = "wb-1"
PERIOD_S = {"1h": 3600, "1d": 86400}
KEYS_1H = ("ema72_1h", "ema72_dist_1h", "ema72_band_1h", "ema72_band_pos_1h", "px_vs_moving")
KEYS_1D = ("ema72_1d", "ema72_dist_1d", "ema72_band_1d", "ema72_band_pos_1d", "ema72_band_rank_1d", "rv_30d",
           "dd_90d", "ema72_cross_days_1d")
CATCHUP_DAYS = 7
MAX_GAP_SHARE = 0.10
MIN_DEVS = 24
WARMUP_PERIOD = 72


# ── pure math ──────────────────────────────────────────────────────────────────────────────────────────────────
def ema(closes: Sequence[float], period: int) -> list[float | None]:
    """EMA with α = 2/(period+1), seeded by the SMA of the first ``period`` values; None before the seed."""
    out: list[float | None] = [None] * len(closes)
    if len(closes) < period:
        return out
    a = 2.0 / (period + 1)
    e = sum(closes[:period]) / period
    out[period - 1] = e
    for i in range(period, len(closes)):
        e = a * closes[i] + (1 - a) * e
        out[i] = e
    return out


def _std(xs: Sequence[float]) -> float | None:
    n = len(xs)
    if n < 2:
        return None
    m = sum(xs) / n
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))


def dev_sigma(closes: Sequence[float], e: Sequence[float | None], window: int, end: int | None = None) -> float | None:
    """Band width σ: RMS of ln(close/EMA) over the last ``window`` bars ending at ``end`` (Bollinger-style dispersion
    of the price around its own EMA, 19 §2.3); None with fewer than ``MIN_DEVS`` deviations."""
    end = len(closes) if end is None else end
    devs = [math.log(c / ev) for c, ev in zip(closes[max(0, end - window):end], e[max(0, end - window):end])
            if ev is not None and ev > 0 and c > 0]
    if len(devs) < MIN_DEVS:
        return None
    return math.sqrt(sum(d * d for d in devs) / len(devs))


def log_rets(closes: Sequence[float]) -> list[float]:
    return [math.log(b / a) for a, b in zip(closes, closes[1:]) if a > 0 and b > 0]


def clip(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def pct_rank(values: dict[Any, float]) -> dict[Any, float]:
    """Cross-sectional percentile rank (r−1)/(n−1) with average ties; n = 1 → 0.5."""
    keys = list(values)
    if not keys:
        return {}
    if len(keys) == 1:
        return {keys[0]: 0.5}
    ranks = _average_ranks([values[k] for k in keys])
    n = len(keys)
    return {k: (r - 1) / (n - 1) for k, r in zip(keys, ranks)}


@dataclass
class SeriesStats:
    status: str                       # ok | failed
    reason: str | None = None
    n: int = 0
    gaps: int = 0
    stale_bars: int = 0
    bar_ts: int | None = None
    event_at: int | None = None
    close: float | None = None
    ema: float | None = None
    sigma: float | None = None
    k: float = 2.0
    band_pos_raw: float | None = None
    moving_price: float | None = None
    cross_days: float | None = None
    cross_side: int | None = None
    cross_censored: bool = False
    rv_30d: float | None = None
    rv_n: int = 0
    dd_90d: float | None = None
    dd_n: int = 0
    extra: dict = field(default_factory=dict)

    @property
    def dist(self) -> float | None:
        return None if self.ema in (None, 0) or self.close is None else self.close / self.ema - 1

    @property
    def band(self) -> float | None:
        return None if self.sigma is None else self.k * self.sigma

    @property
    def band_pos(self) -> float | None:
        return None if self.band_pos_raw is None else clip(self.band_pos_raw, -2.0, 2.0)


def series_stats(bars: Sequence[tuple[int, float, float | None]], *, period: str, expected_last_ts: int,
                 ema_period: int, k: float, min_bars: int, stale_tol: int) -> SeriesStats:
    """``bars`` = complete bars ``(ts, close, moving_price)`` ascending, ``ts ≤ expected_last_ts`` (19 §2.2)."""
    ps = PERIOD_S[period]
    bars = [b for b in bars if b[1] is not None and b[1] > 0]
    st = SeriesStats("failed", n=len(bars), k=k)
    if not bars:
        st.reason = "insufficient_bars"
        return st
    last_ts = bars[-1][0]
    st.bar_ts, st.event_at = last_ts, last_ts + ps
    st.stale_bars = max(0, (expected_last_ts - last_ts) // ps)
    span = (last_ts - bars[0][0]) // ps + 1
    st.gaps = max(0, span - len(bars))
    if len(bars) < min_bars:
        st.reason = "insufficient_bars"
        return st
    if st.stale_bars > stale_tol:
        st.reason = "stale_bars"
        return st
    if span > 0 and st.gaps / span > MAX_GAP_SHARE:
        st.reason = "window_gaps"
        return st
    closes = [b[1] for b in bars]
    e = ema(closes, ema_period) if len(closes) >= ema_period else ema(closes, len(closes))
    st.close, st.ema, st.moving_price = closes[-1], e[-1], bars[-1][2]
    st.sigma = dev_sigma(closes, e, ema_period)
    if st.sigma is None:
        st.reason = "insufficient_bars"
        return st
    if st.sigma <= 0:
        st.reason = "zero_vol"
        return st
    st.band_pos_raw = math.log(st.close / st.ema) / (k * st.sigma)
    if period == "1d":
        signs = [(i, 1 if c >= ev else -1) for i, (c, ev) in enumerate(zip(closes, e)) if ev is not None]
        side = signs[-1][1]
        j = next((idx for idx, sg in reversed(signs) if sg != side), None)
        if j is None:
            st.cross_days, st.cross_censored = (last_ts - bars[signs[0][0]][0]) / 86400, True
        else:
            st.cross_days = (last_ts - bars[j + 1][0]) / 86400
        st.cross_side = side
        r30 = log_rets(closes[-31:])
        st.rv_n = len(r30)
        sd = _std(r30)
        st.rv_30d = sd * math.sqrt(365) if sd is not None and len(r30) >= 20 else None
        w90 = closes[-90:]
        st.dd_n = len(w90)
        st.dd_90d = closes[-1] / max(w90) - 1 if len(w90) >= 14 else None
    st.status, st.reason = "ok", None
    return st


def overlay(bars: Sequence[tuple[int, float]], *, ema_period: int, k: float) -> list[dict]:
    """Per-bar EMA and band edges for the chart (same math as the stored rows: rolling ``dev_sigma``)."""
    closes = [c for _, c in bars]
    e = ema(closes, ema_period)
    out = []
    for i, (ts, c) in enumerate(bars):
        sd = dev_sigma(closes, e, ema_period, end=i + 1)
        ev = e[i]
        up = lo = None
        if ev is not None and sd:
            up, lo = ev * math.exp(k * sd), ev * math.exp(-k * sd)
        out.append({"ts": ts, "ema": ev, "upper": up, "lower": lo})
    return out


# ── DB I/O ─────────────────────────────────────────────────────────────────────────────────────────────────────
async def occupants_for(db: "Database", as_of: int) -> list[dict]:
    return await db.fetchall(
        "SELECT occupant_key, netuid, status FROM occupants WHERE netuid != 0 AND status IN ('active','pre_launch') "
        "AND first_seen_at <= ? ORDER BY netuid", (as_of,), writer=True)


async def load_bars(db: "Database", occupant_key: str, period: str, last_ts: int, n: int) -> list[tuple]:
    rows = await db.fetchall(
        "SELECT ts, close, moving_price FROM price_bars WHERE occupant_key = ? AND period = ? AND ts <= ? "
        "ORDER BY ts DESC LIMIT ?", (occupant_key, period, last_ts, n), writer=True)
    return [(r["ts"], r["close"], r["moving_price"]) for r in reversed(rows)]


def _row(occ: str, key: str, as_of: int, value: float | None, vj: dict, *, run_id: str, observed_at: int,
         backfilled: int) -> dict:
    vj = {**vj, "def": DEF}
    if vj.get("status") != "ok":
        value = None
    if backfilled:
        vj["backfilled"] = True
    return {"occupant_key": occ, "metric_key": key, "as_of": as_of, "spec_version": SPEC_VERSION, "stage": "raw",
            "value": value, "value_json": vj, "observed_at": observed_at, "source": "chain", "run_id": run_id,
            "backfilled": backfilled}


def _base(st: SeriesStats, period: str) -> dict:
    d: dict[str, Any] = {"status": st.status, "period": period, "n": st.n, "gaps": st.gaps, "bar_ts": st.bar_ts}
    if st.status == "ok" and st.n < 2 * WARMUP_PERIOD:
        d["warmup"] = True                   # EMA still seed-dominated (< 2 × period bars, 19 §2.2)
    if st.stale_bars:
        d["stale_bars"] = st.stale_bars
    if st.reason:
        d["reason"] = st.reason
    return d


def rows_for(occ: str, period: str, st: SeriesStats, *, as_of: int, run_id: str, observed_at: int,
             backfilled: int = 0) -> list[dict]:
    b = _base(st, period)
    kw = dict(run_id=run_id, observed_at=observed_at, backfilled=backfilled)
    sfx = "_" + period
    out = [
        _row(occ, "ema72" + sfx, as_of, st.ema, {**b, "close": st.close}, **kw),
        _row(occ, "ema72_dist" + sfx, as_of, st.dist, {**b, "ema": st.ema}, **kw),
        _row(occ, "ema72_band" + sfx, as_of, st.band,
             {**b, "mode": "sigma", "k": st.k, "sigma": st.sigma,
              "upper": st.ema * math.exp(st.band) if st.band is not None and st.ema else None,
              "lower": st.ema * math.exp(-st.band) if st.band is not None and st.ema else None}, **kw),
        _row(occ, "ema72_band_pos" + sfx, as_of, st.band_pos, {**b, "raw": st.band_pos_raw}, **kw),
    ]
    if period == "1h":
        if st.status == "ok" and st.moving_price and st.moving_price > 0:
            out.append(_row(occ, "px_vs_moving", as_of, math.log(st.close / st.moving_price),
                            {**b, "moving_price": st.moving_price}, **kw))
        else:
            out.append(_row(occ, "px_vs_moving", as_of, None,
                            b if st.status != "ok" else {**b, "status": "failed", "reason": "moving_price_null"}, **kw))
    else:
        def opt(key: str, v: float | None, need: int, have: int, **x: Any) -> None:
            if st.status == "ok" and v is not None:
                out.append(_row(occ, key, as_of, v, {**b, **x}, **kw))
            elif st.status == "ok":
                out.append(_row(occ, key, as_of, None, {**b, "status": "failed", "reason": "insufficient_bars",
                                                        "required": need, "have": have}, **kw))
            else:
                out.append(_row(occ, key, as_of, None, b, **kw))
        opt("rv_30d", st.rv_30d, 20, st.rv_n)
        opt("dd_90d", st.dd_90d, 14, st.dd_n)
        opt("ema72_cross_days_1d", st.cross_days, 2, st.n, side=st.cross_side, censored=st.cross_censored)
    return out


def not_applicable_rows(occ: str, keys: Sequence[str], as_of: int, **kw: Any) -> list[dict]:
    return [_row(occ, k, as_of, None, {"status": "not_applicable", "reason": "pre_launch"}, **kw) for k in keys]


# ── slots ──────────────────────────────────────────────────────────────────────────────────────────────────────
@dataclass
class SlotResult:
    rows: int = 0
    ok: int = 0
    failed: int = 0
    events: int = 0


async def run_period(db: "Database", period: str, as_of: int, *, run_id: str, observed_at: int,
                     backfilled: int = 0) -> SlotResult:
    s = app_config.settings
    ps = PERIOD_S[period]
    expected_last = as_of - ps
    tol = s.WB_STALE_BARS_1H if period == "1h" else 1
    res = SlotResult()
    rows: list[dict] = []
    band_ok: dict[str, float] = {}
    netuid_of: dict[str, int] = {}
    for o in await occupants_for(db, as_of):
        occ = o["occupant_key"]
        netuid_of[occ] = o["netuid"]
        keys = KEYS_1H if period == "1h" else KEYS_1D
        kw = dict(run_id=run_id, observed_at=observed_at, backfilled=backfilled)
        if o["status"] == "pre_launch":
            rows += not_applicable_rows(occ, keys, as_of, **kw)
            continue
        bars = await load_bars(db, occ, period, expected_last, s.WB_EMA_WINDOW_BARS)
        st = series_stats(bars, period=period, expected_last_ts=expected_last, ema_period=s.WB_EMA_PERIOD,
                          k=s.WB_EMA_BAND_K, min_bars=s.WB_EMA_MIN_BARS, stale_tol=tol)
        rows += rows_for(occ, period, st, as_of=as_of, **kw)
        if st.status == "ok":
            res.ok += 1
            if period == "1d" and st.band_pos is not None:
                band_ok[occ] = st.band_pos
        else:
            res.failed += 1
    if period == "1d":
        ranks = pct_rank(band_ok)
        for occ in netuid_of:
            if occ in ranks:
                rows.append(_row(occ, "ema72_band_rank_1d", as_of, ranks[occ],
                                 {"status": "ok", "n": len(ranks)}, run_id=run_id, observed_at=observed_at,
                                 backfilled=backfilled))
            elif not any(r["occupant_key"] == occ and r["metric_key"] == "ema72_band_rank_1d" for r in rows):
                prev = next((r for r in rows if r["occupant_key"] == occ and r["metric_key"] == "ema72_band_pos_1d"),
                            None)
                vj = dict(prev["value_json"]) if prev else {"status": "failed", "reason": "insufficient_bars"}
                vj.pop("raw", None)
                rows.append(_row(occ, "ema72_band_rank_1d", as_of, None, vj, run_id=run_id,
                                 observed_at=observed_at, backfilled=backfilled))
        if app_config.settings.WB_EVENTS_ENABLED and not backfilled:
            res.events = await band_exit_events(db, band_ok, netuid_of, as_of=as_of, run_id=run_id,
                                                observed_at=observed_at)
    res.rows = await put_metric_values(db, rows)
    return res


async def band_exit_events(db: "Database", band: dict[str, float], netuid_of: dict[str, int], *, as_of: int,
                           run_id: str, observed_at: int) -> int:
    from app.storage.events import emit_event
    n = 0
    prev = {r["occupant_key"]: r["value"] for r in await db.fetchall(
        "SELECT occupant_key, value FROM metric_values WHERE metric_key = 'ema72_band_pos_1d' AND as_of = ? "
        "AND stage = 'raw' AND status = 'ok'", (as_of - 86400,), writer=True)}
    for occ, bp in band.items():
        p = prev.get(occ)
        if p is not None and abs(p) <= 1.0 < abs(bp):
            eid = await emit_event(db, event_type="ema72_band_exit", occupant_key=occ, netuid=netuid_of[occ],
                                   event_at=as_of, observed_at=observed_at,
                                   payload={"netuid": netuid_of[occ], "band_pos": bp, "prev_band_pos": p,
                                            "side": 1 if bp > 0 else -1},
                                   dedupe_key=f"{occ}|{as_of // 86400}", source="chain", run_id=run_id)
            n += eid is not None
    return n


async def has_daily(db: "Database", as_of: int) -> bool:
    return bool(await db.fetchval("SELECT 1 FROM metric_values WHERE metric_key = 'ema72_1d' AND as_of = ? AND "
                                  "stage = 'raw' LIMIT 1", (as_of,), writer=True))


async def run_all(db: "Database", now: int, *, run_id: str) -> dict:
    s = app_config.settings
    out: dict[str, Any] = {}
    if "1h" in s.WB_EMA_TIMEFRAMES:
        h = floor_hour(now)
        r = await run_period(db, "1h", h, run_id=run_id, observed_at=now)
        out["1h"] = {"as_of": to_iso(h), "rows": r.rows, "ok": r.ok, "failed": r.failed}
    if "1d" in s.WB_EMA_TIMEFRAMES:
        d0 = floor_day(now)
        done = []
        for i in range(CATCHUP_DAYS, -1, -1):
            d = d0 - i * 86400
            if await has_daily(db, d):
                continue
            r = await run_period(db, "1d", d, run_id=run_id, observed_at=now, backfilled=1 if i else 0)
            done.append({"as_of": to_iso(d), "rows": r.rows, "ok": r.ok, "failed": r.failed, "events": r.events})
        out["1d"] = done
    return out


class TechnicalsJob(CollectorJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        rep = await run_all(run.rt.db, run.started_at, run_id=run.run_id)
        rows = rep.get("1h", {}).get("rows", 0) + sum(d["rows"] for d in rep.get("1d", []))
        h = rep.get("1h", {})
        note = f"1h as_of={h.get('as_of')} ok={h.get('ok')} failed={h.get('failed')}"
        if rep.get("1d"):
            last = rep["1d"][-1]
            note += f" · 1d slots={len(rep['1d'])} last={last['as_of']} ok={last['ok']} failed={last['failed']}"
        logger.info("wb_technicals", data=rep)
        return JobOutcome(rows=rows, note=note, meta=rep)


# ── CLI ────────────────────────────────────────────────────────────────────────────────────────────────────────
async def _cli(as_of: str | None) -> int:
    from app.storage.db import Database
    from app.util.time import parse_ts
    s = app_config.settings
    db = Database(s.path(s.DB_PATH))
    await db.connect()
    await db.apply_schema()
    try:
        now = parse_ts(as_of + ":00:00Z" if as_of and len(as_of) == 13 else as_of) if as_of else now_ts()
        rep = await run_all(db, now, run_id=f"cli:{now_ts()}")
        print(rep)
        return 0
    finally:
        await db.close()


if __name__ == "__main__":
    import argparse
    import asyncio
    import sys
    p = argparse.ArgumentParser(prog="python -m app.features.technicals")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--as-of", default=None)
    a = p.parse_args()
    sys.exit(asyncio.run(_cli(a.as_of)))
