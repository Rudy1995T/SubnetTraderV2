"""19 §2.2–§2.3 technicals: EMA math, band, windows, gaps, staleness, running bars, occupant isolation, ranks."""
import json
import math

import pytest

from app.features import technicals as T
from tests.wb.conftest import DAY, HOUR, T0, add_bars, add_occupant, wave


def ref_ema(xs, p):
    a = 2 / (p + 1)
    e = sum(xs[:p]) / p
    for x in xs[p:]:
        e = a * x + (1 - a) * e
    return e


def test_ema_matches_recursive_reference_and_seed():
    xs = wave(300, amp=0.2)
    e = T.ema(xs, 72)
    assert e[70] is None and e[71] == pytest.approx(sum(xs[:72]) / 72)
    assert e[-1] == pytest.approx(ref_ema(xs, 72), rel=1e-12)


def test_window_seed_effect_is_negligible():
    xs = wave(1200, amp=0.3, drift=0.001)
    full = T.ema(xs, 72)[-1]
    win = T.ema(xs[-432:], 72)[-1]
    assert abs(win / full - 1) < 1e-3


def _stats(bars, period="1h", expected=None, **kw):
    args = dict(period=period, expected_last_ts=expected if expected is not None else bars[-1][0], ema_period=72,
                k=2.0, min_bars=72, stale_tol=3)
    args.update(kw)
    return T.series_stats(bars, **args)


def _bars(closes, ps=HOUR, end=T0, mps=None):
    n = len(closes)
    return [(end - (n - 1 - i) * ps, c, None if mps is None else mps[i]) for i, c in enumerate(closes)]


def test_band_position_definition_and_clip():
    closes = wave(200, amp=0.05)
    st = _stats(_bars(closes))
    assert st.status == "ok"
    e = T.ema(closes, 72)
    devs = [math.log(c / v) for c, v in zip(closes[-72:], e[-72:])]
    sigma = math.sqrt(sum(d * d for d in devs) / len(devs))
    assert st.sigma == pytest.approx(sigma)
    assert st.band_pos_raw == pytest.approx(math.log(closes[-1] / e[-1]) / (2 * sigma))
    spike = closes[:-1] + [closes[-1] * 3]
    st2 = _stats(_bars(spike))
    assert st2.band_pos == 2.0 and st2.band_pos_raw > 2


def test_insufficient_bars_and_zero_vol():
    assert _stats(_bars(wave(71))).reason == "insufficient_bars"
    assert _stats(_bars(wave(80))).reason == "insufficient_bars"            # < 24 deviations after the seed
    assert _stats(_bars([1.0] * 200)).reason == "zero_vol"


def test_window_gaps_and_stale_bars():
    closes = wave(200)
    bars = [b for i, b in enumerate(_bars(closes)) if i % 5 != 0]      # 20 % missing
    st = _stats(bars)
    assert st.status == "failed" and st.reason == "window_gaps" and st.gaps == 39   # first bar dropped too
    few = [b for i, b in enumerate(_bars(closes)) if i not in (50, 51, 52)]
    assert _stats(few).status == "ok" and _stats(few).gaps == 3
    assert _stats(_bars(closes), expected=T0 + 3 * HOUR).status == "ok"      # 3 bars late = tolerated
    late = _stats(_bars(closes), expected=T0 + 4 * HOUR)
    assert late.reason == "stale_bars" and late.stale_bars == 4


def test_daily_extras_cross_days_and_censoring():
    up = [1.0 * math.exp(0.01 * i) for i in range(150)]
    st = _stats(_bars(up, ps=DAY), period="1d")
    assert st.cross_censored and st.cross_side == 1 and st.dd_90d == pytest.approx(0.0)
    assert st.rv_30d == pytest.approx(0.0, abs=1e-9)
    dip = up[:-5] + [up[-6] * 0.5] * 5
    st2 = _stats(_bars(dip, ps=DAY), period="1d")
    assert st2.cross_side == -1 and st2.cross_days == pytest.approx(4.0) and not st2.cross_censored
    assert st2.dd_90d == pytest.approx(-0.5)


def test_pct_rank_ties():
    r = T.pct_rank({"a": 1.0, "b": 2.0, "c": 2.0, "d": 3.0})
    assert r == {"a": 0.0, "b": 0.5, "c": 0.5, "d": 1.0}
    assert T.pct_rank({"x": 5.0}) == {"x": 0.5}


def test_overlay_matches_stored_numbers():
    closes = wave(500, amp=0.1)
    ov = T.overlay([(i, c) for i, c in enumerate(closes)], ema_period=72, k=2.0)
    st = _stats(_bars(closes[-432:]))
    assert ov[-1]["ema"] == pytest.approx(st.ema, rel=1e-3)
    assert ov[-1]["upper"] == pytest.approx(st.ema * math.exp(st.band), rel=2e-2)


@pytest.mark.asyncio
async def test_run_period_rows_isolation_running_bar_and_prelaunch(db, wb_settings):
    wb_settings()
    old = await add_occupant(db, 7, 100, status="deregistered")
    cur = await add_occupant(db, 7, 5000)
    pre = await add_occupant(db, 8, 6000, status="pre_launch")
    as_of = T0 + 10 * HOUR
    # the old occupant has a wild series under the same netuid — it must not leak into the current one
    await add_bars(db, 7, old, "1h", [100.0] * 50, end_ts=T0 - 500 * HOUR)
    await add_bars(db, 7, cur, "1h", wave(300), end_ts=as_of - HOUR, mps=[1.0] * 300)
    # a running bar (ts = as_of) exists and must be ignored
    await db.execute("INSERT INTO price_bars (netuid, period, ts, occupant_key, close, source, observed_at) "
                     "VALUES (7, '1h', ?, ?, 999.0, 'sse_rollup', ?)", (as_of, cur, as_of))
    res = await T.run_period(db, "1h", as_of, run_id="r", observed_at=as_of + 700)
    assert res.ok == 1 and res.rows == 10
    rows = {(r["occupant_key"], r["metric_key"]): r for r in await db.fetchall("SELECT * FROM metric_values")}
    vj = json.loads(rows[(cur, "ema72_1h")]["value_json"])
    assert vj["bar_ts"] == as_of - HOUR and vj["status"] == "ok" and vj["def"] == "wb-1"
    assert rows[(cur, "ema72_1h")]["source"] == "chain"
    assert rows[(cur, "ema72_1h")]["value"] == pytest.approx(T.ema(wave(300)[-432:], 72)[-1])
    assert rows[(cur, "px_vs_moving")]["value"] == pytest.approx(math.log(wave(300)[-1]))
    assert json.loads(rows[(pre, "ema72_band_pos_1h")]["value_json"])["status"] == "not_applicable"
    assert (old, "ema72_1h") not in rows


@pytest.mark.asyncio
async def test_daily_slot_rank_and_catchup(db, wb_settings):
    wb_settings(WB_EMA_TIMEFRAMES=["1d"])
    occs = []
    for n in range(1, 5):
        o = await add_occupant(db, n, 1000 + n)
        occs.append(o)
        await add_bars(db, n, o, "1d", wave(200, amp=0.05 * n, period=30 + n), end_ts=T0 - DAY)
    young = await add_occupant(db, 9, 9000)
    await add_bars(db, 9, young, "1d", wave(20), end_ts=T0 - DAY)
    out = await T.run_all(db, T0 + 12 * 60, run_id="r")
    assert len(out["1d"]) == T.CATCHUP_DAYS + 1
    ranks = {r["occupant_key"]: r for r in await db.fetchall(
        "SELECT * FROM metric_values WHERE metric_key = 'ema72_band_rank_1d' AND as_of = ?", (T0,))}
    assert sorted(r["value"] for o, r in ranks.items() if o != young) == pytest.approx([0, 1 / 3, 2 / 3, 1])
    assert json.loads(ranks[young]["value_json"])["reason"] == "insufficient_bars"
    back = await db.fetchval("SELECT COUNT(*) FROM metric_values WHERE as_of < ? AND backfilled = 1", (T0,))
    assert back > 0
    again = await T.run_all(db, T0 + 13 * 60, run_id="r2")
    assert again["1d"] == []                                               # nothing left to catch up


@pytest.mark.asyncio
async def test_band_exit_event_opt_in(db, wb_settings):
    wb_settings(WB_EMA_TIMEFRAMES=["1d"], WB_EVENTS_ENABLED=True)
    o = await add_occupant(db, 3, 33)
    closes = wave(200, amp=0.02)
    await add_bars(db, 3, o, "1d", closes, end_ts=T0 - 2 * DAY)
    await T.run_period(db, "1d", T0 - DAY, run_id="a", observed_at=T0 - DAY + 600)
    await add_bars(db, 3, o, "1d", [closes[-1] * 1.8], end_ts=T0 - DAY)
    res = await T.run_period(db, "1d", T0, run_id="b", observed_at=T0 + 600)
    assert res.events == 1
    ev = await db.fetchone("SELECT * FROM events WHERE event_type = 'ema72_band_exit'")
    assert json.loads(ev["payload"])["side"] == 1 and ev["source"] == "chain"
