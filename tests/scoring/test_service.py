"""Spec 12 §9 integration tests on a small synthetic world: EMA restart invariance, catch-up, kill switch, shadow
specs, pre-launch, immutability, events, rank stability, regime reuse, universe launch rule, backfill, rescore."""
import json
import sqlite3
from dataclasses import replace
from datetime import date
from types import MappingProxyType

import pytest
import pytest_asyncio

import app.config as app_config
from app.features.engine import FeatureEngine
from app.scoring import engine as E
from app.scoring import spec as SP
from app.scoring.engine import ScoringService, make_slot
from app.storage.db import Database
from app.storage.pit import put_metric_values

from .fixtures import add_occupant, choppy_chain_hourly, raw

DAY = 86400
D0 = 1790121600                                          # 2026-09-23 00:00Z
OCCS = [f"{n}:{100 + n}" for n in range(1, 13)]          # 12 active occupants


def _vals(i: int, day: int) -> dict:
    """Deterministic per-occupant, per-day raw values with some drift."""
    x = (i * 7 + day * 3) % 12
    return {"emission_share": 0.5 + x, "liquidity_depth": 100.0 + 10 * i + day, "chain_buys": float((i + day) % 5),
            "gate_margin": 0.5 + i / 6, "dereg_safety": i / 12, "exit_capacity": .3, "net_flow_30d": (x - 6) / 20,
            "active_authors_28d": float(i % 6), "site_uptime_30d": .9, "team_doxxed": .6 if i % 2 else 1.0,
            "venue_reachable": 1.0}


async def seed_day(db, slot: int, *, occs=OCCS, obs: int | None = None, backfilled: int = 0) -> None:
    rows = []
    day = (slot - D0) // DAY
    for i, o in enumerate(occs, start=1):
        for k, v in _vals(i, day).items():
            rows.append(raw(o, k, v, as_of=slot, observed_at=slot + 300 if obs is None else obs, backfilled=backfilled))
        rows += [raw(o, "emission_enabled_flag", 1.0, as_of=slot, observed_at=slot + 300),
                 raw(o, "dereg_risk_flag", 0.0, as_of=slot, observed_at=slot + 300),
                 raw(o, "immune_flag", 0.0, as_of=slot, observed_at=slot + 300)]
    await put_metric_values(db, rows)


async def world(db, days: int = 10) -> None:
    for o in OCCS:
        await add_occupant(db, o)
    for d in range(days):
        await seed_day(db, D0 + d * DAY)
        await choppy_chain_hourly(db, D0 + d * DAY) if d == 0 else None


@pytest.fixture
def no_fns(monkeypatch, tmp_root):
    monkeypatch.setattr(FeatureEngine, "producer_fns", lambda self: {})
    return tmp_root


async def _run(db, slot, t0=None, trigger="schedule", **kw):
    t0 = slot + 904 if t0 is None else t0
    svc = ScoringService(db, clock=lambda: t0)
    return await svc.run_slot(make_slot(slot, t0, trigger=trigger), t0=t0, **kw)


def _strip(rows):
    return [{k: v for k, v in r.items() if k not in ("id", "run_id")} for r in rows]


# ── EMA / restart invariance ─────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_restart_invariance(no_fns, tmp_path):
    out = []
    for split in (False, True):
        db = Database(tmp_path / f"r{int(split)}.db")
        await db.connect()
        await db.apply_schema()
        await world(db)
        for d in range(10):
            if split and d == 5:                      # "new process": a fresh connection and service
                await db.close()
                db = Database(tmp_path / f"r{int(split)}.db")
                await db.connect()
            await _run(db, D0 + d * DAY)
        out.append(_strip(await db.fetchall("SELECT * FROM scores ORDER BY as_of, occupant_key")))
        await db.close()
    assert out[0] == out[1] and len(out[0]) == 120


@pytest.mark.asyncio
async def test_ema_gap_reseed_and_provisional_not_prev(db, no_fns):
    await world(db, days=10)
    await _run(db, D0)
    await _run(db, D0 + 6 * 3600)                     # provisional
    r1 = await db.fetchone("SELECT * FROM scores WHERE as_of = ? AND occupant_key = ?", (D0 + DAY, OCCS[0])) or {}
    assert not r1
    await _run(db, D0 + DAY)
    a = await db.fetchone("SELECT * FROM scores WHERE as_of = ? AND occupant_key = ?", (D0, OCCS[0]))
    b = await db.fetchone("SELECT * FROM scores WHERE as_of = ? AND occupant_key = ?", (D0 + DAY, OCCS[0]))
    assert b["composite_smoothed"] == pytest.approx(.25 * b["composite"] + .75 * a["composite_smoothed"])
    await _run(db, D0 + 9 * DAY)                      # gap > 7 d → reseed
    c = await db.fetchone("SELECT * FROM scores WHERE as_of = ? AND occupant_key = ?", (D0 + 9 * DAY, OCCS[0]))
    assert c["composite_smoothed"] == pytest.approx(c["composite"]) and c["prev_tier"] is None


# ── catch-up ─────────────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_catchup_plan_and_rows(db, no_fns):
    await world(db, days=4)
    await _run(db, D0)
    now = D0 + 3 * DAY + 20 * 60
    svc = ScoringService(db, clock=lambda: now)
    out = await svc.run_scheduled(now)
    runs = await db.fetchall("SELECT as_of, trigger, mode, backfilled, ok FROM score_runs ORDER BY as_of")
    assert [(r["as_of"], r["trigger"], r["mode"], r["backfilled"]) for r in runs] == [
        (D0, "schedule", "strict", 0), (D0 + DAY, "catchup", "reconstructed", 1),
        (D0 + 2 * DAY, "catchup", "reconstructed", 1), (D0 + 3 * DAY, "schedule", "strict", 0)]
    assert out["rows"] == 36
    s = await db.fetchone("SELECT * FROM scores WHERE as_of = ? LIMIT 1", (D0 + DAY,))
    assert s["backfilled"] == 1 and s["input_cutoff"] == D0 + DAY + 900
    again = await svc.run_scheduled(now + 60)
    assert again["slots"][-1]["note"] == "already_scored" and again["rows"] == 0


@pytest.mark.asyncio
async def test_failed_attempt_is_retried_and_killed_not(db, no_fns, tmp_root):
    await world(db, days=3)
    await _run(db, D0)
    # killed slot D0+1d: never replanned
    (tmp_root / "SCORING_KILLED").write_text("")
    await _run(db, D0 + DAY)
    (tmp_root / "SCORING_KILLED").unlink()
    now = D0 + 2 * DAY + 1200
    out = await ScoringService(db, clock=lambda: now).run_scheduled(now)
    assert [x["as_of"] for x in out["slots"]] == [D0 + 2 * DAY]


# ── kill switch ──────────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_kill_switch(db, no_fns, tmp_root):
    await world(db, days=1)
    (tmp_root / "SCORING_KILLED").write_text("")
    res = await _run(db, D0)
    assert res["killed"] is True
    run = await db.fetchone("SELECT * FROM score_runs")
    assert run["killed"] == 1 and run["n_scored"] == 0
    for t in ("scores", "red_flag_state", "regime_daily", "rank_stability_daily"):
        assert await db.fetchval(f"SELECT COUNT(*) FROM {t}") == 0, t
    assert await db.fetchval("SELECT COUNT(*) FROM metric_values WHERE stage = 'norm'") == 0
    assert await db.fetchval("SELECT COUNT(*) FROM events WHERE source = 'scoring'") == 0
    assert await db.fetchval("SELECT COUNT(*) FROM metric_values WHERE stage = 'raw' AND source = 'scoring'") > 0


# ── shadow specs ─────────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_shadow_spec(db, no_fns, tmp_path, monkeypatch):
    await world(db, days=1)
    await _run(db, D0)
    primary = _strip(await db.fetchall("SELECT * FROM scores ORDER BY occupant_key"))
    ev_primary = await db.fetchval("SELECT COUNT(*) FROM events WHERE source = 'scoring'")
    db2 = Database(tmp_path / "shadow.db")
    await db2.connect()
    await db2.apply_schema()
    await world(db2, days=1)
    test_spec = replace(SP.TS_1_0, version="ts-test", v_w_by_regime=MappingProxyType(
        {"TRENDING": .3, "DISPERSED": .3, "CHOPPY": .3, "DEAD": .3, "UNKNOWN": .3}))
    specs = MappingProxyType({"ts-1.0": SP.TS_1_0, "ts-test": test_spec})
    monkeypatch.setattr(SP, "SPECS", specs)
    monkeypatch.setattr(E, "SPECS", specs)
    s = app_config.settings.model_copy(update={"SCORING_SHADOW_SPECS": "ts-test"})
    monkeypatch.setattr(app_config, "settings", s)
    await _run(db2, D0)
    rows = await db2.fetchall("SELECT * FROM scores ORDER BY occupant_key")
    sh = [r for r in rows if r["spec_version"] == "ts-test"]
    assert len(sh) == 12 and all(r["shadow"] == 1 and r["v_w"] == .3 for r in sh)
    assert _strip([r for r in rows if r["spec_version"] == "ts-1.0"]) == primary
    assert await db2.fetchval("SELECT COUNT(*) FROM metric_values WHERE stage='norm' AND spec_version='ts-test'") == 45 * 12
    assert await db2.fetchval("SELECT COUNT(*) FROM events WHERE source = 'scoring'") == ev_primary
    assert await db2.fetchval("SELECT COUNT(*) FROM score_runs WHERE spec_version='ts-test' AND shadow=1") == 1
    assert await db2.fetchval("SELECT COUNT(*) FROM regime_daily") == 1
    await db2.close()


# ── pre-launch, universe ─────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_prelaunch_and_root(db, no_fns):
    await world(db, days=1)
    pre = "50:5000"
    await add_occupant(db, pre, status="pre_launch", activated_at=None)
    await add_occupant(db, "0:0")
    await put_metric_values(db, [raw(pre, "emission_share", 99.0), raw(pre, "active_authors_28d", 5.0),
                                 raw(pre, "fte_devs_84d", 2.0), raw(pre, "commit_days_90d", 30.0),
                                 raw(pre, "site_uptime_30d", 1.0), raw(pre, "team_doxxed", 1.0),
                                 raw(pre, "venue_reachable", 1.0), raw("0:0", "emission_share", 1.0)])
    await _run(db, D0)
    r = await db.fetchone("SELECT * FROM scores WHERE occupant_key = ?", (pre,))
    assert r["status"] == "pre_launch" and r["quality"] is None and r["velocity"] is None
    assert r["composite"] is None and r["composite_smoothed"] is None and r["tier"] is None
    ps = json.loads(r["pillar_scores"])
    assert ps["P1"] is None and ps["P2"] is not None and r["insufficient_data"] == 0
    assert r["confidence"] == pytest.approx((20 * .5 + 10 * .3 + 18 * .25 + 12 * .1) / 60)
    n = json.loads(await db.fetchval("SELECT value_json FROM metric_values WHERE stage='norm' AND occupant_key=? "
                                     "AND metric_key='emission_share'", (OCCS[0],)))
    assert n["cohort_n"] == 12                                     # pre-launch + root excluded from rankg
    assert await db.fetchval("SELECT COUNT(*) FROM scores WHERE netuid = 0") == 0


@pytest.mark.asyncio
async def test_universe_launch_instant(db, no_fns):
    await db.execute("INSERT INTO block_times (block_number, ts_ms, endpoint, observed_at) VALUES (?,?,?,?)",
                     (1000, (D0 - 10 * DAY) * 1000, "t", D0))
    await db.execute("INSERT INTO block_times (block_number, ts_ms, endpoint, observed_at) VALUES (?,?,?,?)",
                     (2000, (D0 - 10 * DAY + 12000) * 1000, "t", D0))
    await add_occupant(db, "1:1", activated_at=D0 + 5 * DAY, first_emission_block=1500)   # 04's late sighting
    await add_occupant(db, "2:2", status="deregistered", activated_at=None, deregistered_at=D0 + 20 * DAY)
    await add_occupant(db, "3:3", status="pre_launch", activated_at=None)
    await add_occupant(db, "4:4", status="deregistered", activated_at=None, deregistered_at=D0 - DAY)
    eng = FeatureEngine(db)
    u = {o.occupant_key: o for o in await eng.universe(D0)}
    assert u["1:1"].status == "active" and u["1:1"].launch_ts == D0 - 10 * DAY + 6000
    assert u["2:2"].status == "active" and u["3:3"].status == "pre_launch" and "4:4" not in u
    u = {o.occupant_key: o for o in await eng.universe(D0 - 10 * DAY)}
    assert u["1:1"].status == "pre_launch"


# ── immutability ─────────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_immutability(db, no_fns):
    await world(db, days=1)
    await _run(db, D0)
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        await db.execute("UPDATE scores SET tier = 'A'")
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        await db.execute("DELETE FROM scores")
    with pytest.raises(sqlite3.IntegrityError):
        await db.execute("DELETE FROM metric_values WHERE stage = 'norm'")
    res = await _run(db, D0, t0=D0 + 3000)
    assert res["note"] == "already_scored"


# ── events ───────────────────────────────────────────────────────────────────────────────────────────────────
async def _prev(db, okey, as_of, composite, insufficient=0, tier="C", smoothed=50.0):
    from .fixtures import prev_score
    await prev_score(db, okey, as_of, smoothed=smoothed, tier=tier, composite=composite, insufficient=insufficient)


@pytest.mark.asyncio
async def test_insufficient_transition(db, no_fns):
    await world(db, days=6)
    thin = "60:6000"
    await add_occupant(db, thin)

    async def ins_events():
        return await db.fetchall("SELECT as_of FROM (SELECT json_extract(payload, '$.as_of') AS as_of FROM events "
                                 "WHERE event_type = 'insufficient_data' AND occupant_key = ?)", (thin,))
    for d, full in ((0, False), (1, False), (2, True), (5, False)):        # day-2 rows are stale by day 5
        if full:
            await seed_day(db, D0 + d * DAY, occs=[thin])
        else:
            await put_metric_values(db, [raw(thin, "emission_share", 1.0, as_of=D0 + d * DAY,
                                             observed_at=D0 + d * DAY + 300)])
        await _run(db, D0 + d * DAY)
        r = await db.fetchone("SELECT insufficient_data FROM scores WHERE occupant_key = ? AND as_of = ?",
                              (thin, D0 + d * DAY))
        assert r["insufficient_data"] == (0 if full else 1)
    assert [x["as_of"] for x in await ins_events()] == [D0, D0 + 5 * DAY]     # only on entering


@pytest.mark.asyncio
async def test_score_jump_threshold(tmp_path, no_fns):
    for delta, fires in ((15.0, True), (14.99, False)):
        db = Database(tmp_path / f"j{delta}.db")
        await db.connect()
        await db.apply_schema()
        await world(db, days=2)
        svc = ScoringService(db, clock=lambda: D0 + 904)
        dry = await svc.run_slot(make_slot(D0, D0 + 904, trigger="schedule"), t0=D0 + 904, dry_run=True)
        c = {r["occupant_key"]: r["composite"] for r in dry["specs"]["ts-1.0"]["rows"]}
        o = OCCS[0]
        await _prev(db, o, D0 - DAY, composite=c[o] - delta)
        await _run(db, D0)
        n = await db.fetchval("SELECT COUNT(*) FROM events WHERE event_type = 'score_jump' AND occupant_key = ?", (o,))
        assert n == (1 if fires else 0), delta
        if fires:
            p = json.loads(await db.fetchval("SELECT payload FROM events WHERE event_type = 'score_jump'"))
            assert p["delta"] == pytest.approx(15.0) and set(p["pillar_deltas"]) == {"P1", "P2", "P3", "P4", "P5", "P6"}
        await db.close()


@pytest.mark.asyncio
async def test_events_disabled_and_catchup_backfilled_payload(db, no_fns, monkeypatch):
    await world(db, days=3)
    await _run(db, D0)
    monkeypatch.setattr(app_config, "settings", app_config.settings.model_copy(update={"SCORING_EVENTS_ENABLED": False}))
    n0 = await db.fetchval("SELECT COUNT(*) FROM events WHERE source = 'scoring'")
    await _run(db, D0 + DAY)
    assert await db.fetchval("SELECT COUNT(*) FROM events WHERE source = 'scoring'") == n0
    monkeypatch.setattr(app_config, "settings", app_config.settings.model_copy(update={"SCORING_EVENTS_ENABLED": True}))
    # a flag raised in a reconstructed run carries payload.backfilled = true
    await put_metric_values(db, [raw(OCCS[1], "dereg_risk_flag", 1.0, as_of=D0 + 2 * DAY + 3600,
                                     observed_at=D0 + 2 * DAY + 3700)])
    t0 = D0 + 2 * DAY + 5 * 3600
    svc = ScoringService(db, clock=lambda: t0)
    await svc.run_slot(make_slot(D0 + 2 * DAY, t0, trigger="catchup", reconstructed=True), t0=t0)
    e = await db.fetchone("SELECT * FROM events WHERE event_type = 'red_flag_raised'")
    assert e is None                                   # the risk row is observed after the reconstructed cutoff
    await put_metric_values(db, [raw(OCCS[1], "dereg_risk_flag", 1.0, as_of=D0 + 3 * DAY, observed_at=D0 + 3 * DAY + 60,
                                     backfilled=1)])
    await seed_day(db, D0 + 3 * DAY)
    t1 = D0 + 3 * DAY + 6 * 3600
    await ScoringService(db, clock=lambda: t1).run_slot(make_slot(D0 + 3 * DAY, t1, trigger="catchup",
                                                                  reconstructed=True), t0=t1)
    e = await db.fetchone("SELECT * FROM events WHERE event_type = 'red_flag_raised'")
    p = json.loads(e["payload"])
    assert p["backfilled"] is True and p["flag"] == "dereg_imminent" and e["severity"] == "critical"
    assert e["dedupe_key"] == f"{OCCS[1]}|red_flag_raised|dereg_imminent|{D0 + 3 * DAY}"


@pytest.mark.asyncio
async def test_scoring_stalled_fires_once_and_clears(rt, monkeypatch, alerts_sink):
    monkeypatch.setattr(FeatureEngine, "producer_fns", lambda self: {})
    db = rt.db
    await world(db, days=1)
    await _run(db, D0)
    svc = ScoringService(db)
    from app.jobs import SPEC_BY_ID
    job = E.ScoringJob(rt, SPEC_BY_ID["scoring_6h"])
    later = D0 + 9 * 3600
    await job._stall(svc, later, after=False)
    await job._stall(svc, later + 60, after=False)
    ev = await db.fetchall("SELECT * FROM events WHERE event_type = 'scoring_stalled'")
    assert len(ev) == 1 and ev[0]["severity"] == "critical" and ev[0]["occupant_key"] is None
    p = json.loads(ev[0]["payload"])
    assert p["last_ok_as_of"] == D0 and p["slot_as_of"] == D0 + 6 * 3600 and p["missed_slots"] == 1
    assert rt.alerts.is_latched("scoring_stalled")
    await job._stall(svc, D0 + 3600, after=True)
    assert not rt.alerts.is_latched("scoring_stalled")


# ── rank stability ───────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_rank_stability_rows(db, no_fns, monkeypatch):
    await world(db, days=2)
    await _run(db, D0)
    from app.scoring import stability as ST
    now = {o: (i, float(i)) for i, o in enumerate(OCCS)}
    assert ST.stats(now, now)["rank_autocorr"] == pytest.approx(1.0)
    rev = {o: (i, float(-i)) for i, o in enumerate(OCCS)}
    s = ST.stats(now, rev)
    assert s["rank_autocorr"] == pytest.approx(-1.0) and s["top20_turnover"] == 0.0
    small = dict(list(now.items())[:9])
    assert ST.stats(small, small)["rank_autocorr"] is None
    shuffled = dict(sorted(now.items(), key=lambda kv: -kv[1][0]))
    assert ST.stats(shuffled, rev)["quintile_turnover"] == ST.stats(now, rev)["quintile_turnover"]
    await _run(db, D0 + DAY)
    rows = await db.fetchall("SELECT * FROM rank_stability_daily WHERE as_of = ?", (D0 + DAY,))
    assert {(r["series"], r["lag_days"]) for r in rows} == {("health", 1), ("health", 7), ("composite_smoothed", 1),
                                                            ("composite_smoothed", 7)}


@pytest.mark.asyncio
async def test_rank_instability_event(db, no_fns, monkeypatch):
    from app.scoring import stability as ST

    async def fake(db_, spec, slot):
        return [{"as_of": slot, "spec_version": spec, "series": "health", "lag_days": 1, "n": 40, "rank_autocorr": .85,
                 "quintile_turnover": .3, "top20_turnover": .2},
                {"as_of": slot, "spec_version": spec, "series": "composite_smoothed", "lag_days": 1, "n": 40,
                 "rank_autocorr": .5, "quintile_turnover": .3, "top20_turnover": .2}]
    monkeypatch.setattr(E, "rank_autocorr_health", fake)
    await world(db, days=1)
    await _run(db, D0)
    ev = await db.fetchall("SELECT * FROM events WHERE event_type = 'rank_instability'")
    assert len(ev) == 1 and ev[0]["severity"] == "info" and ev[0]["occupant_key"] is None
    assert ev[0]["dedupe_key"] == f"system|rank_instability|ts-1.0|{D0}"
    assert json.loads(ev[0]["payload"])["series"] == "health"
    assert ST.MIN_N == 10


# ── regime ───────────────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_regime_live_and_reuse(db, no_fns):
    from app.quant.regime import REGIME_TS_1_0, V_W_BY_REGIME_TS_1_0, regime_for_slot
    await choppy_chain_hourly(db, D0)
    r = await regime_for_slot(db, D0, D0 + 900, "strict", REGIME_TS_1_0, V_W_BY_REGIME_TS_1_0)
    assert r.label == "CHOPPY" and r.v_w == .15
    r = await regime_for_slot(db, D0 + DAY, D0 + DAY + 900, "strict", REGIME_TS_1_0, V_W_BY_REGIME_TS_1_0)
    assert r.label == "UNKNOWN" and r.metrics["thin_universe"] is True and r.v_w == .15
    await db.execute("INSERT INTO regime_daily (day_ts, spec_version, label, raw_regime, v_w, source, computed_at, "
                     "backfilled) VALUES (?,?,?,?,?,?,?,?)", (D0 + DAY, "ts-1.0", "TRENDING", "TRENDING", .25, "backfill",
                                                              D0, 1))
    r = await regime_for_slot(db, D0 + DAY, D0 + DAY + 900, "strict", REGIME_TS_1_0, V_W_BY_REGIME_TS_1_0,
                              spec_version="ts-1.0", official=True)
    assert r.label == "TRENDING" and r.v_w == .25 and r.metrics["reused"] == "backfill"


# ── backfill, rescore ────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_backfill_memory_flags_no_events(db, no_fns):
    await world(db, days=6)
    await put_metric_values(db, [raw(OCCS[2], "dereg_risk_flag", 1.0, as_of=D0 + DAY + 3600, observed_at=D0 + DAY + 3700)])
    await _run(db, D0 + 5 * DAY)                      # a live slot exists
    live_state = await db.fetchall("SELECT * FROM red_flag_state")
    svc = ScoringService(db, clock=lambda: D0 + 6 * DAY)
    out = await svc.backfill("ts-1.0", date(2026, 9, 23))
    assert out["slots"] == 5 and out["to"] == "2026-09-27T00:00:00Z"
    assert await db.fetchval("SELECT COUNT(*) FROM scores WHERE backfilled = 1") == 60
    assert await db.fetchall("SELECT * FROM red_flag_state") == live_state
    assert await db.fetchval("SELECT COUNT(*) FROM events WHERE source = 'scoring' AND observed_at >= ?",
                             (D0 + 6 * DAY,)) == 0
    assert await db.fetchval("SELECT COUNT(*) FROM score_runs WHERE trigger = 'backfill'") == 5
    assert await db.fetchval("SELECT COUNT(*) FROM metric_values WHERE stage='raw' AND source='scoring' "
                             "AND as_of < ?", (D0 + 5 * DAY,)) == 0          # no fns / derive in backfill
    out2 = await svc.backfill("ts-1.0", date(2026, 9, 23))
    assert out2["slots"] == 0


@pytest.mark.asyncio
async def test_rescore_refusals(db, no_fns, tmp_root):
    await world(db, days=3)
    await _run(db, D0 + DAY)
    svc = ScoringService(db, clock=lambda: D0 + 2 * DAY + 3 * 3600)
    with pytest.raises(E.ScoringRefused) as ei:
        await svc.rescore(date(2026, 9, 24), "ts-1.0")
    assert ei.value.code == "slot_already_scored"
    with pytest.raises(E.ScoringRefused) as ei:
        await svc.rescore(date(2026, 9, 23), "ts-1.0")
    assert ei.value.code == "slot_regresses_state"
    with pytest.raises(E.ScoringRefused):
        await svc.rescore(None, "ts-9")
    (tmp_root / "SCORING_KILLED").write_text("")
    with pytest.raises(E.ScoringRefused) as ei:
        await svc.rescore(None, "ts-1.0")
    assert ei.value.code == "killed"
    (tmp_root / "SCORING_KILLED").unlink()
    rid = await svc.rescore(date(2026, 9, 25), "ts-1.0")
    assert rid.startswith("rs-")
    assert await db.fetchval("SELECT trigger FROM score_runs WHERE run_id = ?", (rid,)) == "rescore"


@pytest.mark.asyncio
async def test_spec_unknown_and_clock(db, no_fns, monkeypatch):
    monkeypatch.setattr(app_config, "settings", app_config.settings.model_copy(update={"SPEC_VERSION": "ts-9"}))
    with pytest.raises(E.ScoringRefused) as ei:
        await ScoringService(db).run_scheduled(D0 + 900)
    assert ei.value.code == "spec_unknown"
    monkeypatch.undo()
    with pytest.raises(E.ScoringRefused) as ei:
        await ScoringService(db).run_slot(make_slot(D0, D0 - 10, trigger="cli"), t0=D0 - 10)
    assert ei.value.code == "clock_before_slot"


@pytest.mark.asyncio
async def test_scores_status(rt, monkeypatch):
    monkeypatch.setattr(FeatureEngine, "producer_fns", lambda self: {})
    st = await E.scores_status(rt)
    assert st["latest_as_of"] is None and st["stale"] is True and st["spec_version"] == "ts-1.0"
    await world(rt.db, days=1)
    await _run(rt.db, D0)
    st = await E.scores_status(rt)
    assert st["latest_as_of"] == "2026-09-23T00:00:00Z" and st["last_run_ok"] is True
