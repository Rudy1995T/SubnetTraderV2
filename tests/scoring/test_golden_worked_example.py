"""Spec 12 §2.14 golden worked example (also the golden test, §9): every stated number, rows and events."""
import json

import pytest
import pytest_asyncio

from app.features.engine import FeatureEngine
from app.scoring.engine import ScoringService, make_slot
from app.storage.catalog import EVENT_TYPES

from .fixtures import SLOT, SN64, T0, TAIL, build_golden

A4, A3 = 1e-4, 1e-3


@pytest_asyncio.fixture
async def golden(db, tmp_root, monkeypatch):
    await build_golden(db)
    monkeypatch.setattr(FeatureEngine, "producer_fns", lambda self: {})      # 08/09/10 rows come from the fixture
    svc = ScoringService(db, clock=lambda: T0)
    res = await svc.run_slot(make_slot(SLOT, T0, trigger="schedule"), t0=T0)
    rows = {r["occupant_key"]: r for r in await db.fetchall("SELECT * FROM scores WHERE as_of = ?", (SLOT,))}
    return db, res, rows


def _m(ex: dict, pillar: str, key: str) -> float | None:
    return ex["pillars"][pillar]["metrics"][key]["m"]


@pytest.mark.asyncio
async def test_run_summary(golden):
    db, res, rows = golden
    assert res["mode"] == "strict" and res["run_kind"] == "official" and res["n_scored"] == 122
    assert "0:0" not in rows and len(rows) == 122                                  # root never scored
    assert await db.fetchval("SELECT COUNT(*) FROM metric_values WHERE stage = 'norm' AND as_of = ?", (SLOT,)) == 45 * 122
    assert await db.fetchval("SELECT COUNT(DISTINCT observed_at) FROM metric_values WHERE stage = 'norm'") == 1
    assert await db.fetchval("SELECT observed_at FROM metric_values WHERE stage = 'norm' LIMIT 1") == T0
    run = await db.fetchone("SELECT * FROM score_runs")
    assert run["ok"] == 1 and run["regime"] == "CHOPPY" and run["n_universe"] == 122 and run["n_events"] == 2


@pytest.mark.asyncio
async def test_sn64_numbers(golden):
    _, _, rows = golden
    r = rows[SN64]
    ex = json.loads(r["explain"])
    p = json.loads(r["pillar_scores"])
    assert _m(ex, "P1", "emission_share") == pytest.approx(.94956, abs=A4)
    assert ex["pillars"]["P1"]["metrics"]["emission_share"]["cohort_n"] == 122
    assert _m(ex, "P1", "liquidity_depth") == pytest.approx(.90874, abs=A4)
    assert _m(ex, "P1", "chain_buys") == pytest.approx(.67972, abs=A4)
    assert _m(ex, "P1", "exit_capacity") == pytest.approx(.56182, abs=A4)
    assert _m(ex, "P1", "holder_dispersion") == pytest.approx(.41667, abs=A4)
    assert _m(ex, "P1", "participation") is None
    assert _m(ex, "P5", "x_engagement_30d") == pytest.approx(.88908, abs=A4)
    assert ex["pillars"]["P5"]["metrics"]["x_engagement_30d"]["cohort_n"] == 58
    assert _m(ex, "P5", "x_followers") == pytest.approx(.98389, abs=A4)
    assert _m(ex, "P6", "event_impact_14d") == pytest.approx(.90940, abs=A4)
    assert _m(ex, "P6", "kol_coverage_30d") == pytest.approx(.77371, abs=A4)
    for k, v in {"P1": .857835, "P2": .930906, "P3": .99, "P4": .912, "P5": .923103, "P6": .751816}.items():
        assert p[k] == pytest.approx(v, abs=A4)
    assert r["quality"] == pytest.approx(90.8293, abs=A3)
    assert r["confidence"] == pytest.approx(.98333, abs=A4)
    assert ex["velocity"]["blend"] == pytest.approx(.593242, abs=A4)
    assert r["velocity"] == pytest.approx(68.8386, abs=A3)
    assert r["composite_raw"] == pytest.approx(87.5307, abs=A3) and r["flag_multiplier"] == 1.0
    assert r["composite"] == pytest.approx(87.5307, abs=A3)
    assert r["composite_smoothed"] == pytest.approx(81.9577, abs=A3)
    assert r["tier"] == "A" and r["prev_tier"] == "A" and r["red_flags"] == "[]"
    assert r["regime"] == "CHOPPY" and r["v_w"] == .15
    assert r["computed_at"] == T0 and r["input_cutoff"] == T0 and r["backfilled"] == 0 and r["shadow"] == 0
    assert [x["metric"] for x in ex["top_positive"]] == ["emission_share", "gate_margin", "dereg_safety"]
    assert [x["impact"] for x in ex["top_positive"]] == pytest.approx([3.15478, 2.631579, 2.535007], abs=A3)
    assert [x["metric"] for x in ex["top_negative"]] == ["discord_growth_30d", "holder_dispersion"]
    assert ex["missing"] == ["participation"] and ex["flags"] == []
    assert ex["prev"] == {"as_of": SLOT - 86400, "composite_smoothed": 80.1, "tier": "A"}


@pytest.mark.asyncio
async def test_tail_numbers(golden):
    _, _, rows = golden
    r = rows[TAIL]
    ex = json.loads(r["explain"])
    p = json.loads(r["pillar_scores"])
    cov = json.loads(r["pillar_coverage"])
    assert _m(ex, "P1", "emission_share") == pytest.approx(.29411, abs=A4)     # 37 tied zeros share r0 = 18
    assert _m(ex, "P1", "liquidity_depth") == pytest.approx(.14239, abs=A4)
    assert _m(ex, "P1", "chain_buys") == pytest.approx(.30761, abs=A4)
    assert _m(ex, "P3", "site_freshness") == pytest.approx(.23636, abs=A4)
    assert _m(ex, "P4", "tenure") == pytest.approx(.73469, abs=A4)
    assert _m(ex, "P2", "review_practice_90d") is None
    for k, v in {"P1": .177847, "P2": .039474, "P3": .519318, "P4": .280969, "P5": 0.0, "P6": .433333}.items():
        assert p[k] == pytest.approx(v, abs=A4)
    assert cov["P5"] == pytest.approx(.30, abs=1e-9)                          # not < .30 → kept
    assert ex["pillars"]["P5"]["metrics"]["silence_days"]["st"] == "absent"
    assert r["quality"] == pytest.approx(18.1950, abs=A3)
    assert r["confidence"] == pytest.approx(.82333, abs=A4)
    assert r["velocity"] == pytest.approx(38.1014, abs=A3)
    assert r["composite_raw"] == pytest.approx(21.1810, abs=A3)
    assert r["flag_multiplier"] == pytest.approx(.54)
    assert r["composite"] == pytest.approx(11.4377, abs=A3)
    assert r["composite_smoothed"] == pytest.approx(26.4094, abs=A3)
    assert r["tier"] == "F" and r["prev_tier"] == "D"
    assert json.loads(r["red_flags"]) == ["dereg_imminent", "stale_identity"]
    assert [x["metric"] for x in ex["top_positive"]] == ["control_stability_90d", "site_age", "tls_ok"]
    assert [x["metric"] for x in ex["top_negative"]] == ["silence_days", "dereg_safety", "gate_margin"]
    assert [x["impact"] for x in ex["top_negative"]] == pytest.approx([-3.333333, -2.941176, -2.941176], abs=A3)
    fl = {f["flag"]: f for f in ex["flags"]}
    assert fl["dereg_imminent"]["since"] == SLOT - 2 * 86400 and fl["dereg_imminent"]["multiplier"] == .6
    assert fl["stale_identity"]["origin"] == "auto" and fl["stale_identity"]["state"] == "active"


@pytest.mark.asyncio
async def test_impacts_sum_to_health_minus_50(golden):
    _, _, rows = golden
    for o in (SN64, TAIL):
        ex = json.loads(rows[o]["explain"])
        s = sum(m["impact"] or 0 for p in ("P1", "P2", "P3", "P4", "P5") for m in ex["pillars"][p]["metrics"].values())
        assert s == pytest.approx(ex["health"] - 50, abs=1e-4)


@pytest.mark.asyncio
async def test_events_exactly(golden):
    db, _, _ = golden
    evs = await db.fetchall("SELECT * FROM events WHERE source = 'scoring' ORDER BY id")
    assert {(e["event_type"], e["occupant_key"]) for e in evs} == {("red_flag_raised", TAIL), ("tier_change", TAIL)}
    for e in evs:
        p = json.loads(e["payload"])
        assert all(k in p for k in EVENT_TYPES[e["event_type"]].payload_keys)
        assert e["event_at"] == SLOT and e["observed_at"] == T0
    raised = next(e for e in evs if e["event_type"] == "red_flag_raised")
    p = json.loads(raised["payload"])
    assert p["flag"] == "stale_identity" and p["human"] is False and p["sticky"] is False
    assert p["flag_multiplier_total"] == pytest.approx(.54) and raised["severity"] == "warning"
    assert p["detail"]["dead_venue"] and p["detail"]["name_turnover"] and p["detail"]["no_comms_30d"]
    assert raised["dedupe_key"] == f"{TAIL}|red_flag_raised|stale_identity|{SLOT}"
    tc = json.loads(next(e for e in evs if e["event_type"] == "tier_change")["payload"])
    assert (tc["from"], tc["to"], tc["direction"]) == ("D", "F", "down")


@pytest.mark.asyncio
async def test_norm_rows_and_derived_raw(golden):
    db, _, _ = golden
    r = await db.fetchone("SELECT value, value_json, status, source FROM metric_values WHERE stage = 'norm' "
                          "AND occupant_key = ? AND metric_key = 'emission_share'", (SN64,))
    vj = json.loads(r["value_json"])
    assert r["source"] == "scoring" and r["status"] == "ok" and vj["raw"] == pytest.approx(9.649)
    assert vj["cohort_n"] == 122 and vj["age_s"] == 0 and vj["src"] == "chain"
    r = await db.fetchone("SELECT value, value_json, status FROM metric_values WHERE stage = 'norm' "
                          "AND occupant_key = ? AND metric_key = 'participation'", (SN64,))
    assert r["value"] is None and r["status"] == "failed"
    rows = await db.fetchall("SELECT occupant_key, metric_key, value, status FROM metric_values WHERE stage = 'raw' "
                             "AND source = 'scoring' AND occupant_key IN (?, ?)", (SN64, TAIL))
    got = {(x["occupant_key"], x["metric_key"]): (x["value"], x["status"]) for x in rows}
    assert got[(SN64, "announcement_cadence_30d")] == (22.0, "ok")
    assert got[(SN64, "silence_days")] == (0.5, "ok")
    assert got[(TAIL, "announcement_cadence_30d")] == (None, "failed")
    assert got[(TAIL, "silence_days")] == (0.0, "absent")


@pytest.mark.asyncio
async def test_rerun_is_idempotent(golden):
    db, _, _ = golden
    svc = ScoringService(db, clock=lambda: T0 + 60)
    res = await svc.run_slot(make_slot(SLOT, T0 + 60, trigger="schedule"), t0=T0 + 60)
    assert res["note"] == "already_scored"
    assert await db.fetchval("SELECT COUNT(*) FROM scores WHERE as_of = ?", (SLOT,)) == 122
