"""Spec 12 §9 test_features_job: raw-only rows keyed floor_hour(now), fn failure handling, evidence fns."""
import asyncio
import json

import pytest

from app.features import registry as R
from app.features.engine import FeatureEngine, FeaturesJob
from app.features.types import MetricResult
from app.jobs import SPEC_BY_ID
from tests.scoring.fixtures import add_occupant

NOW = 1790121600 + 7 * 3600 + 1234


async def _run(rt):
    from app.jobs import RunContext
    import app.config as app_config
    from app.logging.logger import logger
    job = FeaturesJob(rt, SPEC_BY_ID["features_hourly"])
    ctx = RunContext(run_id="fh1", started_at=NOW, trigger="manual", rt=rt, settings=app_config.settings, log=logger)
    return await job.collect(ctx)


@pytest.mark.asyncio
async def test_raw_only_hour_keyed(rt, monkeypatch):
    for o in ("1:10", "2:20"):
        await add_occupant(rt.db, o)
    await add_occupant(rt.db, "0:0")

    async def ok(db, o, t):
        assert t.tzinfo is not None and int(t.timestamp()) == NOW
        return MetricResult(1.0, {"members": 10})

    async def boom(db, o, t):
        raise RuntimeError("x")

    async def bad(db, o, t):
        return 5

    async def ev(db, o, t):
        return MetricResult(2.0, {})
    called = []

    async def never(db, o, t):
        called.append(o)
        return MetricResult(1.0, {})
    monkeypatch.setattr(FeatureEngine, "producer_fns", lambda self: {"venue_reachable": ok, "discord_members": boom,
                                                                     "presence_ratio": bad, "tg_members": ev})
    R.register_metric("announcement_cadence_30d", fn=never)
    out = await _run(rt)
    assert out.rows > 0 and not called
    rows = await rt.db.fetchall("SELECT * FROM metric_values")
    assert {r["stage"] for r in rows} == {"raw"}
    assert {r["as_of"] for r in rows} == {(NOW // 3600) * 3600}
    assert "0:0" not in {r["occupant_key"] for r in rows}
    by = {(r["occupant_key"], r["metric_key"]): r for r in rows}
    assert by[("1:10", "venue_reachable")]["status"] == "ok" and by[("1:10", "venue_reachable")]["source"] == "community"
    assert json.loads(by[("1:10", "discord_members")]["value_json"])["reason"] == "fn_error"
    assert json.loads(by[("1:10", "presence_ratio")]["value_json"])["reason"] == "bad_return"
    assert by[("1:10", "tg_members")]["value"] == 2.0
    assert by[("1:10", "silence_days")]["source"] == "scoring"
    assert by[("1:10", "announcement_cadence_30d")]["status"] == "failed"
    n_before = len(rows)
    out2 = await _run(rt)
    assert out2.rows == 0 and len(await rt.db.fetchall("SELECT * FROM metric_values")) == n_before


@pytest.mark.asyncio
async def test_missing_module_and_budget_timeout(rt, monkeypatch):
    await add_occupant(rt.db, "1:10")
    import sys

    import app.features.registry as REG
    monkeypatch.setitem(sys.modules, "app.features.sentiment", None)       # simulate an unbuilt producer module
    for k in ("event_impact_14d", "polarity_7d", "attention_7d", "kol_coverage_30d"):
        if k in REG.ATTACHED_FNS:
            monkeypatch.delitem(REG.ATTACHED_FNS, k)
    eng = FeatureEngine(rt.db)
    fns = eng.producer_fns()
    assert "event_impact_14d" not in fns and "event_impact_14d" in eng.stats["unavailable"]
    assert "venue_reachable" in fns

    async def slow(db, o, t):
        await asyncio.sleep(5)
    eng.producer_fns = lambda: {"venue_reachable": slow}
    occ = await eng.universe(NOW)
    await eng.compute_registered(occ, NOW, (NOW // 3600) * 3600, "r", mode="strict", timeout_s=0.01)
    vj = json.loads(await rt.db.fetchval("SELECT value_json FROM metric_values WHERE metric_key = 'venue_reachable'"))
    assert vj["reason"] == "fn_timeout"
    eng2 = FeatureEngine(rt.db)

    async def fast(db, o, t):
        return MetricResult(1.0, {})
    eng2.producer_fns = lambda: {"discord_members": fast}
    await eng2.compute_registered(occ, NOW, (NOW // 3600) * 3600, "r", mode="strict", budget_s=-1)
    vj = json.loads(await rt.db.fetchval("SELECT value_json FROM metric_values WHERE metric_key = 'discord_members'"))
    assert vj["reason"] == "fn_budget"


@pytest.mark.asyncio
async def test_evidence_fn_registered(rt, monkeypatch):
    await add_occupant(rt.db, "1:10")
    monkeypatch.setattr(R, "EVIDENCE_FNS", {})

    async def vid(db, o, t):
        return MetricResult(7.0, {})
    R.register_metric("video_uploads_90d", fn=vid)
    eng = FeatureEngine(rt.db)
    assert eng.producer_fns()["video_uploads_90d"] is vid
