"""Spec 11 §9 T39–T41 (+ T40 subset): integrity_features rows, PIT, slots, first-success-wins, catch-up."""
from __future__ import annotations

import json

import pytest

from app.integrity import constants as K
from app.integrity.features import missing_official_days, run_features, slot_as_of
from app.storage.catalog import METRIC_KEYS, RETIRED_METRIC_KEYS
from app.util.time import floor_day
from tests.integrity.conftest import DAY, HOUR, OWNER, T0, seed_coverage, seed_hourly, seed_occ, seed_snapshot

pytestmark = pytest.mark.asyncio


async def world(db, t):
    o = await seed_occ(db, 64, last_seen_block=9_000_000)
    k = o["occupant_key"]
    await seed_occ(db, 0, reg=0, owner_coldkey=None)                       # root: never written
    for d in range(31, -1, -1):
        await seed_snapshot(db, k, 64, t - d * DAY - 600, owner_cut_per_block=0.18, owner_cut_enabled=1,
                            owner_total_alpha=700_000.0, owner_available_alpha=86_000.0, tmc_owner_perpetual=614_000.0,
                            rufus_baked_at=t - 3600, rufus_stale=0, rufus_auto_lock=1, rufus_owner_perp=614_000.0,
                            rufus_owner_pct=0.225)
    await seed_hourly(db, k, 64, t - t % HOUR - HOUR, perp=614_000.0, stake=700_000.0, tao_liq=200_000.0, price=0.067,
                      total_stake=2_800_000.0, block=9_100_000)
    await seed_coverage(db, OWNER, start=t - 40 * DAY, ts=t - 3 * HOUR)
    return k


async def rows(db, k, as_of):
    return {r["metric_key"]: r for r in await db.fetchall(
        "SELECT * FROM metric_values WHERE occupant_key = ? AND as_of = ? AND source = 'integrity'", (k, as_of))}


async def test_features_rows_and_pit(db, iset):                                    # T39
    t = floor_day(T0) + 7 * 60
    k = await world(db, t)
    out = await run_features(db, decision_time=t, as_of=floor_day(t), run_id="f1")
    assert out["occupants"] == 1
    r = await rows(db, k, floor_day(t))
    assert set(r) == set(K.ALL_KEYS) and len(r) == 16
    assert not set(r) & set(RETIRED_METRIC_KEYS) and all(key in METRIC_KEYS for key in r)
    for key, row in r.items():
        vj = json.loads(row["value_json"])
        assert row["stage"] == "raw" and row["spec_version"] == "ts-1.0" and vj["status"] == row["status"]
        assert vj["status"] == "ok" or vj.get("reason")
    assert r["owner_commitment"]["value"] == pytest.approx(0.926286, abs=1e-6)
    assert json.loads(r["owner_commitment"]["value_json"])["auto_lock"] is True
    assert r["owner_retention_30d"]["value"] == pytest.approx(1.0)
    assert r["tenure"]["value"] == pytest.approx((9_100_000 - 4_531_295) * 12 / DAY, abs=0.01)
    assert r["owner_extraction_trigger"]["value"] == 0.0 and r["owner_control_change_trigger"]["value"] == 0.0
    assert r["team_doxxed"]["status"] == "failed" and r["team_verified_count"]["value"] == 0.0
    assert await db.fetchval("SELECT COUNT(*) FROM metric_values WHERE occupant_key LIKE '0:%'") == 0
    assert await db.fetchval("SELECT COUNT(*) FROM metric_values WHERE stage = 'norm'") == 0
    # provisional slot rows never touch the official row
    t6 = floor_day(t) + 6 * HOUR + 7 * 60
    assert slot_as_of(t6) == floor_day(t) + 6 * HOUR and slot_as_of(t) == floor_day(t)
    await run_features(db, decision_time=t6, as_of=slot_as_of(t6), run_id="f2")
    assert (await rows(db, k, floor_day(t)))["owner_commitment"]["run_id"] == "f1"
    assert len(await rows(db, k, floor_day(t) + 6 * HOUR)) == 16


async def test_first_success_wins(db, iset):
    t = floor_day(T0) + 7 * 60
    k = await world(db, t)
    await db.execute("DELETE FROM chain_hourly")                                   # no pool yet → S5 failed
    await run_features(db, decision_time=t, as_of=floor_day(t), run_id="f1")
    r = await rows(db, k, floor_day(t))
    assert r["owner_overhang"]["status"] == "failed" and r["owner_commitment"]["status"] == "ok"
    await seed_hourly(db, k, 64, t - t % HOUR, perp=614_000.0, stake=700_000.0, tao_liq=200_000.0, price=0.067)
    await run_features(db, decision_time=t + 60, as_of=floor_day(t), run_id="f3")
    r = await rows(db, k, floor_day(t))
    assert r["owner_overhang"]["run_id"] == "f3" and r["owner_overhang"]["status"] == "ok"
    assert r["owner_commitment"]["run_id"] == "f1"


async def test_snapshot_pit_and_staleness(db, iset):
    t = floor_day(T0) + 7 * 60
    k = await world(db, t)
    await seed_snapshot(db, k, 64, t + 600, owner_cut_per_block=0.18, owner_cut_enabled=1, owner_total_alpha=1.0,
                        owner_available_alpha=1.0, tmc_owner_perpetual=1.0)        # observed after decision → ignored
    out = await run_features(db, decision_time=t, as_of=floor_day(t), run_id="f", write=False)
    s2 = {r.key: r for r in out["results"][k]}["owner_commitment"]
    assert s2.value_json["total"] == 700_000.0
    late = t + 40 * HOUR                                                           # newest snapshot 40 h old
    await db.execute("DELETE FROM owner_snapshots WHERE observed_at > ?", (t,))
    out = await run_features(db, decision_time=late, as_of=floor_day(late), run_id="g", write=False)
    by = {r.key: r for r in out["results"][k]}
    assert by["control_stability_90d"].status == "failed" and by["control_stability_90d"].reason == "no_owner_snapshot"
    assert by["owner_control_change_trigger"].status == "failed"
    assert by["owner_retention_30d"].value is None


async def test_absence_vs_failure(db, iset):                                       # T40 (owner rows)
    t = floor_day(T0) + 7 * 60
    o = await seed_occ(db, 3, reg=4165565)
    k = o["occupant_key"]
    out = await run_features(db, decision_time=t, as_of=floor_day(t), run_id="a", write=False)
    by = {r.key: r for r in out["results"][k]}
    for key in ("owner_commitment", "owner_retention_30d", "control_stability_90d", "owner_overhang"):
        assert by[key].value is None and by[key].status in ("failed", "not_applicable"), key
    assert by["owner_identity"].status == "failed"
    await seed_snapshot(db, k, 3, t - 600, owner_cut_per_block=0.18, owner_cut_enabled=1, owner_total_alpha=0.0,
                        owner_available_alpha=0.0, tmc_owner_perpetual=0.0)
    await seed_hourly(db, k, 3, t - t % HOUR - HOUR, perp=0.0, stake=0.0, tao_liq=74_000.0, price=0.0245)
    out = await run_features(db, decision_time=t, as_of=floor_day(t), run_id="b", write=False)
    by = {r.key: r for r in out["results"][k]}
    assert by["owner_commitment"].value == 0.0 and by["owner_commitment"].status == "ok"   # SN3: lock 0, age > 90 d
    assert by["owner_overhang"].value == pytest.approx(1.0) and by["owner_overhang"].status == "ok"   # pre-dTAO reg
    assert by["owner_overhang"].value_json["legacy_unaccounted"] is False
    from app.util.time import parse_iso
    await db.execute("UPDATE occupants SET registered_ts = ?, registered_at_est = ? WHERE occupant_key = ?",
                     (parse_iso("2026-03-01T00:00:00Z"),) * 2 + (k,))
    out = await run_features(db, decision_time=t, as_of=floor_day(t), run_id="c", write=False)
    s5 = {r.key: r for r in out["results"][k]}["owner_overhang"]
    assert s5.value == pytest.approx(0.8) and s5.value_json["legacy_unaccounted"] is True     # −0.2 (T30)
    assert by["owner_retention_30d"].reason == "activity_coverage"


async def test_catchup_backfilled(db, iset, rt):                                   # T41
    from app.integrity.features import IntegrityFeaturesJob
    from app.jobs import SPEC_BY_ID, RunContext
    import app.config as app_config
    t = floor_day(T0) + 7 * 60
    k = await world(rt.db, t)
    assert await missing_official_days(rt.db, now=t, days=7) == [floor_day(t) - d * DAY for d in range(7, 0, -1)]
    job = IntegrityFeaturesJob(rt, SPEC_BY_ID["integrity_features"])
    ctx = RunContext(run_id="cu", started_at=t, trigger="schedule", rt=rt, settings=app_config.settings, log=None)
    out = await job.collect(ctx)
    assert "catchup_rows" in out.note
    bf = await rt.db.fetchall("SELECT DISTINCT as_of, backfilled, json_extract(value_json, '$.backfilled') AS b FROM "
                              "metric_values WHERE occupant_key = ? AND as_of < ?", (k, floor_day(t)))
    assert bf and all(r["backfilled"] == 1 and r["b"] == 1 for r in bf)
    first = floor_day(t) - 7 * DAY
    assert {r["as_of"] for r in bf} <= {first + d * DAY for d in range(7)}
    snap0 = await rt.db.fetchval("SELECT MIN(observed_at) FROM owner_snapshots")
    assert min(r["as_of"] for r in bf) >= floor_day(snap0)
