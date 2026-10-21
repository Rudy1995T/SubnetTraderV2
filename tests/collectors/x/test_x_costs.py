"""Spec 09 §9.1 test_x_costs: check before every paid call, record(…, requests=0, …) after it (16's keyword-only
signature), denial → paused:budget + one x_budget_paused (none for kill_switch), x_budget_resumed on the next allowed
check, top-20 degradation, no soft alert, JobOutcome.cost_usd = Σ recorded USD."""
from __future__ import annotations

import json

import pytest

from tests.collectors.x.conftest import (DAY, NOW, Decision, FakeGovernor, FakeProvider, make_collector, post,
                                         seed_account, seed_occupant, seed_x_watch)


async def _accounts(db, n=3):
    for i in range(n):
        o = await seed_occupant(db, 20 + i, name=f"Name{20 + i}")
        await seed_x_watch(db, o, f"team_{i}")
        await seed_account(db, str(100 + i), f"team_{i}", okeys=[o])


def _tl(n=3):
    return {str(100 + i): [[post(NOW - 60 * (i + 1), str(100 + i), f"team_{i}", f"post {i}", seq=i)]] for i in range(n)}


@pytest.mark.asyncio
async def test_check_before_every_call_and_record_after(db, xset):
    xset()
    await _accounts(db)
    fp = FakeProvider(timelines=_tl(), cost_per_call=0.002)
    gov = FakeGovernor()
    out = await make_collector(db, fp, gov).run_team_poll()
    assert len(gov.checks) == 3 == len(fp.calls) and gov.order == ["check", "record"] * 3
    assert all(r["requests"] == 0 and r["provider"] == "twitterapi_io" and r["job"] == "x_team_poll"
               and r["run_id"] == "run-1" and r["units"] == {"posts": 1} for r in gov.records)
    assert out.cost_usd == pytest.approx(sum(r["usd"] for r in gov.records)) == pytest.approx(0.006)


@pytest.mark.asyncio
async def test_record_is_keyword_only():
    gov = FakeGovernor()
    with pytest.raises(TypeError):
        await gov.record("twitterapi_io", 0.1, 0)  # type: ignore[misc]


@pytest.mark.asyncio
async def test_hard_cap_denial_stops_and_emits_once(db, xset):
    xset()
    await _accounts(db)
    fp = FakeProvider(timelines=_tl())
    gov = FakeGovernor([Decision(True), Decision(False, "hard_cap", usd_mtd=25.0, cap_usd=25.0)])
    out = await make_collector(db, fp, gov).run_team_poll()
    assert len(fp.calls) == 1 and len(gov.records) == 1
    assert out.note.startswith("paused:budget reason=hard_cap")
    assert await db.fetchval("SELECT COUNT(*) FROM social_posts") == 1             # what was fetched is written
    ev = await db.fetchall("SELECT * FROM events WHERE event_type = 'x_budget_paused'")
    assert len(ev) == 1 and ev[0]["dedupe_key"] == "twitterapi_io|2026-10" and ev[0]["occupant_key"] is None
    assert json.loads(ev[0]["payload"])["reason"] == "hard_cap"
    gov2 = FakeGovernor([Decision(False, "hard_cap", usd_mtd=25.0, cap_usd=25.0)])
    out2 = await make_collector(db, fp, gov2, run_id="run-2").run_team_poll()
    assert out2.note.startswith("paused:budget") and len(fp.calls) == 1
    assert await db.fetchval("SELECT COUNT(*) FROM events WHERE event_type = 'x_budget_paused'") == 1
    assert not await db.fetchval("SELECT 1 FROM events WHERE event_type = 'spend_80pct'")           # 16's, never ours


@pytest.mark.asyncio
async def test_kill_switch_denial_emits_nothing(db, xset):
    xset()
    await _accounts(db)
    fp = FakeProvider(timelines=_tl())
    out = await make_collector(db, fp, FakeGovernor([Decision(False, "kill_switch")])).run_team_poll()
    assert fp.calls == [] and out.note.startswith("paused:budget reason=kill_switch")
    assert not await db.fetchval("SELECT 1 FROM events WHERE event_type = 'x_budget_paused'")


@pytest.mark.asyncio
async def test_resumed_on_next_allowed_check(db, xset):
    xset()
    await _accounts(db, 1)
    fp = FakeProvider(timelines=_tl(1))
    await make_collector(db, fp, FakeGovernor([Decision(False, "pacing", projected_usd=30.0)])).run_team_poll()
    nxt = NOW + 20 * DAY                                                              # next month
    await make_collector(db, fp, FakeGovernor(), run_id="run-2", now=nxt).run_team_poll()
    ev = await db.fetchone("SELECT * FROM events WHERE event_type = 'x_budget_resumed'")
    assert ev is not None and json.loads(ev["payload"]) == {"provider": "twitterapi_io", "month": "2026-11"}
    await make_collector(db, fp, FakeGovernor(), run_id="run-3", now=nxt + DAY).run_team_poll()
    assert await db.fetchval("SELECT COUNT(*) FROM events WHERE event_type = 'x_budget_resumed'") == 1


@pytest.mark.asyncio
async def test_projected_over_90pct_degrades_mentions_to_top20(db, xset):
    xset(X_MENTIONS_TOP_N=40)
    keys = [await seed_occupant(db, n, name=f"Name{n}") for n in range(1, 31)]
    for i, k in enumerate(keys):
        await db.execute("INSERT INTO chain_hourly (netuid, hour_ts, occupant_key, event_at, observed_at, source, "
                         "emission) VALUES (?,?,?,?,?,?,?)", (i + 1, NOW - 3600, k, NOW - 3600, NOW, "sse", 100.0 - i))
    fp = FakeProvider()
    gov = FakeGovernor([Decision(True, projected_usd=23.0, cap_usd=25.0)])
    out = await make_collector(db, fp, gov, job_id="x_mentions_poll").run_mentions_poll()
    assert len(fp.calls) == 20 and "degraded=top20" in out.note
    top = json.loads(await db.fetchval("SELECT value_json FROM job_state WHERE job = 'x_mentions_poll' AND "
                                       "key = 'last_top'"))
    assert len(top["keys"]) == 30
    fp2 = FakeProvider()
    await make_collector(db, fp2, FakeGovernor(), job_id="x_mentions_poll", run_id="run-2").run_mentions_poll()
    assert len(fp2.calls) == 20                                               # rest of the month
    later = NOW + 20 * DAY                                                    # new month: the flag is per month
    for i, k in enumerate(keys):
        await db.execute("INSERT INTO chain_hourly (netuid, hour_ts, occupant_key, event_at, observed_at, source, "
                         "emission) VALUES (?,?,?,?,?,?,?)", (i + 1, later - 3600, k, later - 3600, later, "sse", 1.0))
    fp3 = FakeProvider()
    await make_collector(db, fp3, FakeGovernor(), job_id="x_mentions_poll", run_id="run-3",
                         now=later).run_mentions_poll()
    assert len(fp3.calls) == 30


@pytest.mark.asyncio
async def test_real_governor_books_usd_without_requests(rt, xset):
    """16's CostGovernor: record(requests=0) books USD + units; Http alone counts requests."""
    xset()
    await _accounts(rt.db, 1)
    fp = FakeProvider(timelines=_tl(1), cost_per_call=0.003)
    c = make_collector(rt.db, fp, rt.costs)
    out = await c.run_team_poll()
    await rt.costs.flush()
    row = await rt.db.fetchone("SELECT usd, requests, units_json FROM provider_spend WHERE provider = 'twitterapi_io'")
    assert row["usd"] == pytest.approx(0.003) and row["requests"] == 0 and out.cost_usd == pytest.approx(0.003)
    assert json.loads(row["units_json"]).get("posts") == 1
