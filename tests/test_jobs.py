"""test_jobs / test_scheduler / test_liveness / test_control (spec 01 §9, 16 §9)."""
import asyncio
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from apscheduler.triggers.cron import CronTrigger

import app.config as app_config
from app import control
from app.jobs import (JOB_TABLE, RETIRED_JOB_IDS, SPEC_BY_ID, CollectorJob, JobOutcome, JobRegistry, JobSpecMismatch,
                      LongTask, _plan_catchup, _catchup_time, build_trigger, declared_counts, previous_fire_time,
                      setup_scheduler, spec_liveness_max_age, stale_ids, trigger_interval_min)
from app.ratelimit import BudgetExhausted
from app.util.time import now_ts

UTC = timezone.utc


class FakeJob(CollectorJob):
    behaviour = "ok"

    async def collect(self, run):
        b = self.behaviour
        if b == "ok":
            return JobOutcome(rows=3, note="n", meta={"a": 1})
        if b == "raise":
            raise ValueError("bad")
        if b == "budget":
            run.rows_committed = 2
            raise BudgetExhausted("tmc", "hour", now_ts() + 60)
        if b == "slow":
            await asyncio.sleep(5)
        if b == "cancel":
            raise asyncio.CancelledError()
        return JobOutcome()


def mk(rt, spec_id="log_retention_daily", **changes):
    sp = replace(SPEC_BY_ID[spec_id], **changes) if changes else SPEC_BY_ID[spec_id]
    return FakeJob(rt, sp)


async def runs(rt):
    return await rt.db.fetchall("SELECT * FROM collector_runs ORDER BY started_at, rowid", writer=True)


# ── wrapper ─────────────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_success_and_failure_and_alerts(rt):
    j = mk(rt, "tmc_hourly", enabled_flags=(), providers=())
    await j.run("cli")
    r = (await runs(rt))[-1]
    assert r["ok"] == 1 and r["rows"] == 3 and json.loads(r["meta_json"]) == {"a": 1}
    assert isinstance(r["started_at"], int)
    j.behaviour = "raise"
    for _ in range(5):
        await j.run("cli")                                                    # never raises
    assert j._status.consecutive_failures == 5
    ev = await rt.db.fetchall("SELECT * FROM events WHERE event_type='collector_failing'", writer=True)
    assert len(ev) == 1
    p = json.loads(ev[0]["payload"])
    assert {"collector", "consecutive_failures", "last_error", "job"} <= set(p)
    j.behaviour = "ok"
    await j.run("cli")
    rec = await rt.db.fetchall("SELECT payload FROM events WHERE event_type='collector_recovered'", writer=True)
    assert len(rec) == 1 and json.loads(rec[0]["payload"])["failures"] == 5
    assert j._status.consecutive_failures == 0


@pytest.mark.asyncio
async def test_budget_timeout_cancel(rt):
    j = mk(rt, "tmc_hourly", enabled_flags=(), providers=(), timeout_sec=1)
    j.behaviour = "budget"
    await j.run("cli")
    r = (await runs(rt))[-1]
    assert r["ok"] == 1 and r["note"].startswith("paused:budget") and r["rows"] == 2
    assert j._status.consecutive_failures == 0
    j.behaviour = "slow"
    await j.run("cli")
    r = (await runs(rt))[-1]
    assert r["ok"] == 0 and "timeout" in r["error"]
    j.behaviour = "cancel"
    with pytest.raises(asyncio.CancelledError):
        await j.run("cli")


@pytest.mark.asyncio
async def test_skips(rt, settings_override, tmp_root):
    j = mk(rt, "log_retention_daily")
    settings_override(LOG_RETENTION_ENABLED=False, KILL_SWITCH_PATH=str(tmp_root / "KILL_SWITCH"),
                      PAUSE_ALL_PATH=str(tmp_root / "PAUSE_ALL"), CONTROL_DIR=str(tmp_root / "data" / "control"),
                      RUN_DIR=str(tmp_root / "data" / "run"))
    await j.run("cli")
    assert j._status.last_skip_reason == "disabled" and await runs(rt) == []
    settings_override(KILL_SWITCH_PATH=str(tmp_root / "KILL_SWITCH"), PAUSE_ALL_PATH=str(tmp_root / "PAUSE_ALL"),
                      CONTROL_DIR=str(tmp_root / "data" / "control"), RUN_DIR=str(tmp_root / "data" / "run"))
    (tmp_root / "PAUSE_ALL").write_text("")
    await j.run("cli")                                                        # ops_exempt ignores PAUSE_ALL
    assert len(await runs(rt)) == 1
    k = mk(rt, "tmc_hourly", enabled_flags=())
    await k.run("cli")
    assert k._status.last_skip_reason == "paused:pause_all"
    (tmp_root / "PAUSE_ALL").unlink()
    (tmp_root / "KILL_SWITCH").write_text("")
    paid = mk(rt, "x_team_poll", enabled_flags=(), enabled_fn=None, providers=())
    await paid.run("cli")
    assert paid._status.last_skip_reason == "paused:kill_switch"
    await k.run("cli")
    assert k._status.last_skip_reason is None                                 # free job keeps running
    (tmp_root / "KILL_SWITCH").unlink()
    # recent skip only for scheduled interval runs
    j2 = mk(rt, "web_check", enabled_flags=(), providers=())
    await j2.run("schedule")
    await j2.run("schedule")
    assert j2._status.last_skip_reason == "recent"
    await j2.run("manual")
    assert j2._status.last_skip_reason is None


@pytest.mark.asyncio
async def test_in_flight_and_cost_reconciliation(rt):
    j = mk(rt, "tmc_hourly", enabled_flags=(), providers=("anthropic",), timeout_sec=5)
    j.behaviour = "slow"
    t = asyncio.create_task(j.run("cli"))
    await asyncio.sleep(0.05)
    await j.run("cli")
    assert j._status.last_skip_reason == "in_flight"
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t

    class CostJob(CollectorJob):
        async def collect(self, run):
            await run.rt.costs.record("anthropic", 0.30, job="x", run_id=run.run_id)
            return JobOutcome(cost_usd=0.50)
    cj = CostJob(rt, replace(SPEC_BY_ID["tmc_hourly"], enabled_flags=(), providers=("anthropic",)))
    await cj.run("cli")
    usd = await rt.db.fetchval("SELECT usd FROM provider_spend WHERE provider='anthropic'", writer=True)
    assert usd == pytest.approx(0.50)
    r = (await runs(rt))[-1]
    assert r["cost_usd"] == pytest.approx(0.50)


@pytest.mark.asyncio
async def test_db_failure_does_not_escape(rt, monkeypatch):
    j = mk(rt, "tmc_hourly", enabled_flags=(), providers=())

    async def boom(*a, **k):
        raise RuntimeError("db locked")
    monkeypatch.setattr(rt.db, "execute", boom)
    await j.run("cli")
    assert j._status.last_ok is not None


# ── scheduler / registry ────────────────────────────────────────────────────────────────────────────────────
def test_job_table_shape():
    ids = [s.id for s in JOB_TABLE]
    assert len(ids) == 58 == len(set(ids))
    assert sum(1 for s in JOB_TABLE if s.kind == "longtask") == 3
    assert not set(ids) & RETIRED_JOB_IDS
    counts = declared_counts()
    assert counts == {0: 7, 1: 2, 2: 7, 3: 5, 4: 6, 5: 3, 6: 4, 7: 5, 8: 2, 9: 12, 10: 1, 11: 4}


@pytest.mark.asyncio
async def test_registry_built_phases(rt):
    JobRegistry.build(rt)
    from tests.conftest import BUILT_PHASE, EARLY_JOBS, EXPECTED_AVAILABLE
    assert {"log_retention_daily", "storage_retention_daily", "backup_daily", "ops_guard_5m", "ops_digest_daily",
            "cost_ledger_hourly", "alert_dispatch"} <= set(rt.jobs)
    assert len(rt.jobs) + len(rt.tasks) == EXPECTED_AVAILABLE and len(rt.unavailable) == 58 - EXPECTED_AVAILABLE
    assert all(SPEC_BY_ID[j].phase <= BUILT_PHASE or j in EARLY_JOBS for j in [*rt.jobs, *rt.tasks])


@pytest.mark.asyncio
async def test_registry_mismatch(rt, stub_module):
    class Bad(CollectorJob):
        timeout_sec = 999

        async def collect(self, run):
            return JobOutcome()
    stub_module("registry.jobs", "RegistryOccupantsJob", Bad)
    with pytest.raises(JobSpecMismatch):
        JobRegistry.build(rt)


def test_triggers():
    t = build_trigger(SPEC_BY_ID["scoring_6h"])
    assert isinstance(t, CronTrigger) and str(t.fields[5]) == "0,6,12,18" and str(t.fields[6]) == "15"
    assert str(build_trigger(SPEC_BY_ID["discord_6h"]).fields[6]) == "0"
    q = build_trigger(SPEC_BY_ID["quant_weekly"])
    assert str(q.fields[4]) == "sun" and str(q.fields[5]) == "5"
    assert trigger_interval_min(SPEC_BY_ID["backup_daily"]) == 1440
    assert trigger_interval_min(SPEC_BY_ID["scoring_6h"]) == 360
    assert trigger_interval_min(SPEC_BY_ID["quant_weekly"]) == 10080
    assert spec_liveness_max_age(SPEC_BY_ID["backfill_tmc"]) is None


@pytest.mark.asyncio
async def test_setup_scheduler_stagger_and_manual(rt, stub_module):
    class Any_(CollectorJob):
        async def collect(self, run):
            return JobOutcome()
    for sp in JOB_TABLE:
        if sp.kind != "longtask":
            mod, _, cls = sp.factory.partition(":")
            stub_module(mod, cls, Any_)
    JobRegistry.build(rt)
    boot = datetime(2026, 9, 24, 12, 0, tzinfo=UTC)
    sched = setup_scheduler(rt, now=boot)
    from tests.conftest import BUILT_PHASE
    expect = ({"telegram_bot"} if BUILT_PHASE < 11 else set()) | ({"chain_sse"} if BUILT_PHASE < 2 else set()) | ({"discord_gateway"} if BUILT_PHASE < 6 else set())
    assert set(rt.unavailable) == expect
    assert sched.get_job("backfill_tmc").next_run_time is None
    staggered = [sp.id for sp in JOB_TABLE if sp.kind == "interval" and sp.first_run == "stagger"]
    times = [sched.get_job(i).next_run_time for i in staggered]
    assert staggered[0] == "registry_occupants" and times == sorted(times) and len(set(times)) == len(times)
    assert sched.get_job("alert_dispatch").next_run_time == boot + timedelta(seconds=30)


@pytest.mark.asyncio
async def test_startup_catchup(rt, stub_module, settings_override):
    class Any_(CollectorJob):
        async def collect(self, run):
            return JobOutcome()
    stub_module("collectors.chain_tmc_rest", "TmcDailyJob", Any_)
    stub_module("integrity.owner_collect", "OwnerCollectJob", Any_)
    JobRegistry.build(rt)
    boot = datetime(2026, 9, 24, 12, 0, tzinfo=UTC)                          # missed 02:10Z
    sched = setup_scheduler(rt, now=boot)
    for fn in rt.pending_setup:
        await fn()
    cj = sched.get_job("tmc_daily:catchup")
    assert cj is not None and cj.trigger.run_date == boot + timedelta(seconds=300)
    assert sched.get_job("owner_collect_daily:catchup") is None               # only inside 20:30-23:15Z
    late = datetime(2026, 9, 24, 21, 0, tzinfo=UTC)
    assert _catchup_time(SPEC_BY_ID["tmc_daily"], late) == datetime(2026, 9, 24, 23, 35, tzinfo=UTC)
    assert _catchup_time(SPEC_BY_ID["owner_collect_daily"], late) == late + timedelta(seconds=300)
    trig = build_trigger(SPEC_BY_ID["tmc_daily"])
    assert previous_fire_time(trig, boot) == datetime(2026, 9, 24, 2, 10, tzinfo=UTC)


# ── liveness ────────────────────────────────────────────────────────────────────────────────────────────────
class FakeTask(LongTask):
    async def run_once(self, stop):
        await stop.wait()


@pytest.mark.asyncio
async def test_liveness(rt, tmp_root, settings_override):
    j = mk(rt, "ops_guard_5m")
    rt.jobs = {"ops_guard_5m": j}
    now = rt.started_at
    assert stale_ids(rt, now + 700) == []
    assert stale_ids(rt, now + 800) == ["ops_guard_5m"]
    j._status.last_run = now + 700
    assert stale_ids(rt, now + 800) == []
    (tmp_root / "KILL_SWITCH").write_text("")
    assert stale_ids(rt, now + 5000) == ["ops_guard_5m"]                     # kill switch does not force live
    (tmp_root / "KILL_SWITCH").unlink()
    rt.jobs = {}
    t = FakeTask(rt, replace(SPEC_BY_ID["chain_sse"], enabled_flags=()))
    rt.tasks = {"chain_sse": t}
    t.last_beat = now - 1200
    t.last_attempt = now + 950
    assert stale_ids(rt, now + 1000) == []
    t.last_attempt = None
    assert stale_ids(rt, now + 1000) == ["chain_sse"]
    rt.tasks = {"telegram_bot": FakeTask(rt, replace(SPEC_BY_ID["telegram_bot"], enabled_flags=()))}
    assert stale_ids(rt, now + 10**7) == []
    rt.tasks = {}
    rt.api_probe_fails = 3
    assert stale_ids(rt, now) == ["api_probe"]
    rt.api_probe_fails = 0
    done = asyncio.get_running_loop().create_future()
    done.set_result(None)
    rt.task_handles["health"] = done
    assert stale_ids(rt, now) == ["api_task"]


# ── control (16 §9 test_control) ────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_control_pause_resume(rt, tmp_root):
    assert control.resolve_targets("github_resolve") == ["github_resolve"]
    assert "github_resolve" in control.resolve_targets("github")
    for bad in ("github_", "tmc_d", "telegram", "tme"):
        with pytest.raises(control.UnknownTarget):
            control.resolve_targets(bad)
    assert "chain_sse" not in control.resolve_targets("tmc") and "chain_sse" in control.resolve_targets("chain")
    await control.pause("github", by="test", db=rt.db)
    gj = mk(rt, "github_resolve", enabled_flags=(), providers=())
    tj = mk(rt, "tmc_daily", enabled_flags=(), providers=())
    assert gj.paused_reason() == "operator" and tj.paused_reason() is None
    assert await rt.db.fetchval("SELECT COUNT(*) FROM job_state WHERE key='paused'", writer=True) == 0
    await control.pause("github_resolve", by="test", db=rt.db)
    res = await control.resume("github_resolve", by="test", db=rt.db)
    assert res["still_paused_by"] == "github"
    await control.resume("github", by="test", db=rt.db)
    assert gj.paused_reason() is None
    (tmp_root / "data" / "control" / "github.paused").write_text("")
    assert gj.paused_reason() == "emergency_file"
    await control.pause("github", by="test", db=rt.db)
    res = await control.resume("github", by="test", db=rt.db)
    assert "file_pause_active" in res and (tmp_root / "data" / "control" / "github.paused").exists()
    (tmp_root / "data" / "control" / "github.paused").unlink()
    (tmp_root / "data" / "control" / "foo.paused").write_text("")
    assert control.scan_unknown_pause_files() == ["foo.paused"] and gj.paused_reason() is None
    ex = mk(rt, "backup_daily")
    await control.pause("backup_daily", by="test", db=rt.db)                 # exact id pauses an ops_exempt job
    assert ex.paused_reason() == "operator"
    await control.resume("backup_daily", by="test", db=rt.db)
    await rt.db.execute("INSERT INTO control_state (target, key, value, set_by, set_at, expires_at) "
                        "VALUES ('web','paused','true','sql',1,2)")
    await control.refresh_cache(rt.db)
    assert mk(rt, "web_check", enabled_flags=()).paused_reason() is None      # expired row ignored


def test_pause_groups_static():
    ids = {s.id for s in JOB_TABLE}
    exempt = control.never_grouped()
    covered = set()
    for g, members in control.PAUSE_GROUPS.items():
        assert members <= ids, g
        assert not members & exempt, g
        covered |= members
    assert not set(control.PAUSE_GROUPS) & ids
    assert ids - exempt <= covered


@pytest.mark.asyncio
async def test_integrity_hold(rt, tmp_root):
    (tmp_root / "data" / "run" / "integrity.json").write_text(json.dumps({"result": "failed"}))
    assert control.pause_all_active()
    assert mk(rt, "tmc_daily", enabled_flags=()).paused_reason() == "pause_all"
    assert mk(rt, "backup_daily").paused_reason() is None
    assert mk(rt, "storage_retention_daily").paused_reason() == "pause_all"
    (tmp_root / "data" / "run" / "integrity.json").write_text(json.dumps({"result": "ok"}))
    assert not control.pause_all_active()
