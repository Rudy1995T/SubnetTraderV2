"""``quant_weekly`` (spec 13 §8.8): niced children, exit-code handling, 01 wrapper integration, pauses, /health."""
from __future__ import annotations

import asyncio
import json

import pytest
from apscheduler.triggers.cron import CronTrigger

from app import control
from app.jobs import SPEC_BY_ID
from app.quant.weekly_job import QuantWeeklyJob


class FakeProc:
    def __init__(self, code, stdout="", hang=False):
        self.returncode = None
        self._code, self._out, self._hang = code, stdout, hang
        self.killed = False

    async def communicate(self):
        if self._hang:
            await asyncio.sleep(3600)
        self.returncode = self._code
        return self._out.encode(), b"boom: details"

    def kill(self):
        self.killed = True
        self.returncode = -9

    async def wait(self):
        return self.returncode


def _result(files, verdict=None, gate_pass=None):
    return "RESULT " + json.dumps({"files": files, "verdict_file": verdict, "gate_pass": gate_pass,
                                   "insufficient_data": False})


@pytest.fixture
def job(rt, settings_override, tmp_root, monkeypatch):
    import app.config as app_config
    s = app_config.Settings(**{**app_config.settings.model_dump(), "QUANT_WEEKLY_JOB_ENABLED": True})
    monkeypatch.setattr(app_config, "settings", s)
    return QuantWeeklyJob(rt, SPEC_BY_ID["quant_weekly"])


def _patch_children(monkeypatch, procs):
    calls = []

    async def fake_exec(*args, **kw):
        calls.append(args)
        return procs.pop(0)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    return calls


@pytest.mark.asyncio
async def test_pipeline_niced_and_outcome(job, monkeypatch):
    calls = _patch_children(monkeypatch, [
        FakeProc(0, _result(["/s/quant_x.db"])),
        FakeProc(1, _result(["/r/f.json", "/r/v.json"], "/r/v.json", False)),
        FakeProc(0, _result(["/r/rs.json"]))])
    await job.run("manual")
    assert all(c[:3] == ("nice", "-n", "10") and c[4:6] == ("-m", "app.quant") for c in calls)
    assert calls[1][6:] == ("report", "--run-all", "--db", "/s/quant_x.db")
    row = await job.rt.db.fetchone("SELECT ok, rows, note, meta_json FROM collector_runs WHERE job='quant_weekly'",
                                   writer=True)
    assert row["ok"] == 1 and row["rows"] == 3 and row["note"].startswith("gate_pass=False verdict=/r/v.json")
    assert json.loads(row["meta_json"])["exit_codes"] == {"snapshot": 0, "report": 1, "rank_stability": 0}
    st = job.status()
    assert st["last_gate_pass"] is False and st["last_verdict_file"] == "/r/v.json"


@pytest.mark.asyncio
async def test_child_exit_2_fails_and_alert_threshold(job, monkeypatch, alerts_sink):
    for _ in range(3):
        _patch_children(monkeypatch, [FakeProc(2, "")])
        await job.run("manual")
    rows = await job.rt.db.fetchall("SELECT ok, error FROM collector_runs WHERE job='quant_weekly'", writer=True)
    assert [r["ok"] for r in rows] == [0, 0, 0] and "exit 2" in rows[0]["error"]
    assert job.fail_alert_threshold() == 2
    n = await job.rt.db.fetchval("SELECT COUNT(*) FROM events WHERE event_type='collector_failing'", writer=True)
    assert n == 1


@pytest.mark.asyncio
async def test_cancellation_kills_child(job, monkeypatch):
    p = FakeProc(0, hang=True)
    _patch_children(monkeypatch, [p])
    task = asyncio.create_task(job.collect(None))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert p.killed


@pytest.mark.asyncio
async def test_pauses_and_trigger(job, tmp_root):
    (tmp_root / "data" / "control" / "quant_weekly.paused").write_text("")
    assert job.paused_reason() == "emergency_file"
    (tmp_root / "data" / "control" / "quant_weekly.paused").unlink()
    await control.pause("quant_weekly", by="test", db=job.rt.db)
    assert job.paused_reason() == "operator"
    await control.resume("quant_weekly", by="test", db=job.rt.db)
    assert job.paused_reason() is None
    import app.config as app_config
    trig = job.spec.trigger(app_config.settings)
    assert isinstance(trig, CronTrigger)
    assert str(trig) == str(CronTrigger(day_of_week="sun", hour=5, minute=0, timezone="UTC"))
    assert job.effective_timeout() == app_config.settings.QUANT_WEEKLY_JOB_TIMEOUT_S
