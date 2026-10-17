"""test_health_api / test_runtime_shutdown / test_cli / test_smoke (spec 01 §9)."""
import asyncio
import json
import os
import socket
from app.storage.migrate import CODE_SCHEMA_VERSION
import subprocess
import sys
from pathlib import Path

import httpx
import pytest
import respx

import app.config as app_config
from app.jobs import JOB_TABLE, JobRegistry, setup_scheduler
from tests.conftest import BUILT_PHASE, EXPECTED_AVAILABLE

ROOT = Path(__file__).resolve().parents[1]


async def _ready(rt):
    JobRegistry.build(rt)
    sched = setup_scheduler(rt)
    sched.start(paused=True)
    return sched


@pytest.mark.asyncio
async def test_health_document(rt, app_client, tmp_root):
    r = await app_client.get("/health")
    h = r.json()
    assert h["status"] == "initializing"                                     # no scheduler yet
    sched = await _ready(rt)
    try:
        r = await app_client.get("/health")
        assert '"status":"ok"' in r.text or '"status":"degraded"' in r.text
        h = r.json()
        for k in ("status", "timestamp", "version", "spec_version", "uptime_sec", "degraded_reasons",
                  "kill_switch_active", "pause_all_active", "scoring_killed", "scheduler", "collectors", "tasks",
                  "spend", "rate_limits", "db", "alerts", "scores", "ops", "telegram_configured"):
            assert k in h, k
        assert h["scheduler"]["declared"] == 57 and h["scheduler"]["available"] == EXPECTED_AVAILABLE
        assert h["spend"] == rt.costs.snapshot()
        for g in ("x", "llm", "taostats", "tmc"):
            assert {"usd", "cap_usd", "pct", "paused", "reserved_usd"} <= set(h["spend"][g])
        assert h["status"] == "ok", h["degraded_reasons"]
        (tmp_root / "PAUSE_ALL").write_text("")
        h = (await app_client.get("/health")).json()
        assert h["status"] == "degraded" and "pause_all" in h["degraded_reasons"] and h["pause_all_active"]
        (tmp_root / "PAUSE_ALL").unlink()
        (tmp_root / "KILL_SWITCH").write_text("")
        h = (await app_client.get("/health")).json()
        assert h["kill_switch_active"] is True and h["status"] == "ok"
        (tmp_root / "KILL_SWITCH").unlink()
        rt.jobs["ops_guard_5m"]._status.consecutive_failures = 5
        h = (await app_client.get("/health")).json()
        assert "collector_failing:ops_guard_5m" in h["degraded_reasons"]
        rt.jobs["ops_guard_5m"]._status.consecutive_failures = 0
        rt.jobs["ops_guard_5m"].status = lambda: 1 / 0                        # a failing block never 500s
        r = await app_client.get("/health")
        assert r.status_code == 200 and "error" in r.json()["collectors"]["ops_guard_5m"]
    finally:
        sched.shutdown(wait=False)


@pytest.mark.asyncio
async def test_services_and_jobs(rt, app_client):
    sched = await _ready(rt)
    try:
        s = (await app_client.get("/api/health/services")).json()["services"]
        assert s["database"]["ok"] and s["scheduler"]["ok"] and "disk" in s
        jobs = (await app_client.get("/api/v1/jobs")).json()["jobs"]
        assert len(jobs) == 57 and sum(1 for j in jobs if j["available"]) == EXPECTED_AVAILABLE
    finally:
        sched.shutdown(wait=False)


@pytest.mark.asyncio
async def test_control_routes(rt, app_client, settings_override, tmp_root):
    sched = await _ready(rt)
    import app.api.app  # noqa: F401
    s = app_config.settings
    settings_override(API_AUTH_TOKEN="tok", **{k: getattr(s, k) for k in (
        "DB_PATH", "JSONL_DIR", "DB_BACKUP_DIR", "RUN_DIR", "CONTROL_DIR", "DATA_DIR", "KILL_SWITCH_PATH",
        "PAUSE_ALL_PATH", "SCORING_KILL_SWITCH_PATH")})
    try:
        assert (await app_client.post("/api/v1/control/pause/backup_daily")).status_code == 401
        H = {"X-Auth-Token": "tok"}
        r = await app_client.post("/api/v1/control/pause/backup_daily", headers=H)
        assert r.status_code == 200 and r.json()["paused"]
        row = await rt.db.fetchone("SELECT target, key, value, set_by FROM control_state", writer=True)
        assert row == {"target": "backup_daily", "key": "paused", "value": "true", "set_by": "api"}
        assert await rt.db.fetchval("SELECT COUNT(*) FROM job_state WHERE key='paused'", writer=True) == 0
        assert (await app_client.post("/api/v1/control/pause/github_", headers=H)).status_code == 404
        r = await app_client.post("/api/v1/control/pause/community", headers=H)
        assert "telegram_bot" not in r.json()["jobs"]
        (tmp_root / "data" / "control" / "backup_daily.paused").write_text("")
        r = await app_client.post("/api/v1/control/resume/backup_daily", headers=H)
        assert r.json()["file_pause_active"].endswith("backup_daily.paused")
        (tmp_root / "data" / "control" / "backup_daily.paused").unlink()
        r = await app_client.post("/api/v1/control/run/log_retention_daily", headers=H)
        assert r.status_code == 200
        await asyncio.sleep(0.2)
        n = await rt.db.fetchval("SELECT COUNT(*) FROM collector_runs WHERE job='log_retention_daily'", writer=True)
        assert n == 1
        from app.jobs import JOB_TABLE
        from tests.conftest import BUILT_PHASE
        unbuilt = next(sp.id for sp in JOB_TABLE if sp.phase > BUILT_PHASE and sp.kind != "longtask")
        assert (await app_client.post(f"/api/v1/control/run/{unbuilt}", headers=H)).status_code == 404  # not built
        r = await app_client.post("/api/v1/control/run/registry_occupants", headers=H)                      # disabled
        assert r.status_code == 409 and r.json()["detail"] == "disabled"
        r = await app_client.post("/api/v1/control/reload-config", headers=H)
        assert r.status_code == 200 and "restart_required" in r.json()
    finally:
        sched.shutdown(wait=False)


# ── runtime shutdown ────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_shutdown_services(rt, monkeypatch, tmp_root):
    import app.runtime as R
    from app.ops import recovery
    calls = []
    monkeypatch.setattr(R, "_hard_exit", lambda code=0: calls.append(code))

    class _T:
        def cancel(self):
            calls.append("deadline_cancel")
    monkeypatch.setattr(R, "_arm_shutdown_deadline", lambda s: _T())
    recovery.write_heartbeat(rt)
    assert recovery.heartbeat_path().exists()
    flushed = []
    orig_flush = rt.costs.flush

    async def fl():
        flushed.append(1)
        return await orig_flush()
    rt.costs.flush = fl
    await R.shutdown_services(rt)
    assert calls[-1] == 0 and calls.count(0) == 1 and flushed == [1]
    assert not recovery.heartbeat_path().exists() and not rt.db.connected


@pytest.mark.asyncio
async def test_close_within_abandons_slow_close():
    import time

    import app.runtime as R

    async def slow():
        await asyncio.sleep(30)
    t0 = time.monotonic()
    await R._close_within("slow", slow(), 0.3)
    assert time.monotonic() - t0 < 1.0


# ── CLI ─────────────────────────────────────────────────────────────────────────────────────────────────────
def _cli(tmp_path, *args):
    env = dict(os.environ, DB_PATH=str(tmp_path / "c.db"), RUN_DIR=str(tmp_path / "run"),
               JSONL_DIR=str(tmp_path / "logs"), DB_BACKUP_DIR=str(tmp_path / "bk"), TAOSENTINEL_ROOT=str(tmp_path))
    return subprocess.run([sys.executable, "-m", "app", *args], cwd=ROOT, env=env, capture_output=True, text=True,
                          timeout=120)


def test_cli(tmp_path):
    r = _cli(tmp_path, "check")
    assert r.returncode == 0, r.stderr
    assert f"schema_version={CODE_SCHEMA_VERSION}" in r.stdout and "sqlite_version=" in r.stdout
    assert (f"OK jobs=57 declared (54 jobs + 3 LongTasks), {EXPECTED_AVAILABLE} available (phase {BUILT_PHASE}), "
            f"{57 - EXPECTED_AVAILABLE} unavailable") in r.stdout
    r = _cli(tmp_path, "jobs")
    assert r.returncode == 0 and len([l for l in r.stdout.splitlines()[1:] if l.strip()]) == 57
    r = _cli(tmp_path, "run-job", "log_retention_daily")
    assert r.returncode == 0, r.stdout + r.stderr
    r = _cli(tmp_path, "run-job", "backfill_github")
    assert r.returncode == 1 and "unavailable" in r.stdout
    r = _cli(tmp_path, "db", "retention", "--dry-run")
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert "maintenance" in out and "chain_prune" not in out                  # no backup yet → destructive gated


# ── smoke ───────────────────────────────────────────────────────────────────────────────────────────────────
def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.mark.smoke
@pytest.mark.asyncio
async def test_smoke_main(tmp_root, settings_override, monkeypatch):
    import app.runtime as R
    port = _free_port()
    s = app_config.settings
    settings_override(HEALTH_PORT=port, TELEGRAM_BOT_TOKEN="1:T", TELEGRAM_CHAT_ID="9", TELEGRAM_ENABLED=True,
                      **{k: getattr(s, k) for k in ("DB_PATH", "JSONL_DIR", "DB_BACKUP_DIR", "RUN_DIR", "CONTROL_DIR",
                                                    "DATA_DIR", "KILL_SWITCH_PATH", "PAUSE_ALL_PATH",
                                                    "SCORING_KILL_SWITCH_PATH")})
    events = []
    monkeypatch.setattr(R, "sd_notify", lambda st: events.append(st))
    exits = []
    monkeypatch.setattr(R, "_hard_exit", lambda code=0: exits.append(code))

    class _T:
        def cancel(self):
            pass
    monkeypatch.setattr(R, "_arm_shutdown_deadline", lambda s: _T())
    with respx.mock(assert_all_mocked=False, assert_all_called=False) as m:
        tg = m.post("https://api.telegram.org/bot1:T/sendMessage").mock(return_value=httpx.Response(200))
        m.route(host="127.0.0.1").pass_through()
        task = asyncio.create_task(R.main(None))
        status = None
        for _ in range(50):
            await asyncio.sleep(0.2)
            try:
                async with httpx.AsyncClient() as c:
                    status = (await c.get(f"http://127.0.0.1:{port}/health")).json()["status"]
                if status == "ok":
                    break
            except Exception:
                continue
        assert status == "ok"
        assert events and events[0] == "READY=1"
        rt = R.get_runtime()
        assert len(rt.jobs) + len(rt.tasks) == EXPECTED_AVAILABLE
        body = json.loads(tg.calls[0].request.content)["text"]
        assert "TaoSentinel online" in body and f"{EXPECTED_AVAILABLE} available / 57 declared" in body
        rt.shutdown_event.set()
        await asyncio.wait_for(task, 12)
    assert exits == [0]
    R.runtime = None
