"""Spec 16 §9 (subset): backup, retention GFS, mirror, restore, costs, recovery, guard/digest, units, security."""
import asyncio
import configparser
import gzip
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from app.storage.migrate import CODE_SCHEMA_VERSION
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import app.config as app_config
from app.storage import backup as bk
from app.util.time import month_key, now_ts

ROOT = Path(__file__).resolve().parents[2]


# ── backups ─────────────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_create_backup_pipeline(rt, tmp_root):
    res = await bk.create_backup(rt.db, wait_for_jobs=False)
    d = tmp_root / "data" / "backups"
    assert (d / f"{res.name}.db").exists() and (d / f"{res.name}.db.gz").exists() and (d / f"{res.name}.json").exists()
    man = json.loads((d / f"{res.name}.json").read_text())
    assert man["sha256"] == bk._sha256_file(d / f"{res.name}.db") == bk._sha256_gz(d / f"{res.name}.db.gz")
    assert man["schema_version"] == CODE_SCHEMA_VERSION and man["counts"]["collector_runs"] == 0 and res.mirror == "off"
    assert bk.latest_backup_age_hours(d) < 0.1
    ok, _ = await bk.verify_snapshot(d / f"{res.name}.db.gz", manifest=d / f"{res.name}.json", full=True)
    assert ok


@pytest.mark.asyncio
async def test_backup_busy_and_disk(rt, tmp_root, monkeypatch):
    lock = bk._FileLock(tmp_root / "data" / "run" / "backup.lock")
    assert lock.acquire()
    with pytest.raises(bk.BackupBusy):
        await bk.create_backup(rt.db, wait_for_jobs=False)
    lock.release()
    Usage = type("U", (), {"free": 10})
    monkeypatch.setattr(bk.shutil, "disk_usage", lambda p: Usage)
    with pytest.raises(bk.BackupError, match="insufficient_disk"):
        await bk.create_backup(rt.db, wait_for_jobs=False)


@pytest.mark.asyncio
async def test_backup_job_failure_fires_event(rt, tmp_root, monkeypatch):
    from app.jobs import SPEC_BY_ID

    async def boom(*a, **k):
        raise bk.BackupError("insufficient_disk: need 9 GB, free 1 GB")
    monkeypatch.setattr(bk, "create_backup", boom)
    job = bk.BackupJob(rt, SPEC_BY_ID["backup_daily"])
    await job.run("cli")
    row = await rt.db.fetchone("SELECT payload FROM events WHERE event_type='backup_failed'", writer=True)
    p = json.loads(row["payload"])
    assert {"error", "last_good_name", "last_good_age_h"} <= set(p)


def test_select_keep_gfs():
    end = datetime(2026, 9, 23, 3, 30, tzinfo=timezone.utc)
    stamps = [(end - timedelta(days=i)).strftime("%Y%m%dT%H%M%SZ") for i in range(60)]
    keep = bk.select_keep(stamps, 7, 8)
    days = sorted(s[:8] for s in keep)
    assert days == ["20260809", "20260816", "20260823", "20260830", "20260906", "20260913",
                    "20260917", "20260918", "20260919", "20260920", "20260921", "20260922", "20260923"]
    irregular = ["20260923T033000Z", "20260923T150000Z", "20260920T033000Z", "20260901T033000Z"]
    assert bk.select_keep(irregular, 1, 2) == {"20260923T150000Z", "20260920T033000Z"}


def _mkstamp(d: Path, st: str, size=10, gz=True):
    (d / f"taosentinel-{st}.db").write_bytes(b"x" * size)
    if gz:
        (d / f"taosentinel-{st}.db.gz").write_bytes(b"y" * size)
    (d / f"taosentinel-{st}.json").write_text("{}")


def test_apply_retention_rules(tmp_root):
    d = tmp_root / "data" / "backups"
    d.mkdir(parents=True, exist_ok=True)
    end = datetime(2026, 9, 23, 3, 30, tzinfo=timezone.utc)
    stamps = [(end - timedelta(days=i)).strftime("%Y%m%dT%H%M%SZ") for i in range(20)]
    for st in stamps:
        _mkstamp(d, st)
    bk.apply_retention(d, daily=7, weekly=8, keep_uncompressed=1, budget_gb=None)
    left = sorted({bk._stamp_of(p.name) for p in d.iterdir()})
    assert set(sorted(stamps)[-7:]) <= set(left)
    dbs = sorted(p.name for p in d.glob("*.db"))
    assert len(dbs) == 1 and stamps[0] in dbs[0]                             # newest kept uncompressed
    bk.apply_retention(d, daily=7, weekly=8, keep_uncompressed=1, budget_gb=60 / 2**30)
    left2 = {bk._stamp_of(p.name) for p in d.iterdir()}
    assert set(sorted(stamps)[-2:]) <= left2 and len(left2) >= 2           # 2 newest never deleted
    (tmp_root / "data" / "run" / "integrity.json").write_text(json.dumps({"result": "failed"}))
    before = set(os.listdir(d))
    bk.apply_retention(d, daily=1, weekly=0, keep_uncompressed=1, budget_gb=None)
    assert set(os.listdir(d)) == before                                     # integrity hold → nothing deleted


def test_mirror_copy(tmp_path):
    src = tmp_path / "a.db.gz"
    src.write_bytes(b"z" * 100)
    mdir = tmp_path / "mirror"
    assert bk.mirror_copy([src], mdir, ".taosentinel-mirror") == ("missing", "mirror_missing")
    assert not mdir.exists()                                                # never mkdir-ed
    mdir.mkdir()
    assert bk.mirror_copy([src], mdir, ".taosentinel-mirror")[0] == "missing"
    (mdir / ".taosentinel-mirror").write_text("")
    assert bk.mirror_copy([src], mdir, ".taosentinel-mirror") == ("ok", None)
    assert (mdir / "a.db.gz").stat().st_size == 100


@pytest.mark.asyncio
async def test_restore_refuses_and_drills(rt, tmp_root):
    from app.ops.recovery import InstanceLock
    res = await bk.create_backup(rt.db, wait_for_jobs=False)
    lock = InstanceLock(tmp_root / "data" / "run" / "taosentinel.lock")
    assert lock.acquire()
    try:
        assert await bk.restore_snapshot(res.manifest["stamp"], yes=True) == 2
        drill = tmp_root / "data" / "tmp" / "drill.db"
        assert await bk.restore_snapshot(res.manifest["stamp"], target=drill) == 0
        c = sqlite3.connect(drill)
        assert c.execute("SELECT MAX(version) FROM schema_version").fetchone()[0] == CODE_SCHEMA_VERSION
        c.close()
    finally:
        lock.release()


# ── costs ───────────────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_costs_governor(rt, settings_override, tmp_root):
    from app.costs import CostGovernor, group_of
    s = app_config.settings
    keep = {k: getattr(s, k) for k in ("DB_PATH", "RUN_DIR", "CONTROL_DIR", "DATA_DIR", "KILL_SWITCH_PATH",
                                       "PAUSE_ALL_PATH", "SCORING_KILL_SWITCH_PATH", "DB_BACKUP_DIR", "JSONL_DIR")}
    settings_override(X_MONTHLY_CAP_USD=1.0, X_PACING_ENABLED=False, **keep)
    gov = rt.costs
    assert group_of("anthropic").name == "llm" and group_of("llm") is group_of("anthropic")
    await gov.record("github", 0.0, requests=1)
    assert await rt.db.fetchval("SELECT COUNT(*) FROM provider_spend", writer=True) == 0   # buffered
    await gov.flush()
    assert await rt.db.fetchval("SELECT requests FROM provider_spend WHERE provider='github'", writer=True) == 1
    await gov.record("twitterapi_io", 0.5, units={"posts": 10})
    await gov.record("twitterapi_io", 0.35, units={"posts": 5})
    row = await rt.db.fetchone("SELECT usd, requests, units_json, updated_at FROM provider_spend "
                               "WHERE provider='twitterapi_io'", writer=True)
    assert row["usd"] == pytest.approx(0.85) and row["requests"] == 0 and json.loads(row["units_json"]) == {"posts": 15}
    assert isinstance(row["updated_at"], int)
    soft = await rt.db.fetchall("SELECT payload, dedupe_key, source FROM events WHERE event_type='spend_80pct'",
                                writer=True)
    assert len(soft) == 1 and soft[0]["dedupe_key"] == f"x|{month_key(now_ts())}|80" and soft[0]["source"] == "cost"
    gov2 = CostGovernor(rt.db, alerts=rt.alerts)
    await gov2.load()
    await gov2.record("twitterapi_io", 0.01)
    assert await rt.db.fetchval("SELECT COUNT(*) FROM events WHERE event_type='spend_80pct'", writer=True) == 1
    d = await gov.check("twitterapi_io", 0.5)
    assert not d.allowed and d.reason == "hard_cap" and gov.is_exhausted("x") and gov.is_exhausted("twitterapi_io")
    assert await rt.db.fetchval("SELECT COUNT(*) FROM events WHERE event_type='spend_cap_reached'", writer=True) == 1
    await gov.set_override("x", 1.0, by="t", reason="drill")
    assert gov.cap_usd("x") == 2.0 and not gov.is_exhausted("x")
    assert (await gov.check("twitterapi_io", 0.1)).allowed
    with pytest.raises(TypeError):
        await gov.record("twitterapi_io", 0.1, 1)                            # keyword-only contract (02 §G5)
    snap = gov.snapshot()
    for g in ("x", "llm", "taostats", "tmc"):
        assert {"usd", "cap_usd", "pct", "paused", "reserved_usd"} <= set(snap[g])


@pytest.mark.asyncio
async def test_costs_reservations_kill_zero(rt, settings_override, tmp_root):
    from app.costs import CostGovernor
    s = app_config.settings
    keep = {k: getattr(s, k) for k in ("DB_PATH", "RUN_DIR", "CONTROL_DIR", "DATA_DIR", "KILL_SWITCH_PATH",
                                       "PAUSE_ALL_PATH", "SCORING_KILL_SWITCH_PATH", "DB_BACKUP_DIR", "JSONL_DIR")}
    settings_override(LLM_MONTHLY_CAP_USD=10, **keep)
    gov = rt.costs
    res = await gov.reserve("anthropic", 2.0, job="llm_classify_submit", run_id="r1")
    assert res is not None and gov.reserved_usd("llm") == 2.0
    gov2 = CostGovernor(rt.db)
    await gov2.load()
    await gov2.commit(res.id, 1.5)
    assert await rt.db.fetchval("SELECT usd FROM provider_spend WHERE provider='anthropic'", writer=True) == 1.5
    r2 = await gov.reserve("anthropic", 1.0, job="j", run_id=None)
    await gov.release(r2)
    await gov.load()
    assert gov.reserved_usd("llm") == 0
    (tmp_root / "KILL_SWITCH").write_text("")
    assert (await gov.check("anthropic", 0.1)).reason == "kill_switch"
    assert (await gov.check("tmc", 0)).allowed
    (tmp_root / "KILL_SWITCH").unlink()
    settings_override(LLM_MONTHLY_CAP_USD=0, **keep)
    assert (await gov.check("anthropic", 0.1)).reason == "zero_cap" and gov.is_exhausted("llm")


# ── recovery ────────────────────────────────────────────────────────────────────────────────────────────────
def test_classify_boot():
    from app.ops.recovery import classify_boot
    assert classify_boot(None, "b", [], 100).kind == "first_boot"
    assert classify_boot(None, "b", [{"at": 1}], 100).kind == "clean"
    assert classify_boot({"ts": 40, "boot_id": "b"}, "b", [], 100).kind == "process_crash"
    i = classify_boot({"ts": 40, "boot_id": "a"}, "b", [], 100)
    assert i.kind == "host_unclean" and i.downtime_sec == 60


@pytest.mark.asyncio
async def test_record_boot_and_report(rt):
    from app.ops.recovery import BootInfo, boots, record_boot, report
    rt.boot = BootInfo("host_unclean", now_ts() - 3600, 3600, "bid", started_at=now_ts())
    for _ in range(35):
        await record_boot(rt)
    assert len(await boots(rt.db)) == 30
    await report(rt, rt.boot)
    row = await rt.db.fetchone("SELECT payload FROM events WHERE event_type='service_recovered_unclean'", writer=True)
    assert json.loads(row["payload"])["kind"] == "host_unclean"


def test_instance_lock(tmp_path):
    from app.ops.recovery import InstanceLock
    a = InstanceLock(tmp_path / "l")
    assert a.acquire()
    r = subprocess.run([sys.executable, "-c", "import fcntl,os,sys; fd=os.open(sys.argv[1], os.O_RDWR); "
                        "fcntl.flock(fd, fcntl.LOCK_EX|fcntl.LOCK_NB)", str(tmp_path / "l")], capture_output=True)
    assert r.returncode != 0
    a.release()


# ── guard / digest ──────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_guard_and_digest(rt, tmp_root, monkeypatch):
    from app.jobs import JobRegistry, SPEC_BY_ID
    from app.ops import guard
    from app.ops.digest import build_digest
    JobRegistry.build(rt)
    Usage = type("U", (), {"free": int(1.0 * 2**30)})
    monkeypatch.setattr(guard.shutil, "disk_usage", lambda p: Usage)
    await rt.jobs["ops_guard_5m"].run("cli")
    assert (tmp_root / "data" / "run" / "heartbeat.json").exists()
    assert await rt.db.fetchval("SELECT COUNT(*) FROM events WHERE event_type='disk_low'", writer=True) == 1
    text = await build_digest(rt)
    assert "TaoSentinel ops" in text and len(text) <= 4000 and "spend" in text


# ── units & security ────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.skipif(sys.platform != "linux", reason="bash")
def test_units_render(tmp_path):
    env = dict(os.environ, SENTINEL_USER="tester", SENTINEL_ROOT="/home/tester/taosentinel")
    r = subprocess.run(["bash", str(ROOT / "ops" / "install-systemd.sh"), "--render-only", str(tmp_path)], env=env,
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr + r.stdout
    for f in tmp_path.iterdir():
        assert "@" not in f.read_text().replace("taosentinel-notify-failure@", "").replace("%i", "") or \
            not any(tok in f.read_text() for tok in ("@SENTINEL", "@NODE", "@MIRROR"))
    cp = configparser.ConfigParser(strict=False, interpolation=None)
    cp.optionxform = str
    cp.read(tmp_path / "taosentinel.service")
    sv = cp["Service"]
    assert (sv["Type"], sv["NotifyAccess"], sv["WatchdogSec"], sv["TimeoutStartSec"]) == ("notify", "main", "120", "120")
    assert (sv["Restart"], sv["RestartSec"], sv["TimeoutStopSec"], sv["KillSignal"]) == ("on-failure", "10", "30",
                                                                                         "SIGTERM")
    assert cp["Unit"]["StartLimitBurst"] == "5" and "time-sync.target" in cp["Unit"]["After"]
    assert sv["StandardOutput"] == "append:/home/tester/taosentinel/data/service.log"
    assert sv["ProtectHome"] == "read-only" and sv["ReadWritePaths"] == "/home/tester/taosentinel/data"
    fe = (tmp_path / "taosentinel-frontend.service").read_text()
    assert "Wants=taosentinel.service" in fe and "Requires=" not in fe
    assert "OnUnitActiveSec=5min" in (tmp_path / "taosentinel-update.timer").read_text()


def test_security_selfcheck(settings_override, tmp_path, monkeypatch):
    from app.ops import security
    monkeypatch.setattr(security, "APP_ROOT", tmp_path)
    (tmp_path / ".env").write_text("X=1")
    os.chmod(tmp_path / ".env", 0o644)
    s = settings_override(HEALTH_HOST="100.64.0.1", API_AUTH_TOKEN="")
    r = security.selfcheck(s)
    assert r["env_mode_ok"] is False and r["bind_ok"] is False
    os.chmod(tmp_path / ".env", 0o600)
    assert security.selfcheck(settings_override())["env_mode_ok"] is True
