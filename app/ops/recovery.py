"""Instance lock, heartbeat, boot classification, post-boot integrity check, unclean-boot report (spec 16 §2.6)."""
from __future__ import annotations

import asyncio
import fcntl
import json
import os
import sqlite3
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import app.config as app_config
from app.logging.logger import logger
from app.util.time import now_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.runtime import Runtime
    from app.storage.db import Database

BootKind = Literal["first_boot", "clean", "process_crash", "host_unclean"]


@dataclass(slots=True)
class BootInfo:
    kind: BootKind
    prev_ts: int | None
    downtime_sec: int | None
    boot_id: str
    orphan_runs: int = 0
    started_at: int = 0


class InstanceLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.fd: int | None = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            return False
        self.fd = fd
        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode())
        return True

    def release(self) -> None:
        if self.fd is not None:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            finally:
                os.close(self.fd)
                self.fd = None


_lock: InstanceLock | None = None


def _run_dir() -> Path:
    s = app_config.settings
    return s.path(s.RUN_DIR)


def heartbeat_path() -> Path:
    return _run_dir() / "heartbeat.json"


def current_boot_id() -> str:
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError:
        return "unknown"


def classify_boot(prev: dict | None, boot_id: str, boots: list, now: int) -> BootInfo:
    if prev is None:
        kind: BootKind = "first_boot" if not boots else "clean"
        return BootInfo(kind, None, None, boot_id, started_at=now)
    prev_ts = int(prev.get("ts") or prev.get("started_at") or now)
    kind = "process_crash" if prev.get("boot_id") == boot_id else "host_unclean"
    return BootInfo(kind, prev_ts, max(0, now - prev_ts), boot_id, started_at=now)


def _read_boots_sync() -> list:
    s = app_config.settings
    db = s.path(s.DB_PATH)
    if not db.exists():
        return []
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5)
        try:
            row = con.execute("SELECT value_json FROM job_state WHERE job = '_ops' AND key = 'boots'").fetchone()
        finally:
            con.close()
        return json.loads(row[0]) if row else []
    except (sqlite3.Error, ValueError):
        return []


async def pre_init(rt: "Runtime") -> BootInfo:
    """Take the instance lock (exit 3 when held), read the previous heartbeat, classify the boot."""
    global _lock
    _lock = InstanceLock(_run_dir() / "taosentinel.lock")
    if not _lock.acquire():
        logger.error(f"another instance holds {app_config.settings.RUN_DIR}/taosentinel.lock")
        sys.exit(3)
    prev = None
    hp = heartbeat_path()
    if hp.exists():
        try:
            prev = json.loads(hp.read_text())
        except (OSError, ValueError):
            prev = {"ts": int(hp.stat().st_mtime), "boot_id": None}
    info = classify_boot(prev, current_boot_id(), _read_boots_sync(), rt.started_at)
    rt.boot = info
    write_heartbeat(rt)
    logger.info("boot classified", data=asdict(info))
    return info


def release_lock() -> None:
    global _lock
    if _lock is not None:
        _lock.release()
        _lock = None


def write_heartbeat(rt: "Runtime") -> None:
    p = heartbeat_path()
    data = {"pid": os.getpid(), "boot_id": current_boot_id(), "started_at": rt.started_at, "ts": now_ts(),
            "version": rt.version}
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(p.name + ".tmp")
        tmp.write_text(json.dumps(data))
        os.replace(tmp, p)
    except OSError as exc:
        logger.warning("heartbeat write failed", data={"error": str(exc)})


def remove_heartbeat() -> None:
    try:
        heartbeat_path().unlink(missing_ok=True)
    except OSError:
        pass


async def count_orphan_runs(db: "Database", boot_at: int) -> int:
    return int(await db.fetchval("SELECT COUNT(*) FROM collector_runs WHERE finished_at IS NULL AND started_at < ?",
                                 (boot_at,)) or 0)


async def record_boot(rt: "Runtime") -> None:
    """After apply_schema: count orphans and append the boot to job_state('_ops','boots') (≤ 30 entries)."""
    info = rt.boot
    if info is None or rt.db is None:
        return
    info.orphan_runs = await count_orphan_runs(rt.db, info.started_at)
    row = await rt.db.fetchone("SELECT value_json FROM job_state WHERE job = '_ops' AND key = 'boots'")
    boots = json.loads(row["value_json"]) if row else []
    boots.append({"at": info.started_at, "kind": info.kind, "prev_ts": info.prev_ts,
                  "downtime_sec": info.downtime_sec, "version": rt.version, "orphan_runs": info.orphan_runs})
    await rt.db.upsert("job_state", [{"job": "_ops", "key": "boots", "value_json": json.dumps(boots[-30:]),
                                      "updated_at": now_ts()}], conflict=("job", "key"))


async def boots(db: "Database") -> list:
    row = await db.fetchone("SELECT value_json FROM job_state WHERE job = '_ops' AND key = 'boots'")
    return json.loads(row["value_json"]) if row else []


def write_integrity(result: str, lines: list[str]) -> None:
    from app.control import integrity_path
    p = integrity_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps({"at": now_ts(), "result": result, "lines": lines[:5]}))
    os.replace(tmp, p)


def quick_check_sync(path: Path) -> list[str]:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    try:
        return [str(r[0]) for r in con.execute("PRAGMA quick_check").fetchall()]
    finally:
        con.close()


async def post_boot_check(rt: "Runtime", info: BootInfo) -> None:
    s = app_config.settings
    if info.kind not in ("process_crash", "host_unclean") or not s.DB_QUICK_CHECK_ON_UNCLEAN:
        return
    try:
        await asyncio.wait_for(rt.shutdown_event.wait(), timeout=s.DB_QUICK_CHECK_DELAY_SEC)
        return
    except asyncio.TimeoutError:
        pass
    lines = await asyncio.to_thread(quick_check_sync, s.path(s.DB_PATH))
    ok = lines == ["ok"]
    write_integrity("ok" if ok else "failed", lines)
    logger.info("post-boot quick_check", data={"result": "ok" if ok else lines[:5]})
    if not ok and rt.alerts is not None:
        await rt.alerts.fire("db_integrity_failed", "🛑 <b>DB integrity check FAILED</b> after an unclean boot\n<code>"
                             + "\n".join(lines[:5]) + "</code>\nNon-ops jobs are held (integrity hold). See runbook.",
                             event_type="db_integrity_failed", severity="critical",
                             payload={"quick_check": "\n".join(lines[:5])},
                             dedupe_key=f"{info.boot_id}|{info.started_at}", source="ops")


async def report(rt: "Runtime", info: BootInfo) -> None:
    if info.kind not in ("process_crash", "host_unclean") or (info.downtime_sec or 0) < 60 or rt.alerts is None:
        return
    missed = []
    try:
        from datetime import datetime, timezone

        from app.jobs import JOB_TABLE, build_trigger, next_fire_times
        start = datetime.fromtimestamp(info.prev_ts or info.started_at, timezone.utc)
        for spec in JOB_TABLE:
            if spec.kind != "cron" or spec.id not in rt.jobs:
                continue
            trig = build_trigger(spec)
            for t in next_fire_times(trig, 3, start):
                if t.timestamp() < info.started_at:
                    missed.append(spec.id)
                    break
    except Exception:  # pragma: no cover
        pass
    down = info.downtime_sec or 0
    text = (f"⚠️ <b>TaoSentinel recovered</b> from {info.kind} · down {down // 3600}h{(down % 3600) // 60:02d}m "
            f"(since {to_iso(info.prev_ts)}) · orphan runs {info.orphan_runs}"
            + (f" · missed: {', '.join(missed)}" if missed else ""))
    await rt.alerts.fire("service_recovered_unclean", text, event_type="service_recovered_unclean", severity="warning",
                         payload={"kind": info.kind, "downtime_sec": down, "prev_ts": info.prev_ts,
                                  "orphan_runs": info.orphan_runs, "sse_gap_blocks_est": down // 12, "missed": missed},
                         dedupe_key=f"{info.boot_id}|{info.started_at}", source="ops")
