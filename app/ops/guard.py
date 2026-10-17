"""``ops_guard_5m`` (spec 16 §2.13.3): heartbeat, disk, mirror/backup staleness, governor + pause-cache refresh,
dead-man ping, WAL size."""
from __future__ import annotations

import shutil
from pathlib import Path

import app.config as app_config
from app import control
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.ops.recovery import write_heartbeat
from app.util.time import now_ts


def _size(p: Path) -> int:
    if p.is_file():
        return p.stat().st_size
    if p.is_dir():
        return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
    return 0


def disk_state() -> dict:
    s = app_config.settings
    data = s.path(s.DATA_DIR)
    usage = shutil.disk_usage(data if data.exists() else s.path("."))
    db = s.path(s.DB_PATH)
    return {"free_gb": round(usage.free / 2**30, 2), "min_gb": s.DISK_FREE_MIN_GB,
            "critical_gb": s.DISK_FREE_CRITICAL_GB,
            "db_gb": round((_size(db) + _size(Path(str(db) + "-wal"))) / 2**30, 3),
            "backups_gb": round(_size(s.path(s.DB_BACKUP_DIR)) / 2**30, 3),
            "logs_gb": round(_size(s.path(s.JSONL_DIR)) / 2**30, 3), "path": str(data)}


def mirror_state() -> str:
    s = app_config.settings
    if not s.DB_BACKUP_MIRROR_DIR:
        return "off"
    d = Path(s.DB_BACKUP_MIRROR_DIR)
    return "ok" if d.is_dir() and (d / s.DB_BACKUP_MIRROR_MARKER).exists() else "missing"


class OpsGuardJob(CollectorJob):
    _last_hourly: int = 0

    async def collect(self, run: RunContext) -> JobOutcome:
        s = app_config.settings
        rt = run.rt
        alerts = rt.alerts
        write_heartbeat(rt)
        ds = disk_state()
        day = now_ts() // 86400
        payload = {k: ds[k] for k in ("free_gb", "min_gb", "db_gb", "backups_gb", "logs_gb", "path")}
        if alerts is not None:
            if ds["free_gb"] < s.DISK_FREE_CRITICAL_GB:
                await alerts.fire("disk_critical", f"🛑 <b>disk critical</b>: {ds['free_gb']} GB free",
                                  event_type="disk_critical", severity="critical", payload=payload,
                                  dedupe_key=f"{day}|{now_ts() // 21600}", source="ops")
            else:
                await alerts.clear("disk_critical")
            if s.DISK_FREE_CRITICAL_GB <= ds["free_gb"] < s.DISK_FREE_MIN_GB:
                await alerts.fire("disk_low", f"⚠️ disk low: {ds['free_gb']} GB free", event_type="disk_low",
                                  severity="warning", payload=payload, dedupe_key=str(day), source="ops")
            elif ds["free_gb"] >= s.DISK_FREE_MIN_GB:
                await alerts.clear("disk_low")
        mirror = mirror_state()
        now = now_ts()
        if now - self._last_hourly >= 3600:
            self._last_hourly = now
            if alerts is not None:
                if mirror == "missing":
                    await alerts.fire("backup_mirror_missing", f"⚠️ backup mirror missing: {s.DB_BACKUP_MIRROR_DIR}",
                                      event_type="backup_mirror_missing",
                                      payload={"mirror_dir": s.DB_BACKUP_MIRROR_DIR, "marker": s.DB_BACKUP_MIRROR_MARKER},
                                      dedupe_key=str(day), source="ops")
                elif mirror == "ok":
                    await alerts.clear("backup_mirror_missing")
                if s.DB_BACKUP_ENABLED:
                    from app.storage.backup import latest_backup_age_hours
                    age = latest_backup_age_hours(s.path(s.DB_BACKUP_DIR))
                    stale = (age is not None and age > s.DB_BACKUP_STALE_HOURS) or \
                            (age is None and now - rt.started_at > s.DB_BACKUP_STALE_HOURS * 3600)
                    if stale:
                        await alerts.fire("backup_stale", f"⚠️ newest DB backup is {age or 0:.0f} h old",
                                          event_type="backup_stale",
                                          payload={"age_hours": round(age, 1) if age is not None else None,
                                                   "stale_hours": s.DB_BACKUP_STALE_HOURS},
                                          dedupe_key=str(day), source="ops")
                    else:
                        await alerts.clear("backup_stale")
        if rt.costs is not None:
            await rt.costs.refresh()
        if rt.db is not None:
            await control.refresh_cache(rt.db)
        control.scan_unknown_pause_files()
        note_dm = ""
        if s.DEADMAN_ENABLED and s.DEADMAN_PING_URL and rt.http is not None:
            try:
                await rt.http.request("deadman", "GET", s.DEADMAN_PING_URL, timeout=10, retries=0,
                                      ok_statuses=(200, 201, 202, 204))
                note_dm = " deadman=ok"
            except Exception as exc:
                logger.info("deadman ping failed", data={"error": type(exc).__name__})
                note_dm = " deadman=fail"
        walp = Path(str(s.path(s.DB_PATH)) + "-wal")
        if walp.exists() and walp.stat().st_size > s.DB_WAL_WARN_MB * 2**20:
            logger.warning("wal_large", data={"wal_mb": round(walp.stat().st_size / 2**20, 1)})
        return JobOutcome(rows=0, note=f"free={ds['free_gb']}GB mirror={mirror}{note_dm}")
