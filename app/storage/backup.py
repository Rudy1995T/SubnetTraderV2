"""Backup pipeline, retention, verify and restore (spec 16 §2.7–§2.8; primitive = ``Database.backup_to``, spec 03).

``create_backup``: lock → wait for ``storage_retention_daily`` → disk preflight → ``backup_to`` (VACUUM INTO tmp →
quick_check → rename) → manifest → gzip + verify → mirror (only when the marker file exists; never ``mkdir``) →
GFS retention (7 daily + 8 weekly, size budget, the 2 newest stamps never deleted, newest kept uncompressed).
"""
from __future__ import annotations

import asyncio
import fcntl
import gzip
import hashlib
import json
import os
import shutil
import socket
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import app.config as app_config
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

_CHUNK = 1 << 20


class BackupError(RuntimeError):
    pass


class BackupBusy(BackupError):
    pass


@dataclass(slots=True)
class BackupResult:
    name: str
    path: str
    size_bytes: int
    sha256: str
    gz_path: str | None
    gz_size_bytes: int | None
    schema_version: int
    mirror: str                     # ok | missing | failed | off
    mirror_error: str | None
    pruned: int
    duration_sec: float
    manifest: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {"name": self.name, "path": self.path, "size_bytes": self.size_bytes, "integrity": "ok",
                "mirror_error": self.mirror_error, "pruned": self.pruned}


def _stamp(ts: int | None = None) -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(now_ts() if ts is None else ts))


def _stamp_of(name: str) -> str | None:
    # taosentinel-<stamp>[-tag].{db,db.gz,json}
    if not name.startswith("taosentinel-"):
        return None
    rest = name[len("taosentinel-"):]
    stamp = rest[:16]
    return stamp if len(stamp) == 16 and stamp[8] == "T" and stamp.endswith("Z") else None


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while b := f.read(_CHUNK):
            h.update(b)
    return h.hexdigest()


def _sha256_gz(path: Path) -> str:
    h = hashlib.sha256()
    with gzip.open(path, "rb") as f:
        while b := f.read(_CHUNK):
            h.update(b)
    return h.hexdigest()


def _dir_size(d: Path) -> int:
    return sum(p.stat().st_size for p in d.glob("taosentinel-*") if p.is_file()) if d.is_dir() else 0


def latest_backup_age_hours(backup_dir: str | Path) -> float | None:
    """Age of the newest manifest (``created_at``), falling back to file mtime; None when no backup exists."""
    d = Path(backup_dir)
    if not d.is_dir():
        return None
    newest: float | None = None
    for m in d.glob("taosentinel-*.json"):
        try:
            ts = float(json.loads(m.read_text()).get("created_at"))
        except (OSError, ValueError, TypeError):
            ts = m.stat().st_mtime
        newest = ts if newest is None or ts > newest else newest
    if newest is None:
        dbs = [p.stat().st_mtime for p in d.glob("taosentinel-*.db*")]
        newest = max(dbs) if dbs else None
    return None if newest is None else max(0.0, (now_ts() - newest) / 3600.0)


def latest_backup(backup_dir: str | Path) -> dict | None:
    d = Path(backup_dir)
    mans = sorted(d.glob("taosentinel-*.json")) if d.is_dir() else []
    if not mans:
        return None
    try:
        return json.loads(mans[-1].read_text())
    except (OSError, ValueError):
        return {"name": mans[-1].stem}


# ── retention (pure GFS rule + filesystem application) ────────────────────────────────────────────────────────
def select_keep(stamps: list[str], daily: int, weekly: int) -> set[str]:
    uniq = sorted(set(stamps), reverse=True)
    keep = set(uniq[:max(0, daily)])
    weeks_seen: list[tuple[int, int]] = []
    for st in uniq:
        d = datetime.strptime(st, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        wk = tuple(d.isocalendar()[:2])
        if wk in weeks_seen:
            continue
        if len(weeks_seen) >= max(0, weekly):
            break
        weeks_seen.append(wk)  # type: ignore[arg-type]
        keep.add(st)
    return keep


def _integrity_bad() -> bool:
    from app.control import integrity_hold_active
    return integrity_hold_active()


def apply_retention(dir: Path, *, daily: int, weekly: int, keep_uncompressed: int, budget_gb: float | None,
                    mirror: bool = False) -> list[str]:
    """Delete files of stamps outside the GFS keep set, drop surplus uncompressed ``.db`` copies, enforce the budget."""
    if _integrity_bad():
        logger.warning("backup retention skipped: integrity hold active")
        return []
    if not dir.is_dir():
        return []
    files: dict[str, list[Path]] = {}
    for p in dir.iterdir():
        if p.is_file() and (st := _stamp_of(p.name)) and not p.name.endswith(".tmp"):
            files.setdefault(st, []).append(p)
    stamps = sorted(files)
    keep = select_keep(stamps, daily, weekly)
    newest2 = set(sorted(stamps)[-2:])
    keep |= newest2
    deleted: list[str] = []
    for st in stamps:
        if st not in keep:
            for p in files[st]:
                p.unlink(missing_ok=True)
                deleted.append(p.name)
    kept_sorted = sorted((s for s in stamps if s in keep), reverse=True)
    for i, st in enumerate(kept_sorted):
        if mirror or i >= max(1, keep_uncompressed):
            dbs = [p for p in files[st] if p.name.endswith(".db")]
            gzs = [p for p in files[st] if p.name.endswith(".db.gz")]
            if dbs and gzs:
                for p in dbs:
                    p.unlink(missing_ok=True)
                    deleted.append(p.name)
    if budget_gb and budget_gb > 0:
        budget = budget_gb * 2**30
        weekly_only = select_keep(stamps, 0, weekly) - set(sorted(stamps, reverse=True)[:daily])
        order = [s for s in sorted(kept_sorted) if s in weekly_only] + [s for s in sorted(kept_sorted)
                                                                          if s not in weekly_only]
        for st in order:
            if _dir_size(dir) <= budget:
                break
            remaining = {_stamp_of(p.name) for p in dir.iterdir() if p.is_file() and _stamp_of(p.name)}
            if len(remaining) <= 2 or st in newest2:
                continue
            for p in dir.glob(f"taosentinel-{st}*"):
                p.unlink(missing_ok=True)
                deleted.append(p.name)
            logger.info("backup_budget_pruned", data={"stamp": st})
    return deleted


# ── pipeline ──────────────────────────────────────────────────────────────────────────────────────────────────
class _FileLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.fd: int | None = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            os.close(self.fd)
            self.fd = None
            return False

    def release(self) -> None:
        if self.fd is not None:
            try:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            finally:
                os.close(self.fd)
                self.fd = None


def _cleanup_tmp(*dirs: Path) -> None:
    cutoff = time.time() - 3600
    for d in dirs:
        if not d or not d.is_dir():
            continue
        for p in d.glob("*.tmp"):
            try:
                if p.stat().st_mtime < cutoff:
                    p.unlink()
            except OSError:
                pass


async def _wait_for_jobs(db: "Database") -> None:
    s = app_config.settings
    ids = [x.strip() for x in s.DB_BACKUP_WAIT_FOR_JOBS.split(",") if x.strip()]
    if not ids:
        return
    await asyncio.sleep(30)
    deadline = time.monotonic() + s.DB_BACKUP_WAIT_MAX_SEC
    while time.monotonic() < deadline:
        busy = []
        try:
            from app.runtime import runtime
        except Exception:  # pragma: no cover
            runtime = None
        for j in ids:
            job = runtime.jobs.get(j) if runtime is not None else None
            if job is not None:
                if job.running:
                    busy.append(j)
            else:
                timeout = 3600
                n = await db.fetchval("SELECT COUNT(*) FROM collector_runs WHERE job = ? AND finished_at IS NULL "
                                      "AND started_at > ?", (j, now_ts() - timeout))
                if n:
                    busy.append(j)
        if not busy:
            return
        await asyncio.sleep(10)
    logger.warning("backup: proceeding while heavy jobs still run", data={"jobs": ids})


def _snapshot_schema_version(path: Path) -> int:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return int(con.execute("SELECT COALESCE(MAX(version), 0) FROM schema_version").fetchone()[0])
    except sqlite3.Error:
        return 0
    finally:
        con.close()


def _snapshot_counts(path: Path) -> dict:
    out: dict[str, int | None] = {}
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        for t in ("scores", "metric_values", "events", "collector_runs"):
            try:
                out[t] = int(con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0])
            except sqlite3.Error:
                out[t] = None
    finally:
        con.close()
    return out


def _write_json_atomic(path: Path, data: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w") as f:
        json.dump(data, f, indent=1, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def compress_and_verify(db_path: Path, sha: str, level: int) -> tuple[Path, int]:
    gz = db_path.with_name(db_path.name + ".gz")
    tmp = gz.with_name(gz.name + ".tmp")
    with open(db_path, "rb") as src, gzip.open(tmp, "wb", compresslevel=level) as dst:
        while b := src.read(_CHUNK):
            dst.write(b)
    with open(tmp, "rb") as f:
        os.fsync(f.fileno())
    if _sha256_gz(tmp) != sha:
        tmp.unlink(missing_ok=True)
        raise BackupError("gzip verification failed (sha mismatch)")
    os.replace(tmp, gz)
    return gz, gz.stat().st_size


def mirror_copy(files: list[Path], mirror_dir: Path, marker: str) -> tuple[str, str | None]:
    if not mirror_dir.is_dir() or not (mirror_dir / marker).exists():
        return "missing", "mirror_missing"
    try:
        for src in files:
            dst = mirror_dir / src.name
            tmp = dst.with_name(dst.name + ".tmp")
            shutil.copyfile(src, tmp)
            with open(tmp, "rb") as f:
                os.fsync(f.fileno())
            if tmp.stat().st_size != src.stat().st_size:
                tmp.unlink(missing_ok=True)
                raise OSError(f"size mismatch copying {src.name}")
            os.replace(tmp, dst)
        return "ok", None
    except OSError as exc:
        return "failed", str(exc)


async def create_backup(db: "Database", *, backup_dir: Path | None = None, mirror_dir: str | None = None,
                        retention: int | None = None, weekly_retention: int | None = None,
                        compress: bool | None = None, max_total_gb: float | None = None, tag: str | None = None,
                        wait_for_jobs: bool = True) -> BackupResult:
    s = app_config.settings
    backup_dir = Path(backup_dir) if backup_dir else s.path(s.DB_BACKUP_DIR)
    mirror_dir = s.DB_BACKUP_MIRROR_DIR if mirror_dir is None else mirror_dir
    retention = s.DB_BACKUP_RETENTION if retention is None else retention
    weekly_retention = s.DB_BACKUP_WEEKLY_RETENTION if weekly_retention is None else weekly_retention
    compress = s.DB_BACKUP_COMPRESS if compress is None else compress
    max_total_gb = s.DB_BACKUP_MAX_TOTAL_GB if max_total_gb is None else max_total_gb
    t0 = time.monotonic()
    lock = _FileLock(s.path(s.RUN_DIR) / "backup.lock")
    if not lock.acquire():
        raise BackupBusy("backup.lock held")
    try:
        target_dir = backup_dir / "manual" if tag else backup_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        mdir = Path(mirror_dir) if mirror_dir else None
        _cleanup_tmp(backup_dir, target_dir, *( [mdir] if mdir else [] ))
        if wait_for_jobs and not tag:
            await _wait_for_jobs(db)
        # preflight (disk guard)
        size = db.path.stat().st_size if db.path.exists() else 0
        walp = Path(str(db.path) + "-wal")
        wal = walp.stat().st_size if walp.exists() else 0
        need = (size + wal) * s.DB_BACKUP_MIN_FREE_FACTOR + (0.5 * size if compress else 0) + s.DISK_FREE_MIN_GB * 2**30
        free = shutil.disk_usage(target_dir).free
        if free < need:
            apply_retention(backup_dir, daily=2, weekly=0, keep_uncompressed=1, budget_gb=None)
            free = shutil.disk_usage(target_dir).free
            if free < need:
                raise BackupError(f"insufficient_disk: need {need / 2**30:.2f} GB, free {free / 2**30:.2f} GB")
        stamp = _stamp()
        name = f"taosentinel-{stamp}" + (f"-{tag}" if tag else "")
        dest = target_dir / f"{name}.db"
        await db.backup_to(dest)
        sha = await asyncio.to_thread(_sha256_file, dest)
        from app import __version__
        manifest = {
            "name": name, "stamp": stamp, "tag": tag, "created_at": now_ts(), "app_version": __version__,
            "spec_version": s.SPEC_VERSION, "schema_version": await asyncio.to_thread(_snapshot_schema_version, dest),
            "size_bytes": dest.stat().st_size, "sha256": sha, "quick_check": "ok", "gz": None,
            "counts": await asyncio.to_thread(_snapshot_counts, dest), "duration_sec": None,
        }
        man_path = target_dir / f"{name}.json"
        _write_json_atomic(man_path, manifest)
        gz_path = None
        gz_size = None
        if compress:
            try:
                gz_path, gz_size = await asyncio.to_thread(compress_and_verify, dest, sha, s.DB_BACKUP_GZIP_LEVEL)
                manifest["gz"] = {"size_bytes": gz_size}
            except Exception as exc:
                logger.warning("backup gzip failed; keeping verified .db", data={"error": str(exc)})
        mirror, mirror_error = "off", None
        if mdir is not None:
            files = [gz_path] if gz_path else [dest]
            mirror, mirror_error = await asyncio.to_thread(mirror_copy, [*files, man_path], mdir,
                                                           s.DB_BACKUP_MIRROR_MARKER)
        pruned: list[str] = []
        try:
            if tag:
                mans = sorted((backup_dir / "manual").glob("taosentinel-*.json"))
                for old in mans[:-s.DB_BACKUP_MANUAL_RETENTION]:
                    st = _stamp_of(old.name)
                    for p in (backup_dir / "manual").glob(f"taosentinel-{st}*"):
                        p.unlink(missing_ok=True)
                        pruned.append(p.name)
            else:
                pruned = apply_retention(backup_dir, daily=retention, weekly=weekly_retention,
                                         keep_uncompressed=s.DB_BACKUP_KEEP_UNCOMPRESSED, budget_gb=max_total_gb)
                if mdir is not None and mirror == "ok":
                    pruned += apply_retention(mdir, daily=retention, weekly=weekly_retention, keep_uncompressed=0,
                                              budget_gb=s.DB_BACKUP_MIRROR_MAX_TOTAL_GB, mirror=True)
        except Exception as exc:
            logger.warning("backup_retention_failed", data={"error": str(exc)})
        manifest["duration_sec"] = round(time.monotonic() - t0, 2)
        _write_json_atomic(man_path, manifest)
        return BackupResult(name, str(dest) if dest.exists() else "", manifest["size_bytes"], sha,
                            str(gz_path) if gz_path else None, gz_size, manifest["schema_version"], mirror,
                            mirror_error, len(pruned), manifest["duration_sec"], manifest)
    finally:
        lock.release()


class BackupJob(CollectorJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        s = app_config.settings
        alerts = run.rt.alerts
        try:
            res = await create_backup(run.rt.db)
        except BackupBusy:
            return JobOutcome(rows=0, note="busy")
        except Exception as exc:
            last = latest_backup(s.path(s.DB_BACKUP_DIR)) or {}
            age = latest_backup_age_hours(s.path(s.DB_BACKUP_DIR))
            if alerts is not None:
                import html
                await alerts.fire("backup_failed", f"⚠️ <b>DB backup FAILED</b>\n<code>{html.escape(str(exc))}</code>\n"
                                  "The SQLite DB is unprotected until this recovers.", event_type="backup_failed",
                                  severity="critical",
                                  payload={"error": str(exc)[:500], "last_good_name": last.get("name"),
                                           "last_good_age_h": round(age, 1) if age is not None else None,
                                           "backup_dir": s.DB_BACKUP_DIR},
                                  dedupe_key=str(now_ts() // 86400), source="ops")
            raise
        if alerts is not None:
            await alerts.clear("backup_failed")
            if res.mirror == "missing":
                await alerts.fire("backup_mirror_missing", f"⚠️ backup mirror missing: {s.DB_BACKUP_MIRROR_DIR}",
                                  event_type="backup_mirror_missing", payload={"mirror_dir": s.DB_BACKUP_MIRROR_DIR,
                                                                               "marker": s.DB_BACKUP_MIRROR_MARKER},
                                  dedupe_key=str(now_ts() // 86400), source="ops")
            elif res.mirror == "failed":
                await alerts.fire("backup_mirror_failed", f"⚠️ backup mirror copy failed: {res.mirror_error}",
                                  event_type="backup_mirror_failed",
                                  payload={"mirror_dir": s.DB_BACKUP_MIRROR_DIR, "error": res.mirror_error},
                                  dedupe_key=str(now_ts() // 86400), source="ops")
            else:
                await alerts.clear("backup_mirror_missing")
                await alerts.clear("backup_mirror_failed")
        await self.set_state("last_result", {**res.manifest, "mirror": res.mirror, "pruned": res.pruned})
        mb = res.size_bytes / 2**20
        return JobOutcome(rows=1, note=f"{res.name} {mb:.1f} MB mirror={res.mirror}", meta={"pruned": res.pruned})


# ── verify / restore ──────────────────────────────────────────────────────────────────────────────────────────
def _find(stamp_or_path: str, source: Literal["local", "mirror"]) -> tuple[Path | None, Path | None]:
    """Return (data file .db or .db.gz, manifest)."""
    s = app_config.settings
    p = Path(stamp_or_path)
    if p.exists():
        man = p.with_name(p.name.replace(".db.gz", ".json").replace(".db", ".json"))
        return p, man if man.exists() else None
    base = Path(s.DB_BACKUP_MIRROR_DIR) if source == "mirror" else s.path(s.DB_BACKUP_DIR)
    for d in (base, base / "manual"):
        if not d.is_dir():
            continue
        for cand in sorted(d.glob(f"taosentinel-{stamp_or_path}*.json")):
            db = cand.with_suffix(".db")
            gz = cand.with_name(cand.stem + ".db.gz")
            return (db if db.exists() else gz if gz.exists() else None), cand
    return None, None


def _materialise(data: Path, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    if data.name.endswith(".gz"):
        with gzip.open(data, "rb") as src, open(out, "wb") as dst:
            shutil.copyfileobj(src, dst, _CHUNK)
    else:
        shutil.copyfile(data, out)


async def verify_snapshot(path: Path, *, full: bool = False, manifest: Path | None = None) -> tuple[bool, str]:
    def _do() -> tuple[bool, str]:
        s = app_config.settings
        tmpdir = s.path(s.DATA_DIR) / "tmp"
        work = tmpdir / f"verify-{os.getpid()}.db"
        try:
            _materialise(path, work)
            if manifest and manifest.exists():
                want = json.loads(manifest.read_text()).get("sha256")
                if want and _sha256_file(work) != want:
                    return False, "sha256 mismatch"
            con = sqlite3.connect(f"file:{work}?mode=ro", uri=True)
            try:
                rows = con.execute("PRAGMA integrity_check" if full else "PRAGMA quick_check").fetchall()
            finally:
                con.close()
            res = [str(r[0]) for r in rows]
            return (res == ["ok"]), "\n".join(res[:5])
        finally:
            work.unlink(missing_ok=True)
    return await asyncio.to_thread(_do)


def _service_running() -> bool:
    s = app_config.settings
    lock = _FileLock(s.path(s.RUN_DIR) / "taosentinel.lock")
    if not lock.acquire():
        return True
    lock.release()
    try:
        with socket.create_connection((s.HEALTH_HOST if s.HEALTH_HOST != "0.0.0.0" else "127.0.0.1", s.HEALTH_PORT),
                                      timeout=1):
            return True
    except OSError:
        return False


async def restore_snapshot(stamp_or_path: str, *, source: Literal["local", "mirror"] = "local",
                           target: Path | None = None, yes: bool = False) -> int:
    from app.storage.migrate import CODE_SCHEMA_VERSION

    s = app_config.settings
    live = s.path(s.DB_PATH)
    drill = target is not None and Path(target).resolve() != live.resolve()
    if not drill and _service_running():
        print("stop the service first: sudo systemctl stop taosentinel")
        return 2
    data, man = _find(stamp_or_path, source)
    if data is None:
        print(f"backup not found: {stamp_or_path}")
        return 1
    staged = s.path(s.DATA_DIR) / "tmp" / f"restore-{Path(data).name.split('.')[0]}.db"
    await asyncio.to_thread(_materialise, data, staged)
    if man is not None:
        want = json.loads(man.read_text()).get("sha256")
        if want and await asyncio.to_thread(_sha256_file, staged) != want:
            staged.unlink(missing_ok=True)
            print("sha256 mismatch — refusing")
            return 1
    con = sqlite3.connect(f"file:{staged}?mode=ro", uri=True)
    try:
        ic = [str(r[0]) for r in con.execute("PRAGMA integrity_check").fetchall()]
    finally:
        con.close()
    if ic != ["ok"]:
        staged.unlink(missing_ok=True)
        print("integrity_check failed — refusing:\n" + "\n".join(ic[:5]))
        return 1
    v = _snapshot_schema_version(staged)
    if v > CODE_SCHEMA_VERSION:
        staged.unlink(missing_ok=True)
        print("snapshot is newer than this code — check out a newer release")
        return 3
    if drill:
        dst = Path(target)
        dst.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged, dst)
        print(f"drill restore written to {dst} (schema_version {v})")
        return 0
    if not yes:
        print("live restore needs --yes")
        staged.unlink(missing_ok=True)
        return 2
    # spend carry-over (current + previous month): max(snapshot, current); copy _costs latches
    if live.exists():
        try:
            cur = sqlite3.connect(f"file:{live}?mode=ro", uri=True)
            snap = sqlite3.connect(str(staged))
            try:
                rows = cur.execute("SELECT provider, month, usd, requests, units_json, updated_at FROM provider_spend "
                                   "ORDER BY month DESC").fetchall()
                for r in rows:
                    snap.execute("INSERT INTO provider_spend (provider, month, usd, requests, units_json, updated_at) "
                                 "VALUES (?,?,?,?,?,?) ON CONFLICT(provider, month) DO UPDATE SET "
                                 "usd = MAX(usd, excluded.usd), requests = MAX(requests, excluded.requests)", r)
                for r in cur.execute("SELECT job, key, value_json, updated_at FROM job_state WHERE job = '_costs'"):
                    snap.execute("INSERT OR REPLACE INTO job_state (job, key, value_json, updated_at) VALUES (?,?,?,?)",
                                 r)
                snap.commit()
            finally:
                cur.close()
                snap.close()
        except sqlite3.Error as exc:
            print(f"spend carry-over skipped ({exc}); reconcile with `python -m app costs set <provider> --usd X`")
    qdir = s.path(s.DATA_DIR) / "quarantine" / _stamp()
    qdir.mkdir(parents=True, exist_ok=True)
    for suffix in ("", "-wal", "-shm"):
        p = Path(str(live) + suffix)
        if p.exists():
            shutil.move(str(p), qdir / p.name)
    os.replace(staged, live)
    for suffix in ("-wal", "-shm"):
        Path(str(live) + suffix).unlink(missing_ok=True)
    from app.control import integrity_path
    integrity_path().unlink(missing_ok=True)
    print(f"restored {data.name} → {live} (old DB quarantined in {qdir}). Next: sudo systemctl start taosentinel; "
          "check /health; collectors re-observe on their next tick.")
    return 0
