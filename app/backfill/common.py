"""Shared plumbing for the Phase 3 backfill steps (spec 17 §2.0, §8.2).

Standalone scripts do their heavy work on a synchronous ``sqlite3`` connection from ``open_db`` (WAL, busy timeout,
``src_rank`` registered from 03's ``precedence.rank_any``) after 03's ``Database.apply_schema()`` ran once; events
go through 03's ``emit_event`` on that ``Database``.
"""
from __future__ import annotations

import asyncio
import fcntl
import json
import os
import shutil
import signal
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator

import app.config as app_config
from app.logging.logger import logger
from app.storage import precedence
from app.util.fs import write_atomic
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

# 17 §2.1 step 1: the objects 17's code relies on (03 schema v1 + migration 0002).
REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "pool_history": ("occupant_key", "source", "event_at", "observed_at", "price_method", "enriched_at", "run_id"),
    "price_bars": ("backfilled", "ts_last", "tmc_block_number", "run_id"),
    "backfill_buckets": ("era", "bucket_ts", "status"),
    "block_times": ("block_number", "ts_ms", "endpoint"),
    "rpc_owner_history": ("netuid", "block_number", "occupant_key", "change"),
    "occupants": ("registered_ts", "deregistered_ts"),
}

EXIT_OK, EXIT_PRECONDITION, EXIT_DISABLED, EXIT_DECODE, EXIT_VERIFY, EXIT_INTERRUPTED = 0, 2, 3, 4, 5, 130


class BackfillBusy(RuntimeError):
    pass


class PreconditionError(RuntimeError):
    """A refusal before any write (exit 2); ``code`` may be 3 for disabled/missing dependency."""

    def __init__(self, msg: str, code: int = EXIT_PRECONDITION) -> None:
        super().__init__(msg)
        self.code = code


def settings():
    return app_config.settings


def backfill_dir() -> Path:
    s = settings()
    d = s.path(s.BACKFILL_DIR)
    d.mkdir(parents=True, exist_ok=True)
    return d


# ── lock ─────────────────────────────────────────────────────────────────────────────────────────────────────
class BackfillLock:
    """``fcntl.flock(LOCK_EX | LOCK_NB)``: two standalone backfill processes never run at once (17 §2.0)."""

    def __init__(self, path: Path | None = None) -> None:
        self.path = path or backfill_dir() / "backfill.lock"
        self._fd: int | None = None

    def __enter__(self) -> "BackfillLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            raise BackfillBusy(f"another backfill holds {self.path}") from None
        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode())
        self._fd = fd
        return self

    def __exit__(self, *exc: Any) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
            finally:
                os.close(self._fd)
                self._fd = None


# ── service / stop ───────────────────────────────────────────────────────────────────────────────────────────
def service_is_up(timeout: float = 2.0) -> bool:
    import httpx
    s = settings()
    try:
        r = httpx.get(f"http://{s.HEALTH_HOST}:{s.HEALTH_PORT}/health", timeout=timeout)
        return r.status_code == 200
    except Exception:
        return False


_stop = {"signal": None}


def install_signal_handlers() -> None:
    """SIGINT/SIGTERM → graceful stop at the next unit boundary (17 §5 standalone semantics)."""
    def _h(signum, _frame) -> None:
        if _stop["signal"] is not None:  # second signal: hard stop
            raise KeyboardInterrupt
        _stop["signal"] = signum
        logger.warning("stop requested; finishing in-flight units", data={"signal": signum})
    signal.signal(signal.SIGINT, _h)
    signal.signal(signal.SIGTERM, _h)


def signalled() -> bool:
    return _stop["signal"] is not None


def reset_stop() -> None:
    _stop["signal"] = None


def stop_requested() -> bool:
    from app.control import pause_all_active
    return signalled() or pause_all_active()


def free_gb(path: Path) -> float:
    return shutil.disk_usage(path).free / 1e9


def check_disk() -> None:
    s = settings()
    d = s.path(s.DATA_DIR)
    d.mkdir(parents=True, exist_ok=True)
    if free_gb(d) < s.BACKFILL_MIN_FREE_GB:
        raise PreconditionError(f"free disk {free_gb(d):.1f} GB < BACKFILL_MIN_FREE_GB {s.BACKFILL_MIN_FREE_GB}")


def check_enabled() -> None:
    if not settings().BACKFILL_ENABLED:
        raise PreconditionError("BACKFILL_ENABLED=false", code=EXIT_DISABLED)


# ── sidecar ──────────────────────────────────────────────────────────────────────────────────────────────────
class Sidecar:
    """Resumable progress file, rewritten atomically (04 ``write_atomic``)."""

    def __init__(self, path: Path, default: dict | None = None) -> None:
        self.path = Path(path)
        self.data: dict = default or {}
        self.load()

    def load(self) -> dict:
        try:
            self.data = json.loads(self.path.read_text())
        except (OSError, ValueError):
            pass
        return self.data

    def save(self) -> None:
        write_atomic(self.path, self.data)

    def unit(self, task: str, key: str) -> dict:
        return self.data.setdefault("tasks", {}).setdefault(task, {}).setdefault(str(key), {})

    def mark(self, task: str, key: str, **fields: Any) -> None:
        self.unit(task, key).update(fields)


# ── DB ───────────────────────────────────────────────────────────────────────────────────────────────────────
def src_rank(source: str | None) -> int:
    return precedence.rank_any(source)


def register_sql_functions(con: sqlite3.Connection) -> None:
    con.create_function("src_rank", 1, precedence.rank_any, deterministic=True)


def open_db(path: Path | str | None = None, *, check: bool = True) -> sqlite3.Connection:
    """Synchronous connection for standalone steps; the schema must already be applied (``prepare_db``)."""
    s = settings()
    p = Path(path) if path is not None else s.path(s.DB_PATH)
    con = sqlite3.connect(str(p), isolation_level=None, check_same_thread=False, timeout=s.DB_BUSY_TIMEOUT_MS / 1000)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode = WAL")
    con.execute(f"PRAGMA busy_timeout = {int(s.DB_BUSY_TIMEOUT_MS)}")
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("PRAGMA synchronous = NORMAL")
    register_sql_functions(con)
    if check:
        missing = missing_objects(con)
        if missing:
            con.close()
            raise PreconditionError("schema lacks 17's objects: " + ", ".join(missing), code=EXIT_DISABLED)
    return con


def missing_objects(con: sqlite3.Connection) -> list[str]:
    out: list[str] = []
    for table, cols in REQUIRED_COLUMNS.items():
        have = {r[1] for r in con.execute(f"PRAGMA table_xinfo({table})")}
        if not have:
            out.append(table)
            continue
        out.extend(f"{table}.{c}" for c in cols if c not in have)
    return out


class SpendBook:
    """Request ledger for standalone scripts (02 §G1): ``provider_spend.requests`` is the one request count.

    ``seed`` loads this month's booked requests into the script's own ``ProviderLimiter`` (so a monthly cap such as
    ``BLOCKMACHINE_MONTHLY_CAP_REQ`` holds across processes); ``add`` buffers each metered request and ``flush``
    increments the row with the same UPSERT as 16's governor (never overwrites, so the service can write too).
    """

    def __init__(self, db_path: Path | str | None = None, *, flush_every: int = 500) -> None:
        s = settings()
        self.path = Path(db_path) if db_path is not None else s.path(s.DB_PATH)
        self.flush_every = int(flush_every)
        self._pending: dict[tuple[str, str], int] = {}

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self.path), isolation_level=None, timeout=settings().DB_BUSY_TIMEOUT_MS / 1000)

    def seed(self, limiter: Any, providers: Any) -> None:
        from app.util.time import month_key
        month = month_key(now_ts())
        try:
            con = self._connect()
            try:
                for p in sorted(set(providers)):
                    r = con.execute("SELECT requests FROM provider_spend WHERE provider = ? AND month = ?",
                                    (p, month)).fetchone()
                    if r is not None and p in limiter.limits:
                        limiter.seed_month(p, int(r[0]) + limiter.month_requests(p))
            finally:
                con.close()
        except sqlite3.Error as exc:
            logger.warning("spend book: seed failed", data={"error": str(exc)[:200]})

    def add(self, provider: str, n: int = 1) -> None:
        from app.util.time import month_key
        k = (provider, month_key(now_ts()))
        self._pending[k] = self._pending.get(k, 0) + int(n)
        if sum(self._pending.values()) >= self.flush_every:
            self.flush()

    def flush(self) -> int:
        pending, self._pending = self._pending, {}
        if not pending:
            return 0
        try:
            con = self._connect()
            try:
                for (p, m), reqs in pending.items():
                    con.execute(
                        "INSERT INTO provider_spend (provider, month, usd, requests, units_json, updated_at) "
                        "VALUES (?,?,0.0,?,'{}',?) ON CONFLICT(provider, month) DO UPDATE SET "
                        "requests = requests + excluded.requests, updated_at = excluded.updated_at",
                        (p, m, reqs, now_ts()))
            finally:
                con.close()
        except sqlite3.Error as exc:
            for k, v in pending.items():
                self._pending[k] = self._pending.get(k, 0) + v
            logger.warning("spend book: flush failed (kept in memory)", data={"error": str(exc)[:200]})
            return 0
        return len(pending)


async def prepare_db(path: Path | str | None = None) -> "Database":
    """Connect 03's ``Database`` and apply the schema (``migrate()``) once; used for events and the schema."""
    from app.storage.db import Database
    s = settings()
    db = Database(Path(path) if path is not None else s.path(s.DB_PATH), reader=False)
    await db.connect()
    await db.apply_schema()
    return db


@contextmanager
def tx(con: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    con.execute("BEGIN IMMEDIATE")
    try:
        yield con
    except BaseException:
        con.execute("ROLLBACK")
        raise
    else:
        con.execute("COMMIT")


# ── collector_runs + events ──────────────────────────────────────────────────────────────────────────────────
@dataclass
class StepRun:
    job: str
    run_id: str
    started_at: int
    rows: int = 0
    note: str | None = None
    meta: dict = field(default_factory=dict)
    ok: bool | None = None
    error: str | None = None
    t0: float = field(default_factory=time.monotonic)

    @property
    def seconds(self) -> float:
        return round(time.monotonic() - self.t0, 3)


@contextmanager
def step_run(con: sqlite3.Connection, job: str) -> Iterator[StepRun]:
    """One ``collector_runs`` row per step; ``run_id = script:<job>:<started_at>`` (03 §2.1)."""
    started = now_ts()
    run = StepRun(job=job, run_id=f"script:{job}:{started}", started_at=started)
    con.execute("INSERT OR IGNORE INTO collector_runs (run_id, job, started_at) VALUES (?,?,?)",
                (run.run_id, job, started))
    try:
        yield run
    except BaseException as exc:
        run.ok = False
        run.error = f"{type(exc).__name__}: {exc}"[:500]
        con.execute("UPDATE collector_runs SET finished_at=?, ok=0, rows=?, error=?, duration_sec=?, note=?, "
                    "meta_json=? WHERE run_id=?",
                    (now_ts(), run.rows, run.error, run.seconds, run.note,
                     json.dumps(run.meta, default=str) if run.meta else None, run.run_id))
        raise
    else:
        run.ok = True
        con.execute("UPDATE collector_runs SET finished_at=?, ok=1, rows=?, duration_sec=?, note=?, meta_json=? "
                    "WHERE run_id=?",
                    (now_ts(), run.rows, run.seconds, run.note,
                     json.dumps(run.meta, default=str) if run.meta else None, run.run_id))


async def emit_step_event(db: "Database", kind: str, *, job: str, era: str | None, run_id: str,
                          **payload: Any) -> int | None:
    """``backfill_done`` / ``backfill_failed`` (17 §3.4): system event, ``source='runtime'``, dedupe ``job|run_id``."""
    from app.storage.events import emit_event
    if kind not in ("backfill_done", "backfill_failed"):
        raise ValueError(kind)
    s = settings()
    now = now_ts()
    body = {"job": job, "era": era, **payload}
    if kind == "backfill_done":
        body.setdefault("rows", 0)
        body.setdefault("seconds", 0)
        cov = s.path(s.COVERAGE_PATH)
        body.setdefault("coverage_path", str(s.COVERAGE_PATH) if cov.exists() else None)
    else:
        body.setdefault("error", "")
        body.setdefault("resume_hint", "")
    return await emit_event(db, event_type=kind, occupant_key=None, netuid=None, event_at=now, observed_at=now,
                            payload=body, dedupe_key=f"{job}|{run_id}", source="runtime", run_id=run_id,
                            alerted_at=None if s.BACKFILL_ALERTS_ENABLED else now)


def emit_step_event_sync(db_path: Path | str | None, kind: str, **kw: Any) -> None:
    """Sync steps: open a short-lived ``Database`` and emit (17 §8.1 'via asyncio.run after their sync work')."""
    async def _go() -> None:
        db = await prepare_db(db_path)
        try:
            await emit_step_event(db, kind, **kw)
        finally:
            await db.close()
    asyncio.run(_go())


def sha256_file(path: Path, limit_bytes: int | None = None) -> str:
    import hashlib
    h = hashlib.sha256()
    n = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
            n += len(chunk)
            if limit_bytes is not None and n >= limit_bytes:
                break
    return h.hexdigest()
