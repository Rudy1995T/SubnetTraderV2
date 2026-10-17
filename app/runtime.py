"""Process lifecycle (spec 01 §2.1, §2.5, §2.8; 16 §2.5–§2.6).

Order: 16 pre_init (instance lock, boot classification) → logs → DB + schema → limiter/Http/governor → AlertLatch →
pause cache → JobRegistry → scheduler start → health server task → READY=1 (nothing slow before it) → watchdog →
LongTasks → service_online notice → unclean-boot report. Shutdown is bounded (≤ SHUTDOWN_DEADLINE_SEC) and ends in
``_hard_exit(0)``.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
import socket
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Awaitable, Callable

import app.config as app_config
from app.logging.logger import logger
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    from app.costs import CostGovernor
    from app.http import Http
    from app.jobs import CollectorJob, LongTask
    from app.notifications.alerts import AlertLatch
    from app.ops.recovery import BootInfo
    from app.ratelimit import ProviderLimiter
    from app.storage.db import Database


@dataclass
class Runtime:
    started_at: int
    shutdown_event: asyncio.Event
    db: "Database | None" = None
    scheduler: "AsyncIOScheduler | None" = None
    limiter: "ProviderLimiter | None" = None
    http: "Http | None" = None
    costs: "CostGovernor | None" = None
    alerts: "AlertLatch | None" = None
    boot: "BootInfo | None" = None
    jobs: dict[str, "CollectorJob"] = field(default_factory=dict)
    tasks: dict[str, "LongTask"] = field(default_factory=dict)
    unavailable: dict[str, str] = field(default_factory=dict)
    task_handles: dict[str, asyncio.Task] = field(default_factory=dict)
    api_probe_fails: int = 0
    version: str = "0.1.0+unknown"
    pending_setup: list[Callable[[], Awaitable[None]]] = field(default_factory=list)
    watchdog_withheld: list[int] = field(default_factory=list)
    watchdog_active: bool = False
    unclean_boots_7d: int = 0
    shutting_down: bool = False
    bg_tasks: set = field(default_factory=set)


runtime: Runtime | None = None


def get_runtime() -> Runtime:
    if runtime is None:
        raise RuntimeError("runtime not initialised")
    return runtime


def get_runtime_or_none() -> Runtime | None:
    return runtime


def compute_version() -> str:
    from app import __version__
    from app.config import APP_ROOT
    try:
        sha = subprocess.run(["git", "-C", str(APP_ROOT), "rev-parse", "--short=7", "HEAD"], capture_output=True,
                             text=True, timeout=3).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        sha = ""
    return f"{__version__}+{sha or 'unknown'}"


# ── ported lifecycle helpers ─────────────────────────────────────────────────────────────────────────────────
def _hard_exit(code: int = 0) -> None:
    logging.shutdown()
    os._exit(code)


def _arm_shutdown_deadline(seconds: float) -> threading.Timer:
    def _fire() -> None:
        logger.error("shutdown deadline exceeded — hard exit", data={"seconds": seconds})
        _hard_exit(0)
    t = threading.Timer(seconds, _fire)
    t.daemon = True
    t.start()
    return t


async def _close_within(name: str, coro: Awaitable[Any], timeout: float) -> None:
    task = asyncio.ensure_future(coro)
    done, _ = await asyncio.wait({task}, timeout=timeout)
    if not done:
        task.cancel()
        logger.warning(f"{name} did not finish within {timeout}s — abandoned")
        return
    exc = task.exception()
    if exc is not None:
        logger.warning(f"{name} raised during shutdown", data={"error": f"{type(exc).__name__}: {exc}"})


def sd_notify(state: str) -> None:
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return
    if addr.startswith("@"):
        addr = "\0" + addr[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.connect(addr)
            sock.sendall(state.encode())
    except OSError as exc:
        logger.warning("sd_notify failed", data={"state": state, "error": str(exc)})


async def shutdown_services(rt: Runtime) -> None:
    s = app_config.settings
    t0 = time.monotonic()
    rt.shutting_down = True
    deadline = _arm_shutdown_deadline(s.SHUTDOWN_DEADLINE_SEC)
    sd_notify("STOPPING=1")
    if rt.scheduler is not None:
        try:
            rt.scheduler.shutdown(wait=False)
        except Exception:  # pragma: no cover
            pass
    handles = [h for k, h in rt.task_handles.items() if k != "health" and not h.done()]
    for h in handles:
        h.cancel()
    if handles:
        await asyncio.wait(handles, timeout=1)
    for t in list(rt.bg_tasks):
        t.cancel()
    if rt.costs is not None:
        await _close_within("cost flush", rt.costs.flush(), 1.0)
    if rt.db is not None:
        await _close_within("DB close", rt.db.close(), s.SHUTDOWN_DB_TIMEOUT_SEC)
    if rt.http is not None:
        await _close_within("HTTP close", rt.http.aclose(), s.SHUTDOWN_HTTP_TIMEOUT_SEC)
    from app.ops.recovery import release_lock, remove_heartbeat
    remove_heartbeat()
    release_lock()
    logger.info(f"Services shut down cleanly ({time.monotonic() - t0:.1f}s)")
    deadline.cancel()
    _hard_exit(0)


@contextlib.contextmanager
def _no_capture():
    yield


async def run_health_server(stop: asyncio.Event) -> None:
    import uvicorn

    from app.api.app import create_app
    s = app_config.settings
    if s.HEALTH_HOST not in ("127.0.0.1", "::1", "localhost") and not s.API_AUTH_TOKEN:
        logger.warning("API bound to a non-loopback address without API_AUTH_TOKEN — mutations are unauthenticated",
                       data={"host": s.HEALTH_HOST})
    config = uvicorn.Config(create_app(), host=s.HEALTH_HOST, port=s.HEALTH_PORT, log_level="warning",
                            access_log=False, lifespan="off")
    server = uvicorn.Server(config)
    if hasattr(server, "capture_signals"):
        server.capture_signals = _no_capture  # type: ignore[method-assign]
    else:
        logger.error("uvicorn.Server.capture_signals missing — SIGTERM may be hijacked (check the uvicorn version)")

    async def _drive_shutdown() -> None:
        await stop.wait()
        server.should_exit = True

    driver = asyncio.create_task(_drive_shutdown())
    try:
        await server.serve()
    finally:
        driver.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await driver


async def _self_probe(rt: Runtime) -> None:
    import httpx
    s = app_config.settings
    host = "127.0.0.1" if s.HEALTH_HOST in ("0.0.0.0", "::") else s.HEALTH_HOST
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"http://{host}:{s.HEALTH_PORT}/health")
        rt.api_probe_fails = 0 if r.status_code == 200 else rt.api_probe_fails + 1
    except Exception:
        rt.api_probe_fails += 1


async def systemd_watchdog_loop(stop: asyncio.Event) -> None:
    s = app_config.settings
    usec = os.environ.get("WATCHDOG_USEC")
    if not s.SYSTEMD_WATCHDOG_ENABLED or not usec:
        return
    rt = get_runtime()
    rt.watchdog_active = True
    interval = max(1.0, int(usec) / 2e6)
    from app.jobs import stale_ids
    while not stop.is_set():
        try:
            if app_config.settings.WATCHDOG_API_SELF_PROBE_ENABLED:
                await _self_probe(rt)
            stale = stale_ids(rt, now_ts())
            if not stale:
                sd_notify("WATCHDOG=1")
            else:
                rt.watchdog_withheld.append(now_ts())
                rt.watchdog_withheld = rt.watchdog_withheld[-500:]
                logger.error("watchdog_withheld", data={"stale": stale})
        except Exception as exc:  # pragma: no cover
            logger.error("watchdog loop error", data={"error": str(exc)})
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


# ── service construction ─────────────────────────────────────────────────────────────────────────────────────
async def init_core(rt: Runtime, *, alert_sink: Callable[[str], Awaitable[bool]] | None = None) -> None:
    """DB + schema, limiter, Http, governor, AlertLatch, pause cache, job registry (shared by serve and run-job)."""
    from app import control
    from app.costs import CostGovernor
    from app.http import Http
    from app.jobs import JobRegistry
    from app.notifications.alerts import AlertLatch
    from app.ratelimit import ProviderLimiter
    from app.storage.db import Database
    s = app_config.settings
    db = Database(s.path(s.DB_PATH))
    await db.connect()
    rt.db = db
    await db.apply_schema()
    limiter = ProviderLimiter.from_settings()
    rt.limiter = limiter
    http = Http(limiter, costs=None)
    rt.http = http
    costs = CostGovernor(db, limiter=limiter, alerts=None)
    await costs.load()
    rt.costs = costs
    http.costs = costs

    alerts = AlertLatch(db, sink=alert_sink)
    costs.alerts = alerts
    rt.alerts = alerts
    await control.refresh_cache(db)
    JobRegistry.build(rt)


async def init_services(rt: Runtime) -> None:
    from app.logging.retention import prune_old_logs
    from app.ops import recovery
    from app.util.paths import ensure_dirs
    ensure_dirs()
    info = await recovery.pre_init(rt)
    for w in app_config.settings.config_warnings:
        logger.warning("config", data={"warning": w})
    try:
        prune_old_logs()
    except Exception as exc:  # pragma: no cover
        logger.warning("prune_old_logs failed", data={"error": str(exc)})
    await init_core(rt)
    await recovery.record_boot(rt)
    try:
        boots = await recovery.boots(rt.db)
        rt.unclean_boots_7d = sum(1 for b in boots if b.get("kind") in ("process_crash", "host_unclean")
                                  and b.get("at", 0) >= now_ts() - 7 * 86400)
    except Exception:  # pragma: no cover
        pass
    _spawn(rt, recovery.post_boot_check(rt, info))
    _spawn(rt, run_startup_backup(rt))


def _spawn(rt: Runtime, coro: Awaitable[Any]) -> asyncio.Task:
    t = asyncio.ensure_future(coro)
    rt.bg_tasks.add(t)
    t.add_done_callback(rt.bg_tasks.discard)
    return t


async def run_startup_backup(rt: Runtime) -> None:
    s = app_config.settings
    try:
        await asyncio.wait_for(rt.shutdown_event.wait(), timeout=s.DB_BACKUP_STARTUP_DELAY_SEC)
        return
    except asyncio.TimeoutError:
        pass
    job = rt.jobs.get("backup_daily")
    if job is None or not s.DB_BACKUP_ENABLED:
        return
    from app import control
    from app.storage.backup import latest_backup_age_hours
    if control.integrity_hold_active():
        logger.info("startup backup skipped: integrity hold")
        return
    age = latest_backup_age_hours(s.path(s.DB_BACKUP_DIR))
    if age is not None and age < s.DB_BACKUP_MIN_AGE_HOURS:
        logger.info("startup backup skipped: recent snapshot", data={"age_hours": round(age, 1)})
        return
    await job.run(trigger="startup")


def _online_text(rt: Runtime) -> str:
    from app.jobs import JOB_TABLE
    s = app_config.settings
    avail = len(rt.jobs) + len(rt.tasks)
    return (f"🛰 <b>TaoSentinel online</b>\nversion {rt.version} · spec {s.SPEC_VERSION} · boot "
            f"{rt.boot.kind if rt.boot else '?'} · jobs {avail} available / {len(JOB_TABLE)} declared\n"
            f"health: http://{s.HEALTH_HOST}:{s.HEALTH_PORT}/health")


async def main(args: argparse.Namespace | None = None) -> None:
    global runtime
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):  # pragma: no cover
            pass
    rt = Runtime(started_at=now_ts(), shutdown_event=stop, version=compute_version())
    runtime = rt
    watchdog_task: asyncio.Task | None = None
    health_task: asyncio.Task | None = None
    try:
        await init_services(rt)
        from app.jobs import setup_scheduler
        sched = setup_scheduler(rt)
        sched.start()
        for fn in rt.pending_setup:
            try:
                await fn()
            except Exception as exc:  # pragma: no cover
                logger.warning("pending setup failed", data={"error": str(exc)})
        health_task = asyncio.create_task(run_health_server(stop))
        rt.task_handles["health"] = health_task
        sd_notify("READY=1")
        logger.info("READY", data={"version": rt.version, "jobs": sorted(rt.jobs), "tasks": sorted(rt.tasks)})
        watchdog_task = asyncio.create_task(systemd_watchdog_loop(stop))
        for tid, task in rt.tasks.items():
            rt.task_handles[tid] = asyncio.create_task(task.run_forever(stop))
        from app.ops import recovery
        recovery.write_heartbeat(rt)
        if rt.alerts is not None:
            await rt.alerts.fire("service_online", _online_text(rt))
            if rt.boot is not None:
                await recovery.report(rt, rt.boot)
        await stop.wait()
    finally:
        if watchdog_task is not None:
            watchdog_task.cancel()
        if health_task is not None:
            with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError, Exception):
                await asyncio.wait_for(asyncio.shield(health_task), 3)
            health_task.cancel()
        await shutdown_services(rt)


def entrypoint(argv: list[str] | None = None) -> None:
    if os.environ.get("TAOSENTINEL_PROCESS_ROLE") == "script":
        raise SystemExit("refusing to `serve` with TAOSENTINEL_PROCESS_ROLE=script")
    asyncio.run(main(None))
