"""``/health`` builder (owner 01, 02 §G6), ``/api/health/services`` and ``/api/v1/jobs``.

Never 500: every block is built in its own try/except and degrades to ``{"error": …}``.
"""
from __future__ import annotations

import inspect
import json
import shutil
import sqlite3
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import app.config as app_config
from app import control
from app.util.time import now_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.runtime import Runtime

router = APIRouter()
_get_runtime: Callable[[], "Runtime | None"] | None = None


def configure(get_runtime: Callable[[], "Runtime | None"]) -> None:
    global _get_runtime
    _get_runtime = get_runtime


def _rt() -> "Runtime | None":
    if _get_runtime is None:
        return None
    try:
        return _get_runtime()
    except Exception:
        return None


class _Compact(JSONResponse):
    def render(self, content: Any) -> bytes:
        return json.dumps(content, separators=(",", ":"), ensure_ascii=False, default=str).encode("utf-8")


def _block(fn: Callable[[], Any]) -> Any:
    try:
        return fn()
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


_sec_cache: tuple[float, dict] | None = None


def _security() -> dict:
    global _sec_cache
    if _sec_cache and time.monotonic() - _sec_cache[0] < 300:
        return _sec_cache[1]
    from app.ops.security import selfcheck
    res = selfcheck(app_config.settings)
    _sec_cache = (time.monotonic(), res)
    return res


async def build_health(rt: "Runtime | None") -> dict:
    s = app_config.settings
    now = now_ts()
    from app import __version__
    version = rt.version if rt else f"{__version__}+unknown"
    out: dict[str, Any] = {"status": "initializing", "timestamp": to_iso(now), "version": version,
                           "spec_version": s.SPEC_VERSION, "uptime_sec": (now - rt.started_at) if rt else 0,
                           "degraded_reasons": []}
    reasons: list[str] = []
    out["kill_switch_active"] = _block(control.kill_switch_active)
    out["pause_all_active"] = _block(control.pause_all_active)
    out["scoring_killed"] = _block(control.scoring_killed)
    if rt is None:
        out["telegram_configured"] = s.telegram_configured()
        return out

    def _sched() -> dict:
        from app.jobs import JOB_TABLE
        running = bool(rt.scheduler is not None and rt.scheduler.running)
        return {"running": running, "declared": len(JOB_TABLE), "available": len(rt.jobs) + len(rt.tasks),
                "unavailable": sorted(rt.unavailable)}
    out["scheduler"] = _block(_sched)

    def _collectors() -> dict:
        res = {}
        for jid, job in rt.jobs.items():
            st = _block(job.status)
            res[jid] = st
            if isinstance(st, dict) and st.get("enabled") and not st.get("paused") \
                    and st.get("consecutive_failures", 0) >= job.fail_alert_threshold():
                reasons.append(f"collector_failing:{jid}")
        return res
    out["collectors"] = _block(_collectors)

    def _tasks() -> dict:
        res = {}
        for tid, task in rt.tasks.items():
            st = _block(task.status)
            res[tid] = st
            if isinstance(st, dict) and st.get("enabled") and not st.get("paused") and not task.connected:
                since = task.connected_since or task.last_attempt or rt.started_at
                if now - max(since, rt.started_at) >= 300:
                    reasons.append(f"task_disconnected:{tid}")
        return res
    out["tasks"] = _block(_tasks)

    def _stale() -> None:
        from app.jobs import stale_ids
        for jid in stale_ids(rt, now):
            if jid not in ("api_task", "api_probe"):
                reasons.append(f"job_stale:{jid}")
    _block(_stale)

    def _spend() -> dict:
        snap = rt.costs.snapshot() if rt.costs is not None else {}
        for g, v in snap.items():
            if v.get("paused") and v.get("paused_reason") != "kill_switch":
                reasons.append(f"budget_paused:{g}")
        return snap
    out["spend"] = _block(_spend)
    out["rate_limits"] = _block(lambda: rt.limiter.snapshot() if rt.limiter is not None else {})

    if "owner_collect_daily" in rt.jobs or "integrity_features" in rt.jobs:   # Phase 7 (11 §7)
        try:
            from app.integrity.health import integrity_health
            out["integrity"] = await integrity_health(rt)
        except Exception as exc:  # pragma: no cover
            out["integrity"] = {"error": f"{type(exc).__name__}: {exc}"}

    # db block
    try:
        db = rt.db
        if db is None or not db.connected:
            out["db"] = {"error": "not connected"}
        else:
            st = await db.stats()
            from app.storage.backup import latest_backup, latest_backup_age_hours
            bdir = s.path(s.DB_BACKUP_DIR)
            lb = latest_backup(bdir)
            age = latest_backup_age_hours(bdir)
            orphans = 0
            try:
                orphans = int(await db.fetchval(
                    "SELECT COUNT(*) FROM collector_runs WHERE finished_at IS NULL AND started_at < ?",
                    (now - 6 * 3600,)) or 0)
            except Exception:
                pass
            out["db"] = {"path": st["path"], "size_bytes": st["size_bytes"], "wal_bytes": st["wal_bytes"],
                         "schema_version": db.schema_version, "schema_warnings": db.schema_warnings,
                         "sqlite_version": sqlite3.sqlite_version, "reader": db.reader_enabled,
                         "last_backup": (lb or {}).get("name"),
                         "last_backup_age_hours": round(age, 2) if age is not None else None, "orphan_runs": orphans}
            if db.schema_warnings:
                reasons.append("schema_warning")
            if st["wal_bytes"] > s.DB_WAL_WARN_MB * 2**20:
                reasons.append("wal_large")
            if s.DB_BACKUP_ENABLED and ((age is not None and age > s.DB_BACKUP_STALE_HOURS) or
                                        (age is None and now - rt.started_at > s.DB_BACKUP_STALE_HOURS * 3600)):
                reasons.append("backup_stale")
    except Exception as exc:
        out["db"] = {"error": f"{type(exc).__name__}: {exc}"}

    # workbench constants probe (19 §6, 02 §K7): stale after 48 h without a successful probe
    try:
        if s.WB_ENABLED and "chain_constants_probe" in rt.jobs and rt.db is not None and rt.db.connected:
            last = await rt.db.fetchval("SELECT MAX(observed_at) FROM chain_constants WHERE source = 'rpc_storage' "
                                        "AND name = 'NetworkRegistrationCost'")
            if now - int(last or rt.started_at) > 48 * 3600:
                reasons.append("chain_probe_stale")
    except Exception:
        pass

    # alerts block
    try:
        from app.notifications.alerts import dispatcher_active
        pending = 0
        if rt.db is not None and rt.db.connected:
            pending = int(await rt.db.fetchval("SELECT COUNT(*) FROM events WHERE alerted_at IS NULL") or 0)
        out["alerts"] = {"dispatcher_active": await dispatcher_active(rt), "pending_immediate": pending,
                         "latched": sorted(rt.alerts.snapshot()) if rt.alerts else []}
    except Exception as exc:
        out["alerts"] = {"error": str(exc)}

    # scores (12's scores_status once scoring exists; async since Phase 8)
    def _scores() -> Any:
        try:
            from app.scoring import engine  # type: ignore[attr-defined]
        except Exception:
            return None
        fn = getattr(engine, "scores_status", None)
        return fn(rt) if fn else None
    out["scores"] = _block(_scores)
    if inspect.isawaitable(out["scores"]):
        try:
            out["scores"] = await out["scores"]
        except Exception as exc:
            out["scores"] = {"error": f"{type(exc).__name__}: {exc}"}
    if isinstance(out["scores"], dict) and out["scores"].get("stale") and s.SCORING_ENABLED:
        reasons.append("scores_stale")

    def _spec_unknown() -> None:
        try:
            from app.scoring.spec import SPECS  # type: ignore[attr-defined]
        except Exception:
            return
        if s.SPEC_VERSION not in SPECS:
            reasons.append("spec_unknown")
    _block(_spec_unknown)

    # ops block (16 §2.13.1)
    def _ops() -> dict:
        from app.ops.guard import disk_state, mirror_state
        ds = disk_state()
        if ds["free_gb"] < s.DISK_FREE_CRITICAL_GB:
            reasons.append("disk_critical")
        elif ds["free_gb"] < s.DISK_FREE_MIN_GB:
            reasons.append("disk_low")
        mir = mirror_state()
        if mir == "missing":
            reasons.append("mirror_missing")
        if control.pause_all_file_active():
            reasons.append("pause_all")
        if control.integrity_hold_active():
            reasons.append("db_integrity")
        boot = rt.boot
        deploy = {}
        dp = s.path(s.RUN_DIR) / "deploy-state.json"
        if dp.exists():
            try:
                deploy = json.loads(dp.read_text())
            except (OSError, ValueError):
                deploy = {"error": "unreadable"}
        from app.storage.backup import latest_backup, latest_backup_age_hours
        age = latest_backup_age_hours(s.path(s.DB_BACKUP_DIR))
        return {
            "boot_kind": boot.kind if boot else None, "started_at": to_iso(rt.started_at),
            "downtime_before_boot_sec": boot.downtime_sec if boot else None,
            "unclean_boots_7d": rt.unclean_boots_7d, "pause_all": control.pause_all_active(),
            "watchdog": {"enabled": bool(s.SYSTEMD_WATCHDOG_ENABLED and rt.watchdog_active),
                         "withheld_24h": sum(1 for t in rt.watchdog_withheld if t >= now - 86400),
                         "api_probe_fails": rt.api_probe_fails},
            "disk": {"free_gb": ds["free_gb"], "min_gb": ds["min_gb"], "critical_gb": ds["critical_gb"]},
            "backup": {"last_name": (latest_backup(s.path(s.DB_BACKUP_DIR)) or {}).get("name"), "mirror": mir,
                       "stale": bool(age is not None and age > s.DB_BACKUP_STALE_HOURS)},
            "deploy": deploy, "security": _security(),
        }
    out["ops"] = _block(_ops)
    out["telegram_configured"] = s.telegram_configured()

    ready = bool(rt.db is not None and rt.db.connected and rt.scheduler is not None and rt.scheduler.running
                 and not rt.shutting_down)
    out["degraded_reasons"] = sorted(set(reasons))
    out["status"] = "initializing" if not ready else ("degraded" if out["degraded_reasons"] else "ok")
    # key order as documented (status first); re-assemble
    order = ["status", "timestamp", "version", "spec_version", "uptime_sec", "degraded_reasons", "kill_switch_active",
             "pause_all_active", "scoring_killed", "scheduler", "collectors", "tasks", "spend", "rate_limits", "db",
             "alerts", "scores", "ops", "telegram_configured"]
    return {k: out[k] for k in order if k in out}


@router.get("/health")
async def health() -> JSONResponse:
    return _Compact(await build_health(_rt()))


# ── /api/health/services (01 Phase-0 subset + spec 14 §3.5 probes) ─────────────────────────────────────────────
# Active probes make one cheap request through ``Http.request(provider, …)``; passive probes read /health,
# the limiter/governor snapshot and collector_runs only (no TMC/Discord/X spend). The whole response is cached
# for HEALTH_SERVICES_CACHE_SEC.
_services_cache: tuple[float, dict] | None = None


def _svc(name: str, state: str, detail: str, latency_ms: int | None = None) -> dict:
    return {"ok": state != "fail", "state": state, "name": name, "detail": detail, "last_check": to_iso(now_ts()),
            "latency_ms": latency_ms}


async def _last_ok(rt: "Runtime", job: str) -> int | None:
    v = await rt.db.fetchval("SELECT MAX(finished_at) FROM collector_runs WHERE job = ? AND ok = 1", (job,))
    return None if v is None else int(v)


def _ago(ts: int | None) -> str:
    from app.notifications.format import fmt_age
    return "never" if ts is None else f"{fmt_age(now_ts() - ts)} ago"


async def probe_database(rt: "Runtime") -> dict:
    t0 = time.monotonic()
    mode = await rt.db.fetchval("PRAGMA journal_mode", writer=True)
    latest = await rt.db.fetchval("SELECT MAX(as_of) FROM scores WHERE spec_version = ?",
                                  (app_config.settings.SPEC_VERSION,))
    slot = to_iso(latest)[:16] + "Z" if latest else "none"
    return _svc("SQLite", "ok", f"{str(mode).lower()} · latest slot {slot}", int((time.monotonic() - t0) * 1000))


async def probe_scheduler(rt: "Runtime") -> dict:
    running = bool(rt.scheduler is not None and rt.scheduler.running)
    det = f"{len(rt.jobs) + len(rt.tasks)} jobs · {len(rt.unavailable)} unavailable"
    return _svc("APScheduler", "ok" if running else "fail", det if running else f"not running · {det}")


async def probe_disk(rt: "Runtime") -> dict:
    s = app_config.settings
    free = shutil.disk_usage(s.path(s.DATA_DIR)).free / 2**30
    return _svc("Disk", "ok" if free >= s.DISK_FREE_MIN_GB else "fail", f"{free:.1f} GB free")


async def probe_backup(rt: "Runtime") -> dict:
    s = app_config.settings
    if not s.DB_BACKUP_ENABLED:
        return _svc("Backup", "disabled", "DB_BACKUP_ENABLED=false")
    from app.storage.backup import latest_backup, latest_backup_age_hours
    bdir = s.path(s.DB_BACKUP_DIR)
    lb = latest_backup(bdir)
    age = latest_backup_age_hours(bdir)
    raw = await rt.db.fetchval("SELECT value_json FROM job_state WHERE job = 'backup_daily' AND key = 'last_result'")
    mirror = (json.loads(raw).get("mirror") if raw else None) or "off"
    ok = age is not None and age <= s.DB_BACKUP_STALE_HOURS and mirror in ("ok", "off")
    name = (lb or {}).get("name") or "none"
    det = f"{name} · {age:.1f} h · mirror {mirror}" if age is not None else f"no backup yet · mirror {mirror}"
    return _svc("Backup", "ok" if ok else "fail", det)


async def probe_telegram(rt: "Runtime") -> dict:
    s = app_config.settings
    if not s.telegram_configured():
        return _svc("Telegram", "disabled", "no token / chat id")
    t0 = time.monotonic()
    from app.notifications.telegram import get_me
    me = await get_me()
    ok = bool(me.get("ok"))
    det = f"@{me.get('result', {}).get('username', '?')}" if ok else str(me.get("description", "getMe failed"))
    return _svc("Telegram", "ok" if ok else "fail", det, int((time.monotonic() - t0) * 1000))


async def probe_github(rt: "Runtime") -> dict:
    s = app_config.settings
    if not s.GITHUB_TOKEN:
        return _svc("GitHub", "disabled", "no GITHUB_TOKEN")
    from app.http import get_http
    t0 = time.monotonic()
    r = await get_http().request("github", "GET", "https://api.github.com/rate_limit", timeout=8, retries=0,
                                 ok_statuses=(200, 401, 403))
    lat = int((time.monotonic() - t0) * 1000)
    if r.status_code != 200:
        return _svc("GitHub", "fail", f"HTTP {r.status_code}", lat)
    core = (r.json().get("resources") or {}).get("core") or {}
    reset = to_iso(core.get("reset"))
    return _svc("GitHub", "ok", f"{core.get('remaining', '?')}/{core.get('limit', '?')} · resets "
                f"{reset[11:16] + 'Z' if reset else '?'}", lat)


async def probe_subtensor_rpc(rt: "Runtime") -> dict:
    s = app_config.settings
    if not s.SUBTENSOR_RPC_ENABLED:
        return _svc("Subtensor RPC", "disabled", "SUBTENSOR_RPC_ENABLED=false")
    from app.http import get_http
    url = s.SUBTENSOR_RPC_URL.replace("wss://", "https://").replace("ws://", "http://")
    t0 = time.monotonic()
    r = await get_http().request("subtensor_rpc", "POST", url, timeout=8, retries=0, ok_statuses=(200,),
                                 json={"jsonrpc": "2.0", "id": 1, "method": "system_health", "params": []})
    lat = int((time.monotonic() - t0) * 1000)
    body = r.json()
    if body.get("error"):
        return _svc("Subtensor RPC", "fail", str(body["error"])[:120], lat)
    res = body.get("result") or {}
    return _svc("Subtensor RPC", "ok", f"peers {res.get('peers', '?')} · syncing "
                f"{str(res.get('isSyncing', '?')).lower()}", lat)


async def probe_anthropic(rt: "Runtime") -> dict:
    s = app_config.settings
    if not s.LLM_ENABLED or not s.ANTHROPIC_API_KEY:
        return _svc("Anthropic", "disabled", "LLM_ENABLED=false" if not s.LLM_ENABLED else "no ANTHROPIC_API_KEY")
    snap = (rt.costs.snapshot() if rt.costs is not None else {}).get("llm") or {}
    spend = f"llm ${snap.get('usd', 0):.2f}/${snap.get('cap_usd') or 0:.0f}"
    if snap.get("paused"):
        return _svc("Anthropic", "fail", f"budget paused · {spend}")
    from app.http import get_http
    t0 = time.monotonic()
    r = await get_http().request("anthropic", "GET", "https://api.anthropic.com/v1/models", timeout=8, retries=0,
                                 headers={"x-api-key": s.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01"},
                                 ok_statuses=(200, 401, 403))
    lat = int((time.monotonic() - t0) * 1000)
    return _svc("Anthropic", "ok" if r.status_code == 200 else "fail",
                f"ok · {spend}" if r.status_code == 200 else f"HTTP {r.status_code}", lat)


async def probe_tmc(rt: "Runtime") -> dict:
    snap = (rt.costs.snapshot() if rt.costs is not None else {}).get("tmc") or {}
    last = await _last_ok(rt, "tmc_hourly")
    month = f"month {snap.get('requests', 0):,}/{snap.get('request_cap') or 0:,}"
    ok = not snap.get("paused") and last is not None and now_ts() - last <= 2 * 3600
    return _svc("TaoMarketCap REST", "ok" if ok else "fail", f"{month} · last ok {_ago(last)}")


async def probe_tmc_sse(rt: "Runtime") -> dict:
    task = rt.tasks.get("chain_sse")
    if task is None or not task.enabled():
        return _svc("TaoMarketCap SSE", "disabled", "chain_sse not running")
    beat = task.last_beat
    ok = bool(task.connected and beat is not None and now_ts() - beat <= 120)
    return _svc("TaoMarketCap SSE", "ok" if ok else "fail",
                f"{'connected' if task.connected else 'disconnected'} · last beat {_ago(beat)}")


async def probe_discord(rt: "Runtime") -> dict:
    if not app_config.settings.DISCORD_ENABLED:
        return _svc("Discord", "disabled", "DISCORD_ENABLED=false")
    last = await _last_ok(rt, "discord_6h")
    ok = last is not None and now_ts() - last <= 7 * 3600
    return _svc("Discord", "ok" if ok else "fail", f"last ok {_ago(last)}")


async def probe_x(rt: "Runtime") -> dict:
    s = app_config.settings
    if s.X_PROVIDER == "null":
        return _svc("X", "disabled", "X_PROVIDER=null")
    snap = (rt.costs.snapshot() if rt.costs is not None else {}).get("x") or {}
    last = await _last_ok(rt, "x_team_poll")
    ok = not snap.get("paused") and last is not None and now_ts() - last <= 26 * 3600
    return _svc("X", "ok" if ok else "fail", f"{s.X_PROVIDER} · last ok {_ago(last)} · "
                f"${snap.get('usd', 0):.2f}/${snap.get('cap_usd') or 0:.0f}")


PROBES: dict[str, Callable[["Runtime"], Any]] = {
    "database": probe_database, "scheduler": probe_scheduler, "disk": probe_disk, "backup": probe_backup,
    "telegram": probe_telegram, "github": probe_github, "subtensor_rpc": probe_subtensor_rpc,
    "anthropic": probe_anthropic, "tmc": probe_tmc, "tmc_sse": probe_tmc_sse, "discord": probe_discord, "x": probe_x,
}
_NAMES = {"database": "SQLite", "scheduler": "APScheduler", "disk": "Disk", "backup": "Backup",
          "telegram": "Telegram", "github": "GitHub", "subtensor_rpc": "Subtensor RPC", "anthropic": "Anthropic",
          "tmc": "TaoMarketCap REST", "tmc_sse": "TaoMarketCap SSE", "discord": "Discord", "x": "X"}


async def build_services(rt: "Runtime | None") -> dict:
    import asyncio
    s = app_config.settings
    if rt is None or rt.db is None or not rt.db.connected:
        return {"timestamp": to_iso(now_ts()), "cached": False,
                "services": {k: _svc(_NAMES[k], "fail", "runtime not ready") for k in PROBES}}

    async def _one(key: str, fn) -> tuple[str, dict]:
        try:
            return key, await asyncio.wait_for(fn(rt), timeout=float(s.HEALTH_PROBE_TIMEOUT_SEC))
        except Exception as exc:
            msg = str(exc).replace(s.TELEGRAM_BOT_TOKEN, "***") if s.TELEGRAM_BOT_TOKEN else str(exc)
            return key, _svc(_NAMES.get(key, key), "fail", f"{type(exc).__name__}: {msg}"[:200])
    pairs = await asyncio.gather(*(_one(k, f) for k, f in PROBES.items()))
    return {"timestamp": to_iso(now_ts()), "cached": False, "services": dict(pairs)}


@router.get("/api/health/services")
async def services() -> JSONResponse:
    global _services_cache
    s = app_config.settings
    if _services_cache and time.monotonic() - _services_cache[0] < s.HEALTH_SERVICES_CACHE_SEC:
        return _Compact({**_services_cache[1], "cached": True})
    body = await build_services(_rt())
    if _rt() is not None:
        _services_cache = (time.monotonic(), body)
    return _Compact(body)


@router.get("/api/v1/jobs")
async def jobs() -> JSONResponse:
    from app.jobs import JOB_TABLE, spec_liveness_max_age
    rt = _rt()
    rows = []
    orphan_runs = 0
    if rt and rt.db is not None and rt.db.connected:
        try:
            orphan_runs = int(await rt.db.fetchval(
                "SELECT COUNT(*) FROM collector_runs c WHERE finished_at IS NULL AND started_at < ?",
                (now_ts() - 6 * 3600,)) or 0)
        except Exception:
            pass
    for spec in JOB_TABLE:
        row: dict[str, Any] = {
            "id": spec.id, "owner": spec.owner, "factory": spec.factory, "kind": spec.kind, "phase": spec.phase,
            "trigger": spec.trigger_desc, "enabled_flags": list(spec.enabled_flags),
            "misfire_grace_sec": spec.misfire_grace_sec, "timeout_sec": spec.timeout_sec,
            "providers": list(spec.providers), "paid": spec.paid, "ops_exempt": spec.ops_exempt,
            "startup_catchup": spec.startup_catchup, "min_gap_factor": spec.min_gap_factor,
            "liveness_max_age_sec": _block(lambda sp=spec: spec_liveness_max_age(sp)),
            "available": bool(rt and (spec.id in rt.jobs or spec.id in rt.tasks)),
            "unavailable_reason": rt.unavailable.get(spec.id) if rt else None,
        }
        if rt:
            obj = rt.jobs.get(spec.id) or rt.tasks.get(spec.id)
            if obj is not None:
                row["status"] = _block(obj.status)
        rows.append(row)
    return _Compact({"jobs": rows, "orphan_runs": orphan_runs})
