"""Kill switches, pause targets and pause precedence (spec 16 §2.11, 02 §B12).

Closed sentinel list: ``KILL_SWITCH`` (paid jobs + governor), ``PAUSE_ALL`` (every non-ops_exempt job),
``SCORING_KILLED`` (no publication), and the read-only emergency inputs ``data/control/<target>.paused``.
Per-target operator pauses live only in ``control_state`` (never ``job_state``); targets are exact JOB_TABLE ids or
``PAUSE_GROUPS`` names — never prefix, provider or substring matches. The app never creates or deletes pause files.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.logging.logger import logger
from app.util.time import now_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.runtime import Runtime
    from app.storage.db import Database

PAUSE_GROUPS: dict[str, frozenset[str]] = {
    "registry": frozenset({"registry_occupants", "registry_identity"}),
    "chain": frozenset({"chain_sse", "tmc_hourly", "tmc_daily", "tmc_weekly", "chain_features", "chain_rpc_hourly",
                        "chain_rpc_metagraph"}),
    "tmc": frozenset({"registry_occupants", "registry_identity", "tmc_hourly", "tmc_daily", "tmc_weekly",
                      "github_html_probe", "owner_collect_daily", "backfill_tmc"}),
    "github": frozenset({"github_resolve", "github_org_enum", "github_activity", "github_metrics", "github_html_probe",
                         "backfill_github"}),
    "web": frozenset({"web_check", "web_metrics", "web_spn2"}),
    "discord": frozenset({"discord_6h", "discord_widget_1h", "discord_gateway"}),
    "telegram_counts": frozenset({"telegram_daily"}),
    "community": frozenset({"discord_6h", "discord_widget_1h", "discord_gateway", "telegram_daily"}),
    "x": frozenset({"x_team_poll", "x_kol_poll", "x_mentions_poll", "x_profile_refresh", "x_ctf_crosscheck"}),
    "rss": frozenset({"rss_feeds_poll", "news_google_poll", "youtube_poll"}),
    "llm": frozenset({"llm_classify_submit", "llm_batch_poll", "llm_rules_fallback", "llm_narrative_weekly"}),
    "integrity": frozenset({"owner_collect_daily", "owner_watch_hourly", "team_seed_daily", "integrity_features",
                            "owner_rpc_seed"}),
    "scoring": frozenset({"features_hourly", "scoring_6h"}),
    "quant": frozenset({"quant_weekly"}),
    "notify": frozenset({"dereg_watch", "needs_review_digest"}),
    "backfill": frozenset({"backfill_tmc", "backfill_github"}),
    "wb": frozenset({"wb_technicals", "wb_dereg", "chain_constants_probe", "wb_github_probe"}),
}
PAUSE_REASONS = ("operator", "kill_switch", "pause_all", "budget", "emergency_file")


class UnknownTarget(ValueError):
    pass


class NotPausable(ValueError):
    """``alert_dispatch`` (the only pager, 02 §D3) and ``telegram_bot`` (``/resume``) are never paused by a
    ``control_state`` row (02 §B12, 14 §2.4: 409 ``not_pausable``); use ``ALERTS_ENABLED`` /
    ``TELEGRAM_COMMANDS_ENABLED`` + reload-config. An emergency ``.paused`` file still applies."""


NOT_PAUSABLE = frozenset({"alert_dispatch", "telegram_bot"})


def declared_ids() -> frozenset[str]:
    from app.jobs import JOB_TABLE
    return frozenset(s.id for s in JOB_TABLE)


def never_grouped() -> frozenset[str]:
    from app.jobs import JOB_TABLE
    return frozenset({s.id for s in JOB_TABLE if s.ops_exempt}) | {"alert_dispatch", "telegram_bot"}


def resolve_targets(name: str) -> list[str]:
    """Exact JOB_TABLE id → [name]; PAUSE_GROUPS name → members minus never_grouped(); else UnknownTarget."""
    if name in declared_ids():
        return [name]
    if name in PAUSE_GROUPS:
        return sorted(PAUSE_GROUPS[name] - never_grouped())
    raise UnknownTarget(name)


# ── file sentinels ─────────────────────────────────────────────────────────────────────────────────────────
def _p(key: str) -> Path:
    s = app_config.settings
    return s.path(getattr(s, key))


def kill_switch_active() -> bool:
    return _p("KILL_SWITCH_PATH").exists()


def scoring_killed() -> bool:
    return _p("SCORING_KILL_SWITCH_PATH").exists()


def integrity_path() -> Path:
    s = app_config.settings
    return s.path(s.RUN_DIR) / "integrity.json"


def integrity_hold_active() -> bool:
    p = integrity_path()
    if not p.exists():
        return False
    try:
        return json.loads(p.read_text()).get("result") != "ok"
    except (OSError, ValueError):
        return True


def pause_all_file_active() -> bool:
    return _p("PAUSE_ALL_PATH").exists()


def pause_all_active() -> bool:
    return pause_all_file_active() or integrity_hold_active()


def _control_dir() -> Path:
    s = app_config.settings
    return s.path(s.CONTROL_DIR)


_unknown_files_logged: set[str] = set()


def file_paused(job_id: str) -> str | None:
    """Relative path of the emergency file pausing ``job_id`` (own id, or a group containing it), else None."""
    d = _control_dir()
    if (d / f"{job_id}.paused").exists():
        return f"{app_config.settings.CONTROL_DIR}/{job_id}.paused"
    if job_id not in never_grouped():
        for g, members in PAUSE_GROUPS.items():
            if job_id in members and (d / f"{g}.paused").exists():
                return f"{app_config.settings.CONTROL_DIR}/{g}.paused"
    return None


def scan_unknown_pause_files() -> list[str]:
    d = _control_dir()
    if not d.is_dir():
        return []
    known = declared_ids() | set(PAUSE_GROUPS)
    out = []
    for f in d.glob("*.paused"):
        stem = f.name[: -len(".paused")]
        if stem not in known:
            out.append(f.name)
            if f.name not in _unknown_files_logged:
                _unknown_files_logged.add(f.name)
                logger.warning("unknown_pause_file", data={"file": f.name})
    return out


# ── control_state cache ────────────────────────────────────────────────────────────────────────────────────
_rows: dict[str, dict[str, Any]] = {}          # target -> row
_by_job: dict[str, list[str]] = {}              # job id -> targets pausing it


def _rebuild_index() -> None:
    _by_job.clear()
    for target in _rows:
        try:
            ids = resolve_targets(target)
        except UnknownTarget:
            logger.warning("unknown_pause_target", data={"target": target})
            continue
        for j in ids:
            _by_job.setdefault(j, []).append(target)


async def refresh_cache(db: "Database") -> None:
    rows = await db.fetchall(
        "SELECT target, key, value, reason, set_by, set_at, expires_at FROM control_state WHERE key = 'paused'")
    _rows.clear()
    for r in rows:
        if r["value"] == "true":
            _rows[r["target"]] = dict(r)
    _rebuild_index()


def operator_pause_target(job_id: str, now: int | None = None) -> str | None:
    now = now_ts() if now is None else now
    for t in _by_job.get(job_id, []):
        row = _rows.get(t)
        if row and (row.get("expires_at") is None or row["expires_at"] >= now):
            return t
    return None


def is_paused(name: str) -> bool:
    return operator_pause_target(name) is not None or file_paused(name) is not None


def _db(db: "Database | None") -> "Database":
    if db is not None:
        return db
    from app.runtime import get_runtime
    rt = get_runtime()
    assert rt.db is not None
    return rt.db


async def _emit(d: "Database", event_type: str, payload: dict, now: int) -> None:
    from app.storage.events import emit_event
    try:
        await emit_event(d, event_type=event_type, occupant_key=None, netuid=None, event_at=now, observed_at=now,
                         payload=payload, dedupe_key=None, source="api", run_id=None)
    except Exception as exc:  # the pause itself already happened; never fail it over an audit row
        logger.warning("control event not written", data={"event_type": event_type, "error": str(exc)[:200]})


async def pause(name: str, *, by: str, reason: str | None = None, hours: float | None = None,
                db: "Database | None" = None) -> dict:
    if name in NOT_PAUSABLE:
        raise NotPausable(name)
    ids = resolve_targets(name)
    now = now_ts()
    expires = now + int(hours * 3600) if hours else None
    d = _db(db)
    await d.upsert("control_state", [{"target": name, "key": "paused", "value": "true", "reason": reason,
                                      "set_by": by, "set_at": now, "expires_at": expires}],
                   conflict=("target", "key"))
    _rows[name] = {"target": name, "key": "paused", "value": "true", "reason": reason, "set_by": by, "set_at": now,
                   "expires_at": expires}
    _rebuild_index()
    logger.info("paused", data={"target": name, "by": by, "reason": reason, "jobs": ids, "expires_at": expires})
    await _emit(d, "collector_paused", {"collector": name, "by": by, "reason": reason, "members": ids}, now)
    return {"paused": True, "target": name, "jobs": ids, "set_by": by, "expires_at": to_iso(expires)}


async def resume(name: str, *, by: str, db: "Database | None" = None) -> dict:
    ids = resolve_targets(name)
    d = _db(db)
    res = await d.execute("DELETE FROM control_state WHERE target = ? AND key = 'paused'", (name,))
    _rows.pop(name, None)
    _rebuild_index()
    out: dict[str, Any] = {"resumed": True, "target": name, "removed_row": res.rowcount > 0, "jobs": ids}
    file_active = {j: f for j in ids if (f := file_paused(j))}
    still_by = {j: t for j in ids if (t := operator_pause_target(j))}
    if file_active:
        files = sorted(set(file_active.values()))
        out["file_pause_active"] = files[0] if len(files) == 1 else files
        out["note"] = "target stays paused until the operator deletes this file"
    if still_by:
        targets = sorted(set(still_by.values()))
        out["still_paused_by"] = targets[0] if len(targets) == 1 else targets
    logger.info("resumed", data={"target": name, "by": by, **{k: v for k, v in out.items() if k != "jobs"}})
    if out["removed_row"]:
        extra = {"file_pause_active": out["file_pause_active"]} if "file_pause_active" in out else {}
        await _emit(d, "collector_resumed", {"collector": name, "by": by, **extra}, now_ts())
    return out


# ── the single precedence implementation (16 §2.11) ─────────────────────────────────────────────────────────
def effective_pause(obj: Any, rt: "Runtime | None") -> str | None:
    """pause_all → kill_switch (paid) → emergency_file → operator → budget; None when running normally."""
    spec = obj.spec
    job_id = spec.id
    exempt = spec.ops_exempt
    if not exempt and pause_all_active():
        return "pause_all"
    if job_id == "storage_retention_daily" and integrity_hold_active():
        return "pause_all"
    if spec.paid and not exempt and kill_switch_active():
        return "kill_switch"
    if file_paused(job_id):
        return "emergency_file"
    if operator_pause_target(job_id):
        return "operator"
    if spec.providers and rt is not None:
        p = spec.providers[0]
        try:
            if rt.limiter is not None and p in rt.limiter.limits and rt.limiter.exhausted(p):
                return "budget"
        except Exception:  # pragma: no cover
            pass
        if rt.costs is not None and rt.costs.is_exhausted(p):
            return "budget"
    return None


def sentinel_state() -> dict[str, bool]:
    return {"kill_switch": kill_switch_active(), "pause_all": pause_all_active(), "scoring_killed": scoring_killed(),
            "integrity_hold": integrity_hold_active()}


def _exists(p: str) -> bool:  # pragma: no cover - helper for shells/tests
    return os.path.exists(p)
