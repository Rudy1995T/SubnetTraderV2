"""All SQL for spec 08's raw tables (``discord_snapshots``, ``telegram_snapshots``, ``announcements``), the team
mention rows it writes into 09's ``social_post_mentions``, gateway heartbeats and ``job_state`` (§3.2, §3.3)."""
from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any, Iterable, Mapping

from app.collectors.discord_api import InviteResult
from app.collectors.discord_topology import LONG_WINDOW, TargetState, TopologyDecision, evidence_window
from app.nlp.textnorm import text_hash, text_norm
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.collectors.discord import DiscordCfg
    from app.collectors.discord_gateway import AnnouncementRow
    from app.storage.db import Database

DSNAP_COLS = ("occupant_key", "netuid", "probe", "guild_id", "invite_code", "observed_at", "event_at", "cycle_at",
              "source", "http_status", "member_count", "presence_count", "presence_ratio", "invite_valid",
              "invite_error_code", "expires_at", "landing_channel_id", "landing_channel_type", "features_json",
              "boosts", "verification_level", "guild_created_at", "widget_enabled", "topology_class", "is_primary",
              "unknown_targets", "source_agreement", "declared_by_json", "meta_json", "run_id")
TGSNAP_COLS = ("occupant_key", "netuid", "handle", "observed_at", "event_at", "cycle_day_ts", "source", "http_status",
               "kind", "title", "members", "online", "online_ratio", "deprecated_marker", "preview_available",
               "preview_posts", "last_post_at", "parse_ok", "error", "run_id")


def post_hash(text: str | None, msg_id: str) -> str:
    """``text_hash`` of the text; empty normalised text → ``sha256('msg:' + msg_id)`` (image-only posts stay distinct)."""
    if text_norm(text):
        return text_hash(text)
    return hashlib.sha256(f"msg:{msg_id}".encode("utf-8")).hexdigest()


def _ph(n: int) -> str:
    return ",".join("?" * n)


# ── discord_snapshots ─────────────────────────────────────────────────────────────────────────────────────────
def invite_row(res: InviteResult, *, occupant_key: str | None, netuid: int | None, cycle_at: int, run_id: str,
               decision: TopologyDecision | None = None, is_primary: bool = False,
               declared_by: list[str] | None = None, source: str = "discord_api") -> dict:
    meta = {k: v for k, v in res.meta.items() if v is not None}
    if res.error:
        meta["error"] = res.error
    return {
        "occupant_key": occupant_key, "netuid": netuid, "probe": "invite", "guild_id": res.guild_id,
        "invite_code": res.code, "observed_at": int(res.fetched_at), "event_at": None, "cycle_at": int(cycle_at),
        "source": source, "http_status": res.http_status, "member_count": res.member_count,
        "presence_count": res.presence_count, "presence_ratio": res.presence_ratio, "invite_valid": res.invite_valid,
        "invite_error_code": res.invite_error_code, "expires_at": res.expires_at,
        "landing_channel_id": res.landing_channel_id, "landing_channel_type": res.landing_channel_type,
        "features_json": json.dumps(list(res.features)) if res.invite_valid is not None and res.guild_id else None,
        "boosts": res.boosts, "verification_level": res.verification_level,
        "guild_created_at": int(res.guild_created_at.timestamp()) if res.guild_created_at else None,
        "widget_enabled": None,
        "topology_class": decision.topology_class if decision else None, "is_primary": int(bool(is_primary)),
        "unknown_targets": len(decision.unknown_targets) if decision else 0,
        "source_agreement": decision.source_agreement if decision else None,
        "declared_by_json": json.dumps(sorted(declared_by)) if declared_by else None,
        "meta_json": json.dumps(meta, ensure_ascii=False, sort_keys=True) if meta else None, "run_id": run_id}


def census_row(dec: TopologyDecision, *, observed_at: int, cycle_at: int, run_id: str, reason: str | None = None
               ) -> dict:
    """Occupant-level row for every occupant without a primary invite row this cycle (counts NULL). The defining
    invite code is the only identifier stored — never a ``discord_handle`` value (a personal username)."""
    r = dec.primary_result
    meta: dict[str, Any] = {}
    if dec.reason or reason:
        meta["reason"] = reason or dec.reason
    return {
        "occupant_key": dec.occupant_key, "netuid": dec.netuid, "probe": "census", "guild_id": dec.primary_guild_id,
        "invite_code": dec.defining_code, "observed_at": int(observed_at), "event_at": None, "cycle_at": int(cycle_at),
        "source": "discord_api", "http_status": None, "member_count": None, "presence_count": None,
        "presence_ratio": None, "invite_valid": r.invite_valid if r is not None else None,
        "invite_error_code": r.invite_error_code if r is not None else None,
        "expires_at": r.expires_at if r is not None else None,
        "landing_channel_id": dec.main_channel_id if dec.topology_class == "main_channel" else None,
        "landing_channel_type": None, "features_json": None, "boosts": None, "verification_level": None,
        "guild_created_at": int(r.guild_created_at.timestamp()) if r is not None and r.guild_created_at else None,
        "widget_enabled": None, "topology_class": dec.topology_class, "is_primary": 1,
        "unknown_targets": len(dec.unknown_targets), "source_agreement": dec.source_agreement,
        "declared_by_json": None, "meta_json": json.dumps(meta) if meta else None, "run_id": run_id}


async def insert_discord_rows(db: "Database", rows: list[dict]) -> int:
    if not rows:
        return 0
    sql = f"INSERT INTO discord_snapshots ({','.join(DSNAP_COLS)}) VALUES ({_ph(len(DSNAP_COLS))})"
    async with db.transaction():
        for r in rows:
            await db.execute(sql, tuple(r.get(c) for c in DSNAP_COLS))
    return len(rows)


async def latest_invite_states(db: "Database", codes: Iterable[str]) -> dict[str, TargetState]:
    codes = sorted(set(codes))
    if not codes:
        return {}
    out: dict[str, TargetState] = {}
    ph = _ph(len(codes))
    rows = await db.fetchall(
        f"SELECT invite_code, invite_valid, guild_id, observed_at, cycle_at, invite_error_code FROM ("
        f" SELECT invite_code, invite_valid, guild_id, observed_at, cycle_at, invite_error_code, ROW_NUMBER() OVER "
        f" (PARTITION BY invite_code ORDER BY observed_at DESC, id DESC) AS rn FROM discord_snapshots "
        f" WHERE probe = 'invite' AND invite_code IN ({ph})) WHERE rn = 1", tuple(codes), writer=True)
    for r in rows:
        out[r["invite_code"]] = TargetState(r["invite_code"], r["invite_valid"], r["guild_id"], int(r["observed_at"]),
                                            int(r["cycle_at"]), r["invite_error_code"])
    agg = await db.fetchall(
        f"SELECT invite_code, MAX(CASE WHEN invite_valid = 1 THEN observed_at END) AS last_valid FROM "
        f"discord_snapshots WHERE probe = 'invite' AND invite_code IN ({ph}) GROUP BY invite_code", tuple(codes),
        writer=True)
    last_valid = {r["invite_code"]: r["last_valid"] for r in agg}
    dead = await db.fetchall(
        f"SELECT s.invite_code, MIN(s.observed_at) AS ds FROM discord_snapshots s WHERE s.probe = 'invite' AND "
        f"s.invite_code IN ({ph}) AND s.invite_valid = 0 AND s.observed_at > COALESCE((SELECT MAX(v.observed_at) "
        f"FROM discord_snapshots v WHERE v.probe = 'invite' AND v.invite_code = s.invite_code AND "
        f"v.invite_valid = 1), 0) GROUP BY s.invite_code", tuple(codes), writer=True)
    dead_since = {r["invite_code"]: r["ds"] for r in dead}
    for code, st in out.items():
        st.last_valid_at = last_valid.get(code)
        st.dead_since = dead_since.get(code)
    return out


async def latest_ok_evidence(db: "Database", codes: Iterable[str], now: int, cfg: "DiscordCfg"
                             ) -> dict[str, InviteResult]:
    """Latest successful (``invite_valid`` not NULL) observation per code inside its freshness window."""
    codes = sorted(set(codes))
    if not codes:
        return {}
    rows = await db.fetchall(
        f"SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY invite_code ORDER BY observed_at DESC, id DESC) "
        f"AS rn FROM discord_snapshots WHERE probe = 'invite' AND invite_valid IS NOT NULL AND invite_code IN "
        f"({_ph(len(codes))}) AND observed_at > ? AND observed_at <= ?) WHERE rn = 1",
        (*codes, now - LONG_WINDOW, now), writer=True)
    out: dict[str, InviteResult] = {}
    for r in rows:
        res = InviteResult.from_row(r)
        if now - res.fetched_at < evidence_window(res, cfg):
            out[res.code] = res
    return out


async def slot_primaries(db: "Database", cycle_at: int) -> set[str]:
    rows = await db.fetchall("SELECT DISTINCT occupant_key FROM discord_snapshots WHERE cycle_at = ? AND "
                             "is_primary = 1 AND probe IN ('invite','census') AND occupant_key IS NOT NULL",
                             (int(cycle_at),), writer=True)
    return {r["occupant_key"] for r in rows}


async def update_watch_invites(db: "Database", fanned: list[tuple[str, InviteResult, str]], now: int,
                               main_guild_id: str) -> int:
    """``(occupant_key, result, class)`` → ``watch_channels`` guild_id / is_main_guild / verified_at + meta merge."""
    n = 0
    for okey, res, cls in fanned:
        if res.invite_valid is None:
            continue
        patch = json.dumps({"last_class": cls, "last_error_code": res.invite_error_code, "last_checked_at": now})
        valid = res.invite_valid == 1
        r = await db.execute(
            "UPDATE watch_channels SET guild_id = COALESCE(?, guild_id), is_main_guild = CASE WHEN ? IS NULL THEN "
            "is_main_guild ELSE ? END, verified_at = CASE WHEN ? THEN ? ELSE verified_at END, "
            "meta_json = json_patch(COALESCE(meta_json, '{}'), ?) "
            "WHERE occupant_key = ? AND platform = 'discord_invite' AND handle = ?",
            (res.guild_id if valid else None, res.guild_id if valid else None,
             int(valid and res.guild_id == main_guild_id), int(valid), now, patch, okey, res.code))
        n += max(0, r.rowcount)
    return n


async def identity_statuses(db: "Database", field: str) -> dict[str, str]:
    rows = await db.fetchall("SELECT occupant_key, status FROM identity_resolved WHERE field = ?", (field,))
    return {r["occupant_key"]: r["status"] for r in rows}


async def primary_guild_map(db: "Database", now: int, days: int = 7) -> dict[str, list[str]]:
    """Primary own-server guild id → occupant keys (latest primary row per occupant within ``days``)."""
    rows = await db.fetchall(
        "SELECT occupant_key, guild_id, topology_class FROM (SELECT occupant_key, guild_id, topology_class, "
        "ROW_NUMBER() OVER (PARTITION BY occupant_key ORDER BY observed_at DESC, id DESC) AS rn FROM discord_snapshots "
        "WHERE is_primary = 1 AND probe IN ('invite','census') AND occupant_key IS NOT NULL AND observed_at > ? AND "
        "observed_at <= ?) WHERE rn = 1", (now - days * 86400, now))
    out: dict[str, list[str]] = {}
    for r in rows:
        if r["topology_class"] == "own_server" and r["guild_id"]:
            out.setdefault(r["guild_id"], []).append(r["occupant_key"])
    return out


# ── telegram_snapshots ────────────────────────────────────────────────────────────────────────────────────────
async def telegram_rows_today(db: "Database", day_ts: int) -> set[tuple[str, str]]:
    rows = await db.fetchall("SELECT occupant_key, handle FROM telegram_snapshots WHERE cycle_day_ts = ?", (day_ts,),
                             writer=True)
    return {(r["occupant_key"], r["handle"]) for r in rows}


async def insert_telegram_snapshot(db: "Database", row: Mapping[str, Any]) -> int:
    r = await db.execute(f"INSERT OR IGNORE INTO telegram_snapshots ({','.join(TGSNAP_COLS)}) VALUES "
                         f"({_ph(len(TGSNAP_COLS))})", tuple(row.get(c) for c in TGSNAP_COLS))
    return max(0, r.rowcount)


# ── announcements + team mentions ─────────────────────────────────────────────────────────────────────────────
def text_purge_after(observed_at: int, store_text: bool, pending_days: int) -> int | None:
    """Amended B10: with the platform's *_STORE_TEXT flag false the text is pending classification only."""
    return None if store_text else int(observed_at) + int(pending_days) * 86400


async def upsert_announcement(db: "Database", row: "AnnouncementRow", *, store_text: bool, pending_days: int,
                              run_id: str | None = None) -> bool:
    """``INSERT OR IGNORE`` + one ``social_post_mentions(match_kind='team')`` row per mapped occupant."""
    if row.platform == "discord" and row.text is not None and not row.via_own_server:
        raise ValueError("Discord text is stored only for crossposts received in the operator's server (B10)")
    occ = row.occupant_keys[0] if len(row.occupant_keys) == 1 else None
    purge = text_purge_after(row.observed_at, store_text, pending_days) if row.text is not None else None
    meta = dict(row.meta)
    if len(row.occupant_keys) > 1:
        meta["candidates"] = list(row.occupant_keys)
    rid = run_id or row.run_id
    async with db.transaction():
        res = await db.execute(
            "INSERT OR IGNORE INTO announcements (msg_id, platform, occupant_key, netuid, source_guild_id, "
            "source_channel_id, message_id, local_channel_id, local_message_id, via_own_server, mapping, "
            "author_is_team, has_attachment, url_count, text, text_purge_after, text_hash, text_len, event_at, "
            "observed_at, meta_json, run_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (row.msg_id, row.platform, occ, row.netuid if occ else None, row.source_guild_id, row.source_channel_id,
             row.message_id, row.local_channel_id, row.local_message_id, row.via_own_server, row.mapping,
             row.author_is_team, row.has_attachment, row.url_count, row.text, purge, row.text_hash, row.text_len,
             row.event_at, row.observed_at, json.dumps(meta, ensure_ascii=False) if meta else None, rid))
        inserted = res.rowcount > 0
        if row.mapping in ("channel", "guild", "guild_ambiguous", "tg_handle"):
            post_id = row.msg_id.split(":", 1)[1]
            for okey in row.occupant_keys:
                await db.execute(
                    "INSERT OR IGNORE INTO social_post_mentions (platform, post_id, occupant_key, match_kind, "
                    "event_at, observed_at, run_id) VALUES (?,?,?,'team',?,?,?)",
                    (row.platform, post_id, okey, row.event_at, row.observed_at, rid))
    return inserted


async def mark_deleted(db: "Database", local_message_id: str, at: int) -> int:
    r = await db.execute("UPDATE announcements SET deleted_at = ? WHERE local_message_id = ? AND deleted_at IS NULL",
                         (int(at), str(local_message_id)))
    return max(0, r.rowcount)


async def write_gateway_heartbeat(db: "Database", hour_ts: int, ok: bool, rows: int, error: str | None) -> None:
    """One ``collector_runs`` row per UTC hour (``run_id = gw:{hour_ts}``), written when the hour is complete."""
    await db.execute(
        "INSERT INTO collector_runs (run_id, job, started_at, finished_at, ok, rows, error, duration_sec, note) "
        "VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET ok = MAX(collector_runs.ok, excluded.ok), "
        "rows = MAX(collector_runs.rows, excluded.rows), error = CASE WHEN MAX(collector_runs.ok, excluded.ok) = 1 "
        "THEN NULL ELSE excluded.error END",
        (f"gw:{int(hour_ts)}", "discord_gateway", int(hour_ts), int(hour_ts) + 3600, int(bool(ok)), int(rows),
         None if ok else (error or "disconnected"), 3600.0, "heartbeat"))


# ── job_state (runtime watermarks / debounce only, 02 §B12) ───────────────────────────────────────────────────
async def get_task_state(db: "Database", job: str, key: str, default: Any = None) -> Any:
    r = await db.fetchone("SELECT value_json FROM job_state WHERE job = ? AND key = ?", (job, key), writer=True)
    if r is None:
        return default
    try:
        return json.loads(r["value_json"])
    except ValueError:
        return default


async def get_task_states(db: "Database", job: str, prefix: str) -> dict[str, Any]:
    rows = await db.fetchall("SELECT key, value_json FROM job_state WHERE job = ? AND key LIKE ?",
                             (job, prefix + "%"), writer=True)
    out: dict[str, Any] = {}
    for r in rows:
        try:
            out[r["key"]] = json.loads(r["value_json"])
        except ValueError:
            continue
    return out


async def set_task_state(db: "Database", job: str, key: str, value: Any) -> None:
    await db.execute("INSERT INTO job_state (job, key, value_json, updated_at) VALUES (?,?,?,?) ON CONFLICT(job, key) "
                     "DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at",
                     (job, key, json.dumps(value, default=str), now_ts()))
