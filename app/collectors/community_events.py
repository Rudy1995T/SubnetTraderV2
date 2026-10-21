"""Community event detectors (spec 08 §2.10, §3.5): invite hygiene, venue changes (debounced), inflation, silence.

All writes go through 03's ``emit_event`` (catalog validation, ``UNIQUE(event_type, dedupe_key)``); 08 never sends
Telegram itself (D3). Debounce state lives in ``job_state`` (runtime state only, B12). Transitions to or from
``unknown`` never emit: a failure is not a venue change.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping

from app.collectors import community_store as store
from app.collectors.discord_api import InviteResult
from app.collectors.discord_topology import TopologyDecision
from app.logging.logger import logger
from app.scoring.spec import INFLATION_DAYS
from app.util.time import floor_day

if TYPE_CHECKING:  # pragma: no cover
    from app.collectors.discord import DiscordCfg
    from app.storage.db import Database

DAY = 86400
REACHABLE = ("own_server", "main_channel")
GONE = ("invalid", "none")
TG_COMMUNITY = ("group", "channel", "private_group", "private_channel")
SILENCE_THRESHOLD = 45
SILENCE_CLOSE_DAYS = 7


async def _emit(db: "Database", **kw: Any) -> int:
    from app.storage.events import emit_event
    try:
        eid = await emit_event(db, source="community", **kw)
        return 1 if eid else 0
    except ValueError as exc:            # catalog violation: a bug, logged loudly but never fatal for the run
        logger.error("community_event_invalid", data={"event_type": kw.get("event_type"), "error": str(exc)})
        return 0


async def _tg_reachable_now(db: "Database", occupant_key: str, now: int) -> bool:
    return bool(await db.fetchval(
        "SELECT 1 FROM telegram_snapshots WHERE occupant_key = ? AND observed_at > ? AND observed_at <= ? AND "
        "parse_ok = 1 AND http_status = 200 AND kind IN ('group','channel','private_group','private_channel') AND "
        "members >= 1 AND deprecated_marker = 0 LIMIT 1", (occupant_key, now - 2 * DAY, now)))


async def emit_invite_events(db: "Database", decisions: Mapping[str, TopologyDecision],
                             fanned: list[tuple[str, InviteResult]], states: Mapping[str, Any],
                             prev_primary: Mapping[str, str | None], now: int, run_id: str,
                             cfg: "DiscordCfg") -> int:
    """``invite_expired`` (stages ``expiring``/``expired``) and ``invite_dead`` for codes fetched this run."""
    n = 0
    tg_cache: dict[str, bool] = {}
    for okey, res in fanned:
        dec = decisions.get(okey)
        if dec is None or res.invite_valid is None:
            continue
        st = states.get(res.code)
        if okey not in tg_cache:
            tg_cache[okey] = await _tg_reachable_now(db, okey, now)
        other = dec.topology_class in REACHABLE and dec.primary_code != res.code or tg_cache[okey]
        is_primary = res.code in (prev_primary.get(okey), dec.primary_code)
        sev = "warning" if is_primary and not other else "info"
        base = {"netuid": dec.netuid, "occupant_key": okey, "invite_code": res.code, "guild_id": res.guild_id,
                "last_valid_at": st.last_valid_at if st else None, "declared_by": dec.declared_by.get(res.code, []),
                "other_reachable_venue": bool(other)}
        if res.invite_valid == 1 and res.expires_at is not None \
                and res.expires_at <= res.fetched_at + cfg.expiry_warn_days * DAY:
            n += await _emit(db, event_type="invite_expired", occupant_key=okey, netuid=dec.netuid,
                             severity="info", event_at=res.fetched_at, observed_at=now,
                             payload={**base, "stage": "expiring", "expires_at": res.expires_at},
                             dedupe_key=f"{okey}|{res.code}|expiring", run_id=run_id)
        elif res.invite_valid == 0 and res.invite_error_code == 50270:
            n += await _emit(db, event_type="invite_expired", occupant_key=okey, netuid=dec.netuid, severity=sev,
                             event_at=res.expires_at or res.fetched_at, observed_at=now,
                             payload={**base, "stage": "expired", "expires_at": res.expires_at},
                             dedupe_key=f"{okey}|{res.code}|expired", run_id=run_id)
        elif res.invite_valid == 0 and res.invite_error_code is not None:
            first_dead = (st.dead_since if st and st.dead_since else None) or res.fetched_at
            if res.fetched_at - first_dead >= 20 * 3600:          # 2 consecutive dead checks ≥ 20 h apart
                n += await _emit(db, event_type="invite_dead", occupant_key=okey, netuid=dec.netuid, severity=sev,
                                 event_at=res.fetched_at, observed_at=now,
                                 payload={**base, "error_code": res.invite_error_code, "first_dead_at": first_dead},
                                 dedupe_key=f"{okey}|{res.code}|{res.invite_error_code}", run_id=run_id)
    return n


async def detect_venue_changes(db: "Database", decisions: list[TopologyDecision], now: int, run_id: str,
                               cfg: "DiscordCfg", *, cycle_at: int, state: dict[str, dict] | None = None) -> int:
    """Class or primary guild changed and held ``COMMUNITY_VENUE_CHANGE_DEBOUNCE`` cycles → one event."""
    if state is None:
        state = await store.get_task_states(db, "discord_6h", "topo:")
    n = 0
    for dec in decisions:
        key = f"topo:{dec.occupant_key}"
        st = state.get(key)
        cls, guild = dec.topology_class, dec.primary_guild_id
        if cls == "unknown" or (st is not None and st.get("last_cycle") == cycle_at):
            continue                                   # failures never count; a same-slot re-run counts once
        if st is None:
            st = {"class": cls, "guild_id": guild, "primary_code": dec.primary_code, "since": now}
        elif (cls, guild) == (st.get("class"), st.get("guild_id")):
            st.update(primary_code=dec.primary_code)
            for k in ("pending_class", "pending_guild_id", "pending_count", "pending_since"):
                st.pop(k, None)
        else:
            if (st.get("pending_class"), st.get("pending_guild_id")) == (cls, guild):
                st["pending_count"] = int(st.get("pending_count", 0)) + 1
            else:
                st.update(pending_class=cls, pending_guild_id=guild, pending_count=1, pending_since=now)
            if st["pending_count"] >= cfg.venue_change_debounce:
                old_cls, old_guild = st.get("class"), st.get("guild_id")
                first = int(st.get("pending_since") or now)
                sev = "warning" if old_cls in REACHABLE and cls in GONE else "info"
                n += await _emit(
                    db, event_type="community_venue_changed", occupant_key=dec.occupant_key, netuid=dec.netuid,
                    severity=sev, event_at=first, observed_at=now,
                    payload={"netuid": dec.netuid, "occupant_key": dec.occupant_key, "platform": "discord",
                             "old_class": old_cls, "new_class": cls, "old_guild_id": old_guild, "new_guild_id": guild,
                             "handle": None, "first_seen_new_at": first,
                             "reason": "guild_changed" if old_cls == cls else "class_changed"},
                    dedupe_key=f"{dec.occupant_key}|discord|{cls}|{guild}|{first // DAY}", run_id=run_id)
                st = {"class": cls, "guild_id": guild, "primary_code": dec.primary_code, "since": first}
        st["last_cycle"] = cycle_at
        state[key] = st
        await store.set_task_state(db, "discord_6h", key, st)
    return n


async def detect_inflation(db: "Database", occupant_keys: list[str], as_of: int, run_id: str, now: int) -> int:
    from app.features.community import inflation_streak
    n = 0
    for okey in occupant_keys:
        s = await inflation_streak(db, okey, as_of)
        if s is None or s["days"] < INFLATION_DAYS:
            continue
        netuid = await db.fetchval("SELECT netuid FROM occupants WHERE occupant_key = ?", (okey,))
        n += await _emit(db, event_type="inflated_community", occupant_key=okey, netuid=netuid, severity="warning",
                         event_at=s["streak_start"], observed_at=now,
                         payload={"netuid": netuid, "occupant_key": okey, "guild_id": s["guild_id"],
                                  "members": s["members"], "presence_median": s["presence_median"],
                                  "ratio": s["ratio"], "days": s["days"], "streak_start": s["streak_start"],
                                  "guild_created_at": s["guild_created_at"],
                                  "source_agreement": s["source_agreement"]},
                         dedupe_key=f"{okey}|{s['guild_id']}|{s['streak_start']}", run_id=run_id)
    return n


async def last_team_comm(db: "Database", occupant_key: str, t: int) -> tuple[int | None, str | None]:
    """Newest team communication observed by ``t`` across X, Discord and Telegram: ``(event_at, platform)``."""
    best: tuple[int | None, str | None] = (None, None)
    x = await db.fetchval(
        "SELECT MAX(event_at) FROM social_posts WHERE platform = 'x' AND is_team_account = 1 AND bot_filtered = 0 "
        "AND occupant_key = ? AND observed_at <= ? AND event_at <= ?", (occupant_key, t, t))
    if x is not None:
        best = (int(x), "x")
    for platform, label in (("discord", "discord"), ("telegram", "tg")):
        v = await db.fetchval(
            "SELECT MAX(a.event_at) FROM social_post_mentions m JOIN announcements a ON a.msg_id = m.platform || ':' "
            "|| m.post_id WHERE m.platform = ? AND m.match_kind = 'team' AND m.occupant_key = ? AND m.observed_at <= ? "
            "AND a.observed_at <= ? AND a.event_at <= ?", (platform, occupant_key, t, t, t))
        if v is not None and (best[0] is None or int(v) > best[0]):
            best = (int(v), label)
    return best


async def detect_silence(db: "Database", occupant_keys: list[str], as_of: int, run_id: str, now: int) -> int:
    """From 12's latest fresh ``silence_days`` raw row (none exist before Phase 8 → no-op); one event per episode,
    re-armed after 7 consecutive daily rows < 45."""
    import json
    n = 0
    for okey in occupant_keys:
        row = await db.fetchone(
            "SELECT value, value_json, status, observed_at FROM metric_values WHERE occupant_key = ? AND "
            "metric_key = 'silence_days' AND stage = 'raw' AND observed_at <= ? AND as_of > ? AND as_of <= ? "
            "ORDER BY as_of DESC LIMIT 1", (okey, as_of, as_of - DAY, as_of))
        if row is None or row["status"] != "ok" or row["value"] is None:
            continue
        key = f"silence:{okey}"
        st = await store.get_task_state(db, "discord_6h", key, {}) or {}
        day = floor_day(as_of)
        if st.get("last_day") == day:
            continue
        st["last_day"] = day
        value = float(row["value"])
        if value >= SILENCE_THRESHOLD:
            st["below"] = 0
            if not st.get("open"):
                vj = json.loads(row["value_json"]) if row["value_json"] else {}
                lc_at, lc_pf = await last_team_comm(db, okey, as_of)
                netuid = await db.fetchval("SELECT netuid FROM occupants WHERE occupant_key = ?", (okey,))
                n += await _emit(db, event_type="silence_45d", occupant_key=okey, netuid=netuid, severity="warning",
                                 event_at=now, observed_at=now,
                                 payload={"netuid": netuid, "occupant_key": okey, "silence_days": value,
                                          "lower_bound": bool(vj.get("lower_bound", False)), "legs": vj.get("legs", {}),
                                          "last_comm_at": lc_at, "last_comm_platform": lc_pf,
                                          "min_leg": vj.get("min_leg")},
                                 dedupe_key=f"{okey}|{day // DAY}", run_id=run_id)
                st.update(open=True, start_day=day)
        elif st.get("open"):
            st["below"] = int(st.get("below", 0)) + 1
            if st["below"] >= SILENCE_CLOSE_DAYS:
                st = {"open": False, "last_day": day}
        await store.set_task_state(db, "discord_6h", key, st)
    return n


async def emit_tg_venue_changes(db: "Database", rows: list[dict], now: int, run_id: str, debounce: int) -> int:
    """Telegram: community kind ↔ non-community kind held ``debounce`` days (``parse_ok=0`` rows ignored)."""
    n = 0
    for r in rows:
        if r.get("parse_ok") != 1 or not r.get("kind"):
            continue
        okey, handle = r["occupant_key"], r["handle"]
        key = f"tg:{okey}:{handle}"
        st = await store.get_task_state(db, "telegram_daily", key)
        comm = r["kind"] in TG_COMMUNITY and not r.get("deprecated_marker")
        day = int(r["cycle_day_ts"])
        if st is None:
            st = {"community": comm, "kind": r["kind"], "since": now}
        elif st.get("last_day") == day:
            continue
        elif comm == st.get("community"):
            st.update(kind=r["kind"])
            for k in ("pending", "pending_kind", "pending_count", "pending_since"):
                st.pop(k, None)
        else:
            if st.get("pending") == comm:
                st["pending_count"] = int(st.get("pending_count", 0)) + 1
            else:
                st.update(pending=comm, pending_kind=r["kind"], pending_count=1, pending_since=now)
            if st["pending_count"] >= debounce:
                first = int(st.get("pending_since") or now)
                sev = "warning" if st.get("community") and r["kind"] in ("not_found", "user_or_unknown") else "info"
                n += await _emit(
                    db, event_type="community_venue_changed", occupant_key=okey, netuid=r.get("netuid"),
                    severity=sev, event_at=first, observed_at=now,
                    payload={"netuid": r.get("netuid"), "occupant_key": okey, "platform": "telegram",
                             "old_class": st.get("kind"), "new_class": r["kind"], "old_guild_id": None,
                             "new_guild_id": None, "handle": handle, "first_seen_new_at": first,
                             "reason": "telegram_kind_changed"},
                    dedupe_key=f"{okey}|telegram|{r['kind']}|{handle}|{first // DAY}", run_id=run_id)
                st = {"community": comm, "kind": r["kind"], "since": first}
        st["last_day"] = day
        await store.set_task_state(db, "telegram_daily", key, st)
    return n
