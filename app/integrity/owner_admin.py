"""Operator owner actions (spec 11 §2.10): whitelist add/retire and the key-compromise post-mortem override.
Effective for decisions with ``t ≥ created_at`` (PIT); rows are retired, never deleted."""
from __future__ import annotations

import json
from typing import TYPE_CHECKING

import app.config as app_config
from app.integrity.models import ActionResult, err
from app.integrity.team import resolve_occupant
from app.integrity.team_parse import valid_url
from app.integrity.whitelist import refresh_whitelist_cache
from app.storage.events import emit_event
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

DAY = 86400
REASONS = ("multisig_migration", "post_compromise_recovery", "disclosed_transfer", "hotkey_rotation",
           "initial_alpha_accounted", "false_positive", "other")
APPLIES = ("coldkey_change", "hotkey_repoint", "identity_churn", "stake_events", "perpetual_off", "extraction",
           "initial_alpha", "all")
OWNER_REVIEW_KINDS = ("owner_extraction_candidate", "owner_change_whitelist")


async def _occ(db: "Database", netuid: int | None, occupant_key: str | None) -> dict | None:
    if occupant_key:
        return await db.fetchone("SELECT * FROM occupants WHERE occupant_key = ?", (occupant_key,), writer=True)
    return await resolve_occupant(db, netuid=netuid)


async def add_whitelist(db: "Database", *, netuid: int | None = None, reason: str, applies_to: str, window_start: int,
                        window_end: int, evidence_url: str, by: str, change_id: int | None = None,
                        old_coldkey: str | None = None, new_coldkey: str | None = None, note: str | None = None,
                        occupant_key: str | None = None, now: int | None = None) -> ActionResult:
    now = int(now if now is not None else now_ts())
    occ = await _occ(db, netuid, occupant_key)
    if occ is None or occ["netuid"] == 0:
        return err("no_current_occupant")
    if reason not in REASONS or applies_to not in APPLIES:
        return err("invalid_kind", occupant_key=occ["occupant_key"])
    if applies_to == "perpetual_off" and reason not in ("post_compromise_recovery", "false_positive"):
        return err("invalid_transition", occupant_key=occ["occupant_key"],
                   warning="perpetual_off may only be whitelisted as post_compromise_recovery|false_positive")
    if not evidence_url:
        return err("evidence_required", occupant_key=occ["occupant_key"])
    if not valid_url(evidence_url):
        return err("invalid_url", occupant_key=occ["occupant_key"])
    if int(window_end) < int(window_start):
        return err("invalid_transition", occupant_key=occ["occupant_key"], warning="window_end < window_start")
    async with db.transaction():
        r = await db.execute(
            "INSERT INTO owner_event_whitelist (occupant_key, netuid, reason, applies_to, window_start, window_end, "
            "old_coldkey, new_coldkey, owner_change_id, evidence_url, note, created_by, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (occ["occupant_key"], occ["netuid"], reason, applies_to, int(window_start), int(window_end), old_coldkey,
             new_coldkey, change_id, evidence_url, note, by, now))
        wid = int(r.lastrowid)
        await refresh_whitelist_cache(db, occ["occupant_key"], now)
    return ActionResult(True, id=wid, occupant_key=occ["occupant_key"], status="active")


async def retire_whitelist(db: "Database", whitelist_id: int, *, by: str, now: int | None = None) -> ActionResult:
    now = int(now if now is not None else now_ts())
    row = await db.fetchone("SELECT * FROM owner_event_whitelist WHERE id = ?", (int(whitelist_id),), writer=True)
    if row is None:
        return err("not_found")
    if row["retired_at"] is not None:
        return err("invalid_transition", id=row["id"], occupant_key=row["occupant_key"], status="retired")
    async with db.transaction():
        await db.execute("UPDATE owner_event_whitelist SET retired_by = ?, retired_at = ? WHERE id = ?",
                         (by, now, row["id"]))
        await refresh_whitelist_cache(db, row["occupant_key"], now)
    return ActionResult(True, id=row["id"], occupant_key=row["occupant_key"], status="retired")


async def list_whitelist(db: "Database", *, netuid: int | None = None) -> list[dict]:
    if netuid is None:
        return await db.fetchall("SELECT * FROM owner_event_whitelist ORDER BY netuid, id", writer=True)
    return await db.fetchall("SELECT * FROM owner_event_whitelist WHERE netuid = ? ORDER BY id", (int(netuid),),
                             writer=True)


async def _active_flags(db: "Database", occupant_key: str) -> set[str]:
    spec = app_config.settings.SPEC_VERSION
    rows = await db.fetchall("SELECT flag FROM red_flag_state WHERE occupant_key = ? AND spec_version = ? "
                             "AND active = 1", (occupant_key, spec), writer=True)
    return {r["flag"] for r in rows}


async def override_key_compromise(db: "Database", *, netuid: int | None = None, window_start: int, window_end: int,
                                  evidence_url: str, by: str, note: str | None = None,
                                  occupant_key: str | None = None, now: int | None = None) -> ActionResult:
    """Master §7.5 post-mortem override (ORO/Connito): whitelist the window for everything, confirm
    ``key_compromise``, clear the active owner flags, record the change and resolve the open owner reviews."""
    now = int(now if now is not None else now_ts())
    occ = await _occ(db, netuid, occupant_key)
    if occ is None or occ["netuid"] == 0:
        return err("no_current_occupant")
    max_days = int(app_config.settings.OWNER_OVERRIDE_MAX_WINDOW_DAYS)
    if int(window_end) - int(window_start) > max_days * DAY:
        return err("window_too_long", occupant_key=occ["occupant_key"])
    if int(window_end) < int(window_start):
        return err("invalid_transition", occupant_key=occ["occupant_key"], warning="window_end < window_start")
    if not evidence_url:
        return err("evidence_required", occupant_key=occ["occupant_key"])
    if not valid_url(evidence_url):
        return err("invalid_url", occupant_key=occ["occupant_key"])
    okey = occ["occupant_key"]
    warnings: list[str] = []
    async with db.transaction():
        wl = await add_whitelist(db, occupant_key=okey, reason="post_compromise_recovery", applies_to="all",
                                 window_start=window_start, window_end=window_end, evidence_url=evidence_url, by=by,
                                 note=note or "key-compromise override", now=now)
        if not wl.ok:
            raise RuntimeError(f"override whitelist failed: {wl.error}")
        wid = wl.id
        await db.execute("INSERT INTO manual_flags (occupant_key, flag, state, evidence_url, note, set_by, set_at) "
                         "VALUES (?, 'key_compromise', 'confirmed', ?, ?, ?, ?)", (okey, evidence_url, note, by, now))
        active = await _active_flags(db, okey)
        for flag in ("owner_extraction", "owner_control_change"):
            if flag in active:
                await db.execute("INSERT INTO manual_flags (occupant_key, flag, state, evidence_url, note, set_by, "
                                 "set_at) VALUES (?, ?, 'cleared', ?, 'superseded by key_compromise override', ?, ?)",
                                 (okey, flag, evidence_url, by, now))
            else:
                warnings.append(f"flag_not_active:{flag}")
        r = await db.execute(
            "INSERT INTO owner_changes (occupant_key, netuid, change_type, old_value, new_value, detected_via, "
            "s3_base_penalty, event_at, observed_at, evidence_json, dedupe_key, run_id) "
            "VALUES (?, ?, 'key_compromise_override', NULL, ?, 'operator', 0, ?, ?, ?, ?, ?)",
            (okey, occ["netuid"], str(wid), now, now,
             json.dumps({"window_start": int(window_start), "window_end": int(window_end), "whitelist_id": wid,
                         "evidence_url": evidence_url, "by": by}),
             f"{okey}|key_compromise_override|{wid}|{now // DAY}", f"operator:{by}"))
        change_id = int(r.lastrowid)
        eid = await emit_event(db, event_type="owner_key_compromise_override", occupant_key=okey, netuid=occ["netuid"],
                               event_at=now, observed_at=now,
                               payload={"netuid": occ["netuid"], "window_start": int(window_start),
                                        "window_end": int(window_end), "whitelist_id": wid,
                                        "evidence_url": evidence_url, "by": by, "change_id": change_id},
                               dedupe_key=f"{okey}|kc|{wid}", source="integrity", run_id=None)
        await db.execute("UPDATE owner_changes SET event_id = ? WHERE id = ?", (eid, change_id))
        ph = ",".join("?" * len(OWNER_REVIEW_KINDS))
        await db.execute(
            f"UPDATE review_queue SET status = 'confirmed', resolution_action = 'key_compromise', resolved_by = ?, "
            f"resolved_at = ?, resolution_note = 'key-compromise override' WHERE occupant_key = ? AND status = 'open' "
            f"AND kind IN ({ph}) AND (ref_table <> 'owner_changes' OR ref_id IS NULL OR ref_id IN (SELECT id FROM "
            f"owner_changes WHERE occupant_key = ? AND event_at BETWEEN ? AND ?))",
            (by, now, okey, *OWNER_REVIEW_KINDS, okey, int(window_start), int(window_end)))
    return ActionResult(True, id=wid, occupant_key=okey, status="confirmed", event_id=eid,
                        warning=";".join(warnings) or None, extra={"change_id": change_id})
