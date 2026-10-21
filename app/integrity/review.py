"""The shared human-review queue (spec 11 §2.10; 03's ``review_queue``). 11 owns the domain functions and the kind
vocabulary; spec 10's ``enqueue_review`` delegates to ``enqueue(kind='llm_flag_candidate', …)``."""
from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING, Any

from app.integrity.models import ActionResult, err
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

DAY = 86400
KINDS_11 = frozenset({"team_candidate", "team_reverify", "team_delisted", "owner_extraction_candidate",
                      "owner_change_whitelist", "owner_source_disagreement"})
KINDS = KINDS_11 | {"llm_flag_candidate"}
OWNER_KINDS = frozenset({"owner_extraction_candidate", "owner_change_whitelist", "owner_source_disagreement"})
ACTIONS = ("confirm_flag", "whitelist", "key_compromise", "dismiss", "done")


def _merge(old: dict, new: dict) -> dict:
    out = dict(old)
    for k, v in new.items():
        if isinstance(v, list) and isinstance(out.get(k), list):
            out[k] = list(dict.fromkeys([*out[k], *v]))
        elif k.startswith("n_") and isinstance(v, (int, float)) and isinstance(out.get(k), (int, float)):
            out[k] = out[k] + v
        else:
            out[k] = v
    return out


async def enqueue(db: "Database", *, occupant_key: str | None, kind: str, dedupe_base: str | None = None,
                  ref_table: str | None = None, ref_id: int | None = None, priority: int = 2,
                  suggested_flag: str | None = None, summary: str | None = None, payload: dict | None = None,
                  evidence_url: str | None = None, event_type: str | None = None, materiality: int | None = None,
                  post_id: str | None = None, label_version: str | None = None, now: int | None = None) -> int | None:
    if kind not in KINDS:
        raise ValueError("invalid_kind")
    now = int(now if now is not None else now_ts())
    body = json.dumps(payload or {}, separators=(",", ":"), default=str)
    if kind == "llm_flag_candidate":
        if not post_id or not label_version:
            raise ValueError("invalid_kind")
        rows = await db.execute_returning(
            "INSERT OR IGNORE INTO review_queue (occupant_key, kind, suggested_flag, post_id, label_version, event_type, "
            "materiality, summary, evidence_url, ref_table, ref_id, priority, payload_json, dedupe_key, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id",
            (occupant_key, kind, suggested_flag, post_id, label_version, event_type, materiality, summary, evidence_url,
             ref_table or "events", ref_id, priority, body, f"llm_flag_candidate|{post_id}|{label_version}", now))
        return int(rows[0]["id"]) if rows else None
    if post_id is not None or not dedupe_base:
        raise ValueError("invalid_kind")
    async with db.transaction():
        cur = await db.fetchone("SELECT id, payload_json FROM review_queue WHERE dedupe_base = ? AND status = 'open' "
                                "ORDER BY id DESC LIMIT 1", (dedupe_base,))
        if cur:
            merged = _merge(json.loads(cur["payload_json"] or "{}"), payload or {})
            await db.execute("UPDATE review_queue SET payload_json = ? WHERE id = ?", (json.dumps(merged, default=str),
                                                                                       cur["id"]))
            return int(cur["id"])
        try:
            r = await db.execute(
                "INSERT INTO review_queue (occupant_key, kind, suggested_flag, event_type, materiality, summary, "
                "evidence_url, ref_table, ref_id, priority, payload_json, dedupe_key, dedupe_base, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (occupant_key, kind, suggested_flag, event_type, materiality, summary, evidence_url, ref_table, ref_id,
                 priority, body, f"{dedupe_base}|{now}", dedupe_base, now))
        except sqlite3.IntegrityError:                    # same base, same second, already open
            return await db.fetchval("SELECT id FROM review_queue WHERE dedupe_key = ? AND status = 'open'",
                                     (f"{dedupe_base}|{now}",))
        return int(r.lastrowid)


async def list_open(db: "Database", *, kind: str | None = None, occupant_key: str | None = None,
                    limit: int = 100) -> list[dict]:
    where, args = ["status = 'open'"], []
    if kind:
        where.append("kind = ?")
        args.append(kind)
    if occupant_key:
        where.append("occupant_key = ?")
        args.append(occupant_key)
    rows = await db.fetchall(f"SELECT * FROM review_queue WHERE {' AND '.join(where)} "
                             "ORDER BY priority, created_at, id LIMIT ?", (*args, int(limit)), writer=True)
    for r in rows:
        r["payload"] = json.loads(r.pop("payload_json") or "{}")
    return rows


def _allowed(action: str, kind: str, flag: str | None) -> bool:
    if action == "dismiss":
        return True
    if kind == "llm_flag_candidate":
        if flag is None:
            return False
        if action in ("confirm_flag", "key_compromise"):
            return flag in ("owner_extraction", "key_compromise")
        return action == "done" and flag == "security_incident"
    if action == "confirm_flag":
        return kind == "owner_extraction_candidate"
    if action == "whitelist":
        return kind in ("owner_change_whitelist", "owner_extraction_candidate")
    if action == "key_compromise":
        return kind in ("owner_extraction_candidate", "owner_change_whitelist")
    if action == "done":
        return kind.startswith("team_") or kind == "owner_source_disagreement"
    return False


async def _confirm_llm(db: "Database", row: dict, by: str) -> str | None:
    """Spec 10's ``confirm_review`` (Phase 9). Absent before Phase 9 → a warning, never an error."""
    try:
        from app.nlp.events import confirm_review  # type: ignore[import-not-found]
    except ImportError:
        return "confirm_review_unavailable"
    await confirm_review(db, row["id"], by=by)
    return None


async def _change_row(db: "Database", row: dict) -> dict | None:
    if row.get("ref_table") == "owner_changes" and row.get("ref_id"):
        return await db.fetchone("SELECT * FROM owner_changes WHERE id = ?", (row["ref_id"],), writer=True)
    return None


async def resolve_review(db: "Database", review_id: int, action: str, *, by: str, note: str | None,
                         evidence_url: str | None = None, whitelist_reason: str | None = None,
                         window: tuple[int, int] | None = None, now: int | None = None) -> ActionResult:
    from app.integrity import owner_admin
    from app.integrity.whitelist import APPLIES_TO_FOR_CHANGE
    now = int(now if now is not None else now_ts())
    row = await db.fetchone("SELECT * FROM review_queue WHERE id = ?", (int(review_id),), writer=True)
    if row is None:
        return err("not_found")
    base = {"id": row["id"], "occupant_key": row["occupant_key"], "status": row["status"]}
    if action not in ACTIONS:
        return err("invalid_transition", **base)
    if row["status"] != "open" or not _allowed(action, row["kind"], row.get("suggested_flag")):
        return err("invalid_transition", **base)
    if action in ("confirm_flag", "whitelist", "key_compromise") and not evidence_url:
        return err("evidence_required", **base)
    warning = None
    status = {"confirm_flag": "confirmed", "whitelist": "confirmed", "key_compromise": "confirmed",
              "dismiss": "rejected", "done": "resolved"}[action]
    change = await _change_row(db, row)
    occ_row = await db.fetchone("SELECT netuid FROM occupants WHERE occupant_key = ?", (row["occupant_key"],),
                                writer=True) if row["occupant_key"] else None
    if action == "confirm_flag":
        flag = "owner_extraction" if row["kind"] == "owner_extraction_candidate" else row["suggested_flag"]
        await db.execute("INSERT INTO manual_flags (occupant_key, flag, state, evidence_url, note, set_by, set_at) "
                         "VALUES (?, ?, 'confirmed', ?, ?, ?, ?)", (row["occupant_key"], flag, evidence_url, note, by,
                                                                  now))
        if row["kind"] == "llm_flag_candidate":
            warning = await _confirm_llm(db, row, by)
    elif action == "whitelist":
        if not whitelist_reason:
            return err("reason_required", **base)
        ev_at = int(change["event_at"]) if change else int(row["created_at"])
        w = window or (ev_at - DAY, ev_at + DAY)
        if change and change["change_type"] == "extraction_detected":
            ev = json.loads(change["evidence_json"] or "{}")
            ats = [int(x) for x in ev.get("event_ats") or [] if x is not None]
            if ats and window is None:
                w = (min(ats) - DAY, max(ats) + DAY)
        applies = APPLIES_TO_FOR_CHANGE.get(change["change_type"], "all") if change else "all"
        old = new = None
        if change and applies == "coldkey_change":
            old, new = change.get("old_value"), change.get("new_value")
        res = await owner_admin.add_whitelist(db, occupant_key=row["occupant_key"],
                                              netuid=occ_row["netuid"] if occ_row else None, reason=whitelist_reason,
                                              applies_to=applies, window_start=int(w[0]), window_end=int(w[1]),
                                              evidence_url=evidence_url, by=by,
                                              change_id=change["id"] if change else None, old_coldkey=old,
                                              new_coldkey=new, note=note, now=now)
        if not res.ok:
            return ActionResult(False, error=res.error, warning=res.warning, **base)
    elif action == "key_compromise":
        ev_at = int(change["event_at"]) if change else int(row["created_at"])
        w = window or (ev_at - DAY, ev_at + DAY)
        res = await owner_admin.override_key_compromise(db, occupant_key=row["occupant_key"],
                                                        netuid=occ_row["netuid"] if occ_row else None,
                                                        window_start=int(w[0]), window_end=int(w[1]),
                                                        evidence_url=evidence_url, by=by, note=note, now=now)
        if not res.ok:
            return ActionResult(False, error=res.error, warning=res.warning, **base)
        warning = res.warning
        if row["kind"] == "llm_flag_candidate":
            warning = await _confirm_llm(db, row, by) or warning
    elif action == "done" and row["kind"] == "llm_flag_candidate":
        warning = await _confirm_llm(db, row, by)       # security_incident acknowledge: no manual_flags row
    await db.execute("UPDATE review_queue SET status = ?, resolved_by = ?, resolved_at = ?, resolution_note = ?, "
                     "resolution_action = ? WHERE id = ? AND status = 'open'",
                     (status, by, now, note, action, row["id"]))
    return ActionResult(True, id=row["id"], occupant_key=row["occupant_key"], status=status, warning=warning)


async def auto_clear(db: "Database", now: int, *, source_agree_hours: int = 48) -> int:
    now = int(now)
    upd = ("UPDATE review_queue SET status = 'expired', resolution_action = 'auto_cleared', resolved_by = 'system', "
           "resolved_at = ?, resolution_note = ? WHERE status = 'open' AND kind = ? AND ")
    n = 0
    r = await db.execute(upd + "NOT EXISTS (SELECT 1 FROM team_members m WHERE m.occupant_key = review_queue."
                         "occupant_key AND m.status = 'candidate')", (now, "no candidates left", "team_candidate"))
    n += max(0, r.rowcount)
    import app.config as app_config
    fresh = now - int(app_config.settings.TEAM_REVERIFY_DAYS) * DAY
    r = await db.execute(upd + "EXISTS (SELECT 1 FROM team_members m WHERE m.id = review_queue.ref_id AND "
                         "(m.status <> 'verified' OR m.last_checked_at >= ?))",
                         (now, "re-verified or no longer verified", "team_reverify", fresh))
    n += max(0, r.rowcount)
    r = await db.execute(upd + "EXISTS (SELECT 1 FROM team_members m WHERE m.id = review_queue.ref_id AND "
                         "(m.status = 'rejected' OR m.source_last_seen_at > review_queue.created_at))",
                         (now, "rejected or re-listed", "team_delisted"))
    n += max(0, r.rowcount)
    r = await db.execute(upd + "created_at <= ? AND NOT EXISTS (SELECT 1 FROM events e WHERE "
                         "e.event_type = 'owner_source_disagreement' AND e.occupant_key = review_queue.occupant_key "
                         "AND e.observed_at > ?)", (now, "sources agree again", "owner_source_disagreement",
                                                    now - source_agree_hours * 3600, now - source_agree_hours * 3600))
    n += max(0, r.rowcount)
    return n
