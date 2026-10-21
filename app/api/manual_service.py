"""The manual writes this spec owns (14 §3.2–§3.5): product status, notes, manual flags. Shared by the API and
Telegram; every write emits ``manual_entry`` (``source='api'``) and returns 11's ``ActionResult`` so both surfaces map
errors the same way. Team/owner/review writes are 11's domain functions, not here.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import app.config as app_config
from app.integrity.models import ActionResult, err
from app.integrity.team_parse import valid_url
from app.storage.events import emit_event
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

PRODUCT_STATUSES = ("none", "waitlist", "beta", "live")
CONFIRMABLE = ("owner_extraction", "key_compromise")
CLEARABLE = ("owner_extraction", "key_compromise", "owner_control_change")


async def _current(db: "Database", netuid: int) -> dict | None:
    r = await db.fetchone("SELECT occupant_key, netuid, name FROM subnets WHERE netuid = ?", (int(netuid),),
                          writer=True)
    return None if r is None or int(r["netuid"]) == 0 else r


async def _entry(db: "Database", occ: dict, *, kind: str, by: str, row_id: int, summary: str, evidence_url: str | None,
                 now: int) -> int | None:
    from app.api import hub
    eid = await emit_event(db, event_type="manual_entry", occupant_key=occ["occupant_key"], netuid=occ["netuid"],
                           severity="info", event_at=now, observed_at=now,
                           payload={"kind": kind, "by": by, "row_id": row_id, "summary": summary[:200],
                                    "evidence_url": evidence_url},
                           dedupe_key=None, source="api", run_id=None)
    hub.nudge()
    return eid


async def set_product(db: "Database", *, netuid: int, status: str | None, evidence_url: str, note: str | None,
                      by: str, now: int | None = None) -> ActionResult:
    now = int(now if now is not None else now_ts())
    occ = await _current(db, netuid)
    if occ is None:
        return err("no_current_occupant")
    if status is not None and status not in PRODUCT_STATUSES:
        return err("invalid_kind", occupant_key=occ["occupant_key"], warning="status none|waitlist|beta|live|null")
    if not evidence_url:
        return err("evidence_required", occupant_key=occ["occupant_key"])
    if not valid_url(evidence_url):
        return err("invalid_url", occupant_key=occ["occupant_key"])
    prev = await db.fetchval("SELECT value FROM manual_notes WHERE occupant_key = ? AND kind = 'product_status' "
                             "ORDER BY created_at DESC, id DESC LIMIT 1", (occ["occupant_key"],), writer=True)
    async with db.transaction():
        r = await db.execute("INSERT INTO manual_notes (occupant_key, kind, value, body, evidence_url, created_at, "
                             "created_by) VALUES (?, 'product_status', ?, ?, ?, ?, ?)",
                             (occ["occupant_key"], status, note, evidence_url, now, by))
        rid = int(r.lastrowid)
        eid = await _entry(db, occ, kind="product", by=by, row_id=rid,
                           summary=f"product_status = {status or 'cleared'} (was {prev or 'unset'})",
                           evidence_url=evidence_url, now=now)
    return ActionResult(True, id=rid, occupant_key=occ["occupant_key"], status=status or "cleared", event_id=eid,
                        extra={"previous": prev})


async def add_note(db: "Database", *, netuid: int | None = None, occupant_key: str | None = None, text: str,
                   evidence_url: str | None, by: str, ref_event_id: int | None = None, require_evidence: bool = True,
                   now: int | None = None) -> ActionResult:
    now = int(now if now is not None else now_ts())
    if occupant_key:
        occ = await db.fetchone("SELECT occupant_key, netuid, name FROM occupants WHERE occupant_key = ?",
                                (occupant_key,), writer=True)
    else:
        occ = await _current(db, int(netuid)) if netuid is not None else None
    if occ is None:
        return err("no_current_occupant")
    text = (text or "").strip()
    if not 1 <= len(text) <= 2000:
        return err("invalid_transition", occupant_key=occ["occupant_key"], warning="text must be 1..2000 chars")
    if require_evidence and not evidence_url:
        return err("evidence_required", occupant_key=occ["occupant_key"])
    if evidence_url and not valid_url(evidence_url):
        return err("invalid_url", occupant_key=occ["occupant_key"])
    async with db.transaction():
        r = await db.execute("INSERT INTO manual_notes (occupant_key, kind, body, evidence_url, ref_event_id, "
                             "created_at, created_by) VALUES (?, 'note', ?, ?, ?, ?, ?)",
                             (occ["occupant_key"], text, evidence_url, ref_event_id, now, by))
        rid = int(r.lastrowid)
        eid = await _entry(db, occ, kind="note", by=by, row_id=rid, summary=text, evidence_url=evidence_url, now=now)
    return ActionResult(True, id=rid, occupant_key=occ["occupant_key"], status="noted", event_id=eid)


async def set_flag(db: "Database", *, netuid: int, flag: str, action: str, evidence_url: str, note: str | None,
                   by: str, spec: str | None = None, now: int | None = None) -> ActionResult:
    """02 §D5: confirm only owner_extraction/key_compromise; clear the three manual flags only while active."""
    from app.api.queries import flag_is_active
    now = int(now if now is not None else now_ts())
    spec = spec or app_config.settings.SPEC_VERSION
    occ = await _current(db, netuid)
    if occ is None:
        return err("no_current_occupant")
    base = {"occupant_key": occ["occupant_key"]}
    if action not in ("confirm", "clear"):
        return err("invalid_kind", **base, warning="action confirm|clear")
    if action == "confirm" and flag not in CONFIRMABLE:
        return err("flag_not_human_confirmable", **base, warning=f"{flag} cannot be confirmed by hand")
    if action == "clear" and flag not in CLEARABLE:
        return err("flag_not_human_confirmable", **base, warning=f"{flag} cannot be cleared by hand")
    if not evidence_url:
        return err("evidence_required", **base)
    if not valid_url(evidence_url):
        return err("invalid_url", **base)
    if action == "clear" and not await flag_is_active(db, occupant_key=occ["occupant_key"], flag=flag, spec=spec):
        return err("flag_not_active", **base, warning=f"{flag} is not active for SN{occ['netuid']}")
    state = "confirmed" if action == "confirm" else "cleared"
    async with db.transaction():
        r = await db.execute("INSERT INTO manual_flags (occupant_key, flag, state, evidence_url, note, set_by, set_at) "
                             "VALUES (?, ?, ?, ?, ?, ?, ?)", (occ["occupant_key"], flag, state, evidence_url, note, by,
                                                             now))
        rid = int(r.lastrowid)
        eid = await _entry(db, occ, kind="flag", by=by, row_id=rid, summary=f"{flag} {state}",
                           evidence_url=evidence_url, now=now)
    return ActionResult(True, id=rid, occupant_key=occ["occupant_key"], status=state, event_id=eid)
