"""Operator overrides (spec 04 §2.7 R2/R3, §3.3). 14 exposes routes/Telegram commands for these (A2)."""
from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

from app.registry.merge import RESOLVED_FIELDS, Merger
from app.registry.models import Occupant
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

MODES = ("fallback", "force", "suppress")
STATUSES = ("active", "needs_review", "rejected", "retired")


def _h(v: str | None) -> str | None:
    return hashlib.sha256(v.strip().encode()).hexdigest() if v is not None else None


async def add_override(db: "Database", *, occupant_key: str, field: str, mode: str, value: str | None = None,
                       suppress_source: str | None = None, suppress_value: str | None = None,
                       reason: str | None, evidence_url: str | None, resolves_event_id: int | None = None,
                       created_by: str, source: str = "manual", source_ref: str | None = None,
                       status: str = "active", created_at: int | None = None, reresolve: bool = True) -> int:
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}")
    if field not in RESOLVED_FIELDS and field not in ("owner_name_editorial",):
        raise ValueError(f"unknown identity field {field!r}")
    if mode in ("force", "suppress") and (not reason or not evidence_url):
        raise ValueError("force/suppress overrides require reason and evidence_url")
    if mode in ("fallback", "force") and not (value or "").strip():
        raise ValueError("fallback/force overrides require a value")
    if mode == "suppress" and (not suppress_source or suppress_value is None):
        raise ValueError("suppress overrides require suppress_source and suppress_value")
    row = await db.fetchone("SELECT * FROM occupants WHERE occupant_key = ?", (occupant_key,))
    if not row:
        raise ValueError(f"unknown occupant {occupant_key!r}")
    ts = created_at if created_at is not None else now_ts()
    res = await db.execute_returning(
        "INSERT OR IGNORE INTO manual_overrides (occupant_key, field, mode, value, value_hash, suppress_source, "
        "suppress_value_hash, source, source_ref, status, reason, evidence_url, resolves_event_id, created_at, "
        "created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id",
        (occupant_key, field, mode, value, _h(value) if value is not None else None, suppress_source,
         _h(suppress_value), source, source_ref, status, reason, evidence_url, resolves_event_id, ts, created_by))
    if not res:
        existing = await db.fetchval(
            "SELECT id FROM manual_overrides WHERE occupant_key = ? AND field = ? AND mode = ? AND source = ? AND "
            "COALESCE(value_hash,'') = COALESCE(?, '') AND COALESCE(suppress_source,'') = COALESCE(?, '') AND "
            "COALESCE(suppress_value_hash,'') = COALESCE(?, '')",
            (occupant_key, field, mode, source, _h(value) if value is not None else None, suppress_source,
             _h(suppress_value)))
        return int(existing)
    oid = int(res[0]["id"])
    if reresolve:
        await Merger(db, f"override:{oid}", now_ts()).resolve_all(Occupant.from_row(row))
    return oid


async def set_override_status(db: "Database", override_id: int, status: str, *, by: str) -> None:
    if status not in ("active", "rejected", "retired"):
        raise ValueError("status must be active|rejected|retired")
    row = await db.fetchone("SELECT * FROM manual_overrides WHERE id = ?", (override_id,))
    if not row:
        raise ValueError(f"unknown override {override_id}")
    await db.execute("UPDATE manual_overrides SET status = ?, retired_at = CASE WHEN ? = 'retired' THEN ? ELSE "
                     "retired_at END WHERE id = ?", (status, status, now_ts(), override_id))
    occ = await db.fetchone("SELECT * FROM occupants WHERE occupant_key = ?", (row["occupant_key"],))
    if occ:
        await Merger(db, f"override:{override_id}:{by}", now_ts()).resolve_all(Occupant.from_row(occ))
