"""Subnet registry & identity (spec 04). Public read API (§3.3) used by the other collectors."""
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from app.registry.legacy import OccupancyLookup, import_edge_notes, import_fundamentals  # noqa: F401
from app.registry.merge import identity_epoch  # noqa: F401
from app.registry.models import Occupant, Resolved
from app.registry.normalise import canonical_website, normalise_x_handle  # noqa: F401
from app.registry.overrides import add_override, set_override_status  # noqa: F401
from app.registry.review import compute_needs_review  # noqa: F401
from app.registry.watch import watch_targets  # noqa: F401
from app.util.time import parse_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database


async def current_occupant(db: "Database", netuid: int) -> Occupant | None:
    r = await db.fetchone("SELECT o.*, s.owner_coldkey, s.owner_hotkey FROM subnets s JOIN occupants o "
                          "USING (occupant_key) WHERE s.netuid = ?", (netuid,))
    return Occupant.from_row(r) if r else None


async def occupant_at(db: "Database", netuid: int, block: int) -> Occupant | None:
    r = await db.fetchone("SELECT * FROM occupants WHERE netuid = ? AND network_registered_at <= ? "
                          "ORDER BY network_registered_at DESC LIMIT 1", (netuid, int(block)))
    return Occupant.from_row(r) if r else None


async def active_occupants(db: "Database", include_pre_launch: bool = False) -> list[Occupant]:
    statuses = ("active", "pre_launch") if include_pre_launch else ("active",)
    ph = ",".join("?" * len(statuses))
    rows = await db.fetchall(f"SELECT o.*, s.owner_coldkey, s.owner_hotkey FROM subnets s JOIN occupants o "
                             f"USING (occupant_key) WHERE o.status IN ({ph}) ORDER BY s.netuid", statuses)
    return [Occupant.from_row(r) for r in rows]


async def resolved(db: "Database", occupant_key: str, field: str,
                   as_of: int | datetime | None = None) -> Resolved | None:
    t = parse_ts(as_of) if as_of is not None else None
    row = await db.fetchone("SELECT * FROM identity_resolved WHERE occupant_key = ? AND field = ?",
                            (occupant_key, field))
    if row and (t is None or t >= row["resolved_at"]):
        import json
        return Resolved(row["occupant_key"], row["field"], row["value"], row["value_norm"], row["ref_kind"],
                        row["source"], row["source_observed_at"], row["status"],
                        json.loads(row["candidates_json"] or "[]"), row["override_id"],
                        json.loads(row["flags_json"]) if row["flags_json"] else [], row["resolved_at"])
    if t is None:
        return None
    from app.registry.merge import Merger
    o = await db.fetchone("SELECT o.*, NULL AS owner_coldkey FROM occupants o WHERE occupant_key = ?", (occupant_key,))
    if not o:
        return None
    res = await Merger(db, "replay", t).compute(Occupant.from_row(o), as_of=t)
    return res.get(field)
