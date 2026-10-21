"""Owner-event whitelist matching (spec 11 §2.9). Metrics always ask ``is_whitelisted(·, t)`` (PIT by ``created_at`` /
``retired_at``); the ``whitelisted`` columns on ``owner_changes``/``owner_stake_events`` are a display cache only."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Iterable

from app.util.time import parse_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

STAKE_KINDS = frozenset({"sell", "sell_full", "swap_out", "transfer_out", "move_internal", "buy", "swap_in",
                         "transfer_in", "lock", "lock_move", "perpetual_on", "perpetual_off", "other"})
_CHANGE_CLASS = {
    "coldkey_swap": {"coldkey_change"}, "owner_transfer": {"coldkey_change"},
    "coldkey_change_unclassified": {"coldkey_change"}, "subnet_owner_changed": {"coldkey_change"},
    "coldkey_swap_announced": {"coldkey_change"},
    "hotkey_repoint": {"hotkey_repoint"}, "hotkey_repoint_identity_churn": {"hotkey_repoint", "identity_churn"},
    "perpetual_off": {"perpetual_off"}, "lock_reduced": {"perpetual_off"},
    "extraction_detected": {"extraction"}, "initial_alpha": {"initial_alpha"},
}
# the applies_to a review "whitelist" action writes for an owner_changes item (§2.10)
APPLIES_TO_FOR_CHANGE = {
    "coldkey_swap": "coldkey_change", "owner_transfer": "coldkey_change", "coldkey_change_unclassified":
    "coldkey_change", "subnet_owner_changed": "coldkey_change", "coldkey_swap_announced": "coldkey_change",
    "hotkey_repoint": "hotkey_repoint", "hotkey_repoint_identity_churn": "identity_churn",
    "perpetual_off": "perpetual_off", "lock_reduced": "perpetual_off",
    # an extraction is the stake events behind it — whitelisting the change itself would not change S1
    "extraction_detected": "stake_events",
}


def item_class(change_type_or_kind: str) -> set[str]:
    if change_type_or_kind in _CHANGE_CLASS:
        return set(_CHANGE_CLASS[change_type_or_kind])
    if change_type_or_kind in STAKE_KINDS or change_type_or_kind == "stake_event":
        return {"stake_events"}
    return set()


@dataclass(frozen=True, slots=True)
class WlRow:
    id: int
    applies_to: str
    reason: str
    window_start: int
    window_end: int
    old_coldkey: str | None
    new_coldkey: str | None
    created_at: int
    retired_at: int | None


class WhitelistSet:
    """All whitelist rows of one occupant, matched in memory (one query per occupant per run)."""

    def __init__(self, rows: Iterable[WlRow]) -> None:
        self.rows = sorted(rows, key=lambda r: r.id)

    @classmethod
    async def load(cls, db: "Database", occupant_key: str) -> "WhitelistSet":
        rows = await db.fetchall("SELECT id, applies_to, reason, window_start, window_end, old_coldkey, new_coldkey, "
                                 "created_at, retired_at FROM owner_event_whitelist WHERE occupant_key = ?",
                                 (occupant_key,), writer=True)
        return cls(WlRow(**r) for r in rows)

    def match(self, *, classes: set[str], event_at: int, t: int, old_coldkey: str | None = None,
              new_coldkey: str | None = None) -> int | None:
        for r in self.rows:
            if r.created_at > t or (r.retired_at is not None and r.retired_at <= t):
                continue
            if not (r.window_start <= event_at <= r.window_end):
                continue
            if r.applies_to != "all" and r.applies_to not in classes:
                continue
            if r.old_coldkey and r.old_coldkey != old_coldkey:
                continue
            if r.new_coldkey and r.new_coldkey != new_coldkey:
                continue
            return r.id
        return None

    def change(self, c, t: int) -> int | None:
        """``c``: an OwnerChange or an owner_changes row dict."""
        g = c.get if isinstance(c, dict) else (lambda k, d=None: getattr(c, k, d))
        ct = g("change_type")
        old = new = None
        if ct in ("coldkey_swap", "owner_transfer", "coldkey_change_unclassified", "subnet_owner_changed",
                  "coldkey_swap_announced"):
            old, new = g("old_value"), g("new_value")
        return self.match(classes=item_class(ct), event_at=int(g("event_at")), t=t, old_coldkey=old, new_coldkey=new)

    def stake_event(self, e, t: int) -> int | None:
        """A stake event: ``old_coldkey`` ↔ its owner coldkey, ``new_coldkey`` ↔ its ``dest_coldkey`` (§2.9)."""
        g = e.get if isinstance(e, dict) else (lambda k, d=None: getattr(e, k, d))
        return self.match(classes={"stake_events"}, event_at=int(g("event_at")), t=t,
                          old_coldkey=g("owner_coldkey"), new_coldkey=g("dest_coldkey"))


async def is_whitelisted(db: "Database", occupant_key: str, *, item_class: set[str], event_at: int,
                         t: int | datetime, old_coldkey: str | None = None,
                         new_coldkey: str | None = None) -> int | None:
    ws = await WhitelistSet.load(db, occupant_key)
    return ws.match(classes=set(item_class), event_at=int(event_at), t=parse_ts(t), old_coldkey=old_coldkey,
                    new_coldkey=new_coldkey)


async def refresh_whitelist_cache(db: "Database", occupant_key: str, t: int) -> int:
    """Re-derive the display cache of one occupant's changes and stake events as of ``t``."""
    ws = await WhitelistSet.load(db, occupant_key)
    n = 0
    for c in await db.fetchall("SELECT id, change_type, old_value, new_value, event_at, whitelist_id FROM owner_changes "
                               "WHERE occupant_key = ?", (occupant_key,), writer=True):
        wid = ws.change(c, t)
        if wid != c["whitelist_id"]:
            await db.execute("UPDATE owner_changes SET whitelisted = ?, whitelist_id = ? WHERE id = ?",
                             (1 if wid else 0, wid, c["id"]))
            n += 1
    for e in await db.fetchall("SELECT event_uid, owner_coldkey, dest_coldkey, event_at, whitelist_id FROM "
                               "owner_stake_events WHERE occupant_key = ?", (occupant_key,), writer=True):
        wid = ws.stake_event(e, t)
        if wid != e["whitelist_id"]:
            await db.execute("UPDATE owner_stake_events SET whitelisted = ?, whitelist_id = ? WHERE event_uid = ? "
                             "AND occupant_key = ?", (1 if wid else 0, wid, e["event_uid"], occupant_key))
            n += 1
    return n
