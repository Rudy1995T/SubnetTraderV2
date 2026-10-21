"""Per-batch alias table for entity linking (spec 10 §2.4 second system block) and the occupant-at-time map (§3.1).

The table is built from 09's shared ``OccupantIndex`` (current occupants, active non-legacy-only X handles, the latest
manual alias note, the unique website host); its sha256 is diagnostic only (``llm_batches.alias_sha256``), never part
of ``classifier_version``. ``OccupantsAt`` maps netuid → occupant at an item's ``event_at`` (greatest
``COALESCE(registered_ts, first_seen_at) ≤ event_at``; none → the link is dropped as ``unmapped``).
"""
from __future__ import annotations

import bisect
import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.nlp.match import OccupantIndex

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

AliasIndex = OccupantIndex          # 10 §8.2 name for the rules classifier's index
HEADER = "ALIASES (netuid|name|symbol|x_handle|extra). Use for entity linking only."


def _cell(v: str | None) -> str:
    return " ".join((v or "").replace("|", "/").split())


def render_alias_table(index: OccupantIndex) -> str:
    lines = [HEADER]
    for r in sorted(index.refs, key=lambda r: r.netuid):
        handles = ";".join(f"@{h}" for h in r.handles)
        extra = [x for x in ([r.website_host] if r.website_host else []) + list(r.aliases) if x]
        lines.append("|".join([str(r.netuid), _cell(r.name), _cell(r.symbol), _cell(handles),
                               _cell(";".join(dict.fromkeys(extra)))]))
    return "\n".join(lines) if len(lines) > 1 else ""


async def build_alias_table(db: "Database") -> tuple[str, str]:
    """(text, sha256); ``("", "")`` when the registry has no current occupant (submit fails ``alias_table_empty``)."""
    text = render_alias_table(await OccupantIndex.load(db))
    return text, (hashlib.sha256(text.encode("utf-8")).hexdigest() if text else "")


@dataclass
class OccupantsAt:
    by_netuid: dict[int, list[tuple[int, str]]]      # netuid → sorted [(start_ts, occupant_key)]

    @classmethod
    async def load(cls, db: "Database") -> "OccupantsAt":
        rows = await db.fetchall("SELECT occupant_key, netuid, COALESCE(registered_ts, first_seen_at) AS start "
                                 "FROM occupants WHERE COALESCE(registered_ts, first_seen_at) IS NOT NULL")
        m: dict[int, list[tuple[int, str]]] = {}
        for r in rows:
            m.setdefault(int(r["netuid"]), []).append((int(r["start"]), r["occupant_key"]))
        for v in m.values():
            v.sort()
        return cls(m)

    def at(self, netuid: int, ts: int) -> str | None:
        seq = self.by_netuid.get(int(netuid))
        if not seq:
            return None
        i = bisect.bisect_right(seq, (int(ts), "￿")) - 1
        return seq[i][1] if i >= 0 else None

    def netuid_of(self, occupant_key: str) -> int | None:
        try:
            return int(str(occupant_key).split(":", 1)[0])
        except ValueError:
            return None


__all__ = ["AliasIndex", "build_alias_table", "render_alias_table", "OccupantsAt", "HEADER"]
