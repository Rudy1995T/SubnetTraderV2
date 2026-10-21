"""Occupant intervals per netuid and death onsets (spec 13 §8.3), read from ``v_quant_occupants`` + ``events``."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

INF = 1 << 62


@dataclass(frozen=True, slots=True)
class OccupantRow:
    key: str
    netuid: int
    reg_block: int
    start: int            # registered_ts (exact, else 04's estimate) or first_seen_at
    end: int              # COALESCE(deregistered_ts, deregistered_at) or INF
    registered_ts: int | None


class OccupantIndex:
    def __init__(self, rows: list[OccupantRow], reoccupied: dict[str, int] | None = None) -> None:
        self._by_key = {r.key: r for r in rows}
        self._by_netuid: dict[int, list[OccupantRow]] = {}
        for r in rows:
            self._by_netuid.setdefault(r.netuid, []).append(r)
        for lst in self._by_netuid.values():
            lst.sort(key=lambda r: (r.start, r.reg_block))
        self._reoccupied = dict(reoccupied or {})

    @classmethod
    def load(cls, con: sqlite3.Connection) -> "OccupantIndex":
        rows = []
        for r in con.execute("SELECT occupant_key, netuid, network_registered_at, registered_ts, first_seen_at, "
                             "deregistered_at, deregistered_ts FROM v_quant_occupants"):
            start = r[3] if r[3] is not None else r[4]
            end = r[6] if r[6] is not None else r[5]
            rows.append(OccupantRow(str(r[0]), int(r[1]), int(r[2]), int(start if start is not None else 0),
                                    int(end) if end is not None else INF,
                                    int(r[3]) if r[3] is not None else None))
        reocc: dict[str, int] = {}
        for r in con.execute("SELECT payload, COALESCE(event_at, observed_at) FROM events "
                             "WHERE event_type = 'netuid_reoccupied'"):
            try:
                p = json.loads(r[0] or "{}")
            except ValueError:
                continue
            old = p.get("old_occupant_key") or p.get("old_key")
            if old and r[1] is not None:
                t = int(r[1])
                reocc[old] = min(t, reocc.get(old, INF))
        return cls(rows, reocc)

    def get(self, occupant_key: str) -> OccupantRow | None:
        return self._by_key.get(occupant_key)

    def occupant_at(self, netuid: int, ts: int) -> tuple[str, int] | None:
        """(occupant_key, reg_block) holding ``netuid`` at ``ts``; overlapping intervals (04's live
        ``deregistered_at`` is the detection time) resolve to the greatest registration block."""
        best: OccupantRow | None = None
        for r in self._by_netuid.get(netuid, ()):
            if r.start <= ts < r.end and (best is None or r.reg_block > best.reg_block):
                best = r
        return (best.key, best.reg_block) if best else None

    def registered_ts(self, occupant_key: str | None) -> int | None:
        r = self._by_key.get(occupant_key) if occupant_key else None
        return r.registered_ts if r else None

    def death_onset(self, occupant_key: str | None) -> tuple[int, str] | None:
        """Earliest of the occupant's deregistration time and a ``netuid_reoccupied`` event naming it as the old key."""
        if not occupant_key:
            return None
        cands = []
        r = self._by_key.get(occupant_key)
        if r is not None and r.end < INF:
            cands.append(r.end)
        if occupant_key in self._reoccupied:
            cands.append(self._reoccupied[occupant_key])
        return (min(cands), "dereg") if cands else None

    def occupants_alive_on(self, day: int) -> list[str]:
        return sorted(r.key for r in self._by_key.values() if r.start <= day < r.end)

    def __len__(self) -> int:
        return len(self._by_key)


def sorted_anchor_list(pairs) -> list[tuple[int, int]]:
    """De-duplicated, block-sorted ``(block, unix)`` anchors whose unix time is non-decreasing."""
    best: dict[int, int] = {}
    for b, t in pairs:
        if b is None or t is None:
            continue
        best.setdefault(int(b), int(t))
    out: list[tuple[int, int]] = []
    for b in sorted(best):
        t = best[b]
        if out and t < out[-1][1]:
            continue          # a non-monotone anchor (bad label) would make the interpolation fold back
        out.append((b, t))
    return out


def load_block_anchors(con: sqlite3.Connection) -> list[tuple[int, int]]:
    """17's exact ``block_times`` first, then ``v_quant_chain_daily (block_number, ts)`` (13 §8.2)."""
    pairs = list(con.execute("SELECT block_number, event_at FROM block_times"))
    have = {p[0] for p in pairs}
    pairs += [p for p in con.execute("SELECT block_number, ts FROM v_quant_chain_daily WHERE block_number IS NOT NULL")
              if p[0] not in have]
    return sorted_anchor_list(pairs)

