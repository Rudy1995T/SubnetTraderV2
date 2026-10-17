"""Historical occupancy reconstruction (spec 17 §2.2).

``network_registered_at`` is itself the successor's registration block, so sampled observations give exact block
boundaries: occupant(reg_i) owns ``[reg_i, reg_{i+1} − 1]``. Observations come from authoritative RPC buckets and
trusted ledger rows (``pool_history.network_registered_at``, which the ledger import stores only from the trust
boundary) plus 04's ``occupants``. 04 owns the status of current occupants; 17 only inserts historical rows and fills
``first_seen_*``, ``registered_ts``, ``predecessor_key``.
"""
from __future__ import annotations

import bisect
import sqlite3
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterable

from app.backfill import common
from app.logging.logger import logger
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.backfill.rpc_client import RpcGateway
    from app.storage.db import Database

HISTORICAL_SOURCES = ("rpc", "onfinality", "legacy_ledger")
STAMP_SLICE = 50_000


@dataclass(frozen=True)
class Obs:
    reg: int
    first_block: int
    last_block: int
    first_ts: int
    last_ts: int
    source: str          # blockmachine | onfinality | opentensor_archive | ledger_* | registry


@dataclass(frozen=True)
class Interval:
    netuid: int
    reg: int
    occupant_key: str
    start_block: int
    end_block: int | None
    first_seen_block: int
    last_seen_block: int
    registered_ts: int | None = None
    end_ts: int | None = None
    first_seen_at: int | None = None
    last_seen_at: int | None = None
    source: str = "rpc"


# ── index ────────────────────────────────────────────────────────────────────────────────────────────────────
class OccupancyIndex:
    def __init__(self, by_netuid: dict[int, list[Interval]]) -> None:
        self._by = {n: sorted(v, key=lambda i: i.start_block) for n, v in by_netuid.items()}
        self._starts = {n: [i.start_block for i in v] for n, v in self._by.items()}

    @classmethod
    def load(cls, con: sqlite3.Connection) -> "OccupancyIndex":
        rows = con.execute("SELECT netuid, network_registered_at, occupant_key, registered_ts, first_seen_block, "
                           "last_seen_block, first_seen_at, last_seen_at, source FROM occupants "
                           "ORDER BY netuid, network_registered_at").fetchall()
        by: dict[int, list[Any]] = {}
        for r in rows:
            by.setdefault(int(r[0]), []).append(r)
        out: dict[int, list[Interval]] = {}
        for n, rs in by.items():
            ivs = []
            for i, r in enumerate(rs):
                nxt = rs[i + 1] if i + 1 < len(rs) else None
                ivs.append(Interval(n, int(r[1]), r[2], int(r[1]), (int(nxt[1]) - 1) if nxt else None,
                                    int(r[4]), int(r[5]), r[3], nxt[3] if nxt else None, r[6], r[7], r[8]))
            out[n] = ivs
        return cls(out)

    def intervals(self, netuid: int) -> list[Interval]:
        return list(self._by.get(netuid, []))

    def netuids(self) -> list[int]:
        return sorted(self._by)

    def occupant_at_block(self, netuid: int, block: int) -> str | None:
        starts = self._starts.get(netuid)
        if not starts:
            return None
        i = bisect.bisect_right(starts, block) - 1
        if i < 0:
            return None
        iv = self._by[netuid][i]
        if iv.end_block is not None and block > iv.end_block:
            return None
        return iv.occupant_key

    def occupant_at_ts(self, netuid: int, ts: int) -> str | None:
        """None when ``registered_ts`` is unknown or ``ts`` precedes every known registration."""
        ivs = self._by.get(netuid) or []
        for iv in reversed(ivs):
            if iv.registered_ts is None:
                return None
            if ts >= iv.registered_ts:
                if iv.end_ts is not None and ts >= iv.end_ts:
                    return None
                return iv.occupant_key
        return None


# ── observations → intervals (pure) ──────────────────────────────────────────────────────────────────────────
def collect_observations(con: sqlite3.Connection) -> dict[int, list[Obs]]:
    out: dict[int, list[Obs]] = {}
    for n, reg, b0, b1, t0, t1, src in con.execute(
            "SELECT netuid, network_registered_at, MIN(block_number), MAX(block_number), MIN(timestamp), "
            "MAX(timestamp), MIN(source) FROM pool_history WHERE network_registered_at IS NOT NULL GROUP BY 1, 2"):
        out.setdefault(int(n), []).append(Obs(int(reg), int(b0), int(b1), int(t0), int(t1), str(src or "")))
    for n, reg, b0, b1, t0, t1 in con.execute(
            "SELECT netuid, network_registered_at, first_seen_block, last_seen_block, first_seen_at, last_seen_at "
            "FROM occupants"):
        out.setdefault(int(n), []).append(Obs(int(reg), int(b0), int(b1), int(t0), int(t1), "registry"))
    return out


def _source_for(srcs: set[str]) -> str:
    if "blockmachine" in srcs or "opentensor_archive" in srcs:
        return "rpc"
    if "onfinality" in srcs:
        return "onfinality"
    if any(s.startswith("ledger_") for s in srcs):
        return "legacy_ledger"
    return "rpc"


def build_intervals(obs: dict[int, list[Obs]]) -> tuple[dict[int, list[Interval]], list[dict]]:
    """(intervals by netuid, anomalies). A netuid with an anomaly gets no intervals (not written, not stamped)."""
    intervals: dict[int, list[Interval]] = {}
    anomalies: list[dict] = []
    for n in sorted(obs):
        merged: dict[int, dict] = {}
        for o in obs[n]:
            m = merged.setdefault(o.reg, {"b0": o.first_block, "b1": o.last_block, "t0": o.first_ts,
                                          "t1": o.last_ts, "src": set()})
            if o.first_block < m["b0"]:
                m["b0"], m["t0"] = o.first_block, o.first_ts
            if o.last_block > m["b1"]:
                m["b1"], m["t1"] = o.last_block, o.last_ts
            m["src"].add(o.source)
        regs = sorted(merged, key=lambda r: merged[r]["b0"])
        bad = None
        for r in regs:
            if r > merged[r]["b0"]:
                bad = {"netuid": n, "reason": "reg_after_first_observation",
                       "detail": f"reg {r} > first observed block {merged[r]['b0']}"}
                break
        if bad is None and regs != sorted(regs):
            bad = {"netuid": n, "reason": "reg_order", "detail": f"first-appearance order {regs[:6]}"}
        if bad is None:
            for a, b in zip(regs, regs[1:]):
                if merged[a]["b1"] >= b:
                    bad = {"netuid": n, "reason": "reg_reappears",
                           "detail": f"reg {a} observed at block {merged[a]['b1']} ≥ successor reg {b}"}
                    break
        if bad is not None:
            anomalies.append(bad)
            continue
        ivs = []
        for i, r in enumerate(regs):
            m = merged[r]
            nxt = regs[i + 1] if i + 1 < len(regs) else None
            srcs = m["src"] - {"registry"}
            ivs.append(Interval(n, r, f"{n}:{r}", r, (nxt - 1) if nxt is not None else None, m["b0"], m["b1"],
                                first_seen_at=m["t0"], last_seen_at=m["t1"],
                                source=_source_for(srcs) if srcs else "registry"))
        intervals[n] = ivs
    return intervals, anomalies


# ── registration timestamps ──────────────────────────────────────────────────────────────────────────────────
async def resolve_registered_ts(rpc: "RpcGateway | None", con: sqlite3.Connection, regs: Iterable[int], *,
                                run_id: str) -> dict[int, int]:
    """Exact ``registered_ts`` per reg block: ``block_times`` first, else one ``chain_getBlockHash`` +
    ``Timestamp.Now`` read (written back to ``block_times``). Unresolvable blocks are left out."""
    regs = sorted({int(r) for r in regs if int(r) > 0})
    out: dict[int, int] = {}
    if regs:
        for i in range(0, len(regs), 900):
            part = regs[i:i + 900]
            q = f"SELECT block_number, ts_ms FROM block_times WHERE block_number IN ({','.join('?' * len(part))})"
            out.update({int(b): int(ms) // 1000 for b, ms in con.execute(q, part)})
    missing = [r for r in regs if r not in out]
    if missing and rpc is not None:
        await rpc.connect()
        got: list[tuple[int, int]] = []
        for r in missing:
            if common.stop_requested():
                break
            try:
                ms = await rpc.timestamp_ms(await rpc.block_hash(r))
            except Exception as exc:
                logger.warning("registered_ts unresolved", data={"block": r, "error": str(exc)[:160]})
                continue
            got.append((r, ms))
            out[r] = ms // 1000
        if got:
            with common.tx(con):
                con.executemany("INSERT OR IGNORE INTO block_times (block_number, ts_ms, endpoint, observed_at, run_id)"
                                " VALUES (?,?,?,?,?)", [(b, ms, rpc.endpoint.label, now_ts(), run_id) for b, ms in got])
    return out


# ── writes ───────────────────────────────────────────────────────────────────────────────────────────────────
def _last_name(con: sqlite3.Connection, key: str) -> tuple[str | None, str | None]:
    r = con.execute("SELECT subnet_name, token_symbol FROM rpc_owner_history WHERE occupant_key = ? "
                    "ORDER BY block_number DESC LIMIT 1", (key,)).fetchone()
    return (r[0], r[1]) if r else (None, None)


def write_occupants(con: sqlite3.Connection, intervals: dict[int, list[Interval]], *, reg_ts: dict[int, int],
                    observed_at: int, run_id: str) -> dict:
    """One transaction per netuid. Historical keys: INSERT (all NOT NULL columns) or extend a 17-written row;
    existing 04 rows: only ``first_seen_*`` = MIN, ``registered_ts`` and ``predecessor_key`` when NULL."""
    rep = {"inserted": 0, "updated": 0, "current_touched": 0, "newer_than_registry": 0, "netuids": 0}
    for n, ivs in sorted(intervals.items()):
        existing = {r["occupant_key"]: dict(r) for r in con.execute(
            "SELECT * FROM occupants WHERE netuid = ?", (n,))}
        live_regs = [r["network_registered_at"] for r in existing.values() if r["status"] in ("active", "pre_launch")]
        cur_reg = max(live_regs) if live_regs else None
        with common.tx(con):
            for i, iv in enumerate(ivs):
                prev_iv = ivs[i - 1] if i > 0 else None
                nxt_iv = ivs[i + 1] if i + 1 < len(ivs) else None
                rts = reg_ts.get(iv.reg)
                pred = prev_iv.occupant_key if prev_iv else None
                succ = nxt_iv.occupant_key if nxt_iv else None
                row = existing.get(iv.occupant_key)
                if row is not None and row["status"] in ("active", "pre_launch"):
                    con.execute(
                        "UPDATE occupants SET first_seen_block = MIN(first_seen_block, ?), "
                        "first_seen_at = CASE WHEN ? < first_seen_block THEN ? ELSE first_seen_at END, "
                        "registered_ts = COALESCE(registered_ts, ?), predecessor_key = COALESCE(predecessor_key, ?) "
                        "WHERE occupant_key = ?",
                        (iv.first_seen_block, iv.first_seen_block, iv.first_seen_at, rts, pred, iv.occupant_key))
                    rep["current_touched"] += 1
                    continue
                if cur_reg is not None and iv.reg > cur_reg:
                    rep["newer_than_registry"] += 1   # 04 has not seen this registration yet; it owns it
                    continue
                if iv.source == "registry":
                    continue                          # a 04 row we have no chain observation for
                end_ts = reg_ts.get(nxt_iv.reg) if nxt_iv else None
                reason = "reoccupied" if nxt_iv else "absent"
                dereg_at = end_ts if nxt_iv else iv.last_seen_at
                if dereg_at is None:
                    dereg_at = iv.last_seen_at
                name, sym = _last_name(con, iv.occupant_key)
                src = iv.source if iv.source in HISTORICAL_SOURCES else "rpc"
                if row is None:
                    con.execute(
                        "INSERT OR IGNORE INTO occupants (occupant_key, netuid, network_registered_at, status, name, "
                        "symbol, first_seen_block, first_seen_at, last_seen_block, last_seen_at, first_emission_block, "
                        "activated_at, deregistered_at, dereg_reason, predecessor_key, successor_key, "
                        "registered_at_est, registered_ts, deregistered_ts, missing_runs, source) "
                        "VALUES (?,?,?,'deregistered',?,?,?,?,?,?,NULL,NULL,?,?,?,?,?,?,?,0,?)",
                        (iv.occupant_key, n, iv.reg, name, sym, iv.first_seen_block, iv.first_seen_at,
                         iv.last_seen_block, iv.last_seen_at, dereg_at, reason, pred, succ, rts, rts, end_ts, src))
                    rep["inserted"] += 1
                elif row["source"] in HISTORICAL_SOURCES or row["status"] == "deregistered":
                    con.execute(
                        "UPDATE occupants SET first_seen_block = MIN(first_seen_block, ?), "
                        "first_seen_at = CASE WHEN ? < first_seen_block THEN ? ELSE first_seen_at END, "
                        "last_seen_block = MAX(last_seen_block, ?), "
                        "last_seen_at = CASE WHEN ? > last_seen_block THEN ? ELSE last_seen_at END, "
                        "registered_ts = COALESCE(registered_ts, ?), registered_at_est = COALESCE(registered_at_est, ?),"
                        " predecessor_key = COALESCE(predecessor_key, ?), successor_key = COALESCE(successor_key, ?), "
                        "deregistered_ts = COALESCE(deregistered_ts, ?), "
                        "dereg_reason = CASE WHEN ? IS NOT NULL AND dereg_reason = 'absent' THEN 'reoccupied' "
                        "  ELSE dereg_reason END, "
                        "deregistered_at = CASE WHEN ? IS NOT NULL AND (dereg_reason = 'absent' OR deregistered_at IS NULL)"
                        "  THEN ? ELSE deregistered_at END, "
                        "name = COALESCE(name, ?), symbol = COALESCE(symbol, ?) WHERE occupant_key = ?",
                        (iv.first_seen_block, iv.first_seen_block, iv.first_seen_at, iv.last_seen_block,
                         iv.last_seen_block, iv.last_seen_at, rts, rts, pred, succ, end_ts, succ, end_ts, end_ts,
                         name, sym, iv.occupant_key))
                    rep["updated"] += 1
        rep["netuids"] += 1
    return rep


async def emit_history_events(db: "Database", *, observed_at: int, run_id: str) -> int:
    """``netuid_reoccupied`` per 17-written transition (idempotent via ``reoccupied:{old}->{new}``)."""
    from app.storage.events import emit_event
    rows = await db.fetchall(
        "SELECT o.occupant_key AS old_key, o.netuid, o.name AS old_name, o.successor_key AS new_key, "
        "n.name AS new_name, n.network_registered_at AS reg_block, n.registered_ts AS new_ts "
        "FROM occupants o JOIN occupants n ON n.occupant_key = o.successor_key "
        "WHERE o.status = 'deregistered' AND o.dereg_reason = 'reoccupied' AND o.source IN "
        "('rpc','onfinality','legacy_ledger') ORDER BY o.netuid, o.network_registered_at", writer=True)
    n = 0
    for r in rows:
        eid = await emit_event(
            db, event_type="netuid_reoccupied", occupant_key=r["new_key"], netuid=r["netuid"], severity="warning",
            event_at=r["new_ts"], observed_at=observed_at,
            payload={"old_key": r["old_key"], "new_key": r["new_key"], "old_name": r["old_name"],
                     "new_name": r["new_name"], "reg_block": r["reg_block"], "backfilled": True},
            dedupe_key=f"reoccupied:{r['old_key']}->{r['new_key']}", source="registry", run_id=run_id,
            alerted_at=observed_at)
        n += eid is not None
    return n


# ── stamping ─────────────────────────────────────────────────────────────────────────────────────────────────
def stamp_all(con: sqlite3.Connection, index: OccupancyIndex, *, netuids: Iterable[int] | None = None) -> dict:
    """NULL ``occupant_key`` → interval key on ``pool_history`` (by block, ≤ 50k rows per statement) and
    ``price_bars`` (by block, else by bucket time vs ``registered_ts``). Never sets ``enriched_at``."""
    rep = {"pool_history_stamped": 0, "price_bars_stamped": 0, "mismatches": 0}
    for n in (sorted(netuids) if netuids is not None else index.netuids()):
        for iv in index.intervals(n):
            hi = iv.end_block if iv.end_block is not None else 2 ** 62
            while True:
                with common.tx(con):
                    cur = con.execute(
                        "UPDATE pool_history SET occupant_key = ? WHERE rowid IN (SELECT rowid FROM pool_history "
                        "WHERE netuid = ? AND block_number BETWEEN ? AND ? AND occupant_key IS NULL LIMIT ?)",
                        (iv.occupant_key, n, iv.start_block, hi, STAMP_SLICE))
                rep["pool_history_stamped"] += max(0, cur.rowcount)
                if cur.rowcount < STAMP_SLICE:
                    break
            rep["mismatches"] += con.execute(
                "SELECT COUNT(*) FROM pool_history WHERE netuid = ? AND block_number BETWEEN ? AND ? "
                "AND occupant_key IS NOT NULL AND occupant_key <> ?", (n, iv.start_block, hi, iv.occupant_key)
            ).fetchone()[0]
            ts_lo = iv.registered_ts
            ts_hi = iv.end_ts if iv.end_ts is not None else 2 ** 62
            with common.tx(con):
                cur = con.execute(
                    "UPDATE price_bars SET occupant_key = ? WHERE netuid = ? AND occupant_key IS NULL AND ("
                    "(block_number IS NOT NULL AND block_number BETWEEN ? AND ?) OR "
                    "(block_number IS NULL AND ? IS NOT NULL AND COALESCE(ts_last, ts) >= ? AND COALESCE(ts_last, ts) < ?))",
                    (iv.occupant_key, n, iv.start_block, hi, ts_lo, ts_lo, ts_hi))
            rep["price_bars_stamped"] += max(0, cur.rowcount)
    rep["unstamped"] = {
        "pool_history": con.execute("SELECT COUNT(*) FROM pool_history WHERE occupant_key IS NULL").fetchone()[0],
        "price_bars_1h": con.execute("SELECT COUNT(*) FROM price_bars WHERE period = '1h' AND occupant_key IS NULL"
                                     ).fetchone()[0]}
    return rep


# ── orchestration ────────────────────────────────────────────────────────────────────────────────────────────
async def reconstruct(con: sqlite3.Connection, rpc: "RpcGateway | None", *, run_id: str,
                      observed_at: int | None = None) -> dict:
    observed_at = observed_at or now_ts()
    obs = collect_observations(con)
    intervals, anomalies = build_intervals(obs)
    regs = {iv.reg for ivs in intervals.values() for iv in ivs}
    reg_ts = await resolve_registered_ts(rpc, con, regs, run_id=run_id)
    w = write_occupants(con, intervals, reg_ts=reg_ts, observed_at=observed_at, run_id=run_id)
    for a in anomalies:
        logger.warning("occupancy anomaly", data=a)
    from app.util.fs import write_atomic
    write_atomic(common.backfill_dir() / "occupancy_anomalies.json", {"at": observed_at, "anomalies": anomalies})
    return {"netuids": len(intervals), "anomalies": anomalies, "reg_ts_resolved": len(reg_ts),
            "regs": len(regs), **w}
