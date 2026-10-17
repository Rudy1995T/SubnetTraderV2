"""Workbench distance-from-deregistration (spec 19 §2.4): graduation-aware pool, rank from the bottom, % gap to the
cull line, immunity remaining, hours in the bottom-k, spot leads, the network "weather" row and two transition events.

Chain rule: at each successful registration (subnet count at ``SubnetLimit``) the lowest ``SubnetMovingPrice`` among
non-immune subnets is culled (no entry = not started = 0; ties → oldest registration). ``IMM`` is the probed
``NetworkImmunityPeriod`` (``chain_constants`` rpc_storage), else the config value.

CLI: ``python -m app.features.dereg_distance run [--as-of YYYY-MM-DDTHH]`` · ``show`` (bottom of the pool).
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Sequence

import app.config as app_config
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.storage.pit import put_metric_values
from app.util.time import floor_hour, now_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

SPEC_VERSION = "ts-1.0"
DEF = "wb-1"
BLOCK_S = 12
BLOCKS_PER_DAY = 7200
DEFAULT_RATE_LIMIT = 14400


# ── pure model ─────────────────────────────────────────────────────────────────────────────────────────────────
@dataclass
class Occ:
    occupant_key: str
    netuid: int
    reg_block: int
    mp: float                 # 0.0 when not started / NULL
    price: float | None
    status: str = "active"


@dataclass
class Model:
    head: int
    imm: int
    horizons: tuple[int, ...]
    reg_gap_days: float
    occ: dict[str, Occ]
    remaining_days: dict[str, float] = field(default_factory=dict)
    pool: list[str] = field(default_factory=list)              # P, ordered bottom → top
    pool_h: dict[int, list[str]] = field(default_factory=dict)  # P_H
    k_h: dict[int, int] = field(default_factory=dict)

    def rank(self, key: str, pool: list[str]) -> int | None:
        try:
            return pool.index(key) + 1
        except ValueError:
            return None

    def line(self, key: str, pool: list[str], k: int) -> float | None:
        others = [self.occ[o].mp for o in pool if o != key]
        return others[k - 1] if len(others) >= k else None

    def safety(self, key: str, h: int) -> float | None:
        if self.remaining_days[key] > h:
            return 1.0
        r, pool, k = self.rank(key, self.pool_h[h]), self.pool_h[h], self.k_h[h]
        if r is None:
            return None
        if len(pool) - k <= 0:
            return 0.0
        return min(1.0, max(0.0, (r - k) / (len(pool) - k)))


def k_for(h: int, reg_gap_days: float) -> int:
    return int(min(max(round(h / reg_gap_days), 1), h // 2))


def _order(occ: dict[str, Occ], keys: Sequence[str]) -> list[str]:
    return sorted(keys, key=lambda o: (occ[o].mp, occ[o].reg_block))


def build_model(occs: Sequence[Occ], *, head: int, imm: int, horizons: Sequence[int], reg_gap_days: float) -> Model:
    m = Model(head, imm, tuple(horizons), reg_gap_days, {o.occupant_key: o for o in occs})
    for o in occs:
        m.remaining_days[o.occupant_key] = max(0, o.reg_block + imm - head) * BLOCK_S / 86400
    m.pool = _order(m.occ, [k for k, d in m.remaining_days.items() if d <= 0])
    for h in horizons:
        m.pool_h[h] = _order(m.occ, [k for k, d in m.remaining_days.items() if d <= h])
        m.k_h[h] = k_for(h, reg_gap_days)
    return m


def reg_gap_mean(reg_times: Sequence[int], fallback_days: float) -> tuple[float, int]:
    ts = sorted(reg_times)
    if len(ts) < 2:
        return fallback_days, len(ts)
    gaps = [(b - a) / 86400 for a, b in zip(ts, ts[1:])]
    return sum(gaps) / len(gaps), len(ts)


def lock_cost_model(c: dict[str, Any], head: int) -> dict[str, Any]:
    """Current lock cost (τ) extrapolated from the probe's runtime-API value with the fitted ``I_eff`` (19 §2.4)."""
    out: dict[str, Any] = {"lock_cost_tau": None, "lock_cost_decay_tau_per_day": None, "i_eff": None}
    L, cost, b0 = c.get("NetworkLastLockCost"), c.get("NetworkRegistrationCost"), c.get("_block")
    lb, mn = c.get("NetworkLastRegisteredBlock"), c.get("NetworkMinLockCost") or 1e9
    if None in (L, cost, b0, lb) or L <= 0:
        return out
    if cost <= mn or 2 * L - cost <= 0 or b0 <= lb:
        out["lock_cost_tau"] = cost / 1e9
        return out
    i_eff = L * (b0 - lb) / (2 * L - cost)
    now_cost = max(mn, 2 * L - L * (head - lb) / i_eff)
    out.update(lock_cost_tau=now_cost / 1e9, i_eff=i_eff,
               lock_cost_decay_tau_per_day=(L * BLOCKS_PER_DAY / i_eff) / 1e9 if now_cost > mn else 0.0)
    return out


def bottom_k_hours(panel: dict[int, list[tuple[str, int, float]]], *, imm: int, k: int, block_at: dict[int, int],
                   keys: Sequence[str]) -> dict[str, tuple[int, int]]:
    """``panel[ts]`` = [(occupant_key, reg_block, moving_price)]; returns occ → (hours in bottom-k, rankable hours)."""
    want = set(keys)
    hit = {o: 0 for o in want}
    seen = {o: 0 for o in want}
    for ts, rows in panel.items():
        head = block_at.get(ts)
        if head is None:
            continue
        pool = sorted(((mp, reg, o) for o, reg, mp in rows if head - reg >= imm), key=lambda x: (x[0], x[1]))
        bottom = {o for _, _, o in pool[:k]}
        for o, reg, _ in rows:
            if o in want and head - reg >= imm:
                seen[o] += 1
                hit[o] += o in bottom
    return {o: (hit[o], seen[o]) for o in want}


# ── DB inputs ──────────────────────────────────────────────────────────────────────────────────────────────────
async def probe_constants(db: "Database", as_of: int) -> dict[str, Any]:
    rows = await db.fetchall(
        "SELECT name, value, block_number, observed_at FROM chain_constants c WHERE source = 'rpc_storage' AND "
        "observed_at = (SELECT MAX(observed_at) FROM chain_constants WHERE source = 'rpc_storage' AND name = c.name "
        "AND observed_at <= ?)", (as_of + 3600,), writer=True)
    out: dict[str, Any] = {}
    for r in rows:
        try:
            out[r["name"]] = float(r["value"])
        except ValueError:
            continue
        if r["name"] == "NetworkRegistrationCost":
            out["_block"], out["_observed_at"] = r["block_number"], r["observed_at"]
    return out


async def immunity_period(db: "Database", consts: dict[str, Any]) -> tuple[int, str]:
    v = consts.get("NetworkImmunityPeriod")
    if v:
        return int(v), "rpc_storage"
    return int(app_config.settings.NETWORK_IMMUNITY_PERIOD_BLOCKS), "config"


async def load_occupants(db: "Database", as_of: int) -> tuple[list[Occ], dict]:
    from app.features.chain_features import snapshot_rows
    snap = await snapshot_rows(db, as_of)
    status = {r["occupant_key"]: r["status"] for r in await db.fetchall(
        "SELECT occupant_key, status FROM occupants WHERE netuid != 0", writer=True)}
    occs: list[Occ] = []
    meta: dict[str, Any] = {"head": None, "snapshot": {}}
    head = 0
    for n, r in sorted(snap.items()):
        if n == 0 or r.occupant_key not in status or status[r.occupant_key] == "deregistered":
            continue
        reg = r.registered_at
        if reg is None:
            continue
        mp = r.f.get("subnet_moving_price")
        occs.append(Occ(r.occupant_key, n, int(reg), float(mp) if mp else 0.0, r.f.get("price"),
                        status[r.occupant_key]))
        meta["snapshot"][r.occupant_key] = r.meta()
        if r.block_number:
            head = max(head, int(r.block_number))
    meta["head"] = head or None
    return occs, meta


async def registrations(db: "Database", as_of: int, window_days: int) -> tuple[list[int], dict | None]:
    rows = await db.fetchall("SELECT registered_ts FROM occupants WHERE netuid != 0 AND registered_ts IS NOT NULL "
                             "AND registered_ts <= ? AND registered_ts > ?", (as_of, as_of - window_days * 86400),
                             writer=True)
    last = await db.fetchone("SELECT network_registered_at AS block, registered_ts AS ts FROM occupants WHERE "
                             "netuid != 0 AND registered_ts IS NOT NULL AND registered_ts <= ? ORDER BY registered_ts "
                             "DESC LIMIT 1", (as_of,), writer=True)
    return [r["registered_ts"] for r in rows], last


async def hourly_panel(db: "Database", netuids: Sequence[int], as_of: int, hours: int = 720
                       ) -> tuple[dict[int, list[tuple[str, int, float]]], dict[int, int]]:
    if not netuids:
        return {}, {}
    ph = ",".join("?" * len(netuids))
    rows = await db.fetchall(
        f"SELECT ts, occupant_key, moving_price, block_number FROM price_bars WHERE netuid IN ({ph}) AND period = '1h' "
        f"AND ts >= ? AND ts < ? AND occupant_key IS NOT NULL", (*netuids, as_of - hours * 3600, as_of), writer=True)
    panel: dict[int, list[tuple[str, int, float]]] = {}
    blocks: dict[int, int] = {}
    for r in rows:
        if r["moving_price"] is None:
            continue
        try:
            reg = int(r["occupant_key"].split(":")[1])
        except (IndexError, ValueError):
            continue
        panel.setdefault(r["ts"], []).append((r["occupant_key"], reg, float(r["moving_price"])))
        if r["block_number"]:
            blocks[r["ts"]] = max(blocks.get(r["ts"], 0), int(r["block_number"]))
    return panel, blocks


# ── rows ───────────────────────────────────────────────────────────────────────────────────────────────────────
def _row(occ: str, key: str, as_of: int, value: float | None, vj: dict, *, run_id: str, observed_at: int,
         event_at: int | None = None) -> dict:
    vj = {**vj, "def": DEF}
    if event_at is not None:
        vj["event_at"] = event_at
    if vj.get("status") != "ok":
        value = None
    return {"occupant_key": occ, "metric_key": key, "as_of": as_of, "spec_version": SPEC_VERSION, "stage": "raw",
            "value": value, "value_json": vj, "observed_at": observed_at, "source": "chain", "run_id": run_id,
            "backfilled": 0}


def metric_rows(m: Model, *, as_of: int, run_id: str, observed_at: int, eta_days: float | None,
                bottom: dict[str, tuple[int, int]], ctx: dict) -> list[dict]:
    out: list[dict] = []
    ok = lambda **x: {"status": "ok", **ctx, **x}                                   # noqa: E731
    na = lambda reason, **x: {"status": "not_applicable", "reason": reason, **ctx, **x}  # noqa: E731
    fail = lambda reason, **x: {"status": "failed", "reason": reason, **ctx, **x}  # noqa: E731
    spot_pool = sorted([k for k in m.pool if m.occ[k].price is not None],
                       key=lambda k: (m.occ[k].price, m.occ[k].reg_block))
    for key, o in m.occ.items():
        put = lambda mk, v, vj: out.append(_row(key, mk, as_of, v, vj, run_id=run_id, observed_at=observed_at))  # noqa: E731
        rem = m.remaining_days[key]
        grad = as_of + int(rem * 86400)
        put("immunity_remaining_days", rem, ok(imm=m.imm, reg_block=o.reg_block, head=m.head))
        r = m.rank(key, m.pool)
        put("dereg_rank", r, ok(pool_size=len(m.pool), mp=o.mp) if r else na("immune", graduates_at=grad))
        line1 = m.line(key, m.pool, 1)
        put("cull_line_1", line1, ok(pool_size=len(m.pool)) if line1 is not None else fail("empty_pool"))
        if line1 is None:
            put("mp_gap_to_cull_1", None, fail("empty_pool"))
        elif line1 <= 0:
            put("mp_gap_to_cull_1", None, na("cull_line_zero"))
        else:
            put("mp_gap_to_cull_1", o.mp / line1 - 1, ok(mp=o.mp, line=line1))
        for h in m.horizons:
            pool, k = m.pool_h[h], m.k_h[h]
            rh = m.rank(key, pool)
            put(f"dereg_rank_{h}d", rh,
                ok(pool_size=len(pool), k=k, mp=o.mp) if rh else na("immune", graduates_at=grad))
            lk = m.line(key, pool, k)
            put(f"cull_line_k_{h}d", lk, ok(k=k, pool_size=len(pool)) if lk is not None else fail("empty_pool", k=k))
            if lk is None:
                put(f"mp_gap_to_cull_k_{h}d", None, fail("empty_pool", k=k))
            elif lk <= 0:
                put(f"mp_gap_to_cull_k_{h}d", None, na("cull_line_zero", k=k))
            else:
                put(f"mp_gap_to_cull_k_{h}d", o.mp / lk - 1, ok(mp=o.mp, line=lk, k=k))
            sf = m.safety(key, h)
            put(f"dereg_safety_{h}d", sf, ok(rank=rh, k=k, pool_size=len(pool), remaining_days=rem))
        hb = bottom.get(key)
        k30 = m.k_h.get(30)
        if hb is None or k30 is None:
            put("hours_in_bottom_k_30d", None, fail("no_history"))
        elif hb[1] < 24:
            put("hours_in_bottom_k_30d", None, fail("insufficient_bars", rankable_hours=hb[1], k=k30))
        else:
            put("hours_in_bottom_k_30d", float(hb[0]), ok(rankable_hours=hb[1], k=k30, rule="today_imm_and_k"))
        sr = m.rank(key, spot_pool)
        if r is None:
            put("spot_rank", None, na("immune", graduates_at=grad))
        elif sr is None:
            put("spot_rank", None, fail("price_null"))
        else:
            put("spot_rank", sr, ok(pool_size=len(spot_pool), price=o.price))
        if o.mp > 0 and o.price:
            put("spot_vs_mp", o.price / o.mp - 1, ok(price=o.price, mp=o.mp))
        else:
            put("spot_vs_mp", None, na("moving_price_zero" if o.mp <= 0 else "price_null"))
        r30 = m.rank(key, m.pool_h[30]) if 30 in m.pool_h else None
        if r30 is None or eta_days is None:
            put("days_to_cull_static", None,
                na("immune", graduates_at=grad) if r30 is None else fail("no_registration_history"))
        else:
            put("days_to_cull_static", eta_days + (r30 - 1) * m.reg_gap_days,
                ok(rank_30d=r30, eta_next_registration_days=eta_days, reg_gap_mean_days=m.reg_gap_days))
    return out


# ── the slot ───────────────────────────────────────────────────────────────────────────────────────────────────
@dataclass
class DeregReport:
    as_of: int
    rows: int = 0
    occupants: int = 0
    pool: int = 0
    events: int = 0
    network: dict | None = None
    bottom: list = field(default_factory=list)


async def run_slot(db: "Database", as_of: int, *, run_id: str, observed_at: int) -> DeregReport:
    s = app_config.settings
    rep = DeregReport(as_of)
    occs, meta = await load_occupants(db, as_of)
    if not occs or not meta["head"]:
        raise RuntimeError(f"no chain snapshot for {to_iso(as_of)} (05 chain_hourly)")
    consts = await probe_constants(db, as_of)
    imm, imm_src = await immunity_period(db, consts)
    reg_ts, last = await registrations(db, as_of, s.WB_DEREG_REG_GAP_WINDOW_DAYS)
    gap, n_regs = reg_gap_mean(reg_ts, s.WB_DEREG_REG_GAP_FALLBACK_DAYS)
    head = int(meta["head"])
    m = build_model(occs, head=head, imm=imm, horizons=tuple(sorted(set(s.WB_DEREG_HORIZONS_DAYS) | {30})),
                    reg_gap_days=gap)
    m.horizons = tuple(sorted(s.WB_DEREG_HORIZONS_DAYS))
    # registration ETA (rate limit and the usual gap)
    # the probe's LastRateLimitedBlock sits a few blocks before the new occupant's NetworkRegisteredAt (queued
    # registration); prefer it only when it names a registration the registry has not stamped yet
    last_block, last_ts = (last or {}).get("block"), (last or {}).get("ts")
    probe_block = consts.get("NetworkLastRegisteredBlock")
    if probe_block and (last_block is None or probe_block > last_block + 100):
        last_block, last_ts = int(probe_block), as_of - (head - int(probe_block)) * BLOCK_S
    rl = int(consts.get("NetworkRateLimit") or DEFAULT_RATE_LIMIT)
    earliest = last_block + rl if last_block else None
    eta = None
    if last_ts is not None:
        eta_rl = max(0.0, (earliest - head) * BLOCK_S / 86400) if earliest else 0.0
        eta = max(eta_rl, (last_ts + gap * 86400 - as_of) / 86400, 0.0)
    panel, blocks = await hourly_panel(db, [o.netuid for o in occs], as_of)
    bottom = bottom_k_hours(panel, imm=imm, k=m.k_h.get(30, 1), block_at=blocks, keys=list(m.occ)) if 30 in m.k_h \
        else {}
    ctx = {"imm": imm, "imm_source": imm_src, "head": head}
    rows = metric_rows(m, as_of=as_of, run_id=run_id, observed_at=observed_at, eta_days=eta, bottom=bottom, ctx=ctx)
    for r in rows:
        snap = meta["snapshot"].get(r["occupant_key"])
        if snap:
            r["value_json"]["snapshot"] = snap
    rep.events = await emit_events(db, m, as_of=as_of, run_id=run_id, observed_at=observed_at)
    rep.rows = await put_metric_values(db, rows)
    lc = lock_cost_model(consts, head)
    net = {"hour_ts": as_of, "head_block": head, "immunity_period_blocks": imm, "immunity_source": imm_src,
           "pool_size": len(m.pool), "pool_size_30d": len(m.pool_h.get(30, [])) or None,
           "pool_size_90d": len(m.pool_h.get(90, [])) or None, "k_30": m.k_h.get(30), "k_90": m.k_h.get(90),
           "reg_gap_mean_days": round(gap, 4), "n_regs_window": n_regs, "last_registration_block": last_block,
           "last_registration_ts": last_ts, "earliest_next_registration_block": earliest,
           "eta_next_registration_days": None if eta is None else round(eta, 4),
           "lock_cost_tau": lc["lock_cost_tau"], "lock_cost_decay_tau_per_day": lc["lock_cost_decay_tau_per_day"],
           "lock_cost_observed_at": consts.get("_observed_at"),
           "graduates_30d": sum(1 for d in m.remaining_days.values() if 0 < d <= 30),
           "graduates_90d": sum(1 for d in m.remaining_days.values() if 0 < d <= 90),
           "cull_line_1": m.occ[m.pool[0]].mp if m.pool else None, "observed_at": observed_at, "run_id": run_id}
    await db.upsert("wb_dereg_network_hourly", [net], conflict=("hour_ts",))
    rep.network = net
    rep.occupants, rep.pool = len(m.occ), len(m.pool)
    rep.bottom = [(m.occ[k].netuid, m.occ[k].mp) for k in m.pool[:5]]
    return rep


async def emit_events(db: "Database", m: Model, *, as_of: int, run_id: str, observed_at: int) -> int:
    from app.storage.events import emit_event
    n = 0
    d7 = observed_at // 604800
    prev = {r["occupant_key"]: r for r in await db.fetchall(
        "SELECT occupant_key, value, value_json FROM metric_values WHERE metric_key = 'dereg_rank_30d' AND as_of = ? "
        "AND stage = 'raw'", (as_of - 3600,), writer=True)}
    pool30, k30 = m.pool_h.get(30, []), m.k_h.get(30)
    for key, o in m.occ.items():
        if k30 is not None and key in prev:
            pv = prev[key]
            pk = (json.loads(pv["value_json"]) if pv["value_json"] else {}).get("k", k30)
            was = pv["value"] is not None and pv["value"] <= pk
            r = m.rank(key, pool30)
            now_in = r is not None and r <= k30
            if was != now_in:
                lk = m.line(key, pool30, k30)
                gap = (o.mp / lk - 1) if lk else None
                eid = await emit_event(
                    db, event_type="dereg_rank_entered_k30" if now_in else "dereg_rank_cleared_k30", occupant_key=key,
                    netuid=o.netuid, event_at=as_of, observed_at=observed_at,
                    payload={"netuid": o.netuid, "rank": r, "k": k30, "pool_size": len(pool30),
                             "mp_gap_to_cull": gap}, dedupe_key=f"{key}|{d7}", source="chain", run_id=run_id)
                n += eid is not None
        rem = m.remaining_days[key]
        if 0 < rem <= 7:
            eid = await emit_event(
                db, event_type="immunity_ends_7d", occupant_key=key, netuid=o.netuid, event_at=as_of,
                observed_at=observed_at,
                payload={"netuid": o.netuid, "graduates_at": as_of + int(rem * 86400),
                         "dereg_rank_30d": m.rank(key, pool30) if pool30 else None},
                dedupe_key=key, source="chain", run_id=run_id)
            n += eid is not None
    return n


class DeregDistanceJob(CollectorJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        as_of = floor_hour(run.started_at)
        rep = await run_slot(run.rt.db, as_of, run_id=run.run_id, observed_at=run.started_at)
        net = rep.network or {}
        note = (f"as_of={to_iso(as_of)} occupants={rep.occupants} pool={rep.pool} imm={net.get('immunity_period_blocks')}"
                f"({net.get('immunity_source')}) k30={net.get('k_30')} gap={net.get('reg_gap_mean_days')}d "
                f"bottom={rep.bottom[:3]} events={rep.events}")
        logger.info("wb_dereg", data={"as_of": to_iso(as_of), "rows": rep.rows, "pool": rep.pool})
        return JobOutcome(rows=rep.rows, note=note, meta={"network": net, "events": rep.events})


# ── CLI ────────────────────────────────────────────────────────────────────────────────────────────────────────
async def _cli(cmd: str, as_of: str | None) -> int:
    from app.storage.db import Database
    from app.util.time import parse_ts
    s = app_config.settings
    db = Database(s.path(s.DB_PATH))
    await db.connect()
    await db.apply_schema()
    try:
        t = floor_hour(parse_ts(as_of + ":00:00Z" if as_of and len(as_of) == 13 else as_of) if as_of else now_ts())
        if cmd == "run":
            rep = await run_slot(db, t, run_id=f"cli:{now_ts()}", observed_at=now_ts())
            print(json.dumps({"rows": rep.rows, "pool": rep.pool, "events": rep.events, "network": rep.network,
                              "bottom": rep.bottom}, indent=1, default=str))
            return 0
        rows = await db.fetchall(
            "SELECT m.occupant_key, m.metric_key, m.value FROM metric_values m WHERE m.as_of = (SELECT MAX(as_of) "
            "FROM metric_values WHERE metric_key = 'dereg_rank_30d') AND m.metric_key IN ('dereg_rank_30d',"
            "'mp_gap_to_cull_k_30d','immunity_remaining_days') ORDER BY m.occupant_key")
        by: dict[str, dict] = {}
        for r in rows:
            by.setdefault(r["occupant_key"], {})[r["metric_key"]] = r["value"]
        for occ, d in sorted(by.items(), key=lambda x: (x[1].get("dereg_rank_30d") or math.inf)):
            if d.get("dereg_rank_30d") is not None and d["dereg_rank_30d"] <= 12:
                print(f"{occ:>14} rank30={d['dereg_rank_30d']:.0f} gap_k30={d.get('mp_gap_to_cull_k_30d')} "
                      f"immunity_days={d.get('immunity_remaining_days'):.1f}")
        return 0
    finally:
        await db.close()


if __name__ == "__main__":
    import argparse
    import asyncio
    import sys
    p = argparse.ArgumentParser(prog="python -m app.features.dereg_distance")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--as-of", default=None)
    sub.add_parser("show")
    a = p.parse_args()
    sys.exit(asyncio.run(_cli(a.cmd, getattr(a, "as_of", None))))
