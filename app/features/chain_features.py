"""P1 on-chain feature job (spec 05 §2.3–§2.7, §3.2–§3.4, §4): snapshot → v440 gate fit → gate tables → P1 raw
``metric_values`` (one ``put_metric_values`` call per slot) → events.

CLI: ``python -m app.features.chain_features run [--as-of YYYY-MM-DD[THH]]`` · ``matrix [--as-of …] [--json]``.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.features.gate_model import GateFit, fit_gate, gate_margin_raw
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.quant.dereg_risk import DeregRiskConfig, components, dereg_hazard_score
from app.storage.pit import put_metric_values
from app.util.time import floor_day, now_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

SPEC_VERSION = "ts-1.0"
SLOT_S = 6 * 3600
LIVE_SOURCES = ("sse", "rest_table", "tick_rollup")
ANALYTICS_SOURCES = ("analytics_1w", "analytics_1m")
P1_KEYS = ("emission_share", "gate_margin", "dereg_safety", "liquidity_depth", "exit_capacity", "net_flow_30d",
           "chain_buys", "holder_dispersion", "participation", "miner_burn_ok")
AUX_KEYS = ("dereg_hazard", "implied_miner_burn", "emission_enabled_flag", "dereg_risk_flag", "immune_flag",
            "occupant_age_days", "vtrust_dispersion", "owner_locked_share")


@dataclass
class SnapshotRow:
    netuid: int
    occupant_key: str
    hour_ts: int
    source: str
    event_at: int
    block_number: int | None
    f: dict = field(default_factory=dict)
    stale_h: int = 0

    @property
    def kind(self) -> str:
        if self.source in LIVE_SOURCES:
            return "live"
        return "historical" if self.source == "historical" else "analytics"

    @property
    def registered_at(self) -> int | None:
        v = self.f.get("network_registered_at")
        if v is None:
            try:
                return int(self.occupant_key.split(":")[1])
            except (IndexError, ValueError):
                return None
        return int(v)

    def meta(self) -> dict:
        d = {"hour_ts": self.hour_ts, "source": self.source, "event_at": self.event_at}
        if self.stale_h:
            d["stale_h"] = self.stale_h
        return d


@dataclass
class SlotReport:
    as_of: int
    snapshot_source: str
    n_rows: int
    n_failed: int
    fit: GateFit | None
    events: int
    n_eligible: int = 0
    degraded: bool = False


# ── snapshot (§2.3) ────────────────────────────────────────────────────────────────────────────────────────────
async def snapshot_rows(db: "Database", as_of: int) -> dict[int, SnapshotRow]:
    target = as_of - 3600
    rows = await db.fetchall("SELECT * FROM chain_hourly WHERE hour_ts >= ? AND hour_ts <= ? ORDER BY hour_ts DESC",
                             (as_of - 3 * 3600, target), writer=True)
    by: dict[int, list[dict]] = {}
    for r in rows:
        by.setdefault(r["netuid"], []).append(r)
    out: dict[int, SnapshotRow] = {}
    for n, rs in by.items():
        pick = next((r for r in rs if r["hour_ts"] == target and r["source"] in LIVE_SOURCES), None)
        stale = 0
        if pick is None:
            pick = next((r for r in rs if r["source"] in LIVE_SOURCES), None)
            if pick is not None:
                stale = (target - pick["hour_ts"]) // 3600
        if pick is None:
            pick = next((r for r in rs if r["hour_ts"] == target), None) or rs[0]
        f = {k: v for k, v in pick.items() if k not in ("netuid", "hour_ts", "occupant_key", "source", "event_at",
                                                        "observed_at", "run_id", "backfilled")}
        out[n] = SnapshotRow(n, pick["occupant_key"], pick["hour_ts"], pick["source"], pick["event_at"],
                             pick["block_number"], f, stale)
    if out:
        return out
    # tier 3: historical (price_bars 1h + chain_daily), only when chain_hourly has nothing at all
    bars = await db.fetchall("SELECT * FROM price_bars WHERE period = '1h' AND ts = ? AND occupant_key IS NOT NULL",
                             (target,), writer=True)
    for b in bars:
        d = await db.fetchone("SELECT * FROM chain_daily WHERE netuid = ? AND occupant_key = ? AND day_ts <= ? "
                              "ORDER BY day_ts DESC LIMIT 1", (b["netuid"], b["occupant_key"], as_of), writer=True)
        # emission: 13's view (chain_daily ∪ 17's daily RPC share — dead occupants, pre-2025-11-17 TMC truncation;
        # 20 N2 item 3), the latest row whose sample time is ≤ as_of
        em = await db.fetchval("SELECT emission_share FROM v_quant_chain_daily WHERE netuid = ? AND occupant_key = ? "
                               "AND ts <= ? ORDER BY day DESC LIMIT 1", (b["netuid"], b["occupant_key"], as_of),
                               writer=True)
        blk = b["block_number"]
        if blk is None:
            blk = await db.fetchval("SELECT block_number FROM block_times WHERE ts_ms <= ? ORDER BY ts_ms DESC "
                                    "LIMIT 1", (as_of * 1000,), writer=True) if await _has_table(db, "block_times") \
                else None
        mp = b["moving_price"]
        f = {"price": b["close"], "subnet_moving_price": mp,
             "tao_liquidity": b["tao_in_pool"] * 1e9 if b["tao_in_pool"] is not None else None,
             "emission": em, "excess_tao": d["chain_buys"] if d else None,
             "is_active": int(bool(mp and mp > 0)), "emission_enabled": None, "block_number": blk}
        out[b["netuid"]] = SnapshotRow(b["netuid"], b["occupant_key"], target, "historical", b["ts_last"] or target,
                                       blk, f)
    return out


async def _has_table(db: "Database", name: str) -> bool:
    return bool(await db.fetchval("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,), writer=True))


def immune_local(row: SnapshotRow, period: int) -> bool | None:
    reg = row.registered_at
    blk = row.block_number if row.block_number is not None else row.f.get("block_number")
    if reg is None or blk is None:
        return None
    return (int(blk) - int(reg)) < int(period)


def eligible_set(rows: dict[int, SnapshotRow]) -> dict[int, SnapshotRow]:
    out = {}
    for n, r in rows.items():
        mp = r.f.get("subnet_moving_price")
        enabled = r.f.get("emission_enabled")
        if r.kind == "historical":
            enabled = 1  # unknown → disabled subnets enter E as fully burned (§2.3 tier 3)
        if n != 0 and enabled == 1 and r.f.get("is_active") == 1 and mp is not None and mp > 0:
            out[n] = r
    return out


# ── dereg (§2.5) ───────────────────────────────────────────────────────────────────────────────────────────────
def dereg_pool(rows: dict[int, SnapshotRow], immune: dict[int, bool | None]) -> list[int]:
    pool = [n for n, r in rows.items() if n != 0 and immune.get(n) is False
            and r.f.get("subnet_moving_price") is not None]
    return sorted(pool, key=lambda n: (rows[n].f["subnet_moving_price"], rows[n].registered_at or 0))


def dereg_cfg(s) -> DeregRiskConfig:
    return DeregRiskConfig(starved_ratio=s.DEREG_STARVED_RATIO, emission_decline_sat=s.DEREG_EMISSION_DECLINE_PCT / 100,
                           price_decline_sat=s.DEREG_PRICE_DECLINE_PCT / 100,
                           depth_decline_sat=s.DEREG_DEPTH_DECLINE_PCT / 100, w_proximity=s.DEREG_W_PROXIMITY,
                           w_starved=s.DEREG_W_STARVED, w_decline=s.DEREG_W_DECLINE, high_risk_at=s.DEREG_HIGH_RISK_AT)


def dereg_metrics(rows: dict[int, SnapshotRow], hist: dict[str, dict[str, list[float]]], cfg: DeregRiskConfig,
                  period: int) -> dict[int, dict]:
    immune = {n: immune_local(r, period) for n, r in rows.items()}
    pool = dereg_pool(rows, immune)
    rank = {n: i + 1 for i, n in enumerate(pool)}
    weights = {n: rows[n].f["subnet_moving_price"] for n in pool}
    out: dict[int, dict] = {}
    for n, r in rows.items():
        if n == 0:
            continue
        d: dict[str, Any] = {"immune": immune[n], "rank": rank.get(n), "pool_size": len(pool)}
        disabled = r.f.get("emission_enabled") == 0
        if immune[n]:
            d["hazard"], d["hazard_status"] = (1.0 if disabled else 0.0), "ok"
            d["components"] = None
        elif n in weights and len(pool) >= 3:
            h = hist.get(r.occupant_key, {})
            kw = dict(emission_hist=h.get("emission"), price_hist=h.get("price"), depth_hist=h.get("depth"), cfg=cfg)
            d["hazard"] = dereg_hazard_score(weights, n, emission_disabled=disabled, **kw)
            d["components"] = components(weights, n, **kw)
            d["hazard_status"] = "ok"
        elif disabled:
            d["hazard"], d["hazard_status"], d["components"] = 1.0, "ok", None
        else:
            d["hazard"], d["hazard_status"], d["components"] = None, "failed", None
        out[n] = d
    return out


async def dereg_history(db: "Database", as_of: int, days: int) -> dict[str, dict[str, list[float]]]:
    lo = floor_day(as_of) - days * 86400
    hist: dict[str, dict[str, list[float]]] = {}
    for r in await db.fetchall("SELECT occupant_key, day_ts, emission_percent, subnet_tao FROM chain_daily WHERE "
                               "day_ts > ? AND day_ts <= ? ORDER BY day_ts", (lo, as_of), writer=True):
        h = hist.setdefault(r["occupant_key"], {"emission": [], "depth": [], "price": []})
        if r["emission_percent"] is not None:
            h["emission"].append(r["emission_percent"])
        if r["subnet_tao"] is not None:
            h["depth"].append(r["subnet_tao"])
    days_ts = [d - 3600 for d in range(lo + 86400, floor_day(as_of) + 1, 86400)]
    if days_ts:
        ph = ",".join("?" * len(days_ts))
        bars = {(r["occupant_key"], r["ts"]): r["close"] for r in await db.fetchall(
            f"SELECT occupant_key, ts, close FROM price_bars WHERE period = '1h' AND ts IN ({ph})", days_ts,
            writer=True)}
        hourly = {(r["occupant_key"], r["hour_ts"]): r["price"] for r in await db.fetchall(
            f"SELECT occupant_key, hour_ts, price FROM chain_hourly WHERE hour_ts IN ({ph}) AND price IS NOT NULL",
            days_ts, writer=True)}
        keys = {k for k, _ in bars} | {k for k, _ in hourly}
        for k in keys:
            if k is None:
                continue
            h = hist.setdefault(k, {"emission": [], "depth": [], "price": []})
            for t in days_ts:
                v = bars.get((k, t), hourly.get((k, t)))
                if v is not None:
                    h["price"].append(v)
    return hist


# ── net flow (§2.7) ────────────────────────────────────────────────────────────────────────────────────────────
async def net_flow(db: "Database", occupant_key: str, as_of: int, snap: SnapshotRow, *, observed_at: int,
                   unit: str = "rao") -> tuple[float | None, dict]:
    age_days = None
    reg, blk = snap.registered_at, snap.block_number
    if reg is not None and blk is not None:
        age_days = (blk - reg) * 12 / 86400
    w = 30 if age_days is None else max(1, min(30, math.floor(age_days)))
    rows = await db.fetchall("SELECT day_ts, total_tao_buy_24h, total_tao_sell_24h, subnet_tao FROM chain_daily "
                             "WHERE occupant_key = ? AND day_ts > ? AND day_ts <= ? AND observed_at <= ? "
                             "ORDER BY day_ts", (occupant_key, as_of - w * 86400, as_of, observed_at), writer=True)
    rows = [r for r in rows if r["total_tao_buy_24h"] is not None and r["total_tao_sell_24h"] is not None]
    need = 24 if w == 30 else max(7, math.ceil(0.8 * w))
    info: dict[str, Any] = {"window_days": w, "rows": len(rows), "required": need}
    if len(rows) < need:
        return None, {**info, "status": "failed", "reason": "insufficient_history"}
    depth = snap.f.get("tao_liquidity") or rows[-1]["subnet_tao"]
    if not depth or depth <= 0:
        return None, {**info, "status": "failed", "reason": "field_null"}
    k = 1e9 if unit == "tau" else 1.0
    flow = sum((r["total_tao_buy_24h"] - r["total_tao_sell_24h"]) * k for r in rows)
    return flow / depth, {**info, "status": "ok", "window_end_day": rows[-1]["day_ts"]}


# ── row building ───────────────────────────────────────────────────────────────────────────────────────────────
def _row(occ: str, key: str, as_of: int, value: float | None, vj: dict, *, run_id: str, observed_at: int,
         backfilled: int) -> dict:
    if vj.get("status") != "ok":
        value = None
    if backfilled:
        vj = {**vj, "backfilled": True}
    return {"occupant_key": occ, "metric_key": key, "as_of": as_of, "spec_version": SPEC_VERSION, "stage": "raw",
            "value": value, "value_json": vj, "observed_at": observed_at, "source": "chain", "run_id": run_id,
            "backfilled": backfilled}


def _ok(snap: SnapshotRow, src: str = "tmc", **extra) -> dict:
    return {"status": "ok", "src": src, "snapshot": snap.meta(), **extra}


def _fail(snap: SnapshotRow | None, reason: str, status: str = "failed", **extra) -> dict:
    d: dict[str, Any] = {"status": status, "reason": reason, "src": "tmc", **extra}
    if snap is not None:
        d["snapshot"] = snap.meta()
    return d


def p1_rows(rows: dict[int, SnapshotRow], fit: GateFit | None, fit_reason: str | None, degraded: bool,
            dereg: dict[int, dict], flows: dict[int, tuple[float | None, dict]], holders: dict[str, dict],
            meta_src: dict[str, dict], net: dict | None, *, as_of: int, run_id: str, observed_at: int,
            backfilled: int, period: int, missing: list[str] = ()) -> list[dict]:
    out: list[dict] = []

    def put(occ, key, value, vj):
        out.append(_row(occ, key, as_of, value, vj, run_id=run_id, observed_at=observed_at, backfilled=backfilled))

    for n, r in sorted(rows.items()):
        occ, f = r.occupant_key, r.f
        if n == 0:
            if net:
                put(occ, "sum_subnet_prices", net.get("sum_subnet_prices"),
                    _ok(r, n_active=net.get("n_active")) if net.get("sum_subnet_prices") is not None
                    else _fail(r, "field_null"))
                put(occ, "tao_usd", net.get("tao_usd"),
                    _ok(r, tao_usd_source=net.get("tao_usd_source")) if net.get("tao_usd") is not None
                    else _fail(r, "tao_usd_unavailable"))
            continue
        kind = r.kind
        started = f.get("is_active") == 1
        pre = not started
        il = immune_local(r, period)
        na = _fail(r, "pre_launch", "not_applicable")
        # emission_share
        if f.get("emission") is None:
            put(occ, "emission_share", None, na if pre else _fail(r, "field_null"))
        else:
            put(occ, "emission_share", f["emission"], na if pre else _ok(r))
        # gate_margin / miner_burn_ok / implied_miner_burn
        if pre:
            for k in ("gate_margin", "miner_burn_ok", "implied_miner_burn"):
                put(occ, k, None, na)
        elif kind == "analytics":
            for k in ("gate_margin", "miner_burn_ok", "implied_miner_burn"):
                put(occ, k, None, _fail(r, "snapshot_backfilled"))
        elif fit is None:
            for k in ("gate_margin", "miner_burn_ok", "implied_miner_burn"):
                put(occ, k, None, _fail(r, fit_reason or "theta_undefined"))
        elif n not in fit.adj:
            for k in ("gate_margin", "miner_burn_ok", "implied_miner_burn"):
                put(occ, k, None, _fail(r, "not_eligible", "not_applicable"))
        elif fit.theta <= 0:     # every moving price 0 (dTAO launch day, 17 replay): no margin is defined
            for k in ("gate_margin", "miner_burn_ok", "implied_miner_burn"):
                put(occ, k, None, _fail(r, "theta_undefined"))
        else:
            put(occ, "gate_margin", gate_margin_raw(fit, n), _ok(r, adj=fit.adj[n], theta=fit.theta))
            ib = fit.implied_burn.get(n)
            src = "rpc" if fit.burn_source == "rpc" else "tmc"
            if degraded and fit.burn_source != "rpc":
                vj = _fail(r, "fit_degraded")
                put(occ, "miner_burn_ok", None, vj)
                put(occ, "implied_miner_burn", None, vj)
            elif ib is None:
                vj = _fail(r, "burn_unidentifiable", pgs=fit.pgs.get(n))
                put(occ, "miner_burn_ok", None, vj)
                put(occ, "implied_miner_burn", None, vj)
            else:
                vj = _ok(r, src, pgs=fit.pgs[n])
                put(occ, "miner_burn_ok", 1.0 - ib, vj)
                put(occ, "implied_miner_burn", ib, vj)
        # dereg_safety / dereg_hazard / flags
        dg = dereg.get(n, {})
        if il is None:
            put(occ, "dereg_safety", None, _fail(r, "no_registration_block"))
        elif il:
            put(occ, "dereg_safety", 1.0, _ok(r, immune_local=True))
        elif kind == "analytics":
            put(occ, "dereg_safety", None, _fail(r, "snapshot_backfilled"))
        elif dg.get("rank") is None:
            put(occ, "dereg_safety", None, _fail(r, "moving_price_null"))
        else:
            rk, ps = dg["rank"], dg["pool_size"]
            val = 0.0 if rk <= 3 else (rk - 1) / (ps - 1)
            put(occ, "dereg_safety", val, _ok(r, rank_from_bottom=rk, pool_size=ps))
        if dg.get("hazard_status") == "ok":
            put(occ, "dereg_hazard", dg["hazard"], _ok(r, components=dg.get("components")))
        else:
            put(occ, "dereg_hazard", None, _fail(r, "hazard_unassessable"))
        if kind == "historical":
            put(occ, "emission_enabled_flag", None, _fail(r, "snapshot_historical"))
        elif f.get("emission_enabled") is None:
            put(occ, "emission_enabled_flag", None, _fail(r, "field_null"))
        else:
            put(occ, "emission_enabled_flag", float(f["emission_enabled"]), _ok(r))
        rk = dg.get("rank")
        badge = f.get("deregistration_risk") if kind == "live" else None
        if rk is None and badge is None and kind != "live":
            put(occ, "dereg_risk_flag", None, _fail(r, "snapshot_backfilled"))
        else:
            flag = 1.0 if (badge == 1 or (rk is not None and rk <= 3)) else 0.0
            put(occ, "dereg_risk_flag", flag, _ok(r, badge=badge, rank_from_bottom=rk))
        put(occ, "immune_flag", None if il is None else float(il),
            _ok(r) if il is not None else _fail(r, "no_registration_block"))
        reg, blk = r.registered_at, r.block_number
        put(occ, "occupant_age_days", (blk - reg) * 12 / 86400 if reg is not None and blk is not None else None,
            _ok(r) if reg is not None and blk is not None else _fail(r, "no_registration_block"))
        # liquidity_depth
        tl = f.get("tao_liquidity")
        if pre:
            put(occ, "liquidity_depth", None, na)
        elif tl is None:
            put(occ, "liquidity_depth", None, _fail(r, "field_null"))
        else:
            put(occ, "liquidity_depth", tl / 1e9, _ok(r))
        # exit_capacity
        price, pa = f.get("price"), f.get("price_after")
        if kind == "historical":
            put(occ, "exit_capacity", None, _fail(r, "snapshot_historical"))
        elif kind == "analytics":
            put(occ, "exit_capacity", None, _fail(r, "snapshot_backfilled"))
        elif pre or (price is not None and pa is not None and price == pa and not f.get("volume")):
            put(occ, "exit_capacity", None, na)
        elif price is None or price <= 0 or pa is None:
            put(occ, "exit_capacity", None, _fail(r, "price_after_unavailable"))
        else:
            put(occ, "exit_capacity", pa / price, _ok(r))
        # net_flow_30d
        if pre:
            put(occ, "net_flow_30d", None, na)
        else:
            v, info = flows.get(n, (None, _fail(r, "insufficient_history")))
            put(occ, "net_flow_30d", v, {**info, "src": "tmc", "snapshot": r.meta()})
        # chain_buys
        ex = f.get("excess_tao")
        mc = f.get("marketcap")
        if mc is None and price is not None and f.get("circulating_supply") is not None:
            mc = price * f["circulating_supply"] / 1e9
        if kind == "historical":
            put(occ, "chain_buys", None, _fail(r, "snapshot_historical"))
        elif pre:
            put(occ, "chain_buys", None, na)
        elif ex is None:
            put(occ, "chain_buys", None, _fail(r, "field_null"))
        elif not mc or mc <= 0:
            put(occ, "chain_buys", None, _fail(r, "marketcap_unavailable"))
        else:
            put(occ, "chain_buys", (ex / 1e9 * 7200) / mc, _ok(r, marketcap=mc))
        # holder_dispersion
        h = holders.get(occ)
        if kind == "historical":
            put(occ, "holder_dispersion", None, _fail(r, "snapshot_historical"))
        elif pre:
            put(occ, "holder_dispersion", None, na)
        elif h is None or h.get("top10_share") is None:
            put(occ, "holder_dispersion", None, _fail(r, "no_holders_snapshot"))
        else:
            put(occ, "holder_dispersion", 1.0 - h["top10_share"],
                _ok(r, snapshot_observed_at=h["observed_at"], n_fetched=h["n_fetched"]))
        # participation / vtrust_dispersion
        m = meta_src.get(occ)
        if kind == "historical":
            put(occ, "participation", None, _fail(r, "snapshot_historical"))
        elif pre:
            put(occ, "participation", None, na)
        elif m is None or not m.get("max_uids"):
            put(occ, "participation", None, _fail(r, "no_metagraph_source"))
        else:
            put(occ, "participation", (m["active_validators"] + m["active_miners"]) / m["max_uids"],
                _ok(r, m["src"]))
        if m is not None and m.get("vtrust_pstdev") is not None:
            put(occ, "vtrust_dispersion", m["vtrust_pstdev"], _ok(r, "rpc"))
        # owner_locked_share
        ol, ts_ = f.get("owner_personal_locked_alpha"), f.get("total_subnet_stake")
        if ol is None or not ts_:
            put(occ, "owner_locked_share", None, _fail(r, "field_null"))
        else:
            put(occ, "owner_locked_share", ol / ts_, _ok(r))
    for occ in missing:
        for k in P1_KEYS + ("emission_enabled_flag", "dereg_risk_flag", "immune_flag"):
            put(occ, k, None, _fail(None, "no_snapshot"))
    return out


# ── side inputs ────────────────────────────────────────────────────────────────────────────────────────────────
async def holders_latest(db: "Database", observed_at: int, max_age_days: int) -> dict[str, dict]:
    rows = await db.fetchall(
        "SELECT h.* FROM holders_snapshots h JOIN (SELECT occupant_key, MAX(observed_at) AS t FROM holders_snapshots "
        "WHERE observed_at <= ? AND observed_at >= ? GROUP BY occupant_key) m "
        "ON h.occupant_key = m.occupant_key AND h.observed_at = m.t",
        (observed_at, observed_at - max_age_days * 86400), writer=True)
    return {r["occupant_key"]: r for r in rows}


async def metagraph_sources(db: "Database", as_of: int) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for r in await db.fetchall("SELECT * FROM rpc_metagraph_daily WHERE as_of = ?", (as_of,), writer=True):
        mx = r["max_uids"] or r["n_uids"]
        if mx and r["active_validators"] is not None and r["active_miners"] is not None:
            out[r["occupant_key"]] = {"active_validators": r["active_validators"], "active_miners": r["active_miners"],
                                      "max_uids": mx, "vtrust_pstdev": r["vtrust_pstdev"], "src": "rpc"}
    return out  # the /subnets/ snapshot carries no active validator/miner counts (verified 2026-09-24)


async def network_row(db: "Database", as_of: int) -> dict | None:
    return await db.fetchone("SELECT * FROM chain_network_hourly WHERE hour_ts <= ? AND hour_ts >= ? "
                             "ORDER BY hour_ts DESC LIMIT 1", (as_of - 3600, as_of - 3 * 3600), writer=True)


async def rpc_burns(db: "Database", as_of: int) -> dict[int, float]:
    return {r["netuid"]: r["miner_burned"] for r in await db.fetchall(
        "SELECT netuid, miner_burned FROM rpc_subnet_daily WHERE as_of = ? AND miner_burned IS NOT NULL", (as_of,),
        writer=True)}


# ── events (§3.4) ──────────────────────────────────────────────────────────────────────────────────────────────
async def _prev_metric(db: "Database", occ: str, key: str, before: int, *, official: bool) -> dict | None:
    return await db.fetchone(
        "SELECT as_of, value FROM metric_values WHERE occupant_key = ? AND metric_key = ? AND stage = 'raw' "
        "AND spec_version = ? AND as_of < ? AND status = 'ok' AND (? = 0 OR as_of % 86400 = 0) "
        "ORDER BY as_of DESC LIMIT 1", (occ, key, SPEC_VERSION, before, 1 if official else 0), writer=True)


async def emit_transitions(db: "Database", rows: dict[int, SnapshotRow], dereg: dict[int, dict], *, as_of: int,
                           run_id: str, observed_at: int, backfilled: int, period: int, high_risk_at: float) -> int:
    from app.storage.events import emit_event
    n_ev = 0
    d7 = observed_at // 604800
    official = as_of % 86400 == 0

    async def emit(et, occ, netuid, payload, dedupe, event_at):
        nonlocal n_ev
        if backfilled:
            payload = {**payload, "backfilled": True}
        eid = await emit_event(db, event_type=et, occupant_key=occ, netuid=netuid, event_at=event_at,
                               observed_at=observed_at, payload=payload, dedupe_key=dedupe, source="chain",
                               run_id=run_id, alerted_at=observed_at if backfilled else None)
        n_ev += eid is not None

    for n, r in sorted(rows.items()):
        if n == 0:
            continue
        occ, f = r.occupant_key, r.f
        dg = dereg.get(n, {})
        il = immune_local(r, period)
        if official and r.kind == "live":
            rk = dg.get("rank")
            flag = 1.0 if (f.get("deregistration_risk") == 1 or (rk is not None and rk <= 3)) else 0.0
            prev = await _prev_metric(db, occ, "dereg_risk_flag", as_of, official=True)
            if prev is not None and prev["as_of"] == as_of - 86400 and prev["value"] != flag:
                et = "dereg_risk_entered" if flag else "dereg_risk_cleared"
                await emit(et, occ, n, {"netuid": n, "moving_price": f.get("subnet_moving_price"),
                                        "rank_from_bottom": rk, "pool_size": dg.get("pool_size")},
                           f"{occ}|{d7}", r.event_at)
        if dg.get("hazard") is not None and dg["hazard"] >= high_risk_at and not il:
            await emit("dereg_proximity", occ, n, {"netuid": n, "hazard": dg["hazard"],
                                                   "components": dg.get("components")}, f"{occ}|{d7}", r.event_at)
        if f.get("is_active") == 1 and r.kind == "live" and f.get("immune") is not None and il is not None \
                and bool(f["immune"]) != il:
            await emit("immunity_mismatch", occ, n, {"netuid": n, "tmc": f["immune"], "local": int(il),
                                                     "age_blocks": (r.block_number or 0) - (r.registered_at or 0)},
                       f"{occ}|{r.event_at // 86400}", r.event_at)
        # emission flip that the recorder missed (no SSE event since the previous slot)
        en = f.get("emission_enabled")
        if r.kind == "live" and en is not None and f.get("is_active") == 1:
            prev = await _prev_metric(db, occ, "emission_enabled_flag", as_of, official=False)
            if prev is not None and prev["value"] != float(en):
                seen = await db.fetchval(
                    "SELECT 1 FROM events WHERE occupant_key = ? AND event_type IN ('emission_disabled',"
                    "'emission_reenabled') AND event_at >= ? LIMIT 1", (occ, prev["as_of"]), writer=True)
                if not seen:
                    et = "emission_reenabled" if en else "emission_disabled"
                    await emit(et, occ, n, {"netuid": n, "block_number": r.block_number, "prev": int(prev["value"]),
                                            "now": int(en), "detected_by": "chain_features"},
                               f"{occ}|{r.block_number}", r.event_at)
    return n_ev


# ── the slot ───────────────────────────────────────────────────────────────────────────────────────────────────
async def run_slot(db: "Database", as_of: int, *, run_id: str, observed_at: int, backfilled: int = 0) -> SlotReport:
    s = app_config.settings
    period = s.NETWORK_IMMUNITY_PERIOD_BLOCKS
    rows = await snapshot_rows(db, as_of)
    if not rows:
        return SlotReport(as_of, "none", 0, 0, None, 0)
    src_counts: dict[str, int] = {}
    for r in rows.values():
        src_counts[r.source] = src_counts.get(r.source, 0) + 1
    snapshot_source = max(src_counts, key=src_counts.get)

    # gate fit on the live/historical eligible set
    E = {n: r for n, r in eligible_set(rows).items() if r.kind != "analytics"}
    fit: GateFit | None = None
    plain: GateFit | None = None
    fit_reason = None
    degraded = False
    rpc = await rpc_burns(db, as_of)
    if len(E) >= 2:
        mp = {n: float(r.f["subnet_moving_price"]) for n, r in E.items()}
        em = {n: max(0.0, float(r.f.get("emission") or 0.0)) / 100 for n, r in E.items()}
        tot = sum(em.values())
        realised = {n: (v / tot if tot > 0 else 0.0) for n, v in em.items()}
        kw = dict(bar_rank=s.GATE_BAR_RANK, h=s.GATE_EXPONENT, max_iter=s.GATE_FIT_MAX_ITER, tol=s.GATE_FIT_TOL,
                  min_pred_share=s.GATE_MIN_PRED_SHARE)
        plain = fit_gate(mp, realised, **kw)
        fit = fit_gate(mp, realised, burn_override=rpc, **kw) if rpc else plain
        if fit is not plain:
            fit = replace(fit, implied_burn={n: rpc.get(n) for n in fit.adj})
        degraded = plain.residual_pp > s.GATE_FIT_TOLERANCE_PP
        blk = max((r.block_number or 0) for r in E.values()) or None
        await db.execute(
            "INSERT OR IGNORE INTO gate_fits (as_of, model_version, block_number, n_eligible, bar_rank, exponent, theta, "
            "residual_pp, iterations, converged, degraded, burn_source, observed_at, run_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (as_of, s.GATE_MODEL_VERSION, blk, len(E), s.GATE_BAR_RANK, s.GATE_EXPONENT, fit.theta, plain.residual_pp,
             plain.iterations, int(plain.converged), int(degraded), fit.burn_source, observed_at, run_id))
        tot_mp = sum(mp.values())
        gsd = []
        for n, r in sorted(rows.items()):
            e = n in fit.adj
            gsd.append((as_of, s.GATE_MODEL_VERSION, n, r.occupant_key, int(e),
                        mp[n] / tot_mp if e else None,
                        fit.adj.get(n), fit.gate.get(n), fit.pred.get(n), realised.get(n), fit.pgs.get(n),
                        fit.implied_burn.get(n) if e else None, (1 - plain.b[n]) if e else None, rpc.get(n),
                        observed_at, run_id))
        await db.executemany(
            "INSERT OR IGNORE INTO gate_subnet_daily (as_of, model_version, netuid, occupant_key, eligible, demand_share,"
            " burn_adjusted_share, gate, predicted_share, realised_share, predicted_gated_share, implied_miner_burn, "
            "fitted_burn, rpc_miner_burned, observed_at, run_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", gsd)
    else:
        fit_reason = "theta_undefined"

    cfg = dereg_cfg(s)
    hist = await dereg_history(db, as_of, s.DEREG_HIST_DAYS)
    dereg = dereg_metrics(rows, hist, cfg, period)
    flows = {}
    for n, r in rows.items():
        if n != 0 and r.f.get("is_active") == 1:
            flows[n] = await net_flow(db, r.occupant_key, as_of, r, observed_at=observed_at)
    holders = await holders_latest(db, observed_at, s.TMC_HOLDERS_MAX_AGE_DAYS)
    meta_src = await metagraph_sources(db, as_of)
    net = await network_row(db, as_of)
    if 0 not in rows and net is not None:
        rows[0] = SnapshotRow(0, "0:0", as_of - 3600, net["source"], net["event_at"] or as_of, net["block_number"],
                              {"is_active": 1})
    present = {r.occupant_key for r in rows.values()}
    missing = [o["occupant_key"] for o in await db.fetchall(
        "SELECT occupant_key FROM occupants WHERE netuid != 0 AND status IN ('active','pre_launch') AND "
        "first_seen_at <= ?", (as_of,), writer=True) if o["occupant_key"] not in present]
    metric_rows = p1_rows(rows, fit, fit_reason, degraded, dereg, flows, holders, meta_src,
                          dict(net) if net else None, as_of=as_of, run_id=run_id, observed_at=observed_at,
                          backfilled=backfilled, period=period, missing=missing)
    await put_metric_values(db, metric_rows)
    n_failed = sum(1 for m in metric_rows if m["value_json"]["status"] == "failed")

    n_ev = await emit_transitions(db, rows, dereg, as_of=as_of, run_id=run_id, observed_at=observed_at,
                                  backfilled=backfilled, period=period, high_risk_at=s.DEREG_HIGH_RISK_AT)
    if plain is not None and degraded:
        from app.storage.events import emit_event
        consec = await _degraded_streak(db, as_of, s.GATE_MODEL_VERSION)
        pay = {"as_of": as_of, "residual_pp": plain.residual_pp, "theta": plain.theta, "n_eligible": len(E),
               "consecutive_days": consec, **({"backfilled": True} if backfilled else {})}
        await emit_event(db, event_type="gate_fit_degraded", occupant_key=None, netuid=None, event_at=as_of,
                         observed_at=observed_at, payload=pay, dedupe_key=str(as_of), source="chain", run_id=run_id,
                         alerted_at=observed_at if backfilled else None)
        n_ev += 1
        if as_of % 86400 == 0 and consec >= s.GATE_BROKEN_AFTER_DAYS:
            await emit_event(db, event_type="gate_model_broken", occupant_key=None, netuid=None, event_at=as_of,
                             observed_at=observed_at, payload={"as_of": as_of, "consecutive_days": consec,
                                                               "residual_pp": plain.residual_pp,
                                                               **({"backfilled": True} if backfilled else {})},
                             dedupe_key=f"gate_model_broken|{observed_at // 604800}", source="chain", run_id=run_id,
                             alerted_at=observed_at if backfilled else None)   # a replay never pages (17 §2.11)
            n_ev += 1
    if as_of % 86400 == 0 and not backfilled:
        await db.executemany(
            "INSERT OR IGNORE INTO chain_constants (name, source, observed_at, value, block_number, endpoint, event_at, "
            "run_id) VALUES (?, 'config', ?, ?, NULL, NULL, NULL, ?)",
            [(k, observed_at, str(getattr(s, k)), run_id) for k in ("NETWORK_IMMUNITY_PERIOD_BLOCKS", "GATE_BAR_RANK",
                                                                     "GATE_EXPONENT", "GATE_MODEL_VERSION")])
    if plain is not None:
        logger.info(f"gate fit: n_eligible={len(E)} theta={plain.theta:.5f} residual_pp={plain.residual_pp:.2f} "
                    f"converged={int(plain.converged)}", data={"as_of": to_iso(as_of), "iterations": plain.iterations})
    return SlotReport(as_of, snapshot_source, len(metric_rows), n_failed, fit, n_ev, len(E), degraded)


async def _degraded_streak(db: "Database", as_of: int, version: str) -> int:
    rows = await db.fetchall("SELECT as_of, degraded FROM gate_fits WHERE model_version = ? AND as_of <= ? AND "
                             "as_of % 86400 = 0 ORDER BY as_of DESC LIMIT 60", (version, as_of), writer=True)
    n, expect = 0, floor_day(as_of)
    for r in rows:
        if r["as_of"] != expect or not r["degraded"]:
            break
        n += 1
        expect -= 86400
    return n


def latest_slot(now: int) -> int:
    return now - now % SLOT_S


class ChainFeaturesJob(CollectorJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        db = run.rt.db
        now = run.started_at
        slot = latest_slot(now)
        caught = 0
        for d in range(floor_day(slot) - run.settings.CHAIN_FEATURES_CATCHUP_DAYS * 86400, floor_day(slot), 86400):
            have = await db.fetchval("SELECT 1 FROM metric_values WHERE metric_key = 'emission_share' AND as_of = ? "
                                     "AND stage = 'raw' LIMIT 1", (d,), writer=True)
            if have:
                continue
            rep = await run_slot(db, d, run_id=run.run_id, observed_at=now, backfilled=1)
            caught += rep.n_rows > 0
        rep = await run_slot(db, slot, run_id=run.run_id, observed_at=now, backfilled=0)
        if rep.n_rows == 0:
            raise RuntimeError(f"no snapshot for {to_iso(slot)} (SSE and fallback both down, no repair yet)")
        fit = rep.fit
        note = (f"as_of={to_iso(slot)} rows={rep.n_rows} failed={rep.n_failed} snapshot={rep.snapshot_source} "
                f"eligible={rep.n_eligible}" + (f" theta={fit.theta:.5f} resid={fit.residual_pp:.2f}" if fit else "")
                + (" DEGRADED" if rep.degraded else "") + (f" catchup_days={caught}" if caught else ""))
        return JobOutcome(rows=rep.n_rows, note=note, meta={"events": rep.events})


# ── CLI ────────────────────────────────────────────────────────────────────────────────────────────────────────
def _parse_as_of(v: str | None) -> int:
    from app.util.time import parse_ts
    if not v or v == "today":
        return floor_day(now_ts())
    if v == "latest":
        return latest_slot(now_ts())
    if len(v) == 10:
        v += "T00"
    if len(v) == 13:
        v += ":00:00Z"
    return parse_ts(v)


async def _cli(cmd: str, as_of: str | None, as_json: bool) -> int:
    from app.scoring.p1_matrix import p1_matrix
    from app.storage.db import Database
    s = app_config.settings
    db = Database(s.path(s.DB_PATH))
    await db.connect()
    await db.apply_schema()
    try:
        t = _parse_as_of(as_of)
        if cmd == "run":
            now = now_ts()
            rep = await run_slot(db, t, run_id=f"cli:{now}", observed_at=now,
                                 backfilled=1 if now - t > 86400 + SLOT_S else 0)
            f = rep.fit
            print(f"as_of={to_iso(t)} snapshot={rep.snapshot_source} rows={rep.n_rows} failed={rep.n_failed} "
                  f"events={rep.events}")
            if f:
                print(f"gate fit: n_eligible={rep.n_eligible} theta={f.theta:.5f} residual_pp={f.residual_pp:.2f} "
                      f"iterations={f.iterations} converged={int(f.converged)} degraded={int(rep.degraded)}")
            return 0 if rep.n_rows else 1
        if as_of is None:
            t = await db.fetchval("SELECT MAX(as_of) FROM metric_values WHERE source = 'chain' AND stage = 'raw' "
                                  "AND metric_key = 'emission_share'") or t
        rows = await p1_matrix(db, t)
        if as_json:
            print(json.dumps(rows, indent=1, default=str))
            return 0
        from app.scoring.p1_matrix import render
        print(render(rows, t))
        return 0
    finally:
        await db.close()


def main(argv: list[str] | None = None) -> int:
    import argparse
    import asyncio
    p = argparse.ArgumentParser(prog="python -m app.features.chain_features")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--as-of", default=None)
    m = sub.add_parser("matrix")
    m.add_argument("--as-of", default=None)
    m.add_argument("--json", action="store_true")
    a = p.parse_args(argv)
    return asyncio.run(_cli(a.cmd, a.as_of, getattr(a, "json", False)))


if __name__ == "__main__":
    import sys
    sys.exit(main())
