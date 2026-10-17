"""Workbench read model (spec 19 §2.6): the screener rows, the per-subnet chart payload and the factor list.

Reads only; every value comes from stored rows (``metric_values``, ``chain_hourly``, ``price_bars``,
``wb_dereg_network_hourly``). The chart overlay uses ``technicals.overlay`` so the plot and the stored rows share math.
"""
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.features.technicals import PERIOD_S, overlay
from app.util.time import floor_day, now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

SPEC_VERSION = "ts-1.0"
TECH_1H = ("ema72_1h", "ema72_dist_1h", "ema72_band_1h", "ema72_band_pos_1h", "px_vs_moving")
TECH_1D = ("ema72_1d", "ema72_dist_1d", "ema72_band_1d", "ema72_band_pos_1d", "ema72_band_rank_1d", "rv_30d",
           "dd_90d", "ema72_cross_days_1d")
DEREG = ("immunity_remaining_days", "dereg_rank", "dereg_rank_30d", "dereg_rank_90d", "cull_line_1",
         "cull_line_k_30d", "cull_line_k_90d", "mp_gap_to_cull_1", "mp_gap_to_cull_k_30d", "mp_gap_to_cull_k_90d",
         "dereg_safety_30d", "dereg_safety_90d", "hours_in_bottom_k_30d", "spot_rank", "spot_vs_mp",
         "days_to_cull_static")
# keys owned by other specs, shown when their producers have written them (latest row ≤ 3 days old)
OTHER = ("repo_pushed_days", "owner_locked_share", "discord_members", "presence_ratio", "x_last_activity_days",
         "tg_last_activity_days", "dereg_safety", "dereg_hazard")
LIVE_SOURCES = ("sse", "rest_table", "tick_rollup")
STALE_AFTER_S = 2 * 3600 + 900

# Slider columns: dir = +1 when a higher value is "better" by the usual reading, −1 when lower is better, 0 = neutral.
FACTORS: list[dict[str, Any]] = [
    {"key": "ema72_band_pos_1d", "label": "Band pos 1d", "dir": -1, "group": "technicals",
     "desc": "ln(close/EMA72)/(2σ) on daily bars; −1/+1 = band edge. Low = stretched below (mean-reversion read)."},
    {"key": "ema72_band_pos_1h", "label": "Band pos 1h", "dir": -1, "group": "technicals",
     "desc": "Same on hourly bars (the old bot's EMA72@1h trend line)."},
    {"key": "ema72_dist_1d", "label": "Dist EMA72 1d", "dir": -1, "group": "technicals",
     "desc": "close/EMA72 − 1 on daily bars."},
    {"key": "px_vs_moving", "label": "Px vs moving", "dir": 1, "group": "technicals",
     "desc": "ln(spot / chain moving price): short-horizon momentum (old factor t +4.1)."},
    {"key": "dd_90d", "label": "Drawdown 90d", "dir": 1, "group": "technicals",
     "desc": "close / 90-day high − 1."},
    {"key": "rv_30d", "label": "Vol 30d", "dir": -1, "group": "technicals", "desc": "Annualised realised vol."},
    {"key": "mp_gap_to_cull_k_30d", "label": "Gap to cull (30d)", "dir": 1, "group": "dereg",
     "desc": "Moving price / k₃₀-th lowest moving price in the 30-day pool − 1. Negative = inside the cull zone."},
    {"key": "dereg_safety_30d", "label": "Dereg safety 30d", "dir": 1, "group": "dereg",
     "desc": "1 = immune past 30 d; 0 = within the expected culls of the next 30 d."},
    {"key": "hours_in_bottom_k_30d", "label": "Hours bottom-k", "dir": -1, "group": "dereg",
     "desc": "Hours of the last 720 in the bottom k₃₀ under today's rules."},
    {"key": "liquidity", "label": "Liquidity τ", "dir": 1, "group": "market", "desc": "TAO in the pool."},
    {"key": "emission_pct", "label": "Emission %", "dir": 1, "group": "market", "desc": "Share of TAO emission."},
    {"key": "dev_activity_wb", "label": "Dev activity", "dir": 1, "group": "activity",
     "desc": "Mean of logcap(commits_30d, 60) and recency(days since push, 14, 120). Computed in the browser."},
    {"key": "social_activity_wb", "label": "Social activity", "dir": 1, "group": "activity",
     "desc": "08/09 legs (Discord size/presence, X/TG recency) — empty until those collectors run."},
]


def _vj(r: dict) -> dict:
    try:
        return json.loads(r["value_json"]) if r.get("value_json") else {}
    except ValueError:
        return {}


async def _latest_as_of(db: "Database", key: str, *, not_after: int) -> int | None:
    return await db.fetchval("SELECT MAX(as_of) FROM metric_values WHERE metric_key = ? AND stage = 'raw' AND "
                             "spec_version = ? AND as_of <= ?", (key, SPEC_VERSION, not_after))


async def _slot_values(db: "Database", keys: tuple[str, ...], as_of: int | None) -> list[dict]:
    if as_of is None:
        return []
    ph = ",".join("?" * len(keys))
    return await db.fetchall(
        f"SELECT occupant_key, metric_key, as_of, value, value_json, status FROM metric_values WHERE metric_key IN "
        f"({ph}) AND stage = 'raw' AND spec_version = ? AND as_of = ?", (*keys, SPEC_VERSION, as_of))


async def _recent_values(db: "Database", keys: tuple[str, ...], since: int) -> list[dict]:
    ph = ",".join("?" * len(keys))
    rows = await db.fetchall(
        f"SELECT occupant_key, metric_key, as_of, value, value_json, status FROM metric_values WHERE metric_key IN "
        f"({ph}) AND stage = 'raw' AND as_of >= ? ORDER BY as_of", (*keys, since))
    latest: dict[tuple[str, str], dict] = {}
    for r in rows:
        latest[(r["occupant_key"], r["metric_key"])] = r       # ascending → the newest wins
    return list(latest.values())


async def latest_chain(db: "Database") -> dict[int, dict]:
    top = await db.fetchval("SELECT MAX(hour_ts) FROM chain_hourly WHERE source IN ('sse','rest_table','tick_rollup')")
    if top is None:
        return {}
    rows = await db.fetchall(
        "SELECT * FROM chain_hourly WHERE hour_ts >= ? AND source IN ('sse','rest_table','tick_rollup') "
        "ORDER BY hour_ts", (top - 3 * 3600,))
    return {r["netuid"]: r for r in rows}


async def price_changes(db: "Database", occs: dict[str, int], t_last: int) -> dict[str, dict]:
    """chg_24h/7d from complete 1 h closes of the current occupant."""
    out: dict[str, dict] = {}
    if not occs:
        return out
    pts = (t_last, t_last - 86400, t_last - 7 * 86400)
    netuids = sorted(set(occs.values()))
    ph = ",".join("?" * len(netuids))
    rows = await db.fetchall(
        f"SELECT occupant_key, ts, close FROM price_bars WHERE netuid IN ({ph}) AND period = '1h' AND ts IN (?,?,?)",
        (*netuids, *pts))
    by: dict[str, dict[int, float]] = {}
    for r in rows:
        by.setdefault(r["occupant_key"], {})[r["ts"]] = r["close"]
    for occ, d in by.items():
        c0 = d.get(pts[0])
        out[occ] = {"chg_24h": (c0 / d[pts[1]] - 1) if c0 and d.get(pts[1]) else None,
                    "chg_7d": (c0 / d[pts[2]] - 1) if c0 and d.get(pts[2]) else None}
    return out


async def screener(db: "Database", now: int | None = None) -> dict:
    now = now or now_ts()
    occs = await db.fetchall(
        "SELECT o.occupant_key, o.netuid, o.status, COALESCE(s.name, o.name) AS name, o.symbol, s.github_url, "
        "s.website_url FROM occupants o LEFT JOIN subnets s ON s.occupant_key = o.occupant_key WHERE o.netuid != 0 "
        "AND o.status IN ('active','pre_launch') ORDER BY o.netuid")
    rows: dict[str, dict] = {}
    for o in occs:
        rows[o["occupant_key"]] = {"netuid": o["netuid"], "occupant_key": o["occupant_key"], "name": o["name"],
                                   "symbol": o["symbol"], "status": o["status"], "github_url": o["github_url"],
                                   "website_url": o["website_url"], "cov": {}}
    chain = await latest_chain(db)
    for r in rows.values():
        c = chain.get(r["netuid"])
        if c is None or c["occupant_key"] != r["occupant_key"]:
            continue
        r.update(price=c["price"], moving_price=c["subnet_moving_price"], mcap_tau=c["marketcap"],
                 liquidity=(c["tao_liquidity"] / 1e9) if c["tao_liquidity"] is not None else None,
                 emission_pct=c["emission"], commits_7d=c["commits_7d"], commits_30d=c["commits_30d"],
                 commits_90d=c["commits_90d"], tmc_dereg_badge=c["deregistration_risk"], chain_hour=c["hour_ts"])
    t_last = (now // 3600) * 3600 - 3600
    for occ, ch in (await price_changes(db, {k: v["netuid"] for k, v in rows.items()}, t_last)).items():
        if occ in rows:
            rows[occ].update(ch)
    as_ofs: dict[str, int | None] = {}
    vals: list[dict] = []
    for grp, keys, rep in (("1h", TECH_1H, "ema72_1h"), ("1d", TECH_1D, "ema72_1d"), ("dereg", DEREG, "dereg_rank")):
        a = await _latest_as_of(db, rep, not_after=now)
        as_ofs[grp] = a
        vals += await _slot_values(db, keys, a)
    vals += await _recent_values(db, OTHER, floor_day(now) - 3 * 86400)
    for v in vals:
        r = rows.get(v["occupant_key"])
        if r is None:
            continue
        vj = _vj(v)
        r[v["metric_key"]] = v["value"]
        cov = {"status": v["status"], "as_of": v["as_of"]}
        if vj.get("reason"):
            cov["reason"] = vj["reason"]
        if vj.get("graduates_at"):
            cov["graduates_at"] = vj["graduates_at"]
        r["cov"][v["metric_key"]] = cov
    net = await db.fetchone("SELECT * FROM wb_dereg_network_hourly ORDER BY hour_ts DESC LIMIT 1")
    fresh = [a for g, a in as_ofs.items() if g != "1d" and a is not None]
    stale = not fresh or now - min(fresh) > STALE_AFTER_S
    return {"as_of": as_ofs, "now": now, "stale": stale, "network": net, "rows": list(rows.values()),
            "factors": FACTORS}


async def chart(db: "Database", netuid: int, period: str = "1h", bars: int = 500, now: int | None = None) -> dict | None:
    s = app_config.settings
    now = now or now_ts()
    sub = await db.fetchone("SELECT s.occupant_key, COALESCE(s.name, o.name) AS name, o.status, "
                            "o.network_registered_at FROM subnets s JOIN occupants o USING (occupant_key) "
                            "WHERE s.netuid = ?", (netuid,))
    if sub is None:
        return None
    ps = PERIOD_S.get(period, 300 if period == "5m" else None)
    if ps is None:
        raise ValueError("period must be 5m, 1h or 1d")
    bars = max(10, min(int(bars), 5000))
    raw = await db.fetchall(
        "SELECT ts, open, high, low, close, moving_price FROM price_bars WHERE occupant_key = ? AND period = ? AND "
        "ts + ? <= ? ORDER BY ts DESC LIMIT ?", (sub["occupant_key"], period, ps, now, bars + s.WB_EMA_WINDOW_BARS))
    raw.reverse()
    ov = overlay([(r["ts"], r["close"]) for r in raw], ema_period=s.WB_EMA_PERIOD, k=s.WB_EMA_BAND_K)
    keep = slice(max(0, len(raw) - bars), len(raw))
    lines: dict[str, Any] = {}
    for r in await db.fetchall(
            "SELECT metric_key, as_of, value, value_json, status FROM metric_values WHERE occupant_key = ? AND "
            "metric_key IN ('cull_line_1','cull_line_k_30d','immunity_remaining_days','dereg_rank_30d',"
            "'mp_gap_to_cull_k_30d','ema72_band_pos_1h','ema72_band_pos_1d') AND stage = 'raw' AND as_of >= ? "
            "ORDER BY as_of", (sub["occupant_key"], now - 3 * 86400)):
        lines[r["metric_key"]] = {"value": r["value"], "as_of": r["as_of"], "status": r["status"],
                                  **{k: v for k, v in _vj(r).items() if k in ("k", "graduates_at", "reason")}}
    imm = lines.get("immunity_remaining_days")
    immunity_end = (imm["as_of"] + int(imm["value"] * 86400)) if imm and imm["value"] else None
    events = await db.fetchall(
        "SELECT event_type, event_at, severity, payload FROM events WHERE occupant_key = ? AND event_type IN "
        "('dereg_rank_entered_k30','dereg_rank_cleared_k30','immunity_ends_7d','ema72_band_exit','dereg_risk_entered',"
        "'dereg_risk_cleared','emission_disabled','emission_reenabled') ORDER BY event_at DESC LIMIT 50",
        (sub["occupant_key"],))
    return {"netuid": netuid, "occupant_key": sub["occupant_key"], "name": sub["name"], "status": sub["status"],
            "period": period, "bars": [dict(r) for r in raw[keep]], "overlay": ov[keep], "lines": lines,
            "warmup": len(raw) < 2 * s.WB_EMA_PERIOD,
            "immunity_end": immunity_end, "events": [dict(e) for e in events]}
