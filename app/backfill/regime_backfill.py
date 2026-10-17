"""Regime history from ``price_bars`` 1h (spec 17 §2.7, step S7).

Uses spec 12's pure port ``app.quant.regime.compute_regime_metrics`` with the frozen ``REGIME_TS_1_0`` params and
``v_w_by_regime`` (from ``app.scoring.spec`` once Phase 8 builds it, else the constants beside the port). If the port
is missing ``RegimeUnavailable`` is raised and the script exits 3 ("requires spec 12 port").
A thin universe is ``UNKNOWN`` (v_w 0.15), never ``DEAD``; live rows are never replaced (INSERT OR IGNORE).
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any, Mapping

from app.backfill import common
from app.quant.regime import V_W_BY_REGIME_TS_1_0 as V_W_TS_1_0
from app.quant.regime import label_for
from app.util.time import now_ts


class RegimeUnavailable(RuntimeError):
    pass


def load_regime() -> tuple[Any, Any, Mapping[str, float]]:
    """(compute_regime_metrics, params, v_w_by_regime) from spec 12's modules, or ``RegimeUnavailable``."""
    try:
        from app.quant.regime import REGIME_TS_1_0, V_W_BY_REGIME_TS_1_0, compute_regime_metrics
    except ImportError as exc:
        raise RegimeUnavailable(f"requires spec 12 port (app/quant/regime.py): {exc}") from None
    try:
        from app.scoring.spec import REGIME_TS_1_0, TS_1_0  # type: ignore[attr-defined,no-redef]
        return compute_regime_metrics, REGIME_TS_1_0, TS_1_0.v_w_by_regime
    except ImportError:         # Phase 8 not built: the frozen ts-1.0 constants beside the port
        return compute_regime_metrics, REGIME_TS_1_0, V_W_BY_REGIME_TS_1_0


def snapshots_for_day(con: sqlite3.Connection, day_ts: int) -> dict[int, list[dict]]:
    """``{netuid: [{"ts": bucket_start, "price": close}]}`` for ``ts ∈ (D − 86400, D]``, netuid ≠ 0, close > 0."""
    out: dict[int, list[dict]] = {}
    for n, ts, close in con.execute(
            "SELECT netuid, ts, close FROM price_bars WHERE period = '1h' AND ts > ? AND ts <= ? AND netuid <> 0 "
            "AND close > 0 ORDER BY netuid, ts", (day_ts - 86400, day_ts)):
        out.setdefault(int(n), []).append({"ts": int(ts), "price": float(close)})
    return out


def default_start(con: sqlite3.Connection) -> int | None:
    """First UTC day with ≥ 10 netuids of 1h bars."""
    r = con.execute("SELECT (ts / 86400) * 86400 d FROM price_bars WHERE period = '1h' AND netuid <> 0 GROUP BY d "
                    "HAVING COUNT(DISTINCT netuid) >= 10 ORDER BY d LIMIT 1").fetchone()
    return (int(r[0]) + 86400) if r else None


def default_end(con: sqlite3.Connection, spec_version: str) -> int:
    r = con.execute("SELECT MIN(day_ts) FROM regime_daily WHERE source = 'live' AND spec_version = ?",
                    (spec_version,)).fetchone()
    today = (now_ts() // 86400) * 86400
    return (int(r[0]) - 86400) if r and r[0] is not None else today - 86400


def backfill_regime(con: sqlite3.Connection, *, start_day: int | None, end_day: int | None, params: Any,
                    spec_version: str, run_id: str, compute: Any = None,
                    v_w_by_regime: Mapping[str, float] = V_W_TS_1_0) -> dict:
    if compute is None:
        compute, params, v_w_by_regime = load_regime()
    start = start_day if start_day is not None else default_start(con)
    end = end_day if end_day is not None else default_end(con, spec_version)
    rep = {"days": 0, "written": 0, "unknown": 0, "from_day": start, "to_day": end, "labels": {}}
    if start is None or end is None or end < start:
        return rep
    d = start
    rows = []
    while d <= end:
        m = compute(snapshots_for_day(con, d), params)
        thin = bool(m.get("thin_universe"))
        label = label_for(m)
        rows.append((d, spec_version, label, m.get("raw_regime"), m.get("vol_24h"), m.get("directional_strength"),
                     m.get("dispersion"), m.get("n_subnets"), int(thin), float(v_w_by_regime.get(label, 0.15)),
                     json.dumps({"series": "price_bars_1h", "window": [d - 86400, d]}), now_ts(), run_id))
        rep["labels"][label] = rep["labels"].get(label, 0) + 1
        rep["unknown"] += label == "UNKNOWN"
        rep["days"] += 1
        d += 86400
    with common.tx(con):
        before = con.total_changes
        con.executemany(
            "INSERT OR IGNORE INTO regime_daily (day_ts, spec_version, label, raw_regime, vol_24h, directional_strength,"
            " dispersion, n_subnets, thin_universe, v_w, tao_usd_ret_24h, sum_prices_ret_24h, inputs_json, source, "
            "computed_at, backfilled, run_id) VALUES (?,?,?,?,?,?,?,?,?,?,NULL,NULL,?,'backfill',?,1,?)", rows)
        rep["written"] = con.total_changes - before
    return rep
