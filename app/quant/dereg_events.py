"""Slot-reset (deregistration) detection on the daily price panel (spec 13 §8.1).

Port of the old SubnetTrader ``app/backtest/dereg_analysis.py:48-96`` (``DeregEvent``, ``load_daily_panel``,
``detect_dereg_events``): ``load_daily_panel`` reads ``v_quant_price_bars`` 1 h bars instead of ``pool_history``;
``reused_slots`` is rewritten over ``v_quant_occupants``. Used as a cross-check of the labels and by the verify list.
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass

DAY = 86400
RESET_RATIO = 0.30


@dataclass
class DeregEvent:
    netuid: int
    day: int
    pre_price: float
    post_price: float
    drop_pct: float
    pre_depth_tao: float


def load_daily_panel(con: sqlite3.Connection) -> dict[int, list[tuple[int, float, float]]]:
    """One ``(sample_ts, last_close, last_depth_tao)`` per netuid per UTC day (last write wins)."""
    panel: dict[int, dict[int, tuple[int, float, float]]] = defaultdict(dict)
    cur = con.execute("SELECT netuid, COALESCE(ts_last, bucket_start + 3600) AS ts, close, tao_in_pool "
                      "FROM v_quant_price_bars WHERE period = '1h' AND netuid != 0 ORDER BY netuid, ts")
    for nu, ts, close, depth in cur:
        p = float(close or 0.0)
        if p <= 0:
            continue
        panel[int(nu)][int(ts) // DAY] = (int(ts), p, float(depth or 0.0))
    return {nu: [days[d] for d in sorted(days)] for nu, days in panel.items()}


def detect_dereg_events(panel: dict[int, list[tuple[int, float, float]]]) -> list[DeregEvent]:
    """Slot resets: a single-day price ratio below ``RESET_RATIO``."""
    events: list[DeregEvent] = []
    for nu, series in panel.items():
        for i in range(1, len(series)):
            (_, p0, d0), (ts1, p1, _) = series[i - 1], series[i]
            if p0 > 0 and p1 > 0 and p1 / p0 < RESET_RATIO:
                events.append(DeregEvent(netuid=nu, day=ts1 // DAY, pre_price=p0, post_price=p1,
                                         drop_pct=(1 - p1 / p0) * 100.0, pre_depth_tao=d0))
    return events


def reused_slots(con: sqlite3.Connection) -> list[tuple[int, int]]:
    """Netuids held by more than one occupant: ``(netuid, n_occupants)``."""
    return [(int(r[0]), int(r[1])) for r in con.execute(
        "SELECT netuid, COUNT(*) FROM v_quant_occupants WHERE netuid != 0 GROUP BY netuid HAVING COUNT(*) > 1 "
        "ORDER BY netuid")]
