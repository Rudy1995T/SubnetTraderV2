"""Ported Phase-2 panel tests (old ``tests/test_factor_backtest.py``) + spec 13 occupant/death/anchor additions."""
from __future__ import annotations

import math

import pytest

from app.quant.factor_panel import (
    DEATH_RETURN,
    build_panel,
    block_to_unix,
    forward_log_return,
    forward_return,
    segment_deaths,
    segment_series,
)

GRID = 4 * 3600


def _rows(prices, depth=10_000.0, start=GRID):
    return [(start + i * GRID, float(p), float(depth)) for i, p in enumerate(prices)]


def _occ_rows(prices, occs):
    return [(GRID * (i + 1), p, 5000.0, 0.0, 0.0, GRID * (i + 1) + 100, o, 0, None) for i, (p, o) in
            enumerate(zip(prices, occs))]


# ── ported ─────────────────────────────────────────────────────────
def test_segment_collapse_is_terminal_of_dying_segment():
    assert segment_series([1.0, 1.1, 0.9, 0.002, 0.05]) == [0, 0, 0, 0, 1]


def test_monotone_decline_is_one_segment():
    assert segment_series([1.0, 0.7, 0.5, 0.4]) == [0, 0, 0, 0]


def test_forward_return_realizes_death_not_healthy_price():
    panel = build_panel({7: _rows([1.0, 1.1, 0.9, 0.002, 0.05])}, grid_seconds=GRID)
    fr = forward_return(panel, 7, 2, horizon=1)
    assert fr is not None and fr < -0.99
    fr2 = forward_return(panel, 7, 1, horizon=2)
    assert fr2 is not None and fr2 < -0.99
    assert forward_return(panel, 7, 4, horizon=1) is None


def test_forward_return_normal_within_segment():
    panel = build_panel({1: _rows([1.0, 1.2, 1.5])}, grid_seconds=GRID)
    assert abs(forward_return(panel, 1, 0, horizon=2) - 0.5) < 1e-9


def test_forward_return_alive_but_no_future_is_none():
    panel = build_panel({1: _rows([1.0, 1.1, 1.2])}, grid_seconds=GRID)
    assert forward_return(panel, 1, 2, horizon=1) is None


def test_build_panel_carries_moving_price_and_volume():
    wide = [(GRID, 0.01, 9_999.0, 0.011, 500_000.0), (2 * GRID, 0.012, 9_999.0, 0.0118, 510_000.0)]
    b0 = build_panel({3: wide}, grid_seconds=GRID).series[3][0]
    assert b0.moving_price == 0.011 and b0.volume == 500_000.0
    narrow = build_panel({3: _rows([1.0, 1.1])}, grid_seconds=GRID).series[3][0]
    assert narrow.moving_price == 0.0 and narrow.volume == 0.0
    assert narrow.ts == narrow.bucket and narrow.occ is None and dict(narrow.pit) == {}


# ── spec 13 additions ─────────────────────────────────────────────
def test_occupant_change_without_collapse_is_a_reoccupied_boundary():
    panel = build_panel({9: _occ_rows([1.0, 1.1, 1.2, 1.3, 1.4], ["9:1", "9:1", "9:1", "9:2", "9:2"])},
                        grid_seconds=GRID)
    bars = panel.series[9]
    assert [b.seg for b in bars] == [0, 0, 0, 1, 1]
    assert panel.seg_death[9] == {0: "reoccupied"}
    assert forward_return(panel, 9, 1, horizon=3) == DEATH_RETURN
    assert forward_return(panel, 9, 0, horizon=2) == pytest.approx(0.2)


def test_unknown_to_known_occupant_is_not_a_boundary():
    segs, death = segment_deaths([1.0, 1.1, 1.2], occupants=[None, "9:2", "9:2"])
    assert segs == [0, 0, 0] and death == {}


def test_both_rules_at_the_same_bar_give_one_price_reset():
    segs, death = segment_deaths([1.0, 1.1, 0.001, 0.002], occupants=["7:1", "7:1", "7:2", "7:2"])
    assert segs == [0, 0, 0, 1] and death == {0: "price_reset"}


def test_crash_of_a_known_living_occupant_is_not_a_death():
    segs, death = segment_deaths([1.0, 1.1, 0.01, 0.02], occupants=["7:1", "7:1", "7:1", "7:1"])
    assert segs == [0, 0, 0, 0] and death == {}
    segs, death = segment_deaths([1.0, 1.1, 0.01, 0.02], occupants=["7:1", None, None, "7:1"])
    assert segs == [0, 0, 0, 1] and death == {0: "price_reset"}        # unknown occupant → ported price rule


def test_forward_log_return_floor():
    panel = build_panel({7: _rows([1.0, 1.1, 0.9, 0.000001, 0.05])}, grid_seconds=GRID)
    assert forward_log_return(panel, 7, 2, 1) == pytest.approx(math.log(0.01))
    assert forward_log_return(panel, 7, 0, 1) == pytest.approx(math.log(1.1))


def test_block_to_unix_anchor_order():
    assert block_to_unix(6_502_594) == 1_758_585_588                       # old constant anchor
    anchors = [(100, 1_000), (200, 2_500)]
    assert block_to_unix(150, anchors) == pytest.approx(1_750)             # interpolated (15 s/block here)
    assert block_to_unix(200, anchors) == 2_500
    assert block_to_unix(210, anchors) == pytest.approx(2_500 + 120)       # extrapolated at 12 s/block
    assert block_to_unix(90, anchors) == pytest.approx(1_000 - 120)
