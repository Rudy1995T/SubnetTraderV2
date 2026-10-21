"""Ported from the old SubnetTrader ``tests/test_factor_backtest.py`` (Phases 1–4, residualisation, net deciles,
deflation; only the imports change — ``_reg_unix`` → ``block_to_unix``) plus spec 13 §9 additions."""
from __future__ import annotations

import math

import pytest

from app.quant.factor_backtest import (
    FACTOR_FUNCS,
    CrossSection,
    _net_return,
    build_cross_sections,
    by_regime,
    compute_deciles,
    compute_deciles_net,
    compute_ic,
    deflate_factors,
    factor_age,
    factor_momentum,
    factor_px_vs_moving,
    factor_volume,
    make_quality_cg,
    quality_cg_value,
    residualize,
    resolve_factor,
    run_factor_study,
)
from app.quant.factor_panel import Bar, block_to_unix, build_panel
from app.util.mathutil import monotone_score, quantile_buckets, spearman_r, zscore

GRID = 4 * 3600


def _rows(prices, depth=10_000.0, start=GRID):
    return [(start + i * GRID, float(p), float(depth)) for i, p in enumerate(prices)]


# ── Phase 1: stats primitives ──────────────────────────────────────
def test_spearman_perfect_and_inverse():
    xs = [1, 2, 3, 4, 5, 6]
    assert spearman_r(xs, [10, 20, 30, 40, 50, 60]) == pytest.approx(1.0)
    assert spearman_r(xs, [60, 50, 40, 30, 20, 10]) == pytest.approx(-1.0)
    assert spearman_r(xs, [1, 4, 9, 16, 25, 36]) == pytest.approx(1.0)


def test_spearman_hand_checked():
    assert abs(spearman_r([1, 2, 3, 4, 5], [10, 20, 30, 50, 40]) - 0.9) < 1e-9


def test_quantile_buckets_identity_and_ties():
    assert quantile_buckets(list(range(10)), 10) == list(range(10))
    out = quantile_buckets([5, 5, 5, 5], 10)
    assert len(out) == 4 and all(0 <= b < 10 for b in out)
    assert quantile_buckets([3, 1, 2], 10) == [6, 0, 3]


def test_zscore_basic_and_degenerate():
    z = zscore([1, 2, 3, 4, 5])
    assert abs(sum(z)) < 1e-9 and abs(z[0] + 1.2649) < 1e-3       # sample std (old math.py)
    assert zscore([2, 2, 2]) == [0.0, 0.0, 0.0]
    assert zscore([1]) == [0.0]


def test_monotone_score():
    assert monotone_score([0.1 * i for i in range(10)]) == pytest.approx(1.0)
    assert monotone_score([-0.1 * i for i in range(10)]) == pytest.approx(-1.0)
    assert monotone_score([0.1, None, 0.2, 0.3, None, 0.4, 0.5]) == pytest.approx(1.0)
    assert monotone_score([0.1, 0.2, 0.3, 0.4]) == 0.0


# ── Phase 3: IC engine ─────────────────────────────────────────────
def _ic_panel(n_netuids=20, n_bars=30):
    series = {}
    for nu in range(1, n_netuids + 1):
        prices = [1.0 * (1.0 + 0.01 * nu) ** t for t in range(n_bars)]
        series[nu] = _rows(prices)
    return build_panel(series, grid_seconds=GRID)


def test_ic_perfect_predictor_is_plus_one():
    panel = _ic_panel()

    def perfect(series, i, k):
        if i + 1 >= len(series) or series[i].price <= 0:
            return None
        return series[i + 1].price / series[i].price - 1.0

    sections, _ = build_cross_sections(panel, perfect, horizon=1, lookback=1, min_depth=0.0, min_names=10, step=1)
    ic = compute_ic(sections)
    assert ic.n_dates > 0 and ic.mean_ic > 0.999


def test_ic_inverse_predictor_is_minus_one():
    panel = _ic_panel()

    def inverse(series, i, k):
        if i + 1 >= len(series) or series[i].price <= 0:
            return None
        return -(series[i + 1].price / series[i].price - 1.0)

    sections, _ = build_cross_sections(panel, inverse, horizon=1, lookback=1, min_depth=0.0, min_names=10, step=1)
    assert compute_ic(sections).mean_ic < -0.999


def test_ic_random_factor_is_near_zero():
    panel = _ic_panel(n_netuids=40, n_bars=40)

    def pseudo_random(series, i, k):
        return float((series[i].bucket * 2654435761 + i * 40503) % 1000)

    sections, _ = build_cross_sections(panel, pseudo_random, horizon=1, lookback=1, min_depth=0.0, min_names=10,
                                       step=1)
    assert abs(compute_ic(sections).mean_ic) < 0.35


# ── Phase 4: decile spreads ────────────────────────────────────────
def test_decile_curve_monotone_for_perfect_factor():
    sections = [CrossSection(d, list(range(20)), [float(j) for j in range(20)], [0.01 * j for j in range(20)])
                for d in range(5)]
    dec = compute_deciles(sections, n_buckets=10)
    curve = [c for c in dec.decile_curve if c is not None]
    assert curve == sorted(curve) and dec.mean_long_short > 0
    assert dec.bottom_decile_mean < dec.top_decile_mean


def test_run_factor_study_smoke():
    panel = _ic_panel(n_netuids=30, n_bars=40)
    results = run_factor_study(panel, factors=["momentum", "size"], horizons=[1, 6], lookback=3, min_depth=0.0,
                               min_names=10)
    assert set(results) == {"momentum", "size"} and set(results["momentum"]) == {1, 6}
    assert results["momentum"][1].ic.n_dates > 0
    assert results["momentum"][1].label == "ret_log_1"


# ── Age factor ─────────────────────────────────────────────────────
def test_age_factor_dates_from_registration_block():
    reg = 6_502_594
    t = int(block_to_unix(reg)) + 10 * 86400
    assert abs(factor_age([Bar(t, 1.0, 9_999.0, 0, reg)], 0, 0) - 10.0) < 0.05


def test_age_factor_none_when_unknown_or_pre_registration():
    reg = 6_502_594
    t = int(block_to_unix(reg)) + 5 * 86400
    assert factor_age([Bar(t, 1.0, 9_999.0, 0, 0)], 0, 0) is None
    pre = int(block_to_unix(reg)) - 3 * 86400
    assert factor_age([Bar(pre, 1.0, 9_999.0, 0, reg)], 0, 0) is None


def test_age_factor_ranks_older_subnets_higher():
    t = int(block_to_unix(8_000_000)) + 100 * 86400
    old = factor_age([Bar(t, 1.0, 1.0, 0, 1_400_000)], 0, 0)
    young = factor_age([Bar(t, 1.0, 1.0, 0, 8_000_000)], 0, 0)
    assert old is not None and young is not None and old > young


def test_age_prefers_exact_registration_time():
    b = Bar(1_000_000, 1.0, 1.0, 0, 6_502_594, pit={"_reg_ts": 1_000_000 - 2 * 86400})
    assert factor_age([b], 0, 0) == pytest.approx(2.0)


def test_build_panel_stamps_reg_block():
    panel = build_panel({5: _rows([1.0, 1.1, 1.2])}, grid_seconds=GRID, reg_blocks={5: 6_502_594})
    assert all(b.reg_block == 6_502_594 for b in panel.series[5])
    assert build_panel({5: _rows([1.0, 1.1])}, grid_seconds=GRID).series[5][0].reg_block == 0


def test_max_depth_restricts_to_band():
    series = {k: _rows([1.0, 1.0 + k * 0.01], depth=2000.0 if k <= 6 else 10_000.0) for k in range(1, 13)}
    panel = build_panel(series, grid_seconds=GRID)
    res = run_factor_study(panel, factors=["momentum"], horizons=[1], lookback=1, min_depth=0.0, max_depth=3000.0,
                           min_names=3)
    assert res["momentum"][1].max_depth == 3000.0


# ── Net-of-slippage decile spreads ─────────────────────────────────
def test_net_return_costs_erode_gains_and_floor_at_death():
    assert abs(_net_return(0.10, 1_000_000.0, 2.0) - 0.10) < 1e-3
    assert _net_return(0.10, 500.0, 5.0) < 0.10
    assert abs(_net_return(-1.0, 500.0, 5.0) + 1.0) < 1e-9


def test_compute_deciles_net_reduces_spread_but_keeps_ranking():
    sections = [CrossSection(d, list(range(20)), [float(j) for j in range(20)], [0.01 * j for j in range(20)],
                             [500.0] * 20) for d in range(5)]
    gross = compute_deciles(sections, n_buckets=10)
    net = compute_deciles_net(sections, trade_tao=5.0, n_buckets=10)
    assert net.mean_long_short < gross.mean_long_short
    assert net.bottom_decile_mean < net.top_decile_mean


def test_volume_and_px_vs_moving_factors():
    bar = Bar(GRID, 0.01, 9_999.0, 0, 0, 0.011, 500_000.0)
    assert abs(factor_volume([bar], 0, 0) - math.log(500_000.0)) < 1e-9
    assert factor_volume([Bar(GRID, 0.01, 9_999.0, 0, 0)], 0, 0) is None
    assert factor_px_vs_moving([Bar(GRID, 0.012, 9_999.0, 0, 0, 0.010, 1.0)], 0, 0) > 0
    assert factor_px_vs_moving([Bar(GRID, 0.008, 9_999.0, 0, 0, 0.010, 1.0)], 0, 0) < 0
    assert factor_px_vs_moving([Bar(GRID, 0.01, 9_999.0, 0, 0)], 0, 0) is None


def test_moving_px_trend_factor():
    from app.quant.factor_backtest import factor_moving_px_trend
    rising = [Bar(GRID * t, 0.01, 9_999.0, 0, 0, 0.010 + 0.001 * t, 0.0) for t in range(5)]
    v = factor_moving_px_trend(rising, 4, 2)
    assert v is not None and abs(v - math.log(0.014 / 0.012)) < 1e-9
    gap = list(rising)
    gap[2] = Bar(GRID * 2, 0.01, 9_999.0, 0, 0, 0.0, 0.0)
    assert factor_moving_px_trend(gap, 4, 2) is None
    crossed = [Bar(GRID * t, 0.01, 9_999.0, 0 if t < 2 else 1, 0, 0.010 + 0.001 * t, 0.0) for t in range(5)]
    assert factor_moving_px_trend(crossed, 4, 3) is None


def test_retired_factors_not_in_universe():
    assert "moving_px_trend" in FACTOR_FUNCS
    assert "net_flow_depth" not in FACTOR_FUNCS and "quality" not in FACTOR_FUNCS


def test_run_factor_study_net_deciles_opt_in():
    panel = _ic_panel(n_netuids=30, n_bars=40)
    with_net = run_factor_study(panel, factors=["size"], horizons=[1], lookback=3, min_depth=0.0, min_names=10,
                                trade_tao=2.0)["size"][1]
    assert with_net.deciles_net is not None and with_net.trade_tao == 2.0
    gross_only = run_factor_study(panel, factors=["size"], horizons=[1], lookback=3, min_depth=0.0,
                                  min_names=10)["size"][1]
    assert gross_only.deciles_net is None


# ── Orthogonality + deflated Sharpe ───────────────────────────────
def test_residualize_collapses_collinear_keeps_orthogonal():
    y = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert all(abs(r) < 1e-9 for r in residualize(y, [[2.0 * v + 1.0 for v in y]]))
    ctrl = [-2.0, -1.0, 0.0, 1.0, 2.0]
    yo = [1.0, -1.0, 0.0, -1.0, 1.0]
    assert all(abs(r - v) < 1e-9 for r, v in zip(residualize(yo, [ctrl]), yo))


def test_orthogonalizing_factor_against_itself_collapses_ic():
    panel = _ic_panel(n_netuids=30, n_bars=40)
    raw, _ = build_cross_sections(panel, factor_momentum, horizon=1, lookback=1, min_depth=0.0, min_names=10, step=1)
    orth, _ = build_cross_sections(panel, factor_momentum, horizon=1, lookback=1, min_depth=0.0, min_names=10, step=1,
                                   controls=[factor_momentum])
    assert abs(compute_ic(raw).mean_ic) > 0.3 and abs(compute_ic(orth).mean_ic) < 0.2


def test_run_factor_study_controls_excludes_self():
    panel = _ic_panel(n_netuids=30, n_bars=40)
    res = run_factor_study(panel, factors=["momentum", "size"], horizons=[1], lookback=1, min_depth=0.0,
                           min_names=10, controls=["size"])
    assert res["size"][1].ic.n_dates > 0 and res["size"][1].controls == []
    assert res["momentum"][1].controls == ["size"]


def test_deflate_factors_returns_bounded_dsr_per_factor():
    panel = _ic_panel(n_netuids=30, n_bars=40)
    results = run_factor_study(panel, factors=["momentum", "size"], horizons=[1], lookback=3, min_depth=0.0,
                               min_names=10)
    d = deflate_factors(results, horizon=1)
    assert set(d) == {"momentum", "size"}
    for ds in d.values():
        assert 0.0 <= ds.dsr <= 1.0 and ds.n_trials == 2


# ── spec 13 additions ─────────────────────────────────────────────
def _pit_panel(values: dict[int, dict], n_bars=4, rets=None):
    """12 names; pit dict per netuid; next-bar return k%."""
    rows = {}
    for k in range(1, 13):
        r = (rets or {}).get(k, k * 0.01)
        rows[k] = [(GRID * (i + 1), 1.0 * (1 + r) ** i, 5000.0, 0.0, 0.0, GRID * (i + 1), f"{k}:1", 1,
                    values.get(k, {})) for i in range(n_bars)]
    return build_panel(rows, grid_seconds=GRID, pit_keys=("P1", "P2", "active_authors_28d", "chain:emission_share"))


def test_quality_cg_renormalisation():
    assert quality_cg_value(0.5, 1.0) == pytest.approx((30 * 0.5 + 20 * 1.0) / 50)
    assert quality_cg_value(None, 0.4) == 0.4 and quality_cg_value(0.7, None) == 0.7
    assert quality_cg_value(None, None) is None
    b = Bar(0, 1.0, 1.0, 0, pit={"P1": 1.0})
    assert make_quality_cg()([b], 0, 0) == 1.0


def test_pit_factor_ic_positive_when_it_predicts_return():
    panel = _pit_panel({k: {"active_authors_28d": k / 20.0} for k in range(1, 13)})
    res = run_factor_study(panel, factors=["active_authors_28d"], horizons=[1], lookback=1, min_depth=0.0,
                           min_names=10)
    assert res["active_authors_28d"][1].ic.mean_ic > 0.9


def test_controls_none_dropped_and_counted_and_emission_share_alias():
    vals = {k: {"active_authors_28d": k / 20.0, "chain:emission_share": float(k % 5)} for k in range(1, 13)}
    vals[3].pop("chain:emission_share")
    panel = _pit_panel(vals)
    res = run_factor_study(panel, factors=["active_authors_28d"], horizons=[1], lookback=1, min_depth=0.0,
                           min_names=10, controls=["emission_share"])
    r = res["active_authors_28d"][1]
    assert r.controls == ["chain:emission_share"]
    assert r.n_dropped_controls > 0


def test_resolve_factor_rules():
    panel = _pit_panel({})
    with pytest.raises(ValueError, match="owner_commitment"):
        resolve_factor("owner_conviction", panel)
    with pytest.raises(KeyError):
        resolve_factor("emission_share", panel)          # not stamped on this panel
    assert resolve_factor("quality_cg", panel) is not None


def test_by_regime_sums_and_unknown():
    secs = [CrossSection(d, list(range(10)), [float(j) for j in range(10)], [0.01 * j for j in range(10)])
            for d in (1, 2, 3)]
    regime_of = {1: {"emission": "taoflow", "market": "TRENDING", "market_source": "backfill"},
                 2: {"emission": "taoflow", "market": "UNKNOWN", "market_source": "missing"}}
    br = by_regime(secs, regime_of)
    assert sum(v["n_dates"] for v in br["market"].values()) == 3
    assert br["market"]["UNKNOWN"]["n_dates"] == 2
    assert sum(v["n_dates"] for v in br["emission"].values()) == 3


def test_deflate_variance_from_trial_log_and_orient():
    panel = _ic_panel(n_netuids=30, n_bars=40)
    res = run_factor_study(panel, factors=["momentum"], horizons=[1], lookback=3, min_depth=0.0, min_names=10)
    d0 = deflate_factors(res, horizon=1, n_trials=10)
    assert d0["momentum"].sr0 == 0.0                    # the ported bug: one factor → variance 0
    d1 = deflate_factors(res, horizon=1, n_trials=10, trial_sharpe_variance=0.04)
    assert d1["momentum"].sr0 > 0
    neg = run_factor_study(panel, factors=["momentum"], horizons=[1], lookback=3, min_depth=0.0, min_names=10)
    neg["momentum"][1].ic.ic_series = [-0.05 - 0.01 * (j % 3) for j in range(30)]
    assert deflate_factors(neg, horizon=1, orient=False)["momentum"].sharpe < 0
    assert deflate_factors(neg, horizon=1, orient=True)["momentum"].sharpe > 0
