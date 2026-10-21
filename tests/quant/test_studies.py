"""Walk-forward, survival and rank-stability studies (spec 13 §8.5) on synthetic panels."""
from __future__ import annotations

import pytest

from app.quant.factor_backtest import make_pit_factor, run_factor_study
from app.quant.factor_panel import build_panel
from app.quant.labels import DeathEvent
from app.quant.rank_stability import rank_stability
from app.quant.survival import survival_study
from app.quant.walk_forward import (
    MIN_FOLDS_TO_CERTIFY,
    make_folds,
    slice_panel,
    walk_forward_factors,
)

D = 86400


def _panel(n=20, bars=60, grid=D, pit=None):
    rows = {}
    for nu in range(1, n + 1):
        rows[nu] = [(grid * (i + 1), (1 + 0.01 * nu) ** i, 5000.0, 0.0, 0.0, grid * (i + 1) + 10, f"{nu}:1", 1,
                     (pit or (lambda nu, i: {}))(nu, i)) for i in range(bars)]
    return build_panel(rows, grid_seconds=grid, pit_keys=("f",))


# ── walk-forward ──────────────────────────────────────────────────
def test_folds_tile_non_overlapping_last_short():
    folds = make_folds(list(range(0, 100)), train_min=50, test_h=20)
    assert [(f.test_lo, f.test_hi) for f in folds] == [(50, 70), (70, 90), (90, 100)]
    assert folds[-1].n_test_bars == 10 and all(f.train_lo == 0 for f in folds)


def test_insufficient_folds_advisory_and_n_oos_folds():
    p = _panel()
    folds = make_folds(p.dates, train_min=30, test_h=10)        # 3 folds
    rep = walk_forward_factors(p, ["momentum"], folds, horizon=2, lookback=2, min_depth=0, min_names=10)
    assert rep.n_folds == 3 < MIN_FOLDS_TO_CERTIFY and not rep.oos_credible and "advisory only" in rep.gate_note
    folds = make_folds(p.dates, train_min=20, test_h=1)         # 1-bar test windows → no forward return inside
    rep = walk_forward_factors(p, ["momentum"], folds, horizon=2, lookback=2, min_depth=0, min_names=10)
    assert rep.n_folds >= 4 and rep.n_oos_folds == 0


def test_slice_panel_carries_fields_and_truncates():
    p = _panel(pit=lambda nu, i: {"f": float(nu)})
    s = slice_panel(p, p.dates[10], p.dates[20])
    b = s.series[3][0]
    assert b.occ == "3:1" and b.ts == b.bucket + 10 and b.pit["f"] == 3.0
    assert len(s.dates) == 10
    from app.quant.factor_panel import forward_return
    assert forward_return(s, 3, 9, 1) is None                  # no look-ahead past the window edge


def test_single_factor_equals_study_on_fold_slices():
    p = _panel(bars=80)
    folds = make_folds(p.dates, train_min=40, test_h=20)
    rep = walk_forward_factors(p, ["momentum"], folds, horizon=2, lookback=2, min_depth=0, min_names=10)
    f = folds[0]
    direct = run_factor_study(slice_panel(p, f.test_lo, f.test_hi), factors=["momentum"], horizons=[2], lookback=2,
                              min_depth=0, min_names=10)["momentum"][2].ic
    assert rep.rows[0].oos["mean_ic"] == pytest.approx(direct.mean_ic) and rep.rows[0].winner == "momentum"


def test_walk_forward_refuses_day_labels():
    with pytest.raises(ValueError):
        walk_forward_factors(_panel(), ["momentum"], [], horizon=2, label="death")


# ── survival ──────────────────────────────────────────────────────
class FakeLabels:
    """death_event from a plan: {netuid: (onset, kind)}; censored netuids → None."""

    def __init__(self, plan, censored=()):
        self.plan, self.censored = plan, set(censored)

    def death_event(self, panel, nu, i, days, *, dereg_only=False):
        if nu in self.censored:
            return None, None
        t = panel.series[nu][i].bucket
        hit = self.plan.get(nu)
        if hit and t < hit[0] <= t + days * D and (not dereg_only or hit[1] in ("dereg", "price_reset",
                                                                                 "reoccupied")):
            return 1, DeathEvent(hit[0], hit[1], f"{nu}@{hit[0]}")
        return 0, None


def _surv_panel(n=30, bars=300):
    return _panel(n=n, bars=bars, pit=lambda nu, i: {"f": float(nu)})


def test_survival_ratio_and_distinct_events():
    p = _surv_panel()
    # bottom tercile (f = 1..10): 6 deaths; top (21..30): 2 deaths; deaths on day 250 so obs at day 1 and 91
    # (step 90 < horizon 180) both see them — each death counts once
    plan = {nu: (p.dates[200], "dereg") for nu in (1, 2, 3, 4, 5, 6, 21, 22)}
    res = survival_study(p, factor_fn=make_pit_factor("f"), label_source=FakeLabels(plan), horizon_days=180,
                         step_days=90, factor="f")
    low, high = res.terciles[0], res.terciles[-1]
    assert low.death_rate > 2 * high.death_rate and res.death_ratio_low_over_high >= 2
    assert res.n_events == 8 and res.n_event_dates == 1 and res.n_events_by_kind == {"dereg": 8}
    assert res.n_obs > res.n_events


def test_survival_inf_zero_and_censored():
    p = _surv_panel()
    res = survival_study(p, factor_fn=make_pit_factor("f"), label_source=FakeLabels({1: (p.dates[50], "dereg")}),
                         horizon_days=90, step_days=90)
    assert res.death_ratio_low_over_high == float("inf")
    res = survival_study(p, factor_fn=make_pit_factor("f"), label_source=FakeLabels({}), horizon_days=90,
                         step_days=90)
    assert res.death_ratio_low_over_high == 0.0
    res2 = survival_study(p, factor_fn=make_pit_factor("f"), label_source=FakeLabels({}, censored={1, 2}),
                          horizon_days=90, step_days=90)
    assert res2.n_censored > 0 and res2.n_obs < res.n_obs


def test_survival_dereg_only_diagnostic():
    p = _surv_panel()
    plan = {nu: (p.dates[50], "zero_emission") for nu in (1, 2, 3, 4)}
    res = survival_study(p, factor_fn=make_pit_factor("f"), label_source=FakeLabels(plan), horizon_days=90,
                         step_days=90)
    assert res.death_ratio_low_over_high == float("inf")
    assert res.death_ratio_dereg_only == 0.0 and res.n_events_dereg_only == 0


def test_survival_min_depth_zero_includes_thin_names():
    rows = {nu: [(D * (i + 1), 1.0, 10.0 if nu <= 5 else 5000.0, 0.0, 0.0, D * (i + 1), f"{nu}:1", 1,
                  {"f": float(nu)}) for i in range(200)] for nu in range(1, 31)}
    p = build_panel(rows, grid_seconds=D, pit_keys=("f",))
    a = survival_study(p, factor_fn=make_pit_factor("f"), label_source=FakeLabels({}), min_depth=0.0)
    b = survival_study(p, factor_fn=make_pit_factor("f"), label_source=FakeLabels({}), min_depth=3000.0)
    assert a.n_names == 30 and b.n_names == 25


# ── rank stability ────────────────────────────────────────────────
def test_rank_stability_identical_and_reversed():
    p = _panel(n=25, bars=10, pit=lambda nu, i: {"f": float(nu)})
    r = rank_stability(p, make_pit_factor("f"), lag=1)
    assert r.mean_rank_autocorr == pytest.approx(1.0) and r.mean_quintile_turnover == 0.0
    assert r.mean_top20_turnover == 0.0 and r.n_pairs == 9
    p = _panel(n=25, bars=2, pit=lambda nu, i: {"f": float(nu) if i == 0 else -float(nu)})
    assert rank_stability(p, make_pit_factor("f"), lag=1).mean_rank_autocorr == pytest.approx(-1.0)


def test_rank_stability_thin_and_order_independent():
    p = _panel(n=8, bars=5, pit=lambda nu, i: {"f": float(nu)})
    r = rank_stability(p, make_pit_factor("f"))
    assert r.n_pairs == 0 and r.mean_rank_autocorr is None and all(s[2] is None for s in r.series)
    p1 = _panel(n=25, bars=6, pit=lambda nu, i: {"f": float((nu * 7 + i) % 25)})
    p2 = _panel(n=25, bars=6, pit=lambda nu, i: {"f": float((nu * 7 + i) % 25)})
    p2.series = dict(reversed(list(p2.series.items())))
    assert rank_stability(p1, make_pit_factor("f")).series == rank_stability(p2, make_pit_factor("f")).series
