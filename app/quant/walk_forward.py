"""Expanding / rolling walk-forward (out-of-sample) validation — factor track (spec 13 §8.5).

Port of the old SubnetTrader ``app/backtest/walk_forward.py`` (``MIN_FOLDS_TO_CERTIFY``, ``Fold``, ``make_folds``,
``FactorFoldRow``, ``WalkForwardReport``, ``_mean``, ``_decay``, ``_coverage_days``, ``_factor_ic_stats``,
``walk_forward_factors``, ``_factor_gate``, ``_report_dict``; ``save_walk_forward_json`` moved to ``report``). The
strategy track and the
``pool_history_loader`` coupling are not ported. ``slice_panel`` carries ``ts/occ/pit`` and rebuilds segmentation in
the window so forward returns self-truncate at the edge; ``WalkForwardReport`` gains ``n_oos_folds``.
"""
from __future__ import annotations

import math
import statistics
from dataclasses import asdict, dataclass, field

from .factor_panel import Panel, build_panel
from .validation import BootstrapCI

DAY = 86400.0
MIN_FOLDS_TO_CERTIFY = 4


@dataclass
class Fold:
    index: int
    train_lo: int
    train_hi: int
    test_lo: int
    test_hi: int
    n_train_bars: int
    n_test_bars: int


def make_folds(dates: list[int], *, train_min: int, test_h: int, mode: str = "expanding",
               train_width: int | None = None) -> list[Fold]:
    """Ordered folds over a sorted bucket axis (ported verbatim)."""
    n = len(dates)
    if test_h < 1:
        raise ValueError("test_h must be >= 1")
    if train_min < 1:
        raise ValueError("train_min must be >= 1")
    if mode not in ("expanding", "rolling"):
        raise ValueError(f"unknown mode {mode!r} (expected 'expanding' or 'rolling')")
    folds: list[Fold] = []
    t = train_min
    fi = 0
    while t < n:
        test_end = min(t + test_h, n)
        if mode == "expanding":
            train_start = 0
        else:
            w = train_width or train_min
            train_start = max(0, t - w)
        test_hi = dates[test_end] if test_end < n else dates[n - 1] + 1
        folds.append(Fold(index=fi, train_lo=dates[train_start], train_hi=dates[t], test_lo=dates[t],
                          test_hi=test_hi, n_train_bars=t - train_start, n_test_bars=test_end - t))
        fi += 1
        t += test_h
    return folds


def slice_panel(panel: Panel, lo: int, hi: int) -> Panel:
    """Sub-panel over buckets in ``[lo, hi)`` rebuilt via ``build_panel`` (segmentation and deaths recomputed in the
    window ⇒ no cross-window look-ahead); every bar field is carried."""
    rows_by_netuid: dict[int, list[tuple]] = {}
    for nu, bars in panel.series.items():
        sub = [(b.bucket, b.price, b.depth, b.moving_price, b.volume, b.ts, b.occ, b.reg_block, b.pit)
               for b in bars if lo <= b.bucket < hi]
        if sub:
            rows_by_netuid[nu] = sub
    out = build_panel(rows_by_netuid, grid_seconds=panel.grid_seconds, scope=panel.scope,
                      regime_of={d: r for d, r in panel.regime_of.items() if lo <= d < hi},
                      pit_keys=panel.pit_keys)
    out.meta = panel.meta
    return out


@dataclass
class FactorFoldRow:
    index: int
    winner: str
    n_train_dates: int
    n_oos_dates: int
    in_sample: dict[str, float]
    oos: dict[str, float]
    test_span_days: float


@dataclass
class WalkForwardReport:
    track: str
    scope: str
    mode: str
    return_basis: str
    horizon_bars: int
    n_folds: int
    n_oos_obs: int
    fold_coverage_days: float
    rows: list = field(default_factory=list)
    pooled_oos: dict[str, float] = field(default_factory=dict)
    pooled_oos_ci: BootstrapCI | None = None
    decay_ratio: dict[str, float] = field(default_factory=dict)
    oos_credible: bool = False
    gate_note: str = ""
    n_oos_folds: int = 0
    factors: list[str] = field(default_factory=list)


def _mean(xs: list[float]) -> float:
    finite = [x for x in xs if math.isfinite(x)]
    return sum(finite) / len(finite) if finite else float("nan")


def _decay(pooled: dict[str, float], per_fold: dict[str, list[float]]) -> dict[str, float]:
    """``decay[m] = pooled_OOS[m] / mean_over_folds(in_sample[m])``."""
    out: dict[str, float] = {}
    for m, num in pooled.items():
        denom = _mean(per_fold.get(m, []))
        if not math.isfinite(num) or not math.isfinite(denom) or denom == 0.0:
            out[m] = float("nan")
        else:
            out[m] = num / denom
    return out


def _coverage_days(folds: list[Fold]) -> float:
    if not folds:
        return 0.0
    return (folds[-1].test_hi - folds[0].test_lo) / DAY


def _factor_ic_stats(ic_series: list[float]) -> dict[str, float]:
    n = len(ic_series)
    if n == 0:
        return {"mean_ic": 0.0, "ic_ir": 0.0, "t_stat": 0.0}
    mean_ic = statistics.mean(ic_series)
    std_ic = statistics.stdev(ic_series) if n > 1 else 0.0
    ir = mean_ic / std_ic if std_ic > 0 else 0.0
    return {"mean_ic": mean_ic, "ic_ir": ir, "t_stat": ir * math.sqrt(n)}


def _factor_gate(*, n_folds: int, pooled: dict[str, float]) -> tuple[bool, str]:
    if n_folds < MIN_FOLDS_TO_CERTIFY:
        return False, f"insufficient folds ({n_folds} < {MIN_FOLDS_TO_CERTIFY}) — advisory only"
    t = pooled.get("t_stat", 0.0)
    if pooled.get("mean_ic", 0.0) > 0 and abs(t) >= 2.0:
        return True, f"OOS-credible (pooled OOS IC t={t:+.2f}, |t| ≥ 2)"
    return False, f"fails OOS gate: pooled OOS IC |t|={abs(t):.2f} < 2"


def walk_forward_factors(panel: Panel, factors: list[str], folds: list[Fold], *, horizon: int, lookback: int = 6,
                         min_depth: float = 3000.0, min_names: int = 10, max_depth: float = float("inf"),
                         scope: str = "", mode: str = "expanding", label: str = "ret_log",
                         death_floor: float = 0.01) -> WalkForwardReport:
    """Select the best in-sample |IC-IR| factor per train slice, score it on the disjoint test slice, pool the OOS
    per-date ICs. ``ret_log`` only: day-based labels do not self-truncate at the slice edge (13 §8.5)."""
    from .factor_backtest import run_factor_study

    if label != "ret_log":
        raise ValueError("walk-forward accepts the ret_log label only")
    rows: list[FactorFoldRow] = []
    pooled_oos_ic: list[float] = []
    is_per_fold: dict[str, list[float]] = {"mean_ic": [], "ic_ir": [], "t_stat": []}
    kw = dict(horizons=[horizon], lookback=lookback, min_depth=min_depth, min_names=min_names, max_depth=max_depth,
              death_floor=death_floor)
    for fold in folds:
        train_panel = slice_panel(panel, fold.train_lo, fold.train_hi)
        test_panel = slice_panel(panel, fold.test_lo, fold.test_hi)
        train_res = run_factor_study(train_panel, factors=factors, **kw)
        cands = [(f, abs(train_res[f][horizon].ic.ic_ir)) for f in factors if train_res[f][horizon].ic.n_dates >= 2]
        if not cands:
            zero = {"mean_ic": 0.0, "ic_ir": 0.0, "t_stat": 0.0}
            rows.append(FactorFoldRow(fold.index, "—", 0, 0, dict(zero), dict(zero),
                                      fold.n_test_bars * panel.grid_seconds / DAY))
            continue
        winner = max(cands, key=lambda x: x[1])[0]
        test_res = run_factor_study(test_panel, factors=[winner], **kw)
        is_ic = train_res[winner][horizon].ic
        oos_ic = test_res[winner][horizon].ic
        is_m = {"mean_ic": is_ic.mean_ic, "ic_ir": is_ic.ic_ir, "t_stat": is_ic.t_stat}
        oos_m = {"mean_ic": oos_ic.mean_ic, "ic_ir": oos_ic.ic_ir, "t_stat": oos_ic.t_stat}
        for k in is_per_fold:
            is_per_fold[k].append(is_m[k])
        pooled_oos_ic.extend(oos_ic.ic_series)
        rows.append(FactorFoldRow(index=fold.index, winner=winner, n_train_dates=is_ic.n_dates,
                                  n_oos_dates=oos_ic.n_dates, in_sample=is_m, oos=oos_m,
                                  test_span_days=fold.n_test_bars * panel.grid_seconds / DAY))
    pooled = _factor_ic_stats(pooled_oos_ic)
    decay = _decay(pooled, is_per_fold)
    credible, note = _factor_gate(n_folds=len(folds), pooled=pooled)
    return WalkForwardReport(track="factor", scope=scope, mode=mode, return_basis="ic", horizon_bars=horizon,
                             n_folds=len(folds), n_oos_obs=len(pooled_oos_ic), fold_coverage_days=_coverage_days(folds),
                             rows=rows, pooled_oos=pooled, pooled_oos_ci=None, decay_ratio=decay,
                             oos_credible=credible, gate_note=note,
                             n_oos_folds=sum(1 for r in rows if r.n_oos_dates >= 1), factors=list(factors))


def folds_for(panel: Panel, *, horizon: int, test_days: int, train_min_fraction: float,
              mode: str = "expanding") -> list[Fold]:
    """``train_min = max(int(len(dates)·frac), 4·horizon)``, ``test_h = test_days·86400 // grid`` (13 §8.5)."""
    n = len(panel.dates)
    if n == 0:
        return []
    train_min = max(int(n * train_min_fraction), 4 * horizon, 1)
    test_h = max(1, test_days * 86400 // panel.grid_seconds)
    return make_folds(panel.dates, train_min=train_min, test_h=test_h, mode=mode)


def _report_dict(report: WalkForwardReport) -> dict:
    d = asdict(report)
    if report.pooled_oos_ci is not None:
        d["pooled_oos_ci"] = asdict(report.pooled_oos_ci)
    return d

