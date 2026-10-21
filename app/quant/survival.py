"""Tercile survival study — master §10 criterion 6 (spec 13 §8.5).

Port of the old SubnetTrader ``app/backtest/quality_band.py:32-140`` (``TercileResult``, ``_percentile``, the pooling
loop, the ``death_ratio_low_over_high`` semantics) with an injected factor and the ``death_90d`` label, terciles
assigned per date cross-sectionally, distinct-death counting and a dereg-only diagnostic.
"""
from __future__ import annotations

import math
import statistics
from dataclasses import dataclass, field

from app.util.mathutil import quantile_buckets
from app.util.time import floor_day

from .factor_panel import Panel, forward_return
from .labels import DEREG_KINDS, LabelSource

INF = float("inf")
_LABELS = ("low", "mid", "high")


@dataclass
class TercileResult:
    label: str
    q_lo: float
    q_hi: float
    n_names: int
    n_obs: int
    n_deaths: int
    death_rate: float
    p5: float
    mean_neg: float
    mean_ret: float
    n_deaths_dereg_only: int = 0
    death_rate_dereg_only: float = float("nan")


@dataclass
class SurvivalResult:
    scope: str
    factor: str
    horizon_days: int
    step_days: int
    min_depth: float
    n_names: int
    n_obs: int
    n_events: int
    n_event_dates: int
    n_events_by_kind: dict[str, int]
    n_censored: int
    terciles: list[TercileResult] = field(default_factory=list)
    death_ratio_low_over_high: float = float("nan")
    n_events_dereg_only: int = 0
    death_ratio_dereg_only: float = float("nan")
    max_depth: float = INF
    n_dates: int = 0


def _percentile(sorted_xs: list[float], p: float) -> float:
    """Linear-interpolated p-th percentile of an already-sorted list (ported)."""
    if not sorted_xs:
        return float("nan")
    if len(sorted_xs) == 1:
        return sorted_xs[0]
    rank = (p / 100.0) * (len(sorted_xs) - 1)
    lo = int(rank)
    frac = rank - lo
    if lo + 1 >= len(sorted_xs):
        return sorted_xs[-1]
    return sorted_xs[lo] + frac * (sorted_xs[lo + 1] - sorted_xs[lo])


def ratio_low_over_high(terciles: list[TercileResult], attr: str = "death_rate") -> float:
    """Bottom ÷ top death rate (ported ``quality_band.py:59-69``): +inf when only the top is event-free, 0.0 when
    both are, NaN when a tercile is empty."""
    lo = next((t for t in terciles if t.label == "low"), None)
    hi = next((t for t in terciles if t.label == "high"), None)
    if lo is None or hi is None or lo.n_obs == 0 or hi.n_obs == 0:
        return float("nan")
    a, b = getattr(lo, attr), getattr(hi, attr)
    if b == 0:
        return INF if a > 0 else 0.0
    return a / b


def survival_study(panel: Panel, *, factor_fn, label_source: LabelSource, horizon_days: int = 90,
                   step_days: int = 90, min_depth: float = 0.0, max_depth: float = INF, n_buckets: int = 3,
                   factor: str = "", lookback: int = 6) -> SurvivalResult:
    step = max(1, step_days * 86400 // panel.grid_seconds)
    h_bars = max(1, horizon_days * 86400 // panel.grid_seconds)
    # per obs: (tercile, nu, factor, death, event, death_dereg, event_dereg, fwd)
    obs: list[tuple] = []
    n_censored = 0
    n_dates = 0
    for t in panel.dates[::step]:
        cands = []
        for nu in sorted(panel.series):
            bars = panel.series[nu]
            i = panel.index_of[nu].get(t)
            if i is None or bars[i].depth < min_depth or bars[i].depth > max_depth:
                continue
            fv = factor_fn(bars, i, lookback)
            if fv is None:
                continue
            d, ev = label_source.death_event(panel, nu, i, horizon_days)
            if d is None:
                n_censored += 1
                continue
            dd, evd = label_source.death_event(panel, nu, i, horizon_days, dereg_only=True)
            cands.append((nu, float(fv), d, ev, dd or 0, evd, forward_return(panel, nu, i, h_bars)))
        if len(cands) < n_buckets:
            continue
        n_dates += 1
        buckets = quantile_buckets([c[1] for c in cands], n_buckets)
        for b, c in zip(buckets, cands):
            obs.append((b, *c))
    terciles: list[TercileResult] = []
    labels = _LABELS if n_buckets == 3 else tuple(str(k) for k in range(n_buckets))
    for idx, lab in enumerate(labels):
        rows = [o for o in obs if o[0] == idx]
        if not rows:
            nan = float("nan")
            terciles.append(TercileResult(lab, nan, nan, 0, 0, 0, nan, nan, nan, nan))
            continue
        qs = [o[2] for o in rows]
        frs = sorted(o[7] for o in rows if o[7] is not None)
        negs = [x for x in frs if x < 0]
        n_d = sum(o[3] for o in rows)
        n_dd = sum(o[5] for o in rows)
        terciles.append(TercileResult(
            label=lab, q_lo=min(qs), q_hi=max(qs), n_names=len({o[1] for o in rows}), n_obs=len(rows), n_deaths=n_d,
            death_rate=n_d / len(rows), p5=_percentile(frs, 5.0),
            mean_neg=statistics.mean(negs) if negs else float("nan"),
            mean_ret=statistics.mean(frs) if frs else float("nan"),
            n_deaths_dereg_only=n_dd, death_rate_dereg_only=n_dd / len(rows)))
    events = {o[4].death_id: o[4] for o in obs if o[4] is not None}
    events_dd = {o[6].death_id for o in obs if o[6] is not None and o[6].kind in DEREG_KINDS}
    by_kind: dict[str, int] = {}
    for e in events.values():
        by_kind[e.kind] = by_kind.get(e.kind, 0) + 1
    return SurvivalResult(
        scope=panel.scope, factor=factor, horizon_days=horizon_days, step_days=step_days, min_depth=min_depth,
        n_names=len({o[1] for o in obs}), n_obs=len(obs), n_events=len(events),
        n_event_dates=len({floor_day(e.onset) for e in events.values()}),
        n_events_by_kind=dict(sorted(by_kind.items())),
        n_censored=n_censored, terciles=terciles, death_ratio_low_over_high=ratio_low_over_high(terciles),
        n_events_dereg_only=len(events_dd),
        death_ratio_dereg_only=ratio_low_over_high(terciles, "death_rate_dereg_only"), max_depth=max_depth,
        n_dates=n_dates)


def is_inf(x: float) -> bool:
    return isinstance(x, float) and math.isinf(x) and x > 0
