"""Offline rank stability (spec 13 §8.5) — 12 §2.12's statistics on snapshot panels (master §7.7 target:
``mean_rank_autocorr ≥ 0.9`` for ``quality`` on the 24 h grid at lag 1)."""
from __future__ import annotations

import statistics
from dataclasses import dataclass, field

from app.util.mathutil import quantile_buckets, spearman_r

from .factor_panel import Panel


@dataclass
class RankStabilityResult:
    scope: str
    factor: str
    lag: int
    n_pairs: int
    mean_rank_autocorr: float | None
    median_rank_autocorr: float | None
    mean_quintile_turnover: float | None
    mean_top20_turnover: float | None
    series: list[tuple[int, int, float | None, float | None, float | None]] = field(default_factory=list)


def _top(names: list[int], vals: list[float], k: int) -> set[int]:
    order = sorted(range(len(names)), key=lambda j: (-vals[j], names[j]))
    return {names[j] for j in order[:k]}


def rank_stability(panel: Panel, factor_fn, *, lag: int = 1, lookback: int = 6, min_depth: float = 0.0,
                   min_names: int = 10, factor: str = "") -> RankStabilityResult:
    """For each date pair ``(t−lag, t)``: names with a non-None factor (and ``depth ≥ min_depth``) on both, sorted by
    netuid; ``n < min_names`` → nulls."""
    dates = panel.dates
    netuids = sorted(panel.series)

    def values_at(t: int) -> dict[int, float]:
        out = {}
        for nu in netuids:
            bars = panel.series[nu]
            i = panel.index_of[nu].get(t)
            if i is None or bars[i].depth < min_depth:
                continue
            v = factor_fn(bars, i, lookback)
            if v is not None:
                out[nu] = float(v)
        return out

    cache: dict[int, dict[int, float]] = {}
    series = []
    for j in range(lag, len(dates)):
        t0, t1 = dates[j - lag], dates[j]
        for t in (t0, t1):
            if t not in cache:
                cache[t] = values_at(t)
        a, b = cache[t0], cache[t1]
        if j - lag - 1 >= 0:
            cache.pop(dates[j - lag - 1], None)          # keep the window cache small
        names = sorted(set(a) & set(b))
        n = len(names)
        if n < min_names:
            series.append((t1, n, None, None, None))
            continue
        fa = [a[x] for x in names]
        fb = [b[x] for x in names]
        ac = spearman_r(fb, fa)
        qa, qb = quantile_buckets(fa, 5), quantile_buckets(fb, 5)
        q_to = sum(1 for x, y in zip(qa, qb) if x != y) / n
        k = min(20, n)
        t_to = len(_top(names, fb, k) - _top(names, fa, k)) / k
        series.append((t1, n, ac, q_to, t_to))
    acs = [s[2] for s in series if s[2] is not None]
    qts = [s[3] for s in series if s[3] is not None]
    tts = [s[4] for s in series if s[4] is not None]
    return RankStabilityResult(
        scope=panel.scope, factor=factor, lag=lag, n_pairs=len(acs),
        mean_rank_autocorr=statistics.mean(acs) if acs else None,
        median_rank_autocorr=statistics.median(acs) if acs else None,
        mean_quintile_turnover=statistics.mean(qts) if qts else None,
        mean_top20_turnover=statistics.mean(tts) if tts else None, series=series)
