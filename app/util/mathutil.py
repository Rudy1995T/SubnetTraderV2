"""Pure-stdlib math helpers (A2: the only math-util module; imported by 05, 11, 12, 13).

Port of the old SubnetTrader ``app/utils/math.py`` 1-177 (the source was not available on the
laptop at build time; re-implemented from the documented signatures and test expectations —
see specs/BUILD-LOG.md) plus ``monotone_score`` for spec 13.
"""
from __future__ import annotations

import math


def pearson_r(xs: list[float], ys: list[float]) -> float:
    """Pearson correlation; 0.0 for fewer than 2 pairs or zero variance."""
    n = min(len(xs), len(ys))
    if n < 2:
        return 0.0
    xs, ys = xs[:n], ys[:n]
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    if sxx <= 0 or syy <= 0:
        return 0.0
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    return sxy / math.sqrt(sxx * syy)


def _average_ranks(xs: list[float]) -> list[float]:
    """1-based ranks with ties sharing the average rank."""
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    ranks = [0.0] * len(xs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def spearman_r(xs: list[float], ys: list[float]) -> float:
    """Spearman rank correlation (Pearson on average ranks)."""
    n = min(len(xs), len(ys))
    if n < 2:
        return 0.0
    return pearson_r(_average_ranks(list(xs[:n])), _average_ranks(list(ys[:n])))


def zscore(xs: list[float]) -> list[float]:
    """Population z-scores; all zeros when the standard deviation is 0."""
    n = len(xs)
    if n == 0:
        return []
    m = sum(xs) / n
    var = sum((x - m) ** 2 for x in xs) / n
    if var <= 0:
        return [0.0] * n
    sd = math.sqrt(var)
    return [(x - m) / sd for x in xs]


def quantile_buckets(values: list[float], n: int = 10) -> list[int]:
    """Assign each value a bucket 0..n-1 by rank (equal-count buckets, ties by position)."""
    m = len(values)
    if m == 0:
        return []
    order = sorted(range(m), key=lambda i: values[i])
    out = [0] * m
    for pos, idx in enumerate(order):
        out[idx] = min(n - 1, pos * n // m)
    return out


def log_returns(prices: list[float]) -> list[float]:
    """ln(p_t / p_{t-1}); pairs with a non-positive price are skipped."""
    out = []
    for a, b in zip(prices, prices[1:]):
        if a and b and a > 0 and b > 0:
            out.append(math.log(b / a))
    return out


def returns_pearson_r(xs: list[float], ys: list[float]) -> float:
    """Pearson correlation of the log-return series of two aligned price series."""
    n = min(len(xs), len(ys))
    rx, ry = [], []
    for i in range(1, n):
        a0, a1, b0, b1 = xs[i - 1], xs[i], ys[i - 1], ys[i]
        if a0 > 0 and a1 > 0 and b0 > 0 and b1 > 0:
            rx.append(math.log(a1 / a0))
            ry.append(math.log(b1 / b0))
    return pearson_r(rx, ry)


def effective_bets(avg_corr: float, n: int) -> float:
    """Effective number of independent bets: n / (1 + (n-1)·ρ̄), clamped to [1, n]."""
    if n <= 0:
        return 0.0
    rho = max(0.0, min(1.0, avg_corr))
    eff = n / (1.0 + (n - 1) * rho)
    return max(1.0, min(float(n), eff))


def rolling_volatility(prices: list[float], window: int = 24) -> float | None:
    """Sample standard deviation of the last ``window`` log returns; None when too few."""
    rets = log_returns(prices[-(window + 1):])
    if len(rets) < 2:
        return None
    m = sum(rets) / len(rets)
    var = sum((r - m) ** 2 for r in rets) / (len(rets) - 1)
    return math.sqrt(var)


def gini_coefficient(values: list[float]) -> float:
    """Gini of the positive values (non-positive ignored); 0.0 for empty or all-equal input."""
    xs = sorted(v for v in values if v is not None and v > 0)
    n = len(xs)
    if n == 0:
        return 0.0
    total = sum(xs)
    if total <= 0:
        return 0.0
    cum = sum((i + 1) * x for i, x in enumerate(xs))
    return (2.0 * cum) / (n * total) - (n + 1.0) / n


def monotone_score(curve: list[float | None]) -> float:
    """Spearman of position vs value over the non-None entries; fewer than 5 points → 0.0."""
    pts = [(i, v) for i, v in enumerate(curve) if v is not None]
    if len(pts) < 5:
        return 0.0
    return spearman_r([float(i) for i, _ in pts], [float(v) for _, v in pts])
