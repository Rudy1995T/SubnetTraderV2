"""Pure-stdlib math helpers (A2: the only math-util module; imported by 05, 11, 12, 13, 19).

Verbatim port of the old SubnetTrader ``app/utils/math.py`` 1-177 (source: ``seed/app/utils/math.py``; spec 12 §8.1,
§8.2). Phase 2 had re-implemented it without the source; Phase 8 replaced that with the original functions
(0-based ``_average_ranks``, ``pearson_r``/``spearman_r`` 0.0 below n = 5, tail alignment, annualised
``rolling_volatility``). ``compute_price_changes`` (178-239, wall-clock dependent) is not ported. Spec 13 appends
``monotone_score``.
"""
from __future__ import annotations

import math
import statistics


def pearson_r(xs: list[float], ys: list[float]) -> float:
    """Pearson correlation coefficient between two equal-length price series.

    Returns 0.0 if series are too short or have zero variance.
    """
    n = min(len(xs), len(ys))
    if n < 5:
        return 0.0
    xs, ys = xs[-n:], ys[-n:]
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    da = math.sqrt(sum((x - mx) ** 2 for x in xs))
    db = math.sqrt(sum((y - my) ** 2 for y in ys))
    if da == 0 or db == 0:
        return 0.0
    return num / (da * db)


def _average_ranks(xs: list[float]) -> list[float]:
    """0-based ranks with ties sharing their average rank (the rank a Spearman
    correlation expects)."""
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    ranks = [0.0] * len(xs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        shared = (i + j) / 2.0
        for k in range(i, j + 1):
            ranks[order[k]] = shared
        i = j + 1
    return ranks


def spearman_r(xs: list[float], ys: list[float]) -> float:
    """Spearman rank correlation — Pearson r on the average-ranked inputs.

    Returns 0.0 for series shorter than 5 (delegated to ``pearson_r``) or with
    no rank variance (all tied).
    """
    n = min(len(xs), len(ys))
    if n < 5:
        return 0.0
    return pearson_r(_average_ranks(xs[:n]), _average_ranks(ys[:n]))


def zscore(xs: list[float]) -> list[float]:
    """Standardize a series to zero mean / unit (sample) std.

    Returns all-zeros for length < 2 or zero variance.
    """
    n = len(xs)
    if n < 2:
        return [0.0] * n
    m = sum(xs) / n
    var = sum((x - m) ** 2 for x in xs) / (n - 1)
    sd = math.sqrt(var)
    if sd == 0:
        return [0.0] * n
    return [(x - m) / sd for x in xs]


def quantile_buckets(values: list[float], n: int = 10) -> list[int]:
    """Assign each value an equal-count bucket index in ``[0, n-1]`` by rank.

    Lowest values land in bucket 0, highest in bucket ``n-1``. Ties are broken
    by position (stable sort), so a heavily-tied series still spreads across
    buckets without crashing. Robust to ``len(values) < n``.
    """
    m = len(values)
    if m == 0:
        return []
    order = sorted(range(m), key=lambda i: values[i])
    buckets = [0] * m
    for rank, idx in enumerate(order):
        b = rank * n // m
        buckets[idx] = b if b < n else n - 1
    return buckets


def log_returns(prices: list[float]) -> list[float]:
    """Log-returns of a chronological price series (drops non-positive steps)."""
    return [
        math.log(prices[i] / prices[i - 1])
        for i in range(1, len(prices))
        if prices[i - 1] > 0 and prices[i] > 0
    ]


def returns_pearson_r(xs: list[float], ys: list[float]) -> float:
    """Pearson r of two price series' log-returns (0.0 if too short).

    Returns are the statistically correct input for "do these move together":
    the level correlation of two independently-trending series is spuriously
    high. The two series are tail-aligned to a common length *before*
    differencing so their return streams stay time-aligned.
    """
    n = min(len(xs), len(ys))
    if n < 6:  # need >= 5 returns, i.e. >= 6 prices
        return 0.0
    rx, ry = log_returns(xs[-n:]), log_returns(ys[-n:])
    m = min(len(rx), len(ry))
    if m < 5:
        return 0.0
    return pearson_r(rx[-m:], ry[-m:])


def effective_bets(avg_corr: float, n: int) -> float:
    """Effective number of independent bets under an equicorrelation model.

    ENB = n / (1 + (n-1)*avg_corr). avg_corr=0 -> n (fully diversified);
    avg_corr=1 -> 1 (one bet). A maximally-hedged book (avg_corr negative enough
    that the denominator <= 0) is more diversified than n independent bets, so
    return +inf rather than dividing by zero or going negative.
    """
    if n <= 1:
        return float(n)
    denom = 1.0 + (n - 1) * avg_corr
    if denom <= 0:
        return float("inf")
    return n / denom


def rolling_volatility(prices: list[float], window: int = 24) -> float | None:
    """Annualized volatility from a price series.

    Args:
        prices: chronological prices (e.g. 4h candles over 7 days = ~42 points)
        window: lookback window in data points (default 24 = ~4 days at 4h)

    Returns:
        Annualized volatility as a decimal (e.g. 0.45 = 45%), or None if
        insufficient data.
    """
    if len(prices) < window + 1:
        return None
    recent = prices[-window - 1:]
    log_returns = [
        math.log(recent[i] / recent[i - 1])
        for i in range(1, len(recent))
        if recent[i - 1] > 0
    ]
    if len(log_returns) < 5:
        return None
    std_dev = statistics.stdev(log_returns)
    # Annualize: assume 4h candles → 6 per day → 2190 per year
    return std_dev * math.sqrt(2190)


def gini_coefficient(values: list[float]) -> float:
    """Gini coefficient (0 = equal, 1 = one holder dominates).

    Expects a list of positive stake/balance values.
    """
    vals = sorted(v for v in values if v > 0)
    n = len(vals)
    if n == 0:
        return 0.0
    total = sum(vals)
    if total == 0:
        return 0.0
    cum = 0.0
    gini_sum = 0.0
    for i, v in enumerate(vals):
        cum += v
        gini_sum += (2 * (i + 1) - n - 1) * v
    return gini_sum / (n * total)


def monotone_score(curve: list[float | None]) -> float:
    """Spearman of position vs value over the non-None entries; fewer than 5 points → 0.0 (spec 13)."""
    pts = [(i, v) for i, v in enumerate(curve) if v is not None]
    if len(pts) < 5:
        return 0.0
    return spearman_r([float(i) for i, _ in pts], [float(v) for _, v in pts])
