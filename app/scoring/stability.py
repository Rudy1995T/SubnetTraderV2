"""Rank-stability monitors (spec 12 §2.12; master §7.7): after each official primary run, for series ``health``
(= ``scores.quality``) and ``composite_smoothed`` at lags 1 and 7 days — Spearman rank autocorrelation, quintile
turnover and top-20 turnover over the occupants present at both slots. Inputs are sorted by netuid first (the ported
``quantile_buckets`` breaks ties by position)."""
from __future__ import annotations

from typing import TYPE_CHECKING

from app.util.mathutil import quantile_buckets, spearman_r

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

DAY = 86400
SERIES = {"health": "quality", "composite_smoothed": "composite_smoothed"}
LAGS = (1, 7)
MIN_N = 10


async def _series(db: "Database", spec_version: str, as_of: int, col: str) -> dict[str, tuple[int, float]]:
    rows = await db.fetchall(
        f"SELECT occupant_key, netuid, {col} AS v FROM scores WHERE spec_version = ? AND as_of = ? "
        f"AND run_kind = 'official' AND status = 'active' AND insufficient_data = 0 AND {col} IS NOT NULL",
        (spec_version, int(as_of)))
    return {r["occupant_key"]: (int(r["netuid"]), float(r["v"])) for r in rows}


def stats(now: dict[str, tuple[int, float]], before: dict[str, tuple[int, float]]) -> dict:
    common = sorted(set(now) & set(before), key=lambda k: (now[k][0], k))
    n = len(common)
    if n < MIN_N:
        return {"n": n, "rank_autocorr": None, "quintile_turnover": None, "top20_turnover": None}
    a = [now[k][1] for k in common]
    b = [before[k][1] for k in common]
    qa, qb = quantile_buckets(a, 5), quantile_buckets(b, 5)
    k20 = min(20, n)
    top = lambda vals: {common[i] for i in sorted(range(n), key=lambda i: (-vals[i], now[common[i]][0]))[:k20]}  # noqa: E731
    return {"n": n, "rank_autocorr": spearman_r(a, b),
            "quintile_turnover": sum(x != y for x, y in zip(qa, qb)) / n,
            "top20_turnover": len(top(a) - top(b)) / k20}


async def rank_autocorr_health(db: "Database", spec_version: str, slot: int) -> list[dict]:
    """One dict per (series, lag) with 03's ``rank_stability_daily`` columns (``computed_at`` added by the caller)."""
    out = []
    for series, col in SERIES.items():
        now = await _series(db, spec_version, slot, col)
        for lag in LAGS:
            before = await _series(db, spec_version, slot - lag * DAY, col)
            out.append({"as_of": int(slot), "spec_version": spec_version, "series": series, "lag_days": lag,
                        **stats(now, before)})
    return out
