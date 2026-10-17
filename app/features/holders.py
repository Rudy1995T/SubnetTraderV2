"""Holder concentration over the fetched top-N (spec 05 §2.6)."""
from __future__ import annotations

from dataclasses import dataclass

from app.util.mathutil import gini_coefficient


@dataclass(frozen=True)
class HolderStats:
    top1_share: float | None
    top10_share: float | None
    hhi_topn: float | None
    gini_topn: float | None
    n_fetched: int
    holders_count: int | None


def concentration(rows: list[dict], count: int | None) -> HolderStats:
    rows = sorted((r for r in rows if r.get("percentage") is not None), key=lambda r: r.get("rank") or 10**9)
    if not rows:
        return HolderStats(None, None, None, None, 0, count)
    pct = [float(r["percentage"]) / 100.0 for r in rows]
    top1 = pct[0]
    top10 = sum(p for r, p in zip(rows, pct) if (r.get("rank") or 10**9) <= 10) if any(r.get("rank") for r in rows) \
        else sum(pct[:10])
    hhi = sum(p * p for p in pct)
    gini = gini_coefficient([float(r.get("total_alpha") or 0) for r in rows])
    return HolderStats(top1, top10, hhi, gini, len(rows), count)
