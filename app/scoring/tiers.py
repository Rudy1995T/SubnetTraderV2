"""Tiers with ±3 hysteresis and the span-7 EMA (spec 12 §2.9). Pure."""
from __future__ import annotations

from typing import Mapping

ORDER = ("F", "D", "C", "B", "A")
LOWER = {"A": 75.0, "B": 60.0, "C": 45.0, "D": 30.0, "F": float("-inf")}


def assign_tier(s: float | None, prev: str | None, h: float = 3.0,
                lower: Mapping[str, float] | None = None) -> str | None:
    """A ≥ 75, B 60–74, C 45–59, D 30–44, F < 30; enter a higher tier at ≥ threshold + h, leave at ≤ threshold − h."""
    lo = LOWER if lower is None else {**lower, "F": float("-inf")}
    if s is None:
        return None
    if prev is None:                                                   # seed: plain thresholds
        return next((t for t in ("A", "B", "C", "D") if s >= lo[t]), "F")
    ip = ORDER.index(prev)
    up = max(i for i, t in enumerate(ORDER) if s >= lo[t] + h)        # enter a higher tier at >= threshold + 3
    if up > ip:
        return ORDER[up]
    if prev != "F" and s <= lo[prev] - h:                             # leave at <= threshold - 3
        return ORDER[max(i for i, t in enumerate(ORDER) if i <= ip and s > lo[t] - h)]
    return prev


def ema(composite: float | None, prev_smoothed: float | None, span: int = 7) -> float | None:
    """``α·composite + (1−α)·prev`` with α = 2/(span+1); seeds with ``composite`` when there is no ``prev``."""
    if composite is None:
        return None
    if prev_smoothed is None:
        return composite
    a = 2.0 / (span + 1.0)
    return a * composite + (1.0 - a) * prev_smoothed
