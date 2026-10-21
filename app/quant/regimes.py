"""Regime tags per cross-section date (spec 13 §2.6) — diagnostic only, never a factor."""
from __future__ import annotations

import bisect
import sqlite3

from app.util.time import floor_day, parse_iso

UNKNOWN = "UNKNOWN"


def parse_boundaries(spec: str) -> list[tuple[int, str]]:
    """``"taoflow:2025-09-22,price_burn:2026-06-25,…"`` → ascending ``[(unix, name), …]``."""
    out = []
    for part in (p.strip() for p in spec.split(",") if p.strip()):
        name, _, date = part.partition(":")
        out.append((parse_iso(f"{date.strip()}T00:00:00Z"), name.strip()))
    out.sort()
    return out


def emission_regime_at(t: int, boundaries: list[tuple[int, str]]) -> str:
    """Name of the last boundary ≤ t; ``pre_<first>`` before the first one."""
    if not boundaries:
        return UNKNOWN
    j = bisect.bisect_right([b[0] for b in boundaries], t) - 1
    return boundaries[j][1] if j >= 0 else f"pre_{boundaries[0][1]}"


def load_market_regimes(con: sqlite3.Connection, spec_version: str) -> dict[int, tuple[str, str]]:
    """``{day_ts: (label, source)}`` from ``regime_daily`` for the run's spec version (02 §B8 vocabulary)."""
    return {int(d): (str(lbl).upper(), str(src)) for d, lbl, src in con.execute(
        "SELECT day_ts, label, source FROM regime_daily WHERE spec_version = ?", (spec_version,))}


def regime_of_dates(dates: list[int], boundaries: list[tuple[int, str]],
                    market: dict[int, tuple[str, str]]) -> tuple[dict[int, dict[str, str]], float]:
    """Per date ``{"emission", "market", "market_source"}`` and the share of dates with a ``regime_daily`` row."""
    out: dict[int, dict[str, str]] = {}
    hit = 0
    for t in dates:
        m = market.get(floor_day(t))
        if m is not None:
            hit += 1
        out[t] = {"emission": emission_regime_at(t, boundaries), "market": m[0] if m else UNKNOWN,
                  "market_source": m[1] if m else "missing"}
    return out, (hit / len(dates) if dates else 0.0)
