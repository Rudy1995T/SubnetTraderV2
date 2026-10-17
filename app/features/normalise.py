"""Master §7.2 normalisers (Phase-2 subset; spec 12 owns this module from Phase 8). ``None`` passes through."""
from __future__ import annotations

import math
from statistics import NormalDist


def _clip01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x


def recency(days: float | None, full_at: float, zero_at: float) -> float | None:
    if days is None:
        return None
    if days <= full_at:
        return 1.0
    if days >= zero_at:
        return 0.0
    return 1.0 - (days - full_at) / (zero_at - full_at)


def logcap(x: float | None, cap: float) -> float | None:
    if x is None:
        return None
    return _clip01(math.log1p(max(0.0, x)) / math.log1p(cap))


def zclip(x: float | None, lo: float, hi: float) -> float | None:
    if x is None:
        return None
    return _clip01((x - lo) / (hi - lo))


def checklist(items: list[bool | None]) -> float | None:
    vals = [bool(i) for i in items if i is not None]
    return sum(vals) / len(vals) if vals else None


def rankg(values: dict[str, float | None], clip_sigma: float = 2.5) -> dict[str, float | None]:
    """Cross-sectional rank → percentile → inverse normal → clip ±σ → [0,1]; None stays None."""
    present = {k: v for k, v in values.items() if v is not None}
    n = len(present)
    out: dict[str, float | None] = {k: None for k in values}
    if n == 0:
        return out
    if n == 1:
        out.update({k: 0.5 for k in present})
        return out
    order = sorted(present, key=lambda k: present[k])
    ranks: dict[str, float] = {}
    i = 0
    while i < n:
        j = i
        while j + 1 < n and present[order[j + 1]] == present[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in order[i:j + 1]:
            ranks[k] = avg
        i = j + 1
    nd = NormalDist()
    for k, r in ranks.items():
        p = (r - 0.5) / n
        z = max(-clip_sigma, min(clip_sigma, nd.inv_cdf(p)))
        out[k] = (z + clip_sigma) / (2 * clip_sigma)
    return out
