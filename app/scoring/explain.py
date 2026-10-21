"""Explain JSON (spec 12 §2.10, rendered verbatim by 14) and metric impacts. Pure.

Impact of metric ``i`` in Health pillar ``k ∈ H``: ``100·(W_k/Σ_H W)·(w_i/A_k)·(m_i − 0.5)`` (``A_k`` = available
within-pillar weight), so ``Σ impact = Health − 50``. P6 metrics carry ``100·v6·(w_i/A_6)·(m_i − 0.5)`` and stay out
of the top lists.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:  # pragma: no cover
    from app.features.types import NormValue
    from app.scoring.spec import ScoringSpec

HEALTH_PILLARS = ("P1", "P2", "P3", "P4", "P5")
ALL_PILLARS = (*HEALTH_PILLARS, "P6")


def r6(x: float | None) -> float | None:
    return None if x is None else round(float(x), 6)


def impacts(spec: "ScoringSpec", snap_o: Mapping[str, "NormValue"], pillars: Mapping[str, tuple],
            health_set: tuple[str, ...]) -> dict[str, float | None]:
    """metric_key → impact (None when m is None or its pillar is not in the Health set / P6 has no score)."""
    w_sum = sum(spec.pillar_weights[k] for k in health_set)
    out: dict[str, float | None] = {}
    for key, d in spec.metrics.items():
        if d.role != "scored":
            continue
        nv = snap_o.get(key)
        m = None if nv is None else nv.m
        score, _cov, avail = pillars.get(d.pillar, (None, 0.0, 0.0))
        if m is None or score is None or not avail:
            out[key] = None
        elif d.pillar == "P6":
            out[key] = 100.0 * spec.velocity_weights[0] * (d.weight / avail) * (m - 0.5)
        elif d.pillar in health_set and w_sum > 0:
            out[key] = 100.0 * (spec.pillar_weights[d.pillar] / w_sum) * (d.weight / avail) * (m - 0.5)
        else:
            out[key] = None
    return out


def top_lists(spec: "ScoringSpec", imp: Mapping[str, float | None]) -> tuple[list[dict], list[dict]]:
    hp = [(k, v) for k, v in imp.items() if v is not None and spec.metrics[k].pillar != "P6"]
    pos = sorted(((k, v) for k, v in hp if v > 0), key=lambda kv: (-kv[1], kv[0]))[:3]
    neg = sorted(((k, v) for k, v in hp if v < 0), key=lambda kv: (kv[1], kv[0]))[:3]
    f = lambda kv: {"metric": kv[0], "pillar": spec.metrics[kv[0]].pillar, "impact": r6(kv[1])}  # noqa: E731
    return [f(x) for x in pos], [f(x) for x in neg]


def build_explain(*, spec: "ScoringSpec", spec_hash: str, as_of: int, run_kind: str, mode: str, status: str,
                  regime: Any, v_w: float, calc: Any, snap_o: Mapping[str, "NormValue"], flags: list[dict],
                  prev: Mapping | None, tier: str | None, composite: float | None,
                  composite_smoothed: float | None, flag_multiplier: float,
                  imp: Mapping[str, float | None]) -> dict:
    top_pos, top_neg = top_lists(spec, imp)
    w_sum = sum(spec.pillar_weights[k] for k in calc.health_set)
    pillars: dict[str, dict] = {}
    for p in ALL_PILLARS:
        score, cov, _avail = calc.pillars.get(p, (None, None, 0.0))
        w = spec.pillar_weights.get(p)
        block: dict[str, Any] = {"score": r6(score), "weight": w,
                                 "points": r6(100.0 * w * score / w_sum) if (w and score is not None and
                                                                             p in calc.health_set and w_sum) else None}
        if run_kind == "official":
            ms = {}
            for d in (d for d in spec.metrics.values() if d.role == "scored" and d.pillar == p):
                nv = snap_o[d.key]
                e = {"m": r6(nv.m), "raw": r6(nv.raw) if isinstance(nv.raw, (int, float)) else nv.raw, "w": d.weight,
                     "st": nv.status or "none", "age_s": nv.age_s, "impact": r6(imp.get(d.key))}
                if d.cross_sectional:
                    e["cohort_n"] = nv.cohort_n
                if nv.reason:
                    e["reason"] = nv.reason
                if nv.fallback:
                    e["fallback"] = True
                ms[d.key] = e
            block["metrics"] = ms
        pillars[p] = block
    rm = regime.metrics or {}
    return {
        "v": 1, "spec_version": spec.version, "spec_hash": spec_hash, "as_of": int(as_of), "run_kind": run_kind,
        "mode": mode, "status": status,
        "regime": {"name": regime.label, "raw": regime.raw_regime, "vol_24h": rm.get("vol_24h"),
                   "directional_strength": rm.get("directional_strength"), "dispersion": rm.get("dispersion"),
                   "n_subnets": rm.get("n_subnets")},
        "v_w": v_w, "health": r6(calc.health),
        "velocity": {"value": r6(calc.velocity), "P6": r6(calc.pillars.get("P6", (None,))[0]),
                     "blend": r6(calc.blend), "blend_parts": {k: r6(v) for k, v in calc.blend_parts.items()}},
        "composite_raw": r6(calc.composite_raw), "flag_multiplier": r6(flag_multiplier), "composite": r6(composite),
        "composite_smoothed": r6(composite_smoothed),
        "prev": None if prev is None else {"as_of": prev["as_of"], "composite_smoothed": r6(prev["composite_smoothed"]),
                                           "tier": prev["tier"]},
        "tier": tier, "confidence": r6(calc.confidence), "insufficient_data": bool(calc.insufficient),
        "coverage": {p: r6(calc.pillars[p][1]) if p in calc.pillars and calc.pillars[p][1] is not None else None
                     for p in ALL_PILLARS},
        "pillars": pillars, "top_positive": top_pos, "top_negative": top_neg, "flags": flags,
        "missing": sorted(k for k, d in spec.metrics.items() if d.role == "scored" and snap_o[k].m is None),
    }
