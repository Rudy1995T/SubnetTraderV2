"""ts-1.0 normalisers (spec 12 §2.5, master §7.2). Pure, stdlib only, no I/O, no settings. Every function maps
``None → None``.

``NORMALISERS`` is the ts-1.0 dispatch; ``normalise_value`` wraps it with the status rules (``absent → 0.0``,
``failed | not_applicable | none → None``). ``rankg`` is applied to a cohort by the feature engine, never per row.
The P4 reference functions (``team_map`` … ``owner_overhang``) state master §7.3's formulas; spec 11 writes the
production values and they are scored through ``prenorm`` (02 §C1).
"""
from __future__ import annotations

import math
from statistics import NormalDist
from types import MappingProxyType
from typing import Any, Callable, Mapping

from app.util.mathutil import _average_ranks

_ND = NormalDist()


def _clip01(x: float) -> float:                                   # port quality.py:91-92
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x


def identity(x: float | None) -> float | None:
    return None if x is None else _clip01(float(x))


def binary(x: float | None) -> float | None:
    return None if x is None else (1.0 if x >= 0.5 else 0.0)


def recency(days: float | None, full_at: float, zero_at: float) -> float | None:   # port quality.py:113-122
    if days is None:
        return None
    if days <= full_at:
        return 1.0
    if days >= zero_at:
        return 0.0
    span = zero_at - full_at
    return _clip01(1.0 - (days - full_at) / span) if span > 0 else 0.0


def age_ramp(days: float | None, young_at: float, mature_at: float, floor: float) -> float | None:
    """Master 'recency inverted': <= young_at -> floor, >= mature_at -> 1.0, linear between."""
    if days is None:
        return None
    return floor + (1.0 - floor) * _clip01((days - young_at) / (mature_at - young_at))


def logcap(x: float | None, cap: float) -> float | None:          # OpenSSF criticality style
    if x is None:
        return None
    return _clip01(math.log1p(max(float(x), 0.0)) / math.log1p(cap))


def zclip(x: float | None, lo: float, hi: float) -> float | None:
    if x is None:
        return None
    return _clip01((float(x) - lo) / (hi - lo))


def zclip_inv(x: float | None, lo: float, hi: float) -> float | None:
    z = zclip(x, lo, hi)
    return None if z is None else 1.0 - z


def zclip_log2(ratio: float | None, lo: float, hi: float) -> float | None:
    """gate_margin: ratio <= 0.25 -> 0, 1.0 -> 2/3, >= 2 -> 1."""
    if ratio is None:
        return None
    if ratio <= 0.0:
        return 0.0
    return zclip(math.log2(ratio), lo, hi)


def checklist(x: float | None, items: Mapping[str, float | None] | None = None) -> float | None:
    if items:
        vals = [1.0 if v else 0.0 for v in items.values() if v is not None]
        return (sum(vals) / len(vals)) if vals else None
    return identity(x)


def bus_factor(k: float | None) -> float | None:
    if k is None:
        return None
    k = int(round(k))
    return 1.0 if k >= 3 else 0.6 if k == 2 else 0.3 if k == 1 else 0.0


def tenure(days: float | None, lo: float, hi: float, floor: float) -> float | None:
    z = zclip(days, lo, hi)
    return None if z is None else max(floor, z)


def event_impact(x: float | None, scale: float) -> float | None:
    """tanh(x/2) -> [-1,1] -> [0,1]; x = 0 (no events) -> 0.5 neutral."""
    return None if x is None else (math.tanh(float(x) / scale) + 1.0) / 2.0


def presence_ratio(r: float | None, members: float | None, *, low: float = 0.05, high: float = 0.35,
                   inflate_below: float = 0.02, inflate_members: float = 1000) -> float | None:
    """= spec 08's norm_presence_ratio (08 §2.9); the registry passes 08's constants as frozen params (the defaults
    are the same ts-1.0 values, kept for 08's direct callers)."""
    if r is None or members is None:
        return None
    if members > inflate_members and r < inflate_below:
        return 0.0                                                # inflation (also inflated_community flag)
    if r < low:
        return _clip01(r / low)                                   # 0 -> 0.0 ... 0.05 -> 1.0
    if r <= high:
        return 1.0                                                # healthy band 0.05-0.35
    return max(0.5, 1.0 - (r - high) / 0.7)                       # tiny / bot-heavy servers, floor 0.5


def prenorm(x: float | None) -> float | None:
    """Producer already wrote the final [0,1] value (spec 11 P4 metrics). Out of range = producer bug -> None."""
    if x is None:
        return None
    x = float(x)
    return None if (x < -1e-9 or x > 1.0 + 1e-9) else _clip01(x)


# --- Reference implementations of master §7.3 P4 (S1/S2/S3/S5 and the team map). ----------------------
# Production values are computed by spec 11 (with its extra edge rules: grace_period, OWNER_MIN_EARNED_ALPHA,
# whitelists, lock_ratio when total = 0) and scored here through `prenorm` (02 §C1). These functions are NOT in
# the ts-1.0 dispatch; they are the executable statement of the master formulas that spec 11's tests must agree
# with on the shared anchors (§9 test_normalise), and the oracle for spec 13's reconstructed P4 history. The
# `value_json` of registry rows 27, 29-32 carries their keyword arguments under the same names (§2.4).

def team_map(n_verified: float | None, n_named: int | None) -> float | None:
    if n_verified is None:
        return None
    if n_verified >= 2:
        return 1.0
    if n_verified >= 1:
        return 0.6
    return 0.3 if (n_named or 0) >= 1 else 0.0


def owner_commitment(lock_ratio: float | None, auto_lock: bool | None, commit_pct: float | None,
                     perpetual_locked: float | None, age_days: float | None) -> float | None:
    """S2: 0.60*lock_ratio + 0.25*auto_lock + 0.15*[commit_pct >= 5 %]; renormalised over known parts."""
    if lock_ratio is None:
        return None
    if perpetual_locked == 0 and auto_lock is False and age_days is not None and age_days > 90:
        return 0.0
    parts = [(0.60, _clip01(lock_ratio))]
    if auto_lock is not None:
        parts.append((0.25, 1.0 if auto_lock else 0.0))
    if commit_pct is not None:
        parts.append((0.15, 1.0 if commit_pct >= 0.05 else 0.0))
    return math.fsum(w * s for w, s in parts) / math.fsum(w for w, _ in parts)


def owner_retention(sell_ratio: float | None, max_7d_sell_impact: float | None,
                    full_exit: bool | None, owner_cut_enabled: bool | None) -> float | None:
    """S1: 1 - clip(sell_ratio); hard 0 on a full-exit extrinsic, a 7-d sell_impact > 2 % or sell_ratio > 1.
    The hard triggers are evaluated first; only then the owner_cut_enabled=False -> 1.0 shortcut (02 §C1)."""
    if (full_exit or (max_7d_sell_impact is not None and max_7d_sell_impact > 0.02)
            or (sell_ratio is not None and sell_ratio > 1.0)):
        return 0.0
    if owner_cut_enabled is False:
        return 1.0
    if sell_ratio is None:
        return None
    return 1.0 - _clip01(sell_ratio)


def control_stability(events: list[dict], as_of_ts: int) -> float:
    """S3: formal exit -> 0; swap -0.6, hotkey re-point + identity change -0.4; halves every 45 d; 90-d lookback."""
    pen = 0.0
    for e in events:
        if e.get("whitelisted"):
            continue
        age_d = (as_of_ts - int(e["event_at"])) / 86400.0
        if age_d < 0 or age_d > 90:
            continue
        if e["kind"] == "perpetual_lock_disabled":
            return 0.0
        base = {"coldkey_swap_unannounced": 0.6, "hotkey_repoint_identity_change": 0.4}.get(e["kind"], 0.0)
        pen += base * 0.5 ** (age_d / 45.0)
    return _clip01(1.0 - pen)


def owner_overhang(dump_impact: float | None, legacy_unaccounted: bool | None) -> float | None:
    """S5: < 5 % -> 1, 25 % -> 0, linear; -0.2 for pre-2026-05-08 registrations with unaccounted owner alpha."""
    z = zclip_inv(dump_impact, 0.05, 0.25)
    if z is None:
        return None
    return _clip01(z - (0.2 if legacy_unaccounted else 0.0))


def rankg(values: Mapping[str, float], *, clip_z: float = 2.5, min_n: int = 10) -> dict[str, float | None]:
    """Cross-sectional rank -> percentile (r0 + 0.5)/n -> inverse normal -> clip +/-clip_z -> [0,1].
    Ties share their average rank. Cohort smaller than min_n -> None for everyone."""
    keys = list(values)
    n = len(keys)
    if n < min_n:
        return {k: None for k in keys}
    ranks0 = _average_ranks([float(values[k]) for k in keys])
    out: dict[str, float | None] = {}
    for k, r0 in zip(keys, ranks0):
        z = _ND.inv_cdf((r0 + 0.5) / n)
        z = -clip_z if z < -clip_z else clip_z if z > clip_z else z
        out[k] = (z + clip_z) / (2.0 * clip_z)
    return out


# ── dispatch (§2.5 "Dispatch") ────────────────────────────────────────────────────────────────────────────────
NORMALISERS: Mapping[str, Callable[..., Any]] = MappingProxyType({
    "identity": identity, "binary": binary, "recency": recency, "age_ramp": age_ramp, "logcap": logcap,
    "zclip": zclip, "zclip_inv": zclip_inv, "zclip_log2": zclip_log2, "checklist": checklist,
    "bus_factor": bus_factor, "tenure": tenure, "event_impact": event_impact, "presence_ratio": presence_ratio,
    "prenorm": prenorm, "rankg": rankg,
})
# value_json inputs of the two multi-input normalisers (bound to None by the registry's import-time check)
MULTI_INPUT: Mapping[str, tuple[str, ...]] = MappingProxyType({"checklist": ("items",),
                                                                "presence_ratio": ("members",)})
STATUSES = ("ok", "absent", "failed", "not_applicable", "none")


class PrenormOutOfRange(ValueError):
    pass


def status_of(result: Any) -> str:
    """Producer-fn result → ``value_json.status`` (§2.3); anything that is not a ``MetricResult`` → ``failed``."""
    from app.features.types import MetricResult
    if not isinstance(result, MetricResult):
        return "failed"
    return result.status


def apply(name: str, value: float | None, params: Mapping[str, float], vj: Mapping[str, Any] | None) -> float | None:
    """Call normaliser ``name`` on an ``ok`` value (scalar ``f(value, **params)``; multi-input reads ``vj``)."""
    vj = vj or {}
    if name == "checklist":
        return checklist(value, items=vj.get("items"))
    if name == "presence_ratio":
        return presence_ratio(value, vj.get("members"), **params)
    if name == "rankg":
        raise ValueError("rankg is cross-sectional: snapshot() applies it to the cohort")
    if name == "prenorm" and value is not None and prenorm(value) is None:
        raise PrenormOutOfRange(f"prenorm value {value!r} outside [0,1]")
    return NORMALISERS[name](value, **params)


def normalise_value(defn: Any, value: float | None, status: str, vj: Mapping[str, Any] | None) -> float | None:
    """Status wrapper + dispatch: ``absent → 0.0``; ``failed | not_applicable | none → None``; ``ok →`` normaliser.

    Raises ``PrenormOutOfRange`` for an out-of-range ``prenorm`` value (the engine maps it to ``None`` with
    ``reason='prenorm_out_of_range'``)."""
    if status == "absent":
        return 0.0
    if status != "ok":
        return None
    return apply(defn.normaliser, value, defn.params, vj)
