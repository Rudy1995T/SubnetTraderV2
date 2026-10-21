"""The two derived P5 metrics (spec 12 §2.6; 02 §C3): ``announcement_cadence_30d`` and ``silence_days`` from the
selected C3 leg rows. Pure — the engine selects the legs (each with its own freshness) and writes the results.
08 and 09 register no producer for these keys."""
from __future__ import annotations

from typing import Mapping

from app.features.types import MetricResult, RawRow

CADENCE_LEGS = ("x_cadence_30d", "discord_announcements_30d", "tg_posts_30d")
SILENCE_LEGS = ("x_last_activity_days", "discord_last_activity_days", "tg_last_activity_days")
X_LEGS = ("x_last_activity_days", "x_cadence_30d")


def _leg_view(legs: Mapping[str, RawRow | None], keys: tuple[str, ...]) -> dict:
    out: dict = {}
    for k in keys:
        r = legs.get(k)
        out[k] = "missing" if r is None else (r.value if r.status == "ok" else r.status)
    return out


def derive_announcement_cadence(legs: Mapping[str, RawRow | None]) -> MetricResult:
    """≥ 1 ``ok`` leg → Σ ok values (absent legs add 0, failed/missing ignored); else failed ``no_ok_leg``."""
    ok = [legs[k] for k in CADENCE_LEGS if legs.get(k) is not None and legs[k].status == "ok"
          and legs[k].value is not None]
    vj = {"legs": _leg_view(legs, CADENCE_LEGS)}
    if not ok:
        return MetricResult(None, {**vj, "reason": "no_ok_leg"})
    return MetricResult(float(sum(r.value for r in ok)), vj)


def x_absent(legs: Mapping[str, RawRow | None], has_x_channel: bool) -> bool:
    """02 §C3 "no X account": an X leg row is ``absent`` (09's no_x_account / x_account_gone) or no team handle."""
    if any(legs.get(k) is not None and legs[k].status == "absent" for k in X_LEGS):
        return True
    return not has_x_channel


def derive_silence_days(legs: Mapping[str, RawRow | None], venue: RawRow | None, x_is_absent: bool) -> MetricResult:
    """min over exact ok legs, else min over lower-bound legs (``lower_bound=true``); else absent only when no venue is
    reachable ∧ no X account (→ 0.0, ``no_reachable_channel``); otherwise failed ``no_ok_leg``."""
    view = _leg_view(legs, SILENCE_LEGS)
    ok = [(k, legs[k]) for k in SILENCE_LEGS if legs.get(k) is not None and legs[k].status == "ok"
          and legs[k].value is not None]
    exact = [(k, r) for k, r in ok if not (r.value_json or {}).get("lower_bound")]
    bound = [(k, r) for k, r in ok if (r.value_json or {}).get("lower_bound")]
    for group, lb in ((exact, False), (bound, True)):
        if group:
            k, r = min(group, key=lambda kr: (kr[1].value, kr[0]))
            return MetricResult(float(r.value), {"legs": view, "min_leg": k, "lower_bound": lb})
    no_venue = (venue is not None and venue.status in ("ok", "absent")
                and (venue.value is None or venue.value < 0.5))
    if no_venue and x_is_absent:
        return MetricResult(0.0, {"legs": view, "min_leg": None, "lower_bound": False},
                            absence_reason="no_reachable_channel")
    return MetricResult(None, {"legs": view, "min_leg": None, "lower_bound": False, "reason": "no_ok_leg"})
