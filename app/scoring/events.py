"""Scoring events (spec 12 §2.11; 02 §D1/§D2): a thin wrapper over 03's ``emit_event`` plus one payload builder per
event type. Payload keys and dedupe patterns are 03 §3.9's; ``source='scoring'`` always."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping

from app.storage.events import emit_event

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

SOURCE = "scoring"


async def emit(db: "Database", *, occupant_key: str | None, netuid: int | None, event_type: str, severity: str,
               event_at: int, observed_at: int, payload: Mapping[str, Any], dedupe_key: str, run_id: str | None,
               source: str = SOURCE) -> bool:
    eid = await emit_event(db, event_type=event_type, occupant_key=occupant_key, netuid=netuid, severity=severity,
                           event_at=event_at, observed_at=observed_at, payload=payload, dedupe_key=dedupe_key,
                           source=source, run_id=run_id)
    return eid is not None


def _bf(p: dict, backfilled: bool) -> dict:
    if backfilled:
        p["backfilled"] = True
    return p


def tier_change_event(*, occ: str, netuid: int, spec: str, as_of: int, prev_tier: str, tier: str, smoothed: float,
                      prev_smoothed: float, composite: float | None, lower: Mapping[str, float], top_positive: list,
                      top_negative: list, red_flags: list[str], backfilled: bool) -> dict:
    order = ("F", "D", "C", "B", "A")
    up = order.index(tier) > order.index(prev_tier)
    threshold = lower.get(tier if up else prev_tier)
    p = {"netuid": netuid, "spec_version": spec, "as_of": as_of, "from": prev_tier, "to": tier,
         "direction": "up" if up else "down", "composite_smoothed": smoothed,
         "prev_composite_smoothed": prev_smoothed, "composite": composite, "threshold": threshold,
         "top_positive": top_positive, "top_negative": top_negative, "red_flags": red_flags}
    return {"event_type": "tier_change", "severity": "info" if up else "warning", "payload": _bf(p, backfilled),
            "dedupe_key": f"{occ}|tier_change|{spec}|{as_of}"}


def flag_raised_event(*, occ: str, netuid: int, spec: str, as_of: int, flag: str, multiplier: float, human: bool,
                      sticky: bool, detail: dict, total: float, raised_at: int, origin: str,
                      backfilled: bool) -> dict:
    p = {"netuid": netuid, "spec_version": spec, "as_of": as_of, "flag": flag, "multiplier": multiplier,
         "human": bool(human), "sticky": bool(sticky), "detail": detail, "flag_multiplier_total": total,
         "since": raised_at, "trigger_event_id": detail.get("trigger_event_id"), "origin": origin}
    return {"event_type": "red_flag_raised", "severity": "critical" if multiplier <= 0.6 else "warning",
            "payload": _bf(p, backfilled), "dedupe_key": f"{occ}|red_flag_raised|{flag}|{raised_at}"}


def flag_cleared_event(*, occ: str, netuid: int, spec: str, as_of: int, flag: str, multiplier: float,
                       raised_at: int | None, reason: str, cleared_at: int, clear_streak_days: int, origin: str,
                       backfilled: bool) -> dict:
    p = {"netuid": netuid, "spec_version": spec, "as_of": as_of, "flag": flag, "multiplier": multiplier,
         "raised_at": raised_at, "reason": reason, "cleared_at": cleared_at, "clear_streak_days": clear_streak_days,
         "origin": origin}
    return {"event_type": "red_flag_cleared", "severity": "info", "payload": _bf(p, backfilled),
            "dedupe_key": f"{occ}|red_flag_cleared|{flag}|{as_of}"}


def score_jump_event(*, occ: str, netuid: int, spec: str, as_of: int, prev: Mapping, composite: float,
                     pillar_deltas: dict, flags_added: list, flags_removed: list, coverage_delta: float | None,
                     backfilled: bool) -> dict:
    p = {"netuid": netuid, "spec_version": spec, "as_of": as_of, "prev_as_of": prev["as_of"],
         "prev_composite": prev["composite"], "composite": composite, "delta": composite - prev["composite"],
         "pillar_deltas": pillar_deltas, "flags_added": flags_added, "flags_removed": flags_removed,
         "coverage_delta": coverage_delta}
    return {"event_type": "score_jump", "severity": "warning", "payload": _bf(p, backfilled),
            "dedupe_key": f"{occ}|score_jump|{spec}|{as_of}"}


def insufficient_event(*, occ: str, netuid: int, spec: str, as_of: int, confidence: float, coverage: dict,
                       missing: list, backfilled: bool) -> dict:
    p = {"netuid": netuid, "spec_version": spec, "as_of": as_of, "confidence": confidence, "coverage": coverage,
         "missing": missing}
    return {"event_type": "insufficient_data", "severity": "info", "payload": _bf(p, backfilled),
            "dedupe_key": f"{occ}|insufficient_data|{spec}|{as_of}"}


def instability_event(*, spec: str, as_of: int, row: Mapping, threshold: float) -> dict:
    p = {"spec_version": spec, "as_of": as_of, "series": row["series"], "lag_days": row["lag_days"], "n": row["n"],
         "rank_autocorr": row["rank_autocorr"], "quintile_turnover": row["quintile_turnover"],
         "top20_turnover": row["top20_turnover"], "threshold": threshold}
    return {"event_type": "rank_instability", "severity": "info", "payload": p,
            "dedupe_key": f"system|rank_instability|{spec}|{as_of}"}
