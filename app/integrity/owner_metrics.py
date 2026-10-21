"""Pure P4 owner metrics (spec 11 §2.5–§2.11): S1, S2, S3, S5, owner_identity, tenure, the two trigger rows and
the S4 holding share. Inputs are already PIT-filtered and whitelist-filtered by the caller (``features.py``); every
function returns a ``RawMetric`` whose ``value`` is the final score (tenure: raw days) and whose ``value_json`` carries
spec 12 §2.4's input keys plus evidence."""
from __future__ import annotations

import math
from typing import Callable, Mapping, Sequence

from app.integrity import constants as K
from app.integrity.classify import OUTFLOW_KINDS, TAO_KINDS
from app.integrity.models import OwnerChange, RawMetric, StakeEvent, absent, failed, not_applicable, ok

DAY = 86400


def _clip01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x


def zclip(x: float, lo: float, hi: float) -> float:
    return _clip01((x - lo) / (hi - lo))


# ── S1 helpers ───────────────────────────────────────────────────────────────────────────────────────────────
def earned_alpha(cut_hourly: Mapping[int, float | None], cut_snapshots: Sequence[float], *,
                 window: tuple[int, int]) -> tuple[float | None, str]:
    """Owner cut earned in W (α): hourly path when ≥ 80 % of W's hours carry ``owner_cut_per_block`` (× 300 blocks),
    else mean snapshot cut × 7200 × days_W, else None. A known 0 (cut disabled) is a value, not a gap."""
    a, b = window
    hours = list(range(a - a % 3600, b, 3600)) or [a - a % 3600]
    have = [float(cut_hourly[h]) for h in hours if cut_hourly.get(h) is not None]
    if hours and len(have) >= 0.8 * len(hours):
        return math.fsum(have) / len(have) * len(hours) * K.BLOCKS_PER_HOUR, "hourly"
    if cut_snapshots:
        days_w = max(1.0, (b - a) / DAY)
        return math.fsum(cut_snapshots) / len(cut_snapshots) * K.BLOCKS_PER_DAY * days_w, "snapshots"
    return None, "unknown"


def _tao(e: StakeEvent) -> float:
    return float(e.tao or 0.0) if e.kind in TAO_KINDS else 0.0


def impact7_series(events: Sequence[StakeEvent], tao_in_hourly: Mapping[int, float], *, window: tuple[int, int],
                   days: int = K.SELL_IMPACT_WINDOW_DAYS) -> list[float]:
    """impact7(d) = τ received over [d − 7 d, d) / mean pool τ over the same span, for daily boundaries d in W
    (d = W.end, W.end − 1 d, …; windows clipped at W.start). Spans without pool data are skipped."""
    a, b = window
    out: list[float] = []
    d = b
    while d > a:
        lo = max(a, d - days * DAY)
        rcv = math.fsum(_tao(e) for e in events if lo <= e.event_at < d)
        pool = [v for h, v in tao_in_hourly.items() if lo <= h < d and v is not None and v > 0]
        if pool and rcv > 0:
            out.append(rcv / (math.fsum(pool) / len(pool)))
        elif pool:
            out.append(0.0)
        d -= DAY
    return out


def s1_owner_retention(events: Sequence[StakeEvent], *, earned: float | None, earned_method: str = "unknown",
                       impact7: Sequence[float], owner_cut_enabled: bool | None, pre_launch: bool,
                       activity_covered: bool, min_earned: float, n_whitelisted: int = 0,
                       coverage_start: int | None = None) -> RawMetric:
    """§2.5, 02 §C1 (amended): H1 full-exit / H2 7-d impact > 2 % / H3 sell_ratio > 1 first (H3 only when
    earned ≥ min_earned); then coverage, earned, the cut-disabled shortcut; else 1 − clip(sell_ratio)."""
    key = "owner_retention_30d"
    ev = [e for e in events if e.success]
    out_alpha = math.fsum(float(e.alpha or 0.0) for e in ev if e.counts_as_outflow)
    tao_rcv = math.fsum(_tao(e) for e in ev)
    max_imp = max(impact7) if impact7 else None
    sell_ratio = (out_alpha / earned) if (earned is not None and earned >= min_earned) else None
    h1 = [e for e in ev if e.kind == "sell_full"]
    h2 = max_imp is not None and max_imp > K.SELL_IMPACT_MAX
    h3 = sell_ratio is not None and sell_ratio > 1.0
    hard = (["full_exit_extrinsic"] if h1 else []) + (["sell_impact_7d"] if h2 else []) + (["sell_ratio"] if h3 else [])
    trig: list[str] = []
    if h1:
        trig += [e.trigger_id for e in h1]
    if h2 or h3:
        trig += [e.trigger_id for e in ev if e.counts_as_outflow]
    vj = {"sell_ratio": sell_ratio, "max_7d_sell_impact": max_imp, "full_exit": bool(h1),
          "owner_cut_enabled": owner_cut_enabled, "earned_alpha": earned, "hard": hard, "out_alpha": out_alpha,
          "earned_method": earned_method, "tao_rcv_30d": tao_rcv, "n_events": len(ev),
          "n_whitelisted_excluded": n_whitelisted, "trigger_ids": sorted(set(trig)),
          "sources": sorted({getattr(e, "source", None) or "" for e in ev} - {""}), "coverage_start": coverage_start}
    if pre_launch:
        return not_applicable(key, "pre_launch", **vj)
    if hard:
        return ok(key, 0.0, **vj)
    if not activity_covered:
        return failed(key, "activity_coverage", **vj)
    if earned is None:
        return failed(key, "earned_unknown", **vj)
    if owner_cut_enabled is False:
        return ok(key, 1.0, **vj)
    if sell_ratio is None:
        return failed(key, "earned_below_min", **vj)
    return ok(key, 1.0 - _clip01(sell_ratio), **vj)


# ── S2 ───────────────────────────────────────────────────────────────────────────────────────────────────────
def s2_owner_commitment(*, perp: float | None, total: float | None, auto: bool | None, commit_pct: float | None,
                        owner_cut_enabled: bool | None, age_days: float | None, **evidence) -> RawMetric:
    key = "owner_commitment"
    auto_source = evidence.pop("auto_source", None)
    if owner_cut_enabled is False:
        auto, auto_source = True, "owner_cut_disabled"
    lock_ratio = None
    if perp is not None and total is not None:
        lock_ratio = _clip01(perp / total) if total > 0 else (0.0 if perp == 0 else 1.0)
    vj = {"lock_ratio": lock_ratio, "auto_lock": auto, "commit_pct": commit_pct, "perpetual_locked": perp,
          "age_days": age_days, "total": total, "auto_source": auto_source, **evidence}
    if perp is None or total is None:
        return failed(key, "no_position" if total is None else "no_lock_data", **vj)
    if perp == 0 and auto in (False, None) and age_days is not None and age_days <= K.COMMITMENT_ZERO_AGE_DAYS:
        return not_applicable(key, "grace_period", **vj)
    if perp == 0 and auto is False and age_days is not None and age_days > K.COMMITMENT_ZERO_AGE_DAYS:
        return ok(key, 0.0, **vj)
    parts = [(0.60, lock_ratio)]
    if auto is not None:
        parts.append((0.25, 1.0 if auto else 0.0))
    if commit_pct is not None:
        parts.append((0.15, 1.0 if commit_pct >= K.COMMIT_PCT_MIN else 0.0))
    return ok(key, math.fsum(w * s for w, s in parts) / math.fsum(w for w, _ in parts), **vj)


# ── S3 ───────────────────────────────────────────────────────────────────────────────────────────────────────
def s3_control_stability(changes: Sequence[OwnerChange], *, t: int, observable: bool,
                         whitelisted: Callable[[OwnerChange], bool], observed_since: int | None = None) -> RawMetric:
    key = "control_stability_90d"
    lo = t - K.S3_WINDOW_DAYS * DAY
    window = [c for c in changes if lo <= c.event_at < t and c.change_type in K.S3_REFERENCE_KIND]
    events = [{"kind": K.S3_REFERENCE_KIND[c.change_type], "event_at": c.event_at, "whitelisted": bool(whitelisted(c)),
               "change_type": c.change_type, "change_id": c.id} for c in window]
    vj = {"observed_since": observed_since, "events": events,
          "change_ids": sorted(c.id for c in window if c.id is not None)}
    if not observable:
        return failed(key, "no_owner_snapshot", **vj)
    live = [c for c, e in zip(window, events) if not e["whitelisted"]]
    if any(c.change_type == "perpetual_off" for c in live):
        return ok(key, 0.0, reason_note="formal_exit", **vj)
    pen = math.fsum(K.S3_BASE_PENALTY.get(c.change_type, 0.0) * 0.5 ** (((t - c.event_at) / DAY) / K.S3_HALF_LIFE_DAYS)
                    for c in live)
    return ok(key, _clip01(1.0 - pen), **vj)


# ── S5 ───────────────────────────────────────────────────────────────────────────────────────────────────────
def s5_owner_overhang(*, avail: float | None, price: float | None, tao_in: float | None, hidden_alpha: bool,
                      initial_alpha_est: float | None = None) -> RawMetric:
    key = "owner_overhang"
    vj = {"dump_impact": None, "legacy_unaccounted": bool(hidden_alpha), "avail": avail, "price": price,
          "tao_in": tao_in, "initial_alpha_est": initial_alpha_est}
    if avail is None:
        return failed(key, "no_position", **vj)
    if price is None or tao_in is None:
        return failed(key, "no_pool", **vj)
    if tao_in <= 0:
        return failed(key, "not_started", **vj)
    di = avail * price / tao_in
    vj["dump_impact"] = di
    base = 1.0 - zclip(di, K.OVERHANG_FULL, K.OVERHANG_ZERO)
    return ok(key, _clip01(base - (K.FREE_ALPHA_PENALTY if hidden_alpha else 0.0)), **vj)


# ── owner_identity, tenure ───────────────────────────────────────────────────────────────────────────────────
def owner_identity_value(name_status: str | None, has_url: bool, has_github: bool, key_match: bool, *,
                         owner_coldkey: str | None = None, name: str | None = None, url: str | None = None,
                         github: str | None = None) -> RawMetric:
    key = "owner_identity"
    vj = {"owner_coldkey": owner_coldkey, "name": name, "url": url, "github": github, "name_status": name_status}
    if owner_coldkey is None:
        return failed(key, "owner_unknown", **vj)
    if not key_match:
        return failed(key, "owner_key_mismatch", **vj)
    if name_status in (None, "unknown"):
        return failed(key, "identities_never_ok", **vj)
    if name_status == "present" and (has_url or has_github):
        return ok(key, 1.0, **vj)
    if name_status == "present":
        return absent(key, "no_url_or_github", **vj)
    if name_status == "junk_only":
        return absent(key, "junk_only", **vj)
    return absent(key, "no_identity_row", **vj)


def tenure_days(head_block: int, network_registered_at: int, block_seconds: float = 12.0) -> float:
    return (int(head_block) - int(network_registered_at)) * float(block_seconds) / DAY


def tenure_row(head_block: int | None, network_registered_at: int | None, *, head_source: str | None,
               block_seconds: float = 12.0) -> RawMetric:
    vj = {"head_block": head_block, "head_source": head_source, "network_registered_at": network_registered_at}
    if network_registered_at is None or network_registered_at <= 0:
        return failed("tenure", "no_registration_block", **vj)
    if head_block is None:
        return failed("tenure", "no_head_block", **vj)
    return ok("tenure", max(0.0, tenure_days(head_block, network_registered_at, block_seconds)), **vj)


# ── triggers ─────────────────────────────────────────────────────────────────────────────────────────────────
def extraction_trigger(s1: RawMetric | None, hard_events: Sequence[StakeEvent], perp_offs: Sequence[OwnerChange], *,
                       observable: bool) -> RawMetric:
    """02 §C2: 1 iff a non-whitelisted S1 hard trigger in [t − 30 d, t) (``s1``'s ``hard`` tokens over the
    already-filtered window) or a non-whitelisted ``perpetual_off`` in [t − 90 d, t)."""
    key = "owner_extraction_trigger"
    reasons = list((s1.value_json.get("hard") if s1 is not None else None) or [])
    ids = list((s1.value_json.get("trigger_ids") if (s1 is not None and reasons) else None) or [])
    times = [e.event_at for e in hard_events]
    via: set[str] = set()
    if reasons:
        via.add("staking_activity")
    if perp_offs:
        reasons.append("perpetual_off")
        ids += [c.trigger_id for c in perp_offs]
        times += [c.event_at for c in perp_offs]
        via |= {c.detected_via for c in perp_offs}
    vj = {"reasons": reasons, "trigger_ids": sorted(set(ids)), "last_event_at": max(times) if times else None,
          "detected_via": sorted(via)}
    if reasons:
        return ok(key, 1.0, **vj)
    if not observable:
        return failed(key, "owner_unobservable", **vj)
    return ok(key, 0.0, **vj)


def control_change_trigger(changes: Sequence[OwnerChange], *, observable: bool,
                           churn: Callable[[OwnerChange], bool]) -> RawMetric:
    """02 §C2: 1 iff a non-whitelisted coldkey change in [t − 30 d, t) (caller-filtered); ``identity_churn`` when the
    change pairs with a re-point or a chain-source identity change within 14 d (makes 12's flag sticky)."""
    key = "owner_control_change_trigger"
    cc = sorted((c for c in changes if c.change_type in K.COLDKEY_CHANGE_TYPES), key=lambda c: c.event_at)
    if cc:
        last = cc[-1]
        vj = {"kind": last.change_type, "event_at": last.event_at, "change_id": last.id,
              "trigger_ids": sorted({c.trigger_id for c in cc}), "identity_churn": any(churn(c) for c in cc)}
        return ok(key, 1.0, **vj)
    vj = {"kind": None, "event_at": None, "change_id": None, "trigger_ids": [], "identity_churn": False}
    if not observable:
        return failed(key, "no_owner_snapshot", **vj)
    return ok(key, 0.0, **vj)


def holding_share(owner_total: float | None, total_subnet_stake: float | None) -> float | None:
    if owner_total is None or not total_subnet_stake or total_subnet_stake <= 0:
        return None
    return owner_total / total_subnet_stake


__all__ = ["earned_alpha", "impact7_series", "s1_owner_retention", "s2_owner_commitment", "s3_control_stability",
           "s5_owner_overhang", "owner_identity_value", "tenure_days", "tenure_row", "extraction_trigger",
           "control_change_trigger", "holding_share", "OUTFLOW_KINDS"]
