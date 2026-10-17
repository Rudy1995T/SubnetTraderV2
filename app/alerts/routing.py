"""Alert routing table — DATA owned by spec 14 §2.4, shipped with 01's Phase-0 ``alert_dispatch`` (02 §D3/§D4).

Routes: ``immediate`` (Telegram now), ``digest``, ``digest_late``, ``none`` (stored only), ``direct``.
Note (BUILD-LOG): 14 marks ``disk_critical``/``db_integrity_failed`` as ``direct``; 02 §D3 (amended) and 16 §11.22
supersede that — every ops row is inserted with ``alerted_at NULL`` and paged by the dispatcher — so both are
``immediate`` here.
"""
from __future__ import annotations

import json
from typing import Any, Callable, Mapping

import app.config as app_config
from app.logging.logger import logger
from app.storage.catalog import TAXONOMY_EVENT_TYPES

Cond = Callable[[Mapping[str, Any], dict], bool]
_SEV_RANK = {"info": 0, "warning": 1, "critical": 2}
_IDENTITY_FIELDS = frozenset({"subnet_name", "github_repo", "subnet_url", "discord"})


def _identity_changed(row: Mapping[str, Any], p: dict) -> bool:
    return row.get("severity") == "warning" and p.get("source") == "chain" and p.get("field") in _IDENTITY_FIELDS


def _registry_anomaly(row: Mapping[str, Any], p: dict) -> bool:
    return bool(row.get("_consecutive3"))


def _critical(row: Mapping[str, Any], p: dict) -> bool:
    return row.get("severity") == "critical"


I, D, N = "immediate", "digest", "none"
ROUTES: dict[str, str | Cond] = {
    "netuid_reoccupied": I, "occupant_activated": N, "identity_changed": _identity_changed,
    "source_disagreement": D, "identity_junk": D, "registry_anomaly": _registry_anomaly,
    "emission_disabled": D, "emission_reenabled": D, "dereg_risk_entered": D, "dereg_risk_cleared": N,
    "dereg_proximity": D, "immunity_mismatch": N, "gate_fit_degraded": D, "gate_model_broken": I,
    "chain_constant_drift": I, "dereg_rank_entered_k30": D, "dereg_rank_cleared_k30": D, "immunity_ends_7d": D,
    "ema72_band_exit": D, "tmc_schema_drift": D, "chain_sse_down": I, "chain_sse_recovered": I,
    "tmc_budget_degraded": D,
    "repo_renamed": N, "repo_moved": D, "repo_gone": D, "repo_restored": N, "repo_archived": D,
    "org_repo_archived": D, "identity_repo_changed": I, "commit_spam_suspected": D,
    "site_down_7d": D, "site_recovered": D, "tls_expiring_14d": D, "site_parked": I, "site_hijack_suspect": I,
    "site_content_reset": D, "site_cross_domain_redirect": D, "docs_gone": D,
    "invite_expired": D, "invite_dead": D, "inflated_community": D, "community_venue_changed": D, "silence_45d": D,
    "x_account_unresolved": D, "x_account_gone": I, "x_handle_changed": N, "x_budget_paused": D,
    "x_budget_resumed": N, "x_ctf_discrepancy": D, "feed_dead": D, "youtube_channel_resolved": N,
    **{t: _critical for t in TAXONOMY_EVENT_TYPES},
    "owner_control_change": D, "owner_coldkey_swap_announced": D, "owner_hotkey_repointed": D,
    "owner_identity_churn": I, "owner_perpetual_off": D, "owner_decaying_lock_added": D, "owner_lock_reduced": D,
    "owner_extraction_detected": D, "owner_key_compromise_override": I, "owner_auto_lock_changed": D,
    "owner_cut_toggled": D, "owner_source_disagreement": D, "owner_position_unexplained": D,
    "owner_concentration": D, "owner_self_dealing_suspected": D, "owner_data_degraded": I,
    "team_member_added": D, "team_member_verified": D, "team_member_rejected": D, "team_member_delisted": D,
    "team_disclosure_changed": D,
    "tier_change": I, "red_flag_raised": I, "red_flag_cleared": I, "score_jump": I, "insufficient_data": D,
    "rank_instability": D,
    "manual_entry": N, "collector_paused": N, "collector_resumed": N, "rescore_requested": N, "dereg_watch": I,
    "collector_failing": I, "collector_recovered": I, "service_recovered_unclean": I, "spend_80pct": I,
    "spend_cap_reached": I, "spend_reset": D, "budget_override_set": D, "backup_failed": I,
    "backup_mirror_missing": I, "backup_mirror_failed": I, "backup_stale": I, "scoring_stalled": I,
    "discord_gateway_fatal": I, "backfill_done": D, "backfill_failed": I, "disk_low": I,
    "disk_critical": I, "db_integrity_failed": I, "schema_warning": I, "retention_skipped": D,
}

_unknown_logged: set[str] = set()


def payload_of(row: Mapping[str, Any]) -> dict:
    p = row.get("payload")
    if isinstance(p, dict):
        return p
    try:
        return json.loads(p) if p else {}
    except ValueError:
        return {}


def route_for(row: Mapping[str, Any], now: int) -> str:
    p = payload_of(row)
    if p.get("backfilled") is True:
        return "digest"
    et = row.get("event_type")
    route = ROUTES.get(et)
    if route is None:
        if et not in _unknown_logged:
            _unknown_logged.add(et)
            logger.warning("unknown event_type routed to digest", data={"event_type": et})
        return "digest"
    r = route if isinstance(route, str) else ("immediate" if route(row, p) else "digest")
    if r == "immediate":
        s = app_config.settings
        if _SEV_RANK.get(row.get("severity"), 0) < _SEV_RANK.get(s.ALERT_MIN_SEVERITY, 0):
            return "digest"
        if now - int(row.get("observed_at") or now) > s.ALERT_LATE_AFTER_HOURS * 3600:
            return "digest_late"
    return r
