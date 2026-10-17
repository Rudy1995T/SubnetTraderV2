"""Canonical catalogs (spec 03 §3.8, §3.9; 02 §B9, §C, §D; master §7.5).

``METRIC_KEYS`` — every allowed ``metric_values.metric_key`` (109 keys: 79 + 30 workbench evidence keys, 19 §3); ``EVENT_TYPES`` — every ``event_type`` any
spec emits (the only events catalog, 02 §D1); ``SOURCES`` — the producer vocabulary (02 §B9); ``RED_FLAGS`` — flag
names and multipliers. ``EVENT_TYPES`` is generated from the spec's §3.9 table; edit the spec, then regenerate.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

Role = Literal["scored", "leg", "aux", "evidence"]

SOURCES: frozenset[str] = frozenset({
    "registry", "chain", "github", "web", "community", "x", "rss", "llm", "integrity", "scoring", "quant", "api",
    "runtime", "storage", "cost", "ops",
})

SEVERITIES = ("info", "warning", "critical")


@dataclass(frozen=True, slots=True)
class MetricSpec:
    pillar: str            # P1..P6
    weight: float          # master §7.3 in-pillar weight (0 for leg/aux/evidence)
    normaliser: str        # master normaliser description (12's registry is authoritative for math)
    producer_spec: str     # "05", "06", …
    source: str            # 02 §B9
    role: Role


def _m(p: str, w: float, norm: str, spec: str, src: str, role: Role = "scored") -> MetricSpec:
    return MetricSpec(p, w, norm, spec, src, role)


METRIC_KEYS: dict[str, MetricSpec] = {
    # P1 · Chain Vitality (pillar weight 30)
    "emission_share": _m("P1", .20, "rankg", "05", "chain"),
    "gate_margin": _m("P1", .15, "zclip(log2 x, -2, +1)", "05", "chain"),
    "dereg_safety": _m("P1", .15, "custom (immune -> 1; bottom-3 -> 0)", "05", "chain"),
    "liquidity_depth": _m("P1", .10, "rankg(log)", "05", "chain"),
    "exit_capacity": _m("P1", .05, "zclip(x, 0.05, 0.6)", "05", "chain"),
    "net_flow_30d": _m("P1", .10, "zclip(x, -0.5, +0.5)", "05", "chain"),
    "chain_buys": _m("P1", .05, "rankg", "05", "chain"),
    "holder_dispersion": _m("P1", .05, "zclip(x, 0.3, 0.9)", "05", "chain"),
    "participation": _m("P1", .05, "zclip(x, 0.2, 0.9)", "05", "chain"),
    "miner_burn_ok": _m("P1", .10, "zclip(x, 0, 0.8)", "05", "chain"),
    "emission_enabled_flag": _m("P1", 0, "-", "05", "chain", "aux"),
    "dereg_risk_flag": _m("P1", 0, "-", "05", "chain", "aux"),
    "immune_flag": _m("P1", 0, "-", "05", "chain", "aux"),
    "implied_miner_burn": _m("P1", 0, "-", "05", "chain", "aux"),
    "dereg_hazard": _m("P1", 0, "-", "05", "chain", "aux"),
    "occupant_age_days": _m("P1", 0, "-", "05", "chain", "aux"),
    "vtrust_dispersion": _m("P1", 0, "-", "05", "chain", "evidence"),
    "owner_locked_share": _m("P1", 0, "-", "05", "chain", "evidence"),
    "sum_subnet_prices": _m("P1", 0, "-", "05", "chain", "evidence"),
    "tao_usd": _m("P1", 0, "-", "05", "chain", "evidence"),
    # P2 · Builder Activity (pillar weight 20)
    "active_authors_28d": _m("P2", .20, "logcap(x, 10)", "06", "github"),
    "fte_devs_84d": _m("P2", .15, "logcap(x, 8)", "06", "github"),
    "commit_days_90d": _m("P2", .15, "logcap(x, 90)", "06", "github"),
    "declared_repo_freshness": _m("P2", .10, "recency(d, 14, 180)", "06", "github"),
    "org_freshness": _m("P2", .10, "recency(d, 7, 90)", "06", "github"),
    "release_cadence_90d": _m("P2", .05, "logcap(x, 6)", "06", "github"),
    "review_practice_90d": _m("P2", .05, "identity", "06", "github"),
    "bus_factor": _m("P2", .10, "custom (>=3 -> 1, 2 -> .6, 1 -> .3)", "06", "github"),
    "repo_hygiene": _m("P2", .05, "checklist", "06", "github"),
    "originality": _m("P2", .05, "custom (1 - template share)", "06", "github"),
    # P3 · Product & Web Presence (pillar weight 10)
    "site_uptime_30d": _m("P3", .30, "identity", "07", "web"),
    "tls_ok": _m("P3", .10, "binary", "07", "web"),
    "site_age": _m("P3", .10, "inverted recency (>=365 -> 1, <=30 -> .2)", "07", "web"),
    "site_freshness": _m("P3", .15, "recency(d, 90, 365)", "07", "web"),
    "docs_present": _m("P3", .15, "binary", "07", "web"),
    "product_status": _m("P3", .20, "map none 0 / waitlist .3 / beta .6 / live 1", "07", "web"),
    # P4 · Team & Governance Integrity (pillar weight 18)
    "team_doxxed": _m("P4", .25, "map (prenorm)", "11", "integrity"),
    "owner_identity": _m("P4", .05, "binary (prenorm)", "11", "integrity"),
    "owner_commitment": _m("P4", .20, "custom S2 (prenorm)", "11", "integrity"),
    "owner_retention_30d": _m("P4", .20, "custom S1 (prenorm)", "11", "integrity"),
    "control_stability_90d": _m("P4", .15, "custom S3 (prenorm)", "11", "integrity"),
    "owner_overhang": _m("P4", .05, "inverted zclip S5 (prenorm)", "11", "integrity"),
    "tenure": _m("P4", .10, "zclip(days, 120, 365), floor .3", "11", "integrity"),
    "owner_extraction_trigger": _m("P4", 0, "-", "11", "integrity", "aux"),
    "owner_control_change_trigger": _m("P4", 0, "-", "11", "integrity", "aux"),
    "owner_lock_ratio": _m("P4", 0, "-", "11", "integrity", "evidence"),
    "owner_dump_impact": _m("P4", 0, "-", "11", "integrity", "evidence"),
    "owner_holding_share": _m("P4", 0, "-", "11", "integrity", "evidence"),
    "owner_sell_impact_7d": _m("P4", 0, "-", "11", "integrity", "evidence"),
    "owner_sell_ratio_30d": _m("P4", 0, "-", "11", "integrity", "evidence"),
    "team_verified_count": _m("P4", 0, "-", "11", "integrity", "evidence"),
    "team_candidate_count": _m("P4", 0, "-", "11", "integrity", "evidence"),
    # P5 · Community & Communication (pillar weight 12)
    "venue_reachable": _m("P5", .10, "binary", "08", "community"),
    "discord_members": _m("P5", .15, "logcap(x, 5000)", "08", "community"),
    "presence_ratio": _m("P5", .10, "custom", "08", "community"),
    "discord_growth_30d": _m("P5", .10, "zclip(x, -0.1, +0.3)", "08", "community"),
    "announcement_cadence_30d": _m("P5", .25, "logcap(x, 12)", "12", "scoring"),
    "x_engagement_30d": _m("P5", .10, "rankg", "09", "x"),
    "x_followers": _m("P5", .05, "logcap(x, 50000)", "09", "x"),
    "silence_days": _m("P5", .15, "recency(d, 7, 45)", "12", "scoring"),
    "x_cadence_30d": _m("P5", 0, "-", "09", "x", "leg"),
    "x_last_activity_days": _m("P5", 0, "-", "09", "x", "leg"),
    "discord_announcements_30d": _m("P5", 0, "-", "08", "community", "leg"),
    "tg_posts_30d": _m("P5", 0, "-", "08", "community", "leg"),
    "discord_last_activity_days": _m("P5", 0, "-", "08", "community", "leg"),
    "tg_last_activity_days": _m("P5", 0, "-", "08", "community", "leg"),
    "discord_topology": _m("P5", 0, "-", "08", "community", "evidence"),
    "inflated_days": _m("P5", 0, "-", "08", "community", "evidence"),
    "tg_members": _m("P5", 0, "-", "08", "community", "evidence"),
    # P6 · Narrative & Sentiment (Velocity only)
    "event_impact_14d": _m("P6", .40, "tanh(x/2) -> [0,1]", "10", "llm"),
    "polarity_7d": _m("P6", .25, "identity (0.5 neutral; < 5 posts -> None)", "10", "llm"),
    "attention_7d": _m("P6", .20, "zclip(z, -2, +3)", "10", "llm"),
    "kol_coverage_30d": _m("P6", .15, "logcap(x, 5)", "10", "llm"),
    "x_mentions_7d": _m("P6", 0, "-", "09", "x", "leg"),
    "x_kol_posts_30d": _m("P6", 0, "-", "09", "x", "leg"),
    "news_mentions_7d": _m("P6", 0, "-", "09", "rss", "leg"),
    "video_uploads_90d": _m("P6", 0, "-", "09", "rss", "evidence"),
    "video_mentions_30d": _m("P6", 0, "-", "09", "rss", "evidence"),
    "blog_posts_90d": _m("P6", 0, "-", "09", "rss", "evidence"),    # Workbench (19 §2.3–§2.5): evidence only, weight 0, never scored (02 §K4)
    "ema72_1h": _m("P1", 0, "-", "19", "chain", "evidence"),
    "ema72_dist_1h": _m("P1", 0, "-", "19", "chain", "evidence"),
    "ema72_band_1h": _m("P1", 0, "-", "19", "chain", "evidence"),
    "ema72_band_pos_1h": _m("P1", 0, "-", "19", "chain", "evidence"),
    "px_vs_moving": _m("P1", 0, "-", "19", "chain", "evidence"),
    "ema72_1d": _m("P1", 0, "-", "19", "chain", "evidence"),
    "ema72_dist_1d": _m("P1", 0, "-", "19", "chain", "evidence"),
    "ema72_band_1d": _m("P1", 0, "-", "19", "chain", "evidence"),
    "ema72_band_pos_1d": _m("P1", 0, "-", "19", "chain", "evidence"),
    "ema72_band_rank_1d": _m("P1", 0, "-", "19", "chain", "evidence"),
    "rv_30d": _m("P1", 0, "-", "19", "chain", "evidence"),
    "dd_90d": _m("P1", 0, "-", "19", "chain", "evidence"),
    "ema72_cross_days_1d": _m("P1", 0, "-", "19", "chain", "evidence"),
    "immunity_remaining_days": _m("P1", 0, "-", "19", "chain", "evidence"),
    "dereg_rank": _m("P1", 0, "-", "19", "chain", "evidence"),
    "dereg_rank_30d": _m("P1", 0, "-", "19", "chain", "evidence"),
    "dereg_rank_90d": _m("P1", 0, "-", "19", "chain", "evidence"),
    "cull_line_1": _m("P1", 0, "-", "19", "chain", "evidence"),
    "cull_line_k_30d": _m("P1", 0, "-", "19", "chain", "evidence"),
    "cull_line_k_90d": _m("P1", 0, "-", "19", "chain", "evidence"),
    "mp_gap_to_cull_1": _m("P1", 0, "-", "19", "chain", "evidence"),
    "mp_gap_to_cull_k_30d": _m("P1", 0, "-", "19", "chain", "evidence"),
    "mp_gap_to_cull_k_90d": _m("P1", 0, "-", "19", "chain", "evidence"),
    "dereg_safety_30d": _m("P1", 0, "-", "19", "chain", "evidence"),
    "dereg_safety_90d": _m("P1", 0, "-", "19", "chain", "evidence"),
    "hours_in_bottom_k_30d": _m("P1", 0, "-", "19", "chain", "evidence"),
    "spot_rank": _m("P1", 0, "-", "19", "chain", "evidence"),
    "spot_vs_mp": _m("P1", 0, "-", "19", "chain", "evidence"),
    "days_to_cull_static": _m("P1", 0, "-", "19", "chain", "evidence"),
    "repo_pushed_days": _m("P2", 0, "-", "19", "github", "evidence"),
}

# Retired names → replacement (02 §C2/§C3); put_metric_values names the replacement in its ValueError.
RETIRED_METRIC_KEYS: dict[str, str] = {
    "owner_lock_disabled_90d": "owner_extraction_trigger",
    "owner_control_change_30d": "owner_control_change_trigger",
    "owner_max_sell_impact_7d": "owner_sell_impact_7d",
    "identity_stability": "control_stability_90d",
    "owner_conviction": "owner_commitment",
    "owner_retention": "owner_retention_30d",
    "discord_last_announcement_days": "discord_last_activity_days",
    "tg_last_post_days": "tg_last_activity_days",
}

PILLAR_WEIGHTS: dict[str, int] = {"P1": 30, "P2": 20, "P3": 10, "P4": 18, "P5": 12}   # P6 feeds Velocity only

RED_FLAGS: dict[str, float] = {
    "emission_disabled": 0.50, "dereg_imminent": 0.60, "miner_burn_zeroed": 0.60, "owner_extraction": 0.70,
    "owner_control_change": 0.85, "key_compromise": 0.50, "repo_gone": 0.80, "all_repos_archived": 0.70,
    "site_down_7d": 0.85, "stale_identity": 0.90, "silence_45d": 0.85, "inflated_community": 0.90,
}
RED_FLAG_FLOOR = 0.25


@dataclass(frozen=True, slots=True)
class EventSpec:
    severity: str                          # default severity (emitters may pass a stricter one)
    alert: str                             # yes | digest | no (14 §2.4 routing mirrors it)
    scope: str                             # occ | sys | occ/sys | sys/occ
    source: str | None                     # emitter's events.source (02 §B9)
    payload_keys: tuple[str, ...]          # required (value may be null)
    optional: tuple[str, ...] = field(default=())
    dedupe: str = ""                       # canonical dedupe_key pattern (documentation)
    backfill_payload_keys: tuple[str, ...] | None = None


EVENT_TYPES: dict[str, EventSpec] = {
    'netuid_reoccupied': EventSpec('warning', 'yes', 'occ', 'registry', ('netuid', 'old_occupant_key', 'new_occupant_key', 'old_network_registered_at', 'new_network_registered_at', 'old_name', 'new_name', 'old_status_before', 'old_last_seen_block', 'head_block', 'new_status'), optional=(), dedupe='new_occupant_key (history rows: reoccupied:{old}->{new})', backfill_payload_keys=("old_key", "new_key", "old_name", "new_name", "reg_block", "backfilled")),
    'occupant_activated': EventSpec('info', 'no', 'occ', 'registry', ('netuid', 'occupant_key', 'first_emission_block', 'head_block', 'days_pre_launch'), optional=(), dedupe='occupant_key'),
    'identity_changed': EventSpec('warning', 'yes', 'occ', 'registry', ('netuid', 'occupant_key', 'source', 'field', 'kind', 'old_value', 'new_value', 'old_hash', 'new_hash', 'old_observed_at', 'resolved_changed'), optional=('via',), dedupe='occ|source|field|new_hash'),
    'source_disagreement': EventSpec('info', 'digest', 'occ', 'registry', ('netuid', 'occupant_key', 'field', 'groups', 'resolved_source', 'pair_hash'), optional=('repo_ids',), dedupe='occ|field|pair_hash'),
    'identity_junk': EventSpec('info', 'digest', 'occ', 'registry', ('netuid', 'occupant_key', 'source', 'field', 'value', 'junk_reason'), optional=(), dedupe='occ|source|field|value_hash'),
    'registry_anomaly': EventSpec('warning', 'yes', 'sys/occ', 'registry', ('reason', 'detail'), optional=('netuid',), dedupe='reason|netuid|run_date'),
    'emission_disabled': EventSpec('critical', 'digest', 'occ', 'chain', ('netuid', 'block_number', 'prev', 'now'), optional=(), dedupe='occ|block_number'),
    'emission_reenabled': EventSpec('info', 'digest', 'occ', 'chain', ('netuid', 'block_number', 'prev', 'now'), optional=(), dedupe='occ|block_number'),
    'dereg_risk_entered': EventSpec('critical', 'digest', 'occ', 'chain', ('netuid', 'moving_price', 'rank_from_bottom', 'pool_size'), optional=(), dedupe='occ|{d7}'),
    'dereg_risk_cleared': EventSpec('info', 'no', 'occ', 'chain', ('netuid', 'moving_price', 'rank_from_bottom', 'pool_size'), optional=(), dedupe='occ|{d7}'),
    'dereg_proximity': EventSpec('critical', 'digest', 'occ', 'chain', ('netuid', 'hazard', 'components'), optional=(), dedupe='occ|{d7}'),
    'immunity_mismatch': EventSpec('info', 'no', 'occ', 'chain', ('netuid', 'tmc', 'local', 'age_blocks'), optional=(), dedupe='occ|day'),
    'gate_fit_degraded': EventSpec('info', 'digest', 'sys', 'chain', ('as_of', 'residual_pp', 'theta', 'n_eligible'), optional=(), dedupe='as_of'),
    'gate_model_broken': EventSpec('warning', 'yes', 'sys', 'chain', ('as_of', 'consecutive_days', 'residual_pp'), optional=(), dedupe='gate_model_broken|{d7}'),
    'chain_constant_drift': EventSpec('warning', 'yes', 'sys', 'chain', ('name', 'config', 'live', 'block'), optional=(), dedupe='name|live'),
    'dereg_rank_entered_k30': EventSpec('info', 'digest', 'occ', 'chain', ('netuid', 'rank', 'k', 'pool_size', 'mp_gap_to_cull'), optional=(), dedupe='occ|{d7}'),
    'dereg_rank_cleared_k30': EventSpec('info', 'digest', 'occ', 'chain', ('netuid', 'rank', 'k', 'pool_size', 'mp_gap_to_cull'), optional=(), dedupe='occ|{d7}'),
    'immunity_ends_7d': EventSpec('warning', 'digest', 'occ', 'chain', ('netuid', 'graduates_at', 'dereg_rank_30d'), optional=(), dedupe='occupant_key'),
    'ema72_band_exit': EventSpec('info', 'digest', 'occ', 'chain', ('netuid', 'band_pos', 'prev_band_pos', 'side'), optional=(), dedupe='occ|day'),
    'tmc_schema_drift': EventSpec('info', 'digest', 'sys', 'chain', ('added', 'removed'), optional=(), dedupe='sha of diff'),
    'chain_sse_down': EventSpec('warning', 'yes', 'sys', 'chain', ('since', 'reconnects', 'last_block'), optional=(), dedupe='since'),
    'chain_sse_recovered': EventSpec('info', 'yes', 'sys', 'chain', ('since', 'reconnects', 'last_block', 'down_seconds'), optional=(), dedupe='since'),
    'tmc_budget_degraded': EventSpec('warning', 'digest', 'sys', 'chain', ('requests_month', 'cap'), optional=(), dedupe='month'),
    'repo_renamed': EventSpec('info', 'no', 'occ', 'github', ('repo_id', 'from', 'to', 'detected_at', 'method'), optional=(), dedupe='sha1(occ|type|repo_id|to)'),
    'repo_moved': EventSpec('warning', 'digest', 'occ', 'github', ('repo_id', 'from', 'to', 'from_owner', 'to_owner', 'detected_at'), optional=(), dedupe='sha1(occ|type|repo_id|to)'),
    'repo_gone': EventSpec('warning', 'digest', 'occ', 'github', ('declared', 'source', 'first_404_at', 'consecutive_404', 'reason'), optional=('repo_id', 'fallback_primary_repo_id'), dedupe='sha1(occ|type|declared)|{d30}'),
    'repo_restored': EventSpec('info', 'no', 'occ', 'github', ('repo_id', 'full_name'), optional=(), dedupe='sha1(occ|type|repo_id)|{d30}'),
    'repo_archived': EventSpec('warning', 'digest', 'occ', 'github', ('repo_id', 'full_name', 'role'), optional=(), dedupe='sha1(occ|type|repo_id)'),
    'org_repo_archived': EventSpec('warning', 'digest', 'occ', 'github', ('owner', 'repo_id', 'full_name', 'is_subnet_related'), optional=(), dedupe='sha1(occ|type|repo_id)'),
    'identity_repo_changed': EventSpec('warning', 'yes', 'occ', 'github', ('source', 'from_ref', 'to_ref', 'from_repo_id', 'to_repo_id'), optional=(), dedupe='sha1(occ|type|to_ref)'),
    'commit_spam_suspected': EventSpec('warning', 'digest', 'occ', 'github', ('repos', 'human_commits_28d', 'flagged_28d', 'flags_histogram', 'force_push_repos'), optional=(), dedupe='occ|{d30}'),
    'site_down_7d': EventSpec('critical', 'digest', 'occ', 'web', ('url', 'streak_days', 'last_status_class', 'last_http_status', 'secondary_up'), optional=(), dedupe='occ|url|streak_start_day'),
    'site_recovered': EventSpec('info', 'digest', 'occ', 'web', ('url', 'down_days'), optional=(), dedupe='occ|url|day'),
    'tls_expiring_14d': EventSpec('warning', 'digest', 'occ', 'web', ('url', 'tls_days_left', 'tls_not_after', 'tls_issuer', 'expired'), optional=(), dedupe='occ|url|tls_not_after'),
    'site_parked': EventSpec('warning', 'yes', 'occ', 'web', ('url', 'final_url', 'parked_signals'), optional=(), dedupe='occ|url|check_day'),
    'site_hijack_suspect': EventSpec('critical', 'yes', 'occ', 'web', ('url', 'final_url', 'hijack_signals', 'change_ratio'), optional=(), dedupe='occ|url|check_day'),
    'site_content_reset': EventSpec('warning', 'digest', 'occ', 'web', ('url', 'prev_hash', 'new_hash', 'prev_len', 'new_len', 'change_ratio'), optional=(), dedupe='occ|url|new_hash'),
    'site_cross_domain_redirect': EventSpec('info', 'digest', 'occ', 'web', ('url', 'final_url', 'final_domain'), optional=(), dedupe='occ|url|final_domain'),
    'docs_gone': EventSpec('info', 'digest', 'occ', 'web', ('url', 'last_up_day', 'streak_days'), optional=(), dedupe='occ|url|streak_start_day'),
    'invite_expired': EventSpec('info', 'digest', 'occ', 'community', ('netuid', 'occupant_key', 'invite_code', 'stage', 'expires_at', 'guild_id', 'last_valid_at', 'declared_by', 'other_reachable_venue'), optional=(), dedupe='occ|code|stage'),
    'invite_dead': EventSpec('info', 'digest', 'occ', 'community', ('netuid', 'occupant_key', 'invite_code', 'error_code', 'first_dead_at', 'last_valid_at', 'guild_id', 'declared_by', 'other_reachable_venue'), optional=(), dedupe='occ|code|error_code'),
    'inflated_community': EventSpec('warning', 'digest', 'occ', 'community', ('netuid', 'occupant_key', 'guild_id', 'members', 'presence_median', 'ratio', 'days', 'streak_start', 'guild_created_at', 'source_agreement'), optional=(), dedupe='occ|guild_id|streak_start'),
    'community_venue_changed': EventSpec('info', 'digest', 'occ', 'community', ('netuid', 'occupant_key', 'platform', 'old_class', 'new_class', 'old_guild_id', 'new_guild_id', 'handle', 'first_seen_new_at', 'reason'), optional=(), dedupe='occ|platform|new_class|new_guild_id or handle|first_seen_new_day'),
    'silence_45d': EventSpec('warning', 'digest', 'occ', 'community', ('netuid', 'occupant_key', 'silence_days', 'lower_bound', 'legs', 'last_comm_at', 'last_comm_platform'), optional=('min_leg',), dedupe='occ|episode_start_date'),
    'x_account_unresolved': EventSpec('warning', 'digest', 'occ/sys', 'x', ('handle', 'kind', 'reason', 'consecutive', 'provider'), optional=(), dedupe='handle|reason|{d7}'),
    'x_account_gone': EventSpec('warning', 'yes', 'occ/sys', 'x', ('handle', 'author_id', 'last_post_at', 'since'), optional=(), dedupe='author_id'),
    'x_handle_changed': EventSpec('info', 'no', 'occ/sys', 'x', ('author_id', 'old_handle', 'new_handle'), optional=(), dedupe='author_id|new_handle'),
    'x_budget_paused': EventSpec('warning', 'digest', 'sys', 'x', ('provider', 'usd_mtd', 'cap_usd', 'projected_usd', 'reason'), optional=(), dedupe='provider|month'),
    'x_budget_resumed': EventSpec('info', 'no', 'sys', 'x', ('provider', 'month'), optional=(), dedupe='provider|month'),
    'x_ctf_discrepancy': EventSpec('warning', 'digest', 'occ', 'x', ('handle', 'ctf_last_post_at', 'provider_last_post_at', 'gap_days'), optional=(), dedupe='handle|{d7}'),
    'feed_dead': EventSpec('warning', 'digest', 'occ/sys', 'rss', ('feed_url', 'kind', 'consecutive_failures', 'last_status'), optional=(), dedupe='feed_url|{d30}'),
    'youtube_channel_resolved': EventSpec('info', 'no', 'occ', 'rss', ('input_url', 'channel_id', 'method'), optional=(), dedupe='occ|channel_id'),
    'product_launch': EventSpec('info', 'yes', 'occ', 'llm', ('post_ids', 'label_version', 'direction', 'materiality', 'summary', 'url', 'confidence', 'n_supporting'), optional=('confirmed_by',), dedupe='10\'s key: occ|event_type|{48 h bucket}|{polarity} — the former "topic hash" slot is the polarity bucket pos|neg|neu from the label\'s direction (+1/−1/0), so opposite directions in one bucket are separate rows (e.g. {occ}|product_launch|{bucket}|pos, 10 §9)'),
    'release_upgrade': EventSpec('info', 'yes', 'occ', 'llm', ('post_ids', 'label_version', 'direction', 'materiality', 'summary', 'url', 'confidence', 'n_supporting'), optional=('confirmed_by',), dedupe='10\'s key: occ|event_type|{48 h bucket}|{polarity} — the former "topic hash" slot is the polarity bucket pos|neg|neu from the label\'s direction (+1/−1/0), so opposite directions in one bucket are separate rows (e.g. {occ}|product_launch|{bucket}|pos, 10 §9)'),
    'partnership_integration': EventSpec('info', 'yes', 'occ', 'llm', ('post_ids', 'label_version', 'direction', 'materiality', 'summary', 'url', 'confidence', 'n_supporting'), optional=('confirmed_by',), dedupe='10\'s key: occ|event_type|{48 h bucket}|{polarity} — the former "topic hash" slot is the polarity bucket pos|neg|neu from the label\'s direction (+1/−1/0), so opposite directions in one bucket are separate rows (e.g. {occ}|product_launch|{bucket}|pos, 10 §9)'),
    'revenue_customer': EventSpec('info', 'yes', 'occ', 'llm', ('post_ids', 'label_version', 'direction', 'materiality', 'summary', 'url', 'confidence', 'n_supporting'), optional=('confirmed_by',), dedupe='10\'s key: occ|event_type|{48 h bucket}|{polarity} — the former "topic hash" slot is the polarity bucket pos|neg|neu from the label\'s direction (+1/−1/0), so opposite directions in one bucket are separate rows (e.g. {occ}|product_launch|{bucket}|pos, 10 §9)'),
    'token_buyback_burn': EventSpec('info', 'yes', 'occ', 'llm', ('post_ids', 'label_version', 'direction', 'materiality', 'summary', 'url', 'confidence', 'n_supporting'), optional=('confirmed_by',), dedupe='10\'s key: occ|event_type|{48 h bucket}|{polarity} — the former "topic hash" slot is the polarity bucket pos|neg|neu from the label\'s direction (+1/−1/0), so opposite directions in one bucket are separate rows (e.g. {occ}|product_launch|{bucket}|pos, 10 §9)'),
    'exchange_listing': EventSpec('info', 'yes', 'occ', 'llm', ('post_ids', 'label_version', 'direction', 'materiality', 'summary', 'url', 'confidence', 'n_supporting'), optional=('confirmed_by',), dedupe='10\'s key: occ|event_type|{48 h bucket}|{polarity} — the former "topic hash" slot is the polarity bucket pos|neg|neu from the label\'s direction (+1/−1/0), so opposite directions in one bucket are separate rows (e.g. {occ}|product_launch|{bucket}|pos, 10 §9)'),
    'funding': EventSpec('info', 'yes', 'occ', 'llm', ('post_ids', 'label_version', 'direction', 'materiality', 'summary', 'url', 'confidence', 'n_supporting'), optional=('confirmed_by',), dedupe='10\'s key: occ|event_type|{48 h bucket}|{polarity} — the former "topic hash" slot is the polarity bucket pos|neg|neu from the label\'s direction (+1/−1/0), so opposite directions in one bucket are separate rows (e.g. {occ}|product_launch|{bucket}|pos, 10 §9)'),
    'emissions_tokenomics_change': EventSpec('info', 'yes', 'occ', 'llm', ('post_ids', 'label_version', 'direction', 'materiality', 'summary', 'url', 'confidence', 'n_supporting'), optional=('confirmed_by',), dedupe='10\'s key: occ|event_type|{48 h bucket}|{polarity} — the former "topic hash" slot is the polarity bucket pos|neg|neu from the label\'s direction (+1/−1/0), so opposite directions in one bucket are separate rows (e.g. {occ}|product_launch|{bucket}|pos, 10 §9)'),
    'team_change': EventSpec('info', 'yes', 'occ', 'llm', ('post_ids', 'label_version', 'direction', 'materiality', 'summary', 'url', 'confidence', 'n_supporting'), optional=('confirmed_by',), dedupe='10\'s key: occ|event_type|{48 h bucket}|{polarity} — the former "topic hash" slot is the polarity bucket pos|neg|neu from the label\'s direction (+1/−1/0), so opposite directions in one bucket are separate rows (e.g. {occ}|product_launch|{bucket}|pos, 10 §9)'),
    'security_incident': EventSpec('info', 'yes', 'occ', 'llm', ('post_ids', 'label_version', 'direction', 'materiality', 'summary', 'url', 'confidence', 'n_supporting'), optional=('confirmed_by',), dedupe='10\'s key: occ|event_type|{48 h bucket}|{polarity} — the former "topic hash" slot is the polarity bucket pos|neg|neu from the label\'s direction (+1/−1/0), so opposite directions in one bucket are separate rows (e.g. {occ}|product_launch|{bucket}|pos, 10 §9)'),
    'delay_missed_milestone': EventSpec('info', 'yes', 'occ', 'llm', ('post_ids', 'label_version', 'direction', 'materiality', 'summary', 'url', 'confidence', 'n_supporting'), optional=('confirmed_by',), dedupe='10\'s key: occ|event_type|{48 h bucket}|{polarity} — the former "topic hash" slot is the polarity bucket pos|neg|neu from the label\'s direction (+1/−1/0), so opposite directions in one bucket are separate rows (e.g. {occ}|product_launch|{bucket}|pos, 10 §9)'),
    'governance_dispute': EventSpec('info', 'yes', 'occ', 'llm', ('post_ids', 'label_version', 'direction', 'materiality', 'summary', 'url', 'confidence', 'n_supporting'), optional=('confirmed_by',), dedupe='10\'s key: occ|event_type|{48 h bucket}|{polarity} — the former "topic hash" slot is the polarity bucket pos|neg|neu from the label\'s direction (+1/−1/0), so opposite directions in one bucket are separate rows (e.g. {occ}|product_launch|{bucket}|pos, 10 §9)'),
    'regulatory': EventSpec('info', 'yes', 'occ', 'llm', ('post_ids', 'label_version', 'direction', 'materiality', 'summary', 'url', 'confidence', 'n_supporting'), optional=('confirmed_by',), dedupe='10\'s key: occ|event_type|{48 h bucket}|{polarity} — the former "topic hash" slot is the polarity bucket pos|neg|neu from the label\'s direction (+1/−1/0), so opposite directions in one bucket are separate rows (e.g. {occ}|product_launch|{bucket}|pos, 10 §9)'),
    'owner_extraction_evidence': EventSpec('info', 'yes', 'occ', 'llm', ('post_ids', 'label_version', 'direction', 'materiality', 'summary', 'url', 'confidence', 'n_supporting'), optional=('confirmed_by',), dedupe='10\'s key: occ|event_type|{48 h bucket}|{polarity} — the former "topic hash" slot is the polarity bucket pos|neg|neu from the label\'s direction (+1/−1/0), so opposite directions in one bucket are separate rows (e.g. {occ}|product_launch|{bucket}|pos, 10 §9)'),
    'giveaway_promo': EventSpec('info', 'yes', 'occ', 'llm', ('post_ids', 'label_version', 'direction', 'materiality', 'summary', 'url', 'confidence', 'n_supporting'), optional=('confirmed_by',), dedupe='10\'s key: occ|event_type|{48 h bucket}|{polarity} — the former "topic hash" slot is the polarity bucket pos|neg|neu from the label\'s direction (+1/−1/0), so opposite directions in one bucket are separate rows (e.g. {occ}|product_launch|{bucket}|pos, 10 §9)'),
    'other': EventSpec('info', 'yes', 'occ', 'llm', ('post_ids', 'label_version', 'direction', 'materiality', 'summary', 'url', 'confidence', 'n_supporting'), optional=('confirmed_by',), dedupe='10\'s key: occ|event_type|{48 h bucket}|{polarity} — the former "topic hash" slot is the polarity bucket pos|neg|neu from the label\'s direction (+1/−1/0), so opposite directions in one bucket are separate rows (e.g. {occ}|product_launch|{bucket}|pos, 10 §9)'),
    'owner_control_change': EventSpec('warning', 'digest', 'occ', 'integrity', ('netuid', 'change_id', 'change_type', 'old_coldkey', 'new_coldkey', 'classification_basis', 'detected_via', 'whitelisted', 'review_id'), optional=(), dedupe='occ|ck|new_coldkey'),
    'owner_coldkey_swap_announced': EventSpec('warning', 'digest', 'occ', 'integrity', ('netuid', 'change_id', 'extrinsic_id', 'block_number', 'announced_at'), optional=(), dedupe='occ|announce|extrinsic_id'),
    'owner_hotkey_repointed': EventSpec('info', 'digest', 'occ', 'integrity', ('netuid', 'change_id', 'old_hotkey', 'new_hotkey'), optional=(), dedupe='occ|hk|new_hotkey'),
    'owner_identity_churn': EventSpec('warning', 'yes', 'occ', 'integrity', ('netuid', 'change_id', 'repoint_change_id', 'identity_event_id', 'field', 'days_apart'), optional=(), dedupe='occ|churn|change_id'),
    'owner_perpetual_off': EventSpec('critical', 'digest', 'occ', 'integrity', ('netuid', 'change_id', 'perp_before', 'perp_after', 'decay_before', 'decay_after', 'detected_via', 'hour_ts'), optional=(), dedupe='occ|perpoff|day'),
    'owner_decaying_lock_added': EventSpec('info', 'digest', 'occ', 'integrity', ('netuid', 'delta_perp', 'delta_decay', 'hour_ts'), optional=(), dedupe='occ|owner_decaying_lock_added|day'),
    'owner_lock_reduced': EventSpec('warning', 'digest', 'occ', 'integrity', ('netuid', 'delta_perp', 'delta_decay', 'hour_ts'), optional=(), dedupe='occ|owner_lock_reduced|day'),
    'owner_extraction_detected': EventSpec('critical', 'digest', 'occ', 'integrity', ('netuid', 'change_id', 'reasons', 'sell_ratio', 'max_impact7', 'tao_rcv_30d', 'out_alpha_30d', 'earned_30d', 'top_event_uids'), optional=(), dedupe='occ|extract|iso_week'),
    'owner_key_compromise_override': EventSpec('warning', 'yes', 'occ', 'integrity', ('netuid', 'window_start', 'window_end', 'whitelist_id', 'evidence_url', 'by'), optional=(), dedupe='occ|kc|whitelist_id'),
    'owner_auto_lock_changed': EventSpec('info', 'digest', 'occ', 'integrity', ('netuid', 'old', 'new', 'source'), optional=(), dedupe='occ|owner_auto_lock_changed|day'),
    'owner_cut_toggled': EventSpec('info', 'digest', 'occ', 'integrity', ('netuid', 'old', 'new', 'source'), optional=(), dedupe='occ|owner_cut_toggled|day'),
    'owner_source_disagreement': EventSpec('info', 'digest', 'occ', 'integrity', ('netuid', 'field', 'values', 'since'), optional=(), dedupe='occ|srcdis|field|day'),
    'owner_position_unexplained': EventSpec('info', 'digest', 'occ', 'integrity', ('netuid', 'prev_total', 'curr_total', 'inflow', 'outflow', 'unexplained'), optional=(), dedupe='occ|unexpl|day'),
    'owner_concentration': EventSpec('info', 'digest', 'occ', 'integrity', ('netuid', 'owner_holding_share', 'lock_ratio'), optional=(), dedupe='occ|conc|iso_week'),
    'owner_self_dealing_suspected': EventSpec('warning', 'digest', 'occ', 'integrity', ('owner_vali_stake_share', 'self_weight', 'owner_holding_share', 'lock_ratio'), optional=(), dedupe='occ|{d30}'),
    'owner_data_degraded': EventSpec('warning', 'yes', 'sys', 'integrity', ('n_none', 'n_total', 'reasons'), optional=(), dedupe='owner_degraded|day'),
    'team_member_added': EventSpec('info', 'digest', 'occ', 'integrity', ('member_id', 'name', 'role', 'source', 'status', 'by'), optional=('checked_at', 'evidence_count', 'domains', 'reason'), dedupe='team|member_id|type|checked_at or day'),
    'team_member_verified': EventSpec('info', 'digest', 'occ', 'integrity', ('member_id', 'name', 'role', 'source', 'status', 'by'), optional=('checked_at', 'evidence_count', 'domains', 'reason'), dedupe='team|member_id|type|checked_at or day'),
    'team_member_rejected': EventSpec('info', 'digest', 'occ', 'integrity', ('member_id', 'name', 'role', 'source', 'status', 'by'), optional=('checked_at', 'evidence_count', 'domains', 'reason'), dedupe='team|member_id|type|checked_at or day'),
    'team_member_delisted': EventSpec('info', 'digest', 'occ', 'integrity', ('member_id', 'name', 'role', 'source', 'status', 'by'), optional=('checked_at', 'evidence_count', 'domains', 'reason'), dedupe='team|member_id|type|checked_at or day'),
    'team_disclosure_changed': EventSpec('info', 'digest', 'occ', 'integrity', ('old', 'new', 'source'), optional=(), dedupe='occ|disc|new|day'),
    'tier_change': EventSpec('info', 'yes', 'occ', 'scoring', ('netuid', 'spec_version', 'as_of', 'from', 'to', 'direction', 'composite_smoothed', 'prev_composite_smoothed', 'composite', 'threshold', 'top_positive', 'top_negative', 'red_flags'), optional=('backfilled',), dedupe='occ|tier_change|spec|as_of'),
    'red_flag_raised': EventSpec('critical', 'yes', 'occ', 'scoring', ('netuid', 'spec_version', 'as_of', 'flag', 'multiplier', 'human', 'sticky', 'detail', 'flag_multiplier_total'), optional=(), dedupe='occ|red_flag_raised|flag|raised_at'),
    'red_flag_cleared': EventSpec('info', 'yes', 'occ', 'scoring', ('netuid', 'spec_version', 'as_of', 'flag', 'multiplier', 'raised_at', 'reason'), optional=(), dedupe='occ|red_flag_cleared|flag|as_of'),
    'score_jump': EventSpec('warning', 'yes', 'occ', 'scoring', ('netuid', 'spec_version', 'as_of', 'prev_as_of', 'prev_composite', 'composite', 'delta', 'pillar_deltas', 'flags_added', 'flags_removed', 'coverage_delta'), optional=(), dedupe='occ|score_jump|spec|as_of'),
    'insufficient_data': EventSpec('info', 'digest', 'occ', 'scoring', ('netuid', 'spec_version', 'as_of', 'confidence', 'coverage', 'missing'), optional=(), dedupe='occ|insufficient_data|spec|as_of'),
    'rank_instability': EventSpec('info', 'digest', 'sys', 'scoring', ('spec_version', 'as_of', 'series', 'lag_days', 'n', 'rank_autocorr', 'quintile_turnover', 'top20_turnover'), optional=(), dedupe='system|rank_instability|spec|as_of'),
    'manual_entry': EventSpec('info', 'no', 'occ', 'api', ('kind', 'by', 'row_id', 'summary', 'evidence_url'), optional=(), dedupe='NULL'),
    'collector_paused': EventSpec('info', 'no', 'sys', 'api', ('collector', 'by', 'reason'), optional=(), dedupe='NULL'),
    'collector_resumed': EventSpec('info', 'no', 'sys', 'api', ('collector', 'by'), optional=(), dedupe='NULL'),
    'rescore_requested': EventSpec('info', 'no', 'sys', 'api', ('as_of', 'spec_version', 'shadow', 'run_id', 'by'), optional=(), dedupe='run_id'),
    'dereg_watch': EventSpec('warning', 'yes', 'occ', 'api', ('netuid', 'name', 'reason', 'moving_price_rank', 'immune', 'blocks_left_immunity', 'dereg_safety'), optional=(), dedupe='occ|reason|day'),
    'collector_failing': EventSpec('warning', 'yes', 'sys', 'runtime', ('collector', 'consecutive_failures', 'last_error'), optional=(), dedupe='collector|first_failure_at'),
    'collector_recovered': EventSpec('info', 'yes', 'sys', 'runtime', ('collector', 'failures', 'down_seconds'), optional=(), dedupe='collector|first_failure_at'),
    'service_recovered_unclean': EventSpec('warning', 'yes', 'sys', 'ops', ('kind', 'downtime_sec', 'prev_ts', 'orphan_runs', 'sse_gap_blocks_est', 'missed'), optional=(), dedupe='boot_id'),
    'spend_80pct': EventSpec('warning', 'yes', 'sys', 'cost', ('provider', 'group', 'month', 'usd_mtd', 'cap_usd', 'pct', 'unit'), optional=('usd_month',), dedupe='group|month|80'),
    'spend_cap_reached': EventSpec('critical', 'yes', 'sys', 'cost', ('provider', 'group', 'month', 'usd_mtd', 'cap_usd', 'reserved_usd', 'denied_est_usd', 'paused_jobs'), optional=('unit', 'usd_month'), dedupe='group|month|cap'),
    'spend_reset': EventSpec('info', 'digest', 'sys', 'cost', ('group', 'month'), optional=('provider',), dedupe='group|month|reset'),
    'budget_override_set': EventSpec('info', 'digest', 'sys', 'cost', ('group', 'month', 'extra_usd', 'by', 'reason', 'kind'), optional=('provider',), dedupe='NULL'),
    'backup_failed': EventSpec('critical', 'yes', 'sys', 'ops', ('error', 'last_good_name', 'last_good_age_h'), optional=('backup_dir',), dedupe='day'),
    'backup_mirror_missing': EventSpec('warning', 'yes', 'sys', 'ops', ('mirror_dir', 'marker'), optional=(), dedupe='day'),
    'backup_mirror_failed': EventSpec('warning', 'yes', 'sys', 'ops', ('mirror_dir', 'error'), optional=(), dedupe='day'),
    'backup_stale': EventSpec('warning', 'yes', 'sys', 'ops', ('age_hours', 'stale_hours'), optional=(), dedupe='day'),
    'scoring_stalled': EventSpec('critical', 'yes', 'sys', 'scoring', ('slot_as_of', 'last_ok_as_of', 'missed_slots', 'last_error'), optional=(), dedupe='slot_as_of'),
    'discord_gateway_fatal': EventSpec('warning', 'yes', 'sys', 'community', ('error', 'close_code', 'retries', 'since'), optional=(), dedupe='since'),
    'backfill_done': EventSpec('info', 'digest', 'sys', 'runtime', ('job', 'era', 'rows', 'seconds', 'coverage_path'), optional=(), dedupe='job|run_id'),
    'backfill_failed': EventSpec('warning', 'yes', 'sys', 'runtime', ('job', 'era', 'error', 'resume_hint'), optional=(), dedupe='job|run_id'),
    'disk_low': EventSpec('warning', 'yes', 'sys', 'ops', ('free_gb', 'min_gb', 'db_gb', 'backups_gb', 'logs_gb'), optional=('path',), dedupe='day'),
    'disk_critical': EventSpec('critical', 'yes', 'sys', 'ops', ('free_gb', 'min_gb', 'db_gb', 'backups_gb', 'logs_gb'), optional=('path',), dedupe='day|6h_bucket'),
    'db_integrity_failed': EventSpec('critical', 'yes', 'sys', 'ops', ('quick_check',), optional=(), dedupe='boot_id'),
    'schema_warning': EventSpec('warning', 'yes', 'sys', 'storage', ('kind', 'version', 'detail'), optional=(), dedupe='kind|version'),
    'retention_skipped': EventSpec('warning', 'digest', 'sys', 'storage', ('reason', 'policies', 'backup_age_hours'), optional=(), dedupe='day'),
}

TAXONOMY_EVENT_TYPES: tuple[str, ...] = (
    "product_launch", "release_upgrade", "partnership_integration", "revenue_customer", "token_buyback_burn",
    "exchange_listing", "funding", "emissions_tokenomics_change", "team_change", "security_incident",
    "delay_missed_milestone", "governance_dispute", "regulatory", "owner_extraction_evidence", "giveaway_promo", "other",
)
