"""Frozen ts-1.0 constants of spec 11 §5 (master §7.3/§7.5). Not env keys: changing one is a new spec_version.
``tests/integrity/test_constants.py`` asserts equality with spec 12's reference functions."""
from __future__ import annotations

from app.util.time import parse_iso

RETENTION_WINDOW_DAYS = 30
SELL_IMPACT_WINDOW_DAYS = 7
SELL_IMPACT_MAX = 0.02
COMMIT_PCT_MIN = 0.05
COMMITMENT_ZERO_AGE_DAYS = 90
S3_WINDOW_DAYS = 90
S3_HALF_LIFE_DAYS = 45
S3_PENALTY_SWAP = 0.6
S3_PENALTY_REPOINT_CHURN = 0.4
S3_PENALTY_PERPETUAL_OFF = 1.0
REPOINT_CHURN_WINDOW_DAYS = 14
CONTROL_CHANGE_WINDOW_DAYS = 30
EXTRACTION_HARD_WINDOW_DAYS = 30
PERPETUAL_OFF_WINDOW_DAYS = 90
OVERHANG_FULL = 0.05
OVERHANG_ZERO = 0.25
FREE_ALPHA_PENALTY = 0.2
FREE_ALPHA_CUTOFF_UTC = "2026-05-08T00:00:00Z"        # hotfix v3.3.15-402 (§11 Q7: should become a block number)
FREE_ALPHA_CUTOFF_TS = parse_iso(FREE_ALPHA_CUTOFF_UTC)
TEAM_MAP = {"verified_2": 1.0, "verified_1": 0.6, "named": 0.3, "anonymous": 0.0}

DAY = 86400
HOUR = 3600
BLOCKS_PER_DAY = 7200
BLOCKS_PER_HOUR = 300

# Change types and their S3 base penalty (owner_changes.s3_base_penalty mirrors these).
COLDKEY_CHANGE_TYPES = frozenset({"coldkey_swap", "owner_transfer", "coldkey_change_unclassified",
                                  "subnet_owner_changed"})
S3_BASE_PENALTY = {**{t: S3_PENALTY_SWAP for t in COLDKEY_CHANGE_TYPES},
                   "hotkey_repoint_identity_churn": S3_PENALTY_REPOINT_CHURN,
                   "perpetual_off": S3_PENALTY_PERPETUAL_OFF}
# 11 §2.9 S3 evidence: change_type → spec 12's reference vocabulary
S3_REFERENCE_KIND = {**{t: "coldkey_swap_unannounced" for t in COLDKEY_CHANGE_TYPES},
                     "hotkey_repoint_identity_churn": "hotkey_repoint_identity_change",
                     "perpetual_off": "perpetual_lock_disabled"}
CHURN_IDENTITY_FIELDS = frozenset({"subnet_name", "github_repo", "subnet_url", "discord"})

P4_KEYS = ("team_doxxed", "owner_identity", "owner_commitment", "owner_retention_30d", "control_stability_90d",
           "owner_overhang", "tenure")
TRIGGER_KEYS = ("owner_extraction_trigger", "owner_control_change_trigger")
EVIDENCE_KEYS = ("owner_lock_ratio", "owner_dump_impact", "owner_holding_share", "owner_sell_impact_7d",
                 "owner_sell_ratio_30d", "team_verified_count", "team_candidate_count")
ALL_KEYS = P4_KEYS + TRIGGER_KEYS + EVIDENCE_KEYS
