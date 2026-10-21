"""Spec 12 §2.4 / §9 test_registry: the 62-row ts-1.0 registry and register_metric."""
import math

import pytest

from app.features import registry as R
from app.scoring.golden import GOLDEN_REGISTRY_HASH
from app.storage.catalog import METRIC_KEYS, RETIRED_METRIC_KEYS

MASTER = {
    "P1": {"emission_share", "gate_margin", "dereg_safety", "liquidity_depth", "exit_capacity", "net_flow_30d",
           "chain_buys", "holder_dispersion", "participation", "miner_burn_ok"},
    "P2": {"active_authors_28d", "fte_devs_84d", "commit_days_90d", "declared_repo_freshness", "org_freshness",
           "release_cadence_90d", "review_practice_90d", "bus_factor", "repo_hygiene", "originality"},
    "P3": {"site_uptime_30d", "tls_ok", "site_age", "site_freshness", "docs_present", "product_status"},
    "P4": {"team_doxxed", "owner_identity", "owner_commitment", "owner_retention_30d", "control_stability_90d",
           "owner_overhang", "tenure"},
    "P5": {"venue_reachable", "discord_members", "presence_ratio", "discord_growth_30d", "announcement_cadence_30d",
           "x_engagement_30d", "x_followers", "silence_days"},
    "P6": {"event_impact_14d", "polarity_7d", "attention_7d", "kol_coverage_30d"},
}


def test_counts_and_pillar_sets():
    M = R.METRICS
    assert len(M) == 62
    assert sum(d.role == "scored" for d in M.values()) == 45
    assert sum(d.role == "leg" for d in M.values()) == 9 and sum(d.role == "aux" for d in M.values()) == 8
    for p, keys in MASTER.items():
        assert {d.key for d in R.scored(p)} == keys
        assert abs(math.fsum(d.weight for d in R.scored(p)) - 1.0) <= 1e-9
    assert dict(R.PILLAR_WEIGHTS) == {"P1": 30, "P2": 20, "P3": 10, "P4": 18, "P5": 12} and "P6" not in R.PILLAR_WEIGHTS


def test_specific_rows():
    M = R.METRICS
    assert {k for k, d in M.items() if d.normaliser == "prenorm"} == {
        "team_doxxed", "owner_identity", "owner_commitment", "owner_retention_30d", "control_stability_90d",
        "owner_overhang"}
    t = M["tenure"]
    assert t.normaliser == "tenure" and dict(t.params) == {"lo": 120, "hi": 365, "floor": .3}
    assert t.fallback_key == "occupant_age_days"
    assert M["participation"].normaliser == "zclip" and dict(M["participation"].params) == {"lo": .2, "hi": .9}
    assert M["kol_coverage_30d"].fallback_key == "x_kol_posts_30d"
    for k in ("owner_extraction_trigger", "owner_control_change_trigger"):
        assert M[k].role == "aux"
    for k in ("x_cadence_30d", "discord_announcements_30d", "tg_posts_30d", "x_last_activity_days",
              "discord_last_activity_days", "tg_last_activity_days"):
        assert M[k].role == "leg"


def test_freshness_per_c6():
    M = R.METRICS
    for k, d in M.items():
        if d.source_spec == "05":
            assert d.freshness_days == 1, k
        elif d.source_spec in ("06", "07", "08"):
            assert d.freshness_days == 2, k
        elif d.source_spec == "09":
            assert d.freshness_days == 3, k
        elif d.source_spec in ("10", "12"):
            assert d.freshness_days == 1, k
        elif d.source_spec == "11":
            assert d.freshness_days == (90 if k == "team_doxxed" else 2), k


def test_catalog_agreement_and_fn_names():
    for k, d in R.METRICS.items():
        assert k not in RETIRED_METRIC_KEYS
        cat = METRIC_KEYS[k]
        assert cat.role == d.role and cat.weight == pytest.approx(d.weight)
        if d.role == "scored":
            assert cat.pillar == d.pillar
        if d.producer == "fn":
            assert d.fn.endswith(f":compute_{k}")
    assert R.registry_hash() == GOLDEN_REGISTRY_HASH["ts-1.0"]


def test_validate_rejects_edits():
    rows = list(R._TABLE)
    from dataclasses import replace
    n, d = rows[0]
    for bad in (replace(d, weight=.25), replace(d, normaliser="zclip"), replace(d, pillar="P2"),
                replace(d, freshness_days=0)):
        with pytest.raises(R.RegistryError):
            R._validate([(n, bad), *rows[1:]], check_golden=False)
    assert not issubclass(R.RegistryError, ImportError)
    R._validate(check_golden=False)


def test_register_metric(monkeypatch):
    monkeypatch.setattr(R, "ATTACHED_FNS", {})
    monkeypatch.setattr(R, "EVIDENCE_FNS", {})

    async def f(db, o, t):
        return None
    R.register_metric("event_impact_14d", pillar="P6", fn=f, freshness_days=1)
    assert R.ATTACHED_FNS["event_impact_14d"] is f
    R.register_metric("x_cadence_30d", pillar="P5", component_of="announcement_cadence_30d", normaliser=None,
                      weight=None, freshness_days=3, fn=f)
    assert R.ATTACHED_FNS["x_cadence_30d"] is f
    R.register_metric("video_uploads_90d", fn=f)
    assert R.EVIDENCE_FNS["video_uploads_90d"] is f and "video_uploads_90d" not in R.METRICS
    with pytest.raises(R.RegistryError):
        R.register_metric("event_impact_14d", pillar="P5", fn=f)
    with pytest.raises(R.RegistryError):
        R.register_metric("x_cadence_30d", fn=f, freshness_days=2)
    with pytest.raises(R.RegistryError):
        R.register_metric("x_cadence_30d", fn=f, component_of="silence_days")
    with pytest.raises(R.RegistryError):
        R.register_metric("not_a_key", fn=f)
    R.register_metric("silence_days", fn=f)                 # derived: accepted, never attached/called
    assert "silence_days" not in R.ATTACHED_FNS


def test_community_registers_its_producers():
    import app.features.community as C  # noqa: F401  (import registers)
    for k in ("venue_reachable", "discord_members", "presence_ratio", "discord_growth_30d",
              "discord_announcements_30d", "tg_posts_30d", "discord_last_activity_days", "tg_last_activity_days"):
        assert k in R.ATTACHED_FNS
    assert C.presence_params() == dict(R.METRICS["presence_ratio"].params)
