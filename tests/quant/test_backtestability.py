"""Backtestability matrix, factor-name rules and the coverage.json reader (spec 13 §2.3, §2.5)."""
from __future__ import annotations

import json

import pytest

from app.quant import backtestability as bt


def _cov(tmp_path, **over):
    d = {"schema": "coverage-1", "generated_at": 1, "reconstructible_from": {"emission_share": 1000, "github_p2": None},
         "forward_only": ["chain_buys"], "caveats": ["github_org_membership_asof_today", "moving_price_enriched_rows"],
         "holes": [{"series": "price_bars_1h", "from_ts": 100, "to_ts": 200, "netuids": "all"},
                   {"series": "pool_history", "from_ts": 0, "to_ts": 10, "netuids": "all"}],
         "series": {"price_bars_1h": {"completeness_by_month": {"2026-04": 0.5, "2026-05": 1.0}}},
         "checks": {"rpc_golden_block": "pass"}}
    d.update(over)
    p = tmp_path / "coverage.json"
    p.write_text(json.dumps(d))
    return p


def test_load_coverage(tmp_path):
    w: list[str] = []
    c = bt.load_coverage(_cov(tmp_path), w)
    assert w == [] and c.golden_ok and c.sha256
    assert c.clamp_for("emission_share") == (True, 1000)
    assert c.clamp_for("raw:emission_share") == (True, 1000)
    assert c.clamp_for("active_authors_28d") == (True, None)          # github_p2 null → no history
    assert c.clamp_for("tenure") == (False, None)
    assert c.in_hole(150) and not c.in_hole(5) and c.holes == [(100, 200)]
    assert c.low_completeness_months == ["2026-04"]
    assert "moving_price_enriched_rows" in c.caveats


def test_coverage_warnings(tmp_path):
    w: list[str] = []
    assert bt.load_coverage(tmp_path / "missing.json", w) is None and w == ["coverage_json_missing"]
    w = []
    assert bt.load_coverage(_cov(tmp_path, schema="coverage-0"), w) is None and w == ["coverage_schema_unknown"]
    c = bt.load_coverage(_cov(tmp_path, checks={"rpc_golden_block": "fail"}), [])
    assert c is not None and not c.golden_ok


def test_forward_only_override(tmp_path):
    c = bt.load_coverage(_cov(tmp_path), [])
    w: list[str] = []
    assert bt.is_reconstructible("emission_share", c, w)
    # chain_buys: forward-only in both 13 and 17 since 2026-10-22 (13 §11 item 14) → no disagreement warning
    assert not bt.is_reconstructible("chain_buys", c, w) and w == []
    assert bt.is_reconstructible("control_stability_90d", c, w) and w == []     # 11 §11 item 12 confirmed
    assert not bt.is_reconstructible("x_engagement_30d", None)
    assert not bt.is_reconstructible("site_uptime_30d", None)


@pytest.mark.parametrize("old,new", [("owner_conviction", "owner_commitment"),
                                     ("owner_retention", "owner_retention_30d"),
                                     ("identity_stability", "control_stability_90d")])
def test_retired_names(old, new):
    with pytest.raises(bt.FactorNameError, match=new):
        bt.classify(old)


def test_classify():
    assert bt.classify("size") == "chain" and bt.classify("chain:emission_share") == "chain_state"
    assert bt.classify("quality_cg") == "quality_cg" and bt.classify("P3") == "pillar"
    assert bt.classify("emission_share") == "metric" and bt.classify("raw:gate_margin") == "raw"
    assert bt.classify("evidence:owner_locked_share") == "evidence"
    for bad in ("owner_locked_share", "evidence:emission_share", "raw:owner_locked_share", "nonsense"):
        with pytest.raises(bt.FactorNameError):
            bt.classify(bad)
    assert bt.pit_keys_for(["quality_cg", "size", "P1"]) == ("P1", "P2")


@pytest.mark.parametrize("name", ["x_engagement_30d", "polarity_7d", "P5", "P6", "velocity", "quality",
                                  "composite_smoothed"])
def test_lookahead_caveat(name):
    assert bt.X_CAVEAT in bt.lookahead_caveats([name])


def test_github_caveat_and_none():
    assert bt.lookahead_caveats(["active_authors_28d"]) == [bt.GITHUB_CAVEAT]
    assert bt.GITHUB_CAVEAT in bt.lookahead_caveats(["quality_cg"])
    assert bt.lookahead_caveats(["size", "emission_share"]) == []


def test_freshness_fallback():
    assert bt.freshness_for("emission_share", 5) == 86400
    assert bt.freshness_for("raw:active_authors_28d", 5) == 2 * 86400
    assert bt.freshness_for("x_followers", 5) == 3 * 86400
    assert bt.freshness_for("team_doxxed", 5) == 90 * 86400
    assert bt.freshness_for("unknown_key", 5) == 5
