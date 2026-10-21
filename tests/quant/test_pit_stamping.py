"""PIT stamping rules (spec 13 §2.1, §8.3; 12 §2.15 items 3–4) on a minimal in-memory DB."""
from __future__ import annotations

import json
import sqlite3

import pytest

from app.quant.backtestability import Coverage
from app.quant.pit import PITStamper

D = 86400
T = 1_790_000_000 - (1_790_000_000 % D)      # an official 00:00Z slot


@pytest.fixture
def con():
    c = sqlite3.connect(":memory:")
    c.execute("CREATE TABLE metric_values (occupant_key, metric_key, as_of, spec_version, stage, value, value_json, "
              "status, observed_at, backfilled)")
    c.execute("CREATE TABLE scores (occupant_key, as_of, spec_version, run_kind, composite_raw, composite, "
              "composite_smoothed, quality, velocity, confidence, pillar_scores, computed_at, input_cutoff, "
              "backfilled, shadow)")
    return c


def mv(con, occ, key, as_of, value, observed, *, stage="norm", raw=None, status="ok", bf=0, spec="ts-1.0"):
    vj = json.dumps({"raw": raw, "status": status}) if stage == "norm" else json.dumps({"status": status})
    con.execute("INSERT INTO metric_values VALUES (?,?,?,?,?,?,?,?,?,?)",
                (occ, key, as_of, spec, stage, value, vj, status, observed, bf))


def sc(con, occ, as_of, q, computed, cutoff, *, bf=0, shadow=0, spec="ts-1.0", kind="official", p1=0.5):
    con.execute("INSERT INTO scores VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (occ, as_of, spec, kind, q, q, q, q, None, 0.9, json.dumps({"P1": p1, "P2": 0.2}), computed, cutoff,
                 bf, shadow))


def load(con, keys, *, mode="strict", official=True, coverage=None, spec="ts-1.0", fresh=None):
    return PITStamper.load(con, keys, spec_version=spec, lo=0, hi=1 << 40, mode=mode,
                           freshness=fresh or {k: 2 * D for k in keys}, default_max_age=2 * D,
                           official_only=official, coverage=coverage)


def test_cutoff_is_the_bucket(con):
    mv(con, "1:1", "active_authors_28d", T, 0.7, T + 3600)
    st = load(con, ["active_authors_28d"])
    assert st.stamp("1:1", T + 3600) == {"active_authors_28d": 0.7}       # observed_at == bucket → used
    assert st.stamp("1:1", T + 3599) == {}                                 # observed after the decision instant
    # a row observed inside the bucket (after `bucket`, before the bar's close sample `ts`) is not admissible
    assert st.stamp("1:1", T) == {}


def test_raw_rows_never_used_for_metric_factors_and_raw_prefix(con):
    mv(con, "1:1", "emission_share", T, 0.4, T, raw=1.25)
    mv(con, "1:1", "emission_share", T, 99.0, T - 10, stage="raw")
    st = load(con, ["emission_share", "raw:emission_share"])
    assert st.stamp("1:1", T) == {"emission_share": 0.4, "raw:emission_share": 1.25}


def test_evidence_reads_raw_rows_with_b5_semantics(con):
    mv(con, "1:1", "owner_locked_share", T, 0.3, T, stage="raw")
    mv(con, "1:1", "owner_locked_share", T + D, None, T + D, stage="raw", status="failed")
    mv(con, "1:1", "owner_locked_share", T + 2 * D, 0.0, T + 2 * D, stage="raw", status="absent")
    st = load(con, ["evidence:owner_locked_share"])
    assert st.stamp("1:1", T + D) == {"evidence:owner_locked_share": 0.3}   # failed skipped → previous value
    assert st.stamp("1:1", T + 2 * D) == {"evidence:owner_locked_share": 0.0}


def test_revision_rule_newest_as_of_wins(con):
    mv(con, "1:1", "active_authors_28d", T + D, 0.8, T + D + 10)
    mv(con, "1:1", "active_authors_28d", T, 0.1, T + D + 20)       # older slot observed later
    st = load(con, ["active_authors_28d"])
    assert st.stamp("1:1", T + D + 30) == {"active_authors_28d": 0.8}


def test_staleness(con):
    mv(con, "1:1", "active_authors_28d", T, 0.7, T)
    st = load(con, ["active_authors_28d"])
    assert st.stamp("1:1", T + 2 * D) == {"active_authors_28d": 0.7}
    assert st.stamp("1:1", T + 2 * D + 1) == {}


def test_official_only_on_24h_grid(con):
    mv(con, "1:1", "active_authors_28d", T + 6 * 3600, 0.9, T + 6 * 3600)      # provisional slot
    assert load(con, ["active_authors_28d"]).stamp("1:1", T + 7 * 3600) == {}
    assert load(con, ["active_authors_28d"], official=False).stamp("1:1", T + 7 * 3600) == \
        {"active_authors_28d": 0.9}
    sc(con, "1:1", T + 6 * 3600, 40.0, T + 6 * 3600 + 900, T + 6 * 3600 + 900, kind="provisional")
    assert load(con, ["quality"]).stamp("1:1", T + 8 * 3600) == {}
    assert load(con, ["quality"], official=False).stamp("1:1", T + 8 * 3600) == {"quality": 40.0}


def test_score_eff_is_computed_at_and_backfilled_score(con):
    sc(con, "1:1", T, 55.0, T + 900, T + 900)
    st = load(con, ["quality", "P1"])
    assert st.stamp("1:1", T + 899) == {}
    assert st.stamp("1:1", T + 900) == {"quality": 55.0, "P1": 0.5}
    sc(con, "2:1", T, 60.0, T + 30 * D, T + 900, bf=1)
    assert load(con, ["quality"]).stamp("2:1", T + 31 * D) == {}                       # strict drops it
    rec = load(con, ["quality"], mode="reconstructed", fresh={"quality": 129600})
    assert rec.stamp("2:1", T + D - 1) == {}
    assert rec.stamp("2:1", T + D) == {"quality": 60.0}                                # eff = as_of + lag


def test_backfilled_metrics_reconstructed_mode_and_clamp(con):
    mv(con, "1:1", "emission_share", T, 0.4, T + 100 * D, bf=1)
    mv(con, "1:1", "emission_share", T - 50 * D, 0.2, T + 100 * D, bf=1)
    assert load(con, ["emission_share"]).stamp("1:1", T + D) == {}
    cov = Coverage(path="x", sha256="", schema="coverage-1", generated_at=0,
                   reconstructible_from={"emission_share": T - 10 * D})
    st = load(con, ["emission_share"], mode="reconstructed", coverage=cov)
    assert st.stamp("1:1", T + D) == {"emission_share": 0.4}
    assert st.stamp("1:1", T - 49 * D) == {}                                         # before reconstructible_from
    assert st.counters["dropped_before_reconstructible_from"] == 1


def test_forward_only_and_lookahead_keys_never_from_backfill(con):
    mv(con, "1:1", "site_uptime_30d", T, 0.9, T + 50 * D, bf=1)
    mv(con, "1:1", "x_engagement_30d", T, 0.9, T + 50 * D, bf=1)
    mv(con, "1:1", "x_engagement_30d", T + D, 0.5, T + D, bf=0)       # a live row is fine
    st = load(con, ["site_uptime_30d", "x_engagement_30d"], mode="reconstructed")
    assert st.stamp("1:1", T + D) == {"x_engagement_30d": 0.5}
    assert st.counters["dropped_forward_only"] == 2
    assert "forward_only_key_dropped:site_uptime_30d" in st.warnings


def test_shadow_rows(con):
    sc(con, "1:1", T, 30.0, T + 900, T + 900, shadow=1)
    sc(con, "1:1", T, 31.0, T + 900, T + 900, shadow=1, spec="ts-1.1")
    assert load(con, ["quality"]).stamp("1:1", T + D) == {}
    assert load(con, ["quality"], spec="ts-1.1").stamp("1:1", T + D) == {"quality": 31.0}


def test_occupants_never_mixed_and_null_values_skipped(con):
    mv(con, "9:1", "active_authors_28d", T, 0.7, T)
    mv(con, "9:2", "active_authors_28d", T, None, T)
    st = load(con, ["active_authors_28d"])
    assert st.stamp("9:2", T + 10) == {}
    assert st.stamp(None, T + 10) == {}
    assert st.stamp("9:1", T + 10) == {"active_authors_28d": 0.7}
