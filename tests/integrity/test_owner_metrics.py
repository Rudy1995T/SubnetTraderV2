"""Spec 11 §9 T21–T25, T30–T31, T43–T44: pure owner metrics against the brief's scenarios and 12's anchors."""
from __future__ import annotations

import inspect

import pytest

from app.features import normalise as N
from app.integrity import constants as K
from app.integrity import owner_metrics as M
from app.integrity.models import OwnerChange, StakeEvent

DAY = 86400
T = 1_790_000_000


def ev(kind, alpha, at, tao=None, uid=None, out=None):
    from app.integrity.classify import OUTFLOW_KINDS
    return StakeEvent(uid or f"{kind}{at}", "ck", kind, kind, int(kind in OUTFLOW_KINDS) if out is None else out, at,
                      alpha=alpha, tao=tao, extrinsic_id=f"x-{at}")


def s1(events, *, earned=21_600.0, cut=True, covered=True, pool=300_000.0):
    win = (T - 30 * DAY, T)
    imp = M.impact7_series(events, {h: pool for h in range(win[0], win[1], 3600)}, window=win)
    return M.s1_owner_retention(events, earned=earned, impact7=imp, owner_cut_enabled=cut, pre_launch=False,
                                activity_covered=covered, min_earned=1.0)


# ── S2 ───────────────────────────────────────────────────────────────────────────────────────────────────────
def test_s2_chutes_sn64():                                                         # T21
    m = M.s2_owner_commitment(perp=614_000, total=700_000, auto=True, commit_pct=0.225, owner_cut_enabled=True,
                              age_days=600)
    assert m.value_json["lock_ratio"] == pytest.approx(0.877143, abs=1e-6)
    assert m.value == pytest.approx(0.926286, abs=1e-6)


def test_s2_mvtrx_sn79():                                                          # T22
    m = M.s2_owner_commitment(perp=1_269_236.8, total=1_300_000, auto=False, commit_pct=0.508, owner_cut_enabled=True,
                              age_days=500)
    assert m.value_json["lock_ratio"] == pytest.approx(0.976336, abs=1e-6)
    assert m.value == pytest.approx(0.735802, abs=1e-6)


def test_s2_zero_rule_and_grace():                                                 # T23
    assert M.s2_owner_commitment(perp=0.0, total=5e5, auto=False, commit_pct=0.0, owner_cut_enabled=True,
                                 age_days=200).value == 0.0
    g = M.s2_owner_commitment(perp=0.0, total=5e5, auto=False, commit_pct=0.0, owner_cut_enabled=True, age_days=40)
    assert g.value is None and g.status == "not_applicable" and g.reason == "grace_period"
    assert M.s2_owner_commitment(perp=5e4, total=1e5, auto=None, commit_pct=0.1, owner_cut_enabled=True,
                                 age_days=300).value == pytest.approx(0.6)
    assert M.s2_owner_commitment(perp=10, total=20, auto=None, commit_pct=None, owner_cut_enabled=True,
                                 age_days=20).value == pytest.approx(0.5)
    m = M.s2_owner_commitment(perp=0.0, total=0.0, auto=None, commit_pct=0.0, owner_cut_enabled=False, age_days=200)
    assert m.value == pytest.approx(0.25) and m.value_json["auto_lock"] is True
    assert M.s2_owner_commitment(perp=None, total=1.0, auto=True, commit_pct=None, owner_cut_enabled=True,
                                 age_days=1).status == "failed"


# ── S1 ───────────────────────────────────────────────────────────────────────────────────────────────────────
def test_s1_covenant_dump():                                                       # T24
    evs = [ev("sell", 60_000, T - 3 * DAY, tao=5_000), ev("sell_full", 90_000, T - 2 * DAY, tao=7_000)]
    earned, method = M.earned_alpha({}, [0.1] * 30, window=(T - 30 * DAY, T))
    assert earned == pytest.approx(21_600) and method == "snapshots"
    m = s1(evs, earned=earned)
    assert m.value == 0.0 and m.status == "ok"
    assert m.value_json["sell_ratio"] == pytest.approx(6.944, abs=1e-3)
    assert m.value_json["max_7d_sell_impact"] == pytest.approx(0.04)
    assert m.value_json["full_exit"] is True
    assert m.value_json["hard"] == ["full_exit_extrinsic", "sell_impact_7d", "sell_ratio"]
    trig = M.extraction_trigger(m, [e for e in evs if e.kind == "sell_full"], [], observable=True)
    assert trig.value == 1.0 and trig.value_json["reasons"] == ["full_exit_extrinsic", "sell_impact_7d", "sell_ratio"]
    assert all(i.startswith("owner_stake_events:") for i in trig.value_json["trigger_ids"])


def test_s1_partial_and_disabled():                                                # T25
    assert s1([ev("sell", 5_400, T - DAY, tao=10)]).value == pytest.approx(0.75)
    assert s1([], earned=0.0, cut=False).value == 1.0
    m = s1([ev("sell", 500, T - DAY, tao=20)], earned=0.0, cut=False)
    assert m.value == 1.0 and m.value_json["sell_ratio"] is None             # 02 §C1 amended: no H3 while cut off
    m = s1([ev("sell_full", 50, T - DAY, tao=1)], earned=0.0, cut=False)
    assert m.value == 0.0 and m.value_json["hard"] == ["full_exit_extrinsic"]
    assert N.owner_retention(.5, None, True, False) == 0
    m = s1([ev("sell", 5_000, T - DAY, tao=9_000)], earned=0.0, cut=False)      # impact 3 %
    assert m.value == 0.0 and m.value_json["hard"] == ["sell_impact_7d"]
    m = s1([], covered=False)
    assert m.value is None and m.reason == "activity_coverage"
    m = s1([ev("sell_full", 10, T - DAY, tao=1)], covered=False)
    assert m.value == 0.0 and m.status == "ok"
    m = s1([ev("sell", 3, T - DAY, tao=0.1)], earned=0.5)
    assert m.value is None and m.reason == "earned_below_min" and m.value_json["sell_ratio"] is None
    assert s1([], earned=None).reason == "earned_unknown"


# ── S3, S5, identity, tenure, triggers ───────────────────────────────────────────────────────────────────────
def chg(ctype, at, cid=1):
    return OwnerChange("64:1", 64, ctype, at, at, "tmc_snapshot", old_value="a", new_value="b",
                       s3_base_penalty=K.S3_BASE_PENALTY.get(ctype, 0.0), id=cid)


def test_s3_decay_and_formal_exit():
    m = M.s3_control_stability([chg("coldkey_swap", T - 45 * DAY)], t=T, observable=True, whitelisted=lambda c: False)
    assert m.value == pytest.approx(0.7)
    assert m.value == pytest.approx(N.control_stability(m.value_json["events"], T))
    assert M.s3_control_stability([chg("coldkey_swap", T - 91 * DAY)], t=T, observable=True,
                                  whitelisted=lambda c: False).value == 1.0
    m = M.s3_control_stability([chg("perpetual_off", T - 80 * DAY)], t=T, observable=True, whitelisted=lambda c: False)
    assert m.value == 0.0
    assert M.s3_control_stability([chg("coldkey_swap", T - DAY)], t=T, observable=True,
                                  whitelisted=lambda c: True).value == 1.0
    assert M.s3_control_stability([], t=T, observable=False, whitelisted=lambda c: False).reason == "no_owner_snapshot"


def test_s5_overhang():                                                            # T30
    f = lambda avail, hidden=False, tao_in=100_000: M.s5_owner_overhang(avail=avail, price=0.05, tao_in=tao_in,  # noqa
                                                                       hidden_alpha=hidden).value
    assert f(100_000) == pytest.approx(1.0)
    assert f(500_000) == pytest.approx(0.0)
    assert f(300_000) == pytest.approx(0.5)
    assert f(300_000, hidden=True) == pytest.approx(0.3)
    assert M.s5_owner_overhang(avail=1, price=0.05, tao_in=0, hidden_alpha=False).reason == "not_started"
    assert M.s5_owner_overhang(avail=None, price=0.05, tao_in=1, hidden_alpha=False).reason == "no_position"


def test_owner_identity_and_tenure():                                              # T31
    kw = dict(owner_coldkey="5X")
    assert M.owner_identity_value("present", True, False, True, **kw).value == 1.0
    m = M.owner_identity_value("present", False, False, True, **kw)
    assert m.value == 0.0 and m.status == "absent" and m.reason == "no_url_or_github"
    assert M.owner_identity_value("absent", False, False, True, **kw).status == "absent"
    assert M.owner_identity_value("unknown", False, False, True, **kw).status == "failed"
    assert M.owner_identity_value("present", True, True, False, **kw).reason == "owner_key_mismatch"
    assert M.tenure_days(9_123_662, 8_693_284) == pytest.approx(59.77, abs=0.01)
    r = M.tenure_row(9_123_662, 8_693_284, head_source="chain_hourly")
    assert r.status == "ok" and r.value == pytest.approx(59.77, abs=0.01)
    assert M.tenure_row(9_123_662, None, head_source=None).reason == "no_registration_block"
    assert M.tenure_row(None, 1, head_source=None).reason == "no_head_block"


def test_triggers_and_trigger_ids():
    c = chg("coldkey_swap", T - 5 * DAY, cid=17)
    r = M.control_change_trigger([c], observable=True, churn=lambda x: False)
    assert r.value == 1.0 and r.value_json["trigger_ids"] == ["owner_changes:17"]
    assert r.value_json["identity_churn"] is False
    assert M.control_change_trigger([c], observable=True, churn=lambda x: True).value_json["identity_churn"] is True
    z = M.control_change_trigger([], observable=True, churn=lambda x: False)
    assert z.value == 0.0 and z.value_json["trigger_ids"] == []
    assert M.control_change_trigger([], observable=False, churn=lambda x: False).status == "failed"
    p = M.extraction_trigger(None, [], [chg("perpetual_off", T - 80 * DAY, cid=5)], observable=True)
    assert p.value == 1.0 and p.value_json["reasons"] == ["perpetual_off"]
    assert p.value_json["trigger_ids"] == ["owner_changes:5"]


# ── T43 spec 12 contract anchors, T44 constants ──────────────────────────────────────────────────────────────
def test_spec12_anchors():                                                         # T43
    assert N.owner_commitment(.85, False, .225, 1e5, 600) == pytest.approx(.66)
    assert N.owner_commitment(0, False, 0, 0, 300) == 0
    assert N.owner_commitment(.5, None, None, 10, 20) == pytest.approx(.5)
    assert N.owner_retention(.1, .001, False, True) == pytest.approx(.9)
    assert N.owner_retention(1.5, None, False, True) == 0
    assert N.owner_retention(.1, .03, False, True) == 0
    assert N.owner_retention(.5, None, False, False) == 1
    assert N.owner_overhang(.03, False) == 1
    assert N.owner_overhang(.18, False) == pytest.approx(.35)
    assert N.owner_overhang(.10, True) == pytest.approx(.55)
    # this spec's functions on equivalent inputs; and reference(**value_json inputs) == value
    cases = [M.s2_owner_commitment(perp=85, total=100, auto=False, commit_pct=.225, owner_cut_enabled=True,
                                   age_days=600),
             M.s2_owner_commitment(perp=0.0, total=100, auto=False, commit_pct=0.0, owner_cut_enabled=True,
                                   age_days=300),
             M.s2_owner_commitment(perp=10, total=20, auto=None, commit_pct=None, owner_cut_enabled=True, age_days=20)]
    assert [c.value for c in cases] == pytest.approx([.66, 0.0, .5])
    for c in cases:
        vj = c.value_json
        assert N.owner_commitment(vj["lock_ratio"], vj["auto_lock"], vj["commit_pct"], vj["perpetual_locked"],
                                  vj["age_days"]) == pytest.approx(c.value, abs=1e-9)
    for m in (s1([ev("sell", 2_160, T - DAY, tao=1)]), s1([ev("sell", 50_000, T - DAY, tao=1)]),
              s1([], earned=0.0, cut=False), s1([ev("sell_full", 1, T - DAY, tao=1)])):
        vj = m.value_json
        assert N.owner_retention(vj["sell_ratio"], vj["max_7d_sell_impact"], vj["full_exit"],
                                 vj["owner_cut_enabled"]) == pytest.approx(m.value, abs=1e-9)
    for di, legacy in ((.03, False), (.18, False), (.10, True)):
        m = M.s5_owner_overhang(avail=di * 1e5 / 0.05, price=0.05, tao_in=1e5, hidden_alpha=legacy)
        assert m.value == pytest.approx(N.owner_overhang(m.value_json["dump_impact"],
                                                          m.value_json["legacy_unaccounted"]), abs=1e-9)
    assert N.team_map(2, 0) == 1.0 and N.team_map(1, 0) == .6 and N.team_map(0, 1) == .3


def test_constants():                                                              # T44
    assert (K.SELL_IMPACT_MAX, K.COMMIT_PCT_MIN, K.OVERHANG_FULL, K.OVERHANG_ZERO, K.FREE_ALPHA_PENALTY) == \
        (0.02, 0.05, 0.05, 0.25, 0.2)
    assert (K.S3_PENALTY_SWAP, K.S3_PENALTY_REPOINT_CHURN, K.S3_HALF_LIFE_DAYS, K.S3_WINDOW_DAYS) == (0.6, 0.4, 45, 90)
    assert (K.CONTROL_CHANGE_WINDOW_DAYS, K.EXTRACTION_HARD_WINDOW_DAYS, K.PERPETUAL_OFF_WINDOW_DAYS) == (30, 30, 90)
    src = inspect.getsource(N.owner_retention) + inspect.getsource(N.owner_overhang) + \
        inspect.getsource(N.control_stability) + inspect.getsource(N.owner_commitment)
    for lit in ("0.02", "0.05, 0.25", "0.2", "0.6", "0.4", "45.0", "90", "0.05"):
        assert lit in src
    import app.config as app_config
    fields = set(app_config.Settings.model_fields)
    for name in ("SELL_IMPACT_MAX", "COMMIT_PCT_MIN", "OVERHANG_FULL", "S3_HALF_LIFE_DAYS", "FREE_ALPHA_PENALTY"):
        assert name not in fields and f"OWNER_{name}" not in fields
    assert set(K.EVIDENCE_KEYS) == {"owner_lock_ratio", "owner_dump_impact", "owner_holding_share",
                                    "owner_sell_impact_7d", "owner_sell_ratio_30d", "team_verified_count",
                                    "team_candidate_count"}
