"""Spec 12 §9 test_flags: evaluators, the §2.8 state machine (hysteresis, human hold, manual clear + D5 suppression,
sticky control change) and the 0.25 floor."""
import pytest

from app.features.types import Occupant, RawRow
from app.scoring.red_flags import FLAGS_TS_1_0 as F, FlagInputs, FlagState, multiplier, step
from app.storage.catalog import RED_FLAGS

DAY = 86400
D0 = 1790121600
OCC = Occupant("9:9", 9, 9, "active")


def _st(fd, trig, s, day, *, detail=None, manual=None):
    return step(s, fd, trig, detail or {}, manual, D0 + day * DAY, spec_version="ts-1.0", occupant_key="9:9",
                computed_at=D0 + day * DAY + 900)


def test_multipliers_and_floor():
    assert {k: f.multiplier for k, f in F.items()} == RED_FLAGS
    fl = [F["emission_disabled"], F["dereg_imminent"], F["miner_burn_zeroed"], F["owner_extraction"]]
    assert multiplier(fl) == .25 and multiplier([]) == 1.0
    assert multiplier([F["dereg_imminent"], F["stale_identity"]]) == pytest.approx(.54)


def test_state_columns_are_03s():
    cols = set(FlagState("o", "f", "s").as_row())
    assert cols == {"occupant_key", "flag", "spec_version", "active", "raised_at", "last_true_at", "false_since",
                    "cleared_at", "human", "sticky", "last_manual_clear_at", "suppress_until", "last_eval",
                    "last_eval_as_of", "detail_json", "updated_at"}


def test_hysteresis_timeline():
    fd = F["dereg_imminent"]
    s, tr = _st(fd, True, None, 0)
    assert tr == "raised" and s.active and s.raised_at == D0
    s, tr = _st(fd, False, s, 1)
    assert s.active and s.false_since == D0 + DAY and s.state() == "clearing"
    s, _ = _st(fd, None, s, 2)                       # None neither resets nor advances
    s, _ = _st(fd, None, s, 3)
    assert s.false_since == D0 + DAY
    for d in range(4, 8):
        s, tr = _st(fd, False, s, d)
        assert tr is None and s.active and s.clear_streak_days(D0 + d * DAY) == d - 1
    s, tr = _st(fd, False, s, 8)
    assert tr == "cleared_hysteresis" and not s.active and s.cleared_at == D0 + 8 * DAY
    # a True on d4 resets the clock
    s, _ = _st(fd, True, None, 0)
    s, _ = _st(fd, False, s, 1)
    s, _ = _st(fd, True, s, 4)
    assert s.false_since is None
    s, _ = _st(fd, False, s, 5)
    s, tr = _st(fd, False, s, 11)
    assert tr is None and s.active
    s, tr = _st(fd, False, s, 12)
    assert tr == "cleared_hysteresis"


def test_human_confirmed_holds_then_manual_clear_suppresses():
    fd = F["owner_extraction"]
    conf = {"state": "confirmed", "set_at": D0 - 10}
    s, tr = _st(fd, False, None, 0, manual=conf)
    assert tr == "raised" and s.human == 1
    for d in range(1, 31):
        s, tr = _st(fd, False, s, d, manual=conf)
        assert s.active and s.human and s.false_since is None and tr is None
    clr = {"state": "cleared", "set_at": D0 + 31 * DAY - 5}
    s, tr = _st(fd, True, s, 31, manual=clr, detail={"trigger_ids": ["owner_stake_events:1"]})
    assert tr == "cleared_manual" and not s.active and s.suppress_until == D0 + 61 * DAY
    s, tr = _st(fd, True, s, 32, manual=clr, detail={"trigger_ids": ["owner_stake_events:1"]})
    assert tr is None and not s.active                                 # same evidence stays cleared
    s, tr = _st(fd, True, s, 33, manual=clr, detail={"trigger_ids": ["owner_stake_events:1", "owner_stake_events:2"]})
    assert tr == "raised"                                              # new trigger id re-raises at once


def test_old_evidence_reraises_after_30d():
    fd = F["owner_extraction"]
    ids = {"trigger_ids": ["a"]}
    s, _ = _st(fd, True, None, 0, detail=ids)
    clr = {"state": "cleared", "set_at": D0 + DAY - 1}
    s, tr = _st(fd, True, s, 1, manual=clr, detail=ids)
    assert tr == "cleared_manual"
    s, tr = _st(fd, True, s, 30, manual=clr, detail=ids)
    assert tr is None
    s, tr = _st(fd, True, s, 31, manual=clr, detail=ids)
    assert tr == "raised"


def test_manual_clear_without_state_ignored():
    s, tr = _st(F["owner_control_change"], False, None, 0, manual={"state": "cleared", "set_at": D0})
    assert s is None and tr is None


def test_control_change_plain_and_sticky():
    fd = F["owner_control_change"]
    s, tr = _st(fd, True, None, 0, detail={"identity_churn": False})
    assert tr == "raised" and not s.sticky
    for d in range(1, 8):
        s, tr = _st(fd, False, s, d)
    s, tr = _st(fd, False, s, 8)
    assert tr == "cleared_hysteresis"
    s, tr = _st(fd, True, None, 0, detail={"identity_churn": True})
    assert s.sticky
    for d in range(1, 61):
        s, tr = _st(fd, False, s, d)
        assert s.active and tr is None
    s, tr = _st(fd, False, s, 61, manual={"state": "cleared", "set_at": D0 + 61 * DAY - 1})
    assert tr == "cleared_manual" and not s.sticky
    s, _ = _st(fd, True, None, 0, detail={"identity_churn": False})
    s, tr = _st(fd, True, s, 1, manual={"state": "cleared", "set_at": D0 + DAY - 1})
    assert tr == "cleared_manual"                                     # non-sticky manual clear works


def _row(key, value, status="ok", as_of=D0, **vj):
    return RawRow("9:9", key, as_of, value, {"status": status, **vj}, status, as_of, "chain", None, 0)


def _fi(raw=None, series=None, events=None, occ=OCC):
    return FlagInputs(raw=raw or {}, series=series or {}, events=events or [], manual={}, occupant=occ, slot=D0,
                      cutoff=D0 + 900)


def test_owner_extraction_trigger_row():
    t, d = F["owner_extraction"].evaluate(_fi({"owner_extraction_trigger": _row(
        "owner_extraction_trigger", 1.0, reasons=["full_exit"], trigger_ids=["b", "a"])}))
    assert t is True and d["trigger_ids"] == ["a", "b"]
    assert F["owner_extraction"].evaluate(_fi({"owner_extraction_trigger": _row("x", None, "failed")}))[0] is None


def test_miner_burn_and_inflated_series():
    fd = F["miner_burn_zeroed"]
    ser = {"implied_miner_burn": {D0 - k * DAY: _row("implied_miner_burn", .9, as_of=D0 - k * DAY) for k in range(3)}}
    assert fd.evaluate(_fi(series=ser))[0] is True
    ser["implied_miner_burn"][D0 - DAY] = _row("implied_miner_burn", .8, as_of=D0 - DAY)
    assert fd.evaluate(_fi(series=ser))[0] is False                   # 0.8 exactly → False
    del ser["implied_miner_burn"][D0 - DAY]
    ser["implied_miner_burn"][D0 - 2 * DAY] = _row("implied_miner_burn", .9, as_of=D0 - 2 * DAY)
    assert fd.evaluate(_fi(series=ser))[0] is None
    fd = F["inflated_community"]
    pr = {D0 - k * DAY: _row("presence_ratio", .01, as_of=D0 - k * DAY, members=2000) for k in range(7)}
    assert fd.evaluate(_fi(series={"presence_ratio": pr}))[0] is True
    del pr[D0 - 3 * DAY]
    assert fd.evaluate(_fi(series={"presence_ratio": pr}))[0] is None
    pr[D0 - 3 * DAY] = _row("presence_ratio", .2, as_of=D0 - 3 * DAY, members=2000)
    assert fd.evaluate(_fi(series={"presence_ratio": pr}))[0] is False


def test_stale_identity_three_terms():
    fd = F["stale_identity"]
    ev = [{"id": 1, "event_type": "identity_changed", "event_at": D0 - 40 * DAY, "observed_at": D0 - 40 * DAY,
           "payload": {"field": "subnet_name", "kind": "changed"}}]
    venue0 = {"venue_reachable": _row("venue_reachable", 0.0, "absent")}
    sil_abs = {"silence_days": _row("silence_days", 0.0, "absent")}
    assert fd.evaluate(_fi({**venue0, **sil_abs}, events=ev))[0] is True
    assert fd.evaluate(_fi({**venue0, **sil_abs}))[0] is False                   # no name turnover
    assert fd.evaluate(_fi({**venue0, "silence_days": _row("silence_days", 10.0)}, events=ev))[0] is False
    assert fd.evaluate(_fi(sil_abs, events=ev))[0] is None                        # venue missing
    junk = [{"id": 2, "event_type": "identity_junk", "event_at": D0 - DAY, "observed_at": D0 - DAY,
             "payload": {"junk_reason": "abandonment_marker"}}]
    assert fd.evaluate(_fi({**venue0, **sil_abs}, events=junk))[0] is True
    old = [{**ev[0], "event_at": D0 - 91 * DAY}]
    assert fd.evaluate(_fi({**venue0, **sil_abs}, events=old))[0] is False


def test_site_down_and_repo_flags():
    fd = F["site_down_7d"]
    evs = [{"id": 5, "event_type": "site_down_7d", "event_at": D0 - DAY, "observed_at": D0 - DAY, "payload": {}}]
    t, d = fd.evaluate(_fi(events=evs))
    assert t is True and d["trigger_event_id"] == 5
    evs.append({"id": 6, "event_type": "site_recovered", "event_at": D0 - 3600, "observed_at": D0 - 3600, "payload": {}})
    assert fd.evaluate(_fi(events=evs))[0] is False
    assert fd.evaluate(_fi())[0] is None
    assert fd.evaluate(_fi({"site_uptime_30d": _row("site_uptime_30d", .9)}))[0] is False
    s, tr = step(None, fd, True, {"trigger_event_id": 5}, None, D0, spec_version="ts-1.0", occupant_key="9:9",
                 computed_at=D0)
    assert s.detail_json["trigger_event_id"] == 5
    rg = F["repo_gone"]
    assert rg.evaluate(_fi({"declared_repo_freshness": _row("d", 0.0, "absent", reason="repo_gone")}))[0] is True
    assert rg.evaluate(_fi({"declared_repo_freshness": _row("d", 5.0)}))[0] is False
    ar = F["all_repos_archived"]
    assert ar.evaluate(_fi({"org_freshness": _row("o", 9.0, all_archived=True)}))[0] is True
    assert ar.evaluate(_fi({"org_freshness": _row("o", 9.0, declared_archived=True, successor_repo_id=None)}))[0]
    assert ar.evaluate(_fi({"org_freshness": _row("o", 9.0, declared_archived=True, successor_repo_id=4)}))[0] is False


def test_chain_flags():
    assert F["emission_disabled"].evaluate(_fi({"emission_enabled_flag": _row("e", 0.0)}))[0] is True
    pre = Occupant("9:9", 9, 9, "pre_launch")
    assert F["emission_disabled"].evaluate(_fi({"emission_enabled_flag": _row("e", 0.0)}, occ=pre))[0] is False
    dr = F["dereg_imminent"]
    assert dr.evaluate(_fi({"dereg_risk_flag": _row("d", 1.0), "immune_flag": _row("i", 0.0)}))[0] is True
    assert dr.evaluate(_fi({"dereg_risk_flag": _row("d", 1.0), "immune_flag": _row("i", 1.0)}))[0] is False
    assert dr.evaluate(_fi({"dereg_risk_flag": _row("d", 1.0)}))[0] is None
    s45 = F["silence_45d"]
    assert s45.evaluate(_fi({"silence_days": _row("s", 45.0)}))[0] is True
    assert s45.evaluate(_fi({"silence_days": _row("s", 0.0, "absent")}))[0] is False
    assert F["key_compromise"].evaluate(_fi())[0] is None
