"""Spec 12 §9 test_tiers / test_pillar / test_velocity / test_explain / test_spec (pure parts)."""
import random

import pytest

from app.features.registry import METRICS, scored
from app.features.types import NormValue, Occupant
from app.scoring import spec as SP
from app.scoring.engine import ScoreCalculator, pillar_score, plan_slots, slot_for
from app.scoring.explain import impacts, top_lists
from app.scoring.tiers import assign_tier, ema
from app.storage.catalog import RED_FLAGS

DAY = 86400


@pytest.mark.parametrize("s,prev,exp", [
    (62, "B", "B"), (77.9, "B", "B"), (78, "B", "A"), (72.1, "A", "A"), (72, "A", "B"), (40, "A", "D"),
    (33, "F", "D"), (32.9, "F", "F"), (27, "D", "F"), (27.01, "D", "D"), (57, "B", "C"), (57.01, "B", "B"),
    (42, "C", "D"), (44, "C", "C"), (90, "D", "A"), (50, None, "C"), (None, "A", None)])
def test_tiers(s, prev, exp):
    assert assign_tier(s, prev) == exp


def test_ema():
    assert ema(None, 50) is None and ema(40, None) == 40
    assert ema(87.530731, 80.10) == pytest.approx(81.957683, abs=1e-6)


def test_pillar_kernel():
    assert pillar_score([(.5, None), (.5, 0.0)], .3) == (0.0, .5)
    sc, cov = pillar_score([(.10, 0.0), (.05, 0.0), (.15, 0.0), (.70, None)], .3)
    assert sc == 0.0 and cov == pytest.approx(.3)
    assert pillar_score([(.25, 1.0), (.75, None)], .3) == (None, .25)
    # port fidelity: the old quality_score's 7 fixed components (coverage ≥ 0.3) give the same value
    w = [.25, .15, .10, .20, .10, .05, .15]
    c = [1.0, None, .5, .2, None, 1.0, .4]
    avail = sum(x for x, v in zip(w, c) if v is not None)
    old = sum(x * v for x, v in zip(w, c) if v is not None) / avail
    assert pillar_score(list(zip(w, c)), .3)[0] == pytest.approx(old)


def _snap(fill: dict[str, float | None]) -> dict[str, NormValue]:
    return {k: NormValue(fill.get(k), None, "ok", None) for k, d in METRICS.items() if d.role == "scored"}


def test_velocity_cases():
    calc = ScoreCalculator(SP.TS_1_0)
    occ = Occupant("1:1", 1, 1, "active")
    base = {d.key: .5 for p in ("P1", "P2", "P3", "P4", "P5") for d in scored(p)}
    p6 = {d.key: .8 for d in scored("P6")}
    c = calc.calc(occ, _snap({**base, **p6, "net_flow_30d": .2, "chain_buys": .4, "attention_7d": .6}), .15)
    assert c.velocity == pytest.approx(100 * (.6 * (.4 * .8 + .25 * .8 + .2 * .6 + .15 * .8) + .4 * .4))
    nob = {**base, **p6, "net_flow_30d": None, "chain_buys": None, "attention_7d": None}
    c = calc.calc(occ, _snap(nob), .15)
    assert c.velocity == pytest.approx(80.0)
    nop6 = {**base, "net_flow_30d": .2, "chain_buys": .4}
    c = calc.calc(occ, _snap(nop6), .15)
    assert c.velocity == pytest.approx(30.0) and c.composite_raw == pytest.approx(.85 * c.health + .15 * 30)
    none = {**base, "net_flow_30d": None, "chain_buys": None}
    c = calc.calc(occ, _snap(none), .25)
    assert c.velocity is None and c.composite_raw == pytest.approx(c.health)
    assert dict(SP.TS_1_0.v_w_by_regime) == {"TRENDING": .25, "DISPERSED": .25, "CHOPPY": .15, "DEAD": .10,
                                             "UNKNOWN": .15}


def test_explain_identity_random():
    rng = random.Random(7)
    calc = ScoreCalculator(SP.TS_1_0)
    occ = Occupant("1:1", 1, 1, "active")
    for _ in range(200):
        fill = {k: (None if rng.random() < .25 else (0.0 if rng.random() < .1 else rng.random()))
                for k, d in METRICS.items() if d.role == "scored"}
        snap = _snap(fill)
        c = calc.calc(occ, snap, .15)
        if c.health is None:
            continue
        imp = impacts(SP.TS_1_0, snap, c.pillars, c.health_set)
        tot = sum(v for k, v in imp.items() if v is not None and METRICS[k].pillar != "P6")
        assert tot == pytest.approx(c.health - 50, abs=1e-9)
        pos, neg = top_lists(SP.TS_1_0, imp)
        assert len(pos) <= 3 and len(neg) <= 3
        assert all(x["impact"] > 0 for x in pos) and all(x["impact"] < 0 for x in neg)
        assert [(-x["impact"], x["metric"]) for x in pos] == sorted((-x["impact"], x["metric"]) for x in pos)


def test_spec_hash_and_flags():
    assert SP.spec_hash(SP.TS_1_0) == SP.GOLDEN_SPEC_HASH["ts-1.0"]
    assert {k: f.multiplier for k, f in SP.TS_1_0.flags.items()} == RED_FLAGS
    from dataclasses import replace
    assert SP.spec_hash(replace(SP.TS_1_0, ema_span=8)) != SP.GOLDEN_SPEC_HASH["ts-1.0"]
    with pytest.raises(SP.SpecUnknown):
        SP.get_spec("ts-9")
    assert not issubclass(SP.SpecHashMismatch, ImportError)


def test_settings_alias_and_official_hour():
    import app.config as C
    s = C.Settings()
    assert s.SCORING_SPEC_VERSION == s.SPEC_VERSION
    with pytest.raises(Exception):
        C.Settings(SCORING_OFFICIAL_HOUR=6)


def test_slot_planning():
    d = 1790121600
    assert slot_for(d + 15 * 60, [0, 6, 12, 18]) == d
    assert slot_for(d - 60, [0, 6, 12, 18]) == d - 6 * 3600
    import app.config as C
    s = C.Settings()
    plan = plan_slots(d - 3 * DAY, d + 20 * 60, s)
    assert [(p.as_of, p.trigger, p.mode, p.backfilled) for p in plan] == [
        (d - 2 * DAY, "catchup", "reconstructed", 1), (d - DAY, "catchup", "reconstructed", 1),
        (d, "schedule", "strict", 0)]
    assert plan[0].cutoff == d - 2 * DAY + 900
    s2 = C.Settings(SCORING_CATCHUP_MAX_SLOTS=1)
    assert [p.as_of for p in plan_slots(d - 5 * DAY, d + 1200, s2)] == [d - DAY, d]
    # missed provisional slots are skipped; no last_done → no catch-up
    assert [p.as_of for p in plan_slots(d - DAY + 6 * 3600, d + 13 * 3600, s)] == [d, d + 12 * 3600]
    assert [p.as_of for p in plan_slots(None, d + 1200, s)] == [d]
    late = plan_slots(None, d + 5 * 3600, s)[0]
    assert late.mode == "reconstructed" and late.cutoff == d + 900
