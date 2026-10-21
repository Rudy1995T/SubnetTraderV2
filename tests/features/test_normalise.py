"""Spec 12 §9 test_normalise + test_rankg: anchors, monotonicity, None/absent handling, rankg cohorts."""
import math

import pytest

from app.features import normalise as N
from app.features.registry import METRICS


def test_anchors():
    assert N.recency(97, 14, 180) == pytest.approx(.5)
    assert N.logcap(10, 10) == 1.0
    assert N.zclip_log2(1, -2, 1) == pytest.approx(2 / 3)
    assert N.zclip_log2(.25, -2, 1) == 0 and N.zclip_log2(2, -2, 1) == 1 and N.zclip_log2(0, -2, 1) == 0
    assert N.age_ramp(30, 30, 365, .2) == pytest.approx(.2)
    assert N.age_ramp(197.5, 30, 365, .2) == pytest.approx(.6)
    assert N.age_ramp(365, 30, 365, .2) == 1
    assert N.tenure(100, 120, 365, .3) == .3 and N.tenure(300, 120, 365, .3) == pytest.approx(.734694, abs=1e-6)
    assert N.tenure(400, 120, 365, .3) == 1
    assert N.event_impact(0, 2) == .5 and N.event_impact(2.30633, 2) == pytest.approx(.909400, abs=1e-5)
    pr = dict(METRICS["presence_ratio"].params)
    assert N.presence_ratio(.01, 2000, **pr) == 0 and N.presence_ratio(.01, 500, **pr) == pytest.approx(.2)
    assert N.presence_ratio(.2, 500, **pr) == 1 and N.presence_ratio(1.0, 500, **pr) == .5
    assert N.presence_ratio(.3513, 390, **pr) == pytest.approx(.998143, abs=1e-6)
    assert N.presence_ratio(.2, None, **pr) is None
    assert N.prenorm(.66) == .66 and N.prenorm(1.2) is None and N.prenorm(None) is None
    assert [N.bus_factor(k) for k in (0, 1, 2, 3)] == [0, .3, .6, 1]
    assert N.checklist(None, items={"a": 1, "b": 0, "c": None, "d": True}) == pytest.approx(2 / 3)
    assert N.checklist(.4) == .4


def test_reference_functions():
    assert N.owner_commitment(.85, False, .225, 1e5, 600) == pytest.approx(.66)
    assert N.owner_commitment(0, False, 0, 0, 300) == 0
    assert N.owner_commitment(.5, None, None, 10, 20) == pytest.approx(.5)
    assert N.owner_retention(.1, .001, False, True) == pytest.approx(.9)
    assert N.owner_retention(1.5, None, False, True) == 0 and N.owner_retention(.1, .03, False, True) == 0
    assert N.owner_retention(.5, None, False, False) == 1 and N.owner_retention(.5, None, True, False) == 0
    assert N.owner_retention(.1, .03, False, False) == 0 and N.owner_retention(1.5, None, False, False) == 0
    t = 1790121600
    assert N.control_stability([{"kind": "coldkey_swap_unannounced", "event_at": t - 45 * 86400}], t) == pytest.approx(.7)
    assert N.control_stability([{"kind": "perpetual_lock_disabled", "event_at": t - 86400}], t) == 0
    assert N.owner_overhang(.03, False) == 1 and N.owner_overhang(.18, False) == pytest.approx(.35)
    assert N.owner_overhang(.10, True) == pytest.approx(.55)


def _sweep():
    x = -5.0
    while x <= 500:
        yield x
        x = round(x + .1, 6)


@pytest.mark.parametrize("f,inc", [
    (lambda x: N.recency(x, 14, 180), False), (lambda x: N.zclip_inv(x, 0, 10), False),
    (lambda x: N.owner_overhang(x / 100, False), False),
    (lambda x: N.logcap(x, 10), True), (lambda x: N.zclip(x, 0, 100), True), (lambda x: N.zclip_log2(x, -2, 1), True),
    (lambda x: N.tenure(x, 120, 365, .3), True), (lambda x: N.age_ramp(x, 30, 365, .2), True),
    (lambda x: N.event_impact(x, 2), True)])
def test_monotone_and_bounded(f, inc):
    prev = None
    for x in _sweep():
        y = f(x)
        assert 0 <= y <= 1
        if prev is not None:
            assert (y >= prev - 1e-12) if inc else (y <= prev + 1e-12)
        prev = y


def test_prenorm_monotone_on_unit():
    vals = [N.prenorm(i / 1000) for i in range(1001)]
    assert vals == sorted(vals)


def test_none_passthrough_and_status_wrapper():
    for f in (N.identity, N.binary, N.bus_factor, N.prenorm):
        assert f(None) is None
    assert N.recency(None, 1, 2) is None and N.logcap(None, 3) is None and N.zclip(None, 0, 1) is None
    for d in METRICS.values():
        if d.role != "scored" or d.cross_sectional:
            continue
        assert N.normalise_value(d, 5.0, "absent", {}) == 0.0
        assert N.normalise_value(d, None, "failed", {}) is None
        assert N.normalise_value(d, None, "not_applicable", {}) is None
        assert N.normalise_value(d, None, "none", {}) is None
    with pytest.raises(N.PrenormOutOfRange):
        N.normalise_value(METRICS["owner_commitment"], 1.3, "ok", {})


# ── rankg ────────────────────────────────────────────────────────────────────────────────────────────────────
def test_rankg_tied_zeros():
    vals = {f"z{i}": 0.0 for i in range(37)} | {f"p{i}": float(i + 1) for i in range(85)}
    out = N.rankg(vals)
    assert len(vals) == 122
    assert {round(out[f"z{i}"], 5) for i in range(37)} == {.29411}


def test_rankg_edges():
    assert set(N.rankg({str(i): 1.0 for i in range(20)}).values()) == {.5}
    assert set(N.rankg({str(i): float(i) for i in range(9)}).values()) == {None}
    assert None not in N.rankg({str(i): float(i) for i in range(10)}).values()
    big = N.rankg({str(i): float(i) for i in range(1000)})
    assert big["0"] == 0.0 and big["999"] == 1.0
    pos = {str(i): (i + 1) * 1.7 for i in range(30)}
    assert N.rankg(pos) == N.rankg({k: math.log(v) for k, v in pos.items()})
