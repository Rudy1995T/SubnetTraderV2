"""Label families (spec 13 §2.4, §8.4)."""
from __future__ import annotations

import sqlite3

import pytest

from app.quant.labels import LabelSource, make_label, parse_label
from app.quant.occupants import OccupantIndex, OccupantRow
from app.quant.panel_builder import PanelRequest, load_factor_panel

from .fixtures import DAY, day


@pytest.fixture
def ls(fixture_db):
    con = sqlite3.connect(fixture_db["path"])
    occ = OccupantIndex.load(con)
    return LabelSource.load(con, occupants=occ, made_at=day(10_000))


def test_label_end_and_censoring(ls, fixture_db):
    assert ls.label_end == day(420) + DAY
    k1 = [k for k in fixture_db["occ_by_key"] if k.startswith("1:")][0]
    assert ls.death_within(k1, day(100), 90) == 0
    assert ls.death_within(k1, day(400), 90) is None                 # right-censored, never assumed 0


def test_death_sources(ls, fixture_db):
    assert ls.death_within(fixture_db["k11"], day(300), 90) == 1      # deregistered, no successor
    assert ls.death_within(fixture_db["k9_old"], day(250), 90) == 1   # payload.old_key of netuid_reoccupied
    evs = ls.onsets(None, None, None, fixture_db["k13"], day(300), day(390))
    assert evs[0].kind == "zero_emission" and evs[0].onset == day(350)
    assert {e.kind for e in evs} == {"zero_emission", "emission_disabled"}
    assert ls.onsets(None, None, None, fixture_db["k13"], day(300), day(390), dereg_only=True) == []


def test_zero_eps_threshold():
    occ = OccupantIndex([OccupantRow("1:1", 1, 1, 0, 1 << 62, 0)])
    days = [d * DAY for d in range(30)]
    for share, expect in ((1e-4, 1), (2e-4, 0)):
        vals = [1.0] * 5 + [share] * 25
        src = LabelSource(occupants=occ, label_end=40 * DAY, shares={"1:1": (days, vals)}, disabled={},
                          zero_runs={"1:1": [5 * DAY]} if share <= 1e-4 else {}, day_ranks={})
        assert src.death_within("1:1", 0, 20) == expect
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE v_quant_chain_daily (occupant_key, day, emission_share)")
    con.execute("CREATE TABLE events (occupant_key, event_type, event_at, observed_at)")
    con.executemany("INSERT INTO v_quant_chain_daily VALUES (?,?,?)",
                    [("1:1", d * DAY, 1.0 if d < 5 else 5e-5) for d in range(12)]
                    + [("2:1", d * DAY, 1.0 if d < 5 else 2e-4) for d in range(12)])
    src = LabelSource.load(con, occupants=occ, made_at=100 * DAY, zero_run_days=7)
    assert src._zero_runs == {"1:1": [5 * DAY]}


def test_demis(ls, fixture_db):
    k11 = fixture_db["k11"]
    s0 = ls.share(k11, day(300))
    assert ls.demis(k11, day(300), 84) == pytest.approx(-s0)       # death in window → −share(t)
    k2 = [k for k in fixture_db["occ_by_key"] if k.startswith("2:")][0]
    assert ls.demis(k2, day(100), 28) == pytest.approx(0.0)
    assert ls.demis(k2, day(410), 28) is None                      # beyond data, alive


def test_gate_cross():
    occ = OccupantIndex([OccupantRow(k, 1, 1, 0, 1 << 62, 0) for k in ("a", "b", "c")]
                        + [OccupantRow("d", 2, 1, 0, 10 * DAY, 0)])
    ranks = {0: {"a": 1, "b": 3, "c": 2, "d": 1}, 28 * DAY: {"a": 3, "b": 1, "c": 2}}
    src = LabelSource(occupants=occ, label_end=100 * DAY, shares={}, disabled={}, zero_runs={}, day_ranks=ranks,
                      top_n=2)
    assert src.gate_cross("a", 0, 28) == -1 and src.gate_cross("b", 0, 28) == 1 and src.gate_cross("c", 0, 28) == 0
    assert src.gate_cross("d", 0, 28) == -1                        # death counts as leaving when it was in
    assert src.gate_cross("e", 0, 28) is None


def test_label_spec():
    assert parse_label("ret_log_7").step_bars(86400) == 7
    assert parse_label("demis_28").step_bars(14400) == 168
    assert parse_label("death_90").id == parse_label("death_90d").id == "death_90d"
    assert make_label("gate_cross", 28).id == "gate_cross_28d"
    with pytest.raises(ValueError):
        parse_label("ret_log_0")


def test_label_fn_uses_the_bucket(qsettings, fixture_db, ls):
    qsettings()
    panel = load_factor_panel(PanelRequest(scope="survivorship"), db_path=fixture_db["path"])
    fn = ls.label_fn(make_label("death", 90))
    i = panel.index_of[11][day(291)]
    assert fn(panel, 11, i) == 1                                   # dereg day 380 within (291, 381]
    i = panel.index_of[11][day(289)]
    assert fn(panel, 11, i) == 0                                   # day 380 is outside (289, 379]
