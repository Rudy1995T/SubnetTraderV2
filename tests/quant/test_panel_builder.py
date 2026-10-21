"""Panel I/O over 03's v_quant_* views (spec 13 §2.1–§2.3, §8.2) on the synthetic snapshot."""
from __future__ import annotations

import math
import shutil
import sqlite3

import pytest

from app.quant.occupants import OccupantIndex
from app.quant.panel_builder import PanelRequest, load_factor_panel

from .fixtures import DAY, day


def _depth(nu: int, h: float) -> float:
    return (4000.0 + 900.0 * nu) * (1 + 0.1 * math.sin(h / 9 + nu))


def _bar(panel, nu, t):
    return panel.series[nu][panel.index_of[nu][t]]


@pytest.fixture
def surv(qsettings, fixture_db):
    qsettings()
    return load_factor_panel(PanelRequest(scope="survivorship", pit_keys=("chain:emission_share",)),
                             db_path=fixture_db["path"])


def test_depth_units_and_tmc_bars(surv):
    b = _bar(surv, 1, day(100))                                 # ledger_import: bar reserves (τ)
    assert b.depth == pytest.approx(_depth(1, 100 + 23 / 24))
    b = _bar(surv, 1, day(300))                                 # tmc_candle: chain_hourly fallback, rao → τ
    assert b.depth == pytest.approx(_depth(1, 300 + 23 / 24)) and b.depth < 1e6
    assert b.ts == day(300) + DAY                               # tmc_candle ts = bucket_start + 3600 of the last hour
    assert b.volume == 0.0 and b.moving_price == pytest.approx(0.02)


def test_daily_depth_fallback(qsettings, fixture_db, tmp_path):
    qsettings()
    p = tmp_path / "copy.db"
    shutil.copy(fixture_db["path"], p)
    con = sqlite3.connect(p)
    con.execute("DELETE FROM chain_hourly WHERE netuid = 2")
    con.commit()
    con.close()
    panel = load_factor_panel(PanelRequest(scope="survivorship"), db_path=str(p))
    assert _bar(panel, 2, day(300)).depth == pytest.approx(_depth(2, 300) + 1.0)   # subnet_tao/1e9


def test_volume_is_the_bucket_sum_of_delta_sources(surv, fixture_db):
    con = sqlite3.connect(fixture_db["path"])
    s = con.execute("SELECT SUM(volume) FROM price_bars WHERE netuid=3 AND period='1h' AND ts >= ? AND ts < ?",
                    (day(100), day(101))).fetchone()[0]
    assert _bar(surv, 3, day(100)).volume == pytest.approx(s)
    assert _bar(surv, 3, day(300)).volume == 0.0


def test_5m_rows_ignored(surv):
    assert all(b.price != 999.0 for b in surv.series[1])


def test_occupants(surv, fixture_db):
    b = _bar(surv, 9, day(320))
    assert b.occ == fixture_db["k9_new"]                         # unstamped bar → OccupantIndex (estimate only)
    assert b.reg_block == int(fixture_db["k9_new"].split(":")[1])
    assert _bar(surv, 9, day(250)).occ == fixture_db["k9_old"]
    con = sqlite3.connect(fixture_db["path"])
    idx = OccupantIndex.load(con)
    assert idx.occupant_at(9, day(300) + 3000)[0] == fixture_db["k9_new"]   # overlap → newer registration
    assert idx.occupant_at(9, day(299))[0] == fixture_db["k9_old"]
    assert idx.occupant_at(9, day(-5000)) is None
    assert surv.meta["occupant_coverage"] == 1.0


def test_reoccupied_boundary_without_collapse(surv):
    bars = surv.series[9]
    i = surv.index_of[9][day(299)]
    assert bars[i].seg != bars[i + 1].seg
    assert surv.seg_death[9][bars[i].seg] == "reoccupied"
    seven = surv.series[7]
    j = surv.index_of[7][day(199)]
    assert surv.seg_death[7][seven[j].seg] == "price_reset"


def test_chain_emission_share_uses_previous_day(surv):
    assert _bar(surv, 13, day(350)).pit["chain:emission_share"] > 0      # day 349's share
    assert _bar(surv, 13, day(351)).pit["chain:emission_share"] == 0.0   # day 350's share


def test_regime_tags_and_provenance(surv):
    assert surv.regime_of[day(100)]["market"] == "UNKNOWN"
    assert surv.regime_of[day(101)]["market"] == "DEAD"                    # labels[(101 // 30) % 4]
    assert surv.regime_of[day(400)]["market_source"] == "live"
    assert surv.regime_of[day(60)]["emission"] == "taoflow"
    assert surv.meta["price_sources"]["ledger_import"] > 0 and surv.meta["price_sources"]["tmc_candle"] > 0
    assert surv.dates[0] == day(52)                                          # QUANT_SURVIVORSHIP_START


def test_live_scope_grid(qsettings, fixture_db):
    qsettings()
    p = load_factor_panel(PanelRequest(scope="live"), db_path=fixture_db["path"])
    assert p.grid_seconds == 14400 and p.dates[0] == day(264)
