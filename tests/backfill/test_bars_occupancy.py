"""Spec 17 §9 tests 5–10: bars (close, moving price, volume, provenance), precedence, enrichment, occupancy."""
from __future__ import annotations

import asyncio
import itertools
import json

import pytest

from app.backfill import bars, common, occupancy
from app.backfill.occupancy import Obs

from .conftest import LIVE_FROM

H = 3600
T0 = (LIVE_FROM // H + 10) * H          # an hour well inside the live-capture era


def _ph(con, n, b, ts, price, *, mp=None, sv=None, source="ledger_pc", nra=None, occ=None, enriched=None,
        observed=None):
    con.execute("INSERT INTO pool_history (netuid, block_number, timestamp, price, tao_in_pool, alpha_in_pool, "
                "moving_price, network_registered_at, subnet_volume, occupant_key, source, observed_at, enriched_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (n, b, ts, price, 10.0 + price, 100.0, mp, nra, sv, occ, source, observed or ts, enriched))


def _rebuild(con, frm, to, now=1790000000):
    return bars.rebuild(con, periods=("1h",), from_ts=frm, to_ts=to, live_from_ts=LIVE_FROM, now=now, run_id="r")


def test_bars_ohlc_close_is_max_ts(bf):
    # hour 0: ledger rows (NULL mp) + an RPC sample with mp a few seconds before the ledger close row
    _ph(bf, 1, 100, T0 + 10, 1.0, sv=10.0, occ="1:5")
    _ph(bf, 1, 101, T0 + 1000, 3.0, sv=11.0, occ="1:5")
    _ph(bf, 1, 102, T0 + 3500, 0.5, mp=0.9, sv=12.0, source="blockmachine", occ="1:5", observed=1790000000)
    _ph(bf, 1, 103, T0 + 3590, 2.0, sv=13.0, occ="1:5")
    # hour 1: live ledger close with its own mp, no RPC
    _ph(bf, 1, 200, T0 + H + 100, 2.5, mp=2.4, sv=20.0, occ="1:5")
    # hour 4 (gap of 3 periods): volume must be NULL
    _ph(bf, 1, 500, T0 + 4 * H + 100, 2.6, mp=2.5, sv=25.0, occ="1:5")
    # hour 5: occupant change → volume NULL
    _ph(bf, 1, 600, T0 + 5 * H + 100, 2.7, mp=2.6, sv=26.0, occ="1:590")
    # hour 6: counter reset → volume NULL
    _ph(bf, 1, 700, T0 + 6 * H + 100, 2.8, mp=2.7, sv=1.0, occ="1:590")
    _rebuild(bf, T0, T0 + 7 * H)
    b = {r["ts"]: dict(r) for r in bf.execute("SELECT * FROM price_bars WHERE netuid = 1 AND period = '1h'")}
    h0 = b[T0]
    assert (h0["open"], h0["high"], h0["low"], h0["close"], h0["ts_last"]) == (1.0, 3.0, 0.5, 2.0, T0 + 3590)
    assert h0["moving_price"] == 0.9 and h0["source"] == "ledger_import"
    assert h0["backfilled"] == 1 and h0["observed_at"] == 1790000000       # mp came from an RPC sample
    h1 = b[T0 + H]
    assert h1["moving_price"] == 2.4 and h1["backfilled"] == 0 and h1["observed_at"] == T0 + H + 100
    assert h1["volume"] == pytest.approx(7.0) and h1["start_volume"] == 13.0
    assert b[T0 + 4 * H]["volume"] is None                                  # gap > 2 periods
    assert b[T0 + 5 * H]["volume"] is None                                  # occupant change
    assert b[T0 + 6 * H]["volume"] is None                                  # negative Δ


def test_bars_seed_era_backfilled(bf):
    t = (LIVE_FROM // H - 100) * H
    _ph(bf, 2, 10, t + 5, 1.0, occ="2:1")
    _rebuild(bf, t, t + H)
    r = bf.execute("SELECT backfilled, observed_at FROM price_bars WHERE netuid = 2").fetchone()
    assert tuple(r) == (1, 1790000000)


def _bar(source, close, ts_last):
    return {"netuid": 7, "ts": T0, "occupant_key": "7:1", "open": close, "high": close, "low": close, "close": close,
            "volume": None, "start_volume": None, "tao_price_usd": None, "tmc_block_number": None, "source": source,
            "event_at": T0, "observed_at": 1, "ts_last": ts_last, "tao_in_pool": None, "alpha_in_pool": None,
            "moving_price": None, "block_number": None, "backfilled": 1, "run_id": "r"}


def test_bars_precedence_order_independent(bf):
    rows = [_bar("blockmachine", 1.0, T0 + 3000), _bar("tmc_candle", 2.0, None), _bar("ledger_import", 3.0, T0 + 3500),
            _bar("sse_rollup", 4.0, T0 + 3599)]
    finals = set()
    for perm in itertools.permutations(rows):
        bf.execute("DELETE FROM price_bars")
        for r in perm:
            bars.upsert_bars(bf, [r], "1h")
        finals.add(tuple(bf.execute("SELECT source, close FROM price_bars WHERE netuid = 7").fetchone()))
    assert finals == {("sse_rollup", 4.0)}
    bf.execute("DELETE FROM price_bars")
    bars.upsert_bars(bf, [_bar("ledger_import", 3.0, T0 + 3500)], "1h")
    later = bars.upsert_bars(bf, [_bar("ledger_import", 3.3, T0 + 3590)], "1h")          # later close wins
    same = bars.upsert_bars(bf, [_bar("ledger_import", 9.9, T0 + 3590)], "1h")           # same ts_last skipped
    assert later["written"] == 1 and same["same_rank_skipped"] == 1
    bars.upsert_bars(bf, [_bar("tmc_candle", 2.0, None)], "1h")
    assert bf.execute("SELECT close FROM price_bars WHERE netuid = 7").fetchone()[0] == 3.3
    bf.execute("DELETE FROM price_bars")
    bars.upsert_bars(bf, [_bar("tmc_candle", 2.0, None)], "1h")
    rep = bars.upsert_bars(bf, [_bar("tmc_candle", 2.2, None)], "1h")                    # NULL ts_last never wins
    assert rep["same_rank_skipped"] == 1


def test_bars_rebuild_order_independent_when_rows_land_later(bf):
    """RPC era first (one sample → flat bar), ledger rows of the same hour imported later: the rebuilt bar keeps the
    RPC close sample but gains the ledger rows' open/high/low — the same bar as ledger-then-RPC. An identical
    recompute writes nothing."""
    _ph(bf, 3, 500, T0 + 3590, 2.0, source="blockmachine", occ="3:1", observed=1790000000)
    _rebuild(bf, T0, T0 + H)
    assert tuple(bf.execute("SELECT open, high, low, close FROM price_bars WHERE netuid = 3").fetchone()) == \
        (2.0, 2.0, 2.0, 2.0)
    _ph(bf, 3, 400, T0 + 60, 1.0, source="ledger_pi", occ="3:1")
    _ph(bf, 3, 450, T0 + 1800, 5.0, source="ledger_pi", occ="3:1")
    rep = _rebuild(bf, T0, T0 + H)
    assert rep["recomputed"] == 1
    r = bf.execute("SELECT source, open, high, low, close, ts_last FROM price_bars WHERE netuid = 3").fetchone()
    assert tuple(r) == ("blockmachine", 1.0, 5.0, 1.0, 2.0, T0 + 3590)
    again = _rebuild(bf, T0, T0 + H)
    assert again["written"] == 0 and again["same_rank_skipped"] == 1


def test_enrich_reserves_null_only(bf):
    tb = _bar("tmc_candle", 2.0, None)
    tb["tmc_block_number"] = 123
    bars.upsert_bars(bf, [tb], "1h")
    _ph(bf, 7, 5555, T0 + 3540, 2.01, mp=1.9, source="blockmachine", occ="7:1")
    n = bars.enrich_reserves(bf, 7, "1h", T0, T0 + H, now=1790000123)
    r = dict(bf.execute("SELECT * FROM price_bars WHERE netuid = 7").fetchone())
    assert n == 1 and r["tao_in_pool"] == pytest.approx(12.01) and r["block_number"] == 5555
    assert r["moving_price"] == 1.9 and r["tmc_block_number"] == 123 and r["backfilled"] == 1
    assert r["observed_at"] == 1790000123 and r["close"] == 2.0
    assert bars.enrich_reserves(bf, 7, "1h", T0, T0 + H, now=1790000999) == 0          # nothing NULL left


def test_build_intervals():
    obs = {40: [Obs(3372582, 6502594, 8409823, 1, 2, "ledger_pc"), Obs(8409860, 8409860, 8443757, 3, 4, "blockmachine"),
                Obs(8409860, 9000000, 9000000, 5, 5, "registry")]}
    iv, an = occupancy.build_intervals(obs)
    assert not an
    a, b = iv[40]
    assert (a.occupant_key, a.start_block, a.end_block) == ("40:3372582", 3372582, 8409859)
    assert (b.occupant_key, b.start_block, b.end_block, b.last_seen_block) == ("40:8409860", 8409860, None, 9000000)
    _, an = occupancy.build_intervals({5: [Obs(100, 200, 900, 1, 2, "x"), Obs(500, 600, 700, 1, 2, "x")]})
    assert an and an[0]["reason"] == "reg_reappears"
    _, an = occupancy.build_intervals({5: [Obs(300, 200, 250, 1, 2, "x")]})
    assert an and an[0]["reason"] == "reg_after_first_observation"


def _occupant(con, key, n, reg, status="active", first=9000000):
    con.execute("INSERT INTO occupants (occupant_key, netuid, network_registered_at, status, first_seen_block, "
                "first_seen_at, last_seen_block, last_seen_at, registered_at_est, source) VALUES "
                "(?,?,?,?,?,?,?,?,?, 'tmc')", (key, n, reg, status, first, 1790000000, first, 1790000000, 1700000000))


def test_write_occupants_historical(bf):
    _occupant(bf, "40:8409860", 40, 8409860)
    for b, reg in ((8400000, 3372582), (8409823, 3372582), (8409900, 8409860)):
        _ph(bf, 40, b, 1781000000 + (b - 8400000) * 12, 1.0, nra=reg, occ=f"40:{reg}", source="blockmachine")
    bf.execute("INSERT INTO rpc_owner_history (netuid, block_number, occupant_key, event_at, observed_at, source, "
               "owner_coldkey, subnet_name, change) VALUES (40, 8409823, '40:3372582', 1, 1, 'blockmachine', 'c', "
               "'OldName', 'first')")
    reg_ts = {3372582: 1715000000, 8409860: 1781500000}
    obs = occupancy.collect_observations(bf)
    iv, an = occupancy.build_intervals(obs)
    rep = occupancy.write_occupants(bf, iv, reg_ts=reg_ts, observed_at=1790000000, run_id="r")
    assert rep["inserted"] == 1 and rep["current_touched"] == 1
    old = dict(bf.execute("SELECT * FROM occupants WHERE occupant_key = '40:3372582'").fetchone())
    assert old["status"] == "deregistered" and old["dereg_reason"] == "reoccupied"
    assert old["successor_key"] == "40:8409860" and old["registered_ts"] == 1715000000
    assert old["deregistered_ts"] == 1781500000 and old["name"] == "OldName" and old["source"] == "rpc"
    cur = dict(bf.execute("SELECT * FROM occupants WHERE occupant_key = '40:8409860'").fetchone())
    assert cur["status"] == "active" and cur["predecessor_key"] == "40:3372582"
    assert cur["first_seen_block"] == 8409900 and cur["registered_ts"] == 1781500000

    async def _events():
        db = await common.prepare_db()
        try:
            n1 = await occupancy.emit_history_events(db, observed_at=1790000000, run_id="r")
            n2 = await occupancy.emit_history_events(db, observed_at=1790000100, run_id="r")
            rows = await db.fetchall("SELECT * FROM events WHERE event_type = 'netuid_reoccupied'")
            return n1, n2, rows
        finally:
            await db.close()
    n1, n2, rows = asyncio.run(_events())
    assert (n1, n2, len(rows)) == (1, 0, 1)
    e = rows[0]
    p = json.loads(e["payload"])
    assert e["severity"] == "warning" and e["source"] == "registry" and e["alerted_at"] == 1790000000
    assert e["dedupe_key"] == "reoccupied:40:3372582->40:8409860" and p["backfilled"] is True
    assert e["occupant_key"] == "40:8409860" and e["event_at"] == 1781500000


def test_stamp_all(bf):
    _occupant(bf, "40:8409860", 40, 8409860)
    bf.execute("INSERT INTO occupants (occupant_key, netuid, network_registered_at, status, first_seen_block, "
               "first_seen_at, last_seen_block, last_seen_at, registered_ts, deregistered_ts, source) VALUES "
               "('40:3372582', 40, 3372582, 'deregistered', 6502594, 1, 8409823, 2, 1715000000, 1781500000, 'rpc')")
    bf.execute("UPDATE occupants SET registered_ts = 1781500000 WHERE occupant_key = '40:8409860'")
    _ph(bf, 40, 3000000, 1700000000, 1.0)                      # below the earliest reg → stays NULL
    _ph(bf, 40, 6502594, 1758585588, 1.0)                      # seed era → old occupant
    _ph(bf, 40, 8409870, 1781500120, 1.0)                      # new occupant
    _ph(bf, 40, 8409800, 1781499000, 1.0, occ="40:8409860")   # trusted but mismatching → counted, kept
    tb = _bar("tmc_candle", 2.0, None)
    tb.update({"netuid": 40, "occupant_key": None, "ts": 1781503200})
    bars.upsert_bars(bf, [tb], "1h")
    idx = occupancy.OccupancyIndex.load(bf)
    assert idx.occupant_at_block(40, 8409859) == "40:3372582" and idx.occupant_at_block(40, 8409860) == "40:8409860"
    assert idx.occupant_at_ts(40, 1781499999) == "40:3372582" and idx.occupant_at_ts(40, 1700000000) is None
    rep = occupancy.stamp_all(bf, idx)
    assert rep["pool_history_stamped"] == 2 and rep["mismatches"] == 1 and rep["price_bars_stamped"] == 1
    assert rep["unstamped"]["pool_history"] == 1
    got = dict(bf.execute("SELECT block_number, occupant_key FROM pool_history WHERE netuid = 40").fetchall())
    assert got[6502594] == "40:3372582" and got[8409870] == "40:8409860" and got[3000000] is None
    assert got[8409800] == "40:8409860"
    assert bf.execute("SELECT COUNT(*) FROM pool_history WHERE enriched_at IS NOT NULL").fetchone()[0] == 0
