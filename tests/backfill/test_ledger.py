"""Spec 17 §9 tests 1–4, 29, 32: old-ledger import, NULL-merge, WAL copies, thinning, the pool_history trigger."""
from __future__ import annotations

import sqlite3

import pytest

from app.backfill import common, ledger_import
from app.backfill.ledger_import import SourceSpec

from .conftest import LIVE_FROM, TRUST, make_old_ledger


def _import(con, path, source="ledger_pc", thin=0, run_started=1790000000, batch=None, tmp=None):
    spec = SourceSpec(path, source)
    p, imm = ledger_import.prepare_source(spec, tmp or path.parent / "tmp")
    side = common.Sidecar(path.parent / f"side_{source}.json", {})
    return ledger_import.import_source(con, spec, p, imm, trust_ts=TRUST, live_from_ts=LIVE_FROM,
                                       run_started=run_started, thin_sec=thin, sidecar=side, pause_s=0,
                                       run_id="t", batch_rows=batch)


def test_import_counts_and_trust(bf, tmp_path):
    src = tmp_path / "seed" / "ledger.db"
    rows = make_old_ledger(src)
    rep = _import(bf, src)
    positive = [r for r in rows if r[3] > 0 and r[4] > 0]
    assert rep.rows_new == len(positive) == bf.execute("SELECT COUNT(*) FROM pool_history").fetchone()[0]
    assert rep.rows_skipped_nonpositive == 1
    # pre-trust reg values are dropped (back-stamped), trusted ones kept with an occupant_key
    pre = bf.execute("SELECT COUNT(*) FROM pool_history WHERE timestamp < ? AND network_registered_at IS NOT NULL",
                     (TRUST,)).fetchone()[0]
    assert pre == 0 and rep.nra_dropped_untrusted == sum(1 for r in rows if r[2] < TRUST and r[7] is not None)
    keyed = bf.execute("SELECT COUNT(*) FROM pool_history WHERE occupant_key IS NOT NULL").fetchone()[0]
    assert keyed == sum(1 for r in positive if r[2] >= TRUST and r[7] is not None)
    assert bf.execute("SELECT occupant_key FROM pool_history WHERE netuid = 40 AND block_number = 8409860"
                      ).fetchone()[0] == "40:8409860"
    # honest observed_at: live capture keeps its timestamp; earlier rows get the run start
    assert bf.execute("SELECT COUNT(*) FROM pool_history WHERE timestamp >= ? AND observed_at <> timestamp",
                      (LIVE_FROM,)).fetchone()[0] == 0
    assert [tuple(r) for r in bf.execute("SELECT DISTINCT observed_at FROM pool_history WHERE timestamp < ?",
                                         (LIVE_FROM,))] == [(1790000000,)]
    assert bf.execute("SELECT COUNT(*) FROM pool_history WHERE event_at IS NOT NULL").fetchone()[0] == 0
    # re-run adds nothing (sidecar + INSERT OR IGNORE)
    rep2 = _import(bf, src)
    assert rep2.rows_new == 0


def test_null_merge_never_overwrites(bf, tmp_path):
    src = tmp_path / "seed" / "ledger.db"
    make_old_ledger(src)
    _import(bf, src)
    bk = tmp_path / "seed" / "ledger_backfill.db"
    c = sqlite3.connect(bk)
    c.execute("CREATE TABLE pool_history (netuid INTEGER, block_number INTEGER, timestamp INTEGER, price REAL, "
              "tao_in_pool REAL, alpha_in_pool REAL, moving_price REAL, network_registered_at INTEGER, "
              "subnet_volume REAL)")
    c.executemany("INSERT INTO pool_history VALUES (?,?,?,?,?,?,?,?,?)", [
        (1, 6502594, 1758585588, 0.01, 100.0, 1000.0, 0.0111, 999, 5.0),        # fills mp+sv; reg pre-trust ignored
        (40, 8409800, 1781497500 - 720, 0.02, 50.0, 2500.0, 0.5, 3372582, 99.0),  # target already non-NULL
        (2, 6502594, 1758585588, 0.02, 100.0, 1000.0, None, 777, None),          # can fill nothing
    ])
    c.commit()
    c.close()
    spec = SourceSpec(bk, "ledger_backfill", merge_only=True)
    changed, mp, sv, nra = ledger_import.merge_nulls(bf, spec, bk, True, trust_ts=TRUST, now=1790000001)
    assert (changed, mp, sv, nra) == (1, 1, 1, 0)
    r = bf.execute("SELECT moving_price, subnet_volume, network_registered_at, enriched_at FROM pool_history "
                   "WHERE netuid = 1 AND block_number = 6502594").fetchone()
    assert tuple(r) == (0.0111, 5.0, None, 1790000001)
    assert bf.execute("SELECT moving_price, subnet_volume FROM pool_history WHERE netuid = 40 AND block_number = "
                      "8409800").fetchone()[0] == 0.021
    # second merge: nothing left to fill, enriched_at unchanged
    assert ledger_import.merge_nulls(bf, spec, bk, True, trust_ts=TRUST, now=1790000999)[0] == 0
    assert bf.execute("SELECT enriched_at FROM pool_history WHERE netuid = 1 AND block_number = 6502594"
                      ).fetchone()[0] == 1790000001


def test_wal_source_checkpointed(bf, tmp_path):
    src = tmp_path / "seed" / "pi.db"
    make_old_ledger(src)
    w = sqlite3.connect(src)
    w.execute("PRAGMA journal_mode = WAL")
    w.execute("PRAGMA wal_autocheckpoint = 0")
    w.execute("INSERT INTO pool_history VALUES (64, 9000000, 1788000000, 0.07, 190000.0, 2700000.0, 0.07, 4531295, 1.0)")
    w.commit()                                               # stays in the -wal (no checkpoint, connection open)
    assert (tmp_path / "seed" / "pi.db-wal").stat().st_size > 0
    path, imm = ledger_import.prepare_source(SourceSpec(src, "ledger_pi"), tmp_path / "tmp")
    assert not imm and path.name.endswith(".checkpointed.db")
    _import(bf, src, source="ledger_pi", tmp=tmp_path / "tmp")
    w.close()
    assert bf.execute("SELECT source FROM pool_history WHERE block_number = 9000000").fetchone()[0] == "ledger_pi"


def test_thin_sec(bf, tmp_path):
    src = tmp_path / "seed" / "ledger.db"
    make_old_ledger(src)
    _import(bf, src, thin=300)
    # dense era: 120 one-minute rows per netuid → 24 five-minute windows, the last row of each kept
    n = bf.execute("SELECT COUNT(*) FROM pool_history WHERE netuid = 1 AND timestamp >= ? AND timestamp < ?",
                   (LIVE_FROM, LIVE_FROM + 7200)).fetchone()[0]
    assert n == len({(LIVE_FROM + i * 60) // 300 for i in range(120)})
    kept = {r[0] for r in bf.execute("SELECT timestamp FROM pool_history WHERE netuid = 1 AND timestamp >= ?",
                                     (LIVE_FROM,))}
    for w in {(LIVE_FROM + i * 60) // 300 for i in range(120)}:
        assert max(t for t in (LIVE_FROM + i * 60 for i in range(120)) if t // 300 == w) in kept


def test_online_batches_import_everything(bf, tmp_path):
    src = tmp_path / "seed" / "ledger.db"
    rows = make_old_ledger(src)
    rep = _import(bf, src, batch=7)
    assert rep.rows_new == sum(1 for r in rows if r[3] > 0 and r[4] > 0)


def test_missing_optional_columns(bf, tmp_path):
    src = tmp_path / "seed" / "old.db"
    make_old_ledger(src, optional_cols=False)
    rep = _import(bf, src, source="ledger_extra")
    assert rep.status == "ok" and rep.rows_new > 0
    assert bf.execute("SELECT COUNT(*) FROM pool_history WHERE moving_price IS NOT NULL").fetchone()[0] == 0


def test_pool_history_trigger(bf):
    bf.execute("INSERT INTO pool_history (netuid, block_number, timestamp, price, tao_in_pool, alpha_in_pool) "
               "VALUES (1, 1, 1, 1.0, 1.0, 1.0)")
    with pytest.raises(sqlite3.DatabaseError, match="NULL-only"):
        bf.execute("UPDATE pool_history SET price = 2.0 WHERE netuid = 1")
    bf.execute("UPDATE pool_history SET moving_price = 0.5, occupant_key = '1:1', enriched_at = 5 WHERE netuid = 1")
    with pytest.raises(sqlite3.DatabaseError, match="NULL-only"):
        bf.execute("UPDATE pool_history SET moving_price = 0.6 WHERE netuid = 1")
    with pytest.raises(sqlite3.DatabaseError, match="append-only"):
        bf.execute("DELETE FROM pool_history")


def test_verify_reports_counts(bf, tmp_path, monkeypatch):
    s = common.settings()
    monkeypatch.setattr(s, "LEDGER_EXPECTED_ROWS", 3)
    src = tmp_path / "seed" / "ledger.db"
    make_old_ledger(src)
    _import(bf, src)
    v = ledger_import.verify(bf, s)
    assert v["ledger_rows"]["pass"] is False and v["golden_row"]["pass"] is True


def test_repair_rpc_prices(bf, tmp_path, monkeypatch):
    import sqlite3 as _sq
    from app.backfill import bars
    from scripts import repair_rpc_prices as rp
    t = 1785000000
    for n, price, tao, alpha in ((1, 0.5, 10.0, 40.0), (2, 0.1, 10.0, 100.0)):   # n=1 stale sqrt, n=2 correct
        bf.execute("INSERT INTO pool_history (netuid, block_number, timestamp, price, tao_in_pool, alpha_in_pool, "
                   "occupant_key, source, observed_at, price_method, run_id) VALUES (?, 8700000, ?, ?, ?, ?, ?, "
                   "'blockmachine', 1, 'swap_sqrt', 'r0')", (n, t, price, tao, alpha, f"{n}:1"))
    bars.rebuild(bf, periods=("1h",), from_ts=t - 3600, to_ts=t + 3600, live_from_ts=LIVE_FROM, now=2, run_id="r0")

    async def fake_fetch(blocks, concurrency):
        return {8700000: {1: 0.25, 2: 0.1}}
    monkeypatch.setattr(rp, "_fetch", fake_fetch)
    assert rp.main(["--db", str(common.settings().path(common.settings().DB_PATH))]) == 0
    rows = {r[0]: tuple(r[1:]) for r in bf.execute(
        "SELECT netuid, price, price_method, run_id FROM pool_history WHERE block_number = 8700000")}
    assert rows[1][:2] == (0.25, "reserve_ratio") and rows[1][2].startswith("script:repair_rpc_prices")
    assert rows[2] == (0.1, "swap_sqrt", "r0")                                   # correct row untouched
    assert bf.execute("SELECT close FROM price_bars WHERE netuid = 1 AND period = '1h'").fetchone()[0] == 0.25
    with pytest.raises(_sq.DatabaseError, match="NULL-only"):                    # trigger is back
        bf.execute("UPDATE pool_history SET price = 9 WHERE netuid = 2")


def test_pi_only_import_verify_counts_rows_held_by_rpc(bf, tmp_path, monkeypatch):
    """No PC ledger: the Pi copy is the first writer (never merged into itself); §10 counts run over the PC span
    and rows whose block an earlier RPC era already wrote count as held, not missing (BUILD-LOG Phase 3 seed run)."""
    s = common.settings()
    pi = tmp_path / "seed" / "pi" / "ledger.db"
    pi.parent.mkdir(parents=True)
    rows = make_old_ledger(pi, extra_rows=[(64, 8600000, ledger_import.PC_MAX_TS + 5000, 0.07, 1.0, 1.0, 0.07,
                                            4531295, 1.0)])
    bf.execute("INSERT INTO pool_history (netuid, block_number, timestamp, price, tao_in_pool, alpha_in_pool, "
               "moving_price, source, observed_at) VALUES (1, 8000000, ?, 0.05, 200.0, 3000.0, 0.049, "
               "'blockmachine', 1)", (LIVE_FROM - 10,))
    bk = tmp_path / "seed" / "ledger_backfill.db"
    c = sqlite3.connect(bk)
    c.execute("CREATE TABLE pool_history (netuid INTEGER, block_number INTEGER, timestamp INTEGER, price REAL, "
              "tao_in_pool REAL, alpha_in_pool REAL, moving_price REAL, network_registered_at INTEGER, "
              "subnet_volume REAL)")
    c.executemany("INSERT INTO pool_history VALUES (?,?,?,?,?,?,?,?,?)", [
        (1, 6502594, 1758585588, 0.01, 100.0, 1000.0, 0.0111, None, 5.0),        # fills a Pi row
        (1, 8000000, LIVE_FROM, 0.05, 200.0, 3000.0, 0.049, None, 7.0)])         # block held by the RPC row
    c.commit()
    c.close()
    in_span = [r for r in rows if r[3] > 0 and r[4] > 0 and r[2] <= ledger_import.PC_MAX_TS]
    monkeypatch.setattr(s, "LEDGER_EXPECTED_ROWS", len(in_span))
    monkeypatch.setattr(s, "LEDGER_EXPECTED_NETUIDS", 4)
    monkeypatch.setattr(s, "LEDGER_BACKFILL_EXPECTED_MERGE", 2)
    out = ledger_import.run_import(bf, online=False, extra=[], thin_sec=0, run_id="t")
    assert set(k for k in out if k.startswith("ledger_")) == {"ledger_pi", "ledger_backfill"}   # no ledger_pi_merge
    assert out["ledger_pi"]["rows_preexisting"] == 1
    v = out["verify"]
    lr = v["ledger_rows"]
    assert (lr["source"], lr["held_by_other_source"], lr["missing"], lr["imported"], lr["pass"]) == \
        ("ledger_pi", 1, 0, len(in_span) - 1, True)
    assert v["ledger_netuids"]["pass"] and v["golden_row"]["pass"]
    m = v["ledger_backfill_merge"]
    assert (m["targets"], m["covered_by_other_source"], m["actual"], m["pass"]) == (2, 1, 2, True)
