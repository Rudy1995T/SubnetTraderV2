"""Spec 17 §9 tests 18–28, 30, 33, 34: TMC tasks, pacing, regime, fundamentals/whales, coverage, Pi, events."""
from __future__ import annotations

import asyncio
import importlib.util
import json
from types import SimpleNamespace

import pytest

import app.config as app_config
from app.backfill import common, coverage, occupancy, pi_recovery, regime_backfill
from app.backfill import tmc_backfill as tb


def run(coro):
    return asyncio.run(coro)


# ── 18/19 candles ────────────────────────────────────────────────────────────────────────────────────────────
def test_candles_parse_aggregate():
    base = 1790000000 // 86400 * 86400
    rows = [[base + i * 300, 900 + i, 1.0, 1.2, 0.9, 1.0 + i / 1000, 2.0, 100.0 + i, 400.0] for i in range(288)]
    rows[5] = ["2026-09-23T00:25:00Z", *rows[5][1:], 370.0]                 # ISO ts + 10 elements
    rows[5][0] = "2026-09-23T00:25:00Z"
    c = tb.parse_compact_candles(rows)
    assert len(c) == 288 and c[0].ts == base
    for bad in ([[1, 2, 3, 4, 5, 6, 7, 8]], [[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]):
        with pytest.raises(tb.ParseError):
            tb.parse_compact_candles(bad)
    h = tb.aggregate(c[:7] + c[12:14], 3600, 6)
    assert [x["ts"] for x in h] == [base] and h[0]["n"] == 7 and h[0]["close"] == c[6].close
    assert h[0]["volume"] == pytest.approx(14.0) and h[0]["start_volume"] == 100.0
    assert len(tb.aggregate(c, 86400, 144)) == 1 and tb.aggregate(c[:100], 86400, 144) == []


def test_detect_label():
    cs = [tb.Candle(1000 * 300 + i * 300, None, None, None, None, 1.0 + i, None, None, None) for i in range(20)]
    start_samples = [(1000 * 300 + i * 300 + 290, 1.0 + i) for i in range(20)]         # sample inside [ts, ts+300)
    assert tb.detect_label(cs, start_samples)[0] == "start"
    end_samples = [(1000 * 300 + i * 300 - 10, 1.0 + i) for i in range(1, 20)]         # labelled by bucket end
    assert tb.detect_label(cs, end_samples)[0] == "end"
    lab, err = tb.detect_label(cs, [(1000 * 300 + 5, 99.0)])
    assert lab == "start" and err is not None and err >= 0.005                           # inconclusive → start


# ── 21 pacing ────────────────────────────────────────────────────────────────────────────────────────────────
def test_hour_ceiling_and_quiet_windows():
    clock = {"t": 1790121600 + 3600}          # 01:00Z
    slept: list[float] = []

    async def sleep(x):
        slept.append(x)
        clock["t"] += x
    hc = tb.HourCeiling(3, "02:05-02:45,20:25-23:35", clock=lambda: clock["t"], sleep=sleep)
    for _ in range(4):
        run(hc.acquire())
    assert slept and slept[0] == pytest.approx(3600)                                    # 4th waits for the hour
    clock["t"] = 1790121600 + 2 * 3600 + 10 * 60                                        # 02:10Z → quiet
    slept.clear()
    run(hc.acquire())
    assert clock["t"] >= 1790121600 + 2 * 3600 + 45 * 60


# ── 20 analytics segments + restamp ──────────────────────────────────────────────────────────────────────────
class _Resp(SimpleNamespace):
    pass


def test_analytics_occupant_segments_and_restamp(bf):
    con = bf
    con.execute("INSERT INTO occupants (occupant_key, netuid, network_registered_at, status, first_seen_block, "
                "first_seen_at, last_seen_block, last_seen_at, registered_ts, source) VALUES "
                "('40:8409860', 40, 8409860, 'active', 8409860, 1781500000, 9000000, 1790000000, 1781500000, 'tmc')")
    rows = [{"ts": 1781400000 + i * 3600, "block_number": 8409860 - 300 + i * 300, "emission_percent": 0.5,
             "subnet_tao": 1.0} for i in range(4)]
    payload = rows

    async def go():
        db = await common.prepare_db()
        try:
            rt = SimpleNamespace(db=db)
            r = SimpleNamespace(rt=rt, run_id="r1", settings=app_config.settings)

            class C:
                async def fetch_analytics(self, n, span):
                    return _Resp(ok=True, status=200, json=payload, error=None, headers={})
            occ = occupancy.OccupancyIndex.load(con)
            a = await tb.task_analytics(r, C(), 40, "analytics1m", occ)
            # the old occupant becomes known → restamp from the stored payload with 0 HTTP calls
            con.execute("INSERT INTO occupants (occupant_key, netuid, network_registered_at, status, "
                        "first_seen_block, first_seen_at, last_seen_block, last_seen_at, registered_ts, source) VALUES "
                        "('40:3372582', 40, 3372582, 'deregistered', 6502594, 1, 8409859, 2, 1715000000, 'rpc')")
            occ2 = occupancy.OccupancyIndex.load(con)
            b = await tb.task_restamp(r, 40, occ2)
            got = await db.fetchall("SELECT occupant_key, COUNT(*) n FROM chain_hourly WHERE netuid = 40 "
                                    "GROUP BY 1 ORDER BY 1")
            return a, b, got
        finally:
            await db.close()
    a, b, got = run(go())
    assert a["status"] == "partial" and a["unattributed"] == 1 and a["rows"] == 3
    assert b["status"] == "done" and b["unattributed"] == 0
    assert [(g["occupant_key"], g["n"]) for g in got] == [("40:3372582", 1), ("40:8409860", 3)]
    assert con.execute("SELECT COUNT(*) FROM chain_hourly WHERE backfilled = 1 AND source = 'analytics_1m'"
                       ).fetchone()[0] == 4


# ── 22 regime ────────────────────────────────────────────────────────────────────────────────────────────────
def test_regime_unknown_not_dead(bf):
    day = 1790121600
    for n in range(1, 12):
        for h in range(24):
            bf.execute("INSERT INTO price_bars (netuid, period, ts, close, source, observed_at, backfilled) VALUES "
                       "(?, '1h', ?, 1.0, 'blockmachine', 1, 1)", (n, day - 86400 + 3600 * (h + 1)))
    bf.execute("INSERT INTO regime_daily (day_ts, spec_version, label, v_w, source, computed_at) VALUES "
               "(?, 'ts-1.0', 'CHOPPY', 0.15, 'live', 1)", (day + 86400,))
    seen = []

    def compute(snaps, params):
        seen.append(snaps)
        if len(snaps) < 10:
            return {"thin_universe": True, "raw_regime": "DEAD", "n_subnets": len(snaps)}
        return {"thin_universe": False, "raw_regime": "TRENDING", "vol_24h": 0.4, "directional_strength": 0.03,
                "dispersion": 0.01, "n_subnets": len(snaps)}
    rep = regime_backfill.backfill_regime(bf, start_day=day - 86400, end_day=day + 86400, params=None,
                                          spec_version="ts-1.0", run_id="r", compute=compute)
    labels = dict(bf.execute("SELECT day_ts, label FROM regime_daily").fetchall())
    assert labels[day - 86400] == "UNKNOWN" and labels[day] == "TRENDING" and labels[day + 86400] == "CHOPPY"
    assert rep["written"] == 2
    assert all(isinstance(p["ts"], int) for s in seen for pts in s.values() for p in pts)
    assert max(p["ts"] for pts in seen[1].values() for p in pts) == day                    # window (D-86400, D]
    r = dict(bf.execute("SELECT * FROM regime_daily WHERE day_ts = ?", (day - 86400,)).fetchone())
    assert r["v_w"] == 0.15 and r["thin_universe"] == 1 and r["source"] == "backfill" and r["backfilled"] == 1
    compute, params, v_w = regime_backfill.load_regime()
    assert params.REGIME_MIN_SUBNETS == 10 and v_w["UNKNOWN"] == 0.15 and v_w["DEAD"] == 0.10


# ── 24 whales ────────────────────────────────────────────────────────────────────────────────────────────────
def test_whale_cohort_import(bf, tmp_path):
    from app.backfill.fundamentals_import import import_whale_cohort
    ck = [{"ss58": f"5{'A' * 46}{i}", "rank": i, "total_stake_tao": 10.0, "balance_total_tao": 11.0,
           "free_fraction": 0.1, "balance_rank": i, "coldkey_swap": None, "label": None, "label_source": None,
           "attribution": None} for i in range(3)]
    (tmp_path / "w.json").write_text(json.dumps({"as_of_block": 8590789, "as_of_ts": "2026-07-10T12:13:43Z",
                                                 "coldkeys": ck, "excluded": [{"rank": 7}]}))
    (tmp_path / "l.json").write_text(json.dumps({"_README": [], "labels": {ck[0]["ss58"]: {"label": "exchange"}}}))
    r = import_whale_cohort(bf, tmp_path / "w.json", tmp_path / "l.json", run_id="r")
    assert (r["whale_rows"], r["whale_excluded"], r["labels_merged"]) == (3, 1, 1)
    assert bf.execute("SELECT label FROM whale_cohort WHERE coldkey = ?", (ck[0]["ss58"],)).fetchone()[0] == "exchange"
    assert import_whale_cohort(bf, tmp_path / "w.json", tmp_path / "l.json", run_id="r")["whale_rows"] == 0


# ── 26 coverage ──────────────────────────────────────────────────────────────────────────────────────────────
COVERAGE_KEYS = {
    "": ["schema", "generated_at", "spec_version", "db_schema_version", "eras", "series", "occupancy", "holes",
         "reconstructible_from", "forward_only", "caveats", "checks", "provenance"],
    "eras": ["gap", "ledger", "pre_ledger", "live"],
    "series": ["pool_history", "price_bars_1h", "price_bars_1d", "price_bars_5m", "chain_daily", "chain_hourly",
               "block_times", "regime_daily", "github_commits"],
    "reconstructible_from": ["price_depth_moving_price", "emission_share", "net_flow_30d", "owner_commitment",
                             "control_stability_90d", "tenure", "github_p2", "regime"],
}


def test_coverage_schema(bf):
    bf.execute("INSERT INTO occupants (occupant_key, netuid, network_registered_at, status, first_seen_block, "
               "first_seen_at, last_seen_block, last_seen_at, registered_ts, source) VALUES "
               "('5:1', 5, 1, 'active', 1, 1, 1, 1, 1789000000, 'tmc')")
    t0 = 1789200000
    for h in list(range(0, 10)) + list(range(22, 30)):          # a 12-hour hole in the middle
        bf.execute("INSERT INTO price_bars (netuid, period, ts, close, source, observed_at, backfilled, tao_in_pool) "
                   "VALUES (5, '1h', ?, 1.0, 'blockmachine', 1, 1, 2.0)", (t0 + h * 3600,))
    doc = coverage.build()
    for path, keys in COVERAGE_KEYS.items():
        node = doc if not path else doc[path]
        assert all(k in node for k in keys), (path, [k for k in keys if k not in node])
    assert doc["schema"] == "coverage-1" and isinstance(doc["generated_at"], int)
    assert doc["eras"]["gap"]["status"] == "not_run" and doc["reconstructible_from"]["control_stability_90d"] is None
    assert "site_age" in doc["forward_only"] and "control_stability_90d" not in doc["forward_only"]   # 11 §11 item 12
    assert not set(doc["forward_only"]) & {k for k, v in doc["reconstructible_from"].items() if v is not None}
    holes = [h for h in doc["holes"] if h["from_ts"] == t0 + 10 * 3600]
    assert holes and holes[0]["to_ts"] == t0 + 22 * 3600
    assert doc["caveats"] == sorted(set(doc["caveats"]))
    p = coverage.write(doc)
    assert json.loads(p.read_text())["schema"] == "coverage-1"


# ── 27/28 Pi ─────────────────────────────────────────────────────────────────────────────────────────────────
def test_pi_guard_and_parse(tmp_path):
    for bad in (tmp_path / "wallets" / "w" / "coldkey", tmp_path / "wallets" / "w" / "hotkeys" / "x"):
        with pytest.raises(pi_recovery.WalletGuardError):
            pi_recovery.safe_open(bad)
    ss = "5FRYKhbmfXPDoHdUUDMx27E3HuMvAzwjzFMMq3rNurUhAyS9"
    (tmp_path / "a.txt").write_text(json.dumps({"ss58Address": ss, "accountId": "0x00"}))
    (tmp_path / "b.txt").write_text(ss + "\n")
    assert pi_recovery.read_coldkey_ss58(tmp_path / "a.txt") == ss == pi_recovery.read_coldkey_ss58(tmp_path / "b.txt")
    (tmp_path / ".env").write_text("TAOSTATS_API_KEY=secret\n# c\nGITHUB_PAT=x\n")
    assert pi_recovery.env_key_names(tmp_path / ".env") == ["GITHUB_PAT", "TAOSTATS_API_KEY"]


def test_pi_tracked_positions_add_open_and_stuck():
    pi = [{"id": 416, "netuid": 99, "status": "CLOSED"}, {"id": 457, "netuid": 40, "status": "OPEN"},
          {"id": 195, "netuid": 30, "status": "STUCK"}, {"id": 500, "netuid": 7, "status": "CLOSED"}]
    t = pi_recovery.tracked_positions(pi)
    assert t == {**pi_recovery.PC_OPEN_POSITIONS, 457: 40, 195: 30}


def test_parse_coldkey_stakes_tolerant():
    a = [{"netuid": 99, "hotkey": "h", "balance_alpha": 5.0, "balance_tao": 0.5}]
    b = {"results": [{"subnet_id": "99", "hotkey_ss58": "h", "alpha": 5, "tao_value": 0.5}]}
    c = {"data": [{"subnet": 99, "hotkey": "h", "stake": "5", "value_tao": "0.5"}]}
    assert tb.parse_coldkey_stakes(a) == tb.parse_coldkey_stakes(b) == tb.parse_coldkey_stakes(c) == \
        [{"netuid": 99, "hotkey": "h", "alpha": 5.0, "tao_value": 0.5}]


def test_parse_coldkey_stakes_tmc_shape_rao():
    """Verified 2026-10-13 shape: ``subnets[]`` with integer rao amounts and ``hotkeys[].address``."""
    p = {"nominator_coldkey": "5C…", "subnets": [
        {"subnet": 40, "balance_alpha": 1, "balance_tao": 0, "realized": -101478784, "hotkeys": [{"address": "5Ef"}]},
        {"subnet": 94, "balance_alpha": 2_500_000_000, "balance_tao": 15_000_000, "hotkeys": []}]}
    assert tb.parse_coldkey_stakes(p) == [{"netuid": 40, "hotkey": "5Ef", "alpha": 1e-9, "tao_value": 0.0},
                                          {"netuid": 94, "hotkey": None, "alpha": 2.5, "tao_value": 0.015}]


# ── 30 preconditions / 34 events / 33 deps ───────────────────────────────────────────────────────────────────
def test_script_preconditions(bf, monkeypatch, tmp_path):
    from scripts import backfill_rpc, import_old_ledger
    s = common.settings()
    monkeypatch.setattr(s, "BACKFILL_ENABLED", False)
    assert backfill_rpc.main(["--probe"]) == 3
    monkeypatch.setattr(s, "BACKFILL_ENABLED", True)
    monkeypatch.setattr(common, "service_is_up", lambda timeout=2.0: True)
    assert import_old_ledger.main(["--phase", "import"]) == 2                    # service up, no --online
    monkeypatch.setattr(common, "service_is_up", lambda timeout=2.0: False)
    assert import_old_ledger.main(["--phase", "import"]) == 2                    # seed missing → exit 2
    (tmp_path / "PAUSE_ALL").write_text("")
    assert common.stop_requested() is True
    (tmp_path / "PAUSE_ALL").unlink()
    with common.BackfillLock():
        with pytest.raises(common.BackfillBusy):
            common.BackfillLock().__enter__()
    bf.execute("CREATE TABLE IF NOT EXISTS x (a)")
    assert not common.missing_objects(bf)


def test_backfill_events(bf, monkeypatch):
    async def go():
        db = await common.prepare_db()
        try:
            a = await common.emit_step_event(db, "backfill_done", job="backfill_rpc:gap", era="gap", run_id="r1",
                                             rows=5, seconds=1.0)
            b = await common.emit_step_event(db, "backfill_done", job="backfill_rpc:gap", era="gap", run_id="r1",
                                             rows=5, seconds=1.0)
            c = await common.emit_step_event(db, "backfill_failed", job="x", era=None, run_id="r2", error="boom",
                                             resume_hint="python -m scripts.x")
            monkeypatch.setattr(common.settings(), "BACKFILL_ALERTS_ENABLED", False)
            d = await common.emit_step_event(db, "backfill_done", job="y", era=None, run_id="r3")
            rows = await db.fetchall("SELECT * FROM events ORDER BY id")
            return a, b, c, d, rows
        finally:
            await db.close()
    a, b, c, d, rows = run(go())
    assert a and b is None and c and d
    assert [(r["event_type"], r["source"], r["severity"], r["dedupe_key"]) for r in rows] == [
        ("backfill_done", "runtime", "info", "backfill_rpc:gap|r1"), ("backfill_failed", "runtime", "warning", "x|r2"),
        ("backfill_done", "runtime", "info", "y|r3")]
    assert rows[0]["alerted_at"] is None and rows[2]["alerted_at"] is not None and rows[0]["occupant_key"] is None
    import app.backfill as pkg
    from pathlib import Path
    src = "".join(p.read_text() for p in Path(pkg.__file__).parent.glob("*.py"))
    assert "send_alert" not in src


def test_deps_backfill_extra():
    assert importlib.util.find_spec("async_substrate_interface") is not None
    assert importlib.util.find_spec("bittensor") is None and importlib.util.find_spec("bittensor_wallet") is None
    from scalecodec.utils.ss58 import ss58_encode  # noqa: F401  (provided by cyscale)
