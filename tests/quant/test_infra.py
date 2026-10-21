"""Trial log, snapshot/resolution and result-file shape (spec 13 §3.4–§3.6, §7, §8.1)."""
from __future__ import annotations

import gzip
import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from app.quant.factor_backtest import run_factor_study
from app.quant.panel_builder import PanelRequest, load_factor_panel
from app.quant.report import factor_payload, save_factor_json
from app.quant.snapshot import (
    RefusedError,
    db_fingerprint,
    make_snapshot,
    newest_backup,
    prune,
    resolve_db,
)
from app.quant.trials import TrialLog, trial_key

BASE = dict(factor="quality_cg", label="ret_log_7", horizon=7, scope="survivorship", controls=[], lookback=6,
            min_depth=3000.0, max_depth=None, step=7, pit_mode="strict", start=None, end=None, spec_version="ts-1.0")


# ── trials ────────────────────────────────────────────────────────
def test_trial_keys():
    k = trial_key(BASE)
    assert trial_key({**BASE}) == k
    for change in ({"pit_mode": "reconstructed"}, {"start": "2025-10-01"}, {"end": "2026-01-01"},
                   {"spec_version": "ts-1.1"}):
        assert trial_key({**BASE, **change}) != k


def test_trial_log_counts_and_variance(tmp_path):
    log = TrialLog(tmp_path / "trials.json")
    (k1,) = log.start([BASE], command="c", git_sha="g", db_sha256="a")
    log.start([BASE], command="c", git_sha="g", db_sha256="b")               # same study, newer snapshot
    assert log.stats() == (1, None)
    log.finish({k1: {"ic_ir": 0.3, "n_dates": 50}})
    (k2,) = log.start([{**BASE, "factor": "P1"}], command="c", git_sha="g", db_sha256="b")
    assert log.stats()[0] == 2 and log.stats()[1] is None                     # crashed run: counted, no result
    log.finish({k2: {"ic_ir": -0.1, "n_dates": 50}})
    n, v = log.stats()
    assert n == 2 and v == pytest.approx(((0.3 - 0.2) ** 2 + (0.1 - 0.2) ** 2) / 1)
    d = json.loads((tmp_path / "trials.json").read_text())
    assert len(d["entries"]) == 3 and d["entries"][0]["results"] is None      # the newest entry got it
    assert not (tmp_path / "trials.json.tmp").exists()


# ── snapshot / resolution ─────────────────────────────────────────
def _backups(tmp_path: Path, src: str) -> Path:
    d = tmp_path / "backups"
    (d / "manual").mkdir(parents=True)
    shutil.copy(src, d / "taosentinel-20261001T033000Z.db")
    shutil.copy(src, d / "taosentinel-20261003T033000Z.db")
    (d / "taosentinel-20261003T033000Z.json").write_text(json.dumps({"quick_check": "corrupt"}))
    (d / "taosentinel-20261002T033000Z-pre_migrate.db").write_bytes(Path(src).read_bytes())
    (d / "taosentinel-20261004T033000Z.db.tmp").write_bytes(b"x")
    with gzip.open(d / "taosentinel-20261005T033000Z.db.gz", "wb") as f:
        f.write(b"x")
    shutil.copy(src, d / "manual" / "taosentinel-20261009T000000Z.db")
    return d


def test_newest_backup_rules(qsettings, fixture_db, tmp_path):
    qsettings()
    d = _backups(tmp_path, fixture_db["path"])
    assert newest_backup(d).name == "taosentinel-20261002T033000Z-pre_migrate.db"   # 03 is not ok, 04 .tmp, 05 .gz


def test_resolve_refusals(qsettings, fixture_db, tmp_path):
    live = tmp_path / "live.db"
    shutil.copy(fixture_db["path"], live)
    qsettings(DB_PATH=str(live))
    with pytest.raises(RefusedError, match="live writer"):
        resolve_db(str(live))
    walled = tmp_path / "w.db"
    shutil.copy(fixture_db["path"], walled)
    (tmp_path / "w.db-wal").write_bytes(b"x" * 10)
    with pytest.raises(RefusedError, match="live writer"):
        resolve_db(str(walled))
    for bad, msg in (("a.db.tmp", "not finished"), ("a.db.gz", "decompress")):
        (tmp_path / bad).write_bytes(b"x")
        with pytest.raises(RefusedError, match=msg):
            resolve_db(str(tmp_path / bad))
    with pytest.raises(RefusedError, match="no snapshot or backup"):
        resolve_db(None)


def test_make_snapshot_and_identical_panels(qsettings, fixture_db, tmp_path):
    qsettings()
    d = _backups(tmp_path, fixture_db["path"])
    src = d / "taosentinel-20261001T033000Z.db"
    out = make_snapshot(src)
    assert out.name == "quant_taosentinel-20261001T033000Z.db" and out.parent == tmp_path / "snapshots"
    con = sqlite3.connect(out)
    names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert names == {"v_quant_price_bars", "v_quant_chain_hourly", "v_quant_chain_daily", "v_quant_occupants",
                     "metric_values", "scores", "regime_daily", "events", "block_times", "schema_version",
                     "quant_snapshot_meta"}
    assert con.execute("SELECT COUNT(*) FROM v_quant_price_bars WHERE period != '1h'").fetchone()[0] == 0
    assert con.execute("SELECT COUNT(*) FROM metric_values WHERE stage='raw' AND metric_key='emission_share'"
                       ).fetchone()[0] == 0
    meta = dict(con.execute("SELECT key, value FROM quant_snapshot_meta"))
    con.close()
    assert meta["source_stamp"] == "taosentinel-20261001T033000Z" and meta["coverage_sha256"]
    cov = out.with_name(out.stem + ".coverage.json")
    assert cov.read_bytes() == Path(fixture_db["coverage"]).read_bytes()
    assert db_fingerprint(out) == db_fingerprint(out) and len(db_fingerprint(out)) == 64
    assert resolve_db(None) == str(out)                                        # snapshots before backups
    req = PanelRequest(scope="survivorship", pit_keys=("active_authors_28d", "chain:emission_share"))
    a = load_factor_panel(req, db_path=str(src))
    b = load_factor_panel(req, db_path=str(out))
    assert a.series == b.series and a.dates == b.dates
    for stamp in ("20261002T000000Z", "20261003T000000Z"):
        make_snapshot(src, tmp_path / "snapshots" / f"quant_taosentinel-{stamp}.db")
    removed = prune(2)
    left = sorted(p.name for p in (tmp_path / "snapshots").glob("quant_*.db"))
    assert left == ["quant_taosentinel-20261002T000000Z.db", "quant_taosentinel-20261003T000000Z.db"]
    assert any(p.name.endswith(".coverage.json") for p in removed)


# ── result shape ──────────────────────────────────────────────────
def _rows_from_factor(blob: dict, name: str) -> list[dict]:
    """Vendored copy of the old explorer's parser (backtest_api.py:173-195)."""
    rows = []
    scope, span = blob.get("scope", "?"), blob.get("span", "?")
    for fname, by_h in (blob.get("factors") or {}).items():
        for h, r in (by_h or {}).items():
            ic, dec = r.get("ic") or {}, r.get("deciles") or {}
            rows.append({"kind": "factor", "file": name, "run_ts": blob.get("timestamp"), "scope": scope, "span": span,
                         "factor": fname, "horizon": int(h), "lookback": int(r.get("lookback", 0)),
                         "min_depth": float(r.get("min_depth", 0.0)), "mean_ic": float(ic.get("mean_ic", 0.0)),
                         "ic_ir": float(ic.get("ic_ir", 0.0)), "t_stat": float(ic.get("t_stat", 0.0)),
                         "pct_positive": float(ic.get("pct_positive", 0.0)),
                         "avg_names": float(ic.get("avg_names", 0.0)),
                         "mean_long_short": float(dec.get("mean_long_short", 0.0)),
                         "spread_t_stat": float(dec.get("spread_t_stat", 0.0))})
    return rows


def test_factor_json_shape(qsettings, fixture_db, tmp_path):
    qsettings()
    panel = load_factor_panel(PanelRequest(scope="survivorship"), db_path=fixture_db["path"])
    res = run_factor_study(panel, factors=["size", "momentum"], horizons=[7, 84], trade_tao=2.0)
    payload = factor_payload(res, panel, min_depth=3000.0, lookback=6, warnings=["coverage_json_missing"],
                             coverage={"regime_coverage": 1.0}, provenance={"spec_version": "ts-1.0"})
    files = save_factor_json(payload, res, tmp_path / "factor_survivorship_20261017_000000.json")
    blob = json.loads(files[0].read_text())
    assert blob["timestamp"].endswith("Z") and blob["insufficient_data"] is True       # h=84 → 4 IC dates
    rows = _rows_from_factor(blob, files[0].name)
    assert len(rows) == 4 and {r["horizon"] for r in rows} == {7, 84}
    r7 = blob["factors"]["size"]["7"]
    assert r7["max_depth"] is None and "ic_series" not in r7["ic"] and r7["label"] == "ret_log_7"
    assert r7["deciles_net"] is not None and set(r7["by_regime"]) >= {"emission", "market", "market_source"}
    side = json.loads(files[1].read_text())
    assert files[1].name == "ic_series_survivorship_20261017_000000.json"
    assert len(side["size"]["7"]) == len(side["dates"]["size"]["7"]) == r7["ic"]["n_dates"]
