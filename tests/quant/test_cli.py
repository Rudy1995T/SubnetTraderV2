"""CLI smoke tests (spec 13 §8.7, §9) — subprocesses on the synthetic snapshot."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def cli(fixture_db, tmp_path_factory):
    base = tmp_path_factory.mktemp("quantcli")
    (base / "backups").mkdir()
    shutil.copy(fixture_db["path"], base / "backups" / "taosentinel-20260925T033000Z.db")
    env = dict(os.environ, TAOSENTINEL_ENV_FILE=str(base / "absent.env"), TAOSENTINEL_ROOT=str(base),
               DB_PATH=str(base / "live.db"), DB_BACKUP_DIR=str(base / "backups"),
               QUANT_RESULTS_DIR=str(base / "results"), QUANT_SNAPSHOT_DIR=str(base / "snapshots"),
               COVERAGE_PATH=fixture_db["coverage"], QUANT_OBSERVE_TELEMETRY_PATH=str(base / "telemetry.json"),
               JSONL_DIR=str(base / "logs"))

    def run(*args, extra_env=None, db=True):
        a = list(args)
        if db and a[0] not in ("snapshot",):
            a += ["--db", fixture_db["path"]]
        r = subprocess.run([sys.executable, "-m", "app.quant", *a], cwd=ROOT, env={**env, **(extra_env or {})},
                           capture_output=True, text=True, timeout=300)
        lines = [ln for ln in r.stdout.splitlines() if ln.startswith("RESULT ")]
        r.result = json.loads(lines[-1][7:]) if lines else None
        return r
    run.base = base
    return run


def test_snapshot_and_refusals(cli):
    r = cli("snapshot", "--prune")
    assert r.returncode == 0, r.stderr
    assert "snapshot written" in r.stdout and "tables: 11" in r.stdout
    assert Path(r.result["files"][0]).name == "quant_taosentinel-20260925T033000Z.db"
    live = cli.base / "live.db"
    shutil.copy(cli.base / "backups" / "taosentinel-20260925T033000Z.db", live)
    r = cli("snapshot", "--from", str(live))
    assert r.returncode == 2 and "refusing to read the live writer DB" in r.stderr
    gz = cli.base / "x.db.gz"
    gz.write_bytes(b"x")
    assert cli("snapshot", "--from", str(gz)).returncode == 2


def test_factor_both_with_output_and_demis(cli):
    out = cli.base / "out" / "P.json"
    r = cli("factor", "--factors", "size,momentum", "--horizons", "7", "--scope", "both", "--output", str(out))
    assert r.returncode in (0, 1), r.stderr
    assert (cli.base / "out" / "P_live.json").is_file() and (cli.base / "out" / "P_survivorship.json").is_file()
    r = cli("factor", "--factors", "active_authors_28d", "--label", "demis", "--horizons", "28", "--scope",
            "survivorship")
    assert r.returncode in (0, 1), r.stderr
    blob = json.loads(Path(r.result["files"][0]).read_text())
    assert blob["factors"]["active_authors_28d"]["28"]["label"] == "demis_28"
    assert blob["factors"]["active_authors_28d"]["28"]["step"] == 28


def test_studies(cli):
    r = cli("survival", "--factors", "quality_cg", "--scope", "survivorship", "--pit-mode", "reconstructed")
    assert r.returncode in (0, 1), r.stderr
    s = json.loads(Path(r.result["files"][0]).read_text())
    assert {"n_events", "n_event_dates", "n_events_by_kind", "death_ratio_dereg_only"} <= set(s)
    r = cli("walk-forward", "--factors", "momentum", "--scope", "survivorship")
    assert r.returncode in (0, 1), r.stderr
    assert json.loads(Path(r.result["files"][0]).read_text())["reports"][0]["n_oos_folds"] >= 1
    r = cli("rank-stability", "--factors", "quality", "--scope", "survivorship", "--pit-mode", "reconstructed")
    assert r.returncode in (0, 1), r.stderr
    rs = json.loads(Path(r.result["files"][0]).read_text())
    assert rs["results"][0]["mean_rank_autocorr"] == pytest.approx(1.0)        # quality is static per name


def test_report_gate_and_from_results(cli):
    r = cli("report", "--run-all", "--pit-mode", "reconstructed")
    assert r.returncode in (0, 1), r.stderr
    v = json.loads(Path(r.result["verdict_file"]).read_text())
    assert v["pending"] == ["c7"] and v["gate_pass"] is False and v["promotable"] is False
    assert isinstance(v["generated_at"], int) and v["trials"]["n_trials"] >= 1
    assert v["provenance"]["evidence_class"] == "reconstructed" and v["provenance"]["db_snapshot"]["sha256"]
    r2 = cli("report", "--from-results", str(cli.base / "results"), "--pit-mode", "reconstructed")
    assert r2.returncode in (0, 1), r2.stderr
    v2 = json.loads(Path(r2.result["verdict_file"]).read_text())
    for k in ("c1_ic_both_scopes", "c2_survives_controls", "c3_deciles_net", "c5_walk_forward", "c6_survival"):
        assert v2["criteria"][k]["pass"] == v["criteria"][k]["pass"], k
    r3 = cli("gate", "--factor", "quality_cg", "--scopes", "survivorship,live", "--pit-mode", "reconstructed")
    assert r3.returncode in (0, 1), r3.stderr
    assert r3.result["verdict_file"]


def test_refusals_and_exit_codes(cli):
    assert cli("report").returncode == 2
    assert cli("gate", "--scopes", "live").returncode == 2
    tel = cli.base / "tel.db"
    tel.write_bytes(b"SQLite format 3\x00" + b"\x00" * 100)
    r = cli("report", "--run-all", "--observe-telemetry", str(tel))
    assert r.returncode == 2 and "SQLite" in r.stderr
    r = cli("factor", "--factors", "owner_conviction", "--horizons", "7")
    assert r.returncode == 2 and "owner_commitment" in r.stderr
    assert cli("factor", "--factors", "size", "--min-names", "5").returncode == 2
    assert cli("factor", "--factors", "size", extra_env={"QUANT_ENABLED": "false"}).returncode == 3
    trials = json.loads((cli.base / "results" / "trials.json").read_text())
    assert trials["n_trials"] == len({e["trial_key"] for e in trials["entries"]})
