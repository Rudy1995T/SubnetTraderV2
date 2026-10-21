"""Spec 13 fixtures: one synthetic snapshot per session (tests/quant/fixtures.py) + settings rooted in tmp dirs."""
from __future__ import annotations

import pytest

import app.config as app_config

from .fixtures import make_fixture_db


@pytest.fixture(scope="session")
def fixture_db(tmp_path_factory):
    d = tmp_path_factory.mktemp("quantfx")
    return make_fixture_db(d / "quant_taosentinel-20260925T000000Z.db")


@pytest.fixture
def qsettings(tmp_path, monkeypatch, fixture_db):
    """Settings with results/snapshots under tmp_path and COVERAGE_PATH at the fixture's coverage.json."""
    def _make(**kw):
        base = dict(DB_PATH=str(tmp_path / "live.db"), DB_BACKUP_DIR=str(tmp_path / "backups"),
                    QUANT_RESULTS_DIR=str(tmp_path / "results"), QUANT_SNAPSHOT_DIR=str(tmp_path / "snapshots"),
                    COVERAGE_PATH=fixture_db["coverage"], JSONL_DIR=str(tmp_path / "logs"),
                    QUANT_OBSERVE_TELEMETRY_PATH=str(tmp_path / "telemetry.json"))
        base.update(kw)
        s = app_config.Settings(**base)
        monkeypatch.setattr(app_config, "settings", s)
        return s
    return _make
