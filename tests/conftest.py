"""Test harness (spec 01 §9). The environment is seeded BEFORE any ``app`` import so the real .env and data/ are
never touched; every later-phase collector is disabled."""
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
_tmp = tempfile.mkdtemp(prefix="taosentinel_test_")
os.environ.setdefault("TAOSENTINEL_ENV_FILE", os.path.join(_tmp, "absent.env"))
os.environ.setdefault("TAOSENTINEL_ROOT", _tmp)
os.environ.setdefault("DB_PATH", os.path.join(_tmp, "test.db"))
os.environ.setdefault("JSONL_DIR", os.path.join(_tmp, "logs"))
os.environ.setdefault("DB_BACKUP_DIR", os.path.join(_tmp, "backups"))
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "")
os.environ.setdefault("TELEGRAM_CHAT_ID", "")
os.environ.setdefault("SYSTEMD_WATCHDOG_ENABLED", "false")
os.environ.setdefault("STARTUP_PRIME_ENABLED", "false")
os.environ.setdefault("DB_BACKUP_STARTUP_DELAY_SEC", "3600")
os.environ.setdefault("HEALTH_PORT", "0")
for k in ("REGISTRY", "TMC", "TMC_SSE", "GITHUB", "WEB", "DISCORD", "DISCORD_WIDGET", "DISCORD_GATEWAY",
          "TELEGRAM_COUNTS", "OWNER", "INTEGRITY_FEATURES", "TEAM_SEED", "FEATURES", "SCORING",
          "X", "RSS", "LLM", "DEREG_WATCH", "NEEDS_REVIEW_DIGEST", "BACKFILL_TMC", "BACKFILL_GITHUB", "DEADMAN", "WB"):
    os.environ.setdefault(f"{k}_ENABLED", "false")

import asyncio  # noqa: E402
import types  # noqa: E402
from pathlib import Path  # noqa: E402

import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402

import app.config as app_config  # noqa: E402


@pytest.fixture
def settings_override(monkeypatch):
    """``settings_override(KEY=value, …)`` rebinds app.config.settings to a fresh Settings with overrides."""
    def _apply(**kw):
        new = app_config.Settings(**kw)
        monkeypatch.setattr(app_config, "settings", new)
        return new
    return _apply


@pytest.fixture
def tmp_root(tmp_path, monkeypatch):
    """Point every runtime path at tmp_path (control files, run dir, backups, logs)."""
    monkeypatch.setattr(app_config, "APP_ROOT", tmp_path)
    s = app_config.Settings(DB_PATH=str(tmp_path / "data" / "t.db"), JSONL_DIR=str(tmp_path / "data" / "logs"),
                            DB_BACKUP_DIR=str(tmp_path / "data" / "backups"), RUN_DIR=str(tmp_path / "data" / "run"),
                            CONTROL_DIR=str(tmp_path / "data" / "control"), DATA_DIR=str(tmp_path / "data"),
                            KILL_SWITCH_PATH=str(tmp_path / "KILL_SWITCH"), PAUSE_ALL_PATH=str(tmp_path / "PAUSE_ALL"),
                            SCORING_KILL_SWITCH_PATH=str(tmp_path / "SCORING_KILLED"))
    monkeypatch.setattr(app_config, "settings", s)
    (tmp_path / "data" / "run").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "control").mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "tmp").mkdir(parents=True, exist_ok=True)
    return tmp_path


@pytest_asyncio.fixture
async def db(tmp_path):
    from app.storage.db import Database
    d = Database(tmp_path / "t.db")
    await d.connect()
    await d.apply_schema()
    yield d
    await d.close()


@pytest.fixture
def alerts_sink():
    sent: list[str] = []

    async def _sink(text: str) -> bool:
        sent.append(text)
        return True
    _sink.sent = sent  # type: ignore[attr-defined]
    return _sink


@pytest_asyncio.fixture
async def rt(tmp_root, alerts_sink):
    """A Runtime with db, limiter, Http, CostGovernor and AlertLatch (no scheduler, no HTTP server)."""
    import app.runtime as runtime_mod
    from app import control
    from app.costs import CostGovernor
    from app.http import Http
    from app.notifications.alerts import AlertLatch
    from app.ratelimit import ProviderLimiter
    from app.runtime import Runtime
    from app.storage.db import Database
    from app.util.time import now_ts
    s = app_config.settings
    r = Runtime(started_at=now_ts(), shutdown_event=asyncio.Event())
    r.db = Database(s.path(s.DB_PATH))
    await r.db.connect()
    await r.db.apply_schema()
    r.limiter = ProviderLimiter.from_settings()
    r.http = Http(r.limiter)
    r.costs = CostGovernor(r.db, limiter=r.limiter)
    await r.costs.load()
    r.http.costs = r.costs
    r.alerts = AlertLatch(r.db, sink=alerts_sink)
    r.costs.alerts = r.alerts
    await control.refresh_cache(r.db)
    control._rows.clear()
    control._by_job.clear()
    old = runtime_mod.runtime
    runtime_mod.runtime = r
    yield r
    runtime_mod.runtime = old
    await r.http.aclose()
    await r.db.close()


@pytest.fixture
def stub_module(monkeypatch):
    """Inject a fake factory module so a later-phase JOB_TABLE row becomes available."""
    made: dict[str, types.ModuleType] = {}

    def _stub(name: str, cls_name: str, cls: type) -> None:
        mod = made.get(name)
        if mod is None:
            mod = made[name] = types.ModuleType(f"app.{name}")
            monkeypatch.setitem(sys.modules, f"app.{name}", mod)
        setattr(mod, cls_name, cls)
    return _stub


@pytest.fixture
def app_client(rt):
    import httpx

    from app.api.app import create_app
    app = create_app(lambda: rt)
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


def pytest_collection_modifyitems(config, items):
    if os.environ.get("TAOSENTINEL_NETWORK_TESTS") == "1":
        return
    skip = pytest.mark.skip(reason="network test (set TAOSENTINEL_NETWORK_TESTS=1)")
    for it in items:
        if "network" in it.keywords:
            it.add_marker(skip)


# Built so far (spec 01 §2.7 "declared vs available"); bump when a phase lands.
BUILT_PHASE = 4
# 3+: 19 workbench rows; 4 = 26 until 17's manual backfill_github (S9, run after the Phase 4 verify) lands → 27
AVAILABLE_BY_PHASE = {0: 7, 1: 9, 2: 16, 3: 21, 4: 26, 5: 30, 6: 34, 7: 39, 8: 41, 9: 53, 10: 54, 11: 57}
EXPECTED_AVAILABLE = AVAILABLE_BY_PHASE[BUILT_PHASE]


def spec_path(name: str) -> Path:
    return Path(__file__).resolve().parents[1] / "specs" / name
