"""Settings inventory, defaults, validators, aliases, dynamic-settings rule (spec 01 §9 test_config)."""
import re
from pathlib import Path

import pytest
from pydantic import ValidationError

import app.config as app_config
from app.config import Settings
from tests.conftest import spec_path


def S(**kw):
    return Settings(**kw)


def test_defaults():
    s = S()
    assert s.HEALTH_PORT in (8090, 0)          # conftest seeds HEALTH_PORT=0
    assert Settings.model_fields["HEALTH_PORT"].default == 8090
    assert s.SPEC_VERSION == "ts-1.0" and s.SCORING_SPEC_VERSION == "ts-1.0"
    assert (s.TMC_RATE_PER_MIN, s.TMC_RATE_BURST, s.TMC_HOURLY_CAP, s.TMC_MONTHLY_CAP) == (8, 2, 500, 90000)
    assert s.X_MONTHLY_CAP_USD == 25 and s.LLM_MONTHLY_CAP_USD == 60
    assert s.CHAIN_TICKS_RETENTION_DAYS == 30 and s.CHAIN_TICKS_BLOCK_STRIDE == 5
    assert s.QUANT_WEEKLY_JOB_CRON == "sun 05:00" and s.BACKFILL_TMC_MAX_PER_HOUR == 180
    assert s.DISCORD_STORE_TEXT is True and s.TELEGRAM_POSTS_STORE_TEXT is False
    assert s.LLM_PENDING_TEXT_MAX_DAYS == 7 and s.DB_BACKUP_MAX_TOTAL_GB == 12 and s.DB_BACKUP_STALE_HOURS == 36
    assert s.NETWORK_IMMUNITY_PERIOD_BLOCKS == 864000


@pytest.mark.parametrize("kw", [
    {"TMC_RATE_PER_MIN": 9, "TMC_RATE_BURST": 2}, {"SCORING_OFFICIAL_HOUR": 6}, {"SUBTENSOR_RPC_MODE": "sdk"},
    {"LOG_LEVEL": "warn"}, {"CONTROL_ALLOWED_ORIGINS": "*"}, {"SHUTDOWN_DEADLINE_SEC": 5.0},
    {"TMC_HOURLY_CAP": 601}, {"X_MONTHLY_CAP_USD": -1}, {"DISK_FREE_CRITICAL_GB": 5.0},
    {"REGISTRY_IDENTITY_AT_UTC": "25:00"}, {"QUANT_WEEKLY_JOB_CRON": "funday 05:00"},
])
def test_validators_reject(kw):
    with pytest.raises(ValidationError):
        S(**kw)


def test_aliases(monkeypatch):
    monkeypatch.delenv("HEALTH_PORT", raising=False)
    monkeypatch.setenv("GITHUB_PAT", "ghp_x")
    monkeypatch.setenv("API_HOST", "127.0.0.2")
    monkeypatch.setenv("API_PORT", "9001")
    monkeypatch.setenv("TAOSTATS_MONTHLY_CALL_CAP", "123")
    s = S()
    assert s.GITHUB_TOKEN == "ghp_x" and s.HEALTH_HOST == "127.0.0.2" and s.HEALTH_PORT == 9001
    assert s.TAOSTATS_MONTHLY_CAP_REQ == 123
    assert any("TAOSTATS_MONTHLY_CALL_CAP" in w for w in s.config_warnings)


def test_scoring_spec_version_read_only(monkeypatch):
    monkeypatch.setenv("SCORING_SPEC_VERSION", "ts-9")
    s = S()
    assert s.SCORING_SPEC_VERSION == s.SPEC_VERSION == "ts-1.0"
    assert any("SCORING_SPEC_VERSION" in w for w in s.config_warnings)
    with pytest.raises(AttributeError):
        s.SCORING_SPEC_VERSION = "x"  # type: ignore[misc]


def test_x_active_and_caps():
    assert not S(X_ENABLED=True, X_PROVIDER="twitterapi_io", TWITTERAPI_IO_KEY="k", X_MONTHLY_CAP_USD=0).x_active()
    assert S(X_ENABLED=True, X_PROVIDER="twitterapi_io", TWITTERAPI_IO_KEY="k").x_active()
    assert not S(X_ENABLED=True, X_PROVIDER="null").x_active()
    assert not S(LLM_ENABLED=True, ANTHROPIC_API_KEY="k", LLM_MONTHLY_CAP_USD=0).llm_active()


def test_fail_safe_downgrades():
    s = S(DEADMAN_ENABLED=True, DISCORD_GATEWAY_ENABLED=True, WEB_SPN2_ENABLED=True)
    assert not s.DEADMAN_ENABLED and not s.DISCORD_GATEWAY_ENABLED and not s.WEB_SPN2_ENABLED
    assert S(ONFINALITY_RATE_PER_SEC=20).ONFINALITY_RATE_PER_SEC == 4.0
    assert S(ONFINALITY_RATE_PER_SEC=20, ONFINALITY_API_KEY="k").ONFINALITY_RATE_PER_SEC == 20


def _spec_inventory() -> set[str]:
    lines = spec_path("01-repo-skeleton-runtime.md").read_text().split("\n")
    s = next(i for i, l in enumerate(lines) if l.startswith("## 5. Configuration"))
    e = next(i for i, l in enumerate(lines) if l.startswith("**Retired keys**"))
    keys: set[str] = set()
    for l in lines[s:e]:
        if l.startswith("| `"):
            keys.update(re.findall(r"`([A-Z0-9_]+)`", l.split("|")[1]))
    return keys


def test_inventory_equals_spec():
    inv = _spec_inventory()
    assert len(inv) == 660
    fields = set(Settings.model_fields) | {"SCORING_SPEC_VERSION"}
    assert inv == fields


def test_no_static_settings_import():
    root = Path(app_config.__file__).resolve().parent
    bad = []
    rx = re.compile(r"^\s*from\s+app\.config\s+import\s+[^\n]*\bsettings\b", re.M)
    for p in root.rglob("*.py"):
        if rx.search(p.read_text()):
            bad.append(str(p))
    assert not bad, f"static settings import (forbidden): {bad}"


def test_public_dict_masks_secrets():
    s = S(TELEGRAM_BOT_TOKEN="123:abc", GITHUB_TOKEN="ghp", DEADMAN_PING_URL="https://hc/x", IA_SECRET_KEY="z",
          ANTHROPIC_API_KEY="sk")
    d = s.public_dict()
    for k in ("TELEGRAM_BOT_TOKEN", "GITHUB_TOKEN", "DEADMAN_PING_URL", "IA_SECRET_KEY", "ANTHROPIC_API_KEY"):
        assert d[k] == "••••••••"
    assert d["TELEGRAM_CHAT_ID"] == s.TELEGRAM_CHAT_ID
    assert d["SCORING_SPEC_VERSION"] == "ts-1.0"


def test_reload_rebinds(monkeypatch):
    monkeypatch.setattr(app_config, "settings", app_config.settings)
    monkeypatch.setenv("LOG_RETENTION_DAYS", "3")
    new, changed = app_config.reload_settings()
    assert app_config.settings is new and new.LOG_RETENTION_DAYS == 3 and "LOG_RETENTION_DAYS" in changed


def test_env_example_roundtrips_defaults():
    root = Path(app_config.__file__).resolve().parents[1]
    ex = Settings(_env_file=str(root / ".env.example"))
    base = Settings(_env_file=None)
    diffs = [k for k in Settings.model_fields if getattr(ex, k) != getattr(base, k)
             and k not in {"HEALTH_PORT", "DB_PATH", "JSONL_DIR", "DB_BACKUP_DIR", "SYSTEMD_WATCHDOG_ENABLED",
                           "STARTUP_PRIME_ENABLED", "DB_BACKUP_STARTUP_DELAY_SEC"} and not k.endswith("_ENABLED")]
    assert diffs == []


def test_terminal_url_must_be_loopback():
    from pydantic import ValidationError
    assert Settings(_env_file=None, TERMINAL_API_URL="http://localhost:8787").TERMINAL_API_URL
    assert Settings(_env_file=None, TERMINAL_API_URL="").TERMINAL_API_URL == ""
    with pytest.raises(ValidationError):
        Settings(_env_file=None, TERMINAL_API_URL="https://example.com:8787")
