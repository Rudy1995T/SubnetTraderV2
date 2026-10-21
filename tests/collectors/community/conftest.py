"""Spec 08 test fixtures: settings, seeding helpers, a fast limiter + Http, fixture loaders."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

import app.config as app_config
from app.util.time import parse_iso

NOW = parse_iso("2026-10-17T00:00:05Z")        # a 00:00Z slot run (cycle_at = 2026-10-17T00:00Z)
DAY = 86400
HOUR = 3600
MAIN = "799672011265015819"
FIX = Path(__file__).resolve().parents[2] / "fixtures" / "community"
API = "https://discord.com/api/v10"


def fixture_bytes(name: str) -> bytes:
    return (FIX / name).read_bytes()


def fixture_json(name: str) -> dict:
    return json.loads(fixture_bytes(name))


def fixture_text(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


def community_settings(tmp_path: Path | None = None, **kw: Any):
    base: dict[str, Any] = dict(DISCORD_ENABLED=True, DISCORD_WIDGET_ENABLED=True, TELEGRAM_COUNTS_ENABLED=True,
                                COMMUNITY_EVENTS_ENABLED=True, DISCORD_BOT_TOKEN="", HTTP_MAX_RETRIES=0)
    if tmp_path is not None:
        base["DISCORD_CHANNELS_SEED_PATH"] = str(tmp_path / "discord_channels.csv")
    base.update(kw)
    return app_config.Settings(_env_file=None, **base)


@pytest.fixture
def cset(tmp_path, monkeypatch):
    """``cset(**overrides)`` installs community settings as app.config.settings and returns them."""
    def _apply(**kw):
        s = community_settings(tmp_path, **kw)
        monkeypatch.setattr(app_config, "settings", s)
        return s
    _apply()
    return _apply


async def seed_occupant(db, netuid: int, *, reg: int = 1000, status: str = "active", name: str | None = None) -> str:
    okey = f"{netuid}:{reg}"
    await db.execute("INSERT OR IGNORE INTO occupants (occupant_key, netuid, network_registered_at, status, name, "
                     "first_seen_block, first_seen_at, last_seen_block, last_seen_at) VALUES (?,?,?,?,?,?,?,?,?)",
                     (okey, netuid, reg, status, name or f"SN{netuid}", 1, NOW - 500 * DAY, 2, NOW))
    await db.execute("INSERT OR REPLACE INTO subnets (netuid, occupant_key, network_registered_at, name, status, "
                     "updated_at) VALUES (?,?,?,?,?,?)", (netuid, okey, reg, name or f"SN{netuid}", status, NOW))
    return okey


async def seed_identity(db, okey: str, field: str, status: str) -> None:
    await db.execute("INSERT OR REPLACE INTO identity_resolved (occupant_key, field, status, candidates_json, "
                     "resolved_at, run_id) VALUES (?,?,?,?,?,?)", (okey, field, status, "[]", NOW - 10 * DAY, "seed"))


async def seed_watch(db, okey: str, platform: str, handle: str, *, source: str = "chain", priority: int = 10,
                     resolved: int = 1, guild_id: str | None = None, declared_by: list[str] | None = None,
                     first_seen: int | None = None, meta: dict | None = None) -> int:
    netuid = int(okey.split(":")[0])
    m = {"declared_by": declared_by or [source], **(meta or {})}
    await db.execute("INSERT INTO watch_channels (occupant_key, netuid, platform, handle, url, source, priority, "
                     "is_resolved, is_main_guild, guild_id, first_seen_at, last_seen_at, meta_json) "
                     "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                     (okey, netuid, platform, handle, None, source, priority, resolved, int(guild_id == MAIN),
                      guild_id, first_seen if first_seen is not None else NOW - 100 * DAY, NOW, json.dumps(m)))
    return int(await db.fetchval("SELECT id FROM watch_channels WHERE occupant_key = ? AND platform = ? AND handle = ?",
                                 (okey, platform, handle)))


def fast_http():
    from app.http import Http
    from app.ratelimit import ProviderLimit, ProviderLimiter, limits_from_settings
    limits = limits_from_settings()
    limits.update(discord=ProviderLimit(1000.0, 1000, 100000), tme=ProviderLimit(1000.0, 1000))
    return Http(ProviderLimiter(limits))


@pytest_asyncio.fixture
async def http():
    h = fast_http()
    yield h
    await h.aclose()


def snapshot_row(okey: str | None, *, probe: str = "invite", observed_at: int, cycle_at: int | None = None,
                 guild_id: str | None = None, code: str | None = None, valid: int | None = 1, members: int | None = None,
                 presence: int | None = None, cls: str | None = None, primary: int = 0, unknown: int = 0,
                 expires_at: int | None = None, error_code: int | None = None, agreement: str | None = None) -> dict:
    return {"occupant_key": okey, "netuid": int(okey.split(":")[0]) if okey else None, "probe": probe,
            "guild_id": guild_id, "invite_code": code, "observed_at": observed_at, "event_at": None,
            "cycle_at": cycle_at if cycle_at is not None else observed_at - observed_at % (6 * HOUR),
            "source": "discord_api", "http_status": 200 if valid == 1 else 404 if valid == 0 else None,
            "member_count": members, "presence_count": presence,
            "presence_ratio": presence / members if members and presence is not None else None,
            "invite_valid": valid, "invite_error_code": error_code, "expires_at": expires_at,
            "topology_class": cls, "is_primary": primary, "unknown_targets": unknown, "source_agreement": agreement,
            "run_id": "seed"}


async def insert_snapshots(db, rows: list[dict]) -> None:
    from app.collectors.community_store import insert_discord_rows
    await insert_discord_rows(db, rows)


async def seed_heartbeat(db, hour_ts: int, ok: bool = True) -> None:
    from app.collectors.community_store import write_gateway_heartbeat
    await write_gateway_heartbeat(db, hour_ts, ok, 0, None)


@pytest.fixture
def frozen(monkeypatch):
    """``frozen(t)`` pins ``now_ts`` in every 08 module (fetch times, census, evidence, events) to ``t``."""
    import app.collectors.community_events as m1
    import app.collectors.community_store as m2
    import app.collectors.discord as m3
    import app.collectors.discord_api as m4
    import app.collectors.discord_gateway as m5
    import app.collectors.telegram as m6
    import app.features.community as m7
    clock = {"t": NOW}

    def _set(t: int) -> None:
        clock["t"] = int(t)
    for m in (m1, m2, m3, m4, m5, m6, m7):
        if hasattr(m, "now_ts"):
            monkeypatch.setattr(m, "now_ts", lambda: clock["t"])
    _set(NOW)
    return _set
