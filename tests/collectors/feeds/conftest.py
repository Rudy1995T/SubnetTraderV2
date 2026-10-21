"""Spec 09 Appendix A + §2.9 test helpers: settings, seeding, a fast limiter + Http, fixture loaders."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

import app.config as app_config
from app.util.time import parse_iso

NOW = parse_iso("2026-10-18T07:00:05Z")
DAY = 86400
FIX = Path(__file__).resolve().parents[2] / "fixtures"


def fixture_bytes(area: str, name: str) -> bytes:
    return (FIX / area / name).read_bytes()


def fixture_text(area: str, name: str) -> str:
    return (FIX / area / name).read_text(encoding="utf-8")


def feeds_settings(tmp_path: Path | None = None, **kw: Any):
    base: dict[str, Any] = dict(RSS_ENABLED=True, YOUTUBE_ENABLED=True, RSS_GOOGLE_NEWS_ENABLED=True,
                                X_CTF_CROSSCHECK_ENABLED=True, HTTP_MAX_RETRIES=0, YOUTUBE_API_KEY="",
                                GOOGLE_NEWS_MIN_INTERVAL_SEC=10.0)
    if tmp_path is not None:
        base["RSS_FEEDS_SEED_PATH"] = str(tmp_path / "feeds.csv")
    base.update(kw)
    return app_config.Settings(_env_file=None, **base)


@pytest.fixture
def fset(tmp_path, monkeypatch):
    """``fset(**overrides)`` installs feed settings as app.config.settings (seed path → an empty tmp CSV)."""
    (tmp_path / "feeds.csv").write_text("kind,label,url,occupant_netuid,enabled,filtered,notes\n")

    def _apply(**kw):
        s = feeds_settings(tmp_path, **kw)
        monkeypatch.setattr(app_config, "settings", s)
        return s
    _apply()
    return _apply


def fast_http():
    from app.http import Http
    from app.ratelimit import ProviderLimit, ProviderLimiter, limits_from_settings
    limits = limits_from_settings()
    for p in ("subnetalpha", "rss", "youtube"):
        limits[p] = ProviderLimit(1000.0, 1000)
    return Http(ProviderLimiter(limits))


@pytest_asyncio.fixture
async def http():
    h = fast_http()
    yield h
    await h.aclose()


async def seed_occupant(db, netuid: int, name: str | None = None, *, reg: int = 1000, status: str = "active") -> str:
    okey = f"{netuid}:{reg}"
    await db.execute("INSERT OR IGNORE INTO occupants (occupant_key, netuid, network_registered_at, status, name, "
                     "first_seen_block, first_seen_at, last_seen_block, last_seen_at) VALUES (?,?,?,?,?,?,?,?,?)",
                     (okey, netuid, reg, status, name or f"subnet{netuid}", 1, NOW - 500 * DAY, 2, NOW))
    await db.execute("INSERT OR REPLACE INTO subnets (netuid, occupant_key, network_registered_at, name, status, "
                     "updated_at) VALUES (?,?,?,?,?,?)", (netuid, okey, reg, name or f"subnet{netuid}", status, NOW))
    return okey


async def seed_watch(db, okey: str | None, platform: str, handle: str, *, source: str = "subnetalpha",
                     url: str | None = None, meta: dict | None = None, first_seen: int | None = None,
                     retired_at: int | None = None, filtered: int = 0) -> int:
    netuid = int(okey.split(":")[0]) if okey else None
    await db.execute("INSERT INTO watch_channels (occupant_key, netuid, platform, handle, url, source, priority, "
                     "filtered, first_seen_at, last_seen_at, retired_at, meta_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                     (okey, netuid, platform, handle, url, source, 20, filtered,
                      first_seen if first_seen is not None else NOW - 100 * DAY, NOW, retired_at,
                      json.dumps(meta or {"declared_by": [source]})))
    return int(await db.fetchval("SELECT id FROM watch_channels WHERE COALESCE(occupant_key, '') = ? AND platform = ? "
                                 "AND handle = ?", (okey or "", platform, handle)))


async def seed_identity(db, okey: str, field: str, value: str, *, observed_at: int | None = None) -> None:
    await db.execute("INSERT INTO identity_observations (occupant_key, netuid, source, field, value, value_hash, "
                     "observed_at, run_id) VALUES (?,?,?,?,?,?,?,?)",
                     (okey, int(okey.split(":")[0]), "subnetalpha", field, value,
                      hashlib.sha256(value.encode()).hexdigest(), observed_at or NOW - DAY, "seed"))


async def seed_feed_ok(db, feed_url: str, at: int, *, kind: str = "rss", occupant_key: str | None = None,
                       status: int = 200, job: str = "rss_feeds_poll") -> None:
    await db.execute("INSERT OR REPLACE INTO feed_state (feed_url, kind, occupant_key, last_fetched_at, last_status, "
                     "consecutive_failures, enabled) VALUES (?,?,?,?,?,0,1)", (feed_url, kind, occupant_key, at, status))
    if status in (200, 304):
        await db.execute("INSERT OR REPLACE INTO job_state (job, key, value_json, updated_at) VALUES (?,?,?,?)",
                         (job, f"feed_ok:{feed_url}", json.dumps({"at": at, "status": status}), at))
