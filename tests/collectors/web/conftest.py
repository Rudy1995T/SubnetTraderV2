"""Spec 07 test fixtures: settings, a CheckContext over a temp DB with mocked DNS / TLS / preflight, seeding."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import app.config as app_config
from app.util.time import parse_iso

NOW = parse_iso("2026-10-17T00:10:00Z")
DAY = 86400
FIX = Path(__file__).resolve().parents[2] / "fixtures" / "web"


def fixture(name: str) -> str:
    return (FIX / name).read_text()


def web_settings(**kw: Any):
    base = dict(WEB_ENABLED=True, WEB_RETRY_DELAY_S=0, WEB_SAME_HOST_GAP_S=1, WEB_ROBOTS_ENABLED=True)
    base.update(kw)
    return app_config.Settings(_env_file=None, **base)


async def seed_occupant(db, netuid: int, *, reg: int = 1000, status: str = "active") -> str:
    okey = f"{netuid}:{reg}"
    await db.execute("INSERT OR IGNORE INTO occupants (occupant_key, netuid, network_registered_at, status, name, "
                     "first_seen_block, first_seen_at, last_seen_block, last_seen_at) VALUES (?,?,?,?,?,?,?,?,?)",
                     (okey, netuid, reg, status, f"SN{netuid}", 1, NOW - 500 * DAY, 2, NOW))
    await db.execute("INSERT OR REPLACE INTO subnets (netuid, occupant_key, network_registered_at, name, status, "
                     "updated_at) VALUES (?,?,?,?,?,?)", (netuid, okey, reg, f"SN{netuid}", status, NOW))
    return okey


async def seed_watch(db, okey: str, platform: str, url: str, *, source: str = "chain", priority: int = 10,
                     resolved: int = 1, first_seen: int | None = None, retired_at: int | None = None) -> int:
    netuid = int(okey.split(":")[0])
    await db.execute("INSERT INTO watch_channels (occupant_key, netuid, platform, handle, url, source, priority, "
                     "is_resolved, first_seen_at, last_seen_at, retired_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                     (okey, netuid, platform, url, url, source, priority, resolved,
                      first_seen if first_seen is not None else NOW - 100 * DAY, NOW, retired_at))
    return int(await db.fetchval("SELECT id FROM watch_channels WHERE occupant_key = ? AND platform = ? AND handle = ?",
                                 (okey, platform, url)))


def fast_limiter():
    """The real limiter table with fast ``web``/``wayback`` buckets (the per-provider counters still work)."""
    from app.ratelimit import ProviderLimit, ProviderLimiter, limits_from_settings
    limits = limits_from_settings()
    limits.update(web=ProviderLimit(1000.0, 1000), wayback=ProviderLimit(1000.0, 1000))
    return ProviderLimiter(limits)


class Clock:
    def __init__(self, t: float) -> None:
        self.t = float(t)

    def __call__(self) -> float:
        return self.t


@pytest.fixture
def mock_net(monkeypatch):
    """DNS answers 93.184.216.34 unless a host is listed in ``dns``; TLS returns a healthy cert unless ``tls`` says
    otherwise; preflight succeeds unless ``preflight`` is set False."""
    from app.collectors.web import check
    from app.collectors.web.fetch import DnsError, TlsInfo
    state: dict[str, Any] = {"dns": {}, "tls": {}, "preflight": True, "tls_calls": []}

    async def fake_dns(host, port, timeout_s):
        v = state["dns"].get(host, "93.184.216.34")
        if isinstance(v, BaseException):
            raise v
        if v == "nxdomain":
            raise DnsError("nxdomain", "gaierror -2")
        return v

    async def fake_tls(limiter, host, port, timeout_s, *, now, warn_days=14):
        state["tls_calls"].append(host)
        v = state["tls"].get(host)
        if v is None:
            return TlsInfo(days_left=200, not_after=int(now) + 200 * DAY, issuer="Let's Encrypt", hostname_match=True,
                           verified=True, handshakes=1)
        return v

    async def fake_preflight(hosts, timeout_s):
        return state["preflight"]

    monkeypatch.setattr(check, "resolve_dns", fake_dns)
    monkeypatch.setattr(check, "inspect_tls", fake_tls)
    monkeypatch.setattr(check, "network_preflight", fake_preflight)
    return state


@pytest.fixture
def make_ctx(db):
    from app.collectors.web.check import CheckContext
    from app.collectors.web.classify import WebHeuristicsConfig
    from app.collectors.web.robots import RobotsCache
    from app.collectors.web.wayback import WaybackClient
    from app.http import Http
    made = []

    def _make(s=None, *, now: float = NOW, dry_run: bool = False, state: dict | None = None):
        s = s or web_settings()
        lim = fast_limiter()
        http = Http(lim)
        clock = Clock(now)
        ctx = CheckContext(db=db, http=http, limiter=lim, s=s, run_id="run-test", started_at=int(now),
                           deadline=float(now + 1380), robots=RobotsCache(http, 7), wayback=WaybackClient(http),
                           heur=WebHeuristicsConfig.from_settings(s), state=state if state is not None else {},
                           dry_run=dry_run, clock=clock)
        made.append(http)
        return ctx
    yield _make


def rows_json(rows: list[dict]) -> str:
    return json.dumps(rows, default=str)
