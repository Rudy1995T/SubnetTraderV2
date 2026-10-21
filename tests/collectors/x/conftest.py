"""Spec 09 test helpers: X settings, a fast Http, fixture loaders, FakeProvider / FakeGovernor, seeding."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pytest
import pytest_asyncio

import app.config as app_config
from app.collectors.x_providers.base import XCall, XPage, XPost, XProviderError, XUser, max_id
from app.util.time import parse_iso

FIX = Path(__file__).resolve().parents[2] / "fixtures" / "x"
NOW = parse_iso("2026-10-17T12:30:00Z")
DAY = 86400
EPOCH_MS = 1288834974657


def fixture_json(rel: str) -> dict:
    return json.loads((FIX / rel).read_text(encoding="utf-8"))


def sf(ts: int, seq: int = 0) -> str:
    """A snowflake id for unix second ``ts``."""
    return str(((int(ts) * 1000 - EPOCH_MS) << 22) | seq)


def dt(ts: int) -> datetime:
    return datetime.fromtimestamp(int(ts), timezone.utc)


def x_settings(**kw: Any):
    base: dict[str, Any] = dict(X_ENABLED=True, X_PROVIDER="twitterapi_io", TWITTERAPI_IO_KEY="test-key",
                                X_BEARER_TOKEN="test-bearer", HTTP_MAX_RETRIES=0, X_RAW_ARCHIVE_ENABLED=False)
    base.update(kw)
    return app_config.settings.model_copy(update=base)


@pytest.fixture
def xset(monkeypatch):
    """``xset(**overrides)`` installs X-active settings (on top of the current ones, e.g. tmp_root paths)."""
    def _apply(**kw):
        s = x_settings(**kw)
        monkeypatch.setattr(app_config, "settings", s)
        return s
    _apply()
    return _apply


def fast_http():
    from app.http import Http
    from app.ratelimit import ProviderLimit, ProviderLimiter, limits_from_settings
    limits = limits_from_settings()
    limits.update(twitterapi_io=ProviderLimit(1000.0, 1000), official_v2=ProviderLimit(1000.0, 1000))
    return Http(ProviderLimiter(limits))


@pytest_asyncio.fixture
async def http():
    h = fast_http()
    yield h
    await h.aclose()


class SpyHttp:
    """Wraps an Http and records every ``request`` call (provider id, ok_statuses)."""

    def __init__(self, inner) -> None:
        self.inner = inner
        self.calls: list[dict] = []

    async def request(self, provider, method, url, **kw):
        self.calls.append({"provider": provider, "method": method, "url": url, **kw})
        return await self.inner.request(provider, method, url, **kw)


# ── fakes ────────────────────────────────────────────────────────────────────────────────────────────────────
def user(uid: str, handle: str, *, followers: int = 5000, following: int = 300, posts: int = 1000,
         created: int | None = None, verified: bool | None = False) -> XUser:
    created_at = dt(created if created is not None else NOW - 900 * DAY)
    return XUser(uid, handle, handle, followers, following, posts, created_at, verified,
                 raw={"id": uid, "userName": handle})


def post(ts: int, author_id: str, handle: str, text: str, *, seq: int = 0, likes: int | None = 10,
         reposts: int | None = 1, replies: int | None = 1, reply_to: str | None = None, repost: bool = False,
         author: XUser | None = None, has_url: bool = False, pid: str | None = None) -> XPost:
    p = pid or sf(ts, seq)
    return XPost(p, author_id, handle, text, "en", dt(ts), likes, reposts, replies, 0, 100, has_url, False, reply_to,
                 repost, None, author, raw={"id": p, "text": text})


@dataclass
class FakeProvider:
    """Scripted provider: ``timelines[author_id]`` = list of pages (XPage | Exception) served in order per run;
    ``searches`` = callable(query, since_id, since_at, cursor) → XPage | Exception; ``users`` = handle/id → XUser |
    None | Exception."""
    name: str = "twitterapi_io"
    cost_per_call: float = 0.001
    users: dict[str, Any] = field(default_factory=dict)
    timelines: dict[str, list] = field(default_factory=dict)
    searches: Callable[..., Any] | None = None
    calls: list[tuple] = field(default_factory=list)
    supports_by_id: bool = True
    timeline_page_size: int = 20
    search_page_size: int = 20

    async def resolve_user(self, handle: str):
        self.calls.append(("resolve_user", handle))
        return self._ret(self.users.get(handle.lower()))

    async def resolve_user_by_id(self, user_id: str):
        self.calls.append(("resolve_user_by_id", user_id))
        return self._ret(self.users.get(user_id))

    async def user_timeline(self, user_id, since_id, max_posts, cursor=None):
        self.calls.append(("user_timeline", user_id, since_id, cursor))
        pages = self.timelines.get(user_id) or []
        if not pages:
            return XPage([], None, None, 0)
        v = pages.pop(0)
        if isinstance(v, Exception):
            raise v
        if isinstance(v, list):                          # a list of posts: build the page like a real provider
            ids = [p.post_id for p in v]
            return XPage([p for p in v if since_id is None or int(p.post_id) > int(since_id)], None, max_id(ids),
                         len(v))
        return v

    async def search_recent(self, query, since_id, since_at, max_posts, cursor=None):
        self.calls.append(("search_recent", tuple(query.from_handles) or tuple(query.terms), since_id, cursor))
        v = self.searches(query, since_id, since_at, cursor) if self.searches else XPage([], None, None, 0)
        if isinstance(v, Exception):
            raise v
        if isinstance(v, list):
            ids = [p.post_id for p in v]
            return XPage([p for p in v if since_id is None or int(p.post_id) > int(since_id)], None, max_id(ids),
                         len(v))
        return v

    @staticmethod
    def _ret(v):
        if isinstance(v, Exception):
            raise v
        return v

    def cost_estimate(self, call: XCall) -> float:
        return self.cost_per_call

    def cost_actual(self, call: XCall, page: XPage | None) -> float:
        return self.cost_per_call

    def render_query(self, q, since_at) -> str:
        return " OR ".join(q.terms or q.from_handles)

    def paid_calls(self) -> int:
        return len(self.calls)


@dataclass
class Decision:
    allowed: bool
    reason: str | None = None
    usd_mtd: float = 0.0
    reserved_usd: float = 0.0
    cap_usd: float | None = 25.0
    projected_usd: float | None = None


class FakeGovernor:
    """16's exact signatures (keyword-only after the USD argument)."""

    def __init__(self, decisions: list[Decision] | Callable[[int], Decision] | None = None) -> None:
        self.decisions = decisions
        self.checks: list[tuple[str, float]] = []
        self.records: list[dict] = []
        self.order: list[str] = []

    async def check(self, provider: str, est_usd: float) -> Decision:
        self.checks.append((provider, est_usd))
        self.order.append("check")
        if callable(self.decisions):
            return self.decisions(len(self.checks))
        if self.decisions:
            return self.decisions.pop(0) if len(self.decisions) > 1 else self.decisions[0]
        return Decision(True)

    async def record(self, provider: str, usd: float, *, requests: int = 0, units: dict | None = None,
                     job: str | None = None, run_id: str | None = None, detail: dict | None = None) -> None:
        self.order.append("record")
        self.records.append({"provider": provider, "usd": usd, "requests": requests, "units": units, "job": job,
                             "run_id": run_id})

    def month_usd(self, name: str) -> float:
        return sum(r["usd"] for r in self.records)

    def run_total(self, run_id: str) -> float:
        return sum(r["usd"] for r in self.records if r["run_id"] == run_id)

    def is_exhausted(self, name: str) -> bool:
        return False


def transient() -> XProviderError:
    return XProviderError("transient", "HTTP 503", status=503)


# ── seeding ──────────────────────────────────────────────────────────────────────────────────────────────────
async def seed_occupant(db, netuid: int, *, name: str | None = None, reg: int = 1000, status: str = "active") -> str:
    okey = f"{netuid}:{reg}"
    await db.execute("INSERT OR IGNORE INTO occupants (occupant_key, netuid, network_registered_at, status, name, "
                     "first_seen_block, first_seen_at, last_seen_block, last_seen_at) VALUES (?,?,?,?,?,?,?,?,?)",
                     (okey, netuid, reg, status, name or f"subnet{netuid}", 1, NOW - 500 * DAY, 2, NOW))
    await db.execute("INSERT OR REPLACE INTO subnets (netuid, occupant_key, network_registered_at, name, status, "
                     "updated_at) VALUES (?,?,?,?,?,?)", (netuid, okey, reg, name or f"subnet{netuid}", status, NOW))
    return okey


async def seed_x_watch(db, okey: str, handle: str, *, source: str = "subnetalpha", first_seen: int | None = None,
                       retired_at: int | None = None) -> int:
    netuid = int(okey.split(":")[0])
    await db.execute("INSERT INTO watch_channels (occupant_key, netuid, platform, handle, url, source, priority, "
                     "is_resolved, first_seen_at, last_seen_at, retired_at, meta_json) "
                     "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                     (okey, netuid, "x", handle, f"https://x.com/{handle}", source, 20, 1,
                      first_seen if first_seen is not None else NOW - 100 * DAY, NOW, retired_at, "{}"))
    return int(await db.fetchval("SELECT id FROM watch_channels WHERE occupant_key = ? AND platform = 'x' AND "
                                 "handle = ?", (okey, handle)))


async def seed_account(db, author_id: str, handle: str, *, kind: str = "team", status: str = "active",
                       okeys: list[str] = (), since_id: str | None = None, last_polled_at: int | None = None,
                       resolved_at: int | None = None, kol_handle: str | None = None) -> None:
    await db.execute("INSERT INTO x_accounts (author_id, handle, handle_lower, kind, kol_handle, status, since_id, "
                     "resolved_at, last_polled_at, provider, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                     (author_id, handle, handle.lower(), kind, kol_handle, status, since_id,
                      resolved_at if resolved_at is not None else NOW - 60 * DAY, last_polled_at, "twitterapi_io",
                      NOW - 60 * DAY))
    for o in okeys:
        await db.execute("INSERT INTO x_account_occupants (author_id, occupant_key, source) VALUES (?,?,?)",
                         (author_id, o, "watch_channels"))


async def seed_kol(db, handle: str, *, credibility: float = 0.8, verified: int = 1, active: int = 1,
                   author_id: str | None = None, team_keys: list[str] | None = None, category: str = "analyst") -> None:
    await db.execute("INSERT INTO kol_accounts (handle, handle_lower, platform, author_id, display_name, category, "
                     "credibility, verified, active, team_occupant_keys, added_at, added_by, updated_at) "
                     "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                     (handle, handle.lower(), "x", author_id, handle, category, credibility, verified, active,
                      json.dumps(team_keys or []), NOW - 30 * DAY, "seed", NOW - 30 * DAY))


def make_collector(db, provider, costs=None, *, job_id: str = "x_team_poll", run_id: str = "run-1", now: int = NOW,
                   clock: Callable[[], int] | None = None):
    from app.collectors.x import XCollector
    return XCollector(db, provider, costs if costs is not None else FakeGovernor(), job_id=job_id, run_id=run_id,
                      now=now, clock=clock or (lambda: now))
