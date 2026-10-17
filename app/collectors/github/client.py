"""GitHub REST client over 01's ``Http`` (spec 06 §6, §8): quota mirror, ETag cache, 403/301 handling.

Owns no limiter and no ``httpx`` client: every call is ``Http.request('github', …)`` (HTML: ``'github_html'``,
which carries no auth header). 01's limiter already penalises the ``github`` bucket until ``x-ratelimit-reset`` when
``remaining`` hits 0, so after a primary-limit 403 this client only decides whether the wait fits the caller's
deadline (else ``QuotaDeferred``) and retries; the bucket does the sleeping.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import re
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Awaitable, Callable
from urllib.parse import urlencode

import app.config as app_config
from app.http import ProviderError
from app.logging.logger import logger
from app.util.time import parse_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.config import Settings
    from app.http import Http
    from app.storage.db import Database

GH_OK = (200, 202, 204, 301, 302, 304, 307, 308, 401, 403, 404, 409, 422, 451)
HTML_OK = (200, 301, 302, 304, 307, 308, 404, 410, 429, 451)
_REDIRECT_ID = re.compile(r"^https://api\.github\.com/repositories/(\d+)(?:[/?].*)?$")
_LINK_NEXT = re.compile(r'<([^>]+)>\s*;\s*rel="next"')
MAX_QUOTA_SLEEP = 3700


class GitHubUnavailable(Exception):
    """Transport/5xx failure after 01's retries, or a secondary limit that did not clear (per call)."""


class GitHubAuthError(Exception):
    """401: the PAT is bad or expired (the run fails at once)."""


class QuotaDeferred(Exception):
    """Waiting for the GitHub window would pass the run's soft deadline."""

    def __init__(self, until: int) -> None:
        super().__init__(f"github quota wait until {to_iso(until)} passes the deadline")
        self.until = until


class GitHubRunDegraded(Exception):
    """Raised at the end of a run (after committing) when > 50 % of its calls failed."""


@dataclass(slots=True)
class GHResponse:
    status: int
    json: Any
    headers: dict[str, str]
    from_cache: bool = False
    location: str | None = None
    url: str = ""
    text: str | None = None

    @property
    def ok(self) -> bool:
        return self.status in (200, 304)


@dataclass(slots=True)
class PageResult:
    items: list
    status: int
    pages: int
    truncated: bool


def parse_link_next(headers: dict[str, str]) -> str | None:
    link = headers.get("link") or headers.get("Link")
    if not link:
        return None
    m = _LINK_NEXT.search(link)
    return m.group(1) if m else None


def parse_redirect_id(location: str | None) -> int | None:
    if not location:
        return None
    m = _REDIRECT_ID.match(location.strip())
    return int(m.group(1)) if m else None


# ── quota mirror (process-wide) ──────────────────────────────────────────────────────────────────────────────
@dataclass
class GitHubQuota:
    limit: int | None = None
    remaining: int | None = None
    used: int | None = None
    reset_at: int | None = None
    resource: str | None = None
    sleeping_until: int | None = None
    token_expires_at: int | None = None
    token_expiration_raw: str | None = None
    sleeps: list[int] = field(default_factory=list)
    clock: Callable[[], float] = time.time
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep

    def update(self, headers: Any) -> None:
        h = {k.lower(): v for k, v in dict(headers).items()}

        def _int(k: str) -> int | None:
            try:
                return int(float(h[k])) if k in h else None
            except (TypeError, ValueError):
                return None
        if "x-ratelimit-remaining" in h:
            self.limit = _int("x-ratelimit-limit")
            self.remaining = _int("x-ratelimit-remaining")
            self.used = _int("x-ratelimit-used")
            self.reset_at = _int("x-ratelimit-reset")
            self.resource = h.get("x-ratelimit-resource")
        raw = h.get("github-authentication-token-expiration")
        if raw:
            self.token_expiration_raw = raw
            try:
                self.token_expires_at = parse_ts(raw.replace(" UTC", "Z").replace(" ", "T", 1))
            except (TypeError, ValueError):
                self.token_expires_at = None

    def expires_at(self) -> int | None:
        if self.token_expires_at is not None:
            return self.token_expires_at
        d = app_config.settings.GITHUB_TOKEN_EXPIRES_AT
        return parse_ts(d + "T00:00:00Z") if d else None

    def effective_reserve(self, job: str, reserve: int | None) -> int:
        if reserve is not None:
            return reserve
        return 0 if job == "github_resolve" else int(app_config.settings.GITHUB_RESERVE_REMAINING)

    async def _sleep_until(self, until: int, deadline: float | None) -> None:
        now = self.clock()
        wait = min(MAX_QUOTA_SLEEP, max(0.0, until - now))
        if deadline is not None and now + wait > deadline:
            raise QuotaDeferred(int(until))
        self.sleeping_until = int(now + wait)
        self.sleeps.append(int(now))
        logger.info("rate_limit_sleep", data={"until": to_iso(self.sleeping_until), "remaining": self.remaining})
        try:
            await self.sleep(wait)
        finally:
            self.sleeping_until = None
        self.remaining = None                  # unknown until the next response

    async def wait_if_reserved(self, job: str, *, reserve: int | None = None, deadline: float | None = None) -> None:
        eff = self.effective_reserve(job, reserve)
        if eff <= 0 or self.remaining is None or self.reset_at is None or self.remaining >= eff:
            return
        if self.reset_at <= self.clock():
            return
        await self._sleep_until(self.reset_at + 2, deadline)

    async def wait_for_reset(self, deadline: float | None) -> None:
        until = (self.reset_at or int(self.clock()) + 60) + 2
        await self._sleep_until(until, deadline)

    def snapshot(self) -> dict:
        self.sleeps = [t for t in self.sleeps if t >= self.clock() - 86400]
        exp = self.expires_at()
        return {"limit": self.limit, "remaining": self.remaining, "used": self.used,
                "reset_at": to_iso(self.reset_at), "resource": self.resource,
                "sleeping_until": to_iso(self.sleeping_until), "token_expires_at": to_iso(exp),
                "token_expiration_raw": self.token_expiration_raw, "rate_limit_sleeps_24h": len(self.sleeps)}


QUOTA = GitHubQuota()


# ── client ───────────────────────────────────────────────────────────────────────────────────────────────────
class GitHubClient:
    def __init__(self, http: "Http", quota: GitHubQuota | None = None, db: "Database | None" = None,
                 settings_getter: Callable[[], "Settings"] | None = None,
                 clock: Callable[[], float] = time.time,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep) -> None:
        self.http = http
        self.quota = quota if quota is not None else QUOTA
        self.db = db
        self._s = settings_getter or (lambda: app_config.settings)
        self.clock = clock
        self.sleep = sleep
        self.token_state = "ok" if self._s().GITHUB_TOKEN else "missing"
        self.serial = False
        self._sem: asyncio.Semaphore | None = None
        self._lock = asyncio.Lock()
        self.stats: dict[str, dict[str, int]] = {}

    # per-job call accounting (GitHubRunDegraded)
    def reset_stats(self, job: str) -> None:
        self.stats[job] = {"calls": 0, "failed": 0, "cached": 0}

    def _count(self, job: str, key: str) -> None:
        self.stats.setdefault(job, {"calls": 0, "failed": 0, "cached": 0})[key] += 1

    def degraded(self, job: str) -> bool:
        st = self.stats.get(job) or {}
        return st.get("calls", 0) >= 4 and st.get("failed", 0) * 2 > st.get("calls", 0)

    @contextlib.asynccontextmanager
    async def _slot(self):
        if self._sem is None:
            self._sem = asyncio.Semaphore(max(1, int(self._s().GITHUB_MAX_CONCURRENCY)))
        async with self._sem:
            if self.serial:
                async with self._lock:
                    yield
            else:
                yield

    def _url(self, path: str) -> str:
        if path.startswith("http://") or path.startswith("https://"):
            return path
        return self._s().GITHUB_API_BASE.rstrip("/") + path

    async def _cache_get(self, key: str) -> dict | None:
        if self.db is None:
            return None
        return await self.db.fetchone("SELECT etag, status, body FROM github_http_cache WHERE url = ?", (key,))

    async def _cache_put(self, key: str, etag: str, body: bytes) -> None:
        if self.db is None:
            return
        await self.db.upsert("github_http_cache", [{"url": key, "etag": etag, "status": 200, "body": body,
                                                    "fetched_at": int(self.clock())}], conflict=("url",))

    async def get(self, path: str, *, params: dict | None = None, etag_key: str | None = None, job: str = "",
                  reserve: int | None = None, deadline: float | None = None,
                  accept: str = "application/vnd.github+json") -> GHResponse:
        s = self._s()
        url = self._url(path)
        await self.quota.wait_if_reserved(job, reserve=reserve, deadline=deadline)
        headers = {"Accept": accept, "X-GitHub-Api-Version": "2022-11-28"}
        cached = None
        ckey = None
        if etag_key is not None and s.GITHUB_ETAG_ENABLED:
            ckey = etag_key or (url + ("?" + urlencode(sorted((params or {}).items())) if params else ""))
            cached = await self._cache_get(ckey)
            if cached:
                headers["If-None-Match"] = cached["etag"]
        primary_waits = 0
        secondary = 0
        while True:
            self._count(job, "calls")
            try:
                async with self._slot():
                    resp = await self.http.request("github", "GET", url, params=params, headers=headers,
                                                   follow_redirects=False, ok_statuses=GH_OK)
            except ProviderError as exc:
                self._count(job, "failed")
                raise GitHubUnavailable(str(exc)) from exc
            hdrs = {k.lower(): v for k, v in resp.headers.items()}
            self.quota.update(hdrs)
            st = resp.status_code
            if st == 401:
                self.token_state = "bad"
                self._count(job, "failed")
                raise GitHubAuthError(f"401 from {path} (bad or expired GITHUB_TOKEN)")
            if st == 403:
                body = resp.text or ""
                if hdrs.get("x-ratelimit-remaining") == "0" and primary_waits < 2:
                    primary_waits += 1
                    await self.quota.wait_for_reset(deadline)
                    continue
                if "retry-after" in hdrs or "secondary rate limit" in body.lower():
                    secondary += 1
                    self.serial = True
                    if secondary > 3:
                        self._count(job, "failed")
                        raise GitHubUnavailable(f"secondary rate limit persisted on {path}")
                    try:
                        wait = float(hdrs.get("retry-after") or 60.0)
                    except ValueError:
                        wait = 60.0
                    wait *= 2 ** (secondary - 1)
                    if deadline is not None and self.clock() + wait > deadline:
                        raise QuotaDeferred(int(self.clock() + wait))
                    logger.warning("github.secondary_limit", data={"path": path, "wait_s": wait, "try": secondary})
                    await self.sleep(wait)
                    continue
                return GHResponse(403, _json(resp), hdrs, url=url, text=body[:500])
            if st == 304 and cached:
                self._count(job, "cached")
                return GHResponse(304, json.loads(bytes(cached["body"]).decode() or "null"), hdrs,
                                  from_cache=True, url=url)
            if st in (301, 302, 307, 308):
                return GHResponse(st, None, hdrs, location=hdrs.get("location"), url=url)
            data = _json(resp) if st in (200, 202, 404, 409, 422, 451) else None
            if st == 200 and ckey and hdrs.get("etag"):
                await self._cache_put(ckey, hdrs["etag"], resp.content)
            self.token_state = "ok" if s.GITHUB_TOKEN else "missing"
            return GHResponse(st, data, hdrs, url=url)

    async def get_all_pages(self, path: str, *, params: dict | None = None, max_pages: int, job: str = "",
                            reserve: int | None = None, deadline: float | None = None) -> PageResult:
        items: list = []
        nxt: str | None = path
        q = params
        pages = 0
        status = 200
        while nxt and pages < max_pages:
            r = await self.get(nxt, params=q, job=job, reserve=reserve, deadline=deadline)
            pages += 1
            status = r.status
            if r.status != 200 or not isinstance(r.json, list):
                return PageResult(items, r.status, pages, False)
            items.extend(r.json)
            nxt = parse_link_next(r.headers)
            q = None                                 # the next link carries the query
        return PageResult(items, status, pages, bool(nxt))

    async def get_html(self, path_or_url: str) -> GHResponse:
        s = self._s()
        url = path_or_url if path_or_url.startswith("http") else s.GITHUB_HTML_BASE.rstrip("/") + "/" + \
            path_or_url.lstrip("/")
        try:
            resp = await self.http.request("github_html", "GET", url, headers={"Accept": "text/html"},
                                           follow_redirects=False, ok_statuses=HTML_OK, retries=1,
                                           max_body_bytes=2_000_000)
        except ProviderError as exc:
            raise GitHubUnavailable(str(exc)) from exc
        hdrs = {k.lower(): v for k, v in resp.headers.items()}
        return GHResponse(resp.status_code, None, hdrs, location=hdrs.get("location"), url=url,
                          text=resp.text if resp.status_code == 200 else None)


def _json(resp: Any) -> Any:
    try:
        return resp.json() if resp.content else None
    except ValueError:
        return None


_clients: dict[int, GitHubClient] = {}


def get_client(rt: Any) -> GitHubClient:
    """One client per Runtime (shared by 06's jobs and 17's backfill); the quota mirror is process-wide."""
    c = _clients.get(id(rt))
    if c is None or c.http is not rt.http or c.db is not rt.db:
        c = GitHubClient(rt.http, QUOTA, rt.db)
        _clients[id(rt)] = c
    return c

