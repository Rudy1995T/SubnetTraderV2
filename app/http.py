"""The one outbound HTTP layer (spec 01 §2.12, 02 §G1).

Every call goes through ``Http.request(provider, method, url, …)`` on the process-wide ``ProviderLimiter``.
Statuses in ``ok_statuses`` return at once; 429 → per-host cooldown + retry; 5xx/timeouts/connect errors →
backoff 1 s, 4 s, 16 s … then ``ProviderError`` (chained); every other status (404, 403, 401, 3xx, 304 …) is
returned unchanged and never retried within the call.
"""
from __future__ import annotations

import asyncio
import re
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

import httpx

import app.config as app_config
from app.logging.logger import logger
from app.ratelimit import BudgetExhausted, ProviderLimiter  # noqa: F401 (BudgetExhausted re-exported)

if TYPE_CHECKING:  # pragma: no cover
    from app.costs import CostGovernor

_LOOPBACK = {"127.0.0.1", "::1", "localhost"}
_BOT_TOKEN = re.compile(r"/bot[^/]+")


def redact_path(path: str) -> str:
    return _BOT_TOKEN.sub("/bot***", path)


def redact_url(url: str) -> str:
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.hostname or ''}{redact_path(parts.path)}"


class ProviderError(Exception):
    def __init__(self, provider: str, message: str, *, status: int | None, url: str, attempts: int) -> None:
        super().__init__(f"{provider}: {message}")
        self.provider = provider
        self.status = status
        self.url = redact_url(url)
        self.attempts = attempts


def _auth_headers(provider: str) -> dict[str, str]:
    s = app_config.settings
    if provider in ("tmc", "tmc_sse") and s.TMC_API_KEY:
        return {"Authorization": s.TMC_API_KEY}
    if provider == "github" and s.GITHUB_TOKEN:
        return {"Authorization": f"Bearer {s.GITHUB_TOKEN}"}
    if provider == "taostats" and s.TAOSTATS_API_KEY:
        return {"Authorization": s.TAOSTATS_API_KEY}
    if provider == "twitterapi_io" and s.TWITTERAPI_IO_KEY:
        return {"X-API-Key": s.TWITTERAPI_IO_KEY}
    if provider == "official_v2" and s.X_BEARER_TOKEN:
        return {"Authorization": f"Bearer {s.X_BEARER_TOKEN}"}
    return {}


def _user_agent(provider: str) -> str:
    s = app_config.settings
    if provider in ("web", "wayback"):
        return s.WEB_USER_AGENT
    if provider == "discord":
        return s.DISCORD_USER_AGENT
    return s.HTTP_USER_AGENT


class Http:
    def __init__(self, limiter: ProviderLimiter, costs: "CostGovernor | None" = None) -> None:
        self.limiter = limiter
        self.costs = costs
        self._clients: dict[str, httpx.AsyncClient] = {}

    def client(self, provider: str) -> httpx.AsyncClient:
        c = self._clients.get(provider)
        if c is None or c.is_closed:
            headers = {"User-Agent": _user_agent(provider), **_auth_headers(provider)}
            c = httpx.AsyncClient(
                headers=headers, timeout=app_config.settings.HTTP_TIMEOUT_SEC, follow_redirects=False,
                limits=httpx.Limits(max_connections=8, max_keepalive_connections=4),
            )
            self._clients[provider] = c
        return c

    async def request(self, provider: str, method: str, url: str, *, params: Any = None, headers: Any = None,
                      json: Any = None, data: Any = None, content: Any = None, timeout: float | None = None,
                      retries: int | None = None, follow_redirects: bool = False,
                      ok_statuses: tuple[int, ...] = (200,), max_body_bytes: int | None = None) -> httpx.Response:
        s = app_config.settings
        parts = urlsplit(url)
        host = parts.hostname or ""
        loopback = host in _LOOPBACK
        lim = self.limiter.limits.get(provider) if not loopback else None
        if not loopback:
            self.limiter._lim(provider)          # unknown provider → KeyError (fail loud)
        max_retries = s.HTTP_MAX_RETRIES if retries is None else retries
        cap = s.HTTP_MAX_BODY_BYTES if max_body_bytes is None else max_body_bytes
        client = self.client(provider)
        attempt = 0
        last_exc: BaseException | None = None
        last_status: int | None = None
        while True:
            attempt += 1
            if not loopback:
                await self.limiter.acquire(provider, host=host)
            t0 = time.monotonic()
            try:
                req = client.build_request(method, url, params=params, headers=headers, json=json, data=data,
                                           content=content, timeout=timeout if timeout is not None else httpx.USE_CLIENT_DEFAULT)
                if not loopback:
                    self.limiter.count(provider)
                    if self.costs is not None and (lim is None or lim.book_spend):
                        await self.costs.record(provider, 0.0, requests=1)
                resp = await client.send(req, stream=True, follow_redirects=follow_redirects)
                try:
                    body = bytearray()
                    truncated = False
                    async for chunk in resp.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > cap:
                            truncated = True
                            del body[cap:]
                            break
                finally:
                    await resp.aclose()
                hdrs = [(k, v) for k, v in resp.headers.multi_items()
                        if k.lower() not in ("content-encoding", "content-length", "transfer-encoding")]
                full = httpx.Response(resp.status_code, headers=hdrs, content=bytes(body), request=req,
                                      extensions={**resp.extensions, "truncated": truncated})
                if truncated:
                    logger.warning("http_body_truncated", data={"provider": provider, "host": host,
                                                                 "path": redact_path(parts.path), "cap": cap})
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_exc = exc
                last_status = None
                logger.debug("http", data={"provider": provider, "method": method, "host": host,
                                           "path": redact_path(parts.path), "status": None,
                                           "ms": int((time.monotonic() - t0) * 1000), "attempt": attempt,
                                           "error": type(exc).__name__})
                if attempt > max_retries:
                    raise ProviderError(provider, f"{type(exc).__name__}: {exc}", status=None, url=url,
                                        attempts=attempt) from exc
                await asyncio.sleep(4 ** (attempt - 1))
                continue
            status = full.status_code
            logger.debug("http", data={"provider": provider, "method": method, "host": host,
                                       "path": redact_path(parts.path), "status": status,
                                       "ms": int((time.monotonic() - t0) * 1000), "attempt": attempt})
            if not loopback:
                self.limiter.observe(provider, full.headers, host=host)
            if status in ok_statuses:
                return full
            if status == 429:
                last_status = status
                last_exc = httpx.HTTPStatusError(f"429 from {host}", request=req, response=full)
                if "retry-after" not in {k.lower() for k in full.headers}:
                    self.limiter.penalize_host(provider, host, 60.0)
                if attempt > max_retries:
                    raise ProviderError(provider, "429 Too Many Requests", status=429, url=url,
                                        attempts=attempt) from last_exc
                continue
            if 500 <= status < 600:
                last_status = status
                last_exc = httpx.HTTPStatusError(f"{status} from {host}", request=req, response=full)
                if attempt > max_retries:
                    raise ProviderError(provider, f"HTTP {status}", status=status, url=url,
                                        attempts=attempt) from last_exc
                await asyncio.sleep(4 ** (attempt - 1))
                continue
            return full

    async def aclose(self) -> None:
        clients = list(self._clients.values())
        self._clients.clear()
        for c in clients:
            try:
                await c.aclose()
            except Exception:  # pragma: no cover
                pass


_fallback: Http | None = None


def get_http() -> Http:
    """The runtime's instance, else a lazily built process-local one (CLI, fallbacks)."""
    global _fallback
    try:
        from app.runtime import runtime
        if runtime is not None and runtime.http is not None:
            return runtime.http
    except Exception:  # pragma: no cover
        pass
    if _fallback is None:
        _fallback = Http(ProviderLimiter.from_settings())
    return _fallback
