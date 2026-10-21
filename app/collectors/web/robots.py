"""robots.txt cache for 07 (spec 07 §2.3 step 1): per origin, in memory, ``WEB_ROBOTS_CACHE_DAYS``.

``can_fetch`` gets the bare token ``TaoSentinel``: ``urllib.robotparser`` compares a group's agent with the part of the
given UA before the first ``/``, so the browser-like ``WEB_USER_AGENT`` would match ``Mozilla``. Unreachable or
non-200 robots ⇒ allowed.
"""
from __future__ import annotations

import time
from typing import TYPE_CHECKING
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

from app.collectors.web.fetch import ALL_STATUSES

if TYPE_CHECKING:  # pragma: no cover
    from app.http import Http

ROBOTS_MAX_BYTES = 65_536
MAX_CRAWL_DELAY_S = 30.0


def parse_robots(text: str) -> RobotFileParser:
    rp = RobotFileParser()
    rp.parse(text.splitlines())
    rp.modified()
    return rp


class RobotsCache:
    def __init__(self, http: "Http", ttl_days: int, ua_token: str = "TaoSentinel",
                 clock=time.monotonic) -> None:
        self.http = http
        self.ttl = float(ttl_days) * 86400.0
        self.token = ua_token
        self.clock = clock
        self._cache: dict[str, tuple[float, RobotFileParser | None]] = {}
        self.fetches = 0

    async def _load(self, origin: str) -> RobotFileParser | None:
        self.fetches += 1
        try:
            resp = await self.http.request("web", "GET", f"{origin}/robots.txt", timeout=5.0, retries=0,
                                           ok_statuses=ALL_STATUSES, follow_redirects=True,
                                           max_body_bytes=ROBOTS_MAX_BYTES)
        except Exception:
            return None
        if resp.status_code != 200:
            return None
        ct = (resp.headers.get("content-type") or "").lower()
        if "html" in ct:                      # a soft-404 page, not a robots file
            return None
        try:
            return parse_robots(resp.content.decode("utf-8", errors="replace"))
        except Exception:
            return None

    async def allowed(self, url: str) -> tuple[bool, float | None]:
        """(allowed, crawl_delay_s ≤ 30). Never raises."""
        p = urlsplit(url)
        origin = f"{p.scheme}://{p.netloc}"
        hit = self._cache.get(origin)
        if hit is None or self.clock() - hit[0] > self.ttl:
            hit = (self.clock(), await self._load(origin))
            self._cache[origin] = hit
        rp = hit[1]
        if rp is None:
            return True, None
        try:
            ok = rp.can_fetch(self.token, url)
            delay = rp.crawl_delay(self.token)
        except Exception:
            return True, None
        d = None
        if delay is not None:
            try:
                d = min(float(delay), MAX_CRAWL_DELAY_S)
            except (TypeError, ValueError):
                d = None
        return bool(ok), d
