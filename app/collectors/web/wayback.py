"""archive.org clients for 07 (spec 07 §2.6): CDX first capture and optional SPN2. Provider ``wayback``.

No Availability fallback: verified 2026-10-17 it answers ``{"archived_snapshots": {}}`` for archived URLs.
"""
from __future__ import annotations

import json
from calendar import timegm
from time import strptime
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from app.collectors.web.classify import registrable_domain
from app.collectors.web.fetch import ALL_STATUSES

if TYPE_CHECKING:  # pragma: no cover
    from app.http import Http

CDX_URL = "https://web.archive.org/cdx/search/cdx"
SPN2_URL = "https://web.archive.org/save"
BACKOFF_MIN_S = 3600


def wayback_target(url: str, shared_platform_hosts: list[str]) -> str:
    """Root ``{registrable_domain}/`` (domain age), or host + path on shared platforms (``proj.vercel.app/``)."""
    p = urlsplit(url)
    host = (p.hostname or "").lower()
    rd = registrable_domain(host)
    if rd in set(shared_platform_hosts):
        path = p.path or "/"
        return f"{host}{path}"
    return f"{rd}/"


def cdx_ts(s: str) -> int:
    return timegm(strptime(s[:14].ljust(14, "0"), "%Y%m%d%H%M%S"))


class WaybackClient:
    def __init__(self, http: "Http") -> None:
        self.http = http
        self.calls = 0
        self.retry_after: int | None = None

    def _retry_after(self, resp) -> int:
        ra = resp.headers.get("retry-after")
        try:
            return max(BACKOFF_MIN_S, int(float(ra))) if ra else BACKOFF_MIN_S
        except ValueError:
            return BACKOFF_MIN_S

    async def first_capture(self, target: str) -> tuple[int | None, str]:
        """(unix_ts | None, 'found' | 'none' | 'error' | 'backoff'). Never raises."""
        self.retry_after = None
        for filt in (True, False):
            params = {"url": target, "output": "json", "fl": "timestamp,original,statuscode", "limit": "1"}
            if filt:
                params["filter"] = "statuscode:200"
            self.calls += 1
            try:
                resp = await self.http.request("wayback", "GET", CDX_URL, params=params, timeout=30.0, retries=0,
                                               ok_statuses=ALL_STATUSES)
            except Exception:
                return None, "error"
            if resp.status_code in (429, 503):
                self.retry_after = self._retry_after(resp)
                return None, "backoff"
            if resp.status_code != 200:
                return None, "error"
            try:
                data = json.loads(resp.content or b"[]")
            except ValueError:
                return None, "error"
            if not isinstance(data, list):
                return None, "error"
            if len(data) >= 2 and isinstance(data[1], list) and data[1]:
                try:
                    return cdx_ts(str(data[1][0])), "found"
                except (ValueError, TypeError):
                    return None, "error"
            if len(data) > 2 or (len(data) == 1 and not isinstance(data[0], list)):
                return None, "error"
        return None, "none"

    async def spn2_save(self, url: str, access_key: str, secret_key: str) -> str | None:
        """Save Page Now 2 request; returns the job_id (no polling in v1). Never raises."""
        self.calls += 1
        try:
            resp = await self.http.request(
                "wayback", "POST", SPN2_URL, timeout=60.0, retries=0, ok_statuses=ALL_STATUSES,
                headers={"Accept": "application/json", "Authorization": f"LOW {access_key}:{secret_key}"},
                data={"url": url, "if_not_archived_within": "7d", "skip_first_archive": "1"})
        except Exception:
            return None
        if resp.status_code in (429, 503):
            self.retry_after = self._retry_after(resp)
            return None
        try:
            body = json.loads(resp.content or b"{}")
        except ValueError:
            return None
        jid = body.get("job_id") if isinstance(body, dict) else None
        return str(jid) if jid else None
