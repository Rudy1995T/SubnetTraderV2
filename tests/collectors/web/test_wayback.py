"""Spec 07 §2.6: CDX first capture (no Availability fallback), target selection, SPN2."""
from __future__ import annotations

import httpx
import pytest
import respx

from app.collectors.web.wayback import CDX_URL, SPN2_URL, WaybackClient, cdx_ts, wayback_target
from app.http import Http
from tests.collectors.web.conftest import fast_limiter, web_settings

SHARED = web_settings().WEB_SHARED_PLATFORM_HOSTS


def client():
    lm = fast_limiter()
    return WaybackClient(Http(lm)), lm


def test_targets():
    assert wayback_target("https://www.macrocosmos.ai/apex", SHARED) == "macrocosmos.ai/"
    assert wayback_target("https://proj.vercel.app", SHARED) == "proj.vercel.app/"
    assert wayback_target("https://medium.com/@proj", SHARED) == "medium.com/@proj"


def test_cdx_ts():
    assert cdx_ts("20240416113415") == 1713267255


@pytest.mark.asyncio
@respx.mock
async def test_first_capture_found():
    route = respx.get(CDX_URL).mock(return_value=httpx.Response(200, json=[
        ["timestamp", "original", "statuscode"], ["20240416113415", "https://www.macrocosmos.ai/", "200"]]))
    wb, lm = client()
    assert await wb.first_capture("macrocosmos.ai/") == (1713267255, "found")
    p = route.calls[0].request.url.params
    assert p["url"] == "macrocosmos.ai/" and p["filter"] == "statuscode:200" and p["limit"] == "1"
    assert lm._hour_used("wayback") == 1 and lm._hour_used("web") == 0
    await wb.http.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_first_capture_none_retries_unfiltered_only():
    route = respx.get(CDX_URL).mock(return_value=httpx.Response(200, json=[]))
    wb, _ = client()
    assert await wb.first_capture("new.example/") == (None, "none")
    assert len(route.calls) == 2 and "filter" not in route.calls[1].request.url.params
    await wb.http.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_first_capture_errors_and_backoff():
    wb, _ = client()
    respx.get(CDX_URL).mock(return_value=httpx.Response(502))
    assert await wb.first_capture("x.example/") == (None, "error")
    respx.get(CDX_URL).mock(return_value=httpx.Response(200, content=b"<html>oops</html>"))
    assert await wb.first_capture("x.example/") == (None, "error")
    respx.get(CDX_URL).mock(side_effect=httpx.ReadTimeout("t"))
    assert await wb.first_capture("x.example/") == (None, "error")
    respx.get(CDX_URL).mock(return_value=httpx.Response(429, headers={"retry-after": "120"}))
    assert await wb.first_capture("x.example/") == (None, "backoff")
    assert wb.retry_after == 3600
    avail = respx.get(url__startswith="https://archive.org/wayback/available")
    assert not avail.called
    await wb.http.aclose()


@pytest.mark.asyncio
@respx.mock
async def test_spn2_save():
    route = respx.post(SPN2_URL).mock(return_value=httpx.Response(200, json={"url": "https://a.example", "job_id": "j1"}))
    wb, _ = client()
    assert await wb.spn2_save("https://a.example", "k", "s") == "j1"
    req = route.calls[0].request
    assert req.headers["authorization"] == "LOW k:s" and b"url=https%3A%2F%2Fa.example" in req.content
    await wb.http.aclose()
