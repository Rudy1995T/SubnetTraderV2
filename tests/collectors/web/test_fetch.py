"""Spec 07 §2.3 steps 1–4: robots, DNS, TLS (generated certs), the manual redirect chain, body cap."""
from __future__ import annotations

import asyncio
import datetime as dt
import socket
import ssl

import httpx
import pytest
import respx

from app.collectors.web import fetch as F
from app.collectors.web.robots import RobotsCache
from app.http import Http
from tests.collectors.web.conftest import fast_limiter

NOW = 1_792_195_800


def make_der(*, days_left: int, sans: list[str], org: str | None = "Let's Encrypt", cn: str = "R11") -> bytes:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID
    key = ec.generate_private_key(ec.SECP256R1())
    now = dt.datetime.fromtimestamp(NOW, dt.timezone.utc)
    issuer_attrs = [x509.NameAttribute(NameOID.COMMON_NAME, cn)]
    if org:
        issuer_attrs.insert(0, x509.NameAttribute(NameOID.ORGANIZATION_NAME, org))
    b = (x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, sans[0] if sans else "x")]))
         .issuer_name(x509.Name(issuer_attrs)).public_key(key.public_key()).serial_number(1)
         .not_valid_before(now - dt.timedelta(days=30)).not_valid_after(now + dt.timedelta(days=days_left, hours=1)))
    if sans:
        b = b.add_extension(x509.SubjectAlternativeName([x509.DNSName(s) for s in sans]), critical=False)
    return b.sign(key, hashes.SHA256()).public_bytes(serialization.Encoding.DER)


def lim():
    return fast_limiter()


@pytest.fixture
def handshake(monkeypatch):
    """``verified`` → DER or an exception for the verified handshake; ``raw`` → DER for the unverified one."""
    st = {"verified": None, "raw": None, "calls": []}

    def fake(host, port, timeout, verify):
        st["calls"].append(verify)
        v = st["verified"] if verify else st["raw"]
        if isinstance(v, BaseException):
            raise v
        return v
    monkeypatch.setattr(F, "_handshake", fake)
    return st


def test_hostname_matches():
    assert F.hostname_matches("example.com", ["example.com"])
    assert F.hostname_matches("sub.example.com", ["*.example.com"])
    assert not F.hostname_matches("a.b.example.com", ["*.example.com"])
    assert not F.hostname_matches("example.com", ["*.example.com"])
    assert not F.hostname_matches("badexample.com", ["example.com"])
    assert not F.hostname_matches("example.com", [])


@pytest.mark.asyncio
async def test_inspect_tls_verified(handshake):
    handshake["verified"] = make_der(days_left=400, sans=["site.example"])
    t = await F.inspect_tls(lim(), "site.example", 443, 5, now=NOW)
    assert t.verified and t.days_left == 400 and t.reason is None and t.issuer == "Let's Encrypt"
    assert t.hostname_match and t.handshakes == 1 and handshake["calls"] == [True]


@pytest.mark.asyncio
async def test_inspect_tls_expiring_and_issuer_cn(handshake):
    handshake["verified"] = make_der(days_left=10, sans=["site.example"], org=None, cn="ZeroSSL ECC")
    t = await F.inspect_tls(lim(), "site.example", 443, 5, now=NOW)
    assert t.days_left == 10 and t.reason == "expiring" and t.issuer == "ZeroSSL ECC"


@pytest.mark.asyncio
async def test_inspect_tls_expired(handshake):
    e = ssl.SSLCertVerificationError("certificate verify failed: certificate has expired")
    e.verify_code = F.VERIFY_EXPIRED
    handshake["verified"] = e
    handshake["raw"] = make_der(days_left=-5, sans=["site.example"])
    t = await F.inspect_tls(lim(), "site.example", 443, 5, now=NOW)
    assert not t.verified and t.days_left < 0 and t.reason == "expired" and t.handshakes == 2
    assert handshake["calls"] == [True, False]


@pytest.mark.asyncio
async def test_inspect_tls_mismatch_and_refused(handshake):
    e = ssl.SSLCertVerificationError("hostname mismatch")
    e.verify_code = F.VERIFY_HOSTNAME
    handshake["verified"] = e
    handshake["raw"] = make_der(days_left=90, sans=["other.example"])
    t = await F.inspect_tls(lim(), "site.example", 443, 5, now=NOW)
    assert t.reason == "hostname_mismatch" and t.hostname_match is False
    handshake["verified"] = ConnectionRefusedError(111, "refused")
    t = await F.inspect_tls(lim(), "site.example", 443, 5, now=NOW)
    assert t.reason == "no_answer" and t.days_left is None


@pytest.mark.asyncio
async def test_inspect_tls_takes_web_tokens(handshake):
    e = ssl.SSLCertVerificationError("x")
    e.verify_code = 20
    handshake["verified"] = e
    handshake["raw"] = make_der(days_left=90, sans=["site.example"])
    lm = lim()
    t = await F.inspect_tls(lm, "site.example", 443, 5, now=NOW)
    assert t.reason == "untrusted" and lm._hour_used("web") == 2


@pytest.mark.asyncio
async def test_resolve_dns(monkeypatch):
    loop = asyncio.get_running_loop()

    async def gai(host, port, type=0):
        if host == "gone.example":
            raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")
        if host == "flaky.example":
            raise socket.gaierror(socket.EAI_AGAIN, "Temporary failure")
        if host == "local.example":
            return [(2, 1, 6, "", ("127.0.0.1", port))]
        return [(2, 1, 6, "", ("93.184.216.34", port))]
    monkeypatch.setattr(loop, "getaddrinfo", gai)
    assert await F.resolve_dns("ok.example", 443, 1) == "93.184.216.34"
    with pytest.raises(F.DnsError) as ei:
        await F.resolve_dns("gone.example", 443, 1)
    assert ei.value.kind == "nxdomain"
    with pytest.raises(F.DnsError) as ei:
        await F.resolve_dns("flaky.example", 443, 1)
    assert ei.value.kind == "temporary"
    with pytest.raises(F.DnsError) as ei:
        await F.resolve_dns("local.example", 443, 1)
    assert ei.value.kind == "private"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_chain_redirects_and_headers():
    respx.get("http://site.example/").mock(return_value=httpx.Response(301, headers={"location": "https://site.example/"}))
    route = respx.get("https://site.example/").mock(return_value=httpx.Response(
        200, headers={"content-type": "text/html; charset=utf-8"}, content=b"<html><title>x</title></html>"))
    http = Http(lim())
    try:
        r = await F.fetch_chain(http, "http://site.example/")
    finally:
        await http.aclose()
    assert r.status == 200 and r.hops == 1 and r.chain[0].status == 301 and r.final_host == "site.example"
    assert r.content_type == "text/html" and r.is_html and r.latency_ms is not None
    sent = route.calls[0].request.headers
    assert "TaoSentinel/" in sent["user-agent"]
    assert "br" not in sent.get("accept-encoding", "")        # 07 never forces Accept-Encoding


@pytest.mark.asyncio
@respx.mock
async def test_fetch_chain_too_many_redirects():
    n = {"i": 0}

    def loop_handler(request):
        n["i"] += 1
        return httpx.Response(302, headers={"location": f"https://loop.example/?n={n['i']}"})
    respx.get(url__startswith="https://loop.example/").mock(side_effect=loop_handler)
    http = Http(lim())
    try:
        r = await F.fetch_chain(http, "https://loop.example/", max_redirects=5)
    finally:
        await http.aclose()
    assert r.exc_kind == "too_many_redirects" and r.hops == 5 and n["i"] == 6


@pytest.mark.asyncio
@respx.mock
async def test_fetch_chain_errors_classified():
    respx.get("https://slow.example/").mock(side_effect=httpx.ReadTimeout("read timed out"))
    respx.get("https://refused.example/").mock(side_effect=httpx.ConnectError("Connection refused"))
    respx.get("https://redir.example/").mock(return_value=httpx.Response(302, headers={"location": "http://127.0.0.1:8090/"}))
    http = Http(lim())
    try:
        assert (await F.fetch_chain(http, "https://slow.example/")).exc_kind == "timeout"
        assert (await F.fetch_chain(http, "https://refused.example/")).exc_kind == "conn"
        blocked = await F.fetch_chain(http, "https://redir.example/")
    finally:
        await http.aclose()
    assert blocked.status == 302 and blocked.hops == 0 and "redirect_blocked" in (blocked.error or "")


@pytest.mark.asyncio
@respx.mock
async def test_body_cap_and_raw_content():
    respx.get("https://big.example/").mock(return_value=httpx.Response(
        200, headers={"content-type": "application/pdf"}, content=b"%PDF" + b"x" * 5000))
    http = Http(lim())
    try:
        r = await F.fetch_chain(http, "https://big.example/", max_body=1000)
    finally:
        await http.aclose()
    assert r.truncated and len(r.body) == 1000 and not r.is_html


@pytest.mark.asyncio
@respx.mock
async def test_robots_bare_token():
    respx.get("https://shy.example/robots.txt").mock(return_value=httpx.Response(
        200, headers={"content-type": "text/plain"},
        content=b"User-agent: TaoSentinel\nDisallow: /\n\nUser-agent: *\nAllow: /\n"))
    respx.get("https://open.example/robots.txt").mock(return_value=httpx.Response(
        200, headers={"content-type": "text/plain"}, content=b"User-agent: OtherBot\nDisallow: /\n"))
    respx.get("https://down.example/robots.txt").mock(return_value=httpx.Response(503))
    http = Http(lim())
    rc = RobotsCache(http, 7)
    try:
        assert (await rc.allowed("https://shy.example/"))[0] is False
        assert (await rc.allowed("https://open.example/"))[0] is True
        assert (await rc.allowed("https://down.example/"))[0] is True
        await rc.allowed("https://shy.example/page")
    finally:
        await http.aclose()
    assert rc.fetches == 3                                      # cached per origin


def test_limiter_host_cooldown_and_no_bucket_penalty():
    lm = lim()
    lm.observe("web", {"retry-after": "999999"}, host="slow.example")
    assert 3500 < lm.host_cooldown("web", "slow.example") <= 3600      # capped for the multi-host provider
    assert lm.host_cooldown("web", "other.example") == 0.0
    lm.observe("web", {"x-ratelimit-remaining": "0", "x-ratelimit-reset": "600"}, host="x.example")
    assert lm._buckets["web"]._penalty_until == 0.0                    # no bucket-wide stall


class _TlsFailHttp:
    """respx replaces ``__cause__``; raise the chain httpcore/httpx really build (ssl → ConnectError → ProviderError)."""

    async def request(self, *a, **k):
        from app.http import ProviderError
        try:
            try:
                raise ssl.SSLCertVerificationError("certificate verify failed: certificate has expired")
            except ssl.SSLError as exc:
                raise httpx.ConnectError("ssl") from exc
        except httpx.ConnectError as exc:
            raise ProviderError("web", "ConnectError", status=None, url="https://badcert.example/", attempts=1) from exc


@pytest.mark.asyncio
async def test_fetch_chain_tls_failure_classified():
    assert (await F.fetch_chain(_TlsFailHttp(), "https://badcert.example/")).exc_kind == "tls"
