"""I/O for 07: DNS, TLS inspection, the manual GET/HEAD redirect chain and the network preflight (spec 07 §2.3
steps 2–4, §7). Everything returns observations; nothing here raises into the job."""
from __future__ import annotations

import asyncio
import socket
import ssl
from dataclasses import dataclass, field
from datetime import timezone
from typing import TYPE_CHECKING, Literal
from urllib.parse import urljoin, urlsplit

import httpx

from app.collectors.web.classify import REDIRECT_STATUSES, is_ip_literal, is_public_ip

if TYPE_CHECKING:  # pragma: no cover
    from app.http import Http
    from app.ratelimit import ProviderLimiter

ALL_STATUSES = tuple(range(100, 600))
ExcKind = Literal["dns", "timeout", "tls", "conn", "too_many_redirects", "protocol", "internal"]
VERIFY_EXPIRED = 10            # X509_V_ERR_CERT_HAS_EXPIRED
VERIFY_HOSTNAME = 62           # X509_V_ERR_HOSTNAME_MISMATCH


@dataclass(frozen=True)
class Hop:
    url: str
    status: int
    location: str | None


@dataclass(frozen=True)
class TlsInfo:
    days_left: int | None = None
    not_after: int | None = None           # unix s (B1)
    issuer: str | None = None
    hostname_match: bool | None = None
    verified: bool | None = None           # the certifi-verified handshake succeeded
    reason: str | None = None              # expiring | expired | hostname_mismatch | untrusted | tls_fail | no_answer
    error: str | None = None
    handshakes: int = 0


@dataclass
class FetchResult:
    url: str
    method: str = "GET"
    final_url: str | None = None
    final_host: str | None = None
    status: int | None = None
    chain: list[Hop] = field(default_factory=list)
    latency_ms: int | None = None
    headers: dict[str, str] = field(default_factory=dict)
    content_type: str | None = None
    body: bytes | None = None
    truncated: bool = False
    exc_kind: ExcKind | None = None
    error: str | None = None
    resolved_ip: str | None = None

    @property
    def hops(self) -> int:
        return len(self.chain)

    @property
    def text(self) -> str | None:
        if self.body is None:
            return None
        enc = "utf-8"
        ct = self.headers.get("content-type", "")
        if "charset=" in ct.lower():
            enc = ct.lower().split("charset=", 1)[1].split(";")[0].strip().strip('"') or "utf-8"
        try:
            return self.body.decode(enc, errors="replace")
        except LookupError:
            return self.body.decode("utf-8", errors="replace")

    @property
    def is_html(self) -> bool:
        ct = (self.content_type or "").lower()
        if ct:
            return "html" in ct or ct in ("text/plain",)
        head = (self.body or b"")[:512].lower()
        return b"<html" in head or b"<!doctype html" in head


# ── DNS ──────────────────────────────────────────────────────────────────────────────────────────────────────
class DnsError(Exception):
    def __init__(self, kind: Literal["nxdomain", "temporary", "private"], msg: str) -> None:
        super().__init__(msg)
        self.kind = kind


_NXDOMAIN = {getattr(socket, n) for n in ("EAI_NONAME", "EAI_NODATA") if hasattr(socket, n)}


async def resolve_dns(host: str, port: int, timeout_s: float) -> str:
    """First address of ``host``. Raises ``DnsError`` (nxdomain | temporary | private) or ``TimeoutError``."""
    loop = asyncio.get_running_loop()
    try:
        async with asyncio.timeout(timeout_s):
            infos = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise DnsError("nxdomain" if exc.errno in _NXDOMAIN else "temporary", f"gaierror {exc.errno}: {exc}") from exc
    if not infos:
        raise DnsError("nxdomain", "no addresses")
    ip = infos[0][4][0]
    if not is_public_ip(ip):
        raise DnsError("private", f"resolves to non-public address {ip}")
    return ip


# ── TLS ──────────────────────────────────────────────────────────────────────────────────────────────────────
def _handshake(host: str, port: int, timeout: float, verify: bool) -> bytes:
    """One TLS handshake; returns the peer certificate (DER). Blocking: run in a thread."""
    if verify:
        import certifi
        ctx = ssl.create_default_context(cafile=certifi.where())
    else:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    with socket.create_connection((host, port), timeout=timeout) as sock:
        with ctx.wrap_socket(sock, server_hostname=host) as ss:
            der = ss.getpeercert(binary_form=True)
    if not der:
        raise ssl.SSLError("no peer certificate")
    return der


def _idna(name: str) -> str:
    name = name.strip().lower().rstrip(".")
    try:
        return name.encode("idna").decode("ascii")
    except UnicodeError:
        return name


def hostname_matches(host: str, san_dns_names: list[str]) -> bool:
    """Exact SAN match, or a one-label wildcard (``*.example.com`` ↔ ``sub.example.com`` only)."""
    h = _idna(host)
    for raw in san_dns_names:
        n = _idna(raw)
        if n == h:
            return True
        if n.startswith("*.") and "*" not in n[2:]:
            suffix = n[2:]
            if h.endswith("." + suffix) and "." not in h[: -len(suffix) - 1] and h[: -len(suffix) - 1]:
                return True
    return False


def parse_cert(der: bytes) -> tuple[int | None, str | None, list[str]]:
    """(not_after unix s, issuer O|CN, SAN DNS names)."""
    from cryptography import x509
    from cryptography.x509.oid import ExtensionOID, NameOID
    cert = x509.load_der_x509_certificate(der)
    na = getattr(cert, "not_valid_after_utc", None)
    if na is None:  # pragma: no cover - cryptography < 42
        na = cert.not_valid_after.replace(tzinfo=timezone.utc)
    issuer = None
    for oid in (NameOID.ORGANIZATION_NAME, NameOID.COMMON_NAME):
        attrs = cert.issuer.get_attributes_for_oid(oid)
        if attrs:
            issuer = str(attrs[0].value)[:200]
            break
    try:
        san = cert.extensions.get_extension_for_oid(ExtensionOID.SUBJECT_ALTERNATIVE_NAME).value
        names = san.get_values_for_type(x509.DNSName)
    except x509.ExtensionNotFound:
        names = []
    return int(na.timestamp()), issuer, list(names)


async def inspect_tls(limiter: "ProviderLimiter | None", host: str, port: int, timeout_s: float, *, now: int,
                      warn_days: int = 14) -> TlsInfo:
    """§2.3 step 3: verified handshake, then (only after a verification error) an unverified one to read the cert.
    Each handshake takes one ``web`` token. Never raises."""
    n = 0

    async def shake(verify: bool) -> bytes:
        nonlocal n
        if limiter is not None:
            await limiter.acquire("web", host=host)
            limiter.count("web")
        n += 1
        return await asyncio.to_thread(_handshake, host, port, timeout_s, verify)

    verified = False
    vcode: int | None = None
    try:
        der = await shake(True)
        verified = True
    except ssl.SSLCertVerificationError as exc:
        vcode = getattr(exc, "verify_code", None)
        try:
            der = await shake(False)
        except Exception as exc2:
            return TlsInfo(verified=False, reason="untrusted", error=f"{type(exc2).__name__}: {exc2}"[:200],
                           handshakes=n)
    except ssl.SSLError as exc:
        return TlsInfo(verified=False, reason="tls_fail", error=f"{type(exc).__name__}: {exc}"[:200], handshakes=n)
    except (OSError, asyncio.TimeoutError) as exc:      # refused / reset / unreachable / timeout
        return TlsInfo(reason="no_answer", error=f"{type(exc).__name__}: {exc}"[:200], handshakes=n)
    except Exception as exc:  # pragma: no cover - defensive
        return TlsInfo(reason="tls_fail", error=f"internal:{type(exc).__name__}: {exc}"[:200], handshakes=n)
    try:
        not_after, issuer, sans = parse_cert(der)
    except Exception as exc:
        return TlsInfo(verified=verified, reason="tls_fail", error=f"cert_parse: {exc}"[:200], handshakes=n)
    days_left = (not_after - now) // 86400
    match = hostname_matches(host, sans)
    if verified:
        reason = ("expired" if days_left <= 0 else "expiring") if days_left <= warn_days else None
    elif vcode == VERIFY_EXPIRED or days_left < 0:
        reason = "expired"
    elif vcode == VERIFY_HOSTNAME or not match:
        reason = "hostname_mismatch"
    else:
        reason = "untrusted"
    return TlsInfo(days_left=int(days_left), not_after=not_after, issuer=issuer, hostname_match=match,
                   verified=verified, reason=reason, handshakes=n)


# ── GET / HEAD chain ─────────────────────────────────────────────────────────────────────────────────────────
def _chain_has(exc: BaseException | None, types: tuple[type, ...]) -> BaseException | None:
    """First exception of ``types`` reachable through ``__cause__`` / ``__context__`` (both are followed)."""
    todo, seen = [exc], set()
    while todo and len(seen) < 20:
        e = todo.pop(0)
        if e is None or id(e) in seen:
            continue
        seen.add(id(e))
        if isinstance(e, types):
            return e
        todo += [e.__cause__, e.__context__]
    return None


def exc_kind_of(exc: BaseException) -> ExcKind:
    """Classify a transport failure from 01's chained ``ProviderError`` (§2.4 rows 1–4)."""
    if _chain_has(exc, (httpx.TimeoutException, asyncio.TimeoutError, socket.timeout)):
        return "timeout"
    if _chain_has(exc, (ssl.SSLError,)):
        return "tls"
    gai = _chain_has(exc, (socket.gaierror,))
    if gai is not None:
        return "dns" if getattr(gai, "errno", None) in _NXDOMAIN else "timeout"
    if _chain_has(exc, (httpx.ConnectError,)):
        return "conn"
    return "protocol"


def _blocked_target(url: str) -> bool:
    h = (urlsplit(url).hostname or "").lower()
    if not h or h == "localhost" or h.endswith(".localhost") or h.endswith(".local"):
        return True
    return is_ip_literal(h) and not is_public_ip(h.strip("[]"))


async def fetch_chain(http: "Http", url: str, *, method: str = "GET", max_redirects: int = 5,
                      max_body: int = 2_000_000, timeout: httpx.Timeout | float = 10.0,
                      headers: dict[str, str] | None = None) -> FetchResult:
    """Manual redirect loop (≤ ``max_redirects`` followed hops) on provider ``web``. Never raises."""
    from app.http import ProviderError
    fr = FetchResult(url=url, method=method)
    hdrs = {"Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.5", "Accept-Language": "en", **(headers or {})}
    cur, meth = url, method
    for i in range(max_redirects + 1):
        try:
            resp = await http.request("web", meth, cur, headers=hdrs, timeout=timeout, retries=0,
                                      ok_statuses=ALL_STATUSES, max_body_bytes=max_body)
        except ProviderError as exc:
            fr.exc_kind = exc_kind_of(exc)
            fr.error = str(exc)[:300]
            fr.final_url = cur
            fr.final_host = (urlsplit(cur).hostname or "").lower() or None
            return fr
        except httpx.DecodingError as exc:
            fr.exc_kind, fr.error = "protocol", f"DecodingError: {exc}"[:300]
            return fr
        except Exception as exc:
            fr.exc_kind, fr.error = "internal", f"internal:{type(exc).__name__}: {exc}"[:300]
            return fr
        if i == 0:
            fr.latency_ms = resp.extensions.get("elapsed_ms")
        status = resp.status_code
        loc = resp.headers.get("location")
        if status in REDIRECT_STATUSES and loc:
            try:
                nxt = urljoin(cur, loc.strip())
            except ValueError:
                nxt = None
            if nxt and urlsplit(nxt).scheme in ("http", "https") and not _blocked_target(nxt):
                if i == max_redirects:
                    fr.exc_kind = "too_many_redirects"
                    fr.status, fr.final_url = status, cur
                    fr.final_host = (urlsplit(cur).hostname or "").lower() or None
                    return fr
                fr.chain.append(Hop(cur, status, nxt))
                if status == 303 and meth != "HEAD":
                    meth = "GET"
                cur = nxt
                continue
            if nxt and _blocked_target(nxt):
                fr.error = "redirect_blocked: non-public target"
        fr.status = status
        fr.final_url = cur
        fr.final_host = (urlsplit(cur).hostname or "").lower() or None
        fr.headers = {k.lower(): v for k, v in resp.headers.items()}
        fr.content_type = (resp.headers.get("content-type") or "").split(";")[0].strip().lower() or None
        fr.body = resp.content if meth != "HEAD" else None
        fr.truncated = bool(resp.extensions.get("truncated"))
        if status in REDIRECT_STATUSES and not loc:
            fr.error = fr.error or "redirect_without_location"
        return fr
    return fr  # pragma: no cover - loop always returns


# ── network preflight (§7) ───────────────────────────────────────────────────────────────────────────────────
async def network_preflight(hosts: list[str], timeout_s: float) -> bool:
    """DNS + TCP connect to 443 of the probe hosts (no HTTP request); True when any succeeds."""
    for h in hosts:
        try:
            async with asyncio.timeout(timeout_s):
                _, w = await asyncio.open_connection(h, 443)
            w.close()
            try:
                await w.wait_closed()
            except Exception:
                pass
            return True
        except Exception:
            continue
    return False
