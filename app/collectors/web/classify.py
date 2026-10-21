"""Pure classification for 07: registrable domain, host exclusion, bot challenge, SPA, parked / hijack signals,
the §2.4 status table and the uptime outcome (spec 07 §2.4–§2.5)."""
from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Iterable, Literal, Mapping

if TYPE_CHECKING:  # pragma: no cover
    from app.collectors.web.fetch import FetchResult

UP_CLASSES = frozenset({"up_2xx", "redirect_ok"})
SOFT_CLASSES = frozenset({"timeout", "conn_error"})             # down only after a non-up previous check
Outcome = Literal["up", "down", "indeterminate"]

PARKED_WEIGHTS = {"parking_host": 3, "strong_phrase": 3, "weak_keywords_2": 2, "parking_script": 1,
                  "ad_template": 1, "tiny_body_links": 1}
HIJACK_WEIGHTS = {"cross_domain_new": 2, "scam_keywords_2": 2, "content_reset": 1, "issuer_and_ip_change": 1}
REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})


@dataclass(frozen=True, slots=True)
class WebHeuristicsConfig:
    parking_hosts: tuple[str, ...]
    strong_phrases: tuple[str, ...]
    weak_keywords: tuple[str, ...]
    hijack_keywords: tuple[str, ...]
    tiny_body_chars: int = 300
    content_reset_ratio: float = 0.2

    @classmethod
    def from_settings(cls, s) -> "WebHeuristicsConfig":
        return cls(tuple(s.WEB_PARKING_HOSTS), tuple(s.WEB_PARKED_STRONG_PHRASES), tuple(s.WEB_PARKED_WEAK_KEYWORDS),
                   tuple(s.WEB_HIJACK_KEYWORDS), int(s.WEB_TINY_BODY_CHARS), float(s.WEB_CONTENT_RESET_RATIO))


# ── hosts ────────────────────────────────────────────────────────────────────────────────────────────────────
def is_ip_literal(host: str) -> bool:
    try:
        ipaddress.ip_address(host.strip("[]"))
        return True
    except ValueError:
        return False


def is_public_ip(ip: str) -> bool:
    try:
        return ipaddress.ip_address(ip).is_global
    except ValueError:
        return False


@lru_cache(maxsize=4096)
def registrable_domain(host: str) -> str:
    """eTLD+1 from the offline PSL snapshot bundled with ``tld`` (ICANN section only: ``x.vercel.app`` →
    ``vercel.app``); "last two labels" when the suffix is unknown or the host is an IP literal."""
    h = (host or "").strip().lower().rstrip(".")
    if not h:
        return ""
    if not is_ip_literal(h):
        try:
            from tld import get_fld
            fld = get_fld(h, fix_protocol=True, search_private=False, fail_silently=True)
            if fld:
                return fld
        except Exception:  # pragma: no cover - import or table failure
            pass
    parts = h.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else h


def is_excluded_host(host: str, excluded: Iterable[str]) -> bool:
    h = (host or "").lower().rstrip(".")
    ex = set(excluded)
    return registrable_domain(h) in ex or h in ex


# ── text tests ───────────────────────────────────────────────────────────────────────────────────────────────
@lru_cache(maxsize=1024)
def _phrase_re(phrase: str) -> re.Pattern:
    return re.compile(r"(?<![a-z0-9])" + re.escape(phrase.lower()) + r"(?![a-z0-9])")


def phrases_in(text: str, phrases: Iterable[str]) -> list[str]:
    t = (text or "").lower()
    return [p for p in phrases if p and _phrase_re(p).search(t)]


_CHALLENGE_BODY = ("just a moment...", "_cf_chl_opt", "challenge-platform", "attention required! | cloudflare")
# vendor-neutral interstitials (WordPress.com/a8c "Checking your browser…", DDoS-Guard, generic JS walls); live 2026-10-17
_GENERIC_CHALLENGE = ("<title>checking your browser", "ddos-guard", "verify you are human",
                      "enable javascript and cookies to continue", "javascript required")


def is_bot_challenge(status: int | None, headers: Mapping[str, str], html: str | None) -> bool:
    if status not in (403, 429, 503):
        return False
    h = {k.lower(): str(v) for k, v in (headers or {}).items()}
    body = (html or "")[:200_000].lower()
    if h.get("cf-mitigated", "").lower() == "challenge":
        return True
    if "cloudflare" in h.get("server", "").lower() and any(m in body for m in _CHALLENGE_BODY):
        return True
    if "vercel security checkpoint" in body:
        return True
    if any(k.startswith("x-akamai") for k in h) and "access denied" in body and "reference #" in body:
        return True
    if "x-sucuri-id" in h:
        return True
    return any(m in body for m in _GENERIC_CHALLENGE)


_SPA_MARKERS = ('<div id="root"', "<div id='root'", '<div id="__next"', '<div id="app"', "<div id='app'",
                "data-reactroot", "__nuxt__", '<script type="module" src="/assets/index-', "_next/static")
_NOSCRIPT_JS = re.compile(r"<noscript[^>]*>[^<]{0,400}enable javascript", re.I)


def detect_spa(html: str | None, plain_len: int, tiny_body_chars: int = 300) -> bool:
    if not html or plain_len >= tiny_body_chars:
        return False
    low = html[:500_000].lower()
    return any(m in low for m in _SPA_MARKERS) or bool(_NOSCRIPT_JS.search(html[:500_000]))


def parked_signals(norm_text: str, title: str | None, final_host: str | None, links: list[str],
                   asset_hosts: list[str], is_spa: bool, content_len: int, cfg: WebHeuristicsConfig) -> list[str]:
    """§2.5.1; ``links`` = absolute ``<a href>`` URLs, ``asset_hosts`` = hosts of script/iframe src + meta generator."""
    from urllib.parse import urlsplit
    sig: list[str] = []
    parking = set(cfg.parking_hosts)
    final_rd = registrable_domain(final_host or "")
    if final_rd and final_rd in parking:
        sig.append("parking_host")
    head = f"{(title or '').lower()} {norm_text[:600]}"
    if phrases_in(head, cfg.strong_phrases):
        sig.append("strong_phrase")
    if len(set(phrases_in(norm_text, cfg.weak_keywords))) >= 2:
        sig.append("weak_keywords_2")
    if any(registrable_domain(h) in parking for h in asset_hosts if h):
        sig.append("parking_script")
    t = norm_text
    if "related searches" in t or "sponsored listings" in t or ("privacy policy" in t and "related links" in t):
        sig.append("ad_template")
    if content_len < cfg.tiny_body_chars and not is_spa:
        ext = {registrable_domain(urlsplit(u).hostname or "") for u in links}
        ext.discard("")
        ext.discard(final_rd)
        if ext:
            sig.append("tiny_body_links")
    return sig


def score(signals: Iterable[str], weights: Mapping[str, int]) -> int:
    return sum(weights.get(s, 0) for s in signals)


def hijack_signals(norm_text: str, prev_text: str | None, *, cross_domain: bool, final_is_parking: bool,
                   prev_same_domain_up: bool, change_ratio: float | None, resolved_ip: str | None,
                   tls_issuer: str | None, prev_ip: str | None, prev_issuer: str | None,
                   cfg: WebHeuristicsConfig) -> list[str]:
    """§2.5.2. Scam keywords count only when **new** versus the previous stored text, so a legitimate prediction-
    market or dapp site that always says "betting"/"airdrop" does not score."""
    sig: list[str] = []
    if cross_domain and not final_is_parking and prev_same_domain_up:
        sig.append("cross_domain_new")
    now_kw = set(phrases_in(norm_text, cfg.hijack_keywords))
    if prev_text:
        now_kw -= set(phrases_in(prev_text, cfg.hijack_keywords))
    if len(now_kw) >= 2:
        sig.append("scam_keywords_2")
    if change_ratio is not None and change_ratio < cfg.content_reset_ratio:
        sig.append("content_reset")
    if resolved_ip and tls_issuer and prev_ip and prev_issuer and resolved_ip != prev_ip and tls_issuer != prev_issuer:
        sig.append("issuer_and_ip_change")
    return sig


# ── §2.4 status table ────────────────────────────────────────────────────────────────────────────────────────
def classify(fr: "FetchResult", *, parked: bool, hijack_score: int, bot_challenge: bool, head_only: bool) -> str:
    k = fr.exc_kind
    if k == "dns":
        return "dns_fail"
    if k == "timeout":
        return "timeout"
    if k == "tls":
        return "tls_fail"
    if k in ("conn", "protocol", "internal"):
        return "conn_error"
    if k == "too_many_redirects":
        return "too_many_redirects"
    s = fr.status
    if s is None:
        return "conn_error"
    if s == 429 or (s in (403, 503) and bot_challenge) or (head_only and s in (405, 501)):
        return "bot_challenge"
    if 200 <= s < 300:
        if hijack_score >= 3:
            return "hijacked_suspect"
        if parked:
            return "parked"
        return "up_2xx" if fr.hops == 0 else "redirect_ok"
    if 400 <= s < 500:
        return "client_error"
    return "server_error"                                   # 5xx, 1xx, 3xx without a followable Location


# ── outcomes (§2.4 footnote) ─────────────────────────────────────────────────────────────────────────────────
def always_indeterminate(status_class: str, error: str | None) -> bool:
    e = error or ""
    return status_class == "bot_challenge" or e.startswith("internal:") or e.startswith("network_suspect")


def outcome(status_class: str, error: str | None, prev_up: bool | None) -> Outcome:
    """``prev_up`` = the previous considered row's "was up" (None = no previous considered row)."""
    if always_indeterminate(status_class, error):
        return "indeterminate"
    if status_class in UP_CLASSES:
        return "up"
    if status_class in SOFT_CLASSES:
        return "down" if prev_up is False else "indeterminate"
    return "down"


def outcomes(rows: list[Mapping]) -> list[Outcome]:
    """Outcomes of one URL's rows in ``event_at`` order; skipped rows do not move the "previous" pointer."""
    out: list[Outcome] = []
    prev_up: bool | None = None
    for r in rows:
        sc, err = r["status_class"], r.get("error")
        out.append(outcome(sc, err, prev_up))
        if not always_indeterminate(sc, err):
            prev_up = sc in UP_CLASSES
    return out
