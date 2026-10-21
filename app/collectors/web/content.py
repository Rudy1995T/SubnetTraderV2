"""Pure content handling for 07: extraction chain, normalisation + hash, word-token diff, link parsing and docs-link
discovery (spec 07 §2.3 steps 6–8, 10)."""
from __future__ import annotations

import difflib
import hashlib
import logging
import re
import unicodedata
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Iterable
from urllib.parse import urljoin, urlsplit

MIN_EXTRACT_CHARS = 200
_DROP_TAGS = ("script", "style", "noscript", "svg", "template")

for _name in ("trafilatura", "trafilatura.core", "trafilatura.main_extractor", "readability", "htmldate", "courlan"):
    logging.getLogger(_name).setLevel(logging.ERROR)


# ── head metadata + links (stdlib parser: tolerant, no lxml tree needed) ─────────────────────────────────────
@dataclass
class PageInfo:
    title: str | None = None
    description: str | None = None
    links: list[tuple[str, str]] = field(default_factory=list)     # (absolute href, anchor text)
    asset_hosts: list[str] = field(default_factory=list)            # script/iframe src hosts, meta generator


class _Collector(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base = base_url
        self.info = PageInfo()
        self._in_title = False
        self._title: list[str] = []
        self._a_href: str | None = None
        self._a_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        a = {k.lower(): (v or "") for k, v in attrs}
        if tag == "title":
            self._in_title = True
        elif tag == "meta":
            name = (a.get("name") or a.get("property") or "").lower()
            if name in ("description", "og:description") and not self.info.description:
                self.info.description = a.get("content", "").strip() or None
            if name == "generator" and a.get("content"):
                self.info.asset_hosts.append(a["content"].strip().lower())
        elif tag in ("script", "iframe") and a.get("src"):
            h = urlsplit(urljoin(self.base, a["src"])).hostname
            if h:
                self.info.asset_hosts.append(h.lower())
        elif tag == "a":
            self._finish_a()
            self._a_href = a.get("href")
            self._a_text = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False
        elif tag == "a":
            self._finish_a()

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self._title.append(data)
        if self._a_href is not None:
            self._a_text.append(data)

    def _finish_a(self) -> None:
        if self._a_href is not None:
            href = self._a_href.strip()
            if href and not href.lower().startswith(("mailto:", "javascript:", "tel:", "data:")):
                try:
                    absu = urljoin(self.base, href)
                except ValueError:
                    absu = None
                if absu and urlsplit(absu).scheme in ("http", "https"):
                    self.info.links.append((absu, " ".join("".join(self._a_text).split())))
        self._a_href = None
        self._a_text = []

    def close(self) -> None:
        super().close()
        self._finish_a()
        t = " ".join("".join(self._title).split())
        self.info.title = t or None


def page_info(html: str, base_url: str) -> PageInfo:
    p = _Collector(base_url)
    try:
        p.feed(html[:3_000_000])
        p.close()
    except Exception:  # pragma: no cover - html.parser is very tolerant
        pass
    return p.info


def outbound_links(html: str, base_url: str) -> list[str]:
    return [u for u, _ in page_info(html, base_url).links]


# ── extraction ───────────────────────────────────────────────────────────────────────────────────────────────
def plain_text(html: str) -> str:
    try:
        import lxml.html
        root = lxml.html.fromstring(html)
        for bad in root.xpath("|".join(f"//{t}" for t in _DROP_TAGS)):
            bad.drop_tree()
        return " ".join(root.text_content().split())
    except Exception:
        return ""


def _readability(html: str) -> str:
    try:
        from trafilatura.readability_lxml import Document
    except Exception:  # module dropped by a future trafilatura
        return ""
    try:
        import lxml.html
        summary = Document(html).summary()
        return " ".join(lxml.html.fromstring(summary).text_content().split()) if summary else ""
    except Exception:
        return ""


def extract_text(html: str, url: str) -> tuple[str, str]:
    """(text, extractor): trafilatura → readability (trafilatura's vendored copy) → plain; 'none' when empty."""
    text = ""
    try:
        import trafilatura
        text = trafilatura.extract(html, url=url, include_comments=False, include_tables=True, include_links=False,
                                   favor_precision=True, output_format="txt") or ""
    except Exception:
        text = ""
    if len(text) >= MIN_EXTRACT_CHARS:
        return text, "trafilatura"
    r = _readability(html)
    if len(r) >= MIN_EXTRACT_CHARS:
        return r, "readability"
    p = plain_text(html)
    if p:
        return p, "plain"
    if text or r:
        return (text, "trafilatura") if len(text) >= len(r) else (r, "readability")
    return "", "none"


def with_head(text: str, title: str | None, description: str | None) -> str:
    """Prepend <title> and the meta/og description so SPA shells hash something meaningful."""
    return "\n".join(x for x in (title, description, text) if x)


# ── normalisation + hash + diff ──────────────────────────────────────────────────────────────────────────────
_MONTHS = r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*"
_NORM_PATTERNS = [
    re.compile(r"\d{4}-\d{2}-\d{2}(?:t[\d:.]+z?)?"),
    re.compile(rf"\b{_MONTHS}\.?\s+\d{{1,2}},?\s+\d{{2,4}}\b"),
    re.compile(rf"\b\d{{1,2}}\s+{_MONTHS}\.?,?\s+\d{{4}}\b"),
    re.compile(r"\b\d{1,2}:\d{2}(?::\d{2})?\s*(?:am|pm)?\b"),
    re.compile(r"\b\d+\s+(?:second|minute|hour|day|week|month|year)s?\s+ago\b"),
    re.compile(r"(?:©|\(c\)|copyright)\s*(?:\d{4}(?:\s*[-–]\s*\d{4})?)?"),
    re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b"),
    re.compile(r"\b[0-9a-f]{16,}\b"),
    re.compile(r"\b[a-z0-9+/=_-]{24,}\b"),
    re.compile(r"\b\d[\d,.]{6,}\b"),
]
_WS = re.compile(r"\s+")


def normalise(text: str) -> str:
    t = unicodedata.normalize("NFKC", text or "").lower()
    for rx in _NORM_PATTERNS:
        t = rx.sub(" ", t)
    return _WS.sub(" ", t).strip()


def content_hash(norm: str | bytes) -> str:
    b = norm.encode() if isinstance(norm, str) else norm
    return hashlib.sha256(b).hexdigest()


def change_ratio(prev_norm: str, cur_norm: str, max_tokens: int = 5000) -> float:
    if prev_norm == cur_norm:
        return 1.0
    a, b = prev_norm.split()[:max_tokens], cur_norm.split()[:max_tokens]
    if not a and not b:
        return 1.0
    return round(difflib.SequenceMatcher(None, a, b, autojunk=False).ratio(), 6)


# ── docs discovery ───────────────────────────────────────────────────────────────────────────────────────────
DOCS_RE = re.compile(r"(?i)\b(docs?|documentation|developers?|api reference|whitepaper|litepaper|gitbook|readthedocs|"
                     r"mintlify)\b")
DOCS_EXTRA_EXCLUDED = ("docs.bittensor.com", "docs.learnbittensor.org")


def find_docs_candidate(links: list[tuple[str, str]], *, landing_urls: Iterable[str], skip_urls: Iterable[str],
                        excluded_hosts: Iterable[str]) -> str | None:
    """First link whose anchor text or href path matches the docs pattern (§2.3 step 10), canonicalised by 04."""
    from app.collectors.web.classify import is_excluded_host
    from app.registry.normalise import canonical_website
    land = {canonical_website(u) for u in landing_urls if u}
    skip = {canonical_website(u) for u in skip_urls if u} | land
    excluded = list(excluded_hosts)
    for href, text in links:
        p = urlsplit(href)
        host = (p.hostname or "").lower()
        if not host or host in DOCS_EXTRA_EXCLUDED or is_excluded_host(host, excluded):
            continue
        path_hit = DOCS_RE.search(p.path.replace("-", " ").replace("_", " ").replace("/", " "))
        host_hit = DOCS_RE.search(host.replace(".", " "))
        if not (DOCS_RE.search(text or "") or path_hit or host_hit):
            continue
        canon = canonical_website(href)
        if not canon or canon in skip:
            continue
        return canon
    return None
