"""Pure team-evidence helpers (spec 11 §2.2.2–§2.2.3): the subnetalpha ``team_info`` parser, name normalisation,
profile classification and the two-source verification rule.

Two-source policy (old ``data/whale_labels.json`` ``_README``): "never assert an identity without two independent
public sources"; ``verified`` = two independent reputable sources. Generalised by 02 §H3: ≥ 2 independent evidence
items, of which at least one is a public URL; the second may be a recorded operator attestation note.
"""
from __future__ import annotations

import hashlib
import html as _html
import re
import unicodedata
from html.parser import HTMLParser
from typing import Iterable, Sequence
from urllib.parse import urlsplit

from app.features.text import _ANON_TOKENS, _looks_anonymous  # noqa: F401 (ported verbatim, shared with 12)
from app.integrity.models import Candidate, TeamInfoParse, TwoSourceResult

_LINE_TAGS = {"p", "li", "br", "div", "tr", "h1", "h2", "h3", "h4", "h5", "h6"}
_SEP = " :–—-|"
ROLE_WORDS = re.compile(r"(co-?founder|founder|ceo|cto|coo|cfo|lead|engineer|developer|researcher|head of \w+|"
                        r"director|advisor|product|marketing|community)", re.I)
ANON_PHRASES = re.compile(
    r"kept private|not (been )?(publicly )?disclosed|no team members have been publicly disclosed|"
    r"(team|founders?|developers?|contributors) (is|are|remains?) (anonymous|pseudonymous|undisclosed|private)|"
    r"anonymous team|pseudonymous team|undisclosed team|prefers? to remain anonymous", re.I)
_PLACEHOLDER = re.compile(r"(awaiting|awating) (data|content)\W*|coming soon\W*|tb[ad]\W*|pending\W*")
_NON_NAME = re.compile(r"\b(team|members?|contributors?|activity|handles|overview|roadmap|leadership|advisors|"
                       r"partners|investors|community|github|website|twitter|linkedin|discord|public|core|key|about|"
                       # 2026-10-18 live pass (11 §2.2.3 step 4): LLM-prose headings of the subnetalpha records
                       r"subnet|ownership|owners?|operators?|program|inception|ltd|llc|inc|labs?|foundation|"
                       r"organi[sz]ation|presence|online|background|expertise|evolution|rebranding|history|backers?|"
                       r"launch|support|funding|partnerships?|collaboration|structure|governance|mission|vision|"
                       r"technology|project|company|corporate|engineers|developers|ecosystem|official|social|media|"
                       r"links?|contacts?|identity|status|summary|notes?|information|details|profiles?|updates|news|"
                       r"blog|docs|documentation)\b", re.I)
_CONNECTIVES = {"and", "with", "of", "the", "for", "in", "on", "by", "to", "from", "at", "&", "is", "are", "who"}
_PARTICLES = {"de", "da", "di", "del", "della", "van", "von", "der", "den", "le", "la", "du", "bin", "al", "el", "y"}
_EMAIL = re.compile(r"\S+@\S+\.\w+")
_DIGITS3 = re.compile(r"\d{3,}")
_DOMAINISH = re.compile(r"\w\.[a-z]{2,}\b", re.I)          # "subnet82.com", "taos.im"
_NAME_ROLE_SPLIT = re.compile(r"\s*(?:,|\s[–—-]\s|[–—|])\s*")


def split_name_role(s: str, *, max_tokens: int = 5) -> tuple[str, str] | None:
    """"Xavier Lyu, Founder" / "Founder/CEO – Bob Wold" → (name, role) when one side is a role and the other a name."""
    parts = _NAME_ROLE_SPLIT.split(s, maxsplit=1)
    if len(parts) != 2:
        return None
    a, b = parts[0].strip(), parts[1].strip()
    if ROLE_WORDS.search(b) and is_plausible_name(a, max_tokens=max_tokens):
        return a, b[:60]
    if ROLE_WORDS.search(a) and is_plausible_name(b, max_tokens=max_tokens):
        return b, a[:60]
    return None

_LINKEDIN = re.compile(r"^https?://([a-z]{2,3}\.)?(www\.)?linkedin\.com/(in|pub|company)/([^/?#]+)", re.I)
_X = re.compile(r"^https?://(www\.|mobile\.)?(x|twitter)\.com/([A-Za-z0-9_]{1,15})(?:[/?#]|$)", re.I)
_X_RESERVED = {"home", "i", "intent", "share", "search", "hashtag"}
_GITHUB = re.compile(r"^https?://(www\.)?github\.com/([A-Za-z0-9-]{1,39})/?$", re.I)
_KIND_PREF = {"linkedin": 0, "x": 1, "github": 2, "website": 3}
_THREE_LABEL = {"co.uk", "com.au", "co.jp", "com.br", "co.in", "github.io", "vercel.app", "netlify.app", "pages.dev",
                "substack.com", "medium.com"}


# ── tokeniser ────────────────────────────────────────────────────────────────────────────────────────────────
class _Tok(HTMLParser):
    """Lines of spans: ('strong', text) | ('text', text) | ('a', href, text). Attributes other than a[href] are
    ignored, which discards the ChatGPT ``data-start``/``class`` leakage."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.lines: list[list[list]] = [[]]
        self._strong = 0
        self._a: list | None = None

    def _newline(self) -> None:
        if self.lines[-1]:
            self.lines.append([])

    def handle_starttag(self, tag, attrs):  # noqa: D401
        tag = tag.lower()
        if tag in _LINE_TAGS:
            self._close_a()
            self._newline()
        elif tag in ("strong", "b"):
            self._strong += 1
        elif tag == "a":
            self._close_a()
            href = next((v for k, v in attrs if k.lower() == "href" and v), None)
            self._a = ["a", (href or "").strip(), ""]

    def handle_startendtag(self, tag, attrs):
        if tag.lower() in _LINE_TAGS:
            self._close_a()
            self._newline()

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in _LINE_TAGS:
            self._close_a()
            self._newline()
        elif tag in ("strong", "b"):
            self._strong = max(0, self._strong - 1)
        elif tag == "a":
            self._close_a()

    def _close_a(self) -> None:
        if self._a is not None:
            self._a[2] = re.sub(r"\s+", " ", self._a[2]).strip()
            self.lines[-1].append(self._a)
            self._a = None

    def handle_data(self, data):
        if not data:
            return
        if self._a is not None:
            self._a[2] += data
            return
        kind = "strong" if self._strong else "text"
        line = self.lines[-1]
        if line and line[-1][0] == kind:
            line[-1][1] += data
        else:
            line.append([kind, data])

    def result(self) -> list[list[list]]:
        self._close_a()
        return [ln for ln in self.lines if ln]


def _tokenise(html: str) -> list[list[list]]:
    t = _Tok()
    try:
        t.feed(html or "")
        t.close()
    except Exception:  # pragma: no cover - HTMLParser is lenient; never raise on editorial input
        pass
    return t.result()


# ── names, urls ──────────────────────────────────────────────────────────────────────────────────────────────
def name_norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(ch for ch in s if not unicodedata.combining(ch)).casefold().strip()
    s = s.lstrip("@")
    s = re.sub(r"[^\w]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def is_plausible_name(s: str, *, max_tokens: int = 5) -> bool:
    s = (s or "").strip()
    if not (2 <= len(s) <= 60):
        return False
    toks = s.split()
    if not (1 <= len(toks) <= max_tokens):
        return False
    if not any(ch.isalpha() for ch in s) or _EMAIL.search(s) or _DIGITS3.search(s):
        return False
    if _looks_anonymous(s) or _NON_NAME.search(s) or any(ch in s for ch in ",:/|–—") or _DOMAINISH.search(s):
        return False
    low = [t.lower() for t in toks]
    if any(t in _CONNECTIVES for t in low):
        return False
    if len(toks) > 1 and any(t[:1].isalpha() and t[:1].islower() and t.lower() not in _PARTICLES for t in toks):
        return False                                     # "Subnet ownership": names are capitalised token-wise
    return not ROLE_WORDS.fullmatch(s.strip(_SEP))     # "Founder" alone is a role heading, not a name


def classify_profile_url(url: str) -> str:
    u = (url or "").strip()
    if _LINKEDIN.match(u):
        return "linkedin"
    m = _X.match(u)
    if m and m.group(3).lower() not in _X_RESERVED:
        return "x"
    if _GITHUB.match(u):
        return "github"
    if re.match(r"^https?://", u, re.I):
        return "website"
    return "other"


def _handle_from_url(url: str) -> str | None:
    m = _LINKEDIN.match(url)
    if m and m.group(3).lower() in ("in", "pub"):
        return m.group(4)
    m = _X.match(url)
    if m and m.group(3).lower() not in _X_RESERVED:
        return m.group(3)
    m = _GITHUB.match(url)
    if m:
        return m.group(2)
    return None


def registrable_domain(url: str) -> str | None:
    try:
        host = (urlsplit((url or "").strip()).hostname or "").lower().rstrip(".")
    except ValueError:
        return None
    if not host:
        return None
    for pre in ("www.", "m.", "mobile."):
        if host.startswith(pre):
            host = host[len(pre):]
            break
    labels = host.split(".")
    if len(labels) <= 2:
        return host
    last2 = ".".join(labels[-2:])
    return ".".join(labels[-3:]) if last2 in _THREE_LABEL else last2


def is_personal_profile(url: str, *, org_handles: Iterable[str] = (), x_handles: Iterable[str] = ()) -> bool:
    u = (url or "").strip()
    orgs = {h.lower() for h in org_handles}
    xs = {h.lower().lstrip("@") for h in x_handles}
    m = _LINKEDIN.match(u)
    if m:
        return m.group(3).lower() in ("in", "pub")
    m = _X.match(u)
    if m:
        h = m.group(3).lower()
        return h not in _X_RESERVED and h not in xs
    m = _GITHUB.match(u)
    if m:
        return m.group(2).lower() not in orgs
    try:
        parts = urlsplit(u)
    except ValueError:
        return False
    host = (parts.hostname or "").lower()
    if host in ("t.me", "telegram.me"):
        return True
    if host.endswith("medium.com") and parts.path.startswith("/@"):
        return True
    return host.endswith(".substack.com") and host.count(".") == 2


def valid_url(u: str | None) -> bool:
    return bool(u) and len(u) <= 500 and bool(re.match(r"^https?://[^\s/]+\.[^\s/]+", u, re.I))


def check_two_source(profile_url: str | None, evidence_urls: Sequence[str], *, attestation_notes: Sequence[str] = (),
                     org_handles: Iterable[str] = (), x_handles: Iterable[str] = (),
                     excluded: Iterable[str] = ("subnetalpha.ai",), min_sources: int = 2) -> TwoSourceResult:
    """evidence_count = distinct non-excluded URL domains + distinct attestation notes (02 §H3); ok needs ≥ 1 new
    evidence item, ≥ 1 public URL domain, ≥ min_sources items and ≥ 1 attestation (official page or a note)."""
    excl = {e.lower() for e in excluded}
    urls = [u for u in [profile_url, *evidence_urls] if u]
    items: list[dict] = []
    domains: list[str] = []
    attest = 0
    for u in urls:
        d = registrable_domain(u)
        if d in excl:
            cls = "excluded"
        elif is_personal_profile(u, org_handles=org_handles, x_handles=x_handles):
            cls = "personal"
        else:
            cls = "attestation"
            attest += 1
        items.append({"url": u, "domain": d, "class": cls})
        if d and d not in excl and d not in domains:
            domains.append(d)
    notes: list[str] = []
    for n in attestation_notes:
        n = (n or "").strip()
        if 10 <= len(n) <= 300 and n not in notes:
            notes.append(n)
    for n in notes:
        items.append({"url": None, "domain": None, "class": "attestation", "note": n})
    count = len(domains) + len(notes)
    reason = None
    if len([u for u in evidence_urls if u]) + len(notes) < 1:
        reason = "evidence_required"
    elif len(domains) < 1:
        reason = "no_public_url"
    elif count < min_sources:
        reason = "too_few_sources"
    elif attest + len(notes) < 1:
        reason = "no_attestation"
    return TwoSourceResult(reason is None, tuple(domains), attest + len(notes), count, reason, tuple(items))


# ── team_info parser ─────────────────────────────────────────────────────────────────────────────────────────
def _role_text(spans: list[list], i: int) -> str:
    buf = []
    for sp in spans[i + 1:]:
        if sp[0] == "strong":
            break
        if sp[0] == "text":
            buf.append(sp[1])
    t = re.sub(r"\s+", " ", _html.unescape("".join(buf))).strip()
    t = re.sub(r"^[\s–—\-:|,]+", "", t).strip()
    return t


def parse_team_info(html: str, *, max_tokens: int = 5, anon_max_chars: int = 200) -> TeamInfoParse:
    html = html or ""
    lines = _tokenise(html)
    cands: dict[str, Candidate] = {}
    order: list[str] = []

    def add(c: Candidate) -> None:
        k = name_norm(c.name)
        if not k:
            return
        if k in cands:
            old = cands[k]
            if old.profile_url is None and c.profile_url:
                cands[k] = Candidate(old.name, old.role or c.role, c.profile_url, c.profile_kind)
            elif old.role is None and c.role:
                cands[k] = Candidate(old.name, c.role, old.profile_url, old.profile_kind)
            return
        cands[k] = c
        order.append(k)

    for spans in lines:
        line_names: list[tuple[int, str]] = []           # (span index, name key) derived from strong spans
        for i, sp in enumerate(spans):
            if sp[0] != "strong":
                continue
            s1 = re.sub(r"\s+", " ", _html.unescape(sp[1])).strip().strip(_SEP).strip()
            if not s1:
                continue
            after = _role_text(spans, i)
            if ROLE_WORDS.fullmatch(s1):
                nm = re.split(r"[,;(]", after, maxsplit=1)[0].strip()
                if is_plausible_name(nm, max_tokens=max_tokens):
                    add(Candidate(nm, s1[:60]))
                    line_names.append((i, name_norm(nm)))
            elif is_plausible_name(s1, max_tokens=max_tokens):
                add(Candidate(s1, after[:60] or None))
                line_names.append((i, name_norm(s1)))
            elif (nr := split_name_role(s1, max_tokens=max_tokens)) is not None:
                add(Candidate(nr[0], nr[1]))
                line_names.append((i, name_norm(nr[0])))
        best: dict[str, tuple[int, str, str]] = {}
        for j, sp in enumerate(spans):
            if sp[0] != "a" or not sp[1]:
                continue
            href, text = sp[1], sp[2]
            kind = classify_profile_url(href)
            if kind == "other":
                continue
            if line_names:                               # the nearest preceding strong name owns the link
                prev = [k for i, k in line_names if i < j]
                k = prev[-1] if prev else line_names[0][1]
                cur = best.get(k)
                if cur is None or _KIND_PREF[kind] < cur[0]:
                    best[k] = (_KIND_PREF[kind], href, kind)
                continue
            t = text.strip().lstrip("@")
            if is_plausible_name(t, max_tokens=max_tokens):
                add(Candidate(t, None, href, kind))
            elif kind in ("linkedin", "x", "github") and is_personal_profile(href):
                h = _handle_from_url(href)
                if h and is_plausible_name(h, max_tokens=max_tokens):
                    add(Candidate(h, None, href, kind))
        for k, (_, href, kind) in best.items():
            c = cands.get(k)
            if c is not None and c.profile_url is None:
                cands[k] = Candidate(c.name, c.role, href, kind)

    text = " ".join(sp[1] if sp[0] != "a" else sp[2] for spans in lines for sp in spans)
    t = re.sub(r"\s+", " ", _html.unescape(text)).strip().lower()
    candidates = tuple(cands[k] for k in order)
    phrase = bool(ANON_PHRASES.search(t))
    short_anon = len(t) <= anon_max_chars and _looks_anonymous(t)
    if not t:
        disclosure = "empty"
    elif candidates:
        disclosure = "named"
    elif phrase or short_anon:
        disclosure = "anonymous"
    elif _PLACEHOLDER.fullmatch(t) or len(t) < 20:
        disclosure = "placeholder"
    else:
        disclosure = "unstructured"
    if _PLACEHOLDER.fullmatch(t):
        disclosure = "placeholder"
    return TeamInfoParse(disclosure, candidates, len(t), hashlib.sha256(html.encode()).hexdigest(),
                         bool(phrase or short_anon))
