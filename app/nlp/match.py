"""Label-free occupant matching shared by 09 (attribution, mention search, news/video rules) and 10 (rules classifier,
alias table): ``OccupantIndex``, ``match_occupants``, ``build_mention_query``, ``url_canonical``, ``PROMO_RE``.

09 §2.5/§2.6/§8.2 call this module ``rules``; it lives in ``match.py`` because ``app/nlp/rules.py`` is 10's rules
classifier (``nlp.rules:LlmRulesFallbackJob``; 09 changelog 2026-10-18). Matching is deliberately conservative: a
wrong subnet is worse than a miss (10 §9.3 entity precision ≥ 0.90), so ordinary-word names need Bittensor context.
"""
from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Literal
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

if TYPE_CHECKING:  # pragma: no cover
    from app.collectors.x_providers.base import MentionQuery
    from app.storage.db import Database

MatchKind = Literal["sn", "cashtag", "handle", "name", "alias"]
KIND_RANK: dict[str, int] = {"sn": 0, "cashtag": 0, "handle": 1, "name": 2, "alias": 3}

NETUID_RE = re.compile(r"(?<![\w$])(?:sn|subnet|netuid)[ \-_]?#?(\d{1,3})(?!\d)", re.I)
CASHTAG_RE = re.compile(r"\$sn(\d{1,3})(?!\d)", re.I)
HANDLE_RE = re.compile(r"(?<![\w@])@([A-Za-z0-9_]{1,15})(?![A-Za-z0-9_])")
CONTEXT_RE = re.compile(r"bittensor|(?<![\w$])\$?tao(?![\w])|(?<!\w)dtao(?!\w)|subnet|netuid|(?<![\w$])\$?sn ?\d{1,3}(?!\d)"
                        r"|opentensor|taostats|\balpha token", re.I)
PROMO_RE = re.compile(r"giveaway|airdrop|whitelist|presale|100x|retweet to win|rt to win|dm me|send \d+|claim your|"
                      r"free mint|🎁|💸|🚀{3,}", re.I)
_PLACEHOLDER_RE = re.compile(r"^(?:subnet ?\d+|sn ?\d+|unknown|deprecated|parked|none|null|n/?a|tbd|-+)$", re.I)
COTERMS = ["bittensor", "TAO", "subnet", "dTAO"]
_TRACKING = re.compile(r"^(utm_.*|ref|ref_src|ref_url|fbclid|gclid|dclid|mc_cid|mc_eid|igshid|si|s)$", re.I)
_AMBIGUOUS_FILE = Path(__file__).with_name("ambiguous_names.txt")


def norm(s: str | None) -> str:
    """NFKC + lower (keeps @handles and URLs — unlike textnorm, which strips them for hashing)."""
    return unicodedata.normalize("NFKC", s or "").lower()


@lru_cache(maxsize=1)
def ambiguous_names() -> frozenset[str]:
    try:
        lines = _AMBIGUOUS_FILE.read_text(encoding="utf-8").splitlines()
    except OSError:
        return frozenset()
    return frozenset(norm(x.strip()) for x in lines if x.strip() and not x.lstrip().startswith("#"))


def is_placeholder(name: str | None) -> bool:
    return not name or not name.strip() or bool(_PLACEHOLDER_RE.match(name.strip()))


def is_ambiguous(name: str) -> bool:
    n = norm(name).strip()
    return len(n) <= 4 or n in ambiguous_names()


def has_context(text: str) -> bool:
    return bool(CONTEXT_RE.search(text or ""))


def url_canonical(url: str) -> str:
    """Lower scheme/host, drop tracking params (utm_*, ref, fbclid, gclid, mc_*), fragment and trailing slash; the
    path keeps its case (09 §8.2)."""
    u = (url or "").strip()
    try:
        p = urlsplit(u)
    except ValueError:
        return u
    if not p.scheme or not p.netloc:
        return u
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if not _TRACKING.match(k)]
    path = p.path.rstrip("/") if p.path not in ("", "/") else ""
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), path, urlencode(q), ""))


def host_of(url: str | None) -> str | None:
    if not url:
        return None
    try:
        h = (urlsplit(url if "://" in url else "https://" + url).hostname or "").lower()
    except ValueError:
        return None
    return h[4:] if h.startswith("www.") else (h or None)


@dataclass(frozen=True)
class OccupantRef:
    occupant_key: str
    netuid: int
    name: str | None
    symbol: str | None = None
    handles: tuple[str, ...] = ()            # active team X handles (case kept); `legacy` rows never count (09 §2.4)
    aliases: tuple[str, ...] = ()            # latest manual_notes(kind='alias') split on ';'
    website_host: str | None = None


@dataclass
class OccupantIndex:
    refs: list[OccupantRef]
    by_key: dict[str, OccupantRef] = field(init=False)
    by_netuid: dict[int, OccupantRef] = field(init=False)
    by_handle: dict[str, list[str]] = field(init=False)      # handle_lower → occupant keys (shared accounts fan out)
    _terms: list[tuple[re.Pattern, str, str, bool]] = field(init=False)   # (regex, occupant_key, kind, needs_context)

    def __post_init__(self) -> None:
        self.by_key = {r.occupant_key: r for r in self.refs}
        self.by_netuid = {r.netuid: r for r in self.refs}
        self.by_handle = {}
        for r in self.refs:
            for h in r.handles:
                self.by_handle.setdefault(h.lower(), [])
                if r.occupant_key not in self.by_handle[h.lower()]:
                    self.by_handle[h.lower()].append(r.occupant_key)
        host_count: dict[str, int] = {}
        for r in self.refs:
            if r.website_host:
                host_count[r.website_host] = host_count.get(r.website_host, 0) + 1
        terms: list[tuple[re.Pattern, str, str, bool]] = []
        for r in self.refs:
            if not is_placeholder(r.name) and len(norm(r.name).strip()) >= 2:
                terms.append((_word_re(r.name or ""), r.occupant_key, "name", is_ambiguous(r.name or "")))
            for a in r.aliases:
                if len(norm(a).strip()) >= 3 and not is_placeholder(a):
                    terms.append((_word_re(a), r.occupant_key, "alias", is_ambiguous(a)))
            h = r.website_host
            if h and host_count.get(h) == 1 and "." in h:
                terms.append((_word_re(h), r.occupant_key, "alias", False))
        self._terms = terms

    @classmethod
    async def load(cls, db: "Database") -> "OccupantIndex":
        """Current occupants (``subnets``, root excluded), their active non-legacy X handles and aliases."""
        subs = await db.fetchall(
            "SELECT netuid, occupant_key, name, symbol, website_url FROM subnets WHERE netuid > 0 "
            "AND status <> 'deregistered' ORDER BY netuid")
        xrows = await db.fetchall(
            "SELECT occupant_key, handle, source FROM watch_channels WHERE platform = 'x' AND retired_at IS NULL "
            "AND occupant_key IS NOT NULL")
        handles: dict[str, list[tuple[str, str]]] = {}
        for x in xrows:
            handles.setdefault(x["occupant_key"], []).append((x["handle"], x["source"]))
        alias_rows = await db.fetchall(
            "SELECT occupant_key, value FROM manual_notes m WHERE kind = 'alias' AND occupant_key IS NOT NULL "
            "AND id = (SELECT MAX(id) FROM manual_notes m2 WHERE m2.kind = 'alias' "
            "AND m2.occupant_key = m.occupant_key)")
        aliases = {a["occupant_key"]: tuple(x.strip() for x in (a["value"] or "").split(";") if x.strip())
                   for a in alias_rows}
        refs = []
        for s in subs:
            hs = handles.get(s["occupant_key"], [])
            usable = sorted({h for h, src in hs if src != "legacy"})        # 09 §2.4 (2026-10-18)
            refs.append(OccupantRef(s["occupant_key"], int(s["netuid"]), s["name"], s["symbol"], tuple(usable),
                                    aliases.get(s["occupant_key"], ()), host_of(s["website_url"])))
        return cls(refs)

    def handles_of(self, occupant_key: str) -> tuple[str, ...]:
        r = self.by_key.get(occupant_key)
        return r.handles if r else ()


def _word_re(term: str) -> re.Pattern:
    t = re.escape(norm(term).strip())
    t = t.replace(r"\ ", r"[\s_-]?") if " " in norm(term).strip() else t
    return re.compile(r"(?<![\w])" + t + r"(?![\w])")


def match_occupants(text: str | None, index: OccupantIndex, restrict_to: Iterable[str] | None = None
                    ) -> list[tuple[str, str]]:
    """[(occupant_key, kind)] in order of first appearance, one entry per occupant (strongest kind wins).

    Kinds: ``sn`` (SN64 / subnet 64 / netuid 64), ``cashtag`` ($SN64), ``handle`` (@team, shared accounts fan out),
    ``name`` (word-bounded registry name), ``alias`` (manual alias or the occupant's unique website host). Names and
    aliases that are ordinary words (``is_ambiguous``) count only with Bittensor context (``CONTEXT_RE``)."""
    if not text:
        return []
    t = norm(text)
    allowed = set(restrict_to) if restrict_to is not None else None
    hits: dict[str, tuple[int, int, str]] = {}          # occupant → (rank, pos, kind)

    def add(occ: str, kind: str, pos: int) -> None:
        if allowed is not None and occ not in allowed:
            return
        cur = hits.get(occ)
        r = KIND_RANK[kind]
        if cur is None:
            hits[occ] = (r, pos, kind)
        else:
            hits[occ] = (min(cur[0], r), min(cur[1], pos), kind if r < cur[0] else cur[2])

    for m in CASHTAG_RE.finditer(t):
        r = index.by_netuid.get(int(m.group(1)))
        if r:
            add(r.occupant_key, "cashtag", m.start())
    for m in NETUID_RE.finditer(t):
        r = index.by_netuid.get(int(m.group(1)))
        if r:
            add(r.occupant_key, "sn", m.start())
    for m in HANDLE_RE.finditer(text):                   # original case; compare lower
        for occ in index.by_handle.get(m.group(1).lower(), []):
            add(occ, "handle", m.start())
    ctx: bool | None = None
    for rx, occ, kind, needs_ctx in index._terms:
        m = rx.search(t)
        if not m:
            continue
        if needs_ctx:
            if ctx is None:
                ctx = has_context(t)
            if not ctx:
                continue
        add(occ, kind, m.start())
    return [(occ, v[2]) for occ, v in sorted(hits.items(), key=lambda kv: (kv[1][1], kv[0]))]


def build_mention_query(occupant: OccupantRef, team_handles: Iterable[str] | None = None) -> "MentionQuery":
    """09 §2.5: name (unless a placeholder), SN{n}, "subnet {n}", $SN{n} and @handles; ambiguous names add the
    Bittensor co-term group."""
    from app.collectors.x_providers.base import MentionQuery
    n = occupant.netuid
    terms: list[str] = []
    name = occupant.name
    if not is_placeholder(name):
        terms.append(f'"{name}"')
    terms += [f"SN{n}", f'"subnet {n}"', f"$SN{n}"]
    for h in (team_handles if team_handles is not None else occupant.handles):
        t = f"@{h}"
        if t not in terms:
            terms.append(t)
    co = list(COTERMS) if (name and not is_placeholder(name) and is_ambiguous(name)) else []
    return MentionQuery(terms=terms, required_coterms=co, from_handles=[])


def mentions_json(pairs: Iterable[tuple[str, str]]) -> str:
    """03's ``occupant_keys_json`` format: ``[{"occupant_key": …, "kind": …}]`` (kinds ``rule``/``query``)."""
    return json.dumps([{"occupant_key": k, "kind": kind} for k, kind in pairs], separators=(",", ":"))


__all__ = ["OccupantRef", "OccupantIndex", "match_occupants", "build_mention_query", "url_canonical", "host_of",
           "PROMO_RE", "CONTEXT_RE", "COTERMS", "is_ambiguous", "is_placeholder", "has_context", "ambiguous_names",
           "norm", "mentions_json"]
