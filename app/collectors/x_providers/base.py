"""X provider protocol and value types (spec 09 §2.2).

Providers are built on 01's ``Http`` (02 §G1): every request is ``http.request(self.name, …)``, retries and rate
limits belong to ``Http``/``ProviderLimiter``, and ``XProviderError`` is the only exception a provider raises.
``name`` is the 02 §G1 provider id and the governor's spend provider (group ``x``).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol

ProviderName = Literal["twitterapi_io", "official_v2", "null"]
ErrorKind = Literal["auth", "rate_limited", "budget", "transient", "parse", "not_found"]


@dataclass(frozen=True)
class XUser:
    id: str                       # provider-stable numeric id as string
    handle: str                   # case as returned by provider
    display_name: str | None
    followers: int | None
    following: int | None
    post_count: int | None
    created_at: datetime | None   # tz-aware UTC
    verified: bool | None         # blue/legacy verified flag; None if not exposed
    raw: dict = field(default_factory=dict, compare=False)


@dataclass(frozen=True)
class XPost:
    post_id: str
    author_id: str
    author_handle: str
    text: str
    lang: str | None
    event_at: datetime            # created_at, UTC
    like_count: int | None
    repost_count: int | None
    reply_count: int | None
    quote_count: int | None
    view_count: int | None
    has_url: bool
    has_media: bool
    in_reply_to: str | None       # post id replied to
    is_repost: bool               # pure retweet
    quoted_post_id: str | None
    author: XUser | None          # populated when the provider embeds author metrics (search results)
    raw: dict = field(default_factory=dict, compare=False)


@dataclass(frozen=True)
class XPage:
    posts: list[XPost]
    next_cursor: str | None
    newest_id: str | None         # max post_id on the page (compared as int)
    billed_posts: int             # posts the provider bills for this page


@dataclass(frozen=True)
class MentionQuery:
    terms: list[str]              # OR-ed terms, already quoted where needed
    required_coterms: list[str]   # AND-ed OR-group, may be empty
    from_handles: list[str]       # packed KOL/team search (from:a OR from:b); exclusive with terms
    exclude_reposts: bool = True
    lang: str | None = None


@dataclass(frozen=True)
class XCall:
    kind: Literal["resolve_user", "user_timeline", "search_recent"]
    expected_posts: int           # collector's estimate (09 §6.2)
    pages: int = 1


class XProviderError(Exception):
    def __init__(self, kind: ErrorKind, message: str = "", *, status: int | None = None) -> None:
        super().__init__(f"{kind}: {message}" if message else kind)
        self.kind: ErrorKind = kind
        self.status = status


class XProvider(Protocol):
    name: ProviderName

    async def resolve_user(self, handle: str) -> XUser | None: ...            # None = verifiably not found/suspended
    async def resolve_user_by_id(self, user_id: str) -> XUser | None: ...
    async def user_timeline(self, user_id: str, since_id: str | None, max_posts: int,
                            cursor: str | None = None) -> XPage: ...
    async def search_recent(self, query: MentionQuery, since_id: str | None, since_at: datetime | None,
                            max_posts: int, cursor: str | None = None) -> XPage: ...
    def cost_estimate(self, call: XCall) -> float: ...                         # USD, before the call
    def cost_actual(self, call: XCall, page: XPage) -> float: ...              # USD, after the call
    def render_query(self, q: MentionQuery, since_at: datetime | None) -> str: ...


def snowflake_ms(post_id: str | int) -> int:
    """X snowflake → unix ms: ``(id >> 22) + 1288834974657`` (09 §2.9)."""
    return (int(post_id) >> 22) + 1288834974657


def id_gt(a: str | None, b: str | None) -> bool:
    """Numeric comparison of X ids (snowflakes exceed 2^53; stored as TEXT)."""
    if a is None:
        return False
    if b is None:
        return True
    return int(a) > int(b)


def max_id(ids: list[str]) -> str | None:
    return max(ids, key=int) if ids else None


# ── shared provider helpers (09 §2.2: one HTTP layer, one error type) ───────────────────────────────────────
OK_STATUSES = (200, 400, 402)


async def provider_get(http, name: str, url: str, *, params: dict | None = None, timeout: float | None = None):
    """``http.request(name, 'GET', …, ok_statuses=(200, 400, 402))``; a ``ProviderError`` left after Http's retries
    maps to ``rate_limited`` (last status 429) or ``transient`` (09 §2.2). No retry, sleep or client of its own."""
    from app.http import ProviderError
    try:
        return await http.request(name, "GET", url, params=params, ok_statuses=OK_STATUSES, timeout=timeout)
    except ProviderError as exc:
        kind: ErrorKind = "rate_limited" if exc.status == 429 else "transient"
        raise XProviderError(kind, str(exc), status=exc.status) from exc


def json_body(resp) -> dict:
    try:
        data = resp.json()
    except ValueError as exc:
        raise XProviderError("parse", f"non-JSON body (HTTP {resp.status_code})", status=resp.status_code) from exc
    if not isinstance(data, dict):
        raise XProviderError("parse", "JSON body is not an object", status=resp.status_code)
    return data


def parse_x_time(v) -> datetime | None:
    """ISO-8601 (official v2) or the classic ``Tue Dec 10 07:00:30 +0000 2024`` (twitterapi.io) → aware UTC."""
    from datetime import timezone
    if v in (None, ""):
        return None
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(float(v), timezone.utc)
    s = str(v).strip()
    try:
        return datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y").astimezone(timezone.utc)
    except ValueError:
        pass
    try:
        d = datetime.fromisoformat(s[:-1] + "+00:00" if s.endswith(("Z", "z")) else s)
    except ValueError:
        return None
    return (d if d.tzinfo else d.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)


def snowflake_dt(post_id: str | int) -> datetime:
    from datetime import timezone
    return datetime.fromtimestamp(snowflake_ms(post_id) / 1000.0, timezone.utc)


def as_int(v) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def group(terms: list[str]) -> str:
    return terms[0] if len(terms) == 1 else "(" + " OR ".join(terms) + ")"
