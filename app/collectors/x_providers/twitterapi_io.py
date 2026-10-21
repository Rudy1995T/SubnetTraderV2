"""twitterapi.io provider (spec 09 §2.3 column 1; primary, unofficial).

Every endpoint, parameter and field name below is as observed in vendor docs at research time and is **UNVERIFIED**
(no key existed at build time; 09 §11 Q2). The parser reads every vendor field through ``FIELD_MAP`` so a rename is a
one-line fix, and the collector archives raw posts (``data/raw/x/``) so a rename can be re-parsed without re-buying.

One provider call = one HTTP page: the collector loops pages and makes one governor check per page (09 §8.2). There is
no server-side ``since_id``: posts are filtered client-side to ``id > since_id`` and ``next_cursor`` is cleared once a
page reaches ``since_id`` (the first item is skipped for that test because a pinned tweet may come first).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.collectors.x_providers.base import (MentionQuery, XCall, XPage, XPost, XProviderError, XUser, as_int, group,
                                             id_gt, json_body, max_id, parse_x_time, provider_get, snowflake_dt,
                                             snowflake_ms)
from app.collectors.x_providers.prices import prices_for

FIELD_MAP: dict[str, dict[str, str]] = {
    "envelope": {"data": "data", "status": "status", "message": "msg"},
    "page": {"tweets": "tweets", "has_next": "has_next_page", "next_cursor": "next_cursor"},
    "user": {"id": "id", "handle": "userName", "display_name": "name", "followers": "followers",
             "following": "following", "post_count": "statusesCount", "created_at": "createdAt",
             "verified": "isBlueVerified", "unavailable": "unavailable"},
    "tweet": {"id": "id", "text": "text", "created_at": "createdAt", "lang": "lang", "likes": "likeCount",
              "reposts": "retweetCount", "replies": "replyCount", "quotes": "quoteCount", "views": "viewCount",
              "in_reply_to": "inReplyToId", "author": "author", "retweeted": "retweeted_tweet",
              "quoted": "quoted_tweet", "entities": "entities", "urls": "urls", "media_root": "extendedEntities",
              "media": "media"},
}
_NOT_FOUND_WORDS = ("not found", "not exist", "suspend", "unavailable")
_BALANCE_WORDS = ("credit", "balance", "insufficient", "recharge")


def _f(obj: Any, kind: str, field: str, default: Any = None) -> Any:
    if not isinstance(obj, dict):
        return default
    return obj.get(FIELD_MAP[kind][field], default)


class TwitterApiIo:
    name = "twitterapi_io"
    supports_by_id = True                     # GET /twitter/user/info?userId= (UNVERIFIED param)
    timeline_page_size = 20
    search_page_size = 20

    def __init__(self, http: Any, settings: Any = None) -> None:
        if settings is None:
            import app.config as app_config
            settings = app_config.settings
        self.http = http
        self.settings = settings
        self.base = str(settings.TWITTERAPI_IO_BASE_URL).rstrip("/")
        self.timeout = float(settings.X_HTTP_TIMEOUT_SEC)

    # ── http + error mapping (09 §2.3 "HTTP errors") ─────────────────────────────────────────────────────────
    async def _get(self, path: str, params: dict) -> tuple[int, dict]:
        resp = await provider_get(self.http, self.name, self.base + path, params=params, timeout=self.timeout)
        st = resp.status_code
        if st in (401, 403):
            raise XProviderError("auth", f"HTTP {st}", status=st)
        if st == 402:
            raise XProviderError("budget", "HTTP 402 (vendor balance)", status=st)
        if st == 429:
            raise XProviderError("rate_limited", "HTTP 429", status=st)
        if st == 404:
            return st, {}
        body = json_body(resp)
        if st == 400:
            raise XProviderError("parse", f"HTTP 400: {str(_f(body, 'envelope', 'message', ''))[:120]}", status=st)
        if st != 200:
            raise XProviderError("parse", f"unexpected HTTP {st}", status=st)
        if str(_f(body, "envelope", "status", "")).lower() == "error":
            msg = str(_f(body, "envelope", "message", "") or "").lower()
            if any(w in msg for w in _BALANCE_WORDS):
                raise XProviderError("budget", msg[:120], status=st)
            if any(w in msg for w in _NOT_FOUND_WORDS):
                return 404, body
            raise XProviderError("parse", f"vendor error: {msg[:120]}", status=st)
        return st, body

    # ── parsing ──────────────────────────────────────────────────────────────────────────────────────────────
    def parse_user(self, d: Any) -> XUser | None:
        if not isinstance(d, dict) or _f(d, "user", "id") in (None, ""):
            return None
        verified = _f(d, "user", "verified")
        return XUser(id=str(_f(d, "user", "id")), handle=str(_f(d, "user", "handle") or ""),
                     display_name=_f(d, "user", "display_name"), followers=as_int(_f(d, "user", "followers")),
                     following=as_int(_f(d, "user", "following")), post_count=as_int(_f(d, "user", "post_count")),
                     created_at=parse_x_time(_f(d, "user", "created_at")),
                     verified=bool(verified) if verified is not None else None, raw=d)

    def parse_post(self, t: Any, *, fallback_author_id: str | None = None) -> XPost:
        if not isinstance(t, dict) or _f(t, "tweet", "id") in (None, ""):
            raise XProviderError("parse", "tweet without id")
        pid = str(_f(t, "tweet", "id"))
        try:
            snowflake_ms(pid)
        except ValueError as exc:
            raise XProviderError("parse", f"non-numeric tweet id {pid!r}") from exc
        author = self.parse_user(_f(t, "tweet", "author"))
        author_id = author.id if author else (fallback_author_id or "")
        if not author_id:
            raise XProviderError("parse", f"tweet {pid} without author")
        text = str(_f(t, "tweet", "text") or "")
        ents = _f(t, "tweet", "entities") or {}
        urls = ents.get(FIELD_MAP["tweet"]["urls"]) if isinstance(ents, dict) else None
        media_root = _f(t, "tweet", "media_root") or {}
        media = media_root.get(FIELD_MAP["tweet"]["media"]) if isinstance(media_root, dict) else None
        quoted = _f(t, "tweet", "quoted")
        rt = _f(t, "tweet", "retweeted")
        reply_to = _f(t, "tweet", "in_reply_to")
        return XPost(post_id=pid, author_id=author_id, author_handle=author.handle if author else "", text=text,
                     lang=_f(t, "tweet", "lang"),
                     event_at=parse_x_time(_f(t, "tweet", "created_at")) or snowflake_dt(pid),
                     like_count=as_int(_f(t, "tweet", "likes")), repost_count=as_int(_f(t, "tweet", "reposts")),
                     reply_count=as_int(_f(t, "tweet", "replies")), quote_count=as_int(_f(t, "tweet", "quotes")),
                     view_count=as_int(_f(t, "tweet", "views")),
                     has_url=bool(urls) or "http://" in text or "https://" in text, has_media=bool(media),
                     in_reply_to=str(reply_to) if reply_to not in (None, "") else None,
                     is_repost=bool(rt) or text.startswith("RT @"),
                     quoted_post_id=str(_f(quoted, "tweet", "id")) if isinstance(quoted, dict)
                     and _f(quoted, "tweet", "id") else None,
                     author=author, raw=t)

    def _page(self, body: dict, since_id: str | None, *, fallback_author_id: str | None = None) -> XPage:
        src = body
        if FIELD_MAP["page"]["tweets"] not in body and isinstance(_f(body, "envelope", "data"), dict):
            src = _f(body, "envelope", "data")          # some endpoints nest the page under data (UNVERIFIED)
        tweets = src.get(FIELD_MAP["page"]["tweets"])
        if tweets is None:
            tweets = []
        if not isinstance(tweets, list):
            raise XProviderError("parse", "tweets is not a list")
        posts = [self.parse_post(t, fallback_author_id=fallback_author_id) for t in tweets]
        ids = [p.post_id for p in posts]
        has_next = bool(src.get(FIELD_MAP["page"]["has_next"], body.get(FIELD_MAP["page"]["has_next"])))
        cursor = src.get(FIELD_MAP["page"]["next_cursor"], body.get(FIELD_MAP["page"]["next_cursor"])) or None
        tail = ids[1:] if len(ids) > 1 else ids          # a pinned tweet may come first
        reached = since_id is not None and bool(tail) and not id_gt(min(tail, key=int), since_id)
        nxt = cursor if (has_next and cursor and posts and not reached) else None
        return XPage(posts=[p for p in posts if id_gt(p.post_id, since_id)], next_cursor=nxt, newest_id=max_id(ids),
                     billed_posts=len(posts))

    # ── protocol ─────────────────────────────────────────────────────────────────────────────────────────────
    async def resolve_user(self, handle: str) -> XUser | None:
        st, body = await self._get("/twitter/user/info", {"userName": handle})
        return self._user_from(st, body)

    async def resolve_user_by_id(self, user_id: str) -> XUser | None:
        st, body = await self._get("/twitter/user/info", {"userId": user_id})
        return self._user_from(st, body)

    def _user_from(self, st: int, body: dict) -> XUser | None:
        if st == 404:
            return None
        data = _f(body, "envelope", "data")
        if not isinstance(data, dict) or _f(data, "user", "unavailable"):
            return None
        u = self.parse_user(data)
        if u is None:
            raise XProviderError("parse", "user/info without id")
        return u

    async def user_timeline(self, user_id: str, since_id: str | None, max_posts: int,
                            cursor: str | None = None) -> XPage:
        params: dict[str, Any] = {"userId": user_id, "includeReplies": "true"}
        if cursor:
            params["cursor"] = cursor
        st, body = await self._get("/twitter/user/last_tweets", params)
        if st == 404:
            raise XProviderError("not_found", f"timeline of {user_id}", status=404)
        return self._page(body, since_id, fallback_author_id=user_id)

    async def search_recent(self, query: MentionQuery, since_id: str | None, since_at: datetime | None,
                            max_posts: int, cursor: str | None = None) -> XPage:
        eff = since_at if since_at is not None else (snowflake_dt(since_id) if since_id else None)
        params: dict[str, Any] = {"query": self.render_query(query, eff), "queryType": "Latest"}
        if cursor:
            params["cursor"] = cursor
        st, body = await self._get("/twitter/tweet/advanced_search", params)
        if st == 404:
            raise XProviderError("parse", "advanced_search returned 404", status=404)
        return self._page(body, since_id)

    def render_query(self, q: MentionQuery, since_at: datetime | None) -> str:
        """Web-search syntax: ``(a OR b) -filter:retweets since:2026-09-21_03:00:00_UTC lang:en`` (the ``since:``
        time suffix is per vendor docs, **UNVERIFIED** until the §9.2 fixtures exist)."""
        parts = [group([f"from:{h}" for h in q.from_handles]) if q.from_handles else group(list(q.terms))]
        if q.required_coterms:
            parts.append(group(list(q.required_coterms)))
        if q.exclude_reposts:
            parts.append("-filter:retweets")
        if since_at is not None:
            parts.append("since:" + since_at.strftime("%Y-%m-%d_%H:%M:%S_UTC"))
        if q.lang:
            parts.append(f"lang:{q.lang}")
        return " ".join(parts)

    def cost_estimate(self, call: XCall) -> float:
        p = prices_for(self.name, self.settings)
        if call.kind == "resolve_user":
            return p["per_profile"]
        pages = max(1, int(call.pages))
        return pages * max(p["min_call"], p["per_tweet"] * (max(0, int(call.expected_posts)) / pages))

    def cost_actual(self, call: XCall, page: XPage | None) -> float:
        p = prices_for(self.name, self.settings)
        if call.kind == "resolve_user":
            return p["per_profile"]
        return max(p["min_call"], p["per_tweet"] * (page.billed_posts if page is not None else 0))
