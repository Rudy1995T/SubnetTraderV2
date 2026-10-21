"""Official X API v2 provider (spec 09 §2.3 column 2; ToS-clean fallback, pay-per-use).

Timelines use the server-side ``since_id`` with ``max_results=20`` (5 on the first poll — the API minimum, ≈ $0.025
per account instead of a 100-post page). Recent search covers 7 days: a stored ``since_id`` whose snowflake time is
older than 6 d 23 h is replaced by ``start_time = now − 6 d 23 h``, and a 400 that names ``since_id`` triggers the same
fallback once (**UNVERIFIED** API behaviour, 09 §7). Search authors come from ``includes.users``
(``expansions=author_id``). Zero-result calls are assumed to bill one post (**UNVERIFIED**).
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from app.collectors.x_providers.base import (MentionQuery, XCall, XPage, XPost, XProviderError, XUser, as_int, group,
                                             id_gt, json_body, max_id, parse_x_time, provider_get, snowflake_dt)
from app.collectors.x_providers.prices import prices_for

TWEET_FIELDS = "created_at,public_metrics,entities,lang,referenced_tweets,in_reply_to_user_id,conversation_id," \
               "attachments,author_id"
USER_FIELDS = "created_at,public_metrics,verified,username"
SEARCH_WINDOW = timedelta(days=6, hours=23)
FIRST_POLL_MAX_RESULTS = 5
TIMELINE_MAX_RESULTS = 20


class OfficialV2:
    name = "official_v2"
    supports_by_id = True
    timeline_page_size = TIMELINE_MAX_RESULTS
    search_page_size = 100

    def __init__(self, http: Any, settings: Any = None, *, clock: Callable[[], float] = time.time) -> None:
        if settings is None:
            import app.config as app_config
            settings = app_config.settings
        self.http = http
        self.settings = settings
        self.base = str(settings.X_OFFICIAL_BASE_URL).rstrip("/")
        self.timeout = float(settings.X_HTTP_TIMEOUT_SEC)
        self.exclude = str(settings.X_OFFICIAL_EXCLUDE or "")
        self.clock = clock

    def _now(self) -> datetime:
        return datetime.fromtimestamp(self.clock(), timezone.utc)

    async def _get(self, path: str, params: dict) -> tuple[int, dict]:
        resp = await provider_get(self.http, self.name, self.base + path, params=params, timeout=self.timeout)
        st = resp.status_code
        if st in (401, 403):
            raise XProviderError("auth", f"HTTP {st}", status=st)
        if st == 402:
            raise XProviderError("budget", "HTTP 402", status=st)
        if st == 429:
            raise XProviderError("rate_limited", "HTTP 429", status=st)
        if st == 404:
            return st, {}
        body = json_body(resp)
        if st not in (200, 400):
            raise XProviderError("parse", f"unexpected HTTP {st}", status=st)
        return st, body

    @staticmethod
    def _bad_request(body: dict) -> XProviderError:
        return XProviderError("parse", f"HTTP 400: {str(body.get('errors') or body.get('detail'))[:120]}", status=400)

    @staticmethod
    def _not_found(body: dict) -> bool:
        errs = body.get("errors") or []
        return "data" not in body and any(isinstance(e, dict) and (e.get("title") == "Not Found Error" or
                                                                    "suspend" in str(e.get("detail", "")).lower())
                                          for e in errs)

    # ── parsing ──────────────────────────────────────────────────────────────────────────────────────────────
    @staticmethod
    def parse_user(d: Any) -> XUser | None:
        if not isinstance(d, dict) or not d.get("id"):
            return None
        pm = d.get("public_metrics") or {}
        v = d.get("verified")
        return XUser(id=str(d["id"]), handle=str(d.get("username") or ""), display_name=d.get("name"),
                     followers=as_int(pm.get("followers_count")), following=as_int(pm.get("following_count")),
                     post_count=as_int(pm.get("tweet_count")), created_at=parse_x_time(d.get("created_at")),
                     verified=bool(v) if v is not None else None, raw=d)

    @staticmethod
    def parse_post(t: Any, users: dict[str, XUser], *, fallback_author_id: str | None = None) -> XPost:
        if not isinstance(t, dict) or not t.get("id"):
            raise XProviderError("parse", "tweet without id")
        pid = str(t["id"])
        author_id = str(t.get("author_id") or fallback_author_id or "")
        if not author_id:
            raise XProviderError("parse", f"tweet {pid} without author_id")
        refs = {r.get("type"): str(r.get("id")) for r in (t.get("referenced_tweets") or []) if isinstance(r, dict)}
        pm = t.get("public_metrics") or {}
        ents = t.get("entities") or {}
        author = users.get(author_id)
        text = str(t.get("text") or "")
        return XPost(post_id=pid, author_id=author_id, author_handle=author.handle if author else "", text=text,
                     lang=t.get("lang"), event_at=parse_x_time(t.get("created_at")) or snowflake_dt(pid),
                     like_count=as_int(pm.get("like_count")), repost_count=as_int(pm.get("retweet_count")),
                     reply_count=as_int(pm.get("reply_count")), quote_count=as_int(pm.get("quote_count")),
                     view_count=as_int(pm.get("impression_count")),
                     has_url=bool(ents.get("urls")) or "http://" in text or "https://" in text,
                     has_media=bool((t.get("attachments") or {}).get("media_keys")),
                     in_reply_to=refs.get("replied_to"), is_repost="retweeted" in refs,
                     quoted_post_id=refs.get("quoted"),
                     author=author, raw=t)

    def _page(self, body: dict, since_id: str | None, *, fallback_author_id: str | None = None) -> XPage:
        users = {u.id: u for u in (self.parse_user(x) for x in ((body.get("includes") or {}).get("users") or []))
                 if u is not None}
        data = body.get("data") or []
        if not isinstance(data, list):
            raise XProviderError("parse", "data is not a list")
        posts = [self.parse_post(t, users, fallback_author_id=fallback_author_id) for t in data]
        meta = body.get("meta") or {}
        billed = as_int(meta.get("result_count"))
        return XPage(posts=[p for p in posts if id_gt(p.post_id, since_id)], next_cursor=meta.get("next_token") or None,
                     newest_id=max_id([p.post_id for p in posts]) or meta.get("newest_id"),
                     billed_posts=billed if billed is not None else len(posts))

    # ── protocol ─────────────────────────────────────────────────────────────────────────────────────────────
    async def resolve_user(self, handle: str) -> XUser | None:
        return await self._user(f"/users/by/username/{handle}")

    async def resolve_user_by_id(self, user_id: str) -> XUser | None:
        return await self._user(f"/users/{user_id}")

    async def _user(self, path: str) -> XUser | None:
        st, body = await self._get(path, {"user.fields": USER_FIELDS})
        if st == 404 or self._not_found(body):
            return None
        if st == 400:
            raise self._bad_request(body)
        u = self.parse_user(body.get("data"))
        if u is None:
            raise XProviderError("parse", "user lookup without data.id")
        return u

    async def user_timeline(self, user_id: str, since_id: str | None, max_posts: int,
                            cursor: str | None = None) -> XPage:
        params: dict[str, Any] = {"max_results": FIRST_POLL_MAX_RESULTS if since_id is None else TIMELINE_MAX_RESULTS,
                                  "tweet.fields": TWEET_FIELDS, "expansions": "referenced_tweets.id"}
        if since_id:
            params["since_id"] = since_id
        if self.exclude:
            params["exclude"] = self.exclude
        if cursor:
            params["pagination_token"] = cursor
        st, body = await self._get(f"/users/{user_id}/tweets", params)
        if st == 404 or self._not_found(body):
            raise XProviderError("not_found", f"timeline of {user_id}", status=st)
        if st == 400:
            raise self._bad_request(body)
        return self._page(body, since_id, fallback_author_id=user_id)

    async def search_recent(self, query: MentionQuery, since_id: str | None, since_at: datetime | None,
                            max_posts: int, cursor: str | None = None) -> XPage:
        floor = self._now() - SEARCH_WINDOW
        sid = since_id
        start: datetime | None = since_at
        if sid is not None and snowflake_dt(sid) < floor:
            sid, start = None, floor                     # the API rejects a since_id older than 7 d (UNVERIFIED)
        if start is not None and start < floor:
            start = floor
        st, body = await self._search(query, sid, start if sid is None else None, max_posts, cursor)
        if st == 400 and sid is not None and "since_id" in str(body).lower():
            st, body = await self._search(query, None, floor, max_posts, cursor)      # one fallback retry
        if st == 400:
            raise self._bad_request(body)
        return self._page(body, since_id)

    async def _search(self, q: MentionQuery, since_id: str | None, start: datetime | None, max_posts: int,
                      cursor: str | None) -> tuple[int, dict]:
        params: dict[str, Any] = {"query": self.render_query(q, None),
                                  "max_results": max(10, min(self.search_page_size, int(max_posts or 0) or 10)),
                                  "sort_order": "recency", "tweet.fields": TWEET_FIELDS, "expansions": "author_id",
                                  "user.fields": USER_FIELDS}
        if since_id:
            params["since_id"] = since_id
        elif start is not None:
            params["start_time"] = start.strftime("%Y-%m-%dT%H:%M:%SZ")
        if cursor:
            params["pagination_token"] = cursor
        return await self._get("/tweets/search/recent", params)

    def render_query(self, q: MentionQuery, since_at: datetime | None) -> str:
        """``(a OR b) -is:retweet lang:en``; the time bound is a parameter (since_id / start_time), not syntax."""
        parts = [group([f"from:{h}" for h in q.from_handles]) if q.from_handles else group(list(q.terms))]
        if q.required_coterms:
            parts.append(group(list(q.required_coterms)))
        if q.exclude_reposts:
            parts.append("-is:retweet")
        if q.lang:
            parts.append(f"lang:{q.lang}")
        return " ".join(parts)

    def cost_estimate(self, call: XCall) -> float:
        p = prices_for(self.name, self.settings)
        if call.kind == "resolve_user":
            return p["per_user"]
        pages = max(1, int(call.pages))
        return pages * p["per_post"] * max(1.0, max(0, int(call.expected_posts)) / pages)

    def cost_actual(self, call: XCall, page: XPage | None) -> float:
        p = prices_for(self.name, self.settings)
        if call.kind == "resolve_user":
            return p["per_user"]
        return p["per_post"] * max(1, page.billed_posts if page is not None else 0)
