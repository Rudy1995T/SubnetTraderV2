"""The no-op provider (spec 09 §2.3 column 3): the default ``X_PROVIDER=null``. It makes no request and costs nothing;
X metrics of occupants with a handle are then ``failed`` (never absent, 02 §C3)."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.collectors.x_providers.base import MentionQuery, XCall, XPage, XUser


class NullProvider:
    name = "null"
    supports_by_id = True
    timeline_page_size = 0
    search_page_size = 0

    def __init__(self, http: Any = None, settings: Any = None) -> None:
        self.http = http

    async def resolve_user(self, handle: str) -> XUser | None:
        return None

    async def resolve_user_by_id(self, user_id: str) -> XUser | None:
        return None

    async def user_timeline(self, user_id: str, since_id: str | None, max_posts: int,
                            cursor: str | None = None) -> XPage:
        return XPage([], None, None, 0)

    async def search_recent(self, query: MentionQuery, since_id: str | None, since_at: datetime | None,
                            max_posts: int, cursor: str | None = None) -> XPage:
        return XPage([], None, None, 0)

    def cost_estimate(self, call: XCall) -> float:
        return 0.0

    def cost_actual(self, call: XCall, page: XPage | None) -> float:
        return 0.0

    def render_query(self, q: MentionQuery, since_at: datetime | None) -> str:
        return ""
