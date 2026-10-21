"""Keyless Discord REST: invite metadata and ``widget.json`` (spec 08 §2.3, §2.5).

Parsers are pure and privacy-first: the invite ``inviter`` object and the widget ``members[]`` list are dropped before
anything else sees the body (§2.11.2). ``fetch_invite`` never raises except ``CloudflareBan`` (a non-JSON 429, which
also penalizes the whole ``discord`` bucket for an hour, persisted across restarts by the limiter).
"""
from __future__ import annotations

import json
import re
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable
from urllib.parse import quote

from app.http import ProviderError
from app.logging.logger import logger
from app.util.time import now_ts, parse_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.http import Http

DISCORD_EPOCH_MS = 1420070400000
DISCORD_HOST = "discord.com"
INVALID_STATUSES = (401, 403, 429)
_INVITE_URL = re.compile(r"discord(?:app)?\.(?:gg|com)/(?:invite/)?([A-Za-z0-9-]+)")


class CloudflareBan(ProviderError):
    """A 429 whose body is not JSON (Cloudflare block page, e.g. error 1015): abort the run."""


def snowflake_to_datetime(snowflake: str | int) -> datetime:
    ms = (int(snowflake) >> 22) + DISCORD_EPOCH_MS
    return datetime.fromtimestamp(ms / 1000, timezone.utc)


def snowflake_ts(snowflake: str | int) -> int:
    return ((int(snowflake) >> 22) + DISCORD_EPOCH_MS) // 1000


@dataclass(slots=True, frozen=True)
class InviteResult:
    code: str
    fetched_at: int                       # unix s (B1)
    http_status: int | None
    invite_valid: int | None              # 1 valid · 0 dead · None failure
    invite_error_code: int | None = None
    guild_id: str | None = None
    member_count: int | None = None
    presence_count: int | None = None
    presence_ratio: float | None = None
    expires_at: int | None = None
    landing_channel_id: str | None = None
    landing_channel_type: int | None = None
    features: tuple[str, ...] = ()
    boosts: int | None = None
    verification_level: int | None = None
    guild_created_at: datetime | None = None
    meta: dict = field(default_factory=dict)
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.invite_valid is not None

    @property
    def landing_channel_name(self) -> str | None:
        return self.meta.get("landing_channel_name")

    @classmethod
    def from_row(cls, r: dict) -> "InviteResult":
        meta = json.loads(r["meta_json"]) if r.get("meta_json") else {}
        feats = tuple(json.loads(r["features_json"])) if r.get("features_json") else ()
        gca = r.get("guild_created_at")
        return cls(code=r["invite_code"], fetched_at=int(r["observed_at"]), http_status=r.get("http_status"),
                   invite_valid=r.get("invite_valid"), invite_error_code=r.get("invite_error_code"),
                   guild_id=r.get("guild_id"), member_count=r.get("member_count"),
                   presence_count=r.get("presence_count"), presence_ratio=r.get("presence_ratio"),
                   expires_at=r.get("expires_at"), landing_channel_id=r.get("landing_channel_id"),
                   landing_channel_type=r.get("landing_channel_type"), features=feats, boosts=r.get("boosts"),
                   verification_level=r.get("verification_level"),
                   guild_created_at=datetime.fromtimestamp(gca, timezone.utc) if gca is not None else None,
                   meta=meta, error=meta.get("error"))


@dataclass(slots=True, frozen=True)
class WidgetResult:
    guild_id: str
    fetched_at: int
    http_status: int | None
    widget_enabled: int | None
    presence_count: int | None = None
    instant_invite_code: str | None = None
    channel_count: int | None = None
    error: str | None = None


def _int(x: Any) -> int | None:
    try:
        return None if x is None or isinstance(x, bool) else int(x)
    except (TypeError, ValueError):
        return None


def _json(body: bytes | str | None) -> Any:
    if body is None:
        return None
    try:
        return json.loads(body)
    except (TypeError, ValueError):
        return None


def _failure(code: str, fetched_at: int, status: int | None, error: str) -> InviteResult:
    return InviteResult(code=code, fetched_at=fetched_at, http_status=status, invite_valid=None,
                        meta={"error": error}, error=error)


def parse_invite(code: str, status: int | None, body: bytes | str | None, fetched_at: int) -> InviteResult:
    """§2.3 table. 200 guild invite → 1; 404 JSON → 0 (+ code); 200 non-guild → 0; anything else → None."""
    fetched_at = int(fetched_at)
    if status is None:
        return _failure(code, fetched_at, None, "transport")
    d = _json(body)
    if status == 404:
        if isinstance(d, dict) and isinstance(d.get("code"), int):
            return InviteResult(code=code, fetched_at=fetched_at, http_status=404, invite_valid=0,
                                invite_error_code=int(d["code"]), meta={}, error=None)
        return _failure(code, fetched_at, 404, "html_404")
    if status != 200:
        return _failure(code, fetched_at, status, f"http_{status}")
    if not isinstance(d, dict):
        return _failure(code, fetched_at, 200, "bad_json")
    d.pop("inviter", None)                         # a person — never stored or logged (§2.11.2)
    d.pop("target_user", None)
    guild = d.get("guild")
    invite_type = _int(d.get("type"))
    if not isinstance(guild, dict) or not guild.get("id") or invite_type not in (None, 0):
        return InviteResult(code=code, fetched_at=fetched_at, http_status=200, invite_valid=0,
                            meta={"error": "not_guild_invite", "invite_type": invite_type},
                            error="not_guild_invite")
    profile = d.get("profile") if isinstance(d.get("profile"), dict) else {}
    members = _int(d.get("approximate_member_count"))
    if members is None:
        members = _int(profile.get("member_count"))
    presence = _int(d.get("approximate_presence_count"))
    if presence is None:
        presence = _int(profile.get("online_count"))
    ratio = presence / members if members and presence is not None and members > 0 else None
    expires_at = None
    if d.get("expires_at"):
        try:
            expires_at = parse_iso(str(d["expires_at"]))
        except ValueError:
            expires_at = None
    channel = d.get("channel") if isinstance(d.get("channel"), dict) else {}
    gid = str(guild["id"])
    try:
        created = snowflake_to_datetime(gid)
    except ValueError:
        created = None
    meta = {"guild_name": guild.get("name"), "vanity_url_code": guild.get("vanity_url_code"),
            "premium_tier": guild.get("premium_tier"), "nsfw_level": guild.get("nsfw_level"),
            "invite_type": invite_type, "invite_flags": d.get("flags"), "landing_channel_name": channel.get("name"),
            "profile_visibility": profile.get("visibility")}
    valid, err_code, error = 1, None, None
    if expires_at is not None and expires_at <= fetched_at:
        valid, err_code, error = 0, 50270, "expired_by_clock"
        meta["error"] = error
    feats = tuple(sorted(str(f) for f in guild.get("features") or ()))
    return InviteResult(code=code, fetched_at=fetched_at, http_status=200, invite_valid=valid,
                        invite_error_code=err_code, guild_id=gid, member_count=members, presence_count=presence,
                        presence_ratio=ratio, expires_at=expires_at,
                        landing_channel_id=str(channel["id"]) if channel.get("id") else None,
                        landing_channel_type=_int(channel.get("type")), features=feats,
                        boosts=_int(guild.get("premium_subscription_count")),
                        verification_level=_int(guild.get("verification_level")), guild_created_at=created,
                        meta=meta, error=error)


def parse_widget(guild_id: str, status: int | None, body: bytes | str | None, fetched_at: int) -> WidgetResult:
    """Keeps presence_count, the instant-invite code and the channel count; ``members[]`` is deleted first."""
    fetched_at = int(fetched_at)
    if status is None:
        return WidgetResult(guild_id, fetched_at, None, None, error="transport")
    d = _json(body)
    if isinstance(d, dict):
        d.pop("members", None)
    if status == 200 and isinstance(d, dict):
        code = None
        inv = d.get("instant_invite")
        if isinstance(inv, str):
            m = _INVITE_URL.search(inv)
            code = m.group(1) if m else None
        chans = d.get("channels")
        return WidgetResult(guild_id, fetched_at, 200, 1, presence_count=_int(d.get("presence_count")),
                            instant_invite_code=code, channel_count=len(chans) if isinstance(chans, list) else None)
    code = d.get("code") if isinstance(d, dict) else None
    if status == 403 and code == 50004:
        return WidgetResult(guild_id, fetched_at, 403, 0)
    if status == 404 and code == 10004:
        return WidgetResult(guild_id, fetched_at, 404, None, error="unknown_guild")
    return WidgetResult(guild_id, fetched_at, status, None, error=f"http_{status}")


class InvalidRequestGuard:
    """In-process count of 401/403/429 responses from ``discord`` over a trailing 10 min (Discord bans at 10,000)."""

    WINDOW = 600.0

    def __init__(self, limit_per_10min: int, clock: Callable[[], float] = time.monotonic) -> None:
        self.limit = int(limit_per_10min)
        self.clock = clock
        self._hits: deque[float] = deque()

    def _prune(self) -> None:
        cut = self.clock() - self.WINDOW
        while self._hits and self._hits[0] <= cut:
            self._hits.popleft()

    def record(self, status: int | None) -> None:
        if status in INVALID_STATUSES:
            self._hits.append(self.clock())

    def allowed(self) -> bool:
        self._prune()
        return len(self._hits) < self.limit

    def count(self) -> int:
        self._prune()
        return len(self._hits)


_guard: InvalidRequestGuard | None = None


def get_guard(limit: int | None = None) -> InvalidRequestGuard:
    """Process-wide singleton shared by the invite and widget jobs (the limit follows the current setting)."""
    global _guard
    import app.config as app_config
    lim = int(limit if limit is not None else app_config.settings.DISCORD_INVALID_GUARD_PER_10MIN)
    if _guard is None:
        _guard = InvalidRequestGuard(lim)
    _guard.limit = lim
    return _guard


def discord_headers(settings, *, with_bot_token: bool) -> dict[str, str]:
    h = {"User-Agent": settings.DISCORD_USER_AGENT, "Accept": "application/json"}
    if with_bot_token and settings.DISCORD_BOT_TOKEN and settings.DISCORD_INVITES_USE_BOT_TOKEN:
        h["Authorization"] = f"Bot {settings.DISCORD_BOT_TOKEN}"
    return h


def _is_json(resp) -> Any:
    try:
        return json.loads(resp.content)
    except (TypeError, ValueError):
        return None


async def fetch_invite(http: "Http", code: str, settings, *, guard: InvalidRequestGuard | None = None
                       ) -> InviteResult:
    """One invite lookup (§2.3). A JSON 429 → Retry-After honoured + exactly one retry; HTML 429 → CloudflareBan."""
    guard = guard or get_guard()
    url = f"{settings.DISCORD_API_BASE.rstrip('/')}/invites/{quote(code, safe='')}"
    params = {"with_counts": "true", "with_expiration": "true"}
    headers = discord_headers(settings, with_bot_token=True)
    for attempt in (1, 2):
        if not guard.allowed():
            return _failure(code, now_ts(), None, "invalid_guard")
        try:
            resp = await http.request("discord", "GET", url, params=params, headers=headers,
                                      ok_statuses=(200, 404, 429), follow_redirects=False, retries=None)
        except CloudflareBan:
            raise
        except ProviderError as exc:
            return _failure(code, now_ts(), exc.status, f"provider_error: {str(exc)[:120]}")
        status = resp.status_code
        guard.record(status)
        if status == 429:
            body = _is_json(resp)
            if not isinstance(body, dict):
                http.limiter.penalize("discord", 3600)
                logger.error("discord_cloudflare_ban", data={"code_len": len(code), "status": 429})
                raise CloudflareBan("discord", "non-JSON 429 (Cloudflare block)", status=429, url=url,
                                    attempts=attempt)
            ra = body.get("retry_after", 1.0)
            http.limiter.observe("discord", {"Retry-After": str(ra)}, host=DISCORD_HOST)
            if attempt == 1:
                continue
            return _failure(code, now_ts(), 429, "rate_limited")
        if status in (401, 403):
            return _failure(code, now_ts(), status, f"http_{status}")
        return parse_invite(code, status, resp.content, now_ts())
    return _failure(code, now_ts(), 429, "rate_limited")   # pragma: no cover


async def fetch_widget(http: "Http", guild_id: str, settings, *, guard: InvalidRequestGuard | None = None
                       ) -> WidgetResult:
    guard = guard or get_guard()
    if not guard.allowed():
        return WidgetResult(guild_id, now_ts(), None, None, error="invalid_guard")
    url = f"{settings.DISCORD_API_BASE.rstrip('/')}/guilds/{quote(str(guild_id), safe='')}/widget.json"
    try:
        resp = await http.request("discord", "GET", url, headers=discord_headers(settings, with_bot_token=False),
                                  ok_statuses=(200, 403, 404, 429), follow_redirects=False)
    except ProviderError as exc:
        return WidgetResult(guild_id, now_ts(), exc.status, None, error="provider_error")
    status = resp.status_code
    guard.record(status)
    if status == 429:
        body = _is_json(resp)
        if not isinstance(body, dict):
            http.limiter.penalize("discord", 3600)
            raise CloudflareBan("discord", "non-JSON 429 (Cloudflare block)", status=429, url=url, attempts=1)
        http.limiter.observe("discord", {"Retry-After": str(body.get("retry_after", 1.0))}, host=DISCORD_HOST)
        return WidgetResult(guild_id, now_ts(), 429, None, error="rate_limited")
    return parse_widget(guild_id, status, resp.content, now_ts())
