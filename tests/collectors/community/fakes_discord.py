"""Duck-typed discord.py stand-ins for the gateway tests (spec 08 §9 fakes_discord.py)."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

AGG = "1100000000000000001"


@dataclass
class Obj:
    id: int
    name: str | None = None


@dataclass
class Flags:
    is_crossposted: bool = False


@dataclass
class Ref:
    guild_id: int | None
    channel_id: int
    message_id: int


@dataclass
class Embed:
    title: str | None = None
    description: str | None = None
    fields: list = field(default_factory=list)
    footer: Any = None
    url: str | None = None
    image: Any = None
    video: Any = None
    thumbnail: Any = None


@dataclass
class Field:
    name: str
    value: str


@dataclass
class Footer:
    text: str


@dataclass
class Media:
    url: str


@dataclass
class FakeMessage:
    id: int
    guild: Obj | None
    channel: Obj
    type: int = 0
    flags: Flags = field(default_factory=Flags)
    reference: Ref | None = None
    webhook_id: int | None = None
    content: str = ""
    embeds: list = field(default_factory=list)
    attachments: list = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime(2026, 10, 16, 12, 0, tzinfo=timezone.utc))
    author: Obj = field(default_factory=lambda: Obj(1, "Some Guild #announcements"))


def crosspost(local_id: int, src_guild: int, src_channel: int, src_msg: int, content: str = "", *,
              embeds: list | None = None, guild: str = AGG, channel: int = 500, created: datetime | None = None,
              attachments: list | None = None) -> FakeMessage:
    return FakeMessage(id=local_id, guild=Obj(int(guild)), channel=Obj(channel), flags=Flags(True),
                       reference=Ref(src_guild, src_channel, src_msg), webhook_id=777, content=content,
                       embeds=embeds or [], attachments=attachments or [],
                       created_at=created or datetime(2026, 10, 16, 12, 0, tzinfo=timezone.utc))


class FakeChannel:
    def __init__(self, cid: int, messages: list[FakeMessage]) -> None:
        self.id = cid
        self.messages = messages
        self.calls: list[dict] = []

    async def history(self, *, after=None, limit=100, oldest_first=True):
        self.calls.append({"after": getattr(after, "id", None), "limit": limit})
        n = 0
        for m in sorted(self.messages, key=lambda m: m.id):
            if after is not None and m.id <= after.id:
                continue
            if n >= limit:
                return
            n += 1
            yield m


@dataclass
class RawDelete:
    guild_id: int
    channel_id: int
    message_id: int
