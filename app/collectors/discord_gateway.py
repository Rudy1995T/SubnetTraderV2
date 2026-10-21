"""Aggregator-server Bot (spec 08 §2.6): a discord.py client that is a member of the operator's own server only and
reads the crossposts Discord delivers there from Followed announcement channels → ``announcements(platform='discord',
via_own_server=1)`` + team ``social_post_mentions``.

Fail-safe posture (whale collector idiom): liveness means "event loop healthy", never "Discord reachable"; a bad
token or a missing privileged intent is fatal and idles the task (one ``discord_gateway_fatal`` page) until the token
changes; everything else backs off in 01's ``LongTask`` supervisor. Operator chatter, foreign guilds and user ids
never reach storage: only crossposts (IS_CROSSPOST + reference + webhook) are stored, with mention ids redacted.
"""
from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.collectors import community_store as store
from app.collectors.discord_api import DISCORD_EPOCH_MS
from app.collectors.discord_channels import ChannelMap, load_channel_map, remap_unmapped
from app.jobs import LongTask
from app.logging.logger import logger
from app.util.time import floor_hour, now_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

JOB = "discord_gateway"
CHANNEL_FOLLOW_ADD = 12
_USER = re.compile(r"<@!?\d+>")
_ROLE = re.compile(r"<@&\d+>")
_CHAN = re.compile(r"<#\d+>")
_EMOJI = re.compile(r"<a?:\w+:\d+>")
_EVERY = re.compile(r"@(everyone|here)\b")
_URL = re.compile(r"https?://\S+")
BEAT_SEC = 30.0
MAP_RELOAD_SEC = 600.0


@dataclass(slots=True)
class AnnouncementRow:
    msg_id: str
    platform: str
    occupant_keys: list[str]
    netuid: int | None
    source_guild_id: str | None
    source_channel_id: str
    message_id: str
    local_channel_id: str | None
    local_message_id: str | None
    via_own_server: int
    mapping: str
    author_is_team: int | None
    has_attachment: int
    url_count: int
    text: str | None
    text_hash: str
    text_len: int
    event_at: int
    observed_at: int
    meta: dict = field(default_factory=dict)
    run_id: str | None = None


@dataclass(slots=True, frozen=True)
class GatewayCfg:
    aggregator_guild_id: str
    channel_ids: frozenset[str]
    main_announce_id: str
    backfill_days: int
    overlap_sec: int
    max_per_channel: int
    text_max_chars: int
    enumerate_follows: bool
    store_text: bool
    pending_days: int
    channels_path: str

    @classmethod
    def from_settings(cls, s=None) -> "GatewayCfg":
        s = s or app_config.settings
        ids = frozenset(x.strip() for x in str(s.DISCORD_AGGREGATOR_CHANNEL_IDS).split(",") if x.strip())
        return cls(str(s.DISCORD_AGGREGATOR_GUILD_ID).strip(), ids, str(s.DISCORD_MAIN_ANNOUNCE_CHANNEL_ID),
                   int(s.DISCORD_BACKFILL_DAYS), int(s.DISCORD_BACKFILL_OVERLAP_SEC),
                   int(s.DISCORD_BACKFILL_MAX_PER_CHANNEL), int(s.DISCORD_TEXT_MAX_CHARS),
                   bool(s.DISCORD_GATEWAY_ENUMERATE_FOLLOWS), bool(s.DISCORD_STORE_TEXT),
                   int(s.LLM_PENDING_TEXT_MAX_DAYS), str(s.path(s.DISCORD_CHANNELS_SEED_PATH)))


# ── pure helpers ─────────────────────────────────────────────────────────────────────────────────────────────
def redact_ids(text: str) -> str:
    """Stored text: ``<@id>`` → ``@user``, ``<@&id>`` → ``@role``, ``<#id>`` → ``#channel``."""
    return _CHAN.sub("#channel", _ROLE.sub("@role", _USER.sub("@user", text or "")))


def discord_clean(text: str) -> str:
    """Hash input: mention / channel / custom-emoji tokens and @everyone/@here removed."""
    s = _EMOJI.sub(" ", _CHAN.sub(" ", _ROLE.sub(" ", _USER.sub(" ", text or ""))))
    return _EVERY.sub(" ", s)


def compose_text(msg: Any) -> str:
    parts = [getattr(msg, "content", "") or ""]
    for e in getattr(msg, "embeds", None) or ():
        parts += [getattr(e, "title", None) or "", getattr(e, "description", None) or ""]
        for f in getattr(e, "fields", None) or ():
            parts += [getattr(f, "name", None) or "", getattr(f, "value", None) or ""]
        footer = getattr(e, "footer", None)
        parts.append(getattr(footer, "text", None) or "")
    return "\n".join(p for p in parts if p)


def _media_url(obj: Any) -> str | None:
    return getattr(obj, "url", None) if obj is not None else None


def _sid(x: Any) -> str | None:
    return None if x is None else str(getattr(x, "id", x))


def _ts(dt: Any) -> int:
    if isinstance(dt, datetime):
        return int((dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp())
    return int(dt)


def _netuid(okey: str) -> int | None:
    try:
        return int(str(okey).split(":", 1)[0])
    except ValueError:
        return None


def classify_message(msg: Any, cmap: ChannelMap, guild_map: dict[str, list[str]], cfg: GatewayCfg, now: int
                     ) -> AnnouncementRow | None:
    """§2.6 steps 1–9 (pure; duck-typed message so tests use fakes)."""
    guild = getattr(msg, "guild", None)
    if guild is None or _sid(guild) != cfg.aggregator_guild_id:
        return None
    mtype = getattr(getattr(msg, "type", None), "value", getattr(msg, "type", None))
    if mtype == CHANNEL_FOLLOW_ADD:
        return None
    flags = getattr(msg, "flags", None)
    ref = getattr(msg, "reference", None)
    if not (getattr(flags, "is_crossposted", False) and ref is not None and getattr(msg, "webhook_id", None)):
        return None                                                   # operator chatter: never stored
    src_guild = _sid(getattr(ref, "guild_id", None))
    src_channel = _sid(getattr(ref, "channel_id", None))
    src_msg = _sid(getattr(ref, "message_id", None))
    if not src_channel or not src_msg:
        return None
    if src_channel == cfg.main_announce_id or cmap.is_ecosystem(src_channel):
        occ, team, mapping = [], 0, "main_announce"
    elif cmap.occupants_for_channel(src_channel):
        occ, team, mapping = cmap.occupants_for_channel(src_channel), 1, "channel"
    elif src_guild and guild_map.get(src_guild):
        occ = list(guild_map[src_guild])
        team, mapping = 1, ("guild" if len(occ) == 1 else "guild_ambiguous")
    else:
        occ, team, mapping = [], None, "unmapped"
    composed = compose_text(msg)
    msg_id = f"discord:{src_msg}"
    text = redact_ids(composed)[:cfg.text_max_chars] or None
    urls = set(_URL.findall(composed))
    has_att = bool(getattr(msg, "attachments", None))
    for e in getattr(msg, "embeds", None) or ():
        if getattr(e, "url", None):
            urls.add(e.url)
        if any(_media_url(getattr(e, a, None)) for a in ("image", "video", "thumbnail")):
            has_att = True
    channel = getattr(msg, "channel", None)
    return AnnouncementRow(
        msg_id=msg_id, platform="discord", occupant_keys=occ, netuid=_netuid(occ[0]) if len(occ) == 1 else None,
        source_guild_id=src_guild, source_channel_id=src_channel, message_id=src_msg,
        local_channel_id=_sid(channel), local_message_id=_sid(getattr(msg, "id", None)), via_own_server=1,
        mapping=mapping, author_is_team=team, has_attachment=int(has_att), url_count=len(urls), text=text,
        text_hash=store.post_hash(discord_clean(composed), msg_id), text_len=len(composed),
        event_at=_ts(getattr(msg, "created_at", now)), observed_at=int(now),
        meta={"embed_count": len(getattr(msg, "embeds", None) or ())})


def snowflake_from_ts(ts: float) -> int:
    return max(0, int(ts * 1000) - DISCORD_EPOCH_MS) << 22


def ts_from_snowflake(sf: int | str) -> float:
    return ((int(sf) >> 22) + DISCORD_EPOCH_MS) / 1000


def token_hash(token: str) -> str:
    return hashlib.sha256((token or "").encode()).hexdigest()[:16]


@dataclass
class _Snow:
    id: int


# ── the LongTask ─────────────────────────────────────────────────────────────────────────────────────────────
class DiscordGatewayTask(LongTask):
    """01 LongTask; ``run_once`` owns one discord.py client session."""

    def __init__(self, rt, spec) -> None:
        super().__init__(rt, spec)
        self.cmap = ChannelMap()
        self.cmap_mtime: float | None = None
        self.guild_map: dict[str, list[str]] = {}
        self.guild_ok = False
        self.foreign_guilds: list[str] = []
        self.channels: list[str] = []
        self.follows: dict[str, Any] = {}             # follower webhooks (needs Manage Webhooks)
        self.follow_labels: dict[str, Any] = {}       # CHANNEL_FOLLOW_ADD system messages
        self.follows_lost: list[str] = []
        self.unmapped_channels: dict[str, str] = {}
        self.last_message_at: int | None = None
        self.backfill_last_at: int | None = None
        self._fatal: dict | None = None
        self._fatal_loaded = False
        self._hour: int | None = None
        self._hour_connected = 0.0
        self._hour_rows = 0
        self._last_tick: float | None = None
        self._session = 0
        self._client: Any = None
        self._warned: set[str] = set()

    # ── config / fatal state ───────────────────────────────────────────────────────────────────────────────
    def _token(self) -> str:
        return str(app_config.settings.DISCORD_BOT_TOKEN or "")

    def _fatal_blocks(self) -> bool:
        return bool(self._fatal and self._fatal.get("token_hash") == token_hash(self._token()))

    def enabled(self) -> bool:
        s = app_config.settings
        if not super().enabled() or not s.DISCORD_BOT_TOKEN or not str(s.DISCORD_AGGREGATOR_GUILD_ID).strip():
            return False
        return not self._fatal_blocks()

    async def _load_fatal(self) -> None:
        if not self._fatal_loaded and self.rt.db is not None:
            self._fatal = await store.get_task_state(self.rt.db, JOB, "fatal")
            self._fatal_loaded = True
            if self._fatal:
                self.fatal_error = self._fatal.get("error")

    async def _set_fatal(self, exc: BaseException) -> None:
        since = now_ts()
        err = f"{type(exc).__name__}: {exc}"[:300]
        self.fatal_error = err
        self._fatal = {"error": err, "since": since, "token_hash": token_hash(self._token())}
        self._fatal_loaded = True
        if self.rt.db is not None:
            await store.set_task_state(self.rt.db, JOB, "fatal", self._fatal)
        logger.error("discord_gateway_fatal", data={"error": err})
        if self.rt.alerts is not None:
            await self.rt.alerts.fire(
                "discord_gateway_fatal", f"⚠️ <b>Discord gateway stopped</b>: {err}\nFix the token / intent in the "
                "developer portal, then reload-config.", event_type="discord_gateway_fatal", severity="warning",
                payload={"error": err, "close_code": getattr(exc, "code", None), "retries": self.reconnects,
                         "since": since}, dedupe_key=str(since), source="community")

    async def _clear_fatal(self) -> None:
        if not self._fatal:
            return
        since = int(self._fatal.get("since") or now_ts())
        payload = {"collector": JOB, "failures": 1, "down_seconds": now_ts() - since}
        self._fatal = None
        self.fatal_error = None
        db = self.rt.db
        if db is not None:
            await db.execute("DELETE FROM job_state WHERE job = ? AND key = 'fatal'", (JOB,))
        alerts = self.rt.alerts
        if alerts is not None and alerts.is_latched("discord_gateway_fatal"):
            await alerts.clear("discord_gateway_fatal", "✅ Discord gateway recovered", event_type="collector_recovered",
                               payload=payload, dedupe_key=f"{JOB}|{since}")
        elif db is not None:
            from app.storage.events import emit_event
            await emit_event(db, event_type="collector_recovered", occupant_key=None, netuid=None, event_at=now_ts(),
                             observed_at=now_ts(), payload=payload, dedupe_key=f"{JOB}|{since}", source="runtime",
                             run_id=None)

    # ── maps ──────────────────────────────────────────────────────────────────────────────────────────────
    async def reload_map(self, *, force: bool = False) -> bool:
        cfg = GatewayCfg.from_settings()
        path = Path(cfg.channels_path)
        try:
            mtime = path.stat().st_mtime
        except OSError:
            mtime = None
        if not force and mtime == self.cmap_mtime:
            return False
        rows, errors = load_channel_map(path, str(app_config.settings.DISCORD_MAIN_GUILD_ID))
        if errors and errors != ["missing"]:
            logger.warning("discord_gateway_csv_errors", data={"count": len(errors)})
        db = self.rt.db
        self.cmap = await ChannelMap.build(db, rows, cfg.main_announce_id) if db is not None else ChannelMap()
        self.cmap_mtime = mtime
        if db is not None:
            n = await remap_unmapped(db, self.cmap, now_ts(), f"gw:{self._session}")
            if n:
                self.unmapped_channels = {k: v for k, v in self.unmapped_channels.items()
                                          if k.split(":", 1)[1] not in self.cmap.announce}
        return True

    async def refresh_guild_map(self) -> None:
        if self.rt.db is not None:
            self.guild_map = await store.primary_guild_map(self.rt.db, now_ts())

    # ── message handling ──────────────────────────────────────────────────────────────────────────────────
    async def ingest(self, msg: Any, cfg: GatewayCfg | None = None) -> bool:
        cfg = cfg or GatewayCfg.from_settings()
        db = self.rt.db
        guild = getattr(msg, "guild", None)
        gid = _sid(guild)
        if gid is None or gid != cfg.aggregator_guild_id:
            if gid is not None and gid not in self.foreign_guilds:
                self.foreign_guilds.append(gid)
            return False
        chan = _sid(getattr(msg, "channel", None))
        mtype = getattr(getattr(msg, "type", None), "value", getattr(msg, "type", None))
        if mtype == CHANNEL_FOLLOW_ADD:
            label = (getattr(msg, "content", "") or "")[:120]
            self.follow_labels[f"{chan}|{label}"] = {"channel_id": chan, "label": label, "at": now_ts()}
            if db is not None:
                await store.set_task_state(db, JOB, "follow_labels", self.follow_labels)
            logger.info("discord_gateway_follow_added", data={"channel_id": chan, "label": label})
            await self._advance(chan, getattr(msg, "id", None))
            return False
        row = classify_message(msg, self.cmap, self.guild_map, cfg, now_ts())
        await self._advance(chan, getattr(msg, "id", None))
        if row is None or db is None:
            return False
        if row.mapping == "unmapped":
            key = f"{row.source_guild_id}:{row.source_channel_id}"
            if key not in self.unmapped_channels:
                label = str(getattr(getattr(msg, "author", None), "name", "") or "")[:100]
                self.unmapped_channels[key] = label                # webhook display name = "Guild #channel" label
                logger.info("discord_gateway_unmapped", data={"source_guild_id": row.source_guild_id,
                                                              "source_channel_id": row.source_channel_id,
                                                              "label": label})
        inserted = await store.upsert_announcement(db, row, store_text=cfg.store_text, pending_days=cfg.pending_days,
                                                   run_id=f"gw:{self._session}")
        if inserted:
            self._hour_rows += 1
            self.last_message_at = now_ts()
        return inserted

    async def _advance(self, channel_id: str | None, msg_id: Any) -> None:
        if channel_id is None or msg_id is None or self.rt.db is None:
            return
        key = f"wm:{channel_id}"
        cur = await store.get_task_state(self.rt.db, JOB, key)
        if cur is None or int(msg_id) > int(cur):
            await store.set_task_state(self.rt.db, JOB, key, str(msg_id))

    async def on_raw_delete(self, payload: Any, cfg: GatewayCfg | None = None) -> None:
        cfg = cfg or GatewayCfg.from_settings()
        if _sid(getattr(payload, "guild_id", None)) != cfg.aggregator_guild_id or self.rt.db is None:
            return
        await store.mark_deleted(self.rt.db, str(getattr(payload, "message_id")), now_ts())

    async def backfill_channel(self, channel: Any, cfg: GatewayCfg | None = None, *, object_factory=None) -> int:
        """Watermark − overlap (first time: now − DISCORD_BACKFILL_DAYS), oldest first, capped (whale port)."""
        cfg = cfg or GatewayCfg.from_settings()
        db = self.rt.db
        cid = _sid(channel)
        wm = await store.get_task_state(db, JOB, f"wm:{cid}") if db is not None else None
        capped_before = bool(await store.get_task_state(db, JOB, f"bfcap:{cid}")) if db is not None else False
        if wm and capped_before:
            after_id = int(wm)                         # a capped page continues exactly where it stopped
            after_ts = ts_from_snowflake(wm)
        elif wm:
            after_ts = ts_from_snowflake(wm) - cfg.overlap_sec
            after_id = snowflake_from_ts(after_ts)
        else:
            after_ts = now_ts() - cfg.backfill_days * 86400
            after_id = snowflake_from_ts(after_ts)
        mk = object_factory or _Snow
        after = mk(id=after_id)
        n = 0
        async for m in channel.history(after=after, limit=cfg.max_per_channel, oldest_first=True):
            await self.ingest(m, cfg)
            n += 1
        capped = n >= cfg.max_per_channel
        if capped:
            logger.warning("discord_gateway_backfill_capped", data={"channel_id": cid, "cap": n})
        if db is not None and capped != capped_before:
            await store.set_task_state(db, JOB, f"bfcap:{cid}", capped)
        self.backfill_last_at = now_ts()
        logger.info("discord_gateway_backfill", data={"channel_id": _sid(channel), "messages": n,
                                                      "after": to_iso(after_ts)})
        return n

    async def on_ready(self, client: Any, cfg: GatewayCfg | None = None) -> None:
        cfg = cfg or GatewayCfg.from_settings()
        guilds = list(getattr(client, "guilds", []) or [])
        self.foreign_guilds = sorted(_sid(g) for g in guilds if _sid(g) != cfg.aggregator_guild_id)
        for g in self.foreign_guilds:
            if f"foreign:{g}" not in self._warned:
                self._warned.add(f"foreign:{g}")
                logger.warning("discord_gateway_foreign_guild", data={"guild_id": g})
        guild = next((g for g in guilds if _sid(g) == cfg.aggregator_guild_id), None)
        if guild is None:
            self.guild_ok = False
            self.last_error = "bot_not_in_aggregator_guild"
            logger.warning("discord_gateway_not_in_aggregator_guild", data={"guild_id": cfg.aggregator_guild_id})
            return
        self.guild_ok = True
        self.last_error = None
        self.mark_connected(True)
        await self._clear_fatal()
        await self.refresh_guild_map()
        me = getattr(guild, "me", None)
        chans = []
        for c in getattr(guild, "text_channels", []) or []:
            if cfg.channel_ids and _sid(c) not in cfg.channel_ids:
                continue
            perms = c.permissions_for(me) if me is not None and hasattr(c, "permissions_for") else None
            if perms is not None and not (perms.read_message_history and perms.view_channel):
                continue
            chans.append(c)
        self.channels = [_sid(c) for c in chans]
        if cfg.enumerate_follows:
            await self._enumerate_follows(chans)
        try:
            import discord
            factory = discord.Object
        except ImportError:  # pragma: no cover
            factory = _Snow
        for c in chans:
            try:
                await self.backfill_channel(c, cfg, object_factory=factory)
            except Exception as exc:  # one channel's history error must not stop the session
                logger.warning("discord_gateway_backfill_failed", data={"channel_id": _sid(c),
                                                                        "error": f"{type(exc).__name__}: {exc}"})

    async def _enumerate_follows(self, chans: list[Any]) -> None:
        current: dict[str, Any] = {}
        for c in chans:
            try:
                hooks = await c.webhooks()
            except Exception as exc:                       # discord.Forbidden without Manage Webhooks
                if "follows" not in self._warned:
                    self._warned.add("follows")
                    logger.info("discord_gateway_follows_unavailable", data={"error": type(exc).__name__})
                return
            for w in hooks:
                if getattr(getattr(w, "type", None), "value", None) != 2:      # 2 = channel follower
                    continue
                sg, sc = getattr(w, "source_guild", None), getattr(w, "source_channel", None)
                key = f"{_sid(sg)}:{_sid(sc)}"
                current[key] = {"local_channel_id": _sid(c), "source_guild_id": _sid(sg),
                                "source_guild": getattr(sg, "name", None), "source_channel_id": _sid(sc),
                                "source_channel": getattr(sc, "name", None)}
        self.follows_lost = sorted(set(self.follows) - set(current))
        self.follows = current
        if self.rt.db is not None:
            await store.set_task_state(self.rt.db, JOB, "follows", self.follows)

    # ── heartbeat ─────────────────────────────────────────────────────────────────────────────────────────
    async def tick(self, connected: bool, now: float | None = None) -> None:
        """Beat bookkeeping: connected seconds per UTC hour; a completed hour writes its heartbeat row."""
        now = float(now if now is not None else now_ts())
        self.beat()
        if self._last_tick is not None and connected:
            self._hour_connected += max(0.0, min(now - self._last_tick, 2 * BEAT_SEC))
        self._last_tick = now
        hour = floor_hour(int(now))
        if self._hour is None:
            self._hour = hour
        elif hour != self._hour:
            await self.flush_hour()
            self._hour, self._hour_connected, self._hour_rows = hour, 0.0, 0

    async def flush_hour(self) -> None:
        if self._hour is None or self.rt.db is None:
            return
        ok = self._hour_connected >= 60
        await store.write_gateway_heartbeat(self.rt.db, self._hour, ok, self._hour_rows,
                                            None if ok else (self.last_error or "disconnected"))

    async def _beat_loop(self, stop: asyncio.Event, client: Any) -> None:
        last_map = 0.0
        while not stop.is_set():
            connected = bool(client.is_ready() and not client.is_closed())
            if connected != self.connected:
                self.mark_connected(connected)
            try:
                await self.tick(connected and self.guild_ok)
                mono = asyncio.get_running_loop().time()
                if mono - last_map >= MAP_RELOAD_SEC:
                    last_map = mono
                    await self.reload_map()
                    await self.refresh_guild_map()
            except Exception as exc:
                logger.warning("discord_gateway_beat_error", data={"error": f"{type(exc).__name__}: {exc}"})
            await self._wait(stop, BEAT_SEC)

    # ── session ───────────────────────────────────────────────────────────────────────────────────────────
    def make_client(self):  # pragma: no cover - real network client
        import discord
        intents = discord.Intents.none()
        intents.guilds = True
        intents.guild_messages = True
        intents.message_content = True
        return discord.Client(intents=intents, max_messages=None, chunk_guilds_at_startup=False,
                              member_cache_flags=discord.MemberCacheFlags.none())

    async def run_once(self, stop: asyncio.Event) -> None:
        await self._load_fatal()
        if self._fatal_blocks():
            return
        import discord
        cfg = GatewayCfg.from_settings()
        self._session = now_ts()
        await self.reload_map(force=True)
        await self.refresh_guild_map()
        client = self.make_client()
        self._client = client
        task = self

        @client.event
        async def on_ready():  # noqa: ANN202
            await task.on_ready(client, cfg)

        @client.event
        async def on_message(message):  # noqa: ANN001, ANN202
            try:
                await task.ingest(message, cfg)
            except Exception as exc:
                logger.warning("discord_gateway_ingest_failed", data={"error": f"{type(exc).__name__}: {exc}"})

        @client.event
        async def on_raw_message_delete(payload):  # noqa: ANN001, ANN202
            await task.on_raw_delete(payload, cfg)

        @client.event
        async def on_guild_join(guild):  # noqa: ANN001, ANN202
            if _sid(guild) != cfg.aggregator_guild_id:
                task.foreign_guilds = sorted(set(task.foreign_guilds) | {_sid(guild)})
                logger.warning("discord_gateway_foreign_guild", data={"guild_id": _sid(guild)})

        @client.event
        async def on_guild_remove(guild):  # noqa: ANN001, ANN202
            if _sid(guild) == cfg.aggregator_guild_id:
                task.guild_ok = False
                task.last_error = "bot_not_in_aggregator_guild"

        @client.event
        async def on_disconnect():  # noqa: ANN202
            task.mark_connected(False)

        @client.event
        async def on_resumed():  # noqa: ANN202
            task.mark_connected(task.guild_ok)

        start = asyncio.create_task(client.start(self._token(), reconnect=True))
        beat = asyncio.create_task(self._beat_loop(stop, client))
        stopper = asyncio.create_task(stop.wait())
        try:
            done, _ = await asyncio.wait({start, stopper}, return_when=asyncio.FIRST_COMPLETED)
            if start in done and start.exception() is not None:
                exc = start.exception()
                if isinstance(exc, (discord.LoginFailure, discord.PrivilegedIntentsRequired)):
                    await self._set_fatal(exc)
                    return
                raise exc
        finally:
            for t in (beat, stopper):
                t.cancel()
            try:
                await client.close()
            except Exception:  # pragma: no cover
                pass
            if not start.done():
                start.cancel()
            self.mark_connected(False)
            try:
                await self.flush_hour()
            except Exception:  # pragma: no cover
                pass

    def status(self) -> dict:
        out = super().status()
        if not app_config.settings.DISCORD_BOT_TOKEN:
            out["disabled_reason"] = "no_token"
        out.update({"guild_ok": self.guild_ok, "channels": self.channels, "follows": list(self.follows.values()),
                    "follow_labels": [v["label"] for v in self.follow_labels.values()],
                    "follows_lost": self.follows_lost,
                    "unmapped_channels": [{"source": k, "label": v} for k, v in self.unmapped_channels.items()],
                    "foreign_guilds": self.foreign_guilds, "last_message_at": to_iso(self.last_message_at),
                    "backfill_last_at": to_iso(self.backfill_last_at), "fatal_error": self.fatal_error,
                    "csv_announce_channels": len(self.cmap.announce)})
        return out

