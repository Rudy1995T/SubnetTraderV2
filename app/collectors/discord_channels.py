"""Operator channel map ``seed/discord_channels.csv`` → ``watch_channels`` (spec 08 §2.8).

Columns ``netuid,channel_id,channel_name,guild_id,kind[,notes]`` (3-column rows still load; fields after the 6th are
re-joined into ``notes``). The loader is the whale ``load_cohort`` fail-safe: it never raises. The CSV is parsed and
synced on **every** ``discord_6h`` run (cheap, idempotent): a restart must not leave the rows untouched long enough
for 04's 14-day retirement. A ``reg=<network_registered_at>`` token in ``notes`` binds a row to one occupant.
"""
from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from app.logging.logger import logger

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

HEADER = ["netuid", "channel_id", "channel_name", "guild_id", "kind", "notes"]
_SNOWFLAKE = re.compile(r"^\d{17,20}$")
_REG = re.compile(r"\breg=(\d+)\b")
OPERATOR_PRIORITY = 45


@dataclass(slots=True, frozen=True)
class ChannelMapRow:
    netuid: int | None
    channel_id: str
    channel_name: str
    guild_id: str
    kind: Literal["announce", "subnet_text"]
    registered_at: int | None
    notes: str
    line: int = 0


def load_channel_map(path: Path, main_guild_id: str = "799672011265015819"
                     ) -> tuple[list[ChannelMapRow], list[str]]:
    """Never raises: missing/unreadable → ``([], [reason])``; malformed rows are skipped and reported."""
    try:
        text = Path(path).read_text(encoding="utf-8-sig")
    except FileNotFoundError:
        return [], ["missing"]
    except (OSError, ValueError) as exc:
        return [], [f"unreadable: {type(exc).__name__}"]
    rows: list[ChannelMapRow] = []
    errors: list[str] = []
    for lineno, rec in enumerate(csv.reader(text.splitlines()), start=1):
        if not rec or not any(x.strip() for x in rec) or rec[0].strip().startswith("#"):
            continue
        if lineno == 1 and rec[0].strip().lower() == "netuid":
            continue
        try:
            rows.append(_parse_row(rec, lineno, main_guild_id))
        except ValueError as exc:
            errors.append(f"line {lineno}: {exc}")
    return rows, errors


def _parse_row(rec: list[str], lineno: int, main_guild_id: str) -> ChannelMapRow:
    if len(rec) > 6:
        rec = rec[:5] + [",".join(rec[5:])]
    rec = [x.strip() for x in rec]
    if len(rec) < 3:
        raise ValueError("need at least netuid,channel_id,channel_name")
    rec += [""] * (6 - len(rec))
    netuid_s, channel_id, name, guild_id, kind, notes = rec
    netuid = None
    if netuid_s:
        if not netuid_s.isdigit() or not (0 <= int(netuid_s) <= 1024):
            raise ValueError(f"bad netuid {netuid_s!r}")
        netuid = int(netuid_s)
    if not _SNOWFLAKE.match(channel_id):
        raise ValueError("bad channel_id (want a 17-20 digit snowflake)")
    guild_id = guild_id or main_guild_id
    if not _SNOWFLAKE.match(guild_id):
        raise ValueError("bad guild_id (want a 17-20 digit snowflake)")
    kind = kind or ("subnet_text" if guild_id == main_guild_id else "announce")
    if kind not in ("announce", "subnet_text"):
        raise ValueError(f"bad kind {kind!r} (announce|subnet_text)")
    if netuid is None and kind != "announce":
        raise ValueError("blank netuid is allowed only for kind=announce (ecosystem channel)")
    m = _REG.search(notes)
    return ChannelMapRow(netuid, channel_id, name, guild_id, kind, int(m.group(1)) if m else None, notes, lineno)


@dataclass
class ChannelMap:
    """In-memory map used by the gateway: announce channel id → occupant keys; ecosystem channel ids."""
    announce: dict[str, list[str]] = field(default_factory=dict)
    ecosystem: set[str] = field(default_factory=set)
    rows: int = 0

    def occupants_for_channel(self, channel_id: str) -> list[str]:
        return list(self.announce.get(str(channel_id), []))

    def is_ecosystem(self, channel_id: str) -> bool:
        return str(channel_id) in self.ecosystem

    @classmethod
    async def build(cls, db: "Database", rows: list[ChannelMapRow], main_announce_id: str) -> "ChannelMap":
        cm = cls(ecosystem={str(main_announce_id)} if main_announce_id else set(), rows=len(rows))
        bound, _ = await bind_rows(db, rows)
        for r in rows:
            if r.kind == "announce" and r.netuid is None:
                cm.ecosystem.add(r.channel_id)
        for r, okey in bound:
            if r.kind == "announce":
                lst = cm.announce.setdefault(r.channel_id, [])
                if okey not in lst:
                    lst.append(okey)
        return cm


async def bind_rows(db: "Database", rows: list[ChannelMapRow]) -> tuple[list[tuple[ChannelMapRow, str]], list[str]]:
    """Bind rows with a netuid to the netuid's current occupant (re-occupation guard, §2.8)."""
    from app.registry import current_occupant
    bound: list[tuple[ChannelMapRow, str]] = []
    stale: list[str] = []
    cache: dict[int, object] = {}
    for r in rows:
        if r.netuid is None:
            continue
        if r.netuid not in cache:
            cache[r.netuid] = await current_occupant(db, r.netuid)
        occ = cache[r.netuid]
        if occ is None or getattr(occ, "status", None) not in ("active", "pre_launch"):
            stale.append(f"line {r.line}: no active occupant for netuid {r.netuid}")
            continue
        if r.registered_at is not None:
            if int(occ.network_registered_at) != r.registered_at:
                stale.append(f"line {r.line}: csv_row_stale_occupant (reg={r.registered_at})")
                continue
        else:
            older = await db.fetchval(
                "SELECT 1 FROM watch_channels WHERE platform = 'discord_channel' AND source = 'operator' AND "
                "handle = ? AND netuid = ? AND occupant_key IS NOT NULL AND occupant_key <> ? LIMIT 1",
                (r.channel_id, r.netuid, occ.occupant_key))
            if older:
                stale.append(f"line {r.line}: csv_row_stale_occupant (add reg={occ.network_registered_at} to notes)")
                continue
        bound.append((r, occ.occupant_key))
    for s in stale:
        logger.warning("discord_csv_row_skipped", data={"reason": s})
    return bound, stale


async def sync_channel_map(db: "Database", rows: list[ChannelMapRow], now: int, run_id: str,
                           main_guild_id: str = "799672011265015819") -> dict:
    """Upsert operator rows (insert-only ``first_seen_at``; a conflicting 04 row keeps its source/priority)."""
    bound, stale = await bind_rows(db, rows)
    ids: list[int] = []
    for r, okey in bound:
        meta = {"kind": r.kind, "channel_name": r.channel_name, "csv_netuid": r.netuid}
        url = f"https://discord.com/channels/{r.guild_id}/{r.channel_id}"
        is_main = int(r.guild_id == main_guild_id)
        await db.execute(
            "INSERT OR IGNORE INTO watch_channels (occupant_key, netuid, platform, handle, url, source, priority, "
            "is_resolved, is_main_guild, guild_id, first_seen_at, last_seen_at, meta_json) "
            "VALUES (?,?,'discord_channel',?,?,'operator',?,0,?,?,?,?,?)",
            (okey, r.netuid, r.channel_id, url, OPERATOR_PRIORITY, is_main, r.guild_id, now, now, json.dumps(meta)))
        await db.execute(
            "UPDATE watch_channels SET last_seen_at = ?, retired_at = NULL, guild_id = COALESCE(guild_id, ?), "
            "is_main_guild = ?, meta_json = json_patch(json_set(COALESCE(meta_json, '{}'), '$.operator_since', "
            "COALESCE(json_extract(meta_json, '$.operator_since'), ?)), ?) "
            "WHERE occupant_key = ? AND platform = 'discord_channel' AND handle = ?",
            (now, r.guild_id, is_main, now, json.dumps(meta), okey, r.channel_id))
        wid = await db.fetchval("SELECT id FROM watch_channels WHERE occupant_key = ? AND platform = "
                                "'discord_channel' AND handle = ?", (okey, r.channel_id), writer=True)
        if wid is not None:
            ids.append(int(wid))
    return {"upserted": len(ids), "stale": len(stale), "stale_rows": stale, "watch_ids": ids,
            "ecosystem": sum(1 for r in rows if r.netuid is None)}


async def remap_unmapped(db: "Database", cmap: ChannelMap, now: int, run_id: str | None = None) -> int:
    """Add ``social_post_mentions(match_kind='team')`` for announcements stored as ``unmapped`` whose source channel
    is now mapped. ``observed_at = now`` (PIT); the ``announcements`` row is never updated."""
    if not cmap.announce:
        return 0
    ph = ",".join("?" * len(cmap.announce))
    rows = await db.fetchall(
        f"SELECT msg_id, message_id, source_channel_id, event_at FROM announcements WHERE platform = 'discord' "
        f"AND mapping = 'unmapped' AND source_channel_id IN ({ph})", tuple(cmap.announce))
    n = 0
    for r in rows:
        for okey in cmap.occupants_for_channel(r["source_channel_id"]):
            res = await db.execute(
                "INSERT OR IGNORE INTO social_post_mentions (platform, post_id, occupant_key, match_kind, event_at, "
                "observed_at, run_id) VALUES ('discord',?,?,'team',?,?,?)",
                (str(r["message_id"]), okey, r["event_at"], now, run_id or f"remap:{now}"))
            n += max(0, res.rowcount)
    if n:
        logger.info("discord_remap_unmapped", data={"mentions": n})
    return n


async def export_known_channels(db: "Database", path: Path, main_guild_id: str = "799672011265015819") -> int:
    """Append the known main-server channel ids (04's chain/subnetalpha/tmc deep links) missing from the CSV."""
    path = Path(path)
    existing, _ = load_channel_map(path, main_guild_id)
    have = {r.channel_id for r in existing}
    rows = await db.fetchall(
        "SELECT w.netuid, w.handle, w.source, o.network_registered_at FROM watch_channels w JOIN occupants o "
        "USING (occupant_key) WHERE w.platform = 'discord_channel' AND w.retired_at IS NULL AND w.guild_id = ? "
        "AND o.status IN ('active','pre_launch') ORDER BY w.netuid", (main_guild_id,))
    new = [r for r in rows if r["handle"] not in have]
    if not new:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(HEADER)
        for r in new:
            w.writerow([r["netuid"], r["handle"], "", main_guild_id, "subnet_text",
                        f"from {r['source']} reg={r['network_registered_at']}"])
            have.add(r["handle"])
    return len(new)
