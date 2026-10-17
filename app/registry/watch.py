"""``watch_channels`` target list (spec 04 §2.9). Insert-or-ignore then update the registry-owned columns only;
never touches ``handle``, ``verified_at``, ``guild_id``, ``needs_resolution`` or other collectors' ``meta_json`` keys."""
from __future__ import annotations

import json
from typing import TYPE_CHECKING

import app.config as app_config
from app.registry.merge import WATCH_PRIORITY, Merger
from app.registry.models import Occupant, Resolved, WatchChannel

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

WATCH_FIELDS = ("discord", "x_url", "telegram", "youtube", "subnet_url", "docs_url")
KIND_PLATFORM = {"discord_invite": "discord_invite", "discord_channel": "discord_channel",
                 "discord_handle": "discord_handle", "x": "x", "x_handle": "x", "telegram_public": "telegram",
                 "telegram_private": "telegram", "youtube_channel": "youtube", "youtube_handle": "youtube",
                 "youtube_video": "youtube_video", "website": "website", "docs": "docs"}
RETIRE_SOURCES = ("chain", "subnetalpha", "tmc", "tmc_coldkey", "manual", "legacy", "operator")


def _target(field: str, kind: str | None, handle: str | None, value_norm: str | None) -> tuple[str, str, str | None] | None:
    if not kind:
        return None
    platform = KIND_PLATFORM.get(kind)
    if platform is None:
        return None
    if field == "docs_url" and platform == "website":
        platform = "docs"
    if platform in ("website", "docs"):
        return platform, value_norm or "", value_norm
    if not handle:
        return None
    url = value_norm if value_norm and value_norm.startswith("http") else None
    return platform, handle, url


class WatchChannelsBuilder:
    def __init__(self, db: "Database", now: int) -> None:
        self.db = db
        self.now = now

    async def rebuild(self, occ: Occupant, resolved: dict[str, Resolved]) -> int:
        targets: dict[tuple[str, str], dict] = {}
        for field in WATCH_FIELDS:
            r = resolved.get(field)
            if r is None:
                continue
            for c in r.cands:
                if c.is_junk or c.suppressed or not c.value:
                    continue
                t = _target(field, c.ref_kind, c.handle, c.value_norm)
                if not t or not t[1]:
                    continue
                platform, handle, url = t
                prio = 5 if c.mode == "force" else WATCH_PRIORITY.get(c.source, 50)
                d = targets.setdefault((platform, handle), {"url": url, "source": c.source, "priority": prio,
                                                            "declared_by": set(), "resolved": False,
                                                            "extra": dict(c.extra), "kind": c.ref_kind})
                d["declared_by"].add(c.source)
                if prio < d["priority"]:
                    d.update(source=c.source, priority=prio)
                if r.status == "present" and r.value_norm == c.value_norm:
                    d["resolved"] = True
        for (platform, handle), d in targets.items():
            ex = d["extra"]
            is_main = int(bool(ex.get("is_main_vanity") or ex.get("is_main_guild")))
            guild = ex.get("guild_id")
            needs = int(platform in ("youtube", "youtube_video") and bool(ex.get("needs_resolution", 0)))
            meta = {"declared_by": sorted(d["declared_by"])}
            await self.db.execute(
                "INSERT OR IGNORE INTO watch_channels (occupant_key, netuid, platform, handle, url, source, priority, "
                "is_resolved, is_main_guild, guild_id, needs_resolution, first_seen_at, last_seen_at, meta_json) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (occ.occupant_key, occ.netuid, platform, handle, d["url"], d["source"], d["priority"],
                 int(d["resolved"]), is_main, guild, needs, self.now, self.now, json.dumps(meta)))
            await self.db.execute(
                "UPDATE watch_channels SET last_seen_at = ?, is_resolved = ?, source = ?, priority = ?, "
                "retired_at = NULL, meta_json = json_set(COALESCE(meta_json, '{}'), '$.declared_by', json(?)) "
                "WHERE occupant_key = ? AND platform = ? AND handle = ?",
                (self.now, int(d["resolved"]), d["source"], d["priority"], json.dumps(meta["declared_by"]),
                 occ.occupant_key, platform, handle))
        return len(targets)

    async def retire_stale(self) -> int:
        cutoff = self.now - app_config.settings.REGISTRY_WATCH_RETIRE_DAYS * 86400
        ph = ",".join("?" * len(RETIRE_SOURCES))
        res = await self.db.execute(
            f"UPDATE watch_channels SET retired_at = ? WHERE retired_at IS NULL AND occupant_key IS NOT NULL AND "
            f"source IN ({ph}) AND last_seen_at < ?", (self.now, *RETIRE_SOURCES, cutoff))
        return res.rowcount


async def watch_targets(db: "Database", platform: str, only_active: bool = True) -> list[WatchChannel]:
    q = "SELECT * FROM watch_channels WHERE platform = ?" + (" AND retired_at IS NULL" if only_active else "")
    out = []
    for r in await db.fetchall(q + " ORDER BY netuid, priority", (platform,)):
        try:
            meta = json.loads(r["meta_json"]) if r["meta_json"] else {}
        except ValueError:
            meta = {}
        out.append(WatchChannel(r["id"], r["occupant_key"], r["netuid"], r["platform"], r["handle"], r["url"],
                                r["source"], r["priority"], bool(r["is_resolved"]), bool(r["is_main_guild"]),
                                r["guild_id"], bool(r["needs_resolution"]), r["verified_at"], meta))
    return out


async def rebuild_for(db: "Database", occ: Occupant, now: int, run_id: str) -> int:
    res = await Merger(db, run_id, now).compute(occ)
    return await WatchChannelsBuilder(db, now).rebuild(occ, res)
