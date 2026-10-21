"""Spec 08 §9 #15–#17: CSV forms, sync/merge, re-occupation guard, per-run touch, PIT remap."""
from __future__ import annotations

import json

import pytest

from app.collectors.discord_channels import ChannelMap, load_channel_map, remap_unmapped, sync_channel_map
from app.registry.watch import WatchChannelsBuilder
from tests.collectors.community.conftest import DAY, MAIN, NOW, seed_occupant, seed_watch

CSV = """netuid,channel_id,channel_name,guild_id,kind,notes
,830075335084474390,📣・announce,799672011265015819,announce,main server global announcements (Followed)
15,1466080859164643389,15・oro・ο,799672011265015819,subnet_text,
106,1052226281472794735,announcements,1039241590092353596,announce,Nodexo own server, Followed, ok
12,1201941624243109888,12・compute
,1201941624243109999,bad,,subnet_text,
7,123,short-id,,,
"""


def test_load_csv_forms(tmp_path):
    p = tmp_path / "c.csv"
    p.write_text(CSV, encoding="utf-8")
    rows, errors = load_channel_map(p)
    by = {r.channel_id: r for r in rows}
    assert by["830075335084474390"].netuid is None and by["830075335084474390"].kind == "announce"
    assert by["1466080859164643389"].kind == "subnet_text" and by["1466080859164643389"].guild_id == MAIN
    assert by["1052226281472794735"].notes == "Nodexo own server, Followed, ok"      # unquoted commas re-joined
    assert by["1201941624243109888"].kind == "subnet_text" and by["1201941624243109888"].guild_id == MAIN  # 3 cols
    assert len(errors) == 2 and any("blank netuid" in e for e in errors) and any("channel_id" in e for e in errors)
    assert load_channel_map(tmp_path / "missing.csv") == ([], ["missing"])


@pytest.mark.asyncio
async def test_sync_merges_chain_row_and_reoccupation(db, tmp_path):
    o12 = await seed_occupant(db, 12)
    await seed_watch(db, o12, "discord_channel", "1201941624243109888", guild_id=MAIN)
    p = tmp_path / "c.csv"
    p.write_text("netuid,channel_id,channel_name,guild_id,kind,notes\n12,1201941624243109888,12・compute,,,\n")
    rows, _ = load_channel_map(p)
    res = await sync_channel_map(db, rows, NOW, "r")
    assert res["upserted"] == 1
    r = await db.fetchone("SELECT * FROM watch_channels WHERE handle = '1201941624243109888'")
    meta = json.loads(r["meta_json"])
    assert r["source"] == "chain" and r["priority"] == 10 and meta["kind"] == "subnet_text" and meta["declared_by"]
    assert meta["operator_since"] == NOW
    # re-occupation: the netuid gets a new occupant; the old operator row blocks a token-less row
    o15 = await seed_occupant(db, 15, reg=1000)
    p.write_text("netuid,channel_id,channel_name,guild_id,kind,notes\n15,1466080859164643389,oro,,,\n")
    rows, _ = load_channel_map(p)
    await sync_channel_map(db, rows, NOW, "r")
    new15 = await seed_occupant(db, 15, reg=5000)
    res = await sync_channel_map(db, rows, NOW + DAY, "r")
    assert res["upserted"] == 0 and "csv_row_stale_occupant" in res["stale_rows"][0]
    p.write_text("netuid,channel_id,channel_name,guild_id,kind,notes\n15,1466080859164643389,oro,,,reg=5000\n")
    rows, _ = load_channel_map(p)
    res = await sync_channel_map(db, rows, NOW + DAY, "r")
    assert res["upserted"] == 1
    assert await db.fetchval("SELECT COUNT(*) FROM watch_channels WHERE occupant_key = ? AND handle = "
                             "'1466080859164643389'", (new15,)) == 1
    assert o15 != new15


@pytest.mark.asyncio
async def test_touch_every_run_prevents_retirement(db, tmp_path, cset):
    o106 = await seed_occupant(db, 106)
    p = tmp_path / "c.csv"
    p.write_text("netuid,channel_id,channel_name,guild_id,kind\n106,1052226281472794735,ann,1039241590092353596,"
                 "announce\n")
    rows, _ = load_channel_map(p)
    for d in range(20):                                   # unchanged file, 20 simulated days of runs
        t = NOW + d * DAY
        await sync_channel_map(db, rows, t, "r")
        await WatchChannelsBuilder(db, t).retire_stale()
    r = await db.fetchone("SELECT last_seen_at, retired_at, first_seen_at FROM watch_channels WHERE occupant_key = ?",
                          (o106,))
    assert r["retired_at"] is None and r["last_seen_at"] == NOW + 19 * DAY and r["first_seen_at"] == NOW


@pytest.mark.asyncio
async def test_remap_unmapped_is_pit(db, tmp_path):
    from app.collectors.community_store import upsert_announcement
    from app.collectors.discord_gateway import AnnouncementRow
    o106 = await seed_occupant(db, 106)
    row = AnnouncementRow("discord:999", "discord", [], None, "1039241590092353596", "1052226281472794735", "999",
                          "55", "56", 1, "unmapped", None, 0, 0, "hello", "h" * 64, 5, NOW - 3600, NOW - 3500)
    await upsert_announcement(db, row, store_text=True, pending_days=7, run_id="gw:1")
    before = await db.fetchone("SELECT * FROM announcements WHERE msg_id = 'discord:999'")
    p = tmp_path / "c.csv"
    p.write_text("netuid,channel_id,channel_name,guild_id,kind\n106,1052226281472794735,ann,1039241590092353596,"
                 "announce\n")
    rows, _ = load_channel_map(p)
    cmap = await ChannelMap.build(db, rows, "830075335084474390")
    assert await remap_unmapped(db, cmap, NOW, "remap") == 1
    assert await remap_unmapped(db, cmap, NOW + 60, "remap") == 0          # idempotent
    m = await db.fetchone("SELECT * FROM social_post_mentions WHERE post_id = '999'")
    assert m["occupant_key"] == o106 and m["observed_at"] == NOW and m["match_kind"] == "team"
    assert await db.fetchone("SELECT * FROM announcements WHERE msg_id = 'discord:999'") == before   # never updated
