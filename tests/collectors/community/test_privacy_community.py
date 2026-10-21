"""Spec 08 §9 #35: no username-bearing value reaches the DB or the logs over every 08 path."""
from __future__ import annotations

import logging

import httpx
import pytest
import respx

from app.collectors.discord import run_invite_cycle, run_widget_pass
from app.collectors.discord_gateway import DiscordGatewayTask
from app.collectors.telegram import run_telegram_pass
from app.jobs import SPEC_BY_ID
from tests.collectors.community.conftest import (API, NOW, fixture_bytes, fixture_text, seed_identity, seed_occupant,
                                                 seed_watch)
from tests.collectors.community.fakes_discord import AGG, crosspost

FORBIDDEN = ("someone", "alice", "avatar", "inviter", "Some One", "tl_arrash", "WahooPredict Support")


async def all_text(db) -> str:
    out = []
    for t in ("discord_snapshots", "telegram_snapshots", "announcements", "metric_values", "events", "job_state",
              "collector_runs"):
        for r in await db.fetchall(f"SELECT * FROM {t}"):
            out += [str(v) for v in r.values() if isinstance(v, str)]
    return "\n".join(out)


@pytest.mark.asyncio
@respx.mock
async def test_privacy(rt, caplog, cset, frozen):
    import app.config as app_config
    s = cset(DB_PATH=app_config.settings.DB_PATH, DISCORD_AGGREGATOR_GUILD_ID=AGG)
    db, http = rt.db, rt.http
    caplog.set_level(logging.DEBUG)
    o = await seed_occupant(db, 106)
    o8 = await seed_occupant(db, 8)
    o30 = await seed_occupant(db, 30)
    for x in (o, o8, o30):
        await seed_identity(db, x, "discord", "present")
    await seed_watch(db, o, "discord_invite", "fwjBwxUcdX")
    await seed_watch(db, o, "discord_invite", "gdmInv", source="subnetalpha")
    await seed_watch(db, o8, "discord_handle", "tl_arrash")
    await seed_watch(db, o30, "telegram", "wahoopredict_support")
    respx.get(f"{API}/invites/fwjBwxUcdX").mock(return_value=httpx.Response(
        200, content=fixture_bytes("invite_own_server_nodexo.json")))
    respx.get(f"{API}/invites/gdmInv").mock(return_value=httpx.Response(200, content=fixture_bytes("invite_group_dm.json")))
    respx.get(f"{API}/invites/bittensor").mock(return_value=httpx.Response(
        200, content=fixture_bytes("invite_vanity_bittensor.json")))
    respx.get(f"{API}/guilds/1039241590092353596/widget.json").mock(return_value=httpx.Response(
        200, content=fixture_bytes("widget_enabled_cooking.json")))
    respx.get("https://t.me/wahoopredict_support").mock(return_value=httpx.Response(
        200, text=fixture_text("tme_user_wahoo.html")))
    await run_invite_cycle(db, http, now=NOW, run_id="r1", s=s)
    await run_widget_pass(db, http, now=NOW + 60, run_id="w1", s=s)
    await run_telegram_pass(db, http, now=NOW + 120, run_id="t1", s=s)
    task = DiscordGatewayTask(rt, SPEC_BY_ID["discord_gateway"])
    from tests.collectors.community.test_discord_gateway import gcfg
    await task.ingest(crosspost(10, 1039241590092353596, 1, 4242, "<@123> and <@&456> and <@!789> hi"), gcfg())
    blob = await all_text(db)
    logs = "\n".join(r.getMessage() + str(getattr(r, "data", "")) for r in caplog.records)
    for word in FORBIDDEN:
        assert word not in blob, word
        assert word not in logs, word
    assert await db.fetchval("SELECT COUNT(*) FROM announcements WHERE text IS NOT NULL AND via_own_server = 0 "
                             "AND platform = 'discord'") == 0
    assert await db.fetchval("SELECT COUNT(*) FROM announcements WHERE text GLOB '*<@[0-9]*' OR text GLOB "
                             "'*<@&[0-9]*' OR text GLOB '*<@!*'") == 0
    assert await db.fetchval("SELECT COUNT(*) FROM telegram_snapshots WHERE kind IN ('user_or_unknown','bot') AND "
                             "title IS NOT NULL") == 0
