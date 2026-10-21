"""Spec 08 §9 #21–#25: crosspost classification, mapping, privacy, backfill, fatal handling, heartbeats."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

import app.config as app_config
from app.collectors.discord_channels import ChannelMap
from app.collectors.discord_gateway import (DiscordGatewayTask, GatewayCfg, classify_message, discord_clean,
                                            snowflake_from_ts, ts_from_snowflake)
from app.collectors.community_store import post_hash
from app.jobs import SPEC_BY_ID
from app.nlp.textnorm import text_hash, text_norm
from tests.collectors.community.conftest import NOW, seed_occupant
from tests.collectors.community.fakes_discord import (AGG, Embed, FakeChannel, FakeMessage, Field, Flags, Footer, Media,
                                                      Obj, RawDelete, crosspost)

OWN_G, OWN_C = 1039241590092353596, 1052226281472794735
MAIN_ANN = 830075335084474390


def gcfg(**kw):
    s = app_config.settings
    base = dict(aggregator_guild_id=AGG, channel_ids=frozenset(), main_announce_id=str(MAIN_ANN), backfill_days=14,
                overlap_sec=600, max_per_channel=2000, text_max_chars=4000, enumerate_follows=True,
                store_text=bool(s.DISCORD_STORE_TEXT), pending_days=7, channels_path="/nonexistent.csv")
    base.update(kw)
    return GatewayCfg(**base)


def cmap(announce=None):
    return ChannelMap(announce=announce or {}, ecosystem={str(MAIN_ANN)})


def test_crosspost_mapped_channel():
    m = crosspost(10, OWN_G, OWN_C, 4242, "<@123> hi https://example.org/x",
                  embeds=[Embed(title="Mainnet v2", description="desc", fields=[Field("k", "v")], footer=Footer("f"),
                                image=Media("https://cdn.example/i.png"))])
    row = classify_message(m, cmap({str(OWN_C): ["106:1"]}), {}, gcfg(), NOW)
    assert row.msg_id == "discord:4242" and row.via_own_server == 1 and row.author_is_team == 1
    assert row.mapping == "channel" and row.occupant_keys == ["106:1"] and row.netuid == 106
    assert row.text.startswith("@user hi") and "Mainnet v2" in row.text and "<@" not in row.text
    composed = "<@123> hi https://example.org/x\nMainnet v2\ndesc\nk\nv\nf"
    assert row.text_hash == text_hash(discord_clean(composed))
    assert row.has_attachment == 1 and row.url_count == 1
    assert row.event_at == int(datetime(2026, 10, 16, 12, 0, tzinfo=timezone.utc).timestamp())


def test_main_announce_and_guild_mapping():
    row = classify_message(crosspost(1, 799672011265015819, MAIN_ANN, 1), cmap(), {}, gcfg(), NOW)
    assert (row.mapping, row.author_is_team, row.occupant_keys) == ("main_announce", 0, [])
    row = classify_message(crosspost(2, OWN_G, 999, 2, "x"), cmap(), {str(OWN_G): ["106:1"]}, gcfg(), NOW)
    assert (row.mapping, row.occupant_keys) == ("guild", ["106:1"])
    row = classify_message(crosspost(3, OWN_G, 999, 3, "x"), cmap(), {str(OWN_G): ["49:1", "50:1"]}, gcfg(), NOW)
    assert (row.mapping, row.netuid) == ("guild_ambiguous", None)
    row = classify_message(crosspost(4, 5, 6, 4, "x"), cmap(), {}, gcfg(), NOW)
    assert (row.mapping, row.author_is_team) == ("unmapped", None)


def test_ignored_messages():
    plain = FakeMessage(id=1, guild=Obj(int(AGG)), channel=Obj(500), content="operator chatter")
    assert classify_message(plain, cmap(), {}, gcfg(), NOW) is None
    no_hook = crosspost(2, OWN_G, OWN_C, 2)
    no_hook.webhook_id = None
    assert classify_message(no_hook, cmap(), {}, gcfg(), NOW) is None
    follow = FakeMessage(id=3, guild=Obj(int(AGG)), channel=Obj(500), type=12, content="Bittensor #📣・announce")
    assert classify_message(follow, cmap(), {}, gcfg(), NOW) is None
    foreign = crosspost(4, OWN_G, OWN_C, 4, guild="1200000000000000002")
    assert classify_message(foreign, cmap(), {}, gcfg(), NOW) is None


def test_clean_and_empty_hash():
    assert text_norm(discord_clean("<@123> Launch <#456> @everyone <:tao:789>")) == "launch"
    a = classify_message(crosspost(1, OWN_G, OWN_C, 71, attachments=["img"]), cmap(), {}, gcfg(), NOW)
    b = classify_message(crosspost(2, OWN_G, OWN_C, 72, attachments=["img"]), cmap(), {}, gcfg(), NOW)
    assert a.text_hash != b.text_hash and a.text_hash == post_hash("", "discord:71") and a.text is None


def test_snowflake_roundtrip():
    sf = snowflake_from_ts(NOW)
    assert abs(ts_from_snowflake(sf) - NOW) < 1


@pytest.fixture
def task(rt):
    return DiscordGatewayTask(rt, SPEC_BY_ID["discord_gateway"])


@pytest.mark.asyncio
@pytest.mark.parametrize("store_text", [True, False])
async def test_ingest_store_text_flag(rt, task, settings_override, store_text):
    settings_override(DISCORD_STORE_TEXT=store_text, DB_PATH=app_config.settings.DB_PATH)
    o = await seed_occupant(rt.db, 106)
    task.cmap = cmap({str(OWN_C): [o]})
    assert await task.ingest(crosspost(10, OWN_G, OWN_C, 4242, "<@123> hi"), gcfg(store_text=store_text)) is True
    r = await rt.db.fetchone("SELECT * FROM announcements WHERE msg_id = 'discord:4242'")
    assert r["text"] == "@user hi" and r["occupant_key"] == o and r["mapping"] == "channel"
    if store_text:
        assert r["text_purge_after"] is None
    else:
        assert r["text_purge_after"] == r["observed_at"] + 7 * 86400
    m = await rt.db.fetchone("SELECT * FROM social_post_mentions WHERE post_id = '4242'")
    assert (m["platform"], m["occupant_key"], m["match_kind"]) == ("discord", o, "team")
    assert await task.ingest(crosspost(10, OWN_G, OWN_C, 4242, "<@123> hi"), gcfg()) is False   # INSERT OR IGNORE
    await task.on_raw_delete(RawDelete(int(AGG), 500, 10), gcfg())
    assert (await rt.db.fetchone("SELECT deleted_at FROM announcements WHERE msg_id = 'discord:4242'"))["deleted_at"]
    assert await task.ingest(FakeMessage(id=11, guild=Obj(int(AGG)), channel=Obj(500), content="chatter"),
                             gcfg()) is False
    assert await rt.db.fetchval("SELECT COUNT(*) FROM announcements") == 1


@pytest.mark.asyncio
async def test_backfill_idempotent(rt, task):
    base = snowflake_from_ts(NOW - 3600)
    msgs = [crosspost(base + i, OWN_G, OWN_C, 9000 + i, f"post {i}") for i in range(5)]
    ch = FakeChannel(500, msgs)
    assert await task.backfill_channel(ch, gcfg()) == 5
    assert await task.backfill_channel(ch, gcfg()) >= 0
    assert await rt.db.fetchval("SELECT COUNT(*) FROM announcements") == 5
    wm = await rt.db.fetchval("SELECT value_json FROM job_state WHERE job = 'discord_gateway' AND key = 'wm:500'")
    assert wm.strip('"') == str(base + 4)
    after = ch.calls[-1]["after"]
    assert abs(ts_from_snowflake(after) - (ts_from_snowflake(base + 4) - 600)) < 1      # watermark − overlap
    capped = FakeChannel(501, [crosspost(base + 100 + i, OWN_G, OWN_C, 9100 + i, f"c{i}", channel=501)
                               for i in range(5)])
    assert await task.backfill_channel(capped, gcfg(max_per_channel=3)) == 3
    assert await task.backfill_channel(capped, gcfg(max_per_channel=3)) == 2           # continues from watermark


@pytest.mark.asyncio
async def test_fatal_login_and_recovery(rt, task, settings_override):
    settings_override(DISCORD_BOT_TOKEN="tokA", DISCORD_AGGREGATOR_GUILD_ID=AGG, DISCORD_GATEWAY_ENABLED=True,
                      DB_PATH=app_config.settings.DB_PATH)

    class LoginFailure(Exception):
        pass
    assert task.enabled() is True
    await task._set_fatal(LoginFailure("Improper token has been passed."))
    assert task.enabled() is False and task.fatal_error.startswith("LoginFailure")
    ev = await rt.db.fetchall("SELECT event_type, severity, payload FROM events WHERE event_type = "
                              "'discord_gateway_fatal'")
    assert len(ev) == 1 and ev[0]["severity"] == "warning"
    fresh = DiscordGatewayTask(rt, SPEC_BY_ID["discord_gateway"])              # a restart keeps it idle
    await fresh._load_fatal()
    assert fresh.enabled() is False
    settings_override(DISCORD_BOT_TOKEN="tokB", DISCORD_AGGREGATOR_GUILD_ID=AGG, DISCORD_GATEWAY_ENABLED=True,
                      DB_PATH=app_config.settings.DB_PATH)
    assert task.enabled() is True                                              # token hash changed
    guild = Obj(int(AGG))
    guild.text_channels = []

    class Client:
        guilds = [guild]
    await task.on_ready(Client(), gcfg())
    assert task.guild_ok and task.fatal_error is None
    rec = await rt.db.fetchall("SELECT payload FROM events WHERE event_type = 'collector_recovered'")
    assert len(rec) == 1 and '"discord_gateway"' in rec[0]["payload"]


@pytest.mark.asyncio
async def test_on_ready_guild_guard(rt, task):
    class Client:
        guilds = [Obj(1200000000000000002)]
    await task.on_ready(Client(), gcfg())
    assert task.guild_ok is False and task.last_error == "bot_not_in_aggregator_guild"
    assert task.foreign_guilds == ["1200000000000000002"]


@pytest.mark.asyncio
async def test_heartbeat_rows(rt, task):
    h0 = NOW - NOW % 3600
    for i in range(0, 3600, 30):
        await task.tick(True, h0 + i)
    await task.tick(False, h0 + 3600)                       # hour rollover writes the completed hour
    for i in range(30, 3600, 30):
        await task.tick(False, h0 + 3600 + i)
    await task.tick(False, h0 + 7200)
    rows = await rt.db.fetchall("SELECT run_id, ok, finished_at, error FROM collector_runs WHERE job = "
                                "'discord_gateway' ORDER BY started_at")
    assert [(r["run_id"], r["ok"]) for r in rows] == [(f"gw:{h0}", 1), (f"gw:{h0 + 3600}", 0)]
    assert rows[0]["finished_at"] == h0 + 3600 and rows[1]["error"] == "disconnected"
