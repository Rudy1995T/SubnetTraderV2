"""Spec 08 §9 #28–#33: P5 producers — presence, inflation, growth, venue matrix, legs (union), PIT."""
from __future__ import annotations

import pytest

from app.collectors.community_store import insert_telegram_snapshot, upsert_announcement
from app.collectors.discord_gateway import AnnouncementRow
from app.features import community as C
from app.features.normalise import presence_ratio
from tests.collectors.community.conftest import cset  # noqa: F401  (fixture)
from tests.collectors.community.conftest import (DAY, HOUR, MAIN, NOW, insert_snapshots, seed_heartbeat,
                                                 seed_identity, seed_occupant, seed_watch, snapshot_row)

T = NOW - NOW % DAY            # 2026-10-17T00:00Z
G = "1419378005230485587"


async def own_samples(db, okey, days, members, presence, guild=G, per_day=4, end=T):
    rows = []
    for d in range(days):
        for k in range(per_day):
            t = end - (d + 1) * DAY + k * 6 * HOUR + 5
            rows.append(snapshot_row(okey, observed_at=t, guild_id=guild, code="Tag101", members=members,
                                     presence=presence, cls="own_server", primary=1))
    await insert_snapshots(db, rows)


@pytest.mark.asyncio
async def test_presence_ratio_contract(db, cset, monkeypatch):
    o = await seed_occupant(db, 101)
    await own_samples(db, o, 1, 1905, 9)
    r = await C.compute_presence_ratio(db, o, T)
    assert r.status == "ok" and r.value == pytest.approx(0.004724, abs=1e-6)
    assert r.value_json["members"] == 1905 and r.value_json["inflated"] is True
    assert presence_ratio(r.value, r.value_json["members"]) == 0.0
    assert presence_ratio(0.01752, 20428) == 0.0 and presence_ratio(0.004, 999) == pytest.approx(0.08)
    assert presence_ratio(0.0715, 1986) == 1.0 and presence_ratio(0.3513, 390) == pytest.approx(0.998, abs=1e-3)
    assert presence_ratio(0.9, 20) == 0.5 and presence_ratio(0.1, None) is None
    monkeypatch.setattr(C, "presence_params", lambda: {**C.PRESENCE_DEFAULTS, "inflate_below": 0.001})
    assert (await C.compute_presence_ratio(db, o, T)).value_json["inflated"] is False


@pytest.mark.asyncio
async def test_inflation_tag101(db, cset):
    from app.collectors.community_events import detect_inflation
    o = await seed_occupant(db, 101)
    await own_samples(db, o, 7, 1905, 9)
    assert await C.inflated_days(db, o, T) == 7
    assert await detect_inflation(db, [o], T, "r", T) == 1
    assert await detect_inflation(db, [o], T, "r2", T + 60) == 0                    # emit_event dedupe
    ev = await db.fetchone("SELECT severity, source, payload FROM events WHERE event_type = 'inflated_community'")
    assert ev["severity"] == "warning" and ev["source"] == "community"
    o2 = await seed_occupant(db, 102)
    await own_samples(db, o2, 6, 1905, 9, guild="G2")
    assert await C.inflated_days(db, o2, T) == 6 and await detect_inflation(db, [o2], T, "r", T) == 0
    o3 = await seed_occupant(db, 103)
    await own_samples(db, o3, 3, 1905, 9, guild="G3")
    await own_samples(db, o3, 1, 1905, 9, guild="G3", per_day=1, end=T - 3 * DAY)    # a 1-sample day ends it
    await own_samples(db, o3, 4, 1905, 9, guild="G3", end=T - 4 * DAY)
    assert await C.inflated_days(db, o3, T) == 3


@pytest.mark.asyncio
async def test_presence_and_growth(db, cset):
    o = await seed_occupant(db, 106)
    await insert_snapshots(db, [snapshot_row(o, observed_at=T - 20 * HOUR + i, guild_id="GN", code="n", members=m,
                                             presence=p, cls="own_server", primary=1)
                                for i, (m, p) in enumerate([(1100, 100), (1100, 120), (1100, 80), (1100, 140)])])
    r = await C.compute_presence_ratio(db, o, T)
    assert r.value == pytest.approx(110 / 1100) and r.value_json["samples"] == 4
    await insert_snapshots(db, [snapshot_row(o, observed_at=T - 30 * DAY - HOUR, guild_id="GN", code="n",
                                             members=1000, presence=90, cls="own_server", primary=1)])
    g = await C.compute_discord_growth_30d(db, o, T)
    assert g.status == "ok" and g.value == pytest.approx(0.10) and g.value_json["base_day"] == 30
    o2 = await seed_occupant(db, 107)
    await insert_snapshots(db, [snapshot_row(o2, observed_at=T - HOUR, guild_id="GX", code="x", members=1100,
                                             presence=100, cls="own_server", primary=1)])
    assert (await C.compute_presence_ratio(db, o2, T)).status == "failed"            # 1 sample
    await insert_snapshots(db, [snapshot_row(o2, observed_at=T - 33 * DAY - HOUR, guild_id="GX", code="x",
                                             members=1000, presence=90, cls="own_server", primary=1)])
    assert (await C.compute_discord_growth_30d(db, o2, T)).value_json["base_day"] == 33
    o3 = await seed_occupant(db, 108)
    await insert_snapshots(db, [snapshot_row(o3, observed_at=T - HOUR, guild_id="GY", code="y", members=1100,
                                             presence=100, cls="own_server", primary=1),
                                snapshot_row(o3, observed_at=T - 34 * DAY - HOUR, guild_id="GY", code="y",
                                             members=1000, presence=90, cls="own_server", primary=1),
                                snapshot_row(o3, observed_at=T - 30 * DAY - HOUR, guild_id="OTHER", code="z",
                                             members=1000, presence=90, cls="own_server", primary=1)])
    assert (await C.compute_discord_growth_30d(db, o3, T)).status == "failed"        # −34 d / other guild
    o4 = await seed_occupant(db, 109)
    await insert_snapshots(db, [snapshot_row(o4, probe="census", observed_at=T - HOUR, guild_id=MAIN,
                                             cls="main_channel", primary=1, valid=None)])
    assert (await C.compute_presence_ratio(db, o4, T)).status == "not_applicable"


async def census(db, okey, cls, *, at=T - HOUR, unknown=0, guild=None):
    await insert_snapshots(db, [snapshot_row(okey, probe="census", observed_at=at, guild_id=guild, cls=cls, primary=1,
                                             valid=None, unknown=unknown)])


async def tg_snap(db, okey, handle, kind, *, members=100, parse_ok=1, at=T - HOUR, deprecated=0, preview=None,
                  last_post=None):
    await insert_telegram_snapshot(db, {"occupant_key": okey, "netuid": int(okey.split(":")[0]), "handle": handle,
                                        "observed_at": at, "cycle_day_ts": at - at % DAY, "source": "tme_page",
                                        "http_status": 200, "kind": kind, "members": members, "parse_ok": parse_ok,
                                        "deprecated_marker": deprecated, "preview_available": preview,
                                        "last_post_at": last_post, "run_id": "s"})


@pytest.mark.asyncio
async def test_venue_reachable_matrix(db, cset):
    cases = {}
    for n in range(1, 10):
        cases[n] = await seed_occupant(db, n)
        await seed_identity(db, cases[n], "telegram", "absent")
        await seed_identity(db, cases[n], "discord", "present")
    o = cases
    await insert_snapshots(db, [snapshot_row(o[1], observed_at=T - HOUR, guild_id="GN", code="n", members=1986,
                                             presence=142, cls="own_server", primary=1)])
    await census(db, o[2], "main_channel", guild=MAIN)
    await census(db, o[3], "main_vanity", guild=MAIN)
    for n in (4, 5, 6):
        await census(db, o[n], "invalid")
    await seed_watch(db, o[4], "telegram", "grp")
    await tg_snap(db, o[4], "grp", "group")
    await seed_watch(db, o[5], "telegram", "usr")
    await tg_snap(db, o[5], "usr", "user_or_unknown", members=None)
    await seed_watch(db, o[6], "telegram", "bad")
    await tg_snap(db, o[6], "bad", None, members=None, parse_ok=0)
    await census(db, o[7], "invalid", at=T - 3 * DAY)
    await census(db, o[8], "none")
    await seed_identity(db, o[8], "discord", "absent")
    await census(db, o[9], "main_vanity", guild=MAIN, unknown=1)

    async def vr(n):
        return await C.compute_venue_reachable(db, o[n], T)
    assert (await vr(1)).value == 1.0 and (await vr(2)).value == 1.0
    assert (await vr(3)).status == "failed"                                  # vanity-only before the map review
    assert (await vr(4)).value == 1.0                                        # invalid + fresh TG group
    r5 = await vr(5)
    assert r5.status == "absent" and r5.value == 0.0 and r5.mapped_json()["reason"] == "no_reachable_venue"
    assert (await vr(6)).status == "failed" and (await vr(7)).status == "failed"
    r8 = await vr(8)
    assert r8.status == "absent" and r8.mapped_json()["reason"] == "no_venue" and r8.mapped_json()["status"] == "absent"
    assert (await vr(9)).status == "failed"                                  # an unknown target blocks 0.0
    cset(DISCORD_CHANNEL_MAP_REVIEWED=True)
    assert (await vr(3)).status == "absent"
    m = await C.compute_discord_members(db, o[1], T)
    assert m.value == 1986 and (await C.compute_discord_members(db, o[2], T)).status == "not_applicable"


def ann(msg_id, platform, okeys, text_hash, event_at, observed_at, mapping="channel"):
    return AnnouncementRow(msg_id, platform, okeys, None, None, "chan", msg_id.split(":", 1)[1].split("/")[-1], None,
                           None, int(platform == "discord"), mapping, 1, 0, 0, None, text_hash, 0, event_at,
                           observed_at)


@pytest.mark.asyncio
async def test_legs_make_union_and_last_days(db, cset):
    o = await seed_occupant(db, 106)
    await seed_watch(db, o, "discord_channel", "1052226281472794735", source="operator", priority=45,
                     guild_id="GN", first_seen=T - 60 * DAY, meta={"kind": "announce", "operator_since": T - 60 * DAY})
    await seed_watch(db, o, "telegram", "SubnetNews", source="subnetalpha")
    await tg_snap(db, o, "SubnetNews", "channel", preview=1)
    await seed_heartbeat(db, T - 2 * HOUR)
    for i, h in enumerate(("hx1", "hx2", "hx3")):                          # 3 team X posts
        await db.execute("INSERT INTO social_posts (platform, post_id, author_id, occupant_key, is_team_account, "
                         "text_hash, event_at, observed_at, source) VALUES ('x',?,?,?,1,?,?,?,'twitterapi_io')",
                         (f"x{i}", "a1", o, h, T - (i + 1) * DAY, T - (i + 1) * DAY + 60))
    rows = [ann("discord:1", "discord", [o], "hx1", T - DAY - 20 * HOUR, T - DAY),      # = X post 20 h apart
            ann("discord:2", "discord", [o], "hd2", T - 3 * DAY, T - 3 * DAY + 10),
            ann("telegram:subnetnews/7", "telegram", [o], "hd2", T - 3 * DAY + HOUR, T - 3 * DAY + HOUR,
                mapping="tg_handle")]
    for r in rows:
        await upsert_announcement(db, r, store_text=True, pending_days=7)
    d = await C.compute_discord_announcements_30d(db, o, T)
    t = await C.compute_tg_posts_30d(db, o, T)
    assert (d.value, t.value) == (1, 0) and d.value_json["dedup_x"] == 1
    assert 3 + d.value + t.value == 4                                         # 12's Σ = the union
    last = await C.compute_discord_last_activity_days(db, o, T)
    assert last.value == pytest.approx(1 + 20 / 24) and last.value_json["lower_bound"] is False   # dupes still count
    tg_last = await C.compute_tg_last_activity_days(db, o, T)
    assert tg_last.value == pytest.approx(3 - 1 / 24) and tg_last.value_json["lower_bound"] is False
    o2 = await seed_occupant(db, 107)
    await seed_watch(db, o2, "discord_channel", "999000000000000001", source="operator", priority=45, guild_id="GZ",
                     first_seen=T - 60 * DAY, meta={"kind": "announce", "operator_since": T - 60 * DAY})
    r = await C.compute_discord_last_activity_days(db, o2, T)
    assert r.value == pytest.approx(60) and r.value_json["lower_bound"] is True
    o3 = await seed_occupant(db, 108)
    assert (await C.compute_discord_announcements_30d(db, o3, T)).status == "not_applicable"
    stale = T + 3 * DAY
    assert (await C.compute_discord_announcements_30d(db, o, stale)).status == "failed"       # heartbeat stale
    assert (await C.compute_tg_posts_30d(db, o, stale)).status == "failed"                    # snapshot stale


@pytest.mark.asyncio
async def test_pit_and_no_combiner_rows(db, cset):
    o = await seed_occupant(db, 106)
    await seed_watch(db, o, "discord_channel", "1052226281472794735", source="operator", priority=45, guild_id="GN",
                     first_seen=T - 60 * DAY, meta={"kind": "announce", "operator_since": T - 60 * DAY})
    await seed_heartbeat(db, T - 2 * HOUR)
    await upsert_announcement(db, ann("discord:5", "discord", [o], "h5", T - DAY, T + HOUR), store_text=True,
                              pending_days=7)
    await census(db, o, "main_channel", at=T + 60, guild=MAIN)
    assert (await C.compute_discord_announcements_30d(db, o, T)).value == 0         # observed after as_of
    assert (await C.compute_venue_reachable(db, o, T)).status == "failed"           # census after as_of
    assert (await C.compute_discord_announcements_30d(db, o, T + 2 * HOUR)).value == 1
    from app.storage.pit import put_metric_values
    rows = [C.to_raw_row(k, r, occupant_key=o, as_of=T, observed_at=T + 60, run_id="cli")
            for k, r in await C.community_metrics(db, o, T, evidence=False)]
    await put_metric_values(db, rows)
    keys = {r["metric_key"] for r in await db.fetchall("SELECT metric_key FROM metric_values")}
    assert keys == set(C.PRODUCERS) and not keys & {"announcement_cadence_30d", "silence_days"}
    assert C.register_producers() == len(C.PRODUCERS) == 8                 # 12's registry accepts every C5 row
