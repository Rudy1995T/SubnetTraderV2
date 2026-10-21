"""Spec 08 §9 #34: invite hygiene, debounced venue changes, silence episodes; payloads pass 03's validation."""
from __future__ import annotations

import json

import httpx
import pytest
import respx

from app.collectors.community_events import detect_silence, detect_venue_changes, emit_tg_venue_changes
from app.collectors.discord import DiscordCfg, run_invite_cycle
from app.collectors.discord_topology import TopologyDecision
from tests.collectors.community.conftest import (API, DAY, HOUR, NOW, community_settings, fixture_bytes,
                                                 seed_identity, seed_occupant, seed_watch)

CFG = DiscordCfg.from_settings(community_settings())


async def events(db, et):
    return [dict(r, payload=json.loads(r["payload"])) for r in
            await db.fetchall("SELECT * FROM events WHERE event_type = ? ORDER BY id", (et,))]


@pytest.mark.asyncio
@respx.mock
async def test_invite_dead_needs_two_checks(db, http, cset, frozen):
    s = cset()
    o = await seed_occupant(db, 65)
    await seed_identity(db, o, "discord", "present")
    await seed_watch(db, o, "discord_invite", "taofu", source="subnetalpha")
    respx.get(f"{API}/invites/bittensor").mock(return_value=httpx.Response(
        200, content=fixture_bytes("invite_vanity_bittensor.json")))
    respx.get(f"{API}/invites/taofu").mock(return_value=httpx.Response(404, content=fixture_bytes("invite_10006.json")))
    await run_invite_cycle(db, http, now=NOW, run_id="r1", s=s)
    assert await events(db, "invite_dead") == []
    frozen(NOW + DAY)
    await run_invite_cycle(db, http, now=NOW + DAY, run_id="r2", s=s)
    ev = await events(db, "invite_dead")
    assert len(ev) == 1 and ev[0]["severity"] == "info" and ev[0]["source"] == "community"
    assert ev[0]["payload"]["first_dead_at"] == NOW and ev[0]["dedupe_key"] == f"{o}|taofu|10006"
    frozen(NOW + 2 * DAY)
    await run_invite_cycle(db, http, now=NOW + 2 * DAY, run_id="r3", s=s)
    assert len(await events(db, "invite_dead")) == 1


@pytest.mark.asyncio
@respx.mock
async def test_invite_expiring_then_expired(db, http, cset, frozen):
    from app.util.time import parse_iso
    s = cset()
    o = await seed_occupant(db, 78)
    await seed_identity(db, o, "discord", "present")
    await seed_watch(db, o, "discord_invite", "umiTemp", source="subnetalpha")
    respx.get(f"{API}/invites/bittensor").mock(return_value=httpx.Response(
        200, content=fixture_bytes("invite_vanity_bittensor.json")))
    respx.get(f"{API}/invites/umiTemp").mock(return_value=httpx.Response(
        200, content=fixture_bytes("invite_expiring_umi.json")))
    t1 = parse_iso("2026-09-22T12:00:05Z")
    frozen(t1)
    await run_invite_cycle(db, http, now=t1, run_id="r1", s=s)
    t2 = parse_iso("2026-09-23T00:00:05Z")
    frozen(t2)
    await run_invite_cycle(db, http, now=t2, run_id="r2", s=s)
    await run_invite_cycle(db, http, now=t2 + 60, run_id="r3", s=s)
    ev = await events(db, "invite_expired")
    assert [e["payload"]["stage"] for e in ev] == ["expiring", "expired"]
    assert ev[1]["severity"] == "warning"                     # was the primary invite; nothing else reachable


def dec(okey, cls, guild=None):
    return TopologyDecision(okey, int(okey.split(":")[0]), cls, primary_guild_id=guild)


@pytest.mark.asyncio
async def test_venue_change_debounce(db, cset):
    o = await seed_occupant(db, 55)
    await detect_venue_changes(db, [dec(o, "own_server", "G1")], NOW, "r", CFG, cycle_at=NOW)
    n = await detect_venue_changes(db, [dec(o, "invalid")], NOW + 6 * HOUR, "r", CFG, cycle_at=NOW + 6 * HOUR)
    assert n == 0
    n = await detect_venue_changes(db, [dec(o, "unknown")], NOW + 12 * HOUR, "r", CFG, cycle_at=NOW + 12 * HOUR)
    assert n == 0                                                            # unknown never counts
    n = await detect_venue_changes(db, [dec(o, "invalid")], NOW + 18 * HOUR, "r", CFG, cycle_at=NOW + 18 * HOUR)
    assert n == 1
    ev = (await events(db, "community_venue_changed"))[0]
    assert ev["severity"] == "warning" and ev["payload"]["old_class"] == "own_server"
    assert ev["payload"]["first_seen_new_at"] == NOW + 6 * HOUR
    n = await detect_venue_changes(db, [dec(o, "invalid")], NOW + 18 * HOUR, "r", CFG, cycle_at=NOW + 18 * HOUR)
    assert n == 0


@pytest.mark.asyncio
async def test_tg_venue_change(db, cset):
    o = await seed_occupant(db, 30)
    base = {"occupant_key": o, "netuid": 30, "handle": "grp", "parse_ok": 1}
    day = NOW - NOW % DAY
    assert await emit_tg_venue_changes(db, [{**base, "kind": "group", "cycle_day_ts": day}], NOW, "t", 2) == 0
    assert await emit_tg_venue_changes(db, [{**base, "kind": "not_found", "cycle_day_ts": day + DAY}], NOW, "t", 2) == 0
    assert await emit_tg_venue_changes(db, [{**base, "kind": None, "parse_ok": 0, "cycle_day_ts": day + 2 * DAY}],
                                       NOW, "t", 2) == 0
    assert await emit_tg_venue_changes(db, [{**base, "kind": "not_found", "cycle_day_ts": day + 3 * DAY}],
                                       NOW, "t", 2) == 1
    ev = (await events(db, "community_venue_changed"))[0]
    assert ev["severity"] == "warning" and ev["payload"]["platform"] == "telegram"


async def silence_row(db, okey, as_of, value):
    from app.storage.pit import put_metric_values
    await put_metric_values(db, [{"occupant_key": okey, "metric_key": "silence_days", "as_of": as_of,
                                  "spec_version": "ts-1.0", "stage": "raw", "value": value,
                                  "value_json": {"status": "ok", "lower_bound": False, "min_leg": "x",
                                                 "legs": {"x": value, "discord": None, "tg": None}},
                                  "observed_at": as_of - 300, "source": "scoring", "run_id": "s"}])


@pytest.mark.asyncio
async def test_silence_episodes(db, cset):
    o = await seed_occupant(db, 9)
    d0 = NOW - NOW % DAY
    for d, v in enumerate([50, 51] + [10] * 7 + [60]):
        t = d0 + d * DAY
        await silence_row(db, o, t, v)
        await detect_silence(db, [o], t, "r", t)
    ev = await events(db, "silence_45d")
    assert len(ev) == 2                                                # re-armed after 7 days < 45
    p = ev[0]["payload"]
    assert p["silence_days"] == 50 and p["last_comm_at"] is None and p["last_comm_platform"] is None
    assert set(p) >= {"netuid", "occupant_key", "silence_days", "lower_bound", "legs", "last_comm_at",
                      "last_comm_platform"}
