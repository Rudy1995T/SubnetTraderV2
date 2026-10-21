"""Spec 08 §9 #18–#20: one invite cycle end to end, idempotent re-run, all-failed, cycle_slot, widget schedule."""
from __future__ import annotations

import json

import httpx
import pytest
import respx

from app.collectors.discord import cycle_slot, run_invite_cycle, run_widget_pass
from app.http import ProviderError
from app.util.time import parse_iso
from tests.collectors.community.conftest import (API, DAY, HOUR, MAIN, NOW, fixture_bytes, insert_snapshots,
                                                 seed_identity, seed_occupant, seed_watch, snapshot_row)

D0 = NOW - NOW % DAY


async def seed_four(db):
    o106 = await seed_occupant(db, 106)
    await seed_watch(db, o106, "discord_invite", "fwjBwxUcdX")
    o2, o5 = await seed_occupant(db, 2), await seed_occupant(db, 5)
    for o in (o2, o5):
        await seed_watch(db, o, "discord_invite", "bittensor", source="subnetalpha", priority=20, guild_id=MAIN)
    o8 = await seed_occupant(db, 8)
    await seed_watch(db, o8, "discord_handle", "tl_arrash")
    root = await seed_occupant(db, 0)                       # root netuid 0 is never polled (06/07/12)
    await seed_watch(db, root, "discord_invite", "rootcode")
    for o in (o106, o2, o5, o8, root):
        await seed_identity(db, o, "discord", "present")
    return o106, o2, o5, o8


def mock_ok():
    own = respx.get(f"{API}/invites/fwjBwxUcdX").mock(
        return_value=httpx.Response(200, content=fixture_bytes("invite_own_server_nodexo.json")))
    van = respx.get(f"{API}/invites/bittensor").mock(
        return_value=httpx.Response(200, content=fixture_bytes("invite_vanity_bittensor.json")))
    root = respx.get(f"{API}/invites/rootcode").mock(return_value=httpx.Response(500))
    return own, van, root


@pytest.mark.asyncio
@respx.mock
async def test_cycle_rows(db, http, cset, frozen):
    s = cset()
    o106, o2, o5, o8 = await seed_four(db)
    own, van, root = mock_ok()
    rep = await run_invite_cycle(db, http, now=NOW, run_id="r1", s=s)
    assert van.call_count == 1 and own.call_count == 1 and root.call_count == 0
    rows = await db.fetchall("SELECT * FROM discord_snapshots ORDER BY id")
    inv = [r for r in rows if r["probe"] == "invite"]
    cen = [r for r in rows if r["probe"] == "census"]
    assert len(inv) == 2 and len(cen) == 3
    prim = next(r for r in inv if r["occupant_key"] == o106)
    assert prim["is_primary"] == 1 and prim["topology_class"] == "own_server" and prim["member_count"] == 1986
    assert prim["cycle_at"] == D0 and json.loads(prim["declared_by_json"]) == ["chain"]
    main_row = next(r for r in inv if r["occupant_key"] is None)
    assert main_row["invite_code"] == "bittensor" and main_row["guild_id"] == MAIN and main_row["is_primary"] == 0
    classes = {r["occupant_key"]: r["topology_class"] for r in cen}
    assert classes == {o2: "main_vanity", o5: "main_vanity", o8: "handle_only"}
    assert all(r["is_primary"] == 1 for r in cen)
    assert next(r for r in cen if r["occupant_key"] == o8)["invite_code"] is None   # never a username
    w = await db.fetchone("SELECT guild_id, verified_at, meta_json FROM watch_channels WHERE handle = 'fwjBwxUcdX'")
    assert w["guild_id"] == "1039241590092353596" and w["verified_at"] == NOW
    assert json.loads(w["meta_json"])["last_class"] == "own_server"
    ev = await db.fetchall("SELECT occupant_key, metric_key, as_of, source, stage, observed_at, value, status "
                           "FROM metric_values ORDER BY occupant_key, metric_key")
    assert len(ev) == 12 and {r["metric_key"] for r in ev} == {"discord_topology", "inflated_days", "tg_members"}
    assert all(r["as_of"] == D0 and r["source"] == "community" and r["stage"] == "raw" for r in ev)
    assert all(r["observed_at"] >= max(x["observed_at"] for x in rows) for r in ev)
    topo = {r["occupant_key"]: r["value"] for r in ev if r["metric_key"] == "discord_topology"}
    assert topo == {o106: 5.0, o2: 3.0, o5: 3.0, o8: 1.0}
    assert rep["classes"] == {"handle_only": 1, "main_vanity": 2, "own_server": 1}


@pytest.mark.asyncio
@respx.mock
async def test_rerun_same_slot_idempotent(db, http, cset, frozen):
    s = cset()
    await seed_four(db)
    own, van, _ = mock_ok()
    await run_invite_cycle(db, http, now=NOW, run_id="r1", s=s)
    frozen(NOW + 600)
    rep = await run_invite_cycle(db, http, now=NOW + 600, run_id="r2", s=s)
    assert rep["due"] == 0 and own.call_count == 1 and van.call_count == 1
    dup = await db.fetchall("SELECT occupant_key FROM discord_snapshots WHERE probe = 'census' GROUP BY occupant_key "
                            "HAVING COUNT(*) > 1")
    assert dup == []


@pytest.mark.asyncio
@respx.mock
async def test_all_failed_raises(db, http, cset, frozen):
    s = cset()
    await seed_four(db)
    respx.get(url__startswith=f"{API}/invites/").mock(return_value=httpx.Response(503))
    with pytest.raises(ProviderError):
        await run_invite_cycle(db, http, now=NOW, run_id="r1", s=s)
    fails = await db.fetchall("SELECT invite_valid, http_status, meta_json FROM discord_snapshots WHERE probe = 'invite'")
    assert fails and all(r["invite_valid"] is None for r in fails)
    cen = await db.fetchall("SELECT topology_class FROM discord_snapshots WHERE probe = 'census'")
    assert {r["topology_class"] for r in cen} <= {"unknown", "handle_only"}     # a failure is never invalid/none


@pytest.mark.asyncio
@respx.mock
async def test_failure_keeps_fresh_evidence(db, http, cset, frozen):
    """A failed fetch never erases valid evidence inside the 2-day window (observation, not forward-fill)."""
    s = cset()
    o106, *_ = await seed_four(db)
    mock_ok()
    await run_invite_cycle(db, http, now=NOW, run_id="r1", s=s)
    respx.get(f"{API}/invites/fwjBwxUcdX").mock(return_value=httpx.Response(502))
    frozen(NOW + 6 * HOUR)
    await run_invite_cycle(db, http, now=NOW + 6 * HOUR, run_id="r2", s=s)
    r = await db.fetchone("SELECT * FROM discord_snapshots WHERE occupant_key = ? AND cycle_at = ? AND is_primary = 1",
                          (o106, D0 + 6 * HOUR))
    assert r["probe"] == "census" and r["topology_class"] == "own_server" and r["guild_id"] == "1039241590092353596"


def test_cycle_slot():
    assert cycle_slot(parse_iso("2026-10-17T06:00:40Z"), (0, 6, 12, 18)) == parse_iso("2026-10-17T06:00:00Z")
    assert cycle_slot(parse_iso("2026-10-17T05:59:00Z"), (0, 6, 12, 18)) == parse_iso("2026-10-17T00:00:00Z")
    assert cycle_slot(parse_iso("2026-10-17T00:30:00Z"), (3, 15)) == parse_iso("2026-10-16T15:00:00Z")


@pytest.mark.asyncio
@respx.mock
async def test_widget_schedule(db, http, cset, frozen):
    s = cset()
    oa, ob, oc = await seed_occupant(db, 64), await seed_occupant(db, 72), await seed_occupant(db, 1)
    t = NOW - 3 * HOUR
    await insert_snapshots(db, [
        snapshot_row(oa, observed_at=t, guild_id="692175473198759938", code="a", members=5000, presence=1300,
                     cls="own_server", primary=1),
        snapshot_row(ob, observed_at=t, guild_id="1039241590092353596", code="b", members=2000, presence=140,
                     cls="own_server", primary=1),
        snapshot_row(oc, probe="census", observed_at=t, guild_id=MAIN, cls="main_channel", primary=1, valid=None)])
    en = respx.get(f"{API}/guilds/692175473198759938/widget.json").mock(
        return_value=httpx.Response(200, content=fixture_bytes("widget_enabled_cooking.json")))
    dis = respx.get(f"{API}/guilds/1039241590092353596/widget.json").mock(
        return_value=httpx.Response(403, content=fixture_bytes("widget_disabled.json")))
    main = respx.get(f"{API}/guilds/{MAIN}/widget.json").mock(return_value=httpx.Response(200, json={}))
    rep = await run_widget_pass(db, http, now=NOW, run_id="w1", s=s)
    assert (rep["due"], en.call_count, dis.call_count, main.call_count) == (2, 1, 1, 0)
    frozen(NOW + HOUR)
    rep = await run_widget_pass(db, http, now=NOW + HOUR, run_id="w2", s=s)
    assert (en.call_count, dis.call_count) == (2, 1)                      # disabled skipped until 7 d
    frozen(NOW + 7 * DAY)
    await insert_snapshots(db, [snapshot_row(oa, observed_at=NOW + 7 * DAY - 60, guild_id="692175473198759938",
                                             code="a", members=5000, presence=1300, cls="own_server", primary=1),
                                snapshot_row(ob, observed_at=NOW + 7 * DAY - 60, guild_id="1039241590092353596",
                                             code="b", members=2000, presence=140, cls="own_server", primary=1)])
    await run_widget_pass(db, http, now=NOW + 7 * DAY, run_id="w3", s=s)
    assert dis.call_count == 2
    w = await db.fetchall("SELECT * FROM discord_snapshots WHERE probe = 'widget' AND guild_id = '692175473198759938'")
    assert w[0]["presence_count"] == 1311 and w[0]["occupant_key"] == oa and w[0]["member_count"] is None
    assert "alice" not in (w[0]["meta_json"] or "")


@pytest.mark.asyncio
@respx.mock
async def test_legacy_only_targets_ignored(db, http, cset, frozen):
    """Targets declared only by ``legacy`` (frozen old fundamentals.json) are not live claims (live 2026-10-17: 5 of
    6 opened a previous occupant's server)."""
    s = cset()
    o10 = await seed_occupant(db, 10)
    await seed_identity(db, o10, "discord", "present")
    await seed_watch(db, o10, "discord_invite", "tRVHp6Vx5N", source="legacy", priority=40)
    await seed_watch(db, o10, "discord_invite", "bittensor", source="subnetalpha", priority=20, guild_id=MAIN)
    o17 = await seed_occupant(db, 17)
    await seed_identity(db, o17, "discord", "present")
    await seed_watch(db, o17, "discord_invite", "404gen", source="subnetalpha", priority=20,
                     declared_by=["legacy", "subnetalpha"])
    stale = respx.get(f"{API}/invites/tRVHp6Vx5N").mock(return_value=httpx.Response(
        200, content=fixture_bytes("invite_own_server_nodexo.json")))
    respx.get(f"{API}/invites/404gen").mock(return_value=httpx.Response(
        200, content=fixture_bytes("invite_own_server_nodexo.json")))
    respx.get(f"{API}/invites/bittensor").mock(return_value=httpx.Response(
        200, content=fixture_bytes("invite_vanity_bittensor.json")))
    rep = await run_invite_cycle(db, http, now=NOW, run_id="r1", s=s)
    assert stale.call_count == 0 and rep["classes"] == {"main_vanity": 1, "own_server": 1}
