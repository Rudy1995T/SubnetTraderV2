"""Spec 08 §9 #10–#14: due tiers, classification precedence, primary stickiness, source agreement."""
from __future__ import annotations

from app.collectors.discord import DiscordCfg
from app.collectors.discord_api import InviteResult, parse_invite
from app.collectors.discord_topology import Target, TargetState, classify_occupant, is_due, source_agreement
from tests.collectors.community.conftest import DAY, HOUR, MAIN, community_settings, fixture_bytes

CFG = DiscordCfg.from_settings(community_settings())
D0 = 1792195200            # 2026-10-17T00:00Z


def st(valid, guild=None, obs=D0 - 6 * HOUR, cycle=None, dead_since=None):
    return TargetState("c", valid, guild, obs, cycle if cycle is not None else obs - obs % (6 * HOUR),
                       dead_since=dead_since)


def due(state, *, code="c", now=D0 + 5, cycle=D0):
    return is_due(state, code=code, now=now, cycle_at=cycle, cfg=CFG)


def test_is_due_tiers():
    assert due(None) is True
    assert due(st(None)) is True                                          # last attempt failed
    assert due(st(1, "G1")) is True                                       # own server: every cycle
    assert due(st(1, MAIN), code="bittensor") is True                     # vanity: every cycle
    assert due(st(1, MAIN, obs=D0 - 3 * DAY)) is False                    # main-channel invite checked 3 d ago
    assert due(st(1, MAIN, obs=D0 - 7 * DAY)) is True
    assert due(st(1, "G1", obs=D0 + 1, cycle=D0)) is False                # already fetched OK in this slot
    six = D0 + 6 * HOUR
    assert due(st(0, obs=D0 - 18 * HOUR, dead_since=D0 - 5 * DAY), now=six, cycle=six) is False   # hour 6
    assert due(st(0, obs=D0 - 21 * HOUR, dead_since=D0 - 5 * DAY)) is True                        # 00Z, 21 h
    assert due(st(0, obs=D0 - 19 * HOUR, dead_since=D0 - 5 * DAY)) is False                       # 00Z, 19 h
    assert due(st(0, obs=D0 - 3 * DAY, dead_since=D0 - 40 * DAY)) is False                        # weekly tier
    assert due(st(0, obs=D0 - 7 * DAY, dead_since=D0 - 40 * DAY)) is True


def inv(code, file, t=D0):
    return parse_invite(code, 200 if "100" not in file and "502" not in file else 404, fixture_bytes(file), t)


def own(code, guild, members=100, *, t=D0):
    return InviteResult(code=code, fetched_at=t, http_status=200, invite_valid=1, guild_id=guild,
                        member_count=members, presence_count=10)


def tgt(code, source="chain", platform="discord_invite", *, resolved=True, prio=10, guild=None, netuid=106,
        declared=None):
    return Target(f"{netuid}:1", netuid, platform, code, source, prio, resolved, guild,
                  {"declared_by": declared or [source]})


def classify(targets, evidence, *, main_alive=True, ident="present", prev=None, netuid=106):
    return classify_occupant(netuid, f"{netuid}:1", targets, evidence, main_alive=main_alive, identity_status=ident,
                             prev_primary_guild=prev, cfg=CFG)


def test_classify_sn106_and_sn64():
    ev = {"t7BMee4w": inv("t7BMee4w", "invite_10006.json"),
          "fwjBwxUcdX": inv("fwjBwxUcdX", "invite_own_server_nodexo.json")}
    d = classify([tgt("t7BMee4w", "subnetalpha", prio=20), tgt("fwjBwxUcdX")], ev)
    assert (d.topology_class, d.primary_code, d.source_agreement) == ("own_server", "fwjBwxUcdX", "one_dead")
    ev = {"chutes": own("chutes", "G64"), "bittensor": inv("bittensor", "invite_vanity_bittensor.json")}
    d = classify([tgt("chutes", netuid=64), tgt("bittensor", "subnetalpha", netuid=64)], ev, netuid=64)
    assert (d.topology_class, d.source_agreement) == ("own_server", "single_source")


def test_classify_main_channel():
    van = {"bittensor": inv("bittensor", "invite_vanity_bittensor.json")}
    deep = tgt("1201941624243109888", platform="discord_channel", guild=MAIN, netuid=12)
    d = classify([deep], van, netuid=12)
    assert d.topology_class == "main_channel" and d.main_channel_id == "1201941624243109888"
    d = classify([deep], {}, main_alive=False, netuid=12)
    assert d.topology_class == "unknown" and d.unknown_targets == ["main_guild_channels"]
    d = classify([tgt("oroInv15", netuid=15)], {"oroInv15": inv("oroInv15", "invite_main_channel_oro.json")},
                 netuid=15)
    assert d.topology_class == "main_channel" and d.main_channel_id == "1466080859164643389"


def test_classify_precedence():
    dead = {"bad": inv("bad", "invite_10006.json")}
    handle = tgt("tl_arrash", platform="discord_handle", netuid=8)
    assert classify([tgt("bad", netuid=8), handle], dead, netuid=8).topology_class == "invalid"
    assert classify([handle], {}, netuid=8).topology_class == "handle_only"
    van = {"bittensor": inv("bittensor", "invite_vanity_bittensor.json")}
    assert classify([tgt("bittensor", "subnetalpha")], van).topology_class == "main_vanity"
    d = classify([], {}, ident="absent")
    assert d.topology_class == "none" and d.reason == "no_discord"
    assert classify([], {}, ident="present").reason == "no_pollable_target"      # total classification
    assert classify([], {}, ident="unknown").topology_class == "unknown"
    # an unknown own-server code with the vanity valid: still a positive class, but unknown_targets is carried
    d = classify([tgt("bittensor", "subnetalpha"), tgt("own1")], van)
    assert d.topology_class == "main_vanity" and d.unknown_targets == ["own1"]


def test_primary_stickiness_and_agreement():
    a, b = own("codeA", "GA", 390), own("codeB", "GB", 900)
    ts = [tgt("codeA", netuid=49), tgt("codeB", "subnetalpha", prio=20, netuid=49)]
    d = classify(ts, {"codeA": a, "codeB": b}, prev="GA", netuid=49)
    assert d.primary_guild_id == "GA"                                     # previous primary kept though smaller
    d = classify(ts, {"codeB": b, "codeA": InviteResult("codeA", D0, 404, 0, 10006)}, prev="GA", netuid=49)
    assert d.primary_guild_id == "GB"
    d = classify(ts, {"codeA": a, "codeB": b}, netuid=49)
    assert d.primary_code == "codeA"                                      # chain-resolved wins without history
    assert source_agreement(ts, {"codeA": a, "codeB": b}, "bittensor") == "disagree"
    ts2 = [tgt("codeA", netuid=49), tgt("codeA2", "subnetalpha", netuid=49)]
    assert source_agreement(ts2, {"codeA": a, "codeA2": own("codeA2", "GA")}, "bittensor") == "agree"
    # legacy (frozen fundamentals.json) never counts as a live claim
    ts3 = [tgt("codeA", netuid=49), tgt("codeB", "legacy", netuid=49)]
    assert source_agreement(ts3, {"codeA": a, "codeB": b}, "bittensor") == "single_source"
