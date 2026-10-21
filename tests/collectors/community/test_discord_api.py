"""Spec 08 §9 #1–#9: invite/widget parsing, snowflakes, fetch behaviour, the invalid-request guard."""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone

import httpx
import pytest
import respx

from app.collectors.discord_api import (CloudflareBan, InvalidRequestGuard, fetch_invite, parse_invite, parse_widget,
                                        snowflake_to_datetime)
from app.collectors.discord_topology import main_channel_match
from app.util.time import parse_iso
from tests.collectors.community.conftest import API, MAIN, community_settings, fixture_bytes

T0 = parse_iso("2026-09-22T12:00:00Z")


def test_parse_invite_own_server():
    r = parse_invite("fwjBwxUcdX", 200, fixture_bytes("invite_own_server_nodexo.json"), T0)
    assert r.invite_valid == 1 and r.guild_id == "1039241590092353596"
    assert (r.member_count, r.presence_count) == (1986, 142)
    assert r.presence_ratio == pytest.approx(0.0715, abs=1e-4)
    assert r.landing_channel_type == 5 and r.features == ("COMMUNITY", "NEWS")
    assert r.guild_created_at == datetime(2022, 11, 7, 18, 15, 3, 253000, tzinfo=timezone.utc)
    assert int(r.guild_created_at.timestamp()) == 1667844903
    dumped = json.dumps(asdict(r), default=str)
    assert "inviter" not in dumped and "someone" not in dumped and "Some One" not in dumped


def test_parse_invite_vanity():
    r = parse_invite("bittensor", 200, fixture_bytes("invite_vanity_bittensor.json"), T0)
    assert r.guild_id == MAIN and (r.member_count, r.presence_count) == (50905, 5575)
    assert r.verification_level == 4 and r.boosts == 63 and r.meta["vanity_url_code"] == "bittensor"
    assert int(r.guild_created_at.timestamp()) == 1610727064


def test_main_channel_match():
    oro = parse_invite("oroInv15", 200, fixture_bytes("invite_main_channel_oro.json"), T0)
    assert main_channel_match(oro, 15, set()) is True
    assert main_channel_match(oro, 16, set()) is False
    van = parse_invite("bittensor", 200, fixture_bytes("invite_vanity_bittensor.json"), T0)
    assert main_channel_match(van, 15, set()) is False          # vanity lands on 📣・announce (live 2026-10-17)
    assert main_channel_match(oro, 16, {"1466080859164643389"}) is True


def test_parse_invite_errors():
    r = parse_invite("t7BMee4w", 404, fixture_bytes("invite_10006.json"), T0)
    assert (r.invite_valid, r.invite_error_code) == (0, 10006)
    r = parse_invite("x", 404, fixture_bytes("invite_50270.json"), T0)
    assert (r.invite_valid, r.invite_error_code) == (0, 50270)
    r = parse_invite("x", 404, b"<html>not found</html>", T0)
    assert r.invite_valid is None and r.error == "html_404"
    r = parse_invite("gdmInv", 200, fixture_bytes("invite_group_dm.json"), T0)
    assert r.invite_valid == 0 and r.error == "not_guild_invite"
    assert "someone" not in json.dumps(asdict(r), default=str)
    assert parse_invite("x", 503, b"", T0).invite_valid is None
    assert parse_invite("x", None, None, T0).error == "transport"


def test_expired_by_clock():
    body = fixture_bytes("invite_expiring_umi.json")
    assert parse_invite("umiTemp", 200, body, parse_iso("2026-09-22T12:00:00Z")).invite_valid == 1
    r = parse_invite("umiTemp", 200, body, parse_iso("2026-09-23T00:00:00Z"))
    assert (r.invite_valid, r.invite_error_code, r.error) == (0, 50270, "expired_by_clock")


def test_snowflake():
    assert snowflake_to_datetime("1419378005230485587") == datetime(2025, 9, 21, 17, 41, 27, 215000, timezone.utc)
    assert int(snowflake_to_datetime("1419378005230485587").timestamp()) == 1758476487
    assert snowflake_to_datetime(MAIN) == datetime(2021, 1, 15, 16, 11, 4, 673000, timezone.utc)
    assert int(snowflake_to_datetime("1419380124024836297").timestamp()) == 1758476992


def test_parse_widget_drops_members():
    w = parse_widget("692175473198759938", 200, fixture_bytes("widget_enabled_cooking.json"), T0)
    assert w.widget_enabled == 1 and w.presence_count == 1311 and w.instant_invite_code == "cookInv"
    assert w.channel_count == 1
    dumped = json.dumps(asdict(w))
    assert "alice" not in dumped and "username" not in dumped and "avatar" not in dumped
    d = parse_widget("1", 403, fixture_bytes("widget_disabled.json"), T0)
    assert d.widget_enabled == 0
    u = parse_widget("1", 404, b'{"message": "Unknown Guild", "code": 10004}', T0)
    assert u.widget_enabled is None and u.error == "unknown_guild"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_invite_http(http):
    s = community_settings(DISCORD_BOT_TOKEN="tok123", DISCORD_INVITES_USE_BOT_TOKEN=True,
                           DISCORD_USER_AGENT="DiscordBot (https://example.invalid, 0.1.0)")
    route = respx.get(f"{API}/invites/fwjBwxUcdX").mock(
        return_value=httpx.Response(200, content=fixture_bytes("invite_own_server_nodexo.json")))
    r = await fetch_invite(http, "fwjBwxUcdX", s, guard=InvalidRequestGuard(100))
    assert r.invite_valid == 1
    req = route.calls.last.request
    assert req.headers["User-Agent"].startswith("DiscordBot (") and req.headers["Authorization"] == "Bot tok123"
    assert req.url.params["with_counts"] == "true"
    s2 = community_settings(DISCORD_BOT_TOKEN="tok123", DISCORD_INVITES_USE_BOT_TOKEN=False)
    await fetch_invite(http, "fwjBwxUcdX", s2, guard=InvalidRequestGuard(100))
    assert "Authorization" not in route.calls.last.request.headers
    dead = respx.get(f"{API}/invites/t7BMee4w").mock(
        return_value=httpx.Response(404, content=fixture_bytes("invite_10006.json")))
    r = await fetch_invite(http, "t7BMee4w", s, guard=InvalidRequestGuard(100))
    assert r.invite_valid == 0 and dead.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_fetch_invite_429_json_then_cloudflare(http):
    s = community_settings()
    route = respx.get(f"{API}/invites/abc").mock(side_effect=[
        httpx.Response(429, json={"message": "You are being rate limited.", "retry_after": 0.01, "global": False}),
        httpx.Response(200, content=fixture_bytes("invite_own_server_nodexo.json"))])
    r = await fetch_invite(http, "abc", s, guard=InvalidRequestGuard(100))
    assert r.invite_valid == 1 and route.call_count == 2              # exactly one retry
    respx.get(f"{API}/invites/cf").mock(return_value=httpx.Response(429, content=fixture_bytes("cf_1015.html")))
    with pytest.raises(CloudflareBan):
        await fetch_invite(http, "cf", s, guard=InvalidRequestGuard(100))
    from app.util.time import now_ts
    snap = http.limiter.snapshot()["discord"]
    until = [parse_iso(v) for k, v in snap.items() if "until" in k and isinstance(v, str)]
    assert until and max(until) >= now_ts() + 3500, snap                 # a 3600 s provider-wide penalty


def test_invalid_guard():
    t = [1000.0]
    g = InvalidRequestGuard(100, clock=lambda: t[0])
    for _ in range(100):
        g.record(403)
    assert g.allowed() is False and g.count() == 100
    t[0] += 601
    assert g.allowed() is True
    g.record(404)
    assert g.count() == 0
