"""Spec 09 A.2–A.3 / §9.1 ``test_youtube.py``: channel-id discovery order (path → API → video page → oEmbed →
channel page), the consent wall, the own-channel test (taostats = ecosystem, shared channels, ``meta_json.ecosystem``),
``meta_json`` merges that keep ``handle``, weekly retry, and ``video_items`` from channel + playlist RSS.
Fixtures trimmed from live responses 2026-10-18 (video page of ``XsNSghevAAQ``, its oEmbed, the Novelty Search
playlist feed, the taostats channel feed, the consent interstitial); ``channel_page.html`` is synthetic."""
from __future__ import annotations

import json

import httpx
import pytest
import respx

from app.collectors.rss import FeedPoller
from app.collectors.youtube import (PLAYLIST_FEED, YOUTUBE_ECOSYSTEM_CHANNELS, YoutubePollJob, channel_id_in,
                                    classify_channels, discover_channel_id, handle_path_of, resolve_pending,
                                    run_youtube_poll, video_id_of, youtube_feeds)
from tests.collectors.feeds.conftest import DAY, NOW, fixture_bytes, fixture_text, seed_occupant, seed_watch

TAOSTATS = "UCxLqly5YxULobLJwcYjMhJQ"
OWN = "UCownChannel64aaaaaaaaaa"
SHARED = "UCsharedChannelbbbbbbbbb"
PID = "PLN0WJc6io8zqEknIutfyEjiJViazrxrzU"
YT = "https://www.youtube.com"


def video_page(cid: str) -> httpx.Response:
    return httpx.Response(200, text=f'<html><script>{{"videoDetails":{{"videoId":"abcdefghijk","channelId":"{cid}",'
                                    f'"x":1}},"externalChannelId":"{cid}"}}</script></html>')


def test_url_helpers():
    assert video_id_of("XsNSghevAAQ") == "XsNSghevAAQ"
    assert video_id_of("https://www.youtube.com/watch?v=XsNSghevAAQ&t=3") == "XsNSghevAAQ"
    assert video_id_of("https://youtu.be/XsNSghevAAQ") == "XsNSghevAAQ"
    assert video_id_of("https://www.youtube.com/shorts/XsNSghevAAQ") == "XsNSghevAAQ"
    assert video_id_of("@taostats") is None and video_id_of("https://www.youtube.com/@taostats") is None
    assert handle_path_of("@taostats") == "@taostats" and handle_path_of("https://www.youtube.com/@taostats/videos") == \
        "@taostats"
    assert handle_path_of("https://www.youtube.com/c/Foo") == "c/Foo"
    assert channel_id_in(TAOSTATS) == TAOSTATS and channel_id_in(f"{YT}/channel/{TAOSTATS}/videos") == TAOSTATS
    assert channel_id_in(f"{YT}/feeds/videos.xml?channel_id={TAOSTATS}") == TAOSTATS
    assert TAOSTATS in YOUTUBE_ECOSYSTEM_CHANNELS


@pytest.mark.asyncio
@respx.mock
async def test_discover_video_page_live_fixture(http, fset):
    route = respx.get(f"{YT}/watch", params={"v": "XsNSghevAAQ"}).mock(
        return_value=httpx.Response(200, text=fixture_text("youtube", "video_page.html")))
    assert await discover_channel_id("https://www.youtube.com/watch?v=XsNSghevAAQ", http) == (TAOSTATS, "video_page")
    h = route.calls[0].request.headers
    assert h["cookie"] == "CONSENT=YES+cb" and "Mozilla" in h["user-agent"]
    assert await discover_channel_id(f"{YT}/channel/{TAOSTATS}", http) == (TAOSTATS, "path")
    assert route.call_count == 1                                          # 'path' needs no request


@pytest.mark.asyncio
@respx.mock
async def test_discover_oembed_then_channel_page(http, fset):
    respx.get(f"{YT}/watch").mock(return_value=httpx.Response(302, headers={
        "location": "https://consent.youtube.com/m?continue=x"}))         # consent wall: not followed
    respx.get(f"{YT}/oembed").mock(return_value=httpx.Response(200, content=fixture_bytes("youtube", "oembed.json")))
    page = respx.get(f"{YT}/@taostats").mock(return_value=httpx.Response(
        200, text=fixture_text("youtube", "consent_page.html")))
    assert await discover_channel_id("XsNSghevAAQ", http) == (None, "none")      # live 2026-10-18 behaviour
    page.mock(return_value=httpx.Response(200, text=fixture_text("youtube", "channel_page.html")))
    assert await discover_channel_id("XsNSghevAAQ", http) == ("UCabcdefghijklmnopqrstuv", "externalId")
    assert await discover_channel_id("@taostats", http) == ("UCabcdefghijklmnopqrstuv", "externalId")
    respx.get(f"{YT}/oembed").mock(return_value=httpx.Response(200, json={"author_url": f"{YT}/channel/{OWN}"}))
    assert await discover_channel_id("XsNSghevAAQ", http) == (OWN, "oembed")


@pytest.mark.asyncio
@respx.mock
async def test_discover_api_first_with_key(http, fset):
    v = respx.get("https://www.googleapis.com/youtube/v3/videos").mock(
        return_value=httpx.Response(200, json={"items": [{"snippet": {"channelId": OWN}}]}))
    c = respx.get("https://www.googleapis.com/youtube/v3/channels").mock(
        return_value=httpx.Response(200, json={"items": [{"id": SHARED}]}))
    page = respx.get(f"{YT}/watch").mock(return_value=video_page(TAOSTATS))
    assert await discover_channel_id("XsNSghevAAQ", http, api_key="k") == (OWN, "api")
    assert v.calls[0].request.url.params["id"] == "XsNSghevAAQ" and page.call_count == 0
    assert await discover_channel_id("@foo", http, api_key="k") == (SHARED, "api")
    assert c.calls[0].request.url.params["forHandle"] == "@foo"
    v.mock(return_value=httpx.Response(200, json={"items": []}))            # API miss → keyless steps
    assert await discover_channel_id("XsNSghevAAQ", http, api_key="k") == (TAOSTATS, "video_page")


async def seed_youtube(db):
    o1 = await seed_occupant(db, 1, "Apex")
    o64 = await seed_occupant(db, 64, "Chutes")
    o10 = await seed_occupant(db, 10, "Pareton")
    o11 = await seed_occupant(db, 11, "Dippy")
    o20 = await seed_occupant(db, 20, "Declared")
    o4 = await seed_occupant(db, 4, "Targon")
    ids = {
        "apex": await seed_watch(db, o1, "youtube_video", "XsNSghevAAQ", url=f"{YT}/watch?v=XsNSghevAAQ"),
        "chutes": await seed_watch(db, o64, "youtube_video", "vid64aaaaaa", url=f"{YT}/watch?v=vid64aaaaaa"),
        "p10": await seed_watch(db, o10, "youtube_video", "vid10aaaaaa"),
        "p11": await seed_watch(db, o11, "youtube_video", "vid11aaaaaa"),
        "decl": await seed_watch(db, o20, "youtube", "UCdeclared20cccccccccccc", url=f"{YT}/channel/UCdeclared20cccccccccccc"),
        "fail": await seed_watch(db, o4, "youtube_video", "vid04aaaaaa"),
    }
    return (o1, o64, o10, o11, o20, o4), ids


@pytest.mark.asyncio
@respx.mock
async def test_resolve_and_own_channel_test(db, http, fset):
    (o1, o64, o10, o11, o20, o4), ids = await seed_youtube(db)
    respx.get(f"{YT}/watch", params={"v": "XsNSghevAAQ"}).mock(return_value=video_page(TAOSTATS))
    respx.get(f"{YT}/watch", params={"v": "vid64aaaaaa"}).mock(return_value=video_page(OWN))
    respx.get(f"{YT}/watch", params={"v": "vid10aaaaaa"}).mock(return_value=video_page(SHARED))
    respx.get(f"{YT}/watch", params={"v": "vid11aaaaaa"}).mock(return_value=video_page(SHARED))
    respx.get(f"{YT}/watch", params={"v": "vid04aaaaaa"}).mock(return_value=httpx.Response(404))
    respx.get(f"{YT}/oembed").mock(return_value=httpx.Response(404))
    rep = await resolve_pending(db, http, now=NOW, run_id="r1")
    assert (rep["pending"], rep["resolved"], rep["failed"]) == (6, 5, 1)
    roles = await classify_channels(db)
    assert roles == {"own": 1, "ecosystem": 1, "shared": 2}
    rows = {k: await db.fetchone("SELECT * FROM watch_channels WHERE id = ?", (i,)) for k, i in ids.items()}
    meta = {k: json.loads(r["meta_json"]) for k, r in rows.items()}
    assert meta["apex"]["channel_id"] == TAOSTATS and meta["apex"]["channel_role"] == "ecosystem"
    assert meta["chutes"]["channel_role"] == "own" and meta["chutes"]["channel_id_method"] == "video_page"
    assert meta["p10"]["channel_role"] == meta["p11"]["channel_role"] == "shared"
    assert meta["decl"]["channel_id"] == "UCdeclared20cccccccccccc" and meta["decl"]["channel_id_method"] == "path"
    assert meta["chutes"]["declared_by"] == ["subnetalpha"]                    # merge keeps other keys
    assert rows["chutes"]["handle"] == "vid64aaaaaa" and rows["chutes"]["verified_at"] == NOW   # handle unchanged
    assert "channel_id" not in meta["fail"] and meta["fail"]["channel_id_attempted_at"] == NOW
    ev = await db.fetchall("SELECT occupant_key, payload FROM events WHERE event_type = 'youtube_channel_resolved'")
    assert len(ev) == 5
    p = next(json.loads(e["payload"]) for e in ev if e["occupant_key"] == o64)
    assert p == {"input_url": f"{YT}/watch?v=vid64aaaaaa", "channel_id": OWN, "method": "video_page",
                 "platform": "youtube_video"}
    assert (await resolve_pending(db, http, now=NOW + DAY, run_id="r2"))["pending"] == 0     # weekly retry
    assert (await resolve_pending(db, http, now=NOW + 8 * DAY, run_id="r3"))["pending"] == 1
    # operator marks the shared channel as an ecosystem channel on any row
    await db.execute("UPDATE watch_channels SET meta_json = json_set(meta_json, '$.ecosystem', json('true')) "
                     "WHERE id = ?", (ids["p10"],))
    assert (await classify_channels(db))["ecosystem"] == 3
    feeds = {f.url: f for f in await youtube_feeds(db)}
    assert set(feeds) == {PLAYLIST_FEED.format(PID), f"{YT}/feeds/videos.xml?channel_id={OWN}",
                          f"{YT}/feeds/videos.xml?channel_id=UCdeclared20cccccccccccc"}
    assert feeds[f"{YT}/feeds/videos.xml?channel_id={OWN}"].occupant_key == o64
    assert feeds[PLAYLIST_FEED.format(PID)].playlist_id == PID and all(f.provider == "youtube" for f in feeds.values())


@pytest.mark.asyncio
@respx.mock
async def test_poll_video_items(db, http, fset):
    (o1, o64, o10, o11, o20, o4), ids = await seed_youtube(db)
    await db.execute("UPDATE watch_channels SET meta_json = json_set(meta_json, '$.channel_id', ?, '$.channel_role', "
                     "'own', '$.channel_id_attempted_at', ?) WHERE id = ?", (OWN, NOW, ids["chutes"]))
    await db.execute("UPDATE watch_channels SET retired_at = ? WHERE id IN (?, ?, ?, ?, ?)",
                     (NOW - DAY, ids["apex"], ids["p10"], ids["p11"], ids["fail"], ids["decl"]))
    respx.get(PLAYLIST_FEED.format(PID)).mock(return_value=httpx.Response(
        200, content=fixture_bytes("youtube", "playlist_rss.xml")))
    respx.get(f"{YT}/feeds/videos.xml", params={"channel_id": OWN}).mock(return_value=httpx.Response(
        200, content=fixture_bytes("youtube", "channel_rss.xml").replace(TAOSTATS.encode(), OWN.encode())))
    rep = await run_youtube_poll(db, http, now=NOW, run_id="r1")
    assert (rep["feeds"], rep["ok"], rep["new"]) == (2, 2, 7)
    pl = {r["video_id"]: r for r in await db.fetchall("SELECT * FROM video_items WHERE source = 'playlist_rss'")}
    assert len(pl) == 4
    t = pl["JfFWZF1GSn4"]
    assert (t["playlist_id"], t["occupant_key"], t["views"], t["channel_id"]) == (PID, None, 678, "UCq-UgxLUK1mYJBxrmOfP7aA")
    assert t["url"] == "https://www.youtube.com/watch?v=JfFWZF1GSn4" and t["title"].startswith("Bittensor’s eTao")
    assert {"occupant_key": o4, "kind": "rule"} in json.loads(t["occupant_keys_json"])       # "SN4 Targon"
    assert {"occupant_key": o10, "kind": "rule"} in json.loads(pl["bpJK99kV5so"]["occupant_keys_json"])  # "(SN10)"
    ch = await db.fetchall("SELECT * FROM video_items WHERE source = 'channel_rss'")
    assert len(ch) == 3 and all(r["occupant_key"] == o64 and r["channel_id"] == OWN and r["occupant_keys_json"] == "[]"
                                for r in ch)
    rep = await run_youtube_poll(db, http, now=NOW + DAY, run_id="r2")
    assert rep["new"] == 0 and await db.fetchval("SELECT COUNT(*) FROM video_items") == 7


@pytest.mark.asyncio
@respx.mock
async def test_job_collect(rt):
    import app.config as app_config
    from app.jobs import SPEC_BY_ID
    s = app_config.settings
    s.RSS_ENABLED = True
    s.YOUTUBE_ENABLED = True
    respx.get(PLAYLIST_FEED.format(PID)).mock(return_value=httpx.Response(
        200, content=fixture_bytes("youtube", "playlist_rss.xml")))
    job = YoutubePollJob(rt, SPEC_BY_ID["youtube_poll"])
    await job.run(trigger="manual")
    row = await rt.db.fetchone("SELECT ok, rows, note FROM collector_runs WHERE job = 'youtube_poll'")
    assert row["ok"] == 1 and row["rows"] == 4 and row["note"].startswith("feeds=1 ok=1")
    assert isinstance(FeedPoller, type)
