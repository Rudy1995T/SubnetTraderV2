"""Spec 08 §9 #26–#27: counts, profile kinds (markup verified live 2026-10-17), previews, layout-change failure."""
from __future__ import annotations

import httpx
import pytest
import respx

from app.collectors.telegram import TmeLayoutChanged, parse_count, parse_tme_preview, parse_tme_profile, run_telegram_pass
from app.util.time import parse_iso
from tests.collectors.community.conftest import DAY, NOW, fixture_text, seed_occupant, seed_watch

TME = "https://t.me"


def prof(name, private=False, status=200):
    return parse_tme_profile(fixture_text(name), private=private, status=status)


def test_parse_count():
    assert parse_count("23 831") == 23831
    assert parse_count("23 326") == 23326
    assert parse_count("1.2K") == 1200 and parse_count("5.81M") == 5810000
    assert parse_count("abc") is None and parse_count("") is None


def test_profiles():
    p = prof("tme_group_natix.html")
    assert (p.kind, p.members, p.online, p.parse_ok, p.title) == ("group", 23831, 619, 1, "NATIX Network")
    assert prof("tme_group_inference_labs.html").members == 7393
    p = prof("tme_private_channel_tensorplex.html", private=True)
    assert (p.kind, p.members) == ("private_channel", 255)
    p = prof("tme_private_group_bitmind.html", private=True)
    assert (p.kind, p.members, p.online, p.deprecated_marker) == ("private_group", 472, 29, 1)
    p = prof("tme_user_wahoo.html")
    assert (p.kind, p.title, p.parse_ok) == ("user_or_unknown", None, 1)
    p = prof("tme_bot.html")                                   # two tgme_page_extra blocks
    assert (p.kind, p.members, p.title) == ("bot", 7594066, None)
    assert prof("tme_channel_public.html").kind == "channel"
    assert prof("tme_not_found.html").kind == "not_found"
    assert prof("tme_not_found.html", status=404).kind == "not_found"
    p = prof("tme_layout_changed.html")
    assert (p.kind, p.parse_ok) == (None, 0)                   # a layout change is a failure, never absence


def test_preview():
    posts = parse_tme_preview(fixture_text("tme_preview_channel.html"), "SubnetNews")
    assert [p.post_no for p in posts] == [101, 103]            # service message skipped
    p = posts[0]
    assert p.event_at == parse_iso("2026-10-15T10:00:00Z") and p.views == 1200
    assert p.text.startswith("Mainnet v2 is live!\nDetails:") and "&amp;" not in p.text and p.url_count == 1
    q = posts[1]
    assert q.has_attachment == 1 and q.text == "" and q.views == 987            # reply quote not taken as text


async def seed_tg(db):
    o72 = await seed_occupant(db, 72)
    o99 = await seed_occupant(db, 99)
    o30 = await seed_occupant(db, 30)
    await seed_watch(db, o72, "telegram", "NATIXNetwork", source="subnetalpha")
    await seed_watch(db, o99, "telegram", "SubnetNews", source="subnetalpha")
    await seed_watch(db, o30, "telegram", "wahoopredict_support", source="subnetalpha")
    return o72, o99, o30


@pytest.mark.asyncio
@pytest.mark.parametrize("store_text", [False, True])
@respx.mock
async def test_run_pass(db, http, cset, frozen, store_text):
    s = cset(TELEGRAM_POSTS_STORE_TEXT=store_text)
    o72, o99, o30 = await seed_tg(db)
    respx.get(f"{TME}/NATIXNetwork").mock(return_value=httpx.Response(200, text=fixture_text("tme_group_natix.html")))
    respx.get(f"{TME}/SubnetNews").mock(return_value=httpx.Response(200, text=fixture_text("tme_channel_public.html")))
    respx.get(f"{TME}/wahoopredict_support").mock(return_value=httpx.Response(200,
                                                                              text=fixture_text("tme_user_wahoo.html")))
    prev = respx.get(f"{TME}/s/SubnetNews").mock(return_value=httpx.Response(
        200, text=fixture_text("tme_preview_channel.html")))
    rep = await run_telegram_pass(db, http, now=NOW, run_id="t1", s=s)
    assert rep["rows"] == 3 and rep["posts"] == 2 and prev.call_count == 1
    snaps = {r["handle"]: r for r in await db.fetchall("SELECT * FROM telegram_snapshots")}
    assert snaps["NATIXNetwork"]["members"] == 23831 and snaps["NATIXNetwork"]["title"] == "NATIX Network"
    assert snaps["wahoopredict_support"]["title"] is None and snaps["wahoopredict_support"]["kind"] == "user_or_unknown"
    sn = snaps["SubnetNews"]
    assert (sn["preview_available"], sn["preview_posts"], sn["last_post_at"]) == (1, 2,
                                                                                  parse_iso("2026-10-16T09:30:00Z"))
    ann = await db.fetchall("SELECT * FROM announcements ORDER BY msg_id")
    assert [a["msg_id"] for a in ann] == ["telegram:subnetnews/101", "telegram:subnetnews/103"]
    a = ann[0]
    assert a["via_own_server"] == 0 and a["mapping"] == "tg_handle" and a["author_is_team"] == 1 and a["text"]
    assert a["text_purge_after"] == (None if store_text else a["observed_at"] + 7 * DAY)
    m = await db.fetchall("SELECT post_id, occupant_key FROM social_post_mentions WHERE platform = 'telegram'")
    assert {r["post_id"] for r in m} == {"subnetnews/101", "subnetnews/103"} and {r["occupant_key"] for r in m} == {o99}
    # second run the same day: first attempt wins, nothing fetched
    rep = await run_telegram_pass(db, http, now=NOW + 3600, run_id="t2", s=s)
    assert rep["due"] == 0 and await db.fetchval("SELECT COUNT(*) FROM telegram_snapshots") == 3


@pytest.mark.asyncio
@respx.mock
async def test_group_preview_redirect_and_layout_change(db, http, cset, frozen):
    s = cset()
    o99 = await seed_occupant(db, 99)
    await seed_watch(db, o99, "telegram", "SubnetNews", source="subnetalpha")
    respx.get(f"{TME}/SubnetNews").mock(return_value=httpx.Response(200, text=fixture_text("tme_channel_public.html")))
    respx.get(f"{TME}/s/SubnetNews").mock(return_value=httpx.Response(302, headers={"location": f"{TME}/SubnetNews"}))
    await run_telegram_pass(db, http, now=NOW, run_id="t1", s=s)
    r = await db.fetchone("SELECT preview_available, preview_posts FROM telegram_snapshots")
    assert (r["preview_available"], r["preview_posts"]) == (0, 0)
    respx.get(f"{TME}/SubnetNews").mock(return_value=httpx.Response(200,
                                                                     text=fixture_text("tme_layout_changed.html")))
    frozen(NOW + DAY)
    with pytest.raises(TmeLayoutChanged):
        await run_telegram_pass(db, http, now=NOW + DAY, run_id="t2", s=s)
    r = await db.fetchone("SELECT parse_ok, kind FROM telegram_snapshots WHERE cycle_day_ts = ?",
                          (NOW + DAY - (NOW + DAY) % DAY,))
    assert (r["parse_ok"], r["kind"]) == (0, None)
