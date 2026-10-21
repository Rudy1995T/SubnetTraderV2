"""Spec 09 A.1–A.2 / §9.1 ``test_news.py``: Google News query set (top-N by emission_share, chain fallback),
" - Publisher" stripping, title+publisher hashing (decoding off) vs decoding on, ``query`` attribution, per-host
spacing on a fake clock, 429 back-off, and the Google + publisher double count in ``news_mentions_7d``.
Fixtures trimmed from live responses (2026-10-18): ``q=Bittensor`` and ``q="Chutes" Bittensor``."""
from __future__ import annotations

import hashlib
import json

import httpx
import pytest
import respx

from app.collectors.news import (google_title_hash, publisher_keyword_match, query_url, run_news_poll,
                                 strip_publisher_suffix, top_n_occupants)
from app.collectors.rss import FeedPoller
from app.nlp.match import OccupantIndex
from app.nlp.textnorm import text_norm
from tests.collectors.feeds.conftest import DAY, NOW, fixture_bytes, seed_feed_ok, seed_occupant, seed_watch

G1 = "https://news.google.com/rss/search?q=Bittensor&hl=en-US&gl=US&ceid=US:en"
CHUTES_Q = 'https://news.google.com/rss/search?q=%22Chutes%22+Bittensor&hl=en-US&gl=US&ceid=US:en'


class FakeClock:
    def __init__(self) -> None:
        self.t = 1000.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.t

    async def sleep(self, w: float) -> None:
        self.sleeps.append(w)
        self.t += w


def xml(name: str) -> httpx.Response:
    return httpx.Response(200, content=fixture_bytes("rss", name), headers={"content-type": "application/xml"})


async def emission(db, okey: str, value: float, as_of: int) -> None:
    await db.execute("INSERT INTO metric_values (occupant_key, metric_key, as_of, spec_version, stage, value, "
                     "value_json, observed_at, source, run_id) VALUES (?, 'emission_share', ?, 'ts-1.0', 'raw', ?, "
                     "'{\"status\":\"ok\"}', ?, 'chain', 'seed')", (okey, as_of, value, as_of))


async def chain_emission(db, okey: str, value: float, hour: int) -> None:
    await db.execute("INSERT INTO chain_hourly (netuid, hour_ts, occupant_key, event_at, observed_at, source, emission) "
                     "VALUES (?,?,?,?,?, 'rest_table', ?)", (int(okey.split(":")[0]), hour, okey, hour, hour, value))


def test_title_helpers():
    assert strip_publisher_suffix("Better Buy: Bittensor or Bitcoin? - Yahoo Finance", "Yahoo Finance") == \
        "Better Buy: Bittensor or Bitcoin?"
    assert strip_publisher_suffix("A - B - 24/7 Wall St.", "24/7 Wall St.") == "A - B"
    assert strip_publisher_suffix("No suffix here", "Yahoo Finance") == "No suffix here"
    assert google_title_hash("Chutes  Wins", "cointelegraph.com") == hashlib.sha256(
        b"t:chutes wins|p:cointelegraph.com").hexdigest()
    assert publisher_keyword_match("Why $TAO and dTAO matter", "bittensor,dtao,$tao")
    assert not publisher_keyword_match("Bitcoin ETF flows", "bittensor,dtao,$tao")


def test_query_url():
    assert query_url("Chutes", 64) == CHUTES_Q
    assert query_url("subnet103", 103) == ("https://news.google.com/rss/search?q=%22SN103%22+Bittensor"
                                           "&hl=en-US&gl=US&ceid=US:en")


@pytest.mark.asyncio
async def test_top_n_with_chain_fallback(db, fset):
    o = {n: await seed_occupant(db, n, f"Name{n}") for n in (1, 2, 3, 4, 5)}
    day = NOW - NOW % DAY
    await emission(db, o[1], 0.05, day)
    await emission(db, o[2], 0.09, day)
    await emission(db, o[2], 0.01, day - DAY)                     # older row of the same occupant ignored
    await emission(db, o[3], 0.50, day - 5 * DAY)                 # stale (> 2 d) → not used
    await chain_emission(db, o[4], 3.0, NOW - 3600)
    await chain_emission(db, o[5], 1.0, NOW - 3600)
    await chain_emission(db, o[1], 9.0, NOW - 3600)               # already chosen through emission_share
    top = await top_n_occupants(db, 3, now=NOW)
    assert [r["netuid"] for r in top] == [2, 1, 4]
    assert [r["netuid"] for r in await top_n_occupants(db, 1, now=NOW)] == [2]


@pytest.mark.asyncio
@respx.mock
async def test_google_poll_rows_attribution_spacing(db, http, fset):
    o64 = await seed_occupant(db, 64, "Chutes")
    await emission(db, o64, 0.08, NOW - NOW % DAY)
    await seed_watch(db, None, "google_news", G1, url=G1, source="seed_feeds")
    clock = FakeClock()
    times: list[float] = []

    def on(name):
        def _h(request):
            times.append(clock())
            return xml(name)
        return _h
    respx.get(G1).mock(side_effect=on("google_news.xml"))
    respx.get(CHUTES_Q).mock(side_effect=on("google_news_chutes.xml"))
    p = FeedPoller(db, http, sleep=clock.sleep, clock=clock)
    rep = await run_news_poll(db, http, now=NOW, run_id="r1", poller=p)
    assert (rep["feeds"], rep["queries"], rep["ok"], rep["new"]) == (2, 1, 2, 8)
    assert len(times) == 2 and times[1] - times[0] >= 10.0 and clock.sleeps == [10.0]

    rows = await db.fetchall("SELECT * FROM news_items WHERE feed_url = ? ORDER BY event_at DESC", (G1,))
    assert len(rows) == 5
    better = [r for r in rows if r["title"] == "Better Buy for the AI Boom: Bittensor or Bitcoin?"]
    assert sorted(r["publisher_domain"] for r in better) == ["247wallst.com", "finance.yahoo.com"]   # 2 publishers
    for r in rows:
        assert r["canonical_url"] is None and r["source"] == "google_news" and r["summary"] is None
        assert r["url"].startswith("https://news.google.com/rss/articles/")
        assert r["url_hash"] == hashlib.sha256(
            f"t:{text_norm(r['title'])}|p:{r['publisher_domain']}".encode()).hexdigest()
        assert not r["title"].endswith(" - " + (r["publisher"] or "~"))
    yahoo = next(r for r in better if r["publisher_domain"] == "finance.yahoo.com")
    assert yahoo["publisher"] == "Yahoo Finance" and yahoo["event_at_source"] == "feed"

    q = await db.fetchall("SELECT * FROM news_items WHERE feed_url = ?", (CHUTES_Q,))
    assert len(q) == 3
    for r in q:
        assert json.loads(r["occupant_keys_json"])[0] == {"occupant_key": o64, "kind": "query"}
    st = await db.fetchone("SELECT * FROM feed_state WHERE feed_url = ?", (CHUTES_Q,))
    assert st["occupant_key"] == o64 and st["kind"] == "google_news" and st["last_status"] == 200


@pytest.mark.asyncio
@respx.mock
async def test_google_429_stops_remaining_queries(db, http, fset):
    for n, name in ((64, "Chutes"), (4, "Targon")):
        await emission(db, await seed_occupant(db, n, name), 0.1 if n == 64 else 0.05, NOW - NOW % DAY)
    await seed_watch(db, None, "google_news", G1, url=G1, source="seed_feeds")
    g = respx.get(G1).mock(return_value=httpx.Response(429))
    other = respx.get(url__regex=r"https://news\.google\.com/rss/search\?q=%22.*").mock(
        return_value=xml("google_news_chutes.xml"))
    clock = FakeClock()
    rep = await run_news_poll(db, http, now=NOW, run_id="r1",
                              poller=FeedPoller(db, http, sleep=clock.sleep, clock=clock))
    assert g.call_count == 1 and other.call_count == 0
    assert (rep["failed"], rep["skipped"], rep["partial"]) == (1, 2, "rate_limited")


@pytest.mark.asyncio
@respx.mock
async def test_decode_on(db, http, fset):
    fset(RSS_GOOGLE_NEWS_DECODE_URLS=True)
    await seed_watch(db, None, "google_news", G1, url=G1, source="seed_feeds")
    respx.get(G1).mock(return_value=xml("google_news.xml"))
    calls = {"n": 0}

    def art(request):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(302, headers={"location": "https://finance.yahoo.com/news/better-buy?utm_source=x"})
        return httpx.Response(200, text="<html>js redirect</html>")
    respx.get(url__regex=r"https://news\.google\.com/rss/articles/.*").mock(side_effect=art)
    clock = FakeClock()
    p = FeedPoller(db, http, index=await OccupantIndex.load(db), sleep=clock.sleep, clock=clock)
    rep = await run_news_poll(db, http, now=NOW, run_id="r1", poller=p)
    assert rep["new"] == 5 and calls["n"] == 5 and p.decode_budget == 55
    rows = await db.fetchall("SELECT * FROM news_items WHERE canonical_url IS NOT NULL")
    assert len(rows) == 1 and rows[0]["canonical_url"] == "https://finance.yahoo.com/news/better-buy"
    assert rows[0]["url_hash"] == hashlib.sha256(rows[0]["canonical_url"].encode()).hexdigest()
    assert all(w == 10.0 for w in clock.sleeps) and len(clock.sleeps) == 5        # article GETs spaced too


@pytest.mark.asyncio
async def test_news_mentions_counts_google_and_publisher_once(db, fset):
    from app.features.social_feeds import compute_news_mentions_7d
    o64 = await seed_occupant(db, 64, "Chutes")
    occ = json.dumps([{"occupant_key": o64, "kind": "rule"}])
    title = "Bittensor subnet Chutes passes 100B tokens a day"
    for uh, url, feed, src in (("a", "https://news.google.com/rss/articles/X", "g", "google_news"),
                               ("b", "https://cointelegraph.com/news/chutes", "https://cointelegraph.com/rss", "rss")):
        await db.execute("INSERT INTO news_items (url_hash, url, title, publisher_domain, feed_url, occupant_keys_json, "
                         "text_hash, event_at, event_at_source, observed_at, source) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                         (uh, url, title, "cointelegraph.com", feed, occ, "h", NOW - DAY, "feed", NOW - DAY, src))
    await seed_feed_ok(db, "https://cointelegraph.com/rss", NOW - 3600)
    r = await compute_news_mentions_7d(db, o64, NOW)
    assert r.status == "ok" and r.value == 1.0 and r.value_json["items"] == 2
