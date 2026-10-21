"""Spec 09 A.1–A.3 / §9.1 ``test_rss.py``: seed registry, ETag/Last-Modified + 304, bozo failure, keyword filter,
``url_hash`` dedupe, permanent redirects, ``feed_dead``, the job wrapper. Feeds trimmed from live responses
(2026-10-18: A Bittensor Journey = Substack, Decrypt = WordPress, Cointelegraph + one synthetic Bittensor item)."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import httpx
import pytest
import respx

from app.collectors.rss import (FeedPoller, FeedRow, RssFeedsPollJob, feed_ok_at, feed_rows, keyword_re,
                                load_seed_feeds, strip_html)
from app.http import ProviderError
from app.nlp.match import OccupantIndex
from tests.collectors.feeds.conftest import DAY, NOW, fixture_bytes, seed_occupant, seed_watch

SEED = Path(__file__).resolve().parents[3] / "seed" / "feeds.csv"
SUB = "https://www.abittensorjourney.com/feed"
CT = "https://cointelegraph.com/rss"


def xml(name: str, **headers) -> httpx.Response:
    return httpx.Response(200, content=fixture_bytes("rss", name), headers={"content-type": "application/xml", **headers})


async def poller(db, http, **kw) -> FeedPoller:
    return FeedPoller(db, http, index=await OccupantIndex.load(db), **kw)


def test_helpers():
    assert strip_html("<p>A &amp; <b>B</b></p>") == "A & B"
    rx = keyword_re("bittensor,dtao,subnet,$tao")
    assert rx.search("Why $TAO pumps") and rx.search("a Subnet launch") and not rx.search("subnets2") \
        and not rx.search("stao")


@pytest.mark.asyncio
async def test_load_seed_feeds(db, fset):
    rep = await load_seed_feeds(db, SEED, now=NOW)
    assert rep == {"rows": 14, "inserted": 14, "skipped": 0}
    rows = {r["label"]: r for r in await db.fetchall("SELECT * FROM watch_channels WHERE source = 'seed_feeds'")}
    dead = {k for k, r in rows.items() if r["retired_at"] is not None}
    assert dead == {"tao.media", "SimplyTao", "Own Your Mind", "bex.co blog"}
    assert rows["Pine Analytics"]["filtered"] == 1 and rows["Cointelegraph"]["filtered"] == 1
    assert rows["A Bittensor Journey"]["filtered"] == 0 and rows["A Bittensor Journey"]["platform"] == "substack"
    nov = rows["Novelty Search playlist"]
    assert nov["platform"] == "youtube" and json.loads(nov["meta_json"])["playlist_id"] == "PLN0WJc6io8zqEknIutfyEjiJViazrxrzU"
    assert all(r["occupant_key"] is None and r["netuid"] is None for r in rows.values())
    await db.execute("UPDATE watch_channels SET filtered = 0 WHERE label = 'Cointelegraph'")      # operator edit
    assert (await load_seed_feeds(db, SEED, now=NOW + DAY))["inserted"] == 0
    assert await db.fetchval("SELECT filtered FROM watch_channels WHERE label = 'Cointelegraph'") == 0
    feeds = await feed_rows(db, ("rss", "substack", "medium"))
    assert len(feeds) == 7 and all(f.kind in ("rss", "substack") for f in feeds)


@pytest.mark.asyncio
@respx.mock
async def test_etag_then_304(db, http, fset):
    await seed_watch(db, None, "substack", SUB, url=SUB, source="seed_feeds")
    seen: list[dict] = []

    def handler(request):
        seen.append(dict(request.headers))
        if len(seen) == 1:
            return xml("substack.xml", etag='W/"b1870-x"', **{"last-modified": "Sat, 17 Oct 2026 10:31:00 GMT"})
        return httpx.Response(304)
    respx.get(SUB).mock(side_effect=handler)
    p = await poller(db, http)
    feeds = await feed_rows(db, ("substack",))
    rep = await p.poll_feeds(feeds, job="rss_feeds_poll", run_id="r1", now=NOW)
    assert (rep["ok"], rep["new"], rep["failed"]) == (1, 3, 0)
    assert "if-none-match" not in seen[0]
    st = await db.fetchone("SELECT * FROM feed_state WHERE feed_url = ?", (SUB,))
    assert (st["etag"], st["last_status"], st["items_seen"], st["kind"]) == ('W/"b1870-x"', 200, 3, "substack")
    assert st["last_new_item_at"] == NOW
    items = await db.fetchall("SELECT * FROM news_items ORDER BY event_at DESC")
    assert [i["source"] for i in items] == ["substack"] * 3
    top = items[0]
    assert top["title"].startswith("Where One Block of TAO Goes") and top["event_at_source"] == "feed"
    assert top["canonical_url"] == "https://www.abittensorjourney.com/p/where-one-block-of-tao-goes-a-step"
    assert top["url_hash"] == hashlib.sha256(top["canonical_url"].encode()).hexdigest()
    assert top["publisher"] == "A Bittensor Journey" and top["publisher_domain"] == "abittensorjourney.com"
    assert top["summary"] and len(top["summary"]) <= 2000 and top["feed_url"] == SUB

    rep = await p.poll_feeds(feeds, job="rss_feeds_poll", run_id="r2", now=NOW + 6 * 3600)
    assert seen[1]["if-none-match"] == 'W/"b1870-x"' and seen[1]["if-modified-since"] == "Sat, 17 Oct 2026 10:31:00 GMT"
    assert (rep["not_modified"], rep["new"]) == (1, 0)
    st = await db.fetchone("SELECT * FROM feed_state WHERE feed_url = ?", (SUB,))
    assert (st["last_status"], st["consecutive_failures"], st["items_seen"], st["etag"]) == (304, 0, 3, 'W/"b1870-x"')
    assert await feed_ok_at(db, SUB) == NOW + 6 * 3600
    assert await db.fetchval("SELECT COUNT(*) FROM news_items") == 3


@pytest.mark.asyncio
@respx.mock
async def test_bozo_failure_and_dedupe(db, http, fset):
    url = "https://decrypt.co/feed"
    await seed_watch(db, None, "rss", url, url=url, source="seed_feeds")
    route = respx.get(url).mock(return_value=httpx.Response(200, content=fixture_bytes("rss", "bozo.xml")))
    p = await poller(db, http)
    feeds = await feed_rows(db, ("rss",))
    rep = await p.poll_feeds(feeds, job="rss_feeds_poll", run_id="r1", now=NOW)
    assert rep["failed"] == 1 and rep["new"] == 0
    st = await db.fetchone("SELECT * FROM feed_state WHERE feed_url = ?", (url,))
    assert (st["last_status"], st["consecutive_failures"]) == (200, 1) and await feed_ok_at(db, url) is None
    route.mock(return_value=xml("wordpress.xml"))
    rep = await p.poll_feeds(feeds, job="rss_feeds_poll", run_id="r2", now=NOW + 60)
    assert rep["new"] == 2
    rep = await p.poll_feeds(feeds, job="rss_feeds_poll", run_id="r3", now=NOW + 120)      # no validators → 200 again
    assert rep["ok"] == 1 and rep["new"] == 0
    st = await db.fetchone("SELECT * FROM feed_state WHERE feed_url = ?", (url,))
    assert (st["consecutive_failures"], st["items_seen"]) == (0, 2)
    assert await db.fetchval("SELECT COUNT(*) FROM news_items") == 2


@pytest.mark.asyncio
@respx.mock
async def test_publisher_keyword_filter_and_attribution(db, http, fset):
    o64 = await seed_occupant(db, 64, "Chutes")
    await seed_watch(db, None, "rss", CT, url=CT, source="seed_feeds", filtered=1)
    respx.get(CT).mock(return_value=xml("cointelegraph.xml"))
    p = await poller(db, http)
    rep = await p.poll_feeds(await feed_rows(db, ("rss",)), job="rss_feeds_poll", run_id="r1", now=NOW)
    assert rep["new"] == 1                                    # 3 general crypto items dropped by the filter
    n = await db.fetchone("SELECT * FROM news_items")
    assert n["canonical_url"] == "https://cointelegraph.com/news/bittensor-chutes-synthetic"   # utm_* stripped
    assert "utm_source" in n["url"] and n["publisher_domain"] == "cointelegraph.com" and n["source"] == "rss"
    assert json.loads(n["occupant_keys_json"]) == [{"occupant_key": o64, "kind": "rule"}]
    await db.execute("UPDATE watch_channels SET filtered = 0")
    await db.execute("DELETE FROM feed_state")
    rep = await p.poll_feeds(await feed_rows(db, ("rss",)), job="rss_feeds_poll", run_id="r2", now=NOW + 60)
    assert rep["new"] == 3                                    # unfiltered: every item kept


@pytest.mark.asyncio
@respx.mock
async def test_permanent_redirect_and_feed_dead(db, http, fset):
    fset(RSS_DEAD_AFTER_FAILURES=2)
    old, new = "https://old.example/feed", "https://new.example/feed"
    await seed_watch(db, None, "rss", old, url=old, source="seed_feeds")
    r_old = respx.get(old).mock(return_value=httpx.Response(301, headers={"location": new}))
    r_new = respx.get(new).mock(return_value=xml("substack.xml"))
    p = await poller(db, http)
    feeds = await feed_rows(db, ("rss",))
    rep = await p.poll_feeds(feeds, job="rss_feeds_poll", run_id="r1", now=NOW)
    assert rep["new"] == 3 and r_old.call_count == 1
    assert await db.fetchval("SELECT effective_url FROM feed_state WHERE feed_url = ?", (old,)) == new
    assert await db.fetchval("SELECT DISTINCT feed_url FROM news_items") == old
    r_new.mock(return_value=httpx.Response(404))
    for i in range(3):
        await p.poll_feeds(feeds, job="rss_feeds_poll", run_id=f"f{i}", now=NOW + (i + 1) * 3600)
    assert r_old.call_count == 1 and r_new.call_count == 3                 # effective URL used directly
    ev = await db.fetchall("SELECT * FROM events WHERE event_type = 'feed_dead'")
    assert len(ev) == 1 and ev[0]["occupant_key"] is None and ev[0]["source"] == "rss"
    p_ = json.loads(ev[0]["payload"])
    assert (p_["feed_url"], p_["kind"], p_["consecutive_failures"], p_["last_status"]) == (old, "rss", 2, 404)
    rep = await p.poll_feeds(feeds, job="rss_feeds_poll", run_id="f9", now=NOW + 5 * 3600)
    assert rep["skipped"] == 1 and r_new.call_count == 3                   # dead → retried daily
    await p.poll_feeds(feeds, job="rss_feeds_poll", run_id="f10", now=NOW + 3 * 3600 + DAY + 1)
    assert r_new.call_count == 4


@pytest.mark.asyncio
@respx.mock
async def test_job_and_whole_run_failure(rt):
    import app.config as app_config
    from app.jobs import SPEC_BY_ID
    s = app_config.settings
    s.RSS_ENABLED = True
    s.HTTP_MAX_RETRIES = 0
    s.RSS_EXTRA_FEEDS = SUB
    respx.get(SUB).mock(return_value=xml("substack.xml"))
    job = RssFeedsPollJob(rt, SPEC_BY_ID["rss_feeds_poll"])
    await job.run(trigger="manual")
    row = await rt.db.fetchone("SELECT ok, rows, note FROM collector_runs WHERE job = 'rss_feeds_poll'")
    assert row["ok"] == 1 and row["rows"] == 3 and row["note"].startswith("feeds=1 ok=1")
    respx.get(SUB).mock(return_value=httpx.Response(503))
    p = FeedPoller(rt.db, rt.http)
    rep = await p.poll_feeds([FeedRow(url=SUB, kind="rss")], job="rss_feeds_poll", run_id="x", now=NOW)
    assert rep["failed"] == 1 and rep["partial"] is None
    from app.collectors.rss import raise_if_all_failed
    with pytest.raises(ProviderError):
        raise_if_all_failed(rep, "rss")
