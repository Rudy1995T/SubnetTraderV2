"""Spec 09 §A.4 / §4 rows ``news_mentions_7d``, ``video_uploads_90d``, ``video_mentions_30d``, ``blog_posts_90d``:
absence vs failure vs ok, PIT reads, registration (registry leg row + catalogued evidence keys)."""
from __future__ import annotations

import json

import pytest

from app.features import social_feeds as F
from tests.collectors.feeds.conftest import DAY, NOW, fset, seed_feed_ok, seed_occupant, seed_watch  # noqa: F401

T = NOW
OWN = "UCownChannel64aaaaaaaaaa"
PID = "PLN0WJc6io8zqEknIutfyEjiJViazrxrzU"
PLAYLIST = f"https://www.youtube.com/feeds/videos.xml?playlist_id={PID}"
CHAN = f"https://www.youtube.com/feeds/videos.xml?channel_id={OWN}"


async def news(db, uh: str, title: str, occ: list[str], *, event_at: int, observed_at: int | None = None,
               domain: str = "example.com", feed: str = "https://example.com/feed", kind: str = "rule") -> None:
    await db.execute("INSERT INTO news_items (url_hash, url, title, publisher_domain, feed_url, occupant_keys_json, "
                     "text_hash, event_at, event_at_source, observed_at, source) VALUES (?,?,?,?,?,?,?,?,?,?,'rss')",
                     (uh, f"https://{domain}/{uh}", title, domain, feed,
                      json.dumps([{"occupant_key": o, "kind": kind} for o in occ]), "h", event_at, "feed",
                      observed_at if observed_at is not None else event_at))


async def video(db, vid: str, channel: str, *, occ: list[str] | None = None, event_at: int, observed_at: int | None = None,
                okey: str | None = None) -> None:
    await db.execute("INSERT INTO video_items (video_id, channel_id, occupant_key, occupant_keys_json, title, event_at, "
                     "observed_at, source) VALUES (?,?,?,?,?,?,?,?)",
                     (vid, channel, okey, json.dumps([{"occupant_key": o, "kind": "rule"} for o in occ or []]),
                      f"video {vid}", event_at, observed_at if observed_at is not None else event_at,
                      "channel_rss" if okey else "playlist_rss"))


def test_registration():
    from app.features import registry as R
    assert F.register_producers() == 4
    assert R.ATTACHED_FNS["news_mentions_7d"] is F.compute_news_mentions_7d
    for k in ("video_uploads_90d", "video_mentions_30d", "blog_posts_90d"):
        assert R.EVIDENCE_FNS[k] is F.EVIDENCE[k]
    assert R.METRICS["news_mentions_7d"].fn == "app.features.social_feeds:compute_news_mentions_7d"


@pytest.mark.asyncio
async def test_news_mentions_7d(db, fset):
    o64 = await seed_occupant(db, 64, "Chutes")
    o4 = await seed_occupant(db, 4, "Targon")
    assert (await F.compute_news_mentions_7d(db, o64, T)).value_json["reason"] == "no_feed_state"
    fset(RSS_ENABLED=False)
    r = await F.compute_news_mentions_7d(db, o64, T)
    assert r.status == "failed" and r.value_json["reason"] == "rss_disabled"
    fset()
    await seed_feed_ok(db, "https://a.example/feed", T - 3600)
    await seed_feed_ok(db, "https://b.example/feed", T - 3 * DAY)                       # stale > 24 h
    await seed_feed_ok(db, "https://c.example/feed", T - 3600, status=404)              # failing
    r = await F.compute_news_mentions_7d(db, o64, T)                                     # 2/3 failed, tail occupant
    assert r.status == "failed" and r.value_json["reason"] == "feeds_failing" and r.value_json["searched"] is False
    q = "https://news.google.com/rss/search?q=%22Chutes%22+Bittensor&hl=en-US&gl=US&ceid=US:en"
    await seed_feed_ok(db, q, T - 2 * 3600, kind="google_news", occupant_key=o64, job="news_google_poll")
    r = await F.compute_news_mentions_7d(db, o64, T)                                     # top-N, own query ok
    assert r.status == "ok" and r.value == 0.0 and r.value_json["searched"] is True
    assert (await F.compute_news_mentions_7d(db, o4, T)).status == "failed"
    await seed_feed_ok(db, "https://b.example/feed", T - 3600)
    await seed_feed_ok(db, "https://c.example/feed", T - 3600)
    await news(db, "1", "Chutes ships v2", [o64], event_at=T - DAY)
    await news(db, "2", "Chutes ships v2", [o64], event_at=T - DAY, kind="query")        # same story, same publisher
    await news(db, "3", "Chutes ships v2", [o64], event_at=T - DAY, domain="other.example")
    await news(db, "4", "Old story", [o64], event_at=T - 8 * DAY)                        # outside 7 d
    await news(db, "5", "Future knowledge", [o64], event_at=T - DAY, observed_at=T + 60)  # PIT: not yet observed
    await news(db, "6", "Targon only", [o4], event_at=T - DAY)
    r = await F.compute_news_mentions_7d(db, o64, T)
    assert r.status == "ok" and r.value == 2.0 and r.value_json["items"] == 3
    assert (await F.compute_news_mentions_7d(db, o4, T)).value == 1.0


@pytest.mark.asyncio
async def test_video_uploads_90d(db, fset):
    o64 = await seed_occupant(db, 64, "Chutes")
    r = await F.compute_video_uploads_90d(db, o64, T)
    assert r.status == "absent" and r.value == 0.0 and r.absence_reason == "no_youtube_channel"
    wid = await seed_watch(db, o64, "youtube_video", "vid64aaaaaa")
    r = await F.compute_video_uploads_90d(db, o64, T)
    assert r.status == "failed" and r.value_json["reason"] == "channel_unresolved"
    await db.execute("UPDATE watch_channels SET meta_json = json_set(meta_json, '$.channel_id', "
                     "'UCxLqly5YxULobLJwcYjMhJQ', '$.channel_role', 'ecosystem') WHERE id = ?", (wid,))
    r = await F.compute_video_uploads_90d(db, o64, T)                       # only the taostats channel → no own one
    assert r.status == "absent" and r.value_json["roles"] == {"ecosystem": 1}
    await db.execute("UPDATE watch_channels SET meta_json = json_set(meta_json, '$.channel_id', ?, '$.channel_role', "
                     "'own') WHERE id = ?", (OWN, wid))
    r = await F.compute_video_uploads_90d(db, o64, T)
    assert r.status == "failed" and r.value_json["reason"] == "rss_stale"
    await seed_feed_ok(db, CHAN, T - 8 * DAY, kind="youtube", job="youtube_poll")
    assert (await F.compute_video_uploads_90d(db, o64, T)).value_json["reason"] == "rss_stale"
    await seed_feed_ok(db, CHAN, T - DAY, kind="youtube", job="youtube_poll")
    r = await F.compute_video_uploads_90d(db, o64, T)
    assert r.status == "ok" and r.value == 0.0                                 # measured zero
    await video(db, "a" * 11, OWN, okey=o64, event_at=T - 10 * DAY)
    await video(db, "b" * 11, OWN, okey=o64, event_at=T - 100 * DAY)            # outside 90 d
    await video(db, "c" * 11, OWN, okey=o64, event_at=T - DAY, observed_at=T + 1)   # PIT
    await video(db, "d" * 11, "UCsomeoneElsexxxxxxxxxxx", event_at=T - DAY)
    r = await F.compute_video_uploads_90d(db, o64, T)
    assert r.value == 1.0 and r.value_json["channels"] == [OWN] and r.value_json["lower_bound"] is False
    assert (await F.compute_video_uploads_90d(db, o64, T - 200 * DAY)).status == "absent"   # row not yet declared


@pytest.mark.asyncio
async def test_video_mentions_30d(db, fset):
    o64 = await seed_occupant(db, 64, "Chutes")
    r = await F.compute_video_mentions_30d(db, o64, T)
    assert r.status == "failed" and r.value_json["reason"] == "playlist_feed_stale"
    await seed_feed_ok(db, PLAYLIST, T - DAY, kind="youtube", job="youtube_poll")
    assert (await F.compute_video_mentions_30d(db, o64, T)).value == 0.0
    wid = await seed_watch(db, o64, "youtube_video", "vid64aaaaaa")
    await db.execute("UPDATE watch_channels SET meta_json = json_set(meta_json, '$.channel_id', ?, '$.channel_role', "
                     "'own') WHERE id = ?", (OWN, wid))
    await video(db, "p" * 11, "UCotherChannelxxxxxxxxxx", occ=[o64], event_at=T - 5 * DAY)
    await video(db, "q" * 11, OWN, occ=[o64], event_at=T - 5 * DAY)             # own channel → uploads, not mentions
    await video(db, "r" * 11, "UCotherChannelxxxxxxxxxx", occ=[o64], event_at=T - 40 * DAY)
    r = await F.compute_video_mentions_30d(db, o64, T)
    assert r.status == "ok" and r.value == 1.0
    fset(YOUTUBE_ENABLED=False)
    assert (await F.compute_video_mentions_30d(db, o64, T)).value_json["reason"] == "youtube_disabled"


@pytest.mark.asyncio
async def test_blog_posts_90d(db, fset):
    o64 = await seed_occupant(db, 64, "Chutes")
    r = await F.compute_blog_posts_90d(db, o64, T)
    assert r.status == "absent" and r.absence_reason == "no_blog_feed"
    feed = "https://chutes.substack.com/feed"
    await seed_watch(db, o64, "substack", feed, url=feed, source="manual")
    r = await F.compute_blog_posts_90d(db, o64, T)
    assert r.status == "failed" and r.value_json["reason"] == "feed_stale"
    await seed_feed_ok(db, feed, T - 2 * DAY, kind="substack")
    assert (await F.compute_blog_posts_90d(db, o64, T)).value == 0.0
    await news(db, "b1", "Post 1", [], event_at=T - 10 * DAY, feed=feed)
    await news(db, "b2", "Post 2", [], event_at=T - 95 * DAY, feed=feed)
    await news(db, "b3", "Post 3", [], event_at=T - DAY, observed_at=T + 5, feed=feed)
    await news(db, "b4", "Other blog", [o64], event_at=T - DAY)
    r = await F.compute_blog_posts_90d(db, o64, T)
    assert r.status == "ok" and r.value == 1.0 and r.value_json == {"feeds": 1, "stale_feeds": 0}
