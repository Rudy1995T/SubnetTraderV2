"""Spec 10 §9 test_queue: SQL privacy gate, bot filter, pending-first ordering, version exclusion, item ids, text
assembly, truncation, text_hash dedup with dedup_of."""
from __future__ import annotations

import sqlite3

import pytest

import app.config as app_config
from app.nlp import queue as Q
from tests.nlp.fakes import BULK_V, DAY, RULES_V, T0, nlp_settings, seed_ann, seed_label, seed_news, seed_occupant, \
    seed_video, seed_x

pytestmark = pytest.mark.asyncio
NOW = T0


async def _seed40(db):
    o = await seed_occupant(db, 64, name="Chutes")
    for i in range(14):                                                       # 14 X posts, 2 bot-filtered
        await seed_x(db, f"10{i:02d}", f"SN64 update {i} with real text", event_at=NOW - DAY + i * 60,
                     author_id=f"a{i}", bot_filtered=1 if i < 2 else 0, likes=i, occupant_key=o if i == 5 else None,
                     is_team=1 if i == 5 else 0)
    for i in range(10):                                                       # 10 announcements
        if i < 3:
            await seed_ann(db, f"telegram:chutes/{i}", f"Pending tg post {i}", event_at=NOW - 2 * DAY + i,
                           purge_after=NOW + (5 - i) * DAY, occupant_key=o)
        elif i < 6:
            await seed_ann(db, f"discord:{900 + i}", f"Own server crosspost {i}", event_at=NOW - DAY + i,
                           occupant_key=o)
        else:
            await seed_ann(db, f"discord:{900 + i}", None, event_at=NOW - DAY + i, via_own_server=0)
    for i in range(8):                                                        # 8 news (2 too old)
        await seed_news(db, f"{i:064x}", f"Headline {i}", f"Summary {i}",
                        event_at=NOW - (5 * DAY if i < 2 else DAY) + i, occupant_keys=[o])
    for i in range(8):
        await seed_video(db, f"vid{i}", f"Video {i}", f"Desc {i}", event_at=NOW - DAY + i,
                         occupant_key=o if i % 2 else None)
    return o


async def test_selection_privacy_order_ids(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    await _seed40(db)
    with pytest.raises(sqlite3.IntegrityError):                                 # 03 CHECK: no third-party text
        await seed_ann(db, "discord:777", "third party text", event_at=NOW, via_own_server=0)
    items = await Q.select_unlabelled(db, bulk_version=BULK_V, now=NOW, lookback_days=3, limit=1000)
    ids = [q.post_id for q in items]
    assert not any(i in ids for i in ("x:1000", "x:1001"))                     # bot_filtered
    assert not any(i.startswith("discord:90") and int(i[-1]) >= 6 for i in ids)  # no text → never selected
    assert ids[:3] == ["telegram:chutes/2", "telegram:chutes/1", "telegram:chutes/0"]   # earliest purge first
    assert ids[3] == "x:1005"                                                  # then team
    kinds = {i.split(":", 1)[0] for i in ids}
    assert kinds == {"x", "telegram", "discord", "news_item", "video_item"}
    assert len([i for i in ids if i.startswith("news_item:")]) == 6            # lookback 3 d
    assert len(ids) == 12 + 6 + 6 + 8
    news = next(q for q in items if q.item_kind == "news_item")
    assert news.text == f"Headline {int(news.item_id, 16)}\nSummary {int(news.item_id, 16)}"
    assert news.post_id == "news_item:" + news.item_id and len(news.item_id) == 64
    vid = next(q for q in items if q.post_id == "video_item:vid1")
    assert vid.author_kind == "team" and vid.source_netuid == 64


async def test_version_exclusion_and_rules_only_stays(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    o = await seed_occupant(db, 64)
    a = await seed_x(db, "1", "first", event_at=NOW - DAY)
    b = await seed_x(db, "2", "second", event_at=NOW - DAY)
    await seed_label(db, a, occ=o, event_at=NOW - DAY, observed_at=NOW - 3600)
    await seed_label(db, b, occ=o, event_at=NOW - DAY, observed_at=NOW - 3600, version=RULES_V, tier="rules")
    ids = [q.post_id for q in await Q.select_unlabelled(db, bulk_version=BULK_V, now=NOW, lookback_days=3,
                                                        limit=10)]
    assert ids == ["x:2"]


async def test_attempts_and_open_batches(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    for i in range(4):
        await seed_x(db, str(i), f"text {i}", event_at=NOW - DAY)
    await db.execute("INSERT INTO llm_batches (batch_key, model, classifier_version, status, created_at) VALUES "
                     "('b1','m',?, 'applied', ?), ('b2','m',?, 'submitted', ?), ('b3','m',?, 'applied', ?)",
                     (BULK_V, NOW, BULK_V, NOW, BULK_V, NOW))
    await db.execute("INSERT INTO llm_batch_items (batch_key, custom_id, post_id, status) VALUES "
                     "('b1','c','x:0','errored'), ('b3','c','x:0','errored'), ('b1','c','x:1','errored'), "
                     "('b2','c','x:2','pending'), ('b1','c','x:3','requeued')")
    items = {q.post_id: q for q in await Q.select_unlabelled(db, bulk_version=BULK_V, now=NOW, lookback_days=3,
                                                             limit=10)}
    assert set(items) == {"x:1", "x:3"}                       # x:0 twice errored → rules; x:2 in an open batch
    assert items["x:1"].per_request_cap == 10 and items["x:1"].attempt == 2
    assert items["x:3"].per_request_cap is None and items["x:3"].attempt == 2


async def test_truncation_and_dedup():
    long = "word " * 400
    t = Q.assemble_text({"text": long}, "social_post", 50)
    assert len(t) == 50 and t.endswith("…")
    assert Q.assemble_text({"title": "T", "summary": None}, "news_item", 100) == "T"
    assert Q.assemble_text({"text": "   "}, "announcement", 100) is None
    mk = lambda p, h: Q.QueueItem(p, "social_post", p, None, "x", "other", None, None, "t", h, 0, None)  # noqa: E731
    prim, dups = Q.dedup([mk("x:1", "h"), mk("x:2", "g"), mk("x:3", "h")])
    assert [q.post_id for q in prim] == ["x:1", "x:2"] and [q.post_id for q in dups["x:1"]] == ["x:3"]


async def test_dedup_copies_label_with_dedup_of(db, monkeypatch):
    from app.nlp.llm_classify import LlmClassifier
    from tests.nlp.fakes import FakeAnthropic, FakeGovernor, FakeLimiter
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    await seed_occupant(db, 64)
    await seed_x(db, "1", "Same text SN64", event_at=NOW - DAY, author_id="a")
    await seed_x(db, "2", "same   TEXT sn64", event_at=NOW - DAY, author_id="b")
    fake = FakeAnthropic()
    clf = LlmClassifier(db, fake, FakeGovernor(), FakeLimiter(), clock=lambda: NOW)
    res = await clf.submit_cycle("r1")
    assert res["n_requests"] == 1 and res["n_items"] == 2
    assert '<items n="1">' in fake.submitted[0]["params"]["messages"][0]["content"]
    fake.end_all()
    await clf.poll_cycle("r2")
    rows = await db.fetchall("SELECT post_id, raw_json FROM post_labels ORDER BY post_id")
    assert [r["post_id"] for r in rows] == ["x:1", "x:2"]
    assert '"dedup_of":"x:1"' in rows[1]["raw_json"] and "dedup_of" not in rows[0]["raw_json"]
