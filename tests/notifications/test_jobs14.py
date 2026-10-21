"""Spec 14 §9 job tests: ``dereg_watch``, ``needs_review_digest`` (watermark + buckets), ``telegram_bot`` idling."""
from __future__ import annotations

import asyncio
import json

import pytest

from app.jobs import SPEC_BY_ID
from app.util.time import floor_day, now_ts
from tests.api.conftest import OCC47, OCC64

pytestmark = pytest.mark.asyncio


async def _run(rt, cls, jid: str) -> dict:
    job = cls(rt, SPEC_BY_ID[jid])
    await job.run(trigger="manual")
    return await rt.db.fetchone("SELECT ok, note, error FROM collector_runs WHERE job = ? ORDER BY rowid DESC "
                                "LIMIT 1", (jid,), writer=True)


async def test_dereg_watch(seeded, cfg):
    from app.notifications.watch import DeregWatchJob, moving_price_ranks, reason_for
    db = seeded.db
    cfg(DEREG_WATCH_ENABLED=True, WATCHLIST_NETUIDS=[47, 64], NETWORK_IMMUNITY_PERIOD_BLOCKS=432000)
    r = await _run(seeded.rt, DeregWatchJob, "dereg_watch")
    assert r["ok"] == 1, r
    rows = await db.fetchall("SELECT occupant_key, payload FROM events WHERE event_type = 'dereg_watch'", writer=True)
    assert [x["occupant_key"] for x in rows] == [OCC47]
    p = json.loads(rows[0]["payload"])
    assert p["reason"] == "bottom3" and p["moving_price_rank"] == 1 and p["immune"] is False
    await _run(seeded.rt, DeregWatchJob, "dereg_watch")                               # same day → deduped
    assert await db.fetchval("SELECT COUNT(*) FROM events WHERE event_type = 'dereg_watch'", writer=True) == 1
    await db.execute("UPDATE chain_ticks SET observed_at = ? WHERE netuid = 47", (now_ts() - 3600,))
    cfg(WATCHLIST_NETUIDS=[47])
    r = await _run(seeded.rt, DeregWatchJob, "dereg_watch")
    assert "47:stale_tick" in r["note"]
    # pure rules
    assert reason_for(immune=True, blocks_left=5 * 7200, risk=1, safety=0.0) == "immunity_ending"
    assert reason_for(immune=True, blocks_left=30 * 7200, risk=1, safety=0.0) is None
    assert reason_for(immune=False, blocks_left=-1, risk=0, safety=0.05) == "safety_metric"
    ticks = {1: {"fresh": True, "subnet_moving_price": 0.3}, 2: {"fresh": True, "subnet_moving_price": 0.1},
             3: {"fresh": False, "subnet_moving_price": 0.01}, 4: {"fresh": True, "subnet_moving_price": 0.05}}
    assert moving_price_ranks(ticks, {4: True}) == {2: 1, 1: 2}


async def test_digest(seeded, cfg, monkeypatch):
    from app.notifications import telegram
    from app.notifications.digest import NeedsReviewDigestJob, digest_watermark
    from app.storage.events import emit_event
    db = seeded.db
    cfg(NEEDS_REVIEW_DIGEST_ENABLED=True, TELEGRAM_BOT_TOKEN="1:x", TELEGRAM_CHAT_ID="42")
    sent: list[str] = []

    async def fake_send(text, **kw):
        sent.append(text)
        return True, 200
    monkeypatch.setattr(telegram, "send_message", fake_send)
    now = now_ts()
    routed = []
    for i in range(5):
        eid = await emit_event(db, event_type="source_disagreement", occupant_key=OCC64, netuid=64, event_at=now,
                               observed_at=now, payload={"netuid": 64, "occupant_key": OCC64, "field": f"f{i}",
                                                         "groups": [], "resolved_source": "chain", "pair_hash": f"h{i}",
                                                         "alert_route": "digest"},
                               dedupe_key=f"d{i}", source="registry", run_id=None, alerted_at=now)
        routed.append(eid)
    pending = await emit_event(db, event_type="source_disagreement", occupant_key=OCC47, netuid=47, event_at=now,
                               observed_at=now, payload={"netuid": 47, "occupant_key": OCC47, "field": "x",
                                                         "groups": [], "resolved_source": "chain", "pair_hash": "p"},
                               dedupe_key="pending", source="registry", run_id=None)          # not routed yet
    # blocked site: 7 bot_challenge days for SN47's primary website
    day = floor_day(now)
    for k in range(7):
        await db.execute("INSERT INTO web_checks (occupant_key, netuid, url, kind, is_primary, source, event_at, "
                         "observed_at, check_day_ts, run_id, status_class) VALUES (?,?,?,?,1,'chain',?,?,?,'r',"
                         "'bot_challenge')", (OCC47, 47, "https://foo.io", "website", day - k * 86400,
                                              day - k * 86400, day - k * 86400))
    r = await _run(seeded.rt, NeedsReviewDigestJob, "needs_review_digest")
    assert r["ok"] == 1, r
    text = "\n".join(sent)
    assert "Needs review" in text and "source_disagreement</b> ×5" in text and "+2 more" in text
    assert "product status unset" in text and "SN47 bot_challenge" in text
    assert await digest_watermark(db) == pending - 1                     # stops below the unrouted row
    sent.clear()
    from app.notifications.commands import CommandDispatcher

    class Bot:
        async def emit(self, chat, text, kb, mid):
            sent.append(text)
    await CommandDispatcher(lambda: seeded.rt).c_digest(Bot(), "42", [])
    assert "none since the last digest" in sent[0]
    assert await digest_watermark(db) == pending - 1                     # /digest never moves it
    # 6 days only → not listed
    from app.api.queries import blocked_sites
    assert await blocked_sites(db, now=now + 86400, days=7) == []


async def test_bot_task_idles_without_token(rt, cfg):
    from app.notifications.bot import TelegramBotTask
    cfg(TELEGRAM_BOT_TOKEN="", TELEGRAM_CHAT_ID="")
    task = TelegramBotTask(rt, SPEC_BY_ID["telegram_bot"])
    stop = asyncio.Event()
    t = asyncio.create_task(task.run_once(stop))
    await asyncio.sleep(0.05)
    assert not t.done() and task.connected is False and task.last_attempt is not None
    stop.set()
    await asyncio.wait_for(t, 2)
