"""Spec 14 §9 SSE tests: hub fan-out with rowid-watermark ids, replay across a restart, caps, slow consumers.

The generator and the hub are driven directly (``httpx``'s ASGI transport buffers a whole response, so an endless
stream cannot be read through it); the HTTP-level checks are the ones that answer at once (503 cap, 404 disabled).
"""
from __future__ import annotations

import asyncio

import httpx
import pytest

from app.api import hub as hubmod
from app.api import stream as stream_api
from tests.api.conftest import D0, OCC47, OCC64, _score

pytestmark = pytest.mark.asyncio


async def _drain(sub, n: int = 50) -> list:
    out = []
    while not sub.queue.empty() and len(out) < n:
        out.append(sub.queue.get_nowait())
    return out


async def test_sse_hub_frames_and_replay(seeded, cfg):
    db = seeded.db
    hub = hubmod.get_hub(db)
    sub = await hub.subscribe(frozenset({"score", "event"}), None)
    await _score(db, OCC64, 64, D0 + 6 * 3600, comp=83.0, quality=85.0, velocity=70.0, conf=0.9, tier="A")
    hub.nudge()
    assert await hub.poll_once() == 1
    f = (await _drain(sub))[0]
    sid = await db.fetchval("SELECT MAX(id) FROM scores")
    eid = await db.fetchval("SELECT MAX(id) FROM events")
    assert f.event == "score" and f.id == f"{sid}-{eid}" and f.data["composite"] == 83.0
    assert f.encode().startswith(f"id: {sid}-{eid}\nevent: score\ndata: {{")
    last = f.id
    hub.unsubscribe(sub)
    # "restart": a new hub; rows written meanwhile are replayed from SQLite
    hubmod.reset()
    await _score(db, OCC47, 47, D0 + 6 * 3600, comp=17.0, quality=30.0, velocity=20.0, conf=0.5, tier="D")
    from app.storage.events import emit_event
    await emit_event(db, event_type="manual_entry", occupant_key=OCC64, netuid=64, event_at=D0, observed_at=D0,
                     payload={"kind": "note", "by": "api", "row_id": 1, "summary": "x", "evidence_url": None},
                     dedupe_key=None, source="api", run_id=None)
    hub2 = hubmod.get_hub(db)
    sub2 = await hub2.subscribe(frozenset({"score", "event"}), hubmod.parse_last_id(last))
    frames = await _drain(sub2)
    assert [x.event for x in frames] == ["score", "event"]
    assert frames[0].data["netuid"] == 47 and frames[1].data["event_type"] == "manual_entry"
    assert frames[1].id == f"{sid + 1}-{eid + 1}"
    hub2.unsubscribe(sub2)
    # idle hub (no subscriber while rows were written): a client without an id starts from now, no backlog
    await emit_event(db, event_type="manual_entry", occupant_key=OCC64, netuid=64, event_at=D0, observed_at=D0,
                     payload={"kind": "note", "by": "api", "row_id": 2, "summary": "y", "evidence_url": None},
                     dedupe_key=None, source="api", run_id=None)
    sub3 = await hub2.subscribe(frozenset({"score", "event"}), None)
    assert await hub2.poll_once() == 0 and await _drain(sub3) == []
    hub2.unsubscribe(sub3)


async def test_sse_replay_too_large_and_slow_consumer(seeded, cfg):
    cfg(STREAM_REPLAY_MAX=1)
    hub = hubmod.get_hub(seeded.db)
    sub = await hub.subscribe(frozenset({"score", "event"}), (0, 0))
    frames = await _drain(sub)
    assert [x.event for x in frames] == ["resync"] and frames[0].data["reason"] == "replay_too_large"
    slow = hubmod.Subscriber(frozenset({"event"}))
    for i in range(hubmod.QUEUE_MAX + 10):
        slow.offer(hubmod.Frame("event", {"i": i}, f"0-{i}"))
    got = await _drain(slow, 1000)
    assert len(got) == hubmod.QUEUE_MAX and sum(1 for g in got if g.event == "resync") == 1
    assert got[-1].data["i"] == hubmod.QUEUE_MAX + 9                     # newest kept, oldest dropped
    hub.unsubscribe(sub)


async def test_sse_generator_health_and_types(seeded, cfg):
    cfg(STREAM_HEALTH_EVERY_SEC=3600)
    hub = hubmod.get_hub(seeded.db)
    sub = await hub.subscribe(frozenset({"health", "event"}), None)
    sub.offer(hubmod.Frame("score", {"x": 1}, "1-1"))                 # not subscribed → filtered
    sub.offer(hubmod.Frame("event", {"x": 2}, "1-2"))
    out = [chunk async for chunk in stream_api.frames(None, hub, sub, max_seconds=0.3)]
    body = "".join(out)
    assert out[0] == ": connected\n\n"
    assert "event: health\n" in body and '"status"' in body and "event: score" not in body
    assert "id: 1-2\nevent: event\n" in body
    assert sub not in hub.subs                                         # deregistered in finally


async def test_sse_connection_cap(seeded, app_client, cfg):
    cfg(STREAM_MAX_CONNECTIONS=2)
    hub = hubmod.get_hub(seeded.db)
    a = await hub.subscribe(frozenset({"score"}), None)
    b = await hub.subscribe(frozenset({"score"}), None)
    r = await app_client.get("/api/v1/stream")
    assert r.status_code == 503 and r.json()["error"] == "too_many_stream_connections"
    assert (await app_client.get("/api/v1/stream?types=bogus")).status_code in (422, 503)
    hub.unsubscribe(a)
    hub.unsubscribe(b)
    await asyncio.sleep(0)


async def test_stream_disabled(rt, cfg):
    from app.api.app import create_app
    cfg(STREAM_ENABLED=False)
    app = create_app(lambda: rt)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        assert (await c.get("/api/v1/stream")).status_code == 404
