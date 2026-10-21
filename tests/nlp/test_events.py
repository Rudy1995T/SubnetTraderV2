"""Spec 10 §9 test_events: clustering, dedupe key, severities, review rows, confirm_review, alerted rows frozen."""
from __future__ import annotations

import json

import pytest

import app.config as app_config
from app.integrity import review
from app.nlp.events import BUCKET_S, confirm_review, enqueue_review, materialise_events
from app.scoring.spec import LABEL_POLICIES
from tests.nlp.fakes import BULK_V, HOUR, T0, nlp_settings, seed_label, seed_occupant, seed_x

pytestmark = pytest.mark.asyncio
POL = LABEL_POLICIES["ts-1.0"]
NOW = T0 + 6 * HOUR
B0 = (T0 // BUCKET_S) * BUCKET_S                    # start of T0's 48-h bucket


@pytest.fixture
def s(monkeypatch):
    st = nlp_settings()
    monkeypatch.setattr(app_config, "settings", st)
    return st


async def _events(db):
    rows = await db.fetchall("SELECT * FROM events ORDER BY id")
    for r in rows:
        r["payload"] = json.loads(r["payload"])
    return rows


async def test_cluster_one_event(db, s):
    o = await seed_occupant(db, 64)
    for i in range(6):                                     # 6 labels within 30 h of one bucket
        pid = await seed_x(db, str(i), f"launch {i}", event_at=B0 + 3600 + i * 6 * HOUR)
        await seed_label(db, pid, occ=o, event_at=B0 + 3600 + i * 6 * HOUR, observed_at=NOW - 60,
                         event_type="product_launch", direction=1, materiality=2 + (i == 3), polarity="bullish")
    res = await materialise_events(db, POL, "r1", now=NOW)
    ev = await _events(db)
    assert res["events_new"] == 1 and len(ev) == 1
    e = ev[0]
    assert e["dedupe_key"] == f"{o}|product_launch|{B0 // BUCKET_S}|pos" and e["severity"] == "info"
    assert e["source"] == "llm" and e["event_at"] == B0 + 3600 and e["payload"]["n_supporting"] == 6
    assert e["payload"]["materiality"] == 3 and e["payload"]["post_ids"][0] == "x:0"
    assert e["payload"]["url"] == "https://x.com/i/status/3"
    assert (await materialise_events(db, POL, "r2", now=NOW + 60))["events_new"] == 0      # idempotent


async def test_severity_directions_and_mat1(db, s):
    o = await seed_occupant(db, 64)
    await seed_label(db, "x:1", occ=o, event_at=T0, observed_at=NOW - 60, event_type="delay_missed_milestone",
                     direction=-1, materiality=2)
    await seed_label(db, "x:2", occ=o, event_at=T0, observed_at=NOW - 60, event_type="team_change",
                     direction=-1, materiality=2)
    await seed_label(db, "x:3", occ=o, event_at=T0, observed_at=NOW - 60, event_type="team_change",
                     direction=1, materiality=2)
    await seed_label(db, "x:4", occ=o, event_at=T0, observed_at=NOW - 60, event_type="release_upgrade",
                     direction=1, materiality=1)
    await seed_label(db, "x:5", occ=o, event_at=T0, observed_at=NOW - 60, event_type="funding",
                     direction=1, materiality=2, is_spam=1)
    await materialise_events(db, POL, "r", now=NOW)
    ev = {(e["event_type"], e["dedupe_key"].rsplit("|", 1)[1]): e["severity"] for e in await _events(db)}
    assert ev == {("delay_missed_milestone", "neg"): "warning", ("team_change", "neg"): "warning",
                  ("team_change", "pos"): "info"}


async def test_mat3_negative_pending_then_confirm(db, s, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings(LLM_ESCALATION_ENABLED=False))
    o = await seed_occupant(db, 24)
    pid = await seed_x(db, "9", "Audit: SN24 inherited weights, owner banned", event_at=T0)
    await seed_label(db, pid, occ=o, event_at=T0, observed_at=NOW - 60, event_type="governance_dispute",
                     direction=-1, materiality=3, polarity="bearish")
    res = await materialise_events(db, POL, "r", now=NOW)
    assert res["reviews"] == 1 and await _events(db) == []
    rq = await db.fetchone("SELECT * FROM review_queue")
    assert rq["kind"] == "llm_flag_candidate" and rq["suggested_flag"] is None and rq["label_version"] == BULK_V
    assert rq["dedupe_key"] == f"llm_flag_candidate|{pid}|{BULK_V}" and rq["evidence_url"].endswith("/9")
    # second enqueue for the same (post_id, label_version) is a no-op
    assert await enqueue_review(db, occupant_key=o, post_id=pid, label_version=BULK_V, event_type="x",
                                materiality=3, summary=None, evidence_url=None, suggested_flag=None, reason="x",
                                priority=2) is None
    eid = await confirm_review(db, rq["id"], by="rudolf", now=NOW + 60)
    ev = await _events(db)
    assert len(ev) == 1 and ev[0]["id"] == eid and ev[0]["severity"] == "critical"
    assert ev[0]["payload"]["confirmed_by"] == "rudolf" and ev[0]["payload"]["confirmed_post_ids"] == [pid]
    assert await db.fetchval("SELECT COUNT(*) FROM manual_flags") == 0
    # materialise afterwards keeps the confirmation (counted now) and does not duplicate
    await materialise_events(db, POL, "r2", now=NOW + 120)
    ev = await _events(db)
    assert len(ev) == 1 and ev[0]["payload"]["confirmed_by"] == "rudolf"


async def test_owner_extraction_review_and_resolve_review_hand_off(db, s):
    o = await seed_occupant(db, 71)
    pid = await seed_x(db, "5", "SN71 owner dumped 40k alpha", event_at=T0)
    await seed_label(db, pid, occ=o, event_at=T0, observed_at=NOW - 20 * HOUR, event_type="owner_extraction_evidence",
                     direction=-1, materiality=3, polarity="bearish")
    await materialise_events(db, POL, "r", now=NOW)                          # escalation timed out → review now
    rq = await db.fetchone("SELECT * FROM review_queue")
    assert rq["suggested_flag"] == "owner_extraction" and rq["priority"] == 1
    r = await review.resolve_review(db, rq["id"], "confirm_flag", by="op", note="seen", evidence_url="https://x.com/5")
    assert r.ok and r.warning is None
    assert await db.fetchval("SELECT flag || ':' || state FROM manual_flags") == "owner_extraction:confirmed"
    e = (await _events(db))[0]
    assert e["event_type"] == "owner_extraction_evidence" and e["payload"]["confirmed_by"] == "op"


async def test_security_key_compromise_suggestion(db, s, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings(LLM_ESCALATION_ENABLED=False))
    o = await seed_occupant(db, 15)
    a = await seed_x(db, "1", "SN15 owner coldkey compromised, alpha dumped", event_at=T0)
    b = await seed_x(db, "2", "SN15 exploit in the validator code, funds lost", event_at=T0)
    for pid in (a, b):
        await seed_label(db, pid, occ=o, event_at=T0, observed_at=NOW - 60, event_type="security_incident",
                         direction=-1, materiality=3, summary="incident")
    await materialise_events(db, POL, "r", now=NOW)
    sug = {r["post_id"]: (r["suggested_flag"], r["priority"]) for r in await db.fetchall("SELECT * FROM review_queue")}
    assert sug == {a: ("key_compromise", 1), b: ("security_incident", 2)}


async def test_alerted_event_payload_frozen(db, s):
    o = await seed_occupant(db, 64)
    await seed_label(db, "x:1", occ=o, event_at=T0, observed_at=NOW - 60, event_type="funding", direction=1,
                     materiality=2)
    await materialise_events(db, POL, "r", now=NOW)
    e = (await _events(db))[0]
    await db.execute("UPDATE events SET alerted_at = ? WHERE id = ?", (NOW, e["id"]))
    await seed_label(db, "x:2", occ=o, event_at=T0 + 60, observed_at=NOW + 10, event_type="funding", direction=1,
                     materiality=3)
    await materialise_events(db, POL, "r2", now=NOW + 20)
    e2 = (await _events(db))[0]
    assert e2["payload"] == e["payload"] and len(await _events(db)) == 1
