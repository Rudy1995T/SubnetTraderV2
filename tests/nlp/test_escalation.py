"""Spec 10 §9 test_escalation: candidate selection, daily cap, governor denial, timeout → review, Sonnet outcomes."""
from __future__ import annotations

import json

import pytest

import app.config as app_config
from app.nlp.llm_classify import LlmClassifier
from tests.nlp.fakes import BULK_V, ESC_V, FIX, HOUR, T0, FakeAnthropic, FakeError, FakeGovernor, FakeLimiter, \
    message, nlp_settings, seed_label, seed_occupant, seed_x

pytestmark = pytest.mark.asyncio
NOW = T0 + 2 * HOUR


def _sonnet(fixture: str):
    body = (FIX / fixture).read_text()
    return lambda kw: message(body, model="claude-sonnet-5-5")


async def _seed4(db):
    o = await seed_occupant(db, 24)
    spec = [("1", "governance_dispute", -1, 3), ("2", "security_incident", -1, 2), ("3", "funding", 1, 3),
            ("4", "delay_missed_milestone", -1, 2)]
    for n, et, d, m in spec:
        pid = await seed_x(db, n, f"SN24 item {n}", event_at=T0)
        await seed_label(db, pid, occ=o, event_at=T0, observed_at=NOW - HOUR, event_type=et, direction=d,
                         materiality=m, polarity="bearish" if d < 0 else "bullish")
    return o


async def test_exactly_the_first_two_escalate(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    await _seed4(db)
    fake = FakeAnthropic(direct_fn=_sonnet("escalation_mat2.json"))
    gov = FakeGovernor()
    out = await LlmClassifier(db, fake, gov, FakeLimiter(), clock=lambda: NOW).escalate("r")
    assert out["candidates"] == 2 and out["escalated"] == 2
    assert [c[0] for c in fake.calls] == ["beta.messages.create"] * 2                # LLM_REFUSAL_FALLBACK default
    kw = fake.direct[0]
    assert kw["max_tokens"] == 4096 and kw["output_config"]["effort"] == "medium"
    assert "pre-flagged as potentially material" in kw["messages"][0]["content"]
    assert "governance_dispute" not in kw["messages"][0]["content"]                 # Haiku label not shown
    rows = await db.fetchall("SELECT post_id, classifier_tier, model_resolved FROM post_labels WHERE "
                             "classifier_version = ? ORDER BY post_id", (ESC_V,))
    assert [(r["post_id"], r["classifier_tier"], r["model_resolved"]) for r in rows] == [
        ("x:1", "escalation", "claude-sonnet-5-5"), ("x:2", "escalation", "claude-sonnet-5-5")]
    assert [x[0] for x in gov.log] == ["check", "record", "check", "record"]
    assert all(x[3] == 1 for x in gov.of("record"))


async def test_plain_create_without_fallback_flag(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings(LLM_REFUSAL_FALLBACK=False))
    await _seed4(db)
    fake = FakeAnthropic(direct_fn=_sonnet("escalation_mat2.json"))
    await LlmClassifier(db, fake, FakeGovernor(), FakeLimiter(), clock=lambda: NOW).escalate("r")
    assert {c[0] for c in fake.calls} == {"messages.create"}


async def test_daily_cap_and_denial_and_timeout(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings(LLM_ESCALATION_MAX_PER_DAY=1))
    await _seed4(db)
    fake = FakeAnthropic(direct_fn=_sonnet("escalation_mat2.json"))
    out = await LlmClassifier(db, fake, FakeGovernor(), FakeLimiter(), clock=lambda: NOW).escalate("r")
    assert out["escalated"] == 1 and out["reviews"] == 1                          # overflow → review only
    rq = await db.fetchone("SELECT post_id, payload_json FROM review_queue")
    assert rq["post_id"] == "x:2" and json.loads(rq["payload_json"])["reason"] == "daily_cap"


async def test_governor_denial_keeps_label_pending(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    await _seed4(db)
    fake = FakeAnthropic(direct_fn=_sonnet("escalation_mat2.json"))
    clf = LlmClassifier(db, fake, FakeGovernor(allow=False), FakeLimiter(), clock=lambda: NOW)
    out = await clf.escalate("r")
    assert out["denied"] == 1 and fake.calls == [] and out["reviews"] == 0
    assert await db.fetchval("SELECT COUNT(*) FROM job_state WHERE key LIKE 'esc:%'") == 0     # no try consumed
    clf.governor = FakeGovernor()
    assert (await clf.escalate("r2"))["escalated"] == 2


async def test_failures_then_review_and_timeout(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    await _seed4(db)
    fake = FakeAnthropic(direct_fn=lambda kw: FakeError(500))
    lim = FakeLimiter()
    clf = LlmClassifier(db, fake, FakeGovernor(), lim, clock=lambda: NOW)
    for _ in range(3):
        assert (await clf.escalate("r"))["failed"] == 2
    out = await clf.escalate("r")
    assert out["reviews"] == 2 and out["failed"] == 0
    flags = {r["post_id"]: r["suggested_flag"] for r in await db.fetchall("SELECT * FROM review_queue")}
    assert flags == {"x:1": None, "x:2": None}         # unescalated candidates → review (§2.2), flag per §3.2
    assert lim.acquired == ["anthropic"] * 6
    # pending > LLM_FALLBACK_AFTER_H → review directly, no call
    o = await seed_occupant(db, 15)
    pid = await seed_x(db, "7", "SN15 coldkey compromised", event_at=T0)
    await seed_label(db, pid, occ=o, event_at=T0, observed_at=NOW - 19 * HOUR, event_type="security_incident",
                     direction=-1, materiality=3)
    n_calls = len(fake.calls)
    out = await clf.escalate("r")
    assert out["reviews"] == 1 and len(fake.calls) == n_calls
    r = await db.fetchone("SELECT suggested_flag, payload_json FROM review_queue WHERE post_id = ?", (pid,))
    assert r["suggested_flag"] == "key_compromise" and json.loads(r["payload_json"])["reason"] == "escalation_timeout"


async def test_sonnet_mat2_no_critical_and_mat3_critical(db, monkeypatch):
    from app.nlp.events import materialise_events
    from app.scoring.spec import LABEL_POLICIES
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    o = await seed_occupant(db, 24)
    pid = await seed_x(db, "1", "Audit: SN24 inherited weights; owner banned", event_at=T0)
    await seed_label(db, pid, occ=o, event_at=T0, observed_at=NOW - HOUR, event_type="governance_dispute",
                     direction=-1, materiality=3)
    clf = LlmClassifier(db, FakeAnthropic(direct_fn=_sonnet("escalation_mat2.json")), FakeGovernor(), FakeLimiter(),
                        clock=lambda: NOW)
    await clf.escalate("r")
    await materialise_events(db, LABEL_POLICIES["ts-1.0"], "r", now=NOW)
    ev = await db.fetchall("SELECT severity, payload FROM events")
    assert [e["severity"] for e in ev] == ["warning"]                              # resolved label is mat 2
    assert "confirmed_by" not in json.loads(ev[0]["payload"])
    o2 = await seed_occupant(db, 25)
    pid2 = await seed_x(db, "2", "SN25 owner banned, mining paused", event_at=T0)
    await seed_label(db, pid2, occ=o2, event_at=T0, observed_at=NOW - HOUR, event_type="governance_dispute",
                     direction=-1, materiality=3)
    clf.client = FakeAnthropic(direct_fn=lambda kw: message(
        (FIX / "escalation_mat3.json").read_text().replace('"netuid": 24', '"netuid": 25'), model="claude-sonnet-5-5"))
    await clf.escalate("r2")
    await materialise_events(db, LABEL_POLICIES["ts-1.0"], "r2", now=NOW + 60)
    crit = await db.fetchone("SELECT severity, payload FROM events WHERE occupant_key = ?", (o2,))
    assert crit["severity"] == "critical" and json.loads(crit["payload"])["confirmed_by"] == "claude-sonnet-5-5"
    assert await db.fetchval("SELECT COUNT(*) FROM review_queue") == 0


async def test_refusal_goes_to_review_unconfirmed(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    o = await seed_occupant(db, 71)
    pid = await seed_x(db, "1", "SN71 owner dumped alpha", event_at=T0)
    await seed_label(db, pid, occ=o, event_at=T0, observed_at=NOW - HOUR, event_type="owner_extraction_evidence",
                     direction=-1, materiality=3)
    fake = FakeAnthropic(direct_fn=lambda kw: message("", stop_reason="refusal", category="cyber"))
    out = await LlmClassifier(db, fake, FakeGovernor(), FakeLimiter(), clock=lambda: NOW).escalate("r")
    assert out["reviews"] == 1 and out["escalated"] == 0
    r = await db.fetchone("SELECT suggested_flag, label_version FROM review_queue")
    assert (r["suggested_flag"], r["label_version"]) == ("owner_extraction", BULK_V)
