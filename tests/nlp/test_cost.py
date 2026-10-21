"""Spec 10 §9 test_cost: estimate_batch, reserve → None (budget, no API call, fallback labels backlog), create failure →
release, commit with usage-derived USD then a units record, UnknownReservation → record, Sonnet check/record, every
SDK call behind limiter.acquire, unpinned version skipped — against a keyword-only governor double."""
from __future__ import annotations

import pytest

import app.config as app_config
from app.costs import estimate_batch, estimate_llm_usd
from app.nlp.llm_classify import LlmClassifier
from app.nlp.rules import rules_fallback_cycle
from tests.nlp.fakes import HOUR, T0, FakeAnthropic, FakeError, FakeGovernor, FakeLimiter, item_obj, n_items, \
    nlp_settings, ok_result, seed_occupant, seed_x, usage

pytestmark = pytest.mark.asyncio


async def test_fake_governor_is_keyword_only():
    g = FakeGovernor()
    with pytest.raises(TypeError):
        await g.record("anthropic", 0.0, 1)                                    # positional after the amount
    with pytest.raises(TypeError):
        await g.reserve("anthropic", 1.0, "job", "run")


async def test_estimate_batch_20_items(monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    texts = ["x" * 700] * 20
    usd = estimate_batch(texts, model="claude-haiku-5-5", system_tokens=4300, n_req=1)
    exp_in = 4300 + 20 * (int(700 * 0.29) + 40)
    exp_out = 20 * 95 + 30
    want = (exp_in * 0.10 + exp_out * 0.50) / 1e6 * 0.5
    assert usd == pytest.approx(want, rel=0.10)
    assert estimate_llm_usd("claude-haiku-4-5-20251001", 1_000_000, 0) == pytest.approx(1.0)   # dated id → alias


async def _seed(db, n=3):
    await seed_occupant(db, 64)
    for i in range(n):
        await seed_x(db, str(i), f"SN64 post {i}", event_at=T0 - HOUR, observed_at=T0 - HOUR)


async def test_budget_skip_no_call_and_fallback_labels(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    await _seed(db)
    fake = FakeAnthropic()
    gov = FakeGovernor(reserve_ok=False)
    res = await LlmClassifier(db, fake, gov, FakeLimiter(), clock=lambda: T0).submit_cycle("r")
    assert res["skipped"] == "budget" and fake.calls == [] and gov.of("reserve")
    out = await rules_fallback_cycle(db, "rf", governor=gov, now=T0)
    assert out["mode"] == "budget" and out["labelled"] == 3


async def test_create_failure_releases(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    await _seed(db)
    fake = FakeAnthropic()
    fake.create_error = FakeError(400, "invalid_request")
    gov = FakeGovernor()
    with pytest.raises(FakeError):
        await LlmClassifier(db, fake, gov, FakeLimiter(), clock=lambda: T0).submit_cycle("r")
    assert gov.of("release") == [("release", "resv-1")]
    b = await db.fetchone("SELECT status, error FROM llm_batches")
    assert b["status"] == "failed" and b["error"].startswith("invalid_request")
    assert {r["status"] for r in await db.fetchall("SELECT status FROM llm_batch_items")} == {"errored"}
    assert await db.fetchval("SELECT COUNT(*) FROM job_state WHERE key LIKE 'resv:%'") == 0
    fake.create_error = FakeError(429)
    lim = FakeLimiter()
    with pytest.raises(FakeError):
        await LlmClassifier(db, fake, gov, lim, clock=lambda: T0 + 1000).submit_cycle("r2")
    assert lim.penalized == [("anthropic", 7.0)]
    fake.create_error = FakeError(401)
    with pytest.raises(RuntimeError, match="auth_failed"):
        await LlmClassifier(db, fake, gov, FakeLimiter(), clock=lambda: T0 + 2000).submit_cycle("r3")


@pytest.mark.parametrize("unknown", [False, True])
async def test_commit_then_units_record(db, monkeypatch, unknown):
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    await _seed(db)
    u = usage(10_000, 2_000, cr=4_000, cw=0)
    fake = FakeAnthropic(results_fn=lambda req: ok_result(
        req["custom_id"], [item_obj(i + 1) for i in range(n_items(req["params"]))], u=u))
    gov = FakeGovernor(commit_unknown=unknown)
    lim = FakeLimiter()
    clf = LlmClassifier(db, fake, gov, lim, clock=lambda: T0)
    await clf.submit_cycle("r1")
    fake.end_all()
    await clf.poll_cycle("r2")
    want = (10_000 * 0.10 + 2_000 * 0.50 + 4_000 * 0.10 * 0.1) / 1e6 * 0.5
    c = gov.of("commit")[0]
    assert c[1] == "resv-1" and c[2] == pytest.approx(want) and c[3]["batch_key"]
    recs = [x for x in gov.of("record") if x[4]]
    assert recs[-1][2] == 0.0 and recs[-1][3] == 0 and recs[-1][4] == {"in": 10_000, "out": 2_000,
                                                                       "cache_read": 4_000, "cache_write": 0}
    if unknown:
        assert [x for x in gov.of("record") if x[2] == pytest.approx(want)]
    # every SDK call behind the limiter and booked with requests=1
    assert len(lim.acquired) == len(fake.calls)
    assert len([x for x in gov.of("record") if x[3] == 1]) == len(fake.calls)
    assert await db.fetchval("SELECT COUNT(*) FROM job_state WHERE key LIKE 'resv:%'") == 0


async def test_unpinned_version_skips(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings(LLM_BULK_MODEL="claude-haiku-4-5-20251001"))
    await _seed(db)
    fake = FakeAnthropic()
    gov = FakeGovernor()
    res = await LlmClassifier(db, fake, gov, FakeLimiter(), clock=lambda: T0).submit_cycle("r")
    assert res["skipped"] == "version_unpinned" and fake.calls == [] and gov.log == []
