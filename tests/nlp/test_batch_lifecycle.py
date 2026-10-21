"""Spec 10 §9 test_batch_lifecycle: submit → in_progress → ended (succeeded / errored invalid_request / expired) →
labels + requeues; restart resumes the same provider batch; apply twice is idempotent; stale batch → cancel → expired;
building batches after a power loss → failed + released."""
from __future__ import annotations

import pytest

import app.config as app_config
from app.nlp.llm_classify import LlmClassifier
from tests.nlp.fakes import BULK_V, DAY, HOUR, T0, FakeAnthropic, FakeGovernor, FakeLimiter, err_result, gone_result, \
    item_obj, n_items, nlp_settings, ok_result, seed_occupant, seed_x

pytestmark = pytest.mark.asyncio


def _results(req):
    cid = req["custom_id"]
    k = int(cid.rsplit("-", 1)[1])
    if k == 0:
        n = n_items(req["params"])
        return ok_result(cid, [item_obj(i + 1, event_type="product_launch", direction=1, materiality=2,
                                        polarity="bullish", intensity=3, entities=[{"netuid": 64, "confidence": 0.9}])
                               for i in range(n)])
    if k == 1:
        return err_result(cid, "invalid_request")
    return gone_result(cid, "expired")


async def _seed(db, n=6):
    await seed_occupant(db, 64, name="Chutes")
    for i in range(n):
        await seed_x(db, f"{i:02d}", f"SN64 post number {i}", event_at=T0 - HOUR, author_id=f"a{i}")


async def test_lifecycle_requeue_restart_idempotent(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings(LLM_BATCH_ITEMS_PER_REQUEST=5))
    await _seed(db, 12)
    fake = FakeAnthropic(results_fn=_results)
    gov = FakeGovernor()
    clf = LlmClassifier(db, fake, gov, FakeLimiter(), clock=lambda: T0)
    res = await clf.submit_cycle("r1")
    assert res["n_requests"] == 3 and res["n_items"] == 12
    b = await db.fetchone("SELECT * FROM llm_batches")
    assert b["status"] == "submitted" and b["provider_batch_id"] == "msgbatch_001" and b["classifier_version"] == BULK_V
    assert await db.fetchval("SELECT value_json FROM job_state WHERE job = 'llm_classify_submit' AND key = ?",
                             (f"resv:{b['batch_key']}",))
    out = await clf.poll_cycle("r2")                                         # in_progress → nothing changes
    assert out["batches"][0]["status"] == "in_progress"
    assert (await db.fetchone("SELECT status FROM llm_batches"))["status"] == "submitted"
    fake.end_all()
    clf2 = LlmClassifier(db, fake, gov, FakeLimiter(), clock=lambda: T0 + 600)   # restart: a fresh classifier
    out = await clf2.poll_cycle("r3")
    assert out["batches"][0]["n_labelled"] == 5 and out["batches"][0]["n_errored"] == 1
    st = {r["status"]: r["n"] for r in await db.fetchall("SELECT status, COUNT(*) AS n FROM llm_batch_items "
                                                         "GROUP BY status")}
    assert st == {"labelled": 5, "errored": 5, "requeued": 2}
    b = await db.fetchone("SELECT * FROM llm_batches")
    assert b["status"] == "applied" and b["n_succeeded"] == 1 and b["n_expired"] == 1 and b["cost_usd"] > 0
    assert [c for c in fake.calls if c[0] == "batches.results"] == [("batches.results", "msgbatch_001")]
    n_rows = await db.fetchval("SELECT COUNT(*) FROM post_labels")
    again = await clf2.apply_batch(b["batch_key"], await fake.messages.batches.results("msgbatch_001"))
    assert again["n_labelled"] == 0 and await db.fetchval("SELECT COUNT(*) FROM post_labels") == n_rows == 5
    ent = await db.fetchval("SELECT COUNT(*) FROM post_label_entities WHERE via = 'model'")
    assert ent == 5
    # next cycle: invalid_request items at 10/request (small), expired at full size
    fake2 = FakeAnthropic()
    clf3 = LlmClassifier(db, fake2, gov, FakeLimiter(), clock=lambda: T0 + 700)
    res = await clf3.submit_cycle("r4")
    sizes = sorted(n_items(r["params"]) for r in fake2.submitted)
    assert res["n_items"] == 7 and sizes == [2, 5]
    att = {r["post_id"]: r["attempt"] for r in await db.fetchall(
        "SELECT post_id, attempt FROM llm_batch_items WHERE batch_key = ?", (res["batch_key"],))}
    assert set(att.values()) == {2}


async def test_stale_batch_cancel_then_expired(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    await _seed(db, 3)
    fake = FakeAnthropic(results_fn=lambda req: gone_result(req["custom_id"], "canceled"))
    gov = FakeGovernor()
    now = [T0]
    clf = LlmClassifier(db, fake, gov, FakeLimiter(), clock=lambda: now[0])
    await clf.submit_cycle("r1")
    now[0] = T0 + 27 * HOUR
    out = await clf.poll_cycle("r2")
    assert out["batches"][0]["status"] == "cancel_requested" and ("batches.cancel", "msgbatch_001") in fake.calls
    fake.end_all()
    await clf.poll_cycle("r3")
    assert (await db.fetchone("SELECT status FROM llm_batches"))["status"] == "expired"
    assert {r["status"] for r in await db.fetchall("SELECT status FROM llm_batch_items")} == {"requeued"}
    assert gov.of("commit")[0][2] == 0.0


async def test_building_batch_after_power_loss(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    await _seed(db, 2)
    await db.execute("INSERT INTO llm_batches (batch_key, model, classifier_version, status, created_at) VALUES "
                     "('bk1', 'claude-haiku-5-5', ?, 'building', ?)", (BULK_V, T0 - DAY))
    await db.execute("INSERT INTO llm_batch_items (batch_key, custom_id, post_id) VALUES ('bk1', 'bk1-0000', 'x:00')")
    await db.execute("INSERT INTO job_state (job, key, value_json, updated_at) VALUES ('llm_classify_submit', "
                     "'resv:bk1', '{\"reservation_id\": \"resv-x\", \"est_usd\": 0.01}', 0)")
    gov = FakeGovernor()
    clf = LlmClassifier(db, FakeAnthropic(), gov, FakeLimiter(), clock=lambda: T0)
    await clf.poll_cycle("r")
    assert (await db.fetchone("SELECT status FROM llm_batches WHERE batch_key = 'bk1'"))["status"] == "failed"
    assert gov.of("release") == [("release", "resv-x")]
    assert await db.fetchval("SELECT status FROM llm_batch_items WHERE batch_key = 'bk1'") == "requeued"


async def test_submit_skips(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    fake = FakeAnthropic()
    clf = LlmClassifier(db, fake, FakeGovernor(), FakeLimiter(), clock=lambda: T0)
    with pytest.raises(RuntimeError, match="alias_table_empty"):
        await clf.submit_cycle("r")
    await seed_occupant(db, 64)
    assert await clf.submit_cycle("r") == {"skipped": "empty_queue"}
    assert fake.calls == []


async def test_jobs_through_wrapper_with_real_governor(rt, monkeypatch):
    """Submit + poll jobs via 01's wrapper, the real CostGovernor (reserve → commit) and the /health extras."""
    import app.nlp.llm_classify as LC
    from app.jobs import SPEC_BY_ID
    s0 = app_config.settings
    monkeypatch.setattr(app_config, "settings", nlp_settings(KILL_SWITCH_PATH=str(s0.KILL_SWITCH_PATH),
                                                             PAUSE_ALL_PATH=str(s0.PAUSE_ALL_PATH),
                                                             CONTROL_DIR=str(s0.CONTROL_DIR), DATA_DIR=str(s0.DATA_DIR),
                                                             RUN_DIR=str(s0.RUN_DIR)))
    await seed_occupant(rt.db, 64)
    from app.util.time import now_ts
    for i in range(3):
        await seed_x(rt.db, str(i), f"SN64 live post {i}", event_at=now_ts() - HOUR)
    fake = FakeAnthropic()
    monkeypatch.setattr(LC, "make_client", lambda s=None: fake)
    sub = LC.LlmClassifySubmitJob(rt, SPEC_BY_ID["llm_classify_submit"])
    await sub.run(trigger="cli")
    r = await rt.db.fetchone("SELECT ok, rows, note, error FROM collector_runs WHERE job = 'llm_classify_submit'")
    assert r["ok"] == 1 and r["rows"] == 3 and r["note"].startswith("submitted msgbatch_001"), r
    assert rt.costs.reserved_usd("llm") > 0
    fake.end_all()
    poll = LC.LlmBatchPollJob(rt, SPEC_BY_ID["llm_batch_poll"])
    await poll.run(trigger="cli")
    r = await rt.db.fetchone("SELECT ok, rows, error FROM collector_runs WHERE job = 'llm_batch_poll'")
    assert r["ok"] == 1 and r["rows"] == 3, r
    assert rt.costs.reserved_usd("llm") == 0 and rt.costs.month_usd("llm") > 0
    h = poll.status()["extra"]
    assert h["open_batches"] == 0 and h["labelled_24h"] == 3 and h["classifier_version_bulk"] == BULK_V
    assert set(h) >= {"queue_depth", "oldest_unlabelled_h", "escalations_24h", "escalation_pending",
                      "parse_error_rate_24h", "fallback_active", "review_open", "classifier_version_escalation",
                      "rubric_sha256"}
