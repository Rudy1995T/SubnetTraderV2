"""Spec 11 §9 T38: review kinds, resolution actions, LLM candidates, auto-clear, confirmed-LLM sync."""
from __future__ import annotations

import pytest

from app.integrity import detectors as D
from app.integrity import review
from app.integrity.features import sync_confirmed_llm_flags
from tests.integrity.conftest import DAY, NEWCK, OWNER, T0, seed_occ

pytestmark = pytest.mark.asyncio
URL = "https://x.com/team/status/9"


async def _change(db, k):
    async def cl():
        return "coldkey_swap", "old_ck_emptied"
    return await D.detect_coldkey_change(db, k, 64, OWNER, NEWCK, event_at=T0, observed_at=T0, run_id="t",
                                         detected_via="tmc_snapshot", classify=cl)


async def test_review_kinds(db, iset):
    o = await seed_occ(db, 64)
    k = o["occupant_key"]
    for kind in sorted(review.KINDS_11):
        assert await review.enqueue(db, occupant_key=k, kind=kind, dedupe_base=f"{kind}|{k}", now=T0)
    assert await review.enqueue(db, occupant_key=k, kind="llm_flag_candidate", post_id="discord:1",
                                label_version="v1", suggested_flag="key_compromise", now=T0)
    assert await review.enqueue(db, occupant_key=k, kind="llm_flag_candidate", post_id="discord:1",
                                label_version="v1", now=T0) is None
    for bad in ("team_candidates", "owner_control_change"):
        with pytest.raises(ValueError, match="invalid_kind"):
            await review.enqueue(db, occupant_key=k, kind=bad, dedupe_base="x", now=T0)
    with pytest.raises(ValueError):
        await review.enqueue(db, occupant_key=k, kind="team_candidate", post_id="p", label_version="v",
                             dedupe_base="b")


async def test_review_resolution(db, iset):                                        # T38
    o = await seed_occ(db, 64)
    k = o["occupant_key"]
    cid = await _change(db, k)
    rid = await db.fetchval("SELECT id FROM review_queue WHERE ref_id = ?", (cid,))
    assert (await review.resolve_review(db, rid, "whitelist", by="r", note=None, whitelist_reason="multisig_migration"
                                        )).error == "evidence_required"
    r = await review.resolve_review(db, rid, "whitelist", by="r", note="disclosed", evidence_url=URL,
                                    whitelist_reason="multisig_migration", now=T0 + DAY)
    assert r.ok and r.status == "confirmed"
    wl = await db.fetchone("SELECT * FROM owner_event_whitelist")
    assert (wl["reason"], wl["applies_to"], wl["owner_change_id"]) == ("multisig_migration", "coldkey_change", cid)
    assert (wl["window_start"], wl["window_end"]) == (T0 - DAY, T0 + DAY)
    assert await db.fetchval("SELECT whitelisted FROM owner_changes WHERE id = ?", (cid,)) == 1
    assert (await review.resolve_review(db, rid, "dismiss", by="r", note=None)).error == "invalid_transition"
    # extraction candidate → confirm_flag writes manual_flags(owner_extraction, confirmed)
    x = await review.enqueue(db, occupant_key=k, kind="owner_extraction_candidate", dedupe_base="oec|1",
                             suggested_flag="owner_extraction", now=T0)
    assert (await review.resolve_review(db, x, "confirm_flag", by="r", note=None, evidence_url=URL)).ok
    assert await db.fetchval("SELECT flag || ':' || state FROM manual_flags") == "owner_extraction:confirmed"
    # LLM candidates
    kc = await review.enqueue(db, occupant_key=k, kind="llm_flag_candidate", post_id="discord:2", label_version="v1",
                              suggested_flag="key_compromise", now=T0)
    r = await review.resolve_review(db, kc, "confirm_flag", by="r", note=None, evidence_url=URL)
    assert r.ok and r.warning is None                       # 10's confirm_review exists since Phase 9
    assert await db.fetchval("SELECT COUNT(*) FROM manual_flags WHERE flag = 'key_compromise'") == 1
    si = await review.enqueue(db, occupant_key=k, kind="llm_flag_candidate", post_id="discord:3", label_version="v1",
                              suggested_flag="security_incident", now=T0)
    assert (await review.resolve_review(db, si, "confirm_flag", by="r", note=None, evidence_url=URL)).error == \
        "invalid_transition"
    r = await review.resolve_review(db, si, "done", by="r", note="ack")
    assert r.ok and r.status == "resolved"
    assert await db.fetchval("SELECT COUNT(*) FROM manual_flags") == 2
    nf = await review.enqueue(db, occupant_key=k, kind="llm_flag_candidate", post_id="discord:4", label_version="v1",
                              now=T0)
    for act in ("confirm_flag", "done", "key_compromise"):
        assert (await review.resolve_review(db, nf, act, by="r", note=None, evidence_url=URL)).error == \
            "invalid_transition"
    assert (await review.resolve_review(db, nf, "dismiss", by="r", note=None)).status == "rejected"
    td = await review.enqueue(db, occupant_key=k, kind="team_delisted", dedupe_base="team_delisted|1", now=T0)
    assert (await review.resolve_review(db, td, "done", by="r", note=None)).status == "resolved"
    rows = await review.list_open(db)
    assert all(r["status"] == "open" for r in rows)


async def test_sync_confirmed_llm_flags(db, iset):
    o = await seed_occ(db, 64)
    from app.storage.events import emit_event
    await emit_event(db, event_type="owner_extraction_evidence", occupant_key=o["occupant_key"], netuid=64,
                     event_at=T0, observed_at=T0, source="llm", run_id="r", dedupe_key="e1",
                     payload={"post_ids": ["discord:1"], "label_version": "v1", "direction": -1, "materiality": 3,
                              "summary": "owner dumped", "url": "https://discord.com/x", "confidence": 0.9,
                              "n_supporting": 1, "confirmed_by": "rudolf"})
    assert await sync_confirmed_llm_flags(db, T0 + 60) == 1
    assert await sync_confirmed_llm_flags(db, T0 + 120) == 0
    f = await db.fetchone("SELECT * FROM manual_flags")
    assert (f["flag"], f["state"], f["set_by"]) == ("owner_extraction", "confirmed", "sync:llm_confirmed")
