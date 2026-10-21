"""Spec 11 §9 T8–T15: team domain functions, team_doxxed, PIT replay, seeding."""
from __future__ import annotations

import sqlite3

import pytest

from app.integrity import team, team_seed
from tests.integrity.conftest import DAY, T0, seed_occ, seed_team_info

LI = "https://www.linkedin.com/in/probe-person"
pytestmark = pytest.mark.asyncio


async def test_state_machine(db, iset):                                           # T8
    await seed_occ(db, 64)
    r = await team.add_member(db, netuid=64, name="Probe Person", role="CTO", profile_url=LI, evidence_url=None,
                              evidence_note=None, source="manual", created_by="cli:rudolf", now=T0)
    assert r.ok and r.status == "candidate"
    v = await team.verify_member(db, r.id, evidence_urls=[LI + "/details"], note=None, by="rudolf", now=T0 + 10)
    assert not v.ok and v.error == "two_source_rule" and v.status == "candidate"
    v = await team.verify_member(db, r.id, evidence_urls=["https://chutes.ai/team"], note=None, by="rudolf",
                                 now=T0 + DAY)
    assert v.ok and v.status == "verified"
    m = await db.fetchone("SELECT * FROM team_members WHERE id = ?", (r.id,))
    assert m["verified_at"] == T0 + DAY and m["evidence_count"] >= 2 and m["evidence_url"]
    ev = await db.fetchone("SELECT payload FROM events WHERE event_type = 'team_member_verified'")
    assert '"checked_at":' in ev["payload"]
    v2 = await team.verify_member(db, r.id, evidence_urls=[], note="re-checked on the team page 2027-01-10",
                                  by="rudolf", now=T0 + 101 * DAY)
    m = await db.fetchone("SELECT * FROM team_members WHERE id = ?", (r.id,))
    assert v2.ok and m["verified_at"] == T0 + DAY and m["last_checked_at"] == T0 + 101 * DAY
    rj = await team.reject_member(db, r.id, reason="not on team page", evidence_url=None, by="rudolf",
                                  now=T0 + 102 * DAY)
    assert rj.ok and rj.status == "rejected"
    r2 = await team.add_member(db, netuid=64, name="Other Person", role=None, profile_url=LI + "2",
                               evidence_url=None, evidence_note=None, source="manual", created_by="api", now=T0)
    with pytest.raises(sqlite3.IntegrityError):
        await db.execute("UPDATE team_members SET status = 'verified', verified_at = 1, verified_by = 'x', "
                         "evidence_url = 'https://a.b/c', evidence_count = 1 WHERE id = ?", (r2.id,))


async def test_domain_errors(db, iset):                                           # T12
    await seed_occ(db, 64)
    assert (await team.add_member(db, netuid=999, name="X Y", role=None, profile_url=None, evidence_url=None,
                                  evidence_note=None, source="manual", created_by="api")).error == "no_current_occupant"
    assert (await team.add_member(db, netuid=64, name="X Y", role=None, profile_url="ftp://a.b/c", evidence_url=None,
                                  evidence_note=None, source="manual", created_by="api")).error == "invalid_url"
    a = await team.add_member(db, netuid=64, name="Jane Doe", role=None, profile_url=LI, evidence_url=LI,
                              evidence_note=None, source="manual", created_by="api", verify=True)
    assert a.ok and a.status == "candidate" and a.warning == "two_source_rule"
    d = await team.add_member(db, netuid=64, name="jane  doe", role=None, profile_url=None, evidence_url=None,
                              evidence_note=None, source="manual", created_by="api")
    assert not d.ok and d.error == "duplicate_member" and d.id == a.id
    assert (await team.verify_member(db, a.id, evidence_urls=[], note=None, by="x")).error == "evidence_required"
    assert (await team.verify_member(db, 9999, evidence_urls=["https://a.io"], note=None, by="x")).error == "not_found"


async def test_team_doxxed_map(db, iset):                                         # T9
    o = await seed_occ(db, 64)
    ok_ = o["occupant_key"]

    async def add(name, verify_at=None):
        r = await team.add_member(db, occupant_key=ok_, name=name, role=None, profile_url=f"https://linkedin.com/in/{name[:3]}",
                                  evidence_url=None, evidence_note=None, source="manual", created_by="api", now=T0)
        if verify_at is not None:
            await team.verify_member(db, r.id, evidence_urls=["https://chutes.ai/team"], note=None, by="x",
                                     now=verify_at)
        return r.id
    t = T0 + 10 * DAY
    assert (await team.compute_team_doxxed(db, ok_, t)).reason == "no_record"
    await seed_team_info(db, ok_, "Awaiting Data...", observed_at=T0)
    m = await team.compute_team_doxxed(db, ok_, t)
    assert m.value is None and m.status == "failed" and m.reason == "placeholder"
    await team.set_disclosure(db, netuid=64, status="anonymous", evidence_url="https://x.com/a/status/1", note=None,
                              by="x", now=T0 + DAY)
    m = await team.compute_team_doxxed(db, ok_, t)
    assert m.value == 0.0 and m.status == "absent" and m.reason == "anonymous"
    await team.set_disclosure(db, netuid=64, status="clear", evidence_url="https://x.com/a/status/2", note=None,
                              by="x", now=T0 + 2 * DAY)
    assert (await team.compute_team_doxxed(db, ok_, t)).reason == "placeholder"
    a = await add("Ann Lee")
    m = await team.compute_team_doxxed(db, ok_, t)
    assert m.value == 0.3 and m.value_json["n_verified"] == 0 and m.value_json["n_named"] == 1
    await team.verify_member(db, a, evidence_urls=["https://chutes.ai/team"], note=None, by="x", now=T0 + 3 * DAY)
    assert (await team.compute_team_doxxed(db, ok_, t)).value == 0.6
    await add("Bob Ray", verify_at=T0 + 4 * DAY)
    m = await team.compute_team_doxxed(db, ok_, t)
    assert m.value == 1.0 and m.value_json["n_verified"] == 2
    m = await team.compute_team_doxxed(db, ok_, T0 + 200 * DAY)                   # both stale → named
    assert m.value == 0.3 and m.value_json["stale_verified"] == 2
    for mid in (a,):
        await team.reject_member(db, mid, reason="left", evidence_url=None, by="x", now=T0 + 5 * DAY)


async def test_team_doxxed_all_rejected(db, iset):
    o = await seed_occ(db, 9)
    await seed_team_info(db, o["occupant_key"], "<p><strong>Ann Lee</strong> – CEO</p>", observed_at=T0)
    await team_seed.seed_subnetalpha(db, now=T0)
    mid = await db.fetchval("SELECT id FROM team_members")
    await team.reject_member(db, mid, reason="not real", evidence_url=None, by="x", now=T0 + 1)
    m = await team.compute_team_doxxed(db, o["occupant_key"], T0 + DAY)
    assert m.value is None and m.reason == "all_rejected"


async def test_team_pit_replay(db, iset):                                         # T10
    o = await seed_occ(db, 64)
    r = await team.add_member(db, occupant_key=o["occupant_key"], name="Ann Lee", role=None,
                              profile_url="https://linkedin.com/in/ann", evidence_url=None, evidence_note=None,
                              source="manual", created_by="api", now=T0)
    await team.verify_member(db, r.id, evidence_urls=["https://chutes.ai/team"], note=None, by="x", now=T0 + DAY)
    await team.reject_member(db, r.id, reason="left", evidence_url=None, by="x", now=T0 + 5 * DAY)
    assert (await team.compute_team_doxxed(db, o["occupant_key"], T0 + 3600)).value == 0.3
    assert (await team.compute_team_doxxed(db, o["occupant_key"], T0 + 2 * DAY)).value == 0.6
    m = await team.compute_team_doxxed(db, o["occupant_key"], T0 + 6 * DAY)
    assert m.value is None and m.value_json["rejected"] == 1


async def test_seed_idempotent_no_resurrection(db, iset):                         # T11
    o = await seed_occ(db, 64)
    ok_ = o["occupant_key"]
    html = "<p><strong>Ann Lee</strong> – CEO</p><p><strong>Bob Ray</strong> – CTO</p>"
    await seed_team_info(db, ok_, html, observed_at=T0)
    r1 = await team_seed.seed_subnetalpha(db, now=T0)
    r2 = await team_seed.seed_subnetalpha(db, now=T0 + DAY)
    assert r1["new"] == 2 and r2["new"] == 0 and r2["seen"] == 2
    assert await db.fetchval("SELECT COUNT(*) FROM team_members") == 2
    assert await db.fetchval("SELECT COUNT(*) FROM team_members WHERE status <> 'candidate'") == 0
    bob = await db.fetchval("SELECT id FROM team_members WHERE name = 'Bob Ray'")
    await team.reject_member(db, bob, reason="unknown", evidence_url=None, by="x", now=T0 + DAY)
    await team_seed.seed_subnetalpha(db, now=T0 + 2 * DAY)
    m = await db.fetchone("SELECT status, source_last_seen_at FROM team_members WHERE id = ?", (bob,))
    assert m["status"] == "rejected" and m["source_last_seen_at"] == T0 + 2 * DAY
    rows = await db.fetchall("SELECT * FROM review_queue WHERE kind = 'team_candidate'")
    assert len(rows) == 1 and '"n_new":2' in rows[0]["payload_json"].replace(" ", "")
    from app.integrity.review import auto_clear, enqueue
    await db.execute("UPDATE team_members SET status = 'rejected' WHERE status = 'candidate'")
    await auto_clear(db, T0 + 3 * DAY)
    assert (await db.fetchone("SELECT status, resolution_action FROM review_queue"))["status"] == "expired"
    await seed_team_info(db, ok_, html + "<p><strong>Cy Tan</strong> – Lead</p>", observed_at=T0 + 4 * DAY)
    await team_seed.seed_subnetalpha(db, now=T0 + 4 * DAY)
    assert await db.fetchval("SELECT COUNT(*) FROM review_queue WHERE kind = 'team_candidate'") == 2
    base = f"team_candidate|{ok_}"
    with pytest.raises(sqlite3.IntegrityError):
        await db.execute("INSERT INTO review_queue (occupant_key, kind, dedupe_key, dedupe_base, created_at) "
                         "SELECT occupant_key, kind, dedupe_key, dedupe_base, created_at FROM review_queue WHERE "
                         "status = 'open' AND dedupe_base = ?", (base,))
    assert await enqueue(db, occupant_key=ok_, kind="team_candidate", dedupe_base=base, payload={"n_new": 1},
                         now=T0 + 5 * DAY) is not None


async def test_seed_stale_for_occupant(db, iset):                                 # T13
    o = await seed_occ(db, 40, reg=8409860, reg_ts=T0)
    await seed_team_info(db, o["occupant_key"], "<p><strong>Ann Lee</strong> – CEO</p>", observed_at=T0 + DAY,
                         event_at=T0 - 30 * DAY)
    r = await team_seed.seed_subnetalpha(db, now=T0 + DAY)
    assert r["new"] == 0 and r["disclosure"] == {"stale_for_occupant": 1}
    m = await team.compute_team_doxxed(db, o["occupant_key"], T0 + 2 * DAY)
    assert m.value is None and m.reason == "stale_for_occupant"


async def test_seed_delisted(db, iset):                                           # T14
    o = await seed_occ(db, 64)
    await seed_team_info(db, o["occupant_key"], "<p><strong>Ann Lee</strong> – CEO</p>", observed_at=T0)
    await team_seed.seed_subnetalpha(db, now=T0)
    mid = await db.fetchval("SELECT id FROM team_members")
    await team.verify_member(db, mid, evidence_urls=["https://chutes.ai/team", "https://theblock.co/p/1"], note=None,
                             by="x", now=T0 + 1)
    await seed_team_info(db, o["occupant_key"], "<p><strong>Bob Ray</strong> – CEO</p>", observed_at=T0 + DAY)
    await team_seed.seed_subnetalpha(db, now=T0 + DAY)
    assert await db.fetchval("SELECT COUNT(*) FROM events WHERE event_type = 'team_member_delisted'") == 1
    assert await db.fetchval("SELECT COUNT(*) FROM review_queue WHERE kind = 'team_delisted'") == 1
    assert await db.fetchval("SELECT status FROM team_members WHERE id = ?", (mid,)) == "verified"


async def test_legacy_seed(db, iset, tmp_path):                                   # T15
    import shutil
    from pathlib import Path
    src = Path(__file__).resolve().parents[2] / "seed" / "subnet_edge"
    if not (src / "sn120_affine.md").exists():
        pytest.skip("legacy notes not copied (checklist A3)")
    d = tmp_path / "edge"
    shutil.copytree(src, d)
    await seed_occ(db, 120, reg=5749344, reg_ts=T0 - 400 * DAY)
    await seed_occ(db, 67, reg=4958013, reg_ts=T0 + 400 * DAY)       # re-registered after the notes' as-of
    as_of = team_seed.legacy_as_of()
    out = await team_seed.seed_legacy(db, d, as_of=as_of, now=T0)
    assert [(c[0], c[1], c[2], c[3]) for c in out["candidates"]] == [(120, "Const", "founder", "sn120_affine.md:14")]
    assert out["notes"] == []
    row = await db.fetchone("SELECT * FROM team_members")
    assert row["source"] == "legacy" and row["source_ref"] == "sn120_affine.md:14" and row["status"] == "candidate"
    await db.execute("UPDATE occupants SET registered_ts = ?, registered_at_est = ? WHERE netuid = 67",
                     (T0 - 400 * DAY, T0 - 400 * DAY))
    out = await team_seed.seed_legacy(db, d, as_of=as_of, now=T0)
    assert out["notes"] == [(67, "sn67_harnyx.md:14")]
    await team_seed.seed_legacy(db, d, as_of=as_of, now=T0)
    assert await db.fetchval("SELECT COUNT(*) FROM manual_notes WHERE kind = 'team_disclosure'") == 1
    assert await db.fetchval("SELECT COUNT(*) FROM team_members") == 1
