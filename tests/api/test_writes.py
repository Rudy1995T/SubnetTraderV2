"""Spec 14 §9 authed-tier tests: auth + origin guard, manual team/product/note/flag/owner routes, review, registry,
needs-review merge, control pause/resume and rescore."""
from __future__ import annotations

import asyncio

import pytest

from tests.api.conftest import D0, OCC9, OCC64, SPEC

pytestmark = pytest.mark.asyncio
U = "https://example.com/evidence"


async def test_auth_and_origin(seeded, app_client, cfg):
    body = {"netuid": 64, "text": "x", "evidence_url": U}
    cfg(API_AUTH_TOKEN="t0k3n")
    assert (await app_client.post("/api/v1/manual/note", json=body)).status_code == 401
    H = {"X-Auth-Token": "t0k3n"}
    assert (await app_client.post("/api/v1/manual/note", json=body, headers=H)).status_code == 201
    r = await app_client.post("/api/v1/manual/note", json=body, headers={**H, "Origin": "https://evil.example"})
    assert r.status_code == 403 and r.json()["error"] == "origin_not_allowed"
    assert (await app_client.post("/api/v1/manual/note", json=body,
                                  headers={**H, "Origin": "http://t"})).status_code == 201         # same origin
    assert (await app_client.post("/api/v1/manual/note", json=body,
                                  headers={**H, "Origin": "http://127.0.0.1:3001"})).status_code == 201  # allowed
    cfg(API_AUTH_TOKEN="")
    assert (await app_client.post("/api/v1/manual/note", json=body)).status_code == 201         # auth off
    r = await app_client.post("/api/v1/control/pause/x", headers={"Origin": "https://evil.example"})
    assert r.status_code == 403                                     # body-less cross-site POST blocked with auth off
    from app.api import wb
    from app.api.errors import origin_guard
    posts = [r for r in wb.router.routes if "POST" in getattr(r, "methods", ())]
    assert posts and all(any(d.dependency is origin_guard for d in r.dependencies) for r in posts)   # 19's POSTs too


async def test_manual_team_routes(seeded, app_client):
    db = seeded.db
    r = await app_client.post("/api/v1/manual/team", json={"netuid": 64, "name": "Ann Lee", "role": "cto",
                                                          "profile_url": "https://x.com/annlee"})
    assert r.status_code == 201 and r.json()["status"] == "candidate"
    mid = r.json()["id"]
    assert await db.fetchval("SELECT COUNT(*) FROM events WHERE event_type = 'team_member_added'", writer=True) == 1
    r = await app_client.post(f"/api/v1/manual/team/{mid}/verify", json={"evidence_urls": ["https://x.com/annlee/status/1"]})
    assert r.status_code == 409 and r.json()["error"] == "needs_second_source"
    r = await app_client.post(f"/api/v1/manual/team/{mid}/verify", json={"evidence_urls": ["https://chutes.ai/team"]})
    assert r.status_code == 200 and r.json()["status"] == "verified"
    r = await app_client.post("/api/v1/manual/team", json={"netuid": 64, "name": "Ann Lee"})
    assert r.status_code == 409 and r.json()["error"] == "duplicate_member"
    r2 = await app_client.post("/api/v1/manual/team", json={"netuid": 64, "name": "Bob Ray"})
    r = await app_client.post(f"/api/v1/manual/team/{r2.json()['id']}/reject", json={"reason": "not on the team"})
    assert r.status_code == 200 and r.json()["status"] == "rejected"
    r = await app_client.post("/api/v1/manual/team/disclosure", json={"netuid": 64, "status": "clear", "evidence_url": U})
    assert r.status_code == 201
    assert await db.fetchval("SELECT value FROM manual_notes WHERE kind = 'team_disclosure'", writer=True) == "clear"
    assert (await app_client.post("/api/v1/manual/team", json={"netuid": 999, "name": "X"})).status_code == 404


async def test_manual_product_and_note(seeded, app_client):
    r = await app_client.post("/api/v1/manual/product", json={"netuid": 64, "status": "beta", "evidence_url": U})
    assert r.status_code == 201 and r.json()["extra"]["previous"] == "live"
    ev = await seeded.db.fetchone("SELECT payload FROM events WHERE event_type = 'manual_entry' ORDER BY id DESC",
                                  writer=True)
    assert '"kind":"product"' in ev["payload"]
    assert (await app_client.post("/api/v1/manual/product", json={"netuid": 64, "status": "live"})).status_code == 422
    r = await app_client.post("/api/v1/manual/product", json={"netuid": 64, "status": None, "evidence_url": U})
    assert r.status_code == 201 and r.json()["status"] == "cleared"
    r = await app_client.post("/api/v1/manual/note", json={"netuid": 64, "text": "hello", "evidence_url": "nope"})
    assert r.status_code == 422 and r.json()["error"] in ("invalid_url", "invalid_body")


async def test_manual_flag_rules(seeded, app_client):
    db = seeded.db
    f = lambda **kw: app_client.post("/api/v1/manual/flag", json={"evidence_url": U, **kw})  # noqa: E731
    r = await f(netuid=64, flag="silence_45d", action="confirm")
    assert r.status_code == 422 and r.json()["error"] == "flag_not_human_confirmable"
    r = await f(netuid=64, flag="owner_control_change", action="clear")
    assert r.status_code == 409 and r.json()["error"] == "flag_not_active"
    assert (await f(netuid=9, flag="owner_control_change", action="clear")).status_code == 201   # sticky, active
    assert (await f(netuid=64, flag="owner_extraction", action="confirm")).status_code == 201
    assert (await f(netuid=64, flag="owner_extraction", action="clear")).status_code == 201      # confirmed = active
    rows = await db.fetchall("SELECT state FROM manual_flags WHERE occupant_key = ? AND flag = 'owner_extraction' "
                             "ORDER BY id", (OCC64,), writer=True)
    assert [r["state"] for r in rows] == ["confirmed", "cleared"]
    assert (await f(netuid=64, flag="owner_extraction", action="clear")).status_code == 409


async def test_manual_owner_routes(seeded, app_client):
    from app.util.time import to_iso
    r = await app_client.post("/api/v1/manual/owner/whitelist", json={
        "netuid": 64, "reason": "multisig_migration", "applies_to": "coldkey_change",
        "window_start": to_iso(D0 - 86400), "window_end": to_iso(D0), "evidence_url": U})
    assert r.status_code == 201, r.json()
    wid = r.json()["id"]
    r = await app_client.post(f"/api/v1/manual/owner/whitelist/{wid}/retire")
    assert r.status_code == 200
    assert await seeded.db.fetchval("SELECT retired_at FROM owner_event_whitelist WHERE id = ?", (wid,),
                                    writer=True) is not None
    r = await app_client.post("/api/v1/manual/owner/compromise", json={
        "netuid": 64, "window_start": to_iso(D0 - 60 * 86400), "window_end": to_iso(D0), "evidence_url": U})
    assert r.status_code == 422 and r.json()["error"] == "window_too_long"
    r = await app_client.post("/api/v1/manual/owner/whitelist", json={
        "netuid": 64, "reason": "bogus", "applies_to": "coldkey_change", "window_start": to_iso(D0),
        "window_end": to_iso(D0), "evidence_url": U})
    assert r.status_code == 422 and r.json()["error"] == "invalid_kind"


async def test_review_routes(seeded, app_client):
    from app.integrity import review as review11
    rid = await review11.enqueue(seeded.db, occupant_key=OCC64, kind="owner_extraction_candidate",
                                 dedupe_base="t|1", summary="big transfer", suggested_flag="owner_extraction")
    tid = await review11.enqueue(seeded.db, occupant_key=OCC64, kind="team_candidate", dedupe_base="t|2")
    b = (await app_client.get("/api/v1/review")).json()
    assert {i["id"] for i in b["items"]} == {rid, tid} and all(i["netuid"] == 64 for i in b["items"])
    assert (await app_client.get("/api/v1/review?kind=team_candidate")).json()["count"] == 1
    r = await app_client.post(f"/api/v1/review/{rid}/resolve", json={"action": "confirm_flag"})
    assert r.status_code == 422 and r.json()["error"] == "evidence_required"
    r = await app_client.post(f"/api/v1/review/{tid}/resolve", json={"action": "confirm_flag", "evidence_url": U})
    assert r.status_code == 409 and r.json()["error"] == "invalid_transition"
    r = await app_client.post(f"/api/v1/review/{rid}/resolve", json={"action": "dismiss"})
    assert r.status_code == 200 and r.json()["status"] == "rejected"


async def test_registry_routes(seeded, app_client, cfg):
    from app.storage.events import emit_event
    db = seeded.db
    cfg(API_AUTH_TOKEN="tok")
    for path in ("/api/v1/registry/overrides", "/api/v1/registry/overrides/1/status", "/api/v1/registry/refresh",
                 "/api/v1/registry/reoccupied/1/ack", "/api/v1/manual/override"):
        assert (await app_client.post(path, json={})).status_code == 401
        r = await app_client.post(path, json={}, headers={"X-Auth-Token": "tok", "Origin": "https://evil.example"})
        assert r.status_code == 403
    H = {"X-Auth-Token": "tok"}
    r = await app_client.post("/api/v1/registry/overrides", headers=H, json={
        "netuid": 64, "field": "subnet_url", "mode": "force", "value": "https://chutes.ai"})
    assert r.status_code == 422 and r.json()["error"] == "invalid_override"
    body = {"netuid": 64, "field": "subnet_url", "mode": "force", "value": "https://chutes.ai", "reason": "site moved",
            "evidence_url": U}
    r = await app_client.post("/api/v1/registry/overrides", headers=H, json=body)
    assert r.status_code == 201, r.json()
    oid = r.json()["id"]
    r = await app_client.post("/api/v1/manual/override", headers=H, json=body)                # alias, same handler
    assert r.status_code == 409 and r.json()["error"] == "override_exists" and r.json()["id"] == oid
    lst = (await app_client.get("/api/v1/registry/overrides?netuid=64")).json()
    assert [o["id"] for o in lst["overrides"]] == [oid]
    assert (await app_client.get(f"/api/v1/registry/overrides/{oid}")).json()["field"] == "subnet_url"
    det = (await app_client.get("/api/v1/registry/64")).json()
    assert det["occupant"]["occupant_key"] == OCC64 and [o["id"] for o in det["overrides"]] == [oid]
    r = await app_client.post(f"/api/v1/registry/overrides/{oid}/status", headers=H, json={"status": "retired"})
    assert r.status_code == 200
    assert (await app_client.get("/api/v1/registry/64")).json()["overrides"] == []
    eid = await emit_event(db, event_type="netuid_reoccupied", occupant_key=OCC64, netuid=64, event_at=D0,
                           observed_at=D0, payload={"netuid": 64, "old_occupant_key": "64:1", "new_occupant_key": OCC64,
                                                    "old_network_registered_at": 1, "new_network_registered_at": 4123456,
                                                    "old_name": "Old", "new_name": "Chutes", "old_status_before": "active",
                                                    "old_last_seen_block": 2, "head_block": 3, "new_status": "active"},
                           dedupe_key=OCC64, source="registry", run_id=None, alerted_at=D0)
    nr = (await app_client.get("/api/v1/registry/needs-review?reason=reoccupied_unreviewed")).json()
    assert any(i.get("event_id") == eid for i in nr["items"])
    r = await app_client.post(f"/api/v1/registry/reoccupied/{eid}/ack", headers=H, json={"note": "reviewed"})
    assert r.status_code == 201
    assert await db.fetchval("SELECT COUNT(*) FROM manual_notes WHERE ref_event_id = ?", (eid,), writer=True) == 1
    nr = (await app_client.get("/api/v1/registry/needs-review?reason=reoccupied_unreviewed")).json()
    assert not any(i.get("event_id") == eid for i in nr["items"])
    r = await app_client.post("/api/v1/registry/reoccupied/1/ack", headers=H, json={"note": "x"})
    assert r.status_code == 422 and r.json()["error"] == "not_reoccupied_event"
    r = await app_client.post("/api/v1/registry/refresh", headers=H, json={})
    assert r.status_code == 202 and set(r.json()["unavailable"]) | set(r.json()["started"]) == {
        "registry_occupants", "registry_identity"}
    assert (await app_client.get("/api/v1/registry/occupants")).json()["count"] >= 3


async def test_needs_review_merge(seeded, monkeypatch):
    from app.api import queries as Q
    from app.registry.models import ReviewItem
    items = [ReviewItem(OCC64, 64, "Chutes", "source_disagreement_open", "github_repo", D0),
             ReviewItem(OCC9, 9, "Baz", "new_occupant", "first seen", D0)]

    async def fake(db, now):
        return items
    monkeypatch.setattr("app.registry.compute_needs_review", fake)
    b = await Q.needs_review(seeded.db, now=D0 + 86400)
    keys = list(b)
    assert keys[:2] == ["registry.new_occupant", "registry.source_disagreement_open"]   # 04's priority order
    assert "product_unset" in b and {x["netuid"] for x in b["product_unset"]} == {47, 9}

    async def boom(db, now):
        raise RuntimeError("x")
    monkeypatch.setattr("app.registry.compute_needs_review", boom)
    b = await Q.needs_review(seeded.db, now=D0 + 86400)
    assert "registry.error" in b and "product_unset" in b


async def test_control_pause_resume(seeded, app_client, tmp_root):
    db = seeded.db
    r = await app_client.post("/api/v1/control/pause/x")
    assert r.status_code == 200 and set(r.json()["jobs"]) == {"x_team_poll", "x_kol_poll", "x_mentions_poll",
                                                              "x_profile_refresh", "x_ctf_crosscheck"}
    assert await db.fetchval("SELECT COUNT(*) FROM control_state WHERE target = 'x'", writer=True) == 1
    assert not list((tmp_root / "data" / "control").iterdir())
    r = await app_client.post("/api/v1/control/pause/githu")
    assert r.status_code == 404 and r.json()["error"] == "unknown_target"
    assert (await app_client.post("/api/v1/control/pause/telegram_bot")).json()["error"] == "not_pausable"
    (tmp_root / "data" / "control" / "x.paused").write_text("")
    r = await app_client.post("/api/v1/control/resume/x")
    assert r.json()["removed_row"] is True and r.json()["file_pause_active"].endswith("x.paused")
    (tmp_root / "data" / "control" / "x.paused").unlink()
    assert (await app_client.post("/api/v1/control/resume/x")).json()["removed_row"] is False


async def test_control_rescore(seeded, app_client, tmp_root, monkeypatch):
    from app.api import control as control_api
    from app.scoring.engine import ScoringService
    calls = []
    gate = asyncio.Event()

    async def fake_rescore(self, as_of, spec, shadow=False):
        calls.append((as_of, spec, shadow))
        await gate.wait()
        return "rs-x"
    monkeypatch.setattr(ScoringService, "rescore", fake_rescore)
    control_api._rescore_task = None
    assert (await app_client.post("/api/v1/control/rescore", json={"shadow": True})).json()["error"] == \
        "shadow_requires_other_spec"
    assert (await app_client.post("/api/v1/control/rescore", json={"spec": "ts-9"})).json()["error"] == "spec_unknown"
    r = await app_client.post("/api/v1/control/rescore", json={"as_of": "2001-01-01"})
    assert r.status_code == 409 and r.json()["error"] == "slot_regresses"
    from app.util.time import to_iso
    r = await app_client.post("/api/v1/control/rescore", json={"as_of": to_iso(D0)[:10]})
    assert r.status_code == 409 and r.json()["error"] == "slot_already_scored"
    r = await app_client.post("/api/v1/control/rescore", json={})
    assert r.status_code == 202 and r.json()["request_id"].startswith("rq-") and r.json()["spec_version"] == SPEC
    await asyncio.sleep(0.05)
    assert calls == [(None, SPEC, False)]
    assert await seeded.db.fetchval("SELECT COUNT(*) FROM events WHERE event_type = 'rescore_requested'",
                                    writer=True) == 1
    r = await app_client.post("/api/v1/control/rescore", json={})
    assert r.status_code == 409 and r.json()["error"] == "rescore_running"
    gate.set()
    await asyncio.sleep(0.05)
    (tmp_root / "SCORING_KILLED").write_text("")
    assert (await app_client.post("/api/v1/control/rescore", json={})).status_code == 423
