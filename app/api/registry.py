"""``/api/v1/registry/*`` (+ the ``/api/v1/manual/override`` alias) over 04's public API (spec 14 §3.5; 04 ships
no router, 04 §8.1). Reads are open; writes are authed + origin-guarded by ``create_app``.
"""
from __future__ import annotations

import asyncio
import dataclasses
from typing import Literal

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.api import hub
from app.api import manual_service as svc
from app.api.common import RuntimeRef, as_bool
from app.api.errors import ApiError, raise_for_result
from app.logging.logger import logger
from app.util.time import now_ts, to_iso

read_router = APIRouter(prefix="/api/v1/registry")
write_router = APIRouter()
_ref = RuntimeRef()
configure = _ref.configure
_bg: set[asyncio.Task] = set()
REGISTRY_JOBS = ("registry_occupants", "registry_identity")


def _iso_times(d: dict) -> dict:
    return {k: (to_iso(v) if isinstance(v, int) and (k.endswith("_at") or k in ("since",)) and v > 10**9 else v)
            for k, v in d.items()}


def _occ(o) -> dict:
    d = dataclasses.asdict(o) if dataclasses.is_dataclass(o) else dict(o)
    return _iso_times(d)


@read_router.get("/occupants")
async def occupants(request: Request) -> dict:
    from app.registry import active_occupants
    rows = await active_occupants(_ref.db(), as_bool(request.query_params.get("include_pre_launch")))
    return {"count": len(rows), "occupants": [_occ(o) for o in rows]}


@read_router.get("/needs-review")
async def needs_review(request: Request) -> dict:
    from app.registry import compute_needs_review
    q = request.query_params
    reason = q.get("reason") or None
    try:
        limit = max(1, min(int(q.get("limit") or 200), 1000))
    except ValueError:
        raise ApiError("bad_param", 422, "limit must be an int") from None
    items = [i.as_dict() for i in await compute_needs_review(_ref.db(), now_ts())]
    if reason:
        items = [i for i in items if i["reason"] == reason]
    return {"count": len(items[:limit]), "items": [_iso_times(i) for i in items[:limit]]}


def _ov_out(r: dict) -> dict:
    return _iso_times({k: v for k, v in r.items() if k not in ("value_hash", "suppress_value_hash")})


@read_router.get("/overrides")
async def overrides(request: Request) -> dict:
    q = request.query_params
    where, args = ["1 = 1"], []
    if q.get("netuid"):
        where.append("o.netuid = ?")
        try:
            args.append(int(q["netuid"]))
        except ValueError:
            raise ApiError("bad_param", 422, "netuid must be an int") from None
    for col in ("field", "status", "source"):
        if q.get(col):
            where.append(f"m.{col} = ?")
            args.append(q[col])
    try:
        limit = max(1, min(int(q.get("limit") or 200), 1000))
    except ValueError:
        raise ApiError("bad_param", 422, "limit must be an int") from None
    rows = await _ref.db().fetchall(
        f"SELECT m.*, o.netuid FROM manual_overrides m JOIN occupants o ON o.occupant_key = m.occupant_key "
        f"WHERE {' AND '.join(where)} ORDER BY m.id DESC LIMIT ?", (*args, limit))
    return {"count": len(rows), "overrides": [_ov_out(r) for r in rows]}


@read_router.get("/overrides/{override_id}")
async def override_get(override_id: int) -> dict:
    r = await _ref.db().fetchone("SELECT m.*, o.netuid FROM manual_overrides m JOIN occupants o "
                                 "ON o.occupant_key = m.occupant_key WHERE m.id = ?", (override_id,))
    if r is None:
        raise ApiError("not_found", 404, f"no override {override_id}")
    return _ov_out(r)


@read_router.get("/{netuid}")
async def registry_netuid(netuid: int) -> dict:
    from app.registry import current_occupant, resolved
    from app.registry.merge import RESOLVED_FIELDS
    db = _ref.db()
    o = await current_occupant(db, netuid)
    if o is None:
        raise ApiError("unknown_netuid", 404, f"SN{netuid} has no current occupant")
    res = {}
    for f in RESOLVED_FIELDS:
        r = await resolved(db, o.occupant_key, f)
        res[f] = None if r is None else {"value": r.value, "source": r.source, "status": r.status,
                                         "override_id": r.override_id}
    ovs = await db.fetchall("SELECT * FROM manual_overrides WHERE occupant_key = ? AND status = 'active' "
                            "ORDER BY id", (o.occupant_key,))
    return {"occupant": _occ(o), "resolved": res, "overrides": [_ov_out(r) for r in ovs]}


# ── writes ─────────────────────────────────────────────────────────────────────────────────────────────────────
class OverrideBody(BaseModel):
    netuid: int | None = None
    occupant_key: str | None = None
    field: str
    mode: Literal["fallback", "force", "suppress"]
    value: str | None = None
    suppress_source: str | None = None
    suppress_value: str | None = None
    reason: str | None = Field(default=None, max_length=500)
    evidence_url: str | None = Field(default=None, max_length=500)
    resolves_event_id: int | None = None


async def _add_override(b: OverrideBody) -> JSONResponse:
    from app.registry import add_override
    db = _ref.db()
    occ = b.occupant_key
    if occ is None:
        if b.netuid is None:
            raise ApiError("invalid_override", 422, "netuid or occupant_key is required")
        occ = await db.fetchval("SELECT occupant_key FROM subnets WHERE netuid = ?", (b.netuid,))
        if occ is None:
            raise ApiError("unknown_netuid", 404, f"SN{b.netuid} has no current occupant")
    before = await db.fetchval("SELECT COUNT(*) FROM manual_overrides", writer=True)
    try:
        oid = await add_override(db, occupant_key=occ, field=b.field, mode=b.mode, value=b.value,
                                 suppress_source=b.suppress_source, suppress_value=b.suppress_value, reason=b.reason,
                                 evidence_url=b.evidence_url, resolves_event_id=b.resolves_event_id, created_by="api")
    except ValueError as exc:
        msg = str(exc)
        if msg.startswith("unknown occupant"):
            raise ApiError("unknown_occupant", 404, msg) from None
        raise ApiError("invalid_override", 422, msg) from None
    after = await db.fetchval("SELECT COUNT(*) FROM manual_overrides", writer=True)
    if after == before:
        raise ApiError("override_exists", 409, "an identical override already exists", id=oid)
    hub.nudge()
    return JSONResponse({"ok": True, "id": oid, "occupant_key": occ}, status_code=201)


@write_router.post("/api/v1/registry/overrides")
async def override_add(b: OverrideBody) -> JSONResponse:
    return await _add_override(b)


@write_router.post("/api/v1/manual/override")
async def override_add_alias(b: OverrideBody) -> JSONResponse:
    return await _add_override(b)


class OverrideStatus(BaseModel):
    status: Literal["active", "rejected", "retired"]


@write_router.post("/api/v1/registry/overrides/{override_id}/status")
async def override_status(override_id: int, b: OverrideStatus) -> dict:
    from app.registry import set_override_status
    try:
        await set_override_status(_ref.db(), override_id, b.status, by="api")
    except ValueError as exc:
        if "unknown override" in str(exc):
            raise ApiError("not_found", 404, str(exc)) from None
        raise ApiError("invalid_override", 422, str(exc)) from None
    hub.nudge()
    return {"ok": True, "id": override_id, "status": b.status}


class Refresh(BaseModel):
    jobs: list[str] = Field(default_factory=lambda: list(REGISTRY_JOBS))


@write_router.post("/api/v1/registry/refresh")
async def refresh(b: Refresh | None = None) -> JSONResponse:
    rt = _ref.rt_or_503()
    jobs = (b or Refresh()).jobs
    bad = [j for j in jobs if j not in REGISTRY_JOBS]
    if bad:
        raise ApiError("unknown_job", 404, f"not a registry job: {bad}")
    out: dict[str, list[str]] = {"started": [], "in_flight": [], "paused": [], "unavailable": []}
    for jid in jobs:
        job = rt.jobs.get(jid)
        if job is None or not job.enabled():
            out["unavailable"].append(jid)
        elif job.running:
            out["in_flight"].append(jid)
        elif job.paused_reason():
            out["paused"].append(jid)
        else:
            t = asyncio.create_task(job.run(trigger="manual"))
            _bg.add(t)
            t.add_done_callback(_bg.discard)
            out["started"].append(jid)
    logger.info("registry refresh requested", data=out)
    return JSONResponse(out, status_code=202)


class Ack(BaseModel):
    note: str = Field(min_length=1, max_length=2000)
    evidence_url: str | None = Field(default=None, max_length=500)


@write_router.post("/api/v1/registry/reoccupied/{event_id}/ack")
async def reoccupied_ack(event_id: int, b: Ack) -> JSONResponse:
    import json
    db = _ref.db()
    e = await db.fetchone("SELECT event_type, payload FROM events WHERE id = ?", (event_id,))
    if e is None:
        raise ApiError("unknown_event", 404, f"no event {event_id}")
    if e["event_type"] != "netuid_reoccupied":
        raise ApiError("not_reoccupied_event", 422, f"event {event_id} is a {e['event_type']}")
    p = json.loads(e["payload"] or "{}")
    occ = p.get("new_occupant_key") or p.get("new_key")
    res = await svc.add_note(db, occupant_key=occ, text=b.note, evidence_url=b.evidence_url, by="api",
                             ref_event_id=event_id, require_evidence=False)
    return JSONResponse(raise_for_result(res), status_code=201)
