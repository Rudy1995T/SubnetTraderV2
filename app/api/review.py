"""``GET /api/v1/review`` (open) and ``POST /api/v1/review/{id}/resolve`` (authed) over 11's review queue (§3.5)."""
from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from app.api import hub
from app.api.common import RuntimeRef
from app.api.errors import ApiError, raise_for_result
from app.integrity import review as review11
from app.util.time import parse_ts, to_iso

read_router = APIRouter()
write_router = APIRouter()
_ref = RuntimeRef()
configure = _ref.configure


def item_out(r: dict) -> dict:
    return {"id": r["id"], "occupant_key": r.get("occupant_key"), "netuid": r.get("netuid"), "kind": r["kind"],
            "priority": r.get("priority"), "summary": r.get("summary"), "evidence_url": r.get("evidence_url"),
            "suggested_flag": r.get("suggested_flag"), "event_type": r.get("event_type"),
            "created_at": to_iso(r.get("created_at")), "status": r.get("status"), "payload": r.get("payload") or {}}


async def list_items(db, *, kind: str | None, netuid: int | None, limit: int) -> list[dict]:
    occ = None
    if netuid is not None:
        occ = await db.fetchval("SELECT occupant_key FROM subnets WHERE netuid = ?", (netuid,))
        if occ is None:
            return []
    rows = await review11.list_open(db, kind=kind, occupant_key=occ, limit=limit)
    keys = {r["occupant_key"] for r in rows if r.get("occupant_key")}
    nets: dict[str, int] = {}
    if keys:
        ph = ",".join("?" * len(keys))
        for r in await db.fetchall(f"SELECT occupant_key, netuid FROM occupants WHERE occupant_key IN ({ph})",
                                   tuple(keys)):
            nets[r["occupant_key"]] = r["netuid"]
    for r in rows:
        r["netuid"] = nets.get(r.get("occupant_key"))
    return rows


@read_router.get("/api/v1/review")
async def review_list(request: Request) -> dict:
    q = request.query_params
    try:
        limit = max(1, min(int(q.get("limit") or 100), 200))
        netuid = int(q["netuid"]) if q.get("netuid") else None
    except ValueError:
        raise ApiError("bad_param", 422, "limit/netuid must be ints") from None
    rows = await list_items(_ref.db(), kind=q.get("kind") or None, netuid=netuid, limit=limit)
    return {"count": len(rows), "items": [item_out(r) for r in rows]}


class Resolve(BaseModel):
    action: Literal["confirm_flag", "whitelist", "key_compromise", "dismiss", "done"]
    note: str | None = Field(default=None, max_length=1000)
    evidence_url: str | None = Field(default=None, max_length=500)
    whitelist_reason: str | None = None
    window_start: str | None = None
    window_end: str | None = None


@write_router.post("/api/v1/review/{review_id}/resolve")
async def review_resolve(review_id: int, b: Resolve) -> dict:
    window = None
    if b.window_start and b.window_end:
        try:
            window = (parse_ts(b.window_start), parse_ts(b.window_end))
        except (TypeError, ValueError):
            raise ApiError("bad_param", 422, "window_start/window_end must be ISO-8601") from None
    res = await review11.resolve_review(_ref.db(), review_id, b.action, by="api", note=b.note,
                                        evidence_url=b.evidence_url, whitelist_reason=b.whitelist_reason,
                                        window=window)
    hub.nudge()
    return raise_for_result(res)
