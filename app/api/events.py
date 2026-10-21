"""``/api/v1/events`` (spec 14 §3.5): filters + ``before_id`` keyset pagination."""
from __future__ import annotations

from fastapi import APIRouter, Request

from app.api import queries as Q
from app.api.common import RuntimeRef, parse_since, str_set
from app.api.errors import ApiError
from app.util.time import to_iso

router = APIRouter()
_ref = RuntimeRef()
configure = _ref.configure
SEVERITIES = {"info", "warning", "critical"}


def event_out(e: dict) -> dict:
    return {"id": e["id"], "netuid": e["netuid"], "occupant_key": e["occupant_key"], "name": e.get("name"),
            "event_type": e["event_type"], "severity": e["severity"], "source": e["source"],
            "event_at": to_iso(e["event_at"]), "observed_at": to_iso(e["observed_at"]), "payload": e["payload"],
            "alerted_at": to_iso(e["alerted_at"])}


def _int(v: str | None, name: str) -> int | None:
    if v in (None, ""):
        return None
    try:
        return int(v)
    except ValueError:
        raise ApiError("bad_param", 422, f"{name} must be an int") from None


@router.get("/api/v1/events")
async def events(request: Request) -> dict:
    db = _ref.db()
    q = request.query_params
    since = parse_since(q.get("since"))
    until = parse_since(q.get("until"), code="bad_until") if q.get("until") else None
    sev = str_set(q.get("severity"))
    if sev and not sev <= SEVERITIES:
        raise ApiError("bad_param", 422, "severity must be info|warning|critical")
    limit = _int(q.get("limit"), "limit") or 200
    limit = max(1, min(limit, 1000))
    rows = await Q.events_query(db, since=since, until=until, severities=sev, types=str_set(q.get("type")),
                                netuid=_int(q.get("netuid"), "netuid"), occupant_key=q.get("occupant") or None,
                                limit=limit, before_id=_int(q.get("before_id"), "before_id"))
    nxt = rows[-1]["id"] if len(rows) == limit else None
    return {"count": len(rows), "next_before_id": nxt, "events": [event_out(e) for e in rows]}
