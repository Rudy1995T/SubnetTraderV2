"""``GET /api/v1/stream`` — SSE (spec 14 §3.5): named ``score``/``event``/``health``/``resync`` frames, ``: ping``."""
from __future__ import annotations

import asyncio
import time
from typing import AsyncIterator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

import app.config as app_config
from app.api import hub as hubmod
from app.api.common import RuntimeRef, str_set
from app.api.errors import ApiError
from app.logging.logger import logger
from app.util.time import now_ts

router = APIRouter()
_ref = RuntimeRef()
configure = _ref.configure
TYPES = frozenset({"score", "event", "health"})
HEADERS = {"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"}


async def health_frame_data() -> dict:
    from app.api.health import build_health
    rt = _ref.rt()
    h = await build_health(rt)
    latest = None
    try:
        if rt is not None and rt.db is not None:
            from app.util.time import to_iso
            latest = to_iso(await rt.db.fetchval("SELECT MAX(as_of) FROM scores WHERE spec_version = ? AND shadow = 0",
                                                 (app_config.settings.SPEC_VERSION,)))
    except Exception:  # pragma: no cover
        pass
    return {"status": h.get("status"), "degraded_reasons": h.get("degraded_reasons", []),
            "kill_switch_active": h.get("kill_switch_active"), "scoring_killed": h.get("scoring_killed"),
            "latest_as_of": latest}


async def frames(request: Request | None, hub: hubmod.StreamHub, sub: hubmod.Subscriber,
                 *, max_seconds: float | None = None) -> AsyncIterator[str]:
    """The SSE body for one subscriber; deregisters in ``finally``. ``max_seconds`` bounds tests."""
    s = app_config.settings
    t0 = time.monotonic()
    next_health = time.monotonic() + (0 if "health" in sub.types else float("inf"))
    try:
        yield ": connected\n\n"
        while True:
            if max_seconds is not None and time.monotonic() - t0 > max_seconds:
                return
            if request is not None and await request.is_disconnected():
                return
            if time.monotonic() >= next_health:
                next_health = time.monotonic() + float(s.STREAM_HEALTH_EVERY_SEC)
                yield hubmod.Frame("health", await health_frame_data()).encode()
            wait = min(float(s.STREAM_HEARTBEAT_SEC), max(0.05, next_health - time.monotonic()))
            if max_seconds is not None:
                wait = min(wait, 0.1)
            try:
                f = await asyncio.wait_for(sub.queue.get(), timeout=wait)
            except asyncio.TimeoutError:
                if max_seconds is None:
                    yield ": ping\n\n"
                continue
            if f.event == "resync":
                sub.resync_pending = False
            yield f.encode()
    finally:
        hub.unsubscribe(sub)


@router.get("/api/v1/stream")
async def stream(request: Request) -> StreamingResponse:
    s = app_config.settings
    db = _ref.db()
    types = str_set(request.query_params.get("types")) or {"score", "event"}
    if not types <= TYPES:
        raise ApiError("bad_param", 422, "types must be a comma list of score|event|health")
    hub = hubmod.get_hub(db)
    if len(hub.subs) >= int(s.STREAM_MAX_CONNECTIONS):
        raise ApiError("too_many_stream_connections", 503, f"max {s.STREAM_MAX_CONNECTIONS} subscribers")
    raw = request.headers.get("last-event-id") or request.query_params.get("last_event_id")
    last = hubmod.parse_last_id(raw)
    sub = await hub.subscribe(frozenset(types), last)
    if raw and last is None:
        sub.offer(hubmod.Frame("resync", {"reason": "bad_last_event_id"}))
    logger.debug("stream subscriber", data={"types": sorted(types), "subs": len(hub.subs), "at": now_ts()})
    return StreamingResponse(frames(request, hub, sub), media_type="text/event-stream", headers=HEADERS)
