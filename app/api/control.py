"""Phase-0 control routes (authed; spec 01 §2.9). Pauses go only to ``control_state`` (02 §B12)."""
from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Callable

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import app.config as app_config
from app import control
from app.logging.logger import logger

if TYPE_CHECKING:  # pragma: no cover
    from app.runtime import Runtime

router = APIRouter(prefix="/api/v1/control")
_get_runtime: Callable[[], "Runtime | None"] | None = None
_bg: set[asyncio.Task] = set()


def configure(get_runtime: Callable[[], "Runtime | None"]) -> None:
    global _get_runtime
    _get_runtime = get_runtime


def _rt() -> "Runtime":
    rt = _get_runtime() if _get_runtime else None
    if rt is None or rt.db is None:
        raise HTTPException(503, "runtime not ready")
    return rt


class PauseBody(BaseModel):
    reason: str | None = None
    hours: float | None = None


@router.post("/pause/{target}")
async def pause(target: str, body: PauseBody | None = None) -> dict:
    rt = _rt()
    body = body or PauseBody()
    try:
        return await control.pause(target, by="api", reason=body.reason, hours=body.hours, db=rt.db)
    except control.NotPausable:
        raise HTTPException(409, detail="not_pausable") from None
    except control.UnknownTarget:
        raise HTTPException(404, detail="unknown_target") from None


@router.post("/resume/{target}")
async def resume(target: str) -> dict:
    rt = _rt()
    try:
        return await control.resume(target, by="api", db=rt.db)
    except control.UnknownTarget:
        raise HTTPException(404, detail="unknown_target") from None


@router.post("/run/{job_id}")
async def run(job_id: str) -> dict:
    rt = _rt()
    job = rt.jobs.get(job_id)
    if job is None:
        raise HTTPException(404, detail="unknown_or_unavailable")
    if job.running:
        raise HTTPException(409, detail="in_flight")
    if not job.enabled():
        raise HTTPException(409, detail="disabled")
    reason = job.paused_reason()
    if reason:
        raise HTTPException(409, detail=f"paused:{reason}")
    t = asyncio.create_task(job.run(trigger="manual"))
    _bg.add(t)
    t.add_done_callback(_bg.discard)
    logger.info("manual run requested", data={"job": job_id})
    return {"started": True, "job": job_id}


@router.post("/reload-config")
async def reload_config() -> dict:
    from app.config import is_secret, reload_settings, restart_required
    _, changed = reload_settings()
    restart_keys = sorted(k for k in changed if restart_required(k))
    return {"changed": sorted(k for k in changed if not is_secret(k)),
            "changed_secrets": len([k for k in changed if is_secret(k)]),
            "restart_required": bool(restart_keys), "restart_keys": restart_keys,
            "spec_version": app_config.settings.SPEC_VERSION}
