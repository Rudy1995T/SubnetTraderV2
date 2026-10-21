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


# ── rescore (spec 14 §3.5; 12 §2.13 rule 5) ──────────────────────────────────────────────────────────────────
class RescoreBody(BaseModel):
    as_of: str | None = None              # YYYY-MM-DD → that day's official slot; null → current slot
    spec: str | None = None
    shadow: bool = False


_rescore_task: asyncio.Task | None = None


def rescore_running(rt: "Runtime") -> bool:
    job = rt.jobs.get("scoring_6h")
    return bool((_rescore_task is not None and not _rescore_task.done()) or (job is not None and job.running))


@router.post("/rescore", status_code=202)
async def rescore(body: RescoreBody | None = None) -> dict:
    global _rescore_task
    import secrets
    from datetime import UTC, date, datetime

    from app.api.errors import ApiError
    from app.scoring.engine import ScoringRefused, ScoringService, cron_hours, slot_for
    from app.scoring.spec import SPECS
    from app.storage.events import emit_event
    from app.util.time import now_ts, to_iso
    rt = _rt()
    b = body or RescoreBody()
    s = app_config.settings
    spec = b.spec or s.SCORING_SPEC_VERSION
    if spec not in SPECS:
        raise ApiError("spec_unknown", 422, f"spec {spec!r} is not in 12's SPECS")
    if b.shadow and spec == s.SPEC_VERSION:
        raise ApiError("shadow_requires_other_spec", 422, "shadow=true needs a spec other than SPEC_VERSION")
    d: date | None = None
    if b.as_of:
        try:
            d = date.fromisoformat(b.as_of)
        except ValueError:
            raise ApiError("bad_param", 422, "as_of must be YYYY-MM-DD") from None
    if control.scoring_killed():
        raise ApiError("scoring_killed", 423, "SCORING_KILLED present")
    if rescore_running(rt):
        raise ApiError("rescore_running", 409, "a scoring run is in progress")
    svc = ScoringService(rt.db)
    now = now_ts()
    slot = slot_for(now, cron_hours()) if d is None else int(datetime(d.year, d.month, d.day, tzinfo=UTC).timestamp())
    if await svc.already_scored(spec, slot):
        raise ApiError("slot_already_scored", 409, f"{to_iso(slot)} is already scored for {spec}")
    latest = await svc.latest_completed(spec)
    if latest is not None and slot < latest:
        raise ApiError("slot_regresses", 409, f"{to_iso(slot)} is older than the latest scored slot {to_iso(latest)}")
    request_id = "rq-" + datetime.now(UTC).strftime("%Y%m%d-%H%M%S") + "-" + secrets.token_hex(2)

    async def _run() -> None:
        try:
            run_id = await svc.rescore(d, spec, b.shadow)
            logger.info("rescore finished", data={"request_id": request_id, "run_id": run_id, "spec": spec})
        except ScoringRefused as exc:
            logger.warning("rescore refused", data={"request_id": request_id, "code": exc.code})
        except Exception as exc:
            logger.error("rescore failed", data={"request_id": request_id, "error": f"{type(exc).__name__}: {exc}"})

    await emit_event(rt.db, event_type="rescore_requested", occupant_key=None, netuid=None, severity="info",
                     event_at=now, observed_at=now,
                     payload={"as_of": slot, "spec_version": spec, "shadow": b.shadow, "run_id": request_id,
                              "by": "api"},
                     dedupe_key=request_id, source="api", run_id=None)
    _rescore_task = asyncio.create_task(_run())
    _bg.add(_rescore_task)
    _rescore_task.add_done_callback(_bg.discard)
    return {"accepted": True, "request_id": request_id, "as_of": to_iso(slot), "spec_version": spec,
            "shadow": b.shadow}
