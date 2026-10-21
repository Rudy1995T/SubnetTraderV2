"""Error shape, ``ActionResult`` → HTTP mapping and the origin guard (spec 14 §3.5–§3.6).

Every 4xx/5xx body is ``{"error": <code>, "detail": <str|object>}``. Phase-0 routes raise
``HTTPException(status, detail="<code>")`` and 19's raise ``detail={"error": …}``; ``install`` maps both to that shape
and keeps ``detail`` as given, so clients reading ``detail`` keep working.
"""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

import app.config as app_config
from app.logging.logger import logger


class ApiError(Exception):
    def __init__(self, code: str, http_status: int = 400, detail: Any = None, **extra: Any) -> None:
        super().__init__(code)
        self.code = code
        self.status = http_status
        self.detail = detail if detail is not None else code
        self.extra = extra


# 11's ActionResult.error codes → (HTTP status, body error code) (§3.5 table)
_ACTION_ERRORS: dict[str, tuple[int, str]] = {
    "no_current_occupant": (404, "no_current_occupant"),
    "not_found": (404, "not_found"),
    "evidence_required": (422, "evidence_required"),
    "invalid_url": (422, "invalid_url"),
    "invalid_kind": (422, "invalid_kind"),
    "window_too_long": (422, "window_too_long"),
    "reason_required": (422, "reason_required"),
    "two_source_rule": (409, "needs_second_source"),
    "duplicate_member": (409, "duplicate_member"),
    "invalid_transition": (409, "invalid_transition"),
    "flag_not_human_confirmable": (422, "flag_not_human_confirmable"),   # 14 manual_service (02 §D5)
    "flag_not_active": (409, "flag_not_active"),
}


def raise_for_result(res: Any) -> dict:
    """Return ``res.to_dict()`` when ok, else raise the mapped ``ApiError`` (unknown codes → 400, logged)."""
    d = res.to_dict() if hasattr(res, "to_dict") else dict(res)
    if d.get("ok"):
        return d
    code = d.get("error") or "failed"
    status, body_code = _ACTION_ERRORS.get(code, (400, code))
    if code not in _ACTION_ERRORS:
        logger.warning("unmapped ActionResult error", data={"error": code})
    detail = d.get("warning") or body_code
    extra = {k: d[k] for k in ("id", "occupant_key", "status") if d.get(k) is not None}
    raise ApiError(body_code, status, detail, **extra)


def _origin_of(request: Request) -> str:
    host = request.headers.get("host") or (request.url.netloc if request.url else "")
    return f"{request.url.scheme}://{host}".lower()


def allowed_origin(origin: str, request: Request) -> bool:
    o = origin.strip().rstrip("/").lower()
    allowed = {x.strip().rstrip("/").lower()
               for x in app_config.settings.CONTROL_ALLOWED_ORIGINS.split(",") if x.strip()}
    return o in allowed or o == _origin_of(request)


async def origin_guard(request: Request) -> None:
    """403 ``origin_not_allowed`` for a request whose ``Origin`` is neither allowed nor its own (§3.6)."""
    origin = request.headers.get("origin")
    if origin is None:
        return
    if not allowed_origin(origin, request):
        raise ApiError("origin_not_allowed", 403, f"origin {origin} is not in CONTROL_ALLOWED_ORIGINS")


def _body(code: str, detail: Any, extra: dict | None = None) -> dict:
    out = {"error": code, "detail": detail}
    if extra:
        out.update(extra)
    return out


def install(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api_error(_request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(_body(exc.code, exc.detail, exc.extra), status_code=exc.status)

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
        d = exc.detail
        if isinstance(d, dict):
            code = str(d.get("error") or "error")
        elif isinstance(d, str) and d and " " not in d:
            code = d
        else:
            code = {401: "unauthorized", 403: "forbidden", 404: "not_found", 405: "method_not_allowed",
                    503: "unavailable"}.get(exc.status_code, "error")
        return JSONResponse(_body(code, d), status_code=exc.status_code, headers=getattr(exc, "headers", None))

    @app.exception_handler(RequestValidationError)
    async def _invalid(_request: Request, exc: RequestValidationError) -> JSONResponse:
        errs = [{k: v for k, v in e.items() if k in ("loc", "msg", "type")} for e in exc.errors()]
        return JSONResponse(_body("invalid_body", errs), status_code=422)

    @app.exception_handler(Exception)
    async def _internal(request: Request, exc: Exception) -> JSONResponse:
        logger.error("unhandled API error", data={"route": request.url.path,
                                                   "error": f"{type(exc).__name__}: {exc}"[:500]})
        return JSONResponse({"error": "internal"}, status_code=500)
