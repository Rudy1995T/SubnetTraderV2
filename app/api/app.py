"""FastAPI app factory (spec 01 §2.9, 14 §8.2): open read routers, authed + origin-guarded write routers, SSE, the
``/ui`` pages and 19's workbench, one error shape (14 §3.5)."""
from __future__ import annotations

from typing import Callable

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

import app.config as app_config
from app.api import control as control_api
from app.api import errors
from app.api import events as events_api
from app.api import export as export_api
from app.api import health as health_api
from app.api import manual as manual_api
from app.api import quant as quant_api
from app.api import registry as registry_api
from app.api import review as review_api
from app.api import scores as scores_api
from app.api import stream as stream_api
from app.api import subnets as subnets_api
from app.api import ui as ui_api
from app.api import wb as wb_api
from app.api_auth import require_auth

_MODULES = (health_api, scores_api, subnets_api, events_api, export_api, quant_api, review_api, registry_api,
            manual_api, control_api, stream_api, ui_api, wb_api)


def configure_all(get_runtime: Callable) -> None:
    for mod in _MODULES:
        mod.configure(get_runtime)


def create_app(get_runtime: Callable | None = None) -> FastAPI:
    s = app_config.settings
    app = FastAPI(title="TaoSentinel", docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[o.strip() for o in s.CONTROL_ALLOWED_ORIGINS.split(",") if o.strip()],
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type", "X-Auth-Token", "Last-Event-ID"],
    )
    if get_runtime is None:
        from app.runtime import get_runtime_or_none
        get_runtime = get_runtime_or_none
    configure_all(get_runtime)
    authed = [Depends(require_auth), Depends(errors.origin_guard)]
    for r in (health_api.router, scores_api.router, subnets_api.router, events_api.router, export_api.router,
              quant_api.router, review_api.read_router, registry_api.read_router):
        app.include_router(r)                                                     # open read tier
    if s.STREAM_ENABLED:
        app.include_router(stream_api.router)
    for r in (manual_api.router, review_api.write_router, control_api.router, registry_api.write_router):
        app.include_router(r, dependencies=authed)                                # authed tier
    if s.WB_ENABLED:
        app.include_router(wb_api.router)        # 19: GETs open, POSTs authed + origin-guarded per route (02 §K1)
    app.include_router(ui_api.router)
    errors.install(app)
    app.add_middleware(SpecHeaderMiddleware)
    return app


class SpecHeaderMiddleware:
    """Adds ``X-TaoSentinel-Spec`` to every response (pure ASGI, so SSE streams are not buffered)."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def _send(message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers") or [])
                headers.append((b"x-taosentinel-spec", app_config.settings.SPEC_VERSION.encode()))
                message = {**message, "headers": headers}
            await send(message)
        await self.app(scope, receive, _send)
