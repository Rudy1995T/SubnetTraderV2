"""FastAPI app factory (spec 01 §2.9): read routers open, the control router behind ``require_auth``."""
from __future__ import annotations

from typing import Callable

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

import app.config as app_config
from app.api import control as control_api
from app.api import health as health_api
from app.api import wb as wb_api
from app.api_auth import require_auth


def configure_all(get_runtime: Callable) -> None:
    health_api.configure(get_runtime)
    control_api.configure(get_runtime)
    wb_api.configure(get_runtime)


def create_app(get_runtime: Callable | None = None) -> FastAPI:
    s = app_config.settings
    app = FastAPI(title="TaoSentinel", docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[o.strip() for o in s.CONTROL_ALLOWED_ORIGINS.split(",") if o.strip()],
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type", "X-Auth-Token"],
    )
    if get_runtime is None:
        from app.runtime import get_runtime_or_none
        get_runtime = get_runtime_or_none
    configure_all(get_runtime)
    app.include_router(health_api.router)
    app.include_router(control_api.router, dependencies=[Depends(require_auth)])
    if s.WB_ENABLED:
        app.include_router(wb_api.router)        # 19: GETs open, POSTs authed per route (02 §K1)
    return app
