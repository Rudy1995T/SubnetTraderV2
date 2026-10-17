"""``X-Auth-Token`` dependency for mutating routes (port of the old ``api_auth.py``; empty token = auth off).

Reads the live ``app.config.settings`` attribute on every request, so a rotated token applies after a reload.
"""
from __future__ import annotations

import hmac

from fastapi import Header, HTTPException

import app.config as app_config


async def require_auth(x_auth_token: str | None = Header(default=None)) -> None:
    expected = app_config.settings.API_AUTH_TOKEN
    if not expected:
        return
    if x_auth_token is None or not hmac.compare_digest(x_auth_token.encode("utf-8"), expected.encode("utf-8")):
        raise HTTPException(status_code=401, detail="Missing or invalid auth token")
