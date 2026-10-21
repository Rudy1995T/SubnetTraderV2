"""The minimal web UI (spec 14 §8.6): static files under ``frontend/ui/`` served at ``/ui`` on this app.

``/ui`` and every non-asset ``/ui/{path}`` return ``index.html`` (``app.js`` routes on ``location.pathname``, so the
deep links ``{ui}/subnet/64`` from alerts open the right view); ``/ui/static/{file}`` serves the assets.
"""
from __future__ import annotations

import re
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse, RedirectResponse

from app.api.errors import ApiError

router = APIRouter(include_in_schema=False)
UI_DIR = Path(__file__).resolve().parents[2] / "frontend" / "ui"
_ASSET = re.compile(r"^[A-Za-z0-9_.-]+$")
_TYPES = {".js": "text/javascript", ".css": "text/css", ".svg": "image/svg+xml", ".html": "text/html"}


def configure(_get_runtime) -> None:  # static router; kept for the uniform create_app loop
    return None


def _index() -> FileResponse:
    page = UI_DIR / "index.html"
    if not page.is_file():
        raise ApiError("not_found", 404, "frontend/ui/index.html missing")
    return FileResponse(page, media_type="text/html", headers={"Cache-Control": "no-cache"})


@router.get("/")
async def root() -> RedirectResponse:
    return RedirectResponse("/ui/matrix", status_code=307)


@router.get("/ui")
async def ui_root() -> FileResponse:
    return _index()


@router.get("/ui/static/{file}")
async def ui_asset(file: str) -> FileResponse:
    p = UI_DIR / file
    if not _ASSET.match(file) or file.startswith(".") or not p.is_file():
        raise ApiError("not_found", 404, "no such asset")
    return FileResponse(p, media_type=_TYPES.get(p.suffix, "application/octet-stream"),
                        headers={"Cache-Control": "no-cache"})


@router.get("/ui/{path:path}")
async def ui_page(path: str) -> FileResponse:
    return _index()
