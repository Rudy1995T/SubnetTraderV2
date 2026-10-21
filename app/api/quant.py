"""``/api/v1/quant/results[/{file}]`` (spec 14 §3.5): lists 13's (and 19's) result JSONs; serves one by name."""
from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import Response

import app.config as app_config
from app.api.errors import ApiError
from app.util.time import to_iso

router = APIRouter()


def configure(_get_runtime) -> None:  # file-only router; kept for the uniform create_app loop
    return None


KINDS = (("factor_", "factor"), ("walk_forward_", "walk_forward"), ("survival_", "survival"),
         ("rank_stability_", "rank_stability"), ("gate_verdict_", "verdict"), ("wb_study_", "wb_study"))
_SKIP_PREFIX = ("ic_series_",)
_NAME = re.compile(r"^[A-Za-z0-9_.-]+\.json$")
_STAMP = re.compile(r"_(\d{8})_(\d{6})\.json$")
_DAY = re.compile(r"_(\d{8})\.json$")
SUMMARY_KEYS = ("scope", "timestamp", "factor", "gate_pass", "pending")
MAX_BYTES = 5 * 2**20


def results_dir() -> Path:
    s = app_config.settings
    return s.path(s.QUANT_RESULTS_DIR)


def kind_of(name: str) -> str | None:
    if name == "trials.json" or name.startswith(_SKIP_PREFIX) or not name.endswith(".json"):
        return None
    for prefix, kind in KINDS:
        if name.startswith(prefix):
            return kind
    return None


def run_ts(name: str) -> str | None:
    m = _STAMP.search(name)
    if m:
        return to_iso(datetime.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S").replace(tzinfo=UTC).timestamp())
    m = _DAY.search(name)
    if m:
        return to_iso(datetime.strptime(m.group(1), "%Y%m%d").replace(tzinfo=UTC).timestamp())
    return None


@router.get("/api/v1/quant/results")
async def results() -> dict:
    d = results_dir()
    files, errors = [], []
    if d.is_dir():
        for p in sorted(d.iterdir(), key=lambda x: x.name, reverse=True):
            kind = kind_of(p.name)
            if kind is None or not p.is_file():
                continue
            entry = {"file": p.name, "kind": kind, "run_ts": run_ts(p.name), "size_bytes": p.stat().st_size,
                     "summary": {}}
            if p.stat().st_size <= MAX_BYTES:
                try:
                    obj = json.loads(p.read_text())
                    if isinstance(obj, dict):
                        entry["summary"] = {k: obj[k] for k in SUMMARY_KEYS if k in obj}
                except (OSError, ValueError) as exc:
                    errors.append(f"{p.name}: {exc}"[:200])
                    continue
            files.append(entry)
    return {"dir": app_config.settings.QUANT_RESULTS_DIR, "files": files, "errors": errors}


@router.get("/api/v1/quant/results/{file}")
async def result_file(file: str) -> Response:
    if not _NAME.match(file):
        raise ApiError("not_found", 404, "no such result file")
    d = results_dir().resolve()
    p = (d / file).resolve()
    if not p.is_relative_to(d) or not p.is_file():
        raise ApiError("not_found", 404, "no such result file")
    if p.stat().st_size > MAX_BYTES:
        raise ApiError("too_large", 413, f"{file} exceeds 5 MB")
    return Response(p.read_bytes(), media_type="application/json")
