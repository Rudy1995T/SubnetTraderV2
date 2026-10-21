"""``/api/v1/matrix.csv`` and ``/api/v1/export/scores.json`` (spec 14 §3.5): the feed as downloads, with ETags."""
from __future__ import annotations

import csv
import io
import json

from fastapi import APIRouter, Request
from fastapi.responses import Response, StreamingResponse

import app.config as app_config
from app import control
from app.api import queries as Q
from app.api import scores as S
from app.api.common import RuntimeRef, as_bool

router = APIRouter()
_ref = RuntimeRef()
configure = _ref.configure

CSV_HEADER = ["netuid", "occupant_key", "name", "status", "tier", "composite", "composite_raw", "quality", "velocity",
              "confidence", "P1", "P2", "P3", "P4", "P5", "P6", "red_flags", "as_of", "computed_at", "spec_version"]


def _params(request: Request) -> dict:
    q = request.query_params
    return {"spec": S.default_spec(q.get("spec")), "min_confidence": 0.0, "tier": None,
            "include_prelaunch": as_bool(q.get("include_prelaunch")),
            "include_insufficient": as_bool(q.get("include_insufficient")), "netuids": None}


async def _prepare(request: Request) -> tuple[dict, str, dict]:
    db = _ref.db()
    params = _params(request)
    meta = await Q.feed_meta(db, spec=params["spec"])
    tag = S.etag(params["spec"], meta, params | {"killed": control.scoring_killed(), "path": request.url.path})
    headers = {"ETag": tag, "Cache-Control": f"public, max-age={int(app_config.settings.EXPORT_CACHE_MAX_AGE_SEC)}"}
    return params, tag, headers


def _cell(v) -> str:
    return "" if v is None else str(v)


@router.get("/api/v1/matrix.csv")
async def matrix_csv(request: Request) -> Response:
    params, tag, headers = await _prepare(request)
    if S.not_modified(request, tag):
        return Response(status_code=304, headers=headers)
    body, _ = await S.build_feed(_ref.db(), params)
    day = (body["as_of"] or "none")[:10]
    headers["Content-Disposition"] = f'attachment; filename="taosentinel-matrix-{day}.csv"'

    def _gen():
        buf = io.StringIO()
        w = csv.writer(buf, lineterminator="\n")
        w.writerow(CSV_HEADER)
        for r in body["scores"]:
            p = r["pillars"]
            w.writerow([r["netuid"], r["occupant_key"], _cell(r["name"]), r["status"], _cell(r["tier"]),
                        _cell(r["composite"]), _cell(r["composite_raw"]), _cell(r["quality"]), _cell(r["velocity"]),
                        _cell(r["confidence"]), *[_cell(p.get(k)) for k in S.PILLAR_KEYS], ";".join(r["red_flags"]),
                        _cell(r["as_of"]), _cell(r["computed_at"]), body["spec_version"]])
        yield buf.getvalue()
    return StreamingResponse(_gen(), media_type="text/csv; charset=utf-8", headers=headers)


@router.get("/api/v1/export/scores.json")
async def scores_json(request: Request) -> Response:
    params, tag, headers = await _prepare(request)
    if S.not_modified(request, tag):
        return Response(status_code=304, headers=headers)
    body, _ = await S.build_feed(_ref.db(), params)
    day = (body["as_of"] or "none")[:10]
    headers["Content-Disposition"] = f'attachment; filename="taosentinel-scores-{day}.json"'
    return Response(json.dumps(body, ensure_ascii=False, allow_nan=False, separators=(",", ":")),
                    media_type="application/json", headers=headers)
