"""``/api/v1/scores/latest`` (the trading feed, 02 §H1) and ``/api/v1/scores/history`` (spec 14 §3.5).

Also holds the row shapes shared with ``/subnets``, the exports and the SSE ``score`` frames.
"""
from __future__ import annotations

import hashlib
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

import app.config as app_config
from app import control
from app.api import queries as Q
from app.api.common import RuntimeRef, as_bool, int_list, r2, r3
from app.api.errors import ApiError
from app.util.time import now_ts, to_iso

router = APIRouter()
_ref = RuntimeRef()
configure = _ref.configure
TIERS = ("A", "B", "C", "D", "F")
PILLAR_KEYS = ("P1", "P2", "P3", "P4", "P5", "P6")


def pillars(p: dict | None) -> dict[str, float | None]:
    p = p or {}
    return {k: r3(p.get(k)) for k in PILLAR_KEYS}


def feed_row(r: dict) -> dict:
    return {"netuid": r["netuid"], "occupant_key": r["occupant_key"], "name": r.get("name"), "status": r["status"],
            "quality": r2(r["quality"]), "velocity": r2(r["velocity"]), "composite": r2(r["composite_smoothed"]),
            "composite_raw": r2(r["composite_raw"]), "confidence": r2(r["confidence"]), "tier": r["tier"],
            "red_flags": list(r["red_flags"]), "pillars": pillars(r["pillar_scores"]),
            "as_of": to_iso(r["as_of"]), "computed_at": to_iso(r["computed_at"])}


def history_point(r: dict) -> dict:
    return {"as_of": to_iso(r["as_of"]), "computed_at": to_iso(r["computed_at"]), "run_kind": r["run_kind"],
            "composite": r2(r["composite_smoothed"]), "composite_raw": r2(r["composite_raw"]),
            "quality": r2(r["quality"]), "velocity": r2(r["velocity"]), "confidence": r2(r["confidence"]),
            "tier": r["tier"], "red_flags": list(r["red_flags"]), "pillars": pillars(r["pillar_scores"]),
            "regime": r["regime"]}


def default_spec(spec: str | None) -> str:
    return spec or app_config.settings.SCORING_SPEC_VERSION


def is_stale(max_as_of: int | None, now: int) -> bool:
    ttl = int(app_config.settings.SCORE_FEED_TTL_SEC)
    return max_as_of is None or now - int(max_as_of) > ttl or control.scoring_killed()


def etag(spec: str, meta: dict, params: dict[str, Any]) -> str:
    ph = hashlib.sha256(repr(sorted(params.items())).encode()).hexdigest()[:12]
    raw = f"{spec}|{meta.get('max_as_of')}|{meta.get('max_id')}|{ph}"
    return 'W/"' + hashlib.sha256(raw.encode()).hexdigest()[:16] + '"'


def not_modified(request: Request, tag: str) -> bool:
    inm = request.headers.get("if-none-match")
    return bool(inm) and tag in [x.strip() for x in inm.split(",")]


def feed_params(request: Request) -> dict[str, Any]:
    q = request.query_params
    tier = q.get("tier") or None
    if tier is not None and tier not in TIERS:
        raise ApiError("bad_param", 422, "tier must be one of A|B|C|D|F")
    try:
        minc = float(q.get("min_confidence") or 0.0)
    except ValueError:
        raise ApiError("bad_param", 422, "min_confidence must be a float") from None
    if not 0.0 <= minc <= 1.0:
        raise ApiError("bad_param", 422, "min_confidence must be within 0..1")
    return {"spec": default_spec(q.get("spec")), "min_confidence": minc, "tier": tier,
            "include_prelaunch": as_bool(q.get("include_prelaunch")),
            "include_insufficient": as_bool(q.get("include_insufficient")), "netuids": int_list(q.get("netuids"))}


async def build_feed(db, params: dict[str, Any], now: int | None = None) -> tuple[dict, dict]:
    """(body, meta) of ``/scores/latest`` for ``params`` — shared with ``/export/scores.json`` and Telegram."""
    now = int(now if now is not None else now_ts())
    spec = params["spec"]
    rows = await Q.latest_scores(db, **params)
    meta = await Q.feed_meta(db, spec=spec)
    newest = max(rows, key=lambda r: (r["as_of"], r["computed_at"])) if rows else None
    body = {
        "as_of": to_iso(newest["as_of"]) if newest else None,
        "computed_at": to_iso(newest["computed_at"]) if newest else None,
        "generated_at": to_iso(now),
        "run_kind": newest["run_kind"] if newest else None,
        "spec_version": spec,
        "ttl_seconds": int(app_config.settings.SCORE_FEED_TTL_SEC),
        "stale": True if not rows else is_stale(newest["as_of"], now),
        "regime": newest["regime"] if newest else None,
        "count": len(rows),
        "scores": [feed_row(r) for r in rows],
    }
    return body, meta


@router.get("/api/v1/scores/latest")
async def scores_latest(request: Request) -> Response:
    db = _ref.db()
    params = feed_params(request)
    meta = await Q.feed_meta(db, spec=params["spec"])
    # `stale` flips with the clock alone (TTL passes, no new row), so it is part of the tag: otherwise a consumer
    # holding the last ETag would keep getting 304 for a body whose `stale` is now true (15 review, 2026-10-18).
    tag = etag(params["spec"], meta, {k: v for k, v in params.items()}
               | {"killed": control.scoring_killed(), "stale": is_stale(meta["max_as_of"], now_ts())})
    headers = {"Cache-Control": "no-store", "ETag": tag}
    if not_modified(request, tag):
        return Response(status_code=304, headers=headers)
    body, _ = await build_feed(db, params)
    return JSONResponse(body, headers=headers)


@router.get("/api/v1/scores/history")
async def scores_history(request: Request) -> dict:
    db = _ref.db()
    q = request.query_params
    ident = q.get("occupant")
    if not ident:
        raise ApiError("bad_param", 422, "occupant is required (occupant_key or netuid)")
    try:
        days = int(q.get("days") or 90)
    except ValueError:
        raise ApiError("bad_param", 422, "days must be an int") from None
    if not 1 <= days <= 730:
        raise ApiError("bad_param", 422, "days must be within 1..730")
    spec = default_spec(q.get("spec"))
    res = await Q.resolve_occupant(db, ident)
    if res is None:
        raise ApiError("unknown_occupant", 404, f"no occupant for {ident!r}")
    occ, netuid = res
    rows = await Q.score_history(db, occupant_key=occ, spec=spec, days=days,
                                 official_only=as_bool(q.get("official_only"), True), now=now_ts())
    return {"occupant_key": occ, "netuid": netuid, "spec_version": spec, "days": days,
            "points": [history_point(r) for r in rows]}
