"""Workbench routes (spec 19 §2.6–§2.8, 02 §K1): ``/wb`` page and ``/api/v1/wb/*`` on 14's app. GETs are open like
``/health``; POSTs sit behind 14's ``X-Auth-Token`` rule."""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

import app.config as app_config
from app import control
from app.api.errors import origin_guard
from app.api_auth import require_auth
from app.logging.logger import logger
from app.util.time import now_ts
from app.wb import screener as scr
from app.wb import vision

if TYPE_CHECKING:  # pragma: no cover
    from app.runtime import Runtime

router = APIRouter()
_get_runtime: Callable[[], "Runtime | None"] | None = None
REPO_ROOT = Path(__file__).resolve().parents[2]
PAGE = REPO_ROOT / "frontend" / "wb" / "index.html"
client_factory: Callable[[str, float], Any] = vision.make_client     # tests replace it


def configure(get_runtime: Callable[[], "Runtime | None"]) -> None:
    global _get_runtime
    _get_runtime = get_runtime


def _rt() -> "Runtime":
    rt = _get_runtime() if _get_runtime else None
    if rt is None or rt.db is None:
        raise HTTPException(503, "runtime not ready")
    return rt


# ── page + reads ───────────────────────────────────────────────────────────────────────────────────────────────
@router.get("/wb", include_in_schema=False)
async def page() -> FileResponse:
    if not PAGE.is_file():
        raise HTTPException(404, "frontend/wb/index.html missing")
    return FileResponse(PAGE, media_type="text/html")


@router.get("/api/v1/wb/screener")
async def get_screener() -> dict:
    return await scr.screener(_rt().db)


@router.get("/api/v1/wb/factors")
async def get_factors() -> dict:
    return {"factors": scr.FACTORS}


@router.get("/api/v1/wb/subnet/{netuid}/chart")
async def get_chart(netuid: int, period: str = Query("1h", pattern="^(5m|1h|1d)$"),
                    bars: int = Query(500, ge=10, le=5000)) -> dict:
    out = await scr.chart(_rt().db, netuid, period, bars)
    if out is None:
        raise HTTPException(404, "unknown_netuid")
    return out


@router.get("/api/v1/wb/notes")
async def get_notes(netuid: int | None = None, limit: int = Query(50, ge=1, le=500)) -> dict:
    db = _rt().db
    if netuid is None:
        rows = await db.fetchall("SELECT * FROM manual_notes WHERE kind = 'chart_read' ORDER BY created_at DESC "
                                 "LIMIT ?", (limit,))
    else:
        rows = await db.fetchall(
            "SELECT n.* FROM manual_notes n JOIN occupants o USING (occupant_key) WHERE o.netuid = ? AND "
            "n.kind IN ('chart_read','note','seed_edge','legacy') ORDER BY n.created_at DESC LIMIT ?", (netuid, limit))
    for r in rows:
        if r.get("payload_json"):
            r["payload"] = json.loads(r.pop("payload_json"))
    return {"notes": rows}


def _study_summary(p: Path) -> dict | None:
    try:
        d = json.loads(p.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(d, dict) or "factors" not in d:
        return None
    return {"created_at": d.get("created_at"), "horizons": d.get("horizons"),
            "ic": {f: {str(h): (v.get("by_h", {}).get(str(h)) or {}).get("ic_mean") for h in d.get("horizons", [])}
                   for f, v in d["factors"].items()}}


@router.get("/api/v1/wb/results")
async def get_results() -> dict:
    s = app_config.settings
    base = s.path(s.DATA_DIR) / "quant"
    out: dict[str, Any] = {"results": [], "legacy": None}
    rdir = base / "results"
    if rdir.is_dir():
        for p in sorted(rdir.glob("*.json"), key=lambda q: q.stat().st_mtime, reverse=True)[:200]:
            item = {"file": p.name, "size": p.stat().st_size, "mtime": int(p.stat().st_mtime)}
            if p.name.startswith("wb_study_"):
                item["summary"] = _study_summary(p)
            out["results"].append(item)
    idx = base / "legacy" / "index.json"
    if idx.is_file():
        try:
            out["legacy"] = json.loads(idx.read_text())
        except ValueError:
            out["legacy"] = {"error": "unreadable index"}
    return out


# ── presets ────────────────────────────────────────────────────────────────────────────────────────────────────
class PresetBody(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    weights: dict[str, float]


class PresetName(BaseModel):
    name: str


@router.get("/api/v1/wb/presets")
async def list_presets() -> dict:
    rows = await _rt().db.fetchall("SELECT id, name, weights_json, created_at, updated_at FROM wb_presets ORDER BY name")
    return {"presets": [{**{k: v for k, v in r.items() if k != "weights_json"},
                         "weights": json.loads(r["weights_json"])} for r in rows]}


@router.post("/api/v1/wb/presets", dependencies=[Depends(require_auth), Depends(origin_guard)])
async def save_preset(body: PresetBody) -> dict:
    known = {f["key"] for f in scr.FACTORS}
    bad = sorted(set(body.weights) - known)
    if bad:
        raise HTTPException(422, detail={"unknown_factors": bad})
    if any(not -1.0 <= w <= 1.0 for w in body.weights.values()):
        raise HTTPException(422, detail="weights must be within [-1, 1]")
    now = now_ts()
    await _rt().db.execute(
        "INSERT INTO wb_presets (name, weights_json, created_at, updated_at) VALUES (?,?,?,?) ON CONFLICT (name) DO "
        "UPDATE SET weights_json = excluded.weights_json, updated_at = excluded.updated_at",
        (body.name, json.dumps(body.weights, sort_keys=True), now, now))
    return {"ok": True, "name": body.name}


@router.post("/api/v1/wb/presets/delete", dependencies=[Depends(require_auth), Depends(origin_guard)])
async def delete_preset(body: PresetName) -> dict:
    res = await _rt().db.execute("DELETE FROM wb_presets WHERE name = ?", (body.name,))
    if res.rowcount == 0:
        raise HTTPException(404, "unknown_preset")
    return {"ok": True}


# ── chart upload + vision read ─────────────────────────────────────────────────────────────────────────────────
async def _read_body(request: Request, cap: int) -> bytes:
    cl = request.headers.get("content-length")
    if cl and cl.isdigit() and int(cl) > cap:
        raise HTTPException(413, "image_too_large")
    buf = bytearray()
    async for chunk in request.stream():
        buf += chunk
        if len(buf) > cap:
            raise HTTPException(413, "image_too_large")
    if not buf:
        raise HTTPException(400, "empty_body")
    return bytes(buf)


@router.post("/api/v1/wb/chart-read", dependencies=[Depends(require_auth), Depends(origin_guard)])
async def chart_read(request: Request, netuid: int | None = None) -> dict:
    rt = _rt()
    db = rt.db
    s = app_config.settings
    media = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
    if media not in vision.MEDIA_TYPES:
        raise HTTPException(415, "unsupported_media_type (image/png, image/jpeg, image/webp)")
    data = await _read_body(request, s.WB_VISION_MAX_IMAGE_MB * 1024 * 1024)
    occ, name = None, None
    if netuid is not None:
        sub = await db.fetchone("SELECT occupant_key, name FROM subnets WHERE netuid = ?", (netuid,))
        if sub is None:
            raise HTTPException(404, "unknown_netuid")
        occ, name = sub["occupant_key"], sub["name"]
    sha = hashlib.sha256(data).hexdigest()
    updir = s.path(s.WB_UPLOAD_DIR)
    updir.mkdir(parents=True, exist_ok=True)
    path = updir / f"{sha}.{vision.MEDIA_TYPES[media]}"
    if not path.exists():
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_bytes(data)
        tmp.replace(path)
    created = now_ts()
    res = await db.execute(
        "INSERT INTO wb_uploads (netuid, occupant_key, path, sha256, media_type, bytes, status, created_at) "
        "VALUES (?,?,?,?,?,?, 'stored', ?)", (netuid, occ, str(path), sha, media, len(data), created))
    upload_id = res.lastrowid
    out: dict[str, Any] = {"upload_id": upload_id, "sha256": sha, "path": str(path)}
    if not s.WB_VISION_ENABLED or not s.ANTHROPIC_API_KEY:
        await db.execute("UPDATE wb_uploads SET status = 'vision_disabled' WHERE id = ?", (upload_id,))
        raise HTTPException(409, detail={"error": "vision_disabled", **out,
                                         "hint": "set ANTHROPIC_API_KEY and WB_VISION_ENABLED=true"})
    costs = rt.costs
    if control.kill_switch_active() or (costs is not None and costs.paused_reason("anthropic")):
        raise HTTPException(409, detail={"error": "paused", **out})
    run_id = f"wb_chart_read:{upload_id}"
    resv = await costs.reserve("anthropic", s.WB_VISION_EST_USD, job="wb_chart_read", run_id=run_id) if costs else None
    if costs is not None and resv is None:
        raise HTTPException(409, detail={"error": "budget", **out})
    t0 = time.monotonic()
    try:
        if rt.limiter is not None:
            await rt.limiter.acquire("anthropic")
        client = client_factory(s.ANTHROPIC_API_KEY, float(s.LLM_REQUEST_TIMEOUT_S))
        result = await vision.read_chart(client, data, media, model=s.WB_VISION_MODEL, netuid=netuid, name=name)
    except Exception as exc:
        if resv is not None:
            await costs.release(resv)
        await db.execute("UPDATE wb_uploads SET status = 'failed' WHERE id = ?", (upload_id,))
        logger.warning("wb chart read failed", data={"upload_id": upload_id, "error": f"{type(exc).__name__}: {exc}"})
        raise HTTPException(502, detail={"error": "vision_failed", "message": f"{type(exc).__name__}: {exc}"[:300],
                                         **out}) from None
    if costs is not None:
        await costs.commit(resv, result["cost_usd"], detail={"upload_id": upload_id, "model": result["model"]})
        await costs.record("anthropic", 0.0, requests=1, job="wb_chart_read", run_id=run_id)
    read = result.get("read") or {}
    payload = {k: v for k, v in result.items() if k != "read"}
    payload.update(read=read, upload_id=upload_id, seconds=round(time.monotonic() - t0, 2))
    note = await db.execute_returning(
        "INSERT INTO manual_notes (occupant_key, kind, title, body, evidence_url, payload_json, created_at, created_by) "
        "VALUES (?, 'chart_read', ?, ?, ?, ?, ?, ?) RETURNING id",
        (occ, f"Chart read{f' SN{netuid}' if netuid is not None else ''}", read.get("summary"), str(path),
         json.dumps(payload, default=str), now_ts(), f"wb_vision:{result['model']}"))
    note_id = note[0]["id"]
    await db.execute("UPDATE wb_uploads SET status = ?, model = ?, note_id = ? WHERE id = ?",
                     (result["status"], result["model"], note_id, upload_id))
    return {**out, "note_id": note_id, "status": result["status"], "read": read, "cost_usd": result["cost_usd"],
            "model": result["model"], "reason": result.get("reason")}
