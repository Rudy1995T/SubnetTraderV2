"""``/api/v1/subnets`` (matrix source) and ``/api/v1/subnets/{netuid}`` (detail + evidence, 02 §H2; spec 14 §3.5)."""
from __future__ import annotations

from fastapi import APIRouter, Request

from app.api import queries as Q
from app.api import scores as S
from app.api.common import RuntimeRef, as_bool, r2, r3
from app.api.errors import ApiError
from app.logging.logger import logger
from app.util.time import now_ts, to_iso

router = APIRouter()
_ref = RuntimeRef()
configure = _ref.configure


def links(sub: dict | None) -> dict:
    sub = sub or {}
    return {"website": sub.get("website_url"), "github": sub.get("github_url"), "docs": sub.get("docs_url"),
            "discord": sub.get("discord_url"), "telegram": sub.get("telegram_url"), "x": sub.get("x_handle")}


@router.get("/api/v1/subnets")
async def subnets_list(request: Request) -> dict:
    db = _ref.db()
    q = request.query_params
    spec = S.default_spec(q.get("spec"))
    now = now_ts()
    subs = await Q.subnet_rows(db, include_prelaunch=as_bool(q.get("include_prelaunch")),
                               include_deregistered=as_bool(q.get("include_deregistered")))
    latest = {r["occupant_key"]: r for r in await Q.latest_scores(db, spec=spec, include_prelaunch=True,
                                                                     include_insufficient=True)}
    spark = await Q.spark_7d(db, spec=spec)
    newest = max((r["as_of"] for r in latest.values()), default=None)
    out = []
    for s in subs:
        r = latest.get(s["occupant_key"])
        score = None
        if r is not None:
            score = {"composite": r2(r["composite_smoothed"]), "quality": r2(r["quality"]),
                     "velocity": r2(r["velocity"]), "confidence": r2(r["confidence"]), "tier": r["tier"],
                     "red_flags": list(r["red_flags"]), "pillars": S.pillars(r["pillar_scores"]),
                     "as_of": to_iso(r["as_of"]), "insufficient_data": bool(r["insufficient_data"])}
        out.append({"netuid": s["netuid"], "occupant_key": s["occupant_key"], "name": s["name"], "symbol": s["symbol"],
                    "status": s["status"], "immune": None if s["immune"] is None else bool(s["immune"]),
                    "network_registered_at": s["network_registered_at"], "links": links(s), "score": score,
                    "spark_7d": spark.get(s["occupant_key"], [])})
    return {"as_of": to_iso(newest), "spec_version": spec, "stale": S.is_stale(newest, now), "subnets": out}


def _flag_objects(explain: dict | None) -> list[dict]:
    out = []
    for f in (explain or {}).get("flags") or []:
        if not isinstance(f, dict):
            continue
        since = f.get("since")
        out.append({"flag": f.get("flag"), "multiplier": f.get("multiplier"),
                    "since": to_iso(since) if isinstance(since, (int, float)) else since, "state": f.get("state"),
                    "origin": f.get("origin"), "human": bool(f.get("human")), "sticky": bool(f.get("sticky"))})
    return out


async def subnet_detail(db, *, netuid: int, occupant: str | None, spec: str, events_limit: int) -> dict:
    if occupant:
        res = await Q.resolve_occupant(db, occupant)
        if res is None or (res[1] != netuid):
            raise ApiError("unknown_occupant", 404, f"no occupant {occupant!r} on SN{netuid}")
        occ = res[0]
    else:
        res = await Q.resolve_occupant(db, netuid)
        if res is None:
            raise ApiError("unknown_netuid", 404, f"SN{netuid} has no current occupant")
        occ = res[0]
    info = await Q.occupant_info(db, occ)
    sub = (info or {}).get("sub") or {}
    now = now_ts()
    row = await Q.latest_score_row(db, occupant_key=occ, spec=spec)
    score = None
    ev: list[dict] = []
    if row is not None:
        explain = row["explain"]
        if row["explain_raw"] and explain is None:
            logger.warning("explain JSON unparsable", data={"occupant_key": occ, "as_of": row["as_of"]})
        score = {"as_of": to_iso(row["as_of"]), "computed_at": to_iso(row["computed_at"]),
                 "run_kind": row["run_kind"], "status": row["status"], "composite": r2(row["composite_smoothed"]),
                 "composite_raw": r2(row["composite_raw"]), "quality": r2(row["quality"]),
                 "velocity": r2(row["velocity"]), "confidence": r2(row["confidence"]), "tier": row["tier"],
                 "insufficient_data": bool(row["insufficient_data"]), "regime": row["regime"],
                 "pillars": S.pillars(row["pillar_scores"]),
                 "coverage": {k: r3(v) for k, v in (row["pillar_coverage"] or {}).items()},
                 "red_flags": _flag_objects(explain) if explain is not None else [],
                 "red_flag_names": list(row["red_flags"]), "explain": explain, "spec_version": spec,
                 "stale": S.is_stale(row["as_of"], now)}
        for e in await Q.evidence(db, occupant_key=occ, spec=spec, as_of=row["as_of"]):
            fresh = e["raw_as_of"] is not None and e["freshness_s"] is not None and \
                now - int(e["raw_as_of"]) <= int(e["freshness_s"])
            m = e["m"]
            if e["raw_status"] in ("failed", "none"):
                m = None
            ev.append({**e, "m": r3(m) if isinstance(m, (int, float)) else m,
                       "raw_as_of": to_iso(e["raw_as_of"]) if e["raw_as_of"] is not None else None, "fresh": fresh})
    from app.integrity import team as team11
    team = await team11.list_members(db, occupant_key=occ)
    man = await Q.manual_state(db, occupant_key=occ)
    disc = man["disclosure"]
    prod = man["product"]
    hist = await Q.score_history(db, occupant_key=occ, spec=spec, days=90, official_only=True, now=now)
    site = await Q.site_state(db, occupant_key=occ)
    head = sub.get("head_block")
    reg = (info or {}).get("network_registered_at")
    return {
        "netuid": netuid, "occupant_key": occ, "name": (info or {}).get("name") or sub.get("name"),
        "status": (info or {}).get("status"), "current": bool((info or {}).get("current")),
        "immune": None if sub.get("immune") is None else bool(sub.get("immune")),
        "tenure_days": round((int(head) - int(reg)) * 12 / 86400, 1) if head and reg is not None else None,
        "links": links(sub), "owner_coldkey": sub.get("owner_coldkey"), "spec_version": spec,
        "score": score, "evidence": ev,
        "events": [{**e, "event_at": to_iso(e["event_at"]), "observed_at": to_iso(e["observed_at"]),
                    "alerted_at": to_iso(e["alerted_at"])}
                   for e in await Q.occupant_events(db, occupant_key=occ, limit=events_limit)],
        "site": None if site is None else {**site, "at": to_iso(site["at"])},
        "team": [{**{k: t.get(k) for k in ("id", "name", "role", "profile_url", "evidence_url", "evidence_count",
                                            "status", "source", "reject_reason")},
                  "verified_at": to_iso(t.get("verified_at")), "last_checked_at": to_iso(t.get("last_checked_at"))}
                 for t in team],
        "disclosure": None if disc is None else {"value": disc["value"], "evidence_url": disc["evidence_url"],
                                                 "set_at": to_iso(disc["created_at"]), "set_by": disc["created_by"]},
        "product": None if prod is None else {"status": prod["value"], "evidence_url": prod["evidence_url"],
                                              "note": prod["body"], "set_at": to_iso(prod["created_at"]),
                                              "set_by": prod["created_by"]},
        "notes": [{"id": n["id"], "text": n["body"], "evidence_url": n["evidence_url"],
                   "created_at": to_iso(n["created_at"]), "created_by": n["created_by"]} for n in man["notes"]],
        "manual_flags": [{**f, "set_at": to_iso(f["set_at"])} for f in man["manual_flags"]],
        "history_90d": [S.history_point(h) for h in hist],
    }


@router.get("/api/v1/subnets/{netuid}")
async def subnet_get(netuid: int, request: Request) -> dict:
    db = _ref.db()
    q = request.query_params
    try:
        lim = max(1, min(int(q.get("events_limit") or 20), 100))
    except ValueError:
        raise ApiError("bad_param", 422, "events_limit must be an int") from None
    return await subnet_detail(db, netuid=netuid, occupant=q.get("occupant") or None,
                               spec=S.default_spec(q.get("spec")), events_limit=lim)
