"""``/health.integrity`` (spec 11 §7): cheap counts + the last run meta of the five jobs. Never raises."""
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.util.time import floor_day, now_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.runtime import Runtime


async def integrity_health(rt: "Runtime") -> dict[str, Any]:
    db = rt.db
    if db is None or not db.connected:
        return {"error": "db not connected"}
    now = now_ts()
    today = floor_day(now)
    s = app_config.settings
    try:
        last = await db.fetchval("SELECT value_json FROM job_state WHERE job = 'owner_collect_daily' AND key = "
                                 "'last_run'")
        meta = json.loads(last) if last else {}
        snaps = await db.fetchval("SELECT COUNT(*) FROM owner_snapshots WHERE observed_at >= ?", (today,))
        cov_ok = await db.fetchval("SELECT COUNT(*) FROM job_state WHERE job = 'owner_collect_daily' AND key LIKE "
                                   "'activity_wm:%' AND json_extract(value_json, '$.ts') >= ?",
                                   (now - int(s.OWNER_SNAPSHOT_MAX_AGE_HOURS) * 3600,))
        team = {r["status"]: r["n"] for r in await db.fetchall("SELECT status, COUNT(*) AS n FROM team_members "
                                                               "GROUP BY status")}
        reverify = await db.fetchval("SELECT COUNT(*) FROM team_members WHERE status = 'verified' AND "
                                     "last_checked_at < ?", (now - int(s.TEAM_REVERIFY_DAYS) * 86400,))
        unknown = await db.fetchval("SELECT COUNT(*) FROM metric_values WHERE metric_key = 'team_doxxed' AND "
                                    "stage = 'raw' AND as_of = (SELECT MAX(as_of) FROM metric_values WHERE "
                                    "metric_key = 'team_doxxed' AND stage = 'raw') AND status = 'failed'")
        review = {r["kind"]: r["n"] for r in await db.fetchall("SELECT kind, COUNT(*) AS n FROM review_queue WHERE "
                                                               "status = 'open' GROUP BY kind")}
        trig = {}
        for key, name in (("owner_extraction_trigger", "owner_extraction"),
                          ("owner_control_change_trigger", "owner_control_change")):
            trig[name] = await db.fetchval(
                "SELECT COUNT(*) FROM metric_values WHERE metric_key = ? AND stage = 'raw' AND value = 1.0 AND "
                "as_of = (SELECT MAX(as_of) FROM metric_values WHERE metric_key = ? AND stage = 'raw')", (key, key))
        rufus_at = await db.fetchval("SELECT value_json FROM job_state WHERE job = 'owner_collect_daily' AND key = "
                                     "'rufus_baked_at'")
        rufus_at = int(json.loads(rufus_at)) if rufus_at else None
        return {
            "owner": {"occupants": meta.get("occupants"), "snapshots_today": snaps,
                      "keys_missing": meta.get("keys_missing", 0), "c1_fail": meta.get("c1_fail", 0),
                      "c2_fail": meta.get("c2_fail", 0), "c3_fail": meta.get("c3_fail", 0),
                      "c4_fail": meta.get("c4_fail", 0), "c4_truncated": meta.get("c4_truncated", 0),
                      "activity_coverage_ok": cov_ok, "tmc_requests_last_run": meta.get("tmc_requests"),
                      "last_run": to_iso(meta["at"]) if meta.get("at") else None},
            "rufus": {"baked_at": to_iso(rufus_at) if rufus_at else None,
                      "stale": None if rufus_at is None else now - rufus_at > int(s.OWNER_RUFUS_MAX_AGE_HOURS) * 3600,
                      "schema_ok": meta.get("rufus") not in ("schema_drift",)},
            "subnetradar": {"parse_ok": meta.get("radar") == "ok" if meta else None, "rows": meta.get("radar_rows")},
            "taostats": {"active": bool(meta.get("c8_active")), "requests_last_run": meta.get("c8_requests", 0)},
            "team": {"candidates": team.get("candidate", 0), "verified": team.get("verified", 0),
                     "rejected": team.get("rejected", 0), "reverify_due": reverify, "unknown": unknown},
            "review": {"open_by_kind": review}, "triggers": trig}
    except Exception as exc:  # pragma: no cover - /health never fails on this block
        return {"error": f"{type(exc).__name__}: {exc}"}
