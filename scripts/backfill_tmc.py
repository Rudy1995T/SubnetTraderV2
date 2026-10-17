"""Launcher for the in-service TMC history backfill job ``backfill_tmc`` (spec 17 §2.4, steps S3/S6).

    python -m scripts.backfill_tmc [--tasks analytics1y,analytics1m,restamp,candles5m,positions]
        [--netuids 1,2,…] [--coldkey SS58] [--redo] [--standalone] [--no-wait]

Service up: writes ``data/backfill/tmc_request.json``, ``POST /api/v1/control/run/backfill_tmc`` and tails the sidecar
(re-triggering after a ``partial:`` soft-deadline end). Service down (or ``--standalone``, which refuses while
``/health`` answers): runs the job through 01's ``run-job`` path in this process — the service is then the only other
TMC caller and it is not running, so the shared-bucket rule holds.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time

import app.config as app_config


def _progress() -> dict:
    from app.backfill.tmc_backfill import _sidecar
    out = {}
    for task, units in _sidecar().data.get("tasks", {}).items():
        st = [u.get("status") for u in units.values()]
        out[task] = {k: st.count(k) for k in ("done", "partial", "failed") if st.count(k)}
    return out


async def _standalone() -> int:
    import app.runtime as runtime_mod
    from app.runtime import Runtime, init_core
    from app.util.time import now_ts
    rt = Runtime(started_at=now_ts(), shutdown_event=asyncio.Event())

    async def _sink(text: str) -> bool:
        print(f"[alert] {text}")
        return True
    runtime_mod.runtime = rt
    try:
        await init_core(rt, alert_sink=_sink)
        job = rt.jobs.get("backfill_tmc")
        if job is None:
            print(json.dumps({"job": "backfill_tmc", "result": "unavailable",
                              "reason": rt.unavailable.get("backfill_tmc")}))
            return 3
        while True:
            before = job._status.last_ok
            await job.run(trigger="cli")
            st = job.status()
            print(json.dumps({k: st.get(k) for k in ("last_ok", "last_error", "last_rows", "last_skip_reason",
                                                     "last_duration_sec")}, default=str))
            if job._status.last_ok == before:
                return 1
            note = await rt.db.fetchval("SELECT note FROM collector_runs WHERE job = 'backfill_tmc' "
                                        "ORDER BY started_at DESC LIMIT 1")
            print(f"note: {note}")
            if not (note or "").startswith("partial:"):
                return 0
    finally:
        if rt.costs is not None:
            await rt.costs.flush()
        runtime_mod.runtime = None
        if rt.http is not None:
            await rt.http.aclose()
        if rt.db is not None:
            await rt.db.close()


def _job_status(payload, job_id: str) -> dict:
    """``GET /api/v1/jobs`` → ``{"jobs": [{"id", …, "status": {…}}]}``; returns the job's ``status`` dict."""
    jobs = payload.get("jobs", []) if isinstance(payload, dict) else payload
    for j in jobs or []:
        if isinstance(j, dict) and j.get("id") == job_id:
            return j.get("status") or {}
    return {}


def _in_service(no_wait: bool) -> int:
    import httpx
    s = app_config.settings
    base = f"http://{s.HEALTH_HOST}:{s.HEALTH_PORT}"
    headers = {"X-Auth-Token": s.API_AUTH_TOKEN} if s.API_AUTH_TOKEN else {}
    def _status() -> dict:
        return _job_status(httpx.get(f"{base}/api/v1/jobs", headers=headers, timeout=10).json(), "backfill_tmc")

    while True:
        # the job's status has no running flag and sets last_run when a run ends: wait for last_run to change
        before = _status().get("last_run")
        r = httpx.post(f"{base}/api/v1/control/run/backfill_tmc", headers=headers, timeout=10)
        if r.status_code == 409 and "in_flight" not in r.text:
            print(f"trigger refused: HTTP 409 {r.text[:200]}", file=sys.stderr)
            return 1
        if r.status_code not in (200, 202, 409):
            print(f"trigger failed: HTTP {r.status_code} {r.text[:200]}", file=sys.stderr)
            return 1
        if no_wait:
            print(r.text)
            return 0
        while True:
            time.sleep(30)
            job = _status()
            print(json.dumps({"progress": _progress(), "last_run": job.get("last_run"),
                              "last_error": job.get("last_error")}, default=str), flush=True)
            if job.get("last_run") != before:
                break
        note = _last_note()
        print(json.dumps({"finished": job.get("last_run"), "rows": job.get("last_rows"), "note": note,
                          "last_error": job.get("last_error")}, default=str), flush=True)
        if not note.startswith("partial:"):
            return 0 if not job.get("last_error") else 1


def _last_note() -> str:
    import sqlite3
    s = app_config.settings
    con = sqlite3.connect(f"file:{s.path(s.DB_PATH)}?mode=ro", uri=True)
    try:
        row = con.execute("SELECT note FROM collector_runs WHERE job = 'backfill_tmc' AND finished_at IS NOT NULL "
                          "ORDER BY finished_at DESC LIMIT 1").fetchone()
    finally:
        con.close()
    return (row[0] if row else None) or ""


def main(argv: list[str] | None = None) -> int:
    from app.backfill import common
    from app.backfill.tmc_backfill import ALL_TASKS, write_request
    p = argparse.ArgumentParser(prog="python -m scripts.backfill_tmc", description=__doc__.split("\n")[0])
    p.add_argument("--tasks", default="analytics1y,analytics1m,restamp,candles5m")
    p.add_argument("--netuids")
    p.add_argument("--coldkey")
    p.add_argument("--redo", action="store_true")
    p.add_argument("--standalone", action="store_true")
    p.add_argument("--no-wait", action="store_true")
    a = p.parse_args(argv)
    tasks = [t.strip() for t in a.tasks.split(",") if t.strip()]
    bad = [t for t in tasks if t not in ALL_TASKS]
    if bad:
        print(f"unknown tasks {bad}; choose from {list(ALL_TASKS)}", file=sys.stderr)
        return 2
    try:
        common.check_enabled()
    except common.PreconditionError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return exc.code
    netuids = [int(x) for x in a.netuids.split(",")] if a.netuids else None
    up = common.service_is_up()
    if a.standalone and up:
        print("refused: --standalone while the service answers /health (use the in-service path)", file=sys.stderr)
        return 2
    write_request(tasks, netuids=netuids, coldkey=a.coldkey, redo=a.redo)
    if up:
        return _in_service(a.no_wait)
    return asyncio.run(_standalone())


if __name__ == "__main__":
    sys.exit(main())
