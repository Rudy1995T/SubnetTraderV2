"""Launcher for the in-service GitHub history backfill job ``backfill_github`` (spec 17 §2.8, step S9).

    python -m scripts.backfill_github [--netuids 1,2,…] [--no-prs] [--redo] [--standalone] [--no-wait] [--once]

Writes ``data/backfill/github_request.json``, then (service up) ``POST /api/v1/control/run/backfill_github`` and tails
the sidecar. A run ending ``partial: outside window`` makes the launcher sleep until ``BACKFILL_GITHUB_WINDOW_UTC``
opens and re-trigger; any other ``partial:`` (soft deadline, quota, pause) re-triggers at once. ``--once`` stops after
the first run. Service down (or ``--standalone``, which refuses while ``/health`` answers): runs the job through 01's
``run-job`` path in this process. Exit codes: 0 done, 1 failed/refused, 2 bad arguments, 3 job unavailable.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time

import app.config as app_config

JOB = "backfill_github"


def _progress() -> dict:
    from app.backfill.github_backfill import _sidecar, progress_counts
    data = _sidecar().data
    return {**progress_counts(data), "rollup": (data.get("_meta") or {}).get("rollup", {}).get("status")}


def _sleep_to_window() -> None:
    from app.backfill.github_backfill import window_state
    s = app_config.settings
    inside, _, nxt = window_state(int(time.time()), s.BACKFILL_GITHUB_WINDOW_UTC)
    if inside or nxt is None:
        return
    print(json.dumps({"waiting_for_window": s.BACKFILL_GITHUB_WINDOW_UTC, "until_ts": nxt + 30}), flush=True)
    while time.time() < nxt + 30:
        time.sleep(min(600.0, max(1.0, nxt + 30 - time.time())))


async def _standalone(once: bool) -> int:
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
        job = rt.jobs.get(JOB)
        if job is None:
            print(json.dumps({"job": JOB, "result": "unavailable", "reason": rt.unavailable.get(JOB)}))
            return 3
        while True:
            before = job._status.last_ok
            await job.run(trigger="cli")
            st = job.status()
            print(json.dumps({k: st.get(k) for k in ("last_ok", "last_error", "last_rows", "last_skip_reason",
                                                     "last_duration_sec")}, default=str))
            if job._status.last_ok == before:
                return 1
            note = await rt.db.fetchval(f"SELECT note FROM collector_runs WHERE job = '{JOB}' "
                                        "ORDER BY started_at DESC LIMIT 1") or ""
            print(f"note: {note}", flush=True)
            if not note.startswith("partial:") or once:
                return 0
            if note.startswith("partial: outside window"):
                await asyncio.to_thread(_sleep_to_window)
    finally:
        if rt.costs is not None:
            await rt.costs.flush()
        runtime_mod.runtime = None
        if rt.http is not None:
            await rt.http.aclose()
        if rt.db is not None:
            await rt.db.close()


def _job_status(payload, job_id: str) -> dict:
    jobs = payload.get("jobs", []) if isinstance(payload, dict) else payload
    for j in jobs or []:
        if isinstance(j, dict) and j.get("id") == job_id:
            return j.get("status") or {}
    return {}


def _last_note() -> str:
    import sqlite3
    s = app_config.settings
    con = sqlite3.connect(f"file:{s.path(s.DB_PATH)}?mode=ro", uri=True)
    try:
        row = con.execute(f"SELECT note FROM collector_runs WHERE job = '{JOB}' AND finished_at IS NOT NULL "
                          "ORDER BY finished_at DESC LIMIT 1").fetchone()
    finally:
        con.close()
    return (row[0] if row else None) or ""


def _in_service(no_wait: bool, once: bool) -> int:
    import httpx
    s = app_config.settings
    base = f"http://{s.HEALTH_HOST}:{s.HEALTH_PORT}"
    headers = {"X-Auth-Token": s.API_AUTH_TOKEN} if s.API_AUTH_TOKEN else {}

    def _status() -> dict:
        return _job_status(httpx.get(f"{base}/api/v1/jobs", headers=headers, timeout=10).json(), JOB)

    while True:
        before = _status().get("last_run")
        r = httpx.post(f"{base}/api/v1/control/run/{JOB}", headers=headers, timeout=10)
        if r.status_code == 404:
            print(f"refused: HTTP 404 {r.text[:200]} (the running service predates S9: restart it)", file=sys.stderr)
            return 3
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
            if job.get("last_run") != before:
                break
            print(json.dumps({"progress": _progress(), "last_error": job.get("last_error")}, default=str), flush=True)
        note = _last_note()
        print(json.dumps({"finished": job.get("last_run"), "rows": job.get("last_rows"), "note": note,
                          "last_error": job.get("last_error")}, default=str), flush=True)
        if not note.startswith("partial:") or once:
            return 0 if not job.get("last_error") else 1
        if note.startswith("partial: outside window"):
            _sleep_to_window()


def main(argv: list[str] | None = None) -> int:
    from app.backfill import common
    from app.backfill.github_backfill import write_request
    from app.util.time import now_ts
    p = argparse.ArgumentParser(prog="python -m scripts.backfill_github", description=__doc__.split("\n")[0])
    p.add_argument("--netuids", help="comma-separated netuids (default: every occupant in scope)")
    p.add_argument("--no-prs", action="store_true", help="skip merged PRs and their reviews")
    p.add_argument("--redo", action="store_true", help="process every repo once more (dedupe keeps rows unique)")
    p.add_argument("--standalone", action="store_true", help="run in this process (service must be down)")
    p.add_argument("--no-wait", action="store_true", help="trigger once and return")
    p.add_argument("--once", action="store_true", help="stop after the first run, even when it is partial")
    a = p.parse_args(argv)
    try:
        netuids = [int(x) for x in a.netuids.split(",") if x.strip()] if a.netuids else None
    except ValueError:
        print("--netuids takes comma-separated integers", file=sys.stderr)
        return 2
    try:
        common.check_enabled()
        if not app_config.settings.BACKFILL_GITHUB_ENABLED:
            raise common.PreconditionError("BACKFILL_GITHUB_ENABLED=false", code=common.EXIT_DISABLED)
    except common.PreconditionError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return exc.code
    up = common.service_is_up()
    if a.standalone and up:
        print("refused: --standalone while the service answers /health (use the in-service path)", file=sys.stderr)
        return 2
    write_request(netuids=netuids, prs=not a.no_prs, redo=a.redo, now=now_ts())
    if up:
        return _in_service(a.no_wait, a.once)
    return asyncio.run(_standalone(a.once))


if __name__ == "__main__":
    sys.exit(main())
