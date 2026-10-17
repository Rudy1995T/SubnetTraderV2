"""``python -m app [serve|check|jobs|run-job <id>|health|db …|backup …|costs …|ops …]`` (spec 01 §2.14)."""
from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys

import app.config as app_config


def _cmd_serve(args: argparse.Namespace) -> int:
    from app.runtime import entrypoint
    entrypoint()
    return 0


async def _check(verbose: bool) -> int:
    from app.jobs import JOB_TABLE
    from app.runtime import Runtime, init_core
    s = app_config.settings
    if verbose:
        print(json.dumps(s.public_dict(), indent=1, default=str))
    for w in s.config_warnings:
        print(f"config warning: {w}")
    rt = Runtime(started_at=__import__("app.util.time", fromlist=["now_ts"]).now_ts(), shutdown_event=asyncio.Event())

    async def _sink(text: str) -> bool:
        print(f"[alert] {text}")
        return True
    try:
        await init_core(rt, alert_sink=_sink)
        qc = await rt.db.fetchall("PRAGMA quick_check", writer=True)
        qc_lines = [str(list(r.values())[0]) for r in qc]
        print(f"schema_version={rt.db.schema_version}")
        print(f"schema_warnings={rt.db.schema_warnings}")
        print(f"sqlite_version={sqlite3.sqlite_version}")
        print(f"quick_check={'ok' if qc_lines == ['ok'] else qc_lines[:5]}")
        n_tasks = sum(1 for sp in JOB_TABLE if sp.kind == "longtask")
        avail = len(rt.jobs) + len(rt.tasks)
        max_phase = max((sp.phase for sp in JOB_TABLE if sp.id in rt.jobs or sp.id in rt.tasks), default=0)
        ok = qc_lines == ["ok"]
        print(f"{'OK' if ok else 'FAIL'} jobs={len(JOB_TABLE)} declared ({len(JOB_TABLE) - n_tasks} jobs + {n_tasks} "
              f"LongTasks), {avail} available (phase {max_phase}), {len(rt.unavailable)} unavailable")
        return 0 if ok else 1
    finally:
        if rt.http is not None:
            await rt.http.aclose()
        if rt.db is not None:
            await rt.db.close()


async def _jobs() -> int:
    from app.jobs import JOB_TABLE, build_trigger, next_fire_times
    from app.runtime import Runtime, init_core
    rt = Runtime(started_at=0, shutdown_event=asyncio.Event())

    async def _sink(text: str) -> bool:
        return True
    try:
        await init_core(rt, alert_sink=_sink)
        print(f"{'id':<24} {'own':<4} {'ph':>2} {'kind':<9} {'avail':<5} {'enabled':<7} {'paused':<14} "
              f"{'ops':<4} {'trigger':<28} next_run")
        for sp in JOB_TABLE:
            obj = rt.jobs.get(sp.id) or rt.tasks.get(sp.id)
            avail = obj is not None
            enabled = obj.enabled() if obj else None
            paused = (obj.paused_reason() if obj and enabled else None) or "-"
            nxt = "-"
            if sp.kind in ("interval", "cron"):
                try:
                    t = next_fire_times(build_trigger(sp), 1)
                    nxt = t[0].strftime("%Y-%m-%d %H:%MZ") if t else "-"
                except Exception:
                    pass
            print(f"{sp.id:<24} {sp.owner:<4} {sp.phase:>2} {sp.kind:<9} {'yes' if avail else 'no':<5} "
                  f"{('yes' if enabled else 'no') if avail else '-':<7} {paused:<14} "
                  f"{'yes' if sp.ops_exempt else 'no':<4} {sp.trigger_desc[:28]:<28} {nxt}")
        return 0
    finally:
        if rt.http is not None:
            await rt.http.aclose()
        if rt.db is not None:
            await rt.db.close()


async def _run_job(job_id: str) -> int:
    from app.jobs import SPEC_BY_ID
    from app.runtime import Runtime, init_core
    from app.util.time import now_ts
    if job_id not in SPEC_BY_ID:
        print(f"unknown job {job_id!r}")
        return 1
    rt = Runtime(started_at=now_ts(), shutdown_event=asyncio.Event())

    async def _sink(text: str) -> bool:
        print(f"[alert] {text}")
        return True
    import app.runtime as runtime_mod
    runtime_mod.runtime = rt
    try:
        await init_core(rt, alert_sink=_sink)
        job = rt.jobs.get(job_id)
        if job is None:
            print(json.dumps({"job": job_id, "result": "unavailable", "reason": rt.unavailable.get(job_id)}))
            return 1
        before = job._status.last_ok
        await job.run(trigger="cli")
        st = job.status()
        print(json.dumps(st, indent=1, default=str))
        if rt.costs is not None:
            await rt.costs.flush()
        return 0 if job._status.last_ok and job._status.last_ok != before else 1
    finally:
        runtime_mod.runtime = None
        if rt.http is not None:
            await rt.http.aclose()
        if rt.db is not None:
            await rt.db.close()


def _cmd_health(args: argparse.Namespace) -> int:
    import httpx
    s = app_config.settings
    host = "127.0.0.1" if s.HEALTH_HOST in ("0.0.0.0", "::") else s.HEALTH_HOST
    try:
        r = httpx.get(f"http://{host}:{s.HEALTH_PORT}/health", timeout=10)
        data = r.json()
    except Exception as exc:
        print(f"unreachable: {exc}")
        return 1
    print(json.dumps(data, indent=1))
    return 0 if data.get("status") in ("ok", "degraded") else 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m app", description="TaoSentinel")
    sub = p.add_subparsers(dest="cmd")
    sub.add_parser("serve", help="run the service (systemd ExecStart)").set_defaults(func=_cmd_serve)
    c = sub.add_parser("check", help="validate config, open DB, apply schema, build the job registry")
    c.add_argument("--verbose", "-v", action="store_true", help="also print every (masked) setting")
    c.set_defaults(func=lambda a: asyncio.run(_check(a.verbose)))
    sub.add_parser("jobs", help="the declared job table").set_defaults(func=lambda a: asyncio.run(_jobs()))
    r = sub.add_parser("run-job", help="run one job once through the wrapper")
    r.add_argument("job_id")
    r.set_defaults(func=lambda a: asyncio.run(_run_job(a.job_id)))
    sub.add_parser("health", help="GET /health of the running service").set_defaults(func=_cmd_health)
    from app.storage import cli as db_cli
    db_cli.register(sub)
    from app.ops import cli as ops_cli
    ops_cli.register(sub)
    return p


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        argv = ["serve"]
    args = build_parser().parse_args(argv)
    if not hasattr(args, "func"):
        build_parser().print_help()
        return 2
    rc = args.func(args)
    return int(rc or 0)


if __name__ == "__main__":
    sys.exit(main())
