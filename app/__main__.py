"""``python -m app [serve|check|jobs|run-job <id>|health|status|db …|backup …|costs …|ops …]`` (spec 01 §2.14)."""
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
        # built phase = the highest P with an available job in every phase 0..P; jobs above it were built early
        have = {sp.phase for sp in JOB_TABLE if sp.id in rt.jobs or sp.id in rt.tasks}
        max_phase = 0
        while max_phase + 1 in have:
            max_phase += 1
        early = sorted(sp.id for sp in JOB_TABLE if sp.phase > max_phase and (sp.id in rt.jobs or sp.id in rt.tasks))
        ok = qc_lines == ["ok"]
        print(f"{'OK' if ok else 'FAIL'} jobs={len(JOB_TABLE)} declared ({len(JOB_TABLE) - n_tasks} jobs + {n_tasks} "
              f"LongTasks), {avail} available (phase {max_phase}{' + early ' + ','.join(early) if early else ''}), "
              f"{len(rt.unavailable)} unavailable")
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


def _fetch_health() -> dict | None:
    import httpx
    s = app_config.settings
    host = "127.0.0.1" if s.HEALTH_HOST in ("0.0.0.0", "::") else s.HEALTH_HOST
    try:
        return httpx.get(f"http://{host}:{s.HEALTH_PORT}/health", timeout=5).json()
    except Exception:
        return None


def _git_head() -> str | None:
    import subprocess
    from pathlib import Path
    try:
        r = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=Path(__file__).resolve().parent.parent,
                           capture_output=True, text=True, timeout=5)
        return r.stdout.strip() or None
    except Exception:
        return None


async def _status() -> int:
    """One screen for the operator (01 §2.14, 20 N3 item 4): /health + the DB read-only."""
    import textwrap
    from app.api import queries as Q
    from app.api.review import list_items
    from app.features.registry import PILLARS, scored
    from app.notifications.format import fmt_age
    from app.storage.db import ReadOnlyDatabase
    from app.util.time import now_ts, to_iso
    s = app_config.settings
    spec = s.SPEC_VERSION
    now = now_ts()
    hm = lambda ts: "-" if ts is None else to_iso(ts)[5:16].replace("T", " ") + "Z"  # noqa: E731
    h = _fetch_health()
    out: list[str] = []
    if h is None:
        out.append(f"TaoSentinel · {to_iso(now)[:16]}Z · service UNREACHABLE on :{s.HEALTH_PORT} (DB part below)")
    else:
        out.append(f"TaoSentinel · {to_iso(now)[:16]}Z · service {h.get('status')} · v{h.get('version')} · "
                   f"up {fmt_age(h.get('uptime_sec') or 0)}" + (f" · degraded: {', '.join(h['degraded_reasons'])}"
                                                               if h.get("degraded_reasons") else ""))
        running = str(h.get("version") or "").partition("+")[2]
        head = _git_head()
        if running and head and not (head.startswith(running) or running.startswith(head)):
            out.append(f"  ! running {running}, checkout is {head}: sudo systemctl restart taosentinel to load it")
    db = ReadOnlyDatabase(s.DB_PATH)
    try:
        await db.connect()
    except Exception as exc:
        out.append(f"DB       cannot open {s.DB_PATH} read-only: {exc}")
        print("\n".join(out))
        return 1
    try:
        meta = await Q.feed_meta(db, spec=spec)
        S = meta.get("max_as_of")
        off = await db.fetchval("SELECT MAX(as_of) FROM scores WHERE spec_version = ? AND shadow = 0 "
                                "AND run_kind = 'official'", (spec,))
        sc = (h or {}).get("scores") or {}
        out.append(f"Scores   latest {hm(S)} {meta.get('run_kind') or '-'} · official {hm(off)} · regime "
                   f"{meta.get('regime') or '-'}" + (" · feed STALE" if sc.get("stale") else ""))
        if S is not None:
            rows = await db.fetchall("SELECT status, tier, insufficient_data FROM scores WHERE spec_version = ? "
                                     "AND shadow = 0 AND as_of = ?", (spec, S))
            tiers = {t: sum(1 for r in rows if r["status"] == "active" and r["tier"] == t) for t in "ABCDF"}
            ins = sum(1 for r in rows if r["insufficient_data"])
            pre = sum(1 for r in rows if r["status"] == "pre_launch")
            out.append("Tiers    " + " · ".join(f"{t} {n}" for t, n in tiers.items())
                       + f" · insufficient {ins} · pre-launch {pre} (of {len(rows)} scored)")
            n, cov = await Q.norm_coverage(db, spec=spec, as_of=S)
            pp = Q.populated_by_pillar(n, cov)
            out.append(f"Metrics  {sum(a for a, _ in pp.values())}/{sum(b for _, b in pp.values())} populated "
                       f"(≥ half of {n}): " + " · ".join(f"{p} {a}/{b}" for p, (a, b) in pp.items()))
            thin = [f"{d.key}={cov.get(d.key, 0)}" for p in PILLARS for d in scored(p) if cov.get(d.key, 0) * 2 < n]
            if thin:
                out += textwrap.wrap("thin: " + ", ".join(thin), 116, initial_indent="         ",
                                     subsequent_indent="           ")
        if h is not None:
            jobs = {**(h.get("collectors") or {}), **(h.get("tasks") or {})}
            pick = lambda f: sorted(j for j, v in jobs.items() if isinstance(v, dict) and f(v)) or ["none"]  # noqa: E731
            sch = h.get("scheduler") or {}
            out.append(f"Jobs     {sch.get('declared')} declared · {sch.get('available')} available · stale: "
                       f"{', '.join(pick(lambda v: v.get('stale')))} · failing: "
                       f"{', '.join(pick(lambda v: v.get('consecutive_failures')))} · paused: "
                       f"{', '.join(pick(lambda v: v.get('paused')))}")
        unrouted = await db.fetchval("SELECT COUNT(*) FROM events WHERE alerted_at IS NULL")
        flags = await Q.active_flags(db, spec=spec)
        reviews = await list_items(db, kind=None, netuid=None, limit=500)
        pending = ((h or {}).get("alerts") or {}).get("pending_immediate")
        out.append(f"Alerts   pending immediate {pending if pending is not None else '?'} · unrouted events {unrouted} · "
                   f"red flags active {len(flags)} · open reviews {len(reviews)} · telegram "
                   f"{'on' if (h or {}).get('telegram_configured') else 'off'}")
        if h is not None:
            parts = []
            for g, v in sorted((h.get("spend") or {}).items()):
                if v.get("request_cap"):
                    parts.append(f"{g} {(v.get('requests') or 0) / 1000:.1f}k/{v['request_cap'] / 1000:.0f}k req")
                elif v.get("cap_usd"):
                    parts.append(f"{g} ${v.get('usd') or 0:.2f}/${v['cap_usd']:g}")
                else:
                    parts.append(f"{g} off")
                if v.get("paused"):
                    parts[-1] += " PAUSED"
            out.append("Spend    " + " · ".join(parts))
            d = h.get("db") or {}
            disk = ((h.get("ops") or {}).get("disk") or {}).get("free_gb")
            out.append(f"DB       {(d.get('size_bytes') or 0) / 1e9:.2f} GB + {(d.get('wal_bytes') or 0) / 1e6:.0f} MB WAL"
                       f" · schema v{d.get('schema_version')} · backup {d.get('last_backup_age_hours')} h ago"
                       + (f" · disk {disk} GB free" if disk is not None else ""))
        else:
            out.append(f"DB       schema v{db.schema_version} (read-only; size/backup need the service)")
    finally:
        await db.close()
    print("\n".join(out))
    return 0 if h is not None and h.get("status") in ("ok", "degraded") else 1


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
    sub.add_parser("status", help="one-screen status: /health + the DB read-only").set_defaults(
        func=lambda a: asyncio.run(_status()))
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
