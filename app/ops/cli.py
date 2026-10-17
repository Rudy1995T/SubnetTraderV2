"""``python -m app backup …`` / ``costs …`` / ``ops …`` (spec 16 §2.8, §2.10, §8.1).

CLI processes never take the service's instance lock and pass an ``AlertLatch`` whose sink prints instead of
sending, so drills never reach Telegram.
"""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

import app.config as app_config


async def _print_sink(text: str) -> bool:
    print(f"[alert] {text}")
    return True


async def _db():
    from app.storage.db import Database
    s = app_config.settings
    db = Database(s.path(s.DB_PATH))
    await db.connect()
    await db.apply_schema()
    return db


# ── backup ──────────────────────────────────────────────────────────────────────────────────────────────────
async def _backup(args: argparse.Namespace) -> int:
    from app.storage import backup as b
    s = app_config.settings
    if args.backup_cmd == "list":
        d = Path(s.DB_BACKUP_MIRROR_DIR) if args.mirror else s.path(s.DB_BACKUP_DIR)
        if args.manual:
            d = d / "manual"
        if not d.is_dir():
            print(f"(no backups in {d})")
            return 0
        print(f"{'stamp':<18} {'tag':<14} {'forms':<7} {'size MB':>9} {'schema':>6} {'age h':>7}")
        for m in sorted(d.glob("taosentinel-*.json")):
            try:
                man = json.loads(m.read_text())
            except ValueError:
                continue
            forms = ",".join(f for f, ext in (("db", ".db"), ("gz", ".db.gz"))
                             if (d / (m.stem + ext)).exists())
            age = (int(__import__("time").time()) - int(man.get("created_at", 0))) / 3600
            print(f"{man.get('stamp', ''):<18} {str(man.get('tag') or ''):<14} {forms:<7} "
                  f"{man.get('size_bytes', 0) / 2**20:>9.1f} {man.get('schema_version', ''):>6} {age:>7.1f}")
        return 0
    if args.backup_cmd == "verify":
        data, man = b._find(args.target, "local")
        if data is None:
            print(f"not found: {args.target}")
            return 1
        ok, detail = await b.verify_snapshot(data, full=args.full, manifest=man)
        print(("OK " if ok else "FAILED ") + detail)
        return 0 if ok else 1
    if args.backup_cmd == "now":
        from app.storage.db import Database
        db = Database(s.path(s.DB_PATH))          # never apply_schema on the live file from the CLI
        await db.connect()
        try:
            res = await b.create_backup(db, tag=args.tag, wait_for_jobs=False)
        finally:
            await db.close()
        print(json.dumps({**res.manifest, "mirror": res.mirror, "mirror_error": res.mirror_error,
                          "pruned": res.pruned}, indent=1))
        return 0
    if args.backup_cmd == "prune":
        deleted = b.apply_retention(s.path(s.DB_BACKUP_DIR), daily=args.keep_daily or s.DB_BACKUP_RETENTION,
                                    weekly=args.keep_weekly if args.keep_weekly is not None else
                                    s.DB_BACKUP_WEEKLY_RETENTION, keep_uncompressed=s.DB_BACKUP_KEEP_UNCOMPRESSED,
                                    budget_gb=s.DB_BACKUP_MAX_TOTAL_GB)
        print(f"deleted {len(deleted)} files")
        return 0
    if args.backup_cmd == "restore":
        return await b.restore_snapshot(args.stamp, source=args.source, target=Path(args.target) if args.target
                                        else None, yes=args.yes)
    return 1


# ── costs ───────────────────────────────────────────────────────────────────────────────────────────────────
async def _costs(args: argparse.Namespace) -> int:
    from app.costs import CostGovernor
    from app.notifications.alerts import AlertLatch
    db = await _db()
    try:
        latch = AlertLatch(db, sink=_print_sink)
        gov = CostGovernor(db, alerts=latch)
        await gov.load()
        if args.costs_cmd == "status":
            print(f"{'group':<10} {'usd':>9} {'cap':>9} {'pct':>6} {'requests':>10} {'req cap':>9}  state")
            for g, v in gov.snapshot().items():
                state = v["paused_reason"] or ("paused" if v["paused"] else "ok")
                cap = "-" if v["cap_usd"] is None else f"{v['cap_usd']:.2f}"
                rc = "-" if v["request_cap"] is None else f"{v['request_cap']:,}"
                print(f"{g:<10} {v['usd']:>9.2f} {cap:>9} {v['pct']:>6.1%} {v['requests']:>10,} {rc:>9}  {state}")
            return 0
        if args.costs_cmd == "override":
            await gov.set_override(args.group, args.usd, by="cli", reason=args.reason)
            print(f"override set: {args.group} +${args.usd:.2f}")
            return 0
        if args.costs_cmd == "clear-override":
            await gov.clear_override(args.group)
            print("override cleared")
            return 0
        if args.costs_cmd == "set":
            await gov.set_absolute(args.provider, args.usd, month=args.month, by="cli")
            print(f"{args.provider} set to ${args.usd:.2f}")
            return 0
        if args.costs_cmd == "record":
            from app.ops.recovery import InstanceLock
            s = app_config.settings
            lock = InstanceLock(s.path(s.RUN_DIR) / "taosentinel.lock")
            live_default = s.path("data/taosentinel.db").resolve() == s.path(s.DB_PATH).resolve()
            if live_default and not lock.acquire():
                print("refusing: `costs record` is for drills on a scratch DB_PATH while the service runs")
                return 2
            lock.release()
            await gov.record(args.provider, args.usd, requests=1)
            await gov.refresh()
            v = gov.snapshot()
            from app.costs import group_of
            g = group_of(args.provider).name
            print(f"{g}: usd={v.get(g, {}).get('usd')} paused_reason={v.get(g, {}).get('paused_reason')}")
            return 0
    finally:
        await db.close()
    return 1


# ── ops ─────────────────────────────────────────────────────────────────────────────────────────────────────
async def _ops(args: argparse.Namespace) -> int:
    from app import control
    s = app_config.settings
    if args.ops_cmd == "selfcheck":
        from app.ops.security import selfcheck_json
        print(selfcheck_json(s))
        return 0
    if args.ops_cmd == "integrity-check":
        from app.ops.recovery import quick_check_sync, write_integrity
        lines = await asyncio.to_thread(quick_check_sync, s.path(s.DB_PATH))
        ok = lines == ["ok"]
        write_integrity("ok" if ok else "failed", lines)
        print("ok" if ok else "\n".join(lines[:5]))
        return 0 if ok else 1
    db = await _db()
    try:
        if args.ops_cmd == "boots":
            from app.ops.recovery import boots
            print(json.dumps(await boots(db), indent=1))
            return 0
        if args.ops_cmd == "pause":
            try:
                res = await control.pause(args.target, by="cli", reason=args.reason, hours=args.hours, db=db)
            except control.NotPausable:
                print(f"{args.target} is not pausable (02 §B12); use ALERTS_ENABLED / TELEGRAM_COMMANDS_ENABLED")
                return 2
            except control.UnknownTarget:
                print(f"unknown target {args.target!r}; groups: {', '.join(sorted(control.PAUSE_GROUPS))} "
                      "(or an exact job id — no prefix matching)")
                return 2
            print(json.dumps(res, indent=1))
            return 0
        if args.ops_cmd == "resume":
            try:
                res = await control.resume(args.target, by="cli", db=db)
            except control.UnknownTarget:
                print(f"unknown target {args.target!r}")
                return 2
            print(json.dumps(res, indent=1))
            return 0
        if args.ops_cmd == "rearm":
            if args.job != "owner_rpc_seed":
                print("only owner_rpc_seed can be re-armed")
                return 2
            await db.execute("DELETE FROM job_state WHERE job = 'owner_rpc_seed' AND key = 'rpc_seed_done'")
            print("owner_rpc_seed re-armed")
            return 0
        if args.ops_cmd == "digest":
            from app.ops.digest import build_digest
            from app.runtime import Runtime
            rt = Runtime(started_at=__import__("app.util.time", fromlist=["now_ts"]).now_ts(),
                         shutdown_event=asyncio.Event())
            rt.db = db
            print(await build_digest(rt))
            return 0
    finally:
        await db.close()
    return 1


def register(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("backup", help="backups (spec 16)")
    bs = p.add_subparsers(dest="backup_cmd", required=True)
    x = bs.add_parser("list")
    x.add_argument("--mirror", action="store_true")
    x.add_argument("--manual", action="store_true")
    x = bs.add_parser("verify")
    x.add_argument("target")
    x.add_argument("--full", action="store_true")
    x = bs.add_parser("now")
    x.add_argument("--tag", default=None)
    x = bs.add_parser("prune")
    x.add_argument("--keep-daily", type=int, default=None)
    x.add_argument("--keep-weekly", type=int, default=None)
    x = bs.add_parser("restore")
    x.add_argument("stamp")
    x.add_argument("--from", dest="source", choices=("local", "mirror"), default="local")
    x.add_argument("--target", default=None)
    x.add_argument("--yes", action="store_true")
    p.set_defaults(func=lambda a: asyncio.run(_backup(a)))

    p = sub.add_parser("costs", help="cost governor (spec 16)")
    cs = p.add_subparsers(dest="costs_cmd", required=True)
    cs.add_parser("status")
    x = cs.add_parser("override")
    x.add_argument("group")
    x.add_argument("--usd", type=float, required=True)
    x.add_argument("--reason", required=True)
    x = cs.add_parser("clear-override")
    x.add_argument("group")
    x = cs.add_parser("set")
    x.add_argument("provider")
    x.add_argument("--usd", type=float, required=True)
    x.add_argument("--month", default=None)
    x = cs.add_parser("record")
    x.add_argument("provider")
    x.add_argument("--usd", type=float, required=True)
    p.set_defaults(func=lambda a: asyncio.run(_costs(a)))

    p = sub.add_parser("ops", help="ops tools (spec 16)")
    os_ = p.add_subparsers(dest="ops_cmd", required=True)
    x = os_.add_parser("digest")
    x.add_argument("--print", action="store_true")
    os_.add_parser("selfcheck")
    os_.add_parser("boots")
    os_.add_parser("integrity-check")
    x = os_.add_parser("pause")
    x.add_argument("target")
    x.add_argument("--reason", default=None)
    x.add_argument("--hours", type=float, default=None)
    x = os_.add_parser("resume")
    x.add_argument("target")
    x = os_.add_parser("rearm")
    x.add_argument("job")
    p.set_defaults(func=lambda a: asyncio.run(_ops(a)))
