"""``python -m app db check|migrate|stats|retention|explain`` (spec 03 §8)."""
from __future__ import annotations

import argparse
import asyncio
import json

import app.config as app_config


async def _open(migrate: bool = True):
    from app.storage.db import Database
    s = app_config.settings
    db = Database(s.path(s.DB_PATH))
    await db.connect()
    if migrate:
        await db.apply_schema()
    return db


async def _cmd(args: argparse.Namespace) -> int:
    from app.util.time import now_ts
    if args.db_cmd == "migrate":
        from app.storage.migrate import migrate
        db = await _open(migrate=False)
        try:
            res = await migrate(db)
            print(f"schema_version={res.version} applied={res.applied} warnings={res.warnings}")
        finally:
            await db.close()
        return 0
    db = await _open()
    try:
        if args.db_cmd == "stats":
            st = await db.stats()
            try:
                rows = await db.fetchall("SELECT name, SUM(pgsize) AS bytes FROM dbstat GROUP BY name "
                                         "ORDER BY bytes DESC LIMIT 25")
                st["tables"] = {r["name"]: r["bytes"] for r in rows}
            except Exception:
                pass
            print(json.dumps(st, indent=1))
            return 0
        if args.db_cmd == "check":
            qc = [list(r.values())[0] for r in await db.fetchall("PRAGMA quick_check", writer=True)]
            fk = await db.fetchall("PRAGMA foreign_key_check", writer=True)
            orphans = {}
            for t in ("metric_values", "scores", "chain_hourly", "chain_daily", "events"):
                try:
                    orphans[t] = await db.fetchval(
                        f"SELECT COUNT(*) FROM {t} x WHERE x.occupant_key IS NOT NULL AND NOT EXISTS "
                        f"(SELECT 1 FROM occupants o WHERE o.occupant_key = x.occupant_key)")
                except Exception as exc:
                    orphans[t] = f"error: {exc}"
            ok = qc == ["ok"] and not fk
            print(json.dumps({"quick_check": qc[:5], "foreign_key_check": fk[:20], "orphans": orphans,
                              "schema_version": db.schema_version, "schema_warnings": db.schema_warnings}, indent=1))
            return 0 if ok else 1
        if args.db_cmd == "retention":
            from app.storage.retention import run_retention
            only = args.only.split(",") if args.only else None
            res = await run_retention(db, now=now_ts(), dry_run=args.dry_run, only=only)
            print(json.dumps(res, indent=1))
            return 0
        if args.db_cmd == "explain":
            for r in await db.fetchall("EXPLAIN QUERY PLAN " + args.sql):
                print(r)
            return 0
    finally:
        await db.close()
    return 1


def register(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("db", help="database maintenance (spec 03)")
    ds = p.add_subparsers(dest="db_cmd", required=True)
    ds.add_parser("check")
    ds.add_parser("migrate")
    ds.add_parser("stats")
    r = ds.add_parser("retention")
    r.add_argument("--dry-run", action="store_true")
    r.add_argument("--only", default=None)
    e = ds.add_parser("explain")
    e.add_argument("sql")
    p.set_defaults(func=lambda a: asyncio.run(_cmd(a)))
