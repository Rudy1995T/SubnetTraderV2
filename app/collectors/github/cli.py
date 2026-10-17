"""``python -m app.collectors.github {normalise|resolve|org|activity|metrics|probe|crosscheck}`` (spec 06 §8).

``resolve --dry-run`` runs the real resolver against a sandbox copy of the selected occupants' rows (temp DB), so
nothing in the live tables changes; ``metrics`` prints unless ``--write``.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

import app.config as app_config


def _ref_repr(r) -> str:
    return (f"DeclaredRef(kind={r.kind!r}, owner={r.owner!r}, repo={r.repo!r}, key={r.key!r}, "
            f"placeholder={r.placeholder})")


async def _open(path: Path | None = None):
    from app.collectors.github.client import GitHubClient
    from app.http import Http
    from app.ratelimit import ProviderLimiter
    from app.storage.db import Database
    s = app_config.settings
    db = Database(path or s.path(s.DB_PATH))
    await db.connect()
    await db.apply_schema()
    http = Http(ProviderLimiter.from_settings())
    return db, http, GitHubClient(http, None, db)


async def _close(db, http) -> None:
    await http.aclose()
    await db.close()


_COPY = (
    ("occupants", "netuid IN ({n})"),
    ("subnets", "netuid IN ({n})"),
    ("identity_resolved", "occupant_key IN (SELECT occupant_key FROM live.occupants WHERE netuid IN ({n}))"),
    ("github_declared_refs", "occupant_key IN (SELECT occupant_key FROM live.occupants WHERE netuid IN ({n}))"),
    ("github_occupant_repos", "occupant_key IN (SELECT occupant_key FROM live.occupants WHERE netuid IN ({n}))"),
    ("github_repos", "repo_id IN (SELECT repo_id FROM live.github_occupant_repos WHERE occupant_key IN "
                     "(SELECT occupant_key FROM live.occupants WHERE netuid IN ({n})))"),
    ("github_orgs", "1 = 1"),
)


async def _sandbox(netuids: list[int]) -> Path:
    """A temp DB with 03's schema and copies of the live rows the resolver reads for ``netuids``."""
    from app.storage.db import Database
    s = app_config.settings
    tmpdir = Path(tempfile.mkdtemp(prefix="gh_dryrun_", dir=str(s.path(s.DATA_DIR) / "tmp")
                                   if (s.path(s.DATA_DIR) / "tmp").is_dir() else None))
    path = tmpdir / "sandbox.db"
    d = Database(path)
    await d.connect()
    await d.apply_schema()
    await d.close()
    live = s.path(s.DB_PATH)
    con = sqlite3.connect(str(path))
    try:
        con.execute("ATTACH DATABASE ? AS live", (str(live),))      # read only: SELECTs below, then DETACH
        n = ",".join(str(int(x)) for x in netuids)
        for table, where in _COPY:
            mine = [r[1] for r in con.execute(f"PRAGMA main.table_info({table})")]
            theirs = {r[1] for r in con.execute(f"PRAGMA live.table_info({table})")}
            cols = ", ".join(c for c in mine if c in theirs)
            con.execute(f"INSERT OR IGNORE INTO main.{table} ({cols}) SELECT {cols} FROM live.{table} "
                        f"WHERE {where.format(n=n)}")
        con.commit()
        con.execute("DETACH DATABASE live")
    finally:
        con.close()
    return path


async def cmd_resolve(netuid: int | None, dry_run: bool) -> int:
    from app.collectors.github.resolver import Resolver
    nets = {netuid} if netuid is not None else None
    path = await _sandbox([netuid] if netuid is not None else list(range(0, 1024))) if dry_run else None
    db, http, client = await _open(path)
    try:
        res = Resolver(db, client, run_id=f"cli:{uuid.uuid4().hex[:8]}", now=int(time.time()),
                       deadline=time.time() + 1700)
        rep = await res.run(netuids=nets)
        for line in rep.trace:
            print(line)
        print(json.dumps({"refs": rep.refs, "attempted": rep.attempted, "failed": rep.failed, "left": rep.left,
                          "events": rep.events, "queue": rep.queue, "dry_run": dry_run}, indent=1))
        if dry_run:
            for e in await db.fetchall("SELECT event_type, payload FROM events ORDER BY id"):
                print(f"event (sandbox only): {e['event_type']} {e['payload'][:200]}")
    finally:
        await _close(db, http)
        if path is not None:
            for p in path.parent.glob("*"):
                p.unlink(missing_ok=True)
            path.parent.rmdir()
    return 0


async def cmd_org(owner: str) -> int:
    from app.collectors.github.models import RepoRow, repo_fields
    from app.collectors.github.orgs import enumerate_owner, pick_primary
    db, http, client = await _open()
    try:
        lst = await enumerate_owner(client, owner)
        print(f"{owner}: kind={lst.kind} repos={len(lst.repos)} ok={lst.ok} cached={lst.from_cache} {lst.error or ''}")
        rows = []
        for p in lst.repos:
            f = repo_fields(p)
            rows.append(RepoRow(f["repo_id"], f["full_name"], f["owner"], f["name"], bool(f["is_fork"]),
                                bool(f["archived"]), f["pushed_at"], f["created_at"], f["stars"], f["description"]))
        from app.util.time import to_iso
        for r in sorted(rows, key=lambda r: -(r.pushed_at or 0))[:30]:
            print(f"  {r.repo_id:>11} {r.full_name:<50} pushed={to_iso(r.pushed_at)} fork={int(r.is_fork)} "
                  f"archived={int(r.archived)} stars={r.stars}")
        prim = pick_primary(rows, -1, None, int(time.time()))
        print(f"primary (no netuid/name bonus): {prim.full_name if prim else None}")
    finally:
        await _close(db, http)
    return 0


async def _netuid_set(db, netuid: int | None) -> set[int] | None:
    return {netuid} if netuid is not None else None


async def cmd_activity(netuid: int | None) -> int:
    from app.collectors.github.activity import run_activity
    db, http, client = await _open()
    try:
        out = await run_activity(db, client, run_id=f"cli:{uuid.uuid4().hex[:8]}", now=int(time.time()),
                                 deadline=time.time() + 10_000, netuids=await _netuid_set(db, netuid))
        print(json.dumps(out, indent=1, default=str))
    finally:
        await _close(db, http)
    return 0


async def cmd_metrics(netuid: int | None, now_iso: str | None, write: bool) -> int:
    from app.collectors.github.metrics import run_metrics
    from app.util.time import parse_ts
    db, http, client = await _open()
    try:
        now = parse_ts(now_iso) if now_iso else int(time.time())
        out = await run_metrics(db, now=now, run_id=f"cli:{uuid.uuid4().hex[:8]}", netuids=await _netuid_set(db, netuid),
                                write=write)
        for r in out.pop("results"):
            print(f"SN{r.netuid} {r.occupant_key}")
            for o in r.observations:
                v = "None" if o.value is None else f"{o.value:.4g}"
                print(f"  {o.metric_key:<24} {o.status:<15} {v:>8}  {o.reason or ''}")
            if r.pushed_row:
                print(f"  {'repo_pushed_days':<24} {r.pushed_row[1].get('status'):<15} "
                      f"{'None' if r.pushed_row[0] is None else f'{r.pushed_row[0]:.4g}':>8}")
        print(json.dumps(out, indent=1, default=str))
    finally:
        await _close(db, http)
    return 0


async def cmd_probe(netuid: int | None) -> int:
    from app.collectors.github import html_probe
    db, http, client = await _open()
    try:
        out = await html_probe.run(db, client, now=int(time.time()), deadline=time.time() + 3000,
                                   netuids=await _netuid_set(db, netuid))
        for line in out.pop("lines"):
            print(line)
        print(json.dumps(out, indent=1, default=str))
    finally:
        await _close(db, http)
    return 0


async def cmd_crosscheck() -> int:
    from app.collectors.chain_tmc import TmcClient
    from app.collectors.github import tmc_crosscheck
    db, http, _ = await _open()
    try:
        out = await tmc_crosscheck.run(db, TmcClient(http), now=int(time.time()))
        print(json.dumps(out, indent=1, default=str))
    finally:
        await _close(db, http)
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m app.collectors.github")
    sub = p.add_subparsers(dest="cmd", required=True)
    n = sub.add_parser("normalise")
    n.add_argument("raw")
    r = sub.add_parser("resolve")
    r.add_argument("--netuid", type=int)
    r.add_argument("--dry-run", action="store_true")
    o = sub.add_parser("org")
    o.add_argument("owner")
    a = sub.add_parser("activity")
    a.add_argument("--netuid", type=int)
    m = sub.add_parser("metrics")
    m.add_argument("--netuid", type=int)
    m.add_argument("--now")
    m.add_argument("--write", action="store_true")
    pr = sub.add_parser("probe")
    pr.add_argument("--netuid", type=int)
    sub.add_parser("crosscheck")
    args = p.parse_args(argv)
    if args.cmd == "normalise":
        from app.collectors.github.urlnorm import normalise
        print("[" + ", ".join(_ref_repr(x) for x in normalise(args.raw)) + "]")
        return 0
    runners: dict[str, Any] = {
        "resolve": lambda: cmd_resolve(args.netuid, args.dry_run),
        "org": lambda: cmd_org(args.owner),
        "activity": lambda: cmd_activity(args.netuid),
        "metrics": lambda: cmd_metrics(args.netuid, args.now, args.write),
        "probe": lambda: cmd_probe(args.netuid),
        "crosscheck": cmd_crosscheck,
    }
    return asyncio.run(runners[args.cmd]())


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
