"""``python -m app.collectors.web`` (spec 07 §2.8, §10).

``--once`` runs one ``web_check`` pass in-process and prints one JSON line per row (``--dry-run``: nothing written;
``--force``: ignore the due filter — today's stored rows are still never replaced). ``--metrics`` prints the six P3
results per occupant and writes only with ``--write``; ``--as-of YYYY-MM-DD`` replays a past day (``backfilled=1``).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import uuid
from datetime import datetime, timezone

import app.config as app_config

_PRINT_SKIP = {"content_text", "redirect_chain", "observed_at", "run_id", "source"}


async def _open():
    from app.http import Http
    from app.ratelimit import ProviderLimiter
    from app.storage.db import Database
    s = app_config.settings
    db = Database(s.path(s.DB_PATH))
    await db.connect()
    await db.apply_schema()
    limiter = ProviderLimiter.from_settings()
    return db, Http(limiter), limiter


async def _once(args) -> int:
    from app.collectors.web.check import CheckContext, run_check
    from app.collectors.web.classify import WebHeuristicsConfig
    from app.collectors.web.robots import RobotsCache
    from app.collectors.web.wayback import WaybackClient
    from app.util.time import now_ts
    s = app_config.settings
    db, http, limiter = await _open()
    try:
        state = {}
        for k in ("wayback_backoff_until", "wayback_last_ok"):
            r = await db.fetchone("SELECT value_json FROM job_state WHERE job = 'web_check' AND key = ?", (k,))
            state[k] = json.loads(r["value_json"]) if r else None
        now = now_ts()
        ctx = CheckContext(db=db, http=http, limiter=limiter, s=s, run_id=f"cli-{uuid.uuid4().hex[:12]}",
                           started_at=now, deadline=float(now + 3600), robots=RobotsCache(http, s.WEB_ROBOTS_CACHE_DAYS),
                           wayback=WaybackClient(http), heur=WebHeuristicsConfig.from_settings(s), state=state,
                           dry_run=args.dry_run)

        def show(row: dict) -> None:
            print(json.dumps({k: v for k, v in row.items() if k not in _PRINT_SKIP}, default=str), flush=True)

        t0 = time.monotonic()
        rep = await run_check(ctx, netuids=set(args.netuid) if args.netuid else None, url=args.url, force=args.force,
                              on_row=show)
        summ = rep.summary()
        summ["duration_s"] = round(time.monotonic() - t0, 1)
        print(json.dumps({"web_check_done": summ}, default=str), file=sys.stderr)
        return 0
    finally:
        await http.aclose()
        await db.close()


async def _metrics(args) -> int:
    from app.features.web_metrics import run_web_metrics
    from app.util.time import now_ts
    db, http, _ = await _open()
    try:
        if args.as_of:
            d = datetime.strptime(args.as_of, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            t_eval, backfilled = int(d.timestamp()) + 5 * 60, True
        else:
            t_eval, backfilled = now_ts(), False
        out = await run_web_metrics(db, t_eval=t_eval, run_id=f"cli-{uuid.uuid4().hex[:12]}", observed_at=now_ts(),
                                    backfilled=backfilled, netuids=set(args.netuid) if args.netuid else None,
                                    write=args.write)
        for okey, res in out.pop("results").items():
            print(json.dumps({"occupant_key": okey,
                              **{k: {"value": r.value, **r.mapped_json()} for k, r in res.items()}}, default=str))
        for e in out.pop("pending_events"):
            print(json.dumps({"event": e}, default=str))
        print(json.dumps({"web_metrics": out}, default=str), file=sys.stderr)
        return 0
    finally:
        await http.aclose()
        await db.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m app.collectors.web")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="one web_check pass")
    mode.add_argument("--metrics", action="store_true", help="compute the six P3 metrics (print; --write stores)")
    ap.add_argument("--netuid", type=int, action="append")
    ap.add_argument("--url")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--as-of")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args(argv)
    return asyncio.run(_once(args) if args.once else _metrics(args))
