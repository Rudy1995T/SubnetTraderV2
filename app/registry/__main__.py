"""``python -m app.registry review [--reason R] [--json]`` (spec 04 §2.10; Phase-1 transport before 14)."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys

import app.config as app_config


async def _review(reason: str | None, as_json: bool) -> int:
    from app.registry.review import compute_needs_review
    from app.storage.db import Database
    from app.util.time import now_ts, to_iso
    s = app_config.settings
    db = Database(s.path(s.DB_PATH))
    await db.connect()
    await db.apply_schema()
    try:
        items = [i for i in await compute_needs_review(db, now_ts()) if not reason or i.reason == reason]
    finally:
        await db.close()
    if as_json:
        print(json.dumps([i.as_dict() for i in items], indent=1))
        return 0
    counts: dict[str, int] = {}
    for i in items:
        counts[i.reason] = counts.get(i.reason, 0) + 1
    for i in items:
        sn = f"SN{i.netuid}" if i.netuid is not None else "-"
        print(f"{i.reason:<26} {sn:<6} {(i.name or '')[:22]:<22} {i.detail[:80]}  {to_iso(i.since) or ''}")
    print("\n" + " · ".join(f"{k} {v}" for k, v in counts.items()) + f" · total {len(items)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m app.registry")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("review")
    r.add_argument("--reason", default=None)
    r.add_argument("--json", action="store_true")
    a = p.parse_args(argv)
    return asyncio.run(_review(a.reason, a.json))


if __name__ == "__main__":
    sys.exit(main())
