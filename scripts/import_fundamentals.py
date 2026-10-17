"""Import the old bot's ``fundamentals.json`` and ``whale_cohort.json`` (spec 17 §2.6, step S2).

    python -m scripts.import_fundamentals [--fundamentals PATH] [--whale-cohort PATH] [--force] [--db PATH]

Run after the RPC eras / ledger stamp (S5) so each record is attributed to the occupant at ``generated_at``.
Idempotent (04 hash-dedupe; ``whale_cohort`` INSERT OR IGNORE). Report: ``data/backfill/fundamentals_import_report.json``.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    from app.backfill import common
    from app.backfill.fundamentals_import import run_import
    p = argparse.ArgumentParser(prog="python -m scripts.import_fundamentals", description=__doc__.split("\n")[0])
    p.add_argument("--fundamentals")
    p.add_argument("--whale-cohort")
    p.add_argument("--force", action="store_true")
    p.add_argument("--db")
    a = p.parse_args(argv)
    try:
        common.check_enabled()
        rep = asyncio.run(run_import(fundamentals=Path(a.fundamentals) if a.fundamentals else None,
                                     whale_cohort=Path(a.whale_cohort) if a.whale_cohort else None,
                                     db_path=Path(a.db) if a.db else None, force=a.force))
    except common.PreconditionError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return exc.code
    print(json.dumps({k: v for k, v in rep.items() if k != "legacy"} |
                     {"legacy": {k: v for k, v in (rep.get("legacy") or {}).items() if k != "per_netuid"}},
                     indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
