"""Build ``data/quant/coverage.json`` (spec 17 §2.9, step S8) — the coverage-1 contract read by spec 13.

    python -m scripts.build_coverage [--db PATH] [--out PATH]

Read-only; safe to run at any time (re-run after each step). Exit 1 leaves the previous file untouched.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    from app.backfill import coverage
    p = argparse.ArgumentParser(prog="python -m scripts.build_coverage", description=__doc__.split("\n")[0])
    p.add_argument("--db")
    p.add_argument("--out")
    a = p.parse_args(argv)
    try:
        doc = coverage.build(a.db)
    except Exception as exc:
        print(f"coverage build failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    out = coverage.write(doc, Path(a.out) if a.out else None)
    summary = {"out": str(out), "eras": {k: v.get("status") if isinstance(v, dict) and "status" in v else v
                                         for k, v in doc["eras"].items()},
               "pool_history_rows": doc["series"]["pool_history"]["rows"],
               "price_bars_1h_rows": doc["series"]["price_bars_1h"]["rows"],
               "holes": len(doc["holes"]), "unstamped": doc["occupancy"]["unstamped"], "checks": doc["checks"],
               "caveats": doc["caveats"]}
    print(json.dumps(summary, indent=1, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
