"""Regime history from ``price_bars`` 1h into ``regime_daily`` (spec 17 §2.7, step S7).

    python -m scripts.backfill_regime [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--db PATH]

Exit 3 if spec 12's ``app/quant/regime.py`` port is missing.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    import app.config as app_config
    from app.backfill import common, regime_backfill
    from app.util.time import parse_iso
    p = argparse.ArgumentParser(prog="python -m scripts.backfill_regime", description=__doc__.split("\n")[0])
    p.add_argument("--from", dest="from_day")
    p.add_argument("--to", dest="to_day")
    p.add_argument("--db")
    a = p.parse_args(argv)
    s = app_config.settings
    try:
        common.check_enabled()
        compute, params, v_w = regime_backfill.load_regime()
    except (common.PreconditionError, regime_backfill.RegimeUnavailable) as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 3
    db_path = Path(a.db) if a.db else None
    import asyncio
    asyncio.run(_prep(db_path))
    con = common.open_db(db_path)
    try:
        start = a.from_day or s.REGIME_BACKFILL_START
        with common.step_run(con, "backfill_regime") as run:
            rep = regime_backfill.backfill_regime(
                con, start_day=parse_iso(start + "T00:00:00Z") if start else None,
                end_day=parse_iso(a.to_day + "T00:00:00Z") if a.to_day else None, params=params,
                spec_version=s.SPEC_VERSION, run_id=run.run_id, compute=compute, v_w_by_regime=v_w)
            run.rows, run.meta = rep["written"], rep
    finally:
        con.close()
    print(json.dumps(rep, indent=1))
    common.emit_step_event_sync(db_path, "backfill_done", job="backfill_regime", era=None, run_id=run.run_id,
                                rows=rep["written"], seconds=run.seconds)
    return 0


async def _prep(db_path):
    from app.backfill import common
    db = await common.prepare_db(db_path)
    await db.close()


if __name__ == "__main__":
    sys.exit(main())
