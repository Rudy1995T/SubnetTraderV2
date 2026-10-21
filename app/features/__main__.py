"""``python -m app.features --metric k1,k2 --as-of YYYY-MM-DD|today [--netuid N] [--write]`` (spec 12 §8.1).

Evaluates producer fns / derived metrics at the date's 00:15Z (now for ``today``) and prints value/status/reason per
occupant; nothing is written unless ``--write`` (then through ``put_metric_values``, keyed by the slot / hour)."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timezone

import app.config as app_config
from app.features.engine import FeatureEngine, result_row
from app.features.registry import DERIVED_KEYS, METRICS
from app.features.types import MetricResult
from app.util.time import floor_hour, now_ts, to_iso

UTC = timezone.utc


def _instant(as_of: str) -> tuple[int, int]:
    """(T_eval, key as_of): ``today`` → (now, floor_hour(now)); a date → (D 00:15Z, D 00:00Z)."""
    if as_of == "today":
        t = now_ts()
        return t, floor_hour(t)
    d = int(datetime.strptime(as_of, "%Y-%m-%d").replace(tzinfo=UTC).timestamp())
    return d + int(app_config.settings.SCORING_CRON_MINUTE) * 60, d


async def _main(a: argparse.Namespace) -> int:
    from app.storage.db import Database
    from app.storage.pit import put_metric_values
    s = app_config.settings
    keys = [k.strip() for k in a.metric.split(",") if k.strip()]
    bad = [k for k in keys if k not in METRICS]
    if bad:
        print(f"unknown metric keys: {bad}", file=sys.stderr)
        return 2
    t, key_as_of = _instant(a.as_of)
    db = Database(a.db or s.path(s.DB_PATH))
    await db.connect()
    try:
        eng = FeatureEngine(db)
        occ = [o for o in await eng.universe(t) if a.netuid is None or o.netuid == a.netuid]
        fns = eng.producer_fns()
        results: list[tuple[str, str, MetricResult]] = []
        when = datetime.fromtimestamp(t, UTC)
        for k in keys:
            if k in DERIVED_KEYS:
                continue
            fn = fns.get(k)
            if fn is None:
                print(f"{k}: no producer fn available (collector metric or later phase)", file=sys.stderr)
                continue
            for o in occ:
                try:
                    res = await fn(db, o.occupant_key, when)
                except Exception as exc:
                    res = MetricResult(None, {"reason": "fn_error", "error": f"{type(exc).__name__}: {exc}"})
                results.append((o.occupant_key, k, res))
        if any(k in DERIVED_KEYS for k in keys):
            results += [r for r in await eng.derive_results(occ, t, t, mode="strict") if r[1] in keys]
        netuid = {o.occupant_key: o.netuid for o in occ}
        print(f"T_eval {to_iso(t)}  key as_of {to_iso(key_as_of)}  occupants {len(occ)}")
        for okey, k, res in sorted(results, key=lambda r: (netuid[r[0]], r[1])):
            vj = res.mapped_json() if isinstance(res, MetricResult) else {"status": "failed", "reason": "bad_return"}
            print(f"{netuid[okey]:>4} {okey:<16} {k:<28} {vj['status']:<15} "
                  f"{'' if res.value is None else res.value!s:<14} {vj.get('reason') or ''}"
                  + (f"  {json.dumps({x: y for x, y in vj.items() if x not in ('status', 'reason')}, default=str)[:120]}"
                     if a.verbose else ""))
        if a.write and results:
            rows = [result_row(k, res, occupant_key=okey, as_of=key_as_of, observed_at=t, run_id=f"cli:{t}",
                               backfilled=False) for okey, k, res in results]
            n = await put_metric_values(db, rows)
            print(f"written: {n} rows (first success wins)")
        return 0
    finally:
        await db.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m app.features", description=__doc__.splitlines()[0])
    p.add_argument("--metric", required=True, help="comma-separated registry keys")
    p.add_argument("--as-of", default="today", help="YYYY-MM-DD (evaluated at 00:15Z) or 'today' (now)")
    p.add_argument("--netuid", type=int)
    p.add_argument("--write", action="store_true")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--db")
    return asyncio.run(_main(p.parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
