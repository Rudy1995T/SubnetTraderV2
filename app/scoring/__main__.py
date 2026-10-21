"""``python -m app.scoring`` (spec 12 §8.1): registry check, run (incl. --dry-run), explain, rescore, backfill,
stability. All times INTEGER unix seconds inside; ISO only on the command line and in printed output."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date

import app.config as app_config
from app.util.time import now_ts, parse_ts, to_iso

DAY = 86400


def _registry(a: argparse.Namespace) -> int:
    from app.features import registry as R
    from app.scoring.golden import GOLDEN_REGISTRY_HASH
    from app.scoring.spec import GOLDEN_SPEC_HASH, TS_1_0, spec_hash
    if a.hash:
        print(R.registry_hash())
        return 0
    R._validate()
    per = {p: len(R.scored(p)) for p in R.PILLARS}
    legs = sum(d.role == "leg" for d in R.METRICS.values())
    aux = sum(d.role == "aux" for d in R.METRICS.values())
    h = R.registry_hash()
    ok = h == GOLDEN_REGISTRY_HASH["ts-1.0"] and spec_hash(TS_1_0) == GOLDEN_SPEC_HASH["ts-1.0"]
    print(f"registry ts-1.0: {len(R.METRICS)} metrics ({sum(per.values())} scored: "
          + ", ".join(f"{p} {n}" for p, n in per.items()) + f"; {legs} legs; {aux} aux); pillar sums OK; "
          f"hash {h[:16]}… {'==' if ok else '!='} golden")
    return 0 if ok else 1


async def _db():
    from app.storage.db import Database
    s = app_config.settings
    db = Database(s.path(s.DB_PATH))
    await db.connect()
    return db


def _slot_arg(v: str | None) -> int | None:
    return None if v is None else int(parse_ts(v if "T" in v else v + "T00:00:00Z"))


async def _run(a: argparse.Namespace) -> int:
    from app import control
    from app.scoring.engine import ScoringService, cron_hours, make_slot, slot_for
    s = app_config.settings
    db = await _db()
    try:
        svc = ScoringService(db)
        t0 = now_ts()
        slot = _slot_arg(a.slot) if a.slot else slot_for(t0, cron_hours())
        sl = make_slot(slot, t0, trigger="cli")
        spec = a.spec or s.SPEC_VERSION
        if not a.dry_run and not control.scoring_killed():
            latest = await svc.latest_completed(spec)
            if latest is not None and slot < latest:
                print(f"refused: {to_iso(slot)} is older than {spec}'s latest completed slot {to_iso(latest)} "
                      "(flag state would regress; use backfill for history)", file=sys.stderr)
                return 3
        res = await svc.run_slot(sl, t0=t0, specs=[spec], dry_run=a.dry_run)
        sp = res["specs"].get(spec) or {}
        if a.dry_run:
            rows = sorted(sp.get("rows") or [], key=lambda r: r["netuid"])
            print(f"{to_iso(slot)} {sl.run_kind} {sl.mode} regime={sp.get('regime')} v_w={sp.get('v_w')} "
                  f"n={len(rows)} (dry run: nothing written; fn/derived metrics read from stored rows only)")
            print(f"{'netuid':>6} {'tier':>4} {'smooth':>7} {'health':>7} {'vel':>6} {'conf':>5}  flags")
            f = lambda x, n=1: "" if x is None else f"{x:.{n}f}"  # noqa: E731
            for r in rows:
                print(f"{r['netuid']:>6} {r['tier'] or '-':>4} {f(r['composite_smoothed']):>7} {f(r['quality']):>7} "
                      f"{f(r['velocity']):>6} {f(r['confidence'], 2):>5}  {','.join(json.loads(r['red_flags']))}"
                      + ("  [insufficient]" if r["insufficient_data"] else "")
                      + ("  [pre_launch]" if r["status"] == "pre_launch" else ""))
            return 0
        print(json.dumps({k: v for k, v in res.items() if k != "specs"} |
                         {"spec": {k: v for k, v in sp.items() if k != "rows"}}, default=str))
        return 0 if not res.get("error") else 1
    finally:
        await db.close()


async def _explain(a: argparse.Namespace) -> int:
    s = app_config.settings
    db = await _db()
    try:
        spec = a.spec or s.SPEC_VERSION
        q = "SELECT * FROM scores WHERE netuid = ? AND spec_version = ?"
        args: list = [a.netuid, spec]
        if a.slot:
            q += " AND as_of = ?"
            args.append(_slot_arg(a.slot))
        r = await db.fetchone(q + " ORDER BY as_of DESC LIMIT 1", args)
        if r is None:
            print(f"no {spec} score for netuid {a.netuid}" + (f" at {a.slot}" if a.slot else ""), file=sys.stderr)
            return 1
        ex = json.loads(r["explain"])
        if a.json:
            print(json.dumps(ex, indent=1, default=str))
            return 0
        print(f"SN{r['netuid']} {r['occupant_key']}  {to_iso(r['as_of'])} {r['run_kind']} {ex.get('mode')}  "
              f"status={r['status']}  tier={r['tier']}  smoothed={ex.get('composite_smoothed')}")
        print(f"  health={ex.get('health')}  velocity={ex['velocity']['value']}  composite_raw={ex.get('composite_raw')}"
              f"  × flags {ex.get('flag_multiplier')} = {ex.get('composite')}  confidence={ex.get('confidence')}")
        print(f"  regime={ex['regime']['name']} (v_w {ex.get('v_w')})")
        for p, b in ex["pillars"].items():
            print(f"  {p}: score={b['score']} weight={b['weight']} points={b['points']} cov={ex['coverage'].get(p)}")
        print("  top +: " + ", ".join(f"{x['metric']} {x['impact']:+.3f}" for x in ex["top_positive"]))
        print("  top −: " + ", ".join(f"{x['metric']} {x['impact']:+.3f}" for x in ex["top_negative"]))
        for fl in ex["flags"]:
            print(f"  flag {fl['flag']} ×{fl['multiplier']} since {to_iso(fl['since'])} {fl['state']} {fl['origin']}")
        print("  missing: " + ", ".join(ex["missing"]))
        return 0
    finally:
        await db.close()


async def _rescore(a: argparse.Namespace) -> int:
    from app.scoring.engine import ScoringRefused, ScoringService
    db = await _db()
    try:
        d = None if a.slot in (None, "now") else date.fromisoformat(a.slot)
        try:
            rid = await ScoringService(db).rescore(d, a.spec, a.shadow)
        except ScoringRefused as exc:
            print(f"refused: {exc.code} {exc}", file=sys.stderr)
            return 3
        print(rid)
        return 0
    finally:
        await db.close()


async def _backfill(a: argparse.Namespace) -> int:
    from app.scoring.engine import ScoringService
    db = await _db()
    try:
        def progress(d: int, res: dict) -> None:
            sp = res["specs"].get(a.spec) or {}
            print(f"{to_iso(d)} scored={sp.get('n_scored')} regime={sp.get('regime')}", flush=True)
        out = await ScoringService(db).backfill(a.spec, date.fromisoformat(a.start),
                                                date.fromisoformat(a.end) if a.end else None, progress=progress)
        print(json.dumps(out))
        return 0
    finally:
        await db.close()


async def _stability(a: argparse.Namespace) -> int:
    s = app_config.settings
    db = await _db()
    try:
        rows = await db.fetchall("SELECT * FROM rank_stability_daily WHERE spec_version = ? AND as_of >= ? "
                                 "ORDER BY as_of DESC, series, lag_days", (a.spec or s.SPEC_VERSION,
                                                                           now_ts() - a.days * DAY))
        for r in rows:
            f = lambda x: "  -  " if x is None else f"{x:.3f}"  # noqa: E731
            print(f"{to_iso(r['as_of'])} {r['series']:<18} lag {r['lag_days']} n={r['n']:<4} rho={f(r['rank_autocorr'])} "
                  f"q5={f(r['quintile_turnover'])} top20={f(r['top20_turnover'])}")
        if not rows:
            print("no rank_stability_daily rows")
        return 0
    finally:
        await db.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m app.scoring")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("registry")
    r.add_argument("--check", action="store_true")
    r.add_argument("--hash", action="store_true")
    r = sub.add_parser("run")
    r.add_argument("--slot", help="ISO instant of the slot (default: the current slot)")
    r.add_argument("--dry-run", action="store_true")
    r.add_argument("--spec")
    r = sub.add_parser("explain")
    r.add_argument("netuid", type=int)
    r.add_argument("--slot")
    r.add_argument("--spec")
    r.add_argument("--json", action="store_true")
    r = sub.add_parser("rescore")
    r.add_argument("--slot", help="YYYY-MM-DD (official slot) or 'now'")
    r.add_argument("--spec", required=True)
    r.add_argument("--shadow", action="store_true")
    r = sub.add_parser("backfill")
    r.add_argument("--spec", required=True)
    r.add_argument("--from", dest="start", required=True)
    r.add_argument("--to", dest="end")
    r = sub.add_parser("stability")
    r.add_argument("--days", type=int, default=14)
    r.add_argument("--spec")
    a = p.parse_args(argv)
    if a.cmd == "registry":
        return _registry(a)
    fn = {"run": _run, "explain": _explain, "rescore": _rescore, "backfill": _backfill, "stability": _stability}[a.cmd]
    return asyncio.run(fn(a))


if __name__ == "__main__":
    raise SystemExit(main())
