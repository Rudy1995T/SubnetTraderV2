"""Historical metric replay (spec 17 §2.11, step S10; spec 20 N2 item 2).

    python -m scripts.replay_history [--owners chain,github,integrity,scoring] [--from YYYY-MM-DD] [--to YYYY-MM-DD]
        [--limit-days N] [--redo] [--spec ts-1.0] [--db PATH]

Per owner and official slot ``D`` (00:00Z), oldest first, the owners' own entry points write ``backfilled = 1`` rows
(``as_of = D``, ``observed_at`` = actual write time, ``spec_version = SPEC_VERSION``, the owner's ``source``):
- chain: 05's ``run_slot(as_of=D, backfilled=1)`` (historical snapshot tier); a day with raw ``emission_share`` rows
  already is skipped, as in 05's catch-up.
- github: 06's ``P2Metrics.compute(occ, now=D − 1, replay=True)`` (= the 23:30Z live run's slot rule) for every
  occupant alive at D; activity keys need 17 S9's repo coverage (``data/backfill/github_progress.json``), else
  ``backfill_incomplete``. Metric rows only (no ``github_authors_daily``, no events: 17 §2.8).
- integrity: 11's ``compute_occupant(t=D, mode='reconstructed')``; only ``owner_commitment`` and
  ``control_stability_90d`` are written (17 §2.11; the other P4 keys are forward-only, 13 §2.5).
- scoring: 12's ``ScoringService.backfill`` (its own skip of scored slots; stops before the first live slot).
Occupants alive at D come from ``occupants`` (registered ≤ D < deregistered), never today's ``subnets`` row, so dead
occupants are replayed and no owner key leaks back in time. Resumable per owner via
``job_state('replay_history', 'last_day:<owner>')``; ``--redo`` ignores the watermark (rows stay first-success-wins).
Exit codes: 0 ok, 2 precondition, 130 interrupted.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Callable

import app.config as app_config

OWNERS = ("chain", "github", "integrity", "scoring")
DEFAULT_FROM = {"chain": "2025-02-14", "integrity": "2025-02-14", "scoring": "2025-09-22"}   # github: SINCE setting
INTEGRITY_KEYS = ("owner_commitment", "control_stability_90d")
DAY = 86400
JOB = "replay_history"


def day_ts(s: str) -> int:
    d = date.fromisoformat(s)
    return int(datetime(d.year, d.month, d.day, tzinfo=UTC).timestamp())


async def default_to(db) -> int:
    """The day before the first live (non-backfilled) score, else yesterday."""
    from app.util.time import floor_day, now_ts
    first_live = await db.fetchval("SELECT MIN(as_of) FROM scores WHERE backfilled = 0", writer=True)
    return (floor_day(int(first_live)) if first_live is not None else floor_day(now_ts())) - DAY


async def watermark(db, owner: str) -> int | None:
    v = await db.fetchval("SELECT value_json FROM job_state WHERE job = ? AND key = ?", (JOB, f"last_day:{owner}"),
                          writer=True)
    return int(json.loads(v)) if v is not None else None


async def set_watermark(db, owner: str, d: int) -> None:
    from app.util.time import now_ts
    await db.execute("INSERT INTO job_state (job, key, value_json, updated_at) VALUES (?,?,?,?) ON CONFLICT (job, key) "
                     "DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at",
                     (JOB, f"last_day:{owner}", json.dumps(d), now_ts()))


async def occupants_alive(db, d: int) -> list[dict]:
    return await db.fetchall(
        "SELECT * FROM occupants WHERE netuid != 0 AND COALESCE(registered_ts, registered_at_est) <= ? "
        "AND (COALESCE(deregistered_ts, deregistered_at) IS NULL OR COALESCE(deregistered_ts, deregistered_at) > ?) "
        "ORDER BY netuid", (d, d), writer=True)


def github_coverage(path: Path) -> Callable[[int, int], bool]:
    """17 §2.8 sidecar (flat ``{repo_id: unit, "_meta": …}``) → ``coverage(repo_id, since)``: commits done and not
    truncated after ``since``; a repo gone on GitHub (404 on both path forms) has nothing left to fetch → covered."""
    units = json.loads(path.read_text()) if path.exists() else {}

    def cov(repo_id: int, since: int) -> bool:
        u = units.get(str(repo_id))
        if not u or u.get("commits") not in ("done", "gone"):
            return False
        return not u.get("truncated") or (u.get("oldest_seen") is not None and int(u["oldest_seen"]) <= since)
    return cov


# ── owners ───────────────────────────────────────────────────────────────────────────────────────────────────
async def replay_chain(db, d: int, run_id: str) -> int:
    from app.features.chain_features import run_slot
    from app.util.time import now_ts
    if await db.fetchval("SELECT 1 FROM metric_values WHERE metric_key = 'emission_share' AND as_of = ? "
                         "AND stage = 'raw' LIMIT 1", (d,), writer=True):
        return 0
    return (await run_slot(db, d, run_id=run_id, observed_at=now_ts(), backfilled=1)).n_rows


async def replay_github(db, d: int, run_id: str, *, calc, cov) -> int:
    from app.collectors.github.metrics import metric_rows, write_metric_values
    from app.registry.models import Occupant
    from app.util.time import now_ts
    rows: list[dict] = []
    for o in await occupants_alive(db, d):
        r = await calc.compute(Occupant.from_row(o), d - 1, replay=True, coverage=cov)
        rows += metric_rows(o["occupant_key"], r.observations, as_of=d, now=now_ts(), run_id=run_id, backfilled=True)
    return await write_metric_values(db, rows)


async def replay_integrity(db, d: int, run_id: str) -> int:
    from app.integrity.features import compute_occupant
    from app.storage.pit import put_metric_values
    from app.util.time import now_ts
    s = app_config.settings
    rows: list[dict] = []
    for o in await occupants_alive(db, d):
        for r in await compute_occupant(db, dict(o), t=d, mode="reconstructed"):
            if r.key in INTEGRITY_KEYS:
                rows.append(r.to_row(occupant_key=o["occupant_key"], as_of=d, observed_at=now_ts(), run_id=run_id,
                                     spec_version=s.SPEC_VERSION, backfilled=True))
    return await put_metric_values(db, rows)


# ── driver ───────────────────────────────────────────────────────────────────────────────────────────────────
async def run_owner(db, owner: str, *, d0: int, d1: int, redo: bool, limit: int | None, run_id: str,
                    spec: str) -> dict:
    from app.backfill import common
    from app.util.time import to_iso
    out = {"owner": owner, "from": to_iso(d0), "to": to_iso(d1), "days": 0, "rows": 0, "stopped": None}
    if owner == "scoring":
        from app.scoring.engine import ScoringService

        def progress(d: int, r: dict) -> None:
            print(f"scoring {to_iso(d)} n={r['specs'].get(spec, {}).get('n_scored')}", flush=True)
        res = await ScoringService(db).backfill(spec, datetime.fromtimestamp(d0, UTC).date(),
                                                datetime.fromtimestamp(d1, UTC).date(), progress=progress)
        out.update(days=res["slots"], rows=res["rows"], skipped=res["skipped"])
        return out
    kw: dict = {}
    if owner == "github":
        from app.collectors.github.metrics import P2Metrics
        s = app_config.settings
        side = s.path(s.DATA_DIR) / "backfill" / "github_progress.json"
        if not side.exists():
            raise common.PreconditionError(f"{side} missing: run 17 S9 (scripts.backfill_github) first")
        kw = {"calc": P2Metrics(db), "cov": github_coverage(side)}
    fn = {"chain": replay_chain, "github": replay_github, "integrity": replay_integrity}[owner]
    last = None if redo else await watermark(db, owner)
    d = max(d0, last + DAY) if last is not None else d0
    while d <= d1:
        if common.stop_requested():
            out["stopped"] = "signal" if common.signalled() else "pause_all"
            break
        out["rows"] += await (fn(db, d, run_id, **kw) if kw else fn(db, d, run_id))
        await set_watermark(db, owner, d)
        out["days"] += 1
        if out["days"] % 10 == 0 or d == d1:
            print(f"{owner} {to_iso(d)} days={out['days']} rows={out['rows']}", flush=True)
        if limit is not None and out["days"] >= limit:
            break
        d += DAY
    return out


async def _run(a: argparse.Namespace) -> int:
    from app.backfill import common
    s = app_config.settings
    db_path = Path(a.db) if a.db else None
    owners = [o.strip() for o in a.owners.split(",") if o.strip()]
    bad = [o for o in owners if o not in OWNERS]
    if bad:
        print(f"refused: unknown owner(s) {bad}; choose from {','.join(OWNERS)}", file=sys.stderr)
        return common.EXIT_PRECONDITION
    common.install_signal_handlers()
    db = await common.prepare_db(db_path)
    con = common.open_db(db_path)
    code = 0
    try:
        d1 = day_ts(a.to) if a.to else await default_to(db)
        for owner in sorted(owners, key=OWNERS.index):
            start = a.start or DEFAULT_FROM.get(owner) or s.BACKFILL_GITHUB_SINCE
            job = f"{JOB}:{owner}"
            try:
                with common.step_run(con, job) as run:
                    out = await run_owner(db, owner, d0=day_ts(start), d1=d1, redo=a.redo, limit=a.limit_days,
                                          run_id=run.run_id, spec=a.spec or s.SPEC_VERSION)
                    run.rows, run.meta = out["rows"], out
                    run.note = f"days={out['days']} rows={out['rows']}" + (
                        f" stopped={out['stopped']}" if out["stopped"] else "")
            except common.PreconditionError as exc:
                print(f"refused: {exc}", file=sys.stderr)
                code = code or exc.code
                continue
            print(json.dumps(out), flush=True)
            if out["stopped"]:
                return common.EXIT_INTERRUPTED if out["stopped"] == "signal" else 0
        return code
    finally:
        con.close()
        await db.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m scripts.replay_history", description=__doc__.split("\n")[0])
    p.add_argument("--owners", default=",".join(OWNERS))
    p.add_argument("--from", dest="start", help="first day (default per owner: chain/integrity 2025-02-14, "
                                                "github BACKFILL_GITHUB_SINCE, scoring 2025-09-22)")
    p.add_argument("--to", help="last day (default: the day before the first live score)")
    p.add_argument("--limit-days", type=int, help="at most N days per owner (testing)")
    p.add_argument("--redo", action="store_true", help="ignore the watermark (rows stay first-success-wins)")
    p.add_argument("--spec", help="scoring spec version (default SPEC_VERSION)")
    p.add_argument("--db", help="database path (default DB_PATH)")
    return asyncio.run(_run(p.parse_args(argv)))


if __name__ == "__main__":
    sys.exit(main())
