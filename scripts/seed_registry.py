"""First population of the registry (spec 04 §2.11).

    python -m scripts.seed_registry [--dry-run] [--no-network] [--legacy-only]
        [--fundamentals seed/fundamentals.json] [--edge-notes seed/subnet_edge]
        [--legacy-as-of 2026-06-02T11:03:41Z] [--edge-as-of 2026-06-18T14:37:08Z]
        [--report data/seed_registry_report.json]

Idempotent. Runs the occupant and identity jobs once through 01's wrapper (same limiter/provider ids), imports the
legacy fundamentals + edge notes when present, rebuilds watch channels and writes a JSON report.
While the service is running, keep this small script's TMC use in mind (2 requests; the per-IP bucket is shared).
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import app.config as app_config


async def _run(a: argparse.Namespace) -> int:
    import app.runtime as runtime_mod
    from app.jobs import SPEC_BY_ID
    from app.registry.jobs import RegistryIdentityJob, RegistryOccupantsJob, current_occupants
    from app.registry.legacy import import_edge_notes, import_fundamentals
    from app.registry.merge import Merger
    from app.registry.review import compute_needs_review
    from app.registry.watch import WatchChannelsBuilder
    from app.runtime import Runtime, init_core
    from app.util.fs import write_atomic
    from app.util.time import now_ts, parse_ts, to_iso

    s = app_config.settings
    rt = Runtime(started_at=now_ts(), shutdown_event=asyncio.Event())

    async def _sink(text: str) -> bool:
        print(f"[alert] {text}")
        return True
    runtime_mod.runtime = rt
    report: dict = {"seeded_at": to_iso(now_ts())}
    try:
        await init_core(rt, alert_sink=_sink)
        db = rt.db
        run_id = f"script:seed_registry:{rt.started_at}"
        await db.execute("INSERT OR IGNORE INTO collector_runs (run_id, job, started_at) VALUES (?,?,?)",
                         (run_id, "seed_registry", rt.started_at))
        if not a.no_network and not a.legacy_only:
            occ = RegistryOccupantsJob(rt, SPEC_BY_ID["registry_occupants"])
            await occ.run(trigger="cli")
            if occ._status.last_error:
                print(f"registry_occupants failed: {occ._status.last_error}")
            ident = RegistryIdentityJob(rt, SPEC_BY_ID["registry_identity"])
            await ident.run(trigger="cli")
            if ident._status.last_error:
                print(f"registry_identity failed: {ident._status.last_error}")
            report["identity_sources"] = (ident._last or {}).get("sources")
        fpath = s.path(a.fundamentals)
        if fpath.exists():
            rep = await import_fundamentals(db, fpath, as_of=parse_ts(a.legacy_as_of), run_id=run_id, occupancy=None)
            report["legacy"] = rep.as_dict()
        else:
            report["legacy"] = f"not found: {fpath} (copy the old bot's data/fundamentals.json to seed/, checklist A3)"
        epath = s.path(a.edge_notes)
        if epath.is_dir():
            rep = await import_edge_notes(db, epath, as_of=parse_ts(a.edge_as_of), run_id=run_id)
            report["edge_notes"] = rep.as_dict()
        else:
            report["edge_notes"] = f"not found: {epath}"
        now = now_ts()
        merger = Merger(db, run_id, now)
        wb = WatchChannelsBuilder(db, now)
        async with db.transaction():
            for o in await current_occupants(db):
                await wb.rebuild(o, await merger.resolve_all(o))
        review = await compute_needs_review(db, now)
        report["occupants"] = {r["status"]: r["n"] for r in await db.fetchall(
            "SELECT status, COUNT(*) AS n FROM occupants GROUP BY status")}
        report["observations_written"] = {r["source"]: r["n"] for r in await db.fetchall(
            "SELECT source, COUNT(*) AS n FROM identity_observations GROUP BY source")}
        report["events"] = {r["event_type"]: r["n"] for r in await db.fetchall(
            "SELECT event_type, COUNT(*) AS n FROM events WHERE source = 'registry' GROUP BY event_type")}
        report["needs_review_count"] = len(review)
        await db.execute("UPDATE collector_runs SET finished_at = ?, ok = 1 WHERE run_id = ?", (now_ts(), run_id))
        if a.dry_run:
            print("(dry-run: report only; the jobs above still wrote their normal rows)")
        write_atomic(s.path(a.report), report)
        import json
        print(json.dumps({k: report[k] for k in ("occupants", "observations_written", "needs_review_count")},
                         indent=1))
        print(f"report: {s.path(a.report)}")
        return 0
    finally:
        runtime_mod.runtime = None
        if rt.http is not None:
            await rt.http.aclose()
        if rt.db is not None:
            await rt.db.close()


def main(argv: list[str] | None = None) -> int:
    s = app_config.settings
    p = argparse.ArgumentParser(prog="python -m scripts.seed_registry")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-network", action="store_true")
    p.add_argument("--legacy-only", action="store_true")
    p.add_argument("--fundamentals", default=s.LEGACY_FUNDAMENTALS_PATH)
    p.add_argument("--edge-notes", default=s.LEGACY_EDGE_NOTES_DIR)
    p.add_argument("--legacy-as-of", default=s.LEGACY_AS_OF)
    p.add_argument("--edge-as-of", default="2026-06-18T14:37:08Z")
    p.add_argument("--report", default="data/seed_registry_report.json")
    a = p.parse_args(argv)
    return asyncio.run(_run(a))


if __name__ == "__main__":
    sys.exit(main())
