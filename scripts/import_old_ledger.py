"""Import the old SubnetTrader ledger(s) into ``pool_history`` (spec 17 §2.1, steps S1 and S5).

    python -m scripts.import_old_ledger [--phase import,stamp,bars,verify|all] [--online] [--extra PATH ...]
        [--thin-sec N] [--db PATH]

``import`` needs the service stopped (``sudo systemctl stop taosentinel``) unless ``--online`` (smaller batches).
``stamp`` = occupancy reconstruct (trusted ledger + RPC observations) + ``stamp_all``; ``bars`` = rebuild 1h/1d over
all of ``pool_history``; ``verify`` = the §10 count checks. Exit: 0 ok, 2 precondition, 3 disabled, 5 verify failed.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

import app.config as app_config


async def _stamp(db_path: Path | None, run_id: str) -> dict:
    from app.backfill import common, occupancy
    from app.backfill.rpc_client import RpcGateway, endpoints_from_settings
    from app.ratelimit import ProviderLimiter
    from app.util.time import now_ts
    s = app_config.settings
    con = common.open_db(db_path)
    rpc = None
    try:
        import async_substrate_interface  # noqa: F401
        rpc = RpcGateway(endpoints_from_settings(s), ProviderLimiter.from_settings(),
                         timeout=s.BACKFILL_RPC_TIMEOUT_SEC, max_attempts=s.BACKFILL_RPC_MAX_ATTEMPTS,
                         spend=common.SpendBook(db_path))
    except ImportError:
        pass
    try:
        out = {"occupancy": await occupancy.reconstruct(con, rpc, run_id=run_id)}
        db = await common.prepare_db(db_path)
        try:
            out["history_events"] = await occupancy.emit_history_events(db, observed_at=now_ts(), run_id=run_id)
        finally:
            await db.close()
        out["stamp"] = occupancy.stamp_all(con, occupancy.OccupancyIndex.load(con))
        return out
    finally:
        if rpc is not None:
            await rpc.close()
        con.close()


def main(argv: list[str] | None = None) -> int:
    from app.backfill import bars, common, ledger_import
    from app.util.time import now_ts
    p = argparse.ArgumentParser(prog="python -m scripts.import_old_ledger", description=__doc__.split("\n")[0])
    p.add_argument("--phase", default="import")
    p.add_argument("--online", action="store_true")
    p.add_argument("--extra", action="append", default=[])
    p.add_argument("--thin-sec", type=int)
    p.add_argument("--db")
    a = p.parse_args(argv)
    s = app_config.settings
    phases = {x.strip() for x in a.phase.split(",") if x.strip()}
    if "all" in phases:
        phases = {"import", "stamp", "bars", "verify"}
    unknown = phases - {"import", "stamp", "bars", "verify"}
    if unknown:
        print(f"unknown phases {sorted(unknown)}", file=sys.stderr)
        return 2
    db_path = Path(a.db) if a.db else None
    thin = s.LEDGER_IMPORT_THIN_SEC if a.thin_sec is None else a.thin_sec
    try:
        common.check_enabled()
        common.check_disk()
        if "import" in phases and not a.online and common.service_is_up():
            raise common.PreconditionError("the service is running: stop it (sudo systemctl stop taosentinel) "
                                           "or pass --online")
    except common.PreconditionError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return exc.code
    common.install_signal_handlers()
    try:
        lock = common.BackfillLock().__enter__()
    except common.BackfillBusy as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    code = 0
    try:
        asyncio.run(_prepare(db_path))
        con = common.open_db(db_path)
        out: dict = {}
        try:
            with common.step_run(con, "import_old_ledger") as run:
                if "import" in phases:
                    out["import"] = ledger_import.run_import(con, online=a.online,
                                                             extra=[Path(x) for x in a.extra], thin_sec=thin,
                                                             run_id=run.run_id)
                    run.rows = sum(v.get("rows_new", 0) for v in out["import"].values() if isinstance(v, dict))
                if "stamp" in phases:
                    out["stamp"] = asyncio.run(_stamp(db_path, run.run_id))
                if "bars" in phases:
                    r = con.execute("SELECT MIN(timestamp), MAX(timestamp) FROM pool_history").fetchone()
                    if r[0] is not None:
                        out["bars"] = bars.rebuild(con, from_ts=r[0], to_ts=r[1] + 1,
                                                   live_from_ts=s.LEDGER_LIVE_FROM_TS, now=now_ts(), run_id=run.run_id)
                if "verify" in phases or "import" in phases:
                    out["verify"] = ledger_import.verify(con, s)
                    if any(not v["pass"] for v in out["verify"].values()):
                        code = common.EXIT_VERIFY
                run.meta = {"phases": sorted(phases), "verify": out.get("verify")}
                con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except common.PreconditionError as exc:
            print(f"refused: {exc}", file=sys.stderr)
            return exc.code
        finally:
            con.close()
        print(json.dumps(out, indent=1, default=str))
        kind = "backfill_failed" if code else "backfill_done"
        extra = ({"error": "verify failed", "resume_hint": "python -m scripts.import_old_ledger --phase verify"}
                 if code else {"rows": run.rows, "seconds": run.seconds})
        common.emit_step_event_sync(db_path, kind, job="import_old_ledger", era=None, run_id=run.run_id, **extra)
        return code
    finally:
        lock.__exit__(None, None, None)


async def _prepare(db_path: Path | None) -> None:
    from app.backfill import common
    db = await common.prepare_db(db_path)
    await db.close()


if __name__ == "__main__":
    sys.exit(main())
