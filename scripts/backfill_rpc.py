"""Archive-RPC history backfill (spec 17 §2.3, step S4).

    python -m scripts.backfill_rpc --probe [--endpoint URL]
    python -m scripts.backfill_rpc --era gap [--era ledger] [--era pre_ledger] [--step SEC]
        [--from-block N --to-block N] [--endpoint URL] [--price-method auto|swap_sqrt|reserve_ratio]
        [--force-unprobed] [--limit N] [--no-post] [--db PATH]
    python -m scripts.backfill_rpc --post-only     # occupancy reconstruct + stamp + bars over all RPC history

Standalone process with its own ``ProviderLimiter`` (the service does not use the archive RPC by default).
Exit codes: 0 ok, 2 precondition, 3 disabled/dependency, 4 decode stop, 5 probe missing/failed, 130 interrupted.
Resumable: ``done``/``skipped`` buckets are never re-fetched; ``failed`` ones are retried.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

import app.config as app_config


async def post_process(db_path: Path | None, *, run_id: str, from_ts: int | None, to_ts: int | None,
                       endpoint_override: str | None, owner_rows: int) -> dict:
    """§2.3 step 8: occupancy.reconstruct → emit history events → stamp_all → bars.rebuild over the range."""
    from app.backfill import bars, common, occupancy
    from app.backfill.rpc_client import RpcGateway, endpoints_from_settings
    from app.ratelimit import ProviderLimiter
    from app.util.time import now_ts
    s = app_config.settings
    con = common.open_db(db_path)
    rpc = RpcGateway(endpoints_from_settings(s, endpoint_override), ProviderLimiter.from_settings(),
                     timeout=s.BACKFILL_RPC_TIMEOUT_SEC, max_attempts=s.BACKFILL_RPC_MAX_ATTEMPTS,
                     backoff_max=s.ARCHIVE_BACKOFF_MAX_SEC, public_max_rps=s.BACKFILL_RPC_PUBLIC_MAX_RPS,
                     spend=common.SpendBook(db_path))
    out: dict = {}
    try:
        out["occupancy"] = await occupancy.reconstruct(con, rpc, run_id=run_id)
        db = await common.prepare_db(db_path)
        try:
            out["history_events"] = await occupancy.emit_history_events(db, observed_at=now_ts(), run_id=run_id)
        finally:
            await db.close()
        idx = occupancy.OccupancyIndex.load(con)
        out["stamp"] = occupancy.stamp_all(con, idx)
        if from_ts is None or to_ts is None:
            r = con.execute("SELECT MIN(timestamp), MAX(timestamp) FROM pool_history WHERE source IN "
                            "('blockmachine','onfinality','opentensor_archive')").fetchone()
            from_ts, to_ts = (r[0], r[1] + 1) if r and r[0] is not None else (None, None)
        if from_ts is not None:
            out["bars"] = bars.rebuild(con, from_ts=from_ts - 86400, to_ts=to_ts + 86400,
                                       live_from_ts=s.LEDGER_LIVE_FROM_TS, now=now_ts(), run_id=run_id)
        if owner_rows:
            con.execute("DELETE FROM job_state WHERE job = 'owner_rpc_seed' AND key = 'rpc_seed_done'")
            out["owner_rpc_seed_rearmed"] = True
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        await rpc.close()
        con.close()
    return out


async def _run(a: argparse.Namespace) -> int:
    from app.backfill import common, rpc_backfill
    from app.util.time import now_ts
    s = app_config.settings
    db_path = Path(a.db) if a.db else None
    try:
        common.check_enabled()
        common.check_disk()
        if s.BACKFILL_RPC_MODE != "runtime_api":
            raise common.PreconditionError("BACKFILL_RPC_MODE=storage is not built (runtime_api decoded every "
                                           "runtime seen; BUILD-LOG Phase 3)", code=common.EXIT_DISABLED)
        try:
            import async_substrate_interface  # noqa: F401
        except ImportError:
            raise common.PreconditionError("pip install -e '.[backfill]' (async-substrate-interface)",
                                           code=common.EXIT_DISABLED) from None
    except common.PreconditionError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return exc.code
    common.install_signal_handlers()
    try:
        lock = common.BackfillLock()
        lock.__enter__()
    except common.BackfillBusy as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return common.EXIT_PRECONDITION
    try:
        db = await common.prepare_db(db_path)
        await db.close()
        if a.probe:
            res = await rpc_backfill.probe(endpoint_override=a.endpoint, db_path=db_path)
            for name, c in res["checks"].items():
                flag = "PASS" if (isinstance(c, dict) and c.get("pass")) else "FAIL"
                print(f"{flag:4}  {name}: " + json.dumps({k: v for k, v in c.items() if k != "pass"}, default=str)
                      if isinstance(c, dict) else f"FAIL  {name}: {c}")
            print(json.dumps({"pass": res["pass"], "endpoint": res["endpoint"],
                              "price_method_match": res["price_method_match"], "asi_version": res["asi_version"],
                              "bittensor_wallet_installed": res["bittensor_wallet_installed"]}))
            return 0 if res["pass"] else common.EXIT_VERIFY
        if a.post_only:
            con = common.open_db(db_path)
            with common.step_run(con, "backfill_rpc:post") as run:
                out = await post_process(db_path, run_id=run.run_id, from_ts=None, to_ts=None,
                                         endpoint_override=a.endpoint, owner_rows=1)
                run.meta = out
                run.rows = out.get("stamp", {}).get("pool_history_stamped", 0)
            con.close()
            print(json.dumps(out, indent=1, default=str))
            return 0
        eras = a.era or list(s.BACKFILL_RPC_ERAS)
        code = 0
        for era in eras:
            con = common.open_db(db_path)
            job = f"backfill_rpc:{era}"
            try:
                with common.step_run(con, job) as run:
                    rep = await rpc_backfill.run_era(
                        era, step=a.step, force_unprobed=a.force_unprobed, endpoint_override=a.endpoint,
                        price_method=a.price_method, from_block=a.from_block, to_block=a.to_block, limit=a.limit,
                        db_path=db_path, run_id=run.run_id)
                    run.rows = rep.rows
                    run.meta = {"era": rep.__dict__}
                    post = None
                    if not a.no_post and rep.done and rep.exit_code in (0,):
                        post = await post_process(db_path, run_id=run.run_id, from_ts=rep.from_ts, to_ts=rep.to_ts,
                                                  endpoint_override=a.endpoint, owner_rows=rep.owner_rows)
                        run.meta["post"] = post
                    run.note = (f"planned={rep.planned} done={rep.done} failed={rep.failed} skipped={rep.skipped} "
                                f"rows={rep.rows}" + (f" stopped={rep.stopped}" if rep.stopped else ""))
                print(json.dumps({"era": rep.__dict__, "post": post}, indent=1, default=str))
                pending = rep.failed or rep.stopped
                ev = await common.prepare_db(db_path)
                try:
                    if rep.exit_code in (common.EXIT_DECODE, common.EXIT_VERIFY):
                        await common.emit_step_event(ev, "backfill_failed", job=job, era=era, run_id=run.run_id,
                                                     error=rep.stopped or "trigger aborts",
                                                     resume_hint=f"python -m scripts.backfill_rpc --era {era}")
                    elif not pending:
                        warns = (post or {}).get("occupancy", {}).get("anomalies") or []
                        await common.emit_step_event(ev, "backfill_done", job=job, era=era, run_id=run.run_id,
                                                     rows=rep.rows, seconds=rep.seconds, warnings=warns[:20])
                finally:
                    await ev.close()
                code = code or rep.exit_code
                if rep.stopped:
                    break
            except common.PreconditionError as exc:
                print(f"refused: {exc}", file=sys.stderr)
                return exc.code
            except Exception as exc:
                ev = await common.prepare_db(db_path)
                try:
                    await common.emit_step_event(ev, "backfill_failed", job=job, era=era,
                                                 run_id=f"script:{job}:{now_ts()}",
                                                 error=f"{type(exc).__name__}: {str(exc)[:300]}",
                                                 resume_hint=f"python -m scripts.backfill_rpc --era {era}")
                finally:
                    await ev.close()
                raise
            finally:
                con.close()
        return code
    finally:
        lock.__exit__(None, None, None)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m scripts.backfill_rpc", description=__doc__.split("\n")[0])
    p.add_argument("--probe", action="store_true")
    p.add_argument("--post-only", action="store_true")
    p.add_argument("--era", action="append", choices=["gap", "ledger", "pre_ledger"])
    p.add_argument("--step", type=int, choices=[900, 1800, 3600, 14400, 86400])
    p.add_argument("--from-block", type=int)
    p.add_argument("--to-block", type=int)
    p.add_argument("--endpoint")
    p.add_argument("--mode", choices=["runtime_api", "storage"])
    p.add_argument("--price-method", choices=["auto", "swap_sqrt", "reserve_ratio"])
    p.add_argument("--force-unprobed", action="store_true")
    p.add_argument("--limit", type=int, help="at most N buckets (testing)")
    p.add_argument("--no-post", action="store_true", help="skip occupancy/stamp/bars after the era")
    p.add_argument("--db", help="database path (default DB_PATH)")
    a = p.parse_args(argv)
    if a.mode:
        app_config.settings.BACKFILL_RPC_MODE = a.mode
    if not (a.probe or a.era or a.post_only):
        a.era = None
    return asyncio.run(_run(a))


if __name__ == "__main__":
    sys.exit(main())
