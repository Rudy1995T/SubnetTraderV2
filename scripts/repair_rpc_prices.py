"""One-off repair of RPC-backfilled prices (BUILD-LOG Phase 3 #17).

    python -m scripts.repair_rpc_prices [--dry-run] [--from-block N] [--to-block N] [--db PATH]

Between runtimes 419 and 424 the chain stopped pricing through the Swap pallet's ``AlphaSqrtPrice`` while that
storage lingered (cleared in runtime 445), so ``auto`` stored stale ``swap_sqrt`` prices for those blocks. For every
stored RPC block whose runtime exposes ``SwapRuntimeApi_current_alpha_price_all`` this re-reads the chain's own price
and corrects only rows that disagree (> 1e-6 relative and > 1.5 rao): ``price``, ``price_method``,
``observed_at = now`` (the corrected value is knowable only now) and ``run_id``. ``pool_history``'s update trigger is
dropped and recreated with its exact DDL inside the same transaction (a failure rolls both back). The affected RPC bars
are then deleted and rebuilt. Read-only against the chain; ≈ 3 metered calls per block.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

import app.config as app_config

RPC_SOURCES = ("blockmachine", "onfinality", "opentensor_archive")
API_FIRST_BLOCK_HINT = 7_800_000      # runtime 377 (block 7,696,958) has no price API; 393 (7,912,684) has it


async def _fetch(blocks: list[int], concurrency: int) -> dict[int, dict[int, float] | None]:
    from app.backfill import common
    from app.backfill.rpc_client import RpcGateway, endpoints_from_settings
    from app.logging.logger import logger
    from app.ratelimit import ProviderLimiter
    s = app_config.settings
    rpc = RpcGateway(endpoints_from_settings(s)[:1], ProviderLimiter.from_settings(),
                     timeout=s.BACKFILL_RPC_TIMEOUT_SEC, max_attempts=s.BACKFILL_RPC_MAX_ATTEMPTS,
                     spend=common.SpendBook())
    out: dict[int, dict[int, float] | None] = {}
    q: asyncio.Queue[int] = asyncio.Queue()
    for b in blocks:
        q.put_nowait(b)

    async def worker() -> None:
        while not q.empty() and not common.stop_requested():
            b = q.get_nowait()
            for attempt in range(3):
                try:
                    h = await rpc.block_hash(b)
                    spec = await rpc.runtime_version(h)
                    out[b] = await rpc.alpha_prices(h, spec, meta_hash_fn=lambda b=b: rpc.block_hash(b + 1))
                    break
                except Exception as exc:
                    if attempt == 2:
                        logger.warning("repair: block unreadable", data={"block": b, "error": str(exc)[:160]})
            if len(out) % 250 == 0:
                logger.info("repair progress", data={"blocks": len(out), "left": q.qsize()})
    try:
        await asyncio.gather(*(worker() for _ in range(concurrency)))
    finally:
        await rpc.close()
    return out


def main(argv: list[str] | None = None) -> int:
    from app.backfill import bars, common
    from app.backfill.rpc_backfill import api_price_method
    from app.util.time import now_ts
    p = argparse.ArgumentParser(prog="python -m scripts.repair_rpc_prices", description=__doc__.split("\n")[0])
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--from-block", type=int, default=API_FIRST_BLOCK_HINT)
    p.add_argument("--to-block", type=int)
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--db")
    a = p.parse_args(argv)
    s = app_config.settings
    db_path = Path(a.db) if a.db else None
    common.install_signal_handlers()
    try:
        lock = common.BackfillLock().__enter__()
    except common.BackfillBusy as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    try:
        con = common.open_db(db_path)
        ph = ",".join("?" * len(RPC_SOURCES))
        args: list = [*RPC_SOURCES, a.from_block]
        sql = f"SELECT DISTINCT block_number FROM pool_history WHERE source IN ({ph}) AND block_number >= ?"
        if a.to_block:
            sql += " AND block_number <= ?"
            args.append(a.to_block)
        blocks = [r[0] for r in con.execute(sql + " ORDER BY block_number", args)]
        print(f"blocks to check: {len(blocks)}", flush=True)
        prices = asyncio.run(_fetch(blocks, a.concurrency))
        run_id = f"script:repair_rpc_prices:{now_ts()}"
        now = now_ts()
        fixes: list[tuple] = []
        checked = no_api = unread = 0
        stats: dict[str, int] = {}
        span: dict[int, list[int]] = {}
        for b in blocks:
            if b not in prices:
                unread += 1
                continue
            api = prices[b]
            if api is None:
                no_api += 1
                continue
            for n, price, meth, tao, alpha, ts in con.execute(
                    f"SELECT netuid, price, price_method, tao_in_pool, alpha_in_pool, timestamp FROM pool_history "
                    f"WHERE block_number = ? AND netuid <> 0 AND source IN ({ph})", (b, *RPC_SOURCES)):
                checked += 1
                ap = api.get(n)
                if ap is None or ap <= 0:
                    continue
                if abs(price - ap) / ap <= 1e-6 or abs(price - ap) <= 1.5e-9:
                    continue
                new_m = api_price_method({"tao_in": round(tao * 1e9), "alpha_in": round(alpha * 1e9)}, ap)
                fixes.append((ap, new_m, now, run_id, n, b))
                stats[f"{meth}->{new_m}"] = stats.get(f"{meth}->{new_m}", 0) + 1
                sp = span.setdefault(n, [ts, ts])
                sp[0], sp[1] = min(sp[0], ts), max(sp[1], ts)
        report = {"blocks": len(blocks), "unread": unread, "no_price_api": no_api, "rows_checked": checked,
                  "rows_fixed": len(fixes), "by_change": stats, "netuids": len(span), "dry_run": a.dry_run,
                  "first_fixed_block": min((f[5] for f in fixes), default=None),
                  "last_fixed_block": max((f[5] for f in fixes), default=None)}
        print(json.dumps(report, indent=1), flush=True)
        if a.dry_run or not fixes:
            return 0 if not unread else 1
        ddl = con.execute("SELECT sql FROM sqlite_schema WHERE type = 'trigger' AND name = 'trg_pool_history_update'"
                          ).fetchone()[0]
        with common.step_run(con, "repair_rpc_prices") as run:
            with common.tx(con):
                con.execute("DROP TRIGGER trg_pool_history_update")
                con.executemany("UPDATE pool_history SET price = ?, price_method = ?, observed_at = ?, run_id = ? "
                                "WHERE netuid = ? AND block_number = ?", fixes)
                con.execute(ddl)
            assert con.execute("SELECT 1 FROM sqlite_schema WHERE name = 'trg_pool_history_update'").fetchone()
            # rebuild the RPC bars the repaired rows fed (tmc_candle / ledger bars are untouched)
            rebuilt = {"deleted": 0, "written": 0}
            for n, (lo, hi) in sorted(span.items()):
                a0, a1 = (lo // 86400) * 86400, (hi // 86400 + 1) * 86400
                with common.tx(con):
                    cur = con.execute(f"DELETE FROM price_bars WHERE netuid = ? AND period IN ('1h','1d') AND ts >= ? "
                                      f"AND ts < ? AND source IN ({ph})", (n, a0, a1, *RPC_SOURCES))
                    rebuilt["deleted"] += max(0, cur.rowcount)
                r = bars.rebuild(con, from_ts=a0, to_ts=a1, netuids=[n], live_from_ts=s.LEDGER_LIVE_FROM_TS, now=now,
                                 run_id=run_id)
                rebuilt["written"] += r["written"]
            run.rows, run.meta = len(fixes), {**report, "bars": rebuilt}
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        print(json.dumps({"bars": rebuilt}), flush=True)
        con.close()
        return 0
    finally:
        lock.__exit__(None, None, None)


if __name__ == "__main__":
    sys.exit(main())
