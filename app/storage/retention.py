"""``storage_retention_daily`` (03:00Z) — rollup guarantee, pruning, text→hash privacy retention, maintenance.

Spec 03 §2.7. Destructive policies run only with a recent verified backup (``RETENTION_REQUIRE_BACKUP_HOURS``), or
with backups disabled, or in a disk emergency; privacy policies always run. Each policy is idempotent and batched
(``RETENTION_DELETE_BATCH`` rows per transaction; the writer lock is released between batches).
"""
from __future__ import annotations

import shutil
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Awaitable, Callable, Sequence

import app.config as app_config
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

_HOT = ("price", "subnet_moving_price", "emission", "emission_enabled", "immune", "deregistration_risk",
        "excess_tao", "subnet_ema_tao_flow", "owner_personal_locked_alpha", "total_locked_alpha",
        "total_subnet_stake", "tao_liquidity")


@dataclass(frozen=True)
class RetentionPolicy:
    id: str
    destructive: bool
    run: Callable[["Database", int, bool], Awaitable[int]]


async def _batched_delete(db: "Database", select_keys_sql: str, delete_sql: str, params: Sequence,
                          dry_run: bool, count_sql: str | None = None) -> int:
    s = app_config.settings
    if dry_run:
        return int(await db.fetchval(count_sql, params) or 0) if count_sql else 0
    total = 0
    batch = int(s.RETENTION_DELETE_BATCH)
    while True:
        async with db.transaction():
            res = await db.execute(delete_sql.format(sub=select_keys_sql), (*params, batch))
        n = max(0, res.rowcount)
        total += n
        if n < batch:
            return total


async def _chain_prune(db: "Database", now: int, dry_run: bool) -> int:
    s = app_config.settings
    cutoff = now - s.CHAIN_TICKS_RETENTION_DAYS * 86400
    if dry_run:
        return int(await db.fetchval("SELECT COUNT(*) FROM chain_ticks WHERE event_at < ?", (cutoff,)) or 0)
    touched = 0
    hot = ", ".join(_HOT)
    t_hot = ", ".join(f"t.{c}" for c in _HOT)
    async with db.transaction():
        # hourly rollup: the hour's last tick, hot columns only (others NULL — never copied from other hours)
        res = await db.execute(
            f"INSERT OR IGNORE INTO chain_hourly (netuid, hour_ts, occupant_key, block_number, event_at, observed_at, "
            f"source, run_id, {hot}) "
            f"SELECT t.netuid, (t.event_at / 3600) * 3600, t.occupant_key, t.block_number, t.event_at, t.observed_at, "
            f"'tick_rollup', t.run_id, {t_hot} FROM chain_ticks t "
            f"JOIN (SELECT netuid, event_at / 3600 AS h, MAX(block_number) AS b FROM chain_ticks WHERE event_at < ? "
            f"GROUP BY netuid, event_at / 3600) last ON last.netuid = t.netuid AND last.b = t.block_number",
            (cutoff,))
        touched += max(0, res.rowcount)
        # 5-minute bars from ticks ordered by block_number
        res = await db.execute(
            "INSERT OR IGNORE INTO price_bars (netuid, period, ts, occupant_key, open, high, low, close, ts_last, "
            "tao_in_pool, moving_price, block_number, source, event_at, observed_at, backfilled, run_id) "
            "SELECT netuid, '5m', b, occ, o, hi, lo, c, tl, tp, mp, bn, 'sse_rollup', tl, ob, 0, rid FROM ("
            "  SELECT netuid, (event_at / 300) * 300 AS b,"
            "    FIRST_VALUE(price) OVER w AS o, MAX(price) OVER p AS hi, MIN(price) OVER p AS lo,"
            "    LAST_VALUE(price) OVER w AS c, LAST_VALUE(event_at) OVER w AS tl,"
            "    LAST_VALUE(tao_liquidity) OVER w / 1e9 AS tp, LAST_VALUE(subnet_moving_price) OVER w AS mp,"
            "    LAST_VALUE(block_number) OVER w AS bn, LAST_VALUE(occupant_key) OVER w AS occ,"
            "    LAST_VALUE(observed_at) OVER w AS ob, LAST_VALUE(run_id) OVER w AS rid,"
            "    ROW_NUMBER() OVER (PARTITION BY netuid, event_at / 300 ORDER BY block_number DESC) AS rn"
            "  FROM chain_ticks WHERE event_at < ? AND price > 0"
            "  WINDOW w AS (PARTITION BY netuid, event_at / 300 ORDER BY block_number"
            "               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),"
            "         p AS (PARTITION BY netuid, event_at / 300)"
            ") WHERE rn = 1 AND c > 0", (cutoff,))
        touched += max(0, res.rowcount)
    netuids = [r["netuid"] for r in await db.fetchall(
        "SELECT DISTINCT netuid FROM chain_ticks WHERE event_at < ?", (cutoff,), writer=True)]
    batch = int(s.RETENTION_DELETE_BATCH)
    for n in netuids:
        while True:
            async with db.transaction():
                res = await db.execute(
                    "DELETE FROM chain_ticks WHERE netuid = ? AND block_number IN (SELECT block_number FROM chain_ticks "
                    "WHERE netuid = ? AND event_at < ? ORDER BY block_number LIMIT ?)", (n, n, cutoff, batch))
            touched += max(0, res.rowcount)
            if res.rowcount < batch:
                break
    return touched


async def _price_bars_5m(db: "Database", now: int, dry_run: bool) -> int:
    cutoff = now - app_config.settings.PRICE_BARS_5M_RETENTION_DAYS * 86400
    return await _batched_delete(
        db, "SELECT netuid, period, ts FROM price_bars WHERE period = '5m' AND ts < ? LIMIT ?",
        "DELETE FROM price_bars WHERE (netuid, period, ts) IN ({sub})", (cutoff,), dry_run,
        "SELECT COUNT(*) FROM price_bars WHERE period = '5m' AND ts < ?")


async def _metric_raw_provisional(db: "Database", now: int, dry_run: bool) -> int:
    cutoff = now - app_config.settings.METRIC_PROVISIONAL_RETENTION_DAYS * 86400
    return await _batched_delete(
        db, "SELECT occupant_key, metric_key, as_of, spec_version, stage FROM metric_values WHERE stage = 'raw' "
            "AND as_of % 86400 <> 0 AND as_of < ? LIMIT ?",
        "DELETE FROM metric_values WHERE (occupant_key, metric_key, as_of, spec_version, stage) IN ({sub})",
        (cutoff,), dry_run,
        "SELECT COUNT(*) FROM metric_values WHERE stage = 'raw' AND as_of % 86400 <> 0 AND as_of < ?")


async def _chain_rest_snapshots(db: "Database", now: int, dry_run: bool) -> int:
    cutoff = now - app_config.settings.CHAIN_REST_SNAPSHOTS_RETENTION_DAYS * 86400
    newest = ("NOT EXISTS (SELECT 1 FROM chain_rest_snapshots n WHERE n.endpoint = c.endpoint AND n.params = c.params "
              "AND n.observed_at > c.observed_at)")
    return await _batched_delete(
        db, f"SELECT endpoint, params, observed_at FROM chain_rest_snapshots c WHERE observed_at < ? AND NOT {newest} "
            f"LIMIT ?",
        "DELETE FROM chain_rest_snapshots WHERE (endpoint, params, observed_at) IN ({sub})", (cutoff,), dry_run,
        f"SELECT COUNT(*) FROM chain_rest_snapshots c WHERE observed_at < ? AND NOT {newest}")


async def _github_http_cache(db: "Database", now: int, dry_run: bool) -> int:
    cutoff = now - app_config.settings.GITHUB_HTTP_CACHE_RETENTION_DAYS * 86400
    return await _batched_delete(
        db, "SELECT rowid FROM github_http_cache WHERE fetched_at < ? LIMIT ?",
        "DELETE FROM github_http_cache WHERE rowid IN ({sub})", (cutoff,), dry_run,
        "SELECT COUNT(*) FROM github_http_cache WHERE fetched_at < ?")


async def _llm_batch_items(db: "Database", now: int, dry_run: bool) -> int:
    cutoff = now - app_config.settings.LLM_BATCH_ITEMS_RETENTION_DAYS * 86400
    cond = ("batch_key IN (SELECT batch_key FROM llm_batches WHERE status IN ('applied','failed','expired','canceled') "
            "AND ended_at < ?)")
    return await _batched_delete(
        db, f"SELECT batch_key, post_id FROM llm_batch_items WHERE {cond} LIMIT ?",
        "DELETE FROM llm_batch_items WHERE (batch_key, post_id) IN ({sub})", (cutoff,), dry_run,
        f"SELECT COUNT(*) FROM llm_batch_items WHERE {cond}")


async def _nlp_text_retention(db: "Database", now: int, dry_run: bool) -> int:
    cutoff = now - app_config.settings.LLM_TEXT_RETENTION_DAYS * 86400
    stmts = [
        ("social_posts", "text", "text IS NOT NULL AND event_at < ?", (cutoff,)),
        ("announcements", "text", "text IS NOT NULL AND event_at < ?", (cutoff,)),
        ("announcements", "text", "text IS NOT NULL AND text_purge_after IS NOT NULL AND (text_purge_after <= ? OR "
                                   "EXISTS (SELECT 1 FROM post_labels l WHERE l.post_id = announcements.msg_id))",
         (now,)),
        ("news_items", "summary", "summary IS NOT NULL AND event_at < ?", (cutoff,)),
        ("video_items", "description", "description IS NOT NULL AND event_at < ?", (cutoff,)),
    ]
    total = 0
    for table, col, where, params in stmts:
        if dry_run:
            total += int(await db.fetchval(f"SELECT COUNT(*) FROM {table} WHERE {where}", params) or 0)
            continue
        async with db.transaction():
            res = await db.execute(f"UPDATE {table} SET {col} = NULL WHERE {where}", params)
        total += max(0, res.rowcount)
    return total


async def _web_text(db: "Database", now: int, dry_run: bool) -> int:
    cutoff = now - app_config.settings.WEB_TEXT_RETENTION_DAYS * 86400
    where = ("content_text IS NOT NULL AND check_day_ts < ? AND EXISTS (SELECT 1 FROM web_checks n "
             "WHERE n.occupant_key = web_checks.occupant_key AND n.url = web_checks.url AND n.kind = web_checks.kind "
             "AND n.content_text IS NOT NULL AND n.check_day_ts > web_checks.check_day_ts)")
    if dry_run:
        return int(await db.fetchval(f"SELECT COUNT(*) FROM web_checks WHERE {where}", (cutoff,)) or 0)
    async with db.transaction():
        res = await db.execute(f"UPDATE web_checks SET content_text = NULL WHERE {where}", (cutoff,))
    return max(0, res.rowcount)


async def _maintenance(db: "Database", now: int, dry_run: bool) -> int:
    if dry_run:
        return 0
    await db.checkpoint("TRUNCATE")
    freed = await db.incremental_vacuum(int(app_config.settings.DB_INCREMENTAL_VACUUM_PAGES))
    await db.optimize()
    return freed


POLICIES: tuple[RetentionPolicy, ...] = (
    RetentionPolicy("chain_prune", True, _chain_prune),
    RetentionPolicy("price_bars_5m", True, _price_bars_5m),
    RetentionPolicy("metric_raw_provisional", True, _metric_raw_provisional),
    RetentionPolicy("chain_rest_snapshots", True, _chain_rest_snapshots),
    RetentionPolicy("github_http_cache", True, _github_http_cache),
    RetentionPolicy("llm_batch_items", True, _llm_batch_items),
    RetentionPolicy("nlp_text_retention", False, _nlp_text_retention),
    RetentionPolicy("web_text", False, _web_text),
    RetentionPolicy("maintenance", False, _maintenance),
)


def _backup_gate() -> tuple[bool, str, float | None]:
    """(destructive allowed, reason, backup_age_hours)."""
    s = app_config.settings
    if not s.DB_BACKUP_ENABLED:
        logger.warning("retention: DB_BACKUP_ENABLED=false — destructive policies run without a backup gate")
        return True, "backups_disabled", None
    try:
        free_gb = shutil.disk_usage(s.path(s.DATA_DIR)).free / 2**30
        if free_gb < s.DISK_FREE_MIN_GB:
            return True, "disk_emergency", None
    except OSError:
        pass
    try:
        from app.storage.backup import latest_backup_age_hours
        age = latest_backup_age_hours(s.path(s.DB_BACKUP_DIR))
    except ImportError:
        age = None
    if age is not None and age <= s.RETENTION_REQUIRE_BACKUP_HOURS:
        return True, "backup_ok", age
    return False, "no_recent_backup", age


async def run_retention(db: "Database", *, now: int, dry_run: bool = False,
                        only: Sequence[str] | None = None) -> dict[str, int]:
    from app.control import integrity_hold_active

    allowed, reason, age = _backup_gate()
    if integrity_hold_active():
        allowed, reason = False, "paused"
    out: dict[str, int] = {}
    skipped: list[str] = []
    for pol in POLICIES:
        if only and pol.id not in only:
            continue
        if pol.destructive and not allowed:
            skipped.append(pol.id)
            continue
        t0 = time.monotonic()
        n = await pol.run(db, now, dry_run)
        out[pol.id] = n
        logger.info("retention policy", data={"policy": pol.id, "rows": n, "seconds": round(time.monotonic() - t0, 2),
                                              "dry_run": dry_run})
    if skipped and not dry_run:
        from app.storage.events import emit_event
        try:
            await emit_event(db, event_type="retention_skipped", occupant_key=None, netuid=None, severity="warning",
                             event_at=now, observed_at=now,
                             payload={"reason": reason, "policies": skipped, "backup_age_hours": age},
                             dedupe_key=str(now // 86400), source="storage", run_id=None)
        except Exception as exc:  # pragma: no cover
            logger.warning("retention_skipped event failed", data={"error": str(exc)})
    out["_skipped"] = len(skipped)
    if skipped:
        out["_skipped_policies"] = skipped  # type: ignore[assignment]
    return out


class RetentionJob(CollectorJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        res = await run_retention(run.rt.db, now=now_ts())
        skipped = res.pop("_skipped_policies", [])
        res.pop("_skipped", None)
        return JobOutcome(rows=sum(v for v in res.values() if isinstance(v, int)),
                          note=f"chain_prune={res.get('chain_prune', 0)} skipped={skipped}", meta=res)
