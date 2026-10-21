"""``dereg_watch`` (spec 14 §2.5): pages once per reason per UTC day for each ``WATCHLIST_NETUIDS`` subnet at risk.

SQLite reads only: the newest ``chain_ticks`` row per netuid (≤ 30 min old, else skipped — never a fabricated risk)
and the occupant's ``dereg_safety`` raw row via 03's ``metric_at``. Immunity is the local rule (master §5).
"""
from __future__ import annotations

import app.config as app_config
from app.api import queries as Q
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.storage.events import emit_event

MAX_TICK_AGE_S = 1800
BLOCKS_PER_DAY = 7200


def moving_price_ranks(ticks: dict[int, dict], immune: dict[int, bool]) -> dict[int, int]:
    """1-based rank by ``subnet_moving_price`` ascending among fresh, non-immune netuids with a price."""
    pool = [(t["subnet_moving_price"], n) for n, t in ticks.items()
            if t["fresh"] and not immune.get(n, False) and t["subnet_moving_price"] is not None and n != 0]
    return {n: i for i, (_, n) in enumerate(sorted(pool), 1)}


def reason_for(*, immune: bool, blocks_left: int, risk: int | None, safety: float | None) -> str | None:
    s = app_config.settings
    if not immune and risk == 1:
        return "bottom3"
    if not immune and safety is not None and safety <= float(s.DEREG_WATCH_SAFETY_MAX):
        return "safety_metric"
    if immune and blocks_left <= int(s.DEREG_WATCH_IMMUNITY_WARN_DAYS) * BLOCKS_PER_DAY:
        return "immunity_ending"
    return None


class DeregWatchJob(CollectorJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        from app.features.registry import METRICS
        from app.storage.pit import metric_at
        s = app_config.settings
        db = run.rt.db
        now = int(run.started_at)
        watch = [int(n) for n in s.WATCHLIST_NETUIDS]
        if not watch:
            return JobOutcome(rows=0, note="watchlist empty")
        ticks = await Q.latest_ticks(db, now=now, max_age_s=MAX_TICK_AGE_S)
        subs = {r["netuid"]: r for r in await db.fetchall(
            "SELECT netuid, occupant_key, name, network_registered_at FROM subnets")}
        period = int(s.NETWORK_IMMUNITY_PERIOD_BLOCKS)
        immune: dict[int, bool] = {}
        for n, t in ticks.items():
            sub = subs.get(n)
            if sub is not None:
                immune[n] = sub["network_registered_at"] + period - int(t["block_number"]) > 0
        ranks = moving_price_ranks(ticks, immune)
        sent, skipped = 0, []
        fresh_s = METRICS["dereg_safety"].freshness_s
        for n in watch:
            sub = subs.get(n)
            t = ticks.get(n)
            if sub is None:
                skipped.append(f"{n}:no_occupant")
                continue
            if t is None or not t["fresh"]:
                skipped.append(f"{n}:stale_tick")
                logger.info("dereg_watch skipped: no fresh tick", data={"netuid": n})
                continue
            blocks_left = sub["network_registered_at"] + period - int(t["block_number"])
            is_immune = blocks_left > 0
            row = await metric_at(db, sub["occupant_key"], "dereg_safety", now, spec_version=s.SPEC_VERSION,
                                  freshness_s=fresh_s)
            safety = None
            if row is not None and getattr(row, "status", None) not in ("failed", "none"):
                safety = row.value
            reason = reason_for(immune=is_immune, blocks_left=blocks_left, risk=t["deregistration_risk"],
                                safety=safety)
            if reason is None:
                continue
            eid = await emit_event(
                db, event_type="dereg_watch", occupant_key=sub["occupant_key"], netuid=n, severity="warning",
                event_at=now, observed_at=now,
                payload={"netuid": n, "name": sub["name"], "reason": reason,
                         "moving_price_rank": None if is_immune else ranks.get(n), "immune": is_immune,
                         "blocks_left_immunity": blocks_left if is_immune else None,
                         "dereg_safety": None if safety is None else round(float(safety), 4)},
                dedupe_key=f"{sub['occupant_key']}|{reason}|{now // 86400}", source="api", run_id=run.run_id)
            if eid is not None:
                sent += 1
        note = f"watch={len(watch)} events={sent}" + (f" skipped={','.join(skipped)}" if skipped else "")
        return JobOutcome(rows=sent, note=note)
