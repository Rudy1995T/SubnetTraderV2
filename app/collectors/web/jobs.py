"""The three 07 jobs (spec 07 §2.7; 01's ``JOB_TABLE`` carries the rows): ``web_check``, ``web_metrics``,
``web_spn2``. Runtime watermarks (``wayback_backoff_until``, ``wayback_last_ok``) live in ``job_state`` (02 §B12)."""
from __future__ import annotations

import asyncio
from typing import Any

from app.collectors.web.check import CheckContext, run_check
from app.collectors.web.classify import WebHeuristicsConfig
from app.collectors.web.robots import RobotsCache
from app.collectors.web.wayback import WaybackClient
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.util.time import now_ts, to_iso

DEADLINE_MARGIN_S = 120
_STATE_KEYS = ("wayback_backoff_until", "wayback_last_ok")


class WebCheckJob(CollectorJob):
    _robots: RobotsCache | None = None
    _last: dict | None = None
    _state: dict | None = None

    def robots(self, run: RunContext) -> RobotsCache:
        if self._robots is None or self._robots.http is not run.rt.http:
            self._robots = RobotsCache(run.rt.http, int(run.settings.WEB_ROBOTS_CACHE_DAYS))
        self._robots.ttl = float(run.settings.WEB_ROBOTS_CACHE_DAYS) * 86400.0
        return self._robots

    async def collect(self, run: RunContext) -> JobOutcome:
        s = run.settings
        state = {k: await self.get_state(k) for k in _STATE_KEYS}
        before = dict(state)
        ctx = CheckContext(db=run.rt.db, http=run.rt.http, limiter=run.rt.limiter, s=s, run_id=run.run_id,
                           started_at=run.started_at,
                           deadline=float(run.started_at + (self.spec.timeout_sec or 1500) - DEADLINE_MARGIN_S),
                           robots=self.robots(run), wayback=WaybackClient(run.rt.http),
                           heur=WebHeuristicsConfig.from_settings(s), state=state, costs=run.rt.costs)
        rep = await run_check(ctx)
        for k in _STATE_KEYS:
            if state.get(k) != before.get(k):
                await self.set_state(k, state[k])
        self._state = state
        summ = rep.summary()
        self._last = summ
        logger.info("web_check_done", data={k: v for k, v in summ.items() if k != "targets"})
        notes = []
        if rep.network_suspect:
            notes.append("network_suspect")
        if rep.internal_errors:
            notes.append(f"internal_errors={rep.internal_errors}")
        if rep.left:
            notes.append(f"partial: {rep.left} urls left")
        notes.append(f"checked={rep.urls_checked}/{rep.urls_due} rows={rep.rows}")
        return JobOutcome(rows=rep.rows, note=" ".join(notes), meta=summ)

    def health_extra(self) -> dict | None:
        last = self._last or {}
        st = self._state or {}
        return {"urls_total": last.get("urls_total"), "urls_due": last.get("left"),
                "urls_due_at_start": last.get("urls_due"), "urls_checked": last.get("urls_checked"),
                "status_counts": last.get("status_counts"), "network_suspect": last.get("network_suspect"),
                "internal_errors": last.get("internal_errors"),
                "wayback_last_ok": to_iso(st.get("wayback_last_ok")) if st.get("wayback_last_ok") else None,
                "wayback_backoff_until": to_iso(st.get("wayback_backoff_until"))
                if st.get("wayback_backoff_until") else None}


class WebMetricsJob(CollectorJob):
    _last: dict | None = None

    async def collect(self, run: RunContext) -> JobOutcome:
        from app.features.web_metrics import run_web_metrics
        out = await run_web_metrics(run.rt.db, t_eval=run.started_at, run_id=run.run_id)
        out.pop("results", None)
        out.pop("pending_events", None)
        self._last = out
        return JobOutcome(rows=out["rows"], note=f"as_of={to_iso(out['as_of'])} occupants={out['occupants']} "
                                                 f"rows={out['rows']} events={out['events']}", meta=out)

    def health_extra(self) -> dict | None:
        last = self._last or {}
        return {"as_of": to_iso(last["as_of"]) if last.get("as_of") else None, "occupants": last.get("occupants"),
                "rows": last.get("rows"), "events": last.get("events")}


SPN2_URLS_SQL = (
    "SELECT w.url FROM web_checks w JOIN (SELECT occupant_key, url, MAX(event_at) AS ev FROM web_checks "
    "WHERE kind = 'website' AND is_primary = 1 GROUP BY occupant_key, url) l ON l.occupant_key = w.occupant_key "
    "AND l.url = w.url AND l.ev = w.event_at WHERE w.kind = 'website' AND w.status_class IN ('up_2xx','redirect_ok') "
    "AND w.event_at >= ? GROUP BY w.url ORDER BY w.url")


class WebSpn2Job(CollectorJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        s = run.settings
        if not (s.WEB_SPN2_ENABLED and s.IA_ACCESS_KEY and s.IA_SECRET_KEY):
            return JobOutcome(note="skipped: SPN2 disabled or IA keys missing")
        urls = [r["url"] for r in await run.rt.db.fetchall(SPN2_URLS_SQL, (now_ts() - 3 * 86400,))]
        wb = WaybackClient(run.rt.http)
        deadline = run.started_at + (self.spec.timeout_sec or 3600) - DEADLINE_MARGIN_S
        saved = failed = 0
        meta: dict[str, Any] = {"urls": len(urls)}
        for i, u in enumerate(urls):
            if now_ts() >= deadline:
                meta["left"] = len(urls) - i
                break
            if i:
                await asyncio.sleep(float(s.WEB_SPN2_MIN_INTERVAL_S))
            jid = await wb.spn2_save(u, s.IA_ACCESS_KEY, s.IA_SECRET_KEY)
            if jid:
                saved += 1
                logger.info("web.spn2_saved", data={"url": u, "job_id": jid})
            else:
                failed += 1
            if wb.retry_after:
                meta["backoff"] = wb.retry_after
                break
        meta.update(saved=saved, failed=failed)
        return JobOutcome(rows=saved, note=f"spn2 saved={saved} failed={failed} of {len(urls)}", meta=meta)
