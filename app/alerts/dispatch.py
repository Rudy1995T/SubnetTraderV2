"""``alert_dispatch`` — the only sender of ``events`` rows to Telegram (spec 01 §2.11, 14 §2.4 delivery rules).

Per tick: select up to ``ALERT_MAX_ROWS_PER_TICK`` rows with ``alerted_at IS NULL`` (oldest first), route them;
digest/digest_late/none rows are marked at once; immediate rows are sent (batched above ``ALERT_BATCH_THRESHOLD``)
and marked when Telegram accepts. A 400 is retried once as plain text, then the row is marked ``alert_failed``.
Network errors / 5xx / 429 mark nothing and end the tick. Telegram unconfigured → immediate rows stay NULL.
"""
from __future__ import annotations

import json

import app.config as app_config
from app.alerts.routing import payload_of, route_for
from app.alerts.templates import render
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.notifications import telegram
from app.notifications.format import chunk
from app.util.time import now_ts


async def _mark(db, row_id: int, route: str, extra: dict | None = None) -> None:
    patch = {"alert_route": route, **(extra or {})}
    await db.execute(
        "UPDATE events SET alerted_at = ?, payload = json_patch(payload, ?) WHERE id = ? AND alerted_at IS NULL",
        (now_ts(), json.dumps(patch), row_id))


class AlertDispatchJob(CollectorJob):
    async def _annotate(self, db, rows: list[dict]) -> None:
        netuids = {r["netuid"] for r in rows if r.get("netuid") is not None}
        names: dict[int, str] = {}
        if netuids:
            try:
                ph = ",".join("?" * len(netuids))
                for r in await db.fetchall(f"SELECT netuid, name FROM subnets WHERE netuid IN ({ph})", tuple(netuids)):
                    if r.get("name"):
                        names[r["netuid"]] = r["name"]
            except Exception:  # subnets may be empty/absent in early phases
                pass
        for r in rows:
            if r.get("netuid") in names:
                r["_name"] = names[r["netuid"]]
            if r["event_type"] == "registry_anomaly":
                p = payload_of(r)
                day = int(r["observed_at"]) // 86400
                n = await db.fetchval(
                    "SELECT COUNT(DISTINCT observed_at / 86400) FROM events WHERE event_type = 'registry_anomaly' "
                    "AND json_extract(payload, '$.reason') IS ? AND json_extract(payload, '$.netuid') IS ? "
                    "AND observed_at / 86400 IN (?, ?)", (p.get("reason"), p.get("netuid"), day - 1, day - 2))
                r["_consecutive3"] = (n or 0) >= 2

    async def collect(self, run: RunContext) -> JobOutcome:
        s = app_config.settings
        db = run.rt.db
        rows = await db.fetchall(
            "SELECT id, occupant_key, netuid, event_type, severity, event_at, observed_at, source, payload, "
            "dedupe_key FROM events WHERE alerted_at IS NULL ORDER BY id LIMIT ?", (s.ALERT_MAX_ROWS_PER_TICK,),
            writer=True)
        if not rows:
            return JobOutcome(rows=0, note="sent=0 digest=0 none=0 failed=0")
        await self._annotate(db, rows)
        now = now_ts()
        immediate: list[dict] = []
        n_digest = n_none = 0
        for r in rows:
            route = route_for(r, now)
            if route == "immediate":
                immediate.append(r)
            elif route == "direct":
                await _mark(db, r["id"], "direct")
                n_none += 1
            else:
                await _mark(db, r["id"], route)
                if route == "none":
                    n_none += 1
                else:
                    n_digest += 1
        sent = failed = 0
        if immediate and telegram.telegram_configured():
            if len(immediate) > s.ALERT_BATCH_THRESHOLD:
                lines = [f"📬 <b>{len(immediate)} alerts</b>"] + [render(r, link=False) for r in immediate]
                ok_all = True
                for part in chunk("\n".join(lines)):
                    ok, status = await telegram.send_message(part)
                    if not ok:
                        if status == 400:
                            ok, status = await telegram.send_message(telegram.strip_html(part), parse_mode=None)
                        if not ok:
                            ok_all = False
                            break
                if ok_all:
                    for r in immediate:
                        await _mark(db, r["id"], "immediate")
                    sent = len(immediate)
            else:
                for r in immediate:
                    text = render(r)
                    ok, status = await telegram.send_message(text)
                    if ok:
                        await _mark(db, r["id"], "immediate")
                        sent += 1
                        continue
                    if status == 400:
                        ok, status2 = await telegram.send_message(telegram.strip_html(text), parse_mode=None)
                        if ok:
                            await _mark(db, r["id"], "immediate")
                            sent += 1
                        else:
                            await _mark(db, r["id"], "immediate", {"alert_failed": True})
                            failed += 1
                            logger.warning("alert row poisoned", data={"id": r["id"], "status": status2})
                        continue
                    break          # network error / 5xx / 429 / 403: stop; next tick retries
        return JobOutcome(rows=sent, note=f"sent={sent} digest={n_digest} none={n_none} failed={failed}")
