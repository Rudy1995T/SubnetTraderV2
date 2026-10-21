"""``needs_review_digest`` (spec 14 §2.6): the weekly Telegram digest — needs-review buckets, every digest-routed
event since the previous digest, and a link to ``{ui}/events``. The ``last_event_id`` watermark moves only after
Telegram accepted every chunk, and never past the oldest row the dispatcher has not routed yet.
"""
from __future__ import annotations

import json

from app.api import queries as Q
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.notifications import telegram
from app.notifications.commands import render_digest_events, render_needs_review
from app.notifications.format import chunk

JOB = "needs_review_digest"
KEY = "last_event_id"


async def digest_watermark(db) -> int:
    raw = await db.fetchval("SELECT value_json FROM job_state WHERE job = ? AND key = ?", (JOB, KEY), writer=True)
    try:
        return int(json.loads(raw)) if raw else 0
    except (TypeError, ValueError):
        return 0


async def render_digest(db, *, now: int, after_id: int, upto_id: int) -> str:
    return (await render_needs_review(db, now=now)) + "\n\n" + await render_digest_events(db, after_id, upto_id)


class NeedsReviewDigestJob(CollectorJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        db = run.rt.db
        after = await digest_watermark(db)
        upto = await Q.digest_upto(db)
        text = await render_digest(db, now=int(run.started_at), after_id=after, upto_id=upto)
        parts = chunk(text)
        if not telegram.telegram_configured():
            return JobOutcome(rows=0, note=f"telegram not configured; {len(parts)} chunks not sent")
        for part in parts:
            ok, status = await telegram.send_message(part)
            if not ok and status == 400:
                ok, status = await telegram.send_message(telegram.strip_html(part), parse_mode=None)
            if not ok:
                raise RuntimeError(f"digest send failed (status {status}); watermark stays at {after}")
        if upto > after:
            await self.set_state(KEY, upto)
        return JobOutcome(rows=len(parts), note=f"chunks={len(parts)} events {after}→{max(after, upto)}")
