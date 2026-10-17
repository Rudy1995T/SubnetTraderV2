"""``AlertLatch`` — latched pager for runtime/ops conditions (spec 01 §2.11, 02 §D3 amended, 16 §2.13.2).

``fire(key, text, event_type=…)`` writes an ``events`` row (``alerted_at NULL``) through ``emit_event`` and the
``alert_dispatch`` job delivers it; a direct ``send_alert`` happens only (a) for process notices without an
``event_type`` (e.g. ``service_online``) and (b) as the fallback when ``emit_event`` raises (DB error, or a
validation error, logged at ERROR so a misnamed type is visible). There is no ``direct`` route and no
"dispatcher inactive" fallback (02 §D3, amended 2026-10-16). A latched key re-fires
only after ``ALERT_REPEAT_MIN``. Latches are in memory (a restart re-arms them).
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Awaitable, Callable

import app.config as app_config
from app.logging.logger import logger
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.runtime import Runtime
    from app.storage.db import Database


async def _default_sink(text: str) -> bool:
    from app.notifications import telegram
    return await telegram.send_alert(text)


class AlertLatch:
    def __init__(self, db: "Database | None", *, sink: Callable[[str], Awaitable[bool]] | None = None,
                 clock: Callable[[], int] = now_ts) -> None:
        self.db = db
        self._sink = sink
        self._clock = clock
        self._latched: dict[str, int] = {}
        self._last_fired: dict[str, int] = {}

    async def sink(self, text: str) -> bool:
        if self._sink is not None:
            return await self._sink(text)
        return await _default_sink(text)

    def is_latched(self, key: str) -> bool:
        return key in self._latched

    def snapshot(self) -> dict[str, int]:
        return dict(self._latched)

    async def _deliver(self, text: str, event_type: str | None, severity: str, payload: dict | None,
                       dedupe_key: str | None, source: str) -> bool:
        if event_type is None:
            return await self.sink(text)
        now = self._clock()
        if self.db is None:
            return await self.sink(text)
        try:
            from app.storage.events import emit_event
            await emit_event(self.db, event_type=event_type, occupant_key=None, netuid=None, severity=severity,
                             event_at=now, observed_at=now, payload={**(payload or {}), "text": text},
                             dedupe_key=dedupe_key, source=source, run_id=None, alerted_at=None)
            return True
        except Exception as exc:
            logger.error("alert event write failed; sending direct", data={"event_type": event_type,
                                                                           "error": f"{type(exc).__name__}: {exc}"})
            return await self.sink(text)

    async def fire(self, key: str, text: str, *, event_type: str | None = None, severity: str = "warning",
                   payload: dict | None = None, dedupe_key: str | None = None, source: str = "runtime") -> bool:
        now = self._clock()
        last = self._last_fired.get(key)
        if key in self._latched and last is not None and now - last < app_config.settings.ALERT_REPEAT_MIN * 60:
            return False
        self._latched.setdefault(key, now)
        self._last_fired[key] = now
        return await self._deliver(text, event_type, severity, payload, dedupe_key, source)

    async def clear(self, key: str, text: str | None = None, *, event_type: str | None = None,
                    severity: str = "info", payload: dict | None = None, dedupe_key: str | None = None,
                    source: str = "runtime") -> bool:
        was = self._latched.pop(key, None) is not None
        self._last_fired.pop(key, None)
        if text is None or not was:
            return False
        return await self._deliver(text, event_type, severity, payload, dedupe_key, source)


async def dispatcher_active(rt: "Runtime | None", db: "Database | None" = None) -> bool:
    """True iff alert_dispatch is built, enabled, not paused and ran (or the process started) ≤ 300 s ago."""
    now = now_ts()
    if rt is not None:
        job = rt.jobs.get("alert_dispatch")
        if job is None or not job.enabled() or job.paused_reason():
            return False
        last = job.status_raw["last_run"] or rt.started_at
        return now - last <= 300
    if db is not None:
        v = await db.fetchval("SELECT MAX(finished_at) FROM collector_runs WHERE job = 'alert_dispatch'")
        return bool(v and v >= now - 300)
    return False
