"""The single events writer (spec 03 §3.9, 02 §D1–§D3).

``emit_event`` validates against ``catalog.EVENT_TYPES`` (unknown type or missing required payload key →
ValueError, before any SQL; extra keys allowed), then ``INSERT … ON CONFLICT DO NOTHING RETURNING id``
(None on a dedupe hit, via ``ux_events_dedupe``).
"""
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Mapping

from app.storage.catalog import EVENT_TYPES, SEVERITIES, SOURCES

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database


def validate_event(event_type: str, payload: Mapping[str, Any] | None, severity: str | None, source: str) -> str:
    spec = EVENT_TYPES.get(event_type)
    if spec is None:
        raise ValueError(f"unknown event_type {event_type!r} (not in the 03 §3.9 catalog)")
    payload = payload or {}
    if not isinstance(payload, Mapping):
        raise ValueError("payload must be a JSON object")
    keys = spec.payload_keys
    if payload.get("backfilled") is True and spec.backfill_payload_keys:
        keys = spec.backfill_payload_keys
    missing = [k for k in keys if k not in payload]
    if missing:
        raise ValueError(f"{event_type}: missing required payload keys {missing}")
    sev = severity or spec.severity
    if sev not in SEVERITIES:
        raise ValueError(f"severity must be one of {SEVERITIES}, got {sev!r} (02 §B2)")
    if source not in SOURCES:
        raise ValueError(f"source {source!r} is not in the 02 §B9 vocabulary")
    return sev


async def emit_event(db: "Database", *, event_type: str, occupant_key: str | None, netuid: int | None,
                     severity: str | None = None, event_at: int | None, observed_at: int,
                     payload: Mapping[str, Any] | None, dedupe_key: str | None, source: str,
                     run_id: str | None, alerted_at: int | None = None) -> int | None:
    sev = validate_event(event_type, payload, severity, source)
    if occupant_key is None and netuid is not None:
        raise ValueError("a system event (occupant_key NULL) must have netuid NULL too")
    body = json.dumps(dict(payload or {}), separators=(",", ":"), sort_keys=True, default=str)
    rows = await db.execute_returning(
        "INSERT INTO events (occupant_key, netuid, event_type, severity, event_at, observed_at, source, payload, "
        "dedupe_key, run_id, alerted_at) VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING RETURNING id",
        (occupant_key, netuid, event_type, sev, event_at, int(observed_at), source, body, dedupe_key, run_id,
         alerted_at),
    )
    return int(rows[0]["id"]) if rows else None


async def mark_alerted(db: "Database", event_id: int, *, at: int | None = None,
                       payload_patch: Mapping[str, Any] | None = None) -> bool:
    """Set ``alerted_at`` once (and merge ``payload_patch`` in the same UPDATE); False when already alerted."""
    from app.util.time import now_ts

    when = int(at if at is not None else now_ts())
    if payload_patch:
        res = await db.execute(
            "UPDATE events SET alerted_at = ?, payload = json_patch(payload, ?) WHERE id = ? AND alerted_at IS NULL",
            (when, json.dumps(dict(payload_patch), separators=(",", ":"), default=str), event_id),
        )
    else:
        res = await db.execute("UPDATE events SET alerted_at = ? WHERE id = ? AND alerted_at IS NULL",
                               (when, event_id))
    return res.rowcount > 0
