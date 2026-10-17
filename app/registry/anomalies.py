"""``registry_anomaly`` 3-consecutive-runs gate (spec 04 §3.5): job_state counters, emitted at the Nth consecutive
run showing the condition and at most once per UTC day while it persists."""
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.storage.events import emit_event

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database


async def _get(db: "Database", job: str, key: str) -> int:
    v = await db.fetchval("SELECT value_json FROM job_state WHERE job = ? AND key = ?", (job, key), writer=True)
    try:
        return int(json.loads(v)) if v else 0
    except (TypeError, ValueError):
        return 0


async def _set(db: "Database", job: str, key: str, n: int, now: int) -> None:
    await db.upsert("job_state", [{"job": job, "key": key, "value_json": json.dumps(n), "updated_at": now}],
                    conflict=("job", "key"))


async def gate(db: "Database", *, job: str, reason: str, condition: bool, now: int, run_id: str | None,
               netuid: int | None = None, occupant_key: str | None = None, detail: Any = None,
               scope: str | None = None) -> int | None:
    """Advance/reset the counter; returns the event id when emitted. ``scope`` separates counters that share a
    reason and netuid (e.g. ``source_failed`` per identity source)."""
    key = f"anomaly:{reason}:{'' if netuid is None else netuid}" + (f":{scope}" if scope else "")
    n = await _get(db, job, key)
    if not condition:
        if n:
            await _set(db, job, key, 0, now)
        return None
    n += 1
    await _set(db, job, key, n, now)
    if n < app_config.settings.REGISTRY_ANOMALY_CONSECUTIVE_RUNS:
        return None
    payload = {"reason": reason, "detail": detail, "consecutive": n}
    if netuid is not None:
        payload["netuid"] = netuid
    return await emit_event(db, event_type="registry_anomaly", occupant_key=occupant_key,
                            netuid=netuid if occupant_key else None, severity="warning", event_at=now,
                            observed_at=now, payload=payload,
                            dedupe_key=f"{reason}|{'' if netuid is None else netuid}|{now // 86400}"
                            + (f"|{scope}" if scope else ""),
                            source="registry", run_id=run_id)


async def counters(db: "Database", job: str) -> dict[str, int]:
    rows = await db.fetchall("SELECT key, value_json FROM job_state WHERE job = ? AND key LIKE 'anomaly:%'", (job,))
    out = {}
    for r in rows:
        try:
            v = int(json.loads(r["value_json"]))
        except (TypeError, ValueError):
            continue
        if v:
            out[r["key"][8:]] = v
    return out
