"""Event helpers for the GitHub collector (spec 06 §3.4): occupant ``events`` rows via 03's ``emit_event`` and raw
repo-side ``github_events`` rows. Dedupe patterns are 03 §3.9's."""
from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

from app.storage.events import emit_event

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

D30 = 2_592_000


def sha1(*parts: Any) -> str:
    return hashlib.sha1("|".join(str(p) for p in parts).encode()).hexdigest()


def d30(ts: int) -> int:
    return int(ts) // D30


def dedupe_for(event_type: str, occ: str, now: int, **k: Any) -> str:
    if event_type in ("repo_renamed", "repo_moved"):
        return sha1(occ, event_type, k["repo_id"], k["to"])
    if event_type == "repo_gone":
        return f"{sha1(occ, event_type, k['declared'])}|{d30(now)}"
    if event_type == "repo_restored":
        return f"{sha1(occ, event_type, k['repo_id'])}|{d30(now)}"
    if event_type in ("repo_archived", "org_repo_archived"):
        return sha1(occ, event_type, k["repo_id"])
    if event_type == "identity_repo_changed":
        return sha1(occ, event_type, k["to_ref"])
    if event_type == "commit_spam_suspected":
        return f"{occ}|{d30(now)}"
    if event_type == "source_disagreement":
        return f"{occ}|github_repo|{k['pair_hash']}"
    raise ValueError(f"no dedupe rule for {event_type}")


async def emit(db: "Database", event_type: str, *, occupant_key: str, netuid: int | None, payload: dict,
               now: int, run_id: str | None, **dedupe_args: Any) -> int | None:
    return await emit_event(db, event_type=event_type, occupant_key=occupant_key, netuid=netuid, event_at=now,
                            observed_at=now, payload=payload,
                            dedupe_key=dedupe_for(event_type, occupant_key, now, **dedupe_args),
                            source="github", run_id=run_id)


async def raw_event(db: "Database", *, repo_id: int, kind: str, dedupe_key: str, payload: dict, now: int,
                    run_id: str, event_at: int | None = None, backfilled: int = 0) -> int:
    res = await db.execute(
        "INSERT OR IGNORE INTO github_events (repo_id, event_kind, event_at, observed_at, dedupe_key, payload_json, "
        "source, backfilled, run_id) VALUES (?,?,?,?,?,?,?,?,?)",
        (int(repo_id), kind, event_at, int(now), dedupe_key, json.dumps(payload, separators=(",", ":"), default=str),
         "github_api", backfilled, run_id))
    return max(0, res.rowcount)
