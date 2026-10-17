"""TMC ``github-stats`` cross-check (spec 06 §8): our declared-repo 30-d commits vs ``chain_hourly.commits_30d``.

No metric effect and no events: results go to the probe run's ``collector_runs.meta_json`` and log lines. TMC is
called only through 05's ``TmcClient`` (G2), ≤ 1 table + ≤ 20 per-netuid requests (05's G3 budget).
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from app.logging.logger import logger

if TYPE_CHECKING:  # pragma: no cover
    from app.collectors.chain_tmc import TmcClient
    from app.storage.db import Database

MAX_DETAIL = 20


def mismatch(ours: int | None, tmc: int | None) -> bool:
    if ours is None or tmc is None:
        return False
    if (ours == 0) != (tmc == 0):
        return True
    lo, hi = sorted((ours, tmc))
    return lo > 0 and hi / lo > 2.0


async def run(db: "Database", tmc: "TmcClient | None", *, now: int) -> dict[str, Any]:
    rows = await db.fetchall(
        "SELECT o.netuid, d.occupant_key, d.repo_id, d.owner, d.repo FROM github_declared_refs d JOIN occupants o "
        "ON o.occupant_key = d.occupant_key JOIN github_occupant_repos r ON r.occupant_key = d.occupant_key AND "
        "r.repo_id = d.repo_id AND r.role = 'declared' WHERE d.kind = 'repo' AND d.state = 'resolved'")
    latest = {int(r["netuid"]): r for r in await db.fetchall(
        "SELECT h.netuid, h.commits_30d, h.hour_ts FROM chain_hourly h JOIN (SELECT netuid, MAX(hour_ts) AS t FROM "
        "chain_hourly WHERE commits_30d IS NOT NULL AND hour_ts >= ? GROUP BY netuid) m ON m.netuid = h.netuid AND "
        "m.t = h.hour_ts", (now - 3 * 86400,))}
    out: dict[str, Any] = {"checked": 0, "mismatches": 0, "tmc_requests": 0, "details": []}
    seen: set[int] = set()
    for r in rows:
        n = int(r["netuid"])
        if n in seen or n not in latest:
            continue
        seen.add(n)
        ours = await db.fetchval("SELECT COUNT(*) FROM github_commits WHERE repo_id = ? AND event_at >= ?",
                                 (r["repo_id"], now - 30 * 86400))
        theirs = latest[n]["commits_30d"]
        out["checked"] += 1
        if not mismatch(int(ours or 0), int(theirs) if theirs is not None else None):
            continue
        out["mismatches"] += 1
        d: dict[str, Any] = {"netuid": n, "ours": int(ours or 0), "tmc": theirs, "declared": f"{r['owner']}/{r['repo']}"}
        if tmc is not None and out["tmc_requests"] < MAX_DETAIL:
            resp = await tmc.get(f"/subnets/github-stats/{n}/")
            out["tmc_requests"] += 1
            if resp.ok and isinstance(resp.json, dict):
                j = resp.json
                d["tmc_key"] = f"{j.get('owner')}/{j.get('repo')}"
                d["tmc_last_pushed_at"] = j.get("last_pushed_at")
                d["tmc_is_valid"] = j.get("is_valid")
        logger.info("github.crosscheck.mismatch", data=d)
        out["details"].append(d)
    return out
