"""Unauthenticated GitHub last-push probe (spec 19 §2.5): one request per declared repo per UTC day →
evidence key ``repo_pushed_days`` (``source='github'``). Runs only while ``GITHUB_TOKEN`` is empty (06 takes over).

Repo URL → ``GET /repos/{owner}/{repo}``; org-only URL → ``GET /orgs/{org}/repos?sort=pushed&per_page=1`` (the most
recently pushed repo). No link → ``absent``; 404/451 → ``failed http_404`` (unobservable, not idle).
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable

from app.jobs import CollectorJob, JobOutcome, RunContext
from app.storage.pit import put_metric_values
from app.util.time import floor_day

if TYPE_CHECKING:  # pragma: no cover
    from app.http import Http
    from app.storage.db import Database

SPEC_VERSION = "ts-1.0"
API = "https://api.github.com"
_GH = re.compile(r"^(?:https?://)?(?:www\.)?github\.com/([A-Za-z0-9_.-]+)(?:/([A-Za-z0-9_.-]+))?", re.I)


def parse_github(url: str | None) -> tuple[str, str | None] | None:
    if not url:
        return None
    m = _GH.match(url.strip())
    if not m:
        return None
    owner, repo = m.group(1), m.group(2)
    if repo and repo.endswith(".git"):
        repo = repo[:-4]
    if owner.lower() in ("orgs", "users", "sponsors", "topics", "features"):
        return None
    return owner, (repo or None)


def _iso_ts(s: str | None) -> int | None:
    if not s:
        return None
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())


def _row(occ: str, as_of: int, value: float | None, vj: dict, *, run_id: str, observed_at: int) -> dict:
    vj = {**vj, "def": "wb-1"}
    if vj.get("status") != "ok":
        value = None
    return {"occupant_key": occ, "metric_key": "repo_pushed_days", "as_of": as_of, "spec_version": SPEC_VERSION,
            "stage": "raw", "value": value, "value_json": vj, "observed_at": observed_at, "source": "github",
            "run_id": run_id, "backfilled": 0}


async def probe_one(http: "Http", owner: str, repo: str | None, now: int) -> tuple[float | None, dict]:
    hdr = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    if repo:
        url = f"{API}/repos/{owner}/{repo}"
        resp = await http.request("github", "GET", url, headers=hdr, ok_statuses=(200, 404, 451), retries=1)
        if resp.status_code != 200:
            return None, {"status": "failed", "reason": f"http_{resp.status_code}", "repo": f"{owner}/{repo}"}
        j = resp.json()
        info = {"repo": j.get("full_name"), "pushed_at": j.get("pushed_at"), "archived": bool(j.get("archived")),
                "stars": j.get("stargazers_count"), "kind": "repo"}
    else:
        url = f"{API}/orgs/{owner}/repos"
        resp = await http.request("github", "GET", url, headers=hdr, params={"sort": "pushed", "per_page": 1},
                                  ok_statuses=(200, 404, 451), retries=1)
        if resp.status_code == 404:
            resp = await http.request("github", "GET", f"{API}/users/{owner}/repos", headers=hdr,
                                      params={"sort": "pushed", "per_page": 1}, ok_statuses=(200, 404, 451), retries=1)
        if resp.status_code != 200 or not resp.json():
            return None, {"status": "failed", "reason": f"http_{resp.status_code}" if resp.status_code != 200
                          else "no_public_repos", "repo": owner}
        j = resp.json()[0]
        info = {"repo": j.get("full_name"), "pushed_at": j.get("pushed_at"), "archived": bool(j.get("archived")),
                "stars": j.get("stargazers_count"), "kind": "org_latest"}
    ts = _iso_ts(info["pushed_at"])
    if ts is None:
        return None, {"status": "failed", "reason": "no_pushed_at", **info}
    return max(0.0, (now - ts) / 86400), {"status": "ok", **info}


async def due(db: "Database", as_of: int) -> list[dict]:
    return await db.fetchall(
        "SELECT s.netuid, s.occupant_key, s.github_url FROM subnets s JOIN occupants o USING (occupant_key) "
        "WHERE s.netuid != 0 AND o.status IN ('active','pre_launch') AND NOT EXISTS (SELECT 1 FROM metric_values m "
        "WHERE m.occupant_key = s.occupant_key AND m.metric_key = 'repo_pushed_days' AND m.as_of = ? AND "
        "m.stage = 'raw') ORDER BY s.netuid", (as_of,), writer=True)


async def run_probe(db: "Database", http: "Http", now: int, *, per_run: int, run_id: str,
                    clock: Callable[[], int] | None = None) -> dict[str, int]:
    """Rows are written as each repo is probed (the no-token bucket paces ~72 s/request, so a run takes up to ~45
    min; a timeout or restart keeps what was already read)."""
    from app.util.time import now_ts
    clock = clock or now_ts
    as_of = floor_day(now)
    stats = {"ok": 0, "absent": 0, "failed": 0, "requests": 0, "rows": 0}
    for t in await due(db, as_of):
        parsed = parse_github(t["github_url"])
        if parsed is None:
            stats["rows"] += await put_metric_values(db, [_row(
                t["occupant_key"], as_of, None, {"status": "absent", "reason": "no_github_link", "url": t["github_url"]},
                run_id=run_id, observed_at=now)])
            stats["absent"] += 1
            continue
        if stats["requests"] >= per_run:
            continue
        stats["requests"] += 1
        value, vj = await probe_one(http, parsed[0], parsed[1], now)
        t_obs = max(now, clock())
        if value is not None:
            value = max(0.0, value + (t_obs - now) / 86400)
        stats["rows"] += await put_metric_values(db, [_row(t["occupant_key"], as_of, value,
                                                           {**vj, "url": t["github_url"]}, run_id=run_id,
                                                           observed_at=t_obs)])
        stats["ok" if vj["status"] == "ok" else "failed"] += 1
    return stats


class WbGithubProbeJob(CollectorJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        st: dict[str, Any] = await run_probe(run.rt.db, run.rt.http, run.started_at,
                                             per_run=run.settings.WB_GITHUB_PROBE_PER_RUN, run_id=run.run_id)
        return JobOutcome(rows=st["rows"], note=" ".join(f"{k}={v}" for k, v in st.items()), meta=st)
