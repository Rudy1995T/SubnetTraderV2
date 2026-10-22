"""In-service GitHub history backfill, job ``backfill_github`` (spec 17 §2.8, step S9; manual trigger, 06's client).

Per in-scope repo (each ``repo_id`` once, id paths ``/repositories/{id}/…`` with a ``/repos/{full_name}/…`` retry on
404): commits since ``BACKFILL_GITHUB_SINCE`` → ``github_commits`` (``backfilled=1``, 06's author keys / bot rule /
spam flags), releases and merged PRs (+ reviews) → ``github_events`` with 06's dedupe keys and ``backfilled=1``, then
06's ``github_activity_daily`` rollup. When every repo is done, a request-free pass writes ``github_authors_daily``
(06's Electric-Capital classifier, injected day) for the days before 06's first live row. Works only inside
``BACKFILL_GITHUB_WINDOW_UTC`` and keeps ``BACKFILL_GITHUB_RESERVE_REMAINING`` of the PAT's hourly quota for 06.
Progress: ``data/backfill/github_progress.json`` ``{repo_id: {status: done|gone|failed, commits: done|partial|gone,
releases, prs: done|skipped, pages, oldest_seen (epoch s), truncated, path_form: id|full_name, rows, events, last_error,
attempts, run_id, request, full_name}, "_meta": {scope_repos, scope_occupants, since, rollup}}``.
No ``events`` are emitted for historical GitHub facts.
"""
from __future__ import annotations

import asyncio
import bisect
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from app.backfill import common
from app.collectors.github import authors, ghevents, spam
from app.collectors.github.activity import load_window, parse_commit, rebuild_activity_daily, review_logins
from app.collectors.github.client import (GitHubAuthError, GitHubUnavailable, QuotaDeferred, get_client,
                                          parse_link_next)
from app.collectors.github.metrics import classify_authors
from app.collectors.github.models import DAY, iso_ts
from app.collectors.github.orgs import scope_repo_ids
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.ratelimit import BudgetExhausted
from app.util.fs import write_atomic
from app.util.time import floor_day, hhmm, parse_iso, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.collectors.github.client import GitHubClient
    from app.storage.db import Database

JOB = "backfill_github"
SCOPE_ROLES = ("declared", "declared_gone", "primary", "org")
RELEASE_MAX_PAGES = 5


# ── request / progress files ─────────────────────────────────────────────────────────────────────────────────
def _request_path() -> Path:
    return common.backfill_dir() / "github_request.json"


def _sidecar() -> common.Sidecar:
    return common.Sidecar(common.backfill_dir() / "github_progress.json", {})


def read_request() -> dict:
    try:
        req = json.loads(_request_path().read_text())
    except (OSError, ValueError):
        req = {}
    return {"netuids": req.get("netuids"), "prs": req.get("prs", True) is not False, "redo": bool(req.get("redo")),
            "requested_at": req.get("requested_at")}


def write_request(*, netuids: list[int] | None, prs: bool, redo: bool, now: int) -> Path:
    p = _request_path()
    write_atomic(p, {"netuids": netuids, "prs": prs, "redo": redo, "requested_at": now})
    return p


def progress_counts(data: dict) -> dict:
    st = [v.get("status") for k, v in data.items() if k != "_meta" and isinstance(v, dict)]
    return {k: st.count(k) for k in ("done", "gone", "partial", "failed") if st.count(k)}


# ── the overnight window (02 §G4) ────────────────────────────────────────────────────────────────────────────
def window_state(ts: int, window: str) -> tuple[bool, int | None, int | None]:
    """(inside, end_of_this_window, next_open) for one ``HH:MM-HH:MM`` UTC range (may wrap midnight); ``""`` =
    always inside."""
    w = (window or "").strip()
    if not w:
        return True, None, None
    a, b = w.split("-", 1)
    (ah, am), (bh, bm) = hhmm(a.strip()), hhmm(b.strip())
    start_s, end_s = ah * 3600 + am * 60, bh * 3600 + bm * 60
    day = floor_day(ts)
    for d in (day - DAY, day):                       # a wrapping window may have opened yesterday
        s0 = d + start_s
        e0 = d + end_s + (DAY if end_s <= start_s else 0)
        if s0 <= ts < e0:
            return True, e0, None
    nxt = day + start_s
    return False, None, nxt if nxt > ts else nxt + DAY


# ── scope ────────────────────────────────────────────────────────────────────────────────────────────────────
async def plan_repos(db: "Database", *, since_ts: int, max_per_occupant: int,
                     netuids: list[int] | None = None) -> list[dict]:
    """§2.8 scope: declared / declared_gone / primary repos always, org repos when non-fork, non-archived and pushed
    since ``since_ts``; ≤ ``max_per_occupant`` per occupant by ``pushed_at`` desc; each ``repo_id`` once (newest
    first). ``occupants`` lists every occupant the repo serves."""
    q = ("SELECT o.occupant_key, o.role, g.* FROM github_occupant_repos o JOIN github_repos g USING (repo_id) "
         "JOIN occupants oc USING (occupant_key) WHERE o.role IN ('declared','declared_gone','primary','org')")
    params: list[Any] = []
    if netuids:
        q += f" AND oc.netuid IN ({','.join('?' * len(netuids))})"
        params += sorted(int(n) for n in netuids)
    per: dict[str, list[dict]] = defaultdict(list)
    for r in await db.fetchall(q, params, writer=True):
        r = dict(r)
        if r["role"] == "org" and (r["is_fork"] or r["archived"] or (r["pushed_at"] or 0) < since_ts):
            continue
        per[r["occupant_key"]].append(r)
    repos: dict[int, dict] = {}
    for okey, rs in per.items():
        rs.sort(key=lambda x: (x["role"] not in ("declared", "declared_gone", "primary"), -(x["pushed_at"] or 0)))
        for r in rs[:max_per_occupant]:
            rid = int(r["repo_id"])
            cur = repos.setdefault(rid, {**{k: v for k, v in r.items() if k not in ("occupant_key", "role")},
                                         "occupants": [], "roles": []})
            cur["occupants"].append(okey)
            cur["roles"].append(r["role"])
    return sorted(repos.values(), key=lambda x: (-(x.get("pushed_at") or 0), x["repo_id"]))


# ── one repo ─────────────────────────────────────────────────────────────────────────────────────────────────
class RepoBackfill:
    """Fetch → one write transaction per repo (idempotent by sha / dedupe key; an interrupted repo is redone)."""

    def __init__(self, db: "Database", client: "GitHubClient", *, since_ts: int, run_id: str, reserve: int,
                 deadline: float | None, max_pages: int, prs: bool, pr_max: int) -> None:
        self.db = db
        self.client = client
        self.since_ts = int(since_ts)
        self.run_id = run_id
        self.reserve = int(reserve)
        self.deadline = deadline
        self.max_pages = int(max_pages)
        self.prs = prs
        self.pr_max = int(pr_max)
        self.requests = 0

    async def _gate(self) -> None:
        """17's stricter reserve: below ``reserve`` remaining, sleep to the reset (``QuotaDeferred`` past the
        deadline). 06's ``get`` repeats the same check with the same reserve, so the two never disagree."""
        await self.client.quota.wait_if_reserved(JOB, reserve=self.reserve, deadline=self.deadline)

    async def _get(self, path: str, params: dict | None = None):
        await self._gate()
        self.requests += 1
        return await self.client.get(path, params=params, job=JOB, reserve=self.reserve, deadline=self.deadline)

    async def _pages(self, path: str, params: dict | None, max_pages: int, stop=None) -> tuple[list, int, int, bool]:
        """(items, status, pages, truncated); ``stop(page_items)`` → True ends early (not truncated)."""
        items: list = []
        nxt: str | None = path
        q = params
        pages = 0
        while nxt and pages < max_pages:
            r = await self._get(nxt, q)
            pages += 1
            if r.status != 200 or not isinstance(r.json, list):
                return items, r.status, pages, False
            items.extend(r.json)
            if stop is not None and stop(r.json):
                return items, 200, pages, False
            nxt = parse_link_next(r.headers)
            q = None                                  # the next link carries the query
        return items, 200, pages, bool(nxt)

    async def _with_fallback(self, repo: dict, sub: str, params: dict | None, max_pages: int, unit: dict,
                             stop=None) -> tuple[list, int, int, bool]:
        """``/repositories/{id}{sub}``; one retry via ``/repos/{full_name}{sub}`` on 404 (records ``path_form``)."""
        rid = int(repo["repo_id"])
        if unit.get("path_form") != "full_name":
            res = await self._pages(f"/repositories/{rid}{sub}", params, max_pages, stop)
            if res[1] != 404 or not repo.get("full_name"):
                unit.setdefault("path_form", "id")
                return res
        res = await self._pages(f"/repos/{repo['full_name']}{sub}", params, max_pages, stop)
        if res[1] != 404:
            unit["path_form"] = "full_name"
        return res

    async def commits(self, repo: dict, unit: dict) -> tuple[list, list, int | None]:
        rid = int(repo["repo_id"])
        items, status, pages, truncated = await self._with_fallback(
            repo, "/commits", {"since": to_iso(self.since_ts), "per_page": 100}, self.max_pages, unit)
        unit["pages"] = pages
        if status in (404, 409):                       # 409: empty repository
            return [], [], status
        if status != 200:
            raise GitHubUnavailable(f"HTTP {status} on commits")
        rows = [c for c in (parse_commit(i, rid) for i in items) if c is not None]
        pairs = [p for p in (authors.alias_for((i.get("author") or {}).get("login"),
                                               ((i.get("commit") or {}).get("author") or {}).get("email"))
                             for i in items) if p]
        unit["truncated"] = truncated
        unit["oldest_seen"] = min((c.event_at for c in rows), default=None)         # epoch s (oldest author date)
        return rows, pairs, 200

    async def releases(self, repo: dict, unit: dict) -> list[tuple]:
        rid = int(repo["repo_id"])
        items, status, _, _ = await self._with_fallback(repo, "/releases", {"per_page": 100}, RELEASE_MAX_PAGES,
                                                        unit)
        out = []
        for x in items if status == 200 else []:
            tag = x.get("tag_name")
            if not tag or x.get("draft"):
                continue
            out.append((rid, "release", iso_ts(x.get("published_at") or x.get("created_at")), f"release:{rid}:{tag}",
                        {"tag_name": tag, "prerelease": bool(x.get("prerelease")), "draft": False,
                         "backfilled": True}))
        return out

    async def merged_prs(self, repo: dict, unit: dict) -> list[tuple]:
        """Closed PRs by ``updated`` desc until a page ends before ``since``; ≤ ``pr_max`` merged since ``since``;
        one ``/reviews`` call per PR not stored yet (06's live rows and earlier backfill runs are skipped)."""
        rid = int(repo["repo_id"])
        since = self.since_ts

        def _stop(page: list) -> bool:
            last = iso_ts((page[-1] or {}).get("updated_at")) if page else None
            merged = sum(1 for p in collected if p.get("merged_at") and (iso_ts(p["merged_at"]) or 0) >= since)
            return (last is not None and last < since) or merged >= self.pr_max

        collected: list = []

        def stop(page: list) -> bool:
            collected.extend(page)
            return _stop(page)
        items, status, _, _ = await self._with_fallback(
            repo, "/pulls", {"state": "closed", "sort": "updated", "direction": "desc", "per_page": 100},
            self.max_pages, unit, stop)
        if status != 200:
            return []
        merged = [p for p in items if p.get("merged_at") and (iso_ts(p["merged_at"]) or 0) >= since]
        merged.sort(key=lambda p: p["merged_at"], reverse=True)
        merged = merged[: self.pr_max]
        keys = [f"pr_merged:{rid}:{p['number']}" for p in merged]
        have: set[str] = set()
        for i in range(0, len(keys), 500):
            part = keys[i:i + 500]
            have |= {x["dedupe_key"] for x in await self.db.fetchall(
                f"SELECT dedupe_key FROM github_events WHERE dedupe_key IN ({','.join('?' * len(part))})", part,
                writer=True)}
        out: list[tuple] = []
        base = (f"/repos/{repo['full_name']}" if unit.get("path_form") == "full_name" else f"/repositories/{rid}")
        for p in merged:
            key = f"pr_merged:{rid}:{p['number']}"
            if key in have:
                continue
            author = (p.get("user") or {}).get("login")
            rv = await self._get(f"{base}/pulls/{p['number']}/reviews", {"per_page": 100})
            reviewers = review_logins(rv.json if rv.status == 200 else None, author)
            out.append((rid, "pr_merged", iso_ts(p["merged_at"]), key,
                        {"number": p["number"], "author_login": author, "review_count": len(reviewers),
                         "reviewers": reviewers,
                         "author_is_bot": authors.is_bot(author, author_type=(p.get("user") or {}).get("type")),
                         "backfilled": True}))
        return out

    async def run(self, repo: dict, unit: dict) -> dict:
        """→ sidecar fields; raises ``QuotaDeferred`` / ``GitHubAuthError`` / ``BudgetExhausted`` to the job."""
        rid = int(repo["repo_id"])
        now = int(self.client.clock())
        commits, pairs, status = await self.commits(repo, unit)
        if status == 404:
            return {"status": "gone", "commits": "gone", "rows": 0, "last_error": "404 on id and full_name paths"}
        events = await self.releases(repo, unit)
        prs_state = "skipped"
        if self.prs:
            events += await self.merged_prs(repo, unit)
            prs_state = "done"
        known: set[str] = set()
        shas = [c.sha for c in commits]
        for i in range(0, len(shas), 500):
            part = shas[i:i + 500]
            known |= {r["sha"] for r in await self.db.fetchall(
                f"SELECT sha FROM github_commits WHERE sha IN ({','.join('?' * len(part))})", part, writer=True)}
        flags = spam.flag_commits(commits, repo_created_at=repo.get("created_at"),
                                  declared_fork=bool(repo.get("is_fork")))
        new = [c for c in commits if c.sha not in known]
        for c in new:
            c.spam_flags = flags.get(c.sha, [])
            c.backfilled = 1
        n_events = 0
        async with self.db.transaction():
            if new:
                await self.db.insert_or_ignore("github_commits", [c.db_row(observed_at=now, run_id=self.run_id)
                                                                  for c in new])
            await authors.record_identities(self.db, pairs, now)
            for e in events:
                n_events += await ghevents.raw_event(self.db, repo_id=e[0], kind=e[1], event_at=e[2],
                                                     dedupe_key=e[3], payload=e[4], now=now, run_id=self.run_id,
                                                     backfilled=1)
            days = {floor_day(c.event_at) for c in new if c.event_at >= self.since_ts}
            live = {r["day_ts"] for r in await self.db.fetchall(
                "SELECT day_ts FROM github_activity_daily WHERE repo_id = ? AND backfilled = 0", (rid,), writer=True)}
            await rebuild_activity_daily(self.db, rid, days - live, now=now, run_id=self.run_id, repo=repo,
                                         backfilled=1)
            await rebuild_activity_daily(self.db, rid, days & live, now=now, run_id=self.run_id, repo=repo)
        return {"status": "done", "commits": "partial" if unit.get("truncated") else "done", "releases": "done",
                "prs": prs_state, "rows": len(new), "events": n_events, "last_error": None}


# ── github_authors_daily (request-free, after every repo is done) ────────────────────────────────────────────
async def rollup_authors_daily(db: "Database", occupant_keys: list[str], *, since_ts: int, until_ts: int, now: int,
                               run_id: str) -> int:
    """06's classifier per occupant and day ``D ∈ [since, until)`` over its 06 scope ``R(o)`` (human, unflagged
    commits; trailing 84 d sliced by bisect), written ``backfilled=1``. ``until`` = 06's first live row, so the live
    rows are never touched; earlier backfilled rows of the occupant are replaced."""
    total = 0
    for okey in occupant_keys:
        rids = await scope_repo_ids(db, okey, include_declared_gone=True)
        if not rids:
            continue
        commits = await load_window(db, rids, since_ts - 84 * DAY, until_ts)
        keys = await authors.resolve_author_keys(db, [c.author_key for c in commits])
        for c in commits:
            c.author_key = keys.get(c.author_key, c.author_key)
        meta = {int(r["repo_id"]): r for r in await db.fetchall(
            f"SELECT repo_id, created_at, is_fork FROM github_repos WHERE repo_id IN ({','.join('?' * len(rids))})",
            rids)}
        by_repo: dict[int, list] = defaultdict(list)
        for c in commits:
            by_repo[c.repo_id].append(c)
        flags: dict[str, list[str]] = {}
        for rid, cs in by_repo.items():
            m = meta.get(rid) or {}
            flags.update(spam.flag_commits(cs, repo_created_at=m.get("created_at"),
                                           declared_fork=bool(m.get("is_fork"))))
        human = sorted(spam.human_unflagged(commits, flags), key=lambda c: c.event_at)

        def _rows() -> list[dict]:
            ts = [c.event_at for c in human]
            out: list[dict] = []
            for d in range(floor_day(since_ts), until_ts, DAY):
                win = human[bisect.bisect_left(ts, d - 84 * DAY):bisect.bisect_left(ts, d + DAY)]
                if not win:
                    continue
                for a in classify_authors(win, d).values():
                    out.append({"occupant_key": okey, "author_key": a.author_key, "day_ts": d,
                                "commits_today": a.today, "cd28": a.w1, "cd56": a.w2, "cd84_3": a.w3, "cd84": a.cd84,
                                "dev_class": a.dev_class, "repos_json": json.dumps(a.repos), "observed_at": now,
                                "run_id": run_id, "backfilled": 1})
            return out
        rows = await asyncio.to_thread(_rows)
        async with db.transaction():
            await db.execute("DELETE FROM github_authors_daily WHERE occupant_key = ? AND backfilled = 1 AND "
                             "day_ts < ?", (okey, until_ts))
            if rows:
                await db.upsert("github_authors_daily", rows, conflict=("occupant_key", "author_key", "day_ts"))
        total += len(rows)
        await asyncio.sleep(0)
    return total


# ── the job ──────────────────────────────────────────────────────────────────────────────────────────────────
class BackfillGithubJob(CollectorJob):
    id = JOB
    phase = 4
    timeout_sec = 14400
    kind = "manual"
    paid = False

    def health_extra(self) -> dict | None:
        data = _sidecar().data
        prog = progress_counts(data)
        return {"progress": prog, "rollup": (data.get("_meta") or {}).get("rollup")} if data else None

    async def collect(self, run: RunContext) -> JobOutcome:
        s = run.settings
        db = run.rt.db
        client = get_client(run.rt)
        return await run_backfill(db, client, s, run_id=run.run_id,
                                  soft_deadline=client.clock() + (self.effective_timeout() or self.spec.timeout_sec)
                                  - 300, paused=self.paused_reason)


async def run_backfill(db: "Database", client: "GitHubClient", s: Any, *, run_id: str, soft_deadline: float,
                       paused=lambda: None, side: common.Sidecar | None = None) -> JobOutcome:
    """One trigger: repos (inside the window, until the soft deadline) → authors rollup when none is pending."""
    req = read_request()
    side = side or _sidecar()
    since_ts = parse_iso(f"{s.BACKFILL_GITHUB_SINCE}T00:00:00Z")
    t0 = time.monotonic()
    repos = await plan_repos(db, since_ts=since_ts, max_per_occupant=int(s.BACKFILL_GITHUB_MAX_REPOS_PER_OCCUPANT),
                             netuids=req["netuids"])
    stamp = req["requested_at"] if req["redo"] else None      # --redo: each repo once more per request

    def _due(r: dict) -> bool:
        u = side.data.get(str(r["repo_id"])) or {}
        return u.get("status") not in ("done", "gone") or (stamp is not None and u.get("request") != stamp)
    todo = [r for r in repos if _due(r)]
    meta = side.data.setdefault("_meta", {})
    meta.update({"scope_repos": len(repos), "scope_occupants": len({o for r in repos for o in r["occupants"]}),
                 "since": s.BACKFILL_GITHUB_SINCE})
    now = int(client.clock())
    inside, w_end, _ = window_state(now, s.BACKFILL_GITHUB_WINDOW_UTC)
    rows = 0
    requests = 0
    stopped: str | None = None
    if todo and not inside:
        side.save()
        return JobOutcome(rows=0, note=f"partial: outside window, {len(todo)} units left")
    deadline = min(soft_deadline, float(w_end)) if w_end is not None else soft_deadline
    bf = RepoBackfill(db, client, since_ts=since_ts, run_id=run_id, reserve=int(s.BACKFILL_GITHUB_RESERVE_REMAINING),
                      deadline=deadline, max_pages=int(s.BACKFILL_GITHUB_MAX_PAGES_PER_REPO),
                      prs=bool(s.BACKFILL_GITHUB_PRS_ENABLED) and req["prs"],
                      pr_max=int(s.BACKFILL_GITHUB_PR_MAX_PER_REPO))
    left = 0
    try:
        for i, repo in enumerate(todo):
            reason = paused()
            if reason:
                stopped = f"paused:{reason}"
                left = len(todo) - i
                break
            if client.clock() >= deadline:
                stopped = "outside window" if w_end is not None and client.clock() >= w_end else "deadline"
                left = len(todo) - i
                break
            key = str(repo["repo_id"])
            unit = side.data.setdefault(key, {})
            try:
                r = await bf.run(repo, unit)
            except QuotaDeferred:
                stopped = "quota"
                left = len(todo) - i
                break
            except (GitHubAuthError, BudgetExhausted):
                side.save()
                raise
            except Exception as exc:  # one bad repo never aborts the run (GitHubUnavailable, decode, …)
                r = {"status": "failed", "rows": 0, "last_error": f"{type(exc).__name__}: {str(exc)[:200]}"}
                logger.warning("backfill_github repo failed", data={"repo_id": repo["repo_id"],
                                                                    "error": r["last_error"]})
            unit.update(r, run_id=run_id, request=stamp, full_name=repo.get("full_name"),
                        attempts=int(unit.get("attempts", 0)) + 1)
            side.save()
            rows += int(r.get("rows") or 0)
    finally:
        requests = bf.requests
        side.save()
    # a repo that failed 3 times no longer holds back the rollup (it stays listed as failed in the sidecar)
    pending = left + sum(1 for r in repos if (u := side.data.get(str(r["repo_id"])) or {}).get("status") == "failed"
                         and int(u.get("attempts") or 0) < 3)
    counts = progress_counts(side.data)
    note = (f"repos={len(repos)} " + " ".join(f"{k}={v}" for k, v in counts.items()) + f" requests={requests}")
    if stopped:
        return JobOutcome(rows=rows, note=f"partial: {stopped}, {left} units left {note}",
                          meta={"progress": counts, "requests": requests})
    if pending:
        return JobOutcome(rows=rows, note=f"{note} pending={pending}", meta={"progress": counts, "requests": requests})
    rollup = meta.get("rollup") or {}
    if rollup.get("status") != "done" or (stamp is not None and rollup.get("request") != stamp):
        first_live = await db.fetchval("SELECT MIN(day_ts) FROM github_authors_daily WHERE backfilled = 0")
        until = int(first_live) if first_live is not None else floor_day(int(client.clock()))
        okeys = sorted({o for r in repos for o in r["occupants"]})
        n = await rollup_authors_daily(db, okeys, since_ts=since_ts, until_ts=until, now=int(client.clock()),
                                       run_id=run_id)
        meta["rollup"] = {"status": "done", "rows": n, "until": to_iso(until), "occupants": len(okeys),
                          "request": stamp}
        side.save()
        note += f" authors_daily={n}"
    await common.emit_step_event(db, "backfill_done", job=JOB, era=None, run_id=run_id, rows=rows,
                                 seconds=round(time.monotonic() - t0, 1))
    return JobOutcome(rows=rows, note=note, meta={"progress": counts, "requests": requests})
