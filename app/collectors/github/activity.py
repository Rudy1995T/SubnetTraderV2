"""Per-repo activity collection (spec 06 §2.5). Every path is id-based (``/repositories/{id}/…``, verified
2026-10-17), so a rename between resolve cycles cannot break collection. Writes are committed per repo; the
watermark and ``activity_ok_at`` advance only with them (power-loss safe, idempotent by sha / dedupe key)."""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.collectors.github import authors, ghevents, spam
from app.collectors.github.client import GitHubUnavailable, QuotaDeferred, parse_redirect_id
from app.collectors.github.models import DAY, CommitRow, RepoRow, iso_ts
from app.logging.logger import logger
from app.util.time import floor_day, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.collectors.github.client import GitHubClient
    from app.storage.db import Database

JOB = "github_activity"


def parse_commit(item: dict, repo_id: int) -> CommitRow | None:
    c = item.get("commit") or {}
    a = c.get("author") or {}
    cm = c.get("committer") or {}
    user = item.get("author") or {}
    sha = item.get("sha")
    event_at = iso_ts(a.get("date")) or iso_ts(cm.get("date"))
    if not sha or event_at is None:
        return None
    login = user.get("login")
    email = a.get("email")
    msg = (c.get("message") or "").split("\n", 1)[0][:200]
    bot = authors.is_bot(login, author_type=user.get("type"), name=a.get("name"), email=email)
    n_par = len(item.get("parents") or [])
    ver = (c.get("verification") or {}).get("verified")
    return CommitRow(sha=sha, repo_id=repo_id, author_key=authors.author_key(login, email), event_at=event_at,
                     n_parents=n_par, author_login=login, author_name=(a.get("name") or None),
                     author_email_hash=authors.email_hash(email), author_type=user.get("type"),
                     committer_date=iso_ts(cm.get("date")), message_head=msg, message_norm_hash=spam.message_hash(msg),
                     verified=None if ver is None else (1 if ver else 0), is_bot=bot, is_merge=n_par >= 2)


@dataclass
class RepoActivityResult:
    repo_id: int
    ok: bool = False
    commits_new: int = 0
    events_new: int = 0
    days: set[int] = field(default_factory=set)
    calls_failed: int = 0
    error: str | None = None


class ActivityCollector:
    def __init__(self, db: "Database", client: "GitHubClient", *, run_id: str, now: int,
                 deadline: float | None = None, job: str = JOB) -> None:
        self.db = db
        self.client = client
        self.run_id = run_id
        self.now = int(now)
        self.deadline = deadline
        self.job = job
        self._template_tree: dict[str, str] | None = None
        self._template_sha: str | None = None
        self._template_branch: str | None = None
        self._template_id: int | None = None
        self._template_full: str | None = None

    async def _get(self, path: str, **kw: Any):
        return await self.client.get(path, job=self.job, deadline=self.deadline, **kw)

    # ── commits ──────────────────────────────────────────────────────────────────────────────────────────────
    async def fetch_commits(self, repo: dict) -> tuple[list[CommitRow], int, list[tuple[str, str]]]:
        """(rows, status, identity pairs): the email→login pairs are taken here, the only place the raw email
        is seen (it is never stored)."""
        s = app_config.settings
        rid = int(repo["repo_id"])
        wm = repo.get("last_commit_seen_at")
        floor = self.now - int(s.GITHUB_ACTIVITY_LOOKBACK_DAYS) * DAY
        since = max(floor, int(wm) - 2 * DAY) if wm else floor
        page = await self.client.get_all_pages(f"/repositories/{rid}/commits",
                                               params={"since": to_iso(since), "per_page": 100},
                                               max_pages=int(s.GITHUB_ACTIVITY_MAX_COMMIT_PAGES), job=self.job,
                                               deadline=self.deadline)
        if page.status == 409:
            return [], 409, []
        rows = [c for c in (parse_commit(i, rid) for i in page.items) if c is not None]
        pairs = [p for p in (authors.alias_for((i.get("author") or {}).get("login"),
                                               ((i.get("commit") or {}).get("author") or {}).get("email"))
                             for i in page.items) if p]
        return rows, page.status, pairs

    async def _sample_stats(self, rid: int, commits: list[CommitRow], flags: dict[str, list[str]]) -> int:
        s = app_config.settings
        n = 0
        for c in commits:
            if n >= int(s.GITHUB_COMMIT_STATS_SAMPLE):
                break
            if not spam.needs_stats(flags.get(c.sha, [])):
                continue
            try:
                r = await self._get(f"/repositories/{rid}/commits/{c.sha}")
            except GitHubUnavailable:
                continue
            n += 1
            if r.ok and isinstance(r.json, dict):
                c.stats_total = (r.json.get("stats") or {}).get("total")
                c.whitespace_only = 1 if spam.whitespace_only_patch(r.json.get("files") or []) else 0
        return n

    async def _known_shas(self, shas: list[str]) -> set[str]:
        out: set[str] = set()
        for i in range(0, len(shas), 500):
            part = shas[i:i + 500]
            ph = ",".join("?" * len(part))
            out |= {r["sha"] for r in await self.db.fetchall(f"SELECT sha FROM github_commits WHERE sha IN ({ph})",
                                                              part, writer=True)}
        return out

    # ── releases / tags / PRs ────────────────────────────────────────────────────────────────────────────────
    async def fetch_releases_tags(self, rid: int, batch_dates: dict[str, int] | None = None) -> list[tuple]:
        """→ github_events rows (repo_id, kind, event_at, dedupe_key, payload). A tag's date comes from its commit:
        this pass's batch, then ``github_commits``, then ≤ 5 ``/commits/{sha}`` look-ups per run."""
        batch_dates = batch_dates or {}
        out: list[tuple] = []
        r = await self._get(f"/repositories/{rid}/releases", params={"per_page": 50})
        rel_tags: set[str] = set()
        if r.status == 200 and isinstance(r.json, list):
            for x in r.json:
                tag = x.get("tag_name")
                if not tag:
                    continue
                rel_tags.add(tag)
                if x.get("draft"):
                    continue
                out.append((rid, "release", iso_ts(x.get("published_at") or x.get("created_at")),
                            f"release:{rid}:{tag}", {"tag_name": tag, "prerelease": bool(x.get("prerelease")),
                                                     "draft": False}))
        t = await self._get(f"/repositories/{rid}/tags", params={"per_page": 50})
        if t.status == 200 and isinstance(t.json, list):
            keys = [f"tag:{rid}:{x.get('name')}" for x in t.json if x.get("name")]
            have: set[str] = set()
            if keys:
                ph = ",".join("?" * len(keys))
                have = {row["dedupe_key"] for row in await self.db.fetchall(
                    f"SELECT dedupe_key FROM github_events WHERE dedupe_key IN ({ph})", keys, writer=True)}
            lookups = 0
            for x in t.json:
                name = x.get("name")
                sha = (x.get("commit") or {}).get("sha")
                if not name or not sha or f"tag:{rid}:{name}" in have or name in rel_tags:
                    continue
                when = batch_dates.get(sha)
                if when is None:
                    row = await self.db.fetchone("SELECT committer_date, event_at FROM github_commits WHERE sha = ?",
                                                 (sha,), writer=True)
                    when = (row["committer_date"] or row["event_at"]) if row else None
                if when is None:
                    if lookups >= 5:                     # the rest wait for the next run
                        continue
                    lookups += 1
                    try:
                        c = await self._get(f"/repositories/{rid}/commits/{sha}")
                    except GitHubUnavailable:
                        continue
                    if c.ok and isinstance(c.json, dict):
                        cm = (c.json.get("commit") or {})
                        when = iso_ts((cm.get("committer") or {}).get("date")) or iso_ts((cm.get("author") or {}).get("date"))
                if when is not None:
                    out.append((rid, "tag", when, f"tag:{rid}:{name}", {"tag_name": name, "sha": sha}))
        return out

    async def fetch_merged_prs_with_reviews(self, rid: int) -> list[tuple]:
        s = app_config.settings
        since = self.now - int(s.GITHUB_PR_LOOKBACK_DAYS) * DAY
        r = await self._get(f"/repositories/{rid}/pulls",
                            params={"state": "closed", "sort": "updated", "direction": "desc", "per_page": 50})
        if r.status != 200 or not isinstance(r.json, list):
            return []
        merged = [p for p in r.json if p.get("merged_at") and (iso_ts(p["merged_at"]) or 0) >= since]
        merged.sort(key=lambda p: p["merged_at"], reverse=True)
        merged = merged[: int(s.GITHUB_PR_MAX_PER_REPO)]
        keys = [f"pr_merged:{rid}:{p['number']}" for p in merged]
        have: set[str] = set()
        if keys:
            ph = ",".join("?" * len(keys))
            have = {x["dedupe_key"] for x in await self.db.fetchall(
                f"SELECT dedupe_key FROM github_events WHERE dedupe_key IN ({ph})", keys, writer=True)}
        out: list[tuple] = []
        for p in merged:
            key = f"pr_merged:{rid}:{p['number']}"
            if key in have:
                continue                                  # stored PRs are not re-fetched (17's key too)
            author = (p.get("user") or {}).get("login")
            reviewers: list[str] = []
            try:
                rv = await self._get(f"/repositories/{rid}/pulls/{p['number']}/reviews", params={"per_page": 100})
            except GitHubUnavailable:
                continue
            if rv.status == 200 and isinstance(rv.json, list):
                for x in rv.json:
                    u = x.get("user") or {}
                    lg = u.get("login")
                    if (x.get("state") in ("APPROVED", "CHANGES_REQUESTED", "COMMENTED") and lg and lg != author
                            and not authors.is_bot(lg, author_type=u.get("type"))):
                        if lg not in reviewers:
                            reviewers.append(lg)
            out.append((rid, "pr_merged", iso_ts(p["merged_at"]), key,
                        {"number": p["number"], "author_login": author, "review_count": len(reviewers),
                         "reviewers": reviewers,
                         "author_is_bot": authors.is_bot(author, author_type=(p.get("user") or {}).get("type"))}))
        return out

    # ── stats / hygiene / originality ────────────────────────────────────────────────────────────────────────
    async def _stats_json(self, path: str) -> tuple[bool, Any]:
        s = app_config.settings
        for i in range(int(s.GITHUB_STATS_202_RETRIES) + 1):
            r = await self._get(path)
            if r.status == 200:
                return True, r.json
            if r.status == 204:
                return True, []
            if r.status != 202 or i == int(s.GITHUB_STATS_202_RETRIES):
                return False, None
            await self.client.sleep(float(s.GITHUB_STATS_202_WAIT_SEC))
        return False, None

    async def fetch_stats(self, rid: int) -> dict[str, Any]:
        upd: dict[str, Any] = {}
        ok, part = await self._stats_json(f"/repositories/{rid}/stats/participation")
        if ok:
            upd["participation_json"] = json.dumps(part if isinstance(part, dict) else {})
        ok2, cont = await self._stats_json(f"/repositories/{rid}/stats/contributors")
        if ok2 and isinstance(cont, list):
            top = sorted(({"login": (c.get("author") or {}).get("login"), "total": c.get("total")} for c in cont),
                         key=lambda x: -(x["total"] or 0))[:100]
            upd["contributors_json"] = json.dumps(top)
        if ok or ok2:
            upd["stats_checked_at"] = self.now
        return upd

    async def _exists(self, path: str) -> bool | None:
        r = await self._get(path)
        if r.status == 200:
            return True
        if r.status == 404:
            return False
        return None

    async def check_hygiene(self, rid: int) -> dict[str, Any]:
        hj: dict[str, Any] = {}
        prof = await self._get(f"/repositories/{rid}/community/profile", etag_key="")
        if prof.ok and isinstance(prof.json, dict):
            files = prof.json.get("files") or {}
            for k in ("license", "readme", "contributing", "code_of_conduct"):
                hj[k] = files.get(k) is not None
        rd = await self._get(f"/repositories/{rid}/readme")
        hj["readme_bytes"] = int((rd.json or {}).get("size") or 0) if rd.status == 200 else (0 if rd.status == 404 else None)
        wf = await self._get(f"/repositories/{rid}/contents/.github/workflows")
        if wf.status == 200 and isinstance(wf.json, list):
            hj["ci"] = any(str(x.get("name", "")).endswith((".yml", ".yaml")) for x in wf.json)
        elif wf.status == 404:
            hj["ci"] = False
        seen: list[bool | None] = []
        for p in (".github/dependabot.yml", "renovate.json", ".github/renovate.json"):
            seen.append(await self._exists(f"/repositories/{rid}/contents/{p}"))
            if seen[-1]:
                break                                     # stop at the first 200
        hj["dep_update"] = True if any(seen) else (False if all(x is False for x in seen) else None)
        if hj.get("contributing") is True:
            hj["security"] = None                         # not needed: security_or_contributing is already true
        else:
            sec = await self._exists(f"/repositories/{rid}/contents/SECURITY.md")
            if not sec:
                sec2 = await self._exists(f"/repositories/{rid}/contents/.github/SECURITY.md")
                sec = sec2 if sec2 is not None else sec
            hj["security"] = sec
        return hj

    async def _tree(self, path: str) -> tuple[dict[str, str], bool, str | None] | None:
        r = await self._get(path, params={"recursive": 1})
        if r.status != 200 or not isinstance(r.json, dict):
            return None
        files = {x["path"]: x["sha"] for x in r.json.get("tree") or [] if x.get("type") == "blob"}
        return files, bool(r.json.get("truncated")), r.json.get("sha")

    async def check_originality(self, repo: dict) -> dict[str, Any] | None:
        s = app_config.settings
        tpl = s.GITHUB_TEMPLATE_REPO.strip().lower()
        rid = int(repo["repo_id"])
        if self._template_tree is None:
            meta = await self._get(f"/repos/{tpl}", etag_key="")
            if meta.status in (301, 302, 307, 308) and parse_redirect_id(meta.location):
                # the template moved (opentensor/… → RaoFoundation/… id 676155963, seen 2026-10-17): follow by id
                meta = await self._get(f"/repositories/{parse_redirect_id(meta.location)}", etag_key="")
            if not meta.ok or not isinstance(meta.json, dict):
                return None
            self._template_id = int(meta.json["id"])
            self._template_full = str(meta.json.get("full_name") or tpl).lower()
            self._template_branch = meta.json.get("default_branch")
            t = await self._tree(f"/repositories/{self._template_id}/git/trees/{self._template_branch or 'HEAD'}")
            if t is None:
                return None
            self._template_tree, _, self._template_sha = t
        branch = repo.get("default_branch") or "HEAD"
        mine = await self._tree(f"/repositories/{rid}/git/trees/{branch}")
        if mine is None:
            return None
        files, truncated, _ = mine
        ident = sum(1 for p, sha in files.items() if self._template_tree.get(p) == sha)
        oj: dict[str, Any] = {"repo_files": len(files), "identical_files": ident,
                              "identical_share": round(ident / max(len(files), 1), 6), "truncated": truncated,
                              "template_sha": self._template_sha, "template_full_name": self._template_full}
        if repo.get("is_fork") and (repo.get("parent_full_name") or "") in (tpl, self._template_full):
            towner = (self._template_full or tpl).split("/")[0]
            base = self._template_branch or "main"
            cmp = await self._get(f"/repositories/{self._template_id}/compare/{towner}:{base}...{repo['owner']}:{branch}")
            if cmp.status == 200 and isinstance(cmp.json, dict):
                oj["ahead_by"] = cmp.json.get("ahead_by")
        return oj

    # ── one repo ─────────────────────────────────────────────────────────────────────────────────────────────
    async def collect_repo(self, repo: dict, *, deep: bool) -> RepoActivityResult:
        """``deep`` = declared/primary repo (hygiene + originality are due-dated there only)."""
        s = app_config.settings
        rid = int(repo["repo_id"])
        res = RepoActivityResult(rid)
        upd: dict[str, Any] = {}
        try:
            commits, status, idents = await self.fetch_commits(repo)
        except GitHubUnavailable as exc:
            res.error = str(exc)[:200]
            res.calls_failed += 1
            return res
        if status == 404:
            await self.db.execute("UPDATE github_declared_refs SET last_attempt_at = NULL WHERE repo_id = ?", (rid,))
            res.error = "404 on commits (queued for re-verify)"
            return res
        if status not in (200, 409):
            res.error = f"HTTP {status} on commits"
            res.calls_failed += 1
            return res
        known = await self._known_shas([c.sha for c in commits])
        new = [c for c in commits if c.sha not in known]
        flags = spam.flag_commits(new, repo_created_at=repo.get("created_at"), declared_fork=bool(repo.get("is_fork")))
        await self._sample_stats(rid, new, flags)
        flags = spam.flag_commits(new, repo_created_at=repo.get("created_at"), declared_fork=bool(repo.get("is_fork")))
        for c in new:
            c.spam_flags = flags.get(c.sha, [])
        events: list[tuple] = []
        dates = {c.sha: c.committer_date or c.event_at for c in commits}
        for step in (lambda: self.fetch_releases_tags(rid, dates), lambda: self.fetch_merged_prs_with_reviews(rid)):
            try:
                events += await step()
            except GitHubUnavailable as exc:
                res.calls_failed += 1
                logger.debug("github.activity.step_failed", data={"repo_id": rid, "error": str(exc)[:200]})
        due = lambda col, days: (repo.get(col) or 0) < self.now - int(days) * DAY  # noqa: E731
        try:
            if due("stats_checked_at", s.GITHUB_STATS_INTERVAL_DAYS):
                upd.update(await self.fetch_stats(rid))
            if deep and due("hygiene_checked_at", s.GITHUB_HYGIENE_INTERVAL_DAYS):
                hj = await self.check_hygiene(rid)
                upd.update(hygiene_json=json.dumps(hj), hygiene_checked_at=self.now)
            if deep and due("originality_checked_at", s.GITHUB_ORIGINALITY_INTERVAL_DAYS):
                oj = await self.check_originality(repo)
                if oj is not None:
                    upd.update(originality_json=json.dumps(oj), originality_checked_at=self.now)
        except GitHubUnavailable as exc:
            res.calls_failed += 1
            logger.debug("github.activity.extra_failed", data={"repo_id": rid, "error": str(exc)[:200]})
        max_cd = max((c.committer_date or c.event_at for c in commits), default=None)
        wm = max(int(repo.get("last_commit_seen_at") or 0), int(max_cd or 0)) or None
        last_pass = repo.get("activity_ok_at")
        fp = spam.detect_force_push(pushed_at=repo.get("pushed_at"), max_committer_date=max_cd or wm, n_new=len(new),
                                    pushed_advanced=last_pass is not None and (repo.get("pushed_at") or 0) > last_pass)
        upd.update(last_commit_seen_at=wm, activity_ok_at=self.now, force_push_suspected=1 if fp else 0)
        async with self.db.transaction():
            if new:
                await self.db.insert_or_ignore("github_commits", [c.db_row(observed_at=self.now, run_id=self.run_id)
                                                                  for c in new])
            await authors.record_identities(self.db, idents, self.now)
            for e in events:
                res.events_new += await ghevents.raw_event(self.db, repo_id=e[0], kind=e[1], event_at=e[2],
                                                           dedupe_key=e[3], payload=e[4], now=self.now,
                                                           run_id=self.run_id)
            days = {floor_day(c.event_at) for c in new}
            await rebuild_activity_daily(self.db, rid, days, now=self.now, run_id=self.run_id, repo=repo)
            sets = ", ".join(f"{k} = ?" for k in upd)
            await self.db.execute(f"UPDATE github_repos SET {sets} WHERE repo_id = ?", [*upd.values(), rid])
        res.ok = True
        res.commits_new = len(new)
        res.days = days
        return res


async def load_window(db: "Database", repo_ids: list[int], start: int, end: int, *,
                      replay: bool = False, observed_by: int | None = None) -> list[CommitRow]:
    if not repo_ids:
        return []
    ph = ",".join("?" * len(repo_ids))
    sql = f"SELECT * FROM github_commits WHERE repo_id IN ({ph}) AND event_at >= ? AND event_at < ?"
    params: list[Any] = [*repo_ids, int(start), int(end)]
    if observed_by is not None:
        sql += " AND (observed_at <= ? OR (? = 1 AND backfilled = 1))"
        params += [int(observed_by), 1 if replay else 0]
    return [CommitRow.from_db(r) for r in await db.fetchall(sql, params, writer=True)]


async def rebuild_activity_daily(db: "Database", repo_id: int, day_tss: set[int], *, now: int, run_id: str,
                                 repo: dict | None = None, backfilled: int = 0) -> int:
    """REPLACE ``github_activity_daily`` for the touched days (read-time spam flags over a ±7-d context)."""
    if not day_tss:
        return 0
    lo, hi = min(day_tss) - 7 * DAY, max(day_tss) + 8 * DAY
    commits = await load_window(db, [repo_id], lo, hi)
    keys = await authors.resolve_author_keys(db, [c.author_key for c in commits])
    for c in commits:
        c.author_key = keys.get(c.author_key, c.author_key)
    flags = spam.flag_commits(commits, repo_created_at=(repo or {}).get("created_at"),
                              declared_fork=bool((repo or {}).get("is_fork")))
    rows = []
    for d in sorted(day_tss):
        day = [c for c in commits if d <= c.event_at < d + DAY]
        human = [c for c in day if not c.is_bot and not c.is_merge]
        kept = [c for c in human if not flags.get(c.sha)]
        rows.append({"repo_id": repo_id, "day_ts": d, "commits_all": len(day), "commits_human": len(kept),
                     "commits_bot": sum(1 for c in day if c.is_bot), "commits_merge": sum(1 for c in day if c.is_merge and not c.is_bot),
                     "commits_spam": len(human) - len(kept), "authors_human": len({c.author_key for c in kept}),
                     "observed_at": now, "run_id": run_id, "backfilled": backfilled})
    return await db.upsert("github_activity_daily", rows, conflict=("repo_id", "day_ts"))


async def run_activity(db: "Database", client: "GitHubClient", *, run_id: str, now: int, deadline: float | None,
                       netuids: set[int] | None = None, clock: Any = None) -> dict[str, Any]:
    """One pass over every in-scope repo (leftovers of a partial run first: ``activity_ok_at`` ASC)."""
    s = app_config.settings
    q = ("SELECT DISTINCT g.*, MAX(CASE WHEN o.role IN ('declared','primary') THEN 1 ELSE 0 END) AS deep "
         "FROM github_occupant_repos o JOIN github_repos g USING (repo_id) JOIN occupants oc USING (occupant_key) "
         "WHERE o.in_scope = 1 AND g.status = 'ok' AND g.disabled = 0")
    params: list[Any] = []
    if netuids:
        q += f" AND oc.netuid IN ({','.join('?' * len(netuids))})"
        params += sorted(netuids)
    q += " GROUP BY g.repo_id ORDER BY g.activity_ok_at IS NOT NULL, g.activity_ok_at, g.repo_id"
    repos = await db.fetchall(q, params, writer=True)
    col = ActivityCollector(db, client, run_id=run_id, now=now, deadline=deadline)
    out = {"repos": len(repos), "ok": 0, "failed": 0, "left": 0, "commits_new": 0, "events_new": 0, "deferred": False}
    now_fn = clock or client.clock
    sem = asyncio.Semaphore(max(1, int(s.GITHUB_MAX_CONCURRENCY)))

    async def one(repo: dict) -> None:
        async with sem:
            if out["deferred"] or (deadline is not None and now_fn() >= deadline):
                out["left"] += 1
                return
            try:
                r = await col.collect_repo(repo, deep=bool(repo.get("deep")))
            except QuotaDeferred:
                out["deferred"] = True
                out["left"] += 1
                return
            if r.ok:
                out["ok"] += 1
                out["commits_new"] += r.commits_new
                out["events_new"] += r.events_new
            else:
                out["failed"] += 1
    await asyncio.gather(*(one(dict(r)) for r in repos))
    return out
