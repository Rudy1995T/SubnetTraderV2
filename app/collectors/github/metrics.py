"""The ten P2 metrics of ts-1.0 (spec 06 §2.7–§2.8, §4) + 19's ``repo_pushed_days`` hand-off.

Absence (verifiable: no ref / placeholder / gone beyond grace with no fallback) → ``status='absent'`` (raw 0.0 for
counts, NULL for day/ratio keys). Failure (unobservable) → ``status='failed'``, value NULL. Genuine inactivity is
``ok`` with 0.0. Never a fabricated number to obtain a 0.
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Iterable

import app.config as app_config
from app.collectors.github import authors, ghevents, spam
from app.collectors.github.activity import load_window
from app.collectors.github.models import DAY, SPEC_VERSION, CommitRow, MetricObservation, RepoRow
from app.collectors.github.orgs import is_subnet_related, pick_primary, scope_repo_ids
from app.collectors.github.resolver import source_refs
from app.registry import active_occupants, resolved
from app.storage.pit import put_metric_values
from app.util.time import floor_day, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.registry.models import Occupant
    from app.storage.db import Database

P2_KEYS = ("active_authors_28d", "fte_devs_84d", "commit_days_90d", "declared_repo_freshness", "org_freshness",
           "release_cadence_90d", "review_practice_90d", "bus_factor", "repo_hygiene", "originality")
COUNT_KEYS = frozenset({"active_authors_28d", "fte_devs_84d", "commit_days_90d", "release_cadence_90d", "bus_factor"})
ACTIVITY_KEYS = ("active_authors_28d", "fte_devs_84d", "commit_days_90d", "release_cadence_90d",
                 "review_practice_90d", "bus_factor")
FRESH_S = 2 * DAY                    # GitHub freshness window (02 §C6)


def slot_for(now: int) -> int:
    """First scoring slot ≥ now (00/06/12/18:00Z, 02 §B4)."""
    return -(-int(now) // 21600) * 21600


def absent(key: str, reason: str, **detail: Any) -> MetricObservation:
    return MetricObservation(key, 0.0 if key in COUNT_KEYS else None, "absent", reason, detail)


def failed(key: str, reason: str, **detail: Any) -> MetricObservation:
    return MetricObservation(key, None, "failed", reason, detail)


def ok(key: str, value: float, **detail: Any) -> MetricObservation:
    reason = detail.pop("reason", None)
    return MetricObservation(key, float(value), "ok", reason, detail)


# ── pure metric functions ────────────────────────────────────────────────────────────────────────────────────
@dataclass(slots=True)
class AuthorClass:
    author_key: str
    w1: int
    w2: int
    w3: int
    cd84: int
    today: int
    dev_class: str
    repos: list[int] = field(default_factory=list)


def _days(commits: Iterable[CommitRow], lo: int, hi: int) -> set[int]:
    return {c.event_at // DAY for c in commits if lo <= c.event_at < hi}


def classify_authors(commits: list[CommitRow], day_ts: int, *, now: int | None = None,
                     fte_days: int | None = None) -> dict[str, AuthorClass]:
    """Electric-Capital classes over human commits ``C(o)`` (§2.7); D = ``day_ts`` (UTC midnight)."""
    fte = app_config.settings.GITHUB_FTE_COMMIT_DAYS_28D if fte_days is None else fte_days
    d = int(day_ts)
    by: dict[str, list[CommitRow]] = defaultdict(list)
    for c in commits:
        by[c.author_key].append(c)
    out: dict[str, AuthorClass] = {}
    for a, cs in by.items():
        w1 = len(_days(cs, d - 28 * DAY, d))
        w2 = len(_days(cs, d - 56 * DAY, d - 28 * DAY))
        w3 = len(_days(cs, d - 84 * DAY, d - 56 * DAY))
        cd84 = len(_days(cs, d - 84 * DAY, d))
        if cd84 < 1:
            continue
        today = sum(1 for c in cs if d <= c.event_at < (now if now is not None else d + DAY) + 1)
        cls = "full_time" if sum(1 for w in (w1, w2, w3) if w >= fte) >= 2 else ("part_time" if cd84 >= 3 else "one_time")
        out[a] = AuthorClass(a, w1, w2, w3, cd84, today, cls,
                             sorted({c.repo_id for c in cs if d - 84 * DAY <= c.event_at < d}))
    return out


def active_authors_28d(classes: dict[str, AuthorClass]) -> int:
    return sum(1 for a in classes.values() if a.w1 >= 1)


def fte_devs_84d(classes: dict[str, AuthorClass]) -> int:
    return sum(1 for a in classes.values() if a.dev_class == "full_time")


def commit_days_90d(commits: list[CommitRow], day_ts: int) -> int:
    return len(_days(commits, int(day_ts) - 90 * DAY, int(day_ts)))


def bus_factor(commits: list[CommitRow], day_ts: int) -> int:
    """Smallest k whose top-k authors reach ≥ 50 % of human commits in 90 d (0 when no commits)."""
    cs = [c for c in commits if int(day_ts) - 90 * DAY <= c.event_at < int(day_ts)]
    if not cs:
        return 0
    counts = sorted(Counter(c.author_key for c in cs).values(), reverse=True)
    total = sum(counts)
    acc = 0
    for k, n in enumerate(counts, 1):
        acc += n
        if acc * 2 >= total:
            return k
    return len(counts)


def release_cadence_90d(events: list[dict], now: int) -> int:
    seen = {(e["repo_id"], (e.get("payload") or {}).get("tag_name")) for e in events
            if e["event_kind"] in ("release", "tag") and e.get("event_at") is not None
            and e["event_at"] >= now - 90 * DAY}
    return len(seen)


def review_practice_90d(events: list[dict], now: int) -> tuple[float | None, int, int]:
    prs = [e for e in events if e["event_kind"] == "pr_merged" and e.get("event_at") is not None
           and e["event_at"] >= now - 90 * DAY]
    human = [e for e in prs if not (e.get("payload") or {}).get("author_is_bot")
             and not authors.is_bot((e.get("payload") or {}).get("author_login"))]
    merged = len(human)
    reviewed = sum(1 for e in human if int((e.get("payload") or {}).get("review_count") or 0) > 0)
    return (reviewed / merged if merged else None), merged, reviewed


def hygiene_items(hj: dict) -> dict[str, bool | None]:
    rb = hj.get("readme_bytes")
    c, sec = hj.get("contributing"), hj.get("security")
    soc = True if (c is True or sec is True) else (False if (c is False and sec is False) else None)
    return {"license": hj.get("license"), "readme_500": None if rb is None else rb >= 500, "ci": hj.get("ci"),
            "security_or_contributing": soc, "dep_update": hj.get("dep_update")}


def repo_hygiene(hj: dict | None, checked_at: int | None, now: int) -> MetricObservation:
    if hj is None or checked_at is None:
        return failed("repo_hygiene", "never_observed")
    items = hygiene_items(hj)
    if checked_at < now - 14 * DAY:
        return failed("repo_hygiene", "activity_stale", items=items, checked_at=to_iso(checked_at))
    seen = [v for v in items.values() if v is not None]
    if len(seen) < 3:
        return failed("repo_hygiene", "api_outage", items=items, observed=len(seen))
    return ok("repo_hygiene", sum(1 for v in seen if v) / len(seen), items=items, observed=len(seen))


def originality(oj: dict | None, checked_at: int | None, repo: RepoRow | None, now: int,
                template: str | None = None) -> MetricObservation:
    tpls = {(template or app_config.settings.GITHUB_TEMPLATE_REPO).lower(), str((oj or {}).get("template_full_name") or "")}
    if repo is not None and repo.is_fork and (repo.parent_full_name or "") not in tpls:
        return ok("originality", 0.0, declared_fork=True, parent=repo.parent_full_name)
    if oj is None or checked_at is None:
        return failed("originality", "never_observed")
    if checked_at < now - 45 * DAY:
        return failed("originality", "activity_stale", checked_at=to_iso(checked_at))
    if repo is not None and repo.is_fork and oj.get("ahead_by") == 0:
        return ok("originality", 0.0, template_fork=True, ahead_by=0)
    share = float(oj.get("identical_files") or 0) / max(int(oj.get("repo_files") or 0), 1)
    return ok("originality", round(1.0 - share, 6), identical_share=round(share, 6),
              repo_files=oj.get("repo_files"), truncated=bool(oj.get("truncated")))


# ── per-occupant computation ─────────────────────────────────────────────────────────────────────────────────
@dataclass
class OccupantResult:
    occupant_key: str
    netuid: int
    observations: list[MetricObservation]
    authors: list[AuthorClass] = field(default_factory=list)
    spam_event: dict | None = None
    pushed_row: tuple[float | None, dict] | None = None


class P2Metrics:
    def __init__(self, db: "Database", *, token_state: str | None = None) -> None:
        self.db = db
        s = app_config.settings
        self.token_state = token_state or ("ok" if s.GITHUB_TOKEN else "missing")

    async def _repo(self, rid: int | None, now: int, replay: bool) -> RepoRow | None:
        if not rid:
            return None
        r = await self.db.fetchone("SELECT * FROM github_repos WHERE repo_id = ?", (rid,))
        if r is None:
            return None
        row = RepoRow.from_db(r)
        if replay:
            snap = await self.db.fetchone("SELECT pushed_at, archived, status FROM github_repo_snapshots WHERE "
                                          "repo_id = ? AND observed_at <= ? ORDER BY observed_at DESC LIMIT 1",
                                          (rid, now))
            if snap is None:
                return None
            row.pushed_at, row.archived, row.status = snap["pushed_at"], bool(snap["archived"]), snap["status"] or "ok"
        return row

    async def compute(self, occ: "Occupant", now: int, *, replay: bool = False,
                      coverage: Callable[[int, int], bool] | None = None) -> OccupantResult:
        db = self.db
        okey = occ.occupant_key
        D = floor_day(now)
        res = await resolved(db, okey, "github_repo")
        out = OccupantResult(okey, occ.netuid, [])

        def all_(fn: Callable[[str], MetricObservation]) -> OccupantResult:
            out.observations = [fn(k) for k in P2_KEYS]
            return out
        if res is None or res.status == "unknown":
            out.pushed_row = (None, {"status": "failed", "reason": "never_observed"})
            return all_(lambda k: failed(k, "never_observed"))
        if res.status in ("absent", "junk_only"):
            reason = "no_github_url" if res.status == "absent" else "placeholder"
            out.pushed_row = (None, {"status": "absent", "reason": "no_github_link"})
            return all_(lambda k: absent(k, reason))
        srefs, declared_src = source_refs(res)
        if declared_src is None:
            reason = "placeholder" if any(sr.placeholder for sr in srefs) else "no_github_url"
            out.pushed_row = (None, {"status": "absent", "reason": "no_github_link"})
            return all_(lambda k: absent(k, reason))
        decl = await db.fetchone("SELECT * FROM github_declared_refs WHERE occupant_key = ? AND source = ?",
                                 (okey, declared_src))
        if decl is None or decl["state"] == "new":
            out.pushed_row = (None, {"status": "failed", "reason": "never_observed"})
            return all_(lambda k: failed(k, "never_observed"))
        roles = {int(r["repo_id"]): r for r in await db.fetchall(
            "SELECT * FROM github_occupant_repos WHERE occupant_key = ?", (okey,))}
        primary_id = next((rid for rid, r in roles.items() if r["role"] == "primary"), None)
        primary = await self._repo(primary_id, now, replay)
        decl_repo = await self._repo(decl["repo_id"] if decl["kind"] == "repo" else None, now, replay)
        alive = decl_repo is not None and decl["state"] == "resolved" and decl_repo.status == "ok"
        grace_end = (decl["first_404_at"] or now) + int(app_config.settings.GITHUB_GONE_GRACE_DAYS) * DAY
        dead_state = decl["state"] in ("gone", "gone_pending", "blocked", "org_empty") or (
            decl["kind"] == "owner" and decl["state"] != "resolved")
        beyond = dead_state and decl["first_404_at"] is not None and now >= grace_end
        dead = beyond and primary is None
        target = decl_repo if alive else primary
        bad = self.token_state == "bad"
        obs: dict[str, MetricObservation] = {}

        # declared_repo_freshness (resolve data: available in degraded mode too)
        key = "declared_repo_freshness"
        if alive:
            lv = await db.fetchval("SELECT last_verified_at FROM github_repos WHERE repo_id = ?", (decl_repo.repo_id,))
            if not replay and (lv or 0) < now - FRESH_S:
                obs[key] = failed(key, "bad_token" if bad else "api_outage", last_verified_at=to_iso(lv))
            elif decl_repo.pushed_at is None:
                obs[key] = failed(key, "never_observed")
            else:
                obs[key] = ok(key, max(0.0, (now - decl_repo.pushed_at) / DAY), repo_id=decl_repo.repo_id,
                              archived=decl_repo.archived)
        elif not beyond and decl_repo is not None and decl_repo.pushed_at is not None:
            obs[key] = ok(key, max(0.0, (now - decl_repo.pushed_at) / DAY), repo_id=decl_repo.repo_id,
                          reason="gone_grace", first_404_at=to_iso(decl["first_404_at"]))
        elif primary is not None and primary.pushed_at is not None and (beyond or decl["kind"] == "owner"):
            obs[key] = ok(key, max(0.0, (now - primary.pushed_at) / DAY), repo_id=primary.repo_id,
                          fallback="org_primary")
        elif dead:
            obs[key] = absent(key, "repo_gone", declared=decl["ref_key"], first_404_at=to_iso(decl["first_404_at"]))
        elif dead_state:
            obs[key] = failed(key, "gone_grace", declared=decl["ref_key"])
        else:
            obs[key] = failed(key, "no_token" if self.token_state == "missing" else "never_observed")

        # org_freshness
        obs["org_freshness"] = await self._org_freshness(okey, occ, decl, decl_repo, alive, beyond, dead_state, now,
                                                         replay)
        if replay and not dead:
            # 13 §2.5: without a PIT repo snapshot, history approximates freshness by the last commit ≤ t; org
            # freshness reads today's pushed_at (look-ahead), so it always does (17 §2.11 replay, 20 N2 item 2)
            if obs["declared_repo_freshness"].status != "ok":
                decl_ids = [int(decl["repo_id"])] if decl["kind"] == "repo" and decl["repo_id"] else []
                obs["declared_repo_freshness"] = await self._last_commit_freshness("declared_repo_freshness",
                                                                                   decl_ids, now)
            obs["org_freshness"] = await self._last_commit_freshness(
                "org_freshness", await scope_repo_ids(db, okey, include_declared_gone=True), now)

        # activity metrics over R(o) (+ the declared-gone repo during grace)
        rids = await scope_repo_ids(db, okey, include_declared_gone=not beyond)
        act: dict[str, MetricObservation] | None = None
        if dead:
            act = {k: absent(k, "repo_gone") for k in ACTIVITY_KEYS}
        elif self.token_state == "missing":
            act = {k: failed(k, "no_token") for k in ACTIVITY_KEYS}
        elif not rids:
            act = {k: failed(k, "gone_grace" if dead_state else "never_observed") for k in ACTIVITY_KEYS}
        elif replay and coverage is not None and not all(coverage(r, D - 90 * DAY) for r in rids):
            act = {k: failed(k, "backfill_incomplete") for k in ACTIVITY_KEYS}
        elif not replay:
            ph = ",".join("?" * len(rids))
            fresh = await db.fetchval(f"SELECT MAX(activity_ok_at) FROM github_repos WHERE repo_id IN ({ph})", rids)
            if fresh is None or fresh < now - int(app_config.settings.GITHUB_ACTIVITY_STALE_DAYS) * DAY:
                act = {k: failed(k, "bad_token" if bad else "activity_stale", last_ok=to_iso(fresh)) for k in ACTIVITY_KEYS}
        if act is None:
            act, out.authors, out.spam_event = await self._activity(occ, rids, now, D, replay)
        obs.update(act)

        # hygiene + originality on the declared (alive) else primary repo
        for key in ("repo_hygiene", "originality"):
            if dead:
                obs[key] = absent(key, "repo_gone")
            elif target is None:
                obs[key] = failed(key, "no_token" if self.token_state == "missing" else
                                  ("gone_grace" if dead_state else "never_observed"))
        if "repo_hygiene" not in obs or "originality" not in obs:
            row = await db.fetchone("SELECT hygiene_json, hygiene_checked_at, originality_json, originality_checked_at "
                                    "FROM github_repos WHERE repo_id = ?", (target.repo_id,))
            hj = json.loads(row["hygiene_json"]) if row and row["hygiene_json"] else None
            oj = json.loads(row["originality_json"]) if row and row["originality_json"] else None
            hc, oc = (row["hygiene_checked_at"], row["originality_checked_at"]) if row else (None, None)
            if replay and hc is not None and hc > now:
                hj, hc = None, None
            if replay and oc is not None and oc > now:
                oj, oc = None, None
            if "repo_hygiene" not in obs:
                obs["repo_hygiene"] = (failed("repo_hygiene", "no_pit_snapshot") if replay and hj is None
                                       else repo_hygiene(hj, hc, now))
                obs["repo_hygiene"].detail["repo_id"] = target.repo_id
            if "originality" not in obs:
                obs["originality"] = (failed("originality", "no_pit_snapshot") if replay and oj is None and not
                                      target.is_fork else originality(oj, oc, target, now))
                obs["originality"].detail["repo_id"] = target.repo_id
        out.observations = [obs[k] for k in P2_KEYS]
        out.pushed_row = self._pushed(decl, decl_repo, primary, alive, dead_state, now)
        return out

    def _pushed(self, decl: dict, decl_repo: RepoRow | None, primary: RepoRow | None, alive: bool,
                dead_state: bool, now: int) -> tuple[float | None, dict]:
        """19's ``repo_pushed_days`` semantics (wb-1) from 06's resolver data."""
        r = decl_repo if alive else (primary if decl["kind"] == "owner" or dead_state else None)
        if r is None or r.pushed_at is None:
            if dead_state:
                return None, {"status": "failed", "reason": "http_404", "repo": decl["ref_key"]}
            return None, {"status": "failed", "reason": "never_observed", "repo": decl["ref_key"]}
        return max(0.0, (now - r.pushed_at) / DAY), {
            "status": "ok", "repo": r.full_name, "pushed_at": to_iso(r.pushed_at), "archived": r.archived,
            "stars": r.stars, "kind": "repo" if r is decl_repo else "org_latest"}

    async def _last_commit_freshness(self, key: str, rids: list[int], now: int) -> MetricObservation:
        if not rids:
            return failed(key, "never_observed")
        last = await self.db.fetchval(f"SELECT MAX(event_at) FROM github_commits WHERE repo_id IN "
                                      f"({','.join('?' * len(rids))}) AND event_at <= ?", [*rids, now])
        if last is None:
            return failed(key, "no_pit_commit", repos=len(rids))
        return ok(key, max(0.0, (now - int(last)) / DAY), approx="last_commit", repos=len(rids))

    async def _org_freshness(self, okey: str, occ: "Occupant", decl: dict, decl_repo: RepoRow | None, alive: bool,
                             beyond: bool, dead_state: bool, now: int, replay: bool) -> MetricObservation:
        key = "org_freshness"
        db = self.db
        owner = decl_repo.owner if decl_repo is not None else (decl["owner"] or "").lower()
        org = await db.fetchone("SELECT * FROM github_orgs WHERE owner = ?", (owner,)) if owner else None
        if org is None:
            if alive and decl_repo.pushed_at is not None and self.token_state == "missing":
                return failed(key, "no_token")
            return failed(key, "no_token" if self.token_state == "missing" else "never_observed")
        if not replay and (org["last_ok_at"] or 0) < now - FRESH_S:
            return failed(key, "bad_token" if self.token_state == "bad" else "api_outage",
                          last_ok_at=to_iso(org["last_ok_at"]))
        repos = [RepoRow.from_db(r) for r in await db.fetchall(
            "SELECT * FROM github_repos WHERE owner = ? AND status = 'ok'", (owner,))]
        if org["kind"] == "user":
            repos = [r for r in repos if (decl_repo and r.repo_id == decl_repo.repo_id)
                     or is_subnet_related(r.name, r.description, occ.name)]
        non_fork = [r for r in repos if not r.is_fork]
        live = [r for r in non_fork if not r.archived]
        arch = [r for r in non_fork if r.archived]
        declared_archived = bool(decl_repo and decl_repo.archived)
        succ = pick_primary(live, occ.netuid, occ.name, now) if declared_archived else None
        detail = {"n_non_fork": len(non_fork), "n_archived": len(arch), "all_archived": bool(non_fork) and not live,
                  "declared_archived": declared_archived, "successor_repo_id": succ.repo_id if succ else None,
                  "owner": owner}
        pool = live or arch
        if alive and decl_repo is not None and decl_repo not in pool and (decl_repo.is_fork or not pool):
            pool = pool + [decl_repo]
        pushed = [r.pushed_at for r in pool if r.pushed_at is not None]
        if pushed:
            return ok(key, max(0.0, (now - max(pushed)) / DAY), **detail)
        if org["kind"] == "gone" or not non_fork:
            if beyond or not dead_state:
                return absent(key, "repo_gone" if org["kind"] == "gone" else "no_github_url", **detail)
            return failed(key, "gone_grace", **detail)
        return failed(key, "never_observed", **detail)

    async def _activity(self, occ: "Occupant", rids: list[int], now: int, D: int, replay: bool
                        ) -> tuple[dict[str, MetricObservation], list[AuthorClass], dict | None]:
        db = self.db
        commits = await load_window(db, rids, D - 90 * DAY, now + 1, replay=replay, observed_by=now)
        keys = await authors.resolve_author_keys(db, [c.author_key for c in commits])
        for c in commits:
            c.author_key = keys.get(c.author_key, c.author_key)
        repo_meta = {int(r["repo_id"]): r for r in await db.fetchall(
            f"SELECT repo_id, created_at, is_fork, force_push_suspected FROM github_repos WHERE repo_id IN "
            f"({','.join('?' * len(rids))})", rids)}
        flags: dict[str, list[str]] = {}
        by_repo: dict[int, list[CommitRow]] = defaultdict(list)
        for c in commits:
            by_repo[c.repo_id].append(c)
        for rid, cs in by_repo.items():
            m = repo_meta.get(rid) or {}
            flags.update(spam.flag_commits(cs, repo_created_at=m.get("created_at"),
                                           declared_fork=bool(m.get("is_fork"))))
        C = spam.human_unflagged(commits, flags)
        classes = classify_authors(C, D, now=now)
        ph = ",".join("?" * len(rids))
        evq = (f"SELECT repo_id, event_kind, event_at, payload_json FROM github_events WHERE repo_id IN ({ph}) AND "
               f"event_kind IN ('release','tag','pr_merged') AND event_at >= ?")
        params: list[Any] = [*rids, now - 90 * DAY]
        if not replay:
            evq += " AND observed_at <= ?"
            params.append(now)
        events = [{**e, "payload": json.loads(e["payload_json"] or "{}")} for e in await db.fetchall(evq, params)]
        rv, merged, reviewed = review_practice_90d(events, now)
        detail = {"repos": len(rids), "human_commits_90d": sum(1 for c in C if c.event_at >= D - 90 * DAY)}
        obs = {
            "active_authors_28d": ok("active_authors_28d", active_authors_28d(classes), **detail),
            "fte_devs_84d": ok("fte_devs_84d", fte_devs_84d(classes), **detail),
            "commit_days_90d": ok("commit_days_90d", commit_days_90d(C, D), **detail),
            "release_cadence_90d": ok("release_cadence_90d", release_cadence_90d(events, now)),
            "review_practice_90d": (ok("review_practice_90d", rv, merged=merged, reviewed=reviewed) if rv is not None
                                    else MetricObservation("review_practice_90d", None, "not_applicable",
                                                           "no_merged_prs", {"merged": 0})),
            "bus_factor": ok("bus_factor", bus_factor(C, D), **detail),
        }
        # commit_spam_suspected over the trailing 28 d
        human28 = [c for c in commits if not c.is_bot and not c.is_merge and c.event_at >= now - 28 * DAY]
        flagged28 = [c for c in human28 if flags.get(c.sha)]
        spam_ev = None
        if len(human28) >= 20 and len(flagged28) * 2 >= len(human28):
            hist = Counter(f for c in flagged28 for f in flags[c.sha])
            spam_ev = {"repos": sorted(rids), "human_commits_28d": len(human28), "flagged_28d": len(flagged28),
                       "flags_histogram": dict(sorted(hist.items())),
                       "force_push_repos": sorted(r for r, m in repo_meta.items() if m.get("force_push_suspected"))}
        return obs, list(classes.values()), spam_ev


# ── writing ──────────────────────────────────────────────────────────────────────────────────────────────────
def metric_rows(okey: str, obs: list[MetricObservation], *, as_of: int, now: int, run_id: str,
                backfilled: bool = False) -> list[dict]:
    rows = []
    for o in obs:
        vj = o.value_json()
        if backfilled:
            vj["backfilled"] = True
        value = o.value if o.status in ("ok", "absent") else None
        rows.append({"occupant_key": okey, "metric_key": o.metric_key, "as_of": as_of, "spec_version": SPEC_VERSION,
                     "stage": "raw", "value": value, "value_json": vj, "observed_at": now, "source": "github",
                     "run_id": run_id, "backfilled": 1 if backfilled else 0})
    return rows


async def write_metric_values(db: "Database", rows: list[dict]) -> int:
    """One ``put_metric_values`` call per run (first success wins; only a failed row is upgraded)."""
    return await put_metric_values(db, rows)


async def write_authors_daily(db: "Database", okey: str, classes: list[AuthorClass], *, day_ts: int, now: int,
                              run_id: str, backfilled: int = 0) -> int:
    rows = [{"occupant_key": okey, "author_key": a.author_key, "day_ts": day_ts, "commits_today": a.today,
             "cd28": a.w1, "cd56": a.w2, "cd84_3": a.w3, "cd84": a.cd84, "dev_class": a.dev_class,
             "repos_json": json.dumps(a.repos), "observed_at": now, "run_id": run_id, "backfilled": backfilled}
            for a in classes]
    async with db.transaction():
        await db.execute("DELETE FROM github_authors_daily WHERE occupant_key = ? AND day_ts = ?", (okey, day_ts))
        if rows:
            await db.upsert("github_authors_daily", rows, conflict=("occupant_key", "author_key", "day_ts"))
    return len(rows)


def pushed_days_row(okey: str, value: float | None, vj: dict, *, now: int, run_id: str) -> dict:
    vj = {**vj, "def": "wb-1", "via": "github_metrics"}
    return {"occupant_key": okey, "metric_key": "repo_pushed_days", "as_of": floor_day(now),
            "spec_version": SPEC_VERSION, "stage": "raw", "value": value if vj.get("status") == "ok" else None,
            "value_json": vj, "observed_at": now, "source": "github", "run_id": run_id, "backfilled": 0}


async def run_metrics(db: "Database", *, now: int, run_id: str, netuids: set[int] | None = None, write: bool = True,
                      token_state: str | None = None) -> dict[str, Any]:
    occs = [o for o in await active_occupants(db, include_pre_launch=True) if o.netuid != 0
            and (netuids is None or o.netuid in netuids)]
    calc = P2Metrics(db, token_state=token_state)
    as_of = slot_for(now)
    D = floor_day(now)
    rows: list[dict] = []
    results: list[OccupantResult] = []
    status = Counter()
    for occ in occs:
        r = await calc.compute(occ, now)
        results.append(r)
        rows += metric_rows(occ.occupant_key, r.observations, as_of=as_of, now=now, run_id=run_id)
        if r.pushed_row is not None:
            rows.append(pushed_days_row(occ.occupant_key, r.pushed_row[0], r.pushed_row[1], now=now, run_id=run_id))
        for o in r.observations:
            status[o.status] += 1
    out: dict[str, Any] = {"occupants": len(occs), "as_of": to_iso(as_of), "status": dict(status), "rows": 0,
                           "authors_rows": 0, "spam_events": 0, "results": results}
    if not write:
        return out
    out["rows"] = await write_metric_values(db, rows)
    for r in results:
        out["authors_rows"] += await write_authors_daily(db, r.occupant_key, r.authors, day_ts=D, now=now,
                                                         run_id=run_id)
        if r.spam_event is not None:
            ev = await ghevents.emit(db, "commit_spam_suspected", occupant_key=r.occupant_key, netuid=r.netuid,
                                     payload=r.spam_event, now=now, run_id=run_id)
            out["spam_events"] += 1 if ev else 0
    return out
