"""The five 06 jobs (spec 06 §2.1; 01's ``JOB_TABLE`` carries the rows). Soft deadlines end a run early with
``note="partial: …"`` instead of letting 01's ``wait_for`` cancel it; leftovers go first next run."""
from __future__ import annotations

from typing import Any

import app.config as app_config
from app.collectors.github.client import GitHubRunDegraded, get_client
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.util.time import floor_day, hhmm

NO_TOKEN_TEXT = "github: no GITHUB_TOKEN — builder pillar degraded"


def next_daily(ts: int, hhmm_s: str) -> int:
    h, m = hhmm(hhmm_s)
    t = floor_day(ts) + h * 3600 + m * 60
    return t if t > ts else t + 86400


class _GithubJob(CollectorJob):
    deadline_margin = 120

    def soft_deadline(self, run: RunContext) -> float:
        return float(run.started_at + (self.spec.timeout_sec or 1800) - self.deadline_margin)

    def client(self, run: RunContext):
        c = get_client(run.rt)
        c.reset_stats(self.id)
        return c

    def check_degraded(self, client) -> None:
        if client.degraded(self.id):
            st = client.stats.get(self.id) or {}
            raise GitHubRunDegraded(f"{st.get('failed')} of {st.get('calls')} GitHub calls failed")

    def _quota_extra(self) -> dict:
        from app.collectors.github.client import QUOTA
        c = get_client(self.rt) if self.rt.http is not None else None
        tok = "missing" if not app_config.settings.GITHUB_TOKEN else (c.token_state if c else "ok")
        return {"token_state": tok, "quota": QUOTA.snapshot()}


class GithubResolveJob(_GithubJob):
    _notice_sent = False

    async def collect(self, run: RunContext) -> JobOutcome:
        from app.collectors.github.resolver import Resolver
        s = run.settings
        if not s.GITHUB_TOKEN and not GithubResolveJob._notice_sent and run.rt.alerts is not None:
            GithubResolveJob._notice_sent = True
            await self._safe(run.rt.alerts.fire("github_no_token", NO_TOKEN_TEXT), "no-token notice")
        client = self.client(run)
        res = Resolver(run.rt.db, client, run_id=run.run_id, now=run.started_at, deadline=self.soft_deadline(run))
        rep = await res.run()
        self._last = {"queue": {"refs": rep.refs, **rep.queue}, "events": rep.events}
        self.check_degraded(client)
        note = (f"partial: {rep.left} refs left" if rep.left else
                f"refs={rep.refs} attempted={rep.attempted} failed={rep.failed}")
        return JobOutcome(rows=rep.attempted, note=note,
                          meta={"queue": rep.queue, "events": rep.events, "left": rep.left,
                                "calls": client.stats.get(self.id)})

    def health_extra(self) -> dict | None:
        return {**self._quota_extra(), "queue": (getattr(self, "_last", None) or {}).get("queue")}


class GithubOrgEnumJob(_GithubJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        from app.collectors.github import ghevents
        from app.collectors.github.orgs import canonical_owner, enumerate_owner, is_subnet_related, store_listing
        from app.collectors.github.resolver import Resolver, source_refs
        from app.registry import active_occupants, resolved
        db = run.rt.db
        if not run.settings.GITHUB_TOKEN:
            return JobOutcome(note="skipped: no GITHUB_TOKEN (degraded mode)")
        client = self.client(run)
        deadline = self.soft_deadline(run)
        now = run.started_at
        occs = [o for o in await active_occupants(db, include_pre_launch=True) if o.netuid != 0]
        plan: list[tuple[Any, dict, str | None, str | None]] = []
        owners: dict[str, list[Any]] = {}
        for occ in occs:
            r = await resolved(db, occ.occupant_key, "github_repo")
            _, src = source_refs(r)
            owner, _ = await canonical_owner(db, occ.occupant_key, src)
            rows = {x["source"]: x for x in await db.fetchall(
                "SELECT * FROM github_declared_refs WHERE occupant_key = ?", (occ.occupant_key,))}
            plan.append((occ, rows, src, owner))
            if owner:
                owners.setdefault(owner, []).append(occ)
        done = left = failed = events = 0
        for owner, linked in sorted(owners.items()):
            if client.clock() >= deadline:
                left += 1
                continue
            prev = await db.fetchone("SELECT kind FROM github_orgs WHERE owner = ?", (owner,))
            listing = await enumerate_owner(client, owner, known_kind=(prev or {}).get("kind"), job=self.id,
                                            deadline=deadline)
            st = await store_listing(db, listing, now=now, run_id=run.run_id)
            done += 1
            failed += 0 if listing.ok else 1
            for f in st["flips"]:
                for occ in linked:
                    ev = await ghevents.emit(db, "org_repo_archived", occupant_key=occ.occupant_key, netuid=occ.netuid,
                                             payload={"owner": owner, "repo_id": f["repo_id"],
                                                      "full_name": f["full_name"],
                                                      "is_subnet_related": is_subnet_related(f["name"], f["description"],
                                                                                             occ.name)},
                                             now=now, run_id=run.run_id, repo_id=f["repo_id"])
                    events += 1 if ev else 0
        res = Resolver(db, client, run_id=run.run_id, now=now, deadline=deadline, job=self.id)
        for occ, rows, src, _ in plan:
            await res.finalise(occ, rows, src, enumerate_missing=False)
        self.check_degraded(client)
        note = f"partial: {left} owners left" if left else f"owners={done} failed={failed}"
        return JobOutcome(rows=done, note=note, meta={"owners": len(owners), "failed": failed,
                                                      "org_repo_archived": events, "events": res.report.events})


class GithubActivityJob(_GithubJob):
    deadline_margin = 300

    def soft_deadline(self, run: RunContext) -> float:
        s = app_config.settings
        hard = run.started_at + (self.spec.timeout_sec or 10500) - 300
        return float(min(hard, next_daily(run.started_at, s.GITHUB_METRICS_CRON) - 600))

    async def collect(self, run: RunContext) -> JobOutcome:
        from app.collectors.github.activity import run_activity
        if not run.settings.GITHUB_TOKEN:
            return JobOutcome(note="skipped: no GITHUB_TOKEN (degraded mode)")
        client = self.client(run)
        deadline = self.soft_deadline(run)
        if deadline <= run.started_at + 60:
            # a catch-up just before the metrics run: work the full timeout instead (metrics reads what is there)
            deadline = float(run.started_at + (self.spec.timeout_sec or 10500) - 300)
        out = await run_activity(run.rt.db, client, run_id=run.run_id, now=run.started_at, deadline=deadline)
        self._last = out
        self.check_degraded(client)
        note = (f"partial: {out['left']} repos left" if out["left"] else
                f"repos={out['repos']} ok={out['ok']} failed={out['failed']} commits+{out['commits_new']}")
        return JobOutcome(rows=out["commits_new"] + out["events_new"], note=note,
                          meta={k: v for k, v in out.items()} | {"calls": client.stats.get(self.id)})

    def health_extra(self) -> dict | None:
        last = getattr(self, "_last", None) or {}
        return {"scope": {"repos_in_scope": last.get("repos")}, "repos_left": last.get("left"),
                "rate_limit_sleeps_24h": self._quota_extra()["quota"]["rate_limit_sleeps_24h"]}


class GithubMetricsJob(_GithubJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        from app.collectors.github.metrics import run_metrics
        tok = get_client(run.rt).token_state if run.rt.http is not None else None
        out = await run_metrics(run.rt.db, now=run.started_at, run_id=run.run_id, token_state=tok)
        out.pop("results", None)
        return JobOutcome(rows=out["rows"], note=f"as_of={out['as_of']} occupants={out['occupants']} "
                                                 f"rows={out['rows']}", meta=out)


class GithubHtmlProbeJob(_GithubJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        from app.collectors.github import html_probe, tmc_crosscheck
        s = run.settings
        client = self.client(run)
        meta: dict[str, Any] = {}
        rows = 0
        if s.GITHUB_HTML_PROBE_ENABLED:
            until = await self.get_state("disabled_until")
            out = await html_probe.run(run.rt.db, client, now=run.started_at, deadline=self.soft_deadline(run),
                                       disabled_until=until)
            if out.get("disabled_until") and out["disabled_until"] != until:
                await self.set_state("disabled_until", out["disabled_until"])
            out.pop("lines", None)
            meta["html_probe"] = out
            rows += out["checked"]
        if s.GITHUB_TMC_CROSSCHECK_ENABLED:
            from app.collectors.chain_tmc import TmcClient
            tmc = TmcClient(run.rt.http) if s.TMC_ENABLED else None
            try:
                meta["tmc_crosscheck"] = await tmc_crosscheck.run(run.rt.db, tmc, now=run.started_at)
            except Exception as exc:          # advisory: never fails the probe run
                logger.warning("github.crosscheck.failed", data={"error": f"{type(exc).__name__}: {exc}"})
        self._last = meta
        hp = meta.get("html_probe") or {}
        cc = meta.get("tmc_crosscheck") or {}
        return JobOutcome(rows=rows, note=f"probed={hp.get('checked', 0)} hints={hp.get('rename_hints', 0)} "
                                          f"crosscheck={cc.get('checked', 0)}/{cc.get('mismatches', 0)}", meta=meta)

    def health_extra(self) -> dict | None:
        last = getattr(self, "_last", None) or {}
        hp = last.get("html_probe") or {}
        cc = last.get("tmc_crosscheck") or {}
        return {"parse_miss_rate": hp.get("parse_miss_rate"), "rename_hints": hp.get("rename_hints"),
                "disabled_until": hp.get("disabled_until"),
                "tmc_crosscheck": {"checked": cc.get("checked"), "mismatches": cc.get("mismatches")}}
