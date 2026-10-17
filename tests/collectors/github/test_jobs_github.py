"""Spec 06 §2.1 / §5 / §7: the five jobs — registry, degraded mode, soft deadlines, health extras."""
import pytest

import app.config as app_config
from app.collectors.github import client as client_mod
from app.collectors.github.jobs import (GithubActivityJob, GithubHtmlProbeJob, GithubMetricsJob, GithubOrgEnumJob,
                                        GithubResolveJob, next_daily)
from app.jobs import SPEC_BY_ID, JobRegistry, RunContext
from app.util.time import parse_iso
from tests.collectors.github.conftest import NOW, R, cand, repo, seed_occupant

IDS = ("github_resolve", "github_org_enum", "github_activity", "github_metrics", "github_html_probe")


@pytest.mark.asyncio
async def test_registry_has_the_five_jobs(rt):
    JobRegistry.build(rt)
    assert set(IDS) <= set(rt.jobs)
    assert "backfill_github" in rt.unavailable                       # 17's S9 job, not part of 06
    assert SPEC_BY_ID["github_metrics"].catchup_avoid_utc == ("18:00-23:30",)
    from app.jobs import spec_liveness_max_age
    assert spec_liveness_max_age(SPEC_BY_ID["github_resolve"]) == 45120


def test_pause_groups_cover_github_jobs():
    from app.control import PAUSE_GROUPS
    assert set(IDS) | {"backfill_github"} == set(PAUSE_GROUPS["github"])
    assert "github_html_probe" in PAUSE_GROUPS["tmc"]


def test_next_daily_and_activity_deadline():
    t = parse_iso("2026-10-17T20:30:00Z")
    assert next_daily(t, "23:30") == parse_iso("2026-10-17T23:30:00Z")
    assert next_daily(parse_iso("2026-10-17T23:45:00Z"), "23:30") == parse_iso("2026-10-18T23:30:00Z")
    job = GithubActivityJob.__new__(GithubActivityJob)
    job.spec = SPEC_BY_ID["github_activity"]
    ctx = RunContext("r", t, "schedule", None, app_config.settings, None)
    assert job.soft_deadline(ctx) == parse_iso("2026-10-17T23:20:00Z")    # metrics cron − 600 s


def _ctx(rt, settings, now=NOW, run_id="run"):
    return RunContext(run_id, int(now), "test", rt, settings, None)


@pytest.mark.asyncio
async def test_degraded_mode_no_token(rt, alerts_sink):
    s = app_config.Settings(_env_file=None, GITHUB_TOKEN="")
    old = app_config.settings
    app_config.settings = s
    GithubResolveJob._notice_sent = False
    try:
        await seed_occupant(rt.db, 39, cands=[cand("chain", "https://github.com/username/repo")])
        out = await GithubResolveJob(rt, SPEC_BY_ID["github_resolve"]).collect(_ctx(rt, s))
        assert out.rows == 0 and alerts_sink.sent and "no GITHUB_TOKEN" in alerts_sink.sent[0]
        assert (await GithubOrgEnumJob(rt, SPEC_BY_ID["github_org_enum"]).collect(_ctx(rt, s))).note.startswith("skipped")
        assert (await GithubActivityJob(rt, SPEC_BY_ID["github_activity"]).collect(_ctx(rt, s))).note.startswith("skipped")
        m = await GithubMetricsJob(rt, SPEC_BY_ID["github_metrics"]).collect(_ctx(rt, s))
        assert m.rows == 11                                           # 10 absent (placeholder) + repo_pushed_days
    finally:
        app_config.settings = old


@pytest.mark.asyncio
async def test_resolve_job_end_to_end_with_health(rt, fake):
    s = app_config.Settings(_env_file=None, GITHUB_TOKEN="t")
    old = app_config.settings
    app_config.settings = s
    c = fake({"/repos/chutesai/chutes": R(200, repo(884216433, "chutesai/chutes"), headers={"x-ratelimit-remaining": "4999",
                                                                                            "x-ratelimit-limit": "5000"})},
             db=rt.db)
    client_mod._clients[id(rt)] = c
    c.http, c.db = rt.http, rt.db
    try:
        await seed_occupant(rt.db, 64, cands=[cand("chain", "https://github.com/chutesai/chutes")])
        job = GithubResolveJob(rt, SPEC_BY_ID["github_resolve"])
        out = await job.collect(_ctx(rt, s))
        assert out.rows == 1 and out.meta["queue"]["resolved"] == 1
        h = job.health_extra()
        assert h["token_state"] == "ok" and h["queue"]["refs"] == 1 and "remaining" in h["quota"]
    finally:
        app_config.settings = old
        client_mod._clients.pop(id(rt), None)


@pytest.mark.asyncio
async def test_degraded_run_raises_after_commit(rt, fake):
    from app.collectors.github.client import GitHubRunDegraded, GitHubUnavailable
    s = app_config.Settings(_env_file=None, GITHUB_TOKEN="t")
    old = app_config.settings
    app_config.settings = s
    c = fake({f"/repos/o/r{i}": GitHubUnavailable("502") for i in range(5)}, db=rt.db)
    client_mod._clients[id(rt)] = c
    c.http, c.db = rt.http, rt.db
    try:
        for i in range(5):
            await seed_occupant(rt.db, 70 + i, cands=[cand("chain", f"https://github.com/o/r{i}")])
        with pytest.raises(GitHubRunDegraded):
            await GithubResolveJob(rt, SPEC_BY_ID["github_resolve"]).collect(_ctx(rt, s))
        n = await rt.db.fetchval("SELECT COUNT(*) FROM github_declared_refs WHERE last_attempt_at IS NOT NULL")
        assert n == 5                                                 # work committed before the raise
    finally:
        app_config.settings = old
        client_mod._clients.pop(id(rt), None)


@pytest.mark.asyncio
async def test_html_probe_job(rt, fake):
    s = app_config.Settings(_env_file=None, GITHUB_TOKEN="t", GITHUB_TMC_CROSSCHECK_ENABLED=False)
    old = app_config.settings
    app_config.settings = s
    html = ('<meta name="octolytics-dimension-repository_id" content="884216433">'
            '<meta name="octolytics-dimension-repository_nwo" content="chutesai/chutes">'
            '<meta name="octolytics-dimension-repository_is_fork" content="false">')
    c = fake({"/repos/rayonlabs/chutes": R(301, location="https://api.github.com/repositories/884216433"),
              "/repositories/884216433": R(200, repo(884216433, "chutesai/chutes"))}, db=rt.db)

    async def get_html(name):
        from app.collectors.github.client import GHResponse
        if name == "rayonlabs/chutes":
            return GHResponse(301, None, {}, location="https://github.com/chutesai/chutes")
        return GHResponse(200, None, {}, text=html)
    c.get_html = get_html
    client_mod._clients[id(rt)] = c
    c.http, c.db = rt.http, rt.db
    try:
        await seed_occupant(rt.db, 64, cands=[cand("chain", "https://github.com/rayonlabs/chutes")])
        from app.collectors.github.resolver import Resolver
        await Resolver(rt.db, c, run_id="r", now=NOW).run()
        job = GithubHtmlProbeJob(rt, SPEC_BY_ID["github_html_probe"])
        out = await job.collect(_ctx(rt, s))
        hp = out.meta["html_probe"]
        assert hp["checked"] == 2 and hp["rename_hints"] == 0 and hp["parse_miss"] == 0   # 301 → known canonical
        await rt.db.execute("UPDATE github_repos SET full_name = 'chutesai/old-name' WHERE repo_id = 884216433")
        out = await job.collect(_ctx(rt, s, run_id="run2"))
        assert out.meta["html_probe"]["rename_hints"] == 2                # nwo differs + 301 to another name
        assert await rt.db.fetchval("SELECT last_attempt_at FROM github_declared_refs") is None   # re-verify queued
        assert job.health_extra()["rename_hints"] == 2
    finally:
        app_config.settings = old
        client_mod._clients.pop(id(rt), None)


def test_parse_octolytics_and_miss():
    from app.collectors.github.html_probe import parse_octolytics
    m = parse_octolytics('<meta name="octolytics-dimension-repository_id" content="5"><p>')
    assert m == {"repository_id": "5"} and parse_octolytics("<html>") == {}


@pytest.mark.asyncio
async def test_tmc_crosscheck_mismatch(db):
    from app.collectors.github import tmc_crosscheck
    okey = await seed_occupant(db, 22, cands=[cand("chain", "https://github.com/datura-ai/desearch")])
    await db.execute("INSERT INTO github_declared_refs (occupant_key, source, raw_value, kind, owner, repo, ref_key, "
                     "state, repo_id, first_seen_at, observed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                     (okey, "chain", "u", "repo", "datura-ai", "desearch", "datura-ai/desearch", "resolved", 9, NOW, NOW))
    await db.execute("INSERT INTO github_occupant_repos (occupant_key, repo_id, role, in_scope, first_seen_at, "
                     "last_seen_at) VALUES (?,9,'declared',1,?,?)", (okey, NOW, NOW))
    await db.execute("INSERT INTO chain_hourly (netuid, hour_ts, occupant_key, event_at, observed_at, source, "
                     "commits_30d) VALUES (22, ?, ?, ?, ?, 'sse', 40)", (NOW - 3600, okey, NOW, NOW))

    class Tmc:
        calls = 0

        async def get(self, path):
            Tmc.calls += 1
            from app.collectors.chain_tmc import TmcResponse
            return TmcResponse(True, 200, {"owner": "datura-ai", "repo": "desearch", "is_valid": True})
    out = await tmc_crosscheck.run(db, Tmc(), now=NOW)
    assert out["checked"] == 1 and out["mismatches"] == 1 and Tmc.calls == 1
    assert out["details"][0]["tmc_key"] == "datura-ai/desearch"
    assert tmc_crosscheck.mismatch(10, 25) and not tmc_crosscheck.mismatch(10, 15) and tmc_crosscheck.mismatch(0, 3)


@pytest.mark.asyncio
async def test_ops_digest_pat_expiry_line(rt, settings_override):
    from app.collectors.github.client import QUOTA
    from app.ops.digest import build_digest
    from app.util.time import now_ts, to_iso
    QUOTA.token_expires_at = None
    settings_override(GITHUB_TOKEN="t", GITHUB_TOKEN_EXPIRES_AT=to_iso(now_ts() + 10 * 86400)[:10])
    text = await build_digest(rt)
    assert "GitHub PAT expires in" in text and "⚠️ GitHub PAT expires" in text
    settings_override(GITHUB_TOKEN="t", GITHUB_TOKEN_EXPIRES_AT="")
    assert "GitHub PAT expiry unknown" in await build_digest(rt)
