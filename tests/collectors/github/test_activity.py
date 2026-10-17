"""Spec 06 §2.5 / §9: per-repo activity collection (id paths, dedupe, watermark, hygiene, originality)."""
import json

import pytest

from app.collectors.github.activity import ActivityCollector, run_activity
from app.collectors.github.orgs import upsert_repo_row
from app.collectors.github.models import repo_fields
from app.util.time import to_iso
from tests.collectors.github.conftest import DAY, NOW, R, repo

RID = 884216433
P = f"/repositories/{RID}"


def commit(sha, t, login="alice", email="alice@x.io", parents=1, msg="feat: thing", typ="User"):
    return {"sha": sha, "commit": {"author": {"name": login, "email": email, "date": to_iso(t)},
                                   "committer": {"date": to_iso(t)}, "message": msg},
            "author": {"login": login, "type": typ} if login else None, "parents": [{"sha": "p"}] * parents}


async def seed_repo(db, **kw):
    f = repo_fields(repo(RID, "chutesai/chutes", **kw))
    await upsert_repo_row(db, f, method="direct", now=NOW - DAY, verified=True)
    return await db.fetchone("SELECT * FROM github_repos WHERE repo_id = ?", (RID,))


def base_routes(commits):
    return {
        f"{P}/commits": R(200, commits),
        f"{P}/releases": R(200, [{"tag_name": "v1.0", "published_at": to_iso(NOW - 3 * DAY), "draft": False,
                                   "prerelease": False},
                                  {"tag_name": "v2.0-draft", "draft": True, "published_at": None}]),
        f"{P}/tags": R(200, [{"name": "v1.0", "commit": {"sha": "c1"}}, {"name": "v0.9", "commit": {"sha": "c2"}},
                             {"name": "v0.1", "commit": {"sha": "old"}}]),
        f"{P}/commits/old": R(200, {"commit": {"committer": {"date": to_iso(NOW - 200 * DAY)}}}),
        f"{P}/pulls": R(200, [{"number": 5, "merged_at": to_iso(NOW - DAY), "user": {"login": "bob"}},
                              {"number": 6, "merged_at": None, "user": {"login": "bob"}},
                              {"number": 7, "merged_at": to_iso(NOW - 2 * DAY), "user": {"login": "dependabot[bot]", "type": "Bot"}}]),
        f"{P}/pulls/5/reviews": R(200, [{"state": "APPROVED", "user": {"login": "carol"}},
                                       {"state": "COMMENTED", "user": {"login": "bob"}},
                                       {"state": "APPROVED", "user": {"login": "coderabbit[bot]", "type": "Bot"}}]),
        f"{P}/pulls/7/reviews": R(200, []),
        f"{P}/stats/participation": [R(202), R(200, {"all": [1] * 52, "owner": [0] * 52})],
        f"{P}/stats/contributors": R(200, [{"author": {"login": "alice"}, "total": 9}]),
        f"{P}/community/profile": R(200, {"files": {"license": {"key": "mit"}, "readme": {}, "contributing": None,
                                                    "code_of_conduct": None}}),
        f"{P}/readme": R(200, {"size": 1200}),
        f"{P}/contents/.github/workflows": R(200, [{"name": "ci.yml"}]),
        f"{P}/contents/.github/dependabot.yml": R(404),
        f"{P}/contents/renovate.json": R(200, {"name": "renovate.json"}),
        f"{P}/contents/SECURITY.md": R(404), f"{P}/contents/.github/SECURITY.md": R(404),
        "/repos/opentensor/bittensor-subnet-template": R(301, location="https://api.github.com/repositories/676155963"),
        "/repositories/676155963": R(200, {"id": 676155963, "full_name": "RaoFoundation/bittensor-subnet-template",
                                           "default_branch": "main"}),
        "/repositories/676155963/git/trees/main": R(200, {"sha": "tpl1", "tree": [
            {"path": "README.md", "type": "blob", "sha": "r0"}, {"path": "neurons/miner.py", "type": "blob", "sha": "m0"}]}),
        f"{P}/git/trees/main": R(200, {"sha": "x", "truncated": False, "tree": [
            {"path": "README.md", "type": "blob", "sha": "r1"}, {"path": "neurons/miner.py", "type": "blob", "sha": "m0"},
            {"path": "src", "type": "tree", "sha": "t"}, {"path": "src/a.py", "type": "blob", "sha": "a"}]}),
    }


@pytest.mark.asyncio
async def test_collect_repo_full_pass(db, fake):
    r = await seed_repo(db)
    commits = [commit("c1", NOW - DAY), commit("c2", NOW - 2 * DAY, login=None, email="12+bob@users.noreply.github.com"),
               commit("c3", NOW - 3 * DAY, login="dependabot[bot]", typ="Bot"), commit("c4", NOW - 4 * DAY, parents=2)]
    c = fake(base_routes(commits), db=db)
    col = ActivityCollector(db, c, run_id="run1", now=NOW)
    res = await col.collect_repo(dict(r), deep=True)
    assert res.ok and res.commits_new == 4
    assert all(p.startswith("/repositories/") or p == "/repos/opentensor/bittensor-subnet-template" for p in c.paths())
    rows = {x["sha"]: x for x in await db.fetchall("SELECT * FROM github_commits")}
    assert rows["c2"]["author_key"] == "bob" and rows["c3"]["is_bot"] == 1 and rows["c4"]["is_merge"] == 1
    assert all(isinstance(x["event_at"], int) and x["backfilled"] == 0 and x["source"] == "github_api" for x in rows.values())
    ev = {e["dedupe_key"]: e for e in await db.fetchall("SELECT * FROM github_events")}
    assert set(ev) == {f"release:{RID}:v1.0", f"tag:{RID}:v0.9", f"tag:{RID}:v0.1", f"pr_merged:{RID}:5",
                       f"pr_merged:{RID}:7"}
    assert ev[f"tag:{RID}:v0.9"]["event_at"] == NOW - 2 * DAY          # date from the stored commit, no look-up
    pr = json.loads(ev[f"pr_merged:{RID}:5"]["payload_json"])
    assert pr["review_count"] == 1 and pr["reviewers"] == ["carol"]
    g = await db.fetchone("SELECT * FROM github_repos WHERE repo_id = ?", (RID,))
    assert g["activity_ok_at"] == NOW and g["last_commit_seen_at"] == NOW - DAY and g["stats_checked_at"] == NOW
    hj = json.loads(g["hygiene_json"])
    assert hj["license"] and hj["readme_bytes"] == 1200 and hj["ci"] and hj["dep_update"] and hj["security"] is False
    oj = json.loads(g["originality_json"])
    assert oj["repo_files"] == 3 and oj["identical_files"] == 1 and oj["template_sha"] == "tpl1"
    assert oj["template_full_name"] == "raofoundation/bittensor-subnet-template"          # moved template followed by id
    daily = await db.fetchall("SELECT * FROM github_activity_daily ORDER BY day_ts")
    assert len(daily) == 4 and sum(d["commits_bot"] for d in daily) == 1 and sum(d["commits_merge"] for d in daily) == 1
    assert c.paths().count(f"{P}/stats/participation") == 2           # one 202 retry


@pytest.mark.asyncio
async def test_second_run_dedupes_and_skips_due_checks(db, fake):
    r = await seed_repo(db)
    c = fake(base_routes([commit("c1", NOW - DAY)]), db=db)
    await ActivityCollector(db, c, run_id="r1", now=NOW).collect_repo(dict(r), deep=True)
    n_calls = len(c.calls)
    r2 = await db.fetchone("SELECT * FROM github_repos WHERE repo_id = ?", (RID,))
    c.t = NOW + DAY
    res = await ActivityCollector(db, c, run_id="r2", now=NOW + DAY).collect_repo(dict(r2), deep=True)
    assert res.commits_new == 0 and res.events_new == 0
    later = [p for p, _ in c.calls[n_calls:]]
    assert f"{P}/pulls/5/reviews" not in later                         # stored PRs are not re-fetched
    assert f"{P}/community/profile" not in later and f"{P}/stats/participation" not in later
    since = c.calls[n_calls][1]["since"]
    assert since == to_iso(NOW - 3 * DAY)                              # watermark − 2 d


@pytest.mark.asyncio
async def test_pr_written_by_backfill_not_duplicated(db, fake):
    from app.collectors.github.ghevents import raw_event
    r = await seed_repo(db)
    await raw_event(db, repo_id=RID, kind="pr_merged", dedupe_key=f"pr_merged:{RID}:5", payload={"number": 5},
                    now=NOW - DAY, run_id="bf", event_at=NOW - DAY, backfilled=1)
    c = fake(base_routes([]), db=db)
    await ActivityCollector(db, c, run_id="r", now=NOW).collect_repo(dict(r), deep=False)
    assert await db.fetchval(f"SELECT COUNT(*) FROM github_events WHERE dedupe_key = 'pr_merged:{RID}:5'") == 1
    assert f"{P}/pulls/5/reviews" not in c.paths()


@pytest.mark.asyncio
async def test_empty_repo_409_is_ok_and_404_requeues(db, fake):
    r = await seed_repo(db)
    c = fake({f"{P}/commits": R(409, {"message": "Git Repository is empty."})}, db=db)
    res = await ActivityCollector(db, c, run_id="r", now=NOW).collect_repo(dict(r), deep=False)
    assert res.ok and res.commits_new == 0
    await db.execute("INSERT INTO github_declared_refs (occupant_key, source, raw_value, kind, state, repo_id, "
                     "first_seen_at, last_attempt_at, observed_at) VALUES ('x:1','chain','u','repo','resolved',?,?,?,?)",
                     (RID, NOW, NOW, NOW))
    c.routes[f"{P}/commits"] = R(404)
    res2 = await ActivityCollector(db, c, run_id="r", now=NOW).collect_repo(dict(r), deep=False)
    assert not res2.ok
    assert await db.fetchval("SELECT last_attempt_at FROM github_declared_refs WHERE occupant_key = 'x:1'") is None


@pytest.mark.asyncio
async def test_template_fork_compare_ahead_by(db, fake):
    r = await seed_repo(db, fork=True, parent="RaoFoundation/bittensor-subnet-template")
    routes = base_routes([])
    routes["/repositories/676155963/compare/raofoundation:main...chutesai:main"] = R(200, {"ahead_by": 0})
    c = fake(routes, db=db)
    await ActivityCollector(db, c, run_id="r", now=NOW).collect_repo(dict(r), deep=True)
    oj = json.loads(await db.fetchval("SELECT originality_json FROM github_repos WHERE repo_id = ?", (RID,)))
    assert oj["ahead_by"] == 0


@pytest.mark.asyncio
async def test_run_activity_scope_and_deadline(db, fake):
    from tests.collectors.github.conftest import cand, seed_occupant
    okey = await seed_occupant(db, 64, cands=[cand("chain", "https://github.com/chutesai/chutes")])
    await seed_repo(db)
    await db.execute("INSERT INTO github_occupant_repos (occupant_key, repo_id, role, in_scope, first_seen_at, "
                     "last_seen_at) VALUES (?,?,?,?,?,?)", (okey, RID, "declared", 1, NOW, NOW))
    c = fake(base_routes([commit("c1", NOW - DAY)]), db=db)
    out = await run_activity(db, c, run_id="r", now=NOW, deadline=NOW - 1)
    assert out["left"] == 1 and out["ok"] == 0
    out2 = await run_activity(db, c, run_id="r", now=NOW, deadline=NOW + 3600)
    assert out2["ok"] == 1 and out2["commits_new"] == 1
