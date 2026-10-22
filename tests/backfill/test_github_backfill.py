"""Spec 17 §9 test 25 (+ scope, window, rollup): the S9 GitHub history backfill against 06's route-table client."""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from app.backfill import common
from app.backfill import github_backfill as gb
from app.collectors.github.client import GitHubQuota
from app.collectors.github.ghevents import raw_event
from app.collectors.github.models import repo_fields
from app.collectors.github.orgs import upsert_repo_row
from app.util.time import parse_iso, to_iso
from tests.collectors.github.conftest import DAY, NOW, FakeClient, R, cand, repo, seed_occupant

RID, RID2 = 884216433, 555
P, P2 = f"/repositories/{RID}", f"/repositories/{RID2}"
SINCE = parse_iso("2025-02-13T00:00:00Z")
IN_WINDOW = parse_iso("2026-10-17T01:00:00Z")


def S(**kw) -> SimpleNamespace:
    base = dict(BACKFILL_GITHUB_SINCE="2025-02-13", BACKFILL_GITHUB_MAX_REPOS_PER_OCCUPANT=30,
                BACKFILL_GITHUB_WINDOW_UTC="00:30-08:00", BACKFILL_GITHUB_RESERVE_REMAINING=1500,
                BACKFILL_GITHUB_MAX_PAGES_PER_REPO=50, BACKFILL_GITHUB_PRS_ENABLED=True,
                BACKFILL_GITHUB_PR_MAX_PER_REPO=200)
    return SimpleNamespace(**{**base, **kw})


def commit(sha, t, login="alice", email="alice@x.io", parents=1):
    return {"sha": sha, "commit": {"author": {"name": login, "email": email, "date": to_iso(t)},
                                   "committer": {"date": to_iso(t)}, "message": f"feat: {sha}"},
            "author": {"login": login, "type": "User"} if login else None, "parents": [{"sha": "p"}] * parents}


def routes(prefix: str = P) -> dict:
    nxt = f"https://api.github.com{prefix}/commits?since=x&page=2"
    return {
        f"{prefix}/commits": R(200, [commit("c1", NOW - 10 * DAY), commit("c2", NOW - 200 * DAY, login="bob")],
                               headers={"link": f'<{nxt}>; rel="next"'}),
        nxt: R(200, [commit("c3", NOW - 400 * DAY), commit("c4", NOW - 401 * DAY, parents=2)]),
        f"{prefix}/releases": R(200, [{"tag_name": "v1.0", "published_at": to_iso(NOW - 3 * DAY), "draft": False},
                                      {"tag_name": "v0.9", "published_at": to_iso(NOW - 300 * DAY), "draft": False},
                                      {"tag_name": "v2-draft", "draft": True, "published_at": None}]),
        f"{prefix}/pulls": R(200, [
            {"number": 5, "merged_at": to_iso(NOW - DAY), "updated_at": to_iso(NOW - DAY), "user": {"login": "bob"}},
            {"number": 6, "merged_at": None, "updated_at": to_iso(NOW - 2 * DAY), "user": {"login": "bob"}},
            {"number": 4, "merged_at": to_iso(NOW - 900 * DAY), "updated_at": to_iso(NOW - 900 * DAY),
             "user": {"login": "bob"}}]),
        f"{prefix}/pulls/5/reviews": R(200, [{"state": "APPROVED", "user": {"login": "carol"}},
                                             {"state": "COMMENTED", "user": {"login": "bob"}}]),
    }


@pytest.fixture
def side(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "backfill_dir", lambda: tmp_path)
    return tmp_path


async def seed(db, rid=RID, full="chutesai/chutes", role="declared", netuid=64, **kw) -> str:
    okey = await seed_occupant(db, netuid, cands=[cand("chain", f"https://github.com/{full}")])
    await upsert_repo_row(db, repo_fields(repo(rid, full, **kw)), method="direct", now=NOW - DAY, verified=True)
    await db.execute("INSERT INTO github_occupant_repos (occupant_key, repo_id, role, in_scope, first_seen_at, "
                     "last_seen_at) VALUES (?,?,?,?,?,?)", (okey, rid, role, 1, NOW, NOW))
    return okey


def client(db, r: dict, t: float = IN_WINDOW) -> FakeClient:
    c = FakeClient(r, db=db, now=t)
    return c


# ── 25: one repo end to end ──────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_github_backfill_repo(db, side):
    await seed(db)
    await raw_event(db, repo_id=RID, kind="release", dedupe_key=f"release:{RID}:v1.0", payload={"tag_name": "v1.0"},
                    now=NOW - DAY, run_id="live", event_at=NOW - 3 * DAY)                   # 06 already has it
    c = client(db, routes())
    out = await gb.run_backfill(db, c, S(), run_id="bf1", soft_deadline=IN_WINDOW + 3600)
    assert not out.note.startswith("partial") and "done=1" in out.note
    assert all(p.startswith("/repositories/") or p.startswith("https://api.github.com/repositories/")
               for p in c.paths())
    rows = {x["sha"]: x for x in await db.fetchall("SELECT * FROM github_commits")}
    assert set(rows) == {"c1", "c2", "c3", "c4"} and all(r["backfilled"] == 1 for r in rows.values())
    assert rows["c4"]["is_merge"] == 1 and rows["c2"]["author_key"] == "bob"
    ev = {e["dedupe_key"]: e for e in await db.fetchall("SELECT * FROM github_events")}
    assert set(ev) == {f"release:{RID}:v1.0", f"release:{RID}:v0.9", f"pr_merged:{RID}:5"}     # draft skipped
    assert ev[f"release:{RID}:v1.0"]["run_id"] == "live"                                     # not duplicated
    assert ev[f"release:{RID}:v0.9"]["backfilled"] == 1
    pr = json.loads(ev[f"pr_merged:{RID}:5"]["payload_json"])
    assert pr["backfilled"] is True and pr["review_count"] == 1 and pr["reviewers"] == ["carol"]
    assert ev[f"pr_merged:{RID}:5"]["backfilled"] == 1
    assert f"{P}/pulls/4/reviews" not in c.paths()                                          # merged before since
    daily = await db.fetchall("SELECT * FROM github_activity_daily")
    assert len(daily) == 4 and all(d["backfilled"] == 1 for d in daily)
    prog = json.loads((side / "github_progress.json").read_text())
    u = prog[str(RID)]
    assert u["status"] == "done" and u["commits"] == "done" and u["pages"] == 2 and u["path_form"] == "id"
    assert u["oldest_seen"] == NOW - 401 * DAY and u["truncated"] is False
    assert prog["_meta"]["rollup"]["status"] == "done" and prog["_meta"]["scope_repos"] == 1
    ad = await db.fetchall("SELECT * FROM github_authors_daily")
    assert ad and all(r["backfilled"] == 1 for r in ad)
    assert await db.fetchval("SELECT COUNT(*) FROM events WHERE event_type = 'backfill_done'") == 1
    # re-run: nothing pending, no duplicates, no requests
    n = len(c.calls)
    again = await gb.run_backfill(db, c, S(), run_id="bf2", soft_deadline=IN_WINDOW + 3600)
    assert len(c.calls) == n and "requests=0" in again.note
    assert await db.fetchval("SELECT COUNT(*) FROM github_commits") == 4
    assert await db.fetchval("SELECT COUNT(*) FROM github_events") == 3


@pytest.mark.asyncio
async def test_path_fallback_gone_and_truncated(db, side):
    await seed(db)
    await seed(db, rid=RID2, full="other/repo", role="org", netuid=65)
    r = {k.replace(P, "/repos/chutesai/chutes"): v for k, v in routes().items() if "page=2" not in k}
    r["/repos/chutesai/chutes/commits"] = R(200, [commit("c1", NOW - DAY)],
                                            headers={"link": '<https://x/next>; rel="next"'})
    c = client(db, r)
    out = await gb.run_backfill(db, c, S(BACKFILL_GITHUB_MAX_PAGES_PER_REPO=1), run_id="bf",
                                soft_deadline=IN_WINDOW + 3600)
    prog = json.loads((side / "github_progress.json").read_text())
    a, b = prog[str(RID)], prog[str(RID2)]
    assert a["status"] == "done" and a["path_form"] == "full_name" and a["truncated"] is True
    assert a["commits"] == "partial"                                                          # page cap hit
    assert "/repos/chutesai/chutes/pulls/5/reviews" in c.paths()                              # same path form
    assert b["status"] == "gone" and b["commits"] == "gone"                                   # 404 on both forms
    assert {f"{P2}/commits", "/repos/other/repo/commits"} <= set(c.paths())
    assert "gone=1" in out.note


# ── window, reserve, deadline ────────────────────────────────────────────────────────────────────────────────
def test_window_state():
    d = parse_iso("2026-10-17T00:00:00Z")
    assert gb.window_state(d + 3600, "00:30-08:00") == (True, d + 8 * 3600, None)
    assert gb.window_state(d + 12 * 3600, "00:30-08:00") == (False, None, d + DAY + 1800)
    assert gb.window_state(d + 600, "00:30-08:00") == (False, None, d + 1800)
    assert gb.window_state(d + 23 * 3600, "22:00-02:00") == (True, d + DAY + 2 * 3600, None)   # wraps midnight
    assert gb.window_state(d + 3600, "22:00-02:00") == (True, d + 2 * 3600, None)
    assert gb.window_state(d + 12 * 3600, "") == (True, None, None)


@pytest.mark.asyncio
async def test_outside_window_zero_requests_and_window_close(db, side):
    await seed(db)
    await seed(db, rid=RID2, full="other/repo", role="org", netuid=65)
    c = client(db, {**routes(), **routes(P2)}, t=NOW)                                        # 12:00Z
    out = await gb.run_backfill(db, c, S(), run_id="bf", soft_deadline=NOW + 3600)
    assert out.note.startswith("partial: outside window, 2 units left") and c.calls == []
    c2 = client(db, {**routes(), **routes(P2)}, t=parse_iso("2026-10-17T07:59:00Z"))
    orig = c2.get

    async def slow_get(*a, **k):                                                              # 30 s per request
        c2.t += 30
        return await orig(*a, **k)
    c2.get = slow_get
    out2 = await gb.run_backfill(db, c2, S(), run_id="bf2", soft_deadline=c2.t + 3600)
    assert out2.note.startswith("partial: outside window, 1 units left")                      # stopped at a repo edge
    prog = json.loads((side / "github_progress.json").read_text())
    assert sum(1 for k, v in prog.items() if k != "_meta" and v.get("status") == "done") == 1


@pytest.mark.asyncio
async def test_own_reserve_sleeps_to_reset_then_defers(db, side):
    await seed(db)
    c = client(db, routes())
    c.quota = GitHubQuota(clock=lambda: c.t, sleep=c._sleep)
    c.quota.remaining, c.quota.reset_at = 1000, int(c.t) + 600                                 # < 1,500, > 06's 300
    out = await gb.run_backfill(db, c, S(), run_id="bf", soft_deadline=c.t + 3600)
    assert c.quota.sleeps and c.t >= IN_WINDOW + 600 and "done=1" in out.note
    c3 = client(db, routes())
    c3.quota = GitHubQuota(clock=lambda: c3.t, sleep=c3._sleep)
    c3.quota.remaining, c3.quota.reset_at = 1000, int(c3.t) + 3000
    (side / "github_progress.json").unlink()
    out3 = await gb.run_backfill(db, c3, S(), run_id="bf3", soft_deadline=c3.t + 600)          # reset past deadline
    assert out3.note.startswith("partial: quota, 1 units left") and c3.calls == []


# ── scope and rollup ─────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_plan_repos_scope(db):
    okey = await seed(db)
    for rid, full, kw in ((11, "chutesai/fork", {"fork": True}), (12, "chutesai/old", {"pushed": SINCE - DAY}),
                          (13, "chutesai/arch", {"archived": True}), (14, "chutesai/live", {"pushed": NOW - 5 * DAY})):
        await upsert_repo_row(db, repo_fields(repo(rid, full, **kw)), method="direct", now=NOW, verified=True)
        await db.execute("INSERT INTO github_occupant_repos (occupant_key, repo_id, role, in_scope, first_seen_at, "
                         "last_seen_at) VALUES (?,?,'org',0,?,?)", (okey, rid, NOW, NOW))
    reps = await gb.plan_repos(db, since_ts=SINCE, max_per_occupant=30)
    assert {r["repo_id"] for r in reps} == {RID, 14}
    one = await gb.plan_repos(db, since_ts=SINCE, max_per_occupant=1)
    assert [r["repo_id"] for r in one] == [RID]                                               # declared first
    assert one[0]["occupants"] == [okey]


@pytest.mark.asyncio
async def test_rollup_stops_before_live_rows(db, side):
    okey = await seed(db)
    c = client(db, routes())
    live_day = parse_iso("2026-10-10T00:00:00Z")
    await db.execute("INSERT INTO github_authors_daily (occupant_key, author_key, day_ts, commits_today, cd28, cd56, "
                     "cd84_3, cd84, dev_class, repos_json, observed_at, run_id, backfilled) VALUES "
                     "(?, 'alice', ?, 0, 1, 0, 0, 1, 'one_time', '[]', ?, 'live', 0)", (okey, live_day, NOW))
    await gb.run_backfill(db, c, S(), run_id="bf", soft_deadline=IN_WINDOW + 3600)
    rows = await db.fetchall("SELECT * FROM github_authors_daily WHERE backfilled = 1")
    assert rows and max(r["day_ts"] for r in rows) < live_day
    assert await db.fetchval("SELECT run_id FROM github_authors_daily WHERE backfilled = 0") == "live"
    days_alice = {r["day_ts"] for r in rows if r["author_key"] == "alice"}
    d_c3 = (NOW - 400 * DAY) // DAY * DAY
    # 06's classifier counts commit-days in [D − 84 d, D): the author appears the day after, for 84 days
    assert d_c3 not in days_alice and d_c3 + DAY in days_alice and d_c3 + 84 * DAY in days_alice
    assert d_c3 + 85 * DAY not in days_alice
