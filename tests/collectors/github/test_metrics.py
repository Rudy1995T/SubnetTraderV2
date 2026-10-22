"""Spec 06 §2.7–§2.8 / §4 / §9: P2 metrics, absence-vs-failure encoding, first success wins, replay."""
import json

import pytest

from app.collectors.github import metrics as M
from app.collectors.github.models import CommitRow
from app.collectors.github.orgs import upsert_repo_row
from app.collectors.github.models import repo_fields
from app.util.time import floor_day, parse_iso
from tests.collectors.github.conftest import DAY, NOW, cand, metric_rows, repo, seed_occupant

D = floor_day(NOW)


def cm(sha, t, author="a", repo_id=1):
    return CommitRow(sha, repo_id, author, int(t))


def days_commits(author, n_days, start, prefix):
    return [cm(f"{prefix}{i}", start + i * DAY + 3600, author) for i in range(n_days)]


def test_electric_capital_classes():
    a = (days_commits("A", 12, D - 28 * DAY, "a1") + days_commits("A", 11, D - 56 * DAY, "a2")
         + days_commits("A", 10, D - 84 * DAY, "a3"))
    b = days_commits("B", 12, D - 28 * DAY, "b")
    c = [cm("c", D - 50 * DAY, "C")]
    cls = M.classify_authors(a + b + c, D, fte_days=10)
    assert {k: v.dev_class for k, v in cls.items()} == {"A": "full_time", "B": "part_time", "C": "one_time"}
    assert M.fte_devs_84d(cls) == 1 and M.active_authors_28d(cls) == 2
    assert (cls["A"].w1, cls["A"].w2, cls["A"].w3, cls["A"].cd84) == (12, 11, 10, 33)


def test_bus_factor_and_commit_days():
    def mk(counts):
        out = []
        for ai, n in enumerate(counts):
            out += [cm(f"{ai}-{i}", D - DAY - i * 60, f"au{ai}") for i in range(n)]
        return out
    assert M.bus_factor(mk([60, 30, 10]), D) == 1
    assert M.bus_factor(mk([40, 35, 25]), D) == 2
    assert M.bus_factor([], D) == 0
    assert M.commit_days_90d([cm("x", D - DAY), cm("y", D - DAY + 5), cm("z", D - 95 * DAY)], D) == 1


def test_review_practice_and_cadence():
    ev = [{"repo_id": 1, "event_kind": "pr_merged", "event_at": NOW - DAY, "payload": {"review_count": 1, "author_login": "x"}},
          {"repo_id": 1, "event_kind": "pr_merged", "event_at": NOW - DAY, "payload": {"review_count": 0, "author_login": "y"}},
          {"repo_id": 1, "event_kind": "pr_merged", "event_at": NOW - DAY,
           "payload": {"review_count": 0, "author_login": "dependabot[bot]"}}]
    assert M.review_practice_90d(ev, NOW) == (0.5, 2, 1)
    assert M.review_practice_90d([], NOW) == (None, 0, 0)
    rel = [{"repo_id": 1, "event_kind": "release", "event_at": NOW - DAY, "payload": {"tag_name": "v1"}},
           {"repo_id": 1, "event_kind": "tag", "event_at": NOW - DAY, "payload": {"tag_name": "v1"}},
           {"repo_id": 1, "event_kind": "tag", "event_at": None, "payload": {"tag_name": "v2"}},
           {"repo_id": 1, "event_kind": "release", "event_at": NOW - 100 * DAY, "payload": {"tag_name": "v0"}}]
    assert M.release_cadence_90d(rel, NOW) == 1


def test_hygiene_and_originality_rules():
    two = {"license": True, "readme_bytes": None, "ci": None, "contributing": None, "security": None, "dep_update": False}
    assert M.repo_hygiene(two, NOW - DAY, NOW).status == "failed"
    full = {"license": True, "readme_bytes": 600, "ci": False, "contributing": True, "security": None, "dep_update": False}
    h = M.repo_hygiene(full, NOW - DAY, NOW)
    assert h.status == "ok" and h.value == pytest.approx(3 / 5) and h.detail["items"]["security_or_contributing"] is True
    assert M.repo_hygiene(full, NOW - 15 * DAY, NOW).status == "failed"
    o = M.originality({"repo_files": 30, "identical_files": 20}, NOW - DAY, None, NOW)
    assert o.value == pytest.approx(1 / 3, abs=1e-5)
    assert M.originality(None, None, None, NOW).status == "failed"
    assert M.originality({"repo_files": 3, "identical_files": 0}, NOW - 46 * DAY, None, NOW).status == "failed"


def test_slot_for():
    assert M.slot_for(parse_iso("2026-10-17T23:30:00Z")) == parse_iso("2026-10-18T00:00:00Z")
    assert M.slot_for(parse_iso("2026-10-17T10:00:00Z")) == parse_iso("2026-10-17T12:00:00Z")
    assert M.slot_for(parse_iso("2026-10-17T12:00:00Z")) == parse_iso("2026-10-17T12:00:00Z")


# ── end-to-end over the DB ───────────────────────────────────────────────────────────────────────────────────
async def setup_resolved(db, *, netuid=64, rid=1, pushed=NOW - 3 * DAY, verified=NOW - 3600, activity_ok=NOW - 3600,
                         org_ok=NOW - 3600, archived=False, extra_repos=()):
    okey = await seed_occupant(db, netuid, name="Chutes", cands=[cand("chain", "https://github.com/chutesai/chutes")])
    f = repo_fields(repo(rid, "chutesai/chutes", pushed=pushed, archived=archived))
    await upsert_repo_row(db, f, method="direct", now=verified, verified=True)
    await db.execute("UPDATE github_repos SET activity_ok_at = ?, hygiene_json = ?, hygiene_checked_at = ?, "
                     "originality_json = ?, originality_checked_at = ? WHERE repo_id = ?",
                     (activity_ok, json.dumps({"license": True, "readme_bytes": 900, "ci": True, "contributing": True,
                                               "dep_update": True}), NOW - DAY,
                      json.dumps({"repo_files": 10, "identical_files": 2}), NOW - DAY, rid))
    for p in extra_repos:
        await upsert_repo_row(db, repo_fields(p), method="org_enum", now=verified, verified=True)
    await db.execute("INSERT INTO github_declared_refs (occupant_key, source, raw_value, kind, owner, repo, ref_key, "
                     "state, repo_id, first_seen_at, observed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                     (okey, "chain", "https://github.com/chutesai/chutes", "repo", "chutesai", "chutes",
                      "chutesai/chutes", "resolved", rid, NOW - 9 * DAY, NOW))
    await db.execute("INSERT INTO github_occupant_repos (occupant_key, repo_id, role, declared_source, in_scope, "
                     "first_seen_at, last_seen_at) VALUES (?,?,?,?,?,?,?)", (okey, rid, "declared", "chain", 1, NOW, NOW))
    await db.execute("INSERT INTO github_orgs (owner, kind, n_public, n_non_fork, first_seen_at, last_ok_at) VALUES "
                     "('chutesai','org',1,1,?,?)", (NOW, org_ok))
    return okey


async def add_commits(db, rid, rows):
    from app.collectors.github.models import CommitRow as C
    await db.insert_or_ignore("github_commits", [C(s, rid, a, t).db_row(observed_at=NOW - 3600, run_id="x") for s, a, t in rows])


@pytest.mark.asyncio
async def test_all_ten_ok_and_written_with_slot_key(db):
    okey = await setup_resolved(db)
    await add_commits(db, 1, [("s1", "alice", NOW - 2 * DAY), ("s2", "bob", NOW - 3 * DAY), ("s3", "alice", NOW - 40 * DAY)])
    out = await M.run_metrics(db, now=NOW, run_id="m1", token_state="ok")
    rows = await metric_rows(db, okey)
    p2 = {k: v for k, v in rows.items() if k in M.P2_KEYS}
    assert len(p2) == 10 and all(r["as_of"] == M.slot_for(NOW) for r in p2.values())
    st = {k: v["status"] for k, v in p2.items()}
    assert st["review_practice_90d"] == "not_applicable" and p2["review_practice_90d"]["value"] is None
    assert all(s == "ok" for k, s in st.items() if k != "review_practice_90d")
    assert p2["active_authors_28d"]["value"] == 2 and p2["commit_days_90d"]["value"] == 3 and p2["bus_factor"]["value"] == 1
    assert p2["declared_repo_freshness"]["value"] == pytest.approx(3.0)
    assert p2["repo_hygiene"]["value"] == 1.0 and p2["originality"]["value"] == pytest.approx(0.8)
    assert rows["repo_pushed_days"]["as_of"] == floor_day(NOW) and rows["repo_pushed_days"]["vj"]["def"] == "wb-1"
    assert all(isinstance(r["observed_at"], int) and isinstance(r["as_of"], int) for r in rows.values())
    assert out["authors_rows"] == 2
    ad = await db.fetchall("SELECT * FROM github_authors_daily WHERE occupant_key = ?", (okey,))
    assert {a["author_key"] for a in ad} == {"alice", "bob"} and all(a["day_ts"] == D for a in ad)
    again = await M.run_metrics(db, now=NOW - 60, run_id="m2", token_state="ok")    # same 12:00Z slot
    assert again["rows"] == 0                                         # first success wins


@pytest.mark.asyncio
async def test_placeholder_only_all_absent(db):
    okey = await seed_occupant(db, 39, cands=[cand("chain", "https://github.com/deprecated/deprecated", junk=True)],
                               status="junk_only")
    await M.run_metrics(db, now=NOW, run_id="m", token_state="ok")
    rows = await metric_rows(db, okey)
    for k in M.P2_KEYS:
        assert rows[k]["status"] == "absent" and rows[k]["vj"]["reason"] == "placeholder"
        assert rows[k]["value"] == (0.0 if k in M.COUNT_KEYS else None)
    assert rows["repo_pushed_days"]["status"] == "absent"


@pytest.mark.asyncio
async def test_no_ref_absent_and_unknown_failed(db):
    a = await seed_occupant(db, 10, status="absent")
    u = await seed_occupant(db, 11, status="unknown")
    await M.run_metrics(db, now=NOW, run_id="m", token_state="ok")
    ra, ru = await metric_rows(db, a), await metric_rows(db, u)
    assert ra["bus_factor"]["status"] == "absent" and ra["bus_factor"]["vj"]["reason"] == "no_github_url"
    assert ru["bus_factor"]["status"] == "failed" and ru["bus_factor"]["vj"]["reason"] == "never_observed"
    assert ru["bus_factor"]["value"] is None


@pytest.mark.asyncio
async def test_stale_activity_fails_activity_metrics_only(db):
    okey = await setup_resolved(db, activity_ok=NOW - 3 * DAY)
    await M.run_metrics(db, now=NOW, run_id="m", token_state="ok")
    rows = await metric_rows(db, okey)
    for k in M.ACTIVITY_KEYS:
        assert rows[k]["status"] == "failed" and rows[k]["vj"]["reason"] == "activity_stale"
    assert rows["declared_repo_freshness"]["status"] == "ok" and rows["org_freshness"]["status"] == "ok"


@pytest.mark.asyncio
async def test_failed_row_upgraded_by_later_success(db):
    okey = await setup_resolved(db, activity_ok=NOW - 3 * DAY)
    await M.run_metrics(db, now=NOW - 600, run_id="m1", token_state="ok")
    await db.execute("UPDATE github_repos SET activity_ok_at = ? WHERE repo_id = 1", (NOW - 300,))
    out = await M.run_metrics(db, now=NOW - 60, run_id="m2", token_state="ok")           # same slot
    rows = await metric_rows(db, okey)
    assert rows["bus_factor"]["status"] == "ok" and out["rows"] == len(M.ACTIVITY_KEYS)


@pytest.mark.asyncio
async def test_unverified_declared_repo_fails_freshness(db):
    okey = await setup_resolved(db, verified=NOW - 3 * DAY)
    await M.run_metrics(db, now=NOW, run_id="m", token_state="ok")
    r = (await metric_rows(db, okey))["declared_repo_freshness"]
    assert r["status"] == "failed" and r["vj"]["reason"] == "api_outage"


@pytest.mark.asyncio
async def test_all_archived_org(db):
    okey = await setup_resolved(db, archived=True)
    await M.run_metrics(db, now=NOW, run_id="m", token_state="ok")
    r = (await metric_rows(db, okey))["org_freshness"]
    assert r["status"] == "ok" and r["vj"]["all_archived"] is True and r["vj"]["declared_archived"] is True


@pytest.mark.asyncio
async def test_successor_when_declared_archived(db):
    okey = await setup_resolved(db, archived=True, extra_repos=[repo(2, "chutesai/chutes-v2", pushed=NOW - DAY)])
    await M.run_metrics(db, now=NOW, run_id="m", token_state="ok")
    r = (await metric_rows(db, okey))["org_freshness"]
    assert r["vj"]["successor_repo_id"] == 2 and r["vj"]["all_archived"] is False and r["value"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_gone_beyond_grace_without_primary_is_absent(db):
    okey = await setup_resolved(db)
    await db.execute("UPDATE github_declared_refs SET state = 'gone', first_404_at = ? WHERE occupant_key = ?",
                     (NOW - 15 * DAY, okey))
    await db.execute("UPDATE github_repos SET status = 'gone' WHERE repo_id = 1")
    await db.execute("UPDATE github_occupant_repos SET role = 'declared_gone', in_scope = 0 WHERE occupant_key = ?", (okey,))
    await M.run_metrics(db, now=NOW, run_id="m", token_state="ok")
    rows = await metric_rows(db, okey)
    assert rows["declared_repo_freshness"]["status"] == "absent" and rows["declared_repo_freshness"]["vj"]["reason"] == "repo_gone"
    assert rows["bus_factor"]["status"] == "absent" and rows["bus_factor"]["value"] == 0.0
    assert rows["repo_pushed_days"]["status"] == "failed" and rows["repo_pushed_days"]["vj"]["reason"] == "http_404"


@pytest.mark.asyncio
async def test_gone_within_grace_keeps_last_good_pushed(db):
    okey = await setup_resolved(db)
    await db.execute("UPDATE github_declared_refs SET state = 'gone_pending', first_404_at = ? WHERE occupant_key = ?",
                     (NOW - 2 * DAY, okey))
    await M.run_metrics(db, now=NOW, run_id="m", token_state="ok")
    r = (await metric_rows(db, okey))["declared_repo_freshness"]
    assert r["status"] == "ok" and r["vj"]["reason"] == "gone_grace" and r["value"] == pytest.approx(3.0)


@pytest.mark.asyncio
async def test_no_token_degraded(db):
    okey = await setup_resolved(db)
    await M.run_metrics(db, now=NOW, run_id="m", token_state="missing")
    rows = await metric_rows(db, okey)
    assert rows["bus_factor"]["vj"]["reason"] == "no_token" and rows["declared_repo_freshness"]["status"] == "ok"


@pytest.mark.asyncio
async def test_spam_event_and_bot_prs(db):
    okey = await setup_resolved(db)
    await add_commits(db, 1, [(f"b{i}", "spammer", NOW - DAY + i) for i in range(25)])
    out = await M.run_metrics(db, now=NOW, run_id="m", token_state="ok")
    assert out["spam_events"] == 1
    ev = await db.fetchone("SELECT * FROM events WHERE event_type = 'commit_spam_suspected'")
    p = json.loads(ev["payload"])
    assert p["human_commits_28d"] == 25 and p["flagged_28d"] == 24 and p["flags_histogram"]["burst"] == 24
    rows = await metric_rows(db, okey)
    assert rows["active_authors_28d"]["value"] == 1                   # the kept first commit of the burst


@pytest.mark.asyncio
async def test_replay_admits_backfilled_and_coverage_gate(db):
    okey = await setup_resolved(db)
    from app.collectors.github.models import CommitRow as C
    row = C("old", 1, "zed", NOW - 10 * DAY, backfilled=1).db_row(observed_at=NOW + 99 * DAY, run_id="bf")
    await db.insert_or_ignore("github_commits", [row])
    await db.execute("INSERT INTO github_repo_snapshots (repo_id, observed_at, pushed_at, archived, status, event_at, "
                     "source) VALUES (1, ?, ?, 0, 'ok', ?, 'github_api')", (NOW - DAY, NOW - 5 * DAY, NOW - 5 * DAY))
    calc = M.P2Metrics(db, token_state="ok")
    occ = (await __import__("app.registry", fromlist=["active_occupants"]).active_occupants(db))[0]
    res = await calc.compute(occ, NOW, replay=True, coverage=lambda rid, t: True)
    o = {x.metric_key: x for x in res.observations}
    assert o["active_authors_28d"].value == 1 and o["declared_repo_freshness"].value == pytest.approx(5.0)
    # 13 §2.5: org freshness in a replay is the last commit ≤ t (today's pushed_at would be look-ahead) …
    assert o["org_freshness"].value == pytest.approx(10.0) and o["org_freshness"].detail["approx"] == "last_commit"
    # … and so is the declared repo's when no PIT snapshot precedes t
    early = {x.metric_key: x for x in (await calc.compute(occ, NOW - 6 * DAY, replay=True,
                                                          coverage=lambda rid, t: True)).observations}
    assert early["declared_repo_freshness"].value == pytest.approx(4.0)
    assert (await calc.compute(occ, NOW - 11 * DAY, replay=True, coverage=lambda rid, t: True)).observations[
        [x.metric_key for x in res.observations].index("org_freshness")].reason == "no_pit_commit"
    res2 = await calc.compute(occ, NOW, replay=True, coverage=lambda rid, t: False)
    assert {x.metric_key: x for x in res2.observations}["bus_factor"].reason == "backfill_incomplete"
    rows = M.metric_rows(okey, res.observations, as_of=D, now=NOW, run_id="rp", backfilled=True)
    assert all(r["backfilled"] == 1 and r["value_json"]["backfilled"] is True for r in rows)
