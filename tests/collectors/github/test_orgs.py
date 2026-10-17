"""Spec 06 §2.4 / §9: org enumeration, primary selection, archived flips, scope."""
import json

import pytest

from app.collectors.github.models import RepoRow
from app.collectors.github.orgs import (enumerate_owner, is_subnet_related, pick_primary, rebuild_scope,
                                        store_listing)
from tests.collectors.github.conftest import DAY, NOW, R, cand, repo, seed_occupant


def rr(rid, name, *, pushed=NOW - DAY, fork=False, archived=False, desc=None, stars=0):
    return RepoRow(rid, name.lower(), name.split("/")[0].lower(), name.split("/")[1], fork, archived, pushed, None,
                   stars, desc)


def test_pick_primary_netuid_bonus_qbittensor():
    repos = [rr(1, "qbittensor-labs/quantum-compute", desc="SN 48 compute", pushed=NOW - 10 * DAY),
             rr(2, "qbittensor-labs/enigma", desc="SN 63 enigma", pushed=NOW - DAY)]
    assert pick_primary(repos, 48, "Quantum", NOW).repo_id == 1
    assert pick_primary(repos, 63, "Enigma", NOW).repo_id == 2


def test_pick_primary_excludes_forks_archived_and_ties_deterministic():
    repos = [rr(5, "o/a", fork=True, pushed=NOW), rr(6, "o/b", archived=True, pushed=NOW), rr(7, "o/c"), rr(8, "o/d")]
    assert pick_primary(repos, 1, None, NOW).repo_id == 7            # same score + pushed: lower id wins (-repo_id)
    assert pick_primary([rr(5, "o/a", fork=True)], 1, None, NOW) is None
    assert pick_primary([rr(9, "o/x", pushed=None)], 1, None, NOW).repo_id == 9


def test_pick_primary_beam():
    repos = [rr(1, "Beam-Network/beam", desc="Beam subnet node", pushed=NOW - 5 * DAY),
             rr(2, "Beam-Network/mls-go", fork=True, pushed=NOW), rr(3, "Beam-Network/site", pushed=NOW - DAY)]
    assert pick_primary(repos, 105, "Beam", NOW).repo_id == 1


def test_is_subnet_related():
    assert is_subnet_related("ditto-screener", "Bittensor subnet screener")
    assert is_subnet_related("sn41-tools", None)
    assert is_subnet_related("chutes-api", None, "Chutes")
    assert not is_subnet_related("dotfiles", "my vim config", "Chutes")


@pytest.mark.asyncio
async def test_orgs_404_falls_back_to_users_and_both_gone(fake):
    c = fake({"/orgs/alice/repos": R(404), "/users/alice/repos": R(200, [repo(1, "alice/x", owner_type="User")])})
    lst = await enumerate_owner(c, "alice")
    assert lst.kind == "user" and len(lst.repos) == 1
    c2 = fake({})
    lst2 = await enumerate_owner(c2, "ghost")
    assert lst2.kind == "gone" and lst2.ok
    c3 = fake({"/users/alice/repos": R(200, [])})
    await enumerate_owner(c3, "alice", known_kind="user")
    assert c3.paths() == ["/users/alice/repos"]                      # no daily /orgs 404 for a known user


@pytest.mark.asyncio
async def test_pagination_by_page_number(fake):
    page1 = [repo(i, f"big/r{i}") for i in range(1, 101)]
    c = fake({"/orgs/big/repos": R(200, page1), "/orgs/big/repos#page2": R(200, [repo(101, "big/r101")])})
    lst = await enumerate_owner(c, "big")
    assert len(lst.repos) == 101 and c.calls[1][1]["page"] == 2


@pytest.mark.asyncio
async def test_chutesai_listing_counts_and_scope(db, fake):
    okey = await seed_occupant(db, 64, name="Chutes", cands=[cand("chain", "https://github.com/chutesai/chutes")])
    listing = [repo(10, "chutesai/sek8s", pushed=NOW - 2 * DAY), repo(11, "chutesai/tdx-measure", fork=True, pushed=NOW),
               repo(12, "chutesai/chutes-audit", pushed=NOW - 5 * DAY), repo(884216433, "chutesai/chutes", pushed=NOW - 80 * DAY),
               repo(13, "chutesai/old", archived=True, pushed=NOW - 3 * DAY)]
    from app.collectors.github.resolver import Resolver
    c = fake({"/repos/chutesai/chutes": R(200, listing[3]), "/orgs/chutesai/repos": R(200, listing)})
    await Resolver(db, c, run_id="r", now=NOW).run()
    lst = await enumerate_owner(c, "chutesai")
    await store_listing(db, lst, now=NOW, run_id="r")
    org = await db.fetchone("SELECT * FROM github_orgs WHERE owner = 'chutesai'")
    assert org["n_public"] == 5 and org["n_non_fork"] == 4 and org["n_archived"] == 1
    assert org["max_pushed_at_non_fork"] == NOW - 2 * DAY and org["kind"] == "org"
    sc = await rebuild_scope(db, okey, netuid=64, name="Chutes", declared_source="chain", now=NOW)
    assert sc.declared_alive and sc.repo_ids[0] == 884216433
    assert 11 not in sc.repo_ids and 13 not in sc.repo_ids and {10, 12} <= set(sc.repo_ids)
    first = await db.fetchval("SELECT resolution_method FROM github_repos WHERE repo_id = 884216433")
    assert first == "direct"                                         # first contact kept, not 'org_enum'


@pytest.mark.asyncio
async def test_archived_flips_need_prior_observation(db, fake):
    a = [repo(1, "ditto-assistant/ditto-platform"), repo(2, "ditto-assistant/ditto-screener"),
         repo(3, "ditto-assistant/dittobench-starter-kit")]
    c = fake({"/orgs/ditto-assistant/repos": R(200, a)})
    st = await store_listing(db, await enumerate_owner(c, "ditto-assistant"), now=NOW, run_id="r")
    assert st["flips"] == []
    b = [{**x, "archived": True} for x in a]
    c.routes["/orgs/ditto-assistant/repos"] = R(200, b)
    st2 = await store_listing(db, await enumerate_owner(c, "ditto-assistant"), now=NOW + DAY, run_id="r2")
    assert sorted(f["repo_id"] for f in st2["flips"]) == [1, 2, 3]
    snaps = await db.fetchval("SELECT COUNT(*) FROM github_repo_snapshots")
    assert snaps == 6                                                 # changed → a new snapshot each


@pytest.mark.asyncio
async def test_org_repo_archived_events_from_job(db, fake, rt):
    from app.collectors.github import client as client_mod
    from app.collectors.github.jobs import GithubOrgEnumJob
    from app.jobs import SPEC_BY_ID, RunContext
    import app.config as app_config
    okey = await seed_occupant(rt.db, 118, name="Ditto", cands=[cand("chain", "https://github.com/orgs/ditto-assistant/repositories")])
    a = [repo(1, "ditto-assistant/ditto-platform", description="bittensor subnet platform"), repo(2, "ditto-assistant/blog")]
    c = fake({"/orgs/ditto-assistant/repos": R(200, a)}, db=rt.db)
    client_mod._clients[id(rt)] = c
    c.http = rt.http
    c.db = rt.db
    s = app_config.Settings(_env_file=None, GITHUB_TOKEN="t")
    job = GithubOrgEnumJob(rt, SPEC_BY_ID["github_org_enum"])
    ctx = RunContext("run1", int(NOW), "test", rt, s, None)
    import app.config
    old = app.config.settings
    app.config.settings = s
    try:
        from app.collectors.github.resolver import Resolver
        await Resolver(rt.db, c, run_id="r0", now=NOW).run()          # resolve runs first (boot stagger)
        out = await job.collect(ctx)
        assert out.meta["org_repo_archived"] == 0
        c.routes["/orgs/ditto-assistant/repos"] = R(200, [{**x, "archived": True} for x in a])
        ctx2 = RunContext("run2", int(NOW + DAY), "test", rt, s, None)
        c.t = NOW + DAY
        out2 = await job.collect(ctx2)
    finally:
        app.config.settings = old
        client_mod._clients.pop(id(rt), None)
    ev = await rt.db.fetchall("SELECT payload FROM events WHERE event_type = 'org_repo_archived'")
    ps = sorted((json.loads(e["payload"]) for e in ev), key=lambda p: p["repo_id"])
    assert [p["repo_id"] for p in ps] == [1, 2] and ps[0]["is_subnet_related"] and not ps[1]["is_subnet_related"]
    assert out2.meta["org_repo_archived"] == 2 and out.rows == 1
    # every repo archived → no primary any more; the owner-only ref is org_empty (absence clock starts)
    prim = await rt.db.fetchval("SELECT repo_id FROM github_occupant_repos WHERE occupant_key = ? AND role = 'primary'", (okey,))
    ref = await rt.db.fetchone("SELECT state, first_404_at FROM github_declared_refs WHERE occupant_key = ?", (okey,))
    assert prim is None and ref["state"] == "org_empty" and ref["first_404_at"] == NOW + DAY


@pytest.mark.asyncio
async def test_user_owner_scope_keeps_only_subnet_repos(db, fake):
    okey = await seed_occupant(db, 123, name="Mantis", cands=[cand("chain", "https://github.com/Barbariandev/MANTIS")])
    listing = [repo(1, "Barbariandev/MANTIS", owner_type="User"),
               repo(2, "Barbariandev/dotfiles", owner_type="User", pushed=NOW),
               repo(3, "Barbariandev/mantis-validator", owner_type="User", pushed=NOW - DAY)]
    from app.collectors.github.resolver import Resolver
    c = fake({"/repos/Barbariandev/MANTIS": R(200, listing[0]), "/repositories/1": R(200, listing[0]),
              "/orgs/barbariandev/repos": R(404), "/users/barbariandev/repos": R(200, listing)})
    await Resolver(db, c, run_id="r", now=NOW).run()
    await store_listing(db, await enumerate_owner(c, "barbariandev"), now=NOW, run_id="r")
    sc = await rebuild_scope(db, okey, netuid=123, name="Mantis", declared_source="chain", now=NOW)
    assert sc.owner_kind == "user" and set(sc.repo_ids) == {1, 3}
