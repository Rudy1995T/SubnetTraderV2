"""Spec 06 §2.3 / §9: resolver state machine on recorded-style fixtures (route-table client)."""
import json

import pytest

from app.collectors.github.resolver import Resolver, ranked_candidates, source_refs
from app.registry.models import Resolved
from tests.collectors.github.conftest import DAY, NOW, R, cand, repo, seed_occupant

CHUTES = 884216433
APEX = 741209532
pytestmark = []


async def run(db, client, now=NOW, netuids=None):
    client.t = float(now)
    res = Resolver(db, client, run_id=f"r{now}", now=now)
    return await res.run(netuids=netuids)


async def events(db, et=None):
    rows = await db.fetchall("SELECT * FROM events" + (" WHERE event_type = ?" if et else "") + " ORDER BY id",
                             (et,) if et else ())
    return [{**r, "p": json.loads(r["payload"])} for r in rows]


def chutes_routes(full="chutesai/chutes"):
    return {"/repos/chutesai/chutes": R(200, repo(CHUTES, full)),
            "/repos/rayonlabs/chutes": R(301, location=f"https://api.github.com/repositories/{CHUTES}"),
            f"/repositories/{CHUTES}": R(200, repo(CHUTES, full))}


@pytest.mark.asyncio
async def test_rayonlabs_chutes_301_moved(db, fake):
    okey = await seed_occupant(db, 64, name="Chutes", cands=[cand("chain", "https://github.com/rayonlabs/chutes")])
    c = fake(chutes_routes())
    rep = await run(db, c)
    ref = await db.fetchone("SELECT * FROM github_declared_refs WHERE occupant_key = ?", (okey,))
    assert ref["state"] == "resolved" and ref["repo_id"] == CHUTES and ref["resolution_method"] == "redirect"
    r = await db.fetchone("SELECT * FROM github_repos WHERE repo_id = ?", (CHUTES,))
    assert r["full_name"] == "chutesai/chutes" and r["resolution_method"] == "redirect"
    aliases = {a["full_name"] for a in await db.fetchall("SELECT full_name FROM github_repo_aliases")}
    assert aliases == {"chutesai/chutes", "rayonlabs/chutes"}
    ev = await events(db, "repo_moved")
    assert len(ev) == 1 and ev[0]["severity"] == "warning" and ev[0]["p"]["to_owner"] == "chutesai"
    assert rep.events.get("repo_moved") == 1
    assert c.paths()[:2] == ["/repos/rayonlabs/chutes", f"/repositories/{CHUTES}"]


@pytest.mark.asyncio
async def test_apex_rename_same_owner(db, fake):
    await seed_occupant(db, 1, name="Apex", cands=[cand("chain", "https://github.com/macrocosm-os/prompting")])
    c = fake({"/repos/macrocosm-os/prompting": R(301, location=f"https://api.github.com/repositories/{APEX}"),
              f"/repositories/{APEX}": R(200, repo(APEX, "macrocosm-os/apex"))})
    await run(db, c)
    ev = await events(db, "repo_renamed")
    assert len(ev) == 1 and ev[0]["p"]["from"] == "macrocosm-os/prompting" and ev[0]["p"]["to"] == "macrocosm-os/apex"
    assert ev[0]["severity"] == "info" and not await events(db, "repo_moved")


@pytest.mark.asyncio
async def test_placeholder_makes_no_http_call(db, fake):
    okey = await seed_occupant(db, 39, cands=[cand("chain", "https://github.com/username/repo")])
    c = fake({})
    await run(db, c)
    assert c.calls == []
    ref = await db.fetchone("SELECT * FROM github_declared_refs WHERE occupant_key = ?", (okey,))
    assert ref["state"] == "placeholder" and ref["is_placeholder"] == 1


@pytest.mark.asyncio
async def test_404_twice_gone_then_org_primary_by_netuid(db, fake):
    listing = [repo(11, "qbittensor-labs/quantum-compute", description="SN 48 quantum compute", pushed=NOW - 3 * DAY),
               repo(12, "qbittensor-labs/enigma", description="SN 63 enigma subnet", pushed=NOW - DAY),
               repo(13, "qbittensor-labs/old-fork", fork=True)]
    routes = {"/repos/qbittensor-labs/quantum": R(404), "/orgs/qbittensor-labs/repos": R(200, listing)}
    k48 = await seed_occupant(db, 48, name="Quantum", cands=[cand("chain", "https://github.com/qbittensor-labs/quantum")])
    k63 = await seed_occupant(db, 63, name="Enigma", cands=[cand("chain", "https://github.com/qbittensor-labs/quantum")])
    c = fake(routes)
    await run(db, c)
    ref = await db.fetchone("SELECT * FROM github_declared_refs WHERE occupant_key = ?", (k48,))
    assert ref["state"] == "gone_pending" and ref["first_404_at"] == NOW and not await events(db, "repo_gone")
    await run(db, c, now=NOW + 6 * 3600)
    ref = await db.fetchone("SELECT * FROM github_declared_refs WHERE occupant_key = ?", (k48,))
    assert ref["state"] == "gone" and ref["consecutive_404"] == 2 and ref["first_404_at"] == NOW
    ev = await events(db, "repo_gone")
    assert {e["occupant_key"] for e in ev} == {k48, k63}
    assert ev[0]["p"]["reason"] == "unresolved" and ev[0]["p"]["first_404_at"] == NOW
    prim = {r["occupant_key"]: r["repo_id"] for r in await db.fetchall(
        "SELECT occupant_key, repo_id FROM github_occupant_repos WHERE role = 'primary'")}
    assert prim == {k48: 11, k63: 12}
    assert ev[0]["p"]["fallback_primary_repo_id"] in (11, 12)
    scope = await db.fetchall("SELECT repo_id FROM github_occupant_repos WHERE occupant_key = ? AND in_scope = 1", (k48,))
    assert 13 not in {r["repo_id"] for r in scope}


@pytest.mark.asyncio
async def test_404_twice_in_same_cycle_window_is_not_confirmed(db, fake):
    okey = await seed_occupant(db, 5, cands=[cand("chain", "https://github.com/a/dead")])
    c = fake({"/repos/a/dead": R(404), "/orgs/a/repos": R(200, [])})
    await run(db, c)
    await run(db, c, now=NOW + 3600)
    ref = await db.fetchone("SELECT state FROM github_declared_refs WHERE occupant_key = ?", (okey,))
    assert ref["state"] == "gone_pending"


@pytest.mark.asyncio
async def test_reverify_by_id_rename_then_404(db, fake):
    okey = await seed_occupant(db, 64, cands=[cand("chain", "https://github.com/chutesai/chutes")])
    c = fake(chutes_routes())
    await run(db, c)
    assert not await events(db, "repo_renamed")
    c.routes[f"/repositories/{CHUTES}"] = R(200, repo(CHUTES, "chutesai/chutes-v2"))
    await run(db, c, now=NOW + 6 * 3600)
    assert c.paths()[-1] == f"/repositories/{CHUTES}"          # never by name once the id is known
    ev = await events(db, "repo_renamed")
    assert len(ev) == 1 and ev[0]["p"]["to"] == "chutesai/chutes-v2"
    assert (await db.fetchone("SELECT full_name FROM github_repos WHERE repo_id = ?", (CHUTES,)))["full_name"] == \
        "chutesai/chutes-v2"
    c.routes[f"/repositories/{CHUTES}"] = R(404)
    c.routes["/orgs/chutesai/repos"] = R(200, [])
    await run(db, c, now=NOW + 12 * 3600)
    ref = await db.fetchone("SELECT * FROM github_declared_refs WHERE occupant_key = ?", (okey,))
    assert ref["state"] == "gone_pending" and ref["repo_id"] == CHUTES


@pytest.mark.asyncio
async def test_restored_after_gone(db, fake):
    okey = await seed_occupant(db, 64, cands=[cand("chain", "https://github.com/chutesai/chutes")])
    c = fake(chutes_routes())
    await run(db, c)
    c.routes[f"/repositories/{CHUTES}"] = R(404)
    c.routes["/orgs/chutesai/repos"] = R(200, [])
    await run(db, c, now=NOW + 6 * 3600)
    await run(db, c, now=NOW + 12 * 3600)
    assert (await db.fetchone("SELECT status FROM github_repos WHERE repo_id = ?", (CHUTES,)))["status"] == "gone"
    c.routes[f"/repositories/{CHUTES}"] = R(200, repo(CHUTES, "chutesai/chutes"))
    await run(db, c, now=NOW + 18 * 3600)
    assert (await db.fetchone("SELECT state FROM github_declared_refs WHERE occupant_key = ?", (okey,)))["state"] == \
        "resolved"
    assert len(await events(db, "repo_restored")) == 1


@pytest.mark.asyncio
async def test_same_id_two_strings_no_disagreement(db, fake):
    await seed_occupant(db, 64, cands=[cand("chain", "https://github.com/chutesai/chutes"),
                                       cand("subnetalpha", "https://github.com/rayonlabs/chutes")])
    c = fake(chutes_routes())
    await run(db, c)
    assert not await events(db, "source_disagreement")
    n04 = await db.fetchval("SELECT COUNT(*) FROM identity_observations")
    assert n04 == 0                                             # nothing written to 04's tables


@pytest.mark.asyncio
async def test_netuid20_disagreement_by_id(db, fake):
    okey = await seed_occupant(db, 20, cands=[cand("chain", "https://github.com/witnessvision/witness_subnet"),
                                              cand("subnetalpha", "https://github.com/chronoseek/bittensor-subnet")])
    c = fake({"/repos/witnessvision/witness_subnet": R(200, repo(1360186689, "witnessvision/witness_subnet")),
              "/repos/chronoseek/bittensor-subnet": R(200, repo(1183382351, "chronoseek/bittensor-subnet")),
              "/orgs/witnessvision/repos": R(200, [])})
    await run(db, c)
    ev = await events(db, "source_disagreement")
    assert len(ev) == 1
    p = ev[0]["p"]
    assert ev[0]["severity"] == "info" and p["field"] == "github_repo" and p["resolved_source"] == "chain"
    assert p["repo_ids"] == [1183382351, 1360186689] and [g["key"] for g in p["groups"]] == ["1183382351", "1360186689"]
    import hashlib
    assert p["pair_hash"] == hashlib.sha256(b"1183382351|1360186689").hexdigest()[:16]
    assert ev[0]["dedupe_key"] == f"{okey}|github_repo|{p['pair_hash']}"
    assert not any("chronoseek" in path for path in c.paths() if path.startswith("/orgs"))
    await run(db, c, now=NOW + 6 * 3600)
    assert len(await events(db, "source_disagreement")) == 1        # dedupe


@pytest.mark.asyncio
async def test_sn117_dead_chain_repo_stays_declared_then_suppress(db, fake):
    chain = cand("chain", "https://github.com/everyframe-studios/everyframe-miner")
    sa = cand("subnetalpha", "https://github.com/glyph-research/glyph-subnet")
    okey = await seed_occupant(db, 117, cands=[chain, sa])
    c = fake({"/repos/everyframe-studios/everyframe-miner": R(404),
              "/repos/glyph-research/glyph-subnet": R(200, repo(555, "glyph-research/glyph-subnet")),
              "/repositories/555": R(200, repo(555, "glyph-research/glyph-subnet")),
              "/orgs/everyframe-studios/repos": R(404), "/users/everyframe-studios/repos": R(404)})
    await run(db, c)
    await run(db, c, now=NOW + 6 * 3600)
    rows = {r["source"]: r for r in await db.fetchall("SELECT * FROM github_declared_refs WHERE occupant_key = ?", (okey,))}
    assert rows["chain"]["state"] == "gone" and rows["subnetalpha"]["state"] == "resolved"
    assert len(await events(db, "repo_gone")) == 1 and not await events(db, "source_disagreement")
    decl = await db.fetchall("SELECT * FROM github_occupant_repos WHERE occupant_key = ? AND role IN ('declared','declared_gone')", (okey,))
    assert decl == []                                            # never resolved → no declared repo row
    assert (await db.fetchone("SELECT kind FROM github_orgs WHERE owner = 'everyframe-studios'"))["kind"] == "gone"
    # a 04 suppress on the chain value → subnetalpha becomes declared
    await seed_occupant(db, 117, cands=[{**chain, "suppressed": True}, sa], winner=sa)
    await run(db, c, now=NOW + 12 * 3600)
    d = await db.fetchone("SELECT * FROM github_occupant_repos WHERE occupant_key = ? AND role = 'declared'", (okey,))
    assert d["repo_id"] == 555 and d["declared_source"] == "subnetalpha"


@pytest.mark.asyncio
async def test_identity_repo_changed_only_on_id_change(db, fake):
    okey = await seed_occupant(db, 100, cands=[cand("chain", "https://github.com/CortexLM/platform")])
    c = fake({"/repos/CortexLM/platform": R(301, location="https://api.github.com/repositories/972956783"),
              "/repositories/972956783": R(200, repo(972956783, "cortexlm/cortex")),
              "/repos/CortexLM/cortex": R(200, repo(972956783, "cortexlm/cortex")),
              "/repos/newteam/other": R(200, repo(42, "newteam/other"))})
    await run(db, c)
    assert not await events(db, "identity_repo_changed")           # first sight is not a change
    await seed_occupant(db, 100, cands=[cand("chain", "https://github.com/CortexLM/cortex")])
    await run(db, c, now=NOW + 6 * 3600)
    assert not await events(db, "identity_repo_changed")           # same id 972956783
    await seed_occupant(db, 100, cands=[cand("chain", "https://github.com/newteam/other")])
    await run(db, c, now=NOW + 12 * 3600)
    ev = await events(db, "identity_repo_changed")
    assert len(ev) == 1 and ev[0]["p"]["from_repo_id"] == 972956783 and ev[0]["p"]["to_repo_id"] == 42
    assert ev[0]["occupant_key"] == okey


@pytest.mark.asyncio
async def test_owner_only_ref_enumerates_and_picks_primary(db, fake):
    okey = await seed_occupant(db, 105, name="Beam", cands=[cand("chain", "https://github.com/orgs/Beam-Network/repositories")])
    listing = [repo(1, "Beam-Network/beam", description="Beam subnet", pushed=NOW - 2 * DAY),
               repo(2, "Beam-Network/mls-go", fork=True, pushed=NOW),
               repo(3, "Beam-Network/docs", pushed=NOW - 40 * DAY)]
    c = fake({"/orgs/Beam-Network/repos": R(200, listing)})
    await run(db, c)
    ref = await db.fetchone("SELECT * FROM github_declared_refs WHERE occupant_key = ?", (okey,))
    assert ref["state"] == "resolved" and ref["repo_id"] == 1 and ref["resolution_method"] == "org_enum"
    roles = {r["repo_id"]: (r["role"], r["in_scope"]) for r in await db.fetchall(
        "SELECT * FROM github_occupant_repos WHERE occupant_key = ?", (okey,))}
    assert roles[1] == ("primary", 1) and roles[2][1] == 0 and roles[3] == ("org", 1)


@pytest.mark.asyncio
async def test_blocked_403(db, fake):
    okey = await seed_occupant(db, 7, cands=[cand("chain", "https://github.com/x/y")])
    c = fake({"/repos/x/y": R(403, {"message": "Repository access blocked"}), "/orgs/x/repos": R(200, [])})
    await run(db, c)
    ref = await db.fetchone("SELECT * FROM github_declared_refs WHERE occupant_key = ?", (okey,))
    assert ref["state"] == "blocked" and ref["first_404_at"] == NOW
    ev = await events(db, "repo_gone")
    assert len(ev) == 1 and ev[0]["p"]["reason"] == "blocked"


@pytest.mark.asyncio
async def test_failure_keeps_state_and_unexpected_location(db, fake):
    from app.collectors.github.client import GitHubUnavailable
    okey = await seed_occupant(db, 8, cands=[cand("chain", "https://github.com/x/y")])
    c = fake({"/repos/x/y": GitHubUnavailable("502")})
    rep = await run(db, c)
    ref = await db.fetchone("SELECT * FROM github_declared_refs WHERE occupant_key = ?", (okey,))
    assert ref["state"] == "new" and rep.failed == 1 and ref["last_attempt_at"] == NOW
    c.routes["/repos/x/y"] = R(301, location="https://github.com/elsewhere")
    await run(db, c, now=NOW + 6 * 3600)
    assert (await db.fetchone("SELECT state FROM github_declared_refs WHERE occupant_key = ?", (okey,)))["state"] == "new"


@pytest.mark.asyncio
async def test_netuid0_and_deadline(db, fake):
    await seed_occupant(db, 0, cands=[cand("chain", "https://github.com/opentensor/subtensor")])
    await seed_occupant(db, 9, cands=[cand("chain", "https://github.com/a/b")])
    c = fake({"/repos/a/b": R(200, repo(9, "a/b"))})
    c.t = NOW
    res = Resolver(db, c, run_id="r", now=NOW, deadline=NOW - 1)
    rep = await res.run()
    assert rep.left == 1 and c.calls == []
    assert await db.fetchval("SELECT COUNT(*) FROM github_declared_refs WHERE occupant_key = '0:1000'") == 0


def test_source_refs_order_and_mapping():
    cands = [cand("legacy", "https://github.com/a/old", override_id=369), cand("legacy", "https://github.com/a/old"),
             cand("tmc_coldkey", "https://github.com/a/tmc", field="owner_github"),
             cand("subnetalpha", "https://github.com/a/sa", demoted=True),
             cand("chain", "https://github.com/a/b, https://github.com/a"),
             cand("chain", "https://github.com/a/extra", field="github_repo__extra_0"),
             cand("subnetalpha", "https://github.com/deprecated/deprecated", junk=True)]
    res = Resolved("o", "github_repo", "https://github.com/a/b, https://github.com/a", None, "github_repo", "chain",
                   NOW, "present", cands)
    ranked = ranked_candidates(res)
    assert ranked[0]["source"] == "chain" and ranked[1]["field"] == "github_repo__extra_0"
    assert ranked[-1]["demoted"] or ranked[-1]["is_junk"]
    srefs, declared = source_refs(res)
    by = {s.source: s for s in srefs}
    assert declared == "chain" and by["chain"].ref.key == "a/b"
    assert set(by) == {"chain", "subnetalpha", "tmc", "seed_fundamentals"}
    assert by["subnetalpha"].ref.key == "a/sa" and not by["subnetalpha"].placeholder
    assert by["tmc"].ref.key == "a/tmc"


def test_source_refs_junk_only_and_suppressed():
    res = Resolved("o", "github_repo", None, None, None, None, None, "junk_only",
                   [cand("chain", "https://github.com/deprecated/deprecated", junk=True),
                    cand("subnetalpha", "https://github.com/a/b", suppressed=True)])
    srefs, declared = source_refs(res)
    assert declared is None and [s.source for s in srefs] == ["chain"] and srefs[0].placeholder
    assert source_refs(None) == ([], None)


@pytest.mark.asyncio
async def test_legacy_only_group_is_not_a_disagreement(db, fake):
    await seed_occupant(db, 29, cands=[cand("chain", "https://github.com/coldint/hotfloat"),
                                       cand("legacy", "https://github.com/coldint/coldint_validator")])
    c = fake({"/repos/coldint/hotfloat": R(200, repo(1, "coldint/hotfloat")),
              "/repos/coldint/coldint_validator": R(200, repo(2, "coldint/coldint_validator"))})
    await run(db, c)
    assert not await events(db, "source_disagreement")
    await seed_occupant(db, 29, cands=[cand("chain", "https://github.com/coldint/hotfloat"),
                                       cand("subnetalpha", "https://github.com/coldint/coldint_validator"),
                                       cand("legacy", "https://github.com/coldint/coldint_validator")])
    c.routes["/repositories/1"] = R(200, repo(1, "coldint/hotfloat"))
    c.routes["/repositories/2"] = R(200, repo(2, "coldint/coldint_validator"))
    await run(db, c, now=NOW + 6 * 3600)
    ev = await events(db, "source_disagreement")
    assert len(ev) == 1 and sorted(ev[0]["p"]["groups"][1]["sources"]) == ["legacy", "subnetalpha"]


@pytest.mark.asyncio
async def test_other_sources_owner_refs_share_canonical_listing(db, fake):
    okey = await seed_occupant(db, 105, name="Beam", cands=[
        cand("chain", "https://github.com/orgs/Beam-Network/repositories"),
        cand("subnetalpha", "https://github.com/Beam-Network"), cand("tmc_coldkey", "https://github.com/someone-else")])
    c = fake({"/orgs/Beam-Network/repos": R(200, [repo(1, "Beam-Network/beam", description="Beam subnet")])})
    await run(db, c)
    st = {r["source"]: (r["state"], r["repo_id"]) for r in await db.fetchall(
        "SELECT source, state, repo_id FROM github_declared_refs WHERE occupant_key = ?", (okey,))}
    assert st["chain"] == ("resolved", 1) and st["subnetalpha"] == ("resolved", 1)
    assert st["tmc"] == ("new", None)                                # another owner: never enumerated (§2.3)
    assert not any("someone-else" in p for p in c.paths())
