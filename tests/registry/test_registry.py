"""Spec 04 §9 (core subset): SCALE/keys, normaliser, occupant lifecycle, identity merge, overrides, legacy, PIT."""
import json
import subprocess

import pytest
import respx

import app.config as app_config
from app.jobs import SPEC_BY_ID
from app.registry import add_override, occupant_at, resolved
from app.registry.jobs import RegistryIdentityJob, RegistryOccupantsJob
from app.registry.legacy import import_fundamentals
from app.registry.normalise import classify, normalise_x_handle
from app.registry.sources.chain_rpc import (PREFIX_HEX, ScaleError, decode_identity_v3, encode_identity_v3,
                                            netuid_from_key, twox128)
from tests.registry.fakes import World, fast_limits, sa_rec, srow

T0 = 1_790_000_000


@pytest.fixture
def clock(monkeypatch):
    c = {"t": T0}
    import app.jobs as jobs_mod
    monkeypatch.setattr(jobs_mod, "now_ts", lambda: c["t"])
    return c


@pytest.fixture
def tweak(monkeypatch):
    def _t(**kw):
        monkeypatch.setattr(app_config, "settings", app_config.settings.model_copy(update=kw))
    return _t


@pytest.fixture
def world(rt, tweak):
    fast_limits(rt)
    tweak(REGISTRY_ENABLED=True, SUBNETALPHA_ENABLED=True, REGISTRY_SUBNETALPHA_MIN_RECORDS=1)
    w = World()
    w.rows = [srow(0, 0, None, active=True, feb="0"), srow(27, 1727132, "Nodexo", owner="ckA"),
              srow(40, 3372582, "Chunking"), srow(64, 4531295, "Chutes", owner="ck64"),
              srow(39, 3280104, "deprecated"), srow(106, 5558480, "Liquidity Provisioning", owner="ck106"),
              srow(112, 5633022, "for sale"), srow(117, 5710859, "everyframe.studio")]
    for n in range(200, 315):                  # ≥ 110 keys like the live chain (121), so the run is not "suspect"
        w.chain[n] = {"subnet_name": f"Filler{n}"}
    w.chain.update({
        27: {"subnet_name": "Nodexo", "logo_url": "https://raw.githubusercontent.com/troy12x/silx-web/main/o.png?"
             "token=GHSAT0AAAAAB"},
        40: {"subnet_name": "Chunking", "github_repo": "https://github.com/Inference-xyz/chunking"},
        64: {"subnet_name": "Chutes", "github_repo": "https://github.com/rayonlabs/chutes", "subnet_url": "https://chutes.ai",
             "discord": "https://discord.gg/chutes"},
        39: {"subnet_name": "deprecated", "github_repo": "https://github.com/deprecated/deprecated",
             "discord": "deprecated"},
        106: {"subnet_name": "Liquidity Provisioning", "discord": "https://discord.gg/fwjBwxUcdX",
              "subnet_contact": "https://discord.com/channels/799672011265015819/1174835090539433994",
              "additional": "API: https://nodexo.ai/api Docs: https://nodexo.ai/docs"},
        112: {"subnet_name": "for sale"},
        117: {"subnet_name": "everyframe.studio", "github_repo": "https://github.com/everyframe-studios/everyframe-miner"},
    })
    w.sa_records = [
        sa_rec(64, "Chutes", "2025-10-16T08:58:23", github_url="https://github.com/rayonlabs/chutes",
               discord_url="https://discord.gg/bittensor", twitterx_url="https://x.com/chutes_ai",
               video_url="https://www.youtube.com/watch?v=abc123DEF45"),
        sa_rec(27, "Nodexo", "2025-10-01T00:00:00", github_url="https://github.com/neuralinternet/SN27",
               twitterx_url="https://x.com/nodex0_"),
        sa_rec(40, "Chunking", "2025-10-01T00:00:00", github_url="https://github.com/Inference-xyz"),
        sa_rec(106, "Nodexo", "2026-08-27T00:00:00", discord_url="https://discord.gg/t7BMee4w"),
        sa_rec(117, "Glyph", "2026-08-01T00:00:00", github_url="https://github.com/glyph-research/glyph-subnet"),
    ]
    w.coldkeys = [{"coldkey": "ck64", "name": "Chutes Inc", "url": "https://chutes.ai", "github_repo": "", "image": "",
                   "discord": "", "description": "", "additional": ""},
                  {"coldkey": "ck106", "name": "Owner51", "url": "", "github_repo": "", "image": "",
                   "discord": "https://discord.gg/fwjBwxUcdX",
                   "description": "", "additional": ""}]
    return w


async def _run(rt, job_id):
    cls = RegistryOccupantsJob if job_id == "registry_occupants" else RegistryIdentityJob
    j = cls(rt, SPEC_BY_ID[job_id])
    await j.run("cli")
    return j


async def _events(rt, et=None):
    q = "SELECT * FROM events" + (" WHERE event_type = ?" if et else "") + " ORDER BY id"
    return await rt.db.fetchall(q, (et,) if et else (), writer=True)


# ── pure helpers ────────────────────────────────────────────────────────────────────────────────────────────
def test_twox128_and_keys():
    assert twox128("SubtensorModule").hex() == "658faa385070e074c85bf6b568cf0555"
    assert twox128("SubnetIdentitiesV3").hex() == "c7cb9786b286b680ca204a6c0920ee52"
    assert PREFIX_HEX == "0x658faa385070e074c85bf6b568cf0555c7cb9786b286b680ca204a6c0920ee52"
    assert netuid_from_key(PREFIX_HEX + "62cf9476b03dd865" + "4000") == 64          # Twox64Concat (spec)
    assert netuid_from_key(PREFIX_HEX + "0115159ebefcc55fb4b107904b05c7f4" + "7700") == 119   # Blake2_128 (live)
    for bad in (PREFIX_HEX + "00", "0x" + "11" * 50):
        with pytest.raises(ValueError):
            netuid_from_key(bad)


def test_scale_roundtrip():
    f = {"subnet_name": "Chutes", "github_repo": "https://github.com/chutesai/chutes", "description": "x" * 102,
         "additional": "y" * 141, "logo_url": "z" * 20000}
    enc = encode_identity_v3(f)
    assert enc.startswith("0x1843687574657388")
    out = decode_identity_v3(enc)
    assert out["subnet_name"] == "Chutes" and out["additional"] == "y" * 141 and len(out["logo_url"]) == 20000
    assert out["subnet_contact"] == ""
    with pytest.raises(ScaleError):
        decode_identity_v3(enc + "00")
    with pytest.raises(ScaleError):
        decode_identity_v3("0x18436875")


def test_normalise_cases():
    assert classify("github.com/orgs/Beam-Network/repositories", "github_repo")[0].kind == "github_owner"
    assert classify("https://github.com/a/b.git", "github_repo")[0].canonical == "https://github.com/a/b"
    assert classify("https://github.com/username/repo", "github_repo")[0].junk_reason == "placeholder_repo"
    assert classify("lium.io", "subnet_url")[0].canonical == "https://lium.io"
    assert classify("CreativeBuilds", "discord")[0].handle == "creativebuilds"
    d = classify("https://discord.com/invite/bittensor", "discord")[0]
    assert d.kind == "discord_invite" and d.extra["is_main_vanity"]
    assert classify("t.me/+ug7C1J7Apf8wNzc1", "telegram")[0].kind == "telegram_private"
    assert classify("for sale", "subnet_name")[0].junk_reason == "abandonment_marker"
    assert classify("Owner51", "owner_name")[0].is_junk
    assert classify("~", "subnet_url")[0].is_junk
    assert classify("", "github_repo") == []
    assert normalise_x_handle("https://mobile.twitter.com/foo/status/123") == "foo"
    assert normalise_x_handle("https://x.com/i/communities/1") is None


# ── occupant lifecycle ──────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_occupant_lifecycle(rt, world, clock):
    with respx.mock(assert_all_called=False) as m:
        world.install(m)
        await _run(rt, "registry_occupants")
        assert world.calls == {"subnets": 1}
        occ = {r["occupant_key"]: r for r in await rt.db.fetchall("SELECT * FROM occupants", writer=True)}
        assert len(occ) == 108 and occ["0:0"]["status"] == "active" and occ["64:4531295"]["name"] == "Chutes"
        assert await _events(rt) == []
        snap = await rt.db.fetchall("SELECT endpoint, params FROM chain_rest_snapshots", writer=True)
        assert snap == [{"endpoint": "/subnets/", "params": "limit=200"}]
        # run 2: same-registration owner change (27), re-registration (40), new pre-launch (108)
        world.rows = [r for r in world.rows if r["netuid"] not in (27, 40)] + [
            srow(27, 1727132, "Orion", owner="ckB", block=9_123_662),
            srow(40, 8409860, "Ralph", active=False, block=9_123_662), srow(108, 9111253, None, active=False,
                                                                            block=9_123_662)]
        for r in world.rows:
            r["latest_snapshot"]["block_number"] = 9_123_662
        clock["t"] += 3600
        await _run(rt, "registry_occupants")
        old = await rt.db.fetchone("SELECT * FROM occupants WHERE occupant_key = '40:3372582'", writer=True)
        new = await rt.db.fetchone("SELECT * FROM occupants WHERE occupant_key = '40:8409860'", writer=True)
        assert old["status"] == "deregistered" and old["dereg_reason"] == "reoccupied"
        assert old["last_seen_block"] == 8_400_000 and old["successor_key"] == "40:8409860"
        assert new["status"] == "pre_launch" and new["predecessor_key"] == "40:3372582" and new["registered_ts"] is None
        ev = await _events(rt, "netuid_reoccupied")
        assert len(ev) == 1 and ev[0]["severity"] == "warning" and ev[0]["source"] == "registry"
        p = json.loads(ev[0]["payload"])
        assert p["old_name"] == "Chunking" and p["new_name"] == "Ralph"
        assert await rt.db.fetchval("SELECT occupant_key FROM subnets WHERE netuid = 27", writer=True) == "27:1727132"
        assert (await occupant_at(rt.db, 40, 8_400_000)).occupant_key == "40:3372582"
        assert (await occupant_at(rt.db, 40, 8_420_000)).occupant_key == "40:8409860"
        # run 3: activation
        world.rows = [r for r in world.rows if r["netuid"] != 40] + [srow(40, 8409860, "Ralph", feb="8420000",
                                                                          block=9_123_700)]
        for r in world.rows:
            r["latest_snapshot"]["block_number"] = 9_123_700
        clock["t"] += 3600
        await _run(rt, "registry_occupants")
        new = await rt.db.fetchone("SELECT * FROM occupants WHERE occupant_key = '40:8409860'", writer=True)
        assert new["status"] == "active" and new["first_emission_block"] == 8420000
        assert len(await _events(rt, "occupant_activated")) == 1


@pytest.mark.asyncio
async def test_missing_and_backwards(rt, world, clock):
    with respx.mock(assert_all_called=False) as m:
        world.install(m)
        await _run(rt, "registry_occupants")
        world.rows = [r for r in world.rows if r["netuid"] != 39]
        world.rows = [srow(64, 4000000, "Chutes") if r["netuid"] == 64 else r for r in world.rows]
        for i in range(3):
            clock["t"] += 3600
            await _run(rt, "registry_occupants")
            row = await rt.db.fetchone("SELECT * FROM occupants WHERE occupant_key = '39:3280104'", writer=True)
            if i < 2:
                assert row["status"] == "active" and row["missing_runs"] == i + 1
        assert row["status"] == "deregistered" and row["dereg_reason"] == "absent"
        an = [json.loads(e["payload"]) for e in await _events(rt, "registry_anomaly")]
        assert sorted(a["reason"] for a in an) == ["netuid_absent", "reg_block_went_backwards"]
        assert await rt.db.fetchval("SELECT occupant_key FROM subnets WHERE netuid = 64", writer=True) == "64:4531295"
        assert await _events(rt, "netuid_reoccupied") == []


@pytest.mark.asyncio
async def test_insane_run_and_table_fallback(rt, world, clock):
    with respx.mock(assert_all_called=False) as m:
        world.install(m)
        world.subnets_status = 503
        j = await _run(rt, "registry_occupants")
        assert j._status.consecutive_failures == 1 and "sanity" in j._status.last_error
        assert await rt.db.fetchval("SELECT COUNT(*) FROM occupants", writer=True) == 0
        assert await rt.db.fetchval("SELECT COUNT(*) FROM chain_rest_snapshots", writer=True) == 0
        world.table_ok = True
        clock["t"] += 60
        await j.run("cli")
        assert j._status.consecutive_failures == 0
        assert await rt.db.fetchval("SELECT endpoint FROM chain_rest_snapshots", writer=True) == "/subnets/table/"


# ── identity ────────────────────────────────────────────────────────────────────────────────────────────────
async def _res(rt, key, field):
    return await rt.db.fetchone("SELECT * FROM identity_resolved WHERE occupant_key = ? AND field = ?", (key, field),
                                writer=True)


@pytest.mark.asyncio
async def test_identity_merge_end_to_end(rt, world, clock):
    with respx.mock(assert_all_called=False) as m:
        world.install(m)
        await _run(rt, "registry_occupants")
        j = await _run(rt, "registry_identity")
        assert j._status.last_error is None, j._status.last_error
        assert world.calls["rpc"] == 5 and world.calls["coldkey"] == 1
        src = j._last["sources"]
        assert src["chain"]["ok"] and src["chain"]["via"] == "rpc" and src["subnetalpha"]["ok"] and src["tmc_coldkey"]["ok"]
        r = await _res(rt, "64:4531295", "github_repo")
        assert r["status"] == "present" and r["value_norm"] == "https://github.com/rayonlabs/chutes" and r["source"] == "chain"
        # SN106 discord: chain fwjBwxUcdX vs subnetalpha t7BMee4w → one disagreement; SN64 vanity excluded
        dis = [json.loads(e["payload"]) for e in await _events(rt, "source_disagreement")]
        disc = [d for d in dis if d["field"] == "discord"]
        assert [(d["netuid"], d["resolved_source"]) for d in disc] == [(106, "chain")]
        assert not [d for d in dis if d["field"] == "github_repo"]
        # reclassified contact + docs + extras
        assert (await _res(rt, "106:5558480", "docs_url"))["value_norm"] == "https://nodexo.ai/docs"
        extra = await rt.db.fetchone("SELECT * FROM identity_observations WHERE occupant_key = '106:5558480' AND "
                                     "field = 'discord__extra_0'", writer=True)
        assert extra["reclassified_from"] == "subnet_contact" and extra["ref_kind"] == "discord_channel"
        # junk / abandonment / leaked secret
        assert (await _res(rt, "39:3280104", "github_repo"))["status"] == "junk_only"
        assert (await _res(rt, "112:5633022", "subnet_name"))["status"] == "present"
        junk = [json.loads(e["payload"]) | {"sev": e["severity"]} for e in await _events(rt, "identity_junk")]
        assert any(x["junk_reason"] == "abandonment_marker" and x["sev"] == "warning" and x["value"] == "for sale"
                   for x in junk)
        leak = [x for x in junk if x["junk_reason"] == "leaked_secret"]
        assert leak and "<redacted>" in leak[0]["value"] and "GHSAT" not in leak[0]["value"]
        # owner placeholder → junk_only; stale editorial for 40 (subnetalpha post predates the new occupant)
        assert (await _res(rt, "106:5558480", "owner_name"))["status"] == "junk_only"
        # watch channels
        wc = {(r["platform"], r["handle"]): r for r in await rt.db.fetchall("SELECT * FROM watch_channels", writer=True)}
        assert wc[("discord_invite", "bittensor")]["is_main_guild"] == 1
        assert wc[("discord_invite", "fwjBwxUcdX")]["priority"] == 10
        assert json.loads(wc[("discord_invite", "fwjBwxUcdX")]["meta_json"])["declared_by"] == ["chain", "tmc_coldkey"]
        assert wc[("x", "chutes_ai")]["netuid"] == 64
        assert wc[("youtube_video", "abc123DEF45")]["needs_resolution"] == 1
        assert wc[("discord_channel", "1174835090539433994")]["guild_id"] == "799672011265015819"
        # rename on chain → warning identity_changed with resolved_changed
        world.chain[64] = dict(world.chain[64], github_repo="https://github.com/chutesai/chutes")
        clock["t"] += 86400
        await _run(rt, "registry_occupants")
        await _run(rt, "registry_identity")
        ch = [e for e in await _events(rt, "identity_changed") if json.loads(e["payload"])["field"] == "github_repo"]
        assert len(ch) == 1 and ch[0]["severity"] == "warning"
        p = json.loads(ch[0]["payload"])
        assert p["old_value"] == "https://github.com/rayonlabs/chutes" and p["resolved_changed"] is True
        r = await _res(rt, "64:4531295", "github_repo")
        assert r["value_norm"] == "https://github.com/chutesai/chutes" and "github_string_mismatch" in json.loads(r["flags_json"])
        # identical third run: no new events
        n = len(await _events(rt))
        clock["t"] += 3600
        await _run(rt, "registry_identity")
        assert len(await _events(rt)) == n


@pytest.mark.asyncio
async def test_owner_change_and_stale_editorial(rt, world, clock):
    with respx.mock(assert_all_called=False) as m:
        world.install(m)
        await _run(rt, "registry_occupants")
        await _run(rt, "registry_identity")
        world.rows = [r for r in world.rows if r["netuid"] not in (27, 40)] + [
            srow(27, 1727132, "Orion", owner="ckB"), srow(40, 8409860, "Ralph", feb="8420000")]
        for r in world.rows:
            r["latest_snapshot"]["block_number"] = 9_123_662
        world.chain[27] = dict(world.chain[27], subnet_name="Orion")
        world.chain[40] = {"subnet_name": "Ralph", "github_repo": "https://github.com/RalphLabsAI/ralph"}
        world.chain_block = 9_123_662
        clock["t"] += 86400
        await _run(rt, "registry_occupants")
        await _run(rt, "registry_identity")
        assert await _events(rt, "netuid_reoccupied") != []
        ch = {(json.loads(e["payload"])["source"], json.loads(e["payload"])["field"]): e
              for e in await _events(rt, "identity_changed") if e["netuid"] == 27}
        assert ch[("chain", "subnet_name")]["severity"] == "warning"
        assert ch[("tmc", "owner_coldkey")]["severity"] == "info"
        r = await _res(rt, "40:8409860", "github_repo")
        assert r["value_norm"] == "https://github.com/RalphLabsAI/ralph" and r["source"] == "chain"
        cands = json.loads(r["candidates_json"])
        assert any(c["source"] == "subnetalpha" and c["demoted"] for c in cands)
        x = await _res(rt, "27:1727132", "x_url")
        assert x["value_norm"] == "https://x.com/nodex0_" and "stale_editorial" in json.loads(x["flags_json"])


@pytest.mark.asyncio
async def test_absence_unknown_and_fallback(rt, world, clock, tweak):
    with respx.mock(assert_all_called=False) as m:
        world.install(m)
        await _run(rt, "registry_occupants")
        tweak(SUBNETALPHA_ENABLED=False)
        world.chain_status = 503
        j = await _run(rt, "registry_identity")
        assert j._last["sources"]["chain"]["via"] == "tmc_snapshot"
        obs = await rt.db.fetchone("SELECT * FROM identity_observations WHERE source = 'chain' AND "
                                   "occupant_key = '64:4531295' AND field = 'subnet_name'", writer=True)
        assert obs["source_ref"] == "tmc_snapshot"
        # 112 has no github anywhere: chain (snapshot) covered, coldkey covered (no row) → absent
        assert (await _res(rt, "112:5633022", "github_repo"))["status"] == "absent"
        tweak(SUBNETALPHA_ENABLED=True)
        world.sa_status = 503
        for _ in range(3):
            clock["t"] += 3600
            j = await _run(rt, "registry_identity")
            assert j._status.last_error is None
        an = [json.loads(e["payload"]) for e in await _events(rt, "registry_anomaly")]
        failed = [a for a in an if a["reason"] == "source_failed"]
        assert failed and {a["detail"]["source"] for a in failed} >= {"subnetalpha"}
        # x_url: subnetalpha never covered → unknown
        assert (await _res(rt, "64:4531295", "x_url"))["status"] == "unknown"


@pytest.mark.asyncio
async def test_overrides(rt, world, clock):
    with respx.mock(assert_all_called=False) as m:
        world.install(m)
        await _run(rt, "registry_occupants")
        await _run(rt, "registry_identity")
    with pytest.raises(ValueError):
        await add_override(rt.db, occupant_key="64:4531295", field="github_repo", mode="force",
                           value="https://github.com/x/y", reason=None, evidence_url=None, created_by="t")
    await add_override(rt.db, occupant_key="64:4531295", field="github_repo", mode="force",
                       value="https://github.com/x/y", reason="op", evidence_url="https://e", created_by="t")
    r = await _res(rt, "64:4531295", "github_repo")
    assert r["value_norm"] == "https://github.com/x/y" and r["source"] == "manual" and r["override_id"]
    await add_override(rt.db, occupant_key="117:5710859", field="github_repo", mode="suppress", suppress_source="chain",
                       suppress_value="https://github.com/everyframe-studios/everyframe-miner", reason="404",
                       evidence_url="https://e", created_by="t")
    r = await _res(rt, "117:5710859", "github_repo")
    assert r["value_norm"] == "https://github.com/glyph-research/glyph-subnet" and r["source"] == "subnetalpha"


@pytest.mark.asyncio
async def test_legacy_import_and_pit(rt, world, clock, tmp_path):
    with respx.mock(assert_all_called=False) as m:
        world.install(m)
        await _run(rt, "registry_occupants")
        await _run(rt, "registry_identity")
        t1 = clock["t"]
        world.chain[64] = dict(world.chain[64], github_repo="https://github.com/chutesai/chutes")
        clock["t"] += 86400
        await _run(rt, "registry_identity")
    r = await resolved(rt.db, "64:4531295", "github_repo", as_of=t1 + 10)
    assert r.value_norm == "https://github.com/rayonlabs/chutes"
    assert await resolved(rt.db, "64:4531295", "github_repo", as_of=t1 - 10) is None
    assert (await resolved(rt.db, "64:4531295", "github_repo")).value_norm == "https://github.com/chutesai/chutes"
    # make 40's current occupant post-date the legacy snapshot
    await rt.db.execute("UPDATE occupants SET registered_at_est = ? WHERE occupant_key = '40:3372582'", (1_790_000_000,))
    fund = {"schema_version": 2, "subnets": {
        "27": {"name": "Something Else", "github": "https://github.com/neuralinternet/SN27"},
        "40": {"name": "Chunking", "github": "https://github.com/Inference-xyz"},
        "64": {"name": "Chutes", "github": "https://github.com/rayonlabs/chutes", "website": "https://chutes.ai"},
        "117": {"name": "everyframe", "website": "~", "github": "https://github.com/username/repo"}}}
    f = tmp_path / "fundamentals.json"
    f.write_text(json.dumps(fund))
    as_of = 1_780_398_221
    rep = await import_fundamentals(rt.db, f, as_of=as_of, run_id="seed", occupancy=None)
    assert rep.skipped_reoccupied == 1 and rep.per_netuid[40] == "skipped_reoccupied"
    assert rep.per_netuid[27] == "needs_review" and rep.per_netuid[64] == "active" and rep.junk == 2
    ov = await rt.db.fetchall("SELECT * FROM manual_overrides WHERE occupant_key = '64:4531295' AND source = 'legacy'",
                              writer=True)
    assert ov and all(o["mode"] == "fallback" and o["source_ref"] == "fundamentals.json@2026-06-02" for o in ov)
    lo = await rt.db.fetchall("SELECT observed_at FROM identity_observations WHERE source = 'legacy'", writer=True)
    assert lo and all(r["observed_at"] == as_of for r in lo)
    n = len(lo)
    rep2 = await import_fundamentals(rt.db, f, as_of=as_of, run_id="seed2", occupancy=None)
    assert rep2.observations == 0
    assert await rt.db.fetchval("SELECT COUNT(*) FROM identity_observations WHERE source = 'legacy'", writer=True) == n


def test_no_send_alert_in_registry():
    import pathlib
    root = pathlib.Path(app_config.__file__).resolve().parent / "registry"
    assert not [p for p in root.rglob("*.py") if "send_alert" in p.read_text()]
