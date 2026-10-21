"""Spec 09 §9.1 test_x_collector: since_id, attribution (§2.6), the upsert rule, error policy (§7), pauses, the
resolution step, profile refresh, KOL polling, routing re-sync and legacy-only rows (§2.4)."""
from __future__ import annotations

import json

import pytest

import app.collectors.x as xmod
from app.collectors.x import XKolPollJob, XProfileRefreshJob, XTeamPollJob, load_kol_seed
from app.collectors.x_providers.base import XProviderError
from app.jobs import SPEC_BY_ID
from app.nlp.textnorm import text_hash
from tests.collectors.x.conftest import (DAY, NOW, FakeProvider, make_collector, post, seed_account, seed_kol,
                                         seed_occupant, seed_x_watch, sf, transient, user)


async def _seed_chutes(db):
    o64 = await seed_occupant(db, 64, name="Chutes")
    await seed_x_watch(db, o64, "chutes_ai")
    return o64


async def _rows(db, sql, params=()):
    return [dict(r) for r in await db.fetchall(sql, params)]


# ── resolution + since_id ────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_resolve_then_since_id_persisted_and_reused(db, xset):
    xset()
    o64 = await _seed_chutes(db)
    t1 = [post(NOW - 3600 * i, "100", "chutes_ai", f"Chutes update {i} on SN64", seq=i) for i in range(1, 4)]
    fp = FakeProvider(users={"chutes_ai": user("100", "chutes_ai")}, timelines={"100": [t1]})
    c = make_collector(db, fp)
    out = await c.run_team_poll()
    acct = await db.fetchone("SELECT * FROM x_accounts WHERE author_id = '100'")
    assert acct["kind"] == "team" and acct["status"] == "active" and acct["since_id"] == t1[0].post_id
    assert acct["last_polled_at"] == NOW and acct["last_post_at"] == NOW - 3600
    assert await db.fetchval("SELECT occupant_key FROM x_account_occupants WHERE author_id = '100'") == o64
    assert await db.fetchval("SELECT COUNT(*) FROM x_account_snapshots WHERE author_id = '100'") == 1
    assert await db.fetchval("SELECT verified_at FROM watch_channels WHERE handle = 'chutes_ai'") == NOW
    rows = await _rows(db, "SELECT * FROM social_posts ORDER BY post_id")
    assert len(rows) == 3 and out.rows == 3 and all(r["occupant_key"] == o64 for r in rows)
    assert all(r["is_team_account"] == 1 and r["bot_filtered"] == 0 and r["source"] == "twitterapi_io" for r in rows)
    r0 = next(r for r in rows if r["post_id"] == t1[0].post_id)
    assert r0["text_hash"] == text_hash(t1[0].text) and r0["url"] == f"https://x.com/chutes_ai/status/{t1[0].post_id}"
    assert r0["engagement_observed_at"] == NOW and json.loads(r0["credibility_flags"]) == ["team"]
    # second run: polls with the stored since_id, X_TIMELINE_MAX_PAGES pages allowed
    t2 = [post(NOW + 60, "100", "chutes_ai", "new post", seq=9)] + t1
    fp.timelines["100"] = [t2]
    c2 = make_collector(db, fp, run_id="run-2", now=NOW + 3600)
    await c2.run_team_poll()
    tl = [x for x in fp.calls if x[0] == "user_timeline"]
    assert tl[0][2] is None and tl[1][2] == t1[0].post_id
    assert await db.fetchval("SELECT since_id FROM x_accounts WHERE author_id = '100'") == t2[0].post_id
    assert [x for x in fp.calls if x[0] == "resolve_user"] == [("resolve_user", "chutes_ai")]   # resolved once


@pytest.mark.asyncio
async def test_unresolved_handle_backs_off_and_emits(db, xset):
    xset(X_UNRESOLVED_RETRY_DAYS=7)
    o = await seed_occupant(db, 7, name="Seven")
    await seed_x_watch(db, o, "ghost_handle")
    fp = FakeProvider(users={"ghost_handle": None})
    await make_collector(db, fp).run_team_poll()
    st = json.loads(await db.fetchval("SELECT value_json FROM job_state WHERE job = 'x_team_poll' AND "
                                      "key = 'unresolved:ghost_handle'"))
    assert st["reason"] == "not_found" and st["consecutive"] == 1 and st["next_retry_at"] == NOW + 7 * DAY
    ev = await db.fetchone("SELECT * FROM events WHERE event_type = 'x_account_unresolved'")
    assert ev["occupant_key"] == o and json.loads(ev["payload"])["provider"] == "twitterapi_io"
    await make_collector(db, fp, run_id="r2", now=NOW + DAY).run_team_poll()           # in back-off: no call
    assert len(fp.calls) == 1
    await make_collector(db, fp, run_id="r3", now=NOW + 8 * DAY).run_team_poll()
    st = json.loads(await db.fetchval("SELECT value_json FROM job_state WHERE key = 'unresolved:ghost_handle'"))
    assert len(fp.calls) == 2 and st["not_found_count"] == 2 and st["consecutive"] == 2


@pytest.mark.asyncio
async def test_legacy_only_rows_are_ignored(db, xset):
    xset()
    o = await seed_occupant(db, 113, name="Tensor")
    await seed_x_watch(db, o, "TensorUSD", source="legacy")
    fp = FakeProvider(users={"tensorusd": user("113", "TensorUSD")})
    await make_collector(db, fp).run_team_poll()
    assert fp.calls == [] and await db.fetchval("SELECT COUNT(*) FROM x_accounts") == 0


# ── attribution (§2.6) ───────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_shared_handle_attribution(db, xset):
    xset()
    o1 = await seed_occupant(db, 1, name="Apex")
    o9 = await seed_occupant(db, 9, name="IOTA")
    for o in (o1, o9):
        await seed_x_watch(db, o, "MacrocosmosAI")
    await seed_account(db, "200", "MacrocosmosAI", okeys=[o1, o9])
    specific = post(NOW - 100, "200", "MacrocosmosAI", "SN9 pretraining run finished", seq=1)
    generic = post(NOW - 200, "200", "MacrocosmosAI", "We are hiring engineers", seq=2)
    fp = FakeProvider(timelines={"200": [[specific, generic]]})
    await make_collector(db, fp).run_team_poll()
    rows = {r["post_id"]: r for r in await _rows(db, "SELECT * FROM social_posts")}
    assert rows[specific.post_id]["occupant_key"] == o9
    assert rows[generic.post_id]["occupant_key"] is None
    team = await _rows(db, "SELECT occupant_key FROM social_post_mentions WHERE post_id = ? AND match_kind = 'team' "
                           "ORDER BY occupant_key", (generic.post_id,))
    assert [r["occupant_key"] for r in team] == sorted([o1, o9])
    assert not await db.fetchval("SELECT 1 FROM social_post_mentions WHERE post_id = ?", (specific.post_id,))


@pytest.mark.asyncio
async def test_team_post_seen_again_by_mention_search_refreshes_engagement_only(db, xset):
    xset()
    o64 = await _seed_chutes(db)
    await seed_account(db, "100", "chutes_ai", okeys=[o64])
    p = post(NOW - 600, "100", "chutes_ai", "Chutes ships v3 on SN64", seq=1, likes=5)
    fp = FakeProvider(timelines={"100": [[p]]})
    await make_collector(db, fp, clock=lambda: NOW).run_team_poll()
    first = await db.fetchone("SELECT * FROM social_posts WHERE post_id = ?", (p.post_id,))
    again = post(NOW - 600, "100", "chutes_ai", "edited text never stored", seq=1, likes=50)
    c = make_collector(db, FakeProvider(), job_id="x_mentions_poll", now=NOW + DAY, clock=lambda: NOW + DAY)
    await c.load()
    assert await c.ingest([again], source="official_v2", match_kind="query", occupant_hint=o64) == 0
    row = await db.fetchone("SELECT * FROM social_posts WHERE post_id = ?", (p.post_id,))
    assert await db.fetchval("SELECT COUNT(*) FROM social_posts") == 1
    assert row["like_count"] == 50 and row["engagement_observed_at"] == NOW + DAY
    assert (row["observed_at"], row["text"], row["event_at"], row["occupant_key"], row["source"]) == \
        (first["observed_at"], first["text"], first["event_at"], first["occupant_key"], "twitterapi_io")
    # own occupant gets no 'query' row from its own post
    assert not await db.fetchval("SELECT 1 FROM social_post_mentions WHERE match_kind = 'query'")


@pytest.mark.asyncio
async def test_ctf_placeholder_row_is_enriched(db, xset):
    xset()
    o64 = await _seed_chutes(db)
    await seed_account(db, "100", "chutes_ai", okeys=[o64])
    pid = sf(NOW - 5000)
    await db.execute("INSERT INTO social_posts (platform, post_id, author_id, author_handle, occupant_key, "
                     "is_team_account, text, text_hash, event_at, observed_at, source, author_credibility) VALUES "
                     "('x',?,?,?,?,1,?,?,?,?,'subnetalpha_ctf',1.0)",
                     (pid, "h:chutes_ai", "chutes_ai", o64, "ctf text", text_hash("ctf text"), NOW - 5000, NOW - 4000))
    c = make_collector(db, FakeProvider())
    await c.load()
    await c.ingest([post(NOW - 5000, "100", "chutes_ai", "ctf text", pid=pid, likes=7)], source="twitterapi_io",
                   match_kind="team")
    r = await db.fetchone("SELECT * FROM social_posts WHERE post_id = ?", (pid,))
    assert (r["source"], r["author_id"], r["like_count"], r["observed_at"]) == ("twitterapi_io", "100", 7, NOW - 4000)


@pytest.mark.asyncio
async def test_mention_search_rows_and_bot_filter(db, xset):
    xset()
    o64 = await _seed_chutes(db)
    o44 = await seed_occupant(db, 44, name="Score")
    young = user("777", "taomoon12345", followers=12, following=2400, posts=9000, created=NOW - 10 * DAY)
    analyst = user("42", "0xSammy", followers=89000)
    a = post(NOW - 100, "42", "0xSammy", "Chutes on bittensor, also Score SN44 looks good", seq=1, author=analyst)
    b = post(NOW - 200, "777", "taomoon12345", "GIVEAWAY RT to win $SN64 airdrop", seq=2, author=young)
    c = make_collector(db, FakeProvider(), job_id="x_mentions_poll")
    await c.load()
    await c.ingest([a, b], source="twitterapi_io", match_kind="query", occupant_hint=o64)
    m = await _rows(db, "SELECT post_id, occupant_key, match_kind FROM social_post_mentions ORDER BY post_id, "
                        "occupant_key")
    assert {(r["post_id"], r["occupant_key"], r["match_kind"]) for r in m} == {
        (a.post_id, o64, "query"), (a.post_id, o44, "rule"), (b.post_id, o64, "query")}
    rows = {r["post_id"]: r for r in await _rows(db, "SELECT * FROM social_posts")}
    assert rows[a.post_id]["occupant_key"] is None and rows[a.post_id]["bot_filtered"] == 0
    assert rows[b.post_id]["bot_filtered"] == 1 and "promo" in json.loads(rows[b.post_id]["credibility_flags"])


@pytest.mark.asyncio
async def test_duplicate_text_across_db_and_authors(db, xset):
    xset()
    await _seed_chutes(db)
    c = make_collector(db, FakeProvider(), job_id="x_mentions_poll")
    await c.load()
    txt = "Huge news for SN64 holders this week, check it"
    await c.ingest([post(NOW - 300, "1", "aaa", txt, seq=1, author=user("1", "aaa"))], source="twitterapi_io",
                   match_kind="rule")
    await c.ingest([post(NOW - 100, "2", "bbb", txt, seq=2, author=user("2", "bbb")),
                    post(NOW - 50, "2", "bbb", txt, seq=3, author=user("2", "bbb"))], source="twitterapi_io",
                   match_kind="rule")
    fl = json.loads(await db.fetchval("SELECT credibility_flags FROM social_posts WHERE post_id = ?",
                                      (sf(NOW - 50, 3),)))
    assert "dup_self" in fl and "dup_coord" in fl


# ── error policy (§7) through 01's wrapper ───────────────────────────────────────────────────────────────────
def _job(cls, rt, job_id):
    return cls(rt, SPEC_BY_ID[job_id])


async def _three_accounts(db):
    keys = []
    for n, h, aid in ((11, "aaa_team", "11"), (12, "bbb_team", "12"), (13, "ccc_team", "13")):
        o = await seed_occupant(db, n, name=f"Name{n}")
        await seed_x_watch(db, o, h)
        await seed_account(db, aid, h, okeys=[o])
        keys.append(o)
    return keys


@pytest.mark.asyncio
async def test_transient_on_one_account_is_partial(rt, xset, monkeypatch):
    xset()
    await _three_accounts(rt.db)
    fp = FakeProvider(timelines={"11": [[post(NOW, "11", "aaa_team", "a", seq=1)]], "12": [transient()],
                                 "13": [[post(NOW, "13", "ccc_team", "c", seq=3)]]})
    monkeypatch.setattr(xmod, "make_provider", lambda s, h: fp)
    job = _job(XTeamPollJob, rt, "x_team_poll")
    await job.run(trigger="test")
    run = await rt.db.fetchone("SELECT * FROM collector_runs WHERE job = 'x_team_poll'")
    assert run["ok"] == 1 and run["note"].startswith("partial:transient")
    assert await rt.db.fetchval("SELECT COUNT(*) FROM social_posts") == 2
    assert await rt.db.fetchval("SELECT consecutive_failures FROM x_accounts WHERE author_id = '12'") == 1


@pytest.mark.asyncio
async def test_majority_transient_raises(rt, xset, monkeypatch):
    xset()
    await _three_accounts(rt.db)
    fp = FakeProvider(timelines={"11": [transient()], "12": [transient()], "13": [[]]})
    monkeypatch.setattr(xmod, "make_provider", lambda s, h: fp)
    await _job(XTeamPollJob, rt, "x_team_poll").run(trigger="test")
    assert await rt.db.fetchval("SELECT ok FROM collector_runs WHERE job = 'x_team_poll'") == 0


@pytest.mark.asyncio
async def test_auth_fails_the_run_after_one_call(rt, xset, monkeypatch):
    xset()
    await _three_accounts(rt.db)
    fp = FakeProvider(timelines={"11": [XProviderError("auth", "HTTP 401", status=401)]})
    monkeypatch.setattr(xmod, "make_provider", lambda s, h: fp)
    await _job(XTeamPollJob, rt, "x_team_poll").run(trigger="test")
    run = await rt.db.fetchone("SELECT * FROM collector_runs WHERE job = 'x_team_poll'")
    assert run["ok"] == 0 and "auth" in run["error"] and len(fp.calls) == 1


@pytest.mark.asyncio
async def test_rate_limited_ends_run_early(rt, xset, monkeypatch):
    xset()
    await _three_accounts(rt.db)
    fp = FakeProvider(timelines={"11": [XProviderError("rate_limited", "429", status=429)]})
    monkeypatch.setattr(xmod, "make_provider", lambda s, h: fp)
    await _job(XTeamPollJob, rt, "x_team_poll").run(trigger="test")
    run = await rt.db.fetchone("SELECT * FROM collector_runs WHERE job = 'x_team_poll'")
    assert run["ok"] == 1 and run["note"].startswith("partial:rate_limited") and len(fp.calls) == 1


@pytest.mark.asyncio
async def test_vendor_402_fails_run_with_provider_balance_event(rt, xset, monkeypatch):
    xset()
    await _three_accounts(rt.db)
    fp = FakeProvider(timelines={"11": [XProviderError("budget", "402", status=402)]})
    monkeypatch.setattr(xmod, "make_provider", lambda s, h: fp)
    await _job(XTeamPollJob, rt, "x_team_poll").run(trigger="test")
    run = await rt.db.fetchone("SELECT * FROM collector_runs WHERE job = 'x_team_poll'")
    assert run["ok"] == 0 and "provider_balance" in run["error"]
    ev = await rt.db.fetchone("SELECT payload FROM events WHERE event_type = 'x_budget_paused'")
    assert json.loads(ev["payload"])["reason"] == "provider_balance"


@pytest.mark.asyncio
@pytest.mark.parametrize("sentinel,reason", [("KILL_SWITCH", "kill_switch"), ("data/control/x.paused",
                                                                              "emergency_file")])
async def test_pauses_make_zero_calls(rt, xset, monkeypatch, tmp_root, sentinel, reason):
    xset()
    await _three_accounts(rt.db)
    fp = FakeProvider()
    monkeypatch.setattr(xmod, "make_provider", lambda s, h: fp)
    (tmp_root / sentinel).parent.mkdir(parents=True, exist_ok=True)
    (tmp_root / sentinel).write_text("")
    job = _job(XTeamPollJob, rt, "x_team_poll")
    await job.run(trigger="test")
    assert fp.calls == [] and job.status()["paused"] == reason
    assert not await rt.db.fetchval("SELECT 1 FROM collector_runs WHERE job = 'x_team_poll'")


@pytest.mark.asyncio
async def test_null_provider_disables_paid_jobs(rt, xset, monkeypatch):
    xset(X_PROVIDER="null")
    fp = FakeProvider()
    monkeypatch.setattr(xmod, "make_provider", lambda s, h: fp)
    for cls, jid in ((XTeamPollJob, "x_team_poll"), (XKolPollJob, "x_kol_poll"),
                     (XProfileRefreshJob, "x_profile_refresh")):
        job = _job(cls, rt, jid)
        assert job.enabled() is False
        await job.run(trigger="test")
        assert job.status()["enabled"] is False
    xset(X_TEAM_ENABLED=False)
    assert _job(XTeamPollJob, rt, "x_team_poll").enabled() is False
    xset(TWITTERAPI_IO_KEY="")
    assert _job(XTeamPollJob, rt, "x_team_poll").enabled() is False
    assert fp.calls == []


# ── routing table, profiles, KOLs ───────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_account_occupants_resync_after_retire(db, xset):
    xset()
    o1 = await seed_occupant(db, 1, name="Apex")
    o9 = await seed_occupant(db, 9, name="IOTA")
    w9 = None
    for o in (o1, o9):
        w = await seed_x_watch(db, o, "MacrocosmosAI")
        w9 = w if o == o9 else w9
    await seed_account(db, "200", "MacrocosmosAI", okeys=[o1])
    c = make_collector(db, FakeProvider())
    await c.load()
    r = await c.sync_account_occupants()
    assert r["added"] == 1
    await db.execute("UPDATE watch_channels SET retired_at = ? WHERE id = ?", (NOW, w9))
    c2 = make_collector(db, FakeProvider())
    await c2.load()
    assert (await c2.sync_account_occupants())["removed"] == 1
    assert [x["occupant_key"] for x in await _rows(db, "SELECT occupant_key FROM x_account_occupants")] == [o1]


@pytest.mark.asyncio
async def test_profile_refresh_gone_after_two_nones_and_rename(db, xset):
    xset(X_PROFILE_REFRESH_DAYS=7)
    o64 = await _seed_chutes(db)
    o5 = await seed_occupant(db, 5, name="Five")
    await seed_x_watch(db, o5, "old_name")
    await seed_account(db, "100", "chutes_ai", okeys=[o64])
    await seed_account(db, "500", "old_name", okeys=[o5])
    fp = FakeProvider(users={"100": None, "500": user("500", "new_name", followers=777)})
    await make_collector(db, fp, job_id="x_profile_refresh").run_profile_refresh()
    assert await db.fetchval("SELECT status FROM x_accounts WHERE author_id = '100'") == "unresolved"
    assert await db.fetchval("SELECT handle FROM x_accounts WHERE author_id = '500'") == "new_name"
    ch = await db.fetchone("SELECT * FROM events WHERE event_type = 'x_handle_changed'")
    assert json.loads(ch["payload"]) == {"author_id": "500", "old_handle": "old_name", "new_handle": "new_name"}
    assert await db.fetchval("SELECT followers FROM x_account_snapshots WHERE author_id = '500'") == 777
    await make_collector(db, fp, job_id="x_profile_refresh", run_id="r2", now=NOW + 7 * DAY).run_profile_refresh()
    assert await db.fetchval("SELECT status FROM x_accounts WHERE author_id = '100'") == "gone"
    gone = await db.fetchone("SELECT * FROM events WHERE event_type = 'x_account_gone'")
    assert gone["occupant_key"] == o64 and json.loads(gone["payload"])["since"] == NOW
    # the renamed account still routes through the old watch handle (snapshot alias)
    c = make_collector(db, FakeProvider())
    await c.load()
    await c.sync_account_occupants()
    assert await db.fetchval("SELECT occupant_key FROM x_account_occupants WHERE author_id = '500'") == o5
    # a later success sets 'active' again
    fp.users["100"] = user("100", "chutes_ai")
    await make_collector(db, fp, job_id="x_profile_refresh", run_id="r3", now=NOW + 14 * DAY).run_profile_refresh()
    assert await db.fetchval("SELECT status FROM x_accounts WHERE author_id = '100'") == "active"


@pytest.mark.asyncio
async def test_kol_search_poll_and_team_wins(db, xset):
    xset(X_KOL_POLL_MODE="search")
    o1 = await seed_occupant(db, 1, name="Apex")
    o64 = await _seed_chutes(db)
    await seed_x_watch(db, o1, "MacrocosmosAI")
    await seed_kol(db, "MacrocosmosAI", credibility=0.7, team_keys=[o1], category="founder")
    await seed_kol(db, "0xSammy", credibility=0.8)
    await seed_kol(db, "yuma_ai", credibility=0.0, verified=0, category="fund")
    fp = FakeProvider(users={"macrocosmosai": user("200", "MacrocosmosAI"), "0xsammy": user("42", "0xSammy")})
    kpost = post(NOW - 100, "42", "0xSammy", "Chutes SN64 is printing", seq=1)
    mpost = post(NOW - 200, "200", "MacrocosmosAI", "Apex SN1 update, also chutes SN64 partner", seq=2)
    fp.searches = lambda q, since_id, since_at, cursor: [kpost, mpost]
    c = make_collector(db, fp, job_id="x_kol_poll")
    out = await c.run_kol_poll()
    accts = {r["author_id"]: r for r in await _rows(db, "SELECT * FROM x_accounts")}
    assert accts["200"]["kind"] == "team" and accts["200"]["kol_handle"] == "MacrocosmosAI"
    assert accts["42"]["kind"] == "kol" and accts["42"]["kol_handle"] == "0xSammy"
    assert await db.fetchval("SELECT author_id FROM kol_accounts WHERE handle = '0xSammy'") == "42"
    assert ("resolve_user", "yuma_ai") not in fp.calls                       # verified=0 is not polled
    search = [x for x in fp.calls if x[0] == "search_recent"]
    assert len(search) == 1 and set(search[0][1]) == {"MacrocosmosAI", "0xSammy"}
    rows = {r["post_id"]: r for r in await _rows(db, "SELECT * FROM social_posts")}
    assert rows[kpost.post_id]["is_kol"] == 1 and rows[kpost.post_id]["kol_handle"] == "0xSammy"
    assert rows[kpost.post_id]["is_team_account"] == 0 and rows[kpost.post_id]["author_credibility"] == 0.8
    m = rows[mpost.post_id]
    assert (m["is_team_account"], m["is_kol"], m["occupant_key"]) == (1, 1, o1)
    rule = {(r["post_id"], r["occupant_key"]) for r in await _rows(
        db, "SELECT post_id, occupant_key FROM social_post_mentions WHERE match_kind = 'rule'")}
    assert rule == {(kpost.post_id, o64), (mpost.post_id, o64)}
    st = await db.fetchone("SELECT * FROM x_search_state WHERE query_key = 'kol:0'")
    assert st["since_id"] == max(kpost.post_id, mpost.post_id, key=int) and st["last_posts"] == 2
    assert out.note.startswith("kol ")


@pytest.mark.asyncio
async def test_kol_seed_load_never_overwrites_operator_edits(db, tmp_path):
    from scripts.build_kol_list import write_csv
    await seed_occupant(db, 120, name="Affine")
    for n in (1, 9, 13, 25, 37, 4, 8, 44, 61, 24):
        await seed_occupant(db, n, name=f"Name{n}")
    p = tmp_path / "kol.csv"
    assert write_csv(p) == 37
    r = await load_kol_seed(db, p, now=NOW)
    assert r["inserted"] == 37
    row = await db.fetchone("SELECT COUNT(*) AS n, SUM(verified) AS v FROM kol_accounts")
    assert (row["n"], row["v"]) == (37, 32)
    assert json.loads(await db.fetchval("SELECT team_occupant_keys FROM kol_accounts WHERE handle = 'const_reborn'")) \
        == ["120:1000"]
    assert len(json.loads(await db.fetchval("SELECT team_occupant_keys FROM kol_accounts WHERE "
                                            "handle = 'MacrocosmosAI'"))) == 3        # SN1/9/13 (25/37 re-registered)
    await db.execute("UPDATE kol_accounts SET credibility = 0.1, added_by = 'operator' WHERE handle = 'taostats'")
    r2 = await load_kol_seed(db, p, now=NOW + 1)
    assert r2["inserted"] == 0
    assert await db.fetchval("SELECT credibility FROM kol_accounts WHERE handle = 'taostats'") == 0.1


@pytest.mark.asyncio
async def test_top_n_falls_back_to_chain_hourly(db, xset):
    xset()
    keys = [await seed_occupant(db, n, name=f"Name{n}") for n in range(1, 6)]
    for i, k in enumerate(keys):
        await db.execute("INSERT INTO chain_hourly (netuid, hour_ts, occupant_key, event_at, observed_at, source, "
                         "emission) VALUES (?,?,?,?,?,?,?)", (i + 1, NOW - 3600, k, NOW - 3600, NOW, "sse", i * 1.0))
    c = make_collector(db, FakeProvider(), job_id="x_mentions_poll")
    await c.load()
    assert [r.occupant_key for r in await c.top_n_occupants(3)] == [keys[4], keys[3], keys[2]]


@pytest.mark.asyncio
async def test_dry_run_plan_makes_no_call(db, xset):
    xset()
    await _seed_chutes(db)
    o = await seed_occupant(db, 2, name="Two")
    await seed_x_watch(db, o, "two_team")
    await seed_account(db, "2", "two_team", okeys=[o], since_id=sf(NOW - DAY))
    from app.collectors.x_providers.twitterapi_io import TwitterApiIo
    from tests.collectors.x.conftest import fast_http
    c = make_collector(db, TwitterApiIo(fast_http()))
    plan = await c.plan("team")
    assert plan["resolutions"] == 1 and plan["pages"] == 2 and plan["requests"] == 3
    assert plan["estimate_usd"] == pytest.approx(0.00018 + 2 * 0.003)


@pytest.mark.asyncio
async def test_raw_archive_written_when_enabled(db, tmp_root, xset):
    import gzip
    xset(X_RAW_ARCHIVE_ENABLED=True)
    o64 = await _seed_chutes(db)
    await seed_account(db, "100", "chutes_ai", okeys=[o64])
    fp = FakeProvider(timelines={"100": [[post(NOW - 60, "100", "chutes_ai", "archived", seq=1)]]})
    await make_collector(db, fp).run_team_poll()
    files = list((tmp_root / "data" / "raw" / "x" / "twitterapi_io").glob("*.jsonl.gz"))
    assert len(files) == 1 and files[0].name == "2026-10-17.jsonl.gz"
    lines = gzip.open(files[0], "rt").read().splitlines()
    assert len(lines) == 1 and json.loads(lines[0])["raw"]["text"] == "archived"


def test_names_merge_keeps_hand_added_lines():
    from scripts.build_kol_list import merge_names
    out = merge_names(["# header", "Hand Added Name", "Apex"], ["Chutes", "Bridge", "subnet12", "Its AI", "Foo"],
                      {"bridge", "its", "ai"})
    assert "Hand Added Name" in out and "Bridge" in out and "Score" in out
    assert "Chutes" not in out and "subnet12" not in out and "Foo" not in out
    assert out == sorted(out, key=str.lower) and not any(x.startswith("#") for x in out)
