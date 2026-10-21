"""Spec 09 §9.1 test_social_x: the §4 absence/failure matrix row by row, the ctf fallback contract (§2.9), PIT reads,
and that no producer exists for announcement_cadence_30d / silence_days."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

import app.config as app_config
import app.features.social_x as sx
from app.features.types import MetricResult
from app.nlp.textnorm import text_hash
from tests.collectors.x.conftest import DAY, NOW, seed_account, seed_kol, seed_occupant, seed_x_watch, sf

AS_OF = datetime.fromtimestamp(NOW, timezone.utc)
FNS = sx.PRODUCERS


@pytest.fixture
def xs(monkeypatch):
    def _apply(**kw):
        base = dict(X_ENABLED=True, X_PROVIDER="twitterapi_io", TWITTERAPI_IO_KEY="k", X_CTF_FALLBACK_METRICS=True,
                    X_PROFILE_REFRESH_DAYS=7, X_UNRESOLVED_RETRY_DAYS=7)
        base.update(kw)
        s = app_config.settings.model_copy(update=base)
        monkeypatch.setattr(app_config, "settings", s)
        return s
    _apply()
    return _apply


async def add_post(db, author_id, ts, *, occ=None, team=1, text="hello", seq=0, likes=1, reposts=0, replies=0,
                   source="twitterapi_io", obs=None, repost=0, reply_to=None, bot=0, kol=None, eng=True):
    pid = sf(ts, seq)
    await db.execute(
        "INSERT INTO social_posts (platform, post_id, author_id, author_handle, occupant_key, is_team_account, is_kol, "
        "kol_handle, text, text_hash, like_count, repost_count, reply_count, engagement_observed_at, is_repost, "
        "in_reply_to, bot_filtered, event_at, observed_at, source, author_credibility) "
        "VALUES ('x',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (pid, author_id, "h", occ, team, int(kol is not None), kol, text, text_hash(text + pid), likes, reposts,
         replies, (obs or ts) if eng else None, repost, reply_to, bot, ts, obs if obs is not None else ts + 60, source,
         1.0))
    return pid


async def mention(db, pid, okey, kind, obs, ev):
    await db.execute("INSERT INTO social_post_mentions (platform, post_id, occupant_key, match_kind, event_at, "
                     "observed_at) VALUES ('x',?,?,?,?,?)", (pid, okey, kind, ev, obs))


async def ctf_state(db, okey, **v):
    val = {"fetched_at": NOW - DAY, "http_status": 200, "n_items": 12, "n_matched": 12, "newest_event_at": None}
    val.update(v)
    await db.execute("INSERT OR REPLACE INTO job_state (job, key, value_json, updated_at) VALUES (?,?,?,?)",
                     ("x_ctf_crosscheck", f"page:{okey}", json.dumps(val), NOW))


async def run(db, key, okey):
    res = await FNS[key](db, okey, AS_OF)
    assert isinstance(res, MetricResult)
    return res


async def _team(db, netuid=64, handle="chutes_ai", aid="100", polled=NOW - 3600, resolved=NOW - 60 * DAY):
    o = await seed_occupant(db, netuid, name=f"Name{netuid}")
    await seed_x_watch(db, o, handle)
    await seed_account(db, aid, handle, okeys=[o], last_polled_at=polled, resolved_at=resolved)
    return o


# ── absence ──────────────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_no_handle_is_absent(db, xs):
    o = await seed_occupant(db, 3, name="Three")
    for k in ("x_cadence_30d", "x_last_activity_days", "x_followers"):
        r = await run(db, k, o)
        assert r.status == "absent" and r.value == 0.0 and r.mapped_json()["reason"] == "no_x_account"
    r = await run(db, "x_engagement_30d", o)
    assert r.status == "not_applicable" and r.value_json["reason"] == "no_x_account"
    xs(X_PROVIDER="null")                                       # absent regardless of the provider
    assert (await run(db, "x_cadence_30d", o)).status == "absent"


@pytest.mark.asyncio
async def test_legacy_only_and_pit_watch_rows(db, xs):
    o = await seed_occupant(db, 113, name="Tensor")
    await seed_x_watch(db, o, "TensorUSD", source="legacy")
    assert (await run(db, "x_cadence_30d", o)).absence_reason == "no_x_account"
    o2 = await seed_occupant(db, 114, name="Later")
    await seed_x_watch(db, o2, "later_team", first_seen=NOW + DAY)                 # not yet declared at as_of
    assert (await run(db, "x_followers", o2)).absence_reason == "no_x_account"
    o3 = await seed_occupant(db, 115, name="Retired")
    await seed_x_watch(db, o3, "retired_team", retired_at=NOW - DAY)
    assert (await run(db, "x_followers", o3)).absence_reason == "no_x_account"


@pytest.mark.asyncio
async def test_gone_account_is_absent(db, xs):
    o = await _team(db)
    await db.execute("UPDATE x_accounts SET status = 'gone' WHERE author_id = '100'")
    for k in ("x_cadence_30d", "x_last_activity_days", "x_followers"):
        assert (await run(db, k, o)).absence_reason == "x_account_gone"
    r = await run(db, "x_engagement_30d", o)
    assert r.status == "not_applicable" and r.value_json["reason"] == "x_account_gone"


@pytest.mark.asyncio
async def test_declared_handle_not_found_twice_is_gone_once_is_failed(db, xs):
    o = await seed_occupant(db, 7, name="Seven")
    await seed_x_watch(db, o, "ghost")
    st = {"reason": "not_found", "consecutive": 1, "not_found_count": 1, "not_found_first_at": NOW - DAY,
          "not_found_last_at": NOW - DAY, "next_retry_at": NOW + 6 * DAY}
    await db.execute("INSERT INTO job_state (job, key, value_json, updated_at) VALUES ('x_team_poll', "
                     "'unresolved:ghost', ?, ?)", (json.dumps(st), NOW))
    r = await run(db, "x_cadence_30d", o)
    assert r.status == "failed" and r.value_json["reason"] == "x_handle_unresolved"
    st.update(not_found_count=2, not_found_first_at=NOW - 9 * DAY, not_found_last_at=NOW - DAY)
    await db.execute("UPDATE job_state SET value_json = ? WHERE key = 'unresolved:ghost'", (json.dumps(st),))
    assert (await run(db, "x_cadence_30d", o)).absence_reason == "x_account_gone"
    assert (await run(db, "x_followers", o)).absence_reason == "x_account_gone"


# ── failure / ok (provider path) ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_polled_with_zero_posts(db, xs):
    o = await _team(db, resolved=NOW - 40 * DAY)
    r = await run(db, "x_cadence_30d", o)
    assert r.status == "ok" and r.value == 0.0
    r = await run(db, "x_last_activity_days", o)
    assert r.status == "ok" and r.value == pytest.approx(40.0) and r.value_json["lower_bound"] is True
    r = await run(db, "x_engagement_30d", o)
    assert r.status == "not_applicable" and r.value_json["reason"] == "no_posts_30d"


@pytest.mark.asyncio
async def test_poll_stale_is_failed(db, xs):
    o = await _team(db, polled=NOW - 4 * DAY)
    for k in ("x_cadence_30d", "x_last_activity_days", "x_engagement_30d"):
        r = await run(db, k, o)
        assert r.status == "failed" and r.value_json["reason"] == "x_poll_stale"


@pytest.mark.asyncio
async def test_counts_originals_engagement_median_and_pit(db, xs):
    o = await _team(db)
    await add_post(db, "100", NOW - 2 * DAY, occ=o, seq=1, likes=10, reposts=2, replies=1)        # 15
    await add_post(db, "100", NOW - 3 * DAY, occ=o, seq=2, likes=0, reposts=0, replies=0)         # 0
    await add_post(db, "100", NOW - 4 * DAY, occ=o, seq=3, likes=4, reposts=1, replies=0)         # 6
    await add_post(db, "100", NOW - 1 * DAY, occ=o, seq=4, repost=1)                              # repost
    await add_post(db, "100", NOW - 5 * DAY, occ=o, seq=5, reply_to="123")                         # reply
    await add_post(db, "100", NOW - 40 * DAY, occ=o, seq=6)                                       # outside 30 d
    await add_post(db, "100", NOW - 100, occ=o, seq=7, obs=NOW + 10)                              # observed later
    assert (await run(db, "x_cadence_30d", o)).value == 3
    r = await run(db, "x_engagement_30d", o)
    assert r.status == "ok" and r.value == 6.0
    r = await run(db, "x_last_activity_days", o)
    assert r.value == pytest.approx(1.0) and r.value_json["lower_bound"] is False          # repost counts here


@pytest.mark.asyncio
async def test_zero_engagement_is_legit(db, xs):
    o = await _team(db)
    await add_post(db, "100", NOW - DAY, occ=o, likes=0, reposts=0, replies=0)
    r = await run(db, "x_engagement_30d", o)
    assert r.status == "ok" and r.value == 0.0


@pytest.mark.asyncio
async def test_shared_handle_team_rows(db, xs):
    o1 = await seed_occupant(db, 1, name="Apex")
    o9 = await seed_occupant(db, 9, name="IOTA")
    for o in (o1, o9):
        await seed_x_watch(db, o, "MacrocosmosAI")
    await seed_account(db, "200", "MacrocosmosAI", okeys=[o1, o9], last_polled_at=NOW - 60)
    pid = await add_post(db, "200", NOW - DAY, occ=None)
    for o in (o1, o9):
        await mention(db, pid, o, "team", NOW - DAY + 60, NOW - DAY)
    await add_post(db, "200", NOW - 2 * DAY, occ=o9, seq=2)
    assert (await run(db, "x_cadence_30d", o1)).value == 1
    assert (await run(db, "x_cadence_30d", o9)).value == 2
    late = await add_post(db, "200", NOW - 3 * DAY, occ=None, seq=3)
    await mention(db, late, o1, "team", NOW + 5, NOW - 3 * DAY)                       # mention row observed later
    assert (await run(db, "x_cadence_30d", o1)).value == 1


@pytest.mark.asyncio
async def test_followers_snapshot_window_and_max(db, xs):
    o = await _team(db)
    await seed_x_watch(db, o, "chutes_alt")
    await seed_account(db, "101", "chutes_alt", okeys=[o], last_polled_at=NOW)
    for aid, obs, f in (("100", NOW - 2 * DAY, 1000), ("100", NOW + 60, 9999), ("101", NOW - DAY, 3000)):
        await db.execute("INSERT INTO x_account_snapshots (author_id, observed_at, followers) VALUES (?,?,?)",
                         (aid, obs, f))
    r = await run(db, "x_followers", o)
    assert r.status == "ok" and r.value == 3000
    await db.execute("DELETE FROM x_account_snapshots")
    await db.execute("INSERT INTO x_account_snapshots (author_id, observed_at, followers) VALUES ('100', ?, 50)",
                     (NOW - 11 * DAY,))                                               # older than 7 + 3 d
    r = await run(db, "x_followers", o)
    assert r.status == "failed" and r.value_json["reason"] == "no_fresh_snapshot"


# ── X inactive: the ctf fallback contract ────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_ctf_only_12_in_window_is_truncated(db, xs):
    xs(X_PROVIDER="null")
    o = await seed_occupant(db, 64, name="Chutes")
    await seed_x_watch(db, o, "chutes_ai")
    for i in range(12):
        await add_post(db, "h:chutes_ai", NOW - (i + 1) * DAY, occ=o, seq=i, source="subnetalpha_ctf", eng=False)
    await ctf_state(db, o, newest_event_at=NOW - DAY)
    r = await run(db, "x_cadence_30d", o)
    assert r.status == "ok" and r.value == 12 and r.value_json["truncated"] is True
    r = await run(db, "x_last_activity_days", o)
    assert r.status == "ok" and r.value == pytest.approx(1.0)
    r = await run(db, "x_engagement_30d", o)
    assert r.status == "failed" and r.value_json["reason"] == "ctf_no_engagement"
    for k in ("x_mentions_7d", "x_kol_posts_30d"):
        r = await run(db, k, o)
        assert r.status == "failed" and r.value_json["reason"] == "x_inactive"


@pytest.mark.asyncio
@pytest.mark.parametrize("state,reason", [
    ({"fetched_at": NOW - 4 * DAY}, "ctf_stale"),
    ({"n_items": 0, "n_matched": 0}, "ctf_no_feed"),
    ({"n_items": 12, "n_matched": 0}, "ctf_handle_mismatch"),
    ({"http_status": 503}, "ctf_fetch_failed"),
    ({"fetched_at": NOW + 3600}, "ctf_stale"),
    (None, "ctf_not_fetched"),
])
async def test_ctf_gate_failures(db, xs, state, reason):
    xs(X_PROVIDER="null")
    o = await seed_occupant(db, 64, name="Chutes")
    await seed_x_watch(db, o, "chutes_ai")
    await add_post(db, "h:chutes_ai", NOW - DAY, occ=o, source="subnetalpha_ctf", eng=False)
    if state is not None:
        await ctf_state(db, o, **state)
    for k in ("x_cadence_30d", "x_last_activity_days"):
        r = await run(db, k, o)
        assert r.status == "failed" and r.value_json["reason"] == reason and r.value is None


@pytest.mark.asyncio
async def test_ctf_fallback_disabled(db, xs):
    xs(X_PROVIDER="null", X_CTF_FALLBACK_METRICS=False)
    o = await seed_occupant(db, 64, name="Chutes")
    await seed_x_watch(db, o, "chutes_ai")
    await ctf_state(db, o)
    r = await run(db, "x_cadence_30d", o)
    assert r.status == "failed" and r.value_json["reason"] == "x_inactive"


@pytest.mark.asyncio
async def test_ctf_rows_ignored_while_x_active(db, xs):
    o = await _team(db)
    await add_post(db, "h:chutes_ai", NOW - DAY, occ=o, source="subnetalpha_ctf", eng=False)
    assert (await run(db, "x_cadence_30d", o)).value == 0


# ── evidence / fallback leg ──────────────────────────────────────────────────────────────────────────────────
async def _x_run(db, job, finished, note=None):
    import uuid
    await db.execute("INSERT INTO collector_runs (run_id, job, started_at, finished_at, ok, note) VALUES (?,?,?,?,1,?)",
                     (uuid.uuid4().hex, job, finished - 10, finished, note))


@pytest.mark.asyncio
async def test_mentions_7d(db, xs):
    o = await _team(db)
    a = await add_post(db, "9", NOW - DAY, team=0, seq=1)
    b = await add_post(db, "9", NOW - 2 * DAY, team=0, seq=2, bot=1)                      # bot_filtered
    c = await add_post(db, "9", NOW - 8 * DAY, team=0, seq=3)                             # outside 7 d
    d = await add_post(db, "100", NOW - DAY, occ=o, seq=4)                                # team post
    for pid in (a, b, c, d):
        await mention(db, pid, o, "rule", NOW - 3600, NOW - DAY)
    await mention(db, a, o, "query", NOW - 3600, NOW - DAY)
    r = await run(db, "x_mentions_7d", o)
    assert r.status == "failed" and r.value_json["reason"] == "x_poll_stale"           # tail, no fresh X run
    await _x_run(db, "x_kol_poll", NOW - 3600)
    r = await run(db, "x_mentions_7d", o)
    assert r.status == "ok" and r.value == 1 and r.value_json["searched"] is False
    await db.execute("INSERT INTO job_state (job, key, value_json, updated_at) VALUES ('x_mentions_poll', 'last_top', "
                     "?, ?)", (json.dumps({"at": NOW - 3600, "keys": [o]}), NOW))
    r = await run(db, "x_mentions_7d", o)
    assert r.status == "failed" and r.value_json["reason"] == "mention_search_stale"
    await db.execute("INSERT INTO x_search_state (query_key, since_id, last_run_at) VALUES (?, NULL, ?)",
                     (f"mention:{o}", NOW - 3600))
    r = await run(db, "x_mentions_7d", o)
    assert r.status == "ok" and r.value == 1 and r.value_json["searched"] is True


@pytest.mark.asyncio
async def test_kol_posts_30d(db, xs):
    o = await _team(db)
    o2 = await seed_occupant(db, 2, name="Two")
    await seed_kol(db, "k_low", credibility=0.4)
    await seed_kol(db, "k_a", credibility=0.8)
    await seed_kol(db, "k_b", credibility=0.6)
    await seed_kol(db, "k_team", credibility=0.9, team_keys=[o])
    r = await run(db, "x_kol_posts_30d", o)
    assert r.status == "failed" and r.value_json["reason"] == "too_few_kols"
    await seed_kol(db, "k_c", credibility=0.7)
    await seed_kol(db, "k_unver", credibility=0.9, verified=0)
    r = await run(db, "x_kol_posts_30d", o)
    assert r.status == "failed" and r.value_json["reason"] == "kol_poll_stale"
    await _x_run(db, "x_kol_poll", NOW - 7200, note="paused:budget reason=hard_cap kol ...")
    assert (await run(db, "x_kol_posts_30d", o)).value_json["reason"] == "kol_poll_stale"
    await _x_run(db, "x_kol_poll", NOW - 3600)
    seq = 0
    for kol, ts, occ in (("k_a", NOW - DAY, o), ("k_a", NOW - 2 * DAY, o), ("k_b", NOW - 3 * DAY, o),
                         ("k_low", NOW - DAY, o), ("k_team", NOW - DAY, o), ("k_c", NOW - 40 * DAY, o),
                         ("k_c", NOW - DAY, o2)):
        seq += 1
        pid = await add_post(db, f"a_{kol}", ts, team=0, kol=kol, seq=seq)
        await mention(db, pid, occ, "rule", ts + 60, ts)
    r = await run(db, "x_kol_posts_30d", o)
    assert r.status == "ok" and r.value == 2 and r.value_json["kols"] == ["k_a", "k_b"]
    assert (await run(db, "x_kol_posts_30d", o2)).value == 1


def test_no_producer_for_derived_keys():
    assert not hasattr(sx, "compute_announcement_cadence_30d") and not hasattr(sx, "compute_silence_days")
    assert set(FNS) == {"x_cadence_30d", "x_engagement_30d", "x_followers", "x_last_activity_days", "x_mentions_7d",
                        "x_kol_posts_30d"}
    from app.features import registry as R
    for k in FNS:
        assert R.ATTACHED_FNS.get(k) is FNS[k]
    assert sx.register_producers() == 6
