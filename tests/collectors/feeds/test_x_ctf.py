"""Spec 09 §2.9 / §9.1 ``test_x_ctf.py``: parser (live 2026-10-18 CSS-only markup + synthetic research-selector
items), rows and the §2.6 conflict rule, shared handles, job_state contract, discrepancy events, run note."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import httpx
import pytest
import respx

from app.collectors.x_ctf import (XCtfCrosscheckJob, ctf_pages, parse_ctf, read_page_state, run_ctf_crosscheck,
                                  run_note, snowflake_to_datetime)
from app.http import ProviderError
from app.nlp.textnorm import text_hash
from tests.collectors.feeds.conftest import DAY, NOW, fixture_text, seed_identity, seed_occupant, seed_watch

SA = "https://subnetalpha.ai/subnet"


def test_snowflake_to_datetime():
    assert snowflake_to_datetime(0) == datetime.fromtimestamp(1288834974.657, timezone.utc)
    d = snowflake_to_datetime("1579236048191045632")
    assert d.tzinfo is not None and d.year == 2022


def test_parse_live_no_feed_markup():
    html = fixture_text("ctf", "no_feed.html")
    assert ".ctf-item" in html and 'data-ctf-style="106"' in html
    assert parse_ctf(html) == []
    assert parse_ctf("") == [] and parse_ctf(None) == [] and parse_ctf("   ") == []


def test_parse_twelve_items():
    items = parse_ctf(fixture_text("ctf", "feed_12.html"))
    assert len(items) == 12 and len({t.post_id for t in items}) == 12
    assert {t.author_handle for t in items} == {"chutes_ai"}
    for t in items:
        d = snowflake_to_datetime(t.post_id)
        assert t.event_at == int(d.timestamp())
        assert t.date_text == f"{d.day} {d.strftime('%b')}"           # the year-less date is only a sanity check
    assert items[0].text.startswith("Chutes now serves 100B") and items[0].has_url
    assert items[2].text == "" and not items[2].has_url               # media-only item
    assert items[1].has_url is False


def test_parse_other_handle():
    items = parse_ctf(fixture_text("ctf", "other_handle.html"))
    assert len(items) == 3 and {t.author_handle for t in items} == {"SomeFanAcct"}


async def seed_pages(db):
    o64 = await seed_occupant(db, 64, "Chutes")
    o1 = await seed_occupant(db, 1, "Apex")
    o9 = await seed_occupant(db, 9, "IOTA")
    o105 = await seed_occupant(db, 105, "Beam")
    o40 = await seed_occupant(db, 40, "Fanpage")
    o50 = await seed_occupant(db, 50, "NoEmbed")
    await seed_watch(db, o64, "x", "chutes_ai")
    await seed_watch(db, o1, "x", "MacrocosmosAI")
    await seed_watch(db, o9, "x", "MacrocosmosAI")
    await seed_watch(db, o105, "x", "beam_sn105")
    await seed_watch(db, o40, "x", "fanpage_team")
    for okey, slug, emb in ((o64, "chutes", "77"), (o1, "apex", "12"), (o105, "beam", "106"), (o40, "fan", "88"),
                            (o50, "noembed", "")):
        await seed_identity(db, okey, "slug", slug)
        await seed_identity(db, okey, "twitter_embed_id", emb)
    await seed_identity(db, o40, "twitter_embed_id", "", observed_at=NOW - 5 * DAY)      # older row; latest wins
    return o64, o1, o9, o105, o40, o50


def mock_pages():
    routes = {
        "chutes": respx.get(f"{SA}/chutes/").mock(return_value=httpx.Response(200, text=fixture_text("ctf", "feed_12.html"))),
        "apex": respx.get(f"{SA}/apex/").mock(return_value=httpx.Response(200, text=fixture_text("ctf", "shared_handle.html"))),
        "beam": respx.get(f"{SA}/beam/").mock(return_value=httpx.Response(200, text=fixture_text("ctf", "no_feed.html"))),
        "fan": respx.get(f"{SA}/fan/").mock(return_value=httpx.Response(200, text=fixture_text("ctf", "other_handle.html"))),
        "noembed": respx.get(f"{SA}/noembed/").mock(return_value=httpx.Response(200, text="")),
    }
    return routes


@pytest.mark.asyncio
async def test_ctf_pages_targets(db, fset):
    await seed_pages(db)
    pages = await ctf_pages(db)
    assert [(p.netuid, p.slug, p.embed_id) for p in pages] == [(1, "apex", "12"), (40, "fan", "88"),
                                                                (64, "chutes", "77"), (105, "beam", "106")]
    assert pages[0].url() == "https://subnetalpha.ai/subnet/apex/"


@pytest.mark.asyncio
@respx.mock
async def test_run_rows_state_events(db, http, fset):
    o64, o1, o9, o105, o40, o50 = await seed_pages(db)
    routes = mock_pages()
    rep = await run_ctf_crosscheck(db, http, now=NOW, run_id="r1")
    assert routes["noembed"].call_count == 0
    assert (rep["pages"], rep["with_feed"], rep["items"], rep["matched"]) == (4, 3, 17, 14)
    assert rep["rows"] == 14 and rep["events"] == 1 and rep["failed"] == 0
    assert run_note(rep).startswith("pages=4 with_feed=3 items=17")

    posts = {r["post_id"]: r for r in await db.fetchall("SELECT * FROM social_posts")}
    assert len(posts) == 14
    chutes = [r for r in posts.values() if r["author_handle"] == "chutes_ai"]
    assert len(chutes) == 12
    for r in chutes:
        assert (r["source"], r["occupant_key"], r["author_id"], r["is_team_account"], r["author_credibility"],
                r["bot_filtered"]) == ("subnetalpha_ctf", o64, "h:chutes_ai", 1, 1.0, 0)
        assert r["like_count"] is None and r["repost_count"] is None and r["engagement_observed_at"] is None
        assert r["event_at"] == int(snowflake_to_datetime(r["post_id"]).timestamp())
        assert r["url"] == f"https://x.com/chutes_ai/status/{r['post_id']}" and r["run_id"] == "r1"
        if r["text"]:
            assert r["text_hash"] == text_hash(r["text"])
        else:                                                            # media-only → 08's message-id hash
            assert r["text_hash"] == hashlib.sha256(f"msg:x:{r['post_id']}".encode()).hexdigest()
    assert (await db.fetchone("SELECT item_id, author_kind FROM social_posts LIMIT 1"))["author_kind"] == "team"

    # shared handle (§2.6): "Apex (SN1)" → SN1; the generic post → occupant NULL + one 'team' row per occupant
    mac = sorted((r for r in posts.values() if r["author_handle"] == "MacrocosmosAI"), key=lambda r: r["event_at"])
    assert mac[1]["occupant_key"] == o1 and mac[0]["occupant_key"] is None
    m = await db.fetchall("SELECT occupant_key, match_kind, event_at FROM social_post_mentions WHERE post_id = ? "
                          "ORDER BY occupant_key", (mac[0]["post_id"],))
    assert [(x["occupant_key"], x["match_kind"]) for x in m] == sorted([(o1, "team"), (o9, "team")])
    assert all(x["event_at"] == mac[0]["event_at"] for x in m)

    st = await read_page_state(db, o64)
    assert set(st) >= {"fetched_at", "http_status", "n_items", "n_matched", "newest_event_at"}
    assert (st["http_status"], st["n_items"], st["n_matched"]) == (200, 12, 12)
    assert st["newest_event_at"] == max(r["event_at"] for r in chutes) and st["last_feed_at"] == st["fetched_at"]
    beam = await read_page_state(db, o105)
    assert (beam["http_status"], beam["n_items"], beam["n_matched"], beam["newest_event_at"]) == (200, 0, 0, None)
    assert beam["last_feed_at"] is None
    fan = await read_page_state(db, o40)
    assert (fan["n_items"], fan["n_matched"]) == (3, 0)
    assert await read_page_state(db, o50) is None

    ev = await db.fetchall("SELECT * FROM events WHERE event_type = 'x_ctf_discrepancy'")
    assert len(ev) == 1 and ev[0]["occupant_key"] == o40 and ev[0]["source"] == "x" and ev[0]["severity"] == "warning"
    p = json.loads(ev[0]["payload"])
    assert (p["reason"], p["ctf_handle"], p["handle"], p["provider_last_post_at"]) == ("handle_mismatch", "SomeFanAcct",
                                                                                       "fanpage_team", None)

    rep2 = await run_ctf_crosscheck(db, http, now=NOW + 60, run_id="r2")       # idempotent by post id
    assert rep2["rows"] == 0 and rep2["matched"] == 14
    assert await db.fetchval("SELECT COUNT(*) FROM social_posts") == 14
    assert await db.fetchval("SELECT COUNT(*) FROM events WHERE event_type = 'x_ctf_discrepancy'") == 1
    assert await db.fetchval("SELECT COUNT(*) FROM social_post_mentions") == 2
    assert await db.fetchval("SELECT COUNT(DISTINCT run_id) FROM social_posts") == 1   # first observation kept


PROVIDER_UPSERT = (                       # 09 §2.6 verbatim, as a paid provider writes it (x.py is another module)
    "INSERT INTO social_posts (platform, post_id, author_id, author_handle, is_team_account, text, text_hash, "
    "like_count, repost_count, reply_count, quote_count, view_count, engagement_observed_at, event_at, observed_at, "
    "source) VALUES ('x',?,?,?,1,?,?,?,?,?,?,?,?,?,?,'twitterapi_io') ON CONFLICT(platform, post_id) DO UPDATE SET "
    "like_count=excluded.like_count, repost_count=excluded.repost_count, reply_count=excluded.reply_count, "
    "quote_count=excluded.quote_count, view_count=excluded.view_count, "
    "engagement_observed_at=excluded.observed_at, "
    "source=CASE WHEN social_posts.source='subnetalpha_ctf' THEN excluded.source ELSE social_posts.source END, "
    "author_id=CASE WHEN social_posts.author_id LIKE 'h:%' THEN excluded.author_id ELSE social_posts.author_id END")


@pytest.mark.asyncio
@respx.mock
async def test_provider_enriches_then_ctf_never_erases(db, http, fset):
    o64, *_ = await seed_pages(db)
    mock_pages()
    await run_ctf_crosscheck(db, http, now=NOW, run_id="r1")
    r = await db.fetchone("SELECT * FROM social_posts WHERE author_handle = 'chutes_ai' ORDER BY event_at DESC LIMIT 1")
    await db.execute(PROVIDER_UPSERT, (r["post_id"], "1234567", "chutes_ai", r["text"], r["text_hash"], 5, 2, 1, 0,
                                       900, NOW + 3600, r["event_at"], NOW + 3600))
    after = await db.fetchone("SELECT * FROM social_posts WHERE post_id = ?", (r["post_id"],))
    assert (after["source"], after["author_id"], after["like_count"]) == ("twitterapi_io", "1234567", 5)
    assert after["observed_at"] == r["observed_at"] and after["engagement_observed_at"] == NOW + 3600
    assert after["occupant_key"] == o64 and after["text"] == r["text"]
    # account now resolved: the next ctf pass writes the real author id; a re-observation never erases engagement
    await db.execute("INSERT INTO x_accounts (author_id, handle, handle_lower, kind, created_at) VALUES "
                     "('1234567', 'chutes_ai', 'chutes_ai', 'team', ?)", (NOW,))
    await run_ctf_crosscheck(db, http, now=NOW + DAY, run_id="r3")
    again = await db.fetchone("SELECT * FROM social_posts WHERE post_id = ?", (r["post_id"],))
    assert (again["source"], again["author_id"], again["like_count"], again["engagement_observed_at"]) == \
        ("twitterapi_io", "1234567", 5, NOW + 3600)
    other = await db.fetchone("SELECT author_id, source FROM social_posts WHERE author_handle = 'chutes_ai' AND post_id "
                              "<> ? LIMIT 1", (r["post_id"],))
    assert (other["author_id"], other["source"]) == ("1234567", "subnetalpha_ctf")    # placeholder replaced


@pytest.mark.asyncio
@respx.mock
async def test_gap_event_only_with_paid_provider(db, http, fset):
    await seed_pages(db)
    mock_pages()
    old = NOW - 40 * DAY
    await db.execute("INSERT INTO social_posts (platform, post_id, author_id, author_handle, is_team_account, text_hash, "
                     "event_at, observed_at, source) VALUES ('x', '1', '1234567', 'chutes_ai', 1, 'h', ?, ?, "
                     "'twitterapi_io')", (old, old))
    rep = await run_ctf_crosscheck(db, http, now=NOW, run_id="r1")                 # X inactive (default null)
    assert rep["events"] == 1                                                     # only the handle mismatch
    fset(X_ENABLED=True, X_PROVIDER="twitterapi_io", TWITTERAPI_IO_KEY="k", X_MONTHLY_CAP_USD=25.0)
    rep = await run_ctf_crosscheck(db, http, now=NOW + 60, run_id="r2")
    gaps = [json.loads(e["payload"]) for e in await db.fetchall(
        "SELECT payload FROM events WHERE event_type = 'x_ctf_discrepancy'") if '"gap"' in e["payload"]]
    assert len(gaps) == 1
    g = gaps[0]
    assert g["handle"] == "chutes_ai" and g["provider_last_post_at"] == old and g["gap_days"] > 7


@pytest.mark.asyncio
@respx.mock
async def test_fetch_failures(db, http, fset, monkeypatch):
    import app.collectors.x_ctf as X
    clock = {"t": NOW}
    monkeypatch.setattr(X, "now_ts", lambda: clock["t"])
    o64, o1, o9, o105, o40, _ = await seed_pages(db)
    mock_pages()
    respx.get(f"{SA}/beam/").mock(return_value=httpx.Response(503))
    rep = await run_ctf_crosscheck(db, http, now=NOW, run_id="r1")
    assert rep["failed"] == 1 and rep["rows"] == 14
    st = await read_page_state(db, o105)
    assert st["http_status"] == 503 and st["n_items"] == 0
    for slug in ("chutes", "apex", "fan"):
        respx.get(f"{SA}/{slug}/").mock(return_value=httpx.Response(500))
    clock["t"] = NOW + DAY
    with pytest.raises(ProviderError):                                           # every page failed → run fails
        await run_ctf_crosscheck(db, http, now=NOW + DAY, run_id="r2")
    st = await read_page_state(db, o64)
    assert st["http_status"] == 500 and st["n_matched"] == 0
    assert (st["last_feed_at"], st["fetched_at"]) == (NOW, NOW + DAY)            # carried from the good run


@pytest.mark.asyncio
@respx.mock
async def test_job_collect_note(rt):
    from app.jobs import SPEC_BY_ID
    db = rt.db
    o105 = await seed_occupant(db, 105, "Beam")
    await seed_identity(db, o105, "slug", "beam")
    await seed_identity(db, o105, "twitter_embed_id", "106")
    respx.get(f"{SA}/beam/").mock(return_value=httpx.Response(200, text=fixture_text("ctf", "no_feed.html")))
    job = XCtfCrosscheckJob(rt, SPEC_BY_ID["x_ctf_crosscheck"])
    await job.run(trigger="manual")
    row = await db.fetchone("SELECT ok, note FROM collector_runs WHERE job = 'x_ctf_crosscheck'")
    assert row["ok"] == 1 and row["note"].startswith("pages=1 with_feed=0 items=0")
