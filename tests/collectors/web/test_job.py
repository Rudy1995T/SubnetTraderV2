"""Spec 07 §9 integration: one web_check pass over seeded watch_channels with respx-mocked sites."""
from __future__ import annotations

import json

import httpx
import pytest
import respx

from app.collectors.web.check import run_check
from app.collectors.web.fetch import TlsInfo
from app.collectors.web.wayback import CDX_URL
from tests.collectors.web.conftest import DAY, NOW, fixture, seed_occupant, seed_watch, web_settings

HTML = {"content-type": "text/html; charset=utf-8"}
CDX_FOUND = [["timestamp", "original", "statuscode"], ["20210305123456", "https://beam.example/", "200"]]


def page(name: str, status: int = 200, headers: dict | None = None) -> httpx.Response:
    return httpx.Response(status, headers={**HTML, **(headers or {})}, content=fixture(name).encode())


async def seed_world(db):
    a = await seed_occupant(db, 1)
    await seed_watch(db, a, "website", "https://x.com/subnet1")                 # excluded only → no website
    b = await seed_occupant(db, 2)
    await seed_watch(db, b, "website", "https://beam.example")
    await seed_watch(db, b, "docs", "https://docs.beam.example")
    c = await seed_occupant(db, 3)
    d = await seed_occupant(db, 4)
    await seed_watch(db, c, "website", "https://studio.example")
    await seed_watch(db, d, "website", "https://studio.example")
    root = await seed_occupant(db, 0)
    await seed_watch(db, root, "website", "https://root.example")              # netuid 0 never checked
    return a, b, c, d


def routes(robots_404: bool = True):
    respx.get(url__regex=r"https://[^/]+/robots\.txt").mock(return_value=httpx.Response(404))
    landing = respx.get("https://beam.example").mock(return_value=page("landing_docs_link.html"))
    docs = respx.get("https://docs.beam.example").mock(return_value=page("landing_ok.html"))
    studio = respx.get("https://studio.example").mock(return_value=page("landing_ok.html"))
    cdx = respx.get(CDX_URL).mock(return_value=httpx.Response(200, json=CDX_FOUND))
    return landing, docs, studio, cdx


@pytest.mark.asyncio
@respx.mock
async def test_one_pass_rows_dedupe_and_due(db, make_ctx, mock_net):
    a, b, c, d = await seed_world(db)
    landing, docs, studio, cdx = routes()
    rep = await run_check(make_ctx())
    assert rep.targets["no_website"] == 1 and rep.targets["excluded"] == 1
    assert landing.call_count == 1 and docs.call_count == 1 and studio.call_count == 1   # one GET per URL
    rows = await db.fetchall("SELECT * FROM web_checks ORDER BY occupant_key, kind")
    assert [(r["occupant_key"], r["kind"]) for r in rows] == [(b, "docs"), (b, "website"), (c, "website"),
                                                              (d, "website")]
    assert {r["status_class"] for r in rows} == {"up_2xx"}
    shared = [r for r in rows if r["url"] == "https://studio.example"]
    assert shared[0]["content_hash"] == shared[1]["content_hash"] and shared[0]["run_id"] == shared[1]["run_id"]
    w = next(r for r in rows if r["kind"] == "website" and r["occupant_key"] == b)
    assert w["tls_ok"] == 1 and w["tls_days_left"] == 200 and w["content_text"] and w["change_ratio"] is None
    assert w["wayback_first_capture"] == 1614947696 and w["wayback_checked_at"] == int(NOW)
    assert w["check_day_ts"] == NOW - NOW % DAY and w["extractor"] in ("trafilatura", "readability", "plain")
    dw = next(r for r in rows if r["kind"] == "docs")
    assert dw["wayback_checked_at"] is None                                       # never for docs
    assert cdx.call_count == 2                                                    # beam + studio targets, once each
    # same day again: nothing due
    rep2 = await run_check(make_ctx())
    assert rep2.urls_due == 0 and rep2.rows == 0
    # +24 h: all due again; wayback cached (no CDX call), capture copied forward, text unchanged → NULL
    rep3 = await run_check(make_ctx(now=NOW + DAY))
    assert rep3.rows == 4 and cdx.call_count == 2                                 # declared docs: no candidate row
    w2 = await db.fetchone("SELECT * FROM web_checks WHERE occupant_key = ? AND kind = 'website' "
                           "ORDER BY event_at DESC LIMIT 1", (b,))
    assert w2["wayback_first_capture"] == 1614947696 and w2["content_text"] is None and w2["change_ratio"] == 1.0


@pytest.mark.asyncio
@respx.mock
async def test_force_and_dry_run(db, make_ctx, mock_net):
    await seed_world(db)
    routes()
    await run_check(make_ctx())
    seen = []
    rep = await run_check(make_ctx(dry_run=True), force=True, on_row=seen.append)
    assert len(seen) == 4 and rep.rows == 0
    rep = await run_check(make_ctx(), force=True)
    assert rep.rows == 0 and rep.skipped_duplicate == 4


@pytest.mark.asyncio
@respx.mock
async def test_docs_autodiscovery_lifecycle(db, make_ctx, mock_net):
    _, b, _, _ = await seed_world(db)
    await db.execute("UPDATE watch_channels SET retired_at = ? WHERE platform = 'docs'", (NOW - DAY,))
    routes()
    await run_check(make_ctx())
    rows = await db.fetchall("SELECT * FROM watch_channels WHERE source = 'web_autodiscover'")
    assert len(rows) == 1
    r = rows[0]
    assert (r["occupant_key"], r["platform"], r["handle"], r["priority"], r["verified_at"]) == \
        (b, "docs", "https://beam.example/docs/getting-started", 90, None)
    w = await db.fetchone("SELECT docs_candidate_url FROM web_checks WHERE occupant_key = ? AND kind = 'website'", (b,))
    assert w["docs_candidate_url"] == "https://beam.example/docs/getting-started"
    # next day: still linked → last_seen refreshed; a declared docs row then supersedes the guess
    respx.get("https://beam.example/docs/getting-started").mock(return_value=page("landing_ok.html"))
    await run_check(make_ctx(now=NOW + DAY))
    r2 = await db.fetchone("SELECT last_seen_at, retired_at FROM watch_channels WHERE source = 'web_autodiscover'")
    assert r2["last_seen_at"] == NOW + DAY and r2["retired_at"] is None
    await db.execute("UPDATE watch_channels SET retired_at = NULL WHERE platform = 'docs' AND source = 'chain'")
    await run_check(make_ctx(now=NOW + 2 * DAY))
    r3 = await db.fetchone("SELECT retired_at FROM watch_channels WHERE source = 'web_autodiscover'")
    assert r3["retired_at"] == NOW + 2 * DAY


@pytest.mark.asyncio
@respx.mock
async def test_robots_disallow_head_only(db, make_ctx, mock_net):
    okey = await seed_occupant(db, 7)
    await seed_watch(db, okey, "website", "https://shy.example")
    respx.get("https://shy.example/robots.txt").mock(return_value=httpx.Response(
        200, headers={"content-type": "text/plain"}, content=b"User-agent: TaoSentinel\nDisallow: /\n"))
    head = respx.head("https://shy.example").mock(return_value=httpx.Response(200))
    get = respx.get("https://shy.example")
    respx.get(CDX_URL).mock(return_value=httpx.Response(200, json=[]))
    await run_check(make_ctx())
    r = await db.fetchone("SELECT * FROM web_checks")
    assert head.call_count == 1 and get.call_count == 0
    assert r["robots_allowed"] == 0 and r["content_hash"] is None and r["status_class"] == "up_2xx"
    assert r["wayback_first_capture"] is None and r["wayback_checked_at"] == int(NOW)     # "no capture"


@pytest.mark.asyncio
@respx.mock
async def test_network_gate(db, make_ctx, mock_net):
    keys = []
    for i in range(10):
        k = await seed_occupant(db, 10 + i)
        keys.append(k)
        await seed_watch(db, k, "website", f"https://s{i}.example")
    respx.get(url__regex=r"https://[^/]+/robots\.txt").mock(return_value=httpx.Response(404))
    respx.get(CDX_URL).mock(return_value=httpx.Response(200, json=[]))
    for i in range(10):
        if i < 7:
            respx.get(f"https://s{i}.example").mock(side_effect=httpx.ConnectTimeout("t"))
        else:
            respx.get(f"https://s{i}.example").mock(return_value=page("landing_ok.html"))
    mock_net["preflight"] = False
    rep = await run_check(make_ctx())
    assert rep.network_suspect and rep.rows == 0                                  # preflight: nothing written
    mock_net["preflight"] = True
    calls = {"n": 0}
    from app.collectors.web import check

    async def flaky_preflight(hosts, timeout_s):
        calls["n"] += 1
        return calls["n"] == 1                       # run-start probe ok, batch re-probe fails
    import pytest as _p
    mp = _p.MonkeyPatch()
    mp.setattr(check, "network_preflight", flaky_preflight)
    try:
        rep = await run_check(make_ctx())
    finally:
        mp.undo()
    assert rep.network_suspect and rep.rows == 3                                  # only the 3 good rows
    due = await check.due_targets(db, (await check.load_targets(db, web_settings()))[0], int(NOW), web_settings())
    assert len(due) == 7                                                          # failing targets stay due
    # genuine failures (network fine): written as timeout rows
    rep = await run_check(make_ctx())
    assert rep.rows == 7 and rep.status_counts["timeout"] == 7


@pytest.mark.asyncio
@respx.mock
async def test_in_run_retry(db, make_ctx, mock_net):
    okey = await seed_occupant(db, 9)
    await seed_watch(db, okey, "website", "https://wobbly.example")
    respx.get("https://wobbly.example/robots.txt").mock(return_value=httpx.Response(404))
    respx.get(CDX_URL).mock(return_value=httpx.Response(200, json=[]))
    respx.get("https://wobbly.example").mock(side_effect=[httpx.ReadTimeout("t"), page("landing_ok.html")])
    ctx = make_ctx(web_settings(WEB_RETRY_DELAY_S=1))
    ctx.clock = __import__("time").time
    ctx.started_at = int(ctx.clock())
    ctx.deadline = ctx.clock() + 600
    rep = await run_check(ctx)
    r = await db.fetchone("SELECT attempts, status_class FROM web_checks")
    assert rep.retried == 1 and r["attempts"] == 2 and r["status_class"] == "up_2xx"


@pytest.mark.asyncio
@respx.mock
async def test_per_check_events(db, make_ctx, mock_net):
    okey = await seed_occupant(db, 5)
    await seed_watch(db, okey, "website", "https://apex.example")
    respx.get(url__regex=r"https://[^/]+/robots\.txt").mock(return_value=httpx.Response(404))
    respx.get(CDX_URL).mock(return_value=httpx.Response(200, json=CDX_FOUND))
    respx.get("https://apex.example").mock(return_value=page("landing_ok.html"))
    mock_net["tls"]["apex.example"] = TlsInfo(days_left=10, not_after=int(NOW) + 10 * DAY, issuer="R11",
                                              hostname_match=True, verified=True, reason="expiring", handshakes=1)
    await run_check(make_ctx())
    # day 2: 301 to a casino on another domain → hijacked_suspect (+ cross-domain jump, new scam text, reset)
    respx.get("https://apex.example").mock(return_value=httpx.Response(301, headers={"location": "https://casino-win.example/"}))
    respx.get("https://casino-win.example/").mock(return_value=page("hijacked_casino.html"))
    await run_check(make_ctx(now=NOW + DAY))
    # day 3: parked
    respx.get("https://apex.example").mock(return_value=page("parked_sedo.html"))
    await run_check(make_ctx(now=NOW + 2 * DAY))
    rows = await db.fetchall("SELECT status_class, tls_ok FROM web_checks ORDER BY event_at")
    assert [r["status_class"] for r in rows] == ["up_2xx", "hijacked_suspect", "parked"]
    assert rows[0]["tls_ok"] == 0
    ev = {r["event_type"]: r for r in await db.fetchall("SELECT * FROM events")}
    assert set(ev) >= {"tls_expiring_14d", "site_hijack_suspect", "site_parked"}
    assert "site_content_reset" not in ev                     # needs an up outcome
    assert ev["tls_expiring_14d"]["severity"] == "warning" and ev["site_hijack_suspect"]["severity"] == "critical"
    assert ev["tls_expiring_14d"]["dedupe_key"] == f"{okey}|https://apex.example|{int(NOW) + 10 * DAY}"
    assert ev["site_parked"]["source"] == "web"
    assert "cross_domain_new" in json.loads(ev["site_hijack_suspect"]["payload"])["hijack_signals"]


@pytest.mark.asyncio
@respx.mock
async def test_cross_domain_rebrand_is_redirect_ok(db, make_ctx, mock_net):
    okey = await seed_occupant(db, 6)
    await seed_watch(db, okey, "website", "https://oldbrand.example")
    respx.get(url__regex=r"https://[^/]+/robots\.txt").mock(return_value=httpx.Response(404))
    respx.get(CDX_URL).mock(return_value=httpx.Response(200, json=[]))
    respx.get("https://oldbrand.example").mock(return_value=httpx.Response(301, headers={"location": "https://newbrand.example/"}))
    respx.get("https://newbrand.example/").mock(return_value=page("landing_ok.html"))
    await run_check(make_ctx())
    r = await db.fetchone("SELECT status_class, cross_domain, redirect_hops FROM web_checks")
    assert (r["status_class"], r["cross_domain"], r["redirect_hops"]) == ("redirect_ok", 1, 1)
    e = await db.fetchone("SELECT payload FROM events WHERE event_type = 'site_cross_domain_redirect'")
    assert json.loads(e["payload"])["final_domain"] == "newbrand.example"


@pytest.mark.asyncio
@respx.mock
async def test_dns_fail_and_internal_error(db, make_ctx, mock_net, monkeypatch):
    k1 = await seed_occupant(db, 11)
    await seed_watch(db, k1, "website", "https://gone.example")
    k2 = await seed_occupant(db, 12)
    await seed_watch(db, k2, "website", "https://buggy.example")
    mock_net["dns"]["gone.example"] = "nxdomain"
    respx.get(url__regex=r"https://[^/]+/robots\.txt").mock(return_value=httpx.Response(404))
    respx.get(CDX_URL).mock(return_value=httpx.Response(200, json=[]))
    respx.get("https://buggy.example").mock(return_value=page("landing_ok.html"))
    from app.collectors.web import check

    def boom(*a, **k):
        raise ValueError("extractor exploded")
    monkeypatch.setattr(check, "_analyse_html", boom)
    rep = await run_check(make_ctx())
    rows = {r["url"]: r for r in await db.fetchall("SELECT * FROM web_checks")}
    assert rows["https://gone.example"]["status_class"] == "dns_fail"
    assert rows["https://gone.example"]["tls_ok"] is None
    b = rows["https://buggy.example"]
    assert b["status_class"] == "conn_error" and b["error"].startswith("internal:ValueError")
    assert rep.internal_errors == 1


@pytest.mark.asyncio
async def test_job_pause_emergency_file(rt, tmp_root):
    from app.collectors.web.jobs import WebCheckJob
    from app.jobs import SPEC_BY_ID
    import app.config as app_config
    app_config.settings.WEB_ENABLED = True
    job = WebCheckJob(rt, SPEC_BY_ID["web_check"])
    (tmp_root / "data" / "control" / "web_check.paused").write_text("")
    await job.run(trigger="manual")
    assert job.status()["paused"] == "emergency_file"
    n = await rt.db.fetchval("SELECT COUNT(*) FROM collector_runs WHERE job = 'web_check'")
    assert n == 0


@pytest.mark.asyncio
@respx.mock
async def test_content_reset_event(db, make_ctx, mock_net):
    okey = await seed_occupant(db, 8)
    await seed_watch(db, okey, "website", "https://apex.example")
    respx.get(url__regex=r"https://[^/]+/robots\.txt").mock(return_value=httpx.Response(404))
    respx.get(CDX_URL).mock(return_value=httpx.Response(200, json=[]))
    respx.get("https://apex.example").mock(return_value=page("landing_ok.html"))
    await run_check(make_ctx())
    respx.get("https://apex.example").mock(return_value=page("landing_docs_link.html"))
    await run_check(make_ctx(now=NOW + DAY))
    r = await db.fetchone("SELECT change_ratio, content_text FROM web_checks ORDER BY event_at DESC LIMIT 1")
    assert r["change_ratio"] < 0.2 and r["content_text"]
    e = await db.fetchone("SELECT payload FROM events WHERE event_type = 'site_content_reset'")
    assert e is not None and json.loads(e["payload"])["prev_len"] >= 300


@pytest.mark.asyncio
@respx.mock
async def test_job_collect_writes_collector_runs_and_health(rt, mock_net):
    from app.collectors.web.jobs import WebCheckJob, WebMetricsJob
    from app.jobs import SPEC_BY_ID
    import app.config as app_config
    app_config.settings.WEB_ENABLED = True
    okey = await seed_occupant(rt.db, 5)
    await seed_watch(rt.db, okey, "website", "https://apex.example")
    respx.get("https://apex.example/robots.txt").mock(return_value=httpx.Response(404))
    respx.get(CDX_URL).mock(return_value=httpx.Response(200, json=CDX_FOUND))
    respx.get("https://apex.example").mock(return_value=page("landing_ok.html"))
    job = WebCheckJob(rt, SPEC_BY_ID["web_check"])
    await job.run(trigger="manual")
    run = await rt.db.fetchone("SELECT ok, rows, note FROM collector_runs WHERE job = 'web_check'")
    assert run["ok"] == 1 and run["rows"] == 1 and "checked=1/1" in run["note"]
    extra = job.status()["extra"]
    assert extra["urls_due"] == 0 and extra["urls_checked"] == 1 and extra["status_counts"] == {"up_2xx": 1}
    assert extra["wayback_last_ok"] is not None
    assert await rt.db.fetchval("SELECT value_json FROM job_state WHERE job = 'web_check' AND key = 'wayback_last_ok'")
    mj = WebMetricsJob(rt, SPEC_BY_ID["web_metrics"])
    await mj.run(trigger="manual")
    mrun = await rt.db.fetchone("SELECT ok, rows FROM collector_runs WHERE job = 'web_metrics'")
    assert mrun["ok"] == 1 and mrun["rows"] == 6
