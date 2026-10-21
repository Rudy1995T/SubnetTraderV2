"""Spec 07 §2.8–§2.9, §3.5, §4: the six P3 metrics, streak events, product_status, the writer."""
from __future__ import annotations

import json

import pytest

from app.features import web_metrics as W
from tests.collectors.web.conftest import seed_occupant, seed_watch

DAY = 86400
D = 1_792_195_200                 # 2026-10-17T00:00Z
T = D + 300                       # the 00:05Z run
URL = "https://apex.example"
CLS = {"U": ("up_2xx", 200), "D": ("server_error", 502), "T": ("timeout", None), "B": ("bot_challenge", 403)}


async def add_row(db, okey: str, url: str, day: int, sc: str, *, kind: str = "website", primary: int = 1,
                  http: int | None = 200, **cols) -> None:
    base = {"occupant_key": okey, "netuid": int(okey.split(":")[0]), "url": url, "kind": kind, "is_primary": primary,
            "source": "chain", "event_at": day + 60, "observed_at": day + 120, "check_day_ts": day, "run_id": "r",
            "status_class": sc, "http_status": http, "tls_ok": 1, "tls_days_left": 200, "robots_allowed": 1,
            "extractor": "trafilatura", "is_spa": 0, "content_len": 900, "content_hash": "h"}
    base.update(cols)
    ks = ",".join(base)
    await db.execute(f"INSERT INTO web_checks ({ks}) VALUES ({','.join('?' * len(base))})", tuple(base.values()))


async def days(db, okey: str, s: str, *, url: str = URL, end: int = D, **cols) -> None:
    """One row per char ending the day before ``end``: U up, D down (502), T timeout, B bot challenge, . none."""
    n = len(s)
    for i, ch in enumerate(s):
        if ch == ".":
            continue
        sc, http = CLS[ch]
        await add_row(db, okey, url, end - (n - i) * DAY, sc, http=http, **cols)


async def occ(db, netuid: int = 64, *, website: str | None = URL) -> str:
    okey = await seed_occupant(db, netuid)
    if website:
        await seed_watch(db, okey, "website", website, first_seen=D - 400 * DAY)
    return okey


async def metrics(db, okey: str, t: int = T) -> dict:
    W.clear_cache()
    cfg = W.WebMetricsConfig.from_settings()
    inp = await W.load_web_inputs(db, okey, t, cfg)
    return W.derive_web_metrics(inp, cfg)


# ── site_uptime_30d ──────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_uptime_basic(db):
    o = await occ(db)
    await days(db, o, "U" * 27 + "DDD")
    m = await metrics(db, o)
    assert m["site_uptime_30d"].value == pytest.approx(27 / 30) and m["site_uptime_30d"].status == "ok"


@pytest.mark.asyncio
async def test_uptime_single_timeout_excluded_double_counts(db):
    o = await occ(db)
    await days(db, o, "U" * 28 + "TU")
    assert (await metrics(db, o))["site_uptime_30d"].value == 1.0
    o2 = await occ(db, 65)
    await days(db, o2, "U" * 27 + "TTU")
    r = (await metrics(db, o2))["site_uptime_30d"]
    assert r.value == pytest.approx(28 / 29) and r.value_json["indeterminate"] == 1


@pytest.mark.asyncio
async def test_uptime_failures_and_absence(db):
    o = await occ(db)
    await days(db, o, "UU")
    r = (await metrics(db, o))["site_uptime_30d"]
    assert r.status == "failed" and r.mapped_json()["reason"] == "insufficient_checks"
    nob = await occ(db, 66, website=None)
    await seed_watch(db, nob, "website", "https://x.com/sn66", first_seen=D - 400 * DAY)
    m = await metrics(db, nob)
    assert m["site_uptime_30d"].status == "absent" and m["site_uptime_30d"].value == 0.0
    assert m["site_uptime_30d"].value_json["excluded"] == ["https://x.com/sn66"]
    assert {m[k].status for k in ("tls_ok", "site_age", "site_freshness")} == {"not_applicable"}
    b = await occ(db, 67)
    await days(db, b, "B" * 10)
    assert (await metrics(db, b))["site_uptime_30d"].status == "failed"
    stale = await occ(db, 68)
    await days(db, stale, "U" * 20 + ".....")                  # web_check paused for 5 days
    m = await metrics(db, stale)
    for k in ("site_uptime_30d", "tls_ok"):
        assert m[k].mapped_json()["reason"] == "no_recent_check"


@pytest.mark.asyncio
async def test_internal_rows_never_down(db):
    o = await occ(db)
    await days(db, o, "U" * 10)
    await add_row(db, o, URL, D - 11 * DAY, "server_error")
    await add_row(db, o, URL, D - 12 * DAY, "conn_error", error="internal:ValueError: x")
    r = (await metrics(db, o))["site_uptime_30d"]
    assert r.value_json["down"] == 1 and r.value_json["indeterminate"] == 1


# ── streak events ────────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_site_down_7d_once_and_per_occupant(db):
    a = await occ(db, 70)
    b = await occ(db, 71)
    for o in (a, b):
        await days(db, o, "UUU" + "D" * 7)
    out = await W.run_web_metrics(db, t_eval=T, run_id="m1")
    assert out["events"] == 2
    ev = await db.fetchall("SELECT * FROM events WHERE event_type = 'site_down_7d' ORDER BY occupant_key")
    assert [e["occupant_key"] for e in ev] == [a, b]
    p = json.loads(ev[0]["payload"])
    assert p["streak_days"] == 7 and ev[0]["severity"] == "critical" and ev[0]["source"] == "web"
    assert ev[0]["dedupe_key"] == f"{a}|{URL}|{(D - 7 * DAY) // DAY}"
    out = await W.run_web_metrics(db, t_eval=T + 3600, run_id="m2")
    assert out["events"] == 0                                  # emit_event dedupe


@pytest.mark.asyncio
async def test_streak_across_timeout_and_recovery(db):
    t_after_d = await occ(db, 69)
    await days(db, t_after_d, "DDDTDDD")                       # a timeout after a down day is down (§2.4)
    out = await W.run_web_metrics(db, t_eval=T, run_id="m0")
    assert out["events"] == 1
    o = await occ(db)
    await days(db, o, "DDDBDDD")
    out = await W.run_web_metrics(db, t_eval=T, run_id="m1")
    assert out["events"] == 0                                  # B neither breaks nor extends: 6 downs
    await add_row(db, o, URL, D, "server_error", http=502)
    out = await W.run_web_metrics(db, t_eval=T + DAY, run_id="m2")
    assert out["events"] == 1
    await add_row(db, o, URL, D + DAY, "up_2xx")
    out = await W.run_web_metrics(db, t_eval=T + 2 * DAY, run_id="m3")
    rec = await db.fetchone("SELECT payload FROM events WHERE event_type = 'site_recovered'")
    p = json.loads(rec["payload"])
    assert out["events"] == 1 and p["reason"] == "up" and p["down_days"] == 7


@pytest.mark.asyncio
async def test_recovered_on_target_change(db):
    o = await occ(db)
    await days(db, o, "D" * 7)
    await W.run_web_metrics(db, t_eval=T, run_id="m1")
    await db.execute("UPDATE watch_channels SET retired_at = ? WHERE handle = ?", (T + 100, URL))
    await seed_watch(db, o, "website", "https://newapex.example", first_seen=T + 100)
    await W.run_web_metrics(db, t_eval=T + DAY, run_id="m2")
    rec = await db.fetchone("SELECT payload FROM events WHERE event_type = 'site_recovered'")
    assert json.loads(rec["payload"]) == {"url": URL, "down_days": 7, "reason": "target_changed"}


@pytest.mark.asyncio
async def test_docs_gone(db):
    o = await occ(db)
    await seed_watch(db, o, "docs", "https://docs.apex.example", first_seen=D - 400 * DAY)
    await days(db, o, "UUUU")
    await days(db, o, "UUDD", url="https://docs.apex.example", kind="docs")
    m = await metrics(db, o)
    assert m["docs_present"].value == 0.0 and m["docs_present"].mapped_json()["reason"] == "docs_down"
    out = await W.run_web_metrics(db, t_eval=T, run_id="m1")
    e = await db.fetchone("SELECT payload FROM events WHERE event_type = 'docs_gone'")
    assert json.loads(e["payload"])["streak_days"] == 2 and out["events"] == 1


# ── tls / age / freshness / docs / product ───────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_tls_ok(db):
    o = await occ(db)
    await days(db, o, "UU", tls_ok=0, tls_days_left=10)
    r = (await metrics(db, o))["tls_ok"]
    assert r.status == "ok" and r.value == 0.0 and r.mapped_json()["reason"] == "expiring"
    o2 = await occ(db, 65)
    await add_row(db, o2, URL, D - 2 * DAY, "up_2xx", tls_ok=0, tls_days_left=None)
    assert (await metrics(db, o2))["tls_ok"].mapped_json()["reason"] == "no_tls"
    await add_row(db, o2, URL, D - DAY, "tls_fail", http=None, tls_ok=0, tls_days_left=None)
    assert (await metrics(db, o2))["tls_ok"].mapped_json()["reason"] == "tls_fail"
    o5 = await occ(db, 69)
    await days(db, o5, "U", end=D - DAY)
    await add_row(db, o5, URL, D - DAY, "tls_fail", http=None, tls_ok=0, tls_days_left=-13)
    assert (await metrics(db, o5))["tls_ok"].mapped_json()["reason"] == "expired"      # chronollm, live
    o3 = await occ(db, 66)
    await days(db, o3, "UT", tls_ok=None)
    assert (await metrics(db, o3))["tls_ok"].mapped_json()["reason"] == "tls_not_evaluated"
    o4 = await occ(db, 67)
    await days(db, o4, "UU")
    assert (await metrics(db, o4))["tls_ok"].value == 1.0


@pytest.mark.asyncio
async def test_site_age(db):
    o = await occ(db)
    await days(db, o, "UU", wayback_first_capture=D - 400 * DAY, wayback_checked_at=D - 2 * DAY)
    r = (await metrics(db, o))["site_age"]
    assert r.value == 400 and r.value_json["source"] == "wayback"
    o2 = await occ(db, 65)
    await days(db, o2, "U" * 20, wayback_checked_at=D - 20 * DAY)
    r = (await metrics(db, o2))["site_age"]
    assert r.value == pytest.approx(20, abs=0.01) and r.value_json["source"] == "self_observed"
    o3 = await occ(db, 66)
    await days(db, o3, "UU")
    assert (await metrics(db, o3))["site_age"].mapped_json()["reason"] == "wayback_unavailable"


@pytest.mark.asyncio
async def test_site_freshness(db):
    o = await occ(db)
    await days(db, o, "U" * 5, change_ratio=1.0, end=D)
    await add_row(db, o, URL, D - 120 * DAY, "up_2xx", change_ratio=0.5)
    r = (await metrics(db, o))["site_freshness"]
    assert r.value == pytest.approx(120, abs=0.01) and r.value_json["baseline"] is False
    o2 = await occ(db, 65)
    await days(db, o2, "UUUUU")
    r = (await metrics(db, o2))["site_freshness"]
    assert r.value == pytest.approx(5, abs=0.01) and r.value_json["baseline"] is True
    o3 = await occ(db, 66)
    await days(db, o3, "UU", is_spa=1, content_len=120)
    assert (await metrics(db, o3))["site_freshness"].status == "not_applicable"
    o4 = await occ(db, 67)
    await days(db, o4, "UU", robots_allowed=0, content_hash=None)
    assert (await metrics(db, o4))["site_freshness"].mapped_json()["reason"] == "robots_head_only"


@pytest.mark.asyncio
async def test_docs_present(db):
    o = await occ(db)
    await seed_watch(db, o, "docs", "https://docs.apex.example", first_seen=D - 400 * DAY)
    await days(db, o, "UU")
    await days(db, o, "UU", url="https://docs.apex.example", kind="docs")
    assert (await metrics(db, o))["docs_present"].value == 1.0
    o2 = await occ(db, 65)
    await days(db, o2, "UU")
    r = (await metrics(db, o2))["docs_present"]
    assert r.status == "absent" and r.value == 0.0 and r.absence_reason == "no_docs_link"
    o3 = await occ(db, 66)
    await seed_watch(db, o3, "docs", "https://docs.x.example", first_seen=D - 400 * DAY)
    await days(db, o3, "UU")
    await days(db, o3, "T", url="https://docs.x.example", kind="docs")
    assert (await metrics(db, o3))["docs_present"].mapped_json()["reason"] == "docs_undetermined"
    o4 = await occ(db, 67)
    await days(db, o4, "DD")
    assert (await metrics(db, o4))["docs_present"].status == "failed"
    o5 = await occ(db, 68, website=None)
    assert (await metrics(db, o5))["docs_present"].status == "not_applicable"


async def note(db, okey: str, value: str | None, at: int) -> None:
    await db.execute("INSERT INTO manual_notes (occupant_key, kind, value, evidence_url, created_at, created_by) "
                     "VALUES (?,?,?,?,?,?)", (okey, "product_status", value, "https://e.example" if value else None,
                                              at, "test"))


@pytest.mark.asyncio
async def test_product_status(db):
    o = await occ(db)
    r = (await metrics(db, o))["product_status"]
    assert r.status == "failed" and r.mapped_json()["reason"] == "unset"
    await note(db, o, "beta", T - 100)
    await note(db, o, "live", T - 50)
    await note(db, o, "none", T + 50)                          # after T_eval: invisible (PIT)
    r = (await metrics(db, o))["product_status"]
    vj = r.mapped_json()
    assert r.value == 1.0 and vj["product_status"] == "live" and vj["status"] == "ok"
    await note(db, o, None, T - 10)
    assert (await metrics(db, o))["product_status"].mapped_json()["reason"] == "cleared"
    o2 = await occ(db, 65)
    await note(db, o2, "none", T - 10)
    r = (await metrics(db, o2))["product_status"]
    assert r.status == "ok" and r.value == 0.0


# ── writer ───────────────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_writer_first_success_and_replay(db):
    o = await occ(db)
    await days(db, o, "UU")                                    # uptime failed (insufficient)
    out = await W.run_web_metrics(db, t_eval=T, run_id="m1")
    assert out["rows"] == 6
    rows = {r["metric_key"]: r for r in await db.fetchall("SELECT * FROM metric_values WHERE source = 'web'")}
    assert set(rows) == set(W.P3_KEYS)
    assert all(r["as_of"] == D and r["stage"] == "raw" and r["spec_version"] == "ts-1.0" for r in rows.values())
    assert rows["site_uptime_30d"]["status"] == "failed" and rows["site_uptime_30d"]["value"] is None
    assert rows["tls_ok"]["status"] == "ok"
    await days(db, o, "U", end=D - 2 * DAY)                    # a third check day appears (late data)
    await W.run_web_metrics(db, t_eval=T + 6 * 3600, run_id="m2")
    rows2 = {r["metric_key"]: r for r in await db.fetchall("SELECT * FROM metric_values WHERE source = 'web'")}
    assert rows2["site_uptime_30d"]["status"] == "ok" and rows2["site_uptime_30d"]["run_id"] == "m2"
    assert rows2["tls_ok"]["run_id"] == "m1"                   # ok rows are never replaced (B4)
    await W.run_web_metrics(db, t_eval=D - DAY + 300, run_id="replay", observed_at=T + 7200, backfilled=True)
    rp = await db.fetchall("SELECT backfilled, value_json FROM metric_values WHERE as_of = ?", (D - DAY,))
    assert rp and all(r["backfilled"] == 1 for r in rp)


@pytest.mark.asyncio
async def test_c5_producer_fns(db):
    from datetime import datetime, timezone
    o = await occ(db)
    await days(db, o, "U" * 5)
    r = await W.compute_site_uptime_30d(db, o, datetime.fromtimestamp(T, timezone.utc))
    assert r.value == 1.0
    r = await W.compute_product_status(db, o, datetime.fromtimestamp(T, timezone.utc))
    assert r.status == "failed"
