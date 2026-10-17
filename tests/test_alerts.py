"""test_alerts / test_dispatch / alert routing (spec 01 §9, 16 §9 test_alert_routing)."""
import json

import httpx
import pytest
import respx

import app.config as app_config
from app.alerts.dispatch import AlertDispatchJob
from app.alerts.routing import route_for
from app.jobs import SPEC_BY_ID
from app.notifications import telegram
from app.notifications.alerts import AlertLatch
from app.storage.events import emit_event
from app.util.time import now_ts


@pytest.mark.asyncio
async def test_send_alert_noop_without_token(settings_override):
    settings_override(TELEGRAM_BOT_TOKEN="", TELEGRAM_CHAT_ID="")
    assert await telegram.send_alert("hi") is False


@pytest.mark.asyncio
async def test_send_alert_posts(rt, settings_override):
    settings_override(TELEGRAM_BOT_TOKEN="123:ABC", TELEGRAM_CHAT_ID="42", TELEGRAM_ENABLED=True)
    with respx.mock(assert_all_mocked=True, assert_all_called=False) as m:
        route = m.post("https://api.telegram.org/bot123:ABC/sendMessage").mock(
            return_value=httpx.Response(200, json={"ok": True}))
        assert await telegram.send_alert("<b>x</b>") is True
        body = json.loads(route.calls[0].request.content)
        assert body["chat_id"] == "42" and body["parse_mode"] == "HTML" and body["text"] == "<b>x</b>"


@pytest.mark.asyncio
async def test_latch_semantics(db, alerts_sink):
    latch = AlertLatch(db, sink=alerts_sink)
    assert await latch.fire("service_online", "online") and alerts_sink.sent == ["online"]
    assert await db.fetchval("SELECT COUNT(*) FROM events") == 0
    p = {"collector": "x", "consecutive_failures": 5, "last_error": "e"}
    assert await latch.fire("collector_failing:x", "t", event_type="collector_failing", payload=p, dedupe_key="x|1")
    assert alerts_sink.sent == ["online"]                                    # row written, no direct send
    row = await db.fetchone("SELECT * FROM events")
    assert row["alerted_at"] is None and row["occupant_key"] is None and json.loads(row["payload"])["text"] == "t"
    assert await latch.fire("collector_failing:x", "t", event_type="collector_failing", payload=p) is False
    assert await latch.clear("collector_failing:x", "recovered", event_type="collector_recovered",
                             payload={"collector": "x", "failures": 5, "down_seconds": 60}, dedupe_key="x|1")
    assert await db.fetchval("SELECT COUNT(*) FROM events") == 2
    assert await latch.fire("disk_critical", "disk!", event_type="disk_critical", severity="critical",
                            payload={"free_gb": 0.1, "min_gb": 2, "db_gb": 1, "backups_gb": 1, "logs_gb": 0})
    assert "disk!" not in alerts_sink.sent                                    # 02 §D3: no direct path


@pytest.mark.asyncio
async def test_latch_falls_back_when_db_write_fails(db, alerts_sink):
    latch = AlertLatch(db, sink=alerts_sink)
    await latch.fire("k", "boom text", event_type="not_a_type", payload={})
    assert alerts_sink.sent == ["boom text"]


def _row(et, sev="warning", payload=None, obs=None, **kw):
    return {"id": 1, "event_type": et, "severity": sev, "payload": json.dumps(payload or {}),
            "observed_at": obs if obs is not None else now_ts(), **kw}


def test_route_for_rules():
    now = now_ts()
    assert route_for(_row("collector_failing"), now) == "immediate"
    assert route_for(_row("collector_failing", payload={"backfilled": True}), now) == "digest"
    assert route_for(_row("totally_unknown"), now) == "digest"
    assert route_for(_row("emission_disabled", "critical"), now) == "digest"
    assert route_for(_row("occupant_activated", "info"), now) == "none"
    assert route_for(_row("collector_failing", obs=now - 30 * 3600), now) == "digest_late"
    ic = {"source": "chain", "field": "github_repo"}
    assert route_for(_row("identity_changed", "warning", ic), now) == "immediate"
    assert route_for(_row("identity_changed", "info", ic), now) == "digest"
    assert route_for(_row("product_launch", "critical"), now) == "immediate"
    assert route_for(_row("product_launch", "info"), now) == "digest"
    assert route_for(_row("disk_critical", "critical"), now) == "immediate"


async def _seed(db, n=1, et="collector_failing"):
    for i in range(n):
        await emit_event(db, event_type=et, occupant_key=None, netuid=None, event_at=now_ts(), observed_at=now_ts(),
                         payload={"collector": f"c{i}", "consecutive_failures": 5, "last_error": "e"},
                         dedupe_key=f"c{i}|1", source="runtime", run_id=None)


@pytest.mark.asyncio
async def test_dispatch_sends_and_marks(rt, settings_override):
    settings_override(TELEGRAM_BOT_TOKEN="1:T", TELEGRAM_CHAT_ID="9", TELEGRAM_ENABLED=True, ALERT_BATCH_THRESHOLD=5)
    await _seed(rt.db, 2)
    await emit_event(rt.db, event_type="spend_reset", occupant_key=None, netuid=None, event_at=1, observed_at=now_ts(),
                     payload={"group": "x", "month": "2026-09"}, dedupe_key="d", source="cost", run_id=None)
    job = AlertDispatchJob(rt, SPEC_BY_ID["alert_dispatch"])
    with respx.mock(assert_all_mocked=True, assert_all_called=False) as m:
        route = m.post("https://api.telegram.org/bot1:T/sendMessage").mock(
            return_value=httpx.Response(200, json={"ok": True}))
        await job.run("cli")
        assert route.call_count == 2
    rows = await rt.db.fetchall("SELECT event_type, alerted_at, payload FROM events ORDER BY id", writer=True)
    assert all(r["alerted_at"] for r in rows)
    assert [json.loads(r["payload"])["alert_route"] for r in rows] == ["immediate", "immediate", "digest"]
    with respx.mock(assert_all_mocked=True, assert_all_called=False) as m:
        route = m.post("https://api.telegram.org/bot1:T/sendMessage").mock(return_value=httpx.Response(200))
        await job.run("cli")
        assert route.call_count == 0                                         # exactly-once


@pytest.mark.asyncio
async def test_dispatch_batch_400_and_outage(rt, settings_override):
    settings_override(TELEGRAM_BOT_TOKEN="1:T", TELEGRAM_CHAT_ID="9", TELEGRAM_ENABLED=True, ALERT_BATCH_THRESHOLD=5)
    job = AlertDispatchJob(rt, SPEC_BY_ID["alert_dispatch"])
    await _seed(rt.db, 7)
    with respx.mock(assert_all_mocked=True, assert_all_called=False) as m:
        route = m.post("https://api.telegram.org/bot1:T/sendMessage").mock(return_value=httpx.Response(200))
        await job.run("cli")
        assert route.call_count == 1 and "7 alerts" in json.loads(route.calls[0].request.content)["text"]
    await emit_event(rt.db, event_type="collector_failing", occupant_key=None, netuid=None, event_at=1,
                     observed_at=now_ts(), payload={"collector": "p", "consecutive_failures": 5, "last_error": "<x"},
                     dedupe_key="p|1", source="runtime", run_id=None)
    with respx.mock(assert_all_mocked=True, assert_all_called=False) as m:
        m.post("https://api.telegram.org/bot1:T/sendMessage").mock(return_value=httpx.Response(400))
        await job.run("cli")
    p = json.loads(await rt.db.fetchval("SELECT payload FROM events WHERE dedupe_key='p|1'", writer=True))
    assert p["alert_failed"] is True
    await emit_event(rt.db, event_type="collector_failing", occupant_key=None, netuid=None, event_at=1,
                     observed_at=now_ts(), payload={"collector": "q", "consecutive_failures": 5, "last_error": "e"},
                     dedupe_key="q|1", source="runtime", run_id=None)
    with respx.mock(assert_all_mocked=True, assert_all_called=False) as m:
        m.post("https://api.telegram.org/bot1:T/sendMessage").mock(return_value=httpx.Response(502))
        await job.run("cli")
    assert await rt.db.fetchval("SELECT alerted_at FROM events WHERE dedupe_key='q|1'", writer=True) is None


@pytest.mark.asyncio
async def test_dispatch_unconfigured_leaves_rows(rt, settings_override):
    settings_override(TELEGRAM_BOT_TOKEN="", TELEGRAM_CHAT_ID="")
    await _seed(rt.db, 1)
    await AlertDispatchJob(rt, SPEC_BY_ID["alert_dispatch"]).run("cli")
    assert await rt.db.fetchval("SELECT alerted_at FROM events", writer=True) is None


def test_alert_dispatch_declared_ops_exempt():
    sp = SPEC_BY_ID["alert_dispatch"]
    assert sp.phase == 0 and sp.ops_exempt


def test_templates_render():
    from app.alerts.templates import render
    t = render(_row("collector_failing", payload={"collector": "tmc_daily", "consecutive_failures": 5,
                                                  "last_error": "<boom>"}))
    assert "tmc_daily" in t and "&lt;boom&gt;" in t
    t = render(_row("spend_80pct", payload={"group": "x", "month": "2026-09", "pct": 0.81, "unit": "usd",
                                            "usd_mtd": 20.3, "cap_usd": 25}))
    assert "81%" in t and "$20.30" in t
    g = render(_row("gate_model_broken", payload={"as_of": 1, "consecutive_days": 7, "residual_pp": 9.1}))
    assert "gate_model_broken" in g
