"""Spec 14 §2.8 / §9 ``test_brief``: the daily market brief on the seeded API fixture."""
from __future__ import annotations

import html
import json
import re

import pytest

from app.jobs import SPEC_BY_ID
from app.notifications.format import utf16_len
from app.util.time import now_ts
from tests.api.conftest import OCC47, OCC64, OCC9, P0, _score

pytestmark = pytest.mark.asyncio
SPEC = "ts-1.0"
HEALTH = {"collectors": {"a": {"stale": True}, "b": {"stale": False}}, "tasks": {}, "alerts": {"pending_immediate": 2}}


def _check_layout(text: str) -> None:
    assert len(text.split("\n")) <= 20
    assert utf16_len(text) <= 4096
    for blk in re.findall(r"<pre>(.*?)</pre>", text, re.S):
        for line in blk.split("\n"):
            assert len(html.unescape(line)) <= 32, line


async def _dtc(db, key: str, as_of: int, v: float) -> None:
    await db.execute("INSERT INTO metric_values (occupant_key, metric_key, as_of, spec_version, stage, value, "
                     "value_json, observed_at, source, run_id) VALUES (?, 'days_to_cull_static', ?, ?, 'raw', ?, ?, ?, "
                     "'chain', 'wb')", (key, as_of, SPEC, v, json.dumps({"status": "ok"}), as_of + 60))


async def test_brief(seeded, cfg, monkeypatch):
    from app.notifications import brief as B
    from app.notifications import telegram
    db = seeded.db
    now = now_ts()
    text = await B.render_brief(db, now=now, health=HEALTH)
    _check_layout(text)
    assert "no slot 24 h earlier" in text and "regime TRENDING" in text            # S = P0 (SN64's provisional slot)

    P = P0 - 86400
    await _score(db, OCC64, 64, P, comp=79.0, conf=0.9, tier="B")
    await _score(db, OCC47, 47, P, comp=22.0, conf=0.55, tier="D", flags=["dereg_imminent"])
    await _score(db, OCC47, 47, P0, comp=15.0, conf=0.55, tier="F", flags=["dereg_imminent"])
    await _score(db, OCC9, 9, P, comp=41.0, conf=0.3, tier="C")
    await _score(db, OCC9, 9, P0, comp=40.0, conf=0.3, tier="C")
    for key, flag, raised, cleared, active in ((OCC64, "site_down_7d", P0, None, 1),
                                               (OCC9, "silence_45d", P - 86400, P0 - 6 * 3600, 0)):
        await db.execute("INSERT INTO red_flag_state (occupant_key, flag, spec_version, active, raised_at, cleared_at, "
                         "human, sticky, last_eval_as_of, updated_at) VALUES (?,?,?,?,?,?,0,0,?,?)",
                         (key, flag, SPEC, active, raised, cleared, P0, now))
    hour = now - now % 3600
    for key, v in ((OCC47, 2.5), (OCC64, 40.0), (OCC9, 6.9)):
        await _dtc(db, key, hour, v)
    await _dtc(db, OCC64, hour - 3600, 1.0)                                              # older hour: ignored
    await db.execute("INSERT INTO wb_dereg_network_hourly (hour_ts, eta_next_registration_days, k_30, observed_at) "
                     "VALUES (?, 3.4, 4, ?)", (hour - 3 * 3600, hour - 3 * 3600 + 60))

    text = await B.render_brief(db, now=now, health=HEALTH)
    _check_layout(text)
    lines = text.split("\n")
    top = re.search(r"Top 5.*?<pre>(.*?)</pre>", text, re.S).group(1).split("\n")
    assert [int(r.split()[0]) for r in top] == [64, 47]                                 # SN9 (conf .3) excluded
    assert top[1].endswith("!") and not top[0].endswith("!")
    movers = re.search(r"movers.*?<pre>(.*?)</pre>", text, re.S).group(1).split("\n")
    assert [int(r.split()[0]) for r in movers] == [47, 64, 9]                           # |Δ| 7.0, 3.4, 1.0
    assert " -7.0 " in movers[0] and " +3.4 " in movers[1]
    tiers = next(x for x in lines if "<b>Tiers</b>" in x)
    assert tiers.index("↓ 47 D→F") < tiers.index("↑ 64 B→A")
    flags = next(x for x in lines if "<b>Flags</b>" in x)
    assert "+1 SN64 site_down_7d" in flags and "−1 SN9 silence_45d" in flags and "dereg_imminent" not in flags
    dereg = next(x for x in lines if "Dereg watch" in x)
    assert "SN47 2.5 d · SN9 6.9 d" in dereg and "SN64" not in dereg
    assert "next reg ≈ 3.4 d" in dereg and "(stale, as of" in dereg
    assert lines[-1].startswith("🩺 0/45 metrics · 1 stale job · 2 alerts pending")
    assert "risk filter" in lines[2]

    # the job: one plain send, no events row, nothing sent without a token
    sent: list[str] = []

    async def fake_send(t, **kw):
        sent.append(t)
        return True, 200
    monkeypatch.setattr(telegram, "send_message", fake_send)
    n_events = await db.fetchval("SELECT COUNT(*) FROM events", writer=True)
    cfg(MARKET_BRIEF_ENABLED=True, TELEGRAM_BOT_TOKEN="", TELEGRAM_CHAT_ID="")
    job = B.MarketBriefJob(seeded.rt, SPEC_BY_ID["market_brief_daily"])
    await job.run(trigger="manual")
    note = await db.fetchval("SELECT note FROM collector_runs WHERE job = 'market_brief_daily' ORDER BY rowid DESC "
                             "LIMIT 1", writer=True)
    assert sent == [] and job._status.last_ok and "telegram not configured" in note
    cfg(TELEGRAM_BOT_TOKEN="1:x", TELEGRAM_CHAT_ID="42")
    await job.run(trigger="manual")
    assert len(sent) == 1 and sent[0].startswith("🌅 <b>Market brief</b>")
    assert await db.fetchval("SELECT COUNT(*) FROM events", writer=True) == n_events


async def test_brief_no_scores(rt):
    from app.notifications.brief import render_brief
    text = await render_brief(rt.db, now=now_ts(), health=None)
    assert text.endswith("No scores for ts-1.0 yet.") and len(text.split("\n")) == 2


async def test_brief_job_row():
    sp = SPEC_BY_ID["market_brief_daily"]
    assert sp.owner == "14" and sp.phase == 11 and sp.ops_exempt and sp.providers == ("telegram",)
    assert sp.enabled_flags == ("MARKET_BRIEF_ENABLED",) and sp.kind == "cron"
