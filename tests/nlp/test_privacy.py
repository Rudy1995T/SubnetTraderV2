"""Spec 10 §9 test_privacy: no text-less announcement, no third-party Discord row, no bot-filtered X post and no deleted
announcement reaches a batch request (asserted on the fake client's captured payloads)."""
from __future__ import annotations

import pytest

import app.config as app_config
from app.nlp.llm_classify import LlmClassifier
from tests.nlp.fakes import DAY, T0, FakeAnthropic, FakeGovernor, FakeLimiter, nlp_settings, seed_ann, \
    seed_occupant, seed_x


@pytest.mark.asyncio
async def test_forbidden_rows_never_sent(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    o = await seed_occupant(db, 64)
    await seed_x(db, "1", "ALLOWED_X SN64 ships", event_at=T0 - DAY)
    await seed_x(db, "2", "BOTPOST SN64 moon", event_at=T0 - DAY, bot_filtered=1)
    await seed_ann(db, "discord:11", "ALLOWED_OWN own server crosspost", event_at=T0 - DAY, occupant_key=o)
    await seed_ann(db, "discord:12", None, event_at=T0 - DAY, via_own_server=0)
    await seed_ann(db, "discord:13", None, event_at=T0 - DAY, via_own_server=1)
    await seed_ann(db, "discord:14", "DELETED crosspost", event_at=T0 - DAY, deleted_at=T0 - 100)
    await seed_ann(db, "telegram:chan/5", "ALLOWED_TG pending", event_at=T0 - DAY, purge_after=T0 + 6 * DAY)
    fake = FakeAnthropic()
    res = await LlmClassifier(db, fake, FakeGovernor(), FakeLimiter(), clock=lambda: T0).submit_cycle("r")
    assert res["n_items"] == 3
    payload = " ".join(r["params"]["messages"][0]["content"] for r in fake.submitted)
    assert "ALLOWED_X" in payload and "ALLOWED_OWN" in payload and "ALLOWED_TG" in payload
    assert "BOTPOST" not in payload and "DELETED" not in payload
    sent = {r["post_id"] for r in await db.fetchall("SELECT post_id FROM llm_batch_items")}
    assert sent == {"x:1", "discord:11", "telegram:chan/5"}
    # Discord authors are never named; only author_kind is carried
    assert "author_kind=\"team\"" in payload and "a1" not in payload
