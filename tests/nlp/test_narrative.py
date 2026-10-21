"""Spec 10 §9 test_narrative: subject selection (A/B tiers, movers, new flags), evidence bundle budget and section
order, Opus fake → manual_notes(kind='llm_narrative'), refusal → no row and no raise, governor denial stops."""
from __future__ import annotations

import json

import pytest

import app.config as app_config
from app.nlp import narrative as N
from tests.nlp.fakes import DAY, FIX, T0, FakeAnthropic, FakeGovernor, FakeLimiter, message, nlp_settings, \
    seed_label, seed_occupant, seed_x

pytestmark = pytest.mark.asyncio
AS_OF = T0 - T0 % DAY


async def _score(db, occ, as_of, *, tier, comp, flags=()):
    await db.execute(
        "INSERT INTO scores (occupant_key, netuid, as_of, spec_version, run_kind, run_id, status, composite, "
        "composite_smoothed, pillar_scores, pillar_coverage, confidence, tier, red_flags, explain, computed_at, "
        "input_cutoff) VALUES (?,?,?,'ts-1.0','official','r','active',?,?,'{\"P1\":0.5}','{\"P1\":1.0}',0.8,?,?,"
        "'{\"why\":\"fixture\"}',?,?)",
        (occ, int(occ.split(":")[0]), as_of, comp, comp, tier, json.dumps(sorted(flags)), as_of + 900, as_of + 900))


async def _seed(db):
    occs = {n: await seed_occupant(db, n, name=f"Sub{n}") for n in (1, 2, 3, 4, 5, 6)}
    plan = {1: ("A", 80, 80, ()), 2: ("B", 65, 64, ()), 3: ("C", 50, 30, ()), 4: ("C", 40, 41, ("repo_gone",)),
            5: ("D", 35, 35, ()), 6: ("D", 20, 21, ())}
    for n, (tier, now_c, prev_c, flags) in plan.items():
        await _score(db, occs[n], AS_OF, tier=tier, comp=now_c, flags=flags)
        await _score(db, occs[n], AS_OF - 7 * DAY, tier=tier, comp=prev_c)
    return occs


async def test_select_subjects(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    occs = await _seed(db)
    subj = await N.select_subjects(db, max_subnets=40)
    assert subj[:2] == [occs[1], occs[2]]                                      # A/B by composite desc
    assert subj[2] == occs[3]                                                  # biggest mover next
    assert occs[4] in subj                                                     # new red flag
    assert len(await N.select_subjects(db, max_subnets=2)) == 2


async def test_evidence_bundle_order_and_budget(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    occs = await _seed(db)
    o = occs[1]
    pid = await seed_x(db, "1", "full text must never appear in the bundle", event_at=T0 - DAY)
    await seed_label(db, pid, occ=o, event_at=T0 - DAY, observed_at=T0 - DAY + 60, event_type="product_launch",
                     direction=1, materiality=2, summary="Sub1 launched v2")
    await db.execute("INSERT INTO manual_notes (occupant_key, kind, title, body, created_at, created_by) VALUES "
                     "(?, 'note', 'op', 'operator note body', ?, 'cli')", (o, T0 - DAY))
    monkeypatch.setattr(N, "now_ts", lambda: T0)
    text, sha = await N.build_evidence(db, o, token_budget=8000)
    heads = [ln[3:] for ln in text.splitlines() if ln.startswith("## ")]
    assert heads == ["IDENTITY", "SCORE_EXPLAIN", "PILLARS", "EVENTS_14D", "TOP_LABELS", "P1_HIGHLIGHTS",
                     "P4_HIGHLIGHTS", "P2_HIGHLIGHTS", "OPERATOR_NOTES"]
    assert "Sub1 launched v2" in text and "full text must never" not in text and len(sha) == 64
    small, _ = await N.build_evidence(db, o, token_budget=120)
    assert len(small) <= int(120 / 0.29) and small.startswith("## IDENTITY")


async def test_run_weekly_writes_and_refusal(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings(LLM_NARRATIVE_MAX_SUBNETS=2))
    occs = await _seed(db)
    body = (FIX / "narrative_ok.json").read_text()
    seen = []

    def direct(kw):
        seen.append(kw)
        if len(seen) == 2:
            return message("", stop_reason="refusal", model="claude-opus-5-5", category="other")
        return message(body, model="claude-opus-5-5")
    fake = FakeAnthropic(direct_fn=direct)
    gov = FakeGovernor()
    res = await N.run_weekly(db, fake, gov, FakeLimiter(), "rn")
    assert res["written"] == 1 and res["refused"] == 1 and res["subjects"] == 2
    kw = seen[0]
    assert kw["max_tokens"] == 8000 and kw["output_config"]["effort"] == "medium" and "timeout" in kw
    assert kw["fallbacks"] == "default"
    row = await db.fetchone("SELECT * FROM manual_notes WHERE kind = 'llm_narrative'")
    assert row["occupant_key"] == occs[1] and row["created_by"] == "llm:claude-opus-5-5"
    p = json.loads(row["payload_json"])
    assert len(p["risks"]) == 3 and p["spec_version"] == "ts-1.0" and p["cost_usd"] > 0 and p["score_as_of"] == AS_OF
    assert [x[0] for x in gov.log] == ["check", "record", "check", "record"] and gov.of("record")[0][3] == 1
    # a re-run within the week skips written subjects; a denial stops the loop
    res = await N.run_weekly(db, fake, FakeGovernor(allow=False), FakeLimiter(), "rn2")
    assert res["skipped"] == 1 and res["denied"] == 1 and res["written"] == 0
