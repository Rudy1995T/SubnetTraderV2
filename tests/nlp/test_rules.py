"""Spec 10 §9 test_rules: a 30-item mini subset (event_type, polarity), never materiality 3, generic names need
Bittensor context, ``$SN64`` → 64 @ 1.0, the fallback job's modes and its label rows."""
from __future__ import annotations

import pytest

import app.config as app_config
from app.nlp.match import OccupantIndex, OccupantRef
from app.nlp.queue import QueueItem
from app.nlp.rules import LlmRulesFallbackJob, RulesClassifier, rules_fallback_cycle
from tests.nlp.fakes import BULK_V, DAY, HOUR, T0, nlp_settings, seed_ann, seed_occupant, seed_x

MINI = [
    ("Exploit on SN15: attacker drained 147k alpha from the pool", "security_incident", "bearish"),
    ("Phishing warning: fake SN64 site, do not connect your wallet", "security_incident", "bearish"),
    ("Hotkey key leaked for subnet 9 validator, funds at risk", "security_incident", "bearish"),
    ("SN71 owner dumped 40k alpha right after the AMA", "owner_extraction_evidence", "bearish"),
    ("Sold everything? The owner sold 20k alpha on SN33 yesterday", "owner_extraction_evidence", "bearish"),
    ("SN50 buyback week 8: 12.4k alpha bought back with API revenue 🚀", "token_buyback_burn", "bullish"),
    ("SN5 burned 8,000 alpha from protocol fees", "token_buyback_burn", "bullish"),
    ("SN8 alpha listed on Kraken today, huge milestone", "exchange_listing", "bullish"),
    ("Now trading on Binance: SN64 alpha perps", "exchange_listing", "neutral"),
    ("Targon SN4 hits $10.4M ARR from paying customers", "revenue_customer", "bullish"),
    ("Revenue update for subnet 62: growing every week", "revenue_customer", "bullish"),
    ("Chutes SN64 is now live on mainnet, try it at chutes.ai", "product_launch", "bullish"),
    ("Public beta of the SN56 trainer launched today, amazing work", "product_launch", "bullish"),
    ("SN19 v2.3 release notes are up, solid improvement", "release_upgrade", "bullish"),
    ("Validator upgrade required for subnet 12 this week", "release_upgrade", "bullish"),
    ("Our CTO is stepping down from SN51, thanks for everything", "team_change", "bearish"),
    ("Alice joins as head of research at SN20, welcome!", "team_change", "bullish"),
    ("SN11 milestone delayed again, very disappointing", "delay_missed_milestone", "bearish"),
    ("The SN44 roadmap item was pushed back to Q1", "delay_missed_milestone", "neutral"),
    ("SN24 owner banned from the Discord after the audit", "governance_dispute", "bearish"),
    ("Subnet 33 validators in a public dispute with the team", "governance_dispute", "bearish"),
    ("SN62 raised a seed round from a crypto fund", "funding", "bullish"),
    ("SN68 closes Series A, great news", "funding", "bullish"),
    ("🎁 Giveaway! RT & follow to win 5 $SN64", "giveaway_promo", "bullish"),
    ("Airdrop for SN99 holders, tag 3 friends", "giveaway_promo", "neutral"),
    ("Score just dropped 3 goals in the second half", "other", "neutral"),
    ("gm bittensor fam, what a day for subnet builders", "other", "neutral"),
    ("Bittensor is dead, TAO is worthless 💀", "other", "bearish"),
    ("Not bad at all, SN9 is not a scam", "other", "bullish"),
    ("SN73 weekly community call notes", "other", "neutral"),
]


def _q(text: str, occ: str | None = None) -> QueueItem:
    return QueueItem("x:1", "social_post", "x:1", occ, "x", "other", None, None, text, "h", T0, None)


def _index() -> OccupantIndex:
    return OccupantIndex([OccupantRef("44:1", 44, "Score"), OccupantRef("64:1", 64, "Chutes", "CHUTES"),
                          OccupantRef("50:1", 50, "Synth")])


@pytest.mark.parametrize("text,et,pol", MINI)
def test_mini_subset(text, et, pol):
    lab = RulesClassifier().classify(_q(text), _index())
    assert (lab.event_type, lab.polarity) == (et, pol)
    assert lab.materiality in (1, 2)                                              # rules never emit 3


def test_entities_and_spam():
    c = RulesClassifier()
    lab = c.classify(_q("Score just dropped 3 goals in the second half"), _index())
    assert lab.entities == [] and lab.is_spam
    lab = c.classify(_q("Score subnet emissions up this week on bittensor"), _index())
    assert [(e.netuid, e.confidence) for e in lab.entities] == [(44, 0.7)] and not lab.is_spam
    lab = c.classify(_q("🎁 Giveaway! RT & follow to win 5 $SN64"), _index())
    assert [(e.netuid, e.confidence) for e in lab.entities] == [(64, 1.0)] and lab.is_promo and lab.confidence == 0.5
    lab = c.classify(_q("Chutes is crushing it"), _index())                     # unambiguous name → context
    assert [(e.netuid, e.confidence) for e in lab.entities] == [(64, 0.7)] and lab.confidence == 0.4
    lab = c.classify(_q("price #64 at 3pm"), _index())
    assert lab.entities == []                                                  # bare #64 is not a netuid
    lab = c.classify(_q("send 50 TAO to claim bit.ly/x subnet 3"), _index())
    assert lab.is_spam
    lab = c.classify(_q("just vibes", occ="64:1"), _index())
    assert not lab.is_spam                                                     # source-bound
    assert all(c.classify(_q(t), None).materiality < 3 for t, _, _ in MINI)


@pytest.mark.asyncio
async def test_fallback_cycle_modes(db, monkeypatch):
    monkeypatch.setattr(app_config, "settings", nlp_settings(LLM_ENABLED=False))
    o = await seed_occupant(db, 64, name="Chutes")
    await seed_x(db, "1", "SN64 exploit drained the pool", event_at=T0 - HOUR, observed_at=T0 - HOUR)
    await seed_ann(db, "telegram:chutes/1", "Pending text", event_at=T0 - HOUR, occupant_key=o,
                   purge_after=T0 + 6 * DAY)
    await seed_x(db, "2", "ancient SN64 post", event_at=T0 - 40 * DAY)
    res = await rules_fallback_cycle(db, "r1", now=T0)
    assert res["mode"] == "llm_off" and res["labelled"] == 2
    rows = await db.fetchall("SELECT post_id, classifier_version, classifier_tier, source, materiality, entities_json "
                             "FROM post_labels ORDER BY post_id")
    assert {(r["classifier_version"], r["classifier_tier"], r["source"]) for r in rows} == {("rules:v2", "rules",
                                                                                            "rules")}
    tg = next(r for r in rows if r["post_id"] == "telegram:chutes/1")
    assert '"via":"source"' in tg["entities_json"]
    ent = await db.fetchall("SELECT post_id, via FROM post_label_entities ORDER BY post_id")
    assert [(e["post_id"], e["via"]) for e in ent] == [("telegram:chutes/1", "source"), ("x:1", "regex")]
    m = await db.fetchall("SELECT platform, post_id, match_kind, classifier_version FROM social_post_mentions")
    assert [(r["platform"], r["post_id"], r["match_kind"]) for r in m] == [("x", "1", "entity")]
    # LLM on: only stale (> LLM_FALLBACK_AFTER_H) items
    monkeypatch.setattr(app_config, "settings", nlp_settings())
    await seed_x(db, "3", "fresh SN64 post", event_at=T0 - HOUR, observed_at=T0 - HOUR)
    await seed_x(db, "4", "stale SN64 post", event_at=T0 - 20 * HOUR, observed_at=T0 - 19 * HOUR)
    res = await rules_fallback_cycle(db, "r2", now=T0)
    assert res["mode"] == "stale_only" and res["labelled"] == 1
    assert await db.fetchval("SELECT COUNT(*) FROM post_labels WHERE post_id = 'x:4'") == 1
    # a budget stop recorded by submit → everything eligible now
    await db.execute("INSERT INTO job_state (job, key, value_json, updated_at) VALUES ('llm_classify_submit', "
                     "'budget_stop', ?, ?)", (f'{{"at": {T0}}}', T0))
    res = await rules_fallback_cycle(db, "r3", now=T0 + 60)
    assert res["mode"] == "budget" and res["labelled"] == 1
    assert BULK_V not in {r["classifier_version"] for r in await db.fetchall("SELECT classifier_version FROM "
                                                                             "post_labels")}


@pytest.mark.asyncio
async def test_fallback_job_runs_through_wrapper(rt, monkeypatch):
    from app.jobs import SPEC_BY_ID
    monkeypatch.setattr(app_config, "settings", nlp_settings(LLM_ENABLED=False, KILL_SWITCH_PATH=str(
        app_config.settings.KILL_SWITCH_PATH), PAUSE_ALL_PATH=str(app_config.settings.PAUSE_ALL_PATH)))
    await seed_occupant(rt.db, 64)
    await seed_x(rt.db, "1", "SN64 is live", event_at=T0)
    job = LlmRulesFallbackJob(rt, SPEC_BY_ID["llm_rules_fallback"])
    await job.run(trigger="cli")
    r = await rt.db.fetchone("SELECT ok, rows, note FROM collector_runs WHERE job = 'llm_rules_fallback'")
    assert r["ok"] == 1 and r["rows"] == 1 and r["note"].startswith("mode=llm_off")
