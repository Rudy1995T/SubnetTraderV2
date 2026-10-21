"""Spec 10 §9 test_sentiment_metrics: the four P6 producer fns on hand-computed fixtures (02 §C5 call shape)."""
from __future__ import annotations

import math
from datetime import datetime, timezone
from statistics import fmean, pstdev

import pytest

from app.features import sentiment as S
from app.features.normalise import event_impact
from app.nlp.events import confirm_review, enqueue_review
from tests.nlp.fakes import BULK_V, DAY, ESC_V, HOUR, T0, seed_ann, seed_label, seed_occupant, seed_x

pytestmark = pytest.mark.asyncio
UTC = timezone.utc
T = T0 + 12 * HOUR
AS_OF = datetime.fromtimestamp(T, UTC)


async def _post(db, occ, n, *, event_at, label=True, observed_at=None, **lab):
    pid = await seed_x(db, str(n), f"post {n}", event_at=event_at, mention=occ, author_id=lab.pop("author", "a1"),
                       credibility=lab.pop("cred", None), is_kol=lab.pop("is_kol", 0), kol_handle=lab.pop("kol", None),
                       likes=lab.pop("likes", 0))
    if label:
        await seed_label(db, pid, occ=occ, event_at=event_at, observed_at=observed_at or event_at + 600, **lab)
    return pid


async def test_call_shape(db):
    o = await seed_occupant(db, 64)
    with pytest.raises(TypeError):
        await S.compute_event_impact_14d(db, o, "2026-10-01T12:00:00Z")
    with pytest.raises(TypeError):
        await S.compute_polarity_7d(db, o, datetime(2026, 10, 1, 12))                # naive
    from app.features.registry import ATTACHED_FNS
    assert ATTACHED_FNS["event_impact_14d"] is S.compute_event_impact_14d
    assert {k for k in ATTACHED_FNS if k in S.PRODUCERS} == set(S.PRODUCERS)


async def test_event_impact_hand_computed(db):
    o = await seed_occupant(db, 64)
    await _post(db, o, 1, event_at=T, observed_at=T, event_type="product_launch", direction=1, materiality=3)
    await _post(db, o, 2, event_at=T - 7 * DAY, event_type="delay_missed_milestone", direction=-1, materiality=2)
    await _post(db, o, 3, event_at=T - 13 * DAY, event_type="funding", direction=1, materiality=2)
    await _post(db, o, 4, event_at=T - 14 * DAY, event_type="exchange_listing", direction=1, materiality=2)
    await _post(db, o, 5, event_at=T - 2 * DAY, event_type="other", direction=0, materiality=1)
    r = await S.compute_event_impact_14d(db, o, AS_OF)
    assert r.status == "ok" and r.value == pytest.approx(2.552045, abs=1e-6)
    assert event_impact(r.value, 2.0) == pytest.approx(0.927711, abs=1e-6)          # 12's row-42 normaliser
    assert r.value_json["n_events"] == 3 and r.value_json["coverage"] == 1.0
    assert r.value_json["top"][0]["event_type"] == "product_launch"
    # 6 labels of one cluster count once (max materiality, earliest event_at)
    for i in range(6):
        await _post(db, o, 10 + i, event_at=T - 13 * DAY + i * HOUR, event_type="funding", direction=1, materiality=2)
    assert (await S.compute_event_impact_14d(db, o, AS_OF)).value == pytest.approx(2.552045, abs=1e-6)


async def test_event_impact_zero_and_pending(db):
    o = await seed_occupant(db, 64)
    r = await S.compute_event_impact_14d(db, o, AS_OF)
    assert r.status == "ok" and r.value == 0.0 and r.absence_reason is None       # never absent
    o2 = await seed_occupant(db, 19)
    for i in range(5):
        await _post(db, o2, 100 + i, event_at=T - DAY, label=i < 2)
    r = await S.compute_event_impact_14d(db, o2, AS_OF)
    assert r.status == "failed" and r.value_json["reason"] == "labels_pending" and r.value_json["coverage"] == 0.4


async def test_event_impact_unconfirmed_mat3_then_confirmed(db):
    o = await seed_occupant(db, 24)
    pid = await _post(db, o, 1, event_at=T - DAY, observed_at=T - DAY, event_type="governance_dispute", direction=-1,
                      materiality=3)
    r = await S.compute_event_impact_14d(db, o, AS_OF)
    assert r.value == 0.0 and r.value_json["pending_confirmation"] == 1
    rid = await enqueue_review(db, occupant_key=o, post_id=pid, label_version=BULK_V, event_type="governance_dispute",
                               materiality=3, summary="s", evidence_url=None, suggested_flag=None, reason="t",
                               priority=2, now=T - HOUR)
    await confirm_review(db, rid, by="rudolf", now=T - 600)
    r = await S.compute_event_impact_14d(db, o, AS_OF)
    assert r.value == pytest.approx(-3 * 0.5 ** (1 / 7), abs=1e-6) and r.value_json["pending_confirmation"] == 0
    early = datetime.fromtimestamp(T - 1200, UTC)                                 # before the confirmation
    assert (await S.compute_event_impact_14d(db, o, early)).value == 0.0
    # an escalation-tier mat-3 negative counts as it stands
    o2 = await seed_occupant(db, 25)
    await _post(db, o2, 2, event_at=T - DAY, event_type="governance_dispute", direction=-1, materiality=3,
                version=ESC_V, tier="escalation")
    assert (await S.compute_event_impact_14d(db, o2, AS_OF)).value == pytest.approx(-3 * 0.5 ** (1 / 7), abs=1e-6)


async def test_polarity_water_filled_cap(db):
    o = await seed_occupant(db, 64)
    n = 0
    for _ in range(7):                                                          # author A: 7 posts, w = 1 each
        n += 1
        await _post(db, o, n, event_at=T - DAY, author="A", cred=1.0, polarity="bullish", intensity=5)
    for a in range(5):                                                          # 5 authors, w = 0.6 each
        n += 1
        await _post(db, o, n, event_at=T - DAY, author=f"B{a}", cred=0.6, polarity="bearish", intensity=2)
    r = await S.compute_polarity_7d(db, o, AS_OF)
    assert r.status == "ok" and r.value == pytest.approx(0.475, abs=1e-6)
    assert r.value_json["n_authors"] == 6 and r.value_json["capped_authors"] == 1
    assert r.value_json["bullish_share"] == pytest.approx(0.25, abs=1e-6)        # 70 % author → exactly 25 %
    assert r.value_json["bearish_share"] == pytest.approx(0.75, abs=1e-6)        # 15 % each


async def test_polarity_few_posts(db):
    o = await seed_occupant(db, 64)
    for i in range(4):
        await _post(db, o, i, event_at=T - DAY, author=f"a{i}", polarity="bullish", intensity=2)
    await _post(db, o, 9, event_at=T - DAY, author="p", polarity="bullish", intensity=2, is_promo=1)
    await seed_ann(db, "discord:1", "team says hi", event_at=T - DAY, occupant_key=o)
    r = await S.compute_polarity_7d(db, o, AS_OF)
    assert r.status == "failed" and r.value_json["reason"] == "labels_pending"     # 4 + 1 unlabelled (+promo item)
    o2 = await seed_occupant(db, 19)
    for i in range(4):
        await _post(db, o2, 50 + i, event_at=T - DAY, author=f"a{i}", polarity="neutral")
    r = await S.compute_polarity_7d(db, o2, AS_OF)
    assert r.status == "not_applicable" and r.value_json["reason"] == "lt_5_posts"
    # announcements count with c = 1.0, e = 0, keyed by channel
    await seed_label(db, "discord:1", occ=o, event_at=T - DAY, observed_at=T - DAY + 60, polarity="bearish",
                     intensity=5)
    r = await S.compute_polarity_7d(db, o, AS_OF)
    assert r.status == "ok" and r.value_json["n_items"] == 5


async def _trending(db, occ, days: dict[int, float]):
    d0 = T - T % DAY
    for k, v in days.items():
        day = d0 - k * DAY
        await db.execute("INSERT INTO chain_trending_daily (day_ts, occupant_key, netuid, views, n_pulls_ok, "
                         "observed_at) VALUES (?,?,?,?,1,?)", (day, occ, int(occ.split(':')[0]), int(v), day + 7800))


async def test_attention_views(db):
    o = await seed_occupant(db, 64)
    series = {k: (100 + (k % 5) * 10 if k > 7 else 180 + k) for k in range(1, 98)}
    await _trending(db, o, series)
    r = await S.compute_attention_7d(db, o, AS_OF)
    base = [float(series[k]) for k in range(8, 98)]
    rec = [float(series[k]) for k in range(1, 8)]
    z = (fmean(rec) - fmean(base)) / max(pstdev(base), max(1.0, 0.1 * fmean(base)))
    assert r.status == "ok" and r.value == pytest.approx(z, abs=1e-6)
    assert r.value_json["baseline_days_views"] == 90 and r.value_json["z_mentions"] is None
    o2 = await seed_occupant(db, 19)
    await _trending(db, o2, {k: 50 for k in range(1, 28)})                      # 20 baseline days only
    r = await S.compute_attention_7d(db, o2, AS_OF)
    assert r.status == "failed" and r.value_json["reason"] == "baseline_short"


async def test_attention_mentions_only(db):
    o = await seed_occupant(db, 64)
    d0 = T - T % DAY
    await seed_label(db, "x:first", occ=None, event_at=d0 - 120 * DAY, observed_at=d0 - 100 * DAY)
    counts = {}
    n = 0
    for k in range(1, 98):
        c = 3 if k <= 7 else (1 if k % 2 == 0 else 0)
        counts[k] = c
        for j in range(c):
            n += 1
            await seed_label(db, f"x:m{n}", occ=o, event_at=d0 - k * DAY + 3600 + j, observed_at=d0 - k * DAY + 7200)
    # an own (source-bound) post on a recent day is excluded
    pid = await seed_x(db, "own1", "own post", event_at=d0 - 2 * DAY + 50, occupant_key=o, is_team=1)
    await seed_label(db, pid, occ=o, event_at=d0 - 2 * DAY + 50, observed_at=d0 - 2 * DAY + 100)
    r = await S.compute_attention_7d(db, o, AS_OF)
    base = [math.log1p(counts[k]) for k in range(8, 98)]
    rec = [math.log1p(counts[k]) for k in range(1, 8)]
    z = (fmean(rec) - fmean(base)) / max(pstdev(base), 0.5)
    assert r.status == "ok" and r.value == pytest.approx(z, abs=1e-6)
    assert r.value_json["z_views"] is None and r.value_json["mentions_7d"] == 21


async def test_kol_coverage(db):
    o = await seed_occupant(db, 64)
    r = await S.compute_kol_coverage_30d(db, o, AS_OF)
    assert r.status == "failed" and r.value_json["reason"] == "kol_poll_stale"
    await db.execute("INSERT INTO collector_runs (run_id, job, started_at, finished_at, ok) VALUES "
                     "('k1', 'x_kol_poll', ?, ?, 1)", (T - DAY, T - DAY + 60))
    kols = [("k1", 0.9, None), ("k2", 0.8, None), ("k3", 0.7, None), ("k4", 0.6, None), ("k5", 0.4, None),
            ("k6", 0.9, f'["{o}"]')]
    for h, c, team in kols:
        await db.execute("INSERT INTO kol_accounts (handle, handle_lower, category, credibility, verified, active, "
                         "team_occupant_keys, added_at, added_by, updated_at) VALUES (?,?,?,?,1,1,?,0,'seed',0)",
                         (h, h, "analyst", c, team))
    for i, h in enumerate(("k1", "k1", "k2", "k5", "k6")):
        await _post(db, o, 200 + i, event_at=T - 3 * DAY, author=h, is_kol=1, kol=h)
    r = await S.compute_kol_coverage_30d(db, o, AS_OF)
    assert r.status == "ok" and r.value == 2.0 and r.value_json["kol_handles"] == ["k1", "k2"]
    assert r.value_json["n_posts"] == 3
    o2 = await seed_occupant(db, 19)
    await _post(db, o2, 300, event_at=T - DAY, author="k1", is_kol=1, kol="k1", label=False)
    r = await S.compute_kol_coverage_30d(db, o2, AS_OF)
    assert r.status == "failed" and r.value_json["reason"] == "labels_pending"
    await db.execute("DELETE FROM kol_accounts WHERE handle IN ('k1','k2')")
    r = await S.compute_kol_coverage_30d(db, o, AS_OF)
    assert r.status == "failed" and r.value_json["reason"] == "kol_list_short"
