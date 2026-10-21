"""Spec 10 §9 test_resolve: escalation › bulk › rules; observed_at > as_of invisible; unpinned versions invisible."""
from __future__ import annotations

import pytest

from app.nlp.resolve import LabelPolicy, resolve, resolve_labels
from app.scoring.spec import LABEL_POLICIES
from tests.nlp.fakes import BULK_V, DAY, ESC_V, RULES_V, T0, seed_label, seed_occupant

pytestmark = pytest.mark.asyncio
POL = LABEL_POLICIES["ts-1.0"]


async def test_precedence_and_pit(db):
    o = await seed_occupant(db, 64)
    ev = T0 - 2 * DAY
    await seed_label(db, "x:1", occ=o, event_at=ev, observed_at=T0 - DAY, version=RULES_V, tier="rules",
                     event_type="other")
    await seed_label(db, "x:1", occ=o, event_at=ev, observed_at=T0 - DAY + 10, version=BULK_V,
                     event_type="governance_dispute", direction=-1, materiality=3)
    await seed_label(db, "x:1", occ=o, event_at=ev, observed_at=T0, version=ESC_V, tier="escalation",
                     event_type="governance_dispute", direction=-1, materiality=2)
    await seed_label(db, "x:1", occ=o, event_at=ev, observed_at=T0 - 5 * DAY, version="claude-haiku-5-5:deadbeef:2",
                     event_type="funding", direction=1, materiality=2)
    r = await resolve(db, POL, "x:1", T0)
    assert (r.classifier_version, r.tier, r.materiality) == (ESC_V, "escalation", 2)
    r = await resolve(db, POL, "x:1", T0 - 1)                                       # escalation not yet observed
    assert (r.classifier_version, r.tier, r.materiality) == (BULK_V, "bulk", 3)
    r = await resolve(db, POL, "x:1", T0 - DAY + 5)
    assert r.classifier_version == RULES_V and r.tier == "fallback"
    assert await resolve(db, POL, "x:1", T0 - 2 * DAY) is None                   # the unpinned row never shows
    other = LabelPolicy(escalation=(), bulk=("claude-haiku-5-5:deadbeef:2",), fallback=())
    assert (await resolve(db, other, "x:1", T0)).event_type == "funding"


async def test_window_and_occupant_filter(db):
    a = await seed_occupant(db, 64)
    b = await seed_occupant(db, 19)
    await seed_label(db, "x:1", occ=a, event_at=T0 - DAY, observed_at=T0 - 10)
    await seed_label(db, "x:2", occ=b, event_at=T0 - DAY, observed_at=T0 - 10)
    await seed_label(db, "x:3", occ=a, event_at=T0 - 7 * DAY, observed_at=T0 - 10)       # exactly at since → out
    await seed_label(db, "x:4", occ=a, event_at=T0 - DAY, observed_at=T0 - 10, conf=0.5)  # below 0.6 → no link
    got = await resolve_labels(db, POL, occupant_key=a, since=T0 - 7 * DAY, until=T0, as_of=T0)
    assert [x.post_id for x in got] == ["x:1"] and got[0].entities[0]["occupant_key"] == a
    allp = await resolve_labels(db, POL, occupant_key=None, since=T0 - 8 * DAY, until=T0, as_of=T0)
    assert [x.post_id for x in allp] == ["x:3", "x:1", "x:2", "x:4"]
    # the occupant filter follows the RESOLVED version's entities: an escalation row that drops the link wins
    await seed_label(db, "x:1", occ=b, event_at=T0 - DAY, observed_at=T0 - 5, version=ESC_V, tier="escalation")
    got = await resolve_labels(db, POL, occupant_key=a, since=T0 - 7 * DAY, until=T0, as_of=T0)
    assert got == []
