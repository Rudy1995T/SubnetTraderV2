"""Spec 10 §9 test_pit_invariance: relabels under a new version observed after T1 never change values at T1 (with
either policy), but do at T3 under the new policy; an unalerted event payload upgraded later does not leak; and no
code path issues ``UPDATE post_labels``."""
from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.features import sentiment as S
from app.nlp.events import materialise_events
from app.nlp.resolve import LabelPolicy
from app.scoring.spec import LABEL_POLICIES
from tests.nlp.fakes import DAY, HOUR, T0, seed_label, seed_occupant, seed_x

UTC = timezone.utc
NEW_V = "claude-haiku-5-5:deadbeef:2"
T1, T2, T3 = T0 + DAY, T0 + 2 * DAY, T0 + 3 * DAY
OLD = LABEL_POLICIES["ts-1.0"]
NEW = LabelPolicy(escalation=OLD.escalation, bulk=(NEW_V, *OLD.bulk), fallback=OLD.fallback)   # a ts-1.1 pinning


async def _all(db, occ, t, pol):
    when = datetime.fromtimestamp(t, UTC)
    out = {}
    for k, fn in S.PRODUCERS.items():
        r = await fn(db, occ, when, policy=pol)
        out[k] = (r.value, r.status, r.value_json.get("reason"))
    return out


@pytest.mark.asyncio
async def test_relabel_invisible_before_observation(db):
    o = await seed_occupant(db, 64)
    for i in range(6):
        pid = await seed_x(db, str(i), f"post {i}", event_at=T0 - DAY + i * HOUR, mention=o, author_id=f"a{i}")
        await seed_label(db, pid, occ=o, event_at=T0 - DAY + i * HOUR, observed_at=T0, polarity="bullish",
                         intensity=3, event_type="product_launch", direction=1, materiality=2)
    await materialise_events(db, OLD, "r", now=T0 + 60)
    snap = await _all(db, o, T1, OLD)
    assert snap["polarity_7d"][1] == "ok" and snap["event_impact_14d"][0] > 0
    for i in range(6):
        await seed_label(db, f"x:{i}", occ=o, event_at=T0 - DAY + i * HOUR, observed_at=T2, version=NEW_V,
                         polarity="bearish", intensity=5, event_type="delay_missed_milestone", direction=-1,
                         materiality=3)
    # an unalerted event payload upgraded at T2 (e.g. a later confirmation) does not leak into T1
    await db.execute("UPDATE events SET payload = json_set(payload, '$.confirmed_by', 'x', '$.confirmed_at', ?) "
                     "WHERE alerted_at IS NULL", (T2,))
    assert await _all(db, o, T1, OLD) == snap
    assert await _all(db, o, T1, NEW) == snap                       # new rows have observed_at > T1 → invisible
    later = await _all(db, o, T3, NEW)
    assert later != await _all(db, o, T3, OLD)
    assert later["polarity_7d"][0] < snap["polarity_7d"][0]


def test_no_update_post_labels_anywhere():
    root = Path(__file__).resolve().parents[2] / "app"
    pat = re.compile(r"UPDATE\s+post_labels", re.I)
    hits = [str(p) for p in root.rglob("*.py") if pat.search(p.read_text(encoding="utf-8"))]
    assert hits == []
