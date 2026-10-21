"""Spec 10 §9.3: the shipped synthetic golden bootstrap (40 items) and the offline eval harness (rules mode, drift)."""
from __future__ import annotations

import json
from pathlib import Path

from app.nlp import eval as E
from app.nlp.schema import ALLOWED_DIRECTION, EVENT_TYPES

GOLDEN = Path(__file__).resolve().parents[1] / "fixtures" / "golden_labels_v1.jsonl"


def test_golden_bootstrap_shape():
    rows = E.load_golden(GOLDEN)
    assert len(rows) == 40 and len({r["id"] for r in rows}) == 40
    for r in rows:
        g = r["gold"]
        assert r["labeller"] == "synthetic" and r["item_kind"] in ("social_post", "announcement", "news_item",
                                                                    "video_item")
        assert g["event_type"] in EVENT_TYPES and g["direction"] in ALLOWED_DIRECTION[g["event_type"]]
        assert g["materiality"] in (1, 2, 3) and g["polarity"] in ("bullish", "bearish", "neutral")
    assert E.load_golden(GOLDEN, reviewed_only=True) == rows                       # no llm-prelabel rows yet


def test_rules_eval_and_drift(tmp_path):
    m = E.run(GOLDEN, "rules", out_dir=tmp_path)
    assert m["n"] == 40 and m["classifier_version"] == "rules:v2"
    for k in ("macro_f1", "polarity_accuracy", "direction_accuracy", "materiality_mae", "entity_precision",
              "entity_recall", "spam_precision", "promo_recall", "polarity_confusion"):
        assert k in m
    assert m["entity_precision"] >= 0.90                                           # a wrong subnet is worse than a miss
    saved = json.loads(Path(m["path"]).read_text())
    assert len(saved["predictions"]) == 40
    d = E.drift(saved, saved)
    assert d["agreement"] == 1.0 and d["accepted"]
    other = json.loads(json.dumps(saved))
    for k in list(other["predictions"])[:20]:
        other["predictions"][k]["event_type"] = "funding"
    assert not E.drift(saved, other)["accepted"]


def test_score_metrics_hand_case():
    golden = [{"id": "a", "gold": {"polarity": "bullish", "event_type": "funding", "direction": 1, "materiality": 2,
                                   "entities": [1], "is_spam": False, "is_promo": False}},
              {"id": "b", "gold": {"polarity": "bearish", "event_type": "regulatory", "direction": -1,
                                   "materiality": 1, "entities": [2], "is_spam": True, "is_promo": False}}]
    preds = {"a": {"polarity": "bullish", "event_type": "funding", "direction": 1, "materiality": 3, "entities": [1, 3],
                   "is_spam": False, "is_promo": False},
             "b": {"polarity": "bullish", "event_type": "other", "direction": 0, "materiality": 1, "entities": [],
                   "is_spam": True, "is_promo": False}}
    m = E.score(golden, preds)
    assert m["macro_f1"] == 0.5 and m["polarity_accuracy"] == 0.5 and m["bullish_bearish_swaps"] == 1
    assert m["materiality_mae"] == 0.5 and m["entity_precision"] == 0.5 and m["entity_recall"] == 0.5
    assert m["spam_precision"] == 1.0
