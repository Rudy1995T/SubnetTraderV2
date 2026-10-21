"""Spec 10 §9 test_schema: batch parsing, per-item rejection, truncation, direction clamping, position salvage."""
from __future__ import annotations

import json

import pytest

from app.nlp.llm_classify import LlmClassifier
from app.nlp.schema import (ALLOWED_DIRECTION, DEFAULT_DIRECTION, LabelV1, RequestRejected, json_schema_batch,
                            json_schema_narrative, json_schema_single, parse_batch_items, parse_narrative)
from tests.nlp.fakes import FIX, item_obj, message


def test_valid_batch_fixture_parses():
    ok, bad = parse_batch_items((FIX / "batch_ok.json").read_text())
    assert len(ok) == 3 and not bad
    lab = ok[0][1]
    assert lab.event_type == "exchange_listing" and lab.entities[0].netuid == 8 and lab.materiality == 3


def test_unknown_event_type_rejects_item_not_batch():
    text = json.dumps({"schema_v": 1, "items": [item_obj(1), item_obj(2, event_type="moon_mission")]})
    ok, bad = parse_batch_items(text)
    assert [x[1].id for x in ok] == ["1"] and len(bad) == 1


def test_summary_truncated_and_direction_clamped():
    lab = LabelV1.from_raw(item_obj(1, summary="x" * 300))
    assert len(lab.summary) == 140 and not lab.clamped
    lab = LabelV1.from_raw(item_obj(1, event_type="product_launch", direction=-1))
    assert lab.direction == 1 and lab.clamped and lab.raw_dict()["direction"] == -1
    lab = LabelV1.from_raw(item_obj(1, event_type="emissions_tokenomics_change", direction=-1))
    assert lab.direction == 0 and lab.clamped                                          # master §7.4: informational
    lab = LabelV1.from_raw(item_obj(1, event_type="team_change", direction=1))
    assert lab.direction == 1 and not lab.clamped
    assert DEFAULT_DIRECTION["team_change"] == -1 and ALLOWED_DIRECTION["other"] == {0}


def test_schema_v_mismatch_rejects_request():
    with pytest.raises(RequestRejected):
        parse_batch_items(json.dumps({"schema_v": 2, "items": [item_obj(1)]}))
    with pytest.raises(RequestRejected):
        parse_batch_items("not json at all")


def test_position_mismatch_salvages_matched():
    clf = LlmClassifier(None, None, None, None)
    pids = ["news_item:" + "a" * 64, "x:1", "x:2"]
    msg = message(json.dumps({"schema_v": 1, "items": [item_obj(3), item_obj(1), item_obj(9), item_obj(1)]}))
    parsed, unmatched = clf.parse_request_result("c-0000", msg, pids)
    assert [p for p, _ in parsed] == ["news_item:" + "a" * 64, "x:2"] and unmatched == ["x:1"]
    parsed, unmatched = clf.parse_request_result("c-0000", message("{}", stop_reason="refusal"), pids)
    assert parsed == [] and unmatched == pids


def test_json_schemas_use_supported_keywords_only():
    allowed = {"type", "properties", "required", "additionalProperties", "items", "enum"}

    def walk(node):
        if isinstance(node, dict):
            for k, v in node.items():
                if k != "properties":
                    assert k in allowed, k
                    walk(v)
                else:
                    for sub in v.values():
                        walk(sub)
            if node.get("type") == "object":
                assert node.get("additionalProperties") is False
        elif isinstance(node, list):
            for x in node:
                if isinstance(x, dict):
                    walk(x)
    for s in (json_schema_batch(), json_schema_single(), json_schema_narrative()):
        walk(s)


def test_narrative_parse():
    n = parse_narrative((FIX / "narrative_ok.json").read_text())
    assert len(n.risks) == 3 and len(n.catalysts) == 3 and 0 <= n.confidence <= 1
    long = parse_narrative(json.dumps({"narrative": "w " * 400, "risks": list("abcd"), "catalysts": [],
                                       "confidence": 3}))
    assert len(long.narrative.split()) <= 301 and len(long.risks) == 3 and long.confidence == 1.0
