"""Per-item label schema v1 (spec 10 §2.4, §8.2): pydantic models for client-side validation and the hand-written JSON
schemas sent as ``output_config.format`` (structured outputs accept only type/enum/required/properties/items/
additionalProperties here — ranges and the 140-char summary are checked client-side, §2.3).

Validation is per item: an unknown ``event_type`` rejects that item, never the batch; ``schema_v ≠ 1`` rejects the whole
request. ``direction`` outside ``ALLOWED_DIRECTION`` is clamped to ``DEFAULT_DIRECTION`` with ``clamped=True``.
Changing anything here means bumping ``LLM_SCHEMA_V`` (a new ``classifier_version``).
"""
from __future__ import annotations

import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, ValidationError, field_validator, model_validator

SCHEMA_V = 1
SUMMARY_MAX = 140
NARRATIVE_MAX_WORDS = 300

EVENT_TYPES: tuple[str, ...] = (
    "product_launch", "release_upgrade", "partnership_integration", "revenue_customer", "token_buyback_burn",
    "exchange_listing", "funding", "emissions_tokenomics_change", "team_change", "security_incident",
    "delay_missed_milestone", "governance_dispute", "regulatory", "owner_extraction_evidence", "giveaway_promo",
    "other")
POLARITIES: tuple[str, ...] = ("bullish", "bearish", "neutral")
EventType = Literal["product_launch", "release_upgrade", "partnership_integration", "revenue_customer",
                    "token_buyback_burn", "exchange_listing", "funding", "emissions_tokenomics_change", "team_change",
                    "security_incident", "delay_missed_milestone", "governance_dispute", "regulatory",
                    "owner_extraction_evidence", "giveaway_promo", "other"]

ALLOWED_DIRECTION: dict[str, set[int]] = {
    "product_launch": {1}, "release_upgrade": {1}, "partnership_integration": {1}, "revenue_customer": {1},
    "token_buyback_burn": {1}, "exchange_listing": {1}, "funding": {1}, "emissions_tokenomics_change": {0},
    "team_change": {-1, 1}, "security_incident": {-1}, "delay_missed_milestone": {-1}, "governance_dispute": {-1},
    "regulatory": {-1}, "owner_extraction_evidence": {-1}, "giveaway_promo": {0}, "other": {0}}
DEFAULT_DIRECTION: dict[str, int] = {k: (next(iter(v)) if len(v) == 1 else 0) for k, v in ALLOWED_DIRECTION.items()}
DEFAULT_DIRECTION["team_change"] = -1


def _clamp01(x: Any) -> float:
    v = float(x)
    return 0.0 if v < 0 else 1.0 if v > 1 else v


class Entity(BaseModel):
    model_config = ConfigDict(extra="ignore")
    netuid: int
    confidence: float

    @field_validator("confidence", mode="before")
    @classmethod
    def _conf(cls, v: Any) -> float:
        return _clamp01(v)

    @field_validator("netuid")
    @classmethod
    def _netuid(cls, v: int) -> int:
        if not 0 <= v <= 1023:
            raise ValueError("netuid out of range")
        return v


class LabelV1(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str
    polarity: Literal["bullish", "bearish", "neutral"]
    intensity: Literal[1, 2, 3, 4, 5]
    event_type: EventType
    direction: Literal[-1, 0, 1]
    materiality: Literal[1, 2, 3]
    entities: list[Entity] = Field(default_factory=list)
    is_spam: bool
    is_promo: bool
    summary: str
    confidence: float
    clamped: bool = False                          # client-side marker (raw_json.clamped); not part of the API schema
    _raw: dict | None = PrivateAttr(default=None)  # the model's item object verbatim (post_labels.raw_json)

    @model_validator(mode="before")
    @classmethod
    def _pre(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        d = dict(data)
        if "id" in d and not isinstance(d["id"], str):
            d["id"] = str(d["id"])
        if isinstance(d.get("summary"), str) and len(d["summary"]) > SUMMARY_MAX:
            d["summary"] = d["summary"][:SUMMARY_MAX]
        if "confidence" in d and isinstance(d["confidence"], (int, float)) and not isinstance(d["confidence"], bool):
            d["confidence"] = _clamp01(d["confidence"])
        et, dr = d.get("event_type"), d.get("direction")
        if et in ALLOWED_DIRECTION and dr in (-1, 0, 1) and dr not in ALLOWED_DIRECTION[et]:
            d["direction"] = DEFAULT_DIRECTION[et]
            d["clamped"] = True
        return d

    def api_dict(self) -> dict:
        """The item as the model would have produced it (no client-side markers)."""
        return self.model_dump(exclude={"clamped"})

    def raw_dict(self) -> dict:
        return dict(self._raw) if isinstance(self._raw, dict) else self.api_dict()

    @classmethod
    def from_raw(cls, raw: Any) -> "LabelV1":
        lab = cls.model_validate(raw)
        lab._raw = dict(raw) if isinstance(raw, dict) else None
        return lab


class BatchOutV1(BaseModel):
    model_config = ConfigDict(extra="ignore")
    schema_v: Literal[1]
    items: list[LabelV1]


class NarrativeV1(BaseModel):
    model_config = ConfigDict(extra="ignore")
    narrative: str
    risks: list[str]
    catalysts: list[str]
    confidence: float

    @model_validator(mode="before")
    @classmethod
    def _pre(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        d = dict(data)
        if isinstance(d.get("narrative"), str):
            words = d["narrative"].split()
            if len(words) > NARRATIVE_MAX_WORDS:
                d["narrative"] = " ".join(words[:NARRATIVE_MAX_WORDS]) + " …"
        for k in ("risks", "catalysts"):
            if isinstance(d.get(k), list):
                d[k] = [str(x) for x in d[k]][:3]
        if isinstance(d.get("confidence"), (int, float)):
            d["confidence"] = _clamp01(d["confidence"])
        return d


# ── JSON schemas for output_config.format (only supported keywords) ──────────────────────────────────────────────
def _item_schema() -> dict:
    return {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "polarity": {"type": "string", "enum": list(POLARITIES)},
            "intensity": {"type": "integer", "enum": [1, 2, 3, 4, 5]},
            "event_type": {"type": "string", "enum": list(EVENT_TYPES)},
            "direction": {"type": "integer", "enum": [-1, 0, 1]},
            "materiality": {"type": "integer", "enum": [1, 2, 3]},
            "entities": {"type": "array", "items": {
                "type": "object", "properties": {"netuid": {"type": "integer"}, "confidence": {"type": "number"}},
                "required": ["netuid", "confidence"], "additionalProperties": False}},
            "is_spam": {"type": "boolean"},
            "is_promo": {"type": "boolean"},
            "summary": {"type": "string"},
            "confidence": {"type": "number"},
        },
        "required": ["id", "polarity", "intensity", "event_type", "direction", "materiality", "entities", "is_spam",
                     "is_promo", "summary", "confidence"],
        "additionalProperties": False,
    }


def json_schema_batch() -> dict:
    return {"type": "object",
            "properties": {"schema_v": {"type": "integer", "enum": [SCHEMA_V]},
                           "items": {"type": "array", "items": _item_schema()}},
            "required": ["schema_v", "items"], "additionalProperties": False}


def json_schema_single() -> dict:
    return _item_schema()


def json_schema_narrative() -> dict:
    return {"type": "object",
            "properties": {"narrative": {"type": "string"}, "risks": {"type": "array", "items": {"type": "string"}},
                           "catalysts": {"type": "array", "items": {"type": "string"}},
                           "confidence": {"type": "number"}},
            "required": ["narrative", "risks", "catalysts", "confidence"], "additionalProperties": False}


# ── parsing ──────────────────────────────────────────────────────────────────────────────────────────────────────
class RequestRejected(ValueError):
    """The whole request's output is unusable (not JSON, not an object, ``schema_v ≠ 1``, no ``items`` list)."""


def _loads(text: str) -> Any:
    t = (text or "").strip()
    if t.startswith("```"):                          # tolerate a fenced block
        t = t.strip("`")
        t = t[t.find("{"):] if "{" in t else t
    try:
        return json.loads(t)
    except ValueError:
        a, b = t.find("{"), t.rfind("}")
        if a >= 0 and b > a:
            try:
                return json.loads(t[a:b + 1])
            except ValueError:
                pass
    raise RequestRejected("invalid_json")


def parse_batch_items(text: str) -> tuple[list[tuple[dict, LabelV1]], list[tuple[Any, str]]]:
    """Validate one request's output. Returns (valid [(raw_item, label)], rejected [(raw_item, error)]). Raises
    ``RequestRejected`` when the request as a whole is unusable."""
    obj = _loads(text)
    if not isinstance(obj, dict):
        raise RequestRejected("not_an_object")
    if obj.get("schema_v") != SCHEMA_V:
        raise RequestRejected(f"schema_v={obj.get('schema_v')!r}")
    items = obj.get("items")
    if not isinstance(items, list):
        raise RequestRejected("items_missing")
    ok: list[tuple[dict, LabelV1]] = []
    bad: list[tuple[Any, str]] = []
    for raw in items:
        try:
            ok.append((raw, LabelV1.from_raw(raw)))
        except ValidationError as exc:
            bad.append((raw, exc.errors()[0].get("type", "invalid") if exc.errors() else "invalid"))
    return ok, bad


def parse_single(text: str) -> LabelV1:
    obj = _loads(text)
    if isinstance(obj, dict) and isinstance(obj.get("items"), list) and len(obj["items"]) == 1:
        obj = obj["items"][0]                      # tolerate the batch envelope
    if not isinstance(obj, dict):
        raise RequestRejected("not_an_object")
    obj.setdefault("id", "1")
    try:
        return LabelV1.from_raw(obj)
    except ValidationError as exc:
        raise RequestRejected(f"invalid_item: {exc.errors()[0].get('type') if exc.errors() else ''}") from exc


def parse_narrative(text: str) -> NarrativeV1:
    obj = _loads(text)
    try:
        return NarrativeV1.model_validate(obj)
    except ValidationError as exc:
        raise RequestRejected("invalid_narrative") from exc


__all__ = ["SCHEMA_V", "EVENT_TYPES", "POLARITIES", "ALLOWED_DIRECTION", "DEFAULT_DIRECTION", "Entity", "LabelV1",
           "BatchOutV1", "NarrativeV1", "json_schema_batch", "json_schema_single", "json_schema_narrative",
           "parse_batch_items", "parse_single", "parse_narrative", "RequestRejected", "SUMMARY_MAX"]
