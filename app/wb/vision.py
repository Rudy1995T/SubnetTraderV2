"""Chart-screenshot read through Claude vision (spec 19 §2.7). Advisory only: the result becomes a ``manual_notes``
row (``kind='chart_read'``), never a metric.

Uses the official ``anthropic`` SDK (imported lazily; 10 §2's sanctioned caller outside ``Http``) with structured
output, adaptive thinking and server-side refusal fallbacks. A refusal, a ``max_tokens`` stop or unparsable output is
``status='unreadable'`` with no retry.
"""
from __future__ import annotations

import base64
import json
from typing import Any

# $ per million tokens (input, output); unknown models are priced as Opus 5.5
PRICES: dict[str, tuple[float, float]] = {
    "claude-opus-5-5": (4.0, 20.0), "claude-opus-5": (5.0, 25.0), "claude-opus-4-8": (5.0, 25.0),
    "claude-sonnet-5-5": (2.0, 10.0), "claude-sonnet-5": (2.0, 10.0), "claude-fable-5-1": (10.0, 50.0),
}
MEDIA_TYPES = {"image/png": "png", "image/jpeg": "jpg", "image/webp": "webp"}

_NUM = {"type": ["number", "null"]}
_CONF = {"type": "number"}
SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "trend": {"type": "string", "enum": ["up", "down", "sideways", "unclear"]},
        "above_visible_mas": {"type": ["boolean", "null"]},
        "drawdown_from_visible_high_pct": _NUM,
        "support_pct_below": _NUM,
        "resistance_pct_above": _NUM,
        "timeframe_guess": {"type": ["string", "null"]},
        "instrument_guess": {"type": ["string", "null"]},
        "axis_last_price_text": {"type": ["string", "null"]},
        "summary": {"type": "string"},
        "confidence": {
            "type": "object",
            "additionalProperties": False,
            "properties": {k: _CONF for k in ("trend", "above_visible_mas", "drawdown", "levels", "timeframe",
                                               "instrument")},
            "required": ["trend", "above_visible_mas", "drawdown", "levels", "timeframe", "instrument"],
        },
    },
    "required": ["trend", "above_visible_mas", "drawdown_from_visible_high_pct", "support_pct_below",
                 "resistance_pct_above", "timeframe_guess", "instrument_guess", "axis_last_price_text", "summary",
                 "confidence"],
}

PROMPT = (
    "This is a screenshot of a price chart, most likely a Bittensor subnet alpha token (TaoStats, TaoMarketCap or "
    "TradingView).{hint} Read it as an analyst would and fill the schema.\n\n"
    "Report relative readings only: the trend over the visible range; whether the last price sits above the moving "
    "averages drawn on the chart (null if none are drawn); the drawdown from the visible high in percent; the nearest "
    "visible support below and resistance above as a percent of the last price. Copy the last price exactly as it is "
    "printed on the axis or label into axis_last_price_text, and do not convert it. Guess the timeframe and the "
    "instrument from labels if visible, else null. For each reading give a confidence between 0 and 1, and keep it "
    "low when the axis is hard to read, the scale may be logarithmic, or several moving-average lines overlap. The "
    "summary is two or three plain sentences on what the chart shows, without investment advice."
)


class VisionUnavailable(RuntimeError):
    pass


def cost_usd(model: str, usage: Any) -> float:
    pin, pout = PRICES.get(model, PRICES["claude-opus-5-5"])
    inp = (getattr(usage, "input_tokens", 0) or 0) + (getattr(usage, "cache_creation_input_tokens", 0) or 0) * 1.25 \
        + (getattr(usage, "cache_read_input_tokens", 0) or 0) * 0.1
    return round((inp * pin + (getattr(usage, "output_tokens", 0) or 0) * pout) / 1e6, 6)


def make_client(api_key: str, timeout: float) -> Any:
    try:
        import anthropic
    except ImportError as exc:  # pragma: no cover - depends on the venv
        raise VisionUnavailable("the 'anthropic' package is not installed (pip install -e '.[llm]')") from exc
    return anthropic.AsyncAnthropic(api_key=api_key, timeout=timeout, max_retries=2)


async def read_chart(client: Any, image: bytes, media_type: str, *, model: str, netuid: int | None = None,
                     name: str | None = None) -> dict:
    hint = f" The operator says it is SN{netuid}{f' ({name})' if name else ''}." if netuid is not None else ""
    resp = await client.beta.messages.create(
        model=model,
        max_tokens=16000,
        thinking={"type": "adaptive"},
        output_config={"effort": "medium", "format": {"type": "json_schema", "schema": SCHEMA}},
        betas=["server-side-fallback-2026-07-01"],
        fallbacks="default",
        messages=[{"role": "user", "content": [
            {"type": "image", "source": {"type": "base64", "media_type": media_type,
                                         "data": base64.standard_b64encode(image).decode("ascii")}},
            {"type": "text", "text": PROMPT.format(hint=hint)},
        ]}],
    )
    served = getattr(resp, "model", model) or model
    out: dict[str, Any] = {"model": served, "stop_reason": resp.stop_reason, "cost_usd": cost_usd(served, resp.usage),
                           "usage": {"input_tokens": getattr(resp.usage, "input_tokens", None),
                                     "output_tokens": getattr(resp.usage, "output_tokens", None)}}
    if resp.stop_reason == "refusal":
        det = getattr(resp, "stop_details", None)
        out.update(status="unreadable", reason="refusal", category=getattr(det, "category", None))
        return out
    if resp.stop_reason == "max_tokens":
        out.update(status="unreadable", reason="max_tokens")
        return out
    text = "".join(getattr(b, "text", "") for b in resp.content if getattr(b, "type", None) == "text")
    try:
        read = json.loads(text)
        if not isinstance(read, dict) or "summary" not in read:
            raise ValueError("schema mismatch")
    except ValueError:
        out.update(status="unreadable", reason="invalid_json")
        return out
    out.update(status="ok", read=read)
    return out
