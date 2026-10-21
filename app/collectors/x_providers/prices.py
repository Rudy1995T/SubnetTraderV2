"""X provider prices (spec 09 §2.3, research §1) with the optional ``X_PRICES_JSON`` override (09 §5).

``twitterapi_io``: ``per_tweet`` $0.15/1k, ``per_profile`` $0.18/1k, ``min_call`` 15 credits per request. The vendor's
"list calls $0.0015 from Oct 1" rule is not known to cover ``last_tweets``/``advanced_search`` (**UNVERIFIED**); if it
does, override ``min_call`` to 0.0015. ``official_v2``: $0.005 per post read, $0.010 per user read; zero-result calls
are assumed to bill one post (**UNVERIFIED**; overestimating is safe).
"""
from __future__ import annotations

import json
from typing import Any

PRICES: dict[str, dict[str, float]] = {
    "twitterapi_io": {"per_tweet": 0.00015, "per_profile": 0.00018, "min_call": 0.00015},
    "official_v2": {"per_post": 0.005, "per_user": 0.010},
    "null": {},
}


def parse_override(raw: str | None) -> dict[str, dict[str, float]]:
    """Validate ``X_PRICES_JSON``: only known providers and keys, non-negative numbers (ValueError otherwise)."""
    if not raw or not raw.strip():
        return {}
    try:
        data: Any = json.loads(raw)
    except ValueError as exc:
        raise ValueError(f"X_PRICES_JSON is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError("X_PRICES_JSON must be an object {provider: {key: usd}}")
    out: dict[str, dict[str, float]] = {}
    for prov, vals in data.items():
        if prov not in PRICES or prov == "null":
            raise ValueError(f"X_PRICES_JSON: unknown provider {prov!r}")
        if not isinstance(vals, dict):
            raise ValueError(f"X_PRICES_JSON[{prov!r}] must be an object")
        for k, v in vals.items():
            if k not in PRICES[prov]:
                raise ValueError(f"X_PRICES_JSON[{prov!r}]: unknown key {k!r}")
            if isinstance(v, bool) or not isinstance(v, (int, float)) or v < 0:
                raise ValueError(f"X_PRICES_JSON[{prov!r}][{k!r}] must be a number ≥ 0")
            out.setdefault(prov, {})[k] = float(v)
    return out


def prices_for(provider: str, settings: Any = None) -> dict[str, float]:
    """Effective price table of one provider (defaults ∪ override)."""
    if settings is None:
        import app.config as app_config
        settings = app_config.settings
    base = dict(PRICES.get(provider, {}))
    base.update(parse_override(getattr(settings, "X_PRICES_JSON", "")).get(provider, {}))
    return base
