"""X providers (spec 09 §2.2–§2.3): ``make_provider(settings, http)`` picks one by ``X_PROVIDER``."""
from __future__ import annotations

from typing import Any

from app.collectors.x_providers.base import XProvider, XProviderError

PROVIDER_NAMES = ("null", "twitterapi_io", "official_v2")


def make_provider(settings: Any, http: Any) -> XProvider:
    """``X_PROVIDER`` → provider instance. An unknown value is a config error (01's validator rejects it at boot);
    a missing key is not checked here — ``settings.x_active()`` keeps the paid jobs disabled (09 §5)."""
    name = str(getattr(settings, "X_PROVIDER", "null") or "null").lower()
    if name == "twitterapi_io":
        from app.collectors.x_providers.twitterapi_io import TwitterApiIo
        return TwitterApiIo(http, settings)
    if name == "official_v2":
        from app.collectors.x_providers.official_v2 import OfficialV2
        return OfficialV2(http, settings)
    if name == "null":
        from app.collectors.x_providers.null import NullProvider
        return NullProvider(http, settings)
    raise ValueError(f"unknown X_PROVIDER {name!r} (expected one of {PROVIDER_NAMES})")


__all__ = ["make_provider", "PROVIDER_NAMES", "XProvider", "XProviderError"]
