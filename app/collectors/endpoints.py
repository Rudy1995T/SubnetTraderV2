"""RPC endpoint failover (spec 05 §8). Re-implemented from the documented contract of the old
``app/chain/endpoints.py`` (the source was not on the laptop — BUILD-LOG): ``is_dead_socket_error``,
``is_quota_error`` and ``EndpointSelector`` (primary first; a failed endpoint cools down, then is retried)."""
from __future__ import annotations

import time
from typing import Callable

_DEAD = ("connection closed", "connection reset", "broken pipe", "websocket", "eof", "timed out", "timeout",
         "cannot connect", "connection refused", "remote end closed", "name or service not known")
_QUOTA = ("-32029", "429", "too many requests", "rate limit", "insufficient compute units", "quota", "exceeded")


def is_dead_socket_error(exc: BaseException | str) -> bool:
    s = str(exc).lower()
    return isinstance(exc, (ConnectionError, TimeoutError)) or any(k in s for k in _DEAD)


def is_quota_error(exc: BaseException | str) -> bool:
    s = str(exc).lower()
    return any(k in s for k in _QUOTA)


class EndpointSelector:
    def __init__(self, urls: list[str], cooldown_sec: float = 300.0,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.urls = [u for u in urls if u]
        if not self.urls:
            raise ValueError("EndpointSelector needs at least one URL")
        self.cooldown_sec = float(cooldown_sec)
        self._clock = clock
        self._bad_until: dict[str, float] = {}

    def current(self) -> str:
        now = self._clock()
        for u in self.urls:
            if self._bad_until.get(u, 0.0) <= now:
                return u
        return min(self.urls, key=lambda u: self._bad_until.get(u, 0.0))  # all cooling: least-recently failed

    def candidates(self) -> list[str]:
        first = self.current()
        return [first] + [u for u in self.urls if u != first]

    def mark_bad(self, url: str, exc: BaseException | str | None = None) -> None:
        self._bad_until[url] = self._clock() + self.cooldown_sec

    def mark_ok(self, url: str) -> None:
        self._bad_until.pop(url, None)
