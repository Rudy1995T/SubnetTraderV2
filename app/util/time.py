"""Time helpers (02 §B1): every stored instant is INTEGER unix seconds UTC.

ISO-8601 ``…Z`` strings exist only at boundaries (HTTP JSON, Telegram text, logs).
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

UTC = timezone.utc


def now_ts() -> int:
    return int(time.time())


def utc_now() -> datetime:
    return datetime.now(UTC)


def floor_hour(ts: int) -> int:
    ts = int(ts)
    return ts - ts % 3600


def floor_day(ts: int) -> int:
    ts = int(ts)
    return ts - ts % 86400


def to_iso(ts: int | float | None) -> str | None:
    """``2026-09-23T10:00:00Z`` (seconds precision); None → None."""
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso_dt(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z") or s.endswith("z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def parse_iso(s: str) -> int:
    """Accepts ``…Z``, ``+00:00`` and naive (= UTC) strings; returns int unix seconds."""
    return int(_parse_iso_dt(s).timestamp())


def parse_ts(x: int | float | str | datetime) -> int:
    """int/float unix seconds, ISO string, or aware datetime → int unix seconds."""
    if isinstance(x, bool):
        raise TypeError("bool is not a timestamp")
    if isinstance(x, (int, float)):
        return int(x)
    if isinstance(x, datetime):
        if x.tzinfo is None:
            x = x.replace(tzinfo=UTC)
        return int(x.timestamp())
    if isinstance(x, str):
        s = x.strip()
        try:
            return int(float(s))
        except ValueError:
            return parse_iso(s)
    raise TypeError(f"cannot parse timestamp from {type(x).__name__}")


def parse_dt(x: int | float | str | datetime) -> datetime:
    """Aware UTC datetime (for ported code that calls ``.timestamp()``)."""
    if isinstance(x, datetime):
        return (x if x.tzinfo else x.replace(tzinfo=UTC)).astimezone(UTC)
    return datetime.fromtimestamp(parse_ts(x), UTC)


def month_key(ts: int) -> str:
    """``YYYY-MM`` (UTC) — the ``provider_spend.month`` key."""
    return datetime.fromtimestamp(int(ts), UTC).strftime("%Y-%m")


def hhmm(s: str) -> tuple[int, int]:
    """``"02:10"`` → ``(2, 10)``; ValueError on bad input."""
    if not isinstance(s, str) or ":" not in s:
        raise ValueError(f"expected HH:MM, got {s!r}")
    h, m = s.strip().split(":", 1)
    if not (h.isdigit() and m.isdigit()):
        raise ValueError(f"expected HH:MM, got {s!r}")
    hi, mi = int(h), int(m)
    if not (0 <= hi <= 23 and 0 <= mi <= 59):
        raise ValueError(f"HH:MM out of range: {s!r}")
    return hi, mi


_DOW = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


def dow_hhmm(s: str) -> tuple[str, int, int]:
    """``"sun 05:00"`` → ``("sun", 5, 0)``."""
    parts = s.strip().lower().split()
    if len(parts) != 2 or parts[0] not in _DOW:
        raise ValueError(f"expected 'dow HH:MM', got {s!r}")
    h, m = hhmm(parts[1])
    return parts[0], h, m


def in_utc_windows(ts: int, windows: str | list[str] | tuple[str, ...]) -> tuple[bool, int | None]:
    """Whether ``ts`` falls inside any ``"HH:MM-HH:MM"`` UTC window; returns (inside, end_ts_of_window)."""
    if isinstance(windows, str):
        windows = [w for w in windows.split(",") if w.strip()]
    day = floor_day(ts)
    for w in windows:
        a, b = w.strip().split("-", 1)
        ah, am = hhmm(a)
        bh, bm = hhmm(b)
        start = day + ah * 3600 + am * 60
        end = day + bh * 3600 + bm * 60
        if start <= ts < end:
            return True, end
    return False, None
