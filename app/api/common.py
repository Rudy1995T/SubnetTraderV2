"""Small helpers shared by the spec-14 routers: runtime access, param parsing, JSON columns, rounding."""
from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any, Callable

from app.api.errors import ApiError
from app.util.time import now_ts, parse_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.runtime import Runtime
    from app.storage.db import Database


class RuntimeRef:
    """The per-router ``configure(get_runtime)`` slot (01's Phase-0 pattern)."""

    def __init__(self) -> None:
        self._get: Callable[[], "Runtime | None"] | None = None

    def configure(self, get_runtime: Callable[[], "Runtime | None"]) -> None:
        self._get = get_runtime

    def rt(self) -> "Runtime | None":
        if self._get is None:
            return None
        try:
            return self._get()
        except Exception:
            return None

    def rt_or_503(self) -> "Runtime":
        rt = self.rt()
        if rt is None or rt.db is None or not rt.db.connected:
            raise ApiError("db_unavailable", 503, "database not connected")
        return rt

    def db(self) -> "Database":
        return self.rt_or_503().db  # type: ignore[return-value]


def as_bool(v: Any, default: bool = False) -> bool:
    if v is None or v == "":
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    raise ApiError("bad_param", 422, f"not a boolean: {v!r}")


def int_list(v: str | None) -> list[int] | None:
    if not v:
        return None
    try:
        return [int(x) for x in v.split(",") if x.strip()]
    except ValueError:
        raise ApiError("bad_param", 422, f"not a comma list of ints: {v!r}") from None


def str_set(v: str | None) -> set[str] | None:
    if not v:
        return None
    out = {x.strip() for x in v.split(",") if x.strip()}
    return out or None


_REL = re.compile(r"^(\d+)([dhm])$")


def parse_since(v: str | None, default: str = "7d", *, now: int | None = None, code: str = "bad_since") -> int:
    """ISO-8601 or relative ``7d|24h|30m`` → unix seconds."""
    v = (v or default).strip()
    m = _REL.match(v)
    if m:
        n = int(m.group(1)) * {"d": 86400, "h": 3600, "m": 60}[m.group(2)]
        return int(now if now is not None else now_ts()) - n
    try:
        return parse_iso(v)
    except (ValueError, TypeError):
        raise ApiError(code, 422, f"unparsable time {v!r} (ISO-8601 or 7d|24h|30m)") from None


def jloads(x: Any, default: Any = None) -> Any:
    if x is None or x == "":
        return default
    if isinstance(x, (dict, list)):
        return x
    try:
        return json.loads(x)
    except (TypeError, ValueError):
        return default


def r2(x: Any) -> float | None:
    return None if x is None else round(float(x), 2)


def r3(x: Any) -> float | None:
    return None if x is None else round(float(x), 3)
