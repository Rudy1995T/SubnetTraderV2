"""X handle normalisation and packed-query chunking (spec 09 §2.4, §2.3).

``normalise_x_handle`` delegates URL parsing to 04's ``app/registry/normalise.py`` (one parser for the whole system)
and adds 09's checks: the ``#!/`` hash-bang form, the full reserved-path list (incl. ``login``, ``status``) and the
``/H/status/…`` form. The handle keeps its case; compare and index on ``lower()``.
"""
from __future__ import annotations

import re
from typing import Iterable

from app.registry.normalise import normalise_x_handle as _registry_normalise

RESERVED = frozenset({"i", "intent", "share", "home", "search", "hashtag", "explore", "settings", "login", "messages",
                      "compose", "notifications", "status", "statuses", "signup", "logout", "tos", "privacy", "about"})
HANDLE_RE = re.compile(r"^[A-Za-z0-9_]{1,15}$")
FROM_CHARS = len("from:") + len(" OR ")           # per-handle overhead in a packed ``(from:a OR from:b)`` group
QUERY_SUFFIX_RESERVE = 60                          # room for " -filter:retweets since:YYYY-MM-DD_HH:MM:SS_UTC lang:xx"


def normalise_x_handle(url_or_handle: str | None) -> str | None:
    """``https://x.com/H``, ``twitter.com/H``, ``x.com/#!/H``, ``@H``, ``H``, ``x.com/H/status/1`` → ``H`` (case kept);
    reserved paths, status URLs without a handle and invalid handles → None."""
    if not url_or_handle or not isinstance(url_or_handle, str):
        return None
    s = url_or_handle.strip()
    s = re.sub(r"/#!/", "/", s)                     # legacy hash-bang URLs
    s = re.sub(r"^#!/", "", s)
    h = _registry_normalise(s)
    if h is None:
        return None
    if h.lower() in RESERVED or not HANDLE_RE.match(h):
        return None
    return h


def packed_len(handles: Iterable[str]) -> int:
    hs = list(handles)
    if not hs:
        return 0
    n = sum(len("from:") + len(h) for h in hs) + len(" OR ") * (len(hs) - 1)
    return n + (2 if len(hs) > 1 else 0)


def chunk_handles(handles: Iterable[str], max_per_query: int = 25, max_chars: int = 512, *,
                  reserve: int = QUERY_SUFFIX_RESERVE) -> list[list[str]]:
    """Greedy packing of ``from:`` handles: ≤ ``max_per_query`` handles and a rendered ``(from:a OR …)`` group of
    ≤ ``max_chars − reserve`` characters per chunk (official recent search: 512-char queries; 09 §2.3). Order and
    case are kept; duplicates (case-insensitive) are dropped."""
    seen: set[str] = set()
    out: list[list[str]] = []
    cur: list[str] = []
    budget = max(1, max_chars - reserve)
    for h in handles:
        if not h or h.lower() in seen:
            continue
        seen.add(h.lower())
        if cur and (len(cur) >= max_per_query or packed_len(cur + [h]) > budget):
            out.append(cur)
            cur = []
        cur.append(h)
    if cur:
        out.append(cur)
    return out


__all__ = ["normalise_x_handle", "chunk_handles", "packed_len", "RESERVED"]
