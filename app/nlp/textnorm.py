"""The one text normaliser and hash for X, Discord and Telegram (spec 08 §2.6 step 7, 02 §B11).

Defined by 08; 09 (X) and 10 (LLM queue) import it so cross-platform dedupe compares like with like. The order is
fixed: NFKC → lower → URLs → mentions → whitespace runs → strip.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata

_URL = re.compile(r"https?://\S+")
_MENTION = re.compile(r"@\w+")
_WS = re.compile(r"\s+")


def text_norm(text: str | None) -> str:
    s = unicodedata.normalize("NFKC", text or "").lower()
    s = _URL.sub(" ", s)
    s = _MENTION.sub(" ", s)
    return _WS.sub(" ", s).strip()


def text_hash(text: str | None) -> str:
    return hashlib.sha256(text_norm(text).encode("utf-8")).hexdigest()
