"""Shared text helpers (spec 11 §8.1, spec 12 §8): the old fundamentals scorer's anonymity tokens.

``_ANON_TOKENS`` and ``_looks_anonymous`` are copied verbatim from the old bot's ``app/strategy/quality.py``
(line 33 and lines 108-110; the copy is ``seed/app/strategy/quality.py``). ``looks_anonymous`` is the public alias
spec 12 imports.
"""
from __future__ import annotations

# Tokens that mean "no real team is attributed" when found in the owner/team
# free-text. Matched as whole-ish words, case-insensitive.
_ANON_TOKENS = ("anonymous", "anon", "unknown", "undisclosed", "tbd", "n/a", "none")


def _looks_anonymous(text: str) -> bool:
    low = f" {text.strip().lower()} "
    return any(f" {tok} " in low or f" {tok}," in low for tok in _ANON_TOKENS)


looks_anonymous = _looks_anonymous
