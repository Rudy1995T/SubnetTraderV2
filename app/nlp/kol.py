"""KOL registry, bot/shill heuristics and the single-account cap (spec 09 §2.7, §2.8, §3.4).

``author_credibility`` and ``cap_author_share`` are pure; their thresholds are module constants frozen with the
scoring ``spec_version`` (not config). Spec 10 §3.4 imports ``cap_author_share`` for ``polarity_7d``.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Iterable, Sequence

from app.nlp.match import PROMO_RE
from app.nlp.textnorm import text_norm

if TYPE_CHECKING:  # pragma: no cover
    from app.collectors.x_providers.base import XPost, XUser
    from app.storage.db import Database

# §2.7 multipliers (frozen)
AGE_YOUNG_DAYS, AGE_NEWISH_DAYS = 30, 180
M_YOUNG, M_NEWISH = 0.3, 0.7
FOLLOWING_MIN, RATIO_MAX, M_FOLLOW_RATIO = 1000, 0.1, 0.5
TINY_FOLLOWERS, M_TINY = 50, 0.6
FIREHOSE_PER_DAY, FAST_PER_DAY, M_FIREHOSE, M_FAST = 100.0, 30.0, 0.2, 0.6
AUTOHANDLE_RE = re.compile(r"^[A-Za-z]+\d{5,}$")
M_AUTOHANDLE = 0.7
M_DUP = 0.3
M_PROMO = 0.2
LINK_ONLY_CHARS, M_LINK_ONLY = 20, 0.5
UNPROFILED_LEVEL = 0.7
KOL_CANDIDATE_MIN_CRED, KOL_CANDIDATE_MIN_SEEN = 0.6, 5
DEFAULT_CREDIBILITY = {"official": 0.9, "founder": 0.8, "explorer": 0.8, "fund": 0.7, "analyst": 0.6, "media": 0.6,
                       "community": 0.5}


@dataclass(frozen=True)
class KolRow:
    handle: str
    handle_lower: str
    credibility: float
    verified: bool
    active: bool
    category: str = "analyst"
    author_id: str | None = None
    display_name: str | None = None
    team_occupant_keys: tuple[str, ...] = ()

    @property
    def pollable(self) -> bool:                       # §2.5: verified, active, credibility > 0
        return self.verified and self.active and self.credibility > 0


class KolRegistry:
    """In-memory index of ``kol_accounts`` by ``handle_lower``."""

    def __init__(self, rows: Iterable[KolRow]) -> None:
        self.rows = {r.handle_lower: r for r in rows}

    @classmethod
    async def load(cls, db: "Database") -> "KolRegistry":
        rows = await db.fetchall("SELECT * FROM kol_accounts")
        out = []
        for r in rows:
            try:
                team = tuple(json.loads(r["team_occupant_keys"] or "[]"))
            except ValueError:
                team = ()
            out.append(KolRow(r["handle"], r["handle_lower"], float(r["credibility"]), bool(r["verified"]),
                              bool(r["active"]), r["category"], r["author_id"], r["display_name"], team))
        return cls(out)

    def get(self, handle: str | None) -> KolRow | None:
        return self.rows.get(handle.lower()) if handle else None

    def pollable(self) -> list[KolRow]:
        return sorted((r for r in self.rows.values() if r.pollable), key=lambda r: r.handle_lower)

    def __len__(self) -> int:
        return len(self.rows)


def _age_days(user: "XUser", at: datetime) -> float | None:
    if user.created_at is None:
        return None
    return max(0.0, (at - user.created_at).total_seconds() / 86400.0)


def account_level(user: "XUser | None", at: datetime) -> tuple[float, list[str]]:
    """Account-level factor for an author that is neither team nor KOL (09 §2.7); ``at`` = the post's time."""
    if user is None:
        return UNPROFILED_LEVEL, ["unprofiled"]
    m, flags = 1.0, []
    age = _age_days(user, at)
    if age is not None:
        if age < AGE_YOUNG_DAYS:
            m *= M_YOUNG
            flags.append("young")
        elif age < AGE_NEWISH_DAYS:
            m *= M_NEWISH
            flags.append("newish")
    if user.following is not None and user.followers is not None and user.following > FOLLOWING_MIN \
            and user.followers / (user.following + 1) < RATIO_MAX:
        m *= M_FOLLOW_RATIO
        flags.append("follow_ratio")
    if user.followers is not None and user.followers < TINY_FOLLOWERS:
        m *= M_TINY
        flags.append("tiny")
    if user.post_count is not None and age is not None:
        rate = user.post_count / max(age, 1.0)
        if rate > FIREHOSE_PER_DAY:
            m *= M_FIREHOSE
            flags.append("firehose")
        elif rate >= FAST_PER_DAY:
            m *= M_FAST
            flags.append("fast")
    if AUTOHANDLE_RE.match(user.handle or ""):
        m *= M_AUTOHANDLE
        flags.append("autohandle")
    return m, flags


def duplicate_flags(post: "XPost", batch: Sequence["XPost"], *, window_s: int, db_self: int = 0,
                    db_coord: int = 0) -> list[str]:
    """``dup_self``: the same normalised text from the same author within ±window (batch ∪ DB); ``dup_coord``: the
    same text from ≥ 2 different authors (this one included) within ±window."""
    h = text_norm(post.text)
    if not h:
        return []
    t0 = post.event_at.timestamp()
    same_author = db_self
    authors = {post.author_id}
    for o in batch:
        if o.post_id == post.post_id or abs(o.event_at.timestamp() - t0) > window_s or text_norm(o.text) != h:
            continue
        if o.author_id == post.author_id:
            same_author += 1
        else:
            authors.add(o.author_id)
    flags = []
    if same_author > 0:
        flags.append("dup_self")
    if len(authors) + db_coord >= 2:
        flags.append("dup_coord")
    return flags


def author_credibility(user: "XUser | None", post: "XPost", batch: Sequence["XPost"], db_dupes: int,
                       kol: KolRow | None, is_team: bool, *, db_coord: int = 0, window_s: int = 600
                       ) -> tuple[float, list[str]]:
    """``clamp(account_level × Π post_level, 0, 1)`` and the flags (09 §2.7). ``db_dupes`` = same-author posts with
    the same text hash in the DB within ±window; ``db_coord`` = other authors with it (09 changelog 2026-10-18: the
    spec's single ``db_dupes`` int split in two). Team → account-level 1.0; KOL → ``kol.credibility``; both keep the
    post-level factors."""
    if is_team:
        level, flags = 1.0, ["team"]
    elif kol is not None:
        level, flags = float(kol.credibility), ["kol"]
    else:
        level, flags = account_level(user, post.event_at)
    post_m = 1.0
    dups = duplicate_flags(post, batch, window_s=window_s, db_self=db_dupes, db_coord=db_coord)
    if dups:
        post_m *= M_DUP
        flags += dups
    if PROMO_RE.search(post.text or ""):
        post_m *= M_PROMO
        flags.append("promo")
    if post.has_url and len(text_norm(post.text)) < LINK_ONLY_CHARS:
        post_m *= M_LINK_ONLY
        flags.append("link_only")
    return min(1.0, max(0.0, level * post_m)), flags


def is_bot_filtered(credibility: float, *, is_team: bool, threshold: float) -> bool:
    """Team posts are never ``bot_filtered`` (09 §2.7)."""
    return (not is_team) and credibility < threshold


def cap_author_share(weights: dict[str, float], cap: float = 0.25) -> dict[str, float]:
    """Water-fill (09 §2.8). S = Σ weights, n = authors with weight > 0, c = max(cap, 1/n). Returns w' with Σ w' = S
    and w'_a ≤ c·S: capped authors get exactly c·S, the rest share the remaining mass in proportion to their
    original weights, repeated until no uncapped author exceeds c·S. Zero weights stay 0; {} → {}."""
    if not weights:
        return {}
    pos = {a: float(w) for a, w in weights.items() if w and w > 0}
    out = {a: 0.0 for a in weights}
    if not pos:
        return out
    total = sum(pos.values())
    c = max(float(cap), 1.0 / len(pos))
    limit = c * total
    capped: set[str] = set()
    while True:
        free = {a: w for a, w in pos.items() if a not in capped}
        rest = total - limit * len(capped)
        fsum = sum(free.values())
        if not free or fsum <= 0:
            break
        new = {a for a, w in free.items() if rest * w / fsum > limit * (1 + 1e-12)}
        if not new:
            break
        capped |= new
    rest = total - limit * len(capped)
    fsum = sum(w for a, w in pos.items() if a not in capped)
    for a, w in pos.items():
        out[a] = limit if a in capped else (rest * w / fsum if fsum > 0 else 0.0)
    return out


__all__ = ["KolRow", "KolRegistry", "author_credibility", "account_level", "duplicate_flags", "is_bot_filtered",
           "cap_author_share", "UNPROFILED_LEVEL", "DEFAULT_CREDIBILITY", "KOL_CANDIDATE_MIN_CRED",
           "KOL_CANDIDATE_MIN_SEEN"]
