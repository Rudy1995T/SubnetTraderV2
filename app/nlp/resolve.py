"""Label resolution under a pinned ``LabelPolicy`` (spec 10 §2.2 "Label resolution", §8.2).

A policy is an ordered list of accepted ``classifier_version``s per tier (escalation › bulk › fallback; within a tier
the first listed wins). ``resolve_labels`` returns, per ``post_id``, the highest-precedence row with
``observed_at ≤ as_of`` — so a relabel under a new version never changes what an old ``as_of`` sees. Rows are never updated (03 trigger).
``LABEL_POLICIES`` (one per scoring spec) lives in ``app/scoring/spec.py``.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

TIERS = ("escalation", "bulk", "fallback")


@dataclass(frozen=True)
class LabelPolicy:
    escalation: tuple[str, ...]
    bulk: tuple[str, ...]
    fallback: tuple[str, ...]

    def versions(self) -> tuple[str, ...]:
        return (*self.escalation, *self.bulk, *self.fallback)

    def rank(self, version: str) -> tuple[int, int] | None:
        for i, tier in enumerate(TIERS):
            vs = getattr(self, tier)
            if version in vs:
                return i, vs.index(version)
        return None

    def tier_of(self, version: str) -> str | None:
        r = self.rank(version)
        return TIERS[r[0]] if r else None


@dataclass
class ResolvedLabel:
    post_id: str
    item_kind: str
    item_id: str
    classifier_version: str
    tier: str                          # policy tier: escalation | bulk | fallback
    polarity: str
    intensity: int
    event_type: str
    direction: int
    materiality: int
    is_spam: int
    is_promo: int
    summary: str | None
    confidence: float
    event_at: int
    observed_at: int
    entities: list[dict] = field(default_factory=list)     # post_label_entities rows of the resolved version
    raw: dict = field(default_factory=dict)
    model_resolved: str | None = None
    classifier_tier: str | None = None                     # the row's own tier column (bulk | escalation | rules)

    @property
    def top_entity(self) -> dict | None:
        return max(self.entities, key=lambda e: (float(e["confidence"]), e["via"] == "source"), default=None)

    def links(self, occupant_key: str, min_conf: float) -> bool:
        return any(e["occupant_key"] == occupant_key and float(e["confidence"]) >= min_conf for e in self.entities)


def policy_for(spec_version: str | None = None) -> LabelPolicy:
    """The pinned policy of a scoring spec (default: the running ``SPEC_VERSION``)."""
    import app.config as app_config
    from app.scoring.spec import LABEL_POLICIES
    return LABEL_POLICIES[spec_version or app_config.settings.SPEC_VERSION]


def pinned_bulk_versions() -> set[str]:
    """Bulk versions read by ``SPEC_VERSION`` or any ``SCORING_SHADOW_SPECS`` entry (§2.2 ``version_unpinned``)."""
    import app.config as app_config
    from app.scoring.spec import LABEL_POLICIES
    s = app_config.settings
    out: set[str] = set()
    for v in [s.SPEC_VERSION, *(x.strip() for x in str(s.SCORING_SHADOW_SPECS or "").split(",") if x.strip())]:
        pol = LABEL_POLICIES.get(v)
        if pol:
            out.update(pol.bulk)
    return out


def _case(policy: LabelPolicy) -> tuple[str, list[Any]]:
    """SQL CASE giving the precedence integer (tier·1000 + position) of a classifier_version."""
    parts, args = [], []
    for ti, tier in enumerate(TIERS):
        for pi, v in enumerate(getattr(policy, tier)):
            parts.append("WHEN ? THEN ?")
            args += [v, ti * 1000 + pi]
    return "CASE l.classifier_version " + " ".join(parts) + " END", args


def _row(r: dict, policy: LabelPolicy) -> ResolvedLabel:
    try:
        raw = json.loads(r.get("raw_json") or "{}")
    except ValueError:
        raw = {}
    return ResolvedLabel(
        post_id=r["post_id"], item_kind=r["item_kind"], item_id=r["item_id"],
        classifier_version=r["classifier_version"], tier=policy.tier_of(r["classifier_version"]) or "fallback",
        polarity=r["polarity"], intensity=int(r["intensity"]),
        event_type=r["event_type"], direction=int(r["direction"]), materiality=int(r["materiality"]),
        is_spam=int(r["is_spam"]), is_promo=int(r["is_promo"]), summary=r["summary"],
        confidence=float(r["confidence"]), event_at=int(r["event_at"]), observed_at=int(r["observed_at"]),
        raw=raw if isinstance(raw, dict) else {}, model_resolved=r.get("model_resolved"),
        classifier_tier=r.get("classifier_tier"))


async def _attach_entities(db: "Database", labels: list[ResolvedLabel]) -> None:
    if not labels:
        return
    by = {(x.post_id, x.classifier_version): x for x in labels}
    keys = list(by)
    for i in range(0, len(keys), 400):
        part = keys[i:i + 400]
        where = " OR ".join("(post_id = ? AND classifier_version = ?)" for _ in part)
        rows = await db.fetchall(
            f"SELECT post_id, classifier_version, occupant_key, netuid, confidence, via FROM post_label_entities "
            f"WHERE {where}", [v for k in part for v in k])
        for r in rows:
            lab = by.get((r["post_id"], r["classifier_version"]))
            if lab is not None:
                lab.entities.append({"occupant_key": r["occupant_key"], "netuid": r["netuid"],
                                     "confidence": float(r["confidence"]), "via": r["via"]})


async def resolve_labels(db: "Database", policy: LabelPolicy, *, occupant_key: str | None, since: int, until: int,
                         as_of: int, min_conf: float = 0.6, post_ids: Iterable[str] | None = None
                         ) -> list[ResolvedLabel]:
    """Resolved labels of items with ``event_at ∈ (since, until]`` as visible at ``as_of``. With ``occupant_key``:
    only those whose resolved version links the occupant with ``confidence ≥ min_conf`` (``ix_ple_occ``)."""
    versions = policy.versions()
    if not versions:
        return []
    case, cargs = _case(policy)
    ph = ",".join("?" * len(versions))
    where = [f"l.classifier_version IN ({ph})", "l.observed_at <= ?", "l.event_at > ?", "l.event_at <= ?"]
    args: list[Any] = [*versions, int(as_of), int(since), int(until)]
    if occupant_key is not None:
        where.append(f"l.post_id IN (SELECT e.post_id FROM post_label_entities e WHERE e.occupant_key = ? "
                     f"AND e.classifier_version IN ({ph}) AND e.observed_at <= ?)")
        args += [occupant_key, *versions, int(as_of)]
    if post_ids is not None:
        pids = list(dict.fromkeys(post_ids))
        if not pids:
            return []
        where.append(f"l.post_id IN ({','.join('?' * len(pids))})")
        args += pids
    sql = (f"SELECT * FROM (SELECT l.*, ROW_NUMBER() OVER (PARTITION BY l.post_id ORDER BY {case}) AS rn "
           f"FROM post_labels l WHERE {' AND '.join(where)}) WHERE rn = 1 ORDER BY event_at, post_id")
    rows = await db.fetchall(sql, [*cargs, *args])
    out = [_row(r, policy) for r in rows]
    await _attach_entities(db, out)
    if occupant_key is not None:
        out = [x for x in out if x.links(occupant_key, min_conf)]
    return out


async def resolve(db: "Database", policy: LabelPolicy, post_id: str, as_of: int) -> ResolvedLabel | None:
    """The single resolved label of ``post_id`` at ``as_of`` (any ``event_at``)."""
    res = await resolve_labels(db, policy, occupant_key=None, since=-(2 ** 62), until=2 ** 62, as_of=as_of,
                               post_ids=[post_id])
    return res[0] if res else None


__all__ = ["LabelPolicy", "ResolvedLabel", "resolve_labels", "resolve", "policy_for", "pinned_bulk_versions", "TIERS"]
