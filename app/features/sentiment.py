"""P6 Narrative/Sentiment producers (spec 10 §3.3, §3.4; 02 §C4/§C5; registry rows 42–45, ``source='llm'``).

``compute_<key>(db, occupant_key, as_of)`` with ``as_of`` = 12's ``T_eval`` (tz-aware UTC ``datetime``). Every read has
``observed_at ≤ t``; labels are resolved at ``t`` under the running scoring spec's ``LABEL_POLICY``, so a later relabel
never changes an earlier value. ``L(t)`` = resolved labels linked to the occupant (confidence ≥ 0.6) with
``is_spam = 0``; ``A(t, W)`` = label-free attributable items (source binding, ``social_post_mentions`` other than
``entity`` rows, news/video ``occupant_keys_json``) that pass the privacy/bot gates. P6 fns never return ``absent``
(it would normalise to 0.0, the most negative event impact): nothing happened is ``ok`` 0, undefined is
``not_applicable``, unmeasurable is ``failed``. Frozen parameters live in ``app/scoring/spec.py``.
"""
from __future__ import annotations

import json
import math
from datetime import datetime
from statistics import fmean, pstdev
from typing import TYPE_CHECKING, Any

from app.features.types import MetricResult
from app.nlp.events import confirmed_posts, needs_confirmation
from app.nlp.kol import cap_author_share
from app.nlp.resolve import LabelPolicy, ResolvedLabel, policy_for, resolve_labels
from app.scoring import spec as SP
from app.util.time import floor_day, parse_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

DAY = 86400


def _t(as_of: Any) -> int:
    if not isinstance(as_of, datetime):
        raise TypeError(f"as_of must be a tz-aware datetime (02 §C5), got {type(as_of).__name__}")
    if as_of.tzinfo is None:
        raise TypeError("as_of must be tz-aware (UTC)")
    return parse_ts(as_of)


def _ok(v: float, **vj: Any) -> MetricResult:
    return MetricResult(float(v), vj)


def _failed(reason: str, **vj: Any) -> MetricResult:
    return MetricResult(None, {"reason": reason, **vj})


def _na(reason: str, **vj: Any) -> MetricResult:
    return MetricResult(None, {"status": "not_applicable", "reason": reason, **vj})


async def _L(db: "Database", policy: LabelPolicy, occ: str, since: int, until: int, t: int) -> list[ResolvedLabel]:
    labs = await resolve_labels(db, policy, occupant_key=occ, since=since, until=until, as_of=t,
                                min_conf=SP.P6_ENTITY_MIN_CONFIDENCE)
    return [x for x in labs if not x.is_spam]


# ── label-free attribution A(t, W) ────────────────────────────────────────────────────────────────────────────
async def attributable(db: "Database", occ: str, since: int, until: int, t: int, *,
                       kinds: tuple[str, ...] = ("x", "ann", "news", "video")) -> dict[str, dict]:
    """{post_id: {kind, kol_handle, is_kol}} for items with ``event_at ∈ (since, until]`` observed by ``t``."""
    out: dict[str, dict] = {}
    if "x" in kinds:
        rows = await db.fetchall(
            "SELECT sp.item_id, sp.is_kol, sp.kol_handle FROM social_posts sp WHERE sp.platform = 'x' "
            "AND sp.bot_filtered = 0 AND sp.event_at > ? AND sp.event_at <= ? AND sp.observed_at <= ? AND "
            "(sp.occupant_key = ? OR EXISTS (SELECT 1 FROM social_post_mentions m WHERE m.platform = 'x' AND "
            "m.post_id = sp.post_id AND m.occupant_key = ? AND m.match_kind <> 'entity' AND m.observed_at <= ?))",
            (since, until, t, occ, occ, t))
        for r in rows:
            out[r["item_id"]] = {"kind": "x", "is_kol": int(r["is_kol"] or 0), "kol_handle": r["kol_handle"]}
    if "ann" in kinds:
        rows = await db.fetchall(
            "SELECT a.msg_id FROM announcements a WHERE (a.platform = 'telegram' OR a.via_own_server = 1) "
            "AND COALESCE(a.text_len, length(a.text), 0) > 0 AND a.event_at > ? AND a.event_at <= ? "
            "AND a.observed_at <= ? AND (a.occupant_key = ? OR EXISTS (SELECT 1 FROM social_post_mentions m WHERE "
            "m.platform = a.platform AND a.msg_id = m.platform || ':' || m.post_id AND m.occupant_key = ? "
            "AND m.match_kind <> 'entity' AND m.observed_at <= ?))", (since, until, t, occ, occ, t))
        for r in rows:
            out[r["msg_id"]] = {"kind": "ann"}
    if "news" in kinds:
        rows = await db.fetchall(
            "SELECT n.url_hash FROM news_items n WHERE n.event_at > ? AND n.event_at <= ? AND n.observed_at <= ? "
            "AND EXISTS (SELECT 1 FROM json_each(n.occupant_keys_json) j WHERE "
            "(CASE j.type WHEN 'object' THEN json_extract(j.value, '$.occupant_key') ELSE j.value END) = ?)", (since, until, t, occ))
        for r in rows:
            out[f"news_item:{r['url_hash']}"] = {"kind": "news"}
    if "video" in kinds:
        rows = await db.fetchall(
            "SELECT v.video_id FROM video_items v WHERE v.event_at > ? AND v.event_at <= ? AND v.observed_at <= ? "
            "AND (v.occupant_key = ? OR EXISTS (SELECT 1 FROM json_each(v.occupant_keys_json) j WHERE "
            "(CASE j.type WHEN 'object' THEN json_extract(j.value, '$.occupant_key') ELSE j.value END) = ?))", (since, until, t, occ, occ))
        for r in rows:
            out[f"video_item:{r['video_id']}"] = {"kind": "video"}
    return out


async def labelled_at(db: "Database", policy: LabelPolicy, post_ids: list[str], t: int) -> set[str]:
    """Post ids with any label of a pinned version observed by ``t``."""
    vs = policy.versions()
    out: set[str] = set()
    for i in range(0, len(post_ids), 400):
        part = post_ids[i:i + 400]
        rows = await db.fetchall(
            f"SELECT DISTINCT post_id FROM post_labels WHERE post_id IN ({','.join('?' * len(part))}) AND "
            f"classifier_version IN ({','.join('?' * len(vs))}) AND observed_at <= ?", [*part, *vs, t])
        out.update(r["post_id"] for r in rows)
    return out


async def coverage(db: "Database", policy: LabelPolicy, occ: str, since: int, until: int, t: int) -> float:
    a = await attributable(db, occ, since, until, t)
    if not a:
        return 1.0
    return len(await labelled_at(db, policy, list(a), t)) / len(a)


# ── event_impact_14d ──────────────────────────────────────────────────────────────────────────────────────────
async def compute_event_impact_14d(db: "Database", occupant_key: str, as_of: datetime, *,
                                   policy: LabelPolicy | None = None) -> MetricResult:
    t = _t(as_of)
    pol = policy or policy_for()
    lo = t - SP.P6_EVENT_WINDOW_S
    labs = [x for x in await _L(db, pol, occupant_key, lo, t, t) if x.materiality >= 2]
    pending = [x for x in labs if needs_confirmation(x)]
    confirmed = await confirmed_posts(db, occupant_key, t) if pending else set()
    counted = [x for x in labs if not needs_confirmation(x) or x.post_id in confirmed]
    clusters: dict[tuple[str, int, int], list[ResolvedLabel]] = {}
    for x in counted:
        clusters.setdefault((x.event_type, x.direction, x.event_at // SP.P6_CLUSTER_WIDTH_S), []).append(x)
    contrib = []
    for (et, d, _b), members in clusters.items():
        m = max(y.materiality for y in members)
        tau = min(y.event_at for y in members)
        c = m * d * 0.5 ** ((t - tau) / SP.P6_HALF_LIFE_S)
        top = sorted(members, key=lambda y: (-y.materiality, y.event_at, y.post_id))[0]
        contrib.append((c, {"event_type": et, "direction": d, "materiality": m, "event_at": tau,
                            "post_id": top.post_id}))
    x = math.fsum(c for c, _ in contrib)
    cov = await coverage(db, pol, occupant_key, lo, t, t)
    vj = {"n_events": len(contrib),
          "top": [r for _, r in sorted(contrib, key=lambda z: (-abs(z[0]), z[1]["event_at"]))[:5]],
          "coverage": round(cov, 4), "pending_confirmation": len([p for p in pending if p.post_id not in confirmed])}
    if x != 0:
        return _ok(round(x, 6), **vj)
    if cov >= SP.P6_COVERAGE_MIN:
        return _ok(0.0, **vj)
    return _failed("labels_pending", **vj)


# ── polarity_7d ───────────────────────────────────────────────────────────────────────────────────────────────
async def compute_polarity_7d(db: "Database", occupant_key: str, as_of: datetime, *,
                              policy: LabelPolicy | None = None) -> MetricResult:
    t = _t(as_of)
    pol = policy or policy_for()
    lo = t - SP.P6_POLARITY_WINDOW_S
    labs = [x for x in await _L(db, pol, occupant_key, lo, t, t)
            if x.item_kind in ("social_post", "announcement") and not x.is_promo]
    xs = [x.post_id[2:] for x in labs if x.item_kind == "social_post"]
    anns = [x.post_id for x in labs if x.item_kind == "announcement"]
    xrows: dict[str, dict] = {}
    for i in range(0, len(xs), 400):
        part = xs[i:i + 400]
        for r in await db.fetchall(
                "SELECT post_id, author_id, author_credibility, bot_filtered, like_count, repost_count, reply_count, "
                f"quote_count FROM social_posts WHERE platform = 'x' AND post_id IN ({','.join('?' * len(part))}) "
                "AND observed_at <= ?", [*part, t]):
            xrows["x:" + r["post_id"]] = r
    arows: dict[str, dict] = {}
    for i in range(0, len(anns), 400):
        part = anns[i:i + 400]
        for r in await db.fetchall(f"SELECT msg_id, source_channel_id FROM announcements WHERE msg_id IN "
                                   f"({','.join('?' * len(part))}) AND observed_at <= ?", [*part, t]):
            arows[r["msg_id"]] = r
    items: list[tuple[str, float, float, str]] = []           # (author, w, s, polarity)
    for x in labs:
        if x.item_kind == "social_post":
            r = xrows.get(x.post_id)
            if r is None or r["bot_filtered"]:
                continue
            c = r["author_credibility"]
            c = SP.P6_UNPROFILED_CREDIBILITY if c is None else float(c)
            e = (r["like_count"] or 0) + 2 * (r["repost_count"] or 0) + (r["reply_count"] or 0) \
                + (r["quote_count"] or 0)
            author = f"x:{r['author_id']}"
        else:
            r = arows.get(x.post_id)
            if r is None:
                continue
            c, e, author = 1.0, 0, f"ch:{r['source_channel_id']}"
        w = c * (1.0 + math.log1p(max(0, e)))
        p = {"bullish": 1, "bearish": -1}.get(x.polarity, 0)
        items.append((author, w, 0.5 + 0.1 * p * x.intensity, x.polarity))
    n = len(items)
    if n < SP.P6_MIN_POSTS:
        a = await attributable(db, occupant_key, lo, t, t, kinds=("x", "ann"))
        lab_ids = await labelled_at(db, pol, list(a), t)
        unlabelled = len([p for p in a if p not in lab_ids])
        vj = {"n_items": n, "unlabelled_posts": unlabelled}
        if n + unlabelled >= SP.P6_MIN_POSTS:
            return _failed("labels_pending", **vj)
        return _na("lt_5_posts", **vj)
    W: dict[str, float] = {}
    for a_, w, _s, _p in items:
        W[a_] = W.get(a_, 0.0) + w
    capped = cap_author_share(W, cap=SP.P6_AUTHOR_CAP)
    num = den = bull = bear = 0.0
    for a_, w, s_, p_ in items:
        wp = w * capped[a_] / W[a_] if W[a_] > 0 else 0.0
        num += wp * s_
        den += wp
        bull += wp if p_ == "bullish" else 0.0
        bear += wp if p_ == "bearish" else 0.0
    if den <= 0:
        return _na("zero_weight", n_items=n)
    return _ok(round(num / den, 6), n_items=n, n_authors=len(W),
               capped_authors=sum(1 for k in W if capped[k] < W[k] * (1 - 1e-12)),
               bullish_share=round(bull / den, 6), bearish_share=round(bear / den, 6), weight_total=round(den, 6))


# ── attention_7d ──────────────────────────────────────────────────────────────────────────────────────────────
def _z(recent: list[float], base: list[float], floor: float) -> float:
    mb = fmean(base)
    return (fmean(recent) - mb) / max(pstdev(base), floor)


async def compute_attention_7d(db: "Database", occupant_key: str, as_of: datetime, *,
                               policy: LabelPolicy | None = None) -> MetricResult:
    t = _t(as_of)
    pol = policy or policy_for()
    d0 = floor_day(t)
    r_lo = d0 - SP.P6_ATTENTION_RECENT_DAYS * DAY
    b_lo = r_lo - SP.P6_ATTENTION_BASELINE_DAYS * DAY
    rows = await db.fetchall("SELECT day_ts, views FROM chain_trending_daily WHERE occupant_key = ? AND day_ts >= ? "
                             "AND day_ts < ? AND observed_at <= ?", (occupant_key, b_lo, d0, t))
    views = {int(r["day_ts"]): float(r["views"]) for r in rows}
    vr = [v for d, v in views.items() if d >= r_lo]
    vb = [v for d, v in views.items() if d < r_lo]
    # mentions: labelled items about the occupant, not source-bound to it; valid from the first pinned label's day
    vs = pol.versions()
    first = await db.fetchval(f"SELECT MIN(observed_at) FROM post_labels WHERE classifier_version IN "
                              f"({','.join('?' * len(vs))}) AND observed_at <= ?", [*vs, t])
    mr: list[float] = []
    mb: list[float] = []
    m7 = 0
    if first is not None:
        labs = await _L(db, pol, occupant_key, b_lo - 1, d0 - 1, t)
        own = await _source_bound(db, occupant_key, [x for x in labs])
        counts: dict[int, int] = {}
        for x in labs:
            if x.post_id in own:
                continue
            day = floor_day(x.event_at)
            counts[day] = counts.get(day, 0) + 1
        valid_from = floor_day(int(first))
        for day in range(max(b_lo, valid_from), d0, DAY):
            v = math.log1p(counts.get(day, 0))
            (mr if day >= r_lo else mb).append(v)
            if day >= r_lo:
                m7 += counts.get(day, 0)
    zs: dict[str, float] = {}
    if len(vb) >= SP.P6_ATTENTION_MIN_BASELINE and len(vr) >= SP.P6_ATTENTION_MIN_RECENT:
        floor_v = max(SP.P6_ATTENTION_VIEWS_FLOOR_MIN, SP.P6_ATTENTION_VIEWS_FLOOR_REL * fmean(vb))
        zs["views"] = _z(vr, vb, floor_v)
    if len(mb) >= SP.P6_ATTENTION_MIN_BASELINE and len(mr) >= SP.P6_ATTENTION_MIN_RECENT:
        zs["mentions"] = _z(mr, mb, SP.P6_ATTENTION_MENT_FLOOR)
    vj = {"z_views": round(zs["views"], 6) if "views" in zs else None,
          "z_mentions": round(zs["mentions"], 6) if "mentions" in zs else None,
          "baseline_days_views": len(vb), "baseline_days_mentions": len(mb), "mentions_7d": m7,
          "views_7d": sum(vr) if vr else None}
    if not zs:
        return _failed("baseline_short", **vj)
    return _ok(round(fmean(zs.values()), 6), **vj)


async def _source_bound(db: "Database", occ: str, labs: list[ResolvedLabel]) -> set[str]:
    """The occupant's own posts, announcements and channel videos among ``labs``."""
    out: set[str] = set()
    by_kind: dict[str, list[ResolvedLabel]] = {}
    for x in labs:
        by_kind.setdefault(x.item_kind, []).append(x)
    specs = {"social_post": ("SELECT 'x:' || post_id AS pid FROM social_posts WHERE platform = 'x' AND "
                             "occupant_key = ? AND post_id IN ({ph})", lambda p: p[2:]),
             "announcement": ("SELECT msg_id AS pid FROM announcements WHERE occupant_key = ? AND msg_id IN ({ph})",
                              lambda p: p),
             "video_item": ("SELECT 'video_item:' || video_id AS pid FROM video_items WHERE occupant_key = ? AND "
                            "video_id IN ({ph})", lambda p: p[len("video_item:"):])}
    for kind, (sql, conv) in specs.items():
        ids = [conv(x.post_id) for x in by_kind.get(kind, [])]
        for i in range(0, len(ids), 400):
            part = ids[i:i + 400]
            rows = await db.fetchall(sql.format(ph=",".join("?" * len(part))), [occ, *part])
            out.update(r["pid"] for r in rows)
    return out


# ── kol_coverage_30d ──────────────────────────────────────────────────────────────────────────────────────────
async def compute_kol_coverage_30d(db: "Database", occupant_key: str, as_of: datetime, *,
                                   policy: LabelPolicy | None = None) -> MetricResult:
    t = _t(as_of)
    pol = policy or policy_for()
    lo = t - SP.P6_KOL_WINDOW_S
    ran = await db.fetchval("SELECT 1 FROM collector_runs WHERE job = 'x_kol_poll' AND ok = 1 AND finished_at > ? "
                            "AND finished_at <= ? LIMIT 1", (t - SP.P6_KOL_POLL_RECENCY_S, t))
    if not ran:
        return _failed("kol_poll_stale")
    kols = await db.fetchall("SELECT handle, credibility, team_occupant_keys FROM kol_accounts WHERE platform = 'x' "
                             "AND verified = 1 AND active = 1")
    if len(kols) < SP.P6_KOL_MIN_VERIFIED:
        return _failed("kol_list_short", verified_kols=len(kols))
    credible: set[str] = set()
    for k in kols:
        try:
            team = json.loads(k["team_occupant_keys"] or "[]")
        except ValueError:
            team = []
        if float(k["credibility"]) >= SP.P6_KOL_MIN_CREDIBILITY and occupant_key not in (team or []):
            credible.add(k["handle"])
    labs = [x for x in await _L(db, pol, occupant_key, lo, t, t) if x.item_kind == "social_post"]
    rows: dict[str, dict] = {}
    ids = [x.post_id[2:] for x in labs]
    for i in range(0, len(ids), 400):
        part = ids[i:i + 400]
        for r in await db.fetchall(
                f"SELECT post_id, is_kol, kol_handle, bot_filtered FROM social_posts WHERE platform = 'x' AND "
                f"post_id IN ({','.join('?' * len(part))}) AND observed_at <= ?", [*part, t]):
            rows["x:" + r["post_id"]] = r
    handles: set[str] = set()
    n_posts = 0
    for x in labs:
        r = rows.get(x.post_id)
        if r and r["is_kol"] == 1 and not r["bot_filtered"] and r["kol_handle"] in credible:
            handles.add(r["kol_handle"])
            n_posts += 1
    a = await attributable(db, occupant_key, lo, t, t, kinds=("x",))
    kol_ids = [p for p, v in a.items() if v.get("is_kol") == 1]
    lab_ids = await labelled_at(db, pol, kol_ids, t)
    unlabelled = len([p for p in kol_ids if p not in lab_ids])
    vj = {"kol_handles": sorted(handles), "n_posts": n_posts, "unlabelled_kol_posts": unlabelled}
    if kol_ids and not lab_ids and unlabelled >= 1:
        return _failed("labels_pending", **vj)
    return _ok(float(len(handles)), **vj)


PRODUCERS = {"event_impact_14d": compute_event_impact_14d, "polarity_7d": compute_polarity_7d,
             "attention_7d": compute_attention_7d, "kol_coverage_30d": compute_kol_coverage_30d}


def register(registry: Any = None) -> int:
    """02 §C5: ``register_metric(key, pillar="P6", fn=…, freshness_days=1)`` × 4."""
    try:
        from app.features.registry import register_metric
    except ImportError:  # pragma: no cover
        return 0
    fn = getattr(registry, "register_metric", None) or register_metric
    for key, f in PRODUCERS.items():
        fn(key, pillar="P6", fn=f, freshness_days=1)
    return len(PRODUCERS)


register()

__all__ = ["compute_event_impact_14d", "compute_polarity_7d", "compute_attention_7d", "compute_kol_coverage_30d",
           "register", "attributable", "coverage", "PRODUCERS"]
