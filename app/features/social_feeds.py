"""Appendix A producers (spec 09 §A.4, §4 rows ``news_mentions_7d`` … ``blog_posts_90d``; 02 §C5): none scored in
ts-1.0. ``news_mentions_7d`` is 12's registry row 53 (evidence-weight leg); ``video_uploads_90d``,
``video_mentions_30d`` and ``blog_posts_90d`` are catalogued evidence keys (→ 12's ``EVIDENCE_FNS``). 12 writes the
rows with ``source='rss'``.

Every item read is PIT (``observed_at ≤ as_of``); feed declarations use ``watch_channels``' PIT columns
(``first_seen_at ≤ as_of`` and not retired by then). Feed health (``feed_state``, ``job_state 'feed_ok:{url}'``) and
the YouTube channel id/role in ``watch_channels.meta_json`` are state, not PIT — the accepted deviation class of 09
§3.3 (02 §J2). Absence first (reference rows), failure second (freshness), then the count.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Awaitable, Callable

import app.config as app_config
from app.features.types import MetricResult
from app.util.time import parse_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

DAY = 86400
NEWS_FRESH_S = DAY                    # §4: "≥ 50 % of enabled feeds failed in the last 24 h"
NEWS_FAILED_SHARE = 0.5
TOPN_RECENT_S = 2 * DAY               # an occupant is "top-N" when its own query was fetched this recently
FEED_STALE_S = 7 * DAY                # §4: RSS / blog / playlist failing > 7 d → failed
CHANNEL_RSS_ITEMS = 15                # a channel feed lists the last 15 uploads → the count is a lower bound there
NEWS_KINDS = ("rss", "substack", "medium", "google_news")
BLOG_KINDS = ("medium", "substack", "rss")
ProducerFn = Callable[["Database", str, Any], Awaitable[MetricResult]]


def _ok(v: float, **vj: Any) -> MetricResult:
    return MetricResult(float(v), vj)


def _failed(reason: str, **vj: Any) -> MetricResult:
    return MetricResult(None, {"reason": reason, **vj})


def _absent(reason: str, **vj: Any) -> MetricResult:
    return MetricResult(0.0, vj, absence_reason=reason)


async def _ok_at(db: "Database", feed_url: str) -> int | None:
    from app.collectors.rss import feed_ok_at
    return await feed_ok_at(db, feed_url)


def _pit_sql(alias: str = "") -> str:
    a = f"{alias}." if alias else ""
    return f"{a}first_seen_at <= ? AND ({a}retired_at IS NULL OR {a}retired_at > ?)"


# ── news ─────────────────────────────────────────────────────────────────────────────────────────────────────
async def news_health(db: "Database", occupant_key: str, t: int) -> dict[str, Any]:
    """Share of enabled shared feeds (everything but per-subnet Google queries) without a successful fetch in 24 h,
    and whether ``occupant_key`` had its own Google News query recently (top-N) and whether that one failed."""
    ph = ",".join("?" * len(NEWS_KINDS))
    rows = await db.fetchall(
        "SELECT f.feed_url, f.kind, f.occupant_key, f.last_fetched_at, (SELECT MAX(CAST(json_extract(j.value_json, "
        "'$.at') AS INTEGER)) FROM job_state j WHERE j.key = 'feed_ok:' || f.feed_url) AS ok_at FROM feed_state f "
        f"WHERE f.enabled = 1 AND f.kind IN ({ph})", NEWS_KINDS)
    shared = [r for r in rows if not (r["kind"] == "google_news" and r["occupant_key"] is not None)]
    own = [r for r in rows if r["kind"] == "google_news" and r["occupant_key"] == occupant_key
           and (r["last_fetched_at"] or 0) > t - TOPN_RECENT_S]
    fresh = lambda r: r["ok_at"] is not None and int(r["ok_at"]) > t - NEWS_FRESH_S  # noqa: E731
    n_failed = sum(1 for r in shared if not fresh(r))
    return {"feeds": len(shared), "feeds_failed": n_failed,
            "failed_share": (n_failed / len(shared)) if shared else 1.0,
            "searched": bool(own), "own_query_ok": any(fresh(r) for r in own)}


async def compute_news_mentions_7d(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    """Distinct ``(title_norm, publisher_domain)`` among ``news_items`` attributed to the occupant (any kind) with
    ``event_at ∈ (as_of − 7 d, as_of]`` — a story seen via Google (title hash) and a publisher feed counts once."""
    from app.nlp.textnorm import text_norm
    s = app_config.settings
    t = parse_ts(as_of)
    if not s.RSS_ENABLED:
        return _failed("rss_disabled")
    h = await news_health(db, occupant_key, t)
    if not h["feeds"]:
        return _failed("no_feed_state", **h)
    if h["failed_share"] >= NEWS_FAILED_SHARE and not (h["searched"] and h["own_query_ok"]):
        return _failed("feeds_failing", **h)
    rows = await db.fetchall(
        "SELECT n.url_hash, n.title, n.publisher_domain FROM news_items n WHERE n.event_at > ? AND n.event_at <= ? "
        "AND n.observed_at <= ? AND EXISTS (SELECT 1 FROM json_each(n.occupant_keys_json) j "
        "WHERE json_extract(j.value, '$.occupant_key') = ?)", (t - 7 * DAY, t, t, occupant_key))
    pairs = {(text_norm(r["title"]), (r["publisher_domain"] or "").lower()) for r in rows}
    return _ok(len(pairs), items=len(rows), searched=h["searched"], feeds=h["feeds"],
               feeds_failed=h["feeds_failed"])


# ── YouTube ──────────────────────────────────────────────────────────────────────────────────────────────────
async def own_channels(db: "Database", occupant_key: str, t: int) -> dict[str, Any]:
    """The occupant's own channel ids at ``t`` (declared ``youtube`` rows; ``youtube_video`` rows whose resolved
    channel passed A.2's own-channel test), plus the rows still unresolved and the roles seen."""
    import json

    from app.collectors.youtube import channel_id_in
    rows = await db.fetchall(
        f"SELECT platform, handle, meta_json FROM watch_channels WHERE occupant_key = ? AND platform IN "
        f"('youtube', 'youtube_video') AND {_pit_sql()}", (occupant_key, t, t))
    own: set[str] = set()
    unresolved = 0
    roles: dict[str, int] = {}
    for r in rows:
        try:
            m = json.loads(r["meta_json"]) if r["meta_json"] else {}
        except ValueError:
            m = {}
        if r["platform"] == "youtube":
            cid = m.get("channel_id") or channel_id_in(r["handle"])
            if cid:
                own.add(cid)
            else:
                unresolved += 1
            continue
        cid, role = m.get("channel_id"), m.get("channel_role")
        if not cid or not role:
            unresolved += 1
            continue
        roles[role] = roles.get(role, 0) + 1
        if role == "own":
            own.add(cid)
    return {"rows": len(rows), "own": sorted(own), "unresolved": unresolved, "roles": roles}


def _yt_disabled() -> str | None:
    s = app_config.settings
    if not s.RSS_ENABLED:
        return "rss_disabled"
    if not s.YOUTUBE_ENABLED:
        return "youtube_disabled"
    return None


async def compute_video_uploads_90d(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    from app.collectors.youtube import CHANNEL_FEED
    t = parse_ts(as_of)
    oc = await own_channels(db, occupant_key, t)
    if not oc["rows"]:
        return _absent("no_youtube_channel")
    if not oc["own"]:
        if oc["unresolved"]:
            return _failed("channel_unresolved", unresolved=oc["unresolved"], roles=oc["roles"])
        return _absent("no_youtube_channel", roles=oc["roles"])        # only ecosystem / shared channels
    off = _yt_disabled()
    if off:
        return _failed(off)
    stale = [c for c in oc["own"] if ((await _ok_at(db, CHANNEL_FEED.format(c))) or 0) <= t - FEED_STALE_S]
    if stale:
        return _failed("rss_stale", channels=oc["own"], stale=stale)
    ph = ",".join("?" * len(oc["own"]))
    n = int(await db.fetchval(
        f"SELECT COUNT(*) FROM video_items WHERE channel_id IN ({ph}) AND event_at > ? AND event_at <= ? "
        f"AND observed_at <= ?", (*oc["own"], t - 90 * DAY, t, t)) or 0)
    return _ok(n, channels=oc["own"], lower_bound=n >= CHANNEL_RSS_ITEMS)


async def compute_video_mentions_30d(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    from app.collectors.youtube import PLAYLIST_FEED
    t = parse_ts(as_of)
    off = _yt_disabled()
    if off:
        return _failed(off)
    pid = (app_config.settings.YOUTUBE_NOVELTY_PLAYLIST_ID or "").strip()
    if not pid:
        return _failed("no_playlist")
    ok_at = await _ok_at(db, PLAYLIST_FEED.format(pid))
    if ok_at is None or ok_at <= t - FEED_STALE_S:
        return _failed("playlist_feed_stale", playlist_id=pid, ok_at=ok_at)
    own = (await own_channels(db, occupant_key, t))["own"]
    ph = ",".join("?" * len(own))
    not_own = f"AND v.channel_id NOT IN ({ph}) " if own else ""
    n = int(await db.fetchval(
        "SELECT COUNT(DISTINCT v.video_id) FROM video_items v WHERE v.event_at > ? AND v.event_at <= ? "
        f"AND v.observed_at <= ? {not_own}AND EXISTS (SELECT 1 FROM json_each(v.occupant_keys_json) j "
        "WHERE json_extract(j.value, '$.occupant_key') = ?)", (t - 30 * DAY, t, t, *own, occupant_key)) or 0)
    return _ok(n, playlist_id=pid)


# ── blogs ────────────────────────────────────────────────────────────────────────────────────────────────────
async def compute_blog_posts_90d(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    t = parse_ts(as_of)
    ph = ",".join("?" * len(BLOG_KINDS))
    rows = await db.fetchall(
        f"SELECT DISTINCT COALESCE(url, handle) AS feed_url FROM watch_channels WHERE occupant_key = ? "
        f"AND platform IN ({ph}) AND {_pit_sql()}", (occupant_key, *BLOG_KINDS, t, t))
    feeds = [r["feed_url"] for r in rows]
    if not feeds:
        return _absent("no_blog_feed")
    if not app_config.settings.RSS_ENABLED:
        return _failed("rss_disabled")
    fresh = [f for f in feeds if ((await _ok_at(db, f)) or 0) > t - FEED_STALE_S]
    if not fresh:
        return _failed("feed_stale", feeds=len(feeds))
    fph = ",".join("?" * len(feeds))
    n = int(await db.fetchval(
        f"SELECT COUNT(*) FROM news_items WHERE feed_url IN ({fph}) AND event_at > ? AND event_at <= ? "
        f"AND observed_at <= ?", (*feeds, t - 90 * DAY, t, t)) or 0)
    return _ok(n, feeds=len(feeds), stale_feeds=len(feeds) - len(fresh))


PRODUCERS: dict[str, ProducerFn] = {"news_mentions_7d": compute_news_mentions_7d}
EVIDENCE: dict[str, ProducerFn] = {"video_uploads_90d": compute_video_uploads_90d,
                                   "video_mentions_30d": compute_video_mentions_30d,
                                   "blog_posts_90d": compute_blog_posts_90d}


def register_producers() -> int:
    """C5: the registry leg row (53) and the three catalogued evidence keys (→ ``EVIDENCE_FNS``)."""
    try:
        from app.features.registry import register_metric  # type: ignore[import-not-found]
    except ImportError:
        return 0
    register_metric("news_mentions_7d", fn=compute_news_mentions_7d, freshness_days=3, weight=0.0)
    for key, fn in EVIDENCE.items():
        register_metric(key, fn=fn, weight=0.0)
    return 1 + len(EVIDENCE)


register_producers()
