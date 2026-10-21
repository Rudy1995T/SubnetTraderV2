"""Classification queue (spec 10 §2.2 "Queue selection"/"Packing", §2.4 user message, §2.5 privacy gate).

Item ids (02 §B11, = ``post_labels.post_id``): ``x:{id}``, ``discord:{source_message_id}``, ``telegram:{handle}/{n}``,
``news_item:{url_hash}``, ``video_item:{video_id}``. The privacy gate is enforced in SQL: announcements only with text
and (Telegram or own-server crossposts); X only ``bot_filtered = 0``. Retry state comes from ``llm_batch_items`` of the
current bulk version: an ``errored`` row (invalid_request / parse failure) sends the item back at 10 per request once,
then to rules; ``requeued`` rows (expired / canceled / other errors) retry at full size; three failed attempts → rules.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Iterable

from app.nlp.textnorm import text_hash as _text_hash

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

DAY = 86400
SMALL_REQUEST = 10                   # §2.2: a failed request's items are re-sent once at 10 per request
MAX_ATTEMPTS = 3
OPEN_BATCH_STATUSES = ("building", "submitted", "ended")
_WS = re.compile(r"\s+")
NEWS_ID_SQL = "('news_item:' || news_items.url_hash)"
VIDEO_ID_SQL = "('video_item:' || video_items.video_id)"
_ITEM_TAG = re.compile(r"<(/?)(items?)\b", re.I)


@dataclass
class QueueItem:
    post_id: str
    item_kind: str                   # social_post | announcement | news_item | video_item
    item_id: str                     # social_posts.item_id | announcements.msg_id | news url_hash | video_id
    occupant_key: str | None         # source-bound occupant (team account, own channel, subnet YouTube channel)
    platform: str                    # x | discord | telegram | news | video
    author_kind: str                 # team | kol | other
    author_key: str | None           # X author_id | announcement source_channel_id | video channel_id
    kol_handle: str | None
    text: str | None
    text_hash: str
    event_at: int
    text_purge_after: int | None
    like_count: int | None = None
    repost_count: int | None = None
    reply_count: int | None = None
    quote_count: int | None = None
    url: str | None = None
    observed_at: int = 0
    views: int | None = None
    credibility: float | None = None
    n_err: int = 0                   # prior 'errored' attempts (current bulk version)
    n_fail: int = 0                  # prior failed attempts ('errored' + 'requeued')
    mention_keys: list[str] = field(default_factory=list)   # news/video occupant_keys_json (label-free attribution)

    @property
    def engagement(self) -> int:
        return (self.like_count or 0) + 2 * (self.repost_count or 0) + (self.reply_count or 0) \
            + (self.quote_count or 0)

    @property
    def source_netuid(self) -> int | None:
        if not self.occupant_key:
            return None
        try:
            return int(self.occupant_key.split(":", 1)[0])
        except ValueError:
            return None

    @property
    def pending(self) -> bool:
        return self.text_purge_after is not None

    @property
    def attempt(self) -> int:
        return self.n_fail + 1

    @property
    def per_request_cap(self) -> int | None:
        return SMALL_REQUEST if self.n_err >= 1 else None


# ── text assembly ────────────────────────────────────────────────────────────────────────────────────────────
def _collapse(s: str | None) -> str:
    return _WS.sub(" ", s or "").strip()


def assemble_text(row: dict, item_kind: str, max_chars: int) -> str | None:
    """X/announcement text; news title + summary; video title + description — whitespace-collapsed (one newline
    between title and body), URLs kept, truncated to ``max_chars`` with ``…``. None when nothing is classifiable."""
    if item_kind in ("social_post", "announcement"):
        parts = [_collapse(row.get("text"))]
    elif item_kind == "news_item":
        parts = [_collapse(row.get("title")), _collapse(row.get("summary"))]
    elif item_kind == "video_item":
        parts = [_collapse(row.get("title")), _collapse(row.get("description"))]
    else:
        raise ValueError(f"unknown item_kind {item_kind!r}")
    text = "\n".join(p for p in parts if p)
    if not text:
        return None
    if max_chars > 1 and len(text) > max_chars:
        text = text[:max_chars - 1].rstrip() + "…"
    return text


# ── row → QueueItem ──────────────────────────────────────────────────────────────────────────────────────────
def _keys_json(v: str | None) -> list[str]:
    try:
        arr = json.loads(v or "[]")
    except ValueError:
        return []
    out = []
    for x in arr if isinstance(arr, list) else []:
        k = x.get("occupant_key") if isinstance(x, dict) else x
        if isinstance(k, str):
            out.append(k)
    return out


def _from_x(r: dict, max_chars: int) -> QueueItem:
    return QueueItem(post_id=r["item_id"], item_kind="social_post", item_id=r["item_id"],
                     occupant_key=r["occupant_key"], platform="x", author_kind=r["author_kind"] or "other",
                     author_key=r["author_id"], kol_handle=r["kol_handle"],
                     text=assemble_text(r, "social_post", max_chars), text_hash=r["text_hash"],
                     event_at=int(r["event_at"]), text_purge_after=None, like_count=r["like_count"],
                     repost_count=r["repost_count"], reply_count=r["reply_count"], quote_count=r["quote_count"],
                     url=r["url"], observed_at=int(r["observed_at"]), credibility=r["author_credibility"])


def _from_ann(r: dict, max_chars: int) -> QueueItem:
    kind = "team" if r["platform"] == "telegram" or r["author_is_team"] == 1 else "other"
    return QueueItem(post_id=r["msg_id"], item_kind="announcement", item_id=r["msg_id"],
                     occupant_key=r["occupant_key"], platform=r["platform"], author_kind=kind,
                     author_key=r["source_channel_id"], kol_handle=None,
                     text=assemble_text(r, "announcement", max_chars), text_hash=r["text_hash"],
                     event_at=int(r["event_at"]), text_purge_after=r["text_purge_after"], url=r["url"],
                     observed_at=int(r["observed_at"]))


def _from_news(r: dict, max_chars: int) -> QueueItem:
    return QueueItem(post_id=f"news_item:{r['url_hash']}", item_kind="news_item", item_id=r["url_hash"],
                     occupant_key=None, platform="news", author_kind="other", author_key=r.get("publisher_domain"),
                     kol_handle=None, text=assemble_text(r, "news_item", max_chars), text_hash=r["text_hash"],
                     event_at=int(r["event_at"]), text_purge_after=None, url=r["url"],
                     observed_at=int(r["observed_at"]), mention_keys=_keys_json(r["occupant_keys_json"]))


def _from_video(r: dict, max_chars: int) -> QueueItem:
    text = assemble_text(r, "video_item", max_chars)
    keys = _keys_json(r["occupant_keys_json"])
    if r["occupant_key"] and r["occupant_key"] not in keys:
        keys.insert(0, r["occupant_key"])
    return QueueItem(post_id=f"video_item:{r['video_id']}", item_kind="video_item", item_id=r["video_id"],
                     occupant_key=r["occupant_key"], platform="video",
                     author_kind="team" if r["occupant_key"] else "other", author_key=r["channel_id"],
                     kol_handle=None, text=text,
                     text_hash=_text_hash((r["title"] or "") + "\n" + (r["description"] or "")),
                     event_at=int(r["event_at"]), text_purge_after=None, url=r["url"],
                     observed_at=int(r["observed_at"]), views=r["views"], mention_keys=keys)


_X_COLS = ("item_id, post_id, occupant_key, author_kind, author_id, kol_handle, text, text_hash, event_at, "
           "observed_at, like_count, repost_count, reply_count, quote_count, url, author_credibility")
_ANN_COLS = ("msg_id, platform, occupant_key, netuid, source_channel_id, author_is_team, text, text_purge_after, "
             "text_hash, url, event_at, observed_at")
_NEWS_COLS = "url_hash, title, summary, url, publisher_domain, occupant_keys_json, text_hash, event_at, observed_at"
_VIDEO_COLS = ("video_id, channel_id, occupant_key, occupant_keys_json, title, description, url, views, event_at, "
               "observed_at")


def _unlabelled(id_expr: str, versions: tuple[str, ...]) -> str:
    ph = ",".join("?" * len(versions))
    return f"NOT EXISTS (SELECT 1 FROM post_labels l WHERE l.post_id = {id_expr} AND l.classifier_version IN ({ph}))"


async def _candidates(db: "Database", *, exclude_versions: tuple[str, ...], min_event_at: int, max_chars: int,
                      observed_before: int | None = None) -> list[QueueItem]:
    """Every privacy-passing, text-bearing item with ``event_at ≥ min_event_at`` and no label of
    ``exclude_versions`` (optionally only items observed before ``observed_before``)."""
    ex = tuple(exclude_versions)
    obs = " AND observed_at < ?" if observed_before is not None else ""
    oarg = [int(observed_before)] if observed_before is not None else []
    out: list[QueueItem] = []
    rows = await db.fetchall(
        f"SELECT {_X_COLS} FROM social_posts WHERE platform = 'x' AND text IS NOT NULL AND bot_filtered = 0 "
        f"AND event_at >= ?{obs} AND {_unlabelled('social_posts.item_id', ex)}", [int(min_event_at), *oarg, *ex])
    out += [_from_x(r, max_chars) for r in rows]
    rows = await db.fetchall(
        f"SELECT {_ANN_COLS} FROM announcements WHERE text IS NOT NULL AND (platform = 'telegram' OR "
        f"via_own_server = 1) AND deleted_at IS NULL AND event_at >= ?{obs} "
        f"AND {_unlabelled('announcements.msg_id', ex)}", [int(min_event_at), *oarg, *ex])
    out += [_from_ann(r, max_chars) for r in rows]
    rows = await db.fetchall(
        f"SELECT {_NEWS_COLS} FROM news_items WHERE event_at >= ?{obs} "
        f"AND {_unlabelled(NEWS_ID_SQL, ex)}",
        [int(min_event_at), *oarg, *ex])
    out += [_from_news(r, max_chars) for r in rows]
    rows = await db.fetchall(
        f"SELECT {_VIDEO_COLS} FROM video_items WHERE event_at >= ?{obs} "
        f"AND {_unlabelled(VIDEO_ID_SQL, ex)}",
        [int(min_event_at), *oarg, *ex])
    out += [_from_video(r, max_chars) for r in rows]
    return [q for q in out if q.text]


async def attempt_stats(db: "Database", bulk_version: str) -> tuple[dict[str, tuple[int, int]], set[str]]:
    """({post_id: (n_err, n_fail)}, post_ids pending in an open batch) for ``bulk_version``."""
    rows = await db.fetchall(
        "SELECT i.post_id, SUM(i.status = 'errored') AS n_err, SUM(i.status IN ('errored','requeued')) AS n_fail, "
        f"SUM(i.status = 'pending' AND b.status IN ({','.join('?' * len(OPEN_BATCH_STATUSES))})) AS n_open "
        "FROM llm_batch_items i JOIN llm_batches b ON b.batch_key = i.batch_key WHERE b.classifier_version = ? "
        "GROUP BY i.post_id", [*OPEN_BATCH_STATUSES, bulk_version])
    stats = {r["post_id"]: (int(r["n_err"] or 0), int(r["n_fail"] or 0)) for r in rows}
    open_ids = {r["post_id"] for r in rows if (r["n_open"] or 0) > 0}
    return stats, open_ids


def llm_exhausted(n_err: int, n_fail: int) -> bool:
    return n_err >= 2 or n_fail >= MAX_ATTEMPTS


def priority_key(q: QueueItem) -> tuple:
    """Pending text first (earliest purge), then team + KOL, then engagement desc, then event_at desc."""
    return (0 if q.pending else 1, q.text_purge_after or 0, 0 if q.author_kind in ("team", "kol") else 1,
            -q.engagement, -q.event_at, q.post_id)


async def select_unlabelled(db: "Database", *, bulk_version: str, now: int, lookback_days: int, limit: int,
                            max_chars: int | None = None) -> list[QueueItem]:
    """§2.2 (a)–(f): items for the next bulk batch, in priority order, at most ``limit``."""
    import app.config as app_config
    mc = int(max_chars if max_chars is not None else app_config.settings.LLM_MAX_ITEM_CHARS)
    cands = await _candidates(db, exclude_versions=(bulk_version,), min_event_at=int(now) - int(lookback_days) * DAY,
                              max_chars=mc)
    stats, open_ids = await attempt_stats(db, bulk_version)
    out = []
    for q in cands:
        if q.post_id in open_ids:
            continue
        q.n_err, q.n_fail = stats.get(q.post_id, (0, 0))
        if llm_exhausted(q.n_err, q.n_fail):
            continue
        out.append(q)
    out.sort(key=priority_key)
    return out[:max(0, int(limit))]


def dedup(items: list[QueueItem]) -> tuple[list[QueueItem], dict[str, list[QueueItem]]]:
    """Identical ``text_hash`` inside one cycle is sent once; returns (primaries, {primary post_id: duplicates})."""
    seen: dict[str, QueueItem] = {}
    prim: list[QueueItem] = []
    dups: dict[str, list[QueueItem]] = {}
    for q in items:
        p = seen.get(q.text_hash)
        if p is None:
            seen[q.text_hash] = q
            prim.append(q)
        else:
            dups.setdefault(p.post_id, []).append(q)
    return prim, dups


def pack_requests(items: list[QueueItem], per_request: int) -> list[list[QueueItem]]:
    """Chunks of ``per_request`` in priority order (items already sent once at full size go in chunks of 10);
    inside a request items are ordered by ``post_id`` (prompt ids 1…n map back through that order)."""
    n = max(1, int(per_request))
    small = [q for q in items if q.per_request_cap is not None]
    normal = [q for q in items if q.per_request_cap is None]
    out: list[list[QueueItem]] = []
    for group, size in ((normal, n), (small, min(n, SMALL_REQUEST))):
        for i in range(0, len(group), size):
            out.append(sorted(group[i:i + size], key=lambda q: q.post_id))
    return out


def _escape(text: str) -> str:
    return _ITEM_TAG.sub(lambda m: "&lt;" + m.group(1) + m.group(2), text)


def _attr(v: Any) -> str:
    return str("" if v is None else v).replace('"', "'").replace("<", "").replace(">", "")


def _engagement_attr(q: QueueItem) -> str:
    if q.platform == "x":
        return f"likes={q.like_count or 0} reposts={q.repost_count or 0} replies={q.reply_count or 0}"
    if q.platform == "video" and q.views is not None:
        return f"views={q.views}"
    return ""


def render_item(q: QueueItem, pos: int) -> str:
    date = datetime.fromtimestamp(int(q.event_at), timezone.utc).strftime("%Y-%m-%d")
    sn = q.source_netuid
    head = (f'<item id="{pos}" platform="{_attr(q.platform)}" author_kind="{_attr(q.author_kind)}" '
            f'source_netuid="{"" if sn is None else sn}" date="{date}" engagement="{_attr(_engagement_attr(q))}">')
    return f"{head}\n{_escape(q.text or '')}\n</item>"


def render_items_block(items: list[QueueItem]) -> str:
    body = "\n".join(render_item(q, i + 1) for i, q in enumerate(items))
    return f'<items n="{len(items)}">\n{body}\n</items>\nClassify every item. Output JSON only.'


def render_single(q: QueueItem) -> str:
    return render_items_block([q])


# ── reload by id (apply time / escalation: text may have been purged since) ───────────────────────────────────
async def load_items(db: "Database", post_ids: Iterable[str], *, max_chars: int | None = None
                     ) -> dict[str, QueueItem]:
    import app.config as app_config
    mc = int(max_chars if max_chars is not None else app_config.settings.LLM_MAX_ITEM_CHARS)
    groups: dict[str, list[str]] = {"x": [], "ann": [], "news": [], "video": []}
    for p in dict.fromkeys(post_ids):
        if p.startswith("x:"):
            groups["x"].append(p[2:])
        elif p.startswith(("discord:", "telegram:")):
            groups["ann"].append(p)
        elif p.startswith("news_item:"):
            groups["news"].append(p[len("news_item:"):])
        elif p.startswith("video_item:"):
            groups["video"].append(p[len("video_item:"):])
    out: dict[str, QueueItem] = {}

    async def q(sql: str, ids: list[str], conv) -> None:
        for i in range(0, len(ids), 400):
            part = ids[i:i + 400]
            for r in await db.fetchall(sql.format(ph=",".join("?" * len(part))), part):
                item = conv(r, mc)
                out[item.post_id] = item
    await q(f"SELECT {_X_COLS} FROM social_posts WHERE platform = 'x' AND post_id IN ({{ph}})", groups["x"], _from_x)
    await q(f"SELECT {_ANN_COLS} FROM announcements WHERE msg_id IN ({{ph}})", groups["ann"], _from_ann)
    await q(f"SELECT {_NEWS_COLS} FROM news_items WHERE url_hash IN ({{ph}})", groups["news"], _from_news)
    await q(f"SELECT {_VIDEO_COLS} FROM video_items WHERE video_id IN ({{ph}})", groups["video"], _from_video)
    return out


def native_mention_id(post_id: str) -> tuple[str, str] | None:
    """(platform, native post id) for ``social_post_mentions`` (X and announcements only)."""
    for plat in ("x", "discord", "telegram"):
        if post_id.startswith(plat + ":"):
            return plat, post_id[len(plat) + 1:]
    return None


__all__ = ["QueueItem", "select_unlabelled", "assemble_text", "pack_requests", "render_items_block", "render_single",
           "dedup", "load_items", "attempt_stats", "llm_exhausted", "priority_key", "native_mention_id",
           "SMALL_REQUEST", "MAX_ATTEMPTS"]
