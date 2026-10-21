"""RSS / Atom feeds (spec 09 Appendix A.1–A.3, A.6; §2.5 row ``rss_feeds_poll``; §3.5 ``feed_dead``).

``FeedPoller`` makes conditional GETs (ETag / Last-Modified) through ``Http.request(provider, …, ok_statuses=(200,
304))`` — ``rss`` for publisher/ecosystem/Medium/Substack/Google News feeds, ``youtube`` for YouTube feeds — parses
with ``feedparser`` and keeps ``feed_state`` (03 §3.2). Redirects are followed here hop by hop (≤ 5) so the final URL
is known and a permanent one is remembered in ``feed_state.effective_url``. ``news.google.com`` requests are spaced by
``GOOGLE_NEWS_MIN_INTERVAL_SEC`` on top of the ``rss`` limiter (the one sleep this layer owns, §6.1). Items land in
``news_items`` (append-only, ``INSERT OR IGNORE`` on ``url_hash``); attribution is ``app.nlp.match``'s rules.

The feed registry is ``seed/feeds.csv`` → ``watch_channels(source='seed_feeds')`` (``INSERT OR IGNORE``; operator
edits survive a re-seed; ``enabled=0`` rows are written retired) plus ``RSS_EXTRA_FEEDS``.
"""
from __future__ import annotations

import argparse
import asyncio
import calendar
import csv
import hashlib
import html as htmlmod
import json
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable
from urllib.parse import urljoin, urlsplit

import app.config as app_config
from app.http import ProviderError
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.http import Http
    from app.nlp.match import OccupantIndex
    from app.storage.db import Database

JOB = "rss_feeds_poll"
DAY = 86400
RSS_KINDS = ("rss", "substack", "medium")
SEED_KINDS = frozenset({"rss", "substack", "medium", "google_news", "youtube"})
NEWS_SOURCES = frozenset({"google_news", "rss", "substack", "medium"})
GOOGLE_NEWS_HOST = "news.google.com"
SUMMARY_MAX = 2000
MAX_REDIRECTS = 5
_REDIRECTS = (301, 302, 303, 307, 308)
_PERMANENT = (301, 308)
_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")


@dataclass(frozen=True, slots=True)
class FeedRow:
    url: str                              # configured feed URL = feed_state.feed_url
    kind: str                             # watch_channels.platform (rss|substack|medium|google_news|youtube)
    occupant_key: str | None = None       # NULL = ecosystem-wide
    netuid: int | None = None
    label: str | None = None
    filtered: bool = False                # publisher feed → keyword filter (A.2)
    provider: str = "rss"                 # 02 §G1 limiter id
    query_occupant: str | None = None     # per-subnet Google News query (adds a ``query`` attribution)
    watch_id: int | None = None
    playlist_id: str | None = None        # YouTube playlist feed
    channel_id: str | None = None         # YouTube channel feed


@dataclass(slots=True)
class FeedFetch:
    status: int | None                    # final HTTP status; None = transport failure
    entries: list = field(default_factory=list)
    etag: str | None = None
    last_modified: str | None = None
    bozo: bool = False
    effective_url: str | None = None      # final URL after redirects
    permanent_url: str | None = None      # set when a 301/308 moved the feed
    error: str | None = None
    feed_meta: dict = field(default_factory=dict)
    skipped: bool = False

    @property
    def ok(self) -> bool:
        return self.error is None and self.status in (200, 304)


def strip_html(s: str | None) -> str:
    return _WS.sub(" ", htmlmod.unescape(_TAG.sub(" ", s or ""))).strip()


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def entry_time(entry: Any) -> int | None:
    for k in ("published_parsed", "updated_parsed"):
        st = dict.get(entry, k) if isinstance(entry, dict) else getattr(entry, k, None)   # no feedparser key fallback
        if st:
            try:
                return int(calendar.timegm(st))
            except (TypeError, ValueError, OverflowError):
                continue
    return None


def keyword_re(keywords: str | None) -> re.Pattern | None:
    kws = [k.strip() for k in (keywords or "").split(",") if k.strip()]
    if not kws:
        return None
    return re.compile("|".join(r"(?<!\w)" + re.escape(k) + r"(?!\w)" for k in kws), re.I)


# ── registry ─────────────────────────────────────────────────────────────────────────────────────────────────
async def load_seed_feeds(db: "Database", path: str | Path | None = None, *, now: int | None = None) -> dict:
    """``seed/feeds.csv`` (``kind,label,url,occupant_netuid,enabled,filtered,notes``) → ``watch_channels``."""
    from app.nlp.match import url_canonical
    s = app_config.settings
    p = Path(path) if path is not None else s.path(s.RSS_FEEDS_SEED_PATH)
    t = int(now if now is not None else now_ts())
    rep = {"rows": 0, "inserted": 0, "skipped": 0}
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        logger.warning("feeds_seed_missing", data={"path": str(p), "error": str(exc)})
        return rep
    for r in csv.DictReader(line for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")):
        rep["rows"] += 1
        kind = (r.get("kind") or "").strip()
        url = (r.get("url") or "").strip()
        if kind not in SEED_KINDS or not url.startswith(("http://", "https://")):
            rep["skipped"] += 1
            logger.warning("feeds_seed_bad_row", data={"kind": kind, "url": url})
            continue
        okey = netuid = None
        nu = (r.get("occupant_netuid") or "").strip()
        if nu:
            row = await db.fetchone("SELECT occupant_key, netuid FROM subnets WHERE netuid = ? AND status <> "
                                    "'deregistered'", (int(nu),))
            if row is None:
                rep["skipped"] += 1
                continue
            okey, netuid = row["occupant_key"], int(row["netuid"])
        enabled = (r.get("enabled") or "1").strip() not in ("0", "false", "False", "no")
        filtered = (r.get("filtered") or "0").strip() in ("1", "true", "True", "yes")
        meta: dict[str, Any] = {"seed": True}
        if (r.get("notes") or "").strip():
            meta["notes"] = r["notes"].strip()
        m = re.search(r"[?&]playlist_id=([\w-]+)", url)
        if m:
            meta["playlist_id"] = m.group(1)
        res = await db.execute(
            "INSERT OR IGNORE INTO watch_channels (occupant_key, netuid, platform, handle, url, label, source, "
            "priority, filtered, first_seen_at, last_seen_at, retired_at, meta_json) "
            "VALUES (?,?,?,?,?,?,'seed_feeds',50,?,?,?,?,?)",
            (okey, netuid, kind, url_canonical(url), url, (r.get("label") or "").strip() or None, int(filtered), t, t,
             None if enabled else t, json.dumps(meta, separators=(",", ":"))))
        rep["inserted"] += max(0, res.rowcount)
    return rep


def _row_to_feed(r: dict, provider: str = "rss") -> FeedRow:
    try:
        meta = json.loads(r["meta_json"]) if r.get("meta_json") else {}
    except ValueError:
        meta = {}
    return FeedRow(url=r["url"] or r["handle"], kind=r["platform"], occupant_key=r["occupant_key"],
                   netuid=r["netuid"], label=r["label"], filtered=bool(r["filtered"]), provider=provider,
                   watch_id=r["id"], playlist_id=meta.get("playlist_id"), channel_id=meta.get("channel_id"))


async def feed_rows(db: "Database", kinds: tuple[str, ...] | set[str], *, s=None) -> list[FeedRow]:
    """Active ``watch_channels`` feeds of ``kinds`` (+ ``RSS_EXTRA_FEEDS`` for ``rss``), deduplicated by URL."""
    s = s or app_config.settings
    ph = ",".join("?" * len(kinds))
    rows = await db.fetchall(f"SELECT * FROM watch_channels WHERE platform IN ({ph}) AND retired_at IS NULL "
                             f"ORDER BY occupant_key IS NOT NULL, netuid, id", tuple(kinds))
    out: dict[str, FeedRow] = {}
    for r in rows:
        f = _row_to_feed(r)
        out.setdefault(f.url, f)
    if "rss" in kinds:
        for u in (x.strip() for x in (s.RSS_EXTRA_FEEDS or "").split(",")):
            if u.startswith(("http://", "https://")):
                out.setdefault(u, FeedRow(url=u, kind="rss", label="RSS_EXTRA_FEEDS"))
    return list(out.values())


# ── poller ───────────────────────────────────────────────────────────────────────────────────────────────────
StoreFn = Callable[[FeedRow, FeedFetch, int, str], Awaitable[int]]


class FeedPoller:
    def __init__(self, db: "Database", http: "Http", *, s=None, index: "OccupantIndex | None" = None,
                 sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.db = db
        self.http = http
        self.s = s or app_config.settings
        self.index = index
        self._sleep = sleep
        self._clock = clock
        self._last_google: float | None = None
        self._kw = keyword_re(self.s.NEWS_KEYWORDS)
        self.decode_budget = 60                    # §6.5: Google URL decoding attempts per run (when enabled)

    async def _space(self, url: str) -> None:
        """``GOOGLE_NEWS_MIN_INTERVAL_SEC`` between ``news.google.com`` requests (per host, inside the collector)."""
        if (urlsplit(url).hostname or "").lower() != GOOGLE_NEWS_HOST:
            return
        gap = float(self.s.GOOGLE_NEWS_MIN_INTERVAL_SEC)
        if self._last_google is not None:
            wait = gap - (self._clock() - self._last_google)
            if wait > 0:
                await self._sleep(wait)
        self._last_google = self._clock()

    async def _get(self, provider: str, url: str, headers: dict[str, str] | None = None) -> Any:
        await self._space(url)
        return await self.http.request(provider, "GET", url, headers=headers or None,
                                       timeout=float(self.s.RSS_HTTP_TIMEOUT_SEC), ok_statuses=(200, 304))

    async def state(self, feed_url: str) -> dict | None:
        return await self.db.fetchone("SELECT * FROM feed_state WHERE feed_url = ?", (feed_url,), writer=True)

    async def fetch(self, feed: FeedRow, state: dict | None = None) -> FeedFetch:
        """One conditional fetch (A.2): 304 → no entries; 200 → feedparser; ``bozo`` with 0 entries → failure."""
        import feedparser
        st = state if state is not None else await self.state(feed.url)
        url = (st or {}).get("effective_url") or feed.url
        headers: dict[str, str] = {}
        if st and st.get("etag"):
            headers["If-None-Match"] = st["etag"]
        if st and st.get("last_modified"):
            headers["If-Modified-Since"] = st["last_modified"]
        hops, all_permanent = 0, True
        try:
            for _ in range(MAX_REDIRECTS + 1):
                r = await self._get(feed.provider, url, headers)
                loc = r.headers.get("location")
                if r.status_code in _REDIRECTS and loc:
                    hops += 1
                    all_permanent = all_permanent and r.status_code in _PERMANENT
                    url = urljoin(url, loc)
                    continue
                break
            else:
                return FeedFetch(None, effective_url=url, error="too_many_redirects")
        except ProviderError as exc:
            return FeedFetch(exc.status, effective_url=url, error=f"provider_error: {str(exc)[:160]}")
        perm = url if hops and all_permanent and url != feed.url else None      # only an all-permanent chain moves it
        if r.status_code == 304:
            return FeedFetch(304, etag=(st or {}).get("etag"), last_modified=(st or {}).get("last_modified"),
                             effective_url=url, permanent_url=perm)
        if r.status_code != 200:
            return FeedFetch(r.status_code, effective_url=url, permanent_url=perm, error=f"http_{r.status_code}")
        parsed = feedparser.parse(r.content)
        entries = list(parsed.entries or [])
        bozo = bool(parsed.get("bozo"))
        if bozo and not entries:
            exc = parsed.get("bozo_exception")
            return FeedFetch(200, bozo=True, effective_url=url, permanent_url=perm,
                             error=f"bozo: {type(exc).__name__ if exc else 'unparseable'}")
        if bozo:
            logger.warning("feed_bozo", data={"feed": feed.url, "entries": len(entries)})
        return FeedFetch(200, entries, r.headers.get("etag"), r.headers.get("last-modified"), bozo, url, perm,
                         feed_meta=dict(parsed.get("feed") or {}))

    async def record(self, feed: FeedRow, res: FeedFetch, *, new_items: int, now: int, job: str,
                     run_id: str | None) -> dict:
        """``feed_state`` (state) + ``job_state('feed_ok:{url}')`` on success + ``feed_dead`` at the threshold."""
        prev = await self.state(feed.url) or {}
        fails = 0 if res.ok else int(prev.get("consecutive_failures") or 0) + 1
        etag, lm = (res.etag, res.last_modified) if res.status in (200, 304) and res.ok else \
            (prev.get("etag"), prev.get("last_modified"))
        eff = res.permanent_url or prev.get("effective_url")
        await self.db.execute(
            "INSERT INTO feed_state (feed_url, kind, occupant_key, effective_url, etag, last_modified, "
            "last_fetched_at, last_status, consecutive_failures, items_seen, last_new_item_at, enabled) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,1) "
            "ON CONFLICT(feed_url) DO UPDATE SET kind = excluded.kind, occupant_key = excluded.occupant_key, "
            "effective_url = excluded.effective_url, etag = excluded.etag, last_modified = excluded.last_modified, "
            "last_fetched_at = excluded.last_fetched_at, last_status = excluded.last_status, "
            "consecutive_failures = excluded.consecutive_failures, "
            "items_seen = feed_state.items_seen + excluded.items_seen, "
            "last_new_item_at = COALESCE(excluded.last_new_item_at, feed_state.last_new_item_at)",
            (feed.url, feed.kind, feed.occupant_key or feed.query_occupant, eff, etag, lm, now, res.status, fails,
             int(new_items), now if new_items else None))
        if res.ok:
            await self.db.upsert("job_state", [{"job": job, "key": f"feed_ok:{feed.url}", "updated_at": now,
                                                "value_json": json.dumps({"at": now, "status": res.status})}],
                                 conflict=("job", "key"))
        dead = int(self.s.RSS_DEAD_AFTER_FAILURES)
        if not res.ok and fails == dead:
            from app.storage.events import emit_event
            okey = feed.occupant_key
            await emit_event(self.db, event_type="feed_dead", occupant_key=okey, netuid=feed.netuid if okey else None,
                             event_at=now, observed_at=now,
                             payload={"feed_url": feed.url, "kind": feed.kind, "consecutive_failures": fails,
                                      "last_status": res.status, "error": res.error},
                             dedupe_key=f"{feed.url}|{now // (30 * DAY)}", source="rss", run_id=run_id)
        return {"consecutive_failures": fails}

    async def _due(self, feed: FeedRow, st: dict | None, now: int) -> bool:
        if st and not st.get("enabled", 1):
            return False                                     # operator off-switch in feed_state
        dead = int(self.s.RSS_DEAD_AFTER_FAILURES)
        if st and int(st.get("consecutive_failures") or 0) >= dead and st.get("last_fetched_at") \
                and now - int(st["last_fetched_at"]) < DAY:
            return False                                     # dead feeds are retried daily (§7)
        return True

    async def poll_feeds(self, feeds: list[FeedRow], *, job: str, run_id: str, now: int | None = None,
                         store: StoreFn | None = None) -> dict[str, Any]:
        store = store or self.store_news
        rep: dict[str, Any] = {"feeds": len(feeds), "ok": 0, "not_modified": 0, "failed": 0, "skipped": 0,
                               "new": 0, "partial": None}
        google_stopped = False
        for feed in feeds:
            t = int(now if now is not None else now_ts())
            st = await self.state(feed.url)
            if not await self._due(feed, st, t) or (google_stopped and feed.kind == "google_news"):
                rep["skipped"] += 1
                continue
            res = await self.fetch(feed, st)
            new = 0
            if res.ok and res.status == 200:
                try:
                    new = await store(feed, res, t, run_id)
                except Exception as exc:          # one malformed feed never kills the run
                    logger.warning("feed_store_failed", data={"feed": feed.url, "error": f"{type(exc).__name__}: "
                                                                                          f"{exc}"})
                    res.error = f"store: {type(exc).__name__}"
            await self.record(feed, res, new_items=new, now=t, job=job, run_id=run_id)
            rep["new"] += new
            if res.ok:
                rep["not_modified" if res.status == 304 else "ok"] += 1
            else:
                rep["failed"] += 1
                logger.info("feed_failed", data={"feed": feed.url, "status": res.status, "error": res.error})
                if feed.kind == "google_news" and res.status in (429, 503):
                    google_stopped = True                     # §7: back off, skip the remaining queries this run
                    rep["partial"] = "rate_limited"
        return rep

    async def poll_all(self, kinds: set[str] | tuple[str, ...], run: RunContext, *, job: str = JOB) -> JobOutcome:
        """§8.2 signature: poll every active feed of ``kinds`` (``rss_feeds_poll`` uses ``rss|substack|medium``)."""
        await load_seed_feeds(self.db, now=run.started_at)
        if self.index is None:
            from app.nlp.match import OccupantIndex
            self.index = await OccupantIndex.load(self.db)
        feeds = await feed_rows(self.db, tuple(kinds), s=self.s)
        rep = await self.poll_feeds(feeds, job=job, run_id=run.run_id)
        raise_if_all_failed(rep, "rss")
        return JobOutcome(rows=rep["new"], meta=rep, note=feeds_note(rep))

    # ── items ────────────────────────────────────────────────────────────────────────────────────────────────
    def keep(self, feed: FeedRow, text: str) -> bool:
        """Publisher feeds (``filtered=1``) keep an item only on a ``NEWS_KEYWORDS`` hit or a registry rule match."""
        if not feed.filtered:
            return True
        if self._kw is not None and self._kw.search(text):
            return True
        if self.index is not None:
            from app.nlp.match import match_occupants
            return bool(match_occupants(text, self.index))
        return False

    def attribution(self, feed: FeedRow, text: str) -> list[tuple[str, str]]:
        from app.nlp.match import match_occupants
        pairs: list[tuple[str, str]] = []
        if feed.query_occupant:
            pairs.append((feed.query_occupant, "query"))
        if self.index is not None:
            for k, _ in match_occupants(text, self.index):
                if all(k != p[0] for p in pairs):
                    pairs.append((k, "rule"))
        return pairs

    async def _decode_google(self, link: str) -> str | None:
        """Optional (``RSS_GOOGLE_NEWS_DECODE_URLS``): one GET without redirects; a ``Location`` off
        ``news.google.com`` is the publisher URL. Google's current article links mostly answer 200 + JS, so this
        often fails and the title+publisher hash is kept (UNVERIFIED longevity, §11 Q10)."""
        if self.decode_budget <= 0:
            return None
        self.decode_budget -= 1
        try:
            await self._space(link)
            r = await self.http.request("rss", "GET", link, timeout=float(self.s.RSS_HTTP_TIMEOUT_SEC),
                                        ok_statuses=(200, 301, 302, 303, 307, 308))
        except ProviderError:
            return None
        loc = r.headers.get("location")
        if r.status_code in _REDIRECTS and loc:
            nxt = urljoin(link, loc)
            if (urlsplit(nxt).hostname or "").lower() != GOOGLE_NEWS_HOST:
                return nxt
        return None

    async def news_item(self, feed: FeedRow, entry: Any, res: FeedFetch, now: int, run_id: str) -> dict | None:
        from app.collectors.news import google_publisher, google_title_hash, strip_publisher_suffix
        from app.nlp.match import host_of, url_canonical
        from app.nlp.textnorm import text_norm
        raw_title = strip_html(entry.get("title"))
        link = (entry.get("link") or "").strip()
        if not raw_title or not link:
            return None
        content = entry.get("content") or []
        body = content[0].get("value") if content and isinstance(content[0], dict) else None
        summary = strip_html(body or entry.get("summary"))[:SUMMARY_MAX] or None
        source = feed.kind if feed.kind in NEWS_SOURCES else "rss"
        if feed.kind == "google_news":
            publisher, domain = google_publisher(entry)
            title = strip_publisher_suffix(raw_title, publisher)
            summary = None                      # Google's description is only the linked title + publisher
            canonical = None
            if self.s.RSS_GOOGLE_NEWS_DECODE_URLS:
                th = google_title_hash(title, domain)
                exists = await self.db.fetchval("SELECT 1 FROM news_items WHERE url_hash = ?", (th,), writer=True)
                dec = None if exists else await self._decode_google(link)
                canonical = url_canonical(dec) if dec else None
            url_hash = sha256(canonical) if canonical else google_title_hash(title, domain)
        else:
            title = raw_title
            canonical = url_canonical(link)
            url_hash = sha256(canonical)
            publisher = (res.feed_meta.get("title") or feed.label or None)
            domain = host_of(canonical)
        text = f"{title} {summary or ''}".strip()
        if not self.keep(feed, text):
            return None
        ev = entry_time(entry)
        raw = {"title": entry.get("title"), "link": link, "id": entry.get("id"), "published": entry.get("published"),
               "updated": dict.get(entry, "updated"), "source": dict(entry.get("source") or {}) or None}
        from app.nlp.match import mentions_json
        lang = res.feed_meta.get("language")
        return {"url_hash": url_hash, "canonical_url": canonical, "url": link, "title": title, "summary": summary,
                "publisher": publisher, "publisher_domain": domain, "feed_url": feed.url, "feed_kind": feed.kind,
                "occupant_keys_json": mentions_json(self.attribution(feed, text)),
                "lang": str(lang)[:16] if lang else None,
                "text_hash": sha256(text_norm(title) + "\n" + text_norm(summary or "")),
                "event_at": ev if ev is not None else now, "event_at_source": "feed" if ev is not None else "observed",
                "observed_at": now, "source": source, "run_id": run_id,
                "raw_json_hash": sha256(json.dumps(raw, sort_keys=True, default=str, separators=(",", ":")))}

    async def store_news(self, feed: FeedRow, res: FeedFetch, now: int, run_id: str) -> int:
        rows: dict[str, dict] = {}
        for e in res.entries:
            row = await self.news_item(feed, e, res, now, run_id)
            if row is not None:
                rows.setdefault(row["url_hash"], row)
        return await self.db.insert_or_ignore("news_items", list(rows.values())) if rows else 0


def raise_if_all_failed(rep: dict[str, Any], provider: str) -> None:
    """A whole-run failure (every attempted feed failed) raises into 01's wrapper; partial failures do not."""
    attempted = rep["feeds"] - rep["skipped"]
    if attempted > 0 and rep["failed"] == attempted:
        raise ProviderError(provider, f"all {attempted} feeds failed", status=None, url=provider, attempts=attempted)


def feeds_note(rep: dict[str, Any]) -> str:
    note = (f"feeds={rep['feeds']} ok={rep['ok']} not_modified={rep['not_modified']} failed={rep['failed']} "
            f"skipped={rep['skipped']} new={rep['new']}")
    return note + (f" partial:{rep['partial']}" if rep.get("partial") else "")


async def feed_ok_at(db: "Database", feed_url: str) -> int | None:
    """Newest successful fetch (200/304) of ``feed_url`` by any Appendix A job (``job_state 'feed_ok:{url}'``)."""
    v = await db.fetchval("SELECT MAX(CAST(json_extract(value_json, '$.at') AS INTEGER)) FROM job_state "
                          "WHERE key = ?", (f"feed_ok:{feed_url}",))
    return int(v) if v is not None else None


class RssFeedsPollJob(CollectorJob):
    """Every ``RSS_POLL_INTERVAL_HOURS``: ``rss|substack|medium`` feeds (A.6)."""
    _last: dict | None = None

    async def collect(self, run: RunContext) -> JobOutcome:
        poller = FeedPoller(run.rt.db, run.rt.http, s=run.settings)
        out = await poller.poll_all(RSS_KINDS, run)
        self._last = out.meta
        return out

    def health_extra(self) -> dict | None:
        return dict(self._last) if self._last else None


# ── CLI ──────────────────────────────────────────────────────────────────────────────────────────────────────
async def open_cli() -> tuple["Database", "Http"]:
    from app.http import Http
    from app.ratelimit import ProviderLimiter
    from app.storage.db import Database
    s = app_config.settings
    db = Database(s.path(s.DB_PATH))
    await db.connect()
    await db.apply_schema()
    return db, Http(ProviderLimiter.from_settings())


async def _cli(args) -> int:
    import uuid
    db, http = await open_cli()
    try:
        if args.seed:
            print(json.dumps(await load_seed_feeds(db)))
            return 0
        from app.nlp.match import OccupantIndex
        await load_seed_feeds(db)
        poller = FeedPoller(db, http, index=await OccupantIndex.load(db))
        feeds = await feed_rows(db, RSS_KINDS)
        if args.dry_run:
            print(json.dumps([f.url for f in feeds]))
            return 0
        rep = await poller.poll_feeds(feeds, job=JOB, run_id=f"cli-{uuid.uuid4().hex[:12]}")
        print(json.dumps(rep, default=str))
        return 0
    finally:
        await http.aclose()
        await db.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m app.collectors.rss")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="poll rss|substack|medium feeds once")
    mode.add_argument("--seed", action="store_true", help="load seed/feeds.csv into watch_channels")
    ap.add_argument("--dry-run", action="store_true")
    return asyncio.run(_cli(ap.parse_args(argv)))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
