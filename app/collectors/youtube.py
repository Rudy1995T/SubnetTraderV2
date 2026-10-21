"""YouTube channel + playlist RSS (spec 09 Appendix A.1–A.3, A.6; §2.5 row ``youtube_poll``; §3.5
``youtube_channel_resolved``; §7 "consent/JS wall").

Daily over provider ``youtube``: (1) channel-id discovery for active occupant rows (``platform`` ``youtube`` — a
declared channel — or ``youtube_video`` — 04's subnetalpha ``video_url``) whose ``meta_json.channel_id`` is missing,
each row retried weekly, at most ``DISCOVERY_MAX_PER_RUN`` lookups per run; the id is merged into ``meta_json``
(``channel_id``, ``channel_id_method``) with ``verified_at`` — ``handle`` is never overwritten; (2) the own-channel test
(A.2, pre-build review 2026-10-18): a channel resolved from a ``youtube_video`` row is the occupant's own only when it
is not an ecosystem channel (``YOUTUBE_ECOSYSTEM_CHANNELS`` or any row with ``meta_json.ecosystem=true``) and no other
occupant's rows resolve to it — else ``meta_json.channel_role`` = ``ecosystem`` | ``shared`` and it is not polled for
the occupant; (3) ``feeds/videos.xml?channel_id=`` for own channels + the Novelty Search playlist → ``video_items``.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import sys
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlsplit

import app.config as app_config
from app.collectors.rss import (FeedFetch, FeedPoller, FeedRow, entry_time, feeds_note, load_seed_feeds,
                                raise_if_all_failed, strip_html)
from app.http import ProviderError
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.http import Http
    from app.storage.db import Database

JOB = "youtube_poll"
PROVIDER = "youtube"
DAY = 86400
YOUTUBE_ECOSYSTEM_CHANNELS: frozenset[str] = frozenset({
    "UCxLqly5YxULobLJwcYjMhJQ",      # taostats (hosts many subnet presentation videos)
    "UCq-UgxLUK1mYJBxrmOfP7aA",      # Opentensor Foundation (Novelty Search playlist owner)
})
DISCOVERY_MAX_PER_RUN = 40          # lookups (≤ 3 requests each at 0.33/s) so the run stays under its 900-s timeout
DISCOVERY_RETRY_S = 7 * DAY          # §7: failures are retried weekly
CHANNEL_FEED = "https://www.youtube.com/feeds/videos.xml?channel_id={}"
PLAYLIST_FEED = "https://www.youtube.com/feeds/videos.xml?playlist_id={}"
YT_API = "https://www.googleapis.com/youtube/v3"
DESKTOP_UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
PAGE_HEADERS = {"User-Agent": DESKTOP_UA, "Cookie": "CONSENT=YES+cb", "Accept-Language": "en"}

_UC = r"UC[0-9A-Za-z_-]{22}"
UC_RE = re.compile(rf"^{_UC}$")
CHANNEL_PATH_RE = re.compile(rf"/channel/({_UC})(?![0-9A-Za-z_-])")
VIDEO_ID_RE = re.compile(r"^[0-9A-Za-z_-]{11}$")
# channel page (A.2 order) — live 2026-10-18 a channel page from this host hit the EU consent wall despite the cookie
EXTERNAL_ID_RE = re.compile(rf'"externalId":"({_UC})"')
CANONICAL_RE = re.compile(rf'<link rel="canonical" href="https://www\.youtube\.com/channel/({_UC})"')
IDENTIFIER_RE = re.compile(rf'<meta itemprop="identifier" content="({_UC})"')
# video page — ``externalChannelId`` verified live 2026-10-18 on XsNSghevAAQ (→ taostats)
EXTERNAL_CHANNEL_ID_RE = re.compile(rf'"externalChannelId":"({_UC})"')
VIDEO_DETAILS_CHANNEL_RE = re.compile(
    rf'"videoDetails":\{{"videoId":"[0-9A-Za-z_-]{{11}}"[^{{}}]*?"channelId":"({_UC})"')
CHANNEL_ID_RE = re.compile(rf'"channelId":"({_UC})"')


def video_id_of(s: str | None) -> str | None:
    """``watch?v=`` / ``youtu.be/`` / ``shorts|embed|live/`` URLs, or a bare 11-char id (04's ``youtube_video``)."""
    t = (s or "").strip()
    if not t:
        return None
    if "/" not in t and "." not in t:
        return t if VIDEO_ID_RE.match(t) and not t.startswith("@") else None
    try:
        p = urlsplit(t if "://" in t else "https://" + t)
    except ValueError:
        return None
    host = (p.hostname or "").lower()
    segs = [x for x in p.path.split("/") if x]
    if host.endswith("youtu.be") and segs:
        return segs[0] if VIDEO_ID_RE.match(segs[0]) else None
    if host.endswith("youtube.com"):
        v = parse_qs(p.query).get("v", [None])[0]
        if segs[:1] == ["watch"] and v and VIDEO_ID_RE.match(v):
            return v
        if len(segs) >= 2 and segs[0] in ("shorts", "embed", "live") and VIDEO_ID_RE.match(segs[1]):
            return segs[1]
    return None


def handle_path_of(s: str | None) -> str | None:
    """``@handle`` / ``youtube.com/@h`` / ``/c/x`` / ``/user/x`` → the channel page path (``@h``, ``c/x``, …)."""
    t = (s or "").strip()
    if t.startswith("@") and "/" not in t:
        return t
    try:
        p = urlsplit(t if "://" in t else "https://" + t)
    except ValueError:
        return None
    if not (p.hostname or "").lower().endswith("youtube.com"):
        return None
    segs = [x for x in p.path.split("/") if x]
    if segs and segs[0].startswith("@"):
        return segs[0]
    if len(segs) >= 2 and segs[0] in ("c", "user"):
        return f"{segs[0]}/{segs[1]}"
    return None


def channel_id_in(s: str | None) -> str | None:
    t = (s or "").strip()
    if UC_RE.match(t):
        return t
    m = CHANNEL_PATH_RE.search(t) or re.search(rf"[?&]channel_id=({_UC})", t)
    return m.group(1) if m else None


async def _get(http: "Http", url: str, *, params: dict | None = None, headers: dict | None = None) -> Any | None:
    try:
        return await http.request(PROVIDER, "GET", url, params=params, headers=headers,
                                  timeout=float(app_config.settings.RSS_HTTP_TIMEOUT_SEC), ok_statuses=(200,))
    except ProviderError as exc:
        logger.info("youtube_lookup_failed", data={"url": url.split("?")[0], "status": exc.status})
        return None


async def _channel_page(http: "Http", path: str) -> tuple[str | None, str]:
    r = await _get(http, f"https://www.youtube.com/{path.lstrip('/')}", headers=PAGE_HEADERS)
    if r is None or r.status_code != 200:            # 3xx → consent.youtube.com interstitial (not followed)
        return None, "none"
    body = r.text
    for rx, method in ((EXTERNAL_ID_RE, "externalId"), (CANONICAL_RE, "canonical"), (IDENTIFIER_RE, "canonical")):
        m = rx.search(body)
        if m:
            return m.group(1), method
    return None, "none"


async def discover_channel_id(url_or_handle: str, http: "Http", *, api_key: str | None = None
                              ) -> tuple[str | None, str]:
    """(channel_id, method ∈ path|api|oembed|externalId|canonical|video_page|none) — A.2's order."""
    key = app_config.settings.YOUTUBE_API_KEY if api_key is None else api_key
    cid = channel_id_in(url_or_handle)
    if cid:
        return cid, "path"
    vid = video_id_of(url_or_handle)
    hpath = None if vid else handle_path_of(url_or_handle)
    if key and (vid or (hpath and hpath.startswith("@"))):
        if vid:
            r = await _get(http, f"{YT_API}/videos", params={"part": "snippet", "id": vid, "key": key})
        else:
            r = await _get(http, f"{YT_API}/channels", params={"part": "id", "forHandle": hpath, "key": key})
        if r is not None and r.status_code == 200:
            try:
                items = r.json().get("items") or []
                got = (items[0].get("snippet") or {}).get("channelId") if vid else items[0].get("id")
            except (ValueError, IndexError, AttributeError):
                got = None
            if got and UC_RE.match(got):
                return got, "api"
    if vid:
        r = await _get(http, f"https://www.youtube.com/watch?v={vid}", headers=PAGE_HEADERS)
        if r is not None and r.status_code == 200:
            body = r.text
            for rx in (EXTERNAL_CHANNEL_ID_RE, VIDEO_DETAILS_CHANNEL_RE, CHANNEL_ID_RE):
                m = rx.search(body)
                if m:
                    return m.group(1), "video_page"
        r = await _get(http, "https://www.youtube.com/oembed",
                       params={"url": f"https://www.youtube.com/watch?v={vid}", "format": "json"})
        if r is not None and r.status_code == 200:
            try:
                author_url = str(r.json().get("author_url") or "")
            except ValueError:
                author_url = ""
            cid = channel_id_in(author_url)
            if cid:
                return cid, "oembed"
            hpath = handle_path_of(author_url)            # live 2026-10-18: author_url is the @handle form
    if hpath:
        return await _channel_page(http, hpath)
    return None, "none"


# ── discovery + own-channel test ─────────────────────────────────────────────────────────────────────────────
def _meta(r: dict) -> dict:
    try:
        return json.loads(r["meta_json"]) if r.get("meta_json") else {}
    except ValueError:
        return {}


async def resolve_pending(db: "Database", http: "Http", *, now: int, run_id: str | None,
                          limit: int = DISCOVERY_MAX_PER_RUN) -> dict[str, int]:
    rows = await db.fetchall(
        "SELECT * FROM watch_channels WHERE platform IN ('youtube', 'youtube_video') AND retired_at IS NULL "
        "AND occupant_key IS NOT NULL AND json_extract(COALESCE(meta_json, '{}'), '$.channel_id') IS NULL "
        "AND COALESCE(json_extract(meta_json, '$.channel_id_attempted_at'), 0) <= ? ORDER BY platform, netuid, id",
        (now - DISCOVERY_RETRY_S,))
    rep = {"pending": len(rows), "resolved": 0, "failed": 0, "lookups": 0}
    cache: dict[str, tuple[str | None, str]] = {}
    from app.storage.events import emit_event
    for r in rows:
        inp = r["handle"] or r["url"] or ""
        if inp not in cache:
            if not channel_id_in(inp):
                if rep["lookups"] >= limit:
                    continue
                rep["lookups"] += 1
            cache[inp] = await discover_channel_id(inp, http)
        cid, method = cache[inp]
        if cid:
            await db.execute(
                "UPDATE watch_channels SET verified_at = ?, meta_json = json_set(COALESCE(meta_json, '{}'), "
                "'$.channel_id', ?, '$.channel_id_method', ?, '$.channel_id_attempted_at', ?) WHERE id = ?",
                (now, cid, method, now, r["id"]))
            rep["resolved"] += 1
            await emit_event(db, event_type="youtube_channel_resolved", occupant_key=r["occupant_key"],
                             netuid=r["netuid"], event_at=now, observed_at=now,
                             payload={"input_url": r["url"] or r["handle"], "channel_id": cid, "method": method,
                                      "platform": r["platform"]},
                             dedupe_key=f"{r['occupant_key']}|{cid}", source="rss", run_id=run_id)
        else:
            await db.execute(
                "UPDATE watch_channels SET meta_json = json_set(COALESCE(meta_json, '{}'), '$.channel_id_method', "
                "'none', '$.channel_id_attempted_at', ?) WHERE id = ?", (now, r["id"]))
            rep["failed"] += 1
    return rep


async def ecosystem_channels(db: "Database") -> set[str]:
    rows = await db.fetchall("SELECT json_extract(meta_json, '$.channel_id') AS cid FROM watch_channels "
                             "WHERE json_extract(meta_json, '$.ecosystem') IN (1, 'true') "
                             "AND json_extract(meta_json, '$.channel_id') IS NOT NULL")
    return set(YOUTUBE_ECOSYSTEM_CHANNELS) | {r["cid"] for r in rows}


async def classify_channels(db: "Database") -> dict[str, int]:
    """A.2's own-channel test → ``meta_json.channel_role`` on every resolved active ``youtube_video`` row."""
    eco = await ecosystem_channels(db)
    rows = await db.fetchall(
        "SELECT id, occupant_key, platform, json_extract(meta_json, '$.channel_id') AS cid, "
        "json_extract(meta_json, '$.channel_role') AS role FROM watch_channels WHERE platform IN ('youtube', "
        "'youtube_video') AND retired_at IS NULL AND occupant_key IS NOT NULL "
        "AND json_extract(meta_json, '$.channel_id') IS NOT NULL")
    owners: dict[str, set[str]] = {}
    for r in rows:
        owners.setdefault(r["cid"], set()).add(r["occupant_key"])
    counts = {"own": 0, "ecosystem": 0, "shared": 0}
    for r in rows:
        if r["platform"] != "youtube_video":
            continue
        role = "ecosystem" if r["cid"] in eco else "shared" if len(owners[r["cid"]]) > 1 else "own"
        counts[role] += 1
        if role != r["role"]:
            await db.execute("UPDATE watch_channels SET meta_json = json_set(COALESCE(meta_json, '{}'), "
                             "'$.channel_role', ?) WHERE id = ?", (role, r["id"]))
    return counts


async def youtube_feeds(db: "Database", *, s=None) -> list[FeedRow]:
    """Own channels (declared ``youtube`` rows + ``youtube_video`` rows with role ``own``), the Novelty Search
    playlist (``YOUTUBE_NOVELTY_PLAYLIST_ID``) and any ecosystem ``youtube`` seed rows."""
    s = s or app_config.settings
    rows = await db.fetchall("SELECT * FROM watch_channels WHERE platform IN ('youtube', 'youtube_video') "
                             "AND retired_at IS NULL ORDER BY netuid, id")
    by_channel: dict[str, list[dict]] = {}
    seeds: list[FeedRow] = []
    for r in rows:
        m = _meta(r)
        if r["occupant_key"] is None:
            if r["platform"] == "youtube":
                url = r["url"] or r["handle"]
                pl = m.get("playlist_id") or (re.search(r"[?&]playlist_id=([\w-]+)", url) or [None, None])[1]
                seeds.append(FeedRow(url=url, kind="youtube", label=r["label"], provider=PROVIDER, watch_id=r["id"],
                                     playlist_id=pl, channel_id=None if pl else channel_id_in(url)))
            continue
        cid = m.get("channel_id") or (channel_id_in(r["handle"]) if r["platform"] == "youtube" else None)
        if not cid or (r["platform"] == "youtube_video" and m.get("channel_role") != "own"):
            continue
        by_channel.setdefault(cid, []).append(r)
    out: dict[str, FeedRow] = {}
    pid = (s.YOUTUBE_NOVELTY_PLAYLIST_ID or "").strip()
    if pid:
        out[PLAYLIST_FEED.format(pid)] = FeedRow(url=PLAYLIST_FEED.format(pid), kind="youtube", label="Novelty Search",
                                                 provider=PROVIDER, playlist_id=pid)
    for f in seeds:
        out.setdefault(f.url, f)
    for cid, rs in by_channel.items():
        okeys = sorted({r["occupant_key"] for r in rs})
        one = okeys[0] if len(okeys) == 1 else None
        u = CHANNEL_FEED.format(cid)
        out.setdefault(u, FeedRow(url=u, kind="youtube", occupant_key=one, netuid=rs[0]["netuid"] if one else None,
                                  label=f"channel {cid}", provider=PROVIDER, channel_id=cid))
    return list(out.values())


# ── items ────────────────────────────────────────────────────────────────────────────────────────────────────
def _video_id(e: Any) -> str | None:
    v = e.get("yt_videoid")
    if not v and str(e.get("id") or "").startswith("yt:video:"):
        v = str(e["id"])[9:]
    return v if v and VIDEO_ID_RE.match(v) else None


def _views(e: Any) -> int | None:
    st = e.get("media_statistics") or {}
    try:
        return int(st.get("views")) if st.get("views") not in (None, "") else None
    except (TypeError, ValueError):
        return None


def make_store_videos(poller: FeedPoller):
    async def store_videos(feed: FeedRow, res: FeedFetch, now: int, run_id: str) -> int:
        from app.nlp.match import match_occupants, mentions_json
        rows: dict[str, dict] = {}
        for e in res.entries:
            vid = _video_id(e)
            cid = e.get("yt_channelid") or feed.channel_id
            title = strip_html(e.get("title"))
            if not vid or not cid or not title:
                continue
            desc = (e.get("summary") or "").strip() or None
            pairs: list[tuple[str, str]] = []
            if feed.occupant_key is None and poller.index is not None:          # playlist / ecosystem feeds
                pairs = [(k, "rule") for k, _ in match_occupants(f"{title} {desc or ''}", poller.index)]
            ev = entry_time(e)
            raw = {"id": vid, "channel": cid, "title": e.get("title"), "published": e.get("published"),
                   "updated": dict.get(e, "updated"), "views": _views(e)}
            rows.setdefault(vid, {
                "video_id": vid, "channel_id": cid, "channel_title": e.get("author") or res.feed_meta.get("title"),
                "occupant_key": feed.occupant_key, "occupant_keys_json": mentions_json(pairs),
                "playlist_id": feed.playlist_id, "title": title, "description": desc, "views": _views(e),
                "event_at": ev if ev is not None else now, "observed_at": now,
                "source": "playlist_rss" if feed.playlist_id else "channel_rss", "run_id": run_id,
                "raw_json_hash": hashlib.sha256(json.dumps(raw, sort_keys=True, default=str).encode()).hexdigest()})
        return await poller.db.insert_or_ignore("video_items", list(rows.values())) if rows else 0
    return store_videos


async def run_youtube_poll(db: "Database", http: "Http", *, now: int, run_id: str, s=None,
                           poller: FeedPoller | None = None, discover: bool = True,
                           dry_run: bool = False) -> dict[str, Any]:
    from app.nlp.match import OccupantIndex
    s = s or app_config.settings
    await load_seed_feeds(db, now=now)
    disc = await resolve_pending(db, http, now=now, run_id=run_id) if discover and not dry_run else {}
    roles = await classify_channels(db)
    feeds = await youtube_feeds(db, s=s)
    if dry_run:
        return {"feeds": len(feeds), "plan": [f.url for f in feeds], "roles": roles}
    poller = poller or FeedPoller(db, http, s=s)
    if poller.index is None:
        poller.index = await OccupantIndex.load(db)
    rep = await poller.poll_feeds(feeds, job=JOB, run_id=run_id, store=make_store_videos(poller))
    rep.update(discovery=disc, roles=roles)
    return rep


class YoutubePollJob(CollectorJob):
    _last: dict | None = None

    async def collect(self, run: RunContext) -> JobOutcome:
        rep = await run_youtube_poll(run.rt.db, run.rt.http, now=run.started_at, run_id=run.run_id, s=run.settings)
        self._last = rep
        raise_if_all_failed(rep, PROVIDER)
        d = rep.get("discovery") or {}
        return JobOutcome(rows=rep["new"], meta=rep, note=feeds_note(rep) + f" resolved={d.get('resolved', 0)} "
                                                                          f"own={rep['roles'].get('own', 0)}")

    def health_extra(self) -> dict | None:
        return dict(self._last) if self._last else None


async def _cli(args) -> int:
    import uuid

    from app.collectors.rss import open_cli
    db, http = await open_cli()
    try:
        if args.discover:
            print(json.dumps(await discover_channel_id(args.discover, http)))
            return 0
        rep = await run_youtube_poll(db, http, now=now_ts(), run_id=f"cli-{uuid.uuid4().hex[:12]}",
                                     dry_run=args.dry_run)
        print(json.dumps(rep, default=str))
        return 0
    finally:
        await http.aclose()
        await db.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m app.collectors.youtube")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="discovery + channel/playlist RSS once")
    mode.add_argument("--discover", metavar="URL_OR_HANDLE", help="resolve one channel id and print it")
    ap.add_argument("--dry-run", action="store_true")
    return asyncio.run(_cli(ap.parse_args(argv)))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
