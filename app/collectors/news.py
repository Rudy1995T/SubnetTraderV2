"""Google News RSS (spec 09 Appendix A.1–A.2, A.6; §2.5 row ``news_google_poll``; §6.5; §7 "Google News 429/503").

Daily: the global ``google_news`` seed queries plus one generated query per top-``RSS_GOOGLE_NEWS_TOP_N`` occupant
(latest raw ``emission_share`` row, fallback the latest ``chain_hourly.emission``): ``"{name}" Bittensor``, or
``"SN{netuid}" Bittensor`` for placeholder names. Items go through ``rss.FeedPoller`` (provider ``rss``, spaced
``GOOGLE_NEWS_MIN_INTERVAL_SEC`` apart). Google appends " - {Publisher}" to titles — stripped; with URL decoding off
(the default) ``url_hash = sha256("t:" + title_norm + "|p:" + publisher_domain)`` and ``canonical_url`` is NULL.
Results of a per-subnet query carry ``{"occupant_key": <queried>, "kind": "query"}``. A 429/503 stops the remaining
queries of the run (``partial:rate_limited``).
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from typing import TYPE_CHECKING, Any
from urllib.parse import quote_plus

import app.config as app_config
from app.collectors.rss import (FeedPoller, FeedRow, feed_rows, feeds_note, keyword_re, load_seed_feeds,
                                raise_if_all_failed)
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

JOB = "news_google_poll"
DAY = 86400
GOOGLE_NEWS_SEARCH = "https://news.google.com/rss/search"
GOOGLE_NEWS_LOCALE = "hl=en-US&gl=US&ceid=US:en"
TOP_N_MAX_AGE_S = 2 * DAY                   # same freshness bound as 09 §2.5's x_mentions top-N


def strip_publisher_suffix(title: str, publisher: str | None) -> str:
    """``"Story - Yahoo Finance"`` → ``"Story"`` (only the exact ``<source>`` title is stripped)."""
    t = (title or "").strip()
    p = (publisher or "").strip()
    if p and t.endswith(f" - {p}") and len(t) > len(p) + 3:
        return t[: -(len(p) + 3)].rstrip()
    return t


def google_publisher(entry: Any) -> tuple[str | None, str | None]:
    """(publisher title, publisher domain) from Google's ``<source url=…>`` element."""
    from app.nlp.match import host_of
    src = entry.get("source") or {}
    title = (src.get("title") or "").strip() or None
    return title, host_of(src.get("href") or src.get("url"))


def google_title_hash(title: str, domain: str | None) -> str:
    from app.nlp.textnorm import text_norm
    return hashlib.sha256(f"t:{text_norm(title)}|p:{(domain or '').lower()}".encode("utf-8")).hexdigest()


def publisher_keyword_match(text: str, keywords: str | None = None) -> bool:
    """``NEWS_KEYWORDS`` (case-insensitive, any; word-bounded) — the publisher-feed filter's keyword half."""
    rx = keyword_re(app_config.settings.NEWS_KEYWORDS if keywords is None else keywords)
    return bool(rx and rx.search(text or ""))


def query_url(name: str | None, netuid: int) -> str:
    from app.nlp.match import is_placeholder
    term = f'"SN{netuid}"' if is_placeholder(name) else f'"{(name or "").strip()}"'
    return f"{GOOGLE_NEWS_SEARCH}?q={quote_plus(term + ' Bittensor')}&{GOOGLE_NEWS_LOCALE}"


async def top_n_occupants(db: "Database", n: int, *, now: int) -> list[dict]:
    """Top-``n`` current occupants by the latest raw ``emission_share`` (``status='ok'``, ``as_of ≥ now − 2 d``),
    filled up from the latest ``chain_hourly.emission`` when fewer exist (before Phase 8 / a scoring outage)."""
    rows = await db.fetchall(
        "SELECT sn.occupant_key, sn.netuid, sn.name, x.value FROM (SELECT occupant_key, value, ROW_NUMBER() OVER "
        "(PARTITION BY occupant_key ORDER BY as_of DESC, observed_at DESC) AS rn FROM metric_values "
        "WHERE metric_key = 'emission_share' AND stage = 'raw' AND status = 'ok' AND as_of >= ? AND as_of <= ?) x "
        "JOIN subnets sn ON sn.occupant_key = x.occupant_key AND sn.netuid > 0 AND sn.status <> 'deregistered' "
        "WHERE x.rn = 1 ORDER BY x.value DESC, sn.netuid LIMIT ?", (now - TOP_N_MAX_AGE_S, now, n))
    out = [dict(r) for r in rows]
    if len(out) < n:
        have = {r["occupant_key"] for r in out}
        fb = await db.fetchall(
            "SELECT sn.occupant_key, sn.netuid, sn.name, x.emission AS value FROM (SELECT occupant_key, emission, "
            "ROW_NUMBER() OVER (PARTITION BY occupant_key ORDER BY hour_ts DESC) AS rn FROM chain_hourly "
            "WHERE hour_ts >= ? AND hour_ts <= ? AND emission IS NOT NULL) x JOIN subnets sn "
            "ON sn.occupant_key = x.occupant_key AND sn.netuid > 0 AND sn.status <> 'deregistered' "
            "WHERE x.rn = 1 ORDER BY x.emission DESC, sn.netuid", (now - TOP_N_MAX_AGE_S, now))
        for r in fb:
            if len(out) >= n:
                break
            if r["occupant_key"] not in have:
                out.append(dict(r))
                have.add(r["occupant_key"])
    return out


async def news_feeds(db: "Database", *, now: int, s=None) -> list[FeedRow]:
    """Global seed queries (``watch_channels`` ``google_news`` rows) + the generated per-subnet queries."""
    s = s or app_config.settings
    feeds = await feed_rows(db, ("google_news",), s=s)
    seen = {f.url for f in feeds}
    for o in await top_n_occupants(db, int(s.RSS_GOOGLE_NEWS_TOP_N), now=now):
        u = query_url(o["name"], int(o["netuid"]))
        if u in seen:
            continue
        seen.add(u)
        feeds.append(FeedRow(url=u, kind="google_news", occupant_key=None, netuid=int(o["netuid"]),
                             label=f"per-subnet SN{o['netuid']}", query_occupant=o["occupant_key"]))
    return feeds


async def run_news_poll(db: "Database", http: Any, *, now: int, run_id: str, s=None, poller: FeedPoller | None = None,
                        dry_run: bool = False) -> dict[str, Any]:
    from app.nlp.match import OccupantIndex
    s = s or app_config.settings
    await load_seed_feeds(db, now=now)
    feeds = await news_feeds(db, now=now, s=s)
    if dry_run:
        return {"feeds": len(feeds), "plan": [f.url for f in feeds]}
    poller = poller or FeedPoller(db, http, s=s)
    if poller.index is None:
        poller.index = await OccupantIndex.load(db)
    rep = await poller.poll_feeds(feeds, job=JOB, run_id=run_id)
    rep["queries"] = sum(1 for f in feeds if f.query_occupant)
    return rep


class NewsGooglePollJob(CollectorJob):
    _last: dict | None = None

    async def collect(self, run: RunContext) -> JobOutcome:
        rep = await run_news_poll(run.rt.db, run.rt.http, now=run.started_at, run_id=run.run_id, s=run.settings)
        self._last = rep
        raise_if_all_failed(rep, "rss")
        return JobOutcome(rows=rep["new"], meta=rep, note=feeds_note(rep) + f" queries={rep['queries']}")

    def health_extra(self) -> dict | None:
        return dict(self._last) if self._last else None


async def _cli(args) -> int:
    import uuid

    from app.collectors.rss import open_cli
    db, http = await open_cli()
    try:
        rep = await run_news_poll(db, http, now=now_ts(), run_id=f"cli-{uuid.uuid4().hex[:12]}",
                                  dry_run=args.dry_run)
        print(json.dumps(rep, default=str))
        return 0
    finally:
        await http.aclose()
        await db.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m app.collectors.news")
    ap.add_argument("--once", action="store_true", required=True, help="one Google News pass")
    ap.add_argument("--dry-run", action="store_true")
    return asyncio.run(_cli(ap.parse_args(argv)))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
