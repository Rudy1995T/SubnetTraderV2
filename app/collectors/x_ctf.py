"""Free subnetalpha ``.ctf`` tweet-feed cross-check (spec 09 §2.5 row ``x_ctf_crosscheck``, §2.9, §3.5).

Daily ``GET https://subnetalpha.ai/subnet/{slug}/`` (provider ``subnetalpha``) for every current occupant whose
04 identity has a ``twitter_embed_id``. Items whose author matches one of the occupant's active non-legacy-only X
handles become ``social_posts(source='subnetalpha_ctf')`` rows (§2.6 conflict rule); a page with items but no match
emits ``x_ctf_discrepancy(reason='handle_mismatch')``; with a paid provider active a > 7 d disagreement of the newest
post emits ``reason='gap'``. Live 2026-10-18 the pages carry only the plugin CSS (0 items) — every fetch still writes
``job_state('x_ctf_crosscheck', 'page:{occupant_key}')`` so 09's producer fns can tell "no feed" from silence.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, AsyncIterator
from urllib.parse import urlsplit

import app.config as app_config
from app.collectors.x_providers.base import snowflake_ms
from app.http import ProviderError
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.http import Http
    from app.nlp.match import OccupantIndex
    from app.storage.db import Database

JOB = "x_ctf_crosscheck"
PROVIDER = "subnetalpha"
SOURCE = "subnetalpha_ctf"
DAY = 86400
GAP_DAYS = 7                                   # §2.9 (b)
PAID_SOURCES = ("twitterapi_io", "official_v2")
_STATUS_RE = re.compile(r"/status(?:es)?/(\d{5,25})")
_URL_RE = re.compile(r"https?://\S+")


@dataclass(frozen=True, slots=True)
class CtfTweet:
    post_id: str
    event_at: int                  # snowflake time, unix seconds (exact)
    author_handle: str | None      # case as linked; None when the item has no parsable X link
    text: str
    date_text: str | None          # ``.ctf-tweet-date`` ("16 Sep") — sanity check only
    has_url: bool


@dataclass(frozen=True, slots=True)
class CtfPage:
    occupant_key: str
    netuid: int
    slug: str
    embed_id: str

    def url(self, base: str | None = None) -> str:
        return f"{(base or page_base()).rstrip('/')}/subnet/{self.slug.strip('/')}/"


@dataclass(slots=True)
class CtfFetch:
    page: CtfPage
    http_status: int | None
    html: str
    error: str | None = None


def page_base() -> str:
    """``https://subnetalpha.ai`` — the host of 04's WP-REST base URL."""
    p = urlsplit(app_config.settings.SUBNETALPHA_BASE_URL)
    return f"{p.scheme or 'https'}://{p.netloc or 'subnetalpha.ai'}"


def snowflake_to_datetime(post_id: str | int) -> datetime:
    """X snowflake → aware UTC datetime (``(id >> 22) + 1288834974657`` ms)."""
    return datetime.fromtimestamp(snowflake_ms(post_id) / 1000.0, timezone.utc)


# ── parser ───────────────────────────────────────────────────────────────────────────────────────────────────
def _has_class(cls: str) -> str:
    return f"contains(concat(' ', normalize-space(@class), ' '), ' {cls} ')"


def _x_handle_of(href: str | None) -> str | None:
    from app.registry.normalise import X_HOSTS, normalise_x_handle
    if not href:
        return None
    try:
        host = (urlsplit(href if "://" in href else "https://" + href).hostname or "").lower()
    except ValueError:
        return None
    if host not in X_HOSTS:
        return None
    return normalise_x_handle(href)


def _text(node: Any) -> str:
    return re.sub(r"[ \t\r\f\v]+", " ", node.text_content() or "").strip()


def parse_ctf(html: str | bytes | None) -> list[CtfTweet]:
    """Research selectors (§2.9): ``.ctf-item``, ``a[href*="/status/"]`` (the id), ``.ctf-tweet-text``,
    ``.ctf-tweet-date``; the author comes from the item's X profile link (``.ctf-author-*`` first — UNVERIFIED,
    no live item exists since 2026-10-18), else the ``/{H}/status/{id}`` link. A page without items → ``[]``."""
    import lxml.html
    if not html or not (html.strip() if isinstance(html, str) else html.strip()):
        return []
    try:
        root = lxml.html.fromstring(html)
    except (ValueError, TypeError, lxml.etree.ParserError):      # type: ignore[attr-defined]
        return []
    out: list[CtfTweet] = []
    seen: set[str] = set()
    for item in root.xpath(f"//*[{_has_class('ctf-item')}]"):
        post_id = status_handle = None
        for a in item.xpath(".//a[contains(@href, '/status/')]"):
            m = _STATUS_RE.search(a.get("href") or "")
            if m:
                post_id = m.group(1)
                status_handle = _x_handle_of(a.get("href"))
                break
        if post_id is None or post_id in seen:
            continue
        seen.add(post_id)
        handle = None
        author_links = item.xpath(f".//*[{_has_class('ctf-author-name')} or {_has_class('ctf-author-box-link')} or "
                                  f"{_has_class('ctf-author-screenname')}]/descendant-or-self::a[@href]")
        profile_links = [a for a in item.xpath(".//a[@href]") if "/status/" not in (a.get("href") or "")]
        for a in list(author_links) + profile_links:
            handle = _x_handle_of(a.get("href"))
            if handle:
                break
        handle = handle or status_handle
        tnodes = item.xpath(f".//*[{_has_class('ctf-tweet-text')}]")
        text = _text(tnodes[0]) if tnodes else ""
        has_url = bool(_URL_RE.search(text)) or bool(
            tnodes and any((a.get("href") or "").startswith("http") for a in tnodes[0].xpath(".//a[@href]")))
        dnodes = item.xpath(f".//*[{_has_class('ctf-tweet-date')}]")
        out.append(CtfTweet(post_id, snowflake_ms(post_id) // 1000, handle, text,
                            _text(dnodes[0]) or None if dnodes else None, has_url))
    return out


# ── targets + fetch ──────────────────────────────────────────────────────────────────────────────────────────
async def ctf_pages(db: "Database") -> list[CtfPage]:
    """Current occupants with a subnetalpha slug and a non-empty ``twitter_embed_id`` (04, ``v_identity_current``)."""
    rows = await db.fetchall(
        "SELECT e.occupant_key, sn.netuid, s.value AS slug, e.value AS embed_id FROM v_identity_current e "
        "JOIN v_identity_current s ON s.occupant_key = e.occupant_key AND s.source = 'subnetalpha' "
        "AND s.field = 'slug' "
        "JOIN subnets sn ON sn.occupant_key = e.occupant_key AND sn.netuid > 0 AND sn.status <> 'deregistered' "
        "WHERE e.source = 'subnetalpha' AND e.field = 'twitter_embed_id' AND TRIM(COALESCE(e.value, '')) <> '' "
        "AND TRIM(COALESCE(s.value, '')) <> '' ORDER BY sn.netuid")
    return [CtfPage(r["occupant_key"], int(r["netuid"]), r["slug"].strip(), r["embed_id"].strip()) for r in rows]


async def fetch_ctf_pages(http: "Http", pages: list[CtfPage]) -> AsyncIterator[CtfFetch]:
    """One GET per page through ``Http`` (provider ``subnetalpha``, 1 req/s shared with 04); never raises."""
    s = app_config.settings
    base = page_base()
    for p in pages:
        try:
            r = await http.request(PROVIDER, "GET", p.url(base), headers={"Accept": "text/html"},
                                   ok_statuses=(200, 404, 410), follow_redirects=True,
                                   timeout=float(s.SUBNETALPHA_TIMEOUT_SEC))
            yield CtfFetch(p, r.status_code, r.text if r.status_code == 200 else "",
                           None if r.status_code == 200 else f"http_{r.status_code}")
        except ProviderError as exc:
            yield CtfFetch(p, exc.status, "", f"provider_error: {str(exc)[:160]}")


# ── rows ─────────────────────────────────────────────────────────────────────────────────────────────────────
def _hash_text(text: str, post_id: str) -> str:
    from app.nlp.textnorm import text_hash, text_norm
    if not text_norm(text):
        return hashlib.sha256(f"msg:x:{post_id}".encode()).hexdigest()
    return text_hash(text)


def _raw_hash(t: CtfTweet) -> str:
    return hashlib.sha256(json.dumps(asdict(t), sort_keys=True, separators=(",", ":")).encode()).hexdigest()


# §2.6 conflict rule; COALESCE keeps engagement when a NULL-engagement ctf observation meets a provider row.
UPSERT_SQL = (
    "INSERT INTO social_posts (platform, post_id, author_id, author_handle, occupant_key, is_team_account, is_kol, "
    "kol_handle, text, text_hash, lang, url, like_count, repost_count, reply_count, quote_count, view_count, "
    "engagement_observed_at, has_url, has_media, in_reply_to, is_repost, quoted_post_id, author_credibility, "
    "credibility_flags, bot_filtered, event_at, observed_at, source, run_id, raw_json_hash) "
    "VALUES ('x',?,?,?,?,1,?,?,?,?,NULL,?,NULL,NULL,NULL,NULL,NULL,NULL,?,NULL,NULL,0,NULL,1.0,?,0,?,?,?,?,?) "
    "ON CONFLICT(platform, post_id) DO UPDATE SET "
    "like_count = COALESCE(excluded.like_count, social_posts.like_count), "
    "repost_count = COALESCE(excluded.repost_count, social_posts.repost_count), "
    "reply_count = COALESCE(excluded.reply_count, social_posts.reply_count), "
    "quote_count = COALESCE(excluded.quote_count, social_posts.quote_count), "
    "view_count = COALESCE(excluded.view_count, social_posts.view_count), "
    "engagement_observed_at = CASE WHEN excluded.like_count IS NULL AND excluded.repost_count IS NULL "
    "AND excluded.reply_count IS NULL THEN social_posts.engagement_observed_at ELSE excluded.observed_at END, "
    "source = CASE WHEN social_posts.source = 'subnetalpha_ctf' THEN excluded.source ELSE social_posts.source END, "
    "author_id = CASE WHEN social_posts.author_id LIKE 'h:%' THEN excluded.author_id ELSE social_posts.author_id END")


async def _author_ids(db: "Database", handles_lower: set[str]) -> dict[str, str]:
    if not handles_lower:
        return {}
    ph = ",".join("?" * len(handles_lower))
    rows = await db.fetchall(f"SELECT handle_lower, author_id FROM x_accounts WHERE handle_lower IN ({ph})",
                             tuple(sorted(handles_lower)))
    return {r["handle_lower"]: r["author_id"] for r in rows}


async def _kol_handles(db: "Database", handles_lower: set[str]) -> dict[str, str]:
    if not handles_lower:
        return {}
    ph = ",".join("?" * len(handles_lower))
    rows = await db.fetchall(f"SELECT handle, handle_lower FROM kol_accounts WHERE verified = 1 AND active = 1 "
                             f"AND handle_lower IN ({ph})", tuple(sorted(handles_lower)))
    return {r["handle_lower"]: r["handle"] for r in rows}


async def store_ctf_posts(db: "Database", page: CtfPage, items: list[CtfTweet], index: "OccupantIndex", *,
                          observed_at: int, run_id: str) -> tuple[int, int]:
    """Write matched items (§2.9 row, §2.6 attribution); returns (posts inserted, mention rows inserted)."""
    from app.nlp.match import match_occupants
    if not items:
        return 0, 0
    hl = {t.author_handle.lower() for t in items if t.author_handle}
    ids = await _author_ids(db, hl)
    kols = await _kol_handles(db, hl)
    posts = mentions = 0
    for t in items:
        h = (t.author_handle or "").lower()
        fan = index.by_handle.get(h) or [page.occupant_key]
        occupant_key: str | None = page.occupant_key
        team_rows: list[str] = []
        if len(fan) > 1:                                         # shared account (§2.6): restricted rule match
            hits = match_occupants(t.text, index, restrict_to=fan)
            if len(hits) == 1:
                occupant_key = hits[0][0]
            else:
                occupant_key, team_rows = None, list(fan)
        exists = await db.fetchval("SELECT 1 FROM social_posts WHERE platform = 'x' AND post_id = ?", (t.post_id,),
                                   writer=True)
        await db.execute(UPSERT_SQL, (
            t.post_id, ids.get(h) or f"h:{h}", t.author_handle, occupant_key, int(h in kols), kols.get(h),
            t.text or None, _hash_text(t.text, t.post_id), f"https://x.com/{t.author_handle}/status/{t.post_id}",
            int(t.has_url), json.dumps(["team"]), t.event_at, observed_at, SOURCE, run_id, _raw_hash(t)))
        posts += int(not exists)
        for okey in team_rows:
            res = await db.execute(
                "INSERT OR IGNORE INTO social_post_mentions (platform, post_id, occupant_key, match_kind, event_at, "
                "observed_at, run_id) VALUES ('x', ?, ?, 'team', ?, ?, ?)",
                (t.post_id, okey, t.event_at, observed_at, run_id))
            mentions += max(0, res.rowcount)
    return posts, mentions


async def write_page_state(db: "Database", occupant_key: str, value: dict) -> None:
    await db.upsert("job_state", [{"job": JOB, "key": f"page:{occupant_key}", "updated_at": now_ts(),
                                   "value_json": json.dumps(value, separators=(",", ":"))}],
                    conflict=("job", "key"))


async def read_page_state(db: "Database", occupant_key: str) -> dict | None:
    v = await db.fetchval("SELECT value_json FROM job_state WHERE job = ? AND key = ?",
                          (JOB, f"page:{occupant_key}"), writer=True)
    try:
        return json.loads(v) if v else None
    except ValueError:
        return None


async def _emit_discrepancy(db: "Database", page: CtfPage, *, handle: str | None, ctf_handle: str | None,
                            ctf_last: int | None, provider_last: int | None, reason: str, now: int,
                            run_id: str) -> bool:
    from app.storage.events import emit_event
    gap = round(abs(ctf_last - provider_last) / DAY, 1) if ctf_last is not None and provider_last is not None \
        else None
    key_handle = (handle or ctf_handle or page.slug).lower()
    ev = await emit_event(
        db, event_type="x_ctf_discrepancy", occupant_key=page.occupant_key, netuid=page.netuid,
        event_at=ctf_last, observed_at=now,
        payload={"handle": handle, "ctf_last_post_at": ctf_last, "provider_last_post_at": provider_last,
                 "gap_days": gap, "reason": reason, "ctf_handle": ctf_handle, "slug": page.slug},
        dedupe_key=f"{key_handle}|{now // (7 * DAY)}", source="x", run_id=run_id)
    return ev is not None


async def _provider_last(db: "Database", handle_lower: str) -> int | None:
    ph = ",".join("?" * len(PAID_SOURCES))
    return await db.fetchval(
        f"SELECT MAX(event_at) FROM social_posts WHERE platform = 'x' AND source IN ({ph}) "
        f"AND lower(author_handle) = ?", (*PAID_SOURCES, handle_lower))


# ── run ──────────────────────────────────────────────────────────────────────────────────────────────────────
async def run_ctf_crosscheck(db: "Database", http: "Http", *, now: int, run_id: str, s=None,
                             dry_run: bool = False, netuids: set[int] | None = None) -> dict[str, Any]:
    from app.nlp.match import OccupantIndex
    s = s or app_config.settings
    pages = [p for p in await ctf_pages(db) if netuids is None or p.netuid in netuids]
    rep: dict[str, Any] = {"pages": len(pages), "with_feed": 0, "items": 0, "matched": 0, "rows": 0,
                           "mentions": 0, "failed": 0, "events": 0}
    if dry_run:
        rep["plan"] = [p.url() for p in pages]
        return rep
    index = await OccupantIndex.load(db)
    paid = bool(s.x_active())
    async for f in fetch_ctf_pages(http, pages):
        p = f.page
        observed = now_ts()
        prev = await read_page_state(db, p.occupant_key) or {}
        items = parse_ctf(f.html) if f.http_status == 200 else []
        own = {h.lower(): h for h in index.handles_of(p.occupant_key)}
        matched = [t for t in items if t.author_handle and t.author_handle.lower() in own]
        newest = max((t.event_at for t in matched), default=None)
        if f.http_status != 200:
            rep["failed"] += 1
            logger.warning("ctf_fetch_failed", data={"slug": p.slug, "status": f.http_status, "error": f.error})
        rep["items"] += len(items)
        rep["matched"] += len(matched)
        rep["with_feed"] += int(bool(items))
        if matched:
            n, m = await store_ctf_posts(db, p, matched, index, observed_at=observed, run_id=run_id)
            rep["rows"] += n
            rep["mentions"] += m
        feed_ok = f.http_status == 200 and bool(matched)
        await write_page_state(db, p.occupant_key, {
            "fetched_at": observed, "http_status": f.http_status, "n_items": len(items), "n_matched": len(matched),
            "newest_event_at": newest,
            "last_feed_at": observed if feed_ok else prev.get("last_feed_at")})   # extra: last fetch with a feed
        if items and not matched:
            authors = Counter(t.author_handle for t in items if t.author_handle)
            ctf_handle = authors.most_common(1)[0][0] if authors else None
            rep["events"] += int(await _emit_discrepancy(
                db, p, handle=next(iter(own.values()), None), ctf_handle=ctf_handle,
                ctf_last=max(t.event_at for t in items), provider_last=None, reason="handle_mismatch", now=observed,
                run_id=run_id))
        elif matched and paid:
            for hl, h in own.items():
                ctf_last = max((t.event_at for t in matched if (t.author_handle or "").lower() == hl), default=None)
                prov_last = await _provider_last(db, hl)
                if ctf_last is None or prov_last is None or abs(ctf_last - prov_last) <= GAP_DAYS * DAY:
                    continue
                rep["events"] += int(await _emit_discrepancy(
                    db, p, handle=h, ctf_handle=h, ctf_last=ctf_last, provider_last=prov_last, reason="gap",
                    now=observed, run_id=run_id))
    logger.info("x_ctf_crosscheck_run", data=rep)
    if pages and rep["failed"] == len(pages):
        raise ProviderError(PROVIDER, f"all {len(pages)} subnetalpha pages failed", status=None,
                            url=page_base(), attempts=len(pages))
    return rep


def run_note(rep: dict[str, Any]) -> str:
    return (f"pages={rep['pages']} with_feed={rep['with_feed']} items={rep['items']} matched={rep['matched']} "
            f"rows={rep['rows']} failed={rep['failed']}")


class XCtfCrosscheckJob(CollectorJob):
    """Free; runs with X inactive (``X_CTF_CROSSCHECK_ENABLED`` only, §2.5)."""
    _last: dict | None = None

    async def collect(self, run: RunContext) -> JobOutcome:
        rep = await run_ctf_crosscheck(run.rt.db, run.rt.http, now=run.started_at, run_id=run.run_id,
                                       s=run.settings)
        self._last = rep
        return JobOutcome(rows=rep["rows"], meta=rep, note=run_note(rep))

    def health_extra(self) -> dict | None:
        return dict(self._last) if self._last else None


# ── CLI ──────────────────────────────────────────────────────────────────────────────────────────────────────
async def _cli(args) -> int:
    import uuid

    from app.http import Http
    from app.ratelimit import ProviderLimiter
    from app.storage.db import Database
    if args.parse:                                     # offline: no DB, no HTTP
        with open(args.parse, encoding="utf-8", errors="replace") as fh:
            print(json.dumps([asdict(t) for t in parse_ctf(fh.read())], ensure_ascii=False))
        return 0
    s = app_config.settings
    db = Database(s.path(s.DB_PATH))
    await db.connect()
    await db.apply_schema()
    http = Http(ProviderLimiter.from_settings())
    try:
        rep = await run_ctf_crosscheck(db, http, now=now_ts(), run_id=f"cli-{uuid.uuid4().hex[:12]}",
                                       dry_run=args.dry_run, netuids=set(args.netuid) if args.netuid else None)
        print(json.dumps(rep, default=str, ensure_ascii=False))
        return 0
    finally:
        await http.aclose()
        await db.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m app.collectors.x_ctf")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="one cross-check pass outside the scheduler")
    mode.add_argument("--parse", metavar="HTML", help="parse a saved profile page and print the items")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--netuid", type=int, action="append")
    return asyncio.run(_cli(ap.parse_args(argv)))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
