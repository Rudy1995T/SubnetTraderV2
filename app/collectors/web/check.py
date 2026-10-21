"""``web_check`` orchestration (spec 07 §2.2–§2.6, §3.2, §3.5, §7).

Targets → due filter → one fetch per distinct URL (concurrency ``WEB_CONCURRENCY``, ``WEB_SAME_HOST_GAP_S`` per
registrable domain) → one ``web_checks`` row per (occupant, url, kind) via ``INSERT OR IGNORE`` → per-check events
through ``emit_event``. The network gate holds rows back in batches of 10; an in-run retry follows a first
timeout/connection error. Nothing here raises for a single URL.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import Counter, OrderedDict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable
from urllib.parse import urlsplit

import httpx

from app.collectors.web import classify as C
from app.collectors.web import content as T
from app.collectors.web.fetch import FetchResult, TlsInfo, DnsError, fetch_chain, inspect_tls, network_preflight, \
    resolve_dns
from app.collectors.web.robots import RobotsCache
from app.collectors.web.wayback import WaybackClient, wayback_target
from app.logging.logger import logger
from app.util.time import floor_day, now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.config import Settings
    from app.http import Http
    from app.ratelimit import ProviderLimiter
    from app.storage.db import Database

RAW_HASH_BYTES = 1_000_000
BATCH = 10
GATE_MIN_FAILING = 5
GATE_SHARE = 0.6
DEADLINE_MARGIN_S = 120
RETRY_BUDGET_S = 60            # a retry must be able to finish before the soft deadline

_ROW_COLS = ("occupant_key", "netuid", "url", "kind", "is_primary", "source", "event_at", "observed_at", "check_day_ts",
             "run_id", "attempts", "status_class", "http_status", "final_url", "final_host", "redirect_hops",
             "redirect_chain", "cross_domain", "resolved_ip", "latency_ms", "robots_allowed", "tls_days_left", "tls_ok",
             "tls_hostname_match", "tls_issuer", "tls_not_after", "content_type", "extractor", "is_spa", "content_hash",
             "content_len", "content_text", "change_ratio", "parked", "parked_signals", "hijack_signals",
             "docs_candidate_url", "wayback_first_capture", "wayback_checked_at", "error")


# ── targets (§2.2) ───────────────────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class WatchTarget:
    id: int
    occupant_key: str
    netuid: int
    kind: str                  # website | docs
    url: str                   # watch_channels.handle (04's canonical URL)
    source: str
    priority: int
    is_resolved: bool
    first_seen_at: int
    is_primary: int            # 1 primary website and every docs row; 0 second website


def valid_url(url: str) -> bool:
    try:
        p = urlsplit(url)
    except ValueError:
        return False
    h = (p.hostname or "").lower()
    if p.scheme not in ("http", "https") or not h or "." not in h:
        return False
    return not (h == "localhost" or h.endswith(".localhost") or C.is_ip_literal(h))


def _strip_www(url: str) -> str:
    p = urlsplit(url)
    h = (p.hostname or "").lower()
    return (h[4:] if h.startswith("www.") else h) + (p.path or "")


def _order_key(r: dict) -> tuple:
    return (int(r["priority"]), int(r["first_seen_at"]), int(r["id"]))


def select_occupant_targets(rows: list[dict], excluded: list[str], max_websites: int
                            ) -> tuple[list[WatchTarget], list[str], list[str]]:
    """(targets, excluded website URLs, invalid URLs) for one occupant's active website/docs rows (§2.2 steps 1–3)."""
    out: list[WatchTarget] = []
    excl: list[str] = []
    invalid: list[str] = []
    webs: list[dict] = []
    for r in rows:
        url = r["handle"]
        if not valid_url(url):
            invalid.append(url)
            continue
        host = (urlsplit(url).hostname or "").lower()
        if C.is_excluded_host(host, excluded):
            if r["platform"] == "website":
                excl.append(url)
            continue
        if r["platform"] == "website":
            webs.append(r)
        else:
            out.append(_target(r, "docs", 1))
    if webs:
        resolved = [r for r in webs if r["is_resolved"]]
        primary = min(resolved or webs, key=_order_key)
        out.insert(0, _target(primary, "website", 1))
        rest = sorted((r for r in webs if r is not primary and _strip_www(r["handle"]) != _strip_www(primary["handle"])),
                      key=_order_key)
        for r in rest[:max(0, max_websites - 1)]:
            out.insert(1, _target(r, "website", 0))
    return out, excl, invalid


def _target(r: dict, kind: str, primary: int) -> WatchTarget:
    return WatchTarget(int(r["id"]), r["occupant_key"], int(r["netuid"]), kind, r["handle"], r["source"],
                       int(r["priority"]), bool(r["is_resolved"]), int(r["first_seen_at"]), primary)


TARGET_SQL = (
    "SELECT w.id, w.occupant_key, w.netuid, w.platform, w.handle, w.url, w.source, w.priority, w.is_resolved, "
    "w.first_seen_at FROM watch_channels w JOIN occupants o USING (occupant_key) "
    "WHERE w.platform IN ('website','docs') AND w.retired_at IS NULL AND o.status IN ('active','pre_launch') "
    "AND o.netuid != 0 ORDER BY w.occupant_key, w.id")


async def load_targets(db: "Database", s: "Settings", *, netuids: set[int] | None = None,
                       url: str | None = None) -> tuple[list[WatchTarget], dict[str, Any]]:
    rows = await db.fetchall(TARGET_SQL)
    by: dict[str, list[dict]] = OrderedDict()
    for r in rows:
        if netuids is not None and int(r["netuid"]) not in netuids:
            continue
        by.setdefault(r["occupant_key"], []).append(r)
    targets: list[WatchTarget] = []
    info = {"occupants": len(by), "excluded": 0, "invalid": 0, "no_website": 0}
    for _, rs in by.items():
        ts, excl, inv = select_occupant_targets(rs, list(s.WEB_EXCLUDED_HOSTS), int(s.WEB_MAX_WEBSITES_PER_OCCUPANT))
        info["excluded"] += len(excl)
        info["invalid"] += len(inv)
        for u in inv:
            logger.info("web.invalid_url", data={"occupant_key": rs[0]["occupant_key"], "url": u})
        if not any(t.kind == "website" for t in ts):
            info["no_website"] += 1
        targets += ts
    if url is not None:
        targets = [t for t in targets if t.url == url]
    return targets, info


async def due_targets(db: "Database", targets: list[WatchTarget], now: int, s: "Settings", *, force: bool = False
                      ) -> list[tuple[WatchTarget, int | None]]:
    """(target, last event_at) for due targets, oldest first (§2.2 step 5: no row today and none within the
    min interval)."""
    today = floor_day(now)
    last: dict[tuple[str, str, str], tuple[int, int]] = {}
    for r in await db.fetchall("SELECT occupant_key, url, kind, MAX(event_at) AS ev, MAX(check_day_ts) AS day "
                               "FROM web_checks GROUP BY occupant_key, url, kind"):
        last[(r["occupant_key"], r["url"], r["kind"])] = (int(r["ev"]), int(r["day"]))
    min_iv = float(s.WEB_PER_URL_MIN_INTERVAL_H) * 3600
    out = []
    for t in targets:
        ev, day = last.get((t.occupant_key, t.url, t.kind), (None, None))
        if not force and ev is not None and (day == today or ev > now - min_iv):
            continue
        out.append((t, ev))
    out.sort(key=lambda x: (x[1] is not None, x[1] or 0))
    return out


# ── one observation of one URL (shared by every target with that URL) ────────────────────────────────────────
@dataclass
class UrlObs:
    url: str
    event_at: int
    attempts: int = 1
    fr: FetchResult | None = None
    tls: TlsInfo | None = None
    robots_allowed: int | None = None
    crawl_delay: float | None = None
    head_only: bool = False
    info: T.PageInfo | None = None
    norm: str | None = None
    extractor: str | None = None
    is_spa: int | None = None
    content_hash: str | None = None
    content_len: int | None = None
    bot_challenge: bool = False
    parked_signals: list[str] | None = None
    internal: str | None = None
    skipped: str | None = None

    @property
    def soft_fail(self) -> bool:
        """A first timeout / connection error: candidate for the in-run retry and the network gate."""
        return self.internal is None and self.fr is not None and self.fr.exc_kind in ("timeout", "conn", "protocol")


@dataclass
class CheckContext:
    db: "Database"
    http: "Http"
    limiter: "ProviderLimiter | None"
    s: "Settings"
    run_id: str
    started_at: int
    deadline: float                       # unix s
    robots: RobotsCache
    wayback: WaybackClient
    heur: C.WebHeuristicsConfig
    state: dict                           # job_state values (wayback_backoff_until, wayback_last_ok)
    dry_run: bool = False
    costs: Any = None                     # 01's CostGovernor (TLS handshakes → provider_spend.requests, G5)
    clock: Callable[[], float] = time.time
    handshakes: int = 0
    wb_cache: dict[str, tuple[int | None, str]] = field(default_factory=dict)
    occ_websites: dict[str, list[str]] = field(default_factory=dict)


async def observe(ctx: CheckContext, url: str, *, attempt: int = 1, event_at: int | None = None) -> UrlObs:
    s = ctx.s
    o = UrlObs(url=url, event_at=int(event_at if event_at is not None else ctx.clock()), attempts=attempt)
    p = urlsplit(url)
    host = (p.hostname or "").lower()
    port = p.port or (443 if p.scheme == "https" else 80)
    try:
        if ctx.limiter is not None and ctx.limiter.host_cooldown("web", host) > 0:
            o.skipped = "cooldown"
            return o
        allowed, delay = True, None
        if s.WEB_ROBOTS_ENABLED:
            allowed, delay = await ctx.robots.allowed(url)
        o.robots_allowed = 1 if allowed else 0
        o.crawl_delay = delay
        o.head_only = not allowed
        method = "GET" if allowed else "HEAD"
        try:
            ip = await resolve_dns(host, port, float(s.WEB_DNS_TIMEOUT_S))
        except DnsError as exc:
            o.fr = FetchResult(url=url, method=method, exc_kind="timeout" if exc.kind == "temporary" else "dns",
                               error=("private_address: " if exc.kind == "private" else "dns: ") + str(exc)[:200],
                               final_host=host)
            return o
        except (TimeoutError, asyncio.TimeoutError):
            o.fr = FetchResult(url=url, method=method, exc_kind="timeout", error="dns: timeout", final_host=host)
            return o
        fr = await fetch_chain(ctx.http, url, method=method, max_redirects=int(s.WEB_MAX_REDIRECTS),
                               max_body=int(s.WEB_MAX_BODY_BYTES),
                               timeout=httpx.Timeout(float(s.WEB_TIMEOUT_S), connect=float(s.WEB_CONNECT_TIMEOUT_S)))
        fr.resolved_ip = ip
        o.fr = fr
        o.tls = await inspect_tls(ctx.limiter, _tls_host(url, fr), 443, float(s.WEB_TIMEOUT_S), now=int(ctx.clock()),
                                  warn_days=int(s.WEB_TLS_WARN_DAYS))
        ctx.handshakes += o.tls.handshakes
        html = fr.text if fr.body is not None and fr.status is not None else None
        if fr.status in (403, 429, 503):
            o.bot_challenge = C.is_bot_challenge(fr.status, fr.headers, html)
        if fr.status is not None and 200 <= fr.status < 300 and method == "GET" and fr.body is not None:
            if fr.is_html and html is not None:
                await asyncio.to_thread(_analyse_html, o, html, fr.final_url or url, ctx.heur)
            else:
                o.extractor = "raw"
                o.content_hash = T.content_hash(fr.body[:RAW_HASH_BYTES])
                o.content_len = len(fr.body)
    except Exception as exc:
        o.internal = f"internal:{type(exc).__name__}: {str(exc)[:200]}"
        logger.warning("web.internal_error", data={"url": url, "error": o.internal})
    return o


def _tls_host(url: str, fr: FetchResult) -> str:
    p = urlsplit(url)
    if p.scheme == "https":
        return (p.hostname or "").lower()
    for u in [h.location for h in fr.chain if h.location] + ([fr.final_url] if fr.final_url else []):
        q = urlsplit(u)
        if q.scheme == "https" and q.hostname:
            return q.hostname.lower()
    return (p.hostname or "").lower()


def _analyse_html(o: UrlObs, html: str, base: str, heur: C.WebHeuristicsConfig) -> None:
    info = T.page_info(html, base)
    o.info = info
    plain = T.plain_text(html)
    text, ext = T.extract_text(html, base)
    norm = T.normalise(T.with_head(text, info.title, info.description))
    o.is_spa = 1 if C.detect_spa(html, len(plain), heur.tiny_body_chars) else 0
    if norm:
        o.norm, o.extractor = norm, ext
        o.content_hash, o.content_len = T.content_hash(norm), len(norm)
    else:
        o.extractor, o.content_len = "none", 0
    o.parked_signals = C.parked_signals(norm, info.title, o.fr.final_host if o.fr else None,
                                        [u for u, _ in info.links], info.asset_hosts, bool(o.is_spa),
                                        o.content_len or 0, heur)


# ── row assembly (§3.2) ──────────────────────────────────────────────────────────────────────────────────────
@dataclass
class Prev:
    last: dict | None
    text: dict | None
    same_domain_up: bool
    wayback: dict | None


async def load_prev(db: "Database", t: WatchTarget) -> Prev:
    key = (t.occupant_key, t.url, t.kind)
    last = await db.fetchone("SELECT status_class, error, content_hash, resolved_ip, tls_issuer, cross_domain, parked "
                             "FROM web_checks WHERE occupant_key = ? AND url = ? AND kind = ? "
                             "ORDER BY event_at DESC, id DESC LIMIT 1", key)
    text = await db.fetchone("SELECT content_hash, content_text, content_len FROM web_checks WHERE occupant_key = ? "
                             "AND url = ? AND kind = ? AND content_text IS NOT NULL ORDER BY event_at DESC, id DESC "
                             "LIMIT 1", key)
    up = await db.fetchval("SELECT 1 FROM web_checks WHERE occupant_key = ? AND url = ? AND kind = ? AND cross_domain = 0 "
                           "AND status_class IN ('up_2xx','redirect_ok') LIMIT 1", key)
    wb = None
    if t.kind == "website":
        wb = await db.fetchone("SELECT wayback_first_capture, wayback_checked_at FROM web_checks WHERE occupant_key = ? "
                               "AND url = ? AND kind = 'website' AND (wayback_first_capture IS NOT NULL OR "
                               "wayback_checked_at IS NOT NULL) ORDER BY (wayback_first_capture IS NULL), event_at DESC "
                               "LIMIT 1", (t.occupant_key, t.url))
    return Prev(last, text, bool(up), wb)


def tls_row(o: UrlObs, url: str, warn_days: int) -> dict:
    tls = o.tls
    if tls is None:
        return {"tls_ok": None, "tls_days_left": None, "tls_not_after": None, "tls_issuer": None,
                "tls_hostname_match": None}
    if tls.reason == "no_answer":
        http_answered = o.fr is not None and o.fr.status is not None
        ok = 0 if (urlsplit(url).scheme == "http" and http_answered) else None
    elif tls.verified is None or tls.days_left is None:
        ok = 0 if tls.reason in ("tls_fail", "untrusted") else None
    else:
        ok = 1 if (tls.verified and tls.days_left > warn_days) else 0
    return {"tls_ok": ok, "tls_days_left": tls.days_left, "tls_not_after": tls.not_after, "tls_issuer": tls.issuer,
            "tls_hostname_match": None if tls.hostname_match is None else int(tls.hostname_match)}


async def _wayback_fields(ctx: CheckContext, t: WatchTarget, prev: Prev) -> tuple[int | None, int | None]:
    s = ctx.s
    wb = prev.wayback or {}
    cap, checked = wb.get("wayback_first_capture"), wb.get("wayback_checked_at")
    if cap is not None:
        return int(cap), checked
    now = int(ctx.clock())
    if checked is not None and now - int(checked) < int(s.WEB_WAYBACK_RECHECK_DAYS) * 86400:
        return None, int(checked)
    if not s.WEB_WAYBACK_ENABLED or now < int(ctx.state.get("wayback_backoff_until") or 0):
        return None, checked
    target = wayback_target(t.url, list(s.WEB_SHARED_PLATFORM_HOSTS))
    res = ctx.wb_cache.get(target)
    if res is None:
        res = await ctx.wayback.first_capture(target)
        ctx.wb_cache[target] = res
        if res[1] == "backoff":
            until = now + int(ctx.wayback.retry_after or 3600)
            ctx.state["wayback_backoff_until"] = until
            logger.warning("wayback_backoff", data={"until": until, "target": target})
        elif res[1] in ("found", "none"):
            ctx.state["wayback_last_ok"] = now
    ts, st = res
    if st == "found":
        return ts, now
    if st == "none":
        return None, now
    return None, checked


async def build_row(ctx: CheckContext, t: WatchTarget, o: UrlObs, prev: Prev) -> tuple[dict, list[dict]]:
    s = ctx.s
    fr = o.fr or FetchResult(url=t.url)
    declared_host = (urlsplit(t.url).hostname or "").lower()
    final_host = fr.final_host
    final_rd = C.registrable_domain(final_host or "")
    cross = bool(final_host) and final_rd != C.registrable_domain(declared_host)
    final_is_parking = final_rd in set(ctx.heur.parking_hosts)
    prev_text = prev.text or {}
    ratio: float | None = None
    text_out: str | None = None
    if o.norm is not None:
        if not prev_text:
            text_out = o.norm[: int(s.WEB_STORE_TEXT_MAX_CHARS)]
        elif prev_text.get("content_hash") == o.content_hash:
            ratio = 1.0
        else:
            ratio = await asyncio.to_thread(T.change_ratio, prev_text.get("content_text") or "", o.norm)
            text_out = o.norm[: int(s.WEB_STORE_TEXT_MAX_CHARS)]
    elif o.extractor == "raw" and prev.last and prev.last.get("content_hash") == o.content_hash:
        ratio = 1.0
    last = prev.last or {}
    hijack = C.hijack_signals(o.norm or "", prev_text.get("content_text"), cross_domain=cross,
                              final_is_parking=final_is_parking, prev_same_domain_up=prev.same_domain_up,
                              change_ratio=ratio, resolved_ip=fr.resolved_ip,
                              tls_issuer=o.tls.issuer if o.tls else None, prev_ip=last.get("resolved_ip"),
                              prev_issuer=last.get("tls_issuer"), cfg=ctx.heur) if o.fr is not None else []
    psig = o.parked_signals or []
    parked = C.score(psig, C.PARKED_WEIGHTS) >= 3 or final_is_parking
    if o.internal:
        status_class, error = "conn_error", o.internal
    else:
        status_class = C.classify(fr, parked=parked, hijack_score=C.score(hijack, C.HIJACK_WEIGHTS),
                                  bot_challenge=o.bot_challenge, head_only=o.head_only)
        error = fr.error
    now = int(ctx.clock())
    row: dict[str, Any] = {
        "occupant_key": t.occupant_key, "netuid": t.netuid, "url": t.url, "kind": t.kind, "is_primary": t.is_primary,
        "source": t.source, "event_at": o.event_at, "observed_at": now, "check_day_ts": floor_day(o.event_at),
        "run_id": ctx.run_id, "attempts": o.attempts, "status_class": status_class, "http_status": fr.status,
        "final_url": fr.final_url, "final_host": final_host, "redirect_hops": fr.hops,
        "redirect_chain": json.dumps([{"url": h.url, "status": h.status, "location": h.location} for h in fr.chain])
        if o.fr is not None else None,
        "cross_domain": int(cross), "resolved_ip": fr.resolved_ip, "latency_ms": fr.latency_ms,
        "robots_allowed": o.robots_allowed, **tls_row(o, t.url, int(s.WEB_TLS_WARN_DAYS)),
        "content_type": fr.content_type, "extractor": o.extractor, "is_spa": o.is_spa,
        "content_hash": o.content_hash, "content_len": o.content_len, "content_text": text_out,
        "change_ratio": ratio, "parked": int(status_class == "parked"),
        "parked_signals": json.dumps(psig) if o.parked_signals is not None else None,
        "hijack_signals": json.dumps(hijack) if o.fr is not None else None,
        "docs_candidate_url": None, "wayback_first_capture": None, "wayback_checked_at": None, "error": error,
    }
    if t.kind == "website" and t.is_primary:
        row["wayback_first_capture"], row["wayback_checked_at"] = await _wayback_fields(ctx, t, prev)
        if status_class in C.UP_CLASSES and o.info is not None and o.extractor not in (None, "none"):
            row["docs_candidate_url"] = T.find_docs_candidate(
                o.info.links, landing_urls=[t.url, fr.final_url or t.url],
                skip_urls=ctx.occ_websites.get(t.occupant_key, []),
                excluded_hosts=list(s.WEB_EXCLUDED_HOSTS))
    return row, check_events(t, row, prev, int(s.WEB_TLS_WARN_DAYS), float(s.WEB_CONTENT_RESET_RATIO))


def check_events(t: WatchTarget, row: dict, prev: Prev, warn_days: int, reset_ratio: float) -> list[dict]:
    """Per-check events of §3.5 (website rows only)."""
    if t.kind != "website":
        return []
    ev: list[dict] = []
    occ, url, sc = t.occupant_key, t.url, row["status_class"]
    day = row["event_at"] // 86400
    last_sc = (prev.last or {}).get("status_class")

    def add(event_type: str, payload: dict, dedupe: str) -> None:
        ev.append({"event_type": event_type, "occupant_key": occ, "netuid": t.netuid, "event_at": row["event_at"],
                   "payload": payload, "dedupe_key": dedupe})

    dl = row["tls_days_left"]
    if t.is_primary and dl is not None and dl <= warn_days:
        add("tls_expiring_14d", {"url": url, "tls_days_left": dl, "tls_not_after": row["tls_not_after"],
                                 "tls_issuer": row["tls_issuer"], "expired": dl <= 0},
            f"{occ}|{url}|{row['tls_not_after']}")
    if sc == "parked" and last_sc != "parked":
        add("site_parked", {"url": url, "final_url": row["final_url"],
                            "parked_signals": json.loads(row["parked_signals"] or "[]")}, f"{occ}|{url}|{day}")
    if sc == "hijacked_suspect" and last_sc != "hijacked_suspect":
        add("site_hijack_suspect", {"url": url, "final_url": row["final_url"],
                                    "hijack_signals": json.loads(row["hijack_signals"] or "[]"),
                                    "change_ratio": row["change_ratio"]}, f"{occ}|{url}|{day}")
    pt = prev.text or {}
    cr = row["change_ratio"]
    if cr is not None and cr < reset_ratio and (pt.get("content_len") or 0) >= 300 and sc in C.UP_CLASSES:
        add("site_content_reset", {"url": url, "prev_hash": pt.get("content_hash"), "new_hash": row["content_hash"],
                                   "prev_len": pt.get("content_len"), "new_len": row["content_len"],
                                   "change_ratio": cr}, f"{occ}|{url}|{row['content_hash']}")
    if row["cross_domain"] and sc in C.UP_CLASSES:
        fd = C.registrable_domain(row["final_host"] or "")
        add("site_cross_domain_redirect", {"url": url, "final_url": row["final_url"], "final_domain": fd},
            f"{occ}|{url}|{fd}")
    return ev


# ── docs-candidate lifecycle (§2.3 step 10) ──────────────────────────────────────────────────────────────────
async def docs_lifecycle(db: "Database", t: WatchTarget, cand: str | None, now: int, retire_days: int) -> dict:
    rows = await db.fetchall("SELECT id, handle, source, retired_at, last_seen_at FROM watch_channels "
                             "WHERE occupant_key = ? AND platform = 'docs'", (t.occupant_key,))
    declared = [r for r in rows if r["source"] != "web_autodiscover" and r["retired_at"] is None]
    auto = [r for r in rows if r["source"] == "web_autodiscover" and r["retired_at"] is None]
    out = {"inserted": 0, "retired": 0}

    async def retire(ids: list[int]) -> None:
        for i in ids:
            await db.execute("UPDATE watch_channels SET retired_at = ? WHERE id = ? AND source = 'web_autodiscover' "
                             "AND retired_at IS NULL", (now, i))
            out["retired"] += 1

    if declared:
        await retire([r["id"] for r in auto])
        return out
    if cand:
        res = await db.execute(
            "INSERT OR IGNORE INTO watch_channels (occupant_key, netuid, platform, handle, url, source, priority, "
            "is_resolved, verified_at, first_seen_at, last_seen_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (t.occupant_key, t.netuid, "docs", cand, cand, "web_autodiscover", 90, 0, None, now, now))
        out["inserted"] = max(0, res.rowcount)
        await db.execute("UPDATE watch_channels SET last_seen_at = ?, retired_at = NULL WHERE occupant_key = ? AND "
                         "platform = 'docs' AND handle = ? AND source = 'web_autodiscover'", (now, t.occupant_key, cand))
        await retire([r["id"] for r in auto if r["handle"] != cand])
    else:
        cutoff = now - retire_days * 86400
        await retire([r["id"] for r in auto if int(r["last_seen_at"]) < cutoff])
    return out


# ── the run ──────────────────────────────────────────────────────────────────────────────────────────────────
@dataclass
class CheckReport:
    urls_total: int = 0
    urls_due: int = 0
    urls_checked: int = 0
    rows: int = 0
    skipped_duplicate: int = 0
    skipped_cooldown: int = 0
    left: int = 0
    retried: int = 0
    events: int = 0
    internal_errors: int = 0
    network_suspect: bool = False
    status_counts: Counter = field(default_factory=Counter)
    docs: Counter = field(default_factory=Counter)
    targets: dict = field(default_factory=dict)
    printed: list[dict] = field(default_factory=list)

    def summary(self) -> dict:
        return {"urls_total": self.urls_total, "urls_due": self.urls_due, "urls_checked": self.urls_checked,
                "rows": self.rows, "skipped_duplicate": self.skipped_duplicate,
                "skipped_cooldown": self.skipped_cooldown, "left": self.left, "retried": self.retried,
                "events": self.events, "internal_errors": self.internal_errors,
                "network_suspect": self.network_suspect, "status_counts": dict(self.status_counts),
                "docs_autodiscover": dict(self.docs), "targets": self.targets}


async def _insert_row(db: "Database", row: dict) -> bool:
    cols = ", ".join(_ROW_COLS)
    res = await db.execute(f"INSERT OR IGNORE INTO web_checks ({cols}) VALUES ({','.join('?' * len(_ROW_COLS))})",
                           tuple(row[c] for c in _ROW_COLS))
    return res.rowcount > 0


async def run_check(ctx: CheckContext, *, netuids: set[int] | None = None, url: str | None = None,
                    force: bool = False, on_row: Callable[[dict], None] | None = None) -> CheckReport:
    from app.storage.events import emit_event
    s, db = ctx.s, ctx.db
    rep = CheckReport()
    targets, info = await load_targets(db, s, netuids=netuids, url=url)
    rep.targets = info
    for t in targets:
        if t.kind == "website":
            ctx.occ_websites.setdefault(t.occupant_key, []).append(t.url)
    rep.urls_total = len({t.url for t in targets})
    due = await due_targets(db, targets, ctx.started_at, s, force=force)
    groups: "OrderedDict[str, list[WatchTarget]]" = OrderedDict()
    for t, _ in due:
        groups.setdefault(t.url, []).append(t)
    rep.urls_due = len(groups)
    if not groups:
        return rep
    probe_timeout = float(s.WEB_CONNECT_TIMEOUT_S)
    if not await network_preflight(list(s.WEB_NETWORK_PROBE_HOSTS), probe_timeout):
        rep.network_suspect = True
        rep.left = len(groups)
        logger.warning("web.network_suspect", data={"stage": "preflight", "urls_due": rep.urls_due})
        return rep

    sem = asyncio.Semaphore(int(s.WEB_CONCURRENCY))
    host_locks: dict[str, asyncio.Lock] = {}
    host_next: dict[str, float] = {}
    gap = float(s.WEB_SAME_HOST_GAP_S)
    stop = asyncio.Event()

    async def guarded(u: str, attempt: int = 1, event_at: int | None = None) -> UrlObs | None:
        rd = C.registrable_domain(urlsplit(u).hostname or "")
        lock = host_locks.setdefault(rd, asyncio.Lock())
        async with sem:
            if stop.is_set() or ctx.clock() >= ctx.deadline:
                return None
            async with lock:
                wait = host_next.get(rd, 0.0) - time.monotonic()
                if wait > 0:
                    await asyncio.sleep(wait)
                if stop.is_set() or ctx.clock() >= ctx.deadline:
                    return None
                o = await observe(ctx, u, attempt=attempt, event_at=event_at)
                host_next[rd] = time.monotonic() + max(gap, o.crawl_delay or 0.0)
                return o

    async def write(o: UrlObs) -> None:
        rep.urls_checked += 1
        if o.internal:
            rep.internal_errors += 1
        for t in groups[o.url]:
            prev = await load_prev(db, t)
            row, events = await build_row(ctx, t, o, prev)
            rep.status_counts[row["status_class"]] += 1
            if on_row is not None:
                on_row(row)
            if ctx.dry_run:
                continue
            if not await _insert_row(db, row):
                rep.skipped_duplicate += 1
                continue
            rep.rows += 1
            for e in events:
                try:
                    eid = await emit_event(db, severity=None, observed_at=row["observed_at"], source="web",
                                           run_id=ctx.run_id, **e)
                    rep.events += 1 if eid else 0
                except Exception as exc:
                    logger.warning("web.event_failed", data={"event_type": e["event_type"], "error": str(exc)})
            if t.kind == "website" and t.is_primary and s.WEB_DOCS_AUTODISCOVER_ENABLED and \
                    row["status_class"] in C.UP_CLASSES and o.info is not None and o.extractor not in (None, "none"):
                d = await docs_lifecycle(db, t, row["docs_candidate_url"], row["observed_at"],
                                         int(s.REGISTRY_WATCH_RETIRE_DAYS))
                rep.docs.update(d)

    retry: list[UrlObs] = []

    async def flush(batch: list[UrlObs]) -> None:
        failing = [o for o in batch if o.soft_fail]
        if len(failing) >= GATE_MIN_FAILING and len(failing) >= GATE_SHARE * len(batch):
            if not await network_preflight(list(s.WEB_NETWORK_PROBE_HOSTS), probe_timeout):
                rep.network_suspect = True
                stop.set()
                logger.warning("web.network_suspect", data={"stage": "batch", "failing": len(failing),
                                                            "batch": len(batch)})
                for o in batch:
                    if not o.soft_fail:
                        await write(o)
                return                                  # failing rows are not written: they stay due
        for o in batch:
            if o.soft_fail and int(s.WEB_RETRY_DELAY_S) > 0:
                retry.append(o)
            else:
                await write(o)

    tasks = [asyncio.create_task(guarded(u)) for u in groups]
    batch: list[UrlObs] = []
    try:
        for fut in asyncio.as_completed(tasks):
            o = await fut
            if o is None:
                continue
            if o.skipped:
                rep.skipped_cooldown += 1
                continue
            batch.append(o)
            if len(batch) >= BATCH:
                await flush(batch)
                batch = []
        if batch:
            await flush(batch)
    finally:
        for tk in tasks:
            tk.cancel()

    if retry:
        delay = int(s.WEB_RETRY_DELAY_S)
        if rep.network_suspect:
            retry.clear()
        late = [o for o in retry if o.event_at + delay + RETRY_BUDGET_S > ctx.deadline]
        late_ids = {id(o) for o in late}
        for o in late:
            await write(o)

        async def again(o: UrlObs) -> UrlObs:
            wait = o.event_at + delay - ctx.clock()
            if wait > 0:
                await asyncio.sleep(wait)
            n = await guarded(o.url, attempt=2, event_at=o.event_at)
            return n if (n is not None and not n.skipped) else o

        todo = [o for o in retry if id(o) not in late_ids]
        rep.retried = len(todo)
        for o in await asyncio.gather(*(again(o) for o in todo)):
            await write(o)
    rep.left = len(groups) - rep.urls_checked - rep.skipped_cooldown
    if ctx.handshakes and ctx.costs is not None and not ctx.dry_run:
        try:
            await ctx.costs.record("web", 0.0, requests=ctx.handshakes)
        except Exception as exc:  # pragma: no cover
            logger.warning("web.cost_record_failed", data={"error": str(exc)})
    return rep
