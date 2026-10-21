"""Telegram public pages (spec 08 §2.7): ``t.me/{h}`` / ``t.me/+{hash}`` profile counts and ``t.me/s/{h}`` channel
previews → ``telegram_snapshots`` and ``announcements(platform='telegram')``. Keyless, stdlib ``html.parser`` only.

Fail-safe: a 200 page with none of the known anchors is a *layout change* (``parse_ok=0``, failure), never absence;
``not_found`` needs Telegram's own not-found markers. One snapshot per (occupant, handle, UTC day), first attempt wins.
Titles of user/bot accounts (a person's display name) are never stored.
"""
from __future__ import annotations

import html as htmlmod
import re
from collections import Counter
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.collectors import community_store as store
from app.http import ProviderError
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.util.time import floor_day, now_ts, parse_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.http import Http
    from app.storage.db import Database

COMMUNITY_KINDS = ("group", "channel", "private_group", "private_channel")
_NUM = r"\d[\d\s,.  ]*[KMB]?"
_MEMBERS = re.compile(rf"(?P<n>{_NUM})\s+members?(?:\s*,\s*(?P<o>{_NUM})\s+online)?", re.I)
_SUBS = re.compile(rf"(?P<n>{_NUM})\s+subscribers?", re.I)
_MONTHLY = re.compile(rf"(?P<n>{_NUM})\s+monthly\s+users?", re.I)
_CONTACT = re.compile(r"You can contact @\w+ right away", re.I)
_AT_HANDLE = re.compile(r"^@\w+$")
_DEPRECATED = re.compile(r"(?i)\b(deprecated|inactive|closed|archived)\b")
_NOT_FOUND_TITLE = re.compile(r"^(Telegram: (Contact|Join|View) @?\S*|Join group chat on Telegram|Telegram Messenger)$")
_URL = re.compile(r"https?://\S+")
_PUBLIC = re.compile(r"^[A-Za-z0-9_]{4,32}$")
_MEDIA = ("tgme_widget_message_photo_wrap", "tgme_widget_message_video_player", "tgme_widget_message_document_wrap",
          "tgme_widget_message_roundvideo", "tgme_widget_message_voice", "tgme_widget_message_sticker",
          "tgme_widget_message_video_wrap", "tgme_widget_message_poll")
_VOID = {"br", "img", "meta", "link", "input", "hr", "source", "wbr", "area", "base", "col", "embed", "param", "track"}


class TmeLayoutChanged(Exception):
    """Every 200 profile page of a run failed to parse."""


@dataclass(slots=True)
class TmeProfile:
    kind: str | None
    title: str | None
    members: int | None
    online: int | None
    deprecated_marker: int
    parse_ok: int
    error: str | None = None


@dataclass(slots=True)
class TmePost:
    post_no: int
    event_at: int
    text: str
    views: int | None
    has_attachment: int
    url_count: int


def parse_count(s: str | None) -> int | None:
    """'23 831' / '23\\u00a0831' → 23831 · '1.2K' → 1200 · '5.81M' → 5810000."""
    if not s:
        return None
    s = s.strip().replace(" ", " ").replace(" ", " ")
    m = re.fullmatch(r"(\d[\d ,.]*?)\s*([KMB])?", s, re.I)
    if not m:
        return None
    num, suf = m.group(1).strip(), (m.group(2) or "").upper()
    if suf:
        try:
            return int(round(float(num.replace(" ", "").replace(",", ".")) * {"K": 1e3, "M": 1e6, "B": 1e9}[suf]))
        except ValueError:
            return None
    digits = re.sub(r"[ ,.]", "", num)
    return int(digits) if digits.isdigit() else None


# ── a tiny class-aware HTML walker ───────────────────────────────────────────────────────────────────────────
class _Node:
    __slots__ = ("tag", "attrs", "classes", "text", "children", "parent")

    def __init__(self, tag: str, attrs: dict[str, str], parent: "_Node | None") -> None:
        self.tag = tag
        self.attrs = attrs
        self.classes = set((attrs.get("class") or "").split())
        self.text: list[str] = []
        self.children: list[_Node] = []
        self.parent = parent

    def full_text(self) -> str:
        return "".join(self.text)

    def iter(self):
        yield self
        for c in self.children:
            yield from c.iter()

    def find_all(self, cls: str) -> list["_Node"]:
        return [n for n in self.iter() if cls in n.classes]

    def find(self, cls: str) -> "_Node | None":
        for n in self.iter():
            if cls in n.classes:
                return n
        return None


class _Tree(HTMLParser):
    """Builds a light tree; every node accumulates the text of all its descendants (``<br>`` → newline)."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.root = _Node("#root", {}, None)
        self.cur = self.root
        self.meta: dict[str, str] = {}

    def handle_starttag(self, tag, attrs):
        a = {k: (v or "") for k, v in attrs}
        if tag == "meta":
            key = a.get("property") or a.get("name")
            if key:
                self.meta[key] = a.get("content", "")
            return
        if tag == "br":
            self._text("\n")
            return
        node = _Node(tag, a, self.cur)
        self.cur.children.append(node)
        if tag not in _VOID:
            self.cur = node

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in _VOID and tag != "meta" and self.cur.tag == tag:
            self.cur = self.cur.parent or self.root

    def handle_endtag(self, tag):
        n = self.cur
        while n is not None and n.tag != tag:
            n = n.parent
        if n is not None and n.parent is not None:
            self.cur = n.parent

    def _text(self, data: str) -> None:
        n = self.cur
        while n is not None:
            n.text.append(data)
            n = n.parent

    def handle_data(self, data):
        self._text(data)


def _tree(html: str) -> _Tree:
    t = _Tree()
    try:
        t.feed(html or "")
        t.close()
    except Exception:  # pragma: no cover - html.parser is lenient; never let a page crash the run
        pass
    return t


def _clean(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def parse_tme_profile(html: str, *, private: bool, status: int) -> TmeProfile:
    """§2.7 step 1 (anchors verified live 2026-10-17)."""
    if status == 404:
        return TmeProfile("not_found", None, None, None, 0, 1)
    if status != 200:
        return TmeProfile(None, None, None, None, 0, 0, f"http_{status}")
    t = _tree(html)
    title_node = t.root.find("tgme_page_title")
    og_title = _clean(t.meta.get("og:title"))
    og_desc = _clean(t.meta.get("og:description"))
    if title_node is None:
        if _NOT_FOUND_TITLE.match(og_title) or (t.root.find("tgme_page") or t.root.find("tgme_action_button_new")):
            return TmeProfile("not_found", None, None, None, 0, 1)
        return TmeProfile(None, None, None, None, 0, 0, "layout_changed")
    span = next((c for c in title_node.children if c.tag == "span"), None)
    title = _clean(span.full_text() if span else title_node.full_text()) or og_title or None
    extras = [_clean(n.full_text()) for n in t.root.find_all("tgme_page_extra")]
    additional = _clean((t.root.find("tgme_page_additional") or _Node("x", {}, None)).full_text())
    deprecated = int(bool(title and _DEPRECATED.search(title)))
    kind = members = online = None
    for txt in extras + [og_desc, og_title]:
        m = _MEMBERS.search(txt)
        if m:
            kind = "private_group" if private else "group"
            members, online = parse_count(m.group("n")), parse_count(m.group("o"))
            break
        m = _SUBS.search(txt)
        if m:
            kind = "private_channel" if private else "channel"
            members = parse_count(m.group("n"))
            break
        m = _MONTHLY.search(txt)
        if m:
            kind, members = "bot", parse_count(m.group("n"))
            break
    if kind is None and private:
        if re.search(r"invited to the channel", additional, re.I):
            kind = "private_channel"
        elif re.search(r"invited to the group", additional, re.I):
            kind = "private_group"
    if kind is None and (_CONTACT.search(og_desc) or any(_AT_HANDLE.match(x) for x in extras)
                         or re.search(r"you can contact", additional, re.I)):
        kind = "user_or_unknown"
    if kind is None:
        return TmeProfile(None, None, None, None, deprecated, 0, "no_count_anchor")
    if kind not in COMMUNITY_KINDS:
        title = None                                   # a person's / bot's display name is never stored
    return TmeProfile(kind, title, members, online, deprecated, 1)


def parse_tme_preview(html: str, handle: str) -> list[TmePost]:
    """§2.7 step 2: one TmePost per ``div.tgme_widget_message[data-post]`` (service messages skipped)."""
    t = _tree(html)
    posts: list[TmePost] = []
    for msg in t.root.find_all("tgme_widget_message"):
        dp = msg.attrs.get("data-post") or ""
        if "/" not in dp or "service_message" in msg.classes:
            continue
        try:
            post_no = int(dp.rsplit("/", 1)[1])
        except ValueError:
            continue
        date = msg.find("tgme_widget_message_date")
        when = None
        for n in (date.iter() if date else msg.iter()):
            if n.tag == "time" and n.attrs.get("datetime"):
                when = n.attrs["datetime"]
                break
        if not when:
            continue
        try:
            event_at = parse_iso(when)
        except ValueError:
            continue
        text_node = None
        for n in msg.find_all("tgme_widget_message_text"):
            p, inside_reply = n.parent, False
            while p is not None and p is not msg:
                if "tgme_widget_message_reply" in p.classes:
                    inside_reply = True
                    break
                p = p.parent
            if not inside_reply:
                text_node = n
                break
        text = htmlmod.unescape(text_node.full_text()).strip() if text_node else ""
        links = set(_URL.findall(text))
        if text_node is not None:
            links |= {n.attrs["href"] for n in text_node.iter() if n.tag == "a"
                      and (n.attrs.get("href") or "").startswith(("http://", "https://"))}
        views_node = msg.find("tgme_widget_message_views")
        has_media = int(any(msg.find(c) is not None for c in _MEDIA))
        posts.append(TmePost(post_no, event_at, text, parse_count(_clean(views_node.full_text())) if views_node
                             else None, has_media, len(links)))
    return posts


# ── job ──────────────────────────────────────────────────────────────────────────────────────────────────────
def _profile_url(base: str, handle: str) -> str:
    return f"{base.rstrip('/')}/{handle}"


async def _get(http: "Http", url: str) -> tuple[int | None, str, str | None]:
    try:
        r = await http.request("tme", "GET", url, headers={"Accept-Language": "en", "Accept": "text/html"},
                               ok_statuses=(200, 301, 302, 404), follow_redirects=False)
        return r.status_code, r.text, None
    except ProviderError as exc:
        return exc.status, "", f"provider_error: {str(exc)[:120]}"


async def run_telegram_pass(db: "Database", http: "Http", *, now: int, run_id: str, s=None, dry_run: bool = False,
                            only_handle: str | None = None) -> dict[str, Any]:
    from app.registry import active_occupants
    from app.registry.watch import watch_targets
    s = s or app_config.settings
    day = floor_day(now)
    occs = {o.occupant_key: o for o in await active_occupants(db, include_pre_launch=True) if o.netuid != 0}
    by_handle: dict[str, list] = {}
    from app.collectors.discord_topology import legacy_only
    for w in await watch_targets(db, "telegram"):
        if legacy_only(w.source, w.meta):
            continue
        if w.occupant_key in occs and (only_handle is None or w.handle == only_handle):
            by_handle.setdefault(w.handle, []).append(w)
    done = await store.telegram_rows_today(db, day)
    due = {h: ws for h, ws in by_handle.items() if any((w.occupant_key, h) not in done for w in ws)}
    rep: dict[str, Any] = {"handles": len(by_handle), "due": len(due), "day": day}
    if dry_run:
        rep["plan"] = sorted(due)
        return rep
    snap_rows: list[dict] = []
    kinds: Counter = Counter()
    pages_200 = parse_fail_200 = transport_fail = posts_new = 0
    for h in sorted(due):
        private = h.startswith("+")
        status, body, err = await _get(http, _profile_url(s.TELEGRAM_BASE_URL, h))
        observed = now_ts()
        if status in (200, 404):
            prof = parse_tme_profile(body, private=private, status=status)
        else:
            prof = TmeProfile(None, None, None, None, 0, 0, err or f"http_{status}")
            transport_fail += 1
        if status == 200:
            pages_200 += 1
            parse_fail_200 += int(not prof.parse_ok)
        preview_available = preview_posts = last_post_at = None
        posts: list[TmePost] = []
        if prof.kind == "channel" and not private and s.TELEGRAM_PREVIEW_ENABLED and _PUBLIC.match(h):
            ps, pbody, perr = await _get(http, f"{s.TELEGRAM_BASE_URL.rstrip('/')}/s/{h}")
            if ps in (301, 302):
                preview_available, preview_posts = 0, 0
            elif ps == 200:
                posts = parse_tme_preview(pbody, h)
                preview_available, preview_posts = int(bool(posts)), len(posts)
                last_post_at = max((p.event_at for p in posts), default=None)
            else:
                logger.warning("telegram_preview_failed", data={"handle": h, "status": ps, "error": perr})
        kinds[prof.kind or "failed"] += 1
        for w in due[h]:
            if (w.occupant_key, h) in done:
                continue
            ratio = prof.online / prof.members if prof.online is not None and prof.members else None
            snap_rows.append({
                "occupant_key": w.occupant_key, "netuid": w.netuid, "handle": h, "observed_at": observed,
                "event_at": None, "cycle_day_ts": day, "source": "tme_page", "http_status": status,
                "kind": prof.kind, "title": prof.title if prof.kind in COMMUNITY_KINDS else None,
                "members": prof.members, "online": prof.online, "online_ratio": ratio,
                "deprecated_marker": prof.deprecated_marker, "preview_available": preview_available,
                "preview_posts": preview_posts, "last_post_at": last_post_at, "parse_ok": prof.parse_ok,
                "error": prof.error, "run_id": run_id})
        if posts:
            posts_new += await _store_posts(db, h, posts, [w.occupant_key for w in due[h]],
                                            {w.occupant_key: w.netuid for w in due[h]}, observed, run_id, s)
    written = 0
    for r in snap_rows:
        written += await store.insert_telegram_snapshot(db, r)
        if r["parse_ok"] == 1 and r["kind"] in COMMUNITY_KINDS:
            await db.execute("UPDATE watch_channels SET verified_at = ? WHERE occupant_key = ? AND platform = "
                             "'telegram' AND handle = ?", (r["observed_at"], r["occupant_key"], r["handle"]))
    n_events = 0
    if s.COMMUNITY_EVENTS_ENABLED:
        from app.collectors.community_events import emit_tg_venue_changes
        n_events = await emit_tg_venue_changes(db, snap_rows, now, run_id, int(s.COMMUNITY_VENUE_CHANGE_DEBOUNCE))
    rep.update(rows=written, posts=posts_new, events=n_events, kinds=dict(kinds), pages_200=pages_200,
               parse_failures=parse_fail_200, transport_failures=transport_fail)
    logger.info("telegram_counts_run", data=rep)
    if pages_200 > 0 and parse_fail_200 == pages_200:
        raise TmeLayoutChanged(f"all {pages_200} t.me pages unparseable")
    if due and transport_fail == len(due):
        raise ProviderError("tme", f"all {len(due)} handles failed", status=None, url=s.TELEGRAM_BASE_URL,
                            attempts=len(due))
    return rep


async def _store_posts(db: "Database", handle: str, posts: list[TmePost], occupant_keys: list[str],
                       netuids: dict[str, int], observed: int, run_id: str, s) -> int:
    from app.collectors.discord_gateway import AnnouncementRow
    hl = handle.lower()
    n = 0
    for p in posts:
        msg_id = f"telegram:{hl}/{p.post_no}"
        th = store.post_hash(p.text, msg_id)
        row = AnnouncementRow(
            msg_id=msg_id, platform="telegram", occupant_keys=list(occupant_keys),
            netuid=netuids.get(occupant_keys[0]) if len(occupant_keys) == 1 else None, source_guild_id=None,
            source_channel_id=hl, message_id=str(p.post_no), local_channel_id=None, local_message_id=None,
            via_own_server=0, mapping="tg_handle", author_is_team=1, has_attachment=p.has_attachment,
            url_count=p.url_count, text=p.text or None, text_hash=th, text_len=len(p.text), event_at=p.event_at,
            observed_at=observed, meta={"views": p.views} if p.views is not None else {}, run_id=run_id)
        n += int(await store.upsert_announcement(db, row, store_text=bool(s.TELEGRAM_POSTS_STORE_TEXT),
                                                 pending_days=int(s.LLM_PENDING_TEXT_MAX_DAYS), run_id=run_id))
    return n


class TelegramCountsJob(CollectorJob):
    _last: dict | None = None

    async def collect(self, run: RunContext) -> JobOutcome:
        rep = await run_telegram_pass(run.rt.db, run.rt.http, now=run.started_at, run_id=run.run_id, s=run.settings)
        self._last = rep
        return JobOutcome(rows=rep.get("rows", 0), meta=rep,
                          note=f"handles={rep['handles']} due={rep['due']} rows={rep.get('rows')} "
                               f"posts={rep.get('posts')}")

    def health_extra(self) -> dict | None:
        return dict(self._last) if self._last else None

