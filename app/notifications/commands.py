"""Telegram command handlers and reply formats (spec 14 §8.4–§8.5, phone layout amended 2026-10-20).

``CommandDispatcher`` is what ``TelegramBot`` calls: it parses ``/command args`` (``shlex``), runs the handler and
replies. Every number comes from ``app/api/queries.py`` — the same query layer as the API and the exports — except the
wallet screens, which render the τ Terminal's payloads (``app/notifications/wallet.py``, 14 §8.4a).
Screens are built for a phone: ``<pre>`` lines stay ≤ 32 characters, lists that do not need columns are plain lines,
and every screen's keyboard leads back to 🏠 Home (``act:home``).
Two-step confirmations: pause/resume use stateless ``do:pause:<t>`` / ``do:resume:<t>`` callbacks; everything with
more arguments goes through an 8-character nonce held in memory for ``TELEGRAM_CONFIRM_TTL_SEC`` (single use).
"""
from __future__ import annotations

import secrets
import shlex
import statistics
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Awaitable, Callable

import app.config as app_config
from app.api import queries as Q
from app.logging.logger import logger
from app.notifications import wallet as W
from app.notifications.format import (bar, cells, chunk, cut, day_month, day_month_hm, dot2, esc, fmt_age, num,
                                      status_dot, tier_dot)
from app.notifications.telegram import (back_keyboard, confirm_keyboard, flags_keyboard, health_keyboard,
                                        main_menu_keyboard, movers_keyboard, sn_keyboard, top_keyboard, _toast)
from app.util.time import now_ts, parse_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.notifications.telegram import TelegramBot
    from app.runtime import Runtime

BOT_COMMANDS: tuple[tuple[str, str], ...] = (
    ("menu", "Home screen"), ("top", "Top subnets (e.g. /top 20 A)"), ("sn", "Subnet card (e.g. /sn 64)"),
    ("wallet", "Wallets: positions, PnL, insights (e.g. /wallet Main)"),
    ("flags", "Active red flags"), ("movers", "Biggest score changes (e.g. /movers 7)"),
    ("health", "Service health"), ("jobs", "Every job's last run"), ("spend", "Paid-provider spend"),
    ("review", "Open review items"), ("needsreview", "What needs a human"), ("digest", "Digest events since the last digest"),
    ("team", "Team: add|verify|reject|list|disclosure"), ("product", "Product status: set"),
    ("flag", "Manual flag: confirm|clear"), ("owner", "Owner: whitelist|compromise"),
    ("pause", "Pause a job or group"), ("resume", "Resume a job or group"), ("help", "Command list"),
)
HELP = "\n".join([
    "<b>📖 TaoSentinel commands</b>",
    "",
    "<b>Look</b>",
    "/menu — home screen with buttons",
    "/top [n] [tier] · /sn &lt;netuid&gt; · /flags · /movers [days]",
    "/wallet [name] — positions, PnL, performance, insights",
    "/health · /jobs · /spend · /review [kind] · /needsreview · /digest",
    "",
    "<b>Act</b> (all ask to confirm or need evidence)",
    '/team add &lt;netuid&gt; "&lt;name&gt;" "&lt;role&gt;" &lt;profile_url&gt;',
    "/team verify &lt;id&gt; &lt;url&gt; [url2] · /team reject &lt;id&gt; \"&lt;reason&gt;\"",
    "/team list &lt;netuid&gt; · /team disclosure &lt;netuid&gt; anonymous|clear &lt;url&gt;",
    "/product set &lt;netuid&gt; none|waitlist|beta|live &lt;url&gt;",
    "/flag confirm|clear &lt;netuid&gt; &lt;flag&gt; &lt;url&gt;",
    "/owner whitelist &lt;review_id&gt; &lt;reason&gt; &lt;url&gt; · /owner compromise &lt;netuid&gt; &lt;from&gt; &lt;to&gt; &lt;url&gt;",
    "/review resolve &lt;id&gt; confirm_flag|dismiss|done [url] [\"note\"]",
    "/pause &lt;target&gt; [hours] · /resume &lt;target&gt;",
])
_SEV_ICON = {"info": "ℹ️", "warning": "⚠️", "critical": "🛑"}
_PILLAR_NAMES = {"P1": "Chain", "P2": "Builder", "P3": "Product", "P4": "Team", "P5": "Community", "P6": "Narrative"}
FLAG_INFO = {   # icon, plain words (12 §2.8 flags)
    "dereg_imminent": ("⚰️", "in the deregistration zone"), "stale_identity": ("🪪", "identity looks abandoned"),
    "emission_disabled": ("🚫", "emissions switched off"), "miner_burn_zeroed": ("🔥", "miner burn zeroed"),
    "owner_extraction": ("💰", "owner extracting value"), "owner_control_change": ("🔑", "owner key changed hands"),
    "key_compromise": ("🔓", "owner key compromised"), "repo_gone": ("📦", "code repository gone"),
    "all_repos_archived": ("🗄", "every repository archived"), "site_down_7d": ("🌐", "website down for 7 days"),
    "silence_45d": ("🔇", "silent for 45 days"), "inflated_community": ("🎈", "community numbers look inflated"),
}
_NR_LABELS = {
    "registry.new_occupant": "new occupants", "registry.reoccupied_unreviewed": "re-occupied, unreviewed",
    "registry.source_disagreement_open": "source disagreements", "registry.abandonment_marker": "abandonment markers",
    "registry.name_turnover": "name turnover", "registry.identity_void": "identity void",
    "registry.chain_identity_missing": "chain identity missing", "registry.legacy_needs_review": "legacy to review",
    "registry.never_activated": "never activated", "registry.unresolved_watch_target": "unresolved watch targets",
    "registry.leaked_secret": "leaked secrets", "registry.error": "registry list FAILED",
    "queue": "open review items", "queue.error": "review queue FAILED", "team_candidates": "team candidates",
    "team_unknown": "team unknown", "product_unset": "product status unset", "blocked_sites": "sites blocked ≥ 7 d",
}
CONF_BAR = 0.4            # 02 §H1: rows below this confidence are insufficient_data and hidden by default


def ui() -> str:
    return app_config.settings.UI_BASE_URL.rstrip("/")


def spec() -> str:
    return app_config.settings.SPEC_VERSION


def hm(ts: int | None) -> str:
    return "-" if ts is None else to_iso(ts)[11:16] + "Z"


def ymd_hm(ts: int | None) -> str:
    return "-" if ts is None else to_iso(ts)[:10] + " " + to_iso(ts)[11:16] + "Z"


def link(url: str | None, label: str) -> str | None:
    if not url:
        return None
    return f'<a href="{esc(url)}">{esc(label)}</a>'


def sn_label(netuid: int, name: str | None, width: int = 18) -> str:
    return f"SN{netuid}" + (f" {esc(cut(name, width))}" if name and name != "?" else "")


# ── read formatters (pure over query rows; tested directly) ─────────────────────────────────────────────────
async def render_home(rt: "Runtime") -> str:
    from app.api.health import build_health
    from app.api.review import list_items
    db = rt.db
    h = await build_health(rt)
    st = h.get("status")
    lines = [f"🛰 <b>TaoSentinel</b> · {status_dot(st)} {esc(st)}",
             f"<i>{day_month_hm(now_ts())} · up {fmt_age(h.get('uptime_sec') or 0)} · {esc(spec())}</i>"]
    if h.get("degraded_reasons"):
        lines.append("⚠️ " + ", ".join(f"<code>{esc(r)}</code>" for r in h["degraded_reasons"]))
    meta = await Q.feed_meta(db, spec=spec())
    allr = await Q.latest_scores(db, spec=spec(), include_insufficient=True)
    ok = [r for r in allr if (r["confidence"] or 0) >= CONF_BAR]
    lines.append("")
    if meta.get("max_as_of") is None:
        lines.append("📊 <b>Scores</b>: none yet")
    else:
        lines.append(f"📊 <b>Scores</b> {day_month_hm(meta['max_as_of'])} · {esc(meta.get('run_kind'))}"
                     + (f" · {esc(meta['regime'])}" if meta.get("regime") not in (None, "UNKNOWN") else ""))
        if allr and not ok:
            lines.append(f"      ⚠️ low confidence: 0 of {len(allr)} pass {CONF_BAR}")
        else:
            tiers: dict[str, int] = {}
            for r in ok:
                tiers[r["tier"] or "-"] = tiers.get(r["tier"] or "-", 0) + 1
            lines.append("      " + " ".join(f"{tier_dot(t)}{t} {tiers[t]}" for t in "ABCDF" if t in tiers)
                         + f" · {len(ok)} subnets")
    flags = await Q.active_flags(db, spec=spec())
    reviews = await list_items(db, kind=None, netuid=None, limit=500)
    lines.append(f"🚩 <b>{len(flags)}</b> red flag{'s' if len(flags) != 1 else ''} · "
                 f"🗂 <b>{len(reviews)}</b> open review{'s' if len(reviews) != 1 else ''}")
    snap = rt.costs.snapshot() if getattr(rt, "costs", None) is not None else {}
    worst = max((float(v.get("pct") or 0) for v in snap.values()), default=0.0)
    paused = [g for g, v in snap.items() if v.get("paused")]
    lines.append(f"💸 spend {worst:.0%} of the fullest cap" + (f" · ⏸ {esc(', '.join(paused))}" if paused else ""))
    lines += ["", "<i>Pick a screen:</i>"]
    return "\n".join(lines)


def _low_conf_note(allr: list[dict]) -> str:
    confs = [r["confidence"] or 0 for r in allr]
    missing = [_PILLAR_NAMES[k] for k in _PILLAR_NAMES
               if sum(1 for r in allr if (r["pillar_scores"] or {}).get(k) is None) > len(allr) / 2]
    out = (f"⚠️ None of the {len(allr)} scored subnets reaches confidence {CONF_BAR} in this run "
           f"(median {dot2(statistics.median(confs))}).")
    if missing:
        out += f"\nMissing for most subnets: {esc(', '.join(missing))}."
    return out


async def render_top(db, n: int = 10, tier: str | None = None, *,
                     everything: bool = False) -> tuple[str, list[int], bool]:
    """Returns (text, netuids shown, offer "show anyway")."""
    n = max(1, min(int(n), 50))
    allr = await Q.latest_scores(db, spec=spec(), include_insufficient=True, tier=tier)
    rows = allr if everything else [r for r in allr if (r["confidence"] or 0) >= CONF_BAR]
    hidden = len(allr) - len(rows)
    tf = f" · tier {esc(tier)}" if tier else ""
    if not allr:
        return f"🏆 <b>Top subnets</b>{tf}\nNo scores for {esc(spec())} yet" + (f" (tier {esc(tier)})" if tier else "") + ".", [], False
    newest = max(allr, key=lambda r: r["as_of"])
    sub = f"<i>{esc(spec())} · {day_month_hm(newest['as_of'])} · {esc(newest['run_kind'])}" + \
          (f" · {esc(newest['regime'])}" if newest.get("regime") not in (None, "UNKNOWN") else "") + "</i>"
    if not rows:
        return "\n".join([f"🏆 <b>Top subnets</b>{tf}", sub, "", _low_conf_note(allr)]), [], True
    lines = [f"🏆 <b>Top {min(n, len(rows))} subnets</b>{tf}", sub, "<pre>",
             f"{'#':>2} {'SN':>3} {'name':<11} T {'comp':>5} conf"]
    for i, r in enumerate(rows[:n], 1):
        fl = "!" if r["red_flags"] else ""
        lines.append(f"{i:>2} {r['netuid']:>3} {cut(r['name'] or '?', 11):<11} {r['tier'] or '-'} "
                     f"{num(r['composite_smoothed'], '.1f', 5)} {dot2(r['confidence']):>3}{fl}")
    lines.append("</pre>")
    tiers: dict[str, int] = {}
    for r in rows:
        tiers[r["tier"] or "-"] = tiers.get(r["tier"] or "-", 0) + 1
    lines.append(" · ".join(f"{tier_dot(t)} {t} {tiers[t]}" for t in "ABCDF" if t in tiers))
    notes = ["! = red flag"]
    if everything:
        notes.append("incl. low-confidence rows")
    elif hidden:
        notes.append(f"{hidden} hidden (confidence &lt; {CONF_BAR})")
    lines.append("<i>" + " · ".join(notes) + "</i> · " + (link(ui() + "/matrix", "matrix") or ""))
    return "\n".join(lines), [r["netuid"] for r in rows[:n]], False


async def render_sn(db, netuid: int) -> str:
    res = await Q.resolve_occupant(db, netuid)
    if res is None:
        return f"SN{int(netuid)} has no current occupant."
    occ = res[0]
    info = await Q.occupant_info(db, occ)
    sub = (info or {}).get("sub") or {}
    row = await Q.latest_score_row(db, occupant_key=occ, spec=spec())
    name = (info or {}).get("name") or sub.get("name") or "?"
    status = (info or {}).get("status") or "?"
    lines = [f"🛰 <b>SN{netuid} · {esc(name)}</b>", f"<code>{esc(occ)}</code> · {esc(status)}"]
    if row is None:
        lines += ["", "No score yet · next official run 00:15Z"]
    else:
        ex = row["explain"] or {}
        if row["status"] == "pre_launch":
            lines.append("\n🌱 pre-launch (no composite)")
        else:
            lines.append(f"\n{tier_dot(row['tier'])} <b>{num(row['composite_smoothed'])}</b> composite · tier "
                         f"<b>{esc(row['tier'] or '—')}</b>" + (" · ⚠️ low confidence" if row["insufficient_data"] else ""))
        lines.append(f"<i>health {num(row['quality'])} · velocity {num(row['velocity'])} · conf "
                     f"{dot2(row['confidence'])} · {day_month_hm(row['as_of'])}</i>")
        ps = row["pillar_scores"] or {}
        lines.append("<pre>")
        for k in ("P1", "P2", "P3", "P4", "P5", "P6"):
            v = ps.get(k)
            lines.append(f"{_PILLAR_NAMES[k]:<10}{cells(v)} {dot2(v) if v is not None else 'n/a'}")
        lines.append("</pre>")
        pos = ex.get("top_positive") or []
        neg = ex.get("top_negative") or []
        if pos:
            lines.append("👍 " + " · ".join(f"{esc(d.get('metric'))} {num(d.get('impact'), '+.1f')}" for d in pos[:3]))
        if neg:
            lines.append("👎 " + " · ".join(f"{esc(d.get('metric'))} {num(d.get('impact'), '+.1f')}" for d in neg[:3]))
        flags = ex.get("flags") or []
        if flags:
            for f in flags:
                since = day_month(f.get("since")) if isinstance(f.get("since"), (int, float)) else "?"
                icon = FLAG_INFO.get(f.get("flag"), ("🚩",))[0]
                lines.append(f"{icon} <code>{esc(f.get('flag'))}</code> ×{num(f.get('multiplier'), '.2f')} since "
                             f"{since} ({esc(f.get('origin'))})")
        else:
            lines.append("✅ no red flags")
        evs = await Q.occupant_events(db, occupant_key=occ, limit=3)
        if evs:
            lines += ["", "<b>Recent events</b>", *[event_line(e) for e in evs]]
    links = [link(sub.get("website_url"), "web"), link(sub.get("github_url"), "github"),
             link(sub.get("discord_url"), "discord"),
             link(f"https://x.com/{sub['x_handle']}" if sub.get("x_handle") else None, "x"),
             link(f"{ui()}/subnet/{netuid}", "open")]
    lines += ["", "🔗 " + " · ".join(x for x in links if x)]
    return "\n".join(lines)


def event_summary(e: dict) -> str:
    p = e.get("payload") or {}
    if p.get("summary"):
        return str(p["summary"])
    et = e["event_type"]
    if et == "tier_change":
        return f"{p.get('from')}→{p.get('to')} {num(p.get('composite_smoothed'))}"
    if et in ("red_flag_raised", "red_flag_cleared"):
        return str(p.get("flag"))
    if et == "identity_changed":
        return str(p.get("field"))
    for k in ("reason", "flag", "field", "url", "collector"):
        if p.get(k):
            return str(p[k])
    return ""


def event_line(e: dict) -> str:
    ts = e.get("event_at") or e.get("observed_at")
    summ = event_summary(e)
    return (f"{_SEV_ICON.get(e['severity'], '•')} <i>{day_month(ts) if ts else '--'}</i> "
            f"<code>{esc(e['event_type'])}</code>" + (f" {esc(cut(summ, 40))}" if summ else ""))


async def render_events(db, netuid: int, limit: int = 10) -> str:
    res = await Q.resolve_occupant(db, netuid)
    if res is None:
        return f"SN{int(netuid)} has no current occupant."
    evs = await Q.occupant_events(db, occupant_key=res[0], limit=limit)
    if not evs:
        return f"📜 <b>SN{netuid} · events</b>\nNone yet."
    return "\n".join([f"📜 <b>SN{netuid} · last {len(evs)} events</b>", "", *[event_line(e) for e in evs], "",
                      link(f"{ui()}/events?netuid={netuid}", "all events") or ""])


async def render_flags(db) -> tuple[str, list[int]]:
    rows = await Q.active_flags(db, spec=spec())
    meta = await Q.feed_meta(db, spec=spec())
    head = [f"🚩 <b>Red flags · {len(rows)} active</b>", f"<i>as of {day_month_hm(meta.get('max_as_of'))}</i>"]
    if not rows:
        return "\n".join(head + ["", "✅ none"]), []
    by: dict[str, list[dict]] = {}
    for r in rows:
        by.setdefault(r["flag"], []).append(r)
    lines = head
    for flag in sorted(by, key=lambda f: (num(by[f][0]["multiplier"], ".2f"), f)):
        rs = by[flag]
        icon, words = FLAG_INFO.get(flag, ("🚩", ""))
        days = {day_month(r["raised_at"]) for r in rs if r["raised_at"]}
        since = f" · since {days.pop()}" if len(days) == 1 else ""
        lines += ["", f"{icon} <b>{esc(flag)}</b> ×{num(rs[0]['multiplier'], '.2f')} · {len(rs)}",
                  f"<i>{esc(words)}{since}</i>" if words or since else ""]
        items = []
        for r in rs:
            mark = (" ✋" if r["origin"] == "human" else " 📌" if r["origin"] == "sticky" else "") + \
                   (" (clearing)" if r["state"] == "clearing" else "")
            items.append(sn_label(r["netuid"], r["name"], 14) + mark)
        lines.append(" · ".join(items))
    lines += ["", "<i>✋ set by a human · 📌 sticky · ×multiplier on the composite</i>"]
    nets = list(dict.fromkeys(r["netuid"] for r in rows))
    return "\n".join(x for x in lines if x is not None), nets


async def render_movers(db, days: int = 7, n: int = 10) -> tuple[str, list[int]]:
    days = max(1, min(int(days), 90))
    up, down = await Q.movers(db, spec=spec(), days=days, n=n, now=now_ts())
    lines = [f"📈 <b>Movers · {days}d</b>", "<i>official composite, now vs then</i>"]
    if not up and not down:
        first = await Q.first_official_as_of(db, spec=spec())
        if first is None:
            lines += ["", "No official scores yet."]
        elif first > now_ts() - days * 86400:
            lines += ["", f"Not enough history: the first official score is from {day_month(first)}, "
                          f"less than {days} days ago. Try a shorter window."]
        else:
            lines += ["", "Nothing moved."]
        return "\n".join(lines), []
    lines.append("<pre>")
    for icon, rows in (("▲ up", up), ("▼ down", down)):
        if not rows:
            continue
        lines.append(f"{icon:<14} {'from':>5} {'to':>5} {'Δ':>5}")
        for r in rows:
            lines.append(f"{r['netuid']:>3} {cut(r['name'] or '?', 10):<10} {num(r['c_from'], '.1f', 5)} "
                         f"{num(r['c_to'], '.1f', 5)} {num(r['delta'], '+.1f', 5)}")
    lines.append("</pre>")
    return "\n".join(lines), [r["netuid"] for r in up[:5] + down[:5]]


def _job_state(st: dict, now: int) -> tuple[str, str, str]:
    """(mark, short state, age) for one job/task row of ``/health``."""
    last = st.get("last_run") or st.get("last_beat")
    try:
        age = fmt_age(now - parse_ts(last)) if last else "-"
    except (TypeError, ValueError):
        age = "-"
    fails = int(st.get("consecutive_failures") or 0)
    if st.get("paused"):
        return "‖", f"paused {st['paused']}", age
    if fails:
        return "✗", f"failed ×{fails}", age
    if st.get("stale"):
        return "!", "stale", age
    return ("✓" if last else "·"), "ok" if last else "not run yet", age


async def render_health(rt: "Runtime", *, all_jobs: bool = False) -> str:
    from app.api.health import build_health
    h = await build_health(rt)
    st = h.get("status")
    lines = [f"🩺 <b>Health</b> · {status_dot(st)} {esc(st)}",
             f"<i>up {fmt_age(h.get('uptime_sec') or 0)} · v{esc(h.get('version'))} · {esc(h.get('spec_version'))}</i>"]
    now = now_ts()
    jobs = {**(h.get("collectors") or {}), **(h.get("tasks") or {})}
    rows = []
    for jid in sorted(jobs):
        s = jobs[jid] if isinstance(jobs[jid], dict) else {}
        if s.get("enabled"):
            rows.append((jid, *_job_state(s, now)))
    if all_jobs:
        order = {"✗": 0, "!": 1, "‖": 2, "✓": 3, "·": 4}
        rows.sort(key=lambda r: (order.get(r[1], 9), r[0]))
        lines += ["", f"<b>All {len(rows)} enabled jobs</b>", "<pre>"]
        lines += [f"{mark} {cut(jid, 22):<22} {age:>6}" for jid, mark, _, age in rows]
        lines += ["</pre>", "<i>✓ ok · ✗ failing · ! stale · ‖ paused · · not run since boot</i>"]
        return "\n".join(lines)
    if h.get("degraded_reasons"):
        lines.append("⚠️ " + ", ".join(f"<code>{esc(r)}</code>" for r in h["degraded_reasons"]))
    sc = h.get("scores") if isinstance(h.get("scores"), dict) else {}
    db = h.get("db") if isinstance(h.get("db"), dict) else {}
    latest = (sc.get("latest_as_of") or "")[11:16]
    official = (sc.get("latest_official_as_of") or "")[5:16].replace("T", " ")
    lines.append("")
    lines.append(f"📊 scores {latest or '-'}Z" + (f" · official {official}Z" if official else "")
                 + (" · ⚠️ stale" if sc.get("stale") else ""))
    age = db.get("last_backup_age_hours")
    lines.append(f"💾 db {(db.get('size_bytes') or 0) / 2**30:.1f} GB · backup "
                 + (f"{age:.1f} h ago" if isinstance(age, (int, float)) else "none"))
    switches = [n for n, k in (("SCORING_KILLED", "scoring_killed"), ("KILL_SWITCH", "kill_switch_active"),
                               ("PAUSE_ALL", "pause_all_active")) if h.get(k)]
    lines.append("🛑 " + ", ".join(switches) + " ON" if switches else "🔌 kill switches off")
    counts = {"✓": 0, "✗": 0, "!": 0, "‖": 0, "·": 0}
    for _, mark, _, _ in rows:
        counts[mark] = counts.get(mark, 0) + 1
    extra = [f"{counts[m]} {w}" for m, w in (("·", "not run yet"), ("✗", "failing"), ("!", "stale"), ("‖", "paused"))
             if counts[m]]
    lines.append(f"⚙️ {len(rows)} jobs · {counts['✓']} ok" + "".join(f" · {x}" for x in extra))
    bad = [r for r in rows if r[1] in ("✗", "!", "‖")]
    if bad:
        lines += ["", "<b>Needs attention</b>", "<pre>"]
        lines += [f"{mark} {cut(jid, 20):<20} {cut(state, 9)}" for jid, mark, state, _ in bad]
        lines.append("</pre>")
    else:
        lines += ["", "✅ every job is healthy"]
    return "\n".join(lines)


async def render_spend(rt: "Runtime") -> str:
    snap = rt.costs.snapshot() if rt is not None and rt.costs is not None else {}
    month = next((v.get("month") for v in snap.values() if v.get("month")), None)
    try:
        mname = time.strftime("%B %Y", time.strptime(month, "%Y-%m")) if month else "this month"
    except ValueError:
        mname = month
    lines = [f"💸 <b>Spend · {esc(mname)}</b>", "<pre>"]
    off = []
    for g in sorted(snap):
        v = snap[g]
        frac = float(v.get("pct") or 0)
        if g == "tmc":
            cap = v.get("request_cap")
            used = f"{(v.get('requests') or 0) / 1000:.1f}k/{(cap or 0) / 1000:.0f}k"
        else:
            cap = v.get("cap_usd")
            if cap == 0:
                off.append(g)
                continue
            used = f"${v.get('usd', 0):.2f}/${cap:g}" if cap is not None else f"${v.get('usd', 0):.2f}"
        lines.append(f"{cut(g, 8):<8} {used:>11} {cells(frac, 6) if cap else '':<6} {f'{frac:.0%}' if cap else '':>4}")
        if v.get("paused") or v.get("paused_reason"):
            lines.append(f"{'':<8} ⏸ {esc(v.get('paused_reason') or 'paused')}")
    lines.append("</pre>")
    if off:
        lines.append(f"<i>off (cap 0): {esc(', '.join(off))}</i>")
    return "\n".join(lines)


async def render_review(db, kind: str | None = None) -> str:
    from app.api.review import list_items
    rows = await list_items(db, kind=kind, netuid=None, limit=200)
    if not rows:
        return "🗂 <b>Open reviews · 0</b>\n✅ nothing waiting"
    now = now_ts()
    by: dict[str, list[dict]] = {}
    for r in rows:
        by.setdefault(r["kind"], []).append(r)
    lines = [f"🗂 <b>Open reviews · {len(rows)}</b>"]
    for k in sorted(by, key=lambda k: (min(int(r.get("priority") or 9) for r in by[k]), k)):
        rs = by[k]
        oldest = max(now - int(r["created_at"]) for r in rs)
        pri = min(int(r.get("priority") or 9) for r in rs)
        lines += ["", f"<b>{esc(k)}</b> · {len(rs)} · priority {pri} · oldest {fmt_age(oldest)}"]
        items = [f"#{r['id']} SN{r['netuid']}" if r.get("netuid") is not None else f"#{r['id']}" for r in rs[:30]]
        lines.append(" · ".join(items) + (" · …" if len(rs) > 30 else ""))
    lines += ["", "<i>Resolve:</i> <code>/review resolve &lt;id&gt; dismiss|done|confirm_flag [url]</code>",
              "<i>Whitelist owner:</i> <code>/owner whitelist &lt;id&gt; &lt;reason&gt; &lt;url&gt;</code>"]
    return "\n".join(lines)


def _bucket_items(key: str, items: list) -> str:
    if key == "queue":
        counts: dict[str, int] = {}
        for it in items:
            counts[it["kind"]] = counts.get(it["kind"], 0) + 1
        return ", ".join(f"{k} {v}" for k, v in sorted(counts.items()))
    if key.endswith(".error"):
        return esc(str(items[0].get("detail", "?")))
    if key == "team_candidates":
        parts = [f"SN{it['netuid']} ×{it['n']}" for it in items]
    elif key == "blocked_sites":
        parts = [f"SN{it['netuid']} {it['reason']}" for it in items]
    else:                                   # one entry per netuid (04 can list several items for one subnet)
        counts: dict[str, int] = {}
        for it in items:
            n = it.get("netuid")
            label = f"SN{n}" if n is not None else esc(cut(it.get("detail") or "?", 20))
            counts[label] = counts.get(label, 0) + 1
        parts = [f"{k} ×{v}" if v > 1 else k for k, v in counts.items()]
    return ", ".join(parts[:12]) + (", …" if len(parts) > 12 else "")


async def render_needs_review(db, now: int | None = None) -> str:
    now = int(now if now is not None else now_ts())
    b = await Q.needs_review(db, now=now, blocked_days=int(app_config.settings.BLOCKED_SITE_DAYS))
    head = f"🧭 <b>Needs review · {day_month(now)}</b>"
    if not b:
        return head + "\n✅ nothing to review"
    lines = [head]
    for key, items in b.items():
        lines.append(f"• <b>{esc(_NR_LABELS.get(key, key))}</b> ({len(items)}): {_bucket_items(key, items)}")
    return "\n".join(lines)


async def render_digest_events(db, after_id: int, upto_id: int) -> str:
    from app.alerts.templates import render as render_alert
    rows = await Q.digest_events(db, after_id=after_id, upto_id=upto_id)
    if not rows:
        return "📬 <b>Digest events</b>\nnone since the last digest"
    by: dict[str, list[dict]] = {}
    for r in rows:
        by.setdefault(r["event_type"], []).append(r)
    lines = [f"📬 <b>Digest events ({len(rows)})</b>"]
    for et in sorted(by, key=lambda k: (-len(by[k]), k)):
        rs = by[et]
        live = [r for r in rs if not _backfilled(r)]
        hist = len(rs) - len(live)
        lines.append(f"\n<b>{esc(et)}</b> ×{len(rs)}" + (f" ({hist} history)" if hist else ""))
        for r in live[:3]:
            lines.append("  " + render_alert(r, link=False).split("\n")[0])
        if len(live) > 3:
            lines.append(f"  … +{len(live) - 3} more · {ui()}/events?type={esc(et)}")
    lines.append("\n" + (link(f"{ui()}/events", "all events") or ""))
    return "\n".join(lines)


def _backfilled(r: dict) -> bool:
    from app.api.common import jloads
    return bool((jloads(r.get("payload"), {}) or {}).get("backfilled"))


# ── dispatcher ─────────────────────────────────────────────────────────────────────────────────────────────────
@dataclass(slots=True)
class Pending:
    kind: str
    args: dict
    expires: float


USAGE = {
    "team": '/team add &lt;netuid&gt; "&lt;name&gt;" "&lt;role&gt;" &lt;profile_url&gt;\n'
            "/team verify &lt;id&gt; &lt;url&gt; [url2] · /team reject &lt;id&gt; \"&lt;reason&gt;\"\n"
            "/team list &lt;netuid&gt; · /team disclosure &lt;netuid&gt; anonymous|clear &lt;url&gt;",
    "product": "/product set &lt;netuid&gt; none|waitlist|beta|live &lt;url&gt;",
    "flag": "/flag confirm|clear &lt;netuid&gt; &lt;flag&gt; &lt;url&gt;",
    "owner": "/owner whitelist &lt;review_id&gt; &lt;reason&gt; &lt;url&gt; · "
             "/owner compromise &lt;netuid&gt; &lt;from&gt; &lt;to&gt; &lt;url&gt;",
    "review": "/review [kind] · /review resolve &lt;id&gt; confirm_flag|dismiss|done [url] [\"note\"]",
    "pause": "/pause &lt;target&gt; [hours] · /resume &lt;target&gt; (job id or group)",
}


class CommandDispatcher:
    def __init__(self, get_runtime: Callable[[], "Runtime | None"]) -> None:
        self._get_runtime = get_runtime
        self.pending: dict[str, Pending] = {}

    # helpers
    def rt(self) -> "Runtime":
        rt = self._get_runtime()
        if rt is None or rt.db is None:
            raise RuntimeError("runtime not ready")
        return rt

    def db(self):
        return self.rt().db

    def _nonce(self, kind: str, **args: Any) -> str:
        now = time.monotonic()
        for k in [k for k, p in self.pending.items() if p.expires < now]:
            self.pending.pop(k, None)
        n = secrets.token_hex(4)
        self.pending[n] = Pending(kind, args, now + int(app_config.settings.TELEGRAM_CONFIRM_TTL_SEC))
        return n

    @staticmethod
    def _result_line(res, ok: Callable[[dict], str]) -> str:
        d = res.to_dict() if hasattr(res, "to_dict") else dict(res)
        if d.get("ok"):
            d = {k: esc("" if v is None else v) for k, v in d.items()}
            return ok(d) + (f" · ⚠️ {d['warning']}" if d.get("warning") else "")
        code = d.get("error")
        if code == "two_source_rule":
            return f"❌ needs a second independent source ({esc(d.get('warning') or '')}) — add e.g. the official team page"
        return f"❌ {esc(code)}" + (f": {esc(d['warning'])}" if d.get("warning") else "")

    # entry points called by TelegramBot
    async def on_command(self, bot: "TelegramBot", chat_id: str, text: str) -> None:
        try:
            parts = shlex.split(text)
        except ValueError:
            await bot.send_message(chat_id, "Could not parse the command (unbalanced quotes?).")
            return
        if not parts:
            return
        cmd = parts[0].split("@", 1)[0].lower()
        args = parts[1:]
        logger.info("Telegram command received", data={"command": cmd})
        handler: Callable[..., Awaitable[None]] | None = {
            "/start": self.c_home, "/menu": self.c_home, "/home": self.c_home, "/help": self.c_help,
            "/top": self.c_top, "/sn": self.c_sn, "/subnet": self.c_sn, "/wallet": self.c_wallet,
            "/wallets": self.c_wallet, "/jobs": self.c_jobs,
            "/flags": self.c_flags, "/movers": self.c_movers, "/health": self.c_health, "/status": self.c_health,
            "/spend": self.c_spend, "/team": self.c_team, "/product": self.c_product, "/flag": self.c_flag,
            "/owner": self.c_owner, "/review": self.c_review, "/pause": self.c_pause, "/resume": self.c_resume,
            "/needsreview": self.c_needsreview, "/digest": self.c_digest,
        }.get(cmd)
        if handler is None:
            await bot.send_message(chat_id, "Unknown command. /help")
            return
        await handler(bot, chat_id, args)

    async def on_callback(self, bot: "TelegramBot", chat_id: str, message_id: int | None, callback_id: str,
                          data: str) -> None:
        await bot.answer_callback(callback_id)          # always first (old telegram.py comment)
        db = self.db()
        if data == "act:home":
            await bot.emit(chat_id, await render_home(self.rt()), main_menu_keyboard(), message_id)
        elif data in ("act:top", "act:top:all"):
            everything = data.endswith(":all")
            text, nets, offer = await render_top(db, everything=everything)
            await bot.emit(chat_id, text, top_keyboard(nets, everything=everything, offer_all=offer), message_id)
        elif data == "act:flags":
            text, nets = await render_flags(db)
            await bot.emit(chat_id, text, flags_keyboard(nets), message_id)
        elif data == "act:movers" or data.startswith("act:mv:"):
            days = int(data[7:]) if data.startswith("act:mv:") and data[7:].isdigit() else 7
            text, nets = await render_movers(db, days)
            await bot.emit(chat_id, text, movers_keyboard(days, nets), message_id)
        elif data in ("act:health", "act:jobs"):
            jobs = data == "act:jobs"
            await bot.emit(chat_id, await render_health(self.rt(), all_jobs=jobs), health_keyboard(all_jobs=jobs),
                           message_id)
        elif data == "act:spend":
            await bot.emit(chat_id, await render_spend(self.rt()), back_keyboard(("🔁 Refresh", "act:spend")),
                           message_id)
        elif data == "act:review":
            await bot.emit(chat_id, await render_review(db), back_keyboard(("🔁 Refresh", "act:review")), message_id)
        elif data == "act:needs":
            await bot.emit(chat_id, await render_needs_review(db), back_keyboard(("🔁 Refresh", "act:needs")),
                           message_id)
        elif data == "act:digest":
            await bot.emit(chat_id, await self._digest_text(), back_keyboard(), message_id)
        elif data.startswith("w:"):
            view, arg, ref = W.parse_callback(data)
            text, kb = await W.screen(db, view, arg, ref)
            await bot.emit(chat_id, text, kb, message_id)
        elif data.startswith("sn:"):
            n = int(data[3:])
            await bot.emit(chat_id, await render_sn(db, n), sn_keyboard(n), message_id)
        elif data.startswith("ev:"):
            n = int(data[3:])
            await bot.emit(chat_id, await render_events(db, n), sn_keyboard(n), message_id)
        elif data.startswith(("do:pause:", "do:resume:")):
            _, verb, target = data.split(":", 2)
            text = await self._do_pause(verb, target, hours=None)
            await bot.emit(chat_id, text, None, message_id)
            await bot.answer_callback(callback_id, _toast(text))
        elif data.startswith("do:x:"):
            p = self.pending.pop(data[5:], None)
            if p is None or p.expires < time.monotonic():
                await bot.emit(chat_id, "⌛ expired — re-issue the command", None, message_id)
                return
            text = await self._execute(p)
            await bot.emit(chat_id, text, None, message_id)
        elif data == "no:x":
            await bot.emit(chat_id, "✖ cancelled", None, message_id)

    async def _execute(self, p: Pending) -> str:
        db = self.db()
        a = p.args
        if p.kind == "flag":
            from app.api.manual_service import set_flag
            res = await set_flag(db, netuid=a["netuid"], flag=a["flag"], action=a["action"],
                                 evidence_url=a["url"], note=None, by="telegram")
            if a["action"] == "confirm":
                return self._result_line(res, lambda d: f"🚩 SN{a['netuid']} {esc(a['flag'])} CONFIRMED · takes effect at the "
                                                        f"next scoring run (≤ 6 h)")
            return self._result_line(res, lambda d: f"✅ SN{a['netuid']} {esc(a['flag'])} cleared (30-d suppression)")
        if p.kind == "owner_whitelist":
            from app.integrity import review as review11
            res = await review11.resolve_review(db, a["review_id"], "whitelist", by="telegram", note=None,
                                                evidence_url=a["url"], whitelist_reason=a["reason"])
            return self._result_line(res, lambda d: f"✅ review #{d['id']} whitelisted")
        if p.kind == "owner_compromise":
            from app.integrity import owner_admin
            res = await owner_admin.override_key_compromise(db, netuid=a["netuid"], window_start=a["start"],
                                                            window_end=a["end"], evidence_url=a["url"],
                                                            by="telegram")
            return self._result_line(res, lambda d: f"🔐 SN{a['netuid']} key compromise recorded · window whitelisted")
        if p.kind == "review_resolve":
            from app.integrity import review as review11
            res = await review11.resolve_review(db, a["review_id"], a["action"], by="telegram", note=a.get("note"),
                                                evidence_url=a.get("url"))
            return self._result_line(res, lambda d: f"✅ review #{a['review_id']} {esc(a['action'])} · {d['status']}")
        if p.kind == "pause":
            return await self._do_pause("pause", a["target"], hours=a.get("hours"))
        return "unknown action"

    async def _do_pause(self, verb: str, target: str, *, hours: float | None) -> str:
        from app import control
        db = self.db()
        try:
            if verb == "pause":
                r = await control.pause(target, by="telegram", reason="telegram", hours=hours, db=db)
                until = f" until {esc(r['expires_at'])}" if r.get("expires_at") else ""
                return f"⏸ {esc(target)} paused by telegram ({len(r['jobs'])} jobs){until}"
            r = await control.resume(target, by="telegram", db=db)
        except control.NotPausable:
            return f"❌ not_pausable: {esc(target)} (use ALERTS_ENABLED / TELEGRAM_COMMANDS_ENABLED)"
        except control.UnknownTarget:
            return f"❌ unknown target {esc(target)} · groups: {esc(', '.join(sorted(control.PAUSE_GROUPS)))}"
        if r.get("file_pause_active"):
            return f"⚠️ {esc(target)} still paused: delete {esc(r['file_pause_active'])}"
        if r.get("still_paused_by"):
            return f"⚠️ {esc(target)} row removed, but still paused by {esc(r['still_paused_by'])}"
        return f"▶️ {esc(target)} resumed" + ("" if r.get("removed_row") else " (was not paused)")

    # commands
    async def c_home(self, bot, chat_id, args) -> None:
        await bot.emit(chat_id, await render_home(self.rt()), main_menu_keyboard(), None)

    async def c_help(self, bot, chat_id, args) -> None:
        await bot.send_message(chat_id, HELP, reply_markup=back_keyboard())

    async def c_wallet(self, bot, chat_id, args) -> None:
        if args:
            text, kb = await W.screen(self.db(), "o", "", " ".join(args))
        else:
            text, kb = await W.screen(self.db(), "l", "", "")
        await bot.emit(chat_id, text, kb, None)

    async def c_jobs(self, bot, chat_id, args) -> None:
        await bot.emit(chat_id, await render_health(self.rt(), all_jobs=True), health_keyboard(all_jobs=True), None)

    async def c_top(self, bot, chat_id, args) -> None:
        n, tier = 10, None
        for a in args:
            if a.isdigit():
                n = int(a)
            elif a.upper() in ("A", "B", "C", "D", "F"):
                tier = a.upper()
            else:
                await bot.send_message(chat_id, "Usage: /top [n] [tier]")
                return
        text, nets, offer = await render_top(self.db(), n, tier)
        await bot.emit(chat_id, text, top_keyboard(nets, offer_all=offer), None)

    async def c_sn(self, bot, chat_id, args) -> None:
        if not args or not args[0].isdigit():
            await bot.send_message(chat_id, "Usage: /sn &lt;netuid&gt;")
            return
        n = int(args[0])
        await bot.emit(chat_id, await render_sn(self.db(), n), sn_keyboard(n), None)

    async def c_flags(self, bot, chat_id, args) -> None:
        text, nets = await render_flags(self.db())
        await bot.emit(chat_id, text, flags_keyboard(nets), None)

    async def c_movers(self, bot, chat_id, args) -> None:
        days = max(1, min(int(args[0]), 90)) if args and args[0].isdigit() else 7
        text, nets = await render_movers(self.db(), days)
        await bot.emit(chat_id, text, movers_keyboard(days, nets), None)

    async def c_health(self, bot, chat_id, args) -> None:
        await bot.emit(chat_id, await render_health(self.rt()), health_keyboard(), None)

    async def c_spend(self, bot, chat_id, args) -> None:
        await bot.emit(chat_id, await render_spend(self.rt()), back_keyboard(("🔁 Refresh", "act:spend")), None)

    async def c_needsreview(self, bot, chat_id, args) -> None:
        await bot.emit(chat_id, await render_needs_review(self.db()), back_keyboard(("🔁 Refresh", "act:needs")),
                       None)

    async def _digest_text(self) -> str:
        from app.notifications.digest import digest_watermark
        db = self.db()
        after = await digest_watermark(db)
        return await render_digest_events(db, after, await Q.digest_upto(db))

    async def c_digest(self, bot, chat_id, args) -> None:
        await bot.emit(chat_id, await self._digest_text(), back_keyboard(), None)

    async def c_team(self, bot, chat_id, args) -> None:
        from app.integrity import team
        db = self.db()
        sub = args[0].lower() if args else ""
        try:
            if sub == "add" and len(args) >= 3:
                netuid, name = int(args[1]), args[2]
                role = args[3] if len(args) > 3 else None
                profile = args[4] if len(args) > 4 else None
                res = await team.add_member(db, netuid=netuid, name=name, role=role, profile_url=profile,
                                            evidence_url=None, evidence_note=None, source="manual",
                                            created_by="telegram", source_ref="telegram")
                text = self._result_line(res, lambda d: f"✅ team_members #{d['id']} added for SN{netuid} "
                                                        f"({d['occupant_key']}) · {d['status']}"
                                                        + (f" · profile: {esc(profile)}" if profile else ""))
            elif sub == "verify" and len(args) >= 3:
                res = await team.verify_member(db, int(args[1]), evidence_urls=args[2:4], note=None, by="telegram")
                text = self._result_line(res, lambda d: f"✅ #{d['id']} verified")
            elif sub == "reject" and len(args) >= 3:
                res = await team.reject_member(db, int(args[1]), reason=args[2], evidence_url=None, by="telegram")
                text = self._result_line(res, lambda d: f"✅ #{d['id']} rejected")
            elif sub == "list" and len(args) >= 2:
                rows = await team.list_members(db, netuid=int(args[1]))
                if not rows:
                    text = f"SN{int(args[1])}: no team members"
                else:
                    lines = [f"<b>SN{int(args[1])} team ({len(rows)})</b>", "<pre>"]
                    for r in rows[:30]:
                        lines.append(f"{r['id']:>4} {esc(cut(r['name'], 22)):<22} {esc(cut(r.get('role') or '-', 14)):<14} "
                                     f"{r['status']:<9} {r.get('evidence_count', 0)}")
                    lines.append("</pre>")
                    text = "\n".join(lines)
            elif sub == "disclosure" and len(args) >= 4:
                res = await team.set_disclosure(db, netuid=int(args[1]), status=args[2], evidence_url=args[3],
                                                note=None, by="telegram")
                text = self._result_line(res, lambda d: f"✅ SN{int(args[1])} disclosure = {d['status']}")
            else:
                text = "Usage:\n" + USAGE["team"]
        except ValueError:
            text = "Usage:\n" + USAGE["team"]
        await bot.emit(chat_id, text, None, None)

    async def c_product(self, bot, chat_id, args) -> None:
        from app.api.manual_service import set_product
        if len(args) != 4 or args[0] != "set" or not args[1].isdigit():
            await bot.send_message(chat_id, "Usage: " + USAGE["product"])
            return
        status = None if args[2] in ("null", "clear") else args[2]
        res = await set_product(self.db(), netuid=int(args[1]), status=status, evidence_url=args[3], note=None,
                                by="telegram")
        prev = (res.extra or {}).get("previous") if res.ok else None
        await bot.send_message(chat_id, self._result_line(
            res, lambda d: f"✅ SN{int(args[1])} product_status = {d['status']} (was {esc(prev or 'unset')})"))

    async def c_flag(self, bot, chat_id, args) -> None:
        if len(args) != 4 or args[0] not in ("confirm", "clear") or not args[1].isdigit():
            await bot.send_message(chat_id, "Usage: " + USAGE["flag"])
            return
        n = self._nonce("flag", action=args[0], netuid=int(args[1]), flag=args[2], url=args[3])
        await bot.send_message(chat_id, f"{'🚩 Confirm' if args[0] == 'confirm' else '✅ Clear'} "
                                        f"<code>{esc(args[2])}</code> on SN{int(args[1])}?\nevidence: {esc(args[3])}",
                               reply_markup=confirm_keyboard(f"do:x:{n}"))

    async def c_owner(self, bot, chat_id, args) -> None:
        try:
            if len(args) == 4 and args[0] == "whitelist":
                n = self._nonce("owner_whitelist", review_id=int(args[1]), reason=args[2], url=args[3])
                q = f"Whitelist review #{int(args[1])} as <code>{esc(args[2])}</code>?"
            elif len(args) == 5 and args[0] == "compromise":
                n = self._nonce("owner_compromise", netuid=int(args[1]), start=parse_ts(args[2]),
                                end=parse_ts(args[3]), url=args[4])
                q = f"Record a key compromise on SN{int(args[1])} for {esc(args[2])} → {esc(args[3])}?"
            else:
                raise ValueError
        except (ValueError, TypeError):
            await bot.send_message(chat_id, "Usage: " + USAGE["owner"])
            return
        await bot.send_message(chat_id, q, reply_markup=confirm_keyboard(f"do:x:{n}"))

    async def c_review(self, bot, chat_id, args) -> None:
        if args and args[0] == "resolve":
            if len(args) < 3 or not args[1].isdigit() or args[2] not in ("confirm_flag", "dismiss", "done"):
                await bot.send_message(chat_id, "Usage: " + USAGE["review"])
                return
            rid, action = int(args[1]), args[2]
            url = next((a for a in args[3:] if a.startswith("http")), None)
            note = next((a for a in args[3:] if not a.startswith("http")), None)
            if action == "confirm_flag":
                n = self._nonce("review_resolve", review_id=rid, action=action, url=url, note=note)
                await bot.send_message(chat_id, f"Confirm the flag of review #{rid}?",
                                       reply_markup=confirm_keyboard(f"do:x:{n}"))
                return
            from app.integrity import review as review11
            res = await review11.resolve_review(self.db(), rid, action, by="telegram", note=note, evidence_url=url)
            await bot.send_message(chat_id, self._result_line(res, lambda d: f"✅ review #{rid} {esc(action)} · {d['status']}"))
            return
        await bot.emit(chat_id, await render_review(self.db(), args[0] if args else None),
                       back_keyboard(("🔁 Refresh", "act:review")), None)

    async def c_pause(self, bot, chat_id, args) -> None:
        await self._pause_cmd(bot, chat_id, args, "pause")

    async def c_resume(self, bot, chat_id, args) -> None:
        await self._pause_cmd(bot, chat_id, args, "resume")

    async def _pause_cmd(self, bot, chat_id, args, verb: str) -> None:
        from app import control
        if not args:
            await bot.send_message(chat_id, "Usage: " + USAGE["pause"] + "\ngroups: " +
                                   esc(", ".join(sorted(control.PAUSE_GROUPS))))
            return
        target = args[0]
        if target in control.NOT_PAUSABLE:
            await bot.send_message(chat_id, f"❌ not_pausable: {esc(target)}")
            return
        try:
            jobs = control.resolve_targets(target)
        except control.UnknownTarget:
            await bot.send_message(chat_id, f"❌ unknown target {esc(target)} · groups: "
                                            f"{esc(', '.join(sorted(control.PAUSE_GROUPS)))}")
            return
        hours = None
        if verb == "pause" and len(args) > 1:
            try:
                hours = float(args[1])
            except ValueError:
                await bot.send_message(chat_id, "Usage: " + USAGE["pause"])
                return
        data = f"do:{verb}:{target}"
        if hours is not None or len(data.encode()) > 64:
            data = f"do:x:{self._nonce('pause', target=target, hours=hours)}"
        await bot.send_message(chat_id, f"{'⏸ Pause' if verb == 'pause' else '▶️ Resume'} <code>{esc(target)}</code> "
                                        f"({len(jobs)} jobs)" + (f" for {hours:g} h" if hours else "") + "?",
                               reply_markup=confirm_keyboard(data))


def build_dispatcher(get_runtime: Callable[[], "Runtime | None"]) -> CommandDispatcher:
    return CommandDispatcher(get_runtime)


__all__ = ["BOT_COMMANDS", "CommandDispatcher", "build_dispatcher", "chunk"]
