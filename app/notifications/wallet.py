"""Telegram wallet view (spec 14 §8.4a): holdings, positions, PnL, performance, insights and trades of the wallets the
τ Terminal tracks.

TaoSentinel never reads a wallet itself (19 §1): every number here comes from the terminal's read API on loopback
(``TERMINAL_API_URL``: ``/api/wallets``, ``/api/positions``, ``/api/insights``, ``/api/trades``). Nothing is stored
or logged beyond the wallet name. The only join done here is TaoSentinel's own latest tier and active red flags beside
each held subnet. τ figures are balances, PnL or ratios — never sums over recycled capital (turnover).
"""
from __future__ import annotations

import re
import time
from typing import Any

import app.config as app_config
from app.api import queries as Q
from app.notifications.format import (PRE_WIDTH, cells, cut, day_month, day_month_hm, dot2, esc, pct, spark, tao,
                                      tier_dot)
from app.notifications.telegram import (back_keyboard, pager_keyboard, performance_keyboard, position_keyboard,
                                        positions_keyboard, wallet_keyboard, wallet_ref, wallets_keyboard)

DUST_TAO = 0.01                  # positions worth less than this (if sold now) are listed as dust
PERIODS = {1: "day", 7: "week", 30: "month", 365: "year"}
INSIGHTS_PER_PAGE = 6
TRADES_PER_PAGE = 15
_SCORE_CARDS = {"momentum", "best_skill", "work_on"}    # already on the performance screen
_KIND = {"buy": "buy", "sell": "sell", "move": "move", "transfer_in": "in", "transfer_out": "out",
         "liquidation": "liq", "collateral": "coll"}


class TerminalUnavailable(Exception):
    """The terminal's API is off (empty ``TERMINAL_API_URL``) or did not answer."""


async def terminal_get(path: str, **params: Any) -> Any:
    base = (app_config.settings.TERMINAL_API_URL or "").rstrip("/")
    if not base:
        raise TerminalUnavailable("off")
    from app.http import get_http
    try:
        resp = await get_http().request("terminal", "GET", base + path, params=params or None, timeout=30,
                                        retries=0, ok_statuses=(200,))
        return resp.json()
    except Exception as exc:                        # ProviderError, connect errors, bad JSON
        raise TerminalUnavailable(type(exc).__name__) from exc


def unavailable_text(exc: TerminalUnavailable) -> str:
    if str(exc) == "off":
        return "👛 <b>Wallets</b>\nThe wallet view is off (<code>TERMINAL_API_URL</code> is empty)."
    base = esc(app_config.settings.TERMINAL_API_URL)
    return ("👛 <b>Wallets</b>\n⚠️ The τ Terminal did not answer at " + base + ".\n"
            "<i>Start it with</i> <code>systemctl --user start tao-terminal</code>")


# ── helpers ────────────────────────────────────────────────────────────────────────────────────────────────────
def short_key(ss58: str | None) -> str:
    s = ss58 or ""
    return s if len(s) <= 12 else f"{s[:6]}…{s[-4:]}"


def month_year(ts: int | None) -> str:
    return "-" if not ts else time.strftime("%b %Y", time.gmtime(int(ts)))


def date_long(ts: int | None) -> str:
    return "-" if not ts else time.strftime("%d %b %Y", time.gmtime(int(ts))).lstrip("0")


def usd(v: float | None) -> str:
    return "" if v is None else f"${v:,.0f}".replace("$-", "−$")


def wallet_icon(w: dict) -> str:
    return "🔑" if w.get("local") else "👁"


def wallet_total(w: dict) -> float | None:
    if w.get("free") is None and w.get("staked_tao") is None:
        return None
    return float(w.get("free") or 0) + float(w.get("staked_tao") or 0)


def find_wallet(wallets: list[dict], sel: str) -> dict | None:
    """A wallet by name (case-insensitive) or coldkey; ``all`` → a synthetic combined entry."""
    if sel.lower() == "all":
        return combined(wallets)
    for w in wallets:
        if sel == w.get("coldkey") or sel.lower() == str(w.get("name", "")).lower():
            return w
    return None


def combined(wallets: list[dict]) -> dict:
    firsts = [w["first_event"] for w in wallets if w.get("first_event")]
    return {"name": "All wallets", "coldkey": None, "all": True, "local": None,
            "free": sum(float(w.get("free") or 0) for w in wallets),
            "staked_tao": sum(float(w.get("staked_tao") or 0) for w in wallets),
            "events": sum(int(w.get("events") or 0) for w in wallets), "first_event": min(firsts) if firsts else None,
            "sync": None}


def ref_of(w: dict) -> str:
    return "all" if w.get("all") else wallet_ref(w)


def api_sel(w: dict) -> str:
    return "all" if w.get("all") else str(w.get("name") or w.get("coldkey"))


def title(w: dict) -> str:
    return "👥 All wallets" if w.get("all") else f"{wallet_icon(w)} {esc(w.get('name'))}"


def wname(w: dict) -> str:
    return "All wallets" if w.get("all") else esc(w.get("name"))


_NEG = re.compile(r"(?<![\w.])-(?=\d)")


def minus(text: object) -> str:
    """The terminal's card text with real minus signs, escaped."""
    return _NEG.sub("−", esc(text))


def subnet_name(p: dict) -> str:
    if p.get("netuid") == 0:
        return "Root"
    return str(p.get("name") or "?")


def pos_value(p: dict) -> float:
    v = p.get("exit_value")
    return float(v if v is not None else (p.get("value_spot") or 0.0))


def pos_return(p: dict) -> float | None:
    cost = p.get("cost") or 0
    u = p.get("unrealized_exit")
    if u is None:
        u = p.get("unrealized_spot")
    return None if not cost or u is None else float(u) / float(cost)


def split_held(pos: dict) -> tuple[list[dict], list[dict]]:
    held = [p for p in pos.get("positions") or [] if (p.get("held") or 0) > 0 and p.get("occupant") == "current"]
    held.sort(key=lambda p: -pos_value(p))
    return [p for p in held if pos_value(p) >= DUST_TAO], [p for p in held if pos_value(p) < DUST_TAO]


async def sentinel_context(db, netuids: list[int]) -> tuple[dict[int, dict], dict[int, list[str]]]:
    """TaoSentinel's latest score row and active red flags per netuid (any confidence)."""
    if db is None or not netuids:
        return {}, {}
    spec = app_config.settings.SPEC_VERSION
    rows = await Q.latest_scores(db, spec=spec, include_prelaunch=True, include_insufficient=True, netuids=netuids)
    flags: dict[int, list[str]] = {}
    for f in await Q.active_flags(db, spec=spec):
        if f["netuid"] in netuids:
            flags.setdefault(int(f["netuid"]), []).append(f["flag"])
    return {int(r["netuid"]): r for r in rows}, flags


# ── screens (pure over the terminal's payloads; tested directly) ──────────────────────────────────────────────
def render_wallets(wallets: list[dict]) -> tuple[str, list[tuple[str, str]]]:
    if not wallets:
        return ("👛 <b>Wallets</b>\nThe τ Terminal tracks no wallets yet (add one in its Wallets tab).", [])
    lines = ["👛 <b>Wallets</b>", f"<i>from the τ Terminal · {day_month_hm(int(time.time()))}</i>", ""]
    items = []
    for w in wallets:
        tot = wallet_total(w)
        n = w.get("n_positions")
        tags = [t for t in ("trading wallet" if w.get("trading") else "",
                            "watch-only" if not w.get("local") else "") if t]
        lines.append(f"{wallet_icon(w)} <b>{esc(w.get('name'))}</b>" + (f" · <i>{', '.join(tags)}</i>" if tags else ""))
        npos = "no positions" if not n else f"{n} position{'s' if n != 1 else ''}"
        lines.append(f"      {tao(tot)} · {npos}")
        items.append((f"{wallet_icon(w)} {cut(w.get('name'), 18)} · {tao(tot, nd=1)}", wallet_ref(w)))
    lines += ["", "<i>Tap a wallet for positions, PnL and insights.</i>"]
    return "\n".join(lines), items


def render_overview(w: dict, pos: dict) -> str:
    t = pos.get("totals") or {}
    open_, dust = split_held(pos)
    worth = t.get("portfolio_tao")
    exit_ = t.get("portfolio_exit_tao")
    px = t.get("tao_usd")
    kind = "" if w.get("all") else (" · watch-only" if not w.get("local") else " · local")
    lines = [f"<b>{title(w)}</b>{kind}"]
    sub = []
    if w.get("coldkey"):
        sub.append(f"<code>{esc(short_key(w['coldkey']))}</code>")
    if w.get("events"):
        sub.append(f"{int(w['events']):,} trades since {month_year(w.get('first_event'))}")
    if sub:
        lines.append(" · ".join(sub))
    lines.append("")
    lines.append(f"💰 <b>{tao(worth)}</b>" + (f"  ≈ {usd(worth * px)}" if worth is not None and px else ""))
    n_open = len(open_)
    lines.append(f"<i>{tao(exit_)} if sold now · {tao(t.get('free_tao'))} free · "
                 f"{n_open} position{'s' if n_open != 1 else ''}" + (f" + {len(dust)} dust" if dust else "") + "</i>")
    cost = t.get("cost") or 0
    ret = (t.get("unrealized_exit") / cost) if cost and t.get("unrealized_exit") is not None else None
    pnl_rows = [f"{'open':<9}{tao(t.get('unrealized_exit'), sign=True):>13}  {pct(ret)}"] if open_ else []
    lines += ["", "<b>Profit &amp; loss</b>", "<pre>", *pnl_rows,
              f"{'realised':<9}{tao(t.get('realized'), sign=True):>13}",
              f"{'total':<9}{tao(t.get('total'), sign=True):>13}",
              "</pre>"]
    notes = []
    if t.get("emissions_value"):
        notes.append(f"open includes {tao(t['emissions_value'])} of emission alpha")
    if t.get("fees_tao"):
        notes.append(f"{tao(t['fees_tao'], nd=3)} paid in fees")
    if notes:
        lines.append("<i>" + " · ".join(notes) + "</i>")
    if open_:
        top = open_[:3]
        lines += ["", "<b>Biggest positions</b>"]
        for p in top:
            r = pos_return(p)
            lines.append(f"• SN{p['netuid']} {esc(cut(subnet_name(p), 16))} · {tao(pos_value(p))}"
                         + (f" · {pct(r)}" if r is not None else ""))
    if t.get("warnings"):
        lines.append(f"\n⚠️ {int(t['warnings'])} history warning(s) — see the terminal's Positions tab")
    sync = w.get("sync") or {}
    if sync.get("at"):
        lines.append(f"\n<i>history synced {day_month_hm(sync['at'])}" + ("" if sync.get("ok") else " · last sync failed")
                     + "</i>")
    return "\n".join(lines)


def render_positions(w: dict, pos: dict, scores: dict[int, dict], flags: dict[int, list[str]]) -> tuple[str, list[int]]:
    open_, dust = split_held(pos)
    head = f"📂 <b>{wname(w)} · {len(open_)} position{'s' if len(open_) != 1 else ''}</b>"
    if not open_ and not dust:
        return head + "\nNo open positions.", []
    lines = [head, "<i>τ = worth if sold now · PnL vs your cost · T = Sentinel tier</i>", "<pre>",
             f"{'SN':>3} {'name':<9} T {'τ':>6} {'PnL':>6}"]
    for p in open_:
        r = pos_return(p)
        tier = (scores.get(p["netuid"]) or {}).get("tier") or "-"
        lines.append(f"{p['netuid']:>3} {cut(subnet_name(p), 9):<9} {tier} {pos_value(p):>6.2f} "
                     f"{pct(r) if r is not None else '—':>6}")
    total = sum(pos_value(p) for p in open_)
    lines.append(f"{'':>3} {'total':<9}   {total:>6.2f}")
    lines.append("</pre>")
    if dust:
        lines.append("🪙 dust: " + ", ".join(f"SN{p['netuid']}" for p in dust))
    held = [p["netuid"] for p in open_ + dust]
    flagged = [(n, flags[n]) for n in held if flags.get(n)]
    if flagged:
        lines.append("")
        names = {p["netuid"]: subnet_name(p) for p in open_ + dust}
        for n, fl in flagged:
            lines.append(f"🚩 SN{n} {esc(names.get(n, ''))}: " + ", ".join(f"<code>{esc(f)}</code>" for f in fl))
    lines.append("\n<i>Tap a subnet for its position card.</i>")
    return "\n".join(lines), [p["netuid"] for p in open_]


def render_position(w: dict, pos: dict, netuid: int, score: dict | None, flags: list[str]) -> str:
    p = next((x for x in pos.get("positions") or [] if x.get("netuid") == netuid and x.get("occupant") == "current"),
             None)
    if p is None:
        return f"📌 <b>SN{netuid}</b> · {wname(w)}\nNo position or history on this subnet."
    r = pos_return(p)
    entry, spot = p.get("avg_entry"), p.get("spot")
    vs = (spot / entry - 1) if entry and spot else None
    lines = [f"📌 <b>SN{netuid} · {esc(subnet_name(p))}</b> · {wname(w)}", "<pre>"]
    rows = [("held", f"{(p.get('held') or 0):,.2f} α"),
            ("entry", f"{entry:.6f} τ/α" if entry else "—"),
            ("now", (f"{spot:.6f} τ/α" if spot else "—") + (f" {pct(vs)}" if vs is not None else "")),
            ("value", tao(p.get("value_spot"))),
            ("if sold", tao(p.get("exit_value"))),
            ("open PnL", tao(p.get("unrealized_exit"), sign=True) + (f" {pct(r)}" if r is not None else "")),
            ("realised", tao(p.get("realized"), sign=True)),
            ("emission", f"{(p.get('emissions_alpha') or 0):,.1f} α"),
            ("trades", f"{p.get('buys', 0)} buys · {p.get('sells', 0)} sells"),
            ("first", date_long(p.get("first_ts"))),
            ("last", date_long(p.get("last_ts")))]
    for k, v in rows:
        lines.append(f"{k:<9}{v}"[:PRE_WIDTH + 4])
    lines.append("</pre>")
    if p.get("warnings"):
        lines.append("⚠️ " + esc("; ".join(map(str, p["warnings"]))[:200]))
    if score:
        comp = score.get("composite_smoothed")
        lines.append(f"🛰 Sentinel: {tier_dot(score.get('tier'))} <b>{esc(score.get('tier') or '—')}</b>"
                     + (f" · {comp:.1f}" if comp is not None else "") + f" · conf {dot2(score.get('confidence'))}")
    lines.append("🚩 " + ", ".join(f"<code>{esc(f)}</code>" for f in flags) if flags else "✅ no red flags")
    return "\n".join(lines)


def _part_line(part: dict) -> str:
    v, d = part.get("value"), part.get("delta")
    arrow = "" if d is None else ("▲" if d > 0.5 else "▼" if d < -0.5 else "·")
    dd = "" if d is None else f"{arrow}{abs(d):.0f}"
    return f"{cut(part.get('label'), 10):<10} {cells((v or 0) / 100)} {(v or 0):>3.0f} {dd}"


def render_performance(w: dict, ins: dict, period: int) -> str:
    name = PERIODS.get(period, "month")
    head = f"📊 <b>{wname(w)} · performance</b>"
    if ins.get("empty"):
        return head + "\nNo trades in this wallet's history yet."
    sc = ins.get("score") or {}
    lines = [head, f"<i>last {name} versus the {name} before</i>", ""]
    if sc.get("score") is not None:
        delta = sc.get("delta")
        was = (f" · was {sc['prev']:.0f} ({esc(sc.get('prev_grade'))})" if sc.get("prev") is not None else "")
        mv = "" if delta is None else (f" · ▲ {delta:+.0f}" if delta > 0 else f" · ▼ {delta:+.0f}").replace("-", "−")
        lines.append(f"🎓 Trader score <b>{sc['score']:.0f}</b> ({esc(sc.get('grade'))}){was}{mv}")
    else:
        lines.append(f"🎓 Trader score: not enough sells in the last {name}")
    parts = (sc.get("parts") or []) if sc.get("score") is not None else []
    if parts:
        lines += ["<pre>", *[_part_line(p) for p in parts]]
        hist = spark([h[1] for h in sc.get("history") or []], 20)
        if hist:
            lines.append(f"{'trend':<10} {hist}"[:PRE_WIDTH + 4])
        lines.append("</pre>")
    sells, prev = sc.get("sells"), sc.get("prev_sells")
    if sells == 0:
        lines.append(f"💵 No sells in the last {name}" + (f" ({prev} the {name} before)" if prev else "") + ".")
    elif sells is not None:
        lines.append(f"💵 Realised this {name}: <b>{tao(sc.get('realized'), sign=True)}</b> over {sells} sell"
                     f"{'s' if sells != 1 else ''}" + (f" ({prev} the {name} before)" if prev is not None else ""))
    stats = {s.get("label"): s.get("value") for s in ins.get("stats") or []}
    life = [f"{esc(stats[k])} {lab}" for k, lab in (("Trades", "trades"), ("Win rate", "of sells won"),
                                                    ("Subnets", "subnets")) if stats.get(k)]
    if life:
        lines.append("📈 Lifetime: " + " · ".join(life))
    if stats.get("Total PnL"):
        lines.append(f"Σ Lifetime PnL: <b>{minus(stats['Total PnL'])}</b>"
                     + (f" · fees {esc(stats['Fees'])}" if stats.get("Fees") else ""))
    curve = spark([c[1] for c in ins.get("curve") or []], 24)
    if curve:
        lines.append(f"<code>{curve}</code> <i>realised PnL over time</i>")
    cards = {c.get("id"): c for c in ins.get("cards") or []}
    extras = [("🌞", "best_month", "best month"), ("🌧", "worst_month", "worst month"), ("📉", "drawdown", "deepest hole"),
              ("⏳", "hold_time", "typical hold"), ("🎯", "streaks", "win rate")]
    shown = [(e, lab, cards[k]["headline"]) for e, k, lab in extras if k in cards]
    if shown:
        lines.append("")
        for e, lab, h in shown:
            lines.append(f"{e} {lab}: {minus(h)}")
    tip = min(parts, key=lambda p: p.get("value") or 0).get("tip") if parts else None
    if tip:
        lines += ["", f"💡 <i>{esc(tip)}</i>"]
    return "\n".join(lines)


def insight_cards(ins: dict) -> list[dict]:
    return [c for c in ins.get("cards") or [] if c.get("id") not in _SCORE_CARDS]


def render_insights(w: dict, ins: dict, page: int) -> tuple[str, int, int]:
    """Returns (text, page, pages); page 0 opens with the persona."""
    head = f"💡 <b>{wname(w)} · insights</b>"
    if ins.get("empty"):
        return head + "\nNo trades in this wallet's history yet.", 0, 0
    cards = insight_cards(ins)
    pages = max(1, -(-len(cards) // INSIGHTS_PER_PAGE))
    page = max(0, min(int(page), pages - 1))
    lines = [head]
    per = ins.get("persona") or {}
    if page == 0 and per:
        lines += ["", f"{esc(per.get('emoji') or '')} <b>{esc(per.get('title'))}</b>",
                  f"<i>{esc(per.get('blurb'))}</i>"]
        if per.get("traits"):
            lines.append(" · ".join(esc(t) for t in per["traits"]))
    for c in cards[page * INSIGHTS_PER_PAGE:(page + 1) * INSIGHTS_PER_PAGE]:
        lines += ["", f"{esc(c.get('emoji') or '•')} <b>{esc(c.get('title'))}</b> — {minus(c.get('headline'))}"]
        if c.get("detail"):
            lines.append(f"<i>{minus(c['detail'])}</i>")
    return "\n".join(lines), page, pages


def render_trades(w: dict, trades: list[dict], page: int, names: dict[int, str]) -> tuple[str, int, int]:
    head = f"🧾 <b>{wname(w)} · trades</b>"
    rows = sorted(trades or [], key=lambda d: (d.get("ts") or 0), reverse=True)
    if not rows:
        return head + "\nNo trades in this wallet's history.", 0, 0
    pages = -(-len(rows) // TRADES_PER_PAGE)
    page = max(0, min(int(page), pages - 1))
    lines = [head, f"<i>newest first · {len(rows):,} in history · τ moved per trade</i>", "<pre>"]
    year = None
    for t in rows[page * TRADES_PER_PAGE:(page + 1) * TRADES_PER_PAGE]:
        ts = int(t.get("ts") or 0)
        y = time.gmtime(ts).tm_year
        if y != year:
            lines.append(f"── {y} ──")
            year = y
        kind = _KIND.get(t.get("kind"), str(t.get("kind") or "?"))[:4]
        if t.get("source") == "paper":
            kind = "p" + kind[:3]
        n = t.get("netuid")
        nm = "(old)" if t.get("old_occupant") else ("Root" if n == 0 else names.get(n, ""))
        lines.append(f"{day_month(ts):<6} {kind:<4} {n if n is not None else '-':>3} {cut(nm, 7):<7} "
                     f"{float(t.get('tao') or 0):>7.2f}")
    lines.append("</pre>")
    lines.append("<i>buy/sell = swaps · move = between your own keys · liq = subnet deregistered</i>")
    return "\n".join(lines), page, pages


# ── dispatcher glue ──────────────────────────────────────────────────────────────────────────────────────────
async def screen(db, view: str, arg: str, ref: str) -> tuple[str, dict]:
    """One wallet screen for callback ``w:<view>:<arg>:<ref>`` (or ``w:l`` → the wallet list)."""
    try:
        wallets = await terminal_get("/api/wallets")
        if view == "l":
            text, items = render_wallets(wallets)
            return text, wallets_keyboard(items)
        w = find_wallet(wallets, ref)
        if w is None:
            text, items = render_wallets(wallets)
            return f"⚠️ No wallet called <code>{esc(ref)}</code>.\n\n" + text, wallets_keyboard(items)
        r, sel = ref_of(w), api_sel(w)
        if view == "o":
            return render_overview(w, await terminal_get("/api/positions", wallet=sel)), wallet_keyboard(r)
        if view == "p":
            pos = await terminal_get("/api/positions", wallet=sel)
            open_, dust = split_held(pos)
            scores, flags = await sentinel_context(db, [p["netuid"] for p in open_ + dust])
            text, nets = render_positions(w, pos, scores, flags)
            return text, positions_keyboard(r, nets)
        if view == "s":
            n = int(arg)
            pos = await terminal_get("/api/positions", wallet=sel)
            scores, flags = await sentinel_context(db, [n])
            return render_position(w, pos, n, scores.get(n), flags.get(n, [])), position_keyboard(r, n)
        if view == "f":
            period = int(arg) if arg.isdigit() and int(arg) in PERIODS else 30
            ins = await terminal_get("/api/insights", wallet=sel, period=period)
            return render_performance(w, ins, period), performance_keyboard(r, period)
        if view == "i":
            ins = await terminal_get("/api/insights", wallet=sel, period=30)
            text, page, pages = render_insights(w, ins, int(arg or 0))
            return text, pager_keyboard("i", r, page, pages)
        if view == "t":
            trades = await terminal_get("/api/trades", wallet=sel)
            names = await Q.subnet_names(db) if db is not None else {}
            text, page, pages = render_trades(w, trades, int(arg or 0), names)
            return text, pager_keyboard("t", r, page, pages, newer_left=True)
    except TerminalUnavailable as exc:
        return unavailable_text(exc), back_keyboard(("🔁 Retry", "w:l"))
    return "Unknown wallet screen.", back_keyboard()


def parse_callback(data: str) -> tuple[str, str, str]:
    """``w:l`` → (l, '', ''); ``w:<view>:<arg>:<ref>`` → (view, arg, ref) — the ref may contain ``:``."""
    parts = data.split(":", 3)
    if len(parts) < 4:
        return parts[1] if len(parts) > 1 else "l", "", ""
    return parts[1], parts[2], parts[3]
