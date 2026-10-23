"""``market_brief_daily`` (spec 14 §2.8): one plain Telegram message a day, ≤ 20 lines, never an event (nothing pages).

Slot ``S`` = the newest non-shadow slot of ``SPEC_VERSION``; ``P = S − 86400``. Lines: regime, top 5 by composite,
five biggest 24-h movers, tier changes, flags raised/cleared, dereg watch (19 §2.4 ``days_to_cull_static`` < 7 d) and
one data-health line. ``python -m app.notifications.brief [--db PATH]`` prints it from a read-only connection.
"""
from __future__ import annotations

import argparse
import asyncio
import sys

import app.config as app_config
from app.api import queries as Q
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.notifications import telegram
from app.notifications.format import cut, day_month, day_month_hm, dot2, esc, num

DAY = 86400
MAX_LINES = 20
DEREG_DAYS = 7.0
WB_STALE_S = 7200
_TIER_RANK = {"A": 0, "B": 1, "C": 2, "D": 3, "F": 4}


def _cap(items: list[str], n: int, sep: str = " · ") -> str:
    return sep.join(items[:n]) + (f" +{len(items) - n}" if len(items) > n else "")


def _tier_line(cur: dict[str, dict], old: dict[str, dict]) -> str:
    down, up = [], []
    for k, r in cur.items():
        o = old.get(k)
        a, b = (o or {}).get("tier"), r.get("tier")
        if a is None or b is None or a == b:
            continue
        (down if _TIER_RANK[b] > _TIER_RANK[a] else up).append((r["netuid"], f"{r['netuid']} {a}→{b}"))
    if not down and not up:
        return "🔀 <b>Tiers</b> no change"
    items = [f"↓ {t}" if i == 0 else t for i, (_, t) in enumerate(sorted(down))] + \
            [f"↑ {t}" if i == 0 else t for i, (_, t) in enumerate(sorted(up))]
    return "🔀 <b>Tiers</b> " + _cap(items, 8)


def _flag_line(raised: list[dict], cleared: list[dict]) -> str:
    def names(rows: list[dict]) -> str:
        return (" " + _cap([f"SN{r['netuid']} {esc(r['flag'])}" for r in rows], 4, ", ")) if rows else ""
    return f"🚩 <b>Flags</b> +{len(raised)}{names(raised)} · −{len(cleared)}{names(cleared)}"


def _dereg_line(dtc: list[dict], net: dict | None, now: int) -> str:
    near = [r for r in dtc if float(r["value"]) < DEREG_DAYS]
    body = _cap([f"SN{r['netuid']} {float(r['value']):.1f} d" for r in near], 4) if near else "none within 7 d"
    if net is not None:
        if net.get("eta_next_registration_days") is not None:
            body += f" · next reg ≈ {float(net['eta_next_registration_days']):.1f} d"
        if now - int(net["observed_at"]) > WB_STALE_S:
            body += f" (stale, as of {day_month_hm(net['hour_ts'])})"
    return f"⚰️ <b>Dereg watch</b> {body}"


def _health_line(n_scored: int, cov: dict[str, int], health: dict | None) -> str:
    pp = Q.populated_by_pillar(n_scored, cov)
    line = f"🩺 {sum(p for p, _ in pp.values())}/{sum(n for _, n in pp.values())} metrics"
    if health is None:
        return line + " · service health unavailable"
    jobs = {**(health.get("collectors") or {}), **(health.get("tasks") or {})}
    stale = sum(1 for v in jobs.values() if isinstance(v, dict) and v.get("stale"))
    pending = (health.get("alerts") or {}).get("pending_immediate")
    return line + f" · {stale} stale job{'s' if stale != 1 else ''} · {pending if pending is not None else '?'} alerts pending"


async def render_brief(db, *, now: int, health: dict | None) -> str:
    spec = app_config.settings.SPEC_VERSION
    meta = await Q.feed_meta(db, spec=spec)
    head = f"🌅 <b>Market brief</b> · {day_month(now)}"
    S = meta.get("max_as_of")
    if S is None:
        return f"{head}\nNo scores for {esc(spec)} yet."
    P = S - DAY
    cur = await Q.scores_at(db, spec=spec, as_of=S)
    old = await Q.scores_at(db, spec=spec, as_of=P)
    regime = next((r["regime"] for r in cur.values() if r.get("regime")), meta.get("regime"))
    lines = [head, f"<i>slot {day_month_hm(S)} · {esc(meta.get('run_kind'))} · regime {esc(regime or '-')}</i>",
             "<i>a risk filter, not a buy signal (13 verdict 23 Oct)</i>"]

    top = (await Q.latest_scores(db, spec=spec))[:5]
    lines.append("🏆 <b>Top 5</b> by composite")
    if top:
        rows = [f"{r['netuid']:>3} {esc(cut(r['name'] or '?', 13)):<13} {r['tier'] or '-'} "
                f"{num(r['composite_smoothed'], '.1f', 5)} {dot2(r['confidence']):>3}{'!' if r['red_flags'] else ''}"
                for r in top]
        lines.append("<pre>" + "\n".join(rows) + "</pre>")
    else:
        lines.append("none reaches confidence 0.4")

    lines.append("📈 <b>24-h movers</b> · Δ composite")
    moves = sorted(((r["composite_smoothed"] - old[k]["composite_smoothed"], r) for k, r in cur.items()
                    if k in old and r["composite_smoothed"] is not None and old[k]["composite_smoothed"] is not None),
                   key=lambda x: (-abs(x[0]), x[1]["netuid"]))[:5]
    if not old:
        lines.append(f"no slot 24 h earlier ({day_month_hm(P)})")
    elif not moves:
        lines.append("nothing moved")
    else:
        rows = [f"{r['netuid']:>3} {esc(cut(r['name'] or '?', 12)):<12} {r['tier'] or '-'} {d:+5.1f} "
                f"{num(r['composite_smoothed'], '.1f', 5)}" for d, r in moves]
        lines.append("<pre>" + "\n".join(rows) + "</pre>")

    lines.append(_tier_line(cur, old) if old else "🔀 <b>Tiers</b> no slot 24 h earlier")
    raised, cleared = await Q.flag_changes(db, spec=spec, after=P, upto=S)
    lines.append(_flag_line(raised, cleared))
    _, dtc = await Q.days_to_cull(db, now=now)
    lines.append(_dereg_line(dtc, await Q.wb_network_latest(db), now))
    n_scored, cov = await Q.norm_coverage(db, spec=spec, as_of=S)
    lines.append(_health_line(n_scored, cov, health))
    return "\n".join(lines)


class MarketBriefJob(CollectorJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        from app.api.health import build_health
        try:
            health = await build_health(run.rt)
        except Exception:
            health = None
        text = await render_brief(run.rt.db, now=int(run.started_at), health=health)
        if not telegram.telegram_configured():
            return JobOutcome(rows=0, note="telegram not configured; brief not sent")
        ok, status = await telegram.send_message(text)
        if not ok and status == 400:
            ok, status = await telegram.send_message(telegram.strip_html(text), parse_mode=None)
        if not ok:
            raise RuntimeError(f"brief send failed (status {status})")
        return JobOutcome(rows=1, note=f"sent {text.count(chr(10)) + 1} lines")


async def _print(db_path: str) -> int:
    import httpx
    from app.storage.db import ReadOnlyDatabase
    from app.util.time import now_ts
    s = app_config.settings
    host = "127.0.0.1" if s.HEALTH_HOST in ("0.0.0.0", "::") else s.HEALTH_HOST
    try:
        health = httpx.get(f"http://{host}:{s.HEALTH_PORT}/health", timeout=5).json()
    except Exception:
        health = None
    db = ReadOnlyDatabase(db_path)
    await db.connect()
    try:
        print(await render_brief(db, now=now_ts(), health=health))
    finally:
        await db.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m app.notifications.brief",
                                description="Print the daily market brief (14 §2.8) from a read-only DB; sends nothing.")
    p.add_argument("--db", default=None, help="database path (default DB_PATH)")
    a = p.parse_args(argv)
    return asyncio.run(_print(a.db or str(app_config.settings.DB_PATH)))


if __name__ == "__main__":
    sys.exit(main())
