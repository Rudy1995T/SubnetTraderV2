"""Alert templates (spec 14 §8.5): per-type renderers, else ``payload.text`` (AlertLatch rows), else a generic line.

Every dynamic value is HTML-escaped; a missing payload key renders ``?`` so a payload variant never breaks a send.
"""
from __future__ import annotations

from typing import Any, Mapping

import app.config as app_config
from app.alerts.routing import payload_of
from app.notifications.format import esc

_ICON = {"critical": "🛑", "warning": "⚠️", "info": "ℹ️"}


class _Safe(dict):
    def __missing__(self, key: str) -> str:
        return "?"


def _f(x: Any, spec: str = ".1f") -> str:
    try:
        return format(float(x), spec)
    except (TypeError, ValueError):
        return "?"


def _sn(row: Mapping[str, Any]) -> str:
    n = row.get("netuid")
    name = row.get("_name")
    if n is None:
        return ""
    return f"SN{esc(n)}" + (f" {esc(name)}" if name else "")


def generic(row: Mapping[str, Any], *, link: bool = True) -> str:
    p = payload_of(row)
    sev = row.get("severity") or "info"
    kv = ", ".join(f"{esc(k)}={esc(str(v)[:60])}" for k, v in list((k, v) for k, v in p.items()
                                                                    if k not in ("text", "alert_route"))[:6])
    sn = _sn(row)
    head = f"{_ICON.get(sev, 'ℹ️')} <b>{esc(row.get('event_type'))}</b>" + (f" · {sn}" if sn else "") + f" · {esc(sev)}"
    return head + (f"\n<code>{kv}</code>" if kv else "")


def render(row: Mapping[str, Any], *, link: bool = True) -> str:
    et = row.get("event_type")
    p = payload_of(row)
    sp = _Safe({k: esc(v) for k, v in p.items()})
    ui = app_config.settings.UI_BASE_URL
    sn = _sn(row)
    try:
        if et == "tier_change":
            arrow = "🔻" if str(p.get("direction")) == "down" else "🔺"
            s = (f"{arrow} <b>{sn}</b> tier {sp['from']} → {sp['to']} · composite {_f(p.get('composite_smoothed'))} "
                 f"(prev {_f(p.get('prev_composite_smoothed'))})")
            return s + (f' · <a href="{esc(ui)}/subnet/{esc(row.get("netuid"))}">open</a>' if link else "")
        if et == "red_flag_raised":
            extras = (" · human" if p.get("human") else "") + (" · sticky" if p.get("sticky") else "")
            det = p.get("detail") or {}
            det_s = ", ".join(f"{k}={v}" for k, v in list(det.items())[:3]) if isinstance(det, dict) else str(det)
            return (f"🚩 <b>{sn}</b> red flag <code>{sp['flag']}</code> ×{_f(p.get('multiplier'), '.2f')} "
                    f"(total ×{_f(p.get('flag_multiplier_total'), '.2f')}){extras}\n<i>{esc(det_s)}</i>")
        if et == "red_flag_cleared":
            try:
                days = f"{(int(p['as_of']) - int(p['raised_at'])) / 86400:.0f}"
            except (KeyError, TypeError, ValueError):
                days = "?"
            return f"✅ <b>{sn}</b> flag <code>{sp['flag']}</code> cleared ({sp['reason']}) after {days} d"
        if et == "score_jump":
            d = p.get("delta")
            icon = "📉" if (isinstance(d, (int, float)) and d < 0) else "📈"
            return (f"{icon} <b>{sn}</b> composite {_f(p.get('prev_composite'))} → {_f(p.get('composite'))} "
                    f"({_f(d, '+.1f')}) · flags +{sp['flags_added']} −{sp['flags_removed']}")
        if et == "collector_failing":
            return (f"⚠️ collector <code>{sp['collector']}</code> failing ×{sp['consecutive_failures']} · last: "
                    f"<i>{sp['last_error']}</i> · /pause {sp['collector']}")
        if et == "collector_recovered":
            try:
                mins = f"{float(p['down_seconds']) / 60:.0f}"
            except (KeyError, TypeError, ValueError):
                mins = "?"
            return f"✅ collector <code>{sp['collector']}</code> recovered after {sp['failures']} failures ({mins} min)"
        if et == "spend_80pct":
            try:
                pct = f"{float(p['pct']):.0%}"
            except (KeyError, TypeError, ValueError):
                pct = "?"
            usd = (f" · ${_f(p.get('usd_mtd'), '.2f')} / ${_f(p.get('cap_usd'), '.2f')}"
                   if p.get("unit") == "usd" else "")
            return f"💸 <code>{sp['group']}</code> at {pct} of its {sp['month']} cap ({sp['unit']}){usd}"
        if et == "spend_cap_reached":
            jobs = ", ".join(p.get("paused_jobs") or []) or "-"
            return f"⛔ <code>{sp['group']}</code> monthly cap reached ({sp['month']}) · paused: {esc(jobs)}"
        if et == "backup_failed":
            return (f"🛑 backup failed: <i>{sp['error']}</i> · last good {sp['last_good_name']} "
                    f"({_f(p.get('last_good_age_h'), '.0f')} h)")
        if et == "dereg_watch":
            immune = "yes" if p.get("immune") else "no"
            left = ""
            if p.get("immune") and p.get("blocks_left_immunity") is not None:
                left = f", {_f(float(p['blocks_left_immunity']) / 7200)} d left"
            return (f"⏰ watchlist <b>{sn}</b> dereg risk: {sp['reason']} · rank {sp['moving_price_rank']} · "
                    f"immune {immune}{left} · dereg_safety {sp['dereg_safety']}")
        if et == "netuid_reoccupied":
            return (f"♻️ <b>SN{esc(row.get('netuid'))}</b> re-registered at block {sp['new_network_registered_at']} · "
                    f"{sp['old_name']} ({sp['old_occupant_key']}) → {sp['new_name']} · fresh history started")
        if et == "identity_changed" and row.get("severity") == "warning":
            return (f"🔁 <b>{sn}</b> {sp['field']} {sp['kind']}: <code>{sp['old_value']}</code> → "
                    f"<code>{sp['new_value']}</code> (source {sp['source']})")
    except Exception:  # pragma: no cover - a template bug must never block the queue
        pass
    if p.get("text"):
        return str(p["text"])
    return generic(row, link=link)
