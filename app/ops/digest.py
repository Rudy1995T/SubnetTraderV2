"""``ops_digest_daily`` (09:00Z, spec 16 §2.13.4): one Telegram report built in-process. Every section tolerates
missing later-phase tables (``sqlite3.OperationalError`` → section omitted)."""
from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import app.config as app_config
from app import control
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.notifications import telegram
from app.notifications.format import chunk, esc, fmt_age
from app.util.time import now_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.runtime import Runtime


async def build_digest(rt: "Runtime") -> str:
    s = app_config.settings
    now = now_ts()
    db = rt.db
    lines = [f"🛰 <b>TaoSentinel ops</b> · {to_iso(now)[:16]}Z · v{esc(rt.version)} · up {fmt_age(now - rt.started_at)}"]
    warn: list[str] = []
    try:
        from app.ops.recovery import boots
        b = [x for x in await boots(db) if x.get("at", 0) >= now - 7 * 86400]
        unclean = [x for x in b if x.get("kind") in ("process_crash", "host_unclean")]
        withheld = sum(1 for t in rt.watchdog_withheld if t >= now - 86400)
        lines.append(f"boots 7d: {len(b)} ({len(unclean)} unclean) · watchdog withheld 24h: {withheld}")
    except Exception:
        pass
    try:
        rows = await db.fetchall(
            "SELECT job, SUM(ok = 1) AS ok, SUM(ok = 0) AS fail, SUM(rows) AS rows, "
            "(SELECT error FROM collector_runs c2 WHERE c2.job = c.job AND c2.ok = 0 ORDER BY started_at DESC LIMIT 1) "
            "AS last_error FROM collector_runs c WHERE started_at >= ? GROUP BY job ORDER BY job", (now - 86400,))
        tbl = ["<pre>job                    ok/fail  rows    last error"]
        seen = set()
        for r in rows:
            seen.add(r["job"])
            err = (r["last_error"] or "")[:28]
            tbl.append(f"{r['job'][:22]:<22} {r['ok'] or 0:>3}/{r['fail'] or 0:<4} {r['rows'] or 0:>6}  {esc(err)}")
        tbl.append("</pre>")
        lines.append("jobs 24h")
        lines.extend(tbl)
        for jid, job in rt.jobs.items():
            if job.spec.kind in ("interval", "cron") and job.enabled() and not job.paused_reason() and jid not in seen \
                    and (job.effective_liveness_max_age() or 0) <= 2 * 86400 and now - rt.started_at > 86400:
                warn.append(f"⚠️ {jid}: 0 runs in 24 h")
    except sqlite3.OperationalError:
        pass
    failing = [j for j, job in rt.jobs.items() if job._status.consecutive_failures >= job.fail_alert_threshold()]
    paused = [f"{j} ({job.paused_reason()})" for j, job in rt.jobs.items() if job.enabled() and job.paused_reason()]
    st = control.sentinel_state()
    lines.append(f"failing now: {esc(', '.join(failing) or 'none')} · paused: {esc(', '.join(paused) or 'none')} · "
                 f"PAUSE_ALL: {'yes' if st['pause_all'] else 'no'} · KILL_SWITCH: {'yes' if st['kill_switch'] else 'no'}"
                 f" · SCORING_KILLED: {'yes' if st['scoring_killed'] else 'no'}")
    if rt.costs is not None:
        parts = []
        for g, v in rt.costs.snapshot().items():
            if v.get("request_cap"):
                parts.append(f"{g} {v['requests']:,}/{v['request_cap']:,} req")
            elif v.get("cap_usd") is not None:
                parts.append(f"{g} ${v['usd']:.2f}/${v['cap_usd']:.0f} ({v['pct']:.0%})" +
                             (" stopped" if v.get("paused") else ""))
        lines.append("spend " + esc(" · ".join(parts)))
    try:
        from app.ops.guard import disk_state, mirror_state
        from app.storage.backup import latest_backup, latest_backup_age_hours
        ds = disk_state()
        st_db = await db.stats()
        lines.append(f"db {st_db['size_bytes'] / 2**30:.2f} GB · WAL {st_db['wal_bytes'] / 2**20:.0f} MB · "
                     f"disk free {ds['free_gb']} GB · logs {ds['logs_gb'] * 1024:.0f} MB")
        lb = latest_backup(s.path(s.DB_BACKUP_DIR))
        age = latest_backup_age_hours(s.path(s.DB_BACKUP_DIR))
        mir = mirror_state()
        lines.append(f"backup {esc((lb or {}).get('name', 'none'))} ({fmt_age(age * 3600) if age is not None else '-'}"
                     f" old) · mirror {mir}")
        if mir == "off":
            warn.append("⚠️ backup mirror not configured")
        if age is None or age > s.DB_BACKUP_STALE_HOURS:
            warn.append("⚠️ backup stale")
        if ds["free_gb"] < 2 * s.DISK_FREE_MIN_GB:
            warn.append("⚠️ disk low")
    except Exception:
        pass
    if s.GITHUB_TOKEN:
        # 06 §6: header if GitHub sent one, else the operator's GITHUB_TOKEN_EXPIRES_AT (fine-grained PATs send none)
        from app.collectors.github.client import QUOTA
        exp = QUOTA.expires_at()
        if exp is None:
            lines.append("GitHub PAT expiry unknown (set GITHUB_TOKEN_EXPIRES_AT)")
        else:
            days = (exp - now) // 86400
            lines.append(f"GitHub PAT expires in {days} d")
            if days < s.PAT_EXPIRY_WARN_DAYS:
                warn.append(f"⚠️ GitHub PAT expires in {days} d (runbook §7.3.3)")
    try:
        from app.ops.security import selfcheck
        sc = selfcheck(s)
        lines.append("security: " + " · ".join([
            f".env 600 {'✓' if sc['env_mode_ok'] else '✗'}", f"secrets 700 {'✓' if sc['secrets_dir_ok'] else '✗'}",
            f"bind {esc(s.HEALTH_HOST)} {'✓' if sc['bind_ok'] else '✗'}",
            f"auth token {'✓' if sc['auth_token_set'] else '–'}", f"wallet dirs {sc['wallet_dirs_found']}"]))
    except Exception:
        pass
    lines.extend(warn)
    return "\n".join(lines)


class OpsDigestJob(CollectorJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        text = await build_digest(run.rt)
        if not telegram.telegram_configured():
            return JobOutcome(rows=0, note="telegram not configured")
        sent = 0
        for part in chunk(text):
            ok, status = await telegram.send_message(part)
            if not ok:
                raise RuntimeError(f"digest send failed (status {status})")
            sent += 1
        return JobOutcome(rows=sent, note=f"chunks={sent}")
