"""P5 X producers (spec 09 §3.3, §4; 02 §C3/§C5): ``x_engagement_30d``, ``x_followers`` (scored), the legs
``x_cadence_30d``/``x_last_activity_days`` (12 derives ``announcement_cadence_30d``/``silence_days`` — no producer
here), evidence ``x_mentions_7d`` and ``x_kol_posts_30d`` (fallback leg of 10's ``kol_coverage_30d``).

Order of decisions (§4 "general rule"): absence from reference rows first (PIT ``watch_channels`` X rows other than
legacy-only ones, ``x_accounts.status``, the resolution back-off), then failure from input freshness
(``x_accounts.last_polled_at``, the ctf fetch state, ``collector_runs``), and only then a value. Every post/mention read
is ``observed_at ≤ as_of``. ``x_accounts.status``/``last_polled_at`` and ``job_state`` are state, not PIT — accepted
(same class as the engagement refresh, 02 §J2).

ctf fallback (§2.9, contract with ``app/collectors/x_ctf.py``): with X inactive and ``X_CTF_FALLBACK_METRICS`` the two
legs read team posts of the occupant only when ``job_state('x_ctf_crosscheck', 'page:{occupant_key}')`` says
``http_status == 200``, ``n_matched ≥ 1`` and ``as_of − 3 d < fetched_at ≤ as_of``; a page fetched OK without items is
``failed`` (``ctf_no_feed``), never a measured silence.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from statistics import median
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.features.types import MetricResult
from app.util.time import parse_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

DAY = 86400
SPEC_VERSION = "ts-1.0"
POLL_MAX_AGE_S = 3 * DAY                 # last successful poll of the account / ctf fetch of its page (§3.3)
CTF_FEED_ITEMS = 12                      # the subnetalpha feed shows the latest ~12 tweets (§2.9)
MIN_VERIFIED_KOLS = 5                    # §4 x_kol_posts_30d: fewer verified KOL rows → failed
KOL_MIN_CREDIBILITY = 0.5
X_POLL_JOBS = ("x_team_poll", "x_kol_poll", "x_mentions_poll")
LEG_OF = {"x_cadence_30d": "announcement_cadence_30d", "x_last_activity_days": "silence_days"}
SCORED = {"x_engagement_30d": 0.10, "x_followers": 0.05}


def snapshot_max_age_s() -> int:
    """``(X_PROFILE_REFRESH_DAYS + 3)`` days (§3.3)."""
    return (int(app_config.settings.X_PROFILE_REFRESH_DAYS) + 3) * DAY


def _ok(v: float, **vj: Any) -> MetricResult:
    return MetricResult(float(v), vj)


def _failed(reason: str, **vj: Any) -> MetricResult:
    return MetricResult(None, {"reason": reason, **vj})


def _na(reason: str, **vj: Any) -> MetricResult:
    return MetricResult(None, {"status": "not_applicable", "reason": reason, **vj})


def _absent(reason: str, **vj: Any) -> MetricResult:
    return MetricResult(0.0, vj, absence_reason=reason)


# ── reference state: absence (§4 column 1) ───────────────────────────────────────────────────────────────────
@dataclass
class XAccountState:
    absent: str | None = None                          # 'no_x_account' | 'x_account_gone'
    handles: list[str] = field(default_factory=list)   # handle_lower of the PIT watch rows
    live: list[dict] = field(default_factory=list)     # non-gone x_accounts rows
    pending: list[str] = field(default_factory=list)   # declared handles with no account yet (not gone)


async def _job_state(db: "Database", job: str, key: str) -> dict | None:
    v = await db.fetchval("SELECT value_json FROM job_state WHERE job = ? AND key = ?", (job, key))
    try:
        return json.loads(v) if v else None
    except ValueError:
        return None


async def team_handles_at(db: "Database", occupant_key: str, t: int) -> list[str]:
    """PIT X handles (12 §2.6's test): first_seen ≤ t and not retired by t; `legacy` rows never count (§2.4)."""
    rows = await db.fetchall(
        "SELECT handle FROM watch_channels WHERE platform = 'x' AND occupant_key = ? AND source <> 'legacy' "
        "AND first_seen_at <= ? AND (retired_at IS NULL OR retired_at > ?)", (occupant_key, t, t))
    return sorted({str(r["handle"]).lower() for r in rows})


def _declared_gone(st: dict | None) -> bool:
    """A declared handle that resolved as not_found/suspended twice ≥ X_UNRESOLVED_RETRY_DAYS apart (§2.5)."""
    if not st or int(st.get("not_found_count") or 0) < 2:
        return False
    a, b = st.get("not_found_first_at"), st.get("not_found_last_at")
    return a is not None and b is not None and int(b) - int(a) >= int(app_config.settings.X_UNRESOLVED_RETRY_DAYS) * DAY


async def account_state(db: "Database", occupant_key: str, t: int) -> XAccountState:
    handles = await team_handles_at(db, occupant_key, t)
    if not handles:
        return XAccountState(absent="no_x_account")
    accts: dict[str, dict] = {}
    pending: list[str] = []
    for hl in handles:
        a = await db.fetchone("SELECT * FROM x_accounts WHERE handle_lower = ?", (hl,))
        if a is None:
            from app.collectors.x import renamed_author
            aid = await renamed_author(db, hl)
            a = await db.fetchone("SELECT * FROM x_accounts WHERE author_id = ?", (aid,)) if aid else None
        if a is not None:
            accts[a["author_id"]] = a
        elif not _declared_gone(await _job_state(db, "x_team_poll", f"unresolved:{hl}")):
            pending.append(hl)
    for a in await db.fetchall("SELECT a.* FROM x_account_occupants o JOIN x_accounts a ON a.author_id = o.author_id "
                               "WHERE o.occupant_key = ?", (occupant_key,)):
        accts.setdefault(a["author_id"], a)
    live = [a for a in accts.values() if a["status"] != "gone"]
    if not live and not pending:
        return XAccountState(absent="x_account_gone", handles=handles)
    return XAccountState(handles=handles, live=live, pending=pending)


# ── freshness: failure (§4 column 2) ─────────────────────────────────────────────────────────────────────────
async def ctf_state(db: "Database", occupant_key: str) -> dict | None:
    return await _job_state(db, "x_ctf_crosscheck", f"page:{occupant_key}")


def ctf_usable(st: dict | None, t: int) -> tuple[bool, str]:
    """(usable, reason-if-not) — the §2.9 gate."""
    if not st:
        return False, "ctf_not_fetched"
    fetched = st.get("fetched_at")
    if fetched is None or not (t - POLL_MAX_AGE_S < int(fetched) <= t):
        return False, "ctf_stale"
    if st.get("http_status") != 200:
        return False, "ctf_fetch_failed"
    if int(st.get("n_items") or 0) == 0:
        return False, "ctf_no_feed"
    if int(st.get("n_matched") or 0) < 1:
        return False, "ctf_handle_mismatch"
    return True, ""


def _x_active() -> bool:
    return bool(app_config.settings.x_active())


async def _provider_failure(st: XAccountState, t: int) -> MetricResult | None:
    """(b) a declared handle not resolved yet; (c) an account without a successful poll within 3 d."""
    if st.pending:
        return _failed("x_handle_unresolved", handles=st.pending)
    stale = [a["handle"] for a in st.live
             if a["last_polled_at"] is None or int(a["last_polled_at"]) <= t - POLL_MAX_AGE_S]
    if stale:
        return _failed("x_poll_stale", handles=stale)
    return None


async def _leg_source(db: "Database", occupant_key: str, t: int) -> tuple[MetricResult | None, str, XAccountState,
                                                                              dict | None]:
    """Decide absence/failure for the two legs; returns (early result | None, mode 'provider'|'ctf', state, ctf)."""
    st = await account_state(db, occupant_key, t)
    if st.absent:
        return _absent(st.absent), "", st, None
    if not _x_active():
        if not bool(app_config.settings.X_CTF_FALLBACK_METRICS):
            return _failed("x_inactive"), "", st, None
        cs = await ctf_state(db, occupant_key)
        ok, why = ctf_usable(cs, t)
        if not ok:
            return _failed(why, x_inactive=True), "", st, cs
        return None, "ctf", st, cs
    bad = await _provider_failure(st, t)
    if bad is not None:
        return bad, "", st, None
    return None, "provider", st, None


_TEAM_POSTS = (
    "FROM social_posts p WHERE p.platform = 'x' AND p.is_team_account = 1 AND p.observed_at <= :t "
    "AND p.event_at <= :t AND (p.occupant_key = :o OR EXISTS (SELECT 1 FROM social_post_mentions m WHERE "
    "m.platform = 'x' AND m.post_id = p.post_id AND m.match_kind = 'team' AND m.occupant_key = :o AND "
    "m.observed_at <= :t))")
_ORIGINAL = " AND p.is_repost = 0 AND p.in_reply_to IS NULL"
_NO_CTF = " AND p.source <> 'subnetalpha_ctf'"


# ── producer fns (02 §C5) ────────────────────────────────────────────────────────────────────────────────────
async def compute_x_cadence_30d(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    t = parse_ts(as_of)
    early, mode, _st, _cs = await _leg_source(db, occupant_key, t)
    if early is not None:
        return early
    sql = ("SELECT COUNT(*) " + _TEAM_POSTS + _ORIGINAL + " AND p.event_at > :lo"
           + (_NO_CTF if mode == "provider" else ""))
    n = int(await db.fetchval(sql, {"t": t, "o": occupant_key, "lo": t - 30 * DAY}) or 0)
    if mode == "ctf":
        return _ok(n, source="subnetalpha_ctf", truncated=n >= CTF_FEED_ITEMS)
    return _ok(n, source="provider")


async def compute_x_last_activity_days(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    t = parse_ts(as_of)
    early, mode, st, cs = await _leg_source(db, occupant_key, t)
    if early is not None:
        return early
    sql = "SELECT MAX(p.event_at) " + _TEAM_POSTS + (_NO_CTF if mode == "provider" else "")
    latest = await db.fetchval(sql, {"t": t, "o": occupant_key})
    if latest is not None:
        return _ok((t - int(latest)) / DAY, lower_bound=False, last_at=int(latest), source=mode)
    if mode == "ctf":
        ne = (cs or {}).get("newest_event_at")
        if ne is not None and int(ne) <= t:
            return _ok((t - int(ne)) / DAY, lower_bound=False, last_at=int(ne), source="ctf_state")
        return _failed("ctf_no_rows")
    resolved = [int(a["resolved_at"]) for a in st.live if a["resolved_at"] is not None]
    if not resolved:
        return _failed("x_handle_unresolved")
    since = max(resolved)                              # conservative: the latest-resolved account
    return _ok(max(0, t - since) / DAY, lower_bound=True, resolved_at=since, source=mode)


async def compute_x_engagement_30d(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    t = parse_ts(as_of)
    st = await account_state(db, occupant_key, t)
    if st.absent:
        return _na(st.absent)
    if not _x_active():
        cs = await ctf_state(db, occupant_key)
        ok, _why = ctf_usable(cs, t)
        return _failed("ctf_no_engagement" if ok and bool(app_config.settings.X_CTF_FALLBACK_METRICS)
                       else "x_inactive")
    bad = await _provider_failure(st, t)
    if bad is not None:
        return bad
    rows = await db.fetchall(
        "SELECT p.like_count, p.repost_count, p.reply_count, p.engagement_observed_at " + _TEAM_POSTS + _ORIGINAL
        + _NO_CTF + " AND p.event_at > :lo", {"t": t, "o": occupant_key, "lo": t - 30 * DAY})
    if not rows:
        return _na("no_posts_30d")
    vals = [(r["like_count"] or 0) + 2 * (r["repost_count"] or 0) + (r["reply_count"] or 0) for r in rows
            if r["engagement_observed_at"] is not None]
    if not vals:
        return _failed("no_engagement_observed", n_posts=len(rows))
    return _ok(float(median(vals)), n_posts=len(vals), lookahead=True)


async def compute_x_followers(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    t = parse_ts(as_of)
    st = await account_state(db, occupant_key, t)
    if st.absent:
        return _absent(st.absent)
    lo = t - snapshot_max_age_s()
    best: int | None = None
    seen = []
    for a in st.live:
        r = await db.fetchone("SELECT followers, observed_at FROM x_account_snapshots WHERE author_id = ? AND "
                              "observed_at <= ? AND observed_at > ? AND followers IS NOT NULL "
                              "ORDER BY observed_at DESC LIMIT 1", (a["author_id"], t, lo))
        if r is not None:
            seen.append(a["handle"])
            best = int(r["followers"]) if best is None else max(best, int(r["followers"]))
    if best is None:
        return _failed("no_fresh_snapshot", handles=[a["handle"] for a in st.live] + st.pending)
    return _ok(best, handles=seen)


async def _x_runs_fresh(db: "Database", jobs: tuple[str, ...], t: int) -> bool:
    ph = ",".join("?" * len(jobs))
    return bool(await db.fetchval(
        f"SELECT 1 FROM collector_runs WHERE job IN ({ph}) AND ok = 1 AND finished_at <= ? AND finished_at > ? "
        "AND (note IS NULL OR note NOT LIKE 'paused:%') LIMIT 1", (*jobs, t, t - POLL_MAX_AGE_S)))


async def compute_x_mentions_7d(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    t = parse_ts(as_of)
    if not _x_active():
        return _failed("x_inactive")
    top = await _job_state(db, "x_mentions_poll", "last_top") or {}
    searched = occupant_key in (top.get("keys") or [])
    if searched:
        ss = await db.fetchone("SELECT last_run_at FROM x_search_state WHERE query_key = ?",
                               (f"mention:{occupant_key}",))
        if ss is None or ss["last_run_at"] is None or int(ss["last_run_at"]) <= t - POLL_MAX_AGE_S:
            return _failed("mention_search_stale")
    elif not await _x_runs_fresh(db, X_POLL_JOBS, t):
        return _failed("x_poll_stale")
    n = await db.fetchval(
        "SELECT COUNT(DISTINCT p.post_id) FROM social_posts p JOIN social_post_mentions m ON m.platform = 'x' AND "
        "m.post_id = p.post_id WHERE p.platform = 'x' AND p.is_team_account = 0 AND p.bot_filtered = 0 AND "
        "p.observed_at <= ? AND p.event_at > ? AND p.event_at <= ? AND m.occupant_key = ? AND m.observed_at <= ?",
        (t, t - 7 * DAY, t, occupant_key, t))
    return _ok(int(n or 0), searched=searched)


async def compute_x_kol_posts_30d(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    t = parse_ts(as_of)
    if not _x_active():
        return _failed("x_inactive")
    n_verified = int(await db.fetchval("SELECT COUNT(*) FROM kol_accounts WHERE verified = 1") or 0)
    if n_verified < MIN_VERIFIED_KOLS:
        return _failed("too_few_kols", verified=n_verified)
    if not await _x_runs_fresh(db, ("x_kol_poll",), t):
        return _failed("kol_poll_stale")
    rows = await db.fetchall(
        "SELECT DISTINCT k.handle FROM social_posts p JOIN social_post_mentions m ON m.platform = 'x' AND "
        "m.post_id = p.post_id JOIN kol_accounts k ON k.handle = p.kol_handle WHERE p.platform = 'x' AND "
        "p.is_kol = 1 AND p.bot_filtered = 0 AND p.observed_at <= ? AND p.event_at > ? AND p.event_at <= ? AND "
        "m.occupant_key = ? AND m.observed_at <= ? AND k.verified = 1 AND k.active = 1 AND k.credibility >= ? AND "
        "NOT EXISTS (SELECT 1 FROM json_each(COALESCE(k.team_occupant_keys, '[]')) j WHERE j.value = ?) "
        "ORDER BY k.handle", (t, t - 30 * DAY, t, occupant_key, t, KOL_MIN_CREDIBILITY, occupant_key))
    return _ok(len(rows), kols=[r["handle"] for r in rows])


PRODUCERS = {
    "x_cadence_30d": compute_x_cadence_30d,
    "x_engagement_30d": compute_x_engagement_30d,
    "x_followers": compute_x_followers,
    "x_last_activity_days": compute_x_last_activity_days,
    "x_mentions_7d": compute_x_mentions_7d,
    "x_kol_posts_30d": compute_x_kol_posts_30d,
}


def register_producers() -> int:
    """§8.3 step 10: one ``register_metric`` per key; nothing for ``announcement_cadence_30d``/``silence_days``.
    Before Phase 8 (no 12 registry) nothing is registered."""
    try:
        from app.features.registry import register_metric  # type: ignore[import-not-found]
    except ImportError:
        return 0
    n = 0
    for key, fn in PRODUCERS.items():
        kw: dict[str, Any] = {"fn": fn, "freshness_days": 3}
        if key in SCORED:
            kw.update(pillar="P5", weight=SCORED[key])
        else:
            kw.update(weight=0.0)
            if key in LEG_OF:
                kw.update(component_of=LEG_OF[key])
        register_metric(key, **kw)
        n += 1
    return n


register_producers()
