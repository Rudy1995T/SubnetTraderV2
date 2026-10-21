"""P5 community producers (spec 08 §2.9, §4.2; 02 §C3/§C5): eight registry keys, three evidence keys.

Every query reads only rows with ``observed_at ≤ as_of`` (point-in-time), and announcements are attributed to an
occupant only through ``social_post_mentions(match_kind='team')`` rows observed by ``as_of``. Absence (→ 0.0) is
decided from census/reference rows first, failure from freshness windows and gateway heartbeats second; nothing is
ever derived from an ``except`` branch. The combiners ``announcement_cadence_30d``/``silence_days`` are spec 12's.
"""
from __future__ import annotations

import json
from datetime import datetime
from statistics import median
from typing import TYPE_CHECKING, Any, Awaitable, Callable

import app.config as app_config
from app.features.types import MetricResult
from app.scoring.spec import GROWTH_BASE_TOLERANCE_DAYS, XPLATFORM_DEDUPE_HOURS
from app.util.time import floor_day, now_ts, parse_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

DAY = 86400
F = 2 * DAY                                     # C6: freshness of every 08 fn metric
SPEC_VERSION = "ts-1.0"
COMMUNITY_KINDS = ("group", "channel", "private_group", "private_channel")
TOPOLOGY_RANK = {"own_server": 5, "main_channel": 4, "main_vanity": 3, "invalid": 2, "handle_only": 1, "none": 0}
PRESENCE_DEFAULTS = {"low": 0.05, "high": 0.35, "inflate_below": 0.02, "inflate_members": 1000}
LEG_OF = {"discord_announcements_30d": "announcement_cadence_30d", "tg_posts_30d": "announcement_cadence_30d",
          "discord_last_activity_days": "silence_days", "tg_last_activity_days": "silence_days"}
ProducerFn = Callable[["Database", str, Any], Awaitable[MetricResult]]


def presence_params() -> dict[str, float]:
    """12's frozen ``presence_ratio`` registry params; the literal defaults before Phase 8 (never duplicated)."""
    try:
        from app.features.registry import METRICS  # type: ignore[import-not-found]
        p = dict(getattr(METRICS["presence_ratio"], "params", {}) or {})
        return {k: float(p.get(k, v)) for k, v in PRESENCE_DEFAULTS.items()}
    except (ImportError, KeyError, AttributeError):
        return dict(PRESENCE_DEFAULTS)


def _ok(v: float, **vj: Any) -> MetricResult:
    return MetricResult(float(v), vj)


def _failed(reason: str, **vj: Any) -> MetricResult:
    return MetricResult(None, {"reason": reason, **vj})


def _na(reason: str, **vj: Any) -> MetricResult:
    return MetricResult(None, {"status": "not_applicable", "reason": reason, **vj})


def _absent(reason: str, **vj: Any) -> MetricResult:
    return MetricResult(0.0, vj, absence_reason=reason)


# ── shared reads ─────────────────────────────────────────────────────────────────────────────────────────────
async def _topology_row(db: "Database", occupant_key: str, t: int) -> dict | None:
    return await db.fetchone(
        "SELECT * FROM discord_snapshots WHERE occupant_key = ? AND is_primary = 1 AND probe IN ('invite','census') "
        "AND observed_at > ? AND observed_at <= ? ORDER BY observed_at DESC, id DESC LIMIT 1",
        (occupant_key, t - F, t))


async def topology_at(db: "Database", occupant_key: str, t: Any) -> tuple[str | None, str | None, dict | None]:
    row = await _topology_row(db, occupant_key, parse_ts(t))
    if row is None:
        return None, None, None
    return row["topology_class"], row["guild_id"], row


async def _samples(db: "Database", occupant_key: str, guild_id: str, lo: int, hi: int, t: int) -> list[dict]:
    """Primary-guild invite samples (one per cycle) with ``observed_at ∈ (lo, min(hi, t)]``."""
    return await db.fetchall(
        "SELECT member_count, presence_count, observed_at FROM discord_snapshots WHERE occupant_key = ? AND "
        "probe = 'invite' AND is_primary = 1 AND guild_id = ? AND invite_valid = 1 AND member_count > 0 AND "
        "observed_at > ? AND observed_at <= ? ORDER BY observed_at", (occupant_key, guild_id, lo, min(hi, t)))


def _ratio(rows: list[dict]) -> tuple[float | None, float | None, float | None]:
    m = [r["member_count"] for r in rows if r["member_count"]]
    p = [r["presence_count"] for r in rows if r["presence_count"] is not None]
    if not m or not p:
        return None, None, None
    mm, pm = float(median(m)), float(median(p))
    return (pm / mm if mm > 0 else None), mm, pm


async def _identity(db: "Database", occupant_key: str, field: str) -> str | None:
    return await db.fetchval("SELECT status FROM identity_resolved WHERE occupant_key = ? AND field = ?",
                             (occupant_key, field))


LIVE_CLAIM_SQL = ("NOT (COALESCE(json_extract(meta_json, '$.declared_by'), json_array(source)) = json_array('legacy'))")


async def _tg_handles(db: "Database", occupant_key: str, t: int) -> list[str]:
    """Declared Telegram handles at ``t`` (legacy-only rows are not live claims, §2.2)."""
    rows = await db.fetchall(
        "SELECT handle FROM watch_channels WHERE occupant_key = ? AND platform = 'telegram' AND first_seen_at <= ? "
        f"AND (retired_at IS NULL OR retired_at > ?) AND {LIVE_CLAIM_SQL}", (occupant_key, t, t))
    return sorted({r["handle"] for r in rows})


async def _tg_latest(db: "Database", occupant_key: str, t: int) -> dict[str, dict]:
    rows = await db.fetchall(
        "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY handle ORDER BY observed_at DESC, id DESC) AS rn "
        "FROM telegram_snapshots WHERE occupant_key = ? AND observed_at > ? AND observed_at <= ?) WHERE rn = 1",
        (occupant_key, t - F, t))
    return {r["handle"]: r for r in rows}


def _tg_reachable(r: dict) -> bool:
    return (r["http_status"] == 200 and r["parse_ok"] == 1 and r["kind"] in COMMUNITY_KINDS
            and (r["members"] or 0) >= 1 and not r["deprecated_marker"])


async def _team_posts(db: "Database", occupant_key: str, platform: str, t: int, lo: int | None = None
                      ) -> list[dict]:
    lo_sql, params = ("AND a.event_at > ? ", (lo,)) if lo is not None else ("", ())
    return await db.fetchall(
        "SELECT a.msg_id, a.text_hash, a.event_at FROM social_post_mentions m JOIN announcements a "
        "ON a.msg_id = m.platform || ':' || m.post_id WHERE m.platform = ? AND m.match_kind = 'team' AND "
        f"m.occupant_key = ? AND m.observed_at <= ? AND a.observed_at <= ? {lo_sql}AND a.event_at <= ? "
        "ORDER BY a.event_at", (platform, occupant_key, t, t, *params, t))


async def _x_team_hashes(db: "Database", occupant_key: str, t: int, lo: int, hi: int) -> dict[str, list[int]]:
    rows = await db.fetchall(
        "SELECT p.text_hash, p.event_at FROM social_posts p WHERE p.platform = 'x' AND p.is_team_account = 1 AND "
        "p.bot_filtered = 0 AND p.observed_at <= ? AND p.event_at > ? AND p.event_at <= ? AND (p.occupant_key = ? OR "
        "EXISTS (SELECT 1 FROM social_post_mentions m WHERE m.platform = 'x' AND m.post_id = p.post_id AND "
        "m.occupant_key = ? AND m.match_kind = 'team' AND m.observed_at <= ?))",
        (t, lo, hi, occupant_key, occupant_key, t))
    out: dict[str, list[int]] = {}
    for r in rows:
        out.setdefault(r["text_hash"], []).append(int(r["event_at"]))
    return out


def _dedupe(rows: list[dict], foreign: dict[str, list[int]], window: int) -> tuple[list[dict], int, int]:
    """Drop rows whose hash equals a foreign post within ±window; equal hashes within window count once."""
    kept: list[dict] = []
    dropped_foreign = dropped_self = 0
    for r in sorted(rows, key=lambda x: x["event_at"]):
        ev = int(r["event_at"])
        if any(abs(ev - e) <= window for e in foreign.get(r["text_hash"], ())):
            dropped_foreign += 1
            continue
        if any(k["text_hash"] == r["text_hash"] and abs(ev - int(k["event_at"])) <= window for k in kept):
            dropped_self += 1
            continue
        kept.append(r)
    return kept, dropped_foreign, dropped_self


async def _gateway_heartbeat_ok(db: "Database", t: int) -> bool:
    return bool(await db.fetchval(
        "SELECT 1 FROM collector_runs WHERE job = 'discord_gateway' AND ok = 1 AND finished_at > ? AND "
        "finished_at <= ? LIMIT 1", (t - F, t)))


async def _followed(db: "Database", occupant_key: str, t: int) -> tuple[bool, int | None]:
    """(followed, follow_since): an operator ``kind='announce'`` channel row, or ≥ 1 guild-mapped announcement."""
    since = await db.fetchval(
        "SELECT MIN(COALESCE(json_extract(meta_json, '$.operator_since'), first_seen_at)) FROM watch_channels "
        "WHERE occupant_key = ? AND platform = 'discord_channel' AND json_extract(meta_json, '$.kind') = 'announce' "
        "AND COALESCE(json_extract(meta_json, '$.operator_since'), first_seen_at) <= ? "
        "AND (retired_at IS NULL OR retired_at > ?)", (occupant_key, t, t))
    if since is not None:
        return True, int(since)
    via_guild = await db.fetchval(
        "SELECT 1 FROM social_post_mentions m JOIN announcements a ON a.msg_id = 'discord:' || m.post_id "
        "WHERE m.platform = 'discord' AND m.match_kind = 'team' AND m.occupant_key = ? AND m.observed_at <= ? "
        "AND a.mapping IN ('guild','guild_ambiguous') LIMIT 1", (occupant_key, t))
    return bool(via_guild), None


async def _tg_channel_state(db: "Database", occupant_key: str, t: int) -> tuple[MetricResult | None, list[dict]]:
    """Fresh public-channel snapshots with a usable preview, or the n/a / failed result explaining why not."""
    handles = await _tg_handles(db, occupant_key, t)
    latest = await _tg_latest(db, occupant_key, t)
    if not handles and not latest:
        return _na("no_telegram"), []
    channels = [r for r in latest.values() if r["parse_ok"] == 1 and r["kind"] == "channel"]
    if not channels:
        unknown = [h for h in handles if h not in latest or latest[h]["parse_ok"] != 1]
        if unknown:
            return _failed("telegram_stale", handles=unknown), []
        return _na("no_public_channel"), []
    usable = [r for r in channels if r["preview_available"] == 1]
    if usable:
        return None, usable
    if all(r["preview_available"] == 0 for r in channels):
        return _na("preview_unavailable"), []
    return _failed("preview_failed"), []


# ── inflation helper ──────────────────────────────────────────────────────────────────────────────────────────
async def inflation_streak(db: "Database", occupant_key: str, as_of: Any) -> dict | None:
    """Consecutive completed UTC days (ending the day before ``as_of``) with r_d < inflate_below over ≥ 2 samples
    and median members > inflate_members. ``None`` when the occupant is not ``own_server``."""
    t = parse_ts(as_of)
    cls, guild, row = await topology_at(db, occupant_key, t)
    if cls != "own_server" or not guild:
        return None
    pp = presence_params()
    D = floor_day(t)
    rows = await _samples(db, occupant_key, guild, D - 61 * DAY, D, t)
    by_day: dict[int, list[dict]] = {}
    for r in rows:
        if int(r["observed_at"]) < D:
            by_day.setdefault(floor_day(int(r["observed_at"])), []).append(r)
    days = 0
    last: tuple[float | None, float | None, float | None] = (None, None, None)
    d = D - DAY
    while True:
        smp = by_day.get(d, [])
        if len(smp) < 2:
            break
        r_d, mm, pm = _ratio(smp)
        if r_d is None or mm is None or not (r_d < pp["inflate_below"] and mm > pp["inflate_members"]):
            break
        if days == 0:
            last = (r_d, mm, pm)
        days += 1
        d -= DAY
    return {"days": days, "streak_start": D - days * DAY, "guild_id": guild, "ratio": last[0], "members": last[1],
            "presence_median": last[2], "guild_created_at": row.get("guild_created_at") if row else None,
            "source_agreement": row.get("source_agreement") if row else None}


async def inflated_days(db: "Database", occupant_key: str, as_of: Any) -> int | None:
    s = await inflation_streak(db, occupant_key, as_of)
    return None if s is None else int(s["days"])


# ── the eight registry producers ──────────────────────────────────────────────────────────────────────────────
async def compute_venue_reachable(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    t = parse_ts(as_of)
    cls, guild, row = await topology_at(db, occupant_key, t)
    handles = await _tg_handles(db, occupant_key, t)
    latest = await _tg_latest(db, occupant_key, t)
    tg = [{"handle": h, "kind": r["kind"], "members": r["members"], "parse_ok": r["parse_ok"]}
          for h, r in sorted(latest.items())]
    vj = {"discord_class": cls, "tg": tg}
    disc_ok = cls in ("own_server", "main_channel") and (row["expires_at"] is None or row["expires_at"] > t)
    tg_ok = any(_tg_reachable(r) for r in latest.values())
    if disc_ok or tg_ok:
        return _ok(1.0, **vj, via="discord" if disc_ok else "telegram")
    if cls is None:
        return _failed("no_census", **vj)
    if cls == "unknown" or (row["unknown_targets"] or 0) > 0:
        return _failed("discord_unknown", **vj, unknown_targets=row["unknown_targets"])
    if cls == "main_vanity" and not app_config.settings.DISCORD_CHANNEL_MAP_REVIEWED:
        return _failed("main_vanity_unreviewed", **vj)
    tg_failed = [h for h in handles if h not in latest or latest[h]["parse_ok"] != 1]
    if tg_failed:
        return _failed("telegram_failed", **vj, handles=tg_failed)
    # every declared venue was freshly and successfully observed and none is reachable (C5)
    tg_ident = await _identity(db, occupant_key, "telegram")
    disc_ident = await _identity(db, occupant_key, "discord")
    if cls == "none" and not handles and disc_ident in (None, "absent", "junk_only") \
            and tg_ident in (None, "absent", "junk_only"):
        return _absent("no_venue", **vj)
    return _absent("no_reachable_venue", **vj)


async def compute_discord_members(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    t = parse_ts(as_of)
    cls, guild, _ = await topology_at(db, occupant_key, t)
    if cls is None or cls == "unknown":
        return _failed("no_census" if cls is None else "discord_unknown")
    if cls != "own_server":
        return _na("not_own_server", discord_class=cls)
    rows = await _samples(db, occupant_key, guild, t - F, t, t)
    if not rows:
        return _failed("primary_not_observed", guild_id=guild)
    r = rows[-1]
    return _ok(r["member_count"], guild_id=guild, observed_at=int(r["observed_at"]))


async def compute_presence_ratio(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    t = parse_ts(as_of)
    cls, guild, _ = await topology_at(db, occupant_key, t)
    if cls is None or cls == "unknown":
        return _failed("no_census" if cls is None else "discord_unknown")
    if cls != "own_server":
        return _na("not_own_server", discord_class=cls)
    rows = await _samples(db, occupant_key, guild, t - DAY, t, t)
    need = int(app_config.settings.DISCORD_PRESENCE_MIN_SAMPLES)
    if len(rows) < need:
        return _failed("insufficient_samples", samples=len(rows), guild_id=guild)
    r, mm, pm = _ratio(rows)
    if r is None:
        return _failed("no_presence", samples=len(rows), guild_id=guild)
    pp = presence_params()
    return _ok(r, members=mm, presence_median=pm, samples=len(rows), guild_id=guild,
               inflated=bool(mm > pp["inflate_members"] and r < pp["inflate_below"]))


async def compute_discord_growth_30d(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    t = parse_ts(as_of)
    cls, guild, _ = await topology_at(db, occupant_key, t)
    if cls is None or cls == "unknown":
        return _failed("no_census" if cls is None else "discord_unknown")
    if cls != "own_server":
        return _na("not_own_server", discord_class=cls)
    now_rows = await _samples(db, occupant_key, guild, t - DAY, t, t)
    if not now_rows:
        return _failed("no_current_sample", guild_id=guild)
    m_now = float(median(r["member_count"] for r in now_rows))
    tol = GROWTH_BASE_TOLERANCE_DAYS
    for k in [30] + [x for i in range(1, tol + 1) for x in (30 - i, 30 + i)]:
        hi = t - k * DAY
        base = await _samples(db, occupant_key, guild, hi - DAY, hi, t)
        if base:
            m_then = float(median(r["member_count"] for r in base))
            if m_then <= 0:
                return _failed("zero_base", guild_id=guild)
            return _ok((m_now - m_then) / m_then, m_now=m_now, m_then=m_then, base_day=k, guild_id=guild)
    return _failed("no_base_sample", guild_id=guild)


async def compute_discord_announcements_30d(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    t = parse_ts(as_of)
    followed, since = await _followed(db, occupant_key, t)
    if not followed:
        return _na("no_followed_channel")
    if not await _gateway_heartbeat_ok(db, t):
        return _failed("gateway_stale")
    w = XPLATFORM_DEDUPE_HOURS * 3600
    rows = await _team_posts(db, occupant_key, "discord", t, lo=t - 30 * DAY)
    xh = await _x_team_hashes(db, occupant_key, t, t - 30 * DAY - w, t + w)
    kept, dx, ds = _dedupe(rows, xh, w)
    return _ok(len(kept), dedup_x=dx, dedup_self=ds, partial_window=bool(since and since > t - 30 * DAY))


async def compute_tg_posts_30d(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    t = parse_ts(as_of)
    bad, channels = await _tg_channel_state(db, occupant_key, t)
    if bad is not None:
        return bad
    w = XPLATFORM_DEDUPE_HOURS * 3600
    rows = await _team_posts(db, occupant_key, "telegram", t, lo=t - 30 * DAY)
    foreign = await _x_team_hashes(db, occupant_key, t, t - 30 * DAY - w, t + w)
    for r in await _team_posts(db, occupant_key, "discord", t, lo=t - 30 * DAY - w):
        foreign.setdefault(r["text_hash"], []).append(int(r["event_at"]))
    kept, dx, ds = _dedupe(rows, foreign, w)
    return _ok(len(kept), dedup_x=dx, dedup_self=ds, handles=[r["handle"] for r in channels])


async def compute_discord_last_activity_days(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    t = parse_ts(as_of)
    followed, since = await _followed(db, occupant_key, t)
    if not followed:
        return _na("no_followed_channel")
    if not await _gateway_heartbeat_ok(db, t):
        return _failed("gateway_stale")
    latest = await db.fetchval(
        "SELECT MAX(a.event_at) FROM social_post_mentions m JOIN announcements a ON a.msg_id = 'discord:' || "
        "m.post_id WHERE m.platform = 'discord' AND m.match_kind = 'team' AND m.occupant_key = ? AND "
        "m.observed_at <= ? AND a.observed_at <= ? AND a.event_at <= ?", (occupant_key, t, t, t))
    if latest is not None:
        return _ok((t - int(latest)) / DAY, lower_bound=False, last_at=int(latest))
    if since is None:
        return _failed("no_follow_since")
    return _ok((t - since) / DAY, lower_bound=True, follow_since=since)


async def compute_tg_last_activity_days(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    t = parse_ts(as_of)
    bad, channels = await _tg_channel_state(db, occupant_key, t)
    if bad is not None:
        return bad
    handles = [r["handle"] for r in channels]
    latest = await db.fetchval(
        "SELECT MAX(a.event_at) FROM social_post_mentions m JOIN announcements a ON a.msg_id = 'telegram:' || "
        "m.post_id WHERE m.platform = 'telegram' AND m.match_kind = 'team' AND m.occupant_key = ? AND "
        "m.observed_at <= ? AND a.observed_at <= ? AND a.event_at <= ?", (occupant_key, t, t, t))
    ph = ",".join("?" * len(handles))
    snap = await db.fetchone(
        f"SELECT MAX(last_post_at) AS lp, MIN(observed_at) AS first_obs FROM telegram_snapshots WHERE occupant_key = ? "
        f"AND handle IN ({ph}) AND observed_at <= ?", (occupant_key, *handles, t))
    cands = [int(x) for x in (latest, snap["lp"] if snap else None) if x is not None and int(x) <= t]
    if cands:
        last = max(cands)
        return _ok((t - last) / DAY, lower_bound=False, last_at=last, handles=handles)
    first = snap["first_obs"] if snap else None
    if first is None:
        return _failed("no_snapshot")
    return _ok((t - int(first)) / DAY, lower_bound=True, observed_since=int(first), handles=handles)


# ── evidence-only keys (03 catalog, never in 12's registry, C3) ───────────────────────────────────────────────
async def compute_discord_topology(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    cls, guild, row = await topology_at(db, occupant_key, parse_ts(as_of))
    if cls is None:
        return _failed("no_census")
    vj = {"class": cls, "guild_id": guild, "source_agreement": row["source_agreement"],
          "unknown_targets": row["unknown_targets"]}
    if cls not in TOPOLOGY_RANK:
        return _failed("unknown", **vj)
    return _ok(TOPOLOGY_RANK[cls], **vj)


async def compute_tg_members(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    latest = await _tg_latest(db, occupant_key, parse_ts(as_of))
    comm = {h: r["members"] for h, r in latest.items() if r["parse_ok"] == 1 and r["kind"] in COMMUNITY_KINDS
            and r["members"] is not None}
    if not comm:
        return _na("no_telegram_community")
    h = max(comm, key=lambda k: comm[k])
    return _ok(comm[h], handle_count=len(comm))


async def compute_inflated_days(db: "Database", occupant_key: str, as_of: Any) -> MetricResult:
    s = await inflation_streak(db, occupant_key, as_of)
    if s is None:
        return _na("not_own_server")
    return _ok(s["days"], guild_id=s["guild_id"], ratio=s["ratio"], members=s["members"])


PRODUCERS: dict[str, ProducerFn] = {
    "venue_reachable": compute_venue_reachable, "discord_members": compute_discord_members,
    "presence_ratio": compute_presence_ratio, "discord_growth_30d": compute_discord_growth_30d,
    "discord_announcements_30d": compute_discord_announcements_30d, "tg_posts_30d": compute_tg_posts_30d,
    "discord_last_activity_days": compute_discord_last_activity_days,
    "tg_last_activity_days": compute_tg_last_activity_days,
}
EVIDENCE: dict[str, ProducerFn] = {"discord_topology": compute_discord_topology, "tg_members": compute_tg_members,
                                   "inflated_days": compute_inflated_days}


def register_producers() -> int:
    """C5: one ``register_metric`` per registry key; before Phase 8 (no 12 registry) nothing to register."""
    try:
        from app.features.registry import register_metric  # type: ignore[import-not-found]
    except ImportError:
        return 0
    n = 0
    for key, fn in PRODUCERS.items():
        kw: dict[str, Any] = {"fn": fn, "pillar": "P5", "freshness_days": 2}
        if key in LEG_OF:
            kw.update(weight=0.0, component_of=LEG_OF[key])
        register_metric(key, **kw)
        n += 1
    return n


def to_raw_row(key: str, res: MetricResult, *, occupant_key: str, as_of: int, observed_at: int, run_id: str,
               backfilled: bool = False) -> dict:
    vj = res.mapped_json()
    st = vj["status"]
    return {"occupant_key": occupant_key, "metric_key": key, "as_of": int(as_of), "spec_version": SPEC_VERSION,
            "stage": "raw", "value": res.value if st in ("ok", "absent") else None, "value_json": vj,
            "observed_at": int(observed_at), "source": "community", "run_id": run_id,
            "backfilled": 1 if backfilled else 0}


async def write_evidence(db: "Database", occupant_keys: list[str], as_of: int, run_id: str,
                         now: int | None = None) -> int:
    """``discord_6h``: the three evidence keys evaluated at ``now`` and stored at ``as_of = cycle_at`` (B4)."""
    from app.storage.pit import put_metric_values
    t = int(now if now is not None else now_ts())
    rows = []
    for okey in occupant_keys:
        for key, fn in EVIDENCE.items():
            rows.append(to_raw_row(key, await fn(db, okey, t), occupant_key=okey, as_of=as_of, observed_at=t,
                                   run_id=run_id))
    return await put_metric_values(db, rows)


async def community_metrics(db: "Database", occupant_key: str, as_of: Any, *, evidence: bool = True
                            ) -> list[tuple[str, MetricResult]]:
    out = [(k, await fn(db, occupant_key, as_of)) for k, fn in PRODUCERS.items()]
    if evidence:
        out += [(k, await fn(db, occupant_key, as_of)) for k, fn in EVIDENCE.items()]
    return out


def dumps(res: MetricResult) -> str:
    return json.dumps({"value": res.value, **res.mapped_json()}, default=str, sort_keys=True)


def as_datetime(ts: int) -> datetime:
    from datetime import timezone
    return datetime.fromtimestamp(int(ts), timezone.utc)


register_producers()
