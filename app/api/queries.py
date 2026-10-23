"""Every SELECT behind the API, Telegram and the exports (spec 14 §8.3). Pure ``(db, …) -> rows``; no FastAPI.

Stored times stay INTEGER unix seconds here (02 §B1); routers and Telegram formatters convert at the boundary.
"""
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from app.api.common import jloads

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

DAY = 86400
_SCORE_COLS = ("s.id, s.occupant_key, s.netuid, sub.name, s.status, s.as_of, s.computed_at, s.run_kind, s.quality, "
               "s.velocity, s.composite_smoothed, s.composite_raw, s.confidence, s.insufficient_data, s.tier, "
               "s.red_flags, s.pillar_scores, s.regime, s.spec_version")


def _score(r: dict) -> dict:
    r = dict(r)
    r["red_flags"] = jloads(r.get("red_flags"), []) or []
    r["pillar_scores"] = jloads(r.get("pillar_scores"), {}) or {}
    return r


# ── feed ─────────────────────────────────────────────────────────────────────────────────────────────────────
async def latest_scores(db: "Database", *, spec: str, min_confidence: float = 0.0, tier: str | None = None,
                        include_prelaunch: bool = False, include_insufficient: bool = False,
                        netuids: list[int] | None = None) -> list[dict]:
    """Latest ``scores`` row per **current** occupant for ``spec`` (02 §H1), sorted composite desc, nulls last.

    Starts from ``subnets`` and takes each occupant's newest non-shadow row through ``ix_scores_latest`` (one table
    row per occupant). A ``MAX(as_of) … GROUP BY`` over ``scores`` had to read every row for ``shadow`` — 52 k rows
    of ≈ 7 KB ``explain`` after the 2026-10-23 backfill, 107 s on a cold cache (BUILD-LOG 2026-10-23 N3)."""
    sql = (f"SELECT {_SCORE_COLS} FROM subnets sub "
           f"JOIN scores s ON s.id = (SELECT s2.id FROM scores s2 INDEXED BY ix_scores_latest "
           f"WHERE s2.spec_version = :spec AND s2.occupant_key = sub.occupant_key AND s2.shadow = 0 "
           f"ORDER BY s2.as_of DESC LIMIT 1) "
           f"WHERE 1 = 1 "
           f"AND (:pre = 1 OR s.status = 'active') AND s.confidence >= :minc "
           f"AND (:ins = 1 OR s.confidence >= 0.4) AND (:tier IS NULL OR s.tier = :tier)")
    params: dict[str, Any] = {"spec": spec, "pre": int(include_prelaunch), "minc": float(min_confidence),
                              "ins": int(include_insufficient), "tier": tier}
    if netuids:
        ph = ",".join(f":n{i}" for i in range(len(netuids)))
        sql += f" AND s.netuid IN ({ph})"
        params.update({f"n{i}": int(n) for i, n in enumerate(netuids)})
    sql += " ORDER BY s.composite_smoothed IS NULL, s.composite_smoothed DESC, s.netuid"
    return [_score(r) for r in await db.fetchall(sql, params)]


async def feed_meta(db: "Database", *, spec: str) -> dict:
    """``{max_as_of, max_id, computed_at, run_kind, regime}`` of the newest non-shadow score row of ``spec``."""
    r = await db.fetchone("SELECT as_of, computed_at, run_kind, regime FROM scores WHERE spec_version = ? AND shadow = 0 "
                          "ORDER BY as_of DESC, id DESC LIMIT 1", (spec,))
    mid = await db.fetchval("SELECT MAX(id) FROM scores WHERE spec_version = ?", (spec,))
    out = {"max_as_of": None, "computed_at": None, "run_kind": None, "regime": None, "max_id": mid}
    if r:
        out.update(max_as_of=r["as_of"], computed_at=r["computed_at"], run_kind=r["run_kind"], regime=r["regime"])
    return out


async def score_history(db: "Database", *, occupant_key: str, spec: str, days: int, official_only: bool,
                        now: int) -> list[dict]:
    rows = await db.fetchall(
        "SELECT s.id, s.occupant_key, s.netuid, s.as_of, s.computed_at, s.run_kind, s.quality, s.velocity, "
        "s.composite_smoothed, s.composite_raw, s.confidence, s.insufficient_data, s.tier, s.red_flags, "
        "s.pillar_scores, s.regime, s.status, s.spec_version, NULL AS name FROM scores s "
        "WHERE s.occupant_key = ? AND s.spec_version = ? AND s.shadow = 0 AND s.as_of >= ? "
        "AND (? = 0 OR s.run_kind = 'official') ORDER BY s.as_of",
        (occupant_key, spec, int(now) - int(days) * DAY, int(official_only)))
    return [_score(r) for r in rows]


async def resolve_occupant(db: "Database", ident: str | int) -> tuple[str, int] | None:
    """A bare netuid → its *current* occupant (``subnets``); an ``occupant_key`` → that occupant (any status)."""
    s = str(ident).strip()
    if s.isdigit():
        r = await db.fetchone("SELECT occupant_key, netuid FROM subnets WHERE netuid = ?", (int(s),))
    else:
        r = await db.fetchone("SELECT occupant_key, netuid FROM occupants WHERE occupant_key = ?", (s,))
    return (r["occupant_key"], int(r["netuid"])) if r else None


# ── matrix ───────────────────────────────────────────────────────────────────────────────────────────────────
async def subnet_rows(db: "Database", *, include_prelaunch: bool, include_deregistered: bool) -> list[dict]:
    statuses = ["active"] + (["pre_launch"] if include_prelaunch else []) + \
        (["deregistered"] if include_deregistered else [])
    ph = ",".join("?" * len(statuses))
    return await db.fetchall(
        "SELECT netuid, occupant_key, network_registered_at, name, symbol, status, immune, owner_coldkey, head_block, "
        "website_url, github_url, docs_url, discord_url, telegram_url, x_handle FROM subnets "
        f"WHERE netuid > 0 AND status IN ({ph}) ORDER BY netuid", statuses)


async def spark_7d(db: "Database", *, spec: str) -> dict[str, list[float]]:
    """Last 7 official composites per ``subnets`` occupant (the only keys ``/subnets`` looks up), 7 index seeks each;
    a window over every official row read the whole table (10.8 s after the 2026-10-23 backfill)."""
    rows = await db.fetchall(
        "SELECT s.occupant_key, s.as_of, s.composite_smoothed FROM subnets sub JOIN scores s ON s.id IN ("
        "SELECT s2.id FROM scores s2 INDEXED BY ix_scores_occ WHERE s2.occupant_key = sub.occupant_key "
        "AND s2.spec_version = ? AND s2.run_kind = 'official' AND s2.shadow = 0 AND s2.composite_smoothed IS NOT NULL "
        "ORDER BY s2.as_of DESC LIMIT 7) ORDER BY s.occupant_key, s.as_of", (spec,))
    out: dict[str, list[float]] = {}
    for r in rows:
        out.setdefault(r["occupant_key"], []).append(round(float(r["composite_smoothed"]), 2))
    return out


# ── subnet detail ────────────────────────────────────────────────────────────────────────────────────────────
async def occupant_info(db: "Database", occupant_key: str) -> dict | None:
    """Occupant + (when current) its ``subnets`` row."""
    o = await db.fetchone("SELECT * FROM occupants WHERE occupant_key = ?", (occupant_key,))
    if o is None:
        return None
    sub = await db.fetchone("SELECT * FROM subnets WHERE occupant_key = ?", (occupant_key,))
    out = dict(o)
    out["current"] = sub is not None
    out["sub"] = dict(sub) if sub else None
    return out


async def latest_score_row(db: "Database", *, occupant_key: str, spec: str) -> dict | None:
    r = await db.fetchone("SELECT * FROM scores WHERE occupant_key = ? AND spec_version = ? AND shadow = 0 "
                          "ORDER BY as_of DESC LIMIT 1", (occupant_key, spec))
    if r is None:
        return None
    r = _score(r)
    r["pillar_coverage"] = jloads(r.get("pillar_coverage"), {}) or {}
    r["explain_raw"] = r.get("explain")
    r["explain"] = jloads(r.get("explain"), None)
    return r


async def evidence(db: "Database", *, occupant_key: str, spec: str, as_of: int) -> list[dict]:
    """``stage='norm'`` rows of that slot (02 §H2) in 12's registry order, + the raw row's ``evidence_url``."""
    from app.features.registry import METRICS, PILLARS, scored
    rows = await db.fetchall("SELECT metric_key, value, value_json, observed_at FROM metric_values "
                             "WHERE occupant_key = ? AND stage = 'norm' AND spec_version = ? AND as_of = ?",
                             (occupant_key, spec, int(as_of)))
    by_key = {r["metric_key"]: r for r in rows}
    order = [d.key for p in PILLARS for d in scored(p)]
    keys = [k for k in order if k in by_key] + sorted(k for k in by_key if k not in order)
    out: list[dict] = []
    wanted: dict[tuple[str, int], int] = {}
    for k in keys:
        r = by_key[k]
        vj = jloads(r["value_json"], {}) or {}
        d = METRICS.get(k)
        raw_key = (d.fallback_key if (d and vj.get("fallback") and d.fallback_key) else k)
        e = {"metric_key": k, "pillar": d.pillar if d else None, "m": r["value"], "raw": vj.get("raw"),
             "raw_status": vj.get("status") or "none", "reason": vj.get("reason"), "raw_as_of": vj.get("raw_as_of"),
             "age_s": vj.get("age_s"), "freshness_s": d.freshness_s if d else None,
             "fallback": bool(vj.get("fallback")), "src": vj.get("src"), "evidence_url": None,
             "_raw_key": raw_key}
        out.append(e)
        if vj.get("raw_as_of") is not None:
            wanted[(raw_key, int(vj["raw_as_of"]))] = len(out) - 1
    if wanted:
        ks = sorted({k for k, _ in wanted})
        ts = sorted({t for _, t in wanted})
        rows = await db.fetchall(
            f"SELECT metric_key, as_of, json_extract(value_json, '$.evidence_url') AS u FROM metric_values "
            f"WHERE occupant_key = ? AND stage = 'raw' AND metric_key IN ({','.join('?' * len(ks))}) "
            f"AND as_of IN ({','.join('?' * len(ts))}) AND json_extract(value_json, '$.evidence_url') IS NOT NULL",
            (occupant_key, *ks, *ts))
        for r in rows:
            i = wanted.get((r["metric_key"], int(r["as_of"])))
            if i is not None:
                out[i]["evidence_url"] = r["u"]
    for e in out:
        e.pop("_raw_key", None)
    return out


async def occupant_events(db: "Database", *, occupant_key: str, limit: int) -> list[dict]:
    rows = await db.fetchall("SELECT id, netuid, occupant_key, event_type, severity, event_at, observed_at, source, "
                             "payload, alerted_at FROM events WHERE occupant_key = ? "
                             "ORDER BY observed_at DESC, id DESC LIMIT ?", (occupant_key, int(limit)))
    for r in rows:
        r["payload"] = jloads(r["payload"], {})
    return rows


async def site_state(db: "Database", *, occupant_key: str) -> dict | None:
    r = await db.fetchone("SELECT event_type, observed_at, event_at, payload FROM events WHERE occupant_key = ? "
                          "AND event_type IN ('site_down_7d','site_recovered') ORDER BY id DESC LIMIT 1",
                          (occupant_key,))
    if r is None:
        return None
    p = jloads(r["payload"], {}) or {}
    return {"url": p.get("url"), "last_event": r["event_type"],
            "reason": (p.get("reason") or "up") if r["event_type"] == "site_recovered" else "down",
            "at": r["event_at"] or r["observed_at"]}


async def manual_state(db: "Database", *, occupant_key: str, notes_limit: int = 20) -> dict:
    disc = await db.fetchone("SELECT value, created_at, created_by, evidence_url FROM manual_notes WHERE "
                             "occupant_key = ? AND kind = 'team_disclosure' ORDER BY created_at DESC, id DESC LIMIT 1",
                             (occupant_key,))
    prod = await db.fetchone("SELECT value, evidence_url, body, created_at, created_by FROM manual_notes WHERE "
                             "occupant_key = ? AND kind = 'product_status' ORDER BY created_at DESC, id DESC LIMIT 1",
                             (occupant_key,))
    notes = await db.fetchall("SELECT id, body, evidence_url, created_at, created_by FROM manual_notes WHERE "
                              "occupant_key = ? AND kind = 'note' ORDER BY created_at DESC, id DESC LIMIT ?",
                              (occupant_key, int(notes_limit)))
    flags = await db.fetchall("SELECT m.flag, m.state, m.evidence_url, m.note, m.set_at, m.set_by FROM manual_flags m "
                              "WHERE m.occupant_key = ? AND m.id = (SELECT id FROM manual_flags x WHERE "
                              "x.occupant_key = m.occupant_key AND x.flag = m.flag ORDER BY set_at DESC, id DESC "
                              "LIMIT 1) ORDER BY m.flag", (occupant_key,))
    return {"disclosure": dict(disc) if disc else None, "product": dict(prod) if prod else None,
            "notes": notes, "manual_flags": flags}


# ── events ───────────────────────────────────────────────────────────────────────────────────────────────────
async def events_query(db: "Database", *, since: int, until: int | None = None, severities: set[str] | None = None,
                       types: set[str] | None = None, netuid: int | None = None, occupant_key: str | None = None,
                       limit: int = 200, before_id: int | None = None) -> list[dict]:
    where, args = ["e.observed_at >= ?"], [int(since)]
    if until is not None:
        where.append("e.observed_at <= ?")
        args.append(int(until))
    if severities:
        where.append(f"e.severity IN ({','.join('?' * len(severities))})")
        args += sorted(severities)
    if types:
        where.append(f"e.event_type IN ({','.join('?' * len(types))})")
        args += sorted(types)
    if netuid is not None:
        where.append("e.netuid = ?")
        args.append(int(netuid))
    if occupant_key:
        where.append("e.occupant_key = ?")
        args.append(occupant_key)
    if before_id is not None:
        where.append("e.id < ?")
        args.append(int(before_id))
    rows = await db.fetchall(
        "SELECT e.id, e.netuid, e.occupant_key, o.name, e.event_type, e.severity, e.source, e.event_at, "
        "e.observed_at, e.payload, e.alerted_at FROM events e LEFT JOIN occupants o ON o.occupant_key = e.occupant_key "
        f"WHERE {' AND '.join(where)} ORDER BY e.id DESC LIMIT ?", (*args, int(limit)))
    for r in rows:
        r["payload"] = jloads(r["payload"], {})
    return rows


# ── flags, movers ────────────────────────────────────────────────────────────────────────────────────────────
async def active_flags(db: "Database", *, spec: str) -> list[dict]:
    from app.storage.catalog import RED_FLAGS
    rows = await db.fetchall(
        "SELECT f.occupant_key, f.flag, f.raised_at, f.false_since, f.human, f.sticky, f.last_eval_as_of, "
        "s.netuid, s.name FROM red_flag_state f JOIN subnets s ON s.occupant_key = f.occupant_key "
        "WHERE f.active = 1 AND f.spec_version = ? ORDER BY s.netuid, f.flag", (spec,))
    for r in rows:
        r["multiplier"] = RED_FLAGS.get(r["flag"])
        r["state"] = "clearing" if r["false_since"] is not None else "active"
        r["origin"] = "human" if r["human"] else ("sticky" if r["sticky"] else "auto")
    return rows


async def flag_is_active(db: "Database", *, occupant_key: str, flag: str, spec: str) -> bool:
    """02 §D5: active in ``red_flag_state`` for ``spec``, or the latest ``manual_flags`` row is ``confirmed``."""
    if await db.fetchval("SELECT 1 FROM red_flag_state WHERE occupant_key = ? AND flag = ? AND spec_version = ? "
                         "AND active = 1", (occupant_key, flag, spec), writer=True):
        return True
    st = await db.fetchval("SELECT state FROM manual_flags WHERE occupant_key = ? AND flag = ? "
                           "ORDER BY set_at DESC, id DESC LIMIT 1", (occupant_key, flag), writer=True)
    return st == "confirmed"


async def movers(db: "Database", *, spec: str, days: int, n: int, now: int) -> tuple[list[dict], list[dict]]:
    """Official ``composite_smoothed`` change of current occupants: latest vs the newest row ≤ ``now − days``."""
    rows = await db.fetchall(
        "WITH cur AS (SELECT occupant_key, composite_smoothed AS c, as_of FROM (SELECT occupant_key, "
        "composite_smoothed, as_of, ROW_NUMBER() OVER (PARTITION BY occupant_key ORDER BY as_of DESC) rn FROM scores "
        "WHERE spec_version = :spec AND shadow = 0 AND run_kind = 'official') WHERE rn = 1), "
        "old AS (SELECT occupant_key, composite_smoothed AS c FROM (SELECT occupant_key, composite_smoothed, "
        "ROW_NUMBER() OVER (PARTITION BY occupant_key ORDER BY as_of DESC) rn FROM scores WHERE spec_version = :spec "
        "AND shadow = 0 AND run_kind = 'official' AND as_of <= :cut) WHERE rn = 1) "
        "SELECT s.netuid, s.name, cur.occupant_key, old.c AS c_from, cur.c AS c_to, cur.c - old.c AS delta "
        "FROM cur JOIN old USING (occupant_key) JOIN subnets s ON s.occupant_key = cur.occupant_key "
        "WHERE cur.c IS NOT NULL AND old.c IS NOT NULL",
        {"spec": spec, "cut": int(now) - int(days) * DAY})
    up = sorted((r for r in rows if r["delta"] > 0), key=lambda r: (-r["delta"], r["netuid"]))[:n]
    down = sorted((r for r in rows if r["delta"] < 0), key=lambda r: (r["delta"], r["netuid"]))[:n]
    return up, down


async def first_official_as_of(db: "Database", *, spec: str) -> int | None:
    """Slot of the oldest official non-shadow score row of ``spec`` (the movers screen explains an empty window)."""
    return await db.fetchval("SELECT MIN(as_of) FROM scores WHERE spec_version = ? AND shadow = 0 "
                             "AND run_kind = 'official'", (spec,))


async def subnet_names(db: "Database") -> dict[int, str]:
    """Current occupant name per netuid (14 §8.4a: labels for the terminal's trade list)."""
    return {int(r["netuid"]): r["name"] for r in await db.fetchall("SELECT netuid, name FROM subnets")
            if r["name"]}


# ── market brief (§2.8) and `python -m app status` ───────────────────────────────────────────────────────────
async def scores_at(db: "Database", *, spec: str, as_of: int) -> dict[str, dict]:
    """``occupant_key → row`` of the non-shadow ``scores`` at exactly ``as_of`` for current active occupants."""
    rows = await db.fetchall(
        f"SELECT {_SCORE_COLS} FROM scores s JOIN subnets sub ON sub.occupant_key = s.occupant_key "
        "WHERE s.spec_version = ? AND s.shadow = 0 AND s.as_of = ? AND s.status = 'active'", (spec, int(as_of)))
    return {r["occupant_key"]: _score(r) for r in rows}


async def flag_changes(db: "Database", *, spec: str, after: int, upto: int) -> tuple[list[dict], list[dict]]:
    """(raised, cleared) ``red_flag_state`` rows of current occupants with ``raised_at``/``cleared_at`` ∈ (after, upto]."""
    rows = await db.fetchall(
        "SELECT f.flag, f.raised_at, f.cleared_at, s.netuid, s.name FROM red_flag_state f "
        "JOIN subnets s ON s.occupant_key = f.occupant_key WHERE f.spec_version = ? "
        "AND ((f.raised_at > ? AND f.raised_at <= ?) OR (f.cleared_at > ? AND f.cleared_at <= ?)) ORDER BY s.netuid, f.flag",
        (spec, int(after), int(upto), int(after), int(upto)))
    raised = [r for r in rows if r["raised_at"] is not None and after < r["raised_at"] <= upto]
    cleared = [r for r in rows if r["cleared_at"] is not None and after < r["cleared_at"] <= upto]
    return raised, cleared


async def days_to_cull(db: "Database", *, now: int) -> tuple[int | None, list[dict]]:
    """Newest ``as_of ≤ now`` of 19 §2.4's ``days_to_cull_static`` and its ``ok`` rows for current active occupants."""
    as_of = await db.fetchval("SELECT MAX(as_of) FROM metric_values WHERE metric_key = 'days_to_cull_static' "
                              "AND stage = 'raw' AND as_of <= ?", (int(now),))
    if as_of is None:
        return None, []
    rows = await db.fetchall(
        "SELECT m.value, s.netuid, s.name FROM metric_values m JOIN subnets s ON s.occupant_key = m.occupant_key "
        "WHERE m.metric_key = 'days_to_cull_static' AND m.stage = 'raw' AND m.as_of = ? AND m.status = 'ok' "
        "AND m.value IS NOT NULL AND s.status = 'active' ORDER BY m.value, s.netuid", (int(as_of),))
    return int(as_of), rows


async def wb_network_latest(db: "Database") -> dict | None:
    return await db.fetchone("SELECT hour_ts, eta_next_registration_days, k_30, observed_at FROM wb_dereg_network_hourly "
                             "ORDER BY hour_ts DESC LIMIT 1")


async def norm_coverage(db: "Database", *, spec: str, as_of: int) -> tuple[int, dict[str, int]]:
    """(active occupants scored at the slot, ``metric_key → non-null stage='norm' values`` at the slot)."""
    n = await db.fetchval("SELECT COUNT(*) FROM scores WHERE spec_version = ? AND shadow = 0 AND as_of = ? "
                          "AND status = 'active'", (spec, int(as_of)))
    rows = await db.fetchall(
        "SELECT metric_key, SUM(value IS NOT NULL) AS v FROM metric_values INDEXED BY ix_metric_values_slot "
        "WHERE stage = 'norm' AND spec_version = ? AND as_of = ? GROUP BY metric_key", (spec, int(as_of)))
    return int(n or 0), {r["metric_key"]: int(r["v"] or 0) for r in rows}


def populated_by_pillar(n_scored: int, cov: dict[str, int]) -> dict[str, tuple[int, int]]:
    """``pillar → (populated, scored keys)``; populated = non-null for at least half the slot's scored occupants."""
    from app.features.registry import PILLARS, scored
    out: dict[str, tuple[int, int]] = {}
    for p in PILLARS:
        keys = [d.key for d in scored(p)]
        out[p] = (sum(1 for k in keys if n_scored and cov.get(k, 0) * 2 >= n_scored), len(keys))
    return out


# ── needs-review buckets (§8.3) ──────────────────────────────────────────────────────────────────────────────
async def needs_review(db: "Database", *, now: int, blocked_days: int = 7, spec: str | None = None) -> dict[str, list]:
    import app.config as app_config
    spec = spec or app_config.settings.SPEC_VERSION
    out: dict[str, list] = {}
    try:
        from app.registry import compute_needs_review
        from app.registry.review import REASONS
        items = await compute_needs_review(db, int(now))
        by: dict[str, list[dict]] = {}
        for it in items:
            d = it.as_dict() if hasattr(it, "as_dict") else dict(it)
            by.setdefault(d["reason"], []).append(d)
        for reason in [*REASONS, *sorted(r for r in by if r not in REASONS)]:
            if by.get(reason):
                out[f"registry.{reason}"] = by[reason]
    except Exception as exc:
        out["registry.error"] = [{"detail": f"{type(exc).__name__}: {exc}"[:200]}]
    try:
        from app.integrity import review as review11
        q = await review11.list_open(db, limit=500)
        for it in q:
            out.setdefault("queue", []).append(it)
    except Exception as exc:
        out["queue.error"] = [{"detail": f"{type(exc).__name__}: {exc}"[:200]}]
    cand = await db.fetchall("SELECT s.netuid, COUNT(*) AS n FROM team_members t JOIN subnets s ON s.occupant_key = "
                             "t.occupant_key WHERE t.status = 'candidate' AND s.status = 'active' GROUP BY s.netuid "
                             "ORDER BY n DESC, s.netuid")
    if cand:
        out["team_candidates"] = cand
    unk = await db.fetchall(
        "SELECT s.netuid, json_extract(m.value_json, '$.status') AS st FROM subnets s JOIN metric_values m "
        "ON m.occupant_key = s.occupant_key AND m.metric_key = 'team_doxxed' AND m.stage = 'norm' "
        "AND m.spec_version = ? AND m.as_of = (SELECT MAX(as_of) FROM metric_values x WHERE x.occupant_key = "
        "s.occupant_key AND x.metric_key = 'team_doxxed' AND x.stage = 'norm' AND x.spec_version = ?) "
        "WHERE s.status = 'active' AND COALESCE(json_extract(m.value_json, '$.status'), 'none') IN ('failed','none') "
        "ORDER BY s.netuid", (spec, spec))
    if unk:
        out["team_unknown"] = unk
    prod = await db.fetchall(
        "SELECT s.netuid FROM subnets s WHERE s.status = 'active' AND s.netuid > 0 AND (SELECT value FROM manual_notes "
        "n WHERE n.occupant_key = s.occupant_key AND n.kind = 'product_status' ORDER BY created_at DESC, id DESC "
        "LIMIT 1) IS NULL ORDER BY s.netuid")
    if prod:
        out["product_unset"] = prod
    blocked = await blocked_sites(db, now=now, days=blocked_days)
    if blocked:
        out["blocked_sites"] = blocked
    return out


async def blocked_sites(db: "Database", *, now: int, days: int) -> list[dict]:
    """Primary website with a row on each of the last ``days`` check days, all ``bot_challenge`` or robots-denied."""
    today = int(now) // DAY * DAY
    start = today - (days - 1) * DAY
    rows = await db.fetchall(
        "SELECT s.netuid, w.check_day_ts, w.status_class, w.robots_allowed FROM web_checks w JOIN subnets s "
        "ON s.occupant_key = w.occupant_key WHERE s.status = 'active' AND w.kind = 'website' AND w.is_primary = 1 "
        "AND w.check_day_ts >= ? AND w.check_day_ts <= ?", (start, today))
    per: dict[int, dict[int, list[dict]]] = {}
    for r in rows:
        per.setdefault(r["netuid"], {}).setdefault(int(r["check_day_ts"]), []).append(r)
    out = []
    for n, by_day in sorted(per.items()):
        if len(by_day) < days:
            continue
        flat = [r for rs in by_day.values() for r in rs]
        if all(r["status_class"] == "bot_challenge" or r["robots_allowed"] == 0 for r in flat):
            why = "bot_challenge" if all(r["status_class"] == "bot_challenge" for r in flat) else "robots"
            out.append({"netuid": n, "reason": why})
    return out


# ── digest ───────────────────────────────────────────────────────────────────────────────────────────────────
async def digest_events(db: "Database", *, after_id: int, upto_id: int) -> list[dict]:
    rows = await db.fetchall(
        "SELECT e.id, e.netuid, e.occupant_key, o.name AS _name, e.event_type, e.severity, e.event_at, e.observed_at, "
        "e.source, e.payload FROM events e LEFT JOIN occupants o ON o.occupant_key = e.occupant_key "
        "WHERE e.id > ? AND e.id <= ? AND (json_extract(e.payload, '$.alert_route') IN ('digest','digest_late') "
        "OR json_extract(e.payload, '$.backfilled') = 1) ORDER BY e.id", (int(after_id), int(upto_id)))
    return rows


async def digest_upto(db: "Database") -> int:
    """``min(MAX(events.id), MIN(unalerted id) − 1)`` — never skip rows the dispatcher has not routed (§2.6)."""
    mx = await db.fetchval("SELECT MAX(id) FROM events", writer=True) or 0
    mn = await db.fetchval("SELECT MIN(id) FROM events WHERE alerted_at IS NULL", writer=True)
    return int(mx) if mn is None else min(int(mx), int(mn) - 1)


# ── chain ticks (dereg_watch) ────────────────────────────────────────────────────────────────────────────────
async def latest_ticks(db: "Database", *, now: int, max_age_s: int = 1800) -> dict[int, dict]:
    rows = await db.fetchall(
        "SELECT t.netuid, t.block_number, t.occupant_key, t.observed_at, t.deregistration_risk, t.subnet_moving_price "
        "FROM chain_ticks t JOIN (SELECT netuid, MAX(block_number) AS b FROM chain_ticks GROUP BY netuid) m "
        "ON m.netuid = t.netuid AND m.b = t.block_number")
    return {int(r["netuid"]): {**r, "fresh": int(now) - int(r["observed_at"]) <= max_age_s} for r in rows}


def dumps(x: Any) -> str:
    return json.dumps(x, separators=(",", ":"), default=str)
