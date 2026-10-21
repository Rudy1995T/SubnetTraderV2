"""``integrity_features`` (spec 11 §3.3, §8.2): the 7 P4 rows, 2 trigger rows and 7 evidence rows per non-root
occupant, written ``stage='raw'``/``source='integrity'`` through ``put_metric_values`` (02 §B4). Reads only rows with
``observed_at ≤ decision time`` (strict); ``mode='reconstructed'`` admits backfilled owner rows by ``event_at`` (§2.1).

Also: D2 churn pairing and D4 extraction detection each run; ``team_reverify`` reminders, ``review.auto_clear`` and
``sync_confirmed_llm_flags`` on the official 00:07Z run; the C5-shaped ``compute_<metric_key>`` entry points."""
from __future__ import annotations

import json
import math
from collections import Counter
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal

import app.config as app_config
from app.features.types import MetricResult
from app.integrity import constants as K
from app.integrity import owner_metrics as M
from app.integrity.classify import OUTFLOW_KINDS
from app.integrity.models import OwnerChange, RawMetric, StakeEvent, failed, ok
from app.integrity.team import compute_team_doxxed
from app.integrity.whitelist import WhitelistSet
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.storage.events import emit_event
from app.storage.pit import put_metric_values
from app.util.time import floor_day, floor_hour, now_ts, parse_iso, parse_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

DAY = K.DAY
HOUR = K.HOUR
Mode = Literal["strict", "reconstructed"]


def _s():
    return app_config.settings


def _pit(mode: Mode, marker: str) -> str:
    """WHERE fragment for owner tables: strict = observed_at ≤ :t; reconstructed admits backfilled rows by event_at."""
    if mode == "strict":
        return "observed_at <= :t"
    return (f"((COALESCE(json_extract({marker}, '$.backfilled'), 0) = 1 AND COALESCE(event_at, observed_at) <= :t) "
            f"OR (COALESCE(json_extract({marker}, '$.backfilled'), 0) <> 1 AND observed_at <= :t))")


# ── input loading ────────────────────────────────────────────────────────────────────────────────────────────
async def _snapshots(db: "Database", okey: str, t: int, mode: Mode) -> tuple[dict | None, int | None]:
    p = _pit(mode, "raw_json")
    snap = await db.fetchone(f"SELECT * FROM owner_snapshots WHERE occupant_key = :o AND {p} "
                             "ORDER BY observed_at DESC LIMIT 1", {"o": okey, "t": t}, writer=True)
    first = await db.fetchval(f"SELECT MIN(observed_at) FROM owner_snapshots WHERE occupant_key = :o AND {p}",
                              {"o": okey, "t": t}, writer=True)
    return snap, first


async def _chain_row(db: "Database", netuid: int, okey: str, t: int) -> dict | None:
    return await db.fetchone("SELECT * FROM chain_hourly WHERE netuid = ? AND occupant_key = ? AND hour_ts <= ? "
                             "AND hour_ts >= ? AND observed_at <= ? ORDER BY hour_ts DESC, source = 'sse' DESC LIMIT 1",
                             (netuid, okey, t, t - 2 * HOUR - 3600, t), writer=True)


async def _sse_row(db: "Database", netuid: int, okey: str, t: int) -> dict | None:
    """Newest SSE-family row ≤ 2 h old (carries owner_personal_* and price; analytics rows do not)."""
    return await db.fetchone("SELECT * FROM chain_hourly WHERE netuid = ? AND occupant_key = ? AND hour_ts <= ? "
                             "AND hour_ts >= ? AND observed_at <= ? AND price IS NOT NULL ORDER BY hour_ts DESC LIMIT 1",
                             (netuid, okey, t, t - 2 * HOUR - 3600, t), writer=True)


def _a(x: Any) -> float | None:
    return None if x is None else float(x) / 1e9


def _stake_event(r: dict) -> StakeEvent:
    e = StakeEvent(r["event_uid"], r["owner_coldkey"], r["function"] or "", r["kind"], int(r["counts_as_outflow"]),
                   int(r["event_at"]), signer=r["signer"], hotkey=r["hotkey"], dest_coldkey=r["dest_coldkey"],
                   from_netuid=r["from_netuid"], to_netuid=r["to_netuid"], alpha=r["alpha"], tao=r["tao"],
                   price=r["price"], is_proxy=r["is_proxy"], success=int(r["success"]),
                   block_number=r["block_number"], extrinsic_id=r["extrinsic_id"])
    setattr(e, "source", r["source"]) if hasattr(e, "__dict__") else None
    return e


async def _stake_events(db: "Database", okey: str, lo: int, t: int, mode: Mode) -> list[dict]:
    p = _pit(mode, "raw_json")
    return await db.fetchall(f"SELECT * FROM owner_stake_events WHERE occupant_key = :o AND event_at >= :lo "
                             f"AND event_at < :t AND {p} AND (superseded_at IS NULL OR superseded_at > :t) "
                             "ORDER BY event_at", {"o": okey, "lo": lo, "t": t}, writer=True)


def _change(r: dict) -> OwnerChange:
    return OwnerChange(r["occupant_key"], r["netuid"], r["change_type"], int(r["event_at"]), int(r["observed_at"]),
                       r["detected_via"], old_value=r["old_value"], new_value=r["new_value"],
                       classification_basis=r["classification_basis"], s3_base_penalty=r["s3_base_penalty"],
                       block_number=r["block_number"], extrinsic_id=r["extrinsic_id"],
                       paired_change_id=r["paired_change_id"], identity_event_id=r["identity_event_id"],
                       evidence=json.loads(r["evidence_json"] or "{}"), id=r["id"], run_id=r["run_id"])


async def _changes(db: "Database", okey: str, lo: int, t: int, mode: Mode) -> list[OwnerChange]:
    p = _pit(mode, "evidence_json")
    rows = await db.fetchall(f"SELECT * FROM owner_changes WHERE occupant_key = :o AND event_at >= :lo "
                             f"AND event_at < :t AND {p} ORDER BY event_at", {"o": okey, "lo": lo, "t": t}, writer=True)
    return [_change(r) for r in rows]


async def _coverage(db: "Database", coldkey: str | None) -> dict | None:
    if not coldkey:
        return None
    v = await db.fetchval("SELECT value_json FROM job_state WHERE job = 'owner_collect_daily' AND key = ?",
                          (f"activity_wm:{coldkey}",), writer=True)
    try:
        return json.loads(v) if v else None
    except ValueError:
        return None


def _first_emission_ts(occ: dict) -> int | None:
    fe = occ.get("first_emission_block")
    reg = occ.get("registered_ts") or occ.get("registered_at_est")
    if fe is None or reg is None:
        return None
    bs = float(_s().BLOCK_SECONDS)
    return int(reg + (int(fe) - int(occ["network_registered_at"])) * bs)


# ── one occupant ─────────────────────────────────────────────────────────────────────────────────────────────
async def compute_occupant(db: "Database", occ: dict, *, t: int, mode: Mode = "strict") -> list[RawMetric]:
    s = _s()
    okey, n = occ["occupant_key"], int(occ["netuid"])
    max_age = int(s.OWNER_SNAPSHOT_MAX_AGE_HOURS) * HOUR
    snap, first_snap = await _snapshots(db, okey, t, mode)
    fresh = snap is not None and t - int(snap["observed_at"]) <= max_age
    if mode == "reconstructed" and snap is not None and json.loads(snap.get("raw_json") or "{}").get("backfilled"):
        fresh = t - int(snap.get("event_at") or snap["observed_at"]) <= max_age
    sn = snap if fresh else None
    ch = await _chain_row(db, n, okey, t)
    sse = await _sse_row(db, n, okey, t)
    ws = await WhitelistSet.load(db, okey)
    out: list[RawMetric] = []

    # ── head block, tenure ────────────────────────────────────────────────────────────────────────────────
    head, head_src = None, None
    if ch and ch.get("block_number"):
        head, head_src = int(ch["block_number"]), "chain_hourly"
    elif occ.get("last_seen_block"):
        head, head_src = int(occ["last_seen_block"]), "occupants"
    tenure = M.tenure_row(head, occ.get("network_registered_at"), head_source=head_src,
                          block_seconds=float(s.BLOCK_SECONDS))
    out.append(tenure)
    age_days = tenure.value

    # ── S1 ────────────────────────────────────────────────────────────────────────────────────────────────
    fe_ts = _first_emission_ts(occ)
    pre_launch = occ.get("first_emission_block") is None or occ.get("status") == "pre_launch"
    w_start = max(t - K.RETENTION_WINDOW_DAYS * DAY, fe_ts or 0)
    raw_ev = await _stake_events(db, okey, w_start, t, mode)
    evs, n_wl = [], 0
    for r in raw_ev:
        if ws.stake_event(r, t):
            n_wl += 1
            continue
        e = _stake_event(r)
        e.raw = {"source": r["source"]}
        evs.append(e)
    cut_snaps = await db.fetchall(
        f"SELECT owner_cut_per_block, owner_cut_enabled FROM owner_snapshots WHERE occupant_key = :o "
        f"AND observed_at >= :lo AND {_pit(mode, 'raw_json')}", {"o": okey, "lo": w_start, "t": t}, writer=True)
    cuts = [0.0 if r["owner_cut_enabled"] == 0 else float(r["owner_cut_per_block"]) for r in cut_snaps
            if r["owner_cut_enabled"] == 0 or r["owner_cut_per_block"] is not None]
    earned, method = M.earned_alpha({}, cuts, window=(w_start, t))
    pool = await db.fetchall("SELECT hour_ts, tao_liquidity FROM chain_hourly WHERE netuid = ? AND occupant_key = ? "
                             "AND hour_ts >= ? AND hour_ts < ? AND observed_at <= ? AND tao_liquidity IS NOT NULL",
                             (n, okey, w_start, t, t), writer=True)
    tao_in_h = {int(r["hour_ts"]): float(r["tao_liquidity"]) / 1e9 for r in pool}
    impact = M.impact7_series(evs, tao_in_h, window=(w_start, t))
    ck = (sn or snap or {}).get("owner_coldkey") or occ.get("owner_coldkey")
    cov = await _coverage(db, ck)
    covered = False
    cov_start = None
    if cov:
        cov_start = cov.get("coverage_start")
        last_ok = cov.get("last_ok") or cov.get("ts")
        covered = (cov_start is not None and int(cov_start) <= w_start and last_ok is not None
                   and t - int(last_ok) <= max_age)
    if mode == "reconstructed" and not covered:
        covered = bool(raw_ev) and any(json.loads(r.get("raw_json") or "{}").get("backfilled") for r in raw_ev)
    cut_enabled = None if sn is None or sn.get("owner_cut_enabled") is None else bool(sn["owner_cut_enabled"])
    s1 = M.s1_owner_retention(evs, earned=earned, earned_method=method, impact7=impact, owner_cut_enabled=cut_enabled,
                              pre_launch=pre_launch, activity_covered=covered,
                              min_earned=float(s.OWNER_MIN_EARNED_ALPHA), n_whitelisted=n_wl,
                              coverage_start=cov_start)
    for e in evs:
        src = (e.raw or {}).get("source")
        if src:
            s1.value_json["sources"] = sorted(set(s1.value_json.get("sources") or []) | {src})
    out.append(s1)

    # ── S2 ────────────────────────────────────────────────────────────────────────────────────────────────
    rufus_fresh = bool(sn and sn.get("rufus_stale") == 0 and sn.get("rufus_baked_at")
                       and t - int(sn["rufus_baked_at"]) <= int(s.OWNER_RUFUS_MAX_AGE_HOURS) * HOUR)
    perp, perp_src = None, None
    if rufus_fresh and sn.get("rufus_owner_perp") is not None:
        perp, perp_src = float(sn["rufus_owner_perp"]), "rufus"
    else:
        hp = None
        if sse is not None:
            from app.integrity.detectors import hourly_perp
            hp = hourly_perp(sse)[0]
        if hp is not None:
            perp, perp_src = hp, "chain_hourly"
        elif sn is not None and sn.get("tmc_owner_perpetual") is not None:
            perp, perp_src = float(sn["tmc_owner_perpetual"]), "owner_snapshots"
    if perp is not None and abs(perp) < 1e-9:
        perp = 0.0
    total, total_src = None, None
    if sn is not None and sn.get("owner_total_alpha") is not None:
        total, total_src = float(sn["owner_total_alpha"]), "coldkey_stakes"
    elif sn is not None and sn.get("tmc_owner_personal_stake") is not None:
        total, total_src = float(sn["tmc_owner_personal_stake"]), "tmc_subnet"
    elif sse is not None and sse.get("owner_personal_stake") is not None:
        total, total_src = _a(sse["owner_personal_stake"]), "chain_hourly"
    auto, auto_src = None, None
    if rufus_fresh and sn.get("rufus_auto_lock") is not None:
        auto, auto_src = bool(sn["rufus_auto_lock"]), "rufus"
    elif sn is not None and sn.get("auto_lock_enabled_tmc") is not None:
        auto, auto_src = bool(sn["auto_lock_enabled_tmc"]), "tmc"
    tss = _a(sse.get("total_subnet_stake")) if sse else None
    commit_pct, proxy = None, False
    if rufus_fresh and sn.get("rufus_owner_pct") is not None:
        commit_pct = float(sn["rufus_owner_pct"])
    elif perp is not None and tss:
        commit_pct, proxy = perp / tss, True
    s2 = M.s2_owner_commitment(perp=perp, total=total, auto=auto, commit_pct=commit_pct, owner_cut_enabled=cut_enabled,
                               age_days=age_days, perp_source=perp_src, total_source=total_src, auto_source=auto_src,
                               eligible_proxy=proxy, rufus_to_owner_perp=(sn or {}).get("rufus_to_owner_perp"),
                               rufus_to_owner_lockers=(sn or {}).get("rufus_to_owner_lockers"))
    out.append(s2)

    # ── S3 + control-change trigger ───────────────────────────────────────────────────────────────────────
    chg = await _changes(db, okey, t - K.S3_WINDOW_DAYS * DAY, t, mode)
    observable = sn is not None
    if mode == "reconstructed" and not observable:
        observable = bool(await db.fetchval("SELECT 1 FROM rpc_owner_history WHERE occupant_key = ? AND event_at <= ? "
                                            "LIMIT 1", (okey, t), writer=True)) or snap is not None
    def wl(c: OwnerChange) -> bool:
        return ws.change(c, t) is not None
    s3 = M.s3_control_stability(chg, t=t, observable=observable, whitelisted=wl, observed_since=first_snap)
    out.append(s3)
    cc30 = [c for c in chg if c.event_at >= t - K.CONTROL_CHANGE_WINDOW_DAYS * DAY
            and c.change_type in K.COLDKEY_CHANGE_TYPES and not wl(c)]
    churn_ev = []
    if cc30:
        ph = ",".join("?" * len(K.CHURN_IDENTITY_FIELDS))
        churn_ev = await db.fetchall(
            f"SELECT event_at FROM events WHERE occupant_key = ? AND event_type = 'identity_changed' "
            f"AND json_extract(payload, '$.source') = 'chain' AND json_extract(payload, '$.field') IN ({ph}) "
            f"AND event_at >= ? AND observed_at <= ?",
            (okey, *sorted(K.CHURN_IDENTITY_FIELDS), t - (K.CONTROL_CHANGE_WINDOW_DAYS + 15) * DAY, t), writer=True)
    reps = [c for c in await _changes(db, okey, t - (K.CONTROL_CHANGE_WINDOW_DAYS + 15) * DAY, t, mode)
            if c.change_type in ("hotkey_repoint", "hotkey_repoint_identity_churn")]
    w14 = K.REPOINT_CHURN_WINDOW_DAYS * DAY

    def churn(c: OwnerChange) -> bool:
        return any(abs(r.event_at - c.event_at) <= w14 for r in reps) or \
            any(abs(int(e["event_at"]) - c.event_at) <= w14 for e in churn_ev)
    obs_trig = sn is not None or (mode == "reconstructed" and observable)
    cct = M.control_change_trigger(cc30, observable=obs_trig, churn=churn)
    perp_offs = [c for c in chg if c.change_type == "perpetual_off" and not wl(c)
                 and c.event_at >= t - K.PERPETUAL_OFF_WINDOW_DAYS * DAY]
    hard_ev = [e for e in evs if e.kind == "sell_full"] if s1.value_json.get("hard") else []
    lock_obs = sn is not None or sse is not None
    ext = M.extraction_trigger(s1, hard_ev, perp_offs, observable=lock_obs)
    out += [ext, cct]

    # ── S5 ────────────────────────────────────────────────────────────────────────────────────────────────
    avail = None
    if sn is not None and sn.get("owner_available_alpha") is not None:
        avail = float(sn["owner_available_alpha"])
    elif sn is not None and sn.get("tmc_owner_personal_stake") is not None and sn.get("tmc_owner_locked") is not None:
        avail = max(0.0, float(sn["tmc_owner_personal_stake"]) - float(sn["tmc_owner_locked"]))
    elif sse is not None and sse.get("owner_personal_stake") is not None and sse.get("owner_personal_locked_alpha") \
            is not None:
        avail = max(0.0, _a(sse["owner_personal_stake"]) - _a(sse["owner_personal_locked_alpha"]))
    price = float(sse["price"]) if sse and sse.get("price") is not None else None
    tao_in = _a(sse.get("tao_liquidity")) if sse else None
    reg = occ.get("registered_ts") or occ.get("registered_at_est")
    est = (sn or {}).get("initial_alpha_est")
    hidden = bool(reg is not None and parse_iso(s.DTAO_LAUNCH_UTC) <= int(reg) < K.FREE_ALPHA_CUTOFF_TS
                  and ws.match(classes={"initial_alpha"}, event_at=int(reg), t=t) is None
                  and not (est is not None and price is not None and tao_in and est * price < 0.01 * tao_in))
    s5 = M.s5_owner_overhang(avail=avail, price=price, tao_in=tao_in, hidden_alpha=hidden, initial_alpha_est=est)
    out.append(s5)

    # ── owner_identity ────────────────────────────────────────────────────────────────────────────────────
    from app.registry import resolved
    name = url = gh = None
    try:
        name = await resolved(db, okey, "owner_name", as_of=t)
        url = await resolved(db, okey, "owner_url", as_of=t)
        gh = await resolved(db, okey, "owner_github", as_of=t)
    except Exception as exc:  # replay merger failures never abort the run
        logger.warning("integrity.identity_resolve_failed", data={"occupant_key": okey, "error": str(exc)})
    sub_ck = occ.get("owner_coldkey")
    snap_ck = (sn or {}).get("owner_coldkey")
    key_match = not (sub_ck and snap_ck and sub_ck != snap_ck)
    oi = M.owner_identity_value(name.status if name else None, bool(url and url.status == "present"),
                                bool(gh and gh.status == "present"), key_match, owner_coldkey=snap_ck or sub_ck,
                                name=name.value if name and name.status == "present" else None,
                                url=url.value if url and url.status == "present" else None,
                                github=gh.value if gh and gh.status == "present" else None)
    out.append(oi)

    # ── team ──────────────────────────────────────────────────────────────────────────────────────────────
    td = await compute_team_doxxed(db, okey, t)
    out.append(td)

    # ── evidence ──────────────────────────────────────────────────────────────────────────────────────────
    def ev_row(key: str, value: float | None, reason: str, **vj: Any) -> RawMetric:
        return ok(key, value, **vj) if value is not None else failed(key, reason, **vj)
    lr = s2.value_json.get("lock_ratio")
    out.append(ev_row("owner_lock_ratio", lr, s2.reason or "no_position"))
    out.append(ev_row("owner_dump_impact", s5.value_json.get("dump_impact"), s5.reason or "no_position"))
    hs = M.holding_share(total, tss)
    out.append(ev_row("owner_holding_share", hs, "no_position", owner_total=total, total_subnet_stake=tss))
    out.append(ev_row("owner_sell_impact_7d", s1.value_json.get("max_7d_sell_impact"), "no_pool"))
    sr = s1.value_json.get("sell_ratio")
    out.append(ev_row("owner_sell_ratio_30d", sr, "earned_below_min" if earned is not None else "earned_unknown"))
    out.append(ok("team_verified_count", float(td.value_json.get("n_verified") or 0)))
    out.append(ok("team_candidate_count", float(td.value_json.get("n_named") or 0)))
    return out


# ── slots and the run ────────────────────────────────────────────────────────────────────────────────────────
def slot_as_of(decision: int, slots: list[str] | None = None) -> int:
    """00:07 → 00:00 (official); 06/12/18:07 → the slot hour (provisional); other times → the latest slot ≤ t."""
    slots = slots or list(_s().INTEGRITY_FEATURES_AT_UTC)
    hours = sorted({int(x.split(":")[0]) for x in slots})
    d = floor_day(decision)
    cands = [d + h * HOUR for h in hours if d + h * HOUR <= decision]
    return max(cands) if cands else d


async def occupants_for_run(db: "Database") -> list[dict]:
    return await db.fetchall("SELECT o.*, s.owner_coldkey AS owner_coldkey, s.owner_hotkey AS owner_hotkey "
                             "FROM subnets s JOIN occupants o USING (occupant_key) "
                             "WHERE o.status IN ('active', 'pre_launch') AND s.netuid <> 0 ORDER BY s.netuid",
                             writer=True)


async def sync_confirmed_llm_flags(db: "Database", t: int) -> int:
    rows = await db.fetchall(
        "SELECT e.id, e.occupant_key, e.observed_at, json_extract(e.payload, '$.url') AS url FROM events e "
        "WHERE e.event_type = 'owner_extraction_evidence' AND json_extract(e.payload, '$.confirmed_by') IS NOT NULL "
        "AND e.observed_at <= ? AND e.occupant_key IS NOT NULL AND NOT EXISTS (SELECT 1 FROM manual_flags f WHERE "
        "f.occupant_key = e.occupant_key AND f.flag = 'owner_extraction' AND f.state = 'confirmed' "
        "AND f.set_at >= e.observed_at)", (int(t),), writer=True)
    for r in rows:
        await db.execute("INSERT INTO manual_flags (occupant_key, flag, state, evidence_url, note, set_by, set_at) "
                         "VALUES (?, 'owner_extraction', 'confirmed', ?, ?, 'sync:llm_confirmed', ?)",
                         (r["occupant_key"], r["url"] or f"event:{r['id']}", f"events.id={r['id']}", int(t)))
    return len(rows)


async def run_features(db: "Database", *, decision_time: int, as_of: int, run_id: str, observed_at: int | None = None,
                       backfilled: bool = False, official: bool | None = None, write: bool = True,
                       side_effects: bool = True, netuids: set[int] | None = None) -> dict:
    from app.integrity.detectors import detect_extraction, pair_identity_churn
    from app.integrity.review import auto_clear, enqueue
    from app.integrity.team import due_reverify
    s = _s()
    official = (as_of % DAY == 0) if official is None else official
    obs = int(observed_at if observed_at is not None else decision_time)
    occs = [o for o in await occupants_for_run(db) if netuids is None or o["netuid"] in netuids]
    status: Counter = Counter()
    n_rows = n_events = 0
    degraded = Counter()
    n_none3 = 0
    results: dict[str, list[RawMetric]] = {}
    for occ in occs:
        okey = occ["occupant_key"]
        try:
            if side_effects and not backfilled:
                await pair_identity_churn(db, okey, now=decision_time, run_id=run_id)
            rows = await compute_occupant(db, occ, t=decision_time)
        except Exception as exc:
            logger.warning("integrity.occupant_failed", data={"occupant_key": okey, "error": f"{type(exc).__name__}: "
                                                                                          f"{exc}"})
            status["occupant_failed"] += 1
            continue
        results[okey] = rows
        by = {r.key: r for r in rows}
        for r in rows:
            status[f"{r.key}:{r.status}"] += 1
        if all(by[k].status != "ok" for k in ("owner_retention_30d", "owner_commitment", "control_stability_90d")):
            n_none3 += 1
            for k in ("owner_retention_30d", "owner_commitment", "control_stability_90d"):
                degraded[f"{k}:{by[k].reason}"] += 1
        if side_effects and not backfilled:
            s1 = by["owner_retention_30d"]
            if s1.value_json.get("hard"):
                lo = max(decision_time - K.RETENTION_WINDOW_DAYS * DAY, 0)
                evs = [_stake_event(r) for r in await _stake_events(db, okey, lo, decision_time, "strict")
                       if r["counts_as_outflow"]]
                n_events += 1 if await detect_extraction(db, okey, occ["netuid"], s1, now=decision_time,
                                                         run_id=run_id, events=evs) else 0
            hs, lr = by["owner_holding_share"].value, by["owner_lock_ratio"].value
            if hs is not None and lr is not None and hs > float(s.OWNER_S4_HOLDING_SHARE_MAX) \
                    and lr < float(s.OWNER_S4_LOCK_RATIO_MIN):
                from app.integrity.detectors import iso_week
                eid = await emit_event(db, event_type="owner_concentration", occupant_key=okey, netuid=occ["netuid"],
                                       event_at=decision_time, observed_at=obs,
                                       payload={"netuid": occ["netuid"], "owner_holding_share": hs, "lock_ratio": lr},
                                       dedupe_key=f"{okey}|conc|{iso_week(decision_time)}", source="integrity",
                                       run_id=run_id)
                n_events += 1 if eid else 0
        if write:
            n_rows += await put_metric_values(db, [r.to_row(occupant_key=okey, as_of=as_of, observed_at=obs,
                                                            run_id=run_id, spec_version=s.SPEC_VERSION,
                                                            backfilled=backfilled) for r in rows])
    extra: dict[str, Any] = {}
    if side_effects and not backfilled and occs and n_none3 > 0.25 * len(occs):
        eid = await emit_event(db, event_type="owner_data_degraded", occupant_key=None, netuid=None,
                               event_at=decision_time, observed_at=obs,
                               payload={"n_none": n_none3, "n_total": len(occs), "reasons": dict(degraded)},
                               dedupe_key=f"owner_degraded|{decision_time // DAY}", source="integrity", run_id=run_id)
        n_events += 1 if eid else 0
    if official and side_effects and not backfilled:
        due = await due_reverify(db, decision_time)
        for mid in due:
            m = await db.fetchone("SELECT occupant_key, name, last_checked_at FROM team_members WHERE id = ?", (mid,),
                                  writer=True)
            await enqueue(db, occupant_key=m["occupant_key"], kind="team_reverify", dedupe_base=f"team_reverify|{mid}",
                          ref_table="team_members", ref_id=mid, priority=3, summary=f"re-verify {m['name']}",
                          payload={"last_checked_at": m["last_checked_at"]}, now=decision_time)
        extra["reverify_due"] = len(due)
        extra["auto_cleared"] = await auto_clear(db, decision_time)
        extra["llm_flags_synced"] = await sync_confirmed_llm_flags(db, decision_time)
    return {"as_of": as_of, "official": official, "occupants": len(occs), "rows": n_rows, "events": n_events,
            "status": dict(status), "results": results, **extra}


async def missing_official_days(db: "Database", *, now: int, days: int) -> list[int]:
    today = floor_day(now)
    want = [today - k * DAY for k in range(1, days + 1)]
    have = {int(r["as_of"]) for r in await db.fetchall(
        "SELECT DISTINCT as_of FROM metric_values WHERE source = 'integrity' AND metric_key = 'tenure' "
        "AND stage = 'raw' AND as_of >= ? AND as_of % 86400 = 0", (today - days * DAY,), writer=True)}
    return sorted(d for d in want if d not in have)


class IntegrityFeaturesJob(CollectorJob):
    _last: dict | None = None

    async def collect(self, run: RunContext) -> JobOutcome:
        db = run.rt.db
        decision = int(run.started_at)
        as_of = slot_as_of(decision)
        out = await run_features(db, decision_time=decision, as_of=as_of, run_id=run.run_id)
        caught = 0
        if as_of % DAY == 0 or run.trigger == "catchup":
            for d in await missing_official_days(db, now=decision, days=int(run.settings.INTEGRITY_FEATURES_CATCHUP_DAYS)):
                if d >= floor_day(decision):
                    continue
                if d < (await self._first_data(db) or d):
                    continue
                r = await run_features(db, decision_time=d + 7 * 60, as_of=d, run_id=run.run_id, observed_at=decision,
                                       backfilled=True, official=True, side_effects=False)
                caught += r["rows"]
        out.pop("results", None)
        self._last = {k: v for k, v in out.items() if k != "status"} | {"catchup_rows": caught}
        return JobOutcome(rows=out["rows"] + caught,
                          note=f"as_of={to_iso(as_of)} occupants={out['occupants']} rows={out['rows']} "
                               f"events={out['events']} catchup_rows={caught}", meta=self._last)

    async def _first_data(self, db: "Database") -> int | None:
        v = await db.fetchval("SELECT MIN(observed_at) FROM owner_snapshots", writer=True)
        return floor_day(int(v)) if v else None

    def health_extra(self) -> dict | None:
        return self._last


# ── 02 §C5 entry points (13/17 replay; 12's registry treats these keys as collector rows) ───────────────────
_CACHE: dict[tuple[str, int, str], dict[str, RawMetric]] = {}


async def _entry(db: "Database", key: str, occupant_key: str, as_of: datetime | int, mode: Mode) -> MetricResult:
    t = parse_ts(as_of)
    ck = (occupant_key, t, mode)
    if ck not in _CACHE:
        occ = await db.fetchone("SELECT o.*, s.owner_coldkey AS owner_coldkey FROM occupants o LEFT JOIN subnets s "
                                "ON s.occupant_key = o.occupant_key WHERE o.occupant_key = ?", (occupant_key,),
                                writer=True)
        if occ is None:
            return MetricResult(None, {"status": "failed", "reason": "no_occupant"})
        if len(_CACHE) > 4096:
            _CACHE.clear()
        _CACHE[ck] = {r.key: r for r in await compute_occupant(db, dict(occ), t=t, mode=mode)}
    return _CACHE[ck][key].to_metric_result()


def clear_cache() -> None:
    _CACHE.clear()


def _make(key: str):
    async def fn(db, occupant_key: str, as_of: datetime, *, mode: Mode = "strict") -> MetricResult:
        return await _entry(db, key, occupant_key, as_of, mode)
    fn.__name__ = f"compute_{key}"
    return fn


compute_team_doxxed_metric = _make("team_doxxed")
compute_owner_identity = _make("owner_identity")
compute_owner_commitment = _make("owner_commitment")
compute_owner_retention_30d = _make("owner_retention_30d")
compute_control_stability_90d = _make("control_stability_90d")
compute_owner_overhang = _make("owner_overhang")
compute_tenure = _make("tenure")
compute_owner_extraction_trigger = _make("owner_extraction_trigger")
compute_owner_control_change_trigger = _make("owner_control_change_trigger")
ENTRY_POINTS = {k: _make(k) for k in K.P4_KEYS + K.TRIGGER_KEYS}


__all__ = ["compute_occupant", "run_features", "IntegrityFeaturesJob", "slot_as_of", "sync_confirmed_llm_flags",
           "ENTRY_POINTS", "OUTFLOW_KINDS", "floor_hour", "now_ts", "math"]
