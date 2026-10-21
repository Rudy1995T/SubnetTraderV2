"""Owner control/lock/extraction detectors D1–D9 (spec 11 §2.9). ``record_change`` writes the ``owner_changes`` row,
its event and its review item in one transaction. A coldkey change is recorded once per (occupant, new coldkey)
within ±3 d and a re-point once per (occupant, new hotkey) within ±3 d — the hourly watch, the daily collect and the
RPC history seed may all see the same transition.

CLI (dry run on a DB copy, §10 step 6): ``python -m app.integrity.detectors drill --db /tmp/ts-copy.db --netuid 64
--fake-coldkey-change``."""
from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Sequence

from app.integrity import constants as K
from app.integrity.models import OwnerChange, RawMetric, StakeEvent
from app.integrity.whitelist import WhitelistSet
from app.storage.catalog import EVENT_TYPES
from app.storage.events import emit_event

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

DAY = 86400
SAME_CHANGE_SLACK = 3 * DAY


def iso_week(ts: int) -> str:
    y, w, _ = datetime.fromtimestamp(int(ts), timezone.utc).isocalendar()
    return f"{y}-W{w:02d}"


async def _existing(db: "Database", okey: str, types: Sequence[str], new_value: str | None, event_at: int,
                    slack: int = SAME_CHANGE_SLACK) -> dict | None:
    ph = ",".join("?" * len(types))
    return await db.fetchone(f"SELECT * FROM owner_changes WHERE occupant_key = ? AND change_type IN ({ph}) "
                             f"AND COALESCE(new_value, '') = COALESCE(?, '') AND event_at BETWEEN ? AND ? LIMIT 1",
                             (okey, *types, new_value, int(event_at) - slack, int(event_at) + slack), writer=True)


async def record_change(db: "Database", change: OwnerChange, *, event_type: str | None, severity: str | None = None,
                        review_kind: str | None = None, suggested_flag: str | None = None, priority: int = 2,
                        payload: dict | None = None, event_dedupe: str | None = None,
                        review_event_type: str | None = None, backfilled: bool = False) -> int | None:
    """owner_changes + events + review_queue in one transaction; returns the change id (None = duplicate)."""
    from app.integrity.review import enqueue
    ws = await WhitelistSet.load(db, change.occupant_key)
    wid = ws.change(change, change.observed_at)
    ev = dict(change.evidence)
    if backfilled:
        ev["backfilled"] = True
    async with db.transaction():
        rows = await db.execute_returning(
            "INSERT INTO owner_changes (occupant_key, netuid, change_type, old_value, new_value, detected_via, "
            "classification_basis, s3_base_penalty, paired_change_id, identity_event_id, block_number, extrinsic_id, "
            "event_at, observed_at, evidence_json, whitelisted, whitelist_id, dedupe_key, run_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (dedupe_key) DO NOTHING RETURNING id",
            (change.occupant_key, change.netuid, change.change_type, change.old_value, change.new_value,
             change.detected_via, change.classification_basis, change.s3_base_penalty, change.paired_change_id,
             change.identity_event_id, change.block_number, change.extrinsic_id, int(change.event_at),
             int(change.observed_at), json.dumps(ev, default=str), 1 if wid else 0, wid, change.dedupe_key,
             change.run_id or "integrity"))
        if not rows:
            return None
        cid = int(rows[0]["id"])
        change.id = cid
        rid = None
        if review_kind:
            rid = await enqueue(db, occupant_key=change.occupant_key, kind=review_kind,
                                dedupe_base=f"{review_kind}|{cid}", ref_table="owner_changes", ref_id=cid,
                                priority=priority, suggested_flag=suggested_flag,
                                event_type=review_event_type or event_type,
                                summary=f"{change.change_type} on SN{change.netuid}",
                                payload={"change_type": change.change_type, "old": change.old_value,
                                         "new": change.new_value, "event_at": change.event_at},
                                now=change.observed_at)
        eid = None
        if event_type:
            p = dict(payload or {})
            keys = EVENT_TYPES[event_type].payload_keys
            p.setdefault("netuid", change.netuid)
            if "change_id" in keys or "change_id" not in p:
                p["change_id"] = cid
            if "review_id" in keys:
                p["review_id"] = rid
            if "whitelisted" in keys:
                p["whitelisted"] = bool(wid)
            if backfilled:
                p["backfilled"] = True
            eid = await emit_event(db, event_type=event_type, occupant_key=change.occupant_key, netuid=change.netuid,
                                   severity=severity, event_at=int(change.event_at), observed_at=int(change.observed_at),
                                   payload=p, dedupe_key=event_dedupe, source="integrity", run_id=change.run_id or None,
                                   alerted_at=int(change.observed_at) if backfilled else None)
        await db.execute("UPDATE owner_changes SET event_id = ?, review_id = ? WHERE id = ?", (eid, rid, cid))
    return cid


# ── D1 coldkey change ────────────────────────────────────────────────────────────────────────────────────────
def classify_coldkey_change(*, taostats_is_swap: bool | None = None, challenger_coldkeys: Sequence[str] = (),
                            days_to_king: float | None = None, radar_rows: Sequence[dict] = (),
                            old_ck: str | None, new_ck: str, netuid: int,
                            old_ck_share: float | None = None, old_ck_status: str = "skip") -> tuple[str, str]:
    """First match wins (§2.9 D1): TaoStats swap → Rufus challenger → subnetradar → C2 heuristic on the old key."""
    if taostats_is_swap:
        return "coldkey_swap", "taostats_is_coldkey_swap"
    if new_ck in set(challenger_coldkeys) or (days_to_king is not None and days_to_king <= 1):
        return "subnet_owner_changed", "rufus_challenger"
    for r in radar_rows:
        if r.get("new_coldkey") == new_ck and (r.get("old_coldkey") in (None, old_ck)) \
                and r.get("netuid") in (None, netuid):
            return "coldkey_swap", "subnetradar"
    if old_ck_status.startswith("ok") and old_ck_share is not None:
        return ("owner_transfer", "old_ck_retains_stake") if old_ck_share >= 0.5 else ("coldkey_swap", "old_ck_emptied")
    return "coldkey_change_unclassified", "unclassified"


async def detect_coldkey_change(db: "Database", occupant_key: str, netuid: int, prev_ck: str | None, curr_ck: str | None,
                                *, event_at: int, observed_at: int, run_id: str, detected_via: str,
                                classify: Callable[[], Awaitable[tuple[str, str]]], block_number: int | None = None,
                                evidence: dict | None = None, backfilled: bool = False,
                                review: bool = True) -> int | None:
    if not prev_ck or not curr_ck or prev_ck == curr_ck:
        return None
    if await _existing(db, occupant_key, tuple(K.COLDKEY_CHANGE_TYPES), curr_ck, event_at):
        return None
    ctype, basis = await classify()
    ch = OwnerChange(occupant_key, netuid, ctype, int(event_at), int(observed_at), detected_via, old_value=prev_ck,
                     new_value=curr_ck, classification_basis=basis, s3_base_penalty=K.S3_PENALTY_SWAP,
                     block_number=block_number, evidence=evidence or {}, run_id=run_id)
    return await record_change(
        db, ch, event_type="owner_control_change", review_kind="owner_change_whitelist" if review else None, priority=1,
        payload={"change_type": ctype, "old_coldkey": prev_ck, "new_coldkey": curr_ck, "classification_basis": basis,
                 "detected_via": detected_via, "swapped_at": int(event_at), "block_number": block_number},
        event_dedupe=f"{occupant_key}|ck|{curr_ck}", review_event_type="owner_control_change", backfilled=backfilled)


# ── D2 hotkey re-point and identity churn ────────────────────────────────────────────────────────────────────
async def detect_hotkey_repoint(db: "Database", occupant_key: str, netuid: int, prev_hk: str | None,
                                curr_hk: str | None, *, event_at: int, observed_at: int, run_id: str,
                                detected_via: str, block_number: int | None = None, backfilled: bool = False,
                                evidence: dict | None = None) -> int | None:
    if not prev_hk or not curr_hk or prev_hk == curr_hk:
        return None
    if await _existing(db, occupant_key, ("hotkey_repoint",), curr_hk, event_at):
        return None
    ch = OwnerChange(occupant_key, netuid, "hotkey_repoint", int(event_at), int(observed_at), detected_via,
                     old_value=prev_hk, new_value=curr_hk, s3_base_penalty=0.0, block_number=block_number,
                     evidence=evidence or {}, run_id=run_id)
    return await record_change(db, ch, event_type="owner_hotkey_repointed",
                               payload={"old_hotkey": prev_hk, "new_hotkey": curr_hk, "block_number": block_number},
                               event_dedupe=f"{occupant_key}|hk|{curr_hk}", backfilled=backfilled)


async def pair_identity_churn(db: "Database", occupant_key: str, *, now: int, run_id: str,
                              window_days: int = K.REPOINT_CHURN_WINDOW_DAYS) -> list[int]:
    """Pair each re-point of the last 2 × 14 d with a chain-source identity change of the D4 warning fields within
    14 d (either order) → ``hotkey_repoint_identity_churn`` (S3 base 0.4) + ``owner_identity_churn`` + review."""
    w = window_days * DAY
    reps = await db.fetchall("SELECT * FROM owner_changes WHERE occupant_key = ? AND change_type = 'hotkey_repoint' "
                             "AND event_at >= ? AND observed_at <= ?", (occupant_key, now - 2 * w, now), writer=True)
    out: list[int] = []
    for rp in reps:
        if await db.fetchval("SELECT 1 FROM owner_changes WHERE occupant_key = ? AND paired_change_id = ? AND "
                             "change_type = 'hotkey_repoint_identity_churn'", (occupant_key, rp["id"]), writer=True):
            continue
        ph = ",".join("?" * len(K.CHURN_IDENTITY_FIELDS))
        ev = await db.fetchone(
            f"SELECT id, event_at, json_extract(payload, '$.field') AS field FROM events WHERE occupant_key = ? "
            f"AND event_type = 'identity_changed' AND json_extract(payload, '$.source') = 'chain' "
            f"AND json_extract(payload, '$.field') IN ({ph}) AND event_at BETWEEN ? AND ? AND observed_at <= ? "
            f"ORDER BY ABS(event_at - ?) LIMIT 1",
            (occupant_key, *sorted(K.CHURN_IDENTITY_FIELDS), rp["event_at"] - w, rp["event_at"] + w, now,
             rp["event_at"]), writer=True)
        if not ev:
            continue
        at = max(int(rp["event_at"]), int(ev["event_at"]))
        days_apart = abs(int(ev["event_at"]) - int(rp["event_at"])) / DAY
        ch = OwnerChange(occupant_key, int(rp["netuid"]), "hotkey_repoint_identity_churn", at, int(now), "features",
                         old_value=rp["old_value"], new_value=rp["new_value"],
                         s3_base_penalty=K.S3_PENALTY_REPOINT_CHURN, paired_change_id=int(rp["id"]),
                         identity_event_id=int(ev["id"]), evidence={"field": ev["field"], "days_apart": days_apart},
                         run_id=run_id)
        cid = await record_change(db, ch, event_type="owner_identity_churn", review_kind="owner_change_whitelist",
                                  priority=1, review_event_type="owner_identity_churn",
                                  payload={"repoint_change_id": int(rp["id"]), "identity_event_id": int(ev["id"]),
                                           "field": ev["field"], "days_apart": round(days_apart, 2)},
                                  event_dedupe=f"{occupant_key}|churn|{rp['id']}")
        if cid:
            out.append(cid)
    return out


# ── D3 lock transitions (pure) ───────────────────────────────────────────────────────────────────────────────
def hourly_perp(row: dict) -> tuple[float | None, float | None]:
    """(perp, decay) in α of a chain_hourly row; ``analytics_1w`` rows carry no perpetual column → locked − decaying."""
    def a(x):
        return None if x is None else float(x) / 1e9
    perp, decay, locked = a(row.get("owner_personal_perpetual_alpha")), a(row.get("owner_personal_decaying_alpha")), \
        a(row.get("owner_personal_locked_alpha"))
    if perp is None and locked is not None and decay is not None:
        perp = locked - decay
    return perp, decay


def detect_lock_transitions(rows: Sequence[dict], *, min_alpha: float, match_tol: float,
                            same_basis: Callable[[str, str], bool] | None = None) -> list[tuple[str, dict]]:
    """Consecutive chain_hourly rows of one occupant → [(change_type, detail)] for D3/D3b/D3c."""
    out: list[tuple[str, dict]] = []
    rows = sorted(rows, key=lambda r: r["hour_ts"])
    for a, b in zip(rows, rows[1:]):
        sa, sb = a.get("source"), b.get("source")
        if sa != sb and not (same_basis and same_basis(sa, sb)):
            continue
        pa, da = hourly_perp(a)
        pb, db_ = hourly_perp(b)
        if None in (pa, da, pb, db_):
            continue
        dp, dd = pb - pa, db_ - da
        detail = {"perp_before": pa, "perp_after": pb, "decay_before": da, "decay_after": db_, "delta_perp": dp,
                  "delta_decay": dd, "hour_ts": int(b["hour_ts"]), "block_number": b.get("block_number")}
        thr = max(min_alpha, 0.01 * pa)
        if dp <= -thr and dd >= (1 - match_tol) * abs(dp):
            out.append(("perpetual_off", detail))
        elif dp <= -thr:
            out.append(("lock_reduced", detail))
        elif dd > 0 and abs(dp) < min_alpha and dd >= min_alpha:
            out.append(("decaying_lock_added", detail))
    return out


async def record_lock_transition(db: "Database", okey: str, netuid: int, ctype: str, detail: dict, *,
                                 observed_at: int, run_id: str, detected_via: str = "chain_hourly",
                                 extrinsic_id: str | None = None) -> int | None:
    at = int(detail["hour_ts"])
    day = at // DAY
    if ctype == "perpetual_off":
        if await _existing(db, okey, ("perpetual_off",), None, at, slack=DAY):
            return None
        ch = OwnerChange(okey, netuid, "perpetual_off", at, observed_at, detected_via, s3_base_penalty=1.0,
                         block_number=detail.get("block_number"), extrinsic_id=extrinsic_id, evidence=detail,
                         run_id=run_id)
        return await record_change(
            db, ch, event_type="owner_perpetual_off", review_kind="owner_extraction_candidate",
            suggested_flag="owner_extraction", priority=1,
            payload={k: detail.get(k) for k in ("perp_before", "perp_after", "decay_before", "decay_after")}
            | {"detected_via": detected_via, "hour_ts": at - at % 3600, "block_number": detail.get("block_number"),
               "extrinsic_id": extrinsic_id},
            event_dedupe=f"{okey}|perpoff|{day}")
    etype = {"lock_reduced": "owner_lock_reduced", "decaying_lock_added": "owner_decaying_lock_added"}[ctype]
    ch = OwnerChange(okey, netuid, ctype, at, observed_at, detected_via, s3_base_penalty=0.0,
                     block_number=detail.get("block_number"), evidence=detail, run_id=run_id)
    return await record_change(db, ch, event_type=etype,
                               review_kind="owner_change_whitelist" if ctype == "lock_reduced" else None,
                               review_event_type=etype,
                               payload={"delta_perp": detail["delta_perp"], "delta_decay": detail["delta_decay"],
                                        "hour_ts": at - at % 3600},
                               event_dedupe=f"{okey}|{etype}|{day}")


# ── D4 / D4b extraction ──────────────────────────────────────────────────────────────────────────────────────
async def _extraction_recent(db: "Database", okey: str, at: int) -> bool:
    return bool(await db.fetchval("SELECT 1 FROM owner_changes WHERE occupant_key = ? AND change_type = "
                                  "'extraction_detected' AND event_at > ?", (okey, at - 7 * DAY), writer=True))


async def detect_extraction(db: "Database", occupant_key: str, netuid: int, s1: RawMetric, *, now: int,
                            run_id: str, events: Sequence[StakeEvent] = ()) -> int | None:
    """D4: a hard S1 trigger in W → one ``extraction_detected`` change per 7 d (a D4b row counts)."""
    vj = s1.value_json
    hard = list(vj.get("hard") or [])
    if not hard or await _extraction_recent(db, occupant_key, now):
        return None
    outs = sorted((e for e in events if e.counts_as_outflow), key=lambda e: -(e.alpha or 0))
    ats = [e.event_at for e in events if e.counts_as_outflow]
    at = max(ats) if ats else now
    ch = OwnerChange(occupant_key, netuid, "extraction_detected", at, now, "features",
                     evidence={"reasons": hard, "trigger_ids": vj.get("trigger_ids"), "event_ats": ats[-50:]},
                     run_id=run_id)
    return await record_change(
        db, ch, event_type="owner_extraction_detected", review_kind="owner_extraction_candidate",
        suggested_flag="owner_extraction", priority=1,
        payload={"reasons": hard, "sell_ratio": vj.get("sell_ratio"), "max_impact7": vj.get("max_7d_sell_impact"),
                 "tao_rcv_30d": vj.get("tao_rcv_30d"), "out_alpha_30d": vj.get("out_alpha"),
                 "earned_30d": vj.get("earned_alpha"), "top_event_uids": [e.event_uid for e in outs[:5]]},
        event_dedupe=f"{occupant_key}|extract|{iso_week(at)}")


async def emit_full_exit_extraction(db: "Database", occupant_key: str, netuid: int, new_events: Sequence[StakeEvent],
                                    *, now: int, run_id: str, whitelist: WhitelistSet | None = None) -> int:
    """D4b at ingest: a newly inserted, non-whitelisted ``sell_full`` → ``extraction_detected`` +
    ``owner_extraction_detected(reasons=['full_exit_extrinsic'])`` (numbers not computed yet → null)."""
    n = 0
    ws = whitelist or await WhitelistSet.load(db, occupant_key)
    for e in new_events:
        if e.kind != "sell_full" or not e.success or ws.stake_event(e, now):
            continue
        if await _extraction_recent(db, occupant_key, e.event_at + 7 * DAY) and \
                await db.fetchval("SELECT 1 FROM owner_changes WHERE occupant_key = ? AND change_type = "
                                  "'extraction_detected' AND event_at BETWEEN ? AND ?",
                                  (occupant_key, e.event_at - 7 * DAY, e.event_at + 7 * DAY), writer=True):
            continue
        ch = OwnerChange(occupant_key, netuid, "extraction_detected", e.event_at, now, "staking_activity",
                         block_number=e.block_number, extrinsic_id=e.extrinsic_id,
                         evidence={"reasons": ["full_exit_extrinsic"], "trigger_ids": [e.trigger_id],
                                   "event_ats": [e.event_at], "event_uid": e.event_uid}, run_id=run_id)
        cid = await record_change(
            db, ch, event_type="owner_extraction_detected", review_kind="owner_extraction_candidate",
            suggested_flag="owner_extraction", priority=1,
            payload={"reasons": ["full_exit_extrinsic"], "sell_ratio": None, "max_impact7": None, "tao_rcv_30d": None,
                     "out_alpha_30d": None, "earned_30d": None, "top_event_uids": [e.event_uid]},
            event_dedupe=f"{occupant_key}|extract|{iso_week(e.event_at)}")
        n += 1 if cid else 0
    return n


# ── D6 toggles, D7/D8 source disagreement ────────────────────────────────────────────────────────────────────
async def detect_toggles(db: "Database", occupant_key: str, netuid: int, prev: dict | None, curr: dict, *, now: int,
                         run_id: str) -> int:
    n = 0
    if prev is None:
        return 0
    pairs = (("rufus_auto_lock", "auto_lock", "owner_auto_lock_changed", "rufus"),
             ("owner_cut_enabled", "owner_cut", "owner_cut_toggled", "tmc_snapshot"))
    for col, stem, etype, via in pairs:
        a, b = prev.get(col), curr.get(col)
        if a is None or b is None or bool(a) == bool(b):
            continue
        ctype = f"{stem}_{'enabled' if b else 'disabled'}"
        ch = OwnerChange(occupant_key, netuid, ctype, now, now, via, old_value=str(int(bool(a))),
                         new_value=str(int(bool(b))), run_id=run_id)
        cid = await record_change(db, ch, event_type=etype,
                                  payload={"old": bool(a), "new": bool(b), "source": via,
                                           "block_number": curr.get("block_number")},
                                  event_dedupe=f"{occupant_key}|{etype}|{now // DAY}")
        n += 1 if cid else 0
    return n


async def detect_source_disagreement(db: "Database", occupant_key: str, netuid: int, field: str,
                                     values: dict[str, Any], *, now: int, since: int, run_id: str) -> int | None:
    from app.integrity.review import enqueue
    vals = {k: v for k, v in values.items() if v is not None}
    if len(set(vals.values())) < 2:
        return None
    eid = await emit_event(db, event_type="owner_source_disagreement", occupant_key=occupant_key, netuid=netuid,
                           event_at=now, observed_at=now,
                           payload={"netuid": netuid, "field": field, "values": vals, "since": since},
                           dedupe_key=f"{occupant_key}|srcdis|{field}|{now // DAY}", source="integrity", run_id=run_id)
    await enqueue(db, occupant_key=occupant_key, kind="owner_source_disagreement",
                  dedupe_base=f"owner_source_disagreement|{occupant_key}:{field}", ref_table="events", ref_id=eid,
                  priority=3, summary=f"{field} disagrees on SN{netuid}", payload={"values": vals, "since": since},
                  event_type="owner_source_disagreement", now=now)
    return eid


# ── drill CLI ────────────────────────────────────────────────────────────────────────────────────────────────
async def _drill(db_path: str, netuid: int, fake: bool) -> int:
    from app.integrity.features import compute_occupant
    from app.storage.db import Database
    from app.util.time import now_ts
    db = Database(db_path)
    await db.connect()
    try:
        occ = await db.fetchone("SELECT o.* FROM subnets s JOIN occupants o USING (occupant_key) WHERE s.netuid = ?",
                                (netuid,), writer=True)
        if occ is None:
            print(json.dumps({"error": "no_current_occupant"}))
            return 1
        now = now_ts()
        prev = await db.fetchone("SELECT owner_coldkey FROM owner_snapshots WHERE occupant_key = ? ORDER BY "
                                 "observed_at DESC LIMIT 1", (occ["occupant_key"],), writer=True)
        old = (prev or {}).get("owner_coldkey") or "5" + "C" * 47
        new = "5" + "D" * 47
        out: dict[str, Any] = {"occupant_key": occ["occupant_key"], "old": old, "new": new}
        if fake:
            async def classify() -> tuple[str, str]:
                return classify_coldkey_change(old_ck=old, new_ck=new, netuid=netuid)
            cid = await detect_coldkey_change(db, occ["occupant_key"], netuid, old, new, event_at=now, observed_at=now,
                                              run_id="drill", detected_via="tmc_snapshot", classify=classify)
            out["change"] = await db.fetchone("SELECT * FROM owner_changes WHERE id = ?", (cid,), writer=True)
            out["events"] = await db.fetchall("SELECT event_type, severity, dedupe_key FROM events WHERE "
                                              "occupant_key = ? AND observed_at >= ?", (occ["occupant_key"], now),
                                              writer=True)
            out["reviews"] = await db.fetchall("SELECT id, kind, event_type, status FROM review_queue WHERE "
                                               "occupant_key = ? AND created_at >= ?", (occ["occupant_key"], now),
                                               writer=True)
        rows = await compute_occupant(db, dict(occ), t=now + 1)
        out["owner_control_change_trigger"] = next((r.value for r in rows if r.key == "owner_control_change_trigger"),
                                                   None)
        print(json.dumps(out, indent=2, default=str))
        return 0
    finally:
        await db.close()


def main(argv: list[str] | None = None) -> int:
    import argparse
    import asyncio
    ap = argparse.ArgumentParser(prog="python -m app.integrity.detectors")
    sub = ap.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("drill", help="dry run on a DB COPY (never the live DB)")
    d.add_argument("--db", required=True)
    d.add_argument("--netuid", type=int, required=True)
    d.add_argument("--fake-coldkey-change", action="store_true")
    a = ap.parse_args(argv)
    import app.config as app_config
    if app_config.settings.path(app_config.settings.DB_PATH).resolve() == __import__("pathlib").Path(a.db).resolve():
        print("refusing to drill on the live DB; make a copy with sqlite3 .backup first")
        return 2
    return asyncio.run(_drill(a.db, a.netuid, a.fake_coldkey_change))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = ["record_change", "classify_coldkey_change", "detect_coldkey_change", "detect_hotkey_repoint",
           "pair_identity_churn", "detect_lock_transitions", "record_lock_transition", "detect_extraction",
           "emit_full_exit_extraction", "detect_toggles", "detect_source_disagreement", "iso_week", "hourly_perp",
           "replace"]
