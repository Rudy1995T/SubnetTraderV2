"""``owner_collect_daily`` (spec 11 §2.3, §8.2): owner keys/cut (C1, from 04's ``/subnets/`` snapshot), position (C2),
owner-hotkey trace (C3), owner staking activity (C4, watermarked), Rufus (C6), subnetradar (C7); one
``owner_snapshots`` row per occupant; D1/D2/D3(Rufus)/D4b/D6/D7/D8 and the reconciliation event.

Window rule (02 §E3/§G3): a run started inside 20:30–23:30Z starts no new coldkey after 23:15Z and issues no request
after 23:30Z; a manual ``run-job`` outside the window uses ``started_at + timeout − 900 s`` as its soft deadline.
Watermarks are committed per coldkey in the same transaction as that coldkey's stake events."""
from __future__ import annotations

import hashlib
import json
import time
from collections import Counter, defaultdict
from typing import TYPE_CHECKING, Any, Callable

import app.config as app_config
from app.integrity import detectors as D
from app.integrity.classify import classify_stake_event, supersede_duplicates
from app.integrity.models import OwnerKeys, RufusSubnet, StakeEvent
from app.integrity.owner_sources import (PacedTmc, RunBudgetSpent, activity_time_format, fetch_activity,
                                         fetch_coldkey_stakes, fetch_neuron_series, fetch_owner_keys, fetch_rufus,
                                         fetch_subnetradar, list_snapshot, map_activity_row, map_coldkey_stakes,
                                         map_neuron_rows, parse_rufus)
from app.integrity.whitelist import WhitelistSet
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.storage.events import emit_event
from app.util.time import floor_day, now_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.http import Http
    from app.storage.db import Database

JOB = "owner_collect_daily"
DAY = 86400
HOUR = 3600


async def get_state(db: "Database", key: str, default: Any = None) -> Any:
    v = await db.fetchval("SELECT value_json FROM job_state WHERE job = ? AND key = ?", (JOB, key), writer=True)
    return json.loads(v) if v else default


async def set_state(db: "Database", key: str, value: Any) -> None:
    await db.execute("INSERT INTO job_state (job, key, value_json, updated_at) VALUES (?,?,?,?) ON CONFLICT (job, key) "
                     "DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at",
                     (JOB, key, json.dumps(value, default=str), now_ts()))


def deadlines(started: int, timeout_s: int) -> tuple[int, int]:
    """(soft, hard): inside the 20:30–23:30Z window → min(started + timeout − 900, 23:15Z) / 23:30Z."""
    d = floor_day(started)
    w0, w_soft, w_end = d + 20 * HOUR + 30 * 60, d + 23 * HOUR + 15 * 60, d + 23 * HOUR + 30 * 60
    soft = started + timeout_s - 900
    hard = started + timeout_s
    if w0 <= started < w_end:
        soft, hard = min(soft, w_soft), min(hard, w_end)
    return soft, hard


async def _store_raw(db: "Database", endpoint: str, payload: Any, *, now: int, run_id: str, source: str) -> None:
    await db.execute("INSERT OR IGNORE INTO chain_rest_snapshots (endpoint, params, observed_at, run_id, http_status, "
                     "payload_json, source) VALUES (?, '', ?, ?, 200, ?, ?)",
                     (endpoint, now, run_id, json.dumps(payload, default=str), source))


async def _stored_rufus(db: "Database") -> Any | None:
    v = await db.fetchval("SELECT payload_json FROM chain_rest_snapshots WHERE endpoint = 'rufus/table.json' "
                          "ORDER BY observed_at DESC LIMIT 1", writer=True)
    return json.loads(v) if v else None


def _reg_ts(occ: dict) -> int | None:
    v = occ.get("registered_ts") or occ.get("registered_at_est")
    return int(v) if v is not None else None


class Collector:
    """One run. ``clock``/``sleep`` reach ``PacedTmc`` (tests inject a fake clock)."""

    def __init__(self, db: "Database", http: "Http", *, now: int, run_id: str, timeout_s: int = 10800,
                 tmc: PacedTmc | None = None, clock: Callable[[], float] = time.time, sleep=None) -> None:
        from app.collectors.chain_tmc import TmcClient
        self.s = app_config.settings
        self.db, self.http, self.now, self.run_id = db, http, int(now), run_id
        self.soft, self.hard = deadlines(self.now, timeout_s)
        kw = {"sleep": sleep} if sleep is not None else {}
        self.tmc = tmc or PacedTmc(TmcClient(http), per_min=int(self.s.OWNER_TMC_MAX_PER_MIN),
                                   per_run=int(self.s.OWNER_TMC_MAX_PER_RUN), stop_at=self.hard, clock=clock, **kw)
        self.clock = clock
        self.stats: Counter = Counter()
        self.rufus: dict[int, RufusSubnet] = {}
        self.rufus_baked_at: int | None = None
        self.rufus_status = "skip"
        self.radar: list[dict] = []
        self.radar_status = "skip"
        self.budget_out = False
        self.c2: dict[str, tuple[dict | None, str]] = {}
        self.c4_status: dict[str, str] = {}
        self.touched: set[str] = set()
        self.snapshots = 0
        self.events = 0
        self.c8_status = "skip"
        self.taostats_swaps: set[tuple[int, str]] = set()

    # ── shared sources ────────────────────────────────────────────────────────────────────────────────────
    async def load_rufus(self) -> None:
        if not self.s.OWNER_RUFUS_ENABLED:
            return
        last = await get_state(self.db, "rufus_baked_at")
        table, st, baked_at, raw = await fetch_rufus(self.http, table_url=self.s.OWNER_RUFUS_TABLE_URL,
                                                     baked_url=self.s.OWNER_RUFUS_BAKED_AT_URL, last_baked_at=last)
        for ep, body in raw.items():
            await _store_raw(self.db, ep, body, now=self.now, run_id=self.run_id, source="rufus")
        if st == "unchanged":
            table = await _stored_rufus(self.db)
        if table is not None:
            self.rufus = parse_rufus(table)
            self.rufus_baked_at = baked_at or last
            if baked_at:
                await set_state(self.db, "rufus_baked_at", baked_at)
        self.rufus_status = st if table is not None or st.startswith("fail") else "no_table"
        if not self.rufus and table is not None:
            self.rufus_status = "schema_drift"

    async def load_radar(self) -> None:
        if not self.s.OWNER_SUBNETRADAR_ENABLED:
            return
        rows, st, body = await fetch_subnetradar(self.http, self.s.OWNER_SUBNETRADAR_URL)
        self.radar, self.radar_status = rows, st
        if body is not None:
            await _store_raw(self.db, "subnetradar/swaps",
                             {"sha256": hashlib.sha256(body.encode()).hexdigest(), "bytes": len(body), "rows": rows},
                             now=self.now, run_id=self.run_id, source="subnetradar")

    # ── C2 / C4 per coldkey ───────────────────────────────────────────────────────────────────────────────
    async def coldkey_stakes(self, ck: str) -> tuple[dict | None, str]:
        if ck not in self.c2:
            if self.budget_out:
                return None, "skip:budget"
            try:
                self.c2[ck] = await fetch_coldkey_stakes(self.tmc, ck)
            except RunBudgetSpent:
                self.budget_out = True
                return None, "skip:budget"
        return self.c2[ck]

    async def activity(self, ck: str, occs: list[dict], backfill_left: list[int]) -> None:
        s = self.s
        wm = await get_state(self.db, f"activity_wm:{ck}")
        first = wm is None
        frm = self.now - int(s.OWNER_ACTIVITY_BACKFILL_DAYS) * DAY if first else \
            int(wm["ts"]) - int(s.OWNER_ACTIVITY_OVERLAP_SEC)
        cov_start = frm if first else wm.get("coverage_start", frm)
        pages = int(s.OWNER_ACTIVITY_MAX_PAGES)
        if first or (wm or {}).get("truncated"):
            pages = max(1, min(pages, backfill_left[0] if backfill_left[0] > 0 else 1))
        try:
            rows, st, trunc = await fetch_activity(self.tmc, ck, frm, self.now,
                                                   page_limit=int(s.OWNER_ACTIVITY_PAGE_LIMIT), max_pages=pages,
                                                   time_format=activity_time_format(s.OWNER_ACTIVITY_TIME_FORMAT))
        except RunBudgetSpent:
            self.budget_out = True
            self.c4_status[ck] = "skip:budget"
            return
        if first or (wm or {}).get("truncated"):
            backfill_left[0] -= max(0, len(rows) // max(1, int(s.OWNER_ACTIVITY_PAGE_LIMIT)))
        self.c4_status[ck] = st
        if trunc:
            self.stats["c4_truncated"] += 1
        if st.startswith("fail"):
            self.stats["c4_fail"] += 1
            return
        mapped = [m for m in (map_activity_row(r) for r in rows) if m]
        matched: set = set()
        async with self.db.transaction():
            for occ in occs:
                okey, n = occ["occupant_key"], int(occ["netuid"])
                reg = _reg_ts(occ)
                new: list[StakeEvent] = []
                for m in mapped:
                    if reg is not None and m["event_at"] < reg:
                        continue                                  # before this occupant existed
                    e = classify_stake_event(m, occupant_netuid=n, owner_coldkey=ck)
                    if e is None:
                        continue
                    matched.add(m["id"] or e.event_uid)
                    if await self._insert_event(okey, n, e, source="tmc_staking_activity"):
                        new.append(e)
                if new:
                    self.touched.add(okey)
                    ws = await WhitelistSet.load(self.db, okey)
                    self.events += await D.emit_full_exit_extraction(self.db, okey, n, new, now=self.now,
                                                                     run_id=self.run_id, whitelist=ws)
                    for e in new:
                        if e.kind == "perpetual_off" and e.success:
                            cid = await D.record_lock_transition(
                                self.db, okey, n, "perpetual_off",
                                {"hour_ts": e.event_at, "block_number": e.block_number, "perp_before": None,
                                 "perp_after": None, "decay_before": None, "decay_after": None, "coldkey": ck},
                                observed_at=self.now, run_id=self.run_id, detected_via="staking_activity",
                                extrinsic_id=e.extrinsic_id)
                            self.events += 1 if cid else 0
            self.stats["c4_rows"] += len(mapped)
            self.stats["c4_rows_dropped"] += sum(1 for m in mapped if m["id"] not in matched)
            last_ts = max((m["event_at"] for m in mapped), default=None)
            prev_ts = int(wm["ts"]) if wm else frm
            if st == "ok" and not trunc:
                new_ts = self.now                             # complete sequence: covered up to now
            else:
                new_ts = last_ts if last_ts is not None else prev_ts   # truncated/partial: only over received rows
            new_ts = max(new_ts, prev_ts)
            # ``ts`` is the end of the contiguous coverage [coverage_start, ts]; S1 needs it ≤ 36 h old (§2.5)
            await set_state(self.db, f"activity_wm:{ck}",
                            {"ts": int(new_ts), "coverage_start": int(cov_start), "truncated": bool(trunc),
                             "last_ok": int(new_ts),
                             "block": max((m["block_number"] or 0 for m in mapped), default=(wm or {}).get("block"))})

    async def _insert_event(self, okey: str, n: int, e: StakeEvent, *, source: str) -> bool:
        r = await self.db.execute(
            "INSERT OR IGNORE INTO owner_stake_events (event_uid, occupant_key, netuid, owner_coldkey, signer, hotkey, "
            "dest_coldkey, function, kind, counts_as_outflow, from_netuid, to_netuid, alpha, tao, price, is_proxy, "
            "success, block_number, extrinsic_id, event_at, observed_at, source, run_id, raw_json) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (e.event_uid, okey, n, e.owner_coldkey, e.signer, e.hotkey, e.dest_coldkey, e.function, e.kind,
             e.counts_as_outflow, e.from_netuid, e.to_netuid, e.alpha, e.tao, e.price, e.is_proxy, e.success,
             e.block_number, e.extrinsic_id, e.event_at, self.now, source, self.run_id,
             json.dumps(e.raw, default=str) if e.raw else None))
        return r.rowcount > 0

    # ── per occupant ──────────────────────────────────────────────────────────────────────────────────────
    async def occupant(self, occ: dict, keys: OwnerKeys | None, c1: str) -> None:
        db, s = self.db, self.s
        okey, n = occ["occupant_key"], int(occ["netuid"])
        prev = await db.fetchone("SELECT * FROM owner_snapshots WHERE occupant_key = ? AND observed_at < ? "
                                 "ORDER BY observed_at DESC LIMIT 1", (okey, self.now), writer=True)
        prev_raw = json.loads(prev["raw_json"] or "{}") if prev else {}
        st: dict[str, str] = {"c1": c1}
        ck, hk = (keys.coldkey, keys.hotkey) if keys else (None, None)
        # C2
        c2 = None
        if ck:
            payload, st["c2"] = await self.coldkey_stakes(ck)
            c2 = map_coldkey_stakes(payload, n)
            if payload is None and st["c2"] == "fail:404" and not (keys and keys.personal_stake):
                c2, st["c2"] = map_coldkey_stakes({"subnets": []}, n), "ok:404_empty"
            elif payload is None and st["c2"] == "fail:404":
                st["c2"] = "fail:404_inconsistent"
        else:
            st["c2"] = "skip:no_keys"
        st["c4"] = self.c4_status.get(ck or "", "skip")
        # C3
        st["c3"] = "skip"
        hk_last = None
        if hk and not self.budget_out and self.tmc.remaining > 0:
            try:
                rows, st["c3"] = await fetch_neuron_series(self.tmc, n, hk, s.OWNER_NEURON_SPAN)
            except RunBudgetSpent:
                self.budget_out, rows, st["c3"] = True, None, "skip:budget"
            if rows:
                mapped = map_neuron_rows(rows)
                await db.insert_or_ignore("owner_position_hourly", [
                    {"occupant_key": okey, "hotkey": hk, "hour_ts": m["hour_ts"], "block_number": m["block_number"],
                     "alpha_stake": m["alpha_stake"], "conviction": m["conviction"],
                     "conviction_locked_alpha": m["conviction_locked_alpha"],
                     "conviction_perpetual_alpha": m["conviction_perpetual_alpha"],
                     "conviction_decaying_alpha": m["conviction_decaying_alpha"], "event_at": m["event_at"],
                     "observed_at": self.now, "source": "tmc_neuron_1w", "run_id": self.run_id} for m in mapped])
                hk_last = mapped[-1] if mapped else None
            if st["c3"].startswith("fail"):
                self.stats["c3_fail"] += 1
        # chain_hourly copy (SSE row ≤ 2 h: owner_personal_*, price, pool)
        ch = await db.fetchone("SELECT * FROM chain_hourly WHERE netuid = ? AND occupant_key = ? AND hour_ts >= ? "
                               "AND price IS NOT NULL ORDER BY hour_ts DESC LIMIT 1",
                               (n, okey, self.now - 3 * HOUR), writer=True)

        def a(x):
            return None if x is None else float(x) / 1e9
        ru = self.rufus.get(n)
        if ru is not None and ru.registration_block not in (None, occ["network_registered_at"]):
            ru = None
        rufus_stale = None
        if ru is not None and self.rufus_baked_at:
            rufus_stale = 1 if self.now - self.rufus_baked_at > int(s.OWNER_RUFUS_MAX_AGE_HOURS) * HOUR else 0
        st["c6"] = self.rufus_status if ru is not None else ("absent" if self.rufus else self.rufus_status)
        st["c7"] = self.radar_status
        st["c8"] = self.c8_status
        from app.registry import resolved
        ident = None
        try:
            ident = await resolved(db, okey, "owner_name")
        except Exception:
            ident = None
        block = keys.block_number if keys else None
        reg = _reg_ts(occ)
        event_at = (reg + (block - int(occ["network_registered_at"])) * int(s.BLOCK_SECONDS)) if (block and reg) \
            else self.now
        # SSE chain_hourly first: the REST owner_personal_* are 0 in the list and intermittently 0 in the detail
        # answer too (probe 2026-10-18: SN64 646,674 α, then 0 fifty minutes later; 11 §2.3.1)
        def pick(col: str, alt: float | None) -> float | None:
            v = a((ch or {}).get(col))
            return v if v is not None else alt
        tmc_stake = pick("owner_personal_stake", keys.personal_stake if keys else None)
        tmc_locked = pick("owner_personal_locked_alpha", keys.locked if keys else None)
        tmc_perp = pick("owner_personal_perpetual_alpha", keys.perpetual if keys else None)
        tmc_decay = pick("owner_personal_decaying_alpha", keys.decaying if keys else None)
        row = {
            "occupant_key": okey, "netuid": n, "observed_at": self.now, "event_at": int(event_at),
            "source": "owner_collect", "run_id": self.run_id, "block_number": block, "owner_coldkey": ck,
            "owner_hotkey": hk, "keys_source": keys.source if keys else None,
            "owner_cut_per_block": keys.owner_cut_per_block if keys else None, "owner_cut_cumulative": None,
            "pending_owner_cut": keys.pending_owner_cut if keys else None,
            "owner_cut_enabled": None if not keys or keys.owner_cut_enabled is None else int(keys.owner_cut_enabled),
            "auto_lock_enabled_tmc": None, "tmc_owner_personal_stake": tmc_stake, "tmc_owner_locked": tmc_locked,
            "tmc_owner_perpetual": tmc_perp, "tmc_owner_decaying": tmc_decay,
            "owner_total_alpha": c2["total"] if c2 else None, "owner_locked_alpha": c2["locked"] if c2 else None,
            "owner_available_alpha": c2["available"] if c2 else None,
            "owner_hotkeys_json": json.dumps(c2["hotkeys"]) if c2 else None,
            "owner_hk_alpha_stake": (hk_last or {}).get("alpha_stake"),
            "owner_hk_conviction": (hk_last or {}).get("conviction"),
            "rufus_baked_at": self.rufus_baked_at if ru else None, "rufus_stale": rufus_stale if ru else None,
            "rufus_auto_lock": None if not ru or ru.auto_lock_enabled is None else int(ru.auto_lock_enabled),
            "rufus_owner_perp": (ru.owner_perp if ru.owner_perp is not None else 0.0) if ru else None,
            "rufus_owner_decay": ru.owner_decay if ru else None, "rufus_owner_pct": ru.owner_pct if ru else None,
            "rufus_to_owner_perp": ru.to_owner_perp if ru else None,
            "rufus_to_owner_lockers": ru.to_owner_lockers if ru else None,
            "rufus_challenger_max_pct": ru.challenger_max_pct if ru else None,
            "rufus_days_to_king": ru.days_to_king if ru else None,
            "total_subnet_stake": a((ch or {}).get("total_subnet_stake")), "tao_in": a((ch or {}).get("tao_liquidity")),
            "price": (ch or {}).get("price"), "initial_alpha_est": prev["initial_alpha_est"] if prev else None,
            "identity_name": ident.value if ident and ident.status == "present" else None,
            "fetch_status_json": json.dumps(st),
            "raw_json": json.dumps({"c1": keys.raw if keys else None, "c2_rows": (c2 or {}).get("rows"),
                                    "rufus_challenger_coldkeys": list(ru.challenger_coldkeys) if ru else [],
                                    "rufus_owner_perpetual_flag": ru.owner_perpetual_flag if ru else None,
                                    "rufus_owner_coldkey": ru.owner_coldkey if ru else None,
                                    "first_emission_block": keys.first_emission_block if keys else None},
                                   default=str)}
        await db.insert_or_ignore("owner_snapshots", [row])
        self.snapshots += 1
        if st["c2"].startswith("fail"):
            self.stats["c2_fail"] += 1
        if not ck:
            self.stats["keys_missing"] += 1
        await self.detect(occ, prev, prev_raw, row, keys, ru)

    async def detect(self, occ: dict, prev: dict | None, prev_raw: dict, row: dict, keys: OwnerKeys | None,
                     ru: RufusSubnet | None) -> None:
        db, s, now = self.db, self.s, self.now
        okey, n = occ["occupant_key"], int(occ["netuid"])
        if prev is None or keys is None:
            return
        old_ck, new_ck = prev.get("owner_coldkey"), row.get("owner_coldkey")

        async def classify() -> tuple[str, str]:
            share, status = None, "skip"
            if old_ck and prev.get("owner_total_alpha"):
                payload, status = await self.coldkey_stakes(old_ck)
                m = map_coldkey_stakes(payload, n)
                if m is not None and prev["owner_total_alpha"] > 0:
                    share = m["total"] / prev["owner_total_alpha"]
            return D.classify_coldkey_change(taostats_is_swap=(n, new_ck) in self.taostats_swaps,
                                             challenger_coldkeys=prev_raw.get("rufus_challenger_coldkeys") or [],
                                             days_to_king=prev.get("rufus_days_to_king"), radar_rows=self.radar,
                                             old_ck=old_ck, new_ck=new_ck, netuid=n, old_ck_share=share,
                                             old_ck_status=status)
        cid = await D.detect_coldkey_change(db, okey, n, old_ck, new_ck, event_at=int(row["event_at"]), observed_at=now,
                                            run_id=self.run_id, detected_via="tmc_snapshot", classify=classify,
                                            block_number=row.get("block_number"))
        self.events += 1 if cid else 0
        cid = await D.detect_hotkey_repoint(db, okey, n, prev.get("owner_hotkey"), row.get("owner_hotkey"),
                                            event_at=int(row["event_at"]), observed_at=now, run_id=self.run_id,
                                            detected_via="tmc_snapshot", block_number=row.get("block_number"))
        self.events += 1 if cid else 0
        self.events += await D.detect_toggles(db, okey, n, prev, row, now=now, run_id=self.run_id)
        # D3 via the Rufus owner row: perpetual true → false
        if prev_raw.get("rufus_owner_perpetual_flag") is True and ru is not None and ru.owner_perpetual_flag is False:
            cid = await D.record_lock_transition(
                db, okey, n, "perpetual_off",
                {"hour_ts": now, "block_number": row.get("block_number"), "perp_before": prev.get("rufus_owner_perp"),
                 "perp_after": ru.owner_perp, "decay_before": prev.get("rufus_owner_decay"),
                 "decay_after": ru.owner_decay}, observed_at=now, run_id=self.run_id, detected_via="rufus")
            self.events += 1 if cid else 0
        # D7 source disagreement (> 24 h: the previous snapshot disagreed too)
        vals = {"tmc": new_ck, "rufus": ru.owner_coldkey if (ru and not row.get("rufus_stale")) else None,
                "registry": occ.get("owner_coldkey")}
        cur = {k: v for k, v in vals.items() if v}
        if len(set(cur.values())) > 1 and prev_raw.get("disagree_owner_coldkey") and \
                now - int(prev["observed_at"]) >= 20 * HOUR:
            eid = await D.detect_source_disagreement(db, okey, n, "owner_coldkey", cur, now=now,
                                                     since=int(prev_raw["disagree_owner_coldkey"]), run_id=self.run_id)
            self.events += 1 if eid else 0
        if len(set(cur.values())) > 1:
            raw = json.loads(row["raw_json"])
            raw["disagree_owner_coldkey"] = prev_raw.get("disagree_owner_coldkey") or now
            await db.execute("UPDATE owner_snapshots SET raw_json = ? WHERE occupant_key = ? AND observed_at = ?",
                             (json.dumps(raw, default=str), okey, now))
        # reconciliation (info only; C5 disabled, §2.3.1)
        pt, ctot = prev.get("owner_total_alpha"), row.get("owner_total_alpha")
        if pt is not None and ctot is not None and prev.get("owner_coldkey") == new_ck:
            flows = await db.fetchall("SELECT kind, counts_as_outflow, alpha FROM owner_stake_events WHERE "
                                      "occupant_key = ? AND event_at > ? AND event_at <= ? AND success = 1 AND "
                                      "superseded_by IS NULL", (okey, int(prev["observed_at"]), now), writer=True)
            outflow = sum(f["alpha"] or 0 for f in flows if f["counts_as_outflow"])
            inflow = sum(f["alpha"] or 0 for f in flows if f["kind"] in ("buy", "swap_in", "transfer_in"))
            unexpl = pt + inflow - outflow - ctot
            if unexpl > max(float(s.OWNER_RECONCILE_MIN_ALPHA), 0.02 * pt):
                eid = await emit_event(db, event_type="owner_position_unexplained", occupant_key=okey, netuid=n,
                                       event_at=now, observed_at=now,
                                       payload={"netuid": n, "prev_total": pt, "curr_total": ctot, "inflow": inflow,
                                                "outflow": outflow, "unexplained": unexpl},
                                       dedupe_key=f"{okey}|unexpl|{now // DAY}", source="integrity", run_id=self.run_id)
                self.events += 1 if eid else 0

    async def radar_misses(self, occs: list[dict], keys: dict[str, OwnerKeys | None]) -> None:
        """D8: a subnetradar owner change for a current occupant (after its registration, ≤ 7 d old) that D1 has not
        recorded within 48 h → ``owner_source_disagreement`` (field ``owner_change``)."""
        for occ in occs:
            okey, n = occ["occupant_key"], int(occ["netuid"])
            reg = _reg_ts(occ) or 0
            k = keys.get(okey)
            prev_cks = {r["owner_coldkey"] for r in await self.db.fetchall(
                "SELECT DISTINCT owner_coldkey FROM owner_snapshots WHERE occupant_key = ? AND owner_coldkey IS NOT "
                "NULL", (okey,), writer=True)}
            for r in self.radar:
                at = r.get("date_ts")
                if at is None or at < reg or at < self.now - 7 * DAY or at > self.now - 2 * DAY:
                    continue
                if r.get("netuid") not in (None, n):
                    continue
                if r.get("netuid") is None and r.get("old_coldkey") not in prev_cks:
                    continue
                seen = await self.db.fetchval("SELECT 1 FROM owner_changes WHERE occupant_key = ? AND new_value = ? "
                                              "LIMIT 1", (okey, r.get("new_coldkey")), writer=True)
                if seen or (k and k.coldkey == r.get("new_coldkey") and r.get("old_coldkey") not in prev_cks):
                    continue
                eid = await D.detect_source_disagreement(
                    self.db, okey, n, "owner_change", {"subnetradar": r.get("new_coldkey"),
                                                       "tmc": k.coldkey if k else None},
                    now=self.now, since=int(at), run_id=self.run_id)
                self.events += 1 if eid else 0

    # ── run ───────────────────────────────────────────────────────────────────────────────────────────────
    async def run(self, *, netuids: set[int] | None = None) -> JobOutcome:
        """``netuids`` limits the run to those subnets (operator spot checks; the scheduled run passes None)."""
        from app.integrity.features import occupants_for_run
        s, db = self.s, self.db
        occs = [o for o in await occupants_for_run(db) if netuids is None or int(o["netuid"]) in netuids]
        list_rows, list_at = await list_snapshot(db, now=self.now)
        mode = s.OWNER_KEYS_SOURCE
        await self.load_rufus()
        await self.load_radar()
        keys: dict[str, OwnerKeys | None] = {}
        c1: dict[str, str] = {}
        live: list[dict] = []
        for occ in occs:
            try:
                k, st = await fetch_owner_keys(self.tmc if not self.budget_out else None, int(occ["netuid"]),
                                               list_rows=list_rows, source_mode=mode)
            except RunBudgetSpent:
                self.budget_out, k, st = True, None, "skip:budget"
            if k and k.network_registered_at and k.network_registered_at != int(occ["network_registered_at"]):
                self.stats["skip_reoccupied"] += 1
                continue
            if st.startswith("fail"):
                self.stats["c1_fail"] += 1
            keys[occ["occupant_key"]], c1[occ["occupant_key"]] = k, st
            live.append(occ)
        by_ck: dict[str, list[dict]] = defaultdict(list)
        for occ in live:
            k = keys.get(occ["occupant_key"])
            if k and k.coldkey:
                by_ck[k.coldkey].append(occ)
        backfill_left = [int(s.OWNER_BACKFILL_MAX_CALLS_PER_RUN)]
        note: list[str] = []
        for ck in sorted(by_ck):
            if self.clock() > self.soft:
                note.append("soft_deadline")
                break
            if self.budget_out:
                break
            await self.coldkey_stakes(ck)
            if not self.budget_out:
                await self.activity(ck, by_ck[ck], backfill_left)
        from app.integrity import taostats_c8
        try:
            c8 = await taostats_c8.run(self, by_ck)
        except Exception as exc:                                  # the keyless path never depends on C8
            c8 = {"error": f"{type(exc).__name__}: {exc}"}
            self.c8_status = "fail"
        self.stats["c8_active"] = int(bool(c8.get("active")))
        for occ in live:
            okey = occ["occupant_key"]
            try:
                await self.occupant(occ, keys.get(okey), c1.get(okey, "skip"))
            except RunBudgetSpent:
                self.budget_out = True
            except Exception as exc:
                self.stats["occupant_failed"] += 1
                logger.warning("owner_collect.occupant_failed", data={"occupant_key": okey,
                                                                      "error": f"{type(exc).__name__}: {exc}"})
        await self.radar_misses(live, keys)
        await supersede_duplicates(db, self.touched, self.now)
        if self.budget_out:
            note.append("budget_cap")
        n_fail = sum(1 for occ in live if c1.get(occ["occupant_key"], "").startswith("fail"))
        n_fail += self.stats["c2_fail"]
        meta = {"tmc_requests": self.tmc.n, **dict(self.stats), "rufus": self.rufus_status, "radar": self.radar_status,
                "radar_rows": len(self.radar), "snapshots": self.snapshots, "occupants": len(live),
                "list_snapshot_at": to_iso(list_at) if list_at else None}
        await set_state(db, "last_run", meta | {"at": self.now})
        if live and n_fail >= 0.25 * len(live):
            raise RuntimeError(f"owner_collect partial: {n_fail}/{len(live)} occupants with C1/C2 failures "
                               f"(rows kept; tmc={self.tmc.n})")
        return JobOutcome(rows=self.snapshots + self.events,
                          note=f"tmc={self.tmc.n} truncated={self.stats['c4_truncated']} snapshots={self.snapshots} "
                               f"events={self.events}" + (" " + " ".join(note) if note else ""), meta=meta)


class OwnerCollectJob(CollectorJob):
    _last: dict | None = None

    async def collect(self, run: RunContext) -> JobOutcome:
        c = Collector(run.rt.db, run.rt.http, now=run.started_at, run_id=run.run_id,
                      timeout_s=int(self.effective_timeout() or 10800))
        out = await c.run()
        self._last = out.meta
        return out

    def health_extra(self) -> dict | None:
        return self._last
