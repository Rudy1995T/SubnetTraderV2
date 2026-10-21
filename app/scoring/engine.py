"""Scoring engine (spec 12 §2.7–§2.13): pillars → Health; P6 + blend → Velocity; regime-scaled composite; red-flag
gates; EMA; tiers; explain; events; immutable ``scores`` rows; catch-up; shadow specs; kill switch; rescore/backfill.

``ScoreCalculator`` is pure. ``ScoringService`` does the I/O: one ``BEGIN IMMEDIATE … COMMIT`` per (slot, spec)
writes the ``stage='norm'`` rows, ``scores``, ``red_flag_state``, ``regime_daily``, events and the ``score_runs``
finish; everything that drives EMA/tiers/catch-up is read back from the tables (restart invariance, §7 item 5).
"""
from __future__ import annotations

import json
import math
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Sequence

import app.config as app_config
from app import control
from app.features.engine import FeatureEngine, Snapshot
from app.features.normalise import _clip01
from app.features.registry import scored
from app.features.store import prune_norm_provisional, put_metric_values
from app.features.types import NormValue, Occupant
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.quant.regime import RegimeResult, regime_for_slot
from app.scoring import events as EV
from app.scoring.explain import HEALTH_PILLARS, build_explain, impacts, r6, top_lists
from app.scoring.red_flags import (FLAG_EVENT_TYPES, FLAG_RAW_KEYS, FLAG_SERIES, FlagInputs, FlagState, multiplier,
                                   step)
from app.scoring.spec import SPECS, ScoringSpec, get_spec, spec_hash
from app.scoring.stability import rank_autocorr_health
from app.scoring.tiers import assign_tier, ema
from app.util.time import now_ts, parse_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.runtime import Runtime
    from app.storage.db import Database

DAY = 86400
UTC = timezone.utc
CATCHUP_WALL_S = 900                       # stop starting catch-up slots after 900 s (§5)
OCCUPANT_ERROR_ABORT = 0.10                # > 10 % of the universe failing aborts the slot (§4 case 19)


class ScoringRefused(Exception):
    """A run refused before scoring (``spec_unknown``, ``clock_before_slot``, ``killed``, …); ``code`` is the reason."""

    def __init__(self, code: str, msg: str = "") -> None:
        super().__init__(msg or code)
        self.code = code


def _s():
    return app_config.settings


def _j(x: Any) -> str:
    return json.dumps(x, sort_keys=True, separators=(",", ":"), default=str)


# ── pure kernel ──────────────────────────────────────────────────────────────────────────────────────────────
def pillar_score(items: Iterable[tuple[float, float | None]], min_coverage: float) -> tuple[float | None, float]:
    """Coverage-renormalised weighted mean in [0,1]; None != 0 (a None item is dropped, a 0.0 item counts).
    Port of the old ``quality.py:207-233`` ``quality_score`` generalised to registry items (``math.fsum``, 1e-9)."""
    items = list(items)
    total_w = math.fsum(w for w, _ in items)
    avail_w = math.fsum(w for w, m in items if m is not None)
    coverage = avail_w / total_w if total_w > 0 else 0.0
    if coverage + 1e-9 < min_coverage or avail_w <= 0:
        return None, coverage
    return _clip01(math.fsum(w * m for w, m in items if m is not None) / avail_w), coverage


@dataclass
class Calc:
    status: str
    pillars: dict[str, tuple[float | None, float | None, float]]    # P → (score, coverage, available weight)
    health_set: tuple[str, ...]
    health: float | None
    confidence: float
    insufficient: bool
    blend: float | None
    blend_parts: dict[str, float | None]
    velocity: float | None
    composite_raw: float | None = None


class ScoreCalculator:
    """Pure §2.7 arithmetic for one occupant's snapshot row."""

    def __init__(self, spec: ScoringSpec) -> None:
        self.spec = spec

    def calc(self, occ: Occupant, snap_o: Mapping[str, NormValue], v_w: float) -> Calc:
        sp = self.spec
        active = occ.status == "active"
        pillars: dict[str, tuple[float | None, float | None, float]] = {}
        for p in ("P1", "P2", "P3", "P4", "P5", "P6"):
            if p == "P1" and not active:
                pillars[p] = (None, None, 0.0)
                continue
            items = [(d.weight, snap_o[d.key].m) for d in scored(p) if d.key in sp.metrics]
            sc, cov = pillar_score(items, sp.pillar_min_coverage)
            pillars[p] = (sc, cov, math.fsum(w for w, m in items if m is not None))
        K = HEALTH_PILLARS if active else HEALTH_PILLARS[1:]
        H = tuple(k for k in K if pillars[k][0] is not None)
        W = sp.pillar_weights
        health = (100.0 * math.fsum(W[k] * pillars[k][0] for k in H) / math.fsum(W[k] for k in H)) if H else None
        confidence = math.fsum(W[k] * (pillars[k][1] or 0.0) for k in K) / math.fsum(W[k] for k in K)
        parts = {k: snap_o[k].m for k in sp.blend_keys}
        avail = [m for m in parts.values() if m is not None]
        blend = (math.fsum(avail) / len(avail)) if avail else None
        p6 = pillars["P6"][0]
        a, b = sp.velocity_weights
        if p6 is not None and blend is not None:
            vel = 100.0 * (a * p6 + b * blend)
        elif p6 is not None:
            vel = 100.0 * p6
        elif blend is not None:
            vel = 100.0 * blend
        else:
            vel = None
        c = Calc(occ.status, pillars, H, health, confidence, active and confidence < sp.insufficient_below, blend,
                 parts, vel)
        if active and health is not None:
            c.composite_raw = health if vel is None else health * (1.0 - v_w) + vel * v_w
        if not active:
            c.health, c.velocity, c.health_set = None, None, ()
        return c


# ── slots ────────────────────────────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Slot:
    as_of: int
    run_kind: str
    mode: str
    cutoff: int
    trigger: str
    backfilled: int


def cron_hours(s: Any = None) -> list[int]:
    s = s or _s()
    return sorted({int(h) for h in str(s.SCORING_CRON_HOURS).split(",") if str(h).strip() != ""})


def slot_for(now: int, hours: Sequence[int]) -> int:
    """Latest ``hh:00Z ≤ now`` with ``hh ∈ hours``."""
    day = now - now % DAY
    for d in (day, day - DAY):
        cands = [d + h * 3600 for h in hours if d + h * 3600 <= now]
        if cands:
            return max(cands)
    raise ValueError("no scoring hour configured")


def run_kind_of(as_of: int) -> str:
    return "official" if as_of % DAY == 0 else "provisional"         # SCORING_OFFICIAL_HOUR pinned to 0 (02 §B6)


def make_slot(as_of: int, t0: int, *, trigger: str, reconstructed: bool | None = None, s: Any = None) -> Slot:
    s = s or _s()
    nominal = as_of + int(s.SCORING_CRON_MINUTE) * 60
    if reconstructed is None:
        reconstructed = t0 - nominal > int(s.SCORING_LATE_GRACE_MIN) * 60
    mode = "reconstructed" if reconstructed else "strict"
    return Slot(int(as_of), run_kind_of(as_of), mode, nominal if reconstructed else int(t0), trigger,
                1 if reconstructed else 0)


def plan_slots(last_done: int | None, now: int, cfg: Any = None) -> list[Slot]:
    """Missed official slots strictly between ``last_done`` and the current slot (most recent
    ``SCORING_CATCHUP_MAX_SLOTS``, oldest first, reconstructed), then the current slot."""
    s = cfg or _s()
    cur = slot_for(now, cron_hours(s))
    out: list[Slot] = []
    if last_done is not None and s.SCORING_CATCHUP_ENABLED and int(s.SCORING_CATCHUP_MAX_SLOTS) > 0:
        d = last_done - last_done % DAY + DAY
        missed = []
        while d < cur:
            if d > last_done:
                missed.append(d)
            d += DAY
        for m in missed[-int(s.SCORING_CATCHUP_MAX_SLOTS):]:
            out.append(make_slot(m, now, trigger="catchup", reconstructed=True, s=s))
    out.append(make_slot(cur, now, trigger="schedule", s=s))
    return out


def mint_run_id(trigger: str, as_of: int) -> str:
    prefix = {"schedule": "sc", "catchup": "cu", "backfill": "bf", "cli": "cli", "rescore": "rs"}[trigger]
    if trigger == "rescore":
        return "rs-" + datetime.now(UTC).strftime("%Y%m%d-%H%M%S") + f"-{uuid.uuid4().hex[:4]}"
    return f"{prefix}-{datetime.fromtimestamp(as_of, UTC).strftime('%Y%m%dT%H')}-{uuid.uuid4().hex[:8]}"


def shadow_specs(s: Any = None) -> list[str]:
    s = s or _s()
    out = []
    for v in (x.strip() for x in str(s.SCORING_SHADOW_SPECS or "").split(",")):
        if not v:
            continue
        if v not in SPECS or v == s.SPEC_VERSION:
            logger.warning("SCORING_SHADOW_SPECS entry dropped", data={"spec": v})
            continue
        if v not in out:
            out.append(v)
    return out


# ── service ──────────────────────────────────────────────────────────────────────────────────────────────────
@dataclass
class SpecRun:
    spec: ScoringSpec
    run_id: str
    shadow: bool
    started_at: int
    note: str | None = None
    result: dict = field(default_factory=dict)


class ScoringService:
    def __init__(self, db: "Database", *, alerts: Any = None, clock: Any = now_ts) -> None:
        self.db = db
        self.alerts = alerts
        self.clock = clock
        self._regime_cache: dict[tuple, RegimeResult] = {}

    # ── table state ───────────────────────────────────────────────────────────────────────────────────────
    async def last_done(self, spec_version: str) -> int | None:
        v = await self.db.fetchval("SELECT MAX(as_of) FROM score_runs WHERE spec_version = ? AND shadow = 0 "
                                   "AND (ok = 1 OR killed = 1)", (spec_version,), writer=True)
        return None if v is None else int(v)

    async def already_scored(self, spec_version: str, as_of: int) -> bool:
        if await self.db.fetchval("SELECT 1 FROM scores WHERE spec_version = ? AND as_of = ? LIMIT 1",
                                  (spec_version, int(as_of)), writer=True):
            return True
        return bool(await self.db.fetchval("SELECT 1 FROM score_runs WHERE spec_version = ? AND as_of = ? AND ok = 1 "
                                           "AND killed = 0 LIMIT 1", (spec_version, int(as_of)), writer=True))

    async def latest_completed(self, spec_version: str) -> int | None:
        v = await self.db.fetchval("SELECT MAX(as_of) FROM scores WHERE spec_version = ?", (spec_version,),
                                   writer=True)
        return None if v is None else int(v)

    # ── scheduled run ─────────────────────────────────────────────────────────────────────────────────────
    async def run_scheduled(self, t0: int | None = None, *, trigger: str = "schedule") -> dict:
        s = _s()
        if s.SPEC_VERSION not in SPECS:
            raise ScoringRefused("spec_unknown", f"spec_unknown: SPEC_VERSION {s.SPEC_VERSION!r} not in SPECS")
        t0 = int(self.clock() if t0 is None else t0)
        plan = plan_slots(await self.last_done(s.SPEC_VERSION), t0, s)
        wall = time.monotonic()
        out = {"slots": [], "rows": 0, "notes": []}
        for sl in plan:
            if sl.trigger == "catchup" and time.monotonic() - wall > CATCHUP_WALL_S:
                out["notes"].append(f"catchup_deferred:{to_iso(sl.as_of)}")
                continue
            if trigger != "schedule" and sl.trigger == "schedule":
                sl = Slot(sl.as_of, sl.run_kind, sl.mode, sl.cutoff, trigger, sl.backfilled)
            res = await self.run_slot(sl, t0=int(self.clock()) if sl.trigger != "schedule" else t0)
            out["slots"].append(res)
            out["rows"] += res.get("n_scored", 0)
        return out

    # ── one slot ──────────────────────────────────────────────────────────────────────────────────────────
    async def run_slot(self, slot: Slot, *, t0: int | None = None, specs: Sequence[str] | None = None,
                       dry_run: bool = False, flag_memory: dict | None = None, emit_events: bool = True,
                       compute_fns: bool = True) -> dict:
        """Score ``slot`` for the primary spec and each shadow spec (or ``specs``). ``flag_memory`` (backfill) keeps
        flag state in memory instead of ``red_flag_state``."""
        s = _s()
        t0 = int(self.clock() if t0 is None else t0)
        if t0 < slot.as_of:
            raise ScoringRefused("clock_before_slot", f"T0 {t0} < slot {slot.as_of}")
        versions = list(specs) if specs is not None else [s.SPEC_VERSION, *shadow_specs(s)]
        for v in versions:
            get_spec(v)
        primary = s.SPEC_VERSION
        killed = (not dry_run) and control.scoring_killed()
        runs: list[SpecRun] = []
        skipped = []
        for v in versions:
            if not dry_run and await self.already_scored(v, slot.as_of):
                skipped.append(v)
                continue
            runs.append(SpecRun(get_spec(v), mint_run_id(slot.trigger, slot.as_of), v != primary, t0))
        summary = {"as_of": slot.as_of, "slot": to_iso(slot.as_of), "run_kind": slot.run_kind, "mode": slot.mode,
                   "trigger": slot.trigger, "killed": killed, "skipped": skipped, "n_scored": 0, "specs": {}}
        if not runs:
            summary["note"] = "already_scored"
            return summary
        if not dry_run:
            for r in runs:
                await self.db.execute(
                    "INSERT INTO score_runs (run_id, spec_version, as_of, run_kind, mode, trigger, shadow, killed, "
                    "backfilled, started_at, spec_hash) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (r.run_id, r.spec.version, slot.as_of, slot.run_kind, slot.mode, slot.trigger, int(r.shadow),
                     int(killed), slot.backfilled, t0, spec_hash(r.spec)))
        eng = FeatureEngine(self.db)
        tm: dict[str, float] = {}
        t = time.monotonic()
        occ = await eng.universe(slot.as_of)
        fn_rows = 0
        if compute_fns and not dry_run:
            try:
                fn_rows = await eng.compute_registered(occ, slot.cutoff, slot.as_of, runs[0].run_id, mode=slot.mode,
                                                       now=t0)
                fn_rows += await eng.derive(occ, slot.as_of, slot.cutoff, slot.as_of, runs[0].run_id,
                                            mode=slot.mode, now=t0)
            except Exception as exc:                       # observations are best-effort; scoring continues
                logger.error("compute_registered/derive failed", data={"slot": slot.as_of,
                                                                       "error": f"{type(exc).__name__}: {exc}"})
        tm["compute_registered_s"] = round(time.monotonic() - t, 3)
        summary["fn_rows"] = fn_rows
        for r in runs:
            try:
                res = await self._score_spec(r, slot, occ, eng, t0=t0, killed=killed, dry_run=dry_run,
                                             emit_events=emit_events and not r.shadow and r.spec.version == primary
                                             and bool(s.SCORING_EVENTS_ENABLED),
                                             flag_memory=flag_memory, timings=dict(tm))
            except Exception as exc:
                err = f"{type(exc).__name__}: {exc}"[:500]
                logger.error("scoring slot failed", data={"slot": slot.as_of, "spec": r.spec.version, "error": err})
                if not dry_run:
                    await self.db.execute("UPDATE score_runs SET finished_at = ?, ok = 0, error = ? WHERE run_id = ? "
                                          "AND spec_version = ?", (int(self.clock()), err, r.run_id, r.spec.version))
                summary["specs"][r.spec.version] = {"ok": False, "error": err, "run_id": r.run_id}
                if r.spec.version == primary:
                    summary["error"] = err
                continue
            summary["specs"][r.spec.version] = res
            if r.spec.version == primary:
                summary["n_scored"] = res["n_scored"]
                summary["run_id"] = r.run_id
        return summary

    async def _regime(self, spec: ScoringSpec, slot: Slot) -> RegimeResult:
        key = (slot.as_of, slot.cutoff, slot.mode, spec.regime, tuple(sorted(spec.v_w_by_regime.items())),
               spec.version if slot.run_kind == "official" else None)
        if key not in self._regime_cache:
            self._regime_cache[key] = await regime_for_slot(
                self.db, slot.as_of, slot.cutoff, slot.mode, spec.regime, spec.v_w_by_regime,
                spec_version=spec.version, official=slot.run_kind == "official")
        return self._regime_cache[key]

    async def _prev_rows(self, spec_version: str, as_of: int, ema_gap_days: int) -> tuple[dict, dict]:
        """(EMA/tier ``prev`` per occupant: latest official within the gap with a smoothed value;
        latest official row of any kind before the slot)."""
        rows = await self.db.fetchall(
            "SELECT * FROM scores WHERE spec_version = ? AND run_kind = 'official' AND as_of < ? AND as_of >= ? "
            "AND composite_smoothed IS NOT NULL ORDER BY occupant_key, as_of DESC",
            (spec_version, int(as_of), int(as_of) - ema_gap_days * DAY), writer=True)
        prev: dict[str, dict] = {}
        for r in rows:
            prev.setdefault(r["occupant_key"], r)
        rows = await self.db.fetchall(
            "SELECT s.* FROM scores s JOIN (SELECT occupant_key, MAX(as_of) AS m FROM scores WHERE spec_version = ? "
            "AND run_kind = 'official' AND as_of < ? GROUP BY occupant_key) l ON l.occupant_key = s.occupant_key "
            "AND l.m = s.as_of WHERE s.spec_version = ?", (spec_version, int(as_of), spec_version), writer=True)
        return prev, {r["occupant_key"]: r for r in rows}

    async def _flag_inputs(self, eng: FeatureEngine, spec: ScoringSpec, slot: Slot, occ: Sequence[Occupant]):
        a0 = slot.as_of - slot.as_of % DAY
        series = {}
        for key, days in FLAG_SERIES.items():
            series[key] = await eng.select_series(key, [a0 - k * DAY for k in range(days)], slot.cutoff, slot.mode,
                                                  spec.raw_spec_version)
        recon = 1 if slot.mode == "reconstructed" else 0
        admit = "(observed_at <= ? OR (? = 1 AND json_extract(payload, '$.backfilled') = 1))"
        ev_rows = await self.db.fetchall(
            f"SELECT id, occupant_key, event_type, event_at, observed_at, payload FROM events "
            f"WHERE event_type IN ('identity_changed','identity_junk') AND occupant_key IS NOT NULL AND {admit} "
            f"AND COALESCE(event_at, observed_at) > ? AND COALESCE(event_at, observed_at) <= ?",
            (slot.cutoff, recon, a0 - 90 * DAY, slot.as_of))
        ev_rows += await self.db.fetchall(
            f"SELECT e.id, e.occupant_key, e.event_type, e.event_at, e.observed_at, e.payload FROM events e "
            f"JOIN (SELECT occupant_key, MAX(id) AS mid FROM events WHERE event_type IN ('site_down_7d','site_recovered') "
            f"AND occupant_key IS NOT NULL AND {admit} AND COALESCE(event_at, observed_at) <= ? GROUP BY occupant_key) l "
            f"ON l.mid = e.id", (slot.cutoff, recon, slot.as_of))
        events: dict[str, list[dict]] = {}
        for e in ev_rows:
            e = dict(e)
            e["payload"] = json.loads(e["payload"] or "{}")
            if e["event_type"] in FLAG_EVENT_TYPES:
                events.setdefault(e["occupant_key"], []).append(e)
        man_rows = await self.db.fetchall(
            "SELECT m.* FROM manual_flags m JOIN (SELECT occupant_key, flag, MAX(set_at) AS ms FROM manual_flags "
            "WHERE set_at <= ? GROUP BY occupant_key, flag) l ON l.occupant_key = m.occupant_key AND l.flag = m.flag "
            "AND l.ms = m.set_at ORDER BY m.id", (slot.cutoff,))
        manual: dict[str, dict[str, dict]] = {}
        for m in man_rows:
            manual.setdefault(m["occupant_key"], {})[m["flag"]] = dict(m)
        return series, events, manual

    async def _score_spec(self, r: SpecRun, slot: Slot, occ: Sequence[Occupant], eng: FeatureEngine, *, t0: int,
                          killed: bool, dry_run: bool, emit_events: bool, flag_memory: dict | None,
                          timings: dict) -> dict:
        s = _s()
        spec = r.spec
        t = time.monotonic()
        raws = await eng.select_raw(eng.snapshot_keys(), slot.as_of, slot.cutoff, slot.mode, spec.raw_spec_version)
        snap: Snapshot = await eng.snapshot(spec, occ, slot.as_of, slot.cutoff, slot.mode, raws=raws)
        timings["snapshot_s"] = round(time.monotonic() - t, 3)
        t = time.monotonic()
        regime = await self._regime(spec, slot)
        v_w = regime.v_w
        prev_ema, prev_any = await self._prev_rows(spec.version, slot.as_of, spec.ema_max_gap_days)
        series, events_in, manual = await self._flag_inputs(eng, spec, slot, occ)
        if flag_memory is not None:
            states = flag_memory.setdefault(spec.version, {})
        else:
            states = {(x["occupant_key"], x["flag"]): FlagState.from_row(x) for x in await self.db.fetchall(
                "SELECT * FROM red_flag_state WHERE spec_version = ?", (spec.version,), writer=True)}
        calc_ = ScoreCalculator(spec)
        h = spec_hash(spec)
        score_rows, state_rows, ev_out = [], [], []
        n_pre = n_ins = errors = 0
        official = slot.run_kind == "official"
        for o in occ:
            try:
                row, st_changes, evs = self._score_occupant(
                    spec, h, slot, o, snap[o.occupant_key], regime, v_w, prev_ema.get(o.occupant_key),
                    prev_any.get(o.occupant_key), raws, series, events_in.get(o.occupant_key, []),
                    manual.get(o.occupant_key, {}), states, t0, r.run_id, r.shadow, calc_, official)
            except Exception as exc:
                errors += 1
                logger.error("scoring occupant failed", data={"occupant_key": o.occupant_key, "slot": slot.as_of,
                                                              "error": f"{type(exc).__name__}: {exc}"})
                continue
            score_rows.append(row)
            state_rows.extend(st_changes)
            if emit_events:
                ev_out.extend(evs)
            n_pre += row["status"] == "pre_launch"
            n_ins += row["insufficient_data"]
        if occ and errors / len(occ) > OCCUPANT_ERROR_ABORT:
            raise RuntimeError(f"{errors} of {len(occ)} occupants failed (> 10 %)")
        timings["score_s"] = round(time.monotonic() - t, 3)
        timings["fn_errors"] = eng.stats["fn_errors"]
        timings["occupant_errors"] = errors
        n_errors = eng.stats["fn_errors"] + errors
        res = {"ok": True, "run_id": r.run_id, "regime": regime.label, "v_w": v_w, "n_universe": len(occ),
               "n_scored": len(score_rows), "n_prelaunch": n_pre, "n_insufficient": n_ins, "n_events": 0,
               "killed": killed, "rows": score_rows if dry_run else None}
        if flag_memory is not None:
            for st in state_rows:
                states[(st.occupant_key, st.flag)] = st
        if dry_run:
            return res
        t = time.monotonic()
        n_ev = 0
        async with self.db.transaction():
            if not killed:
                await put_metric_values(self.db, eng.norm_rows(spec, snap, slot.as_of, slot.run_kind, t0, r.run_id,
                                                               slot.backfilled))
                for row in score_rows:
                    cols = list(row)
                    await self.db.execute(f"INSERT INTO scores ({', '.join(cols)}) VALUES ({','.join('?' * len(cols))})",
                                          [row[c] for c in cols])
                if flag_memory is None and state_rows:
                    await self.db.upsert("red_flag_state", [st.as_row() for st in state_rows],
                                         conflict=("occupant_key", "flag", "spec_version"))
                if official and not r.shadow:
                    m = regime.metrics
                    await self.db.execute(
                        "INSERT OR IGNORE INTO regime_daily (day_ts, spec_version, label, raw_regime, vol_24h, "
                        "directional_strength, dispersion, n_subnets, thin_universe, v_w, tao_usd_ret_24h, "
                        "sum_prices_ret_24h, inputs_json, source, computed_at, backfilled, run_id) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'live',?,?,?)",
                        (slot.as_of, spec.version, regime.label, regime.raw_regime, m.get("vol_24h"),
                         m.get("directional_strength"), m.get("dispersion"), m.get("n_subnets"),
                         int(bool(m.get("thin_universe"))), v_w, m.get("tao_usd_ret_24h"),
                         m.get("sum_prices_ret_24h"), _j(m), t0, slot.backfilled, r.run_id))
                for e in ev_out:
                    if await EV.emit(self.db, occupant_key=e["occupant_key"], netuid=e["netuid"],
                                     event_type=e["event_type"], severity=e["severity"], event_at=slot.as_of,
                                     observed_at=t0, payload=e["payload"], dedupe_key=e["dedupe_key"],
                                     run_id=r.run_id):
                        n_ev += 1
            timings["write_s"] = round(time.monotonic() - t, 3)
            await self.db.execute(
                "UPDATE score_runs SET finished_at = ?, ok = 1, regime = ?, n_universe = ?, n_scored = ?, "
                "n_prelaunch = ?, n_insufficient = ?, n_events = ?, n_errors = ?, timings_json = ? "
                "WHERE run_id = ? AND spec_version = ?",
                (int(self.clock()), regime.label, len(occ), 0 if killed else len(score_rows), n_pre, n_ins, n_ev,
                 n_errors, _j(timings), r.run_id, spec.version))
        res["n_events"] = n_ev
        if killed:
            res["n_scored"] = 0
            return res
        if official and not r.shadow and spec.version == s.SPEC_VERSION:
            await self._after_official(spec, slot, t0, emit=emit_events)
        return res

    def _score_occupant(self, spec: ScoringSpec, h: str, slot: Slot, o: Occupant, snap_o: Mapping[str, NormValue],
                        regime: RegimeResult, v_w: float, prev: Mapping | None, prev_any: Mapping | None,
                        raws: Mapping, series: Mapping, events: list[dict], manual: Mapping[str, dict],
                        states: dict, t0: int, run_id: str, shadow: bool, calc_: ScoreCalculator,
                        official: bool) -> tuple[dict, list[FlagState], list[dict]]:
        okey = o.occupant_key
        calc = calc_.calc(o, snap_o, v_w)
        # red flags (every run kind, every status)
        fi = FlagInputs(raw={k: (raws.get((okey, k))) for k in FLAG_RAW_KEYS},
                        series={k: series[k].get(okey, {}) for k in series}, events=events, manual=manual,
                        occupant=o, slot=slot.as_of, cutoff=slot.cutoff)
        changes: list[FlagState] = []
        transitions: list[tuple[FlagState, str]] = []
        active_states: list[FlagState] = []
        for fd in spec.flags.values():
            trig, detail = fd.evaluate(fi)
            new, tr = step(states.get((okey, fd.name)), fd, trig, detail, manual.get(fd.name), slot.as_of,
                           spec_version=spec.version, occupant_key=okey, computed_at=t0,
                           suppress_days=spec.manual_clear_suppress_days)
            if new is not None:
                changes.append(new)
                if new.active:
                    active_states.append(new)
            if tr:
                transitions.append((new, tr))
        mult = multiplier((spec.flags[x.flag] for x in active_states), spec.flag_floor)
        names = sorted(x.flag for x in active_states)
        active = o.status == "active"
        composite = None if calc.composite_raw is None else calc.composite_raw * mult
        smoothed = ema(composite, prev["composite_smoothed"] if prev else None, spec.ema_span) if active else None
        tier = (assign_tier(smoothed, prev["tier"] if prev else None, spec.tier_hysteresis, spec.tier_lower)
                if active else None)
        imp = impacts(spec, snap_o, calc.pillars, calc.health_set)
        flags_ex = [{"flag": x.flag, "multiplier": spec.flags[x.flag].multiplier, "since": x.raised_at,
                     "state": x.state(), "origin": x.origin(), "human": bool(x.human), "sticky": bool(x.sticky),
                     "clear_streak_days": x.clear_streak_days(slot.as_of), "detail": x.detail_json}
                    for x in sorted(active_states, key=lambda x: x.flag)]
        explain = build_explain(spec=spec, spec_hash=h, as_of=slot.as_of, run_kind=slot.run_kind, mode=slot.mode,
                                status=o.status, regime=regime, v_w=v_w, calc=calc, snap_o=snap_o, flags=flags_ex,
                                prev=prev, tier=tier, composite=composite, composite_smoothed=smoothed,
                                flag_multiplier=mult, imp=imp)
        row = {"occupant_key": okey, "netuid": o.netuid, "as_of": slot.as_of, "spec_version": spec.version,
               "run_kind": slot.run_kind, "run_id": run_id, "status": o.status, "quality": calc.health,
               "velocity": calc.velocity, "composite_raw": calc.composite_raw, "flag_multiplier": mult,
               "composite": composite, "composite_smoothed": smoothed,
               "pillar_scores": _j({p: calc.pillars[p][0] for p in calc.pillars}),
               "pillar_coverage": _j({p: calc.pillars[p][1] for p in calc.pillars}),
               "confidence": calc.confidence, "insufficient_data": int(calc.insufficient), "tier": tier,
               "prev_tier": prev["tier"] if prev else None, "red_flags": _j(names), "regime": regime.label,
               "v_w": v_w, "explain": _j(explain), "computed_at": t0, "input_cutoff": slot.cutoff,
               "backfilled": slot.backfilled, "shadow": int(shadow)}
        evs: list[dict] = []
        bf = bool(slot.backfilled)
        for st, tr in transitions:
            fd = spec.flags[st.flag]
            if tr == "raised":
                e = EV.flag_raised_event(occ=okey, netuid=o.netuid, spec=spec.version, as_of=slot.as_of, flag=st.flag,
                                         multiplier=fd.multiplier, human=bool(st.human), sticky=bool(st.sticky),
                                         detail=st.detail_json, total=mult, raised_at=st.raised_at,
                                         origin=st.origin(), backfilled=bf)
            else:
                e = EV.flag_cleared_event(occ=okey, netuid=o.netuid, spec=spec.version, as_of=slot.as_of, flag=st.flag,
                                          multiplier=fd.multiplier, raised_at=st.raised_at,
                                          reason="manual" if tr == "cleared_manual" else "hysteresis",
                                          cleared_at=st.cleared_at, clear_streak_days=st.clear_streak_days(slot.as_of),
                                          origin="human" if tr == "cleared_manual" else "auto", backfilled=bf)
            evs.append({**e, "occupant_key": okey, "netuid": o.netuid})
        if official and active:
            top_pos, top_neg = top_lists(spec, imp)
            if (prev is not None and prev["tier"] and tier and tier != prev["tier"] and not calc.insufficient
                    and not prev["insufficient_data"]):
                evs.append({**EV.tier_change_event(occ=okey, netuid=o.netuid, spec=spec.version, as_of=slot.as_of,
                                                   prev_tier=prev["tier"], tier=tier, smoothed=r6(smoothed),
                                                   prev_smoothed=r6(prev["composite_smoothed"]), composite=r6(composite),
                                                   lower=spec.tier_lower, top_positive=top_pos,
                                                   top_negative=top_neg, red_flags=names, backfilled=bf),
                            "occupant_key": okey, "netuid": o.netuid})
            if (prev_any is not None and prev_any["composite"] is not None and composite is not None
                    and abs(composite - prev_any["composite"]) >= float(_s().SCORE_JUMP_ALERT_POINTS)):
                pp = json.loads(prev_any["pillar_scores"] or "{}")
                deltas = {p: (None if calc.pillars[p][0] is None or pp.get(p) is None
                              else r6(calc.pillars[p][0] - pp[p])) for p in calc.pillars}
                old = set(json.loads(prev_any["red_flags"] or "[]"))
                evs.append({**EV.score_jump_event(occ=okey, netuid=o.netuid, spec=spec.version, as_of=slot.as_of,
                                                  prev=prev_any, composite=composite, pillar_deltas=deltas,
                                                  flags_added=sorted(set(names) - old),
                                                  flags_removed=sorted(old - set(names)),
                                                  coverage_delta=r6(calc.confidence - prev_any["confidence"]),
                                                  backfilled=bf), "occupant_key": okey, "netuid": o.netuid})
            if calc.insufficient and (prev_any is None or not prev_any["insufficient_data"]):
                evs.append({**EV.insufficient_event(
                    occ=okey, netuid=o.netuid, spec=spec.version, as_of=slot.as_of, confidence=r6(calc.confidence),
                    coverage={p: r6(calc.pillars[p][1]) for p in HEALTH_PILLARS},
                    missing=explain["missing"], backfilled=bf), "occupant_key": okey, "netuid": o.netuid})
        return row, changes, evs

    async def _after_official(self, spec: ScoringSpec, slot: Slot, t0: int, *, emit: bool) -> None:
        s = _s()
        if s.RANK_STABILITY_ENABLED:
            rows = await rank_autocorr_health(self.db, spec.version, slot.as_of)
            async with self.db.transaction():
                for r in rows:
                    await self.db.execute(
                        "INSERT OR IGNORE INTO rank_stability_daily (as_of, spec_version, series, lag_days, n, "
                        "rank_autocorr, quintile_turnover, top20_turnover, computed_at) VALUES (?,?,?,?,?,?,?,?,?)",
                        (r["as_of"], r["spec_version"], r["series"], r["lag_days"], r["n"], r["rank_autocorr"],
                         r["quintile_turnover"], r["top20_turnover"], t0))
                    thr = float(s.RANK_AUTOCORR_ALERT_BELOW)
                    if (emit and r["series"] == "health" and r["lag_days"] == 1 and r["rank_autocorr"] is not None
                            and r["rank_autocorr"] < thr):
                        e = EV.instability_event(spec=spec.version, as_of=slot.as_of, row=r, threshold=thr)
                        await EV.emit(self.db, occupant_key=None, netuid=None, event_type=e["event_type"],
                                      severity=e["severity"], event_at=slot.as_of, observed_at=t0,
                                      payload=e["payload"], dedupe_key=e["dedupe_key"], run_id=None)
        cutoff = slot.as_of - int(s.METRIC_NORM_PROVISIONAL_RETENTION_DAYS) * DAY
        n = await prune_norm_provisional(self.db, cutoff)
        if n:
            logger.info("pruned provisional norm rows", data={"rows": n, "before": cutoff})

    # ── stall detection (§2.11) ───────────────────────────────────────────────────────────────────────────
    async def stall_state(self, now: int) -> dict:
        s = _s()
        last = await self.db.fetchval("SELECT MAX(as_of) FROM scores WHERE spec_version = ? AND shadow = 0",
                                      (s.SPEC_VERSION,), writer=True)
        out = {"last_ok_as_of": None if last is None else int(last), "stale": False, "slot_as_of": None,
               "missed_slots": 0, "last_error": None}
        if last is None:
            return out
        last = int(last)
        hours = cron_hours(s)
        missed = []
        d = last - last % DAY
        while d <= now:
            for hh in hours:
                t = d + hh * 3600
                if last < t <= now:
                    missed.append(t)
            d += DAY
        out["missed_slots"] = len(missed)
        out["slot_as_of"] = missed[0] if missed else None
        out["stale"] = now - last > int(s.SCORING_STALE_AFTER_MIN) * 60
        out["last_error"] = await self.db.fetchval(
            "SELECT error FROM collector_runs WHERE job = 'scoring_6h' AND ok = 0 ORDER BY started_at DESC LIMIT 1",
            writer=True)
        return out

    # ── operator entry points (§2.13) ─────────────────────────────────────────────────────────────────────
    async def rescore(self, as_of: date | None, spec_version: str, shadow: bool = False) -> str:
        if spec_version not in SPECS:
            raise ScoringRefused("spec_unknown")
        if control.scoring_killed():
            raise ScoringRefused("killed", "SCORING_KILLED present")
        t0 = int(self.clock())
        if as_of is None:
            sl = make_slot(slot_for(t0, cron_hours()), t0, trigger="rescore")
        else:
            d = int(datetime(as_of.year, as_of.month, as_of.day, tzinfo=UTC).timestamp())
            sl = make_slot(d, t0, trigger="rescore", reconstructed=True)
        if await self.already_scored(spec_version, sl.as_of):
            raise ScoringRefused("slot_already_scored")
        latest = await self.latest_completed(spec_version)
        if latest is not None and sl.as_of < latest:
            raise ScoringRefused("slot_regresses_state", "older than the spec's latest completed slot")
        res = await self.run_slot(sl, t0=t0, specs=[spec_version])
        return (res["specs"].get(spec_version) or {}).get("run_id", "")

    async def backfill(self, spec_version: str, start: date, end: date | None = None, *,
                       progress: Any = None) -> dict:
        """Official slots oldest-first, reconstructed, flag state in memory, no fns/derive, no events (§2.13)."""
        get_spec(spec_version)
        first_live = await self.db.fetchval("SELECT MIN(as_of) FROM scores WHERE spec_version = ? AND backfilled = 0",
                                            (spec_version,), writer=True)
        d0 = int(datetime(start.year, start.month, start.day, tzinfo=UTC).timestamp())
        if end is not None:
            d1 = int(datetime(end.year, end.month, end.day, tzinfo=UTC).timestamp())
        else:
            d1 = (int(first_live) - DAY) if first_live is not None else (slot_for(int(self.clock()), [0]) - DAY)
        if first_live is not None:
            d1 = min(d1, int(first_live) - DAY)
        memory: dict = {}
        out = {"slots": 0, "skipped": 0, "rows": 0, "from": to_iso(d0), "to": to_iso(d1)}
        d = d0
        while d <= d1:
            if await self.already_scored(spec_version, d):
                out["skipped"] += 1
            else:
                sl = make_slot(d, int(self.clock()), trigger="backfill", reconstructed=True)
                res = await self.run_slot(sl, specs=[spec_version], flag_memory=memory, emit_events=False,
                                          compute_fns=False)
                if res.get("error"):
                    raise RuntimeError(f"backfill {to_iso(d)}: {res['error']}")
                out["slots"] += 1
                out["rows"] += res["specs"].get(spec_version, {}).get("n_scored", 0)
                if progress:
                    progress(d, res)
            d += DAY
        return out


# ── /health.scores (01 §2.9) ─────────────────────────────────────────────────────────────────────────────────
async def scores_status(rt: "Runtime") -> dict | None:
    db = getattr(rt, "db", None)
    if db is None or not getattr(db, "connected", False):
        return None
    s = _s()
    sv = s.SPEC_VERSION
    latest = await db.fetchval("SELECT MAX(as_of) FROM scores WHERE spec_version = ? AND shadow = 0", (sv,))
    latest_off = await db.fetchval("SELECT MAX(as_of) FROM scores WHERE spec_version = ? AND shadow = 0 "
                                   "AND run_kind = 'official'", (sv,))
    last = await db.fetchone("SELECT ok, killed, as_of, error FROM score_runs WHERE spec_version = ? AND shadow = 0 "
                             "ORDER BY started_at DESC LIMIT 1", (sv,))
    now = now_ts()
    stale = latest is None or now - int(latest) > int(s.SCORING_STALE_AFTER_MIN) * 60
    return {"latest_as_of": to_iso(latest), "latest_official_as_of": to_iso(latest_off), "stale": stale,
            "spec_version": sv, "spec_known": sv in SPECS, "shadow_specs": shadow_specs(s),
            "last_run_ok": None if last is None else (None if last["ok"] is None else bool(last["ok"])),
            "last_run_killed": None if last is None else bool(last["killed"])}


# ── scoring_6h ───────────────────────────────────────────────────────────────────────────────────────────────
_STALL_KEY = "scoring_stalled"


class ScoringJob(CollectorJob):
    _last: dict | None = None

    async def _stall(self, svc: ScoringService, now: int, *, after: bool) -> None:
        alerts = getattr(self.rt, "alerts", None)
        if alerts is None:
            return
        st = await svc.stall_state(now)
        if st["stale"] and not after:
            text = (f"⚠️ scoring stalled: no {_s().SPEC_VERSION} score since {to_iso(st['last_ok_as_of'])} "
                    f"({st['missed_slots']} missed slots)")
            await alerts.fire(_STALL_KEY, text, event_type="scoring_stalled", severity="critical",
                              payload={k: st[k] for k in ("slot_as_of", "last_ok_as_of", "missed_slots", "last_error")}
                              | {"spec_version": _s().SPEC_VERSION},
                              dedupe_key=str(st["slot_as_of"]), source="scoring")
        elif not st["stale"] and after:
            await alerts.clear(_STALL_KEY, f"✅ scoring recovered: latest {to_iso(st['last_ok_as_of'])}")

    async def collect(self, run: RunContext) -> JobOutcome:
        db = run.rt.db
        now = int(run.started_at)
        svc = ScoringService(db, alerts=getattr(run.rt, "alerts", None))
        await self._stall(svc, now, after=False)
        out = await svc.run_scheduled(now)
        await self._stall(svc, int(now_ts()), after=True)
        slots = out["slots"]
        errors = [x for x in slots if x.get("error")]
        self._last = {"slots": [{k: x.get(k) for k in ("slot", "run_kind", "mode", "trigger", "n_scored", "note",
                                                        "killed", "error")} for x in slots], "notes": out["notes"]}
        if errors and all(x.get("error") for x in slots if not x.get("note")):
            raise RuntimeError(f"scoring failed: {errors[-1]['error']}")
        note = ", ".join(f"{x['slot']}:{x.get('note') or ('killed' if x.get('killed') else x.get('n_scored'))}"
                         for x in slots)
        if len(slots) == 1 and slots[0].get("note") == "already_scored":
            note = "already_scored"
        return JobOutcome(rows=out["rows"], note=note, meta=self._last)

    def health_extra(self) -> dict | None:
        return self._last
