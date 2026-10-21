"""Feature engine (spec 12 §2.6): the scoring universe, producer-fn evaluation, derived metrics, PIT raw-row
selection, the normalised snapshot, and the hourly ``features_hourly`` job.

``compute_registered`` and ``derive`` write ``stage='raw'`` rows (first success wins, 02 §B4); ``snapshot`` is the
only producer of normalised values; ``norm_rows`` materialises them as ``stage='norm'`` rows (written by scoring).
"""
from __future__ import annotations

import asyncio
import importlib
import json
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from app.features import registry as REG
from app.features.derived import (CADENCE_LEGS, SILENCE_LEGS, derive_announcement_cadence, derive_silence_days,
                                  x_absent)
from app.features.normalise import PrenormOutOfRange, normalise_value, rankg
from app.features.registry import METRICS, MetricDef
from app.features.store import SELECT_RAW_SQL, put_metric_values, select_series_sql
from app.features.types import MetricResult, NormValue, Occupant, RawRow
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.storage.catalog import METRIC_KEYS
from app.util.time import floor_hour, now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.scoring.spec import ScoringSpec
    from app.storage.db import Database

UTC = timezone.utc
RAW_SV = "ts-1.0"
BLOCK_S = 12
Snapshot = dict[str, dict[str, NormValue]]       # occupant_key → metric_key → NormValue (all 45 scored keys)

_UNAVAILABLE_LOGGED: set[str] = set()            # producer_unavailable is logged once per process


def _s():
    import app.config as app_config
    return app_config.settings


def _raw_row(key: str, r: Mapping[str, Any]) -> RawRow:
    vj = r.get("value_json")
    return RawRow(r["occupant_key"], key, int(r["as_of"]), r["value"], json.loads(vj) if vj else {}, r["status"],
                  int(r["observed_at"]), r["source"], r.get("run_id"), int(r.get("backfilled") or 0))


def result_row(key: str, res: Any, *, occupant_key: str, as_of: int, observed_at: int, run_id: str,
               backfilled: bool, spec_version: str = RAW_SV) -> dict:
    """A producer-fn / derived ``MetricResult`` → a ``stage='raw'`` row (§2.3 status mapping; ``bad_return``)."""
    if not isinstance(res, MetricResult):
        res = MetricResult(None, {"reason": "bad_return"})
    vj = res.mapped_json()
    st = vj["status"]
    value = res.value if st in ("ok", "absent") else None
    return {"occupant_key": occupant_key, "metric_key": key, "as_of": int(as_of), "spec_version": spec_version,
            "stage": "raw", "value": None if value is None else float(value), "value_json": vj,
            "observed_at": int(observed_at), "source": METRIC_KEYS[key].source, "run_id": run_id,
            "backfilled": 1 if backfilled else 0}


def _failed_row(key: str, reason: str, **kw: Any) -> dict:
    return result_row(key, MetricResult(None, {"reason": reason}), **kw)


class FeatureEngine:
    def __init__(self, db: "Database", *, registry: Mapping[str, MetricDef] = METRICS) -> None:
        self.db = db
        self.registry = registry
        self._block_cache: dict[int, int | None] = {}
        self.stats: dict[str, Any] = {"fn_errors": 0, "fn_timeouts": 0, "fn_budget": 0, "bad_return": 0,
                                      "unavailable": []}
        self._warned: set[str] = set()

    # ── universe ──────────────────────────────────────────────────────────────────────────────────────────
    async def _block_ts(self, block: int) -> int | None:
        """17's ``block_times`` interpolated (12 s/block outside its range); None without any row."""
        if block in self._block_cache:
            return self._block_cache[block]
        lo = await self.db.fetchone("SELECT block_number, event_at FROM block_times WHERE block_number <= ? "
                                    "ORDER BY block_number DESC LIMIT 1", (block,))
        hi = await self.db.fetchone("SELECT block_number, event_at FROM block_times WHERE block_number >= ? "
                                    "ORDER BY block_number ASC LIMIT 1", (block,))
        if lo and hi and lo["block_number"] != hi["block_number"]:
            b0, t0, b1, t1 = lo["block_number"], lo["event_at"], hi["block_number"], hi["event_at"]
            ts = int(t0 + (t1 - t0) * (block - b0) / (b1 - b0))
        elif lo or hi:
            a = lo or hi
            ts = int(a["event_at"] + BLOCK_S * (block - a["block_number"]))
        else:
            ts = None
        self._block_cache[block] = ts
        return ts

    async def universe(self, slot: int) -> list[Occupant]:
        """Non-root occupants seen by ``slot`` and not deregistered by it; status from the launch instant (§2.6)."""
        rows = await self.db.fetchall(
            "SELECT occupant_key, netuid, network_registered_at, status, activated_at, first_emission_block "
            "FROM occupants WHERE netuid <> 0 AND first_seen_at <= ? AND (deregistered_at IS NULL OR deregistered_at > ?) "
            "ORDER BY netuid, occupant_key", (int(slot), int(slot)))
        out = []
        for r in rows:
            terms = [int(r["activated_at"])] if r["activated_at"] is not None else []
            if r["first_emission_block"] is not None:
                bt = await self._block_ts(int(r["first_emission_block"]))
                if bt is not None:
                    terms.append(bt)
            launch = min(terms) if terms else None
            if launch is not None:
                st = "active" if launch <= slot else "pre_launch"
            else:
                st = "pre_launch" if r["status"] == "pre_launch" else "active"
            out.append(Occupant(r["occupant_key"], int(r["netuid"]), int(r["network_registered_at"]), st, launch))
        return out

    # ── producer fns ──────────────────────────────────────────────────────────────────────────────────────
    def producer_fns(self) -> dict[str, Any]:
        """metric_key → callable for every fn row (attached or lazily imported) plus ``EVIDENCE_FNS``; modules that do
        not import yet (later phases) are skipped (``producer_unavailable``, logged once per process)."""
        out: dict[str, Any] = {}
        for key, d in self.registry.items():
            if d.producer != "fn" or not d.fn:
                continue
            fn = REG.ATTACHED_FNS.get(key)
            if fn is None:
                mod, name = d.fn.split(":")
                try:
                    m = importlib.import_module(mod)
                except ImportError:
                    if mod not in _UNAVAILABLE_LOGGED:
                        _UNAVAILABLE_LOGGED.add(mod)
                        logger.info("producer_unavailable", data={"module": mod})
                    self.stats["unavailable"].append(key)
                    continue
                fn = REG.ATTACHED_FNS.get(key) or getattr(m, name, None)      # import may have registered it
            if fn is None:
                self.stats["unavailable"].append(key)
                continue
            out[key] = fn
        for key, fn in REG.EVIDENCE_FNS.items():
            out.setdefault(key, fn)
        return out

    async def compute_registered(self, occ: Sequence[Occupant], t_eval: int, key_as_of: int, run_id: str, *,
                                 mode: str, now: int | None = None, budget_s: float | None = None,
                                 timeout_s: float | None = None) -> int:
        """Call every producer fn for every occupant at ``T_eval`` (sequential, read-only fns) and write the raw rows
        (``as_of = key_as_of``; strict: ``observed_at = t_eval``; reconstructed: ``observed_at = now``, backfilled)."""
        s = _s()
        budget = float(s.FEATURE_COMPUTE_BUDGET_SEC if budget_s is None else budget_s)
        tmo = float(s.FEATURE_FN_TIMEOUT_SEC if timeout_s is None else timeout_s)
        recon = mode == "reconstructed"
        obs = int(now if (recon and now is not None) else (now_ts() if recon else t_eval))
        kw = dict(as_of=key_as_of, observed_at=obs, run_id=run_id, backfilled=recon)
        when = datetime.fromtimestamp(int(t_eval), UTC)
        t0 = time.monotonic()
        rows: list[dict] = []
        bad_logged: set[str] = set()
        for key, fn in self.producer_fns().items():
            for o in occ:
                okw = dict(kw, occupant_key=o.occupant_key)
                if time.monotonic() - t0 > budget:
                    self.stats["fn_budget"] += 1
                    rows.append(_failed_row(key, "fn_budget", **okw))
                    continue
                try:
                    res = await asyncio.wait_for(fn(self.db, o.occupant_key, when), tmo)
                except asyncio.TimeoutError:
                    self.stats["fn_timeouts"] += 1
                    self.stats["fn_errors"] += 1
                    rows.append(_failed_row(key, "fn_timeout", **okw))
                    continue
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.stats["fn_errors"] += 1
                    if key not in self._warned:
                        self._warned.add(key)
                        logger.error("producer fn raised", data={"metric_key": key, "occupant_key": o.occupant_key,
                                                                 "error": f"{type(exc).__name__}: {exc}"})
                    rows.append(_failed_row(key, "fn_error", **okw))
                    continue
                if not isinstance(res, MetricResult):
                    self.stats["bad_return"] += 1
                    if key not in bad_logged:
                        bad_logged.add(key)
                        logger.error("producer fn returned a non-MetricResult", data={"metric_key": key,
                                                                                      "type": type(res).__name__})
                rows.append(result_row(key, res, **okw))
        return await put_metric_values(self.db, rows) if rows else 0

    # ── PIT selection ─────────────────────────────────────────────────────────────────────────────────────
    async def select_raw(self, keys: Sequence[str], slot: int, cutoff: int, mode: str, raw_sv: str = RAW_SV,
                         fresh_override_s: int | None = None) -> dict[tuple[str, str], RawRow]:
        """§2.2: per (occupant, key) the latest fresh row whose status is not failed/none; else the latest failed one."""
        out: dict[tuple[str, str], RawRow] = {}
        for k in keys:
            fresh = int(fresh_override_s if fresh_override_s is not None else self.registry[k].freshness_s)
            rows = await self.db.fetchall(SELECT_RAW_SQL, {"k": k, "raw_sv": raw_sv, "slot": int(slot),
                                                           "fresh_s": fresh, "cutoff": int(cutoff),
                                                           "reconstructed": 1 if mode == "reconstructed" else 0})
            for r in rows:
                ok = (r["occupant_key"], k)
                cur = out.get(ok)
                if cur is None:
                    out[ok] = _raw_row(k, r)
                elif not cur.usable and r["status"] not in ("failed", "none"):
                    out[ok] = _raw_row(k, r)
        return out

    async def select_series(self, key: str, as_ofs: Sequence[int], cutoff: int, mode: str,
                            raw_sv: str = RAW_SV) -> dict[str, dict[int, RawRow]]:
        out: dict[str, dict[int, RawRow]] = {}
        if not as_ofs:
            return out
        rows = await self.db.fetchall(select_series_sql(len(as_ofs)),
                                      (key, raw_sv, *[int(a) for a in as_ofs], int(cutoff),
                                       1 if mode == "reconstructed" else 0))
        for r in rows:
            out.setdefault(r["occupant_key"], {})[int(r["as_of"])] = _raw_row(key, r)
        return out

    # ── derived metrics ───────────────────────────────────────────────────────────────────────────────────
    async def _x_channel_occupants(self, cutoff: int) -> set[str]:
        rows = await self.db.fetchall(
            "SELECT DISTINCT occupant_key FROM watch_channels WHERE platform = 'x' AND occupant_key IS NOT NULL "
            "AND first_seen_at <= ? AND (retired_at IS NULL OR retired_at > ?)", (int(cutoff), int(cutoff)))
        return {r["occupant_key"] for r in rows}

    async def derive_results(self, occ: Sequence[Occupant], slot: int, cutoff: int, *, mode: str,
                             raw_sv: str = RAW_SV) -> list[tuple[str, str, MetricResult]]:
        """``(occupant_key, derived_key, MetricResult)`` from the selected C3 legs (§2.6; nothing written)."""
        legs = await self.select_raw((*CADENCE_LEGS, *SILENCE_LEGS, "venue_reachable"), slot, cutoff, mode, raw_sv)
        xs = await self._x_channel_occupants(cutoff)
        out = []
        for o in occ:
            lg = {k: legs.get((o.occupant_key, k)) for k in (*CADENCE_LEGS, *SILENCE_LEGS)}
            out.append((o.occupant_key, "announcement_cadence_30d", derive_announcement_cadence(lg)))
            out.append((o.occupant_key, "silence_days", derive_silence_days(
                lg, legs.get((o.occupant_key, "venue_reachable")), x_absent(lg, o.occupant_key in xs))))
        return out

    async def derive(self, occ: Sequence[Occupant], slot: int, cutoff: int, key_as_of: int, run_id: str, *,
                     mode: str, now: int | None = None, raw_sv: str = RAW_SV) -> int:
        recon = mode == "reconstructed"
        obs = int(now if (recon and now is not None) else (now_ts() if recon else cutoff))
        rows = [result_row(key, res, occupant_key=okey, as_of=key_as_of, observed_at=obs, run_id=run_id,
                           backfilled=recon, spec_version=raw_sv)
                for okey, key, res in await self.derive_results(occ, slot, cutoff, mode=mode, raw_sv=raw_sv)]
        return await put_metric_values(self.db, rows) if rows else 0

    # ── snapshot ──────────────────────────────────────────────────────────────────────────────────────────
    def snapshot_keys(self) -> tuple[str, ...]:
        return tuple(k for k, d in self.registry.items() if d.role in ("scored", "leg", "aux"))

    async def snapshot(self, spec: "ScoringSpec", occ: Sequence[Occupant], slot: int, cutoff: int, mode: str,
                       raws: dict[tuple[str, str], RawRow] | None = None) -> Snapshot:
        if raws is None:
            raws = await self.select_raw(self.snapshot_keys(), slot, cutoff, mode, spec.raw_spec_version)
        snap: Snapshot = {o.occupant_key: {} for o in occ}
        scored = [d for d in spec.metrics.values() if d.role == "scored"]
        for d in scored:
            cohort: dict[str, float] = {}
            for o in occ:
                nv = self._norm_one(spec, d, o, raws, slot)
                snap[o.occupant_key][d.key] = nv
                if d.cross_sectional and nv.status == "ok" and nv.raw is not None and nv.reason is None \
                        and o.status == "active":
                    cohort[o.occupant_key] = float(nv.raw)
            if d.cross_sectional:
                ranked = rankg(cohort, clip_z=spec.rankg_clip_z, min_n=spec.rankg_min_n)
                n = len(cohort)
                if 0 < n < spec.rankg_min_n and f"cohort:{d.key}" not in self._warned:
                    self._warned.add(f"cohort:{d.key}")
                    logger.warning("rankg cohort too small", data={"metric_key": d.key, "n": n, "slot": slot})
                for okey, m in ranked.items():
                    nv = snap[okey][d.key]
                    nv.m, nv.cohort_n = m, n
                    if m is None:
                        nv.reason = "cohort_too_small"
        return snap

    def _norm_one(self, spec: "ScoringSpec", d: MetricDef, o: Occupant, raws: Mapping, slot: int) -> NormValue:
        if o.status == "pre_launch" and (d.pillar == "P1" or d.cross_sectional):
            return NormValue(None, None, None, "pre_launch")
        r = raws.get((o.occupant_key, d.key))
        fallback = False
        if (r is None or not r.usable) and d.fallback_key:
            fb = raws.get((o.occupant_key, d.fallback_key))
            if fb is not None and fb.usable:
                r, fallback = fb, True
        if r is None:
            return NormValue(None, None, None, "no_fresh_row")
        nv = NormValue(None, r.value, r.status, r.reason if r.status != "ok" else None, max(0, slot - r.as_of),
                       r.as_of, r.observed_at, r.source, fallback)
        if not r.usable:
            nv.reason = r.reason or "failed"
            return nv
        if r.status == "absent":
            nv.m = 0.0
            return nv
        if r.status != "ok":                         # not_applicable
            return nv
        if d.cross_sectional:
            return nv                                 # m assigned by the cohort pass
        try:
            nv.m = normalise_value(d, r.value, r.status, r.value_json)
        except PrenormOutOfRange:
            nv.m, nv.reason = None, "prenorm_out_of_range"
            tag = f"prenorm:{d.key}"
            if tag not in self._warned:
                self._warned.add(tag)
                logger.warning("prenorm value out of range", data={"metric_key": d.key, "value": r.value,
                                                                   "occupant_key": o.occupant_key})
        return nv

    def norm_rows(self, spec: "ScoringSpec", snap: Snapshot, slot: int, run_kind: str, computed_at: int,
                  run_id: str, backfilled: int) -> list[dict]:
        rows = []
        for okey, metrics in snap.items():
            for key, nv in metrics.items():
                vj: dict[str, Any] = {"raw": nv.raw, "reason": nv.reason, "raw_as_of": nv.raw_as_of,
                                      "raw_observed_at": nv.raw_observed_at, "age_s": nv.age_s, "src": nv.src,
                                      "fallback": nv.fallback}
                if nv.status is not None:
                    vj["status"] = nv.status
                if spec.metrics[key].cross_sectional:
                    vj["cohort_n"] = nv.cohort_n
                rows.append({"occupant_key": okey, "metric_key": key, "as_of": int(slot),
                             "spec_version": spec.version, "stage": "norm", "value": nv.m, "value_json": vj,
                             "observed_at": int(computed_at), "source": "scoring", "run_id": run_id,
                             "backfilled": int(backfilled)})
        return rows


# ── features_hourly ──────────────────────────────────────────────────────────────────────────────────────────
class FeaturesJob(CollectorJob):
    """Hourly raw-only refresh of the fn and derived metrics (keyed ``floor_hour(now)``; never ``stage='norm'``)."""

    async def collect(self, run: RunContext) -> JobOutcome:
        db = run.rt.db
        now = int(run.started_at)
        eng = FeatureEngine(db)
        occ = await eng.universe(now)
        n = await eng.compute_registered(occ, now, floor_hour(now), run.run_id, mode="strict")
        n += await eng.derive(occ, now, now, floor_hour(now), run.run_id, mode="strict")
        st = eng.stats
        return JobOutcome(rows=n, note=f"occupants={len(occ)} rows={n} fn_errors={st['fn_errors']} "
                                       f"unavailable={len(set(st['unavailable']))}",
                          meta={k: (sorted(set(v)) if isinstance(v, list) else v) for k, v in st.items()})
