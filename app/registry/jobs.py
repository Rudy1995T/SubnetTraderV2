"""Registry jobs (spec 04 §2.1, §8.3): ``registry_occupants`` (every 6 h) and ``registry_identity`` (02:40Z)."""
from __future__ import annotations

import json
from typing import Any

import app.config as app_config
from app.collectors.chain_tmc import TmcClient, latest_rest_snapshot, write_rest_snapshot
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.ratelimit import BudgetExhausted
from app.registry import anomalies
from app.registry.merge import Merger
from app.registry.models import Occupant, RegistryRunError, RpcError, SourceRunResult
from app.registry.normalise import classify
from app.registry.observations import ObservationWriter
from app.registry.occupants import OccupantTracker, ensure_occupants_fresh
from app.registry.review import compute_needs_review
from app.registry.sources.chain_rpc import IDENTITY_V3_FIELDS, SubtensorRpc
from app.registry.sources.subnetalpha import ACF_FIELD_MAP, SubnetalphaError, fetch_subnetalpha
from app.registry.sources.tmc import (fetch_occupant_inputs, parse_coldkey_identities, snapshot_block,
                                      snapshot_identities)
from app.registry.watch import WatchChannelsBuilder
from app.util.time import now_ts, to_iso

OCC_JOB = "registry_occupants"
ID_JOB = "registry_identity"
COLDKEY_FIELDS = {"name": "owner_name", "url": "owner_url", "github_repo": "owner_github", "discord": "owner_discord",
                  "image": "owner_image", "description": "owner_description", "additional": "owner_additional"}


async def _state(db, job: str, key: str, default: Any = None) -> Any:
    v = await db.fetchval("SELECT value_json FROM job_state WHERE job = ? AND key = ?", (job, key))
    return json.loads(v) if v else default


async def _set_state(db, job: str, key: str, value: Any) -> None:
    await db.upsert("job_state", [{"job": job, "key": key, "value_json": json.dumps(value), "updated_at": now_ts()}],
                    conflict=("job", "key"))


async def sync_occupants(run: RunContext) -> dict:
    """Fetch (+ table fallback) → sanity → T1–T6 in one transaction → snapshot → watermarks. Raises on a failed run."""
    db = run.rt.db
    now = run.started_at
    tmc = TmcClient(run.rt.http)
    inputs, resp, kind, err = await fetch_occupant_inputs(tmc)
    prev_head = await _state(db, OCC_JOB, "head_block")
    head = max((i.block_number for i in inputs if i.block_number), default=None) if inputs else None
    if inputs and prev_head and head is not None and head < int(prev_head):
        inputs, err = [], f"head_block went backwards ({head} < {prev_head})"
    if not inputs or head is None:
        await anomalies.gate(db, job=OCC_JOB, reason="sanity_failed", condition=True, now=now, run_id=run.run_id,
                             detail={"error": err})
        raise RegistryRunError(f"sanity: {err}")
    first_sane = await _state(db, OCC_JOB, "first_sane_at")
    res = await OccupantTracker(db, run.run_id, now).sync(inputs, head, prior_sane=first_sane is not None)
    await write_rest_snapshot(db, kind, resp, run_id=run.run_id, observed_at=now)
    await anomalies.gate(db, job=OCC_JOB, reason="sanity_failed", condition=False, now=now, run_id=run.run_id)
    await _set_state(db, OCC_JOB, "head_block", head)
    if first_sane is None:
        await _set_state(db, OCC_JOB, "first_sane_at", now)
    counts = {r["status"]: r["n"] for r in await db.fetchall(
        "SELECT o.status, COUNT(*) AS n FROM subnets s JOIN occupants o USING (occupant_key) GROUP BY o.status")}
    return {"head_block": head, "rows_in": len(inputs), "fallback_used": kind == "subnets_table", "kind": kind,
            "fallback_reason": err, **res.counts(), "active": counts.get("active", 0),
            "pre_launch": counts.get("pre_launch", 0), "deregistered": counts.get("deregistered", 0)}


class RegistryOccupantsJob(CollectorJob):
    _last: dict | None = None

    async def collect(self, run: RunContext) -> JobOutcome:
        meta = await sync_occupants(run)
        self._last = meta
        note = (f"head={meta['head_block']} rows={meta['rows_in']} active={meta['active']} "
                f"pre_launch={meta['pre_launch']} reoccupied={meta['reoccupied']}"
                + (" fallback=table" if meta["fallback_used"] else ""))
        return JobOutcome(rows=meta["rows_in"], note=note, meta=meta)

    def health_extra(self) -> dict | None:
        m = self._last
        if not m:
            return None
        return {k: m.get(k) for k in ("head_block", "active", "pre_launch", "deregistered", "fallback_used", "rows_in")}


async def current_occupants(db) -> list[Occupant]:
    rows = await db.fetchall("SELECT o.*, s.owner_coldkey, s.owner_hotkey, s.name AS tmc_name FROM subnets s "
                             "JOIN occupants o USING (occupant_key) WHERE o.status <> 'deregistered' ORDER BY s.netuid")
    out = []
    for r in rows:
        o = Occupant.from_row(r)
        o.name = r["tmc_name"] or r["name"]
        out.append(o)
    return out


class RegistryIdentityJob(CollectorJob):
    _last: dict | None = None

    async def collect(self, run: RunContext) -> JobOutcome:
        s = app_config.settings
        db = run.rt.db
        now = run.started_at
        inline = await ensure_occupants_fresh(db, run)
        occs = await current_occupants(db)
        if not occs:
            raise RegistryRunError("no_occupants")
        by_netuid = {o.netuid: o for o in occs}
        w = ObservationWriter(db, run.run_id, now)
        sources: dict[str, SourceRunResult] = {}
        anomalies_seen: list[str] = []

        # ── chain ─────────────────────────────────────────────────────────────────────────────────────────────
        chain = SourceRunResult(False)
        rows: dict[int, dict[str, str]] | None = None
        block = None
        via = None
        suspect = False
        raw_hex: dict[int, str] = {}
        decode_errors: dict[int, str] = {}
        low_keys = False
        if s.REGISTRY_CHAIN_ENABLED:
            rpc = SubtensorRpc([s.SUBTENSOR_RPC_URL, *s.SUBTENSOR_RPC_FALLBACK_URLS], run.rt.http,
                               s.SUBTENSOR_RPC_TIMEOUT_SEC)
            try:
                cr = await rpc.read_subnet_identities_v3(s.REGISTRY_CHAIN_QUERY_CHUNK)
                if cr.n_keys - cr.bad_keys < s.REGISTRY_CHAIN_MIN_KEYS:
                    low_keys = True
                    chain.error = f"sanity:low_key_count ({cr.n_keys})"
                else:
                    rows, block, via = cr.rows, cr.block_number, "rpc"
                    raw_hex, decode_errors = cr.raw, cr.decode_errors
                    suspect = cr.n_keys < 110
                chain.extra.update(keys=cr.n_keys, bad_keys=cr.bad_keys, decode_errors=len(cr.decode_errors),
                                   block_number=cr.block_number, calls=cr.calls)
            except (RpcError, BudgetExhausted) as exc:
                chain.error = f"{type(exc).__name__}: {exc}"[:300]
        if rows is None:
            snap = await latest_rest_snapshot(db, "subnets", before=now, max_age_s=12 * 3600)
            if snap and snap.payload:
                rows, block, via = snapshot_identities(snap.payload), snapshot_block(snap.payload), "tmc_snapshot"
        if rows is not None:
            async with db.transaction():
                for occ in occs:
                    if occ.netuid in decode_errors and occ.netuid not in rows:
                        await w.write_raw("chain", "subnet", occ, {"value_hex": raw_hex.get(occ.netuid),
                                                                    "decode_error": decode_errors[occ.netuid]},
                                          source_ref=None, event_at=None, block_number=block)
                        await w.bump_miss("chain", occ)
                        continue
                    ident = rows.get(occ.netuid)
                    if ident is None:
                        await w.mark_coverage("chain", occ, False, block)
                        continue
                    if via == "rpc":
                        await w.write_raw("chain", "subnet", occ, {"value_hex": raw_hex.get(occ.netuid), **ident},
                                          source_ref=None, event_at=None, block_number=block)
                    for field in IDENTITY_V3_FIELDS:
                        if field not in ident:
                            continue
                        raw = ident[field] or ""
                        await w.write_value(occ, "chain", field, raw, classify(raw, field), event_at=None,
                                            block_number=block, source_ref="tmc_snapshot" if via == "tmc_snapshot"
                                            else None, suspect=suspect, via=via)
                    await w.mark_coverage("chain", occ, True, block)
                    await w.finish_source(occ, "chain", block_number=block)
            chain.ok = True
            chain.via = via
            chain.n = len(rows)
        sources["chain"] = chain

        # ── subnetalpha ───────────────────────────────────────────────────────────────────────────────────────
        sa = SourceRunResult(False)
        missing_keys: set[str] = set()
        if s.SUBNETALPHA_ENABLED:
            try:
                res = await fetch_subnetalpha(run.rt.http)
                missing_keys = res.missing_acf_keys
                sa.extra.update(records=len(res.records), pages=res.pages, duplicate_netuids=res.duplicate_netuids,
                                missing_acf_keys=sorted(missing_keys))
                if len(res.records) < s.REGISTRY_SUBNETALPHA_MIN_RECORDS:
                    sa.error = f"sanity:records={len(res.records)}"
                else:
                    async with db.transaction():
                        for occ in occs:
                            rec = res.records.get(occ.netuid)
                            if rec is None:
                                await w.mark_coverage("subnetalpha", occ, False, None)
                                continue
                            await w.write_raw("subnetalpha", "subnet", occ,
                                              {"id": rec.wp_id, "slug": rec.slug, "title": rec.name, "acf": rec.acf},
                                              source_ref=rec.slug, event_at=rec.modified_ts, block_number=None)
                            await w.write(occ, "subnetalpha", "slug", None, rec.slug or "", event_at=rec.modified_ts,
                                          block_number=None, source_ref=rec.slug)
                            await w.write_value(occ, "subnetalpha", "subnet_name", rec.name,
                                                classify(rec.name, "subnet_name"), event_at=rec.modified_ts,
                                                block_number=None, source_ref=rec.slug)
                            for key, field in ACF_FIELD_MAP.items():
                                if key in missing_keys:
                                    continue
                                val = rec.acf.get(key)
                                raw = "" if val is None or val is False else str(val)
                                refs = [] if field == "twitter_embed_id" else classify(raw, field)
                                await w.write_value(occ, "subnetalpha", field, raw, refs, event_at=rec.modified_ts,
                                                    block_number=None, source_ref=rec.slug)
                            await w.mark_coverage("subnetalpha", occ, True, None)
                            await w.finish_source(occ, "subnetalpha")
                    sa.ok = True
                    sa.n = len(res.records)
            except SubnetalphaError as exc:
                sa.error = str(exc)[:300]
            except Exception as exc:          # ProviderError, BudgetExhausted on a free provider, bad shapes
                sa.error = f"{type(exc).__name__}: {exc}"[:300]
        sources["subnetalpha"] = sa

        # ── TMC name + owner coldkey (from the occupant snapshot) and coldkey identities ─────────────────────────
        async with db.transaction():
            for occ in occs:
                await w.write(occ, "tmc", "owner_coldkey", None, occ.owner_coldkey or "", event_at=None,
                              block_number=None, source_ref=None)
                nm = occ.name or ""
                await w.write_value(occ, "tmc", "subnet_name", nm, classify(nm, "subnet_name"), event_at=None,
                                    block_number=None, source_ref=None)
                await w.mark_coverage("tmc", occ, True, None)
        ck = SourceRunResult(False)
        if s.REGISTRY_TMC_COLDKEY_ENABLED:
            try:
                resp = await TmcClient(run.rt.http).fetch_coldkey_identities()
                if resp.ok:
                    await write_rest_snapshot(db, "coldkey_identities", resp, run_id=run.run_id, observed_at=now)
                    idx = parse_coldkey_identities(resp)
                    matched = 0
                    async with db.transaction():
                        for occ in occs:
                            if not occ.owner_coldkey:
                                continue
                            row = idx.get(occ.owner_coldkey)
                            if row is not None:
                                matched += 1
                                await w.write_raw("tmc_coldkey", "coldkey", occ, row, source_ref=occ.owner_coldkey,
                                                  event_at=None, block_number=None)
                                for col, field in COLDKEY_FIELDS.items():
                                    raw = str(row.get(col) or "")
                                    await w.write_value(occ, "tmc_coldkey", field, raw, classify(raw, field),
                                                        event_at=None, block_number=None, source_ref=occ.owner_coldkey)
                            await w.mark_coverage("tmc_coldkey", occ, row is not None, None)
                            await w.finish_source(occ, "tmc_coldkey")
                    ck.ok, ck.n = True, len(idx)
                    ck.extra.update(rows=len(idx), matched_owners=matched)
                else:
                    ck.error = resp.error
            except BudgetExhausted:
                ck.error = "budget"
        sources["tmc_coldkey"] = ck

        # ── merge, watch channels ─────────────────────────────────────────────────────────────────────────────
        merger = Merger(db, run.run_id, now)
        wb = WatchChannelsBuilder(db, now)
        async with db.transaction():
            for occ in occs:
                resolved = await merger.resolve_all(occ, change_events=w.change_events)
                await wb.rebuild(occ, resolved)
            retired = await wb.retire_stale()

        # ── anomaly gates, bookkeeping ────────────────────────────────────────────────────────────────────────
        for name, res in sources.items():
            enabled = {"chain": s.REGISTRY_CHAIN_ENABLED, "subnetalpha": s.SUBNETALPHA_ENABLED,
                       "tmc_coldkey": s.REGISTRY_TMC_COLDKEY_ENABLED}[name]
            failed = enabled and (not res.ok or (name == "chain" and res.via != "rpc"))
            key = f"source_fail:{name}"
            if failed:
                n = int(await _state(db, ID_JOB, key, 0)) + 1
                await _set_state(db, ID_JOB, key, n)
            else:
                await _set_state(db, ID_JOB, key, 0)
                await _set_state(db, ID_JOB, f"last_ok:{name}", now)
            ev = await anomalies.gate(db, job=ID_JOB, reason="source_failed", condition=bool(failed), now=now,
                                      run_id=run.run_id, scope=name, detail={"source": name, "last_error": res.error,
                                                                 "last_ok_at": await _state(db, ID_JOB,
                                                                                            f"last_ok:{name}")})
            if failed:
                anomalies_seen.append(f"source_failed:{name}")
            del ev
        await anomalies.gate(db, job=ID_JOB, reason="chain_low_key_count", condition=low_keys or suspect, now=now,
                             run_id=run.run_id, detail=chain.extra)
        await anomalies.gate(db, job=ID_JOB, reason="acf_key_missing", condition=bool(missing_keys), now=now,
                             run_id=run.run_id, detail={"missing": sorted(missing_keys)})
        prev_run = await _state(db, ID_JOB, "last_started_at")
        await anomalies.gate(db, job=ID_JOB, reason="clock_skew", condition=bool(prev_run and now < int(prev_run)),
                             now=now, run_id=run.run_id, detail={"prev": prev_run, "now": now})
        await _set_state(db, ID_JOB, "last_started_at", now)
        review = await compute_needs_review(db, now)
        events = dict(w.events)
        events.update(merger.events)
        meta = {"inline_occupant_sync": inline, "sources": {k: v.as_dict() for k, v in sources.items()},
                "observations": w.written, "events_emitted": events, "watch_retired": retired,
                "needs_review_count": len(review), "anomalies": anomalies_seen}
        self._last = {**meta, "at": now}
        if not any(r.ok for r in sources.values()):
            raise RegistryRunError("all identity sources failed: " +
                                   "; ".join(f"{k}={v.error}" for k, v in sources.items()))
        note = (f"obs={w.written} chain={'ok/' + str(chain.via) if chain.ok else 'fail'} "
                f"subnetalpha={'ok' if sa.ok else 'fail'} coldkey={'ok' if ck.ok else ck.error or 'off'} "
                f"review={len(review)}")
        logger.info("registry identity run", data={"note": note, "events": events})
        return JobOutcome(rows=w.written, note=note, meta=meta)

    def health_extra(self) -> dict | None:
        m = self._last
        if not m:
            return None
        return {"sources": m["sources"], "events_emitted": m["events_emitted"],
                "needs_review_count": m["needs_review_count"], "last_identity_run": to_iso(m["at"])}
