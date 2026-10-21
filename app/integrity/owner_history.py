"""``owner_rpc_seed`` (spec 11 §8.1): one-shot import of spec 17's keyless ``rpc_owner_history`` into ``owner_changes``
(``detected_via='rpc_owner_history'``, ``evidence_json.backfilled = true`` — the §2.1 replay marker).

17's ``change`` column is not trusted: out-of-order bucket writes repeat a change (live SN70: four rows flagged
``owner_coldkey`` with one real change; BUILD-LOG Phase 3 item 9). The seed compares consecutive (coldkey, hotkey)
states per occupant ordered by block; the first row of an occupant is its baseline. Armed while
``job_state(owner_rpc_seed, rpc_seed_done)`` is absent (17's ``backfill_rpc`` or ``python -m app ops rearm
owner_rpc_seed`` delete it); re-runs are idempotent (owner_changes dedupe)."""
from __future__ import annotations

from typing import TYPE_CHECKING

from app.integrity import constants as K
from app.integrity import detectors as D
from app.jobs import CollectorJob, JobOutcome, RunContext

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database


async def seed_from_rpc_owner_history(db: "Database", *, now: int, run_id: str, since_block: int = 0) -> dict:
    rows = await db.fetchall(
        "SELECT h.*, o.network_registered_at FROM rpc_owner_history h JOIN occupants o USING (occupant_key) "
        "WHERE h.block_number >= ? AND h.netuid <> 0 ORDER BY h.occupant_key, h.block_number", (int(since_block),),
        writer=True)
    out = {"rows": len(rows), "coldkey": 0, "hotkey": 0, "collapsed": 0}
    prev: dict | None = None
    for r in rows:
        if r["block_number"] < r["network_registered_at"]:
            continue                                       # before this occupant existed: never a baseline
        if prev is None or prev["occupant_key"] != r["occupant_key"]:
            prev = r                                       # baseline of the occupant
            continue
        changed = False
        in_window = r["event_at"] >= now - K.S3_WINDOW_DAYS * K.DAY
        ev = {"rpc_block": r["block_number"], "rpc_change": r["change"], "backfilled": True}
        if r["owner_coldkey"] and prev["owner_coldkey"] and r["owner_coldkey"] != prev["owner_coldkey"]:
            changed = True

            async def classify() -> tuple[str, str]:
                return "coldkey_change_unclassified", "unclassified"
            cid = await D.detect_coldkey_change(db, r["occupant_key"], r["netuid"], prev["owner_coldkey"],
                                                r["owner_coldkey"], event_at=int(r["event_at"]), observed_at=now,
                                                run_id=run_id, detected_via="rpc_owner_history", classify=classify,
                                                block_number=r["block_number"], evidence=ev, backfilled=True,
                                                review=in_window)
            out["coldkey"] += 1 if cid else 0
        if r["owner_hotkey"] and prev["owner_hotkey"] and r["owner_hotkey"] != prev["owner_hotkey"]:
            changed = True
            cid = await D.detect_hotkey_repoint(db, r["occupant_key"], r["netuid"], prev["owner_hotkey"],
                                                r["owner_hotkey"], event_at=int(r["event_at"]), observed_at=now,
                                                run_id=run_id, detected_via="rpc_owner_history",
                                                block_number=r["block_number"], backfilled=True, evidence=ev)
            out["hotkey"] += 1 if cid else 0
        if not changed and ("owner_coldkey" in (r["change"] or "") or "owner_hotkey" in (r["change"] or "")):
            out["collapsed"] += 1
        prev = r
    return out


class OwnerRpcSeedJob(CollectorJob):
    _last: dict | None = None

    async def collect(self, run: RunContext) -> JobOutcome:
        # read the row itself (not the job's state cache): ``ops rearm`` deletes it while the service runs
        if await run.rt.db.fetchval("SELECT 1 FROM job_state WHERE job = ? AND key = 'rpc_seed_done'", (self.id,),
                                    writer=True):
            return JobOutcome(rows=0, note="not armed (rpc_seed_done set)")
        out = await seed_from_rpc_owner_history(run.rt.db, now=run.started_at, run_id=run.run_id)
        await self.set_state("rpc_seed_done", {"at": run.started_at, **out})
        self._last = out
        return JobOutcome(rows=out["coldkey"] + out["hotkey"],
                          note=f"rows={out['rows']} coldkey={out['coldkey']} hotkey={out['hotkey']} "
                               f"collapsed={out['collapsed']}", meta=out)

    def health_extra(self) -> dict | None:
        return self._last
