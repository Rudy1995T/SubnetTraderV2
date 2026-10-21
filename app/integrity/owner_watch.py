"""``owner_watch_hourly`` (spec 11 §2.9 D1/D3/D3b/D3c, DB only): scans ``chain_hourly`` since ``job_state.watch_wm``
(including back-filled hours) for owner-coldkey changes (``subnet_owner``) and perpetual→decaying lock transitions
at zero REST cost. Hourly D1 classification has no C2 call (DB only): TaoStats/Rufus/subnetradar evidence from the
last snapshot, else ``coldkey_change_unclassified``; the daily collect sees the same change and does not duplicate
it (one change per occupant and new coldkey within ±3 d)."""
from __future__ import annotations

import json
from typing import TYPE_CHECKING

import app.config as app_config
from app.integrity import detectors as D
from app.jobs import CollectorJob, JobOutcome, RunContext

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

HOUR = 3600
FIRST_LOOKBACK_H = 48


def _same_basis(a: str, b: str) -> bool:
    """A pair spanning two sources counts only when 05's units probe marks them identical (``sse``/``rest_table``/
    ``tick_rollup`` are the same TMC table feed; ``analytics_1w`` rows are derived and differ)."""
    live = {"sse", "rest_table", "tick_rollup"}
    return a in live and b in live


async def last_radar_rows(db: "Database") -> list[dict]:
    v = await db.fetchval("SELECT payload_json FROM chain_rest_snapshots WHERE endpoint = 'subnetradar/swaps' "
                          "ORDER BY observed_at DESC LIMIT 1", writer=True)
    try:
        return list((json.loads(v) or {}).get("rows") or []) if v else []
    except ValueError:
        return []


async def watch(db: "Database", *, now: int, run_id: str, since: int | None) -> dict:
    s = app_config.settings
    from app.integrity.features import occupants_for_run
    occs = await occupants_for_run(db)
    lo = since if since is not None else now - FIRST_LOOKBACK_H * HOUR
    radar = await last_radar_rows(db)
    out = {"occupants": len(occs), "changes": 0, "rows": 0, "wm": lo}
    max_hour = lo
    for occ in occs:
        okey, n = occ["occupant_key"], int(occ["netuid"])
        rows = await db.fetchall(
            "SELECT hour_ts, source, block_number, subnet_owner, owner_personal_perpetual_alpha, "
            "owner_personal_decaying_alpha, owner_personal_locked_alpha FROM chain_hourly WHERE netuid = ? AND "
            "occupant_key = ? AND hour_ts >= ? AND hour_ts <= ? ORDER BY hour_ts", (n, okey, lo - HOUR, now),
            writer=True)
        out["rows"] += len(rows)
        if not rows:
            continue
        max_hour = max(max_hour, int(rows[-1]["hour_ts"]))
        for ctype, detail in D.detect_lock_transitions(rows, min_alpha=float(s.OWNER_PERP_OFF_MIN_ALPHA),
                                                       match_tol=float(s.OWNER_PERP_OFF_MATCH_TOL),
                                                       same_basis=_same_basis):
            if detail["hour_ts"] < lo:
                continue
            cid = await D.record_lock_transition(db, okey, n, ctype, detail, observed_at=now, run_id=run_id)
            out["changes"] += 1 if cid else 0
        owners = [r for r in rows if r.get("subnet_owner") and str(r["subnet_owner"]).startswith("5")]
        snap = await db.fetchone("SELECT raw_json, rufus_days_to_king FROM owner_snapshots WHERE occupant_key = ? "
                                 "ORDER BY observed_at DESC LIMIT 1", (okey,), writer=True)
        raw = json.loads((snap or {}).get("raw_json") or "{}")
        for a, b in zip(owners, owners[1:]):
            if a["subnet_owner"] == b["subnet_owner"] or int(b["hour_ts"]) < lo:
                continue
            old, new = a["subnet_owner"], b["subnet_owner"]

            async def classify(old=old, new=new) -> tuple[str, str]:
                return D.classify_coldkey_change(challenger_coldkeys=raw.get("rufus_challenger_coldkeys") or [],
                                                 days_to_king=(snap or {}).get("rufus_days_to_king"), radar_rows=radar,
                                                 old_ck=old, new_ck=new, netuid=n)
            cid = await D.detect_coldkey_change(db, okey, n, old, new, event_at=int(b["hour_ts"]), observed_at=now,
                                                run_id=run_id, detected_via="chain_hourly", classify=classify,
                                                block_number=b.get("block_number"))
            out["changes"] += 1 if cid else 0
    out["wm"] = max_hour
    return out


class OwnerWatchJob(CollectorJob):
    _last: dict | None = None

    async def collect(self, run: RunContext) -> JobOutcome:
        since = await self.get_state("watch_wm")
        out = await watch(run.rt.db, now=run.started_at, run_id=run.run_id, since=int(since) if since else None)
        await self.set_state("watch_wm", out["wm"])
        self._last = out
        return JobOutcome(rows=out["changes"], note=f"occupants={out['occupants']} hourly_rows={out['rows']} "
                                                    f"changes={out['changes']}", meta=out)

    def health_extra(self) -> dict | None:
        return self._last
