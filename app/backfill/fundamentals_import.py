"""Legacy ``fundamentals.json`` + ``whale_cohort.json`` import (spec 17 §2.6, step S2).

Links/owner go through 04's ``app.registry.legacy.import_fundamentals`` (``source='legacy'``,
``source_ref='fundamentals.json@2026-06-02'``, hash-dedupe → re-runs are no-ops), attributed point-in-time with the
reconstructed ``OccupancyIndex`` (observations → the occupant at ``generated_at``; overrides only when that is still
the current occupant). ``whale_cohort`` is dormant (no reader until a TaoStats key revives the whale port).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from app.backfill import common
from app.logging.logger import logger
from app.util.fs import write_atomic
from app.util.time import now_ts, parse_iso

LEGACY_REF = "fundamentals.json@2026-06-02"


def import_whale_cohort(con: sqlite3.Connection, cohort_path: Path, labels_path: Path | None, *, run_id: str) -> dict:
    """``coldkeys[]`` → ``whale_cohort`` (INSERT OR IGNORE on (as_of_block, coldkey)); labels merged before insert."""
    doc = json.loads(Path(cohort_path).read_text(encoding="utf-8"))
    labels: dict[str, Any] = {}
    if labels_path is not None and Path(labels_path).exists():
        ld = json.loads(Path(labels_path).read_text(encoding="utf-8"))
        labels = ld.get("labels") or {} if isinstance(ld, dict) else {}
    as_of_block = int(doc["as_of_block"])
    obs = now_ts()
    rows, merged = [], 0
    for r in doc.get("coldkeys") or []:
        ss58 = r.get("ss58")
        if not ss58:
            continue
        lab = labels.get(ss58)
        label, label_source, attribution = r.get("label"), r.get("label_source"), r.get("attribution")
        if isinstance(lab, dict):
            label = lab.get("label", label)
            label_source = lab.get("label_source", label_source or "whale_labels.json")
            attribution = lab.get("attribution", attribution)
            merged += 1
        elif isinstance(lab, str):
            label, label_source = lab, label_source or "whale_labels.json"
            merged += 1
        rows.append((as_of_block, ss58, r.get("rank"), r.get("balance_total_tao"), label, label_source,
                     attribution or "heuristic", r.get("total_stake_tao"), r.get("free_fraction"),
                     r.get("balance_rank"), json.dumps(r["coldkey_swap"]) if isinstance(r.get("coldkey_swap"),
                                                                                      (dict, list))
                     else r.get("coldkey_swap"), obs, "whale_cohort_json"))
    with common.tx(con):
        before = con.total_changes
        con.executemany("INSERT OR IGNORE INTO whale_cohort (as_of_block, coldkey, rank, balance_total_tao, label, "
                        "label_source, attribution, total_stake_tao, free_fraction, balance_rank, coldkey_swap, "
                        "observed_at, source) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        written = con.total_changes - before
    return {"whale_rows": written, "whale_candidates": len(rows), "whale_excluded": len(doc.get("excluded") or []),
            "whale_as_of_ts": doc.get("as_of_ts"), "whale_as_of_block": as_of_block, "labels_merged": merged}


async def _team_info(db, records: dict[int, dict], occ, as_of: int, run_id: str) -> int:
    """A newer file may carry ``team_info`` → one ``identity_observations`` row (spec 11 parses it later)."""
    from app.registry.models import Occupant
    from app.registry.observations import ObservationWriter
    n = 0
    for netuid, rec in records.items():
        txt = rec.get("team_info")
        if not txt:
            continue
        key = occ.occupant_at_ts(netuid, as_of)
        if key is None:
            continue
        r = await db.fetchone("SELECT * FROM occupants WHERE occupant_key = ?", (key,))
        if not r:
            continue
        w = ObservationWriter(db, run_id, as_of)
        if await w.write(Occupant.from_row(r), "legacy", "team_info", None,
                         txt if isinstance(txt, str) else json.dumps(txt), event_at=as_of, block_number=None,
                         source_ref=LEGACY_REF):
            n += 1
    return n


async def run_import(*, fundamentals: Path | None = None, whale_cohort: Path | None = None,
                     db_path: Path | None = None, force: bool = False) -> dict:
    from app.backfill.occupancy import OccupancyIndex
    from app.registry.legacy import _records, import_fundamentals
    s = common.settings()
    fpath = fundamentals or s.path(s.LEGACY_FUNDAMENTALS_PATH)
    wpath = whale_cohort or s.path(s.WHALE_COHORT_SEED_PATH)
    lpath = s.path(s.WHALE_LABELS_SEED_PATH)
    rep: dict[str, Any] = {"fundamentals": str(fpath), "whale_cohort": str(wpath)}
    db = await common.prepare_db(db_path)
    con = common.open_db(db_path)
    try:
        with common.step_run(con, "import_fundamentals") as run:
            occ = OccupancyIndex.load(con)
            if fpath.exists():
                data = json.loads(fpath.read_text(encoding="utf-8"))
                as_of = int(data.get("generated_at_unix") or parse_iso(s.LEGACY_AS_OF))
                legacy_as_of = parse_iso(s.LEGACY_AS_OF)
                if abs(as_of - legacy_as_of) > 1 and not force:
                    raise common.PreconditionError(
                        f"fundamentals generated_at_unix {as_of} disagrees with LEGACY_AS_OF {s.LEGACY_AS_OF} "
                        "(pass --force to import at the file's own time)")
                lr = await import_fundamentals(db, fpath, as_of=as_of, run_id=run.run_id, occupancy=occ,
                                               created_by="import_fundamentals")
                rep["legacy"] = lr.as_dict()
                rep["raw_snapshots"] = await db.fetchval(
                    "SELECT COUNT(*) FROM identity_raw_snapshots WHERE source = 'legacy' AND source_ref = ?",
                    (LEGACY_REF,))
                rep["team_info_rows"] = await _team_info(db, _records(data), occ, as_of, run.run_id)
                rep["attributed_via"] = {"occupancy": lr.attributed_via_occupancy, "guard_04": lr.attributed_via_guard}
            else:
                rep["legacy"] = {"status": "not_found"}
            if wpath.exists():
                rep.update(import_whale_cohort(con, wpath, lpath, run_id=run.run_id))
            else:
                rep["whale"] = {"status": "not_found"}
            run.rows = int((rep.get("legacy") or {}).get("observations", 0) or 0) + int(rep.get("whale_rows", 0))
            run.meta = {k: v for k, v in rep.items() if k != "legacy"}
        rep["run_id"] = run.run_id
        write_atomic(common.backfill_dir() / "fundamentals_import_report.json", rep)
        await common.emit_step_event(db, "backfill_done", job="import_fundamentals", era=None, run_id=run.run_id,
                                     rows=run.rows, seconds=run.seconds)
        logger.info("fundamentals import done", data={k: v for k, v in rep.items() if k != "legacy"})
        return rep
    finally:
        con.close()
        await db.close()
