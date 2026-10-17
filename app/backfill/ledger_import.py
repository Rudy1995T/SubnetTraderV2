"""Old-ledger import (spec 17 §2.1): PC → Pi → extras into ``pool_history`` (INSERT OR IGNORE by
``(netuid, block_number)``), NULL-only merge of ``moving_price``/``subnet_volume`` from ``ledger_backfill.db`` and the
other copies, honest ``observed_at`` (live capture vs later backfill), ``network_registered_at`` only from the trust
boundary. Sources are attached read-only; a copy with a non-empty ``-wal`` is checkpointed into a private copy first.
"""
from __future__ import annotations

import sqlite3
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from app.backfill import common
from app.logging.logger import logger
from app.util.fs import write_atomic
from app.util.time import now_ts

REQUIRED_SRC_COLS = ("netuid", "block_number", "timestamp", "price", "tao_in_pool", "alpha_in_pool")
OPTIONAL_SRC_COLS = ("moving_price", "network_registered_at", "subnet_volume")


@dataclass(frozen=True)
class SourceSpec:
    path: Path
    source: str                 # ledger_pc | ledger_pi | ledger_extra | ledger_backfill
    merge_only: bool = False


@dataclass
class SourceReport:
    source: str
    path: str
    rows_in: int = 0
    rows_new: int = 0
    rows_skipped_nonpositive: int = 0
    rows_preexisting: int = 0        # (netuid, block) already held by an earlier writer (e.g. an RPC era row)
    nra_dropped_untrusted: int = 0
    merged_mp: int = 0
    merged_sv: int = 0
    merged_nra: int = 0
    min_ts: int | None = None
    max_ts: int | None = None
    min_block: int | None = None
    max_block: int | None = None
    netuids: int = 0
    status: str = "ok"
    missing_columns: list[str] = field(default_factory=list)


def discover_sources(s, extra: list[Path] | None = None) -> list[SourceSpec]:
    out: list[SourceSpec] = []
    pc = s.path(s.LEDGER_SEED_PATH)
    if pc.exists():
        out.append(SourceSpec(pc, "ledger_pc"))
    pi = s.path(s.LEDGER_PI_SEED_PATH)
    if pi.exists():
        out.append(SourceSpec(pi, "ledger_pi"))
    for e in [*(s.path(x) for x in s.LEDGER_EXTRA_SEED_PATHS), *(extra or [])]:
        if Path(e).exists():
            out.append(SourceSpec(Path(e), "ledger_extra"))
    for cand in (s.path(s.LEDGER_BACKFILL_SEED_PATH), s.path(s.SEED_DIR) / "backups" / "ledger_backfill.db"):
        if cand.exists():
            out.append(SourceSpec(cand, "ledger_backfill", merge_only=True))
            break
    return out


def _quick_check(path: Path) -> str:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        rows = con.execute("PRAGMA quick_check").fetchall()
    finally:
        con.close()
    return "ok" if [r[0] for r in rows] == ["ok"] else "; ".join(str(r[0]) for r in rows[:3])


def prepare_source(spec: SourceSpec, tmp_dir: Path) -> tuple[Path, bool]:
    """(path to attach, immutable?) — a non-empty ``-wal`` is checkpointed into ``tmp/<name>.checkpointed.db``."""
    if not spec.path.exists():
        raise common.PreconditionError(f"seed file missing: {spec.path}")
    wal = Path(str(spec.path) + "-wal")
    if wal.exists() and wal.stat().st_size > 0:
        tmp_dir.mkdir(parents=True, exist_ok=True)
        dst = tmp_dir / f"{spec.path.stem}.checkpointed.db"
        if dst.exists():
            dst.unlink()
        src = sqlite3.connect(str(spec.path))
        try:
            d = sqlite3.connect(str(dst))
            src.backup(d)
            d.close()
        finally:
            src.close()
        qc = _quick_check(dst)
        if qc != "ok":
            raise common.PreconditionError(f"checkpointed copy of {spec.path} failed quick_check: {qc}")
        return dst, False
    qc = _quick_check(spec.path)
    if qc != "ok":
        raise common.PreconditionError(f"seed DB {spec.path} failed quick_check: {qc}")
    return spec.path, True


def _attach(con: sqlite3.Connection, path: Path, immutable: bool) -> list[str]:
    uri = f"file:{path}?mode=ro" + ("&immutable=1" if immutable else "")
    con.execute("ATTACH DATABASE ? AS src", (uri,))
    cols = [r[1] for r in con.execute("PRAGMA src.table_info(pool_history)")]
    return cols


def _detach(con: sqlite3.Connection) -> None:
    try:
        con.execute("DETACH DATABASE src")
    except sqlite3.OperationalError:
        pass


def _col(cols: list[str], c: str) -> str:
    return c if c in cols else f"NULL AS {c}"


def import_source(con: sqlite3.Connection, spec: SourceSpec, attach_path: Path, immutable: bool, *, trust_ts: int,
                  live_from_ts: int, run_started: int, thin_sec: int, sidecar: common.Sidecar, pause_s: float,
                  run_id: str, batch_rows: int | None = None) -> SourceReport:
    rep = SourceReport(spec.source, str(spec.path))
    cols = _attach(con, attach_path, immutable)
    try:
        missing = [c for c in REQUIRED_SRC_COLS if c not in cols]
        if missing:
            rep.status, rep.missing_columns = "bad_schema", missing
            return rep
        mp, nra, sv = (_col(cols, c) for c in OPTIONAL_SRC_COLS)
        nra_expr = "network_registered_at" if "network_registered_at" in cols else "NULL"
        thin_clause = ""
        if thin_sec:
            thin_clause = (" AND rowid IN (SELECT rowid FROM (SELECT rowid, ROW_NUMBER() OVER (PARTITION BY "
                           "timestamp / :thin ORDER BY timestamp DESC) rn FROM src.pool_history WHERE netuid = :netuid)"
                           " WHERE rn = 1)")
        sql = (
            "INSERT OR IGNORE INTO main.pool_history (netuid, block_number, timestamp, price, tao_in_pool, "
            "alpha_in_pool, moving_price, network_registered_at, subnet_volume, occupant_key, source, event_at, "
            "observed_at, price_method, run_id) "
            f"SELECT netuid, block_number, timestamp, price, tao_in_pool, alpha_in_pool, {mp.split(' AS ')[0]}, "
            f"CASE WHEN timestamp >= :trust THEN {nra_expr} END, {sv.split(' AS ')[0]}, "
            f"CASE WHEN timestamp >= :trust AND {nra_expr} IS NOT NULL THEN netuid || ':' || {nra_expr} END, "
            ":source, NULL, CASE WHEN timestamp >= :live_from THEN timestamp ELSE :run_started END, NULL, :run_id "
            "FROM src.pool_history WHERE netuid = :netuid AND price > 0 AND tao_in_pool > 0 "
            "AND block_number >= :b_lo AND block_number <= :b_hi" + thin_clause)
        netuids = [r[0] for r in con.execute("SELECT DISTINCT netuid FROM src.pool_history ORDER BY netuid")]
        rep.netuids = len(netuids)
        for n in netuids:
            unit = sidecar.data.setdefault(spec.source, {}).setdefault(str(n), {})
            stats = con.execute(
                "SELECT COUNT(*), SUM(price <= 0 OR tao_in_pool <= 0), "
                f"SUM(timestamp < ? AND {nra_expr} IS NOT NULL), MIN(timestamp), MAX(timestamp), "
                "MIN(block_number), MAX(block_number) FROM src.pool_history WHERE netuid = ?", (trust_ts, n)).fetchone()
            rep.rows_in += stats[0] or 0
            rep.rows_skipped_nonpositive += stats[1] or 0
            rep.nra_dropped_untrusted += stats[2] or 0
            for i, v in ((3, "min_ts"), (5, "min_block")):
                if stats[i] is not None:
                    setattr(rep, v, stats[i] if getattr(rep, v) is None else min(getattr(rep, v), stats[i]))
            for i, v in ((4, "max_ts"), (6, "max_block")):
                if stats[i] is not None:
                    setattr(rep, v, stats[i] if getattr(rep, v) is None else max(getattr(rep, v), stats[i]))
            if unit.get("done"):
                continue
            ranges = [(stats[5], stats[6])] if stats[5] is not None else []
            if batch_rows and stats[0] and stats[0] > batch_rows:
                bounds = [r[0] for r in con.execute(
                    "SELECT block_number FROM (SELECT block_number, ROW_NUMBER() OVER (ORDER BY block_number) rn "
                    "FROM src.pool_history WHERE netuid = ?) WHERE rn % ? = 1", (n, batch_rows))]
                bounds.append(stats[6] + 1)
                ranges = [(bounds[i], bounds[i + 1] - 1) for i in range(len(bounds) - 1)]
            new = 0
            for lo, hi in ranges:
                with common.tx(con):
                    cur = con.execute(sql, {"trust": trust_ts, "live_from": live_from_ts, "run_started": run_started,
                                            "source": spec.source, "run_id": run_id, "netuid": n, "thin": thin_sec,
                                            "b_lo": lo, "b_hi": hi})
                    new += max(0, cur.rowcount)
                if pause_s:
                    time.sleep(pause_s)
            rep.rows_new += new
            if not thin_sec:
                rep.rows_preexisting += (stats[0] or 0) - (stats[1] or 0) - new
            unit.update({"rows_in": stats[0], "rows_new": new, "done": True})
            sidecar.save()
            if common.stop_requested():
                rep.status = "stopped"
                break
    finally:
        _detach(con)
    return rep


MERGE_SQL = """
UPDATE main.pool_history AS p SET
  moving_price  = COALESCE(p.moving_price,  b.moving_price),
  subnet_volume = COALESCE(p.subnet_volume, b.subnet_volume),
  network_registered_at = COALESCE(p.network_registered_at, CASE WHEN b.timestamp >= :trust THEN b.network_registered_at END),
  occupant_key  = COALESCE(p.occupant_key, CASE WHEN b.timestamp >= :trust AND b.network_registered_at IS NOT NULL
                                              THEN b.netuid || ':' || b.network_registered_at END),
  enriched_at   = COALESCE(p.enriched_at, :now)
FROM src.pool_history AS b
WHERE b.netuid = p.netuid AND b.block_number = p.block_number AND b.netuid = :netuid AND p.netuid = :netuid
  AND (   (p.moving_price  IS NULL AND b.moving_price  IS NOT NULL)
       OR (p.subnet_volume IS NULL AND b.subnet_volume IS NOT NULL)
       OR (p.network_registered_at IS NULL AND b.network_registered_at IS NOT NULL AND b.timestamp >= :trust))
"""

COUNT_SQL = """
SELECT SUM(p.moving_price IS NULL AND b.moving_price IS NOT NULL),
       SUM(p.subnet_volume IS NULL AND b.subnet_volume IS NOT NULL),
       SUM(p.network_registered_at IS NULL AND b.network_registered_at IS NOT NULL AND b.timestamp >= :trust)
FROM main.pool_history p JOIN src.pool_history b ON b.netuid = p.netuid AND b.block_number = p.block_number
WHERE p.netuid = :netuid AND b.netuid = :netuid
"""


def merge_nulls(con: sqlite3.Connection, spec: SourceSpec, attach_path: Path, immutable: bool, *, trust_ts: int,
                now: int) -> tuple[int, int, int, int]:
    """(rows changed, mp, sv, nra) — never overwrites a non-NULL value (03 trigger enforces it too)."""
    cols = _attach(con, attach_path, immutable)
    changed = mp = sv = nra = 0
    try:
        sel = ", ".join(f"{c}" if c in cols else f"NULL AS {c}" for c in
                        ("netuid", "block_number", "timestamp", *OPTIONAL_SRC_COLS))
        con.execute("DROP VIEW IF EXISTS temp.merge_src")
        con.execute(f"CREATE TEMP VIEW merge_src AS SELECT {sel} FROM src.pool_history")
        merge = MERGE_SQL.replace("src.pool_history", "temp.merge_src")
        count = COUNT_SQL.replace("src.pool_history", "temp.merge_src")
        for (n,) in con.execute("SELECT DISTINCT netuid FROM src.pool_history ORDER BY netuid").fetchall():
            c = con.execute(count, {"trust": trust_ts, "netuid": n}).fetchone()
            with common.tx(con):
                cur = con.execute(merge, {"trust": trust_ts, "now": now, "netuid": n})
            changed += max(0, cur.rowcount)
            mp += c[0] or 0
            sv += c[1] or 0
            nra += c[2] or 0
        con.execute("DROP VIEW IF EXISTS temp.merge_src")
    finally:
        _detach(con)
    return changed, mp, sv, nra


PC_MAX_TS = 1781904274           # last PC-ledger row (block 8,443,757): the span the §10 counts were verified on


def _attach_ro(con: sqlite3.Connection, path: Path, alias: str) -> None:
    wal = Path(str(path) + "-wal")
    imm = "" if wal.exists() and wal.stat().st_size > 0 else "&immutable=1"   # read the WAL, never ignore it
    con.execute(f"ATTACH DATABASE ? AS {alias}", (f"file:{path}?mode=ro{imm}",))


def verify(con: sqlite3.Connection, s) -> dict:
    """17 §10 items 1–4 (count checks; ``0`` in a setting disables that check).

    The counts were verified on the PC ledger. Without it the first writer (the Pi copy, a superset of the PC
    ledger) is checked over the PC span (``timestamp ≤ PC_MAX_TS``). Rows count as present when *any* source holds
    their ``(netuid, block_number)``: an earlier RPC era row at the same block wins INSERT OR IGNORE (first writer),
    so it is reported as ``held_by_other_source``, not as missing. With the seed files present the source is
    attached read-only for these checks; otherwise only the imported rows are counted.
    """
    out: dict = {}
    specs = discover_sources(s)
    primary = [x for x in specs if not x.merge_only]
    backfill = next((x for x in specs if x.merge_only), None)
    first = primary[0] if primary else None
    src_name = first.source if first else "ledger_pc"
    span = "" if src_name == "ledger_pc" else f" AND timestamp <= {PC_MAX_TS}"
    r = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT netuid) FROM pool_history WHERE source = ?{span}",
                    (src_name,)).fetchone()
    imported, imported_netuids = r[0], r[1]
    attached: list[str] = []
    try:
        if first is not None:
            _attach_ro(con, first.path, "v_first")
            attached.append("v_first")
            q = con.execute(
                "SELECT COUNT(*), COUNT(DISTINCT f.netuid), SUM(m.netuid IS NULL), "
                "SUM(m.netuid IS NOT NULL AND m.source <> ?) FROM v_first.pool_history f "
                "LEFT JOIN main.pool_history m ON m.netuid = f.netuid AND m.block_number = f.block_number "
                f"WHERE f.price > 0 AND f.tao_in_pool > 0{span.replace('timestamp', 'f.timestamp')}",
                (src_name,)).fetchone()
            src_rows, src_netuids, missing, held = q[0], q[1], q[2] or 0, q[3] or 0
            if s.LEDGER_EXPECTED_ROWS:
                out["ledger_rows"] = {"expected": s.LEDGER_EXPECTED_ROWS, "actual": src_rows, "source": src_name,
                                      "imported": imported, "held_by_other_source": held, "missing": missing,
                                      "pass": src_rows == s.LEDGER_EXPECTED_ROWS and missing == 0
                                      and imported + held == src_rows}
            if s.LEDGER_EXPECTED_NETUIDS:
                out["ledger_netuids"] = {"expected": s.LEDGER_EXPECTED_NETUIDS, "actual": src_netuids,
                                         "pass": src_netuids == s.LEDGER_EXPECTED_NETUIDS == imported_netuids}
        else:
            if s.LEDGER_EXPECTED_ROWS:
                out["ledger_rows"] = {"expected": s.LEDGER_EXPECTED_ROWS, "actual": imported, "source": src_name,
                                      "pass": imported == s.LEDGER_EXPECTED_ROWS}
            if s.LEDGER_EXPECTED_NETUIDS:
                out["ledger_netuids"] = {"expected": s.LEDGER_EXPECTED_NETUIDS, "actual": imported_netuids,
                                         "pass": imported_netuids == s.LEDGER_EXPECTED_NETUIDS}
        if s.LEDGER_BACKFILL_EXPECTED_MERGE:
            e = con.execute("SELECT COUNT(*) FROM pool_history WHERE enriched_at IS NOT NULL AND source LIKE 'ledger_%'"
                            ).fetchone()[0]
            chk = {"expected": s.LEDGER_BACKFILL_EXPECTED_MERGE, "actual": e, "pass": e == s.LEDGER_BACKFILL_EXPECTED_MERGE}
            if first is not None and backfill is not None:
                # merge targets = backfill rows that fill a NULL of the first writer's copy; those whose block an
                # earlier RPC row already holds (moving_price set) are counted as covered by that row
                _attach_ro(con, backfill.path, "v_bf")
                attached.append("v_bf")
                t = con.execute(
                    "SELECT COUNT(*), SUM(m.source NOT LIKE 'ledger_%') FROM v_bf.pool_history b "
                    "JOIN v_first.pool_history f ON f.netuid = b.netuid AND f.block_number = b.block_number "
                    "LEFT JOIN main.pool_history m ON m.netuid = b.netuid AND m.block_number = b.block_number "
                    "WHERE (f.moving_price IS NULL AND b.moving_price IS NOT NULL) "
                    "OR (f.subnet_volume IS NULL AND b.subnet_volume IS NOT NULL)").fetchone()
                targets, covered = t[0], t[1] or 0
                chk.update({"targets": targets, "covered_by_other_source": covered, "actual": e + covered,
                            "pass": targets == s.LEDGER_BACKFILL_EXPECTED_MERGE == e + covered})
            out["ledger_backfill_merge"] = chk
    finally:
        for a in attached:
            try:
                con.execute(f"DETACH DATABASE {a}")
            except sqlite3.OperationalError:
                pass
    g = con.execute("SELECT price, tao_in_pool, network_registered_at, occupant_key FROM pool_history "
                    "WHERE netuid = 64 AND block_number = 8443757 AND source LIKE 'ledger_%'").fetchone()
    if g is not None:
        out["golden_row"] = {"expected": "0.064653121|187212.134573233|4531295|64:4531295",
                             "actual": f"{g[0]}|{g[1]}|{g[2]}|{g[3]}",
                             "pass": abs(g[0] - 0.064653121) < 1e-12 and abs(g[1] - 187212.134573233) < 1e-6
                             and g[2] == 4531295 and g[3] == "64:4531295"}
    bad = con.execute("SELECT COUNT(*) FROM price_bars WHERE source = 'ledger_import' AND backfilled = 0 "
                      "AND ts_last < ?", (s.LEDGER_LIVE_FROM_TS,)).fetchone()[0]
    out["no_live_bar_before_live_from"] = {"expected": 0, "actual": bad, "pass": bad == 0}
    return out


def report_path() -> Path:
    return common.backfill_dir() / "ledger_import_report.json"


def run_import(con: sqlite3.Connection, *, online: bool, extra: list[Path], thin_sec: int, run_id: str) -> dict:
    """Phase ``import`` (after preflight): every source, then the NULL-merge, then bars for the imported range."""
    from app.backfill import bars
    s = common.settings()
    specs = discover_sources(s, extra)
    primary = [x for x in specs if not x.merge_only]
    if not primary:
        raise common.PreconditionError(f"seed file missing: {s.path(s.LEDGER_SEED_PATH)} (checklist A3)")
    sidecar = common.Sidecar(common.backfill_dir() / "ledger_import.json", {})
    tmp = common.backfill_dir() / "tmp"
    run_started = now_ts()
    pause = 0.05 if online else 0.0
    batch = s.LEDGER_IMPORT_BATCH_ROWS_ONLINE if online else None
    reports: dict[str, dict] = {}
    for sp in primary:
        path, imm = prepare_source(sp, tmp)
        r = import_source(con, sp, path, imm, trust_ts=s.LEDGER_NRA_TRUST_FROM_TS, live_from_ts=s.LEDGER_LIVE_FROM_TS,
                          run_started=run_started, thin_sec=thin_sec, sidecar=sidecar, pause_s=pause, run_id=run_id,
                          batch_rows=batch)
        reports.setdefault(sp.source, {}).update(asdict(r))
        logger.info("ledger source imported", data={k: v for k, v in asdict(r).items() if k != "path"})
        if r.status == "stopped":
            break
    # NULL-merge from every source except the first writer (merge sources + later copies; PC → Pi → extras)
    merges = [x for x in specs if x.merge_only] + primary[1:]
    for sp in merges:
        path, imm = prepare_source(sp, tmp)
        changed, mp, sv, nra = merge_nulls(con, sp, path, imm, trust_ts=s.LEDGER_NRA_TRUST_FROM_TS, now=now_ts())
        key = sp.source if sp.merge_only else f"{sp.source}_merge"
        reports.setdefault(key, {"source": sp.source, "path": str(sp.path)}).update(
            {"merged_rows": changed, "merged_mp": mp, "merged_sv": sv, "merged_nra": nra})
    lo = min((r.get("min_ts") for r in reports.values() if r.get("min_ts") is not None), default=None)
    hi = max((r.get("max_ts") for r in reports.values() if r.get("max_ts") is not None), default=None)
    out: dict = dict(reports)       # per-source keys at the top level (17 §10 item 2: jq '.ledger_backfill.merged_mp')
    out["run"] = {"run_id": run_id, "thin_sec": thin_sec, "online": online, "started_at": run_started}
    if lo is not None:
        out["bars"] = bars.rebuild(con, from_ts=lo, to_ts=hi + 1, live_from_ts=s.LEDGER_LIVE_FROM_TS, now=now_ts(),
                                   run_id=run_id)
    out["verify"] = verify(con, s)
    write_atomic(report_path(), out)
    return out
