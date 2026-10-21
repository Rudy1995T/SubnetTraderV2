"""Reduced read-only snapshots and DB resolution (spec 13 §2, §3.4, §5 ``QUANT_DB_PATH``, §7, §8.1).

The harness never reads the live writer (02 §J2): ``resolve_db`` refuses ``DB_PATH`` (samefile), a file with a
non-empty ``-wal`` sibling, ``*.tmp`` and ``*.gz``. ``make_snapshot`` copies the 11 relations the harness reads from
the newest uncompressed backup (16) into ``data/quant/snapshots/quant_<source-stamp>.db`` — the ``v_quant_*`` views
materialised as tables of the same names, so the harness SQL is identical on a backup and on a snapshot.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
from pathlib import Path

import app.config as app_config
from app.util.time import now_ts

from .backtestability import SCORED_KEYS

SNAPSHOT_PREFIX = "quant_"
RELATIONS = ("v_quant_price_bars", "v_quant_chain_hourly", "v_quant_chain_daily", "v_quant_occupants",
             "metric_values", "scores", "regime_daily", "events", "block_times", "schema_version",
             "quant_snapshot_meta")
_CHUNK = 1 << 20


class RefusedError(Exception):
    """A DB the harness must not read (CLI exit 2)."""


def _stamp_of(name: str) -> str | None:
    """``taosentinel-<YYYYMMDDTHHMMSSZ>[-tag].<ext>`` → stamp (16's naming)."""
    if not name.startswith("taosentinel-"):
        return None
    st = name[len("taosentinel-"):][:16]
    return st if len(st) == 16 and st[8] == "T" and st.endswith("Z") else None


def _manifest_ok(db: Path) -> bool:
    m = db.with_suffix(".json")
    if not m.is_file():
        return True
    try:
        return json.loads(m.read_text()).get("quick_check", "ok") == "ok"
    except (OSError, ValueError):
        return False


def newest_backup(directory: str | Path | None = None) -> Path | None:
    """Newest uncompressed ``taosentinel-<stamp>[-tag].db`` in the backup dir (not ``manual/``) whose manifest is ok."""
    d = Path(directory) if directory else app_config.settings.path(app_config.settings.DB_BACKUP_DIR)
    if not d.is_dir():
        return None
    cands = [(st, p) for p in d.glob("taosentinel-*.db") if p.is_file() and (st := _stamp_of(p.name))]
    for _, p in sorted(cands, reverse=True):
        if _manifest_ok(p):
            return p
    return None


def newest_snapshot(directory: str | Path | None = None) -> Path | None:
    d = Path(directory) if directory else app_config.settings.path(app_config.settings.QUANT_SNAPSHOT_DIR)
    snaps = sorted(p for p in d.glob(f"{SNAPSHOT_PREFIX}*.db") if p.is_file()) if d.is_dir() else []
    return snaps[-1] if snaps else None


def check_readable(path: str | Path) -> Path:
    """Refusals of 13 §7 for an explicit path; returns the resolved path."""
    p = Path(path)
    name = p.name
    if name.endswith(".tmp"):
        raise RefusedError(f"backup not finished: {p}")
    if name.endswith(".gz"):
        raise RefusedError(f"decompress first or pick the uncompressed newest backup: {p}")
    if not p.is_file():
        raise RefusedError(f"no such database: {p}")
    live = app_config.settings.path(app_config.settings.DB_PATH)
    try:
        same = live.exists() and os.path.samefile(p, live)
    except OSError:
        same = False
    wal = p.with_name(p.name + "-wal")
    if same or (wal.exists() and wal.stat().st_size > 0):
        raise RefusedError("refusing to read the live writer DB; run 'python -m app.quant snapshot' first")
    return p


def resolve_db(db_arg: str | None) -> str:
    """``--db`` → ``QUANT_DB_PATH`` → newest snapshot → newest uncompressed backup (13 §5)."""
    s = app_config.settings
    explicit = db_arg or s.QUANT_DB_PATH
    if explicit:
        return str(check_readable(s.path(explicit)))
    snap = newest_snapshot()
    if snap is not None:
        return str(check_readable(snap))
    bk = newest_backup()
    if bk is not None:
        return str(check_readable(bk))
    raise RefusedError(f"no snapshot or backup found (searched {s.path(s.QUANT_SNAPSHOT_DIR)}/{SNAPSHOT_PREFIX}*.db "
                       f"and {s.path(s.DB_BACKUP_DIR)}/taosentinel-<stamp>.db)")


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while b := f.read(_CHUNK):
            h.update(b)
    return h.hexdigest()


def db_fingerprint(path: str | Path) -> str:
    """sha256 of the DB file; a snapshot's ``.sha256`` sidecar (written at creation) is reused while still newer,
    a backup's manifest hash while its size matches."""
    p = Path(path)
    side = p.with_name(p.name + ".sha256")
    try:
        if side.is_file() and side.stat().st_mtime >= p.stat().st_mtime:
            v = side.read_text().strip()
            if len(v) == 64:
                return v
    except OSError:
        pass
    return _source_sha(p)                 # a backup: 16's manifest hash when it describes this exact file


def git_sha() -> str:
    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=str(app_config.APP_ROOT),
                             capture_output=True, text=True, timeout=10)
        sha = out.stdout.strip() if out.returncode == 0 else ""
        return sha or "unknown"
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def _source_sha(src: Path) -> str:
    m = src.with_suffix(".json")
    try:
        d = json.loads(m.read_text())
        if d.get("sha256") and int(d.get("size_bytes", -1)) == src.stat().st_size:
            return str(d["sha256"])          # 16's manifest hash of this exact file
    except (OSError, ValueError, TypeError):
        pass
    return sha256_file(src)


def make_snapshot(src: str | Path, out: str | Path | None = None, *, coverage_path: str | Path | None = None) -> Path:
    """Write the reduced snapshot (+ coverage copy, sha256 sidecar); ``<out>.tmp`` → quick_check → atomic rename."""
    s = app_config.settings
    src = check_readable(src)
    stem = src.stem                                      # taosentinel-<stamp>[-tag]
    out = Path(out) if out else s.path(s.QUANT_SNAPSHOT_DIR) / f"{SNAPSHOT_PREFIX}{stem}.db"
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp")
    if tmp.exists():
        tmp.unlink()
    cov_src = Path(coverage_path) if coverage_path else s.path(s.COVERAGE_PATH)
    cov_dst = out.with_name(out.stem + ".coverage.json")
    cov_sha = None
    con = sqlite3.connect(str(tmp), uri=True)        # uri=True so the read-only ATTACH URI is honoured
    try:
        con.execute("PRAGMA journal_mode=OFF")
        con.execute("PRAGMA synchronous=OFF")
        con.execute("ATTACH DATABASE ? AS src", (f"file:{src}?mode=ro",))
        scored = ",".join(f"'{k}'" for k in sorted(SCORED_KEYS))
        stmts = [
            "CREATE TABLE v_quant_price_bars AS SELECT * FROM src.v_quant_price_bars WHERE period = '1h'",
            "CREATE TABLE v_quant_chain_hourly AS SELECT * FROM src.v_quant_chain_hourly",
            "CREATE TABLE v_quant_chain_daily AS SELECT * FROM src.v_quant_chain_daily",
            "CREATE TABLE v_quant_occupants AS SELECT * FROM src.v_quant_occupants",
            "CREATE TABLE metric_values AS SELECT occupant_key, metric_key, as_of, spec_version, stage, value, "
            "value_json, status, observed_at, source, run_id, backfilled FROM src.metric_values WHERE stage = 'norm' "
            f"OR (stage = 'raw' AND metric_key NOT IN ({scored}))",
            "CREATE TABLE scores AS SELECT * FROM src.scores",
            "CREATE TABLE regime_daily AS SELECT * FROM src.regime_daily",
            "CREATE TABLE events AS SELECT * FROM src.events "
            "WHERE event_type IN ('netuid_reoccupied','emission_disabled')",
            "CREATE TABLE block_times AS SELECT block_number, ts_ms, event_at, endpoint, observed_at, source, run_id "
            "FROM src.block_times",
            "CREATE TABLE schema_version AS SELECT * FROM src.schema_version",
            "CREATE INDEX ix_pb ON v_quant_price_bars(period, netuid, bucket_start)",
            "CREATE INDEX ix_ch ON v_quant_chain_hourly(netuid, hour)",
            "CREATE INDEX ix_cd ON v_quant_chain_daily(occupant_key, day)",
            "CREATE INDEX ix_mv ON metric_values(stage, spec_version, metric_key, as_of)",
            "CREATE INDEX ix_sc ON scores(spec_version, as_of)",
            "CREATE INDEX ix_ev ON events(event_type)",
        ]
        for q in stmts:
            con.execute(q)
        try:
            cls_ver = con.execute("SELECT MAX(classifier_version) FROM src.post_labels").fetchone()[0]
        except sqlite3.Error:
            cls_ver = None
        max_obs = max((con.execute(f"SELECT MAX(observed_at) FROM {t}").fetchone()[0] or 0)
                      for t in ("v_quant_price_bars", "v_quant_chain_daily", "metric_values"))
        con.commit()
        con.execute("DETACH DATABASE src")
        if cov_src.is_file():
            shutil.copyfile(cov_src, cov_dst)
            cov_sha = sha256_file(cov_dst)
        meta = {"source_path": str(src), "source_sha256": _source_sha(src), "source_stamp": stem,
                "made_at": str(now_ts()), "max_classifier_version": str(cls_ver) if cls_ver is not None else "none",
                "coverage_sha256": cov_sha or "", "max_observed_at": str(int(max_obs))}
        con.execute("CREATE TABLE quant_snapshot_meta (key TEXT PRIMARY KEY, value TEXT)")
        con.executemany("INSERT INTO quant_snapshot_meta VALUES (?,?)", sorted(meta.items()))
        con.commit()
        ok = con.execute("PRAGMA quick_check").fetchone()[0]
        if ok != "ok":
            raise RuntimeError(f"snapshot quick_check failed: {ok}")
    except BaseException:
        con.close()
        tmp.unlink(missing_ok=True)
        raise
    con.close()
    os.replace(tmp, out)
    out.with_name(out.name + ".sha256").write_text(sha256_file(out) + "\n")
    return out


def prune(keep: int | None = None, directory: str | Path | None = None) -> list[Path]:
    """Delete all but the newest ``keep`` snapshots (+ coverage copies and sidecars)."""
    s = app_config.settings
    keep = max(1, int(keep if keep is not None else s.QUANT_SNAPSHOT_KEEP))
    d = Path(directory) if directory else s.path(s.QUANT_SNAPSHOT_DIR)
    snaps = sorted(p for p in d.glob(f"{SNAPSHOT_PREFIX}*.db") if p.is_file()) if d.is_dir() else []
    removed = []
    for p in snaps[:-keep]:
        for f in (p, p.with_name(p.stem + ".coverage.json"), p.with_name(p.name + ".sha256")):
            if f.exists():
                f.unlink()
                removed.append(f)
    return removed


def snapshot_meta(con: sqlite3.Connection) -> dict[str, str]:
    try:
        return {k: v for k, v in con.execute("SELECT key, value FROM quant_snapshot_meta")}
    except sqlite3.Error:
        return {}


def missing_relations(con: sqlite3.Connection) -> list[str]:
    """Required relation/columns of 13 §3.1 absent from a DB (exit 2 lists them)."""
    need = {
        "v_quant_price_bars": ("netuid", "occupant_key", "period", "bucket_start", "ts_last", "close", "volume",
                               "tao_in_pool", "moving_price", "source", "observed_at", "backfilled"),
        "v_quant_chain_hourly": ("netuid", "hour", "tao_in_pool", "moving_price"),
        "v_quant_chain_daily": ("occupant_key", "netuid", "day", "ts", "block_number", "emission_share",
                                "tao_in_pool", "observed_at"),
        "v_quant_occupants": ("occupant_key", "netuid", "network_registered_at", "registered_ts", "first_seen_at",
                              "deregistered_at", "deregistered_ts"),
        "metric_values": ("occupant_key", "metric_key", "as_of", "spec_version", "stage", "value", "value_json",
                          "status", "observed_at", "backfilled"),
        "scores": ("occupant_key", "as_of", "spec_version", "run_kind", "composite_raw", "composite",
                   "composite_smoothed", "quality", "velocity", "pillar_scores", "confidence", "computed_at",
                   "input_cutoff", "backfilled", "shadow"),
        "regime_daily": ("day_ts", "spec_version", "label", "source"),
        "events": ("occupant_key", "event_type", "event_at", "observed_at", "payload"),
        "block_times": ("block_number", "event_at"),
    }
    missing = []
    for rel, cols in need.items():
        have = {r[1] for r in con.execute(f"PRAGMA table_xinfo({rel})")}
        if not have:
            missing.append(rel)
            continue
        missing += [f"{rel}.{c}" for c in cols if c not in have]
    return missing
