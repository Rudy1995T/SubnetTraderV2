"""``data/quant/coverage.json`` — the ``coverage-1`` contract with spec 13 (17 §2.9, 02 §J2).

Pure read-only SQL (``mode=ro``). Every key is always present (``null`` when it cannot be computed); every time is
INTEGER unix seconds UTC. Written atomically; a build error leaves the previous file untouched.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from app.backfill import common
from app.util.fs import write_atomic
from app.util.time import now_ts, to_iso

SCHEMA_ID = "coverage-1"
HOLE_MIN_HOURS = 6
HOLE_SHARE = 0.5
# 17 §3.4: keys whose history this phase provides (the owners' replays turn it into metric_values).
RECONSTRUCTIBLE_KEYS = frozenset({
    "emission_share", "net_flow_30d", "liquidity_depth", "dereg_safety", "gate_margin", "miner_burn_ok", "tenure",
    "owner_commitment", "active_authors_28d", "fte_devs_84d", "commit_days_90d", "bus_factor",
    "release_cadence_90d", "review_practice_90d", "declared_repo_freshness", "org_freshness"})
ERA_BLOCKS = {"pre_ledger": (4920351, 6502593), "ledger": (6502594, 8443757)}
CHAIN_HOURLY_SOURCES = ("sse", "rest_table", "tick_rollup", "analytics_1w", "analytics_1m")
RPC_SOURCES = ("blockmachine", "onfinality", "opentensor_archive")
STEPS = ("import_old_ledger", "import_fundamentals", "backfill_rpc:gap", "backfill_rpc:ledger",
         "backfill_rpc:pre_ledger", "backfill_rpc:post", "backfill_tmc", "backfill_regime", "backfill_github")


def _one(con: sqlite3.Connection, sql: str, args: tuple = ()) -> Any:
    r = con.execute(sql, args).fetchone()
    return r[0] if r else None


def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    return bool(_one(con, "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?", (name,)))


def _month(ts: int) -> str:
    return (to_iso(ts) or "")[:7]


def _sha_cached(path: Path) -> str | None:
    """sha256 of a seed input, cached by (size, mtime) in ``data/backfill/sha_cache.json``."""
    if not path.exists():
        return None
    cache_p = common.backfill_dir() / "sha_cache.json"
    try:
        cache = json.loads(cache_p.read_text())
    except (OSError, ValueError):
        cache = {}
    st = path.stat()
    key = f"{path}|{st.st_size}|{int(st.st_mtime)}"
    if key not in cache:
        cache[key] = common.sha256_file(path)
        try:
            write_atomic(cache_p, cache)
        except OSError:
            pass
    return cache[key]


# ── pieces ───────────────────────────────────────────────────────────────────────────────────────────────────
def _eras(con: sqlite3.Connection, now: int, live_from: int | None) -> dict:
    from app.backfill.rpc_backfill import ledger_span, plan_buckets
    s = common.settings()
    lo, hi, imported = ledger_span(con)
    ledger_max_block = _one(con, "SELECT MAX(block_number) FROM pool_history WHERE source LIKE 'ledger_%'") \
        or ERA_BLOCKS["ledger"][1]
    out: dict[str, Any] = {}
    plan_now = min(now, live_from + 7200) if live_from else now
    for era in ("pre_ledger", "ledger", "gap"):
        rows = dict(con.execute("SELECT status, COUNT(*) FROM backfill_buckets WHERE era = ? GROUP BY status",
                                (era,)).fetchall())
        done, failed, skipped = rows.get("done", 0), rows.get("failed", 0), rows.get("skipped", 0)
        if not rows:
            status = "not_run"
        else:
            remaining = len(plan_buckets(con, era, step=3600, now=plan_now,
                                         min_netuids=s.BACKFILL_RPC_MIN_NETUIDS_PER_HOUR))
            status = "done" if remaining == 0 and failed == 0 else "partial"
        if era == "gap":
            fb = int(ledger_max_block) + 1
            tb = _one(con, "SELECT MAX(block_number) FROM backfill_buckets WHERE era = 'gap' AND status = 'done'")
        else:
            fb, tb = ERA_BLOCKS[era]
            if era == "ledger":
                tb = int(ledger_max_block)
        out[era] = {"from_block": fb, "to_block": tb, "status": status, "buckets_done": done,
                    "buckets_failed": failed, "buckets_skipped": skipped}
    out["live"] = {"from_ts": live_from}
    return out


def _by_source(con: sqlite3.Connection, table: str, where: str = "1") -> dict:
    return {str(src): n for src, n in con.execute(f"SELECT source, COUNT(*) FROM {table} WHERE {where} GROUP BY source")}


def _intervals(con: sqlite3.Connection, now: int) -> dict[int, list[tuple[int, int]]]:
    from app.backfill.rpc_backfill import DTAO_LAUNCH_TS
    out: dict[int, list[tuple[int, int]]] = {}
    for n, rts, est, dts, status in con.execute(
            "SELECT netuid, registered_ts, registered_at_est, deregistered_ts, status FROM occupants WHERE netuid <> 0"):
        start = rts if rts is not None else est
        if start is None:
            continue
        end = dts if dts is not None else (now if status in ("active", "pre_launch") else None)
        if end is None:
            continue
        a, b = max(int(start), DTAO_LAUNCH_TS), min(int(end), now)
        if b > a:
            out.setdefault(int(n), []).append((a, b))
    return out


def _expected_per_bucket(intervals: dict[int, list[tuple[int, int]]], period: int, lo: int, hi: int) -> dict[int, int]:
    """Number of netuids expected in each bucket start ts ∈ [lo, hi) (sweep over interval edges)."""
    delta: dict[int, int] = {}
    for ivs in intervals.values():
        for a, b in ivs:
            s0 = max(lo, -(-a // period) * period)        # first bucket fully after registration
            e0 = min(hi, (b // period) * period)
            if e0 <= s0:
                continue
            delta[s0] = delta.get(s0, 0) + 1
            delta[e0] = delta.get(e0, 0) - 1
    out: dict[int, int] = {}
    cur = 0
    keys = sorted(delta)
    ki = 0
    t = lo
    while t < hi:
        while ki < len(keys) and keys[ki] <= t:
            cur += delta[keys[ki]]
            ki += 1
        out[t] = cur
        t += period
    return out


def _bars_series(con: sqlite3.Connection, period: str, intervals: dict, now: int) -> tuple[dict, list[dict]]:
    sec = {"1h": 3600, "1d": 86400}[period]
    rows = _one(con, "SELECT COUNT(*) FROM price_bars WHERE period = ?", (period,)) or 0
    out = {"rows": rows, "by_source": _by_source(con, "price_bars", f"period = '{period}'"),
           "completeness_by_month": {}, "with_reserves_share": None}
    holes: list[dict] = []
    if not rows:
        return out, holes
    out["with_reserves_share"] = round(_one(con, "SELECT AVG(tao_in_pool IS NOT NULL) FROM price_bars WHERE period=?",
                                            (period,)), 4)
    lo = _one(con, "SELECT MIN(ts) FROM price_bars WHERE period = ? AND netuid <> 0", (period,))
    hi = min(now, (_one(con, "SELECT MAX(ts) FROM price_bars WHERE period = ?", (period,)) or lo) + sec)
    covered = dict(con.execute("SELECT ts, COUNT(DISTINCT netuid) FROM price_bars WHERE period = ? AND netuid <> 0 "
                               "AND ts >= ? AND ts < ? GROUP BY ts", (period, lo, hi)).fetchall())
    exp = _expected_per_bucket(intervals, sec, lo, hi)
    by_m: dict[str, list[int]] = {}
    for t, e in exp.items():
        m = by_m.setdefault(_month(t), [0, 0])
        m[0] += min(covered.get(t, 0), e) if e else 0
        m[1] += e
    out["completeness_by_month"] = {k: round(c / e, 4) for k, (c, e) in sorted(by_m.items()) if e}
    if period == "1h":
        run_start = None
        for t in sorted(exp):
            e = exp[t]
            bad = e > 0 and covered.get(t, 0) < HOLE_SHARE * e
            if bad and run_start is None:
                run_start = t
            if (not bad or t == max(exp)) and run_start is not None:
                end = t if not bad else t + sec
                if (end - run_start) // sec >= HOLE_MIN_HOURS:
                    holes.append({"series": "price_bars_1h", "from_ts": run_start, "to_ts": end, "netuids": "all",
                                  "reason": "fewer than 50% of expected netuids have a 1h bar"})
                run_start = None
    return out, holes


def _reconstructible_from(con: sqlite3.Connection) -> dict:
    daily = _table_exists(con, "chain_daily")
    em = _one(con, "SELECT MIN(day_ts) FROM chain_daily WHERE emission_percent IS NOT NULL") if daily else None
    nf = None
    if daily:
        r = con.execute("SELECT day_ts FROM (SELECT day_ts, COUNT(*) OVER (PARTITION BY netuid ORDER BY day_ts "
                        "ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) c FROM chain_daily WHERE total_tao_buy_24h IS NOT "
                        "NULL) WHERE c >= 24 ORDER BY day_ts LIMIT 1").fetchone()
        nf = r[0] if r else None
    oc = _one(con, "SELECT MIN(day_ts) FROM chain_daily WHERE owner_personal_locked_alpha > 0") if daily else None
    gh = _one(con, "SELECT MIN(event_at) FROM github_commits") if _table_exists(con, "github_commits") else None
    s = common.settings()
    rg = _one(con, "SELECT MIN(day_ts) FROM regime_daily WHERE label <> 'UNKNOWN' AND spec_version = ?",
              (s.SPEC_VERSION,))
    return {"price_depth_moving_price": _one(con, "SELECT MIN(timestamp) FROM pool_history WHERE moving_price IS NOT "
                                                  "NULL AND tao_in_pool IS NOT NULL"),
            "emission_share": em, "net_flow_30d": nf, "owner_commitment": oc, "control_stability_90d": None,
            "tenure": _one(con, "SELECT MIN(registered_ts) FROM occupants"), "github_p2": gh, "regime": rg}


def _forward_only() -> list[str]:
    from app.storage.catalog import METRIC_KEYS
    return sorted(k for k, m in METRIC_KEYS.items() if m.role == "scored" and k not in RECONSTRUCTIBLE_KEYS)


def volume_unit_ratio(con: sqlite3.Connection) -> float | None:
    """Median TMC 5m ``start_volume`` ÷ chain ``subnet_volume`` for SN64 (17 §10 item 13); ≈ 1.0 ⇒ both are τ."""
    rows = [r[0] for r in con.execute(
        "SELECT pb.start_volume / ph.subnet_volume FROM price_bars pb JOIN pool_history ph ON ph.netuid = 64 "
        "AND ph.source IN ('blockmachine','onfinality','opentensor_archive') AND ph.timestamp BETWEEN pb.ts AND pb.ts + 299 "
        "WHERE pb.netuid = 64 AND pb.period = '5m' AND pb.source = 'tmc_candle' AND ph.subnet_volume > 0 "
        "AND pb.start_volume IS NOT NULL LIMIT 200")]
    if len(rows) < 10:
        return None
    rows.sort()
    return rows[len(rows) // 2]


def _caveats(con: sqlite3.Connection) -> list[str]:
    s = common.settings()
    out: set[str] = set()
    vr = volume_unit_ratio(con)
    if vr is None or abs(vr - 1.0) > 1e-3:
        out.add("subnet_volume_unit_unverified")
    methods = {r[0] for r in con.execute("SELECT DISTINCT price_method FROM pool_history WHERE price_method IS NOT NULL")}
    has_ledger = bool(_one(con, "SELECT 1 FROM pool_history WHERE source LIKE 'ledger_%' LIMIT 1"))
    if len(methods) > 1 or (has_ledger and methods):
        out.add("price_method_mixed")
    if has_ledger:
        out.update({"seed_era_daily_only_before_2026-04-22", "seed_era_taostats_price"})
        if _one(con, "SELECT 1 FROM pool_history WHERE source LIKE 'ledger_%' AND timestamp < ? LIMIT 1",
                (s.LEDGER_LIVE_FROM_TS,)):
            out.add("ledger_observed_later_before_1777468595")
    if _one(con, "SELECT 1 FROM pool_history WHERE enriched_at IS NOT NULL LIMIT 1"):
        out.add("moving_price_enriched_rows")
    if _table_exists(con, "github_commits") and _one(con, "SELECT 1 FROM github_commits WHERE backfilled = 1 LIMIT 1"):
        out.add("github_org_membership_asof_today")
    try:
        lab = json.loads((common.backfill_dir() / "tmc_progress.json").read_text()).get("candle_label")
        if lab in ("start", "end", "unverified"):
            out.add(f"tmc_candle_label:{lab}")
    except (OSError, ValueError):
        pass
    return sorted(out)


def _checks(con: sqlite3.Connection) -> dict:
    from app.backfill import ledger_import, rpc_backfill
    s = common.settings()
    out: dict[str, Any] = {}
    if _one(con, "SELECT 1 FROM pool_history WHERE source LIKE 'ledger_%' LIMIT 1"):
        for k, v in ledger_import.verify(con, s).items():
            out[k] = v
    vr = volume_unit_ratio(con)
    if vr is not None:
        out["subnet_volume_unit"] = {"expected": 1.0, "actual": round(vr, 6), "pass": abs(vr - 1.0) <= 1e-3}
    p = rpc_backfill.load_probe()
    out["rpc_golden_block"] = ("pass" if p.get("pass") else "fail") if p else "not_run"
    out["occupant_key_mismatch_rows"] = _one(
        con, "SELECT COUNT(*) FROM pool_history WHERE network_registered_at IS NOT NULL AND occupant_key IS NOT NULL "
             "AND occupant_key <> netuid || ':' || network_registered_at") or 0
    return out


def _provenance(con: sqlite3.Connection) -> list[dict]:
    s = common.settings()
    out = []
    for step in STEPS:
        r = con.execute("SELECT run_id, finished_at, rows FROM collector_runs WHERE job = ? AND ok = 1 "
                        "ORDER BY started_at DESC LIMIT 1", (step,)).fetchone()
        if not r:
            continue
        inputs: dict[str, str | None] = {}
        if step == "import_old_ledger":
            for key in ("LEDGER_SEED_PATH", "LEDGER_BACKFILL_SEED_PATH", "LEDGER_PI_SEED_PATH"):
                pth = s.path(getattr(s, key))
                if pth.exists():
                    inputs[str(getattr(s, key))] = _sha_cached(pth)
        elif step == "import_fundamentals":
            for key in ("LEGACY_FUNDAMENTALS_PATH", "WHALE_COHORT_SEED_PATH"):
                pth = s.path(getattr(s, key))
                if pth.exists():
                    inputs[str(getattr(s, key))] = _sha_cached(pth)
        out.append({"step": step, "run_id": r[0], "finished_at": r[1], "rows": r[2], "inputs_sha256": inputs})
    return out


def _occupancy(con: sqlite3.Connection) -> dict:
    try:
        anomalies = json.loads((common.backfill_dir() / "occupancy_anomalies.json").read_text()).get("anomalies", [])
    except (OSError, ValueError):
        anomalies = []
    reocc = [{"netuid": n, "old": o, "new": nw, "at_ts": t} for n, o, nw, t in con.execute(
        "SELECT o.netuid, o.occupant_key, o.successor_key, o.deregistered_ts FROM occupants o "
        "WHERE o.successor_key IS NOT NULL ORDER BY o.netuid, o.network_registered_at")]
    return {"occupants": _one(con, "SELECT COUNT(*) FROM occupants") or 0,
            "historical": _one(con, "SELECT COUNT(*) FROM occupants WHERE status = 'deregistered' AND source IN "
                                    "('rpc','onfinality','legacy_ledger')") or 0,
            "anomalies": anomalies, "reoccupations": reocc,
            "unstamped": {"pool_history": _one(con, "SELECT COUNT(*) FROM pool_history WHERE occupant_key IS NULL"),
                          "price_bars_1h": _one(con, "SELECT COUNT(*) FROM price_bars WHERE period = '1h' AND "
                                                     "occupant_key IS NULL")}}


# ── build ────────────────────────────────────────────────────────────────────────────────────────────────────
def build(db_path: str | Path | None = None, *, settings=None) -> dict:
    s = settings or common.settings()
    p = Path(db_path) if db_path is not None else s.path(s.DB_PATH)
    con = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    try:
        now = now_ts()
        live_from = _one(con, "SELECT MIN(hour_ts) FROM chain_hourly WHERE source IN ('sse','rest_table')")
        intervals = _intervals(con, now)
        ph = {"rows": _one(con, "SELECT COUNT(*) FROM pool_history") or 0,
              "netuids": _one(con, "SELECT COUNT(DISTINCT netuid) FROM pool_history") or 0,
              "min_ts": _one(con, "SELECT MIN(timestamp) FROM pool_history"),
              "max_ts": _one(con, "SELECT MAX(timestamp) FROM pool_history"),
              "by_source": {str(src): {"rows": n, "min_ts": a, "max_ts": b} for src, n, a, b in con.execute(
                  "SELECT source, COUNT(*), MIN(timestamp), MAX(timestamp) FROM pool_history GROUP BY source")},
              "moving_price_nonnull_share": None, "occupant_key_null_rows": _one(
                  con, "SELECT COUNT(*) FROM pool_history WHERE occupant_key IS NULL") or 0,
              "price_method": {"swap_sqrt": 0, "reserve_ratio": 0}}
        if ph["rows"]:
            ph["moving_price_nonnull_share"] = round(_one(con, "SELECT AVG(moving_price IS NOT NULL) FROM pool_history"),
                                                     4)
            for m, n in con.execute("SELECT price_method, COUNT(*) FROM pool_history WHERE price_method IS NOT NULL "
                                    "GROUP BY 1"):
                ph["price_method"][str(m)] = n
        b1h, holes = _bars_series(con, "1h", intervals, now)
        b1d, _ = _bars_series(con, "1d", intervals, now)
        try:
            tmc = json.loads((common.backfill_dir() / "tmc_progress.json").read_text()).get("tasks", {})
        except (OSError, ValueError):
            tmc = {}
        unattributed = sum(int(u.get("unattributed") or 0) for u in tmc.get("analytics1y", {}).values())
        rd = con.execute("SELECT COUNT(*), SUM(label = 'UNKNOWN'), MIN(day_ts), MAX(day_ts) FROM regime_daily "
                         "WHERE spec_version = ?", (s.SPEC_VERSION,)).fetchone()
        gh_exists = _table_exists(con, "github_commits")
        try:
            gp = json.loads((common.backfill_dir() / "github_progress.json").read_text())
            truncated = sum(1 for v in gp.values() if isinstance(v, dict) and v.get("truncated"))
        except (OSError, ValueError):
            truncated = 0
        doc = {
            "schema": SCHEMA_ID, "generated_at": now, "spec_version": s.SPEC_VERSION,
            "db_schema_version": _one(con, "SELECT MAX(version) FROM schema_version"),
            "eras": _eras(con, now, live_from),
            "series": {
                "pool_history": ph,
                "price_bars_1h": b1h,
                "price_bars_1d": b1d,
                "price_bars_5m": {"rows": _one(con, "SELECT COUNT(*) FROM price_bars WHERE period = '5m'") or 0,
                                  "by_source": _by_source(con, "price_bars", "period = '5m'")},
                "chain_daily": {"rows": _one(con, "SELECT COUNT(*) FROM chain_daily") or 0,
                                "min_day": _one(con, "SELECT MIN(day_ts) FROM chain_daily"),
                                "max_day": _one(con, "SELECT MAX(day_ts) FROM chain_daily"),
                                "unattributed_rows": unattributed},
                "chain_hourly": {"rows_by_source": {k: _one(con, "SELECT COUNT(*) FROM chain_hourly WHERE source = ?",
                                                            (k,)) or 0 for k in CHAIN_HOURLY_SOURCES}},
                "block_times": {"rows": _one(con, "SELECT COUNT(*) FROM block_times") or 0,
                                "min_block": _one(con, "SELECT MIN(block_number) FROM block_times"),
                                "max_block": _one(con, "SELECT MAX(block_number) FROM block_times")},
                "regime_daily": {"days": rd[0] or 0, "unknown_days": rd[1] or 0, "from_day": rd[2], "to_day": rd[3]},
                "github_commits": {
                    "repos": (_one(con, "SELECT COUNT(DISTINCT repo_id) FROM github_commits") or 0) if gh_exists else 0,
                    "repos_truncated": truncated,
                    "min_event_at": _one(con, "SELECT MIN(event_at) FROM github_commits") if gh_exists else None,
                    "backfilled_rows": (_one(con, "SELECT COUNT(*) FROM github_commits WHERE backfilled = 1") or 0)
                    if gh_exists else 0},
            },
            "occupancy": _occupancy(con),
            "holes": holes,
            "reconstructible_from": _reconstructible_from(con),
            "forward_only": _forward_only(),
            "caveats": _caveats(con),
            "checks": _checks(con),
            "provenance": _provenance(con),
        }
        return doc
    finally:
        con.close()


def write(doc: dict, path: Path | None = None) -> Path:
    s = common.settings()
    p = path or s.path(s.COVERAGE_PATH)
    write_atomic(p, doc)
    return p

