"""Panel I/O over 03's ``v_quant_*`` views (spec 13 §2.1–§2.3, §8.2) — replaces the old ``load_factor_panel``.

Read-only (``mode=ro``) against a snapshot or uncompressed backup, never the live writer (``snapshot.resolve_db``).
"""
from __future__ import annotations

import bisect
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import app.config as app_config
from app.util.time import parse_iso

from .backtestability import CHAIN_STATE_FACTORS, Coverage, freshness_for, load_coverage
from .factor_panel import RESET_DROP_RATIO, Panel, _ro_conn, block_to_unix, build_panel
from .occupants import OccupantIndex, load_block_anchors
from .pit import PITStamper
from .regimes import load_market_regimes, parse_boundaries, regime_of_dates

DAY = 86400
DELTA_VOLUME_SOURCES = ("ledger_import", "blockmachine", "onfinality", "opentensor_archive")
CHAIN_SHARE_MAX_AGE = 3 * DAY
HI_INF = 1 << 62


@dataclass(frozen=True)
class PanelRequest:
    scope: str
    start: int | None = None
    end: int | None = None
    grid_seconds: int | None = None
    pit_keys: tuple[str, ...] = ()
    pit_mode: str = "strict"
    spec_version: str = "ts-1.0"
    reset_ratio: float = RESET_DROP_RATIO


def scope_defaults(scope: str) -> tuple[int, int]:
    """(grid_seconds, default start) of a scope (13 §2.2)."""
    s = app_config.settings
    if scope == "survivorship":
        return int(s.QUANT_GRID_SURVIVORSHIP_S), parse_iso(f"{s.QUANT_SURVIVORSHIP_START}T00:00:00Z")
    if scope == "live":
        return int(s.QUANT_GRID_LIVE_S), parse_iso(f"{s.QUANT_LIVE_ERA_START}T00:00:00Z")
    raise ValueError(f"unknown scope {scope!r} (expected 'live' or 'survivorship')")


def coverage_path_for(db_path: str) -> Path:
    """A snapshot's own coverage copy (``quant_<stamp>.coverage.json``) when present, else ``COVERAGE_PATH``."""
    p = Path(db_path)
    sib = p.with_name(p.stem + ".coverage.json")
    if sib.is_file():
        return sib
    return app_config.settings.path(app_config.settings.COVERAGE_PATH)


def _chain_share_index(con, lo: int, hi: int) -> dict[str, tuple[list[int], list[float]]]:
    out: dict[str, tuple[list[int], list[float]]] = {}
    for occ, day, share in con.execute(
            "SELECT occupant_key, day, emission_share FROM v_quant_chain_daily WHERE emission_share IS NOT NULL "
            "AND day BETWEEN ? AND ? ORDER BY occupant_key, day", (lo, hi)):
        days, vals = out.setdefault(occ, ([], []))
        days.append(int(day))
        vals.append(float(share))
    return out


def chain_share_at(index, occ: str | None, bucket: int) -> float | None:
    """``chain:emission_share``: the previous day's (``day ≤ bucket − 1 d``) share, at most 3 d old (13 §2.3)."""
    if not occ or occ not in index:
        return None
    days, vals = index[occ]
    j = bisect.bisect_right(days, bucket - DAY) - 1
    if j < 0 or bucket - days[j] > CHAIN_SHARE_MAX_AGE:
        return None
    return vals[j]


_AUTO = object()


def load_factor_panel(req: PanelRequest, *, db_path: str, freshness: Mapping[str, int] | None = None,
                      coverage: "Coverage | None | object" = _AUTO, warnings: list[str] | None = None) -> Panel:
    """Load the PIT panel (13 §8.2). ``coverage`` defaults to the snapshot's copy / ``COVERAGE_PATH``; pass a loaded
    :class:`Coverage` (or None) to reuse one. Warning codes accumulate in ``panel.meta['warnings']``."""
    s = app_config.settings
    grid0, lo0 = scope_defaults(req.scope)
    grid = int(req.grid_seconds or grid0)
    lo = int(req.start if req.start is not None else lo0)
    hi = int(req.end if req.end is not None else HI_INF)
    warnings = list(warnings or [])
    if coverage is _AUTO:
        coverage = load_coverage(coverage_path_for(db_path), warnings)
    if coverage is not None and req.pit_mode == "reconstructed" and not coverage.golden_ok:
        warnings.append("rpc_golden_block_failed")
    con = _ro_conn(db_path)
    try:
        dv = ",".join(f"'{x}'" for x in DELTA_VOLUME_SOURCES)
        cur = con.execute(
            "SELECT netuid, (bucket_start / :grid) * :grid AS bucket, close, tao_in_pool, moving_price, occupant_key, "
            "source, backfilled, MAX(COALESCE(ts_last, bucket_start + 3600)) AS ts, SUM(volume) AS vol_sum, "
            f"COUNT(*) AS n_rows, SUM(source IN ({dv}) AND volume IS NOT NULL) AS n_vol "
            "FROM v_quant_price_bars WHERE period = '1h' AND netuid != 0 AND bucket_start >= :lo "
            "AND bucket_start < :hi GROUP BY netuid, bucket ORDER BY netuid, bucket",
            {"grid": grid, "lo": lo, "hi": hi})
        raw = [tuple(r) for r in cur]
        # depth / moving-price fallback (v_quant_chain_hourly last write in the bucket, then chain_daily on 24 h)
        need = any(r[3] is None or r[4] is None for r in raw)
        hourly: dict[tuple[int, int], tuple[float | None, float | None, int]] = {}
        if need:
            for nu, b, depth, mp, hr in con.execute(
                    "SELECT netuid, (hour / :grid) * :grid AS b, tao_in_pool, moving_price, MAX(hour) FROM "
                    "v_quant_chain_hourly WHERE hour >= :lo AND hour < :hi AND tao_in_pool IS NOT NULL "
                    "GROUP BY netuid, b", {"grid": grid, "lo": lo, "hi": hi}):
                hourly[(int(nu), int(b))] = (depth, mp, int(hr))
        daily_depth: dict[tuple[int, int], float] = {}
        if need and grid == DAY:
            for nu, day, depth in con.execute(
                    "SELECT netuid, day, tao_in_pool FROM v_quant_chain_daily WHERE tao_in_pool IS NOT NULL AND "
                    "day >= ? AND day < ?", (lo, hi)):
                daily_depth[(int(nu), int(day))] = float(depth)
        occ_index = OccupantIndex.load(con)
        anchors = load_block_anchors(con)
        stamper = None
        pit_keys = tuple(k for k in req.pit_keys if k not in CHAIN_STATE_FACTORS)
        if pit_keys:
            fresh = {k: (freshness or {}).get(k, freshness_for(k, s.QUANT_PIT_MAX_AGE_DEFAULT_S)) for k in pit_keys}
            span = max([*fresh.values(), int(s.QUANT_SCORE_MAX_AGE_S)])
            stamper = PITStamper.load(con, pit_keys, spec_version=req.spec_version, lo=lo - span - DAY, hi=hi,
                                      mode=req.pit_mode, freshness=fresh,
                                      default_max_age=int(s.QUANT_PIT_MAX_AGE_DEFAULT_S),
                                      official_only=(grid == DAY), coverage=coverage,
                                      score_max_age=int(s.QUANT_SCORE_MAX_AGE_S))
            for w in stamper.warnings:
                if w not in warnings:
                    warnings.append(w)
        share_idx = _chain_share_index(con, lo - CHAIN_SHARE_MAX_AGE - DAY, hi) \
            if "chain:emission_share" in req.pit_keys else None
        market = load_market_regimes(con, req.spec_version)
        daily_occs = {r[0] for r in con.execute("SELECT DISTINCT occupant_key FROM v_quant_chain_daily")}
    finally:
        con.close()

    rows_by_netuid: dict[int, list[tuple]] = defaultdict(list)
    price_sources: dict[str, int] = defaultdict(int)
    n_backfilled = n_occ = 0
    reg_ts_cache: dict[str, int | None] = {}
    for nu, bucket, close, depth, mp, occ, src, bf, ts, vol_sum, n_rows, n_vol in raw:
        nu, bucket, ts = int(nu), int(bucket), int(ts)
        if depth is None or mp is None:
            h = hourly.get((nu, bucket))
            if h is not None and h[2] <= ts:
                depth = h[0] if depth is None else depth
                mp = h[1] if mp is None else mp
        if depth is None and grid == DAY:
            depth = daily_depth.get((nu, bucket))
        volume = float(vol_sum) if vol_sum is not None and n_vol == n_rows else 0.0
        if not occ:
            hit = occ_index.occupant_at(nu, ts)
            occ = hit[0] if hit else None
        reg = int(occ.split(":", 1)[1]) if occ else 0
        pit: dict[str, float] = {}
        if occ:
            n_occ += 1
            if occ not in reg_ts_cache:
                rts = occ_index.registered_ts(occ)
                reg_ts_cache[occ] = rts if rts is not None else (int(block_to_unix(reg, anchors)) if reg else None)
            if reg_ts_cache[occ] is not None:
                pit["_reg_ts"] = float(reg_ts_cache[occ])
            if stamper is not None:
                pit.update(stamper.stamp(occ, bucket))
            if share_idx is not None:
                v = chain_share_at(share_idx, occ, bucket)
                if v is not None:
                    pit["chain:emission_share"] = v
        price_sources[str(src)] += 1
        n_backfilled += int(bf or 0)
        rows_by_netuid[nu].append((bucket, float(close), float(depth or 0.0), float(mp or 0.0), volume, ts, occ,
                                   reg, pit))
    n_bars = len(raw)
    dates = sorted({r[0] for rows in rows_by_netuid.values() for r in rows})
    boundaries = parse_boundaries(s.QUANT_EMISSION_REGIME_BOUNDARIES)
    regime_of, regime_cov = regime_of_dates(dates, boundaries, market)
    if not market:
        warnings.append("regime_missing")
    # TMC's daily analytics cover only each netuid's current occupant (BUILD-LOG Phase 3 item 8): occupants that died
    # inside the span have no chain_daily rows, so chain:emission_share and the day labels cannot see them (13 §2.4)
    dead = [r.key for r in occ_index._by_key.values() if lo <= r.end < hi and r.netuid != 0]
    dead_cov = {"n_dead": len(dead), "n_with_chain_daily": sum(1 for k in dead if k in daily_occs)}
    if share_idx is not None and dead and dead_cov["n_with_chain_daily"] < 0.5 * len(dead) \
            and "chain_daily_excludes_dead_occupants" not in warnings:
        warnings.append("chain_daily_excludes_dead_occupants")
    panel = build_panel(rows_by_netuid, grid_seconds=grid, reset_ratio=req.reset_ratio, scope=req.scope,
                        regime_of=regime_of, pit_keys=tuple(req.pit_keys))
    panel.meta = {
        "db_path": db_path, "lo": lo, "hi": hi, "pit_mode": req.pit_mode, "spec_version": req.spec_version,
        "price_sources": dict(sorted(price_sources.items())),
        "price_bars_backfilled_frac": round(n_backfilled / n_bars, 6) if n_bars else 0.0,
        "occupant_coverage": round(n_occ / n_bars, 6) if n_bars else 0.0,
        "regime_coverage": round(regime_cov, 6),
        "warnings": warnings,
        "pit_counters": dict(stamper.counters) if stamper else {},
        "pit_used_rows": stamper.used_rows if stamper else 0,
        "pit_used_backfilled": stamper.used_backfilled if stamper else 0,
        "shadow_rows": stamper.shadow_rows if stamper else 0,
        "chain_state_used": share_idx is not None,
        "dead_occupant_chain_daily": dead_cov,
        "anchors": anchors,
        "_coverage": coverage,
        "_occupants": occ_index,
    }
    return panel

