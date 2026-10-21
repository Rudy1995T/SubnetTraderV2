"""Volatility regime classifier — pure port of the old ``app/strategy/regime.py`` (spec 12 §2.7, §8.2).

Classifies the subnet market as TRENDING / DISPERSED / CHOPPY / DEAD from realized volatility, directional strength
and cross-sectional dispersion of per-subnet price series. Ported verbatim from the old module's constants,
``_bucket_snapshots``, ``_log_returns``, ``_std``, ``classify_regime`` and ``compute_regime_metrics`` (source:
the Pi copy, ``seed/app/strategy/regime.py``, HEAD 2026-08-09). Changes, per spec 12 §8.2:

* ``_bucket_snapshots`` reads ``ts = float(parse_ts(ts_raw))`` (02 §B1's ``parse_iso`` returns ``int``); INTEGER unix
  ``ts`` and ISO strings bucket identically.
* ``settings`` receives a frozen ``RegimeParams`` with the old attribute names (old ``config.py`` ``REGIME_*``).
* ``RegimeFilter`` (live loop, gates, debounce) is not ported (02 §B8: no debounce). The old function still returns
  ``DEAD`` for a thin universe; callers map ``thin_universe`` to ``UNKNOWN`` (02 §B8, spec 12 §2.7 step 3).

Phase 3 created this file for spec 17's regime backfill (17 §8.3 step 8); spec 12 owns it and adds
``regime_for_slot`` in Phase 8. ``REGIME_TS_1_0`` is defined here and re-exported by ``app/scoring/spec.py``.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Iterable, Mapping

from app.util.time import parse_ts

TRENDING = "TRENDING"
DISPERSED = "DISPERSED"
CHOPPY = "CHOPPY"
DEAD = "DEAD"
UNKNOWN = "UNKNOWN"

_ALL_REGIMES = (TRENDING, DISPERSED, CHOPPY, DEAD)


@dataclass(frozen=True)
class RegimeParams:
    """Old ``config.py`` ``REGIME_*`` attribute names, so the ported functions are unchanged."""
    REGIME_VOL_WINDOW_HOURS: int = 24
    REGIME_BUCKET_HOURS: int = 4
    REGIME_MIN_BUCKETS: int = 6
    REGIME_MIN_SUBNETS: int = 10
    REGIME_VOL_TREND_THRESHOLD: float = 0.30
    REGIME_VOL_CHOP_FLOOR: float = 0.10
    REGIME_VOL_DEAD_THRESHOLD: float = 0.05
    REGIME_DIR_THRESHOLD: float = 0.02
    REGIME_DISP_THRESHOLD: float = 0.015


# ts-1.0 regime parameters (frozen; changing any value is a new spec_version — spec 12 §2.13).
REGIME_TS_1_0 = RegimeParams()
V_W_BY_REGIME_TS_1_0: Mapping[str, float] = MappingProxyType(
    {TRENDING: .25, DISPERSED: .25, CHOPPY: .15, DEAD: .10, UNKNOWN: .15})


@dataclass
class RegimeResult:
    label: str
    raw_regime: str
    metrics: dict = field(default_factory=dict)
    v_w: float = .15


def label_for(metrics: Mapping) -> str:
    """02 §B8: a thin universe is ``UNKNOWN`` (never the old ``DEAD``); otherwise the raw label."""
    if metrics.get("thin_universe"):
        return UNKNOWN
    return str(metrics.get("raw_regime") or UNKNOWN).upper()


def _bucket_snapshots(
    snaps: list[dict], bucket_hours: int, window_hours: int
) -> list[float]:
    """Downsample a per-subnet snapshot series into fixed-size time buckets.

    Edges are anchored at the **most recent observation** and step backwards
    by ``bucket_hours`` for up to ``window_hours // bucket_hours`` buckets.
    Anchoring at the end (rather than the first observation + bucket_sec)
    makes the count independent of sampling jitter on the earliest point —
    a snapshot that lands 5 min after ``since`` should not cost a whole
    bucket. Edges that would fall before the first observation are dropped
    to avoid projecting prices into a period with no data.

    For each surviving edge, take the last snapshot at or before that edge
    (last-observation-carried-forward). Requires snapshots to carry ``ts``
    (INTEGER unix seconds or ISO8601) and ``price``.
    """
    if not snaps or bucket_hours <= 0 or window_hours <= 0:
        return []
    parsed: list[tuple[float, float]] = []
    for row in snaps:
        ts_raw = row.get("ts")
        price = row.get("price")
        if not ts_raw or price is None:
            continue
        try:
            ts = float(parse_ts(ts_raw))
            p = float(price)
        except (ValueError, TypeError):
            continue
        if p <= 0:
            continue
        parsed.append((ts, p))
    if not parsed:
        return []
    parsed.sort(key=lambda x: x[0])

    first_ts = parsed[0][0]
    last_ts = parsed[-1][0]
    bucket_sec = bucket_hours * 3600
    n_buckets = max(1, window_hours // bucket_hours)

    edges: list[float] = []
    for i in range(n_buckets - 1, -1, -1):
        edge = last_ts - i * bucket_sec
        if edge >= first_ts - 1e-6:
            edges.append(edge)
    if not edges:
        return []

    prices: list[float] = []
    idx = 0
    last_price = parsed[0][1]
    for edge_ts in edges:
        while idx < len(parsed) and parsed[idx][0] <= edge_ts:
            last_price = parsed[idx][1]
            idx += 1
        prices.append(last_price)
    return prices


def _log_returns(prices: Iterable[float]) -> list[float]:
    out: list[float] = []
    prev: float | None = None
    for p in prices:
        if prev is not None and prev > 0 and p > 0:
            try:
                out.append(math.log(p / prev))
            except ValueError:
                pass
        prev = p
    return out


def _std(values: list[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    return math.sqrt(var)


def classify_regime(
    vol: float,
    dir_strength: float,
    dispersion: float,
    settings,
) -> str:
    """Pure state-table mapping from aggregate metrics → regime label."""
    if vol < settings.REGIME_VOL_DEAD_THRESHOLD:
        return DEAD
    if (
        vol >= settings.REGIME_VOL_TREND_THRESHOLD
        and dir_strength >= settings.REGIME_DIR_THRESHOLD
    ):
        return TRENDING
    if dispersion >= settings.REGIME_DISP_THRESHOLD:
        return DISPERSED
    if (
        vol >= settings.REGIME_VOL_CHOP_FLOOR
        and dir_strength < settings.REGIME_DIR_THRESHOLD
    ):
        return CHOPPY
    return DEAD


def compute_regime_metrics(
    per_netuid_snapshots: dict[int, list[dict]],
    settings,
) -> dict:
    """Compute aggregate regime metrics from per-subnet snapshot slices.

    Each snapshot row must carry ``ts`` (INTEGER unix seconds or ISO8601) and ``price``.
    """
    bucket_hours = max(1, int(settings.REGIME_BUCKET_HOURS))
    window_hours = max(1, int(settings.REGIME_VOL_WINDOW_HOURS))
    min_buckets = max(2, int(settings.REGIME_MIN_BUCKETS))
    min_subnets = max(1, int(settings.REGIME_MIN_SUBNETS))

    per_subnet_vol: list[float] = []
    per_subnet_abs_ret_mean: list[float] = []
    per_subnet_window_ret: list[float] = []

    for _netuid, snaps in per_netuid_snapshots.items():
        if not snaps:
            continue
        prices = _bucket_snapshots(snaps, bucket_hours, window_hours)
        if len(prices) < min_buckets:
            continue
        rets = _log_returns(prices)
        if len(rets) < 2:
            continue
        bars_per_day = max(1, 24 // bucket_hours)
        ann_factor = math.sqrt(bars_per_day * 365)
        vol = _std(rets) * ann_factor
        per_subnet_vol.append(vol)
        per_subnet_abs_ret_mean.append(
            sum(abs(r) for r in rets) / len(rets)
        )
        per_subnet_window_ret.append(math.log(prices[-1] / prices[0]))

    n_subnets = len(per_subnet_vol)
    thin = n_subnets < min_subnets

    if n_subnets == 0 or thin:
        return {
            "vol_24h": 0.0,
            "directional_strength": 0.0,
            "dispersion": 0.0,
            "raw_regime": DEAD,
            "n_subnets": n_subnets,
            "thin_universe": True,
        }

    vol_agg = sum(per_subnet_vol) / n_subnets
    dir_strength = sum(per_subnet_abs_ret_mean) / n_subnets
    dispersion = _std(per_subnet_window_ret)

    return {
        "vol_24h": round(vol_agg, 4),
        "directional_strength": round(dir_strength, 5),
        "dispersion": round(dispersion, 5),
        "raw_regime": classify_regime(vol_agg, dir_strength, dispersion, settings),
        "n_subnets": n_subnets,
        "thin_universe": False,
    }


# ── live per-slot classification (spec 12 §2.7; Phase 8) ────────────────────────────────────────────────────
_REGIME_SQL = ("SELECT netuid, hour_ts, price FROM chain_hourly WHERE hour_ts > ? AND hour_ts <= ? AND netuid <> 0 "
               "AND is_active = 1 AND price > 0 AND (observed_at <= ? OR (? = 1 AND backfilled = 1)) "
               "ORDER BY netuid, hour_ts")


async def _net_point(db, col: str, t: int, cutoff: int) -> float | None:
    v = await db.fetchval(f"SELECT {col} FROM chain_network_hourly WHERE hour_ts > ? AND hour_ts <= ? "
                          f"AND observed_at <= ? AND {col} > 0 ORDER BY hour_ts DESC LIMIT 1", (t - 3600, t, cutoff))
    return None if v is None else float(v)


async def _diagnostics(db, slot: int, cutoff: int) -> dict:
    out: dict = {}
    for name, col in (("tao_usd_ret_24h", "tao_usd"), ("sum_prices_ret_24h", "sum_subnet_prices")):
        a, b = await _net_point(db, col, slot - 86400, cutoff), await _net_point(db, col, slot, cutoff)
        out[name] = math.log(b / a) if (a and b) else None
    return out


async def regime_for_slot(db, slot: int, cutoff: int, mode: str, params: RegimeParams,
                          v_w_by_regime: Mapping[str, float], *, spec_version: str | None = None,
                          official: bool = False) -> RegimeResult:
    """Regime at ``slot`` from the 24 h of ``chain_hourly`` prices admitted by ``cutoff`` (spec 12 §2.7 steps 1–4).

    Step 3a: an official slot reuses an existing ``regime_daily(day_ts=slot, spec_version)`` row (17's backfill, or
    an earlier run) — ``v_w`` is re-derived from ``v_w_by_regime`` so the stored row and ``scores.regime`` agree."""
    if official and spec_version:
        row = await db.fetchone("SELECT * FROM regime_daily WHERE day_ts = ? AND spec_version = ?",
                                (int(slot), spec_version))
        if row is not None:
            label = str(row["label"]).upper()
            metrics = {"vol_24h": row["vol_24h"], "directional_strength": row["directional_strength"],
                       "dispersion": row["dispersion"], "raw_regime": row["raw_regime"] or label,
                       "n_subnets": row["n_subnets"], "thin_universe": bool(row["thin_universe"]),
                       "tao_usd_ret_24h": row["tao_usd_ret_24h"], "sum_prices_ret_24h": row["sum_prices_ret_24h"],
                       "reused": row["source"]}
            return RegimeResult(label, metrics["raw_regime"], metrics, float(v_w_by_regime[label]))
    rows = await db.fetchall(_REGIME_SQL, (int(slot) - 86400, int(slot), int(cutoff),
                                           1 if mode == "reconstructed" else 0))
    per: dict[int, list[dict]] = {}
    for r in rows:
        per.setdefault(int(r["netuid"]), []).append({"ts": int(r["hour_ts"]), "price": float(r["price"])})
    metrics = compute_regime_metrics(per, params)
    label = label_for(metrics)
    metrics.update(await _diagnostics(db, int(slot), int(cutoff)))
    return RegimeResult(label, str(metrics["raw_regime"]).upper(), metrics, float(v_w_by_regime[label]))
