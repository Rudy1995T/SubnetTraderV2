"""Spec 12 §9 ``test_regime.py`` (port created in Phase 3 for spec 17 §2.7).

Expected metrics were generated once from the old ``seed/app/strategy/regime.py`` (Pi copy, HEAD 2026-08-09) on the
fixture below with ISO-string ``ts`` and are pinned here; the port must reproduce them with ISO and INTEGER ``ts``.
Bucketing tests are the old ``tests/test_regime_filter.py`` pure-function cases, with fixed timestamps.
"""
from __future__ import annotations

import math
import random
from dataclasses import replace

from app.quant.regime import (CHOPPY, DEAD, DISPERSED, REGIME_TS_1_0, TRENDING, UNKNOWN, V_W_BY_REGIME_TS_1_0,
                              _bucket_snapshots, classify_regime, compute_regime_metrics, label_for)
from app.util.time import to_iso

END = 1790121600            # 2026-09-23T00:00Z

GOLDEN = {
    "trending": ((0.012, 0.012), 0.03, 1,
                 {"vol_24h": 2.3653, "directional_strength": 0.05268, "dispersion": 0.10175, "raw_regime": TRENDING}),
    "dead": ((0.0, 0.0), 0.0002, 2,
             {"vol_24h": 0.0179, "directional_strength": 0.00032, "dispersion": 0.00086, "raw_regime": DEAD}),
    "dispersed": ((0.004, -0.004), 0.001, 3,
                  {"vol_24h": 0.0936, "directional_strength": 0.01569, "dispersion": 0.08202, "raw_regime": DISPERSED}),
    "choppy": ((0.0, 0.0), 0.0025, 4,
               {"vol_24h": 0.2299, "directional_strength": 0.00379, "dispersion": 0.00912, "raw_regime": CHOPPY}),
}


def _fixture(drift: tuple[float, float], vol: float, seed: int) -> dict[int, list[tuple[int, float]]]:
    """12 subnets × 25 hourly closes over (END − 86400, END]; odd netuids take drift[0], even drift[1]."""
    out = {}
    for n in range(1, 13):
        rng = random.Random(seed * 1000 + n)
        p, rows = 0.05, []
        for h in range(25):
            p *= math.exp((drift[0] if n % 2 else drift[1]) + rng.gauss(0, vol))
            rows.append((END - 86400 + 3600 * h, p))
        out[n] = rows
    return out


def test_port_reproduces_old_module_outputs():
    for name, (drift, vol, seed, want) in GOLDEN.items():
        fx = _fixture(drift, vol, seed)
        as_int = {n: [{"ts": t, "price": p} for t, p in r] for n, r in fx.items()}
        as_iso = {n: [{"ts": to_iso(t), "price": p} for t, p in r] for n, r in fx.items()}
        for snaps in (as_int, as_iso):
            m = compute_regime_metrics(snaps, REGIME_TS_1_0)
            assert m == {**want, "n_subnets": 12, "thin_universe": False}, name


def test_thin_universe_is_unknown_with_baseline_weight():
    fx = _fixture((0.012, 0.012), 0.03, 1)
    few = {n: [{"ts": t, "price": p} for t, p in fx[n]] for n in range(1, 10)}       # 9 < MIN_SUBNETS 10
    m = compute_regime_metrics(few, REGIME_TS_1_0)
    assert m["thin_universe"] is True and m["raw_regime"] == DEAD and m["n_subnets"] == 9
    assert label_for(m) == UNKNOWN and V_W_BY_REGIME_TS_1_0[UNKNOWN] == 0.15
    assert compute_regime_metrics({7: []}, replace(REGIME_TS_1_0, REGIME_MIN_SUBNETS=1))["thin_universe"] is True


def test_v_w_by_regime():
    assert V_W_BY_REGIME_TS_1_0 == {TRENDING: .25, DISPERSED: .25, CHOPPY: .15, DEAD: .10, UNKNOWN: .15}
    assert label_for({"thin_universe": False, "raw_regime": "trending"}) == TRENDING


def test_frozen_ts_1_0_params():
    p = REGIME_TS_1_0
    assert (p.REGIME_VOL_WINDOW_HOURS, p.REGIME_BUCKET_HOURS, p.REGIME_MIN_BUCKETS, p.REGIME_MIN_SUBNETS) == (24, 4, 6, 10)
    assert (p.REGIME_VOL_TREND_THRESHOLD, p.REGIME_VOL_CHOP_FLOOR, p.REGIME_VOL_DEAD_THRESHOLD,
            p.REGIME_DIR_THRESHOLD, p.REGIME_DISP_THRESHOLD) == (0.30, 0.10, 0.05, 0.02, 0.015)


def test_classify_regime_state_table():
    s = REGIME_TS_1_0
    assert classify_regime(vol=0.06, dir_strength=0.01, dispersion=0.01, settings=s) == DEAD
    assert classify_regime(vol=0.30, dir_strength=0.02, dispersion=0.0, settings=s) == TRENDING
    assert classify_regime(vol=0.10, dir_strength=0.0, dispersion=0.015, settings=s) == DISPERSED
    assert classify_regime(vol=0.10, dir_strength=0.019, dispersion=0.0, settings=s) == CHOPPY
    assert classify_regime(vol=0.04, dir_strength=0.5, dispersion=0.5, settings=s) == DEAD


def test_bucket_snapshots_10min_cadence_gives_6_buckets():
    rows = [{"ts": END - 600 * (143 - i), "price": 0.05} for i in range(144)]          # 23h50m span
    assert len(_bucket_snapshots(rows, bucket_hours=4, window_hours=24)) == 6


def test_bucket_snapshots_drops_edges_before_first_observation_and_window():
    rows = [{"ts": END - 43200 + int(43200 * i / 5), "price": 100 + i} for i in range(6)]
    assert len(_bucket_snapshots(rows, bucket_hours=4, window_hours=24)) == 4
    assert len(_bucket_snapshots(rows, bucket_hours=4, window_hours=12)) == 3


def test_bucket_snapshots_locf_and_invalid_rows():
    rows = [{"ts": END - 43200, "price": 1.0}, {"ts": END, "price": 2.0}]
    assert _bucket_snapshots(rows, bucket_hours=4, window_hours=24) == [1.0, 1.0, 1.0, 2.0]
    assert _bucket_snapshots([], bucket_hours=4, window_hours=24) == []
    assert _bucket_snapshots([{"ts": END, "price": 1.0}], bucket_hours=0, window_hours=24) == []
    bad = [{"ts": END, "price": 0.0}, {"ts": END, "price": -1.0}, {"ts": END, "price": None},
           {"ts": None, "price": 1.0}, {"ts": "not-a-date", "price": 1.0}]
    assert _bucket_snapshots(bad, bucket_hours=4, window_hours=24) == []


def test_int_and_iso_ts_bucket_identically():
    rows = [(END - 3600 * h, 1.0 + 0.01 * h) for h in range(30)]
    a = _bucket_snapshots([{"ts": t, "price": p} for t, p in rows], 4, 24)
    b = _bucket_snapshots([{"ts": to_iso(t), "price": p} for t, p in rows], 4, 24)
    assert a == b and len(a) == 6
