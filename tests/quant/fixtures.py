"""Synthetic snapshot for the harness tests (spec 13 §9): 03's real schema (models.sql + migrations) filled through
the base tables with 03's names and units, so the ``v_quant_*`` views and their rao → τ conversions are exercised.

Day 0 = 2025-08-01 (so the survivorship scope starts on day 52 and the live scope on day 264). Planted structure:

* ``price_bars`` 1 h: ``ledger_import`` (ts_last, reserves, Δ volume) to day 270, ``tmc_candle`` after (NULL ts_last /
  reserves / moving_price); a few ``5m`` rows that must be ignored.
* netuid 7 re-registered on day 200 with the collapse captured; netuid 9 re-registered on day 300 without a collapse
  (successor: ``registered_ts`` NULL, ``registered_at_est`` set, price rows unstamped); netuid 11 deregistered on day
  380 with no successor; netuid 13 at zero emission from day 350 (+ ``emission_disabled`` on day 355).
* ``metric_values`` norm rows: ``emission_share`` (backfilled, observed day 419), ``active_authors_28d`` (live,
  observed ``as_of + 900``, predicts the drift), ``site_uptime_30d`` (live from day 400), ``x_engagement_30d``
  (backfilled, from day 405); raw rows with *different* values; raw ``owner_locked_share`` (ok / failed / absent).
* ``scores`` 4 slots/day; backfilled to day 399 (computed_at day 419), live after; one ``shadow=1`` ``ts-1.1`` set.
* ``regime_daily`` backfill to day 380 then live, day 100 missing; ``block_times`` anchors; ``coverage.json``.
"""
from __future__ import annotations

import asyncio
import json
import math
import random
import sqlite3
from pathlib import Path

DAY = 86400
T0 = 1754006400                 # 2025-08-01 00:00Z
B_ANCHOR, T_ANCHOR = 6_000_000, T0   # block at day 0 (12 s/block)
SPEC = "ts-1.0"


def day(d: float) -> int:
    return int(T0 + d * DAY)


def block_at(ts: int) -> int:
    return B_ANCHOR + (ts - T_ANCHOR) // 12


def drift(nu: int) -> float:
    """Per-day log drift; netuid order is mixed so drift is not a function of netuid."""
    return ((nu * 37) % 24 - 11.5) * 0.0012


def _apply_schema(path: Path) -> None:
    from app.storage.db import Database

    async def _go():
        db = Database(path)
        await db.connect()
        await db.apply_schema()
        await db.close()
    asyncio.run(_go())


def occ_key(nu: int, reg_ts: int) -> str:
    return f"{nu}:{block_at(reg_ts)}"


def make_fixture_db(path: str | Path, *, n_netuids: int = 24, days: int = 420, seed: int = 0) -> dict:
    """Build the fixture DB (+ ``coverage.json`` next to it as ``<stem>.coverage.json``); returns facts for tests."""
    path = Path(path)
    _apply_schema(path)
    rng = random.Random(seed)
    con = sqlite3.connect(path)
    netuids = list(range(1, n_netuids + 1))
    first_reg = {nu: day(-100 - nu) for nu in netuids}

    # occupants: (key, nu, reg_ts, status, est, dereg_at, dereg_ts) and the time-interval per occupant
    occ_rows = []
    spans: dict[int, list[tuple[str, int, int]]] = {}
    for nu in netuids:
        k0 = occ_key(nu, first_reg[nu])
        if nu == 7:
            k1 = occ_key(7, day(200))
            occ_rows += [(k0, nu, first_reg[nu], None, "deregistered", day(200) + 3600, day(200)),
                         (k1, nu, day(200), None, "active", None, None)]
            spans[nu] = [(k0, day(-1000), day(200)), (k1, day(200), day(10_000))]
        elif nu == 9:
            k1 = f"9:{block_at(day(300))}"
            occ_rows += [(k0, nu, first_reg[nu], None, "deregistered", day(300) + 7200, None),
                         (k1, nu, None, day(300) + 600, "active", None, None)]
            spans[nu] = [(k0, day(-1000), day(300)), (None, day(300), day(10_000))]   # successor rows unstamped
        elif nu == 11:
            occ_rows.append((k0, nu, first_reg[nu], None, "deregistered", day(380) + 3600, day(380)))
            spans[nu] = [(k0, day(-1000), day(380) + 1)]
        else:
            occ_rows.append((k0, nu, first_reg[nu], None, "active", None, None))
            spans[nu] = [(k0, day(-1000), day(10_000))]
    for key, nu, reg_ts, est, status, dereg_at, dereg_ts in occ_rows:
        reg_block = int(key.split(":")[1])
        t_first = reg_ts if reg_ts is not None else est
        con.execute(
            "INSERT INTO occupants (occupant_key, netuid, network_registered_at, status, first_seen_block, "
            "first_seen_at, last_seen_block, last_seen_at, deregistered_at, dereg_reason, registered_at_est, "
            "registered_ts, deregistered_ts, source) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (key, nu, reg_block, status, reg_block, t_first, reg_block, dereg_at or day(days), dereg_at,
             "reoccupied" if status == "deregistered" and nu != 11 else ("absent" if nu == 11 else None),
             est if est is not None else reg_ts + 60, reg_ts, dereg_ts, "rpc"))
    occ_by_key = {r[0]: r for r in occ_rows}

    def occupant(nu: int, ts: int, stamped: bool = True) -> str | None:
        for k, lo, hi in spans[nu]:
            if lo <= ts < hi:
                return k
        return None

    def true_occ(nu: int, ts: int) -> str | None:
        if nu == 9 and ts >= day(300):
            return f"9:{block_at(day(300))}"
        return occupant(nu, ts)

    # price bars
    bars = []
    end_h = days * 24
    for nu in netuids:
        logp = math.log(0.01 + nu * 0.001)
        depth0 = 4000.0 + 900.0 * nu
        last_h = (380 * 24) if nu == 11 else end_h
        for h in range(last_h + 1):
            ts = T0 + h * 3600
            logp += drift(nu) / 24 + rng.gauss(0, 0.004)
            if nu == 7 and h == 200 * 24:
                logp += math.log(0.01)           # captured collapse: the new registration's price
            price = math.exp(logp)
            d = h / 24
            depth = depth0 * (1 + 0.1 * math.sin(d / 9 + nu))
            occ = occupant(nu, ts)
            if d <= 270:
                vol = abs(rng.gauss(50, 10)) + nu
                bars.append((nu, "1h", ts, occ, price, vol, ts + 3540, depth, depth / price, price * 0.98,
                             block_at(ts + 3540), "ledger_import", ts + 3540, 1))
            else:
                bars.append((nu, "1h", ts, occ, price, 12.0, None, None, None, None, None, "tmc_candle", ts + 7200, 1))
    con.executemany(
        "INSERT INTO price_bars (netuid, period, ts, occupant_key, close, volume, ts_last, tao_in_pool, alpha_in_pool, "
        "moving_price, block_number, source, observed_at, backfilled) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", bars)
    con.executemany(
        "INSERT INTO price_bars (netuid, period, ts, occupant_key, close, source, observed_at) "
        "VALUES (?,?,?,?,?,?,?)", [(1, "5m", day(300) + 300 * j, occupant(1, day(300)), 999.0, "tmc_candle",
                                    day(300)) for j in range(5)])

    # chain_hourly (depth fallback, rao) for days > 270; chain_daily for every day
    ch = []
    cd = []
    for nu in netuids:
        depth0 = 4000.0 + 900.0 * nu
        for h in range(271 * 24, end_h + 1):
            ts = T0 + h * 3600
            occ = true_occ(nu, ts)
            if occ is None or (nu == 11 and ts > day(380)):
                continue
            depth = depth0 * (1 + 0.1 * math.sin((h / 24) / 9 + nu))
            ch.append((nu, ts, occ, block_at(ts), ts, ts + 60, "analytics_1m", depth * 1e9, 0.02))
        for dd in range(days + 1):
            ts = day(dd)
            occ = true_occ(nu, ts)
            if occ is None or (nu == 11 and dd > 380):
                continue
            share = 0.0 if (nu == 13 and dd >= 350) else 0.2 + 0.05 * ((nu * 7) % 24)
            depth = depth0 * (1 + 0.1 * math.sin(dd / 9 + nu))
            cd.append((nu, ts, occ, block_at(ts), ts, day(days) + 100, "analytics_1y", (depth + 1.0) * 1e9, share))
    con.executemany("INSERT INTO chain_hourly (netuid, hour_ts, occupant_key, block_number, event_at, observed_at, "
                    "source, tao_liquidity, subnet_moving_price) VALUES (?,?,?,?,?,?,?,?,?)", ch)
    con.executemany("INSERT INTO chain_daily (netuid, day_ts, occupant_key, block_number, event_at, observed_at, "
                    "source, subnet_tao, emission_percent) VALUES (?,?,?,?,?,?,?,?,?)", cd)

    # metric_values
    mv = []

    def norm(occ, key, as_of, value, raw, observed, bf):
        mv.append((occ, key, as_of, SPEC, "norm", value, json.dumps({"raw": raw, "status": "ok"}), observed, "scoring",
                   "r", bf))

    def raw_row(occ, key, as_of, value, status, observed, bf=0):
        mv.append((occ, key, as_of, SPEC, "raw", value, json.dumps({"status": status}), observed, "chain", "r", bf))

    for nu in netuids:
        rank_drift = sorted(netuids, key=drift).index(nu) / (len(netuids) - 1)
        for dd in range(days):
            as_of = day(dd)
            occ = true_occ(nu, as_of)
            if occ is None or (nu == 11 and dd > 380):
                continue
            share = 0.2 + 0.05 * ((nu * 7) % 24)
            norm(occ, "emission_share", as_of, min(1.0, share / 1.5), share, day(419), 1)
            norm(occ, "active_authors_28d", as_of, rank_drift, 3 + nu, as_of + 900, 0)
            raw_row(occ, "active_authors_28d", as_of, 99.0 + nu, "ok", as_of + 600)          # never a factor
            raw_row(occ, "emission_share", as_of, 50.0, "ok", as_of + 600)
            if dd >= 400:
                norm(occ, "site_uptime_30d", as_of, 0.9, 0.99, as_of + 900, 0)
            if dd >= 405:
                norm(occ, "x_engagement_30d", as_of, rank_drift, 10.0, day(419), 1)
            st = "failed" if dd % 10 == 3 else ("absent" if dd % 10 == 5 else "ok")
            raw_row(occ, "owner_locked_share", as_of, None if st == "failed" else (0.0 if st == "absent" else
                                                                                   0.1 + nu / 100), st, as_of + 600)
    con.executemany("INSERT INTO metric_values (occupant_key, metric_key, as_of, spec_version, stage, value, "
                    "value_json, observed_at, source, run_id, backfilled) VALUES (?,?,?,?,?,?,?,?,?,?,?)", mv)

    # scores: 4 slots/day; backfilled to day 399
    sc = []
    for nu in netuids:
        rank_drift = sorted(netuids, key=drift).index(nu) / (len(netuids) - 1)
        for dd in range(days):
            for slot in range(4):
                as_of = day(dd) + slot * 6 * 3600
                occ = true_occ(nu, as_of)
                if occ is None or (nu == 11 and dd > 380):
                    continue
                bf = 1 if dd <= 399 else 0
                computed = day(419) if bf else as_of + 900
                p1, p2 = 0.3 + 0.5 * rank_drift, 0.6 - 0.2 * rank_drift
                q = 100 * (30 * p1 + 20 * p2) / 50
                sc.append((occ, nu, as_of, SPEC, "official" if slot == 0 else "provisional", "r", "active", q, 50.0,
                           q, q, q, json.dumps({"P1": p1, "P2": p2, "P3": None}), json.dumps({"P1": 1.0}), 0.8,
                           computed, as_of + 900, bf, 0))
                if dd == 410 and slot == 0:
                    sc.append((occ, nu, as_of, "ts-1.1", "official", "r", "active", 11.0, 50.0, 11.0, 11.0, 11.0,
                               json.dumps({"P1": 0.11, "P2": 0.11}), json.dumps({"P1": 1.0}), 0.8, as_of + 900,
                               as_of + 900, 0, 1))
    con.executemany("INSERT INTO scores (occupant_key, netuid, as_of, spec_version, run_kind, run_id, status, quality, "
                    "velocity, composite_raw, composite, composite_smoothed, pillar_scores, pillar_coverage, "
                    "confidence, "
                    "computed_at, input_cutoff, backfilled, shadow) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", sc)

    # regime_daily (day 100 missing), events, block_times
    labels = ("TRENDING", "DISPERSED", "CHOPPY", "DEAD")
    con.executemany("INSERT INTO regime_daily (day_ts, spec_version, label, v_w, source, computed_at, backfilled) "
                    "VALUES (?,?,?,?,?,?,?)",
                    [(day(dd), SPEC, labels[(dd // 30) % 4], 0.15, "backfill" if dd <= 380 else "live",
                      day(dd) + 900, 1 if dd <= 380 else 0) for dd in range(days + 1) if dd != 100])
    k13 = occ_key(13, first_reg[13])
    k9_old = occ_key(9, first_reg[9])
    k9_new = f"9:{block_at(day(300))}"
    con.execute("INSERT INTO events (occupant_key, netuid, event_type, severity, event_at, observed_at, source, "
                "payload) "
                "VALUES (?,?,?,?,?,?,?,?)", (k13, 13, "emission_disabled", "warning", day(355), day(355) + 60, "chain",
                                              "{}"))
    con.execute("INSERT INTO events (occupant_key, netuid, event_type, severity, event_at, observed_at, source, "
                "payload) "
                "VALUES (?,?,?,?,?,?,?,?)", (k9_new, 9, "netuid_reoccupied", "warning", day(300), day(419), "registry",
                                              json.dumps({"backfilled": True, "old_key": k9_old, "new_key": k9_new})))
    anchors = [block_at(t) for t in (day(-300), day(0), day(150), day(300), day(400))]
    con.executemany("INSERT INTO block_times (block_number, ts_ms, endpoint, observed_at) VALUES (?,?,?,?)",
                    [(b, (T_ANCHOR + (b - B_ANCHOR) * 12) * 1000, "fixture", day(419)) for b in anchors])
    con.commit()
    con.close()

    cov = {"schema": "coverage-1", "generated_at": day(419), "spec_version": SPEC,
           "series": {"price_bars_1h": {"completeness_by_month": {"2025-08": 1.0, "2025-12": 0.5}}},
           "occupancy": {"unstamped": {"price_bars_1h": 0}},
           "holes": [{"series": "price_bars_1h", "from_ts": day(150) - 3600, "to_ts": day(150) + 3600,
                      "netuids": "all", "reason": "fixture"}],
           "reconstructible_from": {"emission_share": day(30), "price_depth_moving_price": day(-200),
                                    "github_p2": None},
           "forward_only": ["site_uptime_30d", "x_engagement_30d"],
           "caveats": ["moving_price_enriched_rows"], "checks": {"rpc_golden_block": "pass"}}
    cov_path = path.with_name(path.stem + ".coverage.json")
    cov_path.write_text(json.dumps(cov))
    return {"path": str(path), "coverage": str(cov_path), "first_reg": first_reg, "k13": k13, "k9_old": k9_old,
            "k9_new": k9_new, "k7_old": occ_key(7, first_reg[7]), "k7_new": occ_key(7, day(200)),
            "k11": occ_key(11, first_reg[11]), "occ_by_key": occ_by_key, "netuids": netuids}
