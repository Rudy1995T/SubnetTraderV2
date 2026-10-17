"""Workbench factor study (spec 19 §2.8) — exploration until spec 13's gated harness exists.

Rebuilds a daily panel in memory from ``price_bars`` 1 d (current-occupant bars only, dead occupants up to their last
bar), computes the workbench factors per day, and reports per factor and horizon: mean daily Spearman IC, its t-stat
on non-overlapping dates, hit rate, top-minus-bottom quintile return, plus a death-label IC and a matched-survivor
death study. Writes ``data/quant/results/wb_study_<YYYYMMDD>.json`` and appends ``wb_trials.jsonl``.

CLI: ``python -m app.wb.study [--horizons 7,28] [--since 2025-06-01] [--min-n 20]``
"""
from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence

import app.config as app_config
from app.features.technicals import dev_sigma, ema, log_rets, _std
from app.util.mathutil import spearman_r
from app.util.time import now_ts, parse_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

DAY = 86400
FACTORS = ("ema72_band_pos_1d", "ema72_dist_1d", "dd_90d", "rv_30d", "px_vs_moving", "dereg_rank_hist",
           "mp_gap_to_cull_1")
NOTES = [
    "Exploration only: not spec 13's gate (no PIT snapshot, no deflated Sharpe, no cost model).",
    "Forward returns use the same occupant's close h days later; a death before t+h leaves the label NULL "
    "(counted in n_died, never back-filled).",
    "Dereg factors use WB_STUDY_IMMUNITY_BLOCKS for the whole history and a linear block clock (7,200/day).",
    "EMA is run over each occupant's full 1 d history (equal to the stored 432-bar window to < 0.1 % after 360 bars).",
    "Survivorship: occupants culled before t+h drop out of the return labels, so a return IC for a dereg factor "
    "describes survivors only; read it next to death_90d and the matched-survivor study.",
]


@dataclass
class Series:
    occ: str
    netuid: int
    reg_block: int
    ts: list[int] = field(default_factory=list)
    close: list[float] = field(default_factory=list)
    mp: list[float | None] = field(default_factory=list)
    liq: list[float | None] = field(default_factory=list)


def factor_series(s: Series, *, period: int = 72, k: float = 2.0) -> dict[str, dict[int, float]]:
    """Per-day technical factors of one occupant (value known at the bar's close)."""
    out: dict[str, dict[int, float]] = {f: {} for f in FACTORS[:5]}
    c = s.close
    e = ema(c, period)
    for i, t in enumerate(s.ts):
        if s.mp[i] and s.mp[i] > 0:
            out["px_vs_moving"][t] = math.log(c[i] / s.mp[i])
        if e[i] is None:
            continue
        out["ema72_dist_1d"][t] = c[i] / e[i] - 1
        sd = dev_sigma(c, e, period, end=i + 1)
        if sd:
            out["ema72_band_pos_1d"][t] = max(-2.0, min(2.0, math.log(c[i] / e[i]) / (k * sd)))
        w90 = c[max(0, i - 89): i + 1]
        if len(w90) >= 14:
            out["dd_90d"][t] = c[i] / max(w90) - 1
        r30 = log_rets(c[max(0, i - 30): i + 1])
        sd30 = _std(r30)
        if sd30 is not None and len(r30) >= 20:
            out["rv_30d"][t] = sd30 * math.sqrt(365)
    return out


def dereg_factors(series: Sequence[Series], *, imm: int, ref_ts: int, ref_block: int) -> dict[str, dict]:
    """``dereg_rank_hist`` and ``mp_gap_to_cull_1`` per (occ, day) from 1 d moving prices."""
    by_day: dict[int, list[tuple[float, int, str]]] = {}
    for s in series:
        for t, mp in zip(s.ts, s.mp):
            if mp is not None:
                by_day.setdefault(t, []).append((mp, s.reg_block, s.occ))
    rank: dict[str, dict[int, float]] = {}
    gap: dict[str, dict[int, float]] = {}
    for t, rows in by_day.items():
        head = ref_block - (ref_ts - (t + DAY)) // 12
        pool = sorted((r for r in rows if head - r[1] >= imm), key=lambda r: (r[0], r[1]))
        for i, (mp, _, occ) in enumerate(pool):
            rank.setdefault(occ, {})[t] = float(i + 1)
            others = pool[1][0] if i == 0 and len(pool) > 1 else pool[0][0]
            if others > 0:
                gap.setdefault(occ, {})[t] = mp / others - 1
    return {"dereg_rank_hist": rank, "mp_gap_to_cull_1": gap}


def binom_two_sided(k: int, n: int) -> float | None:
    if n == 0:
        return None
    pk = sum(math.comb(n, i) for i in range(0, min(k, n - k) + 1)) / 2 ** n
    return min(1.0, 2 * pk)


def ic_stats(per_day: dict[int, tuple[list[float], list[float]]], h: int, min_n: int) -> dict:
    ics: list[tuple[int, float]] = []
    spreads: list[float] = []
    n_obs = 0
    for t in sorted(per_day):
        xs, ys = per_day[t]
        if len(xs) < min_n:
            continue
        r = spearman_r(xs, ys)
        if r is None or math.isnan(r):
            continue
        ics.append((t, r))
        n_obs += len(xs)
        order = sorted(range(len(xs)), key=lambda i: xs[i])
        q = max(1, len(xs) // 5)
        spreads.append(sum(ys[i] for i in order[-q:]) / q - sum(ys[i] for i in order[:q]) / q)
    if not ics:
        return {"n_days": 0, "n_obs": 0}
    vals = [r for _, r in ics]
    nonover, last = [], None
    for t, r in ics:
        if last is None or t - last >= h * DAY:
            nonover.append(r)
            last = t
    sd = _std(nonover)
    return {"ic_mean": sum(vals) / len(vals), "ic_t": (sum(nonover) / len(nonover)) / sd * math.sqrt(len(nonover))
            if sd else None, "n_nonoverlap": len(nonover), "hit_rate": sum(1 for v in vals if v > 0) / len(vals),
            "q_spread": sum(spreads) / len(spreads), "n_days": len(vals), "n_obs": n_obs}


async def load_series(db: "Database") -> tuple[list[Series], dict[str, dict]]:
    occ = {r["occupant_key"]: r for r in await db.fetchall(
        "SELECT occupant_key, netuid, network_registered_at, status, deregistered_ts FROM occupants WHERE netuid != 0")}
    rows = await db.fetchall("SELECT occupant_key, netuid, ts, close, moving_price, tao_in_pool FROM price_bars "
                             "WHERE period = '1d' AND occupant_key IS NOT NULL AND netuid != 0 ORDER BY occupant_key, ts")
    out: dict[str, Series] = {}
    for r in rows:
        o = occ.get(r["occupant_key"])
        if o is None or not r["close"] or r["close"] <= 0:
            continue
        s = out.get(r["occupant_key"])
        if s is None:
            s = out[r["occupant_key"]] = Series(r["occupant_key"], r["netuid"], int(o["network_registered_at"]))
        s.ts.append(r["ts"])
        s.close.append(r["close"])
        s.mp.append(r["moving_price"])
        s.liq.append(r["tao_in_pool"])
    return list(out.values()), occ


async def run_study(db: "Database", *, horizons: Sequence[int] = (7, 28), since: int | None = None,
                    min_n: int = 20, now: int | None = None) -> dict:
    s = app_config.settings
    t0 = time.monotonic()
    now = now or now_ts()
    series, occ = await load_series(db)
    ref = await db.fetchone("SELECT MAX(block_number) AS b, MAX(ts) AS t FROM price_bars WHERE period = '1h' AND "
                            "ts > ? AND block_number IS NOT NULL", (now - 3 * DAY,))
    ref_block, ref_ts = (int(ref["b"]), int(ref["t"]) + 3600) if ref and ref["b"] else (0, now)
    fac: dict[str, dict[str, dict[int, float]]] = {f: {} for f in FACTORS}
    for sr in series:
        for f, v in factor_series(sr, period=s.WB_EMA_PERIOD, k=s.WB_EMA_BAND_K).items():
            fac[f][sr.occ] = v
    if ref_block:
        for f, v in dereg_factors(series, imm=s.WB_STUDY_IMMUNITY_BLOCKS, ref_ts=ref_ts, ref_block=ref_block).items():
            fac[f] = v
    close = {sr.occ: dict(zip(sr.ts, sr.close)) for sr in series}
    last_ts = {sr.occ: sr.ts[-1] for sr in series}
    dead_at = {k: (o["deregistered_ts"] or None) for k, o in occ.items() if o["status"] == "deregistered"}
    result: dict[str, Any] = {"kind": "wb_study", "created_at": to_iso(now), "horizons": list(horizons),
                              "since": to_iso(since) if since else None, "min_n": min_n,
                              "n_occupants": len(series), "factors": {}, "notes": NOTES}
    for f in FACTORS:
        entry: dict[str, Any] = {"by_h": {}}
        for h in horizons:
            per_day: dict[int, tuple[list[float], list[float]]] = {}
            died = 0
            for o, vals in fac[f].items():
                cl = close[o]
                for t, x in vals.items():
                    if since and t < since:
                        continue
                    if t + h * DAY > last_ts[o]:
                        died += o in dead_at and t + h * DAY <= ref_ts
                        continue
                    c1 = cl.get(t + h * DAY)
                    if c1 is None:
                        continue
                    d = per_day.setdefault(t, ([], []))
                    d[0].append(x)
                    d[1].append(math.log(c1 / cl[t]))
            entry["by_h"][str(h)] = {**ic_stats(per_day, h, min_n), "n_died": died}
        # death within 90 d as the label
        per_day = {}
        for o, vals in fac[f].items():
            da = dead_at.get(o)
            for t, x in vals.items():
                if (since and t < since) or t + 90 * DAY > ref_ts:
                    continue
                d = per_day.setdefault(t, ([], []))
                d[0].append(x)
                d[1].append(1.0 if da and t < da <= t + 90 * DAY else 0.0)
        entry["death_90d"] = ic_stats(per_day, 90, min_n)
        entry["matched"] = matched_deaths(fac[f], series, dead_at, since=since)
        result["factors"][f] = entry
    result["seconds"] = round(time.monotonic() - t0, 2)
    return result


def matched_deaths(vals: dict[str, dict[int, float]], series: Sequence[Series], dead_at: dict[str, int | None], *,
                   since: int | None) -> dict:
    """Dead occupant's factor at T−7/−30 d vs the mean of the 3 survivors nearest in pool TAO on the same day."""
    liq: dict[int, list[tuple[float, str]]] = {}
    for sr in series:
        for t, q in zip(sr.ts, sr.liq):
            if q is not None:
                liq.setdefault(t, []).append((q, sr.occ))
    out: dict[str, Any] = {}
    for lag in (7, 30):
        below = n = 0
        for o, da in dead_at.items():
            if not da or (since and da < since) or o not in vals:
                continue
            t = (da // DAY) * DAY - lag * DAY
            x = vals[o].get(t)
            if x is None:
                continue
            mine = next((q for q, oo in liq.get(t, []) if oo == o), None)
            if mine is None:
                continue
            surv = [(abs(q - mine), oo) for q, oo in liq.get(t, []) if oo != o and oo not in dead_at
                    and vals.get(oo, {}).get(t) is not None]
            surv.sort()
            if len(surv) < 3:
                continue
            m = sum(vals[oo][t] for _, oo in surv[:3]) / 3
            n += 1
            below += x < m
        out[f"t{lag}"] = {"n": n, "share_below_matched": below / n if n else None,
                          "sign_test_p": binom_two_sided(below, n)}
    return out


def write_result(res: dict, base: Path) -> Path:
    rdir = base / "results"
    rdir.mkdir(parents=True, exist_ok=True)
    stamp = res["created_at"][:10].replace("-", "")
    path = rdir / f"wb_study_{stamp}.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(res, indent=1, default=str))
    tmp.replace(path)
    n_tests = len(res["factors"]) * (len(res["horizons"]) + 1)
    with (rdir / "wb_trials.jsonl").open("a") as fh:
        fh.write(json.dumps({"created_at": res["created_at"], "file": path.name, "factors": list(res["factors"]),
                             "horizons": res["horizons"], "since": res["since"], "n_tests": n_tests}) + "\n")
    return path


def render(res: dict) -> str:
    lines = [f"wb study {res['created_at']} · occupants={res['n_occupants']} · {res['seconds']}s (exploration, not 13's gate)"]
    hs = res["horizons"]
    lines.append(f"{'factor':<22}" + "".join(f"{'IC'+str(h):>9}{'t':>7}{'hit':>6}" for h in hs) +
                 f"{'IC dth90':>10}{'below@T-7':>11}{'p':>7}")
    for f, e in res["factors"].items():
        row = f"{f:<22}"
        for h in hs:
            b = e["by_h"][str(h)]
            row += (f"{b.get('ic_mean', float('nan')):>9.3f}{(b.get('ic_t') or float('nan')):>7.2f}"
                    f"{b.get('hit_rate', float('nan')):>6.2f}")
        d, m = e["death_90d"], e["matched"]["t7"]
        row += f"{d.get('ic_mean', float('nan')):>10.3f}"
        row += f"{(m['share_below_matched'] if m['share_below_matched'] is not None else float('nan')):>11.2f}"
        row += f"{(m['sign_test_p'] if m['sign_test_p'] is not None else float('nan')):>7.3f}"
        lines.append(row)
    return "\n".join(lines)


async def _cli(horizons: str, since: str | None, min_n: int) -> int:
    from app.storage.db import Database
    s = app_config.settings
    db = Database(s.path(s.DB_PATH))
    await db.connect()
    try:
        res = await run_study(db, horizons=[int(x) for x in horizons.split(",")],
                              since=parse_ts(since + "T00:00:00Z") if since else None, min_n=min_n)
    finally:
        await db.close()
    path = write_result(res, s.path(s.DATA_DIR) / "quant")
    print(render(res))
    print(f"→ {path}")
    return 0


if __name__ == "__main__":
    import argparse
    import asyncio
    import sys
    p = argparse.ArgumentParser(prog="python -m app.wb.study")
    p.add_argument("--horizons", default="7,28")
    p.add_argument("--since", default=None)
    p.add_argument("--min-n", type=int, default=20)
    a = p.parse_args()
    sys.exit(asyncio.run(_cli(a.horizons, a.since, a.min_n)))
