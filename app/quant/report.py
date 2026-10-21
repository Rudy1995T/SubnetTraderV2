"""Console + JSON output for the harness (spec 13 §3.4–§3.6).

Port of the old SubnetTrader ``app/backtest/factor_report.py:14-160`` (``_fmt_pct``, ``_trade_tao``, ``_max_depth``,
``_depth_label``, ``_span``, ``print_factor_report``, ``_factor_result_dict``, ``save_factor_json``) plus the other
result files. Every time in a file is INTEGER unix s except the top-level ``timestamp`` (02 §B1); non-finite floats
are written as ``null`` (``allow_nan=False`` after sanitising). Files are timestamped, never overwritten.
"""
from __future__ import annotations

import json
import math
import os
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import app.config as app_config
from app.util.time import now_ts, to_iso

from .factor_backtest import FactorResult
from .factor_panel import Panel


def results_dir() -> Path:
    return app_config.settings.path(app_config.settings.QUANT_RESULTS_DIR)


def stamp(ts: int | None = None) -> str:
    return datetime.fromtimestamp(ts if ts is not None else now_ts(), tz=timezone.utc).strftime("%Y%m%d_%H%M%S")


def clean(o: Any) -> Any:
    """JSON-safe copy: dataclasses → dicts, tuples → lists, non-finite floats → None."""
    if is_dataclass(o) and not isinstance(o, type):
        o = asdict(o)
    if isinstance(o, float):
        return o if math.isfinite(o) else None
    if isinstance(o, dict):
        return {str(k): clean(v) for k, v in o.items() if not (isinstance(k, str) and k.startswith("_"))}
    if isinstance(o, (list, tuple, set, frozenset)):
        return [clean(v) for v in o]
    return o


def write_json(path: Path, data: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(clean(data), f, indent=2, allow_nan=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    return path


def result_path(kind: str, tag: str, *, output: str | None = None, scope_suffix: str | None = None,
                ts: int | None = None) -> Path:
    """``--output P`` (``_<scope>`` inserted before the suffix for ``--scope both``) or the default timestamped
    name under ``QUANT_RESULTS_DIR``."""
    if output:
        p = Path(output)
        if scope_suffix:
            p = p.with_name(f"{p.stem}_{scope_suffix}{p.suffix}")
        return p
    return results_dir() / f"{kind}_{tag}_{stamp(ts)}.json"


# ── ported console helpers ─────────────────────────────────────────────────────────────────────────────────────
def _fmt_pct(v: float | None, width: int = 7) -> str:
    if v is None:
        return "—".rjust(width)
    sign = "+" if v > 0 else ""
    return f"{sign}{v * 100:.1f}%".rjust(width)


def _trade_tao(results: dict[str, dict[int, FactorResult]]) -> float | None:
    for by_h in results.values():
        for r in by_h.values():
            if r.trade_tao is not None:
                return r.trade_tao
    return None


def _max_depth(results: dict[str, dict[int, FactorResult]]) -> float:
    for by_h in results.values():
        for r in by_h.values():
            if r.max_depth != float("inf"):
                return r.max_depth
    return float("inf")


def _depth_label(min_depth: float, results: dict[str, dict[int, FactorResult]]) -> str:
    mx = _max_depth(results)
    return f"{min_depth:g}τ" if mx == float("inf") else f"{min_depth:g}..{mx:g}τ (band)"


def _span(panel: Panel) -> str:
    if not panel.dates:
        return "empty"
    lo = datetime.fromtimestamp(panel.dates[0], tz=timezone.utc).date()
    hi = datetime.fromtimestamp(panel.dates[-1], tz=timezone.utc).date()
    return f"{lo} → {hi}"


def print_factor_report(results: dict[str, dict[int, FactorResult]], panel: Panel, *, min_depth: float,
                        lookback: int) -> None:
    print(f"\n{'=' * 78}")
    print("  CROSS-SECTIONAL FACTOR STUDY")
    print(f"{'=' * 78}")
    print(f"  scope={panel.scope}  grid={panel.grid_seconds // 3600}h  span {_span(panel)}  "
          f"netuids={panel.n_netuids}  bars={panel.n_bars:,}")
    print(f"  depth={_depth_label(min_depth, results)}  lookback={lookback} bars")
    trade_tao = _trade_tao(results)
    if trade_tao is not None:
        print(f"  net deciles: round-trip AMM cost on {trade_tao:g}τ/name (nD1/nD10/n(LS))")
    print()
    for fname, by_h in results.items():
        print(f"  ── factor: {fname} {'─' * max(1, 60 - len(fname))}")
        net_hdr = f" {'nD1':>8} {'nD10':>8} {'n(LS)':>8}" if trade_tao is not None else ""
        print(f"    {'h':>3} {'N':>4} {'names':>6} {'meanIC':>8} {'IC_IR':>7} {'t':>6} {'%pos':>6} {'D1':>8} "
              f"{'D10':>8} {'D10-D1':>8} {'t(LS)':>6}{net_hdr}")
        for h, r in sorted(by_h.items()):
            ic, dec = r.ic, r.deciles
            gap_h = r.median_gap_seconds / 3600.0
            warn = ""
            if gap_h > r.step * (panel.grid_seconds / 3600.0) * 1.5 and r.label.startswith("ret_log"):
                warn = f"  ⚠ sparse (median gap {gap_h:.0f}h)"
            net = ""
            if r.deciles_net is not None:
                nd = r.deciles_net
                net = (f" {_fmt_pct(nd.bottom_decile_mean)} {_fmt_pct(nd.top_decile_mean)} "
                       f"{_fmt_pct(nd.mean_long_short)}")
            print(f"    {h:>3} {ic.n_dates:>4} {ic.avg_names:>6.0f} {ic.mean_ic:>+8.3f} {ic.ic_ir:>+7.2f} "
                  f"{ic.t_stat:>+6.1f} {ic.pct_positive * 100:>5.0f}% {_fmt_pct(dec.bottom_decile_mean)} "
                  f"{_fmt_pct(dec.top_decile_mean)} {_fmt_pct(dec.mean_long_short)} {dec.spread_t_stat:>+6.1f}"
                  f"{net}{warn}")
        first_h = sorted(by_h)[0]
        curve = by_h[first_h].deciles.decile_curve
        if curve:
            print(f"    decile curve (h={first_h}, D1..D{len(curve)}): {' '.join(_fmt_pct(c, 6) for c in curve)}\n")
        else:
            print()


def summary_lines(results: dict[str, dict[int, FactorResult]], scope: str) -> list[str]:
    out = []
    for f, by_h in results.items():
        for h, r in sorted(by_h.items()):
            out.append(f"{scope:<12} {f:<28} {r.label:<16} n_dates={r.ic.n_dates:<4} mean_ic={r.ic.mean_ic:+.4f} "
                       f"t={r.ic.t_stat:+.2f} names={r.ic.avg_names:.0f}" + ("  insufficient" if r.insufficient_data
                                                                               else ""))
    return out


def _factor_result_dict(r: FactorResult) -> dict:
    d = asdict(r)
    if r.max_depth == float("inf"):
        d["max_depth"] = None
    d["ic"].pop("ic_series", None)
    d["ic"].pop("ic_dates", None)
    d["deciles"]["bottom_decile_mean"] = r.deciles.bottom_decile_mean
    d["deciles"]["top_decile_mean"] = r.deciles.top_decile_mean
    if r.deciles_net is not None:
        d["deciles_net"]["bottom_decile_mean"] = r.deciles_net.bottom_decile_mean
        d["deciles_net"]["top_decile_mean"] = r.deciles_net.top_decile_mean
    return d


def factor_payload(results: dict[str, dict[int, FactorResult]], panel: Panel, *, min_depth: float, lookback: int,
                   warnings: list[str], coverage: dict, provenance: dict, deflated: dict | None = None,
                   ts: int | None = None) -> dict:
    out = {
        "timestamp": to_iso(ts if ts is not None else now_ts()), "scope": panel.scope,
        "grid_seconds": panel.grid_seconds, "span": _span(panel), "n_netuids": panel.n_netuids,
        "n_bars": panel.n_bars, "min_depth_tao": min_depth, "lookback": lookback,
        "insufficient_data": any(r.insufficient_data for by_h in results.values() for r in by_h.values()),
        "warnings": list(warnings), "coverage": coverage,
        "factors": {f: {str(h): _factor_result_dict(r) for h, r in by_h.items()} for f, by_h in results.items()},
    }
    if deflated is not None:
        out["deflated"] = deflated
    out["provenance"] = provenance
    return out


def ic_series_payload(results: dict[str, dict[int, FactorResult]]) -> dict:
    out: dict = {f: {str(h): r.ic.ic_series for h, r in by_h.items()} for f, by_h in results.items()}
    out["dates"] = {f: {str(h): r.ic.ic_dates for h, r in by_h.items()} for f, by_h in results.items()}
    return out


def save_factor_json(payload: dict, results: dict[str, dict[int, FactorResult]], path: Path) -> list[Path]:
    """The factor file in the old explorer shape plus its ``ic_series_<scope>_<ts>.json`` sidecar."""
    write_json(path, payload)
    name = path.name.replace("factor_", "ic_series_", 1) if path.name.startswith("factor_") \
        else f"ic_series_{path.name}"
    side = write_json(path.with_name(name), ic_series_payload(results))
    return [path, side]
