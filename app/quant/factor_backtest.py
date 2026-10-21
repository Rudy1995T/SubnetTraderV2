"""Cross-sectional factor study on the death-inclusive panel (spec 13 §2.3, §8.5).

Port of the old SubnetTrader ``app/backtest/factor_backtest.py``: the chain factor functions (``factor_quality`` and
``factor_net_flow_depth`` dropped), ``_solve``/``residualize``, ``compute_ic``/``compute_deciles``, the net-of-cost
deciles and ``deflate_factors``. Extensions: PIT factors read from ``Bar.pit``, injected labels (``label_fn``), simple
returns carried for netting (``raw_fwds``), drop counters, hole-date skipping, per-regime IC, and the deflation fix
(trial-log variance + ``orient``). Pure stats over a :class:`Panel`; no DB.
"""
from __future__ import annotations

import math
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable

from app.util.mathutil import monotone_score, quantile_buckets, spearman_r, zscore

from .backtestability import FactorNameError, classify, resolve_control  # noqa: F401 (re-export)
from .economics import EconomicsConfig, net_round_trip
from .factor_panel import Bar, Panel, block_to_unix, forward_return
from .labels import LabelSource, make_label, ret_log_label_fn, simple_return_fn
from .slippage import estimate_entry_slippage, estimate_exit_slippage

FactorFn = Callable[[list[Bar], int, int], "float | None"]
INF = float("inf")


# ── Factor functions: (series, i, k) -> float | None (ported, factor_backtest.py:63-203) ─────────────────────────
def _in_segment_window(series: list[Bar], i: int, k: int) -> list[Bar] | None:
    """The ``k+1`` bars ending at ``i`` iff they are all in one segment."""
    if k <= 0 or i - k < 0:
        return None
    if series[i - k].seg != series[i].seg:
        return None
    return series[i - k: i + 1]


def factor_momentum(series: list[Bar], i: int, k: int) -> float | None:
    """Trailing K-bar log return."""
    w = _in_segment_window(series, i, k)
    if w is None:
        return None
    p0, p1 = w[0].price, w[-1].price
    if p0 <= 0 or p1 <= 0:
        return None
    return math.log(p1 / p0)


def factor_size(series: list[Bar], i: int, k: int) -> float | None:
    """Log pool depth (τ) — the liquidity/size control."""
    d = series[i].depth
    return math.log(d) if d > 0 else None


def factor_meanrev_z(series: list[Bar], i: int, k: int) -> float | None:
    """Negated z-score of the current price within its trailing K-bar window (high = oversold)."""
    w = _in_segment_window(series, i, k)
    if w is None:
        return None
    prices = [b.price for b in w]
    mean = sum(prices) / len(prices)
    sd = statistics.pstdev(prices)
    if sd == 0:
        return None
    return -(prices[-1] - mean) / sd


def factor_flow_z(series: list[Bar], i: int, k: int) -> float | None:
    """Latest per-bar Δdepth% standardised against the trailing K-bar distribution of Δdepth%."""
    w = _in_segment_window(series, i, k)
    if w is None:
        return None
    depths = [b.depth for b in w]
    if any(d <= 0 for d in depths):
        return None
    deltas = [(depths[j] - depths[j - 1]) / depths[j - 1] for j in range(1, len(depths))]
    if len(deltas) < 2:
        return None
    return zscore(deltas)[-1]


def _reg_unix(bar: Bar) -> float | None:
    """Exact registration time stamped by the panel builder (``pit['_reg_ts']``), else the block anchor."""
    v = bar.pit.get("_reg_ts")
    if v is not None:
        return float(v)
    return block_to_unix(bar.reg_block) if bar.reg_block > 0 else None


def factor_age(series: list[Bar], i: int, k: int) -> float | None:
    """Occupant age in days; None when unknown or for bars before the occupant's registration."""
    bar = series[i]
    if bar.reg_block <= 0 and "_reg_ts" not in bar.pit:
        return None
    reg = _reg_unix(bar)
    if reg is None:
        return None
    age_s = bar.bucket - reg
    if age_s <= 0:
        return None
    return age_s / 86400.0


def factor_volume(series: list[Bar], i: int, k: int) -> float | None:
    """Log bucket turnover (Δ subnet_volume sum, τ); None when unusable."""
    v = series[i].volume
    return math.log(v) if v > 0 else None


def factor_volume_surge(series: list[Bar], i: int, k: int) -> float | None:
    """Latest per-bar Δvolume% standardised against the trailing K-bar distribution."""
    w = _in_segment_window(series, i, k)
    if w is None:
        return None
    vols = [b.volume for b in w]
    if any(v <= 0 for v in vols):
        return None
    deltas = [(vols[j] - vols[j - 1]) / vols[j - 1] for j in range(1, len(vols))]
    if len(deltas) < 2:
        return None
    return zscore(deltas)[-1]


def factor_px_vs_moving(series: list[Bar], i: int, k: int) -> float | None:
    """Log spread of spot over the chain's moving price."""
    mp = series[i].moving_price
    p = series[i].price
    if mp <= 0 or p <= 0:
        return None
    return math.log(p / mp)


def factor_moving_px_trend(series: list[Bar], i: int, k: int) -> float | None:
    """Trailing K-bar log trend of the chain's moving price."""
    w = _in_segment_window(series, i, k)
    if w is None:
        return None
    mp_now, mp_then = w[-1].moving_price, w[0].moving_price
    if mp_now <= 0 or mp_then <= 0:
        return None
    return math.log(mp_now / mp_then)


_CHAIN: dict[str, FactorFn] = {
    "momentum": factor_momentum,
    "flow_z": factor_flow_z,
    "meanrev_z": factor_meanrev_z,
    "size": factor_size,
    "age": factor_age,
    "volume": factor_volume,
    "volume_surge": factor_volume_surge,
    "px_vs_moving": factor_px_vs_moving,
    "moving_px_trend": factor_moving_px_trend,
}
FACTOR_FUNCS = _CHAIN      # ported name: the chain universe (no net_flow_depth, no static quality)


def chain_factors() -> dict[str, FactorFn]:
    return dict(_CHAIN)


def make_pit_factor(name: str) -> FactorFn:
    return lambda bars, i, k: bars[i].pit.get(name)


def quality_cg_value(p1: float | None, p2: float | None) -> float | None:
    """``(30·P1 + 20·P2)/50`` when both, the sole non-None pillar when one is None (13 §2.3)."""
    if p1 is not None and p2 is not None:
        return (30.0 * p1 + 20.0 * p2) / 50.0
    return p1 if p1 is not None else p2


def make_quality_cg() -> FactorFn:
    return lambda bars, i, k: quality_cg_value(bars[i].pit.get("P1"), bars[i].pit.get("P2"))


def resolve_factor(name: str, panel: Panel) -> FactorFn:
    """chain | quality_cg | PIT (must be stamped on the panel, else KeyError); retired/non-factor → ValueError."""
    kind = classify(name)          # raises FactorNameError (a ValueError)
    if kind == "chain":
        return _CHAIN[name]
    if kind == "quality_cg":
        if not {"P1", "P2"} <= set(panel.pit_keys):
            raise KeyError("quality_cg needs P1,P2 stamped on the panel")
        return make_quality_cg()
    if name not in panel.pit_keys:
        raise KeyError(f"{name} not stamped on the panel (pit_keys={list(panel.pit_keys)})")
    return make_pit_factor(name)


# ── Cross-section assembly + aggregation ────────────────────────────────────────────────────────────────────────
@dataclass
class CrossSection:
    date: int
    netuids: list[int]
    factors: list[float]
    fwds: list[float]
    depths: list[float] = field(default_factory=list)
    raw_fwds: list[float] = field(default_factory=list)   # simple returns (netting input); empty = fwds are simple


@dataclass
class ICResult:
    n_dates: int = 0
    mean_ic: float = 0.0
    std_ic: float = 0.0
    ic_ir: float = 0.0
    t_stat: float = 0.0
    pct_positive: float = 0.0
    avg_names: float = 0.0
    ic_series: list[float] = field(default_factory=list)
    ic_dates: list[int] = field(default_factory=list)


@dataclass
class DecileResult:
    n_buckets: int = 10
    decile_curve: list[float | None] = field(default_factory=list)
    mean_long_short: float = 0.0
    spread_t_stat: float = 0.0
    n_spread_dates: int = 0

    @property
    def bottom_decile_mean(self) -> float | None:
        return self.decile_curve[0] if self.decile_curve else None

    @property
    def top_decile_mean(self) -> float | None:
        return self.decile_curve[-1] if self.decile_curve else None


@dataclass
class FactorResult:
    factor: str
    horizon: int
    lookback: int
    min_depth: float
    step: int
    ic: ICResult
    deciles: DecileResult
    median_gap_seconds: float = 0.0
    deciles_net: DecileResult | None = None
    trade_tao: float | None = None
    max_depth: float = INF
    label: str = ""
    pit_mode: str = "strict"
    controls: list[str] = field(default_factory=list)
    overlapping: bool = False
    monotone_score: float = 0.0
    by_regime: dict = field(default_factory=dict)
    n_none_factor: int = 0
    n_none_label: int = 0
    n_dropped_controls: int = 0
    n_hole_dates: int = 0
    insufficient_data: bool = False


def _solve(a: list[list[float]], b: list[float]) -> list[float] | None:
    """Gauss-Jordan solve of ``a x = b`` with partial pivoting; None if singular."""
    k = len(b)
    m = [row[:] + [b[i]] for i, row in enumerate(a)]
    for col in range(k):
        piv = max(range(col, k), key=lambda r: abs(m[r][col]))
        if abs(m[piv][col]) < 1e-12:
            return None
        m[col], m[piv] = m[piv], m[col]
        pv = m[col][col]
        m[col] = [v / pv for v in m[col]]
        for r in range(k):
            if r != col and m[r][col] != 0.0:
                f = m[r][col]
                m[r] = [v - f * m[col][j] for j, v in enumerate(m[r])]
    return [m[i][k] for i in range(k)]


def residualize(y: list[float], controls: list[list[float]]) -> list[float]:
    """Cross-sectional OLS residuals of ``y`` on ``[1, *controls]``; singular design → ``y`` unchanged."""
    n = len(y)
    cols = [[1.0] * n] + [list(c) for c in controls]
    k = len(cols)
    xtx = [[sum(cols[a][i] * cols[b][i] for i in range(n)) for b in range(k)] for a in range(k)]
    xty = [sum(cols[a][i] * y[i] for i in range(n)) for a in range(k)]
    beta = _solve(xtx, xty)
    if beta is None:
        return list(y)
    return [y[i] - sum(beta[a] * cols[a][i] for a in range(k)) for i in range(n)]


def build_cross_sections(panel: Panel, factor_fn: FactorFn, *, horizon: int, lookback: int, min_depth: float,
                         min_names: int, step: int, controls: list | None = None, max_depth: float = INF,
                         label_fn=None, raw_fn=None, hole_dates: set[int] | None = None,
                         counters: dict[str, int] | None = None) -> tuple[list[CrossSection], list[int]]:
    """One pass over the rebalance grid → eligible ``(netuid, factor, label)`` triples per date + realised forward
    gaps. ``label_fn(panel, nu, i)`` defaults to the ported simple ``forward_return``; ``raw_fn`` (simple returns)
    feeds the net deciles. ``counters`` (if given) receives ``n_none_factor/n_none_label/n_dropped_controls/
    n_hole_dates``. Names are visited netuid-sorted (deterministic)."""
    ctrls = controls or []
    label = label_fn or (lambda p, nu, i: forward_return(p, nu, i, horizon))
    c = counters if counters is not None else {}
    for key in ("n_none_factor", "n_none_label", "n_dropped_controls", "n_hole_dates"):
        c.setdefault(key, 0)
    sections: list[CrossSection] = []
    gaps: list[int] = []
    netuids = sorted(panel.series)
    for t in panel.dates[::step]:
        if hole_dates and t in hole_dates:
            c["n_hole_dates"] += 1
            continue
        nus: list[int] = []
        fs: list[float] = []
        frs: list[float] = []
        ds: list[float] = []
        raws: list[float] = []
        cvecs: list[list[float]] = [[] for _ in ctrls]
        for nu in netuids:
            bars = panel.series[nu]
            i = panel.index_of[nu].get(t)
            if i is None or bars[i].depth < min_depth or bars[i].depth > max_depth:
                continue
            fv = factor_fn(bars, i, lookback)
            if fv is None:
                c["n_none_factor"] += 1
                continue
            cvals = [cf(bars, i, lookback) for cf in ctrls]
            if any(cv is None for cv in cvals):
                c["n_dropped_controls"] += 1
                continue
            fr = label(panel, nu, i)
            if fr is None:
                c["n_none_label"] += 1
                continue
            rf = raw_fn(panel, nu, i) if raw_fn is not None else None
            nus.append(nu)
            fs.append(float(fv))
            frs.append(float(fr))
            ds.append(bars[i].depth)
            if raw_fn is not None:
                raws.append(float(rf) if rf is not None else math.exp(float(fr)) - 1.0)
            for kk, cv in enumerate(cvals):
                cvecs[kk].append(float(cv))
            tgt = min(i + horizon, panel.seg_end[nu][i])
            if tgt > i:
                gaps.append(bars[tgt].bucket - bars[i].bucket)
        if len(nus) >= min_names:
            if ctrls:
                fs = residualize(fs, cvecs)
            sections.append(CrossSection(t, nus, fs, frs, ds, raws))
    return sections, gaps


def compute_ic(sections: list[CrossSection]) -> ICResult:
    ics = [spearman_r(s.factors, s.fwds) for s in sections]
    n = len(ics)
    if n == 0:
        return ICResult()
    mean_ic = statistics.mean(ics)
    std_ic = statistics.stdev(ics) if n > 1 else 0.0
    ir = mean_ic / std_ic if std_ic > 0 else 0.0
    return ICResult(n_dates=n, mean_ic=mean_ic, std_ic=std_ic, ic_ir=ir, t_stat=ir * math.sqrt(n),
                    pct_positive=sum(1 for x in ics if x > 0) / n,
                    avg_names=statistics.mean(len(s.netuids) for s in sections), ic_series=ics,
                    ic_dates=[s.date for s in sections])


def compute_deciles(sections: list[CrossSection], n_buckets: int = 10) -> DecileResult:
    per_date_decile: list[list[float]] = [[] for _ in range(n_buckets)]
    spreads: list[float] = []
    for s in sections:
        if len(s.factors) < n_buckets:
            continue
        buckets = quantile_buckets(s.factors, n_buckets)
        grouped: dict[int, list[float]] = defaultdict(list)
        for b, fr in zip(buckets, s.fwds):
            grouped[b].append(fr)
        date_means = {b: statistics.mean(v) for b, v in grouped.items()}
        for b in range(n_buckets):
            if b in date_means:
                per_date_decile[b].append(date_means[b])
        if 0 in date_means and (n_buckets - 1) in date_means:
            spreads.append(date_means[n_buckets - 1] - date_means[0])
    curve = [statistics.mean(vals) if vals else None for vals in per_date_decile]
    mean_spread = statistics.mean(spreads) if spreads else 0.0
    spread_t = 0.0
    if len(spreads) > 1:
        sd = statistics.stdev(spreads)
        if sd > 0:
            spread_t = mean_spread / (sd / math.sqrt(len(spreads)))
    return DecileResult(n_buckets=n_buckets, decile_curve=curve, mean_long_short=mean_spread,
                        spread_t_stat=spread_t, n_spread_dates=len(spreads))


def _net_return(gross: float, depth: float, trade_tao: float, economics: EconomicsConfig | None = None) -> float:
    """Round-trip return after execution cost on both legs of a ``trade_tao`` position (entry-bar depth both legs)."""
    if economics is not None and economics.enabled:
        if trade_tao <= 0:
            return 0.0
        pnl_tao, _, _ = net_round_trip(1.0, 1.0 + gross, depth, trade_tao, hold_hours=0.0, emission_rate=None,
                                       cfg=economics)
        return pnl_tao / trade_tao
    s_in = estimate_entry_slippage(trade_tao, depth)
    exit_value = trade_tao * (1.0 + gross)
    s_out = estimate_exit_slippage(max(exit_value, 0.0), depth)
    return (1.0 + gross) * (1.0 - s_out / 100.0) / (1.0 + s_in / 100.0) - 1.0


def compute_deciles_net(sections: list[CrossSection], trade_tao: float, n_buckets: int = 10,
                        economics: EconomicsConfig | None = None,
                        transform: Callable[[float], float] | None = None) -> DecileResult:
    """Deciles on returns net of round-trip cost (ranking unchanged). With ``raw_fwds`` the simple returns are netted
    and ``transform`` (e.g. the ``ret_log`` label) re-applied (13 §8.5)."""
    net_sections = []
    for s in sections:
        src = s.raw_fwds if s.raw_fwds else s.fwds
        nets = [_net_return(fr, d, trade_tao, economics) for fr, d in zip(src, s.depths)]
        if s.raw_fwds and transform is not None:
            nets = [transform(x) for x in nets]
        net_sections.append(CrossSection(s.date, s.netuids, s.factors, nets, s.depths))
    return compute_deciles(net_sections, n_buckets)


def _ic_stats(ics: list[float]) -> dict:
    n = len(ics)
    if n == 0:
        return {"n_dates": 0, "mean_ic": None, "t_stat": None}
    m = statistics.mean(ics)
    sd = statistics.stdev(ics) if n > 1 else 0.0
    return {"n_dates": n, "mean_ic": m, "t_stat": (m / sd * math.sqrt(n)) if sd > 0 else 0.0}


def by_regime(sections: list[CrossSection], regime_of: dict[int, dict[str, str]]) -> dict:
    """Per-regime IC (diagnostic; 13 §2.6). Missing tags are ``UNKNOWN`` (market) / ``unknown`` (emission)."""
    em: dict[str, list[float]] = defaultdict(list)
    mk: dict[str, list[float]] = defaultdict(list)
    mk_src: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    src_n: dict[str, int] = {"backfill": 0, "live": 0}
    for s in sections:
        ic = spearman_r(s.factors, s.fwds)
        tag = regime_of.get(s.date, {})
        em[tag.get("emission", "unknown")].append(ic)
        lbl = tag.get("market", "UNKNOWN")
        src = tag.get("market_source", "missing")
        mk[lbl].append(ic)
        mk_src[src][lbl].append(ic)
        src_n[src] = src_n.get(src, 0) + 1
    return {"emission": {k: _ic_stats(v) for k, v in sorted(em.items())},
            "market": {k: _ic_stats(v) for k, v in sorted(mk.items())},
            "market_source": src_n,
            "market_by_source": {s: {k: _ic_stats(v) for k, v in sorted(d.items())} for s, d in sorted(mk_src.items())}}


def run_factor_study(panel: Panel, *, factors: list[str], horizons: list[int], label_family: str = "ret_log",
                     lookback: int = 6, min_depth: float = 3000.0, min_names: int = 10, step: int | None = None,
                     trade_tao: float | None = None, controls: list[str] | None = None, max_depth: float = INF,
                     economics: EconomicsConfig | None = None, label_source: LabelSource | None = None,
                     hole_dates: set[int] | None = None, death_floor: float = 0.01, pit_mode: str = "strict",
                     min_ic_dates: int = 10) -> dict[str, dict[int, FactorResult]]:
    """Every (factor × horizon) over a panel. ``horizons`` are bars for ``ret_log`` and days otherwise; ``step=None``
    = non-overlapping. ``controls`` residualise each factor (its own name auto-excluded; ``emission_share`` →
    ``chain:emission_share``)."""
    results: dict[str, dict[int, FactorResult]] = {}
    for fname in factors:
        fn = resolve_factor(fname, panel)
        ctrl_names = []
        for c in controls or []:
            rc = resolve_control(c)
            if c == fname or rc == fname or rc in ctrl_names:
                continue
            ctrl_names.append(rc)
        ctrl_fns = [resolve_factor(c, panel) for c in ctrl_names]
        results[fname] = {}
        for h in horizons:
            lab = make_label(label_family, h)
            base_step = lab.step_bars(panel.grid_seconds)
            step_h = step if step else base_step
            if label_family == "ret_log":
                label_fn = label_source.label_fn(lab) if label_source else ret_log_label_fn(h, death_floor)
                raw_fn = simple_return_fn(h)
            else:
                if label_source is None:
                    raise ValueError(f"label {lab.id} needs a LabelSource")
                label_fn, raw_fn = label_source.label_fn(lab), None
            counters: dict[str, int] = {}
            sections, gaps = build_cross_sections(
                panel, fn, horizon=base_step, lookback=lookback, min_depth=min_depth, min_names=min_names,
                step=step_h, controls=ctrl_fns, max_depth=max_depth, label_fn=label_fn, raw_fn=raw_fn,
                hole_dates=hole_dates, counters=counters)
            ic = compute_ic(sections)
            dec = compute_deciles(sections)
            net = None
            if trade_tao and raw_fn is not None:
                net = compute_deciles_net(sections, trade_tao, economics=economics,
                                          transform=lambda r: math.log(max(1.0 + r, death_floor)))
            results[fname][h] = FactorResult(
                factor=fname, horizon=h, lookback=lookback, min_depth=min_depth, step=step_h, ic=ic, deciles=dec,
                median_gap_seconds=statistics.median(gaps) if gaps else 0.0, deciles_net=net,
                trade_tao=trade_tao, max_depth=max_depth, label=lab.id, pit_mode=pit_mode, controls=ctrl_names,
                overlapping=step_h < base_step, monotone_score=monotone_score(dec.decile_curve),
                by_regime=by_regime(sections, panel.regime_of), insufficient_data=ic.n_dates < min_ic_dates,
                **counters)
    return results


def deflate_factors(results: dict[str, dict[int, FactorResult]], *, horizon: int, n_trials: int | None = None,
                    trial_sharpe_variance: float | None = None, orient: bool = True) -> dict:
    """Deflated Sharpe of each factor's IC series at ``horizon`` (Bailey–López de Prado).

    Review fix (13 §8.5): ``trial_sharpe_variance`` (from the trial log) overrides the in-run variance, which is 0
    with one factor and silently ignores N; ``orient=False`` keeps a negative-mean series negative (the gate's
    pre-registered direction is IC > 0).
    """
    from .validation import deflated_sharpe_ratio

    per = {f: by_h[horizon] for f, by_h in results.items() if horizon in by_h}
    if trial_sharpe_variance is None:
        irs = [abs(r.ic.ic_ir) for r in per.values() if r.ic.ic_series]
        tsv = statistics.pvariance(irs) if len(irs) > 1 else 0.0
    else:
        tsv = float(trial_sharpe_variance)
    n = n_trials or len(per)
    out = {}
    for f, r in per.items():
        series = r.ic.ic_series
        if len(series) < 2:
            continue
        if orient and statistics.mean(series) < 0:
            series = [-x for x in series]
        out[f] = deflated_sharpe_ratio(series, n_trials=n, trial_sharpe_variance=tsv)
    return out

