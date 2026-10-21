# Ported verbatim from the old SubnetTrader app/backtest/validation.py (spec 13 §8.1).
"""Quant validation harness for backtest results.

Three tools, applied to a completed backtest's per-trade series:

  1. ``block_bootstrap_ci``   — moving-block bootstrap confidence intervals on
     expectancy / Sharpe / profit factor / max drawdown. Block (not i.i.d.)
     because trades are serially dependent; i.i.d. would understate the CI.
  2. ``deflated_sharpe_ratio`` — Bailey & López de Prado (2014): the Sharpe
     discounted for the number of configs tried (selection bias) and for
     non-normal returns (skew/kurtosis). DSR > 0.95 ⇒ the edge survives the
     multiple-testing correction.
  3. ``probability_of_ruin``  — fixed-stake Monte Carlo over block-resampled
     returns: can the 10 τ pot blow up given ~2 τ/slot sizing.

Pure functions over a return list — no engine or DB coupling. See
``specs/quant-validation-harness.md``.
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from statistics import NormalDist

_EULER_MASCHERONI = 0.5772156649015329
_NORM = NormalDist()


# ── metric statistics (each maps a per-trade list -> one number) ──────────────

def _expectancy(pnl_pct: list[float]) -> float:
    """Mean per-trade return (%)."""
    return sum(pnl_pct) / len(pnl_pct) if pnl_pct else 0.0


def _sharpe_per_trade(pnl_pct: list[float]) -> float:
    """Per-trade Sharpe = mean/std (sample std, ddof=1) — matches the engine's
    pre-annualization ratio. 0.0 for <2 trades or zero variance."""
    n = len(pnl_pct)
    if n < 2:
        return 0.0
    mean = sum(pnl_pct) / n
    var = sum((r - mean) ** 2 for r in pnl_pct) / (n - 1)
    return mean / math.sqrt(var) if var > 0 else 0.0


def _profit_factor(pnl_tao: list[float]) -> float:
    """Σ wins / |Σ losses| on realized τ. inf when there are no losses."""
    gross_win = sum(p for p in pnl_tao if p > 0)
    gross_loss = abs(sum(p for p in pnl_tao if p <= 0))
    return gross_win / gross_loss if gross_loss > 0 else float("inf")


def _max_drawdown_tao(pnl_tao: list[float]) -> float:
    """Peak-to-trough drawdown (τ) of the cumulative-PnL equity path, in trade
    order. Order-dependent — this is why the bootstrap must preserve blocks."""
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for p in pnl_tao:
        equity += p
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return max_dd


# ── 1. moving-block bootstrap ─────────────────────────────────────────────────

@dataclass
class BootstrapCI:
    metric: str
    point: float          # statistic on the observed sample
    lo: float             # lower percentile (e.g. 2.5%)
    hi: float             # upper percentile (e.g. 97.5%)
    block_len: int
    n_resamples: int


def _block_len_default(n: int) -> int:
    return max(1, round(n ** (1.0 / 3.0))) if n > 0 else 1


def _resample_blocks(series: list, block_len: int, rng: random.Random) -> list:
    """One moving-block bootstrap replicate of ``series`` (same length)."""
    n = len(series)
    n_blocks = math.ceil(n / block_len)
    max_start = n - block_len  # inclusive; overlapping blocks
    out: list = []
    for _ in range(n_blocks):
        start = rng.randint(0, max_start) if max_start > 0 else 0
        out.extend(series[start:start + block_len])
    return out[:n]


def block_bootstrap_ci(
    series: list[float],
    statistic,
    *,
    metric: str = "metric",
    block_len: int | None = None,
    n_resamples: int = 2000,
    alpha: float = 0.05,
    seed: int = 0,
) -> BootstrapCI:
    """Percentile CI for ``statistic`` via the moving-block bootstrap.

    ``series`` is the per-trade input in trade (exit-time) order; ``statistic``
    maps such a list to one number. Degenerate inputs (empty, single, or
    all-equal) yield a zero-width CI at the point estimate rather than raising.
    """
    point = statistic(series)
    n = len(series)
    if n < 2:
        return BootstrapCI(metric, point, point, point, block_len or 1, 0)
    bl = block_len or _block_len_default(n)
    rng = random.Random(seed)
    reps: list[float] = []
    for _ in range(n_resamples):
        val = statistic(_resample_blocks(series, bl, rng))
        if math.isfinite(val):
            reps.append(val)
    if not reps:
        return BootstrapCI(metric, point, point, point, bl, 0)
    reps.sort()
    lo = reps[max(0, int((alpha / 2) * len(reps)))]
    hi = reps[min(len(reps) - 1, int((1 - alpha / 2) * len(reps)))]
    return BootstrapCI(metric, point, lo, hi, bl, len(reps))


# ── 2. deflated Sharpe ratio ──────────────────────────────────────────────────

@dataclass
class DeflatedSharpe:
    sharpe: float          # per-trade SR = μ/σ
    skew: float            # γ3
    kurtosis: float        # γ4 (non-excess; normal = 3)
    n_trades: int
    n_trials: int          # N configs searched
    sr0: float             # expected-max Sharpe under the null (benchmark)
    psr_zero: float        # PSR against SR*=0 (significance ignoring selection)
    dsr: float             # PSR against SR*=SR0 (selection-corrected)


def _skew_kurt(pnl_pct: list[float], mean: float, std: float) -> tuple[float, float]:
    n = len(pnl_pct)
    if std == 0:
        return 0.0, 3.0
    g3 = sum((r - mean) ** 3 for r in pnl_pct) / (n * std ** 3)
    g4 = sum((r - mean) ** 4 for r in pnl_pct) / (n * std ** 4)
    return g3, g4


def _psr(sr: float, sr_star: float, n: int, skew: float, kurt: float) -> float:
    """Probabilistic Sharpe Ratio: P(true SR > sr_star) given the sample."""
    if n < 2:
        return 0.0
    denom = 1.0 - skew * sr + ((kurt - 1.0) / 4.0) * sr ** 2
    if denom <= 0:
        return float("nan")
    z = (sr - sr_star) * math.sqrt(n - 1) / math.sqrt(denom)
    return _NORM.cdf(z)


def expected_max_sharpe(trial_sharpe_variance: float, n_trials: int) -> float:
    """SR0 — expected maximum Sharpe of ``n_trials`` independent zero-edge trials
    (Bailey & López de Prado 2014, eq. for the benchmark Sharpe)."""
    if n_trials <= 1 or trial_sharpe_variance <= 0:
        return 0.0
    z1 = _NORM.inv_cdf(1.0 - 1.0 / n_trials)
    z2 = _NORM.inv_cdf(1.0 - 1.0 / (n_trials * math.e))
    return math.sqrt(trial_sharpe_variance) * (
        (1.0 - _EULER_MASCHERONI) * z1 + _EULER_MASCHERONI * z2
    )


def deflated_sharpe_ratio(
    pnl_pct: list[float],
    *,
    n_trials: int,
    trial_sharpe_variance: float,
) -> DeflatedSharpe:
    """Deflated Sharpe for a config's per-trade returns.

    ``n_trials`` and ``trial_sharpe_variance`` describe the config search the
    winner was drawn from (variance of the per-trade Sharpe across all configs
    tried). DSR = PSR(SR0) is the probability the edge is real after correcting
    for that selection.
    """
    n = len(pnl_pct)
    sr = _sharpe_per_trade(pnl_pct)
    mean = sum(pnl_pct) / n if n else 0.0
    var = sum((r - mean) ** 2 for r in pnl_pct) / (n - 1) if n >= 2 else 0.0
    std = math.sqrt(var)
    skew, kurt = _skew_kurt(pnl_pct, mean, std)
    sr0 = expected_max_sharpe(trial_sharpe_variance, n_trials)
    return DeflatedSharpe(
        sharpe=sr, skew=skew, kurtosis=kurt, n_trades=n, n_trials=n_trials,
        sr0=sr0,
        psr_zero=_psr(sr, 0.0, n, skew, kurt),
        dsr=_psr(sr, sr0, n, skew, kurt),
    )


# ── 3. probability of ruin ────────────────────────────────────────────────────

@dataclass
class RuinResult:
    pot_tao: float
    stake_tao: float
    horizon_trades: int
    n_sims: int
    p_ruin: float                      # P(pot ≤ ruin_floor at any step)
    dd_exceed: dict[int, float] = field(default_factory=dict)  # pct -> P(maxDD≥pct)
    terminal_p5: float = 0.0
    terminal_p50: float = 0.0
    terminal_p95: float = 0.0


def probability_of_ruin(
    pnl_pct: list[float],
    *,
    pot_tao: float = 10.0,
    position_pct: float = 0.20,
    horizon_trades: int | None = None,
    n_sims: int = 5000,
    dd_thresholds: tuple[int, ...] = (30, 50, 70, 90),
    ruin_floor: float = 0.0,
    block_len: int | None = None,
    seed: int = 0,
) -> RuinResult:
    """Fixed-stake Monte-Carlo ruin estimate over block-resampled returns.

    Stake is fixed at ``position_pct · pot_tao`` τ per trade (≈ one slot); each
    resampled trade moves the pot by ``stake · r/100``. Block resampling keeps
    losing streaks clustered (i.i.d. would understate ruin). Reports P(ruin),
    P(maxDD ≥ threshold) and terminal-pot percentiles.
    """
    if not pnl_pct:
        return RuinResult(pot_tao, position_pct * pot_tao, 0, 0, 0.0)
    stake = position_pct * pot_tao
    horizon = horizon_trades or len(pnl_pct)
    bl = block_len or _block_len_default(len(pnl_pct))
    rng = random.Random(seed)

    ruined = 0
    dd_hits = {t: 0 for t in dd_thresholds}
    terminals: list[float] = []
    for _ in range(n_sims):
        # one block-resampled path of `horizon` returns
        path: list[float] = []
        while len(path) < horizon:
            start = rng.randint(0, max(0, len(pnl_pct) - bl))
            path.extend(pnl_pct[start:start + bl])
        path = path[:horizon]

        pot = pot_tao
        peak = pot_tao
        max_dd_pct = 0.0
        hit_ruin = False
        for r in path:
            pot += stake * r / 100.0
            if pot <= ruin_floor:
                hit_ruin = True
                pot = ruin_floor
                break
            peak = max(peak, pot)
            max_dd_pct = max(max_dd_pct, (peak - pot) / peak * 100.0)
        if hit_ruin:
            ruined += 1
            max_dd_pct = 100.0
        for t in dd_thresholds:
            if max_dd_pct >= t:
                dd_hits[t] += 1
        terminals.append(pot)

    terminals.sort()

    def pct(p: float) -> float:
        return terminals[min(len(terminals) - 1, int(p * len(terminals)))]

    return RuinResult(
        pot_tao=pot_tao,
        stake_tao=stake,
        horizon_trades=horizon,
        n_sims=n_sims,
        p_ruin=ruined / n_sims,
        dd_exceed={t: dd_hits[t] / n_sims for t in dd_thresholds},
        terminal_p5=pct(0.05),
        terminal_p50=pct(0.50),
        terminal_p95=pct(0.95),
    )


# ── orchestration ─────────────────────────────────────────────────────────────

@dataclass
class ValidationReport:
    strategy_id: str
    n_trades: int
    cis: list[BootstrapCI]
    deflated: DeflatedSharpe
    ruin: RuinResult


def validate_result(
    result,
    *,
    trial_sharpes: list[float],
    pot_tao: float = 10.0,
    position_pct: float = 0.20,
    n_resamples: int = 2000,
    n_sims: int = 5000,
    seed: int = 0,
) -> ValidationReport:
    """Run all three tools on a ``BacktestResult``.

    ``trial_sharpes`` is the per-trade Sharpe of every config in the search
    universe (used for the deflated-Sharpe N and variance). Returns the
    populated ``ValidationReport``; printing is the caller's job.
    """
    pnl_pct = [t.pnl_pct for t in result.trades]
    pnl_tao = [t.pnl_tao for t in result.trades]

    cis = [
        block_bootstrap_ci(pnl_pct, _expectancy, metric="expectancy %",
                           n_resamples=n_resamples, seed=seed),
        block_bootstrap_ci(pnl_pct, _sharpe_per_trade, metric="sharpe (per-trade)",
                           n_resamples=n_resamples, seed=seed),
        block_bootstrap_ci(pnl_tao, _profit_factor, metric="profit factor",
                           n_resamples=n_resamples, seed=seed),
        block_bootstrap_ci(pnl_tao, _max_drawdown_tao, metric="max drawdown τ",
                           n_resamples=n_resamples, seed=seed),
    ]

    n_trials = len(trial_sharpes)
    if n_trials >= 2:
        m = sum(trial_sharpes) / n_trials
        trial_var = sum((s - m) ** 2 for s in trial_sharpes) / (n_trials - 1)
    else:
        trial_var = 0.0
    deflated = deflated_sharpe_ratio(
        pnl_pct, n_trials=n_trials, trial_sharpe_variance=trial_var
    )

    ruin = probability_of_ruin(
        pnl_pct, pot_tao=pot_tao, position_pct=position_pct,
        n_sims=n_sims, seed=seed,
    )

    return ValidationReport(
        strategy_id=result.strategy_id,
        n_trades=len(result.trades),
        cis=cis,
        deflated=deflated,
        ruin=ruin,
    )
