# Ported verbatim from the old SubnetTrader app/backtest/slippage.py (spec 13 §8.1).
"""
Simple slippage model for backtest simulation.

Uses constant-product AMM estimate: slippage ~ trade_size / pool_depth * factor.
"""
from __future__ import annotations

# Conservative multiplier for constant-product estimate
SLIPPAGE_FACTOR = 2.0
MAX_SLIPPAGE_PCT = 5.0


def estimate_entry_slippage(
    trade_tao: float,
    pool_tao: float,
    slippage_factor: float = SLIPPAGE_FACTOR,
    max_slippage_pct: float = MAX_SLIPPAGE_PCT,
) -> float:
    """Estimate entry slippage as a percentage.

    Args:
        trade_tao: TAO amount being spent.
        pool_tao: Total TAO in the pool.

    Returns:
        Slippage percentage (0-max_slippage_pct).
    """
    if pool_tao <= 0:
        return max_slippage_pct
    raw = (trade_tao / pool_tao) * slippage_factor * 100.0
    return min(raw, max_slippage_pct)


def estimate_exit_slippage(
    alpha_value_tao: float,
    pool_tao: float,
    slippage_factor: float = SLIPPAGE_FACTOR,
    max_slippage_pct: float = MAX_SLIPPAGE_PCT,
) -> float:
    """Estimate exit slippage as a percentage.

    Args:
        alpha_value_tao: TAO value of alpha tokens being sold.
        pool_tao: Total TAO in the pool.

    Returns:
        Slippage percentage (0-max_slippage_pct).
    """
    if pool_tao <= 0:
        return max_slippage_pct
    raw = (alpha_value_tao / pool_tao) * slippage_factor * 100.0
    return min(raw, max_slippage_pct)


def apply_entry_slippage(price: float, slippage_pct: float) -> float:
    """Return effective entry price after slippage (higher = worse)."""
    return price * (1.0 + slippage_pct / 100.0)


def apply_exit_slippage(price: float, slippage_pct: float) -> float:
    """Return effective exit price after slippage (lower = worse)."""
    return price * (1.0 - slippage_pct / 100.0)
