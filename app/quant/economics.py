# Ported verbatim from the old SubnetTrader app/backtest/economics.py (spec 13 §8.1).
"""Realistic backtest economics: real swap fee + constant-product (convex,
asymmetric) price impact + staking-emission carry.

The constant-product math mirrors the LIVE executor's ``_constant_product_swap``
(``app/chain/executor.py:415``) so the backtest charges what the chain charges.
Gated by ``EconomicsConfig.enabled`` — when False, callers keep using
``slippage.py`` and every pre-existing result reproduces byte-for-byte.

Why no alpha-reserve plumbing
-----------------------------
A dTAO pool's spot price *is* its reserve ratio (``price = tao_in / alpha_in``).
Written as a **multiplier on spot**, the alpha reserve cancels on both legs, so
the realized fill price is a pure function of ``(spot, tao_depth, notional_τ,
fee)`` — every quantity the backtest already carries. With ``f = fee_rate``,
``x = tao_in_pool`` (τ depth) and a trade whose τ notional at spot is ``N``::

    buy  (spend N τ):  m_buy  = (1 + N·(1−f)/x) / (1−f)        # ≥ 1/(1−f); avg price
                                                                #   LINEAR in N
    sell (alpha worth N τ):
                       m_sell = (1−f) / (1 + (N/x)·(1−f))       # ≤ (1−f); convex in N
    effective_entry_price = spot · m_buy   (≥ spot)
    effective_exit_price  = spot · m_sell  (≤ spot)

The asymmetry is genuine: a constant-product buy's *average* fill price is linear
in input τ (avg = spot·(1 + dx/x)), while the sell's is convex (hyperbolic). The
total round-trip τ cost is super-linear in size — bigger trades pay a higher rate,
which is what flips thin-cap capacity verdicts and the linear-capped slippage model
cannot represent.

The fee is embedded in the realized price exactly as the chain charges it (so the
curve matches the executor oracle *and* a real live fill — what spec #1
calibrates against). ``swap_fee_tao`` is an attribution helper that reports the
fee portion; it is **not** subtracted from PnL a second time.

``impact_scale`` (default 1.0) scales the pure price-impact deviation only,
keeping the fee exact — ``1.0`` = raw AMM prediction, ``0.0`` = fee-only. Spec #1
fits it from live fills.

See ``specs/feature-backtest-total-return-economics.md``.
"""
from __future__ import annotations

from dataclasses import dataclass

HOURS_PER_YEAR = 8760.0


@dataclass(frozen=True)
class EconomicsConfig:
    enabled: bool = False             # master toggle (--realistic-economics)
    fee_rate: float = 0.003           # 0.3% = live executor fee_bps=30 (executor.py:427)
    impact_scale: float = 1.0         # calibration knob — FITTED by spec #1's live fills
    carry_model: str = "off"          # "off" | "flat_apr" | "per_bar"
    carry_apr: float = 0.0            # used iff carry_model == "flat_apr"


# ── Alpha-free effective prices (the convex, asymmetric core; engines call these) ──

def effective_entry_price(
    spot: float, tao_in_pool: float, tao_spent: float, cfg: EconomicsConfig
) -> float:
    """Realized buy price = ``spot · m_buy`` (≥ spot; convex in size; fee embedded).

    ``tao_spent`` is the τ notional bought. Returns ``spot`` unchanged when the
    pool depth or trade size is non-positive (no impact to model).
    """
    f = cfg.fee_rate
    if spot <= 0 or tao_in_pool <= 0 or tao_spent <= 0 or f >= 1.0:
        return spot
    impact_factor = 1.0 + tao_spent * (1.0 - f) / tao_in_pool       # ≥ 1
    scaled_impact = 1.0 + cfg.impact_scale * (impact_factor - 1.0)
    fee_factor = 1.0 / (1.0 - f)
    return spot * fee_factor * scaled_impact


def effective_exit_price(
    spot: float, tao_in_pool: float, notional_tao: float, cfg: EconomicsConfig
) -> float:
    """Realized sell price = ``spot · m_sell`` (≤ spot; convex in size; fee embedded).

    ``notional_tao`` is the spot-τ value of the alpha being sold. Returns ``spot``
    unchanged when the pool depth or notional is non-positive.

    Warning: the sell price decays hyperbolically toward 0 as ``notional_tao``
    approaches and exceeds ``tao_in_pool`` (≈40 % of spot at notional = depth,
    ≈2 % at 5× depth). This is correct constant-product behaviour — oversized
    positions *should* be crushed — so callers must keep size below the depth
    floor (the engine force-exits below ``min_pool_depth_tao``; the factor path
    floors at ``min_depth``).
    """
    f = cfg.fee_rate
    if spot <= 0 or tao_in_pool <= 0 or notional_tao <= 0 or f >= 1.0:
        return spot
    impact_factor = 1.0 / (1.0 + (notional_tao / tao_in_pool) * (1.0 - f))   # ≤ 1
    scaled_impact = 1.0 - cfg.impact_scale * (1.0 - impact_factor)
    fee_factor = 1.0 - f
    return spot * fee_factor * scaled_impact


# ── Constant-product curve — reserve form (the executor oracle anchor) ────────────

def cp_buy_alpha_out(
    tao_in_pool: float, alpha_in_pool: float, tao_spent: float, cfg: EconomicsConfig
) -> float:
    """Alpha received for spending ``tao_spent`` τ. Mirrors executor.py:431-437
    (buy leg). Defined via ``effective_entry_price`` at the pool's own spot so the
    two are identical by construction; used by tests as the executor oracle."""
    if tao_in_pool <= 0 or alpha_in_pool <= 0:
        return 0.0
    spot = tao_in_pool / alpha_in_pool
    eff = effective_entry_price(spot, tao_in_pool, tao_spent, cfg)
    return tao_spent / eff if eff > 0 else 0.0


def cp_sell_tao_out(
    tao_in_pool: float, alpha_in_pool: float, alpha_sold: float, cfg: EconomicsConfig
) -> float:
    """TAO received for selling ``alpha_sold`` alpha. Mirrors executor.py:438-444
    (sell leg). NOT the mirror of the buy — the curve is asymmetric."""
    if tao_in_pool <= 0 or alpha_in_pool <= 0 or alpha_sold <= 0:
        return 0.0
    spot = tao_in_pool / alpha_in_pool
    notional = alpha_sold * spot
    eff = effective_exit_price(spot, tao_in_pool, notional, cfg)
    return alpha_sold * eff


# ── Fee attribution + emission carry ──────────────────────────────────────────────

def swap_fee_tao(notional_tao: float, cfg: EconomicsConfig) -> float:
    """ATTRIBUTION ONLY: ``fee_rate × notional``. The fee is already embedded in the
    effective price (above); this reports the fee portion for ``total_fees_tao`` and
    is **not** subtracted from PnL a second time. Replaces flow's flat 0.0006 τ."""
    return cfg.fee_rate * notional_tao


def accrue_carry(
    amount_tao: float,
    hold_hours: float,
    emission_rate: float | None,
    cfg: EconomicsConfig,
) -> float:
    """Emission carry over the hold.

    * ``off``      → 0.
    * ``flat_apr`` → ``amount_tao × carry_apr × hold_hours / 8760`` (uniform yield).
    * ``per_bar``  → deferred; needs a per-bar emission series in ``pool_history``
      (raises ``NotImplementedError``).
    """
    model = cfg.carry_model
    if model == "off":
        return 0.0
    if model == "flat_apr":
        return amount_tao * cfg.carry_apr * (hold_hours / HOURS_PER_YEAR)
    if model == "per_bar":
        raise NotImplementedError(
            "carry_model='per_bar' needs a per-bar emission series; pool_history "
            "has no emission column (see the spec's carry-data gap)."
        )
    raise ValueError(f"unknown carry_model {model!r} (expected off|flat_apr|per_bar)")


# ── Round-trip convenience (factor path + the #3/#4/#5 net-return number) ──────────

def net_round_trip(
    spot_entry: float,
    spot_exit: float,
    tao_in_pool: float,
    size_tao: float,
    hold_hours: float,
    emission_rate: float | None,
    cfg: EconomicsConfig,
) -> tuple[float, float, float]:
    """One round trip → ``(pnl_tao, fee_tao, carry_tao)``.

    ``pnl_tao = size·(eff_exit − eff_entry)/eff_entry + carry``  (fee already in the
    prices, not re-subtracted). Uses a single ``tao_in_pool`` for both legs — the
    factor path's documented entry-bar-depth assumption. ``fee_tao``/``carry_tao``
    are reported components.

    ``emission_rate`` is reserved for ``carry_model="per_bar"`` (deferred per-bar
    accrual); all implemented tiers (``off``, ``flat_apr``) ignore it, so callers
    pass ``None``.
    """
    eff_entry = effective_entry_price(spot_entry, tao_in_pool, size_tao, cfg)
    if eff_entry <= 0:
        return (0.0, 0.0, 0.0)
    alpha_qty = size_tao / eff_entry
    exit_notional = alpha_qty * spot_exit
    eff_exit = effective_exit_price(spot_exit, tao_in_pool, exit_notional, cfg)
    gross_pnl = size_tao * (eff_exit - eff_entry) / eff_entry
    carry = accrue_carry(size_tao, hold_hours, emission_rate, cfg)
    fee = swap_fee_tao(size_tao, cfg) + swap_fee_tao(exit_notional, cfg)
    return (gross_pnl + carry, fee, carry)


def economics_badge(cfg: EconomicsConfig | None) -> dict | None:
    """JSON badge stamped on a BacktestResult so the /research explorer can tell a
    net-of-cost row apart from a gross one (and not rank different impact_scale /
    carry configs side by side). None when economics is off/absent."""
    if cfg is None or not cfg.enabled:
        return None
    return {
        "enabled": True,
        "fee_rate": cfg.fee_rate,
        "impact_scale": cfg.impact_scale,
        "carry_model": cfg.carry_model,
        "carry_apr": cfg.carry_apr,
    }
