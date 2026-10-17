"""v440 emission-gate reproduction and implied miner burn (spec 05 §2.3).

demand share (moving price) → burn-adjusted share → θ = ``bar_rank``-th highest → Hill gate ``1/(1+(θ/s)^h)`` →
predicted post-gate share; ``b_i = 1 − miner_burned_i`` fitted multiplicatively so predictions match realised shares.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GateFit:
    theta: float
    b: dict[int, float]
    adj: dict[int, float]
    gate: dict[int, float]
    pred: dict[int, float]
    pgs: dict[int, float]
    implied_burn: dict[int, float | None]
    residual_pp: float
    iterations: int
    converged: bool
    burn_source: str


def _clip(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _normalise(d: dict[int, float]) -> dict[int, float]:
    t = sum(d.values())
    return {k: (v / t if t > 0 else 0.0) for k, v in d.items()}


def fit_gate(mp: dict[int, float], realised: dict[int, float], *, bar_rank: int = 32, h: float = 3.0,
             burn_override: dict[int, float] | None = None, max_iter: int = 50, tol: float = 1e-4,
             min_pred_share: float = 1e-4) -> GateFit:
    """Caller guarantees ``len(mp) >= 2`` and every ``mp > 0``; ``realised`` is renormalised over the same set."""
    demand = _normalise(mp)

    def predict(b: dict[int, float]):
        adj = _normalise({i: demand[i] * b[i] for i in mp})
        pos = sorted((v for v in adj.values() if v > 0), reverse=True)
        theta = pos[bar_rank - 1] if len(pos) >= bar_rank else (pos[-1] if pos else 0.0)
        gate = {i: (0.0 if adj[i] <= 0 or theta <= 0 else 1.0 / (1.0 + (theta / adj[i]) ** h)) for i in mp}
        pred = _normalise({i: adj[i] * gate[i] for i in mp})
        return adj, theta, gate, pred

    if burn_override:
        b = {i: _clip(1.0 - burn_override.get(i, 0.0), 0.0, 1.0) for i in mp}
    else:
        b = {i: 1.0 for i in mp}
    it = 0
    delta = 0.0
    for it in range(max_iter):
        adj, theta, gate, pred = predict(b)
        if burn_override:
            break
        # Two deviations from spec 05 §2.3 (BUILD-LOG): (1) b is held, not reset to 1.0, when pred ≤ min_pred_share
        # (the reset makes a fully-burned subnet oscillate 1 → 0 → 1); (2) the multiplicative step is damped by the
        # local elasticity d ln(pred)/d ln(b) = 1 + h·(1 − gate): the undamped step overshoots below θ, where the
        # Hill gate multiplies the sensitivity by up to 1 + h, and never converges.
        new_b = {}
        for i in mp:
            if pred[i] <= min_pred_share:
                new_b[i] = b[i]
            elif realised[i] <= 0:
                new_b[i] = 0.0
            else:
                step = (realised[i] / pred[i]) ** (1.0 / (1.0 + h * (1.0 - gate[i])))
                new_b[i] = _clip(b[i] * step, 0.0, 1.0)
        delta = max(abs(new_b[i] - b[i]) for i in mp)
        b = new_b
        if delta < tol:
            break
    adj, theta, gate, pred = predict(b)
    resid_pp = 100.0 * sum(abs(pred[i] - realised[i]) for i in mp)
    pgs = {i: predict({**b, i: 1.0})[3][i] for i in mp}
    imb: dict[int, float | None] = {i: (None if pgs[i] < min_pred_share else _clip(1.0 - realised[i] / pgs[i], 0.0, 1.0))
                                    for i in mp}
    return GateFit(theta, b, adj, gate, pred, pgs, imb, resid_pp, it + 1, bool(burn_override) or delta < tol,
                   "rpc" if burn_override else "fit")


def gate_margin_raw(fit: GateFit, netuid: int) -> float | None:
    if netuid not in fit.adj or fit.theta <= 0:
        return None
    return fit.adj[netuid] / fit.theta
