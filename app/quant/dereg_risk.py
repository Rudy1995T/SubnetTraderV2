"""Deregistration hazard (pure stdlib). Re-implementation of the old ``app/strategy/dereg_risk.py`` from its documented
contract (research 01-factor-validation-tooling §6; the source was not on the laptop — BUILD-LOG):

    hazard = w_proximity·proximity + w_starved·starved + w_decline·decline ∈ [0, 1]
    emission_disabled → 1.0;  proximity None (pool < 3 or netuid not in the pool) → 0.0

* ``proximity`` = 1 − (fraction of the pool strictly below me); the weight map is the non-immune moving price.
* ``starved``  = 1.0 when my weight ÷ the pool median < ``starved_ratio`` (UNVERIFIED: binary in this port).
* ``decline``  = max over emission / price / depth histories of min(1, start→end drop ÷ saturation).
"""
from __future__ import annotations

from dataclasses import dataclass
from statistics import median


@dataclass(frozen=True)
class DeregRiskConfig:
    starved_ratio: float = 0.20
    emission_decline_sat: float = 0.30
    price_decline_sat: float = 0.40
    depth_decline_sat: float = 0.30
    w_proximity: float = 0.45
    w_starved: float = 0.25
    w_decline: float = 0.30
    high_risk_at: float = 0.60
    reset_drop_ratio: float = 0.30


def emission_cull_proximity(emissions: dict[int, float], netuid: int) -> float | None:
    if len(emissions) < 3 or netuid not in emissions:
        return None
    mine = emissions[netuid]
    below = sum(1 for k, v in emissions.items() if k != netuid and v < mine)
    return 1.0 - below / (len(emissions) - 1)


def relative_emission(emissions: dict[int, float], netuid: int) -> float | None:
    if not emissions or netuid not in emissions:
        return None
    med = median(emissions.values())
    return emissions[netuid] / med if med > 0 else None


def pct_decline(series: list[float] | None) -> float:
    vals = [v for v in (series or []) if v is not None]
    if len(vals) < 2 or vals[0] <= 0:
        return 0.0
    return max(0.0, (vals[0] - vals[-1]) / vals[0])


def is_fresh_registration(price_hist: list[float] | None, cfg: DeregRiskConfig = DeregRiskConfig()) -> bool:
    vals = [v for v in (price_hist or []) if v]
    return any(b / a < cfg.reset_drop_ratio for a, b in zip(vals, vals[1:]) if a > 0)


def components(emissions: dict[int, float], netuid: int, *, emission_hist=None, price_hist=None, depth_hist=None,
               cfg: DeregRiskConfig = DeregRiskConfig()) -> dict[str, float | None]:
    prox = emission_cull_proximity(emissions, netuid)
    rel = relative_emission(emissions, netuid)
    starved = 1.0 if (rel is not None and rel < cfg.starved_ratio) else 0.0
    decline = max(min(1.0, pct_decline(emission_hist) / cfg.emission_decline_sat),
                  min(1.0, pct_decline(price_hist) / cfg.price_decline_sat),
                  min(1.0, pct_decline(depth_hist) / cfg.depth_decline_sat))
    return {"proximity": prox, "starved": starved, "decline": decline}


def dereg_hazard_score(emissions: dict[int, float], netuid: int, *, emission_disabled: bool = False,
                       emission_hist: list[float] | None = None, price_hist: list[float] | None = None,
                       depth_hist: list[float] | None = None, cfg: DeregRiskConfig = DeregRiskConfig()) -> float:
    if emission_disabled:
        return 1.0
    c = components(emissions, netuid, emission_hist=emission_hist, price_hist=price_hist, depth_hist=depth_hist, cfg=cfg)
    if c["proximity"] is None:
        return 0.0
    s = cfg.w_proximity * c["proximity"] + cfg.w_starved * c["starved"] + cfg.w_decline * c["decline"]
    return max(0.0, min(1.0, s))


def rank_by_hazard(emissions: dict[int, float], cfg: DeregRiskConfig = DeregRiskConfig()) -> list[tuple[int, float]]:
    return sorted(((n, dereg_hazard_score(emissions, n, cfg=cfg)) for n in emissions), key=lambda x: -x[1])
