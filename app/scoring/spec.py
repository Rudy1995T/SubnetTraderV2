"""Frozen scoring specs (spec 12 §2.13; master §7). ``TS_1_0`` = every number that defines ts-1.0's composite.

Scoring thresholds are not config (01 §5 rule): changing one means a new ``ScoringSpec`` with a new ``version``.
``spec_hash(TS_1_0)`` is asserted against ``GOLDEN_SPEC_HASH`` at import (``SpecHashMismatch`` fails boot on
purpose, §4 case 20). Earlier phases' frozen constants (08) also live here.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, fields
from types import MappingProxyType
from typing import Any, Final, Mapping

from app.features.registry import METRICS, PILLAR_WEIGHTS, registry_hash
from app.quant.regime import REGIME_TS_1_0, V_W_BY_REGIME_TS_1_0, RegimeParams
from app.scoring.golden import GOLDEN_REGISTRY_HASH, GOLDEN_SPEC_HASH
from app.scoring.red_flags import FLAGS_TS_1_0, FlagDef

SPEC_VERSION = "ts-1.0"

# spec 08 §2.9 (community)
XPLATFORM_DEDUPE_HOURS = 48          # same text_hash across X / Discord / Telegram within ±48 h counts once
GROWTH_BASE_TOLERANCE_DAYS = 3       # discord_growth_30d base sample searched at 30 ± 3 d
INFLATION_DAYS = 7                   # inflated_community after 7 consecutive inflated days

RANKG_MIN_N: Final = 10

# spec 10 §2.2/§5 (P6 + label resolution) — frozen with ts-1.0, deliberately NOT ScoringSpec fields (adding a field
# would change GOLDEN_SPEC_HASH); tests/nlp/test_rubric.py guards the rubric hash ↔ LABEL_POLICIES pairing instead.
from app.nlp.resolve import LabelPolicy  # noqa: E402

LABEL_POLICIES: Final[Mapping[str, LabelPolicy]] = MappingProxyType({
    "ts-1.0": LabelPolicy(escalation=("claude-sonnet-5-5:e47d012b:1",),
                          bulk=("claude-haiku-5-5:e47d012b:1",),
                          fallback=("rules:v2",)),
})
P6_HALF_LIFE_S: Final = 7 * 86400              # event_impact_14d decay 0.5^((t − τ)/7 d)
P6_EVENT_WINDOW_S: Final = 14 * 86400          # (t − 14 d, t]
P6_CLUSTER_WIDTH_S: Final = 172800             # 48-h cluster buckets (= events dedupe bucket)
P6_EVENT_SCALE: Final = 2.0                    # tanh(x/2) (12's event_impact normaliser)
P6_UNPROFILED_CREDIBILITY: Final = 0.7         # polarity weight c when author_credibility is NULL
P6_AUTHOR_CAP: Final = 0.25                    # water-filled single-author share
P6_MIN_POSTS: Final = 5
P6_POLARITY_WINDOW_S: Final = 7 * 86400
P6_ATTENTION_RECENT_DAYS: Final = 7
P6_ATTENTION_BASELINE_DAYS: Final = 90
P6_ATTENTION_MIN_RECENT: Final = 4
P6_ATTENTION_MIN_BASELINE: Final = 21
P6_ATTENTION_VIEWS_FLOOR_REL: Final = 0.1      # floor_views = max(1, 0.1·mean_B)
P6_ATTENTION_VIEWS_FLOOR_MIN: Final = 1.0
P6_ATTENTION_MENT_FLOOR: Final = 0.5
P6_KOL_MIN_CREDIBILITY: Final = 0.5
P6_KOL_MIN_VERIFIED: Final = 5
P6_KOL_POLL_RECENCY_S: Final = 3 * 86400
P6_KOL_WINDOW_S: Final = 30 * 86400
P6_ENTITY_MIN_CONFIDENCE: Final = 0.6
P6_COVERAGE_MIN: Final = 0.5


class SpecHashMismatch(Exception):
    """``spec_hash`` differs from the golden value (plain Exception, never ImportError — §4 case 20)."""


class SpecUnknown(KeyError):
    pass


@dataclass(frozen=True)
class ScoringSpec:
    version: str                                   # "ts-1.0"
    raw_spec_version: str                          # raw metric definitions it reads ("ts-1.0")
    metrics: Mapping[str, Any]                     # registry used (ts-1.0: registry.METRICS)
    pillar_weights: Mapping[str, float]            # {P1:30, P2:20, P3:10, P4:18, P5:12}
    pillar_min_coverage: float = 0.3
    velocity_weights: tuple[float, float] = (0.6, 0.4)          # (P6, blend)
    blend_keys: tuple[str, ...] = ("net_flow_30d", "chain_buys", "attention_7d")
    v_w_by_regime: Mapping[str, float] = V_W_BY_REGIME_TS_1_0
    flags: Mapping[str, FlagDef] = FLAGS_TS_1_0
    flag_floor: float = 0.25
    ema_span: int = 7
    ema_max_gap_days: int = 7
    tier_lower: Mapping[str, float] = MappingProxyType({"A": 75.0, "B": 60.0, "C": 45.0, "D": 30.0})
    tier_hysteresis: float = 3.0
    insufficient_below: float = 0.4
    rankg_clip_z: float = 2.5
    rankg_min_n: int = RANKG_MIN_N
    manual_clear_suppress_days: int = 30
    regime: RegimeParams = REGIME_TS_1_0


TS_1_0: Final = ScoringSpec(version="ts-1.0", raw_spec_version="ts-1.0", metrics=METRICS,
                            pillar_weights=PILLAR_WEIGHTS)
SPECS: Final[Mapping[str, ScoringSpec]] = MappingProxyType({"ts-1.0": TS_1_0})


def _canon(v: Any) -> Any:
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, bool) or v is None or isinstance(v, (int, str)):
        return v
    if isinstance(v, FlagDef):
        return {"name": v.name, "multiplier": repr(v.multiplier), "clear_days": v.clear_days,
                "human_confirmable": v.human_confirmable, "manual_clearable": v.manual_clearable,
                "sticky_on": v.sticky_on, "evaluate": f"{v.evaluate.__module__}.{v.evaluate.__qualname__}"}
    if isinstance(v, RegimeParams):
        return {f.name: _canon(getattr(v, f.name)) for f in fields(v)}
    if isinstance(v, Mapping):
        return {str(k): _canon(x) for k, x in sorted(v.items(), key=lambda kv: str(kv[0]))}
    if isinstance(v, (tuple, list)):
        return [_canon(x) for x in v]
    if callable(v):
        return f"{v.__module__}.{v.__qualname__}"
    raise TypeError(f"spec_hash: cannot canonicalise {type(v).__name__}")


def spec_hash(spec: ScoringSpec) -> str:
    """sha256 of the canonical JSON (sorted keys, floats by repr, fns by qualified name); the registry by its hash."""
    body: dict[str, Any] = {}
    for f in fields(spec):
        v = getattr(spec, f.name)
        body[f.name] = registry_hash() if f.name == "metrics" and v is METRICS else _canon(v)
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def get_spec(version: str) -> ScoringSpec:
    try:
        return SPECS[version]
    except KeyError:
        raise SpecUnknown(version) from None


def _check() -> None:
    for k in TS_1_0.blend_keys:                                 # §2.4 validation 7: blend keys are scored rows
        if METRICS[k].role != "scored":
            raise SpecHashMismatch(f"blend key {k} is not a scored registry row")
    if set(TS_1_0.v_w_by_regime) != {"TRENDING", "DISPERSED", "CHOPPY", "DEAD", "UNKNOWN"}:
        raise SpecHashMismatch("v_w_by_regime must cover the five 02 §B8 labels")
    h = spec_hash(TS_1_0)
    if GOLDEN_SPEC_HASH.get("ts-1.0") != h:
        raise SpecHashMismatch(f"spec_hash(ts-1.0) {h} != GOLDEN_SPEC_HASH['ts-1.0'] (ts-1.0 is frozen; a change "
                               "needs a new spec_version, §2.13)")


_check()

__all__ = ["SPEC_VERSION", "ScoringSpec", "TS_1_0", "SPECS", "REGIME_TS_1_0", "GOLDEN_SPEC_HASH",
           "GOLDEN_REGISTRY_HASH", "get_spec", "spec_hash", "SpecHashMismatch", "SpecUnknown", "RANKG_MIN_N",
           "LABEL_POLICIES", "LabelPolicy"]
