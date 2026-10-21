"""Honest backtestability matrix, factor-name rules and the ``coverage.json`` reader (spec 13 §2.3, §2.5, §3.1)."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path

from app.storage.catalog import METRIC_KEYS, RETIRED_METRIC_KEYS

DAY = 86400
_PRICE_BURN = 1782345600          # 2026-06-25 00:00Z — implied-burn fit exists only from the price_burn regime

GITHUB_CAVEAT = "github_org_membership_asof_today"
X_CAVEAT = "x_engagement_refreshed_in_place"
COMPOSITE_CAVEAT = "historical_composite_is_p1_p2_renormalisation"

# key -> {"class", "lag_s", "group", "caveats", "min_as_of"?}; `group` names the coverage.json reconstructible_from
# entry that clamps it. Machine form of the §2.5 table; `--pit-mode reconstructed` consults it.
def _k(cls: str, group: str | None, caveats: tuple[str, ...] = (), lag: int = DAY, min_as_of: int | None = None):
    d = {"class": cls, "lag_s": lag, "group": group, "caveats": list(caveats)}
    if min_as_of is not None:
        d["min_as_of"] = min_as_of
    return d


_P2_RECON = ("active_authors_28d", "fte_devs_84d", "commit_days_90d", "bus_factor", "release_cadence_90d",
             "review_practice_90d")
RECONSTRUCTIBLE_KEYS: dict[str, dict] = {
    # P1 (05 chain replay; 17 history)
    "emission_share": _k("reconstructible", "emission_share"),
    "gate_margin": _k("reconstructible", "price_depth_moving_price", ("gate_margin_pre_v440_regime_tag",)),
    "dereg_safety": _k("reconstructible", "price_depth_moving_price", ("immunity_period_assumed_constant",)),
    "liquidity_depth": _k("reconstructible", "price_depth_moving_price"),
    "net_flow_30d": _k("reconstructible", "net_flow_30d"),
    "chain_buys": _k("reconstructible", "net_flow_30d"),
    "miner_burn_ok": _k("partial", "price_depth_moving_price", min_as_of=_PRICE_BURN),
    # P2 (06 replay over 17 backfill_github)
    **{k: _k("reconstructible", "github_p2", (GITHUB_CAVEAT,)) for k in _P2_RECON},
    "declared_repo_freshness": _k("approx", "github_p2", (GITHUB_CAVEAT,)),
    "org_freshness": _k("approx", "github_p2", (GITHUB_CAVEAT,)),
    # P4
    "tenure": _k("reconstructible", "tenure"),
    # weight-0 raw keys read through `evidence:` (05's chain replay)
    "owner_locked_share": _k("reconstructible", "owner_commitment"),
    "implied_miner_burn": _k("partial", "price_depth_moving_price", min_as_of=_PRICE_BURN),
    "dereg_hazard": _k("reconstructible", "price_depth_moving_price"),
    "occupant_age_days": _k("reconstructible", "tenure"),
    # scores (12's backfilled runs: historically the P1+P2 renormalisation)
    "P1": _k("reconstructible", None),
    "P2": _k("reconstructible", None, (GITHUB_CAVEAT,)),
    "quality": _k("mixed", None, (COMPOSITE_CAVEAT,)),
    "composite_raw": _k("mixed", None, (COMPOSITE_CAVEAT,)),
    "composite": _k("mixed", None, (COMPOSITE_CAVEAT,)),
    "composite_smoothed": _k("mixed", None, (COMPOSITE_CAVEAT,)),
    "confidence": _k("mixed", None, (COMPOSITE_CAVEAT,)),
}

CHAIN_FACTORS = ("momentum", "flow_z", "meanrev_z", "size", "age", "volume", "volume_surge", "px_vs_moving",
                 "moving_px_trend")
CHAIN_STATE_FACTORS = ("chain:emission_share",)
SCORE_FACTORS = ("composite_raw", "composite", "composite_smoothed", "quality", "velocity", "confidence")
PILLARS = ("P1", "P2", "P3", "P4", "P5", "P6")
HEALTH_FACTORS = frozenset({"quality", "quality_cg", "P1", "P2", "P3", "P4", "P5"})
PROMOTABLE_FACTORS = frozenset({"quality", "composite_smoothed"})
SCORED_KEYS = frozenset(k for k, s in METRIC_KEYS.items() if s.role == "scored")
RETIRED_NAMES: dict[str, str] = dict(RETIRED_METRIC_KEYS)
LOOKAHEAD_KEYS = frozenset({"x_engagement_30d", "polarity_7d"})
# factors that contain a look-ahead key (13 §2.3)
_X_CARRIERS = frozenset({"x_engagement_30d", "polarity_7d", "P5", "P6", "velocity", "quality", "composite_raw",
                         "composite", "composite_smoothed"})
_GITHUB_CARRIERS = frozenset({"P2", "quality_cg", "quality", "composite_raw", "composite", "composite_smoothed"})
CONTROL_ALIASES = {"emission_share": "chain:emission_share"}


class FactorNameError(ValueError):
    """A retired or non-factor name (CLI exit 2)."""


def base_key(name: str) -> str:
    return name.split(":", 1)[1] if ":" in name else name


def classify(name: str) -> str:
    """'chain' | 'chain_state' | 'quality_cg' | 'score' | 'pillar' | 'metric' | 'raw' | 'evidence'; raises
    :class:`FactorNameError` for retired/unknown/non-scored names."""
    if name in CHAIN_FACTORS:
        return "chain"
    if name in CHAIN_STATE_FACTORS:
        return "chain_state"
    if name == "quality_cg":
        return "quality_cg"
    if name in SCORE_FACTORS:
        return "score"
    if name in PILLARS:
        return "pillar"
    b = base_key(name)
    if b in RETIRED_NAMES:
        raise FactorNameError(f"{b} is retired (02 §C2); use {RETIRED_NAMES[b]}")
    if name.startswith("raw:"):
        if b in SCORED_KEYS:
            return "raw"
        raise FactorNameError(f"{name}: raw: needs a scored metric_key (norm rows); use evidence:{b}")
    if name.startswith("evidence:"):
        spec = METRIC_KEYS.get(b)
        if spec is None:
            raise FactorNameError(f"unknown metric_key {b}")
        if spec.role == "scored":
            raise FactorNameError(f"{name}: {b} is scored — use the factor name {b} (norm rows)")
        return "evidence"
    if name in SCORED_KEYS:
        return "metric"
    spec = METRIC_KEYS.get(name)
    if spec is not None:
        raise FactorNameError(f"{name} is a {spec.role} key without norm rows; use evidence:{name}")
    raise FactorNameError(f"unknown factor {name!r}")


def pit_keys_for(names) -> tuple[str, ...]:
    """PIT keys the panel must stamp for these factor/control names (chain factors need none)."""
    out: list[str] = []
    for n in names:
        kind = classify(n)
        need = ["P1", "P2"] if kind == "quality_cg" else ([] if kind in ("chain",) else [n])
        for k in need:
            if k not in out:
                out.append(k)
    return tuple(out)


def resolve_control(name: str) -> str:
    return CONTROL_ALIASES.get(name, name)


def lookahead_caveats(names) -> list[str]:
    out = []
    bases = {base_key(n) for n in names} | set(names)
    if bases & _X_CARRIERS:
        out.append(X_CAVEAT)
    if bases & (_GITHUB_CARRIERS | set(_P2_RECON) | {"declared_repo_freshness", "org_freshness", "bus_factor",
                                                      "repo_hygiene", "originality"}):
        out.append(GITHUB_CAVEAT)
    return out


# ── freshness (13 §3.1: 12's registry, else 02 §C6 through 03's catalog) ─────────────────────────────────────────
_SOURCE_FRESH = {"chain": DAY, "github": 2 * DAY, "web": 2 * DAY, "community": 2 * DAY, "x": 3 * DAY,
                 "llm": DAY, "scoring": DAY, "integrity": 2 * DAY}


def freshness_for(key: str, default: int) -> int:
    b = base_key(key)
    try:   # spec 12's registry (Phase 8); absent before
        from app.features.registry import METRICS  # type: ignore[import-not-found]
        m = METRICS.get(b)
        if m is not None and getattr(m, "freshness_s", None):
            return int(m.freshness_s)
    except ImportError:
        pass
    if b == "team_doxxed":
        return 90 * DAY
    spec = METRIC_KEYS.get(b)
    if spec is not None and spec.source in _SOURCE_FRESH:
        return _SOURCE_FRESH[spec.source]
    return int(default)


# ── coverage.json (17) ───────────────────────────────────────────────────────────────────────────────────────────
@dataclass
class Coverage:
    path: str
    sha256: str
    schema: str
    generated_at: int | None
    reconstructible_from: dict[str, int | None] = field(default_factory=dict)
    forward_only: frozenset[str] = frozenset()
    holes: list[tuple[int, int]] = field(default_factory=list)          # price_bars_1h, netuids "all"
    low_completeness_months: list[str] = field(default_factory=list)
    unstamped: dict = field(default_factory=dict)
    caveats: list[str] = field(default_factory=list)
    golden_ok: bool = True

    def clamp_for(self, key: str) -> tuple[bool, int | None]:
        """(known, earliest admissible as_of) for a backfilled row of ``key`` — ``(True, None)`` = no history."""
        b = base_key(key)
        group = RECONSTRUCTIBLE_KEYS.get(b, {}).get("group")
        for k in (b, group):
            if k and k in self.reconstructible_from:
                return True, self.reconstructible_from[k]
        return False, None

    def in_hole(self, t: int) -> bool:
        return any(lo <= t <= hi for lo, hi in self.holes)

    def provenance(self) -> dict:
        return {"path": self.path, "sha256": self.sha256, "schema": self.schema, "generated_at": self.generated_at}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_coverage(path: str | Path | None, warnings: list[str] | None = None) -> Coverage | None:
    """17's ``coverage.json`` (schema ``coverage-1``) or None (missing/unreadable/unknown schema → warning code)."""
    w = warnings if warnings is not None else []
    p = Path(path) if path else None
    if p is None or not p.is_file():
        w.append("coverage_json_missing")
        return None
    try:
        d = json.loads(p.read_text())
    except (OSError, ValueError):
        w.append("coverage_schema_unknown")
        return None
    if not isinstance(d, dict) or d.get("schema") != "coverage-1":
        w.append("coverage_schema_unknown")
        return None
    rf = {k: (int(v) if v is not None else None) for k, v in (d.get("reconstructible_from") or {}).items()}
    holes = [(int(h["from_ts"]), int(h["to_ts"])) for h in d.get("holes") or []
             if h.get("series") == "price_bars_1h" and h.get("netuids") == "all"]
    months = sorted(m for m, v in ((d.get("series") or {}).get("price_bars_1h") or {})
                    .get("completeness_by_month", {}).items() if v is not None and v < 0.8)
    golden = (d.get("checks") or {}).get("rpc_golden_block")
    return Coverage(path=str(p), sha256=_sha256(p), schema="coverage-1", generated_at=d.get("generated_at"),
                    reconstructible_from=rf, forward_only=frozenset(d.get("forward_only") or ()), holes=holes,
                    low_completeness_months=months, unstamped=dict((d.get("occupancy") or {}).get("unstamped") or {}),
                    caveats=list(d.get("caveats") or []), golden_ok=(golden == "pass"))


def is_reconstructible(key: str, coverage: Coverage | None, warnings: list[str] | None = None) -> bool:
    """Whether backfilled rows of ``key`` may enter in reconstructed mode (17's forward_only list wins)."""
    b = base_key(key)
    if b in LOOKAHEAD_KEYS or b not in RECONSTRUCTIBLE_KEYS:
        return False
    if coverage is not None and b in coverage.forward_only:
        if warnings is not None and f"coverage_forward_only:{b}" not in warnings:
            warnings.append(f"coverage_forward_only:{b}")
        return False
    return True
