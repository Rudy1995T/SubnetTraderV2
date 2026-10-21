"""The ts-1.0 metric registry (spec 12 §2.4; 02 §A2/§C1–§C7): 62 declarative rows — 45 scored metrics (P1…P6),
9 legs, 8 aux flag inputs — validated at import (any violation raises ``RegistryError``, so boot fails loudly).

Every number here is part of ``ts-1.0`` (01 §5: not config); an edit changes ``registry_hash()`` and fails the
golden check (§2.13). Spec 03 owns the key catalog (``app.storage.catalog.METRIC_KEYS``); this registry never adds
keys to it. Producer fns attach through ``register_metric`` (02 §C5) and are imported lazily by the feature engine.
"""
from __future__ import annotations

import hashlib
import inspect
import json
import math
import re
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Final, Iterable, Mapping

from app.features.normalise import MULTI_INPUT, NORMALISERS
from app.storage.catalog import METRIC_KEYS, RETIRED_METRIC_KEYS

if TYPE_CHECKING:  # pragma: no cover
    from datetime import datetime

    from app.features.types import MetricResult
    from app.storage.db import Database

ProducerFn = Callable[["Database", str, "datetime"], Awaitable["MetricResult"]]


class RegistryError(Exception):
    """Invalid registry or registration (plain Exception, never ImportError — §4 case 20)."""


@dataclass(frozen=True, slots=True)
class MetricDef:
    key: str                       # ^[a-z0-9_]{3,48}$
    pillar: str | None             # "P1".."P6"; None for legs/aux
    weight: float                  # within-pillar weight; 0.0 for legs/aux
    normaliser: str                # key of NORMALISERS; "none" for legs/aux
    params: Mapping[str, float]    # frozen normaliser parameters (MappingProxyType)
    freshness_days: float
    source_spec: str               # "05".."11", or "12" for derived
    producer: str                  # "collector" | "fn" | "derived"
    role: str = "scored"           # "scored" | "leg" | "aux"
    fn: str | None = None          # dotted "module:callable" of a producer fn (lazy import)
    fallback_key: str | None = None
    absence_rule: str = ""         # human-readable mirror of the producer's absence table

    @property
    def freshness_s(self) -> int:
        return int(self.freshness_days * 86400)

    @property
    def cross_sectional(self) -> bool:
        return self.normaliser == "rankg"

    @property
    def source(self) -> str:
        """The producing spec's single ``source`` (02 §B9) — 03's catalog value."""
        return METRIC_KEYS[self.key].source


PILLAR_WEIGHTS: Final = MappingProxyType({"P1": 30.0, "P2": 20.0, "P3": 10.0, "P4": 18.0, "P5": 12.0})
PILLARS: Final = ("P1", "P2", "P3", "P4", "P5", "P6")
DERIVED_KEYS: Final = ("announcement_cadence_30d", "silence_days")
# leg → the metric it feeds (02 §C3; x_kol_posts_30d is 45's fallback); used by register_metric's component_of check
COMPONENT_OF: Final = MappingProxyType({
    "x_cadence_30d": "announcement_cadence_30d", "discord_announcements_30d": "announcement_cadence_30d",
    "tg_posts_30d": "announcement_cadence_30d", "x_last_activity_days": "silence_days",
    "discord_last_activity_days": "silence_days", "tg_last_activity_days": "silence_days",
    "x_kol_posts_30d": "kol_coverage_30d"})
# master §7.3 scored sets per pillar (validation 1)
_MASTER_SCORED: Final = {
    "P1": {"emission_share", "gate_margin", "dereg_safety", "liquidity_depth", "exit_capacity", "net_flow_30d",
           "chain_buys", "holder_dispersion", "participation", "miner_burn_ok"},
    "P2": {"active_authors_28d", "fte_devs_84d", "commit_days_90d", "declared_repo_freshness", "org_freshness",
           "release_cadence_90d", "review_practice_90d", "bus_factor", "repo_hygiene", "originality"},
    "P3": {"site_uptime_30d", "tls_ok", "site_age", "site_freshness", "docs_present", "product_status"},
    "P4": {"team_doxxed", "owner_identity", "owner_commitment", "owner_retention_30d", "control_stability_90d",
           "owner_overhang", "tenure"},
    "P5": {"venue_reachable", "discord_members", "presence_ratio", "discord_growth_30d", "announcement_cadence_30d",
           "x_engagement_30d", "x_followers", "silence_days"},
    "P6": {"event_impact_14d", "polarity_7d", "attention_7d", "kol_coverage_30d"},
}
_PRENORM_KEYS: Final = frozenset({"team_doxxed", "owner_identity", "owner_commitment", "owner_retention_30d",
                                  "control_stability_90d", "owner_overhang"})
_KEY_RE = re.compile(r"^[a-z0-9_]{3,48}$")

_C8 = "app.features.community"
_X = "app.features.social_x"
_P6 = "app.features.sentiment"


def _r(n: int, key: str, pillar: str | None, w: float, norm: str, params: dict | None, fresh: float, src: str,
       producer: str, *, role: str = "scored", fn_mod: str | None = None, fallback: str | None = None,
       absence: str = "") -> tuple[int, MetricDef]:
    fn = f"{fn_mod}:compute_{key}" if fn_mod else None
    return n, MetricDef(key, pillar, w, norm, MappingProxyType(dict(params or {})), fresh, src, producer, role, fn,
                        fallback, absence)


# ── the table (ts-1.0) = master §7.3 + 02 §C1–C7; row numbers = spec 12 §2.4 ─────────────────────────────────
_TABLE: Final[tuple[tuple[int, MetricDef], ...]] = (
    # P1 · Chain Vitality (05)
    _r(1, "emission_share", "P1", .20, "rankg", None, 1, "05", "collector", absence="real 0 ranked / no 00:00Z row"),
    _r(2, "gate_margin", "P1", .15, "zclip_log2", {"lo": -2.0, "hi": 1.0}, 1, "05", "collector",
       absence="ratio 0 -> 0.0 / not eligible, fit missing"),
    _r(3, "dereg_safety", "P1", .15, "identity", None, 1, "05", "collector",
       absence="bottom-3 non-immune -> raw 0.0 ok / moving price null"),
    _r(4, "liquidity_depth", "P1", .10, "rankg", None, 1, "05", "collector", absence="drained pool ranked lowest"),
    _r(5, "exit_capacity", "P1", .05, "zclip", {"lo": 0.05, "hi": 0.6}, 1, "05", "collector",
       absence="never / price <= 0"),
    _r(6, "net_flow_30d", "P1", .10, "zclip", {"lo": -0.5, "hi": 0.5}, 1, "05", "collector",
       absence="real 0 / too few daily rows"),
    _r(7, "chain_buys", "P1", .05, "rankg", None, 1, "05", "collector", absence="excess_tao=0 ranked / mcap null"),
    _r(8, "holder_dispersion", "P1", .05, "zclip", {"lo": 0.3, "hi": 0.9}, 1, "05", "collector",
       absence="never / no holders snapshot <= 14 d"),
    _r(9, "participation", "P1", .05, "zclip", {"lo": 0.2, "hi": 0.9}, 1, "05", "collector",
       absence="counts / max_uids only (02 §C7) / no metagraph source"),
    _r(10, "miner_burn_ok", "P1", .10, "zclip", {"lo": 0.0, "hi": 0.8}, 1, "05", "collector",
       absence="fully burned -> raw 0.0 ok / deep tail, fit degraded"),
    # P2 · Builder Activity (06)
    _r(11, "active_authors_28d", "P2", .20, "logcap", {"cap": 10.0}, 2, "06", "collector",
       absence="no GitHub ref / placeholder / gone -> absent; no token, stale -> failed"),
    _r(12, "fte_devs_84d", "P2", .15, "logcap", {"cap": 8.0}, 2, "06", "collector", absence="as 11"),
    _r(13, "commit_days_90d", "P2", .15, "logcap", {"cap": 90.0}, 2, "06", "collector", absence="as 11"),
    _r(14, "declared_repo_freshness", "P2", .10, "recency", {"full_at": 14.0, "zero_at": 180.0}, 2, "06",
       "collector", absence="reason repo_gone -> absent (repo_gone flag)"),
    _r(15, "org_freshness", "P2", .10, "recency", {"full_at": 7.0, "zero_at": 90.0}, 2, "06", "collector",
       absence="as 11; value_json archive keys drive all_repos_archived"),
    _r(16, "release_cadence_90d", "P2", .05, "logcap", {"cap": 6.0}, 2, "06", "collector", absence="as 11"),
    _r(17, "review_practice_90d", "P2", .05, "identity", None, 2, "06", "collector",
       absence="as 11; 0 merged PRs -> not_applicable"),
    _r(18, "bus_factor", "P2", .10, "bus_factor", None, 2, "06", "collector", absence="as 11"),
    _r(19, "repo_hygiene", "P2", .05, "checklist", None, 2, "06", "collector",
       absence="as 11; < 3 of 5 items observed -> failed"),
    _r(20, "originality", "P2", .05, "identity", None, 2, "06", "collector", absence="as 11"),
    # P3 · Product & Web Presence (07)
    _r(21, "site_uptime_30d", "P3", .30, "identity", None, 2, "07", "collector",
       absence="no website -> absent / < 3 definitive check days"),
    _r(22, "tls_ok", "P3", .10, "binary", None, 2, "07", "collector", absence="expired -> raw 0 ok"),
    _r(23, "site_age", "P3", .10, "age_ramp", {"young_at": 30.0, "mature_at": 365.0, "floor": 0.2}, 2, "07",
       "collector", absence="never / Wayback never answered"),
    _r(24, "site_freshness", "P3", .15, "recency", {"full_at": 90.0, "zero_at": 365.0}, 2, "07", "collector",
       absence="never / SPA shell"),
    _r(25, "docs_present", "P3", .15, "binary", None, 2, "07", "collector", absence="no docs link -> 0 / site down"),
    _r(26, "product_status", "P3", .20, "identity", None, 2, "07", "collector",
       absence="manual none -> 0 / unset -> None"),
    # P4 · Team & Governance Integrity (11; rows 27–32 pre-normalised, 02 §C1)
    _r(27, "team_doxxed", "P4", .25, "prenorm", None, 90, "11", "collector",
       absence="anonymous -> 0.0 / no record, all rejected -> None"),
    _r(28, "owner_identity", "P4", .05, "prenorm", None, 2, "11", "collector",
       absence="no identity row -> 0.0 / coldkey unknown"),
    _r(29, "owner_commitment", "P4", .20, "prenorm", None, 2, "11", "collector",
       absence="zero lock, auto off, age > 90 d -> 0.0 / grace_period"),
    _r(30, "owner_retention_30d", "P4", .20, "prenorm", None, 2, "11", "collector",
       absence="hard trigger -> 0.0 first, then cut disabled -> 1.0 / pre-launch"),
    _r(31, "control_stability_90d", "P4", .15, "prenorm", None, 2, "11", "collector",
       absence="non-whitelisted perpetual_off -> 0.0 / no owner snapshot <= 36 h"),
    _r(32, "owner_overhang", "P4", .05, "prenorm", None, 2, "11", "collector",
       absence="dump_impact >= 25 % -> 0.0 / position unknown"),
    _r(33, "tenure", "P4", .10, "tenure", {"lo": 120.0, "hi": 365.0, "floor": 0.3}, 2, "11", "collector",
       fallback="occupant_age_days", absence="never 0 / no_registration_block -> failed"),
    # P5 · Community & Communication (08 fns, 12 derived, 09 fns)
    _r(34, "venue_reachable", "P5", .10, "binary", None, 2, "08", "fn", fn_mod=_C8,
       absence="no reachable venue -> 0.0 / class unknown"),
    _r(35, "discord_members", "P5", .15, "logcap", {"cap": 5000.0}, 2, "08", "fn", fn_mod=_C8,
       absence="— / not own_server"),
    _r(36, "presence_ratio", "P5", .10, "presence_ratio",
       {"low": 0.05, "high": 0.35, "inflate_below": 0.02, "inflate_members": 1000.0}, 2, "08", "fn", fn_mod=_C8,
       absence="— / < 2 samples in 24 h"),
    _r(37, "discord_growth_30d", "P5", .10, "zclip", {"lo": -0.1, "hi": 0.3}, 2, "08", "fn", fn_mod=_C8,
       absence="— / < 30 ± 3 d of counts"),
    _r(38, "announcement_cadence_30d", "P5", .25, "logcap", {"cap": 12.0}, 1, "12", "derived",
       absence="sum of ok legs 46–48; no ok leg -> failed"),
    _r(39, "x_engagement_30d", "P5", .10, "rankg", None, 3, "09", "fn", fn_mod=_X, absence="never / 0 posts"),
    _r(40, "x_followers", "P5", .05, "logcap", {"cap": 50000.0}, 3, "09", "fn", fn_mod=_X,
       absence="no handle, gone -> absent"),
    _r(41, "silence_days", "P5", .15, "recency", {"full_at": 7.0, "zero_at": 45.0}, 1, "12", "derived",
       absence="min over ok legs 49–51; absent only without venue and X account (02 §C3)"),
    # P6 · Narrative & Sentiment (10 fns; Velocity only)
    _r(42, "event_impact_14d", "P6", .40, "event_impact", {"scale": 2.0}, 1, "10", "fn", fn_mod=_P6,
       absence="no events -> raw 0 ok / labels delayed"),
    _r(43, "polarity_7d", "P6", .25, "identity", None, 1, "10", "fn", fn_mod=_P6, absence="— / < 5 posts"),
    _r(44, "attention_7d", "P6", .20, "zclip", {"lo": -2.0, "hi": 3.0}, 1, "10", "fn", fn_mod=_P6,
       absence="— / baseline < 21 d"),
    _r(45, "kol_coverage_30d", "P6", .15, "logcap", {"cap": 5.0}, 1, "10", "fn", fn_mod=_P6,
       fallback="x_kol_posts_30d", absence="0 mentions -> raw 0 ok / no KOL run in 30 d"),
    # legs (02 §C3)
    _r(46, "x_cadence_30d", None, 0.0, "none", None, 3, "09", "fn", role="leg", fn_mod=_X,
       absence="no handle / gone -> absent"),
    _r(47, "discord_announcements_30d", None, 0.0, "none", None, 2, "08", "fn", role="leg", fn_mod=_C8,
       absence="0 = followed channel, no posts / None = no followed channel"),
    _r(48, "tg_posts_30d", None, 0.0, "none", None, 2, "08", "fn", role="leg", fn_mod=_C8,
       absence="0 = fresh preview / None = no public channel"),
    _r(49, "x_last_activity_days", None, 0.0, "none", None, 3, "09", "fn", role="leg", fn_mod=_X,
       absence="no handle / gone -> absent"),
    _r(50, "discord_last_activity_days", None, 0.0, "none", None, 2, "08", "fn", role="leg", fn_mod=_C8,
       absence="never absent; value_json.lower_bound"),
    _r(51, "tg_last_activity_days", None, 0.0, "none", None, 2, "08", "fn", role="leg", fn_mod=_C8,
       absence="never absent; value_json.lower_bound"),
    _r(52, "x_mentions_7d", None, 0.0, "none", None, 3, "09", "fn", role="leg", fn_mod=_X,
       absence="evidence only; not an attention_7d input"),
    _r(53, "news_mentions_7d", None, 0.0, "none", None, 3, "09", "fn", role="leg",
       fn_mod="app.features.social_feeds", absence="per 09 A.4"),
    _r(54, "x_kol_posts_30d", None, 0.0, "none", None, 3, "09", "fn", role="leg", fn_mod=_X,
       absence="fallback leg of 45"),
    # aux flag inputs
    _r(55, "emission_enabled_flag", None, 0.0, "none", None, 1, "05", "collector", role="aux",
       absence="1/0 -> emission_disabled"),
    _r(56, "dereg_risk_flag", None, 0.0, "none", None, 1, "05", "collector", role="aux",
       absence="1/0 -> dereg_imminent"),
    _r(57, "immune_flag", None, 0.0, "none", None, 1, "05", "collector", role="aux",
       absence="1/0 -> dereg_imminent"),
    _r(58, "implied_miner_burn", None, 0.0, "none", None, 1, "05", "collector", role="aux",
       absence="[0,1] -> miner_burn_zeroed"),
    _r(59, "dereg_hazard", None, 0.0, "none", None, 1, "05", "collector", role="aux", absence="evidence [0,1]"),
    _r(60, "occupant_age_days", None, 0.0, "none", None, 1, "05", "collector", role="aux",
       absence="days since registration (tenure fallback)"),
    _r(61, "owner_extraction_trigger", None, 0.0, "none", None, 2, "11", "collector", role="aux",
       absence="02 §C2 -> owner_extraction; value_json.{reasons, trigger_ids}"),
    _r(62, "owner_control_change_trigger", None, 0.0, "none", None, 2, "11", "collector", role="aux",
       absence="02 §C2 -> owner_control_change; value_json.{kind, trigger_ids, identity_churn}"),
)

METRICS: Final[Mapping[str, MetricDef]] = MappingProxyType({d.key: d for _, d in _TABLE})
PILLAR_OF: Final[Mapping[str, str | None]] = MappingProxyType({k: d.pillar for k, d in METRICS.items()})
EVIDENCE_FNS: Final[dict[str, ProducerFn]] = {}
ATTACHED_FNS: Final[dict[str, ProducerFn]] = {}          # register_metric case (a): overrides the dotted path
IGNORED_FNS: Final[dict[str, ProducerFn]] = {}           # fns registered for derived keys — never called (§2.4)


def scored(pillar: str) -> tuple[MetricDef, ...]:
    """Scored metrics of ``pillar`` in registry order."""
    return tuple(d for _, d in _TABLE if d.role == "scored" and d.pillar == pillar)


def scored_keys() -> tuple[str, ...]:
    return tuple(d.key for _, d in _TABLE if d.role == "scored")


def _canonical() -> list[dict]:
    """Canonical rows for the hash: fn paths (wiring) and absence_rule (documentation) excluded."""
    out = []
    for n, d in _TABLE:
        out.append({"n": n, "key": d.key, "pillar": d.pillar, "weight": repr(float(d.weight)),
                    "normaliser": d.normaliser, "params": {k: repr(float(v)) for k, v in sorted(d.params.items())},
                    "freshness_days": repr(float(d.freshness_days)), "source_spec": d.source_spec,
                    "producer": d.producer, "role": d.role, "fallback_key": d.fallback_key})
    return out


def registry_hash() -> str:
    """sha256 of the canonical JSON of ``_TABLE`` (sorted keys; floats by repr; fn paths excluded)."""
    body = json.dumps(_canonical(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode()).hexdigest()


def _bind_check(d: MetricDef) -> None:
    f = NORMALISERS[d.normaliser]
    sig = inspect.signature(f)
    extra = {k: None for k in MULTI_INPUT.get(d.normaliser, ())}
    try:
        if d.normaliser == "presence_ratio":
            sig.bind(0.5, None, **d.params)
        else:
            sig.bind(0.5, **extra, **d.params)
    except TypeError as exc:
        raise RegistryError(f"{d.key}: normaliser {d.normaliser}{dict(d.params)} does not bind: {exc}") from None


def _validate(table: Iterable[tuple[int, MetricDef]] | None = None, *, check_golden: bool = True) -> None:
    """§2.4 import-time validation 1–9; raises ``RegistryError``."""
    rows = list(_TABLE if table is None else table)
    defs = [d for _, d in rows]
    keys = [d.key for d in defs]
    errs: list[str] = []
    # 1 keys, counts, master sets
    if len(set(keys)) != len(keys):
        errs.append("duplicate keys")
    errs += [f"bad key {k!r}" for k in keys if not _KEY_RE.match(k)]
    if len(defs) != 62:
        errs.append(f"expected 62 rows, got {len(defs)}")
    for p, want in _MASTER_SCORED.items():
        got = {d.key for d in defs if d.role == "scored" and d.pillar == p}
        if got != want:
            errs.append(f"{p} scored set differs from master §7.3: +{sorted(got - want)} -{sorted(want - got)}")
    n_leg = sum(d.role == "leg" for d in defs)
    n_aux = sum(d.role == "aux" for d in defs)
    if (n_leg, n_aux) != (9, 8):
        errs.append(f"expected 9 legs + 8 aux, got {n_leg} + {n_aux}")
    # 2 pillar weight sums
    for p in PILLARS:
        s = math.fsum(d.weight for d in defs if d.role == "scored" and d.pillar == p)
        if abs(s - 1.0) > 1e-9:
            errs.append(f"{p} weights sum to {s!r}")
    # 3 pillar weights
    if dict(PILLAR_WEIGHTS) != {"P1": 30.0, "P2": 20.0, "P3": 10.0, "P4": 18.0, "P5": 12.0} or "P6" in PILLAR_WEIGHTS:
        errs.append("PILLAR_WEIGHTS differ from {P1:30,P2:20,P3:10,P4:18,P5:12} (P6 → Velocity only)")
    by_key = {d.key: d for d in defs}
    for d in defs:
        # 4 roles
        if d.role == "scored":
            if d.pillar not in PILLARS or d.weight <= 0 or d.normaliser == "none":
                errs.append(f"{d.key}: scored row needs pillar, weight > 0 and a normaliser")
        elif d.role in ("leg", "aux"):
            if d.pillar is not None or d.weight != 0 or d.normaliser != "none":
                errs.append(f"{d.key}: {d.role} row needs pillar None, weight 0, normaliser 'none'")
        else:
            errs.append(f"{d.key}: bad role {d.role!r}")
        # 5 normaliser + params
        if d.normaliser != "none":
            if d.normaliser not in NORMALISERS:
                errs.append(f"{d.key}: unknown normaliser {d.normaliser!r}")
            else:
                try:
                    _bind_check(d)
                except RegistryError as exc:
                    errs.append(str(exc))
        # 6 freshness, source spec, producer, fn naming, prenorm placement
        if not d.freshness_days > 0:
            errs.append(f"{d.key}: freshness_days must be > 0")
        if d.source_spec not in ("05", "06", "07", "08", "09", "10", "11", "12"):
            errs.append(f"{d.key}: bad source_spec {d.source_spec!r}")
        if d.producer not in ("collector", "fn", "derived"):
            errs.append(f"{d.key}: bad producer {d.producer!r}")
        if d.producer == "fn" and (not d.fn or d.fn.rsplit(":", 1)[-1] != f"compute_{d.key}"):
            errs.append(f"{d.key}: fn producer must name compute_{d.key} (02 §C5)")
        if (d.normaliser == "prenorm") != (d.key in _PRENORM_KEYS):
            errs.append(f"{d.key}: prenorm must be used exactly on rows 27–32 (02 §C1)")
        if d.producer == "derived" and d.key not in DERIVED_KEYS:
            errs.append(f"{d.key}: only {DERIVED_KEYS} are derived here")
        # 7 fallback
        if d.fallback_key is not None:
            fb = by_key.get(d.fallback_key)
            if fb is None or fb.role not in ("leg", "aux"):
                errs.append(f"{d.key}: fallback_key {d.fallback_key!r} must name a leg/aux row")
        # 9 catalog agreement
        cat = METRIC_KEYS.get(d.key)
        if d.key in RETIRED_METRIC_KEYS:
            errs.append(f"{d.key}: retired name (02 §C2/§C3)")
        if cat is None:
            errs.append(f"{d.key}: not in 03's METRIC_KEYS")
        else:
            if cat.role != d.role:
                errs.append(f"{d.key}: catalog role {cat.role!r} != {d.role!r}")
            if abs(float(cat.weight) - d.weight) > 1e-12:
                errs.append(f"{d.key}: catalog weight {cat.weight} != {d.weight}")
            if d.role == "scored" and cat.pillar != d.pillar:
                errs.append(f"{d.key}: catalog pillar {cat.pillar} != {d.pillar}")
    if errs:
        raise RegistryError("registry ts-1.0 invalid: " + "; ".join(errs))
    # 8 golden hash
    if check_golden and table is None:
        from app.scoring.golden import GOLDEN_REGISTRY_HASH
        h = registry_hash()
        if GOLDEN_REGISTRY_HASH.get("ts-1.0") != h:
            raise RegistryError(f"registry_hash {h} != GOLDEN_REGISTRY_HASH['ts-1.0'] (a frozen ts-1.0 edit needs "
                                "a new spec_version, §2.13)")


def register_metric(key: str, *, fn: ProducerFn, pillar: str | None = None, freshness_days: float | None = None,
                    **declared: Any) -> None:
    """02 §C5 registration entry point. (a) a registry key → attach ``fn``; (b) a catalogued evidence key →
    ``EVIDENCE_FNS``; (c) anything else → ``RegistryError``. Never adds keys to ``METRICS``."""
    if not callable(fn):
        raise RegistryError(f"register_metric({key!r}): fn is not callable")
    d = METRICS.get(key)
    if d is None:
        cat = METRIC_KEYS.get(key)
        if cat is not None and cat.role == "evidence" and key not in RETIRED_METRIC_KEYS:
            w = declared.get("weight")
            if w not in (None, 0, 0.0):
                raise RegistryError(f"register_metric({key!r}): evidence keys carry weight 0, got {w!r}")
            EVIDENCE_FNS[key] = fn
            return
        raise RegistryError(f"register_metric({key!r}): not a ts-1.0 registry key nor a catalogued evidence key")
    errs = []
    want_pillar = d.pillar if d.role == "scored" else METRIC_KEYS[key].pillar
    if pillar is not None and pillar != want_pillar:
        errs.append(f"pillar {pillar!r} != {want_pillar!r}")
    if freshness_days is not None and float(freshness_days) != float(d.freshness_days):
        errs.append(f"freshness_days {freshness_days!r} != {d.freshness_days!r}")
    w = declared.get("weight")
    if w is not None and abs(float(w) - d.weight) > 1e-12:
        errs.append(f"weight {w!r} != {d.weight!r}")
    comp = declared.get("component_of")
    if comp is not None and COMPONENT_OF.get(key) != comp:
        errs.append(f"component_of {comp!r} != {COMPONENT_OF.get(key)!r}")
    if errs:
        raise RegistryError(f"register_metric({key!r}): " + "; ".join(errs))
    if d.producer == "derived":
        IGNORED_FNS[key] = fn              # 12 derives these from the C3 legs; a producer for them is never called
        from app.logging.logger import logger
        logger.warning("producer registered for a derived metric is ignored",
                       data={"metric_key": key, "fn": getattr(fn, "__qualname__", repr(fn))})
        return
    ATTACHED_FNS[key] = fn


def register_many(rows: Iterable[Mapping[str, Any]]) -> None:
    """Spec 08-style list-of-dicts → ``register_metric`` per row (each row has ``key`` and ``fn``)."""
    for r in rows:
        kw = dict(r)
        key = kw.pop("key", None) or kw.pop("metric_key")
        fn = kw.pop("fn")
        register_metric(key, fn=fn, **kw)


_validate()
