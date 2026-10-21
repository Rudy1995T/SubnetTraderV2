"""The pre-registered evidence gate of master §10 as a machine-readable verdict (spec 13 §2.7, §3.6, §8.6). Pure."""
from __future__ import annotations

import math
from dataclasses import dataclass, field

import app.config as app_config
from app.util.time import now_ts

from .backtestability import HEALTH_FACTORS, PROMOTABLE_FACTORS
from .factor_backtest import FactorResult
from .labels import LabelSpec
from .survival import SurvivalResult
from .walk_forward import WalkForwardReport

TELEMETRY_SCHEMA = "sentinel-telemetry-1"
SCOPES = ("live", "survivorship")


@dataclass
class GateConfig:
    horizon_days: int = 7
    min_folds: int = 4
    dsr_threshold: float = 0.95
    decay_min: float = 0.25
    monotone_min: float = 0.6
    survival_horizon_days: int = 90
    survival_min_events: int = 10
    survival_ratio_min: float = 2.0
    observe_min_days: int = 30
    observe_min_coverage: float = 0.60
    observe_min_entries: int = 20
    upsize_min_t: float = 3.0
    trade_tao: float = 2.0
    test_days: int = 28
    ic_t_min: float = 2.0

    @classmethod
    def from_settings(cls) -> "GateConfig":
        s = app_config.settings
        return cls(horizon_days=s.QUANT_GATE_HORIZON_DAYS, min_folds=s.QUANT_MIN_FOLDS,
                   dsr_threshold=s.QUANT_DSR_THRESHOLD, decay_min=s.QUANT_DECAY_MIN, monotone_min=s.QUANT_MONOTONE_MIN,
                   survival_horizon_days=s.QUANT_SURVIVAL_HORIZON_DAYS, survival_min_events=s.QUANT_SURVIVAL_MIN_EVENTS,
                   survival_ratio_min=s.QUANT_SURVIVAL_RATIO_MIN, observe_min_days=s.QUANT_OBSERVE_MIN_DAYS,
                   observe_min_coverage=s.QUANT_OBSERVE_MIN_COVERAGE, observe_min_entries=s.QUANT_OBSERVE_MIN_ENTRIES,
                   upsize_min_t=s.QUANT_UPSIZE_MIN_T, trade_tao=s.QUANT_TRADE_TAO, test_days=s.QUANT_WF_TEST_DAYS)


@dataclass
class ScopeInputs:
    factor_res: FactorResult | None
    controlled_res: FactorResult | None
    dsr: object | None                     # validation.DeflatedSharpe
    wf: WalkForwardReport | None
    surv: SurvivalResult | None
    horizon_bars: int = 0
    extra: dict = field(default_factory=dict)


def _fin(x) -> float | None:
    return float(x) if isinstance(x, (int, float)) and math.isfinite(float(x)) else None


def _ic_check(res: FactorResult | None, label: LabelSpec, horizon_bars: int, t_min: float) -> tuple[bool, str | None,
                                                                                                     dict]:
    if res is None:
        return False, "missing_result", {}
    d = {"mean_ic": res.ic.mean_ic, "t_stat": res.ic.t_stat, "n_dates": res.ic.n_dates, "step": res.step}
    expected = f"ret_log_{horizon_bars}" if label.family == "ret_log" else label.id
    if res.label != expected:
        return False, "label_mismatch", d
    if res.overlapping or res.step != horizon_bars:
        return False, "overlapping_steps", d
    if res.insufficient_data:
        return False, "insufficient_data", d
    ok = res.ic.mean_ic > 0 and res.ic.t_stat >= t_min
    return ok, None if ok else "ic_not_significant", d


def _both(per: dict[str, tuple[bool, str | None, dict]]) -> tuple[bool, str | None]:
    reasons = [f"{s}:{r}" for s, (ok, r, _) in per.items() if not ok and r]
    ok = all(v[0] for v in per.values())
    return ok, (None if ok else (", ".join(reasons) or "failed"))


def evaluate_gate(*, factor: str, label: LabelSpec, live: ScopeInputs, surv: ScopeInputs, n_trials: int,
                  trial_sharpe_var: float | None, cfg: GateConfig, observe_telemetry: dict | None,
                  telemetry_reason: str | None = None, provenance: dict | None = None,
                  trials_file: str | None = None, generated_at: int | None = None) -> dict:
    scopes = {"live": live, "survivorship": surv}
    crit: dict[str, dict] = {}
    warnings: list[str] = []

    # c1 / c2 — rank-IC > 0 with t ≥ 2 on both scopes, non-overlapping; then with controls
    per = {s: _ic_check(si.factor_res, label, si.horizon_bars, cfg.ic_t_min) for s, si in scopes.items()}
    ok, reason = _both(per)
    crit["c1_ic_both_scopes"] = {"pass": ok, "rule": "step==horizon and mean_ic>0 and t>=2 on both scopes",
                                 "reason": reason, **{s: v[2] for s, v in per.items()}}
    per2 = {s: _ic_check(si.controlled_res, label, si.horizon_bars, cfg.ic_t_min) for s, si in scopes.items()}
    ok2, reason2 = _both(per2)
    ctrls = next((si.controlled_res.controls for si in scopes.values() if si.controlled_res), [])
    crit["c2_survives_controls"] = {"pass": ok2, "rule": "the c1 rule on the controlled study, both scopes",
                                    "reason": reason2, "controls": ctrls, **{s: v[2] for s, v in per2.items()}}

    # c3 — monotone-ish deciles, net of cost at trade_tao
    per3 = {}
    for s, si in scopes.items():
        r = si.factor_res
        if r is None:
            per3[s] = (False, "missing_result", {})
            continue
        net = r.deciles_net
        d = {"monotone_score": r.monotone_score, "net_long_short": net.mean_long_short if net else None,
             "net_spread_t": net.spread_t_stat if net else None}
        if net is None:
            per3[s] = (False, "no_net_deciles", d)
        elif r.insufficient_data or r.overlapping:
            per3[s] = (False, "insufficient_data" if r.insufficient_data else "overlapping_steps", d)
        else:
            okk = r.monotone_score >= cfg.monotone_min and net.mean_long_short > 0 and net.spread_t_stat >= 2.0
            per3[s] = (okk, None if okk else "deciles_not_monotone_net", d)
    ok3, reason3 = _both(per3)
    crit["c3_deciles_net"] = {"pass": ok3, "rule": f"monotone_score>={cfg.monotone_min} and net long-short>0 and "
                              "net spread t>=2, both scopes", "reason": reason3, "trade_tao": cfg.trade_tao,
                              **{s: v[2] for s, v in per3.items()}}

    # c4 — DSR > 0.95 with N, V from the trial log (necessary, not sufficient)
    per4 = {}
    for s, si in scopes.items():
        ds = si.dsr
        d = {"dsr": _fin(getattr(ds, "dsr", None)), "sr0": _fin(getattr(ds, "sr0", None))}
        if ds is None:
            per4[s] = (False, "missing_dsr", d)
        elif d["dsr"] is None:
            per4[s] = (False, "non_finite_dsr", d)
        else:
            okk = d["dsr"] > cfg.dsr_threshold
            per4[s] = (okk, None if okk else "dsr_below_threshold", d)
    if n_trials < 2:
        ok4, reason4 = False, "trial_log_too_small"
    else:
        ok4, reason4 = _both(per4)
    crit["c4_dsr"] = {"pass": ok4, "rule": f"DSR>{cfg.dsr_threshold} on both scopes (N, V from trials.json)",
                      "reason": reason4, "n_trials": n_trials, "trial_sharpe_var": trial_sharpe_var,
                      **{s: v[2] for s, v in per4.items()}}

    # c5 — walk-forward with ≥ 4 OOS folds
    per5: dict[str, dict] = {}
    non_adv: list[tuple[str, bool]] = []
    for s, si in scopes.items():
        wf = si.wf
        if wf is None:
            per5[s] = {"n_folds": 0, "n_oos_folds": 0, "advisory_only": True, "reason": "missing_walk_forward"}
            continue
        d = {"n_folds": wf.n_folds, "n_oos_folds": wf.n_oos_folds, "advisory_only": wf.n_oos_folds < cfg.min_folds,
             "pooled_oos_mean_ic": _fin(wf.pooled_oos.get("mean_ic")),
             "pooled_oos_t": _fin(wf.pooled_oos.get("t_stat")),
             "decay_ratio_mean_ic": _fin(wf.decay_ratio.get("mean_ic"))}
        per5[s] = d
        if not d["advisory_only"]:
            okk = (d["pooled_oos_mean_ic"] is not None and d["pooled_oos_mean_ic"] > 0 and d["pooled_oos_t"] is not None
                   and abs(d["pooled_oos_t"]) >= 2.0 and d["decay_ratio_mean_ic"] is not None
                   and d["decay_ratio_mean_ic"] >= cfg.decay_min)
            non_adv.append((s, okk))
    ok5 = bool(non_adv) and all(v for _, v in non_adv)
    reason5 = None if ok5 else ("all_scopes_advisory" if not non_adv else
                                ", ".join(f"{s}:oos_failed" for s, v in non_adv if not v))
    crit["c5_walk_forward"] = {"pass": ok5, "rule": f"every scope with >= {cfg.min_folds} OOS folds: pooled OOS IC>0, "
                               f"|t|>=2, decay>={cfg.decay_min}; at least one such scope", "reason": reason5,
                               "test_days": cfg.test_days, **per5}

    # c6 — survival route (Health-type factors only)
    if factor not in HEALTH_FACTORS:
        crit["c6_survival"] = {"pass": None, "rule": "bottom/top tercile death rate >= 2 with n_events >= 10, both "
                               "scopes", "reason": "not_health", "horizon_days": cfg.survival_horizon_days,
                               "live": {}, "survivorship": {}}
    else:
        per6 = {}
        for s, si in scopes.items():
            sr = si.surv
            if sr is None:
                per6[s] = (False, "missing_survival", {})
                continue
            ratio = sr.death_ratio_low_over_high
            inf = isinstance(ratio, float) and math.isinf(ratio) and ratio > 0
            d = {"n_events": sr.n_events, "n_event_dates": sr.n_event_dates, "ratio": _fin(ratio), "ratio_inf": inf,
                 "n_events_by_kind": sr.n_events_by_kind, "n_events_dereg_only": sr.n_events_dereg_only,
                 "ratio_dereg_only": _fin(sr.death_ratio_dereg_only)}
            if sr.n_events < cfg.survival_min_events:
                per6[s] = (False, f"n_events<{cfg.survival_min_events}", d)
            elif not inf and (d["ratio"] is None or d["ratio"] < cfg.survival_ratio_min):
                per6[s] = (False, "ratio_below_threshold", d)
            else:
                per6[s] = (True, None, d)
        ok6, reason6 = _both(per6)
        crit["c6_survival"] = {"pass": ok6, "rule": f"n_events>={cfg.survival_min_events} and bottom/top tercile "
                               f"ratio>={cfg.survival_ratio_min} (+inf counts), both scopes", "reason": reason6,
                               "horizon_days": cfg.survival_horizon_days, **{s: v[2] for s, v in per6.items()}}

    # c7 — observe-mode telemetry (15 §2.7 export)
    c7 = {"pass": None, "rule": f"days>={cfg.observe_min_days} and entry_coverage>{cfg.observe_min_coverage} and "
          f"n_entries>={cfg.observe_min_entries}", "reason": None, "days": None, "entry_coverage": None,
          "n_entries": None}
    echo = None
    t = observe_telemetry
    if t is None:
        c7["reason"] = telemetry_reason or "pending: telemetry file absent"
    elif t.get("schema") is not None and t.get("schema") != TELEMETRY_SCHEMA:
        c7["reason"] = "telemetry_schema_unknown"
    else:
        cov = t.get("entry_coverage", t.get("live_signal_coverage"))
        c7.update(days=t.get("days"), entry_coverage=cov, n_entries=t.get("n_entries"))
        echo = {"since": t.get("since"), "days": t.get("days"), "entry_coverage": cov, "n_entries": t.get("n_entries")}
        if t.get("n_entries") is None:
            c7["reason"] = "pending: n_entries missing"
        elif t.get("days") is None or cov is None:
            c7["reason"] = "pending: days/entry_coverage missing"
        else:
            entries = t.get("entries")
            if isinstance(entries, list) and len(entries) != int(t["n_entries"]):
                warnings.append("telemetry_entries_length_mismatch")
            okk = (float(t["days"]) >= cfg.observe_min_days and float(cov) > cfg.observe_min_coverage
                   and int(t["n_entries"]) >= cfg.observe_min_entries)
            c7["pass"] = okk
            c7["reason"] = None if okk else "telemetry_below_threshold"
    crit["c7_observe_telemetry"] = c7

    return_route = all(crit[k]["pass"] is True for k in ("c1_ic_both_scopes", "c2_survives_controls", "c3_deciles_net",
                                                        "c4_dsr", "c5_walk_forward"))
    survival_route = crit["c6_survival"]["pass"] is True
    gate_pass = bool((return_route or survival_route) and c7["pass"] is True)
    pending = ["c7"] if c7["pass"] is None else []

    # u1 — Phase-E upsize criterion (never bypasses the base gate)
    u_scopes = {}
    strong = False
    for s in SCOPES:
        d = per5.get(s, {})
        u_scopes[s] = {"n_oos_folds": d.get("n_oos_folds", 0), "pooled_oos_t": d.get("pooled_oos_t")}
        if (d.get("n_oos_folds", 0) >= cfg.min_folds and (d.get("pooled_oos_mean_ic") or 0) > 0
                and (d.get("pooled_oos_t") or 0) >= cfg.upsize_min_t):
            strong = True
    u1_pass = bool(gate_pass and survival_route and strong)
    crit["u1_upsize"] = {"pass": u1_pass, "rule": f"gate_pass and c6 and pooled OOS IC>0 with t>={cfg.upsize_min_t:g} "
                         f"on a scope with n_oos_folds>={cfg.min_folds}", "reason": None if u1_pass else "not_met",
                         **u_scopes, "survival_route_pass": survival_route}

    prov = provenance or {}
    return {
        "spec_version": prov.get("spec_version", "ts-1.0"), "factor": factor, "label": label.id,
        "gate_horizon_days": cfg.horizon_days,
        "horizon_bars": {s: scopes[s].horizon_bars for s in SCOPES},
        "generated_at": int(generated_at if generated_at is not None else now_ts()),
        "provenance": prov, "criteria": crit,
        "gate_pass_return_route": return_route, "gate_pass_survival_route": survival_route, "gate_pass": gate_pass,
        "pending": pending, "upsize_gate_pass": u1_pass,
        "promotable": bool(gate_pass and factor in PROMOTABLE_FACTORS),
        "checks": {"rank_ic_both_scopes": crit["c1_ic_both_scopes"]["pass"],
                   "size_control": crit["c2_survives_controls"]["pass"],
                   "decile_monotone_net": crit["c3_deciles_net"]["pass"], "dsr": crit["c4_dsr"]["pass"],
                   "walk_forward": crit["c5_walk_forward"]["pass"], "survival_route": survival_route,
                   "observe_telemetry_30d": c7["pass"]},
        "observe_telemetry": echo,
        "evidence_class": prov.get("evidence_class"), "lookahead_caveats": prov.get("lookahead_caveats", []),
        "trials": {"n_trials": n_trials, "trial_sharpe_var": trial_sharpe_var, "trials_file": trials_file},
        "warnings": warnings,
    }
