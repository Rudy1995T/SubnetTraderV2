"""The pre-registered gate (spec 13 §2.7, §3.6, §8.6) on hand-built inputs."""
from __future__ import annotations

import json
import math

import pytest

from app.quant.factor_backtest import DecileResult, FactorResult, ICResult
from app.quant.gate import GateConfig, ScopeInputs, evaluate_gate
from app.quant.labels import make_label
from app.quant.survival import SurvivalResult
from app.quant.validation import DeflatedSharpe
from app.quant.walk_forward import WalkForwardReport

CFG = GateConfig()
TEL_OK = {"schema": "sentinel-telemetry-1", "since": 1, "days": 30, "entry_coverage": 0.61, "n_entries": 20}


def fr(h, *, mean=0.05, t=3.0, step=None, monotone=0.8, net_ls=0.02, net_t=2.5, insufficient=False, controls=()):
    return FactorResult(factor="quality", horizon=h, lookback=6, min_depth=3000.0, step=step or h,
                        ic=ICResult(n_dates=50, mean_ic=mean, t_stat=t), deciles=DecileResult(),
                        deciles_net=DecileResult(mean_long_short=net_ls, spread_t_stat=net_t), label=f"ret_log_{h}",
                        overlapping=(step or h) < h, monotone_score=monotone, insufficient_data=insufficient,
                        controls=list(controls))


def wf(n_oos, mean=0.03, t=2.5, decay=0.6):
    return WalkForwardReport(track="factor", scope="", mode="expanding", return_basis="ic", horizon_bars=7,
                             n_folds=n_oos, n_oos_obs=10, fold_coverage_days=1.0, n_oos_folds=n_oos,
                             pooled_oos={"mean_ic": mean, "t_stat": t}, decay_ratio={"mean_ic": decay})


def sv(n_events=12, ratio=2.5):
    return SurvivalResult(scope="", factor="quality", horizon_days=90, step_days=90, min_depth=0, n_names=50,
                          n_obs=100, n_events=n_events, n_event_dates=5, n_events_by_kind={"dereg": n_events},
                          n_censored=0, death_ratio_low_over_high=ratio)


def ds(dsr=0.97):
    return DeflatedSharpe(sharpe=0.5, skew=0, kurtosis=3, n_trades=50, n_trials=10, sr0=0.2, psr_zero=0.99, dsr=dsr)


def inputs(scope, **kw):
    h = 42 if scope == "live" else 7
    base = dict(factor_res=fr(h), controlled_res=fr(h, controls=("size",)), dsr=ds(), wf=wf(6), surv=sv())
    base.update({k: (v(h) if callable(v) else v) for k, v in kw.items()})
    return ScopeInputs(horizon_bars=h, **base)


def gate(factor="quality", live=None, surv=None, n_trials=10, telemetry=TEL_OK, **kw):
    return evaluate_gate(factor=factor, label=make_label("ret_log", 7), live=live or inputs("live"),
                         surv=surv or inputs("survivorship"), n_trials=n_trials, trial_sharpe_var=0.04, cfg=CFG,
                         observe_telemetry=telemetry, provenance={"spec_version": "ts-1.0"}, generated_at=123, **kw)


def test_all_pass_and_shape():
    v = gate()
    assert v["gate_pass"] and v["gate_pass_return_route"] and v["promotable"] and v["pending"] == []
    assert set(v["criteria"]) == {"c1_ic_both_scopes", "c2_survives_controls", "c3_deciles_net", "c4_dsr",
                                  "c5_walk_forward", "c6_survival", "c7_observe_telemetry", "u1_upsize"}
    for k, c in v["criteria"].items():
        assert {"pass", "rule", "reason"} <= set(c)
        if k not in ("c7_observe_telemetry", "u1_upsize"):
            assert {"live", "survivorship"} <= set(c)
    assert isinstance(v["generated_at"], int) and v["upsize_gate_pass"] == v["criteria"]["u1_upsize"]["pass"]
    assert set(v["checks"]) == {"rank_ic_both_scopes", "size_control", "decile_monotone_net", "dsr", "walk_forward",
                                "survival_route", "observe_telemetry_30d"}
    json.dumps(v, allow_nan=False)


def test_c1_overlapping_and_failures():
    v = gate(live=inputs("live", factor_res=lambda h: fr(h, step=h // 2)))
    assert v["criteria"]["c1_ic_both_scopes"]["pass"] is False
    assert "overlapping_steps" in v["criteria"]["c1_ic_both_scopes"]["reason"]
    v = gate(surv=inputs("survivorship", factor_res=lambda h: fr(h, t=1.9)))
    assert v["criteria"]["c1_ic_both_scopes"]["pass"] is False
    v = gate(surv=inputs("survivorship", factor_res=lambda h: fr(h, insufficient=True)))
    assert "insufficient_data" in v["criteria"]["c1_ic_both_scopes"]["reason"]


def test_c2_c3():
    v = gate(surv=inputs("survivorship", controlled_res=lambda h: fr(h, t=0.5)))
    assert v["criteria"]["c2_survives_controls"]["pass"] is False and not v["gate_pass_return_route"]
    v = gate(live=inputs("live", factor_res=lambda h: fr(h, monotone=0.5)))
    assert v["criteria"]["c3_deciles_net"]["pass"] is False
    v = gate(live=inputs("live", factor_res=lambda h: fr(h, net_t=1.0)))
    assert v["criteria"]["c3_deciles_net"]["pass"] is False


def test_c4():
    assert gate(n_trials=1)["criteria"]["c4_dsr"]["reason"] == "trial_log_too_small"
    assert gate(live=inputs("live", dsr=ds(0.9)))["criteria"]["c4_dsr"]["pass"] is False
    assert gate(live=inputs("live", dsr=ds(float("nan"))))["criteria"]["c4_dsr"]["pass"] is False


def test_c5_scope_rules():
    v = gate(live=inputs("live", wf=wf(3, t=0.1)))           # advisory live, passing 6-fold survivorship
    c5 = v["criteria"]["c5_walk_forward"]
    assert c5["pass"] is True and c5["live"]["advisory_only"] is True
    assert gate(surv=inputs("survivorship", wf=wf(6, t=1.5)))["criteria"]["c5_walk_forward"]["pass"] is False
    assert gate(surv=inputs("survivorship", wf=wf(6, decay=float("nan"))))["criteria"]["c5_walk_forward"][
        "pass"] is False
    v = gate(live=inputs("live", wf=wf(2)), surv=inputs("survivorship", wf=wf(3)))
    assert v["criteria"]["c5_walk_forward"]["reason"] == "all_scopes_advisory"


def test_c6():
    v = gate(surv=inputs("survivorship", surv=sv(ratio=math.inf)))
    c6 = v["criteria"]["c6_survival"]
    assert c6["pass"] is True and c6["survivorship"]["ratio"] is None and c6["survivorship"]["ratio_inf"] is True
    assert gate(surv=inputs("survivorship", surv=sv(n_events=9)))["criteria"]["c6_survival"]["reason"] == \
        "survivorship:n_events<10"
    for f in ("velocity", "P6"):
        assert gate(factor=f)["criteria"]["c6_survival"] == {**gate(factor=f)["criteria"]["c6_survival"],
                                                            "pass": None, "reason": "not_health"}


def test_survival_route_alone():
    bad = inputs("survivorship", factor_res=lambda h: fr(h, t=0.1))
    v = gate(surv=bad)
    assert not v["gate_pass_return_route"] and v["gate_pass_survival_route"] and v["gate_pass"]


def test_c7():
    v = gate(telemetry=None)
    assert v["criteria"]["c7_observe_telemetry"]["pass"] is None and v["gate_pass"] is False
    assert v["pending"] == ["c7"] and v["observe_telemetry"] is None
    assert gate(telemetry={**TEL_OK, "n_entries": 19})["criteria"]["c7_observe_telemetry"]["pass"] is False
    t = dict(TEL_OK)
    t.pop("n_entries")
    assert gate(telemetry=t)["criteria"]["c7_observe_telemetry"]["pass"] is None
    t = dict(TEL_OK)
    t["live_signal_coverage"] = t.pop("entry_coverage")
    assert gate(telemetry=t)["criteria"]["c7_observe_telemetry"]["pass"] is True
    assert gate(telemetry={**TEL_OK, "schema": "x-2"})["criteria"]["c7_observe_telemetry"]["reason"] == \
        "telemetry_schema_unknown"
    assert gate(telemetry={**TEL_OK, "days": 29})["criteria"]["c7_observe_telemetry"]["pass"] is False
    assert gate(telemetry={**TEL_OK, "entries": [1, 2]})["warnings"] == ["telemetry_entries_length_mismatch"]


@pytest.mark.parametrize("t,folds,c6_ok,expect", [(3.1, 4, True, True), (2.9, 6, True, False), (3.5, 3, True, False),
                                                  (3.5, 6, False, False)])
def test_u1(t, folds, c6_ok, expect):
    v = gate(live=inputs("live", wf=wf(3)), surv=inputs("survivorship", wf=wf(folds, t=t),
                                                         surv=sv() if c6_ok else sv(n_events=1)))
    assert v["upsize_gate_pass"] is expect


def test_promotable():
    assert gate(factor="quality_cg")["promotable"] is False and gate(factor="quality_cg")["gate_pass"] is True
    assert gate(factor="composite_smoothed")["promotable"] is True
