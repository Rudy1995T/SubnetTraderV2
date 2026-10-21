"""``python -m app.quant …`` — the validation-harness CLI (spec 13 §8.7).

Exit codes: 0 results written · 1 results written but ``insufficient_data`` · 2 refused/error · 3 ``QUANT_ENABLED``
is false. Every subcommand ends with one machine line ``RESULT {"files": […], "verdict_file": …, "gate_pass": …,
"insufficient_data": …}``. Offline, read-only, no network.
"""
from __future__ import annotations

import argparse
import json
import math
import random
import shlex
import sqlite3
import sys
from dataclasses import asdict
from pathlib import Path

import app.config as app_config
from app.util.time import now_ts, parse_iso, to_iso

from . import backtestability as bt
from .economics import EconomicsConfig
from .factor_backtest import (
    DecileResult,
    FactorResult,
    ICResult,
    deflate_factors,
    resolve_factor,
    run_factor_study,
)
from .gate import GateConfig, ScopeInputs, evaluate_gate
from .labels import LabelSource, make_label
from .panel_builder import PanelRequest, coverage_path_for, load_factor_panel, scope_defaults
from .provenance import collect, db_info, gate_thresholds
from .rank_stability import rank_stability
from .report import (
    factor_payload,
    print_factor_report,
    result_path,
    results_dir,
    save_factor_json,
    stamp,
    summary_lines,
    write_json,
)
from .snapshot import RefusedError, git_sha, make_snapshot, missing_relations, newest_backup, prune, resolve_db
from .survival import SurvivalResult, TercileResult, survival_study
from .trials import TrialLog
from .validation import deflated_sharpe_ratio
from .walk_forward import FactorFoldRow, WalkForwardReport, _report_dict, folds_for, walk_forward_factors

DAY = 86400
SCOPES = ("live", "survivorship")


class UsageError(Exception):
    """Refused input (exit 2)."""


def _settings():
    return app_config.settings


def _csv(s: str | None) -> list[str]:
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def _ints(s: str | None) -> list[int]:
    try:
        return [int(x) for x in _csv(s)]
    except ValueError as exc:
        raise UsageError(f"expected comma-separated integers, got {s!r}") from exc


def _date(s: str | None, end: bool = False) -> int | None:
    if not s:
        return None
    try:
        t = parse_iso(f"{s}T00:00:00Z")
    except ValueError as exc:
        raise UsageError(f"bad date {s!r} (YYYY-MM-DD)") from exc
    return t + DAY if end else t


def _scopes(s: str) -> list[str]:
    return list(SCOPES) if s == "both" else [s]


def _result_line(files: list, verdict: str | None = None, gate_pass: bool | None = None,
                 insufficient: bool = False) -> None:
    print("RESULT " + json.dumps({"files": [str(f) for f in files], "verdict_file": verdict, "gate_pass": gate_pass,
                                  "insufficient_data": bool(insufficient)}))


# ── run context ──────────────────────────────────────────────────────────────────────────────────────────────────
class Ctx:
    def __init__(self, args: argparse.Namespace, command: str) -> None:
        s = _settings()
        self.args = args
        self.command = command
        self.db_path = resolve_db(getattr(args, "db", None))
        con = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        try:
            miss = missing_relations(con)
        finally:
            con.close()
        if miss:
            raise UsageError("snapshot lacks required relations/columns: " + ", ".join(miss))
        self.db = db_info(self.db_path)
        self.warnings: list[str] = []
        self.coverage = bt.load_coverage(coverage_path_for(self.db_path), self.warnings)
        self.spec_version = getattr(args, "spec_version", None) or s.QUANT_SPEC_VERSION
        self.pit_mode = getattr(args, "pit_mode", None) or s.QUANT_PIT_MODE
        if self.pit_mode not in ("strict", "reconstructed"):
            raise UsageError(f"--pit-mode {self.pit_mode!r}")
        if self.pit_mode == "reconstructed" and self.coverage is None and "coverage_json_missing" in self.warnings:
            print("warning: coverage.json missing — reconstructed mode uses RECONSTRUCTIBLE_KEYS alone",
                  file=sys.stderr)
        if self.pit_mode == "reconstructed" and self.coverage is not None and not self.coverage.golden_ok:
            print("warning: coverage.json rpc_golden_block != pass", file=sys.stderr)
        self.seed = int(getattr(args, "seed", None) if getattr(args, "seed", None) is not None else s.QUANT_SEED)
        random.seed(self.seed)
        self.start = _date(getattr(args, "start", None))
        self.end = _date(getattr(args, "end", None), end=True)
        self.trials = TrialLog(results_dir() / "trials.json")
        self._labels: LabelSource | None = None
        self._panels: dict[str, object] = {}
        self.git_sha = git_sha()

    def panel(self, scope: str, names) -> object:
        """The scope's panel; one cached per scope and reused while its PIT keys cover the request."""
        keys = bt.pit_keys_for(names)
        hit = self._panels.get(scope)
        if hit is not None and set(keys) <= set(hit.pit_keys):
            return hit
        req = PanelRequest(scope=scope, start=self.start, end=self.end, pit_keys=keys, pit_mode=self.pit_mode,
                           spec_version=self.spec_version, reset_ratio=_settings().QUANT_RESET_DROP_RATIO)
        panel = load_factor_panel(req, db_path=self.db_path, coverage=self.coverage, warnings=self.warnings)
        self._panels[scope] = panel
        return panel

    def labels(self, panel) -> LabelSource:
        if self._labels is None:
            s = _settings()
            con = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
            try:
                self._labels = LabelSource.load(con, occupants=panel.meta["_occupants"],
                                                made_at=self.db.get("made_at") or now_ts(), top_n=s.QUANT_GATE_TOP_N,
                                                zero_run_days=s.QUANT_ZERO_RUN_DAYS,
                                                zero_eps_pct=s.QUANT_ZERO_EMISSION_EPS_PCT,
                                                death_floor=s.QUANT_DEATH_FLOOR_RETURN)
            finally:
                con.close()
        return self._labels

    def holes(self, panel) -> set[int]:
        if self.coverage is None:
            return set()
        return {t for t in panel.dates if self.coverage.in_hole(t)}

    def provenance(self, panel, names, *, label_end=None, economics=None, thresholds=None) -> dict:
        cmd = "python -m app.quant " + " ".join(shlex.quote(a) for a in sys.argv[1:]) if sys.argv[1:] else self.command
        p = collect(self.db, command=cmd, pit_mode=self.pit_mode, spec_version=self.spec_version, seed=self.seed,
                    panel_meta=panel.meta if panel is not None else None, label_end=label_end,
                    lookahead=bt.lookahead_caveats(names), economics=economics, thresholds=thresholds)
        return p.as_dict()

    def coverage_block(self, panel, n_hole_dates: int = 0) -> dict:
        m = panel.meta
        return {"regime_coverage": m.get("regime_coverage", 0.0), "occupant_coverage": m.get("occupant_coverage", 0.0),
                "n_hole_dates": n_hole_dates, "dead_occupant_chain_daily": m.get("dead_occupant_chain_daily"),
                "low_completeness_months": self.coverage.low_completeness_months if self.coverage else []}

    def all_warnings(self, panel=None, *, uses_chain_daily: bool = False) -> list[str]:
        out = list(self.warnings)
        for w in (panel.meta.get("warnings", []) if panel is not None else []):
            if w not in out:
                out.append(w)
        dc = panel.meta.get("dead_occupant_chain_daily", {}) if panel is not None else {}
        if uses_chain_daily and dc.get("n_dead") and dc["n_with_chain_daily"] < 0.5 * dc["n_dead"] \
                and "chain_daily_excludes_dead_occupants" not in out:
            out.append("chain_daily_excludes_dead_occupants")
        return out

    def trial(self, **kw) -> dict:
        base = {"pit_mode": self.pit_mode, "spec_version": self.spec_version,
                "start": getattr(self.args, "start", None), "end": getattr(self.args, "end", None)}
        base.update(kw)
        if isinstance(base.get("max_depth"), float) and math.isinf(base["max_depth"]):
            base["max_depth"] = None
        return base

    def start_trials(self, tuples: list[dict]) -> list[str]:
        cmd = "python -m app.quant " + " ".join(sys.argv[1:]) if sys.argv[1:] else self.command
        return self.trials.start(tuples, command=cmd, git_sha=self.git_sha, db_sha256=self.db["sha256"])


def _validate_names(names: list[str]) -> None:
    for n in names:
        bt.classify(n)


def _economics(args) -> EconomicsConfig | None:
    s = _settings()
    if getattr(args, "no_economics", False):
        return None
    return EconomicsConfig(enabled=True, fee_rate=s.QUANT_FEE_RATE, impact_scale=s.QUANT_IMPACT_SCALE,
                           carry_model="off")


def _default_horizons(family: str, scope: str) -> list[int]:
    s = _settings()
    if family == "ret_log":
        return _ints(s.QUANT_HORIZONS_24H if scope == "survivorship" else s.QUANT_HORIZONS_4H)
    return {"demis": [28, 84], "death": [s.QUANT_SURVIVAL_HORIZON_DAYS],
            "gate_cross": [s.QUANT_GATE_CROSS_DAYS]}[family]


def _controls_arg(args, default: str = "") -> list[str]:
    raw = args.controls if args.controls is not None else default
    return [bt.resolve_control(c) for c in _csv(raw)]


def _deflated_block(results, horizon: int, n: int, v: float | None, orient: bool) -> dict:
    d = deflate_factors(results, horizon=horizon, n_trials=max(n, 1), trial_sharpe_variance=v, orient=orient)
    return {f: {**asdict(ds), "trial_sharpe_var": v, "orient": orient} for f, ds in d.items()}


# ── factor ───────────────────────────────────────────────────────────────────────────────────────────────────────
def run_factor_scope(ctx: Ctx, scope: str, *, factors, horizons, family, controls, lookback, min_depth, max_depth,
                     min_names, step, trade_tao, economics, deflate, output, scope_suffix, do_print,
                     deflate_orient=True, tag=None):
    names = list(factors) + list(controls)
    tuples = []
    panel_grid = scope_defaults(scope)[0]
    for f in factors:
        for h in horizons:
            lab = make_label(family, h)
            tuples.append(ctx.trial(factor=f, label=lab.id, horizon=h, scope=scope,
                                    controls=[c for c in controls if c != f], lookback=lookback, min_depth=min_depth,
                                    max_depth=max_depth, step=step or lab.step_bars(panel_grid)))
    keys = ctx.start_trials(tuples)
    panel = ctx.panel(scope, names)
    labels = ctx.labels(panel)
    holes = ctx.holes(panel)
    results = run_factor_study(panel, factors=list(factors), horizons=list(horizons), label_family=family,
                               lookback=lookback, min_depth=min_depth, min_names=min_names, step=step,
                               trade_tao=trade_tao or None, controls=list(controls) or None, max_depth=max_depth,
                               economics=economics, label_source=labels, hole_dates=holes,
                               death_floor=_settings().QUANT_DEATH_FLOOR_RETURN, pit_mode=ctx.pit_mode,
                               min_ic_dates=_settings().QUANT_MIN_IC_DATES)
    deflated = None
    if deflate:
        n, v = ctx.trials.stats()
        deflated = _deflated_block(results, min(horizons), n, v, deflate_orient)
    prov = ctx.provenance(panel, names, label_end=labels.label_end if labels else None, economics=economics)
    n_holes = max((r.n_hole_dates for by_h in results.values() for r in by_h.values()), default=0)
    payload = factor_payload(results, panel, min_depth=min_depth, lookback=lookback,
                             warnings=ctx.all_warnings(panel, uses_chain_daily=family != "ret_log"),
                             coverage=ctx.coverage_block(panel, n_holes),
                             provenance=prov, deflated=deflated)
    path = result_path("factor", tag or scope, output=output, scope_suffix=scope_suffix)
    files = save_factor_json(payload, results, path)
    fin = {}
    for k, (f, h) in zip(keys, [(f, h) for f in factors for h in horizons]):
        r = results[f][h]
        fin[k] = {"ic_ir": r.ic.ic_ir, "n_dates": r.ic.n_dates}
    ctx.trials.finish(fin)
    if do_print:
        print_factor_report(results, panel, min_depth=min_depth, lookback=lookback)
    for line in summary_lines(results, scope):
        print(line)
    return results, panel, files, payload


def cmd_factor(args) -> int:
    s = _settings()
    factors = _csv(args.factors)
    if not factors:
        raise UsageError("--factors is required")
    controls = _controls_arg(args)
    _validate_names(factors + controls)
    ctx = Ctx(args, "factor")
    files: list[Path] = []
    insufficient = False
    for scope in _scopes(args.scope):
        horizons = _ints(args.horizons) if args.horizons else _default_horizons(args.label, scope)
        results, _, fs, payload = run_factor_scope(
            ctx, scope, factors=factors, horizons=horizons, family=args.label, controls=controls,
            lookback=args.lookback if args.lookback is not None else s.QUANT_LOOKBACK_BARS,
            min_depth=args.min_depth if args.min_depth is not None else s.QUANT_MIN_DEPTH_TAO,
            max_depth=args.max_depth, min_names=args.min_names, step=args.step,
            trade_tao=args.trade_tao if args.trade_tao is not None else s.QUANT_TRADE_TAO,
            economics=_economics(args), deflate=args.deflate, output=args.output,
            scope_suffix=scope if args.scope == "both" and args.output else None, do_print=args.print)
        files += fs
        insufficient = insufficient or payload["insufficient_data"]
    _result_line(files, insufficient=insufficient)
    return 1 if insufficient else 0


# ── survival ────────────────────────────────────────────────────────────────────────────────────────────────────
def run_survival_scope(ctx: Ctx, scope: str, factor: str, *, horizon_days, step_days, min_depth, max_depth,
                       output=None, scope_suffix=None, tag=None):
    keys = ctx.start_trials([ctx.trial(factor=factor, label=f"death_{horizon_days}d", horizon=horizon_days,
                                       scope=scope, controls=[], lookback=_settings().QUANT_LOOKBACK_BARS,
                                       min_depth=min_depth, max_depth=max_depth, step=step_days)])
    panel = ctx.panel(scope, [factor])
    labels = ctx.labels(panel)
    res = survival_study(panel, factor_fn=resolve_factor(factor, panel), label_source=labels,
                         horizon_days=horizon_days, step_days=step_days, min_depth=min_depth, max_depth=max_depth,
                         factor=factor, lookback=_settings().QUANT_LOOKBACK_BARS)
    out = asdict(res)
    out["max_depth"] = None if math.isinf(res.max_depth) else res.max_depth
    out["ratio_inf"] = bool(math.isinf(res.death_ratio_low_over_high))
    out["insufficient_data"] = res.n_obs == 0
    out["warnings"] = ctx.all_warnings(panel, uses_chain_daily=True)
    out["provenance"] = ctx.provenance(panel, [factor], label_end=labels.label_end)
    path = write_json(result_path("survival", tag or scope, output=output, scope_suffix=scope_suffix), out)
    ctx.trials.finish({keys[0]: {"ic_ir": None, "n_dates": res.n_dates}})
    ratio = "inf" if out["ratio_inf"] else (f"{res.death_ratio_low_over_high:.2f}"
                                            if math.isfinite(res.death_ratio_low_over_high) else "null")
    print(f"{scope:<12} {factor:<24} n_obs={res.n_obs} n_events={res.n_events} n_event_dates={res.n_event_dates} "
          f"by_kind={res.n_events_by_kind} ratio={ratio} dereg_only_events={res.n_events_dereg_only}")
    return res, path


def cmd_survival(args) -> int:
    s = _settings()
    factors = _csv(args.factors)
    if not factors:
        raise UsageError("--factors is required")
    _validate_names(factors)
    ctx = Ctx(args, "survival")
    files, insufficient = [], False
    for scope in _scopes(args.scope):
        for f in factors:
            res, p = run_survival_scope(
                ctx, scope, f, horizon_days=args.horizon_days or s.QUANT_SURVIVAL_HORIZON_DAYS,
                step_days=args.step_days or s.QUANT_SURVIVAL_HORIZON_DAYS, min_depth=args.min_depth,
                max_depth=args.max_depth, output=args.output,
                scope_suffix=(scope if args.scope == "both" else None) if args.output else None,
                tag=scope if len(factors) == 1 else f"{scope}_{f.replace(':', '-')}")
            files.append(p)
            insufficient = insufficient or res.n_obs == 0
    _result_line(files, insufficient=insufficient)
    return 1 if insufficient else 0


# ── walk-forward ───────────────────────────────────────────────────────────────────────────────────────────────
def run_wf_scope(ctx: Ctx, scope: str, factors: list[str], horizons: list[int], *, test_days, frac, mode, lookback,
                 min_depth, max_depth, min_names, output=None, scope_suffix=None):
    keys = ctx.start_trials([ctx.trial(factor=",".join(factors), label=f"ret_log_{h}", horizon=h, scope=scope,
                                       controls=[], lookback=lookback, min_depth=min_depth, max_depth=max_depth,
                                       step=h) for h in horizons])
    panel = ctx.panel(scope, factors)
    reports = []
    for h in horizons:
        folds = folds_for(panel, horizon=h, test_days=test_days, train_min_fraction=frac, mode=mode)
        reports.append(walk_forward_factors(panel, factors, folds, horizon=h, lookback=lookback, min_depth=min_depth,
                                            min_names=min_names, max_depth=max_depth, scope=scope, mode=mode,
                                            death_floor=_settings().QUANT_DEATH_FLOOR_RETURN))
    out = {"timestamp": to_iso(now_ts()), "reports": [_report_dict(r) for r in reports],
           "warnings": ctx.all_warnings(panel), "provenance": ctx.provenance(panel, factors)}
    path = write_json(result_path("walk_forward", scope, output=output, scope_suffix=scope_suffix), out)
    ctx.trials.finish({k: {"ic_ir": None, "n_dates": r.n_oos_obs} for k, r in zip(keys, reports)})
    for r in reports:
        print(f"{scope:<12} {','.join(factors):<24} h={r.horizon_bars} folds={r.n_folds} n_oos_folds={r.n_oos_folds} "
              f"pooled_oos_ic={r.pooled_oos.get('mean_ic', 0):+.4f} t={r.pooled_oos.get('t_stat', 0):+.2f} — "
              f"{r.gate_note}")
    return reports, path


def cmd_walk_forward(args) -> int:
    s = _settings()
    factors = _csv(args.factors)
    if not factors:
        raise UsageError("--factors is required")
    if args.label != "ret_log":
        raise UsageError("walk-forward accepts --label ret_log only")
    _validate_names(factors)
    ctx = Ctx(args, "walk-forward")
    files, insufficient = [], False
    for scope in _scopes(args.scope):
        grid = scope_defaults(scope)[0]
        horizons = _ints(args.horizons) if args.horizons else [s.QUANT_GATE_HORIZON_DAYS * DAY // grid]
        reports, p = run_wf_scope(
            ctx, scope, factors, horizons, test_days=args.test_days or s.QUANT_WF_TEST_DAYS,
            frac=args.train_min_fraction if args.train_min_fraction is not None else s.QUANT_WF_TRAIN_MIN_FRACTION,
            mode=args.mode, lookback=args.lookback if args.lookback is not None else s.QUANT_LOOKBACK_BARS,
            min_depth=args.min_depth if args.min_depth is not None else s.QUANT_MIN_DEPTH_TAO,
            max_depth=args.max_depth, min_names=args.min_names, output=args.output,
            scope_suffix=scope if args.scope == "both" and args.output else None)
        files.append(p)
        insufficient = insufficient or any(r.n_oos_folds == 0 for r in reports)
    _result_line(files, insufficient=insufficient)
    return 1 if insufficient else 0


# ── rank-stability ─────────────────────────────────────────────────────────────────────────────────────────────
def cmd_rank_stability(args) -> int:
    s = _settings()
    factors = _csv(args.factors)
    if not factors:
        raise UsageError("--factors is required")
    _validate_names(factors)
    ctx = Ctx(args, "rank-stability")
    files, insufficient = [], False
    for scope in _scopes(args.scope):
        lookback = args.lookback if args.lookback is not None else s.QUANT_LOOKBACK_BARS
        keys = ctx.start_trials([ctx.trial(factor=f, label=f"rank_autocorr_lag{args.lag}", horizon=args.lag,
                                           scope=scope, controls=[], lookback=lookback, min_depth=args.min_depth,
                                           max_depth=None, step=1) for f in factors])
        panel = ctx.panel(scope, factors)
        res = [rank_stability(panel, resolve_factor(f, panel), lag=args.lag, lookback=lookback,
                              min_depth=args.min_depth, min_names=args.min_names, factor=f) for f in factors]
        out = {"timestamp": to_iso(now_ts()), "scope": scope, "results": [asdict(r) for r in res],
               "insufficient_data": all(r.n_pairs == 0 for r in res), "warnings": ctx.all_warnings(panel),
               "provenance": ctx.provenance(panel, factors)}
        p = write_json(result_path("rank_stability", scope, output=args.output,
                                   scope_suffix=scope if args.scope == "both" and args.output else None), out)
        ctx.trials.finish({k: {"ic_ir": None, "n_dates": r.n_pairs} for k, r in zip(keys, res)})
        for r in res:
            mac = "null" if r.mean_rank_autocorr is None else f"{r.mean_rank_autocorr:.3f}"
            print(f"{scope:<12} {r.factor:<24} lag={r.lag} n_pairs={r.n_pairs} mean_rank_autocorr={mac}")
        files.append(p)
        insufficient = insufficient or out["insufficient_data"]
    _result_line(files, insufficient=insufficient)
    return 1 if insufficient else 0


# ── report / gate ──────────────────────────────────────────────────────────────────────────────────────────────
def load_telemetry(path: str | None) -> tuple[dict | None, str | None]:
    s = _settings()
    p = s.path(path or s.QUANT_OBSERVE_TELEMETRY_PATH)
    if not p.is_file():
        return None, f"pending: telemetry file absent ({path or s.QUANT_OBSERVE_TELEMETRY_PATH})"
    head = p.read_bytes()[:16]
    if head.startswith(b"SQLite format 3"):
        raise UsageError(f"{p} is a SQLite file — pass 15's sentinel-telemetry-1 JSON export, not the bot DB")
    try:
        d = json.loads(p.read_text())
    except (OSError, ValueError):
        return None, "pending: telemetry file unreadable"
    return (d, None) if isinstance(d, dict) else (None, "pending: telemetry file not an object")


def _gate_common(args) -> tuple[str, int, list[str]]:
    s = _settings()
    factor = args.factor or s.QUANT_GATE_FACTOR
    days = args.horizon_days or s.QUANT_GATE_HORIZON_DAYS
    controls = _controls_arg(args, s.QUANT_CONTROLS)
    _validate_names([factor] + controls)
    for scope in SCOPES:
        grid = scope_defaults(scope)[0]
        hb = days * DAY // grid
        hz = _ints(s.QUANT_HORIZONS_24H if scope == "survivorship" else s.QUANT_HORIZONS_4H)
        if hb not in hz:
            raise UsageError(f"gate horizon {days} d = {hb} bars is not in the {scope} horizon list {hz}")
    return factor, days, controls


def run_all(args, *, extra_horizons: list[int] | None = None) -> int:
    s = _settings()
    factor, days, controls = _gate_common(args)
    telemetry, t_reason = load_telemetry(args.observe_telemetry)
    ctx = Ctx(args, "report")
    eco = _economics(args)
    trade_tao = args.trade_tao if args.trade_tao is not None else s.QUANT_TRADE_TAO
    lookback, min_depth = s.QUANT_LOOKBACK_BARS, s.QUANT_MIN_DEPTH_TAO
    inputs: dict[str, ScopeInputs] = {}
    files: list[Path] = []
    prov_main = None
    for scope in SCOPES:
        grid = scope_defaults(scope)[0]
        hb = days * DAY // grid
        hz = [hb] + [h for h in (extra_horizons or []) if h != hb]
        ctx.panel(scope, [factor] + controls)            # one panel per scope for every study below
        gross, panel, fs, _ = run_factor_scope(
            ctx, scope, factors=[factor], horizons=hz, family="ret_log", controls=[], lookback=lookback,
            min_depth=min_depth, max_depth=math.inf, min_names=s.QUANT_MIN_NAMES, step=None, trade_tao=trade_tao,
            economics=eco, deflate=False, output=None, scope_suffix=None, do_print=False)
        files += fs
        ctrl, _, fs2, _ = run_factor_scope(
            ctx, scope, factors=[factor], horizons=[hb], family="ret_log", controls=controls, lookback=lookback,
            min_depth=min_depth, max_depth=math.inf, min_names=s.QUANT_MIN_NAMES, step=None, trade_tao=trade_tao,
            economics=eco, deflate=False, output=None, scope_suffix=None, do_print=False, tag=f"{scope}_ctrl")
        files += fs2
        n, v = ctx.trials.stats()
        series = gross[factor][hb].ic.ic_series
        dsr = deflated_sharpe_ratio(series, n_trials=max(n, 1), trial_sharpe_variance=v or 0.0) \
            if len(series) >= 2 else None
        wf_reports, p = run_wf_scope(ctx, scope, [factor], [hb], test_days=s.QUANT_WF_TEST_DAYS,
                                     frac=s.QUANT_WF_TRAIN_MIN_FRACTION, mode="expanding", lookback=lookback,
                                     min_depth=min_depth, max_depth=math.inf, min_names=s.QUANT_MIN_NAMES)
        files.append(p)
        surv = None
        if factor in bt.HEALTH_FACTORS:
            surv, p = run_survival_scope(ctx, scope, factor, horizon_days=s.QUANT_SURVIVAL_HORIZON_DAYS,
                                         step_days=s.QUANT_SURVIVAL_HORIZON_DAYS, min_depth=0.0, max_depth=math.inf)
            files.append(p)
        inputs[scope] = ScopeInputs(gross[factor][hb], ctrl[factor][hb], dsr, wf_reports[0], surv, horizon_bars=hb)
        if scope == "survivorship":
            prov_main = ctx.provenance(panel, [factor] + controls, label_end=ctx.labels(panel).label_end,
                                       economics=eco, thresholds=gate_thresholds())
    n, v = ctx.trials.stats()
    return _write_verdict(ctx, factor, days, inputs, n, v, telemetry, t_reason, prov_main, files)


def _write_verdict(ctx, factor, days, inputs, n, v, telemetry, t_reason, prov, files) -> int:
    verdict = evaluate_gate(factor=factor, label=make_label("ret_log", days), live=inputs["live"],
                            surv=inputs["survivorship"], n_trials=n, trial_sharpe_var=v, cfg=GateConfig.from_settings(),
                            observe_telemetry=telemetry, telemetry_reason=t_reason, provenance=prov,
                            trials_file=str(ctx.trials.path))
    vp = write_json(results_dir() / f"gate_verdict_{factor.replace(':', '-')}_{stamp()}.json", verdict)
    files.append(vp)
    for k, c in verdict["criteria"].items():
        print(f"{k:<24} pass={c.get('pass')!s:<5} {c.get('reason') or ''}")
    print(f"gate_pass={verdict['gate_pass']} pending={verdict['pending']} promotable={verdict['promotable']} "
          f"verdict={vp}")
    insufficient = any(getattr(si.factor_res, "insufficient_data", True) for si in inputs.values())
    _result_line(files, verdict=str(vp), gate_pass=verdict["gate_pass"], insufficient=insufficient)
    return 1 if insufficient else 0


# from-results: rebuild the study objects from the newest matching files
def _fr_from_json(d: dict) -> FactorResult:
    ic = ICResult(**{k: v for k, v in d["ic"].items() if k in ICResult.__dataclass_fields__})
    dec = DecileResult(**{k: v for k, v in d["deciles"].items() if k in DecileResult.__dataclass_fields__})
    net = None
    if d.get("deciles_net"):
        net = DecileResult(**{k: v for k, v in d["deciles_net"].items() if k in DecileResult.__dataclass_fields__})
    kw = {k: v for k, v in d.items() if k in FactorResult.__dataclass_fields__ and k not in ("ic", "deciles",
                                                                                          "deciles_net")}
    if kw.get("max_depth") is None:
        kw["max_depth"] = math.inf
    return FactorResult(ic=ic, deciles=dec, deciles_net=net, **kw)


def _newest(dirp: Path, pattern: str, pred) -> tuple[Path, dict] | tuple[None, None]:
    for p in sorted(dirp.glob(pattern), reverse=True):
        try:
            d = json.loads(p.read_text())
        except (OSError, ValueError):
            continue
        if pred(d):
            return p, d
    return None, None


def from_results(args) -> int:
    s = _settings()
    factor, days, controls = _gate_common(args)
    telemetry, t_reason = load_telemetry(args.observe_telemetry)
    dirp = Path(args.from_results)
    if not dirp.is_dir():
        raise UsageError(f"--from-results {dirp} is not a directory")
    spec = args.spec_version or s.QUANT_SPEC_VERSION
    pit = args.pit_mode or s.QUANT_PIT_MODE
    inputs: dict[str, ScopeInputs] = {}
    files: list[Path] = []
    prov = None
    ctx_trials = TrialLog(results_dir() / "trials.json")
    n, v = ctx_trials.stats()

    def ok_prov(d):
        p = d.get("provenance") or {}
        return p.get("spec_version") == spec and p.get("pit_mode") == pit

    for scope in SCOPES:
        hb = days * DAY // scope_defaults(scope)[0]
        key = str(hb)

        def has(d, ctrl):
            r = ((d.get("factors") or {}).get(factor) or {}).get(key)
            return bool(r) and r.get("label") == f"ret_log_{hb}" and bool(r.get("controls")) == ctrl and ok_prov(d)

        gp, gd = _newest(dirp, f"factor_{scope}_2*.json", lambda d: has(d, False))
        cp, cd = _newest(dirp, f"factor_{scope}_ctrl_*.json", lambda d: has(d, True))
        wp, wd = _newest(dirp, f"walk_forward_{scope}_*.json",
                         lambda d: ok_prov(d) and any(r.get("factors") == [factor] and r.get("horizon_bars") == hb
                                                      for r in d.get("reports", [])))
        sp, sd = _newest(dirp, f"survival_{scope}_*.json", lambda d: ok_prov(d) and d.get("factor") == factor)
        if gp is None or cp is None:
            raise UsageError(f"no matching factor results for {factor} ret_log_{hb} ({scope}, {spec}, {pit}) in {dirp}")
        gross = _fr_from_json(gd["factors"][factor][key])
        ctrl = _fr_from_json(cd["factors"][factor][key])
        side = gp.with_name(gp.name.replace("factor_", "ic_series_", 1))
        series = []
        if side.is_file():
            series = (json.loads(side.read_text()).get(factor) or {}).get(key) or []
        dsr = deflated_sharpe_ratio(series, n_trials=max(n, 1), trial_sharpe_variance=v or 0.0) \
            if len(series) >= 2 else None
        wf = None
        if wd is not None:
            r = next(r for r in wd["reports"] if r.get("factors") == [factor] and r.get("horizon_bars") == hb)
            r = dict(r)
            r["rows"] = [FactorFoldRow(**x) for x in r.get("rows", [])]
            r["decay_ratio"] = {k: (math.nan if x is None else x) for k, x in (r.get("decay_ratio") or {}).items()}
            r["pooled_oos_ci"] = None
            wf = WalkForwardReport(**{k: x for k, x in r.items() if k in WalkForwardReport.__dataclass_fields__})
        surv = None
        if sd is not None:
            t = [TercileResult(**{k: (math.nan if x is None and k not in ("label",) else x) for k, x in tt.items()})
                 for tt in sd.get("terciles", [])]
            ratio = math.inf if sd.get("ratio_inf") else (math.nan if sd.get("death_ratio_low_over_high") is None
                                                          else sd["death_ratio_low_over_high"])
            kw = {k: x for k, x in sd.items() if k in SurvivalResult.__dataclass_fields__ and k != "terciles"}
            kw["death_ratio_low_over_high"] = ratio
            kw["death_ratio_dereg_only"] = math.nan if kw.get("death_ratio_dereg_only") is None \
                else kw["death_ratio_dereg_only"]
            kw["max_depth"] = math.inf if kw.get("max_depth") is None else kw["max_depth"]
            surv = SurvivalResult(terciles=t, **kw)
        inputs[scope] = ScopeInputs(gross, ctrl, dsr, wf, surv, horizon_bars=hb)
        files += [p for p in (gp, cp, wp, sp) if p is not None]
        if scope == "survivorship":
            prov = dict(gd.get("provenance") or {})
            prov["thresholds"] = gate_thresholds()

    class _C:
        trials = ctx_trials
    return _write_verdict(_C, factor, days, inputs, n, v, telemetry, t_reason, prov, files)


def cmd_report(args) -> int:
    if bool(args.run_all) == bool(args.from_results):
        raise UsageError("report needs exactly one of --run-all or --from-results DIR")
    return run_all(args) if args.run_all else from_results(args)


def cmd_gate(args) -> int:
    if sorted(_csv(args.scopes)) != sorted(SCOPES):
        raise UsageError("--scopes must be live,survivorship")
    if args.spec:
        args.spec_version = args.spec
    extra = _ints(args.horizons) if args.horizons else None
    return run_all(args, extra_horizons=extra)


# ── snapshot ────────────────────────────────────────────────────────────────────────────────────────────────────
def cmd_snapshot(args) -> int:
    s = _settings()
    src = args.from_path
    if not src:
        bk = newest_backup()
        if bk is None:
            raise UsageError(f"no uncompressed backup found in {s.path(s.DB_BACKUP_DIR)} (taosentinel-<stamp>.db)")
        src = str(bk)
    out = make_snapshot(src, args.out)
    size = out.stat().st_size
    print(f"snapshot written {out} (tables: 11, size {size / 1e6:.1f} MB) source={src}")
    removed = prune() if args.prune else []
    for r in removed:
        print(f"pruned {r}")
    _result_line([out])
    return 0


# ── parser ──────────────────────────────────────────────────────────────────────────────────────────────────────
def _common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--start")
    p.add_argument("--end")
    p.add_argument("--db")
    p.add_argument("--output")
    p.add_argument("--spec-version", dest="spec_version")
    p.add_argument("--seed", type=int)
    p.add_argument("--pit-mode", dest="pit_mode", choices=["strict", "reconstructed"])


def _min_names(v: str) -> int:
    n = int(v)
    if n < 10:
        raise argparse.ArgumentTypeError("--min-names must be >= 10 (spearman_r degenerates below 5 names)")
    return n


def build_parser() -> argparse.ArgumentParser:
    s = _settings()
    p = argparse.ArgumentParser(prog="python -m app.quant", description="TaoSentinel validation harness (spec 13)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("snapshot")
    sp.add_argument("--from", dest="from_path")
    sp.add_argument("--out")
    sp.add_argument("--prune", action="store_true")

    scope_kw = dict(choices=["live", "survivorship", "both"])
    f = sub.add_parser("factor")
    f.add_argument("--factors", required=True)
    f.add_argument("--horizons")
    f.add_argument("--scope", default="survivorship", **scope_kw)
    f.add_argument("--label", default="ret_log", choices=["ret_log", "demis", "death", "gate_cross"])
    f.add_argument("--controls")
    f.add_argument("--lookback", type=int)
    f.add_argument("--min-depth", dest="min_depth", type=float)
    f.add_argument("--max-depth", dest="max_depth", type=float, default=math.inf)
    f.add_argument("--min-names", dest="min_names", type=_min_names, default=s.QUANT_MIN_NAMES)
    f.add_argument("--step", type=int)
    f.add_argument("--trade-tao", dest="trade_tao", type=float)
    f.add_argument("--no-economics", dest="no_economics", action="store_true")
    f.add_argument("--deflate", action="store_true")
    f.add_argument("--print", action="store_true")
    _common(f)

    sv = sub.add_parser("survival")
    sv.add_argument("--factors", required=True)
    sv.add_argument("--scope", default="both", **scope_kw)
    sv.add_argument("--horizon-days", dest="horizon_days", type=int)
    sv.add_argument("--step-days", dest="step_days", type=int)
    sv.add_argument("--min-depth", dest="min_depth", type=float, default=0.0)
    sv.add_argument("--max-depth", dest="max_depth", type=float, default=math.inf)
    _common(sv)

    w = sub.add_parser("walk-forward")
    w.add_argument("--factors", required=True)
    w.add_argument("--horizons")
    w.add_argument("--scope", default="both", **scope_kw)
    w.add_argument("--label", default="ret_log")
    w.add_argument("--test-days", dest="test_days", type=int)
    w.add_argument("--train-min-fraction", dest="train_min_fraction", type=float)
    w.add_argument("--mode", default="expanding", choices=["expanding", "rolling"])
    w.add_argument("--lookback", type=int)
    w.add_argument("--min-depth", dest="min_depth", type=float)
    w.add_argument("--max-depth", dest="max_depth", type=float, default=math.inf)
    w.add_argument("--min-names", dest="min_names", type=_min_names, default=s.QUANT_MIN_NAMES)
    _common(w)

    r = sub.add_parser("rank-stability")
    r.add_argument("--factors", required=True)
    r.add_argument("--scope", default="survivorship", **scope_kw)
    r.add_argument("--lag", type=int, default=1)
    r.add_argument("--lookback", type=int)
    r.add_argument("--min-depth", dest="min_depth", type=float, default=0.0)
    r.add_argument("--min-names", dest="min_names", type=_min_names, default=s.QUANT_MIN_NAMES)
    _common(r)

    for name in ("report", "gate"):
        g = sub.add_parser(name)
        if name == "report":
            g.add_argument("--run-all", dest="run_all", action="store_true")
            g.add_argument("--from-results", dest="from_results")
        else:
            g.add_argument("--spec")
            g.add_argument("--scopes", default="live,survivorship")
            g.add_argument("--horizons")
        g.add_argument("--factor")
        g.add_argument("--horizon-days", dest="horizon_days", type=int)
        g.add_argument("--controls")
        g.add_argument("--trade-tao", dest="trade_tao", type=float)
        g.add_argument("--no-economics", dest="no_economics", action="store_true")
        g.add_argument("--observe-telemetry", dest="observe_telemetry")
        _common(g)
    return p


_HANDLERS = {"snapshot": cmd_snapshot, "factor": cmd_factor, "survival": cmd_survival,
             "walk-forward": cmd_walk_forward, "rank-stability": cmd_rank_stability, "report": cmd_report,
             "gate": cmd_gate}


def main(argv: list[str] | None = None) -> int:
    if not _settings().QUANT_ENABLED:
        print("quant disabled", file=sys.stderr)
        return 3
    args = build_parser().parse_args(argv)
    try:
        return _HANDLERS[args.cmd](args)
    except (RefusedError, UsageError, bt.FactorNameError, KeyError) as exc:
        msg = exc.args[0] if isinstance(exc, KeyError) and exc.args else str(exc)
        print(f"error: {msg}", file=sys.stderr)
        _result_line([])
        return 2
    except OSError as exc:
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        _result_line([])
        return 2


if __name__ == "__main__":
    sys.exit(main())

