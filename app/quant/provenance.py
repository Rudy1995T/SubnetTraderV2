"""Result provenance (spec 13 §3.5 ``provenance``): code, data and coverage fingerprints for every result file."""
from __future__ import annotations

import os
import sqlite3
from dataclasses import asdict, dataclass, field

import app.config as app_config

from .economics import EconomicsConfig, economics_badge
from .snapshot import db_fingerprint, git_sha, snapshot_meta


@dataclass
class Provenance:
    spec_version: str
    classifier_version: str
    git_sha: str
    schema_version: int | None
    seed: int
    pit_mode: str
    db_snapshot: dict
    label_end: int | None = None
    coverage_json: dict | None = None
    coverage_caveats: list[str] = field(default_factory=list)
    price_sources: dict[str, int] = field(default_factory=dict)
    price_bars_backfilled_frac: float = 0.0
    shadow_rows: int = 0
    economics: dict | None = None
    lookahead_caveats: list[str] = field(default_factory=list)
    evidence_class: str = "chain_state"
    command: str = ""
    thresholds: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return asdict(self)


def db_info(db_path: str) -> dict:
    """Snapshot identity: path, sha256, size, source backup, max observed_at, made_at, classifier, schema."""
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        meta = snapshot_meta(con)
        try:
            schema = con.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]
        except sqlite3.Error:
            schema = None
        if meta.get("max_observed_at"):
            max_obs = int(meta["max_observed_at"])
        else:
            max_obs = max((con.execute(f"SELECT MAX(observed_at) FROM {t}").fetchone()[0] or 0)
                          for t in ("v_quant_price_bars", "v_quant_chain_daily", "metric_values"))
        if "max_classifier_version" in meta:
            cls_ver = meta["max_classifier_version"]
        else:
            try:
                cls_ver = con.execute("SELECT MAX(classifier_version) FROM post_labels").fetchone()[0]
            except sqlite3.Error:
                cls_ver = None
    finally:
        con.close()
    return {"path": db_path, "sha256": db_fingerprint(db_path), "size_bytes": os.path.getsize(db_path),
            "source_backup": meta.get("source_path", db_path), "max_observed_at": int(max_obs or 0),
            "made_at": int(meta["made_at"]) if meta.get("made_at") else None,
            "_classifier_version": str(cls_ver) if cls_ver is not None else "none", "_schema_version": schema}


def collect(db: dict, *, command: str, pit_mode: str, spec_version: str, seed: int, panel_meta: dict | None = None,
            label_end: int | None = None, lookahead: list[str] | None = None,
            economics: EconomicsConfig | None = None, thresholds: dict | None = None) -> Provenance:
    pm = panel_meta or {}
    cov = pm.get("_coverage")
    used_rows, used_bf = pm.get("pit_used_rows", 0), pm.get("pit_used_backfilled", 0)
    evidence = "reconstructed" if used_bf else ("strict" if used_rows else "chain_state")
    look = list(lookahead or [])
    snap = {k: v for k, v in db.items() if not k.startswith("_")}
    return Provenance(
        spec_version=spec_version, classifier_version=db.get("_classifier_version", "none"), git_sha=git_sha(),
        schema_version=db.get("_schema_version"), seed=seed, pit_mode=pit_mode, db_snapshot=snap,
        label_end=label_end, coverage_json=cov.provenance() if cov is not None else None,
        coverage_caveats=list(cov.caveats) if cov is not None else [],
        price_sources=dict(pm.get("price_sources", {})),
        price_bars_backfilled_frac=float(pm.get("price_bars_backfilled_frac", 0.0)),
        shadow_rows=int(pm.get("shadow_rows", 0)), economics=economics_badge(economics), lookahead_caveats=look,
        evidence_class=evidence, command=command, thresholds=dict(thresholds or {}))


def gate_thresholds() -> dict:
    s = app_config.settings
    return {k: getattr(s, k) for k in (
        "QUANT_GATE_HORIZON_DAYS", "QUANT_MIN_FOLDS", "QUANT_DSR_THRESHOLD", "QUANT_DECAY_MIN", "QUANT_MONOTONE_MIN",
        "QUANT_SURVIVAL_HORIZON_DAYS", "QUANT_SURVIVAL_MIN_EVENTS", "QUANT_SURVIVAL_RATIO_MIN",
        "QUANT_OBSERVE_MIN_DAYS", "QUANT_OBSERVE_MIN_COVERAGE", "QUANT_OBSERVE_MIN_ENTRIES", "QUANT_UPSIZE_MIN_T",
        "QUANT_TRADE_TAO", "QUANT_FEE_RATE", "QUANT_IMPACT_SCALE", "QUANT_WF_TEST_DAYS", "QUANT_ZERO_RUN_DAYS",
        "QUANT_ZERO_EMISSION_EPS_PCT", "QUANT_MIN_IC_DATES")}
