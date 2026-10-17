"""Provisional P1 matrix (spec 05 §8, Phase 2 only; 14's matrix supersedes it in Phase 11). CLI only.

Normalise each raw P1 row with the master §7.3 normaliser, then P1 = Σ w·m / Σ w over present metrics
(coverage-renormalised; coverage < 0.3 ⇒ None). ``rankg`` runs over active occupants of the same ``as_of``.
"""
from __future__ import annotations

import json
import math
from typing import TYPE_CHECKING, Callable

from app.features.normalise import rankg, zclip
from app.storage.catalog import METRIC_KEYS
from app.util.time import to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

SPEC_VERSION = "ts-1.0"
P1_WEIGHTS = {k: m.weight for k, m in METRIC_KEYS.items() if m.pillar == "P1" and m.role == "scored"}
MIN_COVERAGE = 0.3
RANKG_KEYS = ("emission_share", "liquidity_depth", "chain_buys")


def _log2_zclip(x: float | None) -> float | None:
    if x is None:
        return None
    if x <= 0:
        return 0.0
    return zclip(math.log2(x), -2.0, 1.0)


PER_ROW: dict[str, Callable[[float | None], float | None]] = {
    "gate_margin": _log2_zclip,
    "dereg_safety": lambda x: x,
    "exit_capacity": lambda x: zclip(x, 0.05, 0.6),
    "net_flow_30d": lambda x: zclip(x, -0.5, 0.5),
    "holder_dispersion": lambda x: zclip(x, 0.3, 0.9),
    "participation": lambda x: zclip(x, 0.2, 0.9),
    "miner_burn_ok": lambda x: zclip(x, 0.0, 0.8),
}


def aggregate(metrics: dict[str, float | None], weights: dict[str, float] = P1_WEIGHTS) -> tuple[float | None, float]:
    total = sum(weights.values())
    present = {k: v for k, v in metrics.items() if v is not None and k in weights}
    wsum = sum(weights[k] for k in present)
    coverage = wsum / total if total else 0.0
    if coverage < MIN_COVERAGE or wsum <= 0:
        return None, coverage
    return sum(weights[k] * v for k, v in present.items()) / wsum, coverage


async def p1_matrix(db: "Database", as_of: int) -> list[dict]:
    rows = await db.fetchall(
        "SELECT m.occupant_key, m.metric_key, m.value, m.status, o.netuid, o.name, o.status AS occ_status "
        "FROM metric_values m JOIN occupants o USING (occupant_key) WHERE m.as_of = ? AND m.stage = 'raw' "
        "AND m.spec_version = ? AND m.source = 'chain'", (as_of, SPEC_VERSION))
    occ: dict[str, dict] = {}
    raw: dict[str, dict[str, float | None]] = {k: {} for k in P1_WEIGHTS}
    for r in rows:
        o = occ.setdefault(r["occupant_key"], {"netuid": r["netuid"], "occupant_key": r["occupant_key"],
                                               "name": r["name"], "status": r["occ_status"], "raw": {}, "st": {}})
        if r["metric_key"] in P1_WEIGHTS:
            o["raw"][r["metric_key"]] = r["value"] if r["status"] == "ok" else None
            o["st"][r["metric_key"]] = r["status"]
    active = [k for k, o in occ.items() if o["status"] == "active" and o["netuid"] != 0]
    for k in RANKG_KEYS:
        ranked = rankg({o: occ[o]["raw"].get(k) for o in active})
        raw[k] = ranked
    out = []
    for key in active:
        o = occ[key]
        m: dict[str, float | None] = {}
        for k in P1_WEIGHTS:
            if k in RANKG_KEYS:
                m[k] = raw[k].get(key)
            else:
                m[k] = PER_ROW[k](o["raw"].get(k))
        p1, cov = aggregate(m)
        out.append({"netuid": o["netuid"], "occupant_key": key, "name": o["name"],
                    "p1": None if p1 is None else round(100 * p1, 2), "coverage": round(cov, 3),
                    "metrics": {k: (None if v is None else round(v, 4)) for k, v in m.items()},
                    "raw": o["raw"], "status": o["st"]})
    out.sort(key=lambda r: (r["p1"] is None, -(r["p1"] or 0)))
    return out


def render(rows: list[dict], as_of: int) -> str:
    short = {"emission_share": "emis", "gate_margin": "gate", "dereg_safety": "dsafe", "liquidity_depth": "liq",
             "exit_capacity": "exit", "net_flow_30d": "flow", "chain_buys": "buys", "holder_dispersion": "hold",
             "participation": "part", "miner_burn_ok": "burn"}
    head = f"{'#':>3} {'SN':>4} {'name':<18} {'P1':>6} {'cov':>5} " + " ".join(f"{v:>5}" for v in short.values())
    lines = [f"P1 matrix as_of {to_iso(as_of)} ({len(rows)} active occupants; metrics normalised 0-1)", head,
             "-" * len(head)]
    for i, r in enumerate(rows, 1):
        cells = " ".join(f"{'—':>5}" if r["metrics"][k] is None else f"{r['metrics'][k]:>5.2f}" for k in short)
        p1 = "  —" if r["p1"] is None else f"{r['p1']:6.1f}"
        lines.append(f"{i:>3} {r['netuid']:>4} {(r['name'] or '')[:18]:<18} {p1:>6} {r['coverage']:>5.2f} {cells}")
    return "\n".join(lines)


def to_json(rows: list[dict]) -> str:
    return json.dumps(rows, indent=1, default=str)
