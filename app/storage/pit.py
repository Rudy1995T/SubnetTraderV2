"""Point-in-time metric storage (spec 03 §2.5–§2.6, 02 §B3–§B5).

``put_metric_values`` is the single ``metric_values`` writer: raw rows are first-success-wins (a failed/none row may
be upgraded by a later success), norm rows are insert-once. ``metric_at`` returns the value knowable at ``t``.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from app.storage.catalog import METRIC_KEYS, RETIRED_METRIC_KEYS, SOURCES

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

RAW_STATUSES = frozenset({"ok", "absent", "failed", "not_applicable"})
_COLS = ("occupant_key", "metric_key", "as_of", "spec_version", "stage", "value", "value_json", "observed_at",
         "source", "run_id", "backfilled")

_RAW_SQL = (
    "INSERT INTO metric_values (occupant_key, metric_key, as_of, spec_version, stage, value, value_json, "
    "observed_at, source, run_id, backfilled) VALUES (?,?,?,?,'raw',?,?,?,?,?,?) "
    "ON CONFLICT (occupant_key, metric_key, as_of, spec_version, stage) DO UPDATE SET "
    "value = excluded.value, value_json = excluded.value_json, observed_at = excluded.observed_at, "
    "source = excluded.source, run_id = excluded.run_id, backfilled = excluded.backfilled "
    "WHERE metric_values.status IN ('failed','none') "
    "AND COALESCE(json_extract(excluded.value_json, '$.status'), 'none') NOT IN ('failed','none') "
    "AND excluded.observed_at >= metric_values.observed_at"
)
_NORM_SQL = (
    "INSERT INTO metric_values (occupant_key, metric_key, as_of, spec_version, stage, value, value_json, "
    "observed_at, source, run_id, backfilled) VALUES (?,?,?,?,'norm',?,?,?,?,?,?) ON CONFLICT DO NOTHING"
)


@dataclass(frozen=True)
class MetricRow:
    occupant_key: str
    metric_key: str
    as_of: int
    spec_version: str
    stage: str
    value: float | None
    status: str
    value_json: dict | None
    observed_at: int
    source: str
    run_id: str
    backfilled: bool = False

    @classmethod
    def from_row(cls, r: Mapping[str, Any]) -> "MetricRow":
        vj = r.get("value_json")
        return cls(r["occupant_key"], r["metric_key"], int(r["as_of"]), r["spec_version"], r["stage"], r["value"],
                   r["status"], json.loads(vj) if vj else None, int(r["observed_at"]), r["source"], r["run_id"],
                   bool(r["backfilled"]))


def _validate(row: Mapping[str, Any]) -> tuple:
    key = row["metric_key"]
    if key in RETIRED_METRIC_KEYS:
        raise ValueError(f"metric_key {key!r} is retired; use {RETIRED_METRIC_KEYS[key]!r} (02 §C2/§C3)")
    spec = METRIC_KEYS.get(key)
    if spec is None:
        raise ValueError(f"unknown metric_key {key!r} (not in catalog.METRIC_KEYS)")
    stage = row.get("stage", "raw")
    if stage not in ("raw", "norm"):
        raise ValueError(f"stage must be raw|norm, got {stage!r}")
    vj = row.get("value_json")
    if isinstance(vj, str):
        vj = json.loads(vj)
    if stage == "raw":
        status = (vj or {}).get("status")
        if status not in RAW_STATUSES:
            raise ValueError(f"{key}: raw row needs value_json.status in {sorted(RAW_STATUSES)}, got {status!r}")
    elif spec.role != "scored":
        raise ValueError(f"{key}: norm rows are only written for role='scored' keys (role={spec.role})")
    src = row["source"]
    if src not in SOURCES:
        raise ValueError(f"source {src!r} is not in the 02 §B9 vocabulary")
    body = json.dumps(vj, separators=(",", ":"), sort_keys=True, default=str) if vj is not None else None
    value = row.get("value")
    return (row["occupant_key"], key, int(row["as_of"]), row["spec_version"], stage,
            None if value is None else float(value), body, int(row["observed_at"]), src, row["run_id"],
            1 if row.get("backfilled") else 0)


async def put_metric_values(db: "Database", rows: Sequence[Mapping[str, Any]]) -> int:
    """Insert (or upgrade a failed raw row); returns rows inserted or upgraded."""
    prepared = [_validate(r) for r in rows]
    if not prepared:
        return 0
    n = 0
    async with db.transaction():
        for p in prepared:
            stage = p[4]
            params = p[:4] + p[5:]
            res = await db.execute(_RAW_SQL if stage == "raw" else _NORM_SQL, params)
            n += max(0, res.rowcount)
    return n


upsert_metric_values = put_metric_values


async def metric_at(db: "Database", occupant_key: str, metric_key: str, t: int, *, spec_version: str,
                    freshness_s: int, official_only: bool = False, strict_backfill: bool = False) -> MetricRow | None:
    row = await db.fetchone(
        "SELECT * FROM metric_values WHERE occupant_key = ? AND metric_key = ? AND spec_version = ? AND stage = 'raw' "
        "AND as_of <= ? AND ? - as_of <= ? AND observed_at <= ? AND (? = 0 OR as_of % 86400 = 0) "
        "AND (? = 0 OR backfilled = 0) ORDER BY (status IN ('failed','none')), as_of DESC LIMIT 1",
        (occupant_key, metric_key, spec_version, int(t), int(t), int(freshness_s), int(t),
         1 if official_only else 0, 1 if strict_backfill else 0),
    )
    return MetricRow.from_row(row) if row else None


async def cross_section(db: "Database", metric_key: str, as_of: int, *, spec_version: str, stage: str = "norm",
                        occupant_keys: Sequence[str] | None = None) -> dict[str, MetricRow]:
    rows = await db.fetchall(
        "SELECT * FROM metric_values WHERE metric_key = ? AND stage = ? AND spec_version = ? AND as_of = ?",
        (metric_key, stage, spec_version, int(as_of)),
    )
    out = {r["occupant_key"]: MetricRow.from_row(r) for r in rows}
    if occupant_keys is not None:
        wanted = set(occupant_keys)
        out = {k: v for k, v in out.items() if k in wanted}
    return out


async def latest_scores(db: "Database", spec_version: str, *, include_prelaunch: bool = False,
                        include_insufficient: bool = False) -> list[dict]:
    return await db.fetchall(
        "SELECT s.* FROM scores s JOIN (SELECT occupant_key, MAX(as_of) AS as_of FROM scores WHERE spec_version = ? "
        "GROUP BY occupant_key) l ON l.occupant_key = s.occupant_key AND l.as_of = s.as_of "
        "WHERE s.spec_version = ? AND (? = 1 OR s.status = 'active') AND (? = 1 OR s.insufficient_data = 0)",
        (spec_version, spec_version, 1 if include_prelaunch else 0, 1 if include_insufficient else 0),
    )
