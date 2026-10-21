"""Feature-store contract over 03's ``metric_values`` (spec 12 §2.2, §3.1; 02 §B3–§B5). No DDL here.

``put_metric_values`` (03's single writer) is re-exported; ``SELECT_RAW_SQL`` is §2.6's PIT selection statement;
``write_norm_rows`` writes ``stage='norm'`` rows insert-once; ``prune_norm_provisional`` deletes provisional norm rows
(03's delete trigger refuses official ones). ``metric_at`` is 03's reader and is not redefined here.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, Sequence

from app.storage.pit import metric_at, put_metric_values, upsert_metric_values  # noqa: F401  (02 §B4/§B5)

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

SELECT_RAW_SQL = (
    "SELECT occupant_key, as_of, value, status, value_json, observed_at, source, run_id, backfilled "
    "FROM metric_values WHERE metric_key = :k AND stage = 'raw' AND spec_version = :raw_sv "
    "AND as_of <= :slot AND as_of >= :slot - :fresh_s "
    "AND (observed_at <= :cutoff OR (:reconstructed = 1 AND backfilled = 1)) "
    "ORDER BY occupant_key, as_of DESC"
)


def select_series_sql(n: int) -> str:
    """Exact-``as_of`` matches (flag series) for ``n`` as_of values, same PIT admission rule."""
    marks = ",".join("?" * n)
    return ("SELECT occupant_key, as_of, value, status, value_json, observed_at, source, run_id, backfilled "
            "FROM metric_values WHERE metric_key = ? AND stage = 'raw' AND spec_version = ? "
            f"AND as_of IN ({marks}) AND (observed_at <= ? OR (? = 1 AND backfilled = 1)) "
            "ORDER BY occupant_key, as_of DESC")


async def write_norm_rows(db: "Database", rows: Sequence[Mapping[str, Any]]) -> int:
    """``stage='norm'`` rows through 03's writer (insert-once, ``ON CONFLICT DO NOTHING``)."""
    for r in rows:
        if r.get("stage") != "norm":
            raise ValueError("write_norm_rows writes stage='norm' rows only")
    return await put_metric_values(db, rows)


async def prune_norm_provisional(db: "Database", before_as_of: int, batch: int = 10_000) -> int:
    """Delete provisional (``as_of % 86400 <> 0``) ``stage='norm'`` rows older than ``before_as_of``, in batches."""
    total = 0
    while True:
        async with db.transaction():
            res = await db.execute(
                "DELETE FROM metric_values WHERE (occupant_key, metric_key, as_of, spec_version, stage) IN ("
                "SELECT occupant_key, metric_key, as_of, spec_version, stage FROM metric_values "
                "WHERE stage = 'norm' AND as_of % 86400 <> 0 AND as_of < ? LIMIT ?)", (int(before_as_of), int(batch)))
        n = max(0, res.rowcount)
        total += n
        if n < batch:
            return total
