"""Source precedence for `precedence`-class tables (spec 03 §3.3).

Upsert WHERE: ``src_rank(excluded.source) > src_rank(stored.source)`` OR (equal rank AND the same-rank rule).
``src_rank`` is registered on every connection as a deterministic SQL function (``rank_any``).
"""
from __future__ import annotations

import re
from typing import Sequence

SOURCE_RANK: dict[str, dict[str, int]] = {
    "chain_hourly": {"sse": 4, "rest_table": 4, "tick_rollup": 3, "analytics_1w": 2, "analytics_1m": 1},
    "chain_daily": {"analytics_1y": 2, "analytics_1w": 1},
    "price_bars": {"sse_rollup": 5, "ledger_import": 4, "tmc_candle": 3, "onfinality": 2,
                   "opentensor_archive": 2, "blockmachine": 2, "taostats": 1},
}

# One global ordering that preserves every per-table ordering above, so a single-argument SQL
# function can serve all three tables (chain_daily's analytics_1y sits above analytics_1w; chain_hourly
# never stores analytics_1y).
_GLOBAL_RANK: dict[str, int] = {
    "sse_rollup": 5, "ledger_import": 4, "tmc_candle": 3, "onfinality": 2, "opentensor_archive": 2, "blockmachine": 2,
    "taostats": 1, "sse": 4, "rest_table": 4, "tick_rollup": 3, "analytics_1y": 3, "analytics_1w": 2, "analytics_1m": 1,
}

# Same-rank predicates per table (03 §3.3). chain_hourly's applies only at rank 4 (live SSE/REST rows).
SAME_RANK_RULE: dict[str, str] = {
    "chain_hourly": "src_rank(excluded.source) = 4 AND excluded.block_number >= chain_hourly.block_number",
    "price_bars": "excluded.ts_last > price_bars.ts_last",
    "chain_daily": "0",
}

_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")


def rank_any(source: str | None) -> int:
    if source is None:
        return 0
    return _GLOBAL_RANK.get(str(source), 0)


def rank(table: str, source: str) -> int:
    return SOURCE_RANK[table].get(source, 0)


def precedence_upsert_sql(table: str, cols: Sequence[str], conflict: Sequence[str], *,
                          coalesce: Sequence[str] = (), keep: Sequence[str] = (),
                          same_rank: str | None = None) -> str:
    """Build ``INSERT … ON CONFLICT DO UPDATE SET … WHERE <outranks>`` for one row of named params.

    ``coalesce``: enrichment columns that take ``COALESCE(excluded.c, stored.c)`` on a winning write.
    ``keep``: columns kept when already set (``COALESCE(stored.c, excluded.c)``, e.g. chain_daily.occupant_key).
    Non-winning writes still fill NULL ``coalesce`` columns via a second statement (see ``enrich_sql``).
    """
    for name in (table, *cols, *conflict, *coalesce, *keep):
        if not _IDENT.match(name):
            raise ValueError(f"bad identifier {name!r}")
    rule = same_rank if same_rank is not None else SAME_RANK_RULE.get(table, "0")
    sets = []
    for c in cols:
        if c in conflict:
            continue
        if c in keep:
            sets.append(f"{c} = COALESCE({table}.{c}, excluded.{c})")
        elif c in coalesce:
            sets.append(f"{c} = COALESCE(excluded.{c}, {table}.{c})")
        else:
            sets.append(f"{c} = excluded.{c}")
    placeholders = ", ".join(f":{c}" for c in cols)
    return (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT ({', '.join(conflict)}) DO UPDATE SET {', '.join(sets)} "
        f"WHERE src_rank(excluded.source) > src_rank({table}.source) "
        f"OR (src_rank(excluded.source) = src_rank({table}.source) AND ({rule}))"
    )


def enrich_sql(table: str, conflict: Sequence[str], columns: Sequence[str]) -> str:
    """NULL-only fill of enrichment columns from a lower/same-ranked source (spec 17 ``enrich_reserves``)."""
    for name in (table, *conflict, *columns):
        if not _IDENT.match(name):
            raise ValueError(f"bad identifier {name!r}")
    sets = ", ".join(f"{c} = COALESCE({c}, :{c})" for c in columns)
    where = " AND ".join(f"{c} = :{c}" for c in conflict)
    nulls = " OR ".join(f"{c} IS NULL" for c in columns)
    return f"UPDATE {table} SET {sets} WHERE {where} AND ({nulls})"
