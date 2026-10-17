"""``pool_history`` → ``price_bars`` 1h/1d, precedence upsert and NULL-only enrichment (spec 17 §2.5).

Close = the ``MAX(timestamp)`` row of the bucket; ``moving_price`` = the latest non-NULL value inside the same
bucket (never carried across buckets); volume = Δ of the cumulative ``subnet_volume`` (NULL across occupant changes,
gaps > 2 periods and counter resets). ``observed_at``/``backfilled`` are honest: only a live-captured, non-enriched
ledger close (and moving price) keeps ``observed_at = ts_last`` / ``backfilled = 0``.
"""
from __future__ import annotations

import sqlite3
from typing import Iterable

from app.backfill import common
from app.storage.precedence import rank_any

PERIODS = {"5m": 300, "1h": 3600, "1d": 86400}
CHUNK_SEC = 31 * 86400

BAR_SQL = """
WITH s AS (
  SELECT netuid, (timestamp / :p) * :p AS b, timestamp, block_number, price, tao_in_pool, alpha_in_pool,
         subnet_volume, occupant_key, source, enriched_at,
         ROW_NUMBER() OVER w_desc AS rn_last, ROW_NUMBER() OVER w_asc AS rn_first,
         MAX(price) OVER w_all AS hi, MIN(price) OVER w_all AS lo,
         FIRST_VALUE(moving_price) OVER (PARTITION BY timestamp / :p
             ORDER BY moving_price IS NULL, timestamp DESC, block_number DESC) AS mp_last,
         FIRST_VALUE(enriched_at IS NOT NULL OR timestamp < :live_from OR source NOT LIKE 'ledger_%')
             OVER (PARTITION BY timestamp / :p
                   ORDER BY moving_price IS NULL, timestamp DESC, block_number DESC) AS mp_reconstructed
  FROM pool_history
  WHERE netuid = :n AND timestamp >= :from - :p AND timestamp < :to
  WINDOW w_all  AS (PARTITION BY timestamp / :p),
         w_desc AS (PARTITION BY timestamp / :p ORDER BY timestamp DESC, block_number DESC),
         w_asc  AS (PARTITION BY timestamp / :p ORDER BY timestamp ASC,  block_number ASC)),
f AS (SELECT b, price AS open FROM s WHERE rn_first = 1),
l AS (SELECT s.*, f.open FROM s JOIN f USING (b) WHERE s.rn_last = 1),
v AS (SELECT l.*, LAG(subnet_volume) OVER o AS prev_sv, LAG(b) OVER o AS prev_b, LAG(occupant_key) OVER o AS prev_occ
      FROM l WINDOW o AS (ORDER BY b)),
w AS (SELECT v.*, (source NOT LIKE 'ledger_%' OR timestamp < :live_from OR enriched_at IS NOT NULL
                   OR (mp_last IS NOT NULL AND mp_reconstructed)) AS reconstructed FROM v)
SELECT netuid, b AS ts, occupant_key, open, hi AS high, lo AS low, price AS close,
       CASE WHEN prev_b >= b - 2 * :p AND prev_occ IS occupant_key AND subnet_volume >= prev_sv
            THEN subnet_volume - prev_sv END                         AS volume,
       CASE WHEN prev_b >= b - 2 * :p AND prev_occ IS occupant_key THEN prev_sv END AS start_volume,
       NULL AS tao_price_usd, NULL AS tmc_block_number,
       timestamp AS ts_last, timestamp AS event_at, tao_in_pool, alpha_in_pool, mp_last AS moving_price, block_number,
       CASE WHEN source LIKE 'ledger_%' THEN 'ledger_import' ELSE source END AS source,
       CASE WHEN reconstructed THEN :now ELSE timestamp END              AS observed_at,
       CASE WHEN reconstructed THEN 1 ELSE 0 END                         AS backfilled,
       :run_id AS run_id
FROM w WHERE b >= :from
"""

BAR_COLS = ("netuid", "period", "ts", "occupant_key", "open", "high", "low", "close", "volume", "start_volume",
            "tao_price_usd", "tmc_block_number", "source", "event_at", "observed_at", "ts_last", "tao_in_pool",
            "alpha_in_pool", "moving_price", "block_number", "backfilled", "run_id")

UPSERT_SQL = (
    f"INSERT INTO price_bars ({', '.join(BAR_COLS)}) VALUES ({', '.join(':' + c for c in BAR_COLS)}) "
    "ON CONFLICT (netuid, period, ts) DO UPDATE SET "
    "occupant_key = COALESCE(excluded.occupant_key, price_bars.occupant_key), open = excluded.open, "
    "high = excluded.high, low = excluded.low, close = excluded.close, volume = excluded.volume, "
    "start_volume = excluded.start_volume, "
    "tao_price_usd = COALESCE(excluded.tao_price_usd, price_bars.tao_price_usd), "
    "tmc_block_number = excluded.tmc_block_number, source = excluded.source, event_at = excluded.event_at, "
    "observed_at = excluded.observed_at, ts_last = excluded.ts_last, "
    "tao_in_pool = COALESCE(excluded.tao_in_pool, price_bars.tao_in_pool), "
    "alpha_in_pool = COALESCE(excluded.alpha_in_pool, price_bars.alpha_in_pool), "
    "moving_price = COALESCE(excluded.moving_price, price_bars.moving_price), "
    "block_number = COALESCE(excluded.block_number, price_bars.block_number), backfilled = excluded.backfilled, "
    "run_id = excluded.run_id "
    "WHERE src_rank(excluded.source) > src_rank(price_bars.source) "
    "OR (src_rank(excluded.source) = src_rank(price_bars.source) AND excluded.ts_last > price_bars.ts_last) "
    "OR (excluded.source = price_bars.source AND excluded.ts_last = price_bars.ts_last "
    "AND excluded.close = price_bars.close AND ("
    + " OR ".join(f"excluded.{c} IS NOT price_bars.{c}" for c in ("open", "high", "low", "volume", "start_volume"))
    + "))")

RECOMPUTE_COLS = ("close", "open", "high", "low", "volume", "start_volume")

ENRICH_SQL = """
WITH s AS (
  SELECT (timestamp / :p) * :p AS b, tao_in_pool, alpha_in_pool, block_number,
         ROW_NUMBER() OVER (PARTITION BY timestamp / :p ORDER BY timestamp DESC, block_number DESC) AS rn,
         FIRST_VALUE(moving_price) OVER (PARTITION BY timestamp / :p
             ORDER BY moving_price IS NULL, timestamp DESC, block_number DESC) AS mp_last
  FROM pool_history WHERE netuid = :n AND timestamp >= :from AND timestamp < :to)
UPDATE price_bars AS pb SET
  tao_in_pool   = COALESCE(pb.tao_in_pool,   s.tao_in_pool),
  alpha_in_pool = COALESCE(pb.alpha_in_pool, s.alpha_in_pool),
  block_number  = COALESCE(pb.block_number,  s.block_number),
  moving_price  = COALESCE(pb.moving_price,  s.mp_last),
  backfilled    = 1, observed_at = MAX(pb.observed_at, :now)
FROM (SELECT * FROM s WHERE rn = 1) AS s
WHERE pb.netuid = :n AND pb.period = :period AND pb.ts = s.b
  AND ((pb.tao_in_pool IS NULL AND s.tao_in_pool IS NOT NULL) OR (pb.moving_price IS NULL AND s.mp_last IS NOT NULL))
"""


def register_sql_functions(con: sqlite3.Connection) -> None:
    common.register_sql_functions(con)


def _would_win(new: dict, old: tuple | None) -> tuple[bool, str]:
    """``old`` = (source, ts_last, *RECOMPUTE_COLS). Same rank: a later close wins (02 §J1); the same source and close
    sample (same close) wins only when the bucket's OHLC/volume changed, i.e. ``rebuild`` saw more rows of the bucket (rows that
    landed after the bar was first built — order independence, 17 §2.5); an identical recompute is skipped."""
    if old is None:
        return True, "new"
    rn, ro = rank_any(new["source"]), rank_any(old[0])
    if rn > ro:
        return True, "outranks"
    if rn == ro:
        if new.get("ts_last") is not None and old[1] is not None and new["ts_last"] > old[1]:
            return True, "later_close"
        if (new["source"] == old[0] and new.get("ts_last") is not None and new.get("ts_last") == old[1]
                and len(old) > 2 and new.get("close") == old[2]
                and tuple(new.get(c) for c in RECOMPUTE_COLS[1:]) != tuple(old[3:])):
            return True, "recomputed"
        return False, "same_rank_skipped"
    return False, "lower_rank_skipped"


def upsert_bars(con: sqlite3.Connection, rows: list[dict], period: str) -> dict:
    """Precedence upsert (order-independent); one transaction for the batch."""
    out = {"written": 0, "recomputed": 0, "same_rank_skipped": 0, "lower_rank_skipped": 0}
    if not rows:
        return out
    by_n: dict[int, list[dict]] = {}
    for r in rows:
        r = {c: r.get(c) for c in BAR_COLS}
        r["period"] = period
        by_n.setdefault(int(r["netuid"]), []).append(r)
    with common.tx(con):
        for n, rs in by_n.items():
            lo, hi = min(r["ts"] for r in rs), max(r["ts"] for r in rs)
            old = {int(r[0]): tuple(r[1:]) for r in con.execute(
                f"SELECT ts, source, ts_last, {', '.join(RECOMPUTE_COLS)} FROM price_bars "
                "WHERE netuid = ? AND period = ? AND ts BETWEEN ? AND ?", (n, period, lo, hi))}
            win = []
            for r in rs:
                ok, why = _would_win(r, old.get(int(r["ts"])))
                if ok:
                    win.append(r)
                    out["recomputed"] += why == "recomputed"
                else:
                    out[why] += 1
            if win:
                con.executemany(UPSERT_SQL, win)
                out["written"] += len(win)
    return out


def enrich_reserves(con: sqlite3.Connection, netuid: int, period: str, from_ts: int, to_ts: int, *, now: int) -> int:
    p = PERIODS[period]
    with common.tx(con):
        before = con.total_changes       # a WITH-led UPDATE reports rowcount -1 in Python's sqlite3
        con.execute(ENRICH_SQL, {"p": p, "n": netuid, "from": from_ts, "to": to_ts, "period": period, "now": now})
        return con.total_changes - before


def bars_for(con: sqlite3.Connection, netuid: int, period: str, from_ts: int, to_ts: int, *, live_from_ts: int,
             now: int, run_id: str) -> list[dict]:
    p = PERIODS[period]
    cur = con.execute(BAR_SQL, {"p": p, "n": netuid, "from": from_ts, "to": to_ts, "live_from": live_from_ts,
                                "now": now, "run_id": run_id})
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def rebuild(con: sqlite3.Connection, *, periods: Iterable[str] = ("1h", "1d"), from_ts: int, to_ts: int,
            netuids: Iterable[int] | None = None, live_from_ts: int, now: int, run_id: str) -> dict:
    """BAR_SQL per netuid and month chunk → ``upsert_bars`` → ``enrich_reserves`` (17 §2.5)."""
    if netuids is None:
        netuids = [r[0] for r in con.execute(
            "SELECT DISTINCT netuid FROM pool_history WHERE timestamp >= ? AND timestamp < ?", (from_ts, to_ts))]
    rep = {"written": 0, "recomputed": 0, "same_rank_skipped": 0, "lower_rank_skipped": 0, "enriched": 0,
           "netuids": 0}
    periods = list(periods)
    for n in sorted(netuids):
        rep["netuids"] += 1
        a = (from_ts // 86400) * 86400
        while a < to_ts:
            b = min(a + CHUNK_SEC, to_ts)
            for per in periods:
                rows = bars_for(con, n, per, a, b, live_from_ts=live_from_ts, now=now, run_id=run_id)
                u = upsert_bars(con, rows, per)
                for k in ("written", "recomputed", "same_rank_skipped", "lower_rank_skipped"):
                    rep[k] += u[k]
                rep["enriched"] += enrich_reserves(con, n, per, a, b, now=now)
            a = b
            if common.stop_requested():
                rep["stopped"] = True
                return rep
    return rep
