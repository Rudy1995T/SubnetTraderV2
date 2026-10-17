-- @allow-destructive widen price_bars.source CHECK with 'blockmachine' (17 Phase 3, BUILD-LOG): rebuild copies every row
-- The operator's Blockmachine archive RPC (Standard plan) is the primary backfill endpoint; its RPC-derived bars are
-- rank 2 like onfinality/opentensor_archive (precedence.SOURCE_RANK). Column order is unchanged, so SELECT * copies.
DROP VIEW IF EXISTS v_quant_price_bars;
CREATE TABLE price_bars_v2 (
  netuid            INTEGER NOT NULL,
  period            TEXT    NOT NULL CHECK (period IN ('5m','1h','1d')),
  ts                INTEGER NOT NULL,
  occupant_key      TEXT,
  open              REAL,
  high              REAL,
  low               REAL,
  close             REAL    NOT NULL CHECK (close > 0),
  volume            REAL,
  start_volume      REAL,
  tao_price_usd     REAL,
  ts_last           INTEGER,
  tao_in_pool       REAL,
  alpha_in_pool     REAL,
  moving_price      REAL,
  block_number      INTEGER,
  tmc_block_number  INTEGER,
  source            TEXT    NOT NULL CHECK (source IN ('sse_rollup','tmc_candle','ledger_import','onfinality',
                                                       'opentensor_archive','blockmachine','taostats')),
  event_at          INTEGER,
  observed_at       INTEGER NOT NULL,
  backfilled        INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),
  run_id            TEXT,
  PRIMARY KEY (netuid, period, ts)
) STRICT, WITHOUT ROWID;
INSERT INTO price_bars_v2 SELECT * FROM price_bars;
DROP TABLE price_bars;
ALTER TABLE price_bars_v2 RENAME TO price_bars;
CREATE INDEX IF NOT EXISTS ix_price_bars_occ ON price_bars(occupant_key, period, ts) WHERE occupant_key IS NOT NULL;
CREATE VIEW IF NOT EXISTS v_quant_price_bars AS
SELECT netuid, occupant_key, period, ts AS bucket_start, ts_last, open, high, low, close, volume,
       tao_in_pool, alpha_in_pool, moving_price, block_number, source, observed_at, backfilled
FROM price_bars;
