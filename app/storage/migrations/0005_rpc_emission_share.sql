-- Dead-occupant emission history (spec 20 N2 item 3, 13 §11 item 16; added 2026-10-22).
-- rpc_subnet_daily.emission_share: 100 · DynamicInfo.tao_in_emission / Σ over netuids ≠ 0 at the day's first-hour block
-- (17 §2.3 `--emission-daily`, as_of = day_ts 00:00Z, source = the archive endpoint). Same percent scale as TMC's
-- emission_percent (median |Δ| 0.0003-0.016 pp since 2025-12; BUILD-LOG 2026-10-22).
ALTER TABLE rpc_subnet_daily ADD COLUMN emission_share REAL;

-- v_quant_chain_daily (13 §3.1): chain_daily rows, plus one RPC row per occupant-day that chain_daily lacks (TMC
-- analytics cover only each netuid's current occupant). Before 2025-11-17 TMC delivered emission_percent truncated to
-- whole percent (100 % integers; 0.92 % → 0), so there the RPC share replaces it when one exists and `source` names
-- the RPC endpoint. tao_in_pool stays chain_daily's (NULL on RPC-only rows; 13's depth falls back to the bars first).
DROP VIEW IF EXISTS v_quant_chain_daily;
CREATE VIEW v_quant_chain_daily AS
SELECT c.occupant_key, c.netuid, c.day_ts AS day, c.event_at AS ts, c.block_number,
       CASE WHEN c.day_ts < 1763337600 AND r.emission_share IS NOT NULL THEN r.emission_share
            ELSE c.emission_percent END AS emission_share,
       c.subnet_tao / 1e9 AS tao_in_pool, c.observed_at, c.backfilled,
       CASE WHEN c.day_ts < 1763337600 AND r.emission_share IS NOT NULL THEN r.source ELSE c.source END AS source
FROM chain_daily c
LEFT JOIN rpc_subnet_daily r ON r.netuid = c.netuid AND r.as_of = c.day_ts AND r.occupant_key = c.occupant_key
UNION ALL
SELECT r.occupant_key, r.netuid, r.as_of AS day, r.event_at AS ts, r.block_number, r.emission_share,
       NULL AS tao_in_pool, r.observed_at, 1 AS backfilled, r.source
FROM rpc_subnet_daily r
WHERE r.emission_share IS NOT NULL AND r.as_of % 86400 = 0
  AND NOT EXISTS (SELECT 1 FROM chain_daily c
                  WHERE c.netuid = r.netuid AND c.day_ts = r.as_of AND c.occupant_key = r.occupant_key);
