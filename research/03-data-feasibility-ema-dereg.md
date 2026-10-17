# [data] data-feasibility-ema-dereg

**Question answered:** what a "subnet investment analyst" tool could compute **today** from the history TaoSentinel has already saved (EMA72 band distance, distance to deregistration, hazard inputs, survivorship for a backtest), and how good that data is.
**Written:** 2026-10-09 by the feasibility sub-task of the spec/roadmap workflow. **Data end:** last RPC cross-section block **9,139,183** at **2026-09-24T17:59:00Z** (`backfill_buckets`, era `gap`); TMC 5m candles to 2026-09-24T21:40Z; `chain_daily` to 2026-09-24. Nothing after 2026-09-24 (service not running — `research/03-current-state-audit-2026-10-09.md:21`).
**Method:** `/home/sqsq/Documents/TaoData/.venv/bin/python` + stdlib `sqlite3` opened `mode=ro` (the venv has no numpy/pandas); every query bounded by `netuid` (PK prefix of `price_bars(netuid, period, ts)` / `pool_history(netuid, block_number)` / index `idx_pool_history_subnet_ts(netuid, timestamp DESC)`) or by `occupant_key` (`ix_price_bars_occ`); whole-table reads only on the small `subnets` (129), `occupants` (180), `backfill_buckets` (14,109) and `chain_daily` last-day slice. Script runtime 31.3 s. Nothing under the repo was modified except this file; outputs are CSVs in `/tmp/claude-1000/-home-sqsq-Documents-TaoData/ceb3f025-b30f-43cf-9842-ef30351da40f/scratchpad/feasibility/` (§8) and the exact code is in §9.
**Index:** not added to `research/INDEX.md` (this task may write only this file); the orchestrator should add the entry.
**Verification key:** every number below comes from `headline.json` / the CSVs written by `feasibility.py` (§9) unless marked *probe* (one-off read-only queries, also listed in §9). UNVERIFIED marks what was not confirmed.

## 0. Headline

- **Duplicates across sources cannot exist** in `price_bars`: the primary key is `(netuid, period, ts)` (DB DDL via `sqlite_master`; `specs/03-storage-schema.md:1118`) — probe `GROUP BY netuid, period, ts HAVING count(*) > 1` → **0** groups. "Prefer `tmc_candle`, else `blockmachine`" is already enforced at write time by source precedence (`tmc_candle` rank 3 > `blockmachine` = `onfinality` rank 2, `app/storage/precedence.py:14-16`; upsert rule `app/backfill/bars.py:58-73`). All three sources share the same grid (bar `ts` = bucket start: `bars.py:21`; TMC candle label = start, `specs/BUILD-LOG.md:239-241`; probe: `ts % 86400 = 0` for 100 % of 1d bars and `ts % 3600 = 0` for 100 % of 1h bars, all sources).
- **Daily history for the current occupant:** 121/129 netuids have ≥ 72 daily bars, 111 ≥ 144, 87 ≥ 365 (median 494 bars = 2025-05-19 → 2026-09-24). Counting *per netuid regardless of occupant* gives 129/129/129 — but 42 netuids carry a predecessor token's bars under the same netuid, so a netuid-keyed series splices two different alphas (never do that). 1h bars ≥ 2,000: 119/129 current occupants (median 11,830).
- **EMA72 is computable now for 120 active non-root occupants** (daily) and all 129 (72-hour). As of the 2026-09-24 bar the market was deep in drawdown: median distance to the daily EMA72 **-16.6%** (p10 -53.9%, p90 +5.8%); 13 above / 107 below; with a ±2σ band on 72-day log-return volatility (median σ 3.37%/day) **96 of 120 sit below the band**, 13 inside, 10 above — the σ-band is far too narrow for trending collapses (§2.4).
- **Distance to deregistration is computable and verified:** at block 9,139,183 the non-immune pool is 113 active subnets; bottom three by on-chain moving price = **SN82, SN113, SN47** — exactly TMC's `deregistration_risk` flags the same day (fixture `tests/fixtures/tmc_table_2026-09-24_block9138613.json`: 47/82/113) and on 2026-09-22 (`research/02-gapfill-keyless-onchain-pillar-source.md:29`). The bottom cluster is tight: rank 4 (SN72) is only 0.45% above the 3rd-lowest, rank 10 only 12.5%.
- **The moving-price prune rule explains the deaths:** of the 51 deregistered occupants, 49/50 rankable ones were **rank 1 (lowest non-immune moving price) in their last hour**, 1 was rank 3 in a three-way tie at moving price 0 (never started emission; the oldest was pruned), 1 died inside immunity (SN73 `kaf`, 9.5 days — UNVERIFIED cause). The signal is **short-horizon**: only 30/50 were bottom-3 seven days before death and 5/48 thirty days before; τenex (67:4965213) went from rank 94 to rank 1 in six days after a 12-hour price collapse (0.0106 → 0.0020 τ, pool TAO 8,411 → 3,594 τ — *probe*). EMA72 distance was negative 30 days before death for 40/50.
- **Backtest material exists:** 50/51 dead occupants have ≥ 90 daily bars (and ≥ 90 hourly-days in `pool_history`) before death; median lifespan 318 days; deaths span 2025-03-17 → 2026-09-20; every death but one is an immediate re-occupation (median 0.44 h from last seen to successor registration).
- **Hazard inputs are complete for 124/129** netuids over the last 30 days (daily `emission_percent` + `subnet_tao` from `chain_daily`, 30 daily price bars, 714 hourly `pool_history` rows); the exceptions are root (no TMC analytics) and four subnets younger than 30 days.
- **Data quality is good but has known traps** (§6): moving price starts at 0 and ramps for a median 286 h after registration (Feb 2025 ratio median 0.15); pre-launch subnets keep moving price 0; the 2026-09-24 daily bar and the 21:00Z hourly candle are partial; 74 netuids miss 23 hours in the 2025-05-20..22 chain halt (no day is missing); Feb–May 2025 is in `pool_history` but has **no bars** (109 netuids, 8,185 netuid-days); `chain_daily` holds only the current occupant's history; the `backfill_buckets.price_method` labels are stale after the 2026-09-25 price repair (rows are right); 0 zero/NULL prices anywhere.

## 1. Bars: coverage, sources, duplicates (task 1)

**Stock (probe, `GROUP BY period, source`):**

| period | source | bars | netuids | first | last |
|---|---|---|---|---|---|
| 1d | blockmachine | 50,813 | 129 | 2025-05-19 | 2026-09-24 |
| 1d | onfinality | 111 | 111 | 2025-05-21 | 2025-05-21 (chain-halt failover) |
| 1d | tmc_candle | 12,416 | 128 | 2026-06-20 | 2026-09-24 |
| 1h | blockmachine | 1,218,708 | 129 | 2025-05-19T00:00Z | 2026-09-24T17:00Z |
| 1h | onfinality | 222 | 111 | 2025-05-21T21:00Z | 2025-05-21T23:00Z |
| 1h | tmc_candle | 298,189 | 128 | 2026-06-19T20:00Z | 2026-09-24T21:00Z (21:00 bar partial, 77 netuids) |
| 5m | tmc_candle | 3,567,624 | 128 | 2026-06-19T20:20Z | 2026-09-24T21:40Z |

- **Duplicates:** impossible by PK (0 groups, probe). Within a netuid's current-occupant series the stored source changes over time instead: 1d switch counts {'0': 12, '1': 43, '2': 1, '3': 73} (73 netuids follow `blockmachine → onfinality (2025-05-21) → blockmachine → tmc_candle (from 2026-06-20)`, 43 younger ones `blockmachine → tmc_candle`, 12 are `tmc_candle` only); 1h switch counts {'0': 12, '1': 43, '4': 1, '5': 73}. Root (netuid 0) has no TMC candles, so it is the only netuid with gap-era `blockmachine` 1h bars (2,324): for the other 128 the gap era (2026-06-19T22:00Z →) is **pure `tmc_candle`** at 1h and 1d. Cross-source consistency: TMC 1d close vs the chain's last hourly price of the same day, 11,840 netuid-days — median |rel diff| **1.3e-06**, 148 days > 1 %, 5 > 5 % (worst: SN102 15.9%, SN10 13.7%, SN103 11.7%, SN41 6.6%, SN2 6.0%) — the two sources are interchangeable for closes.
- **Bar semantics** (`app/backfill/bars.py:1-6,19-52`): RPC bars close on the bucket's last hourly `pool_history` row (so `high`/`low` of blockmachine bars are hourly-sampled, not true extremes); `moving_price` = latest non-NULL inside the bucket; `volume` = Δ cumulative `subnet_volume` (NULL across occupant changes / gaps / counter resets → 248 NULL 1d volumes, 0 NULL opens, 0 NULL moving prices, min close 8.14e-07 τ).

**Thresholds (current occupant only — the series the tool must use):**

| threshold | netuids (of 129) |
|---|---|
| ≥ 72 daily bars | 121 |
| ≥ 144 daily bars | 111 |
| ≥ 365 daily bars | 87 |
| ≥ 2,000 hourly bars | 119 |
| ≥ 72 / 144 / 365 **hourly-days in `pool_history`** (includes un-barred Feb–May 2025) | 121 / 111 / 87 |

Per netuid *including predecessors*: 129 / 129 / 129 and 129 ≥ 2,000 h — meaningless for pricing (42 netuids splice ≥ 2 occupants; all 51 historical occupants' bars are stamped with their own `occupant_key`, 0 NULL stamps — probe).

**Netuids with < 144 daily bars (young or re-registered):**

| netuid | name | status | 1d bars | first bar | 1h bars | hourly-days (pool_history) |
|---|---|---|---|---|---|---|
| 16 | kenju | active | 94 | 2026-06-23 | 2271 | 95 |
| 26 | Perturb | active | 142 | 2026-05-06 | 3395 | 142 |
| 35 |  | active | 13 | 2026-09-12 | 312 | 14 |
| 36 | Epago | active | 35 | 2026-08-21 | 854 | 36 |
| 40 | Ralph | active | 102 | 2026-06-15 | 2441 | 102 |
| 58 |  | active | 87 | 2026-06-30 | 2100 | 88 |
| 59 |  | pre_launch | 28 | 2026-08-28 | 673 | 29 |
| 69 | Herald | active | 140 | 2026-05-08 | 3348 | 140 |
| 70 |  | active | 43 | 2026-08-13 | 1051 | 44 |
| 76 | Ormas | active | 19 | 2026-09-06 | 456 | 20 |
| 86 |  | pre_launch | 62 | 2026-07-25 | 1492 | 63 |
| 90 | KubeTEE | active | 72 | 2026-07-15 | 1740 | 73 |
| 92 | Available | active | 110 | 2026-06-07 | 2635 | 110 |
| 99 | Thirty Spokes | active | 79 | 2026-07-08 | 1896 | 80 |
| 103 | Capcomp | active | 52 | 2026-08-04 | 1261 | 53 |
| 108 |  | pre_launch | 4 | 2026-09-21 | 97 | 5 |
| 116 | Memo | active | 118 | 2026-05-30 | 2826 | 118 |
| 122 | CookingTAO | active | 126 | 2026-05-22 | 3015 | 126 |

Un-barred prefix: pool_history starts 2025-02-13T21:59Z but bars start 2025-05-19 (`research/03-current-state-audit-2026-10-09.md:108`) — 109 netuids have 8,185 netuid-days of hourly data with no bar; a daily series built from `pool_history` (last row per UTC day) covers them, as done for the survivorship study (§5).

## 2. EMA72 band distance (task 2)

**Definition used (proposed for the spec):**
- Series: the current occupant's bars only (`occupant_key = subnets.occupant_key`), ordered by `ts`; daily = `period='1d'` closes; hourly = `period='1h'` closes (72 bars = 72 h).
- `EMA72_t = α·close_t + (1−α)·EMA72_{t−1}`, `α = 2/73`, seeded with the SMA of the first 72 closes; **valid only when ≥ 72 bars exist** (else flagged, not ranked). Calendar gaps (the 23-hour chain halt) are treated as consecutive observations.
- Distance `d = close/EMA72 − 1`.
- Band: `σ72` = sample std of the last 72 one-bar log returns; `upper/lower = EMA72·exp(±k·σ72)`, `k = 2`; **position in band** `pos = ln(close/EMA72)/(k·σ72)` so |pos| ≤ 1 ⇔ inside the band, pos = −3 ⇔ three half-widths below. σ needs 73 closes.
- As-of: daily = the **2026-09-24 bar (partial day: RPC rows end 17:59Z, TMC 5m end 21:40Z)**, with the same metric at the previous complete bar (09-23) alongside; hourly = the last complete hour **2026-09-24T20:00Z** (the 21:00Z candle exists for 77 netuids but is partial → dropped; root ends 17:00Z).

**Validity:** daily EMA valid for 121/129 (σ for 120); invalid: SN35 (13 bars, active), SN36 (35 bars, active), SN59 (28 bars, pre_launch), SN70 (43 bars, active), SN76 (19 bars, active), SN86 (62 bars, pre_launch), SN103 (52 bars, active), SN108 (4 bars, pre_launch). Hourly valid for 129/129.

**Distribution over the 120 valid active non-root occupants (daily):** p10 -53.9%, p25 -24.4%, median -16.6%, p75 -8.8%, p90 +5.8%; above EMA 13, below 107; band: above 10 / inside 13 / below 96. σ72 (daily log returns): p10 0.97%, median 3.37%, p90 7.95% per day. Hourly distance: p10 -3.67%, median -0.68%, p90 +1.74%.

### 2.1 Ten most above the daily EMA72 (2026-09-24 bar)

| netuid | name | 1d bars | close τ | EMA72 | dist | σ72 | band pos | dist @09-23 | 30d ret | 90d ret |
|---|---|---|---|---|---|---|---|---|---|---|
| 80 | OpenRoboto | 277 | 0.02795 | 0.01546 | +80.7% | 12.26% | +2.41 | +75.0% | +77.9% | +676.2% |
| 112 | for sale | 488 | 0.00673 | 0.003966 | +69.7% | 7.14% | +3.70 | +80.7% | +99.8% | +22.7% |
| 38 | ChronoLLM | 259 | 0.0173 | 0.01216 | +42.3% | 6.62% | +2.66 | +34.7% | +74.9% | +15.0% |
| 53 | engy | 494 | 0.03442 | 0.02591 | +32.8% | 6.31% | +2.25 | +29.0% | +22.5% | +438.7% |
| 41 | Almanac | 494 | 0.006555 | 0.005365 | +22.2% | 3.02% | +3.32 | +14.9% | +33.1% | -7.0% |
| 54 | Yanez | 494 | 0.006075 | 0.005013 | +21.2% | 5.20% | +1.85 | +21.7% | +25.8% | +18.5% |
| 93 | Bitcast | 494 | 0.01878 | 0.01645 | +14.2% | 4.78% | +1.39 | +9.9% | +14.8% | +64.6% |
| 28 | SayGM | 494 | 0.02102 | 0.01841 | +14.2% | 4.86% | +1.37 | +10.6% | +15.9% | +41.5% |
| 23 | Trishool | 494 | 0.005232 | 0.004614 | +13.4% | 4.30% | +1.46 | +6.9% | +31.4% | -17.9% |
| 51 | lium.io | 494 | 0.08845 | 0.0781 | +13.3% | 2.10% | +2.96 | +15.3% | +8.9% | +63.2% |

### 2.2 Ten most below the daily EMA72

| netuid | name | 1d bars | close τ | EMA72 | dist | σ72 | band pos | dist @09-23 | 30d ret | 90d ret |
|---|---|---|---|---|---|---|---|---|---|---|
| 96 | Verathos | 202 | 0.004759 | 0.05703 | -91.7% | 4.71% | -26.37 | -91.7% | -38.9% | -75.0% |
| 67 | Harnyx | 265 | 0.003862 | 0.02433 | -84.1% | 2.64% | -34.89 | -84.3% | -30.8% | -64.9% |
| 116 | Memo | 118 | 0.002061 | 0.009272 | -77.8% | 19.34% | -3.89 | -78.1% | -72.6% | -84.7% |
| 102 | ConnitoAI | 181 | 0.006437 | 0.02283 | -71.8% | 8.99% | -7.04 | -76.7% | -34.3% | -63.4% |
| 99 | Thirty Spokes | 79 | 0.001442 | 0.005032 | -71.3% | 7.95% | -7.86 | -71.5% | -60.5% |  |
| 16 | kenju | 94 | 0.001123 | 0.003819 | -70.6% | 8.94% | -6.85 | -71.8% | -41.5% | -92.6% |
| 92 | Available | 110 | 0.00156 | 0.005186 | -69.9% | 9.93% | -6.05 | -70.0% | -68.1% | -88.0% |
| 40 | Ralph | 102 | 0.002901 | 0.008813 | -67.1% | 3.95% | -14.07 | -67.2% | -47.2% | -90.3% |
| 126 | Attelier | 225 | 0.004459 | 0.01349 | -67.0% | 4.00% | -13.83 | -67.4% | +4.5% | -57.7% |
| 91 | cascade | 210 | 0.005613 | 0.01519 | -63.1% | 6.07% | -8.20 | -63.4% | -31.9% | -65.5% |

### 2.3 72-hour EMA on 1h bars (as of 2026-09-24T20:00Z) — ten most above / below

| netuid | name | 1h bars | close τ | EMA72h | dist | σ72h | band pos |
|---|---|---|---|---|---|---|---|
| 38 | ChronoLLM | 6199 | 0.0173 | 0.01422 | +21.6% | 4.32% | +2.27 |
| 102 | ConnitoAI | 4340 | 0.006629 | 0.00559 | +18.6% | 1.93% | +4.41 |
| 78 | Umi | 3922 | 0.004434 | 0.003965 | +11.8% | 3.18% | +1.76 |
| 23 | Trishool | 11,830 | 0.005232 | 0.00471 | +11.1% | 1.94% | +2.70 |
| 41 | Almanac | 11,830 | 0.006722 | 0.006243 | +7.7% | 1.89% | +1.95 |
| 84 |  | 3524 | 0.002641 | 0.00248 | +6.5% | 1.11% | +2.84 |
| 80 | OpenRoboto | 6640 | 0.02782 | 0.0263 | +5.8% | 2.24% | +1.25 |
| 53 | engy | 11,830 | 0.0344 | 0.03291 | +4.5% | 1.71% | +1.30 |
| 54 | Yanez | 11,830 | 0.006075 | 0.00586 | +3.7% | 1.26% | +1.43 |
| 112 | for sale | 11,704 | 0.00673 | 0.006518 | +3.2% | 3.07% | +0.52 |

| netuid | name | 1h bars | close τ | EMA72h | dist | σ72h | band pos |
|---|---|---|---|---|---|---|---|
| 36 | Epago | 854 | 0.006866 | 0.009796 | -29.9% | 1.57% | -11.35 |
| 70 |  | 1050 | 0.001815 | 0.002085 | -13.0% | 2.24% | -3.10 |
| 103 | Capcomp | 1260 | 0.001377 | 0.001523 | -9.6% | 1.42% | -3.54 |
| 35 |  | 312 | 0.002769 | 0.003056 | -9.4% | 1.29% | -3.82 |
| 16 | kenju | 2271 | 0.001123 | 0.001211 | -7.3% | 2.25% | -1.68 |
| 98 | NeverPlayAlone | 11,830 | 0.002138 | 0.002289 | -6.6% | 1.36% | -2.51 |
| 20 | Witness | 11,830 | 0.00229 | 0.002422 | -5.5% | 0.60% | -4.66 |
| 71 | Leadpoet | 11,830 | 0.003837 | 0.004056 | -5.4% | 0.97% | -2.86 |
| 99 | Thirty Spokes | 1895 | 0.001442 | 0.001519 | -5.1% | 0.56% | -4.66 |
| 42 | Unknown | 11,830 | 0.002159 | 0.002269 | -4.9% | 0.57% | -4.40 |

### 2.4 Observations for the spec
- Distances are dominated by regime: 107/120 below the daily EMA, and the "most below" list is the dereg-watch list (SN96, 67, 116, 102, 99, 16, 92, 40, 126, 91 — five of them immune re-registrations with < 120 bars). The ±2σ band (median half-width ≈ 6.7%) is narrow against −60..−90 % distances, so **band position saturates**; use the distance itself (or its percentile over the occupant's own history) for ranking and keep the band for "stretched vs. mean-reverting" labelling. An ATR-based band would need true highs/lows — blockmachine bars are hourly-sampled, only `tmc_candle` bars (since 2026-06-20) carry real intrabar extremes.
- The 2026-09-24 bar is partial: distances moved by up to ~11 pp vs the 09-23 bar (SN112 +69.7% vs +80.7%). A live tool must mark the running bar.
- Hourly EMA72 (3-day) ranks a different set (short-term momentum: SN38, 102, 78 above; SN36, 70, 103 below) — both horizons are available at the same cost.

## 3. Distance to deregistration (task 3)

**Method:** cross-section = `pool_history` rows at the last block (9,139,183; one PK lookup per netuid, 0 stale rows). `immune ⇔ block − network_registered_at < 864,000` (`NetworkImmunityPeriod` = 864,000 blocks = 120 d: `research/02-gapfill-keyless-onchain-pillar-source.md:28`, `app/config.py:209`, `app/features/chain_features.py:128`). Pool P = active, non-root, non-immune, moving price not NULL (113 subnets); rank from the bottom by `moving_price` (ties → older registration, per `specs/05-chain-collector.md:203`); `gap→lowest = (mp − mp₁)/mp`, `gap→3rd = (mp − mp₃)/mp` (negative inside the bottom three); for immune ones `blocks left = reg + 864,000 − block`, days = ×12 s. The 30-day history re-ranks every hourly cross-section of the last 30 days for the current occupant only (721 hours).

### 3.1 Bottom 10 non-immune at block 9,139,183 (2026-09-24T17:59:00Z)

| rank↑ | netuid | name | moving price τ | price τ | gap→lowest | gap→3rd | age d | pool τ | emission % | h in bottom-3 /30d | min rank 30d | max rank 30d |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 82 | Compelle | 0.0019506 | 0.0019391 | 0.00% | -5.07% | 155 | 1,230 | 0.0033 | 299 | 1 | 37 |
| 2 | 113 | LongShort | 0.0019709 | 0.001974 | 1.02% | -3.99% | 280 | 1,616 | 0 | 345 | 1 | 10 |
| 3 | 47 | GPUForge | 0.0020495 | 0.001995 | 4.83% | +0.00% | 250 | 1,065 | 0.00401 | 191 | 1 | 35 |
| 4 | 72 | StreetVision by NATIX | 0.0020588 | 0.0020259 | 5.25% | +0.45% | 566 | 6,036 | 0 | 465 | 1 | 5 |
| 5 | 65 | True Performance Network | 0.0021538 | 0.0021389 | 9.43% | +4.84% | 582 | 5,605 | 1.9e-05 | 0 | 5 | 22 |
| 6 | 98 | NeverPlayAlone | 0.0021906 | 0.0021382 | 10.96% | +6.44% | 513 | 4,548 | 0.00577 | 0 | 6 | 19 |
| 7 | 42 | Unknown | 0.0022189 | 0.0021595 | 12.09% | +7.63% | 767 | 6,871 | 0 | 170 | 2 | 9 |
| 8 | 27 | Orion | 0.0022857 | 0.0023191 | 14.66% | +10.33% | 1029 | 6,210 | 0 | 230 | 1 | 8 |
| 9 | 106 | Nodexo | 0.0023288 | 0.0023289 | 16.24% | +11.99% | 497 | 5,879 | 0.00675 | 0 | 9 | 29 |
| 10 | 6 | Numinous | 0.0023426 | 0.0023354 | 16.73% | +12.51% | 822 | 7,509 | 0.0069 | 0 | 10 | 20 |

Lowest moving price 0.001951 τ (SN82), 3rd-lowest 0.002050 τ (SN47); safest three: SN51 lium.io (rank 113, mp 0.0893), SN64 Chutes (112, 0.0678), SN4 Targon (111, 0.0541). TMC's `deregistration_risk` = bottom-3 non-immune by moving price (`specs/05-chain-collector.md:267`) → our bottom three match TMC's flags of 2026-09-24 (fixture block 9,138,613: SN47 0.0020586, SN82 0.0019517, SN113 0.0019704) and of 2026-09-22 (`research/02-gapfill-keyless-onchain-pillar-source.md:29`). Note how little separates ranks 1–12 (moving prices 0.00195–0.00237 τ, 18 %), and how volatile the ranks are over 30 days: SN82 299 h in bottom-3 (rank 1–37, now 1), SN113 345 h in bottom-3 (rank 1–10, now 2), SN47 191 h in bottom-3 (rank 1–35, now 3), SN72 465 h in bottom-3 (rank 1–5, now 4), SN42 170 h in bottom-3 (rank 2–9, now 7), SN27 230 h in bottom-3 (rank 1–8, now 8), SN84 255 h in bottom-3 (rank 1–22, now 20), SN117 58 h in bottom-3 (rank 2–32, now 29), SN66 150 h in bottom-3 (rank 2–57, now 33).

### 3.2 Immune subnets — immunity remaining and the "cliff" on expiry

| netuid | name | status | moving price τ | price τ | age d | blocks left | days left | immunity ends (est.) | on expiry |
|---|---|---|---|---|---|---|---|---|---|
| 116 | Memo | active | 0.0020662 | 0.0020796 | 117.3 | 19,547 | 2.7 | 2026-09-27T11:08:24Z |  |
| 92 | Available | active | 0.0015498 | 0.0015325 | 109.3 | 76,823 | 10.7 | 2026-10-05T10:03:36Z | rank 1 on expiry (mp < lowest non-immune) |
| 40 | Ralph | active | 0.0029574 | 0.0029341 | 101.3 | 134,677 | 18.7 | 2026-10-13T10:54:24Z |  |
| 16 | kenju | active | 0.0011493 | 0.0011231 | 94.2 | 185,463 | 25.8 | 2026-10-20T12:11:36Z | rank 1 on expiry (mp < lowest non-immune) |
| 58 |  | active | 0.0061202 | 0.0060239 | 87.2 | 235,834 | 32.8 | 2026-10-27T12:05:48Z |  |
| 99 | Thirty Spokes | active | 0.0014715 | 0.0014426 | 78.8 | 296,873 | 41.2 | 2026-11-04T23:33:36Z | rank 1 on expiry (mp < lowest non-immune) |
| 90 | KubeTEE | active | 0.022453 | 0.021909 | 72.3 | 343,487 | 47.7 | 2026-11-11T10:56:24Z |  |
| 86 |  | pre_launch | 0 | 0.00599 | 61.9 | 418,101 | 58.1 | 2026-11-21T19:39:12Z | never started (mp = 0) → rank 1 on expiry unless emission starts |
| 103 | Capcomp | active | 0.0014408 | 0.0013784 | 52.3 | 487,197 | 67.7 | 2026-12-01T09:58:24Z | rank 1 on expiry (mp < lowest non-immune) |
| 70 |  | active | 0.001992 | 0.0018164 | 43.6 | 550,388 | 76.4 | 2026-12-10T04:36:36Z | bottom-3 on expiry |
| 36 | Epago | active | 0.0098608 | 0.0070681 | 35.4 | 609,176 | 84.6 | 2026-12-18T08:34:12Z |  |
| 59 |  | pre_launch | 0 | 0.0051227 | 27.8 | 663,588 | 92.2 | 2026-12-25T21:56:36Z | never started (mp = 0) → rank 1 on expiry unless emission starts |
| 76 | Ormas | active | 0.0036794 | 0.0064434 | 18.8 | 728,661 | 101.2 | 2027-01-03T22:51:12Z |  |
| 35 |  | active | 0.0030744 | 0.0027693 | 12.8 | 771,508 | 107.2 | 2027-01-09T21:40:36Z |  |
| 108 |  | pre_launch | 0 | 0.0044421 | 3.9 | 836,070 | 116.1 | 2027-01-18T20:53:00Z | never started (mp = 0) → rank 1 on expiry unless emission starts |

Four immune subnets (SN16, 92, 99, 103) already have a moving price **below the lowest non-immune one** — each becomes the prune candidate the hour its immunity ends unless its price recovers; SN116's immunity ended 2026-09-27 (3 days after the data end) at a moving price that would rank 5th (between SN72 and SN65); SN70 would enter the bottom three. The three pre-launch subnets keep moving price 0 (no emission started — `immune` quirk in `specs/05-chain-collector.md:267`); the same state is what killed 100:5460392 Signal (§5).

## 4. Hazard inputs: 30-day histories (task 4)

Window 2026-08-26 → 2026-09-24 (30 days). `chain_daily` (TMC `analytics_1y`, 2025-09-24 → 2026-09-24, 128 netuids — `research/03-current-state-audit-2026-10-09.md:111`) gives daily `emission_percent` (share %, e.g. SN64 9.505 on 09-24), `subnet_tao` (pool depth in rao, ÷1e9 = τ, per the column comment in the DDL), `volume_24h`, owner locks; `price_bars` 1d gives the daily price; `pool_history` gives 714 hourly rows (= 29 full days + 18 h). **Complete for 124/129**; incomplete:

| netuid | name | status | chain_daily rows /30 | emission_percent | subnet_tao | 1d price bars /30 | hourly rows /714 | chain_daily total | chain_daily first |
|---|---|---|---|---|---|---|---|---|---|
| 0 |  | active | 0 | 0 | 0 | 30 | 714 | 0 |  |
| 35 |  | active | 14 | 14 | 14 | 13 | 714 | 14 | 2026-09-11 |
| 59 |  | pre_launch | 29 | 29 | 29 | 28 | 714 | 29 | 2026-08-27 |
| 76 | Ormas | active | 20 | 20 | 20 | 19 | 714 | 20 | 2026-09-05 |
| 108 |  | pre_launch | 5 | 5 | 5 | 4 | 714 | 5 | 2026-09-20 |

Depth of `chain_daily` per netuid ranges 5 → 366 rows because TMC analytics return **only the current occupant's history** (`specs/BUILD-LOG.md:202-203`); predecessor emission shares are unrecoverable from TMC (a dead occupant's hazard study must use `pool_history` price/depth/moving price, which exist hourly for all of them). The old bot's hazard (`app/quant/dereg_risk.py:1-10`: proximity 0.45 / starved 0.25 / decline 0.30, declines over emission, price and depth windows, `:17-27`) can therefore be fed today for every active occupant.

## 5. Survivorship facts for a backtest (task 5)

51 occupants are `deregistered` (all `dereg_reason = reoccupied`, exact `registered_ts`/`deregistered_ts`, successor known; `occupants` table). Death time used = `last_seen_at` (last hourly cross-section containing the occupant); `deregistered_ts` = the successor's registration (median 0.44 h later → the chain prunes at the next registration). Coverage before death: **50/51 have ≥ 90 daily bars** and ≥ 90 hourly-days; the exception is 73:5076911 `kaf` (died after 9.5 days, inside immunity, before bars start — 229 hourly rows only). Lifespans 9.5 → 839 days (median 318).

| occupant | name | registered | first seen | last seen (death) | h→successor | life d | 1d bars | 1h bars | hourly-days | ≥90 d bars |
|---|---|---|---|---|---|---|---|---|---|---|
| 73:5076911 | kaf | 2025-03-07 | 2025-03-07 | 2025-03-17T03:59 | 48.73 | 10 | 0 | 0 | 11 | False |
| 100:5460392 | Signal | 2025-04-29 | 2025-04-29 | 2025-10-19T11:56 | 0.33 | 173 | 153 | 3661 | 174 | True |
| 49:4107524 | Polaris Cloud | 2024-10-23 | 2025-02-13 | 2025-10-31T22:58 | 0.34 | 373 | 165 | 3960 | 261 | True |
| 105:5545447 | SoundsRight | 2025-05-11 | 2025-05-11 | 2025-11-09T00:58 | 0.57 | 181 | 174 | 4154 | 183 | True |
| 86:5275445 | The Alpha Arena | 2025-04-04 | 2025-04-04 | 2025-11-19T03:58 | 0.87 | 229 | 184 | 4397 | 230 | True |
| 94:5386070 | Eastworld | 2025-04-19 | 2025-04-19 | 2025-11-25T21:58 | 0.12 | 220 | 190 | 4559 | 221 | True |
| 92:5356404 | ReinforcedAI | 2025-04-15 | 2025-04-15 | 2025-12-02T23:58 | 0.35 | 232 | 198 | 4729 | 232 | True |
| 90:5328705 | brain | 2025-04-11 | 2025-04-11 | 2025-12-09T20:59 | 0.35 | 242 | 204 | 4894 | 243 | True |
| 108:5586824 | Internet of Intelligence | 2025-05-17 | 2025-05-17 | 2025-12-15T16:58 | 0.84 | 212 | 210 | 5034 | 213 | True |
| 113:5651055 | taonado | 2025-05-27 | 2025-05-27 | 2025-12-17T16:59 | 0.87 | 204 | 204 | 4896 | 205 | True |
| 80:5188340 | redpull | 2025-03-23 | 2025-03-23 | 2025-12-22T04:58 | 0.03 | 274 | 217 | 5190 | 275 | True |
| 31:2719593 | CANDLES | 2024-04-07 | 2025-02-13 | 2025-12-25T04:57 | 0.72 | 626 | 220 | 5262 | 316 | True |
| 87:5292605 | AceGuard | 2025-04-06 | 2025-04-06 | 2025-12-30T01:59 | 0.84 | 267 | 225 | 5379 | 269 | True |
| 67:4965213 | τenex | 2025-02-20 | 2025-02-20 | 2026-01-02T23:59 | 0.90 | 317 | 229 | 5473 | 317 | True |
| 109:5598862 | dogelayer(old) | 2025-05-19 | 2025-05-19 | 2026-01-05T20:57 | 0.46 | 232 | 231 | 5539 | 232 | True |
| 38:3756684 | Distributed Training | 2024-09-04 | 2025-02-13 | 2026-01-09T13:58 | 0.65 | 492 | 235 | 5631 | 331 | True |
| 114:5671631 | Level 114 | 2025-05-30 | 2025-05-30 | 2026-01-13T11:59 | 0.04 | 228 | 228 | 5471 | 229 | True |
| 47:4236387 | Reboot | 2024-11-10 | 2025-02-13 | 2026-01-17T08:59 | 0.77 | 433 | 243 | 5818 | 339 | True |
| 15:3684902 | BitQuant | 2024-08-25 | 2025-02-13 | 2026-01-21T01:58 | 0.35 | 513 | 247 | 5907 | 343 | True |
| 99:5453192 | Neza | 2025-04-28 | 2025-04-28 | 2026-01-27T18:58 | 0.13 | 274 | 253 | 6068 | 275 | True |
| 107:5565706 | KDN-1 | 2025-05-14 | 2025-05-14 | 2026-02-02T17:59 | 0.22 | 264 | 259 | 6211 | 265 | True |
| 126:5841630 | Tiger Beta | 2025-06-23 | 2025-06-23 | 2026-02-12T04:58 | 0.75 | 234 | 234 | 5617 | 235 | True |
| 76:5114649 | Safe Scan | 2025-03-12 | 2025-03-12 | 2026-02-19T00:57 | 0.31 | 343 | 276 | 6602 | 345 | True |
| 91:5342193 | τensorprox | 2025-04-13 | 2025-04-13 | 2026-02-27T04:57 | 0.79 | 320 | 284 | 6798 | 321 | True |
| 96:5410917 | FLock OFF | 2025-04-23 | 2025-04-23 | 2026-03-07T10:58 | 0.35 | 318 | 292 | 6996 | 319 | True |
| 97:5432489 | FlameWire 🔥 | 2025-04-26 | 2025-04-26 | 2026-03-13T08:58 | 0.35 | 321 | 298 | 7138 | 322 | True |
| 70:5008428 | Vericore | 2025-02-26 | 2025-02-26 | 2026-03-20T14:58 | 0.20 | 387 | 305 | 7312 | 388 | True |
| 102:5499703 | Vocence | 2025-05-05 | 2025-05-05 | 2026-03-28T00:57 | 0.28 | 327 | 313 | 7490 | 328 | True |
| 36:4871163 | Web Agents - Autoppia | 2025-02-06 | 2025-02-13 | 2026-04-04T12:58 | 0.71 | 422 | 320 | 7670 | 416 | True |
| 78:5143608 | Loosh | 2025-03-16 | 2025-03-16 | 2026-04-14T10:58 | 0.23 | 394 | 330 | 7908 | 395 | True |
| 82:5216791 | Hermes | 2025-03-27 | 2025-03-27 | 2026-04-22T19:58 | 0.52 | 392 | 338 | 8109 | 392 | True |
| 57:4343091 | Sparket.AI | 2024-11-25 | 2025-02-13 | 2026-04-27T02:58 | 0.55 | 518 | 343 | 8212 | 439 | True |
| 84:5248864 | ChipForge (Tatsu) | 2025-03-31 | 2025-03-31 | 2026-05-01T00:58 | 0.08 | 396 | 347 | 8306 | 397 | True |
| 26:4893280 | beqar | 2025-02-10 | 2025-02-13 | 2026-05-06T09:58 | 0.01 | 450 | 352 | 8435 | 448 | True |
| 69:4979613 | ain | 2025-02-22 | 2025-02-22 | 2026-05-08T09:59 | 0.53 | 440 | 354 | 8483 | 441 | True |
| 122:5778578 | Bitrecs | 2025-06-14 | 2025-06-14 | 2026-05-22T06:58 | 0.58 | 342 | 342 | 8205 | 343 | True |
| 116:5699219 | TaoLend | 2025-06-03 | 2025-06-03 | 2026-05-30T03:58 | 0.44 | 361 | 361 | 8659 | 362 | True |
| 92:7013758 | TensorClaw | 2025-12-03 | 2025-12-03 | 2026-06-07T02:58 | 0.38 | 186 | 186 | 4467 | 187 | True |
| 40:3372582 | Chunking | 2024-07-12 | 2025-02-13 | 2026-06-15T03:58 | 0.39 | 702 | 392 | 9389 | 488 | True |
| 16:2765446 | BitAds | 2024-04-15 | 2025-02-13 | 2026-06-22T04:59 | 0.79 | 798 | 400 | 9559 | 495 | True |
| 58:4367003 | Pending | 2024-11-28 | 2025-02-13 | 2026-06-29T08:58 | 0.66 | 577 | 407 | 9731 | 502 | True |
| 99:7415113 | Leoma | 2026-01-27 | 2026-01-27 | 2026-07-07T20:59 | 0.66 | 161 | 162 | 3867 | 162 | True |
| 90:7063126 | ogham | 2025-12-09 | 2025-12-09 | 2026-07-14T08:59 | 0.09 | 216 | 218 | 5197 | 218 | True |
| 86:6914378 | ⚒ | 2025-11-19 | 2025-11-19 | 2026-07-24T16:59 | 0.90 | 248 | 248 | 5942 | 248 | True |
| 103:5515448 | Djinn | 2025-05-07 | 2025-05-07 | 2026-08-03T07:59 | 0.31 | 453 | 442 | 10,570 | 454 | True |
| 70:7787562 | NexisGen | 2026-03-20 | 2026-03-20 | 2026-08-12T01:59 | 1.02 | 144 | 146 | 3468 | 146 | True |
| 36:7894898 | Leoma | 2026-04-04 | 2026-04-04 | 2026-08-20T05:59 | 0.98 | 138 | 139 | 3306 | 139 | True |
| 59:4401833 | Babelbit | 2024-12-03 | 2025-02-13 | 2026-08-27T19:59 | 0.36 | 632 | 466 | 11,158 | 561 | True |
| 76:7574784 | Phylax | 2026-02-19 | 2026-02-19 | 2026-09-05T20:59 | 0.32 | 199 | 199 | 4773 | 199 | True |
| 35:3037158 | OxMarkets | 2024-05-26 | 2025-02-13 | 2026-09-11T19:59 | 0.41 | 839 | 481 | 11,518 | 576 | True |
| 108:7105263 | Prometheon | 2025-12-15 | 2025-12-15 | 2026-09-20T19:59 | 0.88 | 279 | 280 | 6700 | 280 | True |

### 5.1 Predictor preview (is a "did EMA / dereg distance predict death" study possible? — yes)

Rank from the bottom among non-immune subnets at T−30 d, T−7 d and T0 (the death hour), from hourly cross-sections rebuilt at the `backfill_buckets` block ≤ that time (129 PK lookups each); EMA72 distance from the occupant's own daily closes (last `pool_history` row per UTC day, so Feb–May 2025 counts).

- **T0:** rank 1 for 49/50; rank 3 for 100:5460392 Signal (moving price 0 for all 4,120 rows = never started emission; three non-immune subnets had mp 0 at that block — SN100, 118, 119 — and the oldest registration was pruned, consistent with the tie rule); immune at death: 73:5076911.
- **T−7 d:** bottom-3 30/50, rank 1 16/50. **T−30 d:** bottom-3 5/48, bottom-5 9/48; 2 were still immune (70:7787562, 36:7894898 — died 24 and 18 days after immunity ended).
- **EMA72 distance < 0:** 40/50 at T−30 d, 43/50 at T−7 d; most deaths were long collapses (distance −80..−99 %), but 67:4965213 τenex (+38 % at T−30 d, rank 94 at T−7 d) and 31:2719593 CANDLES (+21 %, rank 32 at T−7 d) died within days of a sudden drop — the "owner exit" pattern the research corpus describes (`research/02-gapfill-owner-extraction-mechanics.md`). The on-chain moving price now tracks price within ~2 days (median mp/price 1.00, p10 0.97 since 2025-10 — §6), so rank is a *lagging* indicator of a dump, not a leading one.

| occupant | death | rank T−30d | rank T−7d | rank T0 | pool T0 | EMA72 dist T−30d | EMA72 dist T−7d | heuristic |
|---|---|---|---|---|---|---|---|---|
| 73:5076911 | 2025-03-17T03:59 |  |  |  | 48 |  |  | immune at death |
| 100:5460392 | 2025-10-19T11:56 | 3 | 3 | 3 | 124 | +3.7% | -5.3% | not lowest at death rank3 |
| 49:4107524 | 2025-10-31T22:58 | 8 | 1 | 1 | 127 | -19.2% | -7.0% | prune consistent rank1 |
| 105:5545447 | 2025-11-09T00:58 | 5 | 1 | 1 | 126 | -99.3% | -98.6% | prune consistent rank1 |
| 86:5275445 | 2025-11-19T03:58 | 4 | 2 | 1 | 125 | -100.0% | -100.0% | prune consistent rank1 |
| 94:5386070 | 2025-11-25T21:58 | 5 | 4 | 1 | 124 | -99.5% | -98.9% | prune consistent rank1 |
| 92:5356404 | 2025-12-02T23:58 | 21 | 2 | 1 | 123 | -93.3% | -88.4% | prune consistent rank1 |
| 90:5328705 | 2025-12-09T20:59 | 7 | 5 | 1 | 122 | -100.0% | -100.0% | prune consistent rank1 |
| 108:5586824 | 2025-12-15T16:58 | 6 | 4 | 1 | 121 | -97.9% | -94.9% | prune consistent rank1 |
| 113:5651055 | 2025-12-17T16:59 | 18 | 8 | 1 | 120 | -96.3% | -92.3% | prune consistent rank1 |
| 80:5188340 | 2025-12-22T04:58 | 27 | 7 | 1 | 119 | +6.7% | +15.7% | prune consistent rank1 |
| 31:2719593 | 2025-12-25T04:57 | 53 | 32 | 1 | 118 | +21.2% | +25.7% | prune consistent rank1 |
| 87:5292605 | 2025-12-30T01:59 | 53 | 2 | 1 | 117 | -86.6% | -83.3% | prune consistent rank1 |
| 67:4965213 | 2026-01-02T23:59 | 59 | 94 | 1 | 116 | +38.5% | +102.4% | prune consistent rank1 |
| 109:5598862 | 2026-01-05T20:57 | 15 | 3 | 1 | 115 | -97.3% | -94.9% | prune consistent rank1 |
| 38:3756684 | 2026-01-09T13:58 | 8 | 3 | 1 | 114 | -4.8% | +4.2% | prune consistent rank1 |
| 114:5671631 | 2026-01-13T11:59 | 4 | 5 | 1 | 113 | -90.3% | -81.1% | prune consistent rank1 |
| 47:4236387 | 2026-01-17T08:59 | 13 | 5 | 1 | 112 | +21.6% | +13.0% | prune consistent rank1 |
| 15:3684902 | 2026-01-21T01:58 | 11 | 1 | 1 | 111 | +4.3% | +13.5% | prune consistent rank1 |
| 99:5453192 | 2026-01-27T18:58 | 11 | 3 | 1 | 110 | -98.4% | -96.4% | prune consistent rank1 |
| 107:5565706 | 2026-02-02T17:59 | 35 | 3 | 1 | 109 | -92.8% | -87.6% | prune consistent rank1 |
| 126:5841630 | 2026-02-12T04:58 | 28 | 1 | 1 | 108 | -97.1% | -95.9% | prune consistent rank1 |
| 76:5114649 | 2026-02-19T00:57 | 37 | 12 | 1 | 108 | +9.5% | +1.5% | prune consistent rank1 |
| 91:5342193 | 2026-02-27T04:57 | 45 | 1 | 1 | 107 | -49.8% | -61.6% | prune consistent rank1 |
| 96:5410917 | 2026-03-07T10:58 | 16 | 2 | 1 | 107 | -69.4% | -54.6% | prune consistent rank1 |
| 97:5432489 | 2026-03-13T08:58 | 21 | 10 | 1 | 107 | -95.5% | -92.0% | prune consistent rank1 |
| 70:5008428 | 2026-03-20T14:58 | 27 | 1 | 1 | 107 | +14.1% | -26.3% | prune consistent rank1 |
| 102:5499703 | 2026-03-28T00:57 | 20 | 4 | 1 | 107 | -81.3% | -74.9% | prune consistent rank1 |
| 36:4871163 | 2026-04-04T12:58 | 23 | 5 | 1 | 107 | +1.3% | -16.3% | prune consistent rank1 |
| 78:5143608 | 2026-04-14T10:58 | 6 | 1 | 1 | 107 | -4.2% | -29.9% | prune consistent rank1 |
| 82:5216791 | 2026-04-22T19:58 | 12 | 1 | 1 | 109 | -8.9% | -26.6% | prune consistent rank1 |
| 57:4343091 | 2026-04-27T02:58 | 3 | 2 | 1 | 109 | -5.7% | -7.2% | prune consistent rank1 |
| 84:5248864 | 2026-05-01T00:58 | 15 | 4 | 1 | 109 | -94.9% | -91.8% | prune consistent rank1 |
| 26:4893280 | 2026-05-06T09:58 | 46 | 1 | 1 | 110 | -8.1% | -32.8% | prune consistent rank1 |
| 69:4979613 | 2026-05-08T09:59 | 15 | 1 | 1 | 109 | +8.0% | -8.9% | prune consistent rank1 |
| 122:5778578 | 2026-05-22T06:58 | 10 | 10 | 1 | 112 | -75.8% | -58.0% | prune consistent rank1 |
| 116:5699219 | 2026-05-30T03:58 | 11 | 7 | 1 | 112 | -81.6% | -70.1% | prune consistent rank1 |
| 92:7013758 | 2026-06-07T02:58 | 37 | 11 | 1 | 112 | -93.0% | -90.5% | prune consistent rank1 |
| 40:3372582 | 2026-06-15T03:58 | 1 | 1 | 1 | 112 | -0.1% | -5.8% | prune consistent rank1 |
| 16:2765446 | 2026-06-22T04:59 | 24 | 1 | 1 | 112 | -16.4% | -24.1% | prune consistent rank1 |
| 58:4367003 | 2026-06-29T08:58 | 3 | 1 | 1 | 112 | -23.4% | -17.3% | prune consistent rank1 |
| 99:7415113 | 2026-07-07T20:59 | 41 | 5 | 1 | 112 | -89.0% | -85.2% | prune consistent rank1 |
| 90:7063126 | 2026-07-14T08:59 | 25 | 2 | 1 | 112 | -77.2% | -73.6% | prune consistent rank1 |
| 86:6914378 | 2026-07-24T16:59 | 7 | 1 | 1 | 112 | -62.0% | -55.7% | prune consistent rank1 |
| 103:5515448 | 2026-08-03T07:59 | 8 | 3 | 1 | 113 | -39.1% | -33.4% | prune consistent rank1 |
| 70:7787562 | 2026-08-12T01:59 |  | 2 | 1 | 112 | -57.5% | -59.4% | prune consistent rank1 |
| 36:7894898 | 2026-08-20T05:59 |  | 1 | 1 | 112 | -37.8% | -62.3% | prune consistent rank1 |
| 59:4401833 | 2026-08-27T19:59 | 1 | 1 | 1 | 113 | -42.5% | -30.4% | prune consistent rank1 |
| 76:7574784 | 2026-09-05T20:59 | 9 | 13 | 1 | 115 | -99.5% | -99.1% | prune consistent rank1 |
| 35:3037158 | 2026-09-11T19:59 | 14 | 2 | 1 | 114 | -26.1% | -20.5% | prune consistent rank1 |
| 108:7105263 | 2026-09-20T19:59 | 32 | 6 | 1 | 114 | -38.1% | -43.1% | prune consistent rank1 |

A proper study needs the matched survivors (same dates, same pool) — all inputs exist hourly for every occupant since 2025-02-13.

## 6. Data-quality issues found (task 6)

1. **Duplicates:** none possible (PK). The precedence rule leaves *source mixtures inside a series* (§1); closes agree to ~1e-6 across sources, highs/lows do not (hourly-sampled RPC bars vs. TMC candles).
2. **Zero / NaN prices:** `pool_history.price ≤ 0` rows **0**, `moving_price NULL` **0**, 1d/1h `close` has a `CHECK (close > 0)` (DDL). `moving_price ≤ 0`: **81,294 rows** — root (14,078 = all, chain reports 0) and the first hours after every registration (SubnetMovingPrice starts at 0; `ema_alpha = base_alpha × blocks_since_start/(blocks_since_start+201,600)`, `research/02-gapfill-keyless-onchain-pillar-source.md:133`) plus **pre-launch subnets that never started emission keep 0 for life** (SN59 670 h, SN86, SN108 today; 100:5460392 for 4,120 h). Largest counts: SN0 14,078, SN100 4,589, SN101 3,651, SN83 3,564, SN95 3,547, SN118 3,273, SN119 3,220, SN115 3,145.
3. **Moving price before it is comparable ("pricing change" / ramp):** at dTAO launch (2025-02-13) every subnet's moving price started near 0 and converged over weeks — Feb 2025 median mp/price **0.15** (74 % of rows < 0.5), March 1.01; every new registration repeats the ramp (hours until |mp/price − 1| < 10 %: median 286 h, max 3,845 h; never for root and the pre-launch trio). Moving-price-based ranks are only meaningful for non-immune subnets (≥ 120 d), which is exactly the prune pool, so the tool is safe if it never ranks immune ones by moving price.

| month | rows | median mp/price | p10 | p90 | share < 0.5 | mean (outlier-prone) |
|---|---|---|---|---|---|---|
| 2025-02 | 24,721 | 0.1537 | 0.03556 | 1.019 | 73.8% | 0.3478 |
| 2025-03 | 57,796 | 1.013 | 0.9432 | 1.061 | 2.8% | 0.9939 |
| 2025-04 | 66,748 | 1.002 | 0.4913 | 1.075 | 10.0% | 0.8984 |
| 2025-05 | 77,880 | 1.004 | 0 | 1.063 | 14.7% | 0.8616 |
| 2025-06 | 88,564 | 1.003 | 0 | 1.037 | 17.4% | 0.8302 |
| 2025-07 | 95,976 | 1.002 | 0 | 1.031 | 13.6% | 0.8681 |
| 2025-08 | 95,976 | 1.003 | 0.8332 | 1.025 | 9.4% | 0.9091 |
| 2025-09 | 92,880 | 1.001 | 0.9191 | 1.019 | 7.7% | 0.9223 |
| 2025-10 | 95,976 | 1.001 | 0.9656 | 1.019 | 3.4% | 0.9645 |
| 2025-11 | 92,880 | 1 | 0.974 | 1.02 | 1.8% | 6.358 |
| 2025-12 | 95,976 | 0.9997 | 0.9668 | 1.019 | 3.3% | 0.9661 |
| 2026-01 | 95,976 | 0.9994 | 0.9684 | 1.02 | 3.4% | 0.9645 |
| 2026-02 | 86,688 | 0.9999 | 0.9721 | 1.018 | 2.6% | 0.985 |
| 2026-03 | 95,976 | 1 | 0.9686 | 1.024 | 2.8% | 0.9734 |
| 2026-04 | 92,880 | 1 | 0.9725 | 1.024 | 2.1% | 0.98 |
| 2026-05 | 95,976 | 1.001 | 0.9742 | 1.024 | 1.3% | 0.9878 |
| 2026-06 | 92,880 | 1.001 | 0.9719 | 1.025 | 1.2% | 0.9892 |
| 2026-07 | 95,976 | 1.001 | 0.9818 | 1.027 | 1.7% | 0.988 |
| 2026-08 | 95,975 | 1.001 | 0.9779 | 1.02 | 2.3% | 0.979 |
| 2026-09 | 73,530 | 1.002 | 0.9775 | 1.025 | 3.6% | 0.9706 |

4. **`price_method` mixes:** rows are `reserve_ratio` 2025-02-13 → 2025-07-07T20:59Z (constant product exact), `swap_sqrt` 2025-07-07T21:59Z → 2026-06-25T19:59Z (`AlphaSqrtPrice²`), `reserve_ratio` again from 2026-06-25 (the chain's own price API; 147,580 rows = 128 × 1,153 h were repaired on 2026-09-25 — `specs/BUILD-LOG.md:250-262`; our count of `run_id LIKE 'script:repair%'` rows = **147,580** over 128 netuids, exact match). **Stale labels:** `backfill_buckets.price_method` still says `swap_sqrt` until 2026-08-12T20:00Z (9,624 buckets) because the repair rewrote rows, not buckets — use the row label. Level shift at the 2025-07-07 boundary (|Δ log price| 20:59 → 21:59 vs. each netuid's own ±7-day hourly moves): median 0.064% vs. local median 0.038%, 26 netuids > 1 %, 6 > 5 %, 11/129 above their own p95 (≈ 6 expected by chance); largest: SN125 111.9%, SN124 16.8%, SN109 12.8%, SN122 10.0% (SN125 was in its first trading hour after a pre-launch placeholder price of 3.805 τ; SN124/109/122 are thin pools registered weeks earlier whose own ±7-day p95 is 6–21 %; root's price is nominal — *probe*). Verdict: no material step; a ≤ 0.1 % discontinuity for a few netuids is UNVERIFIED either way.
5. **Chain halt 2025-05-20T23:00 → 05-22T12:00Z:** 23 hourly buckets `skipped: no_block_in_hour` (`backfill_buckets`; `specs/BUILD-LOG.md:237-238`) → 74 netuids alive then miss exactly 23 hourly bars; **no daily bar is missing** (0 calendar gaps in any current-occupant daily series); 2025-05-21's daily bar is the OnFinality failover close for 111 netuids (bars per day 05-19..05-22: 110, 111, 111, 112 — *probe*). Hourly EMA/volatility over those days sees a 23-hour jump.
6. **Partial last bars:** the 2026-09-24 1d bar (both sources) and the 21:00Z 1h candle (77 netuids) are running bars; `chain_daily` 2026-09-24 is an EOD analytics point taken at 2026-09-24 (UNVERIFIED whether TMC's "day" closed).
7. **Un-barred Feb–May 2025** (109 netuids, 8,185 netuid-days) — fix is `python -m scripts.backfill_rpc --post-only` (`research/03-current-state-audit-2026-10-09.md:108`); until then build daily closes from `pool_history`.
8. **Occupant splicing:** 42 netuids hold ≥ 2 occupants' bars; `chain_daily`/`chain_hourly` hold only the current occupant (`specs/BUILD-LOG.md:202-203`); always key series by `occupant_key`.
9. **`chain_hourly` has no live rows** (all `analytics_1m`, 2026-08-25 → 2026-09-24T21:00Z, 41 netuids at the last hour) — the TMC `immune`/`deregistration_risk` badges are not stored anywhere; we recompute them from `pool_history` (and they match TMC).
10. **Volume:** 1d `volume` NULL in 248 bars by design (occupant change / > 2-period gap / counter reset, `bars.py:4-5,42-44`); `subnet_volume` is τ (BUILD-LOG final results).
11. **Identity links for the other pillars (active 126):** github 114, website 113, x 102, discord 95, telegram 7 — links only; no activity has been collected (Phases 4–9 unbuilt).

## 7. What the tool could compute today vs. what is missing

| signal the operator asked for | computable from saved history today? | evidence |
|---|---|---|
| Alpha price charts (1h/1d since 2025-05-19, 5m since 2026-06-19, hourly reserves since 2025-02-13) | **Yes**, to 2026-09-24 | §1 |
| Distance to the 72-EMA band (daily and 72-h) | **Yes** for 120 active occupants (daily) / 129 (hourly); definition proposed in §2 | §2 |
| Distance to deregistration (rank, % gap to the cut, immunity left, 30-day rank history) | **Yes**, matches TMC flags; immunity cliff list is new information | §3 |
| Dereg hazard inputs (30-d emission share, depth, price) | **Yes** for 124/129 | §4 |
| Backtest of "did EMA/dereg distance predict death" | **Yes** — 50 deaths with ≥ 90 d of history + hourly cross-sections for matched survivors; preview shows rank-at-death = 1 for 49/50, 7-day lead for 60 %, 30-day lead for 10 % | §5 |
| GitHub activity, socials activity | **No** — 0 rows collected (links for 114 / 102 / 95 subnets exist) | §6 item 11; audit :225-226 |
| Anything after 2026-09-24 | **No** — collectors stopped; `backfill_rpc --era gap` + `backfill_tmc --redo` would extend the history (audit §7) | audit :21,170-182 |

Implications for the spec: (a) rank by `occupant_key`, never by netuid; (b) the dereg metric should be the **% gap of moving price to the 3rd-lowest non-immune** plus hours-in-bottom-3 over 30 d and the immunity-cliff flag, not the rank alone (ranks 1–12 are within 18 %); (c) EMA distance should be reported as a percentile of the occupant's own history and paired with a liquidity/owner-flow signal, because the deadliest moves (τenex, CANDLES) were 12-hour collapses that no 72-day EMA anticipates; (d) a live tool must distinguish running bars.

## 8. Files produced (scratchpad, session-local)

Directory `/tmp/claude-1000/-home-sqsq-Documents-TaoData/ceb3f025-b30f-43cf-9842-ef30351da40f/scratchpad/feasibility/`:

| file | rows | content |
|---|---|---|
| `bars_coverage_per_netuid.csv` | 129 | per netuid: 1d/1h bar counts by source, current-occupant counts, first/last, missing days/hours, source switches, pool_history days, identity-link flags, threshold flags |
| `ema72_daily_latest.csv` / `ema72_hourly_latest.csv` | 129 / 129 | EMA72, distance, σ72, band, position, previous-complete-bar values, 30/90-day returns |
| `ema72_daily_top10_above.csv`, `..._below.csv`, `ema72_hourly_top10_above.csv`, `..._below.csv` | 10 each | the ranked lists of §2 |
| `dereg_distance_latest.csv` | 129 | cross-section at block 9,139,183: immunity, rank, gaps, immunity left, 30-day rank history, last-day emission share |
| `hazard_inputs_30d.csv` | 129 | 30-day completeness of emission_percent / subnet_tao / price / hourly rows per netuid |
| `survivorship_dead_occupants.csv` | 51 | the 51 dead occupants with dates, lifespan, bar and hourly-day counts |
| `survivorship_preview_predictors.csv` | 51 | rank and EMA72 distance at T−30 d / T−7 d / T0 per dead occupant, death-cause heuristic |
| `dq_pool_history_per_netuid.csv` | 129 | zero/NULL counts, price_method ranges, repaired rows, moving-price ramp hours |
| `dq_moving_price_ratio_by_month.csv` | 20 | mp/price median, p10, p90, share < 0.5 by month |
| `dq_price_method_boundary_jumps.csv` | 129 | 2025-07-07 boundary step vs. local volatility per netuid |
| `dq_1d_crosscheck_tmc_vs_chain.csv` | 128 | TMC 1d close vs. chain last-hour price per netuid |
| `headline.json` | — | all aggregate numbers quoted above |
| `feasibility.py`, `make_report.py` | — | the code (§9) and this report's renderer |

## 9. Exact SQL / Python used

### 9.1 One-off probe queries (read-only, `sqlite3.connect('file:…/taosentinel.db?mode=ro', uri=True)`)

```sql
-- schema and indexes
SELECT type, name, tbl_name, sql FROM sqlite_master WHERE type IN ('table','index') ORDER BY tbl_name;
-- stock by period/source (one scan of price_bars, 6 s)
SELECT period, source, count(*), min(ts), max(ts), count(DISTINCT netuid) FROM price_bars GROUP BY period, source;
-- grid alignment
SELECT source, ts % 86400, count(*) FROM price_bars WHERE period='1d' GROUP BY 1, 2;
SELECT source, ts % 3600,  count(*) FROM price_bars WHERE period='1h' GROUP BY 1, 2;
-- duplicates (impossible by PK; verified)
SELECT count(*) FROM (SELECT netuid, period, ts FROM price_bars WHERE period='1d' GROUP BY netuid, period, ts HAVING count(*) > 1);
-- stamping
SELECT source, sum(occupant_key IS NULL), count(*) FROM price_bars WHERE period='1d' GROUP BY source;
-- eras, last block, price-method timeline, chain-halt holes (backfill_buckets is small)
SELECT era, status, count(*), min(bucket_ts), max(bucket_ts) FROM backfill_buckets GROUP BY era, status;
SELECT era, bucket_ts, block_number, block_ts, price_method, endpoint FROM backfill_buckets WHERE status='done' ORDER BY bucket_ts DESC LIMIT 3;
SELECT bucket_ts, price_method, block_number FROM backfill_buckets WHERE status='done' ORDER BY bucket_ts;   -- runs computed in Python
SELECT bucket_ts, error FROM backfill_buckets WHERE status='skipped' ORDER BY bucket_ts;
-- bars per day around the halt and at the end
SELECT ts, count(*), group_concat(DISTINCT source) FROM price_bars WHERE period='1d' AND ts BETWEEN 1747526400 AND 1748044800 GROUP BY ts;
SELECT ts, source, count(*) FROM price_bars WHERE period='1h' AND ts >= 1790258400 GROUP BY ts, source;
-- per-netuid bounded probes (SN64 / SN1 / SN125 / SN67 / SN31 / SN100 / SN84)
SELECT price_method, count(*), min(timestamp), max(timestamp) FROM pool_history WHERE netuid=64 GROUP BY price_method;
SELECT count(*), min(timestamp), max(timestamp) FROM pool_history WHERE netuid=64 AND run_id LIKE 'script:repair%';
SELECT strftime('%Y-%m', timestamp, 'unixepoch'), count(*), avg(moving_price/price), min(moving_price/price), max(moving_price/price)
  FROM pool_history WHERE netuid=64 GROUP BY 1;
SELECT timestamp, block_number, price, moving_price, tao_in_pool, occupant_key FROM pool_history
  WHERE netuid=67 AND timestamp BETWEEN 1767376740 AND 1767405540 ORDER BY timestamp;          -- τenex last hours
SELECT count(*), sum(moving_price=0), min(price), max(price) FROM pool_history WHERE netuid=100 AND occupant_key='100:5460392';
SELECT netuid, moving_price, price FROM pool_history WHERE block_number=9139183 AND netuid IN (116,92,16,99,103,70);
-- chain_hourly has no live rows / flags
SELECT max(hour_ts) FROM chain_hourly;
SELECT count(*), sum(immune), sum(immune_local), sum(is_active) FROM chain_hourly WHERE hour_ts = (SELECT max(hour_ts) FROM chain_hourly);
-- occupants
SELECT status, source, count(*) FROM occupants GROUP BY status, source;
SELECT dereg_reason, count(*), sum(registered_ts IS NULL), sum(deregistered_ts IS NULL) FROM occupants WHERE status='deregistered' GROUP BY dereg_reason;
```

TMC cross-check: `tests/fixtures/tmc_table_2026-09-24_block9138613.json` (129 rows; `deregistration_risk` true for `subnet` 47, 82, 113 with `subnet_moving_price` 0.0020586 / 0.0019517 / 0.0019704, `immune` false).

### 9.2 `feasibility.py` (the script that produced every CSV and `headline.json`)

```python
#!/usr/bin/env python
"""Data-feasibility study: EMA72 band distance + deregistration distance from the saved TaoSentinel history.

Read-only (mode=ro). Stdlib only (the venv has no numpy/pandas). Every query is bounded by netuid (PK prefix /
(netuid, timestamp) index) or by occupant_key (ix_price_bars_occ); the only whole-table reads are the tiny
backfill_buckets / subnets / occupants tables.
"""
from __future__ import annotations

import csv
import json
import math
import os
import sqlite3
import statistics as st
import sys
import time
import datetime as dt
from collections import defaultdict

DB = "file:/home/sqsq/Documents/TaoData/data/taosentinel.db?mode=ro"
OUT = "/tmp/claude-1000/-home-sqsq-Documents-TaoData/ceb3f025-b30f-43cf-9842-ef30351da40f/scratchpad/feasibility/"
IMMUNITY_BLOCKS = 864_000          # NetworkImmunityPeriod (120 d) — research/02-gapfill-keyless-onchain-pillar-source.md:28
BLOCK_S = 12
DAY = 86_400
HOUR = 3_600
EMA_N = 72
BAND_K = 2.0
BARS_START_TS = 1_747_612_800      # 2025-05-19T00:00Z — first 1d/1h bar in price_bars (Feb–May 2025 un-barred)
T0 = time.time()
GAP_ERA_FROM_TS = int(dt.datetime(2026, 6, 19, 22, tzinfo=dt.UTC).timestamp())   # first `gap` era bucket (backfill_buckets)


def iso(t):
    return dt.datetime.fromtimestamp(int(t), dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ") if t is not None else None


def day(t):
    return dt.datetime.fromtimestamp(int(t), dt.UTC).strftime("%Y-%m-%d") if t is not None else None


def write_csv(name, rows, cols):
    path = os.path.join(OUT, name)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({c: ("" if r.get(c) is None else (f"{r[c]:.6g}" if isinstance(r[c], float) else r[c])) for c in cols})
    print(f"  wrote {name} ({len(rows)} rows) t={time.time()-T0:.1f}s", file=sys.stderr)
    return path


def ema_last(vals, n=EMA_N):
    """EMA with alpha=2/(n+1). Seed = SMA of the first n values when len>=n (valid), else the first value (invalid)."""
    if not vals:
        return None, False
    a = 2.0 / (n + 1)
    if len(vals) >= n:
        e = sum(vals[:n]) / n
        start = n
        valid = True
    else:
        e = vals[0]
        start = 1
        valid = False
    for v in vals[start:]:
        e = a * v + (1 - a) * e
    return e, valid


def band_metrics(closes, n=EMA_N, k=BAND_K):
    """Return dict: close, ema, dist_pct, sigma (sample std of the last n log returns), upper, lower, pos, n_bars.

    Band: upper/lower = EMA·exp(±k·sigma); pos = ln(close/EMA) / (k·sigma)  (|pos| ≤ 1 ⇔ inside the band).
    """
    out = {"n_bars": len(closes), "close": None, "ema": None, "dist_pct": None, "sigma": None, "upper": None,
           "lower": None, "pos": None, "ema_valid": False, "sigma_valid": False}
    if not closes:
        return out
    e, valid = ema_last(closes, n)
    c = closes[-1]
    out.update(close=c, ema=e, ema_valid=valid, dist_pct=(c / e - 1) * 100 if e else None)
    rets = [math.log(b / a) for a, b in zip(closes[:-1], closes[1:]) if a > 0 and b > 0]
    if len(rets) >= n:
        s = st.stdev(rets[-n:])
        out.update(sigma=s, sigma_valid=True)
        if s > 0 and e:
            out.update(upper=e * math.exp(k * s), lower=e * math.exp(-k * s), pos=math.log(c / e) / (k * s))
    return out


con = sqlite3.connect(DB, uri=True)
con.row_factory = sqlite3.Row
cur = con.cursor()

# ---------------------------------------------------------------- reference tables (tiny)
subnets = {r["netuid"]: dict(r) for r in cur.execute(
    "SELECT netuid, occupant_key, network_registered_at, name, status, is_active, immune, head_block, "
    "github_url, discord_url, x_handle, website_url, telegram_url FROM subnets ORDER BY netuid")}
occupants = {r["occupant_key"]: dict(r) for r in cur.execute(
    "SELECT occupant_key, netuid, network_registered_at, status, name, first_seen_block, first_seen_at, last_seen_block, "
    "last_seen_at, deregistered_at, dereg_reason, predecessor_key, successor_key, registered_ts, deregistered_ts, source "
    "FROM occupants")}
last_bucket = cur.execute("SELECT era, bucket_ts, block_number, block_ts, price_method FROM backfill_buckets "
                          "WHERE status='done' ORDER BY bucket_ts DESC LIMIT 1").fetchone()
LAST_BLOCK, LAST_BLOCK_TS = last_bucket["block_number"], last_bucket["block_ts"]
buckets = cur.execute("SELECT bucket_ts, block_number, block_ts FROM backfill_buckets WHERE status='done' ORDER BY bucket_ts").fetchall()
bucket_by_ts = {b["bucket_ts"]: (b["block_number"], b["block_ts"]) for b in buckets}
bucket_ts_sorted = [b["bucket_ts"] for b in buckets]
LAST_DAY_TS = cur.execute("SELECT max(day_ts) FROM chain_daily").fetchone()[0]
LAST_1D_TS = cur.execute("SELECT max(ts) FROM price_bars WHERE netuid=64 AND period='1d'").fetchone()[0]
LAST_COMPLETE_1H_TS = int(dt.datetime(2026, 9, 24, 20, tzinfo=dt.UTC).timestamp())   # 1790280000: the 21:00Z tmc_candle (77 netuids) is partial (5m data ends 21:40Z); 20:00Z is the last complete hour
N_WITH_LAST_COMPLETE = sum(1 for n in subnets if cur.execute("SELECT 1 FROM price_bars WHERE netuid=? AND period='1h' AND ts=?", (n, LAST_COMPLETE_1H_TS)).fetchone())
print(f"netuids with a 1h bar at {iso(LAST_COMPLETE_1H_TS)}: {N_WITH_LAST_COMPLETE}", file=sys.stderr)
print(f"LAST_BLOCK={LAST_BLOCK} at {iso(LAST_BLOCK_TS)}; chain_daily last day {day(LAST_DAY_TS)}; last 1d bar {day(LAST_1D_TS)}",
      file=sys.stderr)


def bucket_block_at(ts):
    """Block of the latest done hourly bucket whose bucket_ts <= ts (bisect on the sorted list)."""
    import bisect
    i = bisect.bisect_right(bucket_ts_sorted, ts) - 1
    if i < 0:
        return None
    return bucket_by_ts[bucket_ts_sorted[i]] + (bucket_ts_sorted[i],)


# ---------------------------------------------------------------- per-netuid pass (bounded queries)
coverage_rows, ema_d_rows, ema_h_rows, dq_ph_rows, boundary_rows, xcheck_rows = [], [], [], [], [], []
ratio_month = defaultdict(lambda: [0, 0, 0.0, 0])   # month -> [rows, rows_ratio_lt_0.5, sum_ratio, rows_mp_null]
ratio_vals = defaultdict(list)                      # month -> list of mp/price ratios (for median / p10 / p90)
rank_hist_rows = []
mp_hist = {}      # netuid -> {hour_ts: (moving_price, reg_block, block)} for the last 30 d
daily_close_from_ph = {}   # netuid -> {day_ts: (last_ts, price)} current occupant only (for x-check)
SWAP_BOUNDARY_TS = 1_751_925_576  # 2025-07-07T21:59:36Z = first swap_sqrt row (SN64); hb = the 21:00Z bucket, hb-1 = the 20:00Z bucket (last reserve_ratio)
THIRTY_D_FROM = LAST_BLOCK_TS - 30 * DAY

for netuid, s in subnets.items():
    occ = s["occupant_key"]
    reg_ts = occupants[occ]["registered_ts"] or 0
    # ---- price_bars 1d (PK prefix netuid, period)
    b1d = cur.execute("SELECT ts, close, open, high, low, volume, moving_price, source, occupant_key, tao_in_pool "
                      "FROM price_bars WHERE netuid=? AND period='1d' ORDER BY ts", (netuid,)).fetchall()
    b1h = cur.execute("SELECT ts, close, source, occupant_key FROM price_bars WHERE netuid=? AND period='1h' ORDER BY ts",
                      (netuid,)).fetchall()
    cur1d = [r for r in b1d if r["occupant_key"] == occ]
    cur1h = [r for r in b1h if r["occupant_key"] == occ]
    src1d = defaultdict(int)
    for r in b1d:
        src1d[r["source"]] += 1
    src1h = defaultdict(int)
    for r in b1h:
        src1h[r["source"]] += 1
    switches_1d = sum(1 for a, b in zip(cur1d[:-1], cur1d[1:]) if a["source"] != b["source"])
    switches_1h = sum(1 for a, b in zip(cur1h[:-1], cur1h[1:]) if a["source"] != b["source"])
    exp_days = ((cur1d[-1]["ts"] - cur1d[0]["ts"]) // DAY + 1) if cur1d else 0
    exp_hours = ((cur1h[-1]["ts"] - cur1h[0]["ts"]) // HOUR + 1) if cur1h else 0
    gap_1h_bm = sum(1 for r in cur1h if r["source"] == "blockmachine" and r["ts"] >= GAP_ERA_FROM_TS)
    # ---- pool_history (PK prefix netuid; ~14k rows per netuid)
    ph = cur.execute("SELECT timestamp, block_number, price, moving_price, occupant_key, price_method, source, run_id, "
                     "network_registered_at, tao_in_pool FROM pool_history WHERE netuid=? ORDER BY timestamp", (netuid,)).fetchall()
    ph_days = {r["timestamp"] // DAY for r in ph}
    ph_days_pre_bars = {d for d in ph_days if d * DAY < BARS_START_TS}
    ph_cur = [r for r in ph if r["occupant_key"] == occ]
    ph_cur_days = {r["timestamp"] // DAY for r in ph_cur}
    # DQ on pool_history
    n_zero_price = sum(1 for r in ph if r["price"] is None or r["price"] <= 0)
    n_mp_null = sum(1 for r in ph if r["moving_price"] is None)
    n_mp_zero = sum(1 for r in ph if r["moving_price"] is not None and r["moving_price"] <= 0)
    n_repaired = sum(1 for r in ph if (r["run_id"] or "").startswith("script:repair"))
    pm_ranges = defaultdict(lambda: [0, None, None])
    for r in ph:
        k = pm_ranges[r["price_method"]]
        k[0] += 1
        k[1] = r["timestamp"] if k[1] is None else min(k[1], r["timestamp"])
        k[2] = r["timestamp"] if k[2] is None else max(k[2], r["timestamp"])
    pm_desc = "; ".join(f"{k}:{v[0]} [{day(v[1])}..{day(v[2])}]" for k, v in sorted(pm_ranges.items(), key=lambda x: str(x[0])))
    # moving-price ramp: per occupant, hours until |mp/price − 1| < 0.10 after registration (for the current occupant)
    ramp_h = None
    for i, r in enumerate(ph_cur):
        if r["price"] and r["moving_price"] is not None and abs(r["moving_price"] / r["price"] - 1) < 0.10:
            ramp_h = i
            break
    for r in ph:
        m = dt.datetime.fromtimestamp(r["timestamp"], dt.UTC).strftime("%Y-%m")
        agg = ratio_month[m]
        agg[0] += 1
        if r["moving_price"] is None or not r["price"]:
            agg[3] += 1
        else:
            ratio = r["moving_price"] / r["price"]
            agg[2] += ratio
            ratio_vals[m].append(ratio)
            if ratio < 0.5:
                agg[1] += 1
    # price-method boundary jump at 2025-07-07 21:00 bucket (reserve_ratio -> swap_sqrt): |log return| vs the local median
    idx = {r["timestamp"] // HOUR: r for r in ph}
    hb = SWAP_BOUNDARY_TS // HOUR
    if (hb - 1) in idx and hb in idx and idx[hb - 1]["price"] > 0 and idx[hb]["price"] > 0:
        jump = abs(math.log(idx[hb]["price"] / idx[hb - 1]["price"]))
        loc = []
        for h in range(hb - 168, hb + 168):
            if h in idx and (h - 1) in idx and idx[h]["price"] > 0 and idx[h - 1]["price"] > 0 and h != hb:
                loc.append(abs(math.log(idx[h]["price"] / idx[h - 1]["price"])))
        boundary_rows.append({"netuid": netuid, "abs_logret_at_boundary": jump, "median_abs_logret_pm7d": st.median(loc) if loc else None,
                              "p95_abs_logret_pm7d": sorted(loc)[int(0.95 * (len(loc) - 1))] if loc else None,
                              "method_before": idx[hb - 1]["price_method"], "method_after": idx[hb]["price_method"]})
    # chain daily close (last hourly row per day) for the current occupant → x-check vs stored tmc_candle 1d close
    dcl = {}
    for r in ph_cur:
        d = r["timestamp"] // DAY
        if d not in dcl or r["timestamp"] > dcl[d][0]:
            dcl[d] = (r["timestamp"], r["price"])
    daily_close_from_ph[netuid] = dcl
    diffs = []
    for r in cur1d:
        if r["source"] == "tmc_candle":
            d = r["ts"] // DAY
            if d in dcl and dcl[d][1] > 0:
                diffs.append(abs(r["close"] / dcl[d][1] - 1))
    if diffs:
        ds = sorted(diffs)
        xcheck_rows.append({"netuid": netuid, "n_days_compared": len(ds), "median_abs_rel_diff": st.median(ds),
                            "mean_abs_rel_diff": sum(ds) / len(ds), "p95_abs_rel_diff": ds[int(0.95 * (len(ds) - 1))],
                            "max_abs_rel_diff": ds[-1], "n_gt_1pct": sum(1 for x in ds if x > 0.01),
                            "n_gt_5pct": sum(1 for x in ds if x > 0.05)})
    # last-30d moving-price history for the dereg rank history
    mp_hist[netuid] = {r["timestamp"] // HOUR: (r["moving_price"], r["network_registered_at"], r["block_number"], r["occupant_key"])
                       for r in ph if r["timestamp"] >= THIRTY_D_FROM}
    dq_ph_rows.append({"netuid": netuid, "occupant_key": occ, "ph_rows": len(ph), "ph_first": iso(ph[0]["timestamp"]) if ph else None,
                       "ph_last": iso(ph[-1]["timestamp"]) if ph else None, "ph_distinct_days": len(ph_days),
                       "ph_days_before_bars_start": len(ph_days_pre_bars), "ph_rows_current_occ": len(ph_cur),
                       "ph_days_current_occ": len(ph_cur_days), "price_le0": n_zero_price, "mp_null": n_mp_null, "mp_le0": n_mp_zero,
                       "repaired_rows": n_repaired, "price_method_ranges": pm_desc,
                       "ramp_hours_to_mp_within_10pct": ramp_h, "sources": ";".join(sorted({r["source"] for r in ph if r["source"]}))})
    coverage_rows.append({
        "netuid": netuid, "name": s["name"], "status": s["status"], "occupant_key": occ, "registered_ts": iso(reg_ts) if reg_ts else None,
        "n_1d_all_occ": len(b1d), "n_1d_current_occ": len(cur1d), "n_1d_blockmachine": src1d.get("blockmachine", 0),
        "n_1d_tmc_candle": src1d.get("tmc_candle", 0), "n_1d_onfinality": src1d.get("onfinality", 0),
        "first_1d_current": day(cur1d[0]["ts"]) if cur1d else None, "last_1d_current": day(cur1d[-1]["ts"]) if cur1d else None,
        "missing_days_current": exp_days - len(cur1d), "source_switches_1d_current": switches_1d,
        "n_1d_null_open": sum(1 for r in cur1d if r["open"] is None), "n_1d_null_volume": sum(1 for r in cur1d if r["volume"] is None),
        "n_1d_null_mp": sum(1 for r in cur1d if r["moving_price"] is None), "min_close_1d": min((r["close"] for r in cur1d), default=None),
        "n_1h_all_occ": len(b1h), "n_1h_current_occ": len(cur1h), "n_1h_blockmachine": src1h.get("blockmachine", 0),
        "n_1h_tmc_candle": src1h.get("tmc_candle", 0), "n_1h_onfinality": src1h.get("onfinality", 0),
        "first_1h_current": iso(cur1h[0]["ts"]) if cur1h else None, "last_1h_current": iso(cur1h[-1]["ts"]) if cur1h else None,
        "missing_hours_current": exp_hours - len(cur1h), "source_switches_1h_current": switches_1h,
        "gap_era_1h_blockmachine_fallback_bars": gap_1h_bm,
        "ph_distinct_days_current_occ": len(ph_cur_days), "ph_days_before_2025_05_19": len(ph_days_pre_bars),
        "ge72_1d": len(cur1d) >= 72, "ge144_1d": len(cur1d) >= 144, "ge365_1d": len(cur1d) >= 365, "ge2000_1h": len(cur1h) >= 2000,
        "has_github": bool(s["github_url"]), "has_discord": bool(s["discord_url"]), "has_x": bool(s["x_handle"]),
        "has_website": bool(s["website_url"]), "has_telegram": bool(s["telegram_url"]),
    })
    # ---- EMA72 daily (current occupant). As-of = last available bar (partial day flagged) + previous complete bar.
    closes_d = [r["close"] for r in cur1d]
    md = band_metrics(closes_d)
    md_prev = band_metrics(closes_d[:-1]) if len(closes_d) > 1 and cur1d[-1]["ts"] == LAST_1D_TS else {"dist_pct": None, "pos": None}
    ret30 = (closes_d[-1] / closes_d[-31] - 1) * 100 if len(closes_d) >= 31 else None
    ret90 = (closes_d[-1] / closes_d[-91] - 1) * 100 if len(closes_d) >= 91 else None
    ema_d_rows.append({"netuid": netuid, "name": s["name"], "status": s["status"], "occupant_key": occ, "n_bars": md["n_bars"],
                       "as_of_bar": day(cur1d[-1]["ts"]) if cur1d else None, "last_bar_source": cur1d[-1]["source"] if cur1d else None,
                       "last_bar_partial_day": bool(cur1d and cur1d[-1]["ts"] == LAST_1D_TS),
                       "close": md["close"], "ema72": md["ema"], "ema_valid_ge72": md["ema_valid"], "dist_pct": md["dist_pct"],
                       "sigma72_logret": md["sigma"], "band_upper": md["upper"], "band_lower": md["lower"], "band_pos": md["pos"],
                       "inside_band": (abs(md["pos"]) <= 1) if md["pos"] is not None else None,
                       "dist_pct_prev_complete_day": md_prev["dist_pct"], "band_pos_prev_complete_day": md_prev["pos"],
                       "ret_30d_pct": ret30, "ret_90d_pct": ret90, "moving_price_last": cur1d[-1]["moving_price"] if cur1d else None,
                       "tao_in_pool_last": cur1d[-1]["tao_in_pool"] if cur1d else None})
    # ---- EMA72 hourly (72 h). Last bar = 21:00Z candle (partial: data ends 21:40Z) → also report the last complete hour.
    cur1h_c = [r for r in cur1h if r["ts"] <= LAST_COMPLETE_1H_TS]
    closes_h = [r["close"] for r in cur1h_c]
    mh = band_metrics(closes_h)
    last_h_ts = cur1h_c[-1]["ts"] if cur1h_c else None
    mh_prev = band_metrics(closes_h[:-1]) if len(closes_h) > 1 else {"dist_pct": None, "pos": None}
    ema_h_rows.append({"netuid": netuid, "name": s["name"], "status": s["status"], "occupant_key": occ, "n_bars": mh["n_bars"],
                       "as_of_bar": iso(last_h_ts), "last_bar_source": cur1h_c[-1]["source"] if cur1h_c else None,
                       "dropped_partial_bars": len(cur1h) - len(cur1h_c),
                       "close": mh["close"], "ema72h": mh["ema"], "ema_valid_ge72": mh["ema_valid"], "dist_pct": mh["dist_pct"],
                       "sigma72_logret": mh["sigma"], "band_upper": mh["upper"], "band_lower": mh["lower"], "band_pos": mh["pos"],
                       "inside_band": (abs(mh["pos"]) <= 1) if mh["pos"] is not None else None,
                       "dist_pct_prev_bar": mh_prev["dist_pct"], "band_pos_prev_bar": mh_prev["pos"]})
    if netuid % 20 == 0:
        print(f"  netuid {netuid} done t={time.time()-T0:.1f}s", file=sys.stderr)

write_csv("bars_coverage_per_netuid.csv", coverage_rows, list(coverage_rows[0].keys()))
write_csv("ema72_daily_latest.csv", ema_d_rows, list(ema_d_rows[0].keys()))
write_csv("ema72_hourly_latest.csv", ema_h_rows, list(ema_h_rows[0].keys()))
write_csv("dq_pool_history_per_netuid.csv", dq_ph_rows, list(dq_ph_rows[0].keys()))
write_csv("dq_price_method_boundary_jumps.csv", boundary_rows, list(boundary_rows[0].keys()))
write_csv("dq_1d_crosscheck_tmc_vs_chain.csv", xcheck_rows, list(xcheck_rows[0].keys()))
def _q(a, q):
    a = sorted(a)
    return a[int(q * (len(a) - 1))] if a else None


rm_rows = [{"month": m, "rows": v[0], "share_ratio_lt_0_5": v[1] / v[0] if v[0] else None,
            "mean_mp_over_price": v[2] / (v[0] - v[3]) if v[0] - v[3] else None, "median_mp_over_price": _q(ratio_vals[m], 0.5),
            "p10_mp_over_price": _q(ratio_vals[m], 0.10), "p90_mp_over_price": _q(ratio_vals[m], 0.90), "rows_mp_null": v[3]}
           for m, v in sorted(ratio_month.items())]
write_csv("dq_moving_price_ratio_by_month.csv", rm_rows, list(rm_rows[0].keys()))

valid_d = [r for r in ema_d_rows if r["ema_valid_ge72"] and r["netuid"] != 0 and r["status"] == "active"]
top_above = sorted(valid_d, key=lambda r: -r["dist_pct"])[:10]
top_below = sorted(valid_d, key=lambda r: r["dist_pct"])[:10]
write_csv("ema72_daily_top10_above.csv", top_above, list(ema_d_rows[0].keys()))
write_csv("ema72_daily_top10_below.csv", top_below, list(ema_d_rows[0].keys()))
valid_h = [r for r in ema_h_rows if r["ema_valid_ge72"] and r["netuid"] != 0 and r["status"] == "active"]
write_csv("ema72_hourly_top10_above.csv", sorted(valid_h, key=lambda r: -r["dist_pct"])[:10], list(ema_h_rows[0].keys()))
write_csv("ema72_hourly_top10_below.csv", sorted(valid_h, key=lambda r: r["dist_pct"])[:10], list(ema_h_rows[0].keys()))

# ---------------------------------------------------------------- dereg distance at LAST_BLOCK (PK lookups)
xs = {}
for netuid, s in subnets.items():
    r = cur.execute("SELECT netuid, block_number, timestamp, price, moving_price, tao_in_pool, alpha_in_pool, network_registered_at, "
                    "occupant_key, price_method FROM pool_history WHERE netuid=? AND block_number=?", (netuid, LAST_BLOCK)).fetchone()
    if r is None:   # fall back to the latest row at/before the last block time (index netuid, timestamp DESC)
        r = cur.execute("SELECT netuid, block_number, timestamp, price, moving_price, tao_in_pool, alpha_in_pool, network_registered_at, "
                        "occupant_key, price_method FROM pool_history WHERE netuid=? AND timestamp<=? ORDER BY timestamp DESC LIMIT 1",
                        (netuid, LAST_BLOCK_TS)).fetchone()
    if r is not None:
        xs[netuid] = dict(r)
cd = {r["netuid"]: dict(r) for r in cur.execute(
    "SELECT netuid, emission_percent, subnet_tao, volume_24h, daily_emission FROM chain_daily WHERE day_ts=?", (LAST_DAY_TS,))}
rows = []
for netuid, s in subnets.items():
    r = xs.get(netuid)
    if r is None:
        rows.append({"netuid": netuid, "name": s["name"], "status": s["status"], "note": "no pool_history row at last block"})
        continue
    reg = r["network_registered_at"] if r["network_registered_at"] is not None else s["network_registered_at"]
    age = LAST_BLOCK - reg
    immune = age < IMMUNITY_BLOCKS
    rows.append({"netuid": netuid, "name": s["name"], "status": s["status"], "occupant_key": r["occupant_key"], "block": r["block_number"],
                 "at": iso(r["timestamp"]), "stale_row": r["block_number"] != LAST_BLOCK, "price": r["price"], "moving_price": r["moving_price"],
                 "price_vs_mp_pct": (r["price"] / r["moving_price"] - 1) * 100 if r["moving_price"] else None,
                 "tao_in_pool_tau": r["tao_in_pool"], "registered_block": reg, "age_blocks": age, "age_days": age * BLOCK_S / DAY,
                 "immune": immune, "immunity_blocks_left": (reg + IMMUNITY_BLOCKS - LAST_BLOCK) if immune else 0,
                 "immunity_days_left": (reg + IMMUNITY_BLOCKS - LAST_BLOCK) * BLOCK_S / DAY if immune else 0.0,
                 "immunity_ends_est": iso(LAST_BLOCK_TS + (reg + IMMUNITY_BLOCKS - LAST_BLOCK) * BLOCK_S) if immune else None,
                 "emission_percent_lastday": cd.get(netuid, {}).get("emission_percent"),
                 "subnet_tao_lastday_rao": cd.get(netuid, {}).get("subnet_tao"), "price_method": r["price_method"]})
pool = sorted([r for r in rows if r.get("netuid") not in (None, 0) and not r.get("immune") and r.get("moving_price") is not None
               and r["status"] == "active"], key=lambda r: (r["moving_price"], -r["registered_block"]))
mp_low = pool[0]["moving_price"]
mp_3rd = pool[2]["moving_price"]
for i, r in enumerate(pool, 1):
    r["rank_from_bottom"] = i
    r["pool_size"] = len(pool)
    r["gap_to_lowest_pct"] = (r["moving_price"] - mp_low) / r["moving_price"] * 100
    r["gap_to_3rd_lowest_pct"] = (r["moving_price"] - mp_3rd) / r["moving_price"] * 100
    r["in_bottom3"] = i <= 3
# 30-day rank history from mp_hist (hourly cross-sections rebuilt in Python)
hours = sorted({h for d in mp_hist.values() for h in d})
rank_hist = defaultdict(lambda: {"hours_seen": 0, "hours_in_bottom3": 0, "hours_in_bottom5": 0, "min_rank": None, "max_rank": None,
                                 "first_rank": None, "last_rank": None})
for h in hours:
    cross = []
    for n, d in mp_hist.items():
        if h in d and n != 0 and subnets[n]["status"] == "active":
            mp, reg, blk, occ = d[h]
            if mp is None or reg is None or occ != subnets[n]["occupant_key"]:   # current occupant only (no predecessor splice)
                continue
            if blk - reg >= IMMUNITY_BLOCKS:
                cross.append((mp, -reg, n))
    cross.sort()
    for i, (_, _, n) in enumerate(cross, 1):
        a = rank_hist[n]
        a["hours_seen"] += 1
        a["hours_in_bottom3"] += i <= 3
        a["hours_in_bottom5"] += i <= 5
        a["min_rank"] = i if a["min_rank"] is None else min(a["min_rank"], i)
        a["max_rank"] = i if a["max_rank"] is None else max(a["max_rank"], i)
        if a["first_rank"] is None:
            a["first_rank"] = i
        a["last_rank"] = i
for r in rows:
    a = rank_hist.get(r.get("netuid"))
    if a:
        r.update({"h30d_seen": a["hours_seen"], "h30d_bottom3": a["hours_in_bottom3"], "h30d_bottom5": a["hours_in_bottom5"],
                  "rank_min_30d": a["min_rank"], "rank_max_30d": a["max_rank"], "rank_30d_ago": a["first_rank"]})
dereg_cols = ["netuid", "name", "status", "occupant_key", "block", "at", "stale_row", "immune", "rank_from_bottom", "pool_size", "in_bottom3",
              "moving_price", "price", "price_vs_mp_pct", "gap_to_lowest_pct", "gap_to_3rd_lowest_pct", "registered_block", "age_blocks",
              "age_days", "immunity_blocks_left", "immunity_days_left", "immunity_ends_est", "tao_in_pool_tau", "emission_percent_lastday",
              "subnet_tao_lastday_rao", "h30d_seen", "h30d_bottom3", "h30d_bottom5", "rank_min_30d", "rank_max_30d", "rank_30d_ago",
              "price_method", "note"]
rows_sorted = sorted(rows, key=lambda r: (r.get("immune", True), r.get("rank_from_bottom") or 10**6, r.get("immunity_blocks_left") or 0))
write_csv("dereg_distance_latest.csv", rows_sorted, dereg_cols)

# ---------------------------------------------------------------- hazard inputs: 30-day history per netuid
WIN_FROM = LAST_DAY_TS - 29 * DAY
hz_rows = []
for netuid, s in subnets.items():
    cdr = cur.execute("SELECT day_ts, emission_percent, subnet_tao, volume_24h, daily_emission, owner_personal_locked_alpha FROM chain_daily "
                      "WHERE netuid=? AND day_ts BETWEEN ? AND ? ORDER BY day_ts", (netuid, WIN_FROM, LAST_DAY_TS)).fetchall()
    cd_all = cur.execute("SELECT count(*), min(day_ts), max(day_ts) FROM chain_daily WHERE netuid=?", (netuid,)).fetchone()
    pb = cur.execute("SELECT count(*) FROM price_bars WHERE netuid=? AND period='1d' AND ts BETWEEN ? AND ? AND occupant_key=?",
                     (netuid, WIN_FROM, LAST_DAY_TS, s["occupant_key"])).fetchone()[0]
    phh = cur.execute("SELECT count(*), sum(moving_price IS NULL) FROM pool_history WHERE netuid=? AND timestamp BETWEEN ? AND ?",
                      (netuid, WIN_FROM, LAST_BLOCK_TS)).fetchone()
    hz_rows.append({"netuid": netuid, "name": s["name"], "status": s["status"], "chain_daily_rows_30d": len(cdr),
                    "emission_percent_nonnull_30d": sum(1 for r in cdr if r["emission_percent"] is not None),
                    "subnet_tao_nonnull_30d": sum(1 for r in cdr if r["subnet_tao"] is not None),
                    "volume_24h_nonnull_30d": sum(1 for r in cdr if r["volume_24h"] is not None),
                    "owner_locked_nonnull_30d": sum(1 for r in cdr if r["owner_personal_locked_alpha"] is not None),
                    "chain_daily_rows_total": cd_all[0], "chain_daily_first": day(cd_all[1]), "chain_daily_last": day(cd_all[2]),
                    "price_1d_bars_30d_current_occ": pb, "pool_history_hours_30d": phh[0], "pool_history_mp_null_30d": phh[1],
                    "emission_percent_first": cdr[0]["emission_percent"] if cdr else None,
                    "emission_percent_last": cdr[-1]["emission_percent"] if cdr else None,
                    "emission_pct_change_30d": ((cdr[-1]["emission_percent"] / cdr[0]["emission_percent"] - 1) * 100)
                    if cdr and cdr[0]["emission_percent"] and cdr[-1]["emission_percent"] is not None else None,
                    "subnet_tao_change_30d_pct": ((cdr[-1]["subnet_tao"] / cdr[0]["subnet_tao"] - 1) * 100)
                    if cdr and cdr[0]["subnet_tao"] and cdr[-1]["subnet_tao"] is not None else None,
                    "hazard_inputs_complete": len(cdr) == 30 and all(r["emission_percent"] is not None and r["subnet_tao"] is not None for r in cdr) and pb == 30})
write_csv("hazard_inputs_30d.csv", hz_rows, list(hz_rows[0].keys()))

# ---------------------------------------------------------------- survivorship: the deregistered occupants
dead = [o for o in occupants.values() if o["status"] == "deregistered"]
sv_rows, pred_rows = [], []
for o in sorted(dead, key=lambda o: o["last_seen_at"]):
    k = o["occupant_key"]
    n1d, min1d, max1d = cur.execute("SELECT count(*), min(ts), max(ts) FROM price_bars WHERE occupant_key=? AND period='1d'", (k,)).fetchone()
    n1h = cur.execute("SELECT count(*) FROM price_bars WHERE occupant_key=? AND period='1h'", (k,)).fetchone()[0]
    ph = cur.execute("SELECT timestamp, block_number, price, moving_price, network_registered_at FROM pool_history WHERE netuid=? AND "
                     "timestamp BETWEEN ? AND ? ORDER BY timestamp", (o["netuid"], o["first_seen_at"], o["last_seen_at"])).fetchall()
    days = {}
    for r in ph:
        d = r["timestamp"] // DAY
        if d not in days or r["timestamp"] > days[d][0]:
            days[d] = (r["timestamp"], r["price"])
    closes = [days[d][1] for d in sorted(days)]
    life_d = (o["last_seen_at"] - (o["registered_ts"] or o["first_seen_at"])) / DAY
    death = o["last_seen_at"]
    # predictors at T-30d and T-7d: non-immune rank from bottom by moving price (cross-section rebuilt from PK lookups)
    pred = {"occupant_key": k, "netuid": o["netuid"], "name": o["name"], "death_last_seen": iso(death), "lifespan_days": life_d,
            "n_daily_closes_from_ph": len(closes)}
    for lab, back in (("t30", 30 * DAY), ("t7", 7 * DAY), ("t0", 0)):
        bb = bucket_block_at(death - back)
        if bb is None:
            continue
        blk, bts, bucket_ts = bb
        cross = []
        me = None
        for n in subnets:   # 129 PK lookups per cross-section
            r = cur.execute("SELECT moving_price, network_registered_at, price FROM pool_history WHERE netuid=? AND block_number=?",
                            (n, blk)).fetchone()
            if r is None or n == 0 or r["moving_price"] is None or r["network_registered_at"] is None:
                continue
            if blk - r["network_registered_at"] >= IMMUNITY_BLOCKS:
                cross.append((r["moving_price"], -r["network_registered_at"], n))
            if n == o["netuid"]:
                me = r
        cross.sort()
        rank = next((i for i, (_, _, n) in enumerate(cross, 1) if n == o["netuid"]), None)
        pred[f"{lab}_block"] = blk
        pred[f"{lab}_rank_from_bottom"] = rank
        pred[f"{lab}_pool_size"] = len(cross)
        pred[f"{lab}_immune"] = (blk - me["network_registered_at"] < IMMUNITY_BLOCKS) if me else None
        pred[f"{lab}_moving_price"] = me["moving_price"] if me else None
        # EMA72 distance on the occupant's own daily closes up to that time
        cl = [days[d][1] for d in sorted(days) if (d + 1) * DAY <= bts]
        m = band_metrics(cl)
        pred[f"{lab}_ema72_dist_pct"] = m["dist_pct"] if m["ema_valid"] else None
        pred[f"{lab}_n_daily_closes"] = len(cl)
    pred["death_cause_heuristic"] = ("prune_consistent_rank1" if pred.get("t0_rank_from_bottom") == 1 else
                                     "immune_at_death" if pred.get("t0_immune") else
                                     ("not_lowest_at_death_rank%s" % pred.get("t0_rank_from_bottom")) if pred.get("t0_rank_from_bottom") else "unknown")
    pred_rows.append(pred)
    sv_rows.append({"occupant_key": k, "netuid": o["netuid"], "name": o["name"], "registered_ts": iso(o["registered_ts"]),
                    "first_seen_at": iso(o["first_seen_at"]), "last_seen_at": iso(o["last_seen_at"]), "deregistered_ts": iso(o["deregistered_ts"]),
                    "hours_last_seen_to_successor_reg": (o["deregistered_ts"] - o["last_seen_at"]) / HOUR if o["deregistered_ts"] else None,
                    "dereg_reason": o["dereg_reason"], "successor_key": o["successor_key"], "predecessor_key": o["predecessor_key"],
                    "lifespan_days": life_d, "n_1d_bars": n1d, "first_1d_bar": day(min1d), "last_1d_bar": day(max1d), "n_1h_bars": n1h,
                    "ph_hourly_rows": len(ph), "ph_distinct_days": len(days), "ge90_daily_bars": n1d >= 90, "ge90_daily_from_ph": len(days) >= 90,
                    "ge72_daily_from_ph": len(days) >= 72, "died_before_bars_start": o["last_seen_at"] < BARS_START_TS})
write_csv("survivorship_dead_occupants.csv", sv_rows, list(sv_rows[0].keys()))
pred_cols = ["occupant_key", "netuid", "name", "death_last_seen", "lifespan_days", "n_daily_closes_from_ph", "t30_block", "t30_rank_from_bottom",
             "t30_pool_size", "t30_immune", "t30_moving_price", "t30_ema72_dist_pct", "t30_n_daily_closes", "t7_block", "t7_rank_from_bottom",
             "t7_pool_size", "t7_immune", "t7_moving_price", "t7_ema72_dist_pct", "t7_n_daily_closes", "t0_block", "t0_rank_from_bottom", "t0_pool_size",
             "t0_immune", "t0_moving_price", "t0_ema72_dist_pct", "t0_n_daily_closes", "death_cause_heuristic"]
write_csv("survivorship_preview_predictors.csv", pred_rows, pred_cols)

# ---------------------------------------------------------------- headline numbers
active = [r for r in coverage_rows if r["status"] == "active"]
hl = {
    "as_of": {"last_block": LAST_BLOCK, "last_block_ts": iso(LAST_BLOCK_TS), "last_1d_bar": day(LAST_1D_TS), "chain_daily_last": day(LAST_DAY_TS)},
    "subnets": {"rows": len(subnets), "active": len(active), "pre_launch": sum(1 for r in coverage_rows if r["status"] == "pre_launch")},
    "bars_1d_current_occ": {f"ge{n}": sum(1 for r in coverage_rows if r["n_1d_current_occ"] >= n) for n in (72, 144, 365)},
    "bars_1d_any_occ": {f"ge{n}": sum(1 for r in coverage_rows if r["n_1d_all_occ"] >= n) for n in (72, 144, 365)},
    "bars_1h_current_occ_ge2000": sum(1 for r in coverage_rows if r["n_1h_current_occ"] >= 2000),
    "bars_1h_any_occ_ge2000": sum(1 for r in coverage_rows if r["n_1h_all_occ"] >= 2000),
    "ph_days_current_occ": {f"ge{n}": sum(1 for r in coverage_rows if r["ph_distinct_days_current_occ"] >= n) for n in (72, 144, 365)},
    "netuids_with_pre_bars_days": sum(1 for r in coverage_rows if r["ph_days_before_2025_05_19"] > 0),
    "sum_pre_bars_days": sum(r["ph_days_before_2025_05_19"] for r in coverage_rows),
    "missing_days_total_current": sum(r["missing_days_current"] for r in coverage_rows),
    "netuids_missing_days_gt0": sum(1 for r in coverage_rows if r["missing_days_current"] > 0),
    "missing_hours_total_current": sum(r["missing_hours_current"] for r in coverage_rows),
    "source_switches_1d_total": sum(r["source_switches_1d_current"] for r in coverage_rows),
    "gap_era_1h_bm_fallback_total": sum(r["gap_era_1h_blockmachine_fallback_bars"] for r in coverage_rows),
    "ema_daily_valid": sum(1 for r in ema_d_rows if r["ema_valid_ge72"]), "ema_daily_sigma_valid": sum(1 for r in ema_d_rows if r["sigma72_logret"] is not None),
    "ema_hourly_valid": sum(1 for r in ema_h_rows if r["ema_valid_ge72"]),
    "ema_daily_inside_band": sum(1 for r in valid_d if r["inside_band"]), "ema_daily_above_band": sum(1 for r in valid_d if r["band_pos"] is not None and r["band_pos"] > 1),
    "ema_daily_below_band": sum(1 for r in valid_d if r["band_pos"] is not None and r["band_pos"] < -1),
    "ema_daily_above_ema": sum(1 for r in valid_d if r["dist_pct"] > 0), "ema_daily_n_valid_active_nonroot": len(valid_d),
    "ema_daily_dist_quantiles": {q: _q([r["dist_pct"] for r in valid_d], q) for q in (0.1, 0.25, 0.5, 0.75, 0.9)},
    "ema_daily_sigma_quantiles": {q: _q([r["sigma72_logret"] for r in valid_d if r["sigma72_logret"] is not None], q) for q in (0.1, 0.5, 0.9)},
    "ema_hourly_dist_quantiles": {q: _q([r["dist_pct"] for r in valid_h], q) for q in (0.1, 0.25, 0.5, 0.75, 0.9)},
    "ema_hourly_dropped_partial_bars": sum(r["dropped_partial_bars"] for r in ema_h_rows), "ema_hourly_as_of_distinct": sorted({r["as_of_bar"] for r in ema_h_rows}),
    "dereg": {"pool_size_non_immune": len(pool), "immune_active": sum(1 for r in rows if r.get("immune") and r["status"] == "active"),
              "pre_launch": sum(1 for r in rows if r["status"] == "pre_launch"), "mp_lowest": mp_low, "mp_3rd": mp_3rd,
              "bottom3": [(r["netuid"], r["moving_price"]) for r in pool[:3]], "stale_rows": sum(1 for r in rows if r.get("stale_row"))},
    "hazard": {"complete_30d": sum(1 for r in hz_rows if r["hazard_inputs_complete"]), "chain_daily_30_rows": sum(1 for r in hz_rows if r["chain_daily_rows_30d"] == 30),
               "chain_daily_0_rows": [r["netuid"] for r in hz_rows if r["chain_daily_rows_30d"] == 0],
               "price_30_bars": sum(1 for r in hz_rows if r["price_1d_bars_30d_current_occ"] == 30)},
    "dead": {"n": len(sv_rows), "ge90_daily_bars": sum(1 for r in sv_rows if r["ge90_daily_bars"]), "ge90_daily_from_ph": sum(1 for r in sv_rows if r["ge90_daily_from_ph"]),
             "ge72_daily_from_ph": sum(1 for r in sv_rows if r["ge72_daily_from_ph"]), "died_before_bars_start": sum(1 for r in sv_rows if r["died_before_bars_start"]),
             "zero_1d_bars": sum(1 for r in sv_rows if r["n_1d_bars"] == 0),
             "median_lifespan_days": st.median(r["lifespan_days"] for r in sv_rows), "min_lifespan_days": min(r["lifespan_days"] for r in sv_rows),
             "max_lifespan_days": max(r["lifespan_days"] for r in sv_rows),
             "median_hours_last_seen_to_successor": st.median(r["hours_last_seen_to_successor_reg"] for r in sv_rows if r["hours_last_seen_to_successor_reg"] is not None),
             "t30_rank_known": sum(1 for p in pred_rows if p.get("t30_rank_from_bottom") is not None),
             "t30_bottom3": sum(1 for p in pred_rows if (p.get("t30_rank_from_bottom") or 99) <= 3),
             "t30_bottom5": sum(1 for p in pred_rows if (p.get("t30_rank_from_bottom") or 99) <= 5),
             "t30_immune": sum(1 for p in pred_rows if p.get("t30_immune")),
             "t7_rank_known": sum(1 for p in pred_rows if p.get("t7_rank_from_bottom") is not None),
             "t7_bottom3": sum(1 for p in pred_rows if (p.get("t7_rank_from_bottom") or 99) <= 3),
             "t7_bottom1": sum(1 for p in pred_rows if (p.get("t7_rank_from_bottom") or 99) <= 1),
             "t7_immune": sum(1 for p in pred_rows if p.get("t7_immune")),
             "t30_ema_known": sum(1 for p in pred_rows if p.get("t30_ema72_dist_pct") is not None),
             "t30_ema_below": sum(1 for p in pred_rows if p.get("t30_ema72_dist_pct") is not None and p["t30_ema72_dist_pct"] < 0),
             "t0_rank_known": sum(1 for p in pred_rows if p.get("t0_rank_from_bottom") is not None),
             "t0_rank1": sum(1 for p in pred_rows if p.get("t0_rank_from_bottom") == 1),
             "t0_bottom3": sum(1 for p in pred_rows if (p.get("t0_rank_from_bottom") or 99) <= 3),
             "t0_immune": sum(1 for p in pred_rows if p.get("t0_immune")),
             "death_cause_counts": {c: sum(1 for p in pred_rows if p["death_cause_heuristic"] == c) for c in sorted({p["death_cause_heuristic"] for p in pred_rows})},
             "t7_ema_known": sum(1 for p in pred_rows if p.get("t7_ema72_dist_pct") is not None),
             "t7_ema_below": sum(1 for p in pred_rows if p.get("t7_ema72_dist_pct") is not None and p["t7_ema72_dist_pct"] < 0)},
    "dq": {"price_le0_rows": sum(r["price_le0"] for r in dq_ph_rows), "mp_null_rows": sum(r["mp_null"] for r in dq_ph_rows),
           "mp_le0_rows": sum(r["mp_le0"] for r in dq_ph_rows), "repaired_rows": sum(r["repaired_rows"] for r in dq_ph_rows),
           "netuids_repaired": sum(1 for r in dq_ph_rows if r["repaired_rows"]),
           "ramp_hours_median": st.median(r["ramp_hours_to_mp_within_10pct"] for r in dq_ph_rows if r["ramp_hours_to_mp_within_10pct"] is not None),
           "ramp_hours_max": max((r["ramp_hours_to_mp_within_10pct"] for r in dq_ph_rows if r["ramp_hours_to_mp_within_10pct"] is not None), default=None),
           "ramp_never": [r["netuid"] for r in dq_ph_rows if r["ramp_hours_to_mp_within_10pct"] is None],
           "xcheck_netuids": len(xcheck_rows), "xcheck_median_of_medians": st.median(r["median_abs_rel_diff"] for r in xcheck_rows),
           "xcheck_days_gt_1pct": sum(r["n_gt_1pct"] for r in xcheck_rows), "xcheck_days_gt_5pct": sum(r["n_gt_5pct"] for r in xcheck_rows),
           "xcheck_days_total": sum(r["n_days_compared"] for r in xcheck_rows),
           "boundary_jump_median": st.median(r["abs_logret_at_boundary"] for r in boundary_rows),
           "boundary_local_median_of_medians": st.median(r["median_abs_logret_pm7d"] for r in boundary_rows if r["median_abs_logret_pm7d"] is not None),
           "boundary_n_gt_p95": sum(1 for r in boundary_rows if r["p95_abs_logret_pm7d"] is not None and r["abs_logret_at_boundary"] > r["p95_abs_logret_pm7d"]),
           "null_open_1d": sum(r["n_1d_null_open"] for r in coverage_rows), "null_volume_1d": sum(r["n_1d_null_volume"] for r in coverage_rows),
           "null_mp_1d": sum(r["n_1d_null_mp"] for r in coverage_rows), "min_close_1d": min(r["min_close_1d"] for r in coverage_rows if r["min_close_1d"] is not None)},
    "identity": {k: sum(1 for r in active if r[k]) for k in ("has_github", "has_discord", "has_x", "has_website", "has_telegram")},
    "runtime_s": round(time.time() - T0, 1),
}
with open(os.path.join(OUT, "headline.json"), "w") as f:
    json.dump(hl, f, indent=1, default=str)
print(json.dumps(hl, indent=1, default=str))

```
