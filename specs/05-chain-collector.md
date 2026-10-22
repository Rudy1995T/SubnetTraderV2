# 05 — Chain collector (TMC SSE + REST, gate/burn model, immunity, dereg hazard, holders, optional RPC)

**Status:** proposed (reconciled to 02 and adversarially reviewed 2026-09-23) · **Phase:** 2 (`TmcClient` itself is built at the start of Phase 1, §11 Q8) · **Depends on:** 02 (binding ledger: B1–B9, C7, D1–D4, E1–E4, F2, G1–G3, J1), 01 (`Http`/`ProviderLimiter`, `CollectorJob`/`LongTask`, `JOB_TABLE`, `/health`, `app/util/time.py`), 03 (all DDL, precedence upserts, `put_metric_values`, events/metric catalogs, `chain_prune` retention policy), 04 (occupant keying; writes the `/subnets/` and coldkey-identities snapshots, G2), 12 (P1 registry rows, flag rules), 16 (liveness, pause resolution) · **Master spec refs:** §2 (reuse of `dereg_risk.py`, `endpoints.py`), §4 (tier-0 TMC, tier-3 lite node), §5 (occupant identity, local immunity 864,000), §6 (`chain_ticks`, `chain_hourly`, `chain_daily`, `price_bars`, `holders_snapshots`, `metric_values`, `events`, `collector_runs`), §7.3 P1 metrics, §7.5 chain red-flag inputs, §7.6 regime inputs, §11 ops, §12 Phase 2.

---

## 1. Purpose and scope

The chain collector is the keyless on-chain recorder for TaoSentinel. It turns taomarketcap's (TMC) free `public/v1` API into point-in-time raw tables and computes the ten **P1 · Chain Vitality** metrics plus the chain-derived inputs for red flags, regime scaling and the quant harness. Phase 2 ends with the **first matrix (P1 only)** rendered from these tables.

**In scope**
- A long-lived **SSE recorder** (`LongTask` `chain_sse`, E1) on `GET /public/v1/sse/subnets/table/` writing `chain_ticks` (12 hot fields every `CHAIN_TICKS_BLOCK_STRIDE` = 5 blocks ≈ 1 min, plus every flag change; F2) and `chain_hourly` (all 44 table fields as 03's named columns, last state per hour), with heartbeat, reconnect/backoff and a REST fallback.
- **`TmcClient`** (G2, `app/collectors/chain_tmc.py`): the only TMC caller in the process. It sits on 01's `Http.request('tmc', …)` and `ProviderLimiter` (G1: 8/min, burst 2, 500/h, 90 k/month; no other limiter module). Specs 04, 06, 11 and 17 call its methods; §6 holds the **TMC budget table for every caller** (G3).
- **REST tiers** (hourly / daily / weekly-sliced) derived from research `02-gapfill-keyless-onchain-pillar-source.md` §6, minus `/subnets/` and coldkey identities, which 04 fetches (G2).
- **v440 gate reproduction** (demand share → burn-adjusted share → θ = 32nd-highest → Hill gate h = 3) with an iterative fit of `(1 − miner_burned)` per subnet, producing `predicted_gated_share` and `implied_miner_burn` per subnet per daily snapshot.
- **Local immunity** (`block − network_registered_at < NETWORK_IMMUNITY_PERIOD_BLOCKS`), the TMC `immune` quirk for not-started subnets.
- **Dereg hazard** = ported `dereg_hazard_score` with proximity on the non-immune moving-price rank, `emission_enabled = False ⇒ 1.0`.
- **Holder concentration** (top-10 share, HHI, Gini) from `/accounts/subnet-holders/{n}/`.
- **Exit capacity** from `price_after / price`; field-semantics glossary; regime input series (Σ subnet prices, TAO/USD).
- **Optional RPC collector** (`SUBTENSOR_RPC_ENABLED=false` by default): bittensor SDK v11 or raw JSON-RPC against a local lite node / OnFinality for `MinerBurned`, `SubnetOwnerCut`, `NetworkImmunityPeriod`, `EmissionBarQuantile`, `EmissionGateExponent`, metagraph active counts and vtrust dispersion, behind the old `EndpointSelector` failover.
- **Power-loss catch-up**: `analytics ?span=1W` hourly rows repair `chain_hourly` gaps, `backfilled = 1`.
- Writing **P1 `metric_values`** (`stage='raw'`, `source='chain'`, slot `as_of`) through 03's `put_metric_values` (B3/B4/B9), the daily `chain_trending_daily` rollup (P6 input for 10), and chain events (03 §3.9 names and severities, D1/B2).

**Out of scope**
- Historical backfill before the service started (17-data-migration-and-backfill), including OnFinality archive reads. 17 calls this spec's `TmcClient` and `upsert_*_from_analytics` helpers (J1).
- Pruning `chain_ticks`: 03's `chain_prune` policy inside `storage_retention_daily` at 03:00Z (E3). This spec has no prune job.
- HTTP routes (14) and alert delivery (14's `alert_dispatch`, D3). This spec only writes `events` rows.
- Identity merging (04), GitHub (06), owner analytics / P4 (11), the general scoring engine and normaliser library beyond the minimal P1 subset needed for the Phase-2 matrix (12), TaoStats-keyed collectors (dormant until a key exists).
- Any wallet, signing or `bittensor-wallet` dependency. The RPC collector is read-only and must import `bittensor` lazily inside the enabled path only.

---

## 2. Design

```
        ┌─────────────────────────────── app/collectors/chain_tmc.py : TmcClient (G2) ───────────────────────────────┐
        │  TmcClient.get(path, params) ─► 01 Http.request('tmc', …) ─► 01 ProviderLimiter['tmc']                    │
        │  8/min · burst 2 · 500/h · 90 k/month (F2); Retry-After/X-RateLimit-* handled by 01; BudgetExhausted       │
        │  propagates (G1); other HTTP failures come back as TmcResponse(ok=False)                                   │
        └──▲──────────────▲──────────────────▲───────────────────▲──────────────────▲────────────────────────────────┘
           │              │                  │                   │                  │ also called by 04 (/subnets/, coldkey
   SSE (provider   ┌──────┴─────┐    ┌───────┴──────┐    ┌───────┴───────┐   ┌──────┴────────┐  identities → chain_rest_snapshots),
   tmc_sse, not    │ chain_sse  │    │ tmc_hourly   │    │ tmc_daily     │   │ tmc_weekly    │  06 (github-stats), 11 (owner
   REST-metered)   │ (LongTask) │    │ cron :07     │    │ cron 02:10Z   │   │ cron 01:10Z   │  endpoints), 17 (backfill_tmc)
  /sse/subnets/    │ full→delta │    │ trending     │    │ constants     │   │ 1/7 netuids/d │
  table/ ─────────►│ silence    │    │ market-data  │    │ burn, lock    │   │ holders       │
                   │ >180 s ──► │    └──────┬───────┘    │ analytics 1W  │   │ candles 1h    │
                   │ /subnets/  │           │            │ pressure      │   │ analytics 1Y  │
                   │ table/ poll│           │            │ trending_daily│   │ validators    │
                   └─────┬──────┘           │            └──────┬────────┘   └──────┬────────┘
        ┌────────────────┼──────────────┐   │                   ▼                   ▼
        ▼                ▼              ▼   ▼            chain_daily         holders_snapshots
  chain_ticks      chain_hourly   chain_network_hourly   chain_hourly (gap)  price_bars '1h'
  (stride 5 +      (44 named      price_bars 5m/1h       chain_constants     chain_daily (1Y)
   flag changes)    cols, last/h) chain_trending         chain_pressure      chain_rest_snapshots
                                                         chain_trending_daily
                                                     │
                                 app/features/chain_features.py  (job chain_features, 00:05Z official + 06/12/18:05Z)
                                 gate_model.fit ─► gate_fits, gate_subnet_daily
                                 dereg_risk (ported) ─► dereg_hazard
                                 holders.concentration
                                 ──► 03 put_metric_values (P1 + aux, stage='raw', source='chain') + events
                                                     │
                     SUBTENSOR_RPC_ENABLED ──► app/collectors/chain_rpc.py ─► chain_constants(source='rpc_storage'),
                                               (EndpointSelector failover)    rpc_subnet_daily, rpc_metagraph_daily
                                                                              (MinerBurned replaces the fit)
```

**Data flow.** The SSE recorder holds an in-memory `TableState` (`dict[int, dict[str, Any]]`, keyed by the row field `subnet` = netuid; `delta.changes` keys arrive as JSON strings and are cast to `int`), replaced by every `full` event and patched by every `delta`. At the first event of every `CHAIN_TICKS_BLOCK_STRIDE`-block window, and on every flag change, it writes the 12 hot fields to `chain_ticks` (one transaction, §2.2). At every UTC hour boundary it flushes the last state of the closing hour to `chain_hourly` and rolls the hour's ticks into 5-minute `price_bars`. The REST jobs fetch history, holders and network-level series on their own cadence through `TmcClient`. `chain_features` runs at 00:05Z, after the 23:00Z hour row has been flushed at ≈ 00:00:1xZ. It reads the **snapshot for slot `as_of`**: the `chain_hourly` rows with `hour_ts = as_of − 3600`, i.e. the last state observed before `as_of` (§2.3). It fits the gate, computes hazard/holders/flow metrics and writes raw values with `as_of` = the slot (00:00Z official; 06/12/18:00Z provisional; B4). The scoring engine (12) normalises and aggregates. In Phase 2 a provisional P1-only matrix (CLI only, §8) applies the master §7.2 normalisers directly.

**Identity.** Every row written by this spec carries `occupant_key = f"{netuid}:{network_registered_at}"`, with `network_registered_at` taken from the same TMC row (table field `network_registered_at`). The key is deterministic, so a delta that changes `network_registered_at` for a netuid (or an `added` row) switches the recorder to the new key from that block on. The recorder logs `sse_reoccupation_seen {netuid, old_key, new_key, block_number}` and does nothing else: 04's `registry_occupants` (every 6 h) mints the occupant and emits `netuid_reoccupied`. 04's public API (04 §3.3) has no per-block hook, and a ≤ 6 h lag is harmless because a new occupant is `pre_launch` (no emission) until its start-call. Windowed features never mix occupants: every history query filters `occupant_key = ?`.

### 2.1 SSE recorder algorithm

`ChainSseTask(LongTask)` (01 §2.6; `id='chain_sse'`, `provider='tmc_sse'`, `liveness_max_age_sec=900`, `backoff_min_sec=TMC_SSE_BACKOFF_MIN_SEC` (1), `backoff_max_sec=TMC_SSE_BACKOFF_MAX_SEC` (60)). `run_once` below is one connection lifetime. 01's `run_forever` supervises it: backoff ×2 with ±20 % jitter, reset to the minimum after ≥ 60 s healthy, never raises, and lets `CancelledError` propagate.

```
async def run_once(self, stop):
    if not (TMC_ENABLED and TMC_SSE_ENABLED): self.state = 'disabled'; await wait(stop, 15); return
    if effective_pause(self, rt): self.state = 'paused'; await wait(stop, 15); return   # B12/16: id chain_sse, group chain, PAUSE_ALL (never 'tmc')
    if now_ts() < self.unauthorized_until: await wait(stop, 15); return                     # 401/403 back-off, fallback keeps running
    async with self.tmc.stream_sse(TMC_SSE_URL) as r:        # 01 Http.client('tmc_sse'): UA + Authorization=TMC_API_KEY if set;
                                                             # Accept: text/event-stream; timeout connect 10 s, read TMC_SSE_READ_TIMEOUT_SEC
        if r.status_code == 429: await wait(stop, max(retry_after(r), 60)); return
        if r.status_code in (401, 403):
            self.state = 'unauthorized'; self.unauthorized_until = now_ts() + TMC_SSE_UNAUTHORIZED_RETRY_SEC
            self.start_fallback('unauthorized'); return
        if r.status_code != 200: raise SseHttpError(r.status_code)
        self.state = 'connected'; got_full = False; watchdog = spawn(self._watchdog(r))       # cancelled when run_once exits
        async for event in parse_sse(r.aiter_lines()):       # 'data:' lines joined, blank line ends an event, ':' = keepalive comment
            if stop.is_set(): return
            try: msg = json.loads(event.data)
            except json.JSONDecodeError: self.bad_events += 1; continue                      # event dropped, stream kept
            if msg.get("type") == "full":
                self.state_tbl.replace(msg["data"]); got_full = True; self.on_full()          # stops fallback, may emit recovered
            elif msg.get("type") == "delta":
                if not got_full: continue                    # never patch a stale state (§7)
                self.state_tbl.patch(msg["changes"], msg.get("added", []))
            else: log_once("sse_unknown_type", msg.get("type")); continue
            self.beat()                                      # 01 LongTask.last_beat
            block = self.state_tbl.block_number()            # envelope block_number, else max(row block_number)
            self.state_tbl.mark_event(block, now_ts())
            if block is None or block <= self.last_block: continue       # dedup / stale upstream cache
            self._write_ticks(block)                         # stride + flag rule, §2.2
            self._maybe_flush_hour()                         # §2.2
            self.last_block = block
    # httpx errors and read timeouts propagate to run_forever (= disconnect; reconnects += 1)
```

- **Heartbeat** = `block_number` advancing. `_watchdog` ticks every 15 s. If `now − last_event_at > TMC_SSE_SILENCE_SEC` (180), **or** the block number has not advanced for 180 s while events keep arriving (stale upstream cache), it sets `state='silent'`, closes the stream (`run_once` returns, forcing a reconnect) and starts the **REST fallback**. The fallback is a child task of `ChainSseTask` that survives reconnects. It calls `TmcClient.fetch_subnets_table()` (`GET /subnets/table/`) every `TMC_TABLE_FALLBACK_SEC` (600) and processes the result as a synthetic `full` with `source='rest_table'`. It stops on the next real `full`.
  - Every fallback attempt sets `LongTask.last_attempt`. A successful poll also calls `beat()` (16 §2.5 amendment 5), so a TMC outage never trips the systemd watchdog.
  - A poll that raises `BudgetExhausted` is skipped (`fallback_state='budget'`, §3.4 `tmc_budget_degraded`). Polling continues at the interval (G1: no priority classes).
  - After `TMC_SSE_ALERT_AFTER_SEC` (1800) in fallback, the task emits `chain_sse_down` once per outage (03: warning, alert yes, delivered by 14's dispatcher, D3). The first real `full` after that emits `chain_sse_recovered` with `{since, reconnects, last_block, down_seconds}` (D2).
- **Anonymous SSE access (UNVERIFIED on the laptop's IP).** Research 02-keyless §0 received a `full` and 2 deltas anonymously in a 15-s test. Research 02-continuity §3 reads the OpenAPI as "SSE requires a key". On 401/403 the task goes `unauthorized`, starts the fallback at once and retries the stream every `TMC_SSE_UNAUTHORIZED_RETRY_SEC` (3600). Cost: ≤ 144 REST req/day (§6 worst case). A permanent 401 means operator decision C4 (TMC key) or life on 10-min table polls (`chain_ticks` then get one row per netuid per 10 min).
- **Block timestamp.** 03's verified 44-field list has no block-timestamp field, so `event_at = observed_at` (arrival; SSE lags the chain by ≤ 1 block plus cache). If a probe (§10 step 1) finds an envelope field `timestamp`/`block_timestamp` (UNVERIFIED), it is used instead. The chosen source is logged once per connection (`event_at_source ∈ {payload, arrival}`).
- **Block number.** Read from the event envelope (`msg["block_number"]`) if present, else from the rows (`max(row["block_number"])`; `block_number` is one of the 44 verified row fields). Only if neither exists on the first `full` does the task open the second SSE slot `/sse/blocks/` (`TMC_SSE_BLOCKS_STREAM=auto`) and pair the latest height with each table event. The process never holds more than `TMC_SSE_MAX_CONNECTIONS` (2, F2) SSE connections.
- **Schema drift.** On every `full`, compare the field set with 03's 44 named `chain_hourly` columns and the frozen list in `data/tmc_table_fields.json` (written on the first connection, §10). New or missing fields → `tmc_schema_drift` (info, digest; dedupe = sha of the diff) once per process lifetime per diff. Fields without a named column are kept verbatim in `chain_hourly.extras_json`.

### 2.2 Hourly flush, 5-minute rollup, tick stride

- `hour_ts = floor_hour(event_at)` (`app/util/time.py`, B1). When an event's hour exceeds `current_hour`, the state **as of the previous event** is written to `chain_hourly` for `current_hour`. The write is 03's precedence upsert (§3.3 ranks: `sse` = `rest_table` 4 > `tick_rollup` 3 > `analytics_1w` 2 > `analytics_1m` 1). A same-rank row is replaced only when `excluded.block_number >= stored.block_number`. A backfilled row therefore only fills a missing `(netuid, hour_ts)`. On graceful shutdown the current partial hour is written the same way; a restart in the same hour replaces it with a later tick. Column mapping, units and `immune_local`: §3.1.
- 5-minute `price_bars` (`period='5m'`, `source='sse_rollup'`, rank 5 in 03): at each hour flush, for every netuid and 5-min bucket `b = floor(event_at/300)*300` of the closing hour's `chain_ticks` (ordered by `block_number`): `open` = first `price`, `close` = last, `high`/`low` = max/min, `ts_last` = `event_at` of the close tick, `block_number` = its block, `moving_price` = its `subnet_moving_price`, `tao_in_pool = tao_liquidity/1e9` (τ), `alpha_in_pool` NULL (not a hot field), `volume` NULL (SSE volume semantics UNVERIFIED; the weekly TMC candles carry volume), `event_at = ts_last`, `backfilled = 0`. Buckets whose close is NULL or ≤ 0 are skipped (03 CHECK `close > 0`). The write is 03's precedence upsert: a same-rank row is replaced only when `excluded.ts_last > stored.ts_last` (J1).
- **Live hourly bars** (`period='1h'`, `source='sse_rollup'`; added 2026-09-24). Spec 13 needs a live 1h series, and TMC 1h candles only arrive weekly (`tmc_weekly`). So at every hour flush, right after the 5m rollup, `ChainSseTask._rollup_1h(hour_ts)` rolls the closing hour's stride-5 `chain_ticks` into one 1h bar per netuid (≈ 60 ticks). Per netuid, ordered by `block_number`: `open` = first `price`, `close` = last `price`, `high`/`low` = max/min `price`, `ts = hour_ts`, `ts_last` = `event_at` of the close tick, and `block_number`, `moving_price` (= `subnet_moving_price`), `tao_in_pool` (= `tao_liquidity/1e9`) and `occupant_key` come from that close tick. `alpha_in_pool` and `volume` are NULL, `event_at = ts_last`, `observed_at` = write time, `backfilled = 0`, and `run_id` is the flush's run id. Ticks whose `occupant_key` differs from the close tick's (a mid-hour reoccupation) are excluded. Ticks with `price` NULL or ≤ 0 are ignored, and a netuid with no valid tick gets no bar (03 CHECK `close > 0`). SQL idea (one statement per flush):
  ```sql
  WITH t AS (SELECT netuid, occupant_key, price, block_number, event_at, subnet_moving_price, tao_liquidity,
                    ROW_NUMBER() OVER (PARTITION BY netuid ORDER BY block_number)      AS rn_first,
                    ROW_NUMBER() OVER (PARTITION BY netuid ORDER BY block_number DESC) AS rn_last
             FROM chain_ticks WHERE event_at >= :h AND event_at < :h + 3600 AND price > 0)
  SELECT netuid, MAX(CASE WHEN rn_first=1 THEN price END) AS open, MAX(price) AS high, MIN(price) AS low,
         MAX(CASE WHEN rn_last=1 THEN price END) AS close, MAX(CASE WHEN rn_last=1 THEN event_at END) AS ts_last, …
  FROM t GROUP BY netuid;   -- occupant filter: see prose above; the result feeds 03's price_bars precedence upsert
  ```
  03's DDL already allows this row (`period IN ('5m','1h','1d')`, `source IN ('sse_rollup', …)`), so no schema change is needed. Precedence (03 §3.3): `sse_rollup` (rank 5) beats `tmc_candle` (3), so the weekly candles only fill hours with no live bar, e.g. SSE outages. A same-rank rewrite (such as the shutdown partial-hour flush followed by a restart in the same hour) replaces the bar only when `excluded.ts_last > stored.ts_last` (J1). **Catch-up:** the first flush after a start also rolls every hour of the last 24 h that has ticks but no 1h `sse_rollup` bar. 1h bars are kept forever (03 retention).
- **Tick stride.** A `chain_ticks` row for `(netuid, block)` is written when:
  - the event opens a new stride window (`block // CHAIN_TICKS_BLOCK_STRIDE > last_tick_block // CHAIN_TICKS_BLOCK_STRIDE`, which is robust to skipped block numbers; default stride **5** ≈ 1/min, F2 and master §6), **or**
  - any of `emission_enabled, immune, deregistration_risk, network_registered_at, is_active` changed for that netuid in this delta (always written, off-stride), **or**
  - the netuid appears in `added`.

  Storage at stride 5 ≈ 129 × 1,440 ≈ 186 k rows/day. With `CHAIN_TICKS_RETENTION_DAYS=30` (F2) the table stays ≈ 0.84 GB (03 §6; stride 1 would be ≈ 4.2 GB). Pruning is **not** a job of this spec: 03's `chain_prune` policy in `storage_retention_daily` (03:00Z, E3) first guarantees the tick→hourly and tick→5m rollups, then deletes in batches.

### 2.3 v440 gate reproduction and implied miner burn

**Snapshot for slot `as_of`** (`snapshot_rows(db, as_of)`, used by every P1 metric): per netuid, the `chain_hourly` row with `hour_ts = as_of − 3600`, i.e. the last state observed before `as_of`. This row exists at 00:05Z because the 23:00Z hour is flushed at ≈ 00:00:1xZ. Rows from `sse` or `rest_table` are *live*; `tick_rollup` rows are live too but carry hot columns only, so metrics needing other fields are `failed` (`field_null`). If that row is missing, or is analytics-only, the snapshot falls back in order:
1. the newest live row with `as_of − 3·3600 ≤ hour_ts < as_of − 3600`, with the lag recorded as `value_json.snapshot.stale_h`;
2. the `analytics_1w`/`analytics_1m` row (power-loss repair). It has no `price`/`subnet_moving_price`, so every metric that needs them is `failed` with reason `snapshot_backfilled` (§4);
3. **historical tier**, only when no `chain_hourly` row exists at all, e.g. 17's `replay_history --as-of D` (17 §2.11). The snapshot is assembled from:
   - the 1h `price_bars` bar with `ts = as_of − 3600` (any source): `close → price`, `moving_price → subnet_moving_price`, `tao_in_pool × 1e9 → tao_liquidity` (rao);
   - the `chain_daily` row with the greatest `day_ts ≤ as_of`: `chain_buys → excess_tao`;
   - `emission` from 13's `v_quant_chain_daily` (`chain_daily` ∪ 17's daily RPC share, migration 0005): the row of the bar's occupant with the greatest `day` whose `ts ≤ as_of`. *(Amended 2026-10-22, 20 N2 item 2: previously `chain_daily.emission_percent`, which has no rows for occupants that died and whole-percent-truncated values before 2025-11-17; `ts ≤ as_of` keeps the RPC sample of day `as_of` (taken ≈ 00:59Z) out of slot `as_of`.)*
   - `block_number` from the bar, else the nearest 17 `block_times` anchor ≤ `as_of` (`ts_ms ≤ as_of·1000`; the built code read a non-existent `ts` column, fixed 2026-10-22).

   `is_active` is inferred as `moving_price > 0`. `emission_enabled` is unknown, so `emission_enabled_flag` is `failed` (`snapshot_historical`). Disabled subnets then enter E as if fully burned; this leaves θ and every other subnet's prediction unchanged, because adjusted shares are renormalised. `exit_capacity`, `chain_buys`, `holder_dispersion` and `participation` are `failed` (`snapshot_historical`), matching 17 §2.11. Rows carry `value_json.snapshot.source = 'historical'`;
4. otherwise `no_snapshot`.

`value_json.snapshot = {hour_ts, source, event_at}` records what was used.

Inputs: per subnet `mp_i = subnet_moving_price`, `e_i = emission / 100` (realised post-gate share), flags. **Eligible set E**: `netuid ≠ 0`, `emission_enabled = 1`, `is_active = 1`, `mp_i` not null and `> 0`. On 2026-09-22 this was 122 of 129: netuid 0, the 4 emission-disabled subnets and the 3 not-started subnets are excluded, with SN108 in both groups. Realised shares are renormalised over E: `r_i = e_i / Σ_E e_j`. If `|E| < 2`, no fit is run: no `gate_fits` row, and `gate_margin`/`miner_burn_ok` are `failed` with reason `theta_undefined`.

```
def fit_gate(mp: dict[int,float], realised: dict[int,float], *, bar_rank=GATE_BAR_RANK, h=GATE_EXPONENT,
             burn_override: dict[int,float] | None = None, max_iter=GATE_FIT_MAX_ITER, tol=GATE_FIT_TOL,
             min_pred_share=GATE_MIN_PRED_SHARE) -> GateFit:            # caller guarantees len(mp) >= 2, all mp > 0
    demand = {i: mp[i] / sum(mp.values()) for i in mp}                      # demand share from moving price
    b = {i: 1.0 for i in mp}                                                # b_i = 1 − miner_burned_i
    if burn_override: b = {i: clip(1 − burn_override.get(i, 0.0), 0, 1) for i in mp}   # RPC MinerBurned path
    it, delta = 0, 0.0
    def predict(b):
        adj = normalise({i: demand[i] * b[i] for i in mp})                  # burn-adjusted share
        pos = sorted((v for v in adj.values() if v > 0), reverse=True)
        theta = pos[bar_rank − 1] if len(pos) >= bar_rank else pos[-1]      # 32nd-highest positive adjusted share
        gate = {i: 0.0 if adj[i] <= 0 else 1 / (1 + (theta / adj[i]) ** h) for i in mp}   # Hill gate
        pred = normalise({i: adj[i] * gate[i] for i in mp})                 # predicted post-gate share
        return adj, theta, gate, pred
    for it in range(max_iter):
        adj, theta, gate, pred = predict(b)
        if burn_override: break
        new_b = {i: (1.0 if pred[i] <= min_pred_share else clip(b[i] * realised[i] / pred[i], 0.0, 1.0)) for i in mp}
        delta = max(abs(new_b[i] − b[i]) for i in mp); b = new_b
        if delta < tol: break
    adj, theta, gate, pred = predict(b)
    resid_pp = 100 * sum(abs(pred[i] − realised[i]) for i in mp)           # research: 3.1 pp on 2026-09-22
    # counterfactual "no own burn" share, others' fitted burns held fixed:
    pgs = {i: predict({**b, i: 1.0})[3][i] for i in mp}                     # predicted_gated_share_i
    imb = {i: (None if pgs[i] < min_pred_share else clip(1 − realised[i] / pgs[i], 0, 1)) for i in mp}
    return GateFit(theta=theta, b=b, adj=adj, gate=gate, pred=pred, pgs=pgs, implied_burn=imb, residual_pp=resid_pp,
                   iterations=it + 1, converged=bool(burn_override) or delta < tol,
                   burn_source='rpc' if burn_override else 'fit')
```

`theta` is well defined because `pos` is non-empty whenever `|E| ≥ 2` and every `mp_i > 0`. Every `adj_i > 0` unless `b_i` fitted to exactly 0.

- The multiplicative update converges to a fixed point where `pred_i = realised_i` for every subnet with `b_i < 1`. Subnets that receive **more** than a zero-burn prediction cannot be fitted (`b_i` clips at 1) and make up the residual. A residual `> GATE_FIT_TOLERANCE_PP` (6.0; this is the model-quality tolerance, not the convergence tolerance `GATE_FIT_TOL`) marks the fit `degraded`: the row is still written, `miner_burn_ok` and `implied_miner_burn` are `failed` (reason `fit_degraded`) for that `as_of`, and `gate_fit_degraded` is emitted (info, digest). `gate_margin` is still written, because θ is dominated by the top-32 subnets.
- **`gate_margin` raw** = `adj_i / theta` (burn-adjusted share ÷ bar), master normaliser `zclip(log2(ratio), −2, +1)`. **`implied_miner_burn_i`** as above; **`miner_burn_ok` raw** = `1 − implied_miner_burn_i`. Deep-tail subnets with `pgs_i < GATE_MIN_PRED_SHARE` (1e-4) have an unidentifiable burn → `failed` with reason `burn_unidentifiable` (not a verifiable absence).
- **RPC path.** When the RPC collector is enabled and `rpc_subnet_daily` has `MinerBurned` for the same `as_of`, the job runs `fit_gate` twice. The plain fit gives diagnostics (`gate_subnet_daily.fitted_burn = 1 − b_i`). The `burn_override` fit supplies the metrics: `implied_miner_burn_i = miner_burned_i` as read, `value_json.src='rpc'`, and `gate_fits.burn_source='rpc'`. `gate_subnet_daily.rpc_miner_burned` keeps the reading. TaoStats, if keyed later, plugs into the same override.
- The fit is pure CPU (≈ 122 counterfactual `predict` calls, < 50 ms). When `GATE_HOURLY_ENABLED=true` (default false) it also runs **hourly** into the same tables with `as_of = hour_ts`, as diagnostics only. The slot rows are the ones metrics use. θ is recomputed on-chain every 360 blocks, so a snapshot is a sample, not an average; this is documented in the metric description.
- Open point recorded in §11: whether θ is literally the 32nd-highest share or derives from `EmissionBarQuantile = 0.61`; `GATE_BAR_RANK` is configurable and the RPC collector stores the live quantile/exponent for a later check.

### 2.4 Immunity

`immune_local = (block_number − network_registered_at) < NETWORK_IMMUNITY_PERIOD_BLOCKS` (864,000 = 120 d, verified on-chain 2026-09-22; the docs' 1,296,000 is the stale runtime default and is what `/general/constants/` returns — never read it from there). Computed at write time for every `chain_hourly` row and at feature time for metrics. **TMC quirk:** not-yet-started subnets (`is_active = False`, `subnet_moving_price = null`, `price == price_after`) show `immune = False`; `immune_local` is authoritative everywhere in this spec; the TMC flag is stored as delivered. A started subnet (`is_active = True`) whose `immune ≠ immune_local` emits `immunity_mismatch` (info) at most once per occupant per day; if the RPC collector reads a live `NetworkImmunityPeriod` different from the config value, `chain_constant_drift` (warning) fires once and the operator updates the config (re-verify quarterly per master §5).

### 2.5 Dereg safety and dereg hazard

- **Pruning pool P** (the chain's cull candidates): occupants with `netuid ≠ 0`, `immune_local = False`, `subnet_moving_price` not null. Rank from the bottom `r_i ∈ {1..|P|}` (1 = lowest moving price = next pruned; ties → older `network_registered_at` ranks lower, matching "ties go to the oldest").
- **`dereg_safety`** (master §7.3, custom normaliser = identity): `1.0` if `immune_local`; else `0.0` if `r_i ≤ 3`; else `(r_i − 1) / (|P| − 1)`. The division is only reached when `|P| ≥ 4`. Not-started subnets have so far all been young, so they get 1.0, but they are `pre_launch` and carry no composite (§4).
- **`dereg_hazard`** (auxiliary, weight 0, events + quant): `dereg_hazard_score(emissions={j: mp_j for j in P}, netuid, emission_disabled=(not emission_enabled), emission_hist=…, price_hist=…, depth_hist=…, cfg=…)` from the verbatim port of `E:\SN_Trader\SubnetTrader\app\strategy\dereg_risk.py` (config lines 31-55, score lines 127-174; file 194 lines).
  - Changes are to the old call site, not to the module. The weight map is the **non-immune moving-price** set (the module docstring, lines 19-24, already says to pass `moving_price`). Immune occupants are excluded from the map and get `hazard = 0.0`, unless `emission_disabled`, which short-circuits to 1.0 inside the function (lines 155-156).
  - The function returns 0.0 when proximity is None (pool < 3, or the netuid not in the pool, lines 158-160). This collector writes `failed` (reason `hazard_unassessable`) for a non-immune occupant in that case instead.
  - History windows: `DEREG_HIST_DAYS=30` daily values, oldest first, occupant-filtered. `emission_hist` = `chain_daily.emission_percent`. `depth_hist` = `chain_daily.subnet_tao`. `price_hist` = the close of the 1h `price_bars` bar with `ts = d − 3600`, i.e. the last hourly close before each day `d`'s 00:00Z, with fallback `chain_hourly.price` at `hour_ts = d − 3600`.
  - `DeregRiskConfig` fields map 1:1 to the `DEREG_*` keys, except `reset_drop_ratio`, which keeps its default because `is_fresh_registration` is not called (occupant keying handles slot resets).
  - `dereg_proximity` (03: **critical**, B2; dedupe `occ|{d7}`) is emitted when `dereg_hazard ≥ DEREG_HIGH_RISK_AT` and not immune. Routing is D4: digest-only, because the immediate pages are 12's `red_flag_raised(dereg_imminent)` and 14's watchlist `dereg_watch`. This spec sends no Telegram message.
- **`dereg_imminent` red-flag input**: metric `dereg_risk_flag = 1.0` when `deregistration_risk = True` **or** `r_i ≤ 3` in P; else 0.0. Spec 12 turns it into the ×0.60 flag.

### 2.6 Holder concentration

Weekly per netuid (the `tmc_weekly` slice): `GET /accounts/subnet-holders/{netuid}/?limit=TMC_HOLDERS_PAGE_LIMIT` (100). The maximum page size is UNVERIFIED: if the response returns fewer rows than requested while `count` is larger, fetch `offset=` pages up to `TMC_HOLDERS_MAX_PAGES=2`. Rows: `rank, coldkey, percentage, total_alpha, realized_pnl, unrealized_pnl`; envelope `count` (SN51 count = 6,559 on 2026-09-22). The denominator of `percentage` is UNVERIFIED (all holders' alpha is assumed). Compute over the fetched top-N (N ≤ 200):
- `top10_share = Σ percentage[rank ≤ 10] / 100`, `top1_share`, `holders_count = count`;
- `hhi_topn = Σ (percentage_i / 100)²` (a lower bound on the true HHI because the tail is truncated — stored as such);
- `gini_topn = gini_coefficient([total_alpha_i])` (ported `E:\SN_Trader\SubnetTrader\app\utils\math.py:160-177` into `app/util/mathutil.py`, pure; a Gini of the top-N only).
- **`holder_dispersion` raw** = `1 − top10_share` (master normaliser `zclip(x, 0.3, 0.9)`). Each slot's row uses the newest `holders_snapshots` row with `observed_at ≤` run time and age ≤ `TMC_HOLDERS_MAX_AGE_DAYS` (14). `value_json.snapshot_observed_at` records it; the weekly value is restated daily, and freshness is judged on the snapshot age, not the row. Whether to exclude the owner coldkey or protocol-owned alpha from the top-10 is open (§11); v1 does **not** exclude.

### 2.7 Exit capacity, chain buys, liquidity, flow

- **`exit_capacity` raw** = `price_after / price` from the slot snapshot (both τ per α; `price_after = T·A/(A+X)²`, `X = total_subnet_stake − total_locked_alpha`, verified to 0.0000 median relative error over 125 subnets). `price ≤ 0` or `price_after` null → `failed`. Not-started subnets (`price == price_after`, volume 0) → `not_applicable` (pre-launch, no composite).
- **`chain_buys` raw** = `(excess_tao / 1e9 × 7200) / market_cap_tao`. `excess_tao` is in rao/block; ×7200 blocks/day gives τ/day of protocol alpha buys. `market_cap_tao` = the table field **`marketcap`** (τ = price × circulating α, a verified name per 03; SN64 ≈ 384,841). If that is null, use `price × circulating_supply / 1e9` (`circulating_supply` is rao-scaled α). `excess_tao = 0` is a **real zero** (the chain injects everything as liquidity) → `0.0`, status `ok`, ranked.
- **`liquidity_depth` raw** = `tao_liquidity / 1e9` (τ). 03 records `tao_liquidity` as rao (SN64 199,012,885,911,739 ≈ 199 k τ). The units probe (§10 step 1) only confirms the magnitude; a contradiction makes every row `failed` with reason `units_unresolved` until `data/tmc_table_fields.json` is fixed. 12 applies `rankg` to raw τ, because the log in master "rankg(log)" does not change ranks.
- **`net_flow_30d` raw** = `Σ_{window} (total_tao_buy_24h − total_tao_sell_24h) / depth`.
  - The window is the occupant's `chain_daily` rows with `as_of − W·86400 < day_ts ≤ as_of` and `observed_at ≤` run time, where `W = min(30, occupant_age_days)`. At the 00:05Z run the newest row is usually yesterday's, because the 1W pull runs at 02:10Z; `value_json.window_end_day` records it. Each row's `*_24h` fields are trailing-24 h totals at its 00:00Z point, so the sum covers the window without overlap.
  - `depth` = the snapshot's `tao_liquidity`, falling back to the newest window row's `subnet_tao`. Both are rao (`subnet_tao`: SN51 153,227,189,355,104 ≈ 153 k τ).
  - The unit of `total_tao_buy/sell_24h` is UNVERIFIED. The units probe compares them with `subnet_tao` and records `units.analytics.total_tao_buy_24h ∈ {rao, tau}`; τ values are ×1e9 before dividing.
  - At least 24 rows are required when `W = 30`, else ≥ max(7, ⌈0.8·W⌉); otherwise `failed` with reason `insufficient_history`.
- **`emission_share` raw** = `emission` (%, post-gate, post-burn). `emission = 0` with `emission_enabled = 1` is a real zero (status `ok`, ranked).
- **`participation` raw** = `(active_validators + active_miners) / max_uids` **only** (C7).
  - Source order: (1) `rpc_metagraph_daily` when `SUBTENSOR_METAGRAPH_ENABLED`. There, active validators = neurons with `active ∧ validator_permit`, active miners = `active ∧ ¬validator_permit`, and `max_uids` = the `max_allowed_uids` hyperparameter (fallback `n_uids`). (2) Keys in the `latest_snapshot` of 04's `/subnets/` snapshot (`chain_rest_snapshots`, kind `subnets`, read not refetched, G2): candidates `active_validators, active_miners, max_allowed_uids, num_uids, subnetwork_n`, presence UNVERIFIED. (3) `/subnets/neurons/{id}/` in the weekly slice when `TMC_NEURONS_ENABLED=true` (≈ 18 req/day, default off; field names UNVERIFIED). (4) Otherwise `failed`, reason `no_metagraph_source`.
  - The value is written with `value_json.src ∈ {rpc, tmc_snapshot, tmc_neurons}`.
  - Validator vtrust dispersion (`pstdev(validator_trust over permit holders)`) is the **evidence key `vtrust_dispersion`** (weight 0, C7), RPC only.

### 2.8 Regime input series

`chain_network_hourly` is written at every hour flush (`hour_ts` = the closing hour, `event_at` = its last tick, `source='sse'` or `'rest_table'`, `run_id`). Columns:
- `sum_subnet_prices = Σ price` over active rows (`netuid ≠ 0`, `is_active = 1`), with `n_active` = the count;
- `total_subnet_tao = Σ tao_liquidity / 1e9` (τ);
- `block_number`;
- `tao_usd` from the latest successful `tmc_hourly` market read ≤ 3 h old.

TAO/USD source order (`tao_usd_source`):
1. `market_data`: `GET /market/market-data/`, CoinMarketCap-sourced. Its shape is UNVERIFIED; the parser tries the keys `price_usd`, `price`, `quote.USD.price`, `tao_price_usd`.
2. `analytics`: the newest `tao_price_usd` from any `chain_daily`/`chain_hourly` analytics row observed in the last 3 h.
3. Otherwise NULL, never forward-filled.

`app/quant/regime.py` (spec 12) reads this table and `chain_hourly`. The 24-h realised vol/trend classifier is not part of this spec.

### 2.9 Field-semantics glossary (TMC table / snapshot / analytics)

| Field | Meaning (verified unless marked) | Use |
|---|---|---|
| `emission` (%) | subnet's share of the 0.5 τ/block TAO emission = `(subnet_tao_in_emission + subnet_excess_tao) / Σ`; post-gate, post-burn (Σ = 100.0) | `emission_share`, gate fit realised share |
| `subnet_moving_price` | on-chain `SubnetMovingPrice` EMA (τ/α, spot capped at 1.0); emission driver and pruning criterion | demand share, dereg rank |
| `price`, `price_after` | spot AMM price; price after selling all unlocked stake (`T·A/(A+X)²`) | `exit_capacity` |
| `excess_tao` (rao/block) | `SubnetExcessTao`: TAO the chain cannot inject (alpha injection capped at `root_prop × alpha_emission`), used to buy alpha = "chain buys"; same series as analytics `chain_buys` and snapshot `subnet_excess_tao` | `chain_buys` |
| `root_prop` (%) | `root_tao×0.18 / (root_tao×0.18 + alpha_issuance)`; injection cap and validator-dividend split; 13.5–90.5, median 16 | glossary / 11 |
| `adr` | alpha_out / alpha_in (docs); < 1 accumulation, > 1 distribution; window UNVERIFIED | stored, unused in ts-1.0 |
| `subnet_ema_tao_flow` | on-chain EMA of net TAO stake flow (= TaoStats `ema_tao_flow`); emission driver Nov-2025→22-Jun-2026 only; now a flow/sentiment feature | stored hot; Velocity later |
| `owner_personal_locked_alpha` = `owner_personal_perpetual_alpha` + `owner_personal_decaying_alpha` | owner conviction locks, rao-scaled α (÷1e9); the identity holds on every row (61/128 owners lock). `total_locked_alpha` covers all lockers | 11 (`owner_commitment`); aux `owner_locked_share` |
| `total_subnet_stake`, `tao_liquidity`, `alpha_liquidity`, `circulating_supply` | α staked (out), pool reserves, circulating α; `tao_liquidity` is rao (÷1e9 = τ), the α fields are rao-scaled (÷1e9) per 03 | depth, `price_after` check, market-cap fallback |
| `marketcap` (τ), `marketcap_rank`, `fdv` (rao) | market cap = price × circulating α (03; the field is `marketcap`, not `market_cap`) | `chain_buys` denominator |
| `subnet_owner` | owner coldkey ss58 (root: `0x00…`) | 04, 11 (read from `chain_hourly`) |
| `immune`, `deregistration_risk`, `emission_enabled`, `is_active` | TMC badges; `immune` is wrong for not-started subnets; `deregistration_risk` = bottom-3 non-immune by moving price (verified 2026-09-22) | §2.4, §2.5 |
| `network_registered_at` | registration block (= TaoStats `registered_at_block`) | occupant_key |
| `commits_7d/30d/90d` | identity repo only; null for 18 subnets | stored; 06 cross-check |
| **IGNORE:** `is_subsidized`, `subsidized_breakpoint`, `chain_buys_per_block` | legacy flow-era fields; all `is_subsidized = False`; uncorrelated with `excess_tao`; semantics UNVERIFIED | stored in their named `chain_hourly` columns (03), never read |
| analytics `emission_percent`, `chain_buys`, `subnet_tao` (rao), `total_tao_buy/sell(_24h)` (unit UNVERIFIED, §2.7), `volume_24h`, `tx_count_24h`, `neuron_registrations_24h`, `rao_recycled_24h`, `total_locked`, `total_decaying`, `owner_personal_locked_alpha`, `owner_personal_decaying_alpha`, `tao_price_usd/eur`, `block_number`, `ts` | hourly (span ≤ 1M, 720 rows) / daily points at 00:00Z (1Y, 366 rows); **no price or moving price** and **no perpetual field** (perpetual = locked − decaying); `block_number` matches the chain ± 1 min | `chain_daily`, gap repair (mapping §3.1) |
| candle `block_number` | 8–90 min off chain time — **never join on it**; join on `timestamp` | `price_bars` |

---

## 3. Data contract

Every timestamp column is INTEGER unix seconds UTC (B1). Helpers `now_ts`, `floor_hour`, `floor_day`, `to_iso`, `parse_ts` come from `app/util/time.py`. ISO-8601 `…Z` strings appear only at the HTTP boundary, i.e. in TMC query parameters `FROM`/`TO`/`from_timestamp`/`to_timestamp` and in parsed responses. Raw tables store values **as delivered** by TMC, with no unit conversion; conversions live in `chain_features`, with units per 03's column comments, confirmed by the units probe.

**03 owns every DDL statement (B7).** This section lists only what 05 writes into 03's tables: key, writer, write rule and column mapping. Every row carries `observed_at` (fetch/write time) and `run_id`, which is 01's `RunContext.run_id`, or `sse:<connected_at>` for the recorder.

### 3.1 Raw tables written (03 §3.3)

| table (03 class) | key | writer | write rule | columns written by 05 |
|---|---|---|---|---|
| `chain_ticks` (append) | `(netuid, block_number)` | `chain_sse` | `INSERT OR IGNORE` | `occupant_key, event_at, observed_at, source ∈ {sse, rest_table}, run_id` + the 12 hot fields as delivered: `price, subnet_moving_price, emission, emission_enabled, immune, deregistration_risk, excess_tao, subnet_ema_tao_flow, owner_personal_locked_alpha, total_locked_alpha, total_subnet_stake, tao_liquidity` |
| `chain_hourly` (precedence) | `(netuid, hour_ts)` | `chain_sse` (live); `tmc_daily` (gap repair); 17 via helper | 03 precedence upsert (§2.2) | **live:** all 44 TMC fields into 03's same-name columns (TMC `subnet` → `netuid`), `immune_local` (§2.4), `extras_json` (delivered fields without a named column, verbatim), `backfilled=0`. **Analytics:** mapping below, `backfilled=1` |
| `chain_daily` (precedence) | `(netuid, day_ts)` | `tmc_daily` (`analytics_1w`), `tmc_weekly` (`analytics_1y`), 17 via helper | `upsert_daily_from_analytics` (J1, §8) | every analytics field into its same-name column (`subnet_tao … tao_price_eur`, `daily_inflation`), `block_number`, `event_at = ts`, `extras_json`, `backfilled` (0 live; 1 for 17 and catch-up) |
| `price_bars` (precedence) | `(netuid, period, ts)` | `chain_sse` (`5m` and `1h`, `sse_rollup`, §2.2); `tmc_weekly` (`1h`, `tmc_candle`, gap fill only) | 03 precedence upsert; same rank replaced iff `excluded.ts_last > stored.ts_last` (J1) | 5m and live 1h: §2.2. 1h candle: `ts` = bucket start, `open, high, low, close` (close ≤ 0 skipped), `volume, start_volume, tao_price_usd`, `tmc_block_number` (never joined on), `ts_last` NULL, `block_number` NULL, `event_at = ts`, `occupant_key` = 04's occupant of the netuid at `ts` (by `occupants.registered_ts`, J1; NULL if unknown), `backfilled=0` |
| `holders_snapshots` (append) | `(netuid, observed_at)` | `tmc_weekly` | INSERT | `occupant_key, run_id, source='tmc_holders', holders_count, n_fetched, top1_share, top10_share, hhi_topn, gini_topn, rows_json` (`[{rank, coldkey, percentage, total_alpha}]`) |
| `chain_trending` (append) | `(observed_at, netuid)` | `tmc_hourly` | `INSERT OR IGNORE`, only for a *successful* pull (HTTP 200 with ≥ 1 entry) | `netuid = entity_id` (assumed to be the netuid, UNVERIFIED; unknown ids are dropped and counted), `occupant_key` (current occupant), `rank` (1-based position), `views, visitors, percentage_change`, `source='tmc_trending'`, `run_id` |
| `chain_trending_daily` (rollup) | `(day_ts, occupant_key)` | `tmc_daily` for the previous UTC day, plus any of the last 7 days still missing | `INSERT OR IGNORE` | Built from that day's **last successful** `chain_trending` pull (the distinct `observed_at` values are the successful pulls). `views` = the listed value, or **0** for every active/pre_launch occupant absent from the pull (verifiable absence). **No row** when no pull that day succeeded (10 reads NULL). Also `visitors, n_pulls_ok, observed_at` (of the pull used), `run_id`. This replaces 10's `chain_daily.tmc_trending_views` (03 Appendix A) |
| `chain_pressure` (append) | `(day_ts, side, netuid, observed_at)` | `tmc_daily` | INSERT | per returned bucket: `day_ts = floor_day(timestamp_ms/1000)`, `side`, `netuid` (−1 = network-wide), `value_rao, ema_price`, `event_at`, `source='tmc_pressure'`, `run_id`. Query: `FROM`/`TO` = ISO-8601 Z bounds of the last 7 closed UTC days, `side=buy\|sell` (**required**), `bucket=1d` (research saw `bucket=hour` without `side` rejected as "Invalid pressure parameters"), `subnet=` only for the watchlist option |
| `chain_network_hourly` (append) | `hour_ts` | `chain_sse` | `INSERT OR IGNORE` | §2.8 |
| `chain_constants` (append) | `(name, source, observed_at)` | `tmc_daily` (`tmc_constants`: `GET /general/constants/?type=subtensor_module`, runtime *defaults* only); `chain_rpc_hourly` (`rpc_storage`, with `endpoint`, `block_number`); `chain_features` (`config`: `NETWORK_IMMUNITY_PERIOD_BLOCKS`, `GATE_*`, once per official run) | INSERT | `value` as a decimal/JSON string. 05's former `rpc_constants` table is merged here (03) |
| `chain_rest_snapshots` (append, 90 d) | `(endpoint, params, observed_at)` | helper `write_rest_snapshot` (§8), called by 04, 05 and 17 | INSERT | one row per logical fetch (paginated pages merged into one payload): `endpoint` = path template, `params` = canonical sorted query without `offset`, `http_status`, `payload_json`, `event_at` NULL, `source='tmc'`, `run_id` |

**Snapshot kinds.** Ledger G2 names snapshots by `kind`; 03's table keys them by `endpoint`. `REST_SNAPSHOT_KINDS` (in `chain_tmc.py`) maps one to the other, and `latest_rest_snapshot(db, kind)` reads by `endpoint`:

| kind | endpoint (params) | writer, cadence | readers |
|---|---|---|---|
| `subnets` | `/subnets/` (`limit=<TMC_SUBNETS_PAGE_LIMIT>`) | 04 `registry_occupants`, every 6 h (G2) | 04, 05 (§2.7 participation fallback), 11 |
| `subnets_table` | `/subnets/table/` | 04, only when `/subnets/` lacks `subnet_owner` or fails (G2) | 04 |
| `coldkey_identities` | `/accounts/coldkey-identities/` | 04 `registry_identity`, 02:40Z (G2) | 04, 11 |
| `burn`, `network_lock_cost` | `/subnets/burn/`, `/subnets/network-lock-cost/` | 05 `tmc_daily` | stored for later use |
| `validators` | `/subnets/validators/` | 05 `tmc_weekly`, on Mondays (`weekday == 0`) | stored for later use |
| `market_data` | `/market/market-data/` | 05 `tmc_hourly` | 05 §2.8 |
| `neurons` | `/subnets/neurons/<n>/` (literal netuid) | 05 `tmc_weekly` when `TMC_NEURONS_ENABLED` | 05 §2.7 |
| `analytics_subnet` (17's own) | `/analytics/subnet/{n}/`, stored with the netuid substituted literally through `path_args={'n': netuid}` (e.g. `endpoint='/analytics/subnet/64/'`); `params='span=1Y'` or `params='span=1M'` | 17 `backfill_tmc` (`analytics1y`, `analytics1m`) | 17 (restamp and its analytics snapshots) |

The SSE fallback's `/subnets/table/` polls are processed in memory and not stored (≈ 169 KB each).

**Analytics → `chain_hourly` mapping** (gap repair through `upsert_hourly_from_analytics`, `source ∈ {analytics_1w, analytics_1m}`, `backfilled=1`):
- `hour_ts = floor_hour(ts)`, `ts → event_at`, `block_number`;
- renamed: `emission_percent → emission`; `subnet_tao → tao_liquidity` (both are pool TAO in rao); `chain_buys → excess_tao` (the same series, research §2); `total_locked → total_locked_alpha` (equivalence UNVERIFIED; the units probe compares one hour);
- same name: `owner_personal_locked_alpha`, `owner_personal_decaying_alpha`, `total_tao_buy_24h`, `total_tao_sell_24h`, `volume_24h`, `tx_count_24h`, `neuron_registrations_24h`, `rao_recycled_24h`, `tao_price_usd`;
- everything else is NULL, including `price`, `subnet_moving_price`, the flags and `owner_personal_perpetual_alpha`. Readers derive perpetual as locked − decaying (11 D3). The units probe records whether the SSE and analytics owner-lock units match (`units.owner_lock_sse_eq_analytics`).

**Analytics → `chain_daily` day alignment.** `span=1Y` points are daily at 00:00Z (`day_ts = floor_day(ts)`). A 1W-derived row takes, for each day, the hourly point with `day_ts ≤ ts < day_ts + 3600`; there is no row for that day if no such point exists. Both sources therefore describe the same 00:00Z instant.

### 3.2 Derived and optional-RPC tables written (03 §3.3)

| table (03 class) | key | writer | columns written by 05 |
|---|---|---|---|
| `gate_fits` (append) | `(as_of, model_version)` | `chain_features` | `block_number` (snapshot), `n_eligible, bar_rank, exponent, theta, residual_pp, iterations, converged, degraded, burn_source ∈ {fit, rpc}, observed_at, run_id`; `INSERT OR IGNORE`, so a catch-up never rewrites a fit |
| `gate_subnet_daily` (rollup) | `(as_of, model_version, netuid)` | `chain_features` | `occupant_key, eligible, demand_share, burn_adjusted_share, gate, predicted_share, realised_share, predicted_gated_share, implied_miner_burn, fitted_burn` (= 1 − `b_i` of the plain fit), `rpc_miner_burned, observed_at, run_id`. Non-eligible netuids get `eligible=0` and NULL shares. `INSERT OR IGNORE` |
| `rpc_subnet_daily` (append) | `(netuid, as_of)` | `chain_rpc_hourly`: the first successful pass in `[as_of, as_of + 3600)` for each slot; also 17's `--emission-daily` backfill (day rows, `source` = the archive endpoint, `emission_share` + `moving_price` only, migration 0005) | `occupant_key, block_number, observed_at, event_at, source='rpc_storage', run_id, miner_burned` (U96F32 → float, UNVERIFIED type), `subnet_owner_cut` (global `SubnetOwnerCut`/65535, replicated), `emission_enabled, moving_price, endpoint` |
| `rpc_metagraph_daily` (append) | `(netuid, as_of)` | `chain_rpc_metagraph` | `occupant_key, block_number, observed_at, event_at, source='rpc_metagraph', run_id, n_uids, max_uids, active_validators, active_miners, permit_count, vtrust_mean, vtrust_pstdev, incentive_to_owner_share, endpoint` |

### 3.3 `metric_values` rows produced (raw values; normalisation in 12)

Row contract (B3, B4, B9; one `await put_metric_values(db, rows)` call per slot, 03 `app/storage/pit.py`; `upsert_metric_values` is only an alias):

| field | value |
|---|---|
| `as_of` | the slot (B4): 00:00Z official (00:05Z run) or 06:00/12:00/18:00Z provisional (06:05/12:05/18:05Z runs). A row is provisional iff `as_of % 86400 != 0`; there is no `is_provisional` column and no separate `spec_version` |
| `stage`, `spec_version` | `'raw'`, `'ts-1.0'` (the metric-definition version) |
| `source` | always `'chain'` (B9), including RPC-derived values; provenance is in `value_json.src ∈ {tmc, rpc, tmc_snapshot, tmc_neurons}` |
| `value` | the raw value below; NULL when the status is not `ok` |
| `value_json` | `{status, reason?, src, snapshot: {hour_ts, source, event_at}, inputs…}`. `status ∈ {ok, failed, not_applicable}` (B3). 05 never writes `absent`: every chain zero is a *measured* value, status `ok`, which 12 normalises (a real `net_flow_30d = 0` must map to 0.5, not 0) |
| `observed_at`, `run_id`, `backfilled` | run time (`now_ts()`), the job's run id, 1 only for catch-up recomputation (§7) |

Write semantics (B4): first success wins. A `failed` row is upgraded by a later run of the same slot. An `ok`/`not_applicable` row is never replaced; a producer that needs fresher data writes the next slot.

| metric_key | raw value written | master normaliser (12 applies it) | w (P1 = 30) |
|---|---|---|---|
| `emission_share` | `emission` (%) | `rankg` | .20 |
| `gate_margin` | `burn_adjusted_share / theta` | `zclip(log2 x, −2, +1)` | .15 |
| `dereg_safety` | `[0,1]` per §2.5 | identity (custom, computed here) | .15 |
| `liquidity_depth` | `tao_liquidity / 1e9` (τ) | `rankg(log)` (≡ `rankg`: log is rank-invariant) | .10 |
| `exit_capacity` | `price_after / price` | `zclip(x, 0.05, 0.6)` | .05 |
| `net_flow_30d` | Σ window (buy_24h − sell_24h) ÷ depth, both rao (§2.7) | `zclip(x, −0.5, +0.5)` | .10 |
| `chain_buys` | `(excess_tao × 7200 / 1e9) ÷ marketcap` (τ/day ÷ τ) | `rankg` | .05 |
| `holder_dispersion` | `1 − top10_share` | `zclip(x, 0.3, 0.9)` | .05 |
| `participation` | `(active_validators + active_miners) / max_uids` (C7) | `zclip(x, 0.2, 0.9)` | .05 |
| `miner_burn_ok` | `1 − implied_miner_burn` | `zclip(x, 0, 0.8)` | .10 |
| *auxiliary / evidence (weight 0, 03 catalog):* `dereg_hazard` `[0,1]`; `implied_miner_burn` `[0,1]` (12's `miner_burn_zeroed` input, `> 0.8` on three consecutive official rows); `emission_enabled_flag` (1/0, `emission_disabled` input); `dereg_risk_flag` (1/0) and `immune_flag` (1/0 = `immune_local`) (`dereg_imminent` inputs); `occupant_age_days` = `(snapshot block_number − network_registered_at) × 12 / 86400` (12's fallback for 11's `tenure`, C1); `vtrust_dispersion` (C7, RPC only); `owner_locked_share` = `owner_personal_locked_alpha / total_subnet_stake` (both rao-scaled α); `sum_subnet_prices` and `tao_usd`, written on the root occupant `0:0` | | | |

Rows are written for every occupant in the snapshot (netuid 1–128, status active or pre_launch). The root occupant `0:0` gets only the two network keys.

### 3.4 Events emitted (`events` table; payload JSON)

All events are written through 03's `emit_event` with `source='chain'` (B9). Names, severities (B2), required payload keys and dedupe patterns are 03 §3.9's (D1). Extra payload keys are allowed. Alert routing belongs to 03/14 (D4): the detection events that are also red flags (`emission_disabled`, `emission_reenabled`, `dereg_risk_entered`, `dereg_proximity`) are digest-only, and the immediate page is 12's `red_flag_raised`. This spec never calls Telegram (D3).

| event_type | severity (03) | emitter | trigger | required payload (03) | dedupe (03) |
|---|---|---|---|---|---|
| `emission_disabled` | critical | `chain_sse` on the flip; `chain_features` only if the flip happened while the recorder was down (no event for the occupant since the previous slot) | `emission_enabled` 1→0 for an active occupant | `netuid, block_number, prev, now` | `occ\|block_number` |
| `emission_reenabled` | info | same | 0→1 | same | `occ\|block_number` |
| `dereg_risk_entered` / `dereg_risk_cleared` | critical / info | `chain_features` (official slots) | `dereg_risk_flag` 0→1 / 1→0 vs the previous official slot | `netuid, moving_price, rank_from_bottom, pool_size` | `occ\|{d7}` |
| `dereg_proximity` | critical | `chain_features` | `dereg_hazard ≥ DEREG_HIGH_RISK_AT` and not immune | `netuid, hazard, components{proximity, starved, decline}`. The components are recomputed with the ported helpers (`emission_cull_proximity`, `relative_emission`, `pct_decline`) exactly as `dereg_risk.py:158-171`, because the score function returns only the total | `occ\|{d7}` |
| `immunity_mismatch` | info | `chain_features` | TMC `immune ≠ immune_local` for a started subnet | `netuid, tmc, local, age_blocks` | `occ\|day` |
| `gate_fit_degraded` | info | `chain_features` | `residual_pp > GATE_FIT_TOLERANCE_PP` | `as_of, residual_pp, theta, n_eligible` (+ `consecutive_days`) | `as_of` |
| `gate_model_broken` | warning | `chain_features` | 7 consecutive official fits degraded (likely a runtime change) | `as_of, consecutive_days, residual_pp` | `gate_model_broken\|{d7}` (**not yet in 03 §3.9**, which must add it per D1: warning, alert yes, sys) |
| `chain_constant_drift` | warning | `chain_rpc_hourly` | RPC live constant ≠ config | `name, config, live, block` | `name\|live` |
| `tmc_schema_drift` | info | `chain_sse` | table field set changed | `added[], removed[]` | sha of the diff |
| `chain_sse_down` | warning | `chain_sse` | `TMC_SSE_ALERT_AFTER_SEC` (1800) in fallback (or unauthorized) | `since, reconnects, last_block` | `since` |
| `chain_sse_recovered` | info | `chain_sse` | first real `full` after a `chain_sse_down` | `since, reconnects, last_block, down_seconds` (D2) | `since` |
| `tmc_budget_degraded` | warning | any 05 TMC job, or the fallback poll | first `BudgetExhausted` caused by the **monthly** cap in a UTC month (G1) | `requests_month, cap` | `month` |

Occupant changes (`netuid_reoccupied`) are emitted by 04 alone (§2 Identity).

---

## 4. Absence-vs-failure table

Status vocabulary is B3's. P1 has no *verifiable absence*: a zero the chain reported is a measured value (`ok`, normalised by 12). `failed` and `not_applicable` both read as None in 12 (B5).

| metric | raw `0.0` with status `ok` (measured zero) | `failed` (value NULL; `value_json.reason`) / `not_applicable` |
|---|---|---|
| `emission_share` | `emission = 0` with `emission_enabled = 1` and `is_active = 1` | `no_snapshot`: no snapshot row for `as_of` (SSE and fallback both down, no repair yet); `field_null` (`emission` null). An analytics-repaired snapshot is usable (`emission_percent`) |
| `gate_margin` | `burn_adjusted_share = 0` for an eligible subnet (fitted `b_i = 0`) | `not_applicable`: not in E (`emission_enabled = 0`, which is the red flag's job, or not started). `failed`: `no_snapshot`, `snapshot_backfilled` (no moving price), `theta_undefined` (`\|E\| < 2`) |
| `dereg_safety` | rank from bottom ≤ 3 in the non-immune pool | `failed`: `moving_price_null` on a started non-immune subnet, `no_registration_block` (immunity not computable), `snapshot_backfilled` |
| `liquidity_depth` | `tao_liquidity = 0` on a started subnet (drained pool; ranked lowest) | `failed`: `field_null`, `units_unresolved` |
| `exit_capacity` | never (ratio > 0 whenever computable) | `failed`: `price ≤ 0`, `price_after` null, `snapshot_backfilled`. `not_applicable`: not started (`price == price_after`, volume 0) |
| `net_flow_30d` | Σ buys = Σ sells = 0 over a complete window (no activity, real) | `failed`: `insufficient_history` (fewer than the required daily rows: analytics fetch failed or occupant < 7 d old) |
| `chain_buys` | `excess_tao = 0` (all TAO injected) | `failed`: `excess_tao` null; `marketcap_unavailable` (`marketcap` null and `price` or `circulating_supply` null, e.g. an analytics-repaired snapshot) |
| `holder_dispersion` | a single holder gives `1 − 1.0 = 0` (real) | `failed`: `no_holders_snapshot` (no snapshot ≤ 14 d: endpoint failed or budget-paused) |
| `participation` | `active = 0` on a started subnet (real, extremely rare) | `failed`: `no_metagraph_source`, **the keyless v1 default** |
| `miner_burn_ok` | `implied_miner_burn = 1` (clipped) → raw 0.0 (fully burned, real). 12's `miner_burn_zeroed` fires on `implied_miner_burn > 0.8` on three consecutive official rows (master §7.5, strict `>`) | `failed`: `burn_unidentifiable` (`predicted_gated_share < 1e-4`), `fit_degraded`, `theta_undefined`, `snapshot_backfilled`. `not_applicable`: not eligible |
| `dereg_hazard` (aux) | immune and emission enabled → 0.0 | `failed`: `hazard_unassessable`. The port returns 0.0 when proximity is None (pool < 3 or netuid not in pool); this collector writes `failed` instead for non-immune occupants |
| `emission_enabled_flag` / `dereg_risk_flag` / `immune_flag` | flag false → 0 | `failed`: `no_snapshot` |

Historical replays (§2.3 tier 3) add reason `snapshot_historical` for `exit_capacity`, `chain_buys`, `holder_dispersion`, `participation` and `emission_enabled_flag`. A fit with `θ ≤ 0` (every eligible realised share 0, as on the dTAO launch day 2025-02-14) writes `gate_margin`, `miner_burn_ok` and `implied_miner_burn` as `failed`/`theta_undefined` (added 2026-10-22: the built code wrote `ok` with a NULL value, which 03's CHECK rejects). There, `dereg_risk_flag` comes from the rank rule alone (`r_i ≤ 3`), because the TMC `deregistration_risk` badge is not in the history.

Rule of thumb, applied everywhere: a value the chain actually reported (including zero) is a real observation (`ok`). A value we could not obtain, or cannot identify from what was reported, is `failed`. Pre-launch occupants (`is_active = 0`) get `not_applicable` for every scored P1 metric except `dereg_safety`. That one follows §2.5: 1.0 while `immune_local`, which covered every not-started subnet on 2026-09-22 (1.7–60 d old); an un-started occupant older than the immunity period would be `failed` with reason `moving_price_null`. The flags, `occupant_age_days` and `dereg_hazard` are written normally, and the registry marks the occupant `pre_launch`.

---

## 5. Configuration (`app/config.py`, pydantic-settings, `UPPER_SNAKE`)

Keys of this spec as 01's `Settings` must carry them (F1: the owner's defaults win). Retired names: `CHAIN_TMC_ENABLED` → `TMC_ENABLED`; `SSE_*` → `TMC_SSE_*` (F2 retires `SSE_MAX_CONNECTIONS`; 01 already uses `TMC_SSE_ENABLED/_URL/_SILENCE_SEC`). `TMC_USER_AGENT` is replaced by 01's `HTTP_USER_AGENT` (one HTTP layer, G1). `TMC_PAUSE_SENTINEL` is gone (B12, see the pause paragraph). `GATE_SNAPSHOT_HOUR_UTC` is gone (the slot rule, §2.3). `DEREG_ALERT_ENABLED` is gone (D4). `SUBTENSOR_RPC_INTERVAL_MIN` and `SUBTENSOR_METAGRAPH_INTERVAL_MIN` are gone (fixed crons, §6.1).

| key | type | default | validator / notes |
|---|---|---|---|
| `TMC_ENABLED` | bool | `true` | master switch for every TMC job of this spec and `chain_sse`; `chain_features` keeps running on stored data |
| `TMC_BASE_URL` | str | `https://api.taomarketcap.com/public/v1` | no trailing slash |
| `TMC_API_KEY` | str | `""` | Pro key (`<prefix>.<secret>`). When non-empty, 01's `Http` sends it as `Authorization` for providers `tmc`/`tmc_sse`. The limiter keeps the anonymous defaults unless the operator raises them (checklist C4) |
| `TMC_RATE_PER_MIN` | int | `8` | 1..10 (F2) |
| `TMC_RATE_BURST` | int | `2` | ≥ 1; validator `TMC_RATE_BURST + TMC_RATE_PER_MIN ≤ 10` (F2), i.e. ≤ 10 requests in any 60-s window = the anonymous per-IP cap |
| `TMC_HOURLY_CAP` | int | `500` | ≤ 600 (F2). The other 100/h are left for `scripts/with_bucket.py` (16 §2.12) |
| `TMC_MONTHLY_CAP` | int | `90000` | ≤ 100,000 (F2). At the cap `ProviderLimiter` raises `BudgetExhausted` and TMC REST jobs are `paused: budget` until the UTC month rolls (G1). SSE is unaffected |
| `TMC_REQUEST_TIMEOUT_SEC` | int | `30` | passed as `timeout=` to `Http.request` for REST calls |
| `TMC_MAX_RETRIES` | int | `2` | passed as `retries=`. 01's `Http` retries 429 (after `Retry-After`) and 5xx/timeouts; 404/401/403 come back unretried (G1) |
| `TMC_SSE_ENABLED` | bool | `true` | LongTask `chain_sse` |
| `TMC_SSE_URL` | str | `https://api.taomarketcap.com/public/v1/sse/subnets/table/` | |
| `TMC_SSE_READ_TIMEOUT_SEC` | int | `90` | > one block (12 s) with margin; a read timeout is a disconnect |
| `TMC_SSE_SILENCE_SEC` | int | `180` | heartbeat: no event, or no block advance → fallback |
| `TMC_SSE_BACKOFF_MIN_SEC` / `TMC_SSE_BACKOFF_MAX_SEC` | float | `1` / `60` | → `ChainSseTask.backoff_min_sec` / `backoff_max_sec` (01 supervisor: ×2, ±20 % jitter); max < 900 s liveness |
| `TMC_SSE_ALERT_AFTER_SEC` | int | `1800` | time in fallback before `chain_sse_down` |
| `TMC_SSE_UNAUTHORIZED_RETRY_SEC` | int | `3600` | ≥ 600; stream retry interval after 401/403 (§2.1) |
| `TMC_SSE_MAX_CONNECTIONS` | int | `2` | 1..2 anonymous (F2). The recorder uses 1, plus 1 for `/sse/blocks/` only if needed |
| `TMC_SSE_BLOCKS_STREAM` | enum `auto\|true\|false` | `auto` | §2.1 |
| `TMC_TABLE_FALLBACK_ENABLED` | bool | `true` | |
| `TMC_TABLE_FALLBACK_SEC` | int | `600` | ≥ 120 |
| `CHAIN_TICKS_BLOCK_STRIDE` | int | `5` | ≥ 1 (F2: ≈ one tick per minute); flag changes are always written |
| `CHAIN_TICKS_RETENTION_DAYS` | int | `30` | 2..365 (F2). Read by 03's `chain_prune` policy in `storage_retention_daily` (03:00Z, E3), not by a 05 job |
| `TMC_HOURLY_ENABLED` | bool | `true` | job `tmc_hourly` |
| `TMC_HOURLY_MINUTE` | int | `7` | 0..59 |
| `TMC_TRENDING_ENABLED` | bool | `true` | `limit=50` (API range 1–50, default 3) |
| `TMC_MARKET_ENABLED` | bool | `true` | TAO/USD |
| `TMC_DAILY_ENABLED` | bool | `true` | job `tmc_daily` |
| `TMC_DAILY_AT_UTC` | str `HH:MM` | `02:10` | after the 00:15Z official scoring and inside 17's quiet window 02:05–02:45 (F2 `BACKFILL_TMC_QUIET_UTC`). Changing it requires changing that window |
| `TMC_SUBNETS_PAGE_LIMIT` | int | `200` | used by `TmcClient.fetch_subnets()` (04 calls it, G2). If the API caps lower (default page 25), paginate with `offset` until `count` |
| `TMC_ANALYTICS_1W_ENABLED` | bool | `true` | 128 req/day: gap repair + daily rows |
| `TMC_ANALYTICS_1W_FIELDS` | str | `ts,block_number,emission_percent,chain_buys,subnet_tao,owner_personal_locked_alpha,owner_personal_decaying_alpha,total_locked,total_decaying,total_tao_buy_24h,total_tao_sell_24h,volume_24h,tx_count_24h,neuron_registrations_24h,rao_recycled_24h,tao_price_usd` | passed as `fields=`. The analytics API has no perpetual field (§2.9) |
| `TMC_PRESSURE_ENABLED` | bool | `true` | network-wide buy + sell, `bucket=1d` |
| `TMC_PRESSURE_WATCHLIST_ENABLED` | bool | `false` | per-subnet pressure for the top-`TMC_PRESSURE_WATCHLIST_SIZE` by emission; + 2 × size req/day |
| `TMC_PRESSURE_WATCHLIST_SIZE` | int | `40` | 1..128 |
| `TMC_WEEKLY_ENABLED` | bool | `true` | job `tmc_weekly`: each day processes the netuids with `netuid % 7 == weekday()` (Mon = 0) |
| `TMC_WEEKLY_AT_UTC` | str `HH:MM` | `01:10` | outside 17's quiet windows and 11's 20:30–23:30Z window |
| `TMC_HOLDERS_ENABLED` | bool | `true` | |
| `TMC_HOLDERS_PAGE_LIMIT` / `TMC_HOLDERS_MAX_PAGES` | int | `100` / `2` | |
| `TMC_HOLDERS_MAX_AGE_DAYS` | int | `14` | oldest snapshot `holder_dispersion` may use |
| `TMC_CANDLES_ENABLED` | bool | `true` | `GET /subnets/{id}/candle-chart-compact/?period=1h&from_timestamp=<ISO Z>&to_timestamp=<ISO Z>`. `from` = the last stored `tmc_candle` 1h bar − 2 h (first run: now − 8 d), `to` = now. Rows are `[ts, block, o, h, l, c, v, start_volume, tao_usd(, tao_eur)]`; the parser accepts 9 or 10 elements (research reports differ) and `ts` as ISO or epoch |
| `TMC_ANALYTICS_1Y_ENABLED` | bool | `true` | 1Y daily rows via `upsert_daily_from_analytics`: `analytics_1y` replaces `analytics_1w`, never the reverse; a same-rank row is never overwritten (J1) |
| `TMC_VALIDATORS_ENABLED` | bool | `true` | 1 req/week (Mondays) into `chain_rest_snapshots` |
| `TMC_NEURONS_ENABLED` | bool | `false` | `/subnets/neurons/{id}/` in the weekly slice for `participation` (≈ 18 req/day) |
| `NETWORK_IMMUNITY_PERIOD_BLOCKS` | int | `864000` | > 0; re-verify quarterly (master §5). The same single key is read by 01/14 |
| `GATE_MODEL_VERSION` | str | `gate-v440-1` | changes when the algorithm changes |
| `GATE_BAR_RANK` | int | `32` | ≥ 2 |
| `GATE_EXPONENT` | float | `3.0` | > 0 |
| `GATE_FIT_MAX_ITER` / `GATE_FIT_TOL` | int / float | `50` / `1e-4` | convergence of the burn fit |
| `GATE_FIT_TOLERANCE_PP` | float | `6.0` | > 0; model quality: a residual above it ⇒ degraded (research baseline 3.1) |
| `GATE_MIN_PRED_SHARE` | float | `1e-4` | below it the burn is unidentifiable ⇒ `failed` |
| `GATE_HOURLY_ENABLED` | bool | `false` | diagnostic hourly fits |
| `GATE_BROKEN_AFTER_DAYS` | int | `7` | ≥ 2; consecutive degraded official fits before `gate_model_broken` |
| `DEREG_STARVED_RATIO` / `DEREG_EMISSION_DECLINE_PCT` / `DEREG_PRICE_DECLINE_PCT` / `DEREG_DEPTH_DECLINE_PCT` | float | `0.20` / `30` / `40` / `30` | > 0 → `DeregRiskConfig` (defaults = the old module's, `dereg_risk.py:42-47`) |
| `DEREG_W_PROXIMITY` / `DEREG_W_STARVED` / `DEREG_W_DECLINE` | float | `0.45` / `0.25` / `0.30` | must sum to 1.0 ± 1e-6 (validator) |
| `DEREG_HIGH_RISK_AT` | float | `0.60` | 0..1; `dereg_proximity` threshold (event only) |
| `DEREG_HIST_DAYS` | int | `30` | 7..90 |
| `CHAIN_FEATURES_ENABLED` | bool | `true` | job `chain_features` |
| `CHAIN_FEATURES_AT_UTC` | list[str] | `["00:05","06:05","12:05","18:05"]` | validator: hours exactly {00, 06, 12, 18}, minute 1..14 (after the slot, before 12's :15 scoring). 00:05 is official, the others provisional |
| `CHAIN_FEATURES_CATCHUP_DAYS` | int | `7` | 0..30. Every run first recomputes missing official `as_of` days in the window from stored raw (`backfilled = 1`, `observed_at` = now) |
| `SUBTENSOR_RPC_ENABLED` | bool | `false` | optional collector; `bittensor` imported lazily only when true |
| `SUBTENSOR_RPC_MODE` | enum `jsonrpc` | `jsonrpc` | `jsonrpc` = `websockets` + SCALE-free reads via `state_getStorage` for scalar constants and `state_call` runtime APIs (optionally decoded with `async-substrate-interface`). The bittensor SDK mode is not offered: the SDK depends on `bittensor-wallet` (master §1.2 non-goal). |
| `SUBTENSOR_LOCAL_WS` | str | `ws://127.0.0.1:9944` | lite node (checklist C7) |
| `SUBTENSOR_RPC_ENDPOINTS` | list[str] | `[SUBTENSOR_LOCAL_WS, "wss://bittensor-finney.api.onfinality.io/public-ws", "wss://entrypoint-finney.opentensor.ai:443"]` | ordered; `ONFINALITY_API_KEY` is appended as `?apikey=` when set (URL form UNVERIFIED — checklist B2) |
| `SUBTENSOR_ENDPOINT_COOLDOWN_SEC` | int | `300` | `EndpointSelector` cooldown (old default) |
| `SUBTENSOR_RPC_CONNECT_TIMEOUT_SEC` / `SUBTENSOR_RPC_READ_TIMEOUT_SEC` | int | `30` / `45` | old `CHAIN_CONNECT_TIMEOUT_SEC` / `CHAIN_READ_TIMEOUT_SEC` |
| `SUBTENSOR_METAGRAPH_ENABLED` | bool | `false` | 128 metagraph reads per pass (job `chain_rpc_metagraph`; also needs `SUBTENSOR_RPC_ENABLED`) |

**Pause semantics (B12 as amended 2026-09-23, 16 §2.11).** Operator pauses are `control_state` rows written by the API, Telegram or CLI. `data/control/<target>.paused` files are an emergency input the app reads but never writes. A target is an **exact job id** (§6.1) or a named group from 16's `PAUSE_GROUPS`; there is no prefix matching. The groups this spec needs 16 to define:
- `chain` = {`chain_sse`, `tmc_hourly`, `tmc_daily`, `tmc_weekly`, `chain_features`, `chain_rpc_hourly`, `chain_rpc_metagraph`};
- `tmc` = {`tmc_hourly`, `tmc_daily`, `tmc_weekly`}, plus whichever 04/11/17 TMC jobs 16 adds. It never includes `chain_sse`: SSE is not REST-metered.

`KILL_SWITCH` does not pause this spec (free provider); `PAUSE_ALL` pauses everything here. While paused, the recorder drops the stream within one watchdog tick (15 s). On resume it reconnects and waits for a fresh `full`.

---

## 6. Cadence, rate limits, cost

### 6.1 Jobs and LongTasks (01's `JOB_TABLE` mirrors this table verbatim, A2/E2)

The table replaces 01's placeholders `chain_rest_daily` and `chain_holders_weekly`. All rows: `paid=False`, `max_instances=1`, `coalesce=True`, `min_gap_factor=0` (cron). Liveness is 16's rule (E4): `last_run`-based for jobs; `chain_sse` = 900 s on `max(last_beat, last_attempt)`.

| id | factory (`app.…`) | trigger | cadence / UTC time | misfire grace s | timeout s | enabled flag | provider | phase | notes |
|---|---|---|---|---|---|---|---|---|---|
| `chain_sse` | `collectors.chain_tmc:ChainSseTask` | **LongTask** (E1) | continuous; REST fallback every `TMC_TABLE_FALLBACK_SEC` (600 s) while silent or unauthorized | — | — (per-connection read timeout `TMC_SSE_READ_TIMEOUT_SEC` 90) | `TMC_SSE_ENABLED` (and `TMC_ENABLED`) | `tmc_sse` (fallback polls: `tmc`) | 2 | starts at boot, not staggered (16 boot step 6); backoff 1 → 60 s |
| `tmc_hourly` | `collectors.chain_tmc_rest:TmcHourlyJob` | cron | every hour at `minute=TMC_HOURLY_MINUTE` (07) | 1800 | 300 | `TMC_HOURLY_ENABLED` | `tmc` | 2 | 2 req: trending, market-data |
| `tmc_daily` | `collectors.chain_tmc_rest:TmcDailyJob` | cron | `TMC_DAILY_AT_UTC` = 02:10Z | 3600 | 5400 | `TMC_DAILY_ENABLED` | `tmc` | 2 | ≈ 133 req ≈ 17 min. **Startup catch-up:** if `last_ok` is older than the most recent scheduled 02:10Z, run at boot + 300 s, unless that lands in 20:25–23:35Z (11's window), in which case run at 23:35Z |
| `tmc_weekly` | `collectors.chain_tmc_rest:TmcWeeklyJob` | cron | `TMC_WEEKLY_AT_UTC` = 01:10Z daily, processing 1/7 of the netuids | 3600 | 3600 | `TMC_WEEKLY_ENABLED` | `tmc` | 2 | ≈ 55–74 req (+ 18 with neurons). No catch-up: a missed slice waits a week, and `holder_dispersion` tolerates 14 d |
| `chain_features` | `features.chain_features:ChainFeaturesJob` | cron | `CHAIN_FEATURES_AT_UTC`: 00:05Z official; 06:05, 12:05, 18:05Z provisional | 10800 | 900 | `CHAIN_FEATURES_ENABLED` | — (DB only) | 2 | computes the latest slot ≤ now. **Startup catch-up:** if `last_ok` is older than the most recent scheduled run (xx:05Z), run at boot + 300 s. Each run first back-fills missing official days (§5 `CHAIN_FEATURES_CATCHUP_DAYS`) |
| `chain_rpc_hourly` | `collectors.chain_rpc:ChainRpcHourlyJob` | cron | every hour at minute 01 | 1800 | 600 | `SUBTENSOR_RPC_ENABLED` | `subtensor_rpc` | 2 | optional, off by default; runs before `chain_features` at :05 |
| `chain_rpc_metagraph` | `collectors.chain_rpc:ChainRpcMetagraphJob` | cron | 00:02, 06:02, 12:02, 18:02Z | 3600 | 1800 | `SUBTENSOR_METAGRAPH_ENABLED` (and `SUBTENSOR_RPC_ENABLED`) | `subtensor_rpc` | 2 | optional, off by default; `as_of` = the slot |

Two mechanisms are needed from 01. **Startup catch-up** is a new `JobSpec` attribute `startup_catchup: bool` (with `catchup_avoid_utc: list[str]`). At boot, if `job_state.last_ok` is older than the job's most recent scheduled fire time ≤ now (computed from its cron fields), 01's `setup_scheduler` sets `next_run_time = boot + 300 s`, shifted past an avoid window. A fixed-age threshold such as "26 h" would miss a run when the box is down across 02:10Z for less than two hours. 11's `owner_collect_daily` needs the same attribute. Misfire grace covers only a suspended process; a power-off restart loses APScheduler's in-memory schedule, so the catch-up attribute is what implements master §11 "laptop power loss" for cron jobs. A **provider list** for 01's `ProviderLimiter` table is also needed: `subtensor_rpc` (lite node unlimited; OnFinality public 5 rps, with a key 40 rps).

### 6.2 Rate limits (G1)

**Provider limits (anonymous, per public IP; research 02-keyless §0):** 10 req/60 s, 600 req/h, 100,000 req per calendar month (UTC), 2 concurrent SSE connections. SSE responses carry `X-RateLimit-Limit: 0` (not metered). Every host behind the same NAT shares these limits, so ad-hoc scripts run through 16's `scripts/with_bucket.py`, which grants 2/min and 100/h while the service is up (16 §2.12).

**One limiter.** `TmcClient` never builds its own HTTP client or bucket. Every REST call is `await rt.http.request('tmc', 'GET', url, params=…, timeout=TMC_REQUEST_TIMEOUT_SEC, retries=TMC_MAX_RETRIES)` on 01's `ProviderLimiter['tmc']`:
- rate `TMC_RATE_PER_MIN/60` per second, burst `TMC_RATE_BURST`, caps `TMC_HOURLY_CAP` and `TMC_MONTHLY_CAP`;
- the monthly count persists hourly in `provider_spend.requests` and is reseeded at boot;
- `X-RateLimit-Remaining/Reset` and `Retry-After` are honoured through `observe`/`penalize`;
- `BudgetExhausted` is raised at either cap.

There are no priority classes (G1). Contention is avoided by the schedule (§6.3), not by queue order. `tmc_sse` is a connection count (≤ `TMC_SSE_MAX_CONNECTIONS`), not a bucket. `app/providers/ratelimit.py`, `get_bucket()` and `provider_usage` are retired.

**`TmcClient` error contract:**
- **200** → `TmcResponse(ok=True, …)`.
- **404/401/403**, returned unretried by 01 → `TmcResponse(ok=False, status=…)`. A 404 is never retried within a run.
- **`ProviderError`** after 01's retries → `TmcResponse(ok=False, error=…)`.
- **`BudgetExhausted` propagates.** Each 05 job lets it propagate after committing the netuids already done. 01 records the run, and later runs skip with `paused: budget` until the hourly window or the UTC month resets (G1). A monthly-cap hit also emits `tmc_budget_degraded` once per month (§3.4).

The 128-netuid loops iterate in ascending netuid with a per-netuid `try/except`. A failed netuid is logged and skipped, and is never retried in the same run beyond 01's `retries`.

### 6.3 TMC budget — every caller (G3; this spec owns the table, A2)

**05's own tiers (requests/day):**

| tier (job) | endpoint | req/day | note |
|---|---|---|---|
| continuous (`chain_sse`) | SSE `/sse/subnets/table/` | 0 | 1 connection (`tmc_sse`) |
| fallback (`chain_sse`) | `/subnets/table/` every 10 min | 0 (≤ 144 when SSE is down or 401) | ≤ 6/h |
| hourly (`tmc_hourly`, :07) | `/analytics/trending/subnets/?limit=50`; `/market/market-data/` | 24 + 24 | |
| daily (`tmc_daily`, 02:10Z) | `/general/constants/?type=subtensor_module` | 1 | |
| daily | `/subnets/burn/`, `/subnets/network-lock-cost/` | 2 | |
| daily | `/analytics/subnet/{n}/?span=1W&fields=…` × 128 | 128 | gap repair + daily rows |
| daily | `/extrinsics/staking-activity/pressure/` buy + sell, network-wide | 2 | + 2 × `TMC_PRESSURE_WATCHLIST_SIZE` with the watchlist option |
| weekly slice (`tmc_weekly`, 01:10Z) | `/accounts/subnet-holders/{n}/` × ≈ 18 (≤ 2 pages) | 18–37 | |
| weekly slice | `/subnets/{id}/candle-chart-compact/?period=1h` × ≈ 18 | 18 | |
| weekly slice | `/analytics/subnet/{n}/?span=1Y` × ≈ 18 | 18 | |
| weekly slice | `/subnets/validators/` | 1 (Mondays) | |
| weekly slice (option) | `/subnets/neurons/{id}/` × ≈ 18 | 0 (18 with `TMC_NEURONS_ENABLED`) | |
| **05 total** | | **≈ 236–255**; **≤ 290** with 10 % retries (the figure used below) | worst ≈ 550: fallback all day + watchlist + neurons + retries |

`/subnets/` and `/accounts/coldkey-identities/` are **not** in 05's tiers: 04 fetches them and 05 reads the snapshots (G2).

**All callers:**

| caller | job(s), time | steady req/day | worst req/day | peak req/h | ref |
|---|---|---|---|---|---|
| 04 registry | `registry_occupants` every 6 h (`/subnets/` 1–2, `/subnets/table/` 0–1); `registry_identity` 02:40Z (coldkey-identities 1, + pages if paginated) | ≤ 15 (≤ 12 + 1, + pages) | 15 | ≤ 3 | 04 §6, G3 |
| 05 own tiers | table above | ≤ 290 | ≈ 550 | 133 + 11 in the 02:00Z hour; 74 + 11 at 01:00Z; ≤ 8 otherwise | this spec |
| 06 github-stats cross-check | `github_html_probe` 03:00Z: 1 × `/subnets/table/` + ≤ 20 × `/subnets/github-stats/{n}/` | ≤ 21 | 21 | 21 | 06 §6, G3 |
| 11 `owner_collect_daily` | 20:30–23:30Z, paced ≤ 5/min | ≈ 390 | ≤ 518 (detail mode) + ≤ 150 per run until the first-run backfill completes | ≤ 300 | 11 §6, G3 |
| 17 `backfill_tmc` | manual, one-off; ≤ `BACKFILL_TMC_MAX_PER_HOUR` (180); sleeps in `BACKFILL_TMC_QUIET_UTC` 02:05–02:45 and 20:25–23:35Z | 0 | 386 + 10 % (once) | ≤ 180 | 17 §2.4, F2 |
| **total** | | **≈ 716/day ≈ 21.5 k/month** (290 + 15 + 21 + 390) | **≈ 1.35 k/day ≈ 40 k/month** (550 + 1.1 × (15 + 21 + 518 + 150)) | **≈ 311/h** in 20:30–23:30Z (11: 300 + 05 hourly 2 + 05 fallback 6 + 04: 3) | G3 |

Check against the caps. Per minute, the limiter enforces 8/min + burst 2 = ≤ 10 in any 60 s. Per hour: 20:30–23:30Z ≈ 311; 01:00Z ≈ 74 + 11 + 180 (17 running) = 265; 02:00Z ≈ 144 (17 asleep); 03:00Z ≈ 21 + 11 + 180 = 212. All are < 500. Per month: worst ≈ 40 k < 90 k. The 02:10Z batch takes ≈ 17 min at 8/min and ends by ≈ 02:27Z, before 04's 02:40Z identity run, inside 17's quiet window. Cost: $0.

**RPC (optional):** local lite node — no limit; OnFinality public 5 rps (error `-32029` above), free key 40 rps; `entrypoint-finney` unmetered but keeps only ~7–13 h of state. Per hourly pass: ≈ 6 scalar reads + 2 map reads (`MinerBurned`, `SubnetEmissionEnabled`) ≈ 10 calls; metagraph pass 128 runtime-API calls paced at 2/s.

---

## 7. Failure modes and fail-safe behaviour

- **Never raise into the scheduler.** Every job body runs inside 01's `CollectorJob.run()` wrapper: swallow, log, write a `collector_runs` row, `consecutive_failures += 1`, and a `collector_failing` event through `AlertLatch` at `JOB_FAIL_ALERT_THRESHOLD` (5). `ChainSseTask` is supervised by 01's `LongTask.run_forever()`: an unexpected exception in `run_once` is logged, `reconnects` increments, and the connection is retried after the backoff. The watchdog is a child of the connection; the REST fallback is a child task of `ChainSseTask`, cancelled on shutdown.
- **Last-good semantics.** `TableState` keeps the last `full`+deltas in memory across reconnects; a reconnect always starts with a new `full` (never a delta applied to stale state — deltas received before the first `full` of a connection are dropped). Rows written during fallback carry `source='rest_table'`; consumers treat both sources as live.
- **Partial rows.** A missing or non-numeric field is stored `NULL` and counted in `/health.tasks.chain_sse.field_nulls_24h`. A value of the wrong type for 03's STRICT column (e.g. a string in an INTEGER flag) is coerced or stored `NULL`, never allowed to abort the batch. A whole malformed event is dropped and counted (`bad_events_24h`). A `full` with `< 100` rows is treated as malformed (TMC always returns 129).
- **Per-netuid isolation** in the 128-loops. Each netuid's rows are committed on their own. A run with ≥ 25 % netuid failures ends by raising `ChainPartialFailure(failed, total)` after its commits. 01's wrapper then records `ok = 0` (so the 5-failure alert can trigger), and the successful rows are kept.
- **Occupant boundary.** If `network_registered_at` changes, the old occupant's rows are untouched. Feature windows for the new occupant start empty (`failed`/`insufficient_history` until there are enough rows), never inherited.
- **Budget exhaustion (G1).** `BudgetExhausted` at the hourly or monthly cap ends the current job run after its commits, and later runs skip with `paused: budget` until the window resets. The first monthly-cap hit in a month emits `tmc_budget_degraded`. The SSE stream is unaffected. The SSE fallback skips polls while exhausted, so an SSE outage during budget exhaustion leaves a `chain_hourly` gap that `tmc_daily` repairs once the budget returns (≤ 7 days back with `span=1W`; older gaps need 17's `analytics1m` task).
- **Power loss / restart.** On start:
  1. `chain_sse` reconnects immediately.
  2. `tmc_daily` catches up at boot + 300 s if its `last_ok` predates the most recent scheduled 02:10Z (§6.1). Its `span=1W` pull repairs `chain_hourly` for the outage: only missing `(netuid, hour_ts)`, `backfilled = 1`, `source='analytics_1w'`, analytics columns only per §3.1, and **no price or moving price** in repaired hours. An outage shorter than a day that does not span 02:10Z is repaired at the next 02:10Z run.
  3. `chain_features` catches up at boot + 300 s if its `last_ok` predates the most recent scheduled run. Every run recomputes missing official days within `CHAIN_FEATURES_CATCHUP_DAYS` from stored raw data (`backfilled = 1`, `observed_at` = now, first-success-wins, so live rows are never replaced). For days whose snapshot is an analytics row, the price and moving-price metrics are `failed` with reason `snapshot_backfilled`.
  4. `chain_ticks` gaps are accepted, not repaired.

  Misfire grace (3 h for `chain_features`) only covers a *suspended* process, e.g. a laptop waking at 02:00Z. A cold boot relies on the catch-up attribute.
- **Alert thresholds** (all are events; delivery is 14's, D3/D4): `chain_sse_down` after 1,800 s in fallback (once per outage, plus `chain_sse_recovered`); `collector_failing` after 5 consecutive job failures (01 wrapper); `gate_fit_degraded` is digest-only until `GATE_BROKEN_AFTER_DAYS` (7) consecutive degraded official fits emit `gate_model_broken` (warning, likely a runtime change); `chain_constant_drift` once per `(name, live)` value.
- **`/health` fields** (01 builds them, G6):
  - `tasks.chain_sse` has 01's keys (`enabled, available, connected, last_beat, last_attempt, reconnects, last_error, last_skip_reason`) plus `state ∈ {disabled, connecting, connected, silent, fallback, unauthorized, paused}`, `last_event_at`, `last_block`, `event_at_source`, `fallback_state`, `bad_events_24h`, `field_nulls_24h`, `ticks_written_24h`, `hourly_rows_last_flush`.
  - `collectors.<id>` for `tmc_hourly`, `tmc_daily`, `tmc_weekly`, `chain_features`, `chain_rpc_hourly`, `chain_rpc_metagraph` uses 01's standard block.
  - `rate_limits.tmc` uses 01's keys (`tokens, hour_used, hour_cap, month_used, month_cap, penalized_until`).
  - Liveness is 16's (E4): `chain_sse` 900 s on `max(last_beat, last_attempt)`, so a TMC outage never causes a restart loop; the jobs use `last_run` with 01's formula. `tmc_daily.last_ok` is **not** a liveness key (16 §11 item 8).

---

## 8. Implementation plan

Files (all under `~/taosentinel/app/` unless noted):

| file | contents |
|---|---|
| `app/collectors/chain_tmc.py` (G2; **built at the start of Phase 1**, because 04 needs it) | **Client.** `@dataclass(slots=True) TmcResponse(ok: bool, status: int \| None, json: Any, headers: dict[str, str], error: str \| None, elapsed_ms: int, url: str)`. `class TmcClient` with `__init__(self, http: Http, settings_getter: Callable[[], Settings])`. **Methods, all `async`:** <br>• `get(self, path: str, params: dict \| None = None, *, timeout: float \| None = None) -> TmcResponse`: `http.request('tmc', 'GET', TMC_BASE_URL + path, params=…, timeout=…, retries=TMC_MAX_RETRIES)`, error contract §6.2, `BudgetExhausted` propagates <br>• `get_paginated(self, path, params=None, *, page_limit: int, max_pages: int) -> TmcResponse`: `limit`/`offset`; accepts a bare list or `{count, next, results}`; stops at `count`, a short page or `max_pages`; `json = {"count": n, "results": [...]}` <br>• `fetch_subnets()` (04 `registry_occupants`, G2), `fetch_subnets_table()` (04 fallback, 06 cross-check, SSE fallback), `fetch_coldkey_identities()` (04 `registry_identity`, G2) <br>• `fetch_analytics(netuid: int, span: str, fields: str \| None = None)` (05, 17); `fetch_candles(netuid: int, period: str, from_ts: int, to_ts: int, *, timeout: float \| None = None)` (ISO Z params; 05, 17) <br>• `fetch_holders(netuid: int)` (paged per §2.6); `fetch_pressure(side: str, from_ts: int, to_ts: int, *, netuid: int \| None = None, bucket: str = '1d')`; `fetch_trending(limit: int = 50)`; `fetch_market()`; `get(path)` for 11's owner endpoints <br>• `@asynccontextmanager stream_sse(self, url: str) -> AsyncIterator[httpx.Response]`: 01 `Http.client('tmc_sse').stream(...)`, holding one `tmc_sse` connection slot <br>**Module helpers.** `def parse_sse(lines: AsyncIterator[str]) -> AsyncIterator[SseEvent]` (pure; `SseEvent(event: str \| None, data: str, id: str \| None)`). `REST_SNAPSHOT_KINDS: dict[str, tuple[str, str]]` (kind → endpoint template, canonical params; §3.1). `async def write_rest_snapshot(db, kind: str, resp: TmcResponse, *, run_id: str, path_args: dict \| None = None) -> None`. `async def latest_rest_snapshot(db, kind: str, *, before: int \| None = None, max_age_s: int \| None = None, path_args: dict \| None = None) -> RestSnapshot \| None` (`RestSnapshot(endpoint, params, observed_at, run_id, http_status, payload)`) <br>**Recorder.** `class TableState`: `replace(rows: list[dict]) -> None`, `patch(changes: dict[str, dict], added: list[dict]) -> set[int]` (netuids touched), `block_number() -> int \| None`, `rows() -> dict[int, dict]`, `changed_flags(netuid: int) -> set[str]`, `field_set() -> frozenset[str]`, `mark_event(block: int \| None, ts: int) -> None`. `class ChainSseTask(LongTask)` (§2.1): `run_once(stop)`, `_watchdog(resp)`, `start_fallback(reason: str)`, `_fallback_loop()`, `on_full()`, `_write_ticks(block: int)`, `_maybe_flush_hour()`, `_flush_hour(hour_ts: int)`, `_rollup_5m(hour_ts: int)`, `_rollup_1h(hour_ts: int)`, `_network_row(hour_ts: int)`, `status() -> dict`. `def immune_local(block: int, network_registered_at: int, period: int) -> bool` |
| `app/collectors/chain_tmc_rest.py` | `class TmcHourlyJob(CollectorJob)`, `TmcDailyJob(CollectorJob)`, `TmcWeeklyJob(CollectorJob)` with `collect(self, run: RunContext) -> JobOutcome` (ids and cadence per §6.1; factories `collectors.chain_tmc_rest:<Class>`). Helpers used by 05 and 17 (J1): <br>• `async def upsert_hourly_from_analytics(db, rows: list[dict], *, netuid: int, occupant_key: str, source: str, run_id: str, observed_at: int, backfilled: int = 1) -> int`: called once per occupant segment (17); maps columns per §3.1; 03's precedence upsert means only missing `(netuid, hour_ts)` land; returns the rows written <br>• `async def upsert_daily_from_analytics(db, rows: list[dict], *, netuid: int, source: str, run_id: str, observed_at: int, occupant_key: str \| None = None, backfilled: int = 0) -> int`: key = a pre-set `row['occupant_key']` if present (J1), else the argument, else 04's `occupant_at(db, netuid, row['block_number'])`; rows with no key are skipped and counted; 03 precedence `analytics_1y` > `analytics_1w`, same rank never overwritten (J1); 1W day alignment per §3.1 <br>• `async def write_trending_daily(db, day_ts: int, *, run_id: str) -> int` (§3.1) <br>• `def canonical_params(params: dict) -> str` |
| `app/collectors/endpoints.py` | **copied verbatim** from `E:\SN_Trader\SubnetTrader\app\chain\endpoints.py` lines 1-81 (`is_dead_socket_error` 31, `is_quota_error` 39, `EndpointSelector` 49-81). It moves out of the retired `app/providers/` package; 17 imports it from here |
| `app/collectors/chain_rpc.py` | `class RpcCollector(selector: EndpointSelector, mode: str, connect_timeout: float, read_timeout: float, limiter: ProviderLimiter)`. Every JSON-RPC request first does `await limiter.acquire('subtensor_rpc')`: websockets bypass `Http`, so the limiter is used directly (17 §2.3 does the same). Methods: `async def read_constants() -> dict[str, int \| float]` (`NetworkImmunityPeriod`, `EmissionBarQuantile`, `EmissionGateExponent`, `SubnetOwnerCut`, `SubnetLimit`, `TotalNetworks`); `async def read_subnet_scalars(netuids: list[int]) -> dict[int, dict]` (`MinerBurned`, `SubnetEmissionEnabled`, `SubnetMovingPrice`); `async def read_metagraph(netuid: int) -> MetagraphStats` (`@dataclass` with exactly the `rpc_metagraph_daily` value columns: `n_uids, max_uids, active_validators, active_miners, permit_count, vtrust_mean, vtrust_pstdev, incentive_to_owner_share, block_number`). Jobs `ChainRpcHourlyJob`, `ChainRpcMetagraphJob` (§6.1). <br>`jsonrpc` mode: `chain_getHeader`; `state_getStorage` on `twox128(pallet) + twox128(item)` [+ u16 LE netuid with the Identity hasher, verified for `SubnetTAO`, `SubnetAlphaIn/Out`, `SubnetMovingPrice`, `SubnetVolume` (research 02-continuity §2); **UNVERIFIED** for `MinerBurned`/`SubnetEmissionEnabled`], decoding u16/u64/U96F32/I96F32/bool; `state_call SubnetInfoRuntimeApi_get_all_dynamic_info` decoded with `async-substrate-interface`. <br>`sdk` mode: `await bt.Subtensor(url)` with `sub.query(bt.storage.SubtensorModule.<Item>, [netuid])`, `sub.subnets.metagraph(netuid)`, `sub.hyperparameters.subnet_emission_enabled(netuid)` (v11 names per research 01-web-chain-apis §2; verify on the box). `bittensor` is imported only inside the enabled path |
| `app/quant/dereg_risk.py` | **ported verbatim** from `E:\SN_Trader\SubnetTrader\app\strategy\dereg_risk.py` lines 1-194 (whole file, pure stdlib, zero app imports; 13 does not import it). `DeregRiskConfig` is built from the `DEREG_*` settings in `chain_features` |
| `app/util/mathutil.py` (**created here in Phase 2**; 12 and 13 extend it later) | copied from `E:\SN_Trader\SubnetTrader\app\utils\math.py`: lines 1-131 (`pearson_r` 9, `_average_ranks` 28, `spearman_r` 45, `zscore` 57, `quantile_buckets` 73, `log_returns` 91, `returns_pearson_r` 100, `effective_bets` 118) **plus `gini_coefficient` lines 160-177**, which lies outside the `1-131` range 12 and 13 cite |
| `app/features/gate_model.py` | `@dataclass(frozen=True) GateFit(theta: float, b: dict[int, float], adj: dict[int, float], gate: dict[int, float], pred: dict[int, float], pgs: dict[int, float], implied_burn: dict[int, float \| None], residual_pp: float, iterations: int, converged: bool, burn_source: str)`; `def fit_gate(mp, realised, *, bar_rank, h, burn_override=None, max_iter, tol, min_pred_share) -> GateFit` (§2.3); `def eligible_set(rows: dict[int, SnapshotRow]) -> dict[int, SnapshotRow]`; `def gate_margin_raw(fit: GateFit, netuid: int) -> float \| None` (`adj/theta` for netuids in the fit) |
| `app/features/holders.py` | `def concentration(rows: list[dict], count: int) -> HolderStats(top1_share, top10_share, hhi_topn, gini_topn, n_fetched)` |
| `app/features/chain_features.py` | `class ChainFeaturesJob(CollectorJob)` (§6.1): `collect()` = catch-up of missing official days, then `run_slot(latest slot ≤ now)`. `@dataclass SnapshotRow(netuid: int, occupant_key: str, hour_ts: int, source: str, event_at: int, block_number: int \| None, f: dict)`. `async def run_slot(db, as_of: int, *, run_id: str, observed_at: int, backfilled: int = 0) -> SlotReport` (fit → gate tables → P1 + aux rows → one `put_metric_values` call → events; with `backfilled=1` it also sets `value_json.backfilled = true`, and its events carry `payload.backfilled = true` with `alerted_at` preset (J1). This is the entry point of 17's `replay_history`). `async def snapshot_rows(db, as_of: int) -> dict[int, SnapshotRow]` (§2.3 rule). `def dereg_metrics(rows, hist, cfg: DeregRiskConfig) -> dict[int, dict]`. `async def net_flow(db, occupant_key: str, as_of: int, snap: SnapshotRow) -> tuple[float \| None, dict]`. `def p1_rows(...) -> list[dict]` (row dicts per §3.3). `async def emit_transitions(db, prev: dict, now: dict, *, as_of: int)`. `@dataclass SlotReport(as_of: int, snapshot_source: str, n_rows: int, n_failed: int, fit: GateFit \| None, events: int)`. CLI `python -m app.features.chain_features run --as-of <YYYY-MM-DD[THH]> \| matrix [--as-of …] [--json]` |
| `app/features/normalise.py` (Phase-2 subset; 12 owns it from Phase 8) | `recency`, `logcap`, `rankg`, `zclip`, `checklist` exactly per master §7.2, with `None` passthrough |
| *(no writer module)* | `metric_values` go only through 03's `app/storage/pit.py::put_metric_values` (Phase 0, B4). The old Phase-2 `store.py` with `INSERT OR REPLACE` is dropped (it would trip 03's delete trigger); 12 keeps `upsert_metric_values` as an alias |
| `app/scoring/p1_matrix.py` (provisional, Phase 2 only) | `def p1_matrix(db, as_of: int) -> list[dict]`: normalise, then the coverage-renormalised weighted mean with master P1 weights, `coverage < 0.3 ⇒ None`; `rankg` over active occupants of the same `as_of`. **CLI only** (`… chain_features matrix`): HTTP routes are 14's (A2), and 14's matrix supersedes it in Phase 11 |
| `scripts/tmc_probe.py` | one-shot, run through `scripts/with_bucket.py` (16) while the service runs. Its SSE connection takes the second of the two per-IP slots, so it refuses to start while `chain_sse` holds a `/sse/blocks/` slot. Connects SSE for `--seconds` (60) and prints the field set, envelope keys, block source and `event_at` source. Units checks: SN64 `tao_liquidity` magnitude (rao ≈ 2e14); SSE vs analytics `owner_personal_locked_alpha` for one hour (11's request); `total_tao_buy_24h` vs `subnet_tao`; analytics `total_locked` vs SSE `total_locked_alpha`. Writes `data/tmc_table_fields.json` = `{captured_at, block_number, fields[44], envelope_keys[], units{…}, event_at_source, block_source}`. `--save-fixture` also writes `tests/fixtures/tmc_table_<date>_block<N>.json` |
| `tests/test_chain_*.py` | §9 |

**Step order.**
1. Phase 1: `TmcClient` on 01's `Http` + `write/latest_rest_snapshot` + tests (respx mock). 04 depends on these.
2. `TableState` + `parse_sse` + `ChainSseTask` with an injected fake stream; the `chain_ticks`/`chain_hourly`/`price_bars`/`chain_network_hourly` writers (03's precedence upserts).
3. `scripts/tmc_probe.py` on the laptop → freeze `data/tmc_table_fields.json`, confirm units.
4. REST jobs (hourly, daily, weekly) and the `JOB_TABLE` rows of §6.1.
5. Port `dereg_risk.py` and `mathutil.py`; write `gate_model.py`, `holders.py`, `chain_features.py` and the minimal `normalise.py`.
6. `p1_matrix.py` + CLI.
7. `chain_rpc.py` behind the flag, plus the `endpoints.py` copy. Timeout/failover pattern from `chain_client.py`: lines 110-166 (`_get_subtensor` connect loop with `mark_ok/mark_bad`, `_failover_subtensor`); 180-195 (`_discard_subtensor`, bounded close under `wait_for`); 197-222 (`asyncio.wait_for(gather(..., return_exceptions=True), timeout=CHAIN_READ_TIMEOUT_SEC)`, which on timeout drops the socket and surfaces the exception per read so failover engages); 237-257 (`SubnetEmissionEnabled` `query_map`: only explicit `False` entries exist; fail open).
8. `/health` wiring (01 G6), event emission, docs.

**Port verbatim (file:line):** `dereg_risk.py:1-194` (config 31-55, score 127-174); `endpoints.py:1-81`; `utils/math.py:1-131` + `160-177`; the connect/failover/timeout skeleton `chain_client.py:110-166, 180-195, 197-222, 237-257`, with the lock at line 79 and timeouts at 27-28. Replace `bt.AsyncSubtensor(network=url)` with the v11 `await bt.Subtensor(url)` or the raw JSON-RPC client, and keep the lock, the bounded close and the `return_exceptions=True` gather.

---

## 9. Tests

**Unit (pytest, asyncio, no network; `httpx.MockTransport` / injected async iterators):**
- `test_parse_sse`: fixture `fixtures/sse_stream.txt` with a `full` (129 rows × the frozen field set), 3 `delta`s, a `: keepalive` comment, a multi-line `data:` event, a truncated final event → yields exactly 4 events, ignores the comment, drops the truncated tail.
- `test_table_state_patch`: `delta.changes` for 2 netuids (string keys `"29"`, `"51"`), plus `added` with a new occupant (`network_registered_at` changed). Expect: state updated with int keys; `changed_flags` reports `network_registered_at`; subsequent rows keyed by the new `occupant_key`; `sse_reoccupation_seen` logged once; no registry call.
- `test_ticks_stride_and_flag_override`: stride 5, a delta at block 9,123,663 flipping `emission_enabled` for SN29 → exactly one `chain_ticks` row (SN29) written at that block; at block 9,123,665 → 129 rows.
- `test_hour_flush_last_tick`: one tick per minute from 13:00 to 13:59, then events at 13:59:48 and 14:00:00. Expect: the `chain_hourly` row for `hour_ts = 13:00` equals the 13:59:48 state; 12 `price_bars` 5m rows with `ts_last` = the last tick of each bucket; one 1h `sse_rollup` bar with `open` = the 13:00 tick price, `close`/`ts_last` = the 13:59:48 tick and `high`/`low` = max/min over the hour, which a stored `tmc_candle` bar for 13:00 does not block; a `chain_network_hourly` row with `sum_subnet_prices` = Σ over active rows only. A second flush of the same hour with a later block replaces the row; an earlier block does not.
- `test_backfill_write_gate`: an existing `sse` hourly row is not replaced by `analytics_1w`. A missing hour is filled with `backfilled = 1`, the §3.1 analytics mapping (`emission_percent → emission`, `subnet_tao → tao_liquidity`, `chain_buys → excess_tao`) and NULL `price`/`subnet_moving_price`.
- `test_upsert_daily_precedence` (J1): an `analytics_1w` row is replaced by `analytics_1y` for the same `(netuid, day_ts)`. `analytics_1w` never replaces `analytics_1y`. A second `analytics_1y` row does not overwrite the first. A row with a pre-set `occupant_key` keeps it even when the argument differs. A row with no resolvable key is skipped and counted. 1W hourly points map to `day_ts` only from the first hour of the day.
- `test_upsert_hourly_segments` (J1): 720 `span=1M` rows spanning a reoccupation, called once per segment → each hour carries its segment's key; existing live hours untouched.
- `test_tmc_client_on_limiter` (G1): every `TmcClient` call goes through a stub `Http.request('tmc', …)` (respx), and `BudgetExhausted` from the stub propagates out of `get()`. A `TmcDailyJob` loop that hits it after 10 netuids has committed those 10, and the exception reaches 01's wrapper. `grep -r "providers.ratelimit\|get_bucket\|provider_usage" app/` → empty.
- `test_tmc_client_error_contract`: respx returning 500 (after 01's retries), 404, timeout, invalid JSON → `TmcResponse.ok = False` with `status`/`error` set; 404 not retried; nothing raised except `BudgetExhausted`. `get_paginated` merges `{count, results}` pages, stops at `count`, a short page or `max_pages`, and accepts a bare list.
- `test_rest_snapshot_kinds`: `write_rest_snapshot(db, 'subnets', resp)` → one row, `endpoint='/subnets/'`, `params='limit=200'`, pages merged. `latest_rest_snapshot(db, 'subnets', before=t)` honours `observed_at ≤ t`. Unknown kind → `KeyError`.
- `test_gate_fit_synthetic`: 40 synthetic subnets with known burns (`b_true`) and realised = predict(b_true). The fit recovers `b` within 1e-3 where `b_true < 1`, with `residual_pp < 0.1` and `implied_miner_burn ≈ 1 − b_true`. A subnet with `pgs < 1e-4` → `implied_burn None` → metric `failed/burn_unidentifiable`. The `burn_override` path returns `converged=True`, `burn_source='rpc'` (no undefined `delta`). `|E| = 1` → no fit, `theta_undefined`.
- `test_gate_fit_live_fixture`: the 2026-09-22 research dump (block 9,123,662) is **not** in the design package, so the fixture is captured at build time with `tmc_probe --save-fixture`. Assertions hold for any date: `residual_pp ≤ GATE_FIT_TOLERANCE_PP`; θ within [0.002, 0.02] (UNVERIFIED range around the research's 0.00739); the rank-32 subnet's gate ∈ [0.35, 0.65]; every eligible subnet with realised share 0 and demand ≥ 1 % has implied burn ≥ 0.95. The research values (θ ≈ 0.00739, residual 3.1 pp, SN49 gate ≈ 0.5, SN95 burn ≈ 1) apply to that date only.
- `test_snapshot_as_of_rule`: the as_of 00:00Z snapshot = the `hour_ts = 23:00Z` row. If it is missing, the 22:00Z live row is used (`stale_h = 1`). An `analytics_1w`-only hour yields `emission_share` ok and `gate_margin`/`exit_capacity`/`dereg_safety` `failed/snapshot_backfilled`. With no `chain_hourly` rows at all, the historical tier builds the snapshot from `price_bars` 1h + `chain_daily`: gate and dereg metrics computed, `exit_capacity`/`chain_buys`/`holder_dispersion`/`participation`/`emission_enabled_flag` `failed/snapshot_historical`.
- `test_trending_daily_zero_vs_missing`: a day with 3 successful pulls → rows from the last pull; unlisted active occupants get `views = 0`; `n_pulls_ok = 3`. A day with only failed pulls (no `chain_trending` rows) → no rows.
- `test_sse_unauthorized_fallback`: a 401 on connect → `state='unauthorized'`, fallback polls start at once through `fetch_subnets_table` (respx), the stream is retried only after `TMC_SSE_UNAUTHORIZED_RETRY_SEC`, and `last_attempt` advances on every poll.
- `test_metric_rows_contract` (B3/B4/B9): all rows have `source='chain'`, `stage='raw'`, `value_json.status ∈ {ok, failed, not_applicable}`, and `value` NULL unless ok. The 06:05Z run writes `as_of = 06:00Z`. A second run of the same slot does not replace an `ok` row but upgrades a `failed` one. `put_metric_values` rejects no 05 key (all in `METRIC_KEYS`).
- `test_immunity_local`: SN116 (828,932 blocks) → immune; SN122 (885,580) → not; a not-started subnet with TMC `immune = False` → `immune_local = True`, `immunity_mismatch` not emitted (not started).
- `test_dereg_safety_and_hazard`: a pool of 20 non-immune moving prices → bottom-3 ⇒ 0.0, top ⇒ 1.0, monotone. `emission_enabled = 0` ⇒ hazard 1.0 even when immune. A non-immune occupant with a null moving price → `failed/hazard_unassessable` (not 0.0). The ported function's outputs are unchanged against the old vectors in `E:\SN_Trader\SubnetTrader\tests\test_dereg_risk.py` (present; port them). The `dereg_proximity` components equal the score's terms (`w·proximity + w·starved + w·decline = hazard`).
- `test_holders_concentration`: 100-row fixture (SN51 shape) → `top10_share`, HHI, Gini match hand-computed values; `count > n_fetched` triggers page 2 when allowed.
- `test_net_flow_window`: 30 daily rows → value; 20 rows → `failed/insufficient_history`; occupant 10 d old with 9 rows → computed over W = 10 (≥ 8 required); `total_tao_buy_24h` probe unit `tau` → ×1e9 before dividing by rao depth.
- `test_absence_vs_failure`: table-driven over §4 rows.
- `test_events_transitions`: two consecutive official snapshots → exactly the expected `dereg_risk_entered` and `dereg_proximity` (deduped on the second day by 03's `occ|{d7}`). An SSE flip → one `emission_disabled` (`occ|block_number`), and `chain_features` does not emit a second one for the same flip.
- `test_events_catalog` (D1/D2/B2): every 05 event type is accepted by 03's `emit_event` with the §3.4 payload. `emission_disabled`, `dereg_risk_entered` and `dereg_proximity` are `critical`; `chain_sse_recovered` carries `down_seconds`; `source='chain'`. `gate_model_broken` passes once 03 lists it (xfail until then).
- `test_p1_matrix_coverage`: one occupant with 2 of 10 metrics present ⇒ `P1 = None` (coverage < 0.3); all present ⇒ weighted mean.
- `test_rpc_endpoint_failover`: `EndpointSelector` with a quota error on the primary ⇒ next URL selected, primary retried after cooldown (fake monotonic clock); `is_quota_error("insufficient compute units")` true.

**Integration / smoke (network, marked `@pytest.mark.live`, skipped in CI):**
- A 60-s SSE capture writes tick rows for 129 netuids at ≥ 2 stride windows; the field set equals `data/tmc_table_fields.json`.
- `TmcDailyJob` dry run for netuids `{51, 64, 95}` (3 analytics calls) → hourly rows for the last 7 d present or backfilled; `rate_limits.tmc.month_used` advances by exactly the calls made.
- `run_slot(as_of=today 00:00Z)` → 10 P1 metric rows for SN51 (`emission_share ≈ 11.7` if run on 2026-09-22) and a `gate_fits` row with `residual_pp ≤ 6`.

**Mocked always:** the httpx transport (respx), the clock (`time.monotonic`/`time.time` via an injectable `now()`), 04's registry functions, 03's `emit_event` sink where noted, and `bittensor` (the RPC tests use a fake `sub` object; `bittensor` is not installed in CI). No test sends Telegram.

---

## 10. Verify checklist (Phase 2 done when all pass on the laptop)

1. `python -m scripts.with_bucket --providers tmc -- python -m scripts.tmc_probe --seconds 60 --save-fixture` → prints `full: 129 rows, 44 fields`, `deltas: ≥ 3`, `block source: envelope|row|blocks-stream`, `event_at source: payload|arrival`, `units: tao_liquidity(SN64)=<value> ⇒ rao` (expected ≈ 2e14) and the owner-lock / buy-sell unit checks, and writes `data/tmc_table_fields.json` (commit it). A `401` here means anonymous SSE is refused (§2.1): record it and continue on the REST fallback.
2. `sqlite3 data/taosentinel.db "select count(distinct block_number), count(*) from chain_ticks where observed_at > strftime('%s','now')-600"` → ≥ 8 distinct blocks and ≈ 129 × that many rows (stride 5, ≈ 1 per minute), plus a few off-stride flag-change rows.
3. After one UTC hour: `select netuid, hour_ts, source, backfilled from chain_hourly where hour_ts = <last full hour> limit 3` → 129 rows, `source='sse'`, `backfilled=0`, `marketcap` and `subnet_owner` non-null. `select count(*) from price_bars where period='5m' and source='sse_rollup' and ts >= <hour>` → ≈ 129 × 12. `select count(*) from price_bars where period='1h' and source='sse_rollup' and ts = <last full hour>` → ≈ 129. `select * from chain_network_hourly order by hour_ts desc limit 1` → non-null `sum_subnet_prices`.
4. Kill the network for 4 min (`sudo ip link set <if> down; sleep 240; up`) → `/health.tasks.chain_sse.state` passes through `silent` → `fallback` → `connected`; `reconnects ≥ 1`; `last_attempt` fresh throughout; no systemd restart; no exception in `journalctl -u taosentinel`.
5. `curl -s localhost:8090/health | jq .rate_limits.tmc` → `hour_cap = 500`, `month_cap = 90000`, `tokens ≤ 2`, `hour_used ≤ 160` outside 20:30–23:30Z. After `tmc_daily` (`POST /api/v1/control/run/tmc_daily` or wait for 02:10Z): `select count(*) from chain_daily where day_ts >= strftime('%s','now')-8*86400` → ≈ 128 × 7; `select count(*) from chain_hourly where backfilled=1` → 0 unless the service was down; `select count(*) from chain_trending_daily where day_ts = <yesterday>` → ≈ 128.
6. Stop the service for 2 h, then start it. If the stop spanned 02:10Z, `tmc_daily` runs by itself within 10 min (startup catch-up); otherwise run it via `POST /api/v1/control/run/tmc_daily`. Then `select count(*) from chain_hourly where backfilled=1 and hour_ts > <stop_ts>` → ≈ 128 × 2 with `source='analytics_1w'`. `select count(*) from metric_values where backfilled=1 and as_of=<missed official day>` > 0 when an official slot was missed.
7. `python -m app.features.chain_features run --as-of today` → log line `gate fit: n_eligible=~122 theta=~0.0074 residual_pp=<6 converged=1` (the research values for 2026-09-22; a later date will differ). `select metric_key, value, json_extract(value_json,'$.status'), source from metric_values where occupant_key like '51:%' and as_of=<today 00:00Z> and stage='raw'` → the 10 P1 keys + auxiliaries, all `source='chain'`. SN95's `miner_burn_ok` ≈ 0.0; `dereg_safety` = 0.0 for the three `deregistration_risk` subnets and 1.0 for every `immune_local` occupant.
8. `select count(*) from holders_snapshots where observed_at > strftime('%s','now')-8*86400` → 128 after a full week (≥ 18 after day 1). `select netuid, top10_share, hhi_topn, gini_topn from holders_snapshots order by observed_at desc limit 3` → values in (0, 1].
9. `python -m app.features.chain_features matrix --json | jq '.[0:5]'` → five rows with `netuid, occupant_key, p1, coverage, metrics{}`; without `--json` it prints the ranked table: **the first matrix** (CLI only; HTTP routes are 14's).
10. `select event_type, severity, count(*) from events where observed_at > strftime('%s','now')-86400 and source='chain' group by 1,2` → `dereg_risk_entered`/`emission_disabled` only if a flip happened, and then `critical`; `immunity_mismatch` ≤ 1/occupant/day; no `tmc_schema_drift` after the first run; `alerted_at` is set only by 14's dispatcher.
11. Optional RPC: `SUBTENSOR_RPC_ENABLED=true SUBTENSOR_RPC_ENDPOINTS='["wss://entrypoint-finney.opentensor.ai:443"]' python -m app.collectors.chain_rpc probe` → prints `NetworkImmunityPeriod=864000`, `EmissionGateExponent`, `EmissionBarQuantile`, `SubnetOwnerCut=11796` and `MinerBurned[95]` (≈ 1.0 expected per the research's inference, UNVERIFIED), and writes `chain_constants(source='rpc_storage')`. A `chain_constant_drift` event appears iff a value differs from config.
12. `pytest tests/test_chain_*.py -q` green; `python -m compileall app` clean; `/health.status = "ok"` for 24 h with `tasks.chain_sse.reconnects < 20`; `python -m app check` lists the seven §6.1 ids.

---

## 11. Open questions / operator decisions

1. **θ definition** — 32nd-highest burn-adjusted share (docs) vs. derived from `EmissionBarQuantile = 0.61` (release note). Rank-32 reproduces the 2026-09-22 table to 3.1 pp; the RPC collector stores the live quantile so this can be settled with a fresh tempo once `MinerBurned` is readable. Until then `GATE_BAR_RANK=32`.
2. **Tick stride vs. disk.** *Resolved (F2, master §6 v0.2):* stride 5, 30-day retention (≈ 0.84 GB), pruned by 03's `chain_prune` policy (E3).
3. **Units.** *Mostly resolved by 03's verified 44-field list:* `tao_liquidity`/`fdv` are rao, the α fields are rao-scaled, `marketcap` is τ, and `block_number` is a row field. Still UNVERIFIED and checked by the probe: a payload block timestamp; the unit of analytics `total_tao_buy/sell_24h`; SSE vs analytics owner-lock units; analytics `total_locked` ≡ table `total_locked_alpha`.
4. **`participation` is `failed` (None) in keyless v1** unless 04's `/subnets/` snapshot exposes active counts. Options: `TMC_NEURONS_ENABLED` (≈ 18 req/day), the RPC metagraph pass, or a TaoStats key. Master weight .05, so leaving it None is cheap.
5. **Holder concentration denominator** — include the owner coldkey (locked alpha) and protocol-owned alpha (chain buys) in top-10, or exclude? v1 includes both; excluding needs the owner coldkey from 04 and a rule for protocol accounts (unknown coldkey).
6. **Monthly counter persistence.** *Resolved (G1):* `provider_spend.requests`; `provider_usage` is retired.
7. **New tables.** *Resolved (03 §3.3):* all adopted. `rpc_constants` is merged into `chain_constants`, and `chain_trending_daily` is added. `chain_daily` also holds 1W-derived rows (`analytics_1w`), so `net_flow_30d` never waits a week.
8. **Phase ordering of the shared TMC client.** *Resolved (G2):* `TmcClient` is built at the start of Phase 1. 04 fetches `/subnets/` (6 h) and coldkey identities (02:40Z) through it and writes the snapshots; 05 and 11 read them.
9. **Not-started subnets and TMC `immune`** — bug or intentional? Irrelevant to scoring (local rule wins) but the `immunity_mismatch` event volume will tell; if it fires for started subnets, escalate.
10. **`/market/market-data/` shape** and whether `/subnets/sum-of-sn-prices/` should replace the locally computed Σ price (kept as a cross-check only, 0 requests in v1).
11. **OnFinality key URL form** for WebSocket (`?apikey=` vs path) — checklist B2; verify when the key exists.
12. **Provisional `as_of` rows.** *Resolved (B3/B4):* the slot `as_of` (06/12/18:00Z) under the same `spec_version`; provisional iff `as_of % 86400 != 0`.
13. **Anonymous SSE access** (UNVERIFIED on the laptop's IP): the two research reports disagree (§2.1). The 401 path keeps P1 alive on 10-min table polls; a permanent 401 is an operator decision (C4 TMC key, $49/mo).
14. **Cross-spec items this spec depends on** (not settled by 02):
    - (a) 01 `JobSpec` needs a `startup_catchup` attribute (§6.1; 11 needs it too).
    - (b) 03 §3.9 needs a `gate_model_broken` row (D1).
    - (c) 16 needs `PAUSE_GROUPS` rows `chain` and `tmc` (§5).
    - (d) G2 says `chain_rest_snapshots(kind=…)` but 03's table has no `kind` column. `REST_SNAPSHOT_KINDS` maps kind → `endpoint`; if 03 adds `kind`, `write_rest_snapshot` fills both.
    - (e) `app/util/mathutil.py` is created here in Phase 2, not by 12 in Phase 8. 12 and 13 cite `utils/math.py:1-131`, which omits `gini_coefficient` (160-177).
    - (f) 17 must import `EndpointSelector` from `app/collectors/endpoints.py` (was `app/providers/endpoints.py`).
    - (g) 11 and 17 still pass `priority=` to `TmcClient.get` and 11 still names `./TMC_PAUSED`. Both are retired (G1, B12).
    - 17's open item Q9(c) (a historical snapshot source for replays) is resolved by the §2.3 historical tier.
15. **Trending `entity_id`** is assumed to equal the netuid (UNVERIFIED); the probe prints one pull.

---

## 12. Sources

- `E:\TaoData\specs\02-cross-cutting-conventions.md` (binding: A2, B1–B9, B12 as amended, C7, D1–D4, E1–E4, F1–F2, G1–G3, J1), read 2026-09-23.
- `E:\TaoData\specs\00-MASTER-SPEC.md` v0.2 §2, §4, §5, §6, §7.2–7.6, §9, §11, §12; `INDEX.md`; `18-operator-checklist.md` (A1, B2, C4, C7).
- Sibling specs read for contracts: 01 §2.6 (`CollectorJob`/`LongTask`), §2.7, §2.9 (`/health`), §2.12 (`Http`/`ProviderLimiter`), §5–§6; 03 §2.5–§2.7, §3.3 (chain DDL, precedence ranks), §3.8–§3.9 (catalogs), Appendix A; 04 §2.2, §3.3, §6; 06 §6; 10 §3 (`attention_7d` views); 11 §6 (TMC budget); 12 §2.4 registry rows 1–10 and 55–60, §2.8 flags; 16 §2.5, §2.11–§2.12; 17 §2.4.
- `E:\TaoData\research\02-gapfill-keyless-onchain-pillar-source.md` (all sections: limits, SSE behaviour, 44-field table, gate reproduction 3.1 pp, immunity 864,000, field semantics, history depth, §6 polling plan, open questions).
- `E:\TaoData\research\01-web-chain-apis.md` §2 (SDK v11 reads, endpoints, lite/archive nodes, OnFinality), §3 (dTAO mechanics: gate formula, `SubnetOwnerCut`, tempo, pruning rule).
- `E:\TaoData\research\02-gapfill-data-continuity-and-backfill.md` §2 (node table, storage-key hasher, rate limits, block↔time caveats), §3 (analytics spans/fields, candle endpoints, `pressure` param gotchas).
- `E:\TaoData\research\01-factor-validation-tooling.md` §6 (dereg hazard model, `DeregRiskConfig`, empirics), §0 (pure-stdlib footprint), reusable assets (`utils/math.py`).
- `E:\SN_Trader\SubnetTrader\app\strategy\dereg_risk.py` lines 1-194 (whole file, ported verbatim; config 31-55, `dereg_hazard_score` 127-174, short-circuit 155-156; verified 2026-09-23) and `tests\test_dereg_risk.py` (present; vectors ported).
- `E:\SN_Trader\SubnetTrader\app\chain\endpoints.py` lines 1-81 (whole file, copied verbatim; verified).
- `E:\SN_Trader\SubnetTrader\app\data\chain_client.py` lines 27-28 (timeouts 45/30), 79 (`_sub_lock`), 110-166 (connect loop, failover), 180-195 (`_discard_subtensor` bounded close), 197-222 (bounded gather + socket drop), 237-257 (`SubnetEmissionEnabled` query_map, fail-open) (verified).
- `E:\SN_Trader\SubnetTrader\app\utils\math.py` lines 1-131 and 160-177 (`gini_coefficient`) (verified).
- `E:\SN_Trader\SubnetTrader\app\config.py` lines 619-625 (`SUBTENSOR_FALLBACK_ENABLED` 624, `SUBTENSOR_ENDPOINT_COOLDOWN_SEC` 625) and 672-682 (`subtensor_endpoints()` ordering convention) (verified).

---

## Changelog
- 2026-09-24 final pass: added the live 1h `price_bars` rollup (`source='sse_rollup'`, `_rollup_1h` at every hour flush from stride-5 `chain_ticks`, with a 24-h catch-up; 03's CHECK already allows it) so that 13 has a live hourly series. Weekly `tmc_candle` bars now only fill gaps.
- 2026-09-24 final pass: named 17's analytics snapshot kind `analytics_subnet`: `endpoint='/analytics/subnet/{n}/'` with the literal netuid, `params='span=1Y'|'span=1M'`.
- 2026-09-23 reconciliation + review:
  - G1/G2: `TmcClient` now lives in `app/collectors/chain_tmc.py` on 01's `Http`/`ProviderLimiter`. Retired `app/providers/ratelimit.py`, `get_bucket`, `provider_usage` and the priority queue; `BudgetExhausted` propagates. 04 fetches `/subnets/` and coldkey identities; snapshot kinds map to 03's `endpoint` column.
  - G3: §6.3 budget table covers every caller (04/05/06/11/17): ≈ 716/day steady, ≈ 1.35 k/day worst, peak ≈ 311/h; all under 8/min, 500/h and 90 k/month.
  - F2: burst 2 (+ sum validator), hourly cap 500, stride 5, tick retention 30 d. `SSE_*` renamed `TMC_SSE_*`. Retired `TMC_PAUSE_SENTINEL`, `TMC_USER_AGENT`, `GATE_SNAPSHOT_HOUR_UTC`, `DEREG_ALERT_ENABLED`.
  - A2/E1–E4: one jobs + LongTask table (§6.1) replaces 01's placeholders. No `chain_prune` job (03 policy). Startup catch-up requested from 01. Liveness per 16 (900 s on `max(last_beat, last_attempt)`).
  - B7: shipped DDL replaced by write maps onto 03's tables: 44 named `chain_hourly` columns + `extras_json`, rao scaling, `marketcap`, `rpc_constants` merged, `chain_trending_daily` added.
  - B1–B4/B9: slot `as_of`; `put_metric_values` (first success wins); `source='chain'`; status inside `value_json`; no `absent` for chain zeros.
  - B2/D1–D4: `high` → `critical`; `chain_sse_recovered` gains `down_seconds`; 03 dedupe keys; 05 sends no Telegram; watchlist alerts are 14's `dereg_watch`; `gate_model_broken` needs a 03 catalog row.
  - C7/J1/B12: `participation` = counts ÷ max_uids only, `vtrust_dispersion` is evidence; `upsert_daily_from_analytics` precedence + pre-set `occupant_key`; per-segment hourly upserts; `price_bars` `ts_last` rule; pauses by exact id or `PAUSE_GROUPS`.
  - Package applied: TMC_RATE_BURST, TMC_HOURLY_CAP, limiter module, 1Y precedence, `chain_sse_recovered` payload, severities, `miner_burn_zeroed` `> 0.8`, `owner_conviction` → `owner_commitment`, all 5 reviewer notes. None rejected.
  - Review fixes: slot snapshot = hour `as_of − 3600` (the 00:00Z hour row does not exist at 00:05Z); `fit_gate` NameError/IndexError guards; SSE 401/403 path; the non-existent 04 `observe_occupant` hook removed; market-cap fallback ×1e9 unit bug; `net_flow_30d` window and units; stride rule robust to skipped blocks.
  - Review fixes: `dereg_proximity` components recomputed (the score returns only the total); `emission_disabled` single emitter; P1 HTTP route dropped (A2: 14 owns routes, CLI only).
  - Citations corrected: `dereg_risk.py` 1-194, `endpoints.py` 1-81, `math.py` `gini_coefficient` 160-177 (outside the cited 1-131), `chain_client.py` 180-195 added, `config.py` 619-625. The live-fixture test no longer depends on a research dump that does not exist.
- 2026-10-22: `rpc_subnet_daily` gains a second writer, 17's `--emission-daily` backfill (day rows with `emission_share`, migration 0005; 20 N2 item 3). The live collector's rows are unchanged.
- 2026-10-22 (20 N2 item 2, BUILD-LOG 2026-10-22): §2.3 tier 3 reads emission from `v_quant_chain_daily` (PIT by `ts ≤ as_of`) and the `block_times` anchor by `ts_ms`; θ ≤ 0 → gate keys `failed/theta_undefined`; `gate_fit_degraded`/`gate_model_broken` from a `backfilled=1` run carry `payload.backfilled` and a preset `alerted_at` (17 §2.11 contract: a replay never pages; the built `gate_model_broken` path did not preset it).
