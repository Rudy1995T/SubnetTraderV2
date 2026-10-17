# [code] current-state-audit

**Topic:** TaoSentinel current-state audit on the laptop, 2026-10-09 (≈ 14:10–14:40Z): data-source/RPC status, whether the service runs, exactly what chain history is saved, what backtests/results exist, what it takes to resume collection, and the 17 available vs 36 unavailable jobs. Groundwork for the "subnet investment analyst" spec + roadmap.

**Method.** Read-only: code and specs read with line numbers; the SQLite DB opened as `file:…?mode=ro` (indexed/limited queries only); `.env` inspected for key *names* only. Commands actually run: `.venv/bin/python -m app check` (asked for by the task), `systemctl` / `ps` / `timedatectl` / `df`, a 2-packet `ping` plus TCP connect tests to the Pi named in checklist A4 (`192.168.0.62`, ports 22 and 8081), and `grep`/`sed` over the repo.

**Side effects of this audit (disclosed).** `python -m app check` appended one INFO line to `data/logs/2026-10-09.jsonl` (14:22:28Z, "jobs unavailable … count 36") and touched `data/taosentinel.db-wal` (0 bytes); no rows were added (`collector_runs`, `events`, `job_state._ops/boots` unchanged — verified by read-only counts). Read-only connections touched `taosentinel.db-shm`. The file written: this report only. Earlier today, *before* this audit, another process (the orchestrator's probe) wrote 3 `block_times` rows (`source='probe'`, `observed_at` 1791555371 = 14:16:11Z), `data/backfill/rpc_probe.json`, `data/backfill/backfill.lock` (pid 4948, no longer running; flock released at exit) and two log lines (14:14:23Z, 14:16:02Z).

---

## 0. Status table

| Area | State on 2026-10-09 | Evidence |
|---|---|---|
| Service (`python -m app serve`) | **Not running. Never installed under systemd.** Last live run 2026-09-24 (two boots: 16:00:32Z first_boot, 19:04:21Z clean; last in-service job 19:04Z). | `systemctl list-unit-files` → no `taosentinel*`; `/etc/systemd/system` has only default dirs; `ps` → no `app serve`; `job_state('_ops','boots')` = 2 entries, both 2026-09-24; `collector_runs` max for ops jobs 1790276666 (19:04:26Z); stale `data/run/taosentinel.lock` pid 88499 (dead) |
| Blockmachine archive RPC (paid, Standard) | **Key SET, probe PASS today** (14:16:11Z): head block 9,246,056, runtime `spec_version` 475, 129 entries, golden checks all pass, 45 requests. Used **only by standalone backfill/repair scripts**, never by the service. | `data/backfill/rpc_probe.json`; `app/config.py:104-109`; `app/backfill/rpc_client.py:8-9,123-149`; `scripts/backfill_rpc.py:9` |
| TaoMarketCap (TMC) keyless API + SSE | **Primary live source, keyless, enabled** (`TMC_ENABLED`/`TMC_SSE_ENABLED` True, no key). Budget 8/min service + 2/min scripts = 10/min, matching the `X-RateLimit-Limit: 10` the orchestrator observed today (129 rows from `/subnets/table/`). Live SSE recorder ran for ≈ 15 s total on the real DB. | `app/config.py:164-184,659,824-825`; `app/collectors/chain_tmc.py:50-170,413-414`; `app/http.py:49-50`; `chain_ticks` 258 rows all at 2026-09-24T19:04:23–38Z |
| TaoStats | Key SET and valid (BUILD-LOG Phase 3), **but `TAOSTATS_ENABLED` resolves False → `taostats_active()` False; no built job uses it** (only Phase 7 `owner_collect_daily` lists it). | resolved Settings print; `app/config.py:110-115,992-993`; `app/jobs.py:259`; `app/http.py:53-54` |
| OnFinality | Key EMPTY; public endpoint is failover #2 behind Blockmachine (used for 222 `pool_history` rows on 2025-05-21). | `app/backfill/rpc_client.py:143-149`; `pool_history GROUP BY source` |
| Public subtensor RPC (`entrypoint-finney.opentensor.ai`) | Used by the registry identity job for `SubnetIdentitiesV3` (121 keys at block 9,138,746 on 09-24). `SUBTENSOR_RPC_ENABLED` False → the optional RPC collector jobs are available but disabled. | `app/config.py:141,231`; `app/registry/sources/chain_rpc.py:4,98-161`; `data/seed_registry_report.json` |
| Saved chain history | **Hourly reserves/price/moving-price for every occupant 2025-02-13T21:59Z → 2026-09-24T17:59Z** (1,711,230 `pool_history` rows, 129 netuids); 1h/1d bars 2025-05-19 → 2026-09-24; 5m TMC candles 2026-06-19 → 2026-09-24T21:40Z; daily analytics 2025-09-24 → 2026-09-24 (128 netuids); hourly analytics only 2026-08-25 → 2026-09-24. **Nothing after 2026-09-24.** | §3 queries |
| Backtests / scores / results | **None.** `metric_values`, `scores`, `score_runs`, `regime_daily`, `rank_stability_daily`, `gate_fits` all 0 rows; `data/quant/` holds only `coverage.json`; no `data/quant/results/`, no `data/backtest/`. | §4 |
| Old bot (SubnetTrader) | Code + ledger on the Windows PC (`E:\SN_Trader`), **not copied** (`seed/` has only `README.example`). PC ledger ends 2026-06-19T21:24Z; Pi (`192.168.0.62`) ran live with real capital ≥ 2026-07-08 with 10 OPEN 1-τ positions; **Pi unreachable from this laptop today** (ping 100 % loss, tcp/22 and tcp/8081 closed/unreachable from 192.168.0.44/24). Whether it is still running or holds alpha: UNVERIFIED. | `seed/`; `data/backfill/pi_recovery_report.json` (`recovered: false`); `research/02-gapfill-data-continuity-and-backfill.md:9-12,78`; `specs/18-operator-checklist.md:16` |
| Sentiment tool | Old repo: designed (two specs, 2026-06-19), **never implemented**. TaoSentinel: X/LLM = Phase 9, **unbuilt**; `LLM_ENABLED` False, `X_ENABLED` False, `X_PROVIDER null`, `ANTHROPIC_API_KEY`/`TWITTERAPI_IO_KEY` EMPTY. | `research/01-prior-sentiment-design.md:7-9`; `app/jobs.py:289-317`; resolved Settings |
| Jobs | 53 declared; **17 available (phase ≤ 3), 36 unavailable** (`python -m app check` re-run today: `schema_version=2`, `quick_check=ok`). | §6 |
| Tests | 179 passed on 2026-09-24 per BUILD-LOG (167 `def test_` statically today; not re-run to avoid writing `.pytest_cache`). | `specs/BUILD-LOG.md` Phase 3 "Final results"; `tests/conftest.py:150` `BUILT_PHASE = 3` |
| Host prerequisites still open | Timezone `Europe/London` (spec wants UTC); `sqlite3` CLI not installed; systemd unit not installed; `data/backups/` **empty** (no backup of the repaired 1.5 GB DB; only the pre-repair copy `data/tmp/pre_price_repair_2026-09-25.db`). Disk 219 GB free. | `timedatectl show`; `which sqlite3`; `ls data/backups`; `df` |

---

## 1. Data sources and keys — how the app actually uses each

### 1.1 `.env` key status (names only; `grep -v '^#' .env | awk -F= '{print $1, (length($2)>0?"SET":"EMPTY")}'`)

| Key | State | Used by (built code) |
|---|---|---|
| `BLOCKMACHINE_API_kEY` (sic, lower-case k; accepted — settings are case-insensitive, `app/config.py:105`) | SET | backfill/repair scripts only (`rpc_client.py:143-144`) |
| `TAOSTATS_API_KEY` | SET | header only (`app/http.py:53-54`); inert while `TAOSTATS_ENABLED` False |
| `TAOSTATS_ENABLED`, `LLM_ENABLED`, `X_ENABLED`, `X_PROVIDER`, `LLM_MONTHLY_CAP_USD`, `X_MONTHLY_CAP_USD`, `DISCORD_CHANNEL_MAP_REVIEWED` | SET (toggles; resolve to False/False/False/`null`/60.0/…) | — |
| `API_AUTH_TOKEN`, `HTTP_USER_AGENT`, `DISCORD_USER_AGENT` | SET | control API auth (`scripts/backfill_tmc.py:75`), HTTP UA |
| `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` | EMPTY | alerts latch to DB only (`alert_dispatch` ran with `sent=0 digest=198`) |
| `ONFINALITY_API_KEY` | EMPTY | keyed failover not configured; public endpoint still in the chain |
| `GITHUB_TOKEN` | EMPTY | Phase 4 (unbuilt) |
| `DISCORD_BOT_TOKEN`, `DISCORD_AGGREGATOR_GUILD_ID` | EMPTY | Phase 6 (unbuilt) |
| `ANTHROPIC_API_KEY`, `TWITTERAPI_IO_KEY` | EMPTY | Phase 9 (unbuilt) |
| `PI_TRADER_COLDKEY` | EMPTY | `backfill_tmc --tasks positions` (checks the Pi's open positions keylessly) cannot run |
| `DB_BACKUP_MIRROR_DIR` | EMPTY | no off-disk backup mirror |

Stale comment: `.env` line 34 still says "Blockmachine: the old key returned HTTP 401 and the design does not need it" — superseded by BUILD-LOG Phase 3 item 1 (Blockmachine is now the primary archive endpoint; the operator renewed it 2026-09-24).

### 1.2 Blockmachine archive RPC (`wss://rpc.blockmachine.io`)
- Settings: `BLOCKMACHINE_API_KEY/_WS_URL/_RATE_PER_SEC (12)/_MONTHLY_CAP_REQ (4,500,000)` — `app/config.py:104-109`; validator 0.1–16 req/s (`:917-918`). Limiter row `blockmachine` 12/s, burst 8, monthly cap 4.5 M (`app/ratelimit.py:107`).
- Endpoint order (`app/backfill/rpc_client.py:8-9,137-149`): Blockmachine (if keyed) → keyed OnFinality → public OnFinality → `wss://archive.chain.opentensor.ai:443`. The key rides in `?authorization=` (`:126-129`); logs/DB rows carry only the label `blockmachine`.
- Consumers: `scripts/backfill_rpc.py` (`--probe`, `--era gap|ledger|pre_ledger`, `--post-only`), `scripts/repair_rpc_prices.py`, and `post_process` occupancy reconstruction. **The 24/7 service does not use it** (`scripts/backfill_rpc.py:9`); the in-service optional collector `chain_rpc_hourly` uses `SUBTENSOR_RPC_ENDPOINTS` and is off (`app/config.py:231-234`).
- Probe today (`data/backfill/rpc_probe.json`): `pass: true` at 2026-10-09T14:16:11Z; golden block 8,443,757 rel. err 0.0 on reserves/volume/moving_price, `price_swap_sqrt` 1.1e-8; dTAO launch 4,920,351 (65 subnets) ok; pre-dTAO `no_runtime_api` (expected); **head 9,246,056, spec_version 475** (was 470 on 09-24 → a new runtime; the gateway loads metadata per `(endpoint, version)` automatically, `rpc_client.py:377-388`); 5 socket requests = 5 tokens per bucket; median latency 43 ms storage / 303 ms runtime call. Chain pace since 09-24: (9,246,056 − 9,139,183) blocks over ≈ 14.85 d ≈ 12.0 s/block.
- Spend so far: ≈ 75 k requests for the three eras (BUILD-LOG) + 1,208-block repair + 45 today — far under the 4.5 M/month cap.

### 1.3 TaoMarketCap keyless API + SSE (`https://api.taomarketcap.com/public/v1`)
- Settings `TMC_*` at `app/config.py:164-208`: REST 8/min burst 2, hourly cap 500, monthly 90,000; SSE table stream `…/sse/subnets/table/` with ≤ 2 connections; scripts get 2/min (`SCRIPT_TMC_RATE_PER_MIN`, `:659`); the validator enforces service + script ≤ 10/min (`:824-825`), i.e. exactly the anonymous `X-RateLimit-Limit: 10` the orchestrator saw today. `Authorization` is sent only when `TMC_API_KEY` is set (`app/http.py:49-50`) — it is not.
- Client `TmcClient` (`app/collectors/chain_tmc.py:50`): `/subnets/` (`:114`), `/subnets/table/` (`:117`), `/subnets/{n}/candle-chart-compact/` (`:133`), pressure (`:143-144`), trending (`:150`), SSE (`:156-170`). REST snapshot kinds `subnets`, `subnets_table`, `burn`, `network_lock_cost`, `validators`, `neurons` (`:178-185`).
- Jobs: `chain_sse` LongTask (`:413-414`, → `chain_ticks`/`chain_hourly`/`price_bars`/`chain_network_hourly`), `tmc_hourly`/`tmc_daily`/`tmc_weekly` (`app/collectors/chain_tmc_rest.py:1-2,212,262,374`), `backfill_tmc` (`app/backfill/tmc_backfill.py:204`). Registry occupants come from `/subnets/` with `/subnets/table/` fallback (`app/registry/sources/tmc.py:76-77`).
- Known limits recorded in BUILD-LOG Phase 2: live deltas carry no block number (recorder estimates 12 s/block between `full` events); `/subnets/` has no active validator/miner counts; analytics only covers the current occupant.
- Spend booked: `provider_spend` has one row `tmc 2026-09 … 384 requests`.

### 1.4 TaoStats, OnFinality, subnetalpha, public RPC
- TaoStats: `TAOSTATS_BASE_URL https://api.taostats.io`, 5/min, 10 k/month cap (`app/config.py:110-115`); `taostats_active()` requires `TAOSTATS_ENABLED` (`:992-993`) — False → unused. BUILD-LOG Phase 3 recorded the key as valid (`GET /api/dtao/pool/history/v1?netuid=64&frequency=by_hour` → 14,064 hourly rows).
- subnetalpha.ai WP REST (`:146-150`): identity source #2 (128 records, 2 pages on 09-24).
- Public RPC `SUBTENSOR_RPC_URL https://entrypoint-finney.opentensor.ai` (`:141`): identity source #1 (`SubnetIdentitiesV3`, Blake2_128Concat keys, `app/registry/sources/chain_rpc.py:4`).

---

## 2. Is the service running? No.

- No systemd unit installed (system or user scope); `ops/systemd/taosentinel.service` + `ops/install-systemd.sh` exist but were never applied (BUILD-LOG Phase 0 "Operator actions still open": `sudo ops/install-systemd.sh && sudo systemctl start taosentinel`). `make status` would print nothing useful; `data/service.log` does not exist.
- Last service activity: boots at 2026-09-24T16:00:32Z (`first_boot`) and 19:04:21Z (`clean`), per `job_state('_ops','boots')`; in-service jobs last ran 19:04:26Z (`ops_guard_5m`, `cost_ledger_hourly`, `alert_dispatch`); the SSE recorder wrote its only 258 `chain_ticks` at 19:04:23–38Z. Everything later on 09-24/25 was standalone scripts (`backfill_rpc:*`, `backfill_tmc`, `repair_rpc_prices` finishing 23:29Z), see `collector_runs`.
- Logs: `data/logs/2026-09-24.jsonl` (last line 2026-09-24T23:25:01Z), then nothing until today's probe lines. Host was rebooted today at 15:04 local (`uptime -s`).
- Stale lock: `data/run/taosentinel.lock` = pid 88499 (dead). The runtime treats a dead-pid lock as stale on boot (UNVERIFIED — not exercised today; delete it if `app serve` refuses to start).

---

## 3. Exactly what history is saved (read-only queries, 2026-10-09)

DB `data/taosentinel.db` 1,548,713,984 bytes, schema v2 (`schema_version` rows `1|baseline`, `2|price_bars_blockmachine`), 80 tables, 6 views. All times UTC.

### 3.1 Raw hourly chain cross-sections — `pool_history`
| metric | value |
|---|---|
| rows / netuids | 1,711,230 / 129 (`SELECT COUNT(*)`, `COUNT(DISTINCT netuid)`) |
| span | `MIN(timestamp)` 1739483940 = **2025-02-13T21:59Z** (dTAO launch) → `MAX(timestamp)` 1790272740 = **2026-09-24T17:59Z** |
| by source | `blockmachine` 1,711,008 (same span); `onfinality` 222 (2025-05-21T20:38–22:04Z, chain-halt failover) |
| per subnet | SN1/SN64/SN8 each 14,086 hourly rows ending 2026-09-24T17:59Z; SN120 11,313 |
| price method | `swap_sqrt` 1,083,213 / `reserve_ratio` 628,017 (147,580 rows repaired 2026-09-25, BUILD-LOG #17) |
| occupant stamping | `occupant_key` NULL rows 0 (`coverage.json`) |

Eras (`backfill_buckets`): `pre_ledger` 5,283 done + 23 skipped (2025-05-20T23:00 → 05-22T12:00Z chain halt), 2025-02-13T21:00 → 2025-09-22T22:00Z; `ledger` 6,479 done, 2025-09-22T23:00 → 2026-06-19T21:00Z; `gap` 2,324 done, 2026-06-19T22:00 → **2026-09-24T17:00Z**; 0 failed anywhere. `block_times` 14,931 rows, blocks 1,411,451 → 9,139,183 (+3 probe anchors today).

### 3.2 Bars — `price_bars` (`GROUP BY period, source`)
| period | source | rows | first | last |
|---|---|---|---|---|
| 1h | blockmachine | 1,218,708 | 2025-05-19T00:00Z | 2026-09-24T17:00Z |
| 1h | onfinality | 222 | 2025-05-21T21:00Z | 2025-05-21T23:00Z |
| 1h | tmc_candle | 298,189 | 2026-06-19T20:00Z | 2026-09-24T21:00Z |
| 1d | blockmachine | 50,813 | 2025-05-19 | 2026-09-24 |
| 1d | onfinality | 111 | 2025-05-21 | 2025-05-21 |
| 1d | tmc_candle | 12,416 | 2026-06-20 | 2026-09-24 |
| 5m | tmc_candle | 3,567,624 | 2026-06-19T20:20Z | 2026-09-24T21:40Z |

**Unreported gap found:** bars begin **2025-05-19**, but `pool_history` begins 2025-02-13 — e.g. SN64 has 2,259 hourly rows before 2025-05-19 and **0** 1h bars there (query on `occupant_key='64:4531295'`). Cause (inferred, UNVERIFIED): the `pre_ledger` era ran twice (first run `ok=0` at 1790283868; second run `planned=2857 done=2837`) and `post_process` rebuilds bars only over the *second* run's `[from_ts-1d, to_ts+1d]` (`scripts/backfill_rpc.py:127,51`), so Feb–May 2025 was never barred. `coverage.json.completeness_by_month` starts at `2025-05` and therefore hides it. Fix: `python -m scripts.backfill_rpc --post-only` (rebuilds occupancy/stamps/bars over MIN..MAX of all RPC rows, `scripts/backfill_rpc.py:46-52,102-111`).

### 3.3 TMC analytics — `chain_hourly`, `chain_daily`
- `chain_daily`: 38,616 rows, 128 netuids, all `analytics_1y`, **2025-09-24 → 2026-09-24** (daily `emission_percent`, `subnet_tao`, buys/sells, `chain_buys`, owner locks…). 0 unattributed.
- `chain_hourly`: 90,799 rows, all `analytics_1m`, **2026-08-25T20:00 → 2026-09-24T21:00Z** (30-day window). `sse`/`rest_table`/`tick_rollup`/`analytics_1w` = 0 rows → **no live-captured hourly rows at all**; `coverage.json.eras.live.from_ts` is `null`. Columns `commits_7d/30d/90d` exist but 0 non-NULL (they come only from the live table stream).
- `chain_ticks` 258 rows (15 s of SSE on 09-24); `chain_rest_snapshots` 258; `holders_snapshots`, `chain_network_hourly`, `chain_pressure`, `chain_trending*`, `gate_fits`, `gate_subnet_daily` all 0.

### 3.4 Occupancy, owners, identity, events
- `occupants`: 180 rows over 129 netuids — 126 `active` + 3 `pre_launch` (source `tmc`), 51 `deregistered` (source `rpc`, reconstructed); `registered_ts` 1696355772 (2023-10-03) → 1789937496 (2026-09-21); 51 re-occupations listed in `coverage.json.occupancy.reoccupations` (netuids 15, 16, 26, 31, 35, 36×2, 38, 40, 47, 49, 57, 58, 59, 67, 69, 70×2, 73, 76×2, 78, 80, 82, 84, 86×2, 87, 90×2, 91, 92×2, 94, 96, 97, 99×2, 100, 102, 103, 105, 107, 108×2, 109, 113, 114, 116, 122, 126).
- `rpc_owner_history`: 2,935 rows, blocks 4,920,432 → 9,118,490, 129 netuids; changes: `first` 1,193, `subnet_name` 1,138, `owner_coldkey` 289, `owner_hotkey` 170, `subnet_name,token_symbol` 64, `token_symbol` 46, mixed 35 (duplicates possible per BUILD-LOG #9).
- Identity: `identity_observations` 5,336, `identity_resolved` 2,064 (16 fields × 129), `identity_raw_snapshots` 336; `subnets` 129 with website 115, github 116, docs 1, discord 97, telegram 7, x 104.
- `events`: 255 — `identity_junk` 130, `source_disagreement` 68, `netuid_reoccupied` 51, `backfill_done` 6; observed 2026-09-24T16:32–21:51Z.
- `collector_runs`: 19 rows, last `repair_rpc_prices` 2026-09-24T23:29Z. `control_state` empty (no pause persists). `provider_spend`: tmc 384 req in 2026-09.

### 3.5 What is **not** saved
No live SSE history; no holders; no TMC `price_after`/exit-capacity history; no GitHub (`github_*` 0), web (`web_checks` 0), Discord/Telegram/X/LLM/news/team/owner-analytics rows; no `whale_*` rows; no imported old-bot ledger (`source LIKE 'ledger_%'` = 0 rows — era bounds fall back to the verified PC span, `app/backfill/rpc_backfill.py:28-29,66-71`). **Data ends 2026-09-24** (17:59Z reserves/moving price; 21:40Z 5m candles; 21:00Z hourly analytics; day 2026-09-24 daily analytics). Nothing has been collected since; today's probe added only 3 `block_times` anchors.

---

## 4. Backtests / results on this laptop: none

- `data/quant/` contains only `coverage.json` (generated 1790292683 = 2026-09-25T00:31Z). No `data/quant/results/`, `data/quant/snapshots/`, or `data/backtest/`.
- Scoring/validation tables are empty: `metric_values` 0, `scores` 0, `score_runs` 0, `regime_daily` 0, `rank_stability_daily` 0, `red_flag_state` 0, `gate_fits` 0. The "first P1 matrix" quoted in BUILD-LOG Phase 2 was computed on a scratch DB from the live table, not persisted here.
- The old bot's ≈ 90 result artefacts (`data/backtest/results/factor_live_*`, `factor_survivorship_*`, `quality_overlay_2026-06-02.md`, …) and its quant stack (`app/backtest/factor_backtest.py`, `walk_forward.py`, `validation.py`) live on the Windows PC (`research/01-factor-validation-tooling.md:70,79-83`) and were **not copied** (checklist A3, `seed/README.example`).
- What would produce results here:
  - **Spec 05 / Phase 2 (built):** `python -m app.features.chain_features run [--as-of …]` writes P1 raw `metric_values` + `gate_fits` for one slot, falling back to `price_bars` 1h + `chain_daily` when `chain_hourly` is empty (`app/features/chain_features.py:81-108,536-609`); `… matrix` renders the P1 ranking (`:694-735`, `app/scoring/p1_matrix.py`). Never run on the real DB; UNVERIFIED that the fallback path works on backfilled rows (not executed — it writes).
  - **Spec 12 / Phase 8 (unbuilt):** `features_hourly` + `scoring_6h` → `metric_values(stage='raw'|'norm')`, `scores`, `regime_daily`, `rank_stability_daily`, `score_runs` (`specs/12-feature-store-and-scoring-engine.md:818-836`), plus spec 17's S10 `replay_history` that back-computes `backfilled=1` scores over the saved history (BUILD-LOG Phase 3 header: "Phase 8 — not built"); S7 regime backfill is guarded until the old `app/strategy/regime.py` is ported.
  - **Spec 13 / Phase 10 (unbuilt):** the ported quant stack + PIT panel builder + gate; results as files `data/quant/results/factor_<scope>_<ts>.json`, `ic_series_*`, `walk_forward_*`, `survival_*`, `rank_stability_*`, `gate_verdict_<factor>_<ts>.json`, `trials.json` (`specs/13-validation-harness.md:179-192`); weekly job `quant_weekly` (Sun 05:00Z, `app/jobs.py:319-321`). Its backtestability matrix (`:93-129`) says which keys can be reconstructed from the saved history (P1 `emission_share`, `gate_margin`, `dereg_safety`, `liquidity_depth`, `net_flow_30d`, `tenure`; P2 once GitHub history exists) and which are forward-only (holders, participation, all P3/P5/P6, most P4).
- Memory of prior evidence (from the old repo, `research/01-factor-validation-tooling.md:53`): on the 8-month panel `age` (+4.0, +5.6 size-controlled) was the cleanest axis; `meanrev_z` t −4.9; nothing significant on the 6-week live panel; the quality overlay collapsed under size control. Nothing of this is reproducible on the laptop until Phases 8 + 10 exist.

---

## 5. Old trading bot and sentiment tool — status as knowable from here

- **SubnetTrader (old bot).** Source at `E:\SN_Trader\SubnetTrader` (Windows PC); PC ledger `pool_history` 7,362,854 rows ending 2026-06-19T21:24:34Z (block 8,443,757); 10 OPEN 1-τ `value` positions (netuids 31, 47, 67, 94, 99, 108, 109, 113, 116, 122); the Pi (`pi@192.168.0.62`) kept running live (≥ 2026-07-08, ≈ 9.26 M rows) and its ledger/wallet are unrecovered (`research/02-gapfill-data-continuity-and-backfill.md:9-12,78`; `specs/18-operator-checklist.md:16`). Today the Pi does not answer ping, SSH (22) or the bot API (8081) from this laptop on the same /24 — it is off, moved, or firewalled (UNVERIFIED which). `data/backfill/pi_recovery_report.json`: `recovered: false`, `positions: []`, `coldkey: ""`. Nothing of the bot runs on this laptop; TaoSentinel's bot feed (spec 15, Phase 12) is unbuilt. The bot's ≈ 1-minute capture loop is the only thing that ever produced sub-5-minute reserve data; TaoSentinel's live equivalent is the TMC SSE recorder (not running).
- **Sentiment analysis tool.** The old repo only had two `proposed` specs (2026-06-19) and no code (`research/01-prior-sentiment-design.md:7-9`). In TaoSentinel it is Phase 9 (`x_*`, `rss_*`, `news_google_poll`, `youtube_poll`, `llm_*`; `app/jobs.py:289-317`), unbuilt, with `X_PROVIDER=null`, `LLM_ENABLED=False`, and both paid keys empty. No `social_posts`, `post_labels`, `llm_batches`, `news_items` rows exist.

---

## 6. Jobs: 17 available vs 36 unavailable (`python -m app check`, 2026-10-09T14:22Z → `OK jobs=53 declared (50 jobs + 3 LongTasks), 17 available (phase 3), 36 unavailable`)

Available (`tests/conftest.py:150` `BUILT_PHASE = 3`; table at `app/jobs.py:155-234`):
- Phase 0 (7): `log_retention_daily`, `storage_retention_daily`, `backup_daily`, `ops_guard_5m`, `ops_digest_daily`, `cost_ledger_hourly`, `alert_dispatch`
- Phase 1 (2): `registry_occupants` (every 360 min), `registry_identity` (02:40Z)
- Phase 2 (7): `chain_sse` (LongTask, continuous), `tmc_hourly` (:07), `tmc_daily` (02:10Z), `tmc_weekly` (01:10Z, 1/7 of netuids), `chain_features` (00/06/12/18:05Z), `chain_rpc_hourly`, `chain_rpc_metagraph` (both gated by `SUBTENSOR_RPC_ENABLED`=False → available but disabled; the metagraph decoder is a stub, `app/collectors/chain_rpc.py:168-172`)
- Phase 3 (1): `backfill_tmc` (manual)

Unavailable, by phase (`by_phase {"4":6,"5":3,"6":4,"7":5,"8":2,"9":12,"10":1,"11":3}`, `app/jobs.py:236-333`):
- 4 GitHub (6): `github_resolve`, `github_org_enum`, `github_activity`, `github_metrics`, `github_html_probe`, `backfill_github`
- 5 Web (3): `web_check`, `web_metrics`, `web_spn2`
- 6 Community (4): `discord_6h`, `discord_widget_1h`, `telegram_daily`, `discord_gateway`
- 7 Team/owner integrity (5): `owner_collect_daily`, `owner_watch_hourly`, `team_seed_daily`, `integrity_features`, `owner_rpc_seed`
- 8 Scoring (2): `features_hourly`, `scoring_6h`
- 9 X/LLM/RSS (12): `x_team_poll`, `x_kol_poll`, `x_mentions_poll`, `x_profile_refresh`, `x_ctf_crosscheck`, `rss_feeds_poll`, `news_google_poll`, `youtube_poll`, `llm_classify_submit`, `llm_batch_poll`, `llm_rules_fallback`, `llm_narrative_weekly`
- 10 Validation (1): `quant_weekly`
- 11 API/Telegram/UI (3): `dereg_watch`, `needs_review_digest`, `telegram_bot`

---

## 7. Resuming collection — what is needed and the exact commands

### 7.1 How the gap bounds are computed (so re-running does extend to today)
- `era_range('gap')` = `[ledger_hi_hour + 1h, (now//3600 − 1)·3600)` where `ledger_hi` falls back to the PC-ledger end 1781904274 when no ledger is imported (`app/backfill/rpc_backfill.py:28-29,66-86`). `plan_buckets` drops buckets already `done`/`skipped` (`:89-108`). So `--era gap` today plans ≈ **356 new hourly buckets (2026-09-24T18:00Z → the hour before now)**, ≈ 5 Blockmachine requests each (≈ 1.8 k requests, ≈ 3 min at the measured ≈ 2 buckets/s), then `post_process` reconstructs occupancy, stamps rows and rebuilds 1h/1d bars over that range (`scripts/backfill_rpc.py:24-60,126-128`). The run refuses without a passing probe for the endpoint (`rpc_backfill.py:653-656`) — today's probe satisfies that. Lock: `data/backfill/backfill.lock` (flock, `app/backfill/common.py:68-94`); the service may run concurrently (it never touches the archive RPC).
- `coverage.json` marks `gap` as `partial` precisely because, with no live SSE rows (`live.from_ts null`), `plan_buckets(now)` always finds un-done hours (`app/backfill/coverage.py:66-93,311`); it flips to `done` only once live capture starts and the RPC era reaches `live_from + 2h`.
- TMC side: `backfill_tmc` skips every netuid whose sidecar unit is `done` unless `--redo` (`app/backfill/tmc_backfill.py:250-252`); `candles5m` fetches the whole `_gap()` = `[ledger_end − 1h, now − 10 min]` per netuid (`:362-365,368-380`), `analytics1m` = TMC's sliding 30-day hourly window, `analytics1y` = sliding 365-day daily window (BUILD-LOG Phase 3: each day of delay loses the oldest day for netuids not yet stored; already stored days are kept — writes are `INSERT OR IGNORE`/rank-guarded). So **`--redo` is required** to extend `price_bars` 5m and `chain_hourly`/`chain_daily` to today. With the service down the quiet windows protect nobody → set `BACKFILL_TMC_QUIET_UTC=""` for the run (BUILD-LOG Phase 3 item 13). Budget: ≤ 180 req/h (`BACKFILL_TMC_MAX_PER_HOUR`), 2/min script share; the 09-24 candle run took 1,721 s for 128 netuids.

### 7.2 Commands (run from `/home/sqsq/Documents/TaoData`; all paths absolute where it matters)
```bash
# 0. sanity (re-run any time; appends one log line)
.venv/bin/python -m app check                       # expect: schema_version=2 … 17 available (phase 3)
rm -f data/run/taosentinel.lock                     # stale pid 88499 from 2026-09-24 (only if `serve` refuses to start)

# 1. fill the RPC gap 2026-09-24T18:00Z → now (Blockmachine; ≈ 356 buckets, ≈ 3 min; probe already PASS today)
.venv/bin/python -m scripts.backfill_rpc --probe    # optional re-check (45 requests), writes data/backfill/rpc_probe.json
.venv/bin/python -m scripts.backfill_rpc --era gap  # pool_history + rpc_owner_history + block_times, then occupancy/stamp/bars
.venv/bin/python -m scripts.backfill_rpc --post-only  # ALSO rebuilds 1h/1d bars for the missing 2025-02-13 → 2025-05-19 window (§3.2)

# 2. TMC history to today (service DOWN variant; --redo is required, see §7.1)
BACKFILL_TMC_QUIET_UTC="" .venv/bin/python -m scripts.backfill_tmc --tasks analytics1y,analytics1m,candles5m --redo
#    service UP variant (POSTs /api/v1/control/run/backfill_tmc with API_AUTH_TOKEN and tails the sidecar):
#    .venv/bin/python -m scripts.backfill_tmc --tasks analytics1y,analytics1m,candles5m --redo

# 3. regenerate coverage
.venv/bin/python -m scripts.build_coverage          # rewrites data/quant/coverage.json

# 4. start live collection (pick one)
make run                                            # foreground, 127.0.0.1:8090; Ctrl-C stops cleanly
#   or 24/7 (needs sudo; also the still-open Phase-0 operator steps):
sudo timedatectl set-timezone Etc/UTC               # spec 02 §I2 / checklist A1 (host is Europe/London)
sudo apt install sqlite3                            # verify checklists use the CLI
sudo ops/install-systemd.sh && sudo systemctl start taosentinel && systemctl status taosentinel --no-pager
curl -s localhost:8090/health | python3 -m json.tool | head -30

# 5. verify data is moving again (read-only)
.venv/bin/python - <<'PY'
import sqlite3, datetime as dt
c = sqlite3.connect('file:data/taosentinel.db?mode=ro', uri=True)
f = lambda t: dt.datetime.fromtimestamp(t, dt.UTC).isoformat() if t else None
print('pool_history SN64 max', f(c.execute("select max(timestamp) from pool_history where netuid=64").fetchone()[0]))
print('chain_hourly by source', c.execute("select source, count(*), max(hour_ts) from chain_hourly group by source").fetchall())
print('gap buckets', c.execute("select status, count(*), max(bucket_ts) from backfill_buckets where era='gap' group by status").fetchall())
PY
```
After the service is up: `tmc_daily` catches up at boot + 300 s and repairs the previous 7 days of hourly analytics (`analytics_1w`, BUILD-LOG Phase 2 item 11); `chain_features` runs at 00/06/12/18:05Z and, once a slot has run, `.venv/bin/python -m app.features.chain_features matrix` prints the ranked P1 matrix; `backup_daily` (03:30Z) will create the first real backup under `data/backups/` (currently none).

### 7.3 Still-missing operator inputs that gate further history
Seed files from the PC/Pi (checklist A3/A4: `seed/ledger.db`, `seed/ledger_backfill.db`, `seed/fundamentals.json`, `seed/whale_*.json`, old `app/` tree, `seed/pi/ledger.db`) → unlocks `import_old_ledger` (1-minute data 2026-04-29 → 06-19 and beyond on the Pi), `import_fundamentals` (2026-06-02 quality snapshot), S7 regime port, verbatim ports of `dereg_risk`/`endpoints`, and the Pi position check (`PI_TRADER_COLDKEY`). Keys for the next phases: `GITHUB_TOKEN` (Phase 4), `DISCORD_BOT_TOKEN` (6), `TELEGRAM_BOT_TOKEN/CHAT_ID` (alerts), `ANTHROPIC_API_KEY`/`TWITTERAPI_IO_KEY` (9).

---

## 8. Relevance to the "subnet investment analyst" tool (signals the operator named)

| Wanted signal | Exists today? | Where / what is missing |
|---|---|---|
| Alpha price (history + charts) | **Yes, to 2026-09-24**: hourly reserves/price/moving price since 2025-02-13 (`pool_history`), 1h/1d bars since 2025-05-19 (Feb–May 2025 bars missing, §3.2), 5m since 2026-06-19 (`price_bars`); `tao_price_usd` on TMC bars/analytics | restart collection (§7); no chart/UI code exists (Phase 11 unbuilt; no `frontend/` dir) |
| Distance from the 72 EMA band | **No such concept anywhere in the corpus** (grep of research/ and specs/: no 72-period EMA). Closest: the chain's own EMA `moving_price`/`subnet_moving_price` and the old factors `px_vs_moving`, `moving_px_trend` (`specs/13-validation-harness.md:71,269`) | computable from `price_bars` (any period) — new metric to specify |
| Distance from deregistration | Building blocks built: `app/quant/dereg_risk.py` (hazard = proximity/starved/decline, `:4-8,68-81`), `chain_features` P1 `dereg_safety` + `immune_local` + TMC `deregistration_risk` flag (`app/features/chain_features.py:28,128-190`); inputs: daily `emission_percent` since 2025-09-24, hourly `moving_price` since 2025-02-13, `registered_ts` for all 180 occupants | 0 `metric_values` rows — never computed on the real DB |
| GitHub activity | **Nothing collected** (`github_*` 0 rows; Phase 4 unbuilt; `GITHUB_TOKEN` empty). 116/129 subnets have a `github_url` in `subnets`; TMC also exposes keyless `/subnets/github-stats/{netuid}/` and `github-commit-chart` (`research/02-gapfill-data-continuity-and-backfill.md:36,75`) | Phase 4 (spec 06) + `backfill_github` |
| Socials activity | Links known (discord 97, x 104, telegram 7, website 115 of 129); **no activity metrics** (`discord_*`, `telegram_snapshots`, `x_*`, `social_posts` all 0; Phases 6/9 unbuilt, keys empty) | Phase 6 (keyless Discord invite counts) and Phase 9 (X, paid) |
| Rank/score to "pick a good investment" | None (`scores` 0); the first ranked output would be the P1 matrix (Phase 2, one command after a slot) and the full `ts-1.0` composite (Phase 8) | Phase 8 scoring, Phase 10 validation gate before any capital (master §10) |

---

## 9. Gaps, risks, open questions

1. **No backup of the repaired DB.** `data/backups/` is empty; the only copies are `data/tmp/pre_price_repair_2026-09-25.db` (pre-repair, 1.5 GB) and `pre_phase3_2026-09-24.db` (11 MB). Run `make backup-now` (or start the service) before touching the DB further.
2. **Feb–May 2025 bars never built** (§3.2) — hidden by `coverage.json`; `--post-only` fixes it (costs ≈ 500 occupancy RPC calls, BUILD-LOG Phase 3 limiter note).
3. **Runtime 475 is new** (was 470 on 09-24): decode of `get_all_dynamic_info`/`current_alpha_price_all` under the new metadata is exercised for the first time by the next `--era gap`; watch for `DecodeError` (exit 4) in that run.
4. **TMC 1Y window slides daily**: days before 2025-10-09 are already stored (from 2025-09-24), so no further loss for existing netuids, but any netuid re-registered after 09-24 starts its history at its re-registration (BUILD-LOG Phase 3 item 8).
5. **Host still not UTC and no systemd unit** — both Phase-0 operator actions remain open since 2026-09-24.
6. UNVERIFIED: `chain_features run` on backfilled-only rows (tier-3 fallback) — not executed because it writes; whether the dead-pid `taosentinel.lock` is auto-reclaimed on boot; the Pi's state (unreachable today).
7. Open questions for the operator: can the Pi be powered on / reached (A4) and the PC seed files copied (A3)? Should TaoStats be enabled (`TAOSTATS_ENABLED=true`) now that the key is valid, to give the owner/whale pillars a source before Phase 7? Which "72 EMA band" definition (period unit, source series: 1h bars vs daily closes vs chain `moving_price`) does the operator mean?

## Sources
- Code: `app/config.py`, `app/backfill/{rpc_client,rpc_backfill,tmc_backfill,coverage,common}.py`, `app/collectors/{chain_tmc,chain_tmc_rest,chain_rpc,endpoints}.py`, `app/jobs.py`, `app/__main__.py`, `app/http.py`, `app/ratelimit.py`, `app/features/chain_features.py`, `app/quant/dereg_risk.py`, `scripts/{backfill_rpc,backfill_tmc}.py`, `tests/conftest.py`, `ops/systemd/taosentinel.service`, `Makefile`, `start.sh`.
- Data: `data/taosentinel.db` (read-only queries listed inline), `data/quant/coverage.json`, `data/backfill/{rpc_probe,tmc_request,tmc_progress,pi_recovery_report,occupancy_anomalies}.json`, `data/logs/*.jsonl` and `*.out`, `data/seed_registry_report.json`.
- Specs/research: `specs/BUILD-LOG.md`, `specs/INDEX.md`, `specs/00-MASTER-SPEC.md:364-390`, `specs/12-feature-store-and-scoring-engine.md:818-836`, `specs/13-validation-harness.md:93-192`, `specs/17-data-migration-and-backfill.md:193,266`, `specs/18-operator-checklist.md:15-17`, `research/INDEX.md`, `research/01-prior-sentiment-design.md`, `research/01-factor-validation-tooling.md`, `research/01-data-layer-and-schema.md`, `research/02-gapfill-data-continuity-and-backfill.md`.
