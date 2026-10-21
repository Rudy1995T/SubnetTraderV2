# TaoSentinel — build log

Findings, deviations and spec-conflict resolutions recorded while implementing (02 §A1: precedence 02 › 00 › owner spec).

## 2026-09-24 — Phase 0 (01 + 03 + Phase-0 subset of 16 + 14's routing/templates)

**Status: done except the two sudo-only verify steps** (systemd install, `sqlite3` CLI) — see "Operator actions" below.

Where it lives: `~/Documents/TaoData` (the spec says `~/taosentinel`; nothing depends on the path — `APP_ROOT` is derived
from the package location and the installer renders absolute paths).

### Verify results (01 §10, 03 §10, 16 §10 B–E subset)
- `pip install -e ".[dev]"` on Python 3.12.3 / Ubuntu 24.04.5; `requirements.lock` written; `import bittensor` → ModuleNotFoundError.
- `python -m app check` → `schema_version=1`, `schema_warnings=[]`, `sqlite_version=3.45.1`, `quick_check=ok`,
  `OK jobs=53 declared (50 jobs + 3 LongTasks), 7 available (phase 0), 46 unavailable`.
- `pytest -q` → 108 passed (incl. the in-process smoke boot).
- Live run against the real `.env`: `/health` `status=ok`, `declared 53 / available 7`; services database/scheduler/disk ok
  (telegram false until a token exists); `/api/v1/jobs` = 53 rows; pause without token → 401; pause writes exactly one
  `control_state` row and 0 `job_state` pause rows; resume deletes it; manual `run/log_retention_daily` → `collector_runs`
  ok=1 with integer `started_at`; `schema_version` = `1|baseline`; `KILL_SWITCH` → `kill_switch_active:true`;
  `PAUSE_ALL` → `degraded_reasons: [pause_all]` with ops jobs still running; SIGTERM → "Services shut down cleanly (0.1s)".
- 80 tables, 6 views, 14 triggers; STRICT everywhere except `pool_history`, `whale_cohort`, `whale_events`.
- Metric catalog 79 keys (45 scored / 9 legs / 8 aux / 17 evidence), every pillar's weights sum to 1.0; 116 event types; 12 red flags.
- Every `JOB_TABLE` liveness max-age equals the 01 §2.7 column (53/53).

### Deviations and decisions
1. **Old-bot source not on the laptop.** Checklist A3 (`seed/app/`) has not been copied yet, so the "verbatim ports"
   (`logger.py`, `api_auth.py`, `utils/math.py`, `main.py` lifecycle, `backup.py`) were re-implemented from the specs'
   descriptions and the research notes (`research/02-gapfill-old-bot-runtime-skeleton.md`). Behaviour matches the
   documented contracts and the listed test expectations; line-level parity with the old code is unverified.
2. **Alert delivery (02 §D3 amended vs 01 §2.11).** 01 still describes `direct` types and a "dispatcher inactive → send
   direct" rule; 02 §D3 (amended) and 16 §11 item 22 supersede it. Implemented: `AlertLatch.fire(event_type=…)` always
   inserts the row with `alerted_at NULL`; a direct `send_alert` happens only for notices without an `event_type`
   (`service_online`) or when the DB write fails. Consequently `disk_critical` / `db_integrity_failed` route
   `immediate` in `app/alerts/routing.py` (14 §2.4 marks them `direct`, which would never page under D3).
3. `build_health(rt)` is `async` (the DB stats it reports are async); 01 shows a sync signature.
4. X jobs declare `providers=("x",)` (the spend-group key) because their real provider is `X_PROVIDER` at run time;
   budget pauses resolve through the governor (`group_of`).
5. `quant_weekly.timeout_sec` is the static default 1800 (the `QUANT_WEEKLY_JOB_TIMEOUT_S` key is carried but a
   JobSpec timeout is not dynamic yet) — revisit in Phase 10.
6. Startup catch-up (E1) is scheduled as a one-shot `DateTrigger` job `<id>:catchup` so the run carries
   `trigger="catchup"`; planning runs right after `scheduler.start()` (quick DB reads, still before READY).
7. Boot records: `pre_init` classifies the boot before the DB opens (reads `job_state('_ops','boots')` read-only via
   sqlite3 when the file exists); the boot row + orphan count are appended after `apply_schema` (`record_boot`).
8. `src_rank(source)` is one global ordering that preserves every per-table order of 03 §3.3
   (`analytics_1y` 3 > `analytics_1w` 2 for chain_daily; chain_hourly never stores `analytics_1y`).
9. List defaults not yet specified inline in 01 (`REGISTRY_JUNK_TOKENS`, `WEB_PARKING_HOSTS`,
   `WEB_PARKED_STRONG_PHRASES`, `WEB_PARKED_WEAK_KEYWORDS`, `WEB_HIJACK_KEYWORDS`, `WEB_SHARED_PLATFORM_HOSTS`) are `[]`
   until their owner phases (1, 5) fill them from 04 §2.6 / 07 §2.5–§2.6.
10. `app/notifications/telegram.py` ships only `send_alert`/`send_message`/`get_me`; the long-poll `TelegramBot`
    (to be "copied in Phase 0, wired by 14") needs the old source — deferred to Phase 11.
11. `deploy/auto-update.sh` not written yet (opt-in; needs the private repo + deploy credential, checklist A6/A7).
    The update unit/timer files exist; do **not** pass `--with-update-timer` to the installer until it does.
12. `costs.PRICES` only lists Haiku 4.5 (UNVERIFIED); spec 10 owns the table — verify current pricing in Phase 9.
13. `Http` sets auth headers for github/taostats/twitterapi_io/official_v2 only; TMC's key header format is UNVERIFIED
    (only needed with a TMC Pro key) — Phase 2.
14. `.env.example` puts each comment on its own line: python-dotenv reads `KEY=  # note` as the value `# note`
    (caught by `test_env_example_roundtrips_defaults`).
15. `ops/install-systemd.sh` renders templates from its own checkout (`REPO_DIR`) and substitutes `SENTINEL_ROOT`
    (the draft read templates from `SENTINEL_ROOT`, which breaks `--render-only` and out-of-tree installs).
16. `JobRegistry.build` logs one summary line for the 46 not-yet-built jobs (per-job lines at DEBUG) instead of 46 INFO lines.

### Operator actions still open for Phase 0 (need sudo)
- `sudo apt install sqlite3` (CLI used by the verify checklists; Python's sqlite3 3.45.1 is fine).
- System timezone is `Europe/London`; the spec requires UTC (checklist A1, 02 §I2): `sudo timedatectl set-timezone Etc/UTC`
  — or run `sudo ops/laptop-prep.sh --user $USER` (does this plus sleep masking, GRUB, swap, UFW, journald).
- `sudo ops/install-systemd.sh && sudo systemctl start taosentinel` for the 24/7 unit (then 01 §10's systemd lines).

---

## Phase 1 — subnet registry and identity (spec 04), built 2026-09-24

Code: `app/registry/` (keys, models, normalise, anomalies, occupants, observations, merge, watch, overrides, review,
legacy, jobs, sources/{tmc,chain_rpc,subnetalpha}), `app/collectors/chain_tmc.py` (client + snapshots, 05 step 1),
`scripts/seed_registry.py`, `python -m app.registry review`. Tests: `tests/registry/` (12).

Live result on the laptop: 126 active + 3 pre-launch occupants; `github_repo` present 116 / absent 11 / junk_only 2;
SN64 → chutesai/chutes; a second identity run is idempotent (0 new observations, 0 new events).

Deviations / findings:
1. `SubnetIdentitiesV3` live storage keys are 102 hex chars (Blake2_128Concat hasher, not Identity); the netuid is
   the last 4 hex chars (u16 LE).
2. R6 disagreement compares **primary** values only (the main vanity Discord invite is excluded); comparing every
   candidate produced hundreds of trivial disagreements.
3. Compare keys: website ignores `www.`; names compare alphanumeric-only (`name_key`) — removes spacing/case noise.
4. Docs-URL coverage is 1 live (spec expected ≥ 10); a docs URL is no longer duplicated as an extra.
5. SN94 is a fourth abandonment marker (spec listed SN39, 73, 112).
6. `ObservationWriter` keeps a per-run extra index and `finish_source` blanks stale extras/docs.
7. The live `/subnets/?limit=200` returns all rows on one page (honoured).
8. Anonymous SSE works on this IP; the stream also sends `{"type":"heartbeat"}` events (not in the spec).
9. `first_emission_block_number` is `'0'` (not null) for not-started subnets; root (SN0) is special-cased active.
10. Chain RPC fallback reads the `/subnets/` snapshot with `before=now` (the run clock), not the wall clock.
11. Anomaly counters are scoped per source (`scope=`), so one failing source cannot trip another's gate.
12. Tests: the fake chain needs ≥ 110 keys or the run is treated as "suspect" (as designed).

## Phase 2 — chain collector and first P1 matrix (spec 05), built 2026-09-24

Code: `app/collectors/chain_tmc.py` (`TableState`, `ChainSseTask`), `app/collectors/chain_tmc_rest.py`
(`TmcHourlyJob`, `TmcDailyJob`, `TmcWeeklyJob`, J1 helpers), `app/collectors/endpoints.py`,
`app/collectors/chain_rpc.py` (optional, off), `app/features/{gate_model,holders,normalise,chain_features}.py`,
`app/quant/dereg_risk.py`, `app/scoring/p1_matrix.py`, `scripts/tmc_probe.py`. Tests: `tests/test_chain.py` (18);
fixture `tests/fixtures/tmc_table_2026-09-24_block9138613.json`. All 16 Phase ≤ 2 jobs are available (`BUILT_PHASE = 2`).

Live checks (laptop, 2026-09-24):
- `tmc_probe`: full = 129 rows × 44 fields; envelope keys `type`, `added` only; units confirmed —
  `tao_liquidity` rao (SN64 1.985e14), analytics `total_tao_buy_24h` **rao**, SSE owner lock = analytics (ratio 1.000),
  `total_locked_alpha` = analytics `total_locked` (1.000). `data/tmc_table_fields.json` written.
- 75-s live SSE run: ticks written for all 129 netuids at the full and at the next stride window.
- Slot run on the live table (scratch DB): n_eligible 122, θ 0.00772, residual 0.13 pp, converged in 20 iterations,
  rank-32 gate 0.50, SN95 implied burn 1.0; the three TMC `deregistration_risk` subnets (SN82, 113, 47) rank 1–3 in the
  non-immune pool → `dereg_safety` 0.0; first P1 matrix printed (top: SN64, 51, 4, 120, 8).

Deviations / findings:
1. **Live deltas carry no block number** — neither per row nor in the envelope (`{type, changes, added}`), and
   `/sse/blocks/` answers 200 but sent nothing in 30 s. Between `full` events the recorder estimates
   `block = full_block + elapsed_s // 12` (`block_source = "row+estimated"`); each `full` (reconnect or REST fallback)
   re-anchors on the real row block. `chain_ticks.block_number` is therefore approximate between fulls (±1–2 blocks).
2. **Gate fit update rule (05 §2.3) does not converge** as written: (a) resetting `b` to 1.0 when
   `pred ≤ min_pred_share` makes fully-burned subnets oscillate 1 → 0 → 1; (b) the plain multiplicative step overshoots
   below θ, where the Hill gate multiplies the sensitivity by up to `1 + h`. Fix: hold `b` in case (a); damp the step by
   the local elasticity, `b ← b · (r/p)^(1/(1 + h·(1 − gate)))`; `realised = 0` → `b = 0`. Residual on live data fell
   from 3.7 pp (non-converged at 50/200/1000 iterations) to 0.13 pp; synthetic burns recovered to 1e-4.
   Consequence: 12 deep-tail subnets are `burn_unidentifiable` (pgs < 1e-4) instead of 4.
3. The hour flush writes one **closing tick** (the hour's last state, `event_at` = last event) for every netuid whose
   last tick is older, so the 5m/1h `sse_rollup` bars close on the hour's final state (§9 hour-flush test).
4. `immunity_mismatch` is emitted by `chain_features` only (§3.4), not by the recorder.
5. `app/quant/dereg_risk.py` and `app/collectors/endpoints.py` are **re-implementations** from their documented
   contracts (the old SubnetTrader sources are not on the laptop). `starved` is binary (weight × 1 when the relative
   emission < `starved_ratio`) — UNVERIFIED against the original lines 127-174; port the old test vectors when the
   source is available.
6. `/subnets/` snapshots carry no active-validator/miner counts (`max_allowed_uids`, `subnetwork_n` only), and the RPC
   metagraph runtime-API decoder is not built (v1 forbids substrate-interface) → `participation` is
   `failed/no_metagraph_source` for every occupant (the documented keyless default); `chain_rpc_metagraph` records a skip.
7. TMC 1h candles are 10-element arrays `[ts, tmc_block, o, h, l, c, volume, start_volume, tao_price_usd, ?]`; the
   10th element is unmapped (UNVERIFIED).
8. `/market/market-data/` has `current_price` (USD) and `usd_quote.price`; both are tried first.
9. Pressure rows are `{timestamp (ms), value}` with no `ema_price` (stored NULL); the side comes from the query.
10. Trending `entity_id` is a string netuid; `rank` = list position (the list is not sorted by views).
11. `tmc_daily` gap repair writes every 1W hour missing from `chain_hourly` (`analytics_1w`, `backfilled=1`), so the
    first run after install back-fills the previous 7 days — intended (§7 power-loss repair), but it means pre-install
    hours exist as analytics rows.
12. `net_flow_30d` needs ≥ 24 daily rows; `tmc_daily` supplies 7 days and `tmc_weekly` adds 1Y history for 1/7 of the
    netuids per day, so the metric fills in over the first week (or immediately after 17's `backfill_tmc`).
13. `GATE_FIT_MAX_ITER=50` kept; the damped fit converges in ≈ 20 iterations.

### Operator actions for Phase 2
- None required (keyless). Start the service (`make run` or systemd) and the SSE recorder begins at once;
  `tmc_daily` catches up at boot + 300 s; `chain_features` runs at 00:05/06:05/12:05/18:05Z.
- `python -m app.features.chain_features matrix` prints the ranked P1 matrix once a slot has run.

## Phase 3 — data migration and backfill (spec 17), built 2026-09-24

**Status: code complete; live RPC + TMC history running/ran; the old bot's seed files (checklist A3/A4) are not on the
laptop yet, so S1/S2/S5 (ledger + fundamentals import) are built and tested against fixtures only, and S7 (regime) is
guarded until spec 12's `app/quant/regime.py` port exists (it needs the old `app/strategy/regime.py`).**

Code: `app/backfill/{common,rpc_client,rpc_backfill,occupancy,bars,tmc_backfill,ledger_import,fundamentals_import,
regime_backfill,coverage,pi_recovery}.py`; scripts `backfill_rpc`, `backfill_tmc`, `import_old_ledger`,
`import_fundamentals`, `backfill_regime`, `build_coverage`, `pi_recovery_report`, `backfill_all`; migration
`0002_price_bars_blockmachine.sql`; tests `tests/backfill/` (36). `BUILT_PHASE = 3` (17 available jobs).
`backfill_github` (S9) is Phase 4 and `replay_history` (S10) is Phase 8 — not built.

Operator inputs used: Blockmachine archive RPC key (Standard plan: 1,000 req/min, 5 M RU/month, 1 RU per call) and the
TaoStats free key (5 req/min, 10 k credits/month; valid — `GET /api/dtao/pool/history/v1?netuid=64&frequency=by_hour`
answered 14,064 hourly SN64 rows back to dTAO launch). Limits are in the operator memory, not repeated here.

### Live results (2026-09-24)
- `backfill_rpc --probe` on Blockmachine: **all golden checks PASS**. Block 8,443,757: `Timestamp.Now` 1781904264001,
  129 entries; `tao_in`, `alpha_in`, `subnet_volume`, `moving_price` rel. err **0.0**; reg 4,531,295; raw
  `SubnetTAO[64]` 187,212.134573233 τ. Price: `swap_sqrt` rel. err 1.15e-8 (reserve ratio 6.1e-4) → **17 §11 item 4
  settled: ledger price = `AlphaSqrtPrice²`** (it also equals `SwapRuntimeApi_current_alpha_price` to the rao).
  6,502,594 → 1758585588000; 4,920,351 → 1739482884000, 65 subnets, `SubnetTAO[64]` 0.709 τ; 4,913,000 →
  `no_runtime_api`; head (spec 470) 129 entries; **5 socket requests = 5 tokens per bucket** (no un-metered ASI call).
  `moving_price` frac bits = 32 confirmed (§11 item 6).
- Era `gap` (2026-06-19 22:00 → 2026-09-24 18:00Z): 2,324/2,324 buckets done, 0 failed, 299,795 `pool_history` rows,
  ≈ 2.1 buckets/s at concurrency 8 / 12 req/s; occupancy found 12 re-occupied netuids in the window (historical rows +
  12 pre-alerted `netuid_reoccupied`, `warning|registry`); 0 unstamped rows; 1h/1d `blockmachine` bars built.
- `ledger` and `pre_ledger` eras and the TMC tasks: see "Final results" below.

### Deviations / findings
1. **Blockmachine is the primary archive endpoint** (spec 18 C6 said "not needed"; the operator renewed it). New
   settings `BLOCKMACHINE_API_KEY/_WS_URL/_RATE_PER_SEC (12)/_MONTHLY_CAP_REQ (4.5 M)` (added to spec 01 §5 and the
   limiter table), provider id `blockmachine` (rank 2 like the other RPC sources). Row provenance `source='blockmachine'`
   in `pool_history`/`price_bars`/`rpc_owner_history`; `occupants.source='rpc'` (03 CHECK). Endpoint order:
   Blockmachine → keyed OnFinality → public OnFinality → opentensor archive. The key rides in `?authorization=`; logs
   and DB rows only ever carry the label. The `.env` spelling `BLOCKMACHINE_API_kEY` works (settings are
   case-insensitive).
2. **Migration 0002 (`@allow-destructive`)** rebuilds `price_bars` to add `'blockmachine'` to its `source` CHECK
   (table was empty; rows are copied; view + index recreated). Schema version is now 2; tests that pinned
   `schema_version == 1` now compare with `CODE_SCHEMA_VERSION`.
3. **Latent bug fixed in `app/storage/db.py`:** the dict row factory crashed on description-less rows
   (`PRAGMA incremental_vacuum` steps), which surfaced once a migration left free pages.
4. **Price method across runtimes** — superseded by item 17 (the sqrt map went *stale* before it disappeared).
5. **Runtime metadata** is loaded with the hash of block b+1 (ASI reads metadata at the parent of the hash it is given),
   so a read at an upgrade block uses the correct runtime.
6. **Block search** adds bisection once the window is bracketed (the spec's 3 delta corrections cannot resolve a chain
   stall); still `no_block_in_hour` when the bracketing blocks are adjacent, `block_search` failure when unbracketed
   after 3 corrections.
7. **No ledger imported yet:** era bounds fall back to the verified PC ledger span (1758585588 → 1781904274), so the
   `ledger` era fetches every hour of that span from the chain (6,479 buckets). When the ledger is imported later its
   rows coexist (different blocks; INSERT OR IGNORE) and its bars outrank the RPC bars (rank 4 vs 2).
8. **TMC analytics only returns the current occupant's history** (SN16 re-registered 2026-06-22 → its 1Y series starts
   2026-06-22). This answers 17 §11 item 13: histories are *not* spliced across occupants — predecessors'
   `emission_percent`/flows are unrecoverable from TMC. Proposal (not built): persist `tao_in_emission`/`emission` from
   the RPC `DynamicInfo` cross-sections (already decoded each bucket) so emission share of dead occupants can be
   reconstructed for 13's survival studies.
9. **`rpc_owner_history` redundancy:** concurrent out-of-order bucket writes can store a change twice (e.g. SN37 at
   14:59 and 15:59 with the same new coldkey). 17 accepts this ("change-oriented, not minimal"); **spec 11's
   `owner_rpc_seed` must collapse consecutive identical states** (Phase 7).
10. `BACKFILL_RPC_MODE=storage` is not built (runtime_api decoded every runtime seen: specs 233 → 470); the script
    exits 3 if it is selected.
11. Python's `sqlite3` reports `rowcount = -1` for `WITH … UPDATE`; enrichment counts use `total_changes`.
12. `ledger_import_report.json` keeps per-source objects at the top level (17 §10 item 2's `jq '.ledger_backfill…'`),
    plus `run`, `bars`, `verify`.
13. The TMC launcher does not take `backfill.lock` (it is an in-service job run through `run-job` when the service is
    down); with the service down the quiet windows (02 §F2) protect nobody, so the 2026-09-24 run used
    `BACKFILL_TMC_QUIET_UTC=""`.
14. `occupants` for root `0:0` has no `registered_ts` (block 0 has no `Timestamp.Now`); 17 §10 item 10 expects 0 NULLs —
    root is the one accepted exception.
15. `backfill_all` gains `--skip-ledger` (run without seed files); S7 exit 3 is treated as "skipped".
16. Occupancy extension: a 17-written historical row can be extended by later eras (`first/last_seen`, `successor`,
    `absent → reoccupied`); 04's live rows only get `first_seen_*` = MIN, `registered_ts`, `predecessor_key`.

### Operator actions for Phase 3
- Copy checklist A3 files to `seed/` (`ledger.db`, `ledger_backfill.db`, `fundamentals.json`, `whale_cohort.json`,
  `whale_labels.json`, the old `app/` tree) and, if recovered, A4 files to `seed/pi/`. Then:
  `sudo systemctl stop taosentinel; python -m scripts.import_old_ledger --phase import; python -m scripts.import_old_ledger --phase stamp,bars; python -m scripts.import_fundamentals; python -m scripts.build_coverage`.
- After the service runs again: `python -m scripts.backfill_tmc --tasks restamp,candles5m` (S6).
- Optional: fix the `.env` comment that still says the Blockmachine key returns 401.

### Final results (2026-09-24/25)
- **RPC eras, all on Blockmachine:** `gap` 2,324 done; `ledger` 6,479 done (835,791 rows; the whole PC-ledger span from
  the chain since no ledger is imported); `pre_ledger` 5,283 done + 23 `skipped: no_block_in_hour`, 0 failed.
  `pool_history` 1,711,230 rows, hourly for every occupant from dTAO launch (2025-02-13 21:59Z) to 2026-09-24.
  Occupancy: 180 occupants (51 historical, all with exact `registered_ts`; only root `0:0` lacks one), 0 anomalies,
  51 pre-alerted `netuid_reoccupied` history events, 0 unstamped rows. ≈ 75 k Blockmachine requests in total.
- **The 23 skipped hours are a chain halt**: no block was produced in 2025-05-20 23:00 → 05-22 12:00Z except a few
  hours; `coverage.json` reports them as the only 3 holes.
- **TMC:** `analytics1y` 128/128 and `analytics1m` 128/128 (0 unattributed, 128,329 rows; `chain_daily` from
  2025-09-24), `candles5m` 128/128 (3,567,624 5m + 298,189 1h + 12,416 1d `tmc_candle` bars over the gap, 0
  unattributed). **Candle label = start** (median |err| vs chain 1.1e-7 on 48 SN1 samples) — 17 §11 item 13 part settled.
- **§10 checks:** item 12 (TMC 1h close vs chain, SN64, 2,326 hours) mean rel. diff 6.7e-5, median 5.7e-6 (< 1 %);
  item 13 `start_volume ÷ subnet_volume` = 1.000000 → **`subnet_volume` is τ** (17 §11 item 5 settled; coverage now
  derives the caveat from data and records `checks.subnet_volume_unit`); `tmc_candle` bars never carry a non-chain
  `block_number` (0); no live `chain_hourly` row is backfilled (0); 1h completeness ≥ 0.927 in every one of 17 months.
- `coverage.json`: pre_ledger/ledger `done`, gap `partial` (it keeps growing until live SSE capture starts),
  `rpc_golden_block: pass`, caveats `price_method_mixed`, `tmc_candle_label:start`. DB size 1.55 GB.
- Tests: 179 passed (`tests/backfill/` 41).

17. **Stale `AlphaSqrtPrice` (data bug found and repaired).** The chain's own price API
    (`SwapRuntimeApi_current_alpha_price_all`, present from ≈ block 7.8 M / runtime 391) shows: `AlphaSqrtPrice²` is the
    price through runtime 422 (block 8,480,138; also confirmed pre-API against TaoStats at blocks 6,121,500 and
    6,787,500 to 1e-16), but from **runtime 423 (≈ block 8,486,710, 2026-06-25) to runtime 443 (block 8,830,966,
    2026-08-12) the chain priced by the reserve ratio while the Swap storage lingered, frozen** (cleared in runtime 445).
    `auto` read the frozen values: 147,580 rows (128 netuids × 1,153 hours) were off by a median 2–27 %.
    Fix: `process_bucket` now takes the chain's API price whenever the runtime has it (1 call; equals the old
    SDK/ledger price to the rao), labelled `reserve_ratio`/`swap_sqrt` by which mechanism it matches; the sqrt map is
    used only before the API existed; root (netuid 0, API says 1.0) keeps the reserve ratio. `scripts/repair_rpc_prices.py`
    re-read the chain price for all 1,208 blocks of that window and corrected exactly those rows (`price`,
    `price_method`, `observed_at = repair time`, `run_id = script:repair_rpc_prices:…`), dropping and recreating
    `trg_pool_history_update` with its exact DDL inside one transaction. No bar needed rebuilding (the window lies in
    the gap, where TMC bars outrank RPC bars and enrichment never copies price). A full dry run over all 4,474 API-era
    blocks found no other bad non-root rows. Backups: `data/tmp/pre_phase3_2026-09-24.db`,
    `data/tmp/pre_price_repair_2026-09-25.db` (delete when no longer wanted).
18. **Gateway concurrency bug (fixed):** with 8 workers, failover/reconnect closed the shared socket under the other
    workers (`'NoneType' has no attribute 'rpc_request'`) and a runtime-load timeout cancelled ASI's shared in-flight
    future, whose `CancelledError` killed the era. Now: connection generations (a worker reconnects only the socket it
    saw die), one shielded runtime-load task per (endpoint, version), leaked cancellations become bucket failures, and
    only endpoint errors count toward failover (not `block_search`/decode/missing API). Regression tests added.
19. Verify query 11's `NOT IN (SELECT … FROM pool_history WHERE netuid = price_bars.netuid)` is too slow on 3.8 M
    bars; use `NOT EXISTS (… ph.netuid = pb.netuid AND ph.block_number = pb.block_number)` (same meaning, 1 s).

## 2026-10-12 — TaoStats API migration (`/api/` → `/v1/`)

TaoStats emailed on 2026-10-12: the API was rebuilt (Rust + ClickHouse) and the old `/api/…/v1` routes **stop
answering on 2026-11-09** (both APIs live 2026-10-08 → 11-09). Same host, same key, same `Authorization: <key>` header.
Migration guide: `https://taostats.io/blog/api-migration-guide.md` (173 old routes: 114 replaced, 21 partial, 38 retired).

- **Code impact: none.** `app/` never calls a TaoStats path — only `TAOSTATS_BASE_URL` (`app/config.py:112`), the
  auth header (`app/http.py:53-54`) and the `taostats` limiter/spend group, all unchanged. `TAOSTATS_ENABLED` stays
  false; no tests change. The only TaoStats consumers are still-unbuilt phases (11 §C8, Phase 7; whale port).
- **Live probe (6 requests, free key):** `/v1/status`, `/v1/subnets/owners?netuid=64`,
  `/v1/subnets/pools/history?netuid=64&frequency=by_hour` (14,495 hourly rows for SN64),
  `/v1/events?pallet=SubtensorModule&name=NetworkRegistrationQueued`,
  `/v1/extrinsics?pallet=SubtensorModule&name=announce_coldkey_swap&include_args=true` → all 200; the old
  `/api/dtao/pool/history/v1` still 200. Same SN64 hour on both APIs: price/reserves identical, `alpha_staked` lower on
  `/v1/` by 12,362.75 α (as the guide says), `market_cap` lower in proportion, amounts are strings.
- **Spec changes:** new 02 §G7 (TaoStats `/v1/` conventions: paths, `order_by`/`order_dir`, `limit ≤ 200` + follow
  `next_page`, unknown params → 400, Decimal strings, SS58 strings, retired routes). 11 §C8 rewritten to the new routes
  and field names (`nominator`→`coldkey`, `DELEGATE/UNDELEGATE`→`stake/unstake`, `call_args`→`args` with
  `include_args=true`, `full_name`→`pallet`+`name`, `is_transfer` never NULL, new `validator_swap` flag); the
  `NetworkRegistrationQueued` filter (was UNVERIFIED) is settled and its `args` already carry `lock_amount` and
  `median_subnet_alpha_price` for `initial_alpha_est`. Paths updated in 00 (§4 sources table, §7 P4 mechanics, §10
  backtestable list), 13 (P4 rows), 17 (§1 scope), 18 (C3 first test), 11 (§6 budget, §11 item 9, §12 sources).
  Tests: 179 passed (unchanged).
- **Retired, no replacement:** `dev_activity/*` (master §4 listed it — GitHub collector 06 is the source anyway),
  `dtao/tao_flow`, `dtao/slippage`, `dtao/hotkey_emission`, validator dividends/yield history, coldkey identity
  history; `subnet/latest`'s `tao_flow`/`ema_tao_flow`/`net_flow_*` fields are gone.
- **Not touched:** `research/*.md` keep the old paths as dated snapshots; the old bot (`seed/`, Pi recovery) is dead
  and not migrated. Seed-era ledger rows sourced from TaoStats (17 §2.1, fetched 2026-05-09) are historical imports
  and unaffected; our own hourly history is from the chain (Blockmachine), not TaoStats.

## 2026-10-12/13 — Phase 3 seed steps from the Pi recovery (S1, S2, S5, S7) + gap extension

The old bot's files were recovered from the Pi's USB root filesystem (2026-10-02, `~/USB_Recovery/rootfs/home/pi/
Desktop/SN_Bot/SubnetTrader/`, none listed in `bad-files.txt`). Copied (permissions `go-rwx`): `seed/pi/ledger.db`
(+`-wal`/`-shm`, 2.3 GB, last write 2026-08-21), `seed/ledger_backfill.db`, `seed/fundamentals.json`
(byte-identical facts to 17 §2.6: `generated_at_unix 1780398221`, 129 records, no `team_info`), `seed/app/` (old tree,
HEAD 2026-08-09, read-only reference), `seed/pi/logs/*.jsonl`, `seed/pi/VALUE_KILLED`,
`seed/pi/wallets/trader_wallet/coldkeypub.txt` (public key only). **Not copied:** the old `.env` (secrets; the report's
`env_keys` is therefore empty), `backups/*.db` (15 GB; nothing in 17 reads them). **Not recovered anywhere:** the PC
ledger (`seed/ledger.db`, Windows PC) and `whale_cohort.json`/`whale_labels.json` (not on the Pi) — the dormant
`whale_cohort` table stays empty. Backup before the import: `data/tmp/pre_seed_import_2026-10-12.db`.

### Pi ledger = superset of the PC ledger
Read-only probe of `seed/pi/ledger.db`: 19,055,650 rows, 129 netuids, blocks 6,502,594 → 8,889,356,
1758585588 → 1787269056 (2026-08-20T23:37:36Z); **exactly 7,362,854 rows with `timestamp ≤ 1781904274`** (the PC
count 17 §2.1 verified), the golden SN64 row at 8,443,757 identical to every digit, 0 non-positive rows,
`nra_backstamped` 0; 11,692,796 one-minute rows beyond the PC (2026-06-19 → 08-21). So a Pi-only import is the PC
import plus two months; it is tagged `ledger_pi`.

### Code changes
1. **`ledger_import.run_import`:** the NULL-merge list is "every source except the first writer"
   (`primary[1:]`), not "every source except `ledger_pc`" — with the Pi as the only primary it was merged into itself.
   `SourceReport.rows_preexisting` = rows whose `(netuid, block)` an earlier writer already held.
2. **`ledger_import.verify` (17 §10 items 1–3):** checks the first writer's copy over the PC span
   (`PC_MAX_TS = 1781904274`) when no PC ledger exists, attaching the seed files read-only. A row counts as present when
   any source holds its `(netuid, block)`: the RPC eras ran *before* S1 (Phase 3, 2026-09-24), so 48,472 Pi rows
   (22,544 in the PC span) landed on blocks a `blockmachine` row already had — INSERT OR IGNORE keeps the chain row
   (first writer). They are reported as `held_by_other_source`, not missing. Same for the `ledger_backfill` merge:
   23,237 targets = 23,010 merged ledger rows + 227 whose block is held by a chain row that already has `moving_price`.
   Falls back to plain counts when the seed files are absent.
3. **`bars.upsert_bars` recompute rule (refines 02 §J1 / 17 §2.5 for order independence):** a bar rebuilt from
   `pool_history` with the **same source and the same close sample** (`ts_last` and `close` equal) now replaces the
   stored bar when open/high/low/volume/start_volume differ, i.e. the bucket gained rows after the bar was built.
   Without it, 23,959 1h `blockmachine` bars in the ledger window stayed flat single-sample bars although their hour
   now holds dense ledger rows — a final table that depends on run order (17 §9 test 6 promises independence). Competing
   sources are unchanged: equal rank still needs a later `ts_last`; `tmc_candle` (`ts_last` NULL) never recomputes; an
   identical recompute is still `same_rank_skipped`. Counter `recomputed` in the bars report.
4. **Regime port:** `app/quant/regime.py` = the old `strategy/regime.py` pure functions (constants, `_bucket_snapshots`,
   `_log_returns`, `_std`, `classify_regime`, `compute_regime_metrics`) per 12 §8.2 (`ts = float(parse_ts(ts_raw))`;
   `RegimeFilter`/debounce not ported) + `RegimeParams`, `RegimeResult`, `label_for` (thin → `UNKNOWN`).
   `REGIME_TS_1_0` and `V_W_BY_REGIME_TS_1_0` are defined **beside the port**, not in `app/scoring/spec.py` (that file
   is Phase 8's; 12 §8.1 lists `REGIME_TS_1_0` there — Phase 8 should re-export it from `app.quant.regime`).
   `regime_backfill.load_regime` prefers `app.scoring.spec` once it exists. `regime_for_slot` is left to Phase 8.
   Old line numbers differ from 12's citations (Pi copy is newer: config `REGIME_*` at 474-485), values identical.
   `tests/quant/test_regime.py` pins outputs generated once from the old module (4 regimes; ISO and INTEGER `ts` agree).
5. **Pi report:** the chain check and console table cover the 10 PC-open positions **plus every Pi `ema_positions`
   row still `OPEN`/`STUCK`** (`pi_recovery.tracked_positions`) — the Pi shows all 10 PC positions closed and two newer
   value positions open (457 SN40, 463 SN94) plus an old `STUCK` scalper row (195 SN30).
6. `scripts/backfill_all.py` enables S1/S5 for any primary ledger source (it only looked for `seed/ledger.db`).

### Ledger vs chain at identical blocks (finding)
Of the 48,096 non-root collisions, 98.8 % agree to 1e-6 in price; the small differences fit the old bot reading
state a block apart from the block number it recorded. The large ones (up to 17 %) are concentrated on one TaoStats
seed day, 2026-03-19 (45 rows at block 7,783,014). The chain rows won (first writer), so no ledger value replaced a
chain value. Root (netuid 0) prices differ by definition (ledger 1.0–4.04 vs reserve ratio) — 581 rows.

### Live results
- **S1** `import_old_ledger --phase import` (service down): `rows_in` 19,055,650 = `rows_new` 19,007,178 +
  `rows_preexisting` 48,472; `ledger_backfill` merged 23,010 (mp = sv, nra 0); verify: `ledger_rows` 7,362,854
  (imported 7,340,310 + held 22,544, missing 0) PASS, `ledger_netuids` 129 PASS, `ledger_backfill_merge` 23,237 PASS,
  golden row PASS, no live bar before `LEDGER_LIVE_FROM_TS` PASS. `observed_at` split over the PC span
  7,299,303 | 41,007 | 0 (= 17 §10 item 1's 7,321,749 | 41,105 less the held rows). DB 1.55 → 4.9 GB.
- **S5** `--phase stamp,bars`: 3,704,219 ledger rows stamped, 0 unstamped, 0 occupancy anomalies; bars written
  297,615, recomputed 94,857. §10 item 10: SN40 `40:3372582|deregistered|reoccupied|40:8409860`, no un-alerted
  backfilled event, no `backfill*` event source, 0 dense pre-05-30 hours without `moving_price`, 0 flat chain bars left
  in hours with ledger rows; only root `0:0` lacks `registered_ts` (Phase 3 item 14).
- **S2** `import_fundamentals`: 129 raw snapshots, 603 observations, `team_info_rows` 0, attributed 128 via occupancy +
  1 via 04's guard (root SN0); re-run → 0 observations. `whale_cohort.json` absent → `whale: not_found`.
- **Gap extension** `backfill_rpc --era gap` (service down since 2026-09-24): 435/435 hours (2026-09-24 18:00 →
  2026-10-12 21:00Z), 0 failed, 56,115 rows, 2,200 Blockmachine requests. Three subnets re-registered in that window
  (SN82 2026-09-26, SN116 10-04, SN92 10-09): stamping reported 652 mismatches = the new occupants' rows, because 04's
  registry had not run. `python -m app run-job registry_occupants` added them (04 wrote three live
  `netuid_reoccupied` warnings, routed when the service runs); a re-stamp → 0 mismatches, `newer_than_registry` 0.
- **S6** `backfill_tmc --tasks restamp,candles5m --redo` (in-process, `BACKFILL_TMC_QUIET_UTC=""` as in Phase 3
  item 13): the gap now starts at the Pi's last row, so 5m candles were re-fetched over 2026-08-21 → 2026-10-12 22:05Z.
  The first run was killed with the session after 45/128 netuids (its `collector_runs` row has no `finished_at`); the
  other 83 were redone with `--netuids` → 128/128 `done`, 0 failed. `tmc_candle` 5m 4,243,735 / 1h 209,124 / 1d 8,320
  rows; label `start`; 0 un-enriched 1h bars; 0 TMC bars with a non-chain block; SN64 TMC 1h close vs chain over the
  434 new hours: mean rel. diff 8.3e-5. `restamp` had nothing to do (all analytics units `done`).
- **S7** `backfill_regime` (ported classifier): 606 days 2025-02-14 → 2026-10-12; DISPERSED 449, TRENDING 155,
  UNKNOWN 2 (launch day, and 2025-05-23 = the chain halt, no hourly bars); 0 thin rows labelled ≠ UNKNOWN; all
  `backfilled = 1`. **Finding for 12/13:** CHOPPY and DEAD never occur with ~128 subnets (24-h cross-sectional
  dispersion is always ≥ 0.015), and TRENDING and DISPERSED share `v_w = 0.25`, so under ts-1.0 the regime scaling
  is effectively constant. The old bot's `data/backtest/regime_timeline.json` (daily seed, 24 h buckets, debounced)
  likewise shows only TRENDING/DISPERSED apart from one DEAD warm-up day.
- **S8** `build_coverage`: every `checks.*` passes (incl. the Pi-aware ledger checks, `subnet_volume_unit` 1.0,
  `rpc_golden_block`), `occupant_key_mismatch_rows` 0, unstamped 0, holes = the 3 documented 2025-05-21/22 chain-halt
  windows only, eras `pre_ledger`/`ledger` done (ledger now ends at the Pi's last block 8,889,356), `gap` partial
  (until live capture starts), `site_age` forward-only.
- **§10 item 18 / Pi report:** `ledger_pi` rows 19,007,178 ≥ `rows_beyond_pc` 11,692,796; the gap era starts at block
  8,889,357. `VALUE_KILLED` present. **TMC `/accounts/coldkey-stakes/` shape verified:**
  `{"nominator_coldkey", "subnets": [{"subnet", "balance_alpha", "balance_tao", "hotkeys": [{"address", …}], …}]}`
  with integer **rao** amounts — the tolerant parser found 0 rows (no `subnets` container) and would have read 1 rao
  as "still staked". Fixed: `subnets` container, rao → α/τ, hotkey from `hotkeys[0].address`; the report flags
  `dust_only` below `DUST_ALPHA` (1e-6 α) and lists `dust_netuids`. Result for the trader coldkey: **no position
  holds stake** (all 13 tracked rows 0 or 1 rao; 18 netuids hold 1-rao dust; free 0.000618 τ). The stored
  `coldkey-subnet-chart` shows ≈ 18.0 τ free + ≈ 5.37 τ staked (SN40 0.75, SN94 3.26, SN100 1.36) still present
  on 2026-10-02 00:00Z (block 9,194,687) and **everything gone by 2026-10-03 00:00Z** (block 9,201,887) — the day
  the wallets were recovered to `~/USB_Recovery/wallets_recovered` (operator to confirm the sweep was theirs).
- Tests: 192 passed (new: `tests/quant/test_regime.py` 9, Pi-only import, rebuild order independence, tracked
  positions, TMC rao shape). `python -m app check` → 17 available (phase 3). DB 5.1 GB.

### Still open for Phase 3
- PC ledger and `whale_cohort.json`/`whale_labels.json` are not recoverable here; `whale_cohort` stays empty (dormant).
- The service has not run since 2026-09-24; `coverage.gap` stays `partial` and the live era has no start until it
  runs (`sudo ops/install-systemd.sh && sudo systemctl start taosentinel`, plus the Phase 0 sudo steps).
- Disk: `data/tmp/` holds three DB backups (2.9 GB incl. `pre_seed_import_2026-10-12.db`) and
  `data/backfill/tmp/ledger.checkpointed.db` (2.3 GB, the WAL-checkpointed Pi copy) — delete when no longer wanted.

## 2026-10-16 — Spec 19 W0 (restart the data)

- `--post-only` prefix rebuild **not needed**: `price_bars` 1d already holds 8,185 bars for 109 netuids before
  2025-05-19 (exactly the un-barred count in `research/03-data-feasibility-ema-dereg.md` §6), written by the
  2026-10-12 S5 `stamp,bars` pass. Bars now start 2025-02-13.
- Gap top-up `backfill_rpc --era gap` (service down): 91/91 hours, 0 failed, 11,739 rows, `pool_history` to
  2026-10-16 15:59Z; bars written 11,132. Post-process reported 26 mismatches / `newer_than_registry` 1 = SN86,
  re-registered 2026-10-15 (`netuid_reoccupied` warning). Same fix as 2026-10-12: `run-job registry_occupants`,
  then `import_old_ledger --phase stamp` → 0 mismatches, 0 unstamped.
- `ops/install-systemd.sh` run by the operator 2026-10-16 18:50 (unit installed + enabled, not yet started).
  `python -m app check`: schema 2, quick_check ok, 17 available (phase 3). System timezone is still Europe/London;
  the app writes UTC epoch/`Z` timestamps, so it does not block the start (02's UTC step stays an operator item).
- Service started by the operator 2026-10-16 18:02Z: `/health` ok, 16 jobs + `chain_sse`, clean boot.
  `data/service.log` is created by systemd as root 0600 (the `append:` target), so the operator user cannot read
  it; the app's own `data/logs/YYYY-MM-DD.jsonl` is readable and holds the same records.
- **Bug fixed in `scripts/backfill_tmc.py`'s in-service path** (first use with the service up): it read
  `GET /api/v1/jobs` as a dict keyed by job id, but the route returns `{"jobs": [{"id", …, "status"}]}`
  (AttributeError after the trigger had already been accepted), and it waited on `running`/`note`, which the job's
  `status()` does not expose (`last_run` is set when a run ends). Now: `_job_status()` picks the job's `status`,
  the wait ends when `last_run` changes, the `partial:` re-trigger reads the note from `collector_runs`, and a 409
  other than `in_flight` (paused/disabled) exits instead of looping.
- `backfill_tmc --tasks restamp,candles5m` without `--redo` is a no-op here (all units `done` since 10-12), so the
  5m top-up runs as `--tasks candles5m --redo` (re-fetches the whole 2026-08-21 → now gap; idempotent upserts).
- 5m top-up: 127/128 in the first run (140,228 rows, 18:06 → 18:40Z); SN46 failed 3× with `OperationalError:
  database is locked` (concurrent writer), re-run alone with `--netuids 46 --redo` → done, 1,056 rows.
- Live capture confirmed: SSE connected, first hourly flush 19:00Z wrote 129 `chain_hourly` rows (`source='sse'`)
  and 1,677 `sse_rollup` bars. `chain_rpc_hourly`/`chain_rpc_metagraph` stay off (`SUBTENSOR_RPC_ENABLED=false`, 05
  default) — the SSE recorder is the live chain source, so live `pool_history` rows only come from backfills.
  `chain_features` 18:05Z failed once ("no snapshot for 18:00Z") because the service started at 18:02; next 00:05Z.
- Last gap hours 16:00 → 18:00Z (`backfill_rpc --era gap`, 2 buckets, run with the service up — the service makes
  no Blockmachine calls), then `build_coverage`: eras `pre_ledger`/`ledger`/`gap` all **done** (gap to block
  9,297,577), `live.from_ts` 2026-10-16T18:00Z, every `checks.*` passes. **W0 complete.**

## 2026-10-16 — 02 conventions: spec review, code audit, fixes

Read-only audit of the built code (Phases 0–3) against every section of 02 (four parallel reviewers: storage B/C8/J1,
events/alerts/control D/B12, jobs/config E/F, providers/health/backups G/I/H). No DDL or event-name drift: `models.sql`
equals 03's SQL, every emitter uses catalogued types with their required keys, and every F2 default matches. Changes
are listed in 02 §Z; the substance:

**02 amended** (spec 02 now says what the code and the earlier BUILD-LOG decisions already do):
- Blockmachine (G1/F2), the `tmc` request group, and the dropped `direct` alert class (D3, Phase 0 item 2).
- The full job-id list (E2, 53), the catch-up users and window fields (E1), and the meaning of the E3 windows.
- The 17 evidence keys (C8) and the integrity hold (B12).
- Wording fixes: B4 failed-row replacement, the B5 failed-row window, B11 `handle_lower`, D4 pre-alerted backfill rows.
- New **§K reconciles spec 19**:
  - routes on the existing app; `WB_UI_PORT` retired;
  - `source='chain'`;
  - per-horizon key names; immune ranks `not_applicable`;
  - `manual_notes.kind='chart_read'`, migration 0003;
  - `wb_discord_counts` retired in favour of 08's keyless `discord_6h`; `chain_probe_stale`.

  19's status line and changelog now point to §K, and its §11 operator questions stay open.

**Code fixed:**
1. `BLOCKMACHINE_MONTHLY_CAP_REQ=0` meant "no cap" (`ratelimit.py`, 01 §5). It now fails closed like every other cap
   (02 §F2). The `.env.example` comments are fixed too.
2. RPC backfill requests were never booked to `provider_spend`, and each script's limiter started at 0, so the 4.5 M/month
   Blockmachine cap reset per process. New `app/backfill/common.py:SpendBook` seeds the limiter from this month's
   `provider_spend.requests` and books every metered request (buffered, same UPSERT as 16's governor). It is wired into all
   `RpcGateway` constructions (`backfill_rpc`, `rpc_backfill` probe/era, `import_old_ledger`, `repair_rpc_prices`).
   Earlier runs are not back-booked; the whole history took ≈ 75 k requests, far under the cap.
3. `Retry-After` in HTTP-date form is now parsed (it used to fall back to 60 s).
4. `registry_identity` declares `providers=("subtensor_rpc", "tmc")` (it had none). The pre-run budget check stays on
   the *first* provider by design (`chain_sse` keeps streaming when the TMC REST budget is out), now stated in 02 §G1.
5. Pausing `alert_dispatch` / `telegram_bot` by exact id is refused (`control.NotPausable`, API 409 `not_pausable`,
   CLI exit 2). 14 and 16 disagreed; 02 §B12 settles it for 14, because under D3 `alert_dispatch` is the only pager.
6. `pause`/`resume` now emit `collector_paused` / `collector_resumed` (`source='api'`, alert `no`), as 14 specifies.
7. Catalog: the ops events emitted by 16 (`backup_*`, `disk_*`, `db_integrity_failed`, `service_recovered_unclean`)
   are catalogued with `source='ops'`, as the emitters already wrote. `gate_model_broken` dedupe = `gate_model_broken|{d7}`
   (05's weekly rule; 03 had `as_of`). 03's tables and `price_bars` CHECK/rank text now include `blockmachine`.
8. `AlertLatch`'s unused `dispatcher_active` parameter is removed (there is no dispatcher-inactive fallback, 02 §D3).

**Tests:** new `tests/test_conventions.py` (22: G1 vocabularies, B9 CHECKs, B2, D1/D2/D4, C2/C3/C8, E1/E2, B12, F2
defaults, fail-closed caps, Retry-After date, SpendBook across processes, B1 ms ISO). `pytest` → **214 passed**;
`python -m app check` → OK, 17 available (phase 3).

**Not done / open:**
- The running service still has the old code. Fixes 3–8 take effect on the next restart (`sudo systemctl restart taosentinel`).
- No code checks the 60 GB disk rule (02 §I1 keeps it as an operator requirement). With a 5.1 GB DB and the 12 GB local
  budget, only the newest few backup stamps fit locally. **Resolved the same day:** the operator raised
  `DB_BACKUP_MAX_TOTAL_GB` to 70 in `.env` (≈ 200 GB free). The full 7 + 8 rotation is ≈ 25 GB now (1.32 GB per `.gz`
  + one 4.8 GB `.db`) and ≈ 45 GB at the year-1 DB size. Applied live via `reload-config` (no restart needed). The
  code default stays 12 (02 §F2); a mirror is still recommended for off-disk copies.
- 01 §2.11 and 14 §2.4 still mention `direct`. 02 §D3 overrides them; the owner texts are re-synced when Phases 9/11 touch them.

## 2026-10-16 — Spec 19 workbench (W2–W6 built)

The spec was rewritten first, then built. 19 now states 02 §K itself, records the new chain facts, fixes the
algorithms exactly (window and seed, staleness, ranks, cull-line-zero) and marks W4b (06/08/09) as the owners' work.
02 §K, 01 §2.7/§5/`degraded_reasons` and 03 §3.8/§3.9/§3.10 are amended to match.

**Chain finding: immunity is now 432,000 blocks (60 d).** The new keyless `chain_constants_probe` read
`NetworkImmunityPeriod = 432000` at block 9,298,478. Public archive reads give 864,000 at blocks 9,246,000 and
9,270,000 and 432,000 at 9,285,000 and 9,295,000, so the change landed around 2026-10-12/14.
- **Not fixed:** `NETWORK_IMMUNITY_PERIOD_BLOCKS` (05) is still 864000, so 05's `immune_local`, `dereg_safety` and
  `dereg_risk_flag` treat 6 subnets as immune that the chain no longer protects: SN16, SN58, SN90, SN99, SN103 and
  SN70. TMC's `immune` flag is stale in the same way (SN16 at 837,617 blocks old still shows immune).
- The probe raised `chain_constant_drift` (05's type, immediate route). **Operator decision (19 §11 Q2):** set
  `NETWORK_IMMUNITY_PERIOD_BLOCKS=432000` in `.env`, then `reload-config`.
- The workbench already reads the probed value.
- Other reads: `LastRateLimitedBlock[0x02]` = the last registration (9,289,316 = SN86). The runtime-API lock cost is
  830.7 τ; fitted `I_eff` ≈ 28,800 blocks, i.e. the stored 57,600 × 0.5. `SubnetMovingAlpha` = 0.0003.

**Design change found while building:** σ for the EMA band is the RMS of `ln(close/EMA72)` over the last 72 bars.
- The first version used the std of one-bar log returns. AMM alpha prices are smooth bar to bar, so that band was
  ±0.1 % on SN64's 1 h chart, and 118 subnets averaged a 1 d band position of −1.3 with most pinned at the ±2 clip.
- With the dispersion around the EMA, the 1 d band positions span −0.98…+1.02 (mean −0.31) with none clipped.
- Rows also carry `warmup` when fewer than 2 × 72 bars exist (a young subnet's 1 d EMA is still dominated by its seed).

**Built:**
- `app/features/technicals.py` (job `wb_technicals`, hourly :12, catches up 7 daily slots);
  `app/features/dereg_distance.py` (`wb_dereg`, hourly :09); `app/collectors/chain_probe.py`
  (`chain_constants_probe`, 01:20Z); `app/collectors/wb_github_probe.py` (`wb_github_probe`, hourly :25, no token).
- `app/wb/{screener,vision,study}.py`, `app/api/wb.py` (routes on 14's app, POSTs behind `X-Auth-Token`),
  `frontend/wb/index.html` (`:8090/wb`, Plotly 2.35.2 from jsDelivr; checked in Chromium light/dark with no console
  errors), `scripts/import_legacy_results.py`.
- Migration 0003 `workbench`, 30 evidence keys (catalog 109), 4 events + routes, `PAUSE_GROUPS['wb']`, 22 `WB_*`
  settings, `/health` reason `chain_probe_stale`.
- Job table 57 (4 rows at phase 3 → 21 available).
- `anthropic==1.15.0` added (optional extra `llm`, lazy import; `requirements.lock` pinned) for the vision read:
  structured output, adaptive thinking, server-side fallbacks (`fallbacks="default"`); a refusal, `max_tokens` stop or
  bad JSON → `unreadable` note. Vision stays off: `ANTHROPIC_API_KEY` is empty and `WB_VISION_ENABLED=false`.

**Deviations from 02 §K (02 amended):**
- The network weather lives in its own `wb_dereg_network_hourly` table, not as new `chain_network_hourly` columns.
- `repo_pushed_days` uses `source='github'`.
- `mp_vs_spot` → `spot_vs_mp`; `spot_rank` added; `dereg_recovery_ratio` deferred.
- The upload is a raw-body POST, so no multipart dependency.
- `wb_technicals` has a single hourly trigger.

**Bugs found by the tests/live run and fixed:**
- The probe's per-step guard was `timeout + 5` (a hang test took 56 s). It is now exactly the timeout.
- `wb_github_probe` paced 45 requests at the no-token rate (~42 min) inside a 1,500 s timeout and wrote rows only at
  the end. It now writes per repo and has a 3,300 s timeout. The first live run was stopped and its `collector_runs`
  row closed by hand (`ok=0`, error says why).

**Live DB (operator note):**
- A technicals CLI run meant for a scratch copy ran against the live DB, because a shell env var did not persist. It
  applied migration 0003 (schema 3; `manual_notes` was empty; `quick_check ok`) and wrote this hour's and 8 daily
  slots' technicals rows (run `cli:1792186176`) with the final code.
- The running service (old code) tolerates the newer schema. It reports `schema_ahead` only at its next boot, and that
  boot will run the new code anyway.
- Then the verify steps ran through `run-job`:
  - probe: 9 constants, 1 drift event;
  - `wb_dereg`: 2,048 rows; pool 119, k₃₀ = 4, mean registration gap 7.5 d, next registration ≈ 6.3 d, lock 826 τ
    (−123 τ/day); bottom SN16/SN70/SN103; 1 `immunity_ends_7d` (SN36);
  - `wb_technicals`: rows already present;
  - a 3-request GitHub probe check: 3 ok + 12 absent links.
- Pre-launch SN59 (moving price 0) becomes the first cull candidate when its immunity ends in ~10 d.

**Study (exploration, `data/quant/results/wb_study_20261016.json`, 183 occupants):**
- Dereg rank and the cull gap predict death strongly: death-IC −0.24, and 98 % of deaths sat below their 3
  size-matched survivors at T−7 d (p ≈ 0).
- Their return ICs are negative (−0.10 at 7 d, t −4.0), which is mostly survivorship: culled occupants drop out of
  the return labels (now stated in the result notes).
- `dd_90d` IC₇ +0.047 (t 3.1), `rv_30d` −0.044 (t −2.6), the EMA band ≈ 0. Nothing here is a gate pass.

**Legacy:** the old bot's 123 result files were copied read-only to `data/quant/legacy/` with `index.json`.

**Tests:** 35 new in `tests/wb/` plus count updates → `pytest` **249 passed**. `python -m app check` → 57 declared,
21 available (phase 3).

**Open:**
- **Restart needed:** `sudo systemctl restart taosentinel`. Until then the running service has none of the new jobs
  or routes.
- Then `NETWORK_IMMUNITY_PERIOD_BLOCKS=432000` (Q2), and the other 19 §11 answers.
- W4b with Phases 4/6/9.

## 2026-10-17 — Phase 4: spec 06 GitHub collector (reviewed, built, verified live)

**Spec review first** (06 Changelog "2026-10-17 pre-build review"; 01, 03, 16, 17 cross-references updated):
- **Bug (config):** the operator `.env` ships `GITHUB_TOKEN=` empty above `GITHUB_PAT=…`. pydantic's alias lookup takes the
  first key present, so the PAT was **silently ignored** (`settings.GITHUB_TOKEN == ""`). `Settings` now falls back to a
  non-empty `GITHUB_PAT` when `GITHUB_TOKEN` is empty (06 §5). Also added the §5 validators the code lacked (ts-1.0 pins,
  metrics cron ≥ activity + 2 h, ranges).
- Settled with the operator's fine-grained PAT (≈ 20 read-only calls): authenticated 304s are free (`x-ratelimit-used`
  unchanged over three conditional GETs); `/repositories/{id}/{commits,releases,pulls,tags,community/profile,readme,
  stats/participation,git/trees,contents}` all work → activity is id-keyed, the "follow a 301 on a sub-path" rule is
  dropped; no `github-authentication-token-expiration` header → new key `GITHUB_TOKEN_EXPIRES_AT`; community profile has
  no `files.security`.
- New keys `GITHUB_STATS_INTERVAL_DAYS` (7: `/stats/*` are cross-checks only, −1.2 k req/day) and
  `GITHUB_TOKEN_EXPIRES_AT` (inventory 653 → 655).
- **Migration 0004** (`github_repos.activity_ok_at`, `stats_checked_at`): the `activity_stale` rule had no clock in v1.
- First sight is not a change: no `identity_repo_changed` (immediate page) / `repo_archived` / `org_repo_archived` on the
  first observation — otherwise ≈ 120 pages on run one.
- Declared-ref derivation from 04's stored `candidates_json` made exact (no `order` stored → from `field`; legacy double
  rows; TMC `owner_github` alternate). User-owned accounts: only subnet-related repos enter `R(o)`.
- Workbench hand-off: `wb_github_probe` stops once a token exists, so `github_metrics` now also writes `repo_pushed_days`.

**Built:** `app/collectors/github/` — `urlnorm, client (GitHubClient, GitHubQuota, QUOTA), resolver, orgs, activity,
spam, authors, metrics, html_probe, tmc_crosscheck, ghevents, models, jobs, cli` (+ `__main__`). The five JOB_TABLE rows
are now available (26 at phase 4; `backfill_github` is 17's S9 job, not built → `AVAILABLE_BY_PHASE[4] = 26`, becomes 27
when it lands). `app/ops/digest.py` prints "GitHub PAT expires in N d" (16 §2.13.4).

**Found by the live run and fixed:**
- Owner-only refs of *non-declared* sources (subnetalpha `github.com/Beam-Network` beside chain `orgs/Beam-Network/…`)
  stayed `new` forever → they now take the canonical owner's listing outcome (06 §2.3).
- 17 of the first 25 `source_disagreement` rows were groups backed only by `legacy` (the frozen old `fundamentals.json`)
  → legacy-only groups no longer count (06 §2.3). **The 17 rows stay in `events`** (the table has a no-delete trigger);
  they are `info`/digest and appear in 04's needs-review `source_disagreement_open` until resolved or superseded.
- The template `opentensor/bittensor-subnet-template` now 301s to `RaoFoundation/bittensor-subnet-template`
  (id 676155963) → originality was `never_observed` everywhere; the template is now followed by id (06 §11 item 12).
- The HTML probe re-queued a re-verify every day for declared stale strings that 301 to the known canonical name.
- Tag dates were looked up in the DB before the same pass's commits were written (extra API calls).

**Live verification (spec §10, on the laptop, 2026-10-17 ~15:00Z; service still on old code):**
- Item 2 `normalise` output exact. Item 3 dry-runs: SN64 `chain chutesai/chutes 200 id=884216433 (direct)`,
  `subnetalpha rayonlabs/chutes → 301 → id=884216433 (redirect) alias`, no disagreement; SN1 → `741209532
  macrocosm-os/apex`, `repo_renamed`.
- Item 4 (`run-job github_resolve`, 320 s, 365 calls, 175 served by 304): 357 per-source refs → 333 resolved, 19
  gone_pending (confirm on the next ≥ 5 h cycle), 2 org_empty, 3 placeholder; 1,267 repos known.
- Item 7 (`run-job github_org_enum`, 111 s): 110 owners, 0 failed; chutesai 54 public / 30 non-fork; primaries for 105
  (`beam-network/beam-core-public`) and 118 (`ditto-assistant/ditto-harness`); 359 repos in scope.
- Activity for SN64 and SN1 only (22 repos, 0 failures, 411 commits, 145 merged PRs, 239 releases/tags). Metrics
  printed (not written, so no slot is locked with partial data): SN64 `org_freshness` 2.0 d ≪ `declared_repo_freshness`
  86 d (the Chutes pattern); SN39 all ten `absent/placeholder`; SN105 `activity_stale` (its activity not yet collected —
  correct). Item 12 probe output exact.
- Not run yet: item 5 `/health`, items 8–11, 13–14 — they need the restarted service (and the bootstrap activity run).

**Tests:** 157 new in `tests/collectors/github/` (+ count/schema updates) → `pytest` **406 passed**; `python -m app
check` → 57 declared, 26 available (phase 4); live schema now 4 (`quick_check ok`).

**Open:**
- **Restart needed:** `sudo systemctl restart taosentinel`. The startup catch-up then runs `github_org_enum`,
  `github_activity` (the ≈ 12 k-request bootstrap; may end `partial` and finish at 20:30Z) and `github_metrics`; the
  official P2 rows come from the 23:30Z run.
- Operator: set `GITHUB_TOKEN_EXPIRES_AT=YYYY-MM-DD`; optionally move the PAT to the `GITHUB_TOKEN=` line.
- `.env` also has `GITHUB_REPO=https://github.com/Rudy1995T/SubnetTraderV2.git` (empty public repo, operator has admin);
  no 06 code uses it — waiting for the operator to say what it is for (16's deploy repo?). Nothing was pushed.
- 17's `backfill_github` (S9) after the Phase 4 verify.

## 2026-10-17 — Phase 5: spec 07 web presence collector (reviewed, built, verified live)

**Spec review first** (07 Changelog "2026-10-17 pre-build review"; 01 §2.12 rule 6/9 updated), checked against the built
01/03/04 code, the live registry and live archive.org:
- **Bug — due rule:** interval-only watermark (`event_at > now − 23.5 h`) made a URL checked at 00:10Z due again at
  23:40Z the same UTC day; the row hits `UNIQUE (…, check_day_ts)` and is dropped, so the URL was re-fetched every tick
  until midnight. Due now also requires no row for today.
- **Bug — docs candidates never retired:** 04 §2.9 explicitly never retires `web_autodiscover` rows, so the spec's
  `last_seen_at` refresh protected nothing. 07 now owns the lifecycle (revive on re-insert — the unique index spans
  retired rows —, retire on a changed candidate, on a declared docs row, after 14 d unlinked on an up parsed page).
- **Availability fallback dropped:** live, `archive.org/wayback/available` answers `{"archived_snapshots": {}}` for
  archived URLs (`macrocosmos.ai/`, even `example.com`), with or without `timestamp`. The CDX `www.` retry is dropped
  (SURT verified: `url=macrocosmos.ai/` returns the `www.` captures). CDX JSON shape verified.
- **Host exclusions from live data:** `vimeo.com` was the *resolved* website of 9 occupants and appears in 17 website rows
  (subnetalpha intro-video links — **a 04 finding**: the subnetalpha video field lands in `subnet_url`), plus 2
  `huggingface.co` model pages. Added with `youtu.be`, `discordapp.com`, `telegram.me`. Root netuid 0 excluded (06/12).
- **False positives designed out:** scam keywords count only when new versus the previous text; `connect your wallet` /
  `claim your reward` dropped (dapp copy + a redesign's `content_reset` would have paged as `site_hijack_suspect`).
- **Network gate:** re-probes before blaming our network; suspect rows are no longer written (stay due) — the old rule
  would also have tripped forever on a batch of genuinely dead sites.
- TLS by a verified (certifi) handshake + unverified read only after a verification error; reasons
  `expired/hostname_mismatch/expiring/untrusted/tls_fail/no_tls`. Spec §9's `DDDTDDDD` example contradicted §2.4's
  timeout rule (fixed). Q15 (01 `Http` gaps) closed: the built `Http` already had the body cap, chained errors and
  per-host `Retry-After`; added `extensions["elapsed_ms"]`, `ProviderLimiter.host_cooldown()`, and `web` no longer takes
  provider-wide `x-ratelimit-*` penalties (`MULTI_HOST_PROVIDERS`, cooldown cap 3,600 s).
- Deps: `tld` (already installed via trafilatura → courlan; offline PSL, `search_private=False`) instead of `tldextract`;
  trafilatura's vendored `readability_lxml` instead of `readability-lxml`; **`cryptography==46.0.7` is the only new
  install** (+ `cffi`, `pycparser`), pinned in `pyproject.toml` and `requirements.lock`.
- Config: the five `WEB_*` list defaults (empty since Phase 0) filled; §5 range validators added; `WEB_USER_AGENT`
  default no longer sends the literal `<operator>` placeholder URL. No new keys (inventory stays 655).

**Built:** `app/collectors/web/` — `classify, content, fetch, robots, wayback, check, jobs, cli` (+ `__main__`);
`app/features/web_metrics.py` (six C5 fns, cached loader, pure core, streak events, writer); `app/features/types.py`
(12's `MetricResult`, created ahead of 12). The three JOB_TABLE rows are available: `python -m app check` → 57 declared,
**29 available (phase 5)**, `quick_check=ok`; `tests/conftest.py BUILT_PHASE = 5`, `AVAILABLE_BY_PHASE[5] = 29`
(30 once 17's `backfill_github` lands).

**Found by the live run and fixed:** `taos.im` answers the collector (not curl) with WordPress.com's a8c-cdn
"Checking your browser… Javascript required" 403 — counted as down; vendor-neutral challenge markers added (§2.5.3), now
`bot_challenge`. `tls_ok` reason preferred `tls_fail` over `expired` for a cert that GET refused (chronollm).

**Live verification (spec §10, 2026-10-17 ~18:30Z; CLI in-process, service still on old code):**
- Item 2 `--once --netuid 64 --force`: `https://chutes.ai` `up_2xx`, TLS 32 d (Google Trust Services), 300 ms,
  Wayback first capture 2024-06-21, docs candidate `chutes.ai/docs/getting-started/quickstart`.
- Item 3 full pass: 155 URLs in **481 s**, 161 rows, 0 internal errors, no network suspect.
- Item 4: 162 rows today, `up_2xx`+`redirect_ok` **92.6 %**; 4 `dns_fail` (taohash.ai, scorepredict.io,
  dojo.tensorplex.ai, bitsota.ai), 4 `client_error` (two 402 = Vercel payment-paused silxinc.com, 404s), 2 `server_error`
  (Cloudflare 530), 1 `tls_fail`, 1 `bot_challenge`.
- Item 5: Wayback answered for 115/115 primary sites, capture found for **97 (84 %)**; `targon.com` reports 2000-04-08
  (the domain's previous owner — §11 Q3 limitation).
- Item 6: **56** `web_autodiscover` docs rows (unverified candidates; a few are whitepaper PDFs, which §2.3 counts).
- Items 11/12: 5 `site_cross_domain_redirect` (e.g. hotfloat.io → coldint.io, stakingalpha.com → investing88.ai,
  discord.hippius.com → a Discord invite — a secondary target), 1 `tls_expiring_14d` (`chronollm.crunchdao.com`, SN38,
  cert **expired 13 d ago** — the dking.bot-style red flag). `http://winnertao.com` (SN72 second site) has a cert on 443
  that expired 1,751 d ago (`tls_ok=0`; no event: not the primary).
- Item 7 previewed print-only with `--metrics --as-of 2026-10-18` (no rows written, so tomorrow's first-success slot is
  untouched): `site_uptime_30d` `failed insufficient_checks` 115 / `absent no_website` 13 (excluded-only, mostly vimeo);
  `tls_ok` ok 114; `site_age` ok 115; `site_freshness` ok 96 (12 `spa_shell`); `docs_present` ok 3 / **absent
  `no_docs_link` 50** / `docs_undetermined` 62 (candidates inserted today, not yet checked); `product_status` unset 128.
- Not run yet: items 8–10, 13–14 (need the restarted service and further days); item 1 below.

**Tests:** 82 new in `tests/collectors/web/` and `tests/features/test_web_metrics.py` (§10 item 1: ≥ 60 ✓, ≈ 30 s ✓) → `pytest` **488 passed**.

**Open:**
- **Restart needed:** `sudo systemctl restart taosentinel`. Today's URLs are not due again until 00:00Z; the next ticks
  check the 56 new docs candidates; the first official P3 rows come from the 00:05Z `web_metrics` run.
- 07 §11 Q7 **settled by the operator (2026-10-17): keep** — the 50 occupants whose landing page links no docs get
  `docs_present = 0.0` (`absent no_docs_link`).
- **04 finding (traced 2026-10-17):** subnetalpha's `video_url_*` fields hold Vimeo links. 04's classifier only knows
  YouTube as a video host, so a Vimeo URL is classed `ref_kind='website'`. It becomes the resolved winner of the
  **`youtube`** field for 11 occupants (SN1, 4, 9, 12, 13, 19, 22, 32, 39, 44, 56) and the watch builder maps any
  `ref_kind='website'` winner to a `platform='website'` row with `is_resolved=1`; the same links are also written as
  `subnet_url__extra_*` secondaries (17 active website rows). SN39 ("deprecated") has a Vimeo link as its resolved
  `subnet_url`. 07 excludes `vimeo.com`; the 04 fix (classify video hosts as video, never as website) is open.
- Optional: append a contact URL to `WEB_USER_AGENT` in `.env` (§11 Q9). sqlite3 CLI still missing for the §10 commands.

## 2026-10-17 — Phase 6: spec 08 community collector (reviewed, built, verified live)

**Spec review first** (08 Changelog "2026-10-17 pre-build review + build"), checked against the built 01/03/04 code,
the live registry and live probes of discord.com / t.me (≈ 20 keyless requests):
- **Event type:** 08 still said the gateway's fatal error reuses `collector_failing` and that no gateway type exists;
  02 §D1 (amended 2026-09-23), 03's catalog and 14's routing already define **`discord_gateway_fatal`**. Now used
  (state persisted in `job_state`, so a restart stays idle until the token changes); recovery → `collector_recovered`.
- **Bug — CSV retirement after restart:** the per-run `last_seen_at` touch walked an in-memory map that is empty after
  a restart while the CSV mtime is unchanged → 04 would retire every operator row 14 d later. The CSV is now parsed and
  synced on every `discord_6h` run (idempotent); remap stays mtime-triggered.
- **Bug — capped backfill:** watermark − overlap could re-read a capped page forever; a capped backfill now resumes
  exactly at the watermark.
- 01's limiter has `penalize()` (file-persisted) → Cloudflare ban uses it (the spec's synthetic Retry-After workaround
  dropped). Classification made total (`none`/`no_pollable_target`); root netuid 0 excluded (06/07/12); census rows
  `is_primary=1`; `discord_handle` values (personal usernames) never stored; `source_agreement` generalised to n sources
  without `legacy`; widget guild set = primary own-server guilds; heartbeats read by `finished_at` (PIT).
- Telegram markup re-verified live: plain-space separators (`23 326 members`), two `tgme_page_extra` blocks on bot
  pages, private-link counts in `tgme_page_extra`; a missing handle / invalid `+hash` answers **200** without a title →
  `not_found` only when Telegram's own markers are present, else `parse_ok=0` (a layout change must not become 0.0).
- discord.py 2.7.1 (already locked since Phase 0) API names verified; vanity now lands on `📣・announce` (type 5).

**Built:** `app/nlp/textnorm.py` (B11 shared normaliser), `app/collectors/{discord_api, discord_topology,
discord_channels, community_store, community_events, discord, discord_gateway, telegram}.py`,
`app/features/community.py` (8 C5 producers + 3 evidence fns; `register_producers()` → 0 before Phase 8),
`app/features/normalise.py::presence_ratio`, `app/scoring/spec.py` (ts-1.0 constants; 12 owns it from Phase 8).
No new config keys, no new dependency, no migration (03 shipped the three tables in v1). The four JOB_TABLE rows are
available: `python -m app check` → 57 declared, **33 available (phase 6)**; `BUILT_PHASE = 6`,
`AVAILABLE_BY_PHASE[6] = 33` (34 once 17's `backfill_github` lands).

**Found by the live run and fixed:**
- **Legacy-only targets point at previous occupants.** 5 of 6 Discord invites declared only by `legacy` opened the
  *previous* project's server (SN10 Pareton → "Sturdy", SN53 engy → "SignalPlus", SN94 Cathedral → "BITSOTA", SN109
  Finsight → "Academia", SN113 LongShort → "TensorUSD"; only SN80 `dogelayer` → "OpenRoboto" is current). 08 now
  ignores legacy-only rows (Discord and Telegram, 08 §2.2). **04 finding (08 §11 Q27):** these rows were first seen
  2026-10-16 17:27Z and attached to the current occupants despite re-registration; Telegram SN27 `neuralinternet`,
  SN53 `bittensor_subnet53`, SN106 `v0idai` look the same. 04's occupancy guard should cover them.
- Dead invites carried `meta.error='dead'` (error must mean failure) — removed.
- The first CLI cycle (before the legacy fix) was deleted (provisional 18:00Z slot: 154 snapshots, 384 evidence rows,
  128 `topo:` states) and re-run. Its 8 `invite_expired` events stay (no-delete trigger; the re-run deduped onto
  them); one concerns a legacy-only code now ignored (SN78 `TWmfwJAtXG`) — info/digest only.

**Live verification (08 §10, 2026-10-17 ~20:30Z; CLI in-process, service still on old code):**
- Item 0: `check` OK (schema 4, `quick_check=ok`); `jobs` lists the four rows as available.
- Item 2 (`--once`): 49 distinct codes, **49 OK, 0 failed**, 8 dead (10006: `chipforge`, `sN5NU32M`, `t7BMee4w`;
  50270: `2XSw62p9Fj`, `8pexneWef`, `N4F7UhEBY`, `adsQPnFRY`, `bvBDat3Gy`), invalid-request count 0. Classes
  own_server 26 · main_channel 26 · main_vanity 62 · none 11 · invalid 3 · unknown 0 (§10 baseline ±5). One vanity row
  per cycle; all instants INTEGER; 384 evidence rows (`discord_topology` ok 128, `inflated_days` ok 26 / n/a 102,
  `tg_members` n/a 128 — written before the Telegram pass). `source_agreement`: agree 11 · disagree 3 · single 26 ·
  one_dead 1. Reference guilds: SN106 Nodexo 1,975/128 · SN64 Chutes 5,966/1,260 · SN122 Cooking 10,935/1,643 ·
  SN101 Tag101 now **748/9** (was 1,905; below the 1,000-member inflation floor) · SN55 chain code → the NIOME
  lookalike 754/10 (`disagree`, Q8). `invite_expired` `expiring` fired for SN14 `SFt8s4gJD`.
- Item 3 (`--widgets`): 26 guilds, 6 enabled / 20 disabled (403 `50004`, 20 invalid requests ≪ 100); no
  `username`/`avatar`/`inviter` anywhere.
- Item 5 (`run-job telegram_daily`): 8 rows / 7 handles, all `parse_ok=1`: NATIX group 23,326/294, inference_labs
  7,476/87, daturavalidator 174 (SN22 + SN51), SN52 private_channel 252, SN34 private_group 474 `deprecated_marker 1`,
  SN25 `ursn25` group 203 (new), SN30 `wahoopredict_support` user_or_unknown with `title NULL`. No public channel → no
  posts yet (as the spec predicted).
- Item 6 (`--metrics`, print only): SN23 `venue_reachable 1.0` (main_channel), `discord_members` n/a; SN64 members
  5,966, presence/growth `failed` (1 sample, no 30-d base) — never 0.0; SN72 `tg_members 23326`. `--export-csv`
  wrote 25 main-server channel ids to `seed/discord_channels.csv` (gitignored).
- Not run: item 4 (needs `DISCORD_BOT_TOKEN`), item 7 (gateway; checklist B4/B5), items 8–9 (need ≥ 2 daily cycles and
  the restarted service).

**Tests:** 57 new (`tests/collectors/community/`, `tests/features/test_community.py`, `tests/nlp/`) → `pytest`
**545 passed**.

**Open:**
- **Restart needed:** `sudo systemctl restart taosentinel` → `discord_6h` at 00:00Z, `discord_widget_1h` hourly,
  `telegram_daily` daily; the gateway stays off until checklist B4 (`DISCORD_BOT_TOKEN`, `DISCORD_AGGREGATOR_GUILD_ID`,
  `DISCORD_GATEWAY_ENABLED=true`).
- Operator (checklist B4/B5, 08 §2.8): create the bot + private feed server, Follow `📣・announce` and own-server
  announcement channels, add `kind=announce` rows to `seed/discord_channels.csv`; record main-server channel ids, then
  `DISCORD_CHANNEL_MAP_REVIEWED=true` (until then the 62 vanity-only occupants get `venue_reachable None`).
- `.env` sets `DISCORD_USER_AGENT` to the literal placeholder `DiscordBot (https://github.com/<operator>/taosentinel,
  0.1.0)`; replace `<operator>` with a real URL before the bot token is added (Bot-token calls should identify the app).
- 04: legacy-only rows on re-registered netuids (Q27); SN80 `dogelayer` needs a `manual_overrides` row to count.


## 2026-10-17 — spec 19 W7: τ Terminal integration

Spec 19 was written before the τ Terminal (`~/tao-terminal`) had charts, wallets, trade history and insights. The
spec got §0.1b + §2.9 (W7) and the terminal now hosts the workbench; TaoSentinel code is unchanged.

- Terminal: `app/workbench.py` (client for `/api/v1/wb/*`, wallet join, pre-trade checks from
  `data/analysis/LESSONS.md`, held-subnet dereg alerts), routes `/api/workbench/screener` and `/api/workbench/checks`,
  `[workbench]` config section, 🔎 Screener tab (weather, sliders/presets/IC, sortable table with a "You" column),
  `emaband` indicator (JS + Python twins, equal to `technicals.overlay`), dereg legend line + cull lines, Positions
  "Risk" column, ticket checks (also in the confirm dialog). Tests 75 → 87. Service `tao-terminal` restarted (paper).
- Terminal bug fixed on the way: `#watchlist`/`#draw-toolbar` used `display:flex`, which overrode the `hidden`
  attribute (watchlist stayed visible under the ticket; an empty drawing toolbar pill showed on the chart).
- **Data finding (TaoSentinel, open):** `price_bars` 1 h and 5 m have no rows for 2026-10-17 00:00–11:00Z on all 129
  netuids while `chain_hourly` has SSE rows for those hours. The window is 12/432 bars, under the 10 % gap rule, so
  the `ema72_*_1h` rows stay `ok` but biased (SN71 `ema72_1h` 0.004881 vs 0.004796 from the terminal's chain candles
  at the same `as_of`). Needs a rollup/backfill of those hours (owner 05/17); the service restarted 19:12Z.


## 2026-10-17/18 — Phase 10 (spec 13 validation harness) built ahead of Phases 7–9

Operator asked to review/improve spec 13 and implement it now (13 §11 item 15). The old SubnetTrader source is now on
disk (`seed/app/backtest/`, `seed/app/utils/math.py`; old tests in the Pi recovery under `~/USB_Recovery/…/tests/`),
so this phase is a real port, not a re-implementation.

**Spec review (13 Changelog "2026-10-17 pre-build review")**, every item from the live data or the old code:
1. Price history is complete hourly from dTAO launch (`blockmachine`) and live (`sse_rollup` 1 h) — the live-era
   caveats and §11 item 1 were obsolete.
2. `blockmachine` (74 % of 1 h bars) was missing from the Δ-`subnet_volume` sources; `Bar.volume` is now the bucket
   *sum* of hourly Δs (last-write-wins kept one hour).
3. c2's control `emission_share` resolved to `raw:emission_share` = 12's norm rows, which don't exist → every name
   would be dropped. New chain-state factor `chain:emission_share` (previous day's `chain_daily` share).
4. 16's manifest key is `quick_check` (spec said UNVERIFIED `integrity`).
5. Trial key uses `start`/`end` as given → weekly re-runs no longer inflate N; `flock` on the trial log.
6. `insufficient_data` "fewer than 2·horizon dates" could never hold at h = 84 → `QUANT_MIN_IC_DATES` (10).
7. Zero emission: 41 % of `chain_daily` rows ≤ 1e-4 % (70 % in 2025-10, so Taoflow did zero outflow subnets);
   288 ≥ 7-day zero runs vs 55 deregistrations → dereg-only survival diagnostic + operator decision (13 §11 item 13).
8. Signatures kept ported (`segment_series` list, `build_cross_sections` 2-tuple) so the old tests run unchanged.

**Found while verifying on the real snapshot (spec updated):**
9. **Price-reset rule over-fires.** In the survivorship window 43 of 47 one-day collapses are crashes of living
   occupants (launches, 0–5 τ dust pools) and most of the 54 re-registrations show no collapse (SN82's is a price
   *rise*, depth 3 739 → 2 τ). With 0 unstamped bars the rule now fires only where the occupant is unknown; occupant
   boundaries are the death signal (`detect_dereg_events` keeps the pure rule as a cross-check: 118 collapses on the
   full history vs 43 reused slots).
10. **`chain_daily` is survivor-only.** None of the 54 occupants that died in the survivorship window has a
    `chain_daily` row (TMC analytics = current occupant only, Phase 3 item 8), so `chain:emission_share` (c2), `demis`,
    `gate_cross` and zero-emission deaths ignore dead subnets. Measured per result
    (`coverage.dead_occupant_chain_daily`) + warning `chain_daily_excludes_dead_occupants`; request to 05/17 to persist
    RPC `DynamicInfo` emission for every occupant (13 §11 item 16).
11. 05's `owner_locked_share` raw rows are all `status='failed'` (1 152/1 152) → `evidence:owner_locked_share` is empty.

**Built:** `app/quant/` — verbatim `economics.py`, `slippage.py`, `validation.py`; ported + extended `factor_panel.py`,
`factor_backtest.py`, `walk_forward.py` (factor track), `survival.py`, `dereg_events.py`, `report.py`; new
`occupants.py`, `pit.py`, `backtestability.py`, `panel_builder.py`, `labels.py`, `regimes.py`, `rank_stability.py`,
`trials.py`, `provenance.py`, `snapshot.py`, `gate.py`, `__main__.py` (CLI), `weekly_job.py`. Cross-cutting:
`mathutil.zscore` aligned with the old sample-std version (no other caller); 01 `JobSpec.timeout_key` (closes Phase 0
item 5; `QUANT_WEEKLY_JOB_TIMEOUT_S` is live); `QUANT_MIN_IC_DATES` + QUANT validators in `Settings`; `python -m app
check` prints `phase 6 + early quant_weekly`; tests carry `EARLY_JOBS`. No new dependency, no migration, no DB writes.

**Tests:** `tests/quant/` 135 (fixture DB built from 03's schema, 52 s) incl. the ported old factor tests.

**Verified on the 2026-10-17 backup** (13 §10): `snapshot` 14 s → `data/quant/snapshots/quant_taosentinel-20261017T122805Z.db`
(284 MB); live-DB and `.gz` refusals exit 2; `detect_dereg_events` 118 (≥ 6); `size,age,momentum` 2025-09→2026-06:
size t −2.8, age +1.9 (h 7) / +4.9 (h 84) — same signs as the old findings, smaller magnitudes (log returns,
occupant-aware panel, non-overlapping steps); `chain:emission_share` 52 dates; walk-forward survivorship 7 OOS folds,
live 3 (advisory); `report --run-all` writes a valid verdict (all of c1–c6 fail with reasons: no scores before
Phase 8; `pending: [c7]`); reproducibility check passes. These 9 `size/age/momentum` tuples are in the real
`data/quant/results/trials.json`; the other verification runs went to a scratch dir.

**Not done / open:** `/health` check needs `sudo systemctl restart taosentinel` (job stays disabled:
`QUANT_WEEKLY_JOB_ENABLED=false`). PIT studies (`quality_cg`, metric keys, scores) wait for Phase 8 + 17 S10
`replay_history`. Operator decisions: 13 §11 items 3 (gate operationalisation), 13 (c6 zero-emission), 14
(`chain_buys`), 16 (dead-occupant emission backfill). `rank_stability` parity with 12's `rank_stability_daily` is
untested until 12 exists.


## 2026-10-18 — Phase 7: spec 11 team & integrity (reviewed, built, spot-verified live)

**Spec review first** (11 Changelog "2026-10-18 pre-build review + live probe"), against 02, the built 03/04/05 code,
the live DB and ≈ 30 keyless requests (TMC SN64/79/3/15, Rufus, subnetradar):
1. **02 conflicts (02 wins):** H3 `sell_ratio > 1` applies only when earned ≥ `OWNER_MIN_EARNED_ALPHA` (02 §C1 amended
   2026-09-23; 11 still zeroed any principal sale while the cut is off) → new `failed`/`earned_below_min`, T25
   rewritten, §11 Q5 settled. `trigger_ids` use 02 §C2's `owner_changes:<id>` / `owner_stake_events:<extrinsic_id>`
   (11 had `chg:`/`ose:`). Evidence keys are **seven** (`owner_sell_ratio_30d` — 11 said 02 had no such key).
   `value_json` input keys renamed to 12 §2.4's (`n_verified/n_named`, `auto_lock/perpetual_locked`,
   `max_7d_sell_impact/full_exit/owner_cut_enabled`, `legacy_unaccounted`, `owner_coldkey/name/url/github`) so 12's
   reference functions reproduce `value` from `value_json` (T43); `owner_identity` is `prenorm`, not `binary`.
2. **Every UNVERIFIED shape resolved** (11 §2.3.1): all TMC amounts rao; C2 one object `{subnets:[…]}`; C3 bare list;
   C4 `{count,next,results}` newest-first, `ordering=block_number` ascending, **`FROM`/`TO` must be ISO — numeric values
   are silently ignored and return the whole history**; on `remove_stake*` rows `from_coldkey` is null and the owner is
   `to_coldkey`. Rufus `{subnets, data:{"<netuid>":{meta, owner{…pct in percent}, rows[]}}}`, baked-at slow (60 s).
   subnetradar is a server-rendered HTML table (no JSON); new owners are 0x pubkeys → SS58 via stdlib blake2b.
3. **Owner cut derived keylessly:** no TMC answer carries owner-cut-per-block, owner-cut-enabled or auto-lock; on every
   emitting subnet `pending_owner_cut / blocks_since_last_step` = 0.18 α/block (18 % of `subnet_alpha_out_emission`);
   SN74 shows 0 with emission on (cut disabled). C1 therefore runs in list mode (04's snapshot, 0 requests); detail
   only when the list is > 12 h old. Auto-lock comes from Rufus only.
4. **REST owner-lock fields are unreliable:** `owner_personal_*` are 0 in the list and were 646,674 α then **0** fifty
   minutes later in the SN64 detail answer → snapshots copy the SSE `chain_hourly` values (REST only as fallback).
5. **C5 dropped:** stake-removed rows have no coldkey and the `hotkey` filter takes `<ss58>-<netuid>` →
   `OWNER_STAKE_REMOVED_ENABLED` defaults false and the validator refuses true; reconciliation is info-only.
6. **`rpc_owner_history` collapse** (Phase 3 item 9): 324 non-first rows are flagged `owner_coldkey`, 191 are real
   changes (SN70: 4 flagged, 1 real). The seed compares consecutive states and ignores `change`; pre-registration rows
   are never a baseline (a test caught that bug).
7. **Team parser on the 128 live records:** the bare name rule kept ≈ 70 LLM headings ("Subnet ownership",
   "Rebranding and Evolution", "NVIDIA Inception Program"); word list + connective/lower-case/domain rules +
   `split_name_role` ("Xavier Lyu, Founder"). Yield 184 candidates in 43 named records (119 with a profile URL), 63
   unstructured, 12 placeholder, 9 anonymous, 1 stale-for-occupant (spec estimated 250–360 from all `<strong>` spans).
8. **Operator decision (§11 Q20):** SN64's owner sent 35–45 k α/month to coldkey `5F6D1y…` via `transfer_stake`
   (Feb–Jun 2026) ≈ its whole 38.9 k α cut → S1 ≈ 0 and H3 (red flag) in months above 1. Stake-event whitelists may
   now pin `dest_coldkey` (`--new-coldkey`), so a disclosed treasury key is one long-window row; alternative = ledger
   change excluding `transfer_out` from S1.
9. 03 deltas re-checked: `owner_changes.extrinsic_id` exists, the catalog has `owner_coldkey_swap_announced` /
   `owner_self_dealing_suspected` (11 §11 Q2 b/c were stale). `app/whale/events.py` source is not on disk (no
   `app/whale/` in `seed/app/` or the Pi recovery) → re-implemented against `/v1/`.

**Built:** `app/integrity/` — `constants`, `models`, `team_parse`, `team`, `team_seed`, `owner_sources`, `classify`,
`owner_metrics`, `detectors` (+ `drill` CLI), `whitelist`, `owner_admin`, `review`, `owner_collect`, `owner_watch`,
`owner_history`, `features`, `taostats_c8`, `health`; `app/features/text.py` (verbatim `_ANON_TOKENS` /
`_looks_anonymous` from `seed/app/strategy/quality.py`), spec 12's P4 reference functions in `features/normalise.py`,
`app/whale/events.py`, `scripts/manual_entry.py`, `scripts/owner_probe.py`; `/health.integrity`. Settings: Phase 7
validators (§5), `OWNER_STAKE_REMOVED_ENABLED=false`. No migration, no new dependency. `BUILT_PHASE = 7`,
`AVAILABLE_BY_PHASE[7] = 38` (39 once 17's `backfill_github` lands); `python -m app check` → 39 available (phase 7 +
early quant_weekly). Legacy notes copied to `seed/subnet_edge/` (checklist A3; gitignored).

Implementation choices: a manual `run-job owner_collect_daily` outside 20:30–23:30Z uses `started_at + timeout − 900`
as soft deadline (the window rule binds scheduled runs); `Collector.run(netuids=…)` for operator spot checks; coldkey
changes are recorded once per (occupant, new coldkey) within ±3 d and re-points per (occupant, new hotkey) so the
hourly watch, the daily collect and the RPC seed never double-penalise S3; subnetradar raw is stored as parsed rows +
sha256 (not the 485 kB page); `decaying_lock_added` also needs Δd ≥ `OWNER_PERP_OFF_MIN_ALPHA`; review error
`reason_required` (reject / whitelist without a reason) and team reason `not_seeded` added; `initial_alpha_est`
(TaoStats `NetworkRegistrationQueued`) not built — keyless S5 applies the uniform −0.2 (§11 Q7).

**Live (2026-10-18 ~11:00Z, CLI, service still on old code):** `owner_probe` 16 TMC calls → `data/owner_field_map.json`
(owner cut 0.18 α/block on 64/79/3/15; Rufus 119 subnets; subnetradar 47 rows). Team seed dry runs: 184 candidates;
legacy exactly SN120 "Const" (founder, `sn120_affine.md:14`) + one SN67 anonymity note, SN103 skipped (re-occupied).
Spot collect for 64/79/3/15 (12 TMC requests, **writes 4 real snapshots + watermarks**) and a no-write features
preview: S2 SN64 0.977 · SN79 0.612 (owner position grew to 1.65 M α; spec's 0.70–0.75 used 1.3 M) · SN3 0.0; S1 SN64
0.666 (13 k α transfer vs 38.9 k earned); S5 SN79 0.22 (post-dTAO −0.2); SN15 `team_doxxed` 0.0 ("kept private").
RPC seed preview (read-only): 5 active occupants changed owner coldkey in the last 30 d (SN102, 109, 112, 117, 47) →
`owner_control_change_trigger = 1` after the seed (subnetradar lists 112/117/47 too).

**Tests:** `tests/integrity/` 73 (T1–T45 incl. the ORO override, PIT whitelists, budget/pacing with a fake clock,
spec 12 anchors); full suite **744 passed**.

**Not run / open:**
- **Restart needed** (`sudo systemctl restart taosentinel`) → `team_seed_daily` 04:20Z, `owner_rpc_seed` 05:20Z (armed),
  `integrity_features` 06/12/18:07Z (+ catch-up), `owner_watch_hourly`, `owner_collect_daily` 20:30Z (first full run
  back-fills 35 d of activity per owner coldkey; ≈ 365 TMC requests).
- §10 steps 2/4–8 as written need the restart and 24 h of running; step 6 (drill on a 5 GB copy) not run (covered by
  tests). Operator decisions: §11 Q4 (S2 grace), Q20 (owner transfers / SN64 treasury whitelist), Q7 (−0.2 penalty).

## 2026-10-18 — Phase 8: spec 12 feature store & ts-1.0 scoring engine (reviewed, built, dry-run verified live)

**Spec review first** (against live schema v4 and the recovered old sources in `seed/app/`; full list in 12's
changelog 2026-10-18):
1. **Pre-launch rule broke history:** 04 sets `occupants.activated_at` = the registry run that first saw activity —
   all 128 live occupants carry 2026-09-25, 17's historical rows NULL — so "`activated_at ≤ slot`" made every slot
   before 2026-09-25 `pre_launch` (the §2.15 backfill would have had no composites). Status now uses the launch
   instant `min(activated_at, block_ts(first_emission_block))` via 17's `block_times`; unknown launch → active unless
   `status='pre_launch'` (12 §2.6, §11 Q22). Open for 04/17: 62 occupants share `first_emission_block = 5228683`
   (2025-03-28, the earliest block 04 saw).
2. **Historical regime:** `chain_hourly` has prices only on SSE rows (since 2026-10-16; analytics rows `price NULL`)
   → official slots reuse an existing `regime_daily` row (17's 606 S7 rows), `v_w` re-derived (12 §2.7 step 3a).
3. `score_runs` key is `(run_id, spec_version)` → one run id per slot (`sc-/cu-/bf-/cli-YYYYMMDDTHH-…`, `rs-…`);
   `n_errors` is a real 03 column (§4 case 19 said otherwise).
4. Registry check 9 compared pillars for legs/aux, but 03 files them under the consuming pillar → role + weight for
   all keys, pillar for scored keys; `registry_hash` also excludes the `absence_rule` text.
5. First success wins stated exactly: a failed raw row is upgraded only by a later *successful* row (03's trigger).
6. **Backfill rules added** (§2.13 item 5): a primary-spec backfill must not step `red_flag_state` (it holds the live
   state) nor call the forward-only 08/09/10 fns (≈ 10⁶ permanent failed official rows) nor emit events → in-memory
   flag state, no fns/derive, no events, stops before the first live slot.
7. Manual clear stores `ids(state) ∪ ids(this run)` (test-found: same-run evidence re-raised the flag next run).
8. `mathutil` is in `app.util` (spec said `app.quant`); `scores_status` async; stall check placement; golden fixture
   needs 57 more `x_engagement_30d` values (spec said 56). 03 already catalogues `owner_sell_ratio_30d` (Q20 stale).

**`mathutil` replaced by the verbatim port** of `seed/app/utils/math.py` 1-177 (Phase 2 had re-implemented it):
0-based `_average_ranks`, `pearson_r`/`spearman_r` 0.0 below n = 5 with tail alignment, `returns_pearson_r` ≥ 6 prices,
`effective_bets` → inf for a hedged book, annualised `rolling_volatility` (×√2190). Only caller of the 1-based ranks
(`features/technicals.pct_rank`) adjusted (same output); quant/wb suites unchanged (178 pass).

**Built:** `app/features/` `registry.py` (62 rows, import-validated, `register_metric`/`register_many`,
`EVIDENCE_FNS`), `normalise.py` (all §2.5 fns, `NORMALISERS`, `normalise_value`), `types.py` (+`RawRow`, `NormValue`,
`Occupant`), `store.py`, `derived.py`, `engine.py` (`FeatureEngine`, `FeaturesJob`), `__main__.py` (preview CLI);
`app/scoring/` `spec.py` (`ScoringSpec`, `TS_1_0`, `spec_hash`; 08's constants kept), `golden.py` (pinned hashes:
registry `8d0e7d0f…`, spec `1f24aa80…`), `red_flags.py`, `tiers.py`, `explain.py`, `events.py`, `stability.py`,
`engine.py` (`ScoreCalculator`, slot planning, `ScoringService` run/catch-up/kill/shadow/rescore/backfill,
`scores_status`, `ScoringJob`), `__main__.py`; `app/quant/regime.py::regime_for_slot`; `/health` awaits
`scores_status` and adds `scores_stale`. `p1_matrix` adapted to the spec `rankg`. No DDL, no migration, no new
dependency. `BUILT_PHASE = 8`, `AVAILABLE_BY_PHASE[8] = 40` (41 with 17's S9). Interpretation: a `failed` raw row is
"unobservable" for `repo_gone`/`all_repos_archived` (→ None, no clear-clock tick), not "present".

**Tests:** `tests/features/` +4 files (registry, normalise/rankg, PIT/store/derived, features job), `tests/scoring/`
golden worked example (every §2.14 number, rows, events — reproduced first time), flags state machine, units (tiers,
EMA, pillar port fidelity, velocity, 200 random Σ impact = Health − 50, spec hash, slot planning), service
integration (restart invariance over 10 slots, catch-up, kill, shadow, pre-launch/root, launch instant, immutability,
insufficient transition, score_jump 15.0/14.99, events off, backfilled payload, scoring_stalled latch, stability,
regime reuse, backfill, rescore refusals). Full suite **842 passed**.

**Live (2026-10-18 12:46Z, CLI; service still on old code):** `registry --check` → "62 metrics (45 scored: P1 10, P2 10,
P3 6, P4 7, P5 8, P6 4; 9 legs; 8 aux); pillar sums OK; hash … == golden". Schema: no `metric_norm`, 03's `scores`
columns, both append-only triggers. `run-job features_hourly` ok: **1,280 raw rows written** at as_of 12:00Z
(provisional) = 128 × (8 community fns + 2 derived), 0 fn errors, 11 X/P6 keys `producer_unavailable` (09/10 unbuilt);
root excluded. `scoring run --dry-run` (12:00Z, nothing written): 128 occupants, regime DISPERSED (v_w .25), SN64 A
81.3, flags SN16 `dereg_imminent`, SN35 + SN59(pre-launch) `stale_identity`, 3 insufficient; 1.0 s wall, 104 MB RSS.
Live data notes: Velocity is mostly the blend of `chain_buys` alone (`net_flow_30d` fails on 126/128 live,
`attention_7d` unbuilt) → the chain_buys zero-tie gives V ≈ 42.7 for many occupants (§11 Q6); P4 empty until
`integrity_features` runs on the new code.

**Not run (deliberate) / open:**
- No real `scoring_6h`/`scores` write: `scores` is append-only, the first official row seeds every EMA, and
  `red_flag_raised`/`tier_change` events would page via the running `alert_dispatch`. They start with the restart
  (`sudo systemctl restart taosentinel`; `scoring_6h` has `startup_catchup`, so the boot run scores the current slot,
  reconstructed if > 45 min late).
- After the restart: §10's run/explain/health/kill-drill/stability/power-loss items; the §2.15 backfill
  (`python -m app.scoring backfill --spec ts-1.0 --from 2025-09-22`) is an operator decision (§11 Q19) — it now stops
  before the first live slot.


## 2026-10-18 — Phase 9: specs 09 (X / KOL / RSS·News·YouTube) + 10 (LLM classification, P6) (reviewed, built, keyless parts verified live)

**Spec review first** (both changelogs "2026-10-18 pre-build review"), against the live DB (schema 4), ~30 keyless
probes, the bundled `claude-api` skill v2.1.301 (model table cached 2026-10-06) and the installed `anthropic==1.15.0`:
- **09 §2.9 — subnetalpha tweet feeds are gone.** 0 of 10 probed pages (then all 63 in the live run) carry a
  `.ctf-item`/`/status/` link; only the plugin CSS (`data-ctf-style="<feed id>"`) remains; 63 embed ids, not 74. The
  fallback is now gated on a fetch with ≥ 1 matched item within 3 d (`job_state('x_ctf_crosscheck','page:{occ}')`),
  otherwise `failed ctf_no_feed` — without the gate every handle-holder would have read "0 posts" → false
  `silence_45d`. With `X_PROVIDER=null` (default) there is now no X data at all (09 Q15).
- **09 §2.4 — `legacy` X rows never count.** 3 occupants have only legacy rows (SN113's `TensorUSD` is the previous
  occupant's account — the 08 Q27 problem), and live SN25 (UR) carries `MacrocosmosAI/legacy` beside its own
  `urnetwork` (found during the live check; the first "legacy-only" rule would have routed Macrocosmos posts to UR).
  One `x_accounts` row per account, team wins (`MacrocosmosAI` is a team handle and a KOL; `handle_lower` is UNIQUE).
- **09 §8 — module clash:** both specs claimed `app/nlp/rules.py`; 09's matching moved to `app/nlp/match.py`
  (JOB_TABLE fixes `nlp.rules:LlmRulesFallbackJob` for 10).
- **09 Appendix A** — feeds probed: Cointelegraph/Decrypt/The Block/A Bittensor Journey/0xSammy/Google News/Novelty
  Search OK; tao.media, SimplyTao, Own Your Mind, bex.co dead (404/500) → seeded `enabled=0`; Pine Analytics is
  general crypto → `filtered=1`. **YouTube own-channel test** added: subnetalpha `video_url`s are mostly third-party
  (SN1's is a taostats Novelty Search episode); ecosystem channels taostats + Opentensor Foundation (the playlist owner,
  found by the build) and channels shared by ≥ 2 occupants are never an occupant's own channel.
- **10 §2.3 — models re-verified at build time:** defaults → `claude-haiku-5-5` (bulk; $0.10/$0.50, Batch $0.05/$0.25,
  fixed id, adaptive thinking on by default → frozen `BULK_EFFORT="low"`, `max_tokens` 8192, blocks read by type,
  refusals = parse failures, cache minimum 512), `claude-sonnet-5-5` (escalation), `claude-opus-5-5` (narrative,
  $4/$20); Sonnet/Opus use `fallbacks="default"` (`server-side-fallback-2026-07-01`, new key `LLM_REFUSAL_FALLBACK`).
  Projected spend ≈ $21–31/month (was $37–48). `PRICES` updated (dated ids resolve to their alias). Async batch results
  form verified in the SDK signature. `LABEL_POLICY` is a module constant in `scoring/spec.py` (a `ScoringSpec` field
  would have changed 12's golden spec hash); `confirm_review(…, note=None)` because 11's built code calls it with `by=`.
- 01 §5 gained `LLM_REFUSAL_FALLBACK` and the new model defaults (config inventory 657 keys); `X_PRICES_JSON` now has
  the validator 09 §5 promised.

**Built** (three parallel build agents on disjoint files, then integrated here): `app/collectors/x_providers/`
(base, prices, twitterapi_io, official_v2, null, `make_provider`), `x_handles.py`, `x.py` (4 paid jobs, resolution,
§2.6 ingest/attribution, governor check→record, budget events, CLI), `x_ctf.py`, `rss.py`, `news.py`, `youtube.py`;
`app/nlp/` `match.py`, `kol.py`, `ambiguous_names.txt`, `schema.py`, `rubric.py`, `prompts/classify_v1.md`
(sha256 `e47d012b…`) + `narrative_v1.md`, `aliases.py`, `queue.py`, `llm_classify.py`, `rules.py`, `lexicon.py`,
`resolve.py`, `events.py`, `narrative.py`, `eval.py`; `app/features/` `social_x.py` (6 fns), `social_feeds.py`
(1 leg + 3 evidence fns), `sentiment.py` (4 P6 fns); `seed/kol_accounts.csv` (37 / 32 verified) + `seed/feeds.csv`
(both now whitelisted in `.gitignore`), `scripts/build_kol_list.py`, `scripts/record_x_fixtures.py`. No DDL, no
migration, no new dependency. `LABEL_POLICIES["ts-1.0"]` = bulk `claude-haiku-5-5:e47d012b:1`, escalation
`claude-sonnet-5-5:e47d012b:1`, fallback `rules:v2`. Tests: `tests/collectors/x/`, `tests/collectors/feeds/`,
`tests/nlp/` (14 new files + fakes), `tests/features/test_social_x|social_feeds|sentiment_metrics.py`; fixtures for
X (synthetic), ctf, rss, youtube (trimmed live + synthetic), llm, the 40-item synthetic golden set. With Phase 9 every
phase 0–10 is built → `tests/conftest.py` `BUILT_PHASE = 10`, `EARLY_JOBS` empty, `AVAILABLE_BY_PHASE[9] = 52`
(Phase 9 has 12 jobs, not 13), `[10] = 53` (54 once 17's `backfill_github` lands). Full suite **1102 passed**
(final state, after the `rules:v2` and legacy-row fixes).

**Notable build deviations** (details in the agents' notes, kept here in short):
- 09: one provider call = one HTTP page (§8.2's one governor check per page wins over §2.3's internal paging);
  `since_id` advances only after a fully successful run; ctf upserts never NULL a paid provider's engagement
  (`COALESCE`); ctf/feeds "last OK fetch" lives in `job_state` (no DDL column); YouTube channel discovery runs daily,
  capped at 40 lookups/run (a Sunday-only run of ~127 rows would exceed the 900-s timeout); redirects followed manually
  (Http returns the original URL); the KOL seed is lazily loaded by `x_kol_poll` when `kol_accounts` is empty;
  `author_credibility` takes `db_dupes` (same author) and `db_coord` (other authors).
- 10: confirmations are stamped `payload.confirmed_at`/`confirmed_post_ids` and `event_impact_14d` reads those
  (an upgraded pre-existing event's `observed_at` would leak a confirmation into earlier `as_of`s); every escalation
  dead end (cap, timeout, purged text, 3 tries, refusal) gets a review row; the rules fallback is bounded to items
  ≤ 30 d old; `count_tokens` runs after a successful submit, so a budget denial makes zero API calls.
- **Rules classifier → `rules:v2`.** The §8.4 regexes as written scored macro-F1 0.442 on the 40-item golden set
  (target 0.45); widened ("socially engineered", spot/perp listings, "stepping away", "unstaked … exchange",
  integrations, regulatory, emissions) and re-ordered. The 0.976 that follows is tuned on the same synthetic items —
  **not** a generalisation estimate; the real check is `eval bootstrap` on week-1 items (checklist D6). The live run then
  exposed two precision bugs: "Preventing Exploits" (a video chapter) → `security_incident`, and "liquidity
  injections" in a TAO explainer → SN77 Liquidity. Fixed (incident wording required; ordinary-word names get
  confidence 0.5 < 0.6 unless a subnet cue is adjacent) and the version bumped to `rules:v2`, because 72 `rules:v1`
  labels had already been written (append-only). v1 is not in `LABEL_POLICY`, so it is invisible to every metric.

**Live verification (CLI, service still on old code; keyless only — no X key, no Anthropic key):**
- `check`: 57 declared, **53 available (phase 10)**, 4 unavailable (17's `backfill_github`, 3 × Phase 11).
- `build_kol_list --load`: 37 / 32 verified (09 §10 item 2). `MacrocosmosAI` team keys corrected to SN1/9/13 (SN25
  and SN37 are now UR and Aurelius) in the seed, the script and the live row.
- `run-job x_ctf_crosscheck`: `pages=63 with_feed=0 items=0`, 62 s, 63 page states all `(200, 0 items)`.
- `run-job rss_feeds_poll`: 7 feeds, 5 OK, 40 new items. **The two `*.substack.com` feeds answer 403
  `cf-mitigated: challenge`** to the app's Python client while curl gets 200 — Cloudflare keys on the TLS fingerprint,
  not headers (tested with four header sets). Not evaded; both feeds (Pine Analytics, filtered; Subnet Edge, dormant
  since Feb) will reach `feed_dead`. Custom-domain Substacks work.
- `run-job news_google_poll`: 42 queries all OK, 322 items, 155 attributed, 411 s.
- `run-job youtube_poll`: Novelty Search playlist 13 videos (all rule-attributed); 46 video channels resolved, **0 own
  channels** (31 shared, 15 ecosystem — one channel sits on 12 occupants' videos) → `video_uploads_90d` absent for
  now (09 Q17); 35 `youtube_channel_resolved` info events.
- `run-job llm_rules_fallback` (LLM off): 72 items labelled (news/video ≤ 30 d) under v1, then again under v2.
  Events: v1 produced `security_incident` **warnings 1081/1082 (SN10, SN19) — false positives, already taken by the
  running alert dispatcher → they will appear in the next digest; ignore them** (events cannot be edited after
  alerting); v2 produced info events only (`revenue_customer` SN10/SN19).
- Metric preview (`python -m app.features … --netuid`, nothing written): SN64 X legs `failed ctf_no_feed`, SN113
  `absent no_x_account` (legacy-only), SN4 `event_impact_14d` 1.57 (rules label), `attention_7d` `baseline_short`
  (2 trending days), `kol_coverage_30d` `kol_poll_stale` (X off → 12 falls back, and the fallback fails too).

**Not run / open:**
- Anything paid: X (no key — provider shapes are synthetic, `FIELD_MAP`-isolated, 09 §9.2 live tests and
  `record_x_fixtures.py` pending) and Anthropic (no key — Haiku 5.5 `effort`+`format` inside Batches and the beta
  `fallbacks` call are unverified until the 10 §9 network smoke tests run).
- **Restart needed** (`sudo systemctl restart taosentinel`) for the 12 Phase 9 jobs to be scheduled; the rules
  fallback then runs hourly and the free collectors on their crons.
- Operator: choose an X plan (09 Q1 — now the only X source), add `ANTHROPIC_API_KEY` + `LLM_ENABLED=true` when wanted
  (≈ $21–31/month), confirm KOL roles (checklist D3), add YouTube channel rows for subnets (09 Q17), golden-set
  labelling after week 1 (D6). 04 follow-ups: legacy X rows on re-registered netuids (09 Q16 / 08 Q27).


## 2026-10-18 — Phase 11: spec 14 (HTTP API, SSE, exports, Telegram commands, web UI) (reviewed, built, tested offline)

**Spec review first** (14's changelog "2026-10-18 pre-build review"), against the built Phases 0–10 code and the laptop:
- **UI → `/ui` on :8090, no Node.** The laptop has no Node.js; 19's `/wb` already ships as one static page on this app and
  02 §K1 retired the second listener. The Next.js fork on `:3001` is replaced by `frontend/ui/{index.html,app.js,style.css}`
  (plain JS, inline-SVG charts, no CDN) served by `app/api/ui.py`. Same origin → no CORS entry, no baked
  `NEXT_PUBLIC_API_TOKEN` (the token lives in the browser's `localStorage`; 14 §11 Q1/Q2 resolved), no frontend unit.
  `UI_BASE_URL` default → `http://127.0.0.1:8090/ui`; `FRONTEND_PORT` retired (key kept). 16's opt-in
  `taosentinel-frontend.service` / `--with-frontend` are now dead: do not install them.
- **Routing re-synced to 02 §D3 (amended) and §K5:** no `direct` class (`disk_critical`/`db_integrity_failed` were
  already `immediate` in code), 19's four workbench types listed; the "dispatcher inactive → send direct" text removed.
- **Factory:** the built `create_app(get_runtime)` + per-router `configure()` pattern kept; `ApiContext` dropped.
- **Rescore:** 12's `ScoringService.rescore` is synchronous and mints its run id at the end → the route refuses
  synchronously (`spec_unknown` 422, `shadow_requires_other_spec` 422, `scoring_killed` 423, `rescore_running` /
  `slot_already_scored` / `slot_regresses` 409), then runs the slot as a background task and answers 202 with a
  `request_id` (also `rescore_requested.run_id`; 14 §11 Q5).
- **Origin guard** admits the request's own origin (browsers send `Origin` on same-origin POSTs — the `/ui` and `/wb`
  forms would otherwise get 403) and now also guards 19's three POSTs.
- **Error shape:** one handler set gives every 4xx/5xx `{error, detail}`; Phase-0 `HTTPException(detail="<code>")`
  and 19's `detail={"error":…}` keep their `detail`, so existing clients/tests are unchanged.
- Quant kinds from the real file names (`gate_verdict_` → verdict, `ic_series_*`/`trials.json` sidecars hidden, 19's
  `wb_study_`); 11's `reason_required` mapped; provider id `telegram`; pause body/replies are 16's built ones (`hours`).
- Events are ordered by `id DESC` (a total order, so `before_id` keyset paging is exact).

**Built:** `app/api/{errors,common,queries,scores,subnets,events,export,quant,hub,stream,manual_service,manual,review,
registry,ui}.py`; `health.py` gets the 12 service probes (4 active, 4 passive, 4 local; response cached
`HEALTH_SERVICES_CACHE_SEC`); `control.py` gets `/control/rescore`; `app.py` rewired (spec header via a pure-ASGI
middleware so SSE is not buffered); `app/notifications/telegram.py` gets the ported `TelegramBot` (Pi-recovery source,
HTTP through `Http.request("telegram", …)`, chat allowlist `TELEGRAM_ALLOWED_CHAT_IDS`); `commands.py`
(`CommandDispatcher`, 19 commands, nonce confirmations), `bot.py` (`telegram_bot` LongTask, idles without a token),
`watch.py` (`dereg_watch`), `digest.py` (`needs_review_digest`, watermark below the oldest unrouted row);
`format.py` += `bar/dot2/num/cut`. No DDL, no migration, no new dependency.

**Tests:** `tests/api/` (conftest seeding SN64/SN47/SN130/SN9; `test_reads` 16, `test_writes` 10, `test_stream` 5) and
`tests/notifications/` (`test_commands` 11, `test_jobs14` 3). The feed is validated against a test-local mirror of
15 §3.2's `FeedPayload` (15 unbuilt). Two existing tests updated for Phase 11 (`test_control_routes` takes its
unbuilt job from `rt.unavailable`; the stagger test no longer expects `telegram_bot` unavailable).
`tests/conftest.py` `BUILT_PHASE = 11`, `AVAILABLE_BY_PHASE[11] = 56` (57 once 17's `backfill_github` lands).
Full suite **1147 passed**.

**Not run (deliberate) / open:**
- Nothing live beyond read-only GETs: the running service still has the old code. `sudo systemctl restart taosentinel`
  brings the routes, `/ui`, the bot (needs `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID`), `dereg_watch` and the Monday digest.
- `WATCHLIST_NETUIDS` is empty → `dereg_watch` pages for nothing until set (14 §11 Q6).
- 14 §10 verify items need the restarted service and real scores (none exist yet: `scores` is empty until the
  restart's first `scoring_6h`).

---

## 2026-10-18 — Phase 12: spec 15 (trading-bot integration) — reviewed, retargeted, consumer built in `~/tao-terminal`

**Review finding that drove the rewrite.** 15 patched the *old* SubnetTrader (`E:\SN_Trader`, `EmaManager._enter`
:2146, `ema_positions` ALTERs, `.env` `SENTINEL_*`, `/health` on :8081). That bot is not being revived: the agreed
plan (`~/tao-terminal/docs/SUBNETTRADER_V2_PLAN.md` §3.1) builds v2 inside the τ Terminal as `tao-bot`, reusing its
`Trader`. 15 is retargeted there (its new §0 table): the hook wraps `Trader.preview` (base preview → factor →
re-preview, so every base gate is decided on the base amount and the sentinel can only shrink an approved entry), the
audit is a `sentinel_decisions` table (`base_tao`, `amount_tao` ⇒ applied ratio exact, `trade_id` → `app_trades`),
config is `config.toml [sentinel]` with `POST /api/sentinel/reload`, alerts are desktop + SSE, and there is no
systemd ordering (user unit vs system unit cannot be ordered; the consumer is neutral without the producer).

**Bugs found and fixed**
1. **14's `/scores/latest` ETag ignored `stale`.** When the TTL passed without a new row, the body's `stale` flipped
   to true but the ETag did not, so an `If-None-Match` consumer got 304 forever. `app/api/scores.py` now hashes
   `is_stale(max_as_of, now)` into the tag; `test_scores_latest_killed_and_empty` asserts it (verified to fail
   without the fix). Needs the TaoSentinel restart to go live.
2. `INSERT OR IGNORE` also swallows NOT NULL violations: a `score_feed_log` row without `stale` vanished silently
   (caught by the telemetry test). `log_feed_rows` defaults the NOT NULL columns.
3. A halved order could fall under the 0.01 τ chain minimum → floored at `[trading] min_tao_per_trade`.
4. `ttl_seconds > 86400` rejected the whole batch → wire range 1..30 d, effective TTL still capped by `ttl_max_sec`.

**Contract checks:** a real body from 14's seeded test app (`build_feed`, `include_insufficient=1`, and the empty
spec) validates with the consumer model and is the terminal fixture `tests/fixtures/scores_latest_producer.json`;
13's verdict keys (`gate_pass`, `pending`, `promotable`, `upsize_gate_pass`, INTEGER `generated_at`) and c7 reader
(`schema`, `days`, `entry_coverage`/`live_signal_coverage`, `n_entries`, `entries` length) match `app/quant/gate.py`
and `__main__.load_telemetry`; the five default block flags exist in `FLAGS_TS_1_0`; `SCORE_FEED_TTL_SEC` is already
25200 (old Q1 resolved).

**Built (in `~/tao-terminal`, not this repo):** `app/sentinel/{models,kill,factor,gate,settings,store,client,storage,
hook,telemetry,runtime}.py`; `termdb.connect` creates `score_feed_log` + `sentinel_decisions`; `server.py` starts a
`SentinelRuntime(consumer="terminal")` (a bad `[sentinel]` section starts it disabled), adds `/api/status.sentinel`,
`GET /api/sentinel[/{netuid}]`, `POST /api/sentinel/{kill,resume,reload}` and a TaoSentinel advice line in the ticket
checks (buys; never resizes manual trades); `config.example.toml [sentinel]`; README section. 82 new tests
(`tests/test_sentinel_*.py`, `tests/sentinel_util.py`, `tests/conftest.py` resetting the process-wide gate cache);
terminal suite **169 passed**. Pre-change copies of the touched terminal files are in the session scratchpad
(`tao-terminal-pre-spec15/`); the terminal is not a git repo. This repo: only the ETag fix + test, the mirror-model
comment in `tests/api/test_reads.py`, spec 15 rewritten.

**Live:** `systemctl --user restart tao-terminal` done — `/api/status.sentinel` = `enabled false, mode_effective off`,
tables created (A2/A3 pass). The feed itself is **not** live: TaoSentinel's running process predates Phase 11, so
`/api/v1/scores/latest` is 404 and `scores` is empty. `[sentinel] enabled` left false (enabling now would only log
failed polls and raise a "feed down" alert after 2 h).

**Open:** `sudo systemctl restart taosentinel` → first scoring slot → 15 §10 T1–T3 (T2 saves the live fixture) → set
`[sentinel] enabled = true` for feed liveness (L1–L4). Phase B needs v2 B1 (the bot engine), which needs the v2 plan
§9 answers. New operator questions 15 §11 Q16 (paper entries count for c7), Q17 (min-trade floor), Q18 (two pollers).

---

## 2026-10-18 — Audit after Phase 12 (service restarted on Phase-12 code, first scores, spec 20 written)

**What happened.** The operator restarted the service at 20:11Z (`boot classified: clean`); the startup catch-up ran
`integrity_features` and `scoring_6h` at 20:16Z → run `sc-20261018T18-b24573bd` (as_of 18:00Z, `provisional` /
`reconstructed`, 128 universe, 128 scored, 1 pre-launch, 3 insufficient, 13 events, 0 errors, 6 s). First real
`scores` rows: tiers A 7 / B 27 / C 48 / D 26 / F 19; regime DISPERSED; confidence mean 0.56; flags `dereg_imminent`
SN16/40/47/70/103/127 and `stale_identity` SN35/39/59/70/103/113. `/api/v1/scores/latest` answers 200 (124 rows, ETag,
`stale=false`). First official slot = 2026-10-19 00:00Z. Both suites green: **1147 passed** (8 min 07 s) here,
**169 passed** in `~/tao-terminal`.

**Findings (details and fixes in `specs/20-consolidation-and-next-steps.md` §1.5):**
1. No alert channel: `telegram_configured=false`, 8 immediate alerts pending, 35 critical events never delivered.
2. Host prep (checklist A1 / 16 §2.2) never ran: timezone Europe/London, sleep targets not masked, lid switch default,
   sqlite3 CLI absent; `systemctl` reports `NeedDaemonReload=yes` for `taosentinel.service` (rendered unit changed on
   disk; content identical to `ops/systemd/taosentinel.service` after placeholder substitution).
3. `chain_daily` has no rows 2026-09-25 → 2026-10-09: the service was down and the TMC 1Y analytics step (17 S3) was not
   re-run after W0; `tmc_daily` writes only the trailing 7 days (`analytics_1w`). Effect: `net_flow_30d` is
   `failed:insufficient_history` on 126/128. Fix = `python -m scripts.backfill_tmc --tasks analytics --redo` (spec 20 N1).
4. P4 is `null` for every occupant at 18:00Z (catch-up ordering: `integrity_features` ran in the same minute as the
   slot). Raw rows now exist (`owner_commitment` ok 117; `control_stability_90d` 124 × `no_owner_snapshot` until the first
   `owner_collect_daily`; `team_doxxed` failed 118 because the 11 team seed was only dry-run).
5. Metric coverage at the first slot: 24 of 45 scored metrics populated for ≥ 90 occupants. Empty for free-to-fix
   reasons: `venue_reachable` 61 × `main_vanity_unreviewed` (B5 not done), announcement legs (no Followed channels,
   B4), `participation` (`TMC_NEURONS_ENABLED=false`), `holder_dispersion` (`tmc_weekly` not yet run on new code),
   `product_status` (manual D2). Empty for budget reasons: every X leg, `polarity_7d`, `kol_coverage_30d`.
6. 45 `Settings` keys are declared but never read anywhere (list in spec 20 §5.1); dead files
   `ops/systemd/taosentinel-frontend.service`, `deploy/taosentinel-update.*` (no `auto-update.sh`),
   `app/quant/dereg_events.py` (no caller). `README.md` still describes Phases 0–2.
7. Disk: `data/tmp` 3.1 GB and `data/backfill/tmp` 2.4 GB of superseded DB copies; 4.9 GB uncompressed backup twin;
   194 GB free.
8. Git: only Phases 0–4 are committed; 41 modified + 152 untracked files (Phases 5–12, spec 19) on a public remote.
9. The terminal's `config.toml` has no `[sentinel]` section → the spec 15 consumer is off although the feed is live.
10. `wallet_dirs_found = 3` in the security self-check (the terminal's wallets share the host) — accepted deviation
    from 16 §2.14; the terminal stays paper-only on this box.

**Changed in this entry (docs only, no code, nothing written to the live DB):** `specs/20-consolidation-and-next-steps.md`
(new: as-built record, work packages N0–N9, operator inputs in order, decision table, bloat inventory);
`specs/INDEX.md` statuses for 06/07/08/13/17/16/19 corrected to the built state and the spec 20 row added;
`CLAUDE.md` added at the repo root (session rules + live-system cautions). Nothing was restarted.

## 2026-10-20 — spec 14 amendment: Telegram bot screens rebuilt for a phone + read-only `/wallet` view

**Why:** the operator used the bot on a phone and found the screens "don't make much sense and look ugly", and asked for
a wallet view (positions, PnL, performance, insights). Screenshots showed every `<pre>` table wrapping (rows of 37–75
characters on a ≈ 34-character phone line), `⬅️ Menu` opening `/health`'s 57-row job table, and `/top` answering
"No scores for ts-1.0 yet" while `/health` reported a score at 18:00Z.

**Spec first:** 14 §2.7 (phone layout, navigation), §5 `TERMINAL_API_URL`, §8.4 (commands/callbacks), new §8.4a
(wallet view), §8.5 (every read screen re-specified), §10 verify line, Changelog; 01 §5 inventory (658 keys);
19 §1 non-goal clarified (the bot reads the terminal's API read-only; TaoSentinel stores no wallet data); INDEX row.

**Reviewed against live data before building (findings):**
1. `/top` empty: the 18:00Z provisional catch-up run (written 19:52Z after three `process_crash` recoveries at
   19:44/19:57/20:11Z) has P1, P2, P3 and P5 null for most occupants → confidence median 0.09, 0/126 active rows reach
   0.4, so the default feed filter hides everything. The 00:00Z official slot passed only 25/127. The bot now says
   exactly that and offers `Show anyway`; **the scoring coverage drop itself is not fixed here** (open item: why the
   chain/builder/product features were missing for those slots). Lead: `chain_features` failed at 19:53:35Z with
   `RuntimeError: no snapshot for 2026-10-20T18:00:00Z (SSE and fallback both down, no repair yet)` — the crash loop
   left no chain snapshot for that slot, so P1 could not be computed; cf. finding 4 of the 2026-10-18 audit.
2. `/movers 7` empty: only two official slots exist (19 and 20 Oct). The screen now names the first official slot and
   offers 1d/7d/30d buttons; 1d already shows real movers.
3. The τ Terminal (127.0.0.1:8787) already serves everything the wallet view needs without a key for GETs:
   `/api/wallets`, `/api/positions` (avg-cost positions, `sim_swap` exit value, unrealised/realised, emissions),
   `/api/insights` (persona, trader score 1D/1W/1M/1Y, ~40 cards), `/api/trades` (≈ 0.9 MB for Main's 1,452 events,
   under the 5 MB body cap; insights ≈ 0.7 s). `Http.request` exempts loopback hosts from provider buckets, so provider
   id `terminal` needs no 02 §G1 row; the setting's validator keeps it loopback-only.
4. The terminal's Insights `Volume` stat (11,519.8 τ for Main) is turnover — not shown (operator rule 2026-10-16).

**Built:** `app/notifications/format.py` (phone helpers: `PRE_WIDTH`, `cells`, `spark`, `tao`, `pct`, tier/status
dots, dates); `app/notifications/telegram.py` (home keyboard, `🏠 Home` everywhere, SN buttons on top/flags/movers,
period buttons, wallet keyboards, `w:<view>:<arg>:<ref>` callbacks ≤ 64 bytes); `app/notifications/commands.py`
(`render_home`; every read screen re-laid; `/menu`, `/home`, `/jobs`, `/wallet`; `HELP` grouped Look/Act; `USAGE` no
longer indexes `HELP` by line number); new `app/notifications/wallet.py`; `app/api/queries.py`
(`first_official_as_of`, `subnet_names`); `app/config.py` + `.env.example` (`TERMINAL_API_URL`).
Mutation handlers (team/product/flag/owner/review resolve/pause/resume) and confirmation texts are unchanged.

**Verified:** every screen rendered from the live DB (read-only shim) + live `/health` + live terminal: no `<pre>`
line > 32 chars, every callback ≤ 64 bytes, every screen reaches `act:home`. Tests: `tests/notifications`
(20, incl. home/navigation/phone-width over 25 screens, wallet list/overview/positions join/position card/performance/
insights paging/trades paging/terminal down/off) and `tests/test_config.py` (loopback validator; inventory 658).

**Operator:** `sudo systemctl restart taosentinel` to load the new bot (the running process never reloads code);
then 14 §10's new Telegram verify line on the phone. Nothing was written to the live DB; the service was not restarted.
