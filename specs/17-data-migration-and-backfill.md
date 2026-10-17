# 17 — Data migration from the old bot and history backfill

**Status:** proposed (reviewed 2026-09-23: aligned to 03's canonical DDL; reconciled with 02 on 2026-09-23 — B1, B2, B8, B9, B12, D1, E1, E2, F2, G1, G3, G4, J1, J2 are applied here and win over any sibling text; see §3.2, §11 and the Changelog) · **Phase:** 3 · **Depends on:** 02 (binding ledger), 01 (Settings, `CollectorJob` + the `manual` trigger kind, `ProviderLimiter`/`TokenBucket`, control API, `run-job`, `app/util/time.py`), 03 (DDL owner and canonical names: every table and column 17 writes, including 17's own `block_times`, `backfill_buckets`, `rpc_owner_history` and the `pool_history`/`price_bars`/`occupants`/`whale_cohort`/`github_*` provenance columns, is folded into 03's schema v1, together with the `price_bars` source ranks and the `pool_history` NULL-only-enrichment trigger. §3.2 here is a reference copy and 03 wins on any conflict), 04 (`occupants`, `identity_observations`, legacy importer `app/registry/legacy.py`), 05 (`TmcClient` on the shared `tmc` limiter, `chain_daily`/`chain_hourly`/`price_bars`/`chain_rest_snapshots` writers, the TMC budget table, `app/collectors/endpoints.py`), 06 (GitHub client/activity/metrics and the PAT budget table — §2.8 only, after Phase 4), 11 (`owner_rpc_seed` reads `rpc_owner_history`), 12 (ported `app/quant/regime.py` — §2.7), 13 (consumer of all history + `coverage.json`, 02 §J2), 16 (`app/control.py` pause checks) · **Master spec refs:** §1.3, §2 (import row), §4 (tier-0 TMC, tier-1 OnFinality), §5, §6, §10, §11, §12 (Phase 3), §13 item 5

---

## 1. Purpose and scope

Phase 3 turns the dead SubnetTrader bot's data and three free history sources (the archive RPC, TMC keyless REST, GitHub) into the point-in-time history that the validation harness (13) needs. It covers 2025-02-13 (dTAO launch, block 4,920,351) to the first hour the SSE recorder (05) wrote live. When this phase finishes, `price_bars` 1h/1d, pool reserves, moving price and occupancy are continuous from 2025-02-13 (the `pre_ledger` era is on by default, master §13 item 9 / checklist A5; from 2025-09-22 if the operator skips it). `chain_daily` covers TMC's 365-day window, `regime_daily` is filled, and `data/quant/coverage.json` states exactly what is and is not reconstructible.

**In scope**
1. `scripts/import_old_ledger.py`: the PC ledger, plus the Pi ledger when recovered and any extra copies, goes into `pool_history` with `INSERT OR IGNORE` by `(netuid, block_number)`. `moving_price`/`subnet_volume` are merged from `ledger_backfill.db` where NULL (NULL-only, marked with `enriched_at`). `occupant_key` is stamped, and the data is downsampled to `price_bars` 1h/1d.
2. `scripts/import_fundamentals.py`: `fundamentals.json` goes into identity observations through 04's legacy importer, plus full raw snapshots. `whale_cohort.json` (+ labels) goes into `whale_cohort` (dormant).
3. `scripts/backfill_rpc.py`: hourly archive-RPC cross-sections (`SubnetInfoRuntimeApi_get_all_dynamic_info` at a block whose time comes from `Timestamp.Now`) for the eras `gap` (8,443,757 → head), `ledger` (hours missing inside 6,502,594 → 8,443,757) and `pre_ledger` (4,920,351 → 6,502,593; default on, skippable). Writes `pool_history`, `block_times` and `rpc_owner_history`, then **reconstructs historical occupancy** into `occupants` and historical `netuid_reoccupied` events.
4. `scripts/backfill_tmc.py`: launcher for the in-service job `backfill_tmc`, which uses the shared TMC bucket. Tasks: `analytics?span=1Y` → `chain_daily`, `?span=1M` → `chain_hourly`, `candle-chart-compact?period=5m` for the gap → `price_bars` 5m/1h/1d, and the Pi open-positions check.
5. `scripts/backfill_regime.py` → `regime_daily` from `price_bars` 1h, using the ported classifier.
6. `scripts/backfill_github.py`: launcher for the in-service job `backfill_github` (Phase 4+). Pulls commits, releases and merged PRs since 2025-02-13 for every resolved repo into 06's tables with `backfilled = 1` (02 §J1), overnight with ≥ 1,500 req/h left for live jobs (02 §G4).
7. `scripts/build_coverage.py` → `data/quant/coverage.json` (the contract with 13, which reads it: 02 §J2; schema in §2.9).
8. `scripts/pi_recovery_report.py` (checklist A4 cross-reference) and the runbook orchestrator `scripts/backfill_all.py`.

**Out of scope**
- Computing historical `metric_values`/`scores`. The owning specs replay with an injected `as_of` (05 `chain_features run --as-of`, 06 `metrics --now`, 12 scoring backfill); §2.11 defines only the hand-off.
- TaoStats-keyed history (`/v1/subnets/pools/history?frequency=by_hour`, formerly `/api/dtao/pool/history/v1`; delegation events; owner swaps — paths per 02 §G7). These stay dormant until a key exists (master §13.2).
- `whale_events` (needs TaoStats). The old trading tables (`ema_positions`, `orders`, …) are **not migrated**, only reported (§2.10). `pool_snapshots` is not imported (it duplicates `pool_history`).
- Per-block or 1-minute RPC reconstruction. Research §5 prices it at 25–57 h; 5-minute price comes from TMC instead.
- Forward capture (05). Any chain write, signing, wallet or `bittensor`/`bittensor-wallet` import. Reading any wallet file other than `coldkeypub.txt`.

---

## 2. Design

### 2.0 Overview, run order, idempotency

```
 seed/ledger.db ───────────┐ ATTACH ?mode=ro&immutable=1
 seed/pi/ledger.db (opt) ──┼──► import_old_ledger ──► pool_history ──┐
 seed/ledger_backfill.db ──┘      (NULL-merge mp/sv)                 │
                                                                      ├──► bars.rebuild / enrich ──► price_bars 1h, 1d
 OnFinality ws (key opt) ─┐                                           │
 opentensor archive ws ───┴► backfill_rpc ──► pool_history, block_times, rpc_owner_history
                                   └──► occupancy.reconstruct ──► occupants (+ historical events), stamp occupant_key
 TMC REST (in-service job, shared bucket) ─► backfill_tmc ─► chain_daily, chain_hourly, price_bars 5m/1h/1d
 seed/fundamentals.json, whale_cohort.json ─► import_fundamentals ─► identity_* (04 code path), whale_cohort
 price_bars 1h ─► backfill_regime ─► regime_daily
 GitHub PAT (in-service job) ─► backfill_github ─► github_commits/activity_daily/authors_daily (backfilled=1), github_events (payload.backfilled)
 all of the above ─► build_coverage ─► data/quant/coverage.json ─► spec 13
```

**Where each step runs.** The rule is *every external call goes through the one process-wide bucket for its provider*. TMC and GitHub are also used by the running service, so their backfills run **inside the service process** as manual-trigger `CollectorJob`s (`backfill_tmc`, `backfill_github`). Each script is only a launcher: it writes a request file, calls `POST /api/v1/control/run/{id}` and tails the progress sidecar. When the service is down, the launcher runs the same job through 01's `run-job` code path in its own process; the service is then the only other TMC caller and it is not running. The archive RPC is not used by the service by default (05's `SUBTENSOR_RPC_ENABLED=false`), so `backfill_rpc` is a standalone process with its own instance of 01's `ProviderLimiter` (`app/ratelimit.py`; provider ids `onfinality`, `opentensor_archive`, 02 §G1). If `/health` shows 05's `chain_rpc_hourly`/`chain_rpc_metagraph` enabled, the script halves its OnFinality rate. Ledger/fundamentals/regime/coverage make no network calls.

| step | command | service | prerequisites | wall time | external calls | idempotency key |
|---|---|---|---|---|---|---|
| S0 | `pip install -e '.[backfill]'`; `python -m scripts.seed_registry` (04) | — | Phase 2 verify passed; checklist A3 files in `seed/`; A4 files in `seed/pi/` if recovered | 2 min | 04's | — |
| S1 | `python -m scripts.import_old_ledger --phase import` | **stopped** (`--online` allowed, slower) | S0 | 10–20 min | 0 | `pool_history (netuid, block_number)`; sidecar `ledger_import.json` per (source, netuid) |
| S2 | `python -m scripts.import_fundamentals` | any | S5 for full attribution (may run after S0; then 04's re-occupancy guard applies and a re-run after S5 adds the newly attributable observations) | < 1 min | 0 | 04 hash-dedupe; `whale_cohort (as_of_block, coldkey)` |
| S3 | `python -m scripts.backfill_tmc --tasks analytics1y,analytics1m` | running | S1 | ≈ 70 min (180/h ceiling, 02 §F2; longer if it meets a quiet window) | 256 TMC | sidecar `tmc_progress.json`; raw payload kept in `chain_rest_snapshots` |
| S4 | `python -m scripts.backfill_rpc --probe`, then `--era gap`, `--era ledger`, `--era pre_ledger` (default on; skippable) | any | S1; probe pass | gap + ledger + pre_ledger: ≈ 1.5 h keyed OnFinality; ≈ 4.7 h public; ≈ 7.5–9.5 h archive. Without pre_ledger: ≈ 0.8 / 2.9 / 4.6–5.8 h (§6) | 4–5 RPC per bucket (+2 per block-search correction) | `backfill_buckets (era, bucket_ts)` + `pool_history` PK |
| S5 | `python -m scripts.import_old_ledger --phase stamp,bars` | any | S4 | 5–15 min | 0 | `UPDATE … WHERE occupant_key IS NULL` (≤ 50,000 rows per statement, 03 §2.2); bars upsert by precedence |
| S6 | `python -m scripts.backfill_tmc --tasks restamp,candles5m[,positions]` | running | S4 | ≈ 17 min + restamp | 128–130 TMC | sidecar |
| S7 | `python -m scripts.backfill_regime` | any | S5, S6 | < 1 min | 0 | `regime_daily (day_ts, spec_version)` |
| S8 | `python -m scripts.build_coverage` | any | any step (re-run after each) | < 1 min | 0 | file rewritten atomically |
| S9 | `python -m scripts.backfill_github` | running | Phase 4 verify passed | ≈ 2 h without PR reviews; ≈ 6–13 h with them, worked only inside `BACKFILL_GITHUB_WINDOW_UTC` (00:30–08:00Z, 02 §G4), i.e. 1–2 nights (resumable across runs) | ≈ 5–6 k GitHub without reviews; + 1 per merged PR (≈ 20–40 k, estimate UNVERIFIED) | `github_commits.sha`, `github_events.dedupe_key`, rollups REPLACE |
| S10 | replays (§2.11), then S8 | any | Phase 8 | — | 0 | owner specs |

`python -m scripts.backfill_all [--skip-pre-ledger] [--skip-github]` runs S1, S3, S4, S5, S2, S6, S7, S8 in that order (S2 after S5 so the fundamentals snapshot is attributed with reconstructed occupancy): it stops and starts the service around S1 via `sudo systemctl stop|start taosentinel` (the unit is a system unit, 01 §8; prompts if unprivileged), stops at the first failed verify, and prints the next manual command. **Analytics 1Y runs early (S3):** TMC's 1Y window slides, so each day of delay loses its oldest day (2025-09-22 was the first row on 2026-09-22). `span=ALL` does not help: it is also capped at the last 365 days (research 02-gapfill-keyless §1). Its raw payloads are kept, so rows whose occupant is still unknown are re-stamped in S6 without re-fetching.

Every step is safe to re-run. Writes are either `INSERT OR IGNORE` on a natural key, a NULL-only `UPDATE`, or a precedence-guarded upsert (§2.5). Progress markers only avoid repeat work and never gate correctness. Every step writes one `collector_runs` row (`job` = step id, e.g. `import_old_ledger`, `backfill_rpc:gap`; a standalone step's `run_id` is `script:<step>:<started_at>`, 03 §2.1) with `rows` and `note`. Every step ends with one catalogued system event (03 §3.9, 02 §D1): `backfill_done` when no unit is left pending, or `backfill_failed` on an abnormal exit (§3.4). The file lock `data/backfill/backfill.lock` (`fcntl.flock`, non-blocking) prevents two *standalone* backfill processes from running at once. In-service jobs rely on `max_instances=1`.

**Pauses (02 §B12).** There is no backfill sentinel file. The in-service jobs call their own `paused_reason()` (01, implemented by 16's `app/control.py::effective_pause`) at every netuid/repo boundary. It covers a `control_state` row for the exact job id or for 16's `PAUSE_GROUPS` target `backfill` (= {`backfill_tmc`, `backfill_github`}), the read-only emergency files `data/control/<job>.paused` / `data/control/backfill.paused`, and `./PAUSE_ALL`. When it returns a reason, the job saves the sidecar and returns `JobOutcome(note="paused:<reason>")`. Standalone scripts are foreground operator commands. They stop cleanly at the next netuid/bucket boundary on SIGINT/SIGTERM, or when 16's `app.control.pause_all_active()` becomes true. `./KILL_SWITCH` does **not** pause backfills, because they are free (02 §B12).

### 2.1 Old-ledger import (`app/backfill/ledger_import.py`)

**Verified source facts (read-only probe 2026-09-23):**

| fact | value |
|---|---|
| rows / netuids / blocks | 7,362,854 / 129 (0..128) / 6,502,594 → 8,443,757 |
| time span | 1758585588 (2025-09-22T23:59:48Z) → 1781904274 (2026-06-19T21:24:34Z) |
| cadence | 2025-09-22 → 2026-04-09: 1 row/day × 98 netuids, all 98 at one block per day at 23:59:48Z (Taostats seed fetched 2026-05-09; real block numbers, `timestamp` = exact chain time); **no rows 04-10 → 04-21**; 1776867395 (04-22T14:16:35Z) → 1777464995 (04-29T12:16:35Z) hourly (21,505 rows, written later by the old `scripts/backfill_pool_history.py`); from **1777468595 (2026-04-29T13:16:35Z) ≈ 1-minute live capture** (`LEDGER_LIVE_FROM_TS`) with holes 05-02, 05-17, 05-21 → 05-28 |
| hour coverage | 6,479 UTC hours from the first to the last row; 1,205 populated; 200 of them have < 100 netuids (seed days); 503 have ≥ 100 netuids but no non-NULL `moving_price` even after the merge (dense era before 05-30); 502 are complete |
| NULL `moving_price`/`network_registered_at`/`subnet_volume` | 3,718,249 rows, all with `timestamp ≤ 1780182420` |
| first live-captured `network_registered_at` | 1780182493 (2026-05-30T23:08:13Z) = `LEDGER_NRA_TRUST_FROM_TS`; 3,644,605 rows at or after it, 131 distinct `netuid:reg` (slot reuse: 40 `3372582→8409860`, 92 `7013758→8352006`) |
| `ledger_backfill.db` | 3,837,187 rows (timestamps 1758585588 → 1780237367), all present in `ledger.db`; `moving_price`/`subnet_volume` non-NULL on 142,175 rows at 1,151 blocks (23,237 rows at 229 blocks before the trust boundary, almost all seed-era daily blocks); **fills 23,237 NULL rows** (both columns on the same rows); its 118,938 non-NULL `network_registered_at` values are all post-trust rows that `ledger.db` already has, so it adds no `network_registered_at`. Written by the old `backfill_metadata.py` with its default 24 h grid (`--grid 86400`, line 124), not hourly |
| golden block | 8,443,757: 129 rows (capture 1781904274; chain `Timestamp.Now` 1781904264). SN64: `price 0.064653121, tao_in_pool 187212.134573233, alpha_in_pool 2897413.298870939, moving_price 0.06478730659000576, network_registered_at 4531295, subnet_volume 4657484.168541687` |
| units | `price` τ/α, `tao_in_pool` τ, `alpha_in_pool` α, `subnet_volume` cumulative ÷1e9 (unit UNVERIFIED τ vs α — see §11), netuid 0 `price` 1.0–4.041665368 (root; semantics unknown); 0 rows with `price ≤ 0` or `tao_in_pool ≤ 0` |
| downsample sizes | 149,079 (netuid, hour) groups; 25,915 (netuid, day) groups |

**Algorithm (`--phase import`)**
1. **Preflight.** `common.open_db` runs 03's `migrate()` and asserts that 17's columns exist (`PRAGMA table_xinfo(pool_history)` contains `occupant_key, source, event_at, observed_at, price_method, enriched_at, run_id`; `price_bars` has `backfilled`; `backfill_buckets`, `block_times` and `rpc_owner_history` exist), else exit 3 with the missing names. `GET http://{HEALTH_HOST}:{HEALTH_PORT}/health` must fail (service stopped) unless `--online`. With `--online`, batches shrink to `LEDGER_IMPORT_BATCH_ROWS_ONLINE` (20,000) and the importer sleeps 50 ms between commits. Take `BackfillLock`. Check each source exists and `PRAGMA quick_check` = `ok`. If a `-wal` sibling is non-empty (typical of a Pi copy), do **not** attach with `immutable=1`, because that would silently ignore the WAL. Instead use `sqlite3.connect(src).backup(sqlite3.connect(tmp))` to copy it into `data/backfill/tmp/<name>.checkpointed.db` (needs ≈ source size of free disk) and attach the copy.
2. `ATTACH 'file:{src}?mode=ro&immutable=1' AS src`. Read `src.pool_history` schema. It must contain `netuid, block_number, timestamp, price, tao_in_pool, alpha_in_pool`. The three later columns are optional (`ledger_bak_pre_sdk_20260513.db` lacks them), and missing ones are selected as `NULL`.
3. **Copy per netuid** (one transaction each; ≤ 120 k rows ≈ 0.5 s). The sidecar `data/backfill/ledger_import.json` records `{source: {netuid: {rows_in, rows_new, done}}}`:
```sql
INSERT OR IGNORE INTO main.pool_history
  (netuid, block_number, timestamp, price, tao_in_pool, alpha_in_pool, moving_price, network_registered_at,
   subnet_volume, occupant_key, source, event_at, observed_at, price_method, run_id)   -- enriched_at stays NULL
SELECT netuid, block_number, timestamp, price, tao_in_pool, alpha_in_pool, moving_price,
       CASE WHEN timestamp >= :trust THEN network_registered_at END,          -- older values may be back-stamped
       subnet_volume,
       CASE WHEN timestamp >= :trust AND network_registered_at IS NOT NULL
            THEN netuid || ':' || network_registered_at END,
       :source, NULL,
       CASE WHEN timestamp >= :live_from THEN timestamp ELSE :run_started END,  -- live capture vs later backfill
       NULL, :run_id
FROM src.pool_history
WHERE netuid = :netuid AND price > 0 AND tao_in_pool > 0
  AND (:thin = 0 OR rowid IN (SELECT rowid FROM (SELECT rowid, ROW_NUMBER() OVER
        (PARTITION BY timestamp / :thin ORDER BY timestamp DESC) rn FROM src.pool_history WHERE netuid = :netuid) WHERE rn = 1));
```
   `:source` ∈ `ledger_pc` (seed/ledger.db), `ledger_pi` (seed/pi/ledger.db), `ledger_extra` (`--extra PATH`, repeatable, e.g. `p0_ledger.db`). The order is always PC → Pi → extras; the first writer wins on an identical `(netuid, block_number)`. `network_registered_at` from before the trust boundary is dropped (NULL). The old bot's `backfill_metadata.py:4-6` says it "was back-fillable from a single read", which means some copies (likely the Pi) back-stamped the *current* occupant onto history. That is wrong for reused slots, so those values are counted in the report as `nra_dropped_untrusted` and never stored. **`observed_at` is honest (master §6 "when we fetched"):** rows the old bot captured live (`timestamp ≥ LEDGER_LIVE_FROM_TS`, 1777468595) keep `observed_at = timestamp`; seed-era rows (Taostats, fetched 2026-05-09) and the 04-22 → 04-29 hourly rows (backfilled later by the old bot) get `observed_at = :run_started` (the unix start of this import run), because their true fetch time is unknown and later than `timestamp`. `event_at` stays NULL for ledger rows (capture wall clock ≈ block time + 10 s, not the block time). `LEDGER_IMPORT_THIN_SEC=0` (default) keeps every row; `300` keeps the last row per 5 minutes (≈ 2.4 M instead of 7.36 M rows, §6). A Pi or extra ledger uses the same `LEDGER_LIVE_FROM_TS` (the Pi ran the same capture loop).
4. **NULL-merge** from each merge source: `ledger_backfill.db`, the Pi ledger, and extras. Never overwrite a non-NULL value:
```sql
UPDATE main.pool_history AS p SET
  moving_price  = COALESCE(p.moving_price,  b.moving_price),
  subnet_volume = COALESCE(p.subnet_volume, b.subnet_volume),
  network_registered_at = COALESCE(p.network_registered_at, CASE WHEN b.timestamp >= :trust THEN b.network_registered_at END),
  occupant_key  = COALESCE(p.occupant_key, CASE WHEN b.timestamp >= :trust AND b.network_registered_at IS NOT NULL
                                              THEN b.netuid || ':' || b.network_registered_at END),
  enriched_at   = COALESCE(p.enriched_at, :now)                -- 03: value knowable only from here (PIT)
FROM src.pool_history AS b
WHERE b.netuid = p.netuid AND b.block_number = p.block_number AND b.netuid = :netuid
  AND (   (p.moving_price  IS NULL AND b.moving_price  IS NOT NULL)
       OR (p.subnet_volume IS NULL AND b.subnet_volume IS NOT NULL)
       OR (p.network_registered_at IS NULL AND b.network_registered_at IS NOT NULL AND b.timestamp >= :trust));
```
   The WHERE clause matches only rows that the merge source can actually fill. (A looser `p.network_registered_at IS NULL` test would match all ≈ 3.7 M pre-trust rows and make `changes()` meaningless.) Expected against `ledger_backfill.db`: `changes()` = 23,237 (verified by read-only join 2026-09-23; its rows are a subset of `ledger.db`, so the INSERT step adds 0). `enriched_at` records that the filled columns became known later than the row's `observed_at`, so 13 treats a merged `moving_price` as reconstructed chain state, not as a live observation. These NULL-only UPDATEs (merge here, occupant stamping in §2.2) are the only writes 17 makes to existing `pool_history` rows. 03's trigger `trg_pool_history_update` aborts anything else. The merge source tag (`ledger_backfill`, `ledger_pi`, `ledger_extra`) goes into the import report, not the row.
5. **Bars** for the imported range: `bars.rebuild(periods=('1h','1d'), from_ts, to_ts)` (§2.5). Rows still lacking `occupant_key` get bars with `occupant_key = NULL` until `--phase stamp`.
6. Write `data/backfill/ledger_import_report.json`: per source `{rows_in, rows_new, rows_skipped_nonpositive, nra_dropped_untrusted, merged_mp, merged_sv, min_ts, max_ts, min_block, max_block, netuids}`, and run the §10 verification queries.

`--phase stamp` = `occupancy.stamp_all()` (§2.2). `--phase bars` = `bars.rebuild` over all of `pool_history`. `--phase verify` = the §10 queries only. `--phase all` = import, stamp (it only stamps what the occupancy index knows), bars, verify.

### 2.2 Historical occupancy reconstruction (`app/backfill/occupancy.py`)

Seed-era rows carry no trustworthy `network_registered_at`. Occupancy is therefore rebuilt from observations `(netuid, network_registered_at, block, ts)` taken from three sources:
- (i) every `backfill_rpc` bucket, which is authoritative (read at that block);
- (ii) trusted ledger rows;
- (iii) 04's current `occupants`/`subnets` (TMC `/subnets/`).

The key property: **`network_registered_at` is itself the successor's registration block**. The chain registers the new occupant at exactly block `R_next`, pruning the predecessor in that block. Hourly or even daily sampling therefore gives exact block boundaries:

```
for netuid in observed netuids:
    obs = sorted((block, reg) for all observations)
    regs = distinct reg values in order of first appearance            # must be strictly increasing
    anomaly if a reg reappears after a different reg, or reg > block of its first observation
    occupant(reg_i) owns blocks [reg_i, reg_{i+1} − 1]  (last one: [reg_last, ∞))
    first_seen_block = min obs block for reg_i;  last_seen_block = max obs block for reg_i
```

**Writes** (one transaction per netuid):
- `block_times` for each `reg_i`. One `chain_getBlockHash(reg_i)` + `Timestamp.Now` read (≈ 150–260 occupants ≈ 500 RPC calls in total) gives the exact registration time `registered_ts(reg_i)`.
- `occupants` (03 §3.2 DDL, which already carries `registered_ts`/`deregistered_ts`; all times INTEGER unix seconds). `INSERT OR IGNORE` for historical keys, with every NOT NULL column set:
  - `occupant_key = f"{netuid}:{reg}"`, `netuid`, `network_registered_at = reg`;
  - `status = 'deregistered'`; `dereg_reason = 'reoccupied'` if a successor exists, else `'absent'`;
  - `first_seen_block`/`last_seen_block` = min/max observed block; `first_seen_at`/`last_seen_at` = the `timestamp` of those observations;
  - `registered_ts` = the exact `registered_ts(reg_i)`; `registered_at_est` = the same value;
  - `deregistered_at` = `registered_ts(reg_{i+1})` when reoccupied, else `last_seen_at` (absent). `deregistered_ts` = `registered_ts(reg_{i+1})` when reoccupied, else NULL (no exact block is known);
  - `predecessor_key`/`successor_key`; `name`/`symbol` from the occupant's last `rpc_owner_history` row;
  - `first_emission_block`/`activated_at` NULL; `missing_runs = 0`;
  - `source = 'onfinality'` when the interval was established by OnFinality buckets, `'rpc'` for the opentensor archive, `'legacy_ledger'` when only trusted ledger rows attest it (03 CHECK vocabulary).

  For rows that already exist (the current universe, written by 04): update only `first_seen_block/first_seen_at = MIN(existing, observed)`, `registered_ts` and `predecessor_key` (if NULL). 04's `registered_at_est` is left as written (03's `v_quant_occupants` prefers `registered_ts`). The status of a current occupant is **never** changed; 04 owns it. This answers 04's open question 5 (exact registration timestamps).
- `events` (03 §3.9 row `netuid_reoccupied` with its backfill key set; 02 §B9, §D1, §J1): one row per historical transition. It is written **after** the netuid's transaction commits, through 03's `emit_event` (the only `events` writer, 03 §2.3 item 5), which validates the type and the backfill key set:
  - `occupant_key = new key`, `netuid`, `severity = 'warning'` (02 §B2), `source = 'registry'` (the producer's source: 04 emits this type live; there is no `backfill_*` source);
  - `event_at = registered_ts(new)`, `observed_at = now`;
  - `payload = {old_key, new_key, old_name, new_name, reg_block, backfilled: true}` (03's backfill key set; `payload.backfilled` routes it to the digest, 02 §D4);
  - `dedupe_key = 'reoccupied:{old}->{new}'` (03 catalogue), `run_id`;
  - **`alerted_at = observed_at`**, passed to `emit_event`: the row is historical and never paged (the dispatcher sends only rows with `alerted_at IS NULL`, 03 index `ix_events_unalerted`; 03's trigger guards only UPDATEs).

  Transitions are re-derived from the 17-written `occupants` rows (`predecessor_key`/`successor_key`) on every run, and `emit_event` inserts with `ON CONFLICT DO NOTHING`. A crash between the commit and the event write therefore heals on the next run.
- **Stamping** (`stamp_all()`):
  - `pool_history`: `UPDATE pool_history SET occupant_key = :k WHERE netuid = :n AND block_number BETWEEN :lo AND :hi AND occupant_key IS NULL`, with `[lo, hi]` walked in slices of ≤ 50,000 rows (03 §2.2 lock-hold rule for concurrent scripts). 03's `trg_pool_history_update` permits exactly this kind of NULL-only write. `enriched_at` is **not** set by stamping. It marks only late-filled *observed* values (`moving_price`, `subnet_volume`, `network_registered_at`), so that live-captured prices keep `backfilled = 0` in their bars (§2.5). The occupant attribution is derived metadata, and its provenance is the `occupants` row (`source`, `registered_ts`). See §11 item 20(d). Rows already keyed (trusted) are cross-checked, and mismatches are counted (expected 0). Rows below the earliest known `reg` stay NULL (`unstamped`; expected 0 once the `ledger` era ran).
  - `price_bars`: `occupant_key` by the bar's chain `block_number` when non-NULL (exact interval lookup), else by `COALESCE(ts_last, ts) ∈ [registered_ts(reg_i), registered_ts(reg_{i+1}))`; only rows with `occupant_key IS NULL` are touched.
  - `chain_daily`/`chain_hourly` rows get their key at write time (§2.4).

`OccupancyIndex.load(db)` exposes `occupant_at_block(netuid, block) -> str | None`, `occupant_at_ts(netuid, ts) -> str | None` (None when `registered_ts` is unknown or the ts precedes every known registration) and `intervals(netuid) -> list[Interval]`. Consumers: 13's `OccupantIndex` may read the same `occupants` columns.

### 2.3 Archive-RPC backfill (`app/backfill/rpc_backfill.py`, `rpc_client.py`)

**Endpoints and providers** (all read-only JSON-RPC over WebSocket through `async-substrate-interface` 2.x, "ASI", used as transport and SCALE decoder; never `lite`/`entrypoint`, whose state is discarded after ~7–13 h):

| order | URL | limiter provider | rate | notes |
|---|---|---|---|---|
| 1 | keyed OnFinality: `wss://bittensor-finney.api.onfinality.io/ws?apikey={ONFINALITY_API_KEY}` (form UNVERIFIED; `--probe` also tries `/public-ws?apikey=`) | `onfinality` | `ONFINALITY_RATE_PER_SEC` 20 | when the key is set (checklist B2) |
| 2 | `ONFINALITY_WS_URL` = `wss://bittensor-finney.api.onfinality.io/public-ws` | `onfinality` | ≤ `BACKFILL_RPC_PUBLIC_MAX_RPS` 4 (01's validator clamps to 4 only when the key is empty; when a key is set and the gateway fails over to this public URL, `RpcGateway` adds a per-endpoint 01 `TokenBucket` at 4/s on top of the shared `onfinality` limiter; not a new limiter module, 02 §G1) | public 5 RPS; error `-32029` above |
| 3 | `ARCHIVE_WS_URL` = `wss://archive.chain.opentensor.ai:443` | `opentensor_archive` | `ARCHIVE_RATE_PER_SEC` 2.0 **and** ≥ `ARCHIVE_MIN_BUCKET_INTERVAL_SEC` 1.5 s between buckets | `-32004 "Historical work rate limit exceeded"` (≈ 30-ref burst, ≈ 3 refs/s refill, UNVERIFIED fit); `state_queryStorageAt` costs one ref **per key**; `chain_getBlockHash` is free |

The selector is 05's `EndpointSelector`, imported with `is_dead_socket_error`/`is_quota_error` from `app/collectors/endpoints.py` (05 §8, 02 §G1; the old `app/providers/` package is retired). Errors `-32029`, `-32004`, HTTP 429 or messages containing `rate limit`/`budget_exhausted` mean **back off on the same endpoint**: sleep `min(5·2^attempt, ARCHIVE_BACKOFF_MAX_SEC)` for up to `BACKFILL_RPC_MAX_ATTEMPTS` (6) tries. A dead socket (`is_dead_socket_error`) means reconnect. Three consecutive failed buckets mean `mark_bad` (cooldown `SUBTENSOR_ENDPOINT_COOLDOWN_SEC`) and move to the next URL. Every JSON-RPC request first does `await limiter.acquire(provider)`, and so does **every request ASI makes internally**. Verified in the ASI 2.2.1 source (`async_substrate.py` `runtime_call` 3513-3594, `init_runtime` 1589-1646, `_get_block_runtime_version_for` 2579-2584): `runtime_call(..., block_hash=H)` silently issues `chain_getHeader(H)` + `state_getRuntimeVersion(parent)` for every new block hash, and on a new runtime version it also issues the block-number, runtime-info and `Metadata_metadata_at_version` calls. These would bypass the bucket, and on the opentensor archive they count against the `-32004` budget. `RpcGateway` therefore never calls ASI's `runtime_call` on the hot path; it sends the raw requests itself through `substrate.rpc_request(method, params)` (one token each) and uses ASI only to decode (§2.3 step 3). Before any call that can trigger a runtime load (`get_runtime_for_version`), it acquires `RUNTIME_LOAD_TOKENS = 6` tokens up front.

**Eras** (`--era`, repeatable). A bucket is one UTC hour `H` (`BACKFILL_RPC_STEP_SEC` 3600; `--step 86400` gives a cheap occupancy-only daily pass):

| era | blocks | buckets planned | ≈ count |
|---|---|---|---|
| `gap` | (max `block_number` of `ledger_*` rows) + 1 → head − 300 | every hour from `floor(ts_max/3600)+1` to `floor(now/3600) − 2` | 2,300 (+24/day of delay); moves automatically if the Pi ledger extends the ledger |
| `ledger` | 6,502,594 → 8,443,757 | hours with `COUNT(DISTINCT netuid)` in `pool_history` `< BACKFILL_RPC_MIN_NETUIDS_PER_HOUR` (100) **or** with no row carrying a non-NULL `moving_price` | ≈ 5,977 (verified 2026-09-23: 6,479 hours in the span, 1,205 populated, 200 of them < 100 netuids, 503 dense-era hours without any `moving_price`; only 502 hours are complete) |
| `pre_ledger` (default on; drop it from `BACKFILL_RPC_ERAS` or use `backfill_all --skip-pre-ledger`) | 4,920,351 → 6,502,593 | every hour | 5,274 |

The `ledger` era also removes survivorship bias from the Taostats seed. That seed shows 98 netuids per day, so occupants that died before 2026-05-09 are absent from it, while the chain cross-section has everyone. The moving-price clause matters for `dereg_safety`/`gate_margin` history: the dense era 2026-04-29 → 05-30 has 129 netuids per hour but `moving_price` NULL, and without it those hours would be skipped.

**Per-bucket procedure** (`process_bucket(era, H)`; up to `BACKFILL_RPC_CONCURRENCY` buckets in flight: 4 keyed / 1 public / 1 archive):
1. `target = H + 3600 − BACKFILL_RPC_SAMPLE_OFFSET_SEC` (60), so the sample is the hour's last "cycle", matching the ledger's `MAX(timestamp)` close convention. `b = BlockSampler.predict(target)` interpolates piecewise-linearly over anchors: exact `block_times` rows plus approximate ledger pairs `(block_number, timestamp − 10)` for live-captured rows (capture ≈ block + 10 s). Seed-era rows (one block per day at 23:59:48Z) already carry the exact chain time and are used without the offset. Outside the anchors it extrapolates at 12.0 s/block from the nearest anchor. Research §2 shows the old linear anchor alone drifts −6.9 h to +32 h, so it must not be used for gridding.
2. `hash = chain_getBlockHash(b)`, then `ts = Timestamp.Now(hash) // 1000`. The storage key is `twox128("Timestamp")‖twox128("Now")` = `0xf0c365c3cf59d671eb72da0e7a4113c49f1f0515f462cdcf84e0f1d6045dfcbb`, decoded as u64 LE ms. Insert `block_times`. If `ts ∉ [H, H+3600)`, set `b += round((target − ts)/12.0)` and repeat up to 3 times. Still outside → bucket `failed`, reason `block_search`. **Block↔time comes only from `Timestamp.Now`**; interpolated blocks are never written as observations.
3. `spec = state_getRuntimeVersion(hash)["specVersion"]` (1 call). `raw = state_call("SubnetInfoRuntimeApi_get_all_dynamic_info", "0x", hash)` (1 call; about 47–49 KB SCALE). Decoding uses ASI with no network: `rt = runtime_cache[spec]`. On a miss, acquire `RUNTIME_LOAD_TOKENS`, then `rt = await substrate.get_runtime_for_version(spec, hash)` (once per runtime version). Then `out_ty = rt.runtime_api_map["SubnetInfoRuntimeApi"]["get_all_dynamic_info"]["output"]` and `entries = (await substrate.decode_scale(f"scale_info::{out_ty}", bytes.fromhex(raw[2:]), runtime=rt)).value`, with the CPU part in `asyncio.to_thread`. If `rt.metadata_v15 is None` (a runtime without metadata v15), fall back to ASI's `runtime_call(..., block_hash=hash)` after acquiring 3 extra tokens for its internal calls. A missing API has two signs: an RPC error "Exported method … is not found", or a `KeyError` in `runtime_api_map` (pre-dTAO runtimes, e.g. block 4,913,000). Either one → bucket `skipped`, reason `no_runtime_api`. The runtime API returns `Vec<Option<DynamicInfo>>` (UNVERIFIED for every runtime), so `None` entries are skipped and counted.
4. Price per §2.3.1.
5. Convert each entry (table below). Skip entries with `price ≤ 0` or `tao_in ≤ 0` (old rule, `backfill_rpc_candles.py:192-200`) and count `skipped_nonpositive`.
6. `rpc_owner_history`: insert a row when `(owner_coldkey, owner_hotkey, subnet_name, token_symbol)` differs from the previous row for the same `occupant_key` (first sighting included). Buckets are processed out of order under concurrency and across eras, so "previous" means the nearest earlier stored row for that occupant (`owner_state` is seeded from the table, not only from memory). A row that makes the *next* stored row redundant is still inserted; the log is change-oriented, not minimal.
7. **One transaction:** `INSERT OR IGNORE` the `pool_history` rows, then `rpc_owner_history` (`source` = the endpoint provider, `run_id`), `block_times` (`endpoint`, `run_id`), and `backfill_buckets(era, H, status='done', block_number, block_ts, n_subnets, rows_written, price_method, endpoint, attempts, observed_at, run_id)` (03 columns). A failure writes only the `backfill_buckets` row (`status='failed'`, `error`); re-runs retry `failed` and never re-fetch `done`.
8. Between buckets: check `pause_all_active()` and SIGINT/SIGTERM (graceful: finish in-flight buckets, save state; exit 130 on a signal, 0 on `PAUSE_ALL`). After the era: `occupancy.reconstruct()` (§2.2), `occupancy.stamp_all()`, `bars.rebuild` over the era's time range (§2.5) and a `collector_runs` row. If the era inserted any `rpc_owner_history` row, delete `job_state(job='owner_rpc_seed', key='rpc_seed_done')`. This re-arms 11's one-shot, idempotent `owner_rpc_seed` import; `job_state` holds runtime watermarks only, 02 §B12.

**Conversion** (DynamicInfo field → `pool_history`/`rpc_owner_history`; the golden test in `--probe` must pass before any era runs):

| runtime field | SCALE type | stored as |
|---|---|---|
| `netuid` | u16 | `netuid` |
| `tao_in` | Compact<u64> rao | `tao_in_pool = raw / 1e9` |
| `alpha_in` | Compact<u64> rao | `alpha_in_pool = raw / 1e9` |
| `moving_price` | I96F32 `{bits}` | `moving_price = bits / 2**32` (frac bits UNVERIFIED — golden test) |
| `subnet_volume` | Compact<u128> | `subnet_volume = raw / 1e9` (same convention as the old `normalise_dynamic_info`, `chain_client.py:711-765`) |
| `network_registered_at` | Compact<u64> | `network_registered_at`; `occupant_key = f"{netuid}:{network_registered_at}"` |
| `owner_coldkey`, `owner_hotkey` | AccountId32 | SS58 prefix 42 (`scalecodec.ss58_encode`) → `rpc_owner_history` |
| `subnet_name`, `token_symbol` | Vec<u8> / Vec<Compact<u8>> | UTF-8 (`errors='replace'`) → `rpc_owner_history` |
| — | — | `timestamp = event_at = ts`; `observed_at = now_ts()` (01 `app/util/time.py`); `source ∈ {onfinality, opentensor_archive}`; `price_method`; `run_id`; `occupant_key` set at write time (the RPC read is authoritative) |

ASI decodes `AccountId32` either as a 32-byte value (tuple, bytes or hex) or as an SS58 string, depending on version and type registry. `convert_entry` accepts all three forms and normalises to SS58 prefix 42 with `scalecodec.utils.ss58.ss58_encode`, which is provided by ASI 2.x's `cyscale` dependency. `Vec<u8>`/`Vec<Compact<u8>>` names may arrive as a list of ints or as a string. Both are handled.

**2.3.1 Price semantics.** The ledger's `price` came from the SDK's `DynamicInfo.price`. At the golden block that is 0.064653121 for SN64, versus the reserve ratio 0.0646140, so it is **not** `tao_in/alpha_in`. SDK ≥ 9 sets it from the Swap pallet: `AlphaSqrtPrice²` (UNVERIFIED; settled by the golden test). `BACKFILL_RPC_PRICE_METHOD` (`auto|swap_sqrt|reserve_ratio`):
- **`swap_sqrt`**: run `state_getKeysPaged(prefix = twox128("Swap")‖twox128("AlphaSqrtPrice"), 256, None, hash)`, cached per runtime `specVersion` (already read in step 3), then `state_queryStorageAt(keys, hash)`. Caveat: a netuid registered after the cached key list was built has no key in it. So the key list is rebuilt whenever `get_all_dynamic_info` returns a netuid without a cached key, and that costs one extra `state_getKeysPaged`. Decode the value as U64F64 LE, `v = int/2**64`, `price = v²`. The netuid is the last 2 key bytes (u16 LE), which is valid for the Identity and `*Concat` hashers alike.
- **`reserve_ratio`**: `price = tao_in / alpha_in`.
- `auto` = `swap_sqrt` on OnFinality when the prefix has keys at that block. It falls back to `reserve_ratio` when the Swap map is empty or absent (older runtimes, where constant-product was exact), on the opentensor archive (per-key cost), or on any error.
- netuid 0 keeps whatever the chosen method yields and is excluded from price assertions (ledger root prices 1.0–4.04 are unexplained, §11). `pool_history.price_method` records the method per row.

**Identity-hasher storage keys** (used by `--probe` and by the fallback `BACKFILL_RPC_MODE=storage`): `key(item, netuid) = twox128("SubtensorModule") ‖ twox128(item) ‖ u16_le(netuid)`. This is verified for `SubnetTAO`: netuid 64 = `0x658faa385070e074c85bf6b568cf0555` `7a57dce016211512d1700561066b85a3` `4000`. Research asserts the same hasher for `SubnetAlphaIn`, `SubnetMovingPrice` and `SubnetVolume` (UNVERIFIED individually); `NetworkRegisteredAt` is UNVERIFIED. `storage` mode is for runtimes where ASI cannot decode `DynamicInfo`, and **OnFinality only**. Per bucket it lists netuids with `state_getKeysPaged` on the `SubnetTAO` prefix, then makes one `state_queryStorageAt` over the five items × netuids, decoding u64/I96F32/u128/u64. Its price is `reserve_ratio` (or `swap_sqrt` as above).

**`--probe`** writes `data/backfill/rpc_probe.json`. `backfill_rpc` refuses to run an era unless the newest probe for the selected endpoint and decoder passed (`--force-unprobed` overrides). Checks:

| block | expectation |
|---|---|
| 8,443,757 | `Timestamp.Now` = 1781904264000; 129 entries. Versus the 129 ledger rows at that block: `tao_in_pool`, `alpha_in_pool`, `subnet_volume`, `moving_price` rel. err ≤ 1e-9; `network_registered_at` equal; `price` (netuid ≠ 0) rel. err ≤ 1e-6 under `swap_sqrt`, and the result is recorded under `reserve_ratio`. Raw `SubnetTAO[64]` via the Identity key = 187,212.13 τ ± 0.01 |
| 6,502,594 | `Timestamp.Now` = 1758585588000 (equals the ledger's first row) |
| 4,920,351 | `Timestamp.Now` = 2025-02-13T21:41:24Z; 65 subnets; `SubnetTAO[64]` ≈ 0.709 τ |
| 4,913,000 | runtime API not found → classified `no_runtime_api` (expected) |
| head | decodes; entries ≥ 128 |

The probe also records `asi_version` (must be ≥ 2.2, see §8.1), `bittensor_wallet_installed` (must be `false`; `importlib.util.find_spec("bittensor_wallet") is None`), the working OnFinality URL form, the measured latency per call type, the number of JSON-RPC requests per bucket counted on the socket (expected 4, or 5 with `swap_sqrt`, measured on a second bucket of the same runtime version so that the one-off runtime load and `state_getKeysPaged` are excluded; a higher count means an un-metered ASI-internal call leaked), and whether `-32004`/`-32029` was hit.

### 2.4 TMC backfill job (`app/backfill/tmc_backfill.py`)

`class BackfillTmcJob(CollectorJob)` has `id='backfill_tmc'`, `phase=3`, `enabled_flag='BACKFILL_TMC_ENABLED'`, `provider='tmc'`, `paid=False`, `timeout_sec=BACKFILL_TMC_TIMEOUT_SEC` (14,400; resumable) and `min_gap_factor=0`. Its trigger is 01's **`manual`** kind (02 §E1: `IntervalTrigger(weeks=520)` with `next_run_time=None`, exempt from the first-run stagger and from liveness). It runs only on `POST /api/v1/control/run/backfill_tmc` or `python -m app run-job backfill_tmc`. §8.1's job table is the one that 01's `JOB_TABLE` mirrors (02 §E2).
- **Input:** `data/backfill/tmc_request.json` = `{tasks: [...], netuids: [..] | null, coldkey: str | null, redo: bool, requested_at: int}` (unix s, 02 §B1), written atomically by the launcher. If it is absent, the default is `["analytics1y","analytics1m","restamp","candles5m"]`.
- **Progress:** `data/backfill/tmc_progress.json` = `{version: 1, gap_start_ts, gap_end_ts, candle_label, tasks: {<task>: {"<netuid>": {status: done|partial|failed, rows, unattributed, attempts, last_error, range: [from, to]}}}}`. It is rewritten atomically after every netuid. `done` entries are skipped unless `redo`.
- **Pacing:** every request goes through 05's `TmcClient` methods (`fetch_analytics`, `fetch_candles`, `get`), i.e. 01's one process-wide `tmc` limiter (8/min + burst 2, `TMC_HOURLY_CAP` 500, `TMC_MONTHLY_CAP` 90,000, `Retry-After`/`X-RateLimit-*` honoured). There are no priority classes and no `priority=` argument (02 §G1). On top of that the job keeps a job-local sliding ceiling `BACKFILL_TMC_MAX_PER_HOUR` (180) and sleeps inside `BACKFILL_TMC_QUIET_UTC = "02:05-02:45,20:25-23:35"` (02 §F2), checked at every netuid boundary:
  - the first window covers 05's 02:10Z `tmc_daily` batch and 04's 02:40Z identity run;
  - the second covers 11's `owner_collect_daily` (20:30–23:30Z, ≤ 300/h).

  The other callers peak at ≈ 311/h in 11's window and at ≤ 144/h in any other hour (05 §6.3). With 180 on top, every hour stays under 500 (e.g. 01:00Z ≈ 265, 03:00Z ≈ 212). 05 §6.3 carries this job's budget row: ≤ 180/h and 386 + 10 % requests, once (02 §G3).
- **Netuids:** 1..max(netuid in `subnets`) (netuid 0 has no alpha market). Each netuid is its own try/except, so one failure never aborts the run.

| task | request | writes | rules |
|---|---|---|---|
| `analytics1y` | `TmcClient.fetch_analytics(n, '1Y')` = `GET /analytics/subnet/{n}/?span=1Y` (all fields) | raw payload → `chain_rest_snapshots` through 05's `write_rest_snapshot` (05 §3.1's "17's own" row: `endpoint='/analytics/subnet/<n>/'` with the netuid substituted literally, `params='span=1Y'`, `source='tmc'`). With a literal path, 03's retention (90 d, keeping the newest row per `(endpoint, params)`) keeps each netuid's newest payload. Rows → 05's `upsert_daily_from_analytics(db, rows, netuid=n, source='analytics_1y', run_id=…, observed_at=…, backfilled=1)`, each row pre-annotated with `row["occupant_key"]` (05 honours it, 02 §J1) | 03 `chain_daily` precedence: `analytics_1y` replaces 05's `analytics_1w` daily rows, never the reverse; a same-rank row is never overwritten (02 §J1). `backfilled=1` is passed explicitly (05's default is 0). `observed_at` = fetch time, `event_at` = the analytics point `ts`. `occupant_key = OccupancyIndex.occupant_at_block(n, row.block_number)` (TMC analytics block ≈ chain ± 1 min). Rows whose occupant is unknown are **not passed** to 05, whose fallback would attribute them through 04's `occupant_at`. They are counted `unattributed` and mark the netuid `partial` |
| `analytics1m` | `TmcClient.fetch_analytics(n, '1M')` | raw → `chain_rest_snapshots` (as above, `params='span=1M'`); rows → 05's `upsert_hourly_from_analytics(db, rows, netuid=n, occupant_key=…, source='analytics_1m', run_id=…, observed_at=…, backfilled=1)`, called **once per occupant segment** of the 720 hourly rows | write gate: only missing `(netuid, hour_ts)`; `source='analytics_1m'`, `backfilled=1`, analytics columns only (no price/moving price) |
| `restamp` | none (0 requests) | re-parses the newest stored `span=1Y`/`1M` payload of each `partial` netuid after the RPC eras | same rules; turns `partial` into `done`. 03's retention keeps the newest row per `(endpoint, params)`, so that payload survives the 90-day prune; `--redo` re-fetches only when fresher data is wanted |
| `candles5m` | `TmcClient.fetch_candles(n, '5m', gap_start, gap_end, timeout=…)` = `GET /subnets/{n}/candle-chart-compact/?period=5m&from_timestamp={gap_start ISO Z}&to_timestamp={gap_end ISO Z}` | `price_bars` 5m (+ aggregated 1h, 1d) | see below |
| `positions` | `TmcClient.get(…)` on `/accounts/coldkey-stakes/{coldkey}/` and `/accounts/coldkey-subnet-chart/{coldkey}/` (params UNVERIFIED; called without params. The OpenAPI names the second path parameter `coldkey_id` (research 02-gapfill-keyless §1), so it may be an internal id rather than an SS58: a 404 there is logged and ignored) | raw → `chain_rest_snapshots`; report → §2.10 | only when `coldkey` is set |

**`candles5m` details.**
- `gap_start = floor5m(MAX(timestamp) of ledger_* rows) − 3600`; the 1 h overlap is for label detection. `gap_end = floor5m(now) − 600`.
- One call per netuid (research: 27,175 rows, 2.8 MB, 11 s) with timeout `BACKFILL_TMC_CANDLE_TIMEOUT_SEC` (90). On timeout or 5xx after retries, it retries in `BACKFILL_TMC_CANDLE_CHUNK_DAYS` (30) windows (≈ 4 calls, still counted against the bucket).
- Compact rows are `[ts, block, o, h, l, c, v, start_volume, tao_usd, tao_eur]` (10 elements per research 02-gapfill-data-continuity §3). Research 02-gapfill-keyless §1 lists 9 (`…, start_volume, tao_price`). The parser accepts 9 or 10 elements (`tao_eur` optional) and rejects anything else as `parse_error` for that netuid. `ts` is accepted as ISO or epoch. `start_volume` is TMC's cumulative `SubnetVolume` (τ per research; UNVERIFIED against the chain counter, §10 item 13).
- **Label convention**, detected once on the overlap hour for SN64 and SN1: compare each 5m `close` with the ledger price at `MAX(timestamp)` in `[ts, ts+300)` (start-label) and in `[ts−300, ts)` (end-label). Choose the hypothesis with the lower median |rel. err|, requiring < 0.5 %; otherwise default `start` and log a warning. Stored as `candle_label`; `price_bars.ts` is always the bucket **start**.
- **5m rows:** `period='5m'`, OHLC, `volume=v`, `start_volume`, `tao_price_usd`, `tmc_block_number=block` (TMC's label, 8–90 min off chain time: **never joined on**), `block_number=NULL`, `ts_last=NULL` (03: NULL for `tmc_candle`; 13 uses `bucket_start + period`), `event_at=ts`, `observed_at` = fetch time, `source='tmc_candle'`, `backfilled=1`, `run_id`, `occupant_key=OccupancyIndex.occupant_at_ts`.
- **1h and 1d** are aggregated locally from the fetched 5m rows (saves 128 requests): open = first, close = last, high = max, low = min, volume = Σ, start_volume = first, tao_price_usd = last, `ts_last` NULL, `tmc_block_number` = last. A bucket is written only if it has ≥ 6 of 12 (1h) or ≥ 144 of 288 (1d) 5m bars. Nothing is ever forward-filled.
- All rows go through `bars.upsert_bars` (precedence, §2.5); then `bars.enrich_reserves(netuid, '1h'|'1d', gap_start, gap_end)`.
- 5m `tmc_candle` rows fall under 03's `price_bars_5m` retention (90 days), so the gap's 5m series is deleted about 90 days after its `ts`. The 1h/1d aggregates are kept forever. Consumers that need 5m history must read it within that window, or the operator raises `PRICE_BARS_5M_RETENTION_DAYS` (03).

Budget: 3 × 128 + 2 = 386 requests (+ ≤ 10 % retries, including the chunked candle retries), once; ≈ 0.5 % of `TMC_MONTHLY_CAP` (90,000). This equals 05 §6.3's row for `backfill_tmc` (02 §G3). A later `--redo` is a new one-off of the same size and must be re-checked against that table.

### 2.5 `price_bars` downsampling, precedence, enrichment (`app/backfill/bars.py`)

`pool_history` → `price_bars` for periods `1h` (3600) and `1d` (86400). Ledger 5m bars are not built (no consumer; 13 uses 1h/1d). Per netuid and month chunk (one transaction each), with the window extended back by one period so `LAG` is defined:

```sql
WITH s AS (
  SELECT netuid, (timestamp / :p) * :p AS b, timestamp, block_number, price, tao_in_pool, alpha_in_pool,
         subnet_volume, occupant_key, source, enriched_at,
         ROW_NUMBER() OVER w_desc AS rn_last, ROW_NUMBER() OVER w_asc AS rn_first,
         MAX(price) OVER w_all AS hi, MIN(price) OVER w_all AS lo,
         -- latest non-NULL moving_price inside the same bucket (never from another bucket):
         FIRST_VALUE(moving_price) OVER (PARTITION BY timestamp / :p
             ORDER BY moving_price IS NULL, timestamp DESC, block_number DESC)                AS mp_last,
         FIRST_VALUE(enriched_at IS NOT NULL OR timestamp < :live_from OR source NOT LIKE 'ledger_%')
             OVER (PARTITION BY timestamp / :p
                   ORDER BY moving_price IS NULL, timestamp DESC, block_number DESC)           AS mp_reconstructed
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
       timestamp AS ts_last, timestamp AS event_at, tao_in_pool, alpha_in_pool, mp_last AS moving_price, block_number,
       CASE WHEN source LIKE 'ledger_%' THEN 'ledger_import' ELSE source END AS source,
       CASE WHEN reconstructed THEN :now ELSE timestamp END              AS observed_at,
       CASE WHEN reconstructed THEN 1 ELSE 0 END                         AS backfilled,
       :run_id AS run_id
FROM w WHERE b >= :from;
```

- **Close = the `MAX(timestamp)` row**, the same bare-column rule as the old `backfill_metadata.py:38-50` and `factor_panel.py:248-256`, written with window functions so that open, high and low are available too. `ts_last` is the actual time of the close sample (kept for audit and the same-rank rule); 13 stamps point-in-time at the bucket, using rows with `observed_at ≤ bucket` (master §10), not `ts_last`. `moving_price` is the latest non-NULL value **inside the same bucket**, which may come from an RPC sample a few seconds before a ledger close row (the dense hours 04-29 → 05-30 that the `ledger` era fills). It is never carried across buckets.
- **`observed_at`/`backfilled` are honest.** A bar keeps `observed_at = ts_last` and `backfilled = 0` only when its close row was captured live by the old bot (`ledger_*` source, `timestamp ≥ LEDGER_LIVE_FROM_TS`, not enriched) **and** its `moving_price` also came from such a row. Every other bar (seed era, the 04-22 → 04-29 hourly rows, merged/enriched values, RPC samples) gets `observed_at` = write time and `backfilled = 1`. This deliberately narrows 03 §2.9's "ledger_import = observed live" wording (§11 item 20).
- Volume is a Δ of the cumulative counter. It is NULL across occupant changes, gaps > 2 periods and counter resets. Nothing is forward-filled. The unit is UNVERIFIED (§11 item 5).

**Precedence upsert** (`upsert_bars(rows, period)`). Order-independent, so S4 and S6 can run in either order:
```sql
INSERT INTO price_bars (netuid, period, ts, occupant_key, open, high, low, close, volume, start_volume, tao_price_usd,
                        tmc_block_number, source, event_at, observed_at, ts_last, tao_in_pool, alpha_in_pool,
                        moving_price, block_number, backfilled, run_id)
VALUES (...)
ON CONFLICT (netuid, period, ts) DO UPDATE SET
  occupant_key = COALESCE(excluded.occupant_key, price_bars.occupant_key), open = excluded.open, high = excluded.high,
  low = excluded.low, close = excluded.close, volume = excluded.volume, start_volume = excluded.start_volume,
  tao_price_usd = COALESCE(excluded.tao_price_usd, price_bars.tao_price_usd), tmc_block_number = excluded.tmc_block_number,
  source = excluded.source, event_at = excluded.event_at, observed_at = excluded.observed_at, ts_last = excluded.ts_last,
  tao_in_pool = COALESCE(excluded.tao_in_pool, price_bars.tao_in_pool),
  alpha_in_pool = COALESCE(excluded.alpha_in_pool, price_bars.alpha_in_pool),
  moving_price = COALESCE(excluded.moving_price, price_bars.moving_price),
  block_number = COALESCE(excluded.block_number, price_bars.block_number), backfilled = excluded.backfilled,
  run_id = excluded.run_id
WHERE src_rank(excluded.source) > src_rank(price_bars.source)
   OR (src_rank(excluded.source) = src_rank(price_bars.source)
       AND excluded.ts_last > price_bars.ts_last);          -- 03 §3.3 / 02 §J1 same-rank rule (NULL ts_last never wins)
```
`src_rank` is **03's** registered deterministic SQL function (`precedence.rank_any` over `precedence.SOURCE_RANK['price_bars']`, registered on every connection by 03's `Database`; 17's `common.open_db` registers the same function for its synchronous connections by importing 03's constant, never its own numbers). The order is `sse_rollup` (5) > `ledger_import` (4) > `tmc_candle` (3) > `onfinality` = `opentensor_archive` (2) > `taostats` (1). The live rows of 05 (`sse_rollup` 5m, its weekly `tmc_candle` 1h) are therefore never downgraded; in the gap, TMC OHLC beats the single-sample RPC bar. A 17-owned row replaced by a higher-rank 17-owned row is not a point-in-time violation, because both are `backfilled=1` reconstructions of immutable chain state. An equal-rank row replaces the stored one only when `excluded.ts_last > stored.ts_last` (03 §3.3, 02 §J1), i.e. a later close sample of the same bucket. So re-running `bars.rebuild` after new same-source rows land in an existing bucket (e.g. Pi rows after 21:24:34 on 2026-06-19 inside the PC's last hour) moves the close forward. An equal-rank row with the same or an earlier `ts_last`, and every `tmc_candle` rewrite (`ts_last` NULL), is skipped and counted `same_rank_skipped`.

**NULL-only enrichment** (`enrich_reserves(netuid, period, from, to)`, called after every `upsert_bars` batch and after `bars.rebuild`). 03 §3.3 allows any source to fill NULL enrichment columns. This fills depth, chain block and moving price onto any bar (a `tmc_candle` bar in the gap, or a `ledger_import` bar in the dense hours whose close row lacks `moving_price`) from the `pool_history` samples of the same bucket:
```sql
WITH s AS (
  SELECT (timestamp / :p) * :p AS b, tao_in_pool, alpha_in_pool, block_number,
         ROW_NUMBER() OVER (PARTITION BY timestamp / :p ORDER BY timestamp DESC, block_number DESC) AS rn,
         FIRST_VALUE(moving_price) OVER (PARTITION BY timestamp / :p
             ORDER BY moving_price IS NULL, timestamp DESC, block_number DESC)                      AS mp_last
  FROM pool_history WHERE netuid = :n AND timestamp >= :from AND timestamp < :to)
UPDATE price_bars AS pb SET
  tao_in_pool   = COALESCE(pb.tao_in_pool,   s.tao_in_pool),
  alpha_in_pool = COALESCE(pb.alpha_in_pool, s.alpha_in_pool),
  block_number  = COALESCE(pb.block_number,  s.block_number),
  moving_price  = COALESCE(pb.moving_price,  s.mp_last),
  backfilled    = 1, observed_at = MAX(pb.observed_at, :now)      -- the enriched bar was knowable only now
FROM (SELECT * FROM s WHERE rn = 1) AS s
WHERE pb.netuid = :n AND pb.period = :period AND pb.ts = s.b
  AND ((pb.tao_in_pool IS NULL AND s.tao_in_pool IS NOT NULL) OR (pb.moving_price IS NULL AND s.mp_last IS NOT NULL));
```
`price_bars.block_number` is therefore always a chain-accurate block (from `pool_history`) or NULL, never TMC's candle label.

### 2.6 Fundamentals and whale-cohort import (`app/backfill/fundamentals_import.py`)

**Verified file facts.**
- `fundamentals.json`: top-level keys `generated_at, generated_at_unix, schema_version, config, count, subnets`, where `subnets` is a dict keyed by the netuid **string** ("0".."128"); `schema_version 2`, `generated_at 2026-06-02T11:03:41.944123Z`, `generated_at_unix 1780398221`, `count 129`. The per-record keys are exactly `age_days, category, components, coverage, coverage_static, discord, github, github_archived, github_pushed_days_ago, github_stars, name, netuid, owner, quality, quality_static, short_desc, slug, sources, telegram, twitter, website`. **There is no `team_info` field (0/129).**
- `whale_cohort.json`: top-level `as_of_block 8590789`, `as_of_ts "2026-07-10T12:13:43.315337+00:00"`, `generated_by`, `params`, 200 `coldkeys` rows `{ss58, rank, total_stake_tao, balance_total_tao, free_fraction, balance_rank, coldkey_swap, label, label_source, attribution}` plus `excluded` (1 row: rank 7, `excluded_reason: "exchange:Taobridge"`).
- `whale_labels.json` = `{"_README": [...], "labels": {}}`. Labels would live under `labels`, keyed by `ss58`, and that dict is empty.

**Steps** (all under one `run_id`; `observed_at = event_at = 1780398221` = `generated_at_unix` as INTEGER unix seconds, per 03's column types; 04's ISO `LEGACY_AS_OF` is parsed with 01's `app.util.time.parse_iso` (02 §B1) and must agree to the second):
1. **Links and owner.** Call 04's legacy fundamentals import (the code path of `seed_registry` step 3), exposed as `app.registry.legacy.import_fundamentals(db, path, *, as_of, run_id, occupancy) -> LegacyReport`. It writes `identity_observations` with `source='legacy'`, `source_ref='fundamentals.json@2026-06-02'`, and `manual_overrides` (`mode='fallback'`, `source='legacy'`), using 04's field map, junk rules and hash-dedupe. Re-runs are therefore no-ops. Improvement over 04's guard: with the reconstructed occupancy, `occ_then = OccupancyIndex.occupant_at_ts(netuid, 1780398221)`. **Observations** go to `occ_then`, which is point-in-time honest: that occupant is the one the values described. **Overrides** are written only if `occ_then` equals the current occupant (04's re-occupancy guard). If `occ_then` is unknown (S2 run before S5), 04's `registered_at_est` guard applies unchanged. `identity_observations.occupant_key` has a foreign key to `occupants` (03), so `occ_then` must already exist there. After S5 it always does, because the trust boundary 1780182493 precedes 1780398221, so every netuid's occupant at `generated_at` is attested by trusted ledger rows.
2. **Raw snapshot.** Each record goes to `identity_raw_snapshots(source='legacy', scope='subnet', netuid, occupant_key=occ_then, source_ref='fundamentals.json@2026-06-02', payload_json=record, payload_hash, observed_at, event_at, block_number=NULL, run_id)` (03 column names) through 04's `ObservationWriter.write_raw` (hash-dedupe, so a re-run writes nothing). This preserves `category`, `quality`, `quality_static`, `components`, `github_pushed_days_ago`, `github_stars`, `github_archived`, `age_days` for audit and cross-check. They are **never** turned into `ts-1.0` metrics (different model).
3. **`team_info`.** If a record carries `team_info` (e.g. a newer Pi copy of the file), write `identity_observations(field='team_info', source='legacy', value=text)`. Spec 11's seeder parses it into *unverified* team candidates. Nothing is written to `team_members` here, and with schema v2 nothing is written at all.
4. **Whale cohort.** `seed/whale_cohort.json` `coldkeys[]` goes into `whale_cohort` with `INSERT OR IGNORE` on `(as_of_block, coldkey = ss58)` (03 columns incl. the appended `total_stake_tao, free_fraction, balance_rank, coldkey_swap`; `observed_at` = the run's import time, because 03 defines it as "import time of the snapshot file", so it is not `as_of_ts`; `source='whale_cohort_json'`). `as_of_ts` is kept in the report. `excluded[]` entries are not cohort members and are not imported; they are counted in the report. Labels from `whale_labels.json["labels"]` (a dict keyed by `ss58`, currently `{}`) are merged into each row as `label, label_source, attribution` **before** the insert. There is no later UPDATE: `whale_cohort` is in 03's `APPEND_ONLY_TABLES` and names no annotation columns (03 §2.5). Labels added to the file after the import are therefore not applied; the dormant table is revisited with the whale port. A Pi copy with a different `as_of_block` becomes an additional snapshot. The table is **dormant**: no job reads it until a TaoStats key revives the whale port (master §2).
5. Report to `data/backfill/fundamentals_import_report.json`: `{legacy: LegacyReport, raw_snapshots, team_info_rows, whale_rows, whale_excluded, whale_as_of_ts, labels_merged, attributed_via: {occupancy: n, guard_04: n}}`.

### 2.7 Regime backfill (`app/backfill/regime_backfill.py`)

The classifier is spec 12's pure port of the old `app/strategy/regime.py` in `app/quant/regime.py`: `compute_regime_metrics(per_netuid_snapshots, params)` (old lines 159-221) and `classify_regime` (131-156), called with spec 12's frozen `REGIME_TS_1_0` (`RegimeParams`, the old `config.py:454-464` values). Those values: bucket 4 h, window 24 h, `MIN_BUCKETS 6`, `MIN_SUBNETS 10`, vol trend 0.30 / chop floor 0.10 / dead 0.05, dir 0.02, disp 0.015. They are not config keys (12: weights and parameters are frozen per `spec_version`). For each official slot `D` (UTC midnight) from `REGIME_BACKFILL_START` (default: the first day with ≥ 10 netuids of `price_bars` 1h) to the day before the first `regime_daily` row with `source='live'` (else yesterday):
1. `snaps = {netuid: [{"ts": ts, "price": close}]}` (INTEGER `ts`, 12's call convention; its port reads it with `parse_ts`, 02 §B1) from `price_bars` `period='1h'`, `ts ∈ (D − 86400, D]` (bucket start, the same window and timestamp convention as 12's `regime_for_slot`, which reads `chain_hourly.hour_ts`), `netuid ≠ 0`, `close > 0`, all sources. This mirrors the live series. It differs from 12 only in the price source (`price_bars` close vs `chain_hourly.price`) and in having no `is_active` filter, because historical `is_active` is unknown.
2. `m = compute_regime_metrics(snaps, params)`. If `m["thin_universe"]`, then `label = 'UNKNOWN'` (02 §B8 uppercase vocabulary, 03's CHECK, same as 12), **not** `DEAD`. The old function returns `DEAD` for thin data, which would mislabel the daily-only seed era. Otherwise `label = m["raw_regime"]`. `v_w` = 12's frozen `v_w_by_regime[label]`: 0.25 for TRENDING/DISPERSED, 0.10 for DEAD, 0.15 otherwise (master §7.6); it is always written (`v_w` is NOT NULL).
3. `INSERT OR IGNORE INTO regime_daily(day_ts=D, spec_version=SPEC_VERSION, label, raw_regime=m["raw_regime"], vol_24h, directional_strength, dispersion, n_subnets, thin_universe, v_w, tao_usd_ret_24h=NULL, sum_prices_ret_24h=NULL, inputs_json={"series":"price_bars_1h","window":[D-86400,D]}, source='backfill', computed_at=now, backfilled=1, run_id)` (03 §3.6 columns; `source ∈ {live, backfill}` is row provenance, not the §B9 vocabulary; there is no `observed_at` column, 02 §B6/§B8). Live rows are never replaced. No debounce is applied (12 does not port `REGIME_DEBOUNCE_CYCLES` either).

Expected result: 2025-09-22 → 2026-04-21 is `UNKNOWN` unless the `ledger` RPC era filled hourly bars, after which it is classified. 04-22 onward is classified.

### 2.8 GitHub history backfill (`app/backfill/github_backfill.py`, job `backfill_github`)

`BackfillGithubJob(CollectorJob)`: `id='backfill_github'`, `phase=4`, `enabled_flag='BACKFILL_GITHUB_ENABLED'`, `provider='github'`, `manual` trigger (02 §E1, as in §2.4), `timeout_sec=BACKFILL_GITHUB_TIMEOUT_SEC` (14,400), resumable. Both backfill jobs stop themselves at a **soft deadline** of `timeout_sec − 300` s at the next repo/netuid boundary. They return `ok` with `note="partial: <n> units left"` rather than letting 01's `asyncio.wait_for` cancel them, which would count as a failure toward the 5-failure alert. The launcher re-triggers until the sidecar shows no pending units (a ≈ 6–13 h GitHub run takes 2–4 triggers). The launcher `scripts/backfill_github.py [--netuids …] [--no-prs] [--redo]` writes `data/backfill/github_request.json`. The job uses 06's process-wide `GitHubClient` (whose `get` already calls `quota.wait_if_reserved(job)` with 06's `GITHUB_RESERVE_REMAINING` = 300). 06's `wait_if_reserved(job)` takes no reserve argument, so the job adds its own, stricter gate before every request: `if quota.remaining is not None and quota.remaining < BACKFILL_GITHUB_RESERVE_REMAINING (1,500): await sleep_until(quota.reset_at)`. Live 06 jobs therefore keep ≥ 1,500 requests of headroom in every rate-limit hour (02 §G4).

**Overnight window (02 §G4).** The job works only inside `BACKFILL_GITHUB_WINDOW_UTC` (default `"00:30-08:00"`). That is after 06's `github_org_enum` (20:00Z) and `github_activity` (20:30Z, < 4 h) batch, so the backfill never competes with 06's heaviest hours; in the window 06 spends only `github_resolve` (≈ 180 calls per 6-h run). A trigger outside the window returns at once. Inside it, the job stops at the next repo boundary when the window closes. Both cases return `ok` with `note="partial: outside window, <n> units left"`; the launcher then sleeps until the window opens and re-triggers. With the reserve gate, 17's share is ≤ 3,500 req/h from the PAT's 5,000/h (also capped by 01's shared `github` limiter, 4,000/h). 06's PAT budget table carries this row (02 §G4).

**Scope.** Every occupant with `github_occupant_repos` rows (active, pre-launch, and the reconstructed deregistered occupants that 06 attributed). Its repos are the rows with `role ∈ {declared, primary, org}` (non-fork unless declared), plus non-archived org repos in `github_repos` with `pushed_at ≥ BACKFILL_GITHUB_SINCE`. At most `BACKFILL_GITHUB_MAX_REPOS_PER_OCCUPANT` (30) repos, by `pushed_at` desc. Each `repo_id` is processed once. Progress sidecar: `data/backfill/github_progress.json` `{repo_id: {commits: done|partial, releases, prs, pages, oldest_seen, truncated, last_error}}`.

**Per repo** (by numeric id, so renames cannot break it):
1. **Commits.** `GET /repositories/{repo_id}/commits?since={BACKFILL_GITHUB_SINCE}T00:00:00Z&per_page=100` via `get_all_pages(path, params=…, max_pages=BACKFILL_GITHUB_MAX_PAGES_PER_REPO, job='backfill_github')` (50 = 5,000 commits, default branch). Sub-paths of `/repositories/{id}` are UNVERIFIED: research verified only `GET /repositories/{id}` itself, and 06's live path is `/repos/{owner}/{repo}/commits`. On a 404 from the id form the job retries once with `/repos/{github_repos.full_name}/commits` and records `path_form` in the sidecar. Each row goes through 06's `authors.author_key`, `authors.is_bot` and `spam.flag_commits`, then `INSERT OR IGNORE INTO github_commits` (03 columns: `event_at` = author date as INTEGER, `observed_at = now`, `source='github_api'`, `run_id`, `backfilled = 1`). Overlap with 06's live 90-day window is harmless (same `sha`). If the page cap is hit: `truncated = true`, `oldest_seen` is recorded, and coverage marks the repo partial.
2. **Releases.** `GET /repositories/{repo_id}/releases?per_page=100` (≤ 5 pages; same path fallback) → `github_events(repo_id, event_kind='release', event_at=parse_iso(published_at), dedupe_key='release:{repo_id}:{tag_name}', payload_json={tag_name, prerelease, draft, backfilled: true}, source='github_api', observed_at, run_id)`. The dedupe key and payload keys are 06 §2.5's, so the live collector and the backfill never duplicate a release; drafts are skipped as in 06. The extra key `backfilled: true` stands in for 02 §J1's `backfilled=1`, because 03's `github_events` has no `backfilled` column (§11 item 24). *(Note 2026-10-17, 06 build: 03's `github_events` now has `backfilled`; set the column to 1 as well when this job is built — 06 §11 item 15. 06's live rows are 0; `ghevents.raw_event(backfilled=1)` takes it.)* Tags are not backfilled: they carry no date without a per-tag commit lookup. The `release_cadence_90d` history therefore counts releases only (a coverage caveat).
3. **Merged PRs** (`BACKFILL_GITHUB_PRS_ENABLED`, default true). `GET /repositories/{repo_id}/pulls?state=closed&sort=updated&direction=desc&per_page=100` until `updated_at < since`, keeping at most `BACKFILL_GITHUB_PR_MAX_PER_REPO` (200) merged PRs. For each, `GET /repositories/{repo_id}/pulls/{n}/reviews` feeds `github_events(event_kind='pr_merged', event_at=parse_iso(merged_at), payload_json={number, author_login, review_count, reviewers, backfilled: true}, dedupe_key='pr_merged:{repo_id}:{number}', source='github_api')`. `review_count` uses 06's rule: states `APPROVED|CHANGES_REQUESTED|COMMENTED` by a non-bot user ≠ PR author. 06 does not name its PR dedupe key; this one is proposed to 06 (§11). Until 06 adopts it, the `UNIQUE` on `dedupe_key` can admit one duplicate per PR if 06 picks a different key.
4. **Rollups.** 06's `github_activity_daily` rollup (REPLACE per `(repo_id, day_ts)`, 03 column name) for days ≥ `BACKFILL_GITHUB_SINCE`. Then 06's Electric-Capital classifier with an injected day produces `github_authors_daily` rows for each occupant and day on which the author has ≥ 1 commit-day in the trailing 84 days. Rows for days before 06's first successful live `github_activity` run get `backfilled = 1`.

**Point-in-time caveat** (06 open question 6). Repo membership is today's resolution. History computed from these rows carries the caveat `github_org_membership_asof_today` in `coverage.json`, and repos that were deleted, made private or force-pushed are invisible. No `events` are emitted for historical GitHub facts.

### 2.9 Coverage ledger (`app/backfill/coverage.py`, `scripts/build_coverage.py`)

`build(db_path) -> dict` is pure read-only SQL (`mode=ro`) and takes < 1 min. It is written atomically (04's `app/utils/fs.write_atomic`) to `COVERAGE_PATH` (`data/quant/coverage.json`) after every step. 13 may call it on its snapshot. Definitions:
- **Expected** (netuid, hour) pairs: hours inside a known occupancy interval `[registered_ts, deregistered_ts)`. Where occupancy is unknown, the netuid counts as expected in any month in which it has at least one row.
- **Completeness** = covered/expected per month and period.
- **Hole** = a maximal run of ≥ 6 consecutive hours in which fewer than 50 % of the expected netuids have a 1h bar.

```json
{
  "schema": "coverage-1", "generated_at": 1790150400, "spec_version": "ts-1.0", "db_schema_version": 1,
  "eras": {
    "pre_ledger": {"from_block": 4920351, "to_block": 6502593, "status": "done|partial|not_run", "buckets_done": 5274, "buckets_failed": 0},
    "ledger":     {"from_block": 6502594, "to_block": 8443757, "status": "done", "buckets_done": 5975, "buckets_failed": 2},
    "gap":        {"from_block": 8443758, "to_block": 9131000, "status": "done", "buckets_done": 2311, "buckets_failed": 0},
    "live":       {"from_ts": 1790000000}
  },
  "series": {
    "pool_history":  {"rows": 7655000, "netuids": 131, "min_ts": 1758585588, "max_ts": 1790140000,
                      "by_source": {"ledger_pc": {"rows": 7362854, "min_ts": 1758585588, "max_ts": 1781904274}, "onfinality": {"rows": 292000}},
                      "moving_price_nonnull_share": 0.53, "occupant_key_null_rows": 0,
                      "price_method": {"swap_sqrt": 290000, "reserve_ratio": 2000}},
    "price_bars_1h": {"rows": 1200000, "by_source": {"ledger_import": 149079, "tmc_candle": 295000, "onfinality": 690000},
                      "completeness_by_month": {"2025-09": 0.97, "2026-04": 0.88}, "with_reserves_share": 0.93},
    "price_bars_1d": {"rows": 48000, "by_source": {}}, "price_bars_5m": {"rows": 3500000, "by_source": {}},
    "chain_daily":   {"rows": 46000, "min_day": 1758499200, "max_day": 1790121600, "unattributed_rows": 0},
    "chain_hourly":  {"rows_by_source": {"sse": 0, "rest_table": 0, "analytics_1m": 92000, "analytics_1w": 0}},
    "block_times":   {"rows": 13100, "min_block": 4920351, "max_block": 9131000},
    "regime_daily":  {"days": 366, "unknown_days": 0, "from_day": 1758499200, "to_day": 1790035200},
    "github_commits":{"repos": 1200, "repos_truncated": 4, "min_event_at": 1739404800, "backfilled_rows": 180000}
  },
  "occupancy": {"occupants": 170, "historical": 41, "anomalies": [],
                "reoccupations": [{"netuid": 40, "old": "40:3372582", "new": "40:8409860", "at_ts": 1781500000}],
                "unstamped": {"pool_history": 0, "price_bars_1h": 0}},
  "holes": [{"series": "price_bars_1h", "from_ts": 1775779200, "to_ts": 1776816000, "netuids": "all",
             "reason": "ledger hole 2026-04-10..21 and ledger era not run"}],
  "reconstructible_from": {"price_depth_moving_price": 1758585588, "emission_share": 1758499200, "net_flow_30d": 1761091200,
                           "owner_commitment": 1780531200, "tenure": 1739482884, "github_p2": 1739404800, "regime": 1758585600,
                           "control_stability_90d": null},
  "forward_only": ["control_stability_90d", "site_age", "team_doxxed", "…"],
  "caveats": ["github_org_membership_asof_today", "subnet_volume_unit_unverified", "price_method_mixed",
              "seed_era_daily_only_before_2026-04-22", "seed_era_taostats_price", "tmc_candle_label:start",
              "ledger_observed_later_before_1777468595", "moving_price_enriched_rows"],
  "checks": {"ledger_rows": {"expected": 7362854, "actual": 7362854, "pass": true},
             "ledger_backfill_merge": {"expected": 23237, "actual": 23237, "pass": true},
             "rpc_golden_block": "pass", "occupant_key_mismatch_rows": 0},
  "provenance": [{"step": "import_old_ledger", "run_id": "…", "finished_at": 1790100000, "rows": 7362854,
                  "inputs_sha256": {"seed/ledger.db": "…"}}]
}
```
**Consumption by 13** (02 §J2: 13 reads this file, or calls `build()` on its backup snapshot):
- `forward_only` marks factors 13 must treat as unobserved before live collection (master §10 backtestability).
- `reconstructible_from` clamps each factor's earliest usable date. Keys are master §7.3 `metric_key`s (`owner_commitment`; the old alias `owner_conviction` is retired, 02 §C2/§J2) plus the group keys `price_depth_moving_price`, `github_p2`, `regime`. Each value is computed from the data, never hard-coded. For example, `owner_commitment` = the first `chain_daily.day_ts` with a non-zero `owner_personal_locked_alpha` for any occupant (research 02-gapfill-keyless §2 saw SN51's first owner lock on 2026-06-04; earlier locks elsewhere are possible).
- `holes` and `completeness_by_month` flag `insufficient_data` dates.
- `occupancy.unstamped` feeds `occupant_coverage`.
- `caveats` are copied into every result's provenance.
- `checks.rpc_golden_block ≠ "pass"` makes `--pit-mode reconstructed` warn (§11).

Numbers in the example are illustrative except the verified ledger values.

**Schema `coverage-1` (exact; the contract with 13, 02 §J2).**
- Every key below is always present. A value that cannot be computed is `null`, never omitted.
- Times are INTEGER unix seconds UTC (02 §B1). `*_day` values are UTC-midnight `day_ts`, month keys are `"YYYY-MM"` (UTC) and intervals are `[from, to)`.
- New keys may be added within `coverage-1`, and readers ignore unknown keys. Removing or renaming a key, or changing a type, bumps `schema`.

| path | type | definition |
|---|---|---|
| `schema` | str | `"coverage-1"` |
| `generated_at` | int | build time |
| `spec_version`, `db_schema_version` | str, int | `SPEC_VERSION`; 03's `schema_version` of the DB read |
| `eras.gap`, `eras.ledger`, `eras.pre_ledger` | object | `from_block`, `to_block` (int; `gap.to_block` = newest `done` bucket's block, null if not run); `status`: `"done"` (every planned bucket `done`/`skipped`), `"partial"` (a bucket `failed` or still unplanned) or `"not_run"` (no `backfill_buckets` rows); `buckets_done`, `buckets_failed`, `buckets_skipped` (int) |
| `eras.live.from_ts` | int \| null | first `chain_hourly.hour_ts` with `source ∈ {sse, rest_table}` |
| `series.pool_history` | object | `rows`, `netuids`, `min_ts`, `max_ts`; `by_source` {source: {`rows`, `min_ts`, `max_ts`}}; `moving_price_nonnull_share` (0..1); `occupant_key_null_rows`; `price_method` {`swap_sqrt`, `reserve_ratio`} (row counts) |
| `series.price_bars_1h`, `series.price_bars_1d` | object | `rows`; `by_source` {source: rows}; `completeness_by_month` {month: covered ÷ expected (netuid, bucket) pairs, 0..1}; `with_reserves_share` (share of bars with non-NULL `tao_in_pool`) |
| `series.price_bars_5m` | object | `rows`; `by_source` {source: rows} |
| `series.chain_daily` | object | `rows`, `min_day`, `max_day`, `unattributed_rows` (Σ `unattributed` in `tmc_progress.json`) |
| `series.chain_hourly` | object | `rows_by_source` {`sse`, `rest_table`, `tick_rollup`, `analytics_1w`, `analytics_1m`} |
| `series.block_times` | object | `rows`, `min_block`, `max_block` |
| `series.regime_daily` | object | over rows of `SPEC_VERSION`: `days`, `unknown_days` (`label='UNKNOWN'`), `from_day`, `to_day` |
| `series.github_commits` | object | `repos`, `repos_truncated` (from `github_progress.json`), `min_event_at` (int \| null), `backfilled_rows` |
| `occupancy` | object | `occupants`, `historical` (17-written deregistered rows); `anomalies` [{`netuid`, `reason`, `detail`}]; `reoccupations` [{`netuid`, `old`, `new`, `at_ts`}]; `unstamped` {`pool_history`, `price_bars_1h`} (rows with NULL `occupant_key`) |
| `holes` | list | [{`series` (`"price_bars_1h"`), `from_ts`, `to_ts`, `netuids` (`"all"` or [int]), `reason` (str)}] per the hole definition above |
| `reconstructible_from` | object | exactly the keys `price_depth_moving_price`, `emission_share`, `net_flow_30d`, `owner_commitment`, `control_stability_90d`, `tenure`, `github_p2`, `regime` → earliest instant with usable data, computed from the DB (int), or null (null for `control_stability_90d` while it is in `forward_only`) |
| `forward_only` | list[str] | sorted master §7.3 `metric_key`s with no reconstructed history, so 13 returns `None` before their first live observation. At least: every P3 key incl. `site_age`, every P5 and P6 key, `team_doxxed`, `owner_identity`, `owner_retention_30d` (unless TaoStats history exists) and `control_stability_90d` (until 11 confirms reconstructed reads, §2.11). A key is never in both this list and `reconstructible_from` with a non-null value |
| `caveats` | list[str] | sorted, unique; vocabulary: `github_org_membership_asof_today`, `subnet_volume_unit_unverified`, `price_method_mixed`, `seed_era_daily_only_before_2026-04-22`, `seed_era_taostats_price`, `tmc_candle_label:start`, `tmc_candle_label:end`, `tmc_candle_label:unverified`, `ledger_observed_later_before_1777468595`, `moving_price_enriched_rows` |
| `checks` | object | one {`expected`, `actual`, `pass` (bool)} per §10 count check (`ledger_rows`, `ledger_backfill_merge`, …); plus `rpc_golden_block` (`"pass"`, `"fail"` or `"not_run"`) and `occupant_key_mismatch_rows` (int) |
| `provenance` | list | per step, from its newest `collector_runs` row: [{`step`, `run_id` (str), `finished_at`, `rows`, `inputs_sha256` {path: hex}}] |

### 2.10 Pi recovery cross-reference (checklist A4; `app/backfill/pi_recovery.py`, `scripts/pi_recovery_report.py`)

**Inputs** (operator copies them to `seed/pi/` per A4): `ledger.db` (+ `-wal`/`-shm`), `backups/*.db`, `logs/*.jsonl`, `pi-journal.txt`, `.env`, and `wallets/trader_wallet/coldkeypub.txt`. **Guard:** the module refuses to open any path whose basename is `coldkey` or that lies under `hotkeys/`, and reads only the *names* of `.env` keys, never their values.

**The report** (`data/backfill/pi_recovery_report.json` + a console table) has four parts:
1. **Pi `pool_history`:** rows, `MAX(timestamp)` (ISO), `MAX(block_number)`, rows beyond the PC maximum 1781904274, and `nra_backstamped` = rows with `timestamp < LEDGER_NRA_TRUST_FROM_TS` and non-NULL `network_registered_at`. It also states whether the S1 import has already run with `ledger_pi`. If not, it prints the command; the `gap` era then starts at the Pi's last block automatically.
2. **Pi `ema_positions`:** the 10 PC-OPEN positions (416 sn99, 417 sn47, 419 sn31, 421 sn122, 424 sn108, 425 sn94, 426 sn116, 429 sn113, 431 sn67, 432 sn109; 1.0 τ each, `strategy='value'`, entered 2026-06-05..06-17). For each: Pi `status`, `exit_ts`, `exit_reason`, `amount_tao_out`, `tao_recovered`, `staked_hotkey`, plus any other `OPEN`/`STUCK` rows.
3. **On-chain check (keyless).**
   - **Coldkey SS58:** read from `coldkeypub.txt`, either as JSON `{"ss58Address": …}` or as a plain SS58 line, or given with `--coldkey`.
   - **Query:** the launcher submits `backfill_tmc` with `tasks=["positions"]` and the coldkey, so the call uses the shared bucket. TMC `/accounts/coldkey-stakes/{coldkey}/` is parsed tolerantly (shape UNVERIFIED): rows are taken from `results|data|items|<root list>`; `netuid` from `netuid|subnet_id|subnet`; `hotkey` from `hotkey|hotkey_ss58`; alpha from `balance_alpha|alpha|stake|balance`; τ value from `balance_tao|tao_value|value_tao`. The raw response is kept in `chain_rest_snapshots`.
   - **Output:** for each of the 10 netuids, `still_staked` (alpha > 0), `alpha`, `tao_value`, and whether the hotkey matches `staked_hotkey`, plus every other netuid with stake. `coldkey-subnet-chart` (hourly stake history) is stored raw so the unstake time can be read manually.
4. **Other artefacts:** presence of `VALUE_KILLED`/`KILL_SWITCH` markers, the last 20 lines of `pi-journal.txt`, and which `.env` key names are present (e.g. `TAOSTATS_API_KEY`, `GITHUB_PAT` for checklist B1/C3).

TaoSentinel never unstakes. The report only tells the operator which positions still hold alpha; acting on that is a manual wallet action on the Pi, outside this system.

### 2.11 Historical metric replay hand-off (step S10)

17 writes raw history only. `scripts/replay_history.py --from YYYY-MM-DD --to YYYY-MM-DD --owners chain,github,integrity,scoring` is a thin, resumable loop (`job_state('replay_history', 'last_day:<owner>')`) that calls the owners' entry points for each day `D`:
- **chain:** 05's `run_slot(db, as_of=D, run_id=…, observed_at=now_ts(), backfilled=1)` (05 §8, the entry point of this loop). When no `chain_hourly` row exists, 05's `snapshot_rows` uses its historical tier (05 §2.3): `price_bars` 1h (close, `moving_price`, `tao_in_pool`) plus `chain_daily` (`emission_percent`).
- **github:** 06's `P2Metrics.compute(occupant_key, now=D 23:59:59Z)`.
- **integrity:** 11's C5 entry points (11 §8.1 `app/integrity/features.py`), written through `put_metric_values` with `source='integrity'` and `backfilled=1`. There are two:
  - `compute_owner_commitment(db, O, as_of=D)` is a partial reconstruction. Only the lock numerator `chain_daily.owner_personal_locked_alpha` has history (TMC 1Y daily); there is no auto-lock or total-stake history.
  - `compute_control_stability_90d(db, O, as_of=D)` reads the `owner_changes` rows that 11's `owner_rpc_seed` derived from `rpc_owner_history` (hourly resolution; no identity-churn pairing).

  Those seeded rows carry `observed_at` = import time (11 T45), so a strict `observed_at ≤ D` read sees none of them. The replay therefore works only if 11's entry points admit backfilled evidence by `event_at ≤ D`, 12's reconstructed mode. Whether they do is UNVERIFIED and is 11's decision (§11 item 26). Until 11 confirms it, `coverage.json` lists `control_stability_90d` under `forward_only`.
- **scoring:** 12's scoring backfill.
- **Not replayed (forward-only):** 07's `site_age`. The Wayback first capture is a timeless fact, but the website's attribution to an occupant (`watch_channels.first_seen_at`, `web_checks.observed_at`) exists only from 07's first run, so a replay before it is `failed`. No one-shot Wayback backfill is planned. `coverage.json` lists `site_age` under `forward_only` together with master §10's forward-only set, so the backtestability matrix stays honest. Also forward-only there: the P3 keys other than `site_age`, P5, P6, `team_doxxed`, `owner_identity` and `owner_retention_30d` (without TaoStats).

Contract every replay must honour (02 §B9, §J1): `as_of = D`; `metric_values.backfilled = 1` and `value_json.backfilled = true`; `observed_at` = actual write time (never faked); `spec_version = SPEC_VERSION`; `source` = the owner's (never a `backfill_*` value); events emitted by a replay carry `payload.backfilled = true` and are written with `alerted_at` preset. Keys whose inputs exist only live (`exit_capacity` needs `price_after`; `chain_buys`; `holder_dispersion`; `participation`; `emission_enabled_flag`) are written `failed` with reason `snapshot_historical` (05 §2.3), i.e. `None` downstream, per 13 §2.5.

---

## 3. Data contract

### 3.1 Tables written

| table | written by | key / write rule | notes |
|---|---|---|---|
| `pool_history` | ledger import, `backfill_rpc` | `(netuid, block_number)` `INSERT OR IGNORE`; NULL-only `UPDATE` of `moving_price, subnet_volume, network_registered_at, occupant_key`, plus `enriched_at` for the merge of observed values only, never for stamping (03 §2.9; trigger `trg_pool_history_update` enforces NULL-only) | the master's "imported `pool_history`"; 03 DDL: a rowid table with nullable provenance columns incl. `run_id` (02 §J1, §3.2 below) |
| `price_bars` | `bars`, `backfill_tmc` | `(netuid, period, ts)` precedence upsert with 03's `src_rank`; a same-rank row is replaced only when `excluded.ts_last > stored.ts_last` (02 §J1, §2.5); NULL-only enrichment | 03 DDL; `source ∈ {ledger_import, tmc_candle, onfinality, opentensor_archive}`; `event_at`, `observed_at`, `backfilled`, `run_id` always set |
| `block_times` | `backfill_rpc`, occupancy | `block_number` `INSERT OR IGNORE` | 03 DDL; exact anchors (13 `block_to_unix` may read it) |
| `backfill_buckets` | `backfill_rpc` | `(era, bucket_ts)` upsert (`failed` → `done`) | 03 DDL; resume + audit |
| `rpc_owner_history` | `backfill_rpc` | `(netuid, block_number)` `INSERT OR IGNORE`, change-only | 03 DDL; consumers 11 (owner-key swap history, S3 `control_stability_90d` back-fill), 04 (name history) |
| `occupants` | occupancy | `occupant_key` `INSERT OR IGNORE`; limited UPDATE (§2.2) | 04 owns the live rows; 03 DDL includes the exact INTEGER `registered_ts`, `deregistered_ts` next to 04's `registered_at_est` (02 §J1) |
| `events` | occupancy; every step's end (§3.4) | only through 03's `emit_event`; `UNIQUE(event_type, dedupe_key)` | historical `netuid_reoccupied` (`severity='warning'`, `source='registry'`, `payload.backfilled=true`, `alerted_at` preset; 02 §B9, §J1) and the system events `backfill_done`/`backfill_failed` (02 §D1) |
| `chain_daily` | `backfill_tmc` → 05 `upsert_daily_from_analytics` | `(netuid, day_ts)` with 03 precedence `analytics_1y` > `analytics_1w`, same rank never overwritten, pre-set `occupant_key` honoured (02 §J1); `source='analytics_1y'`, `backfilled=1` | 03 DDL (`occupant_key NOT NULL`) |
| `chain_hourly` | `backfill_tmc` → 05 `upsert_hourly_from_analytics` | missing `(netuid, hour_ts)` only (`analytics_1m` is 03's lowest rank); `source='analytics_1m'`, `backfilled=1` | 03 DDL |
| `chain_rest_snapshots` | `backfill_tmc` | 03 key `(endpoint, params, observed_at)`; literal path per netuid | raw analytics/positions payloads (restamp); 90-day retention (03) |
| `identity_observations`, `identity_raw_snapshots`, `manual_overrides` | fundamentals import via 04 code | 04 hash-dedupe / UNIQUE | `source='legacy'`, `source_ref='fundamentals.json@2026-06-02'`, times INTEGER |
| `whale_cohort` | fundamentals import | `(as_of_block, coldkey)` | dormant; 03 DDL with appended columns; `source='whale_cohort_json'` |
| `regime_daily` | regime backfill | `(day_ts, spec_version)` `INSERT OR IGNORE` | 03 DDL (02 §B8): `label` uppercase incl. `UNKNOWN`, `v_w` NOT NULL, `source='backfill'`, `computed_at`, `backfilled=1` |
| `github_commits`, `github_events`, `github_activity_daily`, `github_authors_daily` | `backfill_github` via 06 functions | 06/03 keys (`sha`, `dedupe_key`, `(repo_id, day_ts)`, `(occupant_key, author_key, day_ts)`) | `backfilled = 1` (02 §J1). `github_events` has no such column, so its rows carry `payload_json.backfilled = true`. `source` is 06's value (`github_api`) |
| `collector_runs`, `job_state` | every step | 03 DDL (01's tables) | job ids §3.4; `job_state` holds `replay_history` watermarks, and `backfill_rpc` deletes `('owner_rpc_seed', 'rpc_seed_done')` to re-arm 11 (§2.3 step 8) |

### 3.2 DDL (reference copy — owned by 03, schema v1)

03 folds every table and column 17 needs into schema v1 (03 §2.4: "spec 17's `ALTER TABLE … ADD COLUMN` tuple … all in v1"). **17 ships no migration and no ALTER tuple.** The excerpt below is what 17's code relies on, and `common.open_db` checks it at start (§2.1 step 1). On any difference 03 wins, and 17's code follows 03.

```sql
-- pool_history (03 §3.3): the nine old columns byte-identical to models.sql:55-70 (rowid table, non-STRICT,
-- index idx_pool_history_subnet_ts (netuid, timestamp DESC)), then nullable provenance columns:
--   occupant_key TEXT     NULL until stamped (§2.2); seed-era rows below the first known registration stay NULL
--   source       TEXT     ledger_pc | ledger_pi | ledger_extra | onfinality | opentensor_archive
--   event_at     INTEGER  rpc: exact block time (Timestamp.Now // 1000); ledger: NULL (timestamp = capture clock)
--   observed_at  INTEGER  ledger: timestamp if >= LEDGER_LIVE_FROM_TS (live capture) else import-run start (§2.1);
--                         rpc: read time (same as 03's column comment and 03 §2.9)
--   price_method TEXT     rpc: swap_sqrt | reserve_ratio; ledger: NULL (SDK price as captured)
--   enriched_at  INTEGER  set once when a NULL observed value (moving_price, subnet_volume,
--                         network_registered_at) is filled later by the merge; occupant stamping never sets it
--   run_id       TEXT     import/backfill run; NULL on raw old-ledger copies
-- no occupant_key index (03: stamping and bars walk (netuid, block_number) / (netuid, timestamp))
-- + trigger trg_pool_history_update: aborts any UPDATE except NULL-only enrichment; trg_pool_history_no_delete.

-- block_times (block_number PK, ts_ms, event_at GENERATED ALWAYS AS (ts_ms / 1000) VIRTUAL — never inserted,
--   endpoint, observed_at, source DEFAULT 'rpc_timestamp', run_id)  STRICT, WITHOUT ROWID
-- backfill_buckets (era CHECK gap|ledger|pre_ledger, bucket_ts, status CHECK done|failed|skipped, block_number,
--   block_ts, n_subnets, rows_written, price_method CHECK swap_sqrt|reserve_ratio, endpoint, attempts, error,
--   observed_at, run_id; PK (era, bucket_ts))  STRICT
-- rpc_owner_history (netuid, block_number, occupant_key, event_at, observed_at, source DEFAULT 'rpc_archive',
--   run_id, owner_coldkey, owner_hotkey, subnet_name, token_symbol, change; PK (netuid, block_number))  STRICT
--   + index ix_rpc_owner_history_occ (occupant_key, block_number)
-- price_bars (03 §3.3): ts, occupant_key, open/high/low/close, volume, start_volume, tao_price_usd, ts_last,
--   tao_in_pool, alpha_in_pool, moving_price, block_number, tmc_block_number,
--   source CHECK (sse_rollup|tmc_candle|ledger_import|onfinality|opentensor_archive|taostats), event_at,
--   observed_at, backfilled, run_id; PK (netuid, period, ts)
-- occupants (03 §3.2): … registered_at_est, registered_ts, deregistered_ts, deregistered_at (all INTEGER),
--   source CHECK (tmc|rpc|legacy_ledger|onfinality|manual)
-- whale_cohort (03 §3.3): the seven old columns + total_stake_tao, free_fraction, balance_rank, coldkey_swap,
--   observed_at, source
-- regime_daily (03 §3.6, 02 §B8): day_ts, spec_version, label CHECK (TRENDING|DISPERSED|CHOPPY|DEAD|UNKNOWN),
--   raw_regime, vol_24h, directional_strength, dispersion, n_subnets, thin_universe, v_w NOT NULL,
--   tao_usd_ret_24h, sum_prices_ret_24h, inputs_json, source CHECK (live|backfill), computed_at, backfilled, run_id;
--   PK (day_ts, spec_version); no observed_at
-- github_commits / github_activity_daily / github_authors_daily: + backfilled INTEGER NOT NULL DEFAULT 0
--   (github_events has no backfilled column: 17 sets payload_json.backfilled = true)
-- events (03 §3.6): severity CHECK (info|warning|critical); source NOT NULL CHECK (02 §B9 set: registry, chain,
--   github, web, community, x, rss, llm, integrity, scoring, quant, api, runtime, storage, cost, ops);
--   dedupe_key + ux_events_dedupe; written only via app/storage/events.py::emit_event
```

### 3.3 Tables read
`subnets`, `occupants` (04); `chain_hourly` (first hour with `source IN ('sse','rest_table')` = `live.from_ts`; `rest_table` counts too because 05 falls back to it when SSE is silent or needs a key, research 02-gapfill-data-continuity §3); `github_repos`, `github_occupant_repos` (06); `collector_runs`, `job_state` (01). Source databases are attached read-only (`seed/ledger.db`, `seed/backups/ledger_backfill.db` or `seed/ledger_backfill.db`, `seed/pi/ledger.db`, `--extra`).

### 3.4 metric_keys, jobs, events, files

- **metric_keys produced:** none. 17 writes raw history. The owners' replays (§2.11) turn it into these `ts-1.0` keys (master §7.3; the normaliser is applied by 12):

| metric_key | pillar · w | normaliser | history this spec provides | from |
|---|---|---|---|---|
| `emission_share` | P1 · .20 | `rankg` | `chain_daily.emission_percent` | TMC 1Y window (≈ 2025-09-23 →) |
| `net_flow_30d` | P1 · .10 | `zclip(x, −0.5, +0.5)` | `chain_daily.total_tao_buy_24h/sell_24h`, `subnet_tao` | window + 30 d |
| `chain_buys` | P1 · .05 | `rankg` | `chain_daily.chain_buys` (numerator only: there is no market-cap history, so 05's replay writes `failed/snapshot_historical`, 05 §2.3) | forward-only in practice |
| `liquidity_depth` | P1 · .10 | `rankg(log)` | `price_bars.tao_in_pool` (ledger + RPC) | 2025-09-22 (2025-02-13 with `pre_ledger`) |
| `dereg_safety` | P1 · .15 | custom | `price_bars.moving_price` rank + `occupants.network_registered_at` | where `moving_price` is non-NULL |
| `gate_margin`, `miner_burn_ok` | P1 · .15 / .10 | `zclip(log2 x, −2, +1)` / `zclip(x, 0, 0.8)` | `moving_price` + `emission_percent` | meaningful from the v440 regime (13 §2.5) |
| `tenure` | P4 · .10 | `zclip(days, 120, 365)`, floor .3 | `occupants.registered_ts` (exact; `v_quant_occupants.registered_ts`) | all occupants |
| `owner_commitment` (retired alias `owner_conviction`, 02 §C2) | P4 · .20 | custom | `chain_daily.owner_personal_locked_alpha` only. That is the numerator of `lock_ratio` and `commit_pct`: `owner_total_stake`, `eligible_alpha_out` and `auto_lock_enabled` have no keyless history, so this is a partial reconstruction | from the first non-zero owner lock in `chain_daily` (computed into `coverage.json`; not a fixed date) |
| `control_stability_90d` | P4 · .15 | custom | `rpc_owner_history` owner-coldkey/hotkey changes per occupant (hourly resolution; whether a swap was announced is unknowable, and there is no `PerpetualLockUpdated` history) | partial; 11 decides whether to use it |
| red flag `owner_control_change` (master §7.5) | ×0.85 | — | the same `rpc_owner_history` change rows give historical onsets of owner-coldkey changes for 13's survival and flag studies | partial |
| `active_authors_28d`, `fte_devs_84d`, `commit_days_90d`, `bus_factor` | P2 · .20/.15/.15/.10 | `logcap 10`/`logcap 8`/`logcap 90`/custom | `github_commits` (`backfilled=1`) | 2025-02-13 |
| `release_cadence_90d`, `review_practice_90d` | P2 · .05/.05 | `logcap 6`/identity | `github_events` release / pr_merged | 2025-02-13 (releases only; PRs if enabled) |
| `declared_repo_freshness`, `org_freshness` | P2 · .10/.10 | `recency(14,180)`/`recency(7,90)` | last commit ≤ t (approx, 13 §2.5) | 2025-02-13 |
- **`collector_runs.job` ids:** `import_old_ledger`, `import_fundamentals`, `backfill_rpc:probe`, `backfill_rpc:gap|ledger|pre_ledger`, `backfill_tmc`, `backfill_regime`, `backfill_github`, `build_coverage`, `replay_history`.
- **Events.** 03 §3.9 is the catalog, and every row goes through 03's `emit_event` (02 §D1):

| event_type | severity | `source` | scope | trigger | payload (03's required keys; extra keys allowed) | dedupe_key | alert |
|---|---|---|---|---|---|---|---|
| `netuid_reoccupied` (history rows) | `warning` | `registry` (02 §B9, §J1) | occ (new) | historical occupant transition found by §2.2 | 03's backfill key set `{old_key, new_key, old_name, new_name, reg_block, backfilled: true}` | `reoccupied:{old}->{new}` | none: `alerted_at` preset (digest route, 02 §D4) |
| `backfill_done` | `info` | `runtime` (§11 item 25) | sys | a step (script or manual job) ends with no unit pending | `{job, era, rows, seconds, coverage_path}`, optional `failed_units`, `warnings` (occupancy anomalies, failed coverage checks) | `{job}\|{run_id}` | digest |
| `backfill_failed` | `warning` | `runtime` | sys | abnormal end after work started: unhandled exception, decode stop (exit 4), probe/verify failure (exit 5), in-service job failure | `{job, era, error, resume_hint}` | `{job}\|{run_id}` | immediate |

  Field values:
  - `job` = the step id from the `collector_runs` list above (e.g. `backfill_rpc:gap`);
  - `era` = the RPC era, or `null` for other steps;
  - `coverage_path` = `COVERAGE_PATH`, or `null` before the first build;
  - `resume_hint` = the exact resuming command (e.g. `python -m scripts.backfill_rpc --era gap`).

  An in-service job emits `backfill_failed` and then re-raises, so 01's wrapper still counts the failure and fires `collector_failing` at the `JOB_FAIL_ALERT_THRESHOLD`-th consecutive failure.

  These cases emit neither event: precondition refusals (exit 2/3, printed only) and partial ends (pause, soft deadline, closed window, `paused: budget`, SIGINT); the partial ends show in `/health` and the sidecars. With `BACKFILL_ALERTS_ENABLED=false`, both events are still written, with `alerted_at` preset (stored, never sent). 17 never calls `send_alert`: 14's `alert_dispatch` routes the rows (02 §D3).
- **Files:** `data/backfill/{ledger_import.json, ledger_import_report.json, rpc_probe.json, tmc_request.json, tmc_progress.json, github_request.json, github_progress.json, fundamentals_import_report.json, pi_recovery_report.json, backfill.lock, tmp/}` and `data/quant/coverage.json`.

---

## 4. Absence-vs-failure table

A backfill writes **history**, not metrics, but the same rule applies. A value the source verifiably reported (including zero, or "this subnet did not exist") is written. A value that could not be obtained leaves **no row / NULL**, is visible in `backfill_buckets`, the sidecars and `coverage.json`, and becomes `None` downstream. Nothing is ever forward-filled, back-filled from a neighbour, or guessed from the current occupant.

| datum | verifiable absence → what is written | collection failure → what is written |
|---|---|---|
| `pool_history` row (netuid, bucket) | netuid not in `get_all_dynamic_info` at that block ⇒ no row. The subnet did not exist; coverage's *expected* set excludes it via occupancy | bucket `failed` (RPC error, decode error, block search) ⇒ no rows for any netuid; `backfill_buckets.status='failed'`; coverage `holes` |
| `price ≤ 0` or `tao_in ≤ 0` in a decoded entry | row skipped (old rule), counted `skipped_nonpositive` per netuid in the bucket report | — |
| `moving_price`/`subnet_volume` on seed-era ledger rows | — | stays NULL in `pool_history` unless a merge source has it at the **same block** (never interpolated). A `price_bars` bar may take `moving_price` from another `pool_history` sample **in the same bucket**, never from another bucket |
| `network_registered_at` / `occupant_key` | — | NULL until occupancy knows the interval; untrusted (pre-2026-05-30) ledger values are dropped, not stored |
| `price_bars` volume | real zero when the cumulative counter did not move (Δ = 0 ⇒ 0.0) | NULL across gaps > 2 periods, occupant changes or counter resets |
| `price_bars` 1h/1d from TMC 5m | — | bucket not written if < `BACKFILL_TMC_MIN_5M_PER_1H` (6) / `…_PER_1D` (144) 5m bars |
| TMC analytics for a netuid | HTTP 200 with an empty list (not yet trading) ⇒ `done`, 0 rows | HTTP error / timeout ⇒ `failed` in the sidecar, no rows; `BudgetExhausted` ⇒ the unit stays pending and the job ends `paused: budget` (02 §G1); unknown occupant ⇒ `partial` (`unattributed`) |
| `regime_daily` | classified label when ≥ `REGIME_MIN_SUBNETS` netuids have ≥ `REGIME_MIN_BUCKETS` buckets (12's frozen `RegimeParams`) | `label = 'UNKNOWN'` (02 §B8, 03/12 vocabulary; `v_w = 0.15`) when thin; **never `DEAD`** |
| GitHub commits for a repo | 200 with an empty page ⇒ `done`, 0 commits. 06's replay then yields `0.0` activity for a resolved scope | 403/5xx after the client's retries, 404/451 (repo gone or blocked) ⇒ `failed`/`gone` in the sidecar. **Replay contract:** 06 must return `None` for days older than its live window when any in-scope repo's backfill is not `done` (or is `truncated` before that day) |
| legacy identity values | junk values (`username/repo`, `www.deprecated.com`) are not imported (04 rules) | missing file ⇒ step skipped with `status: not_found` in the report |
| Pi ledger / wallet pub | not recovered ⇒ report `pi: not_recovered`; nothing written | unreadable/corrupt ⇒ exit 2; no partial import (per-netuid transactions) |
| Pi positions on chain | TMC 200 without a row for a netuid ⇒ `still_staked=false` | TMC failure ⇒ `still_staked=null` (`unknown`) |

---

## 5. Configuration

These are new keys on `Settings` (01), `UPPER_SNAKE`, with `.env.example` entries. 01's inventory contains every key of this table, grouped under 17, and this spec's defaults win for them (02 §F1). Reused keys are **not** redefined:
- `ONFINALITY_API_KEY`, `ONFINALITY_WS_URL`, `ONFINALITY_RATE_PER_SEC`, `TMC_*`, `SEED_DIR`, `DB_PATH`, `DB_BUSY_TIMEOUT_MS`, `DATA_DIR`, `HEALTH_HOST`, `HEALTH_PORT`, `API_AUTH_TOKEN`, `SPEC_VERSION`, `JOB_FAIL_ALERT_THRESHOLD`, `KILL_SWITCH_PATH`, `PAUSE_ALL_PATH` (01);
- `SUBTENSOR_ENDPOINT_COOLDOWN_SEC` (05);
- `LEGACY_FUNDAMENTALS_PATH`, `LEGACY_AS_OF` (04);
- `GITHUB_*` (06);
- `PRICE_BARS_5M_RETENTION_DAYS`, `DB_BACKUP_*` (03).
- Regime parameters are **not** config: 17 uses 12's frozen `REGIME_TS_1_0` (`RegimeParams`).

| key | type | default | validator / notes |
|---|---|---|---|
| `BACKFILL_ENABLED` | bool | `True` | master switch: `False` ⇒ jobs skip (`disabled`), scripts exit 3 |
| `BACKFILL_DIR` | path | `"data/backfill"` | sidecars, requests, reports, lock, `tmp/` |
| `BACKFILL_ALERTS_ENABLED` | bool | `True` | `false` ⇒ `backfill_done`/`backfill_failed` rows are written with `alerted_at` preset (stored, never sent; §3.4) |
| `BACKFILL_MIN_FREE_GB` | float | `3.0` | preflight on `DATA_DIR`; below ⇒ refuse (exit 2) |
| `LEDGER_SEED_PATH` | path | `"seed/ledger.db"` | checklist A3 |
| `LEDGER_BACKFILL_SEED_PATH` | path | `"seed/ledger_backfill.db"` | also probed at `seed/backups/ledger_backfill.db` |
| `LEDGER_PI_SEED_PATH` | path | `"seed/pi/ledger.db"` | optional (A4) |
| `LEDGER_EXTRA_SEED_PATHS` | list[path] | `[]` | e.g. `seed/p0_ledger.db`; INSERT OR IGNORE + NULL-merge |
| `LEDGER_NRA_TRUST_FROM_TS` | int | `1780182493` | first live-captured `network_registered_at` |
| `LEDGER_LIVE_FROM_TS` | int | `1777468595` | first row of the old bot's live 1-minute capture (2026-04-29T13:16:35Z, verified: first hour with ≥ 10 rows per netuid). Earlier ledger rows get `observed_at` = import time and `backfilled` bars (§2.1, §2.5) |
| `LEDGER_IMPORT_THIN_SEC` | int | `0` | `0` or `60..3600` |
| `LEDGER_IMPORT_BATCH_ROWS_ONLINE` | int | `20000` | `--online` only |
| `LEDGER_EXPECTED_ROWS` / `LEDGER_EXPECTED_NETUIDS` / `LEDGER_BACKFILL_EXPECTED_MERGE` | int | `7362854` / `129` / `23237` | verification only; `0` disables the check |
| `WHALE_COHORT_SEED_PATH` / `WHALE_LABELS_SEED_PATH` | path | `"seed/whale_cohort.json"` / `"seed/whale_labels.json"` | |
| `ARCHIVE_WS_URL` | str | `"wss://archive.chain.opentensor.ai:443"` | |
| `ARCHIVE_RATE_PER_SEC` | float | `2.0` | `0.1..3.0` (limiter provider `opentensor_archive`, burst 4) |
| `ARCHIVE_MIN_BUCKET_INTERVAL_SEC` | float | `1.5` | `≥ 0.5` (the old `--sleep 0.25` is too fast, research §2) |
| `ARCHIVE_BACKOFF_MAX_SEC` | int | `120` | |
| `BACKFILL_RPC_ENDPOINTS` | list[str] | `[]` ⇒ derived: keyed OnFinality (if key), `ONFINALITY_WS_URL`, `ARCHIVE_WS_URL` | validator rejects `lite.`/`entrypoint-finney` hosts (state discarded) |
| `BACKFILL_RPC_ERAS` | list[str] | `["gap","ledger","pre_ledger"]` | subset of `{gap, ledger, pre_ledger}`; `pre_ledger` is on by default (master §13 item 9, checklist A5) |
| `BACKFILL_RPC_STEP_SEC` | int | `3600` | ∈ `{900, 1800, 3600, 14400, 86400}` |
| `BACKFILL_RPC_SAMPLE_OFFSET_SEC` | int | `60` | `0..600` |
| `BACKFILL_RPC_MIN_NETUIDS_PER_HOUR` | int | `100` | `ledger` era: hours below this, or hours with no non-NULL `moving_price`, are fetched |
| `BACKFILL_RPC_PUBLIC_MAX_RPS` | float | `4.0` | `0.5..5.0`; per-endpoint sub-bucket for the public OnFinality URL even when a key is set (public limit 5 RPS, `-32029`) |
| `BACKFILL_RPC_CONCURRENCY` | int | `0` (auto: 4 keyed OnFinality, 1 otherwise) | `0..8` |
| `BACKFILL_RPC_MAX_ATTEMPTS` | int | `6` | per bucket |
| `BACKFILL_RPC_TIMEOUT_SEC` | float | `45` | per call (old `CHAIN_READ_TIMEOUT_SEC`) |
| `BACKFILL_RPC_PRICE_METHOD` | enum | `"auto"` | `auto`, `swap_sqrt`, `reserve_ratio` |
| `BACKFILL_RPC_MODE` | enum | `"runtime_api"` | `runtime_api`, `storage` (storage: OnFinality only — validator) |
| `DTAO_LAUNCH_BLOCK` / `LEDGER_START_BLOCK` | int | `4920351` / `6502594` | era bounds; the ledger end is read from data |
| `BACKFILL_TMC_ENABLED` | bool | `True` | job `backfill_tmc` |
| `BACKFILL_TMC_TIMEOUT_SEC` | int | `14400` | |
| `BACKFILL_TMC_MAX_PER_HOUR` | int | `180` (02 §F2) | `1..TMC_HOURLY_CAP − 320` (05 §6.3: 11's ≤ 300/h + 05/04's ≤ 20/h) |
| `BACKFILL_TMC_QUIET_UTC` | str | `"02:05-02:45,20:25-23:35"` (02 §F2) | comma list of UTC `HH:MM-HH:MM` ranges, or `""` |
| `BACKFILL_TMC_CANDLE_TIMEOUT_SEC` | int | `90` | |
| `BACKFILL_TMC_CANDLE_CHUNK_DAYS` | int | `30` | fallback window |
| `BACKFILL_TMC_MIN_5M_PER_1H` / `BACKFILL_TMC_MIN_5M_PER_1D` | int | `6` / `144` | `1..12` / `1..288` |
| `PI_TRADER_COLDKEY` | str | `""` | SS58 regex `^5[1-9A-HJ-NP-Za-km-z]{47}$`; public key only |
| `BACKFILL_GITHUB_ENABLED` | bool | `True` | job `backfill_github` (Phase 4+) |
| `BACKFILL_GITHUB_SINCE` | str | `"2025-02-13"` | ISO date (dTAO launch) |
| `BACKFILL_GITHUB_MAX_PAGES_PER_REPO` | int | `50` | `1..200` |
| `BACKFILL_GITHUB_MAX_REPOS_PER_OCCUPANT` | int | `30` | `1..100` |
| `BACKFILL_GITHUB_PRS_ENABLED` | bool | `True` | |
| `BACKFILL_GITHUB_PR_MAX_PER_REPO` | int | `200` | |
| `BACKFILL_GITHUB_RESERVE_REMAINING` | int | `1500` | must be `> GITHUB_RESERVE_REMAINING` |
| `BACKFILL_GITHUB_TIMEOUT_SEC` | int | `14400` | |
| `BACKFILL_GITHUB_WINDOW_UTC` | str | `"00:30-08:00"` | one UTC `HH:MM-HH:MM` range (may wrap midnight) or `""` (any time; the reserve gate still applies); the overnight window of 02 §G4 (§2.8) |
| `REGIME_BACKFILL_START` | str | `""` (auto) | ISO date |
| `COVERAGE_PATH` | path | `"data/quant/coverage.json"` | |

**Pause and kill semantics (02 §B12).**
- `BACKFILL_ENABLED=false` disables everything in this spec.
- **In-service jobs.** The operator pause is a `control_state` row for the exact id `backfill_tmc`/`backfill_github` or for 16's group target `backfill` (both jobs; never prefix matching). `POST /api/v1/control/pause/{id}`, Telegram and `python -m app ops pause` write it; it is the only persistent pause store. `data/control/<job>.paused` and `data/control/backfill.paused` are read-only emergency inputs that the app never writes, and `./PAUSE_ALL` pauses both jobs. A pause found mid-run stops the job at the next unit boundary with the sidecar saved and `JobOutcome(note="paused:<reason>")`, using B12's reason vocabulary. A resume deletes only the row, and the next trigger continues from the sidecar.
- **Standalone scripts.** SIGINT/SIGTERM lets in-flight buckets finish, flushes the sidecar and exits 130. `./PAUSE_ALL` (16's `pause_all_active()`) is checked at every unit boundary and ends the step with exit 0, state saved.
- There is no backfill-specific sentinel. The former `./BACKFILL_PAUSED` / `BACKFILL_PAUSE_PATH` is retired because the sentinel list is closed (02 §B12).
- `KILL_SWITCH` does **not** pause backfills, because they are free.
- All path keys resolve against `APP_ROOT` via `settings.path()` (01).

---

## 6. Cadence, rate limits, cost

Nothing here is scheduled. All steps are one-off and operator-triggered, and re-run only after new seed data (Pi) or a failure. The only jobs are the two `manual` jobs in §8.1's table (02 §E1); 01's `JOB_TABLE` mirrors that table (02 §E2). After Phase 3, forward capture is 05's job. 05's `analytics?span=1W` repair and weekly 1Y/candle pulls keep `chain_daily`/`price_bars` continuous.

| provider | limit honoured | calls | wall time |
|---|---|---|---|
| TMC (01's shared `tmc` limiter, in-service) | ≤ 8/min, job ceiling 180/h, quiet windows 02:05–02:45 and 20:25–23:35Z, 500/h and 90 k/month caps (02 §F2, §G3) | S3: 256 (analytics 1Y + 1M); S6: 128 candles + 0 restamp + 2 positions; ≤ +10 % retries. Total 386 + 10 %, once = 05 §6.3's row | S3 ≈ 70 min (180 in ≈ 23 min, then the ceiling waits for the hour to slide), S6 ≈ 17 min; plus any quiet-window sleep |
| OnFinality keyed (`onfinality`, 20/s, concurrency 4) | 40 rps plan | 5 metered calls/bucket (`chain_getBlockHash`, `Timestamp.Now` storage, `state_getRuntimeVersion`, raw `state_call`, `state_queryStorageAt` for the swap price) + 2 per block-search correction + `RUNTIME_LOAD_TOKENS` (6) per new runtime version + ≈ 500 for occupancy `registered_ts` | latency-bound (runtime API 0.34–1.1 s): ≈ 2.5–3 buckets/s ⇒ gap (≈ 2,300) ≈ 13–16 min; ledger (≈ 5,977) ≈ 35–40 min; pre_ledger (5,274) ≈ 30–35 min |
| OnFinality public (`onfinality`, ≤ 4/s, concurrency 1) | 5 RPS, `-32029` | same | ≈ 0.8 buckets/s at 5 calls ⇒ gap ≈ 48 min; ledger ≈ 2.1 h; pre_ledger ≈ 1.8 h. Research's "≈ 25–30 min for the gap" was measured unpaced at 3 calls/bucket; it does not include the runtime-version call or the limiter |
| opentensor archive (`opentensor_archive`, 2/s + ≥ 1.5 s/bucket) | `-32004` budget | reserve-ratio price (no per-key reads) ⇒ 4 calls/bucket (`chain_getBlockHash` is not counted by the node's budget but is by our bucket) | ≈ 0.4–0.5 buckets/s ⇒ gap ≈ 77–96 min; ledger ≈ 3.3–4.2 h; pre_ledger ≈ 2.9–3.7 h |
| GitHub PAT (06 client, own reserve 1,500, window 00:30–08:00Z; 02 §G4) | 5,000/h; 01's `github` limiter `GITHUB_RATE_PER_HOUR` 4,000/h; ≥ 1,500/h always left for live 06 jobs | ≈ 1,000–1,500 repos × (≈ 3 commit pages + 1 releases + 1–3 PR pages) ≈ 5–6 k calls, + 1 review call per merged PR (≤ 200 per repo; ≈ 20–40 k in total, UNVERIFIED: depends on PR volume) | effective ≈ 3,000/h after 06's live use and the 1,500 reserve ⇒ ≈ 2 h without reviews (one night); ≈ 6–13 h with reviews, i.e. 1–2 nights of the 7.5-h window (the job is resumable; `--no-prs` skips PR listing and reviews) |
| none (local) | — | ledger import ≈ 7.4 M rows; bars ≈ 175 k (ledger) + ≈ 1.1 M (RPC/TMC) bar rows; regime ≈ 366–585 days | S1 10–20 min; S5 5–15 min; S7 < 1 min |

**Cost:** $0 for all providers (OnFinality free key, TMC keyless, GitHub PAT). TMC use is ≈ 425 requests once (386 + 10 %), against `TMC_MONTHLY_CAP` 90,000 (the IP limit is 100,000).

**Disk** (estimates at ≈ 115 B/row incl. index for the old `pool_history` columns plus ≈ 25–40 B for 03's provenance columns and the partial `occupant_key` index; ≈ 95–110 B for bars):

| item | default | with `LEDGER_IMPORT_THIN_SEC=300` | notes |
|---|---|---|---|
| `pool_history` ledger (7.36 M rows + indexes) | ≈ 1.0–1.15 GB | ≈ 0.3–0.35 GB | |
| `pool_history` RPC gap + ledger-fill (≈ 1.06 M rows: 2,300 + 5,977 buckets × ≈ 128 netuids) | ≈ 0.15 GB | same | + ≈ 0.08 GB with `pre_ledger` (5,274 buckets × 65–128 netuids) |
| Pi extra rows (≈ 1.9 M, if recovered) | ≈ 0.25 GB | ≈ 0.07 GB | |
| `price_bars` 5m gap (≈ 3.5 M) / 1h (≈ 1.2 M) / 1d | ≈ 0.33 / 0.13 / 0.01 GB | same | |
| `chain_daily`, `chain_hourly`, `chain_rest_snapshots` payloads | ≈ 0.12 GB | same | raw JSON |
| GitHub commits + authors_daily | ≈ 0.14 GB | same | Phase 4+ |
| **total added** | **≈ 1.8–2.4 GB** | **≈ 1.1–1.4 GB** | the brief's ≈ 1.5 GB is at the low end |
| transient | WAL up to ≈ 0.5 GB (checkpoint `TRUNCATE` after each phase); Pi checkpointed copy ≈ source size (deleted after import) | | |
| backups (03/16) | each gzip copy grows by ≈ 40–50 % of the added history (≈ 0.8–1.2 GB); 03's `DB_BACKUP_MAX_TOTAL_GB` = 12 caps the local backup directory (the 2 newest copies are never pruned), so the added history reduces how many old copies are kept rather than overflowing the disk | | 5m `tmc_candle` gap rows are deleted by 03's 90-day retention |

The CPU work is SCALE decode (≈ 50–150 ms per bucket, run in `asyncio.to_thread`) and SQLite window queries. RSS stays ≤ 400 MB for standalone scripts. The in-service jobs add ≤ 50 MB to the service.

---

## 7. Failure modes and fail-safe behaviour

**Invariant:** no backfill can alter a live-observed value. Every write is an `INSERT OR IGNORE`, an `UPDATE … WHERE <col> IS NULL`, or a `src_rank`-guarded upsert that can only replace a lower-rank *derived* bar. `sse`/`rest_table` rows of 05, trusted ledger values and 04's current-occupant status are never modified. For `pool_history` the rule is enforced in the database by 03's `trg_pool_history_update` and `trg_pool_history_no_delete`. `observed_at` is never set earlier than the time the value actually became known (§2.1, §2.5).

| failure | detection | behaviour | resume |
|---|---|---|---|
| seed file missing | path check | step exits 2 with the exact path; report `not_found`; coverage `eras.*.status='not_run'` | copy file, re-run |
| seed DB corrupt | `PRAGMA quick_check` ≠ `ok` | exit 2 before any write | re-copy (checklist A3/A4) |
| Pi copy has a non-empty `-wal` | file stat | private checkpointed copy via `sqlite3.backup`, attach the copy | automatic |
| service running during S1 | `/health` answers | refuse (exit 2) unless `--online` (small batches) | stop service or `--online` |
| low disk | `shutil.disk_usage(DATA_DIR)` < `BACKFILL_MIN_FREE_GB` | refuse (exit 2) | free space / `LEDGER_IMPORT_THIN_SEC` |
| process killed mid-step | — | per-netuid / per-bucket transactions roll back; sidecar untouched for that unit | re-run (idempotent) |
| OnFinality `-32029` / archive `-32004` / 429 | JSON-RPC error code / message | backoff `min(5·2^n, 120 s)` ≤ 6 tries ⇒ bucket `failed`; 3 consecutive failed buckets ⇒ endpoint `mark_bad` + failover | next run retries `failed` only |
| dead socket / timeout (> `BACKFILL_RPC_TIMEOUT_SEC`) | `is_dead_socket_error`, `asyncio.wait_for` | reconnect (bounded close, old `chain_client.py:197-222` pattern); counts as an attempt | automatic |
| ASI cannot decode a runtime | exception in `decode_scale` / runtime load | bucket `failed: decode:<specVersion>`; ≥ 10 consecutive decode failures on one `specVersion` ⇒ stop the era, exit 4, advice `--mode storage` (OnFinality) | re-run with `--mode storage` for that block range |
| schema lacks 17's columns/tables | `common.open_db` check (§2.1 step 1) | exit 3 listing the missing names; nothing written | apply 03's current schema (Phase 0) |
| a write would change a non-NULL `pool_history` value | 03 trigger `RAISE(ABORT, 'pool_history: only NULL-only enrichment is allowed')` | that unit's transaction rolls back; counted `trigger_aborts`; the step continues with the next unit and exits 5 at the end | bug: fix the writer (never drop the trigger) |
| ASI makes an un-metered internal call | `--probe` counts socket requests per bucket (> 5 ⇒ fail) | era refused until fixed | fix `RpcGateway` token accounting |
| `get_all_dynamic_info` returns `None` entries | decoded value | entry skipped, counted `none_entries` (not a failure) | — |
| TMC compact candle row with unexpected arity | parser | netuid `failed: parse_error`, raw kept in the log (first 3 rows) | fix parser; `--redo` |
| probe golden mismatch | `rpc_probe.json` | eras refused (exit 5), mismatching fields printed (typical: frac bits, rao/τ, price method) | fix conversion; re-probe |
| no block inside an hour (chain stall) | block at `b` has `ts < H` and `b+1` has `ts ≥ H+3600` | bucket `skipped: no_block_in_hour` (verifiable absence) | — |
| block search diverges | > 3 corrections | bucket `failed: block_search` | re-run (anchors improve) |
| occupancy anomaly (reg reappears / `reg > first obs block`) | §2.2 checks | that netuid is not written or stamped; `coverage.occupancy.anomalies`; listed in the step's `backfill_done` payload `warnings` (digest, §3.4) | manual review; `--occupancy-override netuid=reg@block` (report-only flag) |
| trusted ledger key ≠ interval key | stamp cross-check | keep the ledger value; count `occupant_key_mismatch_rows`; the coverage check fails and is listed in `build_coverage`'s `backfill_done` payload `warnings` | investigate |
| TMC 429 / `X-RateLimit-Remaining: 0` | 05 client | client waits (`Retry-After`/reset); job continues | automatic |
| TMC hourly or monthly cap | `BudgetExhausted` | the job ends `paused: budget` (02 §G1), not as a failure; the sidecar keeps finished netuids; no event | next hour / next month / raised cap, then re-trigger |
| TMC 5m payload timeout | httpx timeout | chunked retry (30-day windows) | automatic |
| candle label inconclusive | median error ≥ 0.5 % both ways | default `start`; caveat `tmc_candle_label:unverified` | manual check |
| analytics rows with unknown occupant | `OccupancyIndex` returns None | not written; `partial`; raw kept | `--tasks restamp` after RPC eras |
| GitHub 403 rate / secondary limit | 06 client | sleeps to reset (06 rules) | automatic |
| GitHub 404/451/409 (gone/blocked/empty) | status | repo `gone`/`blocked`/`done(0)` in the sidecar | — |
| regime classifier module missing | `ImportError` on `app.quant.regime` | exit 3 "requires spec 12 port"; coverage `regime_daily.days = 0` | re-run after Phase 8 |
| 05 helpers missing (Phase 2 incomplete) | 01 `JobRegistry` import | `backfill_tmc` `available=false` | finish Phase 2 |
| service crash during an in-service job | orphan `collector_runs` row (01) | sidecar consistent to the last finished netuid/repo | re-trigger |
| coverage build error | exception | exit 1; the previous `coverage.json` is untouched (atomic replace) | fix, re-run |
| wallet-file guard hit | basename `coldkey` or under `hotkeys/` | refuse, exit 2 | — |

`/health` (01 shape) shows `collectors.backfill_tmc` and `collectors.backfill_github` with the standard fields (`last_run, last_ok, last_error, consecutive_failures, rows, paused`) plus `progress: {<task>: {done, total, failed}}`, read from the sidecar by the job's `status()` override (01 §2.6). Standalone scripts are visible through their `collector_runs` rows and `data/backfill/*.json`.

---

## 8. Implementation plan

### 8.1 Files

```
app/backfill/__init__.py
app/backfill/common.py            BackfillLock, service_is_up, stop_requested, Sidecar, step_run, emit_step_event, src_rank, open_db
app/backfill/ledger_import.py     SourceSpec, SourceReport, discover_sources, prepare_source, import_source, merge_nulls, verify, run
app/backfill/occupancy.py         Interval, OccupancyIndex, collect_observations, build_intervals, resolve_registered_ts, write_occupants, emit_history_events, stamp_all
app/backfill/bars.py              BAR_SQL, rebuild, upsert_bars, enrich_reserves, register_sql_functions
app/backfill/rpc_client.py        Endpoint, RpcGateway, RpcError/RateLimited/DecodeError/NoRuntimeApi, twox128, storage_key, TIMESTAMP_NOW_KEY, decoders
app/backfill/rpc_backfill.py      Bucket, BucketResult, plan_buckets, BlockSampler, locate_block, convert_entry, prices_swap_sqrt, process_bucket, write_bucket, run_era, probe
app/backfill/tmc_backfill.py      BackfillTmcJob, HourCeiling, task_analytics, task_restamp, task_candles5m, task_positions, parse_compact_candles, detect_label, aggregate
app/backfill/github_backfill.py   BackfillGithubJob, plan_repos, backfill_repo, rebuild_rollups
app/backfill/fundamentals_import.py  run_import, import_whale_cohort
app/backfill/regime_backfill.py   snapshots_for_day, backfill_regime
app/backfill/coverage.py          build, write, SCHEMA_ID = "coverage-1"
app/backfill/pi_recovery.py       PC_OPEN_POSITIONS, safe_open, read_coldkey_ss58, ledger_summary, positions_summary, parse_coldkey_stakes, build_report
scripts/import_old_ledger.py  import_fundamentals.py  backfill_rpc.py  backfill_tmc.py  backfill_github.py
scripts/backfill_regime.py  build_coverage.py  pi_recovery_report.py  replay_history.py  backfill_all.py
tests/backfill/…              (§9)
```

- **Packaging:** `pyproject.toml` gets an optional extra `backfill = ["async-substrate-interface>=2.2,<3", "xxhash>=3"]`. Verified from the old venv's `async_substrate_interface-2.2.1.dist-info/METADATA`: runtime requirements are `aiosqlite>=0.21,<1.0`, `cyscale>=0.3.3,<1.0`, `websockets>=14.1`, `xxhash`, and `bittensor-wallet` is listed **only under the `dev` extra**. Do **not** add `scalecodec`: `cyscale` 0.5.0 installs the top-level `scalecodec` package itself, and a second distribution would overwrite it. Code imports `scalecodec.utils.ss58.ss58_encode` from there. ASI 1.x is excluded because its dependency set is UNVERIFIED and early 1.x releases are believed to have required `bittensor-wallet`. It is imported lazily inside `rpc_client.py`, so the core service never needs it. **No `bittensor`, no `bittensor-wallet`, no `substrate-interface`** (01 §2.3): `tests/test_deps.py` additionally runs after `pip install -e '.[backfill]'` and asserts `find_spec("bittensor_wallet") is None`. The bar and merge SQL needs SQLite ≥ 3.33 (`UPDATE … FROM`) and ≥ 3.25 (window functions). 03 already asserts ≥ 3.37 (STRICT tables) at connect, and Ubuntu 22.04 ships 3.37.
- **Job table** (the only job table of this spec; 01's `JOB_TABLE` mirrors it verbatim, 02 §E2; 16's `PAUSE_GROUPS` group `backfill` = both rows):

| id | factory (`app.…`) | trigger | misfire grace | enabled flag | phase | provider | paid | timeout s |
|---|---|---|---|---|---|---|---|---|
| `backfill_tmc` | `backfill.tmc_backfill:BackfillTmcJob` | `manual` | — | `BACKFILL_TMC_ENABLED` | 3 | `tmc` | no | 14400 |
| `backfill_github` | `backfill.github_backfill:BackfillGithubJob` | `manual` | — | `BACKFILL_GITHUB_ENABLED` | 4 | `github` | no | 14400 |

- **Manual trigger:** 01's `manual` kind (02 §E1): `IntervalTrigger(weeks=520)` with `next_run_time=None`, exempt from the first-run stagger and from liveness (02 §E4); runs only via `POST /api/v1/control/run/{id}` or `python -m app run-job {id}`. 17 sets no liveness override.
- **Events:** standalone scripts keep the 03 `Database` they opened for `apply_schema()` and write every event through 03's `emit_event` (`occupancy.emit_history_events`, `common.emit_step_event`; sync steps call it via `asyncio.run` after their sync work). In-service jobs use `run.db`.
- **Database access:** standalone scripts use synchronous `sqlite3` via `common.open_db(path)`, which sets `journal_mode=WAL`, `busy_timeout=DB_BUSY_TIMEOUT_MS`, `foreign_keys=ON` and registers `src_rank` from 03's `app.storage.precedence.rank_any` (never 17's own numbers). It applies the schema by calling 03's `Database.apply_schema()` (= 03's `migrate()`) once (async, before the sync work) and then runs the §2.1 step 1 column check. In-service jobs call the same sync functions (`bars`, `occupancy`) through `await asyncio.to_thread(fn, db_path, …)` on a private connection, so each unit's write transaction is short (≤ 1 s) and the service's own writes interleave through `busy_timeout`.

### 8.2 Key signatures

```python
# common.py
class BackfillBusy(RuntimeError): ...
class BackfillLock:                                   # fcntl.flock(LOCK_EX | LOCK_NB) on BACKFILL_DIR/backfill.lock
    def __init__(self, path: Path) -> None
    def __enter__(self) -> "BackfillLock"; def __exit__(self, *exc) -> None
def service_is_up(timeout: float = 2.0) -> bool       # httpx GET http://HEALTH_HOST:HEALTH_PORT/health → 200
def stop_requested() -> bool                           # standalone: SIGINT/SIGTERM flag or 16's app.control.pause_all_active()
                                                       # (in-service jobs call their own paused_reason() instead, 02 §B12)
class Sidecar:
    def __init__(self, path: Path) -> None
    def load(self) -> dict; def save(self) -> None     # write_atomic (04 app/utils/fs.py)
    def unit(self, task: str, key: str) -> dict; def mark(self, task: str, key: str, **fields) -> None
@contextmanager
def step_run(con: sqlite3.Connection, job: str) -> Iterator["StepRun"]   # collector_runs start/finish (ok, rows, error, duration_sec, note);
                                                       # run_id = f"script:{job}:{started_at}" (03 §2.1)
async def emit_step_event(db: "Database", kind: str, *, job: str, era: str | None, run_id: str, **payload) -> int | None
    # kind ∈ {'backfill_done','backfill_failed'}; 03 emit_event(source='runtime', occupant_key=None, netuid=None,
    #   dedupe_key=f"{job}|{run_id}", alerted_at=now if not BACKFILL_ALERTS_ENABLED else None)
def src_rank(source: str | None) -> int                # thin wrapper over 03's precedence.rank_any('price_bars' ranks:
                                                       #   sse_rollup 5 > ledger_import 4 > tmc_candle 3 > onfinality = opentensor_archive 2 > taostats 1)
# time conversions: 01's app.util.time (parse_ts, parse_iso, to_iso, now_ts; 02 §B1) — no 17-local helpers
def open_db(path: Path) -> sqlite3.Connection

# ledger_import.py
@dataclass(frozen=True)
class SourceSpec: path: Path; source: str; merge_only: bool = False     # source ∈ ledger_pc|ledger_pi|ledger_extra|ledger_backfill
def discover_sources(settings) -> list[SourceSpec]
def prepare_source(spec: SourceSpec, tmp_dir: Path) -> Path             # quick_check; WAL → checkpointed copy
def import_source(con, spec: SourceSpec, *, trust_ts: int, live_from_ts: int, run_started: int, thin_sec: int,
                  sidecar: Sidecar, pause_s: float) -> SourceReport
def merge_nulls(con, spec: SourceSpec, *, trust_ts: int, now: int) -> tuple[int, int, int]   # (mp, sv, nra) rows changed
def verify(con, settings) -> dict                                       # §10 queries → {name: {expected, actual, pass}}
def run(phases: set[str], *, online: bool, extra: list[Path]) -> int    # exit code 0|2|3|5

# occupancy.py
@dataclass(frozen=True)
class Interval:
    netuid: int; reg: int; occupant_key: str; start_block: int; end_block: int | None
    first_seen_block: int; last_seen_block: int; registered_ts: int | None; end_ts: int | None
class OccupancyIndex:
    @classmethod
    def load(cls, con) -> "OccupancyIndex"                   # from occupants (registered_ts/deregistered_ts) + reg blocks
    def occupant_at_block(self, netuid: int, block: int) -> str | None
    def occupant_at_ts(self, netuid: int, ts: int) -> str | None
    def intervals(self, netuid: int) -> list[Interval]
def collect_observations(con) -> dict[int, list[tuple[int, int, int]]]
    # SELECT netuid, network_registered_at, MIN(block_number), MAX(block_number) FROM pool_history
    #   WHERE network_registered_at IS NOT NULL GROUP BY 1, 2   ∪ occupants(network_registered_at, first/last_seen_block)
def build_intervals(obs) -> tuple[dict[int, list[Interval]], list[dict]]        # pure; (intervals, anomalies)
async def resolve_registered_ts(rpc: "RpcGateway", con, regs: set[int]) -> dict[int, int]   # block_times cache first
def write_occupants(con, intervals, *, current: dict[int, str], observed_at: int, run_id: str) -> dict
async def emit_history_events(db: "Database", *, observed_at: int, run_id: str) -> int
    # netuid_reoccupied per 17-written transition via 03 emit_event(source='registry', payload.backfilled=True,
    #   alerted_at=observed_at); idempotent through dedupe_key 'reoccupied:{old}->{new}'
def stamp_all(con, index: OccupancyIndex) -> dict        # {pool_history_stamped, price_bars_stamped, mismatches, unstamped}

# bars.py
def register_sql_functions(con) -> None                  # src_rank
def rebuild(con, *, periods=("1h", "1d"), from_ts: int, to_ts: int, netuids: Iterable[int] | None = None,
            live_from_ts: int, now: int, run_id: str) -> int              # BAR_SQL + upsert_bars + enrich_reserves
def upsert_bars(con, rows: list[dict], period: str) -> dict               # {written, same_rank_skipped, lower_rank_skipped}
def enrich_reserves(con, netuid: int, period: str, from_ts: int, to_ts: int, *, now: int) -> int

# rpc_client.py
@dataclass(frozen=True)
class Endpoint: url: str; provider: str; kind: str       # kind ∈ onfinality|archive
class RpcGateway:
    def __init__(self, endpoints: list[Endpoint], limiter: "ProviderLimiter", *, timeout: float,
                 max_attempts: int, backoff_max: int, cooldown_sec: int) -> None
    async def connect(self) -> None; async def close(self) -> None
    @property
    def endpoint(self) -> Endpoint
    async def head(self) -> tuple[int, str]
    async def block_hash(self, block: int) -> str
    async def timestamp_ms(self, block_hash: str) -> int                  # TIMESTAMP_NOW_KEY, u64 LE
    async def storage(self, key: str, block_hash: str) -> str | None
    async def keys_paged(self, prefix: str, block_hash: str, count: int = 256) -> list[str]
    async def query_storage_at(self, keys: list[str], block_hash: str) -> dict[str, str | None]
    async def runtime_version(self, block_hash: str) -> int              # state_getRuntimeVersion → specVersion (1 token)
    async def dynamic_info(self, block_hash: str, spec_version: int) -> list[dict | None]
        # raw state_call (1 token) + ASI decode_scale with the cached Runtime for spec_version
        # (miss: acquire RUNTIME_LOAD_TOKENS, then substrate.get_runtime_for_version); raises NoRuntimeApi / DecodeError
    requests_sent: int                                                   # socket-level counter (probe: requests per bucket)
def twox128(name: str) -> bytes                          # xxh64(seed 0) ‖ xxh64(seed 1), LE (same keys as 05's app/collectors/chain_rpc.py)
def storage_key(pallet: str, item: str, netuid: int | None = None) -> str   # Identity hasher: + u16 LE
def fixed_to_float(bits: int, frac_bits: int) -> float

# rpc_backfill.py
@dataclass(frozen=True)
class Bucket: era: str; bucket_ts: int
def plan_buckets(con, era: str, *, step: int, now: int, min_netuids: int) -> list[Bucket]
    # excludes done/skipped; ledger era = hours with < min_netuids netuids OR no non-NULL moving_price
class BlockSampler:
    def __init__(self, anchors: list[tuple[int, int]]) -> None           # (block, unix ts), sorted
    def predict(self, target_ts: int) -> int
    def add(self, block: int, ts: int) -> None
async def locate_block(rpc, sampler, bucket_ts: int, *, step: int, offset: int) -> tuple[int, str, int] | None
    # (block, hash, ts); None = no_block_in_hour; raises BlockSearchFailed
def convert_entry(entry: dict, *, ts: int, observed_at: int, source: str, price: float | None, price_method: str) -> dict | None
async def prices_swap_sqrt(rpc, block_hash: str, spec_version: int, key_cache: dict[int, list[str]]) -> dict[int, float] | None
async def process_bucket(rpc, sampler, bucket: Bucket, *, settings, owner_state: dict[str, tuple]) -> "BucketResult"
def write_bucket(con, bucket: Bucket, result: "BucketResult", *, run_id: str) -> None   # one transaction
async def probe(settings) -> dict                       # §2.3 checks → rpc_probe.json
async def run_era(era: str, *, settings, step: int | None = None, force_unprobed: bool = False) -> int

# tmc_backfill.py
class BackfillTmcJob(CollectorJob):
    id = "backfill_tmc"; name = "TMC history backfill"; phase = 3
    enabled_flag = "BACKFILL_TMC_ENABLED"; interval_flag = None; cron = None
    provider = "tmc"; paid = False; timeout_sec = 14400; min_gap_factor = 0.0
    # trigger 'manual' comes from the §8.1 job table (02 §E1); no liveness override
    async def collect(self, run: RunContext) -> JobOutcome
class HourCeiling:
    def __init__(self, max_per_hour: int, quiet_utc: str, clock=time.time) -> None
    async def acquire(self) -> None
def parse_compact_candles(payload: Any) -> list["Candle"]   # [ts, block, o, h, l, c, v, start_volume, tao_usd(, tao_eur)]
                                                             # 9 or 10 elements; else ParseError
def detect_label(candles: list["Candle"], ledger_px: list[tuple[int, float]]) -> tuple[str, float]   # ('start'|'end', median rel err)
def aggregate(candles: list["Candle"], period: int, min_count: int) -> list[dict]
async def task_analytics(run, client, netuid: int, span: str, occ, sidecar) -> dict
async def task_restamp(run, netuid: int, occ, sidecar) -> dict
async def task_candles5m(run, client, netuid: int, gap: tuple[int, int], label: str, occ, sidecar) -> dict
async def task_positions(run, client, coldkey: str) -> dict

# github_backfill.py
class BackfillGithubJob(CollectorJob): id = "backfill_github"; phase = 4; provider = "github"; …  (as above)
def plan_repos(con, *, since: str, max_per_occupant: int) -> list["RepoPlan"]
async def backfill_repo(run, gh: "GitHubClient", quota: "GitHubQuota", plan: "RepoPlan", *, since: str, sidecar) -> dict
async def rebuild_rollups(run, *, since_day_ts: int, live_first_day_ts: int | None) -> dict   # INTEGER day_ts (02 §B1)
def in_window(now: int, window_utc: str) -> bool        # BACKFILL_GITHUB_WINDOW_UTC (02 §G4)

# fundamentals_import.py / regime_backfill.py / coverage.py / pi_recovery.py
async def run_import(settings) -> int                   # builds 03 Database, calls app.registry.legacy.import_fundamentals
def import_whale_cohort(con, cohort_path: Path, labels_path: Path | None, *, run_id: str) -> dict
def snapshots_for_day(con, day_ts: int) -> dict[int, list[dict]]   # price_bars 1h, ts ∈ (day_ts − 86400, day_ts]
def backfill_regime(con, *, start_day: int | None, end_day: int | None, params: "RegimeParams", spec_version: str,
                    run_id: str) -> dict                             # writes 03 regime_daily rows (source='backfill')
def build(db_path: str, *, settings) -> dict; def write(doc: dict, path: Path) -> None
PC_OPEN_POSITIONS = {416: 99, 417: 47, 419: 31, 421: 122, 424: 108, 425: 94, 426: 116, 429: 113, 431: 67, 432: 109}
def safe_open(path: Path, mode: str = "r")              # raises WalletGuardError for basename 'coldkey' or any 'hotkeys' parent
def read_coldkey_ss58(path: Path) -> str | None
def ledger_summary(pi_db: Path, *, pc_max_ts: int, trust_ts: int) -> dict
def positions_summary(pi_db: Path, ids: Iterable[int]) -> list[dict]
def parse_coldkey_stakes(payload: Any) -> list[dict]
def build_report(settings, *, coldkey: str | None, tmc_positions: dict | None) -> dict
```

**Script CLIs** (all `python -m scripts.<name>`; exit 0 ok, 2 precondition, 3 disabled or dependency missing, 4 decode stop, 5 probe/verify failed, 130 interrupted):
```
import_old_ledger  [--phase import,stamp,bars,verify|all] [--online] [--extra PATH ...] [--thin-sec N]
import_fundamentals [--fundamentals PATH] [--whale-cohort PATH] [--force]
backfill_rpc       --probe | --era gap|ledger|pre_ledger [--era …] [--step SEC] [--from-block N --to-block N]
                   [--endpoint URL] [--mode runtime_api|storage] [--price-method auto|swap_sqrt|reserve_ratio] [--force-unprobed]
backfill_tmc       [--tasks analytics1y,analytics1m,restamp,candles5m,positions] [--netuids 1,2,…] [--coldkey SS58]
                   [--redo] [--standalone] [--no-wait]
backfill_github    [--netuids …] [--no-prs] [--redo] [--standalone] [--no-wait]
backfill_regime    [--from YYYY-MM-DD] [--to YYYY-MM-DD]
build_coverage     [--db PATH] [--out PATH]
pi_recovery_report [--coldkey SS58] [--no-chain-check]
replay_history     --from YYYY-MM-DD --to YYYY-MM-DD --owners chain,github,integrity,scoring
backfill_all       [--skip-pre-ledger] [--skip-github] [--yes]
```
The launchers `backfill_tmc`/`backfill_github` work as follows:
- If `service_is_up()`: write the request file, `POST /api/v1/control/run/{id}` with `X-Auth-Token: API_AUTH_TOKEN`, then poll `GET /api/v1/jobs` and the sidecar every 10 s, printing progress. A 409 (already running) means tail only. While the sidecar shows pending units after a `partial:` note, the launcher re-triggers; for `backfill_github` it first sleeps until `BACKFILL_GITHUB_WINDOW_UTC` opens.
- Otherwise, or with `--standalone`, which **refuses if `/health` answers**: build the runtime without scheduler or HTTP (01 `run-job` path) and run the job through its wrapper.

### 8.3 Step order (build)

1. Confirm 03's schema v1 carries every §3.2 object (it does as of 03's 2026-09-23 revision; no 17 migration). Then config keys and validators, `common.py` (incl. the column check and 03's `src_rank` registration), and tests for the lock, sidecar and `src_rank`.
2. `bars.py` and `occupancy.build_intervals` (pure), with their unit tests (§9 #5–#10).
3. `ledger_import.py` + script. On the laptop, run S1 against the real `seed/` files and check §10 items 1–4.
4. `fundamentals_import.py` + script (S2). This depends on 04's `app.registry.legacy.import_fundamentals`; if 04 put that logic in the script, refactor it into that function first.
5. `rpc_client.py` + `--probe`. Run it on the laptop: public OnFinality, then keyed, then the archive. It must pass the golden block before step 6; fix the conversions if it fails.
6. `rpc_backfill.py` eras, `occupancy.write_occupants`/`stamp_all`, S4 then S5.
7. `tmc_backfill.py` job, launcher and `JOB_TABLE` rows. Then S3 (can precede 5–6) and S6.
8. `regime_backfill.py`. It needs spec 12's `app/quant/regime.py`. If Phase 8 is not built yet, port the pure functions now (old `regime.py:22-221` without `RegimeFilter`) into that module exactly as 12 specifies; 12 then owns the file.
9. `coverage.py`, `pi_recovery.py`, `backfill_all.py`; S7, S8.
10. At Phase 4: `github_backfill.py` (S9). At Phase 8: `replay_history.py` (S10).

### 8.4 What to port (file:line in `E:\SN_Trader\SubnetTrader\`)

| old code | lines | new home | how |
|---|---|---|---|
| `app/data/backfill_metadata.py` `representative_blocks` | 38-50 | `bars.enrich_reserves` subquery, `plan_buckets` idea | bare-column `MAX(timestamp)` per bucket — keep the SQL shape |
| `app/data/backfill_metadata.py` `_apply` | 53-65 | `ledger_import.merge_nulls`, `write_bucket` | `moving_price IS NULL` write gate + commit per unit — keep the semantics |
| `app/data/backfill_metadata.py` docstring | 1-21 | §2.1 note | the "no quota" claim for the archive is **obsolete** (research §2: `-32004`) |
| `scripts/backfill_rpc_candles.py` `_block_timestamp` | 58-69 | `RpcGateway.timestamp_ms` | `Timestamp::Now` ms → s; **drop** the linear fallback (a failed read fails the bucket) |
| `scripts/backfill_rpc_candles.py` sample loop | 172-210 | `process_bucket`, `convert_entry` | per-block cross-section, skip `price ≤ 0 or tao_in ≤ 0` (199), per-netuid dedup |
| `scripts/backfill_rpc_candles.py` progress sidecar | 42, 120-149 | `Sidecar` | resumable progress file, now atomic |
| `app/data/chain_client.py` `normalise_dynamic_info`, `_balance_to_tao` | 682-765 | `convert_entry` | field names and ÷1e9 convention; we decode raw runtime dicts, not SDK objects |
| `app/chain/endpoints.py` | 1-81 (`is_dead_socket_error` 31, `is_quota_error` 39, `EndpointSelector` 49-81) | imported from 05's `app/collectors/endpoints.py` (02 §G1; never copied again) | `EndpointSelector`, `is_dead_socket_error`, `is_quota_error` (extend with `-32004`, `-32029`) |
| `scripts/backfill_whale_events.py` `INSERT_SQL`, `open_db` | 49-55, 71-77 | `import_whale_cohort`, `common.open_db` | `INSERT OR IGNORE` shape; WAL + `busy_timeout=5000` (but 03's `migrate()`, not `executescript(models.sql)`) |
| `scripts/seed_pool_history.py` `_to_unix` | 27-43 | — | **not ported**: 01's `app.util.time.parse_ts` covers ISO `Z` and epoch (02 §B1) |
| `app/backtest/pool_history_loader.py` `detect_dense_era_start` | 58-78 | `coverage.py` cadence classification | samples/netuid/day rule, read-only |
| `app/storage/models.sql` `whale_cohort` | 158-167 | 03 §3.3 DDL | + appended columns (03) |
| `app/strategy/regime.py` | 22-221 | consumed via 12's `app/quant/regime.py` | pure functions only; `thin_universe` ⇒ `UNKNOWN` (03/12 vocabulary) |
| `app/data/chain_client.py` `_gather_pool_reads` | 197-222 | `RpcGateway` timeout/reconnect | `asyncio.wait_for` + drop the socket on timeout |
| `app/data/fundamentals_collect.py` `write_atomic` | 388-393 | via 04's `app/utils/fs.py` | verbatim (04) |

---

## 9. Tests

`tests/backfill/` uses pytest and pytest-asyncio. There is no network and no real seed DB. Fixtures:
- `fixtures.make_old_ledger(path, …)` builds an **old-schema** `pool_history` (with and without the 3 later columns). It covers 4 netuids: a daily seed era with NULL columns, a back-stamped `network_registered_at` on pre-trust rows, 1-minute dense rows, and netuid 40 re-registered at block 8,409,860 inside the trusted era.
- `make_backfill_db` (a subset with `moving_price`).
- `FakeGateway` implements the `RpcGateway` API with a deterministic `ts(block) = 1758585588 + 12.02·(block − 6502594)` plus one injected 2-h stall, scripted errors, and decoded `dynamic_info` fixtures built from the golden SN64 row.
- TMC payloads served through `httpx.MockTransport`; GitHub via `respx` (06 style).
- The target DB is always 03's real schema (`migrate()` on a temp file), so CHECKs (`events.severity`, `events.source`, `price_bars.source`, `regime_daily.label`), foreign keys and the `pool_history` triggers are exercised. No test uses a hand-written copy of 17's tables.

| # | test | asserts |
|---|---|---|
| 1 | `test_import_counts_and_trust` | rows = source rows with `price > 0 and tao_in_pool > 0`; pre-trust `network_registered_at` stored NULL and counted `nra_dropped_untrusted`; `occupant_key` only on trusted rows; `observed_at = timestamp` for rows ≥ `LEDGER_LIVE_FROM_TS` and `= run_started` for earlier rows; `event_at` NULL; re-run adds 0 rows |
| 2 | `test_null_merge_never_overwrites` | NULL filled from the merge source and `enriched_at` set once; non-NULL main value unchanged even if the source differs; a source row that can fill nothing (target `network_registered_at` NULL, source value pre-trust) is not matched, so `changes()` counts only really filled rows; counts returned |
| 3 | `test_wal_source_checkpointed` | rows present only in a `-wal` are imported via the checkpointed copy (an `immutable=1` attach of the original would miss them) |
| 4 | `test_thin_sec` | `thin=300` keeps exactly the last row per 5 min per netuid |
| 5 | `test_bars_ohlc_close_is_max_ts` | open = first, close = `MAX(timestamp)` row, hi/lo, `ts_last`; `moving_price` = latest non-NULL in the same bucket (an RPC sample before a NULL ledger close row), never from the previous bucket; volume = Δ across buckets; NULL across occupant change, gap > 2 periods, negative Δ; `observed_at = ts_last`/`backfilled=0` only for a live-captured, non-enriched ledger close; seed-era and RPC bars → write time / `backfilled=1` |
| 6 | `test_bars_precedence_order_independent` | all 24 write orders of {`onfinality`, `tmc_candle`, `ledger_import`, `sse_rollup`} give an identical final table (with 03's `src_rank` registered); `sse_rollup` never replaced; an equal-rank row with a later `ts_last` replaces the close (Pi row after the PC's last row), one with the same/earlier `ts_last` or a NULL-`ts_last` `tmc_candle` rewrite is skipped and counted `same_rank_skipped` |
| 7 | `test_enrich_reserves_null_only` | `tmc_candle` bar gets reserves + chain `block_number` from the same-bucket RPC row; a `ledger_import` bar with NULL `moving_price` gets it from a same-bucket RPC row and flips to `backfilled=1`, `observed_at` = now; existing non-NULL values untouched; `tmc_block_number` preserved |
| 8 | `test_build_intervals` | netuid 40 → two intervals split at 8,409,859/8,409,860; reg reappearing → anomaly; `reg > first obs block` → anomaly |
| 9 | `test_write_occupants_historical` | the current occupant's status is unchanged; the historical one is inserted `deregistered/reoccupied` with predecessor/successor keys; `registered_ts` from `block_times`; all NOT NULL `occupants` columns set and `source ∈ {onfinality, rpc, legacy_ledger}`; one `netuid_reoccupied` written through 03's `emit_event` with `severity='warning'`, `source='registry'`, `payload.backfilled=true` (validated against 03's backfill key set), `dedupe_key='reoccupied:{old}->{new}'` and `alerted_at` set (03's real DDL: a `warn` severity or a `backfill_rpc` source would fail the CHECKs); re-run → still one; a crash injected after the occupancy commit → the next run writes the missing event |
| 10 | `test_stamp_all` | seed rows keyed by block interval; trusted mismatching row counted, not overwritten; rows below the earliest reg stay NULL |
| 11 | `test_block_sampler` | drifted anchors → hit within ≤ 1 correction; stall hour → `None` (`no_block_in_hour`); divergence → `BlockSearchFailed` after 3 |
| 12 | `test_convert_entry_golden` | raw rao/bits fixture for SN64 → exactly `187212.134573233 / 2897413.298870939 / 0.06478730659000576 / 4531295 / 4657484.168541687`; `price ≤ 0` skipped; `None` entries skipped and counted; `AccountId32` as bytes, int tuple, hex and SS58 → the same SS58; netuid 0 excluded from price checks |
| 13 | `test_swap_sqrt_price` | U64F64 LE → `v²`; netuid parsed from Identity and Twox64Concat keys alike |
| 14 | `test_storage_keys` | `storage_key("SubtensorModule","SubnetTAO",64)` = `0x658faa385070e074c85bf6b568cf05557a57dce016211512d1700561066b85a34000`; `TIMESTAMP_NOW_KEY` constant |
| 15 | `test_rpc_backoff_failover` | `-32004` ×2 → sleeps 5 s, 10 s (fake clock) → OK; 6 failures → bucket `failed`; 3 failed buckets → `mark_bad` + next endpoint; config validator rejects `lite.`/`entrypoint-finney` |
| 16 | `test_plan_buckets` | `gap` starts after the ledger max; `ledger` picks empty hours, hours with < 100 netuids and dense hours with no non-NULL `moving_price`, and skips complete hours; `done`/`skipped` excluded, `failed` included |
| 17 | `test_write_bucket_atomic` | exception inside the transaction → no `pool_history` rows and no `done` marker |
| 18 | `test_candles_parse_aggregate` | ISO and epoch `ts`; 9- and 10-element rows parse, 8 or 11 → `ParseError`; 5m rows with `tmc_block_number` set, `block_number` NULL, `ts_last` NULL; 1h only with ≥ 6 bars, 1d only with ≥ 144; `ts` = bucket start |
| 19 | `test_detect_label` | start-labelled and end-labelled synthetic series detected; inconclusive → `start` + caveat |
| 20 | `test_analytics_occupant_segments` | 1M rows spanning a reoccupation → `upsert_hourly_from_analytics` called once per segment with the right key; unknown occupant → `partial`; `restamp` from `chain_rest_snapshots` → `done` with 0 HTTP calls |
| 21 | `test_tmc_job_pacing` | every request via `TmcClient` methods with no `priority` argument; `HourCeiling` 180/h and both quiet windows `02:05-02:45,20:25-23:35` with a fake clock; `done` sidecar units skipped; `BudgetExhausted` → `paused: budget`, units kept pending, no event; `upsert_daily_from_analytics` called with `backfilled=1` and only rows carrying `occupant_key` |
| 22 | `test_regime_unknown_not_dead` | thin day → `UNKNOWN` with `v_w = 0.15`; dense day equals the ported function's label with the master §7.6 `v_w`; window is `(D − 86400, D]` on bucket starts; snapshots carry INTEGER `ts`; rows written with 03's columns (`day_ts`, uppercase `label`, non-NULL `v_w`, `raw_regime`, `spec_version`, `computed_at`, `source='backfill'`, `backfilled=1`); a `source='live'` row is not overwritten |
| 23 | `test_fundamentals_import` | 04 importer called with `source_ref='fundamentals.json@2026-06-02'`; raw snapshots = records; a netuid reoccupied after `generated_at` → observation on the historical key, no override; no `team_info` → 0 rows; with `team_info` → 1 row |
| 24 | `test_whale_cohort_import` | 200 rows from `coldkeys[]`, `excluded[]` not imported (counted), labels read from `labels{}` keyed by ss58 and merged, re-run idempotent |
| 25 | `test_github_backfill_repo` | 2 commit pages + releases + PRs/reviews → rows with `backfilled=1`; release `dedupe_key='release:{repo_id}:{tag_name}'` (a release already written by 06 is not duplicated), drafts skipped; PR `dedupe_key='pr_merged:{repo_id}:{number}'`; `/repositories/{id}/…` 404 → retry via `/repos/{full_name}/…`; page cap → `truncated`; 404 on both → `gone`; own reserve 1,500 blocks with fake quota; `github_events` rows carry `payload_json.backfilled=true`; a trigger outside `BACKFILL_GITHUB_WINDOW_UTC` (fake clock) returns `partial: outside window` with 0 requests, and the window closing mid-run stops at the next repo; re-run → no duplicates |
| 26 | `test_coverage_schema` | output validates against the `coverage-1` JSON schema of §2.9 (in tests): every listed key present (null, never omitted), every time an int; a 12-h gap produces one `holes` entry; completeness math |
| 27 | `test_pi_guard_and_parse` | `safe_open(".../coldkey")` and `.../hotkeys/x` raise; `coldkeypub.txt` JSON and plain forms parse; `positions_summary` on a fixture `ema_positions` |
| 28 | `test_parse_coldkey_stakes_tolerant` | three payload shapes → identical normalised rows |
| 29 | `test_no_live_row_mutation` | DB with `sse` `chain_hourly`, `sse_rollup` bars and trusted ledger rows: after every writer runs, those rows hash identical |
| 30 | `test_script_preconditions` | S1 with `/health` up → exit 2; `BACKFILL_ENABLED=false` → exit 3; schema without 17's columns → exit 3 listing them; `./PAUSE_ALL` → standalone step stops at the next unit, exit 0, sidecar saved; a `control_state` row for `backfill` (16 group) or `backfill_tmc` → the in-service job stops at the next netuid with `JobOutcome.note == "paused:operator"`; no code path creates or deletes a file under `data/control/`; standalone launcher with the service up → refuses |
| 34 | `test_backfill_events` | a completed step → one `backfill_done` (`source='runtime'`, `occupant_key` NULL, payload `job, era, rows, seconds, coverage_path`, `dedupe_key='{job}\|{run_id}'`, `alerted_at` NULL); an exception after work started → one `backfill_failed` (`job, era, error, resume_hint`) and, in-service, the wrapper still records the failure; exit 2/3 and partial ends → no event; `BACKFILL_ALERTS_ENABLED=false` → rows written with `alerted_at` set; 17 code never imports `send_alert`; `backfill_rpc` era with new `rpc_owner_history` rows deletes `job_state('owner_rpc_seed','rpc_seed_done')` |
| 31 | `test_rpc_token_accounting` | with a fake ASI substrate that counts socket requests: one bucket = 4 requests (5 with `swap_sqrt`) and exactly as many `limiter.acquire` calls; ASI's `runtime_call` is never called on the hot path; a new `specVersion` acquires `RUNTIME_LOAD_TOKENS` before `get_runtime_for_version`; the public-URL sub-bucket caps at `BACKFILL_RPC_PUBLIC_MAX_RPS` even when a key is set |
| 32 | `test_pool_history_trigger` | on 03's real schema: `UPDATE pool_history SET price = …` aborts; NULL-only enrichment of `moving_price`/`occupant_key` + `enriched_at` succeeds; a second enrichment of the same column aborts; a unit whose write aborts rolls back alone and is counted `trigger_aborts` |
| 33 | `test_deps_backfill_extra` | after installing `.[backfill]`: `find_spec("async_substrate_interface")` present, `find_spec("bittensor")` and `find_spec("bittensor_wallet")` are `None`; `from scalecodec.utils.ss58 import ss58_encode` works (provided by `cyscale`) |

**Live** (`@pytest.mark.live`, skipped in CI): `backfill_rpc --probe` golden checks on OnFinality public; one `gap` bucket end to end into a temp DB; TMC `candle-chart-compact` for SN64 over 2 h (1 request); `analytics/subnet/64/?span=1M` (1 request).

**Mocked always:** `RpcGateway` (`FakeGateway`), `TmcClient` transport, GitHub HTTP, clock, Telegram, `service_is_up`.

---

## 10. Verify checklist (Phase 3 is done when all applicable items pass on the laptop)

`DB=data/taosentinel.db`. Numbers assume a fresh DB, `LEDGER_IMPORT_THIN_SEC=0` and no Pi ledger. With the Pi ledger, items 1–2 gain `source='ledger_pi'` rows, reported separately.

1. `sudo systemctl stop taosentinel && python -m scripts.import_old_ledger --phase import` → exit 0. Then
   `sqlite3 $DB "select count(*), count(distinct netuid), min(timestamp), max(timestamp), min(block_number), max(block_number) from pool_history where source='ledger_pc'"`
   → `7362854|129|1758585588|1781904274|6502594|8443757`.
   `sqlite3 $DB "select sum(observed_at = timestamp), sum(observed_at <> timestamp), sum(event_at is not null) from pool_history where source='ledger_pc'"` → `7321749|41105|0` (verified split at `LEDGER_LIVE_FROM_TS` = 1777468595).
2. `sqlite3 $DB "select sum(moving_price is not null), sum(subnet_volume is not null), sum(network_registered_at is not null), count(distinct occupant_key) from pool_history where source='ledger_pc'"`
   → `3667842|3667842|3644605|131`. `jq '.ledger_backfill.merged_mp' data/backfill/ledger_import_report.json` → `23237`; `select count(*) from pool_history where enriched_at is not null` → `23237` (stamping never sets it).
3. Golden row: `sqlite3 $DB "select price, tao_in_pool, alpha_in_pool, moving_price, network_registered_at, subnet_volume, occupant_key from pool_history where netuid=64 and block_number=8443757"`
   → `0.064653121|187212.134573233|2897413.29887094|0.0647873065900058|4531295|4657484.16854169|64:4531295` (the sqlite3 3.44 CLI prints 15 significant digits; verified against the source ledger).
4. `sqlite3 $DB "select period, count(*) from price_bars where source='ledger_import' group by 1"` → `1d|25915` and `1h|149079`. `select count(*) from price_bars where source='ledger_import' and backfilled=0 and ts_last < 1777468595` → `0`. Start the service again: `/health.status == "ok"`.
5. `python -m scripts.import_fundamentals` → report `raw_snapshots: 129`, `whale_rows: 200`, `team_info_rows: 0`. Then
   `sqlite3 $DB "select count(*) from identity_raw_snapshots where source='legacy' and source_ref='fundamentals.json@2026-06-02'"` → `129`; `select count(*) from whale_cohort where as_of_block=8590789` → `200`. Re-run → `observations_written.legacy: 0`.
6. `python -m scripts.backfill_tmc --tasks analytics1y,analytics1m` (service running) → the launcher prints progress and ends with `done`.
   - `jq '[.tasks.analytics1y[].status]|group_by(.)|map({(.[0]):length})|add' data/backfill/tmc_progress.json` → `done` (plus `partial` until S6 restamp).
   - `sqlite3 $DB "select count(*), count(distinct netuid), date(min(day_ts),'unixepoch') from chain_daily where source='analytics_1y'"` → ≈ 44–47 k rows, ≥ 120 netuids, min ≈ today − 365 d.
   - During the run, `curl -s localhost:8090/health | jq '.rate_limits.tmc | .hour_used <= .hour_cap'` → `true` (01 §2.9 shape: `{tokens, hour_used, hour_cap, month_used, month_cap, penalized_until}`), and 05's own jobs show no failures. The job's requests in any trailing hour stay ≤ 180, and it makes none inside 02:05–02:45 or 20:25–23:35Z (check the JSONL log). Afterwards, `select source, json_extract(payload,'$.rows') from events where event_type='backfill_done' and json_extract(payload,'$.job')='backfill_tmc'` → one row, `runtime|<rows>`.
7. `python -m scripts.backfill_rpc --probe` → every golden check `PASS`. `jq '.pass, .endpoint, .onfinality_url_form, .price_method_match' data/backfill/rpc_probe.json` → `true`, the endpoint used, the working URL form, `"swap_sqrt"` (or `"reserve_ratio"`, recorded in §11 item 4).
8. `python -m scripts.backfill_rpc --era gap`, then:
   - `sqlite3 $DB "select status, count(*) from backfill_buckets where era='gap' group by 1"` → `done` ≈ 2,300 (+24 per day since 2026-09-22), `failed` ≤ 1 %.
   - `select count(distinct netuid), min(block_number) from pool_history where source in ('onfinality','opentensor_archive') and block_number > 8443757` → ≥ 128, min within ≈ 300 blocks of 8,443,757.
   - Monotonic block time: `select count(*) from (select ts_ms, lag(ts_ms) over (order by block_number) p from block_times) where ts_ms <= p` → `0`.
9. `python -m scripts.backfill_rpc --era ledger` → `select status, count(*) from backfill_buckets where era='ledger' group by 1` → `done` ≈ 5,977. Then `sqlite3 $DB "select count(*) from (select timestamp/3600 h from pool_history where timestamp between 1758585588 and 1781904274 group by h having count(distinct netuid) < 100 or sum(moving_price is not null) = 0)"` → ≤ 1 % of the 6,479 ledger hours (only `failed`/`skipped` buckets).
10. `python -m scripts.import_old_ledger --phase stamp,bars`, then:
    - `sqlite3 $DB "select count(*) from pool_history where occupant_key is null"` → `0`.
    - `select occupant_key, status, dereg_reason, successor_key from occupants where netuid=40 order by network_registered_at` → `40:3372582|deregistered|reoccupied|40:8409860` then the current `40:8409860` row (status per 04).
    - `select count(*) from occupants where registered_ts is null` → `0`.
    - `select count(*) from events where event_type='netuid_reoccupied' and json_extract(payload,'$.backfilled')=1 and alerted_at is null` → `0`; `select distinct severity, source from events where event_type='netuid_reoccupied' and json_extract(payload,'$.backfilled')=1` → `warning|registry`. `select count(*) from events where source like 'backfill%'` → `0` (02 §B9; the CHECK forbids it anyway).
    - `select count(*) from price_bars where period='1h' and ts between 1777468595 and 1780182420 and moving_price is null` → ≈ 0 (the dense pre-05-30 hours now carry `moving_price` from same-bucket RPC samples).
    - No Telegram message arrives for historical reoccupations.
11. `python -m scripts.backfill_tmc --tasks restamp,candles5m`, then:
    - `jq '.candle_label' data/backfill/tmc_progress.json` → `"start"`.
    - `sqlite3 $DB "select period, source, count(*) from price_bars where source in ('tmc_candle','onfinality','opentensor_archive','ledger_import') group by 1,2"` → 5m `tmc_candle` ≈ 3.5 M, 1h `tmc_candle` ≈ 290 k, 1h `ledger_import` still `149079`.
    - `select count(*) from price_bars where period='1h' and source='tmc_candle' and tao_in_pool is null and ts < (select max(bucket_ts) from backfill_buckets where era='gap' and status='done')` → ≈ 0 (enriched).
    - `select count(*) from price_bars where source='tmc_candle' and block_number is not null and block_number not in (select block_number from pool_history where netuid=price_bars.netuid)` → `0`.
    - All `analytics1y` units are `done`.
12. TMC vs chain agreement in the gap (SN64):
    `sqlite3 $DB "select count(*), avg(abs(pb.close-ph.price)/ph.price) from price_bars pb join pool_history ph on ph.netuid=pb.netuid and ph.timestamp between pb.ts+3000 and pb.ts+3599 and ph.source='onfinality' where pb.netuid=64 and pb.period='1h' and pb.source='tmc_candle'"`
    → count ≈ 2,300, mean rel. diff < 0.01.
13. Volume-unit check (§11 item 5), for one gap day `D`:
    `select (select sum(volume) from price_bars where netuid=64 and period='5m' and source='tmc_candle' and ts>=:D and ts<:D+86400) / ((select subnet_volume from pool_history where netuid=64 and timestamp<:D+86400 order by timestamp desc limit 1) - (select subnet_volume from pool_history where netuid=64 and timestamp<:D order by timestamp desc limit 1))`
    → ≈ 1.0 means the same unit. A second, direct check compares the cumulative counters: `select pb.start_volume / ph.subnet_volume from price_bars pb join pool_history ph on ph.netuid = pb.netuid and ph.source='onfinality' and ph.timestamp between pb.ts and pb.ts+600 where pb.netuid=64 and pb.period='5m' and pb.source='tmc_candle' limit 20`. TMC's `start_volume` is cumulative `SubnetVolume` in τ per research, so ≈ 1.0 means `subnet_volume` is τ. Record the ratios in `coverage.json.caveats` if they are not ≈ 1.
14. `python -m scripts.backfill_regime`, then:
    - `sqlite3 $DB "select label, count(*) from regime_daily where source='backfill' group by 1"` → TRENDING/DISPERSED/CHOPPY/DEAD counts, with `UNKNOWN` only for days without hourly bars.
    - `select count(*) from regime_daily where thin_universe=1 and label<>'UNKNOWN'` → `0`; `select count(*) from regime_daily where source='backfill' and backfilled<>1` → `0`.
15. `python -m scripts.build_coverage && jq '.checks, .occupancy.unstamped, (.holes|length), .caveats' data/quant/coverage.json` → every `checks.*.pass == true`, `rpc_golden_block == "pass"`, unstamped all 0, holes only the documented ones (none if the `ledger` era ran). `jq '.forward_only | index("site_age") != null' data/quant/coverage.json` → `true`, and every time field is a number (`jq '.generated_at|type'` → `"number"`).
16. Idempotency: re-run `import_old_ledger --phase import` → every `rows_new: 0`; `backfill_rpc --era gap` → `planned 0 buckets` (except the newest hours); `backfill_tmc` → all units skipped. `select count(*) from pool_history` is unchanged.
17. Invariant: `sqlite3 $DB "select count(*) from chain_hourly where source in ('sse','rest_table') and backfilled=1"` → `0`. `select count(*) from price_bars where source='sse_rollup'` equals the value before S6.
18. Pi, if recovered: `python -m scripts.pi_recovery_report --coldkey <SS58 from seed/pi/wallets/trader_wallet/coldkeypub.txt>` → a table of the 10 positions with `pi_status`, `still_staked`, `alpha`, `tao_value`; `jq '.pool_history.max_ts_iso, .pool_history.rows_beyond_pc'`. After S1, `select count(*) from pool_history where source='ledger_pi'` ≥ `rows_beyond_pc`, and the `gap` era starts after the Pi's last block.
19. GitHub (at Phase 4): `python -m scripts.backfill_github`, then:
    - `sqlite3 $DB "select count(*), date(min(event_at),'unixepoch') from github_commits where backfilled=1"` → > 0 rows, min ≥ `2025-02-13` (`event_at` is INTEGER unix, 03).
    - `select count(*) from github_activity_daily where backfilled=1 and day_ts < 1748736000` (2025-06-01) → > 0.
    - `select count(*) from (select dedupe_key from github_events group by dedupe_key having count(*) > 1)` → `0` (it is a UNIQUE column; the query guards against a schema drift).
    - `jq '[.[]|select(.truncated==true)]|length' data/backfill/github_progress.json` is reported in coverage.
    - `/health.collectors.github_*` show no failures during the run.
20. `pytest tests/backfill -q` green; `python -m compileall app scripts` clean; `python -m app jobs` lists `backfill_tmc` and `backfill_github` as `manual`, and `/health.status == "ok"` throughout S3, S6 and S9. `python -m app ops pause backfill` mid-run → both jobs stop at the next unit with `paused:operator`; `python -m app ops resume backfill` + re-trigger continues from the sidecar. During S9, GitHub `x-ratelimit-remaining` never drops below 1,500 because of the backfill, and it makes no request outside 00:30–08:00Z (02 §G4).

---

## 11. Open questions / operator decisions

1. **Identity source name (brief vs sibling).** The brief asks for `source='legacy_fundamentals'`, `observed_at=generated_at_unix`. 04's `identity_observations.source` CHECK allows only `chain|subnetalpha|tmc_coldkey|tmc|manual|legacy`, and 04 already imports this file as `source='legacy'` at `LEGACY_AS_OF`. **Implemented 04's names:** `source='legacy'` and `source_ref='fundamentals.json@2026-06-02'` distinguish the file, and `observed_at` = `generated_at_unix` as INTEGER 1780398221 (02 §B1; the same instant as the brief). This is 02 §J1's rule. A distinct source value would need 04's CHECK widened.
2. **`team_info` does not exist** in `fundamentals.json` schema v2 (verified 0/129 records; only `owner`, the editorial owner name). No team candidates can come from this file. Spec 11 must seed from the live subnetalpha ACF `team_info`. The code path (§2.6 step 3) remains for a newer file.
3. **Disk and backups vs master "one SQLite file".** The history adds ≈ 1.8–2.4 GB to the single DB (**implemented per master**). Backups are gzip-compressed (≈ 40–50 %, 03/16), and 03's `DB_BACKUP_MAX_TOTAL_GB` = 12 caps the local backup directory. So the history does not overflow checklist A1's "≥ 60 GB free" (02 §I1); it means fewer old copies are kept (03 §11 item 1 already records that 7 daily + 8 weekly copies do not fit late in year 1). Options: (a) `LEDGER_IMPORT_THIN_SEC=300` (−0.7 GB; no consumer needs 1-minute rows); (b) a USB/NAS mirror with a larger budget (16 §2.2 step 13); (c) a static read-only `history.db` attached at query time (a master change). Recommendation: (a) + (b), decided before S1.
4. **Price semantics UNVERIFIED.** The hypothesis is ledger `price` = Swap `AlphaSqrtPrice²`. The golden probe settles it. If neither `swap_sqrt` nor `reserve_ratio` matches the ledger to 1e-6, the SDK's exact derivation must be reproduced; otherwise a ≈ 0.06 % level shift appears at the ledger→RPC boundary (caveat `price_method_mixed`). The netuid 0 ledger prices (1.0–4.04) are unexplained and excluded.
5. **Volume units.** `subnet_volume` (÷1e9: τ or α?) versus the TMC candle `volume` unit is UNVERIFIED. §10 item 13 measures the ratio. Until it is confirmed, 13 must not mix `volume` across sources.
6. **Unverified decoding details**, all settled by `--probe` against the golden block: `moving_price` I96F32 frac bits (32), and the Identity hasher for `SubnetAlphaIn`/`SubnetMovingPrice`/`SubnetVolume`/`NetworkRegisteredAt`.
7. **OnFinality keyed URL form** (`/ws?apikey=` vs `/public-ws?apikey=`) is UNVERIFIED. The probe records the working form, which answers 01's `ONFINALITY_WS_URL` note. The operator then pastes it into `.env`.
8. **Requests to 01.** (a) Resolved by 02 §E1 (the `manual` trigger kind). (b) Open: 01's `ProviderLimiter` table needs the row `opentensor_archive` (`ARCHIVE_RATE_PER_SEC`, burst 4); 02 §G1 already lists the id. The public-OnFinality sub-bucket is a 17-internal 01 `TokenBucket`. (c) Covered by 02 §F1: 01's inventory holds §5's keys, including `LEDGER_LIVE_FROM_TS`, `BACKFILL_RPC_PUBLIC_MAX_RPS` and `BACKFILL_GITHUB_WINDOW_UTC`; the retired `BACKFILL_PAUSE_PATH` is dropped. (d) Resolved by 02 §B12: there is no sentinel, and pauses use `control_state`.
9. **Requests to 05 and 13.** All resolved:
   - (a) per-segment `upsert_hourly_from_analytics` — 05 §8;
   - (b) a pre-set `occupant_key` honoured by `upsert_daily_from_analytics` — 02 §J1, 05 §8;
   - (c) a historical snapshot tier for replays — 05 §2.3;
   - (d) module paths: `TmcClient` lives in `app/collectors/chain_tmc.py` and the limiter in `app/ratelimit.py` — 02 §G1, §G2;
   - (e) 13 reads `coverage.json` — 02 §J2, schema in §2.9.
10. **Column-name reconciliation.** Resolved by 03's canonical DDL and views: `v_quant_price_bars.bucket_start`, `v_quant_chain_daily.day/emission_share/tao_in_pool`, `v_quant_chain_hourly.hour/tao_in_pool/moving_price`, and `v_quant_occupants.registered_ts` (= `COALESCE(registered_ts, registered_at_est)`); every occupant time is INTEGER. 17 writes 03's names only. `regime_daily` uses `day_ts` and `label` (02 §B8). What remains: 13 tags a missing regime as lowercase `unknown`, while 03/12/17 store uppercase `UNKNOWN`, so 13's regime join must upper-case or map.
11. **Historical events convention.** Resolved by 02 §J1/§D3/§D4: history rows keep the producer's `source`, carry `payload.backfilled = true` and are inserted with `alerted_at` preset. The dispatcher sends only rows with `alerted_at IS NULL`.
12. **Regime comparability.** Resolved: 12 writes live rows with the same raw rule (no debounce, thin ⇒ `UNKNOWN`, same `RegimeParams`). The residual differences are the price source (`price_bars` close here vs `chain_hourly.price` live) and the missing historical `is_active` filter. 13 should report `by_regime` results split at the first `source='live'` day if they diverge.
13. **Unverified TMC details:** the `candle-chart-compact` label convention (auto-detected), the `coldkey-stakes`/`coldkey-subnet-chart` payload shapes and params (tolerant parser, raw kept), and whether analytics history of a reused netuid is spliced across occupants (handled by block-based stamping either way).
14. **`pre_ledger` era** (2025-02-13 → 2025-09-22, ≈ 1 h on OnFinality) is **on by default**, following master §13 item 9 ("recommended yes") and checklist A5 ("pre-ledger backfill on"). It gives 13's death-inclusive panels the whole dTAO era (5 more months, including the early deaths). The operator may skip it with `backfill_all --skip-pre-ledger` or `BACKFILL_RPC_ERAS`.
15. **GitHub PR/review backfill** is on by default (≤ 200 reviewed PRs per repo). Turn it off (`--no-prs`) if the PAT budget is tight; `review_practice_90d` then becomes forward-only in 13's matrix.
16. **TMC 1Y window urgency.** It loses one day per day, so S3 should run on the first day of Phase 3. `span=ALL` is also capped at 365 days (research 02-gapfill-keyless §1). Older `emission_percent` is recoverable only via a TaoStats key or an RPC emission replay (not planned).
17. **Mixed seed-era semantics.** Seed-era ledger rows are Taostats daily bars (Taostats price, real blocks). The `ledger` era adds chain hourly rows around them, so a day's bars mix sources. This is accepted with caveat `seed_era_taostats_price`.
18. **Trust boundary for Pi rows.** `LEDGER_NRA_TRUST_FROM_TS` (2026-05-30T23:08:13Z) is derived from the PC copy. If the Pi report shows live-captured `network_registered_at` earlier (e.g. the value changes over time for a reused netuid before that date), the operator may lower it. The default is kept.
19. **Decoder choice.** The brief allows "async-substrate-interface or bittensor SDK v11 `runtime_call`". Only ASI ≥ 2.2 is implemented: the `bittensor` SDK depends on `bittensor-wallet`, which the master forbids (§1.2), while ASI 2.2.1's runtime requirements are verified free of it (§8.1). If ASI cannot decode some runtime, the fallback is `BACKFILL_RPC_MODE=storage` (raw Identity-hashed storage reads on OnFinality), not the SDK.
20. **Former disagreements with 03's comments** are now resolved in 03's 2026-09-23 text:
    - (a) ledger `observed_at` is `timestamp` only from `LEDGER_LIVE_FROM_TS`;
    - (b) the `price_bars` same-rank `ts_last` rule, 02 §J1;
    - (c) the `volume` Δ comment;
    - (d) stamping never sets `enriched_at`.

    One residue remains: 03 §2.5's trigger-table row still says the NULL-only enrichment "including `occupant_key` … must also set `enriched_at`". That contradicts 03 §2.9 and 03's DDL comment, which 17 follows. The trigger allows either, so this is 03's wording to fix.
21. **Requests to 06.** (a) Adopt the PR dedupe key `pr_merged:{repo_id}:{number}` (06 §2.5 names the release key but not the PR key). (b) `GitHubQuota.wait_if_reserved(job)` has no per-job reserve, so 17 implements its own 1,500 gate; an optional `reserve=` argument would be cleaner. (c) Sub-paths of `/repositories/{id}/…` (commits, releases, pulls) are UNVERIFIED; 17 falls back to `/repos/{full_name}/…`.
22. **5m retention.** 03's `PRICE_BARS_5M_RETENTION_DAYS` (90) deletes the oldest days of the 5m gap series almost immediately (2026-06-19 is 96 days before 2026-09-23). The 1h/1d aggregates survive. If anyone needs 5m history for the gap, raise the retention before S6.
23. **Archive budget accounting UNVERIFIED.** It is unknown whether `state_getRuntimeVersion` and `chain_getHeader` count against the opentensor archive's `-32004` "historical_references" budget. Research only established that `chain_getBlockHash` does not count. The probe measures throttling with the 4-call bucket, and `ARCHIVE_MIN_BUCKET_INTERVAL_SEC` is the lever.
24. **`github_events` has no `backfilled` column** (03 §3.4), while 02 §J1 asks backfilled rows to carry `backfilled=1`. 17 sets `payload_json.backfilled = true` instead (§2.8). 03 could add the column; this is not assumed.
25. **`events.source` for `backfill_done`/`backfill_failed`.** 03 §3.9's emitter column ("17 backfill (manual jobs)") gives no §B9 value, and 17 is not in B9's producer list. 17 uses `runtime`, the value of the other job-run system events (01's `collector_failing`). 03 should state it.
26. **Reconstructed reads for P4 replays.** 11's seeded `owner_changes` carry `observed_at` = import time (11 T45). `control_stability_90d` history therefore needs 11's entry points to admit backfilled evidence by `event_at` in 12's reconstructed mode. Until 11 confirms this, the key is listed as `forward_only` in `coverage.json` (§2.11). `owner_commitment` reads `chain_daily`, whose rows carry the fetch time too: the same question applies, UNVERIFIED.
27. **Re-arming 11's seed.** 17 re-arms `owner_rpc_seed` by deleting `job_state('owner_rpc_seed','rpc_seed_done')` (§2.3 step 8). 11 says only "re-armed when spec 17 writes new buckets"; the "absent row = armed" reading is UNVERIFIED against 11's code.

---

## 12. Sources

- **Master and sibling specs:**
  - `E:\TaoData\specs\02-cross-cutting-conventions.md` (binding ledger; B1, B2, B8, B9, B12, D1–D4, E1, E2, F1, F2, G1, G3, G4, I1, J1, J2).
  - `16-ops-deploy-and-observability.md` §2.11 (`PAUSE_GROUPS` group `backfill`, `app/control.py`), 11 §5–§8 (`owner_rpc_seed`, `compute_<metric_key>`), 07 §2.8 (`site_age`).
  - `E:\TaoData\specs\00-MASTER-SPEC.md` §1.3, §2 (import row, reuse table), §4, §5, §6, §7.3 (metric keys/weights), §7.5 (`owner_control_change`), §7.6 (`v_w`), §10, §11, §12, §13 item 5.
  - `INDEX.md`.
  - `03-storage-schema.md` (revision of 2026-09-23): §2.4 (v1 folds 17's tables and columns), §2.9 (legacy import contract), §3.2 (`occupants`), §3.3 (precedence ranks, `price_bars`, `pool_history` + trigger, `block_times`, `backfill_buckets`, `rpc_owner_history`, `whale_cohort`), §3.4 (GitHub tables), §3.6 (`events`, `regime_daily`), §3.7 (views), §3.9 (events catalogue), retention (`price_bars_5m`, `chain_rest_snapshots`), `DB_BACKUP_MAX_TOTAL_GB`.
  - `12-feature-store-and-scoring-engine.md` §2.7 (`regime_for_slot`, `RegimeParams`, `UNKNOWN`).
  - `18-operator-checklist.md` A1 (disk), A3 (seed copy), A4 (Pi recovery, 10 OPEN positions, coldkey check via TMC), B1, B2 (`ONFINALITY_API_KEY`), C3.
  - `01-repo-skeleton-runtime.md` §2.6 (`CollectorJob`, `RunContext`), §2.7 (`JOB_TABLE`), §2.12 (`ProviderLimiter`, `Http`), §2.14 (`run-job`), §5 (Settings inventory: `ONFINALITY_*`, `TMC_*`, `SEED_DIR`), §6 (limiter table, scripts sharing rule).
  - `04-subnet-registry-and-identity.md` §2.11 (seed/legacy import), §3.1 (`occupants`, `identity_observations`, `identity_raw_snapshots`, `manual_overrides`, `events.dedupe_key`), §5 (`LEGACY_*`), §8, §11 item 5.
  - `05-chain-collector.md` §2.9 (field glossary, candle `block_number` caveat), §3.1 (`chain_daily`, `chain_hourly`, `price_bars`, `chain_rest_snapshots` DDL), §5, §6 (TMC bucket, priorities), §7 (write gate), §8 (`TmcClient`, `fetch_analytics`, `upsert_*_from_analytics`, `endpoints.py`).
  - `06-github-collector.md` §3.2 (DDL), §5, §8 (`GitHubClient`, `GitHubQuota`, `activity`, `spam`, `authors`, `P2Metrics`), §11 item 6.
  - `13-validation-harness.md` §2.1–2.6 (panel semantics, scopes, backtestability matrix, regimes), §3.1 (columns read), §7, §8 (`block_to_unix`), §11 items 1–2.
- **Research:**
  - `E:\TaoData\research\02-gapfill-data-continuity-and-backfill.md` (all: §1 local data, §2 node liveness/depth/rate limits, Identity hasher, `-32004`, block↔time drift, §3 TMC candle/analytics endpoints, §5 cost model, §6 sequence, §7 Pi recovery items).
  - `01-data-layer-and-schema.md` §1–2 (ledger tables, cadence, gaps, slot reuse), §3 (`normalise_dynamic_info`, archive recipe), §5–6 (fundamentals, reusable scripts), Gotchas (units, 1-minute vs daily era).
  - `02-gapfill-keyless-onchain-pillar-source.md` §1 (OpenAPI paths: `span=ALL` = 365 d, compact candle arrays, `coldkey-subnet-chart/{coldkey_id}`), §2 (owner locks, first SN51 lock 2026-06-04), §5 (history depth, analytics fields), §6 (polling plan, what still needs a node).
  - `01-fundamentals-scorer.md` §3 (on-chain identity reads, `backfill_metadata.py`), §7 (`fundamentals.json` schema), §8 (slot-reuse / look-ahead caveats).
- **Old bot (`E:\SN_Trader\SubnetTrader\`):**
  - `app\data\backfill_metadata.py` 1-135 (all; default `--grid 86400` at line 124-125).
  - `scripts\backfill_rpc_candles.py` 1-80, 150-230, 255-282 (`_block_timestamp` 58-69, sample loop 171-215, non-positive skip 197-200, `--sleep 0.25` 265).
  - `scripts\seed_pool_history.py` 1-80 (skim, `_to_unix` 27-43).
  - `scripts\backfill_whale_events.py` 40-120 (`INSERT_SQL` 49-55, `open_db` 71-77).
  - `app\backtest\pool_history_loader.py` 1-80 (`detect_dense_era_start` 58-78).
  - `app\backtest\factor_panel.py` 245-256 (single-`MAX()` bare-column close).
  - `app\chain\endpoints.py` 1-81.
  - `app\data\chain_client.py` 197-222, 682-765.
  - `app\strategy\regime.py` 22-221.
  - `app\storage\models.sql` 130-167.
  - `app\config.py` 453-469 (`REGIME_*` defaults).
- **Read-only probes run for this spec (2026-09-23, Python `sqlite3` with `mode=ro&immutable=1`):**
  - `data\ledger.db` `pool_history`: counts, NULL spans, trust boundary 1780182493, 131 occupant keys, golden block 8,443,757 (129 rows), monthly cadence, 149,079/25,915 bucket groups. Re-verified by the 2026-09-23 review: all of these, plus 0 non-positive rows, the live-capture boundary 1777468595 (7,321,749 rows at or after it, 41,105 before), 6,479 hours in the span of which 1,205 are populated, 200 have < 100 netuids, 503 have no `moving_price` and 502 are complete, netuid 0 price range, one block per seed day at 23:59:48Z, and netuid 40/92 reg transitions (last old-occupant blocks 8,409,823 / 8,352,005).
  - `data\backups\ledger_backfill.db`: 3,837,187 rows ⊂ ledger; merge fills 23,237 rows (23,237 at 229 pre-trust blocks); its 118,938 non-NULL `network_registered_at` values are all on post-trust rows that the ledger already has.
  - `data\fundamentals.json` (top-level keys, `subnets` dict keyed by netuid string, schema v2 record keys, no `team_info`).
  - `data\whale_cohort.json` (200 coldkeys, `as_of_block` 8,590,789, `as_of_ts` with microseconds, 1 `excluded`).
  - `data\whale_labels.json` (`_README` + empty `labels`).
- **Package source inspected (review 2026-09-23):** `E:\SN_Trader\SubnetTrader\.venv\Lib\site-packages\async_substrate_interface-2.2.1.dist-info\METADATA` (runtime requirements), `async_substrate_interface\async_substrate.py` (`init_runtime` 1589, `_get_block_runtime_version_for` 2579, `runtime_call` 3513-3594, `decode_scale` 1562, `rpc_request` 2875), `cyscale-0.5.0.dist-info` (provides `scalecodec`). The twox128 prefixes for `Timestamp`/`Now`, `SubtensorModule`/`SubnetTAO` and `Swap`/`AlphaSqrtPrice` were recomputed with a pure-Python xxh64.


## Changelog
- 2026-09-23 reconciliation:
  - **J1/B9:** historical `netuid_reoccupied` rows are written with `source='registry'` through 03's `emit_event`, using the backfill key set, `payload.backfilled`, and `alerted_at` preset. No `backfill_*` source remains.
  - **J1:** `pool_history` gains `run_id` and loses 17's occupant index. `price_bars` same-rank replacement uses `ts_last`. `upsert_daily_from_analytics` gets `backfilled=1` and only rows that already carry a key.
  - **J1/B1:** `github_events` carries `payload.backfilled`. `block_times.event_at` is generated. All times are INTEGER via 01's `app/util/time.py` (no `to_unix`/`iso_z`); `coverage.json` times are ints.
  - **B8:** `regime_daily.label` replaces `market_regime`, with uppercase values, non-NULL `v_w` and INTEGER regime `ts`.
  - **D1/D3:** the Telegram `send_alert` calls are replaced by the catalogued `backfill_done`/`backfill_failed` events (`source='runtime'`, §11 item 25). Occupancy anomalies go to the digest.
  - **E1/E2:** the job table is the only one, with trigger `manual`; the `liveness_max_age_sec=10**9` hack is removed.
  - **F2/G3:** TMC ceiling 180/h; quiet windows `02:05-02:45,20:25-23:35`; the estimate of 386 + 10 % once equals 05 §6.3's row. S3 takes ≈ 70 min. `BudgetExhausted` means `paused: budget`.
  - **G1/G2:** TMC calls go through `TmcClient` methods (no `priority=`). `EndpointSelector` comes from `app/collectors/endpoints.py`; the public-URL sub-bucket is a 01 `TokenBucket`. Monthly cap 90,000.
  - **G4:** `BACKFILL_GITHUB_WINDOW_UTC` `00:30-08:00` plus the reserve of 1,500 req/h.
  - **B12:** `BACKFILL_PAUSE_PATH` is retired. Pauses use `control_state`/16's `backfill` group/emergency files; standalone scripts stop on SIGINT or `PAUSE_ALL`.
  - **J2:** exact `coverage-1` schema, which 13 reads, adding `forward_only` (`site_age`, `control_stability_90d` until 11 confirms reconstructed reads). The replay gains `integrity`. 13 stamps PIT at the bucket, not at `ts_last`.
  - **Package and reviews:** package A applied (same fix as F2/G3); reviewer notes applied (03: rowid table, generated `event_at`; 11: re-arm `owner_rpc_seed`). `pre_ledger` is on by default (master §13.9, A5); `whale_cohort` labels are merged at insert (append-only); `chain_buys` is marked forward-only (05 §2.3).
