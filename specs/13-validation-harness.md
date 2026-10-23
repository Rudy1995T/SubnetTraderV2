# 13 — Validation harness: ported quant stack, PIT panel builder, labels, evidence gates, CLI

**Status:** built 2026-10-17 ahead of Phases 7–9 (reconciled with 02 2026-09-23; pre-build review against the live data 2026-10-17; see Changelog) · **Phase:** 10 · **Depends on:** 02-cross-cutting-conventions (binding: B1, B3, B5, B6, B8, B9, B12, C1–C3, C6, D1, D3, E2–E4, F2, G6, I1, J1, J2), 01-repo-skeleton-runtime (`CollectorJob` wrapper, `JOB_TABLE`, `/health`), 03-storage-schema (DDL and the `v_quant_*` views), 05-chain-collector, 06-github-collector, 11-team-and-integrity (P4 keys), 12-feature-store-and-scoring-engine (§2.15 PIT contract, registry, `app/util/mathutil.py` + `regime.py`), 15-trading-bot-integration (gate-verdict consumer), 16-ops-deploy-and-observability (backups, liveness), 17-data-migration-and-backfill (history + `data/quant/coverage.json`) · **Master spec refs:** §1.3 (point-in-time, absence≠failure, occupant identity), §2 (reuse table, quant row), §5, §6, §7 (`ts-1.0` metric_keys), §7.6 (regime), §7.7 (rank churn), §9 (Quant CLI), **§10 (panel, labels, backtestability, gate)**, §11, §12

---

## 1. Purpose and scope

The harness answers one question honestly: *does a TaoSentinel score (or any single metric) predict forward alpha-token returns, emission-share growth, survival or gate crossing, out of sample, after size, cost and multiple-testing corrections?* It is the only path by which a score may leave `observe` mode in the trading contract (spec 15). It is offline, read-only, deterministic, and makes **no external calls**.

**In scope**
- Port of the pure-stdlib quant stack from `E:\SN_Trader\SubnetTrader\app\backtest\` into `app/quant/` (master §2 row "backtest/…"), with the `settings`/`FundamentalsStore` coupling removed and `db_path` injected.
- A point-in-time (PIT) **panel builder** over 03's `v_quant_price_bars` + `v_quant_chain_hourly`/`v_quant_chain_daily` + `v_quant_occupants` views (which already convert rao → τ, 02 §J2), at the 24 h survivorship grid (from 2025-09-22) and the 4 h live grid (from 2026-04-22), death-inclusive and occupant-aware, stamping every factor from `metric_values` (`stage='norm'`) / `scores` with effective observation time `≤ bucket` (master §10; never forward-filled).
- Four label families (forward log return, forward Δ emission share, 90-d death, top-32 gate crossing).
- Study runners: factor IC/deciles (gross + net-of-cost), deflation with a persistent trial log, expanding walk-forward, tercile survival, rank stability, regime-split reporting.
- The pre-registered evidence gate of master §10 as a machine-readable verdict JSON.
- CLI `python -m app.quant …`, result JSON in the old explorer shape, an honest backtestability matrix, and the first study to run at Phase 10.

**Out of scope**
- Any trading, allocation or bot-side logic (spec 15). Any collector. Any write to the live DB other than the 01 wrapper's `collector_runs` row (no `metric_values`, `scores` or `events`; 13 writes no metric rows, so history it needs must be written by the owners' replays, 17 §2.11). The regime classifier itself (`app/quant/regime.py`, ported by spec 12 because scoring needs it at run time; 12 also creates `app/util/mathutil.py`); this spec only *reads* `regime_daily`. The strategy (per-trade) walk-forward track, `probability_of_ruin`, `validate_result` (ported for completeness, unused). IC-weighted re-weighting of `ts-1.0` (Phase-C decision, master §7.3).

---

## 2. Design

```
 DB_BACKUP_DIR/taosentinel-<stamp>.db (newest UNCOMPRESSED backup, 16; never DB_PATH, *.tmp, *.gz)
        │  python -m app.quant snapshot  → data/quant/snapshots/quant_<stamp>.db (+ coverage copy)
        ▼  app.quant.snapshot.resolve_db()  read-only, mode=ro
   v_quant_price_bars ─┐                  ┌──────────────────────────────────┐
   v_quant_chain_hourly├─ PanelSource ──► │ panel_builder.load_factor_panel  │──► Panel(24h|4h)
   v_quant_chain_daily ┘   (SQL)          │   • last-write-wins per bucket   │      Bar(bucket, price, depth, seg,
   v_quant_occupants ─── OccupantIndex ─► │   • occupant-aware segmentation  │          reg_block, moving_price, volume,
   metric_values (stage='norm') ┐         │   • death kinds                  │          ts, occ, pit{…})
   scores (computed_at, input_cutoff) ├─ PITStamper ─► • PIT stamping, cutoff = bucket │
   regime_daily (day_ts, label) ┘         └──────────────────────────────────┘
   data/quant/coverage.json (17) ──► backtestability.load_coverage (clamps, holes, caveats)
                                                     │
        labels.py ◄── v_quant_chain_daily / v_quant_occupants / events (death, Δemission, gate-cross)
                                                     ▼
   factor_backtest.run_factor_study ─► IC, deciles, deciles_net (economics @ trade_tao=2), by_regime
   deflate_factors(N, V from trials.json)   walk_forward.walk_forward_factors (≥4 OOS folds)
   survival.survival_study (terciles, ≥10 events)   rank_stability.rank_stability
                                                     ▼
   gate.evaluate_gate ─► gate-verdict JSON (per criterion + 15's keys)   report.save_factor_json (old explorer shape)
                                                     ▼
   data/quant/results/{factor,survival,walk_forward,rank_stability}_<scope>_<ts>.json, gate_verdict_<factor>_<ts>.json
```

### 2.1 Panel semantics (ported + extended)
- **Grid and bucket.** `bucket = bucket_start // grid * grid`; one bar per `(netuid, bucket)` by last-write-wins on the sample timestamp `COALESCE(ts_last, bucket_start + 3600)` (SQLite single-`MAX()` bare-column rule, ported from `factor_panel.py:248-256`). `Bar.bucket` (the bucket-start epoch) is the **decision instant** for every PIT check: a factor value is admissible only if its effective observation time is `≤ bucket` (master §10 "observed_at ≤ bucket"). `Bar.ts` is the timestamp of the sample that supplied the close price, i.e. the execution/entry instant (`ts ≥ bucket`); the forward return starts there, so deciding at `bucket` and entering at `ts` is an execution delay, never a look-ahead. This is stricter than cutting off at `ts` (the earlier draft) and than the bucket end.
- **Physical series = netuid; identity = occupant.** Bars are grouped by `netuid` (the price series is physical) and each bar is stamped with `occ` = the bar's `v_quant_price_bars.occupant_key` (stamped by 05/17 from block intervals), else `OccupantIndex.occupant_at(netuid, ts)`; `None` when unknown (seed-era rows below the earliest known registration stay unattributable, 03 §2.9). `reg_block` is parsed from `occ` (`"{netuid}:{network_registered_at}"`).
- **Segmentation = occupant boundaries ∪ price-reset rule where the occupant is unknown.** The ported one-step rule (`price[i]/price[i-1] < 0.30` ⇒ bar `i` is the *terminal* bar of the dying segment, death kind `price_reset`) fires only when the occupants of bars `i-1` and `i` are not both known and equal: a one-step crash of a known living occupant is a price move, not a death. (Measured on the 2026-10-17 snapshot: of 47 one-day collapses in the survivorship window only 4 fall within ±2 d of an occupant change — the rest are launch crashes and dust pools at 0–5 τ depth — while most of the 54 re-registrations in that window show no collapse at all, e.g. SN82's re-registration is a price *rise* with depth 3 739 → 2 τ; with 0 unstamped 1 h bars the occupant rule is the authoritative death signal. `dereg_events.detect_dereg_events` keeps the pure price rule as a cross-check only.) Additionally, when `occ[i-1]` and `occ[i]` are both known and differ, a hard boundary is forced: if the price rule also fired at `i`, nothing changes; otherwise bar `i-1` is the terminal bar and the old segment's death kind is `reoccupied` (realised at `DEATH_RETURN = −1.0`, because a `network_registered_at` change *is* a deregistration even when the collapse bar was not captured). A transition unknown→known occupant is **not** a boundary.
- **Forward return** (ported `forward_return`, extended): inside the segment → `price[i+h]/price[i] − 1`; overshoot into a segment that died by `price_reset` → `price[seg_end]/price[i] − 1` (≈ −100 %); died by `reoccupied` → `−1.0`; alive but data ran out → `None`. `forward_log_return` = `log(max(1 + r, QUANT_DEATH_FLOOR_RETURN))` (floor 0.01 ⇒ −4.6).
- **PIT stamping.** For factor `f` on bar `b`: the value of the row with the greatest `as_of` among rows for `b.occ` with `eff ≤ b.bucket`, provided `b.bucket − as_of ≤ max_age(f)`; otherwise `None` (12 §2.15 items 3–4). `eff` (effective observation time): `stage='norm'` rows → `observed_at` (= the scoring run's `computed_at`, 02 §B3); `scores` rows → `computed_at` (02 §B6; `input_cutoff ≤ computed_at` is asserted). Rows with `backfilled = 1` are dropped in `--pit-mode strict`; in `--pit-mode reconstructed` they are admitted only for keys in the reconstructible set (§2.5) with `eff = as_of + lag_s(f)` (scores: `eff = max(input_cutoff, as_of + lag_s)`), and never before `coverage.json.reconstructible_from` for that key (§2.5). Nothing is ever written back; there is no forward-fill — a stale observation outside `max_age` is `None`, not the last value.
- **Official slots on the 24 h grid.** On the 24 h grid only official rows are stamped (`scores.run_kind = 'official'`, `metric_values.as_of % 86400 = 0`, 02 §B3/§B6); the 4 h grid uses every run kind. Provisional `stage='norm'` rows are kept only `METRIC_NORM_PROVISIONAL_RETENTION_DAYS` (14 d, 03 §11 item 6), so older 4 h bars fall back to the official rows inside the freshness window.
- **`--pit-mode` scope.** The mode governs `metric_values`/`scores` rows only. Price, depth and moving-price bars and the `chain:` factors (§2.3) are chain state at their block time; reconstructed history (17's ledger/Blockmachine/RPC/TMC bars and daily analytics, enriched `moving_price`) is admissible in both modes (master §10 "honestly backtestable") and its share is reported in provenance (`price_bars_backfilled_frac`, coverage caveat `moving_price_enriched_rows`).
- **Never mix occupants.** A metric row is joined by `occupant_key`, so a prior occupant's metrics cannot leak onto a successor's bars and vice versa. For bars with `occ = None`, all PIT factors are `None`; chain-only factors (`momentum`, `size`, …) still compute from the physical series.

### 2.2 Scopes
| scope | grid | start (UTC) | end | source of price/depth | expected dates |
|---|---|---|---|---|---|
| `survivorship` | 86 400 s | `QUANT_SURVIVORSHIP_START` = 2025-09-22 (imported ledger start; `chain_daily` — emission share, the `demis`/`gate_cross`/zero-emission labels and the `chain:emission_share` control — begins 2025-09-24) | snapshot max | `v_quant_price_bars` `period='1h'` (one row per `(netuid, hour)`; the stored row is already 03's precedence winner among `ledger_import`, `blockmachine`, `tmc_candle`, `sse_rollup`, `onfinality`, `opentensor_archive`, `taostats`); depth = the bar's `tao_in_pool` (τ) else `v_quant_chain_hourly.tao_in_pool` else `v_quant_chain_daily.tao_in_pool` (both τ via the views); `moving_price` = the bar's else `v_quant_chain_hourly.moving_price` | ~390 |
| `live` | 14 400 s | `QUANT_LIVE_ERA_START` = 2026-04-22 (dense sub-daily era of the old ledger; kept so results stay comparable with the old bot's) | snapshot max | same (never the 24 h `chain_daily` depth) | ~1 080 |

Data reality (verified on the 2026-10-17 backup): 1 h bars exist for every occupant from dTAO launch (2025-02-13, Blockmachine archive replay) with `completeness_by_month = 1.0` in every month, and 05's SSE recorder writes `sse_rollup` 1 h bars live (`ts_last` and reserves set, `volume` NULL), so both panels reach the snapshot's last hour. The only holes are the 2025-05-20/22 chain halt (before both scope starts). `--start` earlier than the scope default is allowed (chain-only factors on the full dTAO history); 5 m bars are not read.

### 2.3 Factors
| name | kind | definition |
|---|---|---|
| `momentum, flow_z, meanrev_z, size, age, volume, volume_surge, px_vs_moving, moving_px_trend` | chain (ported `factor_backtest.py:63-203`) | unchanged; `age` uses `block_to_unix` (§8.2) on the bar's occupant registration block. `Bar.volume` = the **sum over the bucket's hourly bars** of `price_bars.volume` (each the Δ of the cumulative `subnet_volume` counter over that hour, 03 §2.9; unit τ, settled by 17 §11 item 5 / `coverage.checks.subnet_volume_unit`), kept only when every hourly row of the bucket has `source ∈ {ledger_import, blockmachine, onfinality, opentensor_archive}` and a non-NULL volume, else `0.0` (factor `None`): `tmc_candle` volume is TMC's own candle quantity and `sse_rollup` volume is NULL, so sources are never mixed. (The old panel read the cumulative counter itself, so its `volume` was a lifetime-turnover level; the Δ-sum is the bucket's turnover — values are not comparable with old results.) `v_quant_chain_hourly.volume` (a 24 h window) is never used as `Bar.volume` |
| `chain:emission_share` | chain state from `v_quant_chain_daily` | `emission_share` (%) of the bar's occupant from the row with the greatest `day ≤ bucket − 86 400` and `bucket − day ≤ 3 d` (the daily analytics point is stamped `day_ts` 00:00Z but may summarise that day, so one day of lag is applied, = the §2.5 `lag_s`); `None` for `occ = None` or no row. Admissible in both PIT modes (chain state, §2.1) and available from 2025-09-24 without any scoring replay. **Blind to occupants that later died**: TMC's daily analytics cover only each netuid's current occupant (BUILD-LOG Phase 3 item 8), so on the 2026-10-17 snapshot none of the 54 occupants that died in the survivorship window has a `chain_daily` row — a controlled study silently drops every future-dead name. Results report `coverage.dead_occupant_chain_daily = {n_dead, n_with_chain_daily}` and carry warning `chain_daily_excludes_dead_occupants` when fewer than half are covered (§11 item 16). *Amended 2026-10-22:* since migration 0005 the view also carries 17's daily RPC emission share for every occupant alive on a day (dead ones included, from 2025-02-14), so the coverage counts and the warning are computed over that union |
| `composite_raw, composite, composite_smoothed, quality, velocity, confidence` | PIT from `scores` | value at `as_of`; `eff = computed_at` (02 §B6); `quality` = Health |
| `P1 … P6` | PIT from `scores.pillar_scores` JSON | keys `"P1"…"P6"`, values in [0,1] or null |
| `quality_cg` | derived in-harness | `(30·P1 + 20·P2)/(30+20)` when both non-None; the sole non-None pillar when one is None; `None` when both None — the chain+GitHub sub-composite, identical on backfilled and live rows |
| any scored `metric_key` of master §7.3 (the 45 keys of 12's `METRICS`) | PIT from `metric_values` `stage='norm'` | `value` = the normalised `m` scoring used (02 §B3/§J2), e.g. `emission_share`, `gate_margin`, `active_authors_28d`; a key without norm rows (leg, aux, evidence) is rejected with exit 2 |
| `raw:<metric_key>` | `json_extract(value_json,'$.raw')` of the **same** `stage='norm'` row | raw (pre-normaliser) input, e.g. the `emission_share` control |
| `evidence:<metric_key>` | PIT from `stage='raw'` rows, 02 §B5 `metric_at` semantics (`status ∉ {failed, none}`, `eff ≤ bucket`, freshness) | weight-0 evidence/aux keys that never get norm rows (03 §3.8), e.g. `evidence:owner_locked_share` (05; the reconstructible owner-lock proxy, 11 §11 item 12), `evidence:implied_miner_burn` |

Controls default to `size,emission_share`; inside `--controls` the name `emission_share` resolves to `chain:emission_share` (the raw % share as chain state). It resolved to `raw:emission_share` before the 2026-10-17 review, but norm rows exist only after 12's scoring backfill (the live DB has none), so c2 would have dropped every name; `raw:emission_share` stays selectable by its full name. A name enters a cross-section only if the factor and **every** control are non-None and the label is non-None; the factor is then OLS-residualised on `[1, controls]` (ported `residualize`, `factor_backtest.py:324-338`).

**Look-ahead flag (02 §J2).** `x_engagement_30d` (P5) and `polarity_7d` (P6, 10's engagement-weighted bullish share) are both computed from `social_posts` engagement that 09 refreshes in place (03 §3.5), so any result whose factor is `x_engagement_30d` or `polarity_7d` or contains either (`P5`, `P6`, `velocity`, `quality`, `composite_raw`, `composite`, `composite_smoothed`) carries `lookahead_caveats += ["x_engagement_refreshed_in_place"]`, and neither key is ever admitted from backfilled rows (§2.5).

### 2.4 Labels
| label id | grid | definition | None when |
|---|---|---|---|
| `ret_log_<h>` (h = 7/28/84 on 24 h; 6/12/42 on 4 h) | bars | `forward_log_return(panel, nu, i, h)` (§2.1) | segment alive and `i+h` past data end |
| `demis_<d>` (d = 28/84) | days (from `v_quant_chain_daily`) | `share(t+d) − share(t)` in percentage points, `share(x)` = `emission_share` of the row with `day = floor_day(x)`; if the occupant's death onset (§8.4) falls in `(t, t+d]` → `−share(t)`; if a zero-emission run (§8.4) starts in `(t, t+d]` → `−share(t)` | no row within ±1 day of `t+d` and occupant alive; `share(t)` None |
| `death_90d` | days | 1 if the occupant's death onset (deregistration: `v_quant_occupants`, `netuid_reoccupied` events, or the segmentation death of its series) **or** an `emission_disabled` event **or** a zero-emission run starts in `(t, t+90 d]`; else 0 (master §10 label (c) plus the "zero-emission" of criterion 6) | `t + 90 d > label_end` (right-censored) — never assumed 0 |
| `gate_cross_28d` | days | rank by `v_quant_chain_daily.emission_share` over the occupants with a non-NULL row on that day at `t` and `t+28 d`; `+1` enters top-`QUANT_GATE_TOP_N` (32), `−1` leaves (death counts as leaving if it was in), `0` otherwise | rank at `t+28` unavailable and alive |

`t` is the bar's decision instant `Bar.bucket` (§2.1). `label_end = min(MAX(v_quant_chain_daily.day) + 86 400, snapshot made_at)` (recorded in provenance). The `ret_log_*` labels drive criteria 1-5; `death_90d` drives criterion 6; `demis_*` and `gate_cross_28d` are reported (no gate criterion) and are regime-tagged — the gate label is mechanically meaningful only in the `gate_v440` regime. Zero-emission runs are regime-dependent: the 2026-06-22 57-subnet cut is one clustered event date, and ≈ 40 miner-burn-zeroed subnets sit at ≈ 0 emission in the price-based era (research 02-gapfill-keyless §gate); survival results therefore also report `n_events` per death kind and the number of distinct event dates (§8.5). Measured on the 2026-10-17 backup: 41 % of `chain_daily` rows have `emission_share ≤ 1e-4` (70 % in 2025-10, i.e. Taoflow did zero net-outflow subnets — §11 item 7), giving 288 zero-emission runs of ≥ 7 days against 55 deregistrations, many of which later recover; the union label is therefore dominated by zero-emission spells, and every survival result also carries the dereg-only diagnostic of §8.5 (§11 item 13). The same `chain_daily` gap as `chain:emission_share` (§2.3) applies to `demis_*`, `gate_cross_*` and the zero-emission part of `death_90d`: they exist only for occupants alive at the end of the TMC pull, so day-label and survival results carry the `chain_daily_excludes_dead_occupants` warning (§11 item 16).

### 2.5 Honest backtestability matrix (what can be reconstructed today)
Reconstructible = a `backfilled = 1` series can be produced from 17's history by the owners' replays (17 §2.11: 05 chain, 06 GitHub, 12 scoring — which writes the `stage='norm'` rows 13 reads) with `as_of` = event day and a bounded lag. A key whose inputs are reconstructible but whose owner has **no** replay in 17 §2.11 (07 web, 11 integrity) yields no historical rows and behaves as forward-only until one exists (counted in `n_none_factor`). Forward-only = must be observed live; the harness returns `None` before the first live observation.

| pillar | metric_key | class | reconstruction source | caveat / lag |
|---|---|---|---|---|
| P1 | `emission_share` | reconstructible | TMC `analytics?span=1Y` daily `emission_percent` (365 d), imported ledger | lag 1 d |
| P1 | `gate_margin` | reconstructible (post 2026-07-28 meaningful) | `moving_price` (ledger + OnFinality backfill) + fitted burn | pre-v440 the gate did not exist → regime-tag |
| P1 | `dereg_safety` | reconstructible | moving-price rank + immunity from `network_registered_at` | `NetworkImmunityPeriod` assumed constant 864 000 |
| P1 | `liquidity_depth` | reconstructible | `tao_in_pool` (ledger, OnFinality, TMC daily `subnet_tao`) | — |
| P1 | `exit_capacity` | forward-only | — | TMC `price_after` exists only live; 17 §2.11's replay writes `None` for it (no approximation is produced by any spec) |
| P1 | `net_flow_30d` | reconstructible | TMC daily `total_tao_buy/sell` | — |
| P1 | `chain_buys` | forward-only (2026-10-22, §11 item 14) | — | 05's historical tier writes it `failed/snapshot_historical`, so no replay produces it |
| P1 | `holder_dispersion`, `participation` | forward-only | no history keyless (metagraph via archive RPC is possible but not planned) | — |
| P1 | `miner_burn_ok` | partial (≥ 2026-06-25) | implied burn fit needs emission + predicted share | pre-regime → None |
| P2 | `active_authors_28d, fte_devs_84d, commit_days_90d, bus_factor, release_cadence_90d, review_practice_90d` | reconstructible | GitHub commits/releases/PR reviews carry timestamps (17 `backfill_github`, 06 replay) | **org membership and repo list as of today** (mild look-ahead, flagged `github_org_membership_asof_today`); deleted/private repos invisible; force-pushed history rewritten; `review_practice_90d` is forward-only when 17 ran with `--no-prs` (17 §11 item 15) |
| P2 | `declared_repo_freshness`, `org_freshness` | approx | days since last commit ≤ t (not `pushed_at`) | understates bot pushes (fine) |
| P2 | `repo_hygiene`, `originality` | forward-only (static snapshot only) | file-existence history is costly; `ahead_by` is today-only | — |
| P3 | `site_age` | inputs reconstructible, no replay | Wayback first capture is fixed | 07 is not in 17 §2.11's replay → forward-only in practice |
| P3 | all others | forward-only | — | — |
| P4 | `tenure` | reconstructible | occupant registration time (`v_quant_occupants.registered_ts`); 12's scoring backfill uses 05's `occupant_age_days` fallback (02 §C1) | — |
| P4 | `owner_commitment` | partial, no replay | `chain_daily.owner_personal_locked_alpha` from `coverage.json.reconstructible_from.owner_commitment` (first non-zero owner lock, computed by 17; 11 cites ≥ 2026-05-13) — lock numerator only, no auto-lock/total-stake history | 11 has no replay in 17 §2.11 → rows `None`; use `evidence:owner_locked_share` (05 chain replay) as the lock-history proxy |
| P4 | `control_stability_90d` | partial (replayed 2026-10-22) | owner coldkey/hotkey changes from 17's `rpc_owner_history` (via 11's `owner_rpc_seed` → `owner_changes`), or TaoStats `/v1/subnets/owners` if keyed (02 §G7); no swap-announcement or `PerpetualLockUpdated` history | as `owner_commitment`: forward-only until an 11 replay writes rows |
| P4 | `owner_retention_30d` | forward-only (UNVERIFIED) | only with TaoStats stake-event history (`/v1/subnets/stake-events`) or a proven staking-activity backfill depth (11 §11 items 9, 12) | — |
| P4 | `owner_overhang`, `team_doxxed`, `owner_identity` | forward-only | no keyless history (02 §J2; manual / no identity history) | — |
| P5, P6 | all (incl. `x_engagement_30d`, `polarity_7d`) | forward-only | Discord/X/LLM labels have no honest history; back-applying today's classifier is look-ahead (Look-Ahead-Bench) | **look-ahead even live:** `x_engagement_30d` and `polarity_7d` weight by `social_posts` engagement refreshed in place, so a stored row's inputs change after `as_of` → caveat `x_engagement_refreshed_in_place` (§2.3, 02 §J2) |
| scores | `quality_cg` | reconstructible | from backfilled P1+P2 pillar scores (spec 12 `backfilled = 1` runs) | evidence_class `reconstructed` |
| scores | `quality, composite_raw, composite, composite_smoothed, velocity` | mixed | historically equal to the P1+P2 renormalisation; forward from Phase 8 | first honest full-composite study ≥ 90 d after Phase 9 |

Retired P4 names (`owner_conviction`, `owner_retention`, `identity_stability`, 02 §C2) are rejected as factor names with exit 2 and a pointer to the master name.

`RECONSTRUCTIBLE_KEYS` in `app/quant/backtestability.py` is the machine-readable form of this table (`{key: {"class": …, "lag_s": …, "group": …, "caveats": [...]}}`; `lag_s` = 86 400 for P1/P2/`tenure` keys and for the score keys, whose inputs lag ≤ 1 d) and is what `--pit-mode reconstructed` consults. It also lists the weight-0 raw keys read through `evidence:` whose history 05's chain replay produces — `owner_locked_share` (group `owner_commitment`), `implied_miner_burn` (partial, ≥ 2026-06-25), `dereg_hazard`, `occupant_age_days` (lag 86 400 each); any other `evidence:` key is forward-only. Keys classed `partial … no replay` (`owner_commitment`, `control_stability_90d`) are not reconstructible for the harness until an 11 replay exists. `backtestability.load_coverage(path)` reads 17's `data/quant/coverage.json` (`COVERAGE_PATH`, schema `coverage-1`; any other schema → warning `coverage_schema_unknown`, file ignored):
- `reconstructible_from[key]` (INTEGER unix; master keys plus the groups `price_depth_moving_price`, `github_p2` → every P2 key, `regime`) clamps the earliest admissible `as_of` of backfilled rows for that key in reconstructed mode (earlier rows are dropped and counted); a **`null`** value (17 writes it for `github_p2` and `control_stability_90d` while no history exists) drops every backfilled row of that key (`backfilled_before_reconstructible_from:<key>`);
- `forward_only` (17's list) overrides `RECONSTRUCTIBLE_KEYS`: a listed key is treated as forward-only in reconstructed mode (warning `coverage_forward_only:<key>` when the two disagree — e.g. 17 lists `chain_buys` although `chain_daily.chain_buys` is populated for every row, §11 item 14);
- `holes` with `series = "price_bars_1h"` and `netuids = "all"`: cross-section dates inside a hole are skipped and counted (`coverage.n_hole_dates`); `completeness_by_month < 0.8` months are listed (`coverage.low_completeness_months`);
- `occupancy.unstamped` → `coverage.occupant_coverage` alongside the measured share of bars with `occ` known;
- `caveats` → copied into every result's `provenance.coverage_caveats` (and `github_org_membership_asof_today` into `lookahead_caveats` when a P2 input is used);
- `checks.rpc_golden_block ≠ "pass"` → `--pit-mode reconstructed` prints a warning and adds `rpc_golden_block_failed` to `warnings`.
A missing file leaves strict mode unaffected; reconstructed mode then uses `RECONSTRUCTIBLE_KEYS` alone and adds warning `coverage_json_missing`.

### 2.6 Regimes
Every cross-section date carries `emission_regime` (from `QUANT_EMISSION_REGIME_BOUNDARIES`: `taoflow` 2025-09-22 →, `price_burn` 2026-06-25 →, `gate_v440` 2026-07-28 →; the 2026-06-22 57-subnet emission cut falls inside `price_burn` and is reported as a discontinuity) and a market regime tag = `regime_daily.label` of the row with `day_ts = floor_day(bucket)` and the run's `spec_version` (02 §B8: `TRENDING|DISPERSED|CHOPPY|DEAD|UNKNOWN`, uppercase; a missing row is also tagged `UNKNOWN` and lowers `coverage.regime_coverage`). The tag is diagnostic only (never a factor), so its 15-min `computed_at` offset from the 00:00Z bucket is immaterial; a regime used as a factor would go through §2.1's PIT rule. Results include `by_regime` IC breakdowns; the gate uses pooled statistics (pre-registered) and the per-regime table is diagnostic. Because 17's backfilled rows (`regime_daily.source='backfill'`) read `price_bars` closes while live rows read `chain_hourly.price` (17 §11 item 12, 12 §11 item 10), `by_regime.market_source` reports `{"backfill": n_dates, "live": n_dates}` and the market breakdown is also split at the first `source='live'` day.

### 2.7 The pre-registered gate (master §10, verbatim criteria → machine rule)
| # | master §10 criterion (verbatim) | rule evaluated by `gate.evaluate_gate` | inputs |
|---|---|---|---|
| c1 | Rank-IC > 0 with t ≥ 2 on **both** scopes at the chosen horizon, non-overlapping steps | per scope: `step == horizon_bars` (an overlapping result is refused, `reason: "overlapping_steps"`) and `ic.mean_ic > 0 and ic.t_stat ≥ 2.0`; pass iff both `live` and `survivorship` pass | factor study, label `ret_log_<H>`, no controls |
| c2 | IC survives `--controls size,emission_share` (else it is a size proxy → NO-GO) | the c1 rule on the study run with `controls = [size, chain:emission_share]`, both scopes | controlled factor study |
| c3 | Monotone-ish decile curve, holds net-of-cost at `trade_tao = 2` with the ported AMM economics | per scope: `monotone_score(deciles.decile_curve) ≥ 0.6` **and** `deciles_net.mean_long_short > 0` **and** `deciles_net.spread_t_stat ≥ 2.0`, `trade_tao = 2`, `EconomicsConfig(enabled=True, fee_rate=0.003, impact_scale=1.0, carry_model="off")`; both scopes (pre-registered operationalisation, §11 item 3) | c1's study |
| c4 | DSR > 0.95 (necessary, not sufficient; log every variant tried as N) | per scope: `deflate_factors(..., n_trials=N, trial_sharpe_variance=V, orient=False).dsr > 0.95`; N = distinct trials in `trials.json`, V = sample variance of `abs(ic_ir)` over their results (§8.5); `N < 2` → `pass: false, reason: "trial_log_too_small"` (with one trial SR0 = 0 and DSR degenerates to PSR); both scopes (pre-registered) | c1's study + trial log |
| c5 | Walk-forward with ≥ 4 OOS folds: pooled OOS IC > 0, \|t\| ≥ 2, decay ratio ≥ 0.25 | per scope, `n_oos_folds` = folds with `n_oos_dates ≥ 1`; a scope with `n_oos_folds < 4` is `advisory_only` (neither passes nor fails); a scope with ≥ 4 passes iff `pooled_oos.mean_ic > 0 and abs(pooled_oos.t_stat) ≥ 2 and decay_ratio["mean_ic"] ≥ 0.25` (non-finite decay → fail). c5 passes iff ≥ 1 scope is non-advisory and every non-advisory scope passes. Single factor (factor-decay mode), expanding, test window `QUANT_WF_TEST_DAYS` (28 d), label `ret_log` | walk-forward |
| c6 | Survival route (alternative to 1-5 for Health): bottom-tercile 90-d death/zero-emission rate ≥ 2× top tercile with n ≥ 10 events, both scopes | Health-type factors only (`quality`, `quality_cg`, `P1`–`P5`; else `pass: null, reason: "not_health"`); per scope `n_events ≥ 10 and ratio ≥ 2.0`, where ratio = +inf (top tercile 0 events, bottom > 0) counts as ≥ 2; label `death_90d`, `step_days = 90`, `min_depth = 0`; both scopes | survival study |
| c7 | ≥ 30 days of `observe`-mode telemetry in the bot with live signal coverage > 60 % | from the telemetry JSON (`--observe-telemetry`, default `QUANT_OBSERVE_TELEMETRY_PATH`): `days ≥ 30 and entry_coverage > 0.60 and n_entries ≥ 20` (15's key names; `live_signal_coverage` accepted as alias of `entry_coverage`; the `n_entries` floor is 15's review addition — coverage over fewer than 20 observed entries is not evidence); file absent or `n_entries` missing → `pass: null` with `reason`; a `schema` key other than `sentinel-telemetry-1` → `pass: null`, `reason: "telemetry_schema_unknown"`; the per-entry `entries[]` (decided/applied factors) is not scored, only its length is checked against `n_entries` (mismatch → warning) | spec 15 §2.7 export (`python -m app.sentinel.telemetry export`, schema `sentinel-telemetry-1`: `{since, days, n_entries, entry_coverage, entries[…]}`) — a JSON file, never the bot's SQLite DB |

`gate_pass_return_route = c1 ∧ c2 ∧ c3 ∧ c4 ∧ c5`; `gate_pass_survival_route = c6` (Health-type factors only); `gate_pass = (return_route ∨ survival_route) ∧ c7 == true`. A pending c7 makes `gate_pass = false` with `pending: ["c7"]`. DSR is necessary, not sufficient: it only ever enters through the conjunction. The gate horizon is fixed *before* the run in config (`QUANT_GATE_HORIZON_DAYS = 7` → 7 bars on 24 h, 42 bars on 4 h; the walk-forward test window is 28 d regardless). Changing the horizon after seeing results is a new trial and is logged as such. For 15's ladder the verdict also carries the Phase-E **upsize criterion** as a machine-readable object `criteria.u1_upsize` = `{pass, rule, live: {n_oos_folds, pooled_oos_t}, survivorship: {…}, survival_route_pass}` with `upsize_gate_pass = u1.pass = gate_pass ∧ c6.pass ∧ (some c5 scope with n_oos_folds ≥ 4 has pooled OOS IC > 0 and t ≥ 3)` (15 §8 row E: pooled OOS IC t ≥ 3 with ≥ 4 folds **and** survival route pass; the upsize verdict never bypasses the base gate), and `promotable = gate_pass ∧ factor ∈ {quality, composite_smoothed}` — the series 15's feed serves (12 §2.15 item 8); a `quality_cg` verdict is research evidence, never promotable (§11 item 11).

---

## 3. Data contract

All reads are against a **snapshot** (§7), never the live writer. Names are 03's canonical DDL and views (02 §A2/§B7/§J2); this spec ships no DDL. Chain amounts are read only through the `v_quant_*` views, which already convert rao → τ (03 §3.7). All times are INTEGER unix seconds (02 §B1).

### 3.1 Tables read
| relation (03) | columns read (exact names) | use |
|---|---|---|
| `v_quant_price_bars` (view over `price_bars`) | `netuid, occupant_key, period, bucket_start, ts_last, close, volume, tao_in_pool (τ), moving_price, block_number, source, observed_at, backfilled`; `WHERE period = '1h'` | price and entry instant per bar; depth/moving price when the source carries them (`ledger_import`, `onfinality`, `opentensor_archive`); `tmc_candle` rows have NULL `ts_last`/reserves/`moving_price`; `source`/`backfilled` feed the volume rule and provenance |
| `v_quant_chain_hourly` (view over `chain_hourly`) | `netuid, occupant_key, hour, block_number, tao_in_pool (= tao_liquidity/1e9), moving_price (= subnet_moving_price), observed_at, backfilled, source` | depth/moving-price fallback on both grids (its `volume` = `volume_24h` is never read) |
| `v_quant_chain_daily` (view over `chain_daily` ∪ `rpc_subnet_daily` day rows, migration 0005) | `occupant_key, netuid, day (= day_ts), ts (= event_at), block_number, emission_share (= emission_percent, %; the RPC share `100·tao_in_emission/Σ` for occupant-days without a `chain_daily` row and, before 2025-11-17, in place of TMC's whole-percent-truncated value), tao_in_pool (= subnet_tao/1e9; NULL on RPC-only rows), observed_at, backfilled (1 on RPC-only rows), source (`analytics_1y`/`analytics_1w`, or the RPC endpoint)` | `demis_*`, zero-emission runs, `gate_cross_28d`, 24 h depth fallback, block↔time anchors, `label_end` |
| `v_quant_occupants` (view over `occupants`) | `occupant_key, netuid, network_registered_at (block), registered_ts (= COALESCE(exact, 04 estimate)), registered_ts_exact, first_seen_at, last_seen_at, status, deregistered_at, deregistered_ts` | `OccupantIndex`, death onsets, `age` |
| `block_times` (17) | `block_number, event_at` | exact anchors for `block_to_unix` |
| `events` | `occupant_key, event_type, event_at, observed_at, payload` for `event_type IN ('netuid_reoccupied','emission_disabled')`; the dead occupant of `netuid_reoccupied` is `payload.old_occupant_key` (04 live) or `payload.old_key` (17 backfill key set) — the row's own `occupant_key` is the successor (03 §3.9) | death onsets for `death_90d`/`demis_*` |
| `metric_values` | `occupant_key, metric_key, as_of, spec_version, stage, value, value_json, status (generated), observed_at, source, backfilled`; factors read `stage='norm'` (`value` = m, `value_json.raw`, `value_json.status`), `evidence:` keys read `stage='raw'` | PIT metric factors, controls |
| `scores` | `occupant_key, as_of (slot instant), spec_version, run_kind, composite_raw, composite, composite_smoothed, quality, velocity, pillar_scores (JSON), confidence, computed_at, input_cutoff, backfilled, shadow` | PIT score factors; `shadow = 0` when `spec_version = SPEC_VERSION` (the primary series); no shadow filter for a non-primary spec (rows are unique per `(occupant_key, as_of, spec_version)`, 02 §B6) |
| `regime_daily` | `day_ts, spec_version, label, source, backfilled, computed_at` | regime tagging (§2.6) |
| `post_labels` | `MAX(classifier_version)` | provenance only (stored in the reduced snapshot's `quant_snapshot_meta`) |
| `schema_version` | `MAX(version)` | recorded in results |

Freshness windows (`max_age`) come from spec 12's metric registry (`app/features/registry.py: METRICS[key].freshness_s`); the fallback when the registry (built in Phase 8) or the key is missing is 02 §C6, resolved through 03's catalog `app/storage/catalog.py: METRIC_KEYS[key]` by producer: `team_doxxed` 90 d; source `chain` 1 d; `github` 2 d; `web` 2 d; `community` 2 d; `x` 3 d; `llm` (P6) 1 d; `scoring` (12-derived P5 combiners) 1 d; `integrity` (other P4 — `owner_identity`, S1–S5, `tenure`) 2 d; anything else `QUANT_PIT_MAX_AGE_DEFAULT_S`. Scores use `QUANT_SCORE_MAX_AGE_S = 129600` (36 h). The harness reads them; it does not define them.

### 3.2 Tables written
| table | rows | by |
|---|---|---|
| `collector_runs` (03 §3.1) | `run_id, job='quant_weekly', started_at, finished_at, ok, rows (= result files written), error, cost_usd=0, duration_sec, note ("gate_pass=<bool> verdict=<file>"), meta_json ({verdict_file, gate_pass, n_results, snapshot, exit_codes})` | the 01 wrapper around the optional weekly job only |

No `metric_values`, `scores` or `events` rows are ever written by this spec; 02 §B9's `source='quant'` is reserved for it should a later version write any. Results are files under `QUANT_RESULTS_DIR`.

### 3.3 metric_keys produced
None. The harness produces no `ts-1.0` metrics; it consumes them. (Rank-stability numbers for live monitoring are spec 12 §2.12's `rank_stability_daily` rows, step `rank_autocorr_health`; the CLI here is the offline, snapshot-based version of the same statistics — §8.5.)

### 3.4 Files written
| path | shape |
|---|---|
| `data/quant/results/factor_<scope>_<YYYYMMDD_HHMMSS>.json` | old explorer shape (§3.5) + new keys |
| `data/quant/results/ic_series_<scope>_<ts>.json` | `{factor: {horizon: [per-date IC…]}, "dates": {factor: {horizon: [bucket…]}}}` sidecar (the old `_kind_for` skips this name) |
| `data/quant/results/walk_forward_<scope>_<ts>.json` | `{timestamp, reports:[WalkForwardReport…]}` (ported `save_walk_forward_json`, `walk_forward.py:588-604`) + `n_oos_folds` per report + `provenance` |
| `data/quant/results/survival_<scope>_<ts>.json` | `SurvivalResult` asdict (§8.5) + `provenance` |
| `data/quant/results/rank_stability_<scope>_<ts>.json` | `RankStabilityResult` asdict (§8.5) + `provenance` |
| `data/quant/results/gate_verdict_<factor>_<ts>.json` | §3.6 (file name as 15 §2.7 expects) |
| `data/quant/results/trials.json` | `{"n_trials": int, "trial_sharpe_var": float\|null, "updated_at": int, "entries": [{trial_key, ts, command, factor, label, horizon, scope, controls, lookback, min_depth, max_depth, step, pit_mode, start, end, spec_version, git_sha, db_sha256, results: {ic_ir, n_dates} \| null}]}` — rewritten atomically on every change under an exclusive `fcntl.flock` on `trials.json.lock` (the weekly job and an operator CLI can run concurrently), entries never removed or edited except filling `results` once; `trial_key` = sha1 of the canonical JSON of `(factor, label, horizon, scope, controls, lookback, min_depth, max_depth, step, pit_mode, start, end, spec_version)`, where `start`/`end` are the flags **as given** (`null` when defaulted to the scope start / snapshot end) — a pre-registered study re-run on a newer snapshot (the weekly job) is the *same* hypothesis, not a new variant, and must not inflate N week after week (`db_sha256` distinguishes the runs; each appends an entry); `n_trials` = count of **distinct** `trial_key`s ever started (a crashed run still counts); `trial_sharpe_var` = sample variance (ddof = 1) of `abs(ic_ir)` over the newest result per distinct key (null when < 2) |
| `data/quant/snapshots/quant_<source-stamp>.db` | reduced snapshot (§8.1 `snapshot.py`), `<source-stamp>` = the backup's `taosentinel-<stamp>`; keep the newest `QUANT_SNAPSHOT_KEEP` (2) |
| `data/quant/snapshots/quant_<source-stamp>.coverage.json` | byte copy of `COVERAGE_PATH` taken with the snapshot (its sha256 goes into provenance) |

### 3.5 Factor result JSON (old explorer shape, keys preserved exactly)
```json
{
  "timestamp": "2026-10-04T03:10:22Z", "scope": "survivorship", "grid_seconds": 86400,
  "span": "2025-09-22 → 2026-10-03", "n_netuids": 131, "n_bars": 41210,
  "min_depth_tao": 3000.0, "lookback": 6, "insufficient_data": false, "warnings": ["coverage_json_missing"],
  "coverage": {"regime_coverage": 0.99, "occupant_coverage": 0.97, "n_hole_dates": 12, "low_completeness_months": ["2026-04"]},
  "factors": { "quality_cg": { "7": {
      "factor": "quality_cg", "horizon": 7, "lookback": 6, "min_depth": 3000.0, "step": 7,
      "ic": {"n_dates": 52, "mean_ic": 0.041, "std_ic": 0.19, "ic_ir": 0.21, "t_stat": 1.55,
             "pct_positive": 0.58, "avg_names": 61.2},
      "deciles": {"n_buckets": 10, "decile_curve": [ -0.031, …, 0.012 ], "mean_long_short": 0.043,
                  "spread_t_stat": 1.7, "n_spread_dates": 52, "bottom_decile_mean": -0.031, "top_decile_mean": 0.012},
      "median_gap_seconds": 604800.0,
      "deciles_net": { "…same keys…" }, "trade_tao": 2.0, "max_depth": null,
      "label": "ret_log_7", "pit_mode": "reconstructed", "controls": [], "overlapping": false, "monotone_score": 0.52,
      "by_regime": {"emission": {"taoflow": {"n_dates": 39, "mean_ic": 0.05, "t_stat": 1.6}, "price_burn": {…}, "gate_v440": {…}},
                    "market": {"TRENDING": {…}, "DISPERSED": {…}, "CHOPPY": {…}, "DEAD": {…}, "UNKNOWN": {…}},
                    "market_source": {"backfill": 40, "live": 12}},
      "n_none_factor": 1210, "n_none_label": 388, "n_dropped_controls": 0, "n_hole_dates": 0
  }}},
  "deflated": {"quality_cg": {"sharpe": 0.21, "skew": 0.1, "kurtosis": 2.9, "n_trades": 52, "n_trials": 37, "sr0": 0.31, "psr_zero": 0.93, "dsr": 0.71,
               "trial_sharpe_var": 0.04, "orient": false}},
  "provenance": {"spec_version": "ts-1.0", "classifier_version": "none", "git_sha": "3f9c1e2", "schema_version": 1,
                 "seed": 0, "pit_mode": "reconstructed",
                 "db_snapshot": {"path": "data/quant/snapshots/quant_taosentinel-20261004T033012Z.db", "sha256": "…", "size_bytes": 812345678,
                                 "source_backup": "data/backups/taosentinel-20261004T033012Z.db", "max_observed_at": 1791000000, "made_at": 1791005000},
                 "label_end": 1790985600,
                 "coverage_json": {"path": "…/quant_taosentinel-20261004T033012Z.coverage.json", "sha256": "…", "schema": "coverage-1", "generated_at": 1790980000},
                 "coverage_caveats": ["github_org_membership_asof_today", "subnet_volume_unit_unverified", "moving_price_enriched_rows"],
                 "price_sources": {"ledger_import": 31000, "onfinality": 6000, "tmc_candle": 4210}, "price_bars_backfilled_frac": 0.93,
                 "shadow_rows": 0,
                 "economics": {"enabled": true, "fee_rate": 0.003, "impact_scale": 1.0, "carry_model": "off", "carry_apr": 0.0},
                 "lookahead_caveats": ["github_org_membership_asof_today"], "evidence_class": "reconstructed",
                 "command": "python -m app.quant factor --factors quality_cg --horizons 7,28,84 --scope survivorship --db …"}
}
```
Field notes: `timestamp` is `to_iso(now_ts())` (02 §B1 boundary format); every other time in result files is INTEGER unix s. `insufficient_data` = any (factor, horizon) with `ic.n_dates < QUANT_MIN_IC_DATES` (10 non-overlapping IC dates; the earlier "fewer than `2·horizon` usable dates" could never hold for an 84-bar horizon with step 84 — a 390-date panel yields ≈ 4 dates — so every default run would have been flagged); `warnings` is the list of counted warning codes (`coverage_json_missing`, `coverage_schema_unknown`, `rpc_golden_block_failed`, `forward_only_key_dropped:<key>`, `backfilled_before_reconstructible_from:<key>`, `regime_missing`); `overlapping` = `step < horizon` (such results are never accepted by the gate); `monotone_score` = `mathutil.monotone_score(deciles.decile_curve)`; `n_hole_dates` = dates skipped inside `coverage.json` holes. `db_snapshot.max_observed_at` = the greatest `observed_at` over `v_quant_price_bars`, `v_quant_chain_daily` and `metric_values` in the snapshot; `made_at` from `quant_snapshot_meta`. `evidence_class ∈ {chain_state, strict, reconstructed}`: `chain_state` when no PIT row was used (chain factors only), `reconstructed` when any `backfilled = 1` row entered, else `strict`. `deflated` is present only with `--deflate` (always in `report`); `trial_sharpe_var` and `orient` are the DSR inputs of §8.5. Non-finite floats are written as `null` (`json.dump(..., allow_nan=False)` after sanitising); the fields `_rows_from_factor` casts with `float()` (`ic.*`, `deciles.mean_long_short`, `deciles.spread_t_stat`, `lookback`, `min_depth`) are finite by construction (ported zero defaults) and never null. `ic_series` is dropped from JSON (ported behaviour) but written to the `ic_series_<scope>_<ts>.json` sidecar (§3.4) so DSR/CI can be recomputed without re-running. `_rows_from_factor` (`backtest_api.py:173-195`) parses this unchanged.

### 3.6 Gate-verdict JSON (`gate_verdict_<factor>_<ts>.json`)
```json
{"spec_version": "ts-1.0", "factor": "quality_cg", "label": "ret_log_7", "gate_horizon_days": 7,
 "horizon_bars": {"survivorship": 7, "live": 42}, "generated_at": 1791005400, "provenance": {"…as §3.5…"},
 "criteria": {
  "c1_ic_both_scopes":  {"pass": false, "rule": "step==horizon and mean_ic>0 and t>=2 on both scopes", "reason": null,
                         "live": {"mean_ic": 0.02, "t_stat": 0.9, "n_dates": 9, "step": 42}, "survivorship": {"mean_ic": 0.041, "t_stat": 1.55, "n_dates": 52, "step": 7}},
  "c2_survives_controls": {"pass": false, "controls": ["size", "chain:emission_share"], "live": {…}, "survivorship": {…}},
  "c3_deciles_net": {"pass": false, "trade_tao": 2.0, "live": {"monotone_score": 0.3, "net_long_short": -0.01, "net_spread_t": -0.4}, "survivorship": {…}},
  "c4_dsr": {"pass": false, "n_trials": 37, "trial_sharpe_var": 0.04, "live": {"dsr": 0.55, "sr0": 0.31}, "survivorship": {"dsr": 0.71, "sr0": 0.31}},
  "c5_walk_forward": {"pass": false, "test_days": 28, "live": {"n_folds": 3, "n_oos_folds": 2, "advisory_only": true},
                      "survivorship": {"n_folds": 7, "n_oos_folds": 6, "advisory_only": false, "pooled_oos_mean_ic": 0.03, "pooled_oos_t": 1.2, "decay_ratio_mean_ic": 0.6}},
  "c6_survival": {"pass": false, "horizon_days": 90, "live": {"n_events": 3, "n_event_dates": 2, "ratio": null, "ratio_inf": false},
                  "survivorship": {"n_events": 6, "n_event_dates": 5, "ratio": 2.5, "ratio_inf": false}},
  "c7_observe_telemetry": {"pass": null, "reason": "pending: telemetry file absent (data/quant/observe_telemetry.json)", "days": null, "entry_coverage": null, "n_entries": null},
  "u1_upsize": {"pass": false, "rule": "gate_pass and c6 and pooled OOS IC>0 with t>=3 on a scope with n_oos_folds>=4",
                "live": {"n_oos_folds": 2, "pooled_oos_t": null}, "survivorship": {"n_oos_folds": 6, "pooled_oos_t": 1.2}, "survival_route_pass": false}},
 "gate_pass_return_route": false, "gate_pass_survival_route": false, "gate_pass": false, "pending": ["c7"],
 "upsize_gate_pass": false, "promotable": false,
 "checks": {"rank_ic_both_scopes": false, "size_control": false, "decile_monotone_net": false, "dsr": false,
            "walk_forward": false, "survival_route": false, "observe_telemetry_30d": null},
 "observe_telemetry": null,
 "evidence_class": "reconstructed", "lookahead_caveats": ["github_org_membership_asof_today"],
 "trials": {"n_trials": 37, "trial_sharpe_var": 0.04, "trials_file": "data/quant/results/trials.json"}}
```
Every criterion object has `pass (bool|null), rule, reason (str|null), live, survivorship` (c7 and u1 carry their own fields); `ratio: null` with `ratio_inf: true` encodes +inf (JSON has no infinity). `u1_upsize` is not part of `gate_pass`; `upsize_gate_pass` mirrors `u1_upsize.pass`. `generated_at` is INTEGER unix s (15 §2.7). `checks{…}` is 15 §2.7's boolean view of c1…c7 (`survival_route` = `gate_pass_survival_route`), and `observe_telemetry` echoes the telemetry file `{since, days, entry_coverage, n_entries}` (null when absent); 15's loader tolerates the extra keys.

### 3.7 Events emitted
None. There is no catalogued `quant_*` event type (03 §3.9, 02 §D1) and only 14's `alert_dispatch` sends Telegram messages from `events` rows (02 §D3), so the weekly job sends no digest of its own: its outcome is `collector_runs.note/meta_json` and `/health.collectors.quant_weekly.{last_gate_pass,last_verdict_file}` (§7); a weekly line in 16's `ops_digest_daily` is a request to 16 (§11 item 12). Failures page through 01's wrapper latch `collector_failing:quant_weekly` (catalogued; payload `collector, consecutive_failures, last_error`) after `QUANT_ALERT_AFTER_FAILURES` consecutive failures (§8.8), recovery through `collector_recovered`.

---

## 4. None-vs-value semantics (absence vs failure, adapted for a consumer spec)

| quantity | real value (0.0 / finite) when | `None` when | note |
|---|---|---|---|
| `Bar.price` | source `close > 0` | bar dropped entirely if `close ≤ 0` (never a reset trigger) | ported `factor_panel.py:150-153` |
| `Bar.depth` | reserve observed in the bucket (`v_quant_price_bars.tao_in_pool` → `v_quant_chain_hourly` → `v_quant_chain_daily` on the 24 h grid; all τ) | stored as `0.0` (not None) → fails `min_depth`, excluded from cross-sections but kept in the series so forward returns and segmentation stay intact | ported semantics |
| `Bar.moving_price`, `Bar.volume` | observed (volume only from the Δ-`subnet_volume` sources, §2.3) | `0.0` sentinel → dependent factors return `None` (`px_vs_moving`, `moving_px_trend`, `volume*`) | ported |
| `Bar.occ` | occupant known at `ts` | `None` → all PIT factors `None`, `age` `None` | unknown ≠ dead |
| PIT factor value | row with `eff ≤ bucket` and `bucket − as_of ≤ max_age` (§2.1) | unobserved, stale, `eff > bucket`, backfilled in `strict` mode, forward-only key or `as_of` before `reconstructible_from` in reconstructed mode, provisional row on the 24 h grid | a `stage='norm'` `value = 0.0` (verifiable absence, raw `status='absent'`) is a real 0.0; a `NULL` value (raw `failed`/`none`/stale) is skipped as if unobserved (the query filters `value IS NOT NULL`, 12 §2.15 item 3) |
| `quality_cg` | at least one of P1/P2 non-None | both None | |
| `ret_log_h` | in-segment or death realised | alive and unobservable | death = `log(0.01)` |
| `demis_d` | share at both ends or death | `t+d` beyond data and alive | |
| `death_90d` | window fully inside data (0) or death onset inside window (1) | right-censored | never assume survival |
| `gate_cross_28d` | ranks at both ends or death | rank at `t+28` unavailable | |
| control value | non-None | any control None → name dropped and counted in `n_dropped_controls` | |
| market regime tag | `regime_daily.label` for `day_ts = floor_day(bucket)` | tag `"UNKNOWN"` (02 §B8 vocabulary; a string, not None) | |
| survival `ratio` | both terciles observed | `null`; +inf is `null` + `ratio_inf: true`; 0/0 is `0.0` (ported `death_ratio_low_over_high`) | |
| IC per date | `≥ min_names` (10) names | date skipped; `spearman_r` returns 0.0 below 5 names — the harness asserts `min_names ≥ 10` | gotcha from research |

---

## 5. Configuration

`Settings(BaseSettings)` fields (spec 01 conventions; `extra="ignore"`; every consumer reads `app.config.settings` dynamically). CLI flags override settings for one run; settings never override CLI flags.

| key | type | default | validator / notes |
|---|---|---|---|
| `QUANT_ENABLED` | bool | `True` | `False` → every CLI subcommand exits 3 with `quant disabled`; the weekly job's `enabled()` is false (skip reason `disabled`) |
| `QUANT_RESULTS_DIR` | str | `data/quant/results` | created on first write |
| `QUANT_SNAPSHOT_DIR` | str | `data/quant/snapshots` | |
| `QUANT_SNAPSHOT_KEEP` | int | `2` | ≥ 1; `snapshot --prune` keeps this many newest snapshots |
| `QUANT_DB_PATH` | str | `""` | default for `--db`; empty → newest `QUANT_SNAPSHOT_DIR/quant_*.db`, else the newest **uncompressed** backup `DB_BACKUP_DIR/taosentinel-<stamp>.db` (16; lexicographic stamp order; tagged names `taosentinel-<stamp>-<tag>.db` included; never `*.db.tmp`, `*.db.gz` or `manual/`; a stamp whose manifest `taosentinel-<stamp>[-tag].json` reports `quick_check ≠ "ok"` is skipped — the key 16's `create_backup` writes, verified on the 2026-10-17 manifest; no manifest → accepted, since the `.db` name appears only after its atomic rename); must never be `DB_PATH` (`os.path.samefile` at use time, 02 §J2) |
| `QUANT_OBSERVE_TELEMETRY_PATH` | str | `data/quant/observe_telemetry.json` | c7 input: 15 §2.7's `sentinel-telemetry-1` JSON `{since, days, n_entries, entry_coverage, entries[…]}` exported from the bot |
| `QUANT_OBSERVE_MIN_DAYS` / `QUANT_OBSERVE_MIN_COVERAGE` / `QUANT_OBSERVE_MIN_ENTRIES` | int / float / int | `30` / `0.60` / `20` | c7 thresholds (master §10 item 7 + 15's `n_entries` floor); coverage is strict `>` |
| `QUANT_UPSIZE_MIN_T` | float | `3.0` | u1 pooled OOS t floor (15 §8 row E) |
| `QUANT_ZERO_RUN_DAYS` | int | `7` | ≥ 1; consecutive daily rows for a zero-emission run |
| `QUANT_ZERO_EMISSION_EPS_PCT` | float | `1e-4` | `emission_share ≤ eps` (percentage points) counts as zero; avoids float noise while staying far below the deep-tail gated shares |
| `QUANT_SEED` | int | `0` | bootstrap/DSR RNG seed |
| `QUANT_SPEC_VERSION` | str | `ts-1.0` | filter on `metric_values.spec_version`/`scores.spec_version` |
| `QUANT_PIT_MODE` | str | `strict` | `∈ {strict, reconstructed}` |
| `QUANT_RESET_DROP_RATIO` | float | `0.30` | `0 < x < 1`; the single slot-reset threshold (same value as `DeregRiskConfig.reset_drop_ratio` in spec 05; defined here as a literal, not imported) |
| `QUANT_SURVIVORSHIP_START` | str (date) | `2025-09-22` | ISO date, UTC |
| `QUANT_LIVE_ERA_START` | str (date) | `2026-04-22` | |
| `QUANT_GRID_SURVIVORSHIP_S` | int | `86400` | multiple of 3600 |
| `QUANT_GRID_LIVE_S` | int | `14400` | |
| `QUANT_HORIZONS_24H` | str | `7,28,84` | comma ints ≥ 1 |
| `QUANT_HORIZONS_4H` | str | `6,12,42` | |
| `QUANT_LOOKBACK_BARS` | int | `6` | ≥ 1 |
| `QUANT_MIN_DEPTH_TAO` | float | `3000.0` | ≥ 0 |
| `QUANT_MIN_NAMES` | int | `10` | **≥ 10 enforced** |
| `QUANT_MIN_IC_DATES` | int | `10` | ≥ 2; a result with fewer IC dates is `insufficient_data` (§3.5) |
| `QUANT_CONTROLS` | str | `size,emission_share` | names resolvable by the factor registry (`emission_share` → `chain:emission_share`, §2.3) |
| `QUANT_TRADE_TAO` | float | `2.0` | ≥ 0; `0` (or `--trade-tao 0`) disables the net deciles |
| `QUANT_FEE_RATE` | float | `0.003` | `EconomicsConfig.fee_rate` |
| `QUANT_IMPACT_SCALE` | float | `1.0` | |
| `QUANT_DEATH_FLOOR_RETURN` | float | `0.01` | `0 < x < 1` |
| `QUANT_SCORE_MAX_AGE_S` | int | `129600` | 36 h |
| `QUANT_PIT_MAX_AGE_DEFAULT_S` | int | `172800` | used when the registry has no freshness for a key |
| `QUANT_GATE_FACTOR` | str | `quality_cg` | pre-registered factor for `report` |
| `QUANT_GATE_LABEL` | str | `ret_log` | label family for c1-c5 |
| `QUANT_GATE_HORIZON_DAYS` | int | `7` | converted to bars per grid; must be in the horizon list for that grid |
| `QUANT_WF_TEST_DAYS` | int | `28` | walk-forward test window |
| `QUANT_WF_TRAIN_MIN_FRACTION` | float | `0.5` | `train_min = max(int(len(dates)·frac), 4·horizon_bars)` |
| `QUANT_MIN_FOLDS` | int | `4` | `MIN_FOLDS_TO_CERTIFY` |
| `QUANT_DSR_THRESHOLD` | float | `0.95` | |
| `QUANT_DECAY_MIN` | float | `0.25` | |
| `QUANT_MONOTONE_MIN` | float | `0.6` | |
| `QUANT_SURVIVAL_HORIZON_DAYS` | int | `90` | |
| `QUANT_SURVIVAL_MIN_EVENTS` | int | `10` | |
| `QUANT_SURVIVAL_RATIO_MIN` | float | `2.0` | |
| `QUANT_GATE_TOP_N` | int | `32` | top-N for `gate_cross` |
| `QUANT_GATE_CROSS_DAYS` | int | `28` | |
| `QUANT_EMISSION_REGIME_BOUNDARIES` | str | `taoflow:2025-09-22,price_burn:2026-06-25,gate_v440:2026-07-28` | `name:date` pairs, ascending |
| `QUANT_WEEKLY_JOB_ENABLED` | bool | `False` | enabled flag of `quant_weekly` (01 registers every built job; the wrapper re-checks the flag each run) |
| `QUANT_WEEKLY_JOB_CRON` | str | `sun 05:00` | 02 §F2/§E3; `<dow> HH:MM` UTC → `CronTrigger(day_of_week, hour, minute, timezone=UTC)`; after the 03:30Z backup and the 04:30Z reboot window |
| `QUANT_WEEKLY_JOB_TIMEOUT_S` | int | `1800` | the job's `timeout_sec`; the child process is killed and the run marked failed beyond this |
| `QUANT_ALERT_AFTER_FAILURES` | int | `2` | the job's `fail_alert_threshold` (§8.8): consecutive failures before 01's `collector_failing:quant_weekly` latch fires (01's global `JOB_FAIL_ALERT_THRESHOLD` = 5 would mean 5 weeks) |

`COVERAGE_PATH` (17's key, default `data/quant/coverage.json`) is read, not redefined. `QUANT_KILL_SENTINEL` of the earlier draft is **retired**: 02 §B12's sentinel list is closed.

Pause semantics (02 §B12): the weekly job is paused only through `control_state` (target `quant_weekly`; API `POST /api/v1/control/pause/quant_weekly`, Telegram or CLI — `resume` deletes the row), by `./PAUSE_ALL` (it is not `ops_exempt`), or by the emergency file `data/control/quant_weekly.paused` (read, never written). `/health` shows the reason in 02 §B12's vocabulary (`operator | pause_all | emergency_file`). `./KILL_SWITCH` does not affect it (not a paid job) and neither does `./SCORING_KILLED` (it reads stored scores). Pauses never affect the operator-invoked CLI. `QUANT_ENABLED=False` is the hard off.

---

## 6. Cadence, rate limits, cost

The harness performs **zero network calls**; no provider token bucket is touched and `provider_spend` is never written. Budget math per provider: TMC 0 req, GitHub 0 req, RPC 0 req, LLM $0, X $0.

CPU/IO budget (Ubuntu laptop, single core, pure Python):
| step | size | expected time |
|---|---|---|
| snapshot copy (11 relations of §8.1 from the newest uncompressed backup) | `metric_values` norm official ≈ 45 keys × 130 occ × 365 d ≈ 2.1 M rows (+ 14 d provisional, + evidence raw rows); `v_quant_price_bars` 1 h ≈ 130 × 8 760 ≈ 1.1 M rows/yr; `v_quant_chain_hourly` ≈ 1.1 M rows/yr | 1-3 min (UNVERIFIED, measure on the laptop) |
| `load_factor_panel` (both scopes, 10 PIT keys) | ~40 k bars (24 h), ~60 k bars (4 h) | 10-30 s |
| `run_factor_study` 5 factors × 3 horizons × 2 controls | ~200 dates × 60 names | 5-20 s |
| `deflate_factors`, bootstrap 2 000 resamples | trivial | < 5 s |
| walk-forward (6 folds, re-run study per fold) | | 20-60 s |
| `report --run-all` (both scopes, all studies) | | 2-5 min |

Weekly job: `QUANT_WEEKLY_JOB_CRON` = `sun 05:00` UTC (02 §E3: after `storage_retention_daily` 03:00Z, `backup_daily` 03:30Z and the 04:30Z reboot window), `max_instances=1`, `coalesce=True`, misfire grace 3600 s, wall-clock cap `QUANT_WEEKLY_JOB_TIMEOUT_S`. The pipeline runs in a **child process** (`nice -n 10 python -m app.quant …`, §8.8), not in a thread: the harness is pure-Python CPU work that would hold the GIL and starve the asyncio loop (SSE recorder, API) no matter what `os.nice` says, and on Linux `os.nice` in a worker thread renices only that pooled thread, which other `to_thread` work later reuses. The child shares the unit's cgroup: peak RSS ≈ 0.5–1 GB on a year-1 snapshot (UNVERIFIED estimate: ≈ 100 k `Bar`s with `pit` dicts + ≈ 2 M PIT tuples), so enabling the job needs `MemoryMax=3G` or running the CLI outside the unit (16 §2.4, §11 item 16); under cgroup OOM the kernel kills the child, not the service. Disk: results ≤ 300 KB per run (keep all); snapshots ≈ 0.3-1 GB each, keep the newest `QUANT_SNAPSHOT_KEEP` (2; `snapshot --prune`). Liveness (16 §2.5): a weekly cron job's `interval_min()` = 10 080 → max-age = 10 080·60·2 + 1 800 + 120 = 1 211 520 s.

---

## 7. Failure modes and fail-safe behaviour

| failure | behaviour |
|---|---|
| `--db`/`--from` resolves to `DB_PATH` (samefile) or a path with a non-empty `-wal` sibling being written | refuse, exit 2, message `refusing to read the live writer DB; run 'python -m app.quant snapshot' first` (02 §J2) |
| `--db`/`--from` is a `*.tmp` (backup in progress) or `*.gz` file | refuse, exit 2 (`backup not finished` / `decompress first or pick the uncompressed newest backup`) |
| no snapshot/backup found | exit 2 with the exact paths searched |
| snapshot lacks a required relation/column of §3.1 (`PRAGMA table_info` on the view or materialised table) | exit 2 listing missing columns (so 03 can be reconciled) |
| `coverage.json` missing / unknown schema / `rpc_golden_block ≠ pass` | warning codes of §3.5; strict mode unaffected |
| `regime_daily` empty | tag `UNKNOWN` for all dates; `coverage.regime_coverage: 0.0`, warning `regime_missing` |
| `v_quant_occupants` empty for a netuid | `occ = None` bars (chain factors still run); `coverage.occupant_coverage` reports it |
| retired or non-scored factor name (`owner_conviction`, a leg, …) | exit 2 naming the master key or the `evidence:` prefix |
| fewer than `QUANT_MIN_IC_DATES` IC dates | study runs, result carries `insufficient_data: true`; gate criteria evaluate `false` with `reason` |
| walk-forward `< QUANT_MIN_FOLDS` folds | report `oos_credible=false, gate_note="insufficient folds … advisory only"` (ported `_factor_gate`) |
| survival `n_events < 10` | `ratio` still reported, criterion `false`, `reason: "n_events<10"` |
| `spearman_r` degenerate (n < 5) | impossible by `min_names ≥ 10` assertion at argument parsing |
| non-finite stats (`nan` decay, `nan` PSR) | serialised as `null`; criterion `false` (the one exception, survival ratio +inf, is `ratio_inf: true` and counts as ≥ 2, §2.7) |
| `git rev-parse` unavailable | `git_sha = "unknown"`; result still written |
| weekly job: child exits 2/3/other or raises | `collect()` raises → 01 wrapper: logged, `collector_runs.ok=0`, `consecutive_failures += 1`; 01's `collector_failing:quant_weekly` latch fires at the job's `fail_alert_threshold` (`QUANT_ALERT_AFTER_FAILURES`, 2); never propagates into the scheduler. Child exit 1 (`insufficient_data`) is a success with `note` |
| weekly job timeout | 01's `asyncio.wait_for` cancels `collect()`, which kills the child (`proc.kill(); await proc.wait()`) before re-raising; treated as failure |
| result dir unwritable | CLI exit 2; job failure |

`/health` (`collectors.quant_weekly`, 01's standard collector block, 02 §G6): `enabled, available, paused (null | "operator" | "pause_all" | "emergency_file", 02 §B12), last_run, last_ok, last_error, last_skip_reason, consecutive_failures, last_rows, last_duration_sec, next_run` plus two keys added by the job's `status()` override: `last_gate_pass (bool|null)`, `last_verdict_file (str|null)`. Last-good semantics: `last_verdict_file` always points at the newest successfully written verdict; a failed run never deletes or overwrites earlier results (timestamped filenames).

---

## 8. Implementation plan

All modules under `app/quant/`, Python 3.12, stdlib only (`aiosqlite` is **not** used here — the harness is synchronous `sqlite3` in read-only mode, run by the weekly job as a child process, §8.8). No `numpy/pandas`. No `bittensor`.

### 8.1 File-by-file port plan
| new file | source | copy verbatim | strip / change |
|---|---|---|---|
| `app/util/mathutil.py` | created in Phase 0 as a re-implementation (the old source was not on the laptop; BUILD-LOG Phase 0 item 1); `monotone_score` already appended | — | **align `zscore` with the old `app/utils/math.py:57-70`** (now on disk at `seed/app/utils/math.py`): sample std (ddof = 1), zeros for n < 2 — the ported Phase-1 test pins `zscore([1..5])[0] = −1.2649`; no other module calls it. Other known divergences (old `pearson_r`/`spearman_r` return 0.0 below 5 points, 0-based ranks) are left to 12: the harness never correlates fewer than `min_names ≥ 10` names, and `monotone_score` keeps its own ≥ 5 guard |
| `app/quant/economics.py` | `app/backtest/economics.py` (whole) | all | nothing |
| `app/quant/slippage.py` | `app/backtest/slippage.py` (whole) | all | nothing (legacy linear model kept for `economics=None` reproduction) |
| `app/quant/validation.py` | `app/backtest/validation.py` (whole) | all | keep `validate_result` (unused); no imports to strip |
| `app/quant/factor_panel.py` | `app/backtest/factor_panel.py` | `segment_series` (extended), `_segment_ends`, `build_panel` (extended), `forward_return` (extended), SQL pattern of `load_factor_panel` L248-256 | **strip** `from app.config import settings`, `from app.data.fundamentals import FundamentalsStore`, `from app.strategy.dereg_risk import DeregRiskConfig`; `RESET_DROP_RATIO = 0.30` literal; `_ro_conn(db_path)` requires a path; `Bar.quality` (8th field) removed and `ts`, `occ`, `pit` **appended** after `volume`, so ported tests that build `Bar(bucket, price, depth, seg, reg)` positionally (test file L217-237, L338-370) run unchanged; `build_panel` keeps the `reg_blocks` kwarg as a per-netuid fallback (L241-248 test) |
| `app/quant/factor_backtest.py` | `app/backtest/factor_backtest.py` | factor fns L63-203 (except `factor_quality` L128-140 and `factor_net_flow_depth` L206-229 — dropped), `_solve` L305-321, `residualize` L324-338, `compute_ic` L397-414, `compute_deciles` L417-448, `_net_return` L451-479, `compute_deciles_net` L482-500, `deflate_factors` L560-590 | imports → `.mathutil`, `.economics`, `.factor_panel`, `.slippage`; `_reg_unix` (L33-40) replaced by `block_to_unix` (its constant-anchor fallback reproduces `_reg_unix` exactly, so the ported age tests only change the import); `CrossSection` gains `raw_fwds`; `build_cross_sections` gains `label_fn`, `regime_of`, drop counters, hole-date skipping; `run_factor_study` gains `label`, `regime_of`, `factor_registry`; `FACTOR_FUNCS` becomes `chain_factors()` + `make_pit_factor`; `deflate_factors` gains `trial_sharpe_variance` and `orient` (§8.5) |
| `app/quant/walk_forward.py` | `app/backtest/walk_forward.py:58-131, 157-183, 271-320 (FactorFoldRow, WalkForwardReport, _mean, _decay, _coverage_days), 462-604` | `MIN_FOLDS_TO_CERTIFY`, `Fold`, `make_folds`, `FactorFoldRow`, `WalkForwardReport`, `_mean`, `_decay`, `_coverage_days`, `_factor_ic_stats`, `walk_forward_factors`, `_factor_gate`, `_report_dict`, `save_walk_forward_json` | **strip** `from .pool_history_loader import …`, the `validation` imports except `BootstrapCI`, `StrategyFoldRow`, `select_winner`, `walk_forward_strategies`, `_strategy_gate`; `slice_panel` rewritten to carry `ts/occ/pit` (§8.3); `WalkForwardReport` gains `n_oos_folds` (rows with `n_oos_dates ≥ 1`); `_factor_gate` kept verbatim for `oos_credible`/`gate_note` (it has no decay test — the gate's c5 rule adds it, §2.7); default path → `QUANT_RESULTS_DIR` |
| `app/quant/survival.py` | `app/backtest/quality_band.py:32-140` | `TercileResult`, `_percentile`, the pooling loop | `factor_quality` (L102) → injected `factor_fn`; death test `fr ≤ −0.90` (L128) → injected `label_fn` (`death_90d`); terciles assigned **per date** cross-sectionally, not per netuid; add `n_events` (distinct occupants), `n_event_dates`, `n_events_by_kind`; rename `BandSurvival → SurvivalResult` |
| `app/quant/dereg_events.py` | `app/backtest/dereg_analysis.py:48-56, 64-96` | `DeregEvent`, `load_daily_panel`, `detect_dereg_events` | `_ro_conn` takes `db_path`; `load_daily_panel` reads `v_quant_price_bars WHERE period='1h'` (sample time `COALESCE(ts_last, bucket_start + 3600)`, price `close`) instead of `pool_history`; `reused_slots` (old L203-213 reads `pool_history`, absent from the snapshot) is rewritten as `SELECT netuid, COUNT(*) FROM v_quant_occupants GROUP BY netuid HAVING COUNT(*) > 1`; used by labels (cross-check) and the verify checklist |
| `app/quant/report.py` | `app/backtest/factor_report.py:14-160` | `_fmt_pct`, `_trade_tao`, `_max_depth`, `_depth_label`, `_span`, `print_factor_report`, `_factor_result_dict`, `save_factor_json` | `RESULTS_DIR` → settings; `save_factor_json` adds the §3.5 keys, `deflated`, `provenance`; timestamps via `to_iso`; non-finite → `null`, `allow_nan=False`; writes the `ic_series_*` sidecar |
| `app/quant/labels.py` | new | — | §8.4 |
| `app/quant/occupants.py` | new | — | `OccupantIndex` |
| `app/quant/pit.py` | new | — | `PITStamper` |
| `app/quant/panel_builder.py` | new (replaces `load_factor_panel` I/O) | — | §8.2 |
| `app/quant/regimes.py` | new | — | `emission_regime_at`, `load_market_regimes` |
| `app/quant/rank_stability.py` | new | — | §8.5 |
| `app/quant/trials.py` | new | — | trial log |
| `app/quant/gate.py` | new | — | §8.6 |
| `app/quant/backtestability.py` | new | — | `RECONSTRUCTIBLE_KEYS`, `HEALTH_FACTORS`, `RETIRED_NAMES`, `load_coverage(path) -> Coverage \| None` (§2.5) |
| `app/quant/snapshot.py` | new | — | `resolve_db(db_arg) -> str`, `newest_backup(dir) -> Path \| None`, `make_snapshot(src, out) -> Path`, `prune(keep)`, `db_fingerprint(path) -> str` (sha256), `git_sha()`. The reduced snapshot holds 11 relations: `v_quant_price_bars` (materialised, `period='1h'`), `v_quant_chain_hourly`, `v_quant_chain_daily`, `v_quant_occupants` (materialised as tables of the same names, so the harness SQL is identical on a full backup and on a snapshot), `metric_values` (`stage='norm'` rows + `stage='raw'` rows of keys outside 12's scored `METRICS`), `scores`, `regime_daily`, `events` (the two §3.1 types), `block_times`, `schema_version`, `quant_snapshot_meta(key, value)` (`source_path`, `source_sha256`, `source_stamp`, `made_at`, `max_classifier_version`, `coverage_sha256`); written to `<out>.tmp`, `PRAGMA quick_check`, atomic rename; the source is opened `mode=ro` |
| `app/quant/provenance.py` | new | — | `Provenance` dataclass + `collect(db_path, command)` |
| `app/quant/__main__.py` | pattern from `app/backtest/__main__.py:713-885` | flag names | §8.7 |
| `app/quant/weekly_job.py` | new | — | §8.8 |

`app/quant/dereg_risk.py` is ported by spec 05 (hazard at run time); this spec does not import it.

### 8.2 Panel types and builder (`factor_panel.py`, `panel_builder.py`)
```python
class Bar(NamedTuple):          # old positional order kept (factor_panel.py:49-56); new fields appended
    bucket: int                 # bucket-start epoch — the PIT decision instant (§2.1)
    price: float
    depth: float                # τ in pool; 0.0 = not observed in bucket (ineligible)
    seg: int
    reg_block: int = 0          # occupant registration block (0 = unknown)
    moving_price: float = 0.0
    volume: float = 0.0         # Δ subnet_volume sources only (§2.3); 0.0 = unusable
    ts: int = 0                 # timestamp of the close sample (entry instant, ≥ bucket); 0 → treated as `bucket`
    occ: str | None = None      # occupant_key of the close sample
    pit: Mapping[str, float] = MappingProxyType({})   # factor -> value with eff ≤ bucket (absent = None)

DEATH_RETURN = -1.0
DeathKind = Literal["price_reset", "reoccupied"]

@dataclass
class Panel:
    scope: str; grid_seconds: int
    series: dict[int, list[Bar]]; index_of: dict[int, dict[int, int]]
    seg_end: dict[int, list[int]]
    seg_death: dict[int, dict[int, DeathKind]]   # netuid -> {seg_id: kind} (absent = alive/data end)
    dates: list[int]
    pit_keys: tuple[str, ...]                    # which PIT factors were stamped
    regime_of: dict[int, dict[str, str]]         # date -> {"emission": ..., "market": ...}

def segment_series(prices: list[float], reset_ratio: float = 0.30) -> list[int]   # ported signature (price rule only)
def segment_deaths(prices: list[float], reset_ratio: float = 0.30,
                   occupants: list[str | None] | None = None) -> tuple[list[int], dict[int, DeathKind]]
def build_panel(rows_by_netuid: dict[int, list[PanelRow]], *, grid_seconds: int, reset_ratio: float = 0.30,
                scope: str = "", regime_of: dict[int, dict[str, str]] | None = None,
                reg_blocks: dict[int, int] | None = None) -> Panel   # reg_blocks: fallback when a row carries reg 0
def forward_return(panel: Panel, netuid: int, i: int, horizon: int) -> float | None
def forward_log_return(panel: Panel, netuid: int, i: int, horizon: int, *, death_floor: float = 0.01) -> float | None
def block_to_unix(block: int, anchors: list[tuple[int, int]] | None = None) -> float
```
`PanelRow = tuple[int, float, float, float, float, int, str | None, int, dict[str, float]]` = `(bucket, price, depth, moving_price, volume, ts, occ, reg_block, pit)` — the old `(bucket, price, depth[, moving_price, volume])` layout (`factor_panel.py:145-153`) is a prefix, so the old 3-/5-tuples are still accepted by `_opt` padding and the ported tests (`_rows` helper, test L44-46) run unchanged (`ts` defaults to `bucket`). `block_to_unix` interpolates piecewise-linearly over `(block_number, unix)` anchors: first 17's exact `block_times` rows, then `v_quant_chain_daily (block_number, ts)` (TMC analytics block numbers match chain time within ~1 min), extrapolated at 12.0 s/block from the nearest anchor outside the anchor range, and the old constant anchor `(6_502_594, 1_758_585_588, 12.0 s)` (`factor_backtest.py:33-40`) when no anchors are given (= old `_reg_unix`, so the ported age tests only change the import) — the linear anchor alone drifts 7-32 h (research 02-gapfill-data-continuity §Block↔time). `age` prefers the occupant's `v_quant_occupants.registered_ts` (exact for every occupant but root on the 2026-10-17 backup), stamped on the bar as the internal `pit` entry `_reg_ts`, and falls back to `block_to_unix(reg_block, anchors)`.

`segment_deaths` algorithm (`segment_series` = its seg list with `occupants=None`, so the ported test `segment_series([…]) == [0, 0, 0, 0, 1]` runs unchanged): iterate `i = 1..n−1`; `both_known = occupants and occupants[i-1] and occupants[i]`; `occ_fire = both_known and occupants[i-1] != occupants[i]`; `price_fire = prev > 0 and price[i]/prev < reset_ratio and (occ_fire or not both_known)`. If `price_fire`: `segs[i] = s; s += 1; death[s-1] = "price_reset"` (ported). Elif `occ_fire`: `s += 1; segs[i] = s; death[s-1] = "reoccupied"`. Else `segs[i] = s`. `forward_return`: `target ≤ end` → ratio; else `kind = seg_death.get(seg)`; `"price_reset"` → `price[end]/p0 − 1`; `"reoccupied"` → `DEATH_RETURN`; `None` → `None`.

```python
# panel_builder.py
@dataclass(frozen=True)
class PanelRequest:
    scope: str; start: int | None = None; end: int | None = None; grid_seconds: int | None = None
    pit_keys: tuple[str, ...] = (); pit_mode: str = "strict"; spec_version: str = "ts-1.0"
    reset_ratio: float = 0.30

def load_factor_panel(req: PanelRequest, *, db_path: str, freshness: Mapping[str, int] | None = None) -> Panel
```
Steps: (1) `_ro_conn(db_path)` (`file:…?mode=ro`); (2) price SQL — `SELECT netuid, (bucket_start/:grid)*:grid AS bucket, close, tao_in_pool, moving_price, occupant_key, source, backfilled, MAX(COALESCE(ts_last, bucket_start + 3600)) AS ts, SUM(volume) AS vol_sum, COUNT(*) AS n_rows, SUM(source IN (<Δ sources>) AND volume IS NOT NULL) AS n_vol FROM v_quant_price_bars WHERE period = '1h' AND netuid != 0 AND bucket_start >= :lo AND bucket_start < :hi GROUP BY netuid, bucket ORDER BY netuid, bucket` for both grids (`'1d'` never — daily candles' close time is ambiguous; 5 m see §2.2); the aggregates do not disturb the single-`MAX()` bare-column rule; `Bar.volume = vol_sum` when `n_vol = n_rows`, else `0.0` (§2.3); `source`/`backfilled` (of the close row) feed provenance; (3) depth/moving-price fallback — for bars with `tao_in_pool IS NULL`, `v_quant_chain_hourly` last-write-wins (`MAX(hour)`) within the bucket and `≤ ts`, then `v_quant_chain_daily` (`day = floor_day(bucket)`) on the 24 h grid only; still missing → `0.0`; `moving_price` likewise from `v_quant_chain_hourly`; (4) `OccupantIndex.load(con)`; `occ` = the row's `occupant_key`, else `occupant_at(netuid, ts)`; `reg_block` parsed from `occ`; (5) `load_coverage(...)`; `PITStamper.load(con, keys, spec_version, lo − max_age, hi, pit_mode, official_only = (grid == 86400), coverage)` then `stamp(occ, bucket) -> dict`; (6) `regime_of` from `regimes.py`; (7) `build_panel`; (8) record `price_sources`, `price_bars_backfilled_frac`, `occupant_coverage` for provenance.

### 8.3 Occupants and PIT (`occupants.py`, `pit.py`)
```python
class OccupantIndex:
    @classmethod
    def load(cls, con: sqlite3.Connection) -> "OccupantIndex"      # from `v_quant_occupants`
    def occupant_at(self, netuid: int, ts: int) -> tuple[str, int] | None
        # (occupant_key, reg_block); interval [registered_ts (view: exact else 04 estimate) or first_seen_at,
        #  COALESCE(deregistered_ts, deregistered_at) or +inf); overlapping intervals (04's live deregistered_at is the
        #  detection time) resolve to the greatest network_registered_at; None before the earliest known registration
    def death_onset(self, occupant_key: str) -> tuple[int, str] | None
        # earliest of (COALESCE(deregistered_ts, deregistered_at), dereg_reason) and netuid_reoccupied events whose
        # payload.old_occupant_key / payload.old_key == occupant_key at COALESCE(event_at, observed_at)
    def occupants_alive_on(self, day: int) -> list[str]

class PITStamper:
    def __init__(self, keys: Sequence[str], *, mode: str, freshness: Mapping[str, int], default_max_age: int,
                 reconstructible: Mapping[str, dict], coverage: "Coverage | None") -> None
    @classmethod
    def load(cls, con, keys, *, spec_version, lo, hi, mode, freshness, default_max_age, official_only,
             coverage) -> "PITStamper"
    def stamp(self, occupant_key: str | None, t_dec: int) -> dict[str, float]   # t_dec = Bar.bucket
    counters: dict[str, int]    # dropped_backfilled, dropped_forward_only, dropped_before_reconstructible_from, …
```
`load` runs one query per source (12 §2.15 items 3–4):
- metric factors and `raw:` keys: `SELECT occupant_key, metric_key, as_of, value, json_extract(value_json,'$.raw') AS raw, observed_at, backfilled FROM metric_values WHERE stage = 'norm' AND spec_version = :S AND metric_key IN (…) AND value IS NOT NULL AND as_of BETWEEN :lo AND :hi [AND as_of % 86400 = 0 when official_only]`; `raw:<k>` takes `raw` of the same row (rows whose `raw` is NULL are skipped for that key);
- `evidence:` keys: the same columns from `stage = 'raw'` rows with `status NOT IN ('failed','none','not_applicable')` and `value IS NOT NULL` (02 §B5; their `spec_version` is the producer's `SPEC_VERSION`); `absent` rows carry their stored `value` (0.0); the official-slot filter does not apply (raw `as_of` is the producer's slot, not a scoring slot);
- scores: `SELECT occupant_key, as_of, run_kind, composite_raw, composite, composite_smoothed, quality, velocity, confidence, pillar_scores, computed_at, input_cutoff, backfilled, shadow FROM scores WHERE spec_version = :S [AND run_kind = 'official' when official_only] [AND shadow = 0 when :S = SPEC_VERSION] AND as_of BETWEEN :lo AND :hi`.
`eff` per §2.1: norm/raw rows `observed_at`, score rows `computed_at` (assert `input_cutoff ≤ computed_at`); `backfilled = 1` rows are dropped in `strict`; in `reconstructed` they get `eff = as_of + RECONSTRUCTIBLE_KEYS[key].lag_s` (scores: `max(input_cutoff, as_of + lag_s)`), keys not reconstructible (incl. `x_engagement_30d`) are dropped with a counted warning, and rows with `as_of < coverage.reconstructible_from[key or group]` are dropped and counted. Rows are grouped by `(key, occupant_key)` and sorted by `eff`; `stamp` binary-searches the last row with `eff ≤ t_dec`, then walks back to select max `as_of` among rows with `eff ≤ t_dec` (bounded scan over the tail), and applies `t_dec − as_of ≤ max_age` (registry freshness; scores `QUANT_SCORE_MAX_AGE_S`).

`slice_panel(panel, lo, hi)` rebuilds `rows_by_netuid` from bars (`(b.bucket, b.price, b.depth, b.moving_price, b.volume, b.ts, b.occ, b.reg_block, b.pit)`) and calls `build_panel` with the same `regime_of`, so segmentation and `seg_death` recompute inside the window and `forward_return` self-truncates (no cross-fold look-ahead, ported intent `walk_forward.py:157-183`).

### 8.4 Labels (`labels.py`)
```python
@dataclass(frozen=True)
class LabelSpec:
    id: str; family: Literal["ret_log", "demis", "death", "gate_cross"]; horizon: int; unit: Literal["bars", "days"]
    def step_bars(self, grid_seconds: int) -> int      # non-overlapping step: bars, or days*86400//grid

class LabelSource:            # loaded once per snapshot
    @classmethod
    def load(cls, con, *, occupants: OccupantIndex, label_end: int, top_n: int = 32, zero_run_days: int = 7,
             zero_eps_pct: float = 1e-4) -> "LabelSource"
    def fwd_log_return(self, panel, nu, i, h) -> float | None
    def demis(self, occupant_key, t: int, days: int) -> float | None
    def death_within(self, occupant_key, t: int, days: int) -> int | None
    def gate_cross(self, occupant_key, t: int, days: int) -> int | None
    def label_fn(self, spec: LabelSpec) -> Callable[[Panel, int, int], float | None]   # (panel, netuid, i)

def parse_label(s: str, grid_seconds: int) -> LabelSpec   # "ret_log_7" | "demis_28" | "death_90" | "gate_cross_28"
```
`death_within` unions four sources and returns the first onset's kind: `OccupantIndex.death_onset` (`dereg`), the segmentation death of the bar's series (`price_reset`/`reoccupied`, onset = the terminal bar's `ts`; the only source for `occ = None` bars), `events.event_type='emission_disabled'` for the occupant (`emission_disabled`, onset `COALESCE(event_at, observed_at)`; live era only), and a precomputed zero-emission run index from `v_quant_chain_daily` (`zero_emission`: `emission_share ≤ QUANT_ZERO_EMISSION_EPS_PCT` for ≥ `QUANT_ZERO_RUN_DAYS` consecutive daily rows on an occupant whose share at `t` was above eps; onset = the run's first day). Censoring: `t + days·86400 > label_end` → `None`. `t` is always `Bar.bucket`.

### 8.5 Studies (`factor_backtest.py`, `survival.py`, `rank_stability.py`, `walk_forward.py`)
```python
FactorFn = Callable[[list[Bar], int, int], float | None]
def chain_factors() -> dict[str, FactorFn]                      # the 9 ported chain factors
def make_pit_factor(name: str) -> FactorFn                       # lambda bars,i,k: bars[i].pit.get(name)
def make_quality_cg() -> FactorFn                                # from pit["P1"], pit["P2"]
def resolve_factor(name: str, panel: Panel) -> FactorFn          # chain | quality_cg | PIT (must be in panel.pit_keys else KeyError);
                                                                 # retired P4 names / non-scored keys → ValueError (CLI exit 2)

@dataclass
class CrossSection: date: int; netuids: list[int]; factors: list[float]; fwds: list[float]; depths: list[float]; raw_fwds: list[float]

def build_cross_sections(panel, factor_fn, *, horizon, lookback, min_depth, min_names, step, controls=None,
                         max_depth=inf, label_fn=None, raw_fn=None, hole_dates=None, counters=None
                         ) -> tuple[list[CrossSection], list[int]]   # ported 2-tuple; counters filled in place;
                                                                    # label_fn None = ported simple forward_return
def run_factor_study(panel, *, factors, horizons, label_family="ret_log", lookback=6, min_depth=3000.0, min_names=10,
                     step=None, trade_tao=None, controls=None, max_depth=inf, economics=None,
                     label_source: LabelSource | None = None) -> dict[str, dict[int, FactorResult]]
def deflate_factors(results, *, horizon, n_trials=None, trial_sharpe_variance=None, orient=True) -> dict[str, DeflatedSharpe]
def by_regime(sections: list[CrossSection], regime_of) -> dict[str, dict[str, dict]]
```
`FactorResult` gains `label: str, pit_mode: str, controls: list[str], overlapping: bool, monotone_score: float, by_regime: dict, n_none_factor: int, n_none_label: int, n_dropped_controls: int, n_hole_dates: int`. Net-of-cost: `_net_return` receives `raw_fwds` (simple returns), nets them through `net_round_trip` at `trade_tao` (ported; `--no-economics` uses the legacy linear `slippage.py` path as the old default did), then the label transform is re-applied (`log(max(1+net, floor))`) before `compute_deciles`. `step=None ⇒ step = label.step_bars(grid)` (non-overlapping; `step < horizon` sets `overlapping = true`).

**Deflation fix (review).** The ported `deflate_factors` takes the trial-Sharpe variance as `pvariance` of `abs(ic_ir)` across the factors *in the current result* and flips negative-mean series (`factor_backtest.py:578-589`). With one factor — `report`'s pre-registered gate factor — that variance is 0, so `expected_max_sharpe` returns SR0 = 0 (`validation.py:167-168`) and `n_trials` is silently ignored. The port therefore accepts `trial_sharpe_variance` (overrides the in-run variance when given) and `orient` (default `True` = ported two-sided behaviour). The gate and `--deflate` pass `n_trials = trials.n_trials` and `trial_sharpe_variance = trials.trial_sharpe_var` (sample variance, ddof = 1, of `abs(ic_ir)` over the newest result of every distinct trial, §3.4), and the gate passes `orient=False` (the pre-registered direction is IC > 0).

```python
@dataclass
class SurvivalResult: scope: str; factor: str; horizon_days: int; step_days: int; min_depth: float; n_names: int; n_obs: int
                      n_events: int; n_event_dates: int; n_events_by_kind: dict[str, int]; n_censored: int
                      terciles: list[TercileResult]; death_ratio_low_over_high: float   # +inf allowed in memory (§3.6 encoding)
                      n_events_dereg_only: int; death_ratio_dereg_only: float           # diagnostic (§2.4)
def survival_study(panel, *, factor_fn, label_source, horizon_days=90, step_days=90, min_depth=0.0, max_depth=inf, n_buckets=3) -> SurvivalResult

@dataclass
class RankStabilityResult: scope: str; factor: str; lag: int; n_pairs: int; mean_rank_autocorr: float | None
                           median_rank_autocorr: float | None; mean_quintile_turnover: float | None; mean_top20_turnover: float | None
                           series: list[tuple[int, int, float | None, float | None, float | None]]   # (date, n, autocorr, quintile_to, top20_to)
def rank_stability(panel, factor_fn, *, lag=1, lookback=6, min_depth=0.0, min_names=10) -> RankStabilityResult
```
`survival_study`: per rebalance date (`step_days`), names with non-None factor and non-None `death_90d` are split into terciles by `quantile_buckets(values, 3)` over netuid-sorted input; per tercile death rate = events / observations. `n_events` counts **distinct** `(occupant_key or netuid-segment, onset)` deaths (a death seen from several start dates when `step_days < horizon_days` counts once), `n_event_dates` the distinct onset days, `n_events_by_kind` per death kind (§8.4). `death_ratio_low_over_high` keeps the ported semantics (`quality_band.py:59-69`: +inf when only the top tercile is event-free, 0.0 when both are, NaN when a tercile is empty). Diagnostic (never a gate input): `SurvivalResult` also carries `n_events_dereg_only` and `death_ratio_dereg_only`, the same statistics with only the kinds `dereg`, `price_reset`, `reoccupied` counted as events (an observation whose first onset is `zero_emission`/`emission_disabled` counts as a survivor unless a dereg-type onset also falls in its window), so a ratio carried by transient zero-emission spells is visible (§2.4, §11 item 13).

`rank_stability` computes 12 §2.12's statistics offline, so its numbers are comparable with `rank_stability_daily`: for consecutive dates `(t−lag, t)` take the names with non-None factor on both (and `depth ≥ min_depth`), **sorted by netuid** (determinism of the ported `quantile_buckets`); `n < min_names` → nulls; `rank_autocorr = spearman_r(f_t, f_{t−lag})`; `quintile_turnover` = share whose `quantile_buckets(·, 5)` bucket differs; `top20_turnover = |top20_t \ top20_{t−lag}| / min(20, n)` (ties by netuid). Target (master §7.7): `mean_rank_autocorr ≥ 0.9` for `quality` on the 24 h grid at `lag = 1`. Differences from the live monitor: bucket-instant PIT stamping instead of slot rows, and no `status`/`insufficient_data` filter.

`walk_forward_factors(panel, factors, folds, *, horizon, lookback, min_depth, min_names, max_depth, scope, mode)` — ported; label is `ret_log` only (forward returns self-truncate at the slice edge; day-based `LabelSource` labels would not, so they are refused); selection rule max |IC-IR| on the train slice retained; a single factor measures factor decay (the pre-registered use), the full set measures search decay (diagnostic). `folds = make_folds(panel.dates, train_min = max(int(len(dates)·QUANT_WF_TRAIN_MIN_FRACTION), 4·horizon), test_h = QUANT_WF_TEST_DAYS·86400 // grid, mode)`.

### 8.6 Gate (`gate.py`)
```python
@dataclass
class ScopeInputs: factor_res: FactorResult; controlled_res: FactorResult; dsr: DeflatedSharpe | None; wf: WalkForwardReport | None; surv: SurvivalResult | None
def evaluate_gate(*, factor: str, label: LabelSpec, live: ScopeInputs, surv: ScopeInputs, n_trials: int,
                  trial_sharpe_var: float | None, cfg: GateConfig, observe_telemetry: dict | None,
                  provenance: Provenance) -> dict     # the §3.6 JSON
```
Pure; every criterion returns `{"pass": bool|None, "rule": str, "live": {...}, "survivorship": {...}, "reason": str|None}` and implements §2.7 exactly (thresholds from `GateConfig` = the `QUANT_*` keys of §5, recorded in the verdict's provenance). It also fills `checks`, `upsize_gate_pass`, `promotable`, `observe_telemetry`. An input with `overlapping = true`, a different label, or `insufficient_data = true` makes the affected criterion `false` with a `reason`.

### 8.7 CLI (`__main__.py`)
```
python -m app.quant snapshot       [--from PATH] [--out PATH] [--prune]
python -m app.quant factor         --factors A,B --horizons 7,28,84 --scope live|survivorship|both [--label ret_log|demis|death|gate_cross]
                                   [--controls size,emission_share] [--lookback 6] [--min-depth 3000] [--max-depth INF] [--min-names 10]
                                   [--step N] [--trade-tao 2] [--no-economics] [--deflate] …common… [--print]
python -m app.quant survival       --factors A --scope … [--horizon-days 90] [--step-days 90] [--min-depth 0] [--max-depth INF] …common…
python -m app.quant walk-forward   --factors A[,B] --horizons 7 --scope … [--test-days 28] [--train-min-fraction 0.5] [--mode expanding|rolling]
                                   [--lookback 6] [--min-depth 3000] [--max-depth INF] [--min-names 10] …common…
python -m app.quant rank-stability --factors A,B --scope … [--lag 1] [--lookback 6] [--min-depth 0] [--min-names 10] …common…
python -m app.quant report         (--run-all | --from-results DIR) [--factor quality_cg] [--horizon-days 7] [--observe-telemetry PATH] …common…
python -m app.quant gate           --factor F [--spec ts-1.0] [--scopes live,survivorship] [--horizons 7,28,84] [--controls …] [--trade-tao 2]
                                   [--observe-telemetry PATH] …common…      # alias of `report --run-all` in 15 §8's spelling
common: [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--db PATH] [--output PATH] [--spec-version ts-1.0] [--seed 0] [--pit-mode strict|reconstructed]
```
Flag semantics (every flag defaults to its §5 key; CLI overrides settings for one run):
- `snapshot`: `--from` = source DB (default: newest uncompressed backup, §5 `QUANT_DB_PATH` rules; refusals §7); `--out` = target path (default `QUANT_SNAPSHOT_DIR/quant_<source-stamp>.db`); `--prune` deletes all but the newest `QUANT_SNAPSHOT_KEEP` snapshots (+ their coverage copies) after writing. Prints `snapshot written <path> (tables: 11, size …)`.
- `--scope both` runs both scopes and writes one file per scope; with `--output P` the files are `P` with `_<scope>` inserted before the suffix. `--start/--end` are UTC dates: `[start 00:00Z, end 00:00Z + 86 400)`.
- `--label` selects the family; each `--horizons` value `h` builds `<family>_<h>`: bars for `ret_log`, **days** for `demis|death|gate_cross` (defaults when omitted: `QUANT_HORIZONS_24H/4H`, `28,84`, `90`, `28`).
- `--controls` (factor, report/gate only): comma names; `emission_share` → `chain:emission_share`; the tested factor is auto-excluded (ported L531). `--step N` overrides the non-overlapping default (result flagged `overlapping`). `--trade-tao 0` disables net deciles; `--no-economics` uses the legacy linear slippage for them. `--deflate` adds the `deflated` block (N and V from the trial log). `--print` prints the ported console report (`print_factor_report`) and decile curves; without it one summary line per (factor, horizon) is printed. `--min-names < 10` is an argument error (exit 2). `--seed` seeds bootstrap/DSR RNG; `--spec-version` filters norm rows and scores.
- `walk-forward` accepts `ret_log` only; `rank-stability --lag` is in bars (1 = 1 day on the 24 h grid).
- `report` requires exactly one of `--run-all` (runs factor gross + controlled with `--deflate`, walk-forward and survival on both scopes for `--factor` at `--horizon-days`, then evaluates the gate) or `--from-results DIR` (assembles the verdict from the newest matching result files, refusing mismatched label/horizon/spec/pit_mode); it writes `gate_verdict_<factor>_<ts>.json`. `--observe-telemetry PATH` names the c7 JSON (§2.7); a SQLite file there is refused with exit 2 (the bot-side export is 15's).
- `gate` maps 15's flags: `--spec` → `--spec-version`; `--scopes` must be `live,survivorship` (anything else → exit 2); the gate horizon stays `--horizon-days`/`QUANT_GATE_HORIZON_DAYS`, extra `--horizons` are computed and written as supplementary factor results only.
Every subcommand: resolves the DB (§7), records `trials.json` entries **before** computing and fills their `results` after, writes results, prints the summary plus a final machine line `RESULT {"files": [...], "verdict_file": str|null, "gate_pass": bool|null, "insufficient_data": bool}`, and exits 0 (results written), 1 (results written but `insufficient_data`), 2 (refused/error), 3 (`QUANT_ENABLED=False`).

### 8.8 Weekly job (`weekly_job.py`)
`class QuantWeeklyJob(CollectorJob)` (01 §2.6): `id='quant_weekly'`, `phase=10`, enabled flags `("QUANT_ENABLED", "QUANT_WEEKLY_JOB_ENABLED")` (01's `JobSpec.enabled_flags` ANDs them), cron from `QUANT_WEEKLY_JOB_CRON` (01's `weekly_at`), `min_gap_factor=0`, `misfire_grace_sec=3600`, `paid=False`, no provider, not `ops_exempt`. Per-job settings are live through 01's `JobSpec` keys: `fail_alert_key="QUANT_ALERT_AFTER_FAILURES"` (already in 01, so §11 item 12's first request is done) and `timeout_key="QUANT_WEEKLY_JOB_TIMEOUT_S"` (added to 01 with this spec: the wrapper's `asyncio.wait_for` uses `getattr(settings, timeout_key)` when set, else the static `timeout_sec` = 1800; closes BUILD-LOG Phase 0 item 5). Pauses come from the wrapper (`control_state`, `PAUSE_ALL`, `data/control/quant_weekly.paused`; 02 §B12).

`collect(run)` runs three child processes in sequence with `asyncio.create_subprocess_exec("nice", "-n", "10", sys.executable, "-m", "app.quant", …)`: `snapshot --prune` (source = newest uncompressed backup) → `report --run-all --db <that snapshot>` for `QUANT_GATE_FACTOR` → `rank-stability --factors quality,composite_smoothed --scope survivorship --lag 1 --db <snapshot>`. It parses each child's `RESULT` line; exit 0/1 continue, anything else raises `RuntimeError("quant child <cmd> exit <n>: <stderr tail>")`. On `CancelledError` (01's timeout) it kills the running child, awaits it and re-raises. It returns `JobOutcome(rows=<result files>, note="gate_pass=<bool> verdict=<file>")` and stores `meta_json` (§3.2) and the `/health` extras (`last_gate_pass`, `last_verdict_file`). No Telegram message of its own (§3.7).

**`JOB_TABLE` row for 01 to mirror (02 §E2/§E3):**

| id | factory (`app.…`) | trigger | UTC time | misfire grace s | timeout s | enabled flag | interval key | phase | provider | paid | `ops_exempt` | `min_gap_factor` | liveness max-age s (16 §2.5) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `quant_weekly` | `quant.weekly_job:QuantWeeklyJob` | cron `day_of_week=sun hour=5 minute=0` UTC (from `QUANT_WEEKLY_JOB_CRON` = `sun 05:00`) | Sun 05:00Z | 3600 | 1800 (`QUANT_WEEKLY_JOB_TIMEOUT_S`) | `QUANT_WEEKLY_JOB_ENABLED` (∧ `QUANT_ENABLED`) | — | 10 | — | no | no | 0 | 1 211 520 |

### 8.9 Step order
1. Copy verbatim modules (`economics`, `slippage`, `validation`); append `monotone_score` to 12's existing `mathutil.py` (and its cases to 12's `tests/quant/test_mathutil.py`); port `tests/test_factor_backtest.py` Phases 1, 3, 4 and make them green against `app.quant`.
2. Port `factor_panel.py` with the extended `Bar/Panel/segment_series/forward_return` (old positional order preserved); ported Phase-2, age and volume/moving-price tests green (only the `_reg_unix` import changes to `block_to_unix`); new occupant-boundary tests.
3. `occupants.py`, `pit.py`, `backtestability.py` (incl. `load_coverage`), `panel_builder.py` + PIT tests on the fixture DB built from 03's `models.sql`.
4. `labels.py` + tests. 5. `factor_backtest.py` port with `label_fn`, `raw_fwds`, `by_regime`. 6. `walk_forward.py`, `survival.py`, `rank_stability.py`. 7. `trials.py`, `provenance.py`, `snapshot.py`, `report.py`. 8. `gate.py`. 9. CLI. 10. `weekly_job.py` + `/health` wiring. 11. Run the first study (§10).

---

## 9. Tests

`tests/quant/` (pytest, no network, no live DB). Fixture factory `tests/quant/fixtures.py::make_fixture_db(path, *, n_netuids=24, days=420, seed=0)` creates the schema by executing 03's `app/storage/models.sql` (so the `v_quant_*` views and their rao → τ conversions are exercised) and fills 03's base tables with 03's names and units: 24 netuids of `price_bars` `period='1h'` (`ledger_import` with `ts_last`/reserves to day 270, `tmc_candle` after with NULL `ts_last`/reserves, plus a few `period='5m'` rows that must be ignored), `chain_hourly` (`hour_ts`, `tao_liquidity` in rao, `subnet_moving_price`) and `chain_daily` (`day_ts`, `emission_percent`, `subnet_tao` in rao), `occupants` including netuid 7 re-registered on day 200 (price collapse captured), netuid 9 re-registered on day 300 **without** a captured collapse bar (occupant change only; the successor row has `registered_ts` NULL and `registered_at_est` set), netuid 11 deregistered on day 380 with no successor, netuid 13 zero-emission from day 350; `metric_values` `stage='norm'` (spec_version `ts-1.0`, `value_json.raw` set) for `emission_share` (backfilled=1, as_of daily 00:00Z, observed_at = day 419), `active_authors_28d` (backfilled=0, observed_at = as_of + 900), `site_uptime_30d` (forward-only, from day 400), `x_engagement_30d` (from day 405), plus `stage='raw'` rows with **different** values for the same keys (must never be read as factors) and raw `owner_locked_share` rows; `scores` 4 slots/day (00:00Z `official`, 06/12/18:00Z `provisional`) with `pillar_scores` P1/P2, `as_of` = slot instant, `computed_at = input_cutoff = as_of + 900` (02 §B6; backfilled rows: `input_cutoff = as_of + 900`, `computed_at` = day 419), backfilled=1 to day 399, 0 after, and one `shadow=1` row set for spec `ts-1.1`; `regime_daily` (`day_ts`, uppercase `label`, `source` `backfill` to day 380 then `live`, one missing day); `events`: one `emission_disabled` on day 355 for netuid 13 and one backfilled `netuid_reoccupied` for netuid 9 whose `occupant_key` is the successor and `payload.old_key` the dead occupant; `block_times` anchors; a `coverage.json` (`coverage-1`) with one `holes` entry and `reconstructible_from.emission_share` = day 30.

| test file | cases |
|---|---|
| `test_mathutil.py` (12's file, extended) | `monotone_score`: increasing curve → 1.0, reversed → −1.0, `None` entries skipped, < 5 values → 0.0 |
| `test_factor_panel.py` | ported Phase 2 (segmentation, death realisation not marked to pre-collapse price) and the age/volume/moving-price tests with positional `Bar(...)` unchanged; **new:** a collapse inside one known occupant is not a boundary (the price rule still applies where the occupant is unknown); occupant change without collapse → boundary, `seg_death == "reoccupied"`, `forward_return == −1.0` beyond the end; unknown→known occupant is not a boundary; both rules firing at the same bar → single boundary with `price_reset`; `forward_log_return` floor; old 3-/5-tuple rows accepted (`ts` defaults to `bucket`); `reg_blocks` kwarg fallback; `block_to_unix` uses `block_times` first, then chain_daily anchors, then the constant anchor (= old `_reg_unix`) |
| `test_panel_builder.py` | reads `v_quant_*` only: depth equals `tao_liquidity / 1e9` and `subnet_tao / 1e9` (never rao); `period='5m'` rows ignored; `tmc_candle` bar `ts = bucket_start + 3600`; 24 h `Bar.volume` = sum of the day's Δ volumes when every hour is a Δ source, `0.0` when any hour is `tmc_candle`/`sse_rollup`; `chain:emission_share` uses the previous day's row (never the bucket's own day); `occ` from `price_bars.occupant_key`, else `OccupantIndex` (successor with only `registered_at_est` resolved via the view's COALESCE); volume zeroed for `tmc_candle` bars; overlapping occupant intervals resolve to the newer registration |
| `test_pit_stamping.py` | cutoff is `bar.bucket`: `observed_at = bucket` → used, `= bucket + 1` → None, and a row observed inside the bucket before `bar.ts` → None; `stage='raw'` rows with other values are never used for metric factors; `raw:` reads `value_json.raw` of the same norm row; `evidence:owner_locked_share` reads raw rows with B5 semantics (failed skipped, absent = 0.0); revision rule (later-observed older `as_of` does not replace a newer `as_of`); staleness → None beyond `max_age`; 24 h grid ignores provisional slots, 4 h grid uses them; scores `eff = computed_at`, backfilled score `eff = max(input_cutoff, as_of + lag)`; `backfilled=1` ignored in `strict`, used in `reconstructed` with `eff = as_of + lag` but not before `reconstructible_from`; forward-only key and `x_engagement_30d` ignored in `reconstructed` with warning counts; `shadow=1` rows ignored for the primary spec and read for `ts-1.1`; prior occupant's metric never appears on the successor's bars and vice versa; `NULL` value rows skipped |
| `test_backtestability.py` | `load_coverage`: `reconstructible_from` clamps, hole dates skipped and counted, caveats copied, `rpc_golden_block ≠ pass` → warning, unknown schema / missing file → warning only; retired names (`owner_conviction`, `owner_retention`, `identity_stability`) raise with the master name; `x_engagement_30d`/`polarity_7d`/P5/P6/`velocity`/`quality` results carry `x_engagement_refreshed_in_place` |
| `test_factor_backtest.py` | ported Phase 3 (IC ≈ +1/−1/0 synthetic), Phase 4 (monotone deciles), `residualize`, `compute_deciles_net`, `deflate_factors`; **new:** `quality_cg` renormalisation; PIT factor IC positive when it predicts return and the within-band test (ports of test L277-305 via `pit`); controls None → name dropped and counted; `emission_share` control resolves to `chain:emission_share`; `by_regime` sums to `n_dates` and uses uppercase `UNKNOWN` for a missing regime day; `deflate_factors` with one factor and `trial_sharpe_variance=None` gives `sr0 == 0`, with a trial-log variance gives `sr0 > 0`; `orient=False` keeps a negative-mean series negative |
| `test_labels.py` | `demis` with death → `−share(t)`; `death_90d` censored → None after `label_end − 90 d`, 1 for netuid 11 (dereg), 1 for netuid 13 (event and zero-run agree), 1 for netuid 9's old occupant via `payload.old_key`, 0 for a survivor; share `≤ eps` counts as zero, `> eps` does not; `gate_cross` +1/−1/0 and death-as-leaving; `LabelSpec.step_bars` on both grids; `t` = bucket |
| `test_walk_forward.py` | folds tile non-overlapping, last short; `< 4` folds → `oos_credible False` with the advisory note; `n_oos_folds` excludes folds with 0 OOS dates; `slice_panel` carries `ts/occ/pit` and truncates forward returns at the window edge; single-factor mode equals run on the fold slices; non-`ret_log` label refused |
| `test_survival.py` | synthetic: bottom tercile dies 3× the top → `ratio ≥ 2`; zero-emission-only deaths raise `death_ratio_low_over_high` but not `death_ratio_dereg_only`; a death seen from two start dates (`step_days < horizon`) counts once in `n_events`; `n_events_by_kind` and `n_event_dates`; top 0 / bottom > 0 → +inf; both 0 → 0.0; censored obs excluded; `min_depth=0` includes sub-floor names |
| `test_rank_stability.py` | identical factor across dates → autocorr 1.0, quintile and top-20 turnover 0; reversed ranks → −1.0; `n < 10` → nulls; netuid-sorted input makes results independent of dict order; matches 12's `rank_stability_daily` numbers on the same fixture values |
| `test_gate.py` | each criterion pass/fail on hand-built inputs; overlapping step → c1 false; c4 with one trial → `trial_log_too_small`; c5: 3-fold live scope is advisory and a 6-fold passing survivorship scope passes, a failing 6-fold scope fails, NaN decay fails; c6 +inf ratio with n ≥ 10 passes, `not_health` for `velocity`/`P6`; c7 absent → `pass None`, `gate_pass False`, `pending ["c7"]`, c7 file `{days: 30, entry_coverage: 0.61, n_entries: 20}` → pass, `n_entries: 19` → false, `n_entries` missing → null, `live_signal_coverage` alias; u1: pooled t 3.1 on a 4-fold scope + c6 pass + gate_pass → true, t 2.9 or 3 folds or c6 fail → false; `checks`/`upsize_gate_pass`/`promotable` (false for `quality_cg`); JSON serialisable with `allow_nan=False` |
| `test_trials.py` | distinct `trial_key` counting (pit_mode/explicit start/end/spec change → new trial; rerun, also on a different snapshot with defaulted start/end → same); entries never removed; entry recorded before compute (simulate crash → counted, `results` null); `trial_sharpe_var` = sample variance of `abs(ic_ir)` over newest results; atomic rewrite |
| `test_snapshot.py` | `resolve_db`/`newest_backup` pick the newest `taosentinel-<stamp>[-tag].db`, skip `*.db.tmp`, `*.db.gz`, `manual/` and stamps whose manifest says `quick_check ≠ ok`; refuse `DB_PATH` (samefile) and a path with a live `-wal`; `make_snapshot` writes exactly the 11 relations with `v_quant_*` materialised as tables and the harness SQL giving identical panels on source and snapshot; coverage copy + sha256; `--prune` keeps `QUANT_SNAPSHOT_KEEP`; `db_fingerprint` stable |
| `test_report_shape.py` | `save_factor_json` output parses with a vendored copy of `_rows_from_factor` (`backtest_api.py:173-195`) and yields the expected rows; the `float()`-cast fields are never null; `max_depth inf → null`; `timestamp` ends in `Z`; sidecar `ic_series_*` written; verdict carries 15 §2.7's keys with INTEGER `generated_at` |
| `test_cli.py` | subprocess smoke: `snapshot`, `factor`, `survival`, `walk-forward`, `rank-stability`, `report --run-all`, `report --from-results`, `gate --scopes live,survivorship` on the fixture DB → exit 0/1, files exist, verdict schema valid, final `RESULT` line parses; `--scope both --output P` writes `P_live`/`P_survivorship`; `--label demis --horizons 28` builds `demis_28` in days; `report` without `--run-all`/`--from-results` → exit 2; `gate --scopes live` → exit 2; SQLite path for `--observe-telemetry` → exit 2; `QUANT_ENABLED=False` → exit 3; `--min-names 5` → argument error |
| `test_weekly_job.py` | children are spawned with `nice -n 10` (subprocess mocked); child exit 1 → success with note, exit 2 → `collect()` raises and the 01 wrapper writes `collector_runs.ok=0`; `fail_alert_threshold = 2` fires `collector_failing:quant_weekly` once; cancellation kills the child; `control_state` pause and `data/control/quant_weekly.paused` → skipped with `paused: "operator"` / `"emergency_file"`; `/health` block has `last_gate_pass`, `last_verdict_file`; trigger = `CronTrigger(day_of_week="sun", hour=5, minute=0, timezone=UTC)` |

Mocks: `git rev-parse` (subprocess), child processes in `test_weekly_job.py`, `settings.DB_PATH`, 01's `AlertLatch`. Nothing else needs mocking — no network.

---

## 10. Verify checklist (Phase 10 done when all pass on the laptop)

```bash
pytest tests/quant -q                                    # expect: all passed, 0 skipped, < 60 s
python -m app.quant snapshot                             # expect: "snapshot written data/quant/snapshots/quant_taosentinel-<stamp>.db (tables: 11, size …)"
                                                         #         source = newest data/backups/taosentinel-<stamp>.db (uncompressed)
python -m app.quant snapshot --from data/taosentinel.db  # expect: exit 2 "refusing to read the live writer DB"
python -m app.quant snapshot --from "$(ls data/backups/taosentinel-*.db.gz | tail -1)"   # expect: exit 2 (compressed)
python -c "import glob,sqlite3; from app.quant.dereg_events import load_daily_panel, detect_dereg_events; p=sorted(glob.glob('data/quant/snapshots/quant_*.db'))[-1]; con=sqlite3.connect(f'file:{p}?mode=ro', uri=True); con.row_factory=sqlite3.Row; print(len(detect_dereg_events(load_daily_panel(con))))"
                                                         # expect: ≥ 6 (SN115×2, SN119×2, SN96, SN82 recovered from the imported ledger) + any post-June deaths
python -m app.quant factor --factors size,age,momentum --horizons 7,28,84 --scope survivorship --print
                                                         # expect: console table; `size` and `age` t-stats of the same sign and similar magnitude as the old findings
                                                         #         (size ≈ −4, age ≈ +4 on the 2025-09→2026-06 span) — proves the port reproduces the old panel
python -m app.quant factor --factors emission_share,active_authors_28d,quality_cg --horizons 7,28,84 --scope both \
       --controls size,emission_share --trade-tao 2 --deflate --pit-mode reconstructed
                                                         # expect: two files; every result has pit_mode=reconstructed, evidence_class=reconstructed,
                                                         #         n_none_factor > 0 for active_authors_28d before the first GitHub backfill day,
                                                         #         provenance.coverage_json set (or warning coverage_json_missing)
python -m app.quant factor --factors owner_conviction --horizons 7 --scope survivorship   # expect: exit 2 naming owner_commitment (02 §C2)
python -m app.quant factor --factors site_uptime_30d --horizons 7 --scope live --pit-mode reconstructed
                                                         # expect: exit 1, insufficient_data, avg_names 0 (forward-only key returns None historically)
python -m app.quant walk-forward --factors quality_cg --horizons 7 --scope both --test-days 28
                                                         # expect: survivorship n_oos_folds ≥ 4; live n_oos_folds 2-6 with "advisory only" when < 4
python -m app.quant survival --factors quality_cg --scope both --min-depth 0
                                                         # expect: n_events, n_event_dates, n_events_by_kind per scope; ratio 0.0 when both terciles have
                                                         #         0 events, null + ratio_inf=true when only the top has 0
python -m app.quant rank-stability --factors quality,composite_smoothed --scope survivorship --lag 1
                                                         # expect: quality mean_rank_autocorr ≥ 0.9 (master §7.7), close to 12's rank_stability_daily lag-1 health values
python -m app.quant report --run-all                     # expect: gate_verdict_quality_cg_<ts>.json with c1-c7 + u1_upsize, gate_pass=false, pending=["c7"], promotable=false
python - <<'EOF'
import json,glob; v=json.load(open(sorted(glob.glob('data/quant/results/gate_verdict_*.json'))[-1]))
assert set(v['criteria'])=={'c1_ic_both_scopes','c2_survives_controls','c3_deciles_net','c4_dsr','c5_walk_forward','c6_survival','c7_observe_telemetry','u1_upsize'}
assert v['upsize_gate_pass'] == v['criteria']['u1_upsize']['pass']
assert {'gate_pass','upsize_gate_pass','checks','observe_telemetry','spec_version','factor'} <= set(v) and isinstance(v['generated_at'], int)
assert v['provenance']['git_sha'] and v['provenance']['db_snapshot']['sha256'] and v['trials']['n_trials']>=1; print("verdict ok")
EOF
jq '.n_trials' data/quant/results/trials.json            # expect: equals the number of distinct study tuples run above (≥ 9)
python -m app.quant factor --factors quality_cg --horizons 7 --scope survivorship --seed 0 --output /tmp/a.json && \
python -m app.quant factor --factors quality_cg --horizons 7 --scope survivorship --seed 0 --output /tmp/b.json && \
python -c "import json;a=json.load(open('/tmp/a.json'));b=json.load(open('/tmp/b.json'));a.pop('timestamp');b.pop('timestamp');a['provenance'].pop('command');b['provenance'].pop('command');assert a==b;print('reproducible')"
curl -s localhost:8090/health | jq '.collectors.quant_weekly'   # expect: 01's collector block + last_gate_pass/last_verdict_file (null until first run);
                                                                 #         next_run = next Sunday 05:00Z when QUANT_WEEKLY_JOB_ENABLED=true
```

**Built ahead of Phases 7–9 (2026-10-17).** Until 12's scoring backfill (`replay_history`, 17 S10) writes `stage='norm'` rows and `scores`, every PIT factor (`quality_cg`, `P1`…, metric keys, `raw:`) is `None` → those studies exit 1 (`insufficient_data`) and the verdict's c1–c6 fail with reasons. Runnable now: snapshot/refusals, `dereg_events`, the chain factors (`size,age,momentum`, …, `chain:emission_share`), `evidence:` keys (05's raw rows, live-only so far), every label, survival/rank-stability/walk-forward on chain factors, `report --run-all` mechanics, `/health`. The `quality_cg`/`emission_share`/`active_authors_28d` lines above wait for Phase 8 (+ 06's `backfill_github` for P2).

**First study to run at Phase 10 (pre-registered):** `report --run-all` with `QUANT_GATE_FACTOR=quality_cg`, `QUANT_GATE_HORIZON_DAYS=7`, `--pit-mode reconstructed`, both scopes; supplementary `factor --factors P1,P2,emission_share,gate_margin,active_authors_28d,commit_days_90d,tenure,evidence:owner_locked_share --horizons 28,84 --label demis` and `--horizons 28 --label gate_cross` (day horizons; `owner_commitment` joins once an 11 replay writes history, §2.5). This verdict is research evidence: `quality_cg` is never `promotable` (§2.7); the promotion verdict for 15 is a later `gate --factor quality` run. Expected pitfalls to read the output against: (1) **size confound** — the old static quality overlay's IC vanished under `--controls size` (t −2.1 → −0.5); expect the same for anything liquidity-correlated, hence c2 is decisive; (2) **~6 deaths in 8 months, 5 of them below 12 τ depth** — with `min_depth=3000` the bottom decile is healthy-small, not dying; the survival route needs `--min-depth 0` and will still likely show `n_events < 10` on the live scope; (3) **2-6 folds on the live scope** → c5 advisory there (c5 is then decided by the survivorship scope alone, §2.7); (4) **three emission regimes** (`taoflow` → `price_burn` with the 2026-06-22 57-subnet cut → `gate_v440`) inside the 365-d window — read `by_regime`; `gate_margin`/`gate_cross` are only meaningful in `gate_v440`; a pooled IC that is carried by one regime is not a stable factor; (5) the high-hazard tier historically paid +6 % median forward (thin-cap rebound) — a negative IC for a "safety" factor is expected, not a bug; (6) 39 % missing 4 h buckets in the imported era → watch `median_gap_seconds` ⚠ sparse; (7) DSR clears 0.95 for nearly everything on ~200-date panels — necessary, not sufficient; (8) `github_org_membership_asof_today` caveat stays in every P2 result until 90 d of live GitHub observation exist; (9) if the Pi ledger was not recovered, the June→September 2026 price path comes from TMC candles + OnFinality depth (spec 17) — check `provenance.price_sources` and `coverage.json` holes before trusting the live scope; (10) zero-emission deaths cluster on 2026-06-22 (57-subnet cut) and on miner-burn-zeroed subnets — read `n_events_by_kind`/`n_event_dates` before believing a survival ratio.

---

## 11. Open questions / operator decisions

1. **Storage — resolved by 02 §J2 / 03 §3.7.** 13 reads 03's `v_quant_*` views (names preserved, rao → τ), `metric_values` `stage='norm'`, `scores.computed_at`/`input_cutoff`, `regime_daily(day_ts, label)`. *Resolved 2026-10-17:* 05 writes `sse_rollup` 1 h bars live, so the live scope has same-day prices; no view change is needed.
2. **Occupancy on imported rows (17):** the old ledger's `network_registered_at` is the *current* occupant back-stamped on older rows (research 01-factor-validation-tooling §1); 17's `occupancy.py` (03 §2.9) drops untrusted values and stamps `price_bars.occupant_key` from archive-RPC intervals. Bars it could not attribute have `occ = None` and PIT factors `None` there — the price-reset rule still realises deaths; `coverage.occupant_coverage` reports the share.
3. **Gate operationalisation of master §10 items 3–5:** master gives no statistic for "holds net-of-cost" and no scope for items 3–5. This spec pre-registers `net spread_t ≥ 2` and `monotone_score ≥ 0.6` on both scopes (c3), DSR on both scopes with N and V from the trial log (c4), and "every scope with ≥ 4 OOS folds passes, at least one exists" (c5). Confirm before the first `report --run-all` (changing it afterwards is a logged trial).
4. **Gate horizon:** 7 d is pre-registered (the live scope's 4 h horizons are ≤ 7 d). If the operator prefers 28 d, the live horizon list must include 168 bars and the live scope will have even fewer independent dates.
5. **Decision instant = `Bar.bucket`** (master §10 "observed_at ≤ bucket"; the earlier draft used `Bar.ts`, which is later and therefore less strict). On the 24 h grid the day-D official score (computed ≈ D 00:15Z) is first usable on bar D+1; this is conservative, never look-ahead. 17 §2.5 still says "13 uses `ts_last` as decision time" — now it is only the entry-price instant.
6. **`reoccupied` death realised at −1.0** when the collapse bar was not captured. Alternative: drop such observations. −1.0 is conservative and matches the ≈ −100 % price-reset semantics; disagreement recorded, master decision (death-inclusive) implemented.
7. **Emission regime boundaries** are dated from research (Taoflow "Nov 2025" activation date unknown; price-based emission landed in v3.4.8-423 on 2026-06-25 per 01-web-chain-apis, while 01-factor-validation-tooling cites v3.4.6-421 on 2026-06-23; v440 on 2026-07-28 per 01-web-ecosystem-2026 — all activation blocks UNVERIFIED). The `taoflow` label covers 2025-09-22 → 2026-06-24 including the pre-Taoflow tail; supply exact activation blocks if known. *Data answer 2026-10-17:* Taoflow did zero emission for net-outflow subnets — 70 % of 2025-10 `chain_daily` rows are ≤ 1e-4 % (41 % overall), so zero-emission "deaths" are inflated in that regime (item 13). *Correction 2026-10-22 (BUILD-LOG):* that reading was wrong. TMC delivered `emission_percent` truncated to whole percent until 2025-11-16 (100 % of 2025-09/10 rows are integers; SN43 0.92 % → 0), so most of those zeros are sub-1 % subnets, not zero emission: the chain's `tao_in_emission` was 0 for 6 of 129 netuids on 2025-10-01. Migration 0005 makes `v_quant_chain_daily` use the RPC share before 2025-11-17, which removes the artefact from the zero-emission runs and `death_90d`.
8. **P4 history (02 §C2/§J2, 11 §11 item 12):** `owner_retention_30d` depends on TMC `stake-removed` pagination depth (UNVERIFIED) or a TaoStats key (master §13 item 2); `owner_commitment` and `control_stability_90d` have reconstructible inputs but no replay in 17 §2.11 writes their rows (11 is not a replay owner) — until one does they are `None` historically and master §10's "honestly backtestable now" for them is not yet true.
9. **Observe telemetry file** (`QUANT_OBSERVE_TELEMETRY_PATH`, exported from the bot by the operator) uses 15's names `{since, days, entry_coverage, n_entries}`. *Resolved 2026-09-24:* 15 §2.7 defines the export (`python -m app.sentinel.telemetry export --out PATH`, schema `sentinel-telemetry-1`, incl. per-entry decided/applied factors) and 15 §8.4 row C passes that JSON as `--observe-telemetry`; a SQLite path is still refused (exit 2).
10. **Weekly job default off.** Turn on only with `MemoryMax=3G` (or run the CLI outside the unit) and after the daily backup (16) is confirmed to finish before the 04:30Z window; it runs Sun 05:00Z (02 §E3).
11. **Promotion factor (15):** 15's `GateVerdict` checks only `gate_pass` and `spec_version`, so a verdict for any factor (e.g. `quality_cg`) would enable active mode for the `quality`/`composite` feed. 13 emits `promotable`. *Resolved 2026-09-24:* 15 §2.7 requires `promotable is True` (plus factor `quality` and the feed's `spec_version`) for `ok`, else observe.
12. **Requests to 01/16:** ~~01's wrapper should honour a per-job `fail_alert_threshold`~~ — done (01 `fail_alert_key`), and 01 gained `timeout_key` with this spec (§8.8); 16's `ops_digest_daily` may add one line from `/health.collectors.quant_weekly` (13 sends no Telegram of its own, §3.7).
13. **c6 and zero emission (operator decision).** *Data 2026-10-23 (first verdict, BUILD-LOG):* with the union label c6 fails for `quality_cg` (ratio 0.66 live / 0.74 survivorship; zero-emission spells are 78/96 and 138/171 events) and `P1` (0.40 / 0.59), while the dereg-only diagnostic is 2.9 / 4.5 (`quality_cg`, 24 / 43 events) and 4.6 / 13.7 (`P1`, 24 / 40; partly mechanical, P1 contains `dereg_safety`). Changing the label now would be chosen after seeing results: a dereg-only c6 needs a pre-registered spec change and counts as a new trial. Master §10 criterion 6 says "death/zero-emission", implemented verbatim (union). In the data, ≥ 7-day zero-emission runs outnumber deregistrations ≈ 5 : 1 (288 vs 55) and many recover, so c6 mostly measures "falls to zero emission for a week". Options: keep the union (default); count only dereg-type deaths (the `death_ratio_dereg_only` diagnostic, §8.5); or raise `QUANT_ZERO_RUN_DAYS` (e.g. 30). Decide before the first `report --run-all` — changing it afterwards is a logged trial.
14. **`chain_buys` reconstructibility (17).** *Resolved 2026-10-22: forward-only on both sides (`RECONSTRUCTIBLE_KEYS` no longer lists it; 05's historical tier never produces it).* 17's `coverage.json.forward_only` lists `chain_buys`, while §2.5 (and the populated `chain_daily.chain_buys` column) say reconstructible. The harness follows `coverage.json` (owner of history) and warns; 17 should confirm which is right.
15. **Out-of-order build.** Phase 10 was built on 2026-10-17 before Phases 7–9 at the operator's request; `quant_weekly` is therefore available while `BUILT_PHASE = 6` (tests carry it as an explicit early job). Leave `QUANT_WEEKLY_JOB_ENABLED=false` until Phase 8's scoring backfill exists — a weekly verdict on an all-`None` factor is noise in the trial log.
16. **Emission history of dead occupants (request to 05/17).** *Resolved 2026-10-22 (20 N2 item 3): 17's `--emission-daily` pass + migration 0005; the text below is the original request.* No table holds emission share for occupants that died (TMC analytics return only the current occupant; `pool_history`/`price_bars` carry no emission). Consequence: `chain:emission_share` (c2's control), `demis`, `gate_cross` and zero-emission deaths see only survivors, and c2 is survivorship-biased (future-dead names lack the control and are dropped). Fix: persist `tao_in_emission`/`emission` from the RPC `DynamicInfo` cross-sections the archive replay already decodes (BUILD-LOG Phase 3 item 8 proposal) into a `chain_daily`-shaped history for every occupant. Until then the warning stays on and the c2 result should be read with it.

---

## 12. Sources

Research: `E:\TaoData\research\01-factor-validation-tooling.md` (primary: module map, line refs, gotchas, empirical priors), `01-web-quant-scoring-methods.md` §2 (PIT, rank stability), §4 (IC/ICIR thresholds, DSR/MinBTL, sample-size math), `01-fundamentals-scorer.md` §8 (look-ahead/size confound/slot reuse), `02-gapfill-data-continuity-and-backfill.md` §1 (data on disk), §2 (block↔time anchors, TMC candle block_number caveat), §5-6 (backfill plan), `01-web-ecosystem-2026.md` §1 (emission regime dates), `02-gapfill-keyless-onchain-pillar-source.md` (v440 gate).
Old bot (ported; line refs re-verified 2026-09-23): `E:\SN_Trader\SubnetTrader\app\backtest\factor_panel.py` (all; `Bar` 49-57, `segment_series` 83-101, `build_panel` 116-177, `forward_return` 180-202, SQL 248-256), `factor_backtest.py` (all; anchor 33-40, factors 63-203, `residualize` 324-338, `deflate_factors` 560-590), `validation.py` (all; `expected_max_sharpe` 164-173), `walk_forward.py:58-131, 157-183, 271-320, 462-604`, `quality_band.py:32-140`, `economics.py` (all), `slippage.py` (all), `dereg_analysis.py:48-96` (the old `reused_slots` 203-213 is replaced), `factor_report.py:14-160`, `app/utils/math.py:1-131` (via 12's `mathutil.py`), `app/backtest_api.py:57-101, 173-195` (result JSON consumer), `app/backtest/__main__.py:713-885` (CLI flag names), `tests/test_factor_backtest.py` (all; L44-46 row helper, L214-248 age/reg-block tests, L253-305 stamped-factor tests). Specs: `02-cross-cutting-conventions.md` (binding), `00-MASTER-SPEC.md` §2, §5, §6, §7, §10, §11, §12; `03` §2.6, §2.9, §3.1–§3.7, §3.9; `12` §2.12, §2.15; `15` §2.7, §8; `16` §2.5, §2.7, §6; `17` §2.9, §2.11, §3.4, §11; `11` §11 item 12; `18-operator-checklist.md` A3/A4 (seed data, Pi recovery).

## Changelog
- 2026-09-23 reconciliation + review:
  - J2/A2/B7: reads 03's `v_quant_*` views (rao → τ), `metric_values` `stage='norm'` (`value` = m, `raw:` = `value_json.raw`), `scores.computed_at`/`input_cutoff`, `regime_daily(day_ts, label)` uppercase with `UNKNOWN`; `coverage.json` reader (clamps, holes, caveats, golden-block check); `x_engagement_30d` look-ahead flag; snapshot only from the newest uncompressed `taosentinel-*.db` (never live DB, `*.tmp`, `*.gz`).
  - C1/C2/J2: master P4 names only (`owner_commitment`, `owner_retention_30d`, `control_stability_90d`, `owner_overhang` forward-only, `tenure`); retired names rejected; `evidence:` prefix for weight-0 raw keys (`owner_locked_share`).
  - B3/B6/12 §2.15: official-only rows on the 24 h grid, `shadow=0` for the primary spec, backfilled scores `eff = max(input_cutoff, as_of + lag)`; fixture `computed_at = as_of + 900`.
  - Master §10: PIT cutoff moved from `Bar.ts` to `Bar.bucket` ("observed_at ≤ bucket"); gate table now quotes §10 verbatim; c5 scope rule (advisory < 4 OOS folds), c6 Health-only with +inf ratio handled, overlapping steps refused.
  - Review: DSR bug fixed — with one factor the ported variance is 0 so SR0 = 0 and N is ignored; N and V now come from the trial log, `orient=False` for the gate.
  - Review: `Bar`/`PanelRow` keep the old positional order (ported tests would have broken); `reused_slots` rewritten (no `pool_history` in snapshots); `exit_capacity` forward-only; volume never mixed across sources; `block_times` anchors; survival counts distinct deaths by kind; rank stability matches 12 §2.12's statistics.
  - F2/E2/E3/E4: `quant_weekly` Sun 05:00Z, `JOB_TABLE` row for 01, liveness 1 211 520 s; pipeline in a niced child process (GIL/`os.nice` fix), memory note (16 `MemoryMax`).
  - B12/D1/D3/G6: `QUANT_KILL_SENTINEL` retired (control_state / PAUSE_ALL / `data/control/quant_weekly.paused`); no own Telegram digest; failures via 01's `collector_failing` with per-job threshold; 01's `/health` block + 2 extras.
  - B1/B9: INTEGER times (ISO `…Z` only at `timestamp`), `collector_runs` columns per 03; no rows written, `source='quant'` reserved.
  - 15: `gate_verdict_<factor>_<ts>.json` with `checks`, `observe_telemetry`, INTEGER `generated_at`, `promotable`, `u1_upsize`/`upsize_gate_pass` (OOS t ≥ 3 with ≥ 4 folds + survival); c7 needs `n_entries ≥ 20`; `gate` alias subcommand.
  - CLI flags, JSON fields and tests fully specified; verify commands made runnable.
- 2026-09-24 final pass: `polarity_7d` (P6, engagement-weighted) flagged look-ahead alongside `x_engagement_30d` (02 §J2) in §2.3, the §2.5 matrix and `test_backtestability`; the caveat now also propagates to `P6`/`velocity`.
- 2026-09-24 final pass: c7 input pinned to 15 §2.7's `sentinel-telemetry-1` JSON export (schema check, `entries[]` length check); §11 items 9 and 11 marked resolved. Verified `promotable`, `u1_upsize`/`upsize_gate_pass` in the verdict and `quant_weekly` Sun 05:00Z (no change needed).
- 2026-10-17 pre-build review against the live data (2026-10-17 backup) and the old source now on disk (`seed/app/backtest/`, `seed/app/utils/math.py`; old tests from the Pi recovery):
  - §2.2: price history is complete hourly from dTAO launch (`blockmachine` archive replay, 03 migration 0002) and live via 05's `sse_rollup` 1 h bars — the "39 % missing 4 h buckets" and "newest ≤ 7 days absent" caveats were obsolete; §11 item 1 resolved.
  - §2.3: `blockmachine` added to the Δ-`subnet_volume` sources (it holds 74 % of 1 h bars; unit τ settled by 17); `Bar.volume` = sum of the bucket's hourly Δ volumes (last-write-wins kept only one hour's Δ on a 24 h bar).
  - §2.3/§2.7: new chain-state factor `chain:emission_share` (previous day's `chain_daily` share); the `emission_share` control resolves to it, because `raw:emission_share` needs 12's norm rows (none exist) and c2 would have dropped every name.
  - §2.5: `coverage.json` `null` → no reconstructible history; 17's `forward_only` list overrides `RECONSTRUCTIBLE_KEYS` (`chain_buys` conflict → §11 item 14); `evidence:` keys of 05's replay added to `RECONSTRUCTIBLE_KEYS`.
  - §3.1: freshness fallback resolved through 03's catalog `source` (12's registry is Phase 8).
  - §3.4: trial key uses `start`/`end` as given, so weekly re-runs of the pre-registered study do not inflate N; `flock` on the trial log.
  - §5: 16's manifest key is `quick_check` (was UNVERIFIED `integrity`); tagged backup names accepted.
  - §2.4/§8.5/§11 items 7, 13: Taoflow zero-emission confirmed in data (41 % of rows ≤ eps; 288 zero runs vs 55 deregistrations) → dereg-only survival diagnostic and an operator decision for c6.
  - §8.1: `mathutil.zscore` aligned with the old sample-std version (ported Phase-1 test).
  - §8.8: 01 `JobSpec.timeout_key` makes `QUANT_WEEKLY_JOB_TIMEOUT_S` live; per-job alert threshold already existed (§11 item 12).
  - §10/§11 item 15: built ahead of Phases 7–9; what is runnable before Phase 8.
  - §2.3/§2.4/§11 item 16 (found while verifying): `chain_daily` (TMC analytics) has no rows for occupants that died, so the emission control and the day labels are survivor-only; measured, reported per result and flagged with a warning; request to 05/17.
  - §2.1/§8.2 (found while verifying on the real snapshot): the price-reset rule no longer fires inside one known occupant — 43 of 47 one-day collapses in the survivorship window were crashes of living occupants (launches, dust pools), and most re-registrations show no collapse; occupant boundaries are the death signal. `insufficient_data` = fewer than `QUANT_MIN_IC_DATES` (10) IC dates (the `2·horizon` rule could never hold at 84 bars). `segment_series`/`build_cross_sections` keep their ported signatures (`segment_deaths`, `counters=`) so the old tests run unchanged; `block_to_unix` extrapolates from the nearest anchor; `age` uses the exact `registered_ts`.
- 2026-10-22 (20 N2 item 3, BUILD-LOG 2026-10-22): §2.3/§3.1/§11 item 16 — `v_quant_chain_daily` now unions 17's daily RPC emission share, so `chain:emission_share`, `demis_*`, `gate_cross_*` and zero-emission deaths cover occupants that died (from 2025-02-14). §11 item 7 corrected: the 2025-09/10 "zero-emission" mass was TMC's whole-percent truncation (until 2025-11-16), not Taoflow; the view uses the RPC share before 2025-11-17. The RPC share matches TMC since 2025-12 at a median 0.0003–0.016 pp, but a few large subnets differ by up to 5 pp on a day (TMC's daily figure for them is not the instantaneous `tao_in_emission` share); that only touches occupant-days TMC lacks.
- 2026-10-22/23 (20 N2 items 5–6, BUILD-LOG 2026-10-23): §2.5 matrix — `chain_buys` forward-only (§11 item 14 resolved), `control_stability_90d` partial and replayed (11 §11 item 12; `RECONSTRUCTIBLE_KEYS` group `control_stability_90d`); §11 item 13 records the first verdict's union vs dereg-only numbers.
