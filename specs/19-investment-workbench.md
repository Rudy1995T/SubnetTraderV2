# 19 — Subnet Investment Workbench (charts, technicals, dereg distance, activity, ranking)

**Status:** `built` W2–W6 2026-10-16 (W0/W1 done earlier; W4b waits for Phases 4/6/9 — see §7) · **Phase:** a workbench track beside the master plan (§7); its jobs are declared with `phase = 3`, the phase whose data they need · **Owner of:** the `wb_*` and `chain_constants_probe` jobs, `app/features/technicals.py`, `app/features/dereg_distance.py`, `app/collectors/chain_probe.py`, `app/collectors/wb_github_probe.py`, `app/wb/` (screener, vision, study), `app/api/wb.py` (`/api/v1/wb/*`), `frontend/wb/` (`/wb`) · **Depends on:** 03 (DDL, migration 0003), 05 (chain snapshot, `chain_constants`), 17 (history), 14 (serving + auth), 16 (cost governor), 06/08/09 (full activity data, later), 13 (gate harness, later) · **Reconciled with 02 §K** (2026-10-16; this text now states every §K decision itself, and §K was amended to match the changes in §12) · **Research:** `research/03-*.md` (2026-10-09) and the chain reads of 2026-10-16 (§2.4).

---

## 0. Why this exists (operator ask, 2026-10-09)

The operator wants to "mess around": look at subnet charts (including screenshots they upload) and see which subnet looks like a good investment. Inputs: GitHub activity, how active the socials are, the alpha price, the distance from the **72 EMA band**, the distance from **deregistration**, and similar signals, with the saved chain history and saved backtests behind them.

TaoSentinel already designs most inputs (P1 chain, P2 GitHub, P5 community, a validation harness). Three things are designed nowhere else: a technical-indicator layer (the 72 EMA band), chart-image upload, and an interactive "what-if" ranking with adjustable weights. This spec adds them as a **workbench**: it reads the same database, writes only weight-0 evidence rows, and never changes the official `ts-1.0` score.

### 0.1 State on 2026-10-16 (verified)

| Item | Status |
|---|---|
| Data (W0) | **Done.** Service running since 2026-10-16 18:02Z (SSE = live chain source); `pool_history`/`price_bars` continuous from dTAO launch to live; 1 d bars from 2025-02-13, 1 h from 2025-02-14, 5 m TMC candles from 2026-06-19 (BUILD-LOG 2026-10-16). |
| Old bot seed (W1) | **Done except the legacy results copy.** Pi ledger imported (S1/S5), fundamentals (S2), regime (S7) on 2026-10-12/13. The old bot's 123 backtest result files were not yet under `data/quant/legacy/` → `scripts/import_legacy_results.py` (§2.8). |
| Keys | `GITHUB_TOKEN` and `ANTHROPIC_API_KEY` are **empty**. Everything in W2–W6 works without them, except the vision read (returns 409 `vision_disabled`) and the full GitHub P2 metrics (Phase 4). |
| Scores / harness | Phase 8 (scoring) and Phase 10 (harness, spec 13) are unbuilt. The workbench study (§2.8) is an exploration tool, not 13's gate. |
| **Chain change found 2026-10-16** | `NetworkImmunityPeriod` is now **432,000 blocks (60 d)**. It was 864,000 (120 d) at blocks 9,246,000 and 9,270,000 and is 432,000 at 9,285,000 and 9,295,000 (public archive reads), so it changed around 2026-10-12/14. The config value `NETWORK_IMMUNITY_PERIOD_BLOCKS=864000` (05) and TMC's `immune` flag are both stale (TMC still marks SN16, 837,617 blocks old, as immune). The workbench reads the live value (§2.4); 05 keeps its config value until the operator updates it (§11 Q2). |

### 0.2 What the history can answer (feasibility research, 2026-09-24 bar)
- EMA72 distance is computable for ~120 active occupants on 1 d bars and all on 1 h bars. The cross-section is skewed (median −16.6 % below the daily EMA72; 96/120 below a ±2σ band), so the screener sorts on a **rank** of the band position, not the raw number.
- Dereg distance reproduced TMC's bottom-3 badge exactly on 2026-09-24. The bottom cluster is tight (rank 4 was 0.45 % above the cull line).
- Of 50 rankable dead occupants, 49 were rank 1 in their last hour, but only 30 were bottom-3 seven days earlier and 5 thirty days earlier. Dereg rank is a **short-horizon** signal; sudden owner-exit collapses need a liquidity/owner-flow signal beside it.
- The old bot found short-horizon mean reversion (`meanrev_z` t −4.9) and momentum (`px_vs_moving` t +4.1) on its 8-month panel, **none** significant on the 6-week live panel, and 0/65 EMA configs passed the deflated-Sharpe bar. Community "72 EMA" use is informal. The band is an **exploration feature** until §2.8 says otherwise.

---

## 1. Scope

**In scope (this spec builds it)**
- **W2 technicals** — §2.3 evidence keys, job `wb_technicals`.
- **W3 dereg distance** — §2.4 evidence keys, network "weather" table, job `wb_dereg`, keyless job `chain_constants_probe`.
- **W4a activity stop-gaps owned here** — job `wb_github_probe` (unauthenticated `pushed_at`), TMC `commits_7d/30d/90d` surfaced from `chain_hourly`.
- **W5 workbench UI** — `/api/v1/wb/*` routes, the `/wb` page (screener, weight sliders, presets, per-subnet chart with EMA band and cull line, upload panel), Claude vision chart read stored as a note.
- **W6 study** — `python -m app.wb.study`: factor rank-IC and a matched-survivor death study on the saved panel, saved under `data/quant/results/`, listed in the UI; legacy results import.

**Not built here (owners build them in their phases; the screener shows them when their rows exist)**
- **W4b** — 06 P2 metrics (needs `GITHUB_TOKEN`), 08's keyless `discord_6h`/`telegram_daily`, 09's free `x_ctf_crosscheck`. 02 §K6 retired a workbench Discord job so absence/topology rules stay 08's.
- 13's harness and gates (Phase 10), official scores (Phase 8).

**Out of scope (v1)**
- Changing `ts-1.0` weights or adding scored keys (a `ts-1.1` `spec_version` with a gate pass, 12 §2.13).
- Trading or capital allocation (spec 15). The workbench is for a human analyst.
- Paid X data, LLM post classification (Phase 9).
- `dereg_recovery_ratio` (what a holder gets back when culled): the formula is UNVERIFIED against the chain's dissolve logic; deferred (§11 Q7).
- Wallet handling or the old bot's positions.

---

## 2. Design

### 2.1 Components and data flow

```
 TMC SSE (05 chain_sse) ─► chain_hourly / price_bars 5m,1h,1d / chain_network_hourly      (live, already running)
 backfills (17)         ─► pool_history / price_bars / occupants (exact registered_ts)
 public finney RPC      ─► chain_constants_probe (01:20Z) ─► chain_constants(source='rpc_storage') [+ chain_constant_drift]
 GitHub REST (no token) ─► wb_github_probe (hourly, ≤ 45 repos) ─► metric_values repo_pushed_days (source='github')
                                                                 │
   wb_technicals (hourly :12) ─► metric_values (source='chain', evidence keys §2.3)   [ema72_band_exit events, opt-in]
   wb_dereg      (hourly :09) ─► metric_values (source='chain', evidence keys §2.4) + wb_dereg_network_hourly + events
                                                                 │
   app/api/wb.py  GET  /api/v1/wb/screener · /subnet/{netuid}/chart · /factors · /presets · /notes · /results
                  POST /api/v1/wb/presets · /api/v1/wb/chart-read            (POST behind 14's X-Auth-Token)
   frontend/wb/index.html  served at GET /wb (Plotly.js from cdnjs, no Node build)
                                                                 │
   python -m app.wb.study ─► data/quant/results/wb_study_<date>.json (+ wb_trials.jsonl) ─► GET /api/v1/wb/results
```

**Time model** (02 §B, 12 §2.2): INTEGER unix seconds everywhere. Every row carries `as_of` (the slot at which the value became knowable), `event_at` (close time of the last bar used) and `observed_at` (write time). Only **complete** bars are read: a bar is complete when `ts + period_seconds ≤ now`. The UI shows "as of the last closed bar".

### 2.2 Price series contract
- Closes come from `price_bars` (`period ∈ {1h, 1d}`; PRIMARY KEY `(netuid, period, ts)`, so cross-source duplicates cannot exist). Rows are selected by `occupant_key = <the occupant>`, never by netuid, so a predecessor's bars never leak (42 netuids carry predecessor bars).
- **Window:** the last `WB_EMA_WINDOW_BARS` (default 432 = 6 × 72) complete bars. The EMA is seeded with the SMA of the first 72 bars of the window; after the remaining ≥ 288 bars the seed's weight is `(1−2/73)^288 ≈ 0.0003`, so the value equals a full-history EMA to < 0.1 % and every run is deterministic and cheap.
- **Gaps:** the EMA runs over the bars that exist (no forward fill). `value_json.gaps` = missing buckets inside the window span. More than 10 % missing → `failed` (`reason: window_gaps`).
- **Staleness:** if the occupant's last complete bar is older than the expected last bucket by more than `WB_STALE_BARS_1H` (3) hours or 1 day for 1 d keys → `failed` (`reason: stale_bars`). A smaller lag is written `ok` with `value_json.stale_bars`.
- Fewer than `WB_EMA_MIN_BARS` (72) bars → `failed` (`reason: insufficient_bars`). With fewer than 2 × 72 bars the EMA is still dominated by its SMA seed: the row is `ok` with `value_json.warmup = true`, and the chart says so (a young subnet's 1 d band is wide).

### 2.3 Technicals (evidence keys; weight 0; `source='chain'`; `spec_version='ts-1.0'`; `value_json.def='wb-1'`)

Hourly keys (`as_of` = top of the current hour, i.e. the close of the last complete 1 h bar):

| metric_key | value | value_json |
|---|---|---|
| `ema72_1h` | EMA (α = 2/73) of the 1 h closes (§2.2 window) | `n, gaps, close, bar_ts` |
| `ema72_dist_1h` | `close / EMA72 − 1` | `ema` |
| `ema72_band_1h` | band half-width in log units `k·σ72`; `σ72` = RMS of `ln(close/EMA72)` over the last 72 bars (how far the price typically strays from its own EMA — Bollinger's dispersion, centred on the EMA), `k = WB_EMA_BAND_K` (2.0); fewer than 24 deviations → `failed insufficient_bars` | `upper = EMA·e^{kσ}, lower = EMA·e^{−kσ}, mode='sigma', k, sigma` |
| `ema72_band_pos_1h` | `ln(close/EMA72) / (k·σ72)`, clipped to [−2, 2]; 0 = on the EMA, ±1 = on the band edge, \|x\| > 1 = outside — **"the distance from the 72 EMA band"**. *(σ of one-bar returns was tried first on 2026-10-16: AMM alpha prices are smooth bar to bar, so that band was ±0.1 % on SN64's 1 h chart and most subnets sat on the ±2 clip; the dispersion around the EMA is the honest width.)* | `raw` (unclipped) |
| `px_vs_moving` | `ln(close / moving_price)` of the last complete 1 h bar (the chain's own ~9 h-half-life EMA; a short-horizon momentum read) | `moving_price` |

Daily keys (`as_of` = the official `00:00Z` slot after the last complete day; written by the first run after 00:00Z, missed days caught up for 7 days with `backfilled=1`):

| metric_key | value |
|---|---|
| `ema72_1d`, `ema72_dist_1d`, `ema72_band_1d`, `ema72_band_pos_1d` | as the 1 h keys, on 1 d closes (EMA72@1d ≈ one quarter) |
| `ema72_band_rank_1d` | cross-sectional percentile rank of `ema72_band_pos_1d` over the occupants with an `ok` value on the same `as_of`: `(r−1)/(n−1)` with average ties, in [0, 1]; `value_json.n` |
| `rv_30d` | sample std of the last 30 daily log returns × √365 (≥ 20 returns, else `failed insufficient_bars`) |
| `dd_90d` | `close / max(close over the last 90 daily bars) − 1` (≥ 14 bars) |
| `ema72_cross_days_1d` | days since the last sign change of `close − EMA72` inside the window; `value_json.side` = +1 above / −1 below; no cross in the window → the window length with `value_json.censored = true` |

Absence: a pre-launch occupant (`occupants.status = 'pre_launch'`) → `not_applicable` for every key. `WB_EMA_BAND_MODE=atr` is not implemented in v1 (true highs/lows exist only on `tmc_candle` bars from 2026-06-20); the setting is validated to `sigma`.

### 2.4 Distance from deregistration (extends 05 §2.5)

**Chain rules** (subtensor source, research 03-web-dereg-mechanics; re-read live 2026-10-16):
- A subnet is culled **only** inside a successful `register_network` while the subnet count is at `SubnetLimit` (128): exactly one per registration, the **lowest `SubnetMovingPrice` among non-immune subnets** (no entry = not started = 0; ties → oldest `NetworkRegisteredAt`). Emission-disabled subnets are in the pool (SN29 has survived 815 d disabled, so `emission_disabled` is not a pruning input).
- Immunity: `head − NetworkRegisteredAt < NetworkImmunityPeriod`, **live value 432,000 blocks (60 d) since ~2026-10-13**.
- Registrations: global rate limit `NetworkRateLimit` = 14,400 blocks (2 d); last registration block = `LastRateLimitedBlock[NetworkLastRegistered]` (storage map key suffix `0x02`; 9,289,316 = SN86 on 2026-10-15). Lock cost = `max(NetworkMinLockCost, 2L − L·Δ/I_eff)`, `L = NetworkLastLockCost`. The stored `NetworkLockReductionInterval` (57,600) is scaled by emission (effective ≈ 28,800 on 2026-10-16), so the probe reads the **exact** cost from the runtime API `SubnetRegistrationRuntimeApi_get_network_registration_cost` (830.7 τ at block 9,298,478) and derives `I_eff = L·Δ/(2L − cost)`.
- Registrations cluster about a week apart (12 in the 90 d to 2026-10-16). The moving price is an EMA of spot with α ≈ 0.0003/block (half-life 8–10 h), so spot leads it by well under a day.

**Inputs at slot `as_of`** (top of the hour; the job runs at :09): the 05 snapshot of the hour that just closed (`chain_features.snapshot_rows`: per netuid the latest live `chain_hourly` row, with `price`, `subnet_moving_price`, `network_registered_at`, `block_number`, `is_active`); `head` = the max `block_number`; `IMM` = the newest `chain_constants` row `NetworkImmunityPeriod` with `source='rpc_storage'` observed ≤ `as_of`, else `NETWORK_IMMUNITY_PERIOD_BLOCKS` (`value_json.imm_source` says which). Registrations come from `occupants.registered_ts` (exact, 17).

**Definitions** (per occupant, netuid ≠ 0; `mp` := 0 when NULL):
- `P` = occupants with `head − reg ≥ IMM` (not immune), **including** not-started and emission-disabled ones; ordered by `(mp asc, reg asc)`.
- `reg_gap_mean` = mean gap between consecutive registrations whose `registered_ts` is in the last `WB_DEREG_REG_GAP_WINDOW_DAYS` (90); fewer than 2 → `WB_DEREG_REG_GAP_FALLBACK_DAYS` (8).
- For each `H ∈ WB_DEREG_HORIZONS_DAYS` (30, 90): `P_H = P ∪ {immune occupants whose immunity ends within H days}`, at their current `mp`; `k_H = clip(round(H / reg_gap_mean), 1, H // 2)` (≈ 4 and 12 today).

| metric_key | definition | absence |
|---|---|---|
| `immunity_remaining_days` | `max(0, reg + IMM − head) · 12 / 86,400` | — |
| `dereg_rank` | 1-based rank in `P` | immune → `not_applicable`, `value_json.graduates_at` (unix s) |
| `dereg_rank_30d`, `dereg_rank_90d` | rank in `P_H` | immune beyond H → `not_applicable` + `graduates_at` |
| `cull_line_1` | lowest `mp` in `P` excluding self | — |
| `cull_line_k_30d`, `cull_line_k_90d` | `k_H`-th lowest `mp` in `P_H` excluding self | — |
| `mp_gap_to_cull_1`, `mp_gap_to_cull_k_30d`, `mp_gap_to_cull_k_90d` | `mp / cull_line − 1` — **"the % distance from deregistration"** (negative = below the line) | a zero line → `not_applicable` (`reason: cull_line_zero`) |
| `dereg_safety_30d`, `dereg_safety_90d` | `1.0` if `immunity_remaining_days > H`, else `clip((rank_H − k_H) / (|P_H| − k_H), 0, 1)` — the proposed replacement for 05's bottom-3 rule (§11 Q3); evidence only | — |
| `hours_in_bottom_k_30d` | hours of the last 720 in which the occupant would rank ≤ `k_30` under **today's** `IMM` and `k_30`, rebuilt from 1 h `price_bars.moving_price` (a what-if under current rules, not a record of past badges) | fewer than 24 rankable hours → `failed insufficient_bars` |
| `spot_rank` | rank in `P` by spot price | immune → `not_applicable` |
| `spot_vs_mp` | `price / mp − 1` (spot leads the moving price; negative = the moving price is still falling toward spot) | `mp = 0` → `not_applicable` |
| `days_to_cull_static` | `eta_next_registration_days + (rank_30 − 1) · reg_gap_mean`: "if nothing moves" | not in `P_30` → `not_applicable` |

All rows use `as_of`, `event_at` = the snapshot's `event_at`, `source='chain'`; pre-launch occupants are still ranked (they are in the pool at `mp = 0`) — this is the one place a pre-launch occupant is not `not_applicable`, because the chain treats it as the first cull candidate.

**Network row** (`wb_dereg_network_hourly`, one per `hour_ts = as_of`): `head_block, immunity_period_blocks, immunity_source, pool_size, pool_size_30d, pool_size_90d, k_30, k_90, reg_gap_mean_days, n_regs_window, last_registration_block, last_registration_ts, earliest_next_registration_block, eta_next_registration_days, lock_cost_tau, lock_cost_decay_tau_per_day, lock_cost_observed_at, graduates_30d, graduates_90d, cull_line_1` + `observed_at, run_id`. `eta_next_registration_days = max(0, earliest-allowed by rate limit, last_registration_ts + reg_gap_mean − as_of)`. `lock_cost_tau` is extrapolated from the newest probe with `I_eff`. The row lives in its own table (owner 19) rather than as new columns on 05's `chain_network_hourly`, so the SSE recorder's `INSERT OR IGNORE` and this job never write the same row.

**`chain_constants_probe`** (daily 01:20Z, keyless `WB_CHAIN_PROBE_URL`, every JSON-RPC step under `WB_CHAIN_PROBE_TIMEOUT_SEC`, provider `subtensor_rpc` through `Http`): reads at the finalized head `NetworkImmunityPeriod`, `SubnetLimit`, `NetworkRateLimit`, `NetworkMinLockCost`, `NetworkLastLockCost`, `NetworkLockReductionInterval`, `SubnetMovingAlpha`, `LastRateLimitedBlock[0x02]` (stored as `NetworkLastRegisteredBlock`) and the runtime-API cost (`NetworkRegistrationCost`), and writes them to `chain_constants(source='rpc_storage', endpoint, block_number)`. It emits 05's `chain_constant_drift` (`{name, config, live, block}`, dedupe `name|live`) when `NetworkImmunityPeriod` differs from `NETWORK_IMMUNITY_PERIOD_BLOCKS`, or when `SubnetLimit`/`NetworkRateLimit`/`NetworkMinLockCost` differ from their previous probed value (`config` = the previous value). 05 catalogues the event as an immediate alert; that routing is kept. **On the first run after this spec is deployed it fires for `NetworkImmunityPeriod` (864,000 → 432,000).**

**Events** (`source='chain'`, digest routing, never pages):

| Type | Severity | When | Payload | Dedupe |
|---|---|---|---|---|
| `dereg_rank_entered_k30` / `dereg_rank_cleared_k30` | info | `dereg_rank_30d ≤ k_30` flips between the previous hourly `ok` row and now (no event on the first run) | `netuid, rank, k, pool_size, mp_gap_to_cull` | `occ\|{d7}` |
| `immunity_ends_7d` | warning | `0 < immunity_remaining_days ≤ 7` | `netuid, graduates_at, dereg_rank_30d` | `occupant_key` |
| `ema72_band_exit` | info | `ema72_band_pos_1d` leaves [−1, 1] (previous day inside); only with `WB_EVENTS_ENABLED=true` | `netuid, band_pos, prev_band_pos, side` | `occ\|day` |

05's `dereg_risk_entered`/`_cleared` (bottom-3 + TMC badge) are unchanged until §11 Q3 is decided.

### 2.5 Activity (GitHub + socials), keyless first

| Signal | Built here (no key) | Full source (owner, later) | Caveats |
|---|---|---|---|
| GitHub commits | TMC `commits_7d/30d/90d` from the newest `chain_hourly` row (declared repo only) | 06 P2 (`GITHUB_TOKEN`): org-wide human commits, authors, bus factor | Chutes shows 0 while its org ships weekly; private development reads as unobservable |
| Last push | `wb_github_probe`: `GET https://api.github.com/repos/{owner}/{repo}` for `subnets.github_url`, unauthenticated (60 req/h/IP; the limiter's no-token bucket is 50/h), ≤ `WB_GITHUB_PROBE_PER_RUN` (45) repos per hourly run, each repo once per UTC day → evidence key **`repo_pushed_days`** (`source='github'`, `as_of` = the day's `00:00Z`): days since `pushed_at`; `value_json.{repo, pushed_at, archived, stars}` | 06 `declared_repo_freshness` | no link → `absent` (value NULL, `reason: no_github_link`); 404/451 → `failed` (`reason: http_404`, unobservable, not idle); archived → `ok` with `archived: true`. The job is enabled only while `GITHUB_TOKEN` is empty; 06 takes over after. |
| Discord / Telegram / X | — (08/09, W4b) | 08 `discord_members`, `presence_ratio`, `tg_last_activity_days`; 09 `x_last_activity_days` | the screener shows them when rows exist, else "—" |

Workbench composites (computed in the browser, never stored, 02 §K3):
- `dev_activity_wb` = mean of the available legs {`logcap(commits_30d, 60)`, `recency(repo_pushed_days, 14, 120)`}; no leg observed → None (shown "—"); `absent` repo → 0.
- `social_activity_wb` = mean of the available legs {`logcap(discord_members, 5000)`, `zclip(presence_ratio, .05, .35)`, `recency(x_last_activity_days, 7, 60)`, `recency(tg_last_activity_days, 7, 60)`}; no venue at all → 0.0 (02 §C3).

`logcap(x, c) = min(1, ln(1+x)/ln(1+c))`; `recency(d, a, b)` = 1 for `d ≤ a`, 0 for `d ≥ b`, linear between; `zclip(x, lo, hi)` = clip to [0,1] of `(x−lo)/(hi−lo)`.

### 2.6 Screener and ranking ("which is a good investment") — exploration, not a score

`GET /api/v1/wb/screener` returns one row per active or pre-launch occupant (netuid ≠ 0): `netuid, occupant_key, name, status, price, chg_24h, chg_7d` (from 1 h closes), `mcap_tau, tao_in_pool, emission_pct` (newest `chain_hourly`), the §2.3/§2.4 values, `repo_pushed_days`, `commits_7d/30d/90d`, the 08/09 legs when present, `owner_locked_share` (05), and per key `{status, as_of, reason}` coverage. The response also carries the newest `wb_dereg_network_hourly` row, `as_of`, and `stale = true` when the newest hourly technicals or dereg row is more than 2 h old.

The page:
- **Table** sortable on every column, coverage badges (`absent` vs `failed`, 00 §1.3), the "stale > 2 h" banner.
- **Weight sliders** (−1 … +1 per column, sign = direction) re-rank client-side with `rank_wb = Σ wᵢ · pct(xᵢ)` over the visible rows (`pct` = percentile rank; a missing value contributes 0.5). Labelled *"exploration — not validated, not fed to the bot"*. Named presets persist in `wb_presets`. Each slider shows the factor's latest study IC from §2.8 when one exists. This keeps 12 §2.13 intact: scored weights change only through a new `spec_version` and a gate.
- **Chart panel** for the selected netuid: `GET /api/v1/wb/subnet/{netuid}/chart?period=1h|1d|5m&bars=N` returns complete bars of the current occupant (`ts, open, high, low, close, moving_price`), the EMA72 and band series computed by the same functions as §2.3 (so the plot equals the stored numbers), the current `cull_line_1`/`cull_line_k_30d` as horizontal lines (moving-price space), the immunity end time, and the occupant's workbench/dereg events.
- **Explain**: the row's raw values, cross-sectional percentiles, the network weather row, the newest study results.
- **Notes/upload panel** (§2.7).

### 2.7 Chart upload (vision) — evidence notes, never metrics

1. **Preferred:** the server-rendered chart (§2.6); every number is reproducible from the DB.
2. **Upload:** `POST /api/v1/wb/chart-read?netuid=N` with the raw image as the request body and `Content-Type: image/png | image/jpeg | image/webp` (no multipart, so no extra dependency), ≤ `WB_VISION_MAX_IMAGE_MB` (5) MB, behind `X-Auth-Token`. The image is stored as `WB_UPLOAD_DIR/<sha256>.<ext>` and a `wb_uploads` row is written. Answers: 413 too large, 415 wrong type, 409 `vision_disabled` (`WB_VISION_ENABLED=false` or no `ANTHROPIC_API_KEY`; the upload is kept, no note), 409 `paused` (`KILL_SWITCH`, or spend group `llm` paused/latched), 409 `budget` (governor denies the reservation).
3. With vision enabled, the image goes to Claude through the official `anthropic` SDK (`AsyncAnthropic`, imported lazily; the one sanctioned caller outside `Http`, 10 §2) — `client.beta.messages.create(model=WB_VISION_MODEL, max_tokens=16000, thinking adaptive, output_config={format: json_schema, effort: medium}, betas=["server-side-fallback-2026-07-01"], fallbacks="default")`. The schema asks for **relative** reads only: `trend` (up/down/sideways), `above_visible_mas` (true/false/null), `drawdown_from_visible_high_pct`, `support_pct_below`/`resistance_pct_above` (% of last price), `timeframe_guess`, `instrument_guess`, `axis_last_price_text` (as printed), `summary`, and a `confidence` (0–1) per field. Before the call: `limiter.acquire("anthropic")`, `governor.reserve("anthropic", WB_VISION_EST_USD, job="wb_chart_read")`; after: `commit` with the usage-priced cost (`claude-opus-5-5` $4/$20 per M tokens) and `record(requests=1)`; on any error `release`.
4. The result is a `manual_notes` row: `kind='chart_read'`, `occupant_key` = the netuid's current occupant, `created_by='wb_vision:<model>'`, `evidence_url` = the stored image path, `body` = the summary, `payload_json` = the structured read + `{model, status ∈ {ok, unreadable}, cost_usd, stop_reason, upload_id}`. `stop_reason: refusal`, `max_tokens` or invalid JSON → `status='unreadable'`, no retry. Known failure modes (hallucinated axis values, misread log scales, EMA ribbons) are why the read is advisory and never written to `metric_values`. `GET /api/v1/wb/notes?netuid=` lists notes.

### 2.8 Study (W6) — exploration until spec 13 exists

`python -m app.wb.study [--horizons 7,28] [--since YYYY-MM-DD] [--step-days 7]` rebuilds a **daily** panel in memory from `price_bars` 1 d (current-occupant bars only; dead occupants included up to their last bar): for each day `t` and occupant with ≥ 72 bars to `t`, the §2.3 daily factors (`ema72_band_pos_1d`, `ema72_dist_1d`, `dd_90d`, `rv_30d`), `px_vs_moving` (daily close vs bar `moving_price`), and the dereg factors `dereg_rank_hist` and `mp_gap_to_cull_1` from 1 d `moving_price` and registration blocks with `WB_STUDY_IMMUNITY_BLOCKS` (864,000, the rule in force for almost the whole history). Labels: forward log returns over each horizon (`close_{t+h}/close_t`, NULL when the occupant died before `t+h`, which is reported, not dropped silently) and `death_90d`.

Outputs per factor and horizon: mean daily Spearman IC, its t-stat on non-overlapping dates (every `h` days), hit rate, top-minus-bottom quintile mean return, `n_days`, `n_obs`. **Matched-survivor death study:** for every occupant deregistered after `--since`, its factor value at T−7 d and T−30 d against the three survivors nearest in `tao_in_pool` on the same day; reported as the share of deaths whose factor was worse than the matched mean, with a sign-test p-value. Results go to `data/quant/results/wb_study_<YYYYMMDD>.json`, every run appends one line to `data/quant/results/wb_trials.jsonl` (a trials count for later deflated-Sharpe work), and `GET /api/v1/wb/results` lists them. These numbers are **not** 13's gate; a factor earns weight only through 13 and `ts-1.1`.

**Legacy results:** `python -m scripts.import_legacy_results [--src <SubnetTrader>/data/backtest/results]` copies the old bot's 123 result files read-only to `data/quant/legacy/` and writes `data/quant/legacy/index.json` (`file, size, mtime, kind, title`); the results route lists them too.

---

## 3. Data contract

**Tables read:** `price_bars`, `chain_hourly`, `occupants`, `subnets`, `chain_constants`, `metric_values`, `manual_notes`, `wb_*`, and 08/09 rows when present.

**Tables written:** `metric_values` (through `put_metric_values`), `wb_dereg_network_hourly`, `chain_constants` (probe), `manual_notes` (`kind='chart_read'`), `wb_presets`, `wb_uploads`, `events` (through `emit_event`), `collector_runs`. Never `stage='norm'` rows, `scores` or `red_flag_state` (02 §K8).

**Migration 0003 `workbench`** (03 owns the DDL text; `-- @allow-destructive` because widening `manual_notes.kind` rebuilds that table, which was empty on 2026-10-16):
```sql
wb_presets(id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, weights_json TEXT NOT NULL CHECK (json_valid(weights_json)),
           created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)
wb_uploads(id INTEGER PRIMARY KEY, netuid INTEGER, occupant_key TEXT, path TEXT NOT NULL, sha256 TEXT NOT NULL,
           media_type TEXT NOT NULL, bytes INTEGER NOT NULL, model TEXT, status TEXT NOT NULL
           CHECK (status IN ('stored','ok','unreadable','failed','vision_disabled')), note_id INTEGER,
           created_at INTEGER NOT NULL)
wb_dereg_network_hourly(hour_ts INTEGER PRIMARY KEY, … §2.4 network fields …, observed_at INTEGER NOT NULL, run_id TEXT)
manual_notes.kind CHECK += 'chart_read'
```

**Metric keys** (03 §3.8, all evidence, weight 0, owner 19): `source='chain'` — `ema72_1h`, `ema72_dist_1h`, `ema72_band_1h`, `ema72_band_pos_1h`, `px_vs_moving`, `ema72_1d`, `ema72_dist_1d`, `ema72_band_1d`, `ema72_band_pos_1d`, `ema72_band_rank_1d`, `rv_30d`, `dd_90d`, `ema72_cross_days_1d`, `immunity_remaining_days`, `dereg_rank`, `dereg_rank_30d`, `dereg_rank_90d`, `cull_line_1`, `cull_line_k_30d`, `cull_line_k_90d`, `mp_gap_to_cull_1`, `mp_gap_to_cull_k_30d`, `mp_gap_to_cull_k_90d`, `dereg_safety_30d`, `dereg_safety_90d`, `hours_in_bottom_k_30d`, `spot_rank`, `spot_vs_mp`, `days_to_cull_static` (29); `source='github'` — `repo_pushed_days` (1). 30 keys; the catalog grows from 79 to 109. A horizon outside {30, 90} in `WB_DEREG_HORIZONS_DAYS` is rejected by the settings validator (no key exists for it).

**Events** (03 §3.9): `dereg_rank_entered_k30`, `dereg_rank_cleared_k30`, `immunity_ends_7d`, `ema72_band_exit` (§2.4); `chain_constant_drift` is 05's existing type.

**Absence vs failure:** see §2.2–§2.5; the screener shows `absent` as "none" and `failed`/missing as "?".

---

## 4. Configuration (01 `Settings`, group 19; defaults win per 02 §F1)

| key | type | default | meaning |
|---|---|---|---|
| `WB_ENABLED` | bool | `true` | registers the `wb_*` jobs, `chain_constants_probe` and the routes |
| `WB_EMA_PERIOD` | int | `72` | EMA length (bars); the metric keys keep the name `ema72_*` (other values are for exploration runs only) |
| `WB_EMA_WINDOW_BARS` | int | `432` | bars read per series (≥ 2 × period) |
| `WB_EMA_MIN_BARS` | int | `72` | below → `failed insufficient_bars` |
| `WB_EMA_TIMEFRAMES` | list | `["1h","1d"]` | subset of {1h, 1d} |
| `WB_EMA_BAND_MODE` | str | `sigma` | only `sigma` in v1 |
| `WB_EMA_BAND_K` | float | `2.0` | band half-width in σ |
| `WB_STALE_BARS_1H` | int | `3` | 1 h bars of lag tolerated before `stale_bars` |
| `WB_DEREG_HORIZONS_DAYS` | list | `[30, 90]` | subset of {30, 90} |
| `WB_DEREG_REG_GAP_WINDOW_DAYS` | int | `90` | window for `reg_gap_mean` |
| `WB_DEREG_REG_GAP_FALLBACK_DAYS` | float | `8.0` | when < 2 registrations in the window |
| `WB_CHAIN_PROBE_URL` | str | `wss://entrypoint-finney.opentensor.ai:443` | keyless probe (called over HTTPS JSON-RPC) |
| `WB_CHAIN_PROBE_TIMEOUT_SEC` | int | `20` | per RPC step |
| `WB_GITHUB_PROBE_ENABLED` | bool | `true` | runs only while `GITHUB_TOKEN` is empty |
| `WB_GITHUB_PROBE_PER_RUN` | int | `45` | repos per hourly run (no-token bucket 50/h) |
| `WB_VISION_ENABLED` | bool | `false` | chart-upload reads; also needs `ANTHROPIC_API_KEY` and the `anthropic` package |
| `WB_VISION_MODEL` | str | `claude-opus-5-5` | vision model id |
| `WB_VISION_EST_USD` | float | `0.10` | reservation per read (actual ≈ $0.01–0.05) |
| `WB_VISION_MAX_IMAGE_MB` | int | `5` | upload cap |
| `WB_UPLOAD_DIR` | path | `data/wb/uploads` | stored images |
| `WB_EVENTS_ENABLED` | bool | `false` | emit `ema72_band_exit` |
| `WB_STUDY_IMMUNITY_BLOCKS` | int | `864000` | immunity rule used for the historical dereg factors in §2.8 |

Keys the operator provides, in order: none for W2–W6; `ANTHROPIC_API_KEY` + `WB_VISION_ENABLED=true` for the vision read; `GITHUB_TOKEN` for Phase 4 (after which `wb_github_probe` turns itself off).

## 5. Jobs, cadence, budgets

| job | trigger | provider | budget |
|---|---|---|---|
| `wb_technicals` | cron hourly at :12 (the 00:12 run also writes the 1 d keys) | DB only | ~2–4 s (128 occupants × 2 series of ≤ 432 bars) |
| `wb_dereg` | cron hourly at :09 | DB only | ~1–2 s (one snapshot + 30 d of 1 h bars for `hours_in_bottom_k_30d`) |
| `chain_constants_probe` | cron daily 01:20Z | `subtensor_rpc` (keyless finney) | ~12 requests/day |
| `wb_github_probe` | cron hourly at :25, timeout 3,300 s | `github` (no-token bucket: burst 10, then one per 72 s) | ≤ 45 req/run ≈ 42 min; ~116 repos → done by ~03:10Z each day; rows written per repo so a cut-off run keeps its reads; off when `GITHUB_TOKEN` is set |
| chart read | on demand (API) | `anthropic` (group `llm`) | ≈ $0.01–0.05/image under `LLM_MONTHLY_CAP_USD` |

All four jobs are `JOB_TABLE` rows (owner 19, phase 3), none `ops_exempt`, none `startup_catchup` (the daily keys catch up by themselves). 16's `PAUSE_GROUPS` gains `wb` = the four jobs. `JOB_TABLE` = 57 rows (54 jobs + 3 LongTasks). Monthly cost: $0 without vision.

## 6. Failure modes and fail-safe behaviour
- Service down → the UI shows the last `as_of` with the stale banner; nothing is interpolated.
- Probe hangs/fails → per-step timeouts, job `failed`, `wb_dereg` keeps using the newest stored constants (or config, flagged); `/health` reason `chain_probe_stale` when `WB_ENABLED` and no successful probe in 48 h.
- No snapshot for the hour (SSE down and no fallback) → `wb_dereg` fails that run (`collector_failing` after the usual threshold); technicals still run from bars.
- A partial last bar never feeds an EMA (§2.1). A stale series is `failed stale_bars`, never silently old.
- Vision refusal/malformed output → `unreadable` note, no retry loop; the governor's cap stops reads at the monthly limit.
- Jobs never raise into the scheduler (01 wrapper) and never write norm rows.

---

## 7. Roadmap (status 2026-10-16)

| Step | Work | Needs | Status |
|---|---|---|---|
| W0 | restart the data (gap backfill, service install/start) | Blockmachine key | **done** 2026-10-16 |
| W1 | seed the old bot (ledger, fundamentals, regime) + legacy results copy | nothing | **done** (ledger etc. 2026-10-12/13; legacy copy via `scripts/import_legacy_results.py`) |
| W2 | technicals layer | W0 | **built** 2026-10-16 |
| W3 | dereg distance + constants probe | W0 | **built** 2026-10-16 |
| W4a | `wb_github_probe`, TMC commits in the screener | nothing | **built** 2026-10-16 |
| W4b | 06 (Phase 4), 08 keyless half (`discord_6h`, `telegram_daily`), 09 `x_ctf_crosscheck` | `GITHUB_TOKEN` for 06 | open — built with those specs |
| W5 | routes, `/wb` page, presets, upload + vision | `ANTHROPIC_API_KEY` for vision only | **built** 2026-10-16 |
| W6 | study + results page | W2/W3 | **built** 2026-10-16 (exploration); 13's gated version after Phase 10 |
| then | Phase 8 scores → tier column; `ts-1.1` only if 13 passes a workbench factor | per 18 | |

## 8. Tests (`tests/wb/`)
- Technicals: EMA against a direct recursive reference; seed/window determinism; gap counting and the 10 % rule; running-bar exclusion; stale bars; band-position clipping; `σ = 0`; occupant isolation (predecessor bars ignored); percentile rank with ties; cross-days with and without a cross; pre-launch → `not_applicable`.
- Dereg: pool includes not-started (`mp` 0) and emission-disabled; tie order by registration; immunity boundary block; graduation-adjusted ranks; `k_H` from registration gaps and the fallback; `dereg_safety_H` monotone in rank; cull-line-zero; `hours_in_bottom_k_30d` on a synthetic panel; transition events only on flips; `immunity_ends_7d` once.
- Probe: storage decoding, runtime-API cost, `I_eff`; drift event once per `(name, live)`; a hanging endpoint → job failure, not a hang.
- GitHub probe: URL parsing, 200/404/no-link rows, one probe per repo per day, off with a token.
- API: screener shape and coverage, chart series equals stored technicals, preset round-trip and auth, chart-read size/type caps, `vision_disabled`, stubbed vision success and refusal → note rows.
- Study: a synthetic panel with a planted factor gives positive IC; JSON written; trials appended.
- Conventions: job table 57, catalog 109 keys, new events catalogued with routes, `PAUSE_GROUPS['wb']`, migration 0003 applies on a fresh DB.

## 9. Verify checklist
```
python -m app check                      → 57 declared, 21 available (phase 3); wb_* + chain_constants_probe available
python -m app run-job chain_constants_probe → chain_constants rows (rpc_storage); chain_constant_drift for NetworkImmunityPeriod
python -m app run-job wb_technicals      → 1 h ema72_* ok for ≥ 120 active occupants; 1 d ok for every occupant with
                                           ≥ 96 daily bars (115 on 2026-10-16); none from a running bar
python -m app run-job wb_dereg           → dereg_rank for every non-immune occupant; lowest moving price = rank 1;
                                           immune rows not_applicable with graduates_at; one wb_dereg_network_hourly row
curl :8090/api/v1/wb/screener | jq '.rows|length'  → ≥ 120; every row has coverage + as_of
browser :8090/wb                         → chart with EMA72 + band + cull line; sliders re-rank; preset saved
python -m app.wb.study                   → data/quant/results/wb_study_<date>.json; wb_trials.jsonl appended
pytest                                   → green
```

## 10. Sources
Local: `research/03-*.md` (2026-10-09), `research/01-factor-validation-tooling.md`, `research/01-web-social-apis.md`, specs 02/05/06/08/10/12/13/14/16/17. Chain: public finney entrypoint and archive JSON-RPC reads on 2026-10-16 (storage `SubtensorModule::{NetworkImmunityPeriod, SubnetLimit, NetworkRateLimit, NetworkMinLockCost, NetworkLastLockCost, NetworkLockReductionInterval, SubnetMovingAlpha, LastRateLimitedBlock}`, runtime API `SubnetRegistrationRuntimeApi_get_network_registration_cost`). Web (2026-10-09): RaoFoundation/subtensor `subnets/subnet.rs`, `coinbase/root.rs`, `stake_utils.rs`, PR #2066, TaoStats dereg docs, TMC API docs.

## 11. Open questions / operator decisions
1. **72 EMA definition:** 1 h bars (the old bot's) with a 1 d twin; band = ±2σ of log returns. Confirm, or name the timeframe/band you watch.
2. **Immunity config:** the chain now says 432,000 blocks. Set `NETWORK_IMMUNITY_PERIOD_BLOCKS=432000` in `.env` (and `reload-config`) so 05's `immune_local`, `dereg_safety` and `dereg_risk_flag` match the chain? The workbench already uses the live value.
3. **05/12 change requests:** pool definition (include not-started/emission-disabled), `k_H` instead of bottom-3, drop the TMC badge from `dereg_risk_flag`, `dereg_imminent` reading `dereg_rank_30d ≤ k_30` — accept into 05/12 (BUILD-LOG entry) or keep as workbench-only keys?
4. **Vision:** add `ANTHROPIC_API_KEY` and set `WB_VISION_ENABLED=true` when you want screenshot reads (≈ $0.01–0.05 each).
5. **Weights:** exploration-only sliders (built) vs starting a `ts-1.1` draft.
6. **GitHub token** timing (unlocks Phase 4 and the honest dev-activity column).
7. **`dereg_recovery_ratio`:** worth calibrating against a real dissolve (TaoStats liquidation price) before adding?

## 12. Changelog
- 2026-10-16 (rebuild + implementation): status → built W2–W6. Folded 02 §K into the text. Chain facts re-read: immunity 432,000 blocks (changed ~2026-10-13), `LastRateLimitedBlock[0x02]`, runtime-API lock cost and `I_eff`; `wb_dereg` reads the probed immunity. Changes vs §K (02 amended the same day): the network weather lives in a new `wb_dereg_network_hourly` table instead of new `chain_network_hourly` columns (no shared row with the SSE recorder); `mp_vs_spot` → `spot_vs_mp`; `dereg_recovery_ratio` deferred; new keys `repo_pushed_days` (source `github`), `spot_rank`; jobs declared at phase 3, `wb_technicals` a single hourly :12 trigger, `wb_github_probe` hourly :25; upload is a raw-body POST (no multipart dependency); vision through the `anthropic` SDK with server-side fallbacks; routes `/presets`, `/notes` replace `POST /wb/weights`; W6 is a workbench study until 13 exists; W4b moved to the owner specs. Exact window/seed, staleness, rank and cull-line-zero rules added.
- 2026-10-16: reconciled by 02 §K (names, owners, conventions).
- 2026-10-09: first draft from the research run.
