# TaoSentinel — Master Specification

**Status:** `design-of-record` · **Version:** 0.2 (reconciled 2026-09-23) · **Date:** 2026-09-22 · **Author:** Claude Fable 5.1 (Ultracode research + synthesis) for Rudolf
**Implements on:** Ubuntu laptop (24/7, occasional power-offs), Python 3.12, SQLite. Windows PC is design/ops only.
**Reads first:** [`../research/INDEX.md`](../research/INDEX.md) (15 research reports the design rests on) · [`INDEX.md`](INDEX.md) (component specs)
**Tie-breaker:** [`02-cross-cutting-conventions.md`](02-cross-cutting-conventions.md) records one binding decision per cross-spec conflict (storage layout, names, events, jobs, config, budgets). Precedence: **02 › this master › the owner spec for a topic › any other spec.**

---

## 0. One-paragraph summary

TaoSentinel is a 24/7 collector-and-scoring service that assigns every Bittensor subnet a **0–100 Health Score** (slow, fundamental) and a **Velocity Score** (fast, sentiment/flow), built from six evidence pillars — on-chain vitality, builder activity (GitHub), product/web presence, team & governance integrity, community & communication, and narrative/sentiment — plus a set of hard **red-flag gates**. Every raw observation is stored **point-in-time** (`event_at` + `observed_at`) so scores can be honestly backtested against forward alpha-token returns, emission-share growth and survival using the quant harness ported from the dead SubnetTrader bot. Nothing touches capital: the only output to a future trading bot is a versioned, fail-safe-neutral score feed that the bot may use as a de-risk multiplier **only after** a pre-registered evidence gate passes. Cost target: **$0–$25/month** for the keyless/free-key build, ≤ **$150/month** with the optional X, TaoStats and LLM tiers turned on.

---

## 1. Goals, non-goals, principles

### 1.1 Goals
1. Continuous, auditable, per-subnet evidence: *who is building, is the product real, is the team accountable, is the chain paying them, is the community alive, what is the narrative.*
2. A single ranked matrix (healthy → unhealthy) the operator can read in 30 seconds, with drill-down to the evidence behind every number.
3. Point-in-time history from day one so the scoring model can be validated (IC / deciles / survival / walk-forward) and only then wired into allocation.
4. Alerts on the events that matter (tier changes, red flags, silence, owner swaps, dereg proximity) via Telegram.
5. A clean, versioned contract for a revived trading bot (`quality` + `velocity` + `red_flags` per subnet), fail-safe-neutral by construction.

### 1.2 Non-goals (v1)
- No trading, staking, wallet or key handling of any kind. No `bittensor-wallet` dependency.
- No scraping that violates a platform's terms: no Discord self-bots, no LinkedIn automation, no use of the operator's own X account for scraping, no backprop.finance automation (ToS forbids it).
- No polished UI in v1. A minimal read-only web matrix + Telegram commands + CSV/JSON export is the v1 surface.
- No "AI predicts price". Sentiment is a small, decaying overlay whose weight is earned by evidence, not assumed.

### 1.3 Design principles (inherited from SubnetTrader, kept deliberately)
- **Evidence before capital.** Every signal ships in `observe` mode; the trading contract multiplies by 1.0 until the Phase-C gate (§10) passes.
- **Point-in-time or it didn't happen.** Every stored observation carries `observed_at`; backtests may only read rows with `observed_at ≤ decision_time`; never forward-fill; never recompute history with a newer classifier without a new `spec_version`.
- **Absence is a signal; failure is not.** *Verifiable absence* (no repo, no website, anonymous team) scores 0. *Collection failure* (API down, rate-limited) scores `None`, is dropped from the weighted mean, and lowers `confidence`. The old bot's bug — an unresolved GitHub repo scoring as *perfect* — must be impossible by construction (see 06-github-collector).
- **Identity = occupant, not netuid.** Netuid slots are recycled on deregistration; 42 of 128 names changed between June and September 2026. The primary key everywhere is `occupant_key = f"{netuid}:{network_registered_at}"`.
- **Fail-safe operations.** Every collector is flag-gated, never raises into the scheduler, exposes `last_run/last_error/consecutive_failures` on `/health`, alerts once on the Nth consecutive failure, and has a file-sentinel kill switch. Paid providers sit behind a **cost governor** with hard monthly caps.
- **Simplicity first.** One Python process, one SQLite file, APScheduler, FastAPI. No queues, no Docker required, no Kubernetes. The old bot proved this shape runs for months on a Raspberry Pi.

---

## 2. What we reuse from `E:\SN_Trader\SubnetTrader` (and what we do not)

| Old asset | Verdict | Notes |
|---|---|---|
| `app/main.py` lifecycle (signal handlers, sd_notify READY/WATCHDOG/STOPPING, uvicorn `capture_signals` override, bounded shutdown, `_hard_exit`) | **Port verbatim** into `app/runtime.py` | Lines 359-447, 3232-3296, 3308-3382, 3385-3486. See research `02-gapfill-old-bot-runtime-skeleton.md`. |
| APScheduler job table pattern (`interval`, `max_instances=1`, `misfire_grace_time`, flag-gated, job wrapper that swallows+logs+alerts) | **Port pattern** | New job ids; keep shape. |
| `app/api_auth.py` (`X-Auth-Token`, `hmac.compare_digest`, empty token = auth off) | **Copy verbatim** | Mount mutating routers with `dependencies=[Depends(require_auth)]`. |
| `app/storage/db.py` (aiosqlite, WAL, `executescript` + idempotent ALTER tuple, `VACUUM INTO` backup with `quick_check`) + `storage/backup.py` | **Port**, add `schema_version` table | Never run VACUUM in the systemd start path. |
| `app/logging/logger.py` (StructuredLogger, daily JSONL rotation, `prune_old_logs`) | **Copy verbatim** | `logger.info(msg, data={...})` convention. |
| `app/notifications/telegram.py` (`send_alert`, long-poll bot, inline keyboards, chat allowlist) | **Copy verbatim**, new command handlers | Transport is decoupled via `TelegramCommandHandlers`. |
| `app/config.py` `Settings(BaseSettings)` conventions (`UPPER_SNAKE`, `<X>_ENABLED`, `<X>_MODE` validators, sentinel kill-switch paths, `extra="ignore"`) | **Port conventions**, new field set | Every consumer must read `app.config.settings` dynamically (old hot-reload bug). |
| `app/strategy/quality.py` (pure scorer: coverage-renormalised weighted mean, `None ≠ 0`) | **Port as the pillar aggregator kernel** | Generalise from 7 fixed components to metric registries. |
| `app/data/fundamentals_collect.py` (`fetch_subnetalpha` WP-REST + ACF field map, `_pick` merge, `write_atomic`) | **Port**, replace `fetch_github`/`parse_repo` entirely | The GitHub path is defective (no `follow_redirects`, 301→dropped, org URLs ignored). |
| `app/backtest/{factor_panel,factor_backtest,validation,walk_forward(factor track),quality_band,economics,slippage,dereg_analysis}.py`, `app/utils/math.py`, `app/strategy/dereg_risk.py` | **Port (pure stdlib)** into `app/quant/` | Strip `app.config.settings` / `FundamentalsStore` imports; inject `db_path` and `quality_by_netuid`. |
| `app/whale/events.py` (Taostats delegation parser, move-stake phantom-pair rule) + `whale_impact.py` `move_groups` SQL | **Port** (dormant until a TaoStats key exists) | |
| `app/chain/endpoints.py` (`EndpointSelector`, `is_quota_error`) | **Copy** | Used by the optional RPC collector. |
| `data/ledger.db` `pool_history` (7.36M rows, 129 netuids, 2025-09-22 → 2026-06-19) + `data/backups/ledger_backfill.db` (hourly `moving_price` back to 2025-09-22) + `data/fundamentals.json` (2026-06-02 snapshot) + `data/whale_cohort.json` | **Import** as seed history (17-data-migration-and-backfill) | Read-only attach; `INSERT OR IGNORE` by `(netuid, block_number)`. |
| `specs/feature-sentiment-ingestion*.md` (payload schema, three-layer fail-safe, clamp rationale, phased A→D, safety-properties table) | **Adopt** as the trading contract (15-trading-bot-integration) | Replace the "PC off at night" premise; generalise `funnel/velocity` to `quality/velocity`. |
| `ops/systemd/*.service`, `ops/install-systemd.sh`, `deploy/auto-update.sh` (+timer), `install.sh` | **Port**, parameterise user/paths | Keep the compileall + `/health` gate + rollback. |
| `frontend/` Next.js shell (`api.ts`, layout, NavBar, SWR tables, lightweight-charts) | **Fork later (Phase 11)** | Dark-only, 3 pages. |
| `research/subnet_edge/*.md` | **Seed** manual notes for ~25 subnets | Verdicts are trading-edge, not health — re-derive. |
| Wallet / executor / EmaManager / strategy code | **Do not port** | |

---

## 3. System architecture

```
                         ┌────────────────────────────────────────────────────────────┐
                         │  taosentinel (one Python 3.12 process, systemd, ~300 MB)    │
                         │                                                            │
  keyless / free-key     │  ┌──────────── collectors (APScheduler jobs) ───────────┐  │
  sources                │  │ registry   chain(TMC SSE+REST, RPC opt)  github       │  │
  ───────────────────►   │  │ web        community(discord/tg)         x(provider)  │  │
  TMC, chain RPC,        │  │ news/rss   youtube                       llm-classify │  │
  subnetalpha, GitHub,   │  └──────────────────────┬──────────────────────────────┘  │
  Discord invites,       │                         ▼                                  │
  t.me, Wayback, RSS,    │   raw tables (append-only, point-in-time: event_at,        │
  twitterapi.io, Claude  │   observed_at, run_id, source)                             │
                         │                         ▼                                  │
                         │   feature engine ──► metric_values (occupant_key, metric,  │
                         │                         as_of, value, observed_at)         │
                         │                         ▼                                  │
                         │   scoring engine (spec_version) ──► scores, tiers, events, │
                         │                         red_flags, confidence              │
                         │                         ▼                                  │
                         │   FastAPI :8090 ── /health, /api/v1/*, SSE, exports        │
                         │   Telegram bot ── /top /sn N /flags /health ; alerts       │
                         │   quant CLI ── python -m app.quant factor|survival|wf      │
                         └────────────────────────────────────────────────────────────┘
                                  │                                  │
                                  ▼                                  ▼
                        minimal Next.js matrix UI          revived SubnetTrader (later)
                        (:3001, read-only)                 GET /api/v1/scores/latest
                                                           factor = 1.0 until Phase-C gate
```

**Process model.** One asyncio process. Collectors are APScheduler interval jobs (`max_instances=1`). The TMC SSE recorder is a long-lived task with reconnect/backoff, not a job. LLM classification runs as a batch job (Anthropic Batch API) every 6 h. Scoring runs at 00:15Z (official daily) and 06/12/18:15Z (provisional).

**Repository layout** (`~/taosentinel`, own git repo, private):
```
app/
  __main__.py  runtime.py  config.py  api_auth.py  logging/  storage/{db.py,models.sql,backup.py}
  registry/    (subnet + identity + occupant keying)
  collectors/  {chain_tmc.py, chain_rpc.py, github.py, web.py, discord.py, telegram.py, x.py, rss.py, youtube.py, news.py}
  nlp/         (llm_classify.py, rules.py, kol.py)
  features/    (metric registry, normalisers, feature engine)
  scoring/     (spec.py = weights & versions, engine.py, tiers.py, red_flags.py)
  quant/       (ported factor_panel, factor_backtest, validation, walk_forward, survival, panel_builder.py)
  api/         (routers: scores, subnets, events, health, control, export)
  notifications/telegram.py
  costs.py     (cost governor)
scripts/       (import_old_ledger.py, backfill_*.py, build_kol_list.py, seed_registry.py, manual_entry.py)
ops/systemd/   deploy/   frontend/ (Phase 11)   tests/   data/ (gitignored)   specs/ (this folder, copied)
```

---

## 4. Data sources and tiers

Full endpoint facts, limits and gotchas are in the research corpus; this table is the binding decision.

| Tier | Source | Used for | Auth / cost | Cadence |
|---|---|---|---|---|
| **0 keyless** | `api.taomarketcap.com/public/v1` **SSE `/sse/subnets/table/`** (129 rows × 44 fields per block) | Primary on-chain recorder: `emission` (post-gate), `subnet_moving_price`, `immune`, `deregistration_risk`, `excess_tao`, `subnet_ema_tao_flow`, `owner_personal_*_alpha`, `total_locked_alpha`, `price`, liquidity, `commits_*` | none; 2 SSE connections, not metered by REST bucket | continuous |
| 0 | TMC REST: `/subnets/`, `/subnets/{id}/`, `/analytics/subnet/{n}/?span=1M\|1Y`, `/subnets/github-stats/{n}/`, `/accounts/subnet-holders/{n}/`, `/accounts/coldkey-identities/`, `/analytics/trending/subnets/`, `/extrinsics/staking-activity/pressure/`, `/subnets/{id}/candle-chart-compact/` | identities v3, daily/hourly history (365 d), holders concentration, owner identities, page-view attention, buy/sell pressure, price OHLCV | none; **10 req/min, 600/h, 100k/month per IP** — one process-wide token bucket | daily / weekly tiers (≈ 600-700 req/day) |
| 0 | Public subtensor RPC (`entrypoint-finney`, JSON-RPC over HTTPS) | `SubnetIdentitiesV3` map (3 calls), live constants | none | daily |
| 0 | `subnetalpha.ai/wp-json/wp/v2/subnet` (ACF) | editorial: team text, roadmap, X handle, Telegram, `modified_gmt` | none; polite 1 req/s | daily |
| 0 | Discord `GET /api/v10/invites/{code}?with_counts=true`, `/guilds/{id}/widget.json` | member/presence counts, invite validity/expiry, guild age | none (attribute with a Bot token once available); 404s free, 403s count | 6 h / weekly |
| 0 | GitHub HTML (`github.com/{o}/{r}`, redirects disabled) + `.atom` commit feeds | zero-quota rename detection, repo id | none | daily |
| 0 | Wayback Availability/CDX, YouTube channel RSS, Medium/Substack RSS, Google News RSS, `t.me/{handle}` | site age, video/blog cadence, news mentions, TG member counts | none | daily |
| **1 free key** | GitHub REST (+GraphQL) with **PAT** | the Builder pillar: repos by numeric id, org enumeration, commits, contributors, releases, PR reviews, community profile | free, 5,000/h | 6 h (repos), daily (org, stats) |
| 1 | **OnFinality** archive RPC (free API key, 40 rps) | backfill of the 2026-06-19 → today gap and pre-ledger dTAO era; block-pinned reads | free key | one-off + catch-up |
| 1 | **Discord Bot** in the operator's own aggregator server (MESSAGE_CONTENT intent) | reads announcement-channel **Follows** (main-server `📣・announce` + own-server type-5 channels) | free; manual Follow setup | gateway |
| 1 | **Telegram Bot** (BotFather) | alerts + `/top /sn /flags /health` commands | free | long-poll |
| 1 | **Anthropic API** (`claude-haiku-4-5` bulk via Batch API; `claude-sonnet-5` escalation; `claude-opus-5` weekly narrative) | event/sentiment classification | ~$20-80/month at 2,000 posts/day (verify prices at build time) | 6 h batch |
| **2 paid, optional** | **twitterapi.io** (primary X provider) | team-account timelines, KOL timelines, mentions search | ~$5-20/month; behind `XProvider` interface with a hard cap | daily (teams), 4×/day (KOLs) |
| 2 | Official X API v2 pay-per-use | ToS-clean fallback | $0.005/post read; hard cap `$75/month` | fallback only |
| 2 | **TaoStats API** Standard ($49/mo, 60/min, 50k/mo) | `frequency=by_hour` pool history (`/v1/subnets/pools/history`), `/v1/subnets/stake-events` whale/owner stake events, `/v1/subnets/owners` swap history (paths per 02 §G7, new API since 2026-10; old `/api/…` routes off 2026-11-09; `dev_activity` was retired with no replacement) | key | only if the user re-subscribes |
| 2 | TMC Pro ($49/mo) / tao.app key (social analytics, identity-changes) | more SSE slots; ready-made Discord/X analytics | optional | — |
| **3 local node** | Self-hosted **lite** subtensor node (`docker compose up -d mainnet-lite`, ~128 GB) | per-block storage reads TMC doesn't expose: `MinerBurned`, `SubnetOwnerCut`, `NetworkImmunityPeriod`, `EmissionBarQuantile`, live metagraph | free; hardware | when the laptop has the disk; **not required for v1** |

**Not used:** Nitter/RSS bridges (dead since 2026-09), backprop.finance (ToS), LinkedIn automation, Discord self-bots, Santiment GitHub metrics (flagged broken Aug 2026), SocialData monitors (too expensive).

---

## 5. Identity and the subnet registry

- **`occupant_key = "{netuid}:{network_registered_at}"`** is the primary key for every long-lived record. `network_registered_at` comes from TMC `/subnets/` (`network_registered_at`) and the old `pool_history` column; it changes only when a slot is re-registered.
- `subnets` table stores the current occupant per netuid; `occupants` stores every occupant ever seen with `first_seen_block/last_seen_block/status ∈ {active, pre_launch, deregistered}`. A change of `network_registered_at` for a netuid emits `netuid_reoccupied` and starts fresh history for the new occupant (GitHub, community, sentiment). Old-occupant rows are kept for survival analysis.
- **Identity merge precedence** (per field): on-chain `SubnetIdentitiesV3` (owner-signed) > subnetalpha ACF > TMC coldkey identity > manual override. Store every source's raw value with `observed_at`; emit `identity_changed` when any source's value changes; emit `source_disagreement` when resolved targets differ (e.g. GitHub repo ids).
- On-chain identity strings must be **SCALE-prefix-stripped** when read through subnetalpha's `subnet-stats` endpoint (leaked compact-length bytes); read the chain or TMC instead for identity strings.
- Immunity is computed locally: `immune = (head_block − network_registered_at) < NetworkImmunityPeriod` with `NetworkImmunityPeriod = 864_000` blocks (120 d, verified on-chain 2026-09-22; docs still say 1,296,000 — stale). Store the constant in config, re-verify quarterly.

---

## 6. Storage model (SQLite, WAL)

Full DDL lives in `03-storage-schema.md`. Binding shape:

**Reference:** `subnets`, `occupants`, `identity_observations` (per source, per field, `observed_at`), `team_members` (manual, evidence-linked), `kol_accounts`, `watch_channels` (discord/tg/x/youtube/rss handles per occupant with `source`, `verified_at`), `manual_notes`.

**Raw (append-only, PIT):** `chain_ticks` (from the SSE stream: 12 hot fields every `CHAIN_TICKS_BLOCK_STRIDE` = 5 blocks ≈ 1 min, 30-day retention; all 44 fields hourly in `chain_hourly`), `chain_daily` (TMC analytics daily rows, 365 d), `chain_hourly`, `price_bars` (5 m/1 h OHLCV from TMC candles; imported `pool_history` downsampled), `holders_snapshots`, `github_repos` (keyed by numeric `repo_id`), `github_activity_daily` (repo_id, day, commits, authors, bot flags), `github_events`, `web_checks`, `discord_snapshots`, `telegram_snapshots`, `social_posts` (X posts: post_id, author, text_hash, text kept 90 d then hash only, engagement, `event_at`, `observed_at`), `post_labels` (post_id, classifier_version, polarity, event_type, materiality, confidence), `news_items`, `video_items`, `announcements` (Discord crossposts received in the operator's own aggregator server, and Telegram channel posts; text rules in 02 §B10).

**Derived:** `metric_values` (occupant_key, metric_key, as_of, spec_version, **stage** `raw|norm`, value REAL NULL, value_json incl. the `status` enum, observed_at, source, run_id, backfilled — one table for producer values and the normalised values scoring used; write/read rules in 02 §B3–B5), `scores` (occupant_key, as_of = **slot instant**, spec_version, computed_at, input_cutoff, composite_raw, composite_smoothed, quality, velocity, pillar_scores JSON, confidence, tier, red_flags = sorted JSON array of flag names, explain JSON — 02 §B6), `red_flag_state`, `regime_daily`, `score_runs`, `control_state` (operator pauses — 02 §B12), `events` (occupant_key, event_type, severity, event_at, observed_at, payload, alerted_at), `collector_runs` (job, started, finished, ok, rows, error, cost_usd), `provider_spend` (provider, month, usd, requests, units_json). Every timestamp column is INTEGER unix seconds (02 §B1).

**Rules.** Every raw row has `observed_at` (when we fetched) and `event_at` (when it happened, if known). `metric_values` is the *only* input to scoring. `scores` rows are never updated; a new `spec_version` writes a parallel series. TaoSentinel never reads third-party Discord servers. Crossposts of Followed announcement channels that arrive in the operator's **own** server may keep text for 90 d (`DISCORD_STORE_TEXT`, default true, user ids redacted), then hash only; Telegram posts are hash-only by default and classified in flight (02 §B10). Store ids, timestamps, author-role flag and the classification in all cases.

---

## 7. The scoring system (spec_version `ts-1.0`)

### 7.1 Overview
```
metric (raw) ─normalise→ m ∈ [0,1] | None
pillar P_k   = Σ w_i·m_i / Σ w_i over non-None metrics      (coverage-renormalised; if coverage_k < 0.3 → P_k = None)
Health (slow) = 100 · Σ W_k·P_k / Σ W_k over non-None pillars (k = 1..5)
Velocity (fast) = 100 · [0.6·P_6 + 0.4·flow_attention_blend]
Composite_raw = Health · (1 − v_w) + Velocity · v_w,  v_w = 0.15 (regime-scalable, §7.6)
Composite = Composite_raw · Π red_flag_multipliers (floor 0.25)
Composite_smoothed = EMA(Composite, span = 7 daily observations)
Tier from Composite_smoothed with ±3 hysteresis; Confidence = weighted metric coverage within freshness windows
```
Pre-launch occupants (`is_active = False`, no emission yet) get `status = pre_launch`, no composite, but pillars 2-5 are computed and shown.

### 7.2 Normalisers (in `app/features/normalise.py`)
- `recency(days, full_at, zero_at)`: 1.0 if `days ≤ full_at`, linear to 0.0 at `zero_at`.
- `logcap(x, cap)`: `log1p(x) / log1p(cap)` clipped to [0,1] (OpenSSF Criticality style).
- `rankg(x)`: cross-sectional rank → percentile → inverse-normal → clip ±2.5σ → map to [0,1]. Used for continuous chain metrics; computed over **active occupants only** on the same `as_of`.
- `zclip(x, lo, hi)`: linear map with clipping (for ratios with natural bounds).
- `checklist(items)`: mean of binary items.
- Any normaliser returns `None` when the input is `None` (collection failure) and **0.0** when the input is a *verifiable absence* — the collector, not the normaliser, decides which (see each collector spec's "absence vs failure" table).

### 7.3 Pillars, metrics, weights

Weights are budget-allocated priors (OECD/JRC guidance: near-equal within pillar, expert priors across). They are **frozen per spec_version**; changing any weight = new version. IC-weighted re-weighting is a Phase-C decision (§10), not a v1 feature.

**P1 · Chain Vitality — pillar weight 30**
| metric_key | definition (source) | normaliser | w |
|---|---|---|---|
| `emission_share` | realised post-gate TAO emission share % (TMC `emission`) | `rankg` | .20 |
| `gate_margin` | burn-adjusted demand share ÷ gate bar θ (θ = 32nd-highest share; reproduce the v440 Hill gate `1/(1+(θ/s)^3)`; TMC `subnet_moving_price` + fitted burn) | `zclip(log2(ratio), −2, +1)` → ≤¼·θ→0, θ→0.67, ≥2θ→1 | .15 |
| `dereg_safety` | immune → 1.0; else non-immune moving-price rank percentile from the bottom; bottom-3 → 0 | custom | .15 |
| `liquidity_depth` | `tao_in_pool` (TAO) | `rankg(log)` | .10 |
| `exit_capacity` | TMC `price_after / price` (price after all unlocked stake sells) | `zclip(x, 0.05, 0.6)` | .05 |
| `net_flow_30d` | (Σ tao_buy − Σ tao_sell over 30 d) ÷ pool depth (TMC analytics) | `zclip(x, −0.5, +0.5)` | .10 |
| `chain_buys` | `excess_tao × 7200` (TAO/day of protocol alpha buys) ÷ market cap | `rankg` | .05 |
| `holder_dispersion` | 1 − top-10 holder share (TMC subnet-holders) | `zclip(x, 0.3, 0.9)` | .05 |
| `participation` | (active validators + active miners) ÷ max_uids; validator vtrust dispersion is stored as evidence only (`vtrust_dispersion`, w 0) in ts-1.0 | `zclip(x, 0.2, 0.9)` | .05 |
| `miner_burn_ok` | 1 − implied_miner_burn, where implied = 1 − emission ÷ predicted_gated_share (fit per tempo; TaoStats/RPC `MinerBurned` when available) | `zclip(x, 0, 0.8)` | .10 |

**P2 · Builder Activity (GitHub, org-level) — pillar weight 20**
| metric_key | definition | normaliser | w |
|---|---|---|---|
| `active_authors_28d` | unique non-bot commit authors (org-wide, non-fork repos) with ≥1 commit-day in 28 d | `logcap(x, 10)` | .20 |
| `fte_devs_84d` | authors with ≥10 commit-days in a rolling 28-d window, sustained over 84 d (Electric Capital) | `logcap(x, 8)` | .15 |
| `commit_days_90d` | org-wide non-bot commit-days in 90 d | `logcap(x, 90)` | .15 |
| `declared_repo_freshness` | days since `pushed_at` of the declared (identity) repo, by numeric id | `recency(d, 14, 180)` | .10 |
| `org_freshness` | days since max `pushed_at` over non-fork, non-archived org repos | `recency(d, 7, 90)` | .10 |
| `release_cadence_90d` | releases + tags in 90 d | `logcap(x, 6)` | .05 |
| `review_practice_90d` | share of merged PRs with ≥1 review (0 PRs → None) | identity | .05 |
| `bus_factor` | contributors covering 50 % of 90-d commits: ≥3→1, 2→0.6, 1→0.3 | custom | .10 |
| `repo_hygiene` | checklist: LICENSE, README ≥ 500 chars, CI workflow, SECURITY/CONTRIBUTING, dependabot/renovate | `checklist` | .05 |
| `originality` | 1 − template share: compare against `opentensor/bittensor-subnet-template` (`ahead_by`, vendored-initial-commit heuristic) | custom | .05 |
Absence rules: no GitHub URL anywhere → all metrics 0 (not None). Declared repo 404/private for > 14 d with no org fallback → `declared_repo_freshness = 0` and `repo_gone` red flag. Commit-spam detection (bursts, `[bot]` authors, whitespace churn) filters raw counts before normalisation.

**P3 · Product & Web Presence — pillar weight 10**
| metric_key | definition | normaliser | w |
|---|---|---|---|
| `site_uptime_30d` | share of daily checks returning 2xx/3xx to a non-parked page | identity | .30 |
| `tls_ok` | valid cert, > 14 d to expiry, hostname matches | binary | .10 |
| `site_age` | days since first Wayback capture | `recency` inverted: ≥365→1, ≤30→0.2 | .10 |
| `site_freshness` | days since last content-hash change (main text, dates stripped) | `recency(d, 90, 365)` | .15 |
| `docs_present` | docs/API-reference URL resolves (from identity/manual) | binary | .15 |
| `product_status` | manual enum: `none 0 / waitlist 0.3 / beta 0.6 / live 1.0` with evidence URL | map | .20 |
Absence: no website URL in any source → `site_uptime_30d = 0`, others None → pillar ≈ 0 with low confidence.

**P4 · Team & Governance Integrity — pillar weight 18**
Mechanics (verified in subtensor code, see `research/02-gapfill-owner-extraction-mechanics.md`): the 18 % owner cut accrues per tempo as **alpha stake on (`SubnetOwnerHotkey`, `SubnetOwner`)**, never as free TAO; owners sell via `remove_stake*`/`unstake_all_alpha`/`swap_stake`/`transfer_stake` from that position. Locks are perpetual or decaying; **auto-lock of the owner cut is opt-in** (`OwnerCutAutoLockEnabled`, only 12/121 subnets on 2026-09-22); the only formal unlock path is `set_perpetual_lock(netuid, false)` → `PerpetualLockUpdated{enabled:false}` — the public exit signal. Keyless data: TMC `/accounts/coldkey-stakes/{owner_ck}/` (`balance_alpha, locked_alpha, is_subnet_owner`), `/analytics/neuron/{netuid}/{owner_hk}/?span=` (hourly `alpha_stake`, `conviction_*`), `/extrinsics/staking-activity/?coldkey=…`, `/transactions/stakes/stake-removed/`, TMC table `owner_personal_{perpetual,decaying}_alpha`, `owner_cut_per_block`, and the Rufus conviction table JSON (`s3.hippius.com/rufus/public/conviction-table/data/table.json`: `auto_lock_enabled`, owner/to_owner/challenger buckets, `days_to_king`). TaoStats (`/v1/subnets/stake-events?coldkey=`, `/v1/subnets/conviction`, `/v1/subnets/owners`, `/v1/extrinsics?signer_address=`; new API per 02 §G7) improves fidelity when keyed.

| metric_key | definition | normaliser | w |
|---|---|---|---|
| `team_doxxed` | manual `team_members` table: ≥2 members with verifiable public profiles → 1.0; 1 → 0.6; named-but-unverifiable → 0.3; anonymous/undisclosed → 0 | map | .25 |
| `owner_identity` | owner coldkey has on-chain identity (TMC coldkey-identities: name + url/github) | binary | .05 |
| `owner_commitment` | S2: `lock_ratio = owner_perpetual_locked ÷ owner_total_stake` (weight .6) + `auto_lock_enabled` (.25) + `commit_pct = owner_perpetual_locked ÷ eligible_alpha_out ≥ 5 %` (.15); zero owner lock ∧ auto-lock off ∧ age > 90 d → 0 | custom | .20 |
| `owner_retention_30d` | S1: `1 − clip(sell_ratio, 0, 1)` where `sell_ratio = Σ owner alpha out of position (30 d) ÷ owner cut earned (30 d ≈ owner_cut_per_block × 7200 × 30)`; `sell_impact = Σ TAO received ÷ avg pool TAO`; any 7-d `sell_impact > 2 %` or `unstake_all_alpha`/`remove_stake_full_limit` → 0; `owner_cut_enabled = false` → 1.0 | custom | .20 |
| `control_stability_90d` | S3: no `PerpetualLockUpdated{enabled:false}` by owner (else 0), no unannounced coldkey swap (−0.6), no owner-hotkey re-point + identity change within 14 d (−0.4); each penalty halves every 45 d; operator whitelist for disclosed multisig migrations / post-compromise recoveries | custom | .15 |
| `owner_overhang` | S5: `dump_impact = owner_available_alpha × price ÷ tao_in`; < 5 % → 1, 25 % → 0 (+ pre-2026-05-08 registrations with unaccounted initial owner alpha → −0.2) | `zclip` inverted | .05 |
| `tenure` | occupant age: < 120 d (immune) → 0.3 … ≥ 365 d → 1 | `zclip(days, 120, 365)`, floor .3 | .10 |
Owner self-dealing (S4: owner validator stake share > 30 %, self-weight > 20 %, owner holding > 25 % with low lock ratio) is a **red-flag event only** in v1 (needs metagraph/weights via RPC or TaoStats); it becomes a metric in ts-1.1.
Absence: `team_members` empty **and** subnetalpha `team_info` matches anonymous tokens → `team_doxxed = 0`; empty and no editorial data → None (needs manual pass; surfaces on the "needs review" list). Owner metrics: owner coldkey unknown → None; TMC endpoints down → None; owner has never received a cut (pre-launch) → `owner_retention_30d` None.

**P5 · Community & Communication — pillar weight 12**
| metric_key | definition | normaliser | w |
|---|---|---|---|
| `venue_reachable` | at least one valid, non-expired community venue (own Discord, main-server channel id, TG group) | binary | .10 |
| `discord_members` | own-server `approximate_member_count` (main-server-only subnets → None) | `logcap(x, 5000)` | .15 |
| `presence_ratio` | daily-median presence ÷ members; healthy 0.05–0.35; < 0.02 with > 1,000 members → 0 (inflation flag) | custom | .10 |
| `discord_growth_30d` | Δ members over 30 d ÷ members | `zclip(x, −0.1, +0.3)` | .10 |
| `announcement_cadence_30d` | team posts in followed announcement channels + team X posts + TG posts, 30 d | `logcap(x, 12)` | .25 |
| `x_engagement_30d` | median (likes + 2·reposts + replies) per team post, cross-sectional | `rankg` | .10 |
| `x_followers` | team account followers | `logcap(x, 50_000)` | .05 |
| `silence_days` | days since last team communication on any channel | `recency(d, 7, 45)` | .15 |
Absence: no venue in any source → `venue_reachable = 0`; Discord/TG metrics None. X metrics follow their own rule: no team X handle or a gone account → `x_followers = 0` and the X legs absent. `silence_days = 0` only when no venue is reachable **and** no X account exists; otherwise it is observed from the legs. `announcement_cadence_30d` and `silence_days` are derived by spec 12 from the per-channel legs (02 §C3).

**P6 · Narrative & Sentiment (fast) — not part of Health; feeds Velocity only (§7.1)**
| metric_key | definition | normaliser | w |
|---|---|---|---|
| `event_impact_14d` | Σ over classified events of `materiality × direction × 0.5^(Δt/7d)`; taxonomy §7.4 | `tanh(x/2)` → [−1,1] → [0,1] | .40 |
| `polarity_7d` | engagement-weighted bullish share of bot-filtered posts mentioning the subnet (team + KOL + mentions), 7 d, with a single-account cap of 25 % | identity (0.5 = neutral; < 5 posts → None) | .25 |
| `attention_7d` | z of (TMC trending views + log mentions) vs the subnet's own 90-d baseline | `zclip(z, −2, +3)` | .20 |
| `kol_coverage_30d` | distinct credible KOLs (from `kol_accounts`, credibility ≥ 0.5) mentioning in 30 d | `logcap(x, 5)` | .15 |
`flow_attention_blend` (the other 40 % of Velocity) = mean of `net_flow_30d`, `chain_buys`, `attention_7d` normalised values.

### 7.4 Event taxonomy (LLM classification output; see 10-llm-classification)
`product_launch (+3)`, `release_upgrade (+1)`, `partnership_integration (+2, requires named counterparty else +1)`, `revenue_customer (+3 if numbers, +2 otherwise)`, `token_buyback_burn (+3)`, `exchange_listing (+3)`, `funding (+2)`, `emissions_tokenomics_change (0, informational)`, `team_change (−2 exit / +1 hire)`, `security_incident (−3)`, `delay_missed_milestone (−2)`, `governance_dispute (−2)`, `regulatory (−1)`, `owner_extraction_evidence (−3, always human-reviewed)`, `giveaway_promo (0, spam signal)`, `other (0)`. Direction and materiality (1–3) are model outputs; **negative events with materiality 3 require a second model pass (Sonnet) before counting.** Every label row stores `classifier_version`.

### 7.5 Red-flag gates (multiplicative, floor 0.25; each also an `events` row with severity)
| flag | trigger | multiplier |
|---|---|---|
| `emission_disabled` | TMC `emission_enabled = False` for an active occupant | ×0.50 |
| `dereg_imminent` | non-immune and in the bottom-3 by moving price (`deregistration_risk`) | ×0.60 |
| `miner_burn_zeroed` | implied_miner_burn > 0.8 for ≥ 3 consecutive daily snapshots | ×0.60 |
| `owner_extraction` | `owner_retention_30d = 0` (sell_ratio > 1, or 7-d sell_impact > 2 % of pool TAO, or `unstake_all_alpha`/`remove_stake_full_limit` by the owner), **or** `PerpetualLockUpdated{enabled:false}` by the owner coldkey (formal exit), or a human-confirmed `owner_extraction_evidence` event. A wallet compromise (ORO/Connito pattern) is indistinguishable on-chain — keep the flag, allow a post-mortem override to `key_compromise`. | ×0.70 |
| `owner_control_change` | unannounced coldkey swap of the owner, or `SubnetOwnerChanged`, within 30 d (auto-clears after 30 d unless paired with identity churn) | ×0.85 |
| `key_compromise` | human-confirmed event | ×0.50 |
| `repo_gone` | declared repo 404/private > 14 d and no active org repo | ×0.80 |
| `all_repos_archived` | every org repo archived, or declared repo archived with no successor | ×0.70 |
| `site_down_7d` | 7 consecutive failed daily checks | ×0.85 |
| `stale_identity` | dead invite + name turnover + no comms 30 d | ×0.90 |
| `silence_45d` | silence_days ≥ 45 | ×0.85 |
| `inflated_community` | presence_ratio < 0.02 with > 1,000 members for 7 d | ×0.90 |
Flags clear automatically when the trigger clears for 7 consecutive days (hysteresis), except human-confirmed ones, which clear manually.

### 7.6 Regime scaling of Velocity weight
`v_w = 0.15` baseline. When the network regime (from `app/quant/regime.py`, TAO/USD + Σ subnet prices realised vol and trend, 24-h window, same classifier as the old bot's `strategy/regime.py`) is `TRENDING` or `DISPERSED`, `v_w = 0.25`; in `DEAD`, `v_w = 0.10`. Regime is logged with each score row. This is the *only* time-varying weight in v1 and it is itself frozen per spec_version.

### 7.7 Confidence, tiers, smoothing, explainability
- `confidence = Σ W_k · coverage_k / Σ W_k` where coverage counts metrics observed within their freshness window (chain 1 d; GitHub 2 d; web 2 d; community fn metrics 2 d, 12-derived P5 combiners 1 d, weekly main-server Discord evidence 8 d; X 3 d; P6 1 d; `team_doxxed` 90 d; other P4 — `owner_identity`, owner S1–S5, `tenure` — 2 d; per-metric values are frozen in spec 12's registry, 02 §C6). `confidence < 0.4` ⇒ row flagged `insufficient_data`, excluded from rank tables by default.
- Tiers on `Composite_smoothed`: **A ≥ 75, B 60–74, C 45–59, D 30–44, F < 30**; enter a higher tier at ≥ threshold+3, drop below at ≤ threshold−3. Tier changes emit `tier_change` events (alerted).
- Each score row stores `explain`: per-pillar contributions, top-3 positive and negative metrics, active flags, coverage, regime, spec_version — the UI and Telegram `/sn N` render this verbatim.
- Rank churn is monitored: daily factor rank autocorrelation and quantile turnover are logged (target rank autocorr ≥ 0.9 on Health).

---

## 8. Collectors — binding decisions (details in specs 04–11)

| Collector | Spec | Key decisions |
|---|---|---|
| Registry & identity | 04 | Daily merge of chain `SubnetIdentitiesV3` (public RPC, 3 calls) + subnetalpha ACF + TMC `/subnets/` snapshot; occupant keying; `identity_changed`, `source_disagreement`, `netuid_reoccupied` events; manual overrides table. |
| Chain (TMC) | 05 | SSE recorder (12 hot fields per block, rest hourly) with heartbeat + REST fallback; daily analytics pull (1W repair + weekly 1Y); weekly holders; gate reproduction + implied miner burn; local immunity; dereg hazard (ported `dereg_risk.py` fed with moving-price rank). Optional RPC/lite-node collector for `MinerBurned`/constants. Process-wide TMC token bucket ≤ 8 req/min. |
| GitHub | 06 | Resolver keyed on numeric repo id (301 → `/repositories/{id}`; 404 → org enumeration; org-only URLs handled); org-level activity; bot/commit-spam filters; Electric-Capital author classification; PAT required; ETag caching; `repo_renamed/moved/gone/archived` events. TMC `github-stats` used only as a cross-check. |
| Web presence | 07 | Daily HEAD/GET with UA, redirect chain, parked-page heuristics, TLS expiry, trafilatura text hash + difflib delta, Wayback first-capture, docs-URL check; SPN2 weekly snapshot optional. |
| Community | 08 | Discord invite metadata every 6 h (own servers), weekly re-validation of main-server channel ids, widget.json hourly where enabled; operator's aggregator server + Bot reading **Followed** announcement channels; `t.me` member counts daily; Telethon explicitly out of scope for v1. No usernames stored. |
| X / KOL | 09 | `XProvider` interface (`twitterapi_io`, `official_v2`, `null`); team accounts daily with `since_id`; KOLs 4×/day; mention search for top-40 subnets daily; cost governor caps; bot heuristics; KOL list seeded from research (`@opentensor, @const_reborn, @taostats, @0xSammy, @KeithSingery, @taomedia_, …`) with a `credibility` column the operator edits. |
| News / RSS / YouTube | 09 (appendix) | Google News RSS, cointelegraph/decrypt/theblock RSS, YouTube channel RSS, Medium/Substack feeds via `feedparser` with ETag; Novelty Search playlist. |
| LLM classification | 10 | Haiku 4.5 Batch API, 20 posts per request, JSON schema output (`polarity, intensity, event_type, direction, materiality, entities[netuids], is_spam, confidence`); Sonnet 5 second pass on materiality-3 negatives; weekly Opus 5 narrative for tier A/B + movers; `classifier_version` on every label; cost governor. |
| Team & integrity | 11 | Manual `team_members` with evidence URLs and `verified_by/verified_at` (CLI + Telegram `/team add`); owner analytics per the S1–S5 model in `research/02-gapfill-owner-extraction-mechanics.md` (owner coldkey/hotkey from TMC `/subnets/{id}/` latest_snapshot; position + locks from `/accounts/coldkey-stakes/{ck}/` and `/analytics/neuron/{netuid}/{hk}/`; sells from `/extrinsics/staking-activity/?coldkey=` + `/transactions/stakes/stake-removed/`; locks/auto-lock from the Rufus conviction `table.json`; swaps from subnetradar + TMC; TaoStats when keyed); red-flag detectors; seed from `research/subnet_edge` and subnetalpha `team_info` parsing (names in `<strong>`, LinkedIn hrefs) as *unverified* candidates. |

---

## 9. Outputs and interfaces

- **HTTP API (`:8090`, loopback by default, `X-Auth-Token` on mutations):** `GET /health` (built by spec 01: `status ok|degraded|initializing`, `degraded_reasons[]`, `version`, `collectors{job:{last_run,last_ok,last_error,consecutive_failures,…}}`, `spend{group:{usd,cap_usd,pct,paused,reserved_usd}}` per spec 16, `db{…}`, `kill_switch_active` — 02 §G6), `GET /api/v1/scores/latest?spec=ts-1.0[&include_insufficient=1]` (latest row per occupant; ISO `as_of`/`computed_at`; `red_flags` as names — 02 §H1), `GET /api/v1/scores/history?occupant=`, `GET /api/v1/subnets/{netuid}` (explain + evidence links), `GET /api/v1/events?since=&severity=`, `GET /api/v1/matrix.csv`, `GET /api/v1/stream` (SSE score updates), `POST /api/v1/manual/{team|product|note|flag}` (authed), `POST /api/v1/control/{pause|resume}/{collector}` (authed).
- **Telegram:** alerts (tier change, new red flag, collector failing ×5, spend ≥ 80 % of cap, backup failure, dereg proximity for watchlist), commands `/top [n]`, `/sn <netuid>`, `/flags`, `/movers`, `/health`, `/team add <netuid> "<name>" "<role>" <url>`, `/pause <collector>`.
- **Quant CLI:** `python -m app.quant factor --factors composite,quality,velocity,gate_margin --horizons 7,28,84 --controls size --scope live|survivorship`, `survival`, `walk-forward`, `rank-stability`, writing `data/quant/results/factor_*.json` in the old explorer's JSON shape.
- **UI (Phase 11):** three pages — Matrix (sortable table + pillar heatmap + tier colour), Subnet (explain + sparklines + evidence), Events. Dark, minimal, forked shell.
- **Trading contract (15):** `GET /api/v1/scores/latest` returns `{as_of, ttl_seconds, spec_version, scores:[{netuid, occupant_key, quality, velocity, composite, confidence, tier, red_flags[]}]}` exactly as spec 14 serves it (the bot has no producer route of its own; a row with null composite/quality/tier is neutral for that netuid); the bot treats missing/stale/low-confidence as neutral, clamps its factor to `[0.5, 1.0]` (de-risk only) until Phase C, and reads `red_flags` as hard entry blocks it may opt into.

---

## 10. Validation and the evidence gates (spec 13)

**Panel.** `app/quant/panel_builder.py` builds a death-inclusive, segmented panel (ported `factor_panel.py`) from `price_bars` (imported `pool_history` + TMC candles + forward capture) at the **24 h grid** (survivorship scope, from 2025-09-22) and **4 h grid** (live scope, from 2026-04-22), stamping every factor **point-in-time** from `metric_values` where `observed_at ≤ bucket`. Never forward-fill; factor returns `None` when unobserved.

**Labels.** (a) forward log return at 7 / 28 / 84 bars (death-inclusive via segment end); (b) forward change in emission share at 28 / 84 d; (c) survival: deregistration or emission_disabled within 90 d; (d) gate-crossing: entering/leaving the top-32 within 28 d.

**Honestly backtestable now (reconstructible history):** P1 chain metrics (TMC 365-d analytics + imported ledger + OnFinality backfill), P2 GitHub metrics (commit history has timestamps; org membership *as of today* is a mild look-ahead — flagged), P4 `owner_commitment` (TMC analytics `owner_personal_locked_alpha` daily, 365 d), `tenure`, `control_stability_90d` (swap history via TaoStats `/v1/subnets/owners` if keyed), `owner_retention_30d` (only with TaoStats delegation history or TMC staking-activity backfill). **Forward-only:** everything in P3, P5, P6 and `team_doxxed`. The first honest full-composite validation therefore needs ≥ 90 days of forward collection; the chain+GitHub sub-composite can be tested at build time.

**Gate to promote any score into the trading contract's `active` mode (pre-registered, same bar as the old bot):**
1. Rank-IC > 0 with t ≥ 2 on **both** scopes at the chosen horizon, non-overlapping steps;
2. IC survives `--controls size,emission_share` (else it is a size proxy → NO-GO);
3. Monotone-ish decile curve, holds net-of-cost at `trade_tao = 2` with the ported AMM economics;
4. DSR > 0.95 (necessary, not sufficient; log every variant tried as N);
5. Walk-forward with ≥ 4 OOS folds: pooled OOS IC > 0, |t| ≥ 2, decay ratio ≥ 0.25;
6. Survival route (alternative to 1-5 for Health): bottom-tercile 90-d death/zero-emission rate ≥ 2× top tercile with n ≥ 10 events, both scopes;
7. ≥ 30 days of `observe`-mode telemetry in the bot with live signal coverage > 60 %.
Failing the gate leaves the feed in `observe` forever at zero cost — that is a valid outcome.

---

## 11. Operations

- **systemd** unit `taosentinel.service` (`Type=notify`, `WatchdogSec=120`, liveness-conditional `WATCHDOG=1` keyed on per-collector `last_run` with per-job max-age, `Restart=on-failure`, `After=network-online.target time-sync.target`), `taosentinel-frontend.service` (Phase 11), optional `taosentinel-update.timer` (git pull + compileall + import smoke + `/health` gate + rollback).
- **Backups:** daily at 03:30Z (after the 03:00Z `storage_retention_daily` job): `VACUUM INTO <dest>.tmp` + `quick_check` + atomic rename into `data/backups/`, gzip'd, 7 daily + 8 weekly inside `DB_BACKUP_MAX_TOTAL_GB` = 12 (the 2 newest are never deleted; the newest is also kept uncompressed for spec 13's snapshot), plus a USB/NAS mirror; backgrounded, never in the start path. **Disk: ≥ 60 GB free required** (02 §I1). Restore runbook in 16.
- **Cost governor (`app/costs.py`):** per-provider monthly USD ledger (`provider_spend`), soft alert at 80 %, hard stop at 100 % (collector auto-pauses, `/health` shows `paused: budget`), caps in config (`X_MONTHLY_CAP_USD=25`, or 75 with the official X API; `LLM_MONTHLY_CAP_USD=60`; `TAOSTATS_MONTHLY_CAP_REQ`); a cap of 0 disables that paid provider (fail-closed). API: `check / reserve / commit / release / record / snapshot` (02 §G5, spec 16).
- **Rate-limit discipline:** one HTTP layer (`app/http.py` over `app/ratelimit.py`, spec 01) with a process-wide token bucket per provider (TMC: 8/min, burst 2, 500/h, 90 k/month; every TMC caller is listed in spec 05's budget table); honour `Retry-After`/`X-RateLimit-*`; never retry a 404 within a cycle; 403 on Discord counts toward the IP ban budget → widget probes weekly.
- **Kill switches:** `./KILL_SWITCH` pauses all paid collectors and LLM; `./PAUSE_ALL` pauses every non-ops job; `./SCORING_KILLED` stops publishing new scores (feed returns last good with `stale=true`). Per-collector pauses (API/Telegram/CLI) are rows in `control_state` (02 §B12).
- **Laptop power loss:** all jobs are idempotent and watermark-based; the SSE recorder catches up from TMC `analytics ?span=1W` hourly rows on restart; GitHub/community re-observe on next tick; scoring re-runs for missed `as_of` days from stored raw data (marked `backfilled=true`).
- **Logs:** JSONL per day, 10 MB rotation, 14-day retention; `collector_runs` table is the long-term audit.

---

## 12. Phased implementation plan

Each phase is a component spec with verify-checks; a phase is "done" when its checks pass on the laptop and `pytest` is green. Order is chosen so that useful output appears early (chain matrix by end of Phase 2) and paid/optional sources come last.

| Phase | Spec | Delivers | Depends on |
|---|---|---|---|
| 0 | 02-cross-cutting-conventions (read first), 01-repo-skeleton-runtime, 03-storage-schema | Repo, config, DB, runtime, `/health`, Telegram alert, tests, systemd | — |
| 1 | 04-subnet-registry-and-identity | Occupant-keyed registry from chain + subnetalpha + TMC; identity events | 0 |
| 2 | 05-chain-collector | SSE recorder, analytics history, gate/burn model, dereg hazard, **first matrix (P1 only)** | 1 |
| 3 | 17-data-migration-and-backfill | Old ledger import, TMC candles/analytics backfill, OnFinality gap fill | 2 |
| 4 | 06-github-collector | Resolver + org activity + P2 metrics | 1 |
| 5 | 07-web-presence-collector | P3 automated metrics | 1 |
| 6 | 08-community-collector | Discord/TG counts, announcement follows | 1 |
| 7 | 11-team-and-integrity | Manual team table, owner analytics, red-flag detectors, P4 | 2 |
| 8 | 12-feature-store-and-scoring-engine | `metric_values`, normalisers, `ts-1.0` scoring, tiers, explain, alerts — **full Health matrix** | 2,4,5,6,7 |
| 9 | 09-x-collector-and-kol, 10-llm-classification | X provider, KOL list, RSS/YouTube/news, LLM labels, P6, Velocity | 8 |
| 10 | 13-validation-harness | Ported quant stack, PIT panel builder, gates, CLI, first chain+GitHub factor study | 3, 8 |
| 11 | 14-api-telegram-and-ui | Full API, Telegram commands, minimal Next.js matrix | 8 |
| 12 | 15-trading-bot-integration | Score feed contract + bot-side consumer (observe mode) | 8, 10, 11 |
| — | 16-ops-deploy-and-observability | systemd, backups, auto-update, cost governor, runbooks | 0 (continuous) |
| — | 18-operator-checklist | Everything the operator must provide/do | — |

Rough effort for a capable coding model working from these specs: Phases 0–2 in the first sitting (the matrix appears), 3–8 over the following few days, 9–12 as keys arrive.

---

## 13. Open decisions (operator input needed — also in 18-operator-checklist)

1. **X access:** approve twitterapi.io (~$5–20/mo, unofficial) as primary, or official X API only (~$70/mo at 3 posts/account/day), or none for v1 (P5/P6 degrade gracefully)?
2. **TaoStats re-subscription:** Standard ($49/mo) unlocks owner/whale stake events and hourly pool history; keyless TMC covers ~85 % of the on-chain pillar without it. Recommend: start keyless, subscribe when Phase 7 needs `owner_retention_30d` at full fidelity.
3. **Local lite node:** only needed for `MinerBurned`/constants per block; needs ~128 GB. Recommend: defer; use the implied-burn fit.
4. **LLM budget:** approve ~$20–60/mo (Haiku Batch + Sonnet escalation)? Without it, P6 falls back to rule-based keyword events (lower quality, $0).
5. **Pi recovery:** the Pi at `192.168.0.62` likely holds ~1.9M more `pool_history` rows and 10 OPEN 1-τ value positions. Recover before the laptop build (checklist).
6. **Repo hosting:** private GitHub repo + PAT for auto-update, or manual `git pull`?
7. **Disk:** how much free space does the laptop have? **≥ 60 GB is required** (DB ≈ 9 GB in year 1; peak ≈ 4.7 × DB + 5 GB during backups). Less only works with shorter tick retention, a smaller backup budget and a USB/NAS mirror.
8. **Discord crosspost text:** keep announcement text for 90 d (`DISCORD_STORE_TEXT=true`, default; better LLM labels) or hash-only (`false`; classification in flight)?
9. **Pre-ledger history:** backfill the pre-ledger dTAO era (2025-02-13 → 2025-09-22, ~1 h on OnFinality) so survival panels cover the whole dTAO period? Recommended yes.

---

## 14. Glossary
`alpha` subnet token · `dTAO` dynamic-TAO pools (since 2025-02-13) · `moving price` on-chain EMA of alpha price (`SubnetMovingPrice`) that drives emission and pruning · `gate` v440 Hill gate concentrating emission in the top ~32 subnets · `MinerBurned` share of miner incentive routed to owner hotkeys and burned (zeroes emission when ≈1) · `excess_tao` protocol TAO used to buy alpha ("chain buys") · `immunity` 864,000 blocks (120 d) after registration during which a subnet cannot be pruned · `occupant` a (netuid, registration block) pair · `PIT` point-in-time · `IC` cross-sectional rank correlation between factor and forward return · `DSR` deflated Sharpe ratio.
