# [code] whale-research-ops

**Topic:** SubnetTrader reusable areas for TaoSentinel: whale tracking, per-subnet research notes, ops/deploy patterns

## Findings

## 1. Whale tracking (`app/whale/`, `scripts/`, `data/whale_*.json`)

**Status (spec header, `specs/feature-whale-wallet-tracker.md:3-8`):** implemented 2026-07-10, Phases 0-2 built and smoke-run, ships dormant (`WHALE_TRACKER_ENABLED=false`). Phase 3 (live signal) never built. Directly portable: `app/whale/events.py` is stdlib-only by design (`events.py:3-7`) so it runs without the app stack.

### Taostats delegation feed (the core endpoint)
- `GET https://api.taostats.io/api/delegation/v1` — constant `DELEGATION_PATH` at `events.py:20`; `PAGE_LIMIT = 200` (Taostats max, `events.py:21`); `DTAO_LAUNCH_TS = 1739404800` (2025-02-13, coverage floor, `events.py:22`).
- Per-event fields (spec:67-72): `id` (unique, e.g. `finney-64-0x…-8590783-185`), `action` (DELEGATE→BUY / UNDELEGATE→SELL, `_ACTION_MAP` `events.py:80`), `nominator` (coldkey, arrives as `{"ss58": …}` object), `delegate` (hotkey), `delegate_name`, `netuid`, `amount` (τ rao string), `alpha` (rao string), `alpha_price_in_tao`, `alpha_price_in_usd`, `usd`, `slippage`, `fee`, `is_transfer`, `transfer_address`, `extrinsic_id`, `block_number`, `timestamp` (ISO-8601 `Z`).
- Query params (`build_delegation_params`, `events.py:135-158`): `page`, `limit`, `order=timestamp_asc`, optional `nominator`, `timestamp_start`, `timestamp_end`. Two verified shapes: per-nominator (backfill) and **global feed with no nominator** (incremental poll).
- Response shape (`parse_delegation_response`, `events.py:161-170`): `{"data": [...], "pagination": {"next_page": N}}`.
- Auth: `Authorization: <key>` header, no `Bearer` prefix (`collector.py:154-156`, `build_whale_cohort.py:76-78`).
- **Rate limit (measured, spec:82-85):** free tier 429s after ~5 calls in 7s, no rate headers, body `{"status_code":429,"message":"Rate Limited. Try Again Later."}`. All three clients self-throttle to `60/req_per_min` seconds between calls (default 5/min = 12s) and do a single 60s backoff retry on 429 (`collector.py:78-88`, `build_whale_cohort.py:70-94`, `backfill_whale_events.py:96-110`).
- Other Taostats endpoints verified (spec:77-91, `build_whale_cohort.py:47-49`): `/api/account/latest/v1?order=balance_total_desc` (rich list; `rank`, `balance_total`, `balance_free`, `address`, `coldkey_swap`), `/api/dtao/stake_balance_aggregated/latest/v1` (staking coldkeys; `coldkey`, `rank`, `total_balance_as_tao`, `block_number`), `/api/exchange/v1` (~10-11 exchange coldkeys with `name`), `/api/identity/latest/v1`. PRO-gated: `/api/dtao/stake_balance/portfolio/v1` (precomputed per-whale P&L).

### Collector design (`app/whale/collector.py`)
- Constants: `OVERLAP_SEC=600`, `MAX_PAGES_PER_POLL=30`, `FAIL_ALERT_THRESHOLD=5` (lines 36-38).
- `_poll_once` (lines 124-190): loads cohort → set of ss58 minus `WHALE_EXCLUDE_COLDKEYS` → saves cohort snapshot idempotently (`db.save_whale_cohort`) → watermark `MAX(whale_events.timestamp) - 600s` → pages the **global** feed ascending, classifies each event, keeps only cohort coldkeys client-side, `INSERT OR IGNORE`. Cost 2-8 pages/poll regardless of cohort size (spec:255-263).
- Fail-safe posture (lines 91-121): never raises into the scheduler; `_status` dict (`last_run`, `last_error`, `last_inserted`, `last_pages`, `consecutive_failures`) surfaced on `/health`; Telegram alert exactly once at the 5th consecutive failure, re-arms on success.
- Config keys (spec:463-477): `WHALE_TRACKER_ENABLED`, `WHALE_POLL_MINUTES=30`, `WHALE_COHORT_PATH`, `WHALE_EXCLUDE_COLDKEYS`, `WHALE_MAX_REQ_PER_MIN=5`, `WHALE_SOURCE`, plus dormant Phase-3 `WHALE_SIGNAL_MODE/FACTOR_FLOOR/FACTOR_CEIL/TTL_SEC/KILL_SWITCH_PATH`.

### Schema (spec:415-450)
`whale_events(id TEXT PK, coldkey, hotkey, netuid, action BUY|SELL, tao_amount, alpha_amount, price_tao, usd, fee_tao, slippage, is_transfer INT, size_vs_pool_pct, block_number, timestamp INT unix, source)` with indexes on `(coldkey, timestamp DESC)` and `(netuid, timestamp DESC)`; `whale_cohort(as_of_block, coldkey, rank, balance_total_tao, label, label_source, attribution, PK(as_of_block, coldkey))` append-only for point-in-time membership. Backfill script's `INSERT_SQL` at `backfill_whale_events.py:49-55` mirrors this column list.

### move_stake phantom-pair gotcha
- Chain-level (spec:50-56): `move_stake`/`transfer_stake`/`swap_stake` emit a `StakeRemoved`+`StakeAdded` pair plus a summary event (`StakeMoved`, `StakeSwapped`, `StakeTransferred`). Same-netuid moves show `fee=0` and a spot-price τ-equivalent despite no AMM trade. Also `AutoStakeAdded` (emission auto-compound) fires ~152×/60 blocks with `extrinsic_idx=None` — filter to `ApplyExtrinsic` phase.
- Taostats-level (spec:273-282): `move_stake` between a whale's own hotkeys emits an UNDELEGATE+DELEGATE pair with same coldkey/netuid/block, fee 0, `is_transfer` **null** (not true!). **240 of 270 raw "trades" from the top-2 whales were phantom move legs** (one batch hotkey reshuffle).
- Fix implemented in `app/backtest/whale_impact.py:158-178`: SQL `GROUP BY coldkey, netuid, block_number HAVING SUM(action='BUY')>0 AND SUM(action='SELL')>0` builds `move_groups`; every row in such a group is dropped and counted as `moved_pairs`. Cross-netuid swaps are kept (they genuinely trade both pools). Raw table keeps everything; the trade view also filters `is_transfer=0 AND netuid>0` (`whale_impact.py:147-149`).

### Cohort ranking (`scripts/build_whale_cohort.py`)
1. Rank by **aggregated stake** (`/api/dtao/stake_balance_aggregated/latest/v1`, root+alpha valued in τ), NOT raw balance — top of balance list is exchange custody ~100% free (lines 11-15). Pull `size * buffer` (default 200 × 1.5) candidates.
2. Enrich from balance rich list (default depth 400) for `free_fraction = balance_free / balance_total`, `balance_rank`, pending `coldkey_swap` (lines 172-188).
3. Exclude in order: `/api/exchange/v1` coldkeys → operator excludes (`--exclude` / `WHALE_EXCLUDE_COLDKEYS`) → `free_fraction >= 0.95` custody heuristic (lines 194-202).
4. Label from `data/whale_labels.json` (`{"labels": {ss58: {label, source, attribution}}}`); unlabeled → `attribution: "heuristic"`.
5. Output `data/whale_cohort.json`: `{as_of_block, as_of_ts, generated_by, params, coldkeys:[{ss58, rank, total_stake_tao, balance_total_tao, free_fraction, balance_rank, coldkey_swap, label, label_source, attribution}], excluded:[...+excluded_reason]}`.
- Actual artifact (2026-07-10, block 8,590,789): 200 coldkeys, rank 1 = 274,350τ staked (2.4% free), rank 7 excluded as `exchange:Taobridge` (`whale_cohort.json:2425`). `whale_labels.json` ships with `"labels": {}` — no identities asserted (Const policy: no ss58 without two independent sources, spec:136-142).
- Backfill (`backfill_whale_events.py`): per-coldkey `nominator=` pages from `DTAO_LAUNCH_TS`; resumable via `<db>.whale_backfill_progress.json` sidecar `{coldkey: last_ts}`; WAL + `busy_timeout=5000`; applies `models.sql` idempotently (lines 71-77).

### Methodology worth carrying (spec:158-183)
Size-match benchmark (Maymin SMB ~1.01%/day mechanical premium), anchor at post-trade spot (t0+1), metaorder decay baseline (~2/3 same-day, ~1/3 permanent), net-of-cost + latency lag, point-in-time cohort (no lookahead), coldkey-swap label rot (`schedule_coldkey_swap`, 5-day delay).

## 2. Per-subnet research notes (`research/subnet_edge/`)

**Scope (README:5):** 128 subnets enumerated from subnetalpha.ai; ~25 got per-subnet reports, rest in `_longtail_brief.md`. Written for a *trading-bot edge* lens, not investment quality — verdicts are "does this subnet give OUR bot a signal", so they need re-scoring for TaoSentinel.

### Template (identical across all sn*.md)
```
# SN<N> <Name>
**Verdict:** NONE|LOW|MEDIUM|MEDIUM-HIGH|HIGH
**Cost:** <pricing/free tier>
**Integration effort:** low|medium|high
## What it produces
## Public API / how to consume   (site, docs, GitHub, dashboard URLs, auth model)
## How it could give our bot edge (numbered use cases + proposed module path)
## Risks / why it might not work
## Next steps if we pursue (numbered)
## Sources
```
Category files (`_category_*.md`) use: Ranked picks → Skipped → First concrete action. Long-tail uses a table `| SN | Name | Verdict | Why |` grouped by vertical.

### Qualitative signals the operator cared about
- Real commercial traction: "~$11k MRR", "~1,000 registered users" (sn22:13), "~100B tokens/day throughput" (sn64:8), "55B+ rows" (sn13:8).
- Public API existence + documentation quality; free tier vs signup-gated pricing (flagged as the #1 unknown, README:79).
- Team transparency: "Awaiting content… team section is a yellow flag" (sn88:26); "anonymous team + thin GitHub make it speculative" (data_intel:11).
- GitHub repo linked and active (every report cites the repo).
- Product status: "waitlist", "public API planned Q3 2026", "paused for new clients" all downgraded verdicts.
- Reliability risk: "subnet-hosted models can disappear if miners deregister" (sn64:26), "startup at $11k MRR" instability.
- Directory "no info" subnets treated as early-stage/stealth/deregistered → watch-only (longtail:105-111).
- Re-scan cadence: 30-90 days, "Bittensor subnets churn fast" (README:80).

### Subnet-specific facts worth carrying forward
- **SN1 Apex** (Macrocosmos): macrocosmos.ai, github.com/macrocosm-os/prompting; "four distinct products"; LOW for bot but big-team signal.
- **SN13 Data Universe** (Macrocosmos): dashboard `sn13-dashboard.api.macrocosmos.ai`, github.com/macrocosm-os/data-universe, HF public dumps (free), "Gravity" on-demand scraping + `GravityClient` SDK, MCP integration; scrapes X + Reddit (YouTube/GitHub planned). **Directly useful to TaoSentinel as a Twitter/Reddit data source.**
- **SN22 Desearch**: desearch.ai, docs `desearch.ai/docs/guide/use-cases/search-engine`, github.com/Desearch-ai/subnet-22; Twitter/Reddit/Google search + AI summary, scoring 50% Twitter/40% summary/10% web; 20 free credits or $5/~500 queries; niche subnets have ~10 tweets/week (n<10 sentiment unstable). **Candidate X/Twitter ingestion path if direct X API is too expensive.**
- **SN50 Synth**: synthdata.co, `api.synthdata.co/docs`, github.com/mode-network/synth-subnet; free `/v2/leaderboard/*`, `/validation/{miner,prompts,realized-path,scores/*}` endpoints; paid live predictions via dashboard.synthdata.co; BTC/ETH/SOL/XAU/XRP/HYPE/WTIOIL + tokenized equities.
- **SN64 Chutes**: chutes.ai, github.com/rayonlabs/chutes; OpenAI-compatible REST, free tier, pay-per-token in TAO/fiat, DeepSeek V3; **cheap LLM substrate for classifying news/tweets/Discord posts.**
- **SN88 Investing**: stakingalpha.com, github.com/mobiusfund/investing (`simst` backtester), @Investing88ai; scores staking strategies by return/vol/drawdown/slippage; "88 Quant Fund" paused Jan 2025.
- **SN19 Blockmachine**: operator is a paying RPC customer ($9/mo) — keyed archive endpoint `wss://rpc.blockmachine.io?authorization={KEY}` (spec:101-103).
- Watch list: SN116 TaoLend, SN113 TensorUSD, SN6 Numinous (waitlist), SN24 Quasar (API Q3 2026), SN14 TAO Hash.
- Full 128-subnet name→netuid map exists in `_longtail_brief.md` (as of mid-2026).

## 3. Ops patterns

### systemd units (`ops/systemd/`)
- `subnettrader.service` (53 lines): `After=network-online.target time-sync.target` (NTP before wall-clock logic, spec:129-131); `[Unit] StartLimitIntervalSec=60 StartLimitBurst=5`; `Type=notify NotifyAccess=main WatchdogSec=120 TimeoutStartSec=120`; `User=pi`; `ExecStart=<repo>/.venv/bin/python -u -m app.main`; `StandardOutput/Error=append:<repo>/data/bot.log` (needs systemd ≥240); `Restart=on-failure RestartSec=10`; `TimeoutStopSec=30 KillSignal=SIGTERM`; hardening `NoNewPrivileges=true ProtectSystem=full ProtectHome=false PrivateTmp=true`; `WantedBy=multi-user.target`.
- `subnettrader-frontend.service`: `After=/Wants=subnettrader.service` (Wants not Requires so backend crash doesn't tear down UI); `Environment=PATH=…/CURRENT/bin` rewritten by installer via `sed` (`ops/install-systemd.sh:24-26`); `ExecStart=/usr/bin/env npm run start -- -p 3000`; logs to `/tmp/frontend.log`.
- `ops/install-systemd.sh`: root check, detects node via `sudo -u pi bash -lc 'command -v node'`, renders frontend unit, `install -m 644` backend unit, `daemon-reload`, `enable`.

### Watchdog + circuit breaker (`specs/ops-chain-resilience.md`)
- Liveness-conditional `WATCHDOG=1` (not bare heartbeat): inline `sd_notify()` to `$NOTIFY_SOCKET` (spec:170-183, handles abstract `@` sockets, no `sdnotify` dep); `systemd_watchdog_loop` pings at `WATCHDOG_USEC/2` only when `_exit_watchers_live(max_age)` — every enabled worker's `last_run` within `WATCHDOG_LIVENESS_MAX_AGE_SEC=90` (< `WatchdogSec=120`). `READY=1` sent after scheduler start, before slow first cycle.
- Circuit breaker (spec:290-407): bounded `ThreadPoolExecutor(max_workers=4)` for blocking RPC; `_consecutive_failures` ≥ `SUBSTRATE_BREAKER_THRESHOLD=3` → open for `SUBSTRATE_BREAKER_COOLDOWN_SEC=30`; reconnect failure opens immediately; half-open is implicit (next tick tries one real query); `chain_connected` exposed on `/health`. `asyncio.wait_for(run_in_executor(...), timeout=15)` on every chain call.
- Retired `watchdog.sh` cron (double-process footgun); `install.sh:156-163` strips legacy crontab lines.

### Git auto-deploy (`deploy/`, `specs/ops-git-auto-deploy.md`)
- `subnettrader-update.timer`: `OnBootSec=2min OnUnitActiveSec=2min AccuracySec=15s`; `.service` is `Type=oneshot`, `TimeoutStartSec=600`, `GIT_TRACE*=0` env.
- `auto-update.sh` (179 lines): PAT read from `.env` `GITHUB_PAT=` with CR/quote stripping (lines 62-72), fetch `https://x-access-token:${tok}@github.com/...`; no-op if SHA equal; `git reset --hard` (only tracked files); pip only if `requirements.txt` changed; frontend rebuild only if `frontend/` changed; **safety gate**: `compileall app/` + `python -c 'import app.main'` (lines 119-141); restart; poll `/health` for `"status":"ok"` up to 60s (lines 154-171); `rollback()` to previous SHA on any failure (lines 39-54). Body wrapped in `main()` called at EOF so mid-run reset can't change executing code.
- Reset-safe because all live state is gitignored (`.env`, `.venv/`, `data/*.db*`, `data/logs`, `data/backups`). Gotcha: tracked `data/fundamentals.json` gets reverted on deploy (spec:37-41).

### Backups (`app/storage/backup.py`, `scripts/backup_db.py`, `config.py:542-547`)
- `Database.backup_to` (`db.py:38-66`): `VACUUM INTO` from a dedicated connection → `PRAGMA quick_check` → atomic rename. Works while live writer runs.
- `create_backup()`: name `ledger-YYYYMMDDTHHMMSSZ.db` (lexicographic == chronological), best-effort mirror to separate device via `.tmp` + `replace`, prune to `retention` newest per dir.
- Settings: `DB_BACKUP_ENABLED=True`, `DB_BACKUP_DIR=data/backups`, `DB_BACKUP_MIRROR_DIR=""`, `DB_BACKUP_RETENTION=7`, `DB_BACKUP_INTERVAL_HOURS=24`, `DB_BACKUP_MIN_AGE_HOURS=12.0`.
- Restore: copy snapshot over `data/ledger.db`, `rm -f *.db-wal *.db-shm` (bot stopped).

### Telegram (`app/notifications/telegram.py`, 546 lines)
- `send_alert(text)` (lines 97-111): fire-and-forget HTML `sendMessage`, 5s timeout, silent no-op without `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID`, token redacted from logs.
- `TelegramBot` long-polling (`getUpdates` timeout 20s, `allowed_updates=["message","callback_query"]`), discards pending updates on start (line 229), chat-id allowlist (lines 271-274, 286-289), `setMyCommands` registration (lines 488-514).
- Command surface: `/status /health`, `/diag`, `/pnl`, `/subnet <n>` (`/sn`, `/price`), `/positions [limit]`, `/close <n>`, `/pause`, `/resume`, `/run`, `/export /tax` (sends CSV document), `/history [limit]`, `/help`.
- Inline keyboard UI: `main_menu_keyboard`, `positions_keyboard`, `confirm_close_keyboard` (two-step confirm for destructive actions), `back_keyboard`; callback data scheme `act:*`, `ask:close:N`, `do:close:N`; `answer_callback` first to clear spinner (lines 301-304); `_toast` strips HTML to ≤190 chars.
- Handler injection via `TelegramCommandHandlers` dataclass of async callables (lines 27-45) — transport fully decoupled from domain.

### Logging (`app/logging/logger.py`)
- `JsonlFileHandler`: date-stamped `data/logs/YYYY-MM-DD.jsonl`, 10 MB rotation to `.N` suffix, line-buffered, entry `{ts, level, logger, message, data?}`.
- `StructuredLogger` singleton `logger` with `(msg, data: dict|None)` signature — **no printf args** (chain-resilience spec:312 notes this). Console + JSONL handlers, level from `LOG_LEVEL` (validated `config.py:637-643`).

### Install / containers
- `install.sh`: OS/arch detect, Python ≥3.11 check, venv + `pip install -r requirements.txt` (Rust hint on failure), Node 20 via NodeSource if missing, `.env.example` → `.env`, `mkdir -p data/logs data/exports`.
- `Dockerfile`: multi-stage `node:20-slim` build → `python:3.11-slim` backend (gcc, libffi-dev, curl; non-root `trader`; `HEALTHCHECK curl -sf :8081/health`) → `node:20-slim` frontend runtime.
- `docker-compose.yml`: `restart: unless-stopped`, `env_file: .env`, volumes `./data:/app/data` + wallet ro + kill-switch mount, healthcheck 60s/10s/3 retries/30s start, json-file logging capped 50m×5 / 10m×3, frontend `depends_on: condition: service_healthy`.

### Exact dependency pins (`requirements.txt`)
```
httpx>=0.27,<1.0
pydantic>=2.5,<3.0
pydantic-settings>=2.1,<3.0
APScheduler>=3.10,<4.0
aiosqlite>=0.19,<1.0
python-dotenv>=1.0,<2.0
bittensor>=10.1,<11.0
bittensor-wallet>=4.0,<5.0
fastapi>=0.109,<1.0
uvicorn[standard]>=0.27,<1.0
matplotlib>=3.9
requests>=2.31
pytest>=7.4,<9.0
pytest-asyncio>=0.23,<1.0
```
Note `__pycache__` has both `cpython-313` and `cpython-314` files — dev box ran 3.14 too. Dockerfile uses 3.11; install.sh requires ≥3.11.

## Reusable assets

- `E:\SN_Trader\SubnetTrader\app\whale\events.py` — Stdlib-only Taostats delegation-event parser/classifier: ss58_of, rao_to_tao, parse_ts, classify_event, build_delegation_params, parse_delegation_response; DELEGATION_PATH, PAGE_LIMIT=200, DTAO_LAUNCH_TS
  - How to reuse: Copy verbatim into TaoSentinel's Taostats client module; zero app dependencies. Extend classify_event with the same-block BUY+SELL pair flag if you want phantom moves tagged at ingest rather than study time.
- `E:\SN_Trader\SubnetTrader\app\whale\collector.py` — Incremental global-feed poller with watermark-minus-overlap, 30-page cap, 5 req/min throttle, 429 backoff, consecutive-failure Telegram alert, /health status dict
  - How to reuse: Template for every TaoSentinel collector job (GitHub, X, Discord, Taostats): copy the _status dict + poll_whale_events/_poll_once split + FAIL_ALERT_THRESHOLD pattern; swap the fetch/classify body.
- `E:\SN_Trader\SubnetTrader\scripts\build_whale_cohort.py` — Point-in-time top-200 whale cohort builder: stake-ranked, exchange/custody/operator exclusions, label join, as_of_block stamp; Throttled sync httpx client with .env key loader
  - How to reuse: Run as-is (needs TAOSTATS_API_KEY) to regenerate the cohort; reuse Throttled + fetch_pages + load_api_key for any Taostats pull script. Per-subnet whale net-flow becomes a scoring input.
- `E:\SN_Trader\SubnetTrader\scripts\backfill_whale_events.py` — Resumable per-coldkey backfill with JSON sidecar progress, WAL+busy_timeout sqlite, idempotent INSERT OR IGNORE, applies models.sql
  - How to reuse: Template for all TaoSentinel backfills (GitHub commit history, X timelines): copy the sidecar-progress + overlap + INSERT OR IGNORE pattern.
- `E:\SN_Trader\SubnetTrader\data\whale_cohort.json` — 200-coldkey whale cohort at block 8,590,789 (2026-07-10), with stake/balance/free_fraction and one Taobridge exclusion
  - How to reuse: Seed cohort for whale-flow-per-subnet feature; regenerate periodically (spec says monthly) and keep snapshots append-only in whale_cohort table.
- `E:\SN_Trader\SubnetTrader\data\whale_labels.json` — Empty hand-maintained label map with attribution policy (verified/heuristic/unverified; two-source rule)
  - How to reuse: Adopt the same schema and policy for doxxed-team / entity attribution in TaoSentinel; never assert identity without two independent sources.
- `E:\SN_Trader\SubnetTrader\app\backtest\whale_impact.py` — load_events trade-view filter (is_transfer=0, netuid>0, same-block BUY+SELL move-pair drop, point-in-time cohort gate) plus size-matched event study
  - How to reuse: Lift the move_groups SQL (lines 158-168) into the whale-flow feature so per-subnet whale net-buy excludes hotkey reshuffles.
- `E:\SN_Trader\SubnetTrader\research\subnet_edge\` — 128-subnet catalog (netuid→name), ~25 per-subnet reports with site/GitHub/docs URLs, category summaries, research template
  - How to reuse: Seed TaoSentinel's subnet registry (name, website, github, docs, twitter) from these URLs; reuse the report template structure for the qualitative-notes field; SN13/SN22/SN64 are candidate data/LLM providers for the tracker itself.
- `E:\SN_Trader\SubnetTrader\ops\systemd\subnettrader.service` — Hardened Type=notify unit with WatchdogSec=120, Restart=on-failure, 5-in-60s start limit, graceful SIGTERM, append: file logging
  - How to reuse: Rename to taosentinel.service, change User/paths to the Ubuntu laptop user, keep every directive. Drop Type=notify/WatchdogSec if the app doesn't implement sd_notify on day one.
- `E:\SN_Trader\SubnetTrader\ops\install-systemd.sh` — Idempotent root installer that renders units and enables them
  - How to reuse: Copy and rename; drop the node-detection block if TaoSentinel's UI is served by the Python backend.
- `E:\SN_Trader\SubnetTrader\deploy\auto-update.sh` — Git pull-on-timer deployer with PAT-from-.env, compileall + import smoke gate, /health probe, automatic rollback
  - How to reuse: Change REPO, SERVICE, GIT_URL_BASE; keep the gate + rollback. Pair with the .timer/.service units in the same directory.
- `E:\SN_Trader\SubnetTrader\specs\ops-chain-resilience.md` — Inline sd_notify() implementation, liveness-conditional watchdog loop, substrate circuit breaker design
  - How to reuse: Copy sd_notify + systemd_watchdog_loop verbatim (spec lines 170-227); generalize _exit_watchers_live to check each collector's last_run. Apply the breaker pattern to every external API client (GitHub, X, Discord, Taostats).
- `E:\SN_Trader\SubnetTrader\app\storage\backup.py` — VACUUM INTO + quick_check verified backups, dated names, mirror dir, retention prune
  - How to reuse: Copy with db.py:38-66 backup_to; wire as daily APScheduler job + scripts/backup_db.py runner; set DB_BACKUP_MIRROR_DIR to a USB/NAS path on the laptop.
- `E:\SN_Trader\SubnetTrader\app\notifications\telegram.py` — send_alert + long-polling TelegramBot with inline keyboards, chat allowlist, two-step confirm, setMyCommands
  - How to reuse: Keep the transport class; replace TelegramCommandHandlers fields with TaoSentinel commands (/top, /subnet N, /alerts, /health, /rescore). send_alert is drop-in for score-change and collector-failure alerts.
- `E:\SN_Trader\SubnetTrader\app\logging\logger.py` — StructuredLogger with console + rotating date-stamped JSONL file handler
  - How to reuse: Copy verbatim; only dependency is settings.LOG_LEVEL and settings.JSONL_DIR.
- `E:\SN_Trader\SubnetTrader\requirements.txt` — Version pins for httpx/pydantic/pydantic-settings/APScheduler/aiosqlite/fastapi/uvicorn/pytest
  - How to reuse: Start TaoSentinel's requirements from these ranges; drop bittensor/bittensor-wallet/matplotlib/requests unless chain writes are needed; add GitHub/X/Discord client libs.

## Gotchas

- Taostats free tier is ~5 req/min with bare 429s and no rate headers; every client must self-throttle to >=12s spacing and back off 60s on 429. Per-coldkey polling of 200 wallets is impossible at that rate — use the global feed and filter client-side.
- Taostats address fields arrive as {"ss58": ...} objects, amounts as rao strings, timestamps as ISO-Z; use events.py helpers rather than raw dict access.
- Taostats Authorization header is the bare key (no 'Bearer ' prefix).
- move_stake between a whale's own hotkeys produces UNDELEGATE+DELEGATE pairs on the same (coldkey, netuid, block) with fee 0 and is_transfer NULL (not true) — 240/270 raw 'trades' from the top-2 whales were phantom. Must drop both legs by group; cross-netuid swaps are real trades and must be kept.
- On-chain AutoStakeAdded (emission auto-compound) fires ~152x per 60 blocks with extrinsic_idx=None; filter to ApplyExtrinsic phase if ever scanning the chain directly.
- Coldkey swaps (schedule_coldkey_swap, 5-day delay) move a whale's entire identity to a new address; cohort/labels silently rot unless swap events are ingested.
- Cohort ranking must use aggregated stake, not balance_total — rank-1 by balance (737k tao) is ~100% free exchange custody that never trades alpha.
- Backfilled whale history is selected-as-of-today (lookahead); only forward-collected data after the first cohort snapshot is clean for point-in-time studies.
- The research notes' NONE/LOW/MEDIUM/HIGH verdicts are 'edge for the trading bot', not investment quality — do not import them as health scores. URLs and facts are valid; verdicts must be re-derived.
- Research is dated (mid-2026) and README:78 admits some subnetalpha.ai slugs 404'd and sub-agents were blocked from web fetch; re-verify every URL.
- systemd StandardOutput=append: needs systemd >=240 (fine on Ubuntu 22.04+). Type=notify makes a missing READY=1 fatal after TimeoutStartSec — only set it when sd_notify is implemented.
- WATCHDOG_LIVENESS_MAX_AGE_SEC must stay below WatchdogSec or the service restart-loops on a healthy box.
- A liveness-conditional watchdog restarts a healthy-but-slow process; long collector passes (e.g. GitHub API paging) must not be counted as 'stale' — track last_run per job and pick the max_age per job cadence.
- auto-update.sh does git reset --hard: any tracked data file (old repo's data/fundamentals.json) is reverted on every deploy. Keep all generated data untracked.
- auto-update.sh requires the service user to have NOPASSWD sudo for systemctl restart (sudo -n).
- StructuredLogger signature is logger.info(msg, data=dict) — no printf-style args; existing code in the old repo assumes this.
- Telegram bot discards pending updates on startup and ignores any chat_id other than the configured one — set TELEGRAM_CHAT_ID to your own chat or nothing responds.
- Old repo's Dockerfile targets python:3.11-slim but the operator's boxes ran 3.13/3.14 (both __pycache__ tags present); pick one interpreter for the laptop and pin it.
- The chain fallback source (WHALE_SOURCE='chain') was specced but never built; collector logs an error and no-ops if set.

## Open questions

- Does the operator still have a valid TAOSTATS_API_KEY, and is a paid tier affordable (unlocks /api/dtao/stake_balance/portfolio/v1 precomputed whale P&L and lifts the 5 req/min throttle)?
- Is the Blockmachine keyed archive RPC subscription ($9/mo, SN19) still active? It was the verified 500-day event-archive source for chain-side spot checks.
- Has the old Pi's data/ledger.db (with any backfilled whale_events and pool_history) been preserved? The whale_events table is the only historical whale dataset; the cohort JSON alone is not enough.
- Should TaoSentinel keep the 200-coldkey cohort or rebuild it (2026-07-10 snapshot is ~10 weeks old; spec recommends monthly refresh)?
- Which Ubuntu user/home path will the laptop use — the systemd units, auto-update.sh and Documentation= lines all hardcode /home/pi/Desktop/SN_Bot/SubnetTrader.
- Will the GitHub repo for TaoSentinel be private (needs GITHUB_PAT in .env for auto-update.sh) or public (can drop the PAT logic)?
- Does the operator want the Telegram command bot (needs a BotFather token + chat id) or only one-way send_alert?
- Should SN13 Data Universe / SN22 Desearch be used as the X/Reddit ingestion path instead of a direct X API subscription? The research files have signup steps but no confirmed pricing.
- The old spec's Phase-3 whale signal (WHALE_SIGNAL_MODE off/observe/active + clamped multiplicative factor) was never built — is that the intended shape for feeding TaoSentinel scores back into a revived trading bot?
