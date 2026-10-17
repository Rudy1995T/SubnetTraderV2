# [code] old-bot-runtime-skeleton

**Topic:** old-bot-runtime-skeleton: SubnetTrader service lifecycle, config, config_api, auth/router injection, frontend, ops, tests — reuse verdict for TaoSentinel

## Findings

## 1. app/main.py (3490 lines) — full lifecycle

**Entrypoint.** `E:\SN_Trader\SubnetTrader\app\__main__.py:1-5` → `from app.main import entrypoint; entrypoint()`. `main.py:3484-3486` `entrypoint()` = `asyncio.run(main())`; `main.py:3489-3490` `if __name__ == "__main__": entrypoint()`. Run as `python -u -m app.main` (systemd unit line 27). `main.py:3389-3391`: `sys.argv[1] == "export"` short-circuits to `export_csvs()` (CSV dump, no server).

**main() lifecycle (3385-3481), in order:**
1. 3393-3404: create `_shutdown_event`; `loop.add_signal_handler(SIGTERM/SIGINT)` → sets the event (NotImplementedError swallowed for Windows).
2. 3410 `await init_services()`.
3. 3412-3414 `scheduler = setup_scheduler(); scheduler.start()`.
4. 3416 `health_task = create_task(run_health_server(_shutdown_event))`.
5. 3421 `sd_notify("READY=1")` — sent AFTER scheduler+server start, BEFORE the slow first cycle (so TimeoutStartSec=120 is not blown).
6. 3422 `watchdog_task = create_task(systemd_watchdog_loop(_shutdown_event))`.
7. 3424-3435 Telegram bot task + "online" alert if token+chat set.
8. 3450-3462 prime: `run_ema_cycle()`, each exit watcher, value monitor (so liveness timestamps are fresh from boot).
9. 3464 `await _shutdown_event.wait()`.
10. finally 3469-3481: cancel telegram, watchdog; `wait_for(shield(health_task), 3)` then cancel; `await shutdown_services()`.

**init_services() (190-356) construction order:** log active strategy flags (194-209) → `prune_old_logs()` 211 → `db = Database(); await db.connect()` 213-214 → optional `reconcile_orphans(db)` for test runner 217-222 → `executor = SwapExecutor(); await executor.initialize()` 224-225 → `taostats = TaostatsClient(db=db)` 227 (class is really `app.data.chain_client.ChainClient`, imported under alias at line 23) → pre-warm `taostats._get_subtensor()` + `get_alpha_prices(force=True)` 233-238 (non-fatal) → `regime_filter = RegimeFilter(db, settings)` 240 → four `EmaManager(db, executor, taostats, <cfg>())` gated by MR_ENABLED/EMA_B_ENABLED/FLOW_ENABLED/VALUE_ENABLED 242-268; value manager gets `ema_value._fundamentals = FundamentalsStore.load()` at 263 → companion callbacks 274-301 → `executor.ensure_root_claim_keep()` if any_live 304-311 → startup reconcile 315-321 → guarded startup backup via `asyncio.create_task(run_db_backup())` 326-340 (backgrounded so VACUUM INTO never gates READY=1; see comment 331-335).

**Module globals (39-92):** `db, executor, taostats, ema_meanrev, ema_trend, ema_flow, ema_value, regime_filter, scheduler, telegram_bot, _shutdown_event`, plus per-watcher status dicts `{"last_run","last_error","last_exit_count"}` (50-71), `_last_successful_cycle` 82, `_health_alerted` latches 83.

**setup_scheduler() (1369-1578)** — `AsyncIOScheduler()`, all jobs `trigger="interval"`, `max_instances=1`:
| id | fn | interval | gate | misfire |
|---|---|---|---|---|
| ema_cycle | run_ema_cycle | minutes=SCAN_INTERVAL_MIN | any of MR/EMA_B/FLOW/VALUE_ENABLED | 60 |
| flow_snapshot_prune | prune_pool_snapshots_job | hours=1 | (FLOW_ENABLED & FLOW_PERSIST_SNAPSHOTS) or REGIME_ENABLED | 300 |
| pool_snapshot_freshness | pool_snapshot_freshness_watchdog | minutes=POOL_SNAPSHOT_FRESHNESS_CHECK_MIN | REGIME_ENABLED | 300 |
| meanrev_exit_watch / trend_exit_watch / flow_exit_watch | run_*_exit_watch | seconds=EMA_EXIT_WATCHER_SEC | <strategy>_ENABLED & EMA_EXIT_WATCHER_ENABLED | max(EMA_EXIT_WATCHER_SEC,5) |
| value_monitor | run_value_monitor | minutes=VALUE_MONITOR_INTERVAL_MIN | VALUE_ENABLED & VALUE_MONITOR_ENABLED | 300 |
| health_scan | run_health_scan | minutes=HEALTH_SCAN_INTERVAL_MIN | HEALTH_SCAN_ENABLED & any strategy | 300 |
| meanrev/trend/flow_entry_watch | run_*_entry_watch | seconds=EMA_ENTRY_WATCHER_SEC | <strategy>_ENABLED & EMA_ENTRY_WATCHER_ENABLED | max(sec,5) |
| log_retention | prune_old_logs (sync fn) | hours=24 | always | 3600 |
| test_run_retention | prune_test_runs_job | hours=24 | TEST_RUNNER_ENABLED | 3600 |
| db_backup | run_db_backup | hours=DB_BACKUP_INTERVAL_HOURS | DB_BACKUP_ENABLED | 3600 |
| pool_history_retention | pool_history_retention_job | hours=24, start_date=utc_now()+12h (1531) | POOL_HISTORY_RETENTION_ENABLED | 3600 |
| whale_collector | whale_collector_job | minutes=WHALE_POLL_MINUTES | WHALE_TRACKER_ENABLED | 300 |
| root_claim | run_root_claim | hours=6 | any_live (non-dry-run) 1550-1556 | 300 |
| fundamentals_refresh | refresh_fundamentals | hours=24 | VALUE_ENABLED | 3600 |

Job wrapper pattern (e.g. `whale_collector_job` 1353-1366, `run_db_backup` 1294-1331): early-return on flag, try/except swallowing all, `logger.error(..., data={"error": str(exc)})`, optional `send_alert(...)` in its own try. `run_health_scan` 1099-1145: latch-per-condition pager (`_health_alerted`), pages once on trip and once on recovery.

**sd_notify + watchdog (3308-3382):** `sd_notify(state)` 3308-3320 sends a datagram to `$NOTIFY_SOCKET` (handles abstract `@` sockets), no-op if unset. `_exit_watchers_live(max_age)` 3323-3361: returns True when kill switch exists (3337), otherwise checks each enabled watcher's `last_run` against max-age (warmup `None` passes, 3356-3357); value monitor uses `max(VALUE_MONITOR_LIVENESS_MAX_AGE_SEC, interval*60+120)` 3349-3352. `systemd_watchdog_loop` 3364-3382: returns if `!SYSTEMD_WATCHDOG_ENABLED or !$WATCHDOG_USEC`; pings `WATCHDOG=1` every `WATCHDOG_USEC/1e6/2` seconds only while live; otherwise logs error and withholds (systemd restarts).

**uvicorn signal-ownership override (3232-3296):** `uvicorn.Config(app, host=HEALTH_HOST, port=HEALTH_PORT, log_level="warning")` 3252; warn if `API_AUTH_TOKEN` empty and bind non-loopback 3259-3265; `server.capture_signals = _no_capture` (a no-op contextmanager) 3277-3280 so uvicorn cannot hijack SIGTERM; `hasattr(server,"capture_signals")` guard 3271-3275; `_drive_shutdown` task sets `server.should_exit=True` when the app event fires 3282-3286; driver cancelled and reaped in finally 3291-3296.

**Graceful shutdown `shutdown_services()` (409-447):** arm daemon-thread deadline `_arm_shutdown_deadline(SHUTDOWN_DEADLINE_SEC)` 425 → `sd_notify("STOPPING=1")` 426 → `scheduler.shutdown(wait=False)` 429-430 → `_close_within("DB close", db.close(), SHUTDOWN_DB_TIMEOUT_SEC)` 433-434 (ledger FIRST) → `executor.shutdown()` sync 438-439 → `_close_within("Chain client close", taostats.close(), SHUTDOWN_CHAIN_TIMEOUT_SEC)` 440-443 → `deadline.cancel(); _hard_exit(0)` 446-447. `_hard_exit` 359-368 = `logging.shutdown(); os._exit(code)` (skips interpreter teardown that would join stuck non-daemon threads). `_close_within` 386-406 uses `asyncio.wait` not `wait_for` so it abandons (cancels without awaiting) a slow close.

**create_health_app() (2144-3229):** whole body wrapped in `try: ... except ImportError: return None` (2146/3228). `FastAPI(title=..., docs_url=None, redoc_url=None)` 2165. CORS 2171-2179: origins from `CONTROL_ALLOWED_ORIGINS.split(",")`, `allow_methods=["GET","POST"]`, `allow_headers=["Content-Type","X-Auth-Token"]`, no credentials. Tiering: `config_router` and `test_router` mounted with `dependencies=[Depends(require_auth)]` (2185, 2189); `strategy_router`, `metrics_router`, `backtest_router` mounted open (2215, 2221, 2228); inline `@app.get` reads open; inline `@app.post` mutations each carry `dependencies=[Depends(require_auth)]` (2480, 2486, 2492, 2498, 2505, 2821, 2992). Full inline route list: 2230 /health, 2285 /api/health/services, 2443 /api/control/status, 2480 pause, 2486 resume, 2492 value-resume, 2498 run-ema-cycle, 2505 reset-dry-run, 2521 /api/ema/portfolio, 2617 positions, 2628 recent-trades, 2639 signals, 2821 close, 2848 /api/regime, 2880 stuck-positions, 2889-3004 /api/flow/*, 3017 slippage-stats, 3082 /api/subnets/{netuid}/history, 3108 spot, 3134 /api/price/tao-usd, 3141 /api/export/trades.csv, 3169 /api/prices (SSE).

**GET /health JSON (2230-2283):**
```
{ status: "ok"|"initializing"  (ok iff any manager instantiated),
  timestamp: iso, meanrev_enabled, trend_enabled, flow_enabled, value_enabled: bool,
  value_killed: bool, kill_switch_active: bool, chain_connected: bool,
  fundamentals?: {subnets:int, stale:bool, age_hours:float|null, quality_gate_min:float}   (2244-2252, only when value mgr + store),
  whale?: {**db.whale_stats(), **collector.status()} | {error:str}   (2254-2259, only WHALE_TRACKER_ENABLED),
  meanrev?/trend?/flow?/value?: get_portfolio_summary(...)   (2272-2279, cached prices allow_stale=True),
  summaries_unavailable?: str   (2281, on chain read failure — never 500s) }
```
Deploy gate greps `"status": "ok"` only (auto-update.sh:161).

**GET /api/health/services (2285-2441):** `{timestamp, services: {<key>: {ok:bool, name:str, detail:str, last_check:iso, latency_ms?:int, ...}}}`. Keys: `taostats` (Subtensor block probe with 10s `wait_for`s, breaker note appended 2308-2309), `telegram` (getMe with httpx), `database` (`SELECT COUNT(*) FROM ema_positions`, detail "WAL mode, N trades"), `wallet` (adds `can_trade, balance_tao, pot_tao`). Every probe try/except → `ok:false, detail:str(exc)`.

**/api/prices SSE (3169-3225):** `StreamingResponse(text/event-stream)`, connection cap `PRICE_FEED_MAX_CONNECTIONS` → 503; waits on `taostats._price_updated` Event with `PRICE_FEED_INTERVAL_SEC` timeout; headers `Cache-Control: no-cache, X-Accel-Buffering: no`; relies on CORSMiddleware for ACAO.

## 2. app/config.py (1059 lines)

`Settings(BaseSettings)` 132-682: `model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")` 133-137. **302 UPPER_SNAKE annotated fields** (grep count 132-685), all typed with defaults, grouped by prefix: `MR_*`, `EMA_B_*`, `FLOW_*`, `VALUE_*`, `REGIME_*`, `WHALE_*`, `DB_BACKUP_*`, `SHUTDOWN_*`, `TEST_*`. Comments explain each block and cite spec files. Triads: `<X>_ENABLED: bool` + `<X>_DRY_RUN: bool` (EMA_B 257-258, FLOW 353-354, VALUE 410-411; MR has ENABLED only 192, dry_run hardcoded False in main 346/2463); mode enums as str validated by `@field_validator` (`EMA_POT_MODE` 197, `WHALE_SIGNAL_MODE` 570 'off'|'observe'|'active', `WHALE_SOURCE` 568). Kill-switch sentinels are file paths: `KILL_SWITCH_PATH="./KILL_SWITCH"` 510, `VALUE_KILL_SWITCH_PATH="./VALUE_KILLED"` 447, `WHALE_KILL_SWITCH_PATH="./WHALE_KILLED"` 574, checked via `os.path.exists` / `value_kill.is_killed`. Numeric "0.0 = OFF" sentinels for hard-risk 520-534. Ops fields: `DB_PATH="data/ledger.db"` 492, `JSONL_DIR="data/logs"` 493, `HEALTH_PORT=8081` 494, `HEALTH_HOST="127.0.0.1"` 501, `CONTROL_ALLOWED_ORIGINS` 505, `API_AUTH_TOKEN=""` 509, `LOG_LEVEL` 490, `LOG_RETENTION_DAYS=3` 491, `SYSTEMD_WATCHDOG_ENABLED` 578, `WATCHDOG_LIVENESS_MAX_AGE_SEC=90` 579, `SHUTDOWN_*` 615-617, `SUBTENSOR_FALLBACK_ENABLED` 624, `BLOCKMACHINE_API_KEY` 144, `TAOSTATS_API_KEY` 153 (deprecated for price path, still used by whale collector), `TELEGRAM_BOT_TOKEN/CHAT_ID` 479-480. Validators: `validate_log_level` 637-644, `validate_whale_signal_mode` 646-653, `validate_whale_source` 655-662, `@model_validator(mode="after") _apply_blockmachine_url` 664-670 (rewrites SUBTENSOR_NETWORK to `wss://rpc.blockmachine.io?authorization=<key>` when key set and network is default), method `subtensor_endpoints()` 672-682. Module singleton `settings = Settings()` 685. Factory fns `meanrev_config/strategy_b_config/flow_config/value_config` 688-1059 build the `StrategyConfig` dataclass (16-129). **Hot reload:** only `config_api.py:483` rebinds `app.config.settings = Settings()`; everything else (`main.py:22`, `strategy_api.py:16`, `test_api.py:20`, `logger.py`, `db.py`) does `from app.config import settings` and keeps the stale object — only `api_auth.py:18/30` reads the live module attribute.

## 3. app/config_api.py (952 lines)

`router = APIRouter(prefix="/api/config")` 23; `ENV_PATH = Path(".env")` 25 (cwd-relative). `REQUIRED_FIELDS` 29, `SECRET_PATTERNS=("PASSWORD","TOKEN","API_KEY")` 31 → `_is_secret` 190-191 substring match, `_mask` 194-195 returns "••••••••". `FIELD_TYPES` 33-86 maps field → type tag (`wallet_name, path, str, telegram_token, telegram_chat_id, bool, float_pos, float_range, int_range, int_enum, pot_mode, log_level`); `FIELD_CONSTRAINTS` 88-123 min/max/values; `WALLET_FIELDS` 126 → `full_restart_required`; `ENV_TEMPLATE_ORDER` 129-185 section layout. `_read_env` 198-211 naive `key=val` parser (no quotes/export handling). `_validate_field` 242-354 per type. `_write_env` 366-389: renders sections + "# Other" for unknown keys, `tmp_path = ENV_PATH.with_suffix(".tmp"); write_text; os.replace` (atomic rename, no fsync). `POST ""` 433-494: validate → 422 with `{success:false, errors}`; merge existing + defaults + new; write; if `body.restart` then `import app.config as config_module; config_module.settings = Settings()` 480-486 (in-process rebind, **no process restart**); response `{success, written_fields, restart_triggered, full_restart_required, restart_error?}`. Frontend `settings/page.tsx:277-285` always sends `restart:true` and shows a banner when `full_restart_required`. Other routes: `/status` 395, `/test-telegram` 497, `/test-taostats` 525, `/test-wallet` 553, `/wallet/detect` 624, `/wallet/create` 662 (writes coldkey files), `/go-live` 754, `/wallet/validate` 853. **Drift:** FIELD_TYPES/ENV_TEMPLATE_ORDER still reference removed `EMA_DRY_RUN, EMA_STRATEGY_TAG, EMA_POT_TAO, EMA_MAX_POSITIONS, EMA_STOP_LOSS_PCT, EMA_PERIOD` etc. (0 hits in config.py); only `EMA_MEANREV_WEIGHT/TREND/FLOW_WEIGHT` survive.

## 4. Auth and router injection

`app/api_auth.py:21-36` `async def require_auth(x_auth_token: str|None = Header(default=None))`: `expected = app_config.settings.API_AUTH_TOKEN`; empty → return (auth disabled); else `hmac.compare_digest(x_auth_token.encode(), expected.encode())` else `HTTPException(401, "Missing or invalid auth token")`. Injection pattern (avoids `import app.main` because main runs as `__main__` — see test_api.py:5-10): each router module has module-level `_x: Callable|None` + `def configure(...)` setting globals (`strategy_api.py:22-36` five accessors; `metrics_api.py:21-27` db getter; `test_api.py:28-38`); `create_health_app` passes lambdas over its globals (`main.py:2188, 2205-2214, 2220`). `backtest_api.py` needs none (reads `data/backtest/results` relative to cwd, 30).

## 5. Frontend (Next 14.2.29 app router, React 18, SWR 2.2.5, Tailwind 3.4, lightweight-charts 5.1)

`src/lib/api.ts:10-14` `API_BASE = NEXT_PUBLIC_API_URL || http://${window.location.hostname}:8081`; `authHeaders()` 21-25 adds `X-Auth-Token` from build-time `NEXT_PUBLIC_API_TOKEN`; `fetcher` 29-30. `hooks/usePriceStream.ts` = **SSE** via `EventSource(`${API_BASE}/api/prices`)` 22, JSON parse per message, close on unmount. `app/layout.tsx` 10-19: `<ConditionalNav/>` + `<main className="p-6">`; `ConditionalNav.tsx` hides nav on `/setup`; `NavBar.tsx` 24-71 static link list, polls `/api/control/status` every 30s (`refreshInterval: 30000`), sticky dark nav with live-dot. `app/page.tsx` redirects to `/ema` or `/setup` via `/api/config/status`. `strategy/page.tsx`: `useSWR(url, fetcher, {refreshInterval: 10000})` 42-44; card grid `grid-cols-1 md:grid-cols-2`; `NavChart` 150-214 dynamic-imports lightweight-charts, `createChart` + `addSeries(LineSeries)` + `createPriceLine`, per-card SWR at 30s. `research/page.tsx`: `useSWR({revalidateOnFocus:true})` 157-161, `useMemo` sort/filter 168-192, plain `<table className="w-full text-sm">` with sortable `<SortTh>` 271-299, StatCard grid. Styling: `globals.css:5-7` `body { @apply bg-gray-950 text-gray-100 }` — dark-only, no theme toggle, no `darkMode` key in `tailwind.config.ts` (empty `extend`). `next.config.js` only `experimental.workerThreads=false, cpus=1` (Pi RAM); no `output`, no rewrites. `tsconfig` alias `@/* → ./src/*`.

## 6. Ops

`ops/systemd/subnettrader.service`: `Type=notify, NotifyAccess=main, WatchdogSec=120, TimeoutStartSec=120, Restart=on-failure, RestartSec=10, StartLimitIntervalSec=60/Burst=5, TimeoutStopSec=30, KillSignal=SIGTERM, NoNewPrivileges, ProtectSystem=full, ProtectHome=false, PrivateTmp`; stdout/err `append:` to `data/bot.log`. Hardcoded: L3 Documentation, L22-23 `User=pi/Group=pi`, L24 WorkingDirectory, L27 ExecStart venv path, L30-31 log paths — all `/home/pi/Desktop/SN_Bot/SubnetTrader`. `subnettrader-frontend.service`: L10-11 pi, L12 WorkingDirectory, L17 nvm PATH placeholder `CURRENT` (sed-rewritten by installer L24). `ops/install-systemd.sh`: L15 `sudo -u pi bash -lc 'command -v node'`, L17 message, L24 sed. `deploy/subnettrader-update.service`: L3, L9-11, L14. `deploy/auto-update.sh`: L22 `REPO=`, L25 `GIT_URL_BASE=github.com/Rudy1995T/SubnetTrader.git`; rollback 39-54; PAT read from `.env` `GITHUB_PAT=` 62-72; safety gate 119-141 (`compileall app/` + `python -c 'import app.main'`); restart 146; health poll 154-171 (20×3s, curl `-fsS`, grep `"status":"ok"`). Timer: OnBootSec/OnUnitActiveSec=2min. `install.sh` and `start.sh` are path-relative (no pi refs); `start.sh:18-19` uses lsof/fuser port kills and `nohup`. Dockerfile/docker-compose exist (user `trader`, HEALTHCHECK curl /health) as an alternative.

## 7. Tests

`pytest.ini`: `asyncio_mode = strict`, `testpaths = tests`, markers `smoke`, `ledger`. `tests/conftest.py:8-17`: sys.path insert + `os.environ.setdefault` for `EMA_DRY_RUN, DB_PATH=":memory:", JSONL_DIR=/tmp/..., TAOSTATS_API_KEY="", BT_WALLET_*` before app import — no fixtures (each test constructs `Database(":memory:")` itself). `app/testing/runner.py:26-29` SUITES `smoke: ["-m","smoke"]`, `regression: []`; spawns `python -m pytest tests/ -q -p no:cacheprovider --junitxml=... -o junit_family=legacy` with whitelisted env (`SAFE_PASSTHROUGH` 37-40). `specs/run_qa.py` (~1000 lines) is a live-API QA harness: hits `http://localhost:8081` + `data/ledger.db` + Taostats, TC-1..TC-14, prints PASS/FAIL/SKIP summary, `sys.exit(1 if failed)`. `tests/test_graceful_shutdown.py:25-31` shows how to test the lifecycle: monkeypatch `main._hard_exit` and `main._arm_shutdown_deadline`.

## Reuse table (component → verdict → files)

| Component | Verdict | Copy from |
|---|---|---|
| Process lifecycle (signal handlers, READY/WATCHDOG/STOPPING sd_notify, uvicorn capture_signals override, bounded shutdown, `_hard_exit`) | **Reuse as-is** (extract to `app/runtime.py`) | main.py 359-447, 3232-3296, 3308-3320, 3364-3382, 3385-3486 |
| Liveness predicate `_exit_watchers_live` | **Adapt** (swap watcher status dicts for collector `last_run` dicts) | main.py 3323-3361 |
| APScheduler job registry pattern (interval, max_instances=1, misfire, flag-gated, phased start_date) | **Adapt** (new job ids; keep shape) | main.py 1369-1578 |
| Job wrapper (flag early-return, swallow+log+alert) | **Reuse as-is** | main.py 1294-1366 |
| Latched pager `run_health_scan` | **Adapt** | main.py 1099-1145 |
| `/health` + `/api/health/services` contract | **Adapt** (replace strategy keys with per-collector freshness blocks; keep `status/timestamp/*_enabled/kill_switch_active`, `services.{ok,name,detail,last_check,latency_ms}`) | main.py 2230-2441 |
| FastAPI app factory + scoped CORS + two-tier auth | **Reuse as-is** | main.py 2144-2228 |
| `require_auth` | **Reuse as-is** | api_auth.py |
| Router `configure()` closure injection | **Reuse as-is** | strategy_api.py 22-36, metrics_api.py 21-27, test_api.py 28-38 |
| SSE price stream | **Adapt** (score-update stream) | main.py 3169-3225, usePriceStream.ts |
| `Settings(BaseSettings)` pattern, validators, sentinel paths | **Adapt** (new field set; keep conventions) | config.py 132-137, 490-510, 542-547, 578-579, 603-617, 637-670 |
| `config_api.py` .env editor | **Rewrite** (FIELD_TYPES drifted; keep `_read_env/_write_env/_mask` 190-211, 356-389 and the in-process `Settings()` rebind idea, but fix stale-import problem) | config_api.py |
| `Database` (aiosqlite, WAL, executescript schema + idempotent ALTER tuple), `backup.py` VACUUM INTO | **Adapt** (new models.sql) | storage/db.py 17-120, storage/backup.py, storage/models.sql |
| `StructuredLogger` + JSONL handler + `prune_old_logs` | **Reuse as-is** | logging/logger.py, main.py 1256-1291 |
| `FundamentalsStore` (JSON file, age_hours/is_stale, last-good on empty reload) | **Adapt** as scoring store precedent | data/fundamentals.py, main.py 1334-1350 |
| Telegram bot/alerts | **Reuse as-is** | notifications/telegram.py |
| Frontend shell (api.ts, layout, NavBar, ConditionalNav, globals.css, tailwind/postcss/tsconfig, next.config) | **Reuse as-is** (rename brand, links) | frontend/src/lib/api.ts, app/layout.tsx, components/*, config files |
| Data pages (SWR + table + lightweight-charts) | **Adapt** | strategy/page.tsx, research/page.tsx |
| systemd units + installer | **Adapt** (parameterize user/paths; drop wallet hardening comments) | ops/systemd/*.service, ops/install-systemd.sh |
| auto-update.sh + timer | **Adapt** (REPO, GIT_URL_BASE, SERVICE, health grep) | deploy/* |
| install.sh / start.sh | **Reuse as-is** (path-relative) | install.sh, start.sh |
| pytest.ini / conftest env-seed / runner SUITES | **Reuse as-is** (rename env keys) | pytest.ini, tests/conftest.py, app/testing/runner.py |
| run_qa.py | **Rewrite** (TC list is trading-specific; keep report/skip/db_query scaffold 51-95) | specs/run_qa.py |
| Wallet/executor/EmaManager/strategy code | **Do not port** | app/chain, app/portfolio, app/strategy |

## Reusable assets

- `E:\SN_Trader\SubnetTrader\app\main.py` — Lifecycle core: shutdown_services/_close_within/_hard_exit/_arm_shutdown_deadline (359-447), run_health_server with uvicorn capture_signals override (3232-3296), sd_notify (3308-3320), systemd_watchdog_loop (3364-3382), main()/entrypoint (3385-3490), create_health_app CORS+auth tiering (2144-2228), /health and /api/health/services (2230-2441), setup_scheduler job table (1369-1578), job wrapper style (1294-1366), latched pager (1099-1145), SSE stream (3169-3225)
  - How to reuse: Extract 359-447, 3232-3296, 3308-3382, 3385-3486 verbatim into a new app/runtime.py with the strategy globals replaced by a collectors registry; copy the scheduler add_job shape and job-wrapper idiom; mirror the /health JSON keys (status/timestamp/*_enabled/kill_switch_active + per-subsystem freshness blocks) and the services.{ok,name,detail,last_check,latency_ms} shape so the auto-update health gate and Docker HEALTHCHECK keep working.
- `E:\SN_Trader\SubnetTrader\app\api_auth.py` — 36-line constant-time X-Auth-Token dependency reading the live app.config.settings attribute
  - How to reuse: Copy unchanged; mount mutating routers with dependencies=[Depends(require_auth)] and keep read routers open exactly as main.py:2185-2228 does.
- `E:\SN_Trader\SubnetTrader\app\strategy_api.py` — configure(*accessors) closure-injection pattern (22-36) that lets routers reach __main__ globals without importing app.main; also metrics_api.py 21-27 and test_api.py 28-38
  - How to reuse: Use the same module-level `_getter: Callable|None` + `configure()` template for every TaoSentinel router (scores_api, collectors_api, config_api) and pass lambdas from the app factory.
- `E:\SN_Trader\SubnetTrader\app\config.py` — pydantic-settings Settings class (132-137 model_config; 302 fields; 637-670 validators; sentinel-path kill switches 447/510/574; ops block 490-510, 542-547, 578-579, 603-617)
  - How to reuse: Keep model_config, naming (UPPER_SNAKE, <X>_ENABLED/<X>_MODE/<X>_KILL_SWITCH_PATH), field_validator enum pattern, and module singleton `settings = Settings()`; replace the strategy fields with collector/scoring fields.
- `E:\SN_Trader\SubnetTrader\app\config_api.py` — _read_env/_write_env atomic .env rewriter (198-211, 366-389), secret masking (31, 190-195), validated POST flow (433-494) with in-process Settings() rebind (480-486)
  - How to reuse: Copy the helpers and the POST flow; regenerate FIELD_TYPES/FIELD_CONSTRAINTS/ENV_TEMPLATE_ORDER from the new Settings.model_fields instead of a hand table (the existing table has drifted).
- `E:\SN_Trader\SubnetTrader\app\storage\db.py` — aiosqlite Database wrapper: WAL+foreign_keys connect (25-31), executescript schema + idempotent ALTER migration tuple (82-120), execute/fetchone/fetchall (121-135), backup_to with PRAGMA quick_check (38-80)
  - How to reuse: Copy the class skeleton and migration idiom; author a new models.sql for subnets/sources/signals/scores tables; keep storage/backup.py create_backup/prune_backups/latest_backup_age_hours as-is.
- `E:\SN_Trader\SubnetTrader\app\logging\logger.py` — StructuredLogger with console + daily JSONL handler keyed off settings.JSONL_DIR/LOG_LEVEL; paired with main.py prune_old_logs (1256-1291)
  - How to reuse: Copy unchanged; keep the `logger.info(msg, data={...})` call convention used by every job.
- `E:\SN_Trader\SubnetTrader\app\data\fundamentals.py` — FundamentalsStore: JSON-file store with load(), age_hours(), is_stale(max_age_hours=168), __len__, plus refresh_fundamentals last-good semantics in main.py 1334-1350 and the /health fundamentals block 2244-2252
  - How to reuse: Direct precedent for a per-subnet score store with freshness reporting; generalize to a ScoreStore with the same is_stale/age_hours API and health block.
- `E:\SN_Trader\SubnetTrader\frontend\src\lib\api.ts` — API_BASE derivation + authHeaders + SWR fetcher (30 lines); with hooks/usePriceStream.ts (SSE EventSource), app/layout.tsx, components/NavBar.tsx, ConditionalNav.tsx, globals.css, tailwind.config.ts, postcss.config.js, tsconfig.json, next.config.js
  - How to reuse: Copy the shell wholesale; change brand string and nav links; keep dark-only Tailwind body classes; reuse strategy/page.tsx (SWR 10s + lightweight-charts NavChart 150-214) and research/page.tsx (sortable table 271-299, StatCard grid) as the templates for the score matrix and per-subnet detail pages.
- `E:\SN_Trader\SubnetTrader\ops\systemd\subnettrader.service` — Type=notify unit with WatchdogSec=120, TimeoutStartSec=120, Restart=on-failure, StartLimit 5/60s, TimeoutStopSec=30, hardening block; plus subnettrader-frontend.service and ops/install-systemd.sh
  - How to reuse: Copy and parameterize: User/Group (L22-23), WorkingDirectory (L24), ExecStart (L27), StandardOutput/Error (L30-31), Documentation (L3); frontend unit L10-12,17; installer L15,17,24. Replace `pi` with the laptop user and `/home/pi/Desktop/SN_Bot/SubnetTrader` with the new repo root (or use %h / a templated env).
- `E:\SN_Trader\SubnetTrader\deploy\auto-update.sh` — Git-pull auto-deploy with PAT-from-.env fetch (62-72), compileall+import safety gate (119-141), restart (146), /health poll gate (154-171), rollback (39-54); with subnettrader-update.service/.timer
  - How to reuse: Copy; change REPO (L22), GIT_URL_BASE (L25), SERVICE (L24), unit paths (service L3,9-11,14); keep the `"status":"ok"` health grep contract.
- `E:\SN_Trader\SubnetTrader\tests\conftest.py` — os.environ.setdefault seeding before app import (DB_PATH=:memory:, JSONL_DIR tmp) + pytest.ini asyncio strict/smoke marker + app/testing/runner.py fixed SUITES with env whitelist
  - How to reuse: Copy pytest.ini and conftest.py; rename seeded env keys; reuse runner.py SUITES/_build_child_env if the on-demand test dashboard is kept; use tests/test_graceful_shutdown.py:25-31 monkeypatch idiom for lifecycle tests.

## Gotchas

- Hot-reload is partial: config_api.py:483 rebinds app.config.settings to a fresh Settings(), but main.py:22, strategy_api.py:16, test_api.py:20, logger.py, db.py all did `from app.config import settings` at import and keep the OLD object; only api_auth.py (import app.config as app_config; line 30) sees the new token. A TaoSentinel config editor must either restart the process or make every consumer read `app.config.settings` dynamically.
- config_api.py FIELD_TYPES/ENV_TEMPLATE_ORDER (33-86, 129-185) reference fields that no longer exist in Settings (EMA_DRY_RUN, EMA_STRATEGY_TAG, EMA_POT_TAO, EMA_MAX_POSITIONS, EMA_STOP_LOSS_PCT, EMA_PERIOD...) — the table drifted from config.py; _get_defaults (214-221) silently skips them. Do not copy the table; derive it from Settings.model_fields.
- _write_env (366-389) uses tmp+os.replace but no fsync and writes `.env.tmp` next to `.env`; _read_env (198-211) does not handle quoted values or `export` prefixes. run_qa.py read_env (23-34) is a second, separate parser.
- Everything is cwd-relative: ENV_PATH=Path('.env') (config_api.py:25), DB_PATH='data/ledger.db', JSONL_DIR='data/logs', KILL_SWITCH_PATH='./KILL_SWITCH', backtest RESULTS_DIR (backtest_api.py:30). The service must run with WorkingDirectory=repo root or these silently resolve elsewhere (backtest_api.py:10-13 comment).
- Routers must NOT `import app.main` — the process starts as `python -m app.main`, so main is `__main__`; a second import creates an empty module whose globals are None (test_api.py:5-10). Use the configure() closure pattern.
- READY=1 (main.py:3421) is sent before the first data cycle; anything slow (startup backup VACUUM INTO, 326-340) must be backgrounded or TimeoutStartSec=120 kills the unit — this bit them on 2026-06-29 (comment 331-335).
- Watchdog: WATCHDOG_LIVENESS_MAX_AGE_SEC (90) must stay < WatchdogSec (120) in the unit; the value monitor uses a separate, clamped max-age (3349-3352) or a healthy 15-min job reads as stale and restart-loops. Any new slow collector needs its own max-age, not the 90s window.
- shutdown_services ends in os._exit(0) via _hard_exit (447) and arms a daemon Timer that also calls os._exit — tests must monkeypatch main._hard_exit and main._arm_shutdown_deadline (tests/test_graceful_shutdown.py:25-31) or pytest dies.
- uvicorn.Server.capture_signals is monkeypatched to a no-op (3277-3280); a uvicorn upgrade that renames it silently reintroduces the SIGTERM hijack (30s stop timeout) — the hasattr guard at 3271 only logs.
- Auth is disabled when API_AUTH_TOKEN is empty (api_auth.py:31-32) and the default HEALTH_HOST is 127.0.0.1 (config.py:501); the frontend bakes NEXT_PUBLIC_API_TOKEN in at build time (api.ts:4-8) so token rotation requires `next build`.
- CORS allow_methods is GET/POST only (main.py:2177); adding PUT/DELETE routes requires updating the middleware. SSE route relies on the middleware for ACAO (3217-3219).
- /api/health/services key is literally 'taostats' for the Subtensor probe (2290-2291 comment: kept for frontend compatibility) — rename in the new contract.
- The frontend is dark-only via globals.css body @apply (5-7); tailwind.config.ts has no darkMode setting and empty theme.extend; next.config.js has no output:'standalone', so the systemd frontend unit runs `npm run start` from the source tree with node_modules present.
- All ops files hardcode user `pi` and `/home/pi/Desktop/SN_Bot/SubnetTrader` (subnettrader.service L3,22-24,27,30-31; frontend.service L10-12,17; install-systemd.sh L15,17,24; subnettrader-update.service L3,9-11,14; auto-update.sh L22) and the GitHub repo `Rudy1995T/SubnetTrader.git` (auto-update.sh L25); StandardOutput append: paths require the data/ dir to exist before first start.
- auto-update.sh reads GITHUB_PAT from the same .env the app loads (Settings has extra='ignore' so it is tolerated) — the PAT lives in plaintext next to app secrets; the ProtectSystem=full unit still allows writes to the repo dir.
- tests/conftest.py seeds EMA_DRY_RUN (a removed field) and DB_PATH=':memory:' — Database(':memory:') per test means the schema executescript runs for every Database() instance; there are no shared fixtures to import.
- The .env on disk carries ~150 legacy keys (DRY_RUN, FAST_TRADING_*, MIRROR_*, W_* weights, etc.) from earlier bot generations that Settings ignores via extra='ignore' — don't treat .env as the source of truth for the field list; use config.py.
- specs/feature-sentiment-ingestion.md (61KB, status proposed, 2026-06-19) already designs a Pi-side POST /api/sentiment/score seam assuming the scorer lives on a separate PC that is off at night; TaoSentinel on a 24/7 laptop inverts that assumption, so its fail-safe-neutral TTL design is reusable but the topology section is stale.

## Open questions

- Should TaoSentinel be a separate repo/service (recommended: copy the runtime skeleton, no wallet/executor code) or a new set of jobs inside SubnetTrader behind a flag, given the existing feature-sentiment-ingestion.md seam design expects a separate scorer pushing to the trader?
- Will the Ubuntu laptop run both services (TaoSentinel :8082? + a revived SubnetTrader :8081) — port, CORS origin, and systemd unit names need to be chosen now to avoid collisions with the copied 8081/3000 defaults.
- Does the new service keep the in-browser .env editor (config_api) at all, or replace it with a restart-required, read-only settings view? If kept, decide between process restart (systemctl) vs. the partial in-process rebind.
- Which health probes belong in /api/health/services for TaoSentinel (GitHub API rate-limit, X API, Discord gateway, Taostats, subtensor RPC, DB, Telegram) and what freshness thresholds mark each 'ok:false'?
- Is the SQLite ledger data at E:\SN_Trader\SubnetTrader\data\ledger.db (pool_history, whale_events, fundamentals.json) to be migrated into TaoSentinel's DB, or read via a side-attach? The schema is in app/storage/models.sql (not read here).
- Will the frontend be forked from this Next.js 14 tree (keeping node_modules-in-tree `npm run start`) or rebuilt; if forked, should next.config.js switch to output:'standalone' for a cleaner systemd unit?
- Should the auto-update timer/PAT flow be carried over (implies the new repo on GitHub with a PAT in .env) or replaced by manual `git pull` + `systemctl restart` on the laptop?
