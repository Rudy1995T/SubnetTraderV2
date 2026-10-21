# 01 — Repo skeleton, config, runtime lifecycle, scheduler, /health, logging, alerts, test harness

**Status:** proposed (reconciled to 02 and adversarially reviewed 2026-09-23; see Changelog) · **Phase:** 0 · **Depends on:** 02 (binding ledger; ids A1…J2 cited inline win over this text), 03 (DDL of every table named here, `Database`, `emit_event`), 16 (Phase-0 subset: `app/costs.py`, `app/control.py`, `app/ops/*`, backup pipeline, systemd units), 14 §2.4/§8.5 (routing table and templates applied by 01's Phase-0 `alert_dispatch`, D3) · **Master spec refs:** §1.3 (fail-safe ops), §2 (reuse table), §3 (process model, repo layout), §6 (storage rules), §9 (`/health` shape), §11 (ops), §12 (phase 0)

---

## 1. Purpose and scope

This spec defines the one-process skeleton every other TaoSentinel component plugs into: repository layout, dependency pins, the `Settings` class carrying the **complete v1 config-key inventory** (F1), the process lifecycle ported from SubnetTrader `main.py`, the `JobSpec`/`JOB_TABLE` registry that mirrors every owner spec's jobs (E1–E4), the `CollectorJob`/`LongTask` wrappers that make "never raise into the scheduler" structural, the liveness-conditional systemd watchdog, the `/health` builder (G6), structured JSONL logging, `AlertLatch` + the Phase-0 `alert_dispatch` job (D3), the one HTTP layer `Http` + `ProviderLimiter` (G1), the shared `app/util/time.py` (B1) and `app/util/mathutil.py` (A2), and the pytest harness.

**In scope (Phase 0 deliverable):**
- `pyproject.toml`, `Makefile`, `start.sh`, `.env.example`, `.gitignore`. The systemd units and installer are 16 §2.4 (verbatim there; not restated here).
- `app/config.py` (`Settings`, validators, aliases, dynamic-settings rule, `reload_settings()`), `app/__main__.py` (CLI), `app/runtime.py` (lifecycle), `app/jobs.py` (`JobSpec`, `JOB_TABLE`, `CollectorJob`, `LongTask`, `JobRegistry`, `setup_scheduler`, liveness), `app/ratelimit.py`, `app/http.py`, `app/logging/`, `app/notifications/telegram.py` (`send_alert`) + `alerts.py` (`AlertLatch`), `app/alerts/dispatch.py` (`alert_dispatch`), `app/api/{app.py,health.py,control.py}`, `app/api_auth.py`, `app/util/time.py`, `app/util/mathutil.py`.
- Phase-0 jobs: 01's `log_retention_daily` and `alert_dispatch` (id owned by 14, body by 01 — D3), plus the declared Phase-0 rows of 03 (`storage_retention_daily`) and 16 (`backup_daily`, `ops_guard_5m`, `ops_digest_daily`, `cost_ledger_hourly`) — 7 available jobs; the full v1 `JOB_TABLE` (53 entries) is declared from day one, later phases only add modules (§2.7).
- No DDL: `schema_version`, `collector_runs`, `provider_spend`, `job_state`, `control_state`, `events` are 03 §3.1/§3.6 (B7); §3 lists what 01 reads and writes.
- Tests, verify checklist, the "hello matrix" smoke.

**Out of scope:** collector logic (04–11), scoring (12), the API/Telegram command surface beyond the Phase-0 routes (14), cost-governor internals, backup/restore, ops jobs, pause-group membership and systemd units (16), the frontend (Phase 11).

---

## 2. Design

### 2.1 Process model

```
python -m app serve
 └─ app/__main__.py ─► app.runtime.entrypoint(argv) ─► asyncio.run(main(args))
      main(args):
        signal handlers (SIGTERM/SIGINT → shutdown_event)        loop.add_signal_handler, as old main.py 3396-3404
        rt = Runtime(started_at=now_ts(), shutdown_event)
        init_services(rt):  16 pre_init (instance lock, boot classification, heartbeat) FIRST
                            · prune_old_logs · Database.connect + apply_schema (= 03 migrate)
                            · ProviderLimiter.from_settings · Http · CostGovernor.load (seeds limiter months)
                            · AlertLatch · control.refresh_cache(db) · JobRegistry.build(rt)
                            · background: 16 post_boot_check (unclean boots), run_startup_backup (delayed)
        scheduler = setup_scheduler(rt); scheduler.start()          (stagger, startup catch-up, manual jobs paused)
        health_task = create_task(run_health_server(shutdown_event)); rt.task_handles["health"] = health_task
        sd_notify("READY=1")                                        ← nothing slow, no job body and no alert before it
        watchdog_task = create_task(systemd_watchdog_loop(shutdown_event))
        per LongTask: rt.task_handles[id] = create_task(task.run_forever(shutdown_event))   (chain_sse unstaggered, 16 boot step 6)
        16 write_heartbeat · AlertLatch.fire("service_online", …) (direct, not stored) · 16 recovery.report
        await shutdown_event.wait()
      except/finally: cancel watchdog · drain uvicorn ≤ 3 s · shutdown_services(rt) → _hard_exit(0)
```

One asyncio loop, one SQLite file (WAL), one `AsyncIOScheduler(timezone="UTC")`. Jobs are `CollectorJob` instances run by APScheduler `interval`/`cron`/`manual` triggers with `max_instances=1, coalesce=True`; long-lived coroutines (`chain_sse` 05, `discord_gateway` 08, `telegram_bot` 14) are `LongTask`s (E1). Every outbound HTTP call goes through `app.http.Http.request(provider, …)` on the process-wide `ProviderLimiter` (G1).

**READY ordering (binding).** `READY=1` is sent after the scheduler has started and the health task exists, and before any job body, long task, alert or backup runs (old `main.py` 3412-3422: scheduler start → health task → `READY=1` "before the (slow) first cycle"; the 2026-06-29 incident where a boot-time `VACUUM INTO` blew `TimeoutStartSec`). Interval jobs first fire at `boot + 30 s` or later (§2.7), startup catch-up at `boot + STARTUP_CATCHUP_DELAY_SEC` (300 s), the startup backup at `boot + DB_BACKUP_STARTUP_DELAY_SEC` (600 s, 16 §2.6 step 9).

Because the entrypoint is `app/__main__.py`, `app.runtime` is an ordinary importable module — the old bot's `__main__` double-import trap (`app/test_api.py:5-10`) disappears. Routers still receive runtime accessors through `configure()` closures (old `app/strategy_api.py:22-36` pattern) so they are testable without a live runtime.

### 2.2 Repository layout (master §3, refined by the owner specs)

```
~/taosentinel/
  pyproject.toml  requirements.lock  Makefile  start.sh  .env.example  .gitignore  README.md
  app/
    __init__.py  __main__.py  runtime.py  config.py  api_auth.py  jobs.py  ratelimit.py  http.py
    costs.py  control.py                                            (16; Phase 0)
    util/        __init__.py  time.py  mathutil.py  paths.py         (01; B1, A2)
    logging/     __init__.py  logger.py  retention.py
    alerts/      __init__.py  dispatch.py (01)  routing.py  templates.py (14 data, shipped in Phase 0 — D3)
    storage/     __init__.py  db.py  migrate.py  models.sql  migrations/  pit.py  events.py  retention.py  backup.py  (03; backup.py 16)
    ops/         __init__.py  recovery.py  guard.py  digest.py  security.py  cli.py        (16)
    registry/    (04)   collectors/ (05–09)   features/ (05 chain_features, 12)   integrity/ (11)
    nlp/         (10)   scoring/ (12)   quant/ (13)   backfill/ (17)
    api/         __init__.py  app.py  health.py  control.py  (Phase 0; 14 adds the rest)
    notifications/ __init__.py  telegram.py  alerts.py  (01)  bot.py watch.py digest.py format.py commands.py (14)
  scripts/       (17 backfills, 09 build_kol_list.py, 04 seed_registry.py, 16 with_bucket.py — later phases)
  ops/  deploy/  (16)      frontend/ (Phase 11)      tests/  conftest.py  test_*.py
  seed/          (operator-provided CSV/JSON; gitignored except *.example)
  data/          (gitignored: taosentinel.db, logs/, backups/, run/, control/, quant/, cache/, tmp/)
  specs/  research/   (copied from E:\TaoData)
```

Every package listed exists in Phase 0 with an `__init__.py` (empty where the phase is later) so a `JOB_TABLE` factory of a later phase fails with `ModuleNotFoundError` on the *module*, not the package (§2.7). File-level ownership inside later packages is the owner spec's.

### 2.3 `pyproject.toml`

```toml
[build-system]
requires = ["setuptools>=69"]
build-backend = "setuptools.build_meta"

[project]
name = "taosentinel"
version = "0.1.0"
requires-python = ">=3.12,<3.13"
dependencies = [
  "httpx>=0.27,<1.0",
  "pydantic>=2.7,<3.0",
  "pydantic-settings>=2.3,<3.0",
  "APScheduler>=3.10,<4.0",
  "aiosqlite>=0.20,<1.0",
  "fastapi>=0.111,<1.0",
  "uvicorn[standard]>=0.30,<1.0",
  "feedparser>=6.0,<7.0",
  "trafilatura>=2.0,<3.0",
  "python-dateutil>=2.9,<3.0",
  "discord.py>=2.4,<3",            # 08 discord_gateway (Phase 6); imported lazily by that module only
]

[project.optional-dependencies]
dev = ["pytest>=8.2,<9.0", "pytest-asyncio>=0.23,<2.0", "respx>=0.21,<1.0"]
backfill = ["async-substrate-interface>=2.2,<3", "xxhash>=3"]      # 17 §8.1; imported lazily by app/backfill/rpc_client.py

[tool.setuptools.packages.find]
include = ["app*", "scripts*"]

[tool.pytest.ini_options]
asyncio_mode = "strict"
testpaths = ["tests"]
markers = [
  "smoke: boots the runtime in-process; fast critical path (< 45 s)",
  "network: touches the real internet (skipped unless TAOSENTINEL_NETWORK_TESTS=1)",
]
```

Rules: **no `bittensor`, no `bittensor-wallet`, no `substrate-interface`** anywhere in the dependency tree, with or without `[backfill]` (17 verified that `async-substrate-interface` 2.2.x lists `bittensor-wallet` only under its own `dev` extra; do not add `scalecodec` — `cyscale` ships it). `tests/test_deps.py` asserts `importlib.util.find_spec(m) is None` for `bittensor`, `bittensor_wallet`, `substrateinterface`, and CI runs it a second time after `pip install -e ".[dev,backfill]"`. After the first install on the laptop, `pip freeze > requirements.lock` is committed; `make install` uses the lock. Later phases add `anthropic` (10) and, if needed, `httpx-sse` (05) by editing this file under their own spec. 05's `SUBTENSOR_RPC_MODE=sdk` would need `bittensor` and is therefore unsupported in v1 (validator rejects it, §5).

### 2.4 `Settings` and the dynamic-settings rule

`app/config.py` keeps the old conventions (`config.py:132-137`): `SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")`, UPPER_SNAKE fields, `<X>_ENABLED` booleans, `<X>_MODE`/enum strings validated by `@field_validator` (old `validate_log_level`, `config.py:637-644`), the three sentinel **paths** of B12, module singleton `settings = Settings()` (`config.py:685`). Additions:

```python
# app/config.py
import os
from pathlib import Path
from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

APP_ROOT: Path = Path(os.environ.get("TAOSENTINEL_ROOT") or Path(__file__).resolve().parents[1])

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=os.environ.get("TAOSENTINEL_ENV_FILE", str(APP_ROOT / ".env")),
        env_file_encoding="utf-8", extra="ignore",
    )
    ...  # full inventory in §5; aliases via Field(validation_alias=AliasChoices(...)):
    # HEALTH_HOST ← API_HOST, HEALTH_PORT ← API_PORT (14/15 spelling), GITHUB_TOKEN ← GITHUB_PAT,
    # TAOSTATS_MONTHLY_CAP_REQ ← TAOSTATS_MONTHLY_CALL_CAP (deprecated, warning logged when used) — F2

    @property
    def SCORING_SPEC_VERSION(self) -> str: return self.SPEC_VERSION      # read-only alias (F2); an env/.env value
                                                                         # of that name is ignored, warning if it differs
    # derived helpers (not env keys)
    def path(self, rel: str | Path) -> Path      # relative → APP_ROOT/rel; absolute unchanged
    def x_active(self) -> bool                    # X_ENABLED and X_PROVIDER != "null" and the provider's key present
                                                  #   and X_MONTHLY_CAP_USD > 0 (cap 0 = provider disabled, F2)
    def llm_active(self) -> bool                  # LLM_ENABLED and ANTHROPIC_API_KEY and LLM_MONTHLY_CAP_USD > 0
    def taostats_active(self) -> bool             # TAOSTATS_ENABLED and TAOSTATS_API_KEY and TAOSTATS_MONTHLY_CAP_REQ > 0
    def public_dict(self) -> dict                 # every field; *_TOKEN, *_KEY, *_SECRET, *_PAT and DEADMAN_PING_URL masked "••••••••"

settings: Settings = Settings()

def get_settings() -> Settings:
    return settings

def reload_settings() -> tuple[Settings, list[str]]:
    """Rebind the module attribute to a fresh Settings(); return (new, changed_keys)."""
    global settings
    old, new = settings, Settings()
    changed = [k for k in Settings.model_fields if getattr(old, k) != getattr(new, k)]
    settings = new
    from app.logging.logger import logger; logger.set_level(new.LOG_LEVEL)
    from app.runtime import runtime
    if runtime is not None and runtime.limiter is not None: runtime.limiter.reload()   # rates/caps apply without restart
    return new, changed
```

**Dynamic-settings rule (binding for every module in `app/`, F1):** read configuration through the module attribute at call time — `import app.config as app_config; app_config.settings.FOO` — or via `get_settings().FOO`. **`from app.config import settings` is forbidden** and is rejected by `tests/test_config.py::test_no_static_settings_import` (regex scan of `app/**/*.py`). Reason: in the old bot `config_api.py:483` rebound `app.config.settings = Settings()` on a hot reload, but `main.py:22`, `strategy_api.py:16`, `test_api.py:20`, `logging/logger.py:14`, `notifications/telegram.py:15` and many `backtest/*` modules had captured the *old* object at import and kept enforcing stale values; only `api_auth.py:18/30` (which imports the module) saw the new token. `POST /api/v1/control/reload-config` (authed) calls `reload_settings()`; jobs re-read their enabled flag(s), keys and caps on every run, so a flag can be turned off (or a key rotated, a cap raised) without a restart. Changing a trigger (interval/cron key), `HEALTH_HOST`/`HEALTH_PORT` or a `*_PATH` requires a restart (`restart_required: true` in the response, with the offending keys).

**Validation policy.** Every validator listed in an owner's §5 table is implemented in `app/config.py` (field or model validators). A malformed value raises at import → boot fails loudly (§7). Fail-safe (not fail-boot) downgrades, each logged once at `warning`: an enabled feature whose credential is missing is forced off (`DISCORD_GATEWAY_ENABLED`, `DEADMAN_ENABLED`, `WEB_SPN2_ENABLED`, `X_*` via `x_active()`, `LLM_*` via `llm_active()`); `GITHUB_RATE_PER_HOUR` is clamped for a missing PAT (06); `ONFINALITY_RATE_PER_SEC` is clamped to 4.0 without a key.

Path resolution: every path-typed key (`DB_PATH`, `JSONL_DIR`, `DATA_DIR`, `SEED_DIR`, `DB_BACKUP_DIR`, `KILL_SWITCH_PATH`, `PAUSE_ALL_PATH`, `SCORING_KILL_SWITCH_PATH`, `CONTROL_DIR`, `RUN_DIR`, `BACKFILL_DIR`, `QUANT_RESULTS_DIR`, `COVERAGE_PATH`, and every `*_SEED_PATH`/`*_DIR` of the owner tables) is resolved with `settings.path()` against `APP_ROOT`, never against the cwd (the old bot's cwd-relative trap, research 02 §Gotchas).

### 2.5 Runtime container

```python
# app/runtime.py
@dataclass
class Runtime:
    started_at: int                                                     # unix s (B1); liveness warm-up clock (16 §2.5)
    shutdown_event: asyncio.Event
    db: "Database | None" = None
    scheduler: "AsyncIOScheduler | None" = None
    limiter: "ProviderLimiter | None" = None
    http: "Http | None" = None
    costs: "CostGovernor | None" = None                                 # 16 app/costs.py
    alerts: "AlertLatch | None" = None
    boot: "BootInfo | None" = None                                      # 16 pre_init result
    jobs: dict[str, "CollectorJob"] = field(default_factory=dict)      # built jobs by id
    tasks: dict[str, "LongTask"] = field(default_factory=dict)         # built LongTasks by id
    unavailable: dict[str, str] = field(default_factory=dict)          # declared id → import error text
    task_handles: dict[str, asyncio.Task] = field(default_factory=dict) # "health" + LongTask ids
    api_probe_fails: int = 0                                            # 16 §2.5 amendment 4
    version: str = "0.1.0+unknown"                                      # + short git sha when available

runtime: Runtime | None = None
def get_runtime() -> Runtime        # raises RuntimeError before main() assigned it
```

Functions ported from `E:\SN_Trader\SubnetTrader\app\main.py` (only symbol renames: `settings.` → `app_config.settings.`, `_exit_watchers_live` → `_jobs_live`, module globals → `rt.*`; line ranges verified 2026-09-23):

| function | old lines | change |
|---|---|---|
| `_hard_exit(code=0)` | 359-368 | none (`logging.shutdown(); os._exit(code)`) |
| `_arm_shutdown_deadline(seconds)` | 371-383 | none (daemon `threading.Timer` backstop) |
| `_close_within(name, coro, timeout)` | 386-406 | none (`asyncio.wait`, abandons a slow close) |
| `shutdown_services(rt)` | 409-447 | order: deadline(`SHUTDOWN_DEADLINE_SEC`) → `sd_notify("STOPPING=1")` → `scheduler.shutdown(wait=False)` → cancel LongTask handles, `asyncio.wait(…, timeout=1)` → `_close_within("cost flush", rt.costs.flush(), 1.0)` (needs the DB, so before it) → `_close_within("DB close", rt.db.close(), SHUTDOWN_DB_TIMEOUT_SEC)` → `_close_within("HTTP close", rt.http.aclose(), SHUTDOWN_HTTP_TIMEOUT_SEC)` → 16 `recovery.remove_heartbeat()` (marks a clean stop) → log `Services shut down cleanly (Xs)` → `deadline.cancel(); _hard_exit(0)`. Worst case 1 + 1 + 5 + 3 = 10 s = the deadline (validator `DB + HTTP + 2 ≤ DEADLINE < 30`) |
| `run_health_server(stop)` | 3232-3296 | `create_app()` from `app/api/app.py`; keeps the `capture_signals` no-op override (3277-3280), the `hasattr(server, "capture_signals")` guard (3271-3275), the non-loopback-without-token warning (3258-3265) and the `_drive_shutdown` driver + reap (3282-3296) |
| `sd_notify(state)` | 3308-3320 | none |
| `_jobs_live()` | 3323-3361 (adapted) | §2.8 (16 §2.5 rules) |
| `systemd_watchdog_loop(stop)` | 3364-3382 | calls `_jobs_live()`; adds 16's API self-probe per tick; logs `watchdog_withheld` with the stale ids |
| `main()` / `entrypoint()` | 3385-3486 | steps in §2.1; argv parsed by `app/__main__.py` (argparse) instead of the `export` short-circuit (3389-3391); the old inline first cycle (3450-3463) has no equivalent — jobs start from the scheduler |

`init_services(rt)` (new, replaces 190-356): 16 `pre_init(rt)` (instance lock `data/run/taosentinel.lock`, boot classification, heartbeat; `sys.exit(3)` when another instance holds the lock) → `prune_old_logs()` → `db = Database(settings.path(DB_PATH)); await db.connect(); await db.apply_schema()` (03 §2.3: `= migrate()`; a `MigrationError`/`SchemaError` fails the boot) → `limiter = ProviderLimiter.from_settings()` → `http = Http(limiter, costs=None)` → `costs = CostGovernor(db, limiter=limiter, alerts=None); await costs.load()` (seeds `limiter.seed_month`) → `http.costs = costs` → `alerts = AlertLatch(db, dispatcher_active=lambda: dispatcher_active(rt))`; `costs.alerts = alerts` → `await control.refresh_cache(db)` (16) → `JobRegistry.build(rt)` → background tasks: 16 `post_boot_check(rt, rt.boot)` (unclean boots only) and `run_startup_backup(rt)`. `run_startup_backup` sleeps `DB_BACKUP_STARTUP_DELAY_SEC` (cancellable), then runs `rt.jobs["backup_daily"].run(trigger="startup")` through the wrapper only if the newest backup is older than `DB_BACKUP_MIN_AGE_HOURS` (old `main.py` 323-340 guard) and 16's integrity check (if scheduled) returned `ok`; it never gates `READY=1` (the 2026-06-29 incident).

### 2.6 `CollectorJob`, `LongTask` and the run wrapper

```python
# app/jobs.py
@dataclass(slots=True)
class JobOutcome:
    rows: int = 0
    cost_usd: float = 0.0
    note: str | None = None
    meta: dict | None = None             # → collector_runs.meta_json (03)

@dataclass(slots=True)
class RunContext:
    run_id: str                # uuid4().hex — stamp it on every raw row written in this run
    started_at: int            # unix s (B1); default observed_at for rows fetched in this run; 12's T0, 11's decision_time
    trigger: str               # "schedule" | "catchup" | "manual" | "startup" | "cli"
    rt: "Runtime"              # db, http, limiter, costs, alerts
    settings: "Settings"       # snapshot taken at run start (read app_config.settings for live values)
    log: "StructuredLogger"
    rows_committed: int = 0    # a job may bump it as it commits; used when collect() raises BudgetExhausted
    @property
    def started_dt(self) -> datetime     # parse_dt(started_at): for C5 producer fns that take a datetime

class CollectorJob(ABC):
    spec: "JobSpec"                                   # the JOB_TABLE row — the single source of scheduling/policy data
    # Owner specs may also declare class attributes (id, phase, enabled_flag, timeout_sec, …) for readability;
    # JobRegistry.build raises JobSpecMismatch at boot if any differs from the JobSpec (test_job_table_matches_classes).
    def __init__(self, rt: "Runtime", spec: "JobSpec") -> None
    @property
    def id(self) -> str                               # spec.id
    def enabled(self) -> bool                         # every spec.enabled_flags attr true on app_config.settings, and
                                                      #   spec.enabled_fn(settings) if set (e.g. 09's x_active())
    def interval_min(self) -> float                   # interval: the key's value in minutes; cron: the largest gap among
                                                      #   the next 8 fire times (16 §2.5 amendment 1); manual: inf
    def effective_liveness_max_age(self) -> int | None   # spec.liveness_max_age_sec or interval_min*60*2 + timeout + 120;
                                                         #   None for manual
    def paused_reason(self) -> str | None             # app.control.effective_pause(self, rt) (16 §2.11): pause_all |
                                                      #   kill_switch | emergency_file | operator | budget | None (B12)
    def fail_alert_threshold(self) -> int             # settings[spec.fail_alert_key] if set, else spec.fail_alert_threshold,
                                                      #   else JOB_FAIL_ALERT_THRESHOLD (5)
    @abstractmethod
    async def collect(self, run: RunContext) -> JobOutcome
    async def run(self, trigger: str = "schedule") -> None   # the wrapper; NEVER raises (except CancelledError)
    def status(self) -> dict                          # /health.collectors.<id> block (§2.9)
    async def get_state(self, key: str, default=None) # job_state(job=id, key) — runtime watermarks only (B12)
    async def set_state(self, key: str, value) -> None
```

`run()` algorithm (port of the `run_db_backup` 1294-1331 / `whale_collector_job` 1353-1366 never-raise idiom, made generic):

1. `st = self._status` (in-memory; timestamps are unix ints, ISO only in `status()`). If a run of this job is already in flight: `st.last_skip_reason="in_flight"`; return (guards manual, catch-up and API runs against the scheduled one).
2. If `not self.enabled()`: `st.last_skip_reason="disabled"`; return (a disabled flag is not a pause, 16 §2.11).
3. `reason = self.paused_reason()`; if set: `st.paused = reason`, `st.last_skip_reason=f"paused:{reason}"`; return. No `collector_runs` row is written, so nothing downstream reads a pause as absence. `ops_exempt` jobs are never paused by `pause_all`, `kill_switch` or a `PAUSE_GROUPS` target, only by an exact-id pause (row or emergency file) — 16 implements it. `budget` checks only the job's primary provider (`spec.providers[0]`): the limiter's hourly/monthly window (`limiter.exhausted(p)`) or the governor group (`costs.is_exhausted(p)`); an exhausted secondary provider raises `BudgetExhausted` inside the run and the job handles it (e.g. 11's TaoStats sub-step).
4. `interval` jobs, `trigger == "schedule"` only: if `job_state.last_ok` is younger than `interval_min*60*spec.min_gap_factor`: `st.last_skip_reason="recent"`; return (restart loops never re-pull daily budgets).
5. `run_id = uuid4().hex; t0 = monotonic()`; INSERT `collector_runs(run_id, job, started_at=now_ts())`.
6. `try: outcome = await asyncio.wait_for(self.collect(ctx), timeout=spec.timeout_sec)`
7. `except asyncio.CancelledError: raise` (shutdown must propagate — never swallow cancellation; the row stays an orphan, which 16 counts).
8. `except BudgetExhausted as exc:` (G1) → not a failure: UPDATE the row `ok=1, rows=ctx.rows_committed, note=f"paused:budget {exc.provider} {exc.window} until {to_iso(exc.until)}"`; `consecutive_failures` unchanged; later runs skip `paused:budget` (step 3) until the window resets.
9. `except Exception as exc:` (incl. `asyncio.TimeoutError` → `error="timeout after Ns"`, and `ProviderError`) → `st.last_error=f"{type(exc).__name__}: {exc}"[:500]`, `st.consecutive_failures += 1` (the first failure of a streak sets `st.first_failure_at = now`), `logger.error(f"{id} failed", data={...})`, UPDATE the row `ok=0, error, duration_sec`. When `st.consecutive_failures == self.fail_alert_threshold()`: `await rt.alerts.fire(f"collector_failing:{id}", text, event_type="collector_failing", severity="warning", payload={"collector": id, "consecutive_failures": n, "last_error": err, "job": id}, dedupe_key=f"{id}|{first_failure_at}", source="runtime")` (03 §3.9 required keys; D1/D3).
10. `else:` `st.last_ok = now`, `st.last_rows = outcome.rows`, UPDATE the row `ok=1, rows, note, meta_json, duration_sec, cost_usd`; `set_state("last_ok", now)`. Cost reconciliation (16 §2.10): `extra = outcome.cost_usd − rt.costs.run_total(run_id)`; if `extra > 1e-9`: `await rt.costs.record(spec.providers[0], extra, requests=0, job=id, run_id=run_id)`; `collector_runs.cost_usd = max(outcome.cost_usd, run_total)`. If the failing latch was set: `await rt.alerts.clear(f"collector_failing:{id}", f"✅ {id} recovered", event_type="collector_recovered", severity="info", payload={"collector": id, "failures": n_before, "down_seconds": now − first_failure_at, "job": id}, dedupe_key=f"{id}|{first_failure_at}")` (D2); then `consecutive_failures = 0`.
11. `finally:` `st.last_run = now_ts(); st.last_duration_sec = round(monotonic()-t0, 3)`. Every DB write in the wrapper is itself inside `try/except Exception: logger.warning(...)`, so a locked DB cannot turn a wrapper into a raise.

`last_run` = last completed *attempt* (ok, failed or budget-ended); it feeds liveness (§2.8): a failing API is not a stall. A hang is bounded by `timeout_sec`, so liveness trips only for a wedged event loop or a scheduler that stopped firing.

`LongTask` (`chain_sse` 05, `discord_gateway` 08, `telegram_bot` 14 — E1):

```python
class LongTask(ABC):
    spec: "JobSpec"                                  # kind="longtask"; liveness_max_age_sec None = not liveness-gated
    backoff_min_sec: float = 5.0; backoff_max_sec: float = 300.0    # owners override (05: 1 → 60 s via TMC_SSE_BACKOFF_*)
    last_beat: int | None; last_attempt: int | None; connected: bool; reconnects: int; last_error: str | None
    def __init__(self, rt: "Runtime", spec: "JobSpec") -> None
    def enabled(self) -> bool                        # as CollectorJob
    def paused_reason(self) -> str | None            # app.control.effective_pause(self, rt) (16 §2.5 amendment 2)
    @abstractmethod
    async def run_once(self, stop: asyncio.Event) -> None   # one connection lifetime; returns on disconnect
    def beat(self) -> None                           # every received event / successful fallback poll → last_beat
    def attempt(self) -> None                        # every REST-fallback poll attempt → last_attempt (amendment 5)
    async def run_forever(self, stop: asyncio.Event) -> None
    def status(self) -> dict  # {enabled, available, paused, connected, last_beat, last_attempt, reconnects, last_error, fatal_error?}
```

`run_forever` (supervisor; never raises; `CancelledError` propagates): `while not stop.is_set()`: if `not enabled()` or `paused_reason()`: record it in `status()`, `connected=False`, wait 15 s (or stop) and loop; else start `run_once(stop)` as a child task and poll every 15 s — if the task becomes disabled or paused, cancel the child (05: the recorder drops the stream within one 15-s tick; 08's 30-s beat is also met). When `run_once` returns or raises: `last_attempt = now`, log (exception → `last_error`), `reconnects += 1`, sleep the backoff (×2 per consecutive failure with ±20 % jitter, capped at `backoff_max_sec`, reset to `backoff_min_sec` after ≥ 60 s healthy). A LongTask that pages uses `rt.alerts.fire("collector_failing:<id>", …, event_type="collector_failing")` itself (08's fatal login errors); 05 emits its own `chain_sse_down`.

### 2.7 `JobSpec`, `JOB_TABLE` and the scheduler (E1–E4)

```python
# app/jobs.py
TriggerFn = Callable[["Settings"], "BaseTrigger"]      # built from live Settings at setup (UTC); restart to change

@dataclass(frozen=True, slots=True)
class JobSpec:
    id: str                                  # the owner's id, verbatim (E2)
    owner: str                               # "03" | "04" | … — the spec whose job table this row mirrors
    factory: str                             # "module:Class" under app.; CollectorJob or LongTask subclass
    kind: Literal["interval", "cron", "manual", "longtask"]                        # E1
    phase: int
    trigger: TriggerFn | None = None         # interval/cron only; manual → IntervalTrigger(weeks=520), longtask → None
    enabled_flags: tuple[str, ...] = ()      # Settings attrs, all must be true; () = always enabled
    enabled_fn: Callable[["Settings"], bool] | None = None      # e.g. 09's Settings.x_active
    misfire_grace_sec: int | None = 300
    timeout_sec: int | None = 1800           # JOB_DEFAULT_TIMEOUT_SEC when the owner names none; None for longtask
    providers: tuple[str, ...] = ()          # G1 ids; [0] is the primary (budget pause, cost reconciliation)
    paid: bool = False                       # KILL_SWITCH pauses paid jobs (B12)
    ops_exempt: bool = False                 # never paused by KILL_SWITCH / PAUSE_ALL / a PAUSE_GROUPS target (E1)
    min_gap_factor: float = 0.5              # 0 for cron and manual
    startup_catchup: bool = False            # E1 (amended): run once at boot if the last scheduled fire was missed
    catchup_avoid_utc: tuple[str, ...] = ()  # "HH:MM-HH:MM" windows; a catch-up landing inside moves to the window end (05, 06)
    catchup_only_utc: str | None = None      # catch-up only when the catch-up instant falls inside this window (11)
    first_run: Literal["stagger", "ops"] | int = "stagger"     # interval jobs: stagger slot, ops (+30 s) or a fixed delay s
    jitter_sec: int | None = None            # APScheduler trigger jitter
    liveness_max_age_sec: int | None = None  # owner-declared value wins (E4); None → formula (§2.8)
    fail_alert_threshold: int | None = None  # per-job collector_failing threshold (≥ 1); None → JOB_FAIL_ALERT_THRESHOLD (5)
    fail_alert_key: str | None = None        # Settings key holding it (13's QUANT_ALERT_AFTER_FAILURES); wins when set
    timeout_key: str | None = None           # Settings key overriding timeout_sec at run time (13's QUANT_WEEKLY_JOB_TIMEOUT_S)

# trigger helpers (all timezone=UTC; the Settings key is read when setup_scheduler runs)
def every(key: str, unit: Literal["sec", "min", "hour"]) -> TriggerFn            # IntervalTrigger(**{unit: s[key]})
def daily_at(key: str) -> TriggerFn                                             # "HH:MM" → CronTrigger(hour, minute)
def hours_at(hours_key: str | str, minute: int | str) -> TriggerFn             # "0,6,12,18" + int or minute key
def weekly_at(key: str) -> TriggerFn                                            # "sun 05:00" → CronTrigger(day_of_week, hour, minute)
def cron(**fields: int | str) -> TriggerFn                                      # literal fields; a str starting "$" names a key

JOB_TABLE: tuple[JobSpec, ...]                  # the rows below, in this order (the order drives the stagger)
class JobSpecMismatch(RuntimeError): ...
class JobRegistry:
    @staticmethod
    def build(rt: "Runtime") -> None            # fills rt.jobs / rt.tasks / rt.unavailable
def setup_scheduler(rt: "Runtime") -> "AsyncIOScheduler"
def previous_fire_time(trigger: "BaseTrigger", now: datetime) -> datetime | None     # steps get_next_fire_time from now − 35 d
```

**`JobRegistry.build(rt)`** imports each factory; `ModuleNotFoundError`/`ImportError`/`AttributeError` → `rt.unavailable[id] = str(exc)` (logged once at INFO as `job unavailable (phase N)`); anything else — including 12's `RegistryError`/`SpecHashMismatch` and `JobSpecMismatch` — raises at boot (a broken module must not boot silently). A duplicate id, or a factory that is not a `CollectorJob` for kinds interval/cron/manual (`LongTask` for longtask), is a `JobSpecMismatch`.

**`setup_scheduler(rt)`** registers every built job **regardless of its flag** (the wrapper re-checks the flags each run, so a flag flipped via reload takes effect without restart) with `max_instances=1, coalesce=True, misfire_grace_time=spec.misfire_grace_sec`, then sets first fire times:
- `interval`: `first_run="stagger"` → `boot + 30 s + JOB_START_STAGGER_SEC × k`, `k` = index among staggered jobs in `JOB_TABLE` order (so 04's `registry_occupants` goes first, 16 §2.6 step 7); `first_run="ops"` → `boot + 30 s` (Phase-0 ops jobs and `alert_dispatch`; `boot + 5 s` when `STARTUP_PRIME_ENABLED`); an int → `boot + n`.
- `cron`: the trigger's natural next fire. When `startup_catchup` and `job_state.last_ok` is older than `previous_fire_time(trigger, now)` (the missed fire — never a fixed age), `next_run_time = boot + STARTUP_CATCHUP_DELAY_SEC` (300 s), moved to the end of any `catchup_avoid_utc` window it lands in and dropped when outside `catchup_only_utc`; it is applied only if earlier than the natural next fire; the run gets `trigger="catchup"`. APScheduler's `misfire_grace_time` only helps a *suspended* process; a restarted process has an empty in-memory job store, which is why E1's attribute exists.
- `manual`: `IntervalTrigger(weeks=520)`, `next_run_time=None` (APScheduler holds it paused). It runs only through `python -m app run-job <id>` or `POST /api/v1/control/run/{id}`; it is exempt from the stagger and from liveness (E1, E4).
- `longtask`: not an APScheduler job; `main()` starts `run_forever` right after READY, unstaggered.

`scoring_6h` checks `SCORING_KILL_SWITCH_PATH` inside `collect()` (12): scores are computed but not published. Per-job enable/disable never needs a restart; a changed trigger key does.

**`JOB_TABLE`** — every job and LongTask exactly as the owner specs define them (E2); owner rows win, and 01 re-syncs on every owner change. Columns: *kind* (E1), *misfire* / *timeout* in s, *paid* (B12), *ops* = `ops_exempt`, *c-up* = `startup_catchup`, *gap* = `min_gap_factor`, *liveness* = max-age s per 16 §2.5 (owner value, else `interval_min·60·2 + timeout + 120`; "—" = exempt / not gated). Every factory, timeout and trigger is copied from the owner row (all owners now name their factories).

| id | owner | factory (`app.…`) | kind | trigger / UTC time (key = default) | misfire | timeout | enabled flags | ph | providers | paid | ops | c-up | gap | liveness |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `log_retention_daily` | 01 | `logging.retention:LogRetentionJob` | interval | every 1440 min (fixed), `first_run=ops` | 3600 | 120 | `LOG_RETENTION_ENABLED` | 0 | — | no | yes | — | 0.5 | 173,040 |
| `storage_retention_daily` | 03 | `storage.retention:RetentionJob` | cron | `STORAGE_RETENTION_CRON_HOUR`:`_MINUTE` = 03:00Z | 3600 | 3600 | `STORAGE_RETENTION_ENABLED` | 0 | — | no | yes | — | 0 | 176,520 |
| `backup_daily` | 16 | `storage.backup:BackupJob` | cron | `DB_BACKUP_CRON_HOUR`:`_MINUTE` = 03:30Z (waits ≤ 1,200 s for `storage_retention_daily`) | 3600 | 5400 | `DB_BACKUP_ENABLED` | 0 | — | no | yes | — | 0 | 178,320 |
| `ops_guard_5m` | 16 | `ops.guard:OpsGuardJob` | interval | every `OPS_GUARD_INTERVAL_MIN` = 5 min, `first_run=ops` | 120 | 60 | `OPS_GUARD_ENABLED` | 0 | — (`deadman` when `DEADMAN_ENABLED`) | no | yes | — | 0 | 780 |
| `ops_digest_daily` | 16 | `ops.digest:OpsDigestJob` | cron | `OPS_DIGEST_HOUR_UTC`:`OPS_DIGEST_MINUTE` = 09:00Z | 3600 | 120 | `OPS_DIGEST_ENABLED` | 0 | — | no | yes | — | 0 | 173,040 |
| `cost_ledger_hourly` | 16 | `costs:CostLedgerJob` | interval | every `COST_LEDGER_INTERVAL_MIN` = 60 min, `first_run=ops` | 600 | 120 | `COST_LEDGER_ENABLED` | 0 | — | no | yes | — | 0 | 7,440 |
| `alert_dispatch` | 14 id, 01 body (D3) | `alerts.dispatch:AlertDispatchJob` | interval | every `ALERT_DISPATCH_INTERVAL_SEC` = 60 s, `first_run=ops` | 120 | 50 | `ALERTS_ENABLED` | 0 | `telegram` (uncapped) | no | yes | — | 0 | 290 |
| `registry_occupants` | 04 | `registry.jobs:RegistryOccupantsJob` | interval | every `REGISTRY_OCCUPANTS_INTERVAL_MIN` = 360 min; `JOB_TABLE` stagger index 0 (first job at boot) | 3600 | 600 | `REGISTRY_ENABLED` | 1 | `tmc` | no | no | — | 0.5 | 43,920 |
| `registry_identity` | 04 | `registry.jobs:RegistryIdentityJob` | cron | `REGISTRY_IDENTITY_AT_UTC` = 02:40Z | 21600 | 1800 | `REGISTRY_ENABLED` | 1 | none at job level (HTTP: `subtensor_rpc`, `subnetalpha`, `tmc`; a TMC `BudgetExhausted` ends only its coldkey step) | no | no | yes | 0 | 174,720 |
| `chain_sse` | 05 | `collectors.chain_tmc:ChainSseTask` | longtask | continuous; REST fallback every `TMC_TABLE_FALLBACK_SEC` = 600 s | — | — | `TMC_ENABLED`, `TMC_SSE_ENABLED` | 2 | `tmc_sse` (fallback `tmc`) | no | no | — | — | 900 (05) |
| `tmc_hourly` | 05 | `collectors.chain_tmc_rest:TmcHourlyJob` | cron | hourly at `TMC_HOURLY_MINUTE` = :07 | 1800 | 300 | `TMC_ENABLED`, `TMC_HOURLY_ENABLED` | 2 | `tmc` | no | no | — | 0 | 7,620 |
| `tmc_daily` | 05 | `collectors.chain_tmc_rest:TmcDailyJob` | cron | `TMC_DAILY_AT_UTC` = 02:10Z; catch-up avoid `20:25-23:35` | 3600 | 5400 | `TMC_ENABLED`, `TMC_DAILY_ENABLED` | 2 | `tmc` | no | no | yes | 0 | 178,320 |
| `tmc_weekly` | 05 | `collectors.chain_tmc_rest:TmcWeeklyJob` | cron | `TMC_WEEKLY_AT_UTC` = 01:10Z daily (1/7 of netuids) | 3600 | 3600 | `TMC_ENABLED`, `TMC_WEEKLY_ENABLED` | 2 | `tmc` | no | no | — | 0 | 176,520 |
| `chain_features` | 05 | `features.chain_features:ChainFeaturesJob` | cron | `CHAIN_FEATURES_AT_UTC` = 00:05 (official), 06:05, 12:05, 18:05Z | 10800 | 900 | `CHAIN_FEATURES_ENABLED` | 2 | — (DB) | no | no | yes | 0 | 44,220 |
| `chain_rpc_hourly` | 05 | `collectors.chain_rpc:ChainRpcHourlyJob` | cron | hourly at :01 | 1800 | 600 | `SUBTENSOR_RPC_ENABLED` | 2 | `subtensor_rpc` | no | no | — | 0 | 7,920 |
| `chain_rpc_metagraph` | 05 | `collectors.chain_rpc:ChainRpcMetagraphJob` | cron | 00:02, 06:02, 12:02, 18:02Z | 3600 | 1800 | `SUBTENSOR_RPC_ENABLED`, `SUBTENSOR_METAGRAPH_ENABLED` | 2 | `subtensor_rpc` | no | no | — | 0 | 45,120 |
| `backfill_tmc` | 17 | `backfill.tmc_backfill:BackfillTmcJob` | manual | run-job / `POST /control/run` only | — | 14400 | `BACKFILL_TMC_ENABLED` | 3 | `tmc` | no | no | — | 0 | — |
| `github_resolve` | 06 | `collectors.github.jobs:GithubResolveJob` | interval | every `GITHUB_RESOLVE_INTERVAL_MIN` = 360 min, stagger | 1800 | 1800 | `GITHUB_ENABLED` | 4 | `github` | no | no | — | 0.5 | 45,120 |
| `github_org_enum` | 06 | `collectors.github.jobs:GithubOrgEnumJob` | cron | `GITHUB_ORG_ENUM_CRON` = 20:00Z | 3600 | 1800 | `GITHUB_ENABLED` | 4 | `github` | no | no | yes | 0 | 174,720 |
| `github_activity` | 06 | `collectors.github.jobs:GithubActivityJob` | cron | `GITHUB_ACTIVITY_CRON` = 20:30Z | 3600 | 10500 | `GITHUB_ENABLED` | 4 | `github` | no | no | yes | 0 | 183,420 |
| `github_metrics` | 06 | `collectors.github.jobs:GithubMetricsJob` | cron | `GITHUB_METRICS_CRON` = 23:30Z; catch-up avoid `18:00-23:30` | 3600 | 1800 | `GITHUB_ENABLED` | 4 | — (DB) | no | no | yes | 0 | 174,720 |
| `github_html_probe` | 06 | `collectors.github.jobs:GithubHtmlProbeJob` | cron | `GITHUB_HTML_PROBE_CRON` = 03:00Z | 3600 | 3600 | `GITHUB_ENABLED` | 4 | `github_html`, `tmc` | no | no | — | 0 | 176,520 |
| `backfill_github` | 17 | `backfill.github_backfill:BackfillGithubJob` | manual | run-job / `POST /control/run` only | — | 14400 | `BACKFILL_GITHUB_ENABLED` | 4 | `github` | no | no | — | 0 | — |
| `web_check` | 07 | `collectors.web:WebCheckJob` | interval | every `WEB_TICK_MINUTES` = 30 min, stagger | 900 | 1500 | `WEB_ENABLED` | 5 | `web`, `wayback` | no | no | — | 0.5 | 5,220 |
| `web_metrics` | 07 | `collectors.web:WebMetricsJob` | cron | `WEB_METRICS_CRON_HOURS`:`WEB_METRICS_CRON_MINUTE` = 00:05 (official), 06:05, 12:05, 18:05Z | 3600 | 600 | `WEB_ENABLED` | 5 | — (DB) | no | no | — | 0 | 43,920 |
| `web_spn2` | 07 | `collectors.web:WebSpn2Job` | cron | `WEB_SPN2_WEEKDAY` + `WEB_SPN2_HOUR_UTC` = Sun 07:00Z | 21600 | 3600 | `WEB_SPN2_ENABLED` | 5 | `wayback` | no | no | — | 0 | 1,213,320 |
| `discord_6h` | 08 | `collectors.discord:DiscordInvitesJob` | cron | `DISCORD_CRON_HOURS`:`DISCORD_CRON_MINUTE` = 00/06/12/18:00Z (pinned, E3) | 1800 | 1800 | `DISCORD_ENABLED` | 6 | `discord` | no | no | — | 0 | 45,120 (08) |
| `discord_widget_1h` | 08 | `collectors.discord:DiscordWidgetJob` | interval | every `DISCORD_WIDGET_INTERVAL_MIN` = 60 min, stagger | 600 | 600 | `DISCORD_WIDGET_ENABLED` | 6 | `discord` | no | no | — | 0.5 | 7,920 |
| `telegram_daily` | 08 | `collectors.telegram:TelegramCountsJob` | interval | every `TELEGRAM_COUNTS_INTERVAL_MIN` = 1440 min, stagger | 3600 | 1800 | `TELEGRAM_COUNTS_ENABLED` | 6 | `tme` | no | no | — | 0.5 | 174,720 |
| `discord_gateway` | 08 | `collectors.discord_gateway:DiscordGatewayTask` | longtask | continuous; `beat()` every 30 s; backoff 5 → 300 s | — | — | `DISCORD_GATEWAY_ENABLED` | 6 | — (own websocket, not `Http`) | no | no | — | — | 600 (08) |
| `owner_collect_daily` | 11 | `integrity.owner_collect:OwnerCollectJob` | cron | `OWNER_COLLECT_CRON` = 20:30Z (pinned; stop 23:30Z); catch-up only `20:30-23:15` | 3600 | 10800 | `OWNER_ENABLED` | 7 | `tmc`, `rufus`, `subnetradar`, `taostats` | no | no | yes | 0 | 108,000 (11) |
| `owner_watch_hourly` | 11 | `integrity.owner_watch:OwnerWatchJob` | cron | hourly at :12 | 1800 | 300 | `OWNER_WATCH_HOURLY_ENABLED` | 7 | — (DB) | no | no | — | 0 | 10,800 (11) |
| `team_seed_daily` | 11 | `integrity.team_seed:TeamSeedJob` | cron | 04:20Z | 21600 | 600 | `TEAM_SEED_ENABLED` | 7 | — (DB) | no | no | — | 0 | 108,000 (11) |
| `integrity_features` | 11 | `integrity.features:IntegrityFeaturesJob` | cron | `INTEGRITY_FEATURES_AT_UTC` = 00:07 (official), 06:07, 12:07, 18:07Z | 10800 | 900 | `INTEGRITY_FEATURES_ENABLED` | 7 | — (DB) | no | no | yes | 0 | 28,800 (11) |
| `owner_rpc_seed` | 11 | `integrity.owner_history:OwnerRpcSeedJob` | cron | 05:20Z (no-op unless armed) | 21600 | 600 | `OWNER_RPC_HISTORY_SEED_ENABLED` | 7 | — (DB) | no | no | — | 0 | 173,520 |
| `features_hourly` | 12 | `features.engine:FeaturesJob` | interval | every `FEATURES_INTERVAL_MIN` = 60 min, stagger | 600 | 1200 | `FEATURES_ENABLED` | 8 | — (DB) | no | no | — | 0.5 | 8,520 |
| `scoring_6h` | 12 | `scoring.engine:ScoringJob` | cron | `SCORING_CRON_HOURS`:`SCORING_CRON_MINUTE` = 00:15 (official), 06:15, 12:15, 18:15Z | 1800 | 1200 | `SCORING_ENABLED` | 8 | — (DB) | no | no | yes | 0 | 44,520 |
| `x_team_poll` | 09 | `collectors.x:XTeamPollJob` | cron | `X_TEAM_POLL_CRON` = 01:30Z | 3600 | 1800 | `X_ENABLED`, `X_TEAM_ENABLED` + `x_active()` | 9 | `twitterapi_io` \| `official_v2` (= `X_PROVIDER`) | yes | no | — | 0 | 174,720 |
| `x_kol_poll` | 09 | `collectors.x:XKolPollJob` | cron | `X_KOL_POLL_CRON_HOURS` at :30 = 00:30/06:30/12:30/18:30Z | 1800 | 900 | `X_ENABLED`, `X_KOL_ENABLED` + `x_active()` | 9 | same | yes | no | — | 0 | 44,220 |
| `x_mentions_poll` | 09 | `collectors.x:XMentionsPollJob` | cron | `X_MENTIONS_POLL_CRON` = 02:00Z | 3600 | 1800 | `X_ENABLED`, `X_MENTIONS_ENABLED` + `x_active()` | 9 | same | yes | no | — | 0 | 174,720 |
| `x_profile_refresh` | 09 | `collectors.x:XProfileRefreshJob` | cron | `X_PROFILE_REFRESH_CRON` = Sun 02:30Z | 21600 | 1800 | `X_ENABLED` + `x_active()` | 9 | same | yes | no | — | 0 | 1,211,520 |
| `x_ctf_crosscheck` | 09 | `collectors.x_ctf:XCtfCrosscheckJob` | cron | `X_CTF_CROSSCHECK_CRON` = 07:00Z | 3600 | 900 | `X_CTF_CROSSCHECK_ENABLED` | 9 | `subnetalpha` | no | no | — | 0 | 173,820 |
| `rss_feeds_poll` | 09 | `collectors.rss:RssFeedsPollJob` | interval | every `RSS_POLL_INTERVAL_HOURS` = 6 h, stagger | 1800 | 900 | `RSS_ENABLED` | 9 | `rss` | no | no | — | 0.5 | 44,220 |
| `news_google_poll` | 09 | `collectors.news:NewsGooglePollJob` | cron | `RSS_GOOGLE_NEWS_POLL_CRON` = 05:00Z | 3600 | 1800 | `RSS_ENABLED`, `RSS_GOOGLE_NEWS_ENABLED` | 9 | `rss` | no | no | — | 0 | 174,720 |
| `youtube_poll` | 09 | `collectors.youtube:YoutubePollJob` | cron | `YOUTUBE_POLL_CRON` = 05:30Z | 3600 | 900 | `RSS_ENABLED`, `YOUTUBE_ENABLED` | 9 | `youtube` | no | no | — | 0 | 173,820 |
| `llm_classify_submit` | 10 | `nlp.llm_classify:LlmClassifySubmitJob` | cron | hours 0,6,12,18 at `LLM_BATCH_OFFSET_MIN` = :20 | 3600 | 600 | `LLM_ENABLED` | 9 | `anthropic` | yes | no | — | 0 | 43,920 |
| `llm_batch_poll` | 10 | `nlp.llm_classify:LlmBatchPollJob` | interval | every `LLM_POLL_INTERVAL_MIN` = 5 min, stagger | 300 | 1800 | `LLM_ENABLED` | 9 | — (10: free; downloads already-billed batches) | no | no | — | 0 | 2,520 |
| `llm_rules_fallback` | 10 | `nlp.rules:LlmRulesFallbackJob` | interval | every 60 min (fixed), stagger | 1800 | 600 | `LLM_RULES_ENABLED` | 9 | — (DB) | no | no | — | 0 | 7,920 |
| `llm_narrative_weekly` | 10 | `nlp.narrative:LlmNarrativeWeeklyJob` | cron | `LLM_NARRATIVE_DAY_OF_WEEK` + `LLM_NARRATIVE_HOUR_UTC` = Sun 01:00Z | 21600 | 5400 | `LLM_NARRATIVE_ENABLED` | 9 | `anthropic` | yes | no | — | 0 | 1,215,120 |
| `quant_weekly` | 13 | `quant.weekly_job:QuantWeeklyJob` | cron | `QUANT_WEEKLY_JOB_CRON` = `sun 05:00` (F2, E3) | 3600 | `QUANT_WEEKLY_JOB_TIMEOUT_S` = 1800 | `QUANT_ENABLED`, `QUANT_WEEKLY_JOB_ENABLED` (default false) | 10 | — (no network) | no | no | — | 0 | 1,211,520 |
| `dereg_watch` | 14 | `notifications.watch:DeregWatchJob` | interval | every `DEREG_WATCH_INTERVAL_MIN` = 60 min, stagger | 600 | 120 | `DEREG_WATCH_ENABLED` | 11 | — (DB) | no | no | — | 0 | 7,440 |
| `needs_review_digest` | 14 | `notifications.digest:NeedsReviewDigestJob` | cron | `NEEDS_REVIEW_DIGEST_DAY` + `NEEDS_REVIEW_DIGEST_HOUR_UTC`:00 = Mon 08:00Z | 3600 | 300 | `NEEDS_REVIEW_DIGEST_ENABLED` | 11 | `telegram` | no | no | — | 0 | 1,210,020 |
| `telegram_bot` | 14 | `notifications.bot:TelegramBotTask` | longtask | continuous long-poll (`TELEGRAM_POLL_TIMEOUT_SEC` = 20 s) | — | — | `TELEGRAM_COMMANDS_ENABLED` | 11 | `telegram` | no | yes | — | — | — (not gated) |

Per-job alert thresholds (`fail_alert_key` / `fail_alert_threshold`; e.g. a weekly job may set 1): `quant_weekly` uses `fail_alert_key="QUANT_ALERT_AFTER_FAILURES"` (13: 2); every other job uses `JOB_FAIL_ALERT_THRESHOLD` (5) — 04's `REGISTRY_ALERT_AFTER_FAILURES`, 06's `GITHUB_ALERT_AFTER_FAILURES`, 07's `WEB_ALERT_AFTER_FAILURES` and 09's `X_ALERT_AFTER_FAILURES` are retired by their owners. Retired placeholder ids (E2): `registry_sync`, `chain_rest_daily`, `chain_holders_weekly`, `github_repos_6h`, `github_org_daily`, `web_daily`, `x_teams_daily`, `x_kols_6h`, `rss_6h`, `llm_classify_6h`, `chain_prune` (now 03's `chain_prune` retention *policy*), and 10's former `nlp_rules_fallback`/`nlp_events_materialise`/`nlp_text_retention` (03 owns text retention, B10). 15 declares no job (H1: the push variant was dropped).

**Workbench rows (19 §5, added 2026-10-16).** Owner 19, declared at phase 3 (the phase whose data they read), none `ops_exempt` or `startup_catchup`: `wb_technicals` (`features.technicals:TechnicalsJob`, cron hourly :12, `WB_ENABLED`, timeout 600), `wb_dereg` (`features.dereg_distance:DeregDistanceJob`, cron hourly :09, `WB_ENABLED`, timeout 600), `chain_constants_probe` (`collectors.chain_probe:ChainConstantsProbeJob`, cron 01:20Z, `WB_ENABLED`, provider `subtensor_rpc`, timeout 300), `wb_github_probe` (`collectors.wb_github_probe:WbGithubProbeJob`, cron hourly :25, `WB_ENABLED` + `WB_GITHUB_PROBE_ENABLED` and only while `GITHUB_TOKEN` is empty, provider `github`, timeout 3300: 45 requests at the no-token pace ≈ 42 min).

**Declared vs available** (`python -m app check`, `/health.scheduler`; recomputed from the table above, E2). 57 entries = 54 jobs + 3 LongTasks (53 + 19's four, amended 2026-10-16). "Available" = factory module importable, cumulative in implementation order:

| after phase | adds | available | unavailable |
|---|---|---|---|
| 0 (01, 03, 16, 14's `alert_dispatch`) | 7 jobs | 7 | 50 |
| 1 (04) | 2 | 9 | 48 |
| 2 (05) | 6 jobs + `chain_sse` | 16 | 41 |
| 3 (17 + 19's workbench rows) | `backfill_tmc` + 4 | 21 | 36 |
| 4 (06 + 17) | 5 + `backfill_github` | 27 | 30 |
| 5 (07) | 3 | 30 | 27 |
| 6 (08) | 3 + `discord_gateway` | 34 | 23 |
| 7 (11) | 5 | 39 | 18 |
| 8 (12) | 2 | 41 | 16 |
| 9 (09, 10) | 8 + 4 | 53 | 4 |
| 10 (13) | 1 | 54 | 3 |
| 11 (14) | 2 + `telegram_bot` | 57 | 0 |

**Proposed `PAUSE_GROUPS` check (16 owns membership).** 01's `test_pause_groups_cover_job_table` asserts every group member is a declared id, no group contains an `ops_exempt` id, and group names are disjoint from job ids (16 §2.11). Against this table, 16's `llm` group still lists the retired `nlp_rules_fallback`, `nlp_events_materialise`, `nlp_text_retention` and lacks `llm_rules_fallback` — 16 must re-sync it (reported).

### 2.8 Liveness and the systemd watchdog (E4; rules owned by 16 §2.5)

`_jobs_live()` (adapted from `_exit_watchers_live`, `main.py` 3323-3361) implements 16's liveness table exactly:

```python
def stale_ids(rt: Runtime, now: int) -> list[str]:
    out = []
    h = rt.task_handles.get("health")
    if h is not None and h.done(): out.append("api_task")                          # 16 amendment 3
    if (app_config.settings.WATCHDOG_API_SELF_PROBE_ENABLED
            and rt.api_probe_fails >= app_config.settings.WATCHDOG_API_PROBE_FAILS): out.append("api_probe")   # amendment 4
    for job in rt.jobs.values():
        max_age = job.effective_liveness_max_age()                                # None for manual (E1)
        if max_age is None or not job.enabled() or job.paused_reason(): continue
        last = job.status_raw["last_run"] or rt.started_at                         # warm-up measured from process start
        if now - last > max_age: out.append(job.id)
    for task in rt.tasks.values():
        max_age = task.spec.liveness_max_age_sec                                  # None → not liveness-gated (telegram_bot)
        if max_age is None or not task.enabled() or task.paused_reason(): continue
        last = max(x for x in (task.last_beat, task.last_attempt, rt.started_at) if x is not None)   # amendment 5
        if now - last > max_age: out.append(task.id)
    return out

def _jobs_live() -> bool:
    return not stale_ids(get_runtime(), now_ts())
```

Deliberate differences from the old bot: `KILL_SWITCH` does **not** short-circuit liveness to `True` (old 3336-3338) — here it pauses only paid jobs, free jobs must still prove liveness, and paused jobs are skipped individually; a never-run job is measured from `rt.started_at` instead of being skipped (16 §2.5). `effective_liveness_max_age() = liveness_max_age_sec or (interval_min*60*2 + timeout_sec + 120)`, with `interval_min()` of a cron trigger = the largest gap among its next 8 fire times; the resulting per-job values are the *liveness* column of §2.7 (owners' declared values — `chain_sse` 900, `discord_gateway` 600, 08's `discord_6h`, 11's rows — win). The old fixed 90-s `WATCHDOG_LIVENESS_MAX_AGE_SEC` is retired.

`systemd_watchdog_loop(stop)` is the verbatim port (3364-3382): it returns if `not SYSTEMD_WATCHDOG_ENABLED or not $WATCHDOG_USEC`; each tick (`WATCHDOG_USEC/2e6` s = 60 s under `WatchdogSec=120`) it first runs 16's optional self-probe (raw `httpx.AsyncClient`, no limiter, 5 s, `GET http://127.0.0.1:{HEALTH_PORT}/health`; failure → `rt.api_probe_fails += 1`, success → 0), then pings `WATCHDOG=1` iff `stale_ids()` is empty; otherwise it logs `error` `watchdog_withheld` with the stale ids (counted by 16's digest) and withholds, so systemd restarts the unit (`Restart=on-failure`).

### 2.9 HTTP app: `/health` builder (G6), services, jobs, control

`app/api/app.py::create_app() -> FastAPI` (port of `main.py` 2144-2228): `FastAPI(title="TaoSentinel", docs_url=None, redoc_url=None)`; `CORSMiddleware(allow_origins=[o.strip() for o in CONTROL_ALLOWED_ORIGINS.split(",") if o.strip()], allow_methods=["GET","POST"], allow_headers=["Content-Type","X-Auth-Token"])` (2171-2179); read routers mounted open; the control router mounted with `dependencies=[Depends(require_auth)]` router-wide (2185 idiom). `app/api_auth.py` is copied unchanged (36 lines: `import app.config as app_config` at 18, `hmac.compare_digest` on UTF-8 bytes at 33-34, empty token = auth off at 31-32). Routers get accessors via `configure(get_runtime=…)`. 14 later adds its routers to the same app; it serves `/health` from 01's builder, never its own shape (G6).

**`app/api/health.py::build_health(rt) -> dict`** (owner 01; 14 serves it, 16's deploy gate reads only `status` and `version`, G6). Served compact (`JSONResponse`, separators `(",", ":")`, key order as below). Times are ISO `…Z` via `to_iso` (B1).

```json
{
  "status": "ok",                         // "ok" | "degraded" | "initializing" (rule below)
  "timestamp": "2026-09-23T10:00:00Z",
  "version": "0.1.0+3fa2c1d",
  "spec_version": "ts-1.0",
  "uptime_sec": 1234,
  "degraded_reasons": [],                 // vocabulary table below; sorted, de-duplicated
  "kill_switch_active": false,
  "pause_all_active": false,              // ./PAUSE_ALL or 16's integrity hold
  "scoring_killed": false,
  "scheduler": {"running": true, "declared": 53, "available": 7, "unavailable": ["registry_occupants", "…"]},
  "collectors": {
    "backup_daily": {
      "owner": "16", "phase": 0, "kind": "cron", "trigger": "cron[hour='3', minute='30']", "enabled": true,
      "paused": null, "paid": false, "ops_exempt": true, "providers": [], "next_run": "…Z",
      "last_run": "…Z", "last_ok": "…Z", "last_error": null, "last_skip_reason": null,
      "consecutive_failures": 0, "last_rows": 1, "last_duration_sec": 4.2, "last_cost_usd": 0.0,
      "liveness_max_age_sec": 178320, "stale": false
    }
  },
  "tasks": {"chain_sse": {"enabled": true, "paused": null, "connected": true, "last_beat": "…Z", "last_attempt": "…Z",
                          "reconnects": 0, "last_error": null, "liveness_max_age_sec": 900, "stale": false}},
  "spend": {"x": {"usd": 0.0, "cap_usd": 25.0, "pct": 0.0, "paused": false, "reserved_usd": 0.0, "…": "16 extras"}},
  "rate_limits": {"tmc": {"tokens": 1.7, "rate_per_sec": 0.133, "burst": 2, "hour_used": 12, "hour_cap": 500,
                          "month_used": 3120, "month_cap": 90000, "penalized_until": null, "exhausted_until": null}},
  "db": {"path": "…/data/taosentinel.db", "size_bytes": 0, "wal_bytes": 0, "schema_version": 1, "schema_warnings": [],
         "sqlite_version": "3.45.1", "last_backup": null, "last_backup_age_hours": null, "orphan_runs": 0},
  "alerts": {"dispatcher_active": true, "pending_immediate": 0, "latched": ["collector_failing:tmc_daily"]},
  "scores": null,                         // 12's scores_status() once scoring is available
  "ops": {"…": "16 §2.13.1 block (boot_kind, watchdog, disk, backup, deploy, security)"},
  "telegram_configured": true
}
```

Rules:
- **`status`**: `initializing` until `rt.db` is connected **and** `rt.scheduler.running` (also after `shutdown_services` began); then `degraded` iff `degraded_reasons` is non-empty, else `ok`. The deploy gate passes on `ok|degraded` with the expected version and fails on `initializing` after its timeout or when unreachable (G6).
- **`spend`** = `rt.costs.snapshot()` verbatim: 16's group-keyed object (`x`, `llm`, `taostats`, plus 16's request-only `tmc`), each with the five required keys `usd, cap_usd, pct, paused, reserved_usd` (G5/G6); 01 adds nothing.
- **`collectors`** holds built jobs only; declared-but-unavailable ids are listed in `scheduler.unavailable`. `paused` uses B12's closed vocabulary `operator | kill_switch | pause_all | budget | emergency_file` (null when running normally; a disabled flag shows `enabled: false` + `last_skip_reason: "disabled"`). Owner-specific extras (e.g. 06's quota, 13's `last_gate_pass`) come from an optional `health_extra() -> dict` on the job class, merged under `collectors.<id>.extra`.
- **Never 500:** every block is built in its own `try/except` and degrades to `{"error": "…"}` (old 2262-2282 idiom); a failing block never changes `status` by itself.

**`degraded_reasons` vocabulary** (01 defines it; 03, 12 and 16 add the rows marked; a reason not in this table is a bug):

| reason | applies when | from |
|---|---|---|
| `collector_failing:<id>` | job enabled, not paused, `consecutive_failures ≥` its alert threshold | 01 |
| `job_stale:<id>` | the id is in `stale_ids()` (§2.8) — visible even when the watchdog is off (dev) | 01 |
| `task_disconnected:<id>` | LongTask enabled, not paused, `connected=false` for ≥ 300 s since its last connect or boot | 01 (08 cites it) |
| `budget_paused:<group>` | `spend.<group>.paused == true` | 01 over 16's snapshot |
| `spec_unknown` | `app.scoring.spec` is importable and `SPEC_VERSION ∉ SPECS`, or `scoring_6h`'s last error is `spec_unknown` (12 case 21) | 12 |
| `scores_stale` | `scoring_6h` available and enabled, not killed, and 12's `scores_status().stale` (no new primary score for > `SCORING_STALE_AFTER_MIN`) | 12 |
| `schema_warning` | `db.schema_warnings` non-empty (`checksum_drift`, `schema_ahead`) | 03 |
| `wal_large` | WAL file > `DB_WAL_WARN_MB` | 03 |
| `backup_stale` | `DB_BACKUP_ENABLED` and (newest backup age > `DB_BACKUP_STALE_HOURS` (36), or no backup and uptime > `DB_BACKUP_STALE_HOURS`) — replaces the literal 48 h | 16 |
| `mirror_missing` | `DB_BACKUP_MIRROR_DIR` set and `DB_BACKUP_MIRROR_MARKER` absent | 16 |
| `disk_low` / `disk_critical` | free space at `DATA_DIR` < `DISK_FREE_MIN_GB` / < `DISK_FREE_CRITICAL_GB` (critical replaces low) | 01 / 16 |
| `pause_all` | `./PAUSE_ALL` exists | 16 |
| `db_integrity` | 16's integrity hold (`data/run/integrity.json` result ≠ `ok`) | 16 |
| `chain_probe_stale` | `WB_ENABLED`, `chain_constants_probe` available, and no successful probe (`chain_constants` row `NetworkRegistrationCost`, `rpc_storage`) for 48 h (uptime counts when none exists yet) — *added 2026-10-16* | 19 (02 §K7) |

`KILL_SWITCH` and `SCORING_KILLED` are deliberate operator states shown by their flags, not degraded reasons.

`GET /api/health/services` (open; shape `{timestamp, services:{key:{ok,name,detail,last_check,latency_ms}}}` from 2285-2441): Phase-0 probes `database` (`SELECT 1` + `COUNT(*) FROM collector_runs`, WAL mode detail), `scheduler`, `telegram` (`getMe` via `Http` provider `telegram`, 5 s, cached `HEALTH_SERVICES_CACHE_SEC`, only when configured), `disk`. 14 appends provider probes; keys are G1 provider ids.

`GET /api/v1/jobs` (open): every **declared** `JOB_TABLE` row (§2.7 columns, `available`, `unavailable_reason`) merged with the `collectors`/`tasks` status blocks, plus `orphan_runs` (`collector_runs` with `finished_at IS NULL` older than the job's timeout). **Phase-0 control routes (authed, `POST`; 14 takes the router over in Phase 11 with the same semantics):**
- `/api/v1/control/pause/{target}` (body `{reason?, hours?}`) → 16's `await control.pause(target, by="api", reason=…, expires_at=…)`: writes the `control_state` row (B12 — the only persistent pause store; `job_state` is never used for pauses). `target` = an exact declared job/LongTask id or a 16 `PAUSE_GROUPS` name, never a prefix; unknown → 404 `unknown_target`. A group never reaches `ops_exempt` jobs; an exact-id pause of an `ops_exempt` job is allowed (16 §2.11).
- `/api/v1/control/resume/{target}` → `control.resume(target, by="api")`: deletes only the row; when `data/control/<target>.paused` (or a group file covering it) still exists the reply is `200 {"resumed": true, "file_pause_active": "data/control/<target>.paused"}` and says the target stays paused until the operator deletes that file (B12).
- `/api/v1/control/run/{job_id}` → `scheduler.modify_job(job_id, next_run_time=now)`; the run gets `trigger="manual"` (skips the `recent` check, still honours disabled/paused). 409 `in_flight` if running, 409 `disabled`/`paused:<reason>` otherwise not runnable, 404 unknown or unavailable. This is how `manual` jobs (17's backfills) are started.
- `/api/v1/control/reload-config` → `reload_settings()`, response `{changed: [non-secret keys], restart_required: bool, restart_keys: [...]}`.

### 2.10 Logging

`app/logging/logger.py` is `E:\SN_Trader\SubnetTrader\app\logging\logger.py` copied verbatim (1-161) with three edits: `import app.config as app_config` (dynamic read of `LOG_LEVEL`/`JSONL_DIR` at construction), default logger name `"taosentinel"`, and a `set_level(level: str)` method. Contract kept: `JsonlFileHandler` writes `data/logs/YYYY-MM-DD.jsonl`, line-buffered, 10 MB rotation to `.N`; entry `{ts, level, logger, message, data?}`; `logger.info(msg, data={...})` — **no printf args**. `app/logging/retention.py::prune_old_logs()` is main.py 1256-1291 verbatim (`LOG_RETENTION_DAYS=14` per master §11); `LogRetentionJob.collect()` calls it and returns `JobOutcome(rows=removed)`. Third-party loggers (`uvicorn`, `apscheduler`, `httpx`) are set to `WARNING`; `httpx` request lines never include query strings with keys (`Http` logs `provider, method, host, path, status, ms` only).

### 2.11 Alerts: `send_alert`, `AlertLatch`, `alert_dispatch` (D1–D4)

**Transport.** `app/notifications/telegram.py::send_alert(text, *, reply_markup=None) -> bool` is `telegram.py` 97-111 with four changes: it returns `True` iff Telegram answered 200; it is a no-op returning `False` when `TELEGRAM_ENABLED` is false or `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` is empty; the POST goes through `get_http().request("telegram", "POST", f"https://api.telegram.org/bot{token}/sendMessage", json={chat_id, text, parse_mode: "HTML", reply_markup?}, timeout=5, retries=0)` (G1: one HTTP layer; provider `telegram` (G1; 14 §6 wrote `telegram_bot`, which is the LongTask id): `TELEGRAM_MAX_MSG_PER_MIN`/60 s⁻¹, burst 5, acquired with a 5-s timeout — 14 §2.7); failures are logged at debug with the token replaced by `***` (old 110-111). Callers pass dynamic content through `html.escape`. The rest of `telegram.py` (`TelegramDocument` 19-23, `TelegramCommandHandlers` 27-45, `TelegramBot` 114-545) is copied in Phase 0 and wired by 14 (`telegram_bot` LongTask, Phase 11).

**`app/notifications/alerts.py::AlertLatch`** ports the latch of `_health_alerted` (`main.py` 83 and `run_health_scan` 1099-1146) and implements D3 plus 16 §2.13.2 rules a–c:

```python
class AlertLatch:
    def __init__(self, db: "Database | None", *, sink: Callable[[str], Awaitable[bool]] = send_alert,
                 dispatcher_active: Callable[[], Awaitable[bool]] | None = None, clock: Callable[[], int] = now_ts) -> None
    async def fire(self, key: str, text: str, *, event_type: str | None = None, severity: str = "warning",
                   payload: dict | None = None, dedupe_key: str | None = None, source: str = "runtime") -> bool
    async def clear(self, key: str, text: str | None = None, *, event_type: str | None = None, severity: str = "info",
                    payload: dict | None = None, dedupe_key: str | None = None, source: str = "runtime") -> bool
    def is_latched(self, key: str) -> bool
    def snapshot(self) -> dict[str, int]          # key → latched_at (unix s); /health.alerts.latched
async def dispatcher_active(rt: "Runtime | None", db: "Database | None" = None) -> bool
```

`fire()`: (1) if `key` is latched and fired less than `ALERT_REPEAT_MIN` ago → `False`; (2) latch it; (3) `event_type is None` (process notices such as `service_online`) → `await sink(text)`, no row (D3); (4) otherwise `row = {occupant_key: None, netuid: None, event_type, severity, event_at: now, observed_at: now, payload: {**payload, "text": text}, dedupe_key, source, run_id: None}` — `payload.text` is an extra key (03 allows extras) the dispatcher sends verbatim; (5) route = `routing.route_for(row)`: `direct` types (03: `disk_critical`, `db_integrity_failed`) → `await sink(text)` first, then `emit_event(…, alerted_at=now)` best-effort (rule b); else if `await dispatcher_active()` → `emit_event(…, alerted_at=None)` and the dispatcher delivers it; else (dispatcher disabled, paused, or its last run finished > 300 s ago — rule c) → `emit_event(…, alerted_at=now)` and, if a row was inserted (not deduplicated), `await sink(text)`; (6) any exception from `emit_event` (DB error, or `ValueError` for an unknown type / missing required payload key, D1) → `logger.error` + `await sink(text)` (D3 fallback). `clear()` unlatches and, if `text` is given, delivers it the same way (e.g. `collector_recovered`). `source` ∈ `runtime | storage | cost | ops` (B9). Latches are in memory (a restart re-arms them); 16 persists its spend latches in `job_state` itself. `dispatcher_active(rt)` is true iff `alert_dispatch` is built, enabled, not paused and its `last_run` (or `rt.started_at` before the first run) is ≤ 300 s old; without a runtime (CLI processes) it reads the newest `collector_runs` row for `alert_dispatch` (`finished_at ≥ now − 300`). CLI processes pass a `sink` that prints instead of sending (16).

**01's own alerts** (every paging key maps to a catalogued type, D1): `service_online` (after READY; direct, not stored; text `🛰 <b>TaoSentinel online</b>\nversion … · spec ts-1.0 · boot <kind> · jobs 7 available / 53 declared\nhealth: http://HOST:PORT/health`), `collector_failing:<id>` → `collector_failing` and its recovery → `collector_recovered` (§2.6 steps 9–10; texts `⚠️ <b>Collector failing</b>: <id> ×N\n<code>last_error</code>` / `✅ <id> recovered`), `watchdog_withheld` (log only). Spend (`spend_80pct`, `spend_cap_reached`), backup, disk and boot alerts are 16's through the same latch; 01's old `budget_soft:`/`budget_hard:` keys are retired (D1).

**`app/alerts/dispatch.py::AlertDispatchJob`** — job `alert_dispatch` (id and delivery rules owned by 14 §2.4, body by 01, Phase 0, `ops_exempt`; D3 amended). Only this job sends Telegram messages for `events` rows (besides the `AlertLatch` fallbacks above), and it sends only rows with `alerted_at IS NULL`.

```python
# app/alerts/routing.py — DATA owned by 14 §2.4 (pinned to 03 §3.9's alert column by 14's test_routing_matches_catalog, D4)
ROUTES: dict[str, "Route"]                    # event_type → fixed route or a condition for "immediate"
def route_for(row: Mapping, now: int) -> str  # "immediate" | "digest" | "digest_late" | "none" | "direct"
    # 14's order: payload.backfilled → digest; unknown event_type → digest (logged once per type, D3); the type's row;
    # an immediate below ALERT_MIN_SEVERITY → digest; older than ALERT_LATE_AFTER_HOURS → digest_late
# app/alerts/templates.py — 14 §8.5 renderers, shipped in Phase 0
def render(row: Mapping) -> str               # per-type template, else payload.text (AlertLatch rows), else a generic
                                              # html-escaped line "{icon} <b>{event_type}</b> SN{netuid} …"
```

`AlertDispatchJob.collect()` per tick (14 §2.4 "Delivery rules", implemented verbatim): (1) select up to `ALERT_MAX_ROWS_PER_TICK` rows `WHERE alerted_at IS NULL ORDER BY id` (03's `ix_events_unalerted`); (2) `route_for(row, now)`; `digest`, `digest_late` and `none` rows get `alerted_at = now` plus `payload.alert_route` in one `UPDATE … WHERE id = ? AND alerted_at IS NULL` (03 lets `payload` change only while `alerted_at IS NULL`), so they never clog the queue; `direct` rows arrive with `alerted_at` already set; (3) `immediate` rows, oldest first, rendered by `render(row)` and sent with `send_alert`; more than `ALERT_BATCH_THRESHOLD` in one tick → batched messages (header `📬 <b>{k} alerts</b>`, one line per row, chunked at 4,000 characters); (4) on Telegram 200 → `alerted_at = now`, `payload.alert_route = "immediate"`; a 400 for one row is retried once as plain text, then the row gets `payload.alert_failed = true` and `alerted_at` (one poisoned row never blocks the queue); network errors, 5xx and 429 mark nothing and end the tick (next tick retries; `digest_late` eventually absorbs a long outage); (5) Telegram unconfigured (`TELEGRAM_ENABLED` false or token/chat empty) → immediate rows are left `NULL` (they turn `digest_late` after `ALERT_LATE_AFTER_HOURS`). Repeat suppression is the emitters' `dedupe_key` + `ux_events_dedupe`, not the dispatcher's. `JobOutcome(rows=sent, note=f"sent={n} digest={d} none={z} failed={f}")`. The dispatcher never hard-codes an event type; 14's `needs_review_digest` (Phase 11) reads the `digest`/`digest_late` rows by `payload.alert_route`.

### 2.12 One HTTP layer: `ProviderLimiter` and `Http` (G1)

```python
# app/ratelimit.py
class BudgetExhausted(Exception):            # re-exported by app.http; raised before any request is sent
    provider: str; window: Literal["hour", "month"]; until: int      # unix s at which the window frees a slot

@dataclass(frozen=True, slots=True)
class ProviderLimit:
    rate_per_sec: float | None               # None = no token bucket (connection-counted or unmetered)
    burst: int = 1
    hourly_cap: int | None = None            # sliding 3,600-s window of sent requests
    monthly_cap: int | None = None           # UTC calendar month; seeded from provider_spend.requests at boot
    max_connections: int | None = None       # tmc_sse (TMC_SSE_MAX_CONNECTIONS)
    book_spend: bool = True                  # False for internal egress (deadman): no provider_spend rows

class TokenBucket:
    def __init__(self, rate_per_sec: float, burst: int, clock=time.monotonic)
    async def acquire(self, n: int = 1, timeout: float | None = None) -> bool   # sleeps until n tokens; False on timeout
    def penalize(self, seconds: float) -> None          # refuse tokens until clock()+seconds

class ProviderLimiter:
    @classmethod
    def from_settings(cls) -> "ProviderLimiter"          # the §6 table, values read from Settings
    def reload(self) -> None                             # re-read rates/caps after reload_settings(); counters kept
    async def acquire(self, provider: str, *, host: str | None = None, timeout: float | None = None) -> bool
        # unknown provider → KeyError (fail loud); cap reached → BudgetExhausted; honours the provider penalty,
        # the per-host cooldown of `host`, and 16's shared penalty file data/run/penalty-<p>.json (re-read ≤ every 5 s)
    def count(self, provider: str) -> None               # one request actually sent: hour/month windows += 1
    def observe(self, provider: str, headers: Mapping[str, str], *, host: str | None = None) -> None
        # X-RateLimit-Remaining == 0 + Reset → penalize the provider until Reset; Retry-After → penalize_host(host)
    def penalize(self, provider: str, seconds: float) -> None          # whole provider (+ 16's penalty file)
    def penalize_host(self, provider: str, host: str, seconds: float) -> None   # per-host cooldown inside the bucket
    def exhausted(self, provider: str) -> tuple[str, int] | None       # (window, until) while a cap is hit
    def month_requests(self, provider: str) -> int
    def seed_month(self, provider: str, requests: int) -> None         # 16's CostGovernor.load()
    def snapshot(self) -> dict                                          # /health.rate_limits
```

A `Retry-After` from one host cools down **only that host** inside the provider's bucket (`penalize_host`), so one slow site cannot stall provider `web` for every other site; single-host providers (`tmc`, `github`, `taostats`, …) are therefore effectively penalised as a whole. `X-RateLimit-*` exhaustion headers (a provider-wide quota) penalise the provider. The penalty file is written only for provider-wide penalties (16 §2.12 rule 3).

```python
# app/http.py
class ProviderError(Exception):              # raised with `from exc` so the transport exception is chained (__cause__)
    provider: str; status: int | None; url: str   # url with the query string and secrets redacted
    attempts: int
class Http:
    def __init__(self, limiter: ProviderLimiter, costs: "CostGovernor | None" = None)
    def client(self, provider: str) -> httpx.AsyncClient   # one per provider: User-Agent = the provider's own UA key when its
        # owner defines one (07 `WEB_USER_AGENT`, 08 `DISCORD_USER_AGENT`) else HTTP_USER_AGENT; provider
        # auth headers (TMC_API_KEY, GITHUB_TOKEN, TWITTERAPI_IO_KEY, X_BEARER_TOKEN, TAOSTATS_API_KEY, DISCORD_BOT_TOKEN when
        # the owner asks for it); timeout=HTTP_TIMEOUT_SEC; follow_redirects=False; Limits(max_connections=8, max_keepalive_connections=4)
    async def request(self, provider: str, method: str, url: str, *, params=None, headers=None, json=None, data=None,
                      content=None, timeout: float | None = None, retries: int | None = None,
                      follow_redirects: bool = False, ok_statuses: tuple[int, ...] = (200,),
                      max_body_bytes: int | None = None) -> httpx.Response
    async def aclose(self) -> None
def get_http() -> Http                       # the runtime's instance, else a lazily built process-local one (CLI, fallbacks)
```

`request()` rules:
1. `await limiter.acquire(provider, host=urlsplit(url).hostname)`; a loopback URL (`127.0.0.1`, `::1`, `localhost` — e.g. 05's local lite node) skips the bucket and the counters.
2. Each attempt that reaches the wire: `limiter.count(provider)` and, when `book_spend`, `await costs.record(provider, 0.0, requests=1)` (buffered; G5's default is `requests=0`, so `Http` passes 1 explicitly and callers that book USD for the same call pass `requests=0`). `limiter.observe(provider, response.headers, host=…)` on every response.
3. Status in `ok_statuses` → returned immediately, never retried (G1; e.g. 08 lists 429, 09 lists 400/402).
4. 429 → `penalize_host(Retry-After or 60 s)` then retry; 5xx, timeouts and connect errors → backoff 1 s, 4 s, 16 s … then retry; at most `retries` (default `HTTP_MAX_RETRIES` = 2) retries, then `raise ProviderError(...) from last_exc` (transport error or `httpx.HTTPStatusError`).
5. Every other status (404, 403, 401, 3xx, 304, 202, other 4xx) is returned unchanged and never retried within the call — the GitHub resolver, Discord probes and TMC 404 handling depend on seeing them.
6. **Body cap:** the body is streamed and cut at `max_body_bytes` (default `HTTP_MAX_BODY_BYTES` = 5 MiB; 07 passes `WEB_MAX_BODY_BYTES`); a cut response carries `response.extensions["truncated"] = True` and is logged `http_body_truncated`; `response.content` holds the first `max_body_bytes` bytes. Every response also carries `extensions["elapsed_ms"]` (send → body read, limiter wait excluded; 07's `latency_ms`).
9. **Multi-host providers** (`MULTI_HOST_PROVIDERS` = {`web`}, 07 §2.1): a `Retry-After` stays a per-host cooldown capped at 3,600 s, `x-ratelimit-*` headers never penalise the whole bucket, and `limiter.host_cooldown(provider, host)` lets the caller skip a cooling host instead of sleeping on it.
7. Logging: `provider, method, host, path, status, ms, attempt` only — never the query string, headers or bodies; a path segment of the form `bot<token>` is logged as `bot***`.
8. `BudgetExhausted` propagates to the job (§2.6 step 8). Calls that cannot use `httpx` (08's discord.py websocket, the Anthropic SDK, 05's websocket RPC) call `await rt.limiter.acquire(p)` + `rt.limiter.count(p)` themselves and book requests with `costs.record(p, 0.0, requests=n)` (G5).

### 2.13 Cost governor (owner 16)

`app/costs.py` is specified entirely by 16 §2.10 (public API = G5; it supersedes this section's earlier Phase-0 subset and ships in Phase 0). 01 only calls `CostGovernor(db, limiter=, alerts=)`, `load()`, `record(provider, usd, *, requests=…, job=…, run_id=…)` (from `Http` and the wrapper's reconciliation), `run_total(run_id)`, `is_exhausted(provider)` (via 16's `effective_pause`), `flush()` (shutdown) and `snapshot()` (`/health.spend`). Cap semantics are F2's: a cap of 0 for a paid group disables it (fail-closed).

### 2.14 CLI (`app/__main__.py`)

```
python -m app                 # == serve
python -m app serve           # full runtime (systemd ExecStart); refuses when TAOSENTINEL_PROCESS_ROLE=script (16)
python -m app check           # load Settings, print public_dict(), open DB, apply schema, PRAGMA quick_check, build JobRegistry;
                              #   prints schema_version, schema_warnings, sqlite_version; exit 0/1
python -m app jobs            # the declared table: id | owner | phase | kind | available | enabled | paused | trigger | next_run
python -m app run-job <id>    # runtime without scheduler/HTTP server; one run through the wrapper (trigger="cli"; manual jobs too);
                              #   prints the status JSON; exit 0 iff ok
python -m app health          # GET http://HEALTH_HOST:HEALTH_PORT/health, pretty-print; exit 0 iff status ∈ {ok, degraded}
```

Other specs register sub-commands through `register(subparsers)` hooks imported when available: 03 `db …`, 16 `backup …`, `costs …`, `ops …` (incl. `ops pause/resume <target>` → `control_state`), 12 `python -m app.scoring …`, 13 `python -m app.quant …`. None of `check/jobs/run-job` takes 16's instance lock.

---

## 3. Data contract

**No DDL here (B7).** Every table 01 touches is defined in 03 §3.1/§3.6 (STRICT, every time column INTEGER unix seconds UTC, B1). What 01 writes and reads:

| table (03) | 01 writes | 01 reads |
|---|---|---|
| `schema_version(version, name, checksum, applied_at)` | via `Database.apply_schema()` = 03 `migrate()` | `check`, `/health.db.schema_version` |
| `collector_runs(run_id, job, started_at, finished_at, ok, rows, error, cost_usd, duration_sec, note, meta_json)` | one row per wrapper run (§2.6; `job` = a `JOB_TABLE` id); `started_at`/`finished_at` unix s | orphans (`finished_at IS NULL`), `/api/v1/jobs`, `dispatcher_active()` in CLI processes |
| `job_state(job, key, value_json, updated_at)` | `last_ok` + collector watermarks only — never pause state (B12) | `min_gap`, startup catch-up |
| `control_state(target, key, value, reason, set_by, set_at, expires_at)` | via 16's `control.pause()/resume()` from the Phase-0 control routes | via 16's cached `effective_pause()` |
| `provider_spend(provider, month, usd, requests, units_json, updated_at)` | via 16's governor (`Http` → `record(p, 0.0, requests=1)`, flushed hourly/at shutdown) | limiter seeding (16 `load()`) |
| `events` | `collector_failing`, `collector_recovered` (via `AlertLatch` → 03 `emit_event`, `source='runtime'`, `occupant_key`/`netuid` NULL); the dispatcher sets `alerted_at` and `payload.suppressed`/`payload.undelivered` while `alerted_at IS NULL` | pending rows (`ix_events_unalerted`) |

**`Database` API this spec calls (03 §2.3):** `Database(path)`, `connect()`, `apply_schema() -> int`, `execute`, `executemany`, `fetchone`, `fetchall`, `fetchval`, `transaction()`, `stats()`, `schema_version`, `schema_warnings`, `close()`; `app/storage/events.py::emit_event(db, *, event_type, occupant_key, netuid, severity, event_at, observed_at, payload, dedupe_key, source, run_id, alerted_at=None) -> int | None` (None = deduplicated). Backups (`create_backup`, `latest_backup_age_hours`) are 16's.

**metric_keys produced:** none. **Events emitted:** `collector_failing` (warning; payload `{collector, consecutive_failures, last_error, job}`, dedupe `{collector}|{first_failure_at}`) and `collector_recovered` (info; `{collector, failures, down_seconds, job}`, same dedupe), both catalogued in 03 §3.9 (D1/D2); `service_online` is a direct notice, not a row (D3).

Invariants imposed on every later job by this contract: every raw row carries `observed_at` (default `RunContext.started_at`, or the actual fetch time), `event_at` when the source gives one, `run_id`, `source` (B9); primary identity is `occupant_key = f"{netuid}:{network_registered_at}"`; never forward-fill; a job writes `0.0`-meaning rows only from a *successful* fetch that proves absence, never from an `except` branch (the wrapper's `ok=0` row is the only trace of a failure).

---

## 4. Status semantics: skip vs failure vs stall (the wrapper-level absence-vs-failure table)

Collector specs map their metrics to 0.0/None; this table fixes what the **runtime** records so those mappings are consistent.

| Condition | `collector_runs` row | `status()` | liveness | alert |
|---|---|---|---|---|
| Flag off (`*_ENABLED=false`) | none | `last_skip_reason="disabled"` | ignored | none |
| Paused (B12 vocabulary; 16 precedence) | none | `paused` ∈ `pause_all \| kill_switch \| emergency_file \| operator \| budget` | ignored | budget: 16's `spend_cap_reached` once per group/month |
| Run in flight when another trigger fires | none | `last_skip_reason="in_flight"` | unchanged | none |
| Skipped as recent (`min_gap`) | none | `last_skip_reason="recent"` | `last_run` unchanged | none |
| `collect()` returns, `rows=0` | `ok=1, rows=0` (**verified nothing to do / verifiable absence**) | `last_ok` updated | fresh | none |
| `collect()` returns, `rows>0` | `ok=1, rows=n` | idem | fresh | none |
| `collect()` raises `BudgetExhausted` (hourly/monthly cap, G1) | `ok=1, rows=rows_committed, note="paused:budget …"` (not a failure) | later runs `paused="budget"` until the window resets | fresh | none from 01 |
| `collect()` raises (incl. `ProviderError`) | `ok=0, error=…` (**collection failure**: downstream metrics stay `None`) | `consecutive_failures+1`, `last_error` | fresh (attempt completed) | `collector_failing` event at the job's threshold (default 5), `collector_recovered` on the next success |
| `collect()` exceeds `timeout_sec` | `ok=0, error="timeout after Ns"` | idem | fresh | idem |
| Process dies mid-run | `ok=NULL, finished_at=NULL` | — | — | none (visible in `/api/v1/jobs` as `orphan_runs`) |
| Wrapper never returns (wedged loop) | in-flight row | `last_run` stale | **stale → watchdog withheld → systemd restart** | log error |
| Job module missing (later phase) | none | listed in `scheduler.unavailable` | ignored | none |
| `manual` job (17's backfills) | only when run via CLI/API | as any run | exempt (E4) | as any run |
| Startup catch-up run (E1) | normal row, `trigger="catchup"` in the log/note | as any run | fresh | as any run |

---

## 5. Configuration (full v1 inventory, F1/F2)

All keys live on the single `Settings` class and are read dynamically (`import app.config as app_config; app_config.settings.<KEY>`, §2.4). **This section is the union of every owner's §5 table** (F1), grouped by owner spec; the owner's type and default win, and the owner's validator/semantics text is binding (01 implements it in `app/config.py`; it is not repeated here). F2's resolved values are applied and marked. Rows marked "full: owner §5" are truncated here only for width. Secrets (`*_TOKEN`, `*_KEY`, `*_SECRET`, `*_PAT`, `DEADMAN_PING_URL`, `IA_SECRET_KEY`) are masked in `public_dict()`, `check` output and logs. `.env.example` lists every key with its default and a one-line comment; the checklist (18) key names are these. Keys that are not `Settings` fields and therefore not listed: process environment (`TAOSENTINEL_ROOT`, `TAOSENTINEL_ENV_FILE`, `TAOSENTINEL_PROCESS_ROLE`, `TAOSENTINEL_NETWORK_TESTS`, `XDG_CACHE_HOME`, `SQLITE_TMPDIR`), 16's `deploy.env` and installer variables, the frontend's `NEXT_PUBLIC_*`, and 15's bot-side `SENTINEL_*` block (it lives in the SubnetTrader repo). Module constants frozen per `SPEC_VERSION` (12's registry, 11's S1–S5 constants, 08's inflation constants) are not config (master §7.3).

**Cap semantics (F2):** a cap of 0 for a paid provider group (`X_MONTHLY_CAP_USD`, `LLM_MONTHLY_CAP_USD`) or a request cap of 0 (`TAOSTATS_MONTHLY_CAP_REQ`, `TMC_MONTHLY_CAP`) disables that provider (deny all, fail-closed); there is no "unlimited" value. Inventory size (2026-09-23 owner tables): **627 keys** in 15 groups.

**Runtime, alerts transport, shared providers (01)** — 35 rows

| key | type | default | note |
|---|---|---|---|
| `SPEC_VERSION` | str | `"ts-1.0"` | frozen scoring version; must be a key of 12's `SPECS` (`spec_unknown` otherwise) |
| `SCORING_SPEC_VERSION` | read-only property | = `SPEC_VERSION` | F2 alias (12, 14, 15 name it); an env value is ignored with a warning if it differs |
| `LOG_LEVEL` | str | `"INFO"` | upper-cased ∈ {DEBUG, INFO, WARNING, ERROR, CRITICAL} (old `config.py:637-644`) |
| `LOG_RETENTION_ENABLED` / `LOG_RETENTION_DAYS` | bool / int | `True` / `14` | master §11 |
| `JSONL_DIR` / `DATA_DIR` / `SEED_DIR` | path | `"data/logs"` / `"data"` / `"seed"` | resolved against `APP_ROOT` |
| `DB_PATH` / `DB_BUSY_TIMEOUT_MS` | path / int | `"data/taosentinel.db"` / `5000` | 03 uses them |
| `HEALTH_HOST` | str | `"127.0.0.1"` | alias `API_HOST` (14/15); warn when non-loopback and `API_AUTH_TOKEN` empty (old `main.py` 3258-3265) |
| `HEALTH_PORT` | int | `8090` | alias `API_PORT`; 1024–65535 (0 = pick a free port, tests only) |
| `API_AUTH_TOKEN` | str | `""` | secret; empty = auth off (I2: set before any `tailscale serve`) |
| `CONTROL_ALLOWED_ORIGINS` | str | `"http://localhost:3001,http://127.0.0.1:3001"` | comma list; validator rejects `*` (14) |
| `KILL_SWITCH_PATH` / `SCORING_KILL_SWITCH_PATH` | path | `"./KILL_SWITCH"` / `"./SCORING_KILLED"` | B12 closed sentinel list (with 16's `PAUSE_ALL_PATH`) |
| `SYSTEMD_WATCHDOG_ENABLED` | bool | `True` | old `config.py:578` |
| `SHUTDOWN_DB_TIMEOUT_SEC` / `SHUTDOWN_HTTP_TIMEOUT_SEC` / `SHUTDOWN_DEADLINE_SEC` | float | `5.0` / `3.0` / `10.0` | validator `DB + HTTP + 2 ≤ DEADLINE < 30` (unit `TimeoutStopSec=30`) |
| `HTTP_USER_AGENT` | str | `"TaoSentinel/0.1 (+https://github.com/<operator>/taosentinel)"` | operator edits |
| `HTTP_TIMEOUT_SEC` / `HTTP_MAX_RETRIES` | float / int | `20.0` / `2` | `Http` defaults |
| `HTTP_MAX_BODY_BYTES` | int | `5242880` | default `max_body_bytes` of `Http.request` (§2.12) |
| `JOB_FAIL_ALERT_THRESHOLD` / `JOB_DEFAULT_TIMEOUT_SEC` | int | `5` / `1800` | master §9 "collector failing ×5" |
| `JOB_START_STAGGER_SEC` / `STARTUP_CATCHUP_DELAY_SEC` | int | `20` / `300` | §2.7 |
| `STARTUP_PRIME_ENABLED` | bool | `True` | ops interval jobs first fire at boot + 5 s instead of + 30 s |
| `ALERT_REPEAT_MIN` | int | `360` | re-fire a still-latched `AlertLatch` key |
| `DISK_FREE_MIN_GB` | float | `2.0` | `disk_low` (16 adds `DISK_FREE_CRITICAL_GB`) |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | str | `""` / `""` | secret token; checklist B3 |
| `TELEGRAM_COMMANDS_ENABLED` / `TELEGRAM_ALLOWED_CHAT_IDS` | bool / str | `True` / `""` | 14's `telegram_bot`; empty list → only `TELEGRAM_CHAT_ID` |
| `DB_BACKUP_ENABLED` / `DB_BACKUP_DIR` / `DB_BACKUP_MIRROR_DIR` | bool / path / str | `True` / `"data/backups"` / `""` | old `config.py:542-547`; pipeline is 16's |
| `DB_BACKUP_RETENTION` / `DB_BACKUP_WEEKLY_RETENTION` | int | `7` / `8` | I1 |
| `DB_BACKUP_INTERVAL_HOURS` / `DB_BACKUP_MIN_AGE_HOURS` | int / float | `24` / `12.0` | staleness math / startup-backup guard (16) |
| `BLOCK_SECONDS` | float | `12.0` | shared (04) |
| `SUBTENSOR_RPC_RATE_PER_SEC` / `SUBNETALPHA_RATE_PER_SEC` | float | `2.0` / `1.0` | limiter rows (04 marks them shared, 01) |
| `ONFINALITY_API_KEY` / `ONFINALITY_WS_URL` | str | `""` / `"wss://bittensor-finney.api.onfinality.io/public-ws"` | checklist B2; keyed URL form UNVERIFIED (17) |
| `ONFINALITY_RATE_PER_SEC` | float | `20.0` | clamped to 4.0 without a key (public 5 rps) |
| `BLOCKMACHINE_API_KEY` | str (secret) | `""` | Blockmachine archive RPC key (17; added 2026-09-24, BUILD-LOG Phase 3); `.env` spelling is case-insensitive |
| `BLOCKMACHINE_WS_URL` | str | `"wss://rpc.blockmachine.io"` | key sent as `?authorization=` |
| `BLOCKMACHINE_RATE_PER_SEC` | float | `12.0` | `0.1..16` (plan: 1,000 req/min) — limiter provider `blockmachine`, burst 8 |
| `BLOCKMACHINE_MONTHLY_CAP_REQ` | int | `4500000` | `≥ 0` (plan: 5 M RU/month, 1 RU per call; `0` = provider disabled, 02 §F2) |
| `TAOSTATS_ENABLED` / `TAOSTATS_API_KEY` / `TAOSTATS_BASE_URL` | bool / str / str | `False` / `""` / `"https://api.taostats.io"` | checklist C3; 11 reads them |
| `TAOSTATS_RATE_PER_MIN` | int | `5` | free tier (60 on Standard); UNVERIFIED |
| `TAOSTATS_MONTHLY_CAP_REQ` | int | `10000` | F2; deprecated alias `TAOSTATS_MONTHLY_CALL_CAP`; 0 = disabled |
| `TAOSTATS_MONTHLY_CAP_USD` | float | `0.0` | display only (flat subscription, 16) |
| `TAOAPP_API_KEY` | str | `""` | reserved (checklist C5) |

**Storage (03)** — 16 rows

| key | type | default | note |
|---|---|---|---|
| `LLM_PENDING_TEXT_MAX_DAYS` | int | `7` | F2/B10 (owner 03); 08 stamps `text_purge_after` |
| `DB_READER_ENABLED` | bool | `True` |  |
| `DB_CACHE_SIZE_MB` | int | 64 |  |
| `DB_MMAP_SIZE_MB` | int | 256 |  |
| `DB_INCREMENTAL_VACUUM_PAGES` | int | 25600 |  |
| `STORAGE_RETENTION_ENABLED` | bool | `True` |  |
| `STORAGE_RETENTION_CRON_HOUR` | int | 3 |  |
| `STORAGE_RETENTION_CRON_MINUTE` | int | 0 |  |
| `RETENTION_REQUIRE_BACKUP_HOURS` | float | 30.0 |  |
| `RETENTION_DELETE_BATCH` | int | 50000 |  |
| `PRICE_BARS_5M_RETENTION_DAYS` | int | 90 |  |
| `METRIC_PROVISIONAL_RETENTION_DAYS` | int | 90 |  |
| `CHAIN_REST_SNAPSHOTS_RETENTION_DAYS` | int | 90 |  |
| `GITHUB_HTTP_CACHE_RETENTION_DAYS` | int | 30 |  |
| `LLM_BATCH_ITEMS_RETENTION_DAYS` | int | 90 |  |
| `WEB_TEXT_RETENTION_DAYS` | int | 365 |  |

**Registry / identity (04)** — 25 rows

| key | type | default | note |
|---|---|---|---|
| `REGISTRY_ENABLED` | bool | `True` |  |
| `REGISTRY_OCCUPANTS_INTERVAL_MIN` | int | `360` |  |
| `REGISTRY_IDENTITY_AT_UTC` | str `HH:MM` | `"02:40"` |  |
| `REGISTRY_CHAIN_ENABLED` | bool | `True` |  |
| `SUBTENSOR_RPC_URL` | str | `"https://entrypoint-finney.opentensor.ai"` |  |
| `SUBTENSOR_RPC_FALLBACK_URLS` | list[str] | `[]` |  |
| `SUBTENSOR_RPC_TIMEOUT_SEC` | float | `15.0` |  |
| `REGISTRY_CHAIN_QUERY_CHUNK` | int | `100` |  |
| `REGISTRY_CHAIN_MIN_KEYS` | int | `90` |  |
| `SUBNETALPHA_ENABLED` | bool | `True` |  |
| `SUBNETALPHA_BASE_URL` | str | `"https://subnetalpha.ai/wp-json/wp/v2/subnet"` |  |
| `SUBNETALPHA_TIMEOUT_SEC` | float | `30` |  |
| `REGISTRY_SUBNETALPHA_MAX_PAGES` | int | `5` |  |
| `REGISTRY_SUBNETALPHA_MIN_RECORDS` | int | `100` |  |
| `REGISTRY_TMC_COLDKEY_ENABLED` | bool | `True` |  |
| `DISCORD_MAIN_GUILD_ID` | str | `"799672011265015819"` |  |
| `REGISTRY_MISSING_RUNS_TO_DEREG` | int | `3` |  |
| `REGISTRY_ANOMALY_CONSECUTIVE_RUNS` | int | `3` |  |
| `REGISTRY_REOBSERVE_DAYS` | int | `30` |  |
| `REGISTRY_WATCH_RETIRE_DAYS` | int | `14` |  |
| `REGISTRY_JUNK_TOKENS` | list[str] | the regex alternatives from §2.6 |  |
| `REGISTRY_STALE_SOURCE_DAYS` | int | `3` |  |
| `LEGACY_FUNDAMENTALS_PATH` | str | `"seed/fundamentals.json"` |  |
| `LEGACY_EDGE_NOTES_DIR` | str | `"seed/subnet_edge"` |  |
| `LEGACY_AS_OF` | str ISO | `"2026-06-02T11:03:41Z"` |  |

**Chain / TMC (05)** — 66 rows

| key | type | default | note |
|---|---|---|---|
| `TMC_ENABLED` | bool | `true` |  |
| `TMC_BASE_URL` | str | `https://api.taomarketcap.com/public/v1` |  |
| `TMC_API_KEY` | str | `""` |  |
| `TMC_RATE_PER_MIN` | int | `8` | 1..10 (F2) |
| `TMC_RATE_BURST` | int | `2` | validator `TMC_RATE_BURST + TMC_RATE_PER_MIN ≤ 10` (F2) |
| `TMC_HOURLY_CAP` | int | `500` | ≤ 600 (F2) |
| `TMC_MONTHLY_CAP` | int | `90000` | ≤ 100,000 (F2) |
| `TMC_REQUEST_TIMEOUT_SEC` | int | `30` |  |
| `TMC_MAX_RETRIES` | int | `2` |  |
| `TMC_SSE_ENABLED` | bool | `true` |  |
| `TMC_SSE_URL` | str | `https://api.taomarketcap.com/public/v1/sse/subnets/table/` |  |
| `TMC_SSE_READ_TIMEOUT_SEC` | int | `90` |  |
| `TMC_SSE_SILENCE_SEC` | int | `180` |  |
| `TMC_SSE_BACKOFF_MIN_SEC` / `TMC_SSE_BACKOFF_MAX_SEC` | float | `1` / `60` |  |
| `TMC_SSE_ALERT_AFTER_SEC` | int | `1800` |  |
| `TMC_SSE_UNAUTHORIZED_RETRY_SEC` | int | `3600` |  |
| `TMC_SSE_MAX_CONNECTIONS` | int | `2` | 1..2; `SSE_MAX_CONNECTIONS` retired (F2) |
| `TMC_SSE_BLOCKS_STREAM` | enum auto/true/false | `auto` |  |
| `TMC_TABLE_FALLBACK_ENABLED` | bool | `true` |  |
| `TMC_TABLE_FALLBACK_SEC` | int | `600` |  |
| `CHAIN_TICKS_BLOCK_STRIDE` | int | `5` | F2 |
| `CHAIN_TICKS_RETENTION_DAYS` | int | `30` | F2; applied by 03 `storage_retention_daily` |
| `TMC_HOURLY_ENABLED` | bool | `true` |  |
| `TMC_HOURLY_MINUTE` | int | `7` |  |
| `TMC_TRENDING_ENABLED` | bool | `true` |  |
| `TMC_MARKET_ENABLED` | bool | `true` |  |
| `TMC_DAILY_ENABLED` | bool | `true` |  |
| `TMC_DAILY_AT_UTC` | str `HH:MM` | `02:10` |  |
| `TMC_SUBNETS_PAGE_LIMIT` | int | `200` |  |
| `TMC_ANALYTICS_1W_ENABLED` | bool | `true` |  |
| `TMC_ANALYTICS_1W_FIELDS` | str | `ts,block_number,emission_percent,chain_buys,subnet_tao,…` (full: owner §5) |  |
| `TMC_PRESSURE_ENABLED` | bool | `true` |  |
| `TMC_PRESSURE_WATCHLIST_ENABLED` | bool | `false` |  |
| `TMC_PRESSURE_WATCHLIST_SIZE` | int | `40` |  |
| `TMC_WEEKLY_ENABLED` | bool | `true` |  |
| `TMC_WEEKLY_AT_UTC` | str `HH:MM` | `01:10` |  |
| `TMC_HOLDERS_ENABLED` | bool | `true` |  |
| `TMC_HOLDERS_PAGE_LIMIT` / `TMC_HOLDERS_MAX_PAGES` | int | `100` / `2` |  |
| `TMC_HOLDERS_MAX_AGE_DAYS` | int | `14` |  |
| `TMC_CANDLES_ENABLED` | bool | `true` |  |
| `TMC_ANALYTICS_1Y_ENABLED` | bool | `true` |  |
| `TMC_VALIDATORS_ENABLED` | bool | `true` |  |
| `TMC_NEURONS_ENABLED` | bool | `false` |  |
| `NETWORK_IMMUNITY_PERIOD_BLOCKS` | int | `864000` |  |
| `GATE_MODEL_VERSION` | str | `gate-v440-1` |  |
| `GATE_BAR_RANK` | int | `32` |  |
| `GATE_EXPONENT` | float | `3.0` |  |
| `GATE_FIT_MAX_ITER` / `GATE_FIT_TOL` | int / float | `50` / `1e-4` |  |
| `GATE_FIT_TOLERANCE_PP` | float | `6.0` |  |
| `GATE_MIN_PRED_SHARE` | float | `1e-4` |  |
| `GATE_HOURLY_ENABLED` | bool | `false` |  |
| `GATE_BROKEN_AFTER_DAYS` | int | `7` |  |
| `DEREG_STARVED_RATIO` / `DEREG_EMISSION_DECLINE_PCT` / `DEREG_PRICE_DECLINE_PCT` / `DEREG_DEPTH_DECLINE_PCT` | float | `0.20` / `30` / `40` / `30` |  |
| `DEREG_W_PROXIMITY` / `DEREG_W_STARVED` / `DEREG_W_DECLINE` | float | `0.45` / `0.25` / `0.30` |  |
| `DEREG_HIGH_RISK_AT` | float | `0.60` |  |
| `DEREG_HIST_DAYS` | int | `30` |  |
| `CHAIN_FEATURES_ENABLED` | bool | `true` |  |
| `CHAIN_FEATURES_AT_UTC` | list[str] | `["00:05","06:05","12:05","18:05"]` |  |
| `CHAIN_FEATURES_CATCHUP_DAYS` | int | `7` |  |
| `SUBTENSOR_RPC_ENABLED` | bool | `false` |  |
| `SUBTENSOR_RPC_MODE` | enum sdk/jsonrpc | `jsonrpc` | 01 validator rejects `sdk` (needs `bittensor`, §2.3) |
| `SUBTENSOR_LOCAL_WS` | str | `ws://127.0.0.1:9944` |  |
| `SUBTENSOR_RPC_ENDPOINTS` | list[str] | `[SUBTENSOR_LOCAL_WS, "wss://bittensor-finney.api.onfina…` (full: owner §5) |  |
| `SUBTENSOR_ENDPOINT_COOLDOWN_SEC` | int | `300` |  |
| `SUBTENSOR_RPC_CONNECT_TIMEOUT_SEC` / `SUBTENSOR_RPC_READ_TIMEOUT_SEC` | int | `30` / `45` |  |
| `SUBTENSOR_METAGRAPH_ENABLED` | bool | `false` |  |

**GitHub (06)** — 36 rows

| key | type | default | note |
|---|---|---|---|
| `GITHUB_ENABLED` | bool | `True` |  |
| `GITHUB_TOKEN` | str | `""` | alias `GITHUB_PAT`; secret |
| `GITHUB_API_BASE` | str | `https://api.github.com` |  |
| `GITHUB_HTML_BASE` | str | `https://github.com` |  |
| `GITHUB_RATE_PER_HOUR` | int | `4000` |  |
| `GITHUB_MAX_CONCURRENCY` | int | `4` |  |
| `GITHUB_RESERVE_REMAINING` | int | `300` |  |
| `GITHUB_RESOLVE_INTERVAL_MIN` | int | `360` |  |
| `GITHUB_ORG_ENUM_CRON` | str | `"20:00"` |  |
| `GITHUB_ACTIVITY_CRON` | str | `"20:30"` |  |
| `GITHUB_METRICS_CRON` | str | `"23:30"` |  |
| `GITHUB_HTML_PROBE_CRON` | str | `"03:00"` |  |
| `GITHUB_ACTIVITY_LOOKBACK_DAYS` | int | `90` |  |
| `GITHUB_ACTIVITY_MAX_REPOS_PER_OCCUPANT` | int | `12` |  |
| `GITHUB_ACTIVITY_MAX_COMMIT_PAGES` | int | `5` |  |
| `GITHUB_ACTIVITY_STALE_DAYS` | int | `2` |  |
| `GITHUB_PR_LOOKBACK_DAYS` | int | `90` |  |
| `GITHUB_PR_MAX_PER_REPO` | int | `20` |  |
| `GITHUB_COMMIT_STATS_SAMPLE` | int | `10` |  |
| `GITHUB_HYGIENE_INTERVAL_DAYS` | int | `7` |  |
| `GITHUB_ORIGINALITY_INTERVAL_DAYS` | int | `30` |  |
| `GITHUB_TEMPLATE_REPO` | str | `opentensor/bittensor-subnet-template` |  |
| `GITHUB_STATS_202_RETRIES` | int | `3` |  |
| `GITHUB_STATS_202_WAIT_SEC` | float | `3.0` |  |
| `GITHUB_ETAG_ENABLED` | bool | `True` |  |
| `GITHUB_ORG_MAX_REPOS` | int | `200` |  |
| `GITHUB_GONE_GRACE_DAYS` | int | `14` |  |
| `GITHUB_SPAM_BURST_N` | int | `10` |  |
| `GITHUB_SPAM_BURST_SEC` | int | `60` |  |
| `GITHUB_SPAM_MIN_STATS_TOTAL` | int | `5` |  |
| `GITHUB_FTE_COMMIT_DAYS_28D` | int | `10` |  |
| `GITHUB_BOT_LOGINS_EXTRA` | str | `""` |  |
| `GITHUB_EXTRA_DENYLIST` | str | `""` |  |
| `GITHUB_HTML_PROBE_ENABLED` | bool | `True` |  |
| `GITHUB_HTML_PROBE_PACING_SEC` | float | `1.0` |  |
| `GITHUB_TMC_CROSSCHECK_ENABLED` | bool | `True` |  |
| `GITHUB_STATS_INTERVAL_DAYS` | int | `7` | 06 §5 (added 2026-10-17) |
| `GITHUB_TOKEN_EXPIRES_AT` | str | `""` | `YYYY-MM-DD` or empty; 06 §5 (added 2026-10-17) |

**Web presence (07)** — 41 rows

| key | type | default | note |
|---|---|---|---|
| `WEB_ENABLED` | bool | `true` |  |
| `WEB_TICK_MINUTES` | int | `30` |  |
| `WEB_RATE_PER_SEC` | float | `1.0` |  |
| `WEB_RATE_BURST` | int | `2` |  |
| `WEB_PER_URL_MIN_INTERVAL_H` | float | `23.5` |  |
| `WEB_SAME_HOST_GAP_S` | float | `10` |  |
| `WEB_NETWORK_PROBE_HOSTS` | list[str] | `["archive.org","github.com"]` |  |
| `WEB_LATEST_MAX_AGE_DAYS` | int | `2` |  |
| `WEB_CONCURRENCY` | int | `4` |  |
| `WEB_TIMEOUT_S` | float | `10` |  |
| `WEB_CONNECT_TIMEOUT_S` | float | `5` |  |
| `WEB_DNS_TIMEOUT_S` | float | `5` |  |
| `WEB_RETRY_DELAY_S` | int | `600` |  |
| `WEB_MAX_REDIRECTS` | int | `5` |  |
| `WEB_MAX_BODY_BYTES` | int | `2_000_000` |  |
| `WEB_USER_AGENT` | str | `Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHT…` (full: owner §5) |  |
| `WEB_ROBOTS_ENABLED` | bool | `true` |  |
| `WEB_ROBOTS_CACHE_DAYS` | int | `7` |  |
| `WEB_EXCLUDED_HOSTS` | list[str] | `["x.com","twitter.com","discord.com","discord.gg","t.me…` (full: owner §5) |  |
| `WEB_MAX_WEBSITES_PER_OCCUPANT` | int | `2` |  |
| `WEB_PARKING_HOSTS` / `WEB_PARKED_STRONG_PHRASES` / `WEB_PARKED_WEAK_KEYWORDS` / `WEB_HIJACK_KEYWORDS` / `WEB_SHARED_PLATFORM_HOSTS` | list[str] | §2.5 / §2.6 defaults |  |
| `WEB_TINY_BODY_CHARS` | int | `300` |  |
| `WEB_CONTENT_RESET_RATIO` | float | `0.2` |  |
| `WEB_FRESHNESS_MIN_DELTA` | float | `0.03` |  |
| `WEB_STORE_TEXT_MAX_CHARS` | int | `32_000` |  |
| `WEB_TLS_WARN_DAYS` | int | `14` |  |
| `WEB_UPTIME_WINDOW_DAYS` | int | `30` |  |
| `WEB_UPTIME_MIN_CHECKS` | int | `3` |  |
| `WEB_DOWN_STREAK_DAYS` | int | `7` |  |
| `WEB_DOCS_GONE_STREAK_DAYS` | int | `2` |  |
| `WEB_DOCS_AUTODISCOVER_ENABLED` | bool | `true` |  |
| `WEB_WAYBACK_ENABLED` | bool | `true` |  |
| `WEB_WAYBACK_RECHECK_DAYS` | int | `30` |  |
| `WAYBACK_RATE_PER_SEC` | float | `0.5` |  |
| `WEB_SPN2_ENABLED` | bool | `false` |  |
| `IA_ACCESS_KEY` / `IA_SECRET_KEY` | str | `""` |  |
| `WEB_SPN2_WEEKDAY` | int | `6` |  |
| `WEB_SPN2_HOUR_UTC` | int | `7` |  |
| `WEB_SPN2_MIN_INTERVAL_S` | float | `10` |  |
| `WEB_METRICS_CRON_HOURS` | str | `"0,6,12,18"` |  |
| `WEB_METRICS_CRON_MINUTE` | int | `5` | < `SCORING_CRON_MINUTE` |

**Community (08)** — 39 rows

| key | type | default | note |
|---|---|---|---|
| `DISCORD_ENABLED` | bool | `True` |  |
| `DISCORD_INTERVAL_MIN` | int | `360` |  |
| `DISCORD_WIDGET_ENABLED` | bool | `True` |  |
| `DISCORD_WIDGET_INTERVAL_MIN` | int | `60` |  |
| `DISCORD_RATE_PER_SEC` | float | `1.0` |  |
| `DISCORD_BOT_TOKEN` | str (secret) | `""` |  |
| `DISCORD_AGGREGATOR_GUILD_ID` | str | `""` |  |
| `DISCORD_GATEWAY_ENABLED` | bool | `False` |  |
| `DISCORD_CHANNELS_SEED_PATH` | path | `"seed/discord_channels.csv"` |  |
| `TELEGRAM_COUNTS_ENABLED` | bool | `True` |  |
| `TELEGRAM_COUNTS_INTERVAL_MIN` | int | `1440` |  |
| `TELEGRAM_COUNTS_RATE_PER_SEC` | float | `0.5` |  |
| `DISCORD_CRON_HOURS` | str | `"0,6,12,18"` |  |
| `DISCORD_CRON_MINUTE` | int | `0` |  |
| `DISCORD_API_BASE` | str | `"https://discord.com/api/v10"` |  |
| `DISCORD_USER_AGENT` | str | `"DiscordBot (https://github.com/<operator>/taosentinel,…` (full: owner §5) |  |
| `DISCORD_INVITES_USE_BOT_TOKEN` | bool | `True` |  |
| `DISCORD_MAIN_VANITY_CODE` | str | `"bittensor"` |  |
| `DISCORD_MAIN_ANNOUNCE_CHANNEL_ID` | str | `"830075335084474390"` |  |
| `DISCORD_MAIN_CHANNEL_RECHECK_DAYS` | int | `7` |  |
| `DISCORD_DEAD_WEEKLY_AFTER_DAYS` | int | `30` |  |
| `DISCORD_INVITE_EXPIRY_WARN_DAYS` | int | `7` |  |
| `DISCORD_INVALID_GUARD_PER_10MIN` | int | `100` |  |
| `DISCORD_WIDGET_DISABLED_RECHECK_DAYS` | int | `7` |  |
| `DISCORD_WIDGET_SKIP_GUILD_IDS` | str | `"799672011265015819"` |  |
| `DISCORD_CHANNEL_MAP_REVIEWED` | bool | `False` |  |
| `DISCORD_PRESENCE_MIN_SAMPLES` | int | `2` |  |
| `DISCORD_AGGREGATOR_CHANNEL_IDS` | str | `""` |  |
| `DISCORD_GATEWAY_ENUMERATE_FOLLOWS` | bool | `True` |  |
| `DISCORD_BACKFILL_DAYS` | int | `14` |  |
| `DISCORD_BACKFILL_OVERLAP_SEC` | int | `600` |  |
| `DISCORD_BACKFILL_MAX_PER_CHANNEL` | int | `2000` |  |
| `DISCORD_TEXT_MAX_CHARS` | int | `4000` |  |
| `TELEGRAM_BASE_URL` | str | `"https://t.me"` |  |
| `TELEGRAM_PREVIEW_ENABLED` | bool | `True` |  |
| `TELEGRAM_POSTS_STORE_TEXT` | bool | `False` | F2/B10 |
| `DISCORD_STORE_TEXT` | bool | `True` | F2/B10 |
| `COMMUNITY_EVENTS_ENABLED` | bool | `True` |  |
| `COMMUNITY_VENUE_CHANGE_DEBOUNCE` | int | `2` |  |

**X / RSS / news / YouTube (09)** — 54 rows

| key | type | default | note |
|---|---|---|---|
| `X_ENABLED` | bool | `False` |  |
| `X_PROVIDER` | str | `"null"` |  |
| `TWITTERAPI_IO_KEY` | str | `""` |  |
| `TWITTERAPI_IO_BASE_URL` | str | `https://api.twitterapi.io` |  |
| `X_BEARER_TOKEN` | str | `""` |  |
| `X_OFFICIAL_BASE_URL` | str | `https://api.x.com/2` |  |
| `X_OFFICIAL_EXCLUDE` | str | `"retweets,replies"` |  |
| `X_MONTHLY_CAP_USD` | float | `25.0` | F2: 25 (75 with `official_v2`); ≥ 0, 0 = X disabled (fail-closed) |
| `X_SOFT_ALERT_PCT` | float | `0.80` |  |
| `X_PACING_ENABLED` | bool | `True` |  |
| `X_PRICES_JSON` | str | `""` |  |
| `X_TEAM_ENABLED` | bool | `True` |  |
| `X_TEAM_POLL_CRON` | str | `"01:30"` |  |
| `X_TEAM_POLL_MODE` | str | `"timeline"` |  |
| `X_TIMELINE_MAX_PAGES` | int | `3` |  |
| `X_KOL_ENABLED` | bool | `True` |  |
| `X_KOL_POLL_CRON_HOURS` | str | `"0,6,12,18"` |  |
| `X_KOL_POLL_MODE` | str | `"search"` |  |
| `X_MENTIONS_ENABLED` | bool | `True` |  |
| `X_MENTIONS_TOP_N` | int | `40` |  |
| `X_MENTIONS_POLL_CRON` | str | `"02:00"` |  |
| `X_SEARCH_MAX_POSTS_PER_QUERY` | int | `200` |  |
| `X_PROFILE_REFRESH_CRON` | str | `"sun 02:30"` |  |
| `X_PROFILE_REFRESH_DAYS` | int | `7` |  |
| `X_UNRESOLVED_RETRY_DAYS` | int | `7` |  |
| `X_MIN_CREDIBILITY` | float | `0.30` |  |
| `X_AUTHOR_SHARE_CAP` | float | `0.25` |  |
| `X_DUPLICATE_WINDOW_MIN` | int | `10` |  |
| `X_RATE_PER_SEC` | float | `1.0` |  |
| `X_OFFICIAL_RATE_PER_SEC` | float | `0.25` |  |
| `X_HTTP_TIMEOUT_SEC` | float | `20.0` |  |
| `X_RAW_ARCHIVE_ENABLED` | bool | `True` |  |
| `X_CTF_CROSSCHECK_ENABLED` | bool | `True` |  |
| `X_CTF_CROSSCHECK_CRON` | str | `"07:00"` |  |
| `X_CTF_FALLBACK_METRICS` | bool | `True` |  |
| `KOL_SEED_PATH` | str | `"seed/kol_accounts.csv"` |  |
| `RSS_ENABLED` | bool | `True` |  |
| `RSS_POLL_INTERVAL_HOURS` | int | `6` |  |
| `RSS_GOOGLE_NEWS_ENABLED` | bool | `True` |  |
| `RSS_GOOGLE_NEWS_TOP_N` | int | `40` |  |
| `RSS_GOOGLE_NEWS_POLL_CRON` | str | `"05:00"` |  |
| `RSS_GOOGLE_NEWS_DECODE_URLS` | bool | `False` |  |
| `RSS_FEEDS_SEED_PATH` | str | `"seed/feeds.csv"` |  |
| `RSS_EXTRA_FEEDS` | str | `""` |  |
| `RSS_RATE_PER_SEC` | float | `0.5` |  |
| `GOOGLE_NEWS_MIN_INTERVAL_SEC` | float | `10.0` |  |
| `RSS_HTTP_TIMEOUT_SEC` | float | `20.0` |  |
| `RSS_DEAD_AFTER_FAILURES` | int | `10` |  |
| `NEWS_KEYWORDS` | str | `"bittensor,dtao,subnet,$tao,taostats,opentensor"` |  |
| `YOUTUBE_ENABLED` | bool | `True` |  |
| `YOUTUBE_POLL_CRON` | str | `"05:30"` |  |
| `YOUTUBE_NOVELTY_PLAYLIST_ID` | str | `"PLN0WJc6io8zqEknIutfyEjiJViazrxrzU"` |  |
| `YOUTUBE_RATE_PER_SEC` | float | `0.33` |  |
| `YOUTUBE_API_KEY` | str | `""` |  |

**LLM classification (10)** — 31 rows

| key | type | default | note |
|---|---|---|---|
| `LLM_ENABLED` | bool | `false` |  |
| `ANTHROPIC_API_KEY` | str | `""` |  |
| `LLM_MONTHLY_CAP_USD` | float | `60` | F2: 60; ≥ 0, 0 = LLM disabled |
| `LLM_BULK_MODEL` | str | `claude-haiku-5-5` | 10 §2.3 (re-verified 2026-10-18) |
| `LLM_ESCALATION_MODEL` | str | `claude-sonnet-5-5` |  |
| `LLM_NARRATIVE_MODEL` | str | `claude-opus-5-5` |  |
| `LLM_REFUSAL_FALLBACK` | bool | `true` | 10 §2.3: Sonnet/Opus calls use `fallbacks="default"` |
| `LLM_SCHEMA_V` | int | `1` |  |
| `LLM_RUBRIC_PATH` | str | `app/nlp/prompts/classify_v1.md` |  |
| `LLM_BATCH_INTERVAL_MIN` | int | `360` |  |
| `LLM_BATCH_OFFSET_MIN` | int | `20` |  |
| `LLM_POLL_INTERVAL_MIN` | int | `5` |  |
| `LLM_BATCH_ITEMS_PER_REQUEST` | int | `20` |  |
| `LLM_BATCH_MAX_REQUESTS` | int | `250` |  |
| `LLM_BATCH_STALE_H` | int | `26` |  |
| `LLM_LOOKBACK_DAYS` | int | `3` |  |
| `LLM_MAX_ITEM_CHARS` | int | `1200` |  |
| `LLM_TOKENS_PER_CHAR` | float | `0.29` |  |
| `LLM_PRICES_JSON` | str | `""` |  |
| `LLM_ENTITY_MIN_CONFIDENCE` | float | `0.6` |  |
| `LLM_ESCALATION_ENABLED` | bool | `true` |  |
| `LLM_ESCALATION_MAX_PER_DAY` | int | `40` |  |
| `LLM_NARRATIVE_ENABLED` | bool | `true` |  |
| `LLM_NARRATIVE_DAY_OF_WEEK` / `LLM_NARRATIVE_HOUR_UTC` | str / int | `sun` / `1` |  |
| `LLM_NARRATIVE_MAX_SUBNETS` | int | `40` |  |
| `LLM_NARRATIVE_EVIDENCE_TOKENS` | int | `8000` |  |
| `LLM_FALLBACK_AFTER_H` | int | `18` |  |
| `LLM_RULES_ENABLED` | bool | `true` |  |
| `LLM_TEXT_RETENTION_DAYS` | int | `90` |  |
| `LLM_REQUEST_TIMEOUT_S` | int | `180` |  |
| `LLM_RATE_PER_SEC` | float | `0.5` |  |
| `LLM_EVAL_ALLOW_NETWORK` | bool | `false` |  |

**Team & integrity (11)** — 52 rows

| key | type | default | note |
|---|---|---|---|
| `TEAM_ENABLED` | bool | `true` |  |
| `TEAM_SEED_ENABLED` | bool | `true` |  |
| `TEAM_SEED_SUBNETALPHA_ENABLED` | bool | `true` |  |
| `TEAM_SEED_LEGACY_ENABLED` | bool | `true` |  |
| `TEAM_LEGACY_NOTES_DIR` | path | `"seed/subnet_edge"` |  |
| `TEAM_LEGACY_AS_OF` | str ISO | `""` → spec 04's `LEGACY_AS_OF` (`2026-06-02T11:03:41Z`) |  |
| `TEAM_VERIFY_MIN_SOURCES` | int | `2` |  |
| `TEAM_EVIDENCE_EXCLUDED_DOMAINS` | list[str] | `["subnetalpha.ai"]` |  |
| `TEAM_REVERIFY_DAYS` | int | `90` |  |
| `TEAM_VERIFY_STALE_DAYS` | int | `180` |  |
| `TEAM_ANON_MAX_CHARS` | int | `200` |  |
| `TEAM_NAME_MAX_TOKENS` | int | `5` |  |
| `OWNER_ENABLED` | bool | `true` |  |
| `OWNER_COLLECT_CRON` | str `HH:MM` | `"20:30"` |  |
| `OWNER_KEYS_SOURCE` | enum auto/list/detail | `auto` |  |
| `OWNER_TMC_MAX_PER_MIN` | int | `5` |  |
| `OWNER_TMC_MAX_PER_RUN` | int | `518` |  |
| `OWNER_ACTIVITY_PAGE_LIMIT` | int | `100` |  |
| `OWNER_ACTIVITY_MAX_PAGES` | int | `10` |  |
| `OWNER_ACTIVITY_OVERLAP_SEC` | int | `3600` |  |
| `OWNER_ACTIVITY_BACKFILL_DAYS` | int | `35` |  |
| `OWNER_BACKFILL_MAX_CALLS_PER_RUN` | int | `128` |  |
| `OWNER_ACTIVITY_TIME_FORMAT` | enum auto/ms/s/iso | `auto` |  |
| `OWNER_AMOUNT_UNITS` | enum auto/rao/unit | `auto` |  |
| `OWNER_STAKE_REMOVED_ENABLED` | bool | `true` |  |
| `OWNER_STAKE_REMOVED_MAX_CALLS_PER_DAY` | int | `20` |  |
| `OWNER_RECONCILE_MIN_ALPHA` | float | `500.0` |  |
| `OWNER_NEURON_SPAN` | enum 1D/1W/1M | `1W` |  |
| `OWNER_RUFUS_ENABLED` | bool | `true` |  |
| `OWNER_RUFUS_TABLE_URL` | str | `"https://s3.hippius.com/rufus/public/conviction-table/d…` (full: owner §5) |  |
| `OWNER_RUFUS_BAKED_AT_URL` | str | `"https://s3.hippius.com/rufus/public/conviction-table/d…` (full: owner §5) |  |
| `OWNER_RUFUS_MAX_AGE_HOURS` | int | `48` |  |
| `OWNER_SUBNETRADAR_ENABLED` | bool | `true` |  |
| `OWNER_SUBNETRADAR_URL` | str | `"https://subnetradar.com/swaps"` |  |
| `OWNER_TAOSTATS_ENABLED` | bool | `true` |  |
| `OWNER_TAOSTATS_EXTRINSIC_WEEKDAY` | int | `0` |  |
| `OWNER_STREAM_ENABLED` | bool | `false` | inert in v1 (no LongTask declared) |
| `OWNER_WATCH_HOURLY_ENABLED` | bool | `true` |  |
| `OWNER_RPC_HISTORY_SEED_ENABLED` | bool | `true` |  |
| `OWNER_SNAPSHOT_MAX_AGE_HOURS` | int | `36` |  |
| `OWNER_MIN_EARNED_ALPHA` | float | `1.0` |  |
| `OWNER_PERP_OFF_MIN_ALPHA` | float | `100.0` |  |
| `OWNER_PERP_OFF_MATCH_TOL` | float | `0.10` |  |
| `OWNER_OVERRIDE_MAX_WINDOW_DAYS` | int | `45` |  |
| `OWNER_SOURCE_DISAGREE_TOL` | float | `0.05` |  |
| `OWNER_S4_ENABLED` | bool | `false` |  |
| `OWNER_S4_HOLDING_SHARE_MAX` / `OWNER_S4_LOCK_RATIO_MIN` | float | `0.25` / `0.5` |  |
| `OWNER_S4_VALI_STAKE_SHARE_MAX` / `OWNER_S4_SELF_WEIGHT_MAX` | float | `0.30` / `0.20` |  |
| `DTAO_LAUNCH_UTC` | str ISO | `"2025-02-13T00:00:00Z"` |  |
| `INTEGRITY_FEATURES_ENABLED` | bool | `true` |  |
| `INTEGRITY_FEATURES_AT_UTC` | list[str] | `["00:07","06:07","12:07","18:07"]` |  |
| `INTEGRITY_FEATURES_CATCHUP_DAYS` | int | `7` |  |

**Features / scoring (12)** — 19 rows

| key | type | default | note |
|---|---|---|---|
| `FEATURES_ENABLED` | bool | `True` |  |
| `FEATURES_INTERVAL_MIN` | int | `60` |  |
| `SCORING_ENABLED` | bool | `True` |  |
| `SCORING_CRON_HOURS` | str | `"0,6,12,18"` |  |
| `SCORING_CRON_MINUTE` | int | `15` |  |
| `SCORING_OFFICIAL_HOUR` | int | `0` | pinned: validator rejects ≠ 0 (B6, F2) |
| `SCORING_SHADOW_SPECS` | str | `""` |  |
| `SCORING_MIN_CONFIDENCE` | float | `0.4` |  |
| `SCORING_EVENTS_ENABLED` | bool | `True` |  |
| `SCORING_CATCHUP_ENABLED` | bool | `True` |  |
| `SCORING_CATCHUP_MAX_SLOTS` | int | `8` |  |
| `SCORING_LATE_GRACE_MIN` | int | `45` |  |
| `SCORING_STALE_AFTER_MIN` | int | `420` |  |
| `SCORE_JUMP_ALERT_POINTS` | float | `15.0` |  |
| `RANK_STABILITY_ENABLED` | bool | `True` |  |
| `RANK_AUTOCORR_ALERT_BELOW` | float | `0.9` |  |
| `METRIC_NORM_PROVISIONAL_RETENTION_DAYS` | int | `14` |  |
| `FEATURE_FN_TIMEOUT_SEC` | float | `20.0` |  |
| `FEATURE_COMPUTE_BUDGET_SEC` | int | `600` |  |

**Validation harness (13)** — 49 rows

| key | type | default | note |
|---|---|---|---|
| `QUANT_ENABLED` | bool | `True` |  |
| `QUANT_RESULTS_DIR` | str | `data/quant/results` |  |
| `QUANT_SNAPSHOT_DIR` | str | `data/quant/snapshots` |  |
| `QUANT_SNAPSHOT_KEEP` | int | `2` |  |
| `QUANT_DB_PATH` | str | `""` |  |
| `QUANT_OBSERVE_TELEMETRY_PATH` | str | `data/quant/observe_telemetry.json` |  |
| `QUANT_OBSERVE_MIN_DAYS` / `QUANT_OBSERVE_MIN_COVERAGE` / `QUANT_OBSERVE_MIN_ENTRIES` | int / float / int | `30` / `0.60` / `20` |  |
| `QUANT_UPSIZE_MIN_T` | float | `3.0` |  |
| `QUANT_ZERO_RUN_DAYS` | int | `7` |  |
| `QUANT_ZERO_EMISSION_EPS_PCT` | float | `1e-4` |  |
| `QUANT_SEED` | int | `0` |  |
| `QUANT_SPEC_VERSION` | str | `ts-1.0` |  |
| `QUANT_PIT_MODE` | str | `strict` |  |
| `QUANT_RESET_DROP_RATIO` | float | `0.30` |  |
| `QUANT_SURVIVORSHIP_START` | str (date) | `2025-09-22` |  |
| `QUANT_LIVE_ERA_START` | str (date) | `2026-04-22` |  |
| `QUANT_GRID_SURVIVORSHIP_S` | int | `86400` |  |
| `QUANT_GRID_LIVE_S` | int | `14400` |  |
| `QUANT_HORIZONS_24H` | str | `7,28,84` |  |
| `QUANT_HORIZONS_4H` | str | `6,12,42` |  |
| `QUANT_LOOKBACK_BARS` | int | `6` |  |
| `QUANT_MIN_DEPTH_TAO` | float | `3000.0` |  |
| `QUANT_MIN_NAMES` | int | `10` |  |
| `QUANT_MIN_IC_DATES` | int | `10` |  |
| `QUANT_CONTROLS` | str | `size,emission_share` |  |
| `QUANT_TRADE_TAO` | float | `2.0` |  |
| `QUANT_FEE_RATE` | float | `0.003` |  |
| `QUANT_IMPACT_SCALE` | float | `1.0` |  |
| `QUANT_DEATH_FLOOR_RETURN` | float | `0.01` |  |
| `QUANT_SCORE_MAX_AGE_S` | int | `129600` |  |
| `QUANT_PIT_MAX_AGE_DEFAULT_S` | int | `172800` |  |
| `QUANT_GATE_FACTOR` | str | `quality_cg` |  |
| `QUANT_GATE_LABEL` | str | `ret_log` |  |
| `QUANT_GATE_HORIZON_DAYS` | int | `7` |  |
| `QUANT_WF_TEST_DAYS` | int | `28` |  |
| `QUANT_WF_TRAIN_MIN_FRACTION` | float | `0.5` |  |
| `QUANT_MIN_FOLDS` | int | `4` |  |
| `QUANT_DSR_THRESHOLD` | float | `0.95` |  |
| `QUANT_DECAY_MIN` | float | `0.25` |  |
| `QUANT_MONOTONE_MIN` | float | `0.6` |  |
| `QUANT_SURVIVAL_HORIZON_DAYS` | int | `90` |  |
| `QUANT_SURVIVAL_MIN_EVENTS` | int | `10` |  |
| `QUANT_SURVIVAL_RATIO_MIN` | float | `2.0` |  |
| `QUANT_GATE_TOP_N` | int | `32` |  |
| `QUANT_GATE_CROSS_DAYS` | int | `28` |  |
| `QUANT_EMISSION_REGIME_BOUNDARIES` | str | `taoflow:2025-09-22,price_burn:2026-06-25,gate_v440:2026-07-28` |  |
| `QUANT_WEEKLY_JOB_ENABLED` | bool | `False` |  |
| `QUANT_WEEKLY_JOB_CRON` | str | `"sun 05:00"` | F2/E3 |
| `QUANT_WEEKLY_JOB_TIMEOUT_S` | int | `1800` |  |
| `QUANT_ALERT_AFTER_FAILURES` | int | `2` |  |

**API / Telegram / alerts (14)** — 31 rows

| key | type | default | note |
|---|---|---|---|
| `UI_BASE_URL` | str | `"http://127.0.0.1:3001"` |  |
| `SCORE_FEED_TTL_SEC` | int | `25200` |  |
| `STREAM_ENABLED` | bool | `True` |  |
| `STREAM_MAX_CONNECTIONS` | int | `8` |  |
| `STREAM_HEARTBEAT_SEC` | int | `15` |  |
| `STREAM_POLL_SEC` | float | `5.0` |  |
| `STREAM_REPLAY_MAX` | int | `1000` |  |
| `STREAM_HEALTH_EVERY_SEC` | int | `60` |  |
| `EXPORT_CACHE_MAX_AGE_SEC` | int | `300` |  |
| `HEALTH_PROBE_TIMEOUT_SEC` | float | `10.0` |  |
| `HEALTH_SERVICES_CACHE_SEC` | int | `60` |  |
| `TELEGRAM_ENABLED` | bool | `True` |  |
| `TELEGRAM_POLL_TIMEOUT_SEC` | int | `20` |  |
| `TELEGRAM_MAX_MSG_PER_MIN` | int | `20` |  |
| `TELEGRAM_CONFIRM_TTL_SEC` | int | `300` |  |
| `TERMINAL_API_URL` | str | `"http://127.0.0.1:8787"` | 14 §8.4a; http(s) loopback URL or empty (= `/wallet` off) |
| `ALERTS_ENABLED` | bool | `True` |  |
| `ALERT_DISPATCH_INTERVAL_SEC` | int | `60` |  |
| `ALERT_MIN_SEVERITY` | str | `"info"` |  |
| `ALERT_LATE_AFTER_HOURS` | int | `24` |  |
| `ALERT_BATCH_THRESHOLD` | int | `5` |  |
| `ALERT_MAX_ROWS_PER_TICK` | int | `200` |  |
| `WATCHLIST_NETUIDS` | list[int] | `[]` |  |
| `DEREG_WATCH_ENABLED` | bool | `True` |  |
| `DEREG_WATCH_INTERVAL_MIN` | int | `60` |  |
| `DEREG_WATCH_SAFETY_MAX` | float | `0.10` |  |
| `DEREG_WATCH_IMMUNITY_WARN_DAYS` | int | `7` |  |
| `NEEDS_REVIEW_DIGEST_ENABLED` | bool | `True` |  |
| `NEEDS_REVIEW_DIGEST_DAY` | str | `"mon"` |  |
| `NEEDS_REVIEW_DIGEST_HOUR_UTC` | int | `8` |  |
| `BLOCKED_SITE_DAYS` | int | `7` |  |
| `FRONTEND_PORT` | int | `3001` |  |

**Ops, backups, cost governor (16)** — 40 rows

| key | type | default | note |
|---|---|---|---|
| `PAUSE_ALL_PATH` | path | `"./PAUSE_ALL"` |  |
| `CONTROL_DIR` | path | `"data/control"` |  |
| `RUN_DIR` | path | `"data/run"` |  |
| `DB_BACKUP_CRON_HOUR` | int | `3` |  |
| `DB_BACKUP_CRON_MINUTE` | int | `30` |  |
| `DB_BACKUP_COMPRESS` | bool | `True` |  |
| `DB_BACKUP_GZIP_LEVEL` | int | `6` |  |
| `DB_BACKUP_MAX_TOTAL_GB` | float | `12.0` | F2 (local dir only) |
| `DB_BACKUP_MIN_FREE_FACTOR` | float | `1.3` |  |
| `DB_BACKUP_KEEP_UNCOMPRESSED` | int | `1` | F2 |
| `DB_BACKUP_MANUAL_RETENTION` | int | `5` |  |
| `DB_BACKUP_MIRROR_MARKER` | str | `".taosentinel-mirror"` |  |
| `DB_BACKUP_MIRROR_MAX_TOTAL_GB` | float | `0.0` | F2 (separate key; 0 = no budget) |
| `DB_BACKUP_STARTUP_DELAY_SEC` | int | `600` |  |
| `DB_BACKUP_WAIT_FOR_JOBS` | str | `"storage_retention_daily"` |  |
| `DB_BACKUP_WAIT_MAX_SEC` | int | `1200` |  |
| `DB_BACKUP_STALE_HOURS` | float | `36.0` | F2 |
| `DB_QUICK_CHECK_ON_UNCLEAN` | bool | `True` |  |
| `DB_QUICK_CHECK_DELAY_SEC` | int | `300` |  |
| `DB_WAL_WARN_MB` | int | `1024` |  |
| `DISK_FREE_CRITICAL_GB` | float | `0.5` |  |
| `OPS_GUARD_ENABLED` | bool | `True` |  |
| `OPS_GUARD_INTERVAL_MIN` | int | `5` |  |
| `OPS_DIGEST_ENABLED` | bool | `True` |  |
| `OPS_DIGEST_HOUR_UTC` | int | `9` |  |
| `OPS_DIGEST_MINUTE` | int | `0` |  |
| `DEADMAN_ENABLED` | bool | `False` |  |
| `DEADMAN_PING_URL` | str | `""` | secret (masked) |
| `WATCHDOG_API_SELF_PROBE_ENABLED` | bool | `True` |  |
| `WATCHDOG_API_PROBE_FAILS` | int | `3` |  |
| `COST_LEDGER_ENABLED` | bool | `True` |  |
| `COST_LEDGER_INTERVAL_MIN` | int | `60` |  |
| `COST_SOFT_ALERT_PCT` | float | `0.8` |  |
| `COST_HARD_STOP_PCT` | float | `1.0` |  |
| `COST_RESERVATION_TTL_SEC` | int | `86400` |  |
| `COST_OVERRIDE_MAX_USD` | float | `100.0` |  |
| `PAT_EXPIRY_WARN_DAYS` | int | `14` |  |
| `SECURITY_SELFCHECK_ENABLED` | bool | `True` |  |
| `SCRIPT_TMC_RATE_PER_MIN` | int | `2` |  |
| `SCRIPT_RATE_SHARE` | float | `0.25` |  |

**Backfill (17)** — 49 rows

| key | type | default | note |
|---|---|---|---|
| `BACKFILL_ENABLED` | bool | `True` |  |
| `BACKFILL_DIR` | path | `"data/backfill"` |  |
| `BACKFILL_ALERTS_ENABLED` | bool | `True` |  |
| `BACKFILL_MIN_FREE_GB` | float | `3.0` |  |
| `LEDGER_SEED_PATH` | path | `"seed/ledger.db"` |  |
| `LEDGER_BACKFILL_SEED_PATH` | path | `"seed/ledger_backfill.db"` |  |
| `LEDGER_PI_SEED_PATH` | path | `"seed/pi/ledger.db"` |  |
| `LEDGER_EXTRA_SEED_PATHS` | list[path] | `[]` |  |
| `LEDGER_NRA_TRUST_FROM_TS` | int | `1780182493` |  |
| `LEDGER_LIVE_FROM_TS` | int | `1777468595` |  |
| `LEDGER_IMPORT_THIN_SEC` | int | `0` |  |
| `LEDGER_IMPORT_BATCH_ROWS_ONLINE` | int | `20000` |  |
| `LEDGER_EXPECTED_ROWS` / `LEDGER_EXPECTED_NETUIDS` / `LEDGER_BACKFILL_EXPECTED_MERGE` | int | `7362854` / `129` / `23237` |  |
| `WHALE_COHORT_SEED_PATH` / `WHALE_LABELS_SEED_PATH` | path | `"seed/whale_cohort.json"` / `"seed/whale_labels.json"` |  |
| `ARCHIVE_WS_URL` | str | `"wss://archive.chain.opentensor.ai:443"` |  |
| `ARCHIVE_RATE_PER_SEC` | float | `2.0` |  |
| `ARCHIVE_MIN_BUCKET_INTERVAL_SEC` | float | `1.5` |  |
| `ARCHIVE_BACKOFF_MAX_SEC` | int | `120` |  |
| `BACKFILL_RPC_ENDPOINTS` | list[str] | `[]` ⇒ derived: keyed OnFinality (if key), `ONFINALITY_W…` (full: owner §5) |  |
| `BACKFILL_RPC_ERAS` | list[str] | `["gap","ledger","pre_ledger"]` |  |
| `BACKFILL_RPC_STEP_SEC` | int | `3600` |  |
| `BACKFILL_RPC_SAMPLE_OFFSET_SEC` | int | `60` |  |
| `BACKFILL_RPC_MIN_NETUIDS_PER_HOUR` | int | `100` |  |
| `BACKFILL_RPC_PUBLIC_MAX_RPS` | float | `4.0` |  |
| `BACKFILL_RPC_CONCURRENCY` | int | `0` (auto: 4 keyed OnFinality, 1 otherwise) |  |
| `BACKFILL_RPC_MAX_ATTEMPTS` | int | `6` |  |
| `BACKFILL_RPC_TIMEOUT_SEC` | float | `45` |  |
| `BACKFILL_RPC_PRICE_METHOD` | enum | `"auto"` |  |
| `BACKFILL_RPC_MODE` | enum | `"runtime_api"` |  |
| `DTAO_LAUNCH_BLOCK` / `LEDGER_START_BLOCK` | int | `4920351` / `6502594` |  |
| `BACKFILL_TMC_ENABLED` | bool | `True` |  |
| `BACKFILL_TMC_TIMEOUT_SEC` | int | `14400` |  |
| `BACKFILL_TMC_MAX_PER_HOUR` | int | `180` | F2 |
| `BACKFILL_TMC_QUIET_UTC` | str | `"02:05-02:45,20:25-23:35"` | F2 |
| `BACKFILL_TMC_CANDLE_TIMEOUT_SEC` | int | `90` |  |
| `BACKFILL_TMC_CANDLE_CHUNK_DAYS` | int | `30` |  |
| `BACKFILL_TMC_MIN_5M_PER_1H` / `BACKFILL_TMC_MIN_5M_PER_1D` | int | `6` / `144` |  |
| `PI_TRADER_COLDKEY` | str | `""` |  |
| `BACKFILL_GITHUB_ENABLED` | bool | `True` |  |
| `BACKFILL_GITHUB_SINCE` | str | `"2025-02-13"` |  |
| `BACKFILL_GITHUB_MAX_PAGES_PER_REPO` | int | `50` |  |
| `BACKFILL_GITHUB_MAX_REPOS_PER_OCCUPANT` | int | `30` |  |
| `BACKFILL_GITHUB_PRS_ENABLED` | bool | `True` |  |
| `BACKFILL_GITHUB_PR_MAX_PER_REPO` | int | `200` |  |
| `BACKFILL_GITHUB_RESERVE_REMAINING` | int | `1500` |  |
| `BACKFILL_GITHUB_TIMEOUT_SEC` | int | `14400` |  |
| `BACKFILL_GITHUB_WINDOW_UTC` | str | `"00:30-08:00"` |  |
| `REGIME_BACKFILL_START` | str | `""` (auto) |  |
| `COVERAGE_PATH` | path | `"data/quant/coverage.json"` |  |
**Investment workbench (19)** — 22 rows *(added 2026-10-16; full semantics: 19 §4)*

| key | type | default | note |
|---|---|---|---|
| `WB_ENABLED` | bool | `True` | registers the `wb_*` jobs, `chain_constants_probe` and `/api/v1/wb/*` |
| `WB_EMA_PERIOD` | int | `72` | ≥ 2 |
| `WB_EMA_WINDOW_BARS` | int | `432` | ≥ 2 × `WB_EMA_PERIOD` |
| `WB_EMA_MIN_BARS` | int | `72` | 2..window |
| `WB_EMA_TIMEFRAMES` | list[str] | `["1h","1d"]` | subset of {1h, 1d} |
| `WB_EMA_BAND_MODE` | str | `"sigma"` | only `sigma` in v1 |
| `WB_EMA_BAND_K` | float | `2.0` | 0.1..10 |
| `WB_STALE_BARS_1H` | int | `3` |  |
| `WB_DEREG_HORIZONS_DAYS` | list[int] | `[30, 90]` | subset of {30, 90} |
| `WB_DEREG_REG_GAP_WINDOW_DAYS` | int | `90` | 7..365 |
| `WB_DEREG_REG_GAP_FALLBACK_DAYS` | float | `8.0` | > 0 |
| `WB_CHAIN_PROBE_URL` | str | `"wss://entrypoint-finney.opentensor.ai:443"` | keyless; called over HTTPS JSON-RPC |
| `WB_CHAIN_PROBE_TIMEOUT_SEC` | int | `20` | per RPC step |
| `WB_GITHUB_PROBE_ENABLED` | bool | `True` | runs only while `GITHUB_TOKEN` is empty |
| `WB_GITHUB_PROBE_PER_RUN` | int | `45` | 1..50 |
| `WB_VISION_ENABLED` | bool | `False` | also needs `ANTHROPIC_API_KEY` |
| `WB_VISION_MODEL` | str | `"claude-opus-5-5"` |  |
| `WB_VISION_EST_USD` | float | `0.10` | governor reservation per read |
| `WB_VISION_MAX_IMAGE_MB` | int | `5` | 1..20 |
| `WB_UPLOAD_DIR` | path | `"data/wb/uploads"` |  |
| `WB_EVENTS_ENABLED` | bool | `False` | `ema72_band_exit` |
| `WB_STUDY_IMMUNITY_BLOCKS` | int | `864000` | historical rule for 19 §2.8 |

**Retired keys** (must not be added back; an old `.env` line is ignored by `extra="ignore"`, except the aliases, which still load):

| retired | replacement | by |
|---|---|---|
| `REGISTRY_INTERVAL_MIN` (01 placeholder) | 04's `REGISTRY_OCCUPANTS_INTERVAL_MIN`, `REGISTRY_IDENTITY_AT_UTC` | E2 |
| `CHAIN_REST_ENABLED`/`_INTERVAL_MIN`, `CHAIN_HOLDERS_ENABLED`/`_INTERVAL_MIN`, `CHAIN_RPC_ENABLED`/`_INTERVAL_MIN`, `CHAIN_TMC_ENABLED`, `TMC_USER_AGENT`, `GATE_SNAPSHOT_HOUR_UTC`, `DEREG_ALERT_ENABLED`, `SUBTENSOR_RPC_INTERVAL_MIN`, `SUBTENSOR_METAGRAPH_INTERVAL_MIN` | 05's `TMC_*_ENABLED`/`*_AT_UTC`, `SUBTENSOR_RPC_ENABLED`, `HTTP_USER_AGENT` | 05, E2 |
| `SSE_MAX_CONNECTIONS` | `TMC_SSE_MAX_CONNECTIONS` | F2 |
| `TMC_RATE_LIMIT_PER_MIN`, `TMC_RATE_LIMIT_PER_HOUR`, `TMC_PAGE_SIZE` (04 draft) | `TMC_RATE_PER_MIN`, `TMC_HOURLY_CAP`, `TMC_SUBNETS_PAGE_LIMIT` (05) | F2 |
| `GITHUB_REPOS_INTERVAL_MIN`, `GITHUB_ORG_INTERVAL_MIN`, `GITHUB_RESOLVE_INTERVAL_HOURS`, `GITHUB_USER_AGENT`, `GITHUB_BUCKET_BURST`, `GITHUB_JOB_MAX_RUNTIME_SEC`, `GITHUB_ALERT_AFTER_FAILURES` | `GITHUB_RESOLVE_INTERVAL_MIN` + 06 crons, `HTTP_USER_AGENT`, burst 10 (§6), per-job timeouts, `JOB_FAIL_ALERT_THRESHOLD` | 06 |
| `WEB_INTERVAL_MIN`, `WEB_SPN_ENABLED`, `WAYBACK_ENABLED`, `WAYBACK_RATE_PER_S`, `WEB_METRICS_CRON_UTC`, `WEB_ALERT_AFTER_FAILURES` | `WEB_TICK_MINUTES`, `WEB_SPN2_ENABLED`, `WEB_WAYBACK_ENABLED`, `WAYBACK_RATE_PER_SEC`, `WEB_METRICS_CRON_HOURS`/`_MINUTE` | 07 |
| `X_TEAMS_INTERVAL_MIN`, `X_KOLS_INTERVAL_MIN`, `X_KOL_POLL_INTERVAL_HOURS`, `X_CTF_CROSSCHECK_DAY`, `X_RATE_LIMIT_PER_MIN`, `X_ALERT_AFTER_FAILURES`, `RSS_INTERVAL_MIN`, `RSS_RATE_LIMIT_PER_MIN`, `GOOGLE_NEWS_RATE_LIMIT_PER_MIN`, `YOUTUBE_RATE_LIMIT_PER_MIN`, `NEWS_ENABLED` | 09's crons, `X_RATE_PER_SEC`, `X_OFFICIAL_RATE_PER_SEC`, `RSS_RATE_PER_SEC`, `GOOGLE_NEWS_MIN_INTERVAL_SEC`, `YOUTUBE_RATE_PER_SEC`, `RSS_GOOGLE_NEWS_ENABLED` | 09 |
| `LLM_INTERVAL_MIN`, `LLM_BATCH_SIZE`, `LLM_CLASSIFIER_VERSION`, `ANTHROPIC_RPM` | `LLM_BATCH_INTERVAL_MIN`/`_OFFSET_MIN`, `LLM_BATCH_ITEMS_PER_REQUEST`, the computed `classifier_version`, `LLM_RATE_PER_SEC` | 10 |
| `TELEGRAM_ALERTS_ENABLED` | `ALERTS_ENABLED` (dispatcher) + `TELEGRAM_ENABLED` (all sends) | 14 |
| `STREAM_REPLAY_BUFFER`, `ALERT_DEDUPE_DEFAULT_MIN`, `API_HOST`/`API_PORT` as keys | `STREAM_REPLAY_MAX`, 03's `dedupe_key`; aliases of `HEALTH_HOST`/`HEALTH_PORT` | 14 |
| `WATCHDOG_LIVENESS_MAX_AGE_SEC`, `SHUTDOWN_CHAIN_TIMEOUT_SEC` | per-job liveness (16 §2.5); `SHUTDOWN_HTTP_TIMEOUT_SEC` | 01 |
| `X_KILL_SWITCH_PATH`, `GITHUB_KILL_SWITCH_PATH`, `COMMUNITY_KILL_SWITCH_PATH`, `WEB_KILL_SENTINEL_PATH`, `TMC_PAUSE_SENTINEL`, `QUANT_KILL_SENTINEL`, `BACKFILL_PAUSE_PATH` | `control_state` pauses; emergency `data/control/<target>.paused` | B12 (amended) |
| `SOCIAL_TEXT_HARD_MAX_DAYS` | `LLM_TEXT_RETENTION_DAYS` + `LLM_PENDING_TEXT_MAX_DAYS` | 03, B10 |
| `TAOSTATS_MONTHLY_CALL_CAP` | alias of `TAOSTATS_MONTHLY_CAP_REQ` (still loads, warns) | F2 |
| `SCHEDULER_MODE` (named by 04, never defined) | none: triggers are always the owner's (tests call `run-job`) | 01 |

**Kill-switch / pause semantics (B12, 16 §2.11).** `KILL_SWITCH_PATH` exists → every `paid=True` job skips `paused:kill_switch` and the governor denies paid groups; free jobs and free LongTasks keep running; `/health.kill_switch_active=true`. `PAUSE_ALL_PATH` exists (or 16's integrity hold) → every non-`ops_exempt` job and LongTask skips `paused:pause_all`. `SCORING_KILL_SWITCH_PATH` exists → `scoring_6h` computes but does not publish; `/health.scoring_killed=true`; the feed serves last good with `stale=true`. Per-target pauses live only in `control_state` (exact ids or 16's `PAUSE_GROUPS`, never prefixes); `data/control/<target>.paused` is an emergency input the app reads and never writes (`paused:emergency_file`). Budget pauses clear at the window reset, a raised cap + `reload-config`, or a 16 override.

---

## 6. Cadence, rate limits, cost

**Provider limiter table (`ProviderLimiter.from_settings()`; one row per G1 provider id, same strings in `provider_spend.provider`, 16's governor and `/health`).** Rates are the owners' keys (§5); a loopback endpoint is never limited (§2.12 rule 1). "Group" = 16's spend group (G1/G5).

| provider | rate (key = default) | burst | hourly cap | monthly cap | group / owner note |
|---|---|---|---|---|---|
| `tmc` | `TMC_RATE_PER_MIN`/60 = 8/min | `TMC_RATE_BURST` = 2 | `TMC_HOURLY_CAP` = 500 | `TMC_MONTHLY_CAP` = 90,000 | 16's request-only `tmc` group; anonymous per-IP limits 10/60 s, 600/h, 100k/month (F2, G3; 05 owns the budget table) |
| `tmc_sse` | no bucket: ≤ `TMC_SSE_MAX_CONNECTIONS` = 2 open streams | — | — | — | unmetered (`X-RateLimit-Limit: 0`); 05 |
| `subnetalpha` | `SUBNETALPHA_RATE_PER_SEC` = 1.0/s | 1 | — | — | 04 (WP-REST) + 09 `x_ctf_crosscheck` |
| `subtensor_rpc` | `SUBTENSOR_RPC_RATE_PER_SEC` = 2.0/s | 4 | — | — | 04 JSON-RPC, 05 optional RPC jobs (a local lite node is loopback → unlimited) |
| `onfinality` | `ONFINALITY_RATE_PER_SEC` = 20/s (clamped 4.0/s without key) | 20 (4 keyless) | — | — | 05 endpoint failover, 17 backfill (40 rps keyed / 5 rps public, UNVERIFIED) |
| `blockmachine` | `BLOCKMACHINE_RATE_PER_SEC` = 12/s | 8 | — | `BLOCKMACHINE_MONTHLY_CAP_REQ` 4.5 M | 17 backfill primary archive (added 2026-09-24, BUILD-LOG Phase 3) |
| `opentensor_archive` | `ARCHIVE_RATE_PER_SEC` = 2.0/s | 4 | — | — | 17 backfill (`ARCHIVE_MIN_BUCKET_INTERVAL_SEC` pacing inside 17) |
| `github` | `GITHUB_RATE_PER_HOUR`/3600 = 4,000/h | 10 | `GITHUB_RATE_PER_HOUR` + 500 = 4,500 | — | PAT limit 5,000/h (06's `GitHubQuota` also reads `x-ratelimit-*`); no token → rate 50/h, cap 60/h |
| `github_html` | 1/`GITHUB_HTML_PROBE_PACING_SEC` = 1.0/s (06: pacing ≥ 0.8 s) | 1 | — | — | 06 `github_html_probe`; zero PAT quota |
| `web` | `WEB_RATE_PER_SEC` = 1.0/s | `WEB_RATE_BURST` = 2 | — | — | 07; per-host `Retry-After` cooldown (§2.12) + 07's `WEB_SAME_HOST_GAP_S` |
| `wayback` | `WAYBACK_RATE_PER_SEC` = 0.5/s | 1 | — | — | 07 Availability/CDX and SPN2 (07 adds `WEB_SPN2_MIN_INTERVAL_S` spacing) |
| `discord` | `DISCORD_RATE_PER_SEC` = 1.0/s | 1 | 200 | — | 08; 403s count toward Discord's invalid-request ban (10,000/10 min) |
| `tme` | `TELEGRAM_COUNTS_RATE_PER_SEC` = 0.5/s | 1 | — | — | 08 `t.me` pages |
| `rss` | `RSS_RATE_PER_SEC` = 0.5/s | 2 | — | — | 09 Appendix A (Google News host spacing `GOOGLE_NEWS_MIN_INTERVAL_SEC` inside 09) |
| `youtube` | `YOUTUBE_RATE_PER_SEC` = 0.33/s | 1 | — | — | 09 Appendix A |
| `twitterapi_io` | `X_RATE_PER_SEC` = 1.0/s | 2 | — | USD via group `x` (`X_MONTHLY_CAP_USD` = 25) | 09 |
| `official_v2` | `X_OFFICIAL_RATE_PER_SEC` = 0.25/s | 2 | — | USD via group `x` (75 with official, checklist C2) | 09; `x-rate-limit-*` honoured |
| `anthropic` | `LLM_RATE_PER_SEC` = 0.5/s | 2 | — | USD via group `llm` (`LLM_MONTHLY_CAP_USD` = 60) | 10; SDK calls acquire/count manually (§2.12 rule 8) |
| `taostats` | `TAOSTATS_RATE_PER_MIN`/60 = 5/min | 1 | — | `TAOSTATS_MONTHLY_CAP_REQ` = 10,000 | group `taostats`; 11 (only when keyed) |
| `rufus` | 1.0/s (fixed, 11) | 1 | — | — | 11 S3 conviction table (2 req/day) |
| `subnetradar` | 0.2/s (fixed, 11) | 1 | — | — | 11 swaps page (1 req/day) |
| `telegram` | `TELEGRAM_MAX_MSG_PER_MIN`/60 = 20/min | 5 | — | — | Bot API sends + long-poll (14, 01's `send_alert`) |
| `deadman` *(internal, not in G1)* | 1 per `OPS_GUARD_INTERVAL_MIN` | 1 | — | — | 16's dead-man ping; `book_spend=False` |

`ProviderLimiter.acquire()` raises `KeyError` for an id not in this table (fail loud; no silent unlimited provider). Sharing rules: one `ProviderLimiter` per process (on `Runtime`); no module constructs its own `httpx.AsyncClient`; scripts next to the service go through 16's `scripts/with_bucket.py` (reduced TMC share, `SCRIPT_TMC_RATE_PER_MIN`); 429 penalties are shared through 16's penalty files and monthly counts through `provider_spend.requests` increments, reseeded at boot so a restart never resets the TMC month.

**Phase-0 budget:** zero external spend. Telegram: ≤ 1 message at boot + alerts + 16's daily digest (≪ 20 msg/min). Local: `storage_retention_daily` (03 §6) and `backup_daily` (16 §2.7) are the heavy I/O; `log_retention_daily`, `cost_ledger_hourly`, `ops_guard_5m` and `alert_dispatch` are sub-second. Steady-state process RSS target ≤ 300 MB (master §3; 16's unit caps at `MemoryMax=2G`).

---

## 7. Failure modes and fail-safe behaviour

| failure | detection | behaviour | `/health` | alert |
|---|---|---|---|---|
| `.env` invalid (validator error) | `Settings()` raises at import | process exits non-zero with the pydantic message; systemd `Restart=on-failure` backs off (`StartLimitBurst=5` in 60 s, then 16's `OnFailure=` notifier) | — | notifier (16) |
| Second instance started by hand | 16 `pre_init` instance lock | `sys.exit(3)` before touching the DB | — | none |
| DB locked / corrupt at boot, migration fails | `connect()` / `apply_schema()` (03 `migrate`) raise | boot fails loudly (`check` reproduces); never auto-restore (16 runbook) | — | journal / notifier |
| DB write fails inside the wrapper | try/except around every wrapper write | job result still recorded in memory; `logger.warning("collector_runs write failed")` | status unchanged | none |
| `emit_event` fails in `AlertLatch.fire` (DB error, unknown type) | exception | direct `send_alert(text)` (D3 fallback) | — | the text itself |
| Job raises / times out | wrapper | `ok=0`, `consecutive_failures++`, next trigger retries | `last_error`, `collector_failing:<id>` at the threshold | `collector_failing` once, `collector_recovered` once |
| Provider 429 | `Http.request` | per-host cooldown for `Retry-After`, retry ≤ `HTTP_MAX_RETRIES`; then `ProviderError` (chained) → job failure | `rate_limits.<p>.penalized_until` | via job failure |
| Hourly/monthly request cap | `ProviderLimiter.acquire` raises `BudgetExhausted` before sending | run ends `ok=1, note=paused:budget …` (not a failure); later runs skip `paused:budget` until the window resets (G1) | `paused="budget"`, `rate_limits.<p>.exhausted_until`, `budget_paused:<group>` when 16's group is stopped | 16's `spend_cap_reached` (request groups) |
| USD cap / cap = 0 | 16 governor (`check`/`reserve`, `is_exhausted`) | paid jobs of that group skip `paused:budget`; cap 0 = disabled (F2) | `spend.<g>.paused` | 16's `spend_80pct` / `spend_cap_reached` |
| `KILL_SWITCH` / `PAUSE_ALL` present | `os.path.exists` each run (16 `effective_pause`) | paid jobs / every non-`ops_exempt` job skip; free LongTasks keep running under `KILL_SWITCH` | `kill_switch_active` / `pause_all_active`, `pause_all` reason | none |
| Event loop wedged, scheduler stopped firing, API task died | `stale_ids()` (§2.8) or the loop cannot run at all | watchdog ping withheld → systemd restarts within `WatchdogSec=120` | `job_stale:<id>` while still reachable | journal `watchdog_withheld` (counted by 16's digest) |
| Telegram unreachable | `send_alert` → `False` | dispatcher leaves rows `NULL` and retries next tick; after `ALERT_LATE_AFTER_HOURS` they become `digest_late` (14) | `alerts.pending_immediate`, `services.telegram.ok=false` | — |
| `alert_dispatch` disabled, paused or stalled | `dispatcher_active()` false | `AlertLatch` inserts rows with `alerted_at` set and sends direct (16 rule c) | `alerts.dispatcher_active=false` | direct |
| Backup fails / stale / mirror missing | 16's `BackupJob`, `ops_guard_5m` | 16 §2.7, §2.13 | `backup_stale`, `mirror_missing` | 16's events |
| Disk below `DISK_FREE_MIN_GB` / `DISK_FREE_CRITICAL_GB` | `shutil.disk_usage(DATA_DIR)` (`/health`, 16's guard) | informational; backups still attempted (16 preflight) | `disk_low` / `disk_critical` | 16's `disk_low` / `disk_critical` |
| SIGTERM during a long job | `shutdown_services` | scheduler stops (no new runs), LongTasks cancelled (1 s), cost flush (1 s), DB close ≤ 5 s, HTTP close ≤ 3 s, heartbeat removed, `_hard_exit(0)` ≤ 10 s after the ≤ 3-s uvicorn drain (< `TimeoutStopSec=30`); the running job's row stays an orphan | — | none |
| uvicorn `capture_signals` renamed | `hasattr` guard | `logger.error` at boot; `test_runtime_shutdown.py` and verify step 16 catch it | — | — |
| Power loss mid-run | orphan `collector_runs` row (`finished_at NULL`); 16 boot classification | next boot: interval jobs re-run after the stagger unless `recent`; cron jobs with `startup_catchup` run once at boot + 300 s when their last scheduled fire was missed (E1); others wait for their next fire; collectors are watermark-based | `orphan_runs` in `/api/v1/jobs`, `ops.boot_kind` | 16's `service_recovered_unclean` |
| Laptop clock jumps (NTP after boot) | unit `After=time-sync.target` (16) | APScheduler uses wall-clock; `coalesce=True` collapses missed runs; liveness uses unix seconds and tolerates one tick | — | — |

**Last-good semantics:** the runtime keeps no cached data itself; `scores` last-good is 12/14/15's. `/health` is always servable: `status` depends on DB + scheduler (and `degraded_reasons`); every block degrades to `{"error": …}` instead of failing the endpoint.

---

## 8. Implementation plan

Files (all new unless "port"; old paths are under `E:\SN_Trader\SubnetTrader\`):

| file | content | port from |
|---|---|---|
| `app/__main__.py` | `argparse` sub-commands (§2.14) → `app.runtime.entrypoint(argv)`; `register(subparsers)` hooks | old `app/__main__.py:1-5` idea |
| `app/config.py` | `APP_ROOT`, `Settings` (§5, aliases, `SCORING_SPEC_VERSION` property), validators, `settings`, `get_settings`, `reload_settings` | `config.py:132-137` (model config), `490-510`, `542-547`, `578-579`, `603-617` (key conventions), `637-644` (`validate_log_level`), `664-670` (model-validator idiom), `685` (singleton) |
| `app/util/time.py` | B1 helpers (signatures below) | old `app/utils/time.py:1-57` (`utc_now`, `parse_iso` idea; `parse_iso` now returns `int`) |
| `app/util/mathutil.py` | A2: the only math-util module; imported by 05, 11, 12, 13 | old `app/utils/math.py:1-177` verbatim (below) + `monotone_score` for 13 |
| `app/util/paths.py` | `resolve(rel) -> Path` (= `settings.path`), `ensure_dirs()` (creates `data/logs`, `data/backups`, `data/run` when run outside systemd) | — |
| `app/logging/logger.py` | `JsonlFileHandler`, `StructuredLogger(+set_level)`, `logger` | `logging/logger.py:1-161` verbatim + 3 edits (§2.10) |
| `app/logging/retention.py` | `prune_old_logs() -> int`, `LogRetentionJob(CollectorJob)` | `main.py:1256-1291` verbatim (+ return count) |
| `app/api_auth.py` | `require_auth` | `api_auth.py:1-36` verbatim |
| `app/notifications/telegram.py` | `send_alert` (§2.11 changes), `TelegramDocument`, `TelegramCommandHandlers`, `TelegramBot` | `notifications/telegram.py:1-545` (`send_alert` 97-111) |
| `app/notifications/alerts.py` | `AlertLatch`, `dispatcher_active` | `main.py` 83 + 1099-1146 (latch pattern) |
| `app/alerts/dispatch.py` | `AlertDispatchJob` (§2.11) | — (14's `routing.py`/`templates.py` are data files shipped with it) |
| `app/ratelimit.py` | `BudgetExhausted`, `ProviderLimit`, `TokenBucket`, `ProviderLimiter` (+ 16's penalty-file sharing) | — |
| `app/http.py` | `Http`, `ProviderError`, `get_http` | — |
| `app/jobs.py` | `JobOutcome`, `RunContext`, `CollectorJob`, `LongTask`, `JobSpec`, trigger helpers, `JOB_TABLE`, `JobSpecMismatch`, `JobRegistry`, `setup_scheduler`, `previous_fire_time`, `stale_ids`, `_jobs_live` | `main.py:1294-1366` (never-raise wrapper), `1369-1578` (`add_job` shape), `3323-3361` (liveness) |
| `app/runtime.py` | `Runtime`, `runtime`, `get_runtime`, `init_services`, `run_startup_backup`, `_hard_exit`, `_arm_shutdown_deadline`, `_close_within`, `shutdown_services`, `run_health_server`, `sd_notify`, `systemd_watchdog_loop`, `main`, `entrypoint` | `main.py:359-447, 3232-3296, 3308-3320, 3364-3382, 3385-3486` (§2.5 table) |
| `app/api/app.py` | `create_app()`, CORS, router mounting, `configure_all(get_runtime)` | `main.py:2144-2228` |
| `app/api/health.py` | `build_health(rt)`, `GET /health`, `GET /api/health/services`, `GET /api/v1/jobs`, `configure(get_runtime)` | `main.py:2230-2441` (shape, never-500 idiom) |
| `app/api/control.py` | authed router: pause/resume (16's `app/control.py`), run, reload-config | `main.py:2480-2490` idea (the old routes wrote the kill-switch file; the new ones never write files, B12) |
| `Makefile` | `venv install install-dev check jobs run test smoke lint logs status` (backup/systemd targets call 16's CLI and installer) | — |
| `start.sh` | `cd "$(dirname "$0")"; . .venv/bin/activate; export TAOSENTINEL_ROOT="$PWD"; exec python -u -m app serve` | `start.sh` (drop port-kill/nohup) |
| `.env.example`, `.gitignore` (`.env .venv/ data/ seed/* !seed/*.example KILL_SWITCH PAUSE_ALL SCORING_KILLED __pycache__/`) | every §5 key with default + comment | — |

Owned elsewhere but Phase 0: `app/storage/*` (03), `app/costs.py`, `app/control.py`, `app/ops/*`, `app/storage/backup.py`, `ops/systemd/*`, installer (16).

```python
# app/util/time.py — B1 (all stored instants are INTEGER unix seconds UTC)
def now_ts() -> int                                  # int(time.time())
def utc_now() -> datetime                            # aware UTC (logging, C5 producer fns)
def floor_hour(ts: int) -> int                       # ts - ts % 3600
def floor_day(ts: int) -> int                        # ts - ts % 86400
def to_iso(ts: int | float | None) -> str | None     # "2026-09-23T10:00:00Z" (seconds precision); None → None
def parse_iso(s: str) -> int                         # accepts "…Z", "+00:00" and naive (= UTC); returns int
def parse_ts(x: int | float | str | datetime) -> int # int/float unix s, ISO string, or aware datetime
def parse_dt(x: int | float | str | datetime) -> datetime   # aware UTC; for ported code that calls .timestamp() (12's regime port)
def month_key(ts: int) -> str                        # "YYYY-MM" (UTC) — provider_spend.month
def hhmm(s: str) -> tuple[int, int]                  # "02:10" → (2, 10); ValueError on bad input (config validators, trigger helpers)
def dow_hhmm(s: str) -> tuple[str, int, int]         # "sun 05:00" → ("sun", 5, 0)

# app/util/mathutil.py — A2: verbatim copy of old app/utils/math.py lines 1-177 (the unused `datetime` import on line 6 may be dropped)
def pearson_r(xs: list[float], ys: list[float]) -> float              # old 9
def _average_ranks(xs: list[float]) -> list[float]                     # old 28
def spearman_r(xs: list[float], ys: list[float]) -> float             # old 45
def zscore(xs: list[float]) -> list[float]                             # old 57
def quantile_buckets(values: list[float], n: int = 10) -> list[int]    # old 73
def log_returns(prices: list[float]) -> list[float]                    # old 91
def returns_pearson_r(xs: list[float], ys: list[float]) -> float      # old 100
def effective_bets(avg_corr: float, n: int) -> float                   # old 118
def rolling_volatility(prices: list[float], window: int = 24) -> float | None   # old 134
def gini_coefficient(values: list[float]) -> float                     # old 160-177 (05's holder concentration)
def monotone_score(curve: list[float | None]) -> float                 # new for 13: spearman_r(range(n), curve) over the
                                                                       #   non-None entries; n < 5 → 0.0
```
`app/quant/mathutil.py` must not exist (A2): 05 §2.6/§8, 12 §1/§8 and 13 §8.1 import `app.util.mathutil` (reported to the coordinator). Old `compute_price_changes` (180+) is not ported.

Step order:
1. Layout + `pyproject.toml` + venv; `make install-dev`; `python -c "import app"`; `tests/test_deps.py`.
2. `util/time.py`, `util/mathutil.py`, `config.py` (full inventory; validators; aliases) → `test_time.py`, `test_mathutil.py`, `test_config.py` green.
3. `logging/` (verbatim + `set_level`) → `test_logging.py`.
4. Storage with 03: `Database`, `migrate`, `emit_event`, `models.sql`; 16's `control.py` and `costs.py` stubs-to-full.
5. `ratelimit.py`, `http.py` → `test_ratelimit.py`, `test_http.py`.
6. `notifications/telegram.py` + `alerts.py`, `alerts/dispatch.py` with 14's `routing.py`/`templates.py` → `test_alerts.py`, `test_dispatch.py` (respx).
7. `jobs.py`: base classes, `JOB_TABLE`, registry, `setup_scheduler`, catch-up, liveness → `test_jobs.py`, `test_scheduler.py`, `test_liveness.py`.
8. Phase-0 jobs: `LogRetentionJob`, `AlertDispatchJob`, and the declared 03/16 rows (built by their owners).
9. `api_auth.py` (verbatim), `api/app.py`, `api/health.py`, `api/control.py` → `test_health_api.py`.
10. `runtime.py` (ports + `init_services`/`main`), `__main__.py` → `test_runtime_shutdown.py`, `test_cli.py`, `test_smoke.py`.
11. 16's units/installer, `Makefile`, `start.sh`, `.env.example`; run §10 on the laptop.

Key signatures not listed above:

```python
# app/runtime.py
async def init_services(rt: Runtime) -> None
async def run_startup_backup(rt: Runtime) -> None
async def shutdown_services(rt: Runtime) -> None
async def run_health_server(stop: asyncio.Event) -> None
def sd_notify(state: str) -> None
async def systemd_watchdog_loop(stop: asyncio.Event) -> None
async def main(args: argparse.Namespace) -> None
def entrypoint(argv: list[str] | None = None) -> None
# app/jobs.py
def setup_scheduler(rt: Runtime) -> AsyncIOScheduler
def stale_ids(rt: Runtime, now: int) -> list[str]
def _jobs_live() -> bool
# app/api/app.py, app/api/health.py
def create_app() -> FastAPI
def build_health(rt: Runtime | None) -> dict          # never raises; rt None → {"status": "initializing", …}
# app/logging/retention.py
def prune_old_logs() -> int
class LogRetentionJob(CollectorJob): async def collect(self, run: RunContext) -> JobOutcome   # rows = files removed
# app/alerts/dispatch.py
class AlertDispatchJob(CollectorJob): async def collect(self, run: RunContext) -> JobOutcome
```

---

## 9. Tests

`tests/conftest.py` (port of the old env-seeding idiom, `tests/conftest.py:1-17`, new keys), executed **before any `app` import**:

```python
import os, sys, tempfile
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
_tmp = tempfile.mkdtemp(prefix="taosentinel_test_")
os.environ.setdefault("TAOSENTINEL_ENV_FILE", os.path.join(_tmp, "absent.env"))   # never read the real .env
os.environ.setdefault("TAOSENTINEL_ROOT", _tmp)
os.environ.setdefault("DB_PATH", os.path.join(_tmp, "test.db"))
os.environ.setdefault("JSONL_DIR", os.path.join(_tmp, "logs"))
os.environ.setdefault("DB_BACKUP_DIR", os.path.join(_tmp, "backups"))
os.environ.setdefault("TELEGRAM_BOT_TOKEN", ""); os.environ.setdefault("TELEGRAM_CHAT_ID", "")
os.environ.setdefault("SYSTEMD_WATCHDOG_ENABLED", "false")
os.environ.setdefault("STARTUP_PRIME_ENABLED", "false")
os.environ.setdefault("DB_BACKUP_STARTUP_DELAY_SEC", "3600")   # no startup backup inside a test run
os.environ.setdefault("HEALTH_PORT", "0")                      # smoke test picks a free port itself
for k in ("REGISTRY", "TMC", "TMC_SSE", "GITHUB", "WEB", "DISCORD", "DISCORD_WIDGET", "DISCORD_GATEWAY",
          "TELEGRAM_COUNTS", "OWNER", "INTEGRITY_FEATURES", "TEAM_SEED", "FEATURES", "SCORING",
          "X", "RSS", "LLM", "DEREG_WATCH", "NEEDS_REVIEW_DIGEST", "BACKFILL_TMC", "BACKFILL_GITHUB", "DEADMAN"):
    os.environ.setdefault(f"{k}_ENABLED", "false")
```

Fixtures: `db` (fresh 03 `Database` on a `tmp_path` file, `apply_schema()` run), `rt` (a `Runtime` with `db`, `ProviderLimiter`, `Http`, 16's `CostGovernor`, `AlertLatch`, no scheduler), `alerts_sink` (monkeypatches `app.notifications.telegram.send_alert` to append to a list and return `True`), `respx_mock` (`respx.mock(assert_all_mocked=True, assert_all_called=False)` — any unmocked HTTP call fails the test), `fake_clock` (monkeypatches `time.monotonic`, `now_ts` and `asyncio.sleep`), `app_client` (`httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app()), base_url="http://t")`), `stub_module(name, cls)` (injects a fake factory module via `sys.modules` so later-phase rows become available).

| test file | cases |
|---|---|
| `test_deps.py` | `find_spec(m) is None` for `bittensor`, `bittensor_wallet`, `substrateinterface`; CI re-runs it after `pip install -e ".[dev,backfill]"` |
| `test_time.py` | `to_iso(1790121600) == "2026-09-23T00:00:00Z"`; `parse_iso` of `…Z`, `+00:00` and naive strings returns the same `int`; `parse_ts` of int/float/str/aware datetime; `parse_dt(x).tzinfo is UTC`; `floor_hour`/`floor_day`; `month_key`; `hhmm("24:00")` → `ValueError` |
| `test_mathutil.py` | the old expectations of `tests/test_factor_backtest.py:51-84` (`spearman_r`, `quantile_buckets`, `zscore`) and `tests/test_ema_manager.py:681-700` (`log_returns`, `returns_pearson_r`, `effective_bets`); `gini_coefficient([1,2,3,4]) == 0.25`, `[5,5,5] → 0.0`, `[] → 0.0`, non-positive values ignored; `monotone_score` on increasing / decreasing / < 5 points; `app.quant.mathutil` does not exist |
| `test_config.py` | defaults (`HEALTH_PORT==8090`, `SPEC_VERSION=="ts-1.0"`, `TMC_RATE_PER_MIN==8`, `TMC_RATE_BURST==2`, `TMC_HOURLY_CAP==500`, `TMC_MONTHLY_CAP==90000`, `X_MONTHLY_CAP_USD==25`, `LLM_MONTHLY_CAP_USD==60`, `CHAIN_TICKS_RETENTION_DAYS==30`, `CHAIN_TICKS_BLOCK_STRIDE==5`, `QUANT_WEEKLY_JOB_CRON=="sun 05:00"`, `BACKFILL_TMC_MAX_PER_HOUR==180`, `DISCORD_STORE_TEXT is True`, `TELEGRAM_POSTS_STORE_TEXT is False`, `LLM_PENDING_TEXT_MAX_DAYS==7`, `DB_BACKUP_MAX_TOTAL_GB==12`, `DB_BACKUP_STALE_HOURS==36`); `TMC_RATE_PER_MIN=9` + `TMC_RATE_BURST=2` → error (F2 sum ≤ 10); `SCORING_OFFICIAL_HOUR=6` → error; `SUBTENSOR_RPC_MODE=sdk` → error; `LOG_LEVEL="warn"` → error; aliases: `GITHUB_PAT`, `API_HOST`/`API_PORT`, `TAOSTATS_MONTHLY_CALL_CAP` (warning); `SCORING_SPEC_VERSION` follows `SPEC_VERSION` and cannot be set; `X_MONTHLY_CAP_USD=0` → `x_active()` False; the §5 inventory equals `Settings.model_fields` (the test parses this spec's §5 tables); `reload_settings()` rebinds and a consumer reading `app_config.settings` sees the change; **`test_no_static_settings_import`**; `public_dict()` masks every secret |
| `test_logging.py` | JSONL entry keys; `data=` payload; rotation when `MAX_BYTES` is monkeypatched to 1 KB creates `.1`; `prune_old_logs` deletes `2020-01-01.jsonl` and `.jsonl.3`, keeps today, returns 2 |
| `test_ratelimit.py` | bucket allows `burst` immediately then waits `1/rate`; `penalize(30)` blocks until fake clock +30; `penalize_host` delays only that host; hourly sliding window and monthly cap raise `BudgetExhausted` with the right `until`; `seed_month`; `exhausted()`; unknown provider → `KeyError`; every G1 id has a row (§6); `reload()` picks up a new rate |
| `test_http.py` (respx) | UA header; 429 then 200 → one retry + host penalty; `ok_statuses=(200, 429)` returns the 429 unretried; 503×3 → `ProviderError` whose `__cause__` is set; `ConnectError` → `ProviderError` chained; 404/301 returned without retry; body > `max_body_bytes` → truncated + `extensions["truncated"]`; `record(p, 0.0, requests=1)` once per wire attempt; query string and `bot<token>` redacted in log data; loopback URL skips the limiter |
| `test_alerts.py` | `send_alert` no-op (`False`) without token/`TELEGRAM_ENABLED`; with token posts `{chat_id, text, parse_mode:"HTML"}` via provider `telegram`; `fire(key, text)` without `event_type` sends directly and writes no row; with `event_type` writes a catalogued row (`payload.text`) and does not send while the dispatcher is active; dispatcher inactive → row with `alerted_at` set + direct send; `emit_event` raising → direct send; `direct` types send first; latch: second `fire` within `ALERT_REPEAT_MIN` is a no-op; `clear` delivers the recovery once |
| `test_dispatch.py` | seeded pending rows: immediate → one `sendMessage`, `alerted_at` + `payload.alert_route`; digest/none/`digest_late`/unknown type/`payload.backfilled` → marked without sending; > `ALERT_BATCH_THRESHOLD` → batched; 400 → plain-text retry then `alert_failed`; 5xx/network → nothing marked, tick ends; Telegram unconfigured → nothing sent or marked; restart re-reads `alerted_at IS NULL` only (exactly-once) |
| `test_jobs.py` | `FakeJob` variants: disabled → no run row; each B12 pause reason (via 16's `effective_pause`) → skip, no row; `ops_exempt` job ignores `PAUSE_ALL`/`KILL_SWITCH` but not an exact-id pause; kill switch pauses `paid=True` only; `BudgetExhausted` → `ok=1`, `note` starts `paused:budget`, `consecutive_failures` unchanged; success writes `ok=1, rows, meta_json`; raise → `ok=0`, **no exception escapes `run()`**; the threshold-th failure → exactly one `collector_failing` row with 03's required keys; per-job `fail_alert_threshold`/`fail_alert_key` honoured; success after → one `collector_recovered` row (`failures`, `down_seconds`); timeout → `ok=0`, error contains `timeout`; `CancelledError` propagates; `recent` skip only for scheduled interval runs; `in_flight` skip; cost reconciliation books only the extra with `requests=0`; DB failure injected into `execute` → run still completes |
| `test_scheduler.py` | `JOB_TABLE` has 53 unique ids (50 jobs + 3 LongTasks) and per-phase counts equal §2.7; every retired placeholder id is absent; with no later-phase modules `JobRegistry.build` yields exactly the 7 phase-0 jobs available; `setup_scheduler` triggers/misfire per row (parametrised; `stub_module` for later phases) — e.g. `scoring_6h` = `CronTrigger(hour="0,6,12,18", minute="15", timezone=UTC)`, `discord_6h` minute 0, `quant_weekly` Sunday 05:00; `manual` rows have `next_run_time is None`; stagger strictly increasing with `registry_occupants` first; startup catch-up: `last_ok` before the missed 02:10Z fire → `tmc_daily` at boot + 300 s, moved to 23:35Z when boot is 21:00Z; `owner_collect_daily` catch-up only inside 20:30–23:15Z; a class attribute differing from its row → `JobSpecMismatch` |
| `test_liveness.py` | all warm-up → live until one max-age after `started_at`, then not live; enabled job stale beyond `effective_liveness_max_age` → not live; disabled/paused/manual stale → live; kill switch does **not** force live; LongTask stale on `max(last_beat, last_attempt)`; `telegram_bot` (no max-age) never stale; `task_handles["health"].done()` → not live; `api_probe_fails ≥ 3` → not live; cron `interval_min()` = largest gap of the next 8 fires; the §2.7 *liveness* column equals the computed value for every row |
| `test_health_api.py` | `/health` has every top-level key of §2.9; `status` is `initializing` without DB/scheduler, `ok` with a clean `rt`, `degraded` with a reason; each `degraded_reasons` row of §2.9 triggered by its condition; `spend` equals `costs.snapshot()`; compact JSON contains `"status":"ok"`; a failing block yields `{"error": …}` and no 500; `/api/health/services.database.ok`; `/api/v1/jobs` lists the 57 declared rows; control routes: 401 without token when set, pause writes a `control_state` row (never `job_state`), group target never reaches `ops_exempt`, resume with an emergency file present returns `file_pause_active`, unknown target 404, `run` on a manual job starts it, `reload-config` returns `changed`/`restart_required` |
| `test_runtime_shutdown.py` | monkeypatch `app.runtime._hard_exit` and `_arm_shutdown_deadline` (old `tests/test_graceful_shutdown.py:25-31` idiom); `shutdown_services` flushes costs, closes DB and HTTP, removes the heartbeat and calls `_hard_exit(0)` once; a `db.close` sleeping 30 s is abandoned within `SHUTDOWN_DB_TIMEOUT_SEC` + 0.5; `run_health_server` replaces `capture_signals` and a SIGTERM sets the shutdown event |
| `test_cli.py` | `python -m app check` exit 0, prints `schema_version=1`, `sqlite_version` and the declared/available line; `jobs` prints 53 rows; `run-job log_retention_daily` exit 0; `run-job backfill_tmc` without its module → exit 1 `unavailable` |
| `test_smoke.py` (`@pytest.mark.smoke`) | start `main()` as a task with a free port; poll `/health` until `status=="ok"` (≤ 10 s); `READY=1` recorded (fake `NOTIFY_SOCKET`) before any job body ran; scheduler lists the 7 phase-0 jobs; with respx `sendMessage` mocked and a fake token, the `service_online` body contains "TaoSentinel online" and "7 available / 53 declared"; set the shutdown event; `_hard_exit(0)` within 12 s |

Mocked: all HTTP (respx), `send_alert` where not under test, `os._exit`/`_hard_exit`, the `threading.Timer` deadline, `NOTIFY_SOCKET`, `time.monotonic`/`now_ts` for buckets and liveness. Never mocked: SQLite (real file in `tmp_path`, 03's schema), the FastAPI app, APScheduler.

---

## 10. Verify checklist (laptop)

```bash
cd ~/taosentinel && python3.12 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]" && pip freeze > requirements.lock
cp .env.example .env            # optionally set TELEGRAM_BOT_TOKEN/CHAT_ID, API_AUTH_TOKEN=$(openssl rand -hex 32)
python -c "import bittensor" ; echo "expect: ModuleNotFoundError"
python -m app check             # expect: schema_version=1, schema_warnings=[], sqlite_version ≥ 3.37, and the last line
                                #   "OK jobs=53 declared (50 jobs + 3 LongTasks), 7 available (phase 0), 46 unavailable"; exit 0
python -m app jobs              # expect a 53-row table; available=yes exactly for log_retention_daily, storage_retention_daily,
                                #   backup_daily, ops_guard_5m, ops_digest_daily, cost_ledger_hourly, alert_dispatch
pytest -q                       # expect: all passed
pytest -q -m smoke              # expect: passed in < 45 s
make run &  PID=$!              # or: python -m app serve
sleep 5; curl -s localhost:8090/health | python -m json.tool | head -40   # "status": "ok", "scheduler": {"running": true, "declared": 53, "available": 7, …}
curl -s localhost:8090/health | grep -cE '"status":"(ok|degraded)"'     # expect 1 (16's deploy-gate contract, G6)
curl -s localhost:8090/api/health/services | python -m json.tool          # database.ok true, scheduler.ok true
curl -s localhost:8090/api/v1/jobs | python -c "import sys,json; print(len(json.load(sys.stdin)['jobs']))"   # 53
curl -s -X POST localhost:8090/api/v1/control/pause/backup_daily          # expect 401 if API_AUTH_TOKEN set
curl -s -X POST -H "X-Auth-Token: $API_AUTH_TOKEN" localhost:8090/api/v1/control/pause/backup_daily   # paused
sqlite3 data/taosentinel.db "select target, key, value, set_by from control_state"                    # backup_daily|paused|true|api
sqlite3 data/taosentinel.db "select count(*) from job_state where key='paused'"                       # 0 (B12: never job_state)
curl -s -X POST -H "X-Auth-Token: $API_AUTH_TOKEN" localhost:8090/api/v1/control/resume/backup_daily  # row deleted
curl -s -X POST -H "X-Auth-Token: $API_AUTH_TOKEN" localhost:8090/api/v1/control/run/log_retention_daily; sleep 2
sqlite3 data/taosentinel.db "select job, ok, rows, started_at, duration_sec from collector_runs order by started_at desc limit 5"   # ≥ 1 row, ok=1, started_at an integer
sqlite3 data/taosentinel.db "select version, name from schema_version"    # 1|baseline
touch KILL_SWITCH; curl -s localhost:8090/health | grep -o '"kill_switch_active":true'; rm KILL_SWITCH
touch PAUSE_ALL; sleep 70; curl -s localhost:8090/health | grep -o '"pause_all"'; rm PAUSE_ALL   # degraded_reasons has pause_all; ops jobs still run
curl -s localhost:8090/health | python -c "import sys,json; h=json.load(sys.stdin); print(h['collectors']['alert_dispatch']['last_run'] is not None)"   # True (dispatcher ticking)
tail -n 3 data/logs/$(date -u +%F).jsonl                                  # JSON lines with ts/level/logger/message
kill -TERM $PID; wait $PID; echo "exit=$?"                                # log: "Services shut down cleanly (<10s)"; exit=0
# Telegram (only if token+chat set): "🛰 TaoSentinel online … jobs 7 available / 53 declared"
# systemd (16's units and installer)
sudo ops/install-systemd.sh && sudo systemctl start taosentinel && sleep 10
systemctl is-active taosentinel                                           # active
systemctl show taosentinel -p NRestarts -p WatchdogTimestamp              # NRestarts=0 ; WatchdogTimestamp recent (watchdog pinging)
sleep 300; systemctl show taosentinel -p NRestarts                        # still 0
journalctl -u taosentinel --since "10 min ago" | grep -c watchdog_withheld   # 0
sudo systemctl stop taosentinel; journalctl -u taosentinel -n 5           # "Services shut down cleanly", no "timed out" line
```

Phase 0 is done when every line above matches and `pytest -q` is green. The "hello matrix" is the `/health` document plus the 53-row job table (7 available) plus the Telegram online message. After each later phase, `python -m app check` must print the §2.7 "declared vs available" row for that phase (e.g. after Phase 2: `16 available … 37 unavailable`).

---

## 11. Open questions / operator decisions

1. **Settled by 02 (kept for traceability):** `job_state`, `control_state` and `provider_spend.requests`/`units_json` are 03 tables (B7, B12, G5); pauses live only in `control_state`; `PAUSE_ALL` exists (B12); `AlertLatch` writes events and `alert_dispatch` ships in Phase 0 (D3); `/health` status vocabulary (G6); limiter caps (F2, G1).
2. **Register-all-then-flag-check** vs. the old bot's register-only-enabled (`main.py` 1369-1578): chosen for hot enable/disable via `reload-config`; cost is one `disabled` skip per trigger per disabled job (no DB rows).
3. **TMC SSE and keys:** research 02-gapfill-keyless observed anonymous SSE working and un-metered, while 02-gapfill-data-continuity says `/public/v1/sse/*` "require a key" — UNVERIFIED; 05 handles 401 with its REST fallback.
4. **httpx ceiling:** pinned `<1.0` to match `respx`; 10's `anthropic` SDK may require a newer `httpx` — whoever bumps it must bump `respx` in the same commit and re-run `pytest`.
5. **Alert latches are in memory:** a restart re-arms `collector_failing` (the next streak pages again); 16 persists its spend latches itself. Acceptable.
6. **`HEALTH_HOST` off-box:** default loopback; a Tailscale address plus `API_AUTH_TOKEN` is the recommended way to reach the UI (I2) — operator decision.
7. **Owner tables that 01 mirrors are still moving** (04, 06, 07, 09, 10, 13, 14 changed on 2026-09-24 while this revision was written). 01's `test_scheduler.py` and `test_config.py` compare `JOB_TABLE`/`Settings` against the owner rows, so drift fails CI instead of shipping.
8. **Internal egress outside G1:** 16's `deadman` ping is a limiter row with `book_spend=False`; the ledger's closed provider list does not name it (UNVERIFIED whether the coordinator wants it added to G1).
9. **Provider numbers UNVERIFIED:** OnFinality 40 rps keyed / 5 public, TaoStats free-tier 5/min & 10k/month, X official-v2 windows, GitHub 304 cost — the owners verify on the laptop; 01 only carries their keys.

---

## 12. Sources

- `E:\TaoData\specs\02-cross-cutting-conventions.md` (A2, B1, B9, B12, D1–D4, E1–E4, F1–F2, G1, G5, G6 — binding)
- `E:\TaoData\specs\00-MASTER-SPEC.md` §1.3, §2, §3, §8, §9, §11, §12
- Owner job and config tables (read 2026-09-23/24): 03 §2.7/§5, 04 §2.1/§5, 05 §5/§6.1, 06 §2.1/§5, 07 §2.7/§5, 08 §5/§6, 09 §2.5/§5/§6.1/App. A, 10 §5/§6.1, 11 §5/§6, 12 §5/§6/§8, 13 §5/§8.8, 14 §2.4/§5/§6.1, 15 §5, 16 §2.5/§2.10/§2.11/§2.13/§5/§6.1, 17 §5/§8.1; `18-operator-checklist.md` (key names)
- `E:\TaoData\research\02-gapfill-old-bot-runtime-skeleton.md` (hot-reload, cwd-relative paths, `__main__` double import, READY-before-backup, watchdog max-age, `capture_signals`)
- `E:\TaoData\research\01-whale-research-ops.md` §3; `E:\TaoData\research\02-gapfill-keyless-onchain-pillar-source.md` (TMC limits, SSE metering, shared-IP bucket)
- `E:\SN_Trader\SubnetTrader\app\main.py` 22, 83, 323-340, 359-447, 1099-1146, 1256-1291, 1294-1366, 1369-1578, 2144-2228, 2230-2441, 2480-2490, 3232-3296, 3308-3320, 3323-3361, 3364-3382, 3385-3486 (all re-verified 2026-09-23)
- `E:\SN_Trader\SubnetTrader\app\api_auth.py` 1-36 (18, 30-36); `app\config.py` 132-137, 490-510, 542-547, 578-579, 603-617, 637-644, 664-670, 685; `app\config_api.py` 483; `app\strategy_api.py` 16, 22-36; `app\test_api.py` 5-10, 20
- `E:\SN_Trader\SubnetTrader\app\logging\logger.py` 1-161 (14, 29, 116); `app\notifications\telegram.py` 15, 19-45, 97-111, 114-545
- `E:\SN_Trader\SubnetTrader\app\utils\math.py` 1-177 (`gini_coefficient` 160-177); `app\utils\time.py` 1-57
- `E:\SN_Trader\SubnetTrader\tests\conftest.py` 1-17, `tests\test_graceful_shutdown.py` 25-31, `tests\test_factor_backtest.py` 51-84, `tests\test_ema_manager.py` 681-700, `pytest.ini`

## Changelog
- 2026-09-23 reconciliation + review:
  - E1–E4, E2: `JOB_TABLE` rebuilt from every owner table — 53 entries (50 jobs + 3 LongTasks); placeholders retired; kinds interval/cron/manual/longtask, `ops_exempt`, `startup_catchup` (+ avoid/only windows), per-job `fail_alert_threshold`; liveness column per 16 §2.5; per-phase declared/available table; verify strings recomputed (7 available / 53 declared in Phase 0).
  - D3 (amended): `alert_dispatch` implemented by 01 in Phase 0 (`app/alerts/dispatch.py`, 14's routing/templates as data); `AlertLatch.fire(key, text, *, event_type, severity, payload, dedupe_key, source)` via `emit_event`, direct fallback; `collector_failing`/`collector_recovered` events (D1, D2); `budget_*` keys retired.
  - F1/F2: §5 is the union of all owner §5 tables — 627 keys in 15 owner groups with owner defaults; F2 values applied; aliases (`API_HOST/PORT`, `GITHUB_PAT`, `TAOSTATS_MONTHLY_CALL_CAP`), `SCORING_SPEC_VERSION` property, cap 0 = disabled; retired-key table incl. B12 per-provider sentinels and `TELEGRAM_ALERTS_ENABLED`.
  - G1: `ProviderLimiter` row for every canonical id (+ `deadman`), GitHub 4,000/h burst 10 cap 4,500; `BudgetExhausted` ends a run as `paused: budget`; `ok_statuses`, per-host `Retry-After` cooldown, `max_body_bytes`, chained `ProviderError`; `Http` books `record(p, 0.0, requests=1)` (G5).
  - G6: `/health` builder with `status ok|degraded|initializing`, `degraded_reasons` vocabulary (incl. `spec_unknown`), 16's `spend` snapshot; control routes write `control_state` (B12), group targets per 16 `PAUSE_GROUPS`.
  - B1/A2: `app/util/time.py` (`now_ts`, `floor_hour`, `floor_day`, `to_iso`, `parse_iso`→int, `parse_ts`, `parse_dt`) and `app/util/mathutil.py` (old `math.py:1-177` incl. `gini_coefficient`, + 13's `monotone_score`).
  - B7: 01's own DDL removed (03 owns every table; INTEGER times). Lifecycle: 16 `pre_init` first, READY before any job/alert, bounded shutdown (1+1+5+3 s ≤ 10 s deadline), `capture_signals` override, `task_handles["health"]`, self-probe, `last_attempt`.
  - Deps: `discord.py>=2.4,<3`; `[backfill]` extra (`async-substrate-interface`, `xxhash`); no bittensor/bittensor-wallet/substrate-interface (tested with and without the extra); 05's `SUBTENSOR_RPC_MODE=sdk` rejected.
  - Review: 14 old-bot line refs re-verified; fixed `run_health_server` sub-ranges, signal-handler/READY lines, `_health_alerted` range (1099-1146), main/entrypoint 3385-3486, telegram class ranges; package findings applied except those contradicting the ledger (see coordinator report).
- 2026-10-20: §5 inventory + `TERMINAL_API_URL` (owner 14 §8.4a, loopback-only τ Terminal read API for the Telegram `/wallet` view); 658 keys.
