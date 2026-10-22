# 03 — Storage schema, migrations, retention and backup

**Status:** proposed, aligned to `02-cross-cutting-conventions.md` (2026-09-23) · **Phase:** 0 · **Depends on:** 01 (Settings, `CollectorJob`, job registry, `/health`, CLI, `app/util/time.py`); soft: 12 (metric registry that `METRIC_KEYS` must cover), 16 (`latest_backup_age_hours` for the retention backup gate) · **Consumed by:** every other spec · **Master spec refs:** §1.3 (PIT, absence ≠ failure, fail-safe ops, simplicity), §2 (reuse of `storage/db.py`, `backup.py`, `pool_history`), §5 (occupant_key), §6 (storage model — binding table list), §7 (metric_keys, red flags, confidence), §10 (PIT panel), §11 (backups, retention, power loss), §12 (Phase 0)

This spec is the **canonical source for every table, column, index, view, trigger and event_type name** (02 §A2, §B7). Only `02-cross-cutting-conventions.md` (cited as "02 §<id>") and the master outrank it. Where a sibling spec (01, 04–17) wrote a different name or type, the name here wins and the rename is listed in **Appendix A — Name reconciliation**. Specs 08, 11, 12, 16 and 17 were written in parallel with this spec; their proposed DDL (all columns, converted to this spec's conventions: INTEGER unix-second times, `STRICT`, CHECKs) is folded into §3 so that §3 is a **superset** of every column any sibling reads or writes. Where two siblings disagree, §3 picks one name and Appendix A records the loser. Ownership split with spec 16: this spec owns the schema, `Database` (incl. the `backup_to` primitive), migrations, PIT helpers, retention and the catalogs; spec 16 owns the backup pipeline, backup CLI and restore (§2.8).

---

## 1. Purpose and scope

**In scope (Phase 0 deliverable; every table is created at Phase 0 even if its writer arrives later):**
- `app/storage/models.sql` — the complete baseline DDL (schema version 1) in §3: ops, reference, raw point-in-time (PIT), derived and imported tables; indexes; consumer views; append-only enforcement triggers.
- `app/storage/db.py` — `Database` class (writer + read-only reader connection, PRAGMAs, `transaction()`, `insert_or_ignore`, `upsert`, `backup_to`, stats).
- `app/storage/migrate.py` — `schema_version` table + ordered, checksummed, idempotent migration runner that replaces the old blind `try/except ALTER` tuple (`E:\SN_Trader\SubnetTrader\app\storage\db.py:82-119`).
- `app/storage/pit.py` — the single `metric_values` **writer** `put_metric_values` (alias `upsert_metric_values`; 02 §B4), which validates keys against `app/storage/catalog.py:METRIC_KEYS` and runs the SQL of §2.5, plus the PIT **read** helpers every consumer uses (`metric_at`, `cross_section`, `latest_scores`; 02 §B5).
- `app/storage/catalog.py` (metric_key + event_type + red-flag catalogs), `app/storage/events.py` (`emit_event`, `mark_alerted`), `app/storage/precedence.py` (source ranks + precedence upsert SQL).
- `app/storage/retention.py` — job `storage_retention_daily`: tick→hourly rollup guarantee, pruning, text-to-hash privacy retention, WAL/vacuum maintenance.
- `app/storage/db.py::Database.backup_to` — the verified snapshot primitive (old `db.py:38-80`, verbatim) and the exported constant `CODE_SCHEMA_VERSION` (highest migration version shipped with the code). The rest of `app/storage/backup.py` (`create_backup`, manifest, gzip, rotation, size budget, mirror, `latest_backup_age_hours`, `restore_snapshot`, `BackupJob`) is **specified by spec 16 §2.7–§2.8**; this spec only fixes the schema-compatibility rule it must apply (§2.8).
- The legacy-import **contract** (§2.9): target DDL, trigger-guarded enrichment, occupant-derivation and `price_bars` derivation rules that spec 17's `app/backfill/*` modules implement (no storage-side `legacy.py`).
- `app/storage/cli.py` — `python -m app db {check,migrate,stats,retention,explain}` (backup/restore commands are spec 16's `python -m app backup …`).
- The canonical **metric_key catalog** (§3.8) and **events catalog** (§3.9).
- Row-growth and disk estimates (§6).

**Out of scope:** what each collector computes (04–11), normaliser math, feature-store write semantics beyond the DDL/trigger contract, and scoring (12), import *scripts* and backfill scheduling (17 — this spec fixes only the target tables and derivation rules), the backup pipeline, restore runbook, systemd/auto-update/off-box mirror hardware (16), API response shapes (14), the trading-bot side tables (15 owns SubnetTrader's `score_feed_log`).

---

## 2. Design

```
 collectors / scoring / API (asyncio, one process)
        │ writes (serialised)                      │ reads (concurrent)
        ▼                                          ▼
  Database.writer  ── asyncio.Lock + contextvar ── Database.reader (mode=ro, query_only)
        │  BEGIN IMMEDIATE … COMMIT                 │  WAL snapshot of last commit
        └──────────────► data/taosentinel.db (+ -wal, -shm) ◄──────┘
                              │           ▲
     storage_retention_daily  │           │ migrate(): models.sql (v1) + migrations/NNNN_*.sql
     (03:00Z) rollup → prune  │           │ schema_version(version, name, checksum, applied_at)
                              ▼
     backup_daily (spec 16, 03:30Z, waits ≤ 20 min for storage_retention_daily):
                   Database.backup_to (VACUUM INTO <dest>.tmp → quick_check → rename) → manifest → gzip
                   (newest copy also kept uncompressed for spec 13) → mirror dir
                   → rotate (7 daily + 8 weekly inside DB_BACKUP_MAX_TOTAL_GB = 12, 2 newest never deleted; 02 §I1)
```

### 2.1 Conventions (binding for all tables)

| Concern | Rule |
|---|---|
| Identity | `occupant_key TEXT = netuid \|\| ':' \|\| network_registered_at` is the primary identity of every long-lived record (master §5). Per-netuid-per-time tables (`chain_ticks`, `price_bars`, `pool_history`) key on `netuid` + time and **also** carry `occupant_key` (nullable only on imported/unattributable rows). `occupants` enforces the format with a CHECK. |
| Instants | `*_at` columns: `INTEGER` unix seconds UTC, in every table (02 §B1). **No TEXT timestamps anywhere** (STRICT tables reject ISO strings in INTEGER columns). ISO-8601 `…Z` strings appear only at boundaries (HTTP JSON, Telegram text, logs) via `app/util/time.py` (01): `now_ts() -> int`, `floor_hour(ts) -> int`, `floor_day(ts) -> int`, `to_iso(ts) -> str`, `parse_iso(s) -> int`, `parse_ts(x) -> int` (int, float, ISO string or aware datetime). |
| Buckets | `*_ts` columns (`hour_ts`, `day_ts`, `check_day_ts`, `price_bars.ts`, `sample_ts`): `INTEGER` unix seconds of the bucket **start**, aligned (`hour_ts % 3600 = 0`, `day_ts % 86400 = 0`). `as_of` (metric_values, scores, gate tables): `INTEGER` unix seconds; official daily rows have `as_of % 86400 = 0`. |
| Calendar keys | Only `provider_spend.month TEXT 'YYYY-MM'` (UTC). |
| PIT columns | Every **raw** table has `observed_at INTEGER NOT NULL` (when we fetched), `event_at INTEGER` (when it happened; NULL if unknown), `run_id TEXT` (`collector_runs.run_id`; `sse:<connected_at>` for the SSE LongTask; `script:<name>:<started_at>` for scripts; NULL only on imported legacy rows), `source TEXT NOT NULL` (provider/endpoint family on raw tables; on `metric_values` and `events` the producer vocabulary of 02 §B9). Never forward-fill: a value not observed is absent, never copied from a neighbour. Backfilled rows keep the producer's `source` and set `backfilled = 1` (02 §B9, §J1). |
| Booleans | `INTEGER` with `CHECK (col IN (0,1))`; NULL = unknown. |
| Types | All new tables are `STRICT` (SQLite ≥ 3.37, asserted at connect): only `INTEGER`, `REAL`, `TEXT`, `BLOB`. Legacy-compatible tables (`pool_history`, `whale_events`, `whale_cohort`) keep the old non-STRICT column definitions column-for-column (names, order, types, constraints) with nullable provenance columns appended, so `INSERT OR IGNORE INTO t (<old columns>) SELECT …` from the old ledger works (§2.9). |
| Amounts | Raw tables store values **as delivered** by the source (units in comments; UNVERIFIED units flagged). Unit conversion happens in features (12) and in the `v_quant_*` views (§3.7). |
| JSON | `TEXT` holding valid JSON; name ends in `_json`, except master-named `events.payload`, `scores.pillar_scores`, `scores.red_flags`, `scores.explain`. `CHECK (json_valid(...))` on derived tables. |
| External ids | TEXT (X/Discord snowflakes exceed 2^53; extrinsic ids are `block-idx`). GitHub `repo_id` is INTEGER. |
| Absence vs failure in `metric_values` | Producers put the status in `value_json.status` (reserved for this enum; domain enums use other keys, e.g. 07's `value_json.product_status`) and never write a `status` column; the table exposes it as the generated column `status = json_extract(value_json,'$.status')`, `none` when the key is missing (02 §B3). Values: `ok` (value is the raw measurement, or a final [0,1] score for pre-normalised metrics, may be 0.0), `absent` (verifiable absence → spec 12 normalises to **0.0** regardless of `value`), `failed` (collection failure → **None**, `value` NULL), `not_applicable` (→ None, e.g. 0 merged PRs); `none` is read as `failed` (on `stage='norm'` rows it also means "no raw row"). CHECKs make `ok` with NULL and `failed`/`none` with a value impossible on raw rows. The collector decides; the normaliser never guesses (master §7.2). |
| Foreign keys | Declared on reference tables only (`subnets`, identity, manual, team, watch, whitelist, x mapping). Raw/derived high-volume tables carry `occupant_key` without an FK (write throughput, import ordering); `python -m app db check` reports orphans. |
| Write classes | Each table comment carries `class:` — **append** (INSERT OR IGNORE on the natural key; never UPDATE except the annotation columns named in the table comment; DELETE only by §2.7 retention), **state** (current-state row; UPSERT allowed; history lives in an append table or is not needed), **precedence** (UPSERT allowed only when the new row's source outranks the stored one), **rollup** (deterministic recompute from append rows; REPLACE allowed). `metric_values`, `scores`, `events`, `pool_history` have bespoke, trigger-enforced rules (§2.5). |

### 2.2 Physical layout, connections, PRAGMAs

One SQLite file at `DB_PATH` (`data/taosentinel.db`, master §1.3 "one SQLite file") in WAL mode. The process opens **two** aiosqlite connections:

- **writer** (`isolation_level=None` = autocommit; transactions are explicit `BEGIN IMMEDIATE … COMMIT`): all INSERT/UPDATE/DELETE, migrations, retention.
- **reader** (`file:{DB_PATH}?mode=ro`, `uri=True`, `PRAGMA query_only=ON`): API, Telegram, scoring/feature reads, `/health`. Rationale: aiosqlite runs each connection on one worker thread, so a 2-s retention batch on the writer would otherwise block every API read; WAL gives the reader a consistent snapshot of the last commit. `DB_READER_ENABLED=false` routes everything to the writer (tests, low-memory hosts).

PRAGMAs applied by `connect()` (writer; the reader gets `busy_timeout`, `temp_store`, `cache_size`, `mmap_size`, `query_only=ON`):

```sql
PRAGMA auto_vacuum = INCREMENTAL;       -- only when the file is brand new (page_count = 0), before any CREATE
PRAGMA journal_mode = WAL;              -- persistent; result must be 'wal' or connect() raises SchemaError
PRAGMA synchronous = NORMAL;            -- WAL-safe: a power cut may lose the last commits, never corrupts
PRAGMA busy_timeout = {DB_BUSY_TIMEOUT_MS};      -- 5000
PRAGMA foreign_keys = ON;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -{DB_CACHE_SIZE_MB * 1024};  -- 64 MB
PRAGMA mmap_size = {DB_MMAP_SIZE_MB * 1048576};  -- 256 MB
PRAGMA wal_autocheckpoint = 1000;       -- pages
PRAGMA journal_size_limit = 67108864;   -- truncate -wal to 64 MB after checkpoints
```

`connect()` raises `SchemaError("sqlite >= 3.37.0 required")` when `sqlite3.sqlite_version_info < (3, 37, 0)` (STRICT, RETURNING, generated columns; Ubuntu 22.04 ships 3.37.2, 24.04 ships 3.45) and `SchemaError("JSON1 missing")` if `SELECT json_valid('{}')` fails.

Concurrent writers outside the service (spec 17 scripts) are allowed: they share `busy_timeout` and must keep each write transaction to ≤ 50,000 rows or ≤ ~1 s lock hold (spec 17's per-netuid import transactions of ≤ 120 k narrow rows ≈ 0.5 s qualify; its `--online` mode uses 20 k-row batches). Full in-place `VACUUM` is **never** run automatically and never in the start path (old 2026-06-29 incident, master §2); space is reclaimed with `incremental_vacuum` (§2.7).

### 2.3 `Database` API (`app/storage/db.py`)

```python
MIN_SQLITE = (3, 37, 0)

@dataclass(frozen=True)
class ExecResult:
    rowcount: int
    lastrowid: int | None

class SchemaError(RuntimeError): ...
class MigrationError(SchemaError): ...
class AppendOnlyViolation(RuntimeError): ...

class Database:
    def __init__(self, path: str | Path, *, busy_timeout_ms: int | None = None,
                 reader: bool | None = None) -> None          # None → read settings dynamically
    path: Path
    @property
    def schema_version(self) -> int                           # 0 until apply_schema() ran
    schema_warnings: list[str]                                # "checksum_drift:<v>", "schema_ahead:<db>><code>"

    async def connect(self) -> None                           # version/JSON1 checks, mkdir, PRAGMAs, writer (+reader)
    async def apply_schema(self) -> int                       # = (await migrate(self)).version (01 calls this)
    async def close(self) -> None                             # reader then writer; idempotent

    async def execute(self, sql: str, params: Sequence | Mapping = ()) -> ExecResult
    async def executemany(self, sql: str, seq: Iterable[Sequence | Mapping]) -> int
    async def fetchone(self, sql: str, params=(), *, writer: bool = False) -> dict | None
    async def fetchall(self, sql: str, params=(), *, writer: bool = False) -> list[dict]
    async def fetchval(self, sql: str, params=(), *, writer: bool = False) -> Any   # first col of first row
    async def insert_or_ignore(self, table: str, rows: Sequence[Mapping[str, Any]], *, chunk: int = 500) -> int
    async def upsert(self, table: str, rows: Sequence[Mapping[str, Any]], *, conflict: Sequence[str],
                     update: Sequence[str] | None = None, where: str | None = None, chunk: int = 500) -> int
    def transaction(self, *, immediate: bool = True) -> AbstractAsyncContextManager[None]
    async def backup_to(self, dest: str | Path) -> dict        # {"path","size_bytes","integrity"}
    async def stats(self) -> dict      # {"path","size_bytes","wal_bytes","page_size","page_count","freelist_count",
                                       #  "schema_version","sqlite_version","journal_mode","auto_vacuum"}
    async def checkpoint(self, mode: str = "PASSIVE") -> tuple[int, int, int]   # PASSIVE|FULL|RESTART|TRUNCATE
    async def incremental_vacuum(self, pages: int) -> int      # pages freed (freelist before − after)
    async def optimize(self) -> None                           # PRAGMA optimize
    async def columns(self, table: str) -> list[str]           # cached PRAGMA table_xinfo, excludes generated cols
```

Semantics (each pinned by a test in §9):
1. **Write serialisation.** A process-wide `asyncio.Lock` (`_wlock`) guards the writer. `execute`/`executemany` outside a transaction take the lock for one autocommitted statement. `transaction()` takes the lock, sets `contextvars.ContextVar _tx_owner = id(self)`, issues `BEGIN IMMEDIATE` (or `BEGIN DEFERRED`), yields, `COMMIT`s, and `ROLLBACK`s on any exception (re-raised). Statements from the owning task inside the block run without re-locking; a nested `transaction()` in the same task joins the outer one (no savepoint). Other tasks' writes wait on the lock — they can never leak into another task's open transaction.
2. **Read routing.** `fetch*` go to the reader unless `writer=True`, the calling task owns an open transaction (read-your-writes), or the reader is disabled.
3. **Identifier safety.** `insert_or_ignore`/`upsert` validate `table` against `^[a-z_][a-z0-9_]*$` and every row key against `columns(table)`; all rows must share one key set (`ValueError` otherwise). Values are always bound parameters. Empty `rows` → `0`, no SQL.
4. **`insert_or_ignore`** builds `INSERT OR IGNORE INTO t (c…) VALUES (?…)` (multi-row `VALUES`), runs all chunks inside one `transaction()`, returns rows actually inserted (sum of `changes()`). The effective chunk is `min(chunk, 32000 // len(columns))` so a statement never exceeds SQLite's 32,766 bound-parameter limit (`chain_hourly` has 60 columns → 533 rows). The same rule applies to `upsert`.
5. **`upsert`** builds `INSERT INTO t (c…) VALUES (?…) ON CONFLICT (conflict…) DO UPDATE SET u = excluded.u, … [WHERE where]`; `update` defaults to every non-conflict column. It raises `AppendOnlyViolation` for tables in `APPEND_ONLY_TABLES` (§2.5); the bespoke write paths are explicit SQL elsewhere: `metric_values` via `pit.py::put_metric_values` (§2.5 SQL; `metric_values` is not in `APPEND_ONLY_TABLES`, the triggers guard it), `events` via `events.py::emit_event`/`mark_alerted`, annotation columns via the owning collector's `UPDATE`.
6. **`execute` return.** `ExecResult(rowcount, lastrowid)`; use `RETURNING id` when an id is needed inside a transaction.
7. **`backup_to`** is the old `Database.backup_to` (`db.py:38-80`) ported verbatim: a dedicated third connection runs `VACUUM INTO ?` to `<dest>.tmp` (a leftover tmp is unlinked first — `VACUUM INTO` refuses to overwrite), a fresh connection runs `PRAGMA quick_check` on the copy, non-`ok` → delete tmp and raise `RuntimeError`, else `os.replace(tmp, dest)`; returns `{"path", "size_bytes", "integrity"}`. Spec 16's pipeline calls it; the snapshot never includes an `ATTACH`ed file.
8. **Row factory** returns `dict` (old `db.py:27` sets `aiosqlite.Row`; `:126-134` convert to `dict`).
9. **`CODE_SCHEMA_VERSION: int`** (module constant in `app/storage/migrate.py`, re-exported by `db.py`) = the highest version returned by `discover()` at import time; spec 16's restore guard and `/health` read it.

### 2.4 Migrations (`app/storage/migrate.py`)

- **Version 1 = `app/storage/models.sql`** — exactly the DDL of §3 (every statement `IF NOT EXISTS`). **Versions ≥ 2 = `app/storage/migrations/NNNN_<snake_name>.sql`** (4-digit, contiguous). Fresh databases replay 1…N in order (no "latest schema" shortcut), so every install converges to the same result.
- `schema_version(version PK, name, checksum, applied_at)`; `PRAGMA user_version` mirrors the max applied version.

```python
@dataclass(frozen=True)
class Migration:
    version: int; name: str; path: Path; sql: str; checksum: str      # sha256 hex of file bytes

@dataclass
class MigrateResult:
    version: int; applied: list[int]; warnings: list[str]

def discover() -> list[Migration]                  # models.sql as v1 + migrations/*.sql sorted; gap/dup → SchemaError
def split_statements(sql: str) -> list[str]        # accumulate lines until sqlite3.complete_statement(buf); drop comment-only
async def migrate(db: Database) -> MigrateResult
```

`migrate()` algorithm:
1. `CREATE TABLE IF NOT EXISTS schema_version (...)` (DDL §3.1). If a pre-existing `schema_version` lacks `name`/`checksum` (a dev DB built from 01's two-column draft), add them (`name TEXT NOT NULL DEFAULT 'baseline'`, `checksum TEXT NOT NULL DEFAULT ''`) — idempotent.
2. `applied = {version: checksum}` from `schema_version`.
3. For each discovered migration with `version ∉ applied`, ascending: `async with db.transaction(immediate=True)`: for each statement of `split_statements(sql)` — if it matches `^\s*ALTER\s+TABLE\s+(\w+)\s+ADD\s+COLUMN\s+(\w+)` and the column already exists (`PRAGMA table_xinfo`) → skip (idempotent ALTER); otherwise execute. Then `INSERT INTO schema_version VALUES (v, name, checksum, now_ts())` and `PRAGMA user_version = v`. Any exception → rollback, raise `MigrationError(f"v{v} {name}: {stmt[:200]}: {exc}")` → boot fails loudly (01 §7).
4. An applied migration whose checksum differs from its file → `logger.error("schema checksum drift", data={"version": v})`, `schema_warnings += ["checksum_drift:<v>"]` (surfaced as `/health.degraded_reasons: schema_warning`); **never re-applied**. After the last migration, each entry of `schema_warnings` (this step and step 5) is written as one `schema_warning` event (§3.9, `source='storage'`, dedupe `kind|version`) through `emit_event`.
5. DB version greater than the highest known file (code rolled back by spec 16's auto-update) → warning `schema_ahead:<db>><code>`, boot continues. Safe because of the **additive-only policy**: migrations ≥ 2 may `CREATE TABLE/INDEX/VIEW/TRIGGER`, `ADD COLUMN` (nullable or constant default), `DROP/CREATE VIEW`, `DROP/CREATE TRIGGER`; they may **not** `DROP TABLE`, `DROP COLUMN`, `RENAME`, change a column type, or tighten a constraint on existing data, unless the file's first line is `-- @allow-destructive <reason>` (lint test enforces; human review required).
6. Forbidden in every migration file (lint test): `BEGIN`, `COMMIT`, `ROLLBACK`, `VACUUM`, `PRAGMA journal_mode`, `PRAGMA auto_vacuum`, `ATTACH`.

All "(new, proposed for 03)" tables and columns from 04–17 are folded into version 1, so Phase 0 ships exactly one migration (`python -m app check` prints `schema_version=1`, as 01 §10 expects). Final DDL lives only here (02 §B7): siblings describe the columns they need and never ship `CREATE TABLE`/`ALTER TABLE` for tables this spec owns. Consequently the sibling-proposed rebuilds are **not** implemented: spec 12's `metric_values_v2` copy-and-rename (v1 already has `stage` and the widened key), spec 12's `metric_norm` table (normalised values are `metric_values` rows with `stage='norm'`, 02 §B3) and its `ALTER TABLE scores/red_flag_state/regime_daily/rank_stability_daily` statements (the columns are in v1), and spec 17's `ALTER TABLE … ADD COLUMN` tuple (`price_bars.ts_last…backfilled`, `occupants.registered_ts/deregistered_ts`, `github_*.backfilled` are all in v1). A dev database created from an earlier draft is discarded, not migrated (nothing of value exists before Phase 0 completes).

### 2.5 Write disciplines and enforcement

`APPEND_ONLY_TABLES` (constant in `db.py`): `identity_observations, identity_raw_snapshots, chain_ticks, chain_trending, chain_pressure, chain_constants, chain_rest_snapshots, holders_snapshots, gate_fits, rpc_subnet_daily, rpc_metagraph_daily, block_times, rpc_owner_history, github_repo_snapshots, github_commits, github_events, web_checks, discord_snapshots, telegram_snapshots, announcements, social_post_mentions, x_account_snapshots, news_items, video_items, post_labels, post_label_entities, owner_snapshots, owner_position_hourly, owner_stake_events, owner_changes, manual_flags, scores, events, pool_history, whale_events, whale_cohort`. `Database.upsert()` refuses them (`AppendOnlyViolation`). The only UPDATEs allowed on them are explicit SQL statements (never `upsert()`) that touch the **annotation columns** named in the table comment: `announcements.deleted_at` (08), `owner_stake_events.{superseded_by, superseded_at, whitelisted, whitelist_id}` and `owner_changes.{whitelisted, whitelist_id, review_id, event_id, paired_change_id, identity_event_id}` (11; PIT readers use `superseded_at ≤ t` and `is_whitelisted(·, t)`, never the display caches), `events` (below), `pool_history` (below). Retention (§2.7) is the only code path allowed to DELETE from them or NULL their text columns.

Bespoke rules, **enforced by triggers** (§3.3, §3.6), so a bug cannot silently rewrite history:

| Table | Allowed after insert | Trigger behaviour |
|---|---|---|
| `scores` | nothing (master §6 "never updated") | `BEFORE UPDATE` / `BEFORE DELETE` → `RAISE(ABORT, 'scores is append-only')`. A new `spec_version` writes a parallel series. |
| `metric_values` | (a) `stage='raw'`: the one upgrade of a `failed`/`none` row by a later successful (`ok`/`absent`/`not_applicable`) observation of the same key — first success wins (02 §B4); `stage='norm'`: nothing (insert-once); (b) DELETE of **provisional** rows (`as_of % 86400 <> 0`): `stage='norm'` by spec 12's pruning (`METRIC_NORM_PROVISIONAL_RETENTION_DAYS`), `stage='raw'` by §2.7 | `BEFORE UPDATE` aborts unless the stored row is a raw `failed`/`none` row, the new status is successful, `observed_at` does not go backwards and no key column changes; `BEFORE DELETE WHEN OLD.as_of % 86400 = 0` → abort (official rows are permanent). |
| `events` | `alerted_at` (NULL → value, once); `payload` and `severity` while `alerted_at IS NULL` (10: `n_supporting`/`materiality`/escalation; 14: `suppressed`) | `BEFORE UPDATE WHEN` any other column changes, or `OLD.alerted_at IS NOT NULL` → abort. No DELETE. |
| `pool_history` | NULL-only enrichment of `moving_price, subnet_volume, network_registered_at, occupant_key` by spec 17 (from `ledger_backfill.db`/RPC); filling an observed value (`moving_price`, `subnet_volume`, `network_registered_at`) must also set `enriched_at`, while stamping `occupant_key` alone must **not** set it (§2.9, column comment) | `BEFORE UPDATE WHEN` a key/price/reserve/`timestamp`/`source`/`observed_at` column changes or an enrichment column changes from a non-NULL value → abort; `BEFORE DELETE` → abort. |
| `post_labels`, `identity_observations`, `manual_flags` | nothing | `BEFORE UPDATE` / `BEFORE DELETE` → abort. Superseded labels are new rows under a new `classifier_version`. |

**`metric_values` write contract** (02 §B3–§B4). The single writer is `app/storage/pit.py::put_metric_values(db, rows) -> int` (rows inserted or upgraded); `upsert_metric_values` is an alias of it (spec 12's `app/features/store.py` may re-export the alias; spec 05's `INSERT OR REPLACE` draft is forbidden — a REPLACE deletes the official row and the delete trigger aborts it). Before any SQL it raises `ValueError` when a `metric_key` is not in `app/storage/catalog.py:METRIC_KEYS`, when `stage ∉ {raw, norm}`, when a raw row's `value_json.status ∉ {ok, absent, failed, not_applicable}`, or when a norm row's key is not a `role='scored'` key; it serialises `value_json` with `json.dumps(…, separators=(",", ":"), sort_keys=True)` and writes every chunk in one `transaction()`. Producers key raw rows by the scoring slot the observation serves (`00:00Z` official, `06/12/18:00Z` provisional; hourly producers use the hour bucket) and write the next slot's `as_of` when they need fresher data.

```sql
-- stage='raw': first success wins; a failed/none row may be upgraded by a later successful observation
INSERT INTO metric_values (occupant_key, metric_key, as_of, spec_version, stage, value, value_json,
                           observed_at, source, run_id, backfilled)
VALUES (:occupant_key, :metric_key, :as_of, :spec_version, 'raw', :value, :value_json,
        :observed_at, :source, :run_id, :backfilled)
ON CONFLICT (occupant_key, metric_key, as_of, spec_version, stage) DO UPDATE SET
  value = excluded.value, value_json = excluded.value_json, observed_at = excluded.observed_at,
  source = excluded.source, run_id = excluded.run_id, backfilled = excluded.backfilled
WHERE metric_values.status IN ('failed','none')
  AND COALESCE(json_extract(excluded.value_json, '$.status'), 'none') NOT IN ('failed','none')
  AND excluded.observed_at >= metric_values.observed_at;
-- stage='norm' (spec 12 only, at scoring time; observed_at = the run's computed_at): insert-once
INSERT INTO metric_values (…same columns…) VALUES (…, 'norm', …) ON CONFLICT DO NOTHING;
```
PIT consequence: a successful raw row is never rewritten, so a replay through `metric_at` at any `t` sees exactly what a live run saw at `t`. The only history change is the `failed → success` upgrade, which carries the later `observed_at`; before that instant a replay sees neither version, so it selects what the live run selected after skipping the failed row (an older fresh row, or None). The exact inputs a score used also survive in `scores.explain` and in that slot's `stage='norm'` rows, the PIT record spec 13 reads.

### 2.6 Point-in-time read patterns (`app/storage/pit.py`)

```python
@dataclass(frozen=True)
class MetricRow:
    occupant_key: str; metric_key: str; as_of: int; spec_version: str; stage: str   # 'raw' | 'norm'
    value: float | None; status: str; value_json: dict | None                        # status = generated column
    observed_at: int; source: str; run_id: str; backfilled: bool = False

async def put_metric_values(db, rows: Sequence[Mapping[str, Any]]) -> int     # the single writer (§2.5)
upsert_metric_values = put_metric_values                                      # alias (02 §B4)
async def metric_at(db, occupant_key: str, metric_key: str, t: int, *, spec_version: str,
                    freshness_s: int, official_only: bool = False,
                    strict_backfill: bool = False) -> MetricRow | None
async def cross_section(db, metric_key: str, as_of: int, *, spec_version: str, stage: str = "norm",
                        occupant_keys: Sequence[str] | None = None) -> dict[str, MetricRow]
async def latest_scores(db, spec_version: str, *, include_prelaunch: bool = False,
                        include_insufficient: bool = False) -> list[dict]
```
The three readers use the reader connection and never write. `MetricRow.value_json` is `json.loads` of the column (None when NULL).

`metric_at` — "the value knowable at decision time `t`" (02 §B5; master §10, spec 12 §2.2, spec 13 §4). It reads `stage='raw'` rows only:
```sql
SELECT * FROM metric_values
WHERE occupant_key = :o AND metric_key = :m AND spec_version = :v AND stage = 'raw'
  AND as_of <= :t AND :t - as_of <= :freshness_s         -- freshness window (spec 12 registry, inclusive)
  AND observed_at <= :t                                   -- PIT: only what had been fetched by t
  AND (:official_only = 0 OR as_of % 86400 = 0)
  AND (:strict_backfill = 0 OR backfilled = 0)
ORDER BY (status IN ('failed','none')), as_of DESC LIMIT 1;   -- usable rows first; candidates are few (PK range)
```
It returns the greatest `as_of` among usable (`status ∉ {failed, none}`) candidates; a `failed` row is skipped, so an older row inside the freshness window is used (the window doing its job, not a forward-fill). Only when every candidate failed does it return the newest failed row (the metric is None, with that row's `reason`). It never reads a row observed after `t` and never looks outside the window, so a replay reproduces what scoring saw. The caller maps raw `absent → 0.0`, `failed | none | not_applicable → None`, `ok → normalise(value)`; no row → None (lowers confidence). Spec 13 factors and spec 14's evidence panel read `stage='norm'` rows (02 §H2, §J2): `value` is the normalised `m`, `value_json.raw` the raw input and `value_json.status` the raw status.

`cross_section` (for `rankg` audits and spec 14 evidence over active occupants on one `as_of`, master §7.2): `SELECT * FROM metric_values WHERE metric_key=:m AND stage=:stage AND spec_version=:v AND as_of=:a` — served by `ix_metric_values_xsec`.

Latest score per occupant (API/feed hot query, served by `ix_scores_latest`):
```sql
SELECT s.* FROM scores s
JOIN (SELECT occupant_key, MAX(as_of) AS as_of FROM scores WHERE spec_version = :v GROUP BY occupant_key) l
  ON l.occupant_key = s.occupant_key AND l.as_of = s.as_of
WHERE s.spec_version = :v
  AND (:include_prelaunch = 1 OR s.status = 'active')
  AND (:include_insufficient = 1 OR s.insufficient_data = 0);
```
Price series (served by the `price_bars` PK): `SELECT … FROM price_bars WHERE netuid=:n AND period=:p AND ts BETWEEN :a AND :b ORDER BY ts`.
Occupant at a block (04's `occupant_at`, served by `ux_occupants_netuid_reg`): `SELECT * FROM occupants WHERE netuid=:n AND network_registered_at <= :block ORDER BY network_registered_at DESC LIMIT 1`.
Identity as of `t`: latest `identity_observations` row per `(occupant_key, source, field)` with `observed_at <= t` (`ix_idobs_latest`); `v_identity_current` is the `t = now` shortcut.

### 2.7 Retention and rollup (`app/storage/retention.py`)

Job `storage_retention_daily` (factory `storage.retention:RetentionJob`, **cron** `STORAGE_RETENTION_CRON_HOUR:STORAGE_RETENTION_CRON_MINUTE` = **03:00Z** (02 §E3: it runs every policy, including the `chain_ticks` prune; there is no `chain_prune` job) — before spec 16's `backup_daily` at 03:30Z, which waits up to 20 min for it via `DB_BACKUP_WAIT_FOR_JOBS=storage_retention_daily`, so the snapshot is taken after pruning; flag `STORAGE_RETENTION_ENABLED`, phase 0, free, `paid=False`, `ops_exempt=True` (spec 16 §2.11), timeout 3600 s, misfire grace 3600 s, `min_gap_factor=0`). It runs the policies below in order; each is idempotent, batched (`RETENTION_DELETE_BATCH` rows per transaction, commit per batch so the writer lock is released between batches), logs `{policy, rows, seconds}`; `rows` = total touched; `JobOutcome(rows=Σ rows, note="chain_prune=… skipped=[…]")`. `python -m app db retention --dry-run` prints counts without writing.

```python
@dataclass(frozen=True)
class RetentionPolicy:
    id: str; destructive: bool
    run: Callable[[Database, int, bool], Awaitable[int]]      # (db, now, dry_run) -> rows touched
POLICIES: tuple[RetentionPolicy, ...]                         # table order below
async def run_retention(db: Database, *, now: int, dry_run: bool = False,
                        only: Sequence[str] | None = None) -> dict[str, int]
class RetentionJob(CollectorJob)                              # id storage_retention_daily
```

**Backup gate.** Policies marked *destructive* run only if spec 16's `app.storage.backup.latest_backup_age_hours(DB_BACKUP_DIR) ≤ RETENTION_REQUIRE_BACKUP_HOURS` (30; yesterday's 03:30Z backup is 23.5 h old at 03:00Z), **or** `DB_BACKUP_ENABLED=false` (warning logged), **or** free disk at `DATA_DIR` < `DISK_FREE_MIN_GB` (disk emergency). Otherwise they are skipped and one `retention_skipped` event is written (the job still returns `ok`). *Privacy* policies are never gated. A missing `latest_backup_age_hours` (spec 16 not built yet, `ImportError`) counts as "no backup".

| policy id | table / columns | rule | kind |
|---|---|---|---|
| `chain_prune` (was 05's job of that name) | `chain_ticks` | cutoff = `now − CHAIN_TICKS_RETENTION_DAYS·86400` (default **30 d**, 02 §F2; 05 writes one tick snapshot every `CHAIN_TICKS_BLOCK_STRIDE` = 5 blocks ≈ 1 min). **Rollup guarantee first:** for every `(netuid, hour_ts)` older than cutoff that has ticks but no `chain_hourly` row, insert one from the hour's last tick (`source='tick_rollup'`, hot columns only, others NULL — never copied from other hours); for every 5-min bucket with ticks but no `5m` `price_bars` row, insert it (`source='sse_rollup'`, OHLC from ticks ordered by `block_number`). Then per netuid: `DELETE FROM chain_ticks WHERE netuid=? AND block_number IN (SELECT block_number FROM chain_ticks WHERE netuid=? AND event_at < ? ORDER BY block_number LIMIT ?)` repeated until 0 (port of the chunked loop `db.py:655-681`) | destructive |
| `price_bars_5m` | `price_bars` `period='5m'` | delete `ts < now − PRICE_BARS_5M_RETENTION_DAYS·86400` (90 d); `1h`/`1d` kept forever | destructive |
| `metric_raw_provisional` | `metric_values` `stage='raw' AND as_of % 86400 <> 0` (05/11's 06/12/18:00Z slot rows, 12's hourly `features_hourly` rows) | delete `as_of < now − METRIC_PROVISIONAL_RETENTION_DAYS·86400` (90 d); official (00:00Z) rows kept forever. Provisional `stage='norm'` rows are pruned by spec 12 itself (`METRIC_NORM_PROVISIONAL_RETENTION_DAYS`, 14 d) | destructive |
| `chain_rest_snapshots` | `chain_rest_snapshots` | delete `observed_at < now − CHAIN_REST_SNAPSHOTS_RETENTION_DAYS·86400` (90 d), keeping the newest row per `(endpoint, params)` | destructive |
| `github_http_cache` | `github_http_cache` | delete `fetched_at < now − GITHUB_HTTP_CACHE_RETENTION_DAYS·86400` (30 d) | destructive (cache) |
| `llm_batch_items` | `llm_batch_items` whose batch `status ∈ {applied, failed, expired, canceled}` and `ended_at < now − LLM_BATCH_ITEMS_RETENTION_DAYS·86400` (90 d) | delete | destructive |
| `nlp_text_retention` (was 10's job of that name) | `social_posts.text` (X), `announcements.text` (08: own-aggregator-server crossposts and Telegram posts), `news_items.summary`, `video_items.description` | set NULL when `event_at < now − LLM_TEXT_RETENTION_DAYS·86400` (90 d), labelled or not (02 §B10: text 90 d, then hash only). **Pending-classification text** (an `announcements` row with `text_purge_after` set, i.e. written while its `DISCORD_STORE_TEXT`/`TELEGRAM_POSTS_STORE_TEXT` flag was false; amended 02 §B10): set NULL as soon as a `post_labels` row with `post_id = msg_id` exists **or** `text_purge_after <= now`. `text_hash`, ids, engagement stay ("hash only") | privacy |
| `web_text` | `web_checks.content_text` | NULL when `check_day_ts < now − WEB_TEXT_RETENTION_DAYS·86400` (365 d), except the newest non-NULL text per `(occupant_key, url, kind)` (07 diffs against it) | privacy/size |
| `maintenance` | — | `PRAGMA wal_checkpoint(TRUNCATE)`; `PRAGMA incremental_vacuum(DB_INCREMENTAL_VACUUM_PAGES)`; `PRAGMA optimize` | — |

Kept forever (no policy): `pool_history`, `price_bars` 1h/1d, `chain_hourly`, `chain_daily`, official `metric_values` (both stages), `scores`, `score_runs`, `events`, `collector_runs` (master §11: long-term audit), identity tables, GitHub commits, owner tables (`owner_position_hourly` ≈ 3 k rows/day — a 400-day rollup is an open question, §11), labels, snapshots, `block_times`, `backfill_buckets`, `rpc_owner_history`. TaoSentinel never reads third-party Discord servers; Discord text arriving in any other way than a crosspost into the operator's own aggregator server is never written (§3.5 CHECK, 02 §B10).

### 2.8 Backup and restore (ownership: spec 16)

The pipeline (`create_backup`: lock → wait ≤ 20 min for `storage_retention_daily` → preflight → `Database.backup_to` → manifest `name.json` → gzip + verify, keeping the newest copy also uncompressed (`DB_BACKUP_KEEP_UNCOMPRESSED=1`) → mirror with marker → 7 daily + 8 weekly rotation inside `DB_BACKUP_MAX_TOTAL_GB` = 12 for the local dir, never deleting the 2 newest; the mirror has its own `DB_BACKUP_MIRROR_MAX_TOTAL_GB`; 02 §I1), `latest_backup_age_hours` (manifest-based), the `backup_daily` cron job (03:30Z), the `python -m app backup {list,verify,now,prune,restore}` CLI and the restore runbook are **specified in spec 16 §2.7–§2.8 and §7.3** and are not repeated here. This spec fixes only what the storage layer guarantees to that pipeline:

1. **Primitive.** `Database.backup_to(dest)` (§2.3 item 7) is the only way a snapshot is taken: `VACUUM INTO <dest>.tmp` from a dedicated connection, `PRAGMA quick_check` on the copy, atomic rename to `<dest>` (02 §I1). Never run in the start path (old 2026-06-29 incident, master §2); never a full in-place `VACUUM`. Spec 13's analysis snapshot is the newest **uncompressed** backup `.db`, never the live DB and never a `*.tmp` file (02 §J2).
2. **Schema compatibility.** A snapshot's schema version is `MAX(version) FROM schema_version`. Restoring a snapshot **older** than `CODE_SCHEMA_VERSION` is allowed (boot migrates forward, §2.4). Restoring a **newer** snapshot is refused by spec 16's guard (`schema_version(snapshot) ≤ CODE_SCHEMA_VERSION`), even though the additive policy would tolerate it at boot — a restore is a deliberate act, so it must match the code. (A *live* DB that is ahead of the code after an auto-update rollback is tolerated with `schema_ahead`, §2.4 step 5.)
3. **What a restore loses.** Everything written after the snapshot: raw observations are re-observed by the collectors (history for that window is lost for sources without history endpoints — Discord/Telegram counts, X engagement, web checks), `scores` for missed slots are re-run by spec 12's catch-up with `backfilled=1`, `chain_hourly` gaps are repaired from TMC `?span=1W` (spec 05), and `provider_spend` is carried over by spec 16's restore step 4.
4. **Quick check after an unclean boot** is spec 16's (`DB_QUICK_CHECK_ON_UNCLEAN`); `python -m app db check` (§8) runs `PRAGMA quick_check` + orphan-FK report on demand.

### 2.9 Legacy import compatibility (target contract for spec 17)

The import *code* is spec 17's (`app/backfill/ledger_import.py`, `occupancy.py`, `bars.py`, `fundamentals_import.py`); this section fixes the storage contract those modules must satisfy. There is no separate `app/storage/legacy.py` (it would duplicate spec 17's modules).

**`pool_history`** keeps the nine old columns with the old names, order, types and constraints (`models.sql:55-70` + index `idx_pool_history_subnet_ts`, lines 72-73), non-STRICT, rowid table, so an `INSERT OR IGNORE INTO pool_history (<9 old columns>) SELECT … FROM old.pool_history` from any old ledger works unchanged. Appended after them are **nullable** provenance columns (spec 17 §3.2 + this spec): `occupant_key, source, event_at, observed_at, price_method, enriched_at, run_id`. Spec 17's import (per netuid, PC → Pi → extras order, first writer wins, `source ∈ {ledger_pc, ledger_pi, ledger_extra}`; `observed_at = timestamp` only for rows the old bot captured live, `timestamp ≥ LEDGER_LIVE_FROM_TS` (1777468595 = 2026-04-29T13:16:35Z), while the 41,105 earlier rows (Taostats seed fetched 2026-05-09, hourly 04-22 → 04-29 backfill) get the import run's start, because they were fetched later (17 §2.1); rows with `price <= 0 OR tao_in_pool <= 0` skipped — the old writer's rule, `chain_client.py:450` `if (s["price"] or 0) > 0`; readers `dereg_analysis.py:74-76`, `pool_history_loader.py:167`):
```sql
ATTACH DATABASE 'file:seed/ledger.db?mode=ro&immutable=1' AS src;   -- scripts only (uri=True); never in migrations;
                                                                   -- a source with a non-empty -wal is first copied
                                                                   -- via sqlite3 backup() and the copy attached (spec 17)
INSERT OR IGNORE INTO main.pool_history
  (netuid, block_number, timestamp, price, tao_in_pool, alpha_in_pool, moving_price, network_registered_at,
   subnet_volume, occupant_key, source, event_at, observed_at, price_method)
SELECT netuid, block_number, timestamp, price, tao_in_pool, alpha_in_pool, moving_price,
       CASE WHEN timestamp >= :trust THEN network_registered_at END,   -- LEDGER_NRA_TRUST_FROM_TS = 1780182493
       subnet_volume,
       CASE WHEN timestamp >= :trust AND network_registered_at IS NOT NULL THEN netuid || ':' || network_registered_at END,
       :source, NULL, CASE WHEN timestamp >= :live_from THEN timestamp ELSE :run_started END, NULL
FROM src.pool_history WHERE netuid = :netuid AND price > 0 AND tao_in_pool > 0;
DETACH DATABASE src;
```
**NULL-only enrichment** (merge of `ledger_backfill.db`'s 23,237 extra `moving_price`/`subnet_volume` values, spec 17 §2.1 step 4, and occupant stamping, spec 17 §2.2) is the only UPDATE allowed: `UPDATE pool_history SET <col> = COALESCE(<col>, :new) …`. The merge of observed values (`moving_price`, `subnet_volume`, `network_registered_at`) also sets `enriched_at = COALESCE(enriched_at, :now)`; occupant stamping does **not** set `enriched_at` (the attribution is derived metadata whose provenance is the `occupants` row), so live-captured prices keep `backfilled = 0` in their bars. Trigger `trg_pool_history_update` (§3.3) aborts any UPDATE that changes a key, `timestamp`, price/reserve column, `source`, `observed_at`, or overwrites a non-NULL enrichment value; `trg_pool_history_no_delete` aborts deletes. PIT: `observed_at` stays the original capture time; an enriched value was knowable only from `enriched_at` — spec 13 must treat enriched `moving_price` as reconstructed chain state (master §10 "honestly backtestable" P1 history), not as a live observation.

**Occupant derivation** (spec 17 `occupancy.py`, rule fixed here). `network_registered_at` is the successor's registration block: per netuid, the distinct trusted registration values in order of first appearance must be strictly increasing, and occupant `reg_i` owns blocks `[reg_i, reg_{i+1} − 1]`. Trusted observations are: every archive-RPC bucket (`rpc_owner_history`, `backfill_buckets`), ledger rows with `timestamp ≥ LEDGER_NRA_TRUST_FROM_TS` (earlier values may be back-stamped by the old `backfill_metadata.py` and are dropped at import), and spec 04's current `occupants`. A `pool_history` row gets `occupant_key = netuid:reg_i` when its `block_number` falls in an interval; seed-era rows below the earliest known registration stay `occupant_key NULL` (unattributable; spec 13 treats them as `occ=None`). Historical occupants are inserted into `occupants` with `source='legacy_ledger'` (ledger-derived) or `'rpc'` (archive reads), `status='deregistered'`, `dereg_reason='reoccupied'` when a successor exists else `'absent'`, `first_seen_block/last_seen_block` = min/max observed block, `registered_ts`/`deregistered_ts` from `block_times`; a current occupant's `status` is never changed by the import (04 owns it). Each historical transition gets one `netuid_reoccupied` event with `source='registry'` (the producer's source; there is no `backfill_*` source, 02 §B9/§J1), `alerted_at = observed_at` (suppressed) and `payload.backfilled = true` (catalog §3.9 backfill key set).

**`price_bars` derivation** (spec 17 `bars.py`, per netuid, periods `1h` and `1d`): `ts = (timestamp / P) * P`; `close`, `ts_last`, `block_number`, `tao_in_pool`, `alpha_in_pool`, `moving_price`, `occupant_key` from the `MAX(timestamp)` row of the bucket (old last-write-wins convention, `db.py:622-653`); `open` from the `MIN(timestamp)` row; `high`/`low` = max/min `price`; `volume` = Δ of the cumulative `subnet_volume` counter vs the previous bucket (NULL across occupant changes, gaps > 2 periods and counter resets; unit UNVERIFIED τ vs α); `source='ledger_import'` for `ledger_*` close rows, else the RPC source; `event_at = ts_last`. A bar keeps `observed_at = ts_last` and `backfilled=0` only when its close row was captured live (`ledger_*`, `timestamp ≥ LEDGER_LIVE_FROM_TS`, not enriched) **and** its `moving_price` came from such a row; every other bar (seed era, the 04-22 → 04-29 hourly rows, enriched values, RPC samples) gets `observed_at` = write time and `backfilled=1` (17 §2.5). Written with the price-bar precedence upsert (§3.3), which never downgrades a live `sse_rollup` bar and replaces a same-rank bar only when `excluded.ts_last > stored.ts_last` (02 §J1).

**`whale_events`, `whale_cohort`** (dormant until a TaoStats key exists, master §2): the old columns unchanged (`models.sql:136-167`); `whale_cohort` gains nullable provenance/extra columns from `data/whale_cohort.json` (`total_stake_tao, free_fraction, balance_rank, coldkey_swap, observed_at, source`); the file imports as one snapshot (`as_of_block = 8590789`, 200 rows).
---

## 3. Data contract

`app/storage/models.sql` is the concatenation of every SQL block in §3.1–§3.6 **in this order** (reference tables before the tables whose FKs point at them). Column comments are part of the contract. Inventory: **80 tables** (§3.1: 6, §3.2: 18, §3.3: 18 + 3 imported, §3.4: 13, §3.5: 16, §3.6: 6), **6 views** (§3.7), **14 triggers** (§3.3: 2, §3.6: 12). `tests/storage/test_schema.py` asserts these counts and every name.

### 3.1 Ops and engine-state tables (writers: 01 runtime, 12 scoring, 16 ops)

```sql
-- class: append (one row per applied migration). Owner: 03.
CREATE TABLE IF NOT EXISTS schema_version (
  version     INTEGER PRIMARY KEY,           -- 1 = models.sql baseline; ≥2 = migrations/NNNN_*.sql
  name        TEXT    NOT NULL,              -- 'baseline' | migration file stem
  checksum    TEXT    NOT NULL,              -- sha256 hex of the migration file bytes
  applied_at  INTEGER NOT NULL               -- unix s
) STRICT;

-- class: state (row inserted at job start, finished at end). Owner: 01 CollectorJob wrapper; scripts too.
CREATE TABLE IF NOT EXISTS collector_runs (
  run_id        TEXT PRIMARY KEY,            -- uuid4 hex | 'sse:<connected_at>' | 'script:<name>:<started_at>'
  job           TEXT NOT NULL,               -- JOB_TABLE id (01 §2.7) or script name
  started_at    INTEGER NOT NULL,            -- unix s; default observed_at of rows written by the run
  finished_at   INTEGER,                     -- NULL = in flight, or orphaned by a crash/power loss
  ok            INTEGER CHECK (ok IN (0,1)), -- NULL in flight
  rows          INTEGER NOT NULL DEFAULT 0,
  error         TEXT,                        -- "ExcType: message", ≤ 500 chars
  cost_usd      REAL    NOT NULL DEFAULT 0.0,
  duration_sec  REAL,
  note          TEXT,                        -- JobOutcome.note (e.g. watermark)
  meta_json     TEXT                         -- per-job detail (04: head_block, per-source ok/failed)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_collector_runs_job_started ON collector_runs(job, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_collector_runs_orphans     ON collector_runs(started_at) WHERE finished_at IS NULL;

-- class: state. Per-job runtime watermarks only (01 §3; 02 §B12). Pause state lives in control_state.
CREATE TABLE IF NOT EXISTS job_state (
  job         TEXT NOT NULL,                 -- job id or script name
  key         TEXT NOT NULL,                 -- 'last_ok' | collector-defined watermark keys
  value_json  TEXT NOT NULL,
  updated_at  INTEGER NOT NULL,
  PRIMARY KEY (job, key)
) STRICT, WITHOUT ROWID;

-- class: state. The ONLY persistent store for operator pauses (02 §B12): API, Telegram and CLI all write it
-- (16 app/control.py pause()/resume()); survives restarts; `resume` deletes the row (and only the row). The file
-- sentinel list is closed: KILL_SWITCH (paid collectors + LLM; free LongTasks keep running), PAUSE_ALL (every
-- non-ops_exempt job), SCORING_KILLED (no new scores published); data/control/<target>.paused files are an
-- emergency input the app reads and never writes; per-provider kill files are retired.
CREATE TABLE IF NOT EXISTS control_state (
  target      TEXT NOT NULL,                 -- exact job id (01 JOB_TABLE) or a 16 PAUSE_GROUPS name (e.g. discord, x,
                                             -- llm); never prefix matching; a group target never pauses ops_exempt jobs
                                             -- or 14's telegram_bot/alert_dispatch
  key         TEXT NOT NULL CHECK (key IN ('paused')),
  value       TEXT NOT NULL CHECK (value IN ('true','false')),
  reason      TEXT,
  set_by      TEXT NOT NULL,                 -- 'api' | 'telegram' | 'cli' | operator name
  set_at      INTEGER NOT NULL,
  expires_at  INTEGER,                       -- NULL = until resumed
  PRIMARY KEY (target, key)
) STRICT, WITHOUT ROWID;

-- class: state. Cost governor ledger (16 app/costs.py, 02 §G5). One row per spend provider per UTC month;
-- increments only (commit/record/flush), never overwritten.
CREATE TABLE IF NOT EXISTS provider_spend (
  provider    TEXT NOT NULL,                 -- canonical provider id (02 §G1, same string in limiter, governor, /health):
                                             -- tmc | github | twitterapi_io | official_v2 | anthropic | taostats | … (groups x/llm/taostats aggregate at read time)
  month       TEXT NOT NULL CHECK (month GLOB '[0-9][0-9][0-9][0-9]-[0-1][0-9]'),
  usd         REAL NOT NULL DEFAULT 0.0,
  requests    INTEGER NOT NULL DEFAULT 0,    -- request counts (02 §G1: replaces 05's provider_usage; not `calls`);
                                             -- shared across processes so a restart cannot reset TMC_MONTHLY_CAP (90k)
  units_json  TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(units_json)),
                                             -- provider units summed per key, e.g. {"posts": n, "profiles": n,
                                             -- "input_tokens": n, "output_tokens": n} (record(..., units=…), 02 §G5)
  updated_at  INTEGER NOT NULL,
  PRIMARY KEY (provider, month)
) STRICT, WITHOUT ROWID;

-- class: state (mutable). Red-flag hysteresis state machine of spec 12 §2.8 (one row per occupant × flag × spec,
-- created on first raise or manual action, updated every scoring run). History = red_flag_raised/_cleared events.
-- Read by 12 (next run), 14 (/flags). Owner: 12. Columns fixed by 02 §B7: the state "clearing" is derived as
-- active = 1 AND false_since IS NOT NULL; origin 'human' maps to human = 1. sticky = owner_control_change whose
-- trigger row has value_json.identity_churn = true (stays active until a manual clear, 02 §D5). A manual clear sets
-- suppress_until = cleared_at + 30 d, but new trigger evidence (trigger ids in detail differing from the cleared
-- ones) re-raises immediately (02 §D5).
CREATE TABLE IF NOT EXISTS red_flag_state (
  occupant_key          TEXT    NOT NULL,
  flag                  TEXT    NOT NULL,    -- master §7.5 flag name (catalog §3.9 RED_FLAGS)
  spec_version          TEXT    NOT NULL,
  active                INTEGER NOT NULL CHECK (active IN (0,1)),
  raised_at             INTEGER,             -- slot of the last activation
  last_true_at          INTEGER,             -- slot of the last True evaluation
  false_since           INTEGER,             -- slot of the first False after the last True (hysteresis clock)
  cleared_at            INTEGER,
  human                 INTEGER NOT NULL DEFAULT 0 CHECK (human IN (0,1)),    -- held by a manual 'confirmed'
  sticky                INTEGER NOT NULL DEFAULT 0 CHECK (sticky IN (0,1)),   -- owner_control_change + identity churn
  last_manual_clear_at  INTEGER,             -- manual_flags.set_at of the last applied manual clear
  suppress_until        INTEGER,             -- post-mortem override window (manual clear + 30 d)
  last_eval             TEXT    CHECK (last_eval IN ('true','false','none')),
  last_eval_as_of       INTEGER NOT NULL,
  detail_json           TEXT    CHECK (detail_json IS NULL OR json_valid(detail_json)),  -- trigger inputs
  updated_at            INTEGER NOT NULL,
  PRIMARY KEY (occupant_key, flag, spec_version)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_red_flag_state_active ON red_flag_state(spec_version, active);
```

### 3.2 Reference tables (writers: 04 registry, 09 KOL seed, 11 team/owner, 14 manual entry)

```sql
-- class: state (status/last_seen move; rows never deleted). Every occupant ever seen (master §5). Owner: 04; 17 adds history.
CREATE TABLE IF NOT EXISTS occupants (
  occupant_key          TEXT PRIMARY KEY,    -- "{netuid}:{network_registered_at}"
  netuid                INTEGER NOT NULL CHECK (netuid BETWEEN 0 AND 1023),
  network_registered_at INTEGER NOT NULL,    -- registration block
  status                TEXT NOT NULL CHECK (status IN ('active','pre_launch','deregistered')),
  name                  TEXT,                -- latest TMC name (convenience)
  symbol                TEXT,
  first_seen_block      INTEGER NOT NULL,
  first_seen_at         INTEGER NOT NULL,
  last_seen_block       INTEGER NOT NULL,
  last_seen_at          INTEGER NOT NULL,
  first_emission_block  INTEGER,
  activated_at          INTEGER,
  deregistered_at       INTEGER,
  dereg_reason          TEXT CHECK (dereg_reason IN ('reoccupied','absent','manual')),
  predecessor_key       TEXT,
  successor_key         TEXT,
  registered_at_est     INTEGER,             -- unix s estimate of the registration block's time (04: head-block math)
  registered_ts         INTEGER,             -- exact unix s of the registration block (Timestamp.Now, 17 block_times; 02 §J1)
  deregistered_ts       INTEGER,             -- exact unix s of the successor's registration block (17); NULL if unknown
  missing_runs          INTEGER NOT NULL DEFAULT 0,
  source                TEXT NOT NULL DEFAULT 'tmc'
                        CHECK (source IN ('tmc','rpc','legacy_ledger','onfinality','manual')),
  CHECK (occupant_key = netuid || ':' || network_registered_at)
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS ux_occupants_netuid_reg ON occupants(netuid, network_registered_at);
CREATE INDEX IF NOT EXISTS ix_occupants_netuid_status ON occupants(netuid, status);

-- class: state. Current occupant per netuid + denormalised resolved identity (written by 04 after merge R7).
CREATE TABLE IF NOT EXISTS subnets (
  netuid                INTEGER PRIMARY KEY CHECK (netuid BETWEEN 0 AND 1023),
  occupant_key          TEXT NOT NULL REFERENCES occupants(occupant_key),
  network_registered_at INTEGER NOT NULL,
  name                  TEXT,
  symbol                TEXT,
  status                TEXT NOT NULL CHECK (status IN ('active','pre_launch','deregistered')),
  is_active             INTEGER CHECK (is_active IN (0,1)),       -- TMC is_active (emission started)
  immune                INTEGER CHECK (immune IN (0,1)),          -- local rule: head − reg < NETWORK_IMMUNITY_PERIOD_BLOCKS
  owner_coldkey         TEXT,
  owner_hotkey          TEXT,
  head_block            INTEGER,
  website_url           TEXT,                -- identity_resolved(subnet_url).value_norm
  github_url            TEXT,                -- identity_resolved(github_repo)
  docs_url              TEXT,                -- identity_resolved(docs_url)
  discord_url           TEXT,                -- identity_resolved(discord)
  telegram_url          TEXT,                -- identity_resolved(telegram)
  x_handle              TEXT,                -- handle of identity_resolved(x_url), no '@'
  updated_at            INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_subnets_occupant ON subnets(occupant_key);

-- class: append (trigger-enforced). Per source, per field, PIT (master §6). Owner: 04.
CREATE TABLE IF NOT EXISTS identity_observations (
  id                INTEGER PRIMARY KEY,
  occupant_key      TEXT NOT NULL REFERENCES occupants(occupant_key),
  netuid            INTEGER NOT NULL,
  source            TEXT NOT NULL CHECK (source IN ('chain','subnetalpha','tmc_coldkey','tmc','manual','legacy')),
  field             TEXT NOT NULL,         -- 04 RESOLVED_FIELDS (subnet_name, github_repo, subnet_url, discord, x_url,
                                           -- telegram, docs_url, description, short_desc, logo_url, subnet_contact,
                                           -- owner_name, owner_url, owner_github, owner_discord, youtube, owner_coldkey,
                                           -- slug, twitter_embed_id) or '<field>__extra_<i>'
  value             TEXT,                  -- raw trimmed; '' = source reports empty
  value_norm        TEXT,
  ref_kind          TEXT,
  value_hash        TEXT NOT NULL,         -- sha256(value)
  is_junk           INTEGER NOT NULL DEFAULT 0 CHECK (is_junk IN (0,1)),
  junk_reason       TEXT,
  reclassified_from TEXT,
  observed_at       INTEGER NOT NULL,
  event_at          INTEGER,               -- subnetalpha modified_gmt; legacy as-of; NULL for chain (use block)
  block_number      INTEGER,               -- chain read block / TMC snapshot block
  run_id            TEXT NOT NULL,
  source_ref        TEXT
) STRICT;
CREATE INDEX IF NOT EXISTS ix_idobs_latest ON identity_observations(occupant_key, source, field, observed_at DESC);
CREATE INDEX IF NOT EXISTS ix_idobs_field  ON identity_observations(field, observed_at);

-- class: state. Did source S cover occupant O in its last ok run? (04 §4 absence semantics)
CREATE TABLE IF NOT EXISTS identity_coverage (
  source              TEXT NOT NULL,
  occupant_key        TEXT NOT NULL REFERENCES occupants(occupant_key),
  has_row             INTEGER NOT NULL CHECK (has_row IN (0,1)),   -- 0 = verifiably no record
  last_ok_at          INTEGER NOT NULL,
  last_ok_run_id      TEXT NOT NULL,
  last_ok_block       INTEGER,
  consecutive_misses  INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (source, occupant_key)
) STRICT, WITHOUT ROWID;

-- class: append. Raw payload audit, written only when the payload hash changes. Owner: 04.
CREATE TABLE IF NOT EXISTS identity_raw_snapshots (
  id            INTEGER PRIMARY KEY,
  source        TEXT NOT NULL,
  scope         TEXT NOT NULL CHECK (scope IN ('subnet','coldkey')),
  netuid        INTEGER,
  occupant_key  TEXT,
  source_ref    TEXT,
  payload_json  TEXT NOT NULL,
  payload_hash  TEXT NOT NULL,
  observed_at   INTEGER NOT NULL,
  event_at      INTEGER,
  block_number  INTEGER,
  run_id        TEXT NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_idraw ON identity_raw_snapshots(source, occupant_key, observed_at DESC);

-- class: state. Per-field resolved winner (history = identity_observations + events). Owner: 04.
CREATE TABLE IF NOT EXISTS identity_resolved (
  occupant_key        TEXT NOT NULL REFERENCES occupants(occupant_key),
  field               TEXT NOT NULL,
  value               TEXT,
  value_norm          TEXT,
  ref_kind            TEXT,
  source              TEXT,
  source_observed_at  INTEGER,
  status              TEXT NOT NULL CHECK (status IN ('present','absent','junk_only','unknown')),
  candidates_json     TEXT NOT NULL,
  override_id         INTEGER,
  flags_json          TEXT,
  resolved_at         INTEGER NOT NULL,
  run_id              TEXT NOT NULL,
  PRIMARY KEY (occupant_key, field)
) STRICT, WITHOUT ROWID;

-- class: state (status transitions only). Identity overrides (master §5 "manual overrides"). Owner: 04.
CREATE TABLE IF NOT EXISTS manual_overrides (
  id                   INTEGER PRIMARY KEY,
  occupant_key         TEXT NOT NULL REFERENCES occupants(occupant_key),
  field                TEXT NOT NULL,      -- identity field only (product status lives in manual_notes)
  mode                 TEXT NOT NULL CHECK (mode IN ('fallback','force','suppress')),
  value                TEXT,
  value_hash           TEXT,
  suppress_source      TEXT,
  suppress_value_hash  TEXT,
  source               TEXT NOT NULL CHECK (source IN ('manual','legacy')),
  source_ref           TEXT,
  status               TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','needs_review','rejected','retired')),
  reason               TEXT,
  evidence_url         TEXT,
  resolves_event_id    INTEGER,
  created_at           INTEGER NOT NULL,
  created_by           TEXT NOT NULL,
  retired_at           INTEGER
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS ux_manual_overrides ON manual_overrides(occupant_key, field, mode, source,
  COALESCE(value_hash,''), COALESCE(suppress_source,''), COALESCE(suppress_value_hash,''));

-- class: state (retire, never delete). Venues/targets per occupant (master §6). Owners: 04 (identity rows),
-- 07 (web_autodiscover docs rows), 08 (discord guild resolution, verified_at), 09 (feeds.csv rows, youtube channel ids).
CREATE TABLE IF NOT EXISTS watch_channels (
  id                INTEGER PRIMARY KEY,
  occupant_key      TEXT REFERENCES occupants(occupant_key),   -- NULL = ecosystem-wide feed (09 feeds.csv)
  netuid            INTEGER,                                   -- denormalised from occupant_key (NULL if ecosystem)
  platform          TEXT NOT NULL CHECK (platform IN ('discord_invite','discord_channel','discord_handle','x','telegram',
                      'youtube','youtube_video','website','docs','rss','medium','substack','google_news')),
  handle            TEXT NOT NULL,         -- invite code | channel id | x handle | tg handle | UC… id | normalised URL
  url               TEXT,
  label             TEXT,                  -- feeds.csv label
  source            TEXT NOT NULL CHECK (source IN ('chain','subnetalpha','tmc','tmc_coldkey','manual','legacy',
                      'operator','web_autodiscover','seed_feeds')),
  priority          INTEGER NOT NULL DEFAULT 50,   -- lower wins: chain 10, subnetalpha 20, tmc/tmc_coldkey 30,
                                                   -- manual(force) 5, manual(fallback)/legacy 40, operator 45, autodiscover 90
  is_resolved       INTEGER NOT NULL DEFAULT 0 CHECK (is_resolved IN (0,1)),   -- equals the resolved winner
  is_main_guild     INTEGER NOT NULL DEFAULT 0 CHECK (is_main_guild IN (0,1)),
  guild_id          TEXT,
  needs_resolution  INTEGER NOT NULL DEFAULT 0 CHECK (needs_resolution IN (0,1)),
  filtered          INTEGER NOT NULL DEFAULT 0 CHECK (filtered IN (0,1)),     -- 09: keyword-filter publisher feed
  verified_at       INTEGER,               -- set only by the collector that validated the target
  first_seen_at     INTEGER NOT NULL,
  last_seen_at      INTEGER NOT NULL,
  retired_at        INTEGER,
  active            INTEGER GENERATED ALWAYS AS (retired_at IS NULL) VIRTUAL,
  meta_json         TEXT
) STRICT;
-- NULL occupant_key must still be unique per (platform, handle): writers use INSERT OR IGNORE, which honours
-- expression unique indexes, then UPDATE by id.
CREATE UNIQUE INDEX IF NOT EXISTS ux_watch_channels ON watch_channels(COALESCE(occupant_key,''), platform, handle);
CREATE INDEX IF NOT EXISTS ix_watch_platform ON watch_channels(platform, retired_at);
CREATE INDEX IF NOT EXISTS ix_watch_guild ON watch_channels(guild_id) WHERE guild_id IS NOT NULL;

-- class: state (status transitions; history = team_member_* events, which are the PIT audit trail). Manual,
-- evidence-linked team table (master §6, checklist D1). Owner: 11 (seed, verify/reject domain API); 14 is the
-- transport (API/Telegram), calling 11's domain functions (02 §H3). PIT: a member counts at t iff verified_at ≤ t and (rejected_at IS NULL or > t);
-- team_doxxed is forward-only (master §10), so in-place status edits are acceptable.
CREATE TABLE IF NOT EXISTS team_members (
  id                  INTEGER PRIMARY KEY,
  occupant_key        TEXT    NOT NULL REFERENCES occupants(occupant_key),
  netuid              INTEGER NOT NULL,
  name                TEXT    NOT NULL CHECK (length(name) <= 120),
  name_norm           TEXT    NOT NULL,      -- 11 §2.2.3 step 5 normalisation (dedupe key)
  role                TEXT    CHECK (role IS NULL OR length(role) <= 60),
  profile_url         TEXT,
  profile_kind        TEXT    CHECK (profile_kind IN ('linkedin','x','github','website','other')),
  evidence_url        TEXT,                  -- primary evidence; required once status = 'verified'
  evidence_note       TEXT,
  evidence_json       TEXT    NOT NULL DEFAULT '[]' CHECK (json_valid(evidence_json)),
                                             -- [{url, domain, class:'personal'|'attestation'|'excluded', added_by, added_at, note}]
  evidence_count      INTEGER NOT NULL DEFAULT 0,   -- distinct non-excluded registrable domains (profile_url ∪ evidence)
  status              TEXT    NOT NULL DEFAULT 'candidate' CHECK (status IN ('candidate','verified','rejected')),
  source              TEXT    NOT NULL CHECK (source IN ('subnetalpha','legacy','manual','llm')),
  source_ref          TEXT,                  -- subnetalpha link@modified_gmt | legacy <file>:<line> | api | telegram | cli
  verified_by         TEXT,
  verified_at         INTEGER,               -- first verification (unix s)
  rejected_by         TEXT,
  rejected_at         INTEGER,
  reject_reason       TEXT,
  first_seen_at       INTEGER NOT NULL,      -- row creation
  last_checked_at     INTEGER,               -- last human verify / re-verify
  source_last_seen_at INTEGER,               -- last seed run whose parse still listed the name
  created_by          TEXT    NOT NULL,      -- seed_subnetalpha | seed_legacy | cli:<by> | api | telegram
  verified            INTEGER GENERATED ALWAYS AS (status = 'verified') VIRTUAL,   -- spec 14 compatibility
  created_at          INTEGER GENERATED ALWAYS AS (first_seen_at) VIRTUAL,        -- spec 14 compatibility
  CHECK (status <> 'verified' OR (evidence_url IS NOT NULL AND evidence_count >= 2
                                  AND verified_at IS NOT NULL AND verified_by IS NOT NULL)),   -- checklist D1
  UNIQUE (occupant_key, name_norm)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_team_members_occ ON team_members(occupant_key, status);

-- class: state. KOL registry (09 seed; operator edits credibility/verified/active/notes — never overwritten by re-seed).
CREATE TABLE IF NOT EXISTS kol_accounts (
  handle              TEXT PRIMARY KEY,    -- case-preserved
  handle_lower        TEXT NOT NULL UNIQUE,
  platform            TEXT NOT NULL DEFAULT 'x' CHECK (platform IN ('x')),
  author_id           TEXT,                -- X user id once resolved (x_accounts)
  display_name        TEXT,
  category            TEXT NOT NULL CHECK (category IN ('official','founder','explorer','fund','media','analyst','community')),
  credibility         REAL NOT NULL CHECK (credibility BETWEEN 0 AND 1),
  verified            INTEGER NOT NULL DEFAULT 0 CHECK (verified IN (0,1)),
  active              INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
  team_occupant_keys  TEXT,                -- JSON array; KOLs that are also a subnet's team account
  added_at            INTEGER NOT NULL,
  added_by            TEXT NOT NULL CHECK (added_by IN ('seed','operator')),
  notes               TEXT,
  updated_at          INTEGER NOT NULL
) STRICT;

-- class: append (new row per change; latest row per (occupant_key, kind) is current). Notes, product status,
-- aliases, LLM narratives, seed edge briefs. Owners: 14 (API/Telegram), 04 (seed), 10 (narrative/alias), 11.
CREATE TABLE IF NOT EXISTS manual_notes (
  id            INTEGER PRIMARY KEY,
  occupant_key  TEXT REFERENCES occupants(occupant_key),     -- NULL = ecosystem-wide
  kind          TEXT NOT NULL CHECK (kind IN ('note','product_status','alias','llm_narrative','seed_edge','legacy',
                                              'team_disclosure')),   -- + 'chart_read' (migration 0003, 19 §2.7; §3.10)
  value         TEXT,        -- product_status: none|waitlist|beta|live, NULL = cleared (→ None); alias: alias string;
                             -- team_disclosure (11): anonymous|clear (operator statement feeding team_doxxed absence)
  title         TEXT,
  body          TEXT,        -- free text: note, narrative (≤ 300 words), edge-brief excerpt
  evidence_url  TEXT,
  payload_json  TEXT,        -- llm_narrative: {risks[3], catalysts[3], confidence, model, evidence_sha256, tokens, cost_usd, ...}
  ref_event_id  INTEGER,     -- e.g. the netuid_reoccupied event a review note answers (04 needs-review)
  source_ref    TEXT,        -- 'research/subnet_edge/sn64_chutes.md', 'fundamentals.json@2026-06-02'
  created_at    INTEGER NOT NULL,
  created_by    TEXT NOT NULL,   -- 'api' | 'telegram' | 'cli' | 'seed_registry' | 'llm:<model>' | operator
  CHECK (kind <> 'product_status' OR value IS NULL OR value IN ('none','waitlist','beta','live')),
  CHECK (kind <> 'product_status' OR value IS NULL OR evidence_url IS NOT NULL),
  CHECK (kind <> 'team_disclosure' OR value IN ('anonymous','clear'))
) STRICT;
CREATE INDEX IF NOT EXISTS ix_manual_notes_occ_kind ON manual_notes(occupant_key, kind, created_at DESC);

-- class: append (trigger-enforced). Human-confirmed red flags and manual clears (14 /manual/flag, Telegram /flag,
-- 11 confirm_flag / key-compromise override). Latest row per (occupant_key, flag) with set_at ≤ cutoff is
-- authoritative; spec 12 §2.8 reads it (human hold, manual clear, 30-d suppression). 02 §D5: `confirmed` only for
-- owner_extraction and key_compromise; `cleared` for those two and owner_control_change, and only while the flag is
-- active (auto-raised or confirmed; 14 answers 409 flag_not_active otherwise). A human-confirmed
-- owner_extraction_evidence label (10) is entered here as flag owner_extraction.
CREATE TABLE IF NOT EXISTS manual_flags (
  id            INTEGER PRIMARY KEY,
  occupant_key  TEXT NOT NULL REFERENCES occupants(occupant_key),
  flag          TEXT NOT NULL CHECK (flag IN ('owner_extraction','key_compromise','owner_control_change')),
  state         TEXT NOT NULL CHECK (state IN ('confirmed','cleared')),
  evidence_url  TEXT NOT NULL,
  note          TEXT,
  set_by        TEXT NOT NULL,
  set_at        INTEGER NOT NULL,
  -- only owner_extraction / key_compromise are human-confirmable; owner_control_change may only be cleared (02 §D5)
  CHECK (state = 'cleared' OR flag IN ('owner_extraction','key_compromise'))
) STRICT;
CREATE INDEX IF NOT EXISTS ix_manual_flags_occ ON manual_flags(occupant_key, flag, set_at DESC);

-- class: state (status transitions). Items needing a human decision: 10 (LLM flag candidates), 11 (team candidates,
-- owner control/extraction/lock/source reviews; 11 owns the review domain functions, 02 §A2, and its kind
-- vocabulary). 04's needs-review list stays computed (no storage). 11's enqueue() merges into the OPEN row with the
-- same dedupe_base, else inserts dedupe_key = f"{dedupe_base}|{created_at}".
CREATE TABLE IF NOT EXISTS review_queue (
  id                 INTEGER PRIMARY KEY,
  occupant_key       TEXT,                 -- NULL only for ecosystem-wide items
  kind               TEXT NOT NULL CHECK (kind IN ('llm_flag_candidate',                          -- 10
                       'team_candidate','team_reverify','team_delisted',                            -- 11 team
                       'owner_extraction_candidate','owner_change_whitelist',                       -- 11 owner
                       'owner_source_disagreement','other')),
  suggested_flag     TEXT CHECK (suggested_flag IN ('owner_extraction','key_compromise','security_incident')),
  post_id            TEXT,                 -- 10: post_labels.post_id
  label_version      TEXT,                 -- 10: classifier_version of the label
  event_type         TEXT,
  materiality        INTEGER CHECK (materiality IS NULL OR materiality BETWEEN 1 AND 3),
  summary            TEXT,
  evidence_url       TEXT,
  ref_table          TEXT,                 -- 11: 'owner_changes' | 'team_members' | 'events'
  ref_id             INTEGER,
  priority           INTEGER NOT NULL DEFAULT 2 CHECK (priority BETWEEN 1 AND 3),   -- 1 urgent … 3 low
  payload_json       TEXT CHECK (payload_json IS NULL OR json_valid(payload_json)),
  dedupe_key         TEXT,                 -- 11: "{dedupe_base}|{created_at}"; 10: "{kind}|{post_id}|{label_version}"
  dedupe_base        TEXT,                 -- 11: e.g. "team_candidate|64:4123456", "owner_change_whitelist|<change_id>"
  created_at         INTEGER NOT NULL,
  status             TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','confirmed','rejected','resolved','expired')),
  resolved_by        TEXT,
  resolved_at        INTEGER,
  resolution_note    TEXT,
  resolution_action  TEXT CHECK (resolution_action IN ('confirm_flag','whitelist','key_compromise','dismiss','done',
                                                       'auto_cleared'))
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS ux_review_queue_post ON review_queue(kind, post_id, label_version)
  WHERE post_id IS NOT NULL;                                                    -- 10: one review per label, forever
CREATE UNIQUE INDEX IF NOT EXISTS ux_review_open_dedupe ON review_queue(dedupe_key)
  WHERE dedupe_key IS NOT NULL AND status = 'open';                             -- one OPEN item per key
CREATE INDEX IF NOT EXISTS ix_review_queue_open ON review_queue(status, priority, created_at);
CREATE INDEX IF NOT EXISTS ix_review_queue_base ON review_queue(dedupe_base, status) WHERE dedupe_base IS NOT NULL;

-- class: state (retire, never delete). Operator whitelist for disclosed multisig migrations, post-compromise
-- recoveries and false positives (master §7.3 control_stability_90d). PIT: effective for decisions with
-- t ≥ created_at and (retired_at IS NULL or t < retired_at). Owner: 11.
CREATE TABLE IF NOT EXISTS owner_event_whitelist (
  id            INTEGER PRIMARY KEY,
  occupant_key  TEXT    NOT NULL REFERENCES occupants(occupant_key),
  netuid        INTEGER NOT NULL,
  reason        TEXT    NOT NULL CHECK (reason IN ('multisig_migration','post_compromise_recovery','disclosed_transfer',
                                                   'hotkey_rotation','initial_alpha_accounted','false_positive','other')),
  applies_to    TEXT    NOT NULL CHECK (applies_to IN ('coldkey_change','hotkey_repoint','identity_churn','stake_events',
                                                       'perpetual_off','extraction','initial_alpha','all')),
  window_start  INTEGER NOT NULL,        -- unix s, inclusive
  window_end    INTEGER NOT NULL,        -- unix s, inclusive
  old_coldkey   TEXT,                    -- optional exact-match constraints
  new_coldkey   TEXT,
  owner_change_id INTEGER,               -- owner_changes.id (optional)
  evidence_url  TEXT    NOT NULL,
  note          TEXT,
  created_by    TEXT    NOT NULL,
  created_at    INTEGER NOT NULL,
  retired_by    TEXT,
  retired_at    INTEGER,
  CHECK (window_end >= window_start),
  CHECK (applies_to <> 'perpetual_off' OR reason IN ('post_compromise_recovery','false_positive'))
) STRICT;
CREATE INDEX IF NOT EXISTS ix_owner_wl_occ ON owner_event_whitelist(occupant_key, window_start);

-- class: state. X account registry (09). kind='kol' rows reference kol_accounts.
CREATE TABLE IF NOT EXISTS x_accounts (
  author_id            TEXT PRIMARY KEY,
  handle               TEXT NOT NULL,
  handle_lower         TEXT NOT NULL UNIQUE,
  kind                 TEXT NOT NULL CHECK (kind IN ('team','kol')),
  kol_handle           TEXT REFERENCES kol_accounts(handle),
  status               TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','gone','unresolved')),
  since_id             TEXT,
  resolved_at          INTEGER,
  last_polled_at       INTEGER,
  last_post_at         INTEGER,
  last_error           TEXT,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  provider             TEXT,
  created_at           INTEGER NOT NULL
) STRICT;

-- class: state. Team account ↔ occupant mapping (shared handles map to several occupants).
CREATE TABLE IF NOT EXISTS x_account_occupants (
  author_id     TEXT NOT NULL REFERENCES x_accounts(author_id),
  occupant_key  TEXT NOT NULL REFERENCES occupants(occupant_key),
  source        TEXT NOT NULL,
  PRIMARY KEY (author_id, occupant_key)
) STRICT, WITHOUT ROWID;

-- class: state. Mention-search watermarks (09).
CREATE TABLE IF NOT EXISTS x_search_state (
  query_key      TEXT PRIMARY KEY,
  since_id       TEXT,
  last_run_at    INTEGER,
  last_posts     INTEGER,
  last_cost_usd  REAL
) STRICT;

-- class: state. RSS/news/YouTube poller state (09 Appendix A).
CREATE TABLE IF NOT EXISTS feed_state (
  feed_url              TEXT PRIMARY KEY,
  kind                  TEXT NOT NULL,       -- watch_channels.platform
  occupant_key          TEXT,
  effective_url         TEXT,
  etag                  TEXT,
  last_modified         TEXT,                -- HTTP header value verbatim (not a timestamp column)
  last_fetched_at       INTEGER,
  last_status           INTEGER,
  consecutive_failures  INTEGER NOT NULL DEFAULT 0,
  items_seen            INTEGER NOT NULL DEFAULT 0,
  last_new_item_at      INTEGER,
  enabled               INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0,1))
) STRICT;
```

### 3.3 Raw chain tables, price bars, chain intermediates (writers: 05, 17)

Source precedence for `precedence`-class tables is a code constant in `app/storage/precedence.py` and is applied in the upsert `WHERE` clause (`rank(excluded.source) > rank(stored.source)`, plus the same-rank rule noted per table):

| table | rank (high → low) | same-rank replace allowed? |
|---|---|---|
| `chain_hourly` | `sse` = `rest_table` (4) > `tick_rollup` (3) > `analytics_1w` (2) > `analytics_1m` (1) | yes for rank 4 when `excluded.block_number >= stored.block_number` (a later tick of the same hour, 05 §2.2) |
| `chain_daily` | `analytics_1y` (2) > `analytics_1w` (1) (05's `upsert_daily_from_analytics` and 17 use this rule; a pre-set `occupant_key` is kept: `occupant_key = COALESCE(stored.occupant_key, excluded.occupant_key)`, 02 §J1) | no (a same-rank row is never overwritten) |
| `price_bars` | `sse_rollup` (5) > `ledger_import` (4) > `tmc_candle` (3) > `onfinality` = `opentensor_archive` = `blockmachine` (2, migration 0002) > `taostats` (1) (spec 17 §2.5: a live bar is never downgraded; TMC OHLC beats a single-sample RPC bar; enrichment columns `tao_in_pool, alpha_in_pool, moving_price, block_number, occupant_key, tao_price_usd` use `COALESCE(excluded.x, stored.x)`) | yes when `excluded.ts_last > stored.ts_last` (a later close sample of the same bucket, 02 §J1; never for `tmc_candle`, whose `ts_last` is NULL); otherwise a lower- or same-ranked source may only fill NULL reserve columns (spec 17 `enrich_reserves`) |

Upsert `WHERE` for all three tables: `src_rank(excluded.source) > src_rank(stored.source) OR (src_rank(excluded.source) = src_rank(stored.source) AND <same-rank rule>)`, the same-rank rule being `excluded.block_number >= stored.block_number` (`chain_hourly`, rank 4 only), `excluded.ts_last > stored.ts_last` (`price_bars`) or `0` (`chain_daily`).

`src_rank(source)` is registered on every connection as a deterministic SQL function (`conn.create_function("src_rank", 1, precedence.rank_any, deterministic=True)`) so the upsert `WHERE` clause can call it; ranks are per-table constants in `precedence.SOURCE_RANK`.

```sql
-- class: append. Per-block hot fields from the TMC SSE table (master §6 "12 hot fields per block"). Owner: 05.
-- Written every CHAIN_TICKS_BLOCK_STRIDE blocks (default 5 ≈ 1 min, + on flag changes). Kept
-- CHAIN_TICKS_RETENTION_DAYS (30; 02 §F2), then only chain_hourly + 5m bars survive (§2.7 chain_prune).
-- No event_at index: retention walks the PK per netuid (disk, §6).
CREATE TABLE IF NOT EXISTS chain_ticks (
  netuid                       INTEGER NOT NULL,
  block_number                 INTEGER NOT NULL,
  occupant_key                 TEXT    NOT NULL,
  event_at                     INTEGER NOT NULL,   -- payload block timestamp if present, else arrival (05 §2.1)
  observed_at                  INTEGER NOT NULL,
  source                       TEXT    NOT NULL CHECK (source IN ('sse','rest_table')),
  run_id                       TEXT,               -- 'sse:<connected_at>' or the REST-fallback run
  price                        REAL,               -- τ per α
  subnet_moving_price          REAL,               -- on-chain EMA price (drives pruning/emission)
  emission                     REAL,               -- % of total TAO emission, post-gate
  emission_enabled             INTEGER CHECK (emission_enabled IN (0,1)),
  immune                       INTEGER CHECK (immune IN (0,1)),              -- TMC value (quirk: false before start)
  deregistration_risk          INTEGER CHECK (deregistration_risk IN (0,1)),
  excess_tao                   REAL,               -- rao/block ("chain buys")
  subnet_ema_tao_flow          REAL,
  owner_personal_locked_alpha  REAL,               -- rao-scaled α as delivered (÷1e9 = α; SN64 613237766512173 ≈ 613 k α)
  total_locked_alpha           REAL,               -- rao-scaled α (÷1e9)
  total_subnet_stake           REAL,               -- rao-scaled α (÷1e9)
  tao_liquidity                REAL,               -- pool TAO in rao (÷1e9 = τ; SN64 199012885911739 ≈ 199 k τ)
  PRIMARY KEY (netuid, block_number)
) STRICT, WITHOUT ROWID;

-- class: precedence. Last state of each UTC hour for every TMC table field (master §6 "others hourly").
-- Named columns = ALL 44 fields of /subnets/table/ and the SSE `full`/`delta` rows (identical key set, verified
-- against the research probe dumps of 2026-09-22, block 9,123,732; `subnet` is stored as netuid) + the local
-- immune_local + analytics-only fields. Units are as delivered (mixed: see comments). A field TMC adds later is kept
-- verbatim in extras_json (and 05 logs tmc_schema_drift) until an additive migration promotes it. Owner: 05; 17 backfills.
CREATE TABLE IF NOT EXISTS chain_hourly (
  netuid                          INTEGER NOT NULL,  -- TMC `subnet`
  hour_ts                         INTEGER NOT NULL,
  occupant_key                    TEXT    NOT NULL,
  block_number                    INTEGER,           -- TMC `block_number`: last block of the hour seen (analytics: point block)
  event_at                        INTEGER NOT NULL,  -- time of the state written (last tick) / analytics point ts
  observed_at                     INTEGER NOT NULL,
  source                          TEXT    NOT NULL CHECK (source IN ('sse','rest_table','tick_rollup','analytics_1w','analytics_1m')),
  run_id                          TEXT,
  backfilled                      INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),
  name                            TEXT,
  symbol                          TEXT,
  logo_url                        TEXT,
  is_active                       INTEGER CHECK (is_active IN (0,1)),
  emission_enabled                INTEGER CHECK (emission_enabled IN (0,1)),
  immune                          INTEGER CHECK (immune IN (0,1)),   -- TMC value (quirk: False before start)
  immune_local                    INTEGER CHECK (immune_local IN (0,1)),   -- (block − reg) < NETWORK_IMMUNITY_PERIOD_BLOCKS (864000)
  deregistration_risk             INTEGER CHECK (deregistration_risk IN (0,1)),
  price                           REAL,              -- τ per α
  price_after                     REAL,              -- τ per α after all unlocked stake sells (exit capacity)
  price_difference_hour           REAL,              -- % change
  price_difference_day            REAL,
  price_difference_week           REAL,
  price_difference_month          REAL,
  subnet_moving_price             REAL,              -- τ per α (on-chain EMA)
  emission                        REAL,              -- % of total TAO emission, post-gate
  excess_tao                      REAL,              -- rao/block ("chain buys")
  subnet_ema_tao_flow             REAL,              -- rao, signed
  owner_personal_stake            REAL,              -- rao-scaled α (÷1e9)
  owner_personal_locked_alpha     REAL,              -- rao-scaled α (÷1e9)
  owner_personal_perpetual_alpha  REAL,              -- rao-scaled α (÷1e9)
  owner_personal_decaying_alpha   REAL,              -- rao-scaled α (÷1e9)
  total_locked_alpha              REAL,              -- rao-scaled α (÷1e9)
  total_subnet_stake              REAL,              -- rao-scaled α (÷1e9)
  tao_liquidity                   REAL,              -- pool TAO, rao (÷1e9 = τ)
  alpha_liquidity                 REAL,              -- pool α, rao-scaled (÷1e9)
  circulating_supply              REAL,              -- rao-scaled α (÷1e9)
  marketcap                       REAL,              -- τ (= price × circulating α; SN64 ≈ 384,841)
  marketcap_rank                  INTEGER,
  fdv                             REAL,              -- rao (÷1e9 = τ)
  volume                          REAL,              -- τ, window UNVERIFIED (likely 24 h)
  miners_tao_per_day              REAL,              -- τ/day
  neuron_regs_burned_24h          REAL,              -- rao burned by neuron registrations, 24 h (UNVERIFIED unit)
  root_prop                       REAL,              -- %
  adr                             REAL,              -- alpha distribution ratio
  network_registered_at           INTEGER,
  subnet_owner                    TEXT,              -- owner coldkey ss58 (root: 0x00…)
  commits_7d                      INTEGER,
  commits_30d                     INTEGER,
  commits_90d                     INTEGER,
  chain_buys_per_block            REAL,              -- legacy flow-model field; stored, not used
  subsidized_breakpoint           REAL,              -- legacy; stored, not used
  is_subsidized                   INTEGER CHECK (is_subsidized IN (0,1)),
  total_tao_buy_24h               REAL,              -- analytics-only fields below (NULL on SSE rows)
  total_tao_sell_24h              REAL,
  volume_24h                      REAL,
  tx_count_24h                    INTEGER,
  neuron_registrations_24h        INTEGER,
  rao_recycled_24h                REAL,
  tao_price_usd                   REAL,
  extras_json                     TEXT,              -- any delivered field without a named column, verbatim
  PRIMARY KEY (netuid, hour_ts)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_chain_hourly_occ ON chain_hourly(occupant_key, hour_ts);

-- class: precedence. TMC analytics daily EOD rows (span=1Y) + daily rows derived from the 1W hourly pull. Owner: 05.
CREATE TABLE IF NOT EXISTS chain_daily (
  netuid                         INTEGER NOT NULL,
  day_ts                         INTEGER NOT NULL,
  occupant_key                   TEXT    NOT NULL,
  block_number                   INTEGER,
  event_at                       INTEGER NOT NULL,  -- analytics point timestamp
  observed_at                    INTEGER NOT NULL,
  source                         TEXT    NOT NULL CHECK (source IN ('analytics_1y','analytics_1w')),
  run_id                         TEXT,
  backfilled                     INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),
  subnet_tao                     REAL,              -- pool depth, rao (÷1e9 = τ; research probe SN51 153227189355104)
  total_supply                   REAL,
  circulation_supply             REAL,
  emission_percent               REAL,              -- = emission share %
  daily_emission                 REAL,
  daily_inflation                REAL,
  total_locked                   REAL,
  owner_personal_locked_alpha    REAL,
  owner_personal_decaying_alpha  REAL,
  total_decaying                 REAL,
  total_tao_buy                  REAL,
  total_tao_sell                 REAL,
  total_tao_buy_24h              REAL,
  total_tao_sell_24h             REAL,
  volume_24h                     REAL,
  tx_count_24h                   INTEGER,
  tx_amount_24h                  REAL,
  neuron_registrations_24h       INTEGER,
  rao_recycled_24h               REAL,
  chain_buys                     REAL,
  tao_price_usd                  REAL,
  tao_price_eur                  REAL,
  extras_json                    TEXT,
  PRIMARY KEY (netuid, day_ts)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_chain_daily_occ ON chain_daily(occupant_key, day_ts);

-- class: append. /analytics/trending/subnets/?limit=50, one row per listed subnet per pull. Owner: 05.
CREATE TABLE IF NOT EXISTS chain_trending (
  observed_at        INTEGER NOT NULL,
  netuid             INTEGER NOT NULL,
  occupant_key       TEXT,
  rank               INTEGER,
  views              INTEGER,
  visitors           INTEGER,
  percentage_change  REAL,
  event_at           INTEGER,
  source             TEXT NOT NULL DEFAULT 'tmc_trending',
  run_id             TEXT,
  PRIMARY KEY (observed_at, netuid)
) STRICT, WITHOUT ROWID;

-- class: rollup. Daily trending views per occupant for P6 attention_7d (10). Written by 05's daily job from the
-- day's LAST successful chain_trending pull: views = listed value, 0 when the occupant is absent from a successful
-- pull (verifiable absence); no row when every pull that day failed (→ NULL for 10). Replaces 10's
-- chain_daily.tmc_trending_views.
CREATE TABLE IF NOT EXISTS chain_trending_daily (
  day_ts        INTEGER NOT NULL,
  occupant_key  TEXT    NOT NULL,
  netuid        INTEGER NOT NULL,
  views         INTEGER NOT NULL,
  visitors      INTEGER,
  n_pulls_ok    INTEGER NOT NULL,
  observed_at   INTEGER NOT NULL,           -- = observed_at of the pull used
  run_id        TEXT,
  PRIMARY KEY (day_ts, occupant_key)
) STRICT, WITHOUT ROWID;

-- class: append. /extrinsics/staking-activity/pressure/ bucket=1d. Owner: 05.
CREATE TABLE IF NOT EXISTS chain_pressure (
  day_ts       INTEGER NOT NULL,
  side         TEXT    NOT NULL CHECK (side IN ('buy','sell')),
  netuid       INTEGER NOT NULL DEFAULT -1,   -- -1 = network-wide
  value_rao    REAL,
  ema_price    REAL,
  event_at     INTEGER,                       -- timestamp_ms / 1000
  observed_at  INTEGER NOT NULL,
  source       TEXT NOT NULL DEFAULT 'tmc_pressure',
  run_id       TEXT,
  PRIMARY KEY (day_ts, side, netuid, observed_at)
) STRICT, WITHOUT ROWID;

-- class: append. Network-level regime inputs (Σ subnet prices, TAO/USD). Owner: 05; read by 12 regime.
CREATE TABLE IF NOT EXISTS chain_network_hourly (
  hour_ts            INTEGER PRIMARY KEY,
  block_number       INTEGER,
  sum_subnet_prices  REAL,
  n_active           INTEGER,
  total_subnet_tao   REAL,
  tao_usd            REAL,
  tao_usd_source     TEXT,
  event_at           INTEGER,
  observed_at        INTEGER NOT NULL,
  source             TEXT NOT NULL DEFAULT 'sse',
  run_id             TEXT
) STRICT, WITHOUT ROWID;

-- class: append. Chain constants: TMC /general/constants/ (runtime defaults), live RPC storage values, config
-- snapshot. Merges 05's rpc_constants (source='rpc_storage'). Owner: 05.
CREATE TABLE IF NOT EXISTS chain_constants (
  name          TEXT    NOT NULL,             -- e.g. NetworkImmunityPeriod, EmissionBarQuantile, SubnetOwnerCut
  source        TEXT    NOT NULL CHECK (source IN ('tmc_constants','rpc_storage','config')),
  observed_at   INTEGER NOT NULL,
  value         TEXT    NOT NULL,             -- decimal/JSON string as delivered
  block_number  INTEGER,
  endpoint      TEXT,                         -- RPC endpoint used (rpc_storage)
  event_at      INTEGER,
  run_id        TEXT,
  PRIMARY KEY (name, source, observed_at)
) STRICT, WITHOUT ROWID;

-- class: append (retention 90 d, newest per endpoint kept). Raw sink for endpoints whose shape is unverified or
-- consumed by other specs (/subnets/, /accounts/coldkey-identities/, /subnets/burn/, …). Writers: whichever spec fetches the
-- endpoint through TmcClient — 04 (/subnets/, /accounts/coldkey-identities/, 02 §G2), 05, 11; any spec may read. DDL owner: 03.
CREATE TABLE IF NOT EXISTS chain_rest_snapshots (
  endpoint      TEXT    NOT NULL,             -- path template, e.g. '/subnets/'
  params        TEXT    NOT NULL DEFAULT '',  -- canonical query string (sorted)
  observed_at   INTEGER NOT NULL,
  run_id        TEXT    NOT NULL,
  http_status   INTEGER,
  payload_json  TEXT,
  event_at      INTEGER,
  source        TEXT NOT NULL DEFAULT 'tmc',
  PRIMARY KEY (endpoint, params, observed_at)
) STRICT, WITHOUT ROWID;

-- class: append. Weekly holder concentration (/accounts/subnet-holders/{n}/). Owner: 05.
CREATE TABLE IF NOT EXISTS holders_snapshots (
  netuid         INTEGER NOT NULL,
  observed_at    INTEGER NOT NULL,
  occupant_key   TEXT    NOT NULL,
  run_id         TEXT    NOT NULL,
  source         TEXT    NOT NULL DEFAULT 'tmc_holders',
  event_at       INTEGER,
  holders_count  INTEGER,
  n_fetched      INTEGER,
  top1_share     REAL,
  top10_share    REAL,
  hhi_topn       REAL,
  gini_topn      REAL,
  rows_json      TEXT,                        -- [{rank, coldkey, percentage, total_alpha}] fetched top-N
  PRIMARY KEY (netuid, observed_at)
) STRICT, WITHOUT ROWID;

-- class: precedence (§3.3 ranks). OHLC(V) bars: SSE rollups from chain_ticks (period '5m' and the live hourly
-- rollup, period '1h', both source='sse_rollup'; the CHECKs below deliberately do not tie source to period),
-- TMC 1h candles (gap fill), imported ledger 1h/1d, archive-RPC backfill (OnFinality / opentensor archive).
-- Owner: 05 (5m + 1h sse_rollup, tmc 1h), 17 (ledger, RPC, enrichment).
-- Read by 13 panel builder (via v_quant_price_bars).
CREATE TABLE IF NOT EXISTS price_bars (
  netuid            INTEGER NOT NULL,
  period            TEXT    NOT NULL CHECK (period IN ('5m','1h','1d')),
  ts                INTEGER NOT NULL,         -- bucket start (aligned)
  occupant_key      TEXT,                     -- NULL = unattributable (seed era, §2.9)
  open              REAL,
  high              REAL,
  low               REAL,
  close             REAL    NOT NULL CHECK (close > 0),
  volume            REAL,                     -- tmc_candle: as delivered; ledger/RPC: Δ of pool_history.subnet_volume
                                              -- (§2.9, unit τ vs α UNVERIFIED); sse_rollup: NULL
  start_volume      REAL,                     -- TMC candle field
  tao_price_usd     REAL,                     -- TMC candle field
  ts_last           INTEGER,                  -- timestamp of the close sample; NULL for tmc_candle
  tao_in_pool       REAL,                     -- τ; NULL when the source lacks reserves
  alpha_in_pool     REAL,                     -- α (TAO units, alpha/1e9)
  moving_price      REAL,
  block_number      INTEGER,                  -- chain-accurate block of the close sample (ledger/sse/onfinality)
  tmc_block_number  INTEGER,                  -- TMC candle label; NOT chain-accurate, never join on it
  source            TEXT    NOT NULL CHECK (source IN ('sse_rollup','tmc_candle','ledger_import','onfinality',
                                                       'opentensor_archive','blockmachine','taostats')),  -- blockmachine: migration 0002
  event_at          INTEGER,                  -- = ts_last when known
  observed_at       INTEGER NOT NULL,         -- = ts_last only for ledger bars captured live (ts_last ≥ 1777468595,
                                              -- not enriched, §2.9); else write time
  backfilled        INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),   -- 1 = reconstructed after the fact (17)
  run_id            TEXT,
  PRIMARY KEY (netuid, period, ts)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_price_bars_occ ON price_bars(occupant_key, period, ts) WHERE occupant_key IS NOT NULL;

-- class: append. Optional RPC/lite-node daily reads (05 §2 optional collector; as_of = 6-h slot, source 'rpc_storage')
-- and 17's daily emission pass (`--emission-daily`; as_of = day_ts 00:00Z, source = the archive endpoint). Owner: 05.
CREATE TABLE IF NOT EXISTS rpc_subnet_daily (
  netuid            INTEGER NOT NULL,
  as_of             INTEGER NOT NULL,
  occupant_key      TEXT    NOT NULL,
  block_number      INTEGER,
  observed_at       INTEGER NOT NULL,
  event_at          INTEGER,
  source            TEXT NOT NULL DEFAULT 'rpc_storage',
  run_id            TEXT,
  miner_burned      REAL,                     -- MinerBurned (U96F32 → float)
  subnet_owner_cut  REAL,
  emission_enabled  INTEGER CHECK (emission_enabled IN (0,1)),
  moving_price      REAL,
  endpoint          TEXT,
  emission_share    REAL,                     -- migration 0005: % = 100·tao_in_emission/Σ netuids ≠ 0 (17 only)
  PRIMARY KEY (netuid, as_of)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS rpc_metagraph_daily (
  netuid                    INTEGER NOT NULL,
  as_of                     INTEGER NOT NULL,
  occupant_key              TEXT    NOT NULL,
  block_number              INTEGER,
  observed_at               INTEGER NOT NULL,
  event_at                  INTEGER,
  source                    TEXT NOT NULL DEFAULT 'rpc_metagraph',
  run_id                    TEXT,
  n_uids                    INTEGER,
  max_uids                  INTEGER,
  active_validators         INTEGER,
  active_miners             INTEGER,
  permit_count              INTEGER,
  vtrust_mean               REAL,
  vtrust_pstdev             REAL,
  incentive_to_owner_share  REAL,
  endpoint                  TEXT,
  PRIMARY KEY (netuid, as_of)
) STRICT, WITHOUT ROWID;

-- class: append (one fit per as_of per model version). v440 gate reproduction (05 §2.3). Derived intermediate.
CREATE TABLE IF NOT EXISTS gate_fits (
  as_of          INTEGER NOT NULL,
  model_version  TEXT    NOT NULL,            -- 'gate-v440-1'
  block_number   INTEGER,
  n_eligible     INTEGER,
  bar_rank       INTEGER,
  exponent       REAL,
  theta          REAL,
  residual_pp    REAL,
  iterations     INTEGER,
  converged      INTEGER CHECK (converged IN (0,1)),
  degraded       INTEGER CHECK (degraded IN (0,1)),
  burn_source    TEXT CHECK (burn_source IN ('fit','rpc')),
  observed_at    INTEGER NOT NULL,
  run_id         TEXT    NOT NULL,
  PRIMARY KEY (as_of, model_version)
) STRICT, WITHOUT ROWID;

-- class: rollup (deterministic per (as_of, model_version)). Per-subnet gate outputs.
CREATE TABLE IF NOT EXISTS gate_subnet_daily (
  as_of                  INTEGER NOT NULL,
  model_version          TEXT    NOT NULL,
  netuid                 INTEGER NOT NULL,
  occupant_key           TEXT    NOT NULL,
  eligible               INTEGER NOT NULL CHECK (eligible IN (0,1)),
  demand_share           REAL,
  burn_adjusted_share    REAL,
  gate                   REAL,
  predicted_share        REAL,
  realised_share         REAL,
  predicted_gated_share  REAL,
  implied_miner_burn     REAL,
  fitted_burn            REAL,
  rpc_miner_burned       REAL,
  observed_at            INTEGER NOT NULL,
  run_id                 TEXT,
  PRIMARY KEY (as_of, model_version, netuid)
) STRICT, WITHOUT ROWID;

-- class: append. Exact block↔time anchors, ONLY from archive-RPC Timestamp.Now reads (never interpolated).
-- Owner: 17 (backfill_rpc, occupancy); read by 13 block_to_unix, 17 occupancy (registered_ts).
CREATE TABLE IF NOT EXISTS block_times (
  block_number  INTEGER PRIMARY KEY,
  ts_ms         INTEGER NOT NULL,           -- Timestamp.Now (ms)
  event_at      INTEGER GENERATED ALWAYS AS (ts_ms / 1000) VIRTUAL,   -- block time, unix s
  endpoint      TEXT    NOT NULL,           -- RPC endpoint used
  observed_at   INTEGER NOT NULL,
  source        TEXT    NOT NULL DEFAULT 'rpc_timestamp',
  run_id        TEXT
) STRICT, WITHOUT ROWID;

-- class: state (resume + audit ledger of the archive-RPC backfill; failed → done upsert). Owner: 17.
CREATE TABLE IF NOT EXISTS backfill_buckets (
  era           TEXT    NOT NULL CHECK (era IN ('gap','ledger','pre_ledger')),
  bucket_ts     INTEGER NOT NULL,           -- hour start (day start with --step 86400)
  status        TEXT    NOT NULL CHECK (status IN ('done','failed','skipped')),
  block_number  INTEGER,
  block_ts      INTEGER,
  n_subnets     INTEGER,
  rows_written  INTEGER,
  price_method  TEXT    CHECK (price_method IN ('swap_sqrt','reserve_ratio')),
  endpoint      TEXT,
  attempts      INTEGER NOT NULL DEFAULT 0,
  error         TEXT,
  observed_at   INTEGER NOT NULL,
  run_id        TEXT    NOT NULL,
  PRIMARY KEY (era, bucket_ts)
) STRICT, WITHOUT ROWID;

-- class: append (change-only log per netuid from archive-RPC buckets). Owner: 17; read by 11 (owner-key history,
-- S3 back-fill), 04 (name history).
CREATE TABLE IF NOT EXISTS rpc_owner_history (
  netuid         INTEGER NOT NULL,
  block_number   INTEGER NOT NULL,
  occupant_key   TEXT    NOT NULL,
  event_at       INTEGER NOT NULL,          -- block time (Timestamp.Now)
  observed_at    INTEGER NOT NULL,
  source         TEXT    NOT NULL DEFAULT 'rpc_archive',
  run_id         TEXT,
  owner_coldkey  TEXT,
  owner_hotkey   TEXT,
  subnet_name    TEXT,
  token_symbol   TEXT,
  change         TEXT    NOT NULL,          -- 'first' | comma list of changed fields
  PRIMARY KEY (netuid, block_number)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_rpc_owner_history_occ ON rpc_owner_history(occupant_key, block_number);
```

**Imported (legacy-compatible, non-STRICT; the nine `pool_history` columns and all `whale_*` columns are column-for-column identical to `E:\SN_Trader\SubnetTrader\app\storage\models.sql:55-73, 136-167` — names, order, types, constraints, defaults, indexes — with nullable provenance columns appended after them, §2.9):**

```sql
-- class: append + NULL-only enrichment (trigger-enforced, §2.5). Imported old-ledger pool series (7,362,854 rows,
-- 2025-09-22 → 2026-06-19, + Pi rows) and spec 17's archive-RPC samples. Owner: 17. Never pruned. Units: price τ/α,
-- tao_in_pool τ, alpha_in_pool α (never rao). The first nine columns are the old DDL; the rest are provenance.
CREATE TABLE IF NOT EXISTS pool_history (
    netuid       INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    timestamp    INTEGER NOT NULL,
    price        REAL    NOT NULL,
    tao_in_pool  REAL    NOT NULL,
    alpha_in_pool REAL   NOT NULL,
    moving_price          REAL,
    network_registered_at INTEGER,
    subnet_volume         REAL,
    occupant_key  TEXT,      -- NULL until stamped by 17's occupancy (seed era may stay NULL = unattributable)
    source        TEXT,      -- ledger_pc | ledger_pi | ledger_extra | onfinality | opentensor_archive (NULL: raw old copy)
    event_at      INTEGER,   -- rpc rows: exact block time; ledger rows: NULL (timestamp = capture wall clock)
    observed_at   INTEGER,   -- ledger rows ≥ LEDGER_LIVE_FROM_TS (1777468595): = timestamp (captured live); earlier
                             -- ledger rows: import run start (fetched later, 17 §2.1); rpc rows: read time
    price_method  TEXT,      -- rpc: swap_sqrt | reserve_ratio; ledger: NULL
    enriched_at   INTEGER,   -- set when a NULL observed value (moving_price, subnet_volume, network_registered_at)
                             -- was filled later (PIT: knowable from here); occupant stamping never sets it
    run_id        TEXT,      -- import/backfill run (collector_runs.run_id); NULL on raw old-ledger copies
    PRIMARY KEY (netuid, block_number)
);
CREATE INDEX IF NOT EXISTS idx_pool_history_subnet_ts
    ON pool_history(netuid, timestamp DESC);
-- no occupant_key index: stamping and bar derivation walk (netuid, block_number) / (netuid, timestamp); saves ≈ 250 MB

CREATE TRIGGER IF NOT EXISTS trg_pool_history_update BEFORE UPDATE ON pool_history
WHEN NEW.netuid IS NOT OLD.netuid OR NEW.block_number IS NOT OLD.block_number
  OR NEW.timestamp IS NOT OLD.timestamp OR NEW.price IS NOT OLD.price
  OR NEW.tao_in_pool IS NOT OLD.tao_in_pool OR NEW.alpha_in_pool IS NOT OLD.alpha_in_pool
  OR NEW.source IS NOT OLD.source OR NEW.observed_at IS NOT OLD.observed_at OR NEW.event_at IS NOT OLD.event_at
  OR NEW.price_method IS NOT OLD.price_method
  OR (OLD.moving_price IS NOT NULL AND NEW.moving_price IS NOT OLD.moving_price)
  OR (OLD.subnet_volume IS NOT NULL AND NEW.subnet_volume IS NOT OLD.subnet_volume)
  OR (OLD.network_registered_at IS NOT NULL AND NEW.network_registered_at IS NOT OLD.network_registered_at)
  OR (OLD.occupant_key IS NOT NULL AND NEW.occupant_key IS NOT OLD.occupant_key)
  OR (OLD.enriched_at IS NOT NULL AND NEW.enriched_at IS NOT OLD.enriched_at)
BEGIN SELECT RAISE(ABORT, 'pool_history: only NULL-only enrichment is allowed'); END;
CREATE TRIGGER IF NOT EXISTS trg_pool_history_no_delete BEFORE DELETE ON pool_history
BEGIN SELECT RAISE(ABORT, 'pool_history is append-only'); END;

-- Dormant (whale tracker port, activates with a TaoStats key).
CREATE TABLE IF NOT EXISTS whale_events (
    id            TEXT    PRIMARY KEY,
    coldkey       TEXT    NOT NULL,
    hotkey        TEXT,
    netuid        INTEGER NOT NULL,
    action        TEXT    NOT NULL,
    tao_amount    REAL    NOT NULL,
    alpha_amount  REAL    NOT NULL,
    price_tao     REAL,
    usd           REAL,
    fee_tao       REAL,
    slippage      REAL,
    is_transfer   INTEGER DEFAULT 0,
    size_vs_pool_pct REAL,
    block_number  INTEGER NOT NULL,
    timestamp     INTEGER NOT NULL,
    source        TEXT    DEFAULT 'taostats'
);
CREATE INDEX IF NOT EXISTS idx_whale_events_coldkey_ts ON whale_events(coldkey, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_whale_events_netuid_ts  ON whale_events(netuid, timestamp DESC);

CREATE TABLE IF NOT EXISTS whale_cohort (
    as_of_block   INTEGER NOT NULL,
    coldkey       TEXT    NOT NULL,
    rank          INTEGER,
    balance_total_tao REAL,
    label         TEXT,
    label_source  TEXT,
    attribution   TEXT DEFAULT 'heuristic',
    total_stake_tao REAL,    -- appended (17, whale_cohort.json)
    free_fraction   REAL,
    balance_rank    INTEGER,
    coldkey_swap    TEXT,
    observed_at     INTEGER, -- import time of the snapshot file
    source          TEXT,    -- 'whale_cohort_json' | 'taostats'
    PRIMARY KEY (as_of_block, coldkey)
);
```
(The in-file comments of the old DDL are dropped; the old columns' names, order, types, constraints, defaults and index definitions are identical, so `INSERT … (<old columns>) SELECT …` from an old ledger works unchanged. `tests/storage/test_schema.py` compares `PRAGMA table_info` of the first nine/sixteen/seven columns with the old DDL executed in memory.)

### 3.4 Raw GitHub and web tables (writers: 06, 07)

```sql
-- class: state. Canonical repos keyed by GitHub numeric id (stable across rename/transfer). Owner: 06.
CREATE TABLE IF NOT EXISTS github_repos (
  repo_id                INTEGER PRIMARY KEY,
  full_name              TEXT NOT NULL,              -- canonical "owner/name" as last verified
  owner                  TEXT NOT NULL,              -- lower-cased login
  name                   TEXT NOT NULL,
  owner_type             TEXT CHECK (owner_type IN ('Organization','User')),
  is_fork                INTEGER NOT NULL DEFAULT 0 CHECK (is_fork IN (0,1)),
  parent_full_name       TEXT,
  archived               INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0,1)),
  disabled               INTEGER NOT NULL DEFAULT 0 CHECK (disabled IN (0,1)),
  visibility             TEXT,
  pushed_at              INTEGER,
  created_at             INTEGER,
  updated_at             INTEGER,
  default_branch         TEXT,
  stars                  INTEGER,
  forks                  INTEGER,
  open_issues            INTEGER,
  subscribers            INTEGER,
  license                TEXT,                       -- license.spdx_id
  size_kb                INTEGER,
  description            TEXT,
  topics_json            TEXT,
  etag                   TEXT,
  resolution_method      TEXT NOT NULL CHECK (resolution_method IN ('direct','redirect','onchain','org_enum','manual','html')),
  declared_by_sources    TEXT NOT NULL DEFAULT '[]', -- JSON [{occupant_key, source, raw}]
  status                 TEXT NOT NULL DEFAULT 'ok' CHECK (status IN ('ok','gone','blocked')),
  consecutive_404        INTEGER NOT NULL DEFAULT 0,
  gone_since             INTEGER,
  first_seen_at          INTEGER NOT NULL,           -- PIT membership start (look-ahead flag in 13)
  last_verified_at       INTEGER NOT NULL,
  last_ok_at             INTEGER,
  last_commit_seen_at    INTEGER,                    -- commits watermark (max committer date stored)
  force_push_suspected   INTEGER NOT NULL DEFAULT 0 CHECK (force_push_suspected IN (0,1)),
  participation_json     TEXT,
  contributors_json      TEXT,
  hygiene_json           TEXT,
  hygiene_checked_at     INTEGER,
  originality_json       TEXT,
  originality_checked_at INTEGER
) STRICT;
-- migration 0004 (2026-10-17, 06 §2.5/§3.2, additive): ALTER TABLE github_repos ADD COLUMN activity_ok_at INTEGER
--   (last successful activity pass: 06's activity_stale clock) and ADD COLUMN stats_checked_at INTEGER (weekly /stats/*).
CREATE INDEX IF NOT EXISTS ix_github_repos_owner ON github_repos(owner, is_fork, archived, pushed_at);

-- class: append. Written when a tracked field changed, and at least daily per repo.
CREATE TABLE IF NOT EXISTS github_repo_snapshots (
  repo_id      INTEGER NOT NULL,
  observed_at  INTEGER NOT NULL,
  full_name    TEXT,
  pushed_at    INTEGER,
  stars        INTEGER,
  forks        INTEGER,
  open_issues  INTEGER,
  archived     INTEGER CHECK (archived IN (0,1)),
  size_kb      INTEGER,
  status       TEXT,
  event_at     INTEGER,                              -- = pushed_at
  source       TEXT NOT NULL DEFAULT 'github_api',
  run_id       TEXT,
  PRIMARY KEY (repo_id, observed_at)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS github_repo_aliases (       -- class: state
  repo_id        INTEGER NOT NULL,
  full_name      TEXT    NOT NULL,                   -- lower-cased
  source         TEXT    NOT NULL CHECK (source IN ('api','redirect','html','declared')),
  first_seen_at  INTEGER NOT NULL,
  last_seen_at   INTEGER NOT NULL,
  PRIMARY KEY (repo_id, full_name)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_github_repo_aliases_name ON github_repo_aliases(full_name);

CREATE TABLE IF NOT EXISTS github_orgs (               -- class: state
  owner                   TEXT PRIMARY KEY,          -- lower-cased
  login                   TEXT,
  kind                    TEXT NOT NULL CHECK (kind IN ('org','user','gone')),
  n_public                INTEGER,
  n_non_fork              INTEGER,
  n_archived              INTEGER,
  n_pushed_30d            INTEGER,
  max_pushed_at_non_fork  INTEGER,
  etag                    TEXT,
  first_seen_at           INTEGER NOT NULL,
  last_enumerated_at      INTEGER,
  last_ok_at              INTEGER,
  consecutive_failures    INTEGER NOT NULL DEFAULT 0
) STRICT;

CREATE TABLE IF NOT EXISTS github_declared_refs (      -- class: state; one row per (occupant, identity source)
  occupant_key       TEXT NOT NULL,
  source             TEXT NOT NULL CHECK (source IN ('chain','subnetalpha','tmc','manual','seed_fundamentals')),
  raw_value          TEXT NOT NULL,
  kind               TEXT CHECK (kind IN ('repo','owner','none')),
  owner              TEXT,
  repo               TEXT,
  ref_key            TEXT,
  is_placeholder     INTEGER NOT NULL DEFAULT 0 CHECK (is_placeholder IN (0,1)),
  state              TEXT NOT NULL CHECK (state IN ('new','resolved','gone_pending','gone','blocked','org_empty','placeholder','none')),
  repo_id            INTEGER,
  resolution_method  TEXT,
  first_seen_at      INTEGER NOT NULL,
  last_attempt_at    INTEGER,
  last_http_status   INTEGER,
  consecutive_404    INTEGER NOT NULL DEFAULT 0,
  first_404_at       INTEGER,
  resolved_at        INTEGER,
  observed_at        INTEGER NOT NULL,
  PRIMARY KEY (occupant_key, source)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS github_occupant_repos (     -- class: state; R(o) membership
  occupant_key         TEXT NOT NULL,
  repo_id              INTEGER NOT NULL,
  role                 TEXT NOT NULL CHECK (role IN ('declared','primary','org','declared_gone')),
  declared_source      TEXT,
  declared_by_sources  TEXT NOT NULL DEFAULT '[]',
  in_scope             INTEGER NOT NULL DEFAULT 0 CHECK (in_scope IN (0,1)),
  first_seen_at        INTEGER NOT NULL,
  last_seen_at         INTEGER NOT NULL,
  PRIMARY KEY (occupant_key, repo_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_github_occ_repos_repo ON github_occupant_repos(repo_id);

-- class: append. PIT: event_at = author date. sha is global (duplicates across repos counted once org-wide).
CREATE TABLE IF NOT EXISTS github_commits (
  sha                TEXT PRIMARY KEY,
  repo_id            INTEGER NOT NULL,
  author_key         TEXT NOT NULL,                  -- canonical author identity (github_author_identities)
  author_login       TEXT,
  author_name        TEXT,
  author_email_hash  TEXT,                           -- sha256(lower(email)); raw email never stored
  author_type        TEXT,
  event_at           INTEGER NOT NULL,
  committer_date     INTEGER,
  n_parents          INTEGER NOT NULL,
  message_head       TEXT,                           -- first line, ≤ 200 chars
  message_norm_hash  TEXT,
  verified           INTEGER CHECK (verified IN (0,1)),
  is_bot             INTEGER NOT NULL DEFAULT 0 CHECK (is_bot IN (0,1)),
  is_merge           INTEGER NOT NULL DEFAULT 0 CHECK (is_merge IN (0,1)),
  spam_flags         TEXT NOT NULL DEFAULT '[]',
  stats_total        INTEGER,
  whitespace_only    INTEGER CHECK (whitespace_only IN (0,1)),
  observed_at        INTEGER NOT NULL,
  source             TEXT NOT NULL DEFAULT 'github_api' CHECK (source IN ('github_api','github_atom')),
  run_id             TEXT NOT NULL,
  backfilled         INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1))   -- 17 backfill_github history pull
) STRICT;
CREATE INDEX IF NOT EXISTS ix_github_commits_repo_day ON github_commits(repo_id, event_at);
CREATE INDEX IF NOT EXISTS ix_github_commits_author   ON github_commits(author_key, event_at);

CREATE TABLE IF NOT EXISTS github_author_identities (  -- class: state
  alias_key      TEXT PRIMARY KEY,
  canonical_key  TEXT NOT NULL,
  first_seen_at  INTEGER NOT NULL
) STRICT;

-- class: rollup of github_commits (REPLACE per (repo_id, day_ts)).
CREATE TABLE IF NOT EXISTS github_activity_daily (
  repo_id        INTEGER NOT NULL,
  day_ts         INTEGER NOT NULL,                   -- UTC day of author date
  commits_all    INTEGER NOT NULL,
  commits_human  INTEGER NOT NULL,
  commits_bot    INTEGER NOT NULL,
  commits_merge  INTEGER NOT NULL,
  commits_spam   INTEGER NOT NULL,
  authors_human  INTEGER NOT NULL,
  observed_at    INTEGER NOT NULL,
  run_id         TEXT    NOT NULL,
  backfilled     INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),
  PRIMARY KEY (repo_id, day_ts)
) STRICT, WITHOUT ROWID;

-- class: rollup. Electric-Capital author classes, one row per author per occupant per run-day.
CREATE TABLE IF NOT EXISTS github_authors_daily (
  occupant_key    TEXT    NOT NULL,
  author_key      TEXT    NOT NULL,
  day_ts          INTEGER NOT NULL,
  commits_today   INTEGER NOT NULL,
  cd28            INTEGER NOT NULL,
  cd56            INTEGER NOT NULL,
  cd84_3          INTEGER NOT NULL,
  cd84            INTEGER NOT NULL,
  dev_class       TEXT    NOT NULL CHECK (dev_class IN ('full_time','part_time','one_time')),
  repos_json      TEXT    NOT NULL,
  observed_at     INTEGER NOT NULL,
  run_id          TEXT    NOT NULL,
  backfilled      INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),
  PRIMARY KEY (occupant_key, author_key, day_ts)
) STRICT, WITHOUT ROWID;

-- class: append. Raw repo-side events (not the occupant-level events table).
CREATE TABLE IF NOT EXISTS github_events (
  id           INTEGER PRIMARY KEY,
  repo_id      INTEGER NOT NULL,
  event_kind   TEXT NOT NULL CHECK (event_kind IN ('release','tag','pr_merged','archived','unarchived','renamed',
                 'transferred','gone','restored','default_branch_changed')),
  event_at     INTEGER,
  observed_at  INTEGER NOT NULL,
  dedupe_key   TEXT NOT NULL UNIQUE,
  payload_json TEXT NOT NULL,
  source       TEXT NOT NULL DEFAULT 'github_api',
  backfilled   INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),   -- 1 = written by 17's backfill (02 §J1)
  run_id       TEXT NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_github_events_repo_kind ON github_events(repo_id, event_kind, event_at);

CREATE TABLE IF NOT EXISTS github_http_cache (         -- class: state (cache; retention 30 d)
  url         TEXT PRIMARY KEY,
  etag        TEXT NOT NULL,
  status      INTEGER NOT NULL,
  body        BLOB NOT NULL,
  fetched_at  INTEGER NOT NULL
) STRICT;

-- class: append (content_text NULLed by retention). One row per (occupant, url, kind, UTC day). Owner: 07.
CREATE TABLE IF NOT EXISTS web_checks (
  id                     INTEGER PRIMARY KEY,
  occupant_key           TEXT    NOT NULL,
  netuid                 INTEGER NOT NULL,
  url                    TEXT    NOT NULL,           -- normalised declared URL
  kind                   TEXT    NOT NULL CHECK (kind IN ('website','docs')),
  is_primary             INTEGER NOT NULL DEFAULT 1 CHECK (is_primary IN (0,1)),
  source                 TEXT    NOT NULL,           -- watch_channels.source at check time
  event_at               INTEGER NOT NULL,           -- request start (07's checked_at)
  observed_at            INTEGER NOT NULL,           -- row write time
  check_day_ts           INTEGER NOT NULL,           -- UTC day of event_at
  run_id                 TEXT    NOT NULL,
  attempts               INTEGER NOT NULL DEFAULT 1,
  status_class           TEXT    NOT NULL CHECK (status_class IN ('up_2xx','redirect_ok','client_error','server_error',
                           'timeout','dns_fail','tls_fail','conn_error','too_many_redirects','bot_challenge','parked','hijacked_suspect')),
  http_status            INTEGER,
  final_url              TEXT,
  final_host             TEXT,
  redirect_hops          INTEGER NOT NULL DEFAULT 0,
  redirect_chain         TEXT,                       -- JSON [{url,status,location}]
  cross_domain           INTEGER NOT NULL DEFAULT 0 CHECK (cross_domain IN (0,1)),
  resolved_ip            TEXT,
  latency_ms             INTEGER,
  robots_allowed         INTEGER CHECK (robots_allowed IN (0,1)),
  tls_days_left          INTEGER,
  tls_ok                 INTEGER CHECK (tls_ok IN (0,1)),
  tls_hostname_match     INTEGER CHECK (tls_hostname_match IN (0,1)),
  tls_issuer             TEXT,
  tls_not_after          INTEGER,
  content_type           TEXT,
  extractor              TEXT CHECK (extractor IN ('trafilatura','readability','plain','raw','none')),
  is_spa                 INTEGER CHECK (is_spa IN (0,1)),
  content_hash           TEXT,                       -- sha256 of normalised text
  content_len            INTEGER,
  content_text           TEXT,                       -- only when hash changed (≤ WEB_STORE_TEXT_MAX_CHARS); retention §2.7
  change_ratio           REAL,
  parked                 INTEGER NOT NULL DEFAULT 0 CHECK (parked IN (0,1)),
  parked_signals         TEXT,
  hijack_signals         TEXT,
  docs_candidate_url     TEXT,
  wayback_first_capture  INTEGER,
  wayback_checked_at     INTEGER,
  error                  TEXT,
  UNIQUE (occupant_key, url, kind, check_day_ts)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_web_checks_url_day ON web_checks(url, kind, check_day_ts);
CREATE INDEX IF NOT EXISTS ix_web_checks_occ_day ON web_checks(occupant_key, kind, check_day_ts);
```

### 3.5 Raw community, social, NLP and owner tables (writers: 08, 09, 10, 11)

```sql
-- class: append (one row per probe attempt). Discord invite metadata (probe='invite'), widget.json samples
-- (probe='widget') and the per-occupant topology census (probe='census'). Owner: 08 (§3.2 DDL adopted).
-- Aggregate counts only: usernames/inviter/widget members[] are never stored (Developer Policy §16).
CREATE TABLE IF NOT EXISTS discord_snapshots (
  id                    INTEGER PRIMARY KEY,
  occupant_key          TEXT,                 -- NULL only for the shared main-guild vanity sample (one per cycle)
  netuid                INTEGER,              -- convenience copy
  probe                 TEXT    NOT NULL CHECK (probe IN ('invite','widget','census')),
  guild_id              TEXT,
  invite_code           TEXT,                 -- case-sensitive; NULL for widget rows and code-less census rows
  observed_at           INTEGER NOT NULL,     -- fetch time (census: classification time)
  event_at              INTEGER,              -- always NULL: counts describe "now" (kept for PIT uniformity)
  cycle_at              INTEGER NOT NULL,     -- scheduled slot start (6-h slot: invite/census; hour: widget)
  source                TEXT    NOT NULL DEFAULT 'discord_api',   -- 'discord_api' (keyless REST) | 'discord_bot'
  http_status           INTEGER,              -- NULL for census
  member_count          INTEGER,              -- approximate_member_count
  presence_count        INTEGER,              -- approximate_presence_count | widget presence_count
  presence_ratio        REAL,                 -- presence/member; NULL if either missing or member_count = 0
  invite_valid          INTEGER CHECK (invite_valid IN (0,1)),   -- 1 valid; 0 dead (404 10006/50270, expired,
                                                                 -- non-guild); NULL collection failure / n.a.
  invite_error_code     INTEGER,              -- Discord JSON error code on 404 (10006 unknown, 50270 expired)
  expires_at            INTEGER,              -- NULL = permanent invite
  landing_channel_id    TEXT,                 -- census main_channel rows: the main-server subnet channel id
  landing_channel_type  INTEGER,              -- 0 text, 2 voice, 5 announcement, 13 stage, 15 forum
  features_json         TEXT CHECK (features_json IS NULL OR json_valid(features_json)),   -- sorted JSON array
  boosts                INTEGER,              -- guild.premium_subscription_count
  verification_level    INTEGER,
  guild_created_at      INTEGER,              -- from the snowflake: ((id >> 22) + 1420070400000) / 1000
  widget_enabled        INTEGER CHECK (widget_enabled IN (0,1)),   -- widget rows: 1 (200), 0 (403/50004), NULL other
  topology_class        TEXT CHECK (topology_class IN ('own_server','main_channel','main_vanity','invalid',
                                                       'handle_only','none','unknown')),
  is_primary            INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0,1)),
  unknown_targets       INTEGER NOT NULL DEFAULT 0,   -- targets without a successful observation in their window
  source_agreement      TEXT CHECK (source_agreement IN ('agree','disagree','one_dead','single_source')),
  declared_by_json      TEXT CHECK (declared_by_json IS NULL OR json_valid(declared_by_json)),
                                              -- JSON array of watch_channels.source values declaring this code
  meta_json             TEXT CHECK (meta_json IS NULL OR json_valid(meta_json)),
                                              -- {guild_name, vanity_url_code, premium_tier, nsfw_level, invite_type,
                                              --  invite_flags, landing_channel_name, profile_visibility,
                                              --  instant_invite_code, channel_count, error}
  run_id                TEXT    NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_dsnap_occ_obs   ON discord_snapshots(occupant_key, observed_at);
CREATE INDEX IF NOT EXISTS ix_dsnap_guild_obs ON discord_snapshots(guild_id, probe, observed_at);
CREATE INDEX IF NOT EXISTS ix_dsnap_code_obs  ON discord_snapshots(invite_code, observed_at);
CREATE INDEX IF NOT EXISTS ix_dsnap_cycle     ON discord_snapshots(cycle_at, probe);

-- class: append. t.me profile/preview page samples, one row per (occupant, handle, UTC day). Owner: 08.
CREATE TABLE IF NOT EXISTS telegram_snapshots (
  id                 INTEGER PRIMARY KEY,
  occupant_key       TEXT    NOT NULL,
  netuid             INTEGER,
  handle             TEXT    NOT NULL,        -- public handle (case preserved) or '+hash'
  observed_at        INTEGER NOT NULL,
  event_at           INTEGER,                 -- always NULL
  cycle_day_ts       INTEGER NOT NULL,        -- UTC day start of the run
  source             TEXT    NOT NULL DEFAULT 'tme_page',
  http_status        INTEGER,                 -- profile page status; NULL on transport failure
  kind               TEXT CHECK (kind IN ('group','channel','private_group','private_channel','bot',
                                          'user_or_unknown','not_found')),
  title              TEXT,                    -- og:title of community kinds only (group/channel); NULL for user/bot accounts
  members            INTEGER,                 -- groups: members; channels: subscribers
  online             INTEGER,                 -- groups only
  online_ratio       REAL,                    -- online / members
  deprecated_marker  INTEGER NOT NULL DEFAULT 0 CHECK (deprecated_marker IN (0,1)),
  preview_available  INTEGER CHECK (preview_available IN (0,1)),   -- public channels: t.me/s rendered >= 1 post
  preview_posts      INTEGER,                 -- post blocks parsed this run
  last_post_at       INTEGER,                 -- newest <time datetime> in the preview
  parse_ok           INTEGER NOT NULL CHECK (parse_ok IN (0,1)),   -- 0 = HTML changed/unparseable (collection failure)
  error              TEXT,
  run_id             TEXT    NOT NULL,
  UNIQUE (occupant_key, handle, cycle_day_ts)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_tgsnap_occ_obs ON telegram_snapshots(occupant_key, observed_at);

-- class: append (annotation: deleted_at; text NULLed by §2.7 nlp_text_retention). Discord messages crossposted
-- (Follow) into the operator's aggregator server, and Telegram channel preview posts (02 §B11: all Discord and
-- Telegram posts land here; 08 never writes social_posts). Owner: 08. 02 §B10: TaoSentinel never reads third-party
-- servers; Discord text exists ONLY for crossposts received in the operator's own server (via_own_server = 1;
-- user/role mention ids redacted by 08; CHECK below). With the platform's *_STORE_TEXT flag true
-- (DISCORD_STORE_TEXT default true) text is kept 90 d; with it false (TELEGRAM_POSTS_STORE_TEXT default false)
-- the text is pending-classification only: written with text_purge_after = observed_at + LLM_PENDING_TEXT_MAX_DAYS
-- × 86400 and NULLed by §2.7 once a post_labels row exists or text_purge_after passes (amended 02 §B10).
CREATE TABLE IF NOT EXISTS announcements (
  msg_id             TEXT PRIMARY KEY,        -- 'discord:{source_message_id}' or 'telegram:{handle_lower}/{post_no}'
                                              -- = spec 10 item_id = post_labels.post_id (02 §B11)
  platform           TEXT    NOT NULL CHECK (platform IN ('discord','telegram')),
  occupant_key       TEXT,                    -- set iff exactly one occupant is mapped (all: social_post_mentions)
  netuid             INTEGER,
  source_guild_id    TEXT,                    -- discord: message.reference.guild_id; telegram: NULL
  source_channel_id  TEXT    NOT NULL,        -- discord: message.reference.channel_id; telegram: handle_lower
  message_id         TEXT    NOT NULL,        -- discord: original message id; telegram: post number
  local_channel_id   TEXT,                    -- aggregator channel the copy arrived in
  local_message_id   TEXT,                    -- id of the crosspost copy (delete events)
  via_own_server     INTEGER NOT NULL DEFAULT 0 CHECK (via_own_server IN (0,1)),   -- spec 10 privacy gate
  mapping            TEXT    NOT NULL CHECK (mapping IN ('channel','guild','guild_ambiguous','main_announce',
                                                         'unmapped','tg_handle')),
  author_is_team     INTEGER CHECK (author_is_team IN (0,1)),   -- 1 team channel, 0 Foundation/other, NULL unknown
  has_attachment     INTEGER NOT NULL DEFAULT 0 CHECK (has_attachment IN (0,1)),
  url_count          INTEGER NOT NULL DEFAULT 0,
  text               TEXT,                    -- <= DISCORD_TEXT_MAX_CHARS; NULLed by §2.7 (90 d, or pending rule below)
  text_purge_after   INTEGER,                 -- set iff written while the *_STORE_TEXT flag was false (pending text)
  text_hash          TEXT    NOT NULL,        -- sha256(app/nlp/textnorm.py(...)) (08, 02 §B11); empty: sha256('msg:' || msg_id)
  text_len           INTEGER,
  event_at           INTEGER NOT NULL,        -- discord: crosspost created_at; telegram: <time datetime>
  observed_at        INTEGER NOT NULL,
  deleted_at         INTEGER,                 -- annotation: MESSAGE_DELETE seen
  meta_json          TEXT CHECK (meta_json IS NULL OR json_valid(meta_json)),   -- {views, embed_count, candidates} (edits ignored)
  run_id             TEXT,                    -- gateway: 'gw:{session_start}'; telegram: job run_id
  -- read aliases (virtual, no storage) for spec 10 and the raw-row source convention
  source             TEXT GENERATED ALWAYS AS
                       (CASE platform WHEN 'discord' THEN 'discord_gateway' ELSE 'tme_preview' END) VIRTUAL,
  guild_id           TEXT GENERATED ALWAYS AS (source_guild_id) VIRTUAL,
  channel_id         TEXT GENERATED ALWAYS AS (source_channel_id) VIRTUAL,
  author_role_flag   TEXT GENERATED ALWAYS AS
                       (CASE author_is_team WHEN 1 THEN 'team' WHEN 0 THEN 'other' END) VIRTUAL,
  url                TEXT GENERATED ALWAYS AS (CASE platform
                       WHEN 'discord' THEN 'https://discord.com/channels/' || source_guild_id || '/'
                                           || source_channel_id || '/' || message_id
                       ELSE 'https://t.me/' || source_channel_id || '/' || message_id END) VIRTUAL,
  CHECK (text IS NULL OR via_own_server = 1 OR platform = 'telegram'),
  CHECK (text_purge_after IS NULL OR text_purge_after >= observed_at)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_ann_occ_event ON announcements(occupant_key, event_at);
CREATE INDEX IF NOT EXISTS ix_ann_purge     ON announcements(text_purge_after) WHERE text IS NOT NULL AND text_purge_after IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_ann_src_event ON announcements(platform, source_channel_id, event_at);
CREATE INDEX IF NOT EXISTS ix_ann_hash      ON announcements(text_hash, event_at);
CREATE INDEX IF NOT EXISTS ix_ann_obs       ON announcements(observed_at);
CREATE INDEX IF NOT EXISTS ix_ann_local     ON announcements(local_message_id);

-- class: state (engagement refreshed in place — accepted PIT deviation, 09 §11.4, flagged look-ahead by 13, 02 §J2;
-- text NULLed by retention after 90 d, then hash only, 02 §B10). One row per X post. Owner: 09. Discord and Telegram
-- posts live in announcements (02 §B11).
CREATE TABLE IF NOT EXISTS social_posts (
  platform                TEXT NOT NULL DEFAULT 'x' CHECK (platform IN ('x')),
  post_id                 TEXT NOT NULL,            -- native id (X snowflake)
  item_id                 TEXT GENERATED ALWAYS AS (platform || ':' || post_id) VIRTUAL,   -- 10: 'x:1969812345'
  author_id               TEXT NOT NULL,
  author_handle           TEXT,
  occupant_key            TEXT,                     -- source-bound occupant (team account); NULL for KOL/mentions
  is_team_account         INTEGER NOT NULL DEFAULT 0 CHECK (is_team_account IN (0,1)),
  is_kol                  INTEGER NOT NULL DEFAULT 0 CHECK (is_kol IN (0,1)),
  kol_handle              TEXT,                     -- kol_accounts.handle when is_kol=1
  author_kind             TEXT GENERATED ALWAYS AS
                          (CASE WHEN is_team_account = 1 THEN 'team' WHEN is_kol = 1 THEN 'kol' ELSE 'other' END) VIRTUAL,
  text                    TEXT,                     -- NULL after LLM_TEXT_RETENTION_DAYS (90 d, §2.7)
  text_hash               TEXT NOT NULL,            -- sha256(app/nlp/textnorm.py(text)) (02 §B11; kept forever)
  lang                    TEXT,
  url                     TEXT,
  like_count              INTEGER,
  repost_count            INTEGER,
  reply_count             INTEGER,
  quote_count             INTEGER,
  view_count              INTEGER,
  engagement_observed_at  INTEGER,
  has_url                 INTEGER CHECK (has_url IN (0,1)),
  has_media               INTEGER CHECK (has_media IN (0,1)),
  in_reply_to             TEXT,
  is_repost               INTEGER NOT NULL DEFAULT 0 CHECK (is_repost IN (0,1)),
  quoted_post_id          TEXT,
  author_credibility      REAL,
  credibility_flags       TEXT,                     -- JSON array
  bot_filtered            INTEGER NOT NULL DEFAULT 0 CHECK (bot_filtered IN (0,1)),   -- 10's spam_flag
  event_at                INTEGER NOT NULL,
  observed_at             INTEGER NOT NULL,
  source                  TEXT NOT NULL CHECK (source IN ('twitterapi_io','official_v2','subnetalpha_ctf')),
  run_id                  TEXT,
  raw_json_hash           TEXT,
  PRIMARY KEY (platform, post_id)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_social_posts_occ     ON social_posts(occupant_key, event_at);
CREATE INDEX IF NOT EXISTS ix_social_posts_author  ON social_posts(author_id, event_at);
CREATE INDEX IF NOT EXISTS ix_social_posts_hash    ON social_posts(text_hash, event_at);
CREATE INDEX IF NOT EXISTS ix_social_posts_obs     ON social_posts(observed_at);
CREATE INDEX IF NOT EXISTS ix_social_posts_item    ON social_posts(item_id);

-- class: append. Post ↔ occupant attribution (team/query/rule/entity). Written by 09 (X) and 08 (Discord/Telegram,
-- post_id = native id so platform || ':' || post_id = announcements.msg_id), plus 10's entity rows (02 §B11).
CREATE TABLE IF NOT EXISTS social_post_mentions (
  platform            TEXT NOT NULL,
  post_id             TEXT NOT NULL,
  occupant_key        TEXT NOT NULL,
  match_kind          TEXT NOT NULL CHECK (match_kind IN ('team','query','rule','entity')),
  classifier_version  TEXT,                         -- entity rows only
  event_at            INTEGER,                      -- the post's / announcement's event_at (copied; NULL if unknown)
  observed_at         INTEGER NOT NULL,
  source              TEXT GENERATED ALWAYS AS ('mention_' || match_kind) VIRTUAL,   -- raw-row convention alias
  run_id              TEXT,
  PRIMARY KEY (platform, post_id, occupant_key, match_kind)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_spm_occ ON social_post_mentions(occupant_key, observed_at);

-- class: append. Profile refreshes (followers etc.). Owner: 09.
CREATE TABLE IF NOT EXISTS x_account_snapshots (
  author_id      TEXT    NOT NULL,
  observed_at    INTEGER NOT NULL,
  handle         TEXT,
  followers      INTEGER,
  following      INTEGER,
  post_count     INTEGER,
  account_created_at INTEGER,
  verified       INTEGER CHECK (verified IN (0,1)),
  provider       TEXT,
  source         TEXT NOT NULL DEFAULT 'x_profile',
  event_at       INTEGER,
  run_id         TEXT,
  raw_json_hash  TEXT,
  PRIMARY KEY (author_id, observed_at)
) STRICT, WITHOUT ROWID;

-- class: append (summary NULLed by retention). RSS/Google News/Substack/Medium items. Owner: 09 Appendix A.
CREATE TABLE IF NOT EXISTS news_items (
  url_hash            TEXT PRIMARY KEY,             -- sha256(canonical_url) or sha256("t:"+title_norm+"|p:"+domain); 10's item_id
  canonical_url       TEXT,
  url                 TEXT NOT NULL,
  title               TEXT NOT NULL,
  summary             TEXT,                         -- HTML-stripped, ≤ 2,000 chars
  publisher           TEXT,
  publisher_domain    TEXT,
  feed_url            TEXT NOT NULL,
  feed_kind           TEXT,
  occupant_keys_json  TEXT NOT NULL DEFAULT '[]',   -- [{"occupant_key":…, "kind":"rule"|"query"}]
  lang                TEXT,
  text_hash           TEXT NOT NULL,                -- sha256(title_norm + "\n" + summary_norm)
  event_at            INTEGER NOT NULL,             -- published (or observed when the feed lacks a date)
  event_at_source     TEXT NOT NULL CHECK (event_at_source IN ('feed','observed')),
  observed_at         INTEGER NOT NULL,
  source              TEXT NOT NULL CHECK (source IN ('google_news','rss','substack','medium')),
  run_id              TEXT,
  raw_json_hash       TEXT
) STRICT;
CREATE INDEX IF NOT EXISTS ix_news_items_event ON news_items(event_at);
CREATE INDEX IF NOT EXISTS ix_news_items_obs   ON news_items(observed_at);

-- class: append (description NULLed by retention). YouTube channel/playlist RSS items. Owner: 09 Appendix A.
CREATE TABLE IF NOT EXISTS video_items (
  video_id            TEXT PRIMARY KEY,             -- 10's item_id
  channel_id          TEXT NOT NULL,
  channel_title       TEXT,
  occupant_key        TEXT,                         -- channel's occupant
  occupant_keys_json  TEXT NOT NULL DEFAULT '[]',   -- rule matches (playlist items)
  playlist_id         TEXT,
  title               TEXT NOT NULL,
  description         TEXT,
  url                 TEXT GENERATED ALWAYS AS ('https://www.youtube.com/watch?v=' || video_id) VIRTUAL,
  views               INTEGER,                      -- first observation only
  event_at            INTEGER NOT NULL,             -- published
  observed_at         INTEGER NOT NULL,
  source              TEXT NOT NULL CHECK (source IN ('channel_rss','playlist_rss')),
  run_id              TEXT,
  raw_json_hash       TEXT
) STRICT;
CREATE INDEX IF NOT EXISTS ix_video_items_occ ON video_items(occupant_key, event_at);

-- class: append (trigger-enforced). One label per (item, classifier_version). Owner: 10.
-- post_id (02 §B11) = the item id itself for posts: 'x:{id}' (= social_posts.item_id), 'discord:{source_message_id}'
--   and 'telegram:{handle_lower}/{n}' (= announcements.msg_id); '{item_kind}:{item_id}' for the other kinds:
--   'news_item:{news_items.url_hash}', 'video_item:{video_items.video_id}'.
CREATE TABLE IF NOT EXISTS post_labels (
  post_id             TEXT NOT NULL,
  item_kind           TEXT NOT NULL CHECK (item_kind IN ('social_post','announcement','news_item','video_item')),
  item_id             TEXT NOT NULL,
  classifier_version  TEXT NOT NULL,                -- '{model}:{rubric_sha8}:{schema_v}' | 'rules:v1'
  classifier_tier     TEXT NOT NULL CHECK (classifier_tier IN ('bulk','escalation','rules')),
  polarity            TEXT NOT NULL CHECK (polarity IN ('bullish','bearish','neutral')),
  intensity           INTEGER NOT NULL CHECK (intensity BETWEEN 1 AND 5),
  event_type          TEXT NOT NULL,                -- master §7.4 taxonomy (catalog §3.9)
  direction           INTEGER NOT NULL CHECK (direction IN (-1,0,1)),
  materiality         INTEGER NOT NULL CHECK (materiality BETWEEN 1 AND 3),
  entities_json       TEXT NOT NULL,
  is_spam             INTEGER NOT NULL CHECK (is_spam IN (0,1)),
  is_promo            INTEGER NOT NULL CHECK (is_promo IN (0,1)),
  summary             TEXT,                         -- model summary (not source text)
  confidence          REAL NOT NULL,
  event_at            INTEGER NOT NULL,             -- copied from the item
  observed_at         INTEGER NOT NULL,             -- label write time (PIT key)
  source              TEXT NOT NULL DEFAULT 'llm' CHECK (source IN ('llm','rules')),
  run_id              TEXT NOT NULL,
  batch_key           TEXT,
  model_resolved      TEXT,
  input_tokens        INTEGER,
  output_tokens       INTEGER,
  cost_usd            REAL,
  raw_json            TEXT,
  PRIMARY KEY (post_id, classifier_version)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_post_labels_event   ON post_labels(event_at);
CREATE INDEX IF NOT EXISTS ix_post_labels_ver_obs ON post_labels(classifier_version, observed_at);
CREATE INDEX IF NOT EXISTS ix_post_labels_item    ON post_labels(item_kind, item_id);

CREATE TABLE IF NOT EXISTS post_label_entities (    -- class: append; child rows of post_labels: event_at/run_id/source
                                                    -- are the parent label's (join on post_id, classifier_version)
  post_id             TEXT NOT NULL,
  classifier_version  TEXT NOT NULL,
  occupant_key        TEXT NOT NULL,
  netuid              INTEGER NOT NULL,
  confidence          REAL NOT NULL,
  via                 TEXT NOT NULL CHECK (via IN ('model','source','regex')),
  observed_at         INTEGER NOT NULL,
  PRIMARY KEY (post_id, classifier_version, occupant_key)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_ple_occ ON post_label_entities(occupant_key, classifier_version);

CREATE TABLE IF NOT EXISTS llm_batches (            -- class: state. Batch API ledger (survives restarts). Owner: 10.
  batch_key           TEXT PRIMARY KEY,
  provider_batch_id   TEXT,
  model               TEXT NOT NULL,
  classifier_version  TEXT NOT NULL,
  alias_sha256        TEXT,
  status              TEXT NOT NULL CHECK (status IN ('building','submitted','ended','applied','failed','expired','canceled')),
  n_requests INTEGER, n_items INTEGER, n_succeeded INTEGER, n_errored INTEGER, n_expired INTEGER, n_parse_errors INTEGER,
  est_input_tokens INTEGER, est_output_tokens INTEGER, est_cost_usd REAL,
  input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER, cost_usd REAL,
  created_at          INTEGER NOT NULL,
  submitted_at        INTEGER,
  ended_at            INTEGER,
  applied_at          INTEGER,
  last_polled_at      INTEGER,
  error               TEXT
) STRICT;

CREATE TABLE IF NOT EXISTS llm_batch_items (        -- class: state (retention 90 d after batch end)
  batch_key  TEXT NOT NULL,
  custom_id  TEXT NOT NULL,
  post_id    TEXT NOT NULL,
  attempt    INTEGER NOT NULL DEFAULT 1,
  status     TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','labelled','errored','requeued','fallback')),
  PRIMARY KEY (batch_key, post_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_lbi_custom ON llm_batch_items(batch_key, custom_id);

-- class: append. One row per occupant per owner_collect run: owner keys, cut, locks, position and the Rufus
-- conviction-table fields (master §7.3 P4; research 02-gapfill-owner-extraction-mechanics §2b/§2d). Owner: 11.
CREATE TABLE IF NOT EXISTS owner_snapshots (
  occupant_key              TEXT    NOT NULL,
  netuid                    INTEGER NOT NULL,
  observed_at               INTEGER NOT NULL,
  event_at                  INTEGER,         -- block time of block_number when known
  source                    TEXT    NOT NULL DEFAULT 'owner_collect',   -- composite of the C1..C8 fetches below
  run_id                    TEXT    NOT NULL,
  block_number              INTEGER,
  owner_coldkey             TEXT,
  owner_hotkey              TEXT,
  keys_source               TEXT CHECK (keys_source IN ('tmc_list','tmc_detail','chain_hourly','rufus')),
  owner_cut_per_block       REAL,            -- TMC subnet page / table (alpha per block)
  owner_cut_cumulative      REAL,
  pending_owner_cut         REAL,
  owner_cut_enabled         INTEGER CHECK (owner_cut_enabled IN (0,1)),
  auto_lock_enabled_tmc     INTEGER CHECK (auto_lock_enabled_tmc IN (0,1)),
  tmc_owner_personal_stake  REAL,
  tmc_owner_locked          REAL,            -- owner_personal_locked_alpha
  tmc_owner_perpetual       REAL,            -- owner_personal_perpetual_alpha
  tmc_owner_decaying        REAL,            -- owner_personal_decaying_alpha
  owner_total_alpha         REAL,            -- C2 /accounts/coldkey-stakes balance_alpha (owner position)
  owner_locked_alpha        REAL,            -- C2 locked_alpha
  owner_available_alpha     REAL,            -- total − locked
  owner_hotkeys_json        TEXT CHECK (owner_hotkeys_json IS NULL OR json_valid(owner_hotkeys_json)),
  owner_hk_alpha_stake      REAL,            -- C3 latest hour (hotkey stake incl. nominators: a trace, not the position)
  owner_hk_conviction       REAL,
  rufus_baked_at            INTEGER,         -- Rufus table.json baked_at_utc
  rufus_stale               INTEGER CHECK (rufus_stale IN (0,1)),
  rufus_auto_lock           INTEGER CHECK (rufus_auto_lock IN (0,1)),
  rufus_owner_perp          REAL,
  rufus_owner_decay         REAL,
  rufus_owner_pct           REAL,            -- owner bucket pct of eligible alpha
  rufus_to_owner_perp       REAL,
  rufus_to_owner_lockers    INTEGER,
  rufus_challenger_max_pct  REAL,
  rufus_days_to_king        REAL,
  total_subnet_stake        REAL,            -- copied from the chain_hourly row used
  tao_in                    REAL,
  price                     REAL,
  initial_alpha_est         REAL,            -- TaoStats only
  identity_name             TEXT,            -- owner coldkey identity name (04) at snapshot time
  fetch_status_json         TEXT    NOT NULL CHECK (json_valid(fetch_status_json)),
                                             -- {"c1":"ok|skip|fail:<err>", …, "c8":…}; per-input absence vs failure
  raw_json                  TEXT,            -- owner subset of the C1 snapshot + the C2 rows for this netuid
  PRIMARY KEY (occupant_key, observed_at)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_owner_snapshots_ck ON owner_snapshots(owner_coldkey, observed_at);

-- class: append. C3 hourly trace of the owner HOTKEY from /analytics/neuron/{n}/{hk}/?span=1W (INSERT OR IGNORE).
-- Owner: 11. ≈ 3 k rows/day; kept forever in v1 (rollup: §11).
CREATE TABLE IF NOT EXISTS owner_position_hourly (
  occupant_key                TEXT    NOT NULL,
  hotkey                      TEXT    NOT NULL,
  hour_ts                     INTEGER NOT NULL,
  block_number                INTEGER,
  alpha_stake                 REAL,
  conviction                  REAL,
  conviction_locked_alpha     REAL,
  conviction_perpetual_alpha  REAL,
  conviction_decaying_alpha   REAL,
  event_at                    INTEGER NOT NULL,   -- analytics point ts
  observed_at                 INTEGER NOT NULL,
  source                      TEXT    NOT NULL DEFAULT 'tmc_neuron_1w',
  run_id                      TEXT,
  PRIMARY KEY (occupant_key, hotkey, hour_ts)
) STRICT, WITHOUT ROWID;

-- class: append (annotations: superseded_by/superseded_at, whitelisted/whitelist_id display cache). Owner-coldkey
-- stake movements (S1 sells, S3 lock toggles), one row per (source event, occupant). PIT: a row is superseded at t
-- only if superseded_at <= t; metrics call is_whitelisted(., t), never the cache. Owner: 11.
CREATE TABLE IF NOT EXISTS owner_stake_events (
  event_uid          TEXT    NOT NULL,       -- source row id; else sha1(extrinsic_id|function|from_netuid|to_netuid|alpha)
  occupant_key       TEXT    NOT NULL,
  netuid             INTEGER NOT NULL,
  owner_coldkey      TEXT    NOT NULL,
  signer             TEXT,
  hotkey             TEXT,
  dest_coldkey       TEXT,
  function           TEXT,                   -- extrinsic call name as delivered (remove_stake_full_limit, …)
  kind               TEXT    NOT NULL CHECK (kind IN ('sell','sell_full','swap_out','transfer_out','move_internal','buy',
                               'swap_in','transfer_in','lock','lock_move','perpetual_on','perpetual_off','other')),
  counts_as_outflow  INTEGER NOT NULL CHECK (counts_as_outflow IN (0,1)),
  from_netuid        INTEGER,
  to_netuid          INTEGER,
  alpha              REAL,                   -- alpha amount (TAO units, never rao)
  tao                REAL,                   -- TAO received/paid
  price              REAL,
  slippage           REAL,
  is_proxy           INTEGER CHECK (is_proxy IN (0,1)),
  success            INTEGER NOT NULL DEFAULT 1 CHECK (success IN (0,1)),
  block_number       INTEGER,
  extrinsic_id       TEXT,                   -- 'block-idx'
  event_at           INTEGER NOT NULL,       -- block time
  observed_at        INTEGER NOT NULL,
  source             TEXT    NOT NULL CHECK (source IN ('tmc_staking_activity','tmc_stake_removed','tmc_stream',
                               'taostats_delegation','taostats_extrinsic','rpc_events')),
  run_id             TEXT    NOT NULL,
  superseded_by      TEXT,                   -- event_uid of the cross-source duplicate that wins
  superseded_at      INTEGER,
  whitelisted        INTEGER NOT NULL DEFAULT 0 CHECK (whitelisted IN (0,1)),   -- display cache only
  whitelist_id       INTEGER,
  raw_json           TEXT,
  PRIMARY KEY (event_uid, occupant_key)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_ose_occ_time ON owner_stake_events(occupant_key, event_at);
CREATE INDEX IF NOT EXISTS ix_ose_ck_block ON owner_stake_events(owner_coldkey, block_number);
CREATE INDEX IF NOT EXISTS ix_ose_extrinsic ON owner_stake_events(extrinsic_id) WHERE extrinsic_id IS NOT NULL;

-- class: append (annotations: whitelisted/whitelist_id, review_id, event_id, paired_change_id, identity_event_id).
-- Control/identity/lock changes of the owner (S3, owner_control_change and owner_extraction inputs). Owner: 11.
CREATE TABLE IF NOT EXISTS owner_changes (
  id                    INTEGER PRIMARY KEY,
  occupant_key          TEXT    NOT NULL,
  netuid                INTEGER NOT NULL,
  change_type           TEXT    NOT NULL CHECK (change_type IN ('coldkey_swap','owner_transfer','subnet_owner_changed',
                          'coldkey_change_unclassified','coldkey_swap_announced','hotkey_repoint',
                          'hotkey_repoint_identity_churn','perpetual_off','decaying_lock_added','lock_reduced',
                          'auto_lock_disabled','auto_lock_enabled','owner_cut_disabled','owner_cut_enabled',
                          'extraction_detected','key_compromise_override')),
  old_value             TEXT,
  new_value             TEXT,
  detected_via          TEXT    NOT NULL,    -- tmc_snapshot | chain_hourly | rufus | staking_activity | taostats |
                                             -- subnetradar | rpc_owner_history | features | operator
  source                TEXT    GENERATED ALWAYS AS (detected_via) VIRTUAL,   -- raw-row convention alias
  classification_basis  TEXT,                -- taostats_is_coldkey_swap | rufus_challenger | subnetradar |
                                             -- old_ck_retains_stake | old_ck_emptied | unclassified
  s3_base_penalty       REAL    NOT NULL DEFAULT 0,   -- 0.6 | 0.4 | 1.0 (hard, perpetual_off) | 0
  paired_change_id      INTEGER,
  identity_event_id     INTEGER,             -- events.id of the paired identity_changed row
  block_number          INTEGER,
  extrinsic_id          TEXT,                -- 'block-idx' when a signed extrinsic is known (D3, D9)
  event_at              INTEGER NOT NULL,
  observed_at           INTEGER NOT NULL,
  review_id             INTEGER,             -- review_queue.id
  event_id              INTEGER,             -- events.id emitted for this change
  evidence_json         TEXT    NOT NULL DEFAULT '{}' CHECK (json_valid(evidence_json)),
  whitelisted           INTEGER NOT NULL DEFAULT 0 CHECK (whitelisted IN (0,1)),   -- display cache only
  whitelist_id          INTEGER,
  dedupe_key            TEXT    NOT NULL UNIQUE,   -- f"{occupant_key}|{change_type}|{new_value or ''}|{event_at // 86400}"
  run_id                TEXT    NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_owner_changes_occ ON owner_changes(occupant_key, event_at);
```

### 3.6 Derived tables and enforcement triggers (writers: 05–12, 14, 01/16)

```sql
-- The ONLY input to scoring (master §6) and the feature store of spec 12 (02 §B3–§B5; §2.5/§2.6 contract). One
-- table, no metric_norm table, no is_provisional column (a row is provisional iff as_of % 86400 <> 0).
-- stage='raw': written by producers (05–12) through put_metric_values, keyed by the scoring slot the observation
--   serves (00:00Z official, 06/12/18:00Z provisional; hourly producers the hour bucket); first success wins, only a
--   failed/none row may be upgraded by a later successful observation. value = the producer's value (a raw
--   measurement, or a final [0,1] score for 11's pre-normalised P4 metrics, 02 §C1); value_json = inputs/evidence.
-- stage='norm': written only by spec 12 at scoring time, insert-once per (slot, scoring spec); spec_version = the
--   scoring spec, value = m in [0,1] or NULL, observed_at = the run's computed_at; the PIT record 13 and 14 read.
-- Key (02 §B3) = (occupant_key, metric_key, as_of, spec_version, stage); the PK order also serves §2.6's lookup.
CREATE TABLE IF NOT EXISTS metric_values (
  occupant_key  TEXT    NOT NULL,
  metric_key    TEXT    NOT NULL,          -- catalog §3.8 (validated by put_metric_values)
  as_of         INTEGER NOT NULL,          -- raw: producer slot key; norm: scoring slot. Official = 00:00Z (as_of % 86400 = 0)
  spec_version  TEXT    NOT NULL,          -- raw: metric-definition version; norm: scoring spec version ('ts-1.0')
  stage         TEXT    NOT NULL DEFAULT 'raw' CHECK (stage IN ('raw','norm')),
  value         REAL,                      -- raw: producer value; norm: m in [0,1]; NULL = None
  value_json    TEXT CHECK (value_json IS NULL OR json_valid(value_json)),
                                           -- raw: {"status","reason",…inputs, evidence_url?}; norm: {"raw","status",
                                           --  "reason","raw_as_of","raw_observed_at","age_s","src","fallback","cohort_n"}
  status        TEXT GENERATED ALWAYS AS (COALESCE(json_extract(value_json, '$.status'), 'none')) VIRTUAL
                CHECK (status IN ('ok','absent','failed','not_applicable','none')),
                                           -- never written by producers; 'none' (key missing; on norm rows also "no
                                           -- raw row") is read as 'failed' (02 §B3)
  observed_at   INTEGER NOT NULL,          -- when this value became knowable (PIT key)
  source        TEXT    NOT NULL CHECK (source IN ('registry','chain','github','web','community','x','rss','llm',
                                                   'integrity','scoring','quant','api','runtime','storage','cost','ops')),
                                           -- exactly the 02 §B9 producer vocabulary (no 'nlp', no backfill_* source)
  run_id        TEXT    NOT NULL,
  backfilled    INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),   -- computed after the fact (12 catch-up, 17)
  CHECK (stage = 'norm' OR status <> 'ok' OR value IS NOT NULL),          -- a NULL is never an 'ok' measurement
  CHECK (stage = 'norm' OR status NOT IN ('failed','not_applicable','none') OR value IS NULL),
  CHECK (stage = 'raw' OR value IS NULL OR value BETWEEN 0 AND 1),
  PRIMARY KEY (occupant_key, metric_key, as_of, spec_version, stage)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_metric_values_xsec ON metric_values(metric_key, stage, spec_version, as_of);
CREATE INDEX IF NOT EXISTS ix_metric_values_slot ON metric_values(stage, spec_version, as_of);
CREATE INDEX IF NOT EXISTS ix_metric_values_obs  ON metric_values(observed_at);

-- Never UPDATEd or DELETEd (triggers). One row per occupant per slot per spec_version (02 §B6 key; run_kind is a
-- function of the slot hour, so spec 12's UNIQUE(..., run_kind) is implied). No observed_at column: computed_at
-- (run instant) with input_cutoff is the PIT key for backtests. Owner: 12 (§2.7-§2.10).
CREATE TABLE IF NOT EXISTS scores (
  id                  INTEGER PRIMARY KEY,
  occupant_key        TEXT    NOT NULL,
  netuid              INTEGER NOT NULL,
  as_of               INTEGER NOT NULL,      -- slot instant: 00:00Z official, 06/12/18:00Z provisional (run at hh:15Z)
  spec_version        TEXT    NOT NULL,
  run_kind            TEXT    NOT NULL CHECK (run_kind IN ('official','provisional')),
  run_id              TEXT    NOT NULL,      -- score_runs.run_id
  status              TEXT    NOT NULL CHECK (status IN ('active','pre_launch')),
  quality             REAL CHECK (quality IS NULL OR quality BETWEEN 0 AND 100),          -- Health (P1-P5)
  velocity            REAL CHECK (velocity IS NULL OR velocity BETWEEN 0 AND 100),
  composite_raw       REAL CHECK (composite_raw IS NULL OR composite_raw BETWEEN 0 AND 100),
  flag_multiplier     REAL    NOT NULL DEFAULT 1.0 CHECK (flag_multiplier BETWEEN 0.25 AND 1.0),   -- floor 0.25
  composite           REAL CHECK (composite IS NULL OR composite BETWEEN 0 AND 100),      -- composite_raw x flag_multiplier
  composite_smoothed  REAL CHECK (composite_smoothed IS NULL OR composite_smoothed BETWEEN 0 AND 100),  -- EMA span 7
  pillar_scores       TEXT    NOT NULL CHECK (json_valid(pillar_scores)),    -- {"P1":0.857835,...,"P6":null}, [0,1]|null
  pillar_coverage     TEXT    NOT NULL CHECK (json_valid(pillar_coverage)),  -- {"P1":0.95,...,"P6":1.0}
  confidence          REAL    NOT NULL CHECK (confidence BETWEEN 0 AND 1),
  insufficient_data   INTEGER NOT NULL DEFAULT 0 CHECK (insufficient_data IN (0,1)),  -- confidence < 0.4
  tier                TEXT CHECK (tier IN ('A','B','C','D','F')),           -- NULL for pre_launch
  prev_tier           TEXT CHECK (prev_tier IN ('A','B','C','D','F')),
  red_flags           TEXT    NOT NULL DEFAULT '[]' CHECK (json_valid(red_flags)),
                                             -- sorted JSON array of active flag NAMES, e.g. ["dereg_imminent","repo_gone"];
                                             -- multiplier/since/state/origin/detail live only in explain.flags (02 §B6)
  regime              TEXT,                  -- TRENDING|DISPERSED|CHOPPY|DEAD|UNKNOWN (12 regime)
  v_w                 REAL,                  -- velocity weight used
  explain             TEXT    NOT NULL DEFAULT '{}' CHECK (json_valid(explain)),   -- master §7.7, spec 12 §2.10
  computed_at         INTEGER NOT NULL,      -- run start T0 (13's computed_at); PIT key for score reads
  input_cutoff        INTEGER NOT NULL,      -- latest observed_at admitted (strict: T0; reconstructed: slot + 15 min)
  backfilled          INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),   -- reconstructed-mode (catch-up) row
  shadow              INTEGER NOT NULL DEFAULT 0 CHECK (shadow IN (0,1)),       -- SCORING_SHADOW_SPECS run
  UNIQUE (occupant_key, as_of, spec_version)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_scores_latest ON scores(spec_version, occupant_key, as_of DESC);
CREATE INDEX IF NOT EXISTS ix_scores_asof   ON scores(spec_version, as_of);
CREATE INDEX IF NOT EXISTS ix_scores_occ    ON scores(occupant_key, spec_version, run_kind, as_of);   -- EMA/tier prev

-- Occupant-scoped and system events (master §6). Catalog §3.9. Alert dispatcher (14) sets alerted_at.
CREATE TABLE IF NOT EXISTS events (
  id            INTEGER PRIMARY KEY,
  occupant_key  TEXT,                        -- NULL = system event
  netuid        INTEGER,
  event_type    TEXT    NOT NULL,
  severity      TEXT    NOT NULL CHECK (severity IN ('info','warning','critical')),
  event_at      INTEGER,                     -- when it happened (NULL unknown → readers use observed_at)
  observed_at   INTEGER NOT NULL,            -- when we detected it
  source        TEXT    NOT NULL CHECK (source IN ('registry','chain','github','web','community','x','rss','llm',
                                                   'integrity','scoring','quant','api','runtime','storage','cost','ops')),
                                             -- emitter, exactly the 02 §B9 vocabulary; backfilled history keeps the
                                             -- producer's source and sets payload.backfilled = true (02 §J1)
  payload       TEXT    NOT NULL DEFAULT '{}' CHECK (json_valid(payload)),
  dedupe_key    TEXT,                        -- per catalog; NULL = never deduplicated
  run_id        TEXT,
  alerted_at    INTEGER,                     -- NULL = not (yet) alerted / not alertable
  CHECK (occupant_key IS NOT NULL OR netuid IS NULL)
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS ux_events_dedupe   ON events(event_type, dedupe_key) WHERE dedupe_key IS NOT NULL;
CREATE INDEX        IF NOT EXISTS ix_events_observed ON events(observed_at DESC, id DESC);
CREATE INDEX        IF NOT EXISTS ix_events_occ      ON events(occupant_key, event_type, event_at);
CREATE INDEX        IF NOT EXISTS ix_events_type     ON events(event_type, observed_at);
CREATE INDEX        IF NOT EXISTS ix_events_unalerted ON events(id) WHERE alerted_at IS NULL;   -- 14 alert_dispatch queue (02 §D3)

-- Network regime per official slot (master §7.6, 02 §B8); written by 12 (live, INSERT OR IGNORE) and 17 (backfill);
-- read by 13 for tagging (day_ts). Live and backfilled rows use the same raw rule: no debounce; a thin universe
-- yields UNKNOWN with v_w 0.15. No observed_at column (computed_at, 02 §B6).
CREATE TABLE IF NOT EXISTS regime_daily (
  day_ts                INTEGER NOT NULL,    -- official slot as_of (00:00Z)
  spec_version          TEXT    NOT NULL,
  label                 TEXT    NOT NULL CHECK (label IN ('TRENDING','DISPERSED','CHOPPY','DEAD','UNKNOWN')),
  raw_regime            TEXT,
  vol_24h               REAL,
  directional_strength  REAL,
  dispersion            REAL,
  n_subnets             INTEGER,
  thin_universe         INTEGER NOT NULL DEFAULT 0 CHECK (thin_universe IN (0,1)),
  v_w                   REAL    NOT NULL,    -- 0.25 TRENDING/DISPERSED, 0.10 DEAD, else 0.15 (frozen per spec_version)
  tao_usd_ret_24h       REAL,                -- diagnostic only in ts-1.0
  sum_prices_ret_24h    REAL,
  inputs_json           TEXT CHECK (inputs_json IS NULL OR json_valid(inputs_json)),   -- 12: {raw_regime, vol_24h, …}
  source                TEXT    NOT NULL DEFAULT 'live' CHECK (source IN ('live','backfill')),
                                             -- row provenance (12 scoring run | 17 script), not the §B9 vocabulary
  computed_at           INTEGER NOT NULL,
  backfilled            INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),   -- 17 rows and 12 catch-up rows
  run_id                TEXT,
  PRIMARY KEY (day_ts, spec_version)
) STRICT, WITHOUT ROWID;

-- Rank churn monitor (master §7.7), after each official primary run. Owner: 12 (§2.12).
CREATE TABLE IF NOT EXISTS rank_stability_daily (
  as_of              INTEGER NOT NULL,       -- official slot
  spec_version       TEXT    NOT NULL,
  series             TEXT    NOT NULL CHECK (series IN ('health','composite_smoothed')),
  lag_days           INTEGER NOT NULL CHECK (lag_days IN (1,7)),
  n                  INTEGER NOT NULL,
  rank_autocorr      REAL,                   -- NULL when n < 10
  quintile_turnover  REAL,
  top20_turnover     REAL,
  computed_at        INTEGER NOT NULL,
  PRIMARY KEY (as_of, spec_version, series, lag_days)
) STRICT, WITHOUT ROWID;

-- One row per (scoring run, spec): catch-up planning, audit, performance timings. Owner: 12 (§2.2, §2.13).
CREATE TABLE IF NOT EXISTS score_runs (
  run_id          TEXT    NOT NULL,
  spec_version    TEXT    NOT NULL,
  as_of           INTEGER NOT NULL,          -- slot
  run_kind        TEXT    NOT NULL CHECK (run_kind IN ('official','provisional')),
  mode            TEXT    NOT NULL CHECK (mode IN ('strict','reconstructed')),
  trigger         TEXT    NOT NULL CHECK (trigger IN ('schedule','catchup','rescore','backfill','cli')),
  shadow          INTEGER NOT NULL DEFAULT 0 CHECK (shadow IN (0,1)),
  killed          INTEGER NOT NULL DEFAULT 0 CHECK (killed IN (0,1)),   -- SCORING_KILLED: computed, not written
  backfilled      INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),
  started_at      INTEGER NOT NULL,
  finished_at     INTEGER,
  ok              INTEGER CHECK (ok IN (0,1)),
  spec_hash       TEXT    NOT NULL,
  regime          TEXT,
  n_universe      INTEGER,
  n_scored        INTEGER,
  n_prelaunch     INTEGER,
  n_insufficient  INTEGER,
  n_events        INTEGER,
  n_errors        INTEGER,                   -- producer fn errors in the run (12 §3.1)
  timings_json    TEXT CHECK (timings_json IS NULL OR json_valid(timings_json)),
  error           TEXT,
  PRIMARY KEY (run_id, spec_version)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_score_runs_slot ON score_runs(spec_version, as_of, run_kind);

-- ── Enforcement triggers (§2.5) ──
CREATE TRIGGER IF NOT EXISTS trg_scores_no_update BEFORE UPDATE ON scores
BEGIN SELECT RAISE(ABORT, 'scores is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_scores_no_delete BEFORE DELETE ON scores
BEGIN SELECT RAISE(ABORT, 'scores is append-only'); END;

-- 02 §B4: only a raw failed/none row may be upgraded, by a later successful observation; norm rows are insert-once.
-- (Status read via json_extract, not the generated column, inside the trigger.)
CREATE TRIGGER IF NOT EXISTS trg_metric_values_update BEFORE UPDATE ON metric_values
WHEN OLD.stage = 'norm'
  OR COALESCE(json_extract(OLD.value_json, '$.status'), 'none') NOT IN ('failed','none')
  OR COALESCE(json_extract(NEW.value_json, '$.status'), 'none') IN ('failed','none')
  OR NEW.observed_at < OLD.observed_at
  OR NEW.occupant_key IS NOT OLD.occupant_key OR NEW.metric_key IS NOT OLD.metric_key
  OR NEW.as_of IS NOT OLD.as_of OR NEW.spec_version IS NOT OLD.spec_version OR NEW.stage IS NOT OLD.stage
BEGIN SELECT RAISE(ABORT, 'metric_values: only a failed raw row may be upgraded by a later successful observation'); END;
CREATE TRIGGER IF NOT EXISTS trg_metric_values_delete BEFORE DELETE ON metric_values
WHEN OLD.as_of % 86400 = 0
BEGIN SELECT RAISE(ABORT, 'metric_values: official rows are permanent'); END;

CREATE TRIGGER IF NOT EXISTS trg_events_update BEFORE UPDATE ON events
WHEN OLD.alerted_at IS NOT NULL
  OR NEW.occupant_key IS NOT OLD.occupant_key OR NEW.netuid IS NOT OLD.netuid
  OR NEW.event_type IS NOT OLD.event_type OR NEW.event_at IS NOT OLD.event_at
  OR NEW.observed_at IS NOT OLD.observed_at OR NEW.source IS NOT OLD.source
  OR NEW.dedupe_key IS NOT OLD.dedupe_key OR NEW.run_id IS NOT OLD.run_id
BEGIN SELECT RAISE(ABORT, 'events: only alerted_at/payload/severity may change, and only before alerting'); END;
CREATE TRIGGER IF NOT EXISTS trg_events_no_delete BEFORE DELETE ON events
BEGIN SELECT RAISE(ABORT, 'events is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_post_labels_no_update BEFORE UPDATE ON post_labels
BEGIN SELECT RAISE(ABORT, 'post_labels is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_post_labels_no_delete BEFORE DELETE ON post_labels
BEGIN SELECT RAISE(ABORT, 'post_labels is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_idobs_no_update BEFORE UPDATE ON identity_observations
BEGIN SELECT RAISE(ABORT, 'identity_observations is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_idobs_no_delete BEFORE DELETE ON identity_observations
BEGIN SELECT RAISE(ABORT, 'identity_observations is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_manual_flags_no_update BEFORE UPDATE ON manual_flags
BEGIN SELECT RAISE(ABORT, 'manual_flags is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_manual_flags_no_delete BEFORE DELETE ON manual_flags
BEGIN SELECT RAISE(ABORT, 'manual_flags is append-only'); END;
```

Severity escalation (`warning` → `critical`, spec 10 materiality-3 confirmation) is allowed only while `alerted_at IS NULL`; once alerted, a stronger signal is a **new** event row with its own `dedupe_key` suffix (`…|mat3`).

"Official" in `trg_metric_values_delete` means `as_of % 86400 = 0`, i.e. `SCORING_OFFICIAL_HOUR = 0`, which is pinned: the settings validator rejects any other value (02 §B6, §F2). Moving the official hour would be a new `spec_version` plus a migration that redefines this trigger (DROP/CREATE TRIGGER is additive-policy compliant).

### 3.7 Views (consumer aliases; no business logic beyond unit conversion)

```sql
-- Latest observation per (occupant, source, field) = 06's "is_current" and 04's "t = now" read.
CREATE VIEW IF NOT EXISTS v_identity_current AS
SELECT io.* FROM identity_observations io
WHERE io.id = (SELECT i2.id FROM identity_observations i2
               WHERE i2.occupant_key = io.occupant_key AND i2.source = io.source AND i2.field = io.field
               ORDER BY i2.observed_at DESC, i2.id DESC LIMIT 1);

-- Latest score per occupant per spec_version (API/feed; filter by spec_version in the caller).
CREATE VIEW IF NOT EXISTS v_latest_scores AS
SELECT s.* FROM scores s
WHERE s.as_of = (SELECT MAX(s2.as_of) FROM scores s2
                 WHERE s2.occupant_key = s.occupant_key AND s2.spec_version = s.spec_version);

-- Spec 13 panel-builder names, preserved exactly (13 §3.1).
CREATE VIEW IF NOT EXISTS v_quant_price_bars AS
SELECT netuid, occupant_key, period, ts AS bucket_start, ts_last, open, high, low, close, volume,
       tao_in_pool, alpha_in_pool, moving_price, block_number, source, observed_at, backfilled
FROM price_bars;

-- Units: tao_liquidity is delivered in rao (research probe 2026-09-22: SN51 154321166411713 vs analytics
-- subnet_tao 153227189355104, both ≈ 153-154 k τ), so the view converts to τ. 05's live units probe re-checks;
-- a mismatch is fixed by a migration that redefines the view (DROP VIEW + CREATE VIEW is additive-policy compliant).
-- volume = analytics volume_24h (NULL on SSE rows; the table's own `volume` field has an UNVERIFIED window).
CREATE VIEW IF NOT EXISTS v_quant_chain_hourly AS
SELECT netuid, occupant_key, hour_ts AS hour, block_number, tao_liquidity / 1e9 AS tao_in_pool,
       subnet_moving_price AS moving_price, volume_24h AS volume, observed_at, backfilled, source
FROM chain_hourly;

-- 13 §3.1: emission_share = TMC emission_percent; tao_in_pool = subnet_tao / 1e9 (subnet_tao is rao — verified
-- against the research probe's 1Y analytics dump for SN51; same redefinition rule as above).
-- Redefined by migration 0005 (2026-10-22, 20 N2 item 3): chain_daily rows plus one rpc_subnet_daily day row per
-- occupant-day that chain_daily lacks (dead occupants); before 2025-11-17 (TMC emission_percent truncated to whole
-- percent) the RPC share replaces TMC's where one exists. RPC-only rows: tao_in_pool NULL, backfilled 1.
CREATE VIEW v_quant_chain_daily AS
SELECT c.occupant_key, c.netuid, c.day_ts AS day, c.event_at AS ts, c.block_number,
       CASE WHEN c.day_ts < 1763337600 AND r.emission_share IS NOT NULL THEN r.emission_share
            ELSE c.emission_percent END AS emission_share,
       c.subnet_tao / 1e9 AS tao_in_pool, c.observed_at, c.backfilled,
       CASE WHEN c.day_ts < 1763337600 AND r.emission_share IS NOT NULL THEN r.source ELSE c.source END AS source
FROM chain_daily c
LEFT JOIN rpc_subnet_daily r ON r.netuid = c.netuid AND r.as_of = c.day_ts AND r.occupant_key = c.occupant_key
UNION ALL
SELECT r.occupant_key, r.netuid, r.as_of AS day, r.event_at AS ts, r.block_number, r.emission_share,
       NULL AS tao_in_pool, r.observed_at, 1 AS backfilled, r.source
FROM rpc_subnet_daily r
WHERE r.emission_share IS NOT NULL AND r.as_of % 86400 = 0
  AND NOT EXISTS (SELECT 1 FROM chain_daily c
                  WHERE c.netuid = r.netuid AND c.day_ts = r.as_of AND c.occupant_key = r.occupant_key);

-- registered_ts: exact (17 block_times) when known, else 04's estimate; registered_ts_exact says which.
CREATE VIEW IF NOT EXISTS v_quant_occupants AS
SELECT occupant_key, netuid, network_registered_at,
       COALESCE(registered_ts, registered_at_est) AS registered_ts, (registered_ts IS NOT NULL) AS registered_ts_exact,
       first_seen_at, last_seen_at, status, deregistered_at, deregistered_ts
FROM occupants;
```

### 3.8 metric_key catalog (keys stored in `metric_values`; spec 12's registry is authoritative for math)

This spec produces no metric values. The table fixes the allowed `metric_key` strings, their producer spec and `metric_values.source` (02 §B9: 05 → `chain`, 06 → `github`, 07 → `web`, 08 → `community`, 09 → `x` or `rss`, 10 → `llm`, 11 → `integrity`, 12 → `scoring`), and the master §7.3 normaliser/weight so writers and the scorer agree on names. `METRIC_KEYS` = exactly the keys of 02 §C1–§C7 ∪ spec 12's registry (62 keys: 45 scored + 9 legs/inputs + 8 aux) ∪ the evidence keys producers write (17) ∪ spec 19's workbench evidence keys (30, added 2026-10-16): **109 keys**. Retired names are rejected (02 §C2: `owner_lock_disabled_90d`, `owner_control_change_30d`, `owner_max_sell_impact_7d`, `identity_stability`, `owner_conviction`, `owner_retention`; §C3: `discord_last_announcement_days`, `tg_last_post_days`). Keys marked *evidence* (w = 0, not in spec 12's registry) are stored for explain, the API evidence table and `ts-1.1` research only.

| pillar (W) | metric_key | producer spec / `source` | master normaliser | w |
|---|---|---|---|---|
| P1 Chain (30) | `emission_share` | 05 / chain | `rankg` | .20 |
| | `gate_margin` | 05 / chain | `zclip(log2 x, −2, +1)` | .15 |
| | `dereg_safety` | 05 / chain | custom (immune → 1; bottom-3 → 0) | .15 |
| | `liquidity_depth` | 05 / chain | `rankg(log)` | .10 |
| | `exit_capacity` | 05 / chain | `zclip(x, 0.05, 0.6)` | .05 |
| | `net_flow_30d` | 05 / chain | `zclip(x, −0.5, +0.5)` | .10 |
| | `chain_buys` | 05 / chain | `rankg` | .05 |
| | `holder_dispersion` | 05 / chain | `zclip(x, 0.3, 0.9)` | .05 |
| | `participation` | 05 / chain — (active validators + active miners) ÷ max_uids only (02 §C7) | `zclip(x, 0.2, 0.9)` | .05 |
| | `miner_burn_ok` | 05 / chain | `zclip(x, 0, 0.8)` | .10 |
| P1 aux (w 0, in 12's registry) | `emission_enabled_flag`, `dereg_risk_flag`, `immune_flag`, `implied_miner_burn` (flag inputs), `dereg_hazard`, `occupant_age_days` (evidence; `tenure` fallback) | 05 / chain | — | 0 |
| P1 evidence (w 0) | `vtrust_dispersion` (02 §C7), `owner_locked_share`, `sum_subnet_prices` (on `0:<reg>`), `tao_usd` (on `0:<reg>`) | 05 / chain | — | 0 |
| P2 Builder (20) | `active_authors_28d` | 06 / github | `logcap(x, 10)` | .20 |
| | `fte_devs_84d` | 06 / github | `logcap(x, 8)` | .15 |
| | `commit_days_90d` | 06 / github | `logcap(x, 90)` | .15 |
| | `declared_repo_freshness` | 06 / github | `recency(d, 14, 180)` | .10 |
| | `org_freshness` | 06 / github | `recency(d, 7, 90)` | .10 |
| | `release_cadence_90d` | 06 / github | `logcap(x, 6)` | .05 |
| | `review_practice_90d` | 06 / github | identity | .05 |
| | `bus_factor` | 06 / github | custom (≥3→1, 2→.6, 1→.3) | .10 |
| | `repo_hygiene` | 06 / github | `checklist` | .05 |
| | `originality` | 06 / github | custom (1 − template share) | .05 |
| P3 Product/Web (10) | `site_uptime_30d` | 07 / web | identity | .30 |
| | `tls_ok` | 07 / web | binary | .10 |
| | `site_age` | 07 / web | inverted recency (≥365→1, ≤30→.2) | .10 |
| | `site_freshness` | 07 / web | `recency(d, 90, 365)` | .15 |
| | `docs_present` | 07 / web | binary | .15 |
| | `product_status` | 07 / web (from `manual_notes`; the enum itself goes in `value_json.product_status`, 02 §B3) | map none 0 / waitlist .3 / beta .6 / live 1 | .20 |
| P4 Team/Gov (18) | `team_doxxed` | 11 / integrity — final [0,1] score in `value` (02 §C1) | map (12: `prenorm`) | .25 |
| | `owner_identity` | 11 / integrity — final [0,1] | binary (12: `prenorm`) | .05 |
| | `owner_commitment` | 11 / integrity — final [0,1] | custom S2 (12: `prenorm`) | .20 |
| | `owner_retention_30d` | 11 / integrity — final [0,1]; hard triggers first, then `owner_cut_enabled=false → 1.0` (02 §C1) | custom S1 (12: `prenorm`) | .20 |
| | `control_stability_90d` | 11 / integrity — final [0,1] | custom S3 (12: `prenorm`) | .15 |
| | `owner_overhang` | 11 / integrity — final [0,1] | inverted `zclip` S5 (12: `prenorm`) | .05 |
| | `tenure` | 11 / integrity — raw days (`tenure_days(head_block, network_registered_at)`; `failed`, reason `no_registration_block`, when unknown); 12 falls back to 05's `occupant_age_days` | `zclip(days, 120, 365)`, floor .3 | .10 |
| P4 aux (w 0, in 12's registry; 02 §C2) | `owner_extraction_trigger` (→ flag `owner_extraction`), `owner_control_change_trigger` (→ flag `owner_control_change`; `value_json.identity_churn = true` makes it sticky) | 11 / integrity | — | 0 |
| P4 evidence (w 0) | `owner_lock_ratio`, `owner_dump_impact`, `owner_holding_share`, `owner_sell_impact_7d`, `team_verified_count`, `team_candidate_count` (02 §C2), `owner_sell_ratio_30d` (listed by 11 §3.3 and 12 §2.4; S4 self-dealing is an event only in ts-1.0: `owner_concentration`) | 11 / integrity | — | 0 |
| P5 Community (12) | `venue_reachable` | 08 / community | binary | .10 |
| | `discord_members` | 08 / community | `logcap(x, 5000)` | .15 |
| | `presence_ratio` | 08 / community | custom | .10 |
| | `discord_growth_30d` | 08 / community | `zclip(x, −0.1, +0.3)` | .10 |
| | `announcement_cadence_30d` | 12 / scoring — derived from the legs below; 08/09 register no producer for it (02 §C3) | `logcap(x, 12)` | .25 |
| | `x_engagement_30d` | 09 / x (look-ahead: engagement refreshed in place, 02 §J2) | `rankg` | .10 |
| | `x_followers` | 09 / x (no team handle or gone account → 0, `absent`) | `logcap(x, 50000)` | .05 |
| | `silence_days` | 12 / scoring — derived from the legs below (02 §C3); `0` (`absent`) only when no venue is reachable **and** no X account exists | `recency(d, 7, 45)` | .15 |
| P5 legs (w 0, in 12's registry; 02 §C3) | `x_cadence_30d`, `x_last_activity_days` (09 / x); `discord_announcements_30d`, `tg_posts_30d`, `discord_last_activity_days`, `tg_last_activity_days` (08 / community) | 08, 09 | — | 0 |
| P5 evidence (w 0) | `discord_topology`, `inflated_days`, `tg_members` (08 / community) | 08 | — | 0 |
| P6 Narrative (Velocity only; 02 §C4) | `event_impact_14d` | 10 / llm | `tanh(x/2)` → [−1,1] → [0,1] | .40 |
| | `polarity_7d` | 10 / llm | identity (0.5 neutral; < 5 posts → None) | .25 |
| | `attention_7d` | 10 / llm | `zclip(z, −2, +3)` | .20 |
| | `kol_coverage_30d` | 10 / llm (fallback leg `x_kol_posts_30d`) | `logcap(x, 5)` | .15 |
| P6 inputs (w 0, in 12's registry) | `x_mentions_7d`, `x_kol_posts_30d` (09 / x); `news_mentions_7d` (09 / rss, `compute_news_mentions_7d`) | 09 | — | 0 |
| P6 evidence (w 0) | `video_uploads_90d`, `video_mentions_30d`, `blog_posts_90d` (09 / rss; `compute_video_uploads_90d`, `compute_video_mentions_30d`, `compute_blog_posts_90d`, 02 §C5) | 09 | — | 0 |
| Workbench evidence (w 0; 19 §2.3–§2.5, 02 §K4; stored under pillar P1 except `repo_pushed_days` P2) | technicals `ema72_1h`, `ema72_dist_1h`, `ema72_band_1h`, `ema72_band_pos_1h`, `px_vs_moving`, `ema72_1d`, `ema72_dist_1d`, `ema72_band_1d`, `ema72_band_pos_1d`, `ema72_band_rank_1d`, `rv_30d`, `dd_90d`, `ema72_cross_days_1d`; dereg distance `immunity_remaining_days`, `dereg_rank`, `dereg_rank_30d`, `dereg_rank_90d`, `cull_line_1`, `cull_line_k_30d`, `cull_line_k_90d`, `mp_gap_to_cull_1`, `mp_gap_to_cull_k_30d`, `mp_gap_to_cull_k_90d`, `dereg_safety_30d`, `dereg_safety_90d`, `hours_in_bottom_k_30d`, `spot_rank`, `spot_vs_mp`, `days_to_cull_static` (19 / chain); `repo_pushed_days` (19 / github) | 19 | — | 0 |

`app/storage/catalog.py: METRIC_KEYS: dict[str, MetricSpec(pillar, weight, normaliser, producer_spec, source, role)]` holds this table (roles `scored` | `leg` | `aux` | `evidence`; 12's registry roles map 1:1, with the three P6 inputs as `leg`). `put_metric_values` rejects a key not in `METRIC_KEYS` (`ValueError`, 02 §B4); `tests/storage/test_catalog.py` asserts that `set(METRIC_KEYS)` equals the 109 keys above, that no retired name is present, and that every key of spec 12's `METRICS` registry is in `METRIC_KEYS` with the same pillar, weight and producer `source`. Freshness windows are frozen in 12's registry (02 §C6), not here. Adding a key = one line here + (if scored) a new `spec_version`.

### 3.9 Events catalog (every `event_type` any spec emits)

This is the only events catalog (02 §D1): `emit_event` rejects an unknown `event_type` and a payload missing a required key (extra keys are allowed), and every emitter uses these names, required keys (02 §D2) and `dedupe_key` patterns. Severity vocabulary is **`info` / `warning` / `critical`** (02 §B2; never `warn`, `high` or `error`; old `high` → `critical`). **alert** (02 §D4; 14 §2.4 mirrors this column row for row): `yes` = immediate Telegram via 14's `alert_dispatch`, `digest` = digest line, `no` = stored only (no type uses a `direct` value: a direct `send_alert` by the emitter is only 02 §D3's fallback when the `events` write fails, in which case no row exists to resend); "yes if …" = conditional as stated. Routing rules that override the column: detection events whose condition is also a red flag are **digest-only** — the immediate page is `red_flag_raised`/`red_flag_cleared`; watchlist dereg alerts are 14's `dereg_watch`; any row with `payload.backfilled = true` goes to the digest regardless of type (17 history rows are also inserted with `alerted_at` preset). Runtime/ops conditions are rows with `occupant_key` NULL written through `AlertLatch.fire(key, text, *, event_type=…, severity, payload, dedupe_key, source)` → `emit_event`, falling back to a direct `send_alert` only when the DB write fails (02 §D3). In `dedupe_key` patterns `occ` = `occupant_key` and `day` = `floor(event_at / 86400)`; these patterns are canonical (02 §D2). Every per-occupant pattern must be unique across occupants because `ux_events_dedupe (event_type, dedupe_key)` is global — hence 07's web patterns start with `occ|url|…` (a URL may be shared by several occupants; 07 §11 Q14). Scope `occ` = `occupant_key` set; `sys` = system event (`occupant_key` and `netuid` NULL). `dedupe_key` patterns use `|` separators; `{d7}` = `floor(observed_at / 604800)`, `{d30}` = `floor(observed_at / 2592000)` (re-emission buckets). All payloads are JSON objects; listed keys are required (a value may be `null` where the emitter cannot compute it), `?` = optional. The emitter column gives the spec and its `events.source` value (02 §B9).

| event_type | emitter (`source`) | severity | alert | scope | payload keys | dedupe_key |
|---|---|---|---|---|---|---|
| `netuid_reoccupied` | 04 registry (17's history rows also `source='registry'`) | warning | yes (history rows: digest, `alerted_at` preset) | occ (new) | live (04): netuid, old_occupant_key, new_occupant_key, old_network_registered_at, new_network_registered_at, old_name, new_name, old_status_before, old_last_seen_block, head_block, new_status. **Backfill key set** (used by `emit_event` when `payload.backfilled = true`; 17 §2.2): old_key, new_key, old_name, new_name, reg_block, backfilled | new_occupant_key (history rows: `reoccupied:{old}->{new}`) |
| `occupant_activated` | 04 registry | info | no | occ | netuid, occupant_key, first_emission_block, head_block, days_pre_launch | occupant_key |
| `identity_changed` | 04 registry | warning only when payload `source='chain'` and field ∈ {subnet_name, github_repo, subnet_url, discord}; else info (incl. `owner_coldkey`; owner swaps page through the red flag) | yes if warning, else digest | occ | netuid, occupant_key, source, field, kind (changed\|added\|removed), old_value, new_value, old_hash, new_hash, old_observed_at, via?, resolved_changed | occ\|source\|field\|new_hash |
| `source_disagreement` | 04 registry (non-GitHub identity fields only: `subnet_name`, `subnet_url`, `discord`, …), 06 github (`field='github_repo'` only; 04 never emits it for `github_repo`) | info | digest | occ | netuid, occupant_key, field (06: `'github_repo'`), groups[{key, sources[], values[]}], resolved_source, pair_hash, repo_ids? (06) | occ\|field\|pair_hash |
| `identity_junk` | 04 registry | info; warning for abandonment_marker/leaked_secret | digest | occ | netuid, occupant_key, source, field, value, junk_reason | occ\|source\|field\|value_hash |
| `registry_anomaly` | 04 registry | warning | yes (3rd consecutive) | sys/occ | reason, netuid?, detail | reason\|netuid\|run_date |
| `emission_disabled` | 05 chain | critical | digest (flag `emission_disabled` pages) | occ | netuid, block_number, prev, now | occ\|block_number |
| `emission_reenabled` | 05 chain | info | digest (flag clear pages) | occ | netuid, block_number, prev, now | occ\|block_number |
| `dereg_risk_entered` | 05 chain | critical | digest (flag `dereg_imminent` pages) | occ | netuid, moving_price, rank_from_bottom, pool_size | occ\|{d7} |
| `dereg_risk_cleared` | 05 chain | info | no | occ | netuid, moving_price, rank_from_bottom, pool_size | occ\|{d7} |
| `dereg_proximity` | 05 chain | critical | digest (watchlist pages via 14's `dereg_watch`) | occ | netuid, hazard, components{proximity, starved, decline} | occ\|{d7} |
| `immunity_mismatch` | 05 chain | info | no | occ | netuid, tmc, local, age_blocks | occ\|day |
| `gate_fit_degraded` | 05 chain | info | digest | sys | as_of, residual_pp, theta, n_eligible | as_of |
| `gate_model_broken` | 05 chain (after 7 consecutive `gate_fit_degraded` days, 05 §5 "Alert thresholds") | warning | yes | sys | as_of, consecutive_days, residual_pp | `gate_model_broken\|{d7}` (05; 02 §D2) |
| `chain_constant_drift` | 05 chain (RPC read); 19 `chain_constants_probe` (keyless, daily) | warning | yes | sys | name, config, live, block | name\|live |
| `dereg_rank_entered_k30` / `dereg_rank_cleared_k30` | 19 chain (`wb_dereg`) | info | digest | occ | netuid, rank, k, pool_size, mp_gap_to_cull | occ\|{d7} |
| `immunity_ends_7d` | 19 chain (`wb_dereg`) | warning | digest | occ | netuid, graduates_at, dereg_rank_30d | occupant_key |
| `ema72_band_exit` | 19 chain (`wb_technicals`, only with `WB_EVENTS_ENABLED`) | info | digest | occ | netuid, band_pos, prev_band_pos, side | occ\|day |
| `tmc_schema_drift` | 05 chain | info | digest | sys | added[], removed[] | sha of diff |
| `chain_sse_down` | 05 chain | warning | yes | sys | since, reconnects, last_block | since |
| `chain_sse_recovered` | 05 chain | info | yes | sys | since, reconnects, last_block, down_seconds | since |
| `tmc_budget_degraded` | 05 chain | warning | digest (`spend_cap_reached`, `unit='requests'`, is the page) | sys | requests_month, cap | month |
| `repo_renamed` | 06 github | info | no | occ | repo_id, from, to, detected_at, method | sha1(occ\|type\|repo_id\|to) |
| `repo_moved` | 06 github | warning | digest | occ | repo_id, from, to, from_owner, to_owner, detected_at | sha1(occ\|type\|repo_id\|to) |
| `repo_gone` | 06 github | warning | digest | occ | repo_id?, declared, source, first_404_at, consecutive_404, reason, fallback_primary_repo_id? | sha1(occ\|type\|declared)\|{d30} |
| `repo_restored` | 06 github | info | no | occ | repo_id, full_name | sha1(occ\|type\|repo_id)\|{d30} |
| `repo_archived` | 06 github | warning | digest | occ | repo_id, full_name, role | sha1(occ\|type\|repo_id) |
| `org_repo_archived` | 06 github | warning | digest | occ | owner, repo_id, full_name, is_subnet_related | sha1(occ\|type\|repo_id) |
| `identity_repo_changed` | 06 github | warning | yes | occ | source, from_ref, to_ref, from_repo_id, to_repo_id | sha1(occ\|type\|to_ref) |
| `commit_spam_suspected` | 06 github | warning | digest | occ | repos[], human_commits_28d, flagged_28d, flags_histogram, force_push_repos | occ\|{d30} |
| `site_down_7d` | 07 web | critical | digest (flag `site_down_7d` pages) | occ | url, streak_days, last_status_class, last_http_status, secondary_up | occ\|url\|streak_start_day |
| `site_recovered` | 07 web | info | digest (flag clear pages) | occ | url, down_days | occ\|url\|day |
| `tls_expiring_14d` | 07 web | warning | digest | occ | url, tls_days_left, tls_not_after, tls_issuer, expired | occ\|url\|tls_not_after |
| `site_parked` | 07 web | warning | yes | occ | url, final_url, parked_signals | occ\|url\|check_day |
| `site_hijack_suspect` | 07 web | critical | yes | occ | url, final_url, hijack_signals, change_ratio | occ\|url\|check_day |
| `site_content_reset` | 07 web | warning | digest | occ | url, prev_hash, new_hash, prev_len, new_len, change_ratio | occ\|url\|new_hash |
| `site_cross_domain_redirect` | 07 web | info | digest | occ | url, final_url, final_domain | occ\|url\|final_domain |
| `docs_gone` | 07 web | info | digest | occ | url, last_up_day, streak_days | occ\|url\|streak_start_day |
| `invite_expired` | 08 community | info; warning if the code is the occupant's primary invite and no other venue is reachable | digest (08 sends no direct alerts; flag `stale_identity` pages) | occ | netuid, occupant_key, invite_code, stage (expiring\|expired), expires_at, guild_id, last_valid_at, declared_by[], other_reachable_venue | occ\|code\|stage |
| `invite_dead` | 08 community | info; warning as above | digest | occ | netuid, occupant_key, invite_code, error_code, first_dead_at, last_valid_at, guild_id, declared_by[], other_reachable_venue | occ\|code\|error_code |
| `inflated_community` | 08 community | warning | digest (flag `inflated_community` pages) | occ | netuid, occupant_key, guild_id, members, presence_median, ratio, days, streak_start, guild_created_at, source_agreement | occ\|guild_id\|streak_start |
| `community_venue_changed` | 08 community | info; warning when {own_server, main_channel} → {invalid, none} or a TG community becomes not_found/user_or_unknown | digest | occ | netuid, occupant_key, platform, old_class, new_class, old_guild_id, new_guild_id, handle, first_seen_new_at, reason | occ\|platform\|new_class\|new_guild_id or handle\|first_seen_new_day |
| `silence_45d` | 08 community | warning | digest (flag `silence_45d` pages) | occ | netuid, occupant_key, silence_days, lower_bound, legs{x, discord, tg}, last_comm_at, last_comm_platform, min_leg? | occ\|episode_start_date |
| `x_account_unresolved` | 09 x | warning | digest | occ/sys (KOL) | handle, kind, reason, consecutive, provider | handle\|reason\|{d7} |
| `x_account_gone` | 09 x | warning | yes | occ/sys | handle, author_id, last_post_at, since | author_id |
| `x_handle_changed` | 09 x | info | no | occ/sys | author_id, old_handle, new_handle | author_id\|new_handle |
| `x_budget_paused` | 09 x | warning | digest (02 §D4: every reason; the budget-stop page is `spend_cap_reached`) | sys | provider, usd_mtd, cap_usd, projected_usd, reason | provider\|month |
| `x_budget_resumed` | 09 x | info | no | sys | provider, month | provider\|month |
| `x_ctf_discrepancy` | 09 x | warning | digest | occ | handle, ctf_last_post_at, provider_last_post_at, gap_days | handle\|{d7} |
| `feed_dead` | 09 rss | warning | digest | occ/sys | feed_url, kind, consecutive_failures, last_status | feed_url\|{d30} |
| `youtube_channel_resolved` | 09 rss | info | no | occ | input_url, channel_id, method | occ\|channel_id |
| taxonomy events: `product_launch`, `release_upgrade`, `partnership_integration`, `revenue_customer`, `token_buyback_burn`, `exchange_listing`, `funding`, `emissions_tokenomics_change`, `team_change`, `security_incident`, `delay_missed_milestone`, `governance_dispute`, `regulatory`, `owner_extraction_evidence`, `giveaway_promo`, `other` | 10 llm (only materiality ≥ 2) | positive → info; negative mat 2 → warning; negative mat 3 confirmed → critical | yes if critical | occ | post_ids[], label_version, direction, materiality, summary, url, confidence, n_supporting, confirmed_by? | 10's key: occ\|event_type\|{48 h bucket}\|{polarity} — the former "topic hash" slot is the polarity bucket `pos`\|`neg`\|`neu` from the label's direction (+1/−1/0), so opposite directions in one bucket are separate rows (e.g. `{occ}\|product_launch\|{bucket}\|pos`, 10 §9) |
| `owner_control_change` | 11 integrity (was `owner_coldkey_swap`, 02 §D1) | warning | digest (flag `owner_control_change` pages) | occ | netuid, change_id, change_type, old_coldkey, new_coldkey, classification_basis, detected_via, whitelisted, review_id | occ\|ck\|new_coldkey |
| `owner_coldkey_swap_announced` | 11 integrity (D9, TaoStats weekly sweep) | warning | digest | occ | netuid, change_id, extrinsic_id, block_number, announced_at | occ\|announce\|extrinsic_id |
| `owner_hotkey_repointed` | 11 integrity | info | digest | occ | netuid, change_id, old_hotkey, new_hotkey | occ\|hk\|new_hotkey |
| `owner_identity_churn` | 11 integrity | warning | yes | occ | netuid, change_id, repoint_change_id, identity_event_id, field, days_apart | occ\|churn\|change_id |
| `owner_perpetual_off` | 11 integrity (was `owner_perpetual_lock_disabled`, 02 §D1) | critical | digest (flag `owner_extraction` pages) | occ | netuid, change_id, perp_before, perp_after, decay_before, decay_after, detected_via, hour_ts | occ\|perpoff\|day |
| `owner_decaying_lock_added` | 11 integrity | info | digest | occ | netuid, delta_perp, delta_decay, hour_ts | occ\|owner_decaying_lock_added\|day |
| `owner_lock_reduced` | 11 integrity | warning | digest | occ | netuid, delta_perp, delta_decay, hour_ts | occ\|owner_lock_reduced\|day |
| `owner_extraction_detected` | 11 integrity (was `owner_sell`/`owner_full_exit_call`, 02 §D1; D4 hard triggers; D4b full-exit extrinsic with `reasons ⊇ ['full_exit_extrinsic']` plus `function?, extrinsic_id?, block_number?`) | critical | digest (flag `owner_extraction` pages) | occ | netuid, change_id, reasons[], sell_ratio, max_impact7, tao_rcv_30d, out_alpha_30d, earned_30d, top_event_uids[≤5] | occ\|extract\|iso_week |
| `owner_key_compromise_override` | 11 integrity | warning | yes | occ | netuid, window_start, window_end, whitelist_id, evidence_url, by | occ\|kc\|whitelist_id |
| `owner_auto_lock_changed` | 11 integrity | info | digest | occ | netuid, old, new, source | occ\|owner_auto_lock_changed\|day |
| `owner_cut_toggled` | 11 integrity (was `owner_cut_enabled_changed`, 02 §D1) | info | digest | occ | netuid, old, new, source | occ\|owner_cut_toggled\|day |
| `owner_source_disagreement` | 11 integrity | info | digest | occ | netuid, field, values{source: value}, since | occ\|srcdis\|field\|day |
| `owner_position_unexplained` | 11 integrity | info | digest | occ | netuid, prev_total, curr_total, inflow, outflow, unexplained | occ\|unexpl\|day |
| `owner_concentration` | 11 integrity | info | digest | occ | netuid, owner_holding_share, lock_ratio (S4 in ts-1.0 = event only) | occ\|conc\|iso_week |
| `owner_self_dealing_suspected` | 11 integrity (S4 in `ts-1.1` only; never emitted by `ts-1.0`) | warning | digest | occ | owner_vali_stake_share, self_weight, owner_holding_share, lock_ratio | occ\|{d30} |
| `owner_data_degraded` | 11 integrity | warning | yes | sys | n_none, n_total, reasons{} (> 25 % of occupants with S1, S2, S3 all None) | owner_degraded\|day |
| `team_member_added`, `team_member_verified`, `team_member_rejected`, `team_member_delisted` | 11 integrity | info | digest | occ | member_id, name, role, source, status, checked_at?, evidence_count?, domains[]?, reason?, by | team\|member_id\|type\|checked_at or day |
| `team_disclosure_changed` | 11 integrity | info | digest | occ | old, new, source | occ\|disc\|new\|day |
| `tier_change` | 12 scoring | info up / warning down | yes | occ | netuid, spec_version, as_of, from, to, direction, composite_smoothed, prev_composite_smoothed, composite, threshold, top_positive, top_negative, red_flags, backfilled? | occ\|tier_change\|spec\|as_of |
| `red_flag_raised` | 12 scoring | critical if multiplier ≤ 0.6, else warning | yes | occ | netuid, spec_version, as_of, flag, multiplier, human, sticky, detail, flag_multiplier_total | occ\|red_flag_raised\|flag\|raised_at |
| `red_flag_cleared` | 12 scoring | info | yes | occ | netuid, spec_version, as_of, flag, multiplier, raised_at, reason (hysteresis\|manual) | occ\|red_flag_cleared\|flag\|as_of |
| `score_jump` | 12 scoring | warning | yes (02 §D4) | occ | netuid, spec_version, as_of, prev_as_of, prev_composite, composite, delta, pillar_deltas{P1..P6}, flags_added, flags_removed, coverage_delta | occ\|score_jump\|spec\|as_of |
| `insufficient_data` | 12 scoring | info | digest | occ | netuid, spec_version, as_of, confidence, coverage{P1..P5}, missing[] | occ\|insufficient_data\|spec\|as_of |
| `rank_instability` | 12 scoring | info | digest | sys | spec_version, as_of, series, lag_days, n, rank_autocorr, quintile_turnover, top20_turnover | system\|rank_instability\|spec\|as_of |
| `manual_entry` | 14 api | info | no | occ | kind (team\|product\|note\|flag), by, row_id, summary, evidence_url | NULL |
| `collector_paused` | 14 api | info | no | sys | collector, by, reason | NULL |
| `collector_resumed` | 14 api | info | no | sys | collector, by | NULL |
| `rescore_requested` | 14 api | info | no | sys | as_of, spec_version, shadow, run_id, by | run_id |
| `dereg_watch` | 14 api | warning | yes | occ | netuid, name, reason, moving_price_rank, immune, blocks_left_immunity, dereg_safety | occ\|reason\|day |
| `collector_failing` | 01 runtime (wrapper → `AlertLatch.fire` → `emit_event`, 02 §D3) | warning | yes | sys | collector, consecutive_failures, last_error (16 adds `job` = same id) | collector\|first_failure_at |
| `collector_recovered` | 01 runtime | info | yes | sys | collector, failures, down_seconds | collector\|first_failure_at |
| `service_recovered_unclean` | 16 ops | warning | yes | sys | kind, downtime_sec, prev_ts, orphan_runs, sse_gap_blocks_est, missed[] | boot_id |
| `spend_80pct` | 16 cost (was `spend_soft_cap` / latch `budget_soft:`, 02 §D1) | warning | yes | sys | provider (= group key; 14 dedupes on it), group, month, usd_mtd, cap_usd, pct, unit (usd\|requests), usd_month? | group\|month\|80 |
| `spend_cap_reached` | 16 cost (was `spend_hard_cap` / latch `budget_hard:`, 02 §D1) | critical | yes | sys | provider (= group key), group, month, usd_mtd, cap_usd, reserved_usd, denied_est_usd, paused_jobs[], unit?, usd_month? | group\|month\|cap |
| `spend_reset` | 16 cost | info | digest | sys | group, month, provider? | group\|month\|reset |
| `budget_override_set` | 16 cost | info | digest | sys | group, month, extra_usd, by, reason, kind (override\|set), provider? | NULL |
| `backup_failed` | 16 ops | critical | yes (direct `send_alert` only as the 02 §D3 DB-write-failure fallback) | sys | error, last_good_name, last_good_age_h (not `last_ok_age_hours`), backup_dir? | day |
| `backup_mirror_missing` | 16 ops | warning | yes | sys | mirror_dir, marker | day |
| `backup_mirror_failed` | 16 ops | warning | yes | sys | mirror_dir, error | day |
| `backup_stale` | 16 ops (`ops_guard_5m`) | warning | yes | sys | age_hours, stale_hours | day |
| `scoring_stalled` | 12 scoring (via `AlertLatch`, 02 §D1/§D3) | critical | yes | sys | slot_as_of, last_ok_as_of, missed_slots, last_error | slot_as_of |
| `discord_gateway_fatal` | 08 community (gateway LongTask gave up after its retry budget) | warning | yes | sys | error, close_code, retries, since | since |
| `backfill_done` | 17 backfill (manual jobs; `source='runtime'` — a system event about the job, not a backfilled data row, 02 §B9) | info | digest | sys | job, era, rows, seconds, coverage_path | job\|run_id |
| `backfill_failed` | 17 backfill (manual jobs; `source='runtime'`) | warning | yes | sys | job, era, error, resume_hint | job\|run_id |
| `disk_low` | 16 ops | warning | yes | sys | free_gb, min_gb, db_gb, backups_gb, logs_gb, path? | day |
| `disk_critical` | 16 ops | critical | yes (normal row via `AlertLatch` → `emit_event` → dispatcher; a direct `send_alert` happens **only** as 02 §D3's fallback when the DB write fails, and then no row exists) | sys | free_gb, min_gb, db_gb, backups_gb, logs_gb, path? | day\|6h_bucket |
| `db_integrity_failed` | 16 ops | critical | yes (as `disk_critical`: direct `send_alert` only as the 02 §D3 DB-write-failure fallback) | sys | quick_check (first 5 lines) | boot_id |
| `schema_warning` | 03 storage | warning | yes | sys | kind (checksum_drift\|schema_ahead), version, detail | kind\|version |
| `retention_skipped` | 03 storage | warning | digest | sys | reason (no_recent_backup\|paused), policies[], backup_age_hours | day |

**Red-flag names and multipliers** (`catalog.RED_FLAGS`, master §7.5) carried in `red_flag_raised.payload.flag`, `scores.red_flags[]` (names) and `red_flag_state.flag`: `emission_disabled` ×0.50, `dereg_imminent` ×0.60, `miner_burn_zeroed` ×0.60, `owner_extraction` ×0.70, `owner_control_change` ×0.85, `key_compromise` ×0.50, `repo_gone` ×0.80, `all_repos_archived` ×0.70, `site_down_7d` ×0.85, `stale_identity` ×0.90, `silence_45d` ×0.85, `inflated_community` ×0.90 (product floored at 0.25). Detection events that share a name with a flag (`site_down_7d` from 07, `silence_45d` and `inflated_community` from 08) and the other producer events (`emission_disabled`/`emission_reenabled`/`dereg_risk_entered`/`dereg_proximity` 05, `site_recovered` 07, `repo_gone` 06, `owner_extraction_detected`/`owner_perpetual_off`/`owner_control_change` 11) are *trigger evidence* only and are **digest-only** (02 §D4); the multiplier is engaged exclusively by spec 12's state machine (`red_flag_state`, fed by the metric rows, e.g. 11's `owner_extraction_trigger`/`owner_control_change_trigger`), which emits `red_flag_raised`/`red_flag_cleared` — the one immediate page per flag transition, and what satisfies master §7.5 "each also an `events` row". Watchlist dereg alerts are 14's `dereg_watch`.

---

### 3.10 Workbench tables (migration 0003 `workbench`, owner 19; added 2026-10-16)

`app/storage/migrations/0003_workbench.sql` (`-- @allow-destructive`: rebuilds `manual_notes` to add the `kind` value `chart_read`; the table was empty when it ran). New tables, all STRICT: `wb_presets(id, name UNIQUE, weights_json CHECK json_valid, created_at, updated_at)` — the screener's named slider presets; `wb_uploads(id, netuid, occupant_key, path, sha256, media_type, bytes, model, status ∈ {stored, ok, unreadable, failed, vision_disabled}, note_id, created_at)` — chart screenshots (19 §2.7); `wb_dereg_network_hourly(hour_ts PK, head_block, immunity_period_blocks, immunity_source, pool_size, pool_size_30d, pool_size_90d, k_30, k_90, reg_gap_mean_days, n_regs_window, last_registration_block, last_registration_ts, earliest_next_registration_block, eta_next_registration_days, lock_cost_tau, lock_cost_decay_tau_per_day, lock_cost_observed_at, graduates_30d, graduates_90d, cull_line_1, observed_at, run_id)` WITHOUT ROWID — the hourly dereg "weather" row (19 §2.4), written only by `wb_dereg` (upsert per hour). A chart read is a `manual_notes` row with `kind='chart_read'`, `created_by='wb_vision:<model>'`, `evidence_url` = the stored image path (02 §K2). Retention: none (small).

## 4. Failure-mode table (storage)

Storage is not a collector, so there is no absence-vs-failure mapping of its own; it **implements** the mapping for everyone else (`metric_values.status`, §2.1). The table below is what the storage layer does when it fails.

| failure | detection | behaviour | `/health` | alert / event |
|---|---|---|---|---|
| SQLite < 3.37 or JSON1 missing | `connect()` probe | `SchemaError`, boot fails (systemd backs off) | — | journal |
| Migration statement fails | `migrate()` | transaction rolled back; DB stays at the previous version; `MigrationError` → boot fails; `python -m app db migrate` reproduces | — | journal |
| Applied migration edited (checksum drift) | `migrate()` | not re-applied; boot continues | `degraded_reasons: schema_warning` | `schema_warning` |
| DB newer than code (rollback) | `migrate()` | boot continues (additive policy) | idem | `schema_warning` |
| `database is locked` beyond `busy_timeout` (external script) | `sqlite3.OperationalError` | propagates → job `ok=0`, retried next tick; reader unaffected (WAL) | job `last_error` | via `collector_failing` at 5 |
| Append-only / STRICT / CHECK violation | `sqlite3.IntegrityError` | statement (and its transaction) aborted; nothing rewritten; job fails with the trigger/constraint message | job `last_error` | via `collector_failing` |
| Unknown `metric_key`, raw row without a valid `value_json.status`, unknown `event_type` or missing required payload key | `put_metric_values` / `emit_event` validation (`ValueError`, before any SQL) | nothing written for that call; the producer's job fails loudly (a naming drift is a bug, not data); `AlertLatch.fire` falls back to a direct `send_alert` (02 §D3) | job `last_error` | via `collector_failing` |
| Disk full (`SQLITE_FULL`) | `OperationalError` | writes fail; jobs fail; next retention run is ungated (disk emergency); operator frees space or raises disk | `disk_low` | `disk_low` |
| WAL > `DB_WAL_WARN_MB` | `stats()` in `/health` | retention `wal_checkpoint(TRUNCATE)`; readers are short queries so checkpoints complete | `degraded_reasons: wal_large` | none |
| Power loss mid-transaction | WAL recovery on next open | uncommitted transaction discarded atomically; `collector_runs` orphan (`finished_at NULL`); jobs are watermark-based and re-run | `orphan_runs` (01) | none |
| Corrupt DB file | `python -m app check` (`quick_check`) / open error | boot fails loudly; **never auto-restore**; §10 restore runbook | — | journal |
| `VACUUM INTO` or `quick_check` fails | `Database.backup_to` | tmp deleted, `RuntimeError`; older backups untouched; spec 16's `create_backup` turns it into a job failure | `db.last_backup_age_hours` | `backup_failed` (16) |
| Backup pipeline failures (disk preflight, gzip verify, mirror missing/failed, budget pruning) | spec 16 `create_backup` | per spec 16 §4 / §7.1 | per spec 16 | `backup_failed`, `backup_mirror_missing`, `backup_mirror_failed` (16) |
| No backup < 30 h at retention time | backup gate | destructive policies skipped; privacy policies run | — | `retention_skipped` |
| Retention batch fails mid-run | exception in a batch | committed batches stay (idempotent), job `ok=0`, next day resumes | job `last_error` | via `collector_failing` |
| Reader cannot open (file just created, permissions) | `connect()` | reader disabled, all reads on the writer, warning logged | `db.reader=false` | none |
| Wall clock jumps backwards (NTP) | — | `observed_at` may be non-monotonic for minutes; PIT reads use `≤ t` and unique keys use `as_of`/natural keys, so nothing breaks; unit `After=time-sync.target` (01) minimises it | — | none |

---

## 5. Configuration

All keys live on 01's `Settings` (read dynamically via `app.config.settings`). Keys owned elsewhere are listed where this spec fixes or depends on their default.

**Owned by 01 (used here, defaults unchanged):** `DB_PATH` (`data/taosentinel.db`), `DB_BUSY_TIMEOUT_MS` (5000), `DATA_DIR`, `DISK_FREE_MIN_GB` (2.0), `DB_BACKUP_ENABLED` (True), `DB_BACKUP_DIR` (`data/backups`), `DB_BACKUP_MIRROR_DIR` (""), `DB_BACKUP_RETENTION` (7), `DB_BACKUP_WEEKLY_RETENTION` (8), `DB_BACKUP_INTERVAL_HOURS` (24), `DB_BACKUP_MIN_AGE_HOURS` (12.0), `LLM_ENABLED`, `HEALTH_HOST`, `HEALTH_PORT` (8090), `SPEC_VERSION` (`ts-1.0`).

**Owned by 16 (used here; not redefined):** `DB_BACKUP_COMPRESS` (True, gzip), `DB_BACKUP_GZIP_LEVEL` (6), `DB_BACKUP_MAX_TOTAL_GB` (**12**, local backup dir only; 02 §F2/§I1), `DB_BACKUP_MIRROR_MAX_TOTAL_GB` (separate key for the USB/NAS mirror), `DB_BACKUP_KEEP_UNCOMPRESSED` (**1**: the newest backup stays uncompressed for spec 13), `DB_BACKUP_CRON_HOUR`/`_MINUTE` (03:30Z), `DB_BACKUP_WAIT_FOR_JOBS` (**`storage_retention_daily`**, 02 §E3), `DB_BACKUP_WAIT_MAX_SEC` (1200), `DB_BACKUP_STALE_HOURS` (36), `DB_WAL_WARN_MB` (1024, `/health` `wal_large`), `DB_QUICK_CHECK_ON_UNCLEAN`. **Owned by 12:** `METRIC_NORM_PROVISIONAL_RETENTION_DAYS` (14; spec 12 prunes provisional `stage='norm'` rows itself), `SCORING_OFFICIAL_HOUR` (0, pinned; the validator rejects any other value, 02 §B6). **Owned by 05:** `CHAIN_TICKS_BLOCK_STRIDE` (5 ≈ one tick snapshot per minute, 02 §F2). **Owned by 08:** `DISCORD_STORE_TEXT` (True), `TELEGRAM_POSTS_STORE_TEXT` (False), `DISCORD_TEXT_MAX_CHARS` (4000). **Owned by 17:** `LEDGER_NRA_TRUST_FROM_TS` (1780182493), `LEDGER_LIVE_FROM_TS` (1777468595).

**Owned by siblings, default fixed here (storage owns retention):**

| key | type | default | validator | note |
|---|---|---|---|---|
| `CHAIN_TICKS_RETENTION_DAYS` | int | **30** (02 §F2) | 2 ≤ x ≤ 365 | full-res ticks window; afterwards `chain_hourly` + 5m bars only |
| `LLM_TEXT_RETENTION_DAYS` | int | 90 (10) | 1 ≤ x ≤ 90 | text → hash-only after this many days, labelled or not (02 §B10) |
| `LLM_PENDING_TEXT_MAX_DAYS` | int | 7 (amended 02 §B10; no owner spec named there — listed here as a retention key) | 1 ≤ x ≤ `LLM_TEXT_RETENTION_DAYS` | 08 stamps `announcements.text_purge_after = observed_at + x·86400` when the platform's `*_STORE_TEXT` flag is false; §2.7 purges at that time or once labelled |
| `WEB_STORE_TEXT_MAX_CHARS` | int | 32000 (07) | 1000 ≤ x ≤ 200000 | cap per stored page text |

**New (03):**

| key | type | default | validator | semantics |
|---|---|---|---|---|
| `DB_READER_ENABLED` | bool | `True` | — | second read-only connection (§2.2) |
| `DB_CACHE_SIZE_MB` | int | 64 | 8 ≤ x ≤ 1024 | per connection |
| `DB_MMAP_SIZE_MB` | int | 256 | 0 ≤ x ≤ 4096 | 0 disables mmap |
| `DB_INCREMENTAL_VACUUM_PAGES` | int | 25600 | ≥ 0 | pages returned per retention run (≈ 100 MB at 4 KiB pages); 0 = off |
| `STORAGE_RETENTION_ENABLED` | bool | `True` | — | job flag |
| `STORAGE_RETENTION_CRON_HOUR` | int | 3 | 0–23 | UTC; must precede `DB_BACKUP_CRON_HOUR:MINUTE` |
| `STORAGE_RETENTION_CRON_MINUTE` | int | 0 | 0–59 | UTC (03:00Z, 02 §E3) |
| `RETENTION_REQUIRE_BACKUP_HOURS` | float | 30.0 | ≥ 1 | backup gate for destructive policies |
| `RETENTION_DELETE_BATCH` | int | 50000 | 1000–200000 | rows per transaction |
| `PRICE_BARS_5M_RETENTION_DAYS` | int | 90 | ≥ 7 | |
| `METRIC_PROVISIONAL_RETENTION_DAYS` | int | 90 | ≥ 14 | provisional `stage='raw'` rows only; official rows are permanent |
| `CHAIN_REST_SNAPSHOTS_RETENTION_DAYS` | int | 90 | ≥ 7 | newest per endpoint kept |
| `GITHUB_HTTP_CACHE_RETENTION_DAYS` | int | 30 | ≥ 1 | |
| `LLM_BATCH_ITEMS_RETENTION_DAYS` | int | 90 | ≥ 7 | |
| `WEB_TEXT_RETENTION_DAYS` | int | 365 | ≥ 30 | newest text per URL kept |

Retired: `SOCIAL_TEXT_HARD_MAX_DAYS` (text now goes to hash-only at `LLM_TEXT_RETENTION_DAYS` unconditionally, 02 §B10). Model validators: `(STORAGE_RETENTION_CRON_HOUR, _MINUTE) ≥ (DB_BACKUP_CRON_HOUR, _MINUTE)` → warning `retention_after_backup` (not fatal; the backup would snapshot unpruned data). Boot fails on a raise with the pydantic message (01 §7).

**Job registration (01 §2.7 must add this row):** `storage_retention_daily` | `storage.retention:RetentionJob` | **cron** `hour=STORAGE_RETENTION_CRON_HOUR minute=STORAGE_RETENTION_CRON_MINUTE` (03:00Z) | misfire 3600 | `STORAGE_RETENTION_ENABLED` | — (`min_gap_factor=0`) | phase 0 | provider — | paid no | `ops_exempt` yes (16 §2.11) | timeout 3600. `backup_daily` is owned by 16 (cron 03:30Z).

**Kill-switch / pause semantics (02 §B12).** Storage jobs are free: `KILL_SWITCH` does not pause them; `SCORING_KILLED` does not affect storage; `PAUSE_ALL` does not either (`ops_exempt`). Operator pause (`control_state(target='backup_daily'|'storage_retention_daily', key='paused', value='true')`, or the emergency file `data/control/<job>.paused`) is honoured by 01's wrapper; a paused backup surfaces as `backup_stale` after `DB_BACKUP_STALE_HOURS` (36) and gates destructive retention after 30 h, which is the intended fail-safe (no pruning without a recent backup).

---

## 6. Cadence, rate limits, cost

No external calls → no provider token bucket (the shared TMC bucket ≤ 8 req/min is never touched), zero USD. Local cost only:

| activity | cadence | cost |
|---|---|---|
| `storage_retention_daily` | cron 03:00Z | ≈ 0.19 M tick deletes/day at the default stride 5 (≈ 0.93 M at stride 1) in 4 (19) batches of 50 k (≈ 1 s lock hold each), ≈ 48 k hourly provisional `metric_values` deletes; checkpoint; ≤ 100 MB incremental vacuum; typically < 5 min |
| `backup_daily` (spec 16) | cron 03:30Z, after retention | disk write ≈ DB size (`VACUUM INTO`) + gzip/sha256 CPU (spec 16 §6.1) |
| WAL checkpoints | automatic every 1000 pages | negligible |
| `PRAGMA optimize` | daily in retention | negligible |

**Row growth and disk (steady state, 129 netuids, ~130 occupants, 7,200 blocks/day).** Row sizes are estimates (STRICT, WITHOUT ROWID where declared, including index overhead) and are **UNVERIFIED** until 30 days of live data (`python -m app db stats` prints per-table sizes via `dbstat` when available).

| table | rows / month | ≈ bytes / row | MB / month | retention → steady-state size |
|---|---|---|---|---|
| `chain_ticks` (stride 5 = default, 02 §F2 / stride 1) | 5.6 M / 27.9 M | 150 | 840 / 4,200 | 30 d window → **0.84 GB / 4.2 GB** |
| `chain_hourly` | 93 k | 650 (with `extras_json`) | 60 | forever |
| `chain_daily` | 3.9 k (+47 k one-off 1Y) | 500 | 2 | forever |
| `price_bars` 5m / 1h | 1.11 M / 93 k | 90 | 100 / 8 | 5m 90 d → 300 MB; 1h forever |
| `price_bars` from ledger + RPC gap (one-off, 17) | ≈ 0.18 M ledger (149 k hourly + 26 k daily groups) + ≈ 0.3 M RPC | 90 | ≈ 45 once | forever |
| `pool_history` import (one-off, + Pi rows) | 7.36 M (+ ≈ 1.9 M Pi) | ≈ 130 with provenance | ≈ 1,200 once (≈ 350 with 17's `LEDGER_IMPORT_THIN_SEC=300`) | forever |
| `holders_snapshots` | 560 | 8 k | 4.5 | forever |
| `chain_rest_snapshots` | ~45 MB of payloads | — | 45 | 90 d → 135 MB |
| `chain_trending` (+daily) | 36 k (+3.9 k) | 60 | 2.5 | forever |
| `metric_values` raw official (≈ 70 keys × 130) | 273 k | 300 | 82 | forever |
| `metric_values` raw provisional (05/11, 06/12/18:00Z) | 234 k | 300 | 70 | 90 d → 210 MB |
| `metric_values` raw provisional hourly (12 `features_hourly`: ≈ 16 fn/derived keys × 130 × 23 h, 02 §B4 hour buckets) | 1.4 M | 300 | 430 | 90 d → ≈ 1.3 GB (30 d with `METRIC_PROVISIONAL_RETENTION_DAYS=30` → 0.43 GB) |
| `metric_values` norm official / provisional (45 keys × 130 per slot) | 175 k / 527 k | 300 | 53 / 158 | official forever; provisional 14 d (12) → 75 MB |
| `scores` (+ `score_runs`, `regime_daily`, `rank_stability_daily`) | 15.6 k | ≈ 1,750 (official 4 KB explain, provisional 1 KB) | 27 | forever |
| `events` | ~10 k | 400 | 4 | forever |
| `github_commits` (+ one-off history ≈ 0.6 M) | 30 k | 300 | 9 (+180 once) | forever |
| `github_activity_daily` / `github_authors_daily` / `github_repo_snapshots` | 12 k / 30 k / 12 k | 100–200 | 10 | forever |
| `web_checks` | 6 k | 1,500 (+ changed text) | 15 | text 365 d |
| `discord_snapshots` / `telegram_snapshots` / `announcements` | 21 k (08: ≈ 710/day) / 240 / 1.5 k | 300 / 300 / 1,000 (own-server text) | 8 | forever (text 90 d) |
| `social_posts` + `social_post_mentions` | 39 k + 45 k | 900 → 400 after text drop | 39 | text 90 d |
| `post_labels` (+ entities) | 39 k | 700 | 27 | forever |
| `owner_snapshots` / `owner_position_hourly` / `owner_stake_events` / `owner_changes` | 4 k / 93 k / 6 k / < 100 | 1,500 / 130 / 500 | 21 | forever (hourly rollup: §11) |
| identity, reference, ops tables (+ `block_times`, `backfill_buckets`, `rpc_owner_history`) | < 25 k | — | < 10 | forever |

**Totals.** One-off seed ≈ 1.45 GB (≈ 0.6 GB with ledger thinning). Continuous growth ≈ **0.39 GB/month** plus capped pools ≈ 2.1 GB (of which ≈ 1.3 GB are 12's hourly provisional raw rows) plus the tick window. Year-1 DB ≈ **9.1 GB at the default stride 5**, ≈ **12.4 GB at stride 1** (UNVERIFIED estimates; ≈ 7.8 / 11 GB without the hourly provisional rows). Backups and peak disk are spec 16's (§6.1: 7 + 8 gzip rotation, local budget `DB_BACKUP_MAX_TOTAL_GB=12` with the 2 newest stamps never pruned, the newest also kept uncompressed, the full rotation on the mirror; peak ≈ 4.7 × DB + 5 GB). With this DB estimate that is ≈ 48 GB at month 12 on stride 5, so **≥ 60 GB free is recommended** (02 §I1). 20 GB is the bare minimum and only with stride 5, the 12 GB local budget and a USB/NAS mirror; it lasts only while the DB is ≲ 3 GB, i.e. the first months (UNVERIFIED). `METRIC_PROVISIONAL_RETENTION_DAYS=30` saves ≈ 0.9 GB if disk is tight.

---

## 7. Failure modes and fail-safe behaviour

1. **History cannot be rewritten by accident.** STRICT types, CHECKs and triggers make the PIT rules structural: ISO strings cannot enter INTEGER time columns, a raw `failed` metric cannot carry a value and a NULL cannot be an `ok` measurement, a successful raw `metric_values` row is never replaced (only a `failed` row may be upgraded by a later success), `norm` rows are insert-once and official rows cannot be deleted, `scores` cannot be updated or deleted, alerted events are frozen, `pool_history` accepts only NULL-only enrichment, and third-party Discord text can only be stored for own-server crossposts.
2. **Nothing destructive runs without a recent verified backup** (retention backup gate), except privacy nulling (always) and disk emergencies (free space < `DISK_FREE_MIN_GB`).
3. **Boot fails loudly, never silently degrades the schema:** version/JSON1 check, migration errors and corruption stop the process; `python -m app check` reproduces the error; restore is manual (spec 16 §2.8).
4. **Backups are verified before they count:** `Database.backup_to` runs `quick_check` on the copy before the atomic rename; spec 16 adds the SHA-256 manifest, gzip verification and the "never delete the two newest" budget rule.
5. **Readers never block writers or vice versa:** WAL + separate reader connection; long analytical work (spec 13) runs on a snapshot file, not the live DB.
6. **Power loss:** WAL + `synchronous=NORMAL` loses at most the last commits; every writer is idempotent on natural keys (`INSERT OR IGNORE`, precedence upserts, `put_metric_values`), so re-running a job after restart converges; a crash inside spec 12's slot transaction leaves nothing (retried by catch-up); the tick → hourly rollup guarantee in `chain_prune` repairs hours the SSE recorder never flushed.
7. **Import cannot corrupt live data:** legacy files are attached `mode=ro&immutable=1` (or a checkpointed copy when a `-wal` exists); `pool_history` is `INSERT OR IGNORE` plus trigger-guarded NULL-only enrichment; derived rows go through precedence upserts that never downgrade a live bar.

---

## 8. Implementation plan

**Files (all under `~/taosentinel/app/storage/` unless noted):**

| file | contents | port from |
|---|---|---|
| `__init__.py` | re-exports `Database`, `ExecResult`, `SchemaError`, `MigrationError`, `AppendOnlyViolation`, `CODE_SCHEMA_VERSION` | — |
| `db.py` | `Database` (§2.3), `APPEND_ONLY_TABLES`, `MIN_SQLITE`, `_apply_pragmas(conn, *, reader: bool)` (also registers `src_rank`), `_row_to_dict` | `E:\SN_Trader\SubnetTrader\app\storage\db.py:17-80` (init/connect/close/backup_to verbatim, then edited: settings read dynamically instead of the old static `from app.config import settings` at `db.py:10`), `:121-134` (execute/fetchone/fetchall) |
| `models.sql` | §3.1–§3.6 DDL + §3.7 views, in document order | `models.sql:55-73, 136-167` (legacy columns verbatim) |
| `migrations/README.md` | naming rule, additive policy, lint rules | — |
| `migrate.py` | `Migration`, `MigrateResult`, `discover`, `split_statements`, `migrate`, `CODE_SCHEMA_VERSION` (§2.4) | replaces `db.py:82-119` |
| `precedence.py` | `SOURCE_RANK: dict[str, dict[str, int]]` for `chain_hourly`, `chain_daily`, `price_bars`; `rank_any(source) -> int` (for `src_rank`); `precedence_upsert_sql(table: str, cols: Sequence[str], conflict: Sequence[str], *, coalesce: Sequence[str] = (), keep: Sequence[str] = (), same_rank: str \| None = None) -> str` (`same_rank` = the §3.3 predicate, e.g. `"excluded.ts_last > price_bars.ts_last"`; `keep` = columns kept when already set, e.g. `chain_daily.occupant_key`) | — |
| `pit.py` | `put_metric_values` + alias `upsert_metric_values` (the single `metric_values` writer, §2.5, 02 §B4); `MetricRow`, `metric_at`, `cross_section`, `latest_scores` (§2.6, 02 §B5; read-only) | — |
| `catalog.py` | `METRIC_KEYS: dict[str, MetricSpec(pillar, weight, normaliser, producer_spec, source, role)]` (§3.8, 79 keys); `RETIRED_METRIC_KEYS: frozenset[str]` (02 §C2/§C3; `put_metric_values` names them in its `ValueError`); `EVENT_TYPES: dict[str, EventSpec(severity, alert, scope, emitter, source, payload_keys, backfill_payload_keys=None)]` (§3.9); `SOURCES: frozenset[str]` (02 §B9, = both CHECK lists); `RED_FLAGS: dict[str, float]` (multipliers, master §7.5) | — |
| `events.py` | `async def emit_event(db, *, event_type, occupant_key, netuid, severity=None, event_at, observed_at, payload, dedupe_key, source, run_id, alerted_at=None) -> int \| None` — validates against `EVENT_TYPES` (unknown type → `ValueError`; severity defaults from catalog; required payload keys checked, extra keys allowed; when `payload.get("backfilled") is True` and the type declares `backfill_payload_keys`, those are checked instead — `netuid_reoccupied`), `INSERT … ON CONFLICT DO NOTHING RETURNING id` (None on dedupe hit); `async def mark_alerted(db, event_id, *, payload_patch: dict \| None) -> bool`. Spec 12's `app/scoring/events.py::emit()`, 16's `AlertLatch.fire(event_type=…)` (02 §D3) and every collector call it | — |
| `retention.py` | `RetentionPolicy`, `POLICIES` (§2.7 order), `run_retention`, `RetentionJob(CollectorJob)` | `db.py:622-653` (bucket last-write-wins idiom for tick rollup), `:655-681` (chunked delete loop) |
| `cli.py` | `python -m app db check\|migrate\|stats\|retention [--dry-run] [--only POLICY]\|explain <sql>` wired into 01's `app/__main__.py` argparse as sub-command `db`; `check` = `PRAGMA quick_check` + `foreign_key_check` + orphan report for FK-less `occupant_key` columns + `schema_warnings` | — |

Not in this spec (to avoid two implementations): `app/storage/backup.py` beyond `Database.backup_to` (spec 16 §8.1), `app/backfill/*` import/derivation code (spec 17 §8.1, must satisfy §2.9). Spec 12's `app/features/store.py` may only re-export `upsert_metric_values` from `pit.py`; it never carries its own SQL.

**Step order:**
1. `models.sql` (copy §3 blocks in order) → `migrate.py` → `db.py` (connect/PRAGMAs/lock/routing) → run `python -m app db migrate` on an empty file; `sqlite3 data/taosentinel.db .schema` must list every table in §3.
2. `precedence.py`, `catalog.py`, `pit.py` (writer + readers), `events.py`.
3. `retention.py` + JOB_TABLE row `storage_retention_daily` (§5); until spec 16's `backup.py` exists the backup gate treats "no backup" as closed (destructive policies skipped, `retention_skipped` event).
4. `cli.py`; update 01's `check` to print `schema_version`, `schema_warnings`, `sqlite_version`.
5. Tests (§9), then verify (§10).

---

## 9. Tests (`tests/storage/`, pytest-asyncio strict, temp-dir DBs, no network)

| file | asserts |
|---|---|
| `test_schema.py` | empty file → `migrate()` → `schema_version=1`, `PRAGMA user_version=1`, `auto_vacuum=2` (incremental); every table/view/trigger named in §3 exists (`EXPECTED_TABLES` = 80, views 6, triggers 14); every non-legacy table has `strict=1` in `pragma_table_list`; the first 9 columns of `pool_history`, all 16 of `whale_events` and the first 7 of `whale_cohort` (`PRAGMA table_info`: name, type, notnull, dflt_value, pk) equal the same DDL executed from the old `models.sql` lines into an in-memory DB, and every appended column is nullable; `CODE_SCHEMA_VERSION == 1`; second `migrate()` applies nothing |
| `test_migrate.py` | temp `migrations/0002_x.sql` adding a column applies once; a repeated `ALTER … ADD COLUMN` for an existing column is skipped; a failing statement rolls back and leaves version 1; edited file → `checksum_drift:2` warning, not re-run; DB at v3 with code at v2 → `schema_ahead` warning, boot ok; lint: `DROP TABLE` without `-- @allow-destructive` fails; `BEGIN`/`VACUUM`/`PRAGMA journal_mode`/`ATTACH` rejected; `split_statements` keeps a `CREATE TRIGGER … BEGIN …; END;` as one statement; gap in numbering → `SchemaError` |
| `test_db.py` | PRAGMAs (`journal_mode=wal`, `synchronous=1`, `foreign_keys=1`, `busy_timeout=5000`); `sqlite_version_info` monkeypatched to (3,36,0) → `SchemaError`; STRICT rejects `'2026-09-22T00:00Z'` in `observed_at`; `transaction()` rolls back on exception; **isolation**: task A in a transaction awaits `asyncio.sleep`, task B's `execute` waits and is not rolled back with A; reader does not see A's uncommitted row, A (owner) does; `insert_or_ignore` with an unknown column → `ValueError`; mixed key sets → `ValueError`; `upsert('scores', …)` and `upsert('pool_history', …)` → `AppendOnlyViolation`; `src_rank('sse_rollup') > src_rank('ledger_import')` on both connections; `backup_to` produces a file passing `integrity_check` with the fixture rows, leaves no `*.tmp`, and a monkeypatched `quick_check ≠ ok` deletes the tmp and raises; `stats()` keys present |
| `test_triggers.py` | `UPDATE scores` / `DELETE FROM scores` → `IntegrityError('scores is append-only')`; metric_values (02 §B4): the §2.5 upsert over a stored `ok`/`absent`/`not_applicable` raw row is a no-op (first success wins); over a stored `failed` row a later `ok` replaces it, a later `failed` or an earlier-`observed_at` success is a no-op; any second write of a `norm` row is a no-op and a direct UPDATE of it aborts; a direct UPDATE lowering `observed_at` or changing `stage` aborts; DELETE of an `as_of % 86400 = 0` row aborts, DELETE of a 06:00Z row succeeds; events: payload/severity/alerted_at change before alert ok, any change after `alerted_at` set aborts, delete aborts; pool_history: filling a NULL `moving_price` ok, overwriting a non-NULL one / changing `price` / DELETE abort; post_labels / identity_observations / manual_flags update+delete abort |
| `test_constraints.py` | `occupants` CHECK rejects `occupant_key='64:1'` with `netuid=65`; `subnets.occupant_key` FK enforced; `metric_values`: raw `{"status":"ok"}` with NULL value aborts, raw `failed` with a value aborts, raw `absent` with NULL ok, `status` generated as `none` when `value_json.status` is missing (and a raw `none` row with a value aborts), `value_json.status='bogus'` aborts, `norm` value 1.5 aborts, sources `nlp`, `chain_tmc`, `features`, `backfill_rpc` abort on `metric_values` and `events` while every 02 §B9 value is accepted; `social_posts` `platform='discord'` and `'telegram'` abort; `control_state.value='yes'` aborts; `review_queue` kind `team_candidate` ok, `team_candidates` aborts; `announcements` Discord text with `via_own_server=0` aborts, with `1` ok, `text_purge_after < observed_at` aborts; `manual_notes` `product_status` `'ga'` and `team_disclosure` `'maybe'` abort; `manual_flags` `confirmed owner_control_change` aborts, `cleared` ok; `team_members` verified with `evidence_count=1` aborts; `watch_channels` duplicate `(NULL, 'rss', url)` ignored by `INSERT OR IGNORE`; `review_queue` second OPEN row with the same `dedupe_key` aborts, after resolving the first it succeeds |
| `test_pit.py` | `put_metric_values`: unknown key → `ValueError`, retired key (`owner_lock_disabled_90d`, `discord_last_announcement_days`) → `ValueError` naming the replacement, raw row without `value_json.status` → `ValueError`, norm row for a leg key → `ValueError`; `metric_at` (02 §B5) returns the greatest usable `as_of` with `observed_at ≤ t` inside `freshness_s` (inclusive); a newer `failed` row is skipped in favour of an older fresh `ok` row; when every candidate failed the newest failed row is returned; a row older than the window or observed after `t` is never returned; honours `official_only`, `strict_backfill`; after a `failed → ok` upgrade at `t2`, `metric_at(t)` for `t < t2` returns what it returned before the upgrade; `cross_section` returns all occupants for the `as_of`; `latest_scores` returns one row per occupant and filters `pre_launch`/`insufficient_data` by default |
| `test_events.py` | `emit_event` with an unknown type (e.g. the retired `owner_sell`, `spend_soft_cap`) → `ValueError`; missing required payload key → `ValueError`, extra keys accepted; `netuid_reoccupied` with `payload.backfilled=true` validates against the backfill key set, without it against 04's set; severity defaults from catalog; duplicate `(event_type, dedupe_key)` → returns `None`, one row; `dedupe_key=None` never deduplicates; `alerted_at` passed at insert (16 direct rows) is stored; `mark_alerted` sets once, second call → False |
| `test_precedence.py` | `chain_hourly`: `sse` overwrites `analytics_1w`; `analytics_1w` does not overwrite `sse`; same-rank `sse` with a later `block_number` overwrites, earlier does not; `price_bars`: `sse_rollup` beats `ledger_import` beats `tmc_candle` beats `onfinality`; a same-rank bar replaces the stored one only when `excluded.ts_last > stored.ts_last` (02 §J1), never for two `tmc_candle` rows; a lower rank never overwrites but its non-NULL `tao_in_pool` fills a NULL via `COALESCE` enrichment; `chain_daily`: `analytics_1y` beats `analytics_1w`, never the reverse, a same-rank row is never overwritten and a pre-set `occupant_key` survives |
| `test_retention.py` | fixture: ticks 40 d old for 2 netuids without `chain_hourly` → `chain_prune` writes `tick_rollup` hourly rows + `sse_rollup` 5m bars, then deletes only ticks older than 30 d; second run → 0 rows; `latest_backup_age_hours` monkeypatched to 40 h (or `ImportError`) → destructive policies skipped, `retention_skipped` event, privacy policies still ran; disk-emergency monkeypatch → destructive runs; `nlp_text_retention` (02 §B10): labelled and unlabelled 91-d X posts → text NULL, `text_hash` intact; 91-d own-server announcement → text NULL; 89-d post → kept; a pending Telegram post (`text_purge_after` = observed + 7 d) → NULL once a `post_labels` row with `post_id = msg_id` exists, or after 7 d unlabelled, and kept on day 3 unlabelled; `web_text` keeps the newest text per URL; `--dry-run` writes nothing and returns counts; official `metric_values` (both stages) untouched, raw provisional > 90 d deleted, norm provisional untouched (spec 12 prunes those) |
| `test_legacy_compat.py` | a fixture old ledger created from the old DDL (`models.sql:55-73`) → `ATTACH … INSERT OR IGNORE INTO pool_history (<9 old columns>) SELECT …` inserts all rows, re-run inserts 0; the spec 17 import statement of §2.9 with `:trust` drops `network_registered_at` before the boundary and stamps `occupant_key` after it, and with `:live_from` sets `observed_at = timestamp` only for live-captured rows (import start otherwise); `price <= 0` rows are skipped; the NULL-only merge `UPDATE … SET moving_price = COALESCE(moving_price, :v), enriched_at = …` succeeds and a second merge with a different value is a no-op; `whale_cohort` accepts the 200-row `whale_cohort.json` shape with `as_of_block=8590789` |
| `test_catalog.py` | every master §7.3 metric_key present with the master weight and normaliser; per-pillar weights sum to 1.00 (P1–P6); P4 = team_doxxed .25, owner_identity .05, owner_commitment .20, owner_retention_30d .20, control_stability_90d .15, owner_overhang .05, tenure .10; `len(METRIC_KEYS) == 79` and no name of `RETIRED_METRIC_KEYS` is a key; every key of spec 12's `METRICS` (when importable) is in `METRIC_KEYS` with the same pillar/weight/source; every `MetricSpec.source` ∈ `SOURCES`; every §3.9 event_type has severity ∈ {info, warning, critical} and alert ∈ {yes, digest, no, direct}; the D4 detection events (`emission_disabled`, `emission_reenabled`, `dereg_risk_entered`, `dereg_proximity`, `site_down_7d`, `site_recovered`, `owner_extraction_detected`, `owner_perpetual_off`, `owner_control_change`) have alert `digest`; `RED_FLAGS` has exactly the 12 master §7.5 flags with their multipliers |

`tests/conftest.py` (01) gains a `db` fixture: `Database(tmp_path/'t.db')`, `connect()`, `apply_schema()`, yield, `close()`.

---

## 10. Verify checklist (laptop; Phase 0 storage is done when all pass)

```bash
cd ~/taosentinel && source .venv/bin/activate
python -c "import sqlite3; print(sqlite3.sqlite_version)"          # >= 3.37.0 (Ubuntu 22.04: 3.37.2, 24.04: 3.45.x)
sqlite3 --version                                                    # CLI also >= 3.37 (needed to read STRICT tables)
python -m app db migrate          # -> "schema_version=1 applied=[1] warnings=[]"
python -m app db migrate          # -> "schema_version=1 applied=[] warnings=[]"
sqlite3 data/taosentinel.db "PRAGMA journal_mode; PRAGMA user_version; PRAGMA auto_vacuum;"   # wal | 1 | 2
sqlite3 data/taosentinel.db "select count(*) from sqlite_schema where type='table' and name not like 'sqlite_%'"   # 80
sqlite3 data/taosentinel.db "select count(*) from sqlite_schema where type='view'"      # 6
sqlite3 data/taosentinel.db "select count(*) from sqlite_schema where type='trigger'"   # 14
sqlite3 data/taosentinel.db "select name from pragma_table_list where schema='main' and strict=0 and type='table' and name not like 'sqlite_%'"
#   -> pool_history, whale_cohort, whale_events
sqlite3 data/taosentinel.db "insert into collector_runs(run_id,job,started_at) values('x','t','2026-09-22T00:00Z')"
#   -> Runtime error: cannot store TEXT value in INTEGER column collector_runs.started_at
sqlite3 data/taosentinel.db "insert into occupants(occupant_key,netuid,network_registered_at,status,first_seen_block,first_seen_at,last_seen_block,last_seen_at) values('64:1',65,1,'active',1,0,1,0)"
#   -> CHECK constraint failed
pytest tests/storage -q                                             # all green
python -m app check | grep -E "schema_version=1|storage_retention_daily"   # schema_version=1; job listed, available, phase 0
python -m app db retention --dry-run                                 # JSON {policy: rows} with zeros on a fresh DB
python -m app db stats                                               # size_bytes, wal_bytes, schema_version 1, journal_mode wal
python -m app db check                                               # quick_check ok, foreign_key_check empty, 0 orphans
# backup primitive (spec 16's pipeline, once built, wraps it; `python -m app backup now` then `backup verify <stamp>`):
python -c "import asyncio; from app.storage.db import Database; from pathlib import Path
async def m():
    d = Database(Path('data/taosentinel.db')); await d.connect(); print(await d.backup_to('/tmp/ts_verify.db')); await d.close()
asyncio.run(m())"                                                    # {'path': '/tmp/ts_verify.db', 'size_bytes': …, 'integrity': 'ok'}
# legacy compatibility on a SCRATCH database (after checklist A3 copied the old ledger to seed/):
DB_PATH=/tmp/ts_legacy.db python -m app db migrate
sqlite3 /tmp/ts_legacy.db "ATTACH 'file:seed/ledger.db?mode=ro&immutable=1' AS old; INSERT OR IGNORE INTO pool_history (netuid,block_number,timestamp,price,tao_in_pool,alpha_in_pool,moving_price,network_registered_at,subnet_volume) SELECT netuid,block_number,timestamp,price,tao_in_pool,alpha_in_pool,moving_price,network_registered_at,subnet_volume FROM old.pool_history LIMIT 1000; SELECT changes();"   # 1000
sqlite3 /tmp/ts_legacy.db "UPDATE pool_history SET price = price + 1 WHERE rowid = 1"   # -> pool_history: only NULL-only enrichment is allowed
rm -f /tmp/ts_legacy.db* /tmp/ts_verify.db
```

Restore runbook: owned by spec 16 (§2.8 `python -m app backup restore <stamp>`, §7.3). Storage-side facts it relies on: a restored snapshot older than `CODE_SCHEMA_VERSION` is migrated forward at boot; a newer one is refused; after restore, collectors re-observe on their next tick, spec 12's catch-up re-scores missed official slots in reconstructed mode (`backfilled=1`), and the SSE recorder repairs hourly gaps from TMC `?span=1W` (spec 05).

---

## 11. Open questions / operator decisions

1. **Disk budget — resolved by 02 §I1 (amended).** ≥ 60 GB free is **required** (peak ≈ 4.7 × DB + 5 GB during backup/VACUUM). By §6's (UNVERIFIED) estimates the DB is ≈ 9 GB at month 12 on stride 5; smaller disks work only with lower tick retention, a smaller backup budget and a USB/NAS mirror. `LEDGER_IMPORT_THIN_SEC=300` (−0.85 GB) and `METRIC_PROVISIONAL_RETENTION_DAYS=30` (−0.9 GB) remain the operator's levers.
2. **Tick volume — resolved by 02 §F2.** `CHAIN_TICKS_RETENTION_DAYS=30`, `CHAIN_TICKS_BLOCK_STRIDE=5` (≈ 0.84 GB window; ≈ 4.2 GB at stride 1). Further recommendation (disagrees with master §1.3 "one SQLite file", **not implemented**): move `chain_ticks` to an attached `data/ticks.db` so `VACUUM main INTO` excludes it.
3. **Discord/Telegram text — resolved by 02 §B10 (amended) and §B11.** TaoSentinel never reads third-party servers. `announcements.text` exists only for own-aggregator-server crossposts (`via_own_server=1`, user ids redacted) and Telegram posts, enforced by a CHECK. With the platform's `*_STORE_TEXT` flag true it is kept 90 d; with it false it is pending-classification text (`text_purge_after`, `LLM_PENDING_TEXT_MAX_DAYS` = 7 d) NULLed once labelled or at the purge time. The earlier draft of this spec (no text column) is withdrawn.
4. **`social_posts` engagement refresh — resolved by 02 §J2.** Accepted as `state` class; 13 flags `x_engagement_30d` as look-ahead. Strict PIT would need a `social_post_engagement` snapshot table (~10× rows).
5. **`metric_values` semantics — resolved by 02 §B3–§B5.** Key `(occupant_key, metric_key, as_of, spec_version, stage)`; `status` generated from `value_json.status` (`none` if missing); no `is_provisional`, no `metric_norm`. Raw rows: first success wins, only a `failed` row may be upgraded by a later success; norm rows insert-once; `metric_at` skips failed rows inside the freshness window. The single writer is this spec's `pit.py::put_metric_values` (alias `upsert_metric_values`). This supersedes both this spec's earlier "latest observation wins" text and 06's assumption that `run_id` is part of the key.
6. **Provisional retention.** Raw provisional rows (05/11's 06/12/18:00Z slots and 12's hourly `features_hourly` rows) are kept 90 d (`METRIC_PROVISIONAL_RETENTION_DAYS`), norm provisional rows 14 d (spec 12). Spec 13's 4-h live scope therefore has intraday factor history for 14 d only; raise `METRIC_NORM_PROVISIONAL_RETENTION_DAYS` (≈ +75 MB per extra 14 d) if 13 needs more. Operator decision.
7. **Severity collapse — resolved by 02 §B2/§D4.** `warn → warning`, `high → critical`; routing is the §3.9 alert column (detection events that duplicate a red flag are digest-only), so the collapse no longer pages twice. 14's dispatcher still rate-limits per `(event_type, occupant_key)`.
8. **Pause storage — resolved by 02 §B12 (amended).** `control_state` is the only persistent pause store (column `value`); targets are exact job ids or 16's `PAUSE_GROUPS` names, never prefixes; `data/control/<target>.paused` files are a read-only emergency input; `job_state` holds watermarks only.
9. **Runtime conditions as events — resolved by 02 §D3.** `collector_failing`, `spend_*`, `backup_*`, `disk_*`, `db_integrity_failed`, `service_recovered_unclean`, `schema_warning` are catalogued with 16's names and 02 §D2's payloads; `AlertLatch.fire(event_type=…)` inserts the row and falls back to a direct `send_alert` only if the DB write fails (this fallback is the only direct send, including for `disk_critical`/`db_integrity_failed`).
10. **Units.** `tao_liquidity`, `subnet_tao`, `alpha_liquidity`, `owner_personal_*`, `total_*` and `fdv` are rao-scaled and `marketcap`/`volume` are τ in the research probe dumps of 2026-09-22 (SN51/SN64 values in §3.3 comments), so the `v_quant_*` views convert with `/ 1e9` in v1; 05's live units probe re-confirms and a mismatch is a view-redefining migration. The table's `volume` window and `neuron_regs_burned_24h` unit remain UNVERIFIED. `pool_history.subnet_volume` unit (τ vs α) is UNVERIFIED (17 §11).
11. **TMC table field list** — resolved: all 44 names are named columns of `chain_hourly` (verified against the research probe's `/subnets/table/` and SSE `full` dumps, identical key sets). `extras_json` only catches fields TMC adds later (05's `tmc_schema_drift`); the earlier draft's `market_cap` was wrong (TMC delivers `marketcap`). `owner_cut_per_block`/`subnet_owner_hotkey` may only be on the per-subnet SSE/detail payload (research 02-gapfill-owner §2b), which is why 11 stores them in `owner_snapshots`.
12. **Job count — resolved by 02 §E2.** This spec adds exactly one declared, phase-0-available job (`storage_retention_daily`); 01 recomputes its verify strings from the final `JOB_TABLE`.
13. **Red-flag naming — resolved by 02 §B6/§D4.** Collector detection events keep their names and are digest-only when they duplicate a flag; gate engagement and the immediate page are only `red_flag_raised`/`red_flag_cleared` from spec 12's state machine. `scores.red_flags` is a sorted array of flag **names**; multiplier/since/detail come from `explain.flags`.
14. **`team_members` status edits in place** are not PIT-exact; acceptable because `team_doxxed` is forward-only (master §10), and 11's `team_member_*` events are the audit trail.
15. **`pool_history` observed time (aligned with 17 §2.1/§2.5).** Ledger rows captured live (`timestamp ≥ LEDGER_LIVE_FROM_TS` = 1777468595) carry `observed_at = timestamp`; the 41,105 earlier rows carry the import time. Values filled later by NULL-only merges carry `enriched_at` (occupant stamping does not). Prices are labels/decision prices; enriched `moving_price` is reconstructed chain state — spec 13 records this in provenance, and only live, unenriched bars keep `backfilled = 0`.
16. **Backup/retention schedule — resolved by 02 §E3.** Retention at 03:00Z (`STORAGE_RETENTION_CRON_MINUTE=0`), `backup_daily` at 03:30Z waiting ≤ 20 min via `DB_BACKUP_WAIT_FOR_JOBS=storage_retention_daily`; `chain_prune` is a policy, not a job (runbook: `python -m app db retention --only chain_prune`).
17. **`owner_position_hourly` growth** (≈ 1.1 M rows/yr): kept forever in v1; a 400-day rollup to daily (11 §6 suggestion) is a later additive migration if disk becomes tight.
18. **Sibling inconsistencies found while reconciling — resolved by 02.** P4 aux keys: canonical `owner_extraction_trigger`/`owner_control_change_trigger`; `owner_lock_disabled_90d`/`owner_control_change_30d` are retired (02 §C2). P5 legs: canonical `discord_last_activity_days`/`tg_last_activity_days`; `discord_last_announcement_days`/`tg_last_post_days` are retired (02 §C3). `regime_daily`: `day_ts` and column `label` (02 §B8; 12/13/17 wrote `day`/`market_regime`). 11's `change_type` list gains `subnet_owner_changed` and `coldkey_swap_announced` (§3.5).
19. **11's §11 Q2 (a)–(j) — decided here** (11 defers these DDL choices to 03): (a)/(b)/(c) 11's wide `owner_snapshots`, `owner_stake_events` and `change_type` list are adopted; (d) whitelist time windows with 11's column names `reason`, `owner_change_id`; (e) 11's `team_members` columns and sources; (f) `review_queue` kinds `team_candidate`, `team_reverify`, `team_delisted`, `owner_extraction_candidate`, `owner_change_whitelist`, `owner_source_disagreement` (+ 10's `llm_flag_candidate`), columns `ref_table`/`ref_id`, `dedupe_base`, `priority`, `resolution_action`; (g) `manual_notes.kind='team_disclosure'`; (h) events per §3.9; (i)/(j) metric keys per 02 §C1/§C2 (11 writes `tenure`).

---

## 12. Sources

- `E:\TaoData\specs\02-cross-cutting-conventions.md` (binding ledger, read 2026-09-23 incl. the amended §B10/§B11/§B12): §A2, §B1–§B12, §C1–§C7, §D1–§D5, §E2–§E3, §F2, §G1, §G5, §H2–§H3, §I1, §J1–§J2.
- `E:\TaoData\specs\00-MASTER-SPEC.md` §1.3, §2, §5, §6, §7.1–§7.7, §9, §10, §11, §12.
- `E:\TaoData\specs\INDEX.md`; `E:\TaoData\specs\18-operator-checklist.md` (A1 disk, A3 seed paths, B6 LLM cap, D1–D5 manual data).
- Sibling data contracts: `01-repo-skeleton-runtime.md` §3, §5, §2.7; `04-subnet-registry-and-identity.md` §3, §4, §2.7; `05-chain-collector.md` §2.1–§2.2, §3, §4, §11; `06-github-collector.md` §3, §4, §11; `07-web-presence-collector.md` §3, §4, §11; `09-x-collector-and-kol.md` §3, §4, §11, Appendix A; `10-llm-classification.md` §2.5, §3, §4, §11; `13-validation-harness.md` §3, §4, §11; `14-api-telegram-and-ui.md` §3, §11; `15-trading-bot-integration.md` §3, §11; `08-community-collector.md` §2.11, §3.1–§3.5, §5.2; `11-team-and-integrity.md` §2.9, §3.1–§3.4, §11; `12-feature-store-and-scoring-engine.md` §2.2–§2.4, §2.8, §2.10–§2.12, §3, §5, §6, §11; `16-ops-deploy-and-observability.md` §2.7, §2.8, §2.11, §2.13, §3, §5.1, §6.1, §11; `17-data-migration-and-backfill.md` §2.1, §2.2, §2.5, §3.1–§3.4.
- Research: `01-data-layer-and-schema.md` (all: old schema, ledger ranges, WAL/VACUUM gotchas, pool_history shape and gaps); `02-gapfill-keyless-onchain-pillar-source.md` §1–§2 (TMC endpoints, 44-field table, field semantics, immunity 864,000); `02-gapfill-github-canonical-repo-resolution.md` §7 (repo-id keying, aliases, org enumeration, events); `02-gapfill-discord-telegram-topology.md` "Metrics enabled" (invite/widget/t.me fields, no-usernames posture); `02-gapfill-owner-extraction-mechanics.md` §2b (TMC owner fields), §3, §5 (S1–S5 inputs); `01-web-quant-scoring-methods.md` §2 (point-in-time paragraph).
- Old bot (line numbers re-verified 2026-09-23): `E:\SN_Trader\SubnetTrader\app\storage\models.sql` (224 lines; `pool_history` 55-70 + index 72-73, `pool_history_hourly`/union 75-131, whale 136-167); `app\storage\db.py` (975 lines): `:10` static settings import (the hot-reload bug), `:25-31` connect/WAL, `:38-80` `backup_to`, `:82-119` blind ALTER tuple, `:121-134` execute/fetch, `:542-589` `insert_pool_history` (`_opt_float`/`_opt_int` 553-563; it does **not** filter `price <= 0`), `:622-653` rollup idiom, `:655-681` chunked prune; `app\storage\backup.py` (all **97** lines; mirror block 72-85, the `mkdir` spec 16 removes at 77); `app\data\chain_client.py:450` (the `price > 0` writer filter); `app\backtest\dereg_analysis.py:45` (`RESET_RATIO = 0.30`), `:64-96`; `app\backtest\pool_history_loader.py:167`; `app\main.py:1294-1331` (`run_db_backup`).

---

## Appendix A — Name reconciliation (their name → canonical name)

**Global rules applied to every sibling:** all TEXT ISO timestamps → `INTEGER` unix seconds (same column name unless listed; 02 §B1); `collector_runs(started, finished)` → `started_at`, `finished_at`; severity `warn` → `warning`, `high` → `critical` (02 §B2); `dedup_key` → `dedupe_key` (a column, never inside `payload`); day/hour bucket columns → `day_ts` / `hour_ts`; `published_at` → `event_at`; `raw_json` on high-volume TMC rows → `extras_json` (only fields without a named column); `metric_values.source`/`events.source` values → the 02 §B9 vocabulary (`chain_tmc`/`chain_rpc` → `chain`, `features` → `scoring`, `manual` → `api`, `owner`/`team` → `integrity`, `nlp` → `llm`, `backfill_*` → the producer's source + `backfilled`/`payload.backfilled`).

| spec | their name | canonical |
|---|---|---|
| 01 | `schema_version(version, applied_at TEXT)` | `schema_version(version PK, name, checksum, applied_at INTEGER)` (runner migrates the old shape) |
| 01 | `collector_runs.started_at/finished_at` TEXT | INTEGER; + `meta_json` (04) |
| 01 | `job_state(job, 'paused')` | `control_state(target=job, key='paused', value='true', …)`; `job_state` keeps watermarks/`last_ok` (02 §B12) |
| 01 | `provider_spend(… updated_at TEXT)` | INTEGER `updated_at`; + `units_json` (09) |
| 01 | `Database.execute → cursor`; `apply_schema` = executescript + ALTER tuple | `ExecResult`; `apply_schema` = `migrate()` |
| 01 | JOB_TABLE 19 jobs | + `storage_retention_daily` (cron 03:00Z, phase 0, `ops_exempt`) — §11 Q12 for totals with 16's jobs |
| 04 | all `*_at` TEXT (occupants, identity_*, manual_overrides, watch_channels, subnets) | INTEGER |
| 04 | `identity_observations.block` | `block_number` |
| 04 | `subnets` (8 columns) | + `network_registered_at, immune, owner_hotkey, website_url, github_url, docs_url, discord_url, telegram_url, x_handle` (14) |
| 04 | `occupants` | + `source`; `registered_at_est` INTEGER |
| 04 | `manual_notes(text, …)` | `manual_notes.body`; `ref_event_id` added for reoccupied-review notes |
| 04 | `UNIQUE(occupant_key, field, mode, source, value_hash, suppress_source, suppress_value_hash)` | expression unique index with `COALESCE(…,'')` (NULL-safe) |
| 04 | `watch_channels UNIQUE(occupant_key, platform, handle)`; platform list | `ux_watch_channels` on `COALESCE(occupant_key,'')` (09's NULL-occupant feeds); platforms + `medium, substack, google_news`; + `netuid, priority, label, filtered`, generated `active`; sources + `tmc, web_autodiscover, seed_feeds` |
| 05 | `ix_chain_ticks_event` | dropped (retention walks the PK; saves ~0.6 GB) |
| 05 | `chain_ticks` (no run_id) | + `run_id` |
| 05 | `chain_hourly.raw_json` (full 44-field row); `market_cap` | all 44 TMC fields are named columns (`marketcap`, `marketcap_rank`, `fdv`, `circulating_supply`, `volume`, `owner_personal_stake`, `miners_tao_per_day`, `neuron_regs_burned_24h`, `price_difference_*`, `logo_url`, `subnet_owner`, …); `extras_json` holds only fields TMC adds later; source + `tick_rollup` |
| 05 | `chain_daily` | + `run_id`, `backfilled`, `daily_inflation`, `extras_json` |
| 05 | `rpc_constants` | merged into `chain_constants` (`source='rpc_storage'`, `endpoint`) |
| 05 | `provider_usage(provider, day, requests)` | folded into `provider_spend.requests` |
| 05 | `price_bars` | + `ts_last, tao_in_pool, alpha_in_pool, moving_price, block_number, event_at, run_id`; `close NOT NULL CHECK > 0` |
| 05 | job `chain_prune`; `CHAIN_TICKS_RETENTION_DAYS=14` | retention policy `chain_prune` in `storage_retention_daily` (02 §E3); default 30, stride 5 (02 §F2) |
| 05 | `metric_values.source` `chain_tmc` / `chain_rpc`; `INSERT OR REPLACE` metric writes | `chain` (02 §B9); `put_metric_values` only (02 §B4) |
| 06 | `identity_observations.field='github'` | `github_repo` (04 vocabulary) |
| 06 | `identity_observations.is_current` | view `v_identity_current` |
| 06 | `github_activity_daily.day`, `github_authors_daily.day` TEXT | `day_ts` INTEGER |
| 06 | `github_repos.size` | `size_kb`; all `*_at` INTEGER |
| 06 | `github_events.payload` | `payload_json`; + `source` |
| 06 | `metric_values` key incl. `run_id` | PK `(occupant_key, metric_key, as_of, spec_version, stage)`; first success wins (02 §B3/§B4) |
| 06 | `source_disagreement` payload `{field:"github", sources{…}, chosen_source}` | 04 shape `{netuid, occupant_key, field:"github_repo", groups[], resolved_source, pair_hash}` + `repo_ids` |
| 07 | `web_checks.checked_at`, `check_day` TEXT | `event_at`, `check_day_ts` INTEGER; `tls_not_after`, `wayback_*` INTEGER |
| 07 | `watch_channels.kind` / `active` / `created_at` / source `chain_identity` | `platform` / generated `active` / `first_seen_at` / `chain` |
| 07 | `manual_overrides(field='product_status', set_by, set_at, cleared_at)` | `manual_notes(kind='product_status', value, evidence_url, created_by, created_at)`; clear = new row with `value NULL` |
| 07 | dedupe via `json_extract(payload,'$.dedupe_key')` | `events.dedupe_key` column + `ux_events_dedupe` |
| 09 | `watch_channels.kind` ('x','youtube','medium','substack','rss','google_news') | `platform` (same values); `occupant_key` nullable; `filtered`, `label` added |
| 09 | `social_posts.created_at`-style ISO columns | INTEGER; `engagement_observed_at` INTEGER |
| 09 | `x_account_snapshots.created_at` | `account_created_at` |
| 09 | `news_items.published_at`, `video_items.published_at` | `event_at`; `news_items.text_hash` added |
| 09 | `provider_spend.calls` | `requests` |
| 10 | `social_posts.post_id` PK `'x:…'` | PK `(platform, post_id)` + generated `item_id = platform:post_id` |
| 10 | `social_posts.kol_id` / `author_kind` / `spam_flag` / `likes, reposts, replies, views` | `kol_handle` / generated `author_kind` / `bot_filtered` / `like_count, repost_count, reply_count, view_count` |
| 10 | `social_posts.via_own_server` (discord rows) | Discord rows live only in `announcements` (`via_own_server`, text only when 1) |
| 10 | `announcements.text` | kept, CHECK `text IS NULL OR via_own_server = 1 OR platform = 'telegram'`; NULLed at 90 d (flag true) or once labelled / at `text_purge_after` (pending text, flag false) — 02 §B10 (amended), §11 Q3 |
| 10 | `post_labels.post_id = '{item_kind}:{item_id}'` (`social_post:x:…`, `announcement:discord:…`) | the item id itself for posts: `x:{id}`, `discord:{source_message_id}`, `telegram:{handle}/{n}` (02 §B11); `news_item:`/`video_item:` prefixes kept for the other kinds |
| 10 | job `nlp_text_retention` "90 d Telegram text" allowance | pending-classification rule (02 §B10 amended) |
| 10 | `news_items.item_id`, `feed`, `occupant_key` | `url_hash`, `feed_url`, `occupant_keys_json` |
| 10 | `kol_accounts.kol_id` | `handle` (PK); + `platform`, `author_id` |
| 10 | `chain_daily.tmc_trending_views`, `chain_daily.day` | `chain_trending_daily.views` (0 vs no-row rule), `day_ts` |
| 10 | `events.dedup_key`, severity `warn` | `dedupe_key`, `warning`; `events.source='llm'` |
| 10 | `manual_notes.author` | `created_by` |
| 10 | `review_queue UNIQUE(kind, post_id, label_version)` | `dedupe_key TEXT UNIQUE` (`"{kind}\|{post_id}\|{label_version}"`) |
| 10 | job `nlp_text_retention` | retention policy of that id |
| 10 | `post_labels` times TEXT | INTEGER; + `source` |
| 13 | `price_bars.bucket_start` | `ts` (served as `bucket_start` by `v_quant_price_bars`) |
| 13 | `chain_hourly.hour, tao_in_pool, moving_price, volume` | `v_quant_chain_hourly` (over `hour_ts, tao_liquidity, subnet_moving_price, volume_24h`) |
| 13 | `chain_daily.day, ts, emission_share, tao_in_pool` | `v_quant_chain_daily` (over `day_ts, event_at, emission_percent, subnet_tao/1e9`; since migration 0005 also `rpc_subnet_daily` day rows: `as_of, event_at, emission_share`) |
| 13 | `occupants.registered_ts` | column `registered_ts` (exact, 17) + `registered_at_est` (04 estimate); `v_quant_occupants.registered_ts` = COALESCE of both |
| 13 | `scores.computed_at` | `computed_at` (kept; 12's name) |
| 13 | `regime_daily.day`, `market_regime` | `day_ts`, `label` (+ `v_w`, `inputs_json`; 02 §B8, §J2) |
| 14 | `scores.as_of` ISO | INTEGER (API formats ISO) |
| 14 | `scores.red_flags` `[{flag, multiplier, since}]` | sorted array of flag names (12, 15); multiplier/since/detail from `explain.flags` |
| 14 | `scores.observed_at` | `computed_at` |
| 14 | `team_members.source ∈ {manual, seed_subnetalpha, seed_research}` | 11's `{subnetalpha, legacy, manual, llm}` (+ `created_by` = seed_subnetalpha, seed_legacy, cli, api, telegram) |
| 14 | `manual_notes.note` | `body` |
| 14 | `team_members.verified` (stored) | generated from `status='verified'` |
| 14 | `data/control/<collector>.paused` files | `control_state` rows; the files remain a read-only emergency input (02 §B12) |
| 14 | `events.source = 'manual'` | `api` (02 §B9) |
| 14 | `chain_ticks.block, ts` | `block_number`, `event_at` |
| 14 | backup glob `taosentinel-*.db` | spec 16's `latest_backup_age_hours` (manifest `taosentinel-*.json`) |
| 15 | `scores.red_flags` as name strings | same (canonical) |
| 15 | `collector_runs(started, finished)` INTEGER | `started_at`, `finished_at` INTEGER |
| 08 | all `*_at`, `cycle_at`, `expires_at`, `guild_created_at`, `last_post_at`, `deleted_at` TEXT | INTEGER unix s |
| 08 | `discord_snapshots.features`, `declared_by` | `features_json`, `declared_by_json`; + `source` (default `discord_api`) |
| 08 | `telegram_snapshots.cycle_day` 'YYYY-MM-DD' | `cycle_day_ts` INTEGER; + `source` (default `tme_page`) |
| 08 | `announcements` (no `source`) | + generated `source` (`discord_gateway` / `tme_preview`); earlier-draft columns `attribution, is_crosspost, has_url, n_attachments` → 08's `mapping, url_count, has_attachment` |
| 08 / 12 / 03 drafts | metric legs `discord_last_announcement_days`, `tg_last_post_days` | `discord_last_activity_days`, `tg_last_activity_days` (08's names, 02 §C3; the old names are retired and rejected by `put_metric_values`) |
| 08 | events `invite_expiring`/`guild_changed`/`community_inflation_suspected`/`tg_handle_gone` (earlier 03 draft) | 08's `invite_expired` (stage), `community_venue_changed`, `inflated_community`, `silence_45d`, `invite_dead`; severity `warn` → `warning` |
| 11 | reference-table times ISO TEXT (`team_members`, `owner_event_whitelist`, `review_queue`) | INTEGER unix s |
| 11 | `team_members` (03 draft: `second_evidence_url, departed_at, note, updated_at`, status `departed`) | 11's columns (`evidence_json/evidence_count`, `rejected_*`, `name_norm` UNIQUE); status `candidate`/`verified`/`rejected` |
| 11 | `owner_snapshots` long form (03 draft: `source, sample_ts, …`) | 11's wide per-run row, PK `(occupant_key, observed_at)`; + `event_at`, `source` |
| 11 | `owner_position_hourly` (new) | adopted; + `run_id` |
| 11 | `owner_stake_events` (03 draft: `source_event_id, coldkey, direction, alpha_amount, tao_amount`) | 11's `event_uid, owner_coldkey, kind, counts_as_outflow, alpha, tao`; PK `(event_uid, occupant_key)`; source list + `rpc_events` |
| 11 | `owner_changes.change_kind`, `source` (03 draft) | `change_type` (+ `subnet_owner_changed`, `coldkey_swap_announced`), `detected_via` (+ generated alias `source`), + `extrinsic_id` |
| 11 | `owner_event_whitelist` (03 drafts: `change_kind, block_from/to`; later `kind`, `change_id`) | 11's `reason, applies_to, window_start/end, old/new_coldkey, owner_change_id` (§11 Q19) |
| 11 | `review_queue` (03 draft kinds `team_candidates`, `owner_control_change`, `owner_identity_churn`, `owner_extraction`, `owner_lock_reduced`; columns `source_table/source_id`) | 11's kinds `team_candidate`, `team_reverify`, `team_delisted`, `owner_extraction_candidate`, `owner_change_whitelist`, `owner_source_disagreement`; `ref_table/ref_id`; + `dedupe_base`; UNIQUE `(kind, post_id, label_version)` and open-only `dedupe_key` kept as partial unique indexes (§11 Q19) |
| 11 / 12 / 03 drafts | aux keys `owner_lock_disabled_90d`, `owner_control_change_30d`; evidence `owner_max_sell_impact_7d` | `owner_extraction_trigger`, `owner_control_change_trigger`; `owner_sell_impact_7d` (02 §C2; `identity_stability`, `owner_conviction`, `owner_retention` also retired) |
| 11 | P4 `value` = raw inputs (V, lock_ratio, sell_ratio, #changes, dump_impact); `tenure` left to 12 | final [0,1] scores in `value`, inputs in `value_json` (12 §2.4 keys); 11 writes `tenure` as raw days (02 §C1) |
| 11 | `metric_values.source` / `events.source` = `owner` / `team` | `integrity` (02 §B9) |
| 11 | events `owner_sell`, `owner_full_exit_call`, `owner_perpetual_lock_disabled`, `owner_coldkey_swap`, `owner_cut_enabled_changed`; 03 draft `owner_self_dealing_suspected` for S4 v1 | `owner_extraction_detected` (`reasons[]` ⊇ `full_exit_extrinsic` for D4b), `owner_perpetual_off`, `owner_control_change`, `owner_cut_toggled` (02 §D1); S4 v1 = `owner_concentration` (`owner_self_dealing_suspected` is ts-1.1 only); `owner_coldkey_swap_announced` added (§3.9) |
| 11 | `chain_hourly.raw_json` → `subnet_owner`, `owner_cut_per_block` | named column `subnet_owner`; other fields in `extras_json` |
| 12 | `metric_values.status` written as a column; `is_provisional` column | generated `status` = `value_json.status` (`none` when missing); provisional ⇔ `as_of % 86400 <> 0` (02 §B3) |
| 12 | `metric_norm` table (`m`, `raw`, `raw_status`, …) | `metric_values` rows with `stage='norm'`: `value` = m, `value_json.{raw, status, reason, raw_as_of, raw_observed_at, age_s, src, fallback, cohort_n}`, `observed_at` = run's `computed_at` (02 §B3) |
| 12 | writer `app/features/store.py::upsert_metric_values` | `app/storage/pit.py::put_metric_values`; `upsert_metric_values` is an alias (02 §B4) |
| 12 | `metric_values.source = 'features'` | `scoring` (02 §B9) |
| 12 | `metric_values_v2` rebuild migration; `ALTER TABLE` statements for 03's tables | not needed (v1 has `stage` and every column; 02 §B7) |
| 12 | `red_flag_state` (03 draft: `state, origin, clear_streak_days, trigger_event_id`) | 12's `active, human, sticky, false_since, suppress_until, last_eval…` |
| 12 | `regime_daily.day`, `market_regime`, `observed_at` | `day_ts`, `label`, `computed_at`; + 17's `source`, `run_id`; `inputs_json` kept (02 §B6, §B8) |
| 12 | `rank_stability_daily` (03 draft: `day_ts, series quality/velocity, quantile_turnover, n_names`) | 12's `as_of, series health/composite_smoothed, lag_days, quintile_turnover, top20_turnover, n` |
| 12 | `score_runs` (new) | adopted |
| 12 | indexes `ix_mv_key_stage_asof`, `ix_mv_stage_asof`, `ix_mv_observed`, `ix_scores_spec_asof`; trigger message `'scores are immutable'` | `ix_metric_values_xsec`, `ix_metric_values_slot`, `ix_metric_values_obs`, `ix_scores_asof`; `'scores is append-only'` (same trigger names) |
| 12 | `scores` (03 draft: `observed_at`, `classifier_version`, red_flags objects) | 12's `computed_at, input_cutoff, run_id NOT NULL, flag_multiplier, pillar_coverage, prev_tier, shadow`; red_flags names |
| 12 | `scores UNIQUE(occupant_key, spec_version, as_of, run_kind)` | `UNIQUE(occupant_key, as_of, spec_version)` (run_kind is a function of the slot) |
| 12 | `metric_values.source` `nlp` (10 fn rows) | `llm` (02 §B9); `integrity` (11) kept |
| 16 | `provider_spend.updated_at` TEXT, ops rows ISO | INTEGER |
| 16 | `DB_BACKUP_WAIT_FOR_JOBS="chain_prune"` | `storage_retention_daily` |
| 16 | `collector_failing.payload.job` (03 draft) | `collector` (16's name) |
| 16 | `spend_soft_cap` / `spend_hard_cap` (03 draft) | 16's `spend_80pct` / `spend_cap_reached` |
| 16 | per-job pause in `job_state(<job>, 'paused')` (§2.11, §3.1) | `control_state(target, 'paused')` (16 §1 and §2.3 already say so) |
| 16 | `control_state.value_json`; prefix-matched pause targets; provider kill files (`X_KILLED`, …) | column `value` (02 §B12); exact job ids or `PAUSE_GROUPS` names; retired (02 §B12 amended) |
| 16 | `DB_BACKUP_MAX_TOTAL_GB` 40, `DB_BACKUP_KEEP_UNCOMPRESSED` 2 (03 §5 draft) | 12 (local dir; mirror `DB_BACKUP_MIRROR_MAX_TOTAL_GB`), 1 (02 §F2, §I1) |
| 03 | own draft: backup pipeline, `DB_BACKUP_COMPRESS/MAX_TOTAL_GB(12)/MIN_FREE_FACTOR`, `db backup`/`db restore` CLI, `app/storage/legacy.py` | ceded to 16 (`backup.py`, keys, `backup …` CLI) and 17 (`app/backfill/*`) |
| 17 | `pool_history` WITHOUT ROWID + provenance NOT NULL | rowid table (old DDL) + nullable provenance columns + `enriched_at`; index name `idx_pool_history_subnet_ts` (17: `ix_pool_history_netuid_ts`) |
| 17 | `pool_history` NULL-only `UPDATE` | allowed only through the trigger-guarded enrichment path; sets `enriched_at` only when an observed value is filled (occupant stamping does not) |
| 17 | `block_times`, `backfill_buckets`, `rpc_owner_history` (new) | adopted (STRICT); + `source`, `run_id` on `block_times`/`rpc_owner_history` |
| 17 | `whale_cohort` + `total_stake_tao, free_fraction, balance_rank, coldkey_swap, observed_at, source` | adopted as nullable trailing columns (`observed_at`, `source` nullable) |
| 17 | `ALTER TABLE price_bars/occupants/github_* ADD COLUMN …` tuple | columns are in v1; no ALTERs |
| 17 | `price_bars` precedence `sse_rollup > ledger_import > tmc_candle > onfinality = opentensor_archive`; "equal rank never replaces" | adopted (03 draft had `ledger_import = onfinality` on top); a same-rank bar replaces when `excluded.ts_last > stored.ts_last` (02 §J1) |
| 17 | `netuid_reoccupied` severity `warn`; `source='backfill_rpc'` | `warning`; `source='registry'` + `payload.backfilled = true`, `alerted_at` preset (02 §B9, §J1); validated against the §3.9 backfill key set |
| 17 | ledger `observed_at = timestamp` for every row; `enriched_at` also on occupant stamping (03 draft) | only for rows ≥ `LEDGER_LIVE_FROM_TS`; `enriched_at` only for merged observed values (17 §2.1, §2.2, §2.5) |
| 03 | own draft `SOCIAL_TEXT_HARD_MAX_DAYS` (120 d, unlabelled text kept longer); "latest observation wins" `metric_values` upsert | retired: text → hash at 90 d or by the pending rule (02 §B10); first success wins (02 §B4) |

## Changelog
- 2026-09-23 reconciliation:
  - B3–B5: `status` generated from `value_json.status` (`none` if missing); single writer `pit.put_metric_values` (alias `upsert_metric_values`); raw first-success-wins with failed→success upgrade only, norm insert-once; trigger rewritten; `metric_at(…, freshness_s)` skips failed rows.
  - B9: `metric_values.source` and `events.source` CHECKs = the exact B9 set; catalog sources are now `chain`/`scoring`/`api`, and `rss` for `news_mentions_7d`.
  - C1–C7: `METRIC_KEYS` = 79 keys. Canonical names are `owner_extraction_trigger`/`owner_control_change_trigger` and `*_last_activity_days`; retired names are rejected. 11 writes `tenure` (raw days) and final P4 scores.
  - D1/D2/D4: renamed 11 events; added `gate_model_broken`, `owner_coldkey_swap_announced` and `owner_self_dealing_suspected` (ts-1.1). Flag-duplicate detections are now digest-only. `netuid_reoccupied` gets a backfill key set; spend events carry `provider`.
  - B6–B8: `regime_daily.label` replaces `market_regime`; `score_runs.n_errors` added; the state rules from B7/D5 are documented; `SCORING_OFFICIAL_HOUR` is pinned.
  - B10 (amended)/B11: added `announcements.text_purge_after`, pending-text purge and an unconditional 90-d text purge; `SOCIAL_TEXT_HARD_MAX_DAYS` retired; `social_posts` is X-only; post ids have no `announcement:` prefix.
  - B12 (amended): `control_state.value`; exact job ids or `PAUSE_GROUPS` targets; closed sentinel list.
  - E3/F2/I1: retention at 03:00Z; stride 5 / 30 d; backups 12 GB local, newest kept uncompressed, `<dest>.tmp`; sizing adds hourly provisional rows (≈ 9 GB at year 1) and states the ≥ 60 GB recommendation.
  - J1: `price_bars` same-rank replacement when `ts_last` is later; `chain_daily` keeps a pre-set `occupant_key`; ledger `observed_at`/`enriched_at` narrowed per 17.
  - G1/G5: `provider_spend` stores spend providers, `requests` and `units_json`.
  - 11's Q2 settled: `review_queue` kinds, `ref_table`/`ref_id` and `dedupe_base`; whitelist `reason`/`owner_change_id`; `owner_changes` gains `coldkey_swap_announced` and `extrinsic_id`.
  - §11/Appendix A: settled items cite 02; P4-aux and P5-leg mappings reversed; the dispatcher index covers every severity (D3).
- 2026-09-24 final pass: §3.9 routing/dedupe — `x_budget_paused` is digest for every reason (D4); 07's eight web patterns are `occ|url|…` (global dedupe index, 07 §11 Q14); 10's label-event "topic hash" slot is the polarity bucket `pos|neg|neu`; `source_disagreement` emitters split (04 non-GitHub identity fields, 06 `github_repo` only); `disk_critical`/`db_integrity_failed`/`backup_failed` route `yes`, with a direct `send_alert` only as the D3 DB-write-failure fallback (the `direct` alert value is retired).
- 2026-09-24 final pass: `github_events` gains `backfilled INTEGER NOT NULL DEFAULT 0` (J1); `backfill_done`/`backfill_failed` are written with `source='runtime'`.
- 2026-09-24 final pass: §2.5 `pool_history` row — stamping `occupant_key` never sets `enriched_at` (matches §2.9 and the column comment); `price_bars` comment states that `sse_rollup` covers both the 5m bars and 05's live `1h` rollup (the CHECKs already allowed it).
- 2026-10-22 (20 N2 item 3, BUILD-LOG 2026-10-22): migration `0005_rpc_emission_share.sql` (additive) — `rpc_subnet_daily.emission_share REAL` (written by 17's `--emission-daily` pass, one row per netuid per day, `as_of` = day 00:00Z, `source` = the archive endpoint) and `v_quant_chain_daily` redefined as `chain_daily` ∪ RPC day rows for occupant-days `chain_daily` lacks, with the RPC share replacing TMC's whole-percent-truncated value before 2025-11-17. Schema v5.
