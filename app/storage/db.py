"""SQLite access layer (spec 03 §2.2–§2.3): one writer + one read-only reader connection, WAL.

Write serialisation: a process-wide asyncio.Lock guards the writer; ``transaction()`` holds it for the block
and marks the owning task via a ContextVar so its statements run without re-locking (read-your-writes too).
"""
from __future__ import annotations

import asyncio
import contextvars
import os
import re
import sqlite3
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Iterable, Mapping, Sequence

import aiosqlite

import app.config as app_config
from app.storage import precedence
from app.storage.migrate import CODE_SCHEMA_VERSION, MigrationError, SchemaError  # noqa: F401 (re-exports)

MIN_SQLITE = (3, 37, 0)

APPEND_ONLY_TABLES: frozenset[str] = frozenset({
    "identity_observations", "identity_raw_snapshots", "chain_ticks", "chain_trending", "chain_pressure",
    "chain_constants", "chain_rest_snapshots", "holders_snapshots", "gate_fits", "rpc_subnet_daily",
    "rpc_metagraph_daily", "block_times", "rpc_owner_history", "github_repo_snapshots", "github_commits",
    "github_events", "web_checks", "discord_snapshots", "telegram_snapshots", "announcements",
    "social_post_mentions", "x_account_snapshots", "news_items", "video_items", "post_labels",
    "post_label_entities", "owner_snapshots", "owner_position_hourly", "owner_stake_events", "owner_changes",
    "manual_flags", "scores", "events", "pool_history", "whale_events", "whale_cohort",
})

_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")
_MAX_PARAMS = 32000

_tx_owner: contextvars.ContextVar[int | None] = contextvars.ContextVar("taosentinel_tx_owner", default=None)


class AppendOnlyViolation(RuntimeError):
    pass


@dataclass(frozen=True)
class ExecResult:
    rowcount: int
    lastrowid: int | None


def _row_to_dict(cursor: Any, row: Sequence[Any]) -> dict[str, Any]:
    desc = cursor.description
    if desc is None:  # e.g. PRAGMA incremental_vacuum steps return description-less rows
        return {}
    return {d[0]: row[i] for i, d in enumerate(desc)}


async def _apply_pragmas(conn: aiosqlite.Connection, *, reader: bool) -> None:
    s = app_config.settings
    await conn.execute(f"PRAGMA busy_timeout = {int(s.DB_BUSY_TIMEOUT_MS)}")
    await conn.execute("PRAGMA temp_store = MEMORY")
    await conn.execute(f"PRAGMA cache_size = -{int(s.DB_CACHE_SIZE_MB) * 1024}")
    await conn.execute(f"PRAGMA mmap_size = {int(s.DB_MMAP_SIZE_MB) * 1048576}")
    if reader:
        await conn.execute("PRAGMA query_only = ON")
    else:
        await conn.execute("PRAGMA synchronous = NORMAL")
        await conn.execute("PRAGMA foreign_keys = ON")
        await conn.execute("PRAGMA wal_autocheckpoint = 1000")
        await conn.execute("PRAGMA journal_size_limit = 67108864")
    await conn.create_function("src_rank", 1, precedence.rank_any, deterministic=True)


class Database:
    def __init__(self, path: str | Path, *, busy_timeout_ms: int | None = None, reader: bool | None = None) -> None:
        self.path = Path(path)
        self._busy_timeout_ms = busy_timeout_ms
        self._reader_wanted = reader
        self._w: aiosqlite.Connection | None = None
        self._r: aiosqlite.Connection | None = None
        self._wlock = asyncio.Lock()
        self._schema_version = 0
        self.schema_warnings: list[str] = []
        self._cols: dict[str, list[str]] = {}
        self.reader_enabled = False

    # ── lifecycle ──────────────────────────────────────────────────────────────────────────────────────────
    @property
    def schema_version(self) -> int:
        return self._schema_version

    @property
    def connected(self) -> bool:
        return self._w is not None

    async def connect(self) -> None:
        if sqlite3.sqlite_version_info < MIN_SQLITE:
            raise SchemaError(f"sqlite >= {'.'.join(map(str, MIN_SQLITE))} required (have {sqlite3.sqlite_version})")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        w = await aiosqlite.connect(str(self.path), isolation_level=None)
        try:
            w.row_factory = _row_to_dict
            if self._busy_timeout_ms is not None:
                await w.execute(f"PRAGMA busy_timeout = {int(self._busy_timeout_ms)}")
            try:
                await (await w.execute("SELECT json_valid('{}')")).fetchone()
            except sqlite3.Error as exc:
                raise SchemaError("JSON1 missing") from exc
            page_count = (await (await w.execute("PRAGMA page_count")).fetchone())["page_count"]
            if page_count == 0:
                await w.execute("PRAGMA auto_vacuum = INCREMENTAL")
            mode = (await (await w.execute("PRAGMA journal_mode = WAL")).fetchone())["journal_mode"]
            if str(mode).lower() != "wal":
                raise SchemaError(f"journal_mode must be wal, got {mode!r}")
            await _apply_pragmas(w, reader=False)
            if self._busy_timeout_ms is not None:
                await w.execute(f"PRAGMA busy_timeout = {int(self._busy_timeout_ms)}")
        except BaseException:
            await w.close()
            raise
        self._w = w
        want_reader = self._reader_wanted if self._reader_wanted is not None else app_config.settings.DB_READER_ENABLED
        if want_reader:
            try:
                r = await aiosqlite.connect(f"file:{self.path}?mode=ro", uri=True, isolation_level=None)
                r.row_factory = _row_to_dict
                await _apply_pragmas(r, reader=True)
                if self._busy_timeout_ms is not None:
                    await r.execute(f"PRAGMA busy_timeout = {int(self._busy_timeout_ms)}")
                self._r = r
                self.reader_enabled = True
            except Exception as exc:  # reader is an optimisation; never fatal (03 §4)
                self._r = None
                self.reader_enabled = False
                try:
                    from app.logging.logger import logger
                    logger.warning("db reader disabled", data={"error": str(exc)})
                except Exception:  # pragma: no cover
                    pass

    async def apply_schema(self) -> int:
        from app.storage.migrate import migrate

        res = await migrate(self)
        self._schema_version = res.version
        self.schema_warnings = list(res.warnings)
        self._cols.clear()
        return res.version

    async def close(self) -> None:
        r, w = self._r, self._w
        self._r = None
        self._w = None
        for conn in (r, w):
            if conn is not None:
                try:
                    await conn.close()
                except Exception:  # pragma: no cover
                    pass

    # ── transactions & execution ───────────────────────────────────────────────────────────────────────────
    def _owns_tx(self) -> bool:
        return _tx_owner.get() == id(self)

    def _writer(self) -> aiosqlite.Connection:
        if self._w is None:
            raise RuntimeError("Database is not connected")
        return self._w

    @asynccontextmanager
    async def transaction(self, *, immediate: bool = True) -> AsyncIterator[None]:
        if self._owns_tx():
            yield  # nested: join the outer transaction
            return
        async with self._wlock:
            token = _tx_owner.set(id(self))
            w = self._writer()
            try:
                await w.execute("BEGIN IMMEDIATE" if immediate else "BEGIN DEFERRED")
                try:
                    yield
                except BaseException:
                    try:
                        await w.execute("ROLLBACK")
                    except Exception:  # pragma: no cover
                        pass
                    raise
                else:
                    await w.execute("COMMIT")
            finally:
                _tx_owner.reset(token)

    async def execute(self, sql: str, params: Sequence | Mapping = ()) -> ExecResult:
        if self._owns_tx():
            cur = await self._writer().execute(sql, params)
            return ExecResult(cur.rowcount, cur.lastrowid)
        async with self._wlock:
            cur = await self._writer().execute(sql, params)
            return ExecResult(cur.rowcount, cur.lastrowid)

    async def executemany(self, sql: str, seq: Iterable[Sequence | Mapping]) -> int:
        seq = list(seq)
        if not seq:
            return 0
        if self._owns_tx():
            cur = await self._writer().executemany(sql, seq)
            return cur.rowcount
        async with self.transaction():
            cur = await self._writer().executemany(sql, seq)
            return cur.rowcount

    async def execute_returning(self, sql: str, params: Sequence | Mapping = ()) -> list[dict]:
        """Run a writing statement with a RETURNING clause and fetch its rows (writer, locked)."""
        if self._owns_tx():
            cur = await self._writer().execute(sql, params)
            return list(await cur.fetchall())
        async with self._wlock:
            cur = await self._writer().execute(sql, params)
            return list(await cur.fetchall())

    def _read_conn(self, writer: bool) -> aiosqlite.Connection:
        if writer or self._owns_tx() or self._r is None:
            return self._writer()
        return self._r

    async def fetchall(self, sql: str, params: Sequence | Mapping = (), *, writer: bool = False) -> list[dict]:
        conn = self._read_conn(writer)
        if conn is self._w and not self._owns_tx():
            async with self._wlock:
                cur = await conn.execute(sql, params)
                return list(await cur.fetchall())
        cur = await conn.execute(sql, params)
        return list(await cur.fetchall())

    async def fetchone(self, sql: str, params: Sequence | Mapping = (), *, writer: bool = False) -> dict | None:
        rows = await self.fetchall(sql, params, writer=writer)
        return rows[0] if rows else None

    async def fetchval(self, sql: str, params: Sequence | Mapping = (), *, writer: bool = False) -> Any:
        row = await self.fetchone(sql, params, writer=writer)
        if not row:
            return None
        return next(iter(row.values()))

    # ── helpers ────────────────────────────────────────────────────────────────────────────────────────────
    async def columns(self, table: str) -> list[str]:
        if not _IDENT.match(table):
            raise ValueError(f"bad table name {table!r}")
        if table not in self._cols:
            rows = await self.fetchall(f"PRAGMA table_xinfo({table})", writer=True)
            if not rows:
                raise ValueError(f"unknown table {table!r}")
            self._cols[table] = [r["name"] for r in rows if r.get("hidden", 0) in (0, 1)]
        return self._cols[table]

    async def _validate_rows(self, table: str, rows: Sequence[Mapping[str, Any]]) -> list[str]:
        cols = await self.columns(table)
        keys = list(rows[0].keys())
        kset = set(keys)
        for k in keys:
            if not _IDENT.match(k) or k not in cols:
                raise ValueError(f"unknown column {k!r} for table {table}")
        for r in rows[1:]:
            if set(r.keys()) != kset:
                raise ValueError("all rows must share one key set")
        return keys

    async def insert_or_ignore(self, table: str, rows: Sequence[Mapping[str, Any]], *, chunk: int = 500) -> int:
        if not rows:
            return 0
        keys = await self._validate_rows(table, rows)
        eff = max(1, min(chunk, _MAX_PARAMS // len(keys)))
        inserted = 0
        async with self.transaction():
            for i in range(0, len(rows), eff):
                part = rows[i:i + eff]
                ph = "(" + ",".join("?" * len(keys)) + ")"
                sql = f"INSERT OR IGNORE INTO {table} ({', '.join(keys)}) VALUES " + ",".join([ph] * len(part))
                params = [r[k] for r in part for k in keys]
                res = await self.execute(sql, params)
                inserted += max(0, res.rowcount)
        return inserted

    async def upsert(self, table: str, rows: Sequence[Mapping[str, Any]], *, conflict: Sequence[str],
                     update: Sequence[str] | None = None, where: str | None = None, chunk: int = 500) -> int:
        if table in APPEND_ONLY_TABLES:
            raise AppendOnlyViolation(f"{table} is append-only; upsert() refused")
        if not rows:
            return 0
        keys = await self._validate_rows(table, rows)
        for c in conflict:
            if c not in keys:
                raise ValueError(f"conflict column {c!r} missing from rows")
        upd = list(update) if update is not None else [k for k in keys if k not in conflict]
        for c in upd:
            if not _IDENT.match(c):
                raise ValueError(f"bad column {c!r}")
        eff = max(1, min(chunk, _MAX_PARAMS // len(keys)))
        changed = 0
        set_clause = ", ".join(f"{c} = excluded.{c}" for c in upd)
        action = f"DO UPDATE SET {set_clause}" + (f" WHERE {where}" if where else "") if upd else "DO NOTHING"
        async with self.transaction():
            for i in range(0, len(rows), eff):
                part = rows[i:i + eff]
                ph = "(" + ",".join("?" * len(keys)) + ")"
                sql = (f"INSERT INTO {table} ({', '.join(keys)}) VALUES " + ",".join([ph] * len(part)) +
                       f" ON CONFLICT ({', '.join(conflict)}) {action}")
                res = await self.execute(sql, [r[k] for r in part for k in keys])
                changed += max(0, res.rowcount)
        return changed

    # ── maintenance ────────────────────────────────────────────────────────────────────────────────────────
    async def backup_to(self, dest: str | Path) -> dict:
        """VACUUM INTO ``<dest>.tmp`` from a dedicated connection → quick_check → atomic rename (03 §2.3 item 7)."""
        dest = Path(dest)
        tmp = dest.with_name(dest.name + ".tmp")
        src = str(self.path)

        def _do() -> dict:
            dest.parent.mkdir(parents=True, exist_ok=True)
            if tmp.exists():
                tmp.unlink()  # VACUUM INTO refuses to overwrite
            con = sqlite3.connect(src)
            try:
                con.execute(f"PRAGMA busy_timeout = {int(app_config.settings.DB_BUSY_TIMEOUT_MS)}")
                con.execute("VACUUM INTO ?", (str(tmp),))
            finally:
                con.close()
            integrity = _quick_check(tmp)
            if integrity != "ok":
                try:
                    tmp.unlink()
                except OSError:
                    pass
                raise RuntimeError(f"backup quick_check failed: {integrity}")
            os.replace(tmp, dest)
            return {"path": str(dest), "size_bytes": dest.stat().st_size, "integrity": integrity}

        return await asyncio.to_thread(_do)

    async def stats(self) -> dict:
        size = self.path.stat().st_size if self.path.exists() else 0
        walp = Path(str(self.path) + "-wal")
        wal = walp.stat().st_size if walp.exists() else 0
        q = lambda p: self.fetchval(f"PRAGMA {p}", writer=True)  # noqa: E731
        return {
            "path": str(self.path), "size_bytes": size, "wal_bytes": wal,
            "page_size": await q("page_size"), "page_count": await q("page_count"),
            "freelist_count": await q("freelist_count"), "schema_version": self._schema_version,
            "sqlite_version": sqlite3.sqlite_version, "journal_mode": await q("journal_mode"),
            "auto_vacuum": await q("auto_vacuum"),
        }

    async def checkpoint(self, mode: str = "PASSIVE") -> tuple[int, int, int]:
        mode = mode.upper()
        if mode not in {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}:
            raise ValueError(mode)
        row = await self.fetchone(f"PRAGMA wal_checkpoint({mode})", writer=True)
        vals = list(row.values()) if row else [0, 0, 0]
        return int(vals[0]), int(vals[1]), int(vals[2])

    async def incremental_vacuum(self, pages: int) -> int:
        before = await self.fetchval("PRAGMA freelist_count", writer=True) or 0
        if pages > 0:
            async with self._wlock:
                cur = await self._writer().execute(f"PRAGMA incremental_vacuum({int(pages)})")
                await cur.fetchall()
        after = await self.fetchval("PRAGMA freelist_count", writer=True) or 0
        return max(0, int(before) - int(after))

    async def optimize(self) -> None:
        await self.execute("PRAGMA optimize")


def _quick_check(path: Path) -> str:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        rows = con.execute("PRAGMA quick_check").fetchall()
    finally:
        con.close()
    lines = [str(r[0]) for r in rows]
    return "ok" if lines == ["ok"] else "\n".join(lines[:5])
