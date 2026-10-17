"""Ordered, checksummed, idempotent migration runner (spec 03 §2.4).

Version 1 = ``models.sql``; versions ≥ 2 = ``migrations/NNNN_<snake_name>.sql`` (contiguous). Fresh DBs replay
1…N in order. Applied migrations are never re-run; a changed checksum is reported (``checksum_drift:<v>``);
a DB ahead of the code is tolerated (``schema_ahead:<db>><code>``, additive-only policy).
"""
from __future__ import annotations

import hashlib
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

STORAGE_DIR = Path(__file__).resolve().parent
MODELS_SQL = STORAGE_DIR / "models.sql"
MIGRATIONS_DIR = STORAGE_DIR / "migrations"

_FILE_RE = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")
_ADD_COLUMN_RE = re.compile(r"^\s*ALTER\s+TABLE\s+(\w+)\s+ADD\s+COLUMN\s+(\w+)", re.IGNORECASE)
_FORBIDDEN_RE = [
    (re.compile(r"^\s*BEGIN\b", re.IGNORECASE), "BEGIN"),
    (re.compile(r"^\s*COMMIT\b", re.IGNORECASE), "COMMIT"),
    (re.compile(r"^\s*END\s+TRANSACTION\b", re.IGNORECASE), "END TRANSACTION"),
    (re.compile(r"^\s*ROLLBACK\b", re.IGNORECASE), "ROLLBACK"),
    (re.compile(r"^\s*VACUUM\b", re.IGNORECASE), "VACUUM"),
    (re.compile(r"^\s*PRAGMA\s+journal_mode\b", re.IGNORECASE), "PRAGMA journal_mode"),
    (re.compile(r"^\s*PRAGMA\s+auto_vacuum\b", re.IGNORECASE), "PRAGMA auto_vacuum"),
    (re.compile(r"^\s*ATTACH\b", re.IGNORECASE), "ATTACH"),
]
_DESTRUCTIVE_RE = [
    (re.compile(r"^\s*DROP\s+TABLE\b", re.IGNORECASE), "DROP TABLE"),
    (re.compile(r"^\s*ALTER\s+TABLE\s+\w+\s+DROP\b", re.IGNORECASE), "DROP COLUMN"),
    (re.compile(r"^\s*ALTER\s+TABLE\s+\w+\s+RENAME\b", re.IGNORECASE), "RENAME"),
]


class SchemaError(RuntimeError):
    pass


class MigrationError(SchemaError):
    pass


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    path: Path
    sql: str
    checksum: str


@dataclass
class MigrateResult:
    version: int
    applied: list[int] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def discover(migrations_dir: Path | None = None, models_sql: Path | None = None) -> list[Migration]:
    """``models.sql`` as v1 + ``migrations/*.sql`` sorted; a gap or duplicate raises SchemaError."""
    models_sql = models_sql or MODELS_SQL
    migrations_dir = migrations_dir or MIGRATIONS_DIR
    raw = models_sql.read_bytes()
    out = [Migration(1, "baseline", models_sql, raw.decode("utf-8"), _checksum(raw))]
    seen: dict[int, Path] = {}
    if migrations_dir.is_dir():
        for p in sorted(migrations_dir.glob("*.sql")):
            m = _FILE_RE.match(p.name)
            if not m:
                raise SchemaError(f"bad migration file name: {p.name} (expected NNNN_snake_name.sql)")
            v = int(m.group(1))
            if v in seen:
                raise SchemaError(f"duplicate migration version {v}: {seen[v].name}, {p.name}")
            seen[v] = p
    expected = 2
    for v in sorted(seen):
        if v != expected:
            raise SchemaError(f"migration numbering gap: expected {expected:04d}, found {v:04d}")
        p = seen[v]
        data = p.read_bytes()
        out.append(Migration(v, _FILE_RE.match(p.name).group(2), p, data.decode("utf-8"), _checksum(data)))
        expected += 1
    return out


def split_statements(sql: str) -> list[str]:
    """Accumulate lines until ``sqlite3.complete_statement``; comment-only chunks are dropped."""
    out: list[str] = []
    buf: list[str] = []
    for line in sql.splitlines():
        if not buf and (not line.strip() or line.strip().startswith("--")):
            continue
        buf.append(line)
        text = "\n".join(buf)
        if sqlite3.complete_statement(text):
            stmt = text.strip()
            if _strip_comments(stmt):
                out.append(stmt)
            buf = []
    rest = "\n".join(buf).strip()
    if rest and _strip_comments(rest):
        raise SchemaError(f"incomplete SQL statement at end of file: {rest[:120]!r}")
    return out


def _strip_comments(stmt: str) -> str:
    return "\n".join(l for l in stmt.splitlines() if not l.strip().startswith("--")).strip().rstrip(";").strip()


def lint(sql: str, *, version: int) -> list[str]:
    """Return lint problems (forbidden statements; destructive DDL without ``-- @allow-destructive``)."""
    problems: list[str] = []
    allow_destructive = sql.lstrip().splitlines()[0].startswith("-- @allow-destructive") if sql.strip() else False
    for stmt in split_statements(sql):
        body = _strip_comments(stmt)
        if re.match(r"^\s*CREATE\s+TRIGGER\b", body, re.IGNORECASE):
            continue  # a trigger body legitimately contains BEGIN … END
        for rx, name in _FORBIDDEN_RE:
            if rx.match(body):
                problems.append(f"v{version}: forbidden statement {name}: {body[:80]}")
        if version >= 2 and not allow_destructive:
            for rx, name in _DESTRUCTIVE_RE:
                if rx.match(body):
                    problems.append(f"v{version}: destructive {name} without '-- @allow-destructive': {body[:80]}")
    return problems


async def migrate(db: "Database", *, migrations: list[Migration] | None = None) -> MigrateResult:
    migs = migrations if migrations is not None else discover()
    code_version = max(m.version for m in migs)
    await db.execute(
        "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY, name TEXT NOT NULL, "
        "checksum TEXT NOT NULL, applied_at INTEGER NOT NULL) STRICT"
    )
    cols = {r["name"] for r in await db.fetchall("PRAGMA table_xinfo(schema_version)", writer=True)}
    if "name" not in cols:
        await db.execute("ALTER TABLE schema_version ADD COLUMN name TEXT NOT NULL DEFAULT 'baseline'")
    if "checksum" not in cols:
        await db.execute("ALTER TABLE schema_version ADD COLUMN checksum TEXT NOT NULL DEFAULT ''")
    applied = {r["version"]: r["checksum"] for r in
               await db.fetchall("SELECT version, checksum FROM schema_version", writer=True)}
    result = MigrateResult(version=max(applied) if applied else 0)
    warnings: list[str] = []
    for m in migs:
        if m.version in applied:
            if applied[m.version] and applied[m.version] != m.checksum:
                warnings.append(f"checksum_drift:{m.version}")
            continue
        problems = lint(m.sql, version=m.version)
        if problems:
            raise MigrationError("; ".join(problems))
        stmt = ""
        try:
            async with db.transaction(immediate=True):
                for stmt in split_statements(m.sql):
                    am = _ADD_COLUMN_RE.match(_strip_comments(stmt))
                    if am:
                        existing = {r["name"] for r in await db.fetchall(f"PRAGMA table_xinfo({am.group(1)})")}
                        if am.group(2) in existing:
                            continue
                    await db.execute(stmt)
                await db.execute("INSERT INTO schema_version (version, name, checksum, applied_at) VALUES (?,?,?,?)",
                                 (m.version, m.name, m.checksum, now_ts()))
                await db.execute(f"PRAGMA user_version = {int(m.version)}")
        except MigrationError:
            raise
        except Exception as exc:
            raise MigrationError(f"v{m.version} {m.name}: {stmt[:200]}: {exc}") from exc
        result.applied.append(m.version)
        result.version = m.version
    db_version = max([result.version, *applied.keys()]) if (applied or result.applied) else 0
    if db_version > code_version:
        warnings.append(f"schema_ahead:{db_version}>{code_version}")
    result.version = db_version
    result.warnings = warnings
    if warnings:
        try:
            from app.logging.logger import logger
            logger.error("schema warnings", data={"warnings": warnings})
        except Exception:  # pragma: no cover
            pass
        await _emit_schema_warnings(db, warnings)
    return result


async def _emit_schema_warnings(db: "Database", warnings: list[str]) -> None:
    from app.storage.events import emit_event

    now = now_ts()
    for w in warnings:
        kind, _, detail = w.partition(":")
        version = detail.split(">")[0] if kind == "schema_ahead" else detail
        try:
            await emit_event(db, event_type="schema_warning", occupant_key=None, netuid=None, severity="warning",
                             event_at=now, observed_at=now,
                             payload={"kind": kind, "version": version, "detail": w,
                                      "text": f"⚠️ <b>schema warning</b>: {w}"},
                             dedupe_key=f"{kind}|{version}", source="storage", run_id=None)
        except Exception:  # pragma: no cover - events table may not exist on an exotic DB
            pass


def _discover_code_version() -> int:
    try:
        return max(m.version for m in discover())
    except Exception:  # pragma: no cover
        return 1


CODE_SCHEMA_VERSION: int = _discover_code_version()
