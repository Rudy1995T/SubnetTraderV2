"""01 §2.14 ``python -m app status`` (20 N3 item 4) and the read-only ``ReadOnlyDatabase`` it uses."""
from __future__ import annotations

import sqlite3

import pytest

import app.__main__ as cli
from app.storage.db import ReadOnlyDatabase

pytestmark = pytest.mark.asyncio


async def test_read_only_database_refuses_writes(seeded):
    db = ReadOnlyDatabase(seeded.db.path)
    await db.connect()
    try:
        assert db.schema_version == seeded.db.schema_version > 0
        assert await db.fetchval("SELECT COUNT(*) FROM scores", writer=True) > 0
        with pytest.raises(sqlite3.OperationalError):
            await db.fetchall("DELETE FROM job_state")
    finally:
        await db.close()


async def test_status(seeded, cfg, monkeypatch, capsys):
    cfg(DB_PATH=seeded.db.path)
    health = {"status": "ok", "version": "0.1.0+abc1234", "uptime_sec": 3600, "degraded_reasons": [],
              "scheduler": {"declared": 58, "available": 58},
              "collectors": {"tmc_daily": {"stale": True}, "youtube_poll": {"consecutive_failures": 2}},
              "tasks": {}, "alerts": {"pending_immediate": 1}, "telegram_configured": True,
              "spend": {"x": {"usd": 1.5, "cap_usd": 25.0}, "tmc": {"requests": 2800, "request_cap": 90000},
                        "llm": {"usd": 0.0, "cap_usd": 0.0}},
              "db": {"size_bytes": 7.85e9, "wal_bytes": 6.7e7, "schema_version": 5, "last_backup_age_hours": 14.1},
              "scores": {"stale": False}}
    monkeypatch.setattr(cli, "_fetch_health", lambda: health)
    monkeypatch.setattr(cli, "_git_head", lambda: "def5678")
    assert await cli._status() == 0
    out = capsys.readouterr().out
    assert "running abc1234, checkout is def5678" in out
    assert "Tiers    A 1 · B 0 · C 0 · D 0 · F 0" in out                       # SN64's slot P0
    assert "/45 populated" in out and "P1 " in out and "P6 " in out
    assert "stale: tmc_daily" in out and "failing: youtube_poll" in out
    assert "pending immediate 1" in out and "red flags active 2" in out
    assert "x $1.50/$25" in out and "tmc 2.8k/90k req" in out and "llm off" in out
    monkeypatch.setattr(cli, "_git_head", lambda: "abc1234")
    monkeypatch.setattr(cli, "_fetch_health", lambda: None)
    assert await cli._status() == 1                                           # service down: DB part still prints
    out = capsys.readouterr().out
    assert "UNREACHABLE" in out and "Scores   latest" in out and "running" not in out
