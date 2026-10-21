"""Spec 11 §10 step 3 (two-source round trip) through ``scripts.manual_entry`` on a temp DB, and the drill guard."""
from __future__ import annotations

import asyncio
import json

from tests.integrity.conftest import seed_occ

LI = "https://www.linkedin.com/in/probe-person"


def run(capsys, *args) -> tuple[int, object]:
    from scripts.manual_entry import main
    code = main(list(args))
    out = capsys.readouterr().out.strip()
    return code, json.loads(out) if out else None


def test_manual_entry_round_trip(tmp_path, capsys, iset):
    from app.storage.db import Database
    path = tmp_path / "copy.db"

    async def prep():
        db = Database(path)
        await db.connect()
        await db.apply_schema()
        await seed_occ(db, 64)
        await db.close()
    asyncio.run(prep())
    code, r = run(capsys, "--db", str(path), "team", "add", "64", "--name", "Probe Person", "--role", "CTO",
                  "--profile", LI, "--evidence", LI, "--verify", "--by", "rudolf")
    assert code == 0 and r["ok"] and r["status"] == "candidate" and r["warning"] == "two_source_rule"
    mid = str(r["id"])
    code, r = run(capsys, "--db", str(path), "team", "verify", mid, "--evidence", "https://chutes.ai/", "--by", "rudolf")
    assert code == 0 and r["status"] == "verified" and r["extra"]["evidence_count"] == 2
    code, r = run(capsys, "--db", str(path), "team", "reject", mid, "--reason", "verify-checklist probe", "--by",
                  "rudolf")
    assert code == 0 and r["status"] == "rejected"
    code, r = run(capsys, "--db", str(path), "team", "reject", mid, "--reason", "again", "--by", "rudolf")
    assert code == 1 and r["error"] == "invalid_transition"
    code, rows = run(capsys, "--db", str(path), "team", "list", "64")
    assert code == 0 and rows[0]["status"] == "rejected"
    code, r = run(capsys, "--db", str(path), "owner", "compromise", "64", "--from", "2026-07-13T00:00:00Z", "--to",
                  "2026-07-22T00:00:00Z", "--evidence", "https://protos.com/oro", "--by", "rudolf")
    assert code == 0 and r["ok"] and "flag_not_active:owner_extraction" in r["warning"]
    code, r = run(capsys, "--db", str(path), "owner", "whitelist", "add", "64", "--reason", "disclosed_transfer",
                  "--applies-to", "stake_events", "--from", "2026-01-01T00:00:00Z", "--to", "2027-01-01T00:00:00Z",
                  "--evidence", "https://chutes.ai/treasury", "--new-coldkey", "5Treasury", "--by", "rudolf")
    assert code == 0 and r["ok"]
    code, rows = run(capsys, "--db", str(path), "owner", "whitelist", "list", "64")
    assert len(rows) == 2
    code, rows = run(capsys, "--db", str(path), "review", "list")
    assert code == 0 and rows == []


def test_drill_refuses_live_db(iset, capsys):
    import app.config as app_config
    from app.integrity.detectors import main
    s = app_config.settings
    assert main(["drill", "--db", str(s.path(s.DB_PATH)), "--netuid", "64"]) == 2
