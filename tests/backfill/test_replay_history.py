"""Spec 17 §2.11 (S10, spec 20 N2 item 2): replay driver — occupants alive at D, S9 coverage hook, resumable owners."""
from __future__ import annotations

import asyncio
import json

from scripts import replay_history as rh

D0 = 1_759_276_800                                   # 2025-10-01T00:00Z


def run(coro):
    return asyncio.run(coro)


def _occ(con, key, netuid, reg, dereg=None, status="active"):
    con.execute("INSERT INTO occupants (occupant_key, netuid, network_registered_at, status, registered_ts, "
                "deregistered_ts, first_seen_block, first_seen_at, last_seen_block, last_seen_at, source) "
                "VALUES (?,?,?,?,?,?,1,1,1,1,'rpc')", (key, netuid, int(key.split(":")[1]), status, reg, dereg))


def test_occupants_alive_includes_dead_and_excludes_future(bf):
    from app.backfill import common
    _occ(bf, "5:1", 5, D0 - 100 * 86400)                                   # alive throughout
    _occ(bf, "9:1", 9, D0 - 200 * 86400, dereg=D0 + 86400, status="deregistered")   # dies the next day
    _occ(bf, "9:2", 9, D0 + 86400)                                         # its successor: not yet registered
    _occ(bf, "0:0", 0, 0)                                                  # root never replayed

    async def go():
        db = await common.prepare_db()
        try:
            return ([o["occupant_key"] for o in await rh.occupants_alive(db, D0)],
                    [o["occupant_key"] for o in await rh.occupants_alive(db, D0 + 2 * 86400)])
        finally:
            await db.close()
    today, later = run(go())
    assert today == ["5:1", "9:1"] and later == ["5:1", "9:2"]


def test_github_coverage_hook(tmp_path):
    p = tmp_path / "github_progress.json"
    p.write_text(json.dumps({"1": {"commits": "done"}, "2": {"commits": "partial", "truncated": True},
                             "3": {"commits": "done", "truncated": True, "oldest_seen": D0 - 10},
                             "4": {"commits": "done", "truncated": True, "oldest_seen": D0 + 10},
                             "6": {"commits": "gone", "status": "gone"}, "_meta": {"scope_repos": 5}}))
    cov = rh.github_coverage(p)
    assert cov(1, D0) and not cov(2, D0) and cov(3, D0) and not cov(4, D0) and not cov(5, D0) and cov(6, D0)
    assert not rh.github_coverage(tmp_path / "missing.json")(1, D0)


def test_owner_resumes_from_watermark(bf, monkeypatch):
    from app.backfill import common
    seen: list[int] = []

    async def fake(db, d, run_id):
        seen.append(d)
        return 1
    monkeypatch.setattr(rh, "replay_chain", fake)

    async def go():
        db = await common.prepare_db()
        try:
            a = await rh.run_owner(db, "chain", d0=D0, d1=D0 + 4 * 86400, redo=False, limit=2, run_id="r", spec="x")
            b = await rh.run_owner(db, "chain", d0=D0, d1=D0 + 4 * 86400, redo=False, limit=None, run_id="r",
                                   spec="x")
            c = await rh.run_owner(db, "chain", d0=D0, d1=D0 + 86400, redo=True, limit=None, run_id="r", spec="x")
            return a, b, c, await rh.watermark(db, "chain")
        finally:
            await db.close()
    a, b, c, wm = run(go())
    assert (a["days"], b["days"], c["days"]) == (2, 3, 2)                  # limit, resume after D0+1, --redo
    assert seen == [D0, D0 + 86400, D0 + 2 * 86400, D0 + 3 * 86400, D0 + 4 * 86400, D0, D0 + 86400]
    assert wm == D0 + 86400


def test_github_owner_refuses_without_s9(bf):
    import pytest
    from app.backfill import common

    async def go():
        db = await common.prepare_db()
        try:
            await rh.run_owner(db, "github", d0=D0, d1=D0, redo=False, limit=None, run_id="r", spec="x")
        finally:
            await db.close()
    with pytest.raises(common.PreconditionError):
        run(go())
