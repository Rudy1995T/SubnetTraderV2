"""Phase 3 (spec 17) fixtures: 03's real schema on a temp DB, an old-schema ledger builder, a fake RPC gateway."""
from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest

import app.config as app_config

TRUST = 1780182493
LIVE_FROM = 1777468595
SEED_T0 = 1758585588
GOLDEN = {"netuid": 64, "tao_in": 187212134573233, "alpha_in": 2897413298870939, "moving_price": {"bits": 278259363},
          "subnet_volume": 4657484168541687, "network_registered_at": 4531295}


@pytest.fixture
def bf(tmp_path, monkeypatch):
    """Settings rooted at tmp_path + a migrated DB; yields an ``open_db`` connection."""
    from app.backfill import common
    monkeypatch.setattr(app_config, "APP_ROOT", tmp_path)
    s = app_config.Settings(
        DB_PATH=str(tmp_path / "data" / "t.db"), JSONL_DIR=str(tmp_path / "data" / "logs"),
        DB_BACKUP_DIR=str(tmp_path / "data" / "backups"), RUN_DIR=str(tmp_path / "data" / "run"),
        CONTROL_DIR=str(tmp_path / "data" / "control"), DATA_DIR=str(tmp_path / "data"),
        KILL_SWITCH_PATH=str(tmp_path / "KILL_SWITCH"), PAUSE_ALL_PATH=str(tmp_path / "PAUSE_ALL"),
        SCORING_KILL_SWITCH_PATH=str(tmp_path / "SCORING_KILLED"), SEED_DIR=str(tmp_path / "seed"),
        BACKFILL_MIN_FREE_GB=0.0)
    monkeypatch.setattr(app_config, "settings", s)
    (tmp_path / "data" / "run").mkdir(parents=True, exist_ok=True)
    (tmp_path / "seed").mkdir(parents=True, exist_ok=True)

    async def _prep():
        db = await common.prepare_db()
        await db.close()
    asyncio.run(_prep())
    common.reset_stop()
    con = common.open_db()
    yield con
    con.close()


def make_old_ledger(path: Path, *, optional_cols: bool = True, extra_rows: list[tuple] | None = None) -> list[tuple]:
    """Old-schema ``pool_history`` (models.sql:55-70) with 4 netuids: a daily seed era with NULL columns and a
    back-stamped ``network_registered_at``, 1-minute dense rows, a trusted era, netuid 40 re-registered at 8,409,860,
    and one non-positive row."""
    con = sqlite3.connect(path)
    cols = "netuid INTEGER, block_number INTEGER, timestamp INTEGER, price REAL, tao_in_pool REAL, alpha_in_pool REAL"
    if optional_cols:
        cols += ", moving_price REAL, network_registered_at INTEGER, subnet_volume REAL"
    con.execute(f"CREATE TABLE pool_history ({cols}, PRIMARY KEY (netuid, block_number))")
    rows: list[tuple] = []
    regs = {1: 1000, 2: 2000, 40: 3372582, 64: 4531295}
    for d in range(3):                                    # seed era (daily, Taostats), back-stamped reg
        for n in regs:
            rows.append((n, 6502594 + d * 7200, SEED_T0 + d * 86400, 0.01 * n, 100.0 + d, 1000.0, None, regs[n], None))
    for i in range(120):                                  # dense live era, before the trust boundary
        for n in (1, 64):
            rows.append((n, 8000000 + i * 5, LIVE_FROM + i * 60, 0.05 + i * 1e-5, 200.0, 3000.0, None, regs[n], None))
    base_b, base_t = 8409800, 1781497500 - 60 * 12
    for i in range(24):                                   # trusted era, netuid 40 old occupant
        rows.append((40, base_b + i, base_t + i * 12, 0.02, 50.0, 2500.0, 0.021, 3372582, 10.0 + i))
    for i in range(24):                                   # netuid 40 new occupant from 8,409,860
        b = 8409860 + i
        rows.append((40, b, 1781497500 + i * 12, 0.03, 60.0, 2000.0, 0.031, 8409860, 1.0 + i))
    for i in range(10):
        rows.append((64, 8443748 + i, 1781904156 + i * 12, 0.064653121, 187212.134573233, 2897413.298870939,
                     0.06478730659000576, 4531295, 4657484.168541687))
    rows.append((2, 8443760, 1781904300, 0.0, 10.0, 10.0, None, None, None))      # non-positive price
    rows.extend(extra_rows or [])
    if optional_cols:
        con.executemany("INSERT INTO pool_history VALUES (?,?,?,?,?,?,?,?,?)", rows)
    else:
        con.executemany("INSERT INTO pool_history VALUES (?,?,?,?,?,?)", [r[:6] for r in rows])
    con.commit()
    con.close()
    return rows


class FakeLimiter:
    def __init__(self) -> None:
        self.acquired = 0
        self.counted = 0

    async def acquire(self, provider, **kw):
        self.acquired += 1
        return True

    def count(self, provider):
        self.counted += 1


class FakeSubstrate:
    """ASI stand-in: ``rpc_request`` answers from a deterministic chain; ``ws.send`` counts socket requests."""

    def __init__(self, chain: "FakeChain") -> None:
        self.chain = chain
        self.sent = 0
        self.runtime_loads = 0
        self.runtime_calls = 0

        class _Ws:
            async def send(inner, payload):  # noqa: N805
                self.sent += 1
                return "id"
        self.ws = _Ws()

    async def rpc_request(self, method, params):
        await self.ws.send({"method": method})
        return {"result": self.chain.answer(method, params)}

    async def get_runtime_for_version(self, v, h):
        self.runtime_loads += 1
        return FakeRuntime()

    async def runtime_call(self, *a, **k):  # must never be used on the hot path
        self.runtime_calls += 1
        raise AssertionError("runtime_call used")

    async def close(self):
        return None


class FakeRuntime:
    metadata_v15 = object()
    runtime_api_map = {"SubnetInfoRuntimeApi": {"get_all_dynamic_info": {"output": 1}}}


class FakeChain:
    """``ts(block) = 1758585588 + 12.02·(block − 6502594)`` with a 2-hour stall injected after ``stall_at``."""

    def __init__(self, stall_at: int | None = None, stall_sec: int = 7200) -> None:
        self.stall_at = stall_at
        self.stall_sec = stall_sec
        self.errors: list[Exception] = []

    def ts(self, block: int) -> int:
        t = SEED_T0 + 12.02 * (block - 6502594)
        if self.stall_at is not None and block > self.stall_at:
            t += self.stall_sec
        return int(t)

    def answer(self, method, params):
        if self.errors:
            raise self.errors.pop(0)
        if method == "chain_getBlockHash":
            return "0x" + int(params[0]).to_bytes(32, "big").hex()
        if method == "state_getStorage":
            block = int(params[1], 16)
            return "0x" + (self.ts(block) * 1000).to_bytes(8, "little").hex()
        if method == "state_getRuntimeVersion":
            return {"specVersion": 419}
        if method == "state_call":
            return "0x00"
        if method == "state_getKeysPaged":
            return []
        if method == "state_queryStorageAt":
            return [{"changes": []}]
        if method == "chain_getFinalizedHead":
            return "0x" + (9000000).to_bytes(32, "big").hex()
        if method == "chain_getHeader":
            return {"number": hex(9000000)}
        raise AssertionError(method)


def make_gateway(chain: FakeChain | None = None, **kw):
    from app.backfill.rpc_client import Endpoint, RpcGateway
    chain = chain or FakeChain()
    subs: list[FakeSubstrate] = []

    async def factory(ep):
        s = FakeSubstrate(chain)
        subs.append(s)
        return s
    sleeps: list[float] = []

    async def sleep(x):
        sleeps.append(x)
    eps = kw.pop("endpoints", None) or [Endpoint("wss://a", "blockmachine", "blockmachine", "blockmachine"),
                                         Endpoint("wss://b", "opentensor_archive", "archive", "archive")]
    gw = RpcGateway(eps, FakeLimiter(), factory=factory, sleep=sleep, **kw)
    gw._fake = {"chain": chain, "subs": subs, "sleeps": sleeps}
    return gw
