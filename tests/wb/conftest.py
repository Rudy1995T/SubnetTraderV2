"""Fixtures for the workbench tests (spec 19 §8): a temp DB with occupants, bars and a chain snapshot."""
from __future__ import annotations

import math

import pytest

HOUR, DAY = 3600, 86400
T0 = 1_792_108_800          # 2026-10-16 00:00Z (an official slot)


async def add_occupant(db, netuid: int, reg: int, *, status: str = "active", name: str | None = None,
                       github: str | None = None, registered_ts: int | None = None) -> str:
    occ = f"{netuid}:{reg}"
    await db.execute(
        "INSERT INTO occupants (occupant_key, netuid, network_registered_at, status, name, first_seen_block, "
        "first_seen_at, last_seen_block, last_seen_at, registered_ts) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (occ, netuid, reg, status, name or f"sn{netuid}", reg, 0, reg, T0, registered_ts))
    if status != "deregistered":
        await db.execute(
            "INSERT OR REPLACE INTO subnets (netuid, occupant_key, network_registered_at, name, status, github_url, "
            "updated_at) VALUES (?,?,?,?,?,?,?)", (netuid, occ, reg, name or f"sn{netuid}", status, github, T0))
    return occ


async def add_bars(db, netuid: int, occ: str, period: str, closes: list[float], *, end_ts: int,
                   mps: list[float | None] | None = None, blocks: list[int] | None = None,
                   skip: set[int] | None = None, liq: float | None = None) -> None:
    ps = {"5m": 300, "1h": HOUR, "1d": DAY}[period]
    n = len(closes)
    rows = []
    for i, c in enumerate(closes):
        if skip and i in skip:
            continue
        ts = end_ts - (n - 1 - i) * ps
        rows.append((netuid, period, ts, occ, c, c, c, c, None if mps is None else mps[i],
                     None if blocks is None else blocks[i], liq, "sse_rollup", ts + ps, ts))
    await db.executemany(
        "INSERT INTO price_bars (netuid, period, ts, occupant_key, open, high, low, close, moving_price, block_number, "
        "tao_in_pool, source, observed_at, ts_last) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)


async def add_snapshot(db, hour_ts: int, rows: list[dict]) -> None:
    """chain_hourly live rows: dicts with netuid, occ, price, mp, reg, block, is_active (1)."""
    for r in rows:
        await db.execute(
            "INSERT INTO chain_hourly (netuid, hour_ts, occupant_key, block_number, event_at, observed_at, source, "
            "is_active, emission_enabled, price, subnet_moving_price, network_registered_at, tao_liquidity, emission, "
            "commits_30d) VALUES (?,?,?,?,?,?, 'sse', ?, 1, ?, ?, ?, ?, ?, ?)",
            (r["netuid"], hour_ts, r["occ"], r["block"], hour_ts + HOUR - 5, hour_ts + HOUR, r.get("is_active", 1),
             r["price"], r.get("mp"), r["reg"], r.get("tao", 1e12), r.get("emission", 1.0), r.get("commits")))


def wave(n: int, base: float = 1.0, amp: float = 0.05, period: float = 40.0, drift: float = 0.0) -> list[float]:
    return [base * math.exp(drift * i + amp * math.sin(2 * math.pi * i / period)) for i in range(n)]


@pytest.fixture
def wb_settings(settings_override):
    def _apply(**kw):
        return settings_override(WB_ENABLED=True, **kw)
    return _apply
