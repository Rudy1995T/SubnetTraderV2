"""Spec 11 test helpers: seeding occupants, identity rows, chain_hourly, owner snapshots / stake events / changes."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import app.config as app_config
from app.util.time import parse_iso

FIX = Path(__file__).resolve().parent / "fixtures"
DAY = 86400
HOUR = 3600
T0 = parse_iso("2026-10-01T00:07:00Z")
OWNER = "5FRYKhbmfXPDoHdUUDMx27E3HuMvAzwjzFMMq3rNurUhAyS9"
OWNER_HK = "5CS3g6nVJM6ouns8n9buN9CzFf2C1YDHVcVGRcxoirKs2xbV"
OTHER = "5F6D1yTyQDwqR8Hjawq733WSnZVpH3X3W2aQhAWyCZq47nrf"
NEWCK = "5Hf5DCoutc7oKxjpvVr6YvhNmBimiw7YsMwaBH4htSNFJZYA"
NEWHK = "5C4xjnQkyK" + "a" * 38


def fx_json(name: str) -> Any:
    return json.loads((FIX / name).read_text())


def fx_text(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


def integrity_settings(**kw):
    base = dict(OWNER_ENABLED=True, INTEGRITY_FEATURES_ENABLED=True, TEAM_SEED_ENABLED=True, HTTP_MAX_RETRIES=0,
                TMC_MAX_RETRIES=0, TAOSTATS_ENABLED=False)
    base.update(kw)
    return app_config.Settings(_env_file=None, **base)


@pytest.fixture
def iset(monkeypatch):
    def _apply(**kw):
        s = integrity_settings(**kw)
        monkeypatch.setattr(app_config, "settings", s)
        return s
    _apply()
    return _apply


async def seed_occ(db, netuid: int, *, reg: int = 4531295, status: str = "active", reg_ts: int | None = None,
                   first_emission_block: int | None = 4600000, owner_coldkey: str | None = OWNER,
                   last_seen_block: int | None = None, name: str | None = None) -> dict:
    okey = f"{netuid}:{reg}"
    reg_ts = reg_ts if reg_ts is not None else parse_iso("2024-12-21T16:53:12Z")
    await db.execute("INSERT OR IGNORE INTO occupants (occupant_key, netuid, network_registered_at, status, name, "
                     "first_seen_block, first_seen_at, last_seen_block, last_seen_at, first_emission_block, "
                     "registered_at_est, registered_ts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                     (okey, netuid, reg, status, name or f"SN{netuid}", reg, reg_ts, last_seen_block or reg + 10,
                      T0, first_emission_block, reg_ts, reg_ts))
    await db.execute("INSERT OR REPLACE INTO subnets (netuid, occupant_key, network_registered_at, name, status, "
                     "owner_coldkey, updated_at) VALUES (?,?,?,?,?,?,?)",
                     (netuid, okey, reg, name or f"SN{netuid}", status, owner_coldkey, T0))
    return dict(await db.fetchone("SELECT o.*, s.owner_coldkey FROM occupants o JOIN subnets s USING (occupant_key) "
                                  "WHERE o.occupant_key = ?", (okey,)))


async def seed_team_info(db, okey: str, html: str, *, observed_at: int, event_at: int | None = None,
                         source_ref: str = "https://subnetalpha.ai/subnet/x/") -> None:
    import hashlib
    netuid = int(okey.split(":")[0])
    await db.execute("INSERT INTO identity_observations (occupant_key, netuid, source, field, value, value_hash, "
                     "observed_at, event_at, run_id, source_ref) VALUES (?,?,?,?,?,?,?,?,?,?)",
                     (okey, netuid, "subnetalpha", "team_info_html", html, hashlib.sha256(html.encode()).hexdigest(),
                      observed_at, event_at if event_at is not None else observed_at, "t", source_ref))


async def seed_hourly(db, okey: str, netuid: int, hour_ts: int, *, source: str = "sse", perp: float | None = None,
                      decay: float | None = 0.0, locked: float | None = None, stake: float | None = None,
                      tao_liq: float = 300_000.0, price: float | None = 0.05, owner: str | None = OWNER,
                      block: int | None = 9_000_000, total_stake: float = 2_000_000.0, observed_at: int | None = None):
    r9 = lambda x: None if x is None else x * 1e9  # noqa: E731
    if locked is None and perp is not None:
        locked = perp + (decay or 0.0)
    await db.execute(
        "INSERT OR REPLACE INTO chain_hourly (netuid, hour_ts, occupant_key, block_number, event_at, observed_at, "
        "source, price, owner_personal_stake, owner_personal_locked_alpha, owner_personal_perpetual_alpha, "
        "owner_personal_decaying_alpha, total_subnet_stake, tao_liquidity, subnet_owner) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (netuid, hour_ts, okey, block, hour_ts, observed_at or hour_ts, source, price, r9(stake),
         r9(locked), None if source == "analytics_1w" else r9(perp), r9(decay), r9(total_stake), r9(tao_liq), owner))


async def seed_snapshot(db, okey: str, netuid: int, observed_at: int, **cols) -> None:
    row = {"occupant_key": okey, "netuid": netuid, "observed_at": observed_at, "event_at": observed_at,
           "source": "owner_collect", "run_id": "t", "owner_coldkey": OWNER, "owner_hotkey": OWNER_HK,
           "keys_source": "tmc_list", "fetch_status_json": "{}", "raw_json": "{}"}
    row.update(cols)
    keys = list(row)
    await db.execute(f"INSERT INTO owner_snapshots ({', '.join(keys)}) VALUES ({','.join('?' * len(keys))})",
                     [row[k] for k in keys])


async def seed_stake(db, okey: str, netuid: int, uid: str, *, kind: str, alpha: float, event_at: int,
                     tao: float | None = None, observed_at: int | None = None, outflow: int | None = None,
                     source: str = "tmc_staking_activity", owner: str = OWNER, dest: str | None = None,
                     block: int | None = None, extrinsic: str | None = None, success: int = 1) -> None:
    from app.integrity.classify import OUTFLOW_KINDS
    await db.execute(
        "INSERT INTO owner_stake_events (event_uid, occupant_key, netuid, owner_coldkey, function, kind, "
        "counts_as_outflow, from_netuid, alpha, tao, success, block_number, extrinsic_id, event_at, observed_at, "
        "source, run_id, dest_coldkey) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (uid, okey, netuid, owner, kind, kind, outflow if outflow is not None else int(kind in OUTFLOW_KINDS), netuid,
         alpha, tao, success, block, extrinsic, event_at, observed_at or event_at, source, "t", dest))


async def seed_coverage(db, coldkey: str, *, start: int, ts: int) -> None:
    await db.execute("INSERT OR REPLACE INTO job_state (job, key, value_json, updated_at) VALUES "
                     "('owner_collect_daily', ?, ?, ?)",
                     (f"activity_wm:{coldkey}", json.dumps({"ts": ts, "coverage_start": start, "last_ok": ts}), ts))
