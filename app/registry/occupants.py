"""Occupant lifecycle T1–T6 (spec 04 §2.2). One transaction per sync (occupants, subnets, events)."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import app.config as app_config
from app.logging.logger import logger
from app.registry import anomalies
from app.registry.keys import occupant_key
from app.registry.models import OccupantInput
from app.storage.events import emit_event

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

JOB = "registry_occupants"


@dataclass
class SyncResult:
    inserted: list[str] = field(default_factory=list)
    activated: list[str] = field(default_factory=list)
    reoccupied: list[tuple[str, str]] = field(default_factory=list)
    anomalies: list[str] = field(default_factory=list)
    missing: list[int] = field(default_factory=list)
    deregistered: list[str] = field(default_factory=list)
    sane: bool = True
    error: str | None = None

    def counts(self) -> dict:
        return {"inserted": len(self.inserted), "activated": len(self.activated), "reoccupied": len(self.reoccupied),
                "missing": len(self.missing), "deregistered": len(self.deregistered), "anomalies": self.anomalies}


def _active(inp: OccupantInput) -> bool:
    if inp.netuid == 0:
        return True
    if not inp.is_active:
        return False
    return inp.from_table or bool(inp.first_emission_block)


def _reg_est(inp: OccupantInput, head_block: int, observed_at: int) -> int:
    est = int(observed_at - (head_block - inp.network_registered_at) * app_config.settings.BLOCK_SECONDS)
    if inp.registered_at_ts and abs(inp.registered_at_ts - est) <= 7 * 86400:
        return int(inp.registered_at_ts)
    return est


class OccupantTracker:
    def __init__(self, db: "Database", run_id: str, observed_at: int) -> None:
        self.db = db
        self.run_id = run_id
        self.observed_at = observed_at

    async def _emit(self, **kw) -> None:
        await emit_event(self.db, source="registry", run_id=self.run_id, observed_at=self.observed_at, **kw)

    async def sync(self, inputs: list[OccupantInput], head_block: int, *, prior_sane: bool) -> SyncResult:
        db = self.db
        now = self.observed_at
        res = SyncResult()
        s = app_config.settings
        async with db.transaction():
            current = {r["netuid"]: r for r in await db.fetchall(
                "SELECT s.netuid, o.* FROM subnets s JOIN occupants o ON o.occupant_key = s.occupant_key")}
            seen: set[int] = set()
            for inp in inputs:
                seen.add(inp.netuid)
                key = occupant_key(inp.netuid, inp.network_registered_at)
                cur = current.get(inp.netuid)
                status_new = "active" if _active(inp) else "pre_launch"
                if cur is None:                                                   # T1
                    await self._insert(inp, key, status_new, head_block)
                    res.inserted.append(key)
                    if status_new == "active" and prior_sane and inp.netuid != 0:
                        await self._activated(key, inp, head_block, days=0)
                    continue
                elif inp.network_registered_at == cur["network_registered_at"]:     # T2
                    activate = cur["status"] == "pre_launch" and status_new == "active"
                    await db.execute(
                        "UPDATE occupants SET last_seen_block = ?, last_seen_at = ?, name = COALESCE(?, name), "
                        "symbol = COALESCE(?, symbol), missing_runs = 0, "
                        "first_emission_block = COALESCE(first_emission_block, ?), "
                        "status = CASE WHEN ? THEN 'active' ELSE status END, "
                        "activated_at = CASE WHEN ? THEN ? ELSE activated_at END "
                        "WHERE occupant_key = ?",
                        (head_block, now, inp.name, inp.symbol, inp.first_emission_block, int(activate),
                         int(activate), now, key))
                    st = "active" if activate else cur["status"]
                    if cur["status"] == "deregistered":
                        st = status_new
                        # an occupant we had marked absent came back with the same registration
                        await db.execute("UPDATE occupants SET status = ?, deregistered_at = NULL, dereg_reason = NULL "
                                         "WHERE occupant_key = ?", (status_new, key))
                    if activate:
                        res.activated.append(key)
                        days = int((now - (cur["first_seen_at"] or now)) / 86400)
                        await self._activated(key, inp, head_block, days=days)
                    await anomalies.gate(db, job=JOB, reason="reg_block_went_backwards", condition=False, now=now,
                                         run_id=self.run_id, netuid=inp.netuid)
                elif inp.network_registered_at > cur["network_registered_at"]:     # T3
                    old = cur["occupant_key"]
                    await self._insert(inp, key, status_new, head_block, predecessor=old)
                    await db.execute(
                        "UPDATE occupants SET status = 'deregistered', deregistered_at = ?, dereg_reason = 'reoccupied', "
                        "successor_key = ? WHERE occupant_key = ?", (now, key, old))
                    await db.execute("UPDATE watch_channels SET retired_at = ? WHERE occupant_key = ? AND "
                                     "retired_at IS NULL", (now, old))
                    res.reoccupied.append((old, key))
                    est = _reg_est(inp, head_block, now)
                    await self._emit(event_type="netuid_reoccupied", occupant_key=key, netuid=inp.netuid,
                                     severity="warning", event_at=est, dedupe_key=key,
                                     payload={"netuid": inp.netuid, "old_occupant_key": old, "new_occupant_key": key,
                                              "old_network_registered_at": cur["network_registered_at"],
                                              "new_network_registered_at": inp.network_registered_at,
                                              "old_name": cur["name"], "new_name": inp.name,
                                              "old_status_before": cur["status"],
                                              "old_last_seen_block": cur["last_seen_block"],
                                              "head_block": head_block, "new_status": status_new})
                    logger.warning("netuid reoccupied", data={"netuid": inp.netuid, "old": old, "new": key})
                    continue
                else:                                                             # T4
                    ev = await anomalies.gate(db, job=JOB, reason="reg_block_went_backwards", condition=True, now=now,
                                              run_id=self.run_id, netuid=inp.netuid, occupant_key=cur["occupant_key"],
                                              detail={"stored": cur["network_registered_at"],
                                                      "incoming": inp.network_registered_at})
                    res.anomalies.append(f"reg_block_went_backwards:{inp.netuid}")
                    if ev:
                        logger.warning("registry anomaly", data={"netuid": inp.netuid})
                    continue
                if not inp.is_active and cur["status"] == "active" and inp.netuid != 0:   # T6: log only
                    logger.info("is_active flipped false (no status change here)", data={"netuid": inp.netuid})
                await self._write_subnet(inp, key, st, head_block)
            for netuid, cur in current.items():                                  # T5
                if netuid in seen or cur["status"] == "deregistered":
                    await anomalies.gate(db, job=JOB, reason="netuid_absent", condition=False, now=now,
                                         run_id=self.run_id, netuid=netuid)
                    continue
                n = (cur["missing_runs"] or 0) + 1
                res.missing.append(netuid)
                if n >= s.REGISTRY_MISSING_RUNS_TO_DEREG:
                    await db.execute("UPDATE occupants SET missing_runs = ?, status = 'deregistered', "
                                     "dereg_reason = 'absent', deregistered_at = ? WHERE occupant_key = ?",
                                     (n, now, cur["occupant_key"]))
                    await db.execute("UPDATE subnets SET status = 'deregistered', updated_at = ? WHERE netuid = ?",
                                     (now, netuid))
                    await db.execute("UPDATE watch_channels SET retired_at = ? WHERE occupant_key = ? AND "
                                     "retired_at IS NULL", (now, cur["occupant_key"]))
                    res.deregistered.append(cur["occupant_key"])
                else:
                    await db.execute("UPDATE occupants SET missing_runs = ? WHERE occupant_key = ?",
                                     (n, cur["occupant_key"]))
                await anomalies.gate(db, job=JOB, reason="netuid_absent", condition=True, now=now, run_id=self.run_id,
                                     netuid=netuid, occupant_key=cur["occupant_key"], detail={"missing_runs": n})
        return res

    async def _insert(self, inp: OccupantInput, key: str, status: str, head_block: int,
                      predecessor: str | None = None) -> None:
        now = self.observed_at
        await self.db.execute(
            "INSERT OR IGNORE INTO occupants (occupant_key, netuid, network_registered_at, status, name, symbol, "
            "first_seen_block, first_seen_at, last_seen_block, last_seen_at, first_emission_block, activated_at, "
            "predecessor_key, registered_at_est, missing_runs, source) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,'tmc')",
            (key, inp.netuid, inp.network_registered_at, status, inp.name, inp.symbol, head_block, now, head_block, now,
             inp.first_emission_block, now if status == "active" else None, predecessor,
             _reg_est(inp, head_block, now)))
        await self._write_subnet(inp, key, status, head_block)

    async def _write_subnet(self, inp: OccupantInput, key: str, status: str, head_block: int) -> None:
        s = app_config.settings
        immune = int((head_block - inp.network_registered_at) < s.NETWORK_IMMUNITY_PERIOD_BLOCKS) \
            if inp.netuid != 0 else 0
        await self.db.execute(
            "INSERT INTO subnets (netuid, occupant_key, network_registered_at, name, symbol, status, is_active, immune, "
            "owner_coldkey, owner_hotkey, head_block, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(netuid) DO UPDATE SET occupant_key = excluded.occupant_key, "
            "network_registered_at = excluded.network_registered_at, name = COALESCE(excluded.name, subnets.name), "
            "symbol = COALESCE(excluded.symbol, subnets.symbol), status = excluded.status, "
            "is_active = excluded.is_active, immune = excluded.immune, "
            "owner_coldkey = COALESCE(excluded.owner_coldkey, subnets.owner_coldkey), "
            "owner_hotkey = COALESCE(excluded.owner_hotkey, subnets.owner_hotkey), "
            "head_block = excluded.head_block, updated_at = excluded.updated_at",
            (inp.netuid, key, inp.network_registered_at, inp.name, inp.symbol, status, int(bool(inp.is_active)), immune,
             inp.owner_coldkey, inp.owner_hotkey, head_block, self.observed_at))

    async def _activated(self, key: str, inp: OccupantInput, head_block: int, *, days: int) -> None:
        await self._emit(event_type="occupant_activated", occupant_key=key, netuid=inp.netuid, severity="info",
                         event_at=self.observed_at, dedupe_key=key,
                         payload={"netuid": inp.netuid, "occupant_key": key,
                                  "first_emission_block": inp.first_emission_block, "head_block": head_block,
                                  "days_pre_launch": days})


async def ensure_occupants_fresh(db: "Database", run, max_age_h: float = 12) -> bool:
    """Run the occupant sync inline when no sane run happened in ``max_age_h`` (identity job step 2)."""
    last = await db.fetchval("SELECT MAX(updated_at) FROM subnets")
    if last and run.started_at - int(last) < max_age_h * 3600:
        return False
    from app.registry.jobs import sync_occupants
    await sync_occupants(run)
    return True


def dumps(x) -> str:
    return json.dumps(x, separators=(",", ":"), default=str)
