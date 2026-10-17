"""Keyless chain constants probe (spec 19 §2.4): the dereg/registration constants from the public finney entrypoint
over HTTPS JSON-RPC (provider ``subtensor_rpc``, every step under ``WB_CHAIN_PROBE_TIMEOUT_SEC``) → ``chain_constants``
(``source='rpc_storage'``) and 05's ``chain_constant_drift`` event.

CLI: ``python -m app.collectors.chain_probe`` prints the live values without writing.
"""
from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.collectors.chain_rpc import decode, http_url, storage_key
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.registry.models import RpcError
from app.registry.sources.chain_rpc import SubtensorRpc
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.http import Http
    from app.storage.db import Database

# storage item → decoder (SubtensorModule plain values)
ITEMS: dict[str, str] = {
    "NetworkImmunityPeriod": "u64",
    "SubnetLimit": "u16",
    "NetworkRateLimit": "u64",
    "NetworkMinLockCost": "u64",
    "NetworkLastLockCost": "u64",
    "NetworkLockReductionInterval": "u64",
    "SubnetMovingAlpha": "i96f32",
}
LAST_REGISTERED_SUFFIX = "02"   # LastRateLimitedBlock[RateLimitKey::NetworkLastRegistered] (unit variant index 2)
COST_API = "SubnetRegistrationRuntimeApi_get_network_registration_cost"
# constants whose change from the previous probe is itself drift (config = previous value)
WATCH_PREVIOUS = ("SubnetLimit", "NetworkRateLimit", "NetworkMinLockCost")


class ChainProbe:
    def __init__(self, http: "Http", url: str, timeout: float) -> None:
        self.url = http_url(url)
        self.endpoint = url
        self.timeout = float(timeout)
        self.rpc = SubtensorRpc([self.url], http, self.timeout)

    async def call(self, method: str, params: list) -> Any:
        try:
            return await asyncio.wait_for(self.rpc.call(self.url, method, params), timeout=self.timeout)
        except asyncio.TimeoutError as exc:
            raise RpcError(f"{method}: timeout after {self.timeout:.0f}s") from exc

    async def read(self) -> tuple[int, dict[str, float | int | None]]:
        at = await self.call("chain_getFinalizedHead", [])
        hdr = await self.call("chain_getHeader", [at])
        block = int(hdr["number"], 16)
        out: dict[str, float | int | None] = {}
        for name, kind in ITEMS.items():
            out[name] = decode(kind, await self.call("state_getStorage", [storage_key(name), at]))
        lr = await self.call("state_getStorage", [storage_key("LastRateLimitedBlock") + LAST_REGISTERED_SUFFIX, at])
        out["NetworkLastRegisteredBlock"] = decode("u64", lr)
        cost = await self.call("state_call", [COST_API, "0x", at])
        out["NetworkRegistrationCost"] = decode("u64", cost)
        return block, out


async def previous_values(db: "Database") -> dict[str, str]:
    rows = await db.fetchall(
        "SELECT name, value FROM chain_constants c WHERE source = 'rpc_storage' AND observed_at = (SELECT "
        "MAX(observed_at) FROM chain_constants WHERE source = 'rpc_storage' AND name = c.name)", writer=True)
    return {r["name"]: r["value"] for r in rows}


async def store(db: "Database", block: int, vals: dict[str, Any], *, endpoint: str, observed_at: int,
                run_id: str) -> tuple[int, int]:
    from app.storage.events import emit_event
    s = app_config.settings
    prev = await previous_values(db)
    recs = [(k, observed_at, str(v), block, endpoint, observed_at, run_id) for k, v in vals.items() if v is not None]
    await db.executemany(
        "INSERT OR IGNORE INTO chain_constants (name, source, observed_at, value, block_number, endpoint, event_at, "
        "run_id) VALUES (?, 'rpc_storage', ?, ?, ?, ?, ?, ?)", recs)
    drift: list[tuple[str, Any, Any]] = []
    imm = vals.get("NetworkImmunityPeriod")
    if imm is not None and int(imm) != int(s.NETWORK_IMMUNITY_PERIOD_BLOCKS):
        drift.append(("NetworkImmunityPeriod", s.NETWORK_IMMUNITY_PERIOD_BLOCKS, int(imm)))
    for name in WATCH_PREVIOUS:
        live, old = vals.get(name), prev.get(name)
        if live is not None and old is not None and str(live) != old:
            drift.append((name, old, live))
    n = 0
    for name, cfg, live in drift:
        eid = await emit_event(db, event_type="chain_constant_drift", occupant_key=None, netuid=None,
                               event_at=observed_at, observed_at=observed_at,
                               payload={"name": name, "config": cfg, "live": live, "block": block,
                                        "detected_by": "chain_constants_probe"},
                               dedupe_key=f"{name}|{live}", source="chain", run_id=run_id)
        n += eid is not None
    return len(recs), n


class ChainConstantsProbeJob(CollectorJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        s = run.settings
        probe = ChainProbe(run.rt.http, s.WB_CHAIN_PROBE_URL, s.WB_CHAIN_PROBE_TIMEOUT_SEC)
        block, vals = await probe.read()
        rows, ev = await store(run.rt.db, block, vals, endpoint=s.WB_CHAIN_PROBE_URL, observed_at=run.started_at,
                               run_id=run.run_id)
        imm = vals.get("NetworkImmunityPeriod")
        cost = vals.get("NetworkRegistrationCost")
        return JobOutcome(rows=rows, note=f"block={block} imm={imm} lock_cost={cost / 1e9 if cost else None:.2f}τ "
                                          f"last_reg={vals.get('NetworkLastRegisteredBlock')} drift_events={ev}",
                          meta={"block": block, "values": vals})


async def _probe() -> int:
    from app.http import Http
    from app.ratelimit import ProviderLimiter
    s = app_config.settings
    http = Http(ProviderLimiter.from_settings())
    try:
        block, vals = await ChainProbe(http, s.WB_CHAIN_PROBE_URL, s.WB_CHAIN_PROBE_TIMEOUT_SEC).read()
        print(f"endpoint={s.WB_CHAIN_PROBE_URL} block={block} (read at {now_ts()})")
        for k, v in vals.items():
            print(f"{k}={v}")
    finally:
        await http.aclose()
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(asyncio.run(_probe()))
