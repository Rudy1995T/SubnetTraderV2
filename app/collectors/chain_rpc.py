"""Optional subtensor RPC collector (spec 05 §6.1, off by default: ``SUBTENSOR_RPC_ENABLED``).

``jsonrpc`` mode only (v1 forbids ``bittensor``/``substrate-interface``, 01 §2.3): ``state_getStorage`` on
``twox128(pallet) + twox128(item)`` [+ u16 LE netuid, Identity hasher — UNVERIFIED for ``MinerBurned`` and
``SubnetEmissionEnabled``]. The metagraph runtime-API decoder is not built (BUILD-LOG), so ``chain_rpc_metagraph``
records a skip and ``participation`` stays ``failed/no_metagraph_source``.

``python -m app.collectors.chain_rpc probe`` prints the constants and ``MinerBurned[95]``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.collectors.endpoints import EndpointSelector, is_dead_socket_error, is_quota_error
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.registry.models import RpcError
from app.registry.sources.chain_rpc import SubtensorRpc, twox128
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.http import Http

PALLET = "SubtensorModule"
# name → (decoder, config key it is checked against or None)
CONSTANTS: dict[str, tuple[str, str | None]] = {
    "NetworkImmunityPeriod": ("u64", "NETWORK_IMMUNITY_PERIOD_BLOCKS"),
    "EmissionBarQuantile": ("u64", None),   # UNVERIFIED type
    "EmissionGateExponent": ("u64", None),  # UNVERIFIED type
    "SubnetOwnerCut": ("u16", None),
    "SubnetLimit": ("u16", None),
    "TotalNetworks": ("u16", None),
}
SUBNET_MAPS = {"MinerBurned": "u96f32", "SubnetEmissionEnabled": "bool", "SubnetMovingPrice": "i96f32"}


def http_url(u: str) -> str:
    return "https://" + u[6:] if u.startswith("wss://") else "http://" + u[5:] if u.startswith("ws://") else u


def storage_key(item: str, netuid: int | None = None) -> str:
    k = twox128(PALLET) + twox128(item)
    if netuid is not None:
        k += int(netuid).to_bytes(2, "little")
    return "0x" + k.hex()


def decode(kind: str, value_hex: str | None) -> float | int | bool | None:
    if value_hex is None:
        return None
    b = bytes.fromhex(value_hex[2:] if value_hex.startswith("0x") else value_hex)
    if kind == "bool":
        return bool(b[0]) if b else None
    if kind in ("u16", "u64"):
        return int.from_bytes(b, "little")
    if kind in ("u96f32", "i96f32"):
        raw = int.from_bytes(b[:16], "little", signed=(kind == "i96f32"))
        return raw / 2**32
    raise ValueError(f"unknown decoder {kind}")


@dataclass
class MetagraphStats:
    n_uids: int | None = None
    max_uids: int | None = None
    active_validators: int | None = None
    active_miners: int | None = None
    permit_count: int | None = None
    vtrust_mean: float | None = None
    vtrust_pstdev: float | None = None
    incentive_to_owner_share: float | None = None
    block_number: int | None = None


class RpcCollector:
    def __init__(self, http: "Http", selector: EndpointSelector, timeout: float) -> None:
        self.rpc = SubtensorRpc([], http, timeout)
        self.selector = selector
        self.endpoint: str | None = None

    async def call(self, method: str, params: list) -> Any:
        last: Exception | None = None
        for u in self.selector.candidates():
            try:
                res = await self.rpc.call(http_url(u), method, params)
                self.selector.mark_ok(u)
                self.endpoint = u
                return res
            except Exception as exc:  # RpcError / ProviderError / httpx errors
                last = exc
                if is_quota_error(exc) or is_dead_socket_error(exc) or isinstance(exc, RpcError):
                    self.selector.mark_bad(u, exc)
                    continue
                raise
        raise RpcError(f"all endpoints failed: {last}")

    async def head(self) -> tuple[str, int]:
        h = await self.call("chain_getFinalizedHead", [])
        hdr = await self.call("chain_getHeader", [h])
        return h, int(hdr["number"], 16)

    async def read_constants(self, at: str) -> dict[str, Any]:
        out = {}
        for name, (kind, _) in CONSTANTS.items():
            out[name] = decode(kind, await self.call("state_getStorage", [storage_key(name), at]))
        return out

    async def read_subnet_scalars(self, netuids: list[int], at: str) -> dict[int, dict]:
        out: dict[int, dict] = {}
        for n in netuids:
            d = {}
            for item, kind in SUBNET_MAPS.items():
                d[item] = decode(kind, await self.call("state_getStorage", [storage_key(item, n), at]))
            out[n] = d
        return out


def _selector(s) -> EndpointSelector:
    return EndpointSelector(list(s.SUBTENSOR_RPC_ENDPOINTS), s.SUBTENSOR_ENDPOINT_COOLDOWN_SEC)


class ChainRpcHourlyJob(CollectorJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        from app.storage.events import emit_event
        s = run.settings
        db = run.rt.db
        now = run.started_at
        rc = RpcCollector(run.rt.http, _selector(s), s.SUBTENSOR_RPC_READ_TIMEOUT_SEC)
        at, block = await rc.head()
        consts = await rc.read_constants(at)
        await db.executemany(
            "INSERT OR IGNORE INTO chain_constants (name, source, observed_at, value, block_number, endpoint, event_at,"
            " run_id) VALUES (?, 'rpc_storage', ?, ?, ?, ?, ?, ?)",
            [(k, now, str(v), block, rc.endpoint, now, run.run_id) for k, v in consts.items() if v is not None])
        for name, (_, cfg_key) in CONSTANTS.items():
            live = consts.get(name)
            if cfg_key and live is not None and int(live) != int(getattr(s, cfg_key)):
                await emit_event(db, event_type="chain_constant_drift", occupant_key=None, netuid=None, event_at=now,
                                 observed_at=now, payload={"name": name, "config": getattr(s, cfg_key), "live": live,
                                                           "block": block},
                                 dedupe_key=f"{name}|{live}", source="chain", run_id=run.run_id)
        slot = now - now % 21600
        rows = 0
        if now - slot < 3600:
            occs = await db.fetchall("SELECT o.netuid, o.occupant_key FROM subnets s JOIN occupants o "
                                     "USING (occupant_key) WHERE o.netuid != 0 ORDER BY o.netuid", writer=True)
            vals = await rc.read_subnet_scalars([o["netuid"] for o in occs], at)
            cut = consts.get("SubnetOwnerCut")
            recs = []
            for o in occs:
                v = vals.get(o["netuid"], {})
                en = v.get("SubnetEmissionEnabled")
                recs.append((o["netuid"], slot, o["occupant_key"], block, now, now, run.run_id, v.get("MinerBurned"),
                             cut / 65535 if cut is not None else None,
                             1 if en is None else int(en),  # only explicit False entries exist; fail open
                             v.get("SubnetMovingPrice"), rc.endpoint))
            await db.executemany(
                "INSERT OR IGNORE INTO rpc_subnet_daily (netuid, as_of, occupant_key, block_number, observed_at, "
                "event_at, source, run_id, miner_burned, subnet_owner_cut, emission_enabled, moving_price, endpoint) "
                "VALUES (?,?,?,?,?,?,'rpc_storage',?,?,?,?,?,?)", recs)
            rows = len(recs)
        return JobOutcome(rows=rows, note=f"block={block} endpoint={rc.endpoint} constants={len(consts)} "
                                          f"subnet_rows={rows}")


class ChainRpcMetagraphJob(CollectorJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        logger.info("chain_rpc_metagraph: runtime-API metagraph decoder not built in v1; participation stays "
                    "failed/no_metagraph_source")
        return JobOutcome(rows=0, note="skipped: metagraph decoder not built (BUILD-LOG)")


async def _probe() -> int:
    from app.http import Http
    from app.ratelimit import ProviderLimiter
    s = app_config.settings
    http = Http(ProviderLimiter.from_settings())
    try:
        rc = RpcCollector(http, _selector(s), s.SUBTENSOR_RPC_READ_TIMEOUT_SEC)
        at, block = await rc.head()
        print(f"endpoint={rc.endpoint} block={block}")
        for k, v in (await rc.read_constants(at)).items():
            print(f"{k}={v}")
        print(f"MinerBurned[95]={(await rc.read_subnet_scalars([95], at))[95]}")
        print(f"(read at {now_ts()})")
    finally:
        await http.aclose()
    return 0


if __name__ == "__main__":
    import asyncio
    import sys
    if sys.argv[1:2] != ["probe"]:
        print("usage: python -m app.collectors.chain_rpc probe")
        sys.exit(2)
    sys.exit(asyncio.run(_probe()))
