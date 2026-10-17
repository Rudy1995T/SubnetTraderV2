"""Archive-RPC gateway for the backfill (spec 17 §2.3).

Transport and SCALE decoding use ``async-substrate-interface`` 2.x (imported lazily; never ``bittensor``). Every
JSON-RPC request the gateway sends first takes one token from 01's process-wide ``ProviderLimiter`` for the
endpoint's provider and, with a ``SpendBook``, is booked to ``provider_spend.requests`` (02 §G1); ASI's ``runtime_call`` is never used on the hot path (it issues un-metered internal calls), and a
runtime load pre-acquires ``RUNTIME_LOAD_TOKENS``. The key-bearing URL is never logged (``Endpoint.label``).

Endpoint order (BUILD-LOG Phase 3): Blockmachine (operator's paid archive, if keyed) → keyed OnFinality → public
OnFinality → opentensor archive.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Awaitable, Callable

import xxhash

from app.collectors.endpoints import EndpointSelector, is_dead_socket_error, is_quota_error
from app.logging.logger import logger

if TYPE_CHECKING:  # pragma: no cover
    from app.backfill.common import SpendBook
    from app.config import Settings
    from app.ratelimit import ProviderLimiter, TokenBucket

RUNTIME_LOAD_TOKENS = 6
CONNECT_TOKENS = 8           # ASI initialize: chain head + the startup runtime load at head
TIMESTAMP_NOW_KEY = "0xf0c365c3cf59d671eb72da0e7a4113c49f1f0515f462cdcf84e0f1d6045dfcbb"
_QUOTA_EXTRA = ("-32004", "historical work rate limit", "budget_exhausted")


# ── errors ───────────────────────────────────────────────────────────────────────────────────────────────────
class RpcError(RuntimeError):
    pass


class RateLimited(RpcError):
    pass


class DeadSocket(RpcError):
    pass


class DecodeError(RpcError):
    pass


class NoRuntimeApi(RpcError):
    pass


def _really_cancelled() -> bool:
    """True when *this* task is being cancelled (vs a CancelledError leaking from a shared ASI future)."""
    t = asyncio.current_task()
    return bool(t is not None and t.cancelling())


def quota_error(exc: BaseException | str) -> bool:
    s = str(exc).lower()
    return is_quota_error(exc) or any(k in s for k in _QUOTA_EXTRA)


def missing_api_error(exc: BaseException | str) -> bool:
    s = str(exc).lower()
    return ("exported method" in s and "not found" in s) or "is not found" in s or "not found in registry" in s


# ── keys & fixed point ───────────────────────────────────────────────────────────────────────────────────────
def twox128(name: str) -> bytes:
    """``xxh64(seed 0) ‖ xxh64(seed 1)``, each little-endian (same prefix as 04/05's storage keys)."""
    b = name.encode()
    return (xxhash.xxh64(b, seed=0).intdigest().to_bytes(8, "little")
            + xxhash.xxh64(b, seed=1).intdigest().to_bytes(8, "little"))


def storage_prefix(pallet: str, item: str) -> str:
    return "0x" + (twox128(pallet) + twox128(item)).hex()


def storage_key(pallet: str, item: str, netuid: int | None = None) -> str:
    """Identity hasher: ``twox128(pallet) ‖ twox128(item) ‖ u16_le(netuid)``."""
    k = storage_prefix(pallet, item)
    return k if netuid is None else k + int(netuid).to_bytes(2, "little").hex()


def netuid_from_key(key: str) -> int:
    """Last 2 key bytes (u16 LE) — valid for Identity and ``*Concat`` hashers alike."""
    return int.from_bytes(bytes.fromhex(key[-4:]), "little")


def fixed_to_float(bits: int, frac_bits: int) -> float:
    return int(bits) / float(1 << frac_bits)


def le_int(hex_value: str | None) -> int | None:
    if hex_value is None:
        return None
    h = hex_value[2:] if hex_value.startswith("0x") else hex_value
    return int.from_bytes(bytes.fromhex(h), "little") if h else None


# ── endpoints ────────────────────────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Endpoint:
    url: str          # may carry a key — never log it
    provider: str     # limiter provider: blockmachine | onfinality | opentensor_archive
    kind: str         # blockmachine | onfinality | onfinality_public | archive
    label: str        # safe to log / store

    @property
    def source(self) -> str:
        """Row provenance (``pool_history.source`` / ``price_bars.source``)."""
        return {"blockmachine": "blockmachine", "archive": "opentensor_archive"}.get(self.kind, "onfinality")

    @property
    def occupant_source(self) -> str:
        """``occupants.source`` vocabulary (03 CHECK: tmc|rpc|legacy_ledger|onfinality|manual)."""
        return "onfinality" if self.kind in ("onfinality", "onfinality_public") else "rpc"


def _classify(url: str, s: "Settings") -> Endpoint:
    low = url.lower()
    base = url.split("?")[0]
    if "blockmachine" in low:
        if "authorization=" not in low and s.BLOCKMACHINE_API_KEY:
            url = f"{base}?authorization={s.BLOCKMACHINE_API_KEY}"
        return Endpoint(url, "blockmachine", "blockmachine", "blockmachine")
    if "onfinality" in low:
        keyed = "apikey=" in low
        return Endpoint(url, "onfinality", "onfinality" if keyed else "onfinality_public",
                        "onfinality" + ("" if keyed else "_public"))
    return Endpoint(url, "opentensor_archive", "archive", base.replace("wss://", ""))


def endpoints_from_settings(s: "Settings", override: str | None = None) -> list[Endpoint]:
    if override:
        return [_classify(override, s)]
    if s.BACKFILL_RPC_ENDPOINTS:
        return [_classify(u, s) for u in s.BACKFILL_RPC_ENDPOINTS]
    out: list[Endpoint] = []
    if s.BLOCKMACHINE_API_KEY:
        out.append(_classify(s.BLOCKMACHINE_WS_URL, s))
    if s.ONFINALITY_API_KEY:
        out.append(Endpoint(f"wss://bittensor-finney.api.onfinality.io/ws?apikey={s.ONFINALITY_API_KEY}",
                            "onfinality", "onfinality", "onfinality"))
    out.append(Endpoint(s.ONFINALITY_WS_URL, "onfinality", "onfinality_public", "onfinality_public"))
    out.append(Endpoint(s.ARCHIVE_WS_URL, "opentensor_archive", "archive", "archive.chain.opentensor.ai"))
    return out


# ── gateway ──────────────────────────────────────────────────────────────────────────────────────────────────
SubstrateFactory = Callable[[Endpoint], Awaitable[Any]]


async def _asi_factory(ep: Endpoint) -> Any:
    from async_substrate_interface.async_substrate import AsyncSubstrateInterface
    sub = AsyncSubstrateInterface(ep.url, ss58_format=42, chain_name="Bittensor", max_retries=2,
                                  retry_timeout=30.0)
    await sub.initialize()
    return sub


class RpcGateway:
    def __init__(self, endpoints: list[Endpoint], limiter: "ProviderLimiter", *, timeout: float = 45.0,
                 max_attempts: int = 6, backoff_max: int = 120, cooldown_sec: int = 300,
                 public_max_rps: float = 4.0, factory: SubstrateFactory | None = None,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
                 spend: "SpendBook | None" = None) -> None:
        if not endpoints:
            raise ValueError("RpcGateway needs at least one endpoint")
        self.endpoints = list(endpoints)
        self._by_url = {e.url: e for e in self.endpoints}
        self.selector = EndpointSelector([e.url for e in self.endpoints], cooldown_sec)
        self.limiter = limiter
        self.spend = spend
        if spend is not None:
            spend.seed(limiter, [e.provider for e in self.endpoints])
        self.timeout = float(timeout)
        self.max_attempts = int(max_attempts)
        self.backoff_max = int(backoff_max)
        self._factory = factory or _asi_factory
        self._sleep = sleep
        self._sub: Any = None
        self._ep: Endpoint | None = None
        self._runtimes: dict[tuple[str, int], Any] = {}
        self._conn_lock = asyncio.Lock()
        self._gen = 0
        self._rt_tasks: dict[tuple[str, int], asyncio.Future] = {}
        self._public_bucket: "TokenBucket | None" = None
        self._public_rps = public_max_rps
        self.requests_sent = 0          # socket-level (ASI internals included)
        self.tokens_acquired = 0
        self.by_method: dict[str, int] = {}
        self.latency_ms: dict[str, list[float]] = {}
        self.throttled = 0

    # ── connection ─────────────────────────────────────────────────────────────────────────────────────────
    # Workers share one connection. ``_gen`` increments on every new connection, so a worker that saw a socket die
    # reconnects only if that same connection is still current (never closes a fresh one under the others), and
    # callers hold a local reference to the substrate they send on.
    @property
    def endpoint(self) -> Endpoint:
        if self._ep is None:
            return self._by_url[self.selector.current()]
        return self._ep

    async def _current(self) -> tuple[Any, int, Endpoint]:
        async with self._conn_lock:
            if self._sub is None:
                ep = self._by_url[self.selector.current()]
                await self._acquire(ep, CONNECT_TOKENS)
                sub = await asyncio.wait_for(self._factory(ep), timeout=self.timeout)
                ws = getattr(sub, "ws", None)
                if ws is not None and hasattr(ws, "send"):
                    orig = ws.send

                    async def _counted(*a: Any, **k: Any) -> Any:
                        self.requests_sent += 1
                        return await orig(*a, **k)
                    ws.send = _counted
                self._sub, self._ep = sub, ep
                self._gen += 1
                logger.info("rpc connected", data={"endpoint": ep.label, "gen": self._gen})
            return self._sub, self._gen, self._ep

    async def connect(self) -> None:
        await self._current()

    async def _drop(self, gen: int | None = None) -> None:
        """Close the current connection (only if it is still generation ``gen`` when given)."""
        async with self._conn_lock:
            if self._sub is None or (gen is not None and gen != self._gen):
                return
            sub, self._sub = self._sub, None
        try:
            await asyncio.wait_for(sub.close(), timeout=10)
        except Exception:  # pragma: no cover - best effort
            pass

    async def close(self) -> None:
        await self._drop(None)
        if self.spend is not None:
            self.spend.flush()

    async def reconnect(self, gen: int | None = None) -> None:
        await self._drop(gen)
        await self._current()

    async def fail_over(self, reason: str, *, from_label: str | None = None) -> Endpoint:
        """``mark_bad`` the current endpoint (cooldown) and move to the next candidate. With ``from_label``, a no-op
        when another worker already moved off that endpoint."""
        cur = self.endpoint
        if from_label is not None and cur.label != from_label:
            return cur
        self.selector.mark_bad(cur.url, reason)
        await self._drop(None)
        self._ep = None
        self._runtimes = {k: v for k, v in self._runtimes.items() if k[0] != cur.url}
        nxt = self.endpoint
        logger.warning("rpc endpoint failover", data={"from": cur.label, "to": nxt.label, "reason": reason[:200]})
        return nxt

    # ── metering ───────────────────────────────────────────────────────────────────────────────────────────
    async def _acquire(self, ep: Endpoint, n: int = 1) -> None:
        from app.ratelimit import TokenBucket
        if ep.kind == "onfinality_public":
            if self._public_bucket is None:
                self._public_bucket = TokenBucket(self._public_rps, 1)
            for _ in range(n):
                await self._public_bucket.acquire()
        for _ in range(n):
            await self.limiter.acquire(ep.provider)
            self.limiter.count(ep.provider)
            if self.spend is not None:
                self.spend.add(ep.provider)
            self.tokens_acquired += 1

    async def call(self, method: str, params: list, *, tokens: int = 1) -> Any:
        """One metered JSON-RPC request with back-off on quota errors and reconnect on dead sockets."""
        last: BaseException | None = None
        for attempt in range(self.max_attempts):
            try:
                sub, gen, ep = await self._current()
            except (asyncio.TimeoutError, OSError, ConnectionError) as exc:
                last = exc
                await self._sleep(min(2 ** attempt, 30))
                continue
            await self._acquire(ep, tokens)
            t0 = time.monotonic()
            try:
                resp = await asyncio.wait_for(sub.rpc_request(method, params), timeout=self.timeout)
            except asyncio.CancelledError:
                if _really_cancelled():
                    raise
                last = RpcError(f"{method}: cancelled inside the transport")
                await self._sleep(min(2 ** attempt, 30))
                continue
            except Exception as exc:
                last = exc
                if missing_api_error(exc):
                    raise NoRuntimeApi(str(exc)[:300]) from exc
                if quota_error(exc):
                    self.throttled += 1
                    wait = min(5 * 2 ** attempt, self.backoff_max)
                    logger.warning("rpc throttled", data={"endpoint": ep.label, "method": method,
                                                          "wait_s": wait, "error": str(exc)[:160]})
                    await self._sleep(wait)
                    continue
                if isinstance(exc, (asyncio.TimeoutError, TimeoutError)) or is_dead_socket_error(exc):
                    logger.warning("rpc socket dead; reconnecting", data={"endpoint": ep.label, "method": method,
                                                                          "error": str(exc)[:160]})
                    await self._sleep(min(2 ** attempt, 30))
                    try:
                        await self.reconnect(gen)
                    except Exception as exc2:  # connect failures count as attempts too
                        last = exc2
                    continue
                raise RpcError(f"{method}: {str(exc)[:300]}") from exc
            ms = (time.monotonic() - t0) * 1000
            self.by_method[method] = self.by_method.get(method, 0) + 1
            lat = self.latency_ms.setdefault(method, [])
            if len(lat) < 500:
                lat.append(ms)
            if not isinstance(resp, dict):
                raise RpcError(f"{method}: empty response")
            if "error" in resp:
                err = resp["error"]
                msg = err.get("message") if isinstance(err, dict) else str(err)
                if missing_api_error(msg):
                    raise NoRuntimeApi(msg)
                raise RpcError(f"{method}: {msg}")
            return resp.get("result")
        if last is not None and quota_error(last):
            raise RateLimited(f"{method}: quota after {self.max_attempts} attempts: {str(last)[:200]}")
        raise DeadSocket(f"{method}: {str(last)[:300] if last else 'failed'}")

    # ── typed reads ────────────────────────────────────────────────────────────────────────────────────────
    async def head(self) -> tuple[int, str]:
        h = await self.call("chain_getFinalizedHead", [])
        hdr = await self.call("chain_getHeader", [h])
        return int(hdr["number"], 16), h

    async def block_hash(self, block: int) -> str:
        h = await self.call("chain_getBlockHash", [int(block)])
        if not h:
            raise RpcError(f"no hash for block {block} (beyond head?)")
        return h

    async def timestamp_ms(self, block_hash: str) -> int:
        v = await self.call("state_getStorage", [TIMESTAMP_NOW_KEY, block_hash])
        ts = le_int(v)
        if ts is None:
            raise RpcError(f"Timestamp.Now missing at {block_hash[:12]}")
        return ts

    async def storage(self, key: str, block_hash: str) -> str | None:
        return await self.call("state_getStorage", [key, block_hash])

    async def keys_paged(self, prefix: str, block_hash: str, count: int = 256) -> list[str]:
        out: list[str] = []
        start: str | None = None
        while True:
            page = await self.call("state_getKeysPaged", [prefix, count, start, block_hash]) or []
            out.extend(page)
            if len(page) < count:
                return out
            start = page[-1]

    async def query_storage_at(self, keys: list[str], block_hash: str) -> dict[str, str | None]:
        if not keys:
            return {}
        res = await self.call("state_queryStorageAt", [keys, block_hash]) or []
        out: dict[str, str | None] = {k: None for k in keys}
        for change_set in res:
            for k, v in change_set.get("changes", []):
                out[k] = v
        return out

    async def runtime_version(self, block_hash: str) -> int:
        rv = await self.call("state_getRuntimeVersion", [block_hash])
        return int(rv["specVersion"])

    async def _runtime(self, spec_version: int, meta_hash_fn: Callable[[], Awaitable[str]]) -> Any:
        """Runtime for ``spec_version``, loaded once per (endpoint, version) by a shared task that waiters await
        through ``asyncio.shield`` (a waiter's timeout never cancels the load for the others). ASI loads metadata at
        the *parent* of the hash it is given, so ``meta_hash_fn`` returns the hash of block b+1 for a read at b."""
        sub, _, ep = await self._current()
        key = (ep.url, int(spec_version))
        rt = self._runtimes.get(key)
        if rt is not None:
            return rt
        task = self._rt_tasks.get(key)
        if task is None:
            async def _load() -> Any:
                meta_hash = await meta_hash_fn()
                await self._acquire(ep, RUNTIME_LOAD_TOKENS)
                return await sub.get_runtime_for_version(int(spec_version), meta_hash)
            task = self._rt_tasks[key] = asyncio.ensure_future(_load())
        try:
            rt = await asyncio.wait_for(asyncio.shield(task), timeout=self.timeout * 3)
        except asyncio.TimeoutError:
            raise RpcError(f"runtime {spec_version}: load timeout") from None
        except asyncio.CancelledError:
            if _really_cancelled():
                raise
            self._rt_tasks.pop(key, None)
            raise RpcError(f"runtime {spec_version}: load cancelled") from None
        except Exception as exc:
            self._rt_tasks.pop(key, None)
            if isinstance(exc, RpcError):
                raise
            raise RpcError(f"runtime {spec_version}: {str(exc)[:200]}") from exc
        self._runtimes[key] = rt
        self._rt_tasks.pop(key, None)
        return rt

    async def dynamic_info(self, block_hash: str, spec_version: int, *,
                           meta_hash_fn: Callable[[], Awaitable[str]]) -> list[dict | None]:
        """Raw ``state_call`` (1 token) + ASI ``scale_decode`` with the cached runtime (CPU in a thread)."""
        from async_substrate_interface.utils.decoding import scale_decode
        raw = await self.call("state_call", ["SubnetInfoRuntimeApi_get_all_dynamic_info", "0x", block_hash])
        if not raw:
            raise DecodeError("empty get_all_dynamic_info result")
        rt = await self._runtime(spec_version, meta_hash_fn)
        api_map = getattr(rt, "runtime_api_map", None) or {}
        try:
            out_ty = api_map["SubnetInfoRuntimeApi"]["get_all_dynamic_info"]["output"]
        except (KeyError, TypeError):
            if getattr(rt, "metadata_v15", None) is None:
                raise DecodeError(f"decode:{spec_version}: no metadata v15") from None
            raise NoRuntimeApi(f"SubnetInfoRuntimeApi.get_all_dynamic_info absent in runtime {spec_version}") \
                from None
        try:
            obj = await asyncio.to_thread(scale_decode, f"scale_info::{out_ty}", bytes.fromhex(raw[2:]), rt)
        except Exception as exc:
            raise DecodeError(f"decode:{spec_version}: {str(exc)[:200]}") from exc
        val = obj.value
        if not isinstance(val, list):
            raise DecodeError(f"decode:{spec_version}: unexpected {type(val).__name__}")
        return val

    async def alpha_prices(self, block_hash: str, spec_version: int, *,
                           meta_hash_fn: Callable[[], Awaitable[str]]) -> dict[int, float] | None:
        """The chain's own price per netuid (``SwapRuntimeApi_current_alpha_price_all``, rao → τ/α; equals the SDK
        price the old ledger stored). ``None`` when the runtime has no such API (older runtimes)."""
        from async_substrate_interface.utils.decoding import scale_decode
        rt = await self._runtime(spec_version, meta_hash_fn)
        try:
            out_ty = rt.runtime_api_map["SwapRuntimeApi"]["current_alpha_price_all"]["output"]
        except (KeyError, TypeError, AttributeError):
            return None
        raw = await self.call("state_call", ["SwapRuntimeApi_current_alpha_price_all", "0x", block_hash])
        if not raw:
            raise DecodeError("empty current_alpha_price_all result")
        try:
            obj = await asyncio.to_thread(scale_decode, f"scale_info::{out_ty}", bytes.fromhex(raw[2:]), rt)
        except Exception as exc:
            raise DecodeError(f"decode:{spec_version}: price_all {str(exc)[:160]}") from exc
        out: dict[int, float] = {}
        for x in obj.value or []:
            if isinstance(x, dict):
                n, p = x.get("netuid"), x.get("price")
            elif isinstance(x, (list, tuple)) and len(x) == 2:
                n, p = x
            else:
                continue
            if n is not None and p is not None:
                out[int(n)] = int(p) / 1e9
        return out

    def stats(self) -> dict:
        def _med(xs: list[float]) -> float | None:
            if not xs:
                return None
            ys = sorted(xs)
            return round(ys[len(ys) // 2], 1)
        return {"endpoint": self.endpoint.label, "requests_sent": self.requests_sent,
                "tokens_acquired": self.tokens_acquired, "by_method": dict(self.by_method),
                "latency_ms_median": {m: _med(v) for m, v in self.latency_ms.items()}, "throttled": self.throttled}
