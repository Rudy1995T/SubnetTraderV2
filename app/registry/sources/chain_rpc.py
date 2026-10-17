"""On-chain ``SubnetIdentitiesV3`` via public JSON-RPC over HTTPS (spec 04 §2.3). No SDK, no websockets.

Reads are pinned to one finalised block; on a ``ProviderError`` the whole read restarts on the next URL.
Verified 2026-09-24 on entrypoint-finney: storage keys are **102** hex chars (Blake2_128Concat, 16-byte hash) — the
spec's 86-char Twox64Concat form is also accepted; the netuid is always the trailing little-endian ``u16``.
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import xxhash

from app.http import ProviderError
from app.registry.models import RpcError, ScaleError

if TYPE_CHECKING:  # pragma: no cover
    from app.http import Http

IDENTITY_V3_FIELDS = ("subnet_name", "github_repo", "subnet_contact", "subnet_url", "discord", "description",
                      "logo_url", "additional")


def twox128(s: str) -> bytes:
    b = s.encode()
    return xxhash.xxh64_intdigest(b, seed=0).to_bytes(8, "little") + xxhash.xxh64_intdigest(b, seed=1).to_bytes(8,
                                                                                                             "little")


PREFIX_HEX = "0x" + (twox128("SubtensorModule") + twox128("SubnetIdentitiesV3")).hex()
_VALID_KEY_LENGTHS = (86, 102)      # Twox64Concat (spec) | Blake2_128Concat (live chain)


def compact_len(b: bytes, i: int) -> tuple[int, int]:
    if i >= len(b):
        raise ScaleError("compact length past end of value")
    m = b[i] & 3
    if m == 0:
        return b[i] >> 2, i + 1
    if m == 1:
        return int.from_bytes(b[i:i + 2], "little") >> 2, i + 2
    if m == 2:
        return int.from_bytes(b[i:i + 4], "little") >> 2, i + 4
    raise ScaleError("big-int compact not expected")


def encode_compact(n: int) -> bytes:
    if n < 64:
        return bytes([n << 2])
    if n < 16384:
        return ((n << 2) | 1).to_bytes(2, "little")
    return ((n << 2) | 2).to_bytes(4, "little")


def encode_identity_v3(fields: dict[str, str]) -> str:
    out = b""
    for name in IDENTITY_V3_FIELDS:
        v = fields.get(name, "").encode()
        out += encode_compact(len(v)) + v
    return "0x" + out.hex()


def decode_identity_v3(value_hex: str) -> dict[str, str]:
    b = bytes.fromhex(value_hex[2:] if value_hex.startswith("0x") else value_hex)
    i = 0
    out = {}
    for name in IDENTITY_V3_FIELDS:
        n, i = compact_len(b, i)
        if i + n > len(b):
            raise ScaleError(f"{name}: length {n} overruns value")
        out[name] = b[i:i + n].decode("utf-8", "replace")
        i += n
    if i != len(b):
        raise ScaleError(f"{len(b) - i} trailing bytes")
    return out


def netuid_from_key(key_hex: str) -> int:
    if len(key_hex) not in _VALID_KEY_LENGTHS or not key_hex.startswith(PREFIX_HEX):
        raise ValueError(f"bad SubnetIdentitiesV3 key {key_hex[:20]}… (len {len(key_hex)})")
    return int.from_bytes(bytes.fromhex(key_hex[-4:]), "little")


@dataclass
class ChainReadResult:
    block_number: int
    block_hash: str
    url: str
    rows: dict[int, dict[str, str]] = field(default_factory=dict)
    raw: dict[int, str] = field(default_factory=dict)
    decode_errors: dict[int, str] = field(default_factory=dict)
    bad_keys: int = 0
    n_keys: int = 0
    calls: int = 0


class SubtensorRpc:
    def __init__(self, urls: list[str], http: "Http", timeout_s: float) -> None:
        self.urls = [u for u in urls if u]
        self.http = http
        self.timeout_s = timeout_s
        self._ids = itertools.count(1)
        self.calls = 0

    async def call(self, url: str, method: str, params: list) -> Any:
        self.calls += 1
        resp = await self.http.request("subtensor_rpc", "POST", url,
                                       json={"jsonrpc": "2.0", "id": next(self._ids), "method": method,
                                             "params": params}, timeout=self.timeout_s)
        if resp.status_code != 200:
            raise RpcError(f"{method}: HTTP {resp.status_code}")
        try:
            body = resp.json()
        except ValueError as exc:
            raise RpcError(f"{method}: invalid JSON") from exc
        if body.get("error"):
            raise RpcError(f"{method}: {body['error']}")
        return body.get("result")

    async def _read(self, url: str, chunk: int) -> ChainReadResult:
        self.calls = 0
        block_hash = await self.call(url, "chain_getFinalizedHead", [])
        header = await self.call(url, "chain_getHeader", [block_hash])
        block = int(header["number"], 16)
        keys: list[str] = []
        start = None
        while True:
            batch = await self.call(url, "state_getKeysPaged", [PREFIX_HEX, 1000, start, block_hash]) or []
            keys.extend(batch)
            if len(batch) < 1000:
                break
            start = batch[-1]
        res = ChainReadResult(block, block_hash, url, n_keys=len(keys))
        good: list[tuple[str, int]] = []
        for k in keys:
            try:
                good.append((k, netuid_from_key(k)))
            except ValueError:
                res.bad_keys += 1
        by_key = dict(good)
        for i in range(0, len(good), chunk):
            part = [k for k, _ in good[i:i + chunk]]
            out = await self.call(url, "state_queryStorageAt", [part, block_hash]) or []
            for blk in out:
                for k, v in blk.get("changes", []):
                    n = by_key.get(k)
                    if n is None:
                        continue
                    if v is None:
                        res.decode_errors[n] = "null value"
                        continue
                    res.raw[n] = v
                    try:
                        res.rows[n] = decode_identity_v3(v)
                    except ScaleError as exc:
                        res.decode_errors[n] = str(exc)
        res.calls = self.calls
        return res

    async def read_subnet_identities_v3(self, chunk: int = 100) -> ChainReadResult:
        last: Exception | None = None
        for url in self.urls:
            try:
                return await self._read(url, chunk)
            except (ProviderError, RpcError, KeyError, TypeError, ValueError) as exc:
                last = exc
                continue
        raise RpcError(f"all RPC endpoints failed: {last}") from last
