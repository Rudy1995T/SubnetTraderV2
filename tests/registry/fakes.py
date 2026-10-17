"""Synthetic TMC / RPC / subnetalpha servers for registry tests (shapes verified live on 2026-09-24)."""
from __future__ import annotations

import json
import re

import httpx

from app.ratelimit import ProviderLimit
from app.registry.sources.chain_rpc import PREFIX_HEX, encode_identity_v3

TMC = "https://api.taomarketcap.com/public/v1"
RPC = "https://entrypoint-finney.opentensor.ai"
SA = "https://subnetalpha.ai/wp-json/wp/v2/subnet"


def fast_limits(rt) -> None:
    for p in ("tmc", "subnetalpha", "subtensor_rpc"):
        rt.limiter.limits[p] = ProviderLimit(10_000.0, 10_000)
    rt.limiter._build_buckets()


def srow(netuid: int, reg: int, name: str | None, *, owner: str | None = None, active: bool = True,
         feb: str | None = "100", block: int = 8_400_000, ids: dict | None = None) -> dict:
    ids = ids if ids is not None else ({"subnetName": name} if name else None)
    return {"netuid": netuid, "is_active": active, "registered_at": None,
            "latest_snapshot": {"network_registered_at": str(reg), "first_emission_block_number": feb if active else "0",
                                "subnet_owner": owner or f"ck{netuid}", "subnet_owner_hotkey": f"hk{netuid}",
                                "token_symbol": "α", "block_number": block, "subnet_identities_v3": ids,
                                "max_allowed_uids": 256, "subnetwork_n": 256}}


def filler(block: int = 8_400_000) -> list[dict]:
    return [srow(n, 1000 + n, f"Filler{n}", block=block) for n in range(200, 300)]


class World:
    """Mutable fake upstream state; ``install(mock)`` wires respx routes."""

    def __init__(self) -> None:
        self.rows: list[dict] = []
        self.table_ok = False
        self.subnets_status = 200
        self.chain: dict[int, dict[str, str]] = {}
        self.chain_status = 200
        self.chain_block = 8_400_000
        self.sa_records: list[dict] = []
        self.sa_status = 200
        self.coldkeys: list[dict] = []
        self.calls: dict[str, int] = {}

    def _count(self, k: str) -> None:
        self.calls[k] = self.calls.get(k, 0) + 1

    def subnets(self, request: httpx.Request) -> httpx.Response:
        self._count("subnets")
        if self.subnets_status != 200:
            return httpx.Response(self.subnets_status)
        rows = self.rows + filler(self.rows[0]["latest_snapshot"]["block_number"] if self.rows else 8_400_000)
        return httpx.Response(200, json={"count": len(rows), "next": None, "previous": None, "results": rows})

    def table(self, request: httpx.Request) -> httpx.Response:
        self._count("table")
        if not self.table_ok:
            return httpx.Response(503)
        rows = self.rows + filler()
        out = []
        for r in rows:
            ls = r["latest_snapshot"]
            out.append({"subnet": r["netuid"], "name": (ls["subnet_identities_v3"] or {}).get("subnetName", "Unknown"),
                        "symbol": "α", "is_active": r["is_active"], "network_registered_at": int(ls["network_registered_at"]),
                        "subnet_owner": ls["subnet_owner"], "block_number": ls["block_number"]})
        return httpx.Response(200, json=out)

    def coldkey(self, request: httpx.Request) -> httpx.Response:
        self._count("coldkey")
        return httpx.Response(200, json=self.coldkeys)

    def rpc(self, request: httpx.Request) -> httpx.Response:
        self._count("rpc")
        if self.chain_status != 200:
            return httpx.Response(self.chain_status)
        body = json.loads(request.content)
        m, p = body["method"], body["params"]
        if m == "chain_getFinalizedHead":
            res = "0xabc"
        elif m == "chain_getHeader":
            res = {"number": hex(self.chain_block)}
        elif m == "state_getKeysPaged":
            res = [self.key(n) for n in sorted(self.chain)]
        elif m == "state_queryStorageAt":
            ch = []
            for k in p[0]:
                n = int.from_bytes(bytes.fromhex(k[-4:]), "little")
                v = self.chain[n]
                ch.append([k, v if isinstance(v, str) else encode_identity_v3(v)])
            res = [{"block": "0xabc", "changes": ch}]
        else:
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "error": {"message": "nope"}})
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": res})

    @staticmethod
    def key(n: int) -> str:
        return PREFIX_HEX + "00" * 16 + n.to_bytes(2, "little").hex()

    def subnetalpha(self, request: httpx.Request) -> httpx.Response:
        self._count("subnetalpha")
        if self.sa_status != 200:
            return httpx.Response(self.sa_status, text="<html>challenge</html>")
        page = int(request.url.params.get("page", "1"))
        recs = self.sa_records
        chunk = recs[(page - 1) * 100: page * 100]
        if not chunk and page > 1:
            return httpx.Response(400, json={"code": "rest_post_invalid_page_number"})
        return httpx.Response(200, json=chunk, headers={"X-WP-Total": str(len(recs))})

    def install(self, mock) -> None:
        mock.get(f"{TMC}/subnets/").mock(side_effect=self.subnets)
        mock.get(f"{TMC}/subnets/table/").mock(side_effect=self.table)
        mock.get(f"{TMC}/accounts/coldkey-identities/").mock(side_effect=self.coldkey)
        mock.post(RPC).mock(side_effect=self.rpc)
        mock.get(url__regex=re.escape(SA) + r".*").mock(side_effect=self.subnetalpha)


ACF_KEYS = ["subnet_number", "subnet_owner", "github_url", "website_url", "twitterx_url", "discord_url", "telegram_url",
            "short_desc", "what_exactly_does_it_do", "what_exactly_is_the_productbuild", "team_info", "roadmap",
            "twitter_embed_id", "video_url", *[f"additional_video_{i}" for i in range(1, 9)]]


def sa_rec(netuid: int, title: str, modified: str = "2026-08-13T10:01:03", **acf) -> dict:
    a = {k: "" for k in ACF_KEYS}
    a["subnet_number"] = f"Subnet {netuid:02d}"
    a.update(acf)
    return {"id": netuid, "slug": title.lower().replace(" ", "-"), "title": {"rendered": title},
            "modified_gmt": modified, "link": f"https://subnetalpha.ai/subnet/{netuid}", "acf": a}
