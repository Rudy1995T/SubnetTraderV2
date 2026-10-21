"""Live owner-endpoint probe (spec 11 §10 step 1; shapes recorded in §2.3.1).

    python -m scripts.owner_probe --netuids 64,79,3,15 [--out data/owner_field_map.json]

≈ 4 TMC calls per netuid (detail, coldkey-stakes, neuron 1W, staking-activity) through 01's limiter, plus Rufus
(2) and subnetradar (1). Prints the key sets and writes the field map with ``write_atomic`` (``auto`` settings read it).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys


async def _main(a: argparse.Namespace) -> int:
    import app.config as app_config
    from app.collectors.chain_tmc import TmcClient
    from app.http import Http
    from app.integrity import owner_sources as S
    from app.ratelimit import ProviderLimiter
    from app.util.fs import write_atomic
    from app.util.time import now_ts, to_iso
    s = app_config.settings
    http = Http(ProviderLimiter.from_settings())
    tmc = S.PacedTmc(TmcClient(http), per_min=int(s.OWNER_TMC_MAX_PER_MIN), per_run=100)
    out: dict = {"probed_at": to_iso(now_ts()), "amount_units": "rao", "activity_time_format": "iso",
                 "activity_ordering": "block_number", "netuids": {}}
    try:
        for n in [int(x) for x in a.netuids.split(",") if x.strip()]:
            rec: dict = {}
            k, st = await S.fetch_owner_keys(tmc, n, list_rows={}, source_mode="detail")
            rec["c1"] = {"status": st, "coldkey": k.coldkey if k else None, "hotkey": k.hotkey if k else None,
                         "owner_cut_per_block": k.owner_cut_per_block if k else None,
                         "owner_cut_enabled": k.owner_cut_enabled if k else None,
                         "perpetual": k.perpetual if k else None}
            if k and k.coldkey:
                p, st2 = await S.fetch_coldkey_stakes(tmc, k.coldkey)
                m = S.map_coldkey_stakes(p, n)
                rec["c2"] = {"status": st2, "top_keys": sorted(p.keys())[:12] if p else None,
                             "total": m["total"] if m else None, "locked": m["locked"] if m else None}
                rows, st4, tr = await S.fetch_activity(tmc, k.coldkey, now_ts() - 35 * 86400, now_ts(),
                                                       page_limit=50, max_pages=1)
                rec["c4"] = {"status": st4, "rows": len(rows), "truncated": tr,
                             "keys": sorted(rows[0].keys()) if rows else [],
                             "functions": sorted({r.get("function") for r in rows})}
            if k and k.hotkey:
                rows, st3 = await S.fetch_neuron_series(tmc, n, k.hotkey, s.OWNER_NEURON_SPAN)
                rec["c3"] = {"status": st3, "rows": len(rows or []), "keys": sorted((rows or [{}])[0].keys())}
            out["netuids"][str(n)] = rec
            print(json.dumps({n: rec}, default=str))
        table, st6, baked, _ = await S.fetch_rufus(http, table_url=s.OWNER_RUFUS_TABLE_URL,
                                                   baked_url=s.OWNER_RUFUS_BAKED_AT_URL, last_baked_at=None)
        parsed = S.parse_rufus(table) if table is not None else {}
        out["rufus"] = {"status": st6, "baked_at": to_iso(baked) if baked else None, "subnets": len(parsed),
                        "shape": "data_by_netuid" if isinstance(table, dict) and "data" in table else type(table).__name__}
        radar, st7, _ = await S.fetch_subnetradar(http, s.OWNER_SUBNETRADAR_URL)
        out["subnetradar"] = {"status": st7, "rows": len(radar)}
        print(json.dumps({"rufus": out["rufus"], "subnetradar": out["subnetradar"], "tmc_requests": tmc.n}))
        write_atomic(s.path(a.out), out)
        print(f"wrote {a.out}")
        return 0
    finally:
        await http.aclose()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m scripts.owner_probe")
    p.add_argument("--netuids", default="64,79,3,15")
    p.add_argument("--out", default="data/owner_field_map.json")
    return asyncio.run(_main(p.parse_args(argv)))


if __name__ == "__main__":
    sys.exit(main())
