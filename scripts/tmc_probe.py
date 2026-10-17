"""One-shot TMC probe (spec 05 §8, §10 step 1): SSE field set, envelope keys, block/event_at source, unit checks.

    python -m scripts.tmc_probe [--seconds 60] [--save-fixture]

Writes ``data/tmc_table_fields.json``. Uses the second anonymous SSE slot and 1 REST call (analytics SN64).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import app.config as app_config  # noqa: E402
from app.collectors.chain_tmc import TmcClient, parse_sse  # noqa: E402
from app.collectors.chain_tmc_rest import analytics_rows  # noqa: E402
from app.http import Http  # noqa: E402
from app.ratelimit import ProviderLimiter  # noqa: E402
from app.util.time import now_ts, to_iso  # noqa: E402


async def probe(seconds: int, save_fixture: bool) -> int:
    s = app_config.settings
    http = Http(ProviderLimiter.from_settings())
    tmc = TmcClient(http)
    full: list[dict] | None = None
    deltas, envelope, types = 0, set(), {}
    delta_block_seen = False
    t_end = time.monotonic() + seconds
    try:
        async with tmc.stream_sse(s.TMC_SSE_URL) as r:
            if r.status_code != 200:
                print(f"SSE HTTP {r.status_code} — anonymous SSE refused; the recorder will live on the REST fallback")
                return 1

            async def lines():
                async for ln in r.aiter_lines():
                    yield ln
                    if time.monotonic() > t_end:
                        return
            async for ev in parse_sse(lines()):
                try:
                    msg = json.loads(ev.data)
                except ValueError:
                    continue
                t = msg.get("type")
                types[t] = types.get(t, 0) + 1
                envelope |= set(msg) - {"data", "changes"}
                if t == "full" and full is None:
                    full = msg["data"]
                elif t == "delta":
                    deltas += 1
                    delta_block_seen |= any("block_number" in v for v in (msg.get("changes") or {}).values())
                if time.monotonic() > t_end:
                    break
    except Exception as exc:  # timeouts etc.
        print(f"SSE error: {type(exc).__name__}: {exc}")
    if not full:
        print("no full event received")
        await http.aclose()
        return 1
    fields = sorted({k for row in full for k in row})
    block = max(int(r.get("block_number") or 0) for r in full)
    block_source = "envelope" if "block_number" in envelope else ("row" if delta_block_seen else "row+estimated")
    event_at_source = "payload" if {"timestamp", "block_timestamp"} & envelope else "arrival"
    print(f"full: {len(full)} rows, {len(fields)} fields; deltas: {deltas}; types: {types}")
    print(f"envelope keys: {sorted(envelope)}; block source: {block_source}; event_at source: {event_at_source}")
    units: dict = {}
    sn64 = next((r for r in full if r.get("subnet") == 64), None)
    if sn64:
        tl = float(sn64.get("tao_liquidity") or 0)
        units["tao_liquidity"] = "rao" if tl > 1e12 else "tau"
        print(f"units: tao_liquidity(SN64)={tl:.4g} ⇒ {units['tao_liquidity']}")
        a = await tmc.fetch_analytics(64, "1W")
        rows = analytics_rows(a.json) if a.ok else []
        if rows:
            last = rows[-1]
            st = float(last.get("subnet_tao") or 0)
            buy = float(last.get("total_tao_buy_24h") or 0)
            units["analytics"] = {"total_tao_buy_24h": "rao" if buy > 1e6 else "tau"}
            ol_sse, ol_an = float(sn64.get("owner_personal_locked_alpha") or 0), float(
                last.get("owner_personal_locked_alpha") or 0)
            units["owner_lock_sse_eq_analytics"] = bool(ol_an and 0.5 < ol_sse / ol_an < 2)
            tl_sse, tl_an = float(sn64.get("total_locked_alpha") or 0), float(last.get("total_locked") or 0)
            units["total_locked_eq_total_locked_alpha"] = bool(tl_an and 0.5 < tl_sse / tl_an < 2)
            print(f"units: total_tao_buy_24h={buy:.4g} vs subnet_tao={st:.4g} ⇒ {units['analytics']['total_tao_buy_24h']}; "
                  f"owner lock SSE/analytics={ol_sse / ol_an if ol_an else float('nan'):.3f}; "
                  f"total_locked_alpha/total_locked={tl_sse / tl_an if tl_an else float('nan'):.3f}")
    out = {"captured_at": now_ts(), "block_number": block, "fields": fields, "envelope_keys": sorted(envelope),
           "units": units, "event_at_source": event_at_source, "block_source": block_source}
    path = s.path(s.DATA_DIR) / "tmc_table_fields.json"
    path.write_text(json.dumps(out, indent=1))
    print(f"wrote {path}")
    if save_fixture:
        fx = ROOT / "tests" / "fixtures" / f"tmc_table_{to_iso(now_ts())[:10]}_block{block}.json"
        fx.write_text(json.dumps(full))
        print(f"wrote {fx}")
    await http.aclose()
    return 0


def main() -> int:
    p = argparse.ArgumentParser(prog="python -m scripts.tmc_probe")
    p.add_argument("--seconds", type=int, default=60)
    p.add_argument("--save-fixture", action="store_true")
    a = p.parse_args()
    return asyncio.run(probe(a.seconds, a.save_fixture))


if __name__ == "__main__":
    sys.exit(main())
