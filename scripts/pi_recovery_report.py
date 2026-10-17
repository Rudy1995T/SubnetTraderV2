"""Pi recovery cross-reference report (spec 17 §2.10, checklist A4).

    python -m scripts.pi_recovery_report [--coldkey SS58] [--no-chain-check]

Reads ``seed/pi/`` (never ``coldkey`` or ``hotkeys/``), then — unless ``--no-chain-check`` — runs the ``backfill_tmc``
``positions`` task through the shared TMC bucket and reports which of the 10 PC-open positions
and the Pi's still-OPEN/STUCK positions still hold alpha.
Writes ``data/backfill/pi_recovery_report.json``.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys


def main(argv: list[str] | None = None) -> int:
    import app.config as app_config
    from app.backfill import common, pi_recovery
    from app.util.fs import write_atomic
    p = argparse.ArgumentParser(prog="python -m scripts.pi_recovery_report", description=__doc__.split("\n")[0])
    p.add_argument("--coldkey")
    p.add_argument("--no-chain-check", action="store_true")
    a = p.parse_args(argv)
    s = app_config.settings
    try:
        coldkey = a.coldkey or pi_recovery.read_coldkey_ss58(
            s.path(s.SEED_DIR) / "pi" / "wallets" / "trader_wallet" / "coldkeypub.txt") or s.PI_TRADER_COLDKEY or None
    except pi_recovery.WalletGuardError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    tmc_payload = None
    if coldkey and not a.no_chain_check:
        from scripts import backfill_tmc
        backfill_tmc.main(["--tasks", "positions", "--coldkey", coldkey])
        con = sqlite3.connect(f"file:{s.path(s.DB_PATH)}?mode=ro", uri=True)
        try:
            r = con.execute("SELECT payload_json FROM chain_rest_snapshots WHERE endpoint = ? "
                            "ORDER BY observed_at DESC LIMIT 1", (f"/accounts/coldkey-stakes/{coldkey}/",)).fetchone()
        finally:
            con.close()
        tmc_payload = json.loads(r[0]) if r and r[0] else None
    rep = pi_recovery.build_report(s, coldkey=coldkey, tmc_positions=tmc_payload)
    write_atomic(common.backfill_dir() / "pi_recovery_report.json", rep)
    print(f"Pi recovered: {rep['recovered']}   pool_history: "
          f"{json.dumps(rep['pool_history']) if isinstance(rep['pool_history'], dict) else rep['pool_history']}")
    pos = {x.get("id"): x for x in rep.get("positions") or []}
    chain = {x["id"]: x for x in (rep["chain"].get("positions") or [])} if rep["chain"].get("status") == "checked" else {}
    print(f"{'id':>4} {'netuid':>6} {'pi_status':>10} {'still_staked':>12} {'alpha':>14} {'tao_value':>10}")
    for pid, n in pi_recovery.tracked_positions(rep.get("positions") or []).items():
        c = chain.get(pid, {})
        print(f"{pid:>4} {n:>6} {str(pos.get(pid, {}).get('status', '?')):>10} {str(c.get('still_staked', '?')):>12} "
              f"{str(c.get('alpha', '?')):>14} {str(c.get('tao_value', '?')):>10}")
    if rep.get("next_command"):
        print(f"next: {rep['next_command']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
