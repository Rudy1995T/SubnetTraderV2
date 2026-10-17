"""Phase 3 runbook orchestrator (spec 17 §2.0): S1, S3, S4, S5, S2, S6, S7, S8 in that order.

    python -m scripts.backfill_all [--skip-pre-ledger] [--skip-github] [--skip-ledger] [--yes]

Stops the service around S1 (``sudo systemctl stop|start taosentinel``; asks first unless ``--yes``), stops at the
first failed step and prints the next manual command. ``--skip-ledger`` runs without the old bot's seed files
(S1/S5 import skipped; the RPC eras then cover the ledger span from the chain). A PC, Pi (``seed/pi/ledger.db``) or
extra ledger enables S1/S5. S7 is skipped (exit 3) if the regime port is missing. Every step is idempotent, so
re-running resumes.
"""
from __future__ import annotations

import argparse
import subprocess
import sys


def _run(cmd: list[str]) -> int:
    print(f"\n$ {' '.join(cmd)}", flush=True)
    return subprocess.call([sys.executable, "-m", *cmd])


def _systemctl(action: str, yes: bool) -> None:
    if not yes:
        ans = input(f"run 'sudo systemctl {action} taosentinel'? [y/N] ").strip().lower()
        if ans != "y":
            print(f"skipped systemctl {action}")
            return
    subprocess.call(["sudo", "systemctl", action, "taosentinel"])


def main(argv: list[str] | None = None) -> int:
    import app.config as app_config
    from app.backfill import common
    p = argparse.ArgumentParser(prog="python -m scripts.backfill_all", description=__doc__.split("\n")[0])
    p.add_argument("--skip-pre-ledger", action="store_true")
    p.add_argument("--skip-github", action="store_true")
    p.add_argument("--skip-ledger", action="store_true")
    p.add_argument("--yes", action="store_true")
    a = p.parse_args(argv)
    s = app_config.settings
    from app.backfill.ledger_import import discover_sources
    has_ledger = any(not x.merge_only for x in discover_sources(s)) and not a.skip_ledger
    eras = ["gap", "ledger"] + ([] if a.skip_pre_ledger else ["pre_ledger"])
    steps: list[tuple[str, list[str], tuple[int, ...]]] = []
    if has_ledger:
        steps.append(("S1", ["scripts.import_old_ledger", "--phase", "import"], (0,)))
    steps += [
        ("S3", ["scripts.backfill_tmc", "--tasks", "analytics1y,analytics1m"], (0,)),
        ("S4", ["scripts.backfill_rpc", *sum((["--era", e] for e in eras), [])], (0,)),
    ]
    if has_ledger:
        steps.append(("S5", ["scripts.import_old_ledger", "--phase", "stamp,bars"], (0,)))
    steps += [
        ("S2", ["scripts.import_fundamentals"], (0,)),
        ("S6", ["scripts.backfill_tmc", "--tasks", "restamp,candles5m"], (0,)),
        ("S7", ["scripts.backfill_regime"], (0, 3)),
        ("S8", ["scripts.build_coverage"], (0,)),
    ]
    if not a.skip_github:
        print("note: S9 (backfill_github) belongs to Phase 4 and is not run here")
    for sid, cmd, ok_codes in steps:
        if sid == "S1" and common.service_is_up():
            _systemctl("stop", a.yes)
        code = _run(cmd)
        if sid == "S1" and not common.service_is_up():
            _systemctl("start", a.yes)
        if code not in ok_codes:
            print(f"\n{sid} failed (exit {code}). Fix the cause, then resume with:\n  python -m {' '.join(cmd)}\n"
                  f"and re-run python -m scripts.backfill_all afterwards (finished work is skipped).")
            return code
        if sid == "S7" and code == 3:
            print("S7 skipped: regime backfill needs the app/quant/regime.py port")
    print("\nPhase 3 steps finished; see data/quant/coverage.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
