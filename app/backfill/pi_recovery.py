"""Pi recovery cross-reference (spec 17 §2.10, checklist A4).

Reads only what the operator copied to ``seed/pi/``. **Guard:** refuses any path whose basename is ``coldkey`` or that
lies under ``hotkeys/``; of ``.env`` it reads key *names* only. TaoSentinel never unstakes — the report only says which
positions still hold alpha (a manual wallet action on the Pi, outside this system).
"""
from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable

from app.backfill import common
from app.backfill.tmc_backfill import parse_coldkey_stakes
from app.util.time import to_iso

# PC-OPEN value positions (ema_positions id → netuid), 1.0 τ each, entered 2026-06-05..06-17.
PC_OPEN_POSITIONS = {416: 99, 417: 47, 419: 31, 421: 122, 424: 108, 425: 94, 426: 116, 429: 113, 431: 67, 432: 109}
PC_MAX_TS = 1781904274
DUST_ALPHA = 1e-6          # α; TMC lists unstaked positions with a 1-rao remainder — not a position
_SS58 = re.compile(r"^5[1-9A-HJ-NP-Za-km-z]{47}$")


class WalletGuardError(PermissionError):
    pass


def _guard(path: Path) -> Path:
    p = Path(path)
    if p.name == "coldkey" or "hotkeys" in p.parts:
        raise WalletGuardError(f"refusing to open wallet secret path {p}")
    return p


def safe_open(path: Path, mode: str = "r"):
    return open(_guard(path), mode)


def read_coldkey_ss58(path: Path) -> str | None:
    """``coldkeypub.txt``: JSON ``{"ss58Address": …}`` or a plain SS58 line."""
    p = _guard(path)
    if not p.exists():
        return None
    with safe_open(p) as f:
        txt = f.read().strip()
    try:
        doc = json.loads(txt)
        if isinstance(doc, dict):
            v = doc.get("ss58Address") or doc.get("ss58_address")
            return v if isinstance(v, str) and _SS58.match(v) else None
    except ValueError:
        pass
    line = txt.splitlines()[0].strip() if txt else ""
    return line if _SS58.match(line) else None


def env_key_names(path: Path) -> list[str]:
    p = _guard(path)
    if not p.exists():
        return []
    names = []
    with safe_open(p) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                names.append(line.split("=", 1)[0].strip())
    return sorted(set(names))


def ledger_summary(pi_db: Path, *, pc_max_ts: int = PC_MAX_TS, trust_ts: int) -> dict:
    p = _guard(pi_db)
    con = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    try:
        cols = {r[1] for r in con.execute("PRAGMA table_info(pool_history)")}
        r = con.execute("SELECT COUNT(*), MAX(timestamp), MAX(block_number) FROM pool_history").fetchone()
        beyond = con.execute("SELECT COUNT(*) FROM pool_history WHERE timestamp > ?", (pc_max_ts,)).fetchone()[0]
        back = con.execute("SELECT COUNT(*) FROM pool_history WHERE timestamp < ? AND network_registered_at IS NOT NULL",
                           (trust_ts,)).fetchone()[0] if "network_registered_at" in cols else 0
        return {"rows": r[0], "max_ts": r[1], "max_ts_iso": to_iso(r[1]) if r[1] else None, "max_block": r[2],
                "rows_beyond_pc": beyond, "nra_backstamped": back}
    finally:
        con.close()


def positions_summary(pi_db: Path, ids: Iterable[int] = PC_OPEN_POSITIONS) -> list[dict]:
    p = _guard(pi_db)
    con = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        cols = {r[1] for r in con.execute("PRAGMA table_info(ema_positions)")}
        if not cols:
            return []
        want = [c for c in ("id", "netuid", "status", "exit_ts", "exit_reason", "amount_tao_out", "tao_recovered",
                            "staked_hotkey", "strategy") if c in cols]
        ids = list(ids)
        rows = con.execute(f"SELECT {', '.join(want)} FROM ema_positions WHERE id IN ({','.join('?' * len(ids))}) "
                           f"OR status IN ('OPEN','STUCK') ORDER BY id", ids).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def tracked_positions(pi_positions: list[dict]) -> dict[int, int]:
    """id → netuid: the 10 PC-open positions plus every Pi row still ``OPEN``/``STUCK`` (opened after the PC copy)."""
    out = dict(PC_OPEN_POSITIONS)
    for p in pi_positions:
        if p.get("status") in ("OPEN", "STUCK") and p.get("id") is not None and p.get("netuid") is not None:
            out[int(p["id"])] = int(p["netuid"])
    return out


def build_report(settings, *, coldkey: str | None, tmc_positions: dict | None) -> dict:
    s = settings
    pi_dir = s.path(s.SEED_DIR) / "pi"
    out: dict[str, Any] = {"pi_dir": str(pi_dir), "recovered": pi_dir.exists()}
    ledger = pi_dir / "ledger.db"
    if ledger.exists():
        out["pool_history"] = ledger_summary(ledger, trust_ts=s.LEDGER_NRA_TRUST_FROM_TS)
        out["positions"] = positions_summary(ledger)
    else:
        out["pool_history"] = "not_recovered"
        out["positions"] = []
    ck = coldkey or read_coldkey_ss58(pi_dir / "wallets" / "trader_wallet" / "coldkeypub.txt") or s.PI_TRADER_COLDKEY
    out["coldkey"] = ck
    chain: dict[str, Any] = {"status": "not_checked"}
    if tmc_positions is not None:
        rows = parse_coldkey_stakes(tmc_positions)
        by_n = {r["netuid"]: r for r in rows if r.get("netuid") is not None}
        pos_hot = {p.get("netuid"): p.get("staked_hotkey") for p in out.get("positions") or []}
        chain = {"status": "checked", "positions": []}
        for pid, n in tracked_positions(out.get("positions") or []).items():
            r = by_n.get(n)
            chain["positions"].append({"id": pid, "netuid": n,
                                       "still_staked": bool(r and (r.get("alpha") or 0) >= DUST_ALPHA),
                                       "dust_only": bool(r and 0 < (r.get("alpha") or 0) < DUST_ALPHA),
                                       "alpha": r.get("alpha") if r else 0.0,
                                       "tao_value": r.get("tao_value") if r else 0.0,
                                       "hotkey_matches": (r.get("hotkey") == pos_hot.get(n)) if r and pos_hot.get(n)
                                       else None})
        tracked = set(tracked_positions(out.get("positions") or []).values())
        chain["other_stakes"] = [r for n, r in by_n.items()
                                 if n not in tracked and (r.get("alpha") or 0) >= DUST_ALPHA]
        chain["dust_netuids"] = sorted(n for n, r in by_n.items() if 0 < (r.get("alpha") or 0) < DUST_ALPHA)
    out["chain"] = chain
    out["markers"] = {m: (pi_dir / m).exists() for m in ("VALUE_KILLED", "KILL_SWITCH")}
    j = pi_dir / "pi-journal.txt"
    out["journal_tail"] = j.read_text(errors="replace").splitlines()[-20:] if j.exists() else None
    out["env_keys"] = env_key_names(pi_dir / ".env")
    con = common.open_db(check=False)
    try:
        out["imported_ledger_pi_rows"] = con.execute(
            "SELECT COUNT(*) FROM pool_history WHERE source = 'ledger_pi'").fetchone()[0]
    finally:
        con.close()
    if isinstance(out["pool_history"], dict) and not out["imported_ledger_pi_rows"]:
        out["next_command"] = "python -m scripts.import_old_ledger --phase import"
    return out
