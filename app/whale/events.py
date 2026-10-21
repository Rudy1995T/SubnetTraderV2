"""TaoStats ``/v1/subnets/stake-events`` helpers (spec 11 §2.3 C8, 02 §G7).

The old bot's ``app/whale/events.py`` (lines 1-170) is not on disk (neither ``seed/app/`` nor the Pi recovery has
``app/whale/``; 2026-10-18), so this is a re-implementation from spec 11 §8.4's description, adapted to the ``/v1/``
response: ``nominator`` → ``coldkey``, ``delegate`` → ``hotkey``, ``delegate_name`` → ``hotkey_name``, ``action``
``stake``/``unstake`` (was ``DELEGATE``/``UNDELEGATE``), amounts as rao **strings** (Decimal), ``is_transfer`` always
a bool, ``validator_swap`` flags both halves of a within-subnet validator move. Stdlib only."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

DELEGATION_PATH = "/v1/subnets/stake-events"
PAGE_LIMIT = 200
DTAO_LAUNCH_TS = 1739404800          # 2025-02-13T00:00:00Z
DTAO_LAUNCH_BLOCK = 4920351
_ACTION_MAP = {"stake": "BUY", "unstake": "SELL", "delegate": "BUY", "undelegate": "SELL"}


def ss58_of(v: Any) -> str | None:
    """Plain SS58 on /v1/ (old API: ``{ss58, hex}`` objects)."""
    if isinstance(v, dict):
        return v.get("ss58")
    return v if isinstance(v, str) and v else None


def rao_to_tao(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(Decimal(str(v)) / Decimal(10 ** 9))
    except (InvalidOperation, ValueError):
        return None


def opt_float(v: Any) -> float | None:
    try:
        return None if v is None or v == "" else float(Decimal(str(v)))
    except (InvalidOperation, ValueError):
        return None


def parse_ts(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(v / 1000) if v > 1e11 else int(v)
    s = str(v).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def classify_event(row: dict) -> dict | None:
    """One stake-events row → a normalised activity row for ``app.integrity.classify.classify_stake_event``
    (``function`` add_stake/remove_stake, ``transfer_stake`` when ``is_transfer``, ``move_stake`` on a
    ``validator_swap`` half). None for a pre-dTAO or malformed row."""
    side = _ACTION_MAP.get(str(row.get("action") or "").lower())
    ts = parse_ts(row.get("timestamp"))
    block = row.get("block_number")
    netuid = row.get("netuid")
    if side is None or ts is None or netuid is None:
        return None
    try:
        if block is not None and int(block) < DTAO_LAUNCH_BLOCK:
            return None
    except (TypeError, ValueError):
        return None
    ck = ss58_of(row.get("coldkey") or row.get("nominator"))
    alpha = rao_to_tao(row.get("alpha") if row.get("alpha") is not None else row.get("alpha_amount"))
    tao = rao_to_tao(row.get("amount") if row.get("amount") is not None else row.get("tao_amount"))
    is_transfer = bool(row.get("is_transfer"))
    if row.get("validator_swap"):
        fn = "move_stake"
    elif is_transfer:
        fn = "transfer_stake"
    else:
        fn = "add_stake" if side == "BUY" else "remove_stake"
    n = int(netuid)
    out = {"id": f"ts:{row.get('id')}" if row.get("id") is not None else None, "extrinsic": row.get("extrinsic_id"),
           "block_number": int(block) if block is not None else None, "event_at": ts, "function": fn, "success": True,
           "is_proxy": False, "signer": ck, "from_hotkey": ss58_of(row.get("hotkey") or row.get("delegate")),
           "to_hotkey": ss58_of(row.get("hotkey") or row.get("delegate")), "price": opt_float(row.get("price")),
           "is_transfer": is_transfer, "side": side, "enabled": None, "raw": row}
    dest = ss58_of(row.get("transfer_address") or row.get("destination_coldkey"))
    if side == "SELL":
        out.update(from_coldkey=ck, to_coldkey=dest if is_transfer else ck, from_netuid=n,
                   to_netuid=n if (is_transfer or row.get("validator_swap")) else None,
                   from_alpha=alpha, to_alpha=alpha if is_transfer else None, from_tao=tao, to_tao=None)
    else:
        out.update(from_coldkey=dest if is_transfer else ck, to_coldkey=ck,
                   from_netuid=n if (is_transfer or row.get("validator_swap")) else None, to_netuid=n,
                   from_alpha=None, to_alpha=alpha, from_tao=tao, to_tao=None)
    return out


def build_delegation_params(coldkey: str, since_ts: int | None, page: int = 1, *, action: str = "all") -> dict:
    """The old builder with ``nominator`` → ``coldkey`` and ``order=timestamp_asc`` → ``order_by``/``order_dir``."""
    p: dict = {"coldkey": coldkey, "order_by": "timestamp", "order_dir": "asc", "limit": PAGE_LIMIT, "page": page}
    if since_ts:
        p["timestamp_start"] = int(since_ts)
    if action in ("stake", "unstake"):
        p["action"] = action
    return p


def parse_delegation_response(body: Any) -> tuple[list[dict], int | None]:
    """→ (rows, next_page) for ``{pagination: {next_page}, data: [...]}``."""
    if not isinstance(body, dict):
        return [], None
    rows = body.get("data") or []
    nxt = (body.get("pagination") or {}).get("next_page")
    return [r for r in rows if isinstance(r, dict)], (int(nxt) if nxt else None)
