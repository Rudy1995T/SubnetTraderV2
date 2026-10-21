"""Owner stake-event classification (spec 11 §2.4), the TaoStats phantom move-pair rule (old ``whale_impact.py``
158-178) and cross-source duplicate superseding (§2.3 source ranking, PIT by ``superseded_at``).

Input rows are the normalised dicts of ``owner_sources.map_activity_row`` (TMC staking-activity, verified shape
§2.3.1) or ``app.whale.events`` (TaoStats): ``{id, extrinsic, block_number, event_at, function, success, is_proxy,
signer, from_coldkey, to_coldkey, from_hotkey, to_hotkey, from_netuid, to_netuid, from_alpha, to_alpha, from_tao,
to_tao, price, is_transfer, enabled}`` — amounts already in α/τ.
"""
from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Iterable, Sequence

from app.integrity.models import StakeEvent

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

SOURCE_RANK: dict[str, int] = {"taostats_delegation": 0, "tmc_staking_activity": 1, "tmc_stake_removed": 2,
                               "tmc_stream": 3, "taostats_extrinsic": 4, "rpc_events": 5}
_SELL = {"remove_stake", "remove_stake_limit"}
_SELL_FULL = {"remove_stake_full_limit", "unstake_all", "unstake_all_alpha"}
_SWAP = {"swap_stake", "swap_stake_limit", "move_stake"}
_BUY = {"add_stake", "add_stake_limit"}
OUTFLOW_KINDS = frozenset({"sell", "sell_full", "swap_out", "transfer_out"})
TAO_KINDS = frozenset({"sell", "sell_full", "swap_out"})          # τ received (§2.5 tao_rcv)


def _fn(name: str | None) -> str:
    f = (name or "").strip()
    if "." in f:
        f = f.rsplit(".", 1)[1]
    return f


def _int(x) -> int | None:
    try:
        return None if x is None or x == "" else int(x)
    except (TypeError, ValueError):
        return None


def event_uid(row: dict) -> str:
    if row.get("id"):
        return str(row["id"])
    raw = f"{row.get('extrinsic')}|{row.get('function')}|{row.get('from_netuid')}|{row.get('to_netuid')}|" \
          f"{row.get('from_alpha') or row.get('to_alpha')}"
    return hashlib.sha1(raw.encode()).hexdigest()


def origin_coldkey(row: dict) -> str | None:
    """The coldkey whose position the call moves: ``from_coldkey``; on removals TMC leaves it null and puts the owner
    in ``to_coldkey`` (the τ destination, §2.3.1); else the signer."""
    if row.get("from_coldkey"):
        return row["from_coldkey"]
    f = _fn(row.get("function"))
    if (f in _SELL or f in _SELL_FULL) and row.get("to_coldkey"):
        return row["to_coldkey"]
    return row.get("signer")


def classify_stake_event(row: dict, *, occupant_netuid: int, owner_coldkey: str) -> StakeEvent | None:
    """One normalised activity row → a ``StakeEvent`` of occupant netuid ``n`` for ``owner_coldkey``, or None when
    the row touches neither the netuid nor the owner's position."""
    n = int(occupant_netuid)
    f = _fn(row.get("function"))
    fn_, tn = _int(row.get("from_netuid")), _int(row.get("to_netuid"))
    src_ck = origin_coldkey(row)
    dst_ck = row.get("to_coldkey")
    out_side = src_ck == owner_coldkey and (fn_ == n or (fn_ is None and f in _SELL_FULL))
    in_side = (dst_ck == owner_coldkey or (dst_ck is None and src_ck == owner_coldkey)) and tn == n
    kind, outflow = "other", 0
    if f in _SELL:
        if out_side:
            kind, outflow = "sell", 1
    elif f in _SELL_FULL:
        if out_side:
            kind, outflow = "sell_full", 1
    elif f in _SWAP:
        if fn_ == n and tn == n and out_side:
            kind = "move_internal"
        elif out_side and tn != n:
            kind, outflow = "swap_out", 1
        elif in_side and fn_ != n:
            kind = "swap_in"
    elif f == "transfer_stake":
        if out_side and dst_ck and dst_ck != owner_coldkey:
            kind, outflow = "transfer_out", 1
        elif out_side and dst_ck == owner_coldkey and tn is not None and tn != n:
            kind, outflow = "swap_out", 1
        elif out_side and dst_ck == owner_coldkey:
            kind = "move_internal"
        elif in_side and src_ck != owner_coldkey:
            kind = "transfer_in"
        elif in_side:
            kind = "swap_in"
    elif f in _BUY:
        if in_side or (src_ck == owner_coldkey and tn == n):
            kind = "buy"
    elif f == "lock_stake":
        kind = "lock"
    elif f == "move_lock":
        kind = "lock_move"
    elif f == "set_perpetual_lock":
        en = row.get("enabled")
        kind = "perpetual_off" if en is False or en in ("false", "False", 0) else "perpetual_on"
    if kind == "other" and not (out_side or in_side or (src_ck == owner_coldkey and n in (fn_, tn))):
        return None
    if fn_ != n and tn != n and not (f in _SELL_FULL and fn_ is None) and kind not in ("perpetual_off",
                                                                                     "perpetual_on"):
        return None
    if src_ck != owner_coldkey and dst_ck != owner_coldkey and row.get("signer") != owner_coldkey:
        return None
    success = 1 if row.get("success", True) not in (False, 0, "false") else 0
    if not success:
        outflow = 0
    inflow_kind = kind in ("buy", "swap_in", "transfer_in")
    alpha = row.get("to_alpha") if inflow_kind else row.get("from_alpha")
    if alpha is None:
        alpha = row.get("from_alpha") if inflow_kind else row.get("to_alpha")
    tao = row.get("from_tao") if kind in TAO_KINDS else (row.get("from_tao") or row.get("to_tao"))
    signer = row.get("signer")
    return StakeEvent(
        event_uid=event_uid(row), owner_coldkey=owner_coldkey, function=row.get("function") or f, kind=kind,
        counts_as_outflow=outflow, event_at=int(row["event_at"]), signer=signer,
        hotkey=row.get("from_hotkey") if not inflow_kind else (row.get("to_hotkey") or row.get("from_hotkey")),
        dest_coldkey=dst_ck, from_netuid=fn_, to_netuid=tn, alpha=alpha, tao=tao, price=row.get("price"),
        is_proxy=1 if (row.get("is_proxy") or (signer and signer != owner_coldkey)) else 0, success=success,
        block_number=_int(row.get("block_number")), extrinsic_id=row.get("extrinsic"),
        is_transfer=row.get("is_transfer"), raw=row.get("raw"))


def mark_phantom_moves(events: Sequence[StakeEvent]) -> list[StakeEvent]:
    """Old ``whale_impact.py`` 158-178: among non-transfer rows (``is_transfer`` NULL counts as non-transfer) with
    netuid > 0, a coldkey/netuid/block holding both a BUY and a SELL leg is a phantom move pair → both legs
    ``move_internal`` (never an outflow). Cross-netuid swaps stay real trades."""
    def net(e: StakeEvent) -> int | None:
        return e.from_netuid if e.kind in OUTFLOW_KINDS else e.to_netuid
    groups: dict[tuple, set[str]] = {}
    for e in events:
        if e.is_transfer or (net(e) or 0) <= 0:
            continue
        side = "sell" if e.kind in ("sell", "sell_full") else "buy" if e.kind == "buy" else None
        if side:
            groups.setdefault((e.owner_coldkey, net(e), e.block_number), set()).add(side)
    out = []
    for e in events:
        key = (e.owner_coldkey, net(e), e.block_number)
        if (not e.is_transfer and e.kind in ("sell", "sell_full", "buy") and groups.get(key) == {"sell", "buy"}):
            e.kind, e.counts_as_outflow = "move_internal", 0
        out.append(e)
    return out


async def supersede_duplicates(db: "Database", occupant_keys: Iterable[str], now: int) -> int:
    """Same owner coldkey, occupant, block and direction with alpha within 0.1 % across sources → the lower-ranked
    row gets ``superseded_by``/``superseded_at`` (PIT: it still counts for decisions before ``superseded_at``)."""
    n = 0
    for okey in occupant_keys:
        rows = await db.fetchall(
            "SELECT event_uid, owner_coldkey, block_number, counts_as_outflow, alpha, source FROM owner_stake_events "
            "WHERE occupant_key = ? AND superseded_by IS NULL AND block_number IS NOT NULL", (okey,), writer=True)
        groups: dict[tuple, list[dict]] = {}
        for r in rows:
            groups.setdefault((r["owner_coldkey"], r["block_number"], r["counts_as_outflow"]), []).append(r)
        for grp in groups.values():
            if len({r["source"] for r in grp}) < 2:
                continue
            grp.sort(key=lambda r: SOURCE_RANK.get(r["source"], 9))
            for i, win in enumerate(grp):
                for lose in grp[i + 1:]:
                    if lose["source"] == win["source"] or lose.get("_sup"):
                        continue
                    a, b = abs(win["alpha"] or 0.0), abs(lose["alpha"] or 0.0)
                    if max(a, b) == 0 or abs(a - b) <= 0.001 * max(a, b):
                        await db.execute("UPDATE owner_stake_events SET superseded_by = ?, superseded_at = ? "
                                         "WHERE event_uid = ? AND occupant_key = ?",
                                         (win["event_uid"], int(now), lose["event_uid"], okey))
                        lose["_sup"] = True
                        n += 1
    return n
