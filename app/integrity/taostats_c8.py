"""C8 TaoStats upgrades (spec 11 §2.3; dormant unless ``settings.taostats_active()`` and ``OWNER_TAOSTATS_ENABLED``;
skipped while ``KILL_SWITCH`` exists). Routes are 02 §G7's ``/v1/`` API: delegation stake events per owner coldkey,
the owner history (``is_coldkey_swap`` → D1 classification) and the weekly ``set_perpetual_lock`` /
``announce_coldkey_swap`` extrinsic sweep (D3 / D9). ``NetworkRegistrationQueued`` → ``initial_alpha_est`` is not
built (the fixed-point ``median_subnet_alpha_price`` decode is unverified; S5 then applies the uniform −0.2)."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.integrity import constants as K
from app.integrity import detectors as D
from app.integrity.classify import classify_stake_event, mark_phantom_moves
from app.integrity.models import OwnerChange
from app.whale import events as W

if TYPE_CHECKING:  # pragma: no cover
    from app.integrity.owner_collect import Collector

MAX_PAGES = 5


def active() -> bool:
    from app.control import kill_switch_active
    s = app_config.settings
    return bool(s.taostats_active() and s.OWNER_TAOSTATS_ENABLED and not kill_switch_active())


async def _get(c: "Collector", path: str, params: dict) -> Any | None:
    from app.http import ProviderError
    s = app_config.settings
    try:
        r = await c.http.request("taostats", "GET", s.TAOSTATS_BASE_URL.rstrip("/") + path, params=params, timeout=30)
    except ProviderError as exc:
        c.stats["c8_fail"] += 1
        c.c8_status = f"fail:{exc.status}"
        return None
    c.stats["c8_requests"] += 1
    try:
        return r.json()
    except ValueError:
        return None


async def delegations(c: "Collector", ck: str, occs: list[dict]) -> int:
    from app.integrity.owner_collect import get_state, set_state
    wm = await get_state(c.db, f"taostats_wm:{ck}")
    since = int(wm) if wm else c.now - int(app_config.settings.OWNER_ACTIVITY_BACKFILL_DAYS) * K.DAY
    rows: list[dict] = []
    page = 1
    for _ in range(MAX_PAGES):
        body = await _get(c, W.DELEGATION_PATH, W.build_delegation_params(ck, since, page))
        part, nxt = W.parse_delegation_response(body)
        rows += [m for m in (W.classify_event(r) for r in part) if m]
        if not nxt:
            break
        page = nxt
    n_new = 0
    for occ in occs:
        evs = [e for e in (classify_stake_event(m, occupant_netuid=int(occ["netuid"]), owner_coldkey=ck) for m in rows)
               if e is not None]
        for e in mark_phantom_moves(evs):
            n_new += 1 if await c._insert_event(occ["occupant_key"], int(occ["netuid"]), e,
                                                source="taostats_delegation") else 0
        if evs:
            c.touched.add(occ["occupant_key"])
    if rows:
        await set_state(c.db, f"taostats_wm:{ck}", max(m["event_at"] for m in rows))
    return n_new


async def owner_history(c: "Collector") -> set[tuple[int, str]]:
    """{(netuid, new_owner_coldkey)} that TaoStats marks ``is_coldkey_swap``."""
    out: set[tuple[int, str]] = set()
    page = 1
    for _ in range(MAX_PAGES):
        body = await _get(c, "/v1/subnets/owners", {"limit": 200, "page": page})
        rows, nxt = W.parse_delegation_response(body)
        for r in rows:
            if r.get("is_coldkey_swap") and r.get("netuid") is not None:
                ck = W.ss58_of(r.get("owner") or r.get("coldkey"))
                if ck:
                    out.add((int(r["netuid"]), ck))
        if not nxt:
            break
        page = nxt
    return out


async def extrinsic_sweep(c: "Collector", ck: str, occs: list[dict]) -> int:
    n = 0
    for name in ("set_perpetual_lock", "announce_coldkey_swap"):
        body = await _get(c, "/v1/extrinsics", {"signer_address": ck, "pallet": "SubtensorModule", "name": name,
                                                "include_args": "true", "limit": 200})
        rows, _ = W.parse_delegation_response(body)
        for r in rows:
            ts = W.parse_ts(r.get("timestamp"))
            if ts is None or not r.get("success", True):
                continue
            args = r.get("args") or {}
            xid = r.get("id") or r.get("extrinsic_id")
            block = r.get("block_number")
            for occ in occs:
                okey, nu = occ["occupant_key"], int(occ["netuid"])
                if args.get("netuid") not in (None, nu):
                    continue
                if name == "set_perpetual_lock" and args.get("enabled") is False:
                    cid = await D.record_lock_transition(
                        c.db, okey, nu, "perpetual_off",
                        {"hour_ts": ts, "block_number": block, "perp_before": None, "perp_after": None,
                         "decay_before": None, "decay_after": None, "coldkey": ck},
                        observed_at=c.now, run_id=c.run_id, detected_via="taostats", extrinsic_id=xid)
                    n += 1 if cid else 0
                elif name == "announce_coldkey_swap":
                    ch = OwnerChange(okey, nu, "coldkey_swap_announced", ts, c.now, "taostats", old_value=ck,
                                     new_value=W.ss58_of(args.get("new_coldkey")), block_number=block,
                                     extrinsic_id=xid, evidence={"args": args}, run_id=c.run_id)
                    cid = await D.record_change(
                        c.db, ch, event_type="owner_coldkey_swap_announced", review_kind="owner_change_whitelist",
                        priority=2, review_event_type="owner_coldkey_swap_announced",
                        payload={"extrinsic_id": xid, "block_number": block, "announced_at": ts},
                        event_dedupe=f"{okey}|announce|{xid}")
                    n += 1 if cid else 0
    return n


async def run(c: "Collector", by_ck: dict[str, list[dict]]) -> dict:
    out: dict = {"active": False}
    if not active():
        c.c8_status = "skip"
        return out
    out["active"] = True
    c.c8_status = "ok"
    c.taostats_swaps = await owner_history(c)
    weekly = datetime.fromtimestamp(c.now, timezone.utc).weekday() == int(
        app_config.settings.OWNER_TAOSTATS_EXTRINSIC_WEEKDAY)
    for ck, occs in sorted(by_ck.items()):
        out["events"] = out.get("events", 0) + await delegations(c, ck, occs)
        if weekly:
            out["extrinsic_changes"] = out.get("extrinsic_changes", 0) + await extrinsic_sweep(c, ck, occs)
    out["swaps"] = len(c.taostats_swaps)
    return out


__all__ = ["active", "run", "json"]
