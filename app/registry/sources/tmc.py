"""Thin wrappers over 05's ``TmcClient`` + pure parsers (spec 04 §2.2, §2.5; no HTTP of its own, G2)."""
from __future__ import annotations

from typing import Any

from app.collectors.chain_tmc import TmcClient, TmcResponse
from app.registry.models import OccupantInput
from app.util.time import parse_ts

V3_MAP = {"subnetName": "subnet_name", "githubRepo": "github_repo", "subnetContact": "subnet_contact",
          "subnetUrl": "subnet_url", "discord": "discord", "description": "description", "logoUrl": "logo_url",
          "additional": "additional"}


def _int(x: Any) -> int | None:
    if x is None or x == "" or isinstance(x, bool):
        return None
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return None


def subnets_row_to_input(row: dict) -> OccupantInput | None:
    netuid = row.get("netuid")
    ls = row.get("latest_snapshot") or {}
    reg = _int(ls.get("network_registered_at"))
    if not isinstance(netuid, int) or reg is None:
        return None
    ids = ls.get("subnet_identities_v3") if isinstance(ls.get("subnet_identities_v3"), dict) else None
    feb = _int(ls.get("first_emission_block_number"))
    reg_at = None
    if row.get("registered_at"):
        try:
            reg_at = parse_ts(row["registered_at"])
        except (TypeError, ValueError):
            reg_at = None
    name = (ids or {}).get("subnetName") or None
    return OccupantInput(netuid, reg, name, ls.get("token_symbol"), bool(row.get("is_active")),
                         feb if feb else None, ls.get("subnet_owner") or None, ls.get("subnet_owner_hotkey") or None,
                         reg_at, _int(ls.get("block_number")), ids)


def table_row_to_input(row: dict) -> OccupantInput | None:
    netuid = row.get("subnet")
    reg = _int(row.get("network_registered_at"))
    if not isinstance(netuid, int) or reg is None:
        return None
    name = row.get("name")
    if name == "Unknown":
        name = None
    return OccupantInput(netuid, reg, name, row.get("symbol"), bool(row.get("is_active")), None,
                         row.get("subnet_owner") or None, None, None, _int(row.get("block_number")), None,
                         from_table=True)


def rows_of(resp: TmcResponse) -> list[dict]:
    body = resp.json
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        return body.get("results") or []
    return []


def is_sane(inputs: list[OccupantInput], rows_in: int) -> tuple[bool, str | None]:
    if not (100 <= rows_in <= 300):
        return False, f"row_count:{rows_in}"
    if len(inputs) != rows_in:
        return False, f"uncoercible_rows:{rows_in - len(inputs)}"
    if not any(i.block_number for i in inputs):
        return False, "no_block_number"
    return True, None


async def fetch_occupant_inputs(tmc: TmcClient) -> tuple[list[OccupantInput], TmcResponse | None, str, str | None]:
    """``/subnets/`` first, ``/subnets/table/`` on failure/insanity → (inputs, resp, kind, error)."""
    r = await tmc.fetch_subnets()
    err = None
    if r.ok:
        rows = rows_of(r)
        inputs = [i for i in (subnets_row_to_input(x) for x in rows) if i]
        ok, why = is_sane(inputs, len(rows))
        if ok:
            return inputs, r, "subnets", None
        err = f"subnets insane: {why}"
    else:
        err = f"subnets failed: {r.error}"
    t = await tmc.fetch_subnets_table()
    if t.ok:
        rows = rows_of(t)
        inputs = [i for i in (table_row_to_input(x) for x in rows) if i]
        ok, why = is_sane(inputs, len(rows))
        if ok:
            return inputs, t, "subnets_table", err
        return [], None, "", f"{err}; table insane: {why}"
    return [], None, "", f"{err}; table failed: {t.error}"


def parse_coldkey_identities(resp: TmcResponse) -> dict[str, dict]:
    return {r["coldkey"]: r for r in rows_of(resp) if isinstance(r, dict) and r.get("coldkey")}


def snapshot_identities(payload: Any) -> dict[int, dict[str, str]]:
    """``latest_snapshot.subnet_identities_v3`` from a stored ``/subnets/`` payload, camelCase → snake_case.
    A key missing from a netuid's dict means *not covered* (no observation), not ''."""
    out: dict[int, dict[str, str]] = {}
    rows = payload.get("results") if isinstance(payload, dict) else payload
    for row in rows or []:
        ls = (row or {}).get("latest_snapshot") or {}
        ids = ls.get("subnet_identities_v3")
        if isinstance(ids, dict) and isinstance(row.get("netuid"), int):
            out[row["netuid"]] = {V3_MAP[k]: (v or "") for k, v in ids.items() if k in V3_MAP}
    return out


def snapshot_block(payload: Any) -> int | None:
    rows = payload.get("results") if isinstance(payload, dict) else payload
    blocks = [_int(((r or {}).get("latest_snapshot") or {}).get("block_number")) for r in rows or []]
    blocks = [b for b in blocks if b]
    return max(blocks) if blocks else None
