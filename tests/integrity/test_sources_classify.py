"""Spec 11 §9 T16, T17, T19, T33, T34 + the verified C1–C4 mappers on trimmed live fixtures (§2.3.1)."""
from __future__ import annotations

import pytest

from app.integrity import owner_sources as S
from app.integrity.classify import classify_stake_event, mark_phantom_moves, supersede_duplicates
from app.whale import events as W
from tests.integrity.conftest import OTHER, OWNER, OWNER_HK, T0, fx_json, fx_text, seed_occ, seed_stake

N = 64


def row(fn, **kw):
    base = {"id": f"{fn}-1", "extrinsic": "9-1", "block_number": 9, "event_at": T0, "function": fn, "success": True,
            "signer": OWNER, "from_coldkey": OWNER, "to_coldkey": OWNER, "from_netuid": N, "to_netuid": N,
            "from_alpha": 10.0, "to_alpha": 10.0, "from_tao": 1.0, "to_tao": 1.0}
    base.update(kw)
    return base


@pytest.mark.parametrize("r,kind,out", [
    (row("remove_stake_limit", from_coldkey=None, to_netuid=None), "sell", 1),
    (row("SubtensorModule.remove_stake", to_netuid=None), "sell", 1),
    (row("unstake_all_alpha", from_netuid=None, to_netuid=None), "sell_full", 1),
    (row("remove_stake_full_limit", to_netuid=None), "sell_full", 1),
    (row("swap_stake", to_netuid=12), "swap_out", 1),
    (row("move_stake"), "move_internal", 0),
    (row("transfer_stake", to_coldkey=OTHER), "transfer_out", 1),
    (row("transfer_stake", to_netuid=12), "swap_out", 1),
    (row("add_stake", from_netuid=None, from_coldkey=None), "buy", 0),
    (row("swap_stake", from_netuid=12), "swap_in", 0),
    (row("transfer_stake", from_coldkey=OTHER, signer=OTHER), "transfer_in", 0),
    (row("lock_stake"), "lock", 0),
    (row("set_perpetual_lock", enabled=False), "perpetual_off", 0),
    (row("set_perpetual_lock", enabled=True), "perpetual_on", 0),
])
def test_classify_stake_event(r, kind, out):                                       # T16
    e = classify_stake_event(r, occupant_netuid=N, owner_coldkey=OWNER)
    assert e is not None and (e.kind, e.counts_as_outflow) == (kind, out)


def test_classify_edges():
    e = classify_stake_event(row("remove_stake", success=False, to_netuid=None), occupant_netuid=N, owner_coldkey=OWNER)
    assert e.success == 0 and e.counts_as_outflow == 0
    e = classify_stake_event(row("remove_stake", signer=OTHER, to_netuid=None), occupant_netuid=N, owner_coldkey=OWNER)
    assert e.is_proxy == 1 and e.counts_as_outflow == 1
    assert classify_stake_event(row("remove_stake", from_netuid=12, to_netuid=None), occupant_netuid=N,
                                owner_coldkey=OWNER) is None
    act = fx_json("staking_activity_64.json")["results"]
    evs = [classify_stake_event(S.map_activity_row(r), occupant_netuid=N, owner_coldkey=OWNER) for r in act]
    kinds = [e.kind for e in evs if e]
    assert kinds[0] == "transfer_out" and "move_internal" in kinds
    assert evs[0].alpha == pytest.approx(13_000.0) and evs[0].dest_coldkey != OWNER


def test_phantom_move_pairs():                                                     # T17
    sell = W.classify_event({"id": 1, "action": "unstake", "coldkey": OWNER, "hotkey": OWNER_HK, "netuid": N,
                             "alpha": "5000000000000", "amount": "300000000000", "block_number": 9_000_000,
                             "timestamp": "2026-10-01T00:00:00.000Z", "is_transfer": False})
    buy = W.classify_event({"id": 2, "action": "stake", "coldkey": OWNER, "hotkey": "5Other", "netuid": N,
                            "alpha": "5000000000000", "amount": "300000000000", "block_number": 9_000_000,
                            "timestamp": "2026-10-01T00:00:00.000Z", "is_transfer": None})
    evs = [classify_stake_event(r, occupant_netuid=N, owner_coldkey=OWNER) for r in (sell, buy)]
    assert [e.kind for e in evs] == ["sell", "buy"]
    out = mark_phantom_moves(evs)
    assert [e.kind for e in out] == ["move_internal", "move_internal"] and not any(e.counts_as_outflow for e in out)
    assert W.classify_event({"action": "stake", "netuid": 1, "block_number": 100, "timestamp": "2024-01-01T00:00:00Z"}) \
        is None                                                                    # pre-dTAO


@pytest.mark.asyncio
async def test_source_supersede_pit(db, iset):                                     # T19
    from app.integrity.features import _stake_events
    o = await seed_occ(db, N)
    k = o["occupant_key"]
    await seed_stake(db, k, N, "tmc-1", kind="sell", alpha=100.0, event_at=T0, block=9, observed_at=T0)
    await seed_stake(db, k, N, "ts-1", kind="sell", alpha=100.05, event_at=T0, block=9, observed_at=T0 + 3600,
                     source="taostats_delegation")
    assert await supersede_duplicates(db, [k], T0 + 3600) == 1
    before = await _stake_events(db, k, T0 - 1, T0 + 1800, "strict")
    assert [r["event_uid"] for r in before] == ["tmc-1"]
    after = await _stake_events(db, k, T0 - 1, T0 + 7200, "strict")
    assert [r["event_uid"] for r in after] == ["ts-1"]


def test_c1_c2_c3_mappers():
    k = S.map_subnet_row(fx_json("subnet_64.json"), source="tmc_detail")
    assert (k.coldkey, k.hotkey) == (OWNER, OWNER_HK)
    assert k.owner_cut_per_block == pytest.approx(0.18, abs=1e-3) and k.owner_cut_enabled is True
    assert k.perpetual == pytest.approx(646_673.716, abs=1e-2) and k.network_registered_at == 4531295
    lk = S.map_subnet_row(fx_json("subnet_64.json"), source="tmc_list")
    assert lk.perpetual is None and lk.coldkey == OWNER                      # list rows carry 0 → not used
    m = S.map_coldkey_stakes(fx_json("coldkey_stakes_64.json"), 64)
    assert m["total"] == pytest.approx(672_572.83, abs=0.01) and m["is_subnet_owner"] is True
    assert m["available"] == pytest.approx(25_899.11, abs=0.01)
    assert S.map_coldkey_stakes({"subnets": []}, 64)["total"] == 0.0
    rows = S.map_neuron_rows(fx_json("neuron_64.json"))
    assert len(rows) == 6 and rows[0]["hour_ts"] % 3600 == 0 and rows[0]["conviction_perpetual_alpha"] > 1e5


def test_rufus_parser_tolerant():                                                  # T33
    a = S.parse_rufus(fx_json("rufus_table.json"))
    b = S.parse_rufus(fx_json("rufus_list.json"))
    assert set(a) == set(b) == {24, 36, 64, 79}
    assert a[64] == b[64]
    assert a[64].auto_lock_enabled is True and a[64].owner_pct == pytest.approx(0.19955)
    assert a[79].owner_perp == pytest.approx(1_269_236.8) and a[79].auto_lock_enabled is False
    assert a[64].registration_block == 4531295 and a[64].owner_coldkey == OWNER
    assert S.parse_rufus("garbage") == {} and S.parse_rufus(None) == {}


def test_subnetradar_parser():                                                     # T34
    rows = S.parse_subnetradar(fx_text("subnetradar.html"))
    assert len(rows) == 3
    assert rows[0]["netuid"] == 113 and S.is_ss58(rows[0]["new_coldkey"]) and rows[0]["block"] == 9303541
    assert rows[1]["netuid"] is None                                          # "All subnets"
    assert rows[0]["date_ts"] is not None
    assert S.parse_subnetradar("<html><p>nothing</p></html>") == []
    assert S.parse_subnetradar("\x00garbage<tr><td>") == []


def test_ss58_encode_roundtrip():
    n = 0
    for ch in OWNER:
        n = n * 58 + S._B58.index(ch)
    pk = n.to_bytes(35, "big")[1:33].hex()
    assert S.ss58_encode("0x" + pk) == OWNER
    assert S.ss58_encode("0x1234") is None
