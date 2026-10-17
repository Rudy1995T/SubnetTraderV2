"""19 §2.4 dereg distance: pool, ties, immunity boundary, graduation ranks, k_H, safety, lines, events, network row."""
import json

import pytest

from app.features import dereg_distance as D
from tests.wb.conftest import DAY, HOUR, T0, add_bars, add_occupant, add_snapshot

IMM = 1000
HEAD = 100_000


def occ(n, reg, mp, price=None, status="active"):
    return D.Occ(f"{n}:{reg}", n, reg, mp, price if price is not None else mp, status)


def model(occs, horizons=(30, 90), gap=8.0, imm=IMM, head=HEAD):
    return D.build_model(occs, head=head, imm=imm, horizons=horizons, reg_gap_days=gap)


def test_pool_includes_not_started_and_orders_ties_by_registration():
    m = model([occ(1, 10, 0.5), occ(2, 20, 0.0, status="pre_launch"), occ(3, 5, 0.5), occ(4, 1, 0.2)])
    assert m.pool == ["2:20", "4:1", "3:5", "1:10"]                 # mp 0 first; tie 0.5 → older reg (5) first


def test_immunity_boundary_block():
    m = model([occ(1, HEAD - IMM, 0.1), occ(2, HEAD - IMM + 1, 0.1)])
    assert m.remaining_days["1:" + str(HEAD - IMM)] == 0 and "1:" + str(HEAD - IMM) in m.pool
    assert m.remaining_days["2:" + str(HEAD - IMM + 1)] == pytest.approx(12 / 86400)
    assert "2:" + str(HEAD - IMM + 1) not in m.pool


def test_graduation_adjusted_ranks():
    # immune newcomer (graduates in ~10 d, horizon 30 includes it) sits below everybody
    imm = 864_000
    head = 10_000_000
    young = head - imm + 72_000                                       # 10 days of immunity left
    m = model([occ(1, 1, 0.3), occ(2, 2, 0.4), occ(3, young, 0.1)], imm=imm, head=head)
    assert m.rank("1:1", m.pool) == 1 and m.rank(f"3:{young}", m.pool) is None
    assert m.pool_h[30][0] == f"3:{young}" and m.rank("1:1", m.pool_h[30]) == 2
    assert m.remaining_days[f"3:{young}"] == pytest.approx(10.0)


def test_k_for_horizon_and_reg_gap():
    assert D.k_for(30, 7.53) == 4 and D.k_for(90, 7.53) == 12
    assert D.k_for(30, 100) == 1 and D.k_for(30, 0.1) == 15
    assert D.reg_gap_mean([0, 8 * DAY, 14 * DAY], 8.0) == (7.0, 3)
    assert D.reg_gap_mean([5], 8.0) == (8.0, 1)


def test_safety_monotone_and_lines():
    occs = [occ(i, i, 0.1 * i) for i in range(1, 21)]
    m = model(occs, gap=7.5)                                          # k_30 = 4
    s = [m.safety(o.occupant_key, 30) for o in occs]
    assert s == sorted(s) and s[0] == 0.0 and s[-1] == 1.0
    assert m.line("1:1", m.pool, 1) == pytest.approx(0.2)            # excluding self
    assert m.line("5:5", m.pool, 1) == pytest.approx(0.1)
    assert m.line("1:1", m.pool_h[30], m.k_h[30]) == pytest.approx(0.5)
    imm_young = occ(99, 10_000_000 - 10, 0.05)
    m2 = model(occs + [imm_young], imm=864_000, head=10_000_000)
    assert m2.safety(imm_young.occupant_key, 30) == 1.0               # immune well past 30 d


def test_metric_rows_absence_rules():
    occs = [occ(1, 1, 0.0, status="pre_launch"), occ(2, 2, 0.2), occ(3, 3, 0.3), occ(4, HEAD - 10, 0.1)]
    m = model(occs)
    rows = D.metric_rows(m, as_of=T0, run_id="r", observed_at=T0, eta_days=2.0, bottom={}, ctx={"imm": IMM})
    by = {(r["occupant_key"], r["metric_key"]): r for r in rows}
    st = lambda o, k: by[(o, k)]["value_json"]["status"]        # noqa: E731
    assert st("1:1", "dereg_rank") == "ok" and by[("1:1", "dereg_rank")]["value"] == 1
    assert st(f"4:{HEAD - 10}", "dereg_rank") == "not_applicable"
    assert by[(f"4:{HEAD - 10}", "dereg_rank")]["value_json"]["graduates_at"] > T0
    assert by[("2:2", "mp_gap_to_cull_1")]["value_json"]["reason"] == "cull_line_zero"      # line = SN1 at mp 0
    assert by[("1:1", "mp_gap_to_cull_1")]["value"] == pytest.approx(-1.0)
    assert st("1:1", "spot_vs_mp") == "not_applicable"
    assert by[("3:3", "days_to_cull_static")]["value"] == pytest.approx(2.0 + 3 * 8.0)   # SN4 graduates < 30 d
    assert st("2:2", "hours_in_bottom_k_30d") == "failed"
    assert all(r["source"] == "chain" and r["value_json"]["def"] == "wb-1" for r in rows)


def test_bottom_k_hours():
    panel = {t: [("a:1", 1, 0.1), ("b:2", 2, 0.2), ("c:3", 3, 0.3 if t < 5 else 0.05)] for t in range(10)}
    blocks = {t: 10_000 for t in range(10)}
    out = D.bottom_k_hours(panel, imm=100, k=1, block_at=blocks, keys=["a:1", "c:3"])
    assert out == {"a:1": (5, 10), "c:3": (5, 10)}


def test_lock_cost_model_fits_effective_interval():
    L = 494e9
    lb, b0 = 9_289_316, 9_298_478
    cost = 2 * L - L * (b0 - lb) / 28_800
    c = {"NetworkLastLockCost": L, "NetworkRegistrationCost": cost, "_block": b0, "NetworkLastRegisteredBlock": lb,
         "NetworkMinLockCost": 1e9}
    out = D.lock_cost_model(c, b0 + 7200)
    assert out["i_eff"] == pytest.approx(28_800)
    assert out["lock_cost_tau"] == pytest.approx((cost - L * 7200 / 28_800) / 1e9)
    assert out["lock_cost_decay_tau_per_day"] == pytest.approx(494 * 7200 / 28_800)


@pytest.mark.asyncio
async def test_run_slot_end_to_end_events_and_network_row(db, wb_settings):
    wb_settings(NETWORK_IMMUNITY_PERIOD_BLOCKS=864_000)
    head = 9_300_000
    as_of = T0 + 5 * HOUR
    specs = [(1, 1_000_000, 0.010), (2, 2_000_000, 0.020), (3, 3_000_000, 0.030), (4, 4_000_000, 0.040),
             (5, 5_000_000, 0.050), (6, 6_000_000, 0.060), (7, head - 864_000 + 36_000, 0.001)]   # SN7: 5 d immunity
    snap = []
    for i, (n, reg, mp) in enumerate(specs):
        o = await add_occupant(db, n, reg, registered_ts=T0 - (60 - 8 * i) * DAY)
        snap.append({"netuid": n, "occ": o, "price": mp * 1.01, "mp": mp, "reg": reg, "block": head})
        await add_bars(db, n, o, "1h", [mp] * 48, end_ts=as_of - HOUR, mps=[mp] * 48, blocks=[head] * 48)
    await add_snapshot(db, as_of - HOUR, snap)
    # probed immunity overrides the config value
    await db.execute("INSERT INTO chain_constants (name, source, observed_at, value) VALUES "
                     "('NetworkImmunityPeriod','rpc_storage',?, '864000')", (as_of - 100,))
    rep = await D.run_slot(db, as_of, run_id="r1", observed_at=as_of + 540)
    assert rep.pool == 6 and rep.bottom[0][0] == 1
    net = await db.fetchone("SELECT * FROM wb_dereg_network_hourly WHERE hour_ts = ?", (as_of,))
    assert net["immunity_source"] == "rpc_storage" and net["k_30"] == 4 and net["graduates_30d"] == 1
    r30 = {r["occupant_key"]: r for r in await db.fetchall(
        "SELECT * FROM metric_values WHERE metric_key = 'dereg_rank_30d' AND as_of = ?", (as_of,))}
    sn7 = f"7:{head - 864_000 + 36_000}"
    assert r30[sn7]["value"] == 1 and r30["1:1000000"]["value"] == 2
    ev = await db.fetchall("SELECT event_type, dedupe_key FROM events")
    assert [e["event_type"] for e in ev] == ["immunity_ends_7d"]
    # next hour: SN4 drops below everybody → enters the k30 zone; SN3 is pushed out
    snap[3]["mp"] = 0.0001
    await add_snapshot(db, as_of, snap)
    rep2 = await D.run_slot(db, as_of + HOUR, run_id="r2", observed_at=as_of + HOUR + 540)
    types = sorted(e["event_type"] for e in await db.fetchall("SELECT event_type FROM events"))
    assert types == ["dereg_rank_cleared_k30", "dereg_rank_entered_k30", "immunity_ends_7d"]
    assert rep2.events == 2                                            # immunity_ends_7d deduped (once per occupant)
    hb = json.loads((await db.fetchone("SELECT value_json FROM metric_values WHERE metric_key = 'hours_in_bottom_k_30d' "
                                       "AND occupant_key = '1:1000000' AND as_of = ?", (as_of,)))["value_json"])
    assert hb["status"] == "ok" and hb["k"] == 4


@pytest.mark.asyncio
async def test_run_slot_without_snapshot_fails_loudly(db, wb_settings):
    wb_settings()
    with pytest.raises(RuntimeError, match="no chain snapshot"):
        await D.run_slot(db, T0, run_id="r", observed_at=T0)
