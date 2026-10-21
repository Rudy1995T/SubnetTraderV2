"""Spec 11 §9 T26–T29, T32, T35, T45: detectors, whitelist PIT, key-compromise override, RPC history seed."""
from __future__ import annotations

import json

import pytest

from app.integrity import detectors as D
from app.integrity import owner_admin
from app.integrity.features import compute_occupant
from app.integrity.owner_history import seed_from_rpc_owner_history
from app.integrity.owner_watch import watch
from tests.integrity.conftest import (DAY, HOUR, NEWCK, NEWHK, OWNER, OWNER_HK, T0, seed_coverage, seed_hourly,
                                      seed_occ, seed_snapshot, seed_stake)

pytestmark = pytest.mark.asyncio


def by_key(rows):
    return {r.key: r for r in rows}


async def observable(db, o, t, *, cut=0.18, pool=150_000.0, snap_days=31):
    """Daily snapshots (cut 0.18 α/block → earned 38,880 α/30 d), an SSE row ≤ 2 h old and full C4 coverage."""
    k, n = o["occupant_key"], o["netuid"]
    for d in range(snap_days, -1, -1):
        await seed_snapshot(db, k, n, t - d * DAY - 600, owner_cut_per_block=cut, owner_cut_enabled=1,
                            owner_total_alpha=200_000.0, owner_available_alpha=50_000.0, tmc_owner_perpetual=150_000.0)
    await seed_hourly(db, k, n, t - t % HOUR, perp=150_000.0, stake=200_000.0, tao_liq=pool)
    await seed_coverage(db, OWNER, start=t - 40 * DAY, ts=t)


async def test_oro_compromise_override(db, iset):                                  # T26
    o = await seed_occ(db, 15, reg=7366897)
    k, n = o["occupant_key"], 15
    dump = T0 - 9 * DAY
    for h in range(-200, 260):
        await seed_hourly(db, k, n, dump + h * HOUR - dump % HOUR, perp=150_000.0, stake=200_000.0, tao_liq=150_000.0)
    for i in range(30):
        await seed_stake(db, k, n, f"oro-{i}", kind="sell", alpha=4_900.0, tao=200.0, event_at=dump + i * 1200,
                         block=8_000_000 + i, extrinsic=f"8000{i}-1")
    swap_at = dump + 8 * DAY

    async def classify():
        return "coldkey_swap", "old_ck_emptied"
    cid = await D.detect_coldkey_change(db, k, n, OWNER, NEWCK, event_at=swap_at, observed_at=swap_at, run_id="t",
                                        detected_via="tmc_snapshot", classify=classify)
    t = swap_at + 60
    await observable(db, o, t, pool=150_000.0)
    r = by_key(await compute_occupant(db, o, t=t))
    assert r["owner_retention_30d"].value == 0.0 and "sell_impact_7d" in r["owner_retention_30d"].value_json["hard"]
    assert r["owner_extraction_trigger"].value == 1.0
    assert r["owner_control_change_trigger"].value == 1.0
    assert r["owner_control_change_trigger"].value_json["trigger_ids"] == [f"owner_changes:{cid}"]
    assert r["control_stability_90d"].value == pytest.approx(0.4, abs=1e-3)
    T = t + HOUR
    await db.execute("INSERT INTO red_flag_state (occupant_key, flag, spec_version, active, last_eval_as_of, "
                     "updated_at) VALUES (?, 'owner_extraction', 'ts-1.0', 1, ?, ?)", (k, t, t))
    long = await owner_admin.override_key_compromise(db, netuid=15, window_start=dump - HOUR, window_end=dump + 60 * DAY,
                                                     evidence_url="https://protos.com/oro", by="rudolf", now=T)
    assert long.error == "window_too_long"
    res = await owner_admin.override_key_compromise(db, netuid=15, window_start=dump - HOUR, window_end=swap_at + DAY,
                                                    evidence_url="https://protos.com/oro", by="rudolf", now=T)
    assert res.ok and res.warning == "flag_not_active:owner_control_change"
    flags = await db.fetchall("SELECT flag, state FROM manual_flags ORDER BY id")
    assert [(f["flag"], f["state"]) for f in flags] == [("key_compromise", "confirmed"), ("owner_extraction", "cleared")]
    rv = await db.fetchone("SELECT status, resolution_action FROM review_queue WHERE kind = 'owner_change_whitelist'")
    assert (rv["status"], rv["resolution_action"]) == ("confirmed", "key_compromise")
    assert await db.fetchval("SELECT COUNT(*) FROM events WHERE event_type = 'owner_key_compromise_override'") == 1
    await seed_snapshot(db, k, n, T + 60, owner_cut_per_block=0.18, owner_cut_enabled=1, owner_total_alpha=200_000.0,
                        owner_available_alpha=50_000.0, tmc_owner_perpetual=150_000.0)
    after = by_key(await compute_occupant(db, o, t=T + 120))
    assert after["owner_retention_30d"].value == pytest.approx(1.0)
    assert after["control_stability_90d"].value == pytest.approx(1.0)
    assert all(e["whitelisted"] for e in after["control_stability_90d"].value_json["events"])
    assert after["owner_extraction_trigger"].value == 0.0 and after["owner_control_change_trigger"].value == 0.0
    before = by_key(await compute_occupant(db, o, t=t))                            # PIT: published values unchanged
    assert before["owner_retention_30d"].value == 0.0 and before["owner_control_change_trigger"].value == 1.0


async def test_perpetual_off_detection(db, iset):                                  # T27
    o = await seed_occ(db, 79, reg=5173967)
    k, n = o["occupant_key"], 79
    h0 = T0 - T0 % HOUR
    await seed_hourly(db, k, n, h0, perp=500_000.0, decay=0.0)
    await seed_hourly(db, k, n, h0 + HOUR, perp=20_000.0, decay=480_000.0)
    out = await watch(db, now=h0 + 2 * HOUR, run_id="w", since=h0)
    assert out["changes"] == 1
    c = await db.fetchone("SELECT * FROM owner_changes WHERE change_type = 'perpetual_off'")
    assert c and c["s3_base_penalty"] == 1.0
    assert await db.fetchval("SELECT dedupe_key FROM events WHERE event_type = 'owner_perpetual_off'") == \
        f"{k}|perpoff|{(h0 + HOUR) // DAY}"
    assert await db.fetchval("SELECT kind FROM review_queue") == "owner_extraction_candidate"
    t = h0 + 3 * HOUR
    await observable(db, o, t)
    r = by_key(await compute_occupant(db, o, t=t))
    assert r["control_stability_90d"].value == 0.0
    assert r["owner_extraction_trigger"].value == 1.0 and r["owner_extraction_trigger"].value_json["reasons"] == \
        ["perpetual_off"]
    t2 = h0 + 92 * DAY
    await observable(db, o, t2, snap_days=1)
    assert by_key(await compute_occupant(db, o, t=t2))["owner_extraction_trigger"].value == 0.0
    # analytics_1w pair (perp = locked − decaying) detects the same drop; an sse→analytics pair is skipped
    rows = [{"hour_ts": 0, "source": "analytics_1w", "owner_personal_locked_alpha": 5e14,
             "owner_personal_decaying_alpha": 0.0, "owner_personal_perpetual_alpha": None},
            {"hour_ts": HOUR, "source": "analytics_1w", "owner_personal_locked_alpha": 5e14,
             "owner_personal_decaying_alpha": 4.8e14, "owner_personal_perpetual_alpha": None}]
    assert [c for c, _ in D.detect_lock_transitions(rows, min_alpha=100, match_tol=0.1)] == ["perpetual_off"]
    mixed = [dict(rows[0], source="sse", owner_personal_perpetual_alpha=5e14), rows[1]]
    assert D.detect_lock_transitions(mixed, min_alpha=100, match_tol=0.1) == []
    flat = [{"hour_ts": 0, "source": "sse", "owner_personal_perpetual_alpha": 0.0,
             "owner_personal_decaying_alpha": 5.8e13, "owner_personal_locked_alpha": 5.8e13}] * 2
    assert D.detect_lock_transitions([dict(flat[0]), dict(flat[1], hour_ts=HOUR)], min_alpha=100, match_tol=0.1) == []
    add = [{"hour_ts": 0, "source": "sse", "owner_personal_perpetual_alpha": 1e14, "owner_personal_decaying_alpha": 0.0},
           {"hour_ts": HOUR, "source": "sse", "owner_personal_perpetual_alpha": 1e14,
            "owner_personal_decaying_alpha": 1e13}]
    assert [c for c, _ in D.detect_lock_transitions(add, min_alpha=100, match_tol=0.1)] == ["decaying_lock_added"]
    red = [{"hour_ts": 0, "source": "sse", "owner_personal_perpetual_alpha": 1e14, "owner_personal_decaying_alpha": 0.0},
           {"hour_ts": HOUR, "source": "sse", "owner_personal_perpetual_alpha": 9e13,
            "owner_personal_decaying_alpha": 0.0}]
    assert [c for c, _ in D.detect_lock_transitions(red, min_alpha=100, match_tol=0.1)] == ["lock_reduced"]
    cid = await D.record_lock_transition(db, k, n, "lock_reduced", D.detect_lock_transitions(red, min_alpha=100,
                                                                                             match_tol=0.1)[0][1]
                                         | {"hour_ts": h0 + 5 * DAY}, observed_at=h0 + 5 * DAY, run_id="t")
    assert cid and await db.fetchval("SELECT COUNT(*) FROM review_queue WHERE event_type = 'owner_lock_reduced'") == 1


async def test_repoint_identity_churn_decay(db, iset):                             # T28
    o = await seed_occ(db, 64)
    k, n = o["occupant_key"], 64
    d0 = T0
    await D.detect_hotkey_repoint(db, k, n, OWNER_HK, NEWHK, event_at=d0, observed_at=d0, run_id="t",
                                  detected_via="tmc_snapshot")
    from app.storage.events import emit_event
    await emit_event(db, event_type="identity_changed", occupant_key=k, netuid=n, event_at=d0 + 10 * DAY,
                     observed_at=d0 + 10 * DAY, source="registry", run_id="r", dedupe_key="x1",
                     payload={"netuid": n, "occupant_key": k, "source": "chain", "field": "github_repo", "kind": "x",
                              "old_value": "a", "new_value": "b", "old_hash": "1", "new_hash": "2",
                              "old_observed_at": 1, "resolved_changed": True})
    made = await D.pair_identity_churn(db, k, now=d0 + 10 * DAY + 60, run_id="t")
    assert len(made) == 1
    c = await db.fetchone("SELECT * FROM owner_changes WHERE change_type = 'hotkey_repoint_identity_churn'")
    assert c["event_at"] == d0 + 10 * DAY and c["s3_base_penalty"] == 0.4
    assert await D.pair_identity_churn(db, k, now=d0 + 11 * DAY, run_id="t") == []
    t10 = d0 + 10 * DAY + 120
    await observable(db, o, t10)
    assert by_key(await compute_occupant(db, o, t=t10))["control_stability_90d"].value == pytest.approx(0.6, abs=1e-3)
    t55 = d0 + 55 * DAY
    await observable(db, o, t55, snap_days=1)
    assert by_key(await compute_occupant(db, o, t=t55))["control_stability_90d"].value == pytest.approx(0.8, abs=1e-3)

    async def cl():
        return "coldkey_swap", "subnetradar"
    await D.detect_coldkey_change(db, k, n, OWNER, NEWCK, event_at=d0 + 5 * DAY, observed_at=d0 + 5 * DAY, run_id="t",
                                  detected_via="tmc_snapshot", classify=cl)
    t = d0 + 12 * DAY
    await observable(db, o, t, snap_days=1)
    cc = by_key(await compute_occupant(db, o, t=t))["owner_control_change_trigger"]
    assert cc.value == 1.0 and cc.value_json["identity_churn"] is True
    t40 = d0 + 40 * DAY
    await observable(db, o, t40, snap_days=1)
    assert by_key(await compute_occupant(db, o, t=t40))["owner_control_change_trigger"].value == 0.0


async def test_s3_decay_whitelist_pit(db, iset):                                   # T29
    o = await seed_occ(db, 64)
    k, n = o["occupant_key"], 64
    t = T0
    swap = t - 45 * DAY

    async def cl():
        return "owner_transfer", "old_ck_retains_stake"
    await D.detect_coldkey_change(db, k, n, OWNER, NEWCK, event_at=swap, observed_at=swap, run_id="t",
                                  detected_via="tmc_snapshot", classify=cl)
    await observable(db, o, t + 7200, snap_days=1)
    s3 = lambda rows: by_key(rows)["control_stability_90d"].value  # noqa: E731
    assert s3(await compute_occupant(db, o, t=t)) == pytest.approx(0.7, abs=1e-3)
    W = t + 3600
    w = await owner_admin.add_whitelist(db, netuid=64, reason="multisig_migration", applies_to="coldkey_change",
                                        window_start=swap - DAY, window_end=swap + DAY,
                                        evidence_url="https://x.com/team/status/1", by="rudolf", now=W)
    assert w.ok
    assert s3(await compute_occupant(db, o, t=W + 60)) == pytest.approx(1.0)
    assert s3(await compute_occupant(db, o, t=t)) == pytest.approx(0.7, abs=1e-3)
    await owner_admin.retire_whitelist(db, w.id, by="rudolf", now=W + 600)
    assert s3(await compute_occupant(db, o, t=W + 1200)) == pytest.approx(0.7, abs=2e-3)
    bad = await owner_admin.add_whitelist(db, netuid=64, reason="multisig_migration", applies_to="perpetual_off",
                                          window_start=swap, window_end=swap + 1, evidence_url="https://a.io/x",
                                          by="r", now=W)
    assert bad.error == "invalid_transition"
    import sqlite3
    with pytest.raises(sqlite3.IntegrityError):
        await db.execute("INSERT INTO owner_event_whitelist (occupant_key, netuid, reason, applies_to, window_start, "
                         "window_end, evidence_url, created_by, created_at) VALUES (?, 64, 'multisig_migration', "
                         "'perpetual_off', 1, 2, 'https://a.io', 'x', 1)", (k,))


async def test_treasury_whitelist_dest_coldkey(db, iset):
    """§11 Q20: a stake-event whitelist with new_coldkey matches only transfers to that destination."""
    o = await seed_occ(db, 64)
    k = o["occupant_key"]
    t = T0
    await seed_stake(db, k, 64, "tr-1", kind="transfer_out", alpha=40_000.0, event_at=t - 5 * DAY, dest="5Treasury")
    await seed_stake(db, k, 64, "tr-2", kind="transfer_out", alpha=1_000.0, event_at=t - 4 * DAY, dest="5Elsewhere")
    await observable(db, o, t)
    assert by_key(await compute_occupant(db, o, t=t))["owner_retention_30d"].value == 0.0     # 41k > 38.9k earned
    await owner_admin.add_whitelist(db, netuid=64, reason="disclosed_transfer", applies_to="stake_events",
                                    window_start=t - 400 * DAY, window_end=t + 400 * DAY, new_coldkey="5Treasury",
                                    evidence_url="https://chutes.ai/treasury", by="rudolf", now=t + 1)
    s1 = by_key(await compute_occupant(db, o, t=t + 2))["owner_retention_30d"]
    assert s1.value == pytest.approx(1 - 1_000 / 38_880, abs=1e-3) and s1.value_json["n_whitelisted_excluded"] == 1


async def test_reoccupancy_isolation(db, iset):                                    # T32
    old = await seed_occ(db, 92, reg=7013758, status="deregistered")
    new = await seed_occ(db, 92, reg=8352006)
    h = T0 - T0 % HOUR
    await seed_hourly(db, old["occupant_key"], 92, h - HOUR, perp=1.0, owner=OWNER)
    await seed_hourly(db, new["occupant_key"], 92, h, perp=1.0, owner=NEWCK)
    await seed_hourly(db, new["occupant_key"], 92, h + HOUR, perp=1.0, owner=NEWCK)
    out = await watch(db, now=h + 2 * HOUR, run_id="w", since=h - 2 * HOUR)
    assert out["changes"] == 0 and await db.fetchval("SELECT COUNT(*) FROM owner_changes") == 0


async def test_coldkey_change_classification(db, iset):                            # T35
    f = D.classify_coldkey_change
    assert f(taostats_is_swap=True, challenger_coldkeys=[NEWCK], old_ck=OWNER, new_ck=NEWCK, netuid=1) == \
        ("coldkey_swap", "taostats_is_coldkey_swap")
    assert f(challenger_coldkeys=[NEWCK], old_ck=OWNER, new_ck=NEWCK, netuid=1) == ("subnet_owner_changed",
                                                                                     "rufus_challenger")
    radar = [{"netuid": None, "old_coldkey": OWNER, "new_coldkey": NEWCK}]
    assert f(radar_rows=radar, old_ck=OWNER, new_ck=NEWCK, netuid=1) == ("coldkey_swap", "subnetradar")
    assert f(old_ck=OWNER, new_ck=NEWCK, netuid=1, old_ck_share=0.6, old_ck_status="ok") == \
        ("owner_transfer", "old_ck_retains_stake")
    assert f(old_ck=OWNER, new_ck=NEWCK, netuid=1, old_ck_share=0.0, old_ck_status="ok") == \
        ("coldkey_swap", "old_ck_emptied")
    assert f(old_ck=OWNER, new_ck=NEWCK, netuid=1, old_ck_status="fail:500") == ("coldkey_change_unclassified",
                                                                                 "unclassified")
    o = await seed_occ(db, 64)

    async def cl():
        return "coldkey_change_unclassified", "unclassified"
    cid = await D.detect_coldkey_change(db, o["occupant_key"], 64, OWNER, NEWCK, event_at=T0, observed_at=T0,
                                        run_id="t", detected_via="tmc_snapshot", classify=cl)
    assert await db.fetchval("SELECT s3_base_penalty FROM owner_changes WHERE id = ?", (cid,)) == 0.6
    ev = await db.fetchone("SELECT severity, payload FROM events WHERE event_type = 'owner_control_change'")
    assert ev["severity"] == "warning" and json.loads(ev["payload"])["review_id"]
    rv = await db.fetchone("SELECT kind, event_type, priority FROM review_queue")
    assert (rv["kind"], rv["event_type"], rv["priority"]) == ("owner_change_whitelist", "owner_control_change", 1)
    assert await D.detect_coldkey_change(db, o["occupant_key"], 64, OWNER, NEWCK, event_at=T0 + HOUR,
                                         observed_at=T0 + HOUR, run_id="t", detected_via="chain_hourly",
                                         classify=cl) is None                          # same change, other detector


async def test_rpc_owner_history_seed(db, iset):                                   # T45
    o = await seed_occ(db, 70, reg=8825571)
    k = o["occupant_key"]
    B = 9_030_999
    rows = [(8_825_866, OWNER, OWNER_HK, "first"), (B, NEWCK, OWNER_HK, "owner_coldkey"),
            (B + 300, NEWCK, OWNER_HK, "owner_coldkey"), (B + 600, NEWCK, NEWHK, "owner_hotkey"),
            (8_000_000, "5Old" + "x" * 44, OWNER_HK, "owner_coldkey")]
    for blk, ck, hk, ch in rows:
        await db.execute("INSERT INTO rpc_owner_history (netuid, block_number, occupant_key, event_at, observed_at, "
                         "owner_coldkey, owner_hotkey, change) VALUES (70,?,?,?,?,?,?,?)",
                         (blk, k, T0 - (9_300_000 - blk) * 12, T0, ck, hk, ch))
    imp = T0 + DAY
    out = await seed_from_rpc_owner_history(db, now=imp, run_id="seed")
    assert (out["coldkey"], out["hotkey"]) == (1, 1) and out["collapsed"] == 1
    ch = await db.fetchall("SELECT change_type, detected_via, event_at, observed_at, evidence_json FROM owner_changes "
                           "ORDER BY event_at")
    assert [c["change_type"] for c in ch] == ["coldkey_change_unclassified", "hotkey_repoint"]
    assert all(c["detected_via"] == "rpc_owner_history" and c["observed_at"] == imp
               and json.loads(c["evidence_json"])["backfilled"] for c in ch)
    assert await db.fetchval("SELECT COUNT(*) FROM events WHERE alerted_at IS NULL") == 0
    again = await seed_from_rpc_owner_history(db, now=imp + 1, run_id="seed2")
    assert (again["coldkey"], again["hotkey"]) == (0, 0)
    from datetime import datetime, timezone
    from app.integrity.features import clear_cache, compute_control_stability_90d
    clear_cache()
    D_ = ch[0]["event_at"] + 10 * DAY
    strict = await compute_control_stability_90d(db, k, datetime.fromtimestamp(D_, timezone.utc))
    assert strict.value is None                                                   # no snapshot, import not yet seen
    rec = await compute_control_stability_90d(db, k, datetime.fromtimestamp(D_, timezone.utc), mode="reconstructed")
    assert rec.value == pytest.approx(1 - 0.6 * 0.5 ** (10 / 45), abs=1e-3)
