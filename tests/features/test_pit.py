"""Spec 12 §9 test_pit / test_store / test_immutability (metric_values side): PIT selection, first success wins,
insert-once norm rows, provisional pruning, derived metrics."""
import sqlite3

import pytest

from app.features import derived as D
from app.features.engine import FeatureEngine
from app.features.store import prune_norm_provisional, write_norm_rows
from app.features.types import RawRow
from app.storage.pit import put_metric_values
from tests.scoring.fixtures import OBS, SLOT, add_occupant, raw

O = "5:500"
K = "gate_margin"                      # freshness 1 d


async def _sel(db, slot=SLOT, cutoff=None, mode="strict", key=K):
    eng = FeatureEngine(db)
    return (await eng.select_raw([key], slot, SLOT + 900 if cutoff is None else cutoff, mode)).get((O, key))


@pytest.mark.asyncio
async def test_observed_after_cutoff_invisible(db):
    await put_metric_values(db, [raw(O, K, 2.0, observed_at=SLOT + 1000)])
    assert await _sel(db, cutoff=SLOT + 999) is None
    assert (await _sel(db, cutoff=SLOT + 1000)).value == 2.0


@pytest.mark.asyncio
async def test_first_success_wins_and_failed_upgrade(db):
    await put_metric_values(db, [raw(O, K, 2.0)])
    assert await put_metric_values(db, [raw(O, K, 3.0, observed_at=OBS + 10)]) == 0
    assert (await _sel(db)).value == 2.0
    for st in ("absent", "not_applicable"):
        k2 = "exit_capacity" if st == "absent" else "review_practice_90d"
        await put_metric_values(db, [raw(O, k2, 0.0 if st == "absent" else None, st)])
        assert await put_metric_values(db, [raw(O, k2, .5, observed_at=OBS + 10)]) == 0
    # failed → replaced by a later success; a replay before the replacement selects the previous fresh as_of
    await put_metric_values(db, [raw(O, K, 1.0, as_of=SLOT - 6 * 3600, observed_at=SLOT - 6 * 3600 + 60)])
    k = "net_flow_30d"
    await put_metric_values(db, [raw(O, k, 0.1, as_of=SLOT - 6 * 3600, observed_at=SLOT - 6 * 3600 + 60),
                                 raw(O, k, None, "failed", observed_at=OBS)])
    assert await put_metric_values(db, [raw(O, k, None, "failed", observed_at=OBS + 5)]) == 0     # failure keeps first
    assert (await _sel(db, key=k)).as_of == SLOT - 6 * 3600                                    # older fresh row used
    assert await put_metric_values(db, [raw(O, k, 0.2, observed_at=SLOT + 2000)]) == 1
    assert (await _sel(db, key=k, cutoff=SLOT + 1999)).as_of == SLOT - 6 * 3600
    assert (await _sel(db, key=k, cutoff=SLOT + 2000)).value == 0.2


@pytest.mark.asyncio
async def test_status_none_read_as_failed(db):
    await put_metric_values(db, [raw(O, K, 1.0, as_of=SLOT - 3600)])
    await db.execute("INSERT INTO metric_values (occupant_key, metric_key, as_of, spec_version, stage, value, "
                     "value_json, observed_at, source, run_id) VALUES (?,?,?,?,?,?,?,?,?,?)",
                     (O, K, SLOT, "ts-1.0", "raw", None, '{"reason":"legacy"}', OBS, "chain", "x"))
    r = await _sel(db)
    assert r.as_of == SLOT - 3600 and r.status == "ok"


@pytest.mark.asyncio
async def test_only_failed_candidates_returns_latest_failed(db):
    await put_metric_values(db, [raw(O, K, None, "failed", as_of=SLOT - 3600, reason="old"),
                                 raw(O, K, None, "failed", reason="new")])
    r = await _sel(db)
    assert r.status == "failed" and r.reason == "new"


@pytest.mark.asyncio
async def test_freshness_boundary(db):
    await put_metric_values(db, [raw(O, K, 1.0, as_of=SLOT - 86400, observed_at=SLOT - 86400 + 5)])
    assert (await _sel(db)).value == 1.0                       # age = window → fresh
    assert await _sel(db, slot=SLOT + 1) is None               # +1 s → stale


@pytest.mark.asyncio
async def test_backfilled_admitted_only_reconstructed(db):
    await put_metric_values(db, [raw(O, K, 1.0, observed_at=SLOT + 99999, backfilled=1)])
    assert await _sel(db) is None
    assert (await _sel(db, mode="reconstructed")).value == 1.0


@pytest.mark.asyncio
async def test_norm_rows_never_raw_input_and_insert_once(db):
    row = {"occupant_key": O, "metric_key": K, "as_of": SLOT, "spec_version": "ts-1.0", "stage": "norm",
           "value": .5, "value_json": {"raw": 1}, "observed_at": OBS, "source": "scoring", "run_id": "r",
           "backfilled": 0}
    assert await write_norm_rows(db, [row]) == 1
    assert await _sel(db) is None
    assert await write_norm_rows(db, [{**row, "value": .9}]) == 0
    assert await db.fetchval("SELECT value FROM metric_values WHERE stage = 'norm'") == .5
    with pytest.raises(sqlite3.IntegrityError):
        await db.execute("DELETE FROM metric_values WHERE stage = 'norm' AND as_of = ?", (SLOT,))
    prov = [{**row, "as_of": SLOT - 30 * 86400 + 6 * 3600}, {**row, "as_of": SLOT - 3600 * 6}]
    await write_norm_rows(db, prov)
    assert await prune_norm_provisional(db, SLOT - 14 * 86400, batch=1) == 1
    assert await db.fetchval("SELECT COUNT(*) FROM metric_values WHERE stage = 'norm'") == 2


@pytest.mark.asyncio
async def test_compute_registered_stamping(db, tmp_root):
    from app.features.types import MetricResult
    await add_occupant(db, O)
    eng = FeatureEngine(db)

    async def fn(db_, okey, when):
        return MetricResult(3.0, {})
    eng.producer_fns = lambda: {"venue_reachable": fn}
    occ = await eng.universe(SLOT)
    t = SLOT + 7 * 3600 + 120
    await eng.compute_registered(occ, t, (t // 3600) * 3600, "r1", mode="strict")
    r = await db.fetchone("SELECT * FROM metric_values WHERE metric_key = 'venue_reachable'")
    assert r["as_of"] == SLOT + 7 * 3600 and r["as_of"] % 86400 != 0 and r["observed_at"] == t
    assert r["backfilled"] == 0 and r["source"] == "community"
    # reconstructed: observed_at = now, backfilled = 1, never overwriting a live row
    await eng.compute_registered(occ, SLOT + 900, SLOT, "r2", mode="reconstructed", now=SLOT + 5 * 86400)
    r = await db.fetchone("SELECT * FROM metric_values WHERE metric_key = 'venue_reachable' AND as_of = ?", (SLOT,))
    assert r["observed_at"] == SLOT + 5 * 86400 and r["backfilled"] == 1
    n = await eng.compute_registered(occ, SLOT + 999, SLOT, "r3", mode="reconstructed", now=SLOT + 6 * 86400)
    assert n == 0


# ── derived (02 §C3) ─────────────────────────────────────────────────────────────────────────────────────────
def _r(key, value, status="ok", **vj):
    return RawRow(O, key, SLOT, value, {"status": status, **vj}, status, OBS, "x", None, 0)


def test_announcement_cadence():
    r = D.derive_announcement_cadence({"x_cadence_30d": _r("x_cadence_30d", 0.0, "absent"),
                                       "discord_announcements_30d": _r("d", 4.0), "tg_posts_30d": None})
    assert r.value == 4.0 and r.status == "ok"
    r = D.derive_announcement_cadence({"x_cadence_30d": _r("x", 0.0, "absent"), "discord_announcements_30d": None})
    assert r.status == "failed" and r.value_json["reason"] == "no_ok_leg"


def test_silence_days():
    legs = {"x_last_activity_days": _r("x", 9.0), "discord_last_activity_days": _r("d", 3.0, lower_bound=True)}
    r = D.derive_silence_days(legs, None, False)
    assert (r.value, r.value_json["min_leg"], r.value_json["lower_bound"]) == (9.0, "x_last_activity_days", False)
    r = D.derive_silence_days({"discord_last_activity_days": _r("d", 3.0, lower_bound=True)}, None, False)
    assert r.value == 3.0 and r.value_json["lower_bound"] is True
    venue0 = _r("venue_reachable", 0.0, "absent")
    assert D.derive_silence_days({}, venue0, True).status == "absent"
    assert D.derive_silence_days({}, venue0, False).status == "failed"          # an X account exists
    assert D.derive_silence_days({}, _r("venue_reachable", 1.0), True).status == "failed"
    assert D.derive_silence_days({}, None, True).status == "failed"              # venue unknown
    assert D.x_absent({"x_cadence_30d": _r("x", 0.0, "absent")}, True) is True
    assert D.x_absent({}, False) is True and D.x_absent({}, True) is False
