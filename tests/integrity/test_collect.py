"""Spec 11 §9 T18, T20, T36, T37, T42: owner_collect_daily over a mocked TMC/Rufus/subnetradar/TaoStats."""
from __future__ import annotations

import json
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
import pytest_asyncio
import respx

from app.integrity.owner_collect import Collector, get_state
from app.util.time import parse_iso, to_iso
from tests.integrity.conftest import DAY, HOUR, OWNER, OWNER_HK, T0, fx_json, fx_text, seed_hourly, seed_occ

pytestmark = pytest.mark.asyncio
TMC = "https://api.taomarketcap.com/public/v1"


B58 = "ABCDEFGHJKLMNPQRSTUVWXYZ"


def ck_of(i: int) -> str:
    return "5" + B58[i % 24] + B58[(i // 24) % 24] + "x" * 45


def hk_of(n: int) -> str:
    return "5H" + B58[n % 24] + B58[(n // 24) % 24] + "y" * 44


@pytest_asyncio.fixture
async def http():
    from app.http import Http
    from app.ratelimit import ProviderLimit, ProviderLimiter, limits_from_settings
    limits = limits_from_settings()
    big = ProviderLimit(1000.0, 1000, 10 ** 6, 10 ** 7)
    limits.update(tmc=big, rufus=big, subnetradar=big, taostats=big)
    h = Http(ProviderLimiter(limits))
    yield h
    await h.aclose()


class Clock:
    def __init__(self, t: float) -> None:
        self.t = float(t)

    def __call__(self) -> float:
        return self.t

    async def sleep(self, s: float) -> None:
        self.t += s


def subnet_row(n: int, reg: int, ck: str, hk: str) -> dict:
    return {"netuid": n, "owner_personal_stake": 0, "latest_snapshot": {
        "subnet_owner": ck, "subnet_owner_hotkey": hk, "pending_owner_cut": "36000000000",
        "blocks_since_last_step": "200", "subnet_alpha_out_emission": "1000000000",
        "first_emission_block_number": str(reg + 10), "network_registered_at": str(reg), "block_number": reg + 500_000}}


async def seed_world(db, n_occ: int, n_ck: int, *, now: int, list_snapshot: bool = True) -> list[dict]:
    occs, rows = [], []
    for i in range(n_occ):
        n = i + 1
        reg = 1_000_000 + n
        ck = ck_of(i % n_ck)
        o = await seed_occ(db, n, reg=reg, owner_coldkey=ck, reg_ts=now - 400 * DAY)
        occs.append(o)
        rows.append(subnet_row(n, reg, ck, hk_of(n)))
        await seed_hourly(db, o["occupant_key"], n, now - now % HOUR, perp=1000.0, stake=5000.0)
    if list_snapshot:
        await db.execute("INSERT INTO chain_rest_snapshots (endpoint, params, observed_at, run_id, payload_json) "
                         "VALUES ('/subnets/', 'limit=200', ?, 'r', ?)",
                         (now - HOUR, json.dumps({"count": len(rows), "results": rows})))
    return occs


class Fake:
    """TMC/Rufus/subnetradar/TaoStats responder; records every request with the fake clock's time."""

    def __init__(self, clock: Clock, *, activity: dict[str, list[dict]] | None = None, c2_fail: set[str] = frozenset(),
                 stakes: dict[str, float] | None = None) -> None:
        self.clock, self.calls = clock, []
        self.activity = activity or {}
        self.c2_fail = set(c2_fail)
        self.stakes = stakes or {}

    def __call__(self, request: httpx.Request) -> httpx.Response:
        u = urlsplit(str(request.url))
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        self.calls.append((self.clock(), u.netloc, u.path, q))
        p = u.path.removeprefix("/public/v1")
        if u.netloc == "s3.hippius.com":
            return httpx.Response(200, json=fx_json("rufus_baked_at.json") if p.endswith("baked-at.json")
                                  else fx_json("rufus_table.json"))
        if u.netloc == "subnetradar.com":
            return httpx.Response(200, text=fx_text("subnetradar.html"), headers={"content-type": "text/html"})
        if u.netloc == "api.taostats.io":
            return httpx.Response(200, json={"pagination": {"next_page": None}, "data": []})
        if p.startswith("/subnets/") and p.count("/") == 3:
            n = int(p.split("/")[2])
            return httpx.Response(200, json=subnet_row(n, 1_000_000 + n, ck_of((n - 1) % 999), hk_of(n)))
        if p.startswith("/accounts/coldkey-stakes/"):
            ck = p.split("/")[3]
            if ck in self.c2_fail:
                return httpx.Response(500, json={})
            subs = [{"subnet": n, "balance_alpha": int(v * 1e9), "locked_alpha": 0, "hotkeys": []}
                    for (c, n), v in self.stakes.items() if c == ck] if self.stakes else \
                [{"subnet": i + 1, "balance_alpha": int(5000e9), "locked_alpha": int(1000e9), "hotkeys": []}
                 for i in range(200)]
            return httpx.Response(200, json={"nominator_coldkey": ck, "subnets": subs})
        if p.startswith("/analytics/neuron/"):
            return httpx.Response(200, json=[{"ts": to_iso(T0 - HOUR), "block_number": 1, "alpha_stake": 1e12,
                                              "conviction": 1e12}])
        if p.startswith("/extrinsics/staking-activity/"):
            rows = [r for r in self.activity.get(q.get("coldkey"), []) if parse_iso(r["timestamp"]) >= parse_iso(q["FROM"])
                    and parse_iso(r["timestamp"]) <= parse_iso(q["TO"])]
            off, lim = int(q.get("offset", 0)), int(q["limit"])
            page = rows[off:off + lim]
            nxt = "x" if off + lim < len(rows) else None
            return httpx.Response(200, json={"count": len(rows), "next": nxt, "previous": None, "results": page})
        return httpx.Response(404, json={})


def act(i: int, at: int, *, fn="remove_stake_limit", alpha=10.0, ck=OWNER, n=1) -> dict:
    return {"id": f"{9000 + i}-1-0", "extrinsic": f"{9000 + i}-1", "block_number": 9000 + i, "timestamp": to_iso(at),
            "function": fn, "success": True, "is_proxy": False, "signer_coldkey": ck, "from_coldkey": None,
            "to_coldkey": ck, "from_hotkey": OWNER_HK, "from_netuid": n, "to_netuid": None,
            "from_alpha_amount": int(alpha * 1e9), "from_tao_amount": int(alpha * 1e8)}


def collector(db, http, clock, now, **kw):
    return Collector(db, http, now=now, run_id=kw.pop("run_id", "run"), clock=clock, sleep=clock.sleep, **kw)


async def test_budget_and_pacing(db, iset, http):                                  # T36
    iset(OWNER_KEYS_SOURCE="auto")
    now = T0
    await seed_world(db, 128, 110, now=now)
    clock = Clock(now)
    fake = Fake(clock)
    with respx.mock(assert_all_called=False) as m:
        m.route().mock(side_effect=fake)
        out = await collector(db, http, clock, now).run()
    tmc = [c for c in fake.calls if c[1] == "api.taomarketcap.com"]
    assert len(tmc) <= 390 and out.meta["tmc_requests"] == len(tmc)
    assert out.meta["snapshots"] == 128
    times = sorted(c[0] for c in tmc)
    assert all(b - a >= 60 - 1e-6 for a, b in zip(times, times[5:]))                # ≤ 5 in any 60 s
    # detail mode (no list snapshot) stays ≤ 518 and never exceeds OWNER_TMC_MAX_PER_RUN
    await db.execute("DELETE FROM chain_rest_snapshots")
    await db.execute("DELETE FROM job_state")
    clock2 = Clock(now + DAY)
    fake2 = Fake(clock2)
    iset(OWNER_TMC_MAX_PER_RUN=300)
    with respx.mock(assert_all_called=False) as m:
        m.route().mock(side_effect=fake2)
        out2 = await collector(db, http, clock2, now + DAY, run_id="run2").run()
    assert len([c for c in fake2.calls if c[1] == "api.taomarketcap.com"]) == 300 == out2.meta["tmc_requests"]
    assert "budget_cap" in out2.note


async def test_activity_watermark_resume(db, iset, http):                          # T18
    iset(OWNER_ACTIVITY_PAGE_LIMIT=10, OWNER_ACTIVITY_MAX_PAGES=2, OWNER_BACKFILL_MAX_CALLS_PER_RUN=128)
    now = T0
    await seed_world(db, 1, 1, now=now)
    ck = ck_of(0)
    rows = [act(i, now - 30 * DAY + i * HOUR, ck=ck) for i in range(25)]
    clock = Clock(now)
    fake = Fake(clock, activity={ck: rows})
    with respx.mock(assert_all_called=False) as m:
        m.route().mock(side_effect=fake)
        await collector(db, http, clock, now).run()
    wm = await get_state(db, f"activity_wm:{ck}")
    assert wm["truncated"] is True and wm["ts"] == parse_iso(rows[19]["timestamp"])
    assert await db.fetchval("SELECT COUNT(*) FROM owner_stake_events") == 20
    now2 = now + DAY
    clock2 = Clock(now2)
    fake.clock = clock2
    with respx.mock(assert_all_called=False) as m:
        m.route().mock(side_effect=fake)
        await collector(db, http, clock2, now2, run_id="run2").run()
    assert await db.fetchval("SELECT COUNT(*) FROM owner_stake_events") == 25
    wm = await get_state(db, f"activity_wm:{ck}")
    assert wm["truncated"] is False and wm["ts"] == now2 and wm["coverage_start"] == now - 35 * DAY


async def test_reconcile_and_full_exit(db, iset, http):                            # T20 + D4b at ingest
    now = T0
    occs = await seed_world(db, 1, 1, now=now)
    ck = ck_of(0)
    k = occs[0]["occupant_key"]
    clock = Clock(now)
    fake = Fake(clock, stakes={(ck, 1): 100_000.0})
    with respx.mock(assert_all_called=False) as m:
        m.route().mock(side_effect=fake)
        await collector(db, http, clock, now).run()
    fake.stakes = {(ck, 1): 95_000.0}
    fake.activity = {ck: [act(1, now + HOUR, fn="unstake_all_alpha", ck=ck, alpha=10.0)]}
    now2 = now + DAY
    clock2 = Clock(now2)
    fake.clock = clock2
    with respx.mock(assert_all_called=False) as m:
        m.route().mock(side_effect=fake)
        await collector(db, http, clock2, now2, run_id="run2").run()
    ev = await db.fetchone("SELECT payload FROM events WHERE event_type = 'owner_position_unexplained'")
    assert ev and json.loads(ev["payload"])["unexplained"] == pytest.approx(4_990.0)
    x = await db.fetchone("SELECT payload FROM events WHERE event_type = 'owner_extraction_detected'")
    assert json.loads(x["payload"])["reasons"] == ["full_exit_extrinsic"]
    assert await db.fetchval("SELECT COUNT(*) FROM review_queue WHERE kind = 'owner_extraction_candidate'") == 1
    snap = await db.fetchone("SELECT * FROM owner_snapshots WHERE occupant_key = ? ORDER BY observed_at DESC", (k,))
    assert snap["owner_cut_per_block"] == pytest.approx(0.18) and snap["owner_cut_enabled"] == 1
    assert json.loads(snap["fetch_status_json"])["c1"] == "ok"            # list snapshot > 12 h old → detail


async def test_collect_failure_isolation(db, iset, http):                          # T37
    now = T0
    await seed_world(db, 8, 8, now=now)
    clock = Clock(now)
    fake = Fake(clock, c2_fail={ck_of(0)})
    with respx.mock(assert_all_called=False) as m:
        m.route().mock(side_effect=fake)
        out = await collector(db, http, clock, now).run()
    sts = [json.loads(r["fetch_status_json"])["c2"] for r in await db.fetchall(
        "SELECT fetch_status_json FROM owner_snapshots ORDER BY netuid")]
    assert sts[0].startswith("fail") and all(s == "ok" for s in sts[1:]) and out.meta["snapshots"] == 8
    fake2 = Fake(Clock(now + DAY), c2_fail={ck_of(i) for i in range(3)})
    with respx.mock(assert_all_called=False) as m:
        m.route().mock(side_effect=fake2)
        with pytest.raises(RuntimeError, match="partial"):
            await collector(db, http, fake2.clock, now + DAY, run_id="r2").run()
    assert await db.fetchval("SELECT COUNT(*) FROM owner_snapshots") == 16          # rows kept


async def test_taostats_path_dormant(db, iset, http, tmp_root):                    # T42
    now = T0
    await seed_world(db, 2, 2, now=now)
    clock = Clock(now)
    fake = Fake(clock)
    with respx.mock(assert_all_called=False) as m:
        m.route().mock(side_effect=fake)
        await collector(db, http, clock, now).run()
    assert not [c for c in fake.calls if c[1] == "api.taostats.io"]
    iset(TAOSTATS_ENABLED=True, TAOSTATS_API_KEY="k", KILL_SWITCH_PATH=str(tmp_root / "KILL_SWITCH"))
    (tmp_root / "KILL_SWITCH").write_text("1")
    fake.calls.clear()
    with respx.mock(assert_all_called=False) as m:
        m.route().mock(side_effect=fake)
        await collector(db, http, Clock(now + DAY), now + DAY, run_id="r2").run()
    assert not [c for c in fake.calls if c[1] == "api.taostats.io"]
    (tmp_root / "KILL_SWITCH").unlink()
    fake.calls.clear()
    with respx.mock(assert_all_called=False) as m:
        m.route().mock(side_effect=fake)
        await collector(db, http, Clock(now + 2 * DAY), now + 2 * DAY, run_id="r3").run()
    ts = [c for c in fake.calls if c[1] == "api.taostats.io"]
    assert any(c[2] == "/v1/subnets/stake-events" for c in ts) and any(c[2] == "/v1/subnets/owners" for c in ts)
