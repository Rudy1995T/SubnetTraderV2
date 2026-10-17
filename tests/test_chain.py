"""Spec 05 §9 (Phase 2): SSE parsing/recorder, hourly flush + rollups, analytics precedence, gate fit, snapshot rule,
P1 metric contract, dereg, holders, net flow, trending rollup, P1 aggregation, RPC endpoint failover."""
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import respx

import app.collectors.chain_tmc as ct
import app.config as app_config
from app.collectors.chain_tmc import ChainSseTask, TableState, parse_sse
from app.collectors.chain_tmc_rest import (upsert_daily_from_analytics, upsert_hourly_from_analytics,
                                           write_trending_daily)
from app.collectors.endpoints import EndpointSelector, is_quota_error
from app.features.chain_features import (SnapshotRow, dereg_metrics, immune_local, net_flow, run_slot,
                                         snapshot_rows)
from app.features.gate_model import fit_gate
from app.features.holders import concentration
from app.quant.dereg_risk import DeregRiskConfig, components, dereg_hazard_score
from app.scoring.p1_matrix import aggregate, p1_matrix

FIXTURE = next(Path(__file__).parent.glob("fixtures/tmc_table_*_block*.json"))
H13 = 1_790_258_400  # 2026-09-24T13:00:00Z (hour-aligned)
assert H13 % 3600 == 0


def table_rows(n: int = 129, block: int = 9_123_660) -> list[dict]:
    rows = []
    for i in range(n):
        rows.append({"subnet": i, "block_number": block, "network_registered_at": 1000 + i, "is_active": True,
                     "emission_enabled": True, "immune": False, "deregistration_risk": False,
                     "price": 0.01 + i / 1000, "price_after": 0.009 + i / 1000, "subnet_moving_price": 0.01 + i / 1000,
                     "emission": 100 / n, "tao_liquidity": 1e13, "excess_tao": 0, "name": f"S{i}",
                     "marketcap": 1000.0, "subnet_owner": f"ck{i}", "total_subnet_stake": 1e15})
    return rows


class Clock:
    def __init__(self, t: int) -> None:
        self.t = t

    def __call__(self) -> int:
        return self.t


@pytest.fixture
def clock(monkeypatch):
    c = Clock(H13)
    monkeypatch.setattr(ct, "now_ts", c)
    return c


def make_task(db) -> ChainSseTask:
    rt = SimpleNamespace(db=db, http=None, started_at=H13)
    return ChainSseTask(rt, SimpleNamespace(id="chain_sse", liveness_max_age_sec=900))


async def _lines(items):
    for x in items:
        yield x


# ── SSE parsing / state ─────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_parse_sse():
    full = json.dumps({"type": "full", "data": table_rows(3)})
    lines = [f"data: {full}", "", ": keepalive", "data: {\"type\":\"delta\",", "data: \"changes\":{}}", "",
             'data: {"type":"delta","changes":{"1":{"price":2}}}', "", 'data: {"type":"delta","changes":{}}', "",
             'data: {"type":"heartbeat"}', "", 'data: {"type":"delta"']
    evs = [e async for e in parse_sse(_lines(lines))]
    assert len(evs) == 5
    assert json.loads(evs[1].data) == {"type": "delta", "changes": {}}


def test_table_state_patch():
    t = TableState()
    t.replace(table_rows(5))
    touched = t.patch({"2": {"price": 9.9}, "3": {"emission_enabled": False}},
                      [{"subnet": 4, "network_registered_at": 99999, "is_active": False}])
    assert touched == {2, 3, 4}
    assert t.rows()[2]["price"] == 9.9 and 3 in t.rows()
    assert t.changed_flags(3) == {"emission_enabled"} and "added" in t.changed_flags(4)
    assert ct.occupant_of(t.rows()[4]) == "4:99999"


@pytest.mark.asyncio
async def test_ticks_stride_and_flag_override(db, clock):
    task = make_task(db)
    await task._on_event("full", table_rows(), None, source="sse")
    assert await db.fetchval("SELECT COUNT(*) FROM chain_ticks") == 129
    clock.t += 36  # +3 blocks → 9_123_663 (same stride window), SN29 flips emission off
    await task._on_event("delta", None, {"changes": {"29": {"emission_enabled": False}}}, source="sse")
    rows = await db.fetchall("SELECT netuid, block_number FROM chain_ticks WHERE block_number = 9123663")
    assert [(r["netuid"], r["block_number"]) for r in rows] == [(29, 9123663)]
    ev = await db.fetchall("SELECT * FROM events WHERE event_type = 'emission_disabled'")
    assert len(ev) == 1 and ev[0]["dedupe_key"] == "29:1029|9123663"
    clock.t += 24  # → 9_123_665: new window
    await task._on_event("delta", None, {"changes": {"1": {"price": 0.5}}}, source="sse")
    assert await db.fetchval("SELECT COUNT(*) FROM chain_ticks WHERE block_number = 9123665") == 129
    assert task.block_source == "row+estimated"


@pytest.mark.asyncio
async def test_hour_flush_last_tick(db, clock):
    task = make_task(db)
    rows = table_rows()
    rows[5]["is_active"] = False  # not counted in sum_subnet_prices
    await task._on_event("full", rows, None, source="sse")
    for m in range(1, 60):
        clock.t = H13 + 60 * m
        await task._on_event("delta", None, {"changes": {"1": {"price": 1.0 + m / 100}}}, source="sse")
    clock.t = H13 + 3588
    await task._on_event("delta", None, {"changes": {"1": {"price": 3.0}}}, source="sse")
    # a stored tmc_candle for 13:00 must not block the live bar
    await db.execute("INSERT INTO price_bars (netuid, period, ts, occupant_key, open, high, low, close, source, "
                     "observed_at) VALUES (1, '1h', ?, '1:1001', 1, 1, 1, 1, 'tmc_candle', ?)", (H13, H13))
    clock.t = H13 + 3600
    await task._on_event("delta", None, {"changes": {"1": {"price": 4.0}}}, source="sse")
    h = await db.fetchone("SELECT * FROM chain_hourly WHERE netuid = 1 AND hour_ts = ?", (H13,))
    assert h["price"] == 3.0 and h["source"] == "sse" and h["backfilled"] == 0 and h["immune_local"] == 0
    assert await db.fetchval("SELECT COUNT(*) FROM chain_hourly WHERE hour_ts = ?", (H13,)) == 129
    b5 = await db.fetchall("SELECT * FROM price_bars WHERE netuid = 1 AND period = '5m' ORDER BY ts")
    assert len(b5) == 12 and all(b["source"] == "sse_rollup" for b in b5)
    b1 = await db.fetchone("SELECT * FROM price_bars WHERE netuid = 1 AND period = '1h' AND ts = ?", (H13,))
    assert b1["source"] == "sse_rollup" and b1["close"] == 3.0 and b1["ts_last"] == H13 + 3588
    assert b1["high"] == 3.0 and b1["open"] == pytest.approx(0.011)
    net = await db.fetchone("SELECT * FROM chain_network_hourly WHERE hour_ts = ?", (H13,))
    expect = sum(r["price"] for r in rows if r["subnet"] not in (0, 5)) - rows[1]["price"] + 3.0
    assert net["n_active"] == 127 and net["sum_subnet_prices"] == pytest.approx(expect)
    # same-rank rewrite: a later block replaces, an earlier one does not
    task.tbl.rows()[1]["price"] = 7.0
    task.tbl.last_block = 9_999_999
    await task._write_hourly(H13)
    assert await db.fetchval("SELECT price FROM chain_hourly WHERE netuid = 1 AND hour_ts = ?", (H13,)) == 7.0
    task.tbl.rows()[1]["price"] = 8.0
    task.tbl.last_block = 1
    for r in task.tbl.rows().values():
        r["block_number"] = 1
    await task._write_hourly(H13)
    assert await db.fetchval("SELECT price FROM chain_hourly WHERE netuid = 1 AND hour_ts = ?", (H13,)) == 7.0


# ── analytics precedence (J1) ──────────────────────────────────────────────────────────────────────────────────
def _an(ts: int, **kw) -> dict:
    base = {"ts": ts, "block_number": 9_000_000 + (ts - H13) // 12, "emission_percent": 1.5, "subnet_tao": 5e12,
            "chain_buys": 7, "total_tao_buy_24h": 10.0, "total_tao_sell_24h": 4.0, "total_locked": 3.0}
    return {**base, **kw}


@pytest.mark.asyncio
async def test_backfill_write_gate(db):
    await db.execute("INSERT INTO chain_hourly (netuid, hour_ts, occupant_key, block_number, event_at, observed_at, "
                     "source, price) VALUES (7, ?, '7:1', 1, ?, ?, 'sse', 0.5)", (H13, H13, H13))
    n = await upsert_hourly_from_analytics(db, [_an(H13 + 60), _an(H13 + 3600)], netuid=7, occupant_key="7:1",
                                           source="analytics_1w", run_id="r", observed_at=H13 + 7200)
    assert n == 1
    kept = await db.fetchone("SELECT * FROM chain_hourly WHERE netuid = 7 AND hour_ts = ?", (H13,))
    assert kept["source"] == "sse" and kept["price"] == 0.5
    new = await db.fetchone("SELECT * FROM chain_hourly WHERE netuid = 7 AND hour_ts = ?", (H13 + 3600,))
    assert new["backfilled"] == 1 and new["emission"] == 1.5 and new["tao_liquidity"] == 5e12
    assert new["excess_tao"] == 7 and new["price"] is None and new["subnet_moving_price"] is None


@pytest.mark.asyncio
async def test_upsert_daily_precedence(db):
    d0 = H13 - H13 % 86400
    w = [_an(d0 + 1800, emission_percent=1.0), _an(d0 + 7200, emission_percent=9.0)]  # only the first hour maps
    assert await upsert_daily_from_analytics(db, w, netuid=3, source="analytics_1w", run_id="a", observed_at=1,
                                             occupant_key="3:1") == 1
    row = await db.fetchone("SELECT * FROM chain_daily WHERE netuid = 3")
    assert row["emission_percent"] == 1.0 and row["source"] == "analytics_1w"
    y = [{**_an(d0, emission_percent=2.0), "occupant_key": "3:1"}]
    await upsert_daily_from_analytics(db, y, netuid=3, source="analytics_1y", run_id="b", observed_at=2,
                                      occupant_key="3:OTHER")
    row = await db.fetchone("SELECT * FROM chain_daily WHERE netuid = 3")
    assert row["source"] == "analytics_1y" and row["emission_percent"] == 2.0 and row["occupant_key"] == "3:1"
    await upsert_daily_from_analytics(db, [_an(d0 + 60, emission_percent=5.0)], netuid=3, source="analytics_1w",
                                      run_id="c", observed_at=3, occupant_key="3:1")
    await upsert_daily_from_analytics(db, [_an(d0, emission_percent=6.0)], netuid=3, source="analytics_1y",
                                      run_id="d", observed_at=4, occupant_key="3:1")
    assert await db.fetchval("SELECT emission_percent FROM chain_daily WHERE netuid = 3") == 2.0
    assert await upsert_daily_from_analytics(db, [_an(d0 - 86400)], netuid=99, source="analytics_1y", run_id="e",
                                             observed_at=5) == 0  # no resolvable key → skipped


async def _occupant(db, netuid: int, reg: int, status: str = "active", first_seen_at: int = 1):
    await db.execute("INSERT INTO occupants (occupant_key, netuid, network_registered_at, status, name, "
                     "first_seen_block, first_seen_at, last_seen_block, last_seen_at) VALUES (?,?,?,?,?,1,?,1,1)",
                     (f"{netuid}:{reg}", netuid, reg, status, f"S{netuid}", first_seen_at))


@pytest.mark.asyncio
async def test_trending_daily_zero_vs_missing(db):
    for n in (1, 2, 3):
        await _occupant(db, n, 10 + n)
    d0 = H13 - H13 % 86400
    for i, t in enumerate((d0 + 100, d0 + 200, d0 + 300)):
        await db.execute("INSERT INTO chain_trending (observed_at, netuid, occupant_key, rank, views) "
                         "VALUES (?, 1, '1:11', 1, ?)", (t, 10 * (i + 1)))
    assert await write_trending_daily(db, d0, run_id="r") == 3
    got = {r["netuid"]: r for r in await db.fetchall("SELECT * FROM chain_trending_daily WHERE day_ts = ?", (d0,))}
    assert got[1]["views"] == 30 and got[2]["views"] == 0 and got[1]["n_pulls_ok"] == 3
    assert await write_trending_daily(db, d0 + 86400, run_id="r") == 0


# ── gate fit ───────────────────────────────────────────────────────────────────────────────────────────────────
def _predict(mp, b, bar_rank=32, h=3.0):
    s = sum(mp.values())
    adj = {i: mp[i] / s * b[i] for i in mp}
    t = sum(adj.values())
    adj = {i: v / t for i, v in adj.items()}
    theta = sorted(adj.values(), reverse=True)[bar_rank - 1]
    g = {i: adj[i] * (1 / (1 + (theta / adj[i]) ** h)) for i in mp}
    t = sum(g.values())
    return {i: v / t for i, v in g.items()}


def test_gate_fit_synthetic():
    mp = {i: 1.0 / (i + 1) for i in range(40)}
    b_true = {i: (0.5 if i in (3, 10) else 1.0) for i in mp}
    fit = fit_gate(mp, _predict(mp, b_true), max_iter=500, tol=1e-9)
    assert fit.residual_pp < 0.1
    assert fit.b[3] == pytest.approx(0.5, abs=1e-3) and fit.b[10] == pytest.approx(0.5, abs=1e-3)
    assert fit.implied_burn[0] == pytest.approx(0.0, abs=1e-3)
    ov = fit_gate(mp, _predict(mp, b_true), burn_override={3: 0.5})
    assert ov.converged and ov.burn_source == "rpc"


def test_gate_fit_live_fixture():
    rows = json.loads(FIXTURE.read_text())
    E = {r["subnet"]: r for r in rows if r["subnet"] != 0 and r["emission_enabled"] and r["is_active"]
         and r["subnet_moving_price"] and float(r["subnet_moving_price"]) > 0}
    mp = {n: float(r["subnet_moving_price"]) for n, r in E.items()}
    em = {n: float(r["emission"] or 0) for n, r in E.items()}
    tot = sum(em.values())
    fit = fit_gate(mp, {n: v / tot for n, v in em.items()})
    assert fit.residual_pp <= app_config.settings.GATE_FIT_TOLERANCE_PP
    assert 0.002 <= fit.theta <= 0.02
    r32 = sorted(fit.adj, key=lambda n: -fit.adj[n])[31]
    assert 0.35 <= fit.gate[r32] <= 0.65
    demand = {n: v / sum(mp.values()) for n, v in mp.items()}
    for n in E:
        if em[n] == 0 and demand[n] >= 0.01 and fit.implied_burn[n] is not None:
            assert fit.implied_burn[n] >= 0.95


# ── snapshot / metric contract ─────────────────────────────────────────────────────────────────────────────────
async def _hourly(db, netuid, hour_ts, source, **f):
    base = {"netuid": netuid, "hour_ts": hour_ts, "occupant_key": f"{netuid}:{1000 + netuid}",
            "block_number": 9_000_000, "event_at": hour_ts + 3599, "observed_at": hour_ts + 3600, "source": source,
            "network_registered_at": 1000 + netuid, "backfilled": int(source.startswith("analytics"))}
    base.update(f)
    cols = list(base)
    await db.execute(f"INSERT INTO chain_hourly ({','.join(cols)}) VALUES ({','.join('?' * len(cols))})",
                     [base[c] for c in cols])


@pytest.mark.asyncio
async def test_snapshot_as_of_rule(db):
    as_of = H13 + 11 * 3600  # 00:00Z
    await _hourly(db, 1, as_of - 3600, "sse", price=1.0)
    await _hourly(db, 2, as_of - 7200, "sse", price=2.0)
    await _hourly(db, 3, as_of - 3600, "analytics_1w", emission=1.0)
    snap = await snapshot_rows(db, as_of)
    assert snap[1].hour_ts == as_of - 3600 and snap[1].stale_h == 0
    assert snap[2].stale_h == 1 and snap[2].kind == "live"
    assert snap[3].kind == "analytics"


@pytest.mark.asyncio
async def test_metric_rows_contract(db):
    rows = json.loads(FIXTURE.read_text())
    as_of = 1_790_294_400  # 2026-09-25T00:00Z
    block = max(r["block_number"] for r in rows)
    for r in rows:
        f = {k: ct.coerce(k, r.get(k)) for k in ct.HOURLY_COLUMNS}
        await _hourly(db, r["subnet"], as_of - 3600, "sse", **{**f, "block_number": block,
                                                               "occupant_key": ct.occupant_of(r)})
    rep = await run_slot(db, as_of, run_id="t1", observed_at=as_of + 300)
    assert rep.n_eligible >= 100 and rep.fit is not None and not rep.degraded
    mv = await db.fetchall("SELECT * FROM metric_values WHERE as_of = ?", (as_of,))
    assert mv and all(m["source"] == "chain" and m["stage"] == "raw" for m in mv)
    assert all(m["status"] in ("ok", "failed", "not_applicable") for m in mv)
    assert all((m["value"] is None) == (m["status"] != "ok") for m in mv)
    badge = {r["subnet"] for r in rows if r["deregistration_risk"]}
    for n in badge:
        v = await db.fetchval("SELECT value FROM metric_values WHERE occupant_key LIKE ? AND metric_key = "
                              "'dereg_safety' AND as_of = ?", (f"{n}:%", as_of))
        assert v == 0.0
    assert await db.fetchval("SELECT COUNT(*) FROM gate_fits WHERE as_of = ?", (as_of,)) == 1
    # first success wins: a second run neither replaces ok rows nor duplicates the fit
    before = await db.fetchval("SELECT COUNT(*) FROM metric_values WHERE run_id = 't1'")
    await run_slot(db, as_of, run_id="t2", observed_at=as_of + 900)
    assert await db.fetchval("SELECT COUNT(*) FROM metric_values WHERE run_id = 't1'") == before
    m = await p1_matrix(db, as_of)
    assert m == []  # the matrix joins occupants; none registered in this db


def test_immunity_local():
    mk = lambda age: SnapshotRow(1, "1:100", 0, "sse", 0, 100 + age, {"network_registered_at": 100})  # noqa: E731
    assert immune_local(mk(828_932), 864_000) is True
    assert immune_local(mk(885_580), 864_000) is False


def test_dereg_safety_and_hazard():
    period = 864_000
    rows = {n: SnapshotRow(n, f"{n}:{n}", 0, "sse", 0, 10_000_000,
                           {"subnet_moving_price": n / 100, "emission_enabled": 1, "is_active": 1,
                            "network_registered_at": n}) for n in range(1, 21)}
    rows[20].f["emission_enabled"] = 0
    rows[21] = SnapshotRow(21, "21:21", 0, "sse", 0, 10_000_000,
                           {"subnet_moving_price": None, "emission_enabled": 1, "is_active": 1,
                            "network_registered_at": 21})
    d = dereg_metrics(rows, {}, DeregRiskConfig(), period)
    assert [d[n]["rank"] for n in (1, 2, 3, 4)] == [1, 2, 3, 4] and d[20]["hazard"] == 1.0
    assert d[21]["hazard_status"] == "failed"
    w = {n: n / 100 for n in range(1, 21)}
    c = components(w, 5)
    cfg = DeregRiskConfig()
    assert dereg_hazard_score(w, 5) == pytest.approx(cfg.w_proximity * c["proximity"] + cfg.w_starved * c["starved"]
                                                     + cfg.w_decline * c["decline"])


def test_holders_concentration():
    rows = [{"rank": i + 1, "percentage": p, "total_alpha": p * 10} for i, p in enumerate([20, 10] + [1] * 98)]
    st = concentration(rows, 500)
    assert st.top1_share == 0.2 and st.top10_share == pytest.approx(0.38)
    assert st.hhi_topn == pytest.approx(0.04 + 0.01 + 98 * 0.0001)
    assert 0 < st.gini_topn < 1 and st.n_fetched == 100


@pytest.mark.asyncio
async def test_net_flow_window(db):
    as_of = H13 - H13 % 86400
    snap = SnapshotRow(5, "5:1", as_of - 3600, "sse", as_of, 1 + 40 * 7200, {"tao_liquidity": 1000.0})
    for i in range(30):
        await db.execute("INSERT INTO chain_daily (netuid, day_ts, occupant_key, event_at, observed_at, source, "
                         "total_tao_buy_24h, total_tao_sell_24h) VALUES (5, ?, '5:1', 0, 0, 'analytics_1w', 10, 4)",
                         (as_of - i * 86400,))
    v, info = await net_flow(db, "5:1", as_of, snap, observed_at=as_of)
    assert v == pytest.approx(30 * 6 / 1000) and info["status"] == "ok"
    await db.execute("DELETE FROM chain_daily WHERE day_ts < ?", (as_of - 19 * 86400,))
    v, info = await net_flow(db, "5:1", as_of, snap, observed_at=as_of)
    assert v is None and info["reason"] == "insufficient_history"
    young = SnapshotRow(5, "5:1", 0, "sse", 0, 1 + 10 * 7200, {"tao_liquidity": 1000.0})
    v, info = await net_flow(db, "5:1", as_of, young, observed_at=as_of)
    assert info["window_days"] == 10 and v is not None


def test_p1_matrix_coverage():
    assert aggregate({"emission_share": 1.0, "gate_margin": None})[0] is None
    full = {k: 0.5 for k in ("emission_share", "gate_margin", "dereg_safety", "liquidity_depth", "exit_capacity",
                             "net_flow_30d", "chain_buys", "holder_dispersion", "participation", "miner_burn_ok")}
    p, cov = aggregate(full)
    assert p == pytest.approx(0.5) and cov == pytest.approx(1.0)


def test_rpc_endpoint_failover():
    t = {"v": 0.0}
    sel = EndpointSelector(["a", "b"], cooldown_sec=300, clock=lambda: t["v"])
    assert sel.current() == "a"
    sel.mark_bad("a", "insufficient compute units")
    assert sel.current() == "b"
    t["v"] = 301
    assert sel.current() == "a"
    assert is_quota_error("insufficient compute units") and not is_quota_error("boom")


@pytest.mark.asyncio
async def test_sse_unauthorized_fallback(rt, monkeypatch):
    from tests.registry.fakes import fast_limits
    fast_limits(rt)
    s = app_config.settings
    monkeypatch.setattr(app_config, "settings", s.model_copy(update={"TMC_TABLE_FALLBACK_SEC": 3600}))
    task = ChainSseTask(rt, SimpleNamespace(id="chain_sse", liveness_max_age_sec=900))
    import asyncio
    with respx.mock(assert_all_called=False) as m:
        m.get(s.TMC_SSE_URL).mock(return_value=httpx.Response(401))
        table = m.get(s.TMC_BASE_URL.rstrip("/") + "/subnets/table/").mock(
            return_value=httpx.Response(200, json=table_rows()))
        await task.run_once(asyncio.Event())
        assert task.state == "unauthorized" and task.unauthorized_until > ct.now_ts()
        for _ in range(50):
            if table.called and task.tbl.rows():
                break
            await asyncio.sleep(0.05)
        assert table.called and task.fallback_state == "polling" and task.last_attempt is not None
        assert await rt.db.fetchval("SELECT COUNT(*) FROM chain_ticks WHERE source = 'rest_table'") == 129
        task.fallback_task.cancel()
