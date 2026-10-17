"""Spec 17 §9 tests 11–17, 31: block search, conversion, swap price, keys, back-off/failover, metering, planning."""
from __future__ import annotations

import asyncio

import pytest

import app.config as app_config
from app.backfill import rpc_backfill as rb
from app.backfill import rpc_client as rc

from .conftest import GOLDEN, SEED_T0, FakeChain, make_gateway


def run(coro):
    return asyncio.run(coro)


# ── 11 block search ──────────────────────────────────────────────────────────────────────────────────────────
def test_block_sampler_and_locate():
    chain = FakeChain()
    s = rb.BlockSampler([(6502594, SEED_T0 - 3600), (6600000, chain.ts(6600000) + 2000)])   # drifted anchors
    gw = make_gateway(chain)
    h = (chain.ts(6550000) // 3600) * 3600
    seen = []
    b, _, ts_ms = run(rb.locate_block(gw, s, h, step=3600, offset=60, seen=seen))
    assert h <= ts_ms // 1000 < h + 3600 and len(seen) <= 2          # hit within ≤ 1 correction
    assert rb.BlockSampler([(10, 1000), (20, 1120)]).predict(1060) == 15
    assert rb.BlockSampler([(10, 1000)]).predict(1000 + 120) == 20   # extrapolates at 12 s/block


def test_locate_stall_and_divergence():
    chain = FakeChain(stall_at=7000000, stall_sec=3 * 3600)
    gw = make_gateway(chain)
    exact = rb.BlockSampler([(b, chain.ts(b)) for b in range(6999000, 7001000, 50)])
    stall_hour = ((chain.ts(7000000) // 3600) + 1) * 3600           # an hour fully inside the 3-hour stall
    assert run(rb.locate_block(gw, exact, stall_hour, step=3600, offset=60)) is None

    class Liar(rb.BlockSampler):
        def predict(self, t):
            return 6502594
    gw2 = make_gateway(FakeChain())

    async def bad_ts(h):  # every read claims the far past → never converges
        return 1000
    gw2.timestamp_ms = bad_ts
    with pytest.raises(rb.BlockSearchFailed):
        run(rb.locate_block(gw2, Liar([(1, 1)]), 1790000000, step=3600, offset=60))


# ── 12 conversion ────────────────────────────────────────────────────────────────────────────────────────────
def test_convert_entry_golden():
    from scalecodec.utils.ss58 import ss58_decode
    ss = "5FRYKhbmfXPDoHdUUDMx27E3HuMvAzwjzFMMq3rNurUhAyS9"
    raw = bytes.fromhex(ss58_decode(ss))
    e = {**GOLDEN, "owner_coldkey": raw, "owner_hotkey": tuple(raw), "subnet_name": [67, 104, 117, 116, 101, 115],
         "token_symbol": "ش"}
    r = rb.convert_entry(e, ts=1781904264, block=8443757, observed_at=1, source="blockmachine", price=0.064653121,
                         price_method="swap_sqrt", run_id="r")
    assert r["tao_in_pool"] == 187212.134573233 and r["alpha_in_pool"] == 2897413.298870939
    assert r["moving_price"] == pytest.approx(0.06478730659000576, rel=1e-15)
    assert r["network_registered_at"] == 4531295 and r["subnet_volume"] == 4657484.168541687
    assert r["occupant_key"] == "64:4531295" and r["event_at"] == 1781904264
    o = rb.owner_fields(e)
    assert o["owner_coldkey"] == ss == o["owner_hotkey"] and o["subnet_name"] == "Chutes" and o["token_symbol"] == "ش"
    assert rb.owner_fields({"owner_coldkey": "0x" + raw.hex()})["owner_coldkey"] == ss
    assert rb.owner_fields({"owner_coldkey": ss})["owner_coldkey"] == ss
    assert rb.convert_entry({**GOLDEN, "tao_in": 0}, ts=1, block=1, observed_at=1, source="x", price=None,
                            price_method="auto", run_id="r") is None
    rr = rb.convert_entry(GOLDEN, ts=1, block=1, observed_at=1, source="x", price=None, price_method="swap_sqrt",
                          run_id="r")
    assert rr["price_method"] == "reserve_ratio" and rr["price"] == pytest.approx(187212.134573233 / 2897413.298870939)


# ── 13/14 swap price + keys ──────────────────────────────────────────────────────────────────────────────────
def test_swap_sqrt_price_and_keys():
    assert rc.storage_key("SubtensorModule", "SubnetTAO", 64) == \
        "0x658faa385070e074c85bf6b568cf05557a57dce016211512d1700561066b85a34000"
    assert rc.TIMESTAMP_NOW_KEY == "0xf0c365c3cf59d671eb72da0e7a4113c49f1f0515f462cdcf84e0f1d6045dfcbb"
    ident = rb.SWAP_SQRT_PREFIX + (64).to_bytes(2, "little").hex()
    concat = rb.SWAP_SQRT_PREFIX + "00" * 8 + (64).to_bytes(2, "little").hex()
    assert rc.netuid_from_key(ident) == rc.netuid_from_key(concat) == 64
    v = int(0.25426978141504425 * 2 ** 64)
    gw = make_gateway()

    async def keys(prefix, h, count=256):
        return [ident]

    async def q(keys, h):
        return {ident: "0x" + v.to_bytes(16, "little").hex()}
    gw.keys_paged, gw.query_storage_at = keys, q
    cache: dict = {}
    prices = run(rb.prices_swap_sqrt(gw, "0x01", 8443757, 419, cache, {64: 4531295}))
    assert prices[64] == pytest.approx(0.06465312174085439, rel=1e-12)
    assert rc.fixed_to_float(278259363, 32) == pytest.approx(0.06478730659000576)


# ── 15 back-off / failover ───────────────────────────────────────────────────────────────────────────────────
def test_rpc_backoff_failover():
    chain = FakeChain()
    gw = make_gateway(chain, max_attempts=6, backoff_max=120)
    chain.errors = [RuntimeError("-32004 Historical work rate limit exceeded")] * 2
    assert run(gw.block_hash(7000000)).startswith("0x")
    assert gw._fake["sleeps"] == [5, 10]
    chain.errors = [RuntimeError("-32029 rate limit")] * 6
    with pytest.raises(rc.RateLimited):
        run(gw.block_hash(7000000))
    assert gw.endpoint.label == "blockmachine"
    run(gw.fail_over("3 failed buckets"))
    assert gw.endpoint.label == "archive"
    chain.errors = [RuntimeError("Exported method SubnetInfoRuntimeApi_get_all_dynamic_info is not found")]
    with pytest.raises(rc.NoRuntimeApi):
        run(gw.call("state_call", ["x", "0x", "0x01"]))


def test_config_rejects_lite_and_orders_endpoints(settings_override):
    with pytest.raises(ValueError):
        app_config.Settings(BACKFILL_RPC_ENDPOINTS=["wss://lite.chain.opentensor.ai:443"])
    s = app_config.Settings(BLOCKMACHINE_API_KEY="k", ONFINALITY_API_KEY="")
    eps = rc.endpoints_from_settings(s)
    assert [e.label for e in eps] == ["blockmachine", "onfinality_public", "archive.chain.opentensor.ai"]
    assert eps[0].url.endswith("?authorization=k") and eps[0].source == "blockmachine"
    assert eps[0].occupant_source == "rpc" and eps[1].occupant_source == "onfinality"
    assert [e.label for e in rc.endpoints_from_settings(app_config.Settings(BLOCKMACHINE_API_KEY=""))][0] == \
        "onfinality_public"


# ── 31 metering ──────────────────────────────────────────────────────────────────────────────────────────────
def test_rpc_token_accounting(monkeypatch):
    chain = FakeChain()
    gw = make_gateway(chain)

    async def dyn(h, spec, *, meta_hash_fn):
        return [dict(GOLDEN)]
    monkeypatch.setattr(gw, "dynamic_info", dyn)
    s = rb.BlockSampler([(b, chain.ts(b)) for b in range(6999000, 7001000, 100)])
    h = (chain.ts(7000000) // 3600) * 3600
    run(gw.connect())
    sub = gw._fake["subs"][0]
    before_sent, before_tok = sub.sent, gw.tokens_acquired
    res = run(rb.process_bucket(gw, s, rb.Bucket("gap", h), step=3600, offset=60, price_method="reserve_ratio",
                                key_cache={}, run_id="r"))
    assert res.status == "done"
    corrections = len(res.block_times) - 1
    # locate (2/probe) + runtime version (dynamic_info is patched here: its state_call is the 4th real call)
    assert sub.sent - before_sent == gw.tokens_acquired - before_tok == 3 + 2 * corrections
    assert sub.runtime_calls == 0
    pub = make_gateway(endpoints=[rc.Endpoint("wss://p", "onfinality", "onfinality_public", "onfinality_public")],
                       public_max_rps=4.0)
    run(pub.block_hash(7000000))
    assert pub._public_bucket is not None and pub._public_bucket.rate == 4.0


# ── 16 planning ──────────────────────────────────────────────────────────────────────────────────────────────
def test_plan_buckets(bf):
    con = bf
    for i in range(3):                                   # ledger rows: hours 0 (complete), 1 (no mp), 2 (few netuids)
        t = rb.LEDGER_START_TS + 3600 * (i + 1)
        for n in range(1, 101 if i < 2 else 20):
            con.execute("INSERT INTO pool_history (netuid, block_number, timestamp, price, tao_in_pool, alpha_in_pool,"
                        " moving_price, source) VALUES (?,?,?,1,1,1,?, 'ledger_pc')",
                        (n, 7000000 + i * 1000 + n, t, 0.5 if i == 0 else None))
    con.execute("INSERT INTO pool_history (netuid, block_number, timestamp, price, tao_in_pool, alpha_in_pool, "
                "source) VALUES (1, 8443757, ?, 1, 1, 1, 'ledger_pc')", (rb.LEDGER_END_TS_PC,))
    now = rb.LEDGER_END_TS_PC + 10 * 86400
    led = rb.plan_buckets(con, "ledger", step=3600, now=now, min_netuids=100)
    hours = {b.bucket_ts for b in led}
    h0 = (rb.LEDGER_START_TS // 3600 + 1) * 3600
    assert h0 not in hours and h0 + 3600 in hours and h0 + 7200 in hours
    gap = rb.plan_buckets(con, "gap", step=3600, now=now, min_netuids=100)
    assert gap[0].bucket_ts == (rb.LEDGER_END_TS_PC // 3600 + 1) * 3600
    assert gap[-1].bucket_ts == (now // 3600 - 2) * 3600
    con.execute("INSERT INTO backfill_buckets (era, bucket_ts, status, observed_at, run_id) VALUES "
                "('gap', ?, 'done', 1, 'r'), ('gap', ?, 'failed', 1, 'r')", (gap[0].bucket_ts, gap[1].bucket_ts))
    gap2 = rb.plan_buckets(con, "gap", step=3600, now=now, min_netuids=100)
    assert gap[0].bucket_ts not in {b.bucket_ts for b in gap2} and gap[1].bucket_ts in {b.bucket_ts for b in gap2}
    pre = rb.plan_buckets(con, "pre_ledger", step=3600, now=now, min_netuids=100)
    assert pre[0].bucket_ts == (rb.DTAO_LAUNCH_TS // 3600) * 3600 and pre[-1].bucket_ts < rb.LEDGER_START_TS


# ── 17 atomic write ──────────────────────────────────────────────────────────────────────────────────────────
def test_write_bucket_atomic(bf):
    res = rb.BucketResult(bucket=rb.Bucket("gap", 1790000000), status="done", endpoint="blockmachine",
                          block_number=9000000, block_ts=1790003540, n_subnets=1, price_method="reserve_ratio",
                          block_times=[(9000000, 1790003540000)])
    res.rows = [rb.convert_entry(GOLDEN, ts=1790003540, block=9000000, observed_at=1, source="blockmachine",
                                 price=None, price_method="reserve_ratio", run_id="r")]
    res.owners = [{"netuid": 64, "block_number": 9000000, "occupant_key": "64:4531295", "event_at": 1790003540,
                   "observed_at": 1, "source": "blockmachine", "run_id": "r", "owner_coldkey": "a",
                   "owner_hotkey": "b", "subnet_name": "Chutes", "token_symbol": "x"}]
    # CHECK violation on the bucket row, the last statement of the transaction (INSERT OR IGNORE would swallow a
    # NOT NULL violation on the data rows)
    broken = rb.BucketResult(**{**res.__dict__, "price_method": "bogus", "bucket": rb.Bucket("gap", 1790003600)})
    with pytest.raises(Exception):
        rb.write_bucket(bf, broken, run_id="r")
    assert bf.execute("SELECT COUNT(*) FROM pool_history").fetchone()[0] == 0
    assert bf.execute("SELECT COUNT(*) FROM backfill_buckets").fetchone()[0] == 0
    out = rb.write_bucket(bf, res, run_id="r")
    assert out == {"pool_rows": 1, "owner_rows": 1}
    assert tuple(bf.execute("SELECT status, rows_written FROM backfill_buckets").fetchone()) == ("done", 1)
    same = rb.BucketResult(**{**res.__dict__, "bucket": rb.Bucket("gap", 1790007200), "block_number": 9000300})
    same.rows = [{**res.rows[0], "block_number": 9000300}]
    same.owners = [{**res.owners[0], "block_number": 9000300}]
    assert rb.write_bucket(bf, same, run_id="r")["owner_rows"] == 0                  # unchanged owner → no row
    failed = rb.BucketResult(bucket=rb.Bucket("gap", 1790000000), status="failed", error="x")
    rb.write_bucket(bf, failed, run_id="r")                                         # never downgrades a done bucket
    assert bf.execute("SELECT status FROM backfill_buckets WHERE bucket_ts = 1790000000").fetchone()[0] == "done"


def test_concurrent_reconnect_never_drops_live_socket():
    """A worker that sees a dead socket reconnects only its own generation; others never see a None transport."""
    chain = FakeChain()
    gw = make_gateway(chain)
    chain.errors = [ConnectionError("connection closed")] * 3

    async def go():
        return await asyncio.gather(*(gw.block_hash(7000000 + i) for i in range(24)), return_exceptions=True)
    out = run(go())
    assert all(isinstance(x, str) for x in out), [x for x in out if not isinstance(x, str)][:3]
    assert len(gw._fake["subs"]) <= 4


def test_runtime_load_shared_and_timeout_safe():
    chain = FakeChain()
    gw = make_gateway(chain, timeout=0.05)
    calls = {"n": 0}

    async def go():
        sub, _, _ = await gw._current()
        orig = sub.get_runtime_for_version

        async def slow(v, h):
            calls["n"] += 1
            await asyncio.sleep(0.3)
            return await orig(v, h)
        sub.get_runtime_for_version = slow

        async def mh():
            return "0x01"
        r = await asyncio.gather(*(gw._runtime(419, mh) for _ in range(5)), return_exceptions=True)
        assert all(isinstance(x, rc.RpcError) for x in r)           # waiters time out as RpcError, not Cancelled
        await asyncio.sleep(0.4)                                      # the shielded load still completes
        return await asyncio.wait_for(gw._runtime(419, mh), 1.0)
    rt = run(go())
    assert rt is not None and calls["n"] == 1


def test_price_api_first(monkeypatch):
    """Where the runtime exposes current_alpha_price_all, the chain's own price wins over a (stale) sqrt map."""
    chain = FakeChain()
    gw = make_gateway(chain)

    async def dyn(h, spec, *, meta_hash_fn):
        return [dict(GOLDEN), {**GOLDEN, "netuid": 7, "tao_in": 10 ** 9, "alpha_in": 4 * 10 ** 9},
                {**GOLDEN, "netuid": 0, "tao_in": 3 * 10 ** 9, "alpha_in": 10 ** 9}]

    async def api(h, spec, *, meta_hash_fn):
        return {64: 0.064653121, 7: 0.25, 0: 1.0}

    async def stale_sqrt(*a, **k):
        raise AssertionError("sqrt map must not be read when the price API exists")
    monkeypatch.setattr(gw, "dynamic_info", dyn)
    monkeypatch.setattr(gw, "alpha_prices", api)
    monkeypatch.setattr(rb, "prices_swap_sqrt", stale_sqrt)
    s = rb.BlockSampler([(b, chain.ts(b)) for b in range(6999000, 7001000, 100)])
    h = (chain.ts(7000000) // 3600) * 3600
    res = run(rb.process_bucket(gw, s, rb.Bucket("gap", h), step=3600, offset=60, price_method="auto",
                                key_cache={}, run_id="r"))
    by = {r["netuid"]: r for r in res.rows}
    assert by[64]["price"] == 0.064653121 and by[64]["price_method"] == "swap_sqrt"      # ≠ reserve ratio
    assert by[7]["price"] == 0.25 and by[7]["price_method"] == "reserve_ratio"           # = reserve ratio
    assert by[0]["price"] == 3.0 and by[0]["price_method"] == "reserve_ratio"            # root keeps the ratio
    assert res.counts["price_api"] is True and res.price_method == "swap_sqrt"


def test_api_price_method_tolerance():
    e = {"tao_in": 187212134573233, "alpha_in": 2897413298870939}
    ratio = e["tao_in"] / e["alpha_in"]
    assert rb.api_price_method(e, round(ratio, 9)) == "reserve_ratio"          # rao-rounded ratio
    assert rb.api_price_method(e, 0.064653121) == "swap_sqrt"
    assert rb.api_price_method(e, None) == "reserve_ratio"
