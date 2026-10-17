"""test_ratelimit / test_http (spec 01 §9)."""
import asyncio
import logging

import httpx
import pytest
import respx

from app.http import Http, ProviderError, redact_path
from app.ratelimit import PROVIDERS, BudgetExhausted, ProviderLimit, ProviderLimiter, TokenBucket, limits_from_settings


class Clock:
    def __init__(self, t=1000.0, wall=1790121600):
        self.t = t
        self.w = wall

    def mono(self):
        return self.t

    def wall(self):
        return int(self.w)

    def adv(self, s):
        self.t += s
        self.w += s


def test_every_provider_has_a_row():
    lim = limits_from_settings()
    assert set(PROVIDERS) == set(lim)
    with pytest.raises(KeyError):
        ProviderLimiter(lim).count("nope")


@pytest.mark.asyncio
async def test_bucket_burst_then_rate(monkeypatch):
    c = Clock()
    b = TokenBucket(rate_per_sec=1.0, burst=2, clock=c.mono)
    assert b.try_take() and b.try_take() and not b.try_take()
    assert b.wait_time() == pytest.approx(1.0)
    c.adv(1.0)
    assert b.try_take()
    b.penalize(30)
    assert b.wait_time() == pytest.approx(30)


@pytest.mark.asyncio
async def test_hourly_and_monthly_caps(tmp_path):
    c = Clock()
    lim = ProviderLimiter({"p": ProviderLimit(100.0, 100, hourly_cap=3, monthly_cap=5)}, mono=c.mono, wall=c.wall,
                          run_dir=tmp_path)
    for _ in range(3):
        await lim.acquire("p")
        lim.count("p")
    with pytest.raises(BudgetExhausted) as ei:
        await lim.acquire("p")
    assert ei.value.window == "hour" and ei.value.until == c.wall() + 3600
    c.adv(3601)
    await lim.acquire("p")
    lim.count("p")
    lim.seed_month("p", 5)
    with pytest.raises(BudgetExhausted) as ei:
        await lim.acquire("p")
    assert ei.value.window == "month"
    assert lim.exhausted("p")[0] == "month" and lim.month_requests("p") == 5


@pytest.mark.asyncio
async def test_host_penalty_and_file_penalty(tmp_path):
    c = Clock()
    lim = ProviderLimiter({"web": ProviderLimit(100.0, 100)}, mono=c.mono, wall=c.wall, run_dir=tmp_path)
    lim.penalize_host("web", "slow.example", 30)
    assert await lim.acquire("web", host="fast.example", timeout=0.01)
    assert not await lim.acquire("web", host="slow.example", timeout=0.01)
    lim.penalize("web", 20)
    assert (tmp_path / "penalty-web.json").exists()
    other = ProviderLimiter({"web": ProviderLimit(100.0, 100)}, mono=c.mono, wall=c.wall, run_dir=tmp_path)
    assert not await other.acquire("web", host="x", timeout=0.01)       # shared 429 penalty (16 §2.12)


def test_reload_picks_up_new_rate(settings_override):
    lim = ProviderLimiter.from_settings()
    assert lim.limits["tmc"].rate_per_sec == pytest.approx(8 / 60)
    settings_override(TMC_RATE_PER_MIN=6)
    lim.reload()
    assert lim.limits["tmc"].rate_per_sec == pytest.approx(6 / 60)


# ── Http ────────────────────────────────────────────────────────────────────────────────────────────────────
class _Costs:
    def __init__(self):
        self.calls = []

    async def record(self, p, usd, *, requests=0, **kw):
        self.calls.append((p, usd, requests))


def _http(tmp_path, **lim):
    limiter = ProviderLimiter({"web": ProviderLimit(1000.0, 1000, **lim), "telegram": ProviderLimit(1000.0, 1000)},
                              run_dir=tmp_path)
    return Http(limiter, costs=_Costs())


@pytest.mark.asyncio
async def test_http_retry_rules(tmp_path, monkeypatch):
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)
    monkeypatch.setattr("app.http.asyncio.sleep", fake_sleep)
    h = _http(tmp_path)
    with respx.mock(assert_all_mocked=True, assert_all_called=False) as m:
        r = m.get("https://a.example/x").mock(side_effect=[httpx.Response(429, headers={"Retry-After": "0"}),
                                                            httpx.Response(200, text="ok")])
        resp = await h.request("web", "GET", "https://a.example/x")
        assert resp.status_code == 200 and r.call_count == 2
        assert len(h.costs.calls) == 2 and h.costs.calls[0] == ("web", 0.0, 1)
        m.get("https://a.example/429").mock(return_value=httpx.Response(429))
        resp = await h.request("web", "GET", "https://a.example/429", ok_statuses=(200, 429))
        assert resp.status_code == 429
        m.get("https://a.example/503").mock(return_value=httpx.Response(503))
        with pytest.raises(ProviderError) as ei:
            await h.request("web", "GET", "https://a.example/503")
        assert ei.value.status == 503 and ei.value.__cause__ is not None and ei.value.attempts == 3
        m.get("https://a.example/conn").mock(side_effect=httpx.ConnectError("down"))
        with pytest.raises(ProviderError) as ei:
            await h.request("web", "GET", "https://a.example/conn")
        assert isinstance(ei.value.__cause__, httpx.ConnectError)
        for code in (404, 301, 403):
            route = m.get(f"https://a.example/{code}").mock(return_value=httpx.Response(code))
            assert (await h.request("web", "GET", f"https://a.example/{code}")).status_code == code
            assert route.call_count == 1
    await h.aclose()


@pytest.mark.asyncio
async def test_http_body_cap_ua_and_loopback(tmp_path, settings_override):
    settings_override(WEB_USER_AGENT="Mozilla/5.0 TaoSentinel/1.0 test")
    h = _http(tmp_path)
    with respx.mock(assert_all_mocked=True, assert_all_called=False) as m:
        route = m.get("https://b.example/big").mock(return_value=httpx.Response(200, content=b"x" * 5000))
        resp = await h.request("web", "GET", "https://b.example/big", max_body_bytes=1000)
        assert len(resp.content) == 1000 and resp.extensions["truncated"] is True
        assert "TaoSentinel/" in route.calls[0].request.headers["user-agent"]
        m.get("http://127.0.0.1:9944/rpc").mock(return_value=httpx.Response(200, text="{}"))
        n_before = len(h.costs.calls)
        await h.request("web", "GET", "http://127.0.0.1:9944/rpc")
        assert len(h.costs.calls) == n_before                               # loopback: no limiter, no counters
    with pytest.raises(KeyError):
        await h.request("nope", "GET", "https://b.example/")
    await h.aclose()


def test_redaction():
    assert redact_path("/bot123:ABC/sendMessage") == "/bot***/sendMessage"
