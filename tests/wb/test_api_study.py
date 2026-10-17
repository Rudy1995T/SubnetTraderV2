"""19 §2.6–§2.8: routes (screener, chart, presets, chart-read with a stub model), /wb page, study."""
import json
import math
from types import SimpleNamespace

import httpx
import pytest

import app.config as app_config
from app.features import technicals as T
from app.wb import study as S
from tests.wb.conftest import DAY, HOUR, T0, add_bars, add_occupant, add_snapshot, wave

PNG = b"\x89PNG\r\n\x1a\n" + b"\0" * 64


@pytest.fixture
def wb_on(monkeypatch):
    def _set(**kw):
        for k, v in {"WB_ENABLED": True, **kw}.items():
            monkeypatch.setattr(app_config.settings, k, v)
    return _set


def client_for(rt):
    from app.api.app import create_app
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(lambda: rt)), base_url="http://t")


async def seed(db, now):
    o1 = await add_occupant(db, 1, 1000, github="https://github.com/a/b")
    o2 = await add_occupant(db, 2, 2000)
    for n, o in ((1, o1), (2, o2)):
        await add_bars(db, n, o, "1h", wave(300, amp=0.03 * n), end_ts=now - HOUR, mps=[1.0] * 300)
        await add_bars(db, n, o, "1d", wave(150, amp=0.05 * n), end_ts=now - now % DAY - DAY)
    await add_snapshot(db, now - HOUR, [{"netuid": 1, "occ": o1, "price": 1.0, "mp": 1.0, "reg": 1000, "block": 9e6,
                                         "commits": 12},
                                        {"netuid": 2, "occ": o2, "price": 2.0, "mp": 2.0, "reg": 2000, "block": 9e6}])
    return o1, o2


@pytest.mark.asyncio
async def test_screener_chart_and_page(rt, wb_on):
    wb_on()
    now = T0 + 5 * HOUR
    o1, _ = await seed(rt.db, now)
    await T.run_period(rt.db, "1h", now - now % HOUR, run_id="r", observed_at=now)
    async with client_for(rt) as c:
        r = await c.get("/api/v1/wb/screener")
        assert r.status_code == 200
        d = r.json()
        rows = {x["netuid"]: x for x in d["rows"]}
        assert set(rows) == {1, 2} and rows[1]["commits_30d"] == 12
        assert rows[1]["cov"]["ema72_band_pos_1h"]["status"] == "ok" and "ema72_band_pos_1h" in rows[1]
        assert d["as_of"]["1h"] == now - now % HOUR and isinstance(d["factors"], list)
        ch = await c.get("/api/v1/wb/subnet/1/chart", params={"period": "1h", "bars": 50})
        cd = ch.json()
        assert len(cd["bars"]) == 50 == len(cd["overlay"]) and cd["occupant_key"] == o1
        stored = rows[1]["ema72_1h"]
        assert cd["overlay"][-1]["ema"] == pytest.approx(stored, rel=1e-3)
        assert (await c.get("/api/v1/wb/subnet/77/chart")).status_code == 404
        assert (await c.get("/api/v1/wb/subnet/1/chart", params={"period": "4h"})).status_code == 422
        page = await c.get("/wb")
        assert page.status_code == 200 and "Subnet Workbench" in page.text


@pytest.mark.asyncio
async def test_presets_roundtrip_and_auth(rt, wb_on):
    wb_on(API_AUTH_TOKEN="sekret")
    async with client_for(rt) as c:
        body = {"name": "value", "weights": {"ema72_band_pos_1d": -0.5, "dereg_safety_30d": 1.0}}
        assert (await c.post("/api/v1/wb/presets", json=body)).status_code == 401
        h = {"X-Auth-Token": "sekret"}
        assert (await c.post("/api/v1/wb/presets", json=body, headers=h)).status_code == 200
        assert (await c.post("/api/v1/wb/presets", json={"name": "x", "weights": {"nope": 1}},
                             headers=h)).status_code == 422
        assert (await c.post("/api/v1/wb/presets", json={"name": "x", "weights": {"rv_30d": 3}},
                             headers=h)).status_code == 422
        p = (await c.get("/api/v1/wb/presets")).json()["presets"]
        assert p[0]["name"] == "value" and p[0]["weights"] == body["weights"]
        assert (await c.post("/api/v1/wb/presets/delete", json={"name": "value"}, headers=h)).status_code == 200
        assert (await c.get("/api/v1/wb/presets")).json()["presets"] == []


class StubBlock(SimpleNamespace):
    pass


class StubClient:
    def __init__(self, stop="end_turn", text=None):
        self.stop, self.text, self.kw = stop, text, None
        self.beta = SimpleNamespace(messages=SimpleNamespace(create=self._create))

    async def _create(self, **kw):
        self.kw = kw
        read = {"trend": "down", "above_visible_mas": False, "drawdown_from_visible_high_pct": 40.0,
                "support_pct_below": 5.0, "resistance_pct_above": 12.0, "timeframe_guess": "1h",
                "instrument_guess": "SN1", "axis_last_price_text": "0.0123", "summary": "Downtrend below the EMA.",
                "confidence": {k: 0.6 for k in ("trend", "above_visible_mas", "drawdown", "levels", "timeframe",
                                                 "instrument")}}
        content = [StubBlock(type="thinking", thinking=""), StubBlock(type="text", text=self.text or json.dumps(read))]
        return SimpleNamespace(model="claude-opus-5-5", stop_reason=self.stop, content=content,
                               usage=SimpleNamespace(input_tokens=2000, output_tokens=500,
                                                     cache_creation_input_tokens=0, cache_read_input_tokens=0),
                               stop_details=SimpleNamespace(category="cyber") if self.stop == "refusal" else None)


@pytest.mark.asyncio
async def test_chart_read_paths(rt, wb_on, monkeypatch, tmp_path):
    from app.api import wb as wb_api
    wb_on(WB_UPLOAD_DIR=str(tmp_path / "up"), WB_VISION_MAX_IMAGE_MB=1)
    await add_occupant(rt.db, 1, 1000)
    async with client_for(rt) as c:
        r = await c.post("/api/v1/wb/chart-read", content=b"x", headers={"Content-Type": "text/plain"})
        assert r.status_code == 415
        r = await c.post("/api/v1/wb/chart-read", content=b"\0" * (1024 * 1024 + 1),
                         headers={"Content-Type": "image/png"})
        assert r.status_code == 413
        r = await c.post("/api/v1/wb/chart-read?netuid=1", content=PNG, headers={"Content-Type": "image/png"})
        assert r.status_code == 409 and r.json()["detail"]["error"] == "vision_disabled"
        assert (tmp_path / "up").is_dir() and len(list((tmp_path / "up").iterdir())) == 1
        wb_on(WB_UPLOAD_DIR=str(tmp_path / "up"), WB_VISION_ENABLED=True, ANTHROPIC_API_KEY="sk-test",
              LLM_MONTHLY_CAP_USD=60.0)
        stub = StubClient()
        monkeypatch.setattr(wb_api, "client_factory", lambda key, timeout: stub)
        r = await c.post("/api/v1/wb/chart-read?netuid=1", content=PNG, headers={"Content-Type": "image/png"})
        assert r.status_code == 200, r.text
        d = r.json()
        assert d["status"] == "ok" and d["read"]["trend"] == "down"
        assert d["cost_usd"] == pytest.approx((2000 * 4 + 500 * 20) / 1e6)
        kw = stub.kw
        assert kw["model"] == "claude-opus-5-5" and kw["fallbacks"] == "default"
        assert kw["output_config"]["format"]["type"] == "json_schema" and kw["thinking"] == {"type": "adaptive"}
        note = await rt.db.fetchone("SELECT * FROM manual_notes WHERE id = ?", (d["note_id"],))
        assert note["kind"] == "chart_read" and note["created_by"] == "wb_vision:claude-opus-5-5"
        assert note["occupant_key"] == "1:1000" and json.loads(note["payload_json"])["status"] == "ok"
        up = await rt.db.fetchone("SELECT * FROM wb_uploads WHERE note_id = ?", (d["note_id"],))
        assert up["status"] == "ok" and up["model"] == "claude-opus-5-5"
        monkeypatch.setattr(wb_api, "client_factory", lambda key, timeout: StubClient(stop="refusal"))
        r = await c.post("/api/v1/wb/chart-read", content=PNG + b"1", headers={"Content-Type": "image/png"})
        assert r.status_code == 200 and r.json()["status"] == "unreadable" and r.json()["reason"] == "refusal"
        monkeypatch.setattr(wb_api, "client_factory", lambda key, timeout: StubClient(text="not json"))
        r = await c.post("/api/v1/wb/chart-read", content=PNG + b"2", headers={"Content-Type": "image/png"})
        assert r.json()["status"] == "unreadable" and r.json()["reason"] == "invalid_json"
        notes = (await c.get("/api/v1/wb/notes")).json()["notes"]
        assert len(notes) == 3
        (tmp_path / "KILL").touch()
        monkeypatch.setattr(app_config.settings, "KILL_SWITCH_PATH", str(tmp_path / "KILL"))
        r = await c.post("/api/v1/wb/chart-read", content=PNG + b"3", headers={"Content-Type": "image/png"})
        assert r.status_code == 409 and r.json()["detail"]["error"] == "paused"


def _series(occ, n, reg, closes, mps, liq, start):
    s = S.Series(occ, n, reg)
    for i, (c, m) in enumerate(zip(closes, mps)):
        s.ts.append(start + i * DAY)
        s.close.append(c)
        s.mp.append(m)
        s.liq.append(liq)
    return s


def test_ic_stats_detects_planted_factor():
    per_day = {}
    for d in range(60):
        xs = [float(i) for i in range(30)]
        ys = [0.01 * i + (0.05 if (i + d) % 7 == 0 else 0) for i in range(30)]
        per_day[T0 + d * DAY] = (xs, ys)
    st = S.ic_stats(per_day, 7, 20)
    assert st["ic_mean"] > 0.8 and st["ic_t"] > 5 and st["hit_rate"] == 1.0 and st["q_spread"] > 0
    assert st["n_nonoverlap"] == 9


def test_binomial_and_dereg_history_factors():
    assert S.binom_two_sided(10, 10) == pytest.approx(2 / 1024)
    assert S.binom_two_sided(5, 10) == 1.0
    a = _series("1:1", 1, 1, [1.0] * 5, [0.1] * 5, 10.0, T0)
    b = _series("2:2", 2, 2, [1.0] * 5, [0.2] * 5, 20.0, T0)
    out = S.dereg_factors([a, b], imm=0, ref_ts=T0 + 10 * DAY, ref_block=10**7)
    assert out["dereg_rank_hist"]["1:1"][T0] == 1 and out["mp_gap_to_cull_1"]["2:2"][T0] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_run_study_and_write(db, tmp_path, wb_on):
    wb_on()
    for n in range(1, 26):
        o = await add_occupant(db, n, 1000 * n)
        closes = [math.exp(0.002 * n * i + 0.03 * math.sin(i / (3 + n))) for i in range(160)]
        await add_bars(db, n, o, "1d", closes, end_ts=T0, mps=[c * 0.99 for c in closes], liq=1000.0 * n)
    await db.execute("UPDATE price_bars SET block_number = 9000000 WHERE netuid = 1 AND ts = ?", (T0,))
    res = await S.run_study(db, horizons=(7,), now=T0 + DAY)
    f = res["factors"]["ema72_dist_1d"]["by_h"]["7"]
    assert f["n_days"] > 0 and -1 <= f["ic_mean"] <= 1
    path = S.write_result(res, tmp_path / "quant")
    assert path.name.startswith("wb_study_") and json.loads(path.read_text())["kind"] == "wb_study"
    trials = (tmp_path / "quant" / "results" / "wb_trials.jsonl").read_text().splitlines()
    assert json.loads(trials[0])["n_tests"] == len(S.FACTORS) * 2
    assert "exploration" in S.render(res)


@pytest.mark.asyncio
async def test_health_chain_probe_stale(rt, wb_on):
    from app.api.health import build_health
    wb_on()
    rt.jobs["chain_constants_probe"] = SimpleNamespace(status=lambda: {}, fail_alert_threshold=lambda: 5)
    rt.started_at -= 49 * 3600
    h = await build_health(rt)
    assert "chain_probe_stale" in h["degraded_reasons"]
    await rt.db.execute("INSERT INTO chain_constants (name, source, observed_at, value) VALUES "
                        "('NetworkRegistrationCost', 'rpc_storage', strftime('%s','now'), '1')")
    assert "chain_probe_stale" not in (await build_health(rt))["degraded_reasons"]
    rt.jobs.pop("chain_constants_probe")
