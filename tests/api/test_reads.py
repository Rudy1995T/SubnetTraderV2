"""Spec 14 §9 read-tier tests: feed contract, filters, history, matrix, detail/evidence, events, exports, quant,
UI pages, error shape, CORS."""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Literal

import pytest
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.util.time import to_iso
from tests.api.conftest import D0, OCC9, OCC47, OCC64, OCC130, P0, SPEC

pytestmark = pytest.mark.asyncio
_OCC_RE = re.compile(r"^\d{1,4}:\d+$")


# Mirror of 15 §3.2's FeedPayload/ScoreRow. The real consumer lives in another repo (~/tao-terminal/app/sentinel/models.py,
# built 2026-10-18), so it cannot be imported here; it carries this route's output as a golden fixture.
class ScoreRow(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)
    netuid: int = Field(ge=0, le=1023)
    occupant_key: str = Field(min_length=3, max_length=17)
    as_of: datetime | None = None
    computed_at: datetime | None = None
    quality: float | None = Field(default=None, ge=0.0, le=100.0)
    velocity: float | None = Field(default=None, ge=0.0, le=100.0)
    composite: float | None = Field(default=None, ge=0.0, le=100.0)
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    tier: Literal["A", "B", "C", "D", "F"] | None = None
    red_flags: tuple[str, ...] = Field(default=(), max_length=32)
    pillars: dict[str, float | None] | None = None     # 15 §3.2 (2026-10-23): display only in the consumer

    @field_validator("occupant_key")
    @classmethod
    def _occ(cls, v: str) -> str:
        assert _OCC_RE.match(v)
        return v

    @field_validator("pillars")
    @classmethod
    def _pillars(cls, v):
        # the consumer turns a malformed value into None; the producer must never send one
        assert v is None or (set(v) <= {"P1", "P2", "P3", "P4", "P5", "P6"}
                             and all(x is None or 0.0 <= x <= 1.0 for x in v.values()))
        return v

    @model_validator(mode="after")
    def _key(self) -> "ScoreRow":
        assert int(self.occupant_key.split(":")[0]) == self.netuid
        return self


class FeedPayload(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)
    as_of: datetime | None = None
    spec_version: str | None = None
    ttl_seconds: int | None = Field(default=None, ge=1, le=86400 * 30)
    stale: bool = False
    generated_at: datetime | None = None
    computed_at: datetime | None = None
    run_kind: str | None = Field(default=None, max_length=16)
    scores: tuple[ScoreRow, ...] = Field(default=(), min_length=0, max_length=1024)

    @model_validator(mode="after")
    def _batch(self) -> "FeedPayload":
        if self.scores:
            assert self.as_of is not None and self.spec_version and self.ttl_seconds
            assert len({r.netuid for r in self.scores}) == len(self.scores)
        return self


async def test_health_passthrough(seeded, app_client):
    from app.api.health import build_health
    body = (await app_client.get("/health")).json()
    ref = await build_health(seeded.rt)
    assert set(body) == set(ref)
    assert body["status"] in ("ok", "degraded", "initializing")


async def test_health_services_probes(seeded, app_client, monkeypatch, cfg):
    from app.api import health
    calls = []

    class FakeHttp:
        async def request(self, provider, method, url, **kw):
            calls.append(provider)
            raise RuntimeError("no network in tests")
    monkeypatch.setattr("app.http.get_http", lambda: FakeHttp())
    cfg(GITHUB_TOKEN="ghp_x", X_PROVIDER="twitterapi_io", LLM_ENABLED=False, SUBTENSOR_RPC_ENABLED=False)
    s = (await app_client.get("/api/health/services")).json()
    assert set(s["services"]) == set(health.PROBES) and len(s["services"]) == 12
    assert s["cached"] is False
    sv = s["services"]
    assert sv["database"]["state"] == "ok"
    assert sv["github"]["state"] == "fail" and "RuntimeError" in sv["github"]["detail"]   # a raising probe
    assert sv["disk"]["state"] in ("ok", "fail")                                           # others unaffected
    assert sv["anthropic"]["state"] == "disabled" and sv["anthropic"]["ok"] is True
    assert sv["subtensor_rpc"]["state"] == "disabled"
    assert sv["telegram"]["state"] == "disabled"
    assert set(calls) == {"github"}                     # passive probes (tmc, tmc_sse, discord, x) made no call
    again = (await app_client.get("/api/health/services")).json()
    assert again["cached"] is True and calls == ["github"]


async def test_scores_latest_contract(seeded, app_client):
    r = await app_client.get("/api/v1/scores/latest")
    assert r.status_code == 200 and r.headers["cache-control"] == "no-store" and r.headers["etag"].startswith('W/"')
    assert r.headers["x-taosentinel-spec"] == SPEC
    b = r.json()
    FeedPayload.model_validate(b)
    assert [x["netuid"] for x in b["scores"]] == [64, 47]          # SN130 pre-launch, SN9 insufficient dropped
    top = b["scores"][0]
    assert top["quality"] == 84.1 and top["composite"] == 82.4 and top["confidence"] == 0.91
    assert top["as_of"] == to_iso(P0) and top["computed_at"] == to_iso(P0 + 900)
    assert top["pillars"]["P3"] is None
    assert b["scores"][1]["red_flags"] == ["dereg_imminent"]
    assert b["as_of"] == to_iso(P0) and b["run_kind"] == "provisional" and b["spec_version"] == SPEC
    assert b["stale"] is False and b["ttl_seconds"] == 10 * 86400 and b["count"] == 2


async def test_scores_latest_filters(seeded, app_client):
    g = lambda q: app_client.get("/api/v1/scores/latest" + q)  # noqa: E731
    assert [x["netuid"] for x in (await g("?min_confidence=0.6")).json()["scores"]] == [64]
    assert [x["netuid"] for x in (await g("?tier=A")).json()["scores"]] == [64]
    pre = (await g("?include_prelaunch=1")).json()["scores"]
    assert any(x["netuid"] == 130 and x["composite"] is None and x["tier"] is None for x in pre)
    assert pre[-1]["netuid"] == 130                                 # nulls last
    assert 9 in [x["netuid"] for x in (await g("?include_insufficient=1")).json()["scores"]]
    assert [x["netuid"] for x in (await g("?netuids=47,9")).json()["scores"]] == [47]
    assert (await g("?tier=Z")).status_code == 422
    r1 = await g("")
    assert (await app_client.get("/api/v1/scores/latest", headers={"If-None-Match": r1.headers["etag"]})).status_code \
        == 304


async def test_scores_latest_killed_and_empty(seeded, app_client, tmp_root, cfg):
    (tmp_root / "SCORING_KILLED").write_text("")
    b = (await app_client.get("/api/v1/scores/latest")).json()
    assert b["stale"] is True and b["count"] == 2
    (tmp_root / "SCORING_KILLED").unlink()
    b = (await app_client.get("/api/v1/scores/latest?spec=ts-9.9")).json()
    assert b == {**b, "count": 0, "scores": [], "as_of": None, "stale": True}
    FeedPayload.model_validate(b)
    fresh = await app_client.get("/api/v1/scores/latest")
    cfg(SCORE_FEED_TTL_SEC=60)
    r = await app_client.get("/api/v1/scores/latest", headers={"If-None-Match": fresh.headers["etag"]})
    assert r.status_code == 200 and r.json()["stale"] is True        # TTL passing alone changes the ETag


async def test_history_resolves_netuid_and_key(seeded, app_client):
    a = (await app_client.get("/api/v1/scores/history?occupant=64&days=30")).json()
    b = (await app_client.get(f"/api/v1/scores/history?occupant={OCC64}&days=30")).json()
    assert a == b and a["occupant_key"] == OCC64 and len(a["points"]) == 8        # provisional dropped
    assert [p["as_of"] for p in a["points"]] == sorted(p["as_of"] for p in a["points"])
    c = (await app_client.get("/api/v1/scores/history?occupant=64&days=30&official_only=0")).json()
    assert len(c["points"]) == 9 and c["points"][-1]["run_kind"] == "provisional"
    assert (await app_client.get("/api/v1/scores/history?occupant=999")).status_code == 404
    assert (await app_client.get("/api/v1/scores/history?occupant=64&days=0")).status_code == 422


async def test_subnets_list_spark(seeded, app_client):
    b = (await app_client.get("/api/v1/subnets")).json()
    by = {s["netuid"]: s for s in b["subnets"]}
    assert set(by) == {64, 47, 9}                                   # pre-launch hidden by default
    assert by[64]["spark_7d"] == sorted(by[64]["spark_7d"]) and len(by[64]["spark_7d"]) == 7
    assert len(by[47]["spark_7d"]) == 2                             # never padded
    assert by[9]["score"]["insufficient_data"] is True              # included here, flagged
    assert by[64]["links"]["x"] == "chutes_ai" and "stale" in b
    b2 = (await app_client.get("/api/v1/subnets?include_prelaunch=1")).json()
    assert 130 in {s["netuid"] for s in b2["subnets"]}


async def test_subnet_detail(seeded, app_client):
    from app.features.registry import METRICS
    b = (await app_client.get("/api/v1/subnets/47")).json()
    assert b["score"]["explain"]["flags"][0]["since"] == D0 - 86400                # explain verbatim (INTEGER)
    f = b["score"]["red_flags"][0]
    assert f["flag"] == "dereg_imminent" and f["multiplier"] == 0.6 and f["since"] == to_iso(D0 - 86400)
    d = (await app_client.get("/api/v1/subnets/64")).json()
    ev = {e["metric_key"]: e for e in d["evidence"]}
    assert len(d["evidence"]) == seeded.n_keys
    assert ev[seeded.absent_key]["raw_status"] == "absent" and ev[seeded.absent_key]["m"] == 0.0
    assert ev[seeded.failed_key]["m"] is None and ev[seeded.failed_key]["raw_status"] == "failed"
    k0 = d["evidence"][0]["metric_key"]
    assert d["evidence"][0]["freshness_s"] == METRICS[k0].freshness_s
    assert ev[seeded.ev_key]["evidence_url"] == "https://example.com/ev"
    assert d["site"]["last_event"] == "site_recovered" and d["site"]["reason"] == "target_changed"
    assert d["team"][0]["name"] == "Jon Durbin" and d["product"]["status"] == "live"
    assert len(d["history_90d"]) == 8 and d["tenure_days"] is not None
    assert (await app_client.get("/api/v1/subnets/999")).json()["error"] == "unknown_netuid"
    assert (await app_client.get(f"/api/v1/subnets/64?occupant={OCC47}")).status_code == 404


async def test_events_filters_pagination(seeded, app_client):
    g = lambda q: app_client.get("/api/v1/events" + q)  # noqa: E731
    allv = (await g("?since=30d")).json()
    assert allv["count"] >= 3
    ids = [e["id"] for e in allv["events"]]
    assert ids == sorted(ids, reverse=True)
    assert all(e["netuid"] == 64 for e in (await g("?since=30d&netuid=64")).json()["events"])
    assert [e["event_type"] for e in (await g("?since=30d&type=site_recovered")).json()["events"]] == ["site_recovered"]
    assert all(e["severity"] == "warning" for e in (await g("?since=30d&severity=warning")).json()["events"])
    page = (await g("?since=30d&limit=1")).json()
    assert page["count"] == 1 and page["next_before_id"] == page["events"][0]["id"]
    nxt = (await g(f"?since=30d&limit=1&before_id={page['next_before_id']}")).json()
    assert nxt["events"][0]["id"] < page["events"][0]["id"]
    assert (await g("?since=yesterday")).json()["error"] == "bad_since"
    assert (await g("?since=24h")).status_code == 200


async def test_matrix_csv_etag(seeded, app_client):
    from tests.api.conftest import _score
    r = await app_client.get("/api/v1/matrix.csv")
    lines = r.text.strip().split("\n")
    assert lines[0] == ("netuid,occupant_key,name,status,tier,composite,composite_raw,quality,velocity,confidence,"
                        "P1,P2,P3,P4,P5,P6,red_flags,as_of,computed_at,spec_version")
    assert any(",dereg_imminent," in ln for ln in lines) and "attachment" in r.headers["content-disposition"]
    assert r.headers["cache-control"].startswith("public, max-age=")
    tag = r.headers["etag"]
    assert (await app_client.get("/api/v1/matrix.csv", headers={"If-None-Match": tag})).status_code == 304
    await _score(seeded.db, OCC9, 9, P0, comp=41.0, quality=41.0, velocity=30.0, conf=0.3, tier="C")
    assert (await app_client.get("/api/v1/matrix.csv")).headers["etag"] != tag


async def test_export_json_equals_latest(seeded, app_client):
    a = (await app_client.get("/api/v1/export/scores.json")).json()
    b = (await app_client.get("/api/v1/scores/latest")).json()
    a.pop("generated_at")
    b.pop("generated_at")
    assert a == b


async def test_quant_results(tmp_root, app_client, cfg):
    d = tmp_root / "q"
    d.mkdir()
    cfg(QUANT_RESULTS_DIR=str(d))
    (d / "factor_survivorship_20260922_101500.json").write_text(json.dumps({"scope": "survivorship", "x": 1}))
    (d / "gate_verdict_quality_20260922_101500.json").write_text(json.dumps({"gate_pass": False, "pending": True}))
    (d / "ic_series_survivorship_20260922_101500.json").write_text("[]")
    (d / "trials.json").write_text("[]")
    (d / "factor_bad_20260922_101500.json").write_text("{nope")
    (tmp_root / "escape.json").write_text("{}")
    b = (await app_client.get("/api/v1/quant/results")).json()
    kinds = {f["file"]: f["kind"] for f in b["files"]}
    assert kinds == {"factor_survivorship_20260922_101500.json": "factor",
                     "gate_verdict_quality_20260922_101500.json": "verdict"}
    assert b["errors"] and "factor_bad" in b["errors"][0]
    f = next(x for x in b["files"] if x["kind"] == "factor")
    assert f["run_ts"] == "2026-09-22T10:15:00Z" and f["summary"] == {"scope": "survivorship"}
    assert (await app_client.get("/api/v1/quant/results/trials.json")).json() == []
    assert (await app_client.get("/api/v1/quant/results/..%2Fescape.json")).status_code == 404
    assert (await app_client.get("/api/v1/quant/results/nope.json")).status_code == 404


async def test_ui_pages(app_client):
    pages = [await app_client.get(p) for p in ("/ui", "/ui/matrix", "/ui/subnet/64", "/ui/events")]
    assert all(r.status_code == 200 and r.headers["content-type"].startswith("text/html") for r in pages)
    assert len({r.text for r in pages}) == 1 and "/ui/static/app.js" in pages[0].text
    js = await app_client.get("/ui/static/app.js")
    assert js.status_code == 200 and "javascript" in js.headers["content-type"]
    assert (await app_client.get("/ui/static/style.css")).status_code == 200
    assert (await app_client.get("/ui/static/nope.js")).status_code == 404
    trav = await app_client.get("/ui/static/..%2Fapp.py")         # never a file outside frontend/ui: SPA fallback
    assert trav.text == pages[0].text
    assert (await app_client.get("/", follow_redirects=False)).headers["location"] == "/ui/matrix"


async def test_error_shape(rt, app_client, cfg):
    cfg(API_AUTH_TOKEN="")
    r = await app_client.post("/api/v1/control/pause/telegram_bot")
    assert r.status_code == 409 and r.json() == {"error": "not_pausable", "detail": "not_pausable"}
    r = await app_client.post("/api/v1/manual/note", json={"netuid": "x"})
    assert r.status_code == 422 and r.json()["error"] == "invalid_body"
    r = await app_client.get("/api/v1/nope")
    assert r.status_code == 404 and r.json()["error"] == "not_found"


async def test_db_unavailable(app_client, rt):
    from app.api import scores
    scores.configure(lambda: None)
    try:
        r = await app_client.get("/api/v1/scores/latest")
        assert r.status_code == 503 and r.json()["error"] == "db_unavailable"
    finally:
        scores.configure(lambda: rt)


async def test_cors_and_methods(app_client):
    ok = await app_client.options("/api/v1/manual/note", headers={
        "Origin": "http://127.0.0.1:3001", "Access-Control-Request-Method": "POST",
        "Access-Control-Request-Headers": "X-Auth-Token,Content-Type"})
    assert ok.status_code == 200 and ok.headers["access-control-allow-origin"] == "http://127.0.0.1:3001"
    bad = await app_client.get("/health", headers={"Origin": "https://evil.example"})
    assert "access-control-allow-origin" not in bad.headers
    assert (await app_client.put("/api/v1/manual/note", json={})).status_code == 405
    assert (await app_client.delete("/api/v1/events")).status_code == 405
