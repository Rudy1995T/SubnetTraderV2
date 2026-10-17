"""19 §2.4 constants probe and §2.5 GitHub probe (fake HTTP, no network)."""
import asyncio
import json

import pytest

from app.collectors import chain_probe as P
from app.collectors import wb_github_probe as G
from app.collectors.chain_rpc import storage_key
from tests.wb.conftest import DAY, T0, add_occupant


class Resp:
    def __init__(self, status, body):
        self.status_code, self._b = status, body

    def json(self):
        return self._b


def le(v: int, n: int) -> str:
    return "0x" + v.to_bytes(n, "little").hex()


STORE = {
    storage_key("NetworkImmunityPeriod"): le(432000, 8), storage_key("SubnetLimit"): le(128, 2),
    storage_key("NetworkRateLimit"): le(14400, 8), storage_key("NetworkMinLockCost"): le(10**9, 8),
    storage_key("NetworkLastLockCost"): le(493_992_245_801, 8),
    storage_key("NetworkLockReductionInterval"): le(57600, 8),
    storage_key("SubnetMovingAlpha"): le(int(0.0003 * 2**32), 16),
    storage_key("LastRateLimitedBlock") + "02": le(9_289_316, 8),
}


class FakeRpc:
    def __init__(self, store=STORE, hang=False):
        self.store, self.hang, self.calls = dict(store), hang, []

    async def request(self, provider, method, url, *, json=None, timeout=None, **kw):
        assert provider == "subtensor_rpc" and url.startswith("https://")
        if self.hang:
            await asyncio.sleep(5)
        m, p = json["method"], json["params"]
        self.calls.append(m)
        res = {"chain_getFinalizedHead": "0xhead", "chain_getHeader": {"number": hex(9_298_478)},
               "state_getStorage": self.store.get(p[0]) if p else None,
               "state_call": le(830_713_145_750, 8)}[m]
        return Resp(200, {"jsonrpc": "2.0", "id": 1, "result": res})


@pytest.mark.asyncio
async def test_probe_reads_and_decodes():
    block, v = await P.ChainProbe(FakeRpc(), "wss://entrypoint-finney.opentensor.ai:443", 5).read()
    assert block == 9_298_478 and v["NetworkImmunityPeriod"] == 432000 and v["SubnetLimit"] == 128
    assert v["NetworkLastRegisteredBlock"] == 9_289_316 and v["NetworkRegistrationCost"] == 830_713_145_750
    assert v["SubnetMovingAlpha"] == pytest.approx(0.0003, rel=1e-6)


@pytest.mark.asyncio
async def test_probe_drift_once_and_previous_value_drift(db, wb_settings):
    wb_settings(NETWORK_IMMUNITY_PERIOD_BLOCKS=864000)
    block, v = await P.ChainProbe(FakeRpc(), "wss://x:443", 5).read()
    n, ev = await P.store(db, block, v, endpoint="wss://x:443", observed_at=T0, run_id="a")
    assert n == 9 and ev == 1
    n, ev = await P.store(db, block, v, endpoint="wss://x:443", observed_at=T0 + 60, run_id="b")
    assert ev == 0                                                   # deduped on name|live
    v2 = {**v, "SubnetLimit": 256}
    _, ev = await P.store(db, block, v2, endpoint="wss://x:443", observed_at=T0 + 120, run_id="c")
    assert ev == 1
    rows = await db.fetchall("SELECT payload FROM events WHERE event_type = 'chain_constant_drift' ORDER BY id")
    pays = [json.loads(r["payload"]) for r in rows]
    assert pays[0]["name"] == "NetworkImmunityPeriod" and pays[0]["live"] == 432000 and pays[0]["config"] == 864000
    assert pays[1] == {**pays[1], "name": "SubnetLimit", "config": "128", "live": 256}
    assert await db.fetchval("SELECT COUNT(*) FROM chain_constants WHERE source = 'rpc_storage'") == 27


@pytest.mark.asyncio
async def test_probe_hang_times_out():
    from app.registry.models import RpcError
    with pytest.raises(RpcError, match="timeout"):
        await P.ChainProbe(FakeRpc(hang=True), "wss://x:443", 0.05).read()


def test_parse_github():
    assert G.parse_github("https://github.com/macrocosm-os/apex") == ("macrocosm-os", "apex")
    assert G.parse_github("github.com/org/repo.git/tree/main") == ("org", "repo")
    assert G.parse_github("https://github.com/chutesai") == ("chutesai", None)
    assert G.parse_github("https://gitlab.com/x/y") is None and G.parse_github(None) is None


class FakeGh:
    def __init__(self):
        self.urls = []

    async def request(self, provider, method, url, *, params=None, **kw):
        assert provider == "github"
        self.urls.append(url)
        if url.endswith("/repos/ok/repo"):
            return Resp(200, {"full_name": "ok/repo", "pushed_at": "2026-10-11T00:00:00Z", "archived": False,
                              "stargazers_count": 3})
        if url.endswith("/orgs/org/repos"):
            return Resp(200, [{"full_name": "org/latest", "pushed_at": "2026-10-15T00:00:00Z"}])
        return Resp(404, {"message": "Not Found"})


@pytest.mark.asyncio
async def test_github_probe_rows_and_once_per_day(db, wb_settings):
    wb_settings()
    await add_occupant(db, 1, 1, github="https://github.com/ok/repo")
    await add_occupant(db, 2, 2, github="https://github.com/org")
    await add_occupant(db, 3, 3, github="https://github.com/gone/repo")
    await add_occupant(db, 4, 4, github=None)
    gh = FakeGh()
    st = await G.run_probe(db, gh, T0 + 3600, per_run=45, run_id="r", clock=lambda: T0 + 3600)
    assert st == {"ok": 2, "absent": 1, "failed": 1, "requests": 3, "rows": 4}
    rows = {r["occupant_key"]: r for r in await db.fetchall(
        "SELECT * FROM metric_values WHERE metric_key = 'repo_pushed_days'")}
    assert rows["1:1"]["value"] == pytest.approx(5 + 1 / 24) and rows["1:1"]["source"] == "github"
    assert json.loads(rows["2:2"]["value_json"])["kind"] == "org_latest"
    assert json.loads(rows["3:3"]["value_json"])["reason"] == "http_404" and rows["3:3"]["value"] is None
    assert json.loads(rows["4:4"]["value_json"])["status"] == "absent"
    st2 = await G.run_probe(db, gh, T0 + 7200, per_run=45, run_id="r2", clock=lambda: T0 + 7200)
    assert st2["requests"] == 0                                        # one probe per repo per UTC day
    st3 = await G.run_probe(db, gh, T0 + DAY + 60, per_run=1, run_id="r3", clock=lambda: T0 + DAY + 60)
    assert st3["requests"] == 1                                        # per-run cap


def test_github_probe_job_off_with_token():
    from app.jobs import SPEC_BY_ID
    import app.config as app_config
    spec = SPEC_BY_ID["wb_github_probe"]
    assert spec.enabled_fn(app_config.Settings(GITHUB_TOKEN="ghp_x")) is False
    assert spec.enabled_fn(app_config.Settings(GITHUB_TOKEN="")) is True
