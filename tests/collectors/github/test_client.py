"""Spec 06 §6/§8/§9: GitHubClient + GitHubQuota over a stub 01 Http."""
import httpx
import pytest

from app.collectors.github.client import (GH_OK, GitHubAuthError, GitHubClient, GitHubQuota, GitHubUnavailable,
                                          QuotaDeferred, parse_link_next, parse_redirect_id)
from app.http import ProviderError
from app.ratelimit import BudgetExhausted
from tests.collectors.github.conftest import NOW


class StubHttp:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def request(self, provider, method, url, **kw):
        self.calls.append((provider, method, url, kw))
        r = self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]
        if isinstance(r, Exception):
            raise r
        return r


def resp(status, body=b"", headers=None):
    if isinstance(body, (dict, list)):
        import json
        body = json.dumps(body).encode()
    return httpx.Response(status, headers=headers or {}, content=body)


def rl(remaining, reset=NOW + 5, used=1):
    return {"x-ratelimit-limit": "5000", "x-ratelimit-remaining": str(remaining), "x-ratelimit-used": str(used),
            "x-ratelimit-reset": str(reset), "x-ratelimit-resource": "core"}


class Clock:
    def __init__(self, t=NOW):
        self.t = float(t)
        self.slept = []

    def __call__(self):
        return self.t

    async def sleep(self, s):
        self.slept.append(s)
        self.t += s


def make(responses, db=None, clock=None):
    clock = clock or Clock()
    http = StubHttp(responses)
    q = GitHubQuota(clock=clock, sleep=clock.sleep)
    return GitHubClient(http, q, db, clock=clock, sleep=clock.sleep), http, clock


@pytest.mark.asyncio
async def test_every_call_goes_through_http_with_ok_statuses():
    c, http, _ = make([resp(200, {"id": 1}, rl(4999))])
    r = await c.get("/repositories/1", job="github_resolve")
    assert r.status == 200 and r.json == {"id": 1}
    prov, method, url, kw = http.calls[0]
    assert (prov, method, url) == ("github", "GET", "https://api.github.com/repositories/1")
    assert kw["ok_statuses"] == GH_OK and kw["follow_redirects"] is False
    assert kw["headers"]["X-GitHub-Api-Version"] == "2022-11-28"
    assert c.quota.remaining == 4999 and c.quota.limit == 5000 and c.quota.resource == "core"


@pytest.mark.asyncio
async def test_etag_stored_and_304_returns_cached_body(db):
    c, http, _ = make([resp(200, {"id": 7, "v": 1}, {**rl(10, used=4), "etag": 'W/"abc"'}),
                       resp(304, b"", rl(10, used=4))], db=db)
    a = await c.get("/repositories/7", etag_key="", job="github_resolve")
    b = await c.get("/repositories/7", etag_key="", job="github_resolve")
    assert a.status == 200 and not a.from_cache
    assert b.status == 304 and b.from_cache and b.json == {"id": 7, "v": 1} and b.ok
    assert http.calls[1][3]["headers"]["If-None-Match"] == 'W/"abc"'
    assert c.quota.used == 4                       # the 304's x-ratelimit-* headers are followed
    assert c.stats["github_resolve"]["cached"] == 1


@pytest.mark.asyncio
async def test_301_returned_unfollowed():
    loc = "https://api.github.com/repositories/884216433"
    c, _, _ = make([resp(301, b"", {"location": loc})])
    r = await c.get("/repos/rayonlabs/chutes")
    assert r.status == 301 and r.location == loc and r.json is None


@pytest.mark.asyncio
async def test_primary_403_sleeps_to_reset_and_retries():
    clock = Clock()
    c, http, _ = make([resp(403, b'{"message":"API rate limit exceeded"}', rl(0, reset=int(NOW) + 5)),
                       resp(200, {"ok": 1}, rl(4999))], clock=clock)
    r = await c.get("/repositories/1", job="github_activity", deadline=NOW + 100)
    assert r.status == 200 and len(http.calls) == 2
    assert clock.slept == [7.0]                     # reset + 2 s
    assert c.quota.sleeping_until is None


@pytest.mark.asyncio
async def test_primary_403_past_deadline_raises_quota_deferred():
    c, _, _ = make([resp(403, b"{}", rl(0, reset=int(NOW) + 600))])
    with pytest.raises(QuotaDeferred):
        await c.get("/repositories/1", job="github_activity", deadline=NOW + 60)


@pytest.mark.asyncio
async def test_secondary_limit_backs_off_three_times_then_unavailable():
    clock = Clock()
    sec = resp(403, b'{"message":"You have exceeded a secondary rate limit"}', {"retry-after": "2"})
    c, http, _ = make([sec], clock=clock)
    with pytest.raises(GitHubUnavailable):
        await c.get("/repositories/1", job="github_activity")
    assert clock.slept == [2.0, 4.0, 8.0] and len(http.calls) == 4 and c.serial


@pytest.mark.asyncio
async def test_other_403_is_returned_as_blocked():
    c, _, _ = make([resp(403, b'{"message":"Repository access blocked"}', rl(4000))])
    r = await c.get("/repos/a/b")
    assert r.status == 403


@pytest.mark.asyncio
async def test_401_raises_auth_error_and_marks_token_bad():
    c, _, _ = make([resp(401, b'{"message":"Bad credentials"}')])
    with pytest.raises(GitHubAuthError):
        await c.get("/repositories/1")
    assert c.token_state == "bad"


@pytest.mark.asyncio
async def test_provider_error_becomes_unavailable_and_budget_propagates():
    c, _, _ = make([ProviderError("github", "HTTP 502", status=502, url="https://api.github.com/x", attempts=4)])
    with pytest.raises(GitHubUnavailable):
        await c.get("/x", job="j")
    assert c.stats["j"]["failed"] == 1
    c2, _, _ = make([BudgetExhausted("github", "hour", int(NOW) + 60)])
    with pytest.raises(BudgetExhausted):
        await c2.get("/x")


@pytest.mark.asyncio
async def test_wait_if_reserved_rules():
    clock = Clock()
    q = GitHubQuota(clock=clock, sleep=clock.sleep)
    q.update(rl(299, reset=int(NOW) + 100))
    await q.wait_if_reserved("github_resolve")            # resolve never waits
    assert clock.slept == []
    await q.wait_if_reserved("github_activity")           # < 300 → waits to reset + 2
    assert clock.slept == [102.0]
    q.update(rl(1499, reset=int(clock.t) + 50))
    await q.wait_if_reserved("backfill_github", reserve=1500)
    assert clock.slept[-1] == 52.0
    q.update(rl(1500, reset=int(clock.t) + 50))
    await q.wait_if_reserved("backfill_github", reserve=1500)
    assert len(clock.slept) == 2
    q.update(rl(10, reset=int(clock.t) + 500))
    with pytest.raises(QuotaDeferred):
        await q.wait_if_reserved("github_activity", deadline=clock.t + 10)


def test_token_expiration_header(settings_override):
    q = GitHubQuota()
    q.update({"github-authentication-token-expiration": "2027-09-01 12:00:00 UTC"})
    assert q.token_expiration_raw == "2027-09-01 12:00:00 UTC"
    from app.util.time import parse_iso
    assert q.token_expires_at == parse_iso("2027-09-01T12:00:00Z")
    q2 = GitHubQuota()
    q2.update({"x-ratelimit-remaining": "1"})
    assert q2.token_expires_at is None and q2.token_expiration_raw is None
    settings_override(GITHUB_TOKEN_EXPIRES_AT="2027-10-16")
    assert q2.expires_at() == parse_iso("2027-10-16T00:00:00Z")
    assert q2.snapshot()["token_expires_at"] == "2027-10-16T00:00:00Z"


@pytest.mark.asyncio
async def test_get_all_pages_follows_link():
    p1 = resp(200, [1, 2], {"link": '<https://api.github.com/repositories/1/commits?page=2>; rel="next"'})
    p2 = resp(200, [3], {})
    c, http, _ = make([p1, p2])
    out = await c.get_all_pages("/repositories/1/commits", params={"per_page": 2}, max_pages=5)
    assert out.items == [1, 2, 3] and out.pages == 2 and not out.truncated
    assert http.calls[1][2] == "https://api.github.com/repositories/1/commits?page=2" and http.calls[1][3]["params"] is None


@pytest.mark.asyncio
async def test_get_all_pages_cap_marks_truncated_and_409():
    p1 = resp(200, [1], {"link": '<https://api.github.com/x?page=2>; rel="next"'})
    c, _, _ = make([p1])
    out = await c.get_all_pages("/x", max_pages=1)
    assert out.truncated and out.items == [1]
    c2, _, _ = make([resp(409, b'{"message":"Git Repository is empty."}')])
    out2 = await c2.get_all_pages("/x", max_pages=3)
    assert out2.status == 409 and out2.items == []


@pytest.mark.asyncio
async def test_html_uses_github_html_provider_without_auth():
    c, http, _ = make([resp(200, b"<html></html>")])
    r = await c.get_html("chutesai/chutes")
    prov, _, url, kw = http.calls[0]
    assert prov == "github_html" and url == "https://github.com/chutesai/chutes"
    assert "Authorization" not in (kw.get("headers") or {}) and r.text == "<html></html>"


def test_http_client_auth_header_only_for_github(settings_override):
    from app.http import _auth_headers
    settings_override(GITHUB_TOKEN="tok")
    assert _auth_headers("github") == {"Authorization": "Bearer tok"}
    assert _auth_headers("github_html") == {}


def test_parsers():
    assert parse_redirect_id("https://api.github.com/repositories/884216433") == 884216433
    assert parse_redirect_id("https://api.github.com/repositories/5/commits?per_page=1") == 5
    assert parse_redirect_id("https://github.com/chutesai/chutes") is None and parse_redirect_id(None) is None
    assert parse_link_next({"link": '<u2>; rel="next", <u9>; rel="last"'}) == "u2"
    assert parse_link_next({}) is None


def test_degraded_rule():
    c = GitHubClient(None, GitHubQuota())
    c.reset_stats("j")
    c.stats["j"].update(calls=10, failed=6)
    assert c.degraded("j")
    c.stats["j"].update(calls=10, failed=5)
    assert not c.degraded("j")


def test_pat_fallback_when_token_line_empty(tmp_path, monkeypatch):
    import app.config as app_config
    env = tmp_path / ".env"
    env.write_text("GITHUB_TOKEN=\nGITHUB_PAT=github_pat_x\n")
    monkeypatch.setenv("TAOSENTINEL_ENV_FILE", str(env))
    monkeypatch.delenv("GITHUB_PAT", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    s = app_config.Settings(_env_file=str(env))
    assert s.GITHUB_TOKEN == "github_pat_x"
    env.write_text("GITHUB_TOKEN=tok_a\nGITHUB_PAT=github_pat_x\n")
    assert app_config.Settings(_env_file=str(env)).GITHUB_TOKEN == "tok_a"


def test_github_settings_validators():
    import app.config as app_config
    with pytest.raises(ValueError):
        app_config.Settings(_env_file=None, GITHUB_GONE_GRACE_DAYS=7)
    with pytest.raises(ValueError):
        app_config.Settings(_env_file=None, GITHUB_METRICS_CRON="21:00")
    with pytest.raises(ValueError):
        app_config.Settings(_env_file=None, GITHUB_TOKEN_EXPIRES_AT="next year")
