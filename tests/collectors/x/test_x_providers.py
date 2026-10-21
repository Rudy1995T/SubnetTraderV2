"""Spec 09 §9.1 test_x_providers: normalisation to XPost, since_id paging, costs, X_PRICES_JSON, error mapping,
one Http.request per call with the provider id and ok_statuses=(200, 400, 402). Fixtures are synthetic (UNVERIFIED
vendor shapes, ``"_synthetic": true``) until scripts/record_x_fixtures.py runs with a key."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx

from app.collectors.x_providers import make_provider
from app.collectors.x_providers.base import MentionQuery, XCall, XPage, XProviderError
from app.collectors.x_providers.null import NullProvider
from app.collectors.x_providers.official_v2 import OfficialV2
from app.collectors.x_providers.prices import parse_override
from app.collectors.x_providers.twitterapi_io import FIELD_MAP, TwitterApiIo
from tests.collectors.x.conftest import SpyHttp, fixture_json

TA = "https://api.twitterapi.io"
OF = "https://api.x.com/2"


def test_fixtures_are_marked_synthetic():
    for rel in ("twitterapi_io/user_info.json", "twitterapi_io/last_tweets_p1.json", "official_v2/search_recent.json"):
        assert fixture_json(rel)["_synthetic"] is True


def test_make_provider(xset, http):
    assert make_provider(xset(X_PROVIDER="twitterapi_io"), http).name == "twitterapi_io"
    assert make_provider(xset(X_PROVIDER="official_v2"), http).name == "official_v2"
    assert make_provider(xset(X_PROVIDER="null"), http).name == "null"


@pytest.mark.asyncio
async def test_null_provider_is_free_and_empty():
    p = NullProvider()
    assert await p.resolve_user("x") is None
    page = await p.user_timeline("1", None, 20)
    assert page.posts == [] and p.cost_estimate(XCall("user_timeline", 20)) == 0.0


# ── twitterapi.io ───────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
@respx.mock
async def test_ta_resolve_user_and_not_found(xset, http):
    xset()
    spy = SpyHttp(http)
    p = TwitterApiIo(spy)
    respx.get(f"{TA}/twitter/user/info", params={"userName": "chutes_ai"}).mock(
        return_value=httpx.Response(200, json=fixture_json("twitterapi_io/user_info.json")))
    respx.get(f"{TA}/twitter/user/info", params={"userName": "ghost"}).mock(
        return_value=httpx.Response(200, json=fixture_json("twitterapi_io/user_not_found.json")))
    respx.get(f"{TA}/twitter/user/info", params={"userName": "gone_acct"}).mock(
        return_value=httpx.Response(200, json=fixture_json("twitterapi_io/user_unavailable.json")))
    respx.get(f"{TA}/twitter/user/info", params={"userName": "fourohfour"}).mock(return_value=httpx.Response(404))
    u = await p.resolve_user("chutes_ai")
    assert (u.id, u.handle, u.followers, u.following, u.post_count, u.verified) == \
        ("1700000000000000001", "chutes_ai", 25400, 310, 1830, True)
    assert u.created_at == datetime(2023, 11, 15, 10, 0, tzinfo=timezone.utc)
    assert await p.resolve_user("ghost") is None
    assert await p.resolve_user("gone_acct") is None
    assert await p.resolve_user("fourohfour") is None
    assert {c["provider"] for c in spy.calls} == {"twitterapi_io"}
    assert all(c["ok_statuses"] == (200, 400, 402) for c in spy.calls)
    assert len(spy.calls) == 4                                       # one request per call, no retry


@pytest.mark.asyncio
@respx.mock
async def test_ta_timeline_normalisation_and_costs(xset, http):
    xset()
    p = TwitterApiIo(http)
    body = fixture_json("twitterapi_io/last_tweets_p1.json")
    respx.get(f"{TA}/twitter/user/last_tweets").mock(return_value=httpx.Response(200, json=body))
    page = await p.user_timeline("1700000000000000001", None, 20)
    assert len(page.posts) == 20 and page.billed_posts == 20 and page.next_cursor == "CURSOR_P2"
    ids = [t["id"] for t in body["tweets"]]
    assert page.newest_id == max(ids, key=int) and page.newest_id != ids[0]       # pinned first, newest = max
    by_id = {x.post_id: x for x in page.posts}
    reply, rt, quote, media, link = (by_id[body["tweets"][i]["id"]] for i in (2, 3, 4, 5, 6))
    assert reply.in_reply_to == body["tweets"][2]["inReplyToId"] and not reply.is_repost
    assert rt.is_repost and rt.in_reply_to is None
    assert quote.quoted_post_id == body["tweets"][4]["quoted_tweet"]["id"] and not quote.is_repost
    assert media.has_media and not media.has_url
    assert link.has_url
    first = page.posts[1]
    assert (first.author_id, first.author_handle, first.like_count, first.repost_count, first.reply_count,
            first.view_count) == ("1700000000000000001", "chutes_ai", 10, 2, 1, 900)
    assert first.author is not None and first.author.followers == 25400
    assert first.event_at.tzinfo is not None
    assert p.cost_actual(XCall("user_timeline", 20), page) == pytest.approx(0.003)          # 20 tweets → 0.003
    assert p.cost_estimate(XCall("user_timeline", 20)) == pytest.approx(0.003)
    assert p.cost_estimate(XCall("resolve_user", 0)) == pytest.approx(0.00018)
    assert p.cost_actual(XCall("search_recent", 20), XPage([], None, None, 0)) == pytest.approx(0.00015)  # min_call


@pytest.mark.asyncio
@respx.mock
async def test_ta_since_id_filtering_across_two_pages(xset, http):
    xset()
    p = TwitterApiIo(http)
    p1, p2 = fixture_json("twitterapi_io/last_tweets_p1.json"), fixture_json("twitterapi_io/last_tweets_p2.json")
    since = p2["tweets"][2]["id"]
    route = respx.get(f"{TA}/twitter/user/last_tweets")
    route.side_effect = [httpx.Response(200, json=p1), httpx.Response(200, json=p2)]
    a = await p.user_timeline("1700000000000000001", since, 20)
    assert a.next_cursor == "CURSOR_P2" and len(a.posts) == 19       # the old pinned tweet is ≤ since_id
    b = await p.user_timeline("1700000000000000001", since, 20, cursor=a.next_cursor)
    assert route.calls[1].request.url.params["cursor"] == "CURSOR_P2"
    assert [x.post_id for x in b.posts] == [t["id"] for t in p2["tweets"][:2]]
    assert b.next_cursor is None and b.billed_posts == 5
    assert max([a.newest_id, b.newest_id], key=int) == max([t["id"] for t in p1["tweets"]], key=int)
    # since_id inside page 1 → no further page is requested
    route.side_effect = [httpx.Response(200, json=p1)]
    c = await p.user_timeline("1700000000000000001", p1["tweets"][10]["id"], 20)
    assert c.next_cursor is None and all(int(x.post_id) > int(p1["tweets"][10]["id"]) for x in c.posts)


@pytest.mark.asyncio
@respx.mock
async def test_ta_search_render_and_authors(xset, http):
    xset()
    p = TwitterApiIo(http)
    body = fixture_json("twitterapi_io/advanced_search.json")
    route = respx.get(f"{TA}/twitter/tweet/advanced_search").mock(return_value=httpx.Response(200, json=body))
    q = MentionQuery(terms=['"Chutes"', "SN64", '"subnet 64"', "$SN64", "@chutes_ai"], required_coterms=[],
                     from_handles=[])
    since_at = datetime(2026, 9, 21, 4, 0, tzinfo=timezone.utc)
    page = await p.search_recent(q, None, since_at, 200)
    sent = route.calls[0].request.url.params
    assert sent["query"] == '("Chutes" OR SN64 OR "subnet 64" OR $SN64 OR @chutes_ai) -filter:retweets ' \
                            'since:2026-09-21_04:00:00_UTC'
    assert sent["queryType"] == "Latest"
    assert len(page.posts) == 3 and {x.author.handle for x in page.posts} == {"0xSammy", "taomoon12345"}
    young = next(x for x in page.posts if x.author_handle == "taomoon12345")
    assert young.author.followers == 12 and young.author.following == 2400
    # client-side since_id filter
    page2 = await p.search_recent(q, body["tweets"][1]["id"], None, 200)
    assert [x.post_id for x in page2.posts] == [body["tweets"][0]["id"]]


@pytest.mark.asyncio
@respx.mock
@pytest.mark.parametrize("status,fixture,kind", [(401, "err_401.json", "auth"), (403, "err_401.json", "auth"),
                                                 (402, "err_402.json", "budget"), (429, "err_429.json", "rate_limited"),
                                                 (400, "err_429.json", "parse")])
async def test_ta_error_mapping(xset, http, status, fixture, kind):
    xset()
    spy = SpyHttp(http)
    p = TwitterApiIo(spy)
    route = respx.get(f"{TA}/twitter/user/last_tweets").mock(
        return_value=httpx.Response(status, json=fixture_json(f"twitterapi_io/{fixture}")))
    with pytest.raises(XProviderError) as ei:
        await p.user_timeline("1", None, 20)
    assert ei.value.kind == kind
    assert route.call_count == 1                                     # no own retry or sleep (Http retries=0 here)


@pytest.mark.asyncio
@respx.mock
async def test_ta_balance_error_in_200_body(xset, http):
    xset()
    p = TwitterApiIo(http)
    respx.get(f"{TA}/twitter/user/last_tweets").mock(
        return_value=httpx.Response(200, json=fixture_json("twitterapi_io/err_402.json")))
    with pytest.raises(XProviderError) as ei:
        await p.user_timeline("1", None, 20)
    assert ei.value.kind == "budget"


@pytest.mark.asyncio
@respx.mock
async def test_ta_5xx_after_http_retries_is_transient(xset, http):
    xset()
    p = TwitterApiIo(http)
    respx.get(f"{TA}/twitter/user/info").mock(return_value=httpx.Response(503))
    with pytest.raises(XProviderError) as ei:
        await p.resolve_user("x")
    assert ei.value.kind == "transient"


def test_prices_override(xset, http):
    s = xset(X_PRICES_JSON='{"twitterapi_io":{"min_call":0.0015},"official_v2":{"per_post":0.004}}')
    ta, of = TwitterApiIo(http, s), OfficialV2(http, s)
    assert ta.cost_estimate(XCall("search_recent", 5)) == pytest.approx(0.0015)
    assert ta.cost_estimate(XCall("user_timeline", 20)) == pytest.approx(0.003)
    assert of.cost_estimate(XCall("user_timeline", 5)) == pytest.approx(0.02)
    with pytest.raises(ValueError):
        parse_override('{"twitterapi_io":{"per_banana":1}}')
    with pytest.raises(ValueError):
        parse_override('{"nitter":{"per_tweet":1}}')
    with pytest.raises(ValueError):
        parse_override('{"twitterapi_io":{"per_tweet":-1}}')
    assert parse_override("") == {}


def test_field_map_is_the_single_rename_point():
    assert FIELD_MAP["user"]["handle"] == "userName" and FIELD_MAP["tweet"]["likes"] == "likeCount"


# ── official v2 ─────────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
@respx.mock
async def test_of_users_and_not_found(xset, http):
    s = xset(X_PROVIDER="official_v2")
    spy = SpyHttp(http)
    p = OfficialV2(spy, s)
    respx.get(f"{OF}/users/by/username/chutes_ai").mock(
        return_value=httpx.Response(200, json=fixture_json("official_v2/users_by_username.json")))
    respx.get(f"{OF}/users/by/username/zz_no_such_user_09").mock(
        return_value=httpx.Response(200, json=fixture_json("official_v2/not_found.json")))
    u = await p.resolve_user("chutes_ai")
    assert (u.id, u.handle, u.followers, u.post_count, u.verified) == ("1700000000000000001", "chutes_ai", 25400,
                                                                       1830, False)
    assert await p.resolve_user("zz_no_such_user_09") is None
    assert {c["provider"] for c in spy.calls} == {"official_v2"}
    assert all(c["ok_statuses"] == (200, 400, 402) for c in spy.calls)
    assert p.cost_estimate(XCall("resolve_user", 0)) == pytest.approx(0.010)


@pytest.mark.asyncio
@respx.mock
async def test_of_timeline_first_poll_since_id_and_empty(xset, http):
    s = xset(X_PROVIDER="official_v2", X_OFFICIAL_EXCLUDE="retweets,replies")
    p = OfficialV2(http, s)
    body = fixture_json("official_v2/users_tweets_p1.json")
    route = respx.get(f"{OF}/users/1700000000000000001/tweets")
    route.side_effect = [httpx.Response(200, json=body),
                         httpx.Response(200, json=fixture_json("official_v2/users_tweets_empty.json"))]
    page = await p.user_timeline("1700000000000000001", None, 20)
    q0 = route.calls[0].request.url.params
    assert q0["max_results"] == "5" and "since_id" not in q0 and q0["exclude"] == "retweets,replies"
    assert len(page.posts) == 5 and page.billed_posts == 5 and page.next_cursor == body["meta"]["next_token"]
    assert page.newest_id == body["meta"]["newest_id"]
    by_id = {x.post_id: x for x in page.posts}
    d = body["data"]
    assert by_id[d[0]["id"]].has_url and by_id[d[0]["id"]].view_count == 1500
    assert by_id[d[1]["id"]].in_reply_to == d[1]["referenced_tweets"][0]["id"]
    assert by_id[d[2]["id"]].quoted_post_id == d[2]["referenced_tweets"][0]["id"] and by_id[d[2]["id"]].has_media
    assert by_id[d[3]["id"]].is_repost
    assert p.cost_actual(XCall("user_timeline", 5), page) == pytest.approx(0.025)
    empty = await p.user_timeline("1700000000000000001", d[0]["id"], 20, cursor="tok")
    q1 = route.calls[1].request.url.params
    assert q1["max_results"] == "20" and q1["since_id"] == d[0]["id"] and q1["pagination_token"] == "tok"
    assert empty.posts == [] and empty.billed_posts == 0
    assert p.cost_actual(XCall("user_timeline", 20), empty) == pytest.approx(0.005)       # empty → 0.005
    assert p.cost_estimate(XCall("user_timeline", 5)) == pytest.approx(0.025)


@pytest.mark.asyncio
@respx.mock
async def test_of_search_includes_join_and_render(xset, http):
    s = xset(X_PROVIDER="official_v2")
    now = datetime(2026, 10, 17, 12, 0, tzinfo=timezone.utc)
    p = OfficialV2(http, s, clock=lambda: now.timestamp())
    route = respx.get(f"{OF}/tweets/search/recent").mock(
        return_value=httpx.Response(200, json=fixture_json("official_v2/search_recent.json")))
    q = MentionQuery(terms=['"Score"', "SN44"], required_coterms=["bittensor", "TAO"], from_handles=[])
    page = await p.search_recent(q, None, None, 200)
    sent = route.calls[0].request.url.params
    assert sent["query"] == '("Score" OR SN44) (bittensor OR TAO) -is:retweet'
    assert sent["max_results"] == "100" and sent["expansions"] == "author_id"
    assert page.posts[0].author_handle == "0xSammy" and page.posts[0].author.followers == 89000


@pytest.mark.asyncio
@respx.mock
async def test_of_old_since_id_falls_back_to_start_time(xset, http):
    s = xset(X_PROVIDER="official_v2")
    now = datetime(2026, 10, 17, 12, 0, tzinfo=timezone.utc)
    p = OfficialV2(http, s, clock=lambda: now.timestamp())
    from tests.collectors.x.conftest import sf
    old = sf(int((now - timedelta(days=8)).timestamp()))
    route = respx.get(f"{OF}/tweets/search/recent").mock(
        return_value=httpx.Response(200, json=fixture_json("official_v2/search_recent.json")))
    await p.search_recent(MentionQuery(["SN64"], [], []), old, None, 100)
    sent = route.calls[0].request.url.params
    assert "since_id" not in sent and sent["start_time"] == "2026-10-10T13:00:00Z"


@pytest.mark.asyncio
@respx.mock
async def test_of_400_naming_since_id_retries_once_with_start_time(xset, http):
    s = xset(X_PROVIDER="official_v2")
    now = datetime(2026, 10, 17, 12, 0, tzinfo=timezone.utc)
    p = OfficialV2(http, s, clock=lambda: now.timestamp())
    from tests.collectors.x.conftest import sf
    recent = sf(int((now - timedelta(days=2)).timestamp()))
    route = respx.get(f"{OF}/tweets/search/recent")
    route.side_effect = [httpx.Response(400, json=fixture_json("official_v2/since_id_400.json")),
                         httpx.Response(200, json=fixture_json("official_v2/search_recent.json"))]
    page = await p.search_recent(MentionQuery(["SN64"], [], []), recent, None, 100)
    assert route.call_count == 2
    assert route.calls[0].request.url.params["since_id"] == recent
    assert "since_id" not in route.calls[1].request.url.params and "start_time" in route.calls[1].request.url.params
    assert len(page.posts) == 1
    # any other 400 → parse
    route.side_effect = [httpx.Response(400, json={"title": "Invalid Request", "detail": "bad query"})]
    with pytest.raises(XProviderError) as ei:
        await p.search_recent(MentionQuery(["SN64"], [], []), None, None, 100)
    assert ei.value.kind == "parse"


@pytest.mark.asyncio
@respx.mock
async def test_of_429_and_auth(xset, http):
    s = xset(X_PROVIDER="official_v2")
    p = OfficialV2(http, s)
    body = fixture_json("official_v2/err_429_headers.json")
    respx.get(f"{OF}/users/by/username/a").mock(return_value=httpx.Response(429, json=body, headers=body["_headers"]))
    respx.get(f"{OF}/users/by/username/b").mock(return_value=httpx.Response(401, json={"title": "Unauthorized"}))
    with pytest.raises(XProviderError) as ei:
        await p.resolve_user("b")
    assert ei.value.kind == "auth"
    with pytest.raises(XProviderError) as ei:                       # last: the limiter honours the reset header
        await p.resolve_user("a")
    assert ei.value.kind == "rate_limited"
