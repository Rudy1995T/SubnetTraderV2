"""Spec 09 §9.1 test_match: ambiguity rule, SN/cashtag/handle matching, mention-query construction and rendering
(≤ 512 chars, provider repost filters), url_canonical."""
from __future__ import annotations

from datetime import datetime, timezone

from app.collectors.x_providers.official_v2 import OfficialV2
from app.collectors.x_providers.twitterapi_io import TwitterApiIo
from app.nlp.match import OccupantIndex, OccupantRef, build_mention_query, match_occupants, url_canonical

IDX = OccupantIndex([
    OccupantRef("64:1", 64, "Chutes", "$SN64", handles=("chutes_ai",)),
    OccupantRef("44:1", 44, "Score"),
    OccupantRef("103:1", 103, "subnet103"),
    OccupantRef("1:1", 1, "Apex", handles=("MacrocosmosAI",)),
    OccupantRef("9:1", 9, "IOTA", handles=("MacrocosmosAI",)),
])


class _S:
    TWITTERAPI_IO_BASE_URL = "https://api.twitterapi.io"
    X_OFFICIAL_BASE_URL = "https://api.x.com/2"
    X_HTTP_TIMEOUT_SEC = 20.0
    X_OFFICIAL_EXCLUDE = "retweets,replies"
    X_PRICES_JSON = ""


def test_ambiguous_name_needs_context():
    assert match_occupants("Score just won the hackathon", IDX) == []
    assert match_occupants("Score on bittensor looks strong", IDX) == [("44:1", "name")]


def test_sn_cashtag_handle():
    assert match_occupants("SN64 is cooking", IDX) == [("64:1", "sn")]
    assert match_occupants("$SN64", IDX) == [("64:1", "cashtag")]
    assert match_occupants("subnet 64 and netuid 44", IDX) == [("64:1", "sn"), ("44:1", "sn")]
    assert match_occupants("gm @chutes_ai", IDX) == [("64:1", "handle")]
    assert {k for k, _ in match_occupants("hi @MacrocosmosAI", IDX)} == {"1:1", "9:1"}       # shared handle fans out
    assert match_occupants("Chutes inference is fast", IDX) == [("64:1", "name")]


def test_restrict_to():
    assert match_occupants("SN9 and SN64 news", IDX, restrict_to={"1:1", "9:1"}) == [("9:1", "sn")]


def test_mention_query_construction():
    q = build_mention_query(IDX.by_key["103:1"])
    assert not any("subnet103" in t for t in q.terms) and q.terms[0] == "SN103"
    q = build_mention_query(IDX.by_key["64:1"])
    assert q.terms == ['"Chutes"', "SN64", '"subnet 64"', "$SN64", "@chutes_ai"] and q.required_coterms == []
    q = build_mention_query(IDX.by_key["44:1"])
    assert q.required_coterms == ["bittensor", "TAO", "subnet", "dTAO"]


def test_rendered_queries_fit_512():
    since = datetime(2026, 9, 21, 4, 0, tzinfo=timezone.utc)
    ta, of = TwitterApiIo(None, _S()), OfficialV2(None, _S())
    for ref in IDX.refs:
        q = build_mention_query(ref)
        a, b = ta.render_query(q, since), of.render_query(q, since)
        assert len(a) <= 512 and len(b) <= 512
        assert "-filter:retweets" in a and "since:2026-09-21_04:00:00_UTC" in a
        assert "-is:retweet" in b and "since:" not in b
    assert ta.render_query(build_mention_query(IDX.by_key["64:1"]), since) == \
        '("Chutes" OR SN64 OR "subnet 64" OR $SN64 OR @chutes_ai) -filter:retweets since:2026-09-21_04:00:00_UTC'


def test_url_canonical():
    assert url_canonical("HTTPS://Example.COM/Path/To?utm_source=x&utm_medium=y&id=3#frag") == \
        "https://example.com/Path/To?id=3"
    assert url_canonical("https://a.io/x/?ref=tw&fbclid=1&gclid=2&mc_cid=3") == "https://a.io/x"
