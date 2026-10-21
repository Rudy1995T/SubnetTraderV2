"""Spec 07 §2.4–§2.5: status table, heuristics, outcomes, registrable domain, host exclusion."""
from __future__ import annotations

import pytest

from app.collectors.web import classify as C
from app.collectors.web import content as T
from app.collectors.web.fetch import FetchResult, Hop
from tests.collectors.web.conftest import fixture, web_settings

HEUR = C.WebHeuristicsConfig.from_settings(web_settings())


def fr(status=200, hops=0, exc=None, final_host="site.example") -> FetchResult:
    f = FetchResult(url="https://site.example", status=status, exc_kind=exc, final_host=final_host)
    f.chain = [Hop(f"https://h{i}.example", 301, "x") for i in range(hops)]
    return f


def cls(f, **kw):
    base = dict(parked=False, hijack_score=0, bot_challenge=False, head_only=False)
    base.update(kw)
    return C.classify(f, **base)


@pytest.mark.parametrize("exc,expected", [("dns", "dns_fail"), ("timeout", "timeout"), ("tls", "tls_fail"),
                                          ("conn", "conn_error"), ("protocol", "conn_error"),
                                          ("internal", "conn_error"), ("too_many_redirects", "too_many_redirects")])
def test_exception_classes(exc, expected):
    assert cls(fr(status=None, exc=exc)) == expected


def test_status_classes():
    assert cls(fr(200)) == "up_2xx"
    assert cls(fr(200, hops=1)) == "redirect_ok"
    assert cls(fr(404)) == "client_error"
    assert cls(fr(502)) == "server_error"
    assert cls(fr(301)) == "server_error"            # 3xx without a followable Location
    assert cls(fr(429)) == "bot_challenge"
    assert cls(fr(403), bot_challenge=True) == "bot_challenge"
    assert cls(fr(403)) == "client_error"
    assert cls(fr(405), head_only=True) == "bot_challenge"
    assert cls(fr(200), parked=True) == "parked"
    assert cls(fr(200), hijack_score=4, parked=True) == "hijacked_suspect"


def test_bot_challenge_detection():
    html = fixture("cf_challenge.html")
    assert C.is_bot_challenge(403, {"cf-mitigated": "challenge"}, html)
    assert C.is_bot_challenge(503, {"server": "cloudflare"}, html)
    assert C.is_bot_challenge(429, {"x-sucuri-id": "1"}, "")
    assert not C.is_bot_challenge(200, {"cf-mitigated": "challenge"}, html)
    assert not C.is_bot_challenge(403, {"server": "nginx"}, "<h1>Forbidden</h1>")
    a8c = ('<!DOCTYPE html><title>Checking your browser...</title><noscript><p>Javascript required</p></noscript>'
           '<script src="/_s/challenge.js"></script>')
    assert C.is_bot_challenge(403, {"server": "nginx", "server-timing": "a8c-cdn"}, a8c)     # taos.im, live


def _parked(name: str, final_host: str = "beamnet.example") -> tuple[list[str], int]:
    html = fixture(name)
    info = T.page_info(html, f"https://{final_host}/")
    text, _ = T.extract_text(html, f"https://{final_host}/")
    norm = T.normalise(T.with_head(text, info.title, info.description))
    plain = T.plain_text(html)
    spa = C.detect_spa(html, len(plain))
    sig = C.parked_signals(norm, info.title, final_host, [u for u, _ in info.links], info.asset_hosts, spa,
                           len(norm), HEUR)
    return sig, C.score(sig, C.PARKED_WEIGHTS)


def test_parked_fixtures():
    sig, sc = _parked("parked_sedo.html")
    assert "strong_phrase" in sig and "parking_script" in sig and sc >= 3
    sig, sc = _parked("parked_godaddy.html", "parked-content.godaddy.com")
    assert "parking_host" in sig and sc >= 3
    sig, sc = _parked("tiny_links.html")
    assert "tiny_body_links" in sig and "weak_keywords_2" in sig and sc >= 3


def test_spa_shell_not_parked():
    html = fixture("spa_shell.html")
    assert C.detect_spa(html, len(T.plain_text(html)))
    sig, sc = _parked("spa_shell.html", "nodexo.example")
    assert "tiny_body_links" not in sig and sc < 3


def test_landing_ok_not_parked():
    sig, sc = _parked("landing_ok.html", "apex.example")
    assert sc < 3


def test_default_server_page_is_parked():
    norm = T.normalise("Welcome to nginx! If you see this page, the nginx web server is successfully installed.")
    sig = C.parked_signals(norm, "Welcome to nginx!", "x.example", [], [], False, len(norm), HEUR)
    assert "strong_phrase" in sig


def test_hijack_signals_new_keywords_only():
    casino = T.normalise(T.extract_text(fixture("hijacked_casino.html"), "https://casino-win.example/")[0])
    sig = C.hijack_signals(casino, "a normal page about inference", cross_domain=True, final_is_parking=False,
                           prev_same_domain_up=True, change_ratio=0.05, resolved_ip="1.1.1.1", tls_issuer="A",
                           prev_ip="2.2.2.2", prev_issuer="B", cfg=HEUR)
    assert C.score(sig, C.HIJACK_WEIGHTS) >= 4 and "cross_domain_new" in sig and "scam_keywords_2" in sig
    # a prediction-market site that always talked about betting: keywords are not new
    sig = C.hijack_signals(casino, casino, cross_domain=False, final_is_parking=False, prev_same_domain_up=True,
                           change_ratio=0.9, resolved_ip=None, tls_issuer=None, prev_ip=None, prev_issuer=None,
                           cfg=HEUR)
    assert sig == []
    # a plain rebrand redirect alone is not a hijack
    sig = C.hijack_signals("a normal page", "a normal page", cross_domain=True, final_is_parking=False,
                           prev_same_domain_up=True, change_ratio=1.0, resolved_ip=None, tls_issuer=None,
                           prev_ip=None, prev_issuer=None, cfg=HEUR)
    assert C.score(sig, C.HIJACK_WEIGHTS) == 2


def test_outcomes_indeterminate_rule():
    def r(sc, err=None):
        return {"status_class": sc, "error": err}
    assert C.outcomes([r("up_2xx"), r("timeout")]) == ["up", "indeterminate"]
    assert C.outcomes([r("up_2xx"), r("timeout"), r("timeout")]) == ["up", "indeterminate", "down"]
    assert C.outcomes([r("timeout")]) == ["indeterminate"]
    assert C.outcomes([r("client_error"), r("conn_error")]) == ["down", "down"]
    # bot_challenge and internal rows are skipped for "previous"
    assert C.outcomes([r("up_2xx"), r("bot_challenge"), r("timeout")]) == ["up", "indeterminate", "indeterminate"]
    assert C.outcomes([r("server_error"), r("conn_error", "internal:ValueError: x")]) == ["down", "indeterminate"]
    assert C.outcomes([r("timeout", "network_suspect"), r("parked")]) == ["indeterminate", "down"]


def test_registrable_domain_and_exclusion():
    assert C.registrable_domain("x.vercel.app") == "vercel.app"
    assert C.registrable_domain("www.macrocosmos.ai") == "macrocosmos.ai"
    assert C.registrable_domain("a.b.co.uk") == "b.co.uk"
    assert C.registrable_domain("1.2.3.4") == "3.4"
    ex = web_settings().WEB_EXCLUDED_HOSTS
    assert C.is_excluded_host("docs.github.com", ex) and C.is_excluded_host("www.x.com", ex)
    assert C.is_excluded_host("vimeo.com", ex) and C.is_excluded_host("huggingface.co", ex)
    assert not C.is_excluded_host("proj.github.io", ex)
