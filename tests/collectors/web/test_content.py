"""Spec 07 §2.3 steps 6–8, 10: extraction chain, normalisation, hash, diff, docs discovery."""
from __future__ import annotations

import pytest

from app.collectors.web import content as T
from tests.collectors.web.conftest import fixture


@pytest.mark.parametrize("raw,gone", [
    ("released 2026-09-22T10:00:00z today", "2026"),
    ("posted September 22, 2026 by the team", "september"),
    ("on 22 sep 2026 we shipped", "sep"),
    ("updated 3 hours ago", "ago"),
    ("© 2026 apex labs", "2026"),
    ("copyright 2024-2026 apex", "2024"),
    ("served 123456789 requests", "123456789"),
    ("nonce deadbeefcafebabe0123456789abcdef01234567 end", "deadbeef"),
    ("id 3f2b8c1e-9a4d-4e2f-8b7a-1c2d3e4f5a6b end", "3f2b8c1e"),
    ("at 10:15 am sharp", "10:15"),
])
def test_normalise_removes_volatile(raw, gone):
    n = T.normalise(raw)
    assert gone not in n
    assert "  " not in n


def test_same_page_different_nonces_hash_identically():
    a = fixture("landing_ok.html").replace("10:15 am", "11:42 pm")
    b = fixture("landing_ok.html").replace("September 22, 2026", "October 1, 2026") + "<!-- nonce 0123456789abcdef0123 -->"
    ta, _ = T.extract_text(a, "https://apex.example/")
    tb, _ = T.extract_text(b, "https://apex.example/")
    assert T.content_hash(T.normalise(ta)) == T.content_hash(T.normalise(tb))


def test_copyright_year_rollover_does_not_change_hash():
    assert T.normalise("hello world © 2026") == T.normalise("hello world © 2027")


def test_change_ratio():
    base = " ".join(f"word{i}" for i in range(400))
    assert T.change_ratio(base, base) == 1.0
    other = " ".join(f"other{i}" for i in range(400))
    assert T.change_ratio(base, other) < 0.2
    edited = base.replace("word10 word11 word12 word13 word14", "fresh text for a new paragraph here")
    assert 0.9 <= T.change_ratio(base, edited) < 1.0


def test_extract_trafilatura_then_meta_prefix():
    html = fixture("landing_ok.html")
    text, ext = T.extract_text(html, "https://apex.example/")
    assert ext == "trafilatura" and "decentralised" in text.lower()
    info = T.page_info(html, "https://apex.example/")
    assert info.title.startswith("Apex Labs")
    assert "bittensor subnet" in (info.description or "").lower()


def test_extract_fallback_chain(monkeypatch):
    import trafilatura
    monkeypatch.setattr(trafilatura, "extract", lambda *a, **k: None)
    text, ext = T.extract_text(fixture("landing_ok.html"), "https://apex.example/")
    assert ext in ("readability", "plain") and len(text) > 200
    monkeypatch.setattr(T, "_readability", lambda html: "")
    text, ext = T.extract_text(fixture("landing_ok.html"), "https://apex.example/")
    assert ext == "plain"


def test_spa_shell_hashes_title_and_description():
    html = fixture("spa_shell.html")
    info = T.page_info(html, "https://nodexo.example/")
    text, ext = T.extract_text(html, "https://nodexo.example/")
    norm = T.normalise(T.with_head(text, info.title, info.description))
    assert "nodexo compute marketplace" in norm
    assert T.content_hash(norm)


def test_find_docs_candidate():
    html = fixture("landing_docs_link.html")
    info = T.page_info(html, "https://beam.example/")
    c = T.find_docs_candidate(info.links, landing_urls=["https://beam.example"], skip_urls=[],
                              excluded_hosts=["github.com"])
    assert c == "https://beam.example/docs/getting-started"     # not #docs, not docs.bittensor.com, not mailto


def test_find_docs_candidate_skips_own_websites_and_excluded():
    links = [("https://beam.example/", "Docs home"), ("https://github.com/beam/docs", "docs"),
             ("https://beam.example/developers", "Build")]
    c = T.find_docs_candidate(links, landing_urls=["https://beam.example"], skip_urls=[],
                              excluded_hosts=["github.com"])
    assert c == "https://beam.example/developers"
    assert T.find_docs_candidate([("https://docs.lium.io/", "")], landing_urls=["https://lium.io"],
                                 skip_urls=["https://docs.lium.io"], excluded_hosts=[]) is None


def test_outbound_links_absolute():
    links = T.outbound_links('<a href="/a">x</a><a href="mailto:x@y.z">m</a><a href="https://o.example/b">o</a>',
                             "https://s.example/p/")
    assert links == ["https://s.example/a", "https://o.example/b"]
