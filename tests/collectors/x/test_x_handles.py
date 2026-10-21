"""Spec 09 §9.1 test_x_handles: URL/handle forms → handle or None (case kept); chunk_handles respects 25/512."""
from __future__ import annotations

import pytest

from app.collectors.x_handles import chunk_handles, normalise_x_handle, packed_len

CASES = [
    ("https://x.com/#!/Foo_Bar?s=20", "Foo_Bar"),
    ("twitter.com/intent/follow?screen_name=x", None),
    ("x.com/foo/status/1", "foo"),
    ("@Foo", "Foo"),
    ("Foo", "Foo"),
    ("Foo_Bar_Baz_123", "Foo_Bar_Baz_123"),            # 15 chars
    ("Foo_Bar_Baz_1234", None),                        # 16 chars
    ("x.com/i/flow/login", None),
    ("https://x.com/login", None),
    ("https://twitter.com/Foo/", "Foo"),
    ("x.com/#!/Foo", "Foo"),
    ("https://x.com/Foo#frag", "Foo"),
    ("https://twitter.com/Foo?lang=en", "Foo"),
    ("https://www.x.com/Foo", "Foo"),
    ("https://mobile.twitter.com/Foo/status/1/photo/1", "Foo"),
    ("http://x.com/@Foo", "Foo"),
    ("x.com/status/123", None),
    ("https://x.com/i/status/123", None),
    ("https://x.com/home", None),
    ("x.com/search?q=bittensor", None),
    ("https://x.com/hashtag/tao", None),
    ("x.com/explore", None),
    ("facebook.com/Foo", None),
    ("x.com/foo-bar", None),
    ("", None),
]


def test_case_table_has_25_forms():
    assert len(CASES) == 25


@pytest.mark.parametrize("raw,want", CASES)
def test_normalise(raw, want):
    assert normalise_x_handle(raw) == want


def test_none_and_non_string():
    assert normalise_x_handle(None) is None
    assert normalise_x_handle(123) is None  # type: ignore[arg-type]


def test_chunk_handles_respects_25_and_512():
    handles = [f"h{i:02d}" + "x" * 12 for i in range(30)]          # 30 × 15-char handles
    assert all(len(h) == 15 for h in handles)
    chunks = chunk_handles(handles)
    assert sum(len(c) for c in chunks) == 30
    assert [h for c in chunks for h in c] == handles                # order kept
    for c in chunks:
        assert len(c) <= 25
        q = "(" + " OR ".join(f"from:{h}" for h in c) + ") -filter:retweets since:2026-10-17_00:00:00_UTC lang:en"
        assert len(q) <= 512 and packed_len(c) <= 512
    short = chunk_handles([f"a{i}" for i in range(60)])
    assert all(len(c) <= 25 for c in short) and len(short) == 3


def test_chunk_handles_dedupes_case_insensitively():
    assert chunk_handles(["Foo", "foo", "Bar"]) == [["Foo", "Bar"]]
