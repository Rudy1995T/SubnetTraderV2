"""Spec 08 §9 #37 — the shared normaliser/hash (02 §B11 golden pairs, imported by 09/10)."""
from app.nlp.textnorm import text_hash, text_norm


def test_golden_pairs():
    assert text_norm("Ｌaunch  https://x.co/a @Team\nNOW") == "launch now"     # NFKC folds the fullwidth Ｌ
    assert text_norm("https://only.example/path") == ""
    assert text_norm(None) == ""
    assert text_hash("A  b") == text_hash("a b")
    assert text_hash("x") != text_hash("y")


def test_order_urls_before_mentions():
    # the URL is removed whole (an @ inside it never survives as a fragment)
    assert text_norm("see https://x.co/@team/post now") == "see now"
    assert text_norm("Hi @team, ship it") == "hi , ship it"
