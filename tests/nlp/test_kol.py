"""Spec 09 §9.1 test_kol: §2.7 multipliers and flags, team/KOL account levels, team posts never bot_filtered, and the
§2.8 water-fill ``cap_author_share``."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.collectors.x_providers.base import XPost, XUser
from app.nlp.kol import KolRow, author_credibility, cap_author_share, is_bot_filtered

T = datetime(2026, 10, 17, 12, 0, tzinfo=timezone.utc)


def u(age_days: float = 1000, followers: int = 5000, following: int = 300, posts: int | None = None,
      handle: str = "someone") -> XUser:
    return XUser("1", handle, handle, followers, following, posts if posts is not None else int(age_days * 2),
                 T - timedelta(days=age_days), None)


def p(text: str = "a normal sentence about bittensor subnets", *, pid: str = "100", author: str = "1",
      at: datetime = T, has_url: bool = False) -> XPost:
    return XPost(pid, author, "someone", text, "en", at, 0, 0, 0, 0, 0, has_url, False, None, False, None, None)


def cred(user, post=None, batch=(), db_dupes=0, kol=None, is_team=False, **kw):
    post = post or p()
    return author_credibility(user, post, list(batch) or [post], db_dupes, kol, is_team, **kw)


@pytest.mark.parametrize("age,mult,flag", [(5, 0.3, "young"), (90, 0.7, "newish"), (3 * 365, 1.0, None)])
def test_account_age(age, mult, flag):
    c, flags = cred(u(age_days=age))
    assert c == pytest.approx(mult)
    assert (flag in flags) if flag else not ({"young", "newish"} & set(flags))


def test_follower_ratio_and_tiny():
    c, f = cred(u(followers=100, following=2000))
    assert c == pytest.approx(0.5) and f == ["follow_ratio"]
    c, f = cred(u(followers=30, following=10))
    assert c == pytest.approx(0.6) and f == ["tiny"]


def test_velocity():
    c, f = cred(u(age_days=1000, posts=150 * 1000))
    assert c == pytest.approx(0.2) and f == ["firehose"]
    c, f = cred(u(age_days=1000, posts=50 * 1000))
    assert c == pytest.approx(0.6) and f == ["fast"]


def test_autohandle_unprofiled_link_only():
    c, f = cred(u(handle="taomoon12345"))
    assert c == pytest.approx(0.7) and "autohandle" in f
    c, f = cred(None)
    assert c == pytest.approx(0.7) and f == ["unprofiled"]
    c, f = cred(u(), p("https://x.co/abc wow", has_url=True))
    assert c == pytest.approx(0.5) and "link_only" in f


def test_duplicates_self_and_coordinated():
    same = "buy this subnet token now before everyone else"
    batch = [p(same, pid=str(200 + i), author="1", at=T + timedelta(minutes=2 * i)) for i in range(3)]
    c, f = cred(u(), batch[0], batch, window_s=600)
    assert c == pytest.approx(0.3) and "dup_self" in f
    pair = [p(same, pid="300", author="A"), p(same, pid="301", author="B", at=T + timedelta(minutes=3))]
    for x in pair:
        c, f = cred(u(), x, pair, window_s=600)
        assert c == pytest.approx(0.3) and "dup_coord" in f and "dup_self" not in f
    far = [p(same, pid="400", author="A"), p(same, pid="401", author="B", at=T + timedelta(minutes=30))]
    assert cred(u(), far[0], far, window_s=600)[0] == pytest.approx(1.0)
    c, f = cred(u(), p(same), db_dupes=1, window_s=600)                       # DB side of the window
    assert "dup_self" in f
    c, f = cred(u(), p(same), db_coord=1, window_s=600)
    assert "dup_coord" in f


def test_promo():
    c, f = cred(u(), p("GIVEAWAY time, RT to win"))
    assert c == pytest.approx(0.2) and "promo" in f


def test_kol_overrides_account_heuristics():
    kol = KolRow("QuasarModels", "quasarmodels", 0.3, True, True, "founder")
    c, f = cred(u(age_days=5, followers=10, following=5000), kol=kol)
    assert c == pytest.approx(0.3) and f == ["kol"]
    c, f = cred(u(), p("airdrop for holders"), kol=kol)                       # post-level still applies
    assert c == pytest.approx(0.06) and "promo" in f


def test_team_level_one_and_never_bot_filtered():
    c, f = cred(u(age_days=5), is_team=True)
    assert c == pytest.approx(1.0) and f == ["team"]
    c, f = cred(u(), p("GIVEAWAY 🎁 for our miners"), is_team=True)
    assert c == pytest.approx(0.2) and "promo" in f
    assert is_bot_filtered(c, is_team=True, threshold=0.3) is False
    assert is_bot_filtered(c, is_team=False, threshold=0.3) is True


def _shares(w):
    s = sum(w.values())
    return {k: v / s for k, v in w.items()}


def test_cap_author_share_water_fill():
    out = cap_author_share({"a": 60, "b": 10, "c": 10, "d": 10, "e": 10})
    assert out["a"] == pytest.approx(25) and all(out[k] == pytest.approx(18.75) for k in "bcde")
    out = cap_author_share({"a": 50, "b": 40, "c": 5, "d": 5})
    assert all(v == pytest.approx(25) for v in out.values())
    out = cap_author_share({"a": 90, "b": 5, "c": 5})
    assert all(v == pytest.approx(100 / 3) for v in out.values())
    out = cap_author_share({"a": 90, "b": 10})
    assert out == {"a": pytest.approx(50), "b": pytest.approx(50)}
    assert cap_author_share({}) == {}
    assert cap_author_share({"a": 0.0, "b": 3.0}) == {"a": 0.0, "b": pytest.approx(3.0)}


@pytest.mark.parametrize("w", [{"a": 60, "b": 10, "c": 10, "d": 10, "e": 10}, {"a": 50, "b": 40, "c": 5, "d": 5},
                               {"a": 90, "b": 5, "c": 5}, {"x": 1, "y": 2, "z": 3, "q": 100, "r": 0.5, "s": 7}])
def test_cap_author_share_invariants(w):
    out = cap_author_share(w)
    n = sum(1 for v in w.values() if v > 0)
    assert sum(out.values()) == pytest.approx(sum(w.values()), abs=1e-9)
    assert all(s <= max(0.25, 1 / n) + 1e-9 for s in _shares(out).values())
