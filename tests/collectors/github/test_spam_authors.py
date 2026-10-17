"""Spec 06 §2.6 / §9: commit-spam filter and author identity (pure + identity table)."""
import pytest

from app.collectors.github import authors, spam
from app.collectors.github.activity import parse_commit
from app.collectors.github.models import CommitRow
from tests.collectors.github.conftest import DAY, NOW


def c(i, t, author="a", msg="work", bot=False, parents=1, stats=None, ws=None):
    return CommitRow(f"s{i}", 1, author, int(t), parents, author, None, None, None, int(t), msg,
                     spam.message_hash(msg), None, bot, parents >= 2, [], stats, ws)


def test_burst_15_in_40s_flags_14():
    cs = [c(i, NOW + i * 2.8, msg=f"m{i}x{chr(97 + i)}") for i in range(15)]
    f = spam.flag_commits(cs, burst_n=10, burst_sec=60, min_stats_total=5)
    flagged = [s for s, fl in f.items() if "burst" in fl]
    assert len(flagged) == 14 and "s0" not in flagged


def test_no_burst_at_threshold():
    cs = [c(i, NOW + i * 5, msg=f"m{chr(97 + i)}") for i in range(10)]
    assert not any(spam.flag_commits(cs, burst_n=10, burst_sec=60, min_stats_total=5).values())


def test_identical_messages_first_per_day_kept():
    times = [NOW, NOW + 60, NOW + DAY, NOW + DAY + 60, NOW + 2 * DAY, NOW + 2 * DAY + 60]
    cs = [c(i, t, msg="Update README 12") for i, t in enumerate(times)]
    f = spam.flag_commits(cs, burst_n=10, burst_sec=60, min_stats_total=5)
    assert sorted(s for s, fl in f.items() if "identical_msg" in fl) == ["s1", "s3", "s5"]


def test_identical_below_five_not_flagged_and_digits_stripped():
    cs = [c(i, NOW + i * DAY, msg=f"bump v{i}") for i in range(4)]
    assert not any(spam.flag_commits(cs, burst_n=10, burst_sec=60, min_stats_total=5).values())
    assert spam.normalise_message("Bump  V1.2.3\n") == "bump v.."


def test_tiny_whitespace_prefork_and_bots_merges_skipped():
    cs = [c(1, NOW, stats=2), c(2, NOW + 99, stats=50, ws=1), c(3, NOW - 10 * DAY), c(4, NOW, bot=True),
          c(5, NOW, parents=2)]
    f = spam.flag_commits(cs, repo_created_at=NOW - 5 * DAY, declared_fork=True, burst_n=10, burst_sec=60,
                          min_stats_total=5)
    assert f["s1"] == ["tiny"] and f["s2"] == ["whitespace_only"] and f["s3"] == ["pre_fork"]
    assert "s4" not in f and "s5" not in f
    kept = spam.human_unflagged(cs, f)
    assert kept == []


def test_read_time_flags_complete_a_burst_across_batches():
    first = [c(i, NOW + i, msg=f"x{chr(97 + i)}") for i in range(6)]
    later = [c(10 + i, NOW + 6 + i, msg=f"y{chr(97 + i)}") for i in range(6)]
    assert not any(spam.flag_commits(first, burst_n=10, burst_sec=60, min_stats_total=5).values())
    both = spam.flag_commits(first + later, burst_n=10, burst_sec=60, min_stats_total=5)
    assert sum(1 for fl in both.values() if "burst" in fl) == 11


@pytest.mark.parametrize("kw,expected", [
    (dict(login="dependabot[bot]"), True), (dict(login="renovate"), True), (dict(login="x", author_type="Bot"), True),
    (dict(login=None, email="12345+renovate[bot]@users.noreply.github.com"), True),
    (dict(login=None, name="github-actions[bot]"), True), (dict(login="alice"), False),
    (dict(login="Copilot-SWE-Agent"), True),
])
def test_is_bot(kw, expected):
    assert authors.is_bot(**kw) is expected


def test_bot_logins_extra(settings_override):
    settings_override(GITHUB_BOT_LOGINS_EXTRA="mybot, other")
    assert authors.is_bot("mybot") and authors.is_bot("other[bot]")


def test_author_key_rules():
    assert authors.author_key("Alice", "a@x.io") == "alice"
    assert authors.author_key(None, "123+bob@users.noreply.github.com") == "bob"
    assert authors.author_key(None, "carol@users.noreply.github.com") == "carol"
    k = authors.author_key(None, " Dave@Example.com ")
    assert k.startswith("email:") and len(k) == 6 + 16 and k == authors.author_key(None, "dave@example.com")
    assert authors.author_key(None, None) == "unknown"
    assert authors.email_hash("A@B.c") == authors.email_hash("a@b.c") and len(authors.email_hash("a@b.c")) == 64
    assert authors.alias_for("Dave", "dave@example.com") == (k, "dave")
    assert authors.alias_for("Bob", "1+bob@users.noreply.github.com") is None


@pytest.mark.asyncio
async def test_identity_table_maps_email_keys(db):
    k = authors.email_key("dave@example.com")
    await authors.record_identities(db, [(k, "dave")], NOW)
    m = await authors.resolve_author_keys(db, [k, "alice"])
    assert m == {k: "dave"}


def test_parse_commit_never_keeps_raw_email():
    item = {"sha": "abc", "commit": {"author": {"name": "Dev", "email": "dev@corp.io", "date": "2026-10-10T10:00:00Z"},
                                     "committer": {"date": "2026-10-10T11:00:00Z"}, "message": "Fix bug\n\nlong",
                                     "verification": {"verified": True}},
            "author": {"login": "DevGuy", "type": "User"}, "parents": [{"sha": "p1"}, {"sha": "p2"}]}
    row = parse_commit(item, 9)
    d = row.db_row(observed_at=NOW, run_id="r")
    assert "dev@corp.io" not in str(d) and d["author_email_hash"] == authors.email_hash("dev@corp.io")
    assert d["author_key"] == "devguy" and d["is_merge"] == 1 and d["message_head"] == "Fix bug" and d["verified"] == 1
    assert isinstance(d["event_at"], int) and d["committer_date"] - d["event_at"] == 3600


def test_whitespace_only_patch():
    assert spam.whitespace_only_patch([{"patch": "@@ -1 +1 @@\n-a  b\n+a b"}])
    assert not spam.whitespace_only_patch([{"patch": "@@ -1 +1 @@\n-a\n+b"}])
    assert not spam.whitespace_only_patch([{"filename": "bin"}]) and not spam.whitespace_only_patch([])


def test_force_push_heuristic():
    assert spam.detect_force_push(pushed_at=NOW, max_committer_date=NOW - 7 * 3600, n_new=0, pushed_advanced=True)
    assert not spam.detect_force_push(pushed_at=NOW, max_committer_date=NOW - 7 * 3600, n_new=1, pushed_advanced=True)
    assert not spam.detect_force_push(pushed_at=NOW, max_committer_date=NOW - 3600, n_new=0, pushed_advanced=True)
    assert not spam.detect_force_push(pushed_at=None, max_committer_date=NOW, n_new=0, pushed_advanced=True)
