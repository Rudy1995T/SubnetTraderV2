"""Commit-spam filter (spec 06 §2.6). Pure and deterministic over stored fields.

Insert time: ``flag_commits`` over the fetch batch sets ``spam_flags`` (``github_commits`` is append-only, so the
column is never updated). Read time: the same function over each window is authoritative, so late commits can still
complete a burst or an identical-message group. Bots and merges are excluded before spam analysis.
"""
from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from typing import Iterable

import app.config as app_config
from app.collectors.github.models import DAY, CommitRow

_WS = re.compile(r"\s+")
_DIGITS = re.compile(r"\d+")
IDENTICAL_MIN = 5
IDENTICAL_WINDOW = 7 * DAY


def normalise_message(msg: str | None) -> str:
    return _WS.sub(" ", _DIGITS.sub("", (msg or "").lower())).strip()


def message_hash(msg: str | None) -> str:
    return hashlib.sha1(normalise_message(msg).encode()).hexdigest()[:16]


def _windows(times: list[int], width: int, min_count: int) -> set[int]:
    """Indices (into the sorted list) covered by any window [t_i, t_i + width] holding ≥ min_count items."""
    out: set[int] = set()
    j = 0
    n = len(times)
    for i in range(n):
        if j < i:
            j = i
        while j + 1 < n and times[j + 1] - times[i] <= width:
            j += 1
        if j - i + 1 >= min_count:
            out.update(range(i, j + 1))
    return out


def flag_commits(commits: Iterable[CommitRow], *, repo_created_at: int | None = None, declared_fork: bool = False,
                 burst_n: int | None = None, burst_sec: int | None = None,
                 min_stats_total: int | None = None) -> dict[str, list[str]]:
    """sha → sorted spam flags for every human (non-bot, non-merge) commit given (flags may be empty)."""
    s = app_config.settings
    burst_n = s.GITHUB_SPAM_BURST_N if burst_n is None else burst_n
    burst_sec = s.GITHUB_SPAM_BURST_SEC if burst_sec is None else burst_sec
    min_stats_total = s.GITHUB_SPAM_MIN_STATS_TOTAL if min_stats_total is None else min_stats_total
    human = [c for c in commits if not c.is_bot and not c.is_merge]
    flags: dict[str, set[str]] = {c.sha: set() for c in human}
    by_author: dict[str, list[CommitRow]] = defaultdict(list)
    for c in human:
        by_author[c.author_key].append(c)
    for rows in by_author.values():
        rows.sort(key=lambda c: (c.event_at, c.sha))
        times = [c.event_at for c in rows]
        # burst: > N commits within burst_sec; the first commit of each burst cluster stays unflagged
        covered = _windows(times, burst_sec, burst_n + 1)
        for i in sorted(covered):
            if i - 1 in covered and times[i] - times[i - 1] <= burst_sec:
                flags[rows[i].sha].add("burst")
        # identical_msg: same normalised message ≥ 5 times within 7 d; first per UTC day stays unflagged
        groups: dict[str, list[int]] = defaultdict(list)
        for i, c in enumerate(rows):
            groups[c.message_norm_hash or message_hash(c.message_head)].append(i)
        for idxs in groups.values():
            gt = [times[i] for i in idxs]
            hit = _windows(gt, IDENTICAL_WINDOW, IDENTICAL_MIN)
            seen_days: set[int] = set()
            for k in sorted(hit):
                day = gt[k] // DAY
                if day in seen_days:
                    flags[rows[idxs[k]].sha].add("identical_msg")
                else:
                    seen_days.add(day)
    for c in human:
        if c.stats_total is not None and c.stats_total < min_stats_total:
            flags[c.sha].add("tiny")
        if c.whitespace_only == 1:
            flags[c.sha].add("whitespace_only")
        if declared_fork and repo_created_at is not None and c.event_at <= repo_created_at:
            flags[c.sha].add("pre_fork")
    return {sha: sorted(f) for sha, f in flags.items()}


def human_unflagged(commits: Iterable[CommitRow], flags: dict[str, list[str]]) -> list[CommitRow]:
    """``C(o)`` members: non-bot, non-merge, no read-time spam flag."""
    return [c for c in commits if not c.is_bot and not c.is_merge and not flags.get(c.sha)]


def needs_stats(flags: list[str]) -> bool:
    """Only new commits already flagged burst/identical_msg get a sampled ``/commits/{sha}`` look-up (§2.5)."""
    return "burst" in flags or "identical_msg" in flags


def whitespace_only_patch(files: list[dict]) -> bool:
    """True when every file's patch changes whitespace only (lines differing only in whitespace)."""
    if not files:
        return False
    for f in files:
        patch = f.get("patch")
        if patch is None:
            return False                       # binary / too large: not provably whitespace-only
        minus: list[str] = []
        plus: list[str] = []
        for line in patch.splitlines():
            if line.startswith(("+++", "---", "@@")):
                continue
            if line.startswith("-"):
                minus.append(_WS.sub("", line[1:]))
            elif line.startswith("+"):
                plus.append(_WS.sub("", line[1:]))
        if [x for x in minus if x] != [x for x in plus if x]:
            return False
    return True


def detect_force_push(*, pushed_at: int | None, max_committer_date: int | None, n_new: int,
                      pushed_advanced: bool) -> bool:
    """Repo-level heuristic: pushed_at runs > 6 h ahead of the newest default-branch commit while the commit page
    returned no new shas although pushed_at advanced (also bumps on other branches — logged only)."""
    if pushed_at is None or max_committer_date is None:
        return False
    return pushed_at - max_committer_date > 6 * 3600 and n_new == 0 and pushed_advanced
