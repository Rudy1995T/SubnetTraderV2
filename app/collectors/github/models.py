"""Row types shared by the GitHub collector modules (spec 06 §8)."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from app.util.time import parse_iso

SPEC_VERSION = "ts-1.0"
DAY = 86400


def iso_ts(s: str | None) -> int | None:
    if not s:
        return None
    try:
        return parse_iso(s)
    except (TypeError, ValueError):
        return None


@dataclass(slots=True)
class RepoRow:
    repo_id: int
    full_name: str
    owner: str
    name: str
    is_fork: bool = False
    archived: bool = False
    pushed_at: int | None = None
    created_at: int | None = None
    stars: int | None = None
    description: str | None = None
    default_branch: str | None = None
    parent_full_name: str | None = None
    owner_type: str | None = None
    status: str = "ok"

    @classmethod
    def from_db(cls, r: dict) -> "RepoRow":
        return cls(int(r["repo_id"]), r["full_name"], r["owner"], r["name"], bool(r.get("is_fork")),
                   bool(r.get("archived")), r.get("pushed_at"), r.get("created_at"), r.get("stars"),
                   r.get("description"), r.get("default_branch"), r.get("parent_full_name"), r.get("owner_type"),
                   r.get("status") or "ok")


def repo_fields(p: dict) -> dict[str, Any]:
    """GitHub repo payload → 03 ``github_repos`` columns (INTEGER times, lower-cased ``full_name``/owner)."""
    owner = p.get("owner") or {}
    lic = p.get("license") or {}
    parent = p.get("parent") or {}
    return {
        "repo_id": int(p["id"]),
        "full_name": str(p.get("full_name") or "").lower(),
        "owner": str(owner.get("login") or str(p.get("full_name") or "/").split("/")[0]).lower(),
        "name": str(p.get("name") or ""),
        "owner_type": owner.get("type") if owner.get("type") in ("Organization", "User") else None,
        "is_fork": 1 if p.get("fork") else 0,
        "parent_full_name": (parent.get("full_name") or "").lower() or None,
        "archived": 1 if p.get("archived") else 0,
        "disabled": 1 if p.get("disabled") else 0,
        "visibility": p.get("visibility"),
        "pushed_at": iso_ts(p.get("pushed_at")),
        "created_at": iso_ts(p.get("created_at")),
        "updated_at": iso_ts(p.get("updated_at")),
        "default_branch": p.get("default_branch"),
        "stars": p.get("stargazers_count"),
        "forks": p.get("forks_count"),
        "open_issues": p.get("open_issues_count"),
        "subscribers": p.get("subscribers_count"),
        "license": lic.get("spdx_id"),
        "size_kb": p.get("size"),
        "description": (p.get("description") or None),
        "topics_json": json.dumps(p.get("topics") or []),
    }


@dataclass(slots=True)
class CommitRow:
    sha: str
    repo_id: int
    author_key: str
    event_at: int
    n_parents: int = 1
    author_login: str | None = None
    author_name: str | None = None
    author_email_hash: str | None = None
    author_type: str | None = None
    committer_date: int | None = None
    message_head: str | None = None
    message_norm_hash: str | None = None
    verified: int | None = None
    is_bot: bool = False
    is_merge: bool = False
    spam_flags: list[str] = field(default_factory=list)
    stats_total: int | None = None
    whitespace_only: int | None = None
    backfilled: int = 0

    @classmethod
    def from_db(cls, r: dict) -> "CommitRow":
        return cls(r["sha"], int(r["repo_id"]), r["author_key"], int(r["event_at"]), int(r["n_parents"]),
                   r.get("author_login"), r.get("author_name"), r.get("author_email_hash"), r.get("author_type"),
                   r.get("committer_date"), r.get("message_head"), r.get("message_norm_hash"), r.get("verified"),
                   bool(r.get("is_bot")), bool(r.get("is_merge")), json.loads(r.get("spam_flags") or "[]"),
                   r.get("stats_total"), r.get("whitespace_only"), int(r.get("backfilled") or 0))

    def db_row(self, *, observed_at: int, run_id: str, source: str = "github_api") -> dict[str, Any]:
        return {"sha": self.sha, "repo_id": self.repo_id, "author_key": self.author_key,
                "author_login": self.author_login, "author_name": self.author_name,
                "author_email_hash": self.author_email_hash, "author_type": self.author_type,
                "event_at": self.event_at, "committer_date": self.committer_date, "n_parents": self.n_parents,
                "message_head": self.message_head, "message_norm_hash": self.message_norm_hash,
                "verified": self.verified, "is_bot": 1 if self.is_bot else 0, "is_merge": 1 if self.is_merge else 0,
                "spam_flags": json.dumps(sorted(self.spam_flags)), "stats_total": self.stats_total,
                "whitespace_only": self.whitespace_only, "observed_at": int(observed_at), "source": source,
                "run_id": run_id, "backfilled": self.backfilled}


@dataclass(slots=True)
class MetricObservation:
    metric_key: str
    value: float | None
    status: str                 # ok | absent | failed | not_applicable
    reason: str | None = None
    detail: dict = field(default_factory=dict)

    def value_json(self) -> dict:
        vj: dict[str, Any] = {"status": self.status, **self.detail}
        if self.status != "ok" or self.reason:          # ok rows may carry e.g. reason='gone_grace' (§4)
            vj["reason"] = self.reason or "unspecified"
        return vj
