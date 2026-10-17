"""Spec 06 test fixtures: a route-table GitHub client (no network), payload builders, registry seeding."""
from __future__ import annotations

import json
from typing import Any, Callable

import pytest

from app.collectors.github.client import GHResponse, GitHubClient, GitHubQuota
from app.util.time import parse_iso, to_iso

NOW = parse_iso("2026-10-17T12:00:00Z")
DAY = 86400


def R(status: int = 200, body: Any = None, headers: dict | None = None, location: str | None = None,
      from_cache: bool = False) -> GHResponse:
    return GHResponse(status, body, {k.lower(): v for k, v in (headers or {}).items()}, from_cache=from_cache,
                      location=location)


def repo(rid: int, full_name: str, *, pushed: int | None = None, fork: bool = False, archived: bool = False,
         owner_type: str = "Organization", description: str | None = None, created: int | None = None,
         parent: str | None = None, stars: int = 1, default_branch: str = "main") -> dict:
    owner = full_name.split("/")[0]
    p = {"id": rid, "full_name": full_name, "name": full_name.split("/")[1], "owner": {"login": owner, "type": owner_type},
         "fork": fork, "archived": archived, "disabled": False, "visibility": "public",
         "pushed_at": to_iso(pushed if pushed is not None else NOW - DAY), "created_at": to_iso(created or NOW - 400 * DAY),
         "updated_at": to_iso(NOW - DAY), "default_branch": default_branch, "stargazers_count": stars,
         "forks_count": 0, "open_issues_count": 0, "size": 10, "description": description, "topics": []}
    if parent:
        p["parent"] = {"full_name": parent}
    return p


class FakeClient(GitHubClient):
    """``GitHubClient`` whose ``get`` answers from a route table: path → response | [responses] | fn(params)."""

    def __init__(self, routes: dict[str, Any] | None = None, *, db=None, now: float = NOW) -> None:
        self.t = float(now)
        super().__init__(http=None, quota=GitHubQuota(clock=lambda: self.t), db=db,
                         clock=lambda: self.t, sleep=self._sleep)
        self.routes: dict[str, Any] = dict(routes or {})
        self.calls: list[tuple[str, dict | None]] = []
        self.token_state = "ok"

    async def _sleep(self, s: float) -> None:
        self.t += s

    async def get(self, path: str, *, params: dict | None = None, etag_key: str | None = None, job: str = "",
                  reserve: int | None = None, deadline: float | None = None, accept: str = "") -> GHResponse:
        self.calls.append((path, params))
        self._count(job, "calls")
        key = path
        if params and "page" in params and f"{path}#page{params['page']}" in self.routes:
            key = f"{path}#page{params['page']}"
        r = self.routes.get(key)
        if r is None:
            return R(404, {"message": "Not Found"})
        if callable(r) and not isinstance(r, GHResponse):
            r = r(params)
        if isinstance(r, list):
            r = r.pop(0) if len(r) > 1 else r[0]
        if isinstance(r, Exception):
            self._count(job, "failed")
            raise r
        return r

    def paths(self) -> list[str]:
        return [p for p, _ in self.calls]


@pytest.fixture
def fake():
    def _make(routes: dict[str, Any] | None = None, **kw) -> FakeClient:
        return FakeClient(routes, **kw)
    return _make


def cand(source: str, value: str, *, field: str = "github_repo", junk: bool = False, demoted: bool = False,
         suppressed: bool = False, observed_at: int = NOW - DAY, override_id: int | None = None) -> dict:
    return {"source": source, "field": field, "value": value, "value_norm": value, "ref_kind": "github_repo",
            "hash": "h", "is_junk": junk, "junk_reason": "placeholder_repo" if junk else None, "demoted": demoted,
            "suppressed": suppressed, "observed_at": observed_at, "override_id": override_id}


async def seed_occupant(db, netuid: int, *, name: str | None = None, cands: list[dict] | None = None,
                        status: str = "present", reg: int = 1000, occ_status: str = "active",
                        winner: dict | None = None) -> str:
    okey = f"{netuid}:{reg}"
    await db.execute("INSERT OR IGNORE INTO occupants (occupant_key, netuid, network_registered_at, status, name, "
                     "first_seen_block, first_seen_at, last_seen_block, last_seen_at) VALUES (?,?,?,?,?,?,?,?,?)",
                     (okey, netuid, reg, occ_status, name or f"Subnet{netuid}", 1, NOW - 500 * DAY, 2, NOW))
    await db.execute("INSERT OR REPLACE INTO subnets (netuid, occupant_key, network_registered_at, name, status, "
                     "updated_at) VALUES (?,?,?,?,?,?)", (netuid, okey, reg, name or f"Subnet{netuid}", occ_status, NOW))
    cands = cands or []
    w = winner or next((c for c in cands if not c["is_junk"] and not c["suppressed"]), None)
    await db.execute("INSERT OR REPLACE INTO identity_resolved (occupant_key, field, value, value_norm, ref_kind, "
                     "source, source_observed_at, status, candidates_json, resolved_at, run_id) VALUES "
                     "(?,?,?,?,?,?,?,?,?,?,?)",
                     (okey, "github_repo", w["value"] if w and status == "present" else None,
                      w["value_norm"] if w and status == "present" else None, "github_repo" if w else None,
                      w["source"] if w and status == "present" else None, NOW - DAY, status, json.dumps(cands),
                      NOW - DAY, "seed"))
    return okey


async def metric_rows(db, okey: str | None = None) -> dict[str, dict]:
    q = "SELECT * FROM metric_values WHERE source = 'github'" + (" AND occupant_key = ?" if okey else "")
    rows = await db.fetchall(q, (okey,) if okey else ())
    return {r["metric_key"]: {**r, "vj": json.loads(r["value_json"])} for r in rows}


Fn = Callable[..., Any]
