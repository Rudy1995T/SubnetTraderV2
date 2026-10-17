"""Org / user enumeration, primary-repo selection and activity scope ``R(o)`` (spec 06 §2.4)."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.collectors.github.client import GitHubUnavailable
from app.collectors.github.models import DAY, RepoRow, repo_fields
from app.collectors.github.urlnorm import _name_token
from app.logging.logger import logger

if TYPE_CHECKING:  # pragma: no cover
    from app.collectors.github.client import GitHubClient
    from app.storage.db import Database

_NETUID_RE = re.compile(r"\bsn\s?(\d+)\b|\bnetuid\s?(\d+)\b|\bsubnet[\s-]?(\d+)\b", re.I)
_SUBNET_RE = re.compile(r"subnet|bittensor|validator|miner|netuid", re.I)
_SN_RE = re.compile(r"\bsn\s?\d+\b", re.I)


def is_subnet_related(name: str, description: str | None, subnet_name: str | None = None) -> bool:
    text = f"{name} {description or ''}"
    if _SUBNET_RE.search(text) or _SN_RE.search(text):
        return True
    tok = _name_token(subnet_name)
    return bool(tok and tok in text.lower())


def pick_primary(repos: list[RepoRow], netuid: int, subnet_name: str | None, now: int) -> RepoRow | None:
    cands = [r for r in repos if not r.is_fork and not r.archived and r.status == "ok"]
    if not cands:
        return None

    def score(r: RepoRow) -> tuple:
        text = f"{r.name} {r.description or ''}"
        b = 0
        m = _NETUID_RE.search(text)
        if m and int(next(g for g in m.groups() if g)) == netuid:
            b += 4                                                      # "SN 48" vs "SN 63" (qbittensor-labs)
        if _SUBNET_RE.search(text):
            b += 2
        tok = _name_token(subnet_name)
        if tok and tok in text.lower():
            b += 1
        age_d = (now - r.pushed_at) / DAY if r.pushed_at else 10 ** 6
        fresh = 3 if age_d <= 7 else 2 if age_d <= 30 else 1 if age_d <= 90 else 0
        return (b + fresh, r.pushed_at or 0, r.stars or 0, -r.repo_id)
    return max(cands, key=score)


@dataclass(slots=True)
class OrgListing:
    owner: str
    kind: str                      # org | user | gone
    repos: list[dict] = field(default_factory=list)
    ok: bool = True
    from_cache: bool = False
    error: str | None = None
    login: str | None = None


async def enumerate_owner(client: "GitHubClient", owner: str, *, known_kind: str | None = None, job: str = "",
                          deadline: float | None = None) -> OrgListing:
    """Kind-aware listing: the known endpoint first; ``/orgs`` 404 → ``/users``; both 404 → ``gone``."""
    s = app_config.settings
    max_pages = max(1, -(-int(s.GITHUB_ORG_MAX_REPOS) // 100))
    order = ["user", "org"] if known_kind == "user" else ["org", "user"]
    for kind in order:
        path = f"/orgs/{owner}/repos" if kind == "org" else f"/users/{owner}/repos"
        params = {"type": "public" if kind == "org" else "owner", "sort": "pushed", "per_page": 100}
        try:
            first = await client.get(path, params=params, etag_key="", job=job, deadline=deadline)
        except GitHubUnavailable as exc:
            return OrgListing(owner, known_kind or "org", ok=False, error=str(exc))
        if first.status == 404:
            continue
        if not first.ok or not isinstance(first.json, list):
            return OrgListing(owner, known_kind or kind, ok=False, error=f"HTTP {first.status}")
        repos = list(first.json)
        pages = 1
        last_n = len(first.json)
        # page numbers, not Link headers: a 304 (cached page 1) carries no usable Link
        while last_n >= 100 and pages < max_pages:
            try:
                r = await client.get(path, params={**params, "page": pages + 1}, job=job, deadline=deadline)
            except GitHubUnavailable as exc:
                return OrgListing(owner, kind, repos, ok=False, error=str(exc))
            if r.status != 200 or not isinstance(r.json, list):
                break
            repos.extend(r.json)
            last_n = len(r.json)
            pages += 1
        login = (repos[0].get("owner") or {}).get("login") if repos else None
        return OrgListing(owner, kind, repos[: s.GITHUB_ORG_MAX_REPOS], from_cache=first.from_cache, login=login)
    return OrgListing(owner, "gone")


async def store_listing(db: "Database", listing: OrgListing, *, now: int, run_id: str) -> dict[str, Any]:
    """Upsert every listed repo + the ``github_orgs`` row; returns archived flips (prior observation required)."""
    owner = listing.owner.lower()
    prev = await db.fetchone("SELECT * FROM github_orgs WHERE owner = ?", (owner,))
    if not listing.ok:
        await db.execute("INSERT INTO github_orgs (owner, kind, first_seen_at, last_enumerated_at, consecutive_failures) "
                         "VALUES (?,?,?,?,1) ON CONFLICT(owner) DO UPDATE SET last_enumerated_at = excluded.last_enumerated_at, "
                         "consecutive_failures = github_orgs.consecutive_failures + 1",
                         (owner, (prev or {}).get("kind") or listing.kind, now, now))
        return {"flips": [], "repo_ids": []}
    fields = [repo_fields(p) for p in listing.repos if p.get("id") is not None]
    ids = [f["repo_id"] for f in fields]
    before: dict[int, dict] = {}
    for i in range(0, len(ids), 500):
        part = ids[i:i + 500]
        ph = ",".join("?" * len(part))
        for r in await db.fetchall(f"SELECT repo_id, archived, full_name FROM github_repos WHERE repo_id IN ({ph})",
                                   part, writer=True):
            before[int(r["repo_id"])] = r
    flips = [f for f in fields if f["repo_id"] in before and int(before[f["repo_id"]]["archived"]) == 0
             and f["archived"] == 1]
    async with db.transaction():
        for f in fields:
            await upsert_repo_row(db, f, method="org_enum", now=now, verified=True)
        await write_snapshots(db, fields, now=now, run_id=run_id)
        non_fork = [f for f in fields if not f["is_fork"]]
        live = [f for f in non_fork if not f["archived"]]
        cutoff = now - 30 * DAY
        await db.execute(
            "INSERT INTO github_orgs (owner, login, kind, n_public, n_non_fork, n_archived, n_pushed_30d, "
            "max_pushed_at_non_fork, etag, first_seen_at, last_enumerated_at, last_ok_at, consecutive_failures) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0) ON CONFLICT(owner) DO UPDATE SET login = COALESCE(excluded.login, "
            "github_orgs.login), kind = excluded.kind, n_public = excluded.n_public, n_non_fork = excluded.n_non_fork, "
            "n_archived = excluded.n_archived, n_pushed_30d = excluded.n_pushed_30d, "
            "max_pushed_at_non_fork = excluded.max_pushed_at_non_fork, last_enumerated_at = excluded.last_enumerated_at, "
            "last_ok_at = excluded.last_ok_at, consecutive_failures = 0",
            (owner, listing.login, listing.kind, len(fields), len(non_fork), sum(1 for f in fields if f["archived"]),
             sum(1 for f in live if (f["pushed_at"] or 0) >= cutoff),
             max((f["pushed_at"] or 0 for f in live), default=None) or None, None, now, now, now))
    return {"flips": flips, "repo_ids": ids}


async def upsert_repo_row(db: "Database", f: dict, *, method: str, now: int, verified: bool,
                          etag: str | None = None) -> None:
    """Insert or refresh a repo row: ``resolution_method``/``first_seen_at`` are kept from first contact; a 200
    restores ``status='ok'``."""
    row = dict(f)
    row.update({"resolution_method": method, "first_seen_at": now, "last_verified_at": now, "last_ok_at": now,
                "status": "ok", "consecutive_404": 0, "gone_since": None})
    if etag is not None:
        row["etag"] = etag
    cols = list(row)
    keep = {"resolution_method", "first_seen_at", "repo_id"}
    upd = ", ".join(f"{c} = excluded.{c}" for c in cols if c not in keep)
    await db.execute(f"INSERT INTO github_repos ({', '.join(cols)}) VALUES ({','.join('?' * len(cols))}) "
                     f"ON CONFLICT(repo_id) DO UPDATE SET {upd}", [row[c] for c in cols])


_SNAP = ("full_name", "pushed_at", "stars", "forks", "open_issues", "archived", "size_kb")


async def write_snapshots(db: "Database", fields: list[dict], *, now: int, run_id: str, status: str = "ok") -> int:
    """``github_repo_snapshots``: when a tracked field changed, and at least daily per repo (PIT source, §2.8)."""
    n = 0
    for f in fields:
        last = await db.fetchone("SELECT * FROM github_repo_snapshots WHERE repo_id = ? ORDER BY observed_at DESC "
                                 "LIMIT 1", (f["repo_id"],), writer=True)
        changed = last is None or any(last.get(k) != f.get(k) for k in _SNAP) or last.get("status") != status
        if not changed and now - int(last["observed_at"]) < DAY:
            continue
        if last is not None and int(last["observed_at"]) == now:
            continue
        await db.execute(
            "INSERT OR IGNORE INTO github_repo_snapshots (repo_id, observed_at, full_name, pushed_at, stars, forks, "
            "open_issues, archived, size_kb, status, event_at, source, run_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (f["repo_id"], now, f.get("full_name"), f.get("pushed_at"), f.get("stars"), f.get("forks"),
             f.get("open_issues"), f.get("archived"), f.get("size_kb"), status, f.get("pushed_at"), "github_api",
             run_id))
        n += 1
    return n


# ── activity scope ───────────────────────────────────────────────────────────────────────────────────────────
@dataclass(slots=True)
class Scope:
    occupant_key: str
    owner: str | None
    declared: RepoRow | None
    declared_alive: bool
    primary: RepoRow | None
    repo_ids: list[int]
    owner_kind: str | None


async def canonical_owner(db: "Database", occupant_key: str, declared_source: str | None) -> tuple[str | None, dict | None]:
    """(owner lower, declared ref row): the declared repo's canonical owner, else the ref's owner as written."""
    if not declared_source:
        return None, None
    ref = await db.fetchone("SELECT * FROM github_declared_refs WHERE occupant_key = ? AND source = ?",
                            (occupant_key, declared_source), writer=True)
    if not ref or ref["state"] in ("placeholder", "none") or not ref["owner"]:
        return None, ref
    if ref["kind"] == "repo" and ref["repo_id"]:
        r = await db.fetchone("SELECT owner FROM github_repos WHERE repo_id = ?", (ref["repo_id"],), writer=True)
        if r:
            return r["owner"], ref
    return ref["owner"].lower(), ref


async def rebuild_scope(db: "Database", occupant_key: str, *, netuid: int, name: str | None,
                        declared_source: str | None, now: int) -> Scope:
    """Rewrite ``github_occupant_repos`` roles + ``in_scope`` for one occupant (§2.4)."""
    s = app_config.settings
    owner, ref = await canonical_owner(db, occupant_key, declared_source)
    declared: RepoRow | None = None
    alive = False
    if ref and ref["kind"] == "repo" and ref["repo_id"]:
        r = await db.fetchone("SELECT * FROM github_repos WHERE repo_id = ?", (ref["repo_id"],), writer=True)
        if r:
            declared = RepoRow.from_db(r)
            alive = r["status"] == "ok" and ref["state"] == "resolved"
    org = await db.fetchone("SELECT kind FROM github_orgs WHERE owner = ?", (owner,), writer=True) if owner else None
    kind = org["kind"] if org else None
    repos: list[RepoRow] = []
    if owner:
        repos = [RepoRow.from_db(r) for r in await db.fetchall(
            "SELECT * FROM github_repos WHERE owner = ? AND status = 'ok' ORDER BY pushed_at DESC", (owner,),
            writer=True)]
    if kind == "user" or (declared is not None and declared.owner_type == "User"):
        kind = "user"
        repos = [r for r in repos if (declared and r.repo_id == declared.repo_id)
                 or is_subnet_related(r.name, r.description, name)]
    primary = None
    if not alive:
        primary = pick_primary([r for r in repos if not declared or r.repo_id != declared.repo_id], netuid, name, now)
    chosen: list[int] = []
    if declared is not None and alive:
        chosen.append(declared.repo_id)
    if primary is not None and primary.repo_id not in chosen:
        chosen.append(primary.repo_id)
    cap = int(s.GITHUB_ACTIVITY_MAX_REPOS_PER_OCCUPANT)
    lookback = now - int(s.GITHUB_ACTIVITY_LOOKBACK_DAYS) * DAY
    for r in repos:
        if len(chosen) >= cap:
            break
        if r.repo_id in chosen or r.is_fork or r.archived or (r.pushed_at or 0) < lookback:
            continue
        chosen.append(r.repo_id)
    rows: list[dict] = []
    roles: dict[int, str] = {}
    if declared is not None:
        roles[declared.repo_id] = "declared" if alive else "declared_gone"
    if primary is not None:
        roles[primary.repo_id] = "primary"
    for r in repos:
        roles.setdefault(r.repo_id, "org")
    prev = {int(r["repo_id"]): r for r in await db.fetchall(
        "SELECT repo_id, role FROM github_occupant_repos WHERE occupant_key = ?", (occupant_key,), writer=True)}
    old_primary = next((rid for rid, r in prev.items() if r["role"] == "primary"), None)
    if primary is not None and old_primary not in (None, primary.repo_id):
        logger.info("github.primary_switch", data={"occupant_key": occupant_key, "from": old_primary,
                                                   "to": primary.repo_id})
    for rid, role in roles.items():
        rows.append({"occupant_key": occupant_key, "repo_id": rid, "role": role,
                     "declared_source": declared_source if role in ("declared", "declared_gone") else None,
                     "in_scope": 1 if rid in chosen else 0, "first_seen_at": now, "last_seen_at": now})
    async with db.transaction():
        if rows:
            await db.upsert("github_occupant_repos", rows, conflict=("occupant_key", "repo_id"),
                            update=("role", "declared_source", "in_scope", "last_seen_at"))
        stale = [rid for rid in prev if rid not in roles]
        for rid in stale:
            await db.execute("UPDATE github_occupant_repos SET in_scope = 0, role = CASE WHEN role IN "
                             "('declared','primary') THEN 'org' ELSE role END WHERE occupant_key = ? AND repo_id = ?",
                             (occupant_key, rid))
    return Scope(occupant_key, owner, declared, alive, primary, chosen, kind)


async def scope_repo_ids(db: "Database", occupant_key: str, *, include_declared_gone: bool = False) -> list[int]:
    """``R(o)``: in-scope repos (+ the declared-gone repo during its grace period, for metrics)."""
    rows = await db.fetchall("SELECT repo_id, role, in_scope FROM github_occupant_repos WHERE occupant_key = ?",
                             (occupant_key,))
    return [int(r["repo_id"]) for r in rows
            if r["in_scope"] or (include_declared_gone and r["role"] == "declared_gone")]


def listing_summary(listing: OrgListing) -> dict:
    return {"owner": listing.owner, "kind": listing.kind, "n": len(listing.repos), "ok": listing.ok,
            "cached": listing.from_cache, "error": listing.error}


def dumps(x: Any) -> str:
    return json.dumps(x, separators=(",", ":"), sort_keys=True, default=str)
