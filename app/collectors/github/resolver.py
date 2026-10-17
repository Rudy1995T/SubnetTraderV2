"""Canonical repo resolver (spec 06 §2.3): declared refs → numeric ``repo_id``; aliases, gone/blocked, events.

Never resolves by name once an id is known (re-verify = ``GET /repositories/{id}``). A dead declared repo is not
replaced by a lower-precedence source's repo: it falls back only to org enumeration of its own owner.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.collectors.github import ghevents
from app.collectors.github.client import GitHubUnavailable, QuotaDeferred, parse_redirect_id
from app.collectors.github.models import repo_fields
from app.collectors.github.orgs import enumerate_owner, rebuild_scope, store_listing, upsert_repo_row, write_snapshots
from app.collectors.github.urlnorm import DeclaredRef, first_ref
from app.logging.logger import logger
from app.registry import active_occupants, resolved
from app.registry.merge import ALTERNATES, DEMOTED_RANK, SOURCE_RANK

if TYPE_CHECKING:  # pragma: no cover
    from app.collectors.github.client import GHResponse, GitHubClient
    from app.registry.models import Occupant, Resolved
    from app.storage.db import Database

JOB = "github_resolve"
SOURCE_MAP = {"chain": "chain", "subnetalpha": "subnetalpha", "tmc_coldkey": "tmc", "tmc": "tmc",
              "manual": "manual", "legacy": "seed_fundamentals"}
SOURCE_04 = {"chain": "chain", "subnetalpha": "subnetalpha", "tmc": "tmc_coldkey", "manual": "manual",
             "seed_fundamentals": "legacy"}
CONFIRM_GAP_S = 5 * 3600            # a 404 must be reproduced ≥ one cycle later (6 h; 5 h tolerates jitter)


# ── declared refs from 04's candidates ───────────────────────────────────────────────────────────────────────
@dataclass(slots=True)
class SourceRef:
    source: str                     # 06 vocabulary (03 CHECK)
    raw: str
    ref: DeclaredRef | None
    placeholder: bool

    @property
    def kind(self) -> str:
        return self.ref.kind if self.ref else "none"


def _order(fld: str) -> int:
    if fld == "github_repo":
        return 0
    if fld.startswith("github_repo__extra_"):
        try:
            return 1 + int(fld.rsplit("_", 1)[1])
        except ValueError:
            return 50
    alts = ALTERNATES.get("github_repo", ())
    return 100 + (alts.index(fld) if fld in alts else 0)


def ranked_candidates(res: "Resolved | None") -> list[dict]:
    """04's order: the winner first, then (demoted → last, source rank, field order, newest)."""
    if res is None:
        return []
    seen: set[tuple[str, str]] = set()
    cands: list[dict] = []
    for c in res.candidates or []:
        k = (c.get("source") or "", (c.get("value_norm") or c.get("value") or "").strip().lower())
        if k in seen or not k[1]:
            continue
        seen.add(k)
        cands.append(c)

    def rank(c: dict) -> tuple:
        win = (res.source is not None and c.get("source") == res.source
               and (c.get("value") == res.value or c.get("value_norm") == res.value_norm))
        r = DEMOTED_RANK if c.get("demoted") else SOURCE_RANK.get(c.get("source") or "", 9)
        return (0 if win else 1, r, _order(c.get("field") or ""), -(c.get("observed_at") or 0))
    return sorted(cands, key=rank)


def source_refs(res: "Resolved | None") -> tuple[list[SourceRef], str | None]:
    """One ref per source (first non-suppressed candidate; its first repo ref, else first owner ref) and the
    declared source (first ranked non-junk, non-placeholder ref)."""
    out: dict[str, SourceRef] = {}
    declared: str | None = None
    for c in ranked_candidates(res):
        src = SOURCE_MAP.get(c.get("source") or "")
        if src is None or c.get("suppressed"):
            continue
        raw = c.get("value") or c.get("value_norm") or ""
        ref = first_ref(raw) or first_ref(c.get("value_norm"))
        ph = bool(c.get("is_junk")) or (ref is not None and ref.placeholder)
        cur = out.get(src)
        if cur is None or (cur.placeholder or cur.ref is None) and ref is not None and not ph:
            out[src] = SourceRef(src, raw, ref, ph)
        if declared is None and ref is not None and not ph:
            declared = src
    return list(out.values()), declared


# ── per-run results ──────────────────────────────────────────────────────────────────────────────────────────
@dataclass(slots=True)
class Verify:
    outcome: str                    # ok | gone | blocked | failed
    repo_id: int | None = None
    method: str | None = None
    status: int | None = None
    full_name: str | None = None
    renamed_from: str | None = None
    note: str | None = None


@dataclass
class ResolveReport:
    refs: int = 0
    attempted: int = 0
    left: int = 0
    failed: int = 0
    events: dict[str, int] = field(default_factory=dict)
    queue: dict[str, int] = field(default_factory=dict)
    trace: list[str] = field(default_factory=list)
    deferred: bool = False

    def bump(self, et: str) -> None:
        self.events[et] = self.events.get(et, 0) + 1


class Resolver:
    def __init__(self, db: "Database", client: "GitHubClient", *, run_id: str, now: int,
                 deadline: float | None = None, job: str = JOB) -> None:
        self.db = db
        self.client = client
        self.run_id = run_id
        self.now = int(now)
        self.deadline = deadline
        self.job = job
        self._verified: dict[int, Verify] = {}
        self._changes: dict[int, list[tuple[str, dict]]] = {}      # repo_id → [(event_type, payload)]
        self._enumerated: set[str] = set()
        self._prev_states: dict[tuple[str, str], str] = {}
        self._prev_declared: dict[str, dict] = {}
        self.report = ResolveReport()

    # ── HTTP steps ───────────────────────────────────────────────────────────────────────────────────────────
    async def _prev_repo(self, repo_id: int) -> dict | None:
        return await self.db.fetchone("SELECT * FROM github_repos WHERE repo_id = ?", (repo_id,), writer=True)

    async def _store_ok(self, payload: dict, method: str, resp: "GHResponse") -> Verify:
        f = repo_fields(payload)
        prev = await self._prev_repo(f["repo_id"])
        async with self.db.transaction():
            await upsert_repo_row(self.db, f, method=method, now=self.now, verified=True, etag=resp.headers.get("etag"))
            await write_snapshots(self.db, [f], now=self.now, run_id=self.run_id)
            await self._alias(f["repo_id"], f["full_name"], "api")
        rid = f["repo_id"]
        if prev is not None:
            if prev["full_name"] != f["full_name"]:
                await self._record_rename(rid, prev["full_name"], f["full_name"], "api")
            if int(prev["archived"]) != f["archived"]:
                kind = "archived" if f["archived"] else "unarchived"
                await ghevents.raw_event(self.db, repo_id=rid, kind=kind, dedupe_key=f"{kind}:{rid}:{self.now // 86400}",
                                         payload={"full_name": f["full_name"]}, now=self.now, run_id=self.run_id)
                if f["archived"]:
                    self._changes.setdefault(rid, []).append(("repo_archived", {"repo_id": rid,
                                                                                "full_name": f["full_name"]}))
            if prev["default_branch"] and f["default_branch"] and prev["default_branch"] != f["default_branch"]:
                await ghevents.raw_event(self.db, repo_id=rid, kind="default_branch_changed",
                                         dedupe_key=f"default_branch_changed:{rid}:{f['default_branch']}",
                                         payload={"from": prev["default_branch"], "to": f["default_branch"]},
                                         now=self.now, run_id=self.run_id)
            if prev["status"] == "gone":
                await ghevents.raw_event(self.db, repo_id=rid, kind="restored", dedupe_key=f"restored:{rid}:{self.now // 86400}",
                                         payload={"full_name": f["full_name"]}, now=self.now, run_id=self.run_id)
                self._changes.setdefault(rid, []).append(("repo_restored", {"repo_id": rid,
                                                                            "full_name": f["full_name"]}))
        return Verify("ok", rid, method, resp.status, f["full_name"])

    async def _alias(self, repo_id: int, full_name: str, source: str) -> None:
        await self.db.execute(
            "INSERT INTO github_repo_aliases (repo_id, full_name, source, first_seen_at, last_seen_at) VALUES (?,?,?,?,?) "
            "ON CONFLICT(repo_id, full_name) DO UPDATE SET last_seen_at = excluded.last_seen_at",
            (repo_id, full_name.lower(), source, self.now, self.now))

    async def _record_rename(self, repo_id: int, old: str, new: str, method: str) -> None:
        old_l, new_l = old.lower(), new.lower()
        if old_l == new_l:
            return
        await self._alias(repo_id, old_l, "redirect" if method == "redirect" else "api")
        moved = old_l.split("/")[0] != new_l.split("/")[0]
        kind = "transferred" if moved else "renamed"
        await ghevents.raw_event(self.db, repo_id=repo_id, kind=kind, dedupe_key=f"{kind}:{repo_id}:{new_l}",
                                 payload={"from": old_l, "to": new_l}, now=self.now, run_id=self.run_id)
        if moved:
            payload = {"repo_id": repo_id, "from": old_l, "to": new_l, "from_owner": old_l.split("/")[0],
                       "to_owner": new_l.split("/")[0], "detected_at": self.now}
            self._changes.setdefault(repo_id, []).append(("repo_moved", payload))
        else:
            self._changes.setdefault(repo_id, []).append(("repo_renamed", {
                "repo_id": repo_id, "from": old_l, "to": new_l, "detected_at": self.now,
                "method": "html" if method == "html" else "api"}))

    async def _store_missing(self, repo_id: int, outcome: str, status: int) -> Verify:
        """404/403/451 on a known id: repo-level counters (one count per run; confirm ≥ 5 h after the first)."""
        prev = await self._prev_repo(repo_id)
        if prev is None:
            return Verify(outcome, repo_id, status=status)
        if outcome == "blocked":
            await self.db.execute("UPDATE github_repos SET status = 'blocked', last_verified_at = ?, "
                                  "gone_since = COALESCE(gone_since, ?) WHERE repo_id = ?", (self.now, self.now, repo_id))
            return Verify("blocked", repo_id, status=status, full_name=prev["full_name"])
        n = int(prev["consecutive_404"] or 0) + 1
        since = prev["gone_since"] or self.now
        confirmed = n >= 2 and self.now - since >= CONFIRM_GAP_S
        await self.db.execute("UPDATE github_repos SET consecutive_404 = ?, gone_since = ?, last_verified_at = ?, "
                              "status = ? WHERE repo_id = ?",
                              (n, since, self.now, "gone" if confirmed or prev["status"] == "gone" else prev["status"],
                               repo_id))
        if confirmed and prev["status"] != "gone":
            await ghevents.raw_event(self.db, repo_id=repo_id, kind="gone", dedupe_key=f"gone:{repo_id}:{self.now // 86400}",
                                     payload={"full_name": prev["full_name"], "first_404_at": since},
                                     now=self.now, run_id=self.run_id)
            await write_snapshots(self.db, [{**prev, "repo_id": repo_id}], now=self.now, run_id=self.run_id,
                                  status="gone")
        return Verify("gone", repo_id, status=status, full_name=prev["full_name"])

    async def verify_by_id(self, repo_id: int) -> Verify:
        if repo_id in self._verified:
            return self._verified[repo_id]
        try:
            r = await self.client.get(f"/repositories/{repo_id}", etag_key="", job=self.job, deadline=self.deadline)
        except GitHubUnavailable as exc:
            v = Verify("failed", repo_id, note=str(exc)[:200])
            self._verified[repo_id] = v
            return v
        if r.ok and isinstance(r.json, dict):
            v = await self._store_ok(r.json, "direct", r)
        elif r.status == 404:
            v = await self._store_missing(repo_id, "gone", 404)
        elif r.status in (403, 451):
            v = await self._store_missing(repo_id, "blocked", r.status)
        elif r.status in (301, 302, 307, 308):
            rid = parse_redirect_id(r.location)
            if rid is None or rid == repo_id:
                logger.warning("resolver.unexpected_location", data={"repo_id": repo_id, "location": r.location})
                v = Verify("failed", repo_id, status=r.status, note="unexpected_location")
            else:
                v = await self.verify_by_id(rid)          # a transferred id that GitHub re-points (rare)
        else:
            v = Verify("failed", repo_id, status=r.status)
        self._verified[repo_id] = v
        return v

    async def first_contact(self, ref: DeclaredRef) -> Verify:
        assert ref.repo is not None
        try:
            r = await self.client.get(f"/repos/{ref.owner}/{ref.repo}", etag_key="", job=self.job,
                                      deadline=self.deadline)
        except GitHubUnavailable as exc:
            return Verify("failed", note=str(exc)[:200])
        if r.ok and isinstance(r.json, dict):
            v = await self._store_ok(r.json, "direct", r)
            if v.full_name and v.full_name != ref.key:   # GitHub answered 200 for a case/alias difference
                await self._alias(v.repo_id, ref.key, "declared")
            self._verified.setdefault(v.repo_id, v)
            return v
        if r.status in (301, 302, 307, 308):
            rid = parse_redirect_id(r.location)
            if rid is None:
                logger.warning("resolver.unexpected_location", data={"ref": ref.key, "location": r.location})
                return Verify("failed", status=r.status, note="unexpected_location")
            try:
                r2 = await self.client.get(f"/repositories/{rid}", etag_key="", job=self.job, deadline=self.deadline)
            except GitHubUnavailable as exc:
                return Verify("failed", note=str(exc)[:200])
            if r2.ok and isinstance(r2.json, dict):
                v = await self._store_ok(r2.json, "redirect", r2)
                await self._record_rename(rid, ref.key, v.full_name or ref.key, "redirect")
                v = Verify("ok", rid, "redirect", r.status, v.full_name, renamed_from=ref.key)
                self._verified[rid] = v
                return v
            if r2.status == 404:
                return Verify("gone", None, status=404)
            return Verify("failed", status=r2.status)
        if r.status == 404:
            return Verify("gone", None, status=404)
        if r.status in (403, 451):
            return Verify("blocked", None, status=r.status)
        return Verify("failed", status=r.status)

    # ── ref rows ─────────────────────────────────────────────────────────────────────────────────────────────
    async def sync_rows(self, occ: "Occupant", srefs: list[SourceRef]) -> dict[str, dict]:
        """Write/refresh one ``github_declared_refs`` row per source; a changed ref key restarts the row."""
        existing = {r["source"]: r for r in await self.db.fetchall(
            "SELECT * FROM github_declared_refs WHERE occupant_key = ?", (occ.occupant_key,), writer=True)}
        rows: dict[str, dict] = {}
        for sr in srefs:
            ref = sr.ref
            key = ref.key if ref else None
            cur = existing.get(sr.source)
            if cur is not None and cur["ref_key"] == key and bool(cur["is_placeholder"]) == sr.placeholder:
                if cur["raw_value"] != sr.raw:
                    await self.db.execute("UPDATE github_declared_refs SET raw_value = ?, observed_at = ? WHERE "
                                          "occupant_key = ? AND source = ?", (sr.raw, self.now, occ.occupant_key,
                                                                               sr.source))
                    cur = {**cur, "raw_value": sr.raw}
                rows[sr.source] = dict(cur)
                continue
            state = "placeholder" if sr.placeholder else ("none" if ref is None else "new")
            row = {"occupant_key": occ.occupant_key, "source": sr.source, "raw_value": sr.raw,
                   "kind": ref.kind if ref else "none", "owner": ref.owner if ref else None,
                   "repo": ref.repo if ref else None, "ref_key": key, "is_placeholder": 1 if sr.placeholder else 0,
                   "state": state, "repo_id": None, "resolution_method": None,
                   "first_seen_at": cur["first_seen_at"] if cur else self.now, "last_attempt_at": None,
                   "last_http_status": None, "consecutive_404": 0, "first_404_at": None, "resolved_at": None,
                   "observed_at": self.now}
            await self.db.upsert("github_declared_refs", [row], conflict=("occupant_key", "source"))
            rows[sr.source] = row
        for src in set(existing) - {sr.source for sr in srefs}:
            await self.db.execute("DELETE FROM github_declared_refs WHERE occupant_key = ? AND source = ?",
                                  (occ.occupant_key, src))
        return rows

    async def resolve_row(self, row: dict) -> dict:
        """One state-machine step for a repo ref (owner refs are resolved through enumeration)."""
        ref = DeclaredRef(row["kind"], row["owner"], row["repo"], row["ref_key"], row["raw_value"], False)
        if row["repo_id"]:
            v = await self.verify_by_id(int(row["repo_id"]))
        else:
            v = await self.first_contact(ref)
        upd: dict[str, Any] = {"last_attempt_at": self.now, "last_http_status": v.status}
        if v.outcome == "ok":
            upd.update(state="resolved", repo_id=v.repo_id, consecutive_404=0, first_404_at=None,
                       resolution_method=row["resolution_method"] or v.method,
                       resolved_at=row["resolved_at"] or self.now)
        elif v.outcome == "gone":
            n = int(row["consecutive_404"] or 0) + 1
            first = row["first_404_at"] or self.now
            confirmed = n >= 2 and self.now - first >= CONFIRM_GAP_S
            upd.update(consecutive_404=n, first_404_at=first,
                       state="gone" if confirmed or row["state"] == "gone" else "gone_pending")
        elif v.outcome == "blocked":
            upd.update(state="blocked", first_404_at=row["first_404_at"] or self.now)
        else:
            self.report.failed += 1
        self.report.trace.append(self._trace(row, v))
        sets = ", ".join(f"{k} = ?" for k in upd)
        await self.db.execute(f"UPDATE github_declared_refs SET {sets} WHERE occupant_key = ? AND source = ?",
                              [*upd.values(), row["occupant_key"], row["source"]])
        return {**row, **upd}

    def _trace(self, row: dict, v: Verify) -> str:
        src = SOURCE_04.get(row["source"], row["source"])
        name = f"{row['owner']}/{row['repo']}" if row["repo"] else row["owner"]
        if v.outcome == "ok":
            via = f"{v.status} → id={v.repo_id}" if v.method == "redirect" else f"{v.status} id={v.repo_id}"
            if v.method == "redirect":
                return f"{src}: {name} → {via} {v.full_name} (redirect) alias"
            return f"{src}: {name} → {via} {v.full_name} ({row['resolution_method'] or v.method})"
        return f"{src}: {name} → {v.outcome} ({v.status or v.note})"

    async def enumerate_and_store(self, owner: str) -> dict | None:
        """Org listing for one canonical owner (once per run)."""
        o = owner.lower()
        if o in self._enumerated:
            return await self.db.fetchone("SELECT * FROM github_orgs WHERE owner = ?", (o,), writer=True)
        self._enumerated.add(o)
        prev = await self.db.fetchone("SELECT kind FROM github_orgs WHERE owner = ?", (o,), writer=True)
        listing = await enumerate_owner(self.client, owner, known_kind=(prev or {}).get("kind"), job=self.job,
                                        deadline=self.deadline)
        await store_listing(self.db, listing, now=self.now, run_id=self.run_id)
        return await self.db.fetchone("SELECT * FROM github_orgs WHERE owner = ?", (o,), writer=True)

    # ── per-occupant finalisation ────────────────────────────────────────────────────────────────────────────
    async def finalise(self, occ: "Occupant", rows: dict[str, dict], declared_src: str | None,
                       *, enumerate_missing: bool = True) -> None:
        db = self.db
        okey = occ.occupant_key
        decl = rows.get(declared_src) if declared_src else None
        # owner-only or dead declared refs fall back to org enumeration of their own owner
        if decl is not None and decl["state"] not in ("placeholder", "none"):
            needs_org = decl["kind"] == "owner" or decl["state"] in ("gone_pending", "gone", "blocked", "org_empty")
            if needs_org and enumerate_missing and self.client.token_state != "missing":
                owner = decl["owner"]
                if decl["repo_id"]:
                    r = await db.fetchone("SELECT owner FROM github_repos WHERE repo_id = ?", (decl["repo_id"],),
                                          writer=True)
                    owner = r["owner"] if r else owner
                org = await db.fetchone("SELECT * FROM github_orgs WHERE owner = ?", (owner.lower(),), writer=True)
                if org is None or (org["last_ok_at"] or 0) < self.now - 86400:
                    await self.enumerate_and_store(owner)
        scope = await rebuild_scope(db, okey, netuid=occ.netuid, name=occ.name, declared_source=declared_src,
                                    now=self.now)
        if decl is not None and decl["kind"] == "owner" and decl["state"] not in ("placeholder", "none"):
            org = await db.fetchone("SELECT kind, n_non_fork, n_archived, last_ok_at FROM github_orgs WHERE owner = ?",
                                    (decl["owner"].lower(),), writer=True)
            if org is not None and org["last_ok_at"] is not None:
                if scope.primary is not None:
                    st, rid = "resolved", scope.primary.repo_id
                elif org["kind"] == "gone":
                    st, rid = "gone", None
                else:
                    st, rid = "org_empty", None
                first = decl["first_404_at"] or (self.now if st != "resolved" else None)
                await db.execute("UPDATE github_declared_refs SET state = ?, repo_id = ?, resolution_method = "
                                 "COALESCE(resolution_method, 'org_enum'), first_404_at = ?, last_attempt_at = ?, "
                                 "resolved_at = COALESCE(resolved_at, CASE WHEN ? = 'resolved' THEN ? END) "
                                 "WHERE occupant_key = ? AND source = ?",
                                 (st, rid, first if st != "resolved" else None, self.now, st, self.now, okey,
                                  decl["source"]))
                if st != decl["state"] and st in ("gone", "org_empty"):
                    decl = {**decl, "state": st, "first_404_at": first}
                    await self._repo_gone(occ, decl, reason="org_empty" if st == "org_empty" else "unresolved",
                                          fallback=None)
                decl = {**decl, "state": st, "repo_id": rid}
        # other sources' owner-only refs on the canonical owner share its listing (no events: not declared)
        if scope.owner:
            org = await db.fetchone("SELECT kind, last_ok_at FROM github_orgs WHERE owner = ?", (scope.owner,),
                                    writer=True)
            target = scope.primary.repo_id if scope.primary else (scope.declared.repo_id if scope.declared_alive
                                                                  else None)
            for r in rows.values():
                if (r["kind"] != "owner" or (decl is not None and r["source"] == decl["source"])
                        or (r["owner"] or "").lower() != scope.owner or org is None or org["last_ok_at"] is None):
                    continue
                st = "resolved" if target else ("gone" if org["kind"] == "gone" else "org_empty")
                await db.execute("UPDATE github_declared_refs SET state = ?, repo_id = ?, resolution_method = "
                                 "COALESCE(resolution_method, 'org_enum'), last_attempt_at = ?, first_404_at = "
                                 "CASE WHEN ? = 'resolved' THEN NULL ELSE COALESCE(first_404_at, ?) END "
                                 "WHERE occupant_key = ? AND source = ?",
                                 (st, target, self.now, st, self.now, okey, r["source"]))
        # repo_gone on the declared ref's transition to gone / blocked
        if decl is not None and decl["kind"] == "repo":
            prev_state = self._prev_states.get((okey, decl["source"]))     # None outside a resolve run
            if decl["state"] in ("gone", "blocked") and prev_state not in (None, "gone", "blocked"):
                reason = "blocked" if decl["state"] == "blocked" else ("404" if decl["repo_id"] else "unresolved")
                await self._repo_gone(occ, decl, reason=reason,
                                      fallback=scope.primary.repo_id if scope.primary else None)
        # identity_repo_changed: declared id changed between cycles (first sight is not a change)
        new_id = int(decl["repo_id"]) if decl and decl["repo_id"] and decl["kind"] == "repo" else None
        prev_decl = self._prev_declared.get(okey)
        if new_id is not None and prev_decl is not None and prev_decl["repo_id"] != new_id:
            await self._emit(occ, "identity_repo_changed", {
                "source": SOURCE_04.get(decl["source"], decl["source"]), "from_ref": prev_decl.get("full_name"),
                "to_ref": decl["ref_key"], "from_repo_id": prev_decl["repo_id"], "to_repo_id": new_id},
                to_ref=decl["ref_key"])
        # repo-level changes (rename/move/archive/restore) for repos this occupant declares or uses as primary
        linked = {int(r["repo_id"]): r["role"] for r in await db.fetchall(
            "SELECT repo_id, role FROM github_occupant_repos WHERE occupant_key = ? AND role IN "
            "('declared','primary','declared_gone')", (okey,), writer=True)}
        ref_ids = {int(r["repo_id"]) for r in rows.values() if r.get("repo_id")}
        for rid in set(linked) | ref_ids:
            for et, payload in self._changes.get(rid, []):
                if et == "repo_archived":
                    if rid not in linked:
                        continue
                    payload = {**payload, "role": "declared" if linked[rid] != "primary" else "primary"}
                await self._emit(occ, et, payload, **{k: payload[k] for k in ("repo_id", "to") if k in payload})
        # source disagreement: ≥ 2 distinct resolved ids among the sources (by id, never by string)
        groups: dict[int, dict] = {}
        for r in rows.values():
            if r["state"] == "resolved" and r["repo_id"] and r["kind"] == "repo":
                g = groups.setdefault(int(r["repo_id"]), {"key": str(int(r["repo_id"])), "sources": [], "values": []})
                g["sources"].append(SOURCE_04.get(r["source"], r["source"]))
                g["values"].append(r["raw_value"])
        # a group backed only by the frozen legacy snapshot (old fundamentals.json) is staleness, not a dispute
        groups = {k: g for k, g in groups.items() if set(g["sources"]) != {"legacy"}}
        if len(groups) >= 2:
            ids = sorted(groups)
            pair_hash = hashlib.sha256("|".join(str(i) for i in ids).encode()).hexdigest()[:16]
            payload = {"netuid": occ.netuid, "occupant_key": okey, "field": "github_repo",
                       "groups": [groups[i] for i in sorted(groups, key=lambda i: str(i))],
                       "resolved_source": SOURCE_04.get(declared_src or "", declared_src),
                       "pair_hash": pair_hash, "repo_ids": ids}
            await self._emit(occ, "source_disagreement", payload, pair_hash=pair_hash)

    async def _repo_gone(self, occ: "Occupant", decl: dict, *, reason: str, fallback: int | None) -> None:
        payload = {"repo_id": decl.get("repo_id"), "declared": decl["ref_key"],
                   "source": SOURCE_04.get(decl["source"], decl["source"]), "first_404_at": decl.get("first_404_at"),
                   "consecutive_404": int(decl.get("consecutive_404") or 0), "reason": reason,
                   "fallback_primary_repo_id": fallback}
        await self._emit(occ, "repo_gone", payload, declared=decl["ref_key"])

    async def _emit(self, occ: "Occupant", et: str, payload: dict, **dedupe: Any) -> None:
        ev = await ghevents.emit(self.db, et, occupant_key=occ.occupant_key, netuid=occ.netuid, payload=payload,
                                 now=self.now, run_id=self.run_id, **dedupe)
        if ev:
            self.report.bump(et)

    # ── the run ──────────────────────────────────────────────────────────────────────────────────────────────
    async def run(self, *, netuids: set[int] | None = None, clock: Any = None) -> ResolveReport:
        db = self.db
        rep = self.report
        occs = [o for o in await active_occupants(db, include_pre_launch=True) if o.netuid != 0
                and (netuids is None or o.netuid in netuids)]
        plan: list[tuple["Occupant", dict[str, dict], str | None]] = []
        for occ in occs:
            for r in await db.fetchall("SELECT o.repo_id, g.full_name FROM github_occupant_repos o LEFT JOIN "
                                       "github_repos g USING (repo_id) WHERE o.occupant_key = ? AND o.role IN "
                                       "('declared','declared_gone')", (occ.occupant_key,), writer=True):
                self._prev_declared[occ.occupant_key] = {"repo_id": int(r["repo_id"]), "full_name": r["full_name"]}
            for r in await db.fetchall("SELECT source, state FROM github_declared_refs WHERE occupant_key = ?",
                                       (occ.occupant_key,), writer=True):
                self._prev_states[(occ.occupant_key, r["source"])] = r["state"]
            res = await resolved(db, occ.occupant_key, "github_repo")
            srefs, declared_src = source_refs(res)
            rows = await self.sync_rows(occ, srefs)
            for src in rows:
                self._prev_states.setdefault((occ.occupant_key, src), "new")
            plan.append((occ, rows, declared_src))
        work = [(occ, src, row) for occ, rows, _ in plan for src, row in rows.items()
                if row["kind"] == "repo" and row["state"] not in ("placeholder", "none")]
        work.sort(key=lambda w: (w[2]["last_attempt_at"] is not None, w[2]["last_attempt_at"] or 0))
        rep.refs = sum(len(rows) for _, rows, _ in plan)
        now_fn = clock or self.client.clock
        for occ, src, row in work:
            if rep.deferred or (self.deadline is not None and now_fn() >= self.deadline):
                rep.left += 1
                continue
            try:
                new = await self.resolve_row(row)
            except QuotaDeferred:
                rep.deferred = True
                rep.left += 1
                continue
            rep.attempted += 1
            for o, rows, _ in plan:
                if o is occ:
                    rows[src] = new
        for occ, rows, declared_src in plan:
            if rep.deferred:
                break
            try:
                await self.finalise(occ, rows, declared_src)
            except QuotaDeferred:
                rep.deferred = True
        q: dict[str, int] = {}
        for r in await db.fetchall("SELECT state, COUNT(*) AS n FROM github_declared_refs GROUP BY state"):
            q[r["state"]] = int(r["n"])
        rep.queue = q
        return rep
