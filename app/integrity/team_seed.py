"""``team_seed_daily`` (spec 11 §2.2.4, DB only): subnetalpha ``team_info`` candidates and the one-shot legacy
``seed/subnet_edge`` notes. Seeders create ``candidate`` rows only — they never verify, never touch ``verified_*`` /
``rejected_*`` and never resurrect a rejected row (the unique key blocks re-insert; they only bump
``source_last_seen_at``)."""
from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

import app.config as app_config
from app.integrity.review import enqueue
from app.integrity.team import add_member, reg_ts
from app.integrity.team_parse import classify_profile_url, name_norm, parse_team_info, valid_url
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.storage.events import emit_event
from app.util.time import parse_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

DAY = 86400
_FOUNDED = re.compile(r"Founded by ([A-Z][\w .'-]{1,60})")
_ROLE = re.compile(r"\b(CEO|CTO|co-?founder|founder|lead)\b[:\s]+([A-Z][\w .'-]{1,60})")
_ANON = re.compile(r"(?i)\banonymous team\b")
_FILE = re.compile(r"^sn(\d+)_.+\.md$")


def _trim(name: str) -> str:
    return re.sub(r"[\s.,;:'\-]+$", "", name.strip())


async def _occupants(db: "Database") -> list[dict]:
    return await db.fetchall("SELECT o.* FROM subnets s JOIN occupants o USING (occupant_key) WHERE o.status <> "
                             "'deregistered' AND s.netuid <> 0 ORDER BY s.netuid", writer=True)


async def seed_subnetalpha(db: "Database", *, now: int, run_id: str | None = None, dry_run: bool = False) -> dict:
    s = app_config.settings
    out = {"occupants": 0, "new": 0, "seen": 0, "delisted": 0, "disclosure": {}, "candidates": []}
    for occ in await _occupants(db):
        okey = occ["occupant_key"]
        out["occupants"] += 1
        obs = await db.fetchall("SELECT id, value, event_at, source_ref FROM identity_observations WHERE "
                                "occupant_key = ? AND source = 'subnetalpha' AND field = 'team_info_html' "
                                "ORDER BY observed_at DESC, id DESC LIMIT 2", (okey,), writer=True)
        if not obs:
            out["disclosure"]["no_record"] = out["disclosure"].get("no_record", 0) + 1
            continue
        cur = obs[0]
        rt = reg_ts(occ)
        if cur["event_at"] is not None and rt is not None and int(cur["event_at"]) < rt - 2 * DAY:
            out["disclosure"]["stale_for_occupant"] = out["disclosure"].get("stale_for_occupant", 0) + 1
            continue
        kw = {"max_tokens": int(s.TEAM_NAME_MAX_TOKENS), "anon_max_chars": int(s.TEAM_ANON_MAX_CHARS)}
        p = parse_team_info(cur["value"] or "", **kw)
        out["disclosure"][p.disclosure] = out["disclosure"].get(p.disclosure, 0) + 1
        new_ids: list[int] = []
        for c in p.candidates:
            out["candidates"].append((occ["netuid"], c.name, c.role))
            nn = name_norm(c.name)
            row = await db.fetchone("SELECT id, status FROM team_members WHERE occupant_key = ? AND name_norm = ?",
                                    (okey, nn), writer=True)
            if dry_run:
                out["new" if row is None else "seen"] += 1
                continue
            if row is None:
                res = await add_member(db, occupant_key=okey, name=c.name[:120], role=(c.role or None) and c.role[:60],
                                       profile_url=c.profile_url if valid_url(c.profile_url) else None,
                                       evidence_url=None,
                                       evidence_note="subnetalpha team_info (unverified seed)", source="subnetalpha",
                                       created_by="seed_subnetalpha", source_ref=f"{cur['source_ref']}@{cur['event_at']}",
                                       now=now)
                if res.ok and res.id:
                    new_ids.append(res.id)
                    out["new"] += 1
                continue
            out["seen"] += 1
            if row["status"] == "candidate":
                await db.execute("UPDATE team_members SET source_last_seen_at = ?, "
                                 "profile_url = COALESCE(profile_url, ?), profile_kind = COALESCE(profile_kind, ?), "
                                 "role = COALESCE(role, ?) WHERE id = ?",
                                 (now, c.profile_url, classify_profile_url(c.profile_url) if c.profile_url else None,
                                  (c.role or None) and c.role[:60], row["id"]))
            else:
                await db.execute("UPDATE team_members SET source_last_seen_at = ? WHERE id = ?", (now, row["id"]))
        if dry_run:
            continue
        if new_ids:
            await enqueue(db, occupant_key=okey, kind="team_candidate", dedupe_base=f"team_candidate|{okey}",
                          ref_table="team_members", ref_id=new_ids[0], priority=3,
                          summary=f"{len(new_ids)} unverified team candidate(s) on SN{occ['netuid']}",
                          payload={"n_new": len(new_ids), "member_ids": new_ids}, now=now)
        if p.disclosure == "named":
            names = {name_norm(c.name) for c in p.candidates}
            gone = await db.fetchall("SELECT * FROM team_members WHERE occupant_key = ? AND status = 'verified' AND "
                                     "source = 'subnetalpha'", (okey,), writer=True)
            for m in gone:
                if m["name_norm"] in names:
                    continue
                eid = await emit_event(db, event_type="team_member_delisted", occupant_key=okey, netuid=occ["netuid"],
                                       event_at=now, observed_at=now,
                                       payload={"member_id": m["id"], "name": m["name"], "role": m["role"],
                                                "source": m["source"], "status": m["status"], "by": "seed_subnetalpha"},
                                       dedupe_key=f"team|{m['id']}|team_member_delisted|{now // DAY}",
                                       source="integrity", run_id=run_id)
                await enqueue(db, occupant_key=okey, kind="team_delisted", dedupe_base=f"team_delisted|{m['id']}",
                              ref_table="team_members", ref_id=m["id"], priority=2,
                              summary=f"{m['name']} no longer listed on SN{occ['netuid']}", payload={"event_id": eid},
                              now=now)
                out["delisted"] += 1
        if len(obs) > 1:
            old = parse_team_info(obs[1]["value"] or "", **kw).disclosure
            if old != p.disclosure:
                await emit_event(db, event_type="team_disclosure_changed", occupant_key=okey, netuid=occ["netuid"],
                                 event_at=int(cur["event_at"] or now), observed_at=now,
                                 payload={"old": old, "new": p.disclosure, "source": "subnetalpha"},
                                 dedupe_key=f"{okey}|disc|{p.disclosure}|{now // DAY}", source="integrity",
                                 run_id=run_id)
    return out


async def seed_legacy(db: "Database", notes_dir: str | Path, *, as_of: int, now: int,
                      dry_run: bool = False) -> dict:
    """``seed/subnet_edge/sn{NN}_*.md`` → candidates (``source='legacy'``) and at most one anonymity note per file;
    skipped when the current occupant registered after ``as_of`` (04 §2.11 re-occupancy guard)."""
    d = Path(notes_dir)
    out = {"files": 0, "candidates": [], "notes": [], "skipped": []}
    if not d.is_dir():
        out["error"] = f"no notes dir {d}"
        return out
    for f in sorted(d.glob("sn*.md")):
        m = _FILE.match(f.name)
        if not m or f.name.startswith("_") or f.name == "README.md":
            continue
        out["files"] += 1
        netuid = int(m.group(1))
        occ = await db.fetchone("SELECT o.* FROM subnets s JOIN occupants o USING (occupant_key) WHERE s.netuid = ? "
                                "AND o.status <> 'deregistered'", (netuid,), writer=True)
        if occ is None:
            out["skipped"].append((f.name, "no_current_occupant"))
            continue
        rt = reg_ts(occ)
        if rt is not None and rt > as_of:
            out["skipped"].append((f.name, "reoccupied_after_as_of"))
            continue
        anon_done = False
        for i, line in enumerate(f.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
            found: list[tuple[str, str]] = []
            for mm in _FOUNDED.finditer(line):
                found.append((_trim(mm.group(1)), "founder"))
            for mm in _ROLE.finditer(line):
                found.append((_trim(mm.group(2)), mm.group(1)))
            for name, role in found:
                if not name:
                    continue
                ref = f"{f.name}:{i}"
                out["candidates"].append((netuid, name, role, ref))
                if not dry_run:
                    await add_member(db, occupant_key=occ["occupant_key"], name=name, role=role, profile_url=None,
                                     evidence_url=None, evidence_note="legacy edge note (unverified seed)",
                                     source="legacy", created_by="seed_legacy", source_ref=ref, now=now)
            if not anon_done and _ANON.search(line):
                anon_done = True
                out["notes"].append((netuid, f"{f.name}:{i}"))
                if not dry_run:
                    exists = await db.fetchval("SELECT 1 FROM manual_notes WHERE occupant_key = ? AND kind = "
                                               "'team_disclosure' AND source_ref = ?",
                                               (occ["occupant_key"], f"{f.name}:{i}"), writer=True)
                    if not exists:
                        await db.execute(
                            "INSERT INTO manual_notes (occupant_key, kind, value, body, evidence_url, source_ref, "
                            "created_at, created_by) VALUES (?, 'team_disclosure', 'anonymous', ?, ?, ?, ?, "
                            "'seed_legacy')", (occ["occupant_key"], line.strip()[:500],
                                               f"file:seed/subnet_edge/{f.name}#L{i}", f"{f.name}:{i}", now))
    return out


def legacy_as_of() -> int:
    s = app_config.settings
    return parse_iso(s.TEAM_LEGACY_AS_OF or s.LEGACY_AS_OF)


class TeamSeedJob(CollectorJob):
    _last: dict | None = None

    async def collect(self, run: RunContext) -> JobOutcome:
        s = run.settings
        db = run.rt.db
        out: dict = {}
        if not s.TEAM_ENABLED:
            return JobOutcome(rows=0, note="TEAM_ENABLED=false")
        if s.TEAM_SEED_SUBNETALPHA_ENABLED:
            sa = await seed_subnetalpha(db, now=run.started_at, run_id=run.run_id)
            sa.pop("candidates", None)
            out["subnetalpha"] = sa
        if s.TEAM_SEED_LEGACY_ENABLED and not await self.get_state("legacy_done"):
            lg = await seed_legacy(db, s.path(s.TEAM_LEGACY_NOTES_DIR), as_of=legacy_as_of(), now=run.started_at)
            out["legacy"] = {"files": lg["files"], "candidates": len(lg["candidates"]), "notes": len(lg["notes"]),
                             "error": lg.get("error")}
            if not lg.get("error"):
                await self.set_state("legacy_done", {"at": run.started_at, **out["legacy"]})
        self._last = out
        sa = out.get("subnetalpha") or {}
        return JobOutcome(rows=sa.get("new", 0) + (out.get("legacy") or {}).get("candidates", 0),
                          note=f"new={sa.get('new', 0)} seen={sa.get('seen', 0)} delisted={sa.get('delisted', 0)} "
                               f"disclosure={sa.get('disclosure')}", meta=out)

    def health_extra(self) -> dict | None:
        return self._last
