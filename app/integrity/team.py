"""Team evidence domain functions (spec 11 §2.2, §2.12): add / verify / reject / disclosure / list, the PIT team state
and ``team_doxxed``. Never raise on user error — every function returns an ``ActionResult``. Verification is always a
human act: seeders only create ``candidate`` rows, and no profile URL is ever fetched (master §1.2)."""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal

import app.config as app_config
from app.integrity.models import ActionResult, RawMetric, TeamMemberState, absent, err, failed, ok
from app.integrity.team_parse import (check_two_source, classify_profile_url, name_norm, parse_team_info,
                                      registrable_domain, valid_url)
from app.storage.events import emit_event
from app.util.time import now_ts, parse_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

DAY = 86400
UNKNOWN_REASONS = ("no_record", "empty", "placeholder", "unstructured", "stale_for_occupant", "all_rejected",
                   "not_seeded")
_LEGACY_OK = {"empty", "placeholder", "unstructured", "no_record", "stale_for_occupant"}


# ── helpers ──────────────────────────────────────────────────────────────────────────────────────────────────
async def resolve_occupant(db: "Database", *, netuid: int | None = None,
                           occupant_key: str | None = None) -> dict | None:
    if occupant_key:
        return await db.fetchone("SELECT * FROM occupants WHERE occupant_key = ? AND status <> 'deregistered'",
                                 (occupant_key,), writer=True)
    if netuid is None:
        return None
    return await db.fetchone("SELECT o.* FROM subnets s JOIN occupants o USING (occupant_key) WHERE s.netuid = ? "
                             "AND o.status <> 'deregistered'", (int(netuid),), writer=True)


def reg_ts(occ: dict) -> int | None:
    v = occ.get("registered_ts")
    return int(v) if v is not None else (int(occ["registered_at_est"]) if occ.get("registered_at_est") else None)


async def occupant_handles(db: "Database", occupant_key: str) -> tuple[set[str], set[str]]:
    """(org_handles, x_handles) from 04's resolved github_repo / x_url — the occupant's own accounts count as
    attestation, never as a personal profile."""
    from app.registry import resolved
    orgs: set[str] = set()
    xs: set[str] = set()
    try:
        gh = await resolved(db, occupant_key, "github_repo")
        x = await resolved(db, occupant_key, "x_url")
    except Exception:
        return orgs, xs
    for r in (gh,):
        if r and r.value:
            m = re.search(r"github\.com/([A-Za-z0-9-]{1,39})", r.value) or re.match(r"^([A-Za-z0-9-]{1,39})/", r.value)
            if m:
                orgs.add(m.group(1).lower())
    if x and x.value:
        m = re.search(r"(?:x|twitter)\.com/([A-Za-z0-9_]{1,15})", x.value) or re.match(r"^@?([A-Za-z0-9_]{1,15})$",
                                                                                         x.value.strip())
        if m:
            xs.add(m.group(1).lower())
    return orgs, xs


def _settings():
    return app_config.settings


def _items(row: dict) -> list[dict]:
    try:
        return list(json.loads(row.get("evidence_json") or "[]"))
    except ValueError:
        return []


async def _emit_team(db: "Database", event_type: str, member: dict, *, by: str, now: int,
                     checked_at: int | None = None, **extra: Any) -> int | None:
    payload = {"member_id": member["id"], "name": member["name"], "role": member.get("role"),
               "source": member["source"], "status": member["status"], "by": by, **extra}
    if checked_at is not None:
        payload["checked_at"] = checked_at
    bucket = checked_at if checked_at is not None else now // DAY
    return await emit_event(db, event_type=event_type, occupant_key=member["occupant_key"], netuid=member["netuid"],
                            event_at=now, observed_at=now, payload=payload, source="integrity", run_id=None,
                            dedupe_key=f"team|{member['id']}|{event_type}|{bucket}")


async def _member(db: "Database", member_id: int) -> dict | None:
    return await db.fetchone("SELECT * FROM team_members WHERE id = ?", (int(member_id),), writer=True)


def _attestation_notes(items: list[dict]) -> list[str]:
    return [i["note"] for i in items if i.get("class") == "attestation" and i.get("url") is None and i.get("note")]


# ── domain functions ─────────────────────────────────────────────────────────────────────────────────────────
async def add_member(db: "Database", *, netuid: int | None = None, occupant_key: str | None = None, name: str,
                     role: str | None, profile_url: str | None, evidence_url: str | None, evidence_note: str | None,
                     source: str, created_by: str, verify: bool = False, source_ref: str | None = None,
                     now: int | None = None) -> ActionResult:
    now = int(now if now is not None else now_ts())
    occ = await resolve_occupant(db, netuid=netuid, occupant_key=occupant_key)
    if occ is None or occ["netuid"] == 0:
        return err("no_current_occupant")
    name = (name or "").strip()
    if not (1 <= len(name) <= 120) or (role and len(role) > 60) or not name_norm(name):
        return err("invalid_transition", occupant_key=occ["occupant_key"], warning="name 1..120 / role <= 60 chars")
    for u in (profile_url, evidence_url):
        if u and not valid_url(u):
            return err("invalid_url", occupant_key=occ["occupant_key"])
    nn = name_norm(name)
    dup = await db.fetchone("SELECT id, status FROM team_members WHERE occupant_key = ? AND name_norm = ?",
                            (occ["occupant_key"], nn), writer=True)
    if dup:
        return ActionResult(False, id=dup["id"], occupant_key=occ["occupant_key"], status=dup["status"],
                            error="duplicate_member")
    s = _settings()
    orgs, xs = await occupant_handles(db, occ["occupant_key"])
    notes = [evidence_note] if (verify and evidence_note and source == "manual") else []
    res = check_two_source(profile_url, [evidence_url] if evidence_url else [], attestation_notes=notes,
                           org_handles=orgs, x_handles=xs, excluded=set(s.TEAM_EVIDENCE_EXCLUDED_DOMAINS),
                           min_sources=int(s.TEAM_VERIFY_MIN_SOURCES))
    items = [dict(i, added_by=created_by, added_at=now) for i in res.items]
    status = "verified" if (verify and res.ok) else "candidate"
    first_url = evidence_url or next((i["url"] for i in items if i.get("url") and i["class"] != "excluded"), None)
    row = {"occupant_key": occ["occupant_key"], "netuid": occ["netuid"], "name": name, "name_norm": nn,
           "role": (role or None), "profile_url": profile_url,
           "profile_kind": classify_profile_url(profile_url) if profile_url else None,
           "evidence_url": evidence_url if status == "candidate" else first_url, "evidence_note": evidence_note,
           "evidence_json": json.dumps(items), "evidence_count": res.evidence_count, "status": status,
           "source": source, "source_ref": source_ref or created_by, "first_seen_at": now,
           "created_by": created_by, "source_last_seen_at": now if source in ("subnetalpha", "legacy") else None}
    if status == "verified":
        row.update(verified_by=created_by, verified_at=now, last_checked_at=now)
    async with db.transaction():
        cols = list(row)
        r = await db.execute(f"INSERT INTO team_members ({', '.join(cols)}) VALUES ({','.join('?' * len(cols))})",
                             [row[c] for c in cols])
        mid = int(r.lastrowid)
        member = dict(row, id=mid)
        eid = await _emit_team(db, "team_member_added", dict(member, status="candidate"), by=created_by, now=now)
        if status == "verified":
            eid = await _emit_team(db, "team_member_verified", member, by=created_by, now=now, checked_at=now,
                                   evidence_count=res.evidence_count, domains=list(res.domains))
    warning = "two_source_rule" if (verify and not res.ok) else None
    return ActionResult(True, id=mid, occupant_key=occ["occupant_key"], status=status, warning=warning,
                        event_id=eid)


async def verify_member(db: "Database", member_id: int, *, evidence_urls: list[str], note: str | None, by: str,
                        now: int | None = None) -> ActionResult:
    now = int(now if now is not None else now_ts())
    m = await _member(db, member_id)
    if m is None:
        return err("not_found")
    evidence_urls = [u.strip() for u in (evidence_urls or []) if u and u.strip()]
    if any(not valid_url(u) for u in evidence_urls):
        return err("invalid_url", id=m["id"], occupant_key=m["occupant_key"], status=m["status"])
    note = (note or "").strip() or None
    if not evidence_urls and not (note and 10 <= len(note) <= 300):
        return err("evidence_required", id=m["id"], occupant_key=m["occupant_key"], status=m["status"])
    s = _settings()
    items = _items(m)
    known = {i.get("url") for i in items if i.get("url")}
    new_urls = [u for u in evidence_urls if u not in known]
    stored_urls = [i["url"] for i in items if i.get("url") and i["url"] != m.get("profile_url")]
    notes = _attestation_notes(items) + ([note] if note else [])
    orgs, xs = await occupant_handles(db, m["occupant_key"])
    res = check_two_source(m.get("profile_url"), stored_urls + new_urls, attestation_notes=notes, org_handles=orgs,
                           x_handles=xs, excluded=set(s.TEAM_EVIDENCE_EXCLUDED_DOMAINS),
                           min_sources=int(s.TEAM_VERIFY_MIN_SOURCES))
    for i in res.items:                                   # append the genuinely new items (urls and the note)
        if (i.get("url") and i["url"] in new_urls) or (i.get("url") is None and note and i.get("note") == note
                                                       and note not in _attestation_notes(items)):
            items.append(dict(i, added_by=by, added_at=now))
    if m.get("profile_url") and not any(i.get("url") == m["profile_url"] for i in items):
        items.insert(0, {"url": m["profile_url"], "domain": registrable_domain(m["profile_url"]),
                         "class": next((i["class"] for i in res.items if i.get("url") == m["profile_url"]),
                                       "personal"), "added_by": m["created_by"], "added_at": m["first_seen_at"]})
    if not res.ok:
        await db.execute("UPDATE team_members SET evidence_json = ?, evidence_count = ? WHERE id = ?",
                         (json.dumps(items), res.evidence_count, m["id"]))
        return ActionResult(False, id=m["id"], occupant_key=m["occupant_key"], status=m["status"],
                            error="two_source_rule", warning=res.reason)
    first_url = m.get("evidence_url") or next((i["url"] for i in items if i.get("url") and i["class"] != "excluded"),
                                              None)
    async with db.transaction():
        await db.execute(
            "UPDATE team_members SET status = 'verified', evidence_json = ?, evidence_count = ?, evidence_url = ?, "
            "verified_at = COALESCE(verified_at, ?), verified_by = COALESCE(verified_by, ?), last_checked_at = ? "
            "WHERE id = ?", (json.dumps(items), res.evidence_count, first_url, now, by, now, m["id"]))
        m2 = await _member(db, m["id"])
        eid = await _emit_team(db, "team_member_verified", m2, by=by, now=now, checked_at=now,
                               evidence_count=res.evidence_count, domains=list(res.domains))
        await db.execute("UPDATE review_queue SET status = 'expired', resolution_action = 'auto_cleared', "
                         "resolved_at = ?, resolved_by = 'system', resolution_note = 're-verified' "
                         "WHERE kind = 'team_reverify' AND status = 'open' AND ref_table = 'team_members' "
                         "AND ref_id = ?", (now, m["id"]))
    return ActionResult(True, id=m["id"], occupant_key=m["occupant_key"], status="verified", event_id=eid,
                        extra={"evidence_count": res.evidence_count, "domains": list(res.domains)})


async def reject_member(db: "Database", member_id: int, *, reason: str, evidence_url: str | None, by: str,
                        now: int | None = None) -> ActionResult:
    now = int(now if now is not None else now_ts())
    m = await _member(db, member_id)
    if m is None:
        return err("not_found")
    reason = (reason or "").strip()
    if not (1 <= len(reason) <= 300):
        return err("reason_required", id=m["id"], occupant_key=m["occupant_key"], status=m["status"])
    if evidence_url and not valid_url(evidence_url):
        return err("invalid_url", id=m["id"], occupant_key=m["occupant_key"], status=m["status"])
    if m["status"] == "rejected":
        return err("invalid_transition", id=m["id"], occupant_key=m["occupant_key"], status=m["status"])
    items = _items(m)
    if evidence_url:
        items.append({"url": evidence_url, "domain": registrable_domain(evidence_url), "class": "rejection",
                      "added_by": by, "added_at": now})
    async with db.transaction():
        await db.execute("UPDATE team_members SET status = 'rejected', rejected_by = ?, rejected_at = ?, "
                         "reject_reason = ?, evidence_json = ? WHERE id = ?",
                         (by, now, reason, json.dumps(items), m["id"]))
        m2 = await _member(db, m["id"])
        eid = await _emit_team(db, "team_member_rejected", m2, by=by, now=now, reason=reason)
    return ActionResult(True, id=m["id"], occupant_key=m["occupant_key"], status="rejected", event_id=eid)


async def set_disclosure(db: "Database", *, netuid: int, status: Literal["anonymous", "clear"], evidence_url: str,
                         note: str | None, by: str, now: int | None = None) -> ActionResult:
    now = int(now if now is not None else now_ts())
    occ = await resolve_occupant(db, netuid=netuid)
    if occ is None or occ["netuid"] == 0:
        return err("no_current_occupant")
    if status not in ("anonymous", "clear"):
        return err("invalid_transition", occupant_key=occ["occupant_key"])
    if not evidence_url:
        return err("evidence_required", occupant_key=occ["occupant_key"])
    if not valid_url(evidence_url):
        return err("invalid_url", occupant_key=occ["occupant_key"])
    old = await db.fetchval("SELECT value FROM manual_notes WHERE occupant_key = ? AND kind = 'team_disclosure' "
                            "ORDER BY created_at DESC, id DESC LIMIT 1", (occ["occupant_key"],), writer=True)
    async with db.transaction():
        r = await db.execute("INSERT INTO manual_notes (occupant_key, kind, value, body, evidence_url, created_at, "
                             "created_by) VALUES (?, 'team_disclosure', ?, ?, ?, ?, ?)",
                             (occ["occupant_key"], status, note, evidence_url, now, by))
        eid = await emit_event(db, event_type="team_disclosure_changed", occupant_key=occ["occupant_key"],
                               netuid=occ["netuid"], event_at=now, observed_at=now,
                               payload={"old": old, "new": status, "source": "manual", "by": by},
                               dedupe_key=f"{occ['occupant_key']}|disc|{status}|{now // DAY}", source="integrity",
                               run_id=None)
    return ActionResult(True, id=int(r.lastrowid), occupant_key=occ["occupant_key"], status=status, event_id=eid)


async def list_members(db: "Database", *, netuid: int | None = None, occupant_key: str | None = None,
                       status: str | None = None, stale_only: bool = False, now: int | None = None) -> list[dict]:
    now = int(now if now is not None else now_ts())
    where, args = ["1 = 1"], []
    if occupant_key:
        where.append("occupant_key = ?")
        args.append(occupant_key)
    elif netuid is not None:
        occ = await resolve_occupant(db, netuid=netuid)
        if occ is None:
            return []
        where.append("occupant_key = ?")
        args.append(occ["occupant_key"])
    if status:
        where.append("status = ?")
        args.append(status)
    if stale_only:
        where.append("status = 'verified' AND last_checked_at < ?")
        args.append(now - int(_settings().TEAM_REVERIFY_DAYS) * DAY)
    rows = await db.fetchall("SELECT id, occupant_key, netuid, name, role, status, source, profile_url, evidence_url, "
                             "evidence_count, verified_at, last_checked_at, rejected_at, reject_reason, first_seen_at "
                             f"FROM team_members WHERE {' AND '.join(where)} ORDER BY netuid, status, name", args,
                             writer=True)
    return rows


# ── PIT state and team_doxxed ────────────────────────────────────────────────────────────────────────────────
async def team_state_as_of(db: "Database", occupant_key: str, t: int | datetime) -> list[TeamMemberState]:
    t = parse_ts(t)
    members = await db.fetchall("SELECT id, name, source, status FROM team_members WHERE occupant_key = ? "
                                "AND first_seen_at <= ?", (occupant_key, t), writer=True)
    evs = await db.fetchall(
        "SELECT event_type, event_at, id, json_extract(payload, '$.member_id') AS mid, "
        "json_extract(payload, '$.checked_at') AS checked_at FROM events WHERE occupant_key = ? "
        "AND event_type IN ('team_member_verified', 'team_member_rejected') AND event_at <= ? AND observed_at <= ? "
        "ORDER BY event_at, id", (occupant_key, t, t), writer=True)
    last: dict[int, tuple[str, int | None]] = {}
    for e in evs:
        if e["mid"] is None:
            continue
        mid = int(e["mid"])
        if e["event_type"] == "team_member_verified":
            last[mid] = ("verified", int(e["checked_at"] if e["checked_at"] is not None else e["event_at"]))
        else:
            last[mid] = ("rejected", None)
    out = []
    for m in members:
        st, ct = last.get(int(m["id"]), ("candidate", None))
        out.append(TeamMemberState(int(m["id"]), m["name"], st, m["source"], ct))
    return out


async def disclosure_as_of(db: "Database", occ: dict, t: int) -> tuple[str, str, bool]:
    """(disclosure, disclosure_source, anon_marker) as of t (§2.2.6)."""
    okey = occ["occupant_key"]
    notes = await db.fetchall("SELECT value, created_by, created_at FROM manual_notes WHERE occupant_key = ? "
                              "AND kind = 'team_disclosure' AND created_at <= ? ORDER BY created_at DESC, id DESC",
                              (okey, t), writer=True)
    manual = next((n for n in notes if n["created_by"] != "seed_legacy"), None)
    legacy = next((n for n in notes if n["created_by"] == "seed_legacy"), None)
    if manual and manual["value"] == "anonymous":
        return "anonymous", "manual", True
    if legacy and manual and manual["value"] == "clear" and manual["created_at"] >= legacy["created_at"]:
        legacy = None                                     # a later manual 'clear' cancels the legacy assertion
    obs = await db.fetchone("SELECT value, event_at FROM identity_observations WHERE occupant_key = ? "
                            "AND source = 'subnetalpha' AND field = 'team_info_html' AND observed_at <= ? "
                            "ORDER BY observed_at DESC, id DESC LIMIT 1", (okey, t), writer=True)
    anon_marker = False
    if obs is None:
        disc = "no_record"
    else:
        rt = reg_ts(occ)
        if obs["event_at"] is not None and rt is not None and int(obs["event_at"]) < rt - 2 * DAY:
            disc = "stale_for_occupant"
        else:
            s = _settings()
            p = parse_team_info(obs["value"] or "", max_tokens=int(s.TEAM_NAME_MAX_TOKENS),
                                anon_max_chars=int(s.TEAM_ANON_MAX_CHARS))
            disc, anon_marker = p.disclosure, p.anon_marker
    if legacy and legacy["value"] == "anonymous" and disc in _LEGACY_OK:
        return "anonymous", "legacy", True
    return disc, "subnetalpha", anon_marker


async def compute_team_doxxed(db: "Database", occupant_key: str, t: int | datetime) -> RawMetric:
    t = parse_ts(t)
    occ = await db.fetchone("SELECT * FROM occupants WHERE occupant_key = ?", (occupant_key,), writer=True)
    if occ is None:
        return failed("team_doxxed", "no_record", n_verified=0, n_named=0, disclosure="no_record")
    stale_s = int(_settings().TEAM_VERIFY_STALE_DAYS) * DAY
    states = await team_state_as_of(db, occupant_key, t)
    V = sum(1 for m in states if m.status == "verified" and m.check_time is not None and m.check_time >= t - stale_s)
    Vstale = sum(1 for m in states if m.status == "verified" and (m.check_time is None or m.check_time < t - stale_s))
    C = sum(1 for m in states if m.status == "candidate") + Vstale
    rejected = sum(1 for m in states if m.status == "rejected")
    D, dsrc, anon_marker = await disclosure_as_of(db, occ, t)
    vj = {"n_verified": V, "n_named": C, "disclosure": D, "stale_verified": Vstale, "rejected": rejected,
          "disclosure_source": dsrc, "anon_marker": anon_marker, "member_ids": sorted(m.member_id for m in states)}
    if V >= 2:
        return ok("team_doxxed", 1.0, **vj)
    if V == 1:
        return ok("team_doxxed", 0.6, **vj)
    if C >= 1:
        return ok("team_doxxed", 0.3, **vj)
    if D == "anonymous":
        return absent("team_doxxed", "anonymous", **vj)
    if rejected and D not in ("empty", "placeholder", "no_record", "stale_for_occupant"):
        reason = "all_rejected"                           # named text, every candidate rejected
    elif D == "named":
        reason = "not_seeded"                             # named text whose candidates team_seed has not stored yet
    else:
        reason = D if D in UNKNOWN_REASONS else "unstructured"
    return failed("team_doxxed", reason, **vj)


async def due_reverify(db: "Database", now: int | datetime) -> list[int]:
    now = parse_ts(now)
    cut = now - int(_settings().TEAM_REVERIFY_DAYS) * DAY
    rows = await db.fetchall("SELECT id FROM team_members WHERE status = 'verified' AND last_checked_at < ? "
                             "ORDER BY id", (cut,), writer=True)
    return [int(r["id"]) for r in rows]
