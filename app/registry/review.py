"""Computed "needs review" list (spec 04 §2.10); no storage. Rendered by 14's digest (Phase 11)."""
from __future__ import annotations

import json
from typing import TYPE_CHECKING

from app.registry.models import ReviewItem
from app.registry.normalise import name_key, strip_name

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

REASONS = ("new_occupant", "reoccupied_unreviewed", "source_disagreement_open", "abandonment_marker", "name_turnover",
           "identity_void", "chain_identity_missing", "legacy_needs_review", "never_activated",
           "unresolved_watch_target", "leaked_secret")


async def compute_needs_review(db: "Database", now: int) -> list[ReviewItem]:
    out: list[ReviewItem] = []
    occ = {r["occupant_key"]: r for r in await db.fetchall("SELECT * FROM occupants")}
    cur = {r["occupant_key"]: r for r in await db.fetchall(
        "SELECT s.netuid, s.name AS sname, o.* FROM subnets s JOIN occupants o USING (occupant_key)")}
    first_sane = await db.fetchval(
        "SELECT value_json FROM job_state WHERE job = 'registry_occupants' AND key = 'first_sane_at'")
    first_sane = int(json.loads(first_sane)) if first_sane else None

    def name(k: str | None) -> str | None:
        r = occ.get(k) if k else None
        return r["name"] if r else None

    # 1 new occupants
    for k, r in cur.items():
        if first_sane and r["first_seen_at"] > first_sane and r["first_seen_at"] >= now - 7 * 86400:
            out.append(ReviewItem(k, r["netuid"], r["name"], "new_occupant", f"first seen block {r['first_seen_block']}",
                                  r["first_seen_at"]))
    # 2 reoccupied without a review note
    for e in await db.fetchall(
            "SELECT e.* FROM events e WHERE event_type = 'netuid_reoccupied' AND NOT EXISTS (SELECT 1 FROM manual_notes n "
            "WHERE n.ref_event_id = e.id) AND COALESCE(json_extract(payload, '$.backfilled'), 0) = 0"):
        p = json.loads(e["payload"])
        out.append(ReviewItem(e["occupant_key"], e["netuid"], p.get("new_name"), "reoccupied_unreviewed",
                              f"{p.get('old_occupant_key')} → {p.get('new_occupant_key')}", e["observed_at"], e["id"]))
    # 3 open source disagreements (latest per occupant, field)
    for e in await db.fetchall(
            "SELECT e.* FROM events e WHERE event_type = 'source_disagreement' AND id = (SELECT MAX(id) FROM events x "
            "WHERE x.event_type = 'source_disagreement' AND x.occupant_key = e.occupant_key AND "
            "json_extract(x.payload, '$.field') = json_extract(e.payload, '$.field')) AND NOT EXISTS (SELECT 1 FROM "
            "manual_overrides m WHERE m.resolves_event_id = e.id AND m.status = 'active')"):
        if e["occupant_key"] not in cur:
            continue
        p = json.loads(e["payload"])
        vals = " vs ".join("/".join(g.get("values", [])[:1]) for g in p.get("groups", []))
        out.append(ReviewItem(e["occupant_key"], e["netuid"], name(e["occupant_key"]), "source_disagreement_open",
                              f"{p.get('field')}: {vals}", e["observed_at"], e["id"]))
    # 4 abandonment markers on the current name (one item per occupant, whichever source flagged it)
    flagged: set[str] = set()
    for e in await db.fetchall(
            "SELECT * FROM events WHERE event_type = 'identity_junk' AND json_extract(payload, '$.junk_reason') = "
            "'abandonment_marker' AND json_extract(payload, '$.field') = 'subnet_name'"):
        k = e["occupant_key"]
        if k in cur and cur[k]["status"] != "deregistered" and k not in flagged:
            res = await db.fetchval("SELECT value FROM identity_resolved WHERE occupant_key = ? AND field = "
                                    "'subnet_name'", (k,))
            if res is not None and res.strip().casefold() == json.loads(e["payload"]).get("value", "").casefold():
                flagged.add(k)
                out.append(ReviewItem(k, e["netuid"], name(k), "abandonment_marker", f"name {res!r}", e["observed_at"],
                                      e["id"]))
    resolved = {}
    for r in await db.fetchall("SELECT occupant_key, field, value_norm, status, source, candidates_json "
                               "FROM identity_resolved"):
        resolved[(r["occupant_key"], r["field"])] = r
    # 5 name turnover: chain name ≠ subnetalpha name
    for k, r in cur.items():
        rr = resolved.get((k, "subnet_name"))
        if not rr:
            continue
        cands = json.loads(rr["candidates_json"] or "[]")
        chain = next((c["value"] for c in cands if c["source"] == "chain" and c["field"] == "subnet_name"), None)
        sa = next((c["value"] for c in cands if c["source"] == "subnetalpha" and c["field"] == "subnet_name"), None)
        if chain and sa and name_key(chain) != name_key(sa):
            out.append(ReviewItem(k, r["netuid"], r["name"], "name_turnover", f"chain {chain!r} vs subnetalpha {sa!r}",
                                  None))
    # 6 identity void, 7 chain identity missing
    cov = {r["occupant_key"]: r["has_row"] for r in await db.fetchall(
        "SELECT occupant_key, has_row FROM identity_coverage WHERE source = 'chain'")}
    for k, r in cur.items():
        if r["netuid"] == 0 or r["status"] != "active":
            continue
        fields = ("github_repo", "subnet_url", "discord", "x_url")
        if all((resolved.get((k, f)) or {}).get("status") in ("absent", "junk_only") for f in fields):
            out.append(ReviewItem(k, r["netuid"], r["name"], "identity_void", "no repo, site, discord or X", None))
        if cov.get(k) == 0:
            out.append(ReviewItem(k, r["netuid"], r["name"], "chain_identity_missing", "no SubnetIdentitiesV3 row",
                                  None))
    # 8 legacy overrides needing review
    for m in await db.fetchall("SELECT * FROM manual_overrides WHERE status = 'needs_review'"):
        out.append(ReviewItem(m["occupant_key"], occ.get(m["occupant_key"], {}).get("netuid"), name(m["occupant_key"]),
                              "legacy_needs_review", f"{m['field']} = {m['value']} ({m['source_ref']})",
                              m["created_at"]))
    # 9 never activated
    for k, r in cur.items():
        if r["status"] == "pre_launch" and r["first_seen_at"] < now - 120 * 86400:
            out.append(ReviewItem(k, r["netuid"], r["name"], "never_activated", "pre_launch > 120 d", r["first_seen_at"]))
    # 10 unresolved watch targets
    for w in await db.fetchall("SELECT * FROM watch_channels WHERE needs_resolution = 1 AND retired_at IS NULL AND "
                               "first_seen_at < ?", (now - 14 * 86400,)):
        out.append(ReviewItem(w["occupant_key"], w["netuid"], name(w["occupant_key"]), "unresolved_watch_target",
                              f"{w['platform']} {w['handle']}", w["first_seen_at"]))
    # 11 leaked secrets still current on chain
    for e in await db.fetchall("SELECT * FROM events WHERE event_type = 'identity_junk' AND "
                               "json_extract(payload, '$.junk_reason') = 'leaked_secret'"):
        p = json.loads(e["payload"])
        latest = await db.fetchval(
            "SELECT value FROM identity_observations WHERE occupant_key = ? AND source = ? AND field = ? "
            "ORDER BY observed_at DESC, id DESC LIMIT 1", (e["occupant_key"], p.get("source"), p.get("field")))
        if latest == p.get("value"):
            out.append(ReviewItem(e["occupant_key"], e["netuid"], name(e["occupant_key"]), "leaked_secret",
                                  f"{p.get('source')}.{p.get('field')}", e["observed_at"], e["id"]))
    order = {r: i for i, r in enumerate(REASONS)}
    out.sort(key=lambda i: (order.get(i.reason, 99), i.netuid if i.netuid is not None else 9999))
    return out
