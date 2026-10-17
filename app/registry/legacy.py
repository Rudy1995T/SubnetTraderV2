"""Legacy imports (spec 04 §2.11, 02 §J1): the old bot's ``fundamentals.json`` and ``research/subnet_edge/*.md``
become ``source='legacy'`` fallback candidates, point-in-time at their as-of, behind the re-occupancy and name guards."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from app.registry.models import LegacyReport, Occupant
from app.registry.normalise import classify, name_key
from app.registry.observations import ObservationWriter
from app.registry.overrides import add_override

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

LEGACY_REF = "fundamentals.json@2026-06-02"
COLUMN_FIELD = {"github": "github_repo", "website": "subnet_url", "twitter": "x_url", "discord": "discord",
                "telegram": "telegram", "owner": "owner_name_editorial", "short_desc": "short_desc"}
_IGNORED_HOSTS = ("subnetalpha.ai", "taostats.io", "taomarketcap.com", "backprop.finance")
_URL_RE = re.compile(r"https?://[^\s)>\]\"']+")


class OccupancyLookup(Protocol):
    def occupant_at_ts(self, netuid: int, ts: int) -> str | None: ...


def _records(data: dict) -> dict[int, dict]:
    raw = data.get("subnets") or data.get("records") or data
    out = {}
    for k, v in (raw.items() if isinstance(raw, dict) else []):
        if isinstance(v, dict) and str(k).isdigit():
            out[int(k)] = v
    return out


async def _current(db: "Database", netuid: int) -> tuple[Occupant | None, str | None]:
    r = await db.fetchone("SELECT o.* FROM subnets s JOIN occupants o USING (occupant_key) WHERE s.netuid = ?",
                          (netuid,))
    if not r:
        return None, None
    name = await db.fetchval("SELECT value FROM identity_resolved WHERE occupant_key = ? AND field = 'subnet_name' "
                             "AND status = 'present'", (r["occupant_key"],))
    return Occupant.from_row(r), name or r["name"]


def _names_match(a: str | None, b: str | None) -> bool:
    if not a or not b:
        return True
    x, y = name_key(a), name_key(b)
    return x == y or x in y or y in x


async def _import_links(db: "Database", occ: Occupant, links: dict[str, str], *, as_of: int, run_id: str,
                        source_ref: str, status: str, write_overrides: bool, created_by: str,
                        rep: LegacyReport) -> bool:
    w = ObservationWriter(db, run_id, as_of)
    imported = False
    for field, value in links.items():
        refs = classify(value, field)
        if not refs:
            continue
        ref = refs[0]
        if ref.is_junk:
            rep.junk += 1
            continue
        if await w.write(occ, "legacy", field, ref, value, event_at=as_of, block_number=None, source_ref=source_ref):
            rep.observations += 1
        imported = True
        if write_overrides and field != "owner_name_editorial":
            await add_override(db, occupant_key=occ.occupant_key, field=field, mode="fallback", value=ref.canonical,
                               reason=None, evidence_url=None, created_by=created_by, source="legacy",
                               source_ref=source_ref, status=status, created_at=as_of, reresolve=False)
            rep.overrides += 1
    return imported


async def import_fundamentals(db: "Database", path: str | Path, *, as_of: int, run_id: str,
                              occupancy: OccupancyLookup | None, created_by: str = "seed_registry") -> LegacyReport:
    rep = LegacyReport()
    p = Path(path)
    data = json.loads(p.read_text(encoding="utf-8"))
    for netuid, rec in sorted(_records(data).items()):
        cur, cur_name = await _current(db, netuid)
        occ_then: Occupant | None = None
        write_overrides = False
        if occupancy is not None:
            k = occupancy.occupant_at_ts(netuid, as_of)
            if k:
                r = await db.fetchone("SELECT * FROM occupants WHERE occupant_key = ?", (k,))
                occ_then = Occupant.from_row(r) if r else None
                if occ_then:
                    rep.attributed_via_occupancy += 1
                    write_overrides = cur is not None and cur.occupant_key == occ_then.occupant_key
        if occ_then is None:
            if cur is None:
                rep.per_netuid[netuid] = "no_occupant"
                continue
            reg = cur.reg_ts
            if reg is not None and reg > as_of:
                rep.skipped_reoccupied += 1
                rep.per_netuid[netuid] = "skipped_reoccupied"
                continue
            occ_then, write_overrides = cur, True
            rep.attributed_via_guard += 1
        status = "active" if _names_match(rec.get("name"), cur_name) else "needs_review"
        links = {f: str(rec.get(col) or "").strip() for col, f in COLUMN_FIELD.items() if str(rec.get(col) or "").strip()}
        w = ObservationWriter(db, run_id, as_of)
        await w.write_raw("legacy", "subnet", occ_then, rec, source_ref=LEGACY_REF, event_at=as_of, block_number=None)
        ok = await _import_links(db, occ_then, links, as_of=as_of, run_id=run_id, source_ref=LEGACY_REF,
                                 status=status, write_overrides=write_overrides, created_by=created_by, rep=rep)
        if ok:
            rep.imported += 1
            if status == "needs_review":
                rep.needs_review += 1
        rep.per_netuid[netuid] = status if ok else "nothing_importable"
    return rep


async def import_edge_notes(db: "Database", directory: str | Path, *, as_of: int, run_id: str,
                            created_by: str = "seed_registry") -> LegacyReport:
    rep = LegacyReport()
    d = Path(directory)
    if not d.is_dir():
        return rep
    for f in sorted(d.glob("sn*.md")):
        m = re.match(r"^sn(\d+)_", f.name)
        if not m:
            continue
        netuid = int(m.group(1))
        cur, cur_name = await _current(db, netuid)
        if cur is None:
            rep.per_netuid[netuid] = "no_occupant"
            continue
        if cur.reg_ts is not None and cur.reg_ts > as_of:
            rep.skipped_reoccupied += 1
            rep.per_netuid[netuid] = "skipped_reoccupied"
            continue
        text = f.read_text(encoding="utf-8", errors="replace")
        hm = re.search(r"^#\s*SN\d+\s+(.+)$", text, re.M)
        note_name = hm.group(1).strip() if hm else None
        status = "active" if _names_match(note_name, cur_name) else "needs_review"
        links: dict[str, str] = {}
        seen_fields: set[str] = set()
        for url in _URL_RE.findall(text):
            url = url.rstrip(".,;)")
            if any(h in url for h in _IGNORED_HOSTS):
                continue
            refs = classify(url, "subnet_url")
            if not refs:
                continue
            ref = refs[0]
            field = "docs_url" if len(refs) > 1 and refs[-1].kind == "docs" else (ref.target_field or "subnet_url")
            if field in seen_fields or field not in ("github_repo", "x_url", "discord", "telegram", "subnet_url",
                                                     "docs_url"):
                continue
            seen_fields.add(field)
            links[field] = ref.canonical
        source_ref = f"subnet_edge/{f.name}"
        ok = await _import_links(db, cur, {k: v for k, v in links.items() if k != "docs_url"}, as_of=as_of,
                                 run_id=run_id, source_ref=source_ref, status=status, write_overrides=True,
                                 created_by=created_by, rep=rep)
        vm = re.search(r"^\*\*Verdict:\*\*.*$", text, re.M)
        if vm:
            exists = await db.fetchval("SELECT 1 FROM manual_notes WHERE kind = 'seed_edge' AND source_ref = ?",
                                       (source_ref,))
            if not exists:
                await db.execute("INSERT INTO manual_notes (occupant_key, kind, body, source_ref, created_at, "
                                 "created_by) VALUES (?, 'seed_edge', ?, ?, ?, ?)",
                                 (cur.occupant_key, vm.group(0), source_ref, as_of, created_by))
        if ok:
            rep.imported += 1
            if status == "needs_review":
                rep.needs_review += 1
        rep.per_netuid[netuid] = status if ok else "nothing_importable"
    return rep
