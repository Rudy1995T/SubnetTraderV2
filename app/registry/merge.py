"""Per-field identity merge R1–R7 (spec 04 §2.7) and point-in-time replay (§3.3 ``resolved``).

Deliberate reading (BUILD-LOG): R6 disagreements compare each source's **primary** value of the field only (not
``__extra_*`` rows or mapped owner alternates), so an owner's company URL never "disagrees" with the subnet URL.
"""
from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Iterable

from app.registry.models import Candidate, Occupant, Resolved
from app.registry.normalise import classify, compare_key
from app.storage.events import emit_event

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

RESOLVED_FIELDS = ("subnet_name", "github_repo", "subnet_url", "discord", "x_url", "telegram", "docs_url",
                   "description", "short_desc", "logo_url", "subnet_contact", "owner_name", "owner_url", "owner_github",
                   "owner_discord", "youtube")
ALTERNATES: dict[str, tuple[str, ...]] = {
    "subnet_url": ("owner_url",), "github_repo": ("owner_github",), "discord": ("owner_discord",),
    "youtube": tuple(f"video_url_{i}" for i in range(9)),
}
SOURCE_RANK = {"chain": 0, "subnetalpha": 1, "tmc_coldkey": 2, "tmc": 3, "manual": 4, "legacy": 5}
DEMOTED_RANK = 6
# which sources can cover a field (R5); None = derived
FIELD_SOURCES: dict[str, tuple[str, ...]] = {
    "github_repo": ("chain", "subnetalpha", "tmc_coldkey"), "subnet_url": ("chain", "subnetalpha", "tmc_coldkey"),
    "discord": ("chain", "subnetalpha", "tmc_coldkey"), "x_url": ("subnetalpha",), "telegram": ("subnetalpha",),
    "youtube": ("subnetalpha",), "subnet_name": ("chain", "subnetalpha", "tmc"), "description": ("chain",),
    "logo_url": ("chain",), "subnet_contact": ("chain",), "short_desc": ("subnetalpha",),
    "owner_name": ("tmc_coldkey",), "owner_url": ("tmc_coldkey",), "owner_github": ("tmc_coldkey",),
    "owner_discord": ("tmc_coldkey",),
}
WATCH_PRIORITY = {"chain": 10, "subnetalpha": 20, "tmc": 30, "tmc_coldkey": 30, "manual": 40, "legacy": 40}


def _latest_by_key(rows: Iterable[dict]) -> dict[tuple[str, str], dict]:
    out: dict[tuple[str, str], dict] = {}
    for r in rows:
        k = (r["source"], r["field"])
        cur = out.get(k)
        if cur is None or (r["observed_at"], r["id"]) > (cur["observed_at"], cur["id"]):
            out[k] = r
    return out


async def load_observations(db: "Database", occ_key: str, as_of: int | None = None) -> list[dict]:
    if as_of is None:
        return await db.fetchall("SELECT * FROM identity_observations WHERE occupant_key = ?", (occ_key,))
    return await db.fetchall("SELECT * FROM identity_observations WHERE occupant_key = ? AND observed_at <= ?",
                             (occ_key, int(as_of)))


async def identity_epoch(db: "Database", occupant_key: str, t: int, *, occ: Occupant | None = None,
                         rows: list[dict] | None = None) -> int:
    if occ is None:
        r = await db.fetchone("SELECT * FROM occupants WHERE occupant_key = ?", (occupant_key,))
        occ = Occupant.from_row(r) if r else None
    epoch = (occ.reg_ts if occ else None) or 0
    if rows is None:
        rows = await db.fetchall(
            "SELECT * FROM identity_observations WHERE occupant_key = ? AND observed_at <= ? AND "
            "((source = 'chain' AND field = 'subnet_name') OR (source = 'tmc' AND field = 'owner_coldkey'))",
            (occupant_key, int(t)))
    for key in (("chain", "subnet_name"), ("tmc", "owner_coldkey")):
        seq = sorted((r for r in rows if (r["source"], r["field"]) == key and r["observed_at"] <= t),
                     key=lambda r: (r["observed_at"], r["id"]))
        for prev, cur in zip(seq, seq[1:]):
            if cur["value_hash"] != prev["value_hash"]:
                epoch = max(epoch, cur["observed_at"])
    return int(epoch)


def _cand(r: dict, order: int) -> Candidate:
    return Candidate(r["source"], r["field"], r["value"] or "", r["value_norm"], r["ref_kind"], r["value_hash"],
                     bool(r["is_junk"]), r["junk_reason"], r["observed_at"], r["event_at"], r["source_ref"], order)


def _override_cand(o: dict, field: str) -> Candidate:
    refs = classify(o["value"] or "", field)
    ref = refs[0] if refs else None
    src = "legacy" if o["source"] == "legacy" else "manual"
    c = Candidate(src, field, o["value"] or "", ref.canonical if ref else o["value"], ref.kind if ref else None,
                  o["value_hash"] or hashlib.sha256((o["value"] or "").strip().encode()).hexdigest(),
                  bool(ref and ref.is_junk), ref.junk_reason if ref else None, o["created_at"], o["created_at"],
                  o["source_ref"], 0, override_id=o["id"], mode=o["mode"])
    if ref:
        c.handle = ref.handle
        c.extra = dict(ref.extra)
    return c


def gather(field: str, latest: dict[tuple[str, str], dict], overrides: list[dict]) -> tuple[list[Candidate], list[dict]]:
    cands: list[Candidate] = []
    for (src, f), r in latest.items():
        if f == field:
            order = 0
        elif f.startswith(field + "__extra_"):
            try:
                order = 1 + int(f.rsplit("_", 1)[1])
            except ValueError:
                continue
        elif f in ALTERNATES.get(field, ()):
            order = 100 + ALTERNATES[field].index(f)
        else:
            continue
        if (r["value"] or "") == "":
            continue
        c = _cand(r, order)
        # handle/extra are not stored: recover them by re-classifying the stored value against the target field
        cls = classify(r["value"] or "", field)
        if cls:
            c.handle = cls[0].handle
            c.extra = dict(cls[0].extra)
        cands.append(c)
    ovs = [o for o in overrides if o["field"] == field]
    for o in ovs:
        if o["mode"] in ("fallback", "force") and (o["value"] or ""):
            cands.append(_override_cand(o, field))
    return cands, ovs


def pick(cands: list[Candidate], overrides: list[dict]) -> tuple[Candidate | None, list[str]]:
    flags: list[str] = []
    forced = [c for c in cands if c.mode == "force" and not c.is_junk]
    if forced:
        return sorted(forced, key=lambda c: -c.observed_at)[0], flags
    eligible = [c for c in cands if c.value and not c.is_junk and not c.suppressed and c.mode != "force"]
    if not eligible:
        return None, flags

    def rank(c: Candidate) -> tuple:
        r = DEMOTED_RANK if c.demoted else SOURCE_RANK.get(c.source, 9)
        return (r, c.order, -c.observed_at)
    win = sorted(eligible, key=rank)[0]
    if win.demoted:
        flags.append("stale_editorial")
    return win, flags


def disagreement(field: str, cands: list[Candidate]) -> dict | None:
    if field == "github_repo":
        return None
    prim = [c for c in cands if c.order == 0 and c.mode is None and not c.is_junk and not c.suppressed
            and not (c.ref_kind == "discord_invite" and c.extra.get("is_main_vanity"))]
    by_kind: dict[str, dict[str, dict]] = {}
    for c in prim:
        key = compare_key(c.ref_kind, c.value_norm, c.handle, c.extra)
        if not key:
            continue
        g = by_kind.setdefault(c.ref_kind or "", {}).setdefault(key, {"key": key, "sources": [], "values": []})
        if c.source not in g["sources"]:
            g["sources"].append(c.source)
            g["values"].append(c.value_norm or c.value)
    for kind, groups in sorted(by_kind.items()):
        if len(groups) > 1:
            srcs = {s for g in groups.values() for s in g["sources"]}
            if len(srcs) < 2:
                continue
            keys = sorted(groups)
            return {"groups": [groups[k] for k in keys],
                    "pair_hash": hashlib.sha256("|".join(keys).encode()).hexdigest()[:16], "kind": kind}
    return None


async def _coverage(db: "Database", occ_key: str, as_of: int | None, latest: dict) -> dict[str, bool]:
    """source → covered (a coverage record exists as of the time)."""
    if as_of is None:
        rows = await db.fetchall("SELECT source, has_row FROM identity_coverage WHERE occupant_key = ?", (occ_key,))
        return {r["source"]: True for r in rows}
    return {src: True for (src, f), r in latest.items() if f == "_coverage"}


def status_for(field: str, win: Candidate | None, cands: list[Candidate], covered: dict[str, bool],
               *, owner_known: bool, netuid: int, sub_status: str | None = None) -> str:
    if win is not None:
        return "present"
    nonempty = [c for c in cands if c.value and not c.suppressed]
    if nonempty and all(c.is_junk for c in nonempty):
        return "junk_only"
    if field == "docs_url":
        return "unknown" if sub_status == "unknown" else "absent"
    import app.config as app_config
    s = app_config.settings
    disabled = {"subnetalpha"} if not s.SUBNETALPHA_ENABLED else set()
    if not s.REGISTRY_TMC_COLDKEY_ENABLED:
        disabled.add("tmc_coldkey")
    srcs = [x for x in FIELD_SOURCES.get(field, ()) if x not in disabled]
    if field.startswith("owner_") and not owner_known:
        return "unknown"
    # R5: absent only when every enabled covering source has reported; a never-covering source → unknown
    return "unknown" if any(not covered.get(x) for x in srcs) else "absent"


class Merger:
    def __init__(self, db: "Database", run_id: str, now: int) -> None:
        self.db = db
        self.run_id = run_id
        self.now = now
        self.events: dict[str, int] = {}

    async def _overrides(self, occ_key: str, as_of: int | None) -> list[dict]:
        if as_of is None:
            return await self.db.fetchall("SELECT * FROM manual_overrides WHERE occupant_key = ? AND status = 'active'",
                                          (occ_key,))
        return await self.db.fetchall(
            "SELECT * FROM manual_overrides WHERE occupant_key = ? AND status = 'active' AND created_at <= ? AND "
            "(retired_at IS NULL OR retired_at > ?)", (occ_key, int(as_of), int(as_of)))

    async def compute(self, occ: Occupant, *, as_of: int | None = None) -> dict[str, Resolved]:
        rows = await load_observations(self.db, occ.occupant_key, as_of)
        if as_of is not None and not rows:
            return {}
        latest = _latest_by_key(rows)
        overrides = await self._overrides(occ.occupant_key, as_of)
        covered = await _coverage(self.db, occ.occupant_key, as_of, latest)
        t = self.now if as_of is None else int(as_of)
        epoch = await identity_epoch(self.db, occ.occupant_key, t, occ=occ, rows=rows)
        owner_known = bool(occ.owner_coldkey) or any(f == "owner_coldkey" and (r["value"] or "")
                                                     for (s, f), r in latest.items())
        out: dict[str, Resolved] = {}
        for field in RESOLVED_FIELDS:
            cands, ovs = gather(field, latest, overrides)
            for o in ovs:
                if o["mode"] == "suppress":
                    for c in cands:
                        if c.source == o["suppress_source"] and c.value_hash == o["suppress_value_hash"]:
                            c.suppressed = True
            for c in cands:
                if c.source == "subnetalpha" and c.event_at is not None and c.event_at < epoch - 2 * 86400:
                    c.demoted = True
            win, flags = pick(cands, ovs)
            if field == "github_repo":
                keys = {c.extra.get("key") for c in cands if c.ref_kind == "github_repo" and not c.is_junk
                        and c.extra.get("key")}
                if len(keys) > 1:
                    flags.append("github_string_mismatch")
            st = status_for(field, win, cands, covered, owner_known=owner_known, netuid=occ.netuid,
                            sub_status=out["subnet_url"].status if "subnet_url" in out else None)
            out[field] = Resolved(occ.occupant_key, field, win.value if win else None, win.value_norm if win else None,
                                  win.ref_kind if win else None, win.source if win else None,
                                  win.observed_at if win else None, st, [c.as_dict() for c in cands],
                                  win.override_id if win else None, flags, t)
            out[field].cands = cands
        return out

    async def resolve_all(self, occ: Occupant, *, change_events: dict | None = None) -> dict[str, Resolved]:
        res = await self.compute(occ)
        db = self.db
        prev = {r["field"]: r for r in await db.fetchall(
            "SELECT field, value_norm, status FROM identity_resolved WHERE occupant_key = ?", (occ.occupant_key,))}
        for field, r in res.items():
            await db.execute(
                "INSERT INTO identity_resolved (occupant_key, field, value, value_norm, ref_kind, source, "
                "source_observed_at, status, candidates_json, override_id, flags_json, resolved_at, run_id) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(occupant_key, field) DO UPDATE SET "
                "value = excluded.value, value_norm = excluded.value_norm, ref_kind = excluded.ref_kind, "
                "source = excluded.source, source_observed_at = excluded.source_observed_at, status = excluded.status, "
                "candidates_json = excluded.candidates_json, override_id = excluded.override_id, "
                "flags_json = excluded.flags_json, resolved_at = excluded.resolved_at, run_id = excluded.run_id",
                (occ.occupant_key, field, r.value, r.value_norm, r.ref_kind, r.source, r.source_observed_at, r.status,
                 json.dumps(r.candidates, default=str), r.override_id, json.dumps(r.flags) if r.flags else None,
                 self.now, self.run_id))
            p = prev.get(field)
            changed = p is not None and (p["value_norm"] or None) != (r.value_norm or None)
            ids = (change_events or {}).get((occ.occupant_key, field), [])
            if changed and ids:
                ph = ",".join("?" * len(ids))
                await db.execute(f"UPDATE events SET payload = json_set(payload, '$.resolved_changed', json('true')) "
                                 f"WHERE id IN ({ph}) AND alerted_at IS NULL", ids)
            elif changed and r.override_id:
                await emit_event(db, event_type="identity_changed", occupant_key=occ.occupant_key, netuid=occ.netuid,
                                 severity="info", event_at=self.now, observed_at=self.now,
                                 payload={"netuid": occ.netuid, "occupant_key": occ.occupant_key, "source": "resolved",
                                          "field": field, "kind": "changed", "old_value": p["value_norm"],
                                          "new_value": r.value_norm, "old_hash": None, "new_hash": None,
                                          "old_observed_at": None, "via": "override", "resolved_changed": True},
                                 dedupe_key=f"{occ.occupant_key}|resolved|{field}|{r.value_norm}", source="registry",
                                 run_id=self.run_id)
            d = disagreement(field, r.cands)
            if d:
                ev = await emit_event(db, event_type="source_disagreement", occupant_key=occ.occupant_key,
                                      netuid=occ.netuid, severity="info", event_at=self.now, observed_at=self.now,
                                      payload={"netuid": occ.netuid, "occupant_key": occ.occupant_key, "field": field,
                                               "groups": d["groups"], "resolved_source": r.source,
                                               "pair_hash": d["pair_hash"]},
                                      dedupe_key=f"{occ.occupant_key}|{field}|{d['pair_hash']}", source="registry",
                                      run_id=self.run_id)
                if ev:
                    self.events["source_disagreement"] = self.events.get("source_disagreement", 0) + 1
        await self._write_subnet_columns(occ, res)
        return res

    async def _write_subnet_columns(self, occ: Occupant, res: dict[str, Resolved]) -> None:
        def val(f: str) -> str | None:
            r = res.get(f)
            return r.value_norm if r and r.status == "present" else None
        discord = res.get("discord")
        discord_url = val("discord") if discord and discord.ref_kind in ("discord_invite", "discord_channel") else None
        x = val("x_url")
        x_handle = x.rstrip("/").rsplit("/", 1)[-1] if x else None
        website = val("subnet_url") if res.get("subnet_url") and res["subnet_url"].ref_kind == "website" else None
        await self.db.execute(
            "UPDATE subnets SET website_url = ?, github_url = ?, docs_url = ?, discord_url = ?, telegram_url = ?, "
            "x_handle = ? WHERE netuid = ? AND occupant_key = ?",
            (website, val("github_repo"), val("docs_url"), discord_url, val("telegram"), x_handle, occ.netuid,
             occ.occupant_key))
