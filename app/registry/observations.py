"""Point-in-time identity observations with hash-dedupe and change events (spec 04 §2.8)."""
from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

import app.config as app_config
from app.registry.models import Occupant
from app.registry.normalise import Ref, _redact
from app.storage.events import emit_event

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

WARNING_CHAIN_FIELDS = frozenset({"subnet_name", "github_repo", "subnet_url", "discord"})
_NO_EVENT_FIELDS = frozenset({"docs_url", "_coverage", "twitter_embed_id"})


def value_hash(raw: str) -> str:
    return hashlib.sha256((raw or "").strip().encode()).hexdigest()


def _silent(source: str, field: str) -> bool:
    return source == "legacy" or "__extra_" in field or field in _NO_EVENT_FIELDS


class ObservationWriter:
    def __init__(self, db: "Database", run_id: str, observed_at: int) -> None:
        self.db = db
        self.run_id = run_id
        self.observed_at = observed_at
        self.written = 0
        self.events: dict[str, int] = {}
        self.change_events: dict[tuple[str, str], list[int]] = {}      # (occupant_key, field) → identity_changed ids
        self._extra_idx: dict[tuple[str, str, str], int] = {}          # (occ, source, target_field) → next index
        self._derived: dict[tuple[str, str], set[str]] = {}            # (occ, source) → extras/docs written this run

    async def _latest(self, occ_key: str, source: str, field: str) -> dict | None:
        return await self.db.fetchone(
            "SELECT * FROM identity_observations WHERE occupant_key = ? AND source = ? AND field = ? "
            "ORDER BY observed_at DESC, id DESC LIMIT 1", (occ_key, source, field))

    def _count(self, et: str) -> None:
        self.events[et] = self.events.get(et, 0) + 1

    async def write(self, occ: Occupant, source: str, field: str, ref: Ref | None, raw: str, *,
                    event_at: int | None, block_number: int | None, source_ref: str | None,
                    suspect: bool = False, via: str | None = None, observed_at: int | None = None) -> bool:
        obs_at = self.observed_at if observed_at is None else observed_at
        raw_t = (raw or "").strip()
        vh = value_hash(raw_t)
        leaked = bool(ref and ref.junk_reason == "leaked_secret")
        stored = _redact(raw_t) if leaked else raw_t
        latest = await self._latest(occ.occupant_key, source, field)
        if latest is not None and latest["value_hash"] == vh:
            if obs_at - latest["observed_at"] < app_config.settings.REGISTRY_REOBSERVE_DAYS * 86400:
                return False
        if latest is not None and latest["value_hash"] != vh and obs_at < latest["observed_at"]:
            return False                              # never insert history behind a newer observation
        await self.db.execute(
            "INSERT INTO identity_observations (occupant_key, netuid, source, field, value, value_norm, ref_kind, "
            "value_hash, is_junk, junk_reason, reclassified_from, observed_at, event_at, block_number, run_id, "
            "source_ref) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (occ.occupant_key, occ.netuid, source, field, stored, ref.canonical if ref else None,
             ref.kind if ref else None, vh, int(bool(ref and ref.is_junk)), ref.junk_reason if ref else None,
             ref.reclassified_from if ref else None, obs_at, event_at, block_number, self.run_id, source_ref))
        self.written += 1
        if _silent(source, field):
            return True
        if latest is not None and latest["value_hash"] != vh:
            old = latest["value"] or ""
            kind = "added" if old == "" else ("removed" if raw_t == "" else "changed")
            if not (suspect and kind == "removed"):
                sev = "warning" if source == "chain" and field in WARNING_CHAIN_FIELDS else "info"
                ev = await emit_event(
                    self.db, event_type="identity_changed", occupant_key=occ.occupant_key, netuid=occ.netuid,
                    severity=sev, event_at=event_at if source == "subnetalpha" and event_at else obs_at,
                    observed_at=obs_at,
                    payload={"netuid": occ.netuid, "occupant_key": occ.occupant_key, "source": source, "field": field,
                             "kind": kind, "old_value": old, "new_value": stored, "old_hash": latest["value_hash"],
                             "new_hash": vh, "old_observed_at": latest["observed_at"], "via": via,
                             "resolved_changed": False},
                    dedupe_key=f"{occ.occupant_key}|{source}|{field}|{vh}", source="registry", run_id=self.run_id)
                if ev:
                    self._count("identity_changed")
                    self.change_events.setdefault((occ.occupant_key, field), []).append(ev)
        if ref is not None and ref.junk_reason:
            sev = "warning" if ref.junk_reason in ("abandonment_marker", "leaked_secret") else "info"
            ev = await emit_event(
                self.db, event_type="identity_junk", occupant_key=occ.occupant_key, netuid=occ.netuid, severity=sev,
                event_at=obs_at, observed_at=obs_at,
                payload={"netuid": occ.netuid, "occupant_key": occ.occupant_key, "source": source, "field": field,
                         "value": stored, "junk_reason": ref.junk_reason},
                dedupe_key=f"{occ.occupant_key}|{source}|{field}|{vh}", source="registry", run_id=self.run_id)
            if ev:
                self._count("identity_junk")
        return True

    async def mark_coverage(self, source: str, occ: Occupant, has_row: bool, block_number: int | None) -> None:
        now = self.observed_at
        await self.db.execute(
            "INSERT INTO identity_coverage (source, occupant_key, has_row, last_ok_at, last_ok_run_id, last_ok_block, "
            "consecutive_misses) VALUES (?,?,?,?,?,?,0) ON CONFLICT(source, occupant_key) DO UPDATE SET "
            "has_row = excluded.has_row, last_ok_at = excluded.last_ok_at, last_ok_run_id = excluded.last_ok_run_id, "
            "last_ok_block = excluded.last_ok_block, consecutive_misses = 0",
            (source, occ.occupant_key, int(has_row), now, self.run_id, block_number))
        await self.write(occ, source, "_coverage", None, "1" if has_row else "0", event_at=None,
                         block_number=block_number, source_ref=None)

    async def bump_miss(self, source: str, occ: Occupant) -> None:
        await self.db.execute("UPDATE identity_coverage SET consecutive_misses = consecutive_misses + 1 "
                              "WHERE source = ? AND occupant_key = ?", (source, occ.occupant_key))

    async def write_raw(self, source: str, scope: str, occ: Occupant | None, payload, *, source_ref: str | None,
                        event_at: int | None, block_number: int | None) -> bool:
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)
        ph = hashlib.sha256(body.encode()).hexdigest()
        if occ is not None:
            last = await self.db.fetchval(
                "SELECT payload_hash FROM identity_raw_snapshots WHERE source = ? AND occupant_key = ? "
                "ORDER BY observed_at DESC, id DESC LIMIT 1", (source, occ.occupant_key))
        else:
            last = await self.db.fetchval(
                "SELECT payload_hash FROM identity_raw_snapshots WHERE source = ? AND source_ref IS ? AND "
                "occupant_key IS NULL ORDER BY observed_at DESC, id DESC LIMIT 1", (source, source_ref))
        if last == ph:
            return False
        await self.db.execute(
            "INSERT INTO identity_raw_snapshots (source, scope, netuid, occupant_key, source_ref, payload_json, "
            "payload_hash, observed_at, event_at, block_number, run_id) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (source, scope, occ.netuid if occ else None, occ.occupant_key if occ else None, source_ref, body, ph,
             self.observed_at, event_at, block_number, self.run_id))
        return True

    async def write_value(self, occ: Occupant, source: str, field: str, raw: str | None, refs: list[Ref], *,
                          event_at: int | None, block_number: int | None, source_ref: str | None,
                          suspect: bool = False, via: str | None = None, observed_at: int | None = None) -> None:
        """Primary under ``field`` + secondaries under ``{target_field}__extra_{i}`` (never the primary's name)."""
        primary = refs[0] if refs else None
        await self.write(occ, source, field, primary, raw or "", event_at=event_at, block_number=block_number,
                         source_ref=source_ref, suspect=suspect, via=via, observed_at=observed_at)
        written = self._derived.setdefault((occ.occupant_key, source), set())
        for ref in refs[1:]:
            tf = ref.target_field or field
            if ref.kind == "docs":
                target = "docs_url"
                if target in written:
                    continue
            else:
                k = (occ.occupant_key, source, tf)
                i = self._extra_idx.get(k, 0)
                self._extra_idx[k] = i + 1
                target = f"{tf}__extra_{i}"
            written.add(target)
            await self.write(occ, source, target, ref, ref.canonical, event_at=event_at, block_number=block_number,
                             source_ref=source_ref, suspect=suspect, via=via, observed_at=observed_at)

    async def finish_source(self, occ: Occupant, source: str, *, block_number: int | None = None,
                            observed_at: int | None = None) -> None:
        """Blank derived rows (``*__extra_*``, ``docs_url``) this source declared before but not in this run."""
        written = self._derived.get((occ.occupant_key, source), set())
        rows = await self.db.fetchall(
            "SELECT field, value FROM identity_observations o WHERE occupant_key = ? AND source = ? AND "
            "(field LIKE '%\\_\\_extra\\_%' ESCAPE '\\' OR field = 'docs_url') AND id = (SELECT MAX(id) FROM "
            "identity_observations i WHERE i.occupant_key = o.occupant_key AND i.source = o.source AND "
            "i.field = o.field)", (occ.occupant_key, source))
        for r in rows:
            if r["field"] not in written and (r["value"] or "") != "":
                await self.write(occ, source, r["field"], None, "", event_at=None, block_number=block_number,
                                 source_ref=None, observed_at=observed_at)
