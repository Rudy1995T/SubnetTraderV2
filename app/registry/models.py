"""Registry data classes and errors (spec 04 §8.1)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class RegistryRunError(RuntimeError):
    pass


class RpcError(RuntimeError):
    pass


class ScaleError(ValueError):
    pass


@dataclass(slots=True)
class Occupant:
    occupant_key: str
    netuid: int
    network_registered_at: int
    status: str
    name: str | None = None
    symbol: str | None = None
    first_seen_at: int | None = None
    registered_at_est: int | None = None
    registered_ts: int | None = None
    owner_coldkey: str | None = None
    owner_hotkey: str | None = None

    @classmethod
    def from_row(cls, r: dict) -> "Occupant":
        return cls(r["occupant_key"], r["netuid"], r["network_registered_at"], r["status"], r.get("name"),
                   r.get("symbol"), r.get("first_seen_at"), r.get("registered_at_est"), r.get("registered_ts"),
                   r.get("owner_coldkey"), r.get("owner_hotkey"))

    @property
    def reg_ts(self) -> int | None:
        return self.registered_ts if self.registered_ts is not None else self.registered_at_est


@dataclass(slots=True)
class OccupantInput:
    netuid: int
    network_registered_at: int
    name: str | None
    symbol: str | None
    is_active: bool
    first_emission_block: int | None
    owner_coldkey: str | None
    owner_hotkey: str | None
    registered_at_ts: int | None
    block_number: int | None
    identities_v3: dict | None = None
    from_table: bool = False


@dataclass(slots=True)
class Candidate:
    source: str
    field: str                 # the observation's own field (primary, __extra_i or mapped alternate)
    value: str
    value_norm: str | None
    ref_kind: str | None
    value_hash: str
    is_junk: bool
    junk_reason: str | None
    observed_at: int
    event_at: int | None
    source_ref: str | None
    order: int = 0             # 0 primary, 1+i extras, 100 mapped alternates
    override_id: int | None = None
    mode: str | None = None
    demoted: bool = False
    suppressed: bool = False
    handle: str | None = None
    extra: dict = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {"source": self.source, "field": self.field, "value": self.value, "value_norm": self.value_norm,
                "ref_kind": self.ref_kind, "hash": self.value_hash, "is_junk": self.is_junk,
                "junk_reason": self.junk_reason, "demoted": self.demoted, "suppressed": self.suppressed,
                "observed_at": self.observed_at, "override_id": self.override_id}


@dataclass(slots=True)
class Resolved:
    occupant_key: str
    field: str
    value: str | None
    value_norm: str | None
    ref_kind: str | None
    source: str | None
    source_observed_at: int | None
    status: str                # present | absent | junk_only | unknown
    candidates: list[dict]
    override_id: int | None = None
    flags: list[str] = field(default_factory=list)
    resolved_at: int | None = None
    cands: list = field(default_factory=list, repr=False)       # Candidate objects (in-memory only)


@dataclass(slots=True)
class ReviewItem:
    occupant_key: str | None
    netuid: int | None
    name: str | None
    reason: str
    detail: str
    since: int | None
    event_id: int | None = None

    def as_dict(self) -> dict:
        return {"occupant_key": self.occupant_key, "netuid": self.netuid, "name": self.name, "reason": self.reason,
                "detail": self.detail, "since": self.since, "event_id": self.event_id}


@dataclass(slots=True)
class WatchChannel:
    id: int
    occupant_key: str | None
    netuid: int | None
    platform: str
    handle: str
    url: str | None
    source: str
    priority: int
    is_resolved: bool
    is_main_guild: bool
    guild_id: str | None
    needs_resolution: bool
    verified_at: int | None
    meta: dict


@dataclass(slots=True)
class LegacyReport:
    imported: int = 0
    needs_review: int = 0
    skipped_reoccupied: int = 0
    junk: int = 0
    attributed_via_occupancy: int = 0
    attributed_via_guard: int = 0
    observations: int = 0
    overrides: int = 0
    per_netuid: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {k: getattr(self, k) for k in ("imported", "needs_review", "skipped_reoccupied", "junk",
                                              "attributed_via_occupancy", "attributed_via_guard", "observations",
                                              "overrides", "per_netuid")}


@dataclass(slots=True)
class SourceRunResult:
    ok: bool
    error: str | None = None
    via: str | None = None
    n: int = 0
    extra: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {"ok": self.ok, "error": self.error, "via": self.via, "n": self.n, **self.extra}
