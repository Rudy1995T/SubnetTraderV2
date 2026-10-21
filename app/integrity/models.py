"""Spec 11 §8.1 data classes. ``RawMetric`` is this spec's producer result; ``to_metric_result()`` maps it onto
spec 12's ``MetricResult`` and ``to_row()`` onto a ``metric_values`` raw row (status only in ``value_json``)."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from app.features.types import MetricResult

Status = Literal["ok", "absent", "failed", "not_applicable"]


@dataclass(frozen=True, slots=True)
class RawMetric:
    key: str
    value: float | None
    status: Status
    reason: str | None = None
    value_json: dict = field(default_factory=dict)

    def mapped_json(self) -> dict:
        vj = dict(self.value_json)
        vj["status"] = self.status
        if self.status != "ok":
            vj["reason"] = self.reason or "unspecified"
        else:
            vj.pop("reason", None)
        return vj

    def to_metric_result(self) -> MetricResult:
        vj = self.mapped_json()
        if self.status == "absent":
            return MetricResult(self.value if self.value is not None else 0.0, vj, absence_reason=self.reason)
        if self.status in ("failed", "not_applicable"):
            return MetricResult(None, vj)
        return MetricResult(self.value, vj)

    def to_row(self, *, occupant_key: str, as_of: int, observed_at: int, run_id: str, spec_version: str,
               backfilled: bool = False) -> dict:
        vj = self.mapped_json()
        if backfilled:
            vj["backfilled"] = True
        value = self.value if self.status in ("ok", "absent") else None
        return {"occupant_key": occupant_key, "metric_key": self.key, "as_of": int(as_of),
                "spec_version": spec_version, "stage": "raw", "value": value, "value_json": vj,
                "observed_at": int(observed_at), "source": "integrity", "run_id": run_id,
                "backfilled": 1 if backfilled else 0}


def ok(key: str, value: float, **vj: Any) -> RawMetric:
    return RawMetric(key, float(value), "ok", None, vj)


def absent(key: str, reason: str, value: float = 0.0, **vj: Any) -> RawMetric:
    return RawMetric(key, float(value), "absent", reason, vj)


def failed(key: str, reason: str, **vj: Any) -> RawMetric:
    return RawMetric(key, None, "failed", reason, vj)


def not_applicable(key: str, reason: str, **vj: Any) -> RawMetric:
    return RawMetric(key, None, "not_applicable", reason, vj)


@dataclass(slots=True)
class ActionResult:
    ok: bool
    id: int | None = None
    occupant_key: str | None = None
    status: str | None = None
    error: str | None = None
    warning: str | None = None
    event_id: int | None = None
    extra: dict | None = None

    def to_dict(self) -> dict:
        d = asdict(self)
        if not d.get("extra"):
            d.pop("extra", None)
        return d


def err(code: str, **kw: Any) -> ActionResult:
    return ActionResult(False, error=code, **kw)


@dataclass(frozen=True, slots=True)
class Candidate:
    name: str
    role: str | None = None
    profile_url: str | None = None
    profile_kind: str | None = None


@dataclass(frozen=True, slots=True)
class TeamInfoParse:
    disclosure: str                      # empty | placeholder | named | anonymous | unstructured
    candidates: tuple[Candidate, ...]
    text_len: int
    html_hash: str
    anon_marker: bool


@dataclass(frozen=True, slots=True)
class TwoSourceResult:
    ok: bool
    domains: tuple[str, ...]
    attestations: int
    evidence_count: int
    reason: str | None
    items: tuple[dict, ...] = ()


@dataclass(frozen=True, slots=True)
class TeamMemberState:
    member_id: int
    name: str
    status: str                          # status as of t
    source: str
    check_time: int | None


@dataclass(frozen=True, slots=True)
class OwnerKeys:
    coldkey: str | None
    hotkey: str | None
    source: str                          # tmc_list | tmc_detail | chain_hourly | rufus
    block_number: int | None = None
    network_registered_at: int | None = None
    first_emission_block: int | None = None
    owner_cut_per_block: float | None = None
    pending_owner_cut: float | None = None
    owner_cut_enabled: bool | None = None
    personal_stake: float | None = None
    locked: float | None = None
    perpetual: float | None = None
    decaying: float | None = None
    raw: dict | None = None


@dataclass(slots=True)
class StakeEvent:
    event_uid: str
    owner_coldkey: str
    function: str
    kind: str
    counts_as_outflow: int
    event_at: int
    signer: str | None = None
    hotkey: str | None = None
    dest_coldkey: str | None = None
    from_netuid: int | None = None
    to_netuid: int | None = None
    alpha: float | None = None
    tao: float | None = None
    price: float | None = None
    is_proxy: int | None = None
    success: int = 1
    block_number: int | None = None
    extrinsic_id: str | None = None
    is_transfer: bool | None = None
    raw: dict | None = None

    @property
    def trigger_id(self) -> str:
        return f"owner_stake_events:{self.extrinsic_id or self.event_uid}"


@dataclass(slots=True)
class OwnerChange:
    occupant_key: str
    netuid: int
    change_type: str
    event_at: int
    observed_at: int
    detected_via: str
    old_value: str | None = None
    new_value: str | None = None
    classification_basis: str | None = None
    s3_base_penalty: float = 0.0
    block_number: int | None = None
    extrinsic_id: str | None = None
    paired_change_id: int | None = None
    identity_event_id: int | None = None
    evidence: dict = field(default_factory=dict)
    id: int | None = None
    run_id: str = ""

    @property
    def dedupe_key(self) -> str:
        return f"{self.occupant_key}|{self.change_type}|{self.new_value or ''}|{self.event_at // 86400}"

    @property
    def trigger_id(self) -> str:
        return f"owner_changes:{self.id}"


@dataclass(frozen=True, slots=True)
class RufusSubnet:
    netuid: int
    owner_coldkey: str | None
    owner_hotkey: str | None
    registration_block: int | None
    auto_lock_enabled: bool | None
    owner_perp: float | None
    owner_decay: float | None
    owner_pct: float | None              # fraction (percent on the wire is divided by 100)
    owner_perpetual_flag: bool | None    # the owner row's ``perpetual``
    to_owner_perp: float | None
    to_owner_lockers: int | None
    challenger_max_pct: float | None
    challenger_coldkeys: tuple[str, ...]
    days_to_king: float | None
