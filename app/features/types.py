"""Feature-store types (spec 12 §2.3, §2.6; 02 §C5): the producer-fn return type and the engine's row types.
Created by 07 ahead of 12; 12 owns the contract.

``absence_reason`` set → ``value_json.status='absent'`` (value as returned, normally 0.0); ``value is None`` →
``not_applicable`` when ``value_json["status"] == "not_applicable"``, else ``failed``; otherwise ``ok``.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Awaitable, Callable

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database


@dataclass(frozen=True, slots=True)
class MetricResult:
    value: float | None
    value_json: dict = field(default_factory=dict)
    absence_reason: str | None = None        # not None → value_json.status='absent', value_json.reason = absence_reason

    @property
    def status(self) -> str:
        if self.absence_reason is not None:
            return "absent"
        if self.value is None:
            return "not_applicable" if self.value_json.get("status") == "not_applicable" else "failed"
        return "ok"

    def mapped_json(self) -> dict[str, Any]:
        """``value_json`` with 12 §2.3's status (and a reason on every non-ok row)."""
        vj = dict(self.value_json)
        st = self.status
        vj["status"] = st
        if st == "absent":
            vj["reason"] = self.absence_reason
        elif st != "ok":
            vj.setdefault("reason", "unspecified")
        return vj


ProducerFn = Callable[["Database", str, datetime], Awaitable[MetricResult]]


@dataclass(frozen=True, slots=True)
class Occupant:
    occupant_key: str
    netuid: int
    network_registered_at: int
    status: str                        # status at the slot: 'active' | 'pre_launch' (12 §2.6 launch instant)
    launch_ts: int | None = None


@dataclass(frozen=True, slots=True)
class RawRow:                          # one spec-03 metric_values row with stage='raw'
    occupant_key: str
    metric_key: str
    as_of: int
    value: float | None
    value_json: dict
    status: str                        # the generated column ('none' = no value_json.status, read as failed)
    observed_at: int
    source: str
    run_id: str | None
    backfilled: int

    @property
    def provisional(self) -> bool:
        return self.as_of % 86400 != 0                                           # 02 §B3

    @property
    def usable(self) -> bool:
        return self.status not in ("failed", "none")

    @property
    def reason(self) -> str | None:
        return (self.value_json or {}).get("reason")


@dataclass(slots=True)
class NormValue:
    m: float | None
    raw: float | None
    status: str | None                 # the selected raw row's status (None = no row selected)
    reason: str | None
    age_s: int | None = None
    raw_as_of: int | None = None
    raw_observed_at: int | None = None
    src: str | None = None
    fallback: bool = False
    cohort_n: int | None = None
