"""Red-flag gates (spec 12 §2.8; master §7.5; 02 §B7/§D5): trigger evaluation and the ``red_flag_state`` machine.

Pure: ``evaluate`` maps a flag's inputs to ``(trigger ∈ {True, False, None}, detail)``; ``step`` advances one state
row (03's columns exactly) and reports the transition. The engine does the I/O. A ``None`` trigger (input missing
or unobservable) neither raises, resets nor advances the clear clock (fail-safe neutral, §7 item 3).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Callable, Iterable, Mapping

from app.features.types import Occupant, RawRow
from app.storage.catalog import RED_FLAG_FLOOR, RED_FLAGS

DAY = 86400


@dataclass
class FlagInputs:
    raw: Mapping[str, RawRow | None]                  # the run's selected raw rows for this occupant (§2.2 PIT rule)
    series: Mapping[str, Mapping[int, RawRow]]        # exact-as_of rows (miner_burn_zeroed, inflated_community)
    events: list[dict]                                # this occupant's events observed ≤ cutoff (04/07 types)
    manual: Mapping[str, dict]                        # flag → latest manual_flags row with set_at ≤ cutoff
    occupant: Occupant
    slot: int
    cutoff: int

    @property
    def a0(self) -> int:
        return self.slot - self.slot % DAY


Evaluator = Callable[[FlagInputs], "tuple[bool | None, dict]"]


@dataclass(frozen=True, slots=True)
class FlagDef:
    name: str
    multiplier: float
    clear_days: int
    human_confirmable: bool
    manual_clearable: bool
    sticky_on: str | None
    evaluate: Evaluator


@dataclass
class FlagState:
    """One ``red_flag_state`` row (03 §3.1 columns, 02 §B7)."""
    occupant_key: str
    flag: str
    spec_version: str
    active: int = 0
    raised_at: int | None = None
    last_true_at: int | None = None
    false_since: int | None = None
    cleared_at: int | None = None
    human: int = 0
    sticky: int = 0
    last_manual_clear_at: int | None = None
    suppress_until: int | None = None
    last_eval: str | None = None
    last_eval_as_of: int = 0
    detail_json: dict = field(default_factory=dict)
    updated_at: int = 0

    def clearing(self) -> bool:
        return bool(self.active) and self.false_since is not None

    def state(self) -> str:
        return "clearing" if self.clearing() else "active"

    def origin(self) -> str:
        return "human" if self.human else "auto"

    def clear_streak_days(self, slot: int) -> int:
        return 0 if self.false_since is None else max(0, (slot - self.false_since) // DAY)

    def as_row(self) -> dict:
        import json
        return {"occupant_key": self.occupant_key, "flag": self.flag, "spec_version": self.spec_version,
                "active": int(self.active), "raised_at": self.raised_at, "last_true_at": self.last_true_at,
                "false_since": self.false_since, "cleared_at": self.cleared_at, "human": int(self.human),
                "sticky": int(self.sticky), "last_manual_clear_at": self.last_manual_clear_at,
                "suppress_until": self.suppress_until, "last_eval": self.last_eval,
                "last_eval_as_of": int(self.last_eval_as_of),
                "detail_json": json.dumps(self.detail_json, sort_keys=True, separators=(",", ":"), default=str),
                "updated_at": int(self.updated_at)}

    @classmethod
    def from_row(cls, r: Mapping) -> "FlagState":
        import json
        d = dict(r)
        d["detail_json"] = json.loads(d.get("detail_json") or "{}")
        return cls(**{k: d.get(k) for k in cls.__dataclass_fields__ if k in d})


# ── input helpers ────────────────────────────────────────────────────────────────────────────────────────────
def _row(fi: FlagInputs, key: str) -> RawRow | None:
    r = fi.raw.get(key)
    return r if r is not None and r.usable else None


def _val(r: RawRow | None) -> float | None:
    if r is None:
        return None
    return 0.0 if (r.status == "absent" and r.value is None) else r.value


def _and3(*terms: bool | None) -> bool | None:
    if any(t is False for t in terms):
        return False
    if all(t is True for t in terms):
        return True
    return None


def _trigger_detail(r: RawRow, keys: Iterable[str]) -> dict:
    vj = r.value_json or {}
    d = {k: vj.get(k) for k in keys if k in vj}
    if "trigger_ids" in d:
        d["trigger_ids"] = sorted(d["trigger_ids"] or [])
    return d


def _event_ts(e: Mapping) -> int:
    return int(e.get("event_at") if e.get("event_at") is not None else e["observed_at"])


# ── evaluators (§2.8 table) ──────────────────────────────────────────────────────────────────────────────────
def _emission_disabled(fi: FlagInputs) -> tuple[bool | None, dict]:
    v = _val(_row(fi, "emission_enabled_flag"))
    if v is None:
        return None, {}
    d = {"emission_enabled_flag": v}
    if v >= 0.5:
        return False, d
    return (True, d) if fi.occupant.status == "active" else (False, d)


def _dereg_imminent(fi: FlagInputs) -> tuple[bool | None, dict]:
    dr, im = _val(_row(fi, "dereg_risk_flag")), _val(_row(fi, "immune_flag"))
    d = {"dereg_risk_flag": dr, "immune_flag": im}
    if (dr is not None and dr < 0.5) or (im is not None and im >= 0.5):
        return False, d
    if dr is not None and im is not None:
        return True, d
    return None, d


def _exact_series(fi: FlagInputs, key: str, days: int) -> list[RawRow | None]:
    s = fi.series.get(key) or {}
    out = []
    for k in range(days):
        r = s.get(fi.a0 - k * DAY)
        out.append(r if r is not None and r.status == "ok" and r.value is not None else None)
    return out


def _miner_burn_zeroed(fi: FlagInputs) -> tuple[bool | None, dict]:
    rows = _exact_series(fi, "implied_miner_burn", 3)
    vals = [None if r is None else r.value for r in rows]
    d = {"implied_miner_burn": vals}
    if all(v is not None and v > 0.8 for v in vals):
        return True, d
    if any(v is not None and v <= 0.8 for v in vals):
        return False, d
    return None, d


def _owner_trigger(key: str, keys: tuple[str, ...]) -> Evaluator:
    def _ev(fi: FlagInputs) -> tuple[bool | None, dict]:
        r = _row(fi, key)
        v = _val(r)
        if r is None or v is None:
            return None, {}
        d = {key: v, **_trigger_detail(r, keys)}
        return (v >= 0.5), d
    return _ev


def _key_compromise(fi: FlagInputs) -> tuple[bool | None, dict]:
    return None, {}                                    # manual 'confirmed' only (02 §D5)


def _repo_gone(fi: FlagInputs) -> tuple[bool | None, dict]:
    r = _row(fi, "declared_repo_freshness")
    if r is None:
        return None, {}
    d = {"status": r.status, "reason": r.reason}
    return (r.status == "absent" and r.reason == "repo_gone"), d


def _all_repos_archived(fi: FlagInputs) -> tuple[bool | None, dict]:
    r = _row(fi, "org_freshness")
    if r is None:
        return None, {}
    vj = r.value_json or {}
    d = {k: vj.get(k) for k in ("all_archived", "declared_archived", "successor_repo_id")}
    trig = bool(vj.get("all_archived")) or (bool(vj.get("declared_archived")) and vj.get("successor_repo_id") is None)
    return trig, d


def _site_down_7d(fi: FlagInputs) -> tuple[bool | None, dict]:
    evs = [e for e in fi.events if e["event_type"] in ("site_down_7d", "site_recovered") and _event_ts(e) <= fi.slot]
    if evs:
        last = max(evs, key=lambda e: (_event_ts(e), int(e["id"])))
        d = {"event_type": last["event_type"], "event_at": _event_ts(last)}
        if last["event_type"] == "site_down_7d":
            return True, {**d, "trigger_event_id": int(last["id"])}
        return False, d
    if _row(fi, "site_uptime_30d") is not None:
        return False, {"event_type": None, "site_uptime_30d": "fresh"}
    return None, {}


def _name_turnover(fi: FlagInputs) -> bool:
    lo = fi.a0 - 90 * DAY
    for e in fi.events:
        t = _event_ts(e)
        if not (lo < t <= fi.slot):
            continue
        p = e.get("payload") or {}
        if e["event_type"] == "identity_changed" and p.get("field") == "subnet_name" and p.get("kind") == "changed":
            return True
        if e["event_type"] == "identity_junk" and p.get("junk_reason") == "abandonment_marker":
            return True
    return False


def _stale_identity(fi: FlagInputs) -> tuple[bool | None, dict]:
    v = _row(fi, "venue_reachable")
    dead = None if v is None else (_val(v) is not None and _val(v) < 0.5)
    turnover = _name_turnover(fi)
    s = _row(fi, "silence_days")
    if s is None:
        no_comms = None
    elif s.status == "absent":
        no_comms = True
    elif s.status == "ok":
        no_comms = s.value >= 30
    else:
        no_comms = None
    d = {"dead_venue": dead, "name_turnover": turnover, "no_comms_30d": no_comms}
    return _and3(dead, turnover, no_comms), d


def _silence_45d(fi: FlagInputs) -> tuple[bool | None, dict]:
    s = _row(fi, "silence_days")
    if s is None or s.status not in ("ok", "absent"):
        return None, {}
    if s.status == "absent":
        return False, {"silence_days": "absent"}
    return (s.value >= 45), {"silence_days": s.value}


def _inflated_community(fi: FlagInputs) -> tuple[bool | None, dict]:
    rows = _exact_series(fi, "presence_ratio", 7)

    def ok(r: RawRow) -> bool:
        m = (r.value_json or {}).get("members")
        return r.value < 0.02 and m is not None and m > 1000
    present = [r for r in rows if r is not None]
    d = {"days_present": len(present), "days_inflated": sum(ok(r) for r in present)}
    if len(present) == 7 and all(ok(r) for r in present):
        return True, d
    if any(not ok(r) for r in present):
        return False, d
    return None, d


_OWNER_X = ("reasons", "trigger_ids")
_OWNER_C = ("kind", "trigger_ids", "identity_churn")


def _fd(name: str, clear_days: int, ev: Evaluator, *, confirm: bool = False, clear: bool = False,
        sticky_on: str | None = None) -> FlagDef:
    return FlagDef(name, RED_FLAGS[name], clear_days, confirm, clear, sticky_on, ev)


FLAGS_TS_1_0: Mapping[str, FlagDef] = MappingProxyType({f.name: f for f in (
    _fd("emission_disabled", 7, _emission_disabled),
    _fd("dereg_imminent", 7, _dereg_imminent),
    _fd("miner_burn_zeroed", 7, _miner_burn_zeroed),
    _fd("owner_extraction", 7, _owner_trigger("owner_extraction_trigger", _OWNER_X), confirm=True, clear=True),
    _fd("owner_control_change", 7, _owner_trigger("owner_control_change_trigger", _OWNER_C), clear=True,
        sticky_on="identity_churn"),
    _fd("key_compromise", 7, _key_compromise, confirm=True, clear=True),
    _fd("repo_gone", 7, _repo_gone),
    _fd("all_repos_archived", 7, _all_repos_archived),
    _fd("site_down_7d", 7, _site_down_7d),
    _fd("stale_identity", 7, _stale_identity),
    _fd("silence_45d", 7, _silence_45d),
    _fd("inflated_community", 7, _inflated_community),
)})
if dict((k, f.multiplier) for k, f in FLAGS_TS_1_0.items()) != RED_FLAGS:          # §2.8: == 03's catalog
    raise RuntimeError("FLAGS_TS_1_0 multipliers differ from catalog.RED_FLAGS")

# raw keys the evaluators read (selected by the run) and exact-as_of series (days back from A0)
FLAG_RAW_KEYS = ("emission_enabled_flag", "dereg_risk_flag", "immune_flag", "owner_extraction_trigger",
                 "owner_control_change_trigger", "declared_repo_freshness", "org_freshness", "site_uptime_30d",
                 "venue_reachable", "silence_days")
FLAG_SERIES = MappingProxyType({"implied_miner_burn": 3, "presence_ratio": 7})
FLAG_EVENT_TYPES = ("identity_changed", "identity_junk", "site_down_7d", "site_recovered")


def evaluate(fd: FlagDef, inputs: FlagInputs) -> tuple[bool | None, dict]:
    return fd.evaluate(inputs)


def _ids(d: Mapping | None) -> set:
    return set((d or {}).get("trigger_ids") or [])


def step(state: FlagState | None, fd: FlagDef, trig: bool | None, detail: dict, manual_row: Mapping | None,
         slot: int, *, spec_version: str, occupant_key: str, computed_at: int,
         suppress_days: int = 30) -> tuple[FlagState | None, str | None]:
    """Advance one flag (§2.8 steps 1–5). Returns ``(state or None, transition)`` with transition ∈
    ``{"raised", "cleared_manual", "cleared_hysteresis", None}``; no row is created until the flag first raises."""
    s = None if state is None else replace(state, detail_json=dict(state.detail_json))
    detail = dict(detail)
    h = manual_row
    confirmed = fd.human_confirmable and h is not None and h.get("state") == "confirmed"
    new_clear = (s is not None and fd.manual_clearable and h is not None and h.get("state") == "cleared"
                 and int(h["set_at"]) > (s.last_manual_clear_at or 0))
    if new_clear:
        s.last_manual_clear_at = int(h["set_at"])
        s.suppress_until = slot + suppress_days * DAY                       # D5: cleared_at + 30 d
        s.detail_json["cleared_trigger_ids"] = sorted(_ids(s.detail_json) | _ids(detail))   # all evidence known now
    eff = trig
    if (trig is True and s is not None and s.suppress_until and slot < s.suppress_until
            and _ids(detail) <= set(s.detail_json.get("cleared_trigger_ids") or [])):
        eff = False
        detail["suppressed"] = True                  # same evidence stays cleared; NEW trigger ids re-raise now (D5)
    transition = None
    carried = None if s is None else s.detail_json.get("cleared_trigger_ids")
    sticky_now = fd.sticky_on is not None and detail.get(fd.sticky_on) is True
    if s is None or not s.active:
        if eff is True or confirmed:
            if s is None:
                s = FlagState(occupant_key, fd.name, spec_version)
            s.active, s.raised_at, s.cleared_at, s.false_since = 1, slot, None, None
            s.human = int(confirmed)
            s.sticky = int(eff is True and sticky_now)
            if eff is True:
                s.last_true_at = slot
            s.detail_json = {**detail, "trigger_event_id": detail.get("trigger_event_id")}
            if carried is not None:
                s.detail_json["cleared_trigger_ids"] = carried
            transition = "raised"
    else:
        s.human = int(confirmed)
        if eff is True:
            s.last_true_at, s.false_since = slot, None
            s.detail_json = {**detail, "trigger_event_id": detail.get("trigger_event_id")}
            if carried is not None:
                s.detail_json["cleared_trigger_ids"] = carried
            if sticky_now:
                s.sticky = 1
        elif eff is False and s.false_since is None:
            s.false_since = slot                     # None leaves the clock untouched
        if s.human or s.sticky:
            s.false_since = None                     # held flags never run the clock
        if new_clear:
            transition = "cleared_manual"
        elif s.false_since is not None and slot - s.false_since >= fd.clear_days * DAY:
            transition = "cleared_hysteresis"
        if transition:
            s.active, s.cleared_at, s.human, s.sticky = 0, slot, 0, 0
    if s is not None:
        s.last_eval = "none" if trig is None else ("true" if trig else "false")
        s.last_eval_as_of = slot
        s.updated_at = computed_at
    return s, transition


def multiplier(active: Iterable[FlagDef], floor: float = RED_FLAG_FLOOR) -> float:
    """``max(floor, Π multiplier)`` over the active flags (1.0 when none)."""
    p = math.prod(f.multiplier for f in active)
    return max(floor, p)
