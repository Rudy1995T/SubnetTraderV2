"""Label families (spec 13 §2.4, §8.4): forward log return, Δ emission share, 90-d death, top-N gate crossing.

``t`` is always the bar's decision instant ``Bar.bucket``. Day-based labels read ``v_quant_chain_daily``; deaths union
the occupant's deregistration (``v_quant_occupants`` / ``netuid_reoccupied``), the segmentation death of the bar's
series, ``emission_disabled`` events and zero-emission runs. Right-censored windows are ``None`` — never 0.
"""
from __future__ import annotations

import bisect
import re
import sqlite3
from dataclasses import dataclass
from typing import Callable, Literal

from app.util.time import floor_day

from .factor_panel import Panel, forward_log_return, forward_return
from .occupants import OccupantIndex

DAY = 86400
DEREG_KINDS = frozenset({"dereg", "price_reset", "reoccupied"})
LabelFn = Callable[[Panel, int, int], "float | None"]


@dataclass(frozen=True)
class LabelSpec:
    id: str
    family: Literal["ret_log", "demis", "death", "gate_cross"]
    horizon: int
    unit: Literal["bars", "days"]

    def step_bars(self, grid_seconds: int) -> int:
        """Non-overlapping step in bars."""
        if self.unit == "bars":
            return max(1, self.horizon)
        return max(1, self.horizon * DAY // grid_seconds)


_LABEL_RE = re.compile(r"^(ret_log|demis|death|gate_cross)_(\d+)d?$")


def make_label(family: str, horizon: int) -> LabelSpec:
    if family == "ret_log":
        return LabelSpec(f"ret_log_{horizon}", "ret_log", horizon, "bars")
    if family == "demis":
        return LabelSpec(f"demis_{horizon}", "demis", horizon, "days")
    if family == "death":
        return LabelSpec(f"death_{horizon}d", "death", horizon, "days")
    if family == "gate_cross":
        return LabelSpec(f"gate_cross_{horizon}d", "gate_cross", horizon, "days")
    raise ValueError(f"unknown label family {family!r}")


def parse_label(s: str, grid_seconds: int | None = None) -> LabelSpec:
    """``ret_log_7`` | ``demis_28`` | ``death_90`` / ``death_90d`` | ``gate_cross_28`` / ``gate_cross_28d``."""
    m = _LABEL_RE.match(s.strip())
    if not m or int(m.group(2)) < 1:
        raise ValueError(f"bad label {s!r}")
    return make_label(m.group(1), int(m.group(2)))


@dataclass(frozen=True, slots=True)
class DeathEvent:
    onset: int
    kind: str
    death_id: str          # occupant key (or "nu:seg" for unattributed bars) + onset — counts distinct deaths


class LabelSource:
    """Loaded once per snapshot (13 §8.4)."""

    def __init__(self, *, occupants: OccupantIndex, label_end: int, shares: dict[str, tuple[list[int], list[float]]],
                 disabled: dict[str, list[int]], zero_runs: dict[str, list[int]], day_ranks: dict[int, dict[str, int]],
                 top_n: int = 32, zero_eps_pct: float = 1e-4, death_floor: float = 0.01) -> None:
        self.occupants = occupants
        self.label_end = int(label_end)
        self._shares = shares
        self._disabled = disabled
        self._zero_runs = zero_runs
        self._ranks = day_ranks
        self.top_n = top_n
        self.eps = zero_eps_pct
        self.death_floor = death_floor

    @classmethod
    def load(cls, con: sqlite3.Connection, *, occupants: OccupantIndex, label_end: int | None = None,
             top_n: int = 32, zero_run_days: int = 7, zero_eps_pct: float = 1e-4,
             made_at: int | None = None, death_floor: float = 0.01) -> "LabelSource":
        shares: dict[str, tuple[list[int], list[float | None]]] = {}
        by_day: dict[int, list[tuple[float, str]]] = {}
        max_day = None
        for occ, day, share in con.execute("SELECT occupant_key, day, emission_share FROM v_quant_chain_daily "
                                           "ORDER BY occupant_key, day"):
            day = int(day)
            max_day = day if max_day is None or day > max_day else max_day
            d, v = shares.setdefault(occ, ([], []))
            d.append(day)
            v.append(None if share is None else float(share))
            if share is not None:
                by_day.setdefault(day, []).append((float(share), occ))
        if label_end is None:
            ends = [x for x in ((max_day + DAY) if max_day is not None else None, made_at) if x is not None]
            label_end = min(ends) if ends else 0
        day_ranks: dict[int, dict[str, int]] = {}
        for day, lst in by_day.items():
            lst.sort(key=lambda x: (-x[0], x[1]))
            day_ranks[day] = {occ: i + 1 for i, (_, occ) in enumerate(lst)}
        disabled: dict[str, list[int]] = {}
        for occ, t in con.execute("SELECT occupant_key, COALESCE(event_at, observed_at) FROM events "
                                  "WHERE event_type = 'emission_disabled' AND occupant_key IS NOT NULL"):
            disabled.setdefault(occ, []).append(int(t))
        for v in disabled.values():
            v.sort()
        zero_runs: dict[str, list[int]] = {}
        for occ, (days, vals) in shares.items():
            runs = []
            i, n = 0, len(days)
            while i < n:
                if vals[i] is not None and vals[i] <= zero_eps_pct:
                    j = i
                    while (j + 1 < n and vals[j + 1] is not None and vals[j + 1] <= zero_eps_pct
                           and days[j + 1] - days[j] == DAY):
                        j += 1
                    if j - i + 1 >= zero_run_days:
                        runs.append(days[i])
                    i = j + 1
                else:
                    i += 1
            if runs:
                zero_runs[occ] = runs
        return cls(occupants=occupants, label_end=label_end, shares=shares, disabled=disabled, zero_runs=zero_runs,
                   day_ranks=day_ranks, top_n=top_n, zero_eps_pct=zero_eps_pct, death_floor=death_floor)

    # ── primitives ──────────────────────────────────────────────────────────────────────────────────────────────
    def share(self, occ: str | None, t: int, tol_days: int = 0) -> float | None:
        if not occ or occ not in self._shares:
            return None
        days, vals = self._shares[occ]
        d = floor_day(t)
        for off in sorted(range(-tol_days, tol_days + 1), key=abs):
            j = bisect.bisect_left(days, d + off * DAY)
            if j < len(days) and days[j] == d + off * DAY and vals[j] is not None:
                return vals[j]
        return None

    def onsets(self, panel: Panel | None, nu: int | None, i: int | None, occ: str | None, t: int, t_end: int,
               *, dereg_only: bool = False) -> list[DeathEvent]:
        """Death onsets in ``(t, t_end]`` from every source, earliest first."""
        out: list[DeathEvent] = []
        d = self.occupants.death_onset(occ)
        if d is not None and t < d[0] <= t_end:
            out.append(DeathEvent(d[0], d[1], f"{occ}@{d[0]}"))
        if panel is not None and nu is not None and i is not None:
            bars = panel.series[nu]
            kind = panel.seg_death.get(nu, {}).get(bars[i].seg)
            if kind is not None:
                end = panel.seg_end[nu][i]
                onset = bars[end].ts or bars[end].bucket
                if t < onset <= t_end:
                    out.append(DeathEvent(onset, kind, f"{occ or f'{nu}:{bars[i].seg}'}@seg"))
        if occ and not dereg_only:
            for e in self._disabled.get(occ, ()):
                if t < e <= t_end:
                    out.append(DeathEvent(e, "emission_disabled", f"{occ}@{e}"))
                    break
            s0 = self.share(occ, t)
            if s0 is not None and s0 > self.eps:
                for z in self._zero_runs.get(occ, ()):
                    if t < z <= t_end:
                        out.append(DeathEvent(z, "zero_emission", f"{occ}@{z}"))
                        break
        out.sort(key=lambda e: e.onset)
        return out

    # ── labels ──────────────────────────────────────────────────────────────────────────────────────────────────
    def fwd_log_return(self, panel: Panel, nu: int, i: int, h: int) -> float | None:
        return forward_log_return(panel, nu, i, h, death_floor=self.death_floor)

    def death_event(self, panel: Panel, nu: int, i: int, days: int, *, dereg_only: bool = False
                    ) -> tuple[int | None, DeathEvent | None]:
        b = panel.series[nu][i]
        t = b.bucket
        t_end = t + days * DAY
        evs = self.onsets(panel, nu, i, b.occ, t, t_end, dereg_only=dereg_only)
        if evs:
            return 1, evs[0]
        if t_end > self.label_end:
            return None, None
        return 0, None

    def death_within(self, occupant_key: str | None, t: int, days: int) -> int | None:
        evs = self.onsets(None, None, None, occupant_key, t, t + days * DAY)
        if evs:
            return 1
        return None if t + days * DAY > self.label_end else 0

    def demis(self, occupant_key: str | None, t: int, days: int, *, panel: Panel | None = None,
              nu: int | None = None, i: int | None = None) -> float | None:
        s0 = self.share(occupant_key, t)
        if s0 is None:
            return None
        t_end = t + days * DAY
        evs = self.onsets(panel, nu, i, occupant_key, t, min(t_end, self.label_end))
        if evs:
            return -s0
        s1 = self.share(occupant_key, t_end, tol_days=1)
        if s1 is None:
            return None
        return s1 - s0

    def gate_cross(self, occupant_key: str | None, t: int, days: int, *, panel: Panel | None = None,
                   nu: int | None = None, i: int | None = None) -> int | None:
        if not occupant_key:
            return None
        r0 = self._ranks.get(floor_day(t), {}).get(occupant_key)
        if r0 is None:
            return None
        was_in = r0 <= self.top_n
        t_end = t + days * DAY
        died = self.onsets(panel, nu, i, occupant_key, t, min(t_end, self.label_end), dereg_only=True)
        if died:
            return -1 if was_in else 0
        if t_end > self.label_end:
            return None
        r1 = self._ranks.get(floor_day(t_end), {}).get(occupant_key)
        if r1 is None:
            return None
        now_in = r1 <= self.top_n
        return (1 if now_in and not was_in else -1 if was_in and not now_in else 0)

    def label_fn(self, spec: LabelSpec) -> LabelFn:
        if spec.family == "ret_log":
            return lambda p, nu, i: self.fwd_log_return(p, nu, i, spec.horizon)
        if spec.family == "demis":
            return lambda p, nu, i: self.demis(p.series[nu][i].occ, p.series[nu][i].bucket, spec.horizon,
                                               panel=p, nu=nu, i=i)
        if spec.family == "death":
            return lambda p, nu, i: self.death_event(p, nu, i, spec.horizon)[0]
        return lambda p, nu, i: self.gate_cross(p.series[nu][i].occ, p.series[nu][i].bucket, spec.horizon,
                                                panel=p, nu=nu, i=i)


def ret_log_label_fn(horizon: int, death_floor: float = 0.01) -> LabelFn:
    """The ``ret_log`` label without a LabelSource (pure panels)."""
    return lambda p, nu, i: forward_log_return(p, nu, i, horizon, death_floor=death_floor)


def simple_return_fn(horizon: int) -> LabelFn:
    return lambda p, nu, i: forward_return(p, nu, i, horizon)
