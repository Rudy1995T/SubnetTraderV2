"""Point-in-time factor stamping (spec 13 §2.1, §8.3; 12 §2.15 items 3–4).

For factor ``f`` on a bar decided at ``t_dec = Bar.bucket`` the stamped value is the row with the greatest ``as_of``
among the occupant's rows whose effective observation time ``eff ≤ t_dec``, provided ``t_dec − as_of ≤ max_age(f)``.
``eff``: norm/raw rows → ``observed_at``; score rows → ``computed_at``. Backfilled rows are dropped in ``strict``; in
``reconstructed`` they enter with ``eff = as_of + lag_s`` (scores ``max(input_cutoff, as_of + lag_s)``) only for
reconstructible keys and never before ``coverage.reconstructible_from``. No forward fill, nothing written back.
"""
from __future__ import annotations

import bisect
import json
import sqlite3
from typing import Mapping, Sequence

from app.scoring.spec import SPEC_VERSION

from .backtestability import (
    PILLARS,
    RECONSTRUCTIBLE_KEYS,
    SCORE_FACTORS,
    Coverage,
    base_key,
    classify,
    is_reconstructible,
)

_SCORE_COLS = ("composite_raw", "composite", "composite_smoothed", "quality", "velocity", "confidence")


class _Series:
    """One (key, occupant) row set sorted by eff with a prefix-max over as_of for O(log n) lookups."""
    __slots__ = ("effs", "best")

    def __init__(self, rows: list[tuple[int, int, float]]) -> None:
        rows.sort(key=lambda r: (r[0], r[1]))
        self.effs = [r[0] for r in rows]
        self.best: list[tuple[int, float]] = []          # (as_of, value) with max as_of over rows[0..j]
        cur: tuple[int, float] | None = None
        for _, as_of, v in rows:
            if cur is None or as_of >= cur[0]:
                cur = (as_of, v)
            self.best.append(cur)

    def at(self, t: int) -> tuple[int, float] | None:
        j = bisect.bisect_right(self.effs, t) - 1
        return self.best[j] if j >= 0 else None


class PITStamper:
    def __init__(self, keys: Sequence[str], *, mode: str, freshness: Mapping[str, int], default_max_age: int,
                 reconstructible: Mapping[str, dict] | None = None, coverage: Coverage | None = None) -> None:
        if mode not in ("strict", "reconstructed"):
            raise ValueError(f"pit mode {mode!r}")
        self.keys = tuple(keys)
        self.mode = mode
        self.freshness = dict(freshness)
        self.default_max_age = int(default_max_age)
        self.reconstructible = reconstructible if reconstructible is not None else RECONSTRUCTIBLE_KEYS
        self.coverage = coverage
        self.counters: dict[str, int] = {}
        self.warnings: list[str] = []
        self.used_rows = 0              # stamped values that came from a PIT row
        self.used_backfilled = 0        # … of which backfilled = 1
        self.shadow_rows = 0
        self._series: dict[tuple[str, str], _Series] = {}
        self._pending: dict[tuple[str, str], list[tuple[int, int, float]]] = {}
        self._backfilled: dict[tuple[str, str], set[int]] = {}

    def _count(self, name: str, n: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + n

    def _warn(self, code: str) -> None:
        if code not in self.warnings:
            self.warnings.append(code)

    def _admit(self, key: str, as_of: int, eff: int, backfilled: int, *, score_cutoff: int | None = None
               ) -> int | None:
        """Effective time of a row or None when the mode drops it (counted)."""
        if not backfilled:
            return eff
        if self.mode == "strict":
            self._count("dropped_backfilled")
            return None
        if not is_reconstructible(key, self.coverage, self.warnings):
            self._count("dropped_forward_only")
            self._warn(f"forward_only_key_dropped:{base_key(key)}")
            return None
        spec = self.reconstructible.get(base_key(key), {})
        if spec.get("min_as_of") is not None and as_of < int(spec["min_as_of"]):
            self._count("dropped_before_reconstructible_from")
            return None
        if self.coverage is not None:
            known, lo = self.coverage.clamp_for(key)
            if known and (lo is None or as_of < lo):
                self._count("dropped_before_reconstructible_from")
                self._warn(f"backfilled_before_reconstructible_from:{base_key(key)}")
                return None
        lag = int(spec.get("lag_s", 86400))
        e = as_of + lag
        if score_cutoff is not None:
            e = max(score_cutoff, e)
        return e

    def _add(self, key: str, occ: str, eff: int, as_of: int, value: float, backfilled: int) -> None:
        self._pending.setdefault((key, occ), []).append((eff, as_of, float(value)))
        if backfilled:
            self._backfilled.setdefault((key, occ), set()).add(as_of)

    def _finish(self) -> None:
        self._series = {k: _Series(v) for k, v in self._pending.items()}
        self._pending = {}

    @classmethod
    def load(cls, con: sqlite3.Connection, keys: Sequence[str], *, spec_version: str, lo: int, hi: int, mode: str,
             freshness: Mapping[str, int], default_max_age: int, official_only: bool,
             coverage: Coverage | None = None, score_max_age: int = 129600) -> "PITStamper":
        self = cls(keys, mode=mode, freshness=freshness, default_max_age=default_max_age, coverage=coverage)
        kinds = {k: classify(k) for k in keys}
        metric = sorted({base_key(k) for k, kd in kinds.items() if kd in ("metric", "raw")})
        evidence = sorted({base_key(k) for k, kd in kinds.items() if kd == "evidence"})
        score_keys = [k for k, kd in kinds.items() if kd in ("score", "pillar")]
        want = set(keys)
        for k in score_keys:
            self.freshness[k] = score_max_age
        if metric:
            q = (f"SELECT occupant_key, metric_key, as_of, value, json_extract(value_json,'$.raw') AS raw, "
                 f"observed_at, backfilled FROM metric_values WHERE stage = 'norm' AND spec_version = ? "
                 f"AND metric_key IN ({','.join('?' * len(metric))}) AND value IS NOT NULL AND as_of BETWEEN ? AND ?"
                 + (" AND as_of % 86400 = 0" if official_only else ""))
            for occ, mk, as_of, value, raw, obs, bf in con.execute(q, (spec_version, *metric, lo, hi)):
                for key, v in ((mk, value), (f"raw:{mk}", raw)):
                    if key not in want or v is None:
                        continue
                    try:
                        fv = float(v)
                    except (TypeError, ValueError):
                        continue
                    eff = self._admit(key, int(as_of), int(obs), int(bf))
                    if eff is not None:
                        self._add(key, occ, eff, int(as_of), fv, int(bf))
        if evidence:
            q = (f"SELECT occupant_key, metric_key, as_of, value, observed_at, backfilled FROM metric_values "
                 f"WHERE stage = 'raw' AND metric_key IN ({','.join('?' * len(evidence))}) AND value IS NOT NULL "
                 f"AND status NOT IN ('failed','none','not_applicable') AND as_of BETWEEN ? AND ?")
            for occ, mk, as_of, value, obs, bf in con.execute(q, (*evidence, lo, hi)):
                key = f"evidence:{mk}"
                eff = self._admit(key, int(as_of), int(obs), int(bf))
                if eff is not None:
                    self._add(key, occ, eff, int(as_of), float(value), int(bf))
        if score_keys:
            q = ("SELECT occupant_key, as_of, composite_raw, composite, composite_smoothed, quality, velocity, "
                 "confidence, pillar_scores, computed_at, input_cutoff, backfilled, shadow FROM scores "
                 "WHERE spec_version = ? AND as_of BETWEEN ? AND ?"
                 + (" AND run_kind = 'official'" if official_only else ""))
            primary = spec_version == SPEC_VERSION
            for r in con.execute(q, (spec_version, lo, hi)):
                occ, as_of, comp = r[0], int(r[1]), r[2:8]
                computed_at, cutoff, bf, shadow = int(r[9]), int(r[10]), int(r[11]), int(r[12])
                if shadow:
                    if primary:
                        self.shadow_rows += 1
                        continue
                if cutoff > computed_at:
                    self._count("score_cutoff_violation")
                    self._warn("score_cutoff_violation")
                    continue
                pillars = {}
                if any(k in PILLARS for k in score_keys):
                    try:
                        pillars = json.loads(r[8] or "{}")
                    except ValueError:
                        pillars = {}
                for key in score_keys:
                    v = comp[_SCORE_COLS.index(key)] if key in SCORE_FACTORS else pillars.get(key)
                    if v is None:
                        continue
                    eff = self._admit(key, as_of, computed_at, bf, score_cutoff=cutoff)
                    if eff is not None:
                        self._add(key, occ, eff, as_of, float(v), bf)
        self._finish()
        return self

    def max_age(self, key: str) -> int:
        return int(self.freshness.get(key, self.freshness.get(base_key(key), self.default_max_age)))

    def stamp(self, occupant_key: str | None, t_dec: int) -> dict[str, float]:
        """``{key: value}`` admissible at decision instant ``t_dec`` (absent key = None)."""
        if not occupant_key:
            return {}
        out: dict[str, float] = {}
        for key in self.keys:
            s = self._series.get((key, occupant_key))
            if s is None:
                continue
            hit = s.at(t_dec)
            if hit is None:
                continue
            as_of, v = hit
            if t_dec - as_of > self.max_age(key):
                continue
            out[key] = v
            self.used_rows += 1
            if as_of in self._backfilled.get((key, occupant_key), ()):
                self.used_backfilled += 1
        return out
