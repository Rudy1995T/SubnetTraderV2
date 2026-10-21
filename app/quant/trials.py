"""Persistent trial log for multiple-testing deflation (spec 13 §3.4, §8.5).

Every study tuple is recorded **before** computing (a crashed run still counts as a trial) and its ``results`` filled
once afterwards. ``n_trials`` = distinct ``trial_key``s ever started; ``trial_sharpe_var`` = sample variance (ddof 1)
of ``abs(ic_ir)`` over the newest result per distinct key. Rewritten atomically under an exclusive ``flock``.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import statistics
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from app.util.time import now_ts

KEY_FIELDS = ("factor", "label", "horizon", "scope", "controls", "lookback", "min_depth", "max_depth", "step",
              "pit_mode", "start", "end", "spec_version")


def trial_key(t: dict) -> str:
    """sha1 of the canonical JSON of the key fields (``start``/``end`` as given — null when defaulted)."""
    canon = json.dumps({k: t.get(k) for k in KEY_FIELDS}, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha1(canon.encode()).hexdigest()


def _empty() -> dict:
    return {"n_trials": 0, "trial_sharpe_var": None, "updated_at": now_ts(), "entries": []}


def _recompute(log: dict) -> dict:
    entries = log.get("entries") or []
    keys = {e["trial_key"] for e in entries}
    newest: dict[str, float] = {}
    for e in entries:                               # file order = chronological; later results win
        r = e.get("results")
        if r and r.get("ic_ir") is not None:
            newest[e["trial_key"]] = abs(float(r["ic_ir"]))
    vals = list(newest.values())
    log["n_trials"] = len(keys)
    log["trial_sharpe_var"] = statistics.variance(vals) if len(vals) >= 2 else None
    log["updated_at"] = now_ts()
    return log


class TrialLog:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    @contextmanager
    def _locked(self) -> Iterator[dict]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path.with_name(self.path.name + ".lock"), "a+") as lk:
            fcntl.flock(lk.fileno(), fcntl.LOCK_EX)
            try:
                log = self._read()
                yield log
                _recompute(log)
                tmp = self.path.with_name(self.path.name + ".tmp")
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(log, f, indent=1, allow_nan=False)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, self.path)
            finally:
                fcntl.flock(lk.fileno(), fcntl.LOCK_UN)

    def _read(self) -> dict:
        try:
            d = json.loads(self.path.read_text())
            if isinstance(d, dict) and isinstance(d.get("entries"), list):
                return d
        except (OSError, ValueError):
            pass
        return _empty()

    def read(self) -> dict:
        return _recompute(self._read())

    def start(self, tuples: list[dict], *, command: str, git_sha: str, db_sha256: str) -> list[str]:
        """Append one entry per study tuple (results null); returns their trial keys."""
        ts = now_ts()
        out = []
        with self._locked() as log:
            for t in tuples:
                k = trial_key(t)
                e = {"trial_key": k, "ts": ts, "command": command, **{f: t.get(f) for f in KEY_FIELDS},
                     "git_sha": git_sha, "db_sha256": db_sha256, "results": None}
                log["entries"].append(e)
                out.append(k)
        return out

    def finish(self, results: dict[str, dict]) -> None:
        """Fill ``results`` once for the newest entry of each key whose results are still null."""
        if not results:
            return
        with self._locked() as log:
            for k, r in results.items():
                for e in reversed(log["entries"]):
                    if e["trial_key"] == k and e.get("results") is None:
                        e["results"] = r
                        break

    def stats(self) -> tuple[int, float | None]:
        d = self.read()
        return int(d["n_trials"]), d["trial_sharpe_var"]
