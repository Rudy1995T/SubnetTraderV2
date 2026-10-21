"""``quant_weekly`` — the optional weekly harness run (spec 13 §8.8). Default off (13 §11 items 10, 15).

The pipeline runs in niced child processes (pure-Python CPU work would hold the GIL and starve the asyncio loop):
``snapshot --prune`` → ``report --run-all`` on that snapshot → ``rank-stability``. Each child's ``RESULT`` line is
parsed; exit 0/1 continue, anything else raises (01's wrapper logs it and latches ``collector_failing`` after
``QUANT_ALERT_AFTER_FAILURES``). No Telegram message of its own (13 §3.7).
"""
from __future__ import annotations

import asyncio
import json
import sys

import app.config as app_config
from app.jobs import CollectorJob, JobOutcome, RunContext


class QuantChildError(RuntimeError):
    pass


def parse_result_line(stdout: str) -> dict | None:
    for line in reversed(stdout.splitlines()):
        if line.startswith("RESULT "):
            try:
                return json.loads(line[len("RESULT "):])
            except ValueError:
                return None
    return None


class QuantWeeklyJob(CollectorJob):
    def __init__(self, rt, spec) -> None:
        super().__init__(rt, spec)
        self.last_gate_pass: bool | None = None
        self.last_verdict_file: str | None = None

    async def _child(self, args: list[str]) -> tuple[int, dict]:
        proc = await asyncio.create_subprocess_exec(
            "nice", "-n", "10", sys.executable, "-m", "app.quant", *args,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=str(app_config.APP_ROOT))
        try:
            out, err = await proc.communicate()
        except asyncio.CancelledError:
            proc.kill()
            await proc.wait()
            raise
        code = proc.returncode
        if code not in (0, 1):
            tail = (err or b"").decode(errors="replace").strip()[-300:]
            raise QuantChildError(f"quant child {' '.join(args[:2])} exit {code}: {tail}")
        res = parse_result_line((out or b"").decode(errors="replace")) or {}
        return code, res

    async def collect(self, run: RunContext) -> JobOutcome:
        exits: dict[str, int] = {}
        code, snap = await self._child(["snapshot", "--prune"])
        exits["snapshot"] = code
        files = list(snap.get("files") or [])
        if not files:
            raise QuantChildError("quant child snapshot wrote no file")
        snapshot = files[0]
        code, rep = await self._child(["report", "--run-all", "--db", snapshot])
        exits["report"] = code
        code, rs = await self._child(["rank-stability", "--factors", "quality,composite_smoothed", "--scope",
                                      "survivorship", "--lag", "1", "--db", snapshot])
        exits["rank_stability"] = code
        results = list(rep.get("files") or []) + list(rs.get("files") or [])
        verdict = rep.get("verdict_file")
        gate_pass = rep.get("gate_pass")
        if verdict:
            self.last_verdict_file = verdict
        self.last_gate_pass = gate_pass
        await self.set_state("last_verdict", {"verdict_file": self.last_verdict_file, "gate_pass": gate_pass})
        meta = {"verdict_file": verdict, "gate_pass": gate_pass, "n_results": len(results), "snapshot": snapshot,
                "exit_codes": exits}
        note = f"gate_pass={gate_pass} verdict={verdict}"
        if exits.get("report") == 1:
            note += " insufficient_data"
        return JobOutcome(rows=len(results), note=note, meta=meta)

    def health_extra(self) -> dict | None:
        cached = self._state_cache.get("last_verdict") or {}
        return {"last_gate_pass": self.last_gate_pass if self.last_gate_pass is not None else cached.get("gate_pass"),
                "last_verdict_file": self.last_verdict_file or cached.get("verdict_file")}

    def status(self) -> dict:
        """01's block plus ``last_gate_pass``/``last_verdict_file`` at its top level (13 §7)."""
        out = super().status()
        out.update(self.health_extra() or {})
        return out
