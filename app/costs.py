"""Cost governor (spec 16 §2.10; public API = 02 §G5).

Spend is ledgered per canonical provider id (``provider_spend``) and capped per group: ``x`` (twitterapi_io,
official_v2; USD), ``llm`` (anthropic; USD), ``taostats`` (request cap), ``tmc`` (request cap). A cap of 0 for a paid
group disables it (fail-closed). 80 % → ``spend_80pct`` once per group-month; 100 % → hard latch +
``spend_cap_reached``; UTC month rollover clears everything. State shared across processes via ``job_state('_costs')``.
"""
from __future__ import annotations

import json
import os
import re
import uuid
from calendar import monthrange
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Sequence

import app.config as app_config
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.util.time import month_key, now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.notifications.alerts import AlertLatch
    from app.ratelimit import ProviderLimiter
    from app.storage.db import Database


@dataclass(frozen=True, slots=True)
class SpendGroup:
    name: str
    members: tuple[str, ...]
    usd_cap_key: str | None
    req_cap_key: str | None
    paid: bool
    pacing_flag: str | None = None


SPEND_GROUPS: dict[str, SpendGroup] = {
    "x": SpendGroup("x", ("twitterapi_io", "official_v2"), "X_MONTHLY_CAP_USD", None, True, "X_PACING_ENABLED"),
    "llm": SpendGroup("llm", ("anthropic",), "LLM_MONTHLY_CAP_USD", None, True),
    "taostats": SpendGroup("taostats", ("taostats",), "TAOSTATS_MONTHLY_CAP_USD", "TAOSTATS_MONTHLY_CAP_REQ", False),
    "tmc": SpendGroup("tmc", ("tmc",), None, "TMC_MONTHLY_CAP", False),
}
_MEMBER_TO_GROUP = {m: g for g, sg in SPEND_GROUPS.items() for m in sg.members}
# taostats' USD key is display-only (flat subscription, never enforced)
_ENFORCED_USD = {"x", "llm"}


def group_of(name: str) -> SpendGroup:
    if name in SPEND_GROUPS:
        return SPEND_GROUPS[name]
    if name in _MEMBER_TO_GROUP:
        return SPEND_GROUPS[_MEMBER_TO_GROUP[name]]
    return SpendGroup(name, (name,), None, None, False)


def spend_provider_for(name: str) -> str:
    if name == "x":
        return app_config.settings.X_PROVIDER if app_config.settings.X_PROVIDER != "null" else "twitterapi_io"
    if name == "llm":
        return "anthropic"
    if name == "taostats":
        return "taostats"
    if name == "tmc":
        return "tmc"
    return name


class UnknownReservation(KeyError):
    pass


@dataclass(slots=True)
class Reservation:
    id: str
    group: str
    provider: str
    est_usd: float
    job: str
    run_id: str | None
    created_at: int
    expires_at: int


@dataclass(slots=True)
class Decision:
    allowed: bool
    reason: str | None
    usd_mtd: float
    reserved_usd: float
    cap_usd: float | None
    projected_usd: float | None


def _days_left_in_month(ts: int) -> int:
    d = datetime.fromtimestamp(ts, timezone.utc)
    return monthrange(d.year, d.month)[1] - d.day + 1


class CostGovernor:
    def __init__(self, db: "Database", *, limiter: "ProviderLimiter | None" = None,
                 alerts: "AlertLatch | None" = None, clock: Callable[[], int] = now_ts) -> None:
        self.db = db
        self.limiter = limiter
        self.alerts = alerts
        self.clock = clock
        self._pending: dict[tuple[str, str], int] = {}           # (provider, month) -> buffered requests
        self._run_totals: dict[str, float] = {}
        self._state: dict[str, dict] = {}                         # job_state('_costs') key -> value
        self._usd: dict[tuple[str, str], float] = {}              # (provider, month) -> usd (DB)
        self._req: dict[tuple[str, str], int] = {}                # (provider, month) -> requests (DB)
        self._month = month_key(clock())
        self._pacing_reason: dict[str, str] = {}

    # ── state io ───────────────────────────────────────────────────────────────────────────────────────────
    async def _load_state(self) -> None:
        rows = await self.db.fetchall("SELECT key, value_json FROM job_state WHERE job = '_costs'", writer=True)
        self._state = {}
        for r in rows:
            try:
                self._state[r["key"]] = json.loads(r["value_json"])
            except ValueError:
                continue
        month = month_key(self.clock())
        rows = await self.db.fetchall("SELECT provider, month, usd, requests FROM provider_spend WHERE month = ?",
                                      (month,), writer=True)
        self._usd = {(r["provider"], r["month"]): float(r["usd"]) for r in rows}
        self._req = {(r["provider"], r["month"]): int(r["requests"]) for r in rows}

    async def _put_state(self, key: str, value: dict) -> None:
        await self.db.upsert("job_state", [{"job": "_costs", "key": key, "value_json": json.dumps(value),
                                            "updated_at": self.clock()}], conflict=("job", "key"))
        self._state[key] = value

    async def _del_state(self, key: str) -> None:
        await self.db.execute("DELETE FROM job_state WHERE job = '_costs' AND key = ?", (key,))
        self._state.pop(key, None)

    async def load(self) -> None:
        await self._load_state()
        month = month_key(self.clock())
        if self.limiter is not None:
            for (p, m), n in self._req.items():
                if m == month and p in self.limiter.limits:
                    self.limiter.seed_month(p, n)
        await self.refresh()

    async def refresh(self) -> None:
        now = self.clock()
        month = month_key(now)
        prev_month = self._month
        await self._load_state()
        if month != prev_month:
            for g in SPEND_GROUPS:
                if f"hard:{g}:{prev_month}" in self._state:
                    await self._fire("spend_reset", f"spend_reset:{g}:{month}",
                                     f"ℹ️ <code>{g}</code> budget reset for {month}",
                                     {"group": g, "month": month, "provider": g}, f"{g}|{month}|reset", "info")
            self._month = month
        for key, val in list(self._state.items()):
            if not key.startswith("resv:"):
                continue
            if val.get("expired_at") is None and now > int(val.get("expires_at", 0)):
                val["expired_at"] = now
                await self._put_state(key, val)
                logger.warning("reservation_expired", data={"id": key[5:], "group": val.get("group")})
            elif val.get("expired_at") is not None and now > int(val["expired_at"]) + 7 * 86400:
                await self._del_state(key)

    # ── queries ────────────────────────────────────────────────────────────────────────────────────────────
    def _month_now(self) -> str:
        return month_key(self.clock())

    def month_usd(self, name: str) -> float:
        g = group_of(name)
        m = self._month_now()
        return sum(self._usd.get((p, m), 0.0) for p in g.members)

    def month_requests(self, name: str) -> int:
        g = group_of(name)
        m = self._month_now()
        return sum(self._req.get((p, m), 0) + self._pending.get((p, m), 0) for p in g.members)

    def override_usd(self, name: str) -> float:
        g = group_of(name)
        o = self._state.get(f"override:{g.name}:{self._month_now()}")
        return float(o.get("extra_usd", 0.0)) if o else 0.0

    def cap_usd(self, name: str) -> float | None:
        g = group_of(name)
        if not g.usd_cap_key:
            return None
        return float(getattr(app_config.settings, g.usd_cap_key)) + self.override_usd(g.name)

    def request_cap(self, name: str) -> int | None:
        g = group_of(name)
        return int(getattr(app_config.settings, g.req_cap_key)) if g.req_cap_key else None

    def reserved_usd(self, name: str) -> float:
        g = group_of(name)
        now = self.clock()
        tot = 0.0
        for key, val in self._state.items():
            if key.startswith("resv:") and val.get("group") == g.name and val.get("expired_at") is None \
                    and int(val.get("expires_at", 0)) >= now:
                tot += float(val.get("est_usd", 0.0))
        return tot

    def _hard_latched(self, g: SpendGroup) -> bool:
        latch = self._state.get(f"hard:{g.name}:{self._month_now()}")
        if not latch:
            return False
        cap = self.cap_usd(g.name)
        if g.name in _ENFORCED_USD and cap is not None:
            return cap <= float(latch.get("cap", 0.0))       # a raised cap / override clears it
        rc = self.request_cap(g.name)
        return rc is not None and rc <= float(latch.get("cap", 0.0))

    def is_exhausted(self, name: str) -> bool:
        g = group_of(name)
        if g.name not in SPEND_GROUPS:
            return False
        if g.paid and g.name in _ENFORCED_USD:
            cap = self.cap_usd(g.name)
            if cap is not None and cap <= 0:
                return True
        if self._hard_latched(g):
            return True
        rc = self.request_cap(g.name)
        if rc is not None:
            if rc <= 0:
                return True
            if self.month_requests(g.name) >= rc:
                return True
        return False

    def paused_reason(self, name: str) -> str | None:
        g = group_of(name)
        if g.paid:
            from app.control import kill_switch_active
            if kill_switch_active():
                return "kill_switch"
        return "budget" if self.is_exhausted(name) else None

    def run_total(self, run_id: str) -> float:
        return self._run_totals.get(run_id, 0.0)

    async def daily_run_rate(self, name: str, days: int = 7) -> float:
        g = group_of(name)
        from app.jobs import JOB_TABLE
        jobs = [s.id for s in JOB_TABLE if s.providers and group_of(s.providers[0]).name == g.name]
        since = self.clock() - days * 86400
        tot = 0.0
        if jobs:
            ph = ",".join("?" * len(jobs))
            tot = float(await self.db.fetchval(
                f"SELECT COALESCE(SUM(cost_usd), 0) FROM collector_runs WHERE job IN ({ph}) AND started_at >= ?",
                (*jobs, since), writer=True) or 0.0)
        if tot <= 0:
            day = datetime.fromtimestamp(self.clock(), timezone.utc).day
            return self.month_usd(g.name) / max(1, day)
        return tot / days

    # ── decisions ──────────────────────────────────────────────────────────────────────────────────────────
    async def _decide(self, provider: str, est_usd: float) -> Decision:
        g = group_of(provider)
        usd = self.month_usd(g.name)
        reserved = self.reserved_usd(g.name)
        cap = self.cap_usd(g.name)
        if g.name not in SPEND_GROUPS:
            return Decision(True, None, usd, reserved, None, None)
        if g.paid:
            from app.control import kill_switch_active
            if kill_switch_active():
                return Decision(False, "kill_switch", usd, reserved, cap, None)
            if cap is not None and cap <= 0:
                return Decision(False, "zero_cap", usd, reserved, cap, None)
        rc = self.request_cap(g.name)
        if rc is not None and rc <= 0:
            return Decision(False, "zero_cap", usd, reserved, cap, None)
        if g.name not in _ENFORCED_USD:
            if rc is not None and self.month_requests(g.name) >= rc:
                return Decision(False, "hard_cap", usd, reserved, cap, None)
            return Decision(True, None, usd, reserved, cap, None)
        await self._load_state()
        usd = self.month_usd(g.name)
        reserved = self.reserved_usd(g.name)
        if self._hard_latched(g):
            return Decision(False, "hard_cap", usd, reserved, cap, None)
        s = app_config.settings
        if cap is not None and (usd >= cap * s.COST_HARD_STOP_PCT or usd + reserved + est_usd > cap):
            await self._latch_hard(g, usd, cap, est_usd)
            return Decision(False, "hard_cap", usd, reserved, cap, None)
        projected = None
        if g.pacing_flag and getattr(s, g.pacing_flag, False) and cap is not None:
            rate = await self.daily_run_rate(g.name, 7)
            projected = usd + reserved + est_usd + rate * _days_left_in_month(self.clock())
            if projected > cap:
                return Decision(False, "pacing", usd, reserved, cap, projected)
        return Decision(True, None, usd, reserved, cap, projected)

    async def check(self, provider: str, est_usd: float) -> Decision:
        d = await self._decide(provider, est_usd)
        if d.reason == "pacing":
            self._pacing_reason[group_of(provider).name] = "pacing"
        else:
            self._pacing_reason.pop(group_of(provider).name, None)
        return d

    async def reserve(self, provider: str, est_usd: float, *, job: str, run_id: str | None) -> Reservation | None:
        d = await self._decide(provider, est_usd)
        g = group_of(provider)
        if not d.allowed:
            logger.info("spend_denied", data={"provider": provider, "group": g.name, "reason": d.reason,
                                              "est_usd": est_usd})
            return None
        now = self.clock()
        res = Reservation(f"resv-{uuid.uuid4().hex[:12]}", g.name, spend_provider_for(provider), float(est_usd), job,
                          run_id, now, now + app_config.settings.COST_RESERVATION_TTL_SEC)
        val = asdict(res)
        val["expired_at"] = None
        await self._put_state(f"resv:{res.id}", val)
        return res

    async def commit(self, res: "Reservation | str", actual_usd: float, *, detail: dict | None = None) -> None:
        rid = res.id if isinstance(res, Reservation) else str(res)
        await self._load_state()
        val = self._state.get(f"resv:{rid}")
        if val is None:
            raise UnknownReservation(rid)
        await self._book(val["provider"], float(actual_usd), requests=0, units=None, job=val.get("job"),
                         run_id=val.get("run_id"), detail=detail)
        await self._del_state(f"resv:{rid}")

    async def release(self, res: "Reservation | str") -> None:
        rid = res.id if isinstance(res, Reservation) else str(res)
        if f"resv:{rid}" not in self._state:
            await self._load_state()
        if f"resv:{rid}" not in self._state:
            logger.info("release of unknown reservation", data={"id": rid})
            return
        await self._del_state(f"resv:{rid}")

    async def record(self, provider: str, usd: float, *, requests: int = 0, units: dict[str, int] | None = None,
                     job: str | None = None, run_id: str | None = None, detail: dict | None = None) -> None:
        if usd == 0 and not units:
            if requests:
                k = (provider, month_key(self.clock()))
                self._pending[k] = self._pending.get(k, 0) + int(requests)
            return
        await self._book(provider, float(usd), requests=int(requests), units=units, job=job, run_id=run_id,
                         detail=detail)

    async def _book(self, provider: str, usd: float, *, requests: int, units: dict[str, int] | None,
                    job: str | None, run_id: str | None, detail: dict | None) -> None:
        now = self.clock()
        month = month_key(now)
        async with self.db.transaction(immediate=True):
            row = await self.db.fetchone("SELECT units_json FROM provider_spend WHERE provider = ? AND month = ?",
                                         (provider, month))
            merged: dict[str, int] = json.loads(row["units_json"]) if row else {}
            for k, v in (units or {}).items():
                merged[k] = merged.get(k, 0) + int(v)
            await self.db.execute(
                "INSERT INTO provider_spend (provider, month, usd, requests, units_json, updated_at) "
                "VALUES (?,?,?,?,?,?) ON CONFLICT(provider, month) DO UPDATE SET usd = usd + excluded.usd, "
                "requests = requests + excluded.requests, units_json = excluded.units_json, "
                "updated_at = excluded.updated_at",
                (provider, month, usd, requests, json.dumps(merged, sort_keys=True), now))
            r2 = await self.db.fetchone("SELECT usd, requests FROM provider_spend WHERE provider = ? AND month = ?",
                                        (provider, month))
        self._usd[(provider, month)] = float(r2["usd"])
        self._req[(provider, month)] = int(r2["requests"])
        if run_id:
            self._run_totals[run_id] = self._run_totals.get(run_id, 0.0) + usd
        logger.info("spend_booked", data={"provider": provider, "usd": usd, "requests": requests, "job": job,
                                          "run_id": run_id, "detail": detail})
        await self._check_soft(group_of(provider))

    async def flush(self) -> int:
        pending, self._pending = self._pending, {}
        n = 0
        for (p, m), reqs in pending.items():
            if reqs <= 0:
                continue
            try:
                await self.db.execute(
                    "INSERT INTO provider_spend (provider, month, usd, requests, units_json, updated_at) "
                    "VALUES (?,?,0.0,?,'{}',?) ON CONFLICT(provider, month) DO UPDATE SET "
                    "requests = requests + excluded.requests, updated_at = excluded.updated_at",
                    (p, m, reqs, self.clock()))
                self._req[(p, m)] = self._req.get((p, m), 0) + reqs
                n += 1
            except Exception:
                self._pending[(p, m)] = self._pending.get((p, m), 0) + reqs
                raise
        for g in ("tmc", "taostats"):
            await self._check_soft(SPEND_GROUPS[g])
            await self._check_request_cap(SPEND_GROUPS[g])
        return n

    # ── latches & alerts ───────────────────────────────────────────────────────────────────────────────────
    async def _fire(self, event_type: str, key: str, text: str, payload: dict, dedupe: str, severity: str) -> None:
        if self.alerts is None:
            logger.warning("cost alert (no latch)", data={"event_type": event_type, **payload})
            return
        await self.alerts.fire(key, text, event_type=event_type, severity=severity, payload=payload,
                               dedupe_key=dedupe, source="cost")

    async def _check_soft(self, g: SpendGroup) -> None:
        if g.name not in SPEND_GROUPS:
            return
        month = self._month_now()
        if f"soft:{g.name}:{month}" in self._state:
            return
        s = app_config.settings
        pct_thr = s.X_SOFT_ALERT_PCT if g.name == "x" else s.COST_SOFT_ALERT_PCT
        if g.name in _ENFORCED_USD:
            cap = self.cap_usd(g.name)
            if not cap or cap <= 0:
                return
            usd = self.month_usd(g.name)
            pct, unit = usd / cap, "usd"
        else:
            rc = self.request_cap(g.name)
            if not rc or rc <= 0:
                return
            usd, cap = self.month_usd(g.name), 0.0
            pct, unit = self.month_requests(g.name) / rc, "requests"
        if pct < pct_thr:
            return
        await self._put_state(f"soft:{g.name}:{month}", {"at": self.clock(), "usd_mtd": usd, "cap": cap, "unit": unit})
        payload = {"provider": g.name, "group": g.name, "month": month, "usd_mtd": round(usd, 4),
                   "cap_usd": cap if unit == "usd" else None, "pct": round(pct, 4), "unit": unit}
        if unit == "requests":
            payload.update(requests=self.month_requests(g.name), request_cap=self.request_cap(g.name))
        await self._fire("spend_80pct", f"spend_80pct:{g.name}:{month}",
                         f"💸 <code>{g.name}</code> at {pct:.0%} of its {month} cap ({unit})", payload,
                         f"{g.name}|{month}|80", "warning")

    async def _check_request_cap(self, g: SpendGroup) -> None:
        rc = self.request_cap(g.name)
        if rc is None or rc <= 0:
            return
        if self.month_requests(g.name) >= rc and not self._hard_latched(g):
            await self._latch_hard(g, self.month_usd(g.name), float(rc), 0.0, unit="requests")

    def _paused_jobs(self, g: SpendGroup) -> list[str]:
        from app.jobs import JOB_TABLE
        return sorted(s.id for s in JOB_TABLE if s.providers and group_of(s.providers[0]).name == g.name)

    async def _latch_hard(self, g: SpendGroup, usd: float, cap: float, est: float, *, unit: str = "usd") -> None:
        month = self._month_now()
        key = f"hard:{g.name}:{month}"
        await self._put_state(key, {"at": self.clock(), "usd_mtd": usd, "cap": cap, "denied_est_usd": est})
        payload = {"provider": g.name, "group": g.name, "month": month, "usd_mtd": round(usd, 4),
                   "cap_usd": cap if unit == "usd" else None, "reserved_usd": round(self.reserved_usd(g.name), 4),
                   "denied_est_usd": est, "paused_jobs": self._paused_jobs(g), "unit": unit}
        if unit == "requests":
            payload.update(requests=self.month_requests(g.name), request_cap=int(cap))
        await self._fire("spend_cap_reached", f"spend_cap_reached:{g.name}:{month}",
                         f"⛔ <code>{g.name}</code> monthly cap reached ({month})", payload,
                         f"{g.name}|{month}|cap", "critical")

    async def set_override(self, group: str, extra_usd: float, *, by: str, reason: str) -> None:
        if group not in SPEND_GROUPS:
            raise ValueError(f"unknown spend group {group!r}")
        if not (0 < extra_usd <= app_config.settings.COST_OVERRIDE_MAX_USD):
            raise ValueError(f"extra_usd must be in (0, {app_config.settings.COST_OVERRIDE_MAX_USD}]")
        month = self._month_now()
        await self._put_state(f"override:{group}:{month}", {"extra_usd": float(extra_usd), "by": by, "reason": reason,
                                                            "at": self.clock()})
        await self._fire("budget_override_set", f"budget_override_set:{group}:{month}:{self.clock()}",
                         f"ℹ️ budget override {group} +${extra_usd:.2f} by {by}",
                         {"group": group, "month": month, "extra_usd": extra_usd, "by": by, "reason": reason,
                          "kind": "override", "provider": group}, None, "info")

    async def clear_override(self, group: str) -> None:
        await self._del_state(f"override:{group}:{self._month_now()}")

    async def set_absolute(self, provider: str, usd: float, *, month: str | None = None, by: str = "cli") -> None:
        month = month or self._month_now()
        await self.db.execute(
            "INSERT INTO provider_spend (provider, month, usd, requests, units_json, updated_at) VALUES (?,?,?,0,'{}',?) "
            "ON CONFLICT(provider, month) DO UPDATE SET usd = excluded.usd, updated_at = excluded.updated_at",
            (provider, month, float(usd), self.clock()))
        g = group_of(provider)
        await self._fire("budget_override_set", f"budget_set:{provider}:{month}:{self.clock()}",
                         f"ℹ️ spend for {provider} set to ${usd:.2f} ({month}) by {by}",
                         {"group": g.name, "month": month, "extra_usd": float(usd), "by": by,
                          "reason": "absolute correction", "kind": "set", "provider": provider}, None, "info")
        await self._load_state()

    # ── /health.spend ──────────────────────────────────────────────────────────────────────────────────────
    def snapshot(self) -> dict:
        out: dict[str, dict[str, Any]] = {}
        month = self._month_now()
        for name, g in SPEND_GROUPS.items():
            usd = self.month_usd(name)
            cap = self.cap_usd(name)
            rc = self.request_cap(name)
            reqs = self.month_requests(name)
            if name in _ENFORCED_USD:
                pct = (usd / cap) if cap else 0.0
            else:
                pct = (reqs / rc) if rc else 0.0
            reason = self.paused_reason(name) or self._pacing_reason.get(name)
            if g.paid and cap is not None and cap <= 0:
                reason = "zero_cap"
            out[name] = {
                "usd": round(usd, 4), "cap_usd": cap, "pct": round(pct, 4),
                "paused": self.is_exhausted(name) or (g.paid and reason == "kill_switch"),
                "reserved_usd": round(self.reserved_usd(name), 4),
                "month": month, "requests": reqs, "request_cap": rc, "paused_reason": reason,
                "override_usd": self.override_usd(name),
                "soft_alerted": f"soft:{name}:{month}" in self._state,
            }
        return out


# ── estimation helpers (spec 10 owns the values; re-verified 2026-10-18 against the claude-api skill) ──────────────
PRICES: dict[str, tuple[float, float]] = {        # USD per MTok (input, output); 10 §2.3/§6.3, skill cache 2026-10-06
    "claude-haiku-5-5": (0.10, 0.50),              # prompts ≤ 100K tokens (TaoSentinel requests are ≈ 10K)
    "claude-sonnet-5-5": (2.0, 10.0),
    "claude-opus-5-5": (4.0, 20.0),
    "claude-haiku-4-5": (1.0, 5.0),
    "claude-sonnet-5": (2.0, 10.0),
    "claude-opus-5": (5.0, 25.0),
}
_DATED = re.compile(r"-\d{8}$")


def _prices() -> dict[str, tuple[float, float]]:
    p = dict(PRICES)
    raw = app_config.settings.LLM_PRICES_JSON
    if raw:
        try:
            for k, v in json.loads(raw).items():
                p[k] = (float(v[0]), float(v[1]))
        except (ValueError, TypeError, IndexError):
            logger.warning("LLM_PRICES_JSON ignored (bad JSON)")
    return p


def estimate_llm_usd(model: str, in_tokens: int, out_tokens: int, *, batch: bool = False,
                     cache_read_tokens: int = 0, cache_write_tokens: int = 0) -> float:
    prices = _prices()
    pin, pout = prices.get(model) or prices.get(_DATED.sub("", model), (0.0, 0.0))   # dated id → its alias
    usd = (in_tokens * pin + out_tokens * pout + cache_read_tokens * pin * 0.1 + cache_write_tokens * pin * 1.25) / 1e6
    return usd * (0.5 if batch else 1.0)


def estimate_batch(texts: Sequence[str], *, model: str, system_tokens: int, n_req: int) -> float:
    tpc = app_config.settings.LLM_TOKENS_PER_CHAR
    in_tok = sum(int(len(t) * tpc) + 40 for t in texts) + system_tokens * max(1, n_req)
    out_tok = 95 * len(texts) + 30 * max(1, n_req)
    return estimate_llm_usd(model, in_tok, out_tok, batch=True)


class CostLedgerJob(CollectorJob):
    """cost_ledger_hourly: flush buffered request counts, refresh latches/month, request-cap alerts."""

    async def collect(self, run: RunContext) -> JobOutcome:
        costs = run.rt.costs
        if costs is None:
            return JobOutcome(note="no governor")
        n = await costs.flush()
        await costs.refresh()
        return JobOutcome(rows=n, note=f"flushed={n}")


def _env_pid() -> int:  # pragma: no cover
    return os.getpid()
