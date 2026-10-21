"""Process-wide per-provider limiter (spec 01 §2.12, §6; 02 §G1; 16 §2.12).

One ``ProviderLimiter`` per process (on ``Runtime``). Token bucket per provider, sliding hourly window, UTC-month
cap seeded from ``provider_spend.requests`` at boot, per-host ``Retry-After`` cooldowns, provider-wide penalties
shared across processes through ``data/run/penalty-<provider>.json``.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal, Mapping

import app.config as app_config
from app.util.time import month_key, now_ts, to_iso


class BudgetExhausted(Exception):
    """Raised before any request is sent when an hourly/monthly request cap is reached (G1)."""

    def __init__(self, provider: str, window: Literal["hour", "month"], until: int) -> None:
        super().__init__(f"{provider} {window} cap reached until {to_iso(until)}")
        self.provider = provider
        self.window = window
        self.until = int(until)


@dataclass(frozen=True, slots=True)
class ProviderLimit:
    rate_per_sec: float | None
    burst: int = 1
    hourly_cap: int | None = None
    monthly_cap: int | None = None
    max_connections: int | None = None
    book_spend: bool = True


class TokenBucket:
    def __init__(self, rate_per_sec: float, burst: int, clock: Callable[[], float] = time.monotonic) -> None:
        self.rate = float(rate_per_sec)
        self.burst = max(1, int(burst))
        self._clock = clock
        self.tokens = float(self.burst)
        self._last = clock()
        self._penalty_until = 0.0

    def _refill(self) -> None:
        now = self._clock()
        if now > self._last:
            self.tokens = min(self.burst, self.tokens + (now - self._last) * self.rate)
        self._last = now

    def penalize(self, seconds: float) -> None:
        self._penalty_until = max(self._penalty_until, self._clock() + max(0.0, seconds))

    def wait_time(self, n: int = 1) -> float:
        self._refill()
        now = self._clock()
        if now < self._penalty_until:
            return self._penalty_until - now
        if self.tokens >= n:
            return 0.0
        return (n - self.tokens) / self.rate if self.rate > 0 else float("inf")

    def try_take(self, n: int = 1) -> bool:
        if self.wait_time(n) == 0.0:
            self.tokens -= n
            return True
        return False

    async def acquire(self, n: int = 1, timeout: float | None = None) -> bool:
        start = self._clock()
        while True:
            w = self.wait_time(n)
            if w == 0.0:
                self.tokens -= n
                return True
            if timeout is not None and (self._clock() - start) + w > timeout:
                return False
            await asyncio.sleep(min(w, 5.0))


# Canonical provider ids (02 §G1) + the internal `deadman` row (16).
PROVIDERS: tuple[str, ...] = (
    "tmc", "tmc_sse", "subnetalpha", "subtensor_rpc", "onfinality", "opentensor_archive", "blockmachine", "github", "github_html",
    "web", "wayback", "discord", "tme", "rss", "youtube", "twitterapi_io", "official_v2", "anthropic", "taostats",
    "rufus", "subnetradar", "telegram", "deadman",
)


# Providers that front many unrelated hosts: Retry-After stays per host (capped), x-ratelimit-* never penalises the
# whole bucket (07 §2.1).
MULTI_HOST_PROVIDERS = frozenset({"web"})
MULTI_HOST_MAX_COOLDOWN_SEC = 3600.0


def limits_from_settings() -> dict[str, ProviderLimit]:
    s = app_config.settings
    gh_token = bool(s.GITHUB_TOKEN)
    onf_keyed = bool(s.ONFINALITY_API_KEY)
    return {
        "tmc": ProviderLimit(s.TMC_RATE_PER_MIN / 60.0, s.TMC_RATE_BURST, s.TMC_HOURLY_CAP, s.TMC_MONTHLY_CAP),
        "tmc_sse": ProviderLimit(None, 1, max_connections=s.TMC_SSE_MAX_CONNECTIONS),
        "subnetalpha": ProviderLimit(s.SUBNETALPHA_RATE_PER_SEC, 1),
        "subtensor_rpc": ProviderLimit(s.SUBTENSOR_RPC_RATE_PER_SEC, 4),
        "onfinality": ProviderLimit(s.ONFINALITY_RATE_PER_SEC if onf_keyed else min(4.0, s.ONFINALITY_RATE_PER_SEC),
                                    20 if onf_keyed else 4),
        "opentensor_archive": ProviderLimit(s.ARCHIVE_RATE_PER_SEC, 4),
        "blockmachine": ProviderLimit(s.BLOCKMACHINE_RATE_PER_SEC, 8, None, s.BLOCKMACHINE_MONTHLY_CAP_REQ),  # 0 = disabled (02 §F2)
        "github": (ProviderLimit(s.GITHUB_RATE_PER_HOUR / 3600.0, 10, s.GITHUB_RATE_PER_HOUR + 500) if gh_token
                   else ProviderLimit(50 / 3600.0, 10, 60)),
        "github_html": ProviderLimit(1.0 / max(0.8, s.GITHUB_HTML_PROBE_PACING_SEC), 1),
        "web": ProviderLimit(s.WEB_RATE_PER_SEC, s.WEB_RATE_BURST),
        "wayback": ProviderLimit(s.WAYBACK_RATE_PER_SEC, 1),
        "discord": ProviderLimit(s.DISCORD_RATE_PER_SEC, 1, 200),
        "tme": ProviderLimit(s.TELEGRAM_COUNTS_RATE_PER_SEC, 1),
        "rss": ProviderLimit(s.RSS_RATE_PER_SEC, 2),
        "youtube": ProviderLimit(s.YOUTUBE_RATE_PER_SEC, 1),
        "twitterapi_io": ProviderLimit(s.X_RATE_PER_SEC, 2),
        "official_v2": ProviderLimit(s.X_OFFICIAL_RATE_PER_SEC, 2),
        "anthropic": ProviderLimit(s.LLM_RATE_PER_SEC, 2),
        "taostats": ProviderLimit(s.TAOSTATS_RATE_PER_MIN / 60.0, 1, None, s.TAOSTATS_MONTHLY_CAP_REQ),
        "rufus": ProviderLimit(1.0, 1),
        "subnetradar": ProviderLimit(0.2, 1),
        "telegram": ProviderLimit(s.TELEGRAM_MAX_MSG_PER_MIN / 60.0, 5),
        "deadman": ProviderLimit(1.0 / (s.OPS_GUARD_INTERVAL_MIN * 60.0), 1, book_spend=False),
    }


class ProviderLimiter:
    def __init__(self, limits: Mapping[str, ProviderLimit], *, mono: Callable[[], float] = time.monotonic,
                 wall: Callable[[], int] = now_ts, run_dir: Path | None = None) -> None:
        self._mono = mono
        self._wall = wall
        self._run_dir = run_dir
        self.limits: dict[str, ProviderLimit] = dict(limits)
        self._buckets: dict[str, TokenBucket] = {}
        self._hour: dict[str, deque[int]] = {p: deque() for p in self.limits}
        self._month: dict[str, tuple[str, int]] = {}
        self._host_until: dict[tuple[str, str], float] = {}
        self._penalty_file_cache: dict[str, tuple[float, float, int]] = {}   # p -> (checked_mono, mtime, until)
        self._connections: dict[str, int] = {}
        self._build_buckets()

    @classmethod
    def from_settings(cls) -> "ProviderLimiter":
        s = app_config.settings
        return cls(limits_from_settings(), run_dir=s.path(s.RUN_DIR))

    def _build_buckets(self) -> None:
        old = self._buckets
        self._buckets = {}
        for p, lim in self.limits.items():
            if lim.rate_per_sec:
                b = TokenBucket(lim.rate_per_sec, lim.burst, clock=self._mono)
                if p in old:
                    b.tokens = min(b.burst, old[p].tokens)
                    b._penalty_until = old[p]._penalty_until
                self._buckets[p] = b
            self._hour.setdefault(p, deque())

    def reload(self) -> None:
        self.limits = limits_from_settings()
        self._build_buckets()

    # ── accounting ─────────────────────────────────────────────────────────────────────────────────────────
    def _lim(self, provider: str) -> ProviderLimit:
        try:
            return self.limits[provider]
        except KeyError:
            raise KeyError(f"unknown provider {provider!r} (no limiter row; 01 §6)") from None

    def month_requests(self, provider: str) -> int:
        mk, n = self._month.get(provider, ("", 0))
        return n if mk == month_key(self._wall()) else 0

    def seed_month(self, provider: str, requests: int) -> None:
        self._month[provider] = (month_key(self._wall()), int(requests))

    def _hour_used(self, provider: str) -> int:
        q = self._hour.setdefault(provider, deque())
        cutoff = self._wall() - 3600
        while q and q[0] <= cutoff:
            q.popleft()
        return len(q)

    def count(self, provider: str) -> None:
        self._lim(provider)
        now = self._wall()
        self._hour.setdefault(provider, deque()).append(now)
        mk = month_key(now)
        cur_mk, n = self._month.get(provider, (mk, 0))
        self._month[provider] = (mk, (n if cur_mk == mk else 0) + 1)

    def exhausted(self, provider: str) -> tuple[str, int] | None:
        lim = self._lim(provider)
        now = self._wall()
        if lim.monthly_cap is not None and self.month_requests(provider) >= lim.monthly_cap:
            from datetime import datetime, timezone
            d = datetime.fromtimestamp(now, timezone.utc)
            nxt = datetime(d.year + (d.month == 12), 1 if d.month == 12 else d.month + 1, 1, tzinfo=timezone.utc)
            return "month", int(nxt.timestamp())
        if lim.hourly_cap is not None and self._hour_used(provider) >= lim.hourly_cap:
            q = self._hour[provider]
            return "hour", (q[0] + 3600) if q else now + 3600
        return None

    # ── penalties ──────────────────────────────────────────────────────────────────────────────────────────
    def _penalty_path(self, provider: str) -> Path | None:
        return (self._run_dir / f"penalty-{provider}.json") if self._run_dir else None

    def _file_penalty_until(self, provider: str) -> int:
        path = self._penalty_path(provider)
        if path is None:
            return 0
        now_m = self._mono()
        cached = self._penalty_file_cache.get(provider)
        if cached and now_m - cached[0] < 5.0:
            return cached[2]
        until = 0
        mtime = 0.0
        try:
            st = path.stat()
            mtime = st.st_mtime
            if cached and cached[1] == mtime:
                until = cached[2]
            else:
                until = int(json.loads(path.read_text()).get("until", 0))
        except (OSError, ValueError):
            until = 0
        self._penalty_file_cache[provider] = (now_m, mtime, until)
        return until

    def penalize(self, provider: str, seconds: float) -> None:
        self._lim(provider)
        b = self._buckets.get(provider)
        if b is not None:
            b.penalize(seconds)
        path = self._penalty_path(provider)
        if path is not None:
            until = self._wall() + int(max(0.0, seconds))
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                tmp = path.with_suffix(".json.tmp")
                tmp.write_text(json.dumps({"until": until, "pid": os.getpid()}))
                os.replace(tmp, path)
                self._penalty_file_cache[provider] = (self._mono(), path.stat().st_mtime, until)
            except OSError:
                pass

    def penalize_host(self, provider: str, host: str, seconds: float) -> None:
        self._lim(provider)
        if provider in MULTI_HOST_PROVIDERS:
            seconds = min(seconds, MULTI_HOST_MAX_COOLDOWN_SEC)
        key = (provider, host or "")
        self._host_until[key] = max(self._host_until.get(key, 0.0), self._mono() + max(0.0, seconds))

    def host_cooldown(self, provider: str, host: str) -> float:
        """Seconds left on a per-host cooldown (``Retry-After``); 0.0 when none (07 skips such hosts per run)."""
        left = self._host_until.get((provider, host or ""), 0.0) - self._mono()
        return left if left > 0 else 0.0

    def observe(self, provider: str, headers: Mapping[str, str], *, host: str | None = None) -> None:
        h = {k.lower(): v for k, v in headers.items()}
        ra = h.get("retry-after")
        if ra is not None:
            try:
                secs = float(ra)
            except ValueError:
                try:   # HTTP-date form (RFC 9110 §10.2.3)
                    from email.utils import parsedate_to_datetime
                    secs = max(0.0, parsedate_to_datetime(ra).timestamp() - self._wall())
                except (TypeError, ValueError, IndexError, OverflowError):
                    secs = 60.0
            self.penalize_host(provider, host or "", secs)
        if provider in MULTI_HOST_PROVIDERS:     # one site's quota headers must not stall every site (07 §2.1)
            return
        rem = h.get("x-ratelimit-remaining") or h.get("x-rate-limit-remaining")
        reset = h.get("x-ratelimit-reset") or h.get("x-rate-limit-reset")
        if rem is not None and reset is not None:
            try:
                if int(float(rem)) <= 0:
                    r = float(reset)
                    wait = r - self._wall() if r > 1e9 else r      # epoch or seconds-until
                    if wait > 0:
                        self.penalize(provider, min(wait, 3600.0))
            except ValueError:
                pass

    # ── acquire ────────────────────────────────────────────────────────────────────────────────────────────
    async def acquire(self, provider: str, *, host: str | None = None, timeout: float | None = None) -> bool:
        self._lim(provider)
        ex = self.exhausted(provider)
        if ex:
            raise BudgetExhausted(provider, ex[0], ex[1])
        start = self._mono()
        while True:
            waits = [0.0]
            file_until = self._file_penalty_until(provider)
            if file_until > self._wall():
                waits.append(float(file_until - self._wall()))
            hu = self._host_until.get((provider, host or ""), 0.0)
            if hu > self._mono():
                waits.append(hu - self._mono())
            b = self._buckets.get(provider)
            if b is not None:
                waits.append(b.wait_time())
            w = max(waits)
            if w <= 0:
                if b is not None:
                    b.tokens -= 1
                return True
            if timeout is not None and (self._mono() - start) + w > timeout:
                return False
            await asyncio.sleep(min(w, 5.0))

    # ── connection-counted providers (tmc_sse) ─────────────────────────────────────────────────────────────
    def open_connection(self, provider: str) -> bool:
        lim = self._lim(provider)
        n = self._connections.get(provider, 0)
        if lim.max_connections is not None and n >= lim.max_connections:
            return False
        self._connections[provider] = n + 1
        return True

    def close_connection(self, provider: str) -> None:
        self._connections[provider] = max(0, self._connections.get(provider, 0) - 1)

    def snapshot(self) -> dict:
        out = {}
        now_w = self._wall()
        for p, lim in self.limits.items():
            b = self._buckets.get(p)
            pen = None
            if b is not None and b._penalty_until > self._mono():
                pen = now_w + int(b._penalty_until - self._mono())
            fu = self._file_penalty_until(p)
            if fu > now_w:
                pen = max(pen or 0, fu)
            ex = self.exhausted(p)
            if b is not None:
                b._refill()
            out[p] = {
                "tokens": round(b.tokens, 3) if b else None,
                "rate_per_sec": round(lim.rate_per_sec, 5) if lim.rate_per_sec else None,
                "burst": lim.burst,
                "hour_used": self._hour_used(p), "hour_cap": lim.hourly_cap,
                "month_used": self.month_requests(p), "month_cap": lim.monthly_cap,
                "penalized_until": to_iso(pen) if pen else None,
                "exhausted_until": to_iso(ex[1]) if ex else None,
            }
            if lim.max_connections is not None:
                out[p]["connections"] = self._connections.get(p, 0)
                out[p]["max_connections"] = lim.max_connections
        return out
