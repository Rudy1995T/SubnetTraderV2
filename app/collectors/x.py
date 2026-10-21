"""X/Twitter collector (spec 09 §2.4–§2.7, §6.4, §7): team timelines, KOL search, mention search, profile refresh.

Every paid call is ``provider.cost_estimate`` → ``costs.check`` → call → ``costs.record(…, requests=0, …)`` (02 §G5;
``Http`` already counted the request). A denial ends the run early (``paused:budget reason=…``, never a failure) after
writing what was fetched; ``auth``/``parse`` errors and a vendor 402 fail the run; ``transient`` errors are counted
per account (``partial:transient``; > 50 % of accounts failing raises); ``rate_limited`` ends the run early.

Attribution and the upsert rule are §2.6's exactly: the first ``observed_at``, ``text``, ``event_at`` and
``occupant_key`` of a post never change; engagement is refreshed with its own timestamp (02 §J2). Legacy-only
``watch_channels`` X rows are ignored (§2.4) — the routing table comes from ``OccupantIndex``, which applies that rule.

CLI: ``python -m app.collectors.x --once team|kol|mentions|profiles|ctf [--dry-run]`` (through 01's wrapper, so flags,
pauses and the governor apply), ``--status``, ``--kol-candidates``.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import dataclasses
import gzip
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable

import app.config as app_config
from app.collectors.community_store import post_hash
from app.collectors.x_handles import chunk_handles, normalise_x_handle
from app.collectors.x_providers import make_provider
from app.collectors.x_providers.base import MentionQuery, XCall, XPage, XPost, XProviderError, XUser, id_gt
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.nlp.kol import DEFAULT_CREDIBILITY, KOL_CANDIDATE_MIN_CRED, KOL_CANDIDATE_MIN_SEEN, KolRegistry, KolRow, \
    author_credibility, is_bot_filtered
from app.nlp.match import OccupantIndex, OccupantRef, build_mention_query, match_occupants
from app.storage.events import emit_event
from app.util.time import month_key, now_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.costs import CostGovernor, Decision
    from app.storage.db import Database

DAY = 86400
DEGRADE_TOP_N = 20                     # §6.4: mentions top-20 once projected spend > 90 % of the cap
DEGRADE_FRACTION = 0.9
SEARCH_LOOKBACK_S = 3600               # §2.5: packed search since_at = last_run_at − 1 h
FIRST_SEARCH_LOOKBACK_S = DAY          # no previous run: one day back (bounded by X_SEARCH_MAX_POSTS_PER_QUERY)
FAIL_RAISE_FRACTION = 0.5              # §7: > 50 % of accounts failing → the run raises
BUDGET_EVENT_REASONS = ("hard_cap", "pacing", "zero_cap")
KOL_CSV_COLUMNS = ("handle", "display_name", "category", "credibility", "verified", "team_occupant_netuids", "notes")
JOB_IDS = {"team": "x_team_poll", "kol": "x_kol_poll", "mentions": "x_mentions_poll", "profiles": "x_profile_refresh",
           "ctf": "x_ctf_crosscheck"}


class BudgetStop(Exception):
    """A governor denial inside a run (§6.4): stop paid calls, keep what was fetched."""

    def __init__(self, reason: str | None) -> None:
        super().__init__(reason or "denied")
        self.reason = reason


class _EndRun(Exception):
    """``rate_limited`` after Http's retries: end the run early (``partial:rate_limited``, §7)."""


def canonical_hash(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str,
                                     ensure_ascii=False).encode("utf-8")).hexdigest()


def _ts(d: datetime) -> int:
    return int(d.timestamp())


def _dt(ts: int) -> datetime:
    return datetime.fromtimestamp(int(ts), timezone.utc)


def _max_id(a: str | None, b: str | None) -> str | None:
    return a if not id_gt(b, a) else b


async def renamed_author(db: "Database", hl: str) -> str | None:
    """A renamed account keeps its old handle in ``watch_channels`` until 04 catches up: map the old handle through
    ``x_handle_changed`` events and earlier snapshots."""
    aid = await db.fetchval(
        "SELECT json_extract(e.payload, '$.author_id') FROM events e JOIN x_accounts a ON a.author_id = "
        "json_extract(e.payload, '$.author_id') WHERE e.event_type = 'x_handle_changed' AND "
        "lower(json_extract(e.payload, '$.old_handle')) = ? ORDER BY e.id DESC LIMIT 1", (hl,))
    if aid:
        return str(aid)
    return await db.fetchval(
        "SELECT s.author_id FROM x_account_snapshots s JOIN x_accounts a ON a.author_id = s.author_id "
        "WHERE lower(s.handle) = ? ORDER BY s.observed_at DESC LIMIT 1", (hl,))


# ── KOL seed (09 §3.4; also used by scripts/build_kol_list.py --load) ──────────────────────────────────────────
def read_kol_csv(path: Path) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        return [dict(r) for r in csv.DictReader(f)]


async def load_kol_seed(db: "Database", path: Path | str, *, now: int | None = None) -> dict[str, int]:
    """``INSERT OR IGNORE`` every seed row (``added_by='seed'``); operator edits are never overwritten. Netuids →
    ``occupant_key`` via ``subnets`` at load time; an empty ``team_occupant_keys`` of an existing row is filled in."""
    t = int(now if now is not None else now_ts())
    subs = {int(r["netuid"]): r["occupant_key"] for r in
            await db.fetchall("SELECT netuid, occupant_key FROM subnets WHERE status <> 'deregistered'")}
    rows, skipped, unmapped = [], 0, 0
    team_keys: dict[str, list[str]] = {}
    for r in read_kol_csv(Path(path)):
        h = normalise_x_handle((r.get("handle") or "").strip())
        cat = (r.get("category") or "analyst").strip()
        if h is None or cat not in DEFAULT_CREDIBILITY:
            skipped += 1
            logger.warning("kol_seed_row_skipped", data={"row": r})
            continue
        cred_s = (r.get("credibility") or "").strip()
        cred = float(cred_s) if cred_s else DEFAULT_CREDIBILITY[cat]
        keys: list[str] = []
        for tok in (r.get("team_occupant_netuids") or "").replace(";", ",").split(","):
            tok = tok.strip()
            if not tok:
                continue
            if tok.isdigit() and int(tok) in subs:
                keys.append(subs[int(tok)])
            else:
                unmapped += 1
        team_keys[h] = keys
        rows.append({"handle": h, "handle_lower": h.lower(), "platform": "x",
                     "display_name": (r.get("display_name") or "").strip() or None, "category": cat,
                     "credibility": max(0.0, min(1.0, cred)),
                     "verified": int((r.get("verified") or "0").strip() == "1"),
                     "active": 1, "team_occupant_keys": json.dumps(keys), "added_at": t, "added_by": "seed",
                     "notes": (r.get("notes") or "").strip() or None, "updated_at": t})
    inserted = await db.insert_or_ignore("kol_accounts", rows)
    filled = 0
    for h, keys in team_keys.items():
        if keys:
            res = await db.execute("UPDATE kol_accounts SET team_occupant_keys = ?, updated_at = ? WHERE handle = ? "
                                   "AND (team_occupant_keys IS NULL OR team_occupant_keys = '[]')",
                                   (json.dumps(keys), t, h))
            filled += res.rowcount or 0
    return {"rows": len(rows), "inserted": inserted, "skipped": skipped, "team_keys_filled": filled,
            "unmapped_netuids": unmapped}


# ── the collector ─────────────────────────────────────────────────────────────────────────────────────────────
class XCollector:
    def __init__(self, db: "Database", provider: Any, costs: "CostGovernor | None", http: Any = None, *,
                 job_id: str, run_id: str, now: int | None = None, settings: Any = None,
                 clock: Callable[[], int] = now_ts) -> None:
        self.db = db
        self.provider = provider
        self.costs = costs
        self.http = http
        self.job_id = job_id
        self.run_id = run_id
        self.clock = clock
        self.now = int(now if now is not None else clock())
        self.s = settings or app_config.settings
        self.index: OccupantIndex = OccupantIndex([])
        self.kols: KolRegistry = KolRegistry([])
        self.kol_by_id: dict[str, KolRow] = {}
        self.team_ids: set[str] = set()
        self.acct_occ: dict[str, list[str]] = {}
        self.cost_usd = 0.0
        self.calls = 0
        self.rows = 0
        self.posts_seen = 0
        self.partial: str | None = None
        self.paused: str | None = None
        self.attempted = 0
        self.failed = 0
        self.resolved = 0
        self.unresolved = 0
        self._allowed_seen = False
        self._seen: set[str] = set()
        self.degraded = False

    # ── setup ─────────────────────────────────────────────────────────────────────────────────────────────
    async def load(self) -> None:
        self.index = await OccupantIndex.load(self.db)
        self.kols = await KolRegistry.load(self.db)
        self.kol_by_id = {r.author_id: r for r in self.kols.rows.values() if r.author_id}
        await self._load_accounts()

    async def _load_accounts(self) -> None:
        self.team_ids = {r["author_id"] for r in
                         await self.db.fetchall("SELECT author_id FROM x_accounts WHERE kind = 'team'")}
        self.acct_occ = {}
        for r in await self.db.fetchall("SELECT author_id, occupant_key FROM x_account_occupants "
                                        "ORDER BY occupant_key"):
            self.acct_occ.setdefault(r["author_id"], []).append(r["occupant_key"])

    async def _author_for_handle(self, hl: str) -> str | None:
        aid = await self.db.fetchval("SELECT author_id FROM x_accounts WHERE handle_lower = ?", (hl,))
        if aid:
            return aid
        return await renamed_author(self.db, hl)

    async def sync_account_occupants(self) -> dict[str, int]:
        """§2.4: re-sync the routing table from active, non-legacy-only ``watch_channels(platform='x')`` rows."""
        want: set[tuple[str, str]] = set()
        for hl, okeys in self.index.by_handle.items():
            aid = await self._author_for_handle(hl)
            if aid is None:
                continue
            await self.db.execute("UPDATE x_accounts SET kind = 'team' WHERE author_id = ? AND kind <> 'team'",
                                  (aid,))                                       # one row per account, team wins
            for ok in okeys:
                want.add((aid, ok))
        have = {(r["author_id"], r["occupant_key"]) for r in await self.db.fetchall(
            "SELECT author_id, occupant_key FROM x_account_occupants WHERE source = 'watch_channels'")}
        added = want - have
        removed = have - want
        if added:
            await self.db.insert_or_ignore("x_account_occupants", [
                {"author_id": a, "occupant_key": o, "source": "watch_channels"} for a, o in sorted(added)])
        for a, o in sorted(removed):
            await self.db.execute("DELETE FROM x_account_occupants WHERE author_id = ? AND occupant_key = ? "
                                  "AND source = 'watch_channels'", (a, o))
        await self._load_accounts()
        return {"pairs": len(want), "added": len(added), "removed": len(removed)}

    # ── governor (§6.4) ───────────────────────────────────────────────────────────────────────────────────
    async def _paid(self, call: XCall, fn: Callable[[], Awaitable[Any]], unit: str) -> Any:
        est = float(self.provider.cost_estimate(call))
        if self.costs is not None:
            d = await self.costs.check(self.provider.name, est)
            if not d.allowed:
                await self._on_denied(d)
                raise BudgetStop(d.reason)
            await self._on_allowed(d)
        self.calls += 1
        result = await fn()
        page = result if isinstance(result, XPage) else None
        actual = float(self.provider.cost_actual(call, page))
        n = page.billed_posts if page is not None else 1
        if self.costs is not None:
            await self.costs.record(self.provider.name, actual, requests=0, units={unit: int(n)}, job=self.job_id,
                                    run_id=self.run_id)
        self.cost_usd += actual
        return result

    async def _on_denied(self, d: "Decision") -> None:
        self.paused = d.reason or "denied"
        logger.info("x_budget_denied", data={"job": self.job_id, "provider": self.provider.name, "reason": d.reason,
                                              "usd_mtd": d.usd_mtd, "cap_usd": d.cap_usd})
        if d.reason in BUDGET_EVENT_REASONS:
            await self._budget_paused_event(d.reason, d.usd_mtd, d.cap_usd, d.projected_usd)

    async def _budget_paused_event(self, reason: str, usd_mtd: Any, cap_usd: Any, projected: Any) -> None:
        prov = self.provider.name
        await emit_event(self.db, event_type="x_budget_paused", occupant_key=None, netuid=None, event_at=self.now,
                         observed_at=self.clock(), payload={"provider": prov, "usd_mtd": usd_mtd, "cap_usd": cap_usd,
                                                            "projected_usd": projected, "reason": reason},
                         dedupe_key=f"{prov}|{month_key(self.now)}", source="x", run_id=self.run_id)

    async def _on_allowed(self, d: "Decision") -> None:
        if not self._allowed_seen:
            # x_budget_resumed: the first allowed check after an x_budget_paused of this provider that has no
            # resume yet — a raised cap within the month or the month rollover (dedupe provider|month)
            self._allowed_seen = True
            prov, month = self.provider.name, month_key(self.now)
            like = prov.replace("_", "\\_") + "|%"
            last_p = await self.db.fetchval("SELECT MAX(id) FROM events WHERE event_type = 'x_budget_paused' AND "
                                            "dedupe_key LIKE ? ESCAPE '\\'", (like,))
            last_r = await self.db.fetchval("SELECT MAX(id) FROM events WHERE event_type = 'x_budget_resumed' AND "
                                            "dedupe_key LIKE ? ESCAPE '\\'", (like,))
            if last_p is not None and (last_r is None or int(last_r) < int(last_p)):
                await emit_event(self.db, event_type="x_budget_resumed", occupant_key=None, netuid=None,
                                 event_at=self.now, observed_at=self.clock(),
                                 payload={"provider": prov, "month": month},
                                 dedupe_key=f"{prov}|{month}", source="x", run_id=self.run_id)
        if (not self.degraded and d.projected_usd is not None and d.cap_usd
                and float(d.projected_usd) > DEGRADE_FRACTION * float(d.cap_usd)):
            self.degraded = True
            key = f"degrade_top20:{month_key(self.now)}"
            await self.db.upsert("job_state", [{"job": JOB_IDS["mentions"], "key": key,
                                                "value_json": json.dumps({"at": self.now,
                                                                          "projected_usd": d.projected_usd,
                                                                          "cap_usd": d.cap_usd}),
                                                "updated_at": self.now}], conflict=("job", "key"))
            logger.info("x_mentions_degraded_top20", data={"projected_usd": d.projected_usd, "cap_usd": d.cap_usd})

    async def degraded_this_month(self) -> bool:
        return bool(await self.db.fetchval("SELECT 1 FROM job_state WHERE job = ? AND key = ?",
                                           (JOB_IDS["mentions"], f"degrade_top20:{month_key(self.now)}")))

    # ── error policy (§7) ─────────────────────────────────────────────────────────────────────────────────
    async def _guarded(self, coro: Awaitable[Any], *, author_id: str | None = None) -> bool:
        """Run one account/query; True on success. BudgetStop/auth/parse/budget propagate."""
        self.attempted += 1
        try:
            await coro
            return True
        except XProviderError as exc:
            if exc.kind in ("transient", "not_found"):
                self.failed += 1
                self.partial = self.partial or "transient"
                if author_id:
                    await self.db.execute("UPDATE x_accounts SET consecutive_failures = consecutive_failures + 1, "
                                          "last_error = ? WHERE author_id = ?", (str(exc)[:300], author_id))
                logger.warning("x_account_failed", data={"job": self.job_id, "author_id": author_id,
                                                          "error": str(exc)[:200]})
                return False
            if exc.kind == "rate_limited":
                self.failed += 1
                self.partial = "rate_limited"
                raise _EndRun(str(exc)) from exc
            if exc.kind == "budget":
                await self._budget_paused_event("provider_balance", self._usd_mtd(), self._cap(), None)
                raise RuntimeError(f"provider_balance: {exc}") from exc
            raise

    def _usd_mtd(self) -> float | None:
        try:
            return float(self.costs.month_usd("x")) if self.costs is not None else None
        except Exception:  # pragma: no cover
            return None

    def _cap(self) -> float | None:
        return float(self.s.X_MONTHLY_CAP_USD)

    def _check_failure_ratio(self) -> None:
        if self.attempted and self.failed > FAIL_RAISE_FRACTION * self.attempted and self.partial == "transient":
            raise RuntimeError(f"{self.failed}/{self.attempted} X accounts failed (partial:transient)")

    # ── resolution step (§2.5) ────────────────────────────────────────────────────────────────────────────
    def _team_targets(self) -> dict[str, tuple[str, list[str]]]:
        out: dict[str, tuple[str, list[str]]] = {}
        for r in self.index.refs:
            for h in r.handles:
                hl = h.lower()
                if hl not in out:
                    out[hl] = (h, list(self.index.by_handle.get(hl, [])))
        return out

    async def _resolve_targets(self, kind: str, *, link: bool = True) -> list[tuple[str, str, list[str]]]:
        """(handle, kind, occupant_keys) with no ``x_accounts`` row and no running back-off."""
        if kind == "team":
            cand = [(h, okeys) for h, okeys in self._team_targets().values()]
        else:
            cand = [(k.handle, list(self.index.by_handle.get(k.handle_lower, []))) for k in self.kols.pollable()]
        out = []
        for h, okeys in cand:
            hl = h.lower()
            aid = await self._author_for_handle(hl)
            if aid is not None:
                if kind == "kol" and link:
                    await self._link_kol(hl, aid)
                continue
            st = await self._state_get(f"unresolved:{hl}")
            if st and int(st.get("next_retry_at") or 0) > self.now:
                continue
            out.append((h, "team" if (kind == "team" or okeys) else "kol", okeys))
        return out

    async def _link_kol(self, hl: str, author_id: str) -> None:
        k = self.kols.get(hl)
        if k is None:
            return
        await self.db.execute("UPDATE kol_accounts SET author_id = ?, updated_at = ? WHERE handle = ? "
                              "AND (author_id IS NULL OR author_id <> ?)", (author_id, self.now, k.handle, author_id))
        await self.db.execute("UPDATE x_accounts SET kol_handle = ? WHERE author_id = ? AND kol_handle IS NULL",
                              (k.handle, author_id))
        self.kol_by_id[author_id] = dataclasses.replace(k, author_id=author_id)

    async def _state_get(self, key: str, job: str | None = None) -> dict | None:
        v = await self.db.fetchval("SELECT value_json FROM job_state WHERE job = ? AND key = ?",
                                   (job or self.job_id, key))
        return json.loads(v) if v else None

    async def _state_put(self, key: str, value: Any, job: str | None = None) -> None:
        await self.db.upsert("job_state", [{"job": job or self.job_id, "key": key,
                                            "value_json": json.dumps(value, default=str), "updated_at": self.now}],
                             conflict=("job", "key"))

    async def _state_del(self, key: str, job: str | None = None) -> None:
        await self.db.execute("DELETE FROM job_state WHERE job = ? AND key = ?", (job or self.job_id, key))

    async def resolve_new_accounts(self, kind: str) -> int:
        n = 0
        for handle, k, okeys in await self._resolve_targets(kind):
            hl = handle.lower()
            try:
                user = await self._paid(XCall("resolve_user", expected_posts=0),
                                        lambda h=handle: self.provider.resolve_user(h), "profiles")
            except XProviderError as exc:
                if exc.kind in ("transient", "not_found", "rate_limited"):
                    await self._unresolved(handle, k, okeys, "error")
                    if exc.kind == "rate_limited":
                        self.partial = "rate_limited"
                        raise _EndRun(str(exc)) from exc
                    self.partial = self.partial or "transient"
                    continue
                if exc.kind == "budget":
                    await self._budget_paused_event("provider_balance", self._usd_mtd(), self._cap(), None)
                    raise RuntimeError(f"provider_balance: {exc}") from exc
                raise
            if user is None:
                await self._unresolved(handle, k, okeys, "not_found")
                continue
            if await self.store_resolved(user, declared=handle, kind=k):
                n += 1
        self.resolved += n
        return n

    async def _unresolved(self, handle: str, kind: str, okeys: list[str], reason: str) -> None:
        hl = handle.lower()
        key = f"unresolved:{hl}"
        prev = await self._state_get(key) or {}
        nf = int(prev.get("not_found_count") or 0)
        st = {"handle": handle, "kind": kind, "reason": reason, "consecutive": int(prev.get("consecutive") or 0) + 1,
              "next_retry_at": self.now + int(self.s.X_UNRESOLVED_RETRY_DAYS) * DAY, "first_at": prev.get("first_at")
              or self.now, "last_at": self.now, "not_found_count": nf + (reason == "not_found"),
              "not_found_first_at": prev.get("not_found_first_at") or (self.now if reason == "not_found" else None),
              "not_found_last_at": self.now if reason == "not_found" else prev.get("not_found_last_at")}
        await self._state_put(key, st)
        self.unresolved += 1
        occ = okeys[0] if (kind == "team" and len(okeys) == 1) else None
        payload: dict[str, Any] = {"handle": handle, "kind": kind, "reason": reason, "consecutive": st["consecutive"],
                                   "provider": self.provider.name}
        if kind == "team" and len(okeys) > 1:
            payload["occupant_keys"] = okeys
        await emit_event(self.db, event_type="x_account_unresolved", occupant_key=occ,
                         netuid=int(occ.split(":")[0]) if occ else None, event_at=self.now, observed_at=self.clock(),
                         payload=payload, dedupe_key=f"{hl}|{reason}|{self.now // 604800}", source="x",
                         run_id=self.run_id)

    async def store_resolved(self, user: XUser, *, declared: str, kind: str) -> bool:
        """Write ``x_accounts`` (+ snapshot, ``kol_accounts.author_id``, X ``verified_at``). Team wins (§2.4)."""
        handle = user.handle or declared
        hl = handle.lower()
        dl = declared.lower()
        is_team = kind == "team" or hl in self.index.by_handle or dl in self.index.by_handle
        kol = self.kols.get(dl) or self.kols.get(hl)
        clash = await self.db.fetchval("SELECT author_id FROM x_accounts WHERE handle_lower = ? AND author_id <> ?",
                                       (hl, user.id))
        if clash:
            logger.warning("x_handle_clash", data={"handle": handle, "author_id": user.id, "other": clash})
            return False
        cur = await self.db.fetchone("SELECT * FROM x_accounts WHERE author_id = ?", (user.id,))
        if cur is None:
            await self.db.execute(
                "INSERT INTO x_accounts (author_id, handle, handle_lower, kind, kol_handle, status, resolved_at, "
                "provider, created_at) VALUES (?,?,?,?,?,'active',?,?,?)",
                (user.id, handle, hl, "team" if is_team else "kol", kol.handle if kol else None, self.now,
                 self.provider.name, self.now))
        else:
            await self.db.execute(
                "UPDATE x_accounts SET handle = ?, handle_lower = ?, kind = CASE WHEN kind = 'team' OR ? THEN 'team' "
                "ELSE 'kol' END, kol_handle = COALESCE(kol_handle, ?), status = 'active', "
                "resolved_at = COALESCE(resolved_at, ?), provider = ? WHERE author_id = ?",
                (handle, hl, int(is_team), kol.handle if kol else None, self.now, self.provider.name, user.id))
        await self._snapshot(user)
        await self.db.execute("UPDATE watch_channels SET verified_at = ? WHERE platform = 'x' AND retired_at IS NULL "
                              "AND lower(handle) IN (?, ?)", (self.now, hl, dl))
        if kol is not None:
            await self._link_kol(kol.handle_lower, user.id)
        for k in {hl, dl}:
            await self._state_del(f"unresolved:{k}", job=JOB_IDS["team"])
            await self._state_del(f"unresolved:{k}", job=JOB_IDS["kol"])
        if is_team:
            self.team_ids.add(user.id)
            okeys = self.index.by_handle.get(hl) or self.index.by_handle.get(dl) or []
            if okeys:
                await self.db.insert_or_ignore("x_account_occupants", [
                    {"author_id": user.id, "occupant_key": o, "source": "watch_channels"} for o in okeys])
                await self._load_accounts()
        return True

    async def _snapshot(self, user: XUser) -> None:
        await self.db.insert_or_ignore("x_account_snapshots", [{
            "author_id": user.id, "observed_at": self.clock(), "handle": user.handle, "followers": user.followers,
            "following": user.following, "post_count": user.post_count,
            "account_created_at": _ts(user.created_at) if user.created_at else None,
            "verified": None if user.verified is None else int(user.verified), "provider": self.provider.name,
            "source": "x_profile", "event_at": None, "run_id": self.run_id, "raw_json_hash": canonical_hash(user.raw)}])

    # ── polling ───────────────────────────────────────────────────────────────────────────────────────────
    async def _team_accounts(self) -> list[dict]:
        """``kind='team'`` accounts in status active/unresolved that still route to an occupant (§2.5)."""
        return await self.db.fetchall(
            "SELECT * FROM x_accounts a WHERE kind = 'team' AND status IN ('active','unresolved') AND EXISTS "
            "(SELECT 1 FROM x_account_occupants o WHERE o.author_id = a.author_id) ORDER BY handle_lower")

    async def _kol_accounts(self) -> list[dict]:
        out = []
        for k in self.kols.pollable():
            a = None
            if k.author_id:
                a = await self.db.fetchone("SELECT * FROM x_accounts WHERE author_id = ?", (k.author_id,))
            if a is None:
                a = await self.db.fetchone("SELECT * FROM x_accounts WHERE handle_lower = ?", (k.handle_lower,))
            if a is not None and a["status"] != "gone":
                out.append(a)
        return out

    def _timeline_expected(self, since_id: str | None) -> int:
        if since_id is None and self.provider.name == "official_v2":
            return 5
        return int(getattr(self.provider, "timeline_page_size", 20) or 20)

    async def poll_timeline(self, acct: dict, *, match_kind: str = "team") -> tuple[int, float]:
        """One governor check per page; first poll = one page (§2.3). ``since_id`` advances to the newest id seen only
        when every page of the run succeeded (an interrupted run re-reads, the upsert dedupes)."""
        since = acct.get("since_id")
        pages = 1 if since is None else max(1, int(self.s.X_TIMELINE_MAX_PAGES))
        size = int(getattr(self.provider, "timeline_page_size", 20) or 20)
        posts: list[XPost] = []
        newest = since
        cursor = None
        c0, r0 = self.cost_usd, self.rows
        ok = False
        try:
            for _ in range(pages):
                page = await self._paid(XCall("user_timeline", expected_posts=self._timeline_expected(since)),
                                        lambda c=cursor: self.provider.user_timeline(acct["author_id"], since, size, c),
                                        "posts")
                posts += page.posts
                newest = _max_id(newest, page.newest_id)
                self._archive(page.posts)
                if not page.next_cursor:
                    break
                cursor = page.next_cursor
            ok = True
        finally:
            if posts:
                await self.ingest(posts, source=self.provider.name, match_kind=match_kind, handle_fill=acct["handle"])
            if posts or ok:
                last_post = max((_ts(p.event_at) for p in posts), default=None)
                await self.db.execute(
                    "UPDATE x_accounts SET since_id = ?, "
                    "last_post_at = MAX(COALESCE(last_post_at, 0), COALESCE(?, 0)), "
                    "last_polled_at = CASE WHEN ? THEN ? ELSE last_polled_at END, "
                    "consecutive_failures = CASE WHEN ? THEN 0 ELSE consecutive_failures END, "
                    "last_error = CASE WHEN ? THEN NULL ELSE last_error END WHERE author_id = ?",
                    (newest if ok else since, last_post, int(ok), self.now, int(ok), int(ok), acct["author_id"]))
                await self.db.execute("UPDATE x_accounts SET last_post_at = NULL WHERE author_id = ? AND "
                                      "last_post_at = 0", (acct["author_id"],))
        return self.rows - r0, self.cost_usd - c0

    async def _search_pages(self, q: MentionQuery, since_id: str | None, since_at: datetime | None,
                            acc: list[XPost], max_posts: int) -> str | None:
        """Page a search until ``max_posts`` or exhaustion; posts are appended to ``acc`` (kept on BudgetStop)."""
        newest = since_id
        cursor = None
        size = int(getattr(self.provider, "search_page_size", 20) or 20)
        while len(acc) < max_posts:
            left = max_posts - len(acc)
            page = await self._paid(XCall("search_recent", expected_posts=min(size, left)),
                                    lambda c=cursor, n=left: self.provider.search_recent(q, since_id, since_at, n, c),
                                    "posts")
            acc += page.posts
            newest = _max_id(newest, page.newest_id)
            self._archive(page.posts)
            if not page.next_cursor or not page.posts:
                break
            cursor = page.next_cursor
        return newest

    async def poll_packed_search(self, handles: list[str], query_key: str, *, match_kind: str) -> tuple[int, float]:
        st = await self.db.fetchone("SELECT * FROM x_search_state WHERE query_key = ?", (query_key,)) or {}
        since_id = st.get("since_id")
        last = st.get("last_run_at")
        since_at = _dt((int(last) - SEARCH_LOOKBACK_S) if last else (self.now - FIRST_SEARCH_LOOKBACK_S))
        q = MentionQuery(terms=[], required_coterms=[], from_handles=list(handles))
        return await self._run_search(q, query_key, since_id, since_at, match_kind=match_kind, occupant_hint=None)

    async def poll_mention(self, ref: OccupantRef) -> tuple[int, float]:
        key = f"mention:{ref.occupant_key}"
        st = await self.db.fetchone("SELECT * FROM x_search_state WHERE query_key = ?", (key,)) or {}
        q = build_mention_query(ref, list(self.index.handles_of(ref.occupant_key)))
        return await self._run_search(q, key, st.get("since_id"), None, match_kind="query",
                                      occupant_hint=ref.occupant_key)

    async def _run_search(self, q: MentionQuery, key: str, since_id: str | None, since_at: datetime | None, *,
                          match_kind: str, occupant_hint: str | None) -> tuple[int, float]:
        acc: list[XPost] = []
        c0, r0 = self.cost_usd, self.rows
        newest = None
        try:
            newest = await self._search_pages(q, since_id, since_at, acc, int(self.s.X_SEARCH_MAX_POSTS_PER_QUERY))
        finally:
            if acc:
                await self.ingest(acc, source=self.provider.name, match_kind=match_kind, occupant_hint=occupant_hint)
        await self.db.upsert("x_search_state", [{"query_key": key, "since_id": newest or since_id,
                                                 "last_run_at": self.now, "last_posts": len(acc),
                                                 "last_cost_usd": round(self.cost_usd - c0, 8)}],
                             conflict=("query_key",))
        return self.rows - r0, self.cost_usd - c0

    def _archive(self, posts: list[XPost]) -> None:
        if not posts or not bool(self.s.X_RAW_ARCHIVE_ENABLED):
            return
        try:
            day = datetime.fromtimestamp(self.now, timezone.utc).strftime("%Y-%m-%d")
            p = self.s.path(self.s.DATA_DIR) / "raw" / "x" / self.provider.name / f"{day}.jsonl.gz"
            p.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(p, "at", encoding="utf-8") as f:
                for x in posts:
                    f.write(json.dumps({"observed_at": self.now, "run_id": self.run_id, "job": self.job_id,
                                        "post_id": x.post_id, "raw": x.raw}, default=str, ensure_ascii=False) + "\n")
        except OSError as exc:  # pragma: no cover - the archive never fails a run
            logger.warning("x_raw_archive_failed", data={"error": str(exc)})

    # ── ingest (§2.6) ─────────────────────────────────────────────────────────────────────────────────────
    async def ingest(self, posts: list[XPost], *, source: str, match_kind: str, occupant_hint: str | None = None,
                     handle_fill: str | None = None) -> int:
        """Normalise + credibility + upsert + mention rows; returns the number of new ``social_posts`` rows."""
        batch: dict[str, XPost] = {}
        for p in posts:
            if handle_fill and not p.author_handle:
                p = dataclasses.replace(p, author_handle=handle_fill)
            batch[p.post_id] = p
        items = list(batch.values())
        window = int(self.s.X_DUPLICATE_WINDOW_MIN) * 60
        threshold = float(self.s.X_MIN_CREDIBILITY)
        new = 0
        for p in items:
            obs = int(self.clock())
            th = post_hash(p.text, f"x:{p.post_id}")
            ev = _ts(p.event_at)
            is_team = p.author_id in self.team_ids
            kol = self.kol_by_id.get(p.author_id) or self.kols.get(p.author_handle)
            if kol is not None and not (kol.verified and kol.active):
                kol = None
            db_self = int(await self.db.fetchval(
                "SELECT COUNT(*) FROM social_posts WHERE text_hash = ? AND author_id = ? AND post_id <> ? AND "
                "event_at BETWEEN ? AND ?", (th, p.author_id, p.post_id, ev - window, ev + window)) or 0)
            db_coord = int(await self.db.fetchval(
                "SELECT COUNT(DISTINCT author_id) FROM social_posts WHERE text_hash = ? AND author_id <> ? AND "
                "post_id <> ? AND event_at BETWEEN ? AND ?", (th, p.author_id, p.post_id, ev - window, ev + window))
                or 0)
            cred, flags = author_credibility(p.author, p, items, db_self, kol, is_team, db_coord=db_coord,
                                             window_s=window)
            own = list(self.acct_occ.get(p.author_id, [])) if is_team else []
            occ: str | None = None
            mentions: list[tuple[str, str]] = []
            if own:
                if len(own) == 1:
                    occ = own[0]
                else:
                    hits = [k for k, _ in match_occupants(p.text, self.index, restrict_to=own)]
                    if len(hits) == 1:
                        occ = hits[0]
                    else:
                        mentions += [(k, "team") for k in own]
            if match_kind == "query" and occupant_hint and occupant_hint not in own:
                mentions.append((occupant_hint, "query"))
            covered = set(own) | {k for k, mk in mentions if mk == "query"}
            mentions += [(k, "rule") for k, _ in match_occupants(p.text, self.index) if k not in covered]
            has_eng = any(v is not None for v in (p.like_count, p.repost_count, p.reply_count, p.quote_count,
                                                  p.view_count))
            existed = await self.db.fetchval("SELECT 1 FROM social_posts WHERE platform = 'x' AND post_id = ?",
                                             (p.post_id,))
            async with self.db.transaction():
                await self.db.execute(UPSERT_SQL, (
                    p.post_id, p.author_id, p.author_handle or None, occ, int(is_team), int(kol is not None),
                    kol.handle if kol is not None else None, p.text, th, p.lang,
                    f"https://x.com/{p.author_handle or 'i/web'}/status/{p.post_id}", p.like_count, p.repost_count,
                    p.reply_count, p.quote_count, p.view_count, obs if has_eng else None, int(p.has_url),
                    int(p.has_media), p.in_reply_to, int(p.is_repost), p.quoted_post_id, round(cred, 6),
                    json.dumps(flags), int(is_bot_filtered(cred, is_team=is_team, threshold=threshold)), ev, obs,
                    source, self.run_id, canonical_hash(p.raw)))
                if mentions:
                    await self.db.executemany(
                        "INSERT OR IGNORE INTO social_post_mentions (platform, post_id, occupant_key, match_kind, "
                        "event_at, observed_at, run_id) VALUES ('x',?,?,?,?,?,?)",
                        [(p.post_id, k, mk, ev, obs, self.run_id) for k, mk in mentions])
            if not existed and p.post_id not in self._seen:
                new += 1
            self._seen.add(p.post_id)
        self.rows += new
        self.posts_seen += len(items)
        return new

    # ── top-N (§2.5) ──────────────────────────────────────────────────────────────────────────────────────
    async def top_n_occupants(self, n: int) -> list[OccupantRef]:
        live = self.index.by_key
        latest: dict[str, tuple[int, float]] = {}
        for r in await self.db.fetchall(
                "SELECT occupant_key, as_of, value FROM metric_values WHERE metric_key = 'emission_share' AND "
                "stage = 'raw' AND status = 'ok' AND as_of >= ? AND value IS NOT NULL", (self.now - 2 * DAY,)):
            k = r["occupant_key"]
            if k in live and (k not in latest or int(r["as_of"]) > latest[k][0]):
                latest[k] = (int(r["as_of"]), float(r["value"]))
        ranked = [k for k, _ in sorted(latest.items(), key=lambda kv: (-kv[1][1], kv[0]))]
        if len(ranked) < n:                                   # before Phase 8: latest chain_hourly.emission
            hourly: dict[str, tuple[int, float]] = {}
            for r in await self.db.fetchall(
                    "SELECT occupant_key, hour_ts, emission FROM chain_hourly WHERE hour_ts >= ? AND emission IS NOT "
                    "NULL", (self.now - 2 * DAY,)):
                k = r["occupant_key"]
                if k in live and (k not in hourly or int(r["hour_ts"]) > hourly[k][0]):
                    hourly[k] = (int(r["hour_ts"]), float(r["emission"]))
            extra = [k for k, _ in sorted(hourly.items(), key=lambda kv: (-kv[1][1], kv[0])) if k not in latest]
            ranked += extra
        return [live[k] for k in ranked[:n]]

    # ── jobs ──────────────────────────────────────────────────────────────────────────────────────────────
    async def _run(self, body: Callable[[], Awaitable[None]], label: str) -> JobOutcome:
        try:
            await body()
        except BudgetStop:
            pass
        except _EndRun:
            pass
        self._check_failure_ratio()
        return await self.outcome(label)

    async def run_team_poll(self, run: RunContext | None = None) -> JobOutcome:
        await self.load()

        async def body() -> None:
            await self.sync_account_occupants()
            await self.resolve_new_accounts("team")
            accts = await self._team_accounts()
            if str(self.s.X_TEAM_POLL_MODE) == "search":
                await self._poll_search_accounts(accts, "team", match_kind="team")
            else:
                for a in accts:
                    await self._guarded(self.poll_timeline(a, match_kind="team"), author_id=a["author_id"])
        return await self._run(body, "team")

    async def _poll_search_accounts(self, accts: list[dict], prefix: str, *, match_kind: str) -> bool:
        by_handle = {a["handle"]: a for a in accts}
        all_ok = True
        for i, chunk in enumerate(chunk_handles(list(by_handle), 25, 512)):
            ok = await self._guarded(self.poll_packed_search(chunk, f"{prefix}:{i}", match_kind=match_kind))
            all_ok = all_ok and ok
            if ok:
                ids = [by_handle[h]["author_id"] for h in chunk]
                await self.db.executemany("UPDATE x_accounts SET last_polled_at = ?, consecutive_failures = 0, "
                                          "last_error = NULL WHERE author_id = ?", [(self.now, a) for a in ids])
        return all_ok

    async def run_kol_poll(self, run: RunContext | None = None) -> JobOutcome:
        if not await self.db.fetchval("SELECT 1 FROM kol_accounts LIMIT 1"):
            path = self.s.path(self.s.KOL_SEED_PATH)
            if Path(path).exists():
                logger.info("kol_seed_loaded", data=await load_kol_seed(self.db, path, now=self.now))
        await self.load()

        async def body() -> None:
            await self.resolve_new_accounts("kol")
            self.kols = await KolRegistry.load(self.db)
            self.kol_by_id = {r.author_id: r for r in self.kols.rows.values() if r.author_id}
            accts = await self._kol_accounts()
            if str(self.s.X_KOL_POLL_MODE) == "timeline":
                for a in accts:
                    await self._guarded(self.poll_timeline(a, match_kind="rule"), author_id=a["author_id"])
            else:
                await self._poll_search_accounts(accts, "kol", match_kind="rule")
        return await self._run(body, "kol")

    async def run_mentions_poll(self, run: RunContext | None = None) -> JobOutcome:
        await self.load()
        n = int(self.s.X_MENTIONS_TOP_N)
        if await self.degraded_this_month():
            self.degraded = True
            n = min(n, DEGRADE_TOP_N)
        occs = await self.top_n_occupants(n)
        await self._state_put("last_top", {"at": self.now, "n": n, "keys": [o.occupant_key for o in occs]})

        async def body() -> None:
            for i, ref in enumerate(occs):
                if self.degraded and i >= DEGRADE_TOP_N:
                    logger.info("x_mentions_degraded_top20", data={"stopped_at": i})
                    break
                await self._guarded(self.poll_mention(ref))
        return await self._run(body, "mentions")

    async def run_profile_refresh(self, run: RunContext | None = None) -> JobOutcome:
        await self.load()

        async def body() -> None:
            age = max(0, int(self.s.X_PROFILE_REFRESH_DAYS) - 1) * DAY
            rows = await self.db.fetchall(
                "SELECT a.*, (SELECT MAX(observed_at) FROM x_account_snapshots s WHERE s.author_id = a.author_id) "
                "AS snap_at FROM x_accounts a WHERE a.kind = 'team' OR EXISTS (SELECT 1 FROM kol_accounts k WHERE "
                "k.verified = 1 AND (k.author_id = a.author_id OR k.handle_lower = a.handle_lower)) "
                "ORDER BY a.handle_lower")
            for a in rows:
                if a["snap_at"] is not None and int(a["snap_at"]) > self.now - age:
                    continue
                await self._guarded(self.refresh_profile(a), author_id=a["author_id"])
        return await self._run(body, "profiles")

    async def refresh_profile(self, a: dict) -> None:
        by_id = bool(getattr(self.provider, "supports_by_id", True))
        user = await self._paid(XCall("resolve_user", expected_posts=0),
                                (lambda: self.provider.resolve_user_by_id(a["author_id"])) if by_id else
                                (lambda: self.provider.resolve_user(a["handle"])), "profiles")
        key = f"none:{a['author_id']}"
        if user is None:
            prev = await self._state_get(key) or {}
            n = int(prev.get("consecutive") or 0) + 1
            since = prev.get("first_at") or self.now
            await self._state_put(key, {"consecutive": n, "first_at": since, "last_at": self.now})
            status = "gone" if n >= 2 else "unresolved"
            await self.db.execute("UPDATE x_accounts SET status = ? WHERE author_id = ?", (status, a["author_id"]))
            if status == "gone" and a["status"] != "gone":
                okeys = self.acct_occ.get(a["author_id"], [])
                occ = okeys[0] if len(okeys) == 1 else None
                payload: dict[str, Any] = {"handle": a["handle"], "author_id": a["author_id"],
                                           "last_post_at": a["last_post_at"], "since": since}
                if len(okeys) > 1:
                    payload["occupant_keys"] = okeys
                await emit_event(self.db, event_type="x_account_gone", occupant_key=occ,
                                 netuid=int(occ.split(":")[0]) if occ else None, event_at=self.now,
                                 observed_at=self.clock(), payload=payload, dedupe_key=a["author_id"], source="x",
                                 run_id=self.run_id)
            return
        await self._state_del(key)
        if user.handle and user.handle.lower() != a["handle_lower"]:
            clash = await self.db.fetchval("SELECT author_id FROM x_accounts WHERE handle_lower = ? AND author_id <> ?",
                                           (user.handle.lower(), a["author_id"]))
            if clash:
                logger.warning("x_handle_clash", data={"author_id": a["author_id"], "new_handle": user.handle})
            else:
                await self.db.execute("UPDATE x_accounts SET handle = ?, handle_lower = ? WHERE author_id = ?",
                                      (user.handle, user.handle.lower(), a["author_id"]))
                okeys = self.acct_occ.get(a["author_id"], [])
                occ = okeys[0] if len(okeys) == 1 else None
                await emit_event(self.db, event_type="x_handle_changed", occupant_key=occ,
                                 netuid=int(occ.split(":")[0]) if occ else None, event_at=self.now,
                                 observed_at=self.clock(), payload={"author_id": a["author_id"],
                                                                    "old_handle": a["handle"],
                                                                    "new_handle": user.handle},
                                 dedupe_key=f"{a['author_id']}|{user.handle}", source="x", run_id=self.run_id)
        await self.db.execute("UPDATE x_accounts SET status = 'active' WHERE author_id = ?", (a["author_id"],))
        await self._snapshot(user)
        self.resolved += 1

    # ── outcome / status ──────────────────────────────────────────────────────────────────────────────────
    async def counts(self) -> dict[str, int]:
        rows = await self.db.fetchall("SELECT kind, status, COUNT(*) AS n FROM x_accounts GROUP BY kind, status")
        c = {"team": 0, "kol": 0, "unresolved": 0, "gone": 0}
        for r in rows:
            c[r["kind"]] = c.get(r["kind"], 0) + int(r["n"])
            if r["status"] in ("unresolved", "gone"):
                c[r["status"]] += int(r["n"])
        c["unresolved_handles"] = int(await self.db.fetchval(
            "SELECT COUNT(*) FROM job_state WHERE job IN ('x_team_poll','x_kol_poll') AND key LIKE 'unresolved:%'")
            or 0)
        c["posts_24h"] = int(await self.db.fetchval(
            "SELECT COUNT(*) FROM social_posts WHERE platform = 'x' AND observed_at > ?", (self.now - DAY,)) or 0)
        return c

    async def outcome(self, label: str) -> JobOutcome:
        c = await self.counts()
        prefix = (f"paused:budget reason={self.paused}" if self.paused else
                  f"partial:{self.partial}" if self.partial else None)
        body = (f"{label} provider={self.provider.name} calls={self.calls} usd={self.cost_usd:.5f} "
                f"new_posts={self.rows} "
                f"seen={self.posts_seen} resolved={self.resolved} failed={self.failed}/{self.attempted} "
                f"team={c['team']} kol={c['kol']} unresolved={c['unresolved'] + c['unresolved_handles']} "
                f"gone={c['gone']} posts_24h={c['posts_24h']}" + (" degraded=top20" if self.degraded else ""))
        meta = {"calls": self.calls, "cost_usd": round(self.cost_usd, 8), "new_posts": self.rows,
                "posts_seen": self.posts_seen, "failed": self.failed, "attempted": self.attempted,
                "paused": self.paused, "partial": self.partial, **c}
        return JobOutcome(rows=self.rows, cost_usd=self.cost_usd, note=f"{prefix} {body}" if prefix else body,
                          meta=meta)

    # ── dry-run plan (§8.3 step 8) ────────────────────────────────────────────────────────────────────────
    async def plan(self, what: str) -> dict[str, Any]:
        """Estimate of one run without any call: resolutions + one page per account/query/occupant."""
        await self.load()
        est = self.provider.cost_estimate
        resolve = est(XCall("resolve_user", expected_posts=0))
        n_res = n_pages = 0
        usd = 0.0
        if what in ("team", "kol"):
            targets = await self._resolve_targets(what, link=False)
            n_res = len(targets)
            usd += n_res * resolve
            if what == "team":
                accts = await self._team_accounts()
                mode = str(self.s.X_TEAM_POLL_MODE)
            else:
                accts = await self._kol_accounts()
                mode = str(self.s.X_KOL_POLL_MODE)
            if mode == "timeline":
                for a in accts:
                    usd += est(XCall("user_timeline", expected_posts=self._timeline_expected(a["since_id"])))
                for _ in targets:
                    usd += est(XCall("user_timeline", expected_posts=self._timeline_expected(None)))
                n_pages = len(accts) + len(targets)
            else:
                handles = [a["handle"] for a in accts] + [h for h, _, _ in targets]
                n_pages = len(chunk_handles(handles, 25, 512))
                size = int(getattr(self.provider, "search_page_size", 20) or 20)
                usd += n_pages * est(XCall("search_recent", expected_posts=size))
        elif what == "mentions":
            n = int(self.s.X_MENTIONS_TOP_N)
            if await self.degraded_this_month():
                n = min(n, DEGRADE_TOP_N)
            n_pages = len(await self.top_n_occupants(n))
            size = int(getattr(self.provider, "search_page_size", 20) or 20)
            usd += n_pages * est(XCall("search_recent", expected_posts=size))
        elif what == "profiles":
            age = max(0, int(self.s.X_PROFILE_REFRESH_DAYS) - 1) * DAY
            n_res = int(await self.db.fetchval(
                "SELECT COUNT(*) FROM x_accounts a WHERE COALESCE((SELECT MAX(observed_at) FROM x_account_snapshots s "
                "WHERE s.author_id = a.author_id), 0) <= ?", (self.now - age,)) or 0)
            usd += n_res * resolve
        return {"job": JOB_IDS[what], "provider": self.provider.name, "resolutions": n_res, "pages": n_pages,
                "requests": n_res + n_pages, "estimate_usd": round(usd, 6)}


UPSERT_SQL = (
    "INSERT INTO social_posts (platform, post_id, author_id, author_handle, occupant_key, is_team_account, is_kol, "
    "kol_handle, text, text_hash, lang, url, like_count, repost_count, reply_count, quote_count, view_count, "
    "engagement_observed_at, has_url, has_media, in_reply_to, is_repost, quoted_post_id, author_credibility, "
    "credibility_flags, bot_filtered, event_at, observed_at, source, run_id, raw_json_hash) "
    "VALUES ('x',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
    "ON CONFLICT(platform, post_id) DO UPDATE SET like_count = excluded.like_count, "
    "repost_count = excluded.repost_count, reply_count = excluded.reply_count, quote_count = excluded.quote_count, "
    "view_count = excluded.view_count, engagement_observed_at = excluded.observed_at, "
    "source = CASE WHEN social_posts.source = 'subnetalpha_ctf' THEN excluded.source ELSE social_posts.source END, "
    "author_id = CASE WHEN social_posts.author_id LIKE 'h:%' THEN excluded.author_id ELSE social_posts.author_id END")


# ── CollectorJob subclasses (§2.5 job table; names fixed by JOB_TABLE) ────────────────────────────────────────
class _XPaidJob(CollectorJob):
    _method = ""
    _flags: tuple[str, ...] = ("X_ENABLED",)
    _last: dict | None = None

    def enabled(self) -> bool:
        """Every listed flag and ``settings.x_active()`` (§2.5)."""
        s = app_config.settings
        return all(bool(getattr(s, f, False)) for f in self._flags) and s.x_active() and super().enabled()

    async def collect(self, run: RunContext) -> JobOutcome:
        provider = make_provider(run.settings, run.rt.http)
        col = XCollector(run.rt.db, provider, run.rt.costs, run.rt.http, job_id=self.id, run_id=run.run_id,
                         now=run.started_at, settings=run.settings)
        out = await getattr(col, self._method)(run)
        self._last = out.meta
        return out

    def health_extra(self) -> dict | None:
        return dict(self._last) if self._last else None


class XTeamPollJob(_XPaidJob):
    _method = "run_team_poll"
    _flags = ("X_ENABLED", "X_TEAM_ENABLED")


class XKolPollJob(_XPaidJob):
    _method = "run_kol_poll"
    _flags = ("X_ENABLED", "X_KOL_ENABLED")


class XMentionsPollJob(_XPaidJob):
    _method = "run_mentions_poll"
    _flags = ("X_ENABLED", "X_MENTIONS_ENABLED")


class XProfileRefreshJob(_XPaidJob):
    _method = "run_profile_refresh"
    _flags = ("X_ENABLED",)


# ── CLI ───────────────────────────────────────────────────────────────────────────────────────────────────────
def _print(obj: Any, *, err: bool = False) -> None:
    print(json.dumps(obj, default=str, ensure_ascii=False), file=sys.stderr if err else sys.stdout, flush=True)


async def _open():
    from app.http import Http
    from app.ratelimit import ProviderLimiter
    from app.storage.db import Database
    s = app_config.settings
    db = Database(s.path(s.DB_PATH))
    await db.connect()
    await db.apply_schema()
    return db, Http(ProviderLimiter.from_settings())


async def status_report(db: "Database", now: int | None = None) -> dict[str, Any]:
    s = app_config.settings
    t = int(now if now is not None else now_ts())
    col = XCollector(db, make_provider(s, None), None, job_id="cli", run_id="cli", now=t)
    c = await col.counts()
    kol = await db.fetchone("SELECT COUNT(*) AS n, COALESCE(SUM(verified), 0) AS v FROM kol_accounts")
    spend = await db.fetchall("SELECT provider, usd, requests FROM provider_spend WHERE provider IN "
                              "('twitterapi_io','official_v2') AND month = ?", (month_key(t),))
    runs = await db.fetchall("SELECT job, MAX(finished_at) AS last_finished, SUM(ok) AS ok_runs, COUNT(*) AS runs "
                             "FROM collector_runs WHERE job LIKE 'x\\_%' ESCAPE '\\' GROUP BY job")
    return {"provider": s.X_PROVIDER, "x_active": s.x_active(), "x_enabled": s.X_ENABLED, "accounts": c,
            "kol_rows": int(kol["n"]) if kol else 0, "kol_verified": int(kol["v"]) if kol else 0,
            "spend_month": [dict(r) for r in spend],
            "runs": [{**dict(r), "last_finished": to_iso(r["last_finished"])} for r in runs]}


async def kol_candidates(db: "Database", now: int | None = None) -> list[dict]:
    """§2.7: authors with ``author_credibility ≥ 0.6`` seen ≥ 5 times in 30 d, not already team/KOL."""
    t = int(now if now is not None else now_ts())
    rows = await db.fetchall(
        "SELECT author_id, MAX(author_handle) AS handle, COUNT(*) AS n, ROUND(AVG(author_credibility), 3) AS cred "
        "FROM social_posts WHERE platform = 'x' AND is_team_account = 0 AND is_kol = 0 AND author_credibility >= ? "
        "AND event_at > ? GROUP BY author_id HAVING COUNT(*) >= ? ORDER BY n DESC, handle",
        (KOL_CANDIDATE_MIN_CRED, t - 30 * DAY, KOL_CANDIDATE_MIN_SEEN))
    return [dict(r) for r in rows]


async def _cli(args: argparse.Namespace) -> int:
    s = app_config.settings
    if args.once:
        job_id = JOB_IDS[args.once]
        if args.once == "ctf":
            try:
                import app.collectors.x_ctf  # noqa: F401  # lazily: owned by the x_ctf module
            except ImportError as exc:
                _print({"job": job_id, "result": "unavailable", "reason": str(exc)})
                return 1
            if args.dry_run:
                _print({"job": job_id, "dry_run": True, "note": "free job (subnetalpha pages); no USD estimate"})
                return 0
        else:
            from app.jobs import SPEC_BY_ID, _enabled
            spec = SPEC_BY_ID[job_id]
            if not (_enabled(spec) and s.x_active()):
                why = "X inactive" if not s.x_active() else "disabled"
                logger.info(f"{job_id} disabled ({why})")
                print(f"{job_id} disabled ({why})", flush=True)
                return 0
            if args.dry_run:
                db, http = await _open()
                try:
                    col = XCollector(db, make_provider(s, http), None, http, job_id=job_id, run_id="dry-run")
                    plan = await col.plan(args.once)
                    print(f"{job_id} estimate_usd≈{plan['estimate_usd']:.2f} requests≈{plan['requests']}", flush=True)
                    _print(plan)
                    return 0
                finally:
                    await http.aclose()
                    await db.close()
        from app.__main__ import _run_job
        return await _run_job(job_id)
    db, http = await _open()
    try:
        if args.status:
            _print(await status_report(db))
            return 0
        if args.kol_candidates:
            for r in await kol_candidates(db):
                _print(r)
            return 0
        return 2
    finally:
        await http.aclose()
        await db.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m app.collectors.x", description="spec 09 X collector")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", choices=sorted(JOB_IDS), help="run one job once through 01's wrapper")
    mode.add_argument("--status", action="store_true", help="account counts, KOL rows, month spend, last runs")
    mode.add_argument("--kol-candidates", action="store_true", help="credible non-KOL authors seen ≥ 5× in 30 d")
    ap.add_argument("--dry-run", action="store_true", help="print the estimate; make no call")
    args = ap.parse_args(argv)
    return asyncio.run(_cli(args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
