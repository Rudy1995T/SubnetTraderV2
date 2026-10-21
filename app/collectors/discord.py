"""Discord keyless collector (spec 08): ``discord_6h`` (invite metadata + topology census) and ``discord_widget_1h``.

Fail-safe posture (port of the whale collector, 08 §8.5): a run never half-writes a slot silently — per-target
failures are rows (``invite_valid NULL``) so the planner and ``/health`` see them; a run whose every attempted target
failed raises (wrapper ``ok=0``, ``collector_failing`` at the threshold); a Cloudflare block aborts after writing the
rows gathered so far. Counts only: usernames, the invite ``inviter``, widget ``members[]`` and ``discord_handle``
values never reach storage or logs.

CLI (``python -m app.collectors.discord``): ``--once [--dry-run] [--code C]``, ``--widgets``,
``--report topology [--netuid N]``, ``--export-csv``, ``--metrics [--as-of YYYY-MM-DD|now] [--netuid N] [--write]``.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import uuid
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.collectors import community_events as ev
from app.collectors import community_store as store
from app.collectors.discord_api import CloudflareBan, fetch_invite, fetch_widget, get_guard
from app.collectors.discord_channels import ChannelMap, load_channel_map, remap_unmapped, sync_channel_map
from app.collectors.discord_topology import (Target, TopologyDecision, classify_occupant, due_order, is_due,
                                             legacy_only)
from app.http import ProviderError
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.util.time import floor_hour, now_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.http import Http
    from app.storage.db import Database

DAY = 86400
HOUR = 3600
INVITE_PLATFORMS = ("discord_invite", "discord_channel", "discord_handle")


@dataclass(slots=True, frozen=True)
class DiscordCfg:
    """Snapshot of the §5 keys taken at run start."""
    cron_hours: tuple[int, ...]
    main_guild_id: str
    vanity_code: str
    main_announce_id: str
    main_channel_recheck_days: int
    dead_weekly_after_days: int
    expiry_warn_days: int
    widget_disabled_recheck_days: int
    widget_skip_guild_ids: frozenset[str]
    channel_map_reviewed: bool
    presence_min_samples: int
    use_bot_token: bool
    events_enabled: bool
    venue_change_debounce: int
    channels_path: str

    @classmethod
    def from_settings(cls, s=None) -> "DiscordCfg":
        s = s or app_config.settings
        hours = tuple(sorted(int(x) for x in str(s.DISCORD_CRON_HOURS).split(",") if x.strip()))
        skip = frozenset(x.strip() for x in str(s.DISCORD_WIDGET_SKIP_GUILD_IDS).split(",") if x.strip())
        return cls(hours, str(s.DISCORD_MAIN_GUILD_ID), str(s.DISCORD_MAIN_VANITY_CODE),
                   str(s.DISCORD_MAIN_ANNOUNCE_CHANNEL_ID), int(s.DISCORD_MAIN_CHANNEL_RECHECK_DAYS),
                   int(s.DISCORD_DEAD_WEEKLY_AFTER_DAYS), int(s.DISCORD_INVITE_EXPIRY_WARN_DAYS),
                   int(s.DISCORD_WIDGET_DISABLED_RECHECK_DAYS), skip | {str(s.DISCORD_MAIN_GUILD_ID)},
                   bool(s.DISCORD_CHANNEL_MAP_REVIEWED), int(s.DISCORD_PRESENCE_MIN_SAMPLES),
                   bool(s.DISCORD_INVITES_USE_BOT_TOKEN), bool(s.COMMUNITY_EVENTS_ENABLED),
                   int(s.COMMUNITY_VENUE_CHANGE_DEBOUNCE), str(s.path(s.DISCORD_CHANNELS_SEED_PATH)))


def cycle_slot(now: int, hours: tuple[int, ...] | list[int]) -> int:
    """Latest cron slot ≤ now (UTC, minute 0)."""
    day = now - now % DAY
    for h in sorted(hours, reverse=True):
        if day + h * HOUR <= now:
            return day + h * HOUR
    return day - DAY + max(hours) * HOUR


async def _targets(db: "Database", occ_keys: set[str]) -> dict[str, list[Target]]:
    from app.registry.watch import watch_targets
    out: dict[str, list[Target]] = {}
    for platform in INVITE_PLATFORMS:
        for w in await watch_targets(db, platform):
            if w.occupant_key not in occ_keys or legacy_only(w.source, w.meta):
                continue
            out.setdefault(w.occupant_key, []).append(Target(
                occupant_key=w.occupant_key, netuid=int(w.netuid), platform=w.platform, handle=w.handle,
                source=w.source, priority=int(w.priority), is_resolved=bool(w.is_resolved), guild_id=w.guild_id,
                meta=w.meta or {}, watch_id=w.id))
    return out


async def _occupants(db: "Database", netuids: set[int] | None = None) -> list:
    from app.registry import active_occupants
    return [o for o in await active_occupants(db, include_pre_launch=True)
            if o.netuid != 0 and (netuids is None or o.netuid in netuids)]


async def sync_csv(db: "Database", cfg: DiscordCfg, now: int, run_id: str) -> dict:
    """Parse + sync the operator CSV every run; remap unmapped crossposts when the file changed (§2.8)."""
    path = Path(cfg.channels_path)
    rows, errors = load_channel_map(path, cfg.main_guild_id)
    if errors and errors != ["missing"]:
        logger.warning("discord_csv_errors", data={"errors": errors[:20], "count": len(errors)})
    out: dict[str, Any] = {"csv_rows": len(rows), "csv_errors": [e for e in errors if e != "missing"],
                           "csv_missing": errors == ["missing"], "csv_stale_rows": [], "remapped": 0}
    if not rows:
        return out
    res = await sync_channel_map(db, rows, now, run_id, cfg.main_guild_id)
    out["csv_stale_rows"] = res["stale_rows"]
    out["csv_synced"] = res["upserted"]
    try:
        mtime = int(path.stat().st_mtime)
    except OSError:
        mtime = None
    if mtime is not None and mtime != await store.get_task_state(db, "discord_6h", "csv_mtime"):
        cmap = await ChannelMap.build(db, rows, cfg.main_announce_id)
        out["remapped"] = await remap_unmapped(db, cmap, now, run_id)
        await store.set_task_state(db, "discord_6h", "csv_mtime", mtime)
    return out


async def run_invite_cycle(db: "Database", http: "Http", *, now: int, run_id: str, s=None, dry_run: bool = False,
                           only_code: str | None = None, netuids: set[int] | None = None,
                           on_plan=None) -> dict[str, Any]:
    """One ``discord_6h`` cycle (§8.3). Returns a report; raises ``CloudflareBan`` / ``ProviderError``."""
    from app.features.community import write_evidence
    s = s or app_config.settings
    cfg = DiscordCfg.from_settings(s)
    cycle_at = cycle_slot(now, cfg.cron_hours)
    rep: dict[str, Any] = {"cycle_at": cycle_at}
    if not dry_run:
        rep.update(await sync_csv(db, cfg, now, run_id))
    occs = await _occupants(db, netuids)
    occ_by_key = {o.occupant_key: o for o in occs}
    targets = await _targets(db, set(occ_by_key))
    by_code: dict[str, list[Target]] = {}
    for ts in targets.values():
        for t in ts:
            if t.platform == "discord_invite":
                by_code.setdefault(t.handle, []).append(t)
    codes = set(by_code) | {cfg.vanity_code}
    states = await store.latest_invite_states(db, codes)
    if only_code:
        due = [only_code]
    else:
        due = sorted((c for c in codes if is_due(states.get(c), code=c, now=now, cycle_at=cycle_at, cfg=cfg)),
                     key=lambda c: (due_order(c, states.get(c), cfg), c))
    rep.update(targets=len(codes), due=len(due), occupants=len(occs))
    if dry_run:
        plan = [{"code": c, "tier": due_order(c, states.get(c), cfg),
                 "last": to_iso(states[c].observed_at) if c in states else None,
                 "last_valid": states[c].invite_valid if c in states else None,
                 "declared_by_occupants": len(by_code.get(c, []))} for c in due]
        rep["plan"] = plan
        if on_plan:
            for p in plan:
                on_plan(p)
        return rep

    results: dict[str, Any] = {}
    guard = get_guard(int(s.DISCORD_INVALID_GUARD_PER_10MIN))
    banned: CloudflareBan | None = None
    for code in due:
        try:
            results[code] = await fetch_invite(http, code, s, guard=guard)
        except CloudflareBan as exc:
            banned = exc
            break
    rep["invalid_10min"] = guard.count()

    evidence = await store.latest_ok_evidence(db, codes, now, cfg)
    for code, res in results.items():
        if res.ok:
            evidence[code] = res
    van = evidence.get(cfg.vanity_code)
    main_alive = bool(van and van.invite_valid == 1 and van.guild_id == cfg.main_guild_id)
    topo_state = await store.get_task_states(db, "discord_6h", "topo:")
    ident = await store.identity_statuses(db, "discord")
    decisions: dict[str, TopologyDecision] = {}
    prev_primary: dict[str, str | None] = {}
    for o in occs:
        prev = topo_state.get(f"topo:{o.occupant_key}") or {}
        prev_guild = prev.get("guild_id") if prev.get("class") == "own_server" else None
        prev_primary[o.occupant_key] = prev.get("primary_code")
        decisions[o.occupant_key] = classify_occupant(
            o.netuid, o.occupant_key, targets.get(o.occupant_key, []), evidence, main_alive=main_alive,
            identity_status=ident.get(o.occupant_key), prev_primary_guild=prev_guild, cfg=cfg)

    rows: list[dict] = []
    fanned: list[tuple[str, Any]] = []
    watch_upd: list[tuple[str, Any, str]] = []
    for code, res in results.items():
        if code == cfg.vanity_code:
            rows.append(store.invite_row(res, occupant_key=None, netuid=None, cycle_at=cycle_at, run_id=run_id))
            watch_upd += [(t.occupant_key, res, decisions[t.occupant_key].topology_class) for t in by_code.get(code, [])]
            continue
        for t in by_code.get(code, []):
            dec = decisions[t.occupant_key]
            prim = dec.topology_class == "own_server" and dec.primary_code == code and res.invite_valid == 1
            rows.append(store.invite_row(res, occupant_key=t.occupant_key, netuid=t.netuid, cycle_at=cycle_at,
                                         run_id=run_id, decision=dec, is_primary=prim, declared_by=t.declared_by))
            fanned.append((t.occupant_key, res))
            watch_upd.append((t.occupant_key, res, dec.topology_class))
    have = await store.slot_primaries(db, cycle_at) | {r["occupant_key"] for r in rows if r["is_primary"]}
    for okey, dec in decisions.items():
        if okey not in have:
            rows.append(store.census_row(dec, observed_at=now, cycle_at=cycle_at, run_id=run_id))
    written = await store.insert_discord_rows(db, rows)
    await store.update_watch_invites(db, watch_upd, now, cfg.main_guild_id)
    rep["evidence_rows"] = await write_evidence(db, list(decisions), cycle_at, run_id, now=now_ts())

    n_events = 0
    if cfg.events_enabled:
        n_events += await ev.emit_invite_events(db, decisions, fanned, states, prev_primary, now, run_id, cfg)
        n_events += await ev.detect_venue_changes(db, list(decisions.values()), now, run_id, cfg, cycle_at=cycle_at,
                                                  state=topo_state)
        if cycle_at % DAY == 0:
            keys = [k for k, d in decisions.items() if d.topology_class == "own_server"]
            n_events += await ev.detect_inflation(db, keys, cycle_at, run_id, now)
            n_events += await ev.detect_silence(db, list(decisions), cycle_at, run_id, now)
    else:
        for dec in decisions.values():          # keep stickiness state even with events off
            if dec.topology_class != "unknown":
                await store.set_task_state(db, "discord_6h", f"topo:{dec.occupant_key}",
                                           {"class": dec.topology_class, "guild_id": dec.primary_guild_id,
                                            "primary_code": dec.primary_code, "last_cycle": cycle_at})
    attempted = len(results)
    ok = sum(1 for r in results.values() if r.ok)
    dead = sum(1 for r in results.values() if r.invite_valid == 0)
    classes = Counter(d.topology_class for d in decisions.values())
    rep.update(rows=written, fetched=attempted, ok=ok, dead=dead, failed=attempted - ok, events=n_events,
               classes=dict(sorted(classes.items())), main_alive=main_alive,
               failures=sorted({r.error for r in results.values() if not r.ok and r.error})[:10])
    logger.info("discord_invites_run", data={k: v for k, v in rep.items() if k not in ("csv_stale_rows",)})
    if banned is not None:
        raise banned
    if attempted > 0 and ok == 0:
        raise ProviderError("discord", f"all {attempted} targets failed", status=None, url=s.DISCORD_API_BASE,
                            attempts=attempted)
    return rep


class DiscordInvitesJob(CollectorJob):
    _last: dict | None = None

    async def collect(self, run: RunContext) -> JobOutcome:
        rep = await run_invite_cycle(run.rt.db, run.rt.http, now=run.started_at, run_id=run.run_id, s=run.settings)
        self._last = rep
        return JobOutcome(rows=rep.get("rows", 0), meta={k: v for k, v in rep.items() if k != "plan"},
                          note=f"cycle={to_iso(rep['cycle_at'])} due={rep.get('due')} ok={rep.get('ok')} "
                               f"dead={rep.get('dead')} failed={rep.get('failed')}")

    def health_extra(self) -> dict | None:
        last = self._last or {}
        keys = ("cycle_at", "targets", "due", "ok", "dead", "failed", "invalid_10min", "classes", "csv_rows",
                "csv_errors", "csv_stale_rows", "main_alive")
        return {k: last.get(k) for k in keys} if last else None


# ── widget ───────────────────────────────────────────────────────────────────────────────────────────────────
async def run_widget_pass(db: "Database", http: "Http", *, now: int, run_id: str, s=None) -> dict[str, Any]:
    s = s or app_config.settings
    cfg = DiscordCfg.from_settings(s)
    hour = floor_hour(now)
    gmap = {g: ks for g, ks in (await store.primary_guild_map(db, now)).items() if g not in cfg.widget_skip_guild_ids}
    latest = {r["guild_id"]: r for r in await db.fetchall(
        "SELECT guild_id, widget_enabled, observed_at, cycle_at, meta_json FROM (SELECT guild_id, widget_enabled, "
        "observed_at, cycle_at, meta_json, ROW_NUMBER() OVER (PARTITION BY guild_id ORDER BY observed_at DESC, id DESC) "
        "AS rn FROM discord_snapshots WHERE probe = 'widget') WHERE rn = 1", writer=True)}
    due = []
    for g in sorted(gmap):
        r = latest.get(g)
        if r is None:
            due.append(g)
        elif int(r["cycle_at"]) == hour:
            continue
        elif r["widget_enabled"] == 1:
            due.append(g)
        elif r["widget_enabled"] == 0:
            if now - int(r["observed_at"]) >= cfg.widget_disabled_recheck_days * DAY - HOUR:
                due.append(g)
        else:
            err = (json.loads(r["meta_json"]) if r["meta_json"] else {}).get("error")
            if err != "unknown_guild" or now - int(r["observed_at"]) >= DAY - HOUR:
                due.append(g)
    netuid_of = {r["occupant_key"]: r["netuid"] for r in await db.fetchall("SELECT occupant_key, netuid FROM occupants")}
    guard = get_guard(int(s.DISCORD_INVALID_GUARD_PER_10MIN))
    rows: list[dict] = []
    ok = failed = 0
    banned: CloudflareBan | None = None
    for g in due:
        try:
            w = await fetch_widget(http, g, s, guard=guard)
        except CloudflareBan as exc:
            banned = exc
            break
        if w.widget_enabled is None and w.error != "unknown_guild":
            failed += 1
        else:
            ok += 1
        meta = {k: v for k, v in (("instant_invite_code", w.instant_invite_code), ("channel_count", w.channel_count),
                                  ("error", w.error)) if v is not None}
        for okey in gmap[g]:
            rows.append({"occupant_key": okey, "netuid": netuid_of.get(okey), "probe": "widget", "guild_id": g,
                         "invite_code": None, "observed_at": int(w.fetched_at), "event_at": None, "cycle_at": hour,
                         "source": "discord_api", "http_status": w.http_status, "member_count": None,
                         "presence_count": w.presence_count, "presence_ratio": None, "invite_valid": None,
                         "widget_enabled": w.widget_enabled, "is_primary": 0, "unknown_targets": 0,
                         "meta_json": json.dumps(meta) if meta else None, "run_id": run_id})
    written = await store.insert_discord_rows(db, rows)
    rep = {"guilds": len(gmap), "due": len(due), "ok": ok, "failed": failed, "rows": written,
           "invalid_10min": guard.count()}
    logger.info("discord_widget_run", data=rep)
    if banned is not None:
        raise banned
    if due and ok == 0 and failed > 0:
        raise ProviderError("discord", f"widget: all {failed} guilds failed", status=None, url=s.DISCORD_API_BASE,
                            attempts=failed)
    return rep


class DiscordWidgetJob(CollectorJob):
    _last: dict | None = None

    async def collect(self, run: RunContext) -> JobOutcome:
        rep = await run_widget_pass(run.rt.db, run.rt.http, now=run.started_at, run_id=run.run_id, s=run.settings)
        self._last = rep
        return JobOutcome(rows=rep["rows"], meta=rep, note=f"guilds={rep['guilds']} due={rep['due']} ok={rep['ok']}")

    def health_extra(self) -> dict | None:
        return dict(self._last) if self._last else None


# ── CLI ──────────────────────────────────────────────────────────────────────────────────────────────────────
async def _open():
    from app.http import Http
    from app.ratelimit import ProviderLimiter
    from app.storage.db import Database
    s = app_config.settings
    db = Database(s.path(s.DB_PATH))
    await db.connect()
    await db.apply_schema()
    return db, Http(ProviderLimiter.from_settings())


def _print(obj: Any, *, err: bool = False) -> None:
    print(json.dumps(obj, default=str, ensure_ascii=False), file=sys.stderr if err else sys.stdout, flush=True)


async def report_topology(db: "Database", netuids: set[int] | None = None) -> list[dict]:
    now = now_ts()
    occs = await _occupants(db, netuids)
    targets = await _targets(db, {o.occupant_key for o in occs})
    out = []
    for o in occs:
        row = await db.fetchone(
            "SELECT * FROM discord_snapshots WHERE occupant_key = ? AND is_primary = 1 AND observed_at > ? "
            "ORDER BY observed_at DESC, id DESC LIMIT 1", (o.occupant_key, now - 2 * DAY))
        ts = targets.get(o.occupant_key, [])
        codes = []
        for t in ts:
            if t.platform != "discord_invite":
                continue
            last = await db.fetchone("SELECT invite_valid, invite_error_code, member_count FROM discord_snapshots "
                                     "WHERE probe = 'invite' AND invite_code = ? ORDER BY observed_at DESC LIMIT 1",
                                     (t.handle,))
            state = "?" if last is None else ("valid" if last["invite_valid"] == 1 else
                                              f"dead:{last['invite_error_code']}" if last["invite_valid"] == 0
                                              else "failed")
            codes.append(f"{t.handle}({'/'.join(t.declared_by)}):{state}")
        chans = [t for t in ts if t.platform == "discord_channel"]
        members = await db.fetchone(
            "SELECT member_count, presence_count FROM discord_snapshots WHERE occupant_key = ? AND probe = 'invite' "
            "AND is_primary = 1 AND invite_valid = 1 ORDER BY observed_at DESC LIMIT 1", (o.occupant_key,))
        out.append({"netuid": o.netuid, "name": o.name, "class": row["topology_class"] if row else None,
                    "guild_id": row["guild_id"] if row else None,
                    "members": members["member_count"] if members else None,
                    "presence": members["presence_count"] if members else None,
                    "agreement": row["source_agreement"] if row else None,
                    "unknown_targets": row["unknown_targets"] if row else None, "codes": codes,
                    "channels": [f"{t.handle}({t.source}{'/' + str(t.meta.get('kind')) if t.meta.get('kind') else ''})"
                                 for t in chans],
                    "handles": sum(1 for t in ts if t.platform == "discord_handle")})
    return out


async def _cli(args) -> int:
    from app.features.community import community_metrics, to_raw_row
    db, http = await _open()
    s = app_config.settings
    try:
        if args.export_csv:
            from app.collectors.discord_channels import export_known_channels
            n = await export_known_channels(db, s.path(s.DISCORD_CHANNELS_SEED_PATH), s.DISCORD_MAIN_GUILD_ID)
            _print({"exported": n, "path": str(s.path(s.DISCORD_CHANNELS_SEED_PATH))})
            return 0
        if args.report:
            for r in await report_topology(db, set(args.netuid) if args.netuid else None):
                _print(r)
            return 0
        rid = f"cli-{uuid.uuid4().hex[:12]}"
        if args.widgets:
            _print(await run_widget_pass(db, http, now=now_ts(), run_id=rid))
            return 0
        if args.once:
            rep = await run_invite_cycle(db, http, now=now_ts(), run_id=rid, dry_run=args.dry_run,
                                         only_code=args.code, netuids=set(args.netuid) if args.netuid else None,
                                         on_plan=_print)
            rep.pop("plan", None)
            _print({"discord_invites_run": rep}, err=True)
            return 0
        if args.metrics:
            from datetime import datetime, timezone
            from app.storage.pit import put_metric_values
            if args.as_of and args.as_of != "now":
                t = int(datetime.strptime(args.as_of, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
            elif args.write:
                print("--write needs --as-of YYYY-MM-DD (an official slot)", file=sys.stderr)
                return 2
            else:
                t = now_ts()
            try:
                import app.features.derived  # noqa: F401  # type: ignore[import-not-found]
                combiners = "derived by spec 12"
            except ImportError:
                combiners = "n/a before Phase 8"
            rows = []
            for o in await _occupants(db, set(args.netuid) if args.netuid else None):
                res = await community_metrics(db, o.occupant_key, t, evidence=not args.write)
                _print({"netuid": o.netuid, "occupant_key": o.occupant_key, "as_of": to_iso(t),
                        **{k: {"value": r.value, **r.mapped_json()} for k, r in res},
                        "announcement_cadence_30d": combiners, "silence_days": combiners})
                if args.write:
                    rows += [to_raw_row(k, r, occupant_key=o.occupant_key, as_of=t, observed_at=now_ts(), run_id=rid)
                             for k, r in res]
            if args.write:
                _print({"written": await put_metric_values(db, rows)}, err=True)
            return 0
        return 2
    finally:
        await http.aclose()
        await db.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m app.collectors.discord")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="one invite cycle outside the scheduler")
    mode.add_argument("--widgets", action="store_true", help="one widget pass")
    mode.add_argument("--report", choices=["topology"])
    mode.add_argument("--export-csv", action="store_true", help="append known main-server channel ids to the CSV")
    mode.add_argument("--metrics", action="store_true", help="print the 08 metrics (--write stores the 8 registry keys)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--code")
    ap.add_argument("--netuid", type=int, action="append")
    ap.add_argument("--as-of")
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args(argv)
    return asyncio.run(_cli(args))


if __name__ == "__main__":  # pragma: no cover
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    sys.exit(main())
