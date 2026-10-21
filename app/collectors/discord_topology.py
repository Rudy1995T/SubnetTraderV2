"""Pure topology logic (spec 08 §2.2, §2.4): the due rule, per-occupant classification, primary pick, source agreement.

``classify_occupant`` works on the *evidence* map — the latest successful observation of each code inside its
freshness window (own-server/vanity 2 d, main-channel and dead codes 8 d, 02 §C6). A failed fetch never erases valid
evidence inside the window, and a code without evidence is ``unknown`` (never ``invalid``).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from app.collectors.discord_api import InviteResult

if TYPE_CHECKING:  # pragma: no cover
    from app.collectors.discord import DiscordCfg

DAY = 86400
HOUR = 3600
TOPOLOGY_RANK = {"own_server": 5, "main_channel": 4, "main_vanity": 3, "invalid": 2, "handle_only": 1, "none": 0}
CLASSES = tuple(TOPOLOGY_RANK) + ("unknown",)
SHORT_WINDOW = 2 * DAY          # own-server and vanity evidence
LONG_WINDOW = 8 * DAY           # weekly re-checked main-server evidence and dead codes (C6)
_MAIN_CH_NAME = re.compile(r"^\D*?(\d{1,3})\s*[・·•|:\-]")


@dataclass(slots=True)
class Target:
    """One active ``watch_channels`` row (discord_invite | discord_channel | discord_handle)."""
    occupant_key: str
    netuid: int
    platform: str
    handle: str
    source: str
    priority: int = 50
    is_resolved: bool = False
    guild_id: str | None = None
    meta: dict = field(default_factory=dict)
    watch_id: int | None = None
    first_seen_at: int | None = None

    @property
    def declared_by(self) -> list[str]:
        d = self.meta.get("declared_by")
        return sorted(set(d)) if isinstance(d, list) and d else [self.source]


def legacy_only(source: str, meta: dict | None) -> bool:
    """A target declared only by ``legacy`` (the old bot's frozen fundamentals.json) is not a live claim: live data
    (2026-10-17) showed 5 of 6 such Discord invites opening a previous occupant's server after re-registration."""
    d = (meta or {}).get("declared_by")
    srcs = sorted(set(d)) if isinstance(d, list) and d else [source]
    return srcs == ["legacy"]


@dataclass(slots=True)
class TargetState:
    """Stored history of one invite code (latest attempt across all occupants)."""
    code: str
    invite_valid: int | None
    guild_id: str | None
    observed_at: int
    cycle_at: int
    invite_error_code: int | None = None
    dead_since: int | None = None        # first observed_at of the current dead streak
    last_valid_at: int | None = None


@dataclass(slots=True)
class TopologyDecision:
    occupant_key: str
    netuid: int
    topology_class: str
    primary_code: str | None = None
    primary_guild_id: str | None = None
    primary_result: InviteResult | None = None
    main_channel_id: str | None = None
    defining_code: str | None = None
    source_agreement: str | None = None
    declared_by: dict[str, list[str]] = field(default_factory=dict)    # code -> sources
    unknown_targets: list[str] = field(default_factory=list)
    reason: str | None = None


def is_due(state: TargetState | None, *, code: str, now: int, cycle_at: int, cfg: "DiscordCfg") -> bool:
    """§2.2 due rule (pure)."""
    if state is None or state.invite_valid is None:
        return True                                                    # never fetched / last attempt failed
    if state.cycle_at == cycle_at:
        return False                                                   # idempotent manual re-run
    if code == cfg.vanity_code:
        return True
    if state.invite_valid == 1 and state.guild_id != cfg.main_guild_id:
        return True
    if state.invite_valid == 1:
        return now - state.observed_at >= cfg.main_channel_recheck_days * DAY - 3 * HOUR
    dead_since = state.dead_since if state.dead_since is not None else state.observed_at
    if now - dead_since < cfg.dead_weekly_after_days * DAY:
        return (cycle_at % DAY) // HOUR == 0 and now - state.observed_at >= 20 * HOUR
    return now - state.observed_at >= 7 * DAY - 3 * HOUR


def due_order(code: str, state: TargetState | None, cfg: "DiscordCfg") -> int:
    """vanity → own → main-channel → new/failed → dead (§8.3 step 4)."""
    if code == cfg.vanity_code:
        return 0
    if state is None or state.invite_valid is None:
        return 3
    if state.invite_valid == 1:
        return 1 if state.guild_id != cfg.main_guild_id else 2
    return 4


def evidence_window(res: InviteResult, cfg: "DiscordCfg") -> int:
    if res.invite_valid == 1 and (res.guild_id != cfg.main_guild_id or res.code == cfg.vanity_code):
        return SHORT_WINDOW
    return LONG_WINDOW


def main_channel_match(res: InviteResult, netuid: int, channel_ids: set[str]) -> bool:
    if res.landing_channel_id and res.landing_channel_id in channel_ids:
        return True
    m = _MAIN_CH_NAME.match(res.landing_channel_name or "")
    return bool(m and int(m.group(1)) == int(netuid))


def pick_primary(own: list[tuple[Target, InviteResult]], prev_primary_guild: str | None
                 ) -> tuple[Target, InviteResult]:
    """Sticky previous guild, then resolved, lower priority, more members, smallest code."""
    pool = [x for x in own if prev_primary_guild and x[1].guild_id == prev_primary_guild] or own
    return min(pool, key=lambda x: (not x[0].is_resolved, x[0].priority, -(x[1].member_count or 0), x[0].handle))


def source_agreement(targets: list[Target], evidence: dict[str, InviteResult], vanity_code: str) -> str | None:
    """agree | disagree | one_dead | single_source | None (no non-vanity evidence) — §2.4.

    ``legacy`` (the frozen old fundamentals.json) is not a live claim and never counts (06's lesson, BUILD-LOG Phase 4).
    """
    valid: dict[str, set[str]] = {}
    dead: dict[str, set[str]] = {}
    for t in targets:
        if t.platform != "discord_invite" or t.handle == vanity_code:
            continue
        r = evidence.get(t.handle)
        if r is None:
            continue
        for s in t.declared_by:
            if s == "legacy":
                continue
            if r.invite_valid == 1 and r.guild_id:
                valid.setdefault(s, set()).add(r.guild_id)
            elif r.invite_valid == 0:
                dead.setdefault(s, set()).add(t.handle)
    groups = [g for g in valid.values() if g]
    if len(groups) >= 2:
        for i in range(len(groups)):
            for j in range(i + 1, len(groups)):
                if not groups[i] & groups[j]:
                    return "disagree"
        return "agree"
    dead_only = [s for s in dead if s not in valid]
    if dead_only and (groups or len(dead_only) >= 2):
        return "one_dead"
    if groups or dead_only:
        return "single_source"
    return None


def classify_occupant(netuid: int, occupant_key: str, targets: list[Target], evidence: dict[str, InviteResult], *,
                      main_alive: bool, identity_status: str | None, prev_primary_guild: str | None,
                      cfg: "DiscordCfg") -> TopologyDecision:
    """§2.4: own_server > main_channel > main_vanity > unknown > invalid > handle_only > none."""
    main = cfg.main_guild_id
    invites = [t for t in targets if t.platform == "discord_invite"]
    channels = [t for t in targets if t.platform == "discord_channel"]
    handles = [t for t in targets if t.platform == "discord_handle"]
    main_chans = [t for t in channels if (t.guild_id or main) == main]
    chan_ids = {t.handle for t in channels}
    own: list[tuple[Target, InviteResult]] = []
    main_ch: list[tuple[Target | None, str | None, InviteResult | None]] = []
    main_van: list[tuple[Target, InviteResult]] = []
    dead: list[tuple[Target, InviteResult]] = []
    unknown: list[str] = []
    for t in invites:
        r = evidence.get(t.handle)
        if r is None or r.invite_valid is None:
            unknown.append(t.handle)
        elif r.invite_valid == 1 and r.guild_id != main:
            own.append((t, r))
        elif r.invite_valid == 1:
            if t.handle != cfg.vanity_code and main_channel_match(r, netuid, chan_ids):
                main_ch.append((t, r.landing_channel_id, r))
            else:
                main_van.append((t, r))
        else:
            dead.append((t, r))
    if main_chans:
        if main_alive:
            main_ch.extend((t, t.handle, None) for t in main_chans)
        else:
            unknown.append("main_guild_channels")
    dec = TopologyDecision(occupant_key=occupant_key, netuid=netuid, topology_class="none",
                           declared_by={t.handle: t.declared_by for t in invites},
                           unknown_targets=unknown,
                           source_agreement=source_agreement(targets, evidence, cfg.vanity_code))
    if own:
        t, r = pick_primary(own, prev_primary_guild)
        dec.topology_class = "own_server"
        dec.primary_code, dec.primary_guild_id, dec.primary_result = t.handle, r.guild_id, r
        dec.defining_code = t.handle
    elif main_ch:
        t, ch, r = main_ch[0]
        dec.topology_class = "main_channel"
        dec.primary_guild_id, dec.main_channel_id = main, ch
        if r is not None:
            dec.defining_code, dec.primary_result = t.handle, r
    elif main_van:
        t, r = main_van[0]
        dec.topology_class = "main_vanity"
        dec.primary_guild_id, dec.defining_code, dec.primary_result = main, t.handle, r
    elif unknown or identity_status == "unknown":
        dec.topology_class = "unknown"
        dec.reason = "evidence_failed" if unknown else "identity_unknown"
    elif dead:
        t, r = dead[0]
        dec.topology_class = "invalid"
        dec.defining_code, dec.primary_result = t.handle, r
    elif handles:
        dec.topology_class = "handle_only"            # handle values are personal usernames: never stored here
    else:
        dec.topology_class = "none"
        dec.reason = "no_pollable_target" if identity_status == "present" else "no_discord"
    return dec
