# 08 — Community collector: Discord + Telegram

**Status:** built 2026-10-17 (Phase 6; pre-build review + live verification in the Changelog; reconciled with 02 on 2026-09-23) · **Phase:** 6 · **Depends on:** 02 (binding reconciliation ledger; it overrides every other document, including this one; ids such as `B10`, `C3`, `D2` are cited inline), 01 (Settings, `CollectorJob`/`LongTask`, `JOB_TABLE`, `Http` + `ProviderLimiter`, `AlertLatch`, `job_state`, `collector_runs`, `/health`), 03 (storage owner — binding conventions of 03 §2.1: INTEGER unix-second instants, `source` + `run_id` + `observed_at` + `event_at` on every raw row, STRICT tables, `_json` suffix, severity `info|warning|critical`, `app/storage/events.py::emit_event()`; §3.2 mirrors the DDL 03 §3.5 adopted from this spec; open items in §11 Q20), 04 (`occupants`, `subnets`, `identity_resolved`, `watch_channels`, `events.dedupe_key`, `DISCORD_MAIN_GUILD_ID`, `current_occupant`/`watch_targets`), 09 (X legs of P5, `social_post_mentions`; 09 and 10 import this spec's `app/nlp/textnorm.py`, B11), 10 (consumes `announcements`), 12 (`app/features/types.py::MetricResult`, `register_metric` (C5), `compute_registered` → 03 `put_metric_values`, registry rows 34–37/47/48/50/51 with dotted fn paths `app.features.community:compute_<key>`, the `presence_ratio` normaliser (this spec's shape, adopted by 12 §2.5), the derived combiners `announcement_cadence_30d`/`silence_days` (12 §2.6), red-flag gates), 16 (pause-target resolution `control_state` / `data/control/<name>.paused`, liveness max-age, Discord-ban runbook) · **Master spec refs:** §1.2, §1.3, §4 (tier 0 Discord invites/widget + `t.me`; tier 1 Discord Bot), §5, §6, §7.2, §7.3 P5, §7.5 (`silence_45d`, `inflated_community`, `stale_identity`), §7.7, §8 (Community row), §11, §12 (phase 6) · **Operator checklist:** 18 B4, B5, D5, E

---

## 1. Purpose and scope

Produce point-in-time evidence of every occupant's community venues (own Discord server, channel inside the main Bittensor server, Telegram group/channel), their size and liveliness, and the team's announcement cadence on Discord/Telegram; compute the Discord/Telegram P5 metrics and the two P5 combiners that also consume spec 09's X legs; emit community events. Everything is keyless except the optional aggregator-server Bot (free token).

**In scope**
1. `discord_6h` — keyless invite-metadata poller (`GET /api/v10/invites/{code}`), cron 00/06/12/18Z, tiered cadence per target, topology classification, per-cycle venue census.
2. `discord_widget_1h` — `widget.json` hourly for guilds with the widget enabled, weekly probe for disabled ones; counts only.
3. `discord_gateway` — `LongTask` running a discord.py 2.x Bot that is a member **only** of the operator's aggregator server and reads crossposts arriving there from Followed announcement channels → `announcements` (platform `discord`).
4. `telegram_daily` — `t.me/{handle}` / `t.me/+{hash}` profile pages (members/online/subscribers) and `t.me/s/{handle}` previews of public broadcast channels (post timestamps + `text_hash`) → `telegram_snapshots`, `announcements` (platform `telegram`).
5. Loader for the operator's `seed/discord_channels.csv` (main-server subnet channel ids, Followed announcement channels) → `watch_channels`.
6. Producer functions (spec 12 §2.3 contract) for the scored P5 metrics `venue_reachable`, `discord_members`, `presence_ratio`, `discord_growth_30d` and the four legs `discord_announcements_30d`, `tg_posts_30d`, `discord_last_activity_days`, `tg_last_activity_days` (02 §C3 names = 12's registry rows 47–51; the retired `discord_last_announcement_days`/`tg_last_post_days` are never written), plus the evidence keys `discord_topology`, `inflated_days`, `tg_members` (C3). The combiners `announcement_cadence_30d` and `silence_days` are **derived by spec 12** (§2.6) from these legs plus spec 09's X legs, and this spec registers no producer for them (C3); this spec fixes the leg semantics (cross-platform dedupe, lower bounds) that make 12's sum equal the union the master asks for (§2.9). The `presence_ratio` normaliser is implemented once, as 12's `app/features/normalise.py::presence_ratio` (the shape this spec specifies in §2.9).
7. Events `invite_expired`, `invite_dead`, `inflated_community`, `community_venue_changed`, `silence_45d` (03 §3.9 names and required payload keys, D1/D2).
8. The manual operator procedure (checklist B4/B5) and the binding ToS posture.
9. `app/nlp/textnorm.py`, the one text normaliser and hash for X, Discord and Telegram (B11; defined here, imported by 09 and 10).

**Out of scope**
- Reading any channel of a server the operator does not own: no self-bots, no user-token automation, no Bot in third-party servers (master §1.2, checklist E). Per-subnet channels inside the main server are type 0 (GUILD_TEXT), cannot be Followed, and stay manual.
- Telethon/MTProto user sessions and any Telegram group message feed (v1). The Telegram *Bot* API is used only for alerts (spec 01/14).
- `/guilds/{id}`, `/guilds/{id}/preview`, `/guilds/{id}/channels` (401 keyless; not needed).
- Parsing invite/handle strings out of identity sources (04 `normalise.py` owns it), X collection (09), LLM classification (10), red-flag multipliers, alerts on red flags and the needs-review digest (12/14).
- Storing any username, user id, avatar, `inviter` object or widget `members[]` entry.
- tao.app social analytics (checklist C5 — optional future source).

---

## 2. Design

### 2.1 Components and data flow

```
 04 registry ─ watch_channels(discord_invite | discord_channel | discord_handle | telegram) ─┐
 seed/discord_channels.csv ─► ChannelMapLoader ─► watch_channels(source='operator', meta.kind)┤
                                                                                              ▼
 discord_6h  cron 00/06/12/18:00Z                                              TargetPlanner (due rule §2.2)
   └─► InviteClient ─► Http('discord') ─► GET discord.com/api/v10/invites/{code}?with_counts=true&with_expiration=true
         │   one fetch per distinct code per cycle · 404 memo (never retried in-cycle) · InvalidRequestGuard
         ├─► TopologyClassifier (per occupant; own_server > main_channel > main_vanity > invalid > handle_only > none)
         ├─► discord_snapshots  (occupant×target rows · ONE main-guild row/cycle · census rows)
         ├─► watch_channels.guild_id / is_main_guild / verified_at   (discord_invite rows only)
         ├─► metric_values evidence keys discord_topology · inflated_days · tg_members (as_of = cycle_at, §2.9)
         └─► community_events: invite_expired · invite_dead · community_venue_changed
                               (00Z run only: inflated_community · silence_45d)
 discord_widget_1h ─► Http('discord') ─► GET /guilds/{id}/widget.json ─► discord_snapshots(probe='widget'), members[] dropped
 discord_gateway (LongTask, discord.py) ══ gateway ══ operator's aggregator guild ONLY
   on_ready → backfill channel.history(after=watermark) ; on_message → classify_message() ─► announcements(platform='discord',
   via_own_server=1) + social_post_mentions(match_kind='team') ; hourly collector_runs heartbeat
 telegram_daily ─► Http('tme') ─► t.me/{h} · t.me/+{hash} · t.me/s/{h} ─► telegram_snapshots, announcements(platform='telegram')
 app/features/community.py (8 producer fns) ◄── spec 12 features_hourly / scoring compute_registered ──► metric_values
        (raw rows via 03 put_metric_values, source='community')   spec 12 derive(): announcement_cadence_30d = Σ legs · silence_days = min legs (+ 09's X legs)
 announcements ─► spec 10 queue.select() (only rows with text, i.e. via_own_server=1)
```

Occupant set for every job: `active_occupants(db, include_pre_launch=True)` (04 §3.3) minus root netuid 0 (as 06/07/12). Deregistered occupants are never polled; their rows stay for survival analysis. `netuid_reoccupied` needs no special handling: the new occupant gets its own `watch_channels` rows from 04 and starts fresh history (master §5).

### 2.2 Targets, cadence tiers and the due rule

Targets come from `watch_targets(db, platform)` (04) for `discord_invite`, `discord_channel`, `discord_handle`, `telegram` (`retired_at IS NULL`). **A row declared only by `legacy`** (`meta.declared_by == ["legacy"]`, the old bot's frozen `fundamentals.json`) **is not a live claim and is ignored** (never fetched, never classified, never counted as a declared Telegram handle): on 2026-10-17, five of the six legacy-only Discord invites opened a *previous* occupant's server after re-registration (SN10 Pareton → "Sturdy", SN53 engy → "SignalPlus", SN94 Cathedral → "BITSOTA", SN109 Finsight → "Academia", SN113 LongShort → "TensorUSD"; only SN80's `dogelayer` → "OpenRoboto" was current). A correct legacy venue is restored through 04 `manual_overrides`. `legacy` also never counts in `source_agreement` (06's lesson). The root cause is a 04 finding (§11 Q27). Discord invite codes are case-sensitive and are fetched **once per distinct code per cycle** regardless of how many occupants declare them; results fan out to every declaring occupant.

| target kind | how recognised | cadence | requests/day |
|---|---|---|---|
| own-server invite | last successful fetch: `guild.id ≠ DISCORD_MAIN_GUILD_ID` | every cycle (4/day) | ≈ 25–30 codes × 4 ≈ 100–120 |
| main vanity | code = `DISCORD_MAIN_VANITY_CODE` (`bittensor`) | every cycle, **1 request** shared by all ≈ 64 occupants | 4 |
| main-channel invite | last successful fetch: `guild.id = DISCORD_MAIN_GUILD_ID`, code ≠ vanity | weekly (`DISCORD_MAIN_CHANNEL_RECHECK_DAYS` = 7) | < 1 (≈ 5 codes, e.g. SN15 `15・oro・ο`) |
| dead code | last successful fetch returned 404 `10006`/`50270` | daily (first 00Z cycle ≥ 20 h after the last check) for `DISCORD_DEAD_WEEKLY_AFTER_DAYS` = 30 d, weekly thereafter | ≤ 9 |
| new / last attempt failed | no row with `invite_valid IS NOT NULL` yet, or last attempt was a failure | next cycle | ~0–2 |
| deep-link / operator channel id (`discord_channel`) | 04 or CSV | **no HTTP** — a bare channel id is unverifiable keylessly (`/channels/{id}` needs a token and membership); census only. Master §8's "weekly re-validation of main-server channel ids" is therefore met through the main-channel invite codes above (§11 Q23) | 0 |
| `discord_handle` | 04 | never polled; census only | 0 |

Due rule (pure, `discord_topology.is_due(state, *, code, now, cycle_at, cfg) -> bool`; `cycle_hour = cycle_at.hour`), where `state` = latest `discord_snapshots` row with `probe='invite' AND invite_code=:code` (any occupant) plus the first `observed_at` of the current dead streak:

```
if state is None or state.invite_valid is None:            return True        # never fetched / last attempt failed
if already fetched OK in this cycle_at:                     return False       # idempotent manual re-run
if code == DISCORD_MAIN_VANITY_CODE:                        return True
if state.invite_valid == 1 and state.guild_id != MAIN:      return True
if state.invite_valid == 1 and state.guild_id == MAIN:      return now - state.observed_at >= RECHECK_DAYS - 3h
# dead
if now - dead_since < DEAD_WEEKLY_AFTER_DAYS:               return cycle_hour == 0 and now - state.observed_at >= 20h
return now - state.observed_at >= 7d - 3h
```

### 2.3 Invite fetch and parse

```
GET {DISCORD_API_BASE}/invites/{urlquote(code, safe='')}?with_counts=true&with_expiration=true
User-Agent: {DISCORD_USER_AGENT}        Accept: application/json
Authorization: Bot {DISCORD_BOT_TOKEN}  ← only when the token is set and DISCORD_INVITES_USE_BOT_TOKEN=true
provider='discord' (01 limiter: 1 req/s, burst 1, hourly cap 200) · follow_redirects=False · ok_statuses=(200, 404, 429)
```

`Http.request` (01 §2.12) already guarantees: 404/403/401 returned unchanged and **never retried**; 5xx/timeouts backoff 1 s, 4 s; `limiter.observe()` penalizes the bucket on `Retry-After`. `fetch_invite` passes `ok_statuses=(200, 404, 429)` so a 429 is handed back instead of being retried blindly (G1: any status listed in `ok_statuses` is returned to the caller without retry): a JSON 429 → `limiter.observe('discord', {"Retry-After": str(body["retry_after"])}, host='discord.com')` (a per-host cooldown that `acquire` waits out) then exactly one more `Http.request`; a second 429 is a failure; a non-JSON 429 is case (c) below. The collector adds: (a) a per-run memo `fetched: dict[code, InviteResult]` so a code (incl. a 404) is requested at most once per run; (b) `InvalidRequestGuard` — counts 401/403/429 responses from provider `discord` over a trailing 10 min (in process); at `DISCORD_INVALID_GUARD_PER_10MIN` (100, ≪ Discord's 10,000/10 min Cloudflare ban threshold) further `discord` requests are refused until the window drains (remaining targets of the run are recorded as failures); (c) a 429 whose body is not JSON (a Cloudflare block page, e.g. error 1015 — exact body shape UNVERIFIED, the fixture is synthetic) → `limiter.penalize('discord', 3600)` (provider-wide; the built 01 limiter persists penalties to a file, so a restart does not resume hammering a ban) and abort the run with `CloudflareBan(ProviderError)`. Unauthenticated responses carry no `X-RateLimit-*` headers; with a Bot token they do and `limiter.observe()` honours them. Counts are edge-cached (`Cache-Control: public, max-age=300`), so nothing polls faster than 5 min and a sample may lag ≤ 5 min.

`parse_invite(code, status, body, fetched_at) -> InviteResult` (pure; signature in §8.2):

| `InviteResult` field | source | rule |
|---|---|---|
| `invite_valid` | status | 200 with `guild` object → 1 · 404 with JSON `code` → 0 · anything else → `None` (failure) |
| `invite_error_code` | 404 body `code` | `10006` Unknown Invite (revoked/never existed) · `50270` Invite is expired · other JSON code stored as-is. `meta.error` is written only for failures and the two synthetic dead cases below (`expired_by_clock`, `not_guild_invite`), never for a plain 404 JSON |
| `guild_id` | `guild.id` | string; 200 without `guild` (group-DM/friend invite, `type ≠ 0`) → `invite_valid=0`, `meta.error='not_guild_invite'` |
| `member_count` | `approximate_member_count` | fallback `profile.member_count` |
| `presence_count` | `approximate_presence_count` | fallback `profile.online_count` |
| `presence_ratio` | derived | `presence/member` when both present and `member > 0`, else NULL |
| `expires_at` | `expires_at` | ISO-8601 in the body → `parse_iso` → stored as INTEGER unix seconds (B1), NULL = permanent. `with_expiration` is no longer documented but `expires_at` is always present (research 02). A valid invite whose `expires_at ≤ fetched_at` is treated as expired (`invite_valid=0`, `invite_error_code=50270`, `meta.error='expired_by_clock'`) |
| `landing_channel_id` / `_type` | `channel.id`, `channel.type` | type 5 = GUILD_ANNOUNCEMENT (Followable) |
| `features_json` | `guild.features` | JSON array, sorted (brief name `features`; 03's `_json` suffix rule) |
| `boosts` | `guild.premium_subscription_count` | |
| `verification_level` | `guild.verification_level` | |
| `guild_created_at` | `snowflake_to_datetime(guild.id)` | `((int(id) >> 22) + 1420070400000) ms` → tz-aware `datetime` in `InviteResult`, stored as INTEGER unix seconds (floor) |
| `meta` | | `{guild_name, vanity_url_code, premium_tier, nsfw_level, invite_type, invite_flags, landing_channel_name, profile_visibility}` — **`inviter` is dropped in the parser and never reaches storage or logs** |

A 404 without a JSON body (HTML error page) is a failure (`None`), not a dead invite.

### 2.4 Topology classification, primary guild, census

After the fetch phase, for each occupant `O` build its evidence set from the **latest successful** observation of each of its targets inside that target's freshness window (not only this run's; a failed fetch never erases older valid evidence inside the window — observation-within-freshness, not forward-fill): own-server/vanity codes 2 d, main-channel and dead codes 8 d (C6: weekly re-checked main-server evidence 8 d; community fn metrics 2 d). Targets with no successful observation inside the window are `unknown`.

```
MAIN = DISCORD_MAIN_GUILD_ID ; main_alive = vanity valid within 2 d
own      = valid targets with guild_id ≠ MAIN (and not expired)
main_ch  = valid main-guild invites whose landing channel is O's channel
           (landing_channel_id ∈ O's discord_channel rows, or landing_channel_name matches
            ^\D*?(\d{1,3})\s*[・·•|:\-] with group(1) == O.netuid)
         ∪ O's discord_channel rows with guild_id == MAIN          (only if main_alive)
main_van = valid main-guild targets not in main_ch (incl. the vanity)
dead     = targets with invite_valid = 0
class = own_server   if own
      | main_channel if main_ch
      | main_vanity  if main_van
      | unknown      if unknown_targets > 0, or identity_resolved(O,'discord').status = 'unknown'
      | invalid      if dead
      | handle_only  if O has discord_handle rows
      | none         (no pollable Discord target: meta.reason='no_discord' when identity_resolved(O,'discord') ∈ {absent, junk_only},
                      'no_pollable_target' when it is 'present' but 04 produced no invite/channel/handle row — the class is total)
unknown_targets = number of O's discord_invite targets with no successful observation in their window
                + 1 if O has main-guild discord_channel rows and the vanity has no successful observation within 2 d
                (discord_handle rows and non-main discord_channel rows are never polled and never count as unknown)
```

A positive class wins even when other targets are unknown (a reachable venue is reachable), but every row carries `unknown_targets`, and no 0.0 is derived while it is > 0 (§4.2): e.g. `main_vanity` + an unknown own-server code → `venue_reachable None`, not 0. `discord_channel` rows whose `guild_id ≠ MAIN` (deep links into own servers) cannot be validated keylessly; they do not change the class (the own server's invite does). `unknown` is an addition to the six classes of the brief: it is the only honest label when the evidence failed (§11 Q2).

**Primary target** (`is_primary=1`), class `own_server` only: keep the previous cycle's primary guild if still in `own` (stickiness — avoids growth series jumping between two servers, e.g. SN49's two "Nepher Robotics" guilds); else prefer the target whose `watch_channels.is_resolved=1`, then lower `watch_channels.priority` (03: manual-force 5 · chain 10 · subnetalpha 20 · tmc/tmc_coldkey 30 · manual-fallback/legacy 40 · operator 45 — master §5 precedence), then larger `member_count`, then lexicographically smallest code. For every other class the occupant's census row (below) is primary.

**`source_agreement`** per occupant: `G_s` = guild ids of valid targets declared by source `s` (every source in `watch_channels.meta.declared_by`, `legacy` excluded), **excluding the main vanity** (a generic fallback, not a claim about a guild). With ≥ 2 non-empty `G_s`: `disagree` if any two are disjoint (SN20, 46, 49, 55, 62, 64, 80, 90 on 2026-09-22), else `agree`; `one_dead` if a source declares only dead codes while another has valid ones; `single_source` if exactly one source has evidence; NULL if no non-vanity evidence. Live 2026-10-17: agree 11 · disagree 3 · single_source 26 · one_dead 1.

**Rows written per run** (§3.2 DDL):
- one `probe='invite'` row per (declaring occupant × code fetched this run), all carrying the occupant-level `topology_class`, `is_primary`, `source_agreement`, `declared_by_json`; failures are written too (`invite_valid NULL`, `http_status`, `meta.error`) so the planner and `/health` see them;
- the main vanity: **one** row per cycle with `occupant_key NULL` (main-server counts are not a subnet metric — "dedupe main server to one sample per cycle");
- one `probe='census'` row (`is_primary=1`) for every occupant that has no primary row in this slot (main_channel, main_vanity, invalid, handle_only, none, unknown, and own_server whose fetch failed this cycle): counts NULL, `guild_id` = the primary/main guild if any, `landing_channel_id` = the main-server channel id for `main_channel`, `invite_code` = the defining **invite code** if any — never a `discord_handle` value (those are personal usernames, e.g. `tl_arrash`, and never reach `discord_snapshots` or logs) —, `invite_valid`/`expires_at` = that code's latest evidence. Every active occupant therefore has ≥ 1 row per cycle and absence is evidenced point-in-time.

Idempotence: a manual re-run inside the same `cycle_at` skips codes already fetched OK and occupants already censused in that slot (planner check; no unique index needed).

### 2.5 Widget poller (`discord_widget_1h`)

Guild set: the **primary** own-server guilds (latest `is_primary=1` row per occupant within 7 d with class `own_server`), minus `DISCORD_WIDGET_SKIP_GUILD_IDS` — a secondary own guild has no occupant to fan its rows out to. Live 2026-10-17: 26 guilds, 6 widgets enabled, 20 disabled (403 `50004`); `members[]` is capped at 100 and `channels` is often empty. Per guild, from its latest `probe='widget'` row: `widget_enabled=1` → fetch every run (hourly); `widget_enabled=0` (403, code `50004` Widget Disabled) → probe only when the last probe is ≥ `DISCORD_WIDGET_DISABLED_RECHECK_DAYS` (7) old — **403s count toward the 10,000/10-min invalid-request budget**; never probed → probe now. Request `GET {DISCORD_API_BASE}/guilds/{id}/widget.json`, same UA, **no** Authorization header, provider `discord`.

`parse_widget(status, body)` keeps only `presence_count`, `instant_invite` (code extracted from the URL) and `len(channels)`; `members[]` (usernames, avatars, statuses) is deleted inside the parser before anything else sees the dict. Rows: `probe='widget'`, fanned out to every occupant whose primary guild is this guild, `member_count NULL`, `presence_count`, `widget_enabled`, `http_status`, `meta={instant_invite_code, channel_count}`. 404 code `10004` (Unknown Guild) → `widget_enabled NULL`, `meta.error='unknown_guild'` (the invite poller confirms next cycle). Widget presence is a diagnostic hourly series (UI). **ts-1.0 `presence_ratio` uses only the invite samples at the four fixed UTC hours**, so every guild is measured at the same times of day (widget and invite presence agree within ~2 %: Cooking 1311 vs 1307, Tensorplex 364 vs 358).

### 2.6 Aggregator-server Bot (`discord_gateway`)

The operator owns a private server ("TaoSentinel Feed", checklist B4). Announcement (type 5) channels of the main server and of own-servers are **Followed** into its text channels by hand (B5); Discord then crossposts every *published* message into the target channel through a channel-follower webhook. The Bot is a member of that one guild only and reads those copies.

- Library `discord.py>=2.4,<3` (MIT; exact latest version UNVERIFIED — pin at build; adds `aiohttp`). `discord.Client(intents=I, max_messages=None, chunk_guilds_at_startup=False, member_cache_flags=discord.MemberCacheFlags.none())` with `I = discord.Intents.none(); I.guilds = True; I.guild_messages = True; I.message_content = True`. `guild_messages` (non-privileged) is required for MESSAGE_CREATE in guild channels; `message_content` is the privileged intent toggled in the portal (< 100 guilds → no verification). `members`/`presences` stay off.
- `run_once(stop)`: build client, register handlers, `await client.start(DISCORD_BOT_TOKEN, reconnect=True)` in a child task, wait for `stop` or client exit, then `await client.close()`. A beat loop calls `self.beat()` every 30 s while the supervisor is alive (liveness = event loop healthy, not "Discord reachable" — a Discord outage must never make systemd restart the process); `connected = client.is_ready() and not client.is_closed()`.
- **Guild guard (`on_ready`)**: the aggregator guild must be in `client.guilds`, else `last_error='bot_not_in_aggregator_guild'` (idle, no reconnect storm). Any other guild in `client.guilds` → logged once at `warning` and listed in `status().foreign_guilds` (`/health`); every event from it is ignored (the bot never reads it; leaving is the operator's decision). No Telegram alert: 03 §3.9 has no type for it and nothing from that guild is read (§11 Q25).
- **Channels read**: text (0) and announcement (5) channels of the aggregator guild where the bot has `read_message_history`, restricted to `DISCORD_AGGREGATOR_CHANNEL_IDS` when non-empty. If `DISCORD_GATEWAY_ENUMERATE_FOLLOWS` and the bot has *Manage Webhooks*, `await channel.webhooks()` lists follower webhooks (`source_guild`, `source_channel` id + name) into `job_state('discord_gateway','follows')` for `/health`; `Forbidden` → logged once, skipped.
- **Backfill (every `on_ready`, i.e. start and every new session)** — port of the whale collector watermark (`collector.py:145-147`): per channel `wm = job_state('discord_gateway', f'wm:{channel_id}')` (last processed local message id); `after = discord.utils.time_snowflake(snowflake_time(wm) − DISCORD_BACKFILL_OVERLAP_SEC)` or, first time, `now − DISCORD_BACKFILL_DAYS`; `async for m in channel.history(after=discord.Object(after), limit=DISCORD_BACKFILL_MAX_PER_CHANNEL, oldest_first=True): await ingest(m)`; the cap is the runaway guard (`collector.py:37,159-182`). A capped backfill records `job_state('discord_gateway', f'bfcap:{channel_id}')`, and the next backfill starts **exactly at the watermark** (no overlap) — otherwise a page that lies wholly inside the overlap window would be re-read forever. `INSERT OR IGNORE` on `msg_id` makes the overlap and repeated `on_ready` harmless. Follows deliver nothing published before the Follow was created; backfill only covers downtime.
- **Live**: `on_message → ingest(m)` (updates the watermark); `on_raw_message_delete` → `UPDATE announcements SET deleted_at=? WHERE local_message_id=? AND deleted_at IS NULL` (set-once; the only in-place write on this append-class table, which 03 §3.5 permits as an annotation; the row is kept and still counts); edits are ignored in v1 (nothing written, §11 Q13). discord.py's own REST rate-limit handling covers history calls; all calls are inside the operator's guild, so no 401/403 is expected.
- **Hourly heartbeat**: one `collector_runs` row per UTC hour (`job='discord_gateway'`, `ok=1` if connected ≥ 1 min that hour else `ok=0, error='disconnected'`, `rows` = messages ingested), written when the hour completes (and as a partial row on shutdown; an upsert keeps `MAX(ok)`, `MAX(rows)`, so a restart inside the hour never downgrades it). `finished_at = hour_ts + 3600` is the instant the row became knowable — the point-in-time coverage log the Discord legs of §2.9 read (by `finished_at`).
- **Fatal errors**: `discord.LoginFailure` (bad/revoked token) or `discord.PrivilegedIntentsRequired` → `fatal_error` set and persisted in `job_state('discord_gateway','fatal') = {error, since, token_hash}` (a restart stays idle), `AlertLatch.fire('discord_gateway_fatal', text, event_type='discord_gateway_fatal', severity='warning', payload={error, close_code, retries, since}, dedupe_key=since, source='community')` — **02 §D1 (amended 2026-09-23) added this system type**, 03's catalog and 14's routing (immediate) carry it; the earlier draft's `collector_failing` reuse is withdrawn. The supervisor stays idle (no reconnect) until the token hash changes (`reload-config`) or `DISCORD_GATEWAY_ENABLED` flips. The next successful `on_ready` clears it with `collector_recovered` (payload `{collector: 'discord_gateway', failures, down_seconds}`, dedupe `discord_gateway|{since}`), emitted directly when the in-memory latch was lost to a restart. Other exceptions → 01 `LongTask` backoff 5 s → 300 s (≤ 288 session starts/day ≪ the 1,000/day limit).

`classify_message(msg, cmap, guild_map, cfg, now) -> AnnouncementRow | None` (pure; `cmap` = in-memory channel map §2.8, `guild_map` = primary own-server guild id → occupant keys from the latest `discord_snapshots` within 7 d):
1. `msg.guild is None or msg.guild.id != DISCORD_AGGREGATOR_GUILD_ID` → None.
2. `msg.type == MessageType.channel_follow_add` → record the follow label in `job_state('discord_gateway','follows')`; return None (the system message's user is not stored).
3. Crosspost test: `msg.flags.is_crossposted` (IS_CROSSPOST, bit 1<<1) **and** `msg.reference is not None` **and** `msg.webhook_id is not None`. Anything else (operator chatter) → None, never stored.
4. `src_guild, src_channel, src_msg = msg.reference.guild_id, msg.reference.channel_id, msg.reference.message_id`.
5. Occupant mapping, first match wins:
   - `src_channel == DISCORD_MAIN_ANNOUNCE_CHANNEL_ID` or an ecosystem CSV row → occupants ∅, `author_is_team=0`, `mapping='main_announce'` (Foundation posts reach P6 through spec 10 entity extraction, not P5 cadence);
   - `src_channel` ∈ `cmap` with `kind='announce'` → its occupant(s), `author_is_team=1`, `mapping='channel'`;
   - `src_guild` = primary own-server guild of exactly one occupant (latest `discord_snapshots` within 7 d) → that occupant, `author_is_team=1`, `mapping='guild'`; of ≥ 2 occupants → all of them, `author_is_team=1`, `mapping='guild_ambiguous'`;
   - otherwise occupants ∅, `author_is_team NULL`, `mapping='unmapped'`; the webhook display name (`"{Guild} #{channel}"`, a channel label, not a person) is logged and `(src_guild, src_channel)` is listed in `status().unmapped_channels` so the operator can add the CSV row.
6. `composed = compose_text(msg)` = `msg.content` + for each embed: title, description, field names/values, footer text — joined by `\n`. `text = redact_ids(composed)[:DISCORD_TEXT_MAX_CHARS]` (4000), where `redact_ids` rewrites `<@!?\d+>` → `@user`, `<@&\d+>` → `@role`, `<#\d+>` → `#channel` so no user id reaches storage. Text is written only because the copy was received in the operator's own server (`via_own_server=1`). With `DISCORD_STORE_TEXT=true` (default, B10/F2), `text_purge_after` is NULL, and 03's `storage_retention_daily` (policy `nlp_text_retention`) NULLs the text after `LLM_TEXT_RETENTION_DAYS` (90 d), keeping `text_hash` (B10). With `DISCORD_STORE_TEXT=false` (operator decision, master §13 item 8), amended B10 still has the collector write the text as **pending classification**, with `text_purge_after = observed_at + LLM_PENDING_TEXT_MAX_DAYS·86400` (default 7 d). 03's `storage_retention_daily` NULLs it (hash kept) as soon as a `post_labels` row exists for `msg_id`, or once `text_purge_after` has passed. Spec 10 labels such rows in its normal 6-h batch.
7. `text_hash` = `text_hash(discord_clean(composed))` (on the untruncated text), where `discord_clean` removes `<@!?\d+>`, `<@&\d+>`, `<#\d+>`, `<a?:\w+:\d+>`, `@everyone`, `@here`. `text_norm`/`text_hash` are this spec's shared `app/nlp/textnorm.py` (B11), in this order: `unicodedata.normalize('NFKC', s)` → `lower()` → URLs `https?://\S+` → `' '` → mentions `@\w+` → `' '` → whitespace runs → `' '` → `strip()`; `text_hash(s) = sha256(text_norm(s).encode('utf-8')).hexdigest()`. 09 (X) and 10 import this module, so cross-platform dedupe (§2.9) compares like with like. Empty normalised text → `sha256('msg:' + msg_id)` so image-only posts never collapse into one hash.
8. `url_count` = distinct `https?://\S+` in content + embed URLs; `has_attachment` = `len(msg.attachments) > 0` or any embed with image/video/thumbnail.
9. `event_at = msg.created_at` (publication/crosspost time) as INTEGER unix s (`parse_ts`, B1); `observed_at` = ingest time (`now_ts()`); `msg_id = f"discord:{src_msg}"` (= spec 10's item id and `post_labels.post_id`, B11); `local_channel_id`, `local_message_id` = the copy's ids.
10. Write `announcements` (`INSERT OR IGNORE`) and, for `mapping ∈ {channel, guild, guild_ambiguous}`, one `social_post_mentions(platform='discord', post_id=src_msg, occupant_key, match_kind='team', event_at (copied from the announcement), observed_at, run_id)` row per mapped occupant (`post_id` is the native id, so `platform || ':' || post_id = msg_id` — 03's `item_id` convention); 08 never writes `social_posts` (B11); `announcements.occupant_key` is set only when exactly one occupant is mapped (same attribution rule as spec 09 §3.3 for shared team handles).

### 2.7 Telegram (`telegram_daily`)

Targets: `watch_channels(platform='telegram')`, handle `h` (public, `^[A-Za-z0-9_]{5,32}$`) or `+hash` (private invite). Each distinct handle is fetched once per run; results fan out per occupant. Provider `tme` (0.5 req/s), UA `HTTP_USER_AGENT`, `Accept-Language: en`, `follow_redirects=False`.

1. `GET {TELEGRAM_BASE_URL}/{h}` (or `/+{hash}`). `parse_tme_profile(html) -> TmeProfile` with stdlib `html.parser` (no new dependency): `og:title`, `og:description`, `div.tgme_page_title > span`, **every** `div.tgme_page_extra` (bot pages carry two: `@BotFather` then `7 594 066 monthly users`), `div.tgme_page_additional`. Numbers: strip U+00A0, U+202F, spaces and commas between digits (live 2026-10-17 the separator is a plain space: `23 326 members, 327 online`); accept `K`/`M` suffixes with decimals (`5.81M`). Rules on the extra texts (fallback chain extras → `og:description` → `og:title`; verified live 2026-10-17: private links carry the count in `tgme_page_extra` too, e.g. `474 members, 20 online` for SN34, and `tgme_page_additional` says "You are invited to the group|channel …"):
   - `(?P<n>\d[\d ,.]*[KM]?)\s+members?(?:,\s*(?P<o>\d[\d ,.]*[KM]?)\s+online)?` → kind `group` (`private_group` for `+hash`); e.g. `23 831 members, 619 online` (NATIX), `7 393 members, 219 online` (inference_labs), `472 members, 29 online` (SN34 private).
   - `(?P<n>…)\s+subscribers?` → `channel` / `private_channel` (SN52 `+ug7C1J7Apf8wNzc1`: 255 subscribers).
   - `monthly users` → `bot`.
   - no count and (description matches `You can contact @\w+ right away`, or an extra is just `@handle`) → `user_or_unknown` (SN30 `wahoopredict_support`; live, an existing user page has a title and a non-existent one does not, so `user_or_unknown` now means "a person or bot-less account").
   - 404, or 200 without `tgme_page_title` **but with Telegram's own not-found markers** (`og:title` `Telegram: Contact @x` / `Join group chat on Telegram`, or the `tgme_page`/`tgme_action_button_new` scaffold) → `not_found` (live: a non-existent handle and an invalid `+hash` both answer 200 this way). A 200 page with neither a title nor those markers is `parse_ok=0` — a layout change must never read as absence (0.0).
   - `deprecated_marker = 1` when the title matches `(?i)\b(deprecated|inactive|closed|archived)\b` (SN34 "BitMind AI (Deprecated)").
   - HTTP 200 with none of the anchors present → `parse_ok=0` (layout change = failure, not absence).
   - `title` is kept only for community kinds (`group`, `channel`, `private_*`); for `user_or_unknown`/`bot` it is set to NULL before storage (a person's display name must not be stored).
2. If kind = `channel` (public) and `TELEGRAM_PREVIEW_ENABLED`: `GET {TELEGRAM_BASE_URL}/s/{h}`. 301/302 (groups/users redirect back to the profile) or zero `tgme_widget_message_wrap` blocks → `preview_available=0`. Else `parse_tme_preview(html) -> list[TmePost]`: for each `div.tgme_widget_message[data-post="{h}/{n}"]` without class `service_message`: `post_no=n`, `event_at` = `a.tgme_widget_message_date time[datetime]` (ISO → `parse_iso` → INTEGER unix s, B1), text = `div.tgme_widget_message_text` (`<br>`→`\n`, tags stripped, `html.unescape`), `views` = `span.tgme_widget_message_views`, `has_attachment` = photo/video/document wrap present. ≈ 20 newest posts per page; no `?before=` paging in v1.
3. Posts → `announcements(platform='telegram', msg_id=f"telegram:{h.lower()}/{n}", source_guild_id NULL, source_channel_id=h.lower(), message_id=n, via_own_server=0, mapping='tg_handle', author_is_team=1` (every `watch_channels` telegram row is team-declared), `text` = the post text, always written (amended B10): with `TELEGRAM_POSTS_STORE_TEXT=false` (default, hash-only after classification), `text_purge_after = observed_at + LLM_PENDING_TEXT_MAX_DAYS·86400` (pending classification; 03 NULLs the text once a `post_labels` row exists or at expiry, hash kept); with `true`, `text_purge_after` is NULL and the 90-d rule applies; `text_hash=text_hash(text)` (B11), `url_count`, `has_attachment`, `meta_json={views}`) + one `social_post_mentions(platform='telegram', post_id=f"{h.lower()}/{n}", occupant_key, match_kind='team', event_at, observed_at, run_id)` row per occupant declaring the handle, all `INSERT OR IGNORE`; never a `social_posts` row (B11). Rows accumulate daily, so the 20-post window only truncates channels posting > 20/day.
4. One `telegram_snapshots` row per (occupant, handle, UTC day) (§3.2, `INSERT OR IGNORE` on `UNIQUE(occupant_key, handle, cycle_day_ts)`). The first attempt of the day wins: a manual re-run is idempotent, and a failed attempt (`parse_ok=0`) is not retried until the next day — the 2-day community freshness tolerates one missed day.

Budget ≈ 6 profile requests/day (7 declared links, 6 distinct handles: SN51 and SN22 share `daturavalidator`) + ≤ 6 preview requests. (All 7 subnet Telegram links on 2026-09-22 are groups/private/users, so the post feed is empty today; the code path exists for channels that appear later.)

### 2.8 Channel map (`seed/discord_channels.csv`) and the operator procedure

**Format** (UTF-8, header row, path `DISCORD_CHANNELS_SEED_PATH`):

```
netuid,channel_id,channel_name,guild_id,kind,notes
,830075335084474390,📣・announce,799672011265015819,announce,main server global announcements (Followed)
15,1466080859164643389,15・oro・ο,799672011265015819,subnet_text,
106,<announce channel id>,announcements,1039241590092353596,announce,Nodexo own server (Followed)
```

Rules: `channel_id`, `guild_id` match `^\d{17,20}$`; blank `guild_id` → `DISCORD_MAIN_GUILD_ID`; blank `kind` → `subnet_text` if the guild is main else `announce`; `kind ∈ {announce, subnet_text}`; blank `netuid` is allowed only for `kind=announce` (ecosystem channel, held in memory, not in `watch_channels`); the columns are exactly checklist B5's `netuid,channel_id,channel_name,guild_id,kind[,notes]` (02 §I2) and 3-column rows (`netuid,channel_id,channel_name`) still load; fields after the 6th are re-joined into `notes` with `,` (an unquoted comma in a note never drops the row); malformed rows are skipped, logged and counted (`status().csv_errors`). **Re-occupation guard** (no extra column): a `notes` token `reg=<network_registered_at>` binds the row to that occupant and the row is skipped with `csv_row_stale_occupant` if it is not the netuid's current occupant; a row without the token binds to the current occupant unless an operator row with the same `channel_id` already exists for an older occupant of that netuid, in which case it is skipped (`csv_row_stale_occupant`) until the operator adds `reg=<current network_registered_at>` to `notes`. This keeps a re-registered netuid from silently inheriting an old channel.

`load_channel_map(path) -> tuple[list[ChannelMapRow], list[str]]` — port of the whale `load_cohort` fail-safe (`collector.py:59-75`): missing/unreadable/malformed file → `([], [reason])`, logged, never raises. `sync_channel_map(db, rows, now, run_id)` upserts `watch_channels(occupant_key=current occupant of netuid, netuid, platform='discord_channel', handle=channel_id, url='https://discord.com/channels/{guild}/{channel}', source='operator', priority=45, guild_id, is_main_guild, meta_json={kind, channel_name, csv_netuid}, first_seen_at=now (insert only), last_seen_at=now, retired_at=NULL)` (all instants INTEGER unix s, 03); on a conflict with 03's `ux_watch_channels (COALESCE(occupant_key,''), platform, handle)` — e.g. a chain deep-link row for the same channel — it merges `meta_json` and keeps the existing `source`/`priority`. **Every** `discord_6h` run parses and syncs the whole file (≤ a few hundred rows, idempotent upserts that also set `last_seen_at = now`); re-parsing only on mtime change would let an unchanged CSV age into 04's 14-day retirement (`REGISTRY_WATCH_RETIRE_DAYS`) — and after a restart the in-memory map the draft relied on is empty while the mtime is unchanged, so nothing would be touched at all. `remap_unmapped` runs only when the mtime changed (`job_state csv_mtime`). The upsert stamps `meta.operator_since` once (the follow start the Discord legs use, since a merged 04 row's `first_seen_at` predates the Follow). Removing a CSV line lets 04 retire the row 14 d later. The gateway reloads its in-memory `ChannelMap` every 10 min on mtime change. The file lives in `seed/` (gitignored; operator data).

**Operator procedure** (executable form of checklist B4 = steps 1–4, B5 = steps 5–10):
1. discord.com/developers/applications → *New Application* "TaoSentinel" → *Bot* → *Reset Token* → `.env` `DISCORD_BOT_TOKEN=`. *Privileged Gateway Intents*: **Message Content Intent ON**; Server Members and Presence **OFF**. *Public Bot* **OFF**.
2. Create a private server "TaoSentinel Feed" with text channels `#feed-bittensor` and `#feed-subnets`.
3. *OAuth2 → URL Generator*: scope `bot`; permissions *View Channels* + *Read Message History* (integer `66560`; adding the optional *Manage Webhooks* → `536937472` lets the bot enumerate follows). Open the URL and add the bot to TaoSentinel Feed **only**.
4. *User Settings → Advanced → Developer Mode* ON; right-click the server → *Copy Server ID* → `DISCORD_AGGREGATOR_GUILD_ID=`; set `DISCORD_GATEWAY_ENABLED=true`; `POST /api/v1/control/reload-config` (authed) or restart.
5. Join `discord.gg/bittensor` with your own account (human use only — nothing on the user account is ever automated).
6. Open `📣・announce` → **Follow** → TaoSentinel Feed / `#feed-bittensor`. The system message "… has added Bittensor #📣・announce to this channel" appears; the bot logs it.
7. For each `NN・name・symbol` subnet channel you can see: right-click → *Copy Channel ID* → append `NN,<id>,<name>,799672011265015819,subnet_text`. 22 are already known from invites/chain deep links (`python -m app.collectors.discord --report topology` lists the occupants still `main_vanity`). When every visible channel is recorded, set `DISCORD_CHANNEL_MAP_REVIEWED=true` (§2.9 `venue_reachable`).
8. For each `own_server` occupant in the report: join via its invite, locate its Announcement channel (megaphone icon; the landing channels of SN35, 17, 65, 106 and 20 are already type 5), click **Follow** → `#feed-subnets`, copy the channel id and append `NN,<id>,<name>,<guild_id>,announce`. Servers without an Announcement channel cannot be Followed — skip them.
9. `python -m app run-job discord_6h` imports the CSV immediately; the gateway picks it up within 10 min.
10. After the first crossposts arrive, `/health` → `tasks.discord_gateway.unmapped_channels` must be empty; add CSV rows for anything listed there.

### 2.9 P5 metric computations (`app/features/community.py`)

**Contract (02 §C5, binding).** One producer coroutine per metric key, `async def compute_<metric_key>(db, occupant_key: str, as_of: datetime) -> MetricResult` with 12's `app/features/types.py::MetricResult(value: float | None, value_json: dict = {}, absence_reason: str | None = None)`. `as_of` is 12's `T_eval` (tz-aware UTC); every query reads only rows with `observed_at ≤ as_of` (INTEGER unix s, B1). A verifiable absence returns `MetricResult(0.0, value_json, absence_reason='no_venue' | 'no_reachable_venue')` → `absent` (normalises to **0.0**); `value=None` with `value_json["status"] == "not_applicable"` → `not_applicable` (None); `value=None` otherwise → `failed` (None); anything else → `ok`. The enum lands in `value_json.status`, from which 03 generates the `status` column (B3); producers never write a status column, and 08's domain enums use other keys (`class`, `reason`). 12's `compute_registered` writes the results through 03's `app/storage/pit.py::put_metric_values` (`stage='raw'`; `as_of` = the scoring slot or, for `features_hourly`, the hour bucket the run serves (B4) — provisional iff `as_of % 86400 ≠ 0`, there is no `is_provisional` column; `observed_at = T_eval`; `source='community'` (B9); `spec_version=SPEC_VERSION`; first success wins). Freshness `F = 2 d` for every 08 fn metric (C6; 12 registry rows 34–37, 47, 48, 50, 51). **Registration (C5):** `app/features/community.py` calls 12's `register_metric(key, fn=compute_<key>, pillar='P5', freshness_days=2, …)` once at import for its eight registry keys (legs add `weight=0.0, component_of='announcement_cadence_30d' | 'silence_days'`); the dotted paths `app.features.community:compute_<key>` in 12's rows name the same fns, and `register_metric` never adds keys. Before Phase 8 the import of 12's registry fails and registration is skipped (§8.2).

Frozen constants of this spec (ts-1.0, appended to `app/scoring/spec.py`, **not** config): `XPLATFORM_DEDUPE_HOURS=48`, `GROWTH_BASE_TOLERANCE_DAYS=3`, `INFLATION_DAYS=7`. The inflation thresholds (0.02, 1,000 members) and the healthy band (0.05–0.35) are 12's frozen `presence_ratio` registry params (`METRICS['presence_ratio'].params`: `inflate_below`, `inflate_members`, `low`, `high`); `community.py` reads them from there (before Phase 8: from the same literal defaults), never duplicating them, so 08's event and 12's gate cannot drift.

Helper `topology_at(O, t)` = latest `is_primary=1` row of `O` with `observed_at ∈ (t−F, t]` → `(class, guild_id, row)`; none → `(None, None, None)`.

| metric_key (registry role, normaliser) | computation — 0.0 / None semantics in §4.2 |
|---|---|
| `venue_reachable` (P5 .10, `binary`) | `1.0` if `disc_ok or tg_ok`: `disc_ok` = class ∈ {own_server, main_channel} and the primary invite's `expires_at` is NULL or `> as_of`; `tg_ok` = any latest-per-handle `telegram_snapshots` row in `(as_of−F, as_of]` with `http_status=200, parse_ok=1, kind ∈ {group, channel, private_group, private_channel}, members ≥ 1, deprecated_marker=0`. Verifiable absence (§4.2) → `MetricResult(0.0, …, absence_reason='no_venue')` when no venue exists in any source, `'no_reachable_venue'` when every declared venue is verifiably dead or not a community (C5). `value_json={discord_class, tg:[{handle, kind, members}], reason}` |
| `discord_members` (P5 .15, `logcap(cap=5000)`) | class `own_server` only: `member_count` of the latest `probe='invite'`, `invite_valid=1` row of the primary guild in `(as_of−F, as_of]`. Known class ≠ `own_server` → `MetricResult(None, {"status": "not_applicable", "reason": "not_own_server"})` (master: "main-server-only subnets → None") |
| `presence_ratio` (P5 .10, 12's `presence_ratio(low=.05, high=.35, inflate_below=.02, inflate_members=1000)` reading `value_json.members`) | class `own_server` only: samples = primary-guild invite rows with `invite_valid=1, member_count>0`, `observed_at ∈ (as_of−24 h, as_of]`, at least `DISCORD_PRESENCE_MIN_SAMPLES` (2); raw `r = median(presence_i) / median(member_i)` (master: "daily-median presence ÷ members"); `value_json={members: median(member_i), presence_median, samples, inflated: members > inflate_members and r < inflate_below}` |
| `discord_growth_30d` (P5 .10, `zclip(lo=-0.1, hi=0.3)`) | class `own_server` only: `m_now` = median `member_count` of primary-guild samples in `(as_of−24 h, as_of]`; `m_then` = same over the 24 h window ending `as_of − k` for the first `k` ∈ `30, 29, 31, 28, 32, 27, 33` d with ≥ 1 sample **of the same guild_id**; raw `(m_now − m_then) / m_then` (denominator: §11 Q6); `value_json={m_now, m_then, base_day, guild_id}`; forward-only, never extrapolated |
| `discord_announcements_30d` (leg of `announcement_cadence_30d`, w 0) | count of `announcements` (`platform='discord'`) joined to a `social_post_mentions(platform='discord', match_kind='team', occupant_key=O)` row with `observed_at ≤ as_of` (PIT attribution; `msg_id = 'discord:' ‖ post_id`), `event_at ∈ (as_of−30 d, as_of]`, `announcements.observed_at ≤ as_of`; drop rows whose `text_hash` equals that of an O-attributed team X post (`social_posts platform='x', is_team_account=1, bot_filtered=0, observed_at ≤ as_of`, attributed by `occupant_key=O` or a team mention) with `|Δevent_at| ≤ XPLATFORM_DEDUPE_HOURS`; rows sharing a `text_hash` within 48 h count once. `value_json={dedup_x, dedup_self, partial_window}` (`partial_window=true` when the follow started after `as_of−30 d`) |
| `tg_posts_30d` (leg of `announcement_cadence_30d`, w 0) | same for `platform='telegram'` (mentions with `platform='telegram'`); additionally drop `text_hash` matches with O's Discord announcements within ±48 h |
| `discord_last_activity_days` (leg of `silence_days`, w 0) | days from O's latest team Discord announcement (any `event_at ≤ as_of`, same PIT join; cross-platform duplicates **do** count here — a post is a communication on every platform it appears) to `as_of`, `value_json.lower_bound=false`; none ever but a followed `kind='announce'` channel exists since `follow_since` (= `first_seen_at` of the operator CSV row) → `as_of − follow_since` with `value_json.lower_bound=true` |
| `tg_last_activity_days` (leg of `silence_days`, w 0) | days since O's latest Telegram post (`announcements`, or `telegram_snapshots.last_post_at`); preview OK with zero posts → days since O's first snapshot of that handle with `value_json.lower_bound=true` |

**Combiners — derived by spec 12 §2.6, not by this spec** (C3; restated so the leg contract is complete; 12 writes them with `source='scoring'` (B9), freshness 1 d (C6); 08 has no `compute_announcement_cadence_30d`/`compute_silence_days`):
- `announcement_cadence_30d` (P5 .25, `logcap(cap=12)`) = Σ over the `ok` legs of (`x_cadence_30d` (09), `discord_announcements_30d`, `tg_posts_30d`); absent legs add 0; no `ok` leg → None. Because the Discord and Telegram legs already dropped cross-platform duplicates (±48 h, same `text_hash`), 12's Σ is the **union** of distinct team communications the master asks for ("team posts in followed announcement channels + team X posts + TG posts").
- `silence_days` (P5 .15, `recency(full_at=7, zero_at=45)`), legs (`x_last_activity_days` (09), `discord_last_activity_days`, `tg_last_activity_days`): exact legs E = `ok` legs without `value_json.lower_bound`, lower-bound legs B = `ok` legs with it; `min(E)` if E ≠ ∅, else `min(B)` with `lower_bound=true`; no `ok` leg ∧ `venue_reachable` raw 0 (ok or absent) ∧ X leg absent (`x_cadence_30d` absent, or no `watch_channels(platform='x')` row with `first_seen_at ≤ cutoff`) → `absent` (→ 0.0; C3: `silence_days = 0` only when no venue is reachable **and** no X account exists); else None. No sentinel value (such as 9999 days) is ever written; absence is the `absent` status.

`x_engagement_30d` (.10, `rankg`) and `x_followers` (.05, `logcap(cap=50000)`) are produced by spec 09 (`app/features/social_x.py`) and complete P5; this spec does not touch them.

**Normalisation** of `presence_ratio` (master: "custom") — this spec's shape, implemented once as 12's `presence_ratio(r, members, low=.05, high=.35, inflate_below=.02, inflate_members=1000)` in `app/features/normalise.py`: `r` or `members` None → None; `members > 1000 ∧ r < 0.02 → 0.0` (inflation); `r < 0.05 → r/0.05`; `0.05 ≤ r ≤ 0.35 → 1.0`; `r > 0.35 → max(0.5, 1 − (r − 0.35)/0.7)` (tiny / bot-heavy servers, floor 0.5 from r = 0.70). Reference values (2026-09-22 research counts, recomputed in review): Tag101 1905/9 → r 0.00472 → **0.0**; Tensorplex 20428/358 → 0.0175 → **0.0** (§11 Q7); Nodexo 1986/142 → 0.0715 → 1.0; Chutes 5885/1245 → 0.2116 → 1.0; Nepher 390/137 → 0.3513 → 0.998.

`inflated_days(db, O, as_of) -> int | None` (helper for the `inflated_community` **event**, `--report` and the evidence key below): number of consecutive completed UTC days, ending the day before `as_of`, whose per-day `r_d = median(presence)/median(members)` over ≥ 2 invite samples of the primary guild has `r_d < inflate_below` and median members `> inflate_members`; a day with < 2 samples ends the streak; `None` if the occupant is not `own_server`. The multiplier itself is 12's `inflated_community` gate (7 exact-`as_of` `presence_ratio` raw rows, 12 §2.8), so the event is evidence only.

Evidence-only keys (12 §2.4 "keys outside this table": no weight, no normaliser, never normalised), produced by `compute_discord_topology` (value = rank own_server 5 · main_channel 4 · main_vanity 3 · invalid 2 · handle_only 1 · none 0; class `unknown` → `failed`; `value_json={class, guild_id, source_agreement, unknown_targets}`), `compute_tg_members` (max `members` over fresh community-kind Telegram rows; none → `not_applicable`) and `compute_inflated_days` (the helper above; not `own_server` → `not_applicable`). They are catalogued evidence keys (C3: 03 `METRIC_KEYS`, role `evidence`) outside 12's registry, so 12 never calls these three fns and `register_metric` does not accept them. **Writer:** every `discord_6h` run, after classification, calls `write_evidence(db, occupant_keys, as_of=cycle_at, run_id)`, which evaluates the three fns for every active occupant at the write instant `now` (so this cycle's rows are visible; inputs all have `observed_at ≤ now`) and writes them through `put_metric_values` with `as_of = cycle_at` (the scoring slot they serve, B4; the run finishes before the :15 scoring run), `stage='raw'`, `source='community'`, `observed_at = now`, first success wins. ≈ 3 × 128 rows per cycle.

### 2.10 Community events

Emitted by `app/collectors/community_events.py` from `discord_6h` (every run: invite events, venue changes; the run at `cycle_at = 00:00Z`: `inflated_community` from `inflated_days` at `as_of = cycle_at`, and `silence_45d` from the latest fresh (≤ 1 d, `observed_at ≤ cycle_at`) `silence_days` raw row that spec 12 derives — before Phase 8 no such row exists and no silence event is emitted) and from `telegram_daily` (Telegram venue changes). All writes go through 03's `app/storage/events.py::emit_event(db, event_type=…, occupant_key=…, netuid=…, severity=…, event_at=…, observed_at=…, payload=…, dedupe_key=…, source='community', run_id=…)` (dedupe on `UNIQUE(event_type, dedupe_key)`; 03 §3.9 lists all five types with their required payload keys, D1/D2 — unknown types or missing keys raise). Severity vocabulary `info | warning | critical` only (B2). **08 never sends Telegram itself** (`alerted_at` NULL, D3): 14's `alert_dispatch` routes by 03's alert column — warning-level `invite_expired`/`invite_dead`/`community_venue_changed` immediately, the rest (incl. `silence_45d`, `inflated_community`) to the digest (checklist D5 "dead invites"). The flag pages are 12's `red_flag_raised`/`red_flag_cleared` (D4) for `silence_45d`, `inflated_community` and `stale_identity`; 12's gates read `silence_days`, `presence_ratio` and `venue_reachable` raw rows (12 §2.8), not these events, so the events are evidence only. Venue changes are debounced: the new state must hold for `COMMUNITY_VENUE_CHANGE_DEBOUNCE` (2) consecutive cycles (Discord) / days (Telegram); pending state lives in `job_state('discord_6h', f'topo:{occupant_key}')` / `job_state('telegram_daily', f'tg:{occupant_key}:{handle}')`. Payloads in §3.5.

### 2.11 ToS posture (binding)

1. Only documented endpoints (`GET /invites/{code}`, `GET /guilds/{id}/widget.json`, the gateway and `channel.history` inside the operator's own guild); ≈ 250 requests/day (≈ 0.003 req/s), ≥ 1 s spacing, `Retry-After` honoured, 404s never retried in-cycle, invalid requests (401/403/429) capped at 100/10 min in-process.
2. Aggregate counts only. `inviter`, widget `members[]`, webhook author names, user ids and avatars are dropped in the parsers; user/role mention ids are redacted from stored crosspost text (§2.6 step 6); Telegram titles of user/bot accounts are not stored; `tests/collectors/test_privacy_community.py` asserts no username-bearing key reaches the DB or the logs.
3. Registered application: when `DISCORD_BOT_TOKEN` is set, invite calls send `Authorization: Bot …` with a `DiscordBot (url, version)` User-Agent so traffic is attributed under the Developer Terms the operator accepted (identical counts with a token are UNVERIFIED — §10 step 4 checks).
4. No self-bots; the operator's user account is used by hand only (Follow clicks, Copy ID). The Bot is invited to the operator's own server only and never reads any other guild.
5. Discord message **text** is stored only for crossposts received in the operator's own server (`via_own_server=1`), with ids redacted, and is used for inference-only classification (spec 10). It is NULLed after 90 d with `DISCORD_STORE_TEXT=true`. With `false` it is held only as pending-classification text: NULLed once labelled, or after `LLM_PENDING_TEXT_MAX_DAYS` (7 d). TaoSentinel never reads third-party servers, and no other Discord text is ever stored (B10, master §6; 03 §3.5 enforces it with a CHECK). Telegram post text follows the same rule: with the default `TELEGRAM_POSTS_STORE_TEXT=false` it is pending-classification text only (amended B10).
6. Private, non-commercial use; no redistribution of API data. The Discord Terms' anti-scraping clause and Developer Policy §15/§16/§20 leave automated polling of the public invite endpoint in a grey zone; the residual risk is the operator's call (§11 Q17). `DISCORD_ENABLED=false` turns all keyless Discord polling off.

---

## 3. Data contract

Names follow master §6 and 03's binding conventions (03 §2.1, 02 §B1): every instant is `INTEGER` unix seconds UTC (ISO-8601 only at the API/Telegram boundary via `app/util/time.py`), bucket columns end in `_ts`, JSON columns end in `_json`, booleans are `INTEGER CHECK (x IN (0,1))`, every raw row carries `observed_at`, `event_at`, `source`, `run_id`, tables are `STRICT` (SQLite ≥ 3.37), Discord snowflakes are `TEXT`. The three tables below mirror the DDL that 03 §3.5 adopted from this spec (re-checked 2026-09-23: `probe` + a separate `source`, `cycle_day_ts`, generated `announcements.source`, the `text` CHECK); 03 owns the final DDL and migration, and any later drift is resolved in 03's favour (§11 Q20).

### 3.1 Tables read

| table | columns | owner |
|---|---|---|
| `occupants`, `subnets` | `occupant_key, netuid, network_registered_at, status` | 04 |
| `identity_resolved` | `occupant_key, field ∈ {discord, telegram}, status ∈ {present, absent, junk_only, unknown}` | 04 |
| `watch_channels` | `id, occupant_key, netuid, platform, handle, url, source, priority, is_resolved, is_main_guild, guild_id, first_seen_at, last_seen_at, retired_at, meta_json` | 04 (+ 08 operator rows) |
| `social_posts`, `social_post_mentions`, `x_accounts` | X dedupe inputs: `platform, post_id, occupant_key, is_team_account, bot_filtered, text_hash, event_at, observed_at`; mentions with `match_kind='team'` | 09 |
| `metric_values` | `silence_days` raw rows (12-derived) for the `silence_45d` event | 12 |
| `job_state`, `control_state`, `collector_runs`, `events` | watermarks/debounce, operator pause (gateway beat check, B12), gateway heartbeat, dedupe | 01/03 |
| `discord_snapshots`, `telegram_snapshots`, `announcements` | own | 08 |

### 3.2 Tables written (DDL as adopted by 03 §3.5)

```sql
-- class: append. master §6 "discord_snapshots". Owner: 08. Aggregate counts only:
-- usernames, user ids, avatars, the invite `inviter` object and widget members[] are never stored.
CREATE TABLE IF NOT EXISTS discord_snapshots (
  id                   INTEGER PRIMARY KEY,
  occupant_key         TEXT,              -- NULL only for the shared main-vanity sample (one per cycle)
  netuid               INTEGER,           -- denormalised convenience copy
  probe                TEXT    NOT NULL CHECK (probe IN ('invite','widget','census')),
  guild_id             TEXT,
  invite_code          TEXT,              -- case-sensitive; NULL for widget rows and census rows without a code
  observed_at          INTEGER NOT NULL,  -- fetch time (census: classification time)
  event_at             INTEGER,           -- always NULL: counts describe "now" (column kept for PIT uniformity)
  cycle_at             INTEGER NOT NULL,  -- scheduled slot start (6-h slot for invite/census rows; hour for widget rows)
  source               TEXT    NOT NULL DEFAULT 'discord_api',   -- 03 PIT `source`: 'discord_api' (keyless REST) | 'discord_bot'
  http_status          INTEGER,           -- NULL for census rows and transport failures
  member_count         INTEGER,           -- approximate_member_count
  presence_count       INTEGER,           -- approximate_presence_count | widget presence_count
  presence_ratio       REAL,              -- presence_count / member_count; NULL if either missing or member_count = 0
  invite_valid         INTEGER CHECK (invite_valid IN (0,1)),  -- 1 valid · 0 dead (404 JSON, expired by clock, non-guild) · NULL failure / n.a.
  invite_error_code    INTEGER,           -- Discord JSON error code on 404 (10006, 50270, …); widget 403 → 50004
  expires_at           INTEGER,           -- NULL = permanent invite
  landing_channel_id   TEXT,              -- census main_channel rows: the main-server subnet channel id
  landing_channel_type INTEGER,           -- 0 text · 2 voice · 5 announcement · 13 stage · 15 forum
  features_json        TEXT CHECK (features_json IS NULL OR json_valid(features_json)),   -- sorted JSON array (brief: "features JSON")
  boosts               INTEGER,           -- guild.premium_subscription_count
  verification_level   INTEGER,
  guild_created_at     INTEGER,           -- from the guild snowflake: ((id >> 22) + 1420070400000) / 1000
  widget_enabled       INTEGER CHECK (widget_enabled IN (0,1)),   -- widget rows: 1 (200) · 0 (403/50004) · NULL (other)
  topology_class       TEXT CHECK (topology_class IN
                         ('own_server','main_channel','main_vanity','invalid','handle_only','none','unknown')),
  is_primary           INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0,1)),
  unknown_targets      INTEGER NOT NULL DEFAULT 0,   -- O's Discord targets without a successful observation in their window
  source_agreement     TEXT CHECK (source_agreement IN ('agree','disagree','one_dead','single_source')),
  declared_by_json     TEXT CHECK (declared_by_json IS NULL OR json_valid(declared_by_json)),  -- watch_channels.source values
  meta_json            TEXT CHECK (meta_json IS NULL OR json_valid(meta_json)),   -- {guild_name, vanity_url_code, premium_tier, nsfw_level, invite_type, invite_flags,
                                          --  landing_channel_name, profile_visibility, instant_invite_code, channel_count, error}
  run_id               TEXT NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_dsnap_occ_obs   ON discord_snapshots (occupant_key, observed_at);
CREATE INDEX IF NOT EXISTS ix_dsnap_guild_obs ON discord_snapshots (guild_id, probe, observed_at);
CREATE INDEX IF NOT EXISTS ix_dsnap_code_obs  ON discord_snapshots (invite_code, observed_at);
CREATE INDEX IF NOT EXISTS ix_dsnap_cycle     ON discord_snapshots (cycle_at, probe);

-- class: append. master §6 "telegram_snapshots". Owner: 08.
CREATE TABLE IF NOT EXISTS telegram_snapshots (
  id                INTEGER PRIMARY KEY,
  occupant_key      TEXT    NOT NULL,
  netuid            INTEGER,
  handle            TEXT    NOT NULL,     -- public handle (case preserved) or '+hash'
  observed_at       INTEGER NOT NULL,
  event_at          INTEGER,              -- always NULL
  cycle_day_ts      INTEGER NOT NULL,     -- UTC day start of the run
  source            TEXT    NOT NULL DEFAULT 'tme_page',
  http_status       INTEGER,              -- profile page status; NULL on transport failure
  kind              TEXT CHECK (kind IN ('group','channel','private_group','private_channel','bot','user_or_unknown','not_found')),
  title             TEXT,                 -- og:title; community kinds only (NULL for user_or_unknown / bot)
  members           INTEGER,              -- groups: members · channels: subscribers
  online            INTEGER,              -- groups only
  online_ratio      REAL,                 -- online / members
  deprecated_marker INTEGER NOT NULL DEFAULT 0 CHECK (deprecated_marker IN (0,1)),
  preview_available INTEGER CHECK (preview_available IN (0,1)),  -- public channels: 1 = t.me/s rendered ≥ 1 post; 0 = redirect/empty; NULL n.a.
  preview_posts     INTEGER,              -- post blocks parsed this run
  last_post_at      INTEGER,              -- newest <time datetime> in the preview
  parse_ok          INTEGER NOT NULL CHECK (parse_ok IN (0,1)),  -- 0 = HTML changed/unparseable (collection failure)
  error             TEXT,
  run_id            TEXT    NOT NULL,
  UNIQUE (occupant_key, handle, cycle_day_ts)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_tgsnap_occ_obs ON telegram_snapshots (occupant_key, observed_at);

-- class: append (one set-once exception: deleted_at; text NULLed by 03's nlp_text_retention policy). master §6 "announcements":
-- crossposted Discord messages received in the operator's aggregator guild + Telegram public-channel posts. Owner: 08.
CREATE TABLE IF NOT EXISTS announcements (
  msg_id            TEXT    PRIMARY KEY,  -- 'discord:{source_message_id}' | 'telegram:{handle_lower}/{post_no}' (= platform:post_id
                                          --  = spec 10 item id = post_labels.post_id, B11)
  platform          TEXT    NOT NULL CHECK (platform IN ('discord','telegram')),
  occupant_key      TEXT,                 -- set iff exactly one occupant was mapped at insert (all mapped occupants: social_post_mentions)
  netuid            INTEGER,
  source_guild_id   TEXT,                 -- discord: message.reference.guild_id · telegram: NULL
  source_channel_id TEXT    NOT NULL,     -- discord: message.reference.channel_id · telegram: handle_lower
  message_id        TEXT    NOT NULL,     -- discord: message.reference.message_id (original) · telegram: post number
  local_channel_id  TEXT,                 -- aggregator channel the copy arrived in
  local_message_id  TEXT,                 -- id of the crosspost copy (delete events)
  via_own_server    INTEGER NOT NULL DEFAULT 0 CHECK (via_own_server IN (0,1)),  -- 1 = received in the operator's guild (spec 10 privacy gate)
  mapping           TEXT    NOT NULL CHECK (mapping IN ('channel','guild','guild_ambiguous','main_announce','unmapped','tg_handle')),
                                          -- mapping at insert time; later re-mapping only adds social_post_mentions rows
  author_is_team    INTEGER CHECK (author_is_team IN (0,1)),   -- 1 team channel · 0 Foundation/other · NULL unknown (at insert)
  has_attachment    INTEGER NOT NULL DEFAULT 0 CHECK (has_attachment IN (0,1)),
  url_count         INTEGER NOT NULL DEFAULT 0,
  text              TEXT,                 -- discord: ONLY when via_own_server = 1 (ids redacted) · telegram: always written;
                                          -- *_STORE_TEXT=true: 03 nlp_text_retention NULLs it after LLM_TEXT_RETENTION_DAYS (90 d);
                                          -- *_STORE_TEXT=false: pending classification, NULLed once post_labels has the msg_id or
                                          -- text_purge_after passes; hash always kept (amended B10)
  -- text_purge_after INTEGER: 03's column; 08 stamps observed_at + LLM_PENDING_TEXT_MAX_DAYS·86400 when the flag is false, else NULL
  text_hash         TEXT    NOT NULL,     -- sha256(text_norm(...)); empty text → sha256('msg:'||msg_id)
  text_len          INTEGER,
  event_at          INTEGER NOT NULL,     -- discord: crosspost created_at · telegram: <time datetime>
  observed_at       INTEGER NOT NULL,
  deleted_at        INTEGER,              -- set once when the copy is deleted; the row still counts
  meta_json         TEXT CHECK (meta_json IS NULL OR json_valid(meta_json)),   -- {views, embed_count, candidates:[occupant_key…]}
  run_id            TEXT,                 -- gateway: 'gw:{session_start}' · telegram: job run_id
  -- read aliases (virtual generated columns, no storage) for spec 10 §3.1 and 03's raw-row `source` convention
  source            TEXT GENERATED ALWAYS AS (CASE platform WHEN 'discord' THEN 'discord_gateway' ELSE 'tme_preview' END) VIRTUAL,
  guild_id          TEXT GENERATED ALWAYS AS (source_guild_id) VIRTUAL,
  channel_id        TEXT GENERATED ALWAYS AS (source_channel_id) VIRTUAL,
  author_role_flag  TEXT GENERATED ALWAYS AS (CASE author_is_team WHEN 1 THEN 'team' WHEN 0 THEN 'other' END) VIRTUAL,
  url               TEXT GENERATED ALWAYS AS (CASE platform
                      WHEN 'discord' THEN 'https://discord.com/channels/' || source_guild_id || '/' || source_channel_id || '/' || message_id
                      ELSE 'https://t.me/' || source_channel_id || '/' || message_id END) VIRTUAL,
  CHECK (text IS NULL OR via_own_server = 1 OR platform = 'telegram')
) STRICT;
CREATE INDEX IF NOT EXISTS ix_ann_occ_event ON announcements (occupant_key, event_at);
CREATE INDEX IF NOT EXISTS ix_ann_src_event ON announcements (platform, source_channel_id, event_at);
CREATE INDEX IF NOT EXISTS ix_ann_hash      ON announcements (text_hash, event_at);
CREATE INDEX IF NOT EXISTS ix_ann_obs       ON announcements (observed_at);
CREATE INDEX IF NOT EXISTS ix_ann_local     ON announcements (local_message_id);
```

Volumes: `discord_snapshots` ≈ 4 × (≈ 45 invite + ≈ 100 census + 1 main) + ≈ 125 widget ≈ 710 rows/day (≈ 260k/yr, < 80 MB); `telegram_snapshots` ≈ 6/day; `announcements` ≈ 10–50/day. Retention: rows are kept forever (03 §2.7 has no policy for them); only `announcements.text` is NULLed after 90 d (B10).

### 3.3 Other writes

- `watch_channels` (04's table): on `discord_invite` rows after a successful fetch — `guild_id`, `is_main_guild`, `verified_at` (valid only), `meta_json` merge `{last_class, last_error_code, last_checked_at}`; operator rows `platform='discord_channel', source='operator', priority=45` and the per-run `last_seen_at` touch (§2.8); on `telegram` rows — `verified_at` when `parse_ok=1` and the kind is a community kind. 08 never inserts `discord_invite`/`telegram` rows and never retires rows (04 does).
- `social_post_mentions` (09's table; 08 writes only `match_kind='team'` rows for its own platforms): `(platform ∈ {discord, telegram}, post_id = native id (so platform:post_id = announcements.msg_id), occupant_key, match_kind='team', event_at = the announcement's event_at, observed_at, run_id, classifier_version NULL)`; `remap_unmapped` adds rows with `observed_at = now` and never updates `announcements`. 08 never writes `social_posts` (B11).
- `job_state` (01/03): `discord_6h` — `topo:{occupant_key}` → `{class, guild_id, pending_class, pending_guild_id, pending_count, since}`, `csv_mtime`; `discord_gateway` — `wm:{channel_id}`, `follows`, `last_ready_at`, `fatal`, `token_hash`; `telegram_daily` — `tg:{occupant_key}:{handle}` debounce. `job_state` holds runtime watermarks only; operator pauses live only in `control_state` (B12).
- `collector_runs` (01 wrapper): `discord_6h`, `discord_widget_1h`, `telegram_daily` per run; `discord_gateway` writes one heartbeat row per UTC hour itself: `run_id='gw:{hour_ts}'`, `job='discord_gateway'`, `started_at=hour_ts`, `finished_at=hour_ts+3600`, `ok`, `rows`, `error`, `note='heartbeat'` (§2.6).
- `metric_values` (03's table): raw rows of the eight registry keys are written by spec 12 (`features_hourly` / scoring `compute_registered`) calling the §2.9 producers, through 03's `app/storage/pit.py::put_metric_values`; the three evidence keys are written by `discord_6h` itself (`write_evidence`, §2.9). Before Phase 8, `python -m app.collectors.discord --metrics --as-of D --write` writes the eight registry keys through the same helper: `stage='raw'`, `as_of = D 00:00Z` (unix s; official because `as_of % 86400 = 0`, B3), `observed_at = now`, `source='community'`, `spec_version=SPEC_VERSION`, the status enum in `value_json.status` (03 generates the column, B3), `value NULL` for None. It never writes `announcement_cadence_30d`/`silence_days` (12-derived, C3).

### 3.4 metric_keys (spec 12 registry rows this spec produces)

| metric_key | 12 row | role | normaliser (12 params) | w | freshness | producer |
|---|---|---|---|---|---|---|
| `venue_reachable` | 34 | P5 scored | `binary` | .10 | 2 d | 08 fn `app.features.community:compute_venue_reachable` |
| `discord_members` | 35 | P5 scored | `logcap(cap=5000)` | .15 | 2 d | 08 fn |
| `presence_ratio` | 36 | P5 scored | `presence_ratio(low=.05, high=.35, inflate_below=.02, inflate_members=1000)` + `value_json.members` | .10 | 2 d | 08 fn |
| `discord_growth_30d` | 37 | P5 scored | `zclip(lo=-0.1, hi=0.3)` | .10 | 2 d | 08 fn |
| `discord_announcements_30d` | 47 | leg of 38 | `none` | 0 | 2 d | 08 fn |
| `tg_posts_30d` | 48 | leg of 38 | `none` | 0 | 2 d | 08 fn |
| `discord_last_activity_days` | 50 | leg of 41 | `none` | 0 | 2 d | 08 fn |
| `tg_last_activity_days` | 51 | leg of 41 | `none` | 0 | 2 d | 08 fn |
| `announcement_cadence_30d` | 38 | P5 scored | `logcap(cap=12)` | .25 | 1 d | 12 derived (Σ legs 46–48) |
| `silence_days` | 41 | P5 scored | `recency(full_at=7, zero_at=45)` | .15 | 1 d | 12 derived (min legs 49–51) |
| `x_engagement_30d`, `x_followers` | 39, 40 | P5 scored | `rankg`, `logcap(cap=50000)` | .10, .05 | 3 d | 09 fn |

P5 weights: .10 + .15 + .10 + .10 + .25 + .10 + .05 + .15 = 1.00, exactly master §7.3. Evidence-only keys (weight 0, in 03's catalog but not in 12's 62-row registry, C3): `discord_topology`, `tg_members`, `inflated_days`, written by `discord_6h` (§2.9). `metric_values.source` is `community` for every 08 key and `scoring` for the two 12-derived combiners (B9). Retired names, never written: `discord_last_announcement_days`, `tg_last_post_days` (C3). The 2 d freshness is the metric-level window; the 8 d window for weekly re-checked main-server evidence applies inside classification (§2.4, C6).

### 3.5 Events (03 `events`: `occupant_key, netuid, event_type, severity, event_at, observed_at, source='community', payload, dedupe_key, run_id, alerted_at=NULL`)

| event_type | severity | trigger | payload (JSON) | `dedupe_key` |
|---|---|---|---|---|
| `invite_expired` | `info`; `warning` if the code is O's primary invite and no other venue is reachable | stage `expiring`: valid invite with `expires_at ≤ observed_at + DISCORD_INVITE_EXPIRY_WARN_DAYS` (a temporary invite published as identity — hygiene; SN80, SN78 on 2026-09-22) · stage `expired`: 404 `50270` (or expired by clock) on a declared code | `{netuid, occupant_key, invite_code, stage, expires_at, guild_id, last_valid_at, declared_by:[…], other_reachable_venue}` | `{occ}\|{code}\|{stage}` |
| `invite_dead` | `info`; `warning` as above | 404 `10006` on 2 consecutive checks ≥ 20 h apart (a code never seen valid counts from its first check — e.g. subnetalpha `t7BMee4w` for SN106, `taofu` for SN65) | `{netuid, occupant_key, invite_code, error_code, first_dead_at, last_valid_at, guild_id, declared_by, other_reachable_venue}` | `{occ}\|{code}\|{error_code}` |
| `inflated_community` | `warning` | 00Z run: `inflated_days ≥ INFLATION_DAYS` (7) | `{netuid, occupant_key, guild_id, members, presence_median, ratio, days, streak_start, guild_created_at, source_agreement}` | `{occ}\|{guild_id}\|{streak_start}` |
| `community_venue_changed` | `info`; `warning` when moving from {own_server, main_channel} to {invalid, none}, or a Telegram group/channel becomes `not_found`/`user_or_unknown` | Discord: occupant class or primary `guild_id` changed and held `COMMUNITY_VENUE_CHANGE_DEBOUNCE` cycles (transitions to/from `unknown` never emit — a failure is not a venue change; the comparison is against the last known class); Telegram: community kind ↔ non-community kind held 2 days (`parse_ok=0` rows ignored) | `{netuid, occupant_key, platform, old_class, new_class, old_guild_id, new_guild_id, handle, first_seen_new_at, reason}` | `{occ}\|{platform}\|{new_class}\|{new_guild_id or handle}\|{first_seen_new_day}` |
| `silence_45d` | `warning` | 00Z run: latest fresh `silence_days` raw row (12-derived, `status='ok'`) has value ≥ 45 and no episode is open (an episode closes after 7 consecutive days < 45; `absent` rows never emit — P5 already scores them) | `{netuid, occupant_key, silence_days, lower_bound, legs:{x, discord, tg}, last_comm_at, last_comm_platform, min_leg}` (`min_leg` is an allowed extra key) | `{occ}\|{episode_start_date}` |

`event_at` (INTEGER unix s): invite events = `observed_at` of the triggering fetch (`expired` uses `expires_at` when known); `inflated_community` = `streak_start` 00:00Z; `community_venue_changed` = `first_seen_new_at`; `silence_45d` = `observed_at` of the triggering run. Types, severities, required payload keys and dedupe patterns equal 03 §3.9 (D1/D2); the draft names `invite_expiring`, `guild_changed`, `community_inflation_suspected`, `tg_handle_gone` are retired (D1). Day placeholders are UTC day numbers `ts // 86400` (03 §3.9's `day` convention): `first_seen_new_day` of `first_seen_new_at`; `episode_start_date` of the episode's first ≥ 45 row. **`silence_45d` payload (D2):** `silence_days`, `lower_bound`, `min_leg` and `legs` come from 12's triggering `silence_days` row (`value_json.legs`, value or status per leg); `last_comm_at` = the newest team communication with `observed_at ≤ cycle_at` across X (`social_posts` with `is_team_account=1`, `bot_filtered=0`, `occupant_key=O`), Discord and Telegram (`announcements` reached through O's `match_kind='team'` mentions), as INTEGER unix s, or NULL if none was ever observed; `last_comm_platform ∈ {x, discord, tg}` names that post's platform and is NULL together with it.

---

## 4. Absence-vs-failure table

**Rule enforced in code:** a feature function first decides *absence* from census/reference rows, then *failure* from freshness windows and gateway heartbeats, and only then computes. No 0.0-meaning value is ever synthesised from an `except` branch; a failed fetch writes a row with `invite_valid NULL` / `parse_ok=0`, which downstream reads as "unknown".

### 4.1 Raw observations

| observation | row written | downstream meaning |
|---|---|---|
| invite 200, guild invite, not expired | `invite_valid=1` + counts | valid venue |
| invite 200 but `expires_at ≤ now` | `invite_valid=0`, `invite_error_code=50270`, `meta.error='expired_by_clock'` | verifiably expired |
| invite 404 JSON `10006` | `invite_valid=0`, `10006` | verifiably dead (revoked / never existed) |
| invite 404 JSON `50270` | `invite_valid=0`, `50270` | verifiably expired |
| invite 404 JSON, other code | `invite_valid=0`, code stored | dead |
| invite 200 without `guild` (group DM / friend invite) | `invite_valid=0`, `meta.error='not_guild_invite'` | not a community venue |
| invite 404 HTML, 401, 403, 429 after retries, 5xx, timeout, `InvalidRequestGuard` refusal | `invite_valid NULL`, `http_status`, `meta.error` | **failure**: target `unknown` unless an older successful observation is inside its freshness window |
| main vanity failure | main row `invite_valid NULL` | `main_alive=false` → occupants relying on main-server channel ids become `unknown` (never `invalid`) |
| widget 200 | `widget_enabled=1`, `presence_count` | diagnostic only |
| widget 403 `50004` | `widget_enabled=0` | verifiably disabled; no metric effect |
| widget 404 `10004` / failure | `widget_enabled NULL`, `meta.error` | none |
| `t.me` 200 → group / channel / private | `kind`, `members`, `online` | Telegram venue |
| `t.me` 200 → `user_or_unknown` / `bot`; 404, or no title with Telegram's not-found markers → `not_found` | `kind` set, `parse_ok=1` | verifiably **not** a community venue |
| `t.me` 200 with no recognisable anchor | `parse_ok=0` | **failure** (layout change) |
| `t.me` 5xx / timeout / 429 after retries | `http_status` (or NULL), `parse_ok=0` | failure |
| `t.me/s` 301/302 or zero post blocks | `preview_available=0` | no post feed — Telegram leg `None` (not "zero posts") |
| gateway disabled / no token / fatal | no `ok=1` heartbeat rows | Discord legs `None` (n/a while no operator `kind=announce` row exists) |
| target declared only by `legacy` | none (ignored, §2.2) | not a venue claim |
| gateway heartbeat `ok=1` within 2 d, followed channel, no posts in window | — | Discord leg count `0` |
| gateway all heartbeats `ok=0` for 2 d | `ok=0` rows | Discord legs `None` |
| `identity_resolved(O, discord\|telegram).status='unknown'` | census `topology_class='unknown'` | `None` unless positive evidence exists elsewhere |
| identity `absent`/`junk_only`, no targets | census `topology_class='none'` | verifiable absence |

### 4.2 Metrics

Statuses are 12's (§2.3): **absent → 0.0**, `ok` → normaliser(value), `failed` / `not_applicable` → **None**. The producer returns `MetricResult(0.0, …, absence_reason=…)` for the first column and `MetricResult(None, …)` (with `value_json.status='not_applicable'` where marked n/a) for the second.

| metric_key | **absent (→ 0.0) — verifiable absence** | **None — failed / n/a** | `ok` value |
|---|---|---|---|
| `venue_reachable` | `absence_reason='no_reachable_venue'`: all targets freshly and successfully observed (`unknown_targets = 0`), none reachable: class ∈ {invalid, handle_only, none} (+ `main_vanity` once `DISCORD_CHANNEL_MAP_REVIEWED=true`) and every Telegram target `user_or_unknown`/`bot`/`not_found`/deprecated; or `absence_reason='no_venue'` for master's "no venue in any source" (class `none`, no Telegram target, `identity_resolved(O,'telegram').status ∈ {absent, junk_only}`) | failed: class `unknown` or no census row in 2 d; `main_vanity` before the map review; a Telegram target failed and nothing is reachable | `1.0` |
| `discord_members` | — (master: "other metrics None" when there is no own server) | n/a: known class ≠ `own_server`; failed: primary invite not observed OK in 2 d, or class unknown | members |
| `presence_ratio` | — (the inflation rule's normalised `0.0` is a *measurement* of an `ok` row, not absence) | n/a: class ≠ `own_server`; failed: < 2 samples in the 24 h window | `median(presence)/median(members)` |
| `discord_growth_30d` | — | n/a: class ≠ `own_server`; failed: no base sample within 30 ± 3 d, primary guild changed, `m_then = 0` | growth (may be negative) |
| `discord_announcements_30d` | — (a real zero is `ok` 0: O has a followed `kind='announce'` channel, or ≥ 1 guild-mapped announcement ever, **and** a `collector_runs(job='discord_gateway', ok=1)` heartbeat with `finished_at ∈ (as_of−2 d, as_of]` exists, no team posts in 30 d) | n/a: no followed channel for O (as 12 row 47); failed: gateway disabled/stale | count |
| `tg_posts_30d` | — (fresh preview `preview_available=1` with no posts in 30 d → `ok` 0) | n/a: no public channel / preview unavailable; failed: snapshot stale or `parse_ok=0` | count |
| `discord_last_activity_days` / `tg_last_activity_days` | — (no posts ever → `ok` lower bound, not absence) | n/a: no followed channel / no public channel with preview; failed: gateway heartbeat / preview not fresh within 2 d | days (`value_json.lower_bound`) |
| `announcement_cadence_30d`, `silence_days` | derived by spec 12 §2.6 (§2.9): cadence has no absent case (no `ok` leg → None); `silence_days` absent iff no `ok` leg ∧ `venue_reachable` raw 0 ∧ X leg absent (C3: no venue reachable **and** no X account) | no `ok` leg otherwise | Σ legs / min legs |

Attribution is point-in-time: producers attribute announcements to occupants **only through `social_post_mentions(match_kind='team')` rows with `observed_at ≤ as_of`** (`announcements.occupant_key`, `mapping` and `author_is_team` record the state at insert and are never updated). A late re-mapping (§7) therefore never rewrites what an earlier `as_of` could see.

---

## 5. Configuration

All keys live on `app.config.Settings`; read dynamically via `app_config.settings.<KEY>` (01 dynamic-settings rule). Flags and cadence tiers are re-read at the start of every run; the gateway re-reads them every 30 s beat.

### 5.1 Keys already in 01's inventory (group "Community (spec 08)"; 08 owns them, F1 — defaults identical)

| key | type | default | use here |
|---|---|---|---|
| `DISCORD_ENABLED` | bool | `True` | gates `discord_6h` |
| `DISCORD_INTERVAL_MIN` | int | `360` | liveness hint only (the trigger is cron, §6) |
| `DISCORD_WIDGET_ENABLED` | bool | `True` | gates `discord_widget_1h` |
| `DISCORD_WIDGET_INTERVAL_MIN` | int | `60` | widget interval |
| `DISCORD_RATE_PER_SEC` | float | `1.0` | `discord` bucket (burst 1, hourly cap 200) |
| `DISCORD_BOT_TOKEN` | str (secret) | `""` | gateway login; `Authorization: Bot` on invite calls |
| `DISCORD_AGGREGATOR_GUILD_ID` | str | `""` | the only guild the Bot reads |
| `DISCORD_GATEWAY_ENABLED` | bool | `False` | gates `discord_gateway`; 01's validator forces `False` unless token **and** guild id are set |
| `DISCORD_CHANNELS_SEED_PATH` | path | `"seed/discord_channels.csv"` | channel map (§2.8) |
| `TELEGRAM_COUNTS_ENABLED` | bool | `True` | gates `telegram_daily` |
| `TELEGRAM_COUNTS_INTERVAL_MIN` | int | `1440` | Telegram interval |
| `TELEGRAM_COUNTS_RATE_PER_SEC` | float | `0.5` | `tme` bucket |

**Keys of other owners that this spec reads** (their owner's default wins, F1):

| key | owner | type | default | use here |
|---|---|---|---|---|
| `DISCORD_MAIN_GUILD_ID` | 04 | str | `"799672011265015819"` | main-server guild id (topology, widget skip) |
| `REGISTRY_WATCH_RETIRE_DAYS` | 04 | int | `14` | operator CSV rows retire 14 d after their last touch (§2.8) |
| `HTTP_USER_AGENT` | 01 | str | `"TaoSentinel/0.1 (+https://github.com/<operator>/taosentinel)"` | `t.me` requests |
| `HTTP_TIMEOUT_SEC` | 01 | float | `20.0` | every request |
| `HTTP_MAX_RETRIES` | 01 | int | `2` | 5xx/timeout retries |
| `JOB_FAIL_ALERT_THRESHOLD` | 01 | int | `5` | `collector_failing` after 5 consecutive failed runs |
| `KILL_SWITCH_PATH` | 01/16 | path | `"./KILL_SWITCH"` | does **not** pause any 08 component (all free, B12) |
| `PAUSE_ALL_PATH` | 16 | path | `"./PAUSE_ALL"` | pauses all four components (B12, 16 §2.11) |
| `SPEC_VERSION` | 01 | str | `"ts-1.0"` | `metric_values.spec_version`; freezes §2.9's constants |
| `API_AUTH_TOKEN` | 01 | str | `""` | control API calls in §2.8 step 4 and §10 |
| `LLM_TEXT_RETENTION_DAYS` | 10 (applied by 03) | int | `90` | `announcements.text` → NULL (B10) |

### 5.2 New keys (this spec; append to 01 §5 "Community (spec 08)")

| key | type | default | validator / semantics |
|---|---|---|---|
| `DISCORD_CRON_HOURS` | str | `"0,6,12,18"` | UTC hours of `discord_6h`; **pinned by E3**: the validator rejects any other value (samples and evidence rows must land on the scoring slots, B4), like `SCORING_OFFICIAL_HOUR`. The key exists so 01 builds the `CronTrigger` from Settings as for `scoring_6h` |
| `DISCORD_CRON_MINUTE` | int | `0` | pinned by E3 (validator rejects ≠ 0) |
| `DISCORD_API_BASE` | str | `"https://discord.com/api/v10"` | must start `https://discord.com/api/v` |
| `DISCORD_USER_AGENT` | str | `"DiscordBot (https://github.com/<operator>/taosentinel, 0.1.0)"` | sent on every discord.com call; Discord requires the `DiscordBot (url, version)` form for Bot-token calls; warn at boot if the token is set and the value does not start with `DiscordBot (` |
| `DISCORD_INVITES_USE_BOT_TOKEN` | bool | `True` | attach `Authorization: Bot …` to invite calls when the token is set |
| `DISCORD_MAIN_VANITY_CODE` | str | `"bittensor"` | the shared main-server invite |
| `DISCORD_MAIN_ANNOUNCE_CHANNEL_ID` | str | `"830075335084474390"` | `^\d{17,20}$`; `📣・announce` |
| `DISCORD_MAIN_CHANNEL_RECHECK_DAYS` | int | `7` | 1–30 |
| `DISCORD_DEAD_WEEKLY_AFTER_DAYS` | int | `30` | 7–365; dead codes daily before, weekly after |
| `DISCORD_INVITE_EXPIRY_WARN_DAYS` | int | `7` | 1–30 (`invite_expired` stage `expiring`) |
| `DISCORD_INVALID_GUARD_PER_10MIN` | int | `100` | 1–1000 (Discord bans at 10,000) |
| `DISCORD_WIDGET_DISABLED_RECHECK_DAYS` | int | `7` | 1–90 |
| `DISCORD_WIDGET_SKIP_GUILD_IDS` | str | `"799672011265015819"` | comma list of snowflakes never widget-probed |
| `DISCORD_CHANNEL_MAP_REVIEWED` | bool | `False` | operator sets `True` after checklist B5 step 7; until then `main_vanity`-only → `venue_reachable None` |
| `DISCORD_PRESENCE_MIN_SAMPLES` | int | `2` | 1–4 (samples per 24 h for `presence_ratio`) |
| `DISCORD_AGGREGATOR_CHANNEL_IDS` | str | `""` | comma list; empty = every readable text/announcement channel of the aggregator guild |
| `DISCORD_GATEWAY_ENUMERATE_FOLLOWS` | bool | `True` | list follower webhooks when *Manage Webhooks* is granted |
| `DISCORD_BACKFILL_DAYS` | int | `14` | 1–90; first-run lookback only |
| `DISCORD_BACKFILL_OVERLAP_SEC` | int | `600` | 0–3600 (port of whale `OVERLAP_SEC`) |
| `DISCORD_BACKFILL_MAX_PER_CHANNEL` | int | `2000` | 100–10000 (port of `MAX_PAGES_PER_POLL` guard) |
| `DISCORD_TEXT_MAX_CHARS` | int | `4000` | 500–8000 |
| `TELEGRAM_BASE_URL` | str | `"https://t.me"` | |
| `TELEGRAM_PREVIEW_ENABLED` | bool | `True` | `t.me/s/{h}` post previews for public channels |
| `TELEGRAM_POSTS_STORE_TEXT` | bool | `False` | B10/F2 (amended): `False` = text kept only as pending classification (`text_purge_after` = `observed_at` + `LLM_PENDING_TEXT_MAX_DAYS`·86400, NULLed once labelled or at expiry; hash kept); `True` keeps the text for 90 d |
| `DISCORD_STORE_TEXT` | bool | `True` | B10/F2: store own-server crosspost text (ids redacted, `via_own_server=1` only) for 90 d, then hash only; `False` = pending-classification text only (`text_purge_after` stamped, NULLed once labelled or after `LLM_PENDING_TEXT_MAX_DAYS`; amended B10; operator decision, master §13 item 8) |
| `COMMUNITY_EVENTS_ENABLED` | bool | `True` | emit §3.5 events |
| `COMMUNITY_VENUE_CHANGE_DEBOUNCE` | int | `2` | 1–8 cycles/days |

No community-specific sentinel key: the former `COMMUNITY_KILL_SWITCH_PATH` (`./COMMUNITY_PAUSED`) is withdrawn because B12 allows only `KILL_SWITCH`, `PAUSE_ALL`, `SCORING_KILLED` and `data/control/<target>.paused` as files (§5.3).

Scoring thresholds are **not** config (01 §5 rule): the inflation thresholds and presence band are 12's `presence_ratio` registry params; this spec's `XPLATFORM_DEDUPE_HOURS` (48), `GROWTH_BASE_TOLERANCE_DAYS` (3) and `INFLATION_DAYS` (7) are frozen per `SPEC_VERSION` in `app/scoring/spec.py`.

### 5.3 Kill-switch and pause semantics

- All four components are free (`paid=False`; the gateway task declares `paid = False` too): `KILL_SWITCH` does **not** pause them, and free LongTasks keep running (B12, 16 §2.11).
- `PAUSE_ALL` pauses all four (none is `ops_exempt`; 16 §2.11 covers jobs **and** LongTasks): the jobs skip with `paused:pause_all`, the gateway supervisor closes the client and idles within one 30-s beat.
- Operator pause (B12): a `control_state(target, key='paused')` row is the only persistent pause, written by `POST /api/v1/control/pause/{target}`, Telegram `/pause` or the CLI. Emergency input without API or DB: the file `data/control/<target>.paused` (the app reads it and never writes it). A target is an exact job id or a group name from 16's `PAUSE_GROUPS`, and there is no provider or prefix matching (02 §B12): `discord` → `discord_6h`, `discord_widget_1h`, `discord_gateway` (16's Discord-ban runbook uses `data/control/discord.paused`); `telegram_counts` (or the exact id `telegram_daily`) → `telegram_daily`; `community` → all four. `tme` is only a provider id, not a pause target. `telegram` is not a target either, and no group can reach 14's `telegram_bot`. The three jobs use 01's `paused_reason()` unchanged (skip, no `collector_runs` row, so nothing downstream reads a pause as absence); the gateway (16 §2.5 amendment 2: `LongTask.paused_reason()`) checks the same resolution every 30-s beat and disconnects within one beat. `DISCORD_GATEWAY_ENABLED=false` + `POST /api/v1/control/reload-config` stays the config-level off switch.
- Any paused/disabled component simply stops writing rows; after the 2-day freshness window its metrics become `None` (lower confidence), never `0.0`.

---

## 6. Cadence, rate limits, cost

**Job and LongTask registry — the complete list for this spec.** 01's `JOB_TABLE` mirrors this table verbatim (A2, E2); there are no other 08 jobs (the CSV import runs inside `discord_6h`, §8.3 step 2; `remap_unmapped` inside the gateway; the `python -m app.collectors.discord …` commands are CLI only).

| id | factory (`app.…`) | trigger | UTC time / interval | misfire grace s | timeout s | enabled flag | phase | provider | paid | liveness max age |
|---|---|---|---|---|---|---|---|---|---|---|
| `discord_6h` | `collectors.discord:DiscordInvitesJob` | cron (UTC), `min_gap_factor=0`, no first-run stagger | **00:00, 06:00, 12:00, 18:00Z** (E3; `DISCORD_CRON_HOURS="0,6,12,18"`, `DISCORD_CRON_MINUTE=0`, both pinned) | 1800 | 1800 | `DISCORD_ENABLED` | 6 | `discord` | no | 45,120 s (01 formula with `DISCORD_INTERVAL_MIN`=360: 2 × 21,600 + 1,800 + 120) |
| `discord_widget_1h` | `collectors.discord:DiscordWidgetJob` | interval, 01 first-run stagger | every 60 min (`DISCORD_WIDGET_INTERVAL_MIN`); no fixed minute | 600 | 600 | `DISCORD_WIDGET_ENABLED` | 6 | `discord` | no | 7,920 s (01 formula; 16 §2.5, E4) |
| `telegram_daily` | `collectors.telegram:TelegramCountsJob` | interval, 01 first-run stagger | every 1440 min (`TELEGRAM_COUNTS_INTERVAL_MIN`); no fixed time (one row per UTC day, first attempt wins) | 3600 | 1800 | `TELEGRAM_COUNTS_ENABLED` | 6 | `tme` | no | 174,720 s (01 formula) |
| `discord_gateway` | `collectors.discord_gateway:DiscordGatewayTask` | **LongTask** (E1) | continuous; `beat()` every 30 s; reconnect backoff 5 → 300 s; hourly `collector_runs` heartbeat | — | — | `DISCORD_GATEWAY_ENABLED` | 6 | — (own websocket; not an `Http` provider) | no | 600 s (`max(last_beat, last_attempt)`, 16 §2.5 amendment 5) |

The cron trigger is what makes the four presence samples land at the same UTC hours for every guild (cross-sectional comparability); `discord_6h` runs at :00 and finishes in ≈ 1 min, well before the :15 scoring runs (E3), so its `as_of = cycle_at` evidence rows are visible to them. None of the four is `ops_exempt` (§5.3).

**Request budget (steady state, 2026-09-22 topology: 25 own servers, 44 distinct codes, 5 widgets enabled, 7 Telegram links):**

| component | requests/day | bucket | invalid (401/403/429) per day |
|---|---|---|---|
| own-server invites (4 cycles) | ≈ 100 | `discord` | 0 |
| main vanity (1/cycle) | 4 | `discord` | 0 |
| main-channel invites (weekly; ≈ 5 codes — the other ≈ 18 main-channel occupants are chain deep links, never fetched) | < 1 | `discord` | 0 |
| dead codes (daily → weekly) | ≤ 9 | `discord` | 0 (404s do not count) |
| widget, enabled guilds (hourly) | ≈ 120 | `discord` | 0 |
| widget, disabled guilds (weekly probe) | ≈ 3 | `discord` | ≈ 3 |
| `t.me` profiles + `t.me/s` previews | ≈ 7–14 | `tme` | — |
| gateway (websocket + history on `on_ready`) | ≈ 0–20 REST | discord.py (token-scoped) | 0 |
| **total** | **≈ 250 (≈ 0.003 req/s)** | | **≈ 3 ≪ 10,000 / 10 min** |

Busiest hour (00Z): ≈ 30 own-server codes + 1 vanity + ≤ 9 dead + ≤ 5 main-channel (only if all due at once) + ≈ 6 widget ≈ 51 requests; the first-ever hour adds ≈ 27 widget first probes (≈ 22 of them 403s) ≈ 78 — all < the 200/h `discord` cap and ≪ 10,000 invalid/10 min. No request is ever issued faster than 1 s apart per provider; nothing polls faster than the 5-min Cloudflare edge cache. The gateway uses one websocket (heartbeat ≈ 41 s), ≤ a handful of session starts per day (limit 1,000), and REST history only on `on_ready`.

This spec makes **no TMC calls** (the shared process-wide TMC bucket ≤ 8 req/min, master §8/spec 05, is untouched) and no chain RPC calls; its only buckets are `discord` and `tme` (01 §6).

**Cost:** $0/month (no paid provider). discord.py + aiohttp add an estimated 20–40 MB RSS (UNVERIFIED) inside the 300 MB process target. Storage ≈ 80 MB/year (§3.2).

---

## 7. Failure modes and fail-safe behaviour

| failure | detection | behaviour | `/health` | alert |
|---|---|---|---|---|
| Discord API down (5xx, timeouts) | `Http` retries exhausted | per-target failure rows (`invite_valid NULL`); if **zero** targets succeeded and ≥ 1 was attempted, `collect()` raises `ProviderError("discord: all N targets failed")` → run `ok=0` | `collectors.discord_6h.last_error`, `consecutive_failures` | 01 wrapper at 5 consecutive (≈ 30 h of cron runs) |
| Cloudflare IP ban (non-JSON 429, e.g. error 1015 — body shape UNVERIFIED) | body is not JSON on 429 | `limiter.observe('discord', {"Retry-After": "3600"})`, write the rows gathered so far, abort run with `CloudflareBan` (failure) | `rate_limits.discord` (01 `snapshot()`) | via failure count |
| Invalid-request guard reached | `InvalidRequestGuard.count ≥ 100` | refuse further `discord` requests for the rest of the 10-min window; remaining targets = failures | `collectors.discord_6h.invalid_10min` | log `warning` |
| Malformed JSON / unexpected shape | `parse_invite` raises `ValueError` | that target = failure; run continues | `last_error` sample | — |
| DB locked / write error | exception inside `collect()` | run fails (wrapper); next cycle re-fetches; planner idempotence prevents duplicate slot rows | wrapper fields | at 5 |
| Power loss / laptop off at a cron slot | APScheduler misfire (`misfire_grace_time=1800`, `coalesce=True`) | slot run late (≤ 30 min) or skipped; `presence_ratio` needs only 2 of 4 samples; growth base search tolerates ±3 d; census gaps > 2 d → metrics `None` | `next_run` | — |
| CSV missing / malformed rows | `load_channel_map` | logged no-op / rows skipped (`collector.py:59-75` idiom); previous `watch_channels` operator rows persist until 04 retires them after 14 d | `csv_errors`, `csv_rows`, `csv_mtime` | — |
| CSV row for a netuid that was re-occupied | an operator row with the same `channel_id` exists for a different (deregistered) occupant | no row is created for the new occupant; `csv_row_stale_occupant` logged until the operator re-confirms by adding `reg=<network_registered_at>` to the row's `notes` (§2.8) | `csv_stale_rows` | — (`/health` and `--report topology` only; no event type exists) |
| Crosspost from an unmapped source channel | `mapping='unmapped'` | stored with `occupant_key NULL`, counts for nobody; on the next channel-map reload `remap_unmapped(db)` adds `social_post_mentions(match_kind='team')` rows with `observed_at = now` — the `announcements` row itself is never updated (append class); earlier `as_of`s stay unchanged (attribution is PIT via mentions) | `tasks.discord_gateway.unmapped_channels` | — |
| Bot token revoked / wrong | `discord.LoginFailure` | fatal: idle until token hash changes on reload (persisted across restarts) | `tasks.discord_gateway.fatal_error` | `discord_gateway_fatal` once; `collector_recovered` (collector `discord_gateway`) after the next good `on_ready` |
| Message Content intent disabled in portal | `discord.PrivilegedIntentsRequired` | fatal idle | idem | idem |
| Bot not in / removed from the aggregator guild | `on_ready` check, `on_guild_remove` | idle, `last_error='bot_not_in_aggregator_guild'`; heartbeats `ok=0` → Discord legs `None` after 2 d | `connected=false` | via heartbeat failures (log) |
| Bot added to a foreign guild | `on_ready` / `on_guild_join` | events from it ignored; never read | `foreign_guilds` | — (`warning` log once; §11 Q25) |
| Discord gateway outage / network loss | discord.py reconnect; supervisor backoff 5 → 300 s | on the new session `on_ready` backfills from the watermark (cap 2,000/channel; the watermark advances, so a capped backfill continues on the next `on_ready`) | `reconnects`, `connected` | `degraded_reasons: task_disconnected:discord_gateway` (01) |
| A Follow is removed / source channel deleted or converted to text / source guild deleted | follower webhook disappears (`follows` diff, needs *Manage Webhooks*) | crossposts stop; the Discord leg reports `0` (indistinguishable from team silence — the honest reading); `follows_lost` listed | `follows_lost` | — |
| Source message deleted after publish | `on_raw_message_delete` on the copy / `SOURCE_MESSAGE_DELETED` flag | `deleted_at` set; the row still counts (it was communicated) | — | — |
| Telegram layout change | every 200 page `parse_ok=0` | job raises `TmeLayoutChanged` → run `ok=0`; rows kept with `parse_ok=0` → Telegram contributions `None` | `collectors.telegram_daily.last_error` | at 5 consecutive (daily job ⇒ 5 days) |
| `t.me` throttling (429) | `Http` | penalize + retry; then per-handle failure | `rate_limits.tme` | — |
| Lookalike / template guild (SN55 chain invite → "NIOME" 1922/9, created 8 min after Tag101's) | `source_agreement='disagree'` on an inflated guild | primary stays chain-resolved (04 precedence) → `inflated_community` may fire; payload carries `source_agreement` and `guild_created_at`; operator suppresses the chain code via 04 `manual_overrides(mode='suppress')` → next cycle primary switches → `community_venue_changed` | per-occupant rows | digest |
| Announcement spam in a followed channel | — | `logcap(x, 12)` saturates at 12; spec 10 labels spam | — | — |

**Last-good semantics:** the collector caches nothing between runs except `job_state` watermarks/debounce; evidence reuse is limited to rows inside their freshness window (§2.4), which is observation, not forward-fill.

---

## 8. Implementation plan

### 8.1 Files

```
app/collectors/discord.py            DiscordInvitesJob, DiscordWidgetJob (01 factories), cycle orchestration, CLI (__main__)
app/collectors/discord_api.py        InviteResult, WidgetResult, parse_invite(), parse_widget(), fetch_invite(), fetch_widget(),
                                     discord_headers(), snowflake_to_datetime(), InvalidRequestGuard (process singleton), CloudflareBan
app/collectors/discord_topology.py   Target, TargetState, TopologyDecision, is_due(), main_channel_match(), classify_occupant(),
                                     pick_primary(), source_agreement(), TOPOLOGY_RANK
app/collectors/discord_channels.py   ChannelMapRow, ChannelMap, load_channel_map(), sync_channel_map(), remap_unmapped(), export_known_channels()
app/collectors/discord_gateway.py    DiscordGatewayTask(LongTask), GatewayCfg, AnnouncementRow, classify_message(), compose_text(), redact_ids(), discord_clean()
app/collectors/telegram.py           TelegramCountsJob (01 factory), TmeProfile, TmePost, parse_count(), parse_tme_profile(),
                                     parse_tme_preview(), TmeLayoutChanged
app/collectors/community_store.py    all SQL for the three raw tables, mentions, heartbeats, task state
app/collectors/community_events.py   invite / venue / inflation / silence detectors
app/nlp/textnorm.py                  text_norm(), text_hash() — the single text normaliser/hash, defined here (B11); 09 and 10 import it
app/features/community.py            the 8 producer fns + 3 evidence fns of §2.9 (12's MetricResult), topology_at(), inflated_days()
app/features/normalise.py            (+) presence_ratio() — the §2.9 shape, added ahead of 12 (12 owns the module from Phase 8)
app/scoring/spec.py                  (+) XPLATFORM_DEDUPE_HOURS, GROWTH_BASE_TOLERANCE_DAYS, INFLATION_DAYS (§2.9; created by 08,
                                     12 owns the file and re-exports REGIME_TS_1_0 from app/quant/regime.py)
seed/discord_channels.csv            header + rows produced by `--export-csv`; operator-maintained afterwards (gitignored)
tests/collectors/community/          conftest.py fakes_discord.py test_discord_api.py test_discord_topology.py test_discord_channels.py
                                     test_discord_jobs.py test_discord_gateway.py test_telegram.py test_community_events.py
                                     test_privacy_community.py   (as built: one package, like 06/07)
tests/features/test_community.py     tests/nlp/test_textnorm.py
tests/fixtures/community/*.json *.html   (synthetic bodies shaped like the live 2026-10-17 responses)
```

`pyproject.toml` (01 §2.3) gains `"discord.py>=2.4,<3.0"` under this spec (pulls `aiohttp`; as built `discord.py==2.7.1`, already in `requirements.lock` since Phase 0; API names `MessageFlags.is_crossposted` (bit 1<<1), `MessageType.channel_follow_add` (12), `WebhookType.channel_follower` (2), `MessageReference.{guild,channel,message}_id`, `history(after=, limit=, oldest_first=)` re-checked against 2.7.1). No other dependency: HTML parsing uses stdlib `html.parser`; no `bittensor`/`bittensor-wallet` (master §1.2).

### 8.2 Signatures

```python
# app/collectors/discord_api.py
@dataclass(slots=True, frozen=True)
class InviteResult:
    code: str; fetched_at: int; http_status: int | None                 # unix s (as built; B1)
    invite_valid: int | None; invite_error_code: int | None; guild_id: str | None
    member_count: int | None; presence_count: int | None; presence_ratio: float | None
    expires_at: int | None; landing_channel_id: str | None; landing_channel_type: int | None   # unix s
    features: tuple[str, ...]; boosts: int | None; verification_level: int | None
    guild_created_at: datetime | None                  # tz-aware UTC; stored as INTEGER unix s
    meta: dict; error: str | None

@dataclass(slots=True, frozen=True)
class WidgetResult:
    guild_id: str; fetched_at: datetime; http_status: int | None; widget_enabled: int | None
    presence_count: int | None; instant_invite_code: str | None; channel_count: int | None; error: str | None

class CloudflareBan(ProviderError): ...
def snowflake_to_datetime(snowflake: str | int) -> datetime                 # ((id >> 22) + 1420070400000) ms, UTC
def discord_headers(settings, *, with_bot_token: bool) -> dict[str, str]    # UA, Accept, optional Authorization
def parse_invite(code: str, status: int | None, body: bytes | None, fetched_at: datetime) -> InviteResult   # drops inviter
def parse_widget(guild_id: str, status: int | None, body: bytes | None, fetched_at: datetime) -> WidgetResult  # deletes members[]
class InvalidRequestGuard:
    def __init__(self, limit_per_10min: int, clock: Callable[[], float] = time.monotonic) -> None
    def record(self, status: int) -> None            # counts 401/403/429 only
    def allowed(self) -> bool
    def count(self) -> int
def get_guard() -> InvalidRequestGuard               # process-wide singleton shared by invite + widget jobs
async def fetch_invite(http: Http, code: str, settings) -> InviteResult     # never raises except CloudflareBan
async def fetch_widget(http: Http, guild_id: str, settings) -> WidgetResult

# app/collectors/discord_topology.py
TOPOLOGY_RANK = {"own_server": 5, "main_channel": 4, "main_vanity": 3, "invalid": 2, "handle_only": 1, "none": 0}
@dataclass(slots=True)
class Target:          # one watch_channels row (discord_invite | discord_channel | discord_handle)
    occupant_key: str; netuid: int; platform: str; handle: str; source: str
    is_resolved: bool; guild_id: str | None; meta: dict
@dataclass(slots=True)
class TargetState:     # stored history of one invite code
    code: str; last_attempt: InviteResult | None; last_ok: InviteResult | None; dead_since: datetime | None
@dataclass(slots=True)
class TopologyDecision:
    occupant_key: str; netuid: int; topology_class: str
    primary_code: str | None; primary_guild_id: str | None; main_channel_id: str | None
    source_agreement: str | None; declared_by: dict[str, list[str]]; unknown_targets: list[str]
def is_due(state: TargetState | None, *, code: str, now: datetime, cycle_at: datetime, cfg: "DiscordCfg") -> bool
def main_channel_match(res: InviteResult, netuid: int, channel_ids: set[str]) -> bool
def classify_occupant(netuid: int, occupant_key: str, targets: list[Target], evidence: dict[str, InviteResult],
                      *, main_alive: bool, identity_status: str | None, prev_primary_guild: str | None,
                      cfg: "DiscordCfg") -> TopologyDecision      # evidence is already window-filtered
def pick_primary(own: list[tuple[Target, InviteResult]], prev_primary_guild: str | None) -> tuple[Target, InviteResult]
def source_agreement(targets: list[Target], evidence: dict[str, InviteResult], vanity_code: str) -> str | None

# app/collectors/discord_channels.py
@dataclass(slots=True, frozen=True)
class ChannelMapRow:
    netuid: int | None; channel_id: str; channel_name: str; guild_id: str
    kind: Literal["announce", "subnet_text"]; registered_at: int | None; notes: str   # registered_at parsed from a notes token reg=<n>
class ChannelMap:
    def occupants_for_channel(self, channel_id: str) -> list[str]          # kind='announce' rows
    def is_ecosystem(self, channel_id: str) -> bool
    @classmethod
    async def build(cls, db, rows: list[ChannelMapRow], main_announce_id: str) -> "ChannelMap"
def load_channel_map(path: Path) -> tuple[list[ChannelMapRow], list[str]]      # never raises
async def sync_channel_map(db, rows: list[ChannelMapRow], now: datetime, run_id: str) -> dict   # {upserted, stale, skipped}
async def remap_unmapped(db, cmap: ChannelMap, now: datetime) -> int
async def export_known_channels(db, path: Path) -> int                          # appends known main-server channel ids

# app/collectors/discord.py
class DiscordInvitesJob(CollectorJob):
    id = "discord_6h"; name = "Discord invites"; phase = 6; enabled_flag = "DISCORD_ENABLED"
    interval_flag = None; cron = None          # built from DISCORD_CRON_HOURS / DISCORD_CRON_MINUTE at registration
    provider = "discord"; paid = False; timeout_sec = 1800; min_gap_factor = 0.0; liveness_max_age_sec = 45120
    # no paused_reason() override in any 08 job: 01's wrapper resolves control_state, data/control/*.paused, PAUSE_ALL (B12)
    async def collect(self, run: RunContext) -> JobOutcome
class DiscordWidgetJob(CollectorJob):
    id = "discord_widget_1h"; name = "Discord widgets"; phase = 6; enabled_flag = "DISCORD_WIDGET_ENABLED"
    interval_flag = "DISCORD_WIDGET_INTERVAL_MIN"; provider = "discord"; paid = False; timeout_sec = 600
    async def collect(self, run: RunContext) -> JobOutcome
@dataclass(slots=True, frozen=True)
class DiscordCfg:        # snapshot of the section-5 keys taken at run start: DiscordCfg.from_settings(app_config.settings)
    cron_hours: list[int]; main_guild_id: str; vanity_code: str; main_announce_id: str
    main_channel_recheck_days: int; dead_weekly_after_days: int; expiry_warn_days: int
    widget_disabled_recheck_days: int; widget_skip_guild_ids: set[str]; channel_map_reviewed: bool
    presence_min_samples: int; use_bot_token: bool; events_enabled: bool; venue_change_debounce: int
    @classmethod
    def from_settings(cls, settings) -> "DiscordCfg"
# GatewayCfg (discord_gateway.py) is the same pattern over DISCORD_AGGREGATOR_GUILD_ID, DISCORD_AGGREGATOR_CHANNEL_IDS,
#   DISCORD_MAIN_ANNOUNCE_CHANNEL_ID, DISCORD_BACKFILL_*, DISCORD_TEXT_MAX_CHARS, DISCORD_GATEWAY_ENUMERATE_FOLLOWS
def cycle_slot(now: datetime, hours: list[int]) -> datetime        # latest cron slot ≤ now (UTC, minute 0)
async def main(argv: list[str]) -> int
#   python -m app.collectors.discord --once [--dry-run] [--code C]         one invite cycle outside the scheduler
#   python -m app.collectors.discord --widgets                             one widget pass
#   python -m app.collectors.discord --report topology [--netuid N]        per-occupant class, guild, members, codes, CSV coverage
#   python -m app.collectors.discord --export-csv                          write known main-server channel ids to the seed CSV
#   python -m app.collectors.discord --metrics [--as-of YYYY-MM-DD|now] [--netuid N] [--write]
#     (as built: default `now`, print only — a 00:00Z as_of cannot see today's rows; --write requires a date and writes
#      only the 8 registry keys at that official slot)

# app/collectors/discord_gateway.py
@dataclass(slots=True)
class AnnouncementRow:
    msg_id: str; platform: str; occupant_keys: list[str]; netuid: int | None
    source_guild_id: str | None; source_channel_id: str; message_id: str
    local_channel_id: str | None; local_message_id: str | None; via_own_server: int; mapping: str
    author_is_team: int | None; has_attachment: int; url_count: int
    text: str | None; text_hash: str; text_len: int; event_at: int; observed_at: int; meta: dict   # unix s
def discord_clean(text: str) -> str                            # strips mention/emoji tokens (hash input)
def redact_ids(text: str) -> str                               # <@id>→@user, <@&id>→@role, <#id>→#channel (stored text)
def compose_text(msg) -> str                                   # content + embed title/description/fields/footer
def classify_message(msg, cmap: ChannelMap, guild_map: dict[str, list[str]], cfg: "GatewayCfg",
                     now: datetime) -> AnnouncementRow | None   # duck-typed msg (tests use fakes)
class DiscordGatewayTask(LongTask):
    id = "discord_gateway"; name = "Discord aggregator bot"; phase = 6
    enabled_flag = "DISCORD_GATEWAY_ENABLED"; provider = None
    liveness_max_age_sec = 600; backoff_min_sec = 5.0; backoff_max_sec = 300.0
    def enabled(self) -> bool            # flag ∧ ¬(fatal ∧ token_hash unchanged); pauses come from 16's LongTask.paused_reason() (B12)
    async def run_once(self, stop: asyncio.Event) -> None
    def status(self) -> dict             # LongTask fields + guild_ok, channels, follows, follows_lost, unmapped_channels,
                                         #   foreign_guilds, last_message_at, backfill_last_at, fatal_error
    async def _on_ready(self) -> None
    async def _on_message(self, msg) -> None
    async def _on_raw_delete(self, payload) -> None
    async def _backfill_channel(self, channel) -> int
    async def _beat_loop(self, stop: asyncio.Event) -> None       # beat() every 30 s; hourly collector_runs heartbeat
    async def _map_reload_loop(self, stop: asyncio.Event) -> None # every 10 min: CSV mtime changed → ChannelMap.build + remap_unmapped (idempotent)

# app/collectors/telegram.py
@dataclass(slots=True)
class TmeProfile:
    kind: str | None; title: str | None; members: int | None; online: int | None
    deprecated_marker: int; parse_ok: int; error: str | None
@dataclass(slots=True)
class TmePost:
    post_no: int; event_at: int; text: str; views: int | None; has_attachment: int; url_count: int   # event_at unix s
class TmeLayoutChanged(Exception): ...
def parse_count(s: str) -> int | None                 # '23 831' / '23\u00a0831' → 23831 · '1.2K' → 1200
def parse_tme_profile(html: str, *, private: bool, status: int) -> TmeProfile
def parse_tme_preview(html: str, handle: str) -> list[TmePost]
class TelegramCountsJob(CollectorJob):
    id = "telegram_daily"; name = "Telegram counts"; phase = 6; enabled_flag = "TELEGRAM_COUNTS_ENABLED"
    interval_flag = "TELEGRAM_COUNTS_INTERVAL_MIN"; provider = "tme"; paid = False; timeout_sec = 1800
    async def collect(self, run: RunContext) -> JobOutcome

# app/collectors/community_store.py
async def latest_invite_states(db, codes: Iterable[str]) -> dict[str, TargetState]
async def latest_ok_evidence(db, codes: Iterable[str], now: datetime, cfg) -> dict[str, InviteResult]   # per-tier windows
async def censused_in_slot(db, cycle_at: int) -> set[str]
async def insert_discord_rows(db, rows: list[dict]) -> int                                   # one transaction
async def update_watch_invites(db, results: Mapping[str, InviteResult], now: datetime) -> None
async def insert_telegram_snapshot(db, row: dict) -> int                                     # INSERT OR IGNORE (one per occupant, handle, day)
async def upsert_announcement(db, row: AnnouncementRow) -> bool                              # INSERT OR IGNORE + team mentions
async def write_gateway_heartbeat(db, hour_ts: int, ok: bool, rows: int, error: str | None) -> None   # run_id 'gw:{hour_ts}'
async def touch_operator_rows(db, watch_ids: list[int], now: int) -> int                     # per-run last_seen_at touch (§2.8)
async def get_task_state(db, job: str, key: str, default=None) -> Any
async def set_task_state(db, job: str, key: str, value: Any) -> None

# app/collectors/community_events.py
# all emitters call 03's app.storage.events.emit_event(db, *, event_type, occupant_key, netuid, severity,
#   event_at, observed_at, payload, dedupe_key, source='community', run_id) -> int | None   (None = dedupe hit)
async def emit_invite_events(db, decisions: list[TopologyDecision], results: Mapping[str, InviteResult],
                             states: Mapping[str, TargetState], now: datetime, run_id: str) -> int
async def detect_venue_changes(db, decisions: list[TopologyDecision], now: datetime, run_id: str) -> int
async def detect_inflation(db, occupant_keys: list[str], as_of: datetime, run_id: str) -> int
async def detect_silence(db, occupant_keys: list[str], as_of: datetime, run_id: str) -> int   # reads 12's silence_days raw rows;
                                                                                                # payload + last_comm_at/_platform (D2)
async def last_team_comm(db, occupant_key: str, t: int) -> tuple[int | None, str | None]      # (ts, 'x'|'discord'|'tg'), observed_at ≤ t
async def emit_tg_venue_changes(db, rows: list[dict], now: datetime, run_id: str) -> int

# app/features/community.py
from app.features.types import MetricResult          # spec 12: (value, value_json={}, absence_reason=None)
from app.features.registry import METRICS            # presence_ratio params (lazy; literal defaults before Phase 8)
# one producer coroutine per 08 key of section 3.4, all with 12's ProducerFn signature
#   async def compute_<metric_key>(db, occupant_key: str, as_of: datetime) -> MetricResult
# i.e. compute_venue_reachable, compute_discord_members, compute_presence_ratio, compute_discord_growth_30d,
#      compute_discord_announcements_30d, compute_tg_posts_30d, compute_discord_last_activity_days,
#      compute_tg_last_activity_days; evidence-only: compute_discord_topology, compute_tg_members, compute_inflated_days
async def topology_at(db, occupant_key: str, t: datetime) -> tuple[str | None, str | None, dict | None]
async def inflated_days(db, occupant_key: str, as_of: datetime) -> int | None
PRODUCERS: dict[str, ProducerFn]                     # the 8 registry keys -> fn (passed to 12's register_metric, C5; also the CLI)
EVIDENCE: dict[str, ProducerFn]                      # discord_topology, tg_members, inflated_days (03 catalog only, C3)
def register_producers() -> int                      # at import: register_metric(key, fn=…, pillar='P5', freshness_days=2[, weight=0.0,
                                                     #   component_of=…]) per PRODUCERS key; ImportError of 12's registry → 0 (pre-Phase 8)
async def write_evidence(db, occupant_keys: list[str], as_of: datetime, run_id: str) -> int   # discord_6h: EVIDENCE fns at now,
                                                     #   rows as_of=cycle_at, stage='raw', source='community' via put_metric_values
async def community_metrics(db, occupant_key: str, as_of: datetime) -> list[tuple[str, MetricResult]]   # CLI --metrics

# app/nlp/textnorm.py  (B11: the only text normaliser/hash; 09 and 10 import it)
def text_norm(text: str) -> str      # NFKC → lower → URLs https?://\S+ → ' ' → mentions @\w+ → ' ' → whitespace runs → ' ' → strip
def text_hash(text: str) -> str      # sha256(text_norm(text).encode('utf-8')).hexdigest()
```

This spec defines no combiner code and no normaliser code (C3): `announcement_cadence_30d`/`silence_days` come from 12's `app/features/derived.py` (no `compute_announcement_cadence_30d`/`compute_silence_days` exists anywhere), `presence_ratio` normalisation from 12's `app/features/normalise.py`. Before Phase 8 (12 not built), `app.features.types`/`registry` do not exist yet: the Phase-6 implementer creates the minimal `MetricResult` dataclass exactly as 12 §2.3 defines it in `app/features/types.py`; `community.py` falls back to the literal `presence_ratio` params on `ImportError`, and `register_producers()` returns 0 (nothing to register with). `put_metric_values` and 03's catalog exist from Phase 0, so `write_evidence` works in Phase 6. Queries against 09's tables (`social_posts`, `social_post_mentions`) are valid from Phase 0 because 03 creates every table up front; they simply return no rows before Phase 9. The CLI `--metrics` prints 12's combiners only when `app.features.derived` imports (else `n/a before Phase 8`).

### 8.3 `DiscordInvitesJob.collect()` step order

1. `cfg = DiscordCfg.from_settings(app_config.settings)`; `cycle_at = cycle_slot(run.started_at, cfg.cron_hours)`; `now = run.started_at`.
2. If the CSV mtime differs from `job_state csv_mtime`: `load_channel_map` → `sync_channel_map` → `remap_unmapped`; store the new mtime. Then, every run: `touch_operator_rows` (§2.8 retirement guard).
3. `occ = active_occupants(db, include_pre_launch=True)`; `targets = watch_targets(db, p)` for `discord_invite`, `discord_channel`, `discord_handle`; group invite targets by code; add the vanity code.
4. `states = latest_invite_states(db, codes)`; `due = [c for c in codes if is_due(states.get(c), code=c, now=now, cycle_at=cycle_at, cfg=cfg)]`, ordered vanity → own → main-channel → new → dead.
5. For each due code: `get_guard().allowed()` else record a guard failure; `res = await fetch_invite(run.http, code, settings)`; memo. `CloudflareBan` → write the rows gathered so far, then re-raise.
6. `evidence = latest_ok_evidence(db, codes, now, cfg)` overlaid with this run's successful results; `main_alive` from the vanity.
7. For each occupant: `classify_occupant(...)` (previous primary guild from `job_state topo:{occ}`).
8. Build rows (§2.4): invite fan-out rows for codes fetched this run, one main row, census rows for occupants without a primary row in this run and not yet censused in `cycle_at`; `insert_discord_rows` in one transaction; `update_watch_invites`; then `write_evidence(db, occ, as_of=cycle_at, run_id)` (the three evidence keys, §2.9).
9. If `COMMUNITY_EVENTS_ENABLED`: `emit_invite_events`, `detect_venue_changes`; if `cycle_at` is 00:00Z: `detect_inflation` (from `inflated_days`) and `detect_silence` (from 12's latest fresh `silence_days` raw row plus `last_team_comm` for the D2 payload keys; no-op before Phase 8) with `as_of = cycle_at`.
10. Update `self._status` extras (`targets, due, fetched_ok, dead, failed, invalid_10min, classes, csv_rows, csv_errors, csv_stale_rows`).
11. If `attempted > 0 and ok == 0`: raise `ProviderError(f"discord: all {attempted} targets failed")`.
12. `logger.info("discord_invites_run", data={cycle_at, due, ok, dead, failed, rows, classes})` (shape of `collector.py:184-190`); `return JobOutcome(rows=rows_written, note=f"cycle={cycle_at} due={n} ok={k} dead={d} failed={f}")`.

`DiscordWidgetJob.collect()`: guild set (§2.5) → due filter from latest widget rows → `fetch_widget` per guild → fan-out rows → `insert_discord_rows` → raise if attempted > 0 and all failed. `TelegramCountsJob.collect()`: distinct handles → profile fetch → preview when a public channel → rows + announcements → `emit_tg_venue_changes` → raise `TmeLayoutChanged` if every 200 page failed to parse.

### 8.4 Build order

1. DDL (§3.2) through spec 03's migration runner (03 folds all proposed tables into migration version 1; if 03 already shipped v1 with its older shape, an additive migration per 03 §2.4 — `ADD COLUMN`s only, a type change needs `-- @allow-destructive`); confirm `python3 -c "import sqlite3; print(sqlite3.sqlite_version)"` ≥ 3.37 (STRICT tables and generated columns; Ubuntu 22.04 ships 3.37.2).
2. `app/nlp/textnorm.py` + tests (if 09 already shipped the normaliser elsewhere, move it here and re-export).
3. `discord_api.py` + fixtures + tests → 4. `discord_topology.py` + tests → 5. `discord_channels.py` (+ `--export-csv`) → 6. `community_store.py`, `DiscordInvitesJob`, CLI `--once/--dry-run/--report` → 7. `DiscordWidgetJob` → 8. `telegram.py` → 9. `community_events.py` → 10. `features/community.py` + `--metrics`.
11. Config keys (§5.2) into `app/config.py` + `.env.example`; `JOB_TABLE` amendment (§6); `pyproject` dependency.
12. `discord_gateway.py` (unit tests with fakes first; live test after checklist B4).
13. Verify checklist (§10); hand the operator the §2.8 procedure and the `--report topology` output.

### 8.5 What is ported from the old bot

| old (`E:\SN_Trader\SubnetTrader\app\whale\collector.py`) | lines | new | how |
|---|---|---|---|
| module docstring: fail-safe posture (never raise, throttle, count failures, page once, re-arm) | 13-16 | docstrings of `discord.py`, `discord_gateway.py`, `telegram.py` | adapt wording |
| `OVERLAP_SEC`, `MAX_PAGES_PER_POLL`, `FAIL_ALERT_THRESHOLD` | 36-38 | `DISCORD_BACKFILL_OVERLAP_SEC`, `DISCORD_BACKFILL_MAX_PER_CHANNEL`; threshold → 01 `JOB_FAIL_ALERT_THRESHOLD` | constants become config |
| `_status` dict + `status()` | 40-51 | superseded by 01 `CollectorJob.status()` / `LongTask.status()`; extras merged in | do not copy |
| `load_cohort()` fail-safe file loader | 59-75 | `load_channel_map()` | port the pattern verbatim: `try: … except (OSError, ValueError): return [], [reason]`, per-row type checks, never raise |
| `_fetch_delegation_page()` single 429 retry | 78-88 | superseded by 01 `Http.request` | do not copy |
| `poll_whale_events()` never-raise boundary, consecutive failures, one-shot Telegram page, re-arm on success | 91-121 | 01 `CollectorJob.run()` for the three jobs; the gateway supervisor applies the same idiom to fatal errors via `AlertLatch` | pattern |
| unsupported-source / missing-cohort logged no-op | 124-138 | gateway without token/guild → disabled by 01's validator + one log line; invites job with 0 targets → `JobOutcome(rows=0, note='no_targets')` | pattern |
| watermark − overlap, first-run lookback | 145-147 | gateway backfill `after` | port logic |
| paged loop with runaway cap and min-interval spacing | 149-182 | backfill `limit=` cap with a warning when hit; request spacing is the limiter's job (no extra sleeps) | adapt |
| structured summary log | 184-190 | `discord_invites_run`, `discord_widget_run`, `telegram_counts_run`, `discord_gateway_backfill` | port shape |

Nothing else in the old bot touches Discord or Telegram (the old `fundamentals_collect.py` only carried the URL strings, which 04 now owns); the old `app/notifications/telegram.py` is the alert transport (01), not a collector.

---

## 9. Tests

Fixtures in `tests/fixtures/community/` (bodies shaped exactly as recorded in research 02-gapfill-discord-telegram-topology):

| fixture | content |
|---|---|
| `invite_own_server_nodexo.json` | code `fwjBwxUcdX`, guild `1039241590092353596` "Nodexo", features `["NEWS","COMMUNITY"]`, channel type 5, 1986/142, `expires_at: null`, plus an `inviter` (`username: "someone"`) that must be dropped |
| `invite_vanity_bittensor.json` | code `bittensor`, `flags: 2`, guild `799672011265015819`, features `NEWS, COMMUNITY, DISCOVERABLE, MEMBER_VERIFICATION_GATE_ENABLED, PREVIEW_ENABLED`, `verification_level: 4`, 63 boosts, tier 3, `vanity_url_code: "bittensor"`, 50905/5575 |
| `invite_main_channel_oro.json` (deep-link case) | synthetic code `oroInv15` → main guild, channel `{id: "1466080859164643389", type: 0, name: "15・oro・ο"}` |
| `invite_10006.json` / `invite_50270.json` | 404 `{"message": "Unknown Invite", "code": 10006}` / 404 `{"message": "Invite is expired.", "code": 50270}` |
| `invite_expiring_umi.json`, `invite_inflated_tag101.json` | guild `1471703293599416333` 172/54, `expires_at 2026-09-22T22:53:28+00:00` (already past on 2026-09-23 — the live code now returns 404) / guild `1419378005230485587` 1905/9, landing `》rules` |
| `invite_group_dm.json`, `cf_1015.html` | `type: 1` without `guild`; a synthetic Cloudflare 1015 HTML body for a 429 (real body UNVERIFIED — the parser keys only on "not JSON") |
| `widget_enabled_cooking.json` / `widget_disabled.json` | guild `692175473198759938`, `presence_count 1311`, `instant_invite`, `members[]` with `username: "alice"` / 403 `{"code": 50004}` |
| `tme_*.html` | NATIX group `23 831 members, 619 online` (U+00A0 thousands separators), inference_labs 7 393/219, Tensorplex private channel 255 subscribers, BitMind private group "(Deprecated)" 472/29, `wahoopredict_support` user page, a public channel profile, a `t.me/s` page with 3 blocks (text, photo, `service_message`), a layout-changed page |
| `fakes_discord.py` | `FakeMessage(id, guild_id, channel_id, type, flags_value, reference, webhook_id, content, embeds, attachments, created_at)`, `FakeChannel.history()`, `FakeClient` that can raise `LoginFailure` |

| # | file · test | asserts |
|---|---|---|
| 1 | `test_discord_api` · `parse_invite_own_server` | guild id, 1986/142, ratio 0.0715 ± 1e-4, landing type 5, sorted features, `guild_created_at == datetime(2022,11,7,18,15,3,253000,UTC)` (stored `1667844903`), `invite_valid 1`; `"inviter"`/`"someone"` absent from `json.dumps(asdict(result))` |
| 2 | · `parse_invite_vanity` | main guild, 50905/5575, verification 4, boosts 63, `vanity_url_code`, `guild_created_at` = 2021-01-15T16:11:04.673Z (stored `1610727064`) |
| 3 | · `main_channel_match` | oro fixture: netuid 15 → True, 16 → False; vanity landing (live 2026-10-17: `📣・announce`, type 5, id `830075335084474390`, no longer `rules`) → False; landing id in the occupant's channel set → True |
| 4 | · `parse_invite_errors` | 10006 / 50270 → `invite_valid 0` + code; 404 HTML → `None`; group DM → `0`, `not_guild_invite` |
| 5 | · `expired_by_clock` | Umi at `2026-09-22T12:00Z` valid; at `2026-09-23T00:00Z` → `0`, `50270`, `expired_by_clock` |
| 6 | · `snowflake` | `1419378005230485587 → 2025-09-21T17:41:27.215Z` (Tag101, unix 1758476487); `799672011265015819 → 2021-01-15T16:11:04.673Z`; `1419380124024836297 → 2025-09-21T17:49:52.375Z` (NIOME lookalike, +8 min 25 s, unix 1758476992) — values recomputed in review |
| 7 | · `parse_widget_drops_members` | no `members`; `alice`/`username`/`avatar` absent; `instant_invite_code` extracted; disabled → `widget_enabled 0` |
| 8 | · `fetch_invite_http` (respx) | UA = `DISCORD_USER_AGENT`; `Authorization` only with token + flag; a 404 route called once per run; JSON 429 `retry_after` → `limiter.observe('discord', {'Retry-After': …})` + exactly one retry; HTML 429 → `CloudflareBan` and a 3600 s penalty in `limiter.snapshot()` |
| 9 | · `invalid_guard` | 100 × 403 → `allowed() False`; +601 s → True; 404s not counted |
| 10 | `test_discord_topology` · `is_due_tiers` | own valid → due every cycle; vanity → due; main-channel checked 3 d ago → no, 7 d → yes; dead 5 d at hour 6 → no, hour 0 with last check 21 h ago → yes; dead 40 d, last 3 d → no, 7 d → yes; never fetched → yes; already OK this `cycle_at` → no |
| 11 | · `classify_sn106` / `classify_sn64` | subnetalpha `t7BMee4w` (10006) + chain `fwjBwxUcdX` → `own_server`, primary `fwjBwxUcdX`, `one_dead`; chain `chutes` + subnetalpha `bittensor` → `own_server`, `single_source` |
| 12 | · `classify_main_channel` | chain deep-link row (main guild) + vanity valid → `main_channel`; vanity failed → `unknown`; oro invite for netuid 15 → `main_channel` |
| 13 | · `classify_precedence` | dead + handle (SN8) → `invalid`; handle only → `handle_only`; vanity only → `main_vanity`; identity `absent` → `none`; identity `unknown` → `unknown` |
| 14 | · `primary_stickiness`, `source_agreement` | SN49's two guilds: previous primary kept though smaller; it dies → chain-resolved wins; chain A vs subnetalpha B → `disagree`; one guild via two codes → `agree` |
| 15 | `test_discord_channels` · `load_csv_forms` | 5- and 3-column rows with defaults; ecosystem row memory-only; blank netuid + `subnet_text` rejected; bad snowflake rejected; missing file → `([], ['missing'])`, no exception |
| 16 | · `sync_merges_chain_row`, `csv_row_stale_occupant`, `touch_every_run` | same channel as a chain deep link → one row, `source='chain'`, meta merged; re-occupied netuid → no new row unless the row's `notes` carries `reg=<network_registered_at>` of the current occupant; an unquoted comma in `notes` → the extra fields are re-joined into `notes`; unchanged CSV mtime over 20 simulated days of runs → operator rows' `last_seen_at` advances every run and 04's 14-day retirement never fires |
| 17 | · `remap_unmapped_is_pit` | mention row `observed_at = remap time`; the `announcements` row is byte-identical before/after (no UPDATE); `discord_announcements_30d` at an earlier `as_of` excludes the post, later includes it |
| 18 | `test_discord_jobs` · `cycle_rows` | own (Nodexo) + two vanity-only + one handle-only: vanity fetched **once**; 1 invite row (`is_primary 1`), 1 main row (`occupant_key NULL`), 3 census rows; `watch_channels.guild_id/verified_at` set; `discord_topology`/`inflated_days`/`tg_members` rows for all 4 occupants at `as_of = cycle_at` with `source='community'`, `stage='raw'`, `observed_at ≥` every input row's `observed_at` |
| 19 | · `rerun_same_slot_idempotent`, `all_failed_raises` | same `cycle_at` → 0 HTTP calls for OK codes, no duplicate census; all 503 → `ProviderError`, failure rows written, wrapper `ok=0` |
| 20 | · `cycle_slot`, `widget_schedule` | `06:00:40Z → 06:00Z`, `05:59Z → 00:00Z`, custom hours honoured; enabled widget hourly, disabled skipped until 7 d, main guild never requested |
| 21 | `test_discord_gateway` · `crosspost_mapped_channel` | reference `(G, C, M)`, `C` = CSV `announce` row of O → `msg_id "discord:M"`, `via_own_server 1`, `author_is_team 1`, `mapping 'channel'`, text includes the embed title, `text_hash` = `sha256(text_norm(discord_clean(composed)))`, one mention row with `platform 'discord'`, `post_id "M"`; `"<@123> hi"` is stored as `"@user hi"`; with `DISCORD_STORE_TEXT=false` → same redacted `text` with `text_purge_after = observed_at + LLM_PENDING_TEXT_MAX_DAYS·86400`, same `text_hash` (`true` → `text_purge_after` NULL) |
| 22 | · `main_announce`, `guild_mapping` | `830075335084474390` → no occupant, `author_is_team 0`; `G` = primary guild of one occupant → `'guild'`, of two → `'guild_ambiguous'`, `occupant_key NULL`, 2 mention rows |
| 23 | · `ignored_messages` | no reference / no webhook → None; type 12 → None + follow recorded; foreign guild → None |
| 24 | · `backfill_idempotent` | 5-message history backfilled twice → 5 rows; watermark = last id; `after` = watermark − 600 s; cap 3 → 3 rows + warning, next backfill continues |
| 25 | · `clean_and_empty_hash`, `fatal_login`, `heartbeat_rows` | `"<@123> Launch <#456> @everyone <:tao:789>"` → `"launch"`; two image-only posts → different hashes; `LoginFailure` → `fatal_error`, exactly one `discord_gateway_fatal` event (system, `warning`), no restart until the token hash changes, then `collector_recovered` after `on_ready`; connected hour → heartbeat `ok=1`, disconnected → `ok=0` |
| 26 | `test_telegram` · `parse_count`, `profiles` | `"23" + U+00A0 + "831" → 23831`, `"1.2K" → 1200`, `"abc" → None`; NATIX group 23831/619, Tensorplex `private_channel` 255, BitMind `private_group` 472/29 `deprecated_marker 1`, wahoopredict `user_or_unknown` with `title NULL`, layout-changed `parse_ok 0`; a second run the same day → no new rows (first attempt wins) |
| 27 | · `preview`, `layout_change_fails_run` | 2 posts (service skipped) with `post_no`, `event_at` parsed from `<time datetime>` to unix s, views; mention rows `post_id "{handle}/{n}"`; rows carry `text` + `text_hash`, with `text_purge_after = observed_at + 7·86400` under the default `TELEGRAM_POSTS_STORE_TEXT=false` (NULL when `true`); `/s/` 302 → `preview_available 0`; all pages broken → `TmeLayoutChanged` |
| 28 | `tests/features/test_community` · `presence_ratio_contract` | `compute_presence_ratio` for Tag101 samples → `ok`, `r ≈ 0.004724`, `value_json.members 1905`, `inflated true`; through 12's `presence_ratio(r, members, low=.05, high=.35, inflate_below=.02, inflate_members=1000)`: Tag101 → 0.0, Tensorplex `(0.01752, 20428)` → 0.0, `(0.004, 999)` → 0.08, Nodexo → 1.0, Nepher `(0.3513, 390)` → 0.998, `(0.9, 20)` → 0.5, members None → None; the thresholds come from `METRICS['presence_ratio'].params` (monkeypatching them changes `inflated`) |
| 29 | · `inflation_tag101` | 4 Tag101 samples/day for 7 completed days → `inflated_days 7`, exactly one `inflated_community` (severity `warning`, `source 'community'`; re-run keeps one via `emit_event` dedupe); 6 days → none; a 1-sample day breaks the streak |
| 30 | · `presence_and_growth` | 4 samples → `median(presence)/median(members)`, 1 sample → `failed`, `main_channel` → `not_applicable`; 1000 @ −30 d → 1100 now = 0.10; base only at −33 d used, −34 d → None; guild switch → None |
| 31 | · `venue_reachable_matrix`, `discord_members` | own_server 1; main_channel 1; vanity-only `failed` / `absent` (0.0) after review; invalid + fresh TG group 1; invalid + TG user `absent` (`reason 'no_reachable_venue'`); census 3 d old `failed`; invalid + TG failure `failed`; `none` + telegram absent `absent` with `reason 'no_venue'`; the status is in `value_json.status` (B3); members 1986 for own_server, `not_applicable` for main_channel |
| 32 | · `legs_make_union`, `last_days` | X 3 posts + Discord 2 (one equal-hash to an X post 20 h apart) + TG 1 (equal-hash to a Discord post) → `discord_announcements_30d 1`, `tg_posts_30d 0`, so 12's Σ = 4 (the union); stale heartbeat → Discord legs `failed`; no followed channel → `not_applicable`; latest Discord post 3 d ago → `discord_last_activity_days 3`, `lower_bound false`; followed 60 d, no posts → 60, `lower_bound true`; no producer ever writes `announcement_cadence_30d` or `silence_days` (asserted on `metric_values` after `--metrics --write`) |
| 33 | · `pit` | rows (announcement, census, mention) with `observed_at > as_of` are ignored |
| 34 | `test_community_events` | first 10006 → no event, second ≥ 20 h later → one `invite_dead` (`warning` only if primary and nothing else reachable); Umi → `expiring` then `expired`, deduped; class change for 1 cycle → nothing, 2 cycles → one `community_venue_changed`; `silence_45d` once per episode, re-armed after 7 days < 45, payload carries every 03-required key incl. `last_comm_at`/`last_comm_platform` (NULL/NULL when nothing was ever posted); every emitted payload passes 03's `emit_event` validation (D1/D2) |
| 35 | `test_privacy_community` | all paths over all fixtures into a temp DB with log capture: no `username`, `inviter`, `alice`, `someone`, `avatar` in any TEXT column or log record; `announcements.text` non-NULL only where `via_own_server=1`; no `<@\d+>` / `<@&\d+>` token in any stored text; `telegram_snapshots.title` NULL for `user_or_unknown`/`bot` rows |
| 36 | live (`network` marker, `TAOSENTINEL_NETWORK_TESTS=1`) | `bittensor` → guild `799672011265015819`, members > 40,000, `0.02 < ratio < 0.3`; `t7BMee4w` → 404 with code ∈ {10006, 50270} |
| 37 | `tests/nlp/test_textnorm` | golden pairs shared with 09/10 (B11): `"Ｌaunch  https://x.co/a @Team\nNOW"` → `"launch now"` (NFKC folds the fullwidth `Ｌ`); URL-only → `""`; `text_hash("A  b") == text_hash("a b")`; order fixed (NFKC → lower → URLs → mentions → whitespace) |

---

## 10. Verify checklist (Phase 6 is done when all pass on the laptop)

```bash
# 0. environment and registration
python3 -c "import sqlite3, discord; print(sqlite3.sqlite_version, discord.__version__)"   # ≥ 3.37 (STRICT) · 2.x
python -m app check                  # exit 0; DISCORD_*/TELEGRAM_*/COMMUNITY_* keys printed, DISCORD_BOT_TOKEN masked
python -m app jobs | grep -E "discord|telegram"
#   discord_6h  6  available  cron 0,6,12,18:00 UTC · discord_widget_1h  interval 60 · telegram_daily  interval 1440 · discord_gateway  task

# 1. offline tests
pytest -q tests/collectors/test_discord_api.py tests/collectors/test_discord_topology.py tests/collectors/test_discord_channels.py \
          tests/collectors/test_discord_jobs.py tests/collectors/test_discord_gateway.py tests/collectors/test_telegram.py \
          tests/collectors/test_community_events.py tests/collectors/test_privacy_community.py tests/features/test_community.py \
          tests/nlp/test_textnorm.py   # green

# 2. one invite cycle
python -m app.collectors.discord --once --dry-run     # lists due codes (≈ 45 on a fresh DB) and the planned requests; writes nothing
python -m app run-job discord_6h                      # exit 0, status ok, rows ≥ 120
DB=data/taosentinel.db
sqlite3 $DB "select topology_class, count(distinct occupant_key) from discord_snapshots
             where cycle_at=(select max(cycle_at) from discord_snapshots where probe='census') and occupant_key is not null
             group by 1 order by 2 desc;"
#   baseline from the 2026-09-22 research (±5), adjusted for SN78 (Umi's temporary invite expired 2026-09-22T22:53Z):
#   main_vanity ≈ 64 · own_server ≈ 24 · main_channel ≈ 18–22 · none ≈ 13 · invalid ≈ 4 · handle_only ≈ 1 · unknown 0
sqlite3 $DB "select count(*) from discord_snapshots where probe='invite' and occupant_key is null
             and cycle_at=(select max(cycle_at) from discord_snapshots where probe='census');"   # 1 (widget rows use hourly cycle_at; probe='invite' already excludes them)
sqlite3 $DB "select distinct invite_code, invite_error_code from discord_snapshots where invite_valid=0;"
#   includes t7BMee4w, 2XSw62p9Fj, 4TzhwgdsAf, TTFXK3pYD, bvBDat3Gy, adsQPnFRY with 10006/50270, and SN78's expired code (50270 or expired_by_clock)
sqlite3 $DB "select netuid, member_count, presence_count, round(presence_ratio,3), datetime(guild_created_at,'unixepoch'), max(observed_at)
             from discord_snapshots where probe='invite' and is_primary=1 and netuid in (101,106,64,122) group by netuid;"   # SQLite bare columns = the max row
#   101 ≈ 1905/≈9/0.005/2025-09-21 17:41:27 · 106 ≈ 1986/≈142 · 64 ≈ 5885/≈1245 · 122 ≈ 10719/≈1307
python -m app run-job discord_6h                      # same slot again → log shows due=0 for OK codes
sqlite3 $DB "select occupant_key from discord_snapshots where probe='census'
             and cycle_at=(select max(cycle_at) from discord_snapshots where probe='census')
             group by occupant_key having count(*)>1;"   # empty
sqlite3 $DB "select typeof(observed_at), typeof(cycle_at), count(*) from discord_snapshots group by 1,2;"   # integer|integer only (03 STRICT)
sqlite3 $DB "select metric_key, count(*), min(source), max(source) from metric_values where stage='raw'
             and metric_key in ('discord_topology','inflated_days','tg_members') group by 1;"   # ≈ active occupants each · community|community

# 3. widget
python -m app run-job discord_widget_1h
sqlite3 $DB "select widget_enabled, count(distinct guild_id) from discord_snapshots where probe='widget' group by 1;"   # 1 → ≈ 5 · 0 → ≈ 20
sqlite3 $DB "select count(*) from discord_snapshots where meta_json like '%username%' or meta_json like '%avatar%' or meta_json like '%inviter%';"   # 0

# 4. Bot-token attribution (UNVERIFIED in research — run once the token exists)
curl -s -A "TaoSentinel-verify/0.1" "https://discord.com/api/v10/invites/bittensor?with_counts=true&with_expiration=true" \
  | jq '.approximate_member_count, .approximate_presence_count'
curl -s -D - -o /tmp/inv.json -A "DiscordBot (https://github.com/<operator>/taosentinel, 0.1.0)" -H "Authorization: Bot $DISCORD_BOT_TOKEN" \
  "https://discord.com/api/v10/invites/bittensor?with_counts=true&with_expiration=true" | grep -i x-ratelimit; jq '.approximate_member_count' /tmp/inv.json
#   expected: counts equal within ±1 % (5-min edge cache); X-RateLimit-* headers only on the second call.
#   If counts differ materially: DISCORD_INVITES_USE_BOT_TOKEN=false and record it in §11 Q10.

# 5. Telegram
python -m app run-job telegram_daily
sqlite3 $DB "select handle, kind, members, online, deprecated_marker, preview_available, parse_ok, title is null from telegram_snapshots
             where cycle_day_ts = cast(strftime('%s', date('now')) as integer);"      # unixepoch() needs SQLite 3.38; 22.04 has 3.37
#   NATIXNetwork group ≈ 23.8k/≈600 · inference_labs group ≈ 7.4k/≈200 · daturavalidator group ≈ 175 · +ug7C1J7Apf8wNzc1 private_channel ≈ 255
#   · +izbzDva6idMzZTYx private_group ≈ 472, deprecated_marker 1 · wahoopredict_support user_or_unknown, title NULL · all parse_ok 1 (6 handles)

# 6. metrics (Phase 6 has no spec-12 engine yet → CLI)
python -m app.collectors.discord --metrics --netuid 101             # as_of = now (a 00:00Z --as-of cannot see today's rows)
#   after ≥ 2 cycles: presence_ratio ok, raw ≈ 0.005, value_json.inflated true (12's normaliser → 0.0 once Phase 8 exists)
#   · discord_members ≈ 1905 · venue_reachable 1.0 · announcement_cadence_30d / silence_days "n/a before Phase 8" (12-derived)
python -m app.collectors.discord --metrics --netuid 23              # venue_reachable 1.0 (main_channel) · discord_members not_applicable
python -m app.collectors.discord --report topology | head -40                      # per-occupant class, guild, members, codes, CSV coverage

# 7. gateway (after checklist B4/B5)
curl -s -X POST -H "X-Auth-Token: $API_AUTH_TOKEN" localhost:8090/api/v1/control/reload-config
curl -s localhost:8090/health | jq '.tasks.discord_gateway'
#   enabled true · connected true · guild_ok true · fatal_error null · foreign_guilds [] · follows lists "Bittensor #📣・announce"
#   Test: make a throwaway server with an Announcement channel, Follow it into #feed-subnets, publish one message:
sqlite3 $DB "select msg_id, mapping, via_own_server, author_is_team, length(text) from announcements order by observed_at desc limit 1;"
#   discord:<id> · unmapped · 1 · NULL · > 0   (within ~5 s)
#   Add a CSV row for that channel (kind announce, any test netuid) → within 10 min a social_post_mentions row (platform 'discord',
#   post_id = the source message id, match_kind 'team') whose observed_at = remap time; announcements.mapping stays 'unmapped' (insert-time
#   state, never updated); the next published message arrives as mapping 'channel'. Stop the service, publish 2 more, start it: both appear once.
sqlite3 $DB "select count(*) from announcements where text glob '*<@[0-9]*' or text glob '*<@&[0-9]*';"   # 0 (ids redacted)
sqlite3 $DB "select count(*) = count(distinct msg_id) from announcements;"                                     # 1
sqlite3 $DB "select started_at, ok, rows from collector_runs where job='discord_gateway' order by started_at desc limit 3;"   # hourly heartbeats

# 8. events (after ≥ 2 daily cycles)
sqlite3 $DB "select event_type, severity, count(*) from events where event_type in
             ('invite_expired','invite_dead','inflated_community','community_venue_changed','silence_45d') group by 1,2;"
#   severities only info|warning · invite_dead ≥ 3 · invite_expired stage 'expired' for SN78 on the first cycles (expired 2026-09-22T22:53Z)
#   · invite_expired stage 'expiring' for SN80 from 2026-10-01 (7 d before its 2026-10-08T13:53Z expiry) · inflated_community for SN101 after 7 days
#   · silence_45d only once spec 12 writes silence_days rows (Phase 8)

# 9. pause (B12: control_state is the store; data/control/<target>.paused is read-only emergency input)
touch data/control/discord.paused; python -m app run-job discord_6h   # skipped: paused:operator, no new collector_runs row;
                                                                       # discord_widget_1h skipped too; gateway disconnects ≤ 30 s
rm data/control/discord.paused
curl -s -X POST -H "X-Auth-Token: $API_AUTH_TOKEN" localhost:8090/api/v1/control/pause/telegram_daily      # telegram_daily only (group telegram_counts works too, §5.3)
sqlite3 $DB "select target, value_json from control_state where target='telegram_daily';"       # telegram_daily|true
curl -s -X POST -H "X-Auth-Token: $API_AUTH_TOKEN" localhost:8090/api/v1/control/resume/telegram_daily
touch KILL_SWITCH; python -m app run-job discord_6h; rm KILL_SWITCH   # runs normally (free job, B12)
```

---

## 11. Open questions / operator decisions

1. **Spec 01 mirror (settled: A2, E1–E3, G1, B12, F1).** 01's `JOB_TABLE` copies §6's table verbatim: cron `discord_6h` at 00/06/12/18:00Z with `min_gap_factor=0`, and LongTask `discord_gateway`. `DISCORD_INTERVAL_MIN` remains only as the liveness-formula input. The §5.2 keys go into 01's inventory and `.env.example`, and `discord.py` goes into `pyproject.toml`. §2.3 relies on G1 (a status listed in `ok_statuses` is returned without retry) and penalizes through `ProviderLimiter.observe` with a synthetic `Retry-After`; the exact `observe` signature is 01's. `KILL_SWITCH` leaves free LongTasks running (B12).
2. **`topology_class='unknown'`** is a seventh value beyond the brief's six; without it a failed fetch would be labelled `invalid`/`none` and become a 0.0.
3. **Posts table and Discord text (settled: B10, B11).** Discord and Telegram posts go only to `announcements`, and 08 never writes `social_posts`. 03 §3.5 still allows `social_posts.platform='telegram'`, which no spec should use. Multi-occupant attribution reuses 09's `social_post_mentions` (`post_id` = native id). Own-server crosspost text is kept 90 d under `DISCORD_STORE_TEXT=true` (the default); the operator may still choose hash-only (master §13 item 8).
4. **Telegram text (settled: amended B10).** By default (`TELEGRAM_POSTS_STORE_TEXT=false`) the text is kept only until spec 10 labels it, at most `LLM_PENDING_TEXT_MAX_DAYS` (7 d), and after that only the hash remains. Setting the flag to `true` keeps the text for 90 d.
5. **`presence_ratio` normaliser outside the master's 0.05–0.35 band** (linear `r/0.05` below; `max(0.5, 1 − (r−0.35)/0.7)` above) is this spec's proposal, adopted verbatim by 12 §2.5; it freezes with ts-1.0 unless changed now.
6. **`discord_growth_30d` denominator:** master "÷ members" implemented as ÷ members 30 d ago; switch to current members before ts-1.0 freezes if that was meant.
7. **Inflation rule vs legitimate quiet servers:** Tensorplex (SN52) 20,428/358 = 0.0175 → normalised 0 and `inflated_community` ×0.90 after 7 d, exactly as the master states. ts-1.1 candidate: exempt absolute presence ≥ 250 or guild age > 2 y.
8. **SN55 lookalike guild:** chain invite `7mJkaJZX` → "NIOME" 1,922/9 created 8 min after Tag101's guild (both land on `》rules`); subnetalpha → the real NIOME (182/28). Chain precedence makes the lookalike primary, so SN55 will be flagged; review both guilds and suppress the chain code via 04 `manual_overrides` if it is not the team's.
9. **≈ 64 `main_vanity`-only occupants:** `venue_reachable` stays `None` until B5 step 7 is done and `DISCORD_CHANNEL_MAP_REVIEWED=true`; then vanity-only → 0.0 (master: a channel id counts, the generic vanity does not).
10. **Bot-token invite calls** — identical counts and `X-RateLimit-*` headers are UNVERIFIED; §10 step 4 settles it.
11. **Checklist 18 (settled: I2).** B5's CSV columns are `netuid,channel_id,channel_name,guild_id,kind[,notes]`, and 3-column rows still load. The re-occupation token `reg=<network_registered_at>` lives in `notes` (§2.8), so no extra column is needed. The "22 pre-filled" rows come from `--export-csv`, and B4 already lists the optional *Manage Webhooks* permission.
12. **`silence_days` lower bounds:** Discord/Telegram legs return observation-start lower bounds (`value_json.lower_bound=true`) when no post was ever seen; 09 §4 returns `None` for the X leg in the same case (09 Q7). 12 §2.6 applies this spec's rule (`min(exact legs)` if any, else `min(lower bounds)`). Using an exact leg over a lower bound can *overstate* silence when the team posted on Discord shortly before the Follow was created; accepted for v1.
13. Crosspost **edits** are ignored in v1 (nothing written); deleted source messages keep their row (`deleted_at` set once) and still count.
14. **Main `📣・announce`** posts: no occupant, `author_is_team=0`; they reach P6 only via spec 10 entities. Confirm.
15. **04 R6 suppression:** 04 lets 08 suppress an invite-vs-invite `source_disagreement` when both codes map to one guild; v1 writes no `manual_overrides` and exposes `source_agreement` instead. Decide.
16. **Widget `instant_invite` as a fallback venue** when the declared invite is dead: stored, not counted in v1.
17. **ToS residual risk:** ≈ 250 req/day to a documented unauthenticated endpoint may still count as "scraping" under Discord's Terms; Developer Terms text UNVERIFIED (403 to curl). Accept §2.11 or set `DISCORD_ENABLED=false` (Discord metrics → `None`).
18. **Freshness reading (settled: C6).** Community fn metrics use 2 d, the 12-derived combiners 1 d, and weekly re-checked main-server Discord evidence 8 d inside classification. The census row still refreshes every 6 h.
19. **Telegram group feeds** need a Telethon user session — out of scope v1 (master §8).
20. **Spec 03 reconciliation (DDL owner).** 03 §3.5 carries this spec's three tables, and §3.2 mirrors them. Settled: (a) the event catalog — 03 §3.9 lists 08's five names (D1), and `silence_45d` requires `last_comm_at`/`last_comm_platform` (D2), which 08 now emits; (b) `announcements.text` — 03 §11 Q3 was resolved in favour of 08/10 (B10). Still to fix in 03 (all minor): (c) the `telegram_snapshots.title` comment should say community kinds only; (d) `announcements.meta_json` lists `edited_count`, which 08 never writes; (e) §3.8 must use C3's names — it still lists the retired `discord_last_announcement_days`/`tg_last_post_days`, and `12 / features` instead of B9's `scoring`; (f) the `announcements.msg_id` comment says `post_labels.post_id = 'announcement:' || msg_id`, but B11 makes it `msg_id` itself; (g) the `social_post_mentions` comment names only 09/10 as writers, although 08 writes `match_kind='team'` rows for `discord`/`telegram`; (h) the `collector_runs.run_id` comment lacks the gateway heartbeat form `gw:{hour_ts}`.
21. **Shared text normaliser (settled: B11).** 08 defines `app/nlp/textnorm.py` (§2.6 step 7, §8.2), and 09 and 10 import it. 09 §2.6's own composition (`lower(strip_urls(strip_mentions(collapse_ws(text))))`, no NFKC) must be replaced by the import; otherwise X and Discord/Telegram hashes never match.
22. **Registry and catalog names (settled: C3).** 12's registry uses this spec's leg names. The evidence keys `discord_topology`, `inflated_days` and `tg_members` belong in 03's catalog and are written by `discord_6h` (§2.9). At the time of writing, 12 §11 Q14 and 03 §3.8 still show the retired names; they must follow C3.
23. **"Weekly re-validation of main-server channel ids" (master §8; 04 §2.9 says 08 re-validates `discord_channel` rows weekly):** a bare channel id cannot be validated keylessly. `GET /channels/{id}` needs a token *and* guild membership, and the Bot is only in the aggregator guild. v1 re-fetches the ≈ 5 **invite codes** that land on main-server channels weekly and trusts deep-link/CSV channel ids while the main vanity is alive (§2.4). A per-channel liveness check would need the operator's manual pass (B5) to be repeated periodically; this is UNVERIFIABLE by automation within the ToS posture.
24. *(Closed by amended 02 §B10.)* "Classified in flight" means pending-classification text. 08 writes the text with `text_purge_after = observed_at + LLM_PENDING_TEXT_MAX_DAYS·86400` (§2.6 step 6, §2.7 step 3). Spec 10's normal 6-h batch labels the post, and 03 NULLs the text once it is labelled or has expired, so these posts do reach P6.
25. *(Closed 2026-10-17.)* 02 §D1 (amended 2026-09-23) added `discord_gateway_fatal` (warning, system, payload `{error, close_code, retries, since}`, dedupe `since`); 03's catalog and 14's routing (immediate) carry it, and §2.6 uses it. Recovery emits `collector_recovered` (collector `discord_gateway`). A foreign guild stays log + `/health` `foreign_guilds` only; a Telegram page for it would need a new 03 type.
26. *(Closed 2026-09-24.)* Prefix matching is retired (02 §B12 as amended). 08's pause targets are 16's `PAUSE_GROUPS` names `discord`, `telegram_counts` and `community`, or exact job ids (§5.3).
27. **04 finding — legacy rows survive re-registration (2026-10-17).** `watch_channels` rows declared only by `legacy` were attached to the *current* occupant of a re-registered netuid (Discord: SN10, 53, 94, 109, 113 open the previous project's server; Telegram: SN27 `neuralinternet`, SN53 `bittensor_subnet53`, SN106 `v0idai` look like predecessors' channels; most were first seen 2026-10-16 17:27Z). 08 ignores legacy-only rows (§2.2); 04 should apply its occupancy guard to these rows and retire them. SN80's `dogelayer` (→ "OpenRoboto", current) needs a `manual_overrides` row if the operator wants it counted.

---

## 12. Sources

- `E:\TaoData\specs\02-cross-cutting-conventions.md` — binding ledger: A2, B1–B4, B9–B12, C3, C5, C6, D1–D4, E1–E4, F1–F2, G1, I2.
- `E:\TaoData\specs\16-ops-deploy-and-observability.md` — §2.5 (liveness rules, amendments 2 and 5), §2.11 (kill switches, pause precedence, `resolve_targets`), §7.3.4 (Discord IP-ban runbook).
- `E:\TaoData\specs\00-MASTER-SPEC.md` — §1.2 (no self-bots), §1.3 (absence vs failure, fail-safe ops, file sentinel), §4 (tier 0 Discord invites/widget, `t.me`; tier 1 Discord Bot), §5 (occupant identity), §6 (`discord_snapshots`, `telegram_snapshots`, `announcements`, no third-party Discord text), §7.2, §7.3 P5 (metric keys, normalisers, weights, absence rule), §7.5 (`silence_45d`, `inflated_community`, `stale_identity`), §7.7 (community freshness 2 d), §8 (Community row), §11, §12 (phase 6).
- `E:\TaoData\specs\01-repo-skeleton-runtime.md` — §2.6 (`CollectorJob`, `LongTask`), §2.7 (`JOB_TABLE`: `discord_6h`, `discord_widget_1h`, `telegram_daily`), §2.9 (`/health`), §2.11 (`AlertLatch`), §2.12 (`Http`, `ProviderLimiter`), §4 (skip vs failure), §5 (Community keys), §6 (`discord` 1/s, hourly cap 200; `tme` 0.5/s).
- `E:\TaoData\specs\03-storage-schema.md` — §2.1 (binding conventions: INTEGER instants, PIT columns incl. `source`, STRICT, `_json`, write classes), §2.5 (append-only tables), §3.1 (`collector_runs`, `job_state`, `control_state`), §3.2 (`identity_resolved`, `watch_channels` + `ux_watch_channels`, `priority`), §3.5 (current `discord_snapshots`/`telegram_snapshots`/`announcements`/`social_posts`/`social_post_mentions` DDL), §3.6 (`events`, `ux_events_dedupe`), §3.9 (event catalog, severities), §11 Q3 (Discord text), Q8 (pause storage), Appendix A (global renames).
- `E:\TaoData\specs\04-subnet-registry-and-identity.md` — §2.6 (Discord/Telegram normalisation), §2.9 (`watch_channels`), §3.1 (DDL), §3.3 (`active_occupants`, `watch_targets`), §3.5 (events, `dedupe_key`), §4, §5 (`DISCORD_MAIN_GUILD_ID`).
- `E:\TaoData\specs\09-x-collector-and-kol.md` — §2.6 (`text_norm`), §3.2 (`social_posts`, `social_post_mentions`), §3.3 (`x_cadence_30d`, `x_engagement_30d`, `x_followers`, `x_last_activity_days`), §4, §11 Q6/Q7.
- `E:\TaoData\specs\12-feature-store-and-scoring-engine.md` — §2.2 (raw `as_of` keying, PIT selection), §2.3 (`MetricResult`, statuses, producer fns), §2.4 (registry rows 34–41, 47–51 with dotted fn paths; closed 62-key table; evidence-only keys), §2.5 (`presence_ratio` normaliser), §2.6 (derived `announcement_cadence_30d`, `silence_days`), §2.8 (`silence_45d`, `inflated_community`, `stale_identity` gates), §11 Q8/Q11/Q14.
- `E:\TaoData\specs\10-llm-classification.md` — §2.2 (queue), §2.5 (privacy table: own-server crossposts only), §3.1 (`announcements` columns read), §3.2 (90-day text retention).
- `E:\TaoData\specs\18-operator-checklist.md` — B4, B5, D5, E.
- `E:\TaoData\research\02-gapfill-discord-telegram-topology.md` — full: invite endpoint behaviour (no rate-limit headers keyless, `max-age=300`, 10006 vs 50270, 404s free), topology counts and per-guild numbers, widget (5 enabled, 403 `50004` counts), Telegram probes, metrics, cadence ≈ 250 req/day, ToS position, manual steps, open questions.
- `E:\TaoData\research\01-web-social-apis.md` — §2 (Bot capabilities, MESSAGE_CONTENT intent, Follow crossposts, rate limits, self-bot ban, Telegram options, discord.py usage).
- `E:\SN_Trader\SubnetTrader\app\whale\collector.py` lines 1-190 (incremental poller, watermark + overlap, runaway cap, fail-safe loader, never-raise boundary, one-shot alert, summary log).
- Discord developer docs: `docs.discord.com/developers/resources/invite` (Get Invite, `with_counts`), `/topics/rate-limits` (invalid-request limit 10,000/10 min, global 50 req/s, User-Agent `DiscordBot (url, version)`), `/resources/channel` (Follow Announcement Channel, channel types, message flags `CROSSPOSTED`/`IS_CROSSPOST`, `CHANNEL_FOLLOW_ADD` type 12), `/topics/gateway` (intents, session-start limit), `/resources/guild` (widget, error 50004); Developer Policy `support-dev.discord.com/hc/en-us/articles/8563934450327`; Terms `discord.com/terms`.
- discord.py docs (`discordpy.readthedocs.io`): `Client`, `Intents`, `MessageFlags.is_crossposted`, `MessageReference`, `TextChannel.history`, `TextChannel.webhooks`, `Webhook.source_guild/source_channel`, `LoginFailure`, `PrivilegedIntentsRequired` — attribute names to be re-checked against the pinned version at build time.
- Telegram public pages `https://t.me/{handle}`, `https://t.me/+{hash}`, `https://t.me/s/{handle}` (structure as probed 2026-09-22 in the research file).

## Changelog
- 2026-10-17 pre-build review + build (Phase 6), checked against the built 01/03/04 code, the live registry and live probes of discord.com and t.me:
  - **Event:** gateway fatal errors use `discord_gateway_fatal` (02 §D1 amended, 03 catalog, 14 routing), not `collector_failing`; persisted fatal state survives a restart; recovery emits `collector_recovered` (Q25 closed).
  - **Limiter:** 01's `ProviderLimiter` now has `penalize(provider, s)` (file-persisted) → used for the Cloudflare ban; JSON 429 → per-host `observe`.
  - **Bug — CSV touch after restart:** the per-run `last_seen_at` touch relied on an in-memory map that is empty after a restart while the mtime is unchanged → 04 would retire operator rows after 14 d. The CSV is now parsed and synced every run; `remap_unmapped` stays mtime-triggered; `meta.operator_since` stamped once.
  - **Bug — capped backfill:** a capped page inside the overlap window would be re-read forever; a capped backfill now resumes exactly at the watermark.
  - **Classification:** total (`none` with `no_pollable_target` when identity is present but 04 produced no row); root netuid 0 excluded; census rows are `is_primary=1`; `discord_handle` values (personal usernames) never stored; `source_agreement` generalised to n sources, `legacy` excluded.
  - **Legacy-only targets ignored** (§2.2, new Q27 for 04): 5 of 6 legacy-only Discord invites opened the previous occupant's server.
  - **Telegram markup** re-verified: plain-space separators, two `tgme_page_extra` blocks on bot pages, private counts in `tgme_page_extra`, not-found answered as 200 → `not_found` only with Telegram's markers, else `parse_ok=0`.
  - Widget guild set = primary own-server guilds; heartbeat rows read by `finished_at` (PIT) and upserted with `MAX(ok)`; `meta.error` only for failures; Discord silence leg counts cross-platform duplicates (activity, not cadence); CLI `--metrics` defaults to `as_of = now`.
  - Live 2026-10-17 (18:00Z slot): 49 codes, 49 OK, 8 dead; classes own_server 26 · main_channel 26 · main_vanity 62 · none 11 · invalid 3 (§10 baseline ±5); widget 26 guilds (6 enabled / 20 disabled); Telegram 8 rows / 7 handles all `parse_ok=1`.
- 2026-09-24 final pass: §5.3 pause targets are now 16's `PAUSE_GROUPS` names `discord`, `telegram_counts` and `community`, or exact ids. `tme` is only a provider id, and the prefix-matching note was dropped (Q26 closed).
- 2026-09-24 final pass: amended B10 applied. With `TELEGRAM_POSTS_STORE_TEXT=false` or `DISCORD_STORE_TEXT=false`, 08 still writes the text, stamping `text_purge_after = observed_at + LLM_PENDING_TEXT_MAX_DAYS·86400`. 03 NULLs it once labelled or expired. Updated §2.6, §2.7, §2.11, DDL comment, §5.2, tests 21/27 and Q4; Q24 closed.
- 2026-09-23 reconciliation:
  - C3/C5: producers `venue_reachable`, `discord_members`, `presence_ratio`, `discord_growth_30d` plus four legs, registered through `register_metric`; absence reasons `no_venue`/`no_reachable_venue`; no combiner producers; retired leg names never written.
  - C3/B4: `discord_6h` writes the evidence keys `discord_topology`, `inflated_days`, `tg_members` at `as_of = cycle_at` (`write_evidence`).
  - B3/B9: the status enum lives in `value_json.status` (no status or `is_provisional` column); `source='community'`, 12-derived rows `scoring`.
  - C6: 2 d fn metrics, 1 d combiners, 8 d weekly main-server evidence.
  - D1/D2: 03 §3.9 names and dedupe placeholders; `silence_45d` payload gains `last_comm_at`, `last_comm_platform` (`last_team_comm`).
  - D3/D4/B2: no direct Telegram; routing is 03's alert column via 14; the gateway's fatal error uses `collector_failing`/`collector_recovered` (superseded 2026-10-17: `discord_gateway_fatal`); the foreign guild is log + `/health` only.
  - B1: ISO inputs parsed with `parse_iso`/`parse_ts`; `social_post_mentions.event_at` written.
  - B10/B11/F2: `DISCORD_STORE_TEXT=true` and `TELEGRAM_POSTS_STORE_TEXT=false` defaults; 03 `nlp_text_retention` owns text NULLing; 08 defines `app/nlp/textnorm.py`; never writes `social_posts`; `msg_id` = `post_labels.post_id`.
  - B12: `COMMUNITY_KILL_SWITCH_PATH` withdrawn; pauses via `control_state` / `data/control/<target>.paused` (16 `resolve_targets`); `KILL_SWITCH` never pauses 08, `PAUSE_ALL` pauses all four.
  - E1–E4/A2: one job table (ids, factory, trigger, UTC time, misfire, timeout, flag, liveness) for 01 to mirror; `discord_6h` cron pinned to 00/06/12/18:00Z.
  - I2: CSV columns `netuid,channel_id,channel_name,guild_id,kind[,notes]`; the re-occupation guard moved from a `registered_at` column to a `notes` token.
  - Package: applied the `silence_45d` payload fix and "no combiner producers"; rejected the leg renames (C3 retires those names) and `value_json.absent` (C5 uses `absence_reason`); new questions Q24–Q26.
