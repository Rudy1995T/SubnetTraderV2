# 14 — HTTP API, SSE, exports, Telegram commands, minimal web UI

**Status:** built 2026-10-18 (reconciled with 02 on 2026-09-23; pre-build review 2026-10-18, see *Changelog* and BUILD-LOG Phase 11) · **Phase:** 11 — except the §2.4 routing table, the §8.5 alert templates and the control routes, which 01 ships in Phase 0 (`alert_dispatch`, 02 §D3) · **Depends on:** 02 (reconciliation ledger — binding over this spec; cited `02 §<id>`), 01 (runtime, `Settings`, job wrapper, `/health` builder, `Http`/`ProviderLimiter`, `create_app`, `alert_dispatch`), 03 (DDL, events catalog §3.9), 04 (registry, needs-review list), 11 (team/owner/review domain functions §2.12), 12 (scores, explain, metric registry, rescore), 13 (quant result JSONs), 16 (`app/control.py`, `PAUSE_GROUPS`, cost governor, backups, systemd units) · **Master spec refs:** §1.3, §2 (reuse table rows `main.py` factory, `api_auth.py`, `telegram.py`, `frontend/`), §5, §6, §7.5, §7.7, §9, §11, §12

---

## 1. Purpose and scope

This spec defines every operator-facing surface of TaoSentinel: the FastAPI application on `:8090` (01's `HEALTH_HOST`/`HEALTH_PORT`), its read/mutation tiers, the SSE stream, CSV/JSON exports, the Telegram command bot, the **alert routing** (02 §A2, §D4) and the minimal web UI, a set of static pages served by the same app at `/ui` (§8.6; like 19's `/wb`, 02 §K1). All three surfaces read the **same** query layer (`app/api/queries.py`) so a number shown in Telegram equals the number in the matrix and in the trading feed.

**In scope**
- `create_app()` factory (one factory; 01 ships its Phase-0 subset), CORS allowlist, two-tier auth (open reads, `X-Auth-Token` mutations) plus an origin guard, runtime injection (`configure(get_runtime)` per router, §8.2).
- The complete route inventory (§3.5), including routes whose semantics other specs own (01 jobs/control, 04 registry, 11 team/owner/review, 12 rescore, 16 budget override): this spec fixes method, path, tier, body, status codes and error mapping.
- Endpoints defined here: `/api/health/services`, `/api/v1/scores/*`, `/api/v1/subnets*`, `/api/v1/events`, `/api/v1/matrix.csv`, `/api/v1/export/scores.json`, `/api/v1/stream`, `/api/v1/quant/results*`, `/api/v1/manual/*`, `/api/v1/review*`, `/api/v1/control/*`. `/health` is served here but built by 01 (02 §G6).
- Telegram: transport (ported), command handlers, reply formats, inline keyboards; the **routing table** (§2.4) and alert templates (§8.5) that 01's Phase-0 `alert_dispatch` job applies (02 §D3); `dereg_watch` for `WATCHLIST_NETUIDS`; the weekly needs-review digest, which also delivers every digest-routed event; the daily market brief (§2.8, added 2026-10-23 by 20 N3).
- Web UI: three static pages (matrix, subnet, events) under `/ui`, no build step and no Node toolchain (§8.6).
- Tests and verify checklist.

**Out of scope**
- Computing any metric or score (spec 12). This component **produces no `metric_values`**.
- The `alert_dispatch` job body (`app/alerts/dispatch.py`, 01, Phase 0 — 02 §D3) and `AlertLatch` (01); this spec owns what is routed where and how messages look.
- The `/health` body (01, 02 §G6); pause/kill mechanics and `PAUSE_GROUPS` (`app/control.py`, 16 §2.11); team/owner/review semantics (11 §2.12); the cost governor (16).
- The in-browser `.env` editor (`config_api.py`) — not ported in v1 (master §2: config is edited on disk, restart required).
- The trading-bot consumer of `/api/v1/scores/latest` (spec 15). This spec fixes the producer contract; 15 has no producer route of its own (02 §H1).
- systemd unit files (spec 16 §2.4). The UI needs no unit of its own: it is served by the backend process.
- Any authentication beyond a single shared token; any TLS termination (loopback + LAN/tailnet only).

---

## 2. Design

### 2.1 Components

```
 runtime.py (spec 01)                                 app/api/
 ┌─────────────────────────┐   get_runtime()           ┌───────────────────────────────────────────┐
 │ db, jobs/tasks, limiter,│ ─────────────────────────►│ app.py  create_app(get_runtime=None)       │
 │ Http, cost governor,    │   (configure(), no import │   health.py    scores.py   subnets.py      │
 │ /health builder         │    of app.runtime)        │   events.py    export.py   stream.py       │
 └─────────────────────────┘                           │   quant.py     manual.py   review.py       │
              │                                        │   registry.py  control.py  ui.py  wb.py(19)│
              │                                        │   queries.py · hub.py (SSE) · errors.py    │
              │                                        └───────────────────────────────────────────┘
              │                                            ▲ same queries · 11 domain fns · 16 app/control.py
              ▼                                            │   · 12 ScoringService.rescore
 app/alerts/  (01 implements, Phase 0, 02 §D3)         app/notifications/commands.py
   dispatch.py  alert_dispatch  ◄── routing.py (§2.4)  build_dispatcher(get_runtime) → CommandDispatcher
                                ◄── templates.py (§8.5)
 app/notifications/  telegram.py (transport + TelegramBot) · bot.py (telegram_bot) · watch.py (dereg_watch)
                     digest.py (needs_review_digest) · format.py (esc, pre tables, chunk)
              │
              ▼  Http.request("telegram", …)  (01 limiter, 02 §G1)
         Telegram API                       browser ── /ui/* pages (frontend/ui/, same origin) ── fetch/SSE ──► :8090
```

- **Runtime injection.** Every router module keeps the Phase-0 pattern that 01 and 19 already ship: a module-level `_get_runtime: Callable[[], Runtime | None]` set by `configure(get_runtime)`, called by `create_app(get_runtime=None)` (production: `app.runtime.get_runtime_or_none`; tests: `lambda: rt`). Routers never import `app.runtime`/`app.main` at module level (the process starts as `__main__`; a second import yields an empty module — old bot `test_api.py:5-10`). There is no separate `ApiContext` dataclass: `rt.db`, `rt.jobs`, `rt.costs`, `rt.limiter` and `build_health(rt)` are the accessors, and the rescore route constructs 12's `ScoringService(rt.db)` itself.
- **`queries.py`** holds every SELECT used by the API, Telegram and exports. Pure functions `(db, …) -> rows`, no FastAPI imports. Stored times are INTEGER unix seconds; conversion to ISO happens only in the response models and Telegram formatters (02 §B1).
- **`hub.py`** (`StreamHub`) drives SSE: the DB is the single source of truth; the hub polls `scores`/`events` rowid watermarks every `STREAM_POLL_SEC` (5 s) and fans out to subscriber queues. This spec's own writes call `hub.nudge()` (module function, a no-op when no hub runs) for an immediate poll; other producers (12's score writer, collectors) are **not** changed to nudge — their rows reach subscribers within one poll interval. No message is ever fabricated in memory that is not already in SQLite; a reconnect replays from SQLite (§3.5).
- **`app/alerts/`**: `alert_dispatch` (01, Phase 0, 02 §D3) reads `events WHERE alerted_at IS NULL`, applies `routing.route_for(row, now)` (§2.4 — data owned here), renders `templates.render(row)` (§8.5), sends, and sets `alerted_at`; restart-safe, exactly-once per row, `ops_exempt` (02 §E1). `dereg_watch` and `needs_review_digest` are this spec's jobs (§2.5, §2.6, job table §6.1).
- **`commands.py`** builds the `CommandDispatcher` from `queries.py`, `manual_service.py` (product/note/flag writes), 11's domain functions (`app.integrity.team`, `owner_admin`, `review`; 02 §H3) and 16's `app/control.py` (pause/resume, 02 §B12); the long-poll `TelegramBot` is ported from the old bot (Pi recovery copy, §8.4) except the handler injection (now a dispatcher object), dispatch table, chat allowlist, `setMyCommands` list and its HTTP client, and joins 01's Phase-0 `send_message`/`send_alert`/`get_me` in `app/notifications/telegram.py`.

### 2.2 Request flow (read tier)

`GET /api/v1/scores/latest` → `scores.py` → `queries.latest_scores(db, spec=…, min_confidence=…, tier=…, include_prelaunch=…, include_insufficient=…, netuids=…)` → one SQL over `scores ⋈ subnets` (latest row per current occupant, 02 §H1) → INTEGER times converted with `to_iso` (02 §B1) → response model → JSON. No server-side caching; the feed and the exports carry ETags (§3.5). Every handler wraps DB access in `try/except` and returns `503 {"error": "db_unavailable"}` when the runtime or `rt.db` is `None`; nothing raises into uvicorn.

### 2.3 Mutation flow (authed tier)

`POST /api/v1/manual/team` → router-level `Depends(require_auth)` + origin guard (§3.6) → pydantic body validation (422) → `app.integrity.team.add_member(db, netuid=…, name, role, profile_url, evidence_url, evidence_note=note, source='manual', created_by='api', verify=verified)` (11 §2.12; 11 resolves the current occupant, writes `team_members` and its own `team_member_added` event) → the returned `ActionResult` is mapped to HTTP by `app/api/errors.py::raise_for_result` (§3.5 error table, e.g. `no_current_occupant` → 404) → `hub.nudge()` → `201 {ok: true, id, occupant_key, status, warning, event_id}`. Telegram `/team add` calls the *same* function with `created_by='telegram'`. The writes this spec owns (`manual_notes` product/note rows, `manual_flags`) go through `app/api/manual_service.py`, which also emits `manual_entry` (§3.3).

### 2.4 Alert routing table (02 §D3/§D4 — data for 01's `alert_dispatch`)

This section is the content of `app/alerts/routing.py` (`ROUTES: dict[str, Route]` + `route_for(row, now) -> str`), which 01 ships with the `alert_dispatch` job in Phase 0 (02 §D3); `test_routing_matches_catalog` (§9) pins it to 03 §3.9. Routes are 03's alert values: **`immediate`** (`yes`: Telegram now), **`digest`** (one line in the weekly digest, §2.6), **`none`** (`no`: stored only). There is **no `direct` route** (02 §D3, amended 2026-10-16): every row, ops rows included, is inserted with `alerted_at IS NULL` and paged by the dispatcher; the only direct sends are `AlertLatch`'s process notices without an `event_type` and its fallback when `emit_event` raises. Repeat suppression is **not** the dispatcher's job: every emitter writes 03's `dedupe_key`, and `ux_events_dedupe` turns a repeat into a no-op (02 §D2).

**`route_for(row, now)`** — only rows with `alerted_at IS NULL` are routed; first match wins:
1. `payload.backfilled = true` → `digest` (02 §D4; 17's history rows also arrive with `alerted_at` preset, 02 §J1).
2. `event_type` not in `ROUTES` → `digest`, logged once per type (02 §D3: an unknown type never pages and never blocks the queue).
3. The type's row below: a fixed route, or `immediate` iff its condition holds, else `digest`.
4. An `immediate` result becomes `digest` when `severity` ranks below `ALERT_MIN_SEVERITY` (default `info`: never), or `digest_late` when `now − observed_at > ALERT_LATE_AFTER_HOURS·3600` (default 24 h — a page that late, e.g. after a Telegram outage or `ALERTS_ENABLED=false`, is noise; the row still reaches the digest).

The dispatcher stores its decision in the same `UPDATE` that sets `alerted_at` (03 lets `payload` change while `alerted_at IS NULL`): `payload.alert_route ∈ {immediate, digest, digest_late, none}`. `digest`, `digest_late` and `none` rows get `alerted_at = now` at once, so they never clog the `ix_events_unalerted` queue; an `immediate` row gets `alerted_at` when Telegram accepts the message.

Rows in 03 §3.9 order; every catalogued type appears exactly once (03's severity column is not repeated):

| event_type | route | condition for `immediate` / note |
|---|---|---|
| `netuid_reoccupied` | immediate | 17's history rows are `backfilled` → digest (rule 1) |
| `occupant_activated` | none | |
| `identity_changed` | immediate if … else digest | `severity = 'warning'` **and** `payload.source = 'chain'` **and** `payload.field ∈ {subnet_name, github_repo, subnet_url, discord}` (02 §D4). `owner_coldkey` changes are `info` → digest; owner swaps page through `red_flag_raised` |
| `source_disagreement` | digest | |
| `identity_junk` | digest | |
| `registry_anomaly` | immediate if … else digest | 3rd consecutive day: rows with the same `payload.reason` and `payload.netuid` exist for both previous UTC days (`floor(observed_at/86400) − 1` and `− 2`) |
| `emission_disabled` | digest | detection event; the page is `red_flag_raised` (02 §D4) |
| `emission_reenabled` | digest | the page is `red_flag_cleared` |
| `dereg_risk_entered` | digest | the page is `red_flag_raised` (`dereg_imminent`) |
| `dereg_risk_cleared` | none | |
| `dereg_proximity` | digest | watchlist pages come from `dereg_watch` (§2.5) |
| `immunity_mismatch` | none | |
| `gate_fit_degraded` | digest | |
| `gate_model_broken` | immediate | |
| `chain_constant_drift` | immediate | 19 reuses 05's type; keeps the page (02 §K5) |
| `dereg_rank_entered_k30`, `dereg_rank_cleared_k30`, `immunity_ends_7d`, `ema72_band_exit` | digest | 19's workbench events (02 §K5) |
| `tmc_schema_drift` | digest | |
| `chain_sse_down` | immediate | |
| `chain_sse_recovered` | immediate | |
| `tmc_budget_degraded` | digest | budget page = `spend_cap_reached` (02 §D4) |
| `repo_renamed` | none | |
| `repo_moved` | digest | |
| `repo_gone` | digest | the page is `red_flag_raised` (`repo_gone`) |
| `repo_restored` | none | |
| `repo_archived` | digest | |
| `org_repo_archived` | digest | |
| `identity_repo_changed` | immediate | |
| `commit_spam_suspected` | digest | |
| `site_down_7d` | digest | the page is `red_flag_raised` (02 §D4) |
| `site_recovered` | digest | the page is `red_flag_cleared` |
| `tls_expiring_14d` | digest | |
| `site_parked` | immediate | |
| `site_hijack_suspect` | immediate | |
| `site_content_reset` | digest | |
| `site_cross_domain_redirect` | digest | |
| `docs_gone` | digest | |
| `invite_expired` | digest | flag `stale_identity` pages |
| `invite_dead` | digest | |
| `inflated_community` | digest | the page is `red_flag_raised` |
| `community_venue_changed` | digest | |
| `silence_45d` | digest | the page is `red_flag_raised` |
| `x_account_unresolved` | digest | |
| `x_account_gone` | immediate | |
| `x_handle_changed` | none | |
| `x_budget_paused` | digest | every reason — 02 §D4 overrides 03's "yes if reason ∈ {pacing, provider_balance}"; the budget page is `spend_cap_reached` |
| `x_budget_resumed` | none | |
| `x_ctf_discrepancy` | digest | |
| `feed_dead` | digest | |
| `youtube_channel_resolved` | none | |
| taxonomy events (the 16 types of 03 §3.9, `product_launch` … `other`) | immediate if … else digest | `severity = 'critical'` (negative, materiality 3, confirmed) |
| `owner_control_change` | digest | the page is `red_flag_raised` (`owner_control_change`), 02 §D4 |
| `owner_coldkey_swap_announced` | digest | |
| `owner_hotkey_repointed` | digest | |
| `owner_identity_churn` | immediate | |
| `owner_perpetual_off` | digest | the page is `red_flag_raised` (`owner_extraction`) |
| `owner_decaying_lock_added` | digest | |
| `owner_lock_reduced` | digest | |
| `owner_extraction_detected` | digest | the page is `red_flag_raised` (`owner_extraction`) |
| `owner_key_compromise_override` | immediate | |
| `owner_auto_lock_changed` | digest | |
| `owner_cut_toggled` | digest | |
| `owner_source_disagreement` | digest | |
| `owner_position_unexplained` | digest | |
| `owner_concentration` | digest | |
| `owner_self_dealing_suspected` | digest | |
| `owner_data_degraded` | immediate | |
| `team_member_added`, `team_member_verified`, `team_member_rejected`, `team_member_delisted` | digest | |
| `team_disclosure_changed` | digest | |
| `tier_change` | immediate | |
| `red_flag_raised` | immediate | |
| `red_flag_cleared` | immediate | |
| `score_jump` | immediate | 02 §D4 |
| `insufficient_data` | digest | 02 §D4 |
| `rank_instability` | digest | 02 §D4 |
| `manual_entry` | none | |
| `collector_paused` | none | |
| `collector_resumed` | none | |
| `rescore_requested` | none | |
| `dereg_watch` | immediate | emitted by this spec (§2.5) |
| `collector_failing` | immediate | |
| `collector_recovered` | immediate | |
| `service_recovered_unclean` | immediate | |
| `spend_80pct` | immediate | |
| `spend_cap_reached` | immediate | |
| `spend_reset` | digest | |
| `budget_override_set` | digest | |
| `backup_failed` | immediate | |
| `backup_mirror_missing` | immediate | |
| `backup_mirror_failed` | immediate | |
| `backup_stale` | immediate | |
| `scoring_stalled` | immediate | |
| `discord_gateway_fatal` | immediate | |
| `backfill_done` | digest | |
| `backfill_failed` | immediate | |
| `disk_low` | immediate | |
| `disk_critical` | immediate | 02 §D3 (amended): was `direct` |
| `db_integrity_failed` | immediate | same |
| `schema_warning` | immediate | |
| `retention_skipped` | digest | |

**Delivery rules** (01 implements them in `alert_dispatch`; this spec owns them): each tick selects up to `ALERT_MAX_ROWS_PER_TICK` rows `WHERE alerted_at IS NULL ORDER BY id` (03's `ix_events_unalerted`), routes them, and sends the `immediate` ones oldest first, rendered by `templates.render(row)` (§8.5). More than `ALERT_BATCH_THRESHOLD` immediate rows in one tick are sent as batched messages (header `📬 <b>{k} alerts</b>`, one line per row, chunked at 4,000 characters, §2.7), which keeps a post-outage burst inside Telegram's per-chat rate. A Telegram `400` for one row (e.g. malformed HTML) is retried once as plain text (tags stripped); if that fails too, the row gets `payload.alert_failed = true` and `alerted_at` is set, so one poisoned row cannot block the queue. Network errors, `5xx` and `429` never mark a row: the tick stops and the next tick retries (rule 4 eventually moves stale rows to the digest).

### 2.5 `dereg_watch`

Job `dereg_watch` (§6.1) evaluates, every `DEREG_WATCH_INTERVAL_MIN`, each `netuid ∈ WATCHLIST_NETUIDS` that has a current occupant (`subnets`), using the newest `chain_ticks` row of that netuid (05) and the occupant's `dereg_safety` raw row (05; already a final [0,1] value, 12 registry row 3) read with 03's `metric_at(occupant_key, 'dereg_safety', now, spec_version=SPEC_VERSION, freshness_s=METRICS['dereg_safety'].freshness_s)` (02 §B5):

```
blocks_left = subnets.network_registered_at + NETWORK_IMMUNITY_PERIOD_BLOCKS − tick.block_number
immune      = blocks_left > 0                 # local rule (master §5), not TMC's `immune` flag (false before start)
reason      = "bottom3"         if not immune and tick.deregistration_risk == 1
            = "safety_metric"   elif not immune and dereg_safety is not None and dereg_safety <= DEREG_WATCH_SAFETY_MAX (0.10)
            = "immunity_ending" elif immune and blocks_left <= DEREG_WATCH_IMMUNITY_WARN_DAYS * 7200   # 7,200 blocks ≈ 1 d at 12 s
            = None              otherwise
```
`moving_price_rank` = 1-based rank of the netuid by `subnet_moving_price` ascending among the newest ticks (≤ 30 min old) of all non-immune netuids (null when its price is null or it is immune). When `reason` is set: `emit_event(event_type='dereg_watch', severity='warning', source='api', occupant_key, netuid, payload={netuid, name, reason, moving_price_rank, immune, blocks_left_immunity: blocks_left if immune else null, dereg_safety}, dedupe_key=f"{occupant_key}|{reason}|{now // 86400}")` — 03's pattern `occ|reason|day`, so a watched subnet pages at most once per reason per UTC day. If the newest tick is older than 30 min (SSE recorder and REST fallback both down) that netuid is skipped and logged; the job never fabricates a risk. SQLite reads only, no provider calls.

### 2.6 Weekly needs-review digest (also the delivery of the `digest` route)

Cron job `needs_review_digest` (§6.1; default Monday 08:00Z via `NEEDS_REVIEW_DIGEST_DAY`/`NEEDS_REVIEW_DIGEST_HOUR_UTC`; checklist D5) sends one message, chunked (§2.7), with three sections:
1. **Needs review** — the `/needsreview` reply (§8.5), built by `queries.needs_review` (§8.3): 04's computed list, 11's open review queue, team, product and blocked-site buckets.
2. **Digest events** — every event routed to the digest since the previous digest: rows with `id > job_state('needs_review_digest','last_event_id')` and (`payload.alert_route ∈ {digest, digest_late}` or `payload.backfilled = true`), grouped by `event_type`: a count, then up to 3 lines rendered with the §8.5 templates (backfilled rows are only counted, marked `(history)`); a type with more rows ends with `… +k more · {ui}/events?type=<type>`.
3. A footer with the `{ui}/events` link.

After Telegram accepts the last chunk the watermark becomes `min(MAX(events.id), (SELECT MIN(id) FROM events WHERE alerted_at IS NULL) − 1)`, so rows the dispatcher has not routed yet are never skipped. The digest never touches `alerted_at` (the dispatcher set it). Before Phase 11 digest rows are stored and marked but only counted in 16's daily ops digest. Telegram `/digest` sends sections 2–3 on demand without moving the watermark.

### 2.7 Telegram message rules

- HTML `parse_mode`; every dynamic string passes through `html.escape` (`esc()`).
- Tables are rendered inside `<pre>…</pre>` with fixed-width columns; links are placed **outside** `<pre>` (Telegram does not render anchors inside `pre`).
- `chunk(text, limit=4000)` (`app/notifications/format.py`, shipped in Phase 0 with the dispatcher; 16's ops digest uses it too) splits on line boundaries and closes/reopens `<pre>` around the cut; the first chunk carries the keyboard. Length is counted in UTF-16 code units of the HTML source (never less than Telegram's post-parse count), so no message exceeds Telegram's 4,096-character limit.
- Every Bot API call (alerts, replies, `getUpdates`) goes through `Http.request("telegram", …)` (02 §G1 provider id `telegram`, amended 2026-09-23; rate `TELEGRAM_MAX_MSG_PER_MIN/60`, burst 5). A long-poll `getUpdates` holds one token for up to `TELEGRAM_POLL_TIMEOUT_SEC` (≈ 3 calls/min idle), leaving ≈ 17/min for sends. The bot token is part of the URL path, so every log line that can carry an error string or URL passes through `_redact` (token → `***`). A `429` is honoured via `Retry-After` by the limiter; an alert row whose send fails keeps `alerted_at = NULL` and is retried next tick.
- Inline-keyboard `callback_data` stays ≤ 64 bytes (Telegram limit); confirmations that need more arguments carry an 8-character nonce (§8.4).
- **Phone layout (amended 2026-10-20).** The operator reads the bot on a phone, where a `<pre>` block wraps after ≈ 34 monospace characters (measured on the operator's screenshots: 40-character rows wrapped, so every table looked broken). Every `<pre>` line is ≤ 32 characters (`format.PRE_WIDTH`); lists that need no columns (flags, reviews, needs-review buckets, events, insight cards) are plain lines outside `<pre>`, grouped and labelled in words. Screens open with an emoji + bold title and an italic context line (`as_of`, run kind, window).
- **Navigation (amended 2026-10-20).** `/start` and `/menu` open a home screen; every read screen's keyboard ends with `🏠 Home` (`act:home`), so a tap never strands the operator (the old `⬅️ Menu` button opened `/health`'s 57-row job table). Button taps edit the message in place.

### 2.8 Daily market brief (`market_brief_daily`, added 2026-10-23 — spec 20 N3 item 2)

Cron job `market_brief_daily` (§6.1; `MARKET_BRIEF_AT_UTC`, default `06:00`) sends **one plain message** through `telegram.send_message` — never an `events` row, so `alert_dispatch`, routing and `alerted_at` are not involved and nothing pages. It is `ops_exempt` (02 §E1: an informational send that `PAUSE_ALL` must not silence, like the dispatcher). Without a configured token/chat it renders and returns `rows=0` with the note `telegram not configured` (as the digest does). SQLite reads plus 01's `build_health(rt)`; no provider other than `telegram`.

**Slot.** `S` = `feed_meta(spec=SPEC_VERSION).max_as_of` (the newest non-shadow slot; at 06:00Z that is the 00:00Z official slot, scored at 00:15Z). No row for `S` → a two-line message "no scores for {spec} yet". `P = S − 86400` (the comparison slot; `scores` rows at exactly `P`). Composite = `composite_smoothed` everywhere (the feed's `composite`, §3.5).

**Lines** (≤ 20 lines of HTML source, `<pre>` lines ≤ 32 characters per §2.7; sections with nothing to say keep their line and say so):

| # | line | source and rule |
|---|---|---|
| 1 | `🌅 <b>Market brief</b> · 24 Oct` | run date (UTC) |
| 2 | `<i>slot 24 Oct 00:00Z · official · regime DISPERSED</i>` | `S`, its `run_kind`, `scores.regime` of `S` as stored (an `UNKNOWN` is shown, not replaced) |
| 3 | `<i>a risk filter, not a buy signal (13 verdict 23 Oct)</i>` | fixed caption: the first gate verdict (BUILD-LOG 2026-10-23) found no link to 7-day returns and 3–14× more deregistrations in the low tiers |
| 4–9 | `🏆 <b>Top 5</b> by composite` + `<pre>` rows `NNN name·········· T  cc.c .cc!` | `latest_scores(spec, include_insufficient=False)` (confidence ≥ 0.4, active, 02 §H1), first 5; `!` = any red flag |
| 10–15 | `📈 <b>24-h movers</b> · Δ composite` + `<pre>` rows `NNN name········ T  ±d.d cc.c` | current active occupants with a non-null composite at both `S` and `P`; the 5 largest `|Δ|`, ties by netuid; none at `P` → "no slot 24 h earlier" |
| 16 | `🔀 <b>Tiers</b> ↓ 34 B→C · 36 D→F … ↑ 104 D→C` | occupants whose non-null `tier` differs between `P` and `S`; downgrades first (by netuid), then upgrades; at most 8 shown, then `+k`; none → `no change` |
| 17 | `🚩 <b>Flags</b> +2 SN103 owner_extraction, … · −0` | `red_flag_state` (`SPEC_VERSION`, current occupants): raised = `raised_at ∈ (P, S]`, cleared = `cleared_at ∈ (P, S]`; at most 4 names per side, then `+k` |
| 18 | `⚰️ <b>Dereg watch</b> SN70 3.4 d · next reg ≈ 3.4 d` | 19 §2.4 evidence key `days_to_cull_static` (raw, `source='chain'`, written hourly by `wb_dereg`) at its newest `as_of ≤ now`, status `ok`, value < 7, current active occupants, ascending, at most 4 then `+k`; `eta_next_registration_days` from the newest `wb_dereg_network_hourly` row; that row older than 2 h → `(stale, as of HH:MMZ)`; nothing under 7 d → `none within 7 d` |
| 19 | `🩺 30/45 metrics · 0 stale jobs · 0 alerts pending` | populated = scored keys (12's registry, 45) whose `stage='norm'` value at `S` is non-null for at least half of the slot's active scored occupants (20 §1.4's reading); stale = `/health` jobs and tasks with `stale: true`; pending = `/health.alerts.pending_immediate` |

*Why `days_to_cull_static` and not `wb_dereg_network_hourly` (20 N3 named the table):* the network table has one row per hour and no per-subnet column; the per-subnet "time to the next cull line" is 19 §2.4's `days_to_cull_static = eta_next_registration_days + (rank₃₀ − 1) · reg_gap_mean` ("if nothing moves"). On 2026-10-23 17:00Z it selected exactly SN70 (3.4 d, rank 1 of k₃₀ = 4); the next subnet was at 10.5 d.

`python -m app.notifications.brief [--db PATH]` renders the brief from a read-only connection (`mode=ro`) and prints it without sending or writing (verify aid, §10).

---

## 3. Data contract

### 3.1 Tables read (03's DDL is authoritative, 02 §B7; every stored time is INTEGER unix seconds, 02 §B1)

| table | columns used | notes |
|---|---|---|
| `subnets` | `netuid, occupant_key, network_registered_at, name, symbol, status, is_active, immune, owner_coldkey, owner_hotkey, head_block, website_url, github_url, docs_url, discord_url, telegram_url, x_handle, updated_at` | current occupant per netuid with 04's resolved identity columns |
| `occupants` | `occupant_key, netuid, network_registered_at, status, name, first_seen_at, last_seen_at, deregistered_at, predecessor_key, successor_key` | history lookups for deregistered occupants |
| `scores` | `id, occupant_key, netuid, as_of, spec_version, run_kind, status, quality, velocity, composite_raw, composite, composite_smoothed, pillar_scores, pillar_coverage, confidence, insufficient_data, tier, red_flags, regime, explain, computed_at, backfilled, shadow` | 02 §B6: `as_of` = slot instant (00:00Z official, 06/12/18:00Z provisional), `computed_at` = run instant, both ISO only at the boundary; no `observed_at` column. `red_flags` = sorted JSON array of flag **names**, e.g. `["dereg_imminent"]`; multiplier/since/state/origin/detail come only from `explain.flags`. `regime` = label string; `pillar_scores` `{P1..P6: float\|null}`; `explain` per 12 §2.10 |
| `metric_values` | `stage='norm'` rows only (02 §H2): `occupant_key, metric_key, as_of, spec_version, value (= m), value_json {raw, status, reason, raw_as_of, raw_observed_at, age_s, src, fallback, cohort_n}, observed_at`; plus `value_json.evidence_url` of the selected `stage='raw'` row for the evidence link | evidence panel of `/subnets/{netuid}` (§3.5) |
| `red_flag_state` | `occupant_key, flag, spec_version, active, human, sticky, raised_at, false_since, last_eval, detail_json` | `/flags`, and the 409 `flag_not_active` check (02 §D5); `state`/`origin` derived per 12 §2.8 |
| `events` | `id, occupant_key (NULL for system events), netuid, event_type, severity, event_at, observed_at, source, payload, dedupe_key, alerted_at` | `severity ∈ {info, warning, critical}` (02 §B2); `/events`, SSE, digest |
| `collector_runs` | `job, started_at, finished_at, ok, error` | passive service probes (newest ok run per job) |
| `team_members` | `id, occupant_key, netuid, name, role, profile_url, evidence_url, evidence_count, status, source, verified_by, verified_at, rejected_at, reject_reason, first_seen_at, last_checked_at` | `status ∈ {candidate, verified, rejected}`, `source ∈ {subnetalpha, legacy, manual, llm}` (02 §H3); read via 11's `team.list_members`, written only by 11 |
| `manual_notes` | `id, occupant_key, kind, value, body, evidence_url, created_at, created_by` | free text lives in `body`; product status = latest `product_status` row (`value ∈ {none, waitlist, beta, live}`, NULL = cleared); latest `team_disclosure` row (`anonymous`\|`clear`) |
| `manual_flags` | `id, occupant_key, flag, state, evidence_url, note, set_by, set_at` | append-only; latest row per `(occupant_key, flag)` is authoritative; `flag ∈ {owner_extraction, key_compromise, owner_control_change}` (02 §D5) |
| `review_queue`, `owner_event_whitelist` | — | only through 11's `review.list_open` / `owner_admin.*` |
| `control_state` | — | only through 16's `app/control.py` (02 §B12) |
| `web_checks` | `occupant_key, url, kind, is_primary, check_day_ts, status_class, robots_allowed` | `blocked_sites` digest bucket (§8.3) |
| `chain_ticks` | newest per `netuid`: `netuid, block_number, occupant_key, observed_at, deregistration_risk, subnet_moving_price` | `dereg_watch` only |
| `job_state` | `('needs_review_digest', 'last_event_id')` | digest watermark (read + write) |
| `metric_values` (raw), `wb_dereg_network_hourly` | raw `days_to_cull_static` rows; newest network row (`hour_ts, eta_next_registration_days, observed_at`) | market brief dereg line (§2.8; 19 §2.4 owns both) |

### 3.2 Tables written

| table | operation | by |
|---|---|---|
| `manual_notes` | INSERT (`kind ∈ {product_status, note}`; text in `body`; `ref_event_id` on a reoccupation ack) | `manual_service.set_product`, `manual_service.add_note`, registry ack route |
| `manual_overrides` | only via 04's `add_override` / `set_override_status` (04 §3.3) | registry write router |
| `manual_flags` | INSERT (append-only) under 02 §D5 (§3.5) | `manual_service.set_flag` |
| `team_members`, `review_queue`, `owner_event_whitelist`, `manual_notes(team_disclosure)`, override `manual_flags` | only via 11's domain functions (11 §2.12) | manual/review routers, Telegram |
| `control_state` | only via 16's `app/control.py::pause()/resume()` (02 §B12) | control router, Telegram `/pause` `/resume` |
| `events` | INSERT through 03's `emit_event`, `source='api'` (`manual_entry`, `collector_paused`, `collector_resumed`, `rescore_requested`, `dereg_watch`) | manual service, control router, `dereg_watch` |
| `events` | UPDATE `alerted_at` + `payload.alert_route` (03 allows both while `alerted_at IS NULL`) | 01's `alert_dispatch` applying §2.4 (not this spec's code) |
| `job_state` | digest watermark | `needs_review_digest` |
| filesystem | nothing — the app never writes `data/control/*.paused` (02 §B12) | — |

### 3.3 Events emitted by this spec (03 §3.9 names, payload keys and dedupe patterns)

| `event_type` | severity | `occupant_key` | payload | `dedupe_key` |
|---|---|---|---|---|
| `manual_entry` | info | occupant | `{kind: "product"\|"note"\|"flag", by: "api"\|"telegram"\|"cli", row_id: int, summary: str, evidence_url: str}` | NULL |
| `collector_paused` | info | NULL | `{collector: <target>, by, reason}` (+ `members[]`) | NULL |
| `collector_resumed` | info | NULL | `{collector: <target>, by}` (+ `file_pause_active` when an emergency file remains) | NULL |
| `rescore_requested` | info | NULL | `{as_of: int\|null, spec_version, shadow: bool, run_id, by}` | `run_id` |
| `dereg_watch` | warning | occupant | `{netuid, name, reason: "bottom3"\|"safety_metric"\|"immunity_ending", moving_price_rank: int\|null, immune: bool, blocks_left_immunity: int\|null, dereg_safety: float\|null}` | `{occ}\|{reason}\|{day}` |

All with `source='api'` (02 §B9). Team/owner/review actions emit 11's own events (`team_member_*`, `team_disclosure_changed`, `owner_key_compromise_override`), so `manual_entry.kind='team'` is no longer used.

### 3.4 Metric keys produced

None. `/health` and `/api/health/services` are diagnostics, not `metric_values`.

### 3.5 Endpoint catalogue

All timestamps are ISO-8601 UTC with `Z` (`to_iso`, 02 §B1), except inside `explain`, which is passed through verbatim (its `as_of`, `prev.as_of` and `flags[].since` stay INTEGER unix seconds, 12 §2.10). Scores are floats rounded to 2 dp in JSON (pillars and `m` to 3 dp); `null` means "not computable" (§4). Errors are `{"error": "<snake_code>", "detail": "<human>"}` with a 4xx/5xx status. Unknown query params are ignored; booleans accept `1|0|true|false`.

**Route inventory** (the complete HTTP surface; semantics owner in the 4th column — this spec fixes transport, tier and error mapping for all of them):

| method | path | tier | semantics | Phase |
|---|---|---|---|---|
| GET | `/health` | open | 01 builds the body (02 §G6) | 0 |
| GET | `/api/health/services` | open | here (01 ships the Phase-0 probes) | 0 / 11 |
| GET | `/api/v1/jobs` | open | 01 §2.9 | 0 |
| POST | `/api/v1/control/pause/{target}`, `/api/v1/control/resume/{target}` | authed | 16 `app/control.py` (02 §B12) | 0 |
| POST | `/api/v1/control/run/{job_id}`, `/api/v1/control/reload-config` | authed | 01 §2.9 | 0 |
| POST | `/api/v1/control/budget/override` | authed | 16 §2.10 | 0 |
| GET | `/api/v1/registry/occupants`, `/api/v1/registry/{netuid}` | open | 04 §3.3 read API (`active_occupants`, `current_occupant`, `resolved`) | 11 |
| GET | `/api/v1/registry/needs-review` | open | 04 §2.10 `compute_needs_review` | 11 |
| GET | `/api/v1/registry/overrides`, `/api/v1/registry/overrides/{id}` | open | 04 `manual_overrides` (03 DDL) | 11 |
| POST | `/api/v1/registry/overrides` (alias `/api/v1/manual/override`) | authed | 04 §3.3 `add_override` | 11 |
| POST | `/api/v1/registry/overrides/{id}/status` | authed | 04 §3.3 `set_override_status` | 11 |
| POST | `/api/v1/registry/refresh` | authed | 01 §2.9 run-now of 04's `registry_occupants` / `registry_identity` | 11 |
| POST | `/api/v1/registry/reoccupied/{event_id}/ack` | authed | 04 §2.10 reason 2 (`manual_notes.ref_event_id`) | 11 |
| GET | `/api/v1/scores/latest`, `/api/v1/scores/history` | open | here (feed, 02 §H1) | 11 |
| GET | `/api/v1/subnets`, `/api/v1/subnets/{netuid}` | open | here (evidence, 02 §H2) | 11 |
| GET | `/api/v1/events` | open | here | 11 |
| GET | `/api/v1/matrix.csv`, `/api/v1/export/scores.json` | open | here | 11 |
| GET | `/api/v1/stream` | open | here | 11 |
| GET | `/api/v1/quant/results`, `/api/v1/quant/results/{file}` | open | here (files: 13) | 11 |
| GET | `/api/v1/review` | open | 11 `review.list_open` | 11 |
| POST | `/api/v1/review/{id}/resolve` | authed | 11 `review.resolve_review` | 11 |
| POST | `/api/v1/manual/team`, `/team/{id}/verify`, `/team/{id}/reject`, `/team/disclosure` | authed | 11 `team.*` (02 §H3) | 11 |
| POST | `/api/v1/manual/product`, `/manual/note`, `/manual/flag` | authed | here (flag rules 02 §D5) | 11 |
| POST | `/api/v1/manual/owner/whitelist`, `/owner/whitelist/{id}/retire`, `/owner/compromise` | authed | 11 `owner_admin.*` | 11 |
| POST | `/api/v1/control/rescore` | authed | 12 §2.13 rule 5 | 11 |
| GET | `/ui`, `/ui/{path}` | open | here: the static UI (§8.6); every path returns `frontend/ui/index.html`, assets under `/ui/static/*` | 11 |
| GET/POST | `/wb`, `/api/v1/wb/*` | open / authed per route | 19 (02 §K1); served by this app, unchanged here | 3 |

Every route is GET or POST (CORS allows only those, §3.6); 11's draft `DELETE …/whitelist/{id}` is exposed as `POST …/whitelist/{id}/retire`.

**Error body.** Every 4xx/5xx body is `{"error": "<snake_code>", "detail": <str|object>}`. The Phase-0 routes raise `HTTPException(status, detail="<code>")` and 19's raise `detail={"error": …}`; one exception handler (`errors.py`) maps both to that shape and keeps `detail` as given, so existing clients reading `detail` keep working. 422 validation errors carry `error: "invalid_body"` and pydantic's list in `detail`. An unhandled exception → `500 {"error": "internal"}`, logged, no traceback in the body.

#### GET `/health` (open) — built by 01, served here

The body is exactly 01's `/health` builder output (01 §2.9, 02 §G6); this spec adds no field and defines no other shape. Fields this spec's consumers read: `status ∈ {ok, degraded, initializing}`, `degraded_reasons[]` (01's vocabulary), `version`, `kill_switch_active`, `scoring_killed`, `collectors.<job>.{last_run, last_ok, last_error, consecutive_failures, paused}`, `tasks.<task>.{connected, last_beat}`, `spend.<group>.{usd, cap_usd, pct, paused, reserved_usd}` (16's `snapshot()`), `db.{size_bytes, last_backup, last_backup_age_hours}`, `ops.pause_all` (16). Excerpt (illustrative only):
```json
{"status": "degraded", "degraded_reasons": ["collector_failing:github_activity"], "version": "0.1.0+3fa2c1d",
 "kill_switch_active": false, "scoring_killed": false,
 "spend": {"x": {"usd": 3.21, "cap_usd": 25.0, "pct": 0.128, "paused": false, "reserved_usd": 0.0}}}
```
16's deploy gate reads only `status` and `version` and passes on `ok|degraded` (02 §G6). Compact `JSONResponse`; never 500s (01).

#### GET `/api/health/services` (open; cached `HEALTH_SERVICES_CACHE_SEC`)

`{"timestamp": "…Z", "cached": false, "services": {<key>: {"ok": bool, "state": "ok"|"fail"|"disabled", "name": str, "detail": str, "last_check": "…Z", "latency_ms": int|null}}}` — 01's Phase-0 shape (built: `database`, `scheduler`, `telegram`, `disk`, without `state`/`cached`) plus `cached`/`state` and the other eight probes; `ok = (state != "fail")` so a disabled optional provider is not a red light. The whole response is cached for `HEALTH_SERVICES_CACHE_SEC` (`cached: true` on a hit), so the active probes run at most once per window however often the page is opened. Keys follow 01 (provider names). **Active** probes make one cheap request through `Http.request(provider, …)`; **passive** probes make none (they read the limiter snapshot, `/health` and `collector_runs`), so the page never spends TMC/Discord/X budget:

| key | kind | probe | ok when | detail example |
|---|---|---|---|---|
| `database` | local | `PRAGMA journal_mode`; `SELECT MAX(as_of) FROM scores WHERE spec_version = :spec` | both succeed | `"wal · latest slot 2026-09-22T12:00Z"` |
| `scheduler` | local | `rt.scheduler.running` | true | `"41 jobs · 3 unavailable"` |
| `disk` | local | `shutil.disk_usage(DATA_DIR)` | free ≥ `DISK_FREE_MIN_GB` | `"41.2 GB free"` |
| `backup` | local | 16's `latest_backup_age_hours(DB_BACKUP_DIR)` (manifest `taosentinel-*.json`, `created_at`) + `job_state('backup_daily','last_result').mirror` | age ≤ `DB_BACKUP_STALE_HOURS` and mirror ∈ {`ok`, `off`} | `"taosentinel-20260923T033012Z · 8.2 h · mirror ok"` |
| `telegram` | active | `getMe` via provider `telegram` (ported `main.py:2325-2366`) | 200 and `result.username` | `"@taosentinel_bot"`; no token/chat → `disabled` |
| `github` | active | `GET https://api.github.com/rate_limit` (quota-free) | 200 | `"4812/5000 · resets 13:00Z"`; no PAT → `disabled` |
| `subtensor_rpc` | active | JSON-RPC `system_health` to `SUBTENSOR_RPC_URL` | 200, no `error` | `"peers 42 · syncing false"`; `SUBTENSOR_RPC_ENABLED=false` → `disabled` (05 runs keyless on TMC) |
| `anthropic` | active | `GET https://api.anthropic.com/v1/models` (`x-api-key`, `anthropic-version: 2023-06-01`; no token cost) | 200 | `"ok · llm $12.44/$60"`; `LLM_ENABLED=false` → `disabled`; `spend.llm.paused` → `fail` `"budget paused"` |
| `tmc` | passive | `/health.spend.tmc` (request group) + newest ok `collector_runs` row of `tmc_hourly` | not paused and last ok ≤ 2 h | `"month 3,120/90,000 · last ok 14 min ago"` |
| `tmc_sse` | passive | `/health.tasks.chain_sse` | connected and `now − last_beat ≤ 120 s` | `"last block 7,201,155 · 9 s ago"` |
| `discord` | passive | newest ok run of `discord_6h` | ≤ 7 h old | `"last ok 2 h ago"`; `DISCORD_ENABLED=false` → `disabled` |
| `x` | passive | `/health.spend.x` + newest ok run of 09's `x_team_poll` | not paused and ≤ 26 h old | `"twitterapi_io · last ok 4 h ago · $3.21/$25"`; `X_PROVIDER=null` → `disabled` |

Every active probe is `asyncio.wait_for(…, HEALTH_PROBE_TIMEOUT_SEC)` inside `try/except` → `state: "fail", detail: str(exc)`; probes run concurrently (`asyncio.gather(return_exceptions=True)`).

#### GET `/api/v1/scores/latest` (open) — the trading contract (02 §H1; spec 15 consumes it unchanged)

Params: `spec` (default `SCORING_SPEC_VERSION`, the read-only alias of 01's `SPEC_VERSION`, 02 §F2), `min_confidence` (float 0–1, default 0.0), `tier` (`A|B|C|D|F`, optional), `include_prelaunch` (default false), `include_insufficient` (default false — rows with `confidence < 0.4` are dropped, master §7.7, 02 §H1), `netuids` (comma list, optional).

```json
{
  "as_of": "2026-09-22T12:00:00Z",
  "computed_at": "2026-09-22T12:15:07Z",
  "generated_at": "2026-09-22T12:40:03Z",
  "run_kind": "provisional",
  "spec_version": "ts-1.0",
  "ttl_seconds": 25200,
  "stale": false,
  "regime": "TRENDING",
  "count": 118,
  "scores": [
    {"netuid": 64, "occupant_key": "64:4123456", "name": "Chutes", "status": "active",
     "quality": 84.1, "velocity": 71.0, "composite": 82.4, "composite_raw": 81.9,
     "confidence": 0.91, "tier": "A", "red_flags": [],
     "pillars": {"P1": 0.81, "P2": 0.90, "P3": 0.60, "P4": 0.70, "P5": 0.78, "P6": 0.62},
     "as_of": "2026-09-22T12:00:00Z", "computed_at": "2026-09-22T12:15:07Z"},
    {"netuid": 47, "occupant_key": "47:4000001", "name": "Foo", "status": "active",
     "quality": 31.0, "velocity": 22.4, "composite": 18.7, "composite_raw": 30.2,
     "confidence": 0.55, "tier": "D", "red_flags": ["dereg_imminent"],
     "pillars": {"P1": 0.21, "P2": 0.35, "P3": 0.40, "P4": 0.30, "P5": 0.25, "P6": 0.20},
     "as_of": "2026-09-22T12:00:00Z", "computed_at": "2026-09-22T12:15:07Z"}
  ]
}
```
- One row per **current** occupant (`JOIN subnets`): its latest `scores` row for `spec` (max `as_of`, 02 §H1). `as_of` is the slot instant, `computed_at` the run instant (02 §B6); `generated_at` is the response build time.
- `composite` = `composite_smoothed`; `quality` = Health (JSON keeps `quality`, UI/Telegram label it "Health"); `composite_raw` = the unflagged composite. `red_flags` = `scores.red_flags` verbatim: sorted flag **names** (02 §B6); multipliers are on `/subnets/{netuid}`.
- A row may carry null `composite`/`quality`/`tier` (every pillar uncovered, pre-launch); 15 treats that netuid as neutral instead of rejecting the batch (02 §H1).
- `ttl_seconds` = `SCORE_FEED_TTL_SEC` (25,200 = the 6-h cadence plus run latency); `stale = (now − max as_of) > ttl_seconds` **or** `./SCORING_KILLED` exists. When killed, the last good rows are still returned with `stale: true` (master §11).
- Top-level `as_of`/`computed_at`/`run_kind`/`regime` are those of the newest returned row; a per-row `as_of` may be older (an occupant skipped by a provisional run keeps its older row).
- Sorted by `composite` desc, nulls last, then `netuid`.
- No rows (unknown `spec`, or everything filtered) → `200 {as_of: null, computed_at: null, count: 0, scores: [], stale: true, …}` — never 404/503, so the consumer's "absent ⇒ neutral" path handles it (15's `FeedPayload.scores` allows `min_length=0`, 02 §H1).
- Headers `Cache-Control: no-store`, `ETag: W/"<spec>:<max as_of>:<max scores.id>:<params hash>"`; `If-None-Match` → 304 (optional for consumers, 02 §H1).

SQL (`queries.latest_scores`; served by 03's `ix_scores_latest`):
```sql
WITH latest AS (
  SELECT occupant_key, MAX(as_of) AS as_of FROM scores WHERE spec_version = :spec GROUP BY occupant_key
)
SELECT s.id, s.occupant_key, s.netuid, sub.name, s.status, s.as_of, s.computed_at, s.run_kind, s.quality, s.velocity,
       s.composite_smoothed, s.composite_raw, s.confidence, s.tier, s.red_flags, s.pillar_scores, s.regime
FROM scores s
JOIN latest l    ON l.occupant_key = s.occupant_key AND l.as_of = s.as_of
JOIN subnets sub ON sub.occupant_key = s.occupant_key          -- current occupants only
WHERE s.spec_version = :spec
  AND (:include_prelaunch = 1 OR s.status = 'active')
  AND s.confidence >= :min_confidence
  AND (:include_insufficient = 1 OR s.confidence >= 0.4)
  AND (:tier IS NULL OR s.tier = :tier)
ORDER BY s.composite_smoothed IS NULL, s.composite_smoothed DESC, s.netuid;
-- :netuids, when given, appends AND s.netuid IN (…)
```

#### GET `/api/v1/scores/history` (open)

Params: `occupant` (required; an `occupant_key` or a bare netuid → the *current* occupant via `subnets`), `days` (int 1–730, default 90), `spec` (default), `official_only` (default true → one point per day).
```json
{"occupant_key": "64:4123456", "netuid": 64, "spec_version": "ts-1.0", "days": 90,
 "points": [{"as_of": "2026-06-25T00:00:00Z", "computed_at": "2026-06-25T00:15:04Z", "run_kind": "official",
             "composite": 74.2, "composite_raw": 75.0, "quality": 77.9, "velocity": 55.1, "confidence": 0.82, "tier": "B",
             "red_flags": [], "pillars": {"P1": 0.7, "P2": 0.8, "P3": null, "P4": 0.6, "P5": 0.7, "P6": 0.5}, "regime": "CHOPPY"}]}
```
Ascending by `as_of`. 404 `unknown_occupant` if neither form resolves.

#### GET `/api/v1/subnets` (open) — matrix source

Params: `include_prelaunch` (default false), `include_deregistered` (default false), `spec`.
Returns registry ⋈ latest score ⋈ 7-day official history:
```json
{"as_of": "2026-09-22T12:00:00Z", "spec_version": "ts-1.0", "stale": false, "subnets": [
  {"netuid": 64, "occupant_key": "64:4123456", "name": "Chutes", "symbol": "SN64", "status": "active", "immune": false,
   "network_registered_at": 4123456, "links": {"website": "https://chutes.ai", "github": "https://github.com/rayonlabs/chutes", "docs": null, "discord": "https://discord.gg/…", "telegram": null, "x": "chutes_ai"},
   "score": {"composite": 82.4, "quality": 84.1, "velocity": 71.0, "confidence": 0.91, "tier": "A", "red_flags": [], "pillars": {"P1": 0.81, "P2": 0.9, "P3": 0.6, "P4": 0.7, "P5": 0.78, "P6": 0.62}, "as_of": "2026-09-22T12:00:00Z", "insufficient_data": false},
   "spark_7d": [78.1, 78.9, 79.4, 80.2, 81.0, 81.7, 82.4]
  }]}
```
`spark_7d` = the last 7 `official` `composite_smoothed` values ascending by `as_of` (fewer if history is shorter; never padded). `score: null` for occupants never scored. `as_of`/`stale` as in the feed. Insufficient rows are **included** here with `insufficient_data: true` (the UI hides them by default).

#### GET `/api/v1/subnets/{netuid}` (open)

Params: `occupant` (optional `occupant_key`, to view a deregistered predecessor), `events_limit` (default 20, max 100), `spec`.
```json
{"netuid": 64, "occupant_key": "64:4123456", "name": "Chutes", "status": "active", "immune": false, "tenure_days": 412.3,
 "links": {…same as list…}, "owner_coldkey": "5F…",
 "score": {"as_of": "2026-09-22T12:00:00Z", "computed_at": "2026-09-22T12:15:07Z", "run_kind": "provisional", "composite": 82.4, "composite_raw": 81.9,
           "quality": 84.1, "velocity": 71.0, "confidence": 0.91, "tier": "A", "regime": "TRENDING", "pillars": {…}, "coverage": {…},
           "red_flags": [{"flag": "dereg_imminent", "multiplier": 0.6, "since": "2026-09-10T00:00:00Z", "state": "active", "origin": "auto", "human": false, "sticky": false}],
           "explain": { /* verbatim scores.explain */ }},
 "evidence": [
   {"metric_key": "active_authors_28d", "pillar": "P2", "m": 0.95, "raw": 9, "raw_status": "ok", "reason": null,
    "raw_as_of": "2026-09-22T00:00:00Z", "age_s": 43200, "freshness_s": 172800, "fresh": true, "fallback": false,
    "src": "github", "evidence_url": "https://github.com/orgs/rayonlabs/people"},
   {"metric_key": "x_followers", "pillar": "P5", "m": 0.0, "raw": 0, "raw_status": "absent", "reason": "no_x_account", "…": "…"}
 ],
 "events": [{"id": 9912, "event_type": "release_upgrade", "severity": "info", "event_at": "…Z", "observed_at": "…Z", "source": "llm", "payload": {…}}],
 "site": {"url": "https://chutes.ai", "last_event": "site_recovered", "reason": "up", "at": "…Z"},
 "team": [{"id": 3, "name": "Jon Durbin", "role": "founder", "profile_url": "https://x.com/…", "evidence_url": "https://…", "evidence_count": 2, "status": "verified", "source": "manual", "verified_by": "rudolf", "verified_at": "…Z", "last_checked_at": "…Z"}],
 "disclosure": null,
 "product": {"status": "live", "evidence_url": "https://chutes.ai/app", "note": null, "set_at": "…Z", "set_by": "telegram"},
 "notes": [{"id": 7, "text": "…", "evidence_url": "https://…", "created_at": "…Z", "created_by": "api"}],
 "manual_flags": [{"flag": "owner_extraction", "state": "cleared", "evidence_url": "…", "set_at": "…Z", "set_by": "api"}],
 "history_90d": [ /* same shape as /scores/history points */ ]}
```
- `score.red_flags` here are objects built from `explain.flags` (the only store of multiplier/since/state/origin, 02 §B6; `since` converted to ISO); `tenure_days = (subnets.head_block − network_registered_at)·12/86400`.
- **`evidence`** (02 §H2, 12 §2.15 item 8) = the `stage='norm'` rows of the displayed score's slot (`metric_values WHERE stage='norm' AND spec_version=:spec AND occupant_key=:occ AND as_of=:score_as_of`), i.e. exactly what scoring used, in 12's registry order (`app.features.registry.scored("P1")…scored("P6")`; `pillar` = `PILLAR_OF[metric_key]`). `m` = `value`; `raw` = `value_json.raw`; `raw_status` = `value_json.status` (missing → `"none"`); `reason`, `age_s`, `fallback`, `src` = `value_json` keys. `freshness_s` = `METRICS[metric_key].freshness_s` (per metric, not per pillar); `fresh` = `raw_as_of` not null and `now − raw_as_of ≤ freshness_s`. `evidence_url` = `value_json.evidence_url` of the selected raw row (`stage='raw'`, key = `metric_key`, or its `fallback_key` when `fallback`, `as_of = raw_as_of`) when present, else null. Rendering (§4): `raw_status='absent'` → "verifiable absence"; `failed`/`none` → "collection failure"; `not_applicable` → "n/a". A value of 0.0 by itself says nothing about absence.
- `site` = the newest `site_down_7d`/`site_recovered` event of the occupant's primary website (07): for `site_recovered`, `reason = payload.reason` (`up` \| `target_changed` \| `target_retired`, 07 §3.5), shown on the page so a flag cleared because the target changed is not mistaken for a recovery; `null` when neither event exists.
- `team` = 11's `team.list_members(db, occupant_key=…)`; `disclosure` = the latest `manual_notes(kind='team_disclosure')` `{value, set_at, set_by}` or null; `product.note` and `notes[].text` come from `manual_notes.body`.
- 404 `unknown_netuid` when the netuid has no current occupant and no `occupant` param; 404 `unknown_occupant` for an unknown `occupant`.

#### GET `/api/v1/events` (open)

Params: `since` (ISO or relative `7d|24h|30m`, default `7d`), `until` (ISO, optional), `severity` (`info|warning|critical`, comma list; default all), `type` (comma list of `event_type`), `netuid` (int), `occupant` (occupant_key), `limit` (1–1000, default 200), `before_id` (int, keyset pagination).
```json
{"count": 1, "next_before_id": 9912,
 "events": [{"id": 9912, "netuid": 64, "occupant_key": "64:4123456", "name": "Chutes", "event_type": "tier_change", "severity": "info", "source": "scoring",
             "event_at": "…Z", "observed_at": "…Z", "payload": {"from": "B", "to": "A", "composite_smoothed": 76.8}, "alerted_at": "…Z"}]}
```
Ordered by `id DESC` (insertion order ≈ `observed_at`; a total order, so the `before_id` keyset is exact). System events have `netuid: null`, `occupant_key: null`. 422 `bad_since` on an unparsable `since`.

#### GET `/api/v1/matrix.csv` (open) and GET `/api/v1/export/scores.json` (open)

Params: `spec`, `include_prelaunch`, `include_insufficient`. CSV header (fixed order):
```
netuid,occupant_key,name,status,tier,composite,composite_raw,quality,velocity,confidence,P1,P2,P3,P4,P5,P6,red_flags,as_of,computed_at,spec_version
```
`red_flags` is the `;`-joined names. Empty cells for `null`. `Content-Disposition: attachment; filename="taosentinel-matrix-<as_of date>.csv"`.
`scores.json` returns exactly the `/api/v1/scores/latest` body (same params) with `Content-Disposition: attachment`.
Both carry `ETag: W/"<sha256(spec|max(as_of)|max(scores.id)|params)[:16]>"` and `Cache-Control: public, max-age=<EXPORT_CACHE_MAX_AGE_SEC>`; `If-None-Match` match → `304`. Bodies are generated with `csv.writer`/`json` into a `StreamingResponse` (≈ 130 rows; no file is written).

#### GET `/api/v1/stream` (open) — SSE

Params: `types` (comma of `score|event|health`, default `score,event`); header `Last-Event-ID` honoured (also `?last_event_id=` for clients that cannot set it).
Response `text/event-stream`, headers `Cache-Control: no-cache`, `Connection: keep-alive`, `X-Accel-Buffering: no` (ported `main.py:3212-3225`; ACAO left to CORSMiddleware). **Caps:** `STREAM_MAX_CONNECTIONS` (8) concurrent subscribers, else `503 {"error":"too_many_stream_connections"}`; per-subscriber queue 256 frames; a replay is at most `STREAM_REPLAY_MAX` (1,000) rows.

Frames:
```
id: 4182-9912
event: score
data: {"occupant_key":"64:4123456","netuid":64,"as_of":"2026-09-23T00:00:00Z","computed_at":"2026-09-23T00:15:09Z","run_kind":"official","composite":82.4,"quality":84.1,"velocity":71.0,"tier":"A","confidence":0.91,"red_flags":[],"spec_version":"ts-1.0"}

id: 4182-9913
event: event
data: {"id":9913,"netuid":64,"event_type":"tier_change","severity":"info","event_at":"…Z","payload":{…}}

event: health
data: {"status":"ok","degraded_reasons":[],"kill_switch_active":false,"scoring_killed":false,"latest_as_of":"…Z"}   (every STREAM_HEALTH_EVERY_SEC, no id)

: ping                                                                    (every STREAM_HEARTBEAT_SEC)
```
`id` = `<scores.id>-<events.id>`, the pair of rowid watermarks after that frame. The hub polls both tables by rowid (every `STREAM_POLL_SEC` or on `hub.nudge()`) and streams only committed rows (score frames: `spec_version = SPEC_VERSION`, `shadow = 0`). A reconnect with an id replays the rows above both watermarks straight from SQLite, so it also works across a service restart; more than `STREAM_REPLAY_MAX` rows or an unparsable id → one `event: resync` `data: {"reason":"replay_too_large"}` and the stream continues live (the client refetches). A scoring run of ≈ 130 rows arrives as ≈ 130 `score` frames in one poll. The `health` frame = fields of 01's `/health` plus `latest_as_of` (`queries.feed_meta`).

#### GET `/api/v1/quant/results` and `/api/v1/quant/results/{file}` (open)

Lists `QUANT_RESULTS_DIR` (13's key, default `data/quant/results`, cwd-relative like the old explorer, `backtest_api.py:30`): `{"dir": "data/quant/results", "files": [{"file": "factor_survivorship_20260922_101500.json", "kind": "factor", "run_ts": "2026-09-22T10:15:00Z", "size_bytes": 18211, "summary": {"scope": "survivorship", "timestamp": "…"}}], "errors": ["bad.json: Expecting value"]}`.
`kind` from the file-name prefixes 13's code actually writes (`app/quant/report.py::result_path`, `__main__._write_verdict`): `factor_` → `factor`, `walk_forward_` → `walk_forward`, `survival_` → `survival`, `rank_stability_` → `rank_stability`, `gate_verdict_` → `verdict`, and 19's `wb_study_` → `wb_study`; the sidecars `ic_series_*.json` and `trials.json` (and `*.lock`, `*.jsonl`) are not listed (the JSON ones are still fetchable by name); other names are skipped. `run_ts` from the `_YYYYMMDD_HHMMSS.json` suffix (port `_run_ts`, `backtest_api.py:80-86`). `summary` = the file's top-level keys among `scope, timestamp, factor, gate_pass, pending` when present, else `{}` (13 writes no `summary` object). `/{file}` returns the raw JSON; `file` must match `^[A-Za-z0-9_.-]+\.json$` and resolve inside the dir (`Path.resolve().is_relative_to`), else 404. Max 5 MB per file (413 otherwise).

#### POST `/api/v1/manual/*` (authed, router-level)

| path | body (pydantic) | calls | success |
|---|---|---|---|
| `/manual/team` | `{netuid:int, name:str(1..120), role:str(1..60)\|null, profile_url:HttpUrl\|null, evidence_url:HttpUrl\|null, verified:bool=false, note:str\|null}` | `team.add_member(db, netuid=…, name, role, profile_url, evidence_url, evidence_note=note, source='manual', created_by='api', verify=verified)` | 201 |
| `/manual/team/{id}/verify` | `{evidence_urls:[HttpUrl] (1..5), note:str\|null}` | `team.verify_member(db, id, evidence_urls=…, note=…, by='api')` | 200 |
| `/manual/team/{id}/reject` | `{reason:str(1..300), evidence_url:HttpUrl\|null}` | `team.reject_member(db, id, reason=…, evidence_url=…, by='api')` | 200 |
| `/manual/team/disclosure` | `{netuid:int, status:"anonymous"\|"clear", evidence_url:HttpUrl, note:str\|null}` | `team.set_disclosure(db, netuid=…, status=…, evidence_url=…, note=…, by='api')` | 201 |
| `/manual/product` | `{netuid:int, status:"none"\|"waitlist"\|"beta"\|"live"\|null, evidence_url:HttpUrl, note:str\|null}` | `manual_service.set_product` → `manual_notes(kind='product_status', value=status, evidence_url, body=note)`; `null` clears (→ None, 03) | 201 |
| `/manual/note` | `{netuid:int, text:str(1..2000), evidence_url:HttpUrl}` | `manual_service.add_note` → `manual_notes(kind='note', body=text, evidence_url)` | 201 |
| `/manual/flag` | `{netuid:int, flag:"owner_extraction"\|"key_compromise"\|"owner_control_change", action:"confirm"\|"clear", evidence_url:HttpUrl, note:str\|null}` | `manual_service.set_flag` → `manual_flags(state='confirmed'\|'cleared')`, rules below | 201 |
| `/manual/owner/whitelist` | `{netuid:int, reason:<03 whitelist reason>, applies_to:<03 applies_to>, window_start:ISO, window_end:ISO, evidence_url:HttpUrl, change_id:int\|null, old_coldkey:str\|null, new_coldkey:str\|null, note:str\|null}` | `owner_admin.add_whitelist(db, …, window_start=parse_ts(…), window_end=parse_ts(…), by='api')` | 201 |
| `/manual/owner/whitelist/{id}/retire` | `{}` | `owner_admin.retire_whitelist(db, id, by='api')` (sets `retired_at`, never deletes) | 200 |
| `/manual/owner/compromise` | `{netuid:int, window_start:ISO, window_end:ISO, evidence_url:HttpUrl, note:str\|null}` | `owner_admin.override_key_compromise(db, netuid=…, window_start=…, window_end=…, evidence_url=…, by='api', note=…)` | 201 |

**`/manual/flag` rules (02 §D5).** `confirm` only for `owner_extraction` and `key_compromise` (422 `flag_not_human_confirmable`). `clear` for `owner_extraction`, `key_compromise` and `owner_control_change` only while the flag is **active**: `red_flag_state.active = 1` for `(occupant_key, flag, SPEC_VERSION)` (auto-raised, sticky or held), or the latest `manual_flags` row of `(occupant_key, flag)` is `confirmed` (not yet applied by a scoring run); otherwise `409 flag_not_active`. A clear makes 12 set `suppress_until = cleared_at + 30 d`; new trigger ids re-raise immediately (02 §D5). A sticky `owner_control_change` (identity churn) leaves only through such a clear or 11's whitelist. A human-confirmed `owner_extraction_evidence` label (10) is entered as `flag='owner_extraction'`. For `key_compromise` the UI and Telegram prefer `/manual/owner/compromise`, which also whitelists the window (11 §2.10).

**Responses and error mapping.** Success: `{"ok": true, "id": 42, "occupant_key": "64:4123456", "status": "candidate", "warning": null, "event_id": 9931}` (11's `ActionResult` fields; this spec's own writes fill `id`, `occupant_key`, `event_id`). `ActionResult.error` → HTTP (`app/api/errors.py::raise_for_result`; 14's own `flag_not_human_confirmable` → 422, `flag_not_active` → 409):

| `ActionResult.error` | HTTP | body `error` |
|---|---|---|
| `no_current_occupant`, `not_found` | 404 | same |
| `evidence_required`, `invalid_url`, `invalid_kind`, `window_too_long`, `reason_required` | 422 | same |
| `two_source_rule` | 409 | `needs_second_source` (02 §H3; `detail` = 11's `warning`, which names the reason — add a second independent source, e.g. the official team page, and retry) |
| `duplicate_member` | 409 | `duplicate_member` (`id` = the existing row) |
| `invalid_transition` | 409 | `invalid_transition` |
| any other code | 400 | same (logged; a new 11 code must not become a 500) |

`add_member(verify=true)` that fails the two-source rule creates a **candidate** and returns 201 with `warning: "two_source_rule"` (11 §2.2.1). Verification needs ≥ 2 independent sources (`evidence_count ≥ 2`, 03's CHECK) on `profile_url` ∪ evidence. `evidence_url` is mandatory for product, note, flag, disclosure, whitelist and compromise (master §8: evidence-linked manual data). This spec's own writes emit `manual_entry` and call `hub.nudge()`; no route triggers a rescore (the next scheduled run picks rows up).

#### GET `/api/v1/review` (open), POST `/api/v1/review/{id}/resolve` (authed)

GET params `kind` (03's `review_queue.kind`), `netuid`, `limit` (1–200, default 100) → `review.list_open(db, kind=…, occupant_key=…, limit=…)`:
`{"count": 1, "items": [{"id": 17, "occupant_key": "113:5210000", "netuid": 113, "kind": "owner_extraction_candidate", "priority": 1, "summary": "…", "evidence_url": "…", "suggested_flag": "owner_extraction", "created_at": "…Z", "status": "open"}]}`.
POST body `{action: "confirm_flag"|"whitelist"|"key_compromise"|"dismiss"|"done", note: str|null, evidence_url: HttpUrl|null, whitelist_reason: str|null, window_start: ISO|null, window_end: ISO|null}` → `review.resolve_review(db, id, action, by='api', note=…, evidence_url=…, whitelist_reason=…, window=(parse_ts(start), parse_ts(end)) if both else None)`; response and errors as for `/manual/*` (`evidence_required` when `confirm_flag`/`whitelist`/`key_compromise` lack `evidence_url`; `invalid_transition` for an action 11's `_allowed` refuses for that kind, e.g. `confirm_flag` on a `team_candidate`; `dismiss` sets status `rejected`).

#### POST `/api/v1/control/pause/{target}`, `/api/v1/control/resume/{target}` (authed)

`target` = an exact job/task id of 01's `JOB_TABLE` or a group name of 16's `PAUSE_GROUPS` (e.g. `discord`, `x`, `llm`, `notify`) — **never prefix matching** (02 §B12); anything else → 404 `unknown_target`. `alert_dispatch` and `telegram_bot` → 409 `not_pausable` (use `ALERTS_ENABLED` / `TELEGRAM_COMMANDS_ENABLED` + `reload-config`); a group target never pauses an `ops_exempt` job (02 §B12; 16 resolves the members). Body optional `{reason: str (≤ 200), hours: float|null}` (16's `pause(…, hours=)`: `expires_at = now + hours`; null = until resumed). Calls 16's `app.control.pause(target, by="api", reason=…, hours=…)` / `resume(target, by="api")` (built in Phase 0), which write/delete the `control_state` row (the only persistent pause store), update the in-process cache and emit `collector_paused`/`collector_resumed`. The bodies are 16's return values, passed through:
```json
{"paused": true, "target": "x", "jobs": ["x_ctf_crosscheck", "x_kol_poll", "x_mentions_poll", "x_profile_refresh", "x_team_poll"],
 "set_by": "api", "expires_at": null}
{"resumed": true, "target": "x", "removed_row": true, "jobs": […], "file_pause_active": "data/control/x.paused",
 "note": "target stays paused until the operator deletes this file", "still_paused_by": "llm"}
```
Pause is an upsert (a repeat refreshes `set_at`/`reason`); resume is idempotent (`removed_row: false` when there was no row). `file_pause_active` and `still_paused_by` appear only when set (02 §B12): if `data/control/<target>.paused` exists (operator-shell emergency input; the app never writes it) the target stays paused. Pause reasons shown anywhere use 02 §B12's vocabulary `operator | kill_switch | pause_all | budget | emergency_file`.

#### POST `/api/v1/control/run/{job_id}`, `/api/v1/control/reload-config`, GET `/api/v1/jobs`

Semantics 01 §2.9, built in Phase 0 and unchanged here: `run` starts the job's wrapper in a background task (`{started: true, job}`); 404 `unknown_or_unavailable`, 409 `in_flight`, `disabled` or `paused:<reason>`; it is the only way to start `manual` jobs such as 17's backfills (02 §E1). `reload-config` → `{changed: [non-secret keys], changed_secrets: n, restart_required: bool, restart_keys, spec_version}`; `/api/v1/jobs` = every `JOB_TABLE` row with its `JobSpec` fields, availability and live `status`.

#### POST `/api/v1/control/rescore` (authed)

Body `{as_of: "YYYY-MM-DD"|null, spec: str|null, shadow: bool=false}`. 12's built `ScoringService(db).rescore(as_of, spec, shadow)` (12 §2.13 rule 5) is **synchronous**: it runs the whole slot (`as_of=None` → current slot; a date → that day's official slot, reconstructed) and returns the run's `run_id` (`rs-YYYYMMDD-HHMMSS-xxxx`) when done. The route therefore (1) validates and refuses synchronously, then (2) starts the run as a background task and answers at once:
- `spec` (default `SCORING_SPEC_VERSION`) not in 12's `SPECS` → 422 `spec_unknown`; `shadow=true` with `spec = SPEC_VERSION` → 422 `shadow_requires_other_spec`;
- `./SCORING_KILLED` → 423 `scoring_killed`;
- a rescore started by this route still running, or `scoring_6h` running → 409 `rescore_running`;
- the slot already scored for that spec → 409 `slot_already_scored`; the slot older than the spec's latest scored slot → 409 `slot_regresses` (12's `ScoringRefused` codes `slot_already_scored` / `slot_regresses_state`, checked with the same `already_scored` / `latest_completed` helpers).

Response `202 {"accepted": true, "request_id": "rq-20260922-124003-1a2b", "as_of": "2026-09-22T12:00:00Z", "spec_version": "ts-1.1", "shadow": true}`; emits `rescore_requested` with `run_id` = the `request_id` (03's required key and dedupe; the real 12 run id is in `score_runs` with `trigger='rescore'`, and the finished task logs `request_id → run_id`). A refusal raised later inside the task (a race) is logged, not returned.

#### 04's registry routes and 16's budget override

04 ships **no** HTTP routes (04 §8.1: "14 owns routes … and calls the §3.3 domain functions"), so this spec's `app/api/registry.py` implements them over 04's public API; its read router joins `read_mods`, its write router joins `write_mods` (auth + origin guard). 04's `ValueError` → 422 `invalid_override` with the message as `detail`; unknown `netuid`/occupant/id → 404. Before Phase 11 the operator uses 04's CLI (`python -m app.registry review [--json]`).
- `GET /api/v1/registry/occupants?include_pre_launch=0` → `active_occupants(db, include_pre_launch)`; `GET /api/v1/registry/{netuid}` → `current_occupant` + `resolved()` for every 04 `RESOLVED_FIELDS` entry (`{value, source, status, override_id}`) + the netuid's active `manual_overrides`.
- `GET /api/v1/registry/needs-review?reason=&limit=200` → `compute_needs_review(db, now=int(time.time()))` in 04's priority order, filtered by `reason`; items are 04's `ReviewItem` with `since` rendered ISO.
- `GET /api/v1/registry/overrides?netuid=&field=&status=&source=&limit=200` (newest first) and `GET /api/v1/registry/overrides/{id}` → `manual_overrides` rows (times ISO).
- `POST /api/v1/registry/overrides` (and its alias `POST /api/v1/manual/override`) body `{netuid:int|null, occupant_key:str|null (one required; netuid → current occupant), field:str (04 RESOLVED_FIELDS), mode:"fallback"|"force"|"suppress", value:str|null, suppress_source:str|null, suppress_value:str|null, reason:str|null, evidence_url:HttpUrl|null, resolves_event_id:int|null}` → `add_override(db, …, created_by=by)` (`by` = `api`); force/suppress without `reason` + `evidence_url` → 422 (04 validates); unique-index clash → 409 `override_exists` with the existing `id` (04's built `add_override` is `INSERT OR IGNORE` and returns the existing id, so the route compares the row count before and after); 201 `{id}`. Passing `resolves_event_id` closes that `source_disagreement_open` item.
- `POST /api/v1/registry/overrides/{id}/status` body `{status:"active"|"rejected"|"retired"}` → `set_override_status(db, id, status, by='api')`; 200 `{id, status}`. This is the only update: rows are never edited or deleted (03 "status transitions only"), so there is no PUT/PATCH/DELETE (CORS is GET/POST, §3.6).
- `POST /api/v1/registry/refresh` body `{jobs: ["registry_occupants","registry_identity"]}` (default both) → 01's run-now for each id (same code path as `/control/run/{job_id}`); 202 `{started:[…], in_flight:[…], paused:[…]}` (a paused job is reported, not forced).
- `POST /api/v1/registry/reoccupied/{event_id}/ack` body `{note:str(1..2000), evidence_url:HttpUrl|null}` → the event must be a `netuid_reoccupied` row (404 `unknown_event`, 422 `not_reoccupied_event`); inserts `manual_notes(kind='note', occupant_key=payload.new_occupant_key, body=note, evidence_url, ref_event_id=event_id, created_by='api')` (clears 04 §2.10 reason 2); 201; emits `manual_entry` (`kind: "note"`). `POST /api/v1/control/budget/override` (authed, `{group, extra_usd, reason}`): semantics 16 §2.10.

### 3.6 CORS, auth, headers summary

- `CORSMiddleware(allow_origins=CONTROL_ALLOWED_ORIGINS.split(","), allow_methods=["GET","POST"], allow_headers=["Content-Type","X-Auth-Token","Last-Event-ID"])`, no credentials (port `main.py:2171-2179`; `CONTROL_ALLOWED_ORIGINS` is 01's key). The `/ui` pages are same-origin, so CORS matters only for other browser clients (e.g. a page on another port); the τ Terminal calls the API server-side. Whether browsers preflight `Last-Event-ID` on a cross-origin EventSource reconnect is **UNVERIFIED**; allowing the header and accepting `?last_event_id=` covers both cases.
- `require_auth` = 01's verbatim copy `app/api_auth.py` (old `api_auth.py:1-36`: reads `app.config.settings.API_AUTH_TOKEN` live, `hmac.compare_digest` on UTF-8 bytes, empty token = auth off), applied router-wide to every POST router. Startup warns when the token is empty and `HEALTH_HOST` is not loopback (old `main.py:3258-3265`); 16's self-check also flags `tailscale serve` exposure without a token (checklist A8).
- **Origin guard** (`app/api/errors.py::origin_guard`, a dependency on every authed router, 19's POSTs included): a request carrying an `Origin` header → allowed iff that origin is in `CONTROL_ALLOWED_ORIGINS` **or equals the request's own origin** (`scheme://Host`; browsers send `Origin` on same-origin POSTs too, so the `/ui` and `/wb` forms need this); otherwise `403 origin_not_allowed`. CORS hides responses but does not stop a cross-site "simple" POST; with `API_AUTH_TOKEN` empty such a POST would otherwise reach body-less routes like `/control/pause/{target}`. A DNS-rebinding page arrives with its own Host, so same-origin matching admits it — on a loopback/tailnet bind with an empty token that is the residual risk, which is why 16's self-check flags a non-loopback bind without a token (checklist A8). curl, the CLI and the bot send no `Origin` and are unaffected.
- Reads are open by design (single operator, loopback/tailnet); no secret is ever served (no tokens, no `.env` values, `/health` masks per 01).
- `docs_url=None, redoc_url=None`.
- Every response carries `X-TaoSentinel-Spec: <SPEC_VERSION>` for debugging mixed feeds.

---

## 4. Failure modes → response semantics (adapted absence-vs-failure table)

| condition | `/scores/latest` | `/subnets/{n}` | Telegram | rationale |
|---|---|---|---|---|
| occupant never scored | omitted (`count` excludes it) | `score: null` | `/sn`: "no score yet (next official run 00:15Z)" | absence of a run is not a 0 score |
| pillar `P_k` coverage < 0.3 | `pillars.Pk: null` | bar rendered `░░░░░░░░░░ n/a` | same | master §7.1: uncovered pillar is dropped, not zero |
| `quality`/`velocity`/`composite` null | row present with nulls, `tier: null` (15: neutral, 02 §H1) | shown as `—` | `—` | never coerce to 0 |
| `confidence < 0.4` | dropped unless `include_insufficient=1` | shown with `insufficient_data` badge | `/top` hides, `/sn` shows badge | master §7.7, 02 §H1 |
| `scores.status = pre_launch` | dropped unless `include_prelaunch=1`; `composite: null` | pillars P2–P5 shown | `/sn` shows "pre-launch" | master §7.1 |
| evidence `raw_status = 'absent'` | — | value `m` + badge "verifiable absence" (+ `reason`) | `m` + `abs` | absence is a signal (02 §H2) |
| evidence `raw_status ∈ {failed, none}` | — | `m: null`, "collection failure" (+ `reason`) | `n/a` | failure is not (02 §B3: `none` reads as `failed`) |
| evidence `raw_status = 'not_applicable'` | — | "n/a (not applicable)" | `n/a` | e.g. pre-launch P1 |
| `./SCORING_KILLED` | last good, `stale: true` | banner "scoring paused" | `/health` shows `SCORING_KILLED yes` | master §11 |
| `as_of` older than `ttl_seconds` | `stale: true` | banner | — | consumer treats as neutral (15) |
| unknown `spec` / no rows | `200`, `count: 0`, `stale: true` | 404 `unknown_netuid` only if no occupant | "no scores for <spec>" | never a silent switch to another spec |
| DB unavailable | `503 db_unavailable` | 503 | "DB unavailable, try later" | never raise |
| `explain` JSON unparsable | — | `explain: null`, `red_flags: []`, log warning | drivers omitted | never 500 on bad JSON |
| manual clear of an inactive flag | — | 409 `flag_not_active` | `❌ flag_not_active` | 02 §D5 |
| team verify with one independent source | — | 409 `needs_second_source` | `❌ needs a second independent source` | 02 §H3 |
| unknown pause target | — | 404 `unknown_target` | usage + valid targets | 02 §B12 (no prefix matching) |
| resume while an emergency file exists | — | 200, `paused: true`, `file_pause_active` | "stays paused until you delete …" | 02 §B12 |
| unknown `event_type` in `events` | — | listed by `/events` | digest only | 02 §D3 |
| SSE subscriber slow (queue full) | — | — | — | drop oldest, send `resync` |
| Telegram API 429 / 5xx / network | — | — | limiter honours `Retry-After`; alert keeps `alerted_at NULL` | exactly-once eventually (§2.4) |

---

## 5. Configuration

All keys live in `app/config.py` `Settings(BaseSettings)` (spec 01 conventions: UPPER_SNAKE, `.env`, `extra="ignore"`). Consumers read `app.config.settings` dynamically, never a captured reference. Keys owned by this spec (02 §F1: 01's inventory lists them unchanged):

| key | type | default | validator / notes |
|---|---|---|---|
| `UI_BASE_URL` | str | `"http://127.0.0.1:8090/ui"` | deep links in alerts and replies (`{ui}/subnet/64`); set to the tailnet URL (e.g. `https://<host>.<tailnet>.ts.net/ui`) when the UI is opened remotely |
| `SCORE_FEED_TTL_SEC` | int | `25200` | 21600–86400; advertised `ttl_seconds` and the `stale` threshold: the 6-h cadence plus run latency (a 12:15Z run publishes slot 12:00Z; the next slot lands ≈ 18:15Z, 6 h 15 min later) |
| `STREAM_ENABLED` | bool | `True` | `/api/v1/stream` returns 404 when false |
| `STREAM_MAX_CONNECTIONS` | int | `8` | 1–64 |
| `STREAM_HEARTBEAT_SEC` | int | `15` | 5–60 |
| `STREAM_POLL_SEC` | float | `5.0` | 1–60; hub poll interval when not nudged |
| `STREAM_REPLAY_MAX` | int | `1000` | 100–10000; rows replayed for a `Last-Event-ID`, beyond → `resync` |
| `STREAM_HEALTH_EVERY_SEC` | int | `60` | 10–600 |
| `EXPORT_CACHE_MAX_AGE_SEC` | int | `300` | `Cache-Control` on exports |
| `HEALTH_PROBE_TIMEOUT_SEC` | float | `10.0` | per active probe |
| `HEALTH_SERVICES_CACHE_SEC` | int | `60` | probe cache |
| `TELEGRAM_ENABLED` | bool | `True` | master switch for every Bot API call (dispatcher sends, bot, digest); while false, immediate rows keep `alerted_at` NULL and reach the digest late (§2.4 rule 4). There is no direct-send fallback (02 §D3 amended) |
| `TELEGRAM_POLL_TIMEOUT_SEC` | int | `20` | long-poll `getUpdates` |
| `TELEGRAM_MAX_MSG_PER_MIN` | int | `20` | 1–60; rate of 01's `telegram` limiter key (01 builds the bucket from it, burst 5) |
| `TELEGRAM_CONFIRM_TTL_SEC` | int | `300` | lifetime of two-step confirmation nonces |
| `TERMINAL_API_URL` | str | `"http://127.0.0.1:8787"` | §8.4a: the τ Terminal's read API for `/wallet`; validator: an http(s) **loopback** URL or empty (empty = wallet view off). Loopback calls are unmetered in 01's `Http` (no provider bucket) |
| `ALERTS_ENABLED` | bool | `True` | enabled flag of `alert_dispatch` (01 implements); false → rows keep `alerted_at` NULL |
| `ALERT_DISPATCH_INTERVAL_SEC` | int | `60` | 15–600 (`IntervalTrigger(seconds=…)`) |
| `ALERT_MIN_SEVERITY` | str | `"info"` | `info\|warning\|critical`; lower immediate rows → digest (§2.4 rule 4) |
| `ALERT_LATE_AFTER_HOURS` | int | `24` | 1–168; older immediate rows → `digest_late` |
| `ALERT_BATCH_THRESHOLD` | int | `5` | immediate rows per tick above which they are sent batched |
| `ALERT_MAX_ROWS_PER_TICK` | int | `200` | 10–1000 |
| `WATCHLIST_NETUIDS` | list[int] | `[]` | comma-separated in `.env`; validator parses `"31,47,113"` |
| `DEREG_WATCH_ENABLED` | bool | `True` | |
| `DEREG_WATCH_INTERVAL_MIN` | int | `60` | 5–1440 |
| `DEREG_WATCH_SAFETY_MAX` | float | `0.10` | `dereg_safety` threshold |
| `DEREG_WATCH_IMMUNITY_WARN_DAYS` | int | `7` | |
| `NEEDS_REVIEW_DIGEST_ENABLED` | bool | `True` | |
| `NEEDS_REVIEW_DIGEST_DAY` | str | `"mon"` | APScheduler `day_of_week` |
| `NEEDS_REVIEW_DIGEST_HOUR_UTC` | int | `8` | 0–23 |
| `BLOCKED_SITE_DAYS` | int | `7` | digest bucket `blocked_sites` (§8.3) |
| `MARKET_BRIEF_ENABLED` | bool | `True` | daily brief (§2.8) |
| `MARKET_BRIEF_AT_UTC` | str | `"06:00"` | `HH:MM` UTC (01's `hhmm` validator) |
| `FRONTEND_PORT` | int | `3001` | **retired 2026-10-18** (the UI is served at `/ui` on `HEALTH_PORT`); the key stays so existing `.env` files load, nothing reads it |

Consumed, owners in parentheses (not redefined here): `HEALTH_HOST` (01, loopback default), `HEALTH_PORT` (01, 8090), `API_AUTH_TOKEN`, `CONTROL_ALLOWED_ORIGINS` (01; this spec asks its validator to reject `*`), `SPEC_VERSION` (01; `SCORING_SPEC_VERSION` is its read-only alias, 02 §F2), `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `TELEGRAM_ALLOWED_CHAT_IDS`, `TELEGRAM_COMMANDS_ENABLED`, `JOB_FAIL_ALERT_THRESHOLD`, `NETWORK_IMMUNITY_PERIOD_BLOCKS` (01); `KILL_SWITCH_PATH`, `SCORING_KILL_SWITCH_PATH`, `PAUSE_ALL_PATH`, `CONTROL_DIR`, `DB_BACKUP_DIR`, `DB_BACKUP_MIRROR_DIR`, `DB_BACKUP_STALE_HOURS`, `DISK_FREE_MIN_GB` (01/16); `QUANT_RESULTS_DIR` (13); `GITHUB_TOKEN`, `DISCORD_ENABLED`, `X_PROVIDER`, `ANTHROPIC_API_KEY`, `LLM_ENABLED`, `SUBTENSOR_RPC_URL`, `SUBTENSOR_RPC_ENABLED` (05/06/08/09/10; probes); `ALERT_DISPATCH_INTERVAL_SEC`, `ALERT_MIN_SEVERITY`, `ALERT_LATE_AFTER_HOURS`, `ALERT_BATCH_THRESHOLD`, `ALERT_MAX_ROWS_PER_TICK`, `ALERTS_ENABLED` are listed above but already built in Phase 0 (01 reads them). Retired from this spec: `API_HOST`/`API_PORT` (= 01's `HEALTH_HOST`/`HEALTH_PORT`), `STREAM_REPLAY_BUFFER`, `ALERT_DEDUPE_DEFAULT_MIN` (dedupe is 03's `dedupe_key`), `FRONTEND_PORT` (above), and 01's `TELEGRAM_ALERTS_ENABLED` (never built; `ALERTS_ENABLED` is the switch).

**Pause / kill semantics (02 §B12, 16 §2.11).** Operator pauses are `control_state` rows written by 16's `app/control.py` (API, Telegram, CLI); targets are exact job ids or `PAUSE_GROUPS` names, never prefixes; `resume` deletes only the row, and an emergency `data/control/<target>.paused` file (operator shell only; the app never writes it) keeps the target paused — the reply says so. `./KILL_SWITCH` pauses paid jobs and the LLM (free LongTasks keep running); `./PAUSE_ALL` pauses every non-`ops_exempt` job — `alert_dispatch` and `telegram_bot` are `ops_exempt` (02 §E1), so alerts and `/resume` keep working; `./SCORING_KILLED` stops new score rows (12), makes the feed `stale: true` and `/control/rescore` answer 423. Pause reasons shown: `operator | kill_switch | pause_all | budget | emergency_file`. `ALERTS_ENABLED=false` stops the dispatcher; rows keep `alerted_at` NULL and are routed when it is re-enabled (late ones to the digest, §2.4 rule 4) — nothing pages meanwhile (02 §D3).

---

## 6. Cadence, rate limits, cost

### 6.1 Jobs and LongTasks (01's `JOB_TABLE` mirrors these rows verbatim, 02 §E2)

| id | factory (`app.…`) | trigger | UTC time / interval | misfire grace s | timeout s | `ops_exempt` | enabled flag | phase | paid | `min_gap_factor` | liveness (16 §2.5) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `alert_dispatch` | `alerts.dispatch:AlertDispatchJob` — **implemented by 01** (02 §D3), routing = §2.4 | interval `seconds=ALERT_DISPATCH_INTERVAL_SEC` | every 60 s | 120 | 50 | **yes** (02 §E1) | `ALERTS_ENABLED` | 0 | no | 0 | formula: 1·60·2 + 50 + 120 = 290 s |
| `telegram_bot` | `notifications.bot:TelegramBotTask` (`LongTask`) | LongTask | long-poll `getUpdates`, `TELEGRAM_POLL_TIMEOUT_SEC` (20 s) | — | — | **yes** (02 §E1) | `TELEGRAM_COMMANDS_ENABLED` | 11 | no | — | not gated (no declared max-age; 16 §2.5) |
| `dereg_watch` | `notifications.watch:DeregWatchJob` | interval `minutes=DEREG_WATCH_INTERVAL_MIN` | every 60 min | 600 | 120 | no | `DEREG_WATCH_ENABLED` | 11 | no | 0 | 7,440 s |
| `needs_review_digest` | `notifications.digest:NeedsReviewDigestJob` | cron `day_of_week=NEEDS_REVIEW_DIGEST_DAY hour=NEEDS_REVIEW_DIGEST_HOUR_UTC minute=0` | Mon 08:00Z | 3600 | 300 | no | `NEEDS_REVIEW_DIGEST_ENABLED` | 11 | no | 0 | weekly: 1,210,020 s (never trips) |
| `market_brief_daily` (2026-10-23) | `notifications.brief:MarketBriefJob` | cron `MARKET_BRIEF_AT_UTC` | 06:00Z | 3600 | 120 | **yes** | `MARKET_BRIEF_ENABLED` | 11 | no | 0 | daily: 173,040 s (`spec_liveness_max_age`) |

`JOB_TABLE` declares `providers=("telegram",)` for `alert_dispatch`, `needs_review_digest`, `market_brief_daily` and `telegram_bot` and none for `dereg_watch`; `telegram` belongs to no spend group, so nothing here is budget-paused. `alert_dispatch` and `telegram_bot` cannot be paused through a group target (02 §B12) and the control routes refuse them by id (§3.5).

### 6.2 Provider use

| consumer | provider (01 limiter) | rate | budget math |
|---|---|---|---|
| active probes `telegram`, `github`, `subtensor_rpc`, `anthropic` | `telegram`, `github`, `subtensor_rpc`, `anthropic` | 1 request each per `HEALTH_SERVICES_CACHE_SEC` at most, only when someone opens the services page | ≤ 60/h per provider worst case, typically a few per day; `/rate_limit` and `/v1/models` cost no quota/tokens |
| passive probes `tmc`, `tmc_sse`, `discord`, `x` | none | — | 0 (limiter snapshot, `/health`, `collector_runs`); nothing is added to 05's TMC budget table (02 §G3) |
| Telegram (alerts, replies, digest, long-poll) | `telegram` | `TELEGRAM_MAX_MSG_PER_MIN` 20, burst 5 | typical day ≈ 10 alerts + ≈ 20 replies; weekly digest 1–3 messages; ≈ 4,300 long-poll calls/day (free) |
| SSE hub poll | none (SQLite) | every 5 s: 2 indexed `id > ?` queries | negligible |
| `/api/v1/subnets` | none | 3 queries (registry join, latest scores, 7-d window) | < 20 ms on ≈ 130 rows |
| `/ui` pages | none | Matrix refetch every 60 s + on SSE `score` frames; Subnet page 30 s; Events 30 s + on SSE `event` frames; header `/health` dot 30 s | loopback / tailnet |

This spec never builds its own client or bucket: every outbound call is `Http.request(provider, …)` on 01's `ProviderLimiter` (02 §G1). Monthly cost of this component: **$0**.

---

## 7. Failure modes and fail-safe behaviour

- **No handler raises.** Routers wrap DB work in `try/except Exception` → structured error JSON + `logger.error(..., data={"route", "error"})`. uvicorn's default 500 handler is overridden to return `{"error": "internal"}` without a traceback.
- **Telegram bot task** (`TelegramBotTask.run_once` → ported `TelegramBot.run`): with no token/chat id it idles (waits on `stop`, re-checks every 60 s, so a token added via `reload-config` is picked up) instead of returning (which would spin `LongTask`'s reconnect backoff); polling exceptions are logged and retried after `retry_delay_sec`; a handler exception is caught in `_dispatch_command` and answered with `"⚠️ command failed: <exc class>"` — the bot never dies on a bad command. Unauthorised chats are ignored (`telegram.py:271-274`, extended to `TELEGRAM_ALLOWED_CHAT_IDS`). Pending updates are discarded on start (`telegram.py:229-234`). `beat()` on every poll attempt; not liveness-gated (16 §2.5), so a Telegram outage never restarts the service.
- **Alert delivery**: `alert_dispatch` runs under 01's job wrapper; send failures and poisoned rows follow §2.4's delivery rules; the dispatcher is `ops_exempt`, so `PAUSE_ALL` never silences it, and it cannot be paused by id (409 `not_pausable`). There is no "dispatcher inactive → send direct" path (02 §D3 amended); `/health.alerts.dispatcher_active` and `pending_immediate` show a stuck queue.
- **SSE**: per-subscriber `asyncio.Queue(maxsize=256)`; on `QueueFull` the hub drops the oldest frame and enqueues a `resync`. The handler waits on the queue with `asyncio.wait_for(…, STREAM_HEARTBEAT_SEC)` so `request.is_disconnected()` (ported `main.py:3183`) is checked at least every heartbeat. A subscriber registers as the generator's first step and deregisters in its `finally` (old `main.py:3180-3210` counter pattern); the cap is checked against the registered set, so a burst of simultaneous handshakes can overshoot it by at most the burst (harmless on loopback).
- **Last-good semantics**: feed and `/subnets` never fall back to a different `spec_version`; a spec without rows yields an empty response with `stale: true`.
- **Alert thresholds** (other specs, surfaced here): `collector_failing` after `JOB_FAIL_ALERT_THRESHOLD` (5) consecutive failures (01); spend 80 %/100 % (16); backup older than `DB_BACKUP_STALE_HOURS` (36 h, 16).
- **`/health`** is 01's (02 §G6); 16's deploy gate parses `status`/`version` and passes on `ok|degraded`. This spec's consumers never grep the raw body (old `auto-update.sh:161` already matched `"status"` whitespace-tolerantly).
- **Watchdog**: the API task is covered by 16 §2.5 amendments 3–4 (`task_handles["health"].done()` + `/health` self-probe); nothing further here.

---

## 8. Implementation plan

### 8.1 Files

```
app/api/__init__.py
app/api/app.py            create_app(get_runtime=None) -> FastAPI   (one factory; 01 shipped the Phase-0 subset)
app/api/errors.py         ApiError(code, status, detail), raise_for_result(ActionResult), origin_guard, handlers
app/api/common.py         rt_or_503(), db_or_503(), parse bools/lists/relative times, json loads helpers
app/api/queries.py        all SELECTs (pure: (db, …) -> rows; no FastAPI)
app/api/health.py         /health (01's builder), /api/health/services, /api/v1/jobs   (01 Phase 0; 14 adds probes)
app/api/scores.py         /api/v1/scores/latest, /history
app/api/subnets.py        /api/v1/subnets, /{netuid}
app/api/events.py         /api/v1/events
app/api/export.py         /api/v1/matrix.csv, /api/v1/export/scores.json
app/api/stream.py         /api/v1/stream
app/api/hub.py            StreamHub + module nudge()
app/api/quant.py          /api/v1/quant/results[/{file}]
app/api/manual.py         POST /api/v1/manual/* (team/owner routes delegate to 11)
app/api/manual_service.py set_product/add_note/set_flag (shared with Telegram)
app/api/review.py         GET /api/v1/review, POST /api/v1/review/{id}/resolve
app/api/registry.py       /api/v1/registry/* + /api/v1/manual/override alias over 04 §3.3 (read + write routers, §3.5)
app/api/control.py        POST /api/v1/control/*   (01 Phase 0: pause/resume/run/reload-config; 14 adds rescore)
app/api/ui.py             GET /ui, /ui/{path} → frontend/ui/index.html; /ui/static/* assets
app/alerts/routing.py     ROUTES (§2.4) + route_for()        — content owned here, shipped by 01 in Phase 0 (02 §D3)
app/alerts/templates.py   render(row, *, link) (§8.5)        — same
app/notifications/format.py     esc, chunk, fmt_age (+ pre_table, bar added here)  — same
app/notifications/telegram.py   01's send_message/send_alert/get_me + the ported TelegramBot (§8.4)
app/notifications/commands.py   CommandDispatcher, build_dispatcher(get_runtime) + render_* formatters
app/notifications/bot.py        TelegramBotTask(LongTask) wrapping TelegramBot.run
app/notifications/watch.py      DeregWatchJob
app/notifications/digest.py     NeedsReviewDigestJob, render_digest()
app/notifications/brief.py      MarketBriefJob, render_brief() + `python -m app.notifications.brief` (§2.8, 2026-10-23)
frontend/ui/index.html, app.js, style.css   (§8.6)
tests/api/…, tests/notifications/…  (§9)
```
Not in this spec: `app/api_auth.py` (01's re-implementation of the old `api_auth.py`), `app/alerts/dispatch.py` (01), `app/notifications/alerts.py` (`AlertLatch`, 01), `app/control.py` (16), `app/integrity/{team,owner_admin,review}.py` (11), `app/registry/review.py` + `app/registry/overrides.py` (04's domain functions; 04 ships no router), `app/api/wb.py` + `frontend/wb/` (19).

### 8.2 Factory and injection

```python
# app/api/app.py
def create_app(get_runtime: Callable[[], Runtime | None] | None = None) -> FastAPI:
    if get_runtime is None:
        from app.runtime import get_runtime_or_none          # 01's runtime calls create_app() without arguments
        get_runtime = get_runtime_or_none
    app = FastAPI(title="TaoSentinel", docs_url=None, redoc_url=None, openapi_url=None)
    app.add_middleware(CORSMiddleware, allow_origins=_origins(), allow_methods=["GET", "POST"],
                       allow_headers=["Content-Type", "X-Auth-Token", "Last-Event-ID"])
    read_mods  = (health, scores, subnets, events, export, quant, review.read, registry.read, ui)
    write_mods = (manual, review.write, control, registry.write)
    for mod in {*read_mods, *write_mods, stream, wb}:
        mod.configure(get_runtime)
    for mod in read_mods:
        app.include_router(mod.router)                                             # open read tier
    if settings.STREAM_ENABLED:
        app.include_router(stream.router)                                          # open read tier
    for mod in write_mods:
        app.include_router(mod.router, dependencies=[Depends(require_auth), Depends(origin_guard)])   # authed
    if settings.WB_ENABLED:
        app.include_router(wb.router)                    # 19: GETs open, POSTs authed + origin-guarded per route
    errors.install(app)                                  # ApiError / HTTPException / validation / 500 handlers
    app.middleware("http")(_spec_header_middleware)      # X-TaoSentinel-Spec
    return app
```
`review` and `registry` each expose a `read` and a `write` router (`review.read_router`, `review.write_router`). The `StreamHub` is created lazily by the first `/api/v1/stream` subscriber, bound to the runtime's DB, and stopped when its last subscriber leaves; `hub.nudge()` is a module function that wakes it if it exists. 01's `run_health_server` serves the app.
### 8.3 Queries (signatures)

```python
async def latest_scores(db, *, spec: str, min_confidence: float = 0.0, tier: str | None = None,
                        include_prelaunch: bool = False, include_insufficient: bool = False,
                        netuids: list[int] | None = None) -> list[dict]
async def feed_meta(db, *, spec: str) -> dict            # {as_of, computed_at, run_kind, regime, max_id} (INTEGER times)
async def score_history(db, *, occupant_key: str, spec: str, days: int, official_only: bool) -> list[dict]
async def resolve_occupant(db, ident: str | int) -> tuple[str, int] | None   # (occupant_key, netuid)
async def subnet_rows(db, *, spec: str, include_prelaunch: bool, include_deregistered: bool) -> list[dict]
async def spark_7d(db, *, spec: str) -> dict[str, list[float]]
async def subnet_detail(db, *, occupant_key: str, spec: str, events_limit: int) -> dict
async def evidence(db, *, occupant_key: str, spec: str, as_of: int) -> list[dict]   # stage='norm' rows of that slot (02 §H2)
async def site_state(db, *, occupant_key: str) -> dict | None     # newest site_down_7d / site_recovered (+ payload.reason)
async def events_query(db, *, since: int, until: int | None, severities: set[str] | None,
                       types: set[str] | None, netuid: int | None, occupant_key: str | None,
                       limit: int, before_id: int | None) -> list[dict]
async def active_flags(db, *, spec: str) -> list[dict]   # red_flag_state active=1 ⋈ subnets; multiplier = catalog.RED_FLAGS
async def flag_is_active(db, *, occupant_key: str, flag: str, spec: str) -> bool   # 02 §D5 test (§3.5)
async def movers(db, *, spec: str, days: int, n: int) -> tuple[list[dict], list[dict]]
async def needs_review(db, *, now: int) -> dict[str, list[dict]]            # buckets below
async def digest_events(db, *, after_id: int, upto_id: int) -> list[dict]  # §2.6 section 2
async def latest_ticks(db, *, max_age_s: int = 1800) -> dict[int, dict]    # dereg_watch + moving_price_rank
```
`needs_review` buckets: first, 04's list — `needs_review` calls `await app.registry.compute_needs_review(db, now)` (04 §2.10/§3.3: `(db, now: int) -> list[ReviewItem]`, items `{occupant_key, netuid, name, reason, detail, since, event_id}`) and merges it as one top-level bucket per 04 reason, key `registry.<reason>` (`registry.new_occupant`, `registry.reoccupied_unreviewed`, `registry.source_disagreement_open`, `registry.abandonment_marker`, `registry.name_turnover`, `registry.identity_void`, `registry.chain_identity_missing`, `registry.legacy_needs_review`, `registry.never_activated`, `registry.unresolved_watch_target`, `registry.leaked_secret`), inserted in 04's priority order before 14's own buckets; empty reasons are omitted and a raising call yields a single `registry.error` bucket (the digest still sends); then `queue` = 11's `review.list_open(db, limit=500)` grouped by kind (includes 10's `llm_flag_candidate`); `team_candidates` = active occupants with `team_members.status='candidate'` rows (count per netuid); `team_unknown` = active occupants whose latest `team_doxxed` norm row has `raw_status ∈ {failed, none}` (11 §2.2.6's None reasons); `product_unset` = active occupants with no `manual_notes(kind='product_status')` row, or whose latest one has `value` NULL (07's manual product override is missing, so `product_status` scores None); `blocked_sites` = active occupants whose primary website (`web_checks.kind='website', is_primary=1`) has a row on each of the last `BLOCKED_SITE_DAYS` (7) check days and every one of them is `status_class='bot_challenge'` (07: 403/503 challenge or 429) or `robots_allowed = 0` — the collector can never see the site, so the operator should set a manual product status or docs override (07 §7).

### 8.4 Telegram port

Source: the old bot's `app/notifications/telegram.py` from the Pi recovery (`~/USB_Recovery/rootfs/home/pi/Desktop/SN_Bot/SubnetTrader/app/notifications/telegram.py`, HEAD 2026-08-09; the Windows copy `E:\SN_Trader\…` the line numbers below were checked against is not on the laptop, so the port goes by function name). 01 shipped only `send_message`/`send_alert`/`get_me`/`strip_html` in Phase 0 (BUILD-LOG Phase 0 item 10); this phase adds `TelegramBot` to the same module and changes, relative to the old class:
- `TelegramCommandHandlers` → a `CommandDispatcher` object (below); the old `TelegramDocument`/`send_document` (CSV export) are dropped — exports are `/api/v1/matrix.csv`;
- keyboards → `main_menu_keyboard()` (home: Top · Flags · Movers / Wallets · Health · Reviews / Spend · To do · Digest), `top_keyboard(netuids, everything, offer_all)`, `sn_keyboard(netuid)`, `flags_keyboard(netuids)`, `movers_keyboard(days, netuids)`, `health_keyboard(all_jobs)`, `confirm_keyboard(yes_data, no_data)`, `back_keyboard(*extra)` (extra buttons + `🏠 Home`), and the wallet keyboards of §8.4a;
- `_dispatch_callback` and `_dispatch_command` → table-driven dispatch (below); the old screen helpers (`_is_paused`, `_open_positions_list`, `_send_status`, `_render_screen`, `_render_close_confirm`) are dropped;
- `_set_my_commands` → new command list;
- `_handle_update`/`_handle_callback` chat check → accept any id in `TELEGRAM_ALLOWED_CHAT_IDS` (comma list; empty → `TELEGRAM_CHAT_ID`, 01); replies go to the chat the update came from;
- `_post`/`_url`/`_fetch_updates` → the same logic over `get_http().request("telegram", …)` (02 §G1; `getUpdates` with `timeout=TELEGRAM_POLL_TIMEOUT_SEC + 10, retries=0`) instead of a private `httpx.AsyncClient`; a non-`ok` body raises `RuntimeError` with the token redacted.
Keep: `run` (the poll loop: `_set_my_commands`, `_discard_pending_updates`, loop until `stop`), `send_message`/`edit_message`/`answer_callback`, `_discard_pending_updates`/`_fetch_updates` offset handling, `_handle_callback`, `_emit` (edit in place for a button tap, else send), `_toast`, `_redact`, and the "answer the callback first" rule. `TelegramBotTask.run_once(stop)` builds the bot from `commands.build_dispatcher(lambda: self.rt)`, marks itself connected, awaits `bot.run(stop)` and calls `beat()` after every poll attempt (the bot takes an `on_poll` callback).

```python
class CommandDispatcher:                       # app/notifications/commands.py; build_dispatcher(get_runtime)
    async def on_command(self, bot, chat_id: str, text: str) -> None      # shlex parse → c_<command>(bot, chat_id, args)
    async def on_callback(self, bot, chat_id, message_id, callback_id, data) -> None   # answers the callback first
    pending: dict[str, Pending]                # nonce → parsed action, TELEGRAM_CONFIRM_TTL_SEC, single use
# pure render functions shared with the digest and tests (all read app/api/queries.py):
render_home(rt) · render_top(db, n, tier, everything=False) -> (text, netuids, offer_all) · render_sn(db, netuid)
render_events(db, netuid) · render_flags(db) -> (text, netuids) · render_movers(db, days) -> (text, netuids)
render_health(rt, all_jobs=False) · render_spend(rt) · render_review(db, kind) · render_needs_review(db) · render_digest_events(db, a, b)
```
The old injected handler dataclass is replaced by this dispatcher object (the bot calls `on_command`/`on_callback`); the bot catches any handler exception and answers `⚠️ command failed: <exc class>`.

Dispatch table (`_dispatch_command`, `shlex.split` kept so quoted names work; every reply ≤ 4,096 characters via `chunk`):

| command | args | handler | reply |
|---|---|---|---|
| `/start`, `/menu`, `/home` | — | `render_home` | home screen + `main_menu_keyboard` |
| `/help` | — | `HELP` | command list (Look / Act) |
| `/top` | `[n=10] [tier]` | `top(n, tier)`; n clamped 1–50 | table + `top_keyboard`; when no row reaches confidence 0.4, a diagnosis + `👀 Show anyway` (`act:top:all`) |
| `/wallet`, `/wallets` | `[name\|coldkey\|all]` | §8.4a | wallet list, or that wallet's overview |
| `/jobs` | — | `render_health(rt, all_jobs=True)` | every enabled job, problems first |
| `/sn`, `/subnet` | `<netuid>` | `sn(netuid)` | card + keyboard `[Refresh] [Events] [Back]` |
| `/flags` | — | `flags()` | grouped by flag + SN buttons |
| `/movers` | `[days=7]` (1–90) | `movers(days)` | table + `1d/7d/30d` buttons; an empty window says why |
| `/health`, `/status` | — | `health()` | summary + jobs needing attention + `health_keyboard` |
| `/spend` | — | `spend()` | one row per group with a bar |
| `/team` | `add <netuid> "<name>" "<role>" <profile_url>` · `verify <id> <url> [<url2>]` · `reject <id> "<reason>"` · `list <netuid>` · `disclosure <netuid> anonymous\|clear <url>` | `team(args)` → 11's `add_member` (candidate, `source='manual'`) / `verify_member` / `reject_member` / `list_members` / `set_disclosure`, all `by='telegram'` (02 §H3) | confirmation line / list (≤ 30 rows) |
| `/product` | `set <netuid> <none\|waitlist\|beta\|live> <url>` | `product_set` | confirmation |
| `/flag` | `confirm\|clear <netuid> <flag> <url>` | two-step (nonce) → `flag` (02 §D5 rules) | confirmation |
| `/owner` | `whitelist <review_id> <reason> <url>` · `compromise <netuid> <from_iso> <to_iso> <url>` | two-step → `review.resolve_review(…, 'whitelist', whitelist_reason=reason, evidence_url=url)` / `owner_admin.override_key_compromise(…)` | confirmation |
| `/review` | `[kind]` · `resolve <id> confirm_flag\|dismiss\|done [<url>] ["<note>"]` | `review.list_open` / `review.resolve_review` (two-step for `confirm_flag`) | list (≤ 20 rows) / confirmation |
| `/pause`, `/resume` | `<target> [hours]` (job id or `PAUSE_GROUPS` name; `hours` only for pause) | unknown target → usage + the valid group names; else two-step: `confirm_keyboard("do:pause:<target>", "no:x")`; the callback → 16's `app.control.pause(target, by='telegram')` / `resume(…)` | edited message + toast (+ emergency-file note on resume); a pause with `hours` confirms through a nonce (`do:x:<nonce>`), the plain form through the stateless `do:pause:<t>` / `do:resume:<t>` |
| `/needsreview` | — | `needsreview()` | digest section 1 |
| `/digest` | — | `digest()` | digest sections 2–3 (watermark unchanged) |
| unknown | | | `Unknown command. /help` |

Callback data (≤ 64 bytes; job ids are ≤ 30 chars, so `do:resume:<t>` fits): `act:home`, `act:top`, `act:top:all`, `act:flags`, `act:movers`, `act:mv:<days>`, `act:health`, `act:jobs`, `act:spend`, `act:review`, `act:needs`, `act:digest`, `w:…` (§8.4a), `sn:<netuid>`, `ev:<netuid>`, `do:pause:<t>`, `do:resume:<t>`, `no:x` (cancel), `do:x:<nonce>` (confirm/cancel a parsed `/flag`, `/owner` or `/review resolve … confirm_flag`; the 8-character nonce maps to the parsed action in memory for `TELEGRAM_CONFIRM_TTL_SEC`, single use; a restart drops pending nonces and the operator re-issues the command). `answer_callback` is always called first (ported comment `telegram.py:300-303`).

### 8.4a Wallet view (`/wallet`, added 2026-10-20 at the operator's request)

The operator asked for the bot to show "a specific wallet, performance, insights, current positions, PnL". Wallets live in the **τ Terminal** (19 §0.1b, §2.9): it holds the local and watched coldkeys, the synced TaoStats trade history, average-cost positions with `sim_swap` exit values, and the Insights trader score. TaoSentinel still never reads a wallet (19 §1): `app/notifications/wallet.py` renders the terminal's own read API and stores nothing.

- **Source:** `GET {TERMINAL_API_URL}/api/wallets`, `/api/positions?wallet=`, `/api/insights?wallet=&period=`, `/api/trades?wallet=` through `Http.request("terminal", …)` (loopback, unmetered; 30-s timeout, no retries). `wallet` = a name, a coldkey or `all` (the terminal's `wallets.resolve`). Read-only: the bot never calls the terminal's POST routes.
- **Join:** beside held subnets, TaoSentinel's own latest score row (any confidence: tier, composite, confidence) and active `red_flag_state` flags. Nothing else crosses.
- **Money rules** (operator, 2026-10-16): τ figures are balances, PnL or ratios. Never a total summed over recycled capital — no "volume"/"invested" figure; the terminal's `Volume` stat is not shown.
- **Failure:** empty `TERMINAL_API_URL` → "wallet view is off"; no answer → "τ Terminal did not answer at … · `systemctl --user start tao-terminal`" with `🔁 Retry`. Never a stack trace.
- **Callbacks:** `w:l` (list) and `w:<view>:<arg>:<ref>`; `ref` = the wallet name when ≤ 40 bytes without `:`, else its coldkey (≤ 64 bytes total); views `o` overview, `p` positions, `s:<netuid>` one position, `f:<1|7|30|365>` performance, `i:<page>` insights, `t:<page>` trades.

Screens (examples from the live terminal on 2026-10-20, Main watch wallet):
```
👛 Wallets                              (w:l; one button per wallet + 👥 All wallets)
🔑 squiddy · trading wallet  /  0.05 τ · no positions
👁 Main · watch-only  /  94.05 τ · 8 positions

👁 Main · watch-only                     (w:o: overview)
5CrWGu…2abp · 1,452 trades since Feb 2025
💰 95.00 τ  ≈ $26,285
94.30 τ if sold now · 0.29 τ free · 5 positions + 3 dust
<pre>open        +9.29 τ  +11.0%  /  realised  −122.22 τ  /  total  −112.94 τ</pre>
open includes 13.27 τ of emission alpha · 2.739 τ paid in fees
Biggest positions: • SN71 Leadpoet · 55.92 τ · +12.0% …
[📂 Positions] [📊 Performance] / [💡 Insights] [🧾 Trades] / [🔁] [👛 Wallets] [🏠]

📂 Main · 5 positions                    (w:p: τ = exit value, PnL = unrealised at exit / cost, T = Sentinel tier)
 SN name      T      τ    PnL
 71 Leadpoet  C  55.92 +12.0%
  0 Root      -   0.94  +0.0%
🪙 dust: SN20, SN127, SN44   (exit value < 0.01 τ)
🚩 SN127 Astrid: dereg_imminent

📌 SN71 · Leadpoet · Main                (w:s:71: held α, entry and now τ/α (+ % vs entry), value, if sold,
                                          open PnL, realised, emission α, buys/sells, first/last trade,
                                          🛰 Sentinel tier · composite · conf, flags)
📊 Main · performance                    (w:f:30: trader score + grade vs the previous window, 10-cell bars per
                                          part with Δ, 20-point score trend, realised in the window over n sells,
                                          lifetime trades / win rate / PnL / fees, realised-PnL sparkline,
                                          best/worst month, deepest hole, typical hold, the weakest part's tip;
                                          buttons 1D 1W 1M 1Y)
💡 Main · insights                       (w:i:<page>: persona on page 1, then the terminal's cards 6 per page,
                                          minus the three score cards already on Performance)
🧾 Main · trades                         (w:t:<page>: newest first, 15 per page, a year divider,
                                          `20 Oct sell   0 Root       9.98`; liq = deregistered, (old) = earlier occupant)
```

### 8.5 Reply formats (exact)

Read screens (rebuilt 2026-10-20 for a phone, §2.7; examples from the live DB on 2026-10-20). Bars are 10 cells (`format.cells`), `█` × round(v·10), `░` rest.

Home (`/start`, `/menu`, `act:home`):
```
🛰 <b>TaoSentinel</b> · 🟡 degraded
<i>20 Oct 22:20Z · up 40m · ts-1.0</i>
⚠️ <code>chain_probe_stale</code>

📊 <b>Scores</b> 20 Oct 18:00Z · provisional
      🟢A 5 🔵B 24 🟡C 47 🟠D 29 🔴F 15 · 120 subnets      (or: ⚠️ low confidence: 0 of 126 pass 0.4)
🚩 <b>11</b> red flags · 🗂 <b>28</b> open reviews
💸 spend 2% of the fullest cap
```
`/top 10` (row = `#`2 `SN`3 `name`11 `T`1 `comp`5.1 `conf`3 + `!` when flagged = 31 chars):
```
🏆 <b>Top 10 subnets</b>
<i>ts-1.0 · 20 Oct 18:00Z · provisional</i>
<pre>
 #  SN name        T  comp conf
 1  51 lium.io     A  84.3 .63
 6  19 blockmachi… B  71.4 .58!
</pre>
🟢 A 5 · 🔵 B 24 · 🟡 C 47 · 🟠 D 29 · 🔴 F 15
<i>! = red flag · 3 hidden (confidence &lt; 0.4)</i> · <a href="…/ui/matrix">matrix</a>
```
When no row reaches 0.4: `⚠️ None of the 126 scored subnets reaches confidence 0.4 in this run (median .09).` + `Missing for most subnets: Chain, Builder, Product, Community.` (pillars null in > half the rows) and a `👀 Show anyway` button (`act:top:all`: every row, `incl. low-confidence rows`). The old reply "No scores for ts-1.0 yet" was wrong in that case (2026-10-20: 128 rows existed at 18:00Z, all below 0.4).

`/sn 64`: title `🛰 <b>SN64 · Chutes</b>`, `<code>64:4531295</code> · active`, then `🟢 <b>80.5</b> composite · tier <b>A</b>` (+ `· ⚠️ low confidence` when `insufficient_data`; `🌱 pre-launch (no composite)` for pre-launch), `<i>health 90.3 · velocity 50.0 · conf .20 · 20 Oct 18:00Z</i>`, a `<pre>` of six pillar rows `Chain     ████████░░ .81` (`null` → `░░░░░░░░░░ n/a`), `👍` top_positive[:3] / `👎` top_negative[:3] (`metric +impact`), one line per flag `⚰️ <code>dereg_imminent</code> ×0.60 since 18 Oct (auto)` or `✅ no red flags`, `<b>Recent events</b>` (3 × `ℹ️ <i>20 Oct</i> <code>type</code> summary≤40`), `🔗 web · github · discord · x · open`. Keyboard `[📜 Events] [🔁 Refresh] / [🏆 Top] [🏠 Home]`.

`/flags` (from `red_flag_state`, `active = 1`, current `SPEC_VERSION`), grouped by flag, most severe multiplier first:
```
🚩 <b>Red flags · 11 active</b>
<i>as of 20 Oct 18:00Z</i>

⚰️ <b>dereg_imminent</b> ×0.60 · 5
<i>in the deregistration zone · since 18 Oct</i>
SN16 Zero Hour · SN40 Ralph · SN47 GPUForge · SN70 Consted 2.0 · SN127 Astrid
…
<i>✋ set by a human · 📌 sticky · ×multiplier on the composite</i>
```
Per subnet: ` ✋` iff `human = 1`, ` 📌` iff `sticky = 1`, ` (clearing)` iff `false_since` is set; `since` is shown when the group shares one day. Icons and words per flag in `commands.FLAG_INFO`.

`/movers 1` (row = `SN`3 `name`10 `from`5 `to`5 `Δ`5 = 32 chars):
```
📈 <b>Movers · 1d</b>
<i>official composite, now vs then</i>
<pre>
▲ up            from    to     Δ
126 Attelier    43.5  47.7  +4.2
▼ down          from    to     Δ
 38 ChronoLLM   53.0  48.0  -5.1
</pre>
```
Empty window: `Not enough history: the first official score is from 19 Oct, less than 7 days ago. Try a shorter window.` (`queries.first_official_as_of`).

`/health` (from 01's `/health`): `🩺 <b>Health</b> · 🟡 degraded`, `<i>up 40m · v0.1.0+33ffd7a · ts-1.0</i>`, reasons as `<code>`, then `📊 scores 18:00Z · official 10-20 00:00Z`, `💾 db 5.1 GB · backup 1.6 h ago`, `🔌 kill switches off` (or `🛑 KILL_SWITCH ON`), `⚙️ 44 jobs · 14 ok · 30 not run yet · 1 failing`, and only the jobs that need attention:
```
<b>Needs attention</b>
<pre>
✗ github_activity      failed ×5
‖ x_team_poll          paused bu…
</pre>
```
(or `✅ every job is healthy`). `📋 All jobs` (`act:jobs`, `/jobs`) lists every enabled job as `✓ alert_dispatch            42s` (mark, id ≤ 22, age), problems first; legend `✓ ok · ✗ failing · ! stale · ‖ paused · · not run since boot`.

`/spend` (one row per group of `/health.spend`; groups with `cap_usd = 0` listed once as off):
```
💸 <b>Spend · October 2026</b>
<pre>
llm        $0.00/$60 ░░░░░░   0%
tmc         1.5k/90k ░░░░░░   2%
</pre>
<i>off (cap 0): taostats</i>
```
A paused group adds a line `⏸ <paused_reason>`.

`/team add 64 "Jon Durbin" "founder" https://x.com/jondurbin` → `✅ team_members #3 added for SN64 (64:4123456) · candidate · profile: https://x.com/jondurbin`. `/team verify 3 https://chutes.ai/team` → `✅ #3 Jon Durbin verified · sources: x.com, chutes.ai` or `❌ needs a second independent source (found: x.com) — add e.g. the official team page`. Usage errors reply the command's usage line.
`/product set 64 live https://chutes.ai/app` → `✅ SN64 product_status = live (was beta)`.
`/flag confirm 113 owner_extraction https://…` → confirm keyboard → `🚩 SN113 owner_extraction CONFIRMED · takes effect at the next scoring run (≤ 6 h)`; `/flag clear 9 owner_control_change https://…` → `✅ … cleared (30-d suppression)` or `❌ flag_not_active: owner_control_change is not active for SN9`.
`/pause x` → confirm keyboard → toast `⏸ x paused by telegram (5 jobs)`; `/resume x` with `data/control/x.paused` present → `⚠️ x still paused: delete data/control/x.paused`.
`/review` (grouped by kind, lowest priority number first; ≤ 30 ids per kind):
```
🗂 <b>Open reviews · 28</b>

<b>owner_extraction_candidate</b> · 27 · priority 1 · oldest 2h28m
#2 SN1 · #3 SN5 · #4 SN6 · …
<i>Resolve:</i> <code>/review resolve &lt;id&gt; dismiss|done|confirm_flag [url]</code>
<i>Whitelist owner:</i> <code>/owner whitelist &lt;id&gt; &lt;reason&gt; &lt;url&gt;</code>
```
`/needsreview` (also section 1 of the weekly digest): `🧭 <b>Needs review · 20 Oct</b>` then one bullet per bucket, `• <b>new occupants</b> (2): SN86, SN103`. Each bucket lists at most 12 items then `…`. 04's buckets render in 04's priority order, labelled from the reason (`registry.new_occupant` → "new occupants", `registry.source_disagreement_open` → "source disagreements", …; unknown reasons render verbatim). `/digest`: `📬 <b>Digest events (n)</b>`, one bold group per event type.

**Alert templates** (`app/alerts/templates.py`; `{ui}` = `UI_BASE_URL`; `{name}` from `subnets`; every value `esc()`-ed; a missing payload key renders as `?` via `format_map`, so a payload variant never breaks a send). Icons by severity for the generic form: 🛑 critical, ⚠️ warning, ℹ️ info.
- generic (any type without its own line; the only form 01 needs on day one): `{icon} <b>{event_type}</b>{ · SN{netuid} {name}} · {severity}\n<code>{k=v, … first 6 payload keys, values ≤ 60 chars}</code>`.
- `tier_change`: `🔺 <b>SN{netuid} {name}</b> tier {from} → {to} · composite {composite_smoothed:.1f} (prev {prev_composite_smoothed:.1f}) · <a href="{ui}/subnet/{netuid}">open</a>` (`🔻` when `direction` is down).
- `red_flag_raised`: `🚩 <b>SN{netuid} {name}</b> red flag <code>{flag}</code> ×{multiplier:.2f} (total ×{flag_multiplier_total:.2f}){ · human}{ · sticky}\n<i>{detail, first 3 keys}</i>`.
- `red_flag_cleared`: `✅ <b>SN{netuid} {name}</b> flag <code>{flag}</code> cleared ({reason}) after {(as_of − raised_at)/86400:.0f} d`.
- `score_jump`: `📈 <b>SN{netuid} {name}</b> composite {prev_composite:.1f} → {composite:.1f} ({delta:+.1f}) · flags +{flags_added} −{flags_removed}` (`📉` when `delta < 0`).
- `collector_failing`: `⚠️ collector <code>{collector}</code> failing ×{consecutive_failures} · last: <i>{last_error}</i> · /pause {collector}`.
- `collector_recovered`: `✅ collector <code>{collector}</code> recovered after {failures} failures ({down_seconds/60:.0f} min)`.
- `spend_80pct`: `💸 <code>{group}</code> at {pct:.0%} of its {month} cap ({unit}){ · ${usd_mtd:.2f} / ${cap_usd:.2f} when unit = usd}`.
- `spend_cap_reached`: `⛔ <code>{group}</code> monthly cap reached ({month}) · paused: {paused_jobs}`.
- `backup_failed`: `🛑 backup failed: <i>{error}</i> · last good {last_good_name} ({last_good_age_h:.0f} h)`.
- `dereg_watch`: `⏰ watchlist <b>SN{netuid} {name}</b> dereg risk: {reason} · rank {moving_price_rank} · immune {yes/no}{, {blocks_left_immunity/7200:.1f} d left} · dereg_safety {dereg_safety}`.
- `netuid_reoccupied`: `♻️ <b>SN{netuid}</b> re-registered at block {new_network_registered_at} · {old_name} ({old_occupant_key}) → {new_name} · fresh history started`.
- `identity_changed` (warning only): `🔁 <b>SN{netuid} {name}</b> {field} {kind}: <code>{old_value}</code> → <code>{new_value}</code> (source {source})`.
- Batched (> `ALERT_BATCH_THRESHOLD`): header `📬 <b>{k} alerts</b>`, then one line per row using the same templates without links.

### 8.6 Web UI (`/ui`, no build step)

*(Changed 2026-10-18; was a fork of the old Next.js frontend on `:3001`.)* The laptop has no Node.js toolchain, 19 already ships its page as one static file served by this app (`/wb`, 02 §K1 retired the second listener), and a same-origin page needs no CORS entry, no baked token and no extra systemd unit. So the UI is three static files under `frontend/ui/` — `index.html`, `app.js` (plain ES2020, no framework, no CDN), `style.css` — served by `app/api/ui.py`: `GET /ui` and every `GET /ui/{path}` that is not an asset return `index.html`; `GET /ui/static/{file}` returns `app.js`/`style.css` (`file` must match `^[A-Za-z0-9_.-]+$` and exist, else 404). `app.js` routes on `location.pathname` (`/ui` → `/ui/matrix`), so the deep links `{ui}/subnet/64` and `{ui}/events?type=…` from alerts and the digest open the right page. Charts and sparklines are inline SVG drawn by `app.js`.

- **API base** = `location.origin`; reads send no token. **Mutations** send `X-Auth-Token` from `localStorage['taosentinel.token']`, set through a small "API token" field in the header (shown only when a write returns 401; the value never leaves the browser except in that header). Token rotation therefore needs no rebuild (§11 Q2 resolved).
- **Theme**: light/dark via `prefers-color-scheme`, CSS custom properties; works at phone width (single column under 720 px, tables scroll horizontally inside their own container).
- **Live updates**: one `EventSource('/api/v1/stream?types=score,event')`; the frames are **named**, so `addEventListener('score' | 'event' | 'resync', …)` (`onmessage` only sees unnamed frames); `score` → refetch the matrix / open subnet (debounced 2 s, a run arrives as ≈ 130 frames), `event` → refetch the events list, `resync` → refetch everything; reconnect (with `Last-Event-ID`) is the browser's.

| route | view | contents | data |
|---|---|---|---|
| `/ui/matrix` | Matrix | filter bar (tier chips A–F, min-confidence slider 0–1, include pre-launch / insufficient toggles, name/netuid search); sortable table: netuid, name, tier badge, composite, health (= `quality`), velocity, P1..P6 heat cells (0–1 → 5-step scale, `null` → hatched "n/a"), confidence, flag chips (`score.red_flags` names), 7-d sparkline (SVG 80×20); CSV link (`/api/v1/matrix.csv?…`); stat row (count, `as_of`, regime, stale banner) | `/api/v1/subnets?…` every 60 s + stream |
| `/ui/subnet/{netuid}` | Subnet | header (tier, composite, confidence, stale / pre-launch / insufficient badges, links); pillar bars with coverage; history chart (composite, health, velocity over `history_90d`, guide lines at 75/60/45/30); explain panel (top ± drivers, `score.red_flags` objects with multiplier/since/state/origin, regime, spec_version); evidence table (metric, pillar, `m`, raw, status badge ok / "verifiable absence" / "collection failure" / n/a, `raw_as_of`, fresh dot vs `freshness_s`, link — 02 §H2); site status (latest `site_down_7d`/`site_recovered` with `reason`); events (last 20); team list with status + add form (POST `/api/v1/manual/team`) + inline verify form (two URL fields, POST `/api/v1/manual/team/{id}/verify`; shows 409 `needs_second_source`); product status + form (POST `/api/v1/manual/product`); notes + form (POST `/api/v1/manual/note`) | `/api/v1/subnets/{n}` every 30 s |
| `/ui/events` | Events | severity chips, type filter (comma list), netuid input, since presets 24h/7d/30d; list with "Load more" via `before_id` | `/api/v1/events?…` every 30 s + stream |
| header (every page) | nav | links Matrix / Events / Workbench (`/wb`); status dot from `/health` every 30 s — green `ok`, amber `degraded` (tooltip = `degraded_reasons`), grey `initializing`, red fetch error | |

Flag, owner and review actions stay on Telegram/CLI (no UI) to keep the UI minimal. Product/note forms require `evidence_url` (`type="url" required`). Errors from a POST show the body's `error` code inline. Behind `tailscale serve` (16 §2.14) the UI is reached on the same tailnet origin as the API, so no CORS entry is needed; only `UI_BASE_URL` changes (for deep links).

### 8.7 Step order

Phase 0 (01 implemented from this spec — built 2026-09-24): `app/alerts/routing.py` (§2.4), `app/alerts/templates.py` (all §8.5 alert templates), `app/notifications/format.py`, the `alert_dispatch` job, the control routes (pause/resume via 16's `app/control.py`, run, reload-config), `/health` + Phase-0 services probes.

Phase 11 (this spec):
1. `errors.py` (handlers, `raise_for_result`, `origin_guard`), `common.py`; `create_app` extension (origin guard on control, spec header, `Last-Event-ID` CORS header).
2. `queries.py`; `scores.py`, `subnets.py` (evidence per 02 §H2), `events.py`. Contract tests (feed shape pinned against a mirror of 15 §3.2's `FeedPayload`, since 15 is unbuilt).
3. `export.py` (ETag), `quant.py`; the remaining `/api/health/services` probes.
4. `hub.py` + `stream.py`.
5. `manual_service.py`, `manual.py` (11 delegation), `review.py`, `registry.py`, `control.py` rescore route.
6. Telegram: `TelegramBot` in `telegram.py`, `commands.py`, `bot.py` (`telegram_bot` LongTask — its `JOB_TABLE` row exists since Phase 0).
7. `watch.py` (`dereg_watch`), `digest.py` (`needs_review_digest`).
8. `ui.py` + `frontend/ui/`.
9. `tests/conftest.py` `BUILT_PHASE = 11` (all 57 rows available except 17's `backfill_github`).

---

## 9. Tests

**Fixtures** (`tests/api/conftest.py`, on top of `tests/conftest.py`'s `rt` — a real `Runtime` with a temp-file DB, limiter, `Http`, governor and `AlertLatch` — and `tmp_root`): `seed_two_occupants(db)` inserts `occupants`/`subnets` for **SN64 `64:4123456`** (active, tier A, composite 82.4, confidence 0.91, `red_flags='[]'`, 8 official history rows + 1 provisional, one `stage='norm'` row per scored metric of its latest slot incl. one `absent` and one `failed`, 3 events, 1 verified team member, product `live`) and **SN47 `47:4000001`** (active, tier D, composite 18.7, confidence 0.55, `red_flags='["dereg_imminent"]'` with `explain.flags=[{"flag":"dereg_imminent","multiplier":0.6,"since":1789948800,"state":"active","origin":"auto",…}]`, a `red_flag_state` row `active=1`, 2 official rows, a `chain_ticks` row with `deregistration_risk=1`), plus optional **SN130 `130:4590000`** `pre_launch` with `composite NULL` and **SN9** with a sticky `owner_control_change` state row. All `scores.as_of` are slot instants (00:00Z / 12:00Z) and `computed_at` = slot + 15 min. `client` = `httpx.AsyncClient(transport=ASGITransport(app=create_app(lambda: rt)), base_url="http://test")`; 11's and 04's domain functions are the real ones (DB-only). `settings_override(API_AUTH_TOKEN=…)` where auth matters; probe egress is mocked by replacing the probe functions (no network).

| test | asserts |
|---|---|
| `test_health_passthrough` | `/health` body == `build_health(rt)` (no added keys, 02 §G6); runtime `None` → `initializing`, never 500 |
| `test_health_services_probes` | the 12 keys present; passive probes make **no** HTTP call (the `Http` mock records none for tmc/discord/x); `cached` true on a second call within 60 s; a probe raising → `state=="fail"`, others unaffected; unconfigured optional providers → `disabled`, `ok` true |
| `test_scores_latest_contract` | 2 rows sorted desc; `quality==84.1`; `ttl_seconds==25200`; `stale` false; `as_of=="…T12:00:00Z"` (slot) and `computed_at=="…T12:15:00Z"`; SN47 `red_flags == ["dereg_imminent"]` (names, 02 §B6/§H1); response validates against a test-local mirror of 15 §3.2's `FeedPayload`/`ScoreRow` (replaced by the real model when 15 lands) |
| `test_scores_latest_filters` | `min_confidence=0.6` → 1 row; `tier=A` → 1; `include_prelaunch` toggles SN130 (`composite None`); a 0.3-confidence row appears only with `include_insufficient=1` |
| `test_scores_latest_killed_and_empty` | `SCORING_KILLED` in tmp cwd → `stale true`, rows returned; unknown spec → 200, `count 0`, `as_of None`, `stale true` |
| `test_history_resolves_netuid_and_key` | `occupant=64` and `occupant=64:4123456` identical; `official_only` drops the provisional row; unknown → 404 |
| `test_subnets_list_spark` | `spark_7d` length 7 ascending; SN47 has 2 points, never padded; `stale` present |
| `test_subnet_detail` | `explain` verbatim; `score.red_flags[0]` = object from `explain.flags` with `multiplier 0.6`; evidence = 45 rows in registry order, `absent` row has `raw_status "absent"` and `m 0.0`, `failed` row `m None`; `freshness_s` = `METRICS[k].freshness_s`; `site.reason` shown for a seeded `site_recovered(reason='target_changed')`; 404 for netuid 999 |
| `test_events_filters_pagination` | `severity`, `type`, `netuid`, relative `since=24h`, `before_id` keyset |
| `test_matrix_csv_etag` | header row exact; `red_flags` `;`-joined names; `ETag`; `If-None-Match` → 304; a new score row changes the ETag |
| `test_export_json_equals_latest` | byte-equal bodies |
| `test_quant_results` | tmp dir with `factor_survivorship_20260922_101500.json`, `verdict_quality_cg_20260922_101500.json`, `trials.json`, `x.ic_series.json`, `bad.json`, `../escape.json` → kinds `factor`/`verdict`, sidecars not listed, `errors`, 404 on traversal |
| `test_auth_and_origin` | POST without header → 401 when token set; with header → 2xx; token empty → 2xx; foreign `Origin` → 403 `origin_not_allowed` even with auth off; same-origin `Origin` (`http://test`) and an allowed origin → allowed; no `Origin` (curl) → allowed |
| `test_manual_team_routes` | add → candidate + 11's `team_member_added` event; verify with one URL on the profile's domain → 409 `needs_second_source`; verify with an attestation URL on a second domain → 200 `status verified`; reject → `rejected`; duplicate → 409 `duplicate_member`; disclosure → `manual_notes(kind='team_disclosure')` |
| `test_manual_flag_rules` | confirm `silence_45d` → 422; clear `owner_control_change` when inactive → 409 `flag_not_active`; clear SN9's sticky `owner_control_change` → 201; confirm then clear `owner_extraction` → 2 rows, latest `cleared` (02 §D5) |
| `test_manual_owner_routes` | whitelist → `owner_event_whitelist` row, retire sets `retired_at`; compromise with a 60-day window → 422 `window_too_long` |
| `test_registry_routes` | unauthenticated POST to `/registry/overrides`, `/overrides/{id}/status`, `/registry/refresh`, `/reoccupied/{id}/ack` → 401, cross-origin → 403; `force` override without `evidence_url` → 422 `invalid_override`; valid override → 201 and `GET /registry/overrides?netuid=` lists it; alias `/manual/override` hits the same handler; status `retired` → 200, then no longer active; ack of a `netuid_reoccupied` event writes `manual_notes.ref_event_id` and removes it from `needs-review?reason=reoccupied_unreviewed`; `refresh` → 202 with `started` = both registry job ids |
| `test_needs_review_merge` | `queries.needs_review` with a stubbed `compute_needs_review` returning `new_occupant` + `source_disagreement_open` items → buckets `registry.new_occupant`, `registry.source_disagreement_open` first, in 04's order, then `queue`, `team_candidates`, …; stub raising → `registry.error` bucket, other buckets intact |
| `test_review_routes` | list open items; `resolve … confirm_flag` without `evidence_url` → 422 `evidence_required`; `dismiss` → status `rejected` |
| `test_control_pause_resume` | pause `x` (group) → one `control_state` row `target='x'`, `jobs` = the group's ids, no file written under `data/control/`; pause `githu` (neither a job id nor a group) → 404 `unknown_target` (no prefix matching); pause `telegram_bot` → 409 `not_pausable` with `{error, detail}`; resume deletes the row; resume with `data/control/x.paused` present → `file_pause_active`; a second resume → `removed_row: false` |
| `test_control_rescore` | 202 with `request_id` and a `rescore_requested` row; the background task calls `ScoringService.rescore` (stubbed); `shadow` with `SPEC_VERSION` → 422 `shadow_requires_other_spec`; unknown spec → 422; running → 409 `rescore_running`; already scored → 409 `slot_already_scored`; killed → 423 |
| `test_cors_and_methods` | `OPTIONS` preflight from an allowed origin OK; disallowed origin → no ACAO; `PUT`/`DELETE` → 405 |
| `test_sse_smoke` | open `/api/v1/stream`; insert a score row + `hub.nudge()` → `event: score` frame < 2 s with `id: <sid>-<eid>`; reconnect with that id after a new `StreamHub` (restart) replays the missed frame; a gap > `STREAM_REPLAY_MAX` → `resync`; 9th connection → 503 |
| `test_stream_disabled` | `STREAM_ENABLED=false` → 404 |
| `test_ui_pages` | `/ui`, `/ui/matrix`, `/ui/subnet/64` → the same HTML; `/ui/static/app.js` → JS; `/ui/static/../x` and an unknown asset → 404 |
| `test_error_shape` | a Phase-0 `HTTPException` body carries both `error` and `detail`; a 422 body has `error: invalid_body`; 19's dict detail keeps `detail.error` |

**Telegram** (`tests/notifications/`): `FakeTransport` subclass of `TelegramBot` overriding `_post` to append `(method, payload)` and `_fetch_updates` to pop a scripted queue.
| test | asserts |
|---|---|
| `test_top_format` | `/top 50` → `<pre>` present, every message ≤ 4,096 UTF-16 units, rows sorted, `fl` column |
| `test_sn_card` | bars match seeded pillars; `null` pillar → `n/a`; drivers from `explain.top_positive`; links outside `<pre>`; keyboard attached |
| `test_flags_movers_health_spend` | non-empty tables; `/flags` shows `sticky` for SN9; `/movers 400` clamps to 90; `/health` uses `status`/`degraded_reasons`; `/spend` one row per `/health.spend` key |
| `test_team_commands` | `shlex` parses quoted name/role; `/team verify` one-source → `needs a second independent source`; rows written with `created_by="telegram"` |
| `test_confirmations` | `/flag confirm …` → keyboard with `do:x:<nonce>` (≤ 64 bytes); callback executes once; a reused or expired nonce → "expired"; `/owner compromise` same |
| `test_pause_two_step` | `/pause x` → keyboard; callback `do:pause:x` → `control_state` row + toast; `answerCallbackQuery` called first |
| `test_unauthorized_chat_ignored` | message from a chat outside `TELEGRAM_ALLOWED_CHAT_IDS` → no `sendMessage` |
| `test_chunking` | 6 KB table → 2 messages, each with balanced `<pre>`, each ≤ 4,096 |
| `test_handler_exception_contained` | handler raising → reply `⚠️ command failed`, loop continues |
| `test_token_redacted_in_logs` | a `telegram_bot` request log line contains `bot***`, never the token |

**Routing and alerts** (`tests/notifications/test_routing.py`, `test_alerts.py`, `test_digest.py`, `test_watch.py`):
- `test_routing_matches_catalog`: the set of `ROUTES` keys (taxonomy row expanded) == 03's `catalog.EVENT_TYPES`; for every type the route equals 03's alert column mapped `yes→immediate`, `digest→digest`, `no→none`, `yes if …→conditional`, except the documented 02 §D4 override `x_budget_paused → digest`.
- `test_route_for_rules`: `payload.backfilled` → digest for `tier_change`; unknown type → digest (logged once); `identity_changed` warning/chain/`github_repo` → immediate, `owner_coldkey` → digest; taxonomy `critical` → immediate, `warning` → digest; `registry_anomaly` third consecutive day → immediate, first → digest; `score_jump` → immediate; `insufficient_data`, `rank_instability`, `owner_extraction_detected`, `emission_disabled` → digest; `ALERT_MIN_SEVERITY=warning` demotes `collector_recovered`; a 30-h-old `tier_change` → `digest_late`.
- `test_dispatch` (drives 01's job with this spec's data): pending rows of each route → `sendMessage` only for immediate ones; `alerted_at` set on every row with the right `payload.alert_route`; > threshold → one batched message; a `400` row retried as plain text then `alert_failed`; a `429` leaves `alerted_at` NULL.
- `test_digest`: section 2 lists digest rows since the watermark (and backfilled rows as counts); the watermark stops below the oldest unrouted row; `/digest` does not move it; buckets `product_unset` and `blocked_sites` (7 `bot_challenge` days → listed, 6 → not).
- `test_brief` (2026-10-23, on the seeded API fixture + a `P` slot, a `wb_dereg` row set and a network row): ≤ 20 lines, every `<pre>` line ≤ 32 characters, ≤ 4,096 UTF-16 units; top 5 excludes the low-confidence row; movers Δ and sign; tier change `↓ … D→F` listed before upgrades; flag raised in the window listed, one raised before `P` not; dereg line lists only `days_to_cull_static < 7`, stale network row marked; no `P` slot → "no slot 24 h earlier"; no scores → the short message; the job sends exactly one `sendMessage`, writes no `events` row, and returns `rows=0` without a token.
- `test_dereg_watch`: SN47 in `WATCHLIST_NETUIDS=[47]` → one `dereg_watch(reason='bottom3')`, a second tick the same day → no new row (dedupe `occ|reason|day`); SN64 → none; stale tick (> 30 min) → no event; immunity with 5 d left → `immunity_ending`.

Mocked: all egress (the Bot API transport is a fake; `Http.request` is monkeypatched where probes run), time through explicit `now` arguments (TTL, staleness, late rule — no `freezegun` dependency), filesystem via `tmp_root`.

---

## 10. Verify checklist (Phase 11 done when all pass on the laptop)

```bash
cd ~/Documents/TaoData && source .venv/bin/activate
pytest tests/api tests/notifications -q                      # all passed
curl -s localhost:8090/health | jq '.status,.version,.degraded_reasons'      # "ok" or "degraded", the deployed version, [] or reasons
curl -s localhost:8090/api/health/services | jq '.services|to_entries[]|[.key,.value.state]'   # 12 rows; database/scheduler/disk/backup/telegram "ok"
curl -s 'localhost:8090/api/v1/scores/latest' | jq '.count,.stale,.spec_version,.as_of,.ttl_seconds,.scores[0].red_flags'   # ≥100, false, "ts-1.0", "…T00:00:00Z"-style slot, 25200, a list of strings
curl -s 'localhost:8090/api/v1/scores/latest?include_prelaunch=1' | jq '[.scores[]|select(.status=="pre_launch")]|length'   # ≥0, composite null on those
curl -s 'localhost:8090/api/v1/subnets' | jq '.subnets[0].spark_7d|length'   # 7 after a week of runs
curl -s 'localhost:8090/api/v1/subnets/64' | jq '.score.explain.top_positive, (.evidence|length), (.evidence[0]|keys)'   # ≤3 drivers, 45, keys incl. m, raw, raw_status, freshness_s
curl -sI 'localhost:8090/api/v1/matrix.csv' | grep -Ei 'etag|cache-control'   # W/"…", public, max-age=300
curl -s 'localhost:8090/api/v1/matrix.csv' | head -1              # exact header line from §3.5
curl -sN --max-time 20 'localhost:8090/api/v1/stream' | head -5   # ": ping" or an "event: health" frame within 15 s
curl -s -X POST localhost:8090/api/v1/manual/note -H 'Content-Type: application/json' -d '{"netuid":64,"text":"x","evidence_url":"https://example.com"}'   # 401 when API_AUTH_TOKEN set
curl -s -X POST localhost:8090/api/v1/manual/note -H "X-Auth-Token: $API_AUTH_TOKEN" -H 'Origin: https://evil.example' -H 'Content-Type: application/json' -d '{"netuid":64,"text":"x","evidence_url":"https://example.com"}'   # 403 origin_not_allowed
curl -s -X POST localhost:8090/api/v1/manual/note -H "X-Auth-Token: $API_AUTH_TOKEN" -H 'Content-Type: application/json' -d '{"netuid":64,"text":"verify note","evidence_url":"https://example.com"}' | jq .ok   # true
curl -s -X POST localhost:8090/api/v1/manual/flag -H "X-Auth-Token: $API_AUTH_TOKEN" -H 'Content-Type: application/json' -d '{"netuid":64,"flag":"owner_control_change","action":"clear","evidence_url":"https://example.com"}' | jq .error   # "flag_not_active" (SN64 has no such flag)
curl -s -X POST localhost:8090/api/v1/control/pause/x -H "X-Auth-Token: $API_AUTH_TOKEN" | jq '.paused,.members'; sqlite3 data/taosentinel.db "select target,value,set_by from control_state"   # true, [x jobs]; x|true|api
touch data/control/x.paused; curl -s -X POST localhost:8090/api/v1/control/resume/x -H "X-Auth-Token: $API_AUTH_TOKEN" | jq '.paused,.file_pause_active'; rm data/control/x.paused   # true, "data/control/x.paused"
curl -s -X POST localhost:8090/api/v1/control/pause/telegram_bot -H "X-Auth-Token: $API_AUTH_TOKEN" | jq .error   # "not_pausable"
sqlite3 data/taosentinel.db "select json_extract(payload,'$.alert_route'),count(*) from events where observed_at > strftime('%s','now','-1 hour') group by 1"   # none/digest/immediate counts; no row older than ~60 s with alerted_at NULL
# Telegram: /top, /sn 64, /flags, /movers, /health, /spend, /review, /needsreview, /digest, /team list 64, /pause x (confirm), /resume x — each answers within 5 s, no message > 4,096 chars
# Telegram (2026-10-20 amendment): /menu → every home button and the 🏠 Home button on each screen; no <pre> line wraps on the operator's phone; /wallet → Main → Positions / a position / Performance 1D–1Y / Insights pages / Trades pages; terminal stopped → the "did not answer" reply
# Alerts: python -c "…emit_event(db, event_type='tier_change', …)" on a scratch occupant → message ≤ 60 s, alerted_at set, payload.alert_route "immediate"
python -m app.notifications.brief                         # (2026-10-23) the brief rendered from the live DB read-only: ≤ 20 lines, no <pre> line > 32 chars, nothing sent or written
# Telegram (2026-10-23): one 🌅 Market brief arrives daily at MARKET_BRIEF_AT_UTC as a plain message (no alert sound beyond a normal message); /health.collectors.market_brief_daily.last_ok advances daily
curl -s -o /dev/null -w '%{http_code} %{content_type}\n' localhost:8090/ui/subnet/64   # 200 text/html
# Browser http://127.0.0.1:8090/ui: /ui/matrix sorts by composite, tier filter, CSV downloads; /ui/subnet/64 chart + evidence (absent vs failure badges) + team form (201, token prompt on 401) + verify form (409 with one source); /ui/events filters; header dot green/amber
```

---

## 11. Open questions / operator decisions

Resolved on 2026-09-23 and removed: column ownership (03's DDL, 02 §B7), alert emission ownership (02 §D3), `quality` naming (12 §2.15, 15), API watchdog (16 §2.5 amendments 3–4), x_provider probe (now passive), `/health` status grep (02 §G6).

1. ~~Ports~~ — resolved 2026-10-18: one listener, `8090` (the UI moved to `/ui`); the old bot's `8081`/`3000` stay free.
2. ~~Token rotation requires `next build`~~ — resolved 2026-10-18: the `/ui` forms read the token from `localStorage` (entered once in the browser), so rotation needs no rebuild.
3. **Digest cadence:** weekly Monday 08:00Z (= 10:00 CEST) carries every digest-routed event, so a digest-only item can wait up to 7 days (anything urgent pages through its red flag). Switch `NEEDS_REVIEW_DIGEST_DAY` to `*` for a daily digest if that is too slow.
4. **Cross-spec items:** (a) 02 §H3 lets an "operator attestation note" be the second verification item; 11's built `verify_member` counts attestation notes (`_attestation_notes`), and this spec passes `note` through; (b) 03 §3.9 still marks `x_budget_paused` "yes if reason ∈ {pacing, provider_balance}" against 02 §D4 — this table follows 02 (the built catalog already says `digest`); (c) resolved: the provider id is `telegram` (02 §G1 amended); (d) resolved: 01's `TELEGRAM_ALERTS_ENABLED` was never built.
5. **Rescore id.** 12's `rescore` mints its run id only when the run finishes, so `rescore_requested.run_id` carries this route's `request_id`. If the operator wants the event to carry 12's id, 12's `rescore` needs an optional `run_id` argument (one line in 12; not done here to keep 12's golden code untouched).
6. **Watchlist.** `WATCHLIST_NETUIDS` is empty by default, so `dereg_watch` pages for nothing until the operator lists held subnets (e.g. `WATCHLIST_NETUIDS=40,94` in `.env`). The τ Terminal already raises desktop alerts for held subnets (19 §2.9); decide whether both should page.

---

## 12. Sources

- `E:\TaoData\specs\02-cross-cutting-conventions.md` (A2, B1–B3, B6, B9, B12, D1–D5, E1–E4, F1–F2, G1, G3, G6, H1–H3, J1).
- `E:\TaoData\specs\00-MASTER-SPEC.md` §1.3, §2, §5, §6, §7.5, §7.7, §9, §11, §12; `INDEX.md`; `18-operator-checklist.md` (A8 token, B3 Telegram, D1–D5 manual data).
- Sibling specs: 01 §2.5–§2.11, §5, §6 (runtime, `/health`, control routes, limiter table, Telegram keys); 03 §3.1–§3.2, §3.6 (`scores`, `events`, `metric_values`, `team_members`, `manual_notes`, `manual_flags`, `review_queue`, `control_state`, `red_flag_state`, `web_checks` DDL; events triggers), §3.9 (events catalog, alert column); 04 §2.10–§2.11 (needs-review, registry routes); 07 §2.4, §3.5 (`bot_challenge`, `site_recovered.reason`); 11 §2.2, §2.10, §2.12 (domain API, `ActionResult`); 12 §2.4, §2.8, §2.10, §2.13, §2.15 (registry, red-flag state, explain, rescore, read contract); 13 §3 (result file names); 15 §3.1–§3.2 (feed model); 16 §2.4, §2.5, §2.7, §2.10, §2.11, §2.13, §2.14 (units, liveness, backup manifests, `/health.spend`, pause precedence, ops routing, tailnet).
- `E:\TaoData\research\02-gapfill-old-bot-runtime-skeleton.md` §1 (`/health`, `/api/health/services`, SSE, two-tier auth), §4 (auth + `configure()` injection), §5 (frontend shell), §7 (tests), Gotchas (hot-reload, cwd-relative paths, routers must not import main, CORS GET/POST only, baked token).
- `E:\TaoData\research\01-whale-research-ops.md` §3 Telegram (send_alert, long-poll, keyboards, two-step confirm, `TelegramCommandHandlers` injection, setMyCommands).
- `E:\TaoData\research\02-gapfill-github-canonical-repo-resolution.md` (`/rate_limit` quota-free); `02-gapfill-discord-telegram-topology.md`.
- Old bot, Pi-recovery copy used for the 2026-10-18 port: `~/USB_Recovery/rootfs/home/pi/Desktop/SN_Bot/SubnetTrader/app/notifications/telegram.py` (HEAD 2026-08-09). Windows copy (line numbers spot-checked 2026-09-23, not on the laptop): `E:\SN_Trader\SubnetTrader\app\main.py` 2144-2228 (factory; CORS 2171-2179; configure lambdas 2205-2214), 2230-2283 (`/health`; 2280-2282 unavailable blocks), 2285-2441 (`/api/health/services`; Telegram probe 2325-2366), 3166-3225 (SSE: counter 3180/3209-3210, `is_disconnected` 3183, headers 3212-3225), 3258-3265 (non-loopback warning), 3424-3435 (bot task); `app\api_auth.py` 1-36; `app\test_api.py` 5-10; `app\strategy_api.py` 1-60 (`configure()`); `app\backtest_api.py` 30 (`RESULTS_DIR`), 80-86 (`_run_ts`); `app\notifications\telegram.py` 27-45, 52-87, 90-94, 97-111, 138-140, 142-168, 170-227, 229-259, 261-294 (chat check 271-274), 296-338 (300-303), 340-347, 349-397 (dropped), 398-486, 488-514, 516-545; `deploy\auto-update.sh` 161 (whitespace-tolerant status grep); `frontend\src\lib\api.ts` 1-30; `frontend\src\hooks\usePriceStream.ts` 1-41 (`onmessage`); `frontend\src\app\research\page.tsx` 99-107 (`StatCard`), 134-154 (`SortTh`); `frontend\src\app\strategy\page.tsx` 150-214 (`NavChart`); `frontend\src\components\NavBar.tsx` 1-71.

---

## Changelog
- 2026-09-23 reconciliation + review:
  - §2.4 rebuilt as 03 §3.9's alert column row for row, liftable as `app/alerts/routing.py` for 01's Phase-0 `alert_dispatch`; `route_for` precedence (backfilled → digest, unknown → digest, late/severity demotion); detection events of red-flag conditions digest-only; `identity_changed` warning-only rule; `score_jump` immediate; budget events and `x_budget_paused` → digest; dedupe left to 03's `dedupe_key` (D3, D4, D2, J1).
  - Feed per H1/B6: latest row per current occupant, slot `as_of` + `computed_at` (ISO), `red_flags` as names, null-tolerant rows, empty = 200; `SCORE_FEED_TTL_SEC` 25200; fixtures/tests switched to names.
  - Evidence panel reads `stage='norm'` rows of the score's slot, absent vs failed from `raw_status`, per-metric freshness from 12's registry (H2, B3).
  - `/health` served from 01's builder, no own shape; `spend` group-keyed per 16 (G6); config de-duplicated to 01/16 owners, `SCORING_SPEC_VERSION` alias (F1, F2).
  - Manual flags: confirm {owner_extraction, key_compromise}, clear also owner_control_change while active, 409 `flag_not_active` (D5); team verify/reject/disclosure, owner whitelist/compromise, review list/resolve routes + Telegram commands calling 11 §2.12, 409 `needs_second_source` (H3); `manual_notes.body`, 03 team vocabulary.
  - Pauses via 16's `app/control.py` → `control_state`; exact ids or `PAUSE_GROUPS`, no prefixes; emergency-file resume reply; reason vocabulary; `alert_dispatch`/`telegram_bot` `ops_exempt` and unpausable (B12, E1).
  - One job table §6.1 (`alert_dispatch` by 01, `telegram_bot`, `dereg_watch`, `needs_review_digest`) (E2, D3).
  - Review fixes: complete route inventory (01/04/11/12/16 routes), origin guard against cross-site POSTs, SSE ids = rowid watermarks (replay survives restarts), named-event `addEventListener`, passive probes (no TMC/Discord/X spend, G3), quant kinds per 13's file names, `dereg_watch` on `block_number`/local immunity with 03 dedupe, Telegram via `Http` with token redaction and 64-byte callback nonces, corrected old-bot line refs.
  - Coordinator additions: digest buckets `product_unset` and `blocked_sites` (07 `bot_challenge`/robots ≥ 7 d); subnet detail shows `site_recovered.payload.reason`.
  - Rejected package items: 12-reviewer's "no manual clear of owner_control_change", "read scores.observed_at / red_flags objects", "metric_norm table" (contradict D5, B6, B3); 11-reviewer's immediate routing of owner detection events and the owner_coldkey suppression rule (contradict D4/D1).
- 2026-09-24 final pass: §8.3 `needs_review` calls 04's `compute_needs_review(db, now: int) -> list[ReviewItem]` and merges its reasons as top-level `registry.<reason>` buckets in 04's priority order (error → `registry.error`); `/needsreview` rendering and `test_needs_review_merge` updated.
- 2026-09-24 final pass: 04 ships no router (04 §8.1), so 14 owns `app/api/registry.py`: registry reads, needs-review, `manual_overrides` list/create/status (no delete — status transitions only), `registry/refresh` (run-now of `registry_occupants`/`registry_identity`) and reoccupation ack (`manual_notes.ref_event_id`); all mutations authed + origin-guarded; Phase 11 (04's CLI before); `test_registry_routes` added.
- 2026-10-18 pre-build review (Phase 11), against the built Phases 0–10 code and the laptop:
  - **UI:** the Next.js fork on `:3001` is replaced by three static files under `frontend/ui/` served at `/ui` by this app (§8.6). Reasons: no Node toolchain on the laptop; 19's `/wb` already set the pattern and 02 §K1 retired the second listener; same origin means no CORS entry, no baked `NEXT_PUBLIC_API_TOKEN` (token in `localStorage`, §11 Q1/Q2 resolved) and no frontend systemd unit. `UI_BASE_URL` default → `http://127.0.0.1:8090/ui`; `FRONTEND_PORT` retired.
  - **Routing synced to 02 §D3 (amended 2026-10-16) and §K5:** no `direct` route class; `disk_critical`/`db_integrity_failed` → `immediate` (as built); no "dispatcher inactive → direct" text left (§2.4, §5 `TELEGRAM_ENABLED`, §7); 19's four workbench types added as `digest`.
  - **Factory:** the built Phase-0 `create_app(get_runtime)` + per-router `configure(get_runtime)` pattern is kept; `ApiContext` dropped (§2.1, §8.2).
  - **Rescore:** 12's built `rescore` is synchronous and mints its run id at the end → the route refuses synchronously, runs the slot in a background task and returns `request_id` (§3.5, §11 Q5). Refusal codes match 12 (`spec_unknown`, `slot_regresses`).
  - **Pause/resume/run:** bodies and codes are the built 16/01 ones (`hours`, `jobs`, `removed_row`, `still_paused_by`; `unknown_or_unavailable`, `in_flight`); one error handler gives every 4xx/5xx `{error, detail}` without breaking clients that read `detail`.
  - **Origin guard** admits the request's own origin (browsers send `Origin` on same-origin POSTs; the `/ui` and `/wb` forms would otherwise get 403) and now also guards 19's POSTs.
  - **Quant kinds** from the real file names (`gate_verdict_`, `ic_series_*` sidecars, 19's `wb_study_`); **error mapping** adds 11's `reason_required` and a 400 catch-all.
  - **SSE:** only this spec's writes nudge the hub; 12/03 are not modified (rows arrive within one 5-s poll).
  - **Telegram:** provider id `telegram` (02 §G1); port source is the Pi-recovery copy, by function name; the bot idles without a token instead of spinning the LongTask backoff; replies go to the originating allowed chat.
  - Tests use the existing `rt`/`tmp_root` fixtures and a mirror of 15's `FeedPayload` (15 unbuilt); no `respx`/`freezegun` dependency.
- 2026-10-20 (operator request after using the bot on a phone: "options don't make much sense and look ugly"; add a wallet view):
  - §2.7: phone layout (`<pre>` ≤ 32 chars; lists as plain grouped lines) and navigation (home screen, `🏠 Home` on every screen; the old `⬅️ Menu` opened `/health`).
  - §8.4: `/menu` home screen, `/jobs`, `/wallet`; callbacks `act:home|top:all|mv:<d>|jobs|needs|digest`, `w:…`; render signatures (`render_top` returns `offer_all`, `render_flags`/`render_movers` return netuids for buttons).
  - §8.4a (new): read-only wallet view rendering the τ Terminal's API; joins only TaoSentinel's tier/flags; turnover rule. 19 §1's "TaoSentinel never reads a wallet" holds (it reads the terminal, stores nothing).
  - §8.5: every read screen re-specified (examples from the live DB). Fixes found while reviewing against live data: `/top` said "No scores for ts-1.0 yet" when 128 rows existed but all were below confidence 0.4 (18:00Z catch-up run with P1–P3 null) → diagnosis + `Show anyway`; `/movers` was silently empty because the first official slot (19 Oct) is younger than the window → says so and offers 1d.
  - §5: `TERMINAL_API_URL` (loopback-only validator; 01 inventory updated).
- 2026-10-23 (spec 20 N3 item 2; reviewed against the live DB before building):
  - §2.8 (new) daily market brief `market_brief_daily` (§6.1 row, §5 keys `MARKET_BRIEF_ENABLED`/`MARKET_BRIEF_AT_UTC`, §8.1 `brief.py`, §9 `test_brief`, §10 lines). A plain send, `ops_exempt`, never an event (nothing pages).
  - Review findings: (1) 20 N3 sourced the dereg watch from `wb_dereg_network_hourly`, which is network-level (one row per hour, no netuid); the per-subnet distance is 19 §2.4's `days_to_cull_static` in `metric_values` — the brief reads that and takes only `eta_next_registration_days` from the network row. (2) Every live official slot since 2026-10-19 has `scores.regime = UNKNOWN` (`regime_daily.n_subnets = 0`, `thin_universe`), while the provisional slots of the same days classify `DISPERSED`; the 00:00Z brief therefore shows `UNKNOWN` until 05/12 fix it (BUILD-LOG 2026-10-23 N3 entry) — the brief shows the stored label rather than borrowing a provisional one. (3) Tier changes run ≈ 10 per day, hence the one-line, capped tier line. (4) "populated / 45" made exact: non-null for at least half the slot's scored occupants (30/45 on 2026-10-23 12:00Z).
  - Line 3's caption presents the score as a risk filter (first 13 verdict: no 7-d return signal; dereg ratios 3–14×).
  - §3.5 SSE (bug found while verifying 15 §2.9.1 against the live stream): `StreamHub` set its watermarks once and stopped polling when the last subscriber left, so a later client **without** `Last-Event-ID` received every row written while nobody listened (a live connect on 2026-10-23 got events up to id 2436, two of them old `red_flag_raised`). `hub.subscribe` now re-initialises the watermarks when the hub is idle; a reconnect id still replays from SQLite. `test_sse_hub_frames_and_replay` covers it.
