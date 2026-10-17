# 02 — Cross-cutting conventions & reconciliation ledger

**Status:** `design-of-record` (binding tie-breaker) · **Phase:** 0 (read before any code) · **Date:** 2026-09-23 · **Last amended:** 2026-10-16, workbench build (changelog §Z)
**Why this exists:** specs 01–17 were written and reviewed in parallel. Independent reviewers made a few contradictory "canonical" choices (e.g. a separate `metric_norm` table vs `metric_values.stage='norm'`; three different pause stores; two names for the same trigger metric). This document records **one decision per conflict**. Every spec has been (or must be) aligned to it. Specs 18 (operator checklist) and 19 (investment workbench, added 2026-10-09) are reconciled in §A2 and §K.

**Precedence when two documents disagree:** `02` (this file) › `00-MASTER-SPEC.md` › the *owner spec* for the topic (§A2) › any other spec. An implementer who finds a remaining contradiction follows this order and records the finding in `specs/BUILD-LOG.md`. A BUILD-LOG decision that changes a rule stated here (a new provider id, an alert class, a vocabulary value) is also folded back into this file as a dated amendment, so this file stays the single place to look; `tests/test_conventions.py` checks the machine-checkable rules against the code.

Decision ids (`B3`, `D4`, …) are stable and are cited by the component specs. Amendments keep their id and are marked *(Amended YYYY-MM-DD)*.

---

## A. Ownership

**A1 — Precedence.** As above.

**A2 — Owner of each cross-cutting topic** (the owner's text wins inside its topic; other specs reference it instead of restating it):

| Topic | Owner spec |
|---|---|
| Table/column/index/view DDL, migrations, the **events catalog** (`event_type` vocabulary, severities, required payload keys, dedupe patterns, alert column), the **metric-key catalog** (`app/storage/catalog.py:METRIC_KEYS`), retention policies | 03 |
| Metric **registry** (pillar, weight, normaliser, params, freshness, absence rule) — must equal master §7 exactly | 12 |
| `Settings` inventory (every config key), `app/util/time.py`, `app/util/mathutil.py` (Phase 0 port of old `app/utils/math.py` lines 1-177 incl. `gini_coefficient` 160-177; imported by 05, 11, 12, 13 — no other math-util module), `app/http.py` + `app/ratelimit.py` (`Http`, `ProviderLimiter`), `JobSpec`/`JOB_TABLE` mechanics, `CollectorJob`/`LongTask` wrappers, runtime lifecycle, `/health` builder | 01 |
| A collector's **job ids, triggers, cadence, timeouts** | that collector's spec (01's `JOB_TABLE` mirrors it verbatim) |
| Cost governor (`app/costs.py`), backups, ops jobs, liveness numbers, systemd units, runbooks, kill-switch semantics | 16 |
| HTTP API routes, SSE, exports, Telegram commands, **alert routing** | 14 |
| Trading-bot consumer of the score feed | 15 |
| Owner/team domain logic (S1–S5, detectors, review queue domain functions) | 11 |
| Backfill/import scripts | 17 |
| TMC access (`TmcClient`) and the **TMC request budget table** | 05 |
| GitHub access and the **GitHub PAT budget table** | 06 |
| Operator inputs: keys, accounts, sudo steps, the order they are needed in | 18 |
| *(Amended 2026-10-16.)* Workbench: `app/features/technicals.py`, `app/features/dereg_distance.py`, `app/collectors/chain_probe.py`, `app/collectors/wb_github_probe.py`, `app/wb/` (screener, vision, study), the `wb_*` and `chain_constants_probe` jobs, the `/api/v1/wb/*` routes and `/wb` page (`app/api/wb.py`, `frontend/wb/`). 14 still owns auth, serving and SSE; 03 still owns the DDL and catalog lines that 19 asks for (§K) | 19 |

---

## B. Storage conventions (owner 03)

**B1 — Time.** Every stored timestamp is `INTEGER` unix seconds UTC, in every table (including 04's `occupants`/`identity_observations`/`events` and 01's `schema_version`/`collector_runs`/`provider_spend`/`job_state`). ISO-8601 `…Z` strings exist only at boundaries (HTTP JSON, Telegram text, logs). Helpers live in `app/util/time.py` (owner 01): `now_ts() -> int`, `floor_hour(ts) -> int`, `floor_day(ts) -> int`, `to_iso(ts) -> str`, `parse_iso(s) -> int`, `parse_ts(x) -> int` (accepts int, float, ISO string or aware datetime), and `parse_dt(x) -> datetime` (aware UTC) for ported old-bot code that expects datetime objects (e.g. the `regime.py` port calls `.timestamp()`); `parse_iso` itself returns `int`.

**B2 — Severity.** `info | warning | critical`. Never `warn`, `high` or `error`; old `high` → `critical`.

**B3 — `metric_values` layout.** One table; no `metric_norm` table.
- Key: `(occupant_key, metric_key, as_of, spec_version, stage)`, `stage ∈ {'raw','norm'}`.
- `status` is a **generated** column = `json_extract(value_json,'$.status')` with vocabulary `ok | absent | failed | not_applicable` (a missing key generates `none`, which readers treat as `failed`). Producers never write a `status` column. `value_json.status` is reserved for this enum. Domain enums use other keys; for example, 07's product enum is `value_json.product_status`.
- There is no `is_provisional` column. A row is provisional iff `as_of % 86400 != 0`.
- `stage='raw'` rows are written by producers: `value` holds the producer's value (raw measurement, or a final [0,1] score for pre-normalised metrics, §C1), and `value_json` holds inputs/evidence.
- `stage='norm'` rows are written only by 12 at scoring time: `spec_version` is the scoring spec, `value = m ∈ [0,1]` or NULL, and `value_json = {raw, status, reason, raw_as_of, raw_observed_at, age_s, src, fallback, cohort_n}`. Their `observed_at` equals the scoring run's `computed_at`.

**B4 — Write rule.** There is a single writer, `app/storage/pit.py::put_metric_values(rows)`; `upsert_metric_values` is an alias. Keys are validated against `METRIC_KEYS`.
- `stage='raw'`: **first success wins.** A row whose status is `failed` (or `none`) is replaced only by a non-failed row whose `observed_at` is ≥ the stored one. A second failure does not replace the first *(wording amended 2026-10-16 to match 03's trigger)*. A row that is `ok`, `absent` or `not_applicable` is never replaced. A producer that needs fresher data writes the next slot's `as_of`.
- `stage='norm'`: insert-once (`ON CONFLICT DO NOTHING`).
- `as_of` convention for producers: the scoring slot the observation serves, which is `00:00Z` (official) or `06:00/12:00/18:00Z` (provisional). Hourly producers use the hour bucket.

**B5 — Read rule.** `metric_at(occupant_key, metric_key, t, *, spec_version, freshness_s)` returns the greatest `as_of ≤ t` among `stage='raw'` rows with `observed_at ≤ t`, `status ≠ 'failed'`, and `t − as_of ≤ freshness_s`. If no such row exists, it returns the newest `failed` row **inside the same freshness window** (the metric is None, with the failure reason). Otherwise it returns None. It never forward-fills and never reads a row observed after `t`. 03's keyword-only `official_only` (only `as_of % 86400 = 0`) and `strict_backfill` (skip `backfilled=1`) narrow the candidate set and never widen it.

**B6 — `scores`.**
- Key: `(occupant_key, as_of, spec_version)`. Rows are never UPDATEd.
- `as_of` is the **slot instant** (`00:00Z` official; `06/12/18:00Z` provisional). `computed_at` is the run instant, and together with `input_cutoff` it is the PIT key for backtests. Neither `scores` nor `regime_daily` has an `observed_at` column.
- `scores.red_flags` is a JSON array of flag-name strings sorted by name, e.g. `["dereg_imminent","stale_identity"]`. Multiplier, since, state, origin and detail live only in `explain.flags`.
- `SCORING_OFFICIAL_HOUR` is pinned to 0; the validator rejects any other value.

**B7 — Final DDL lives only in 03.** Specs 12, 17 and the others describe the columns they need; they never ship `ALTER TABLE`/`CREATE TABLE` for tables 03 owns. `red_flag_state` uses 03's columns (`active, human, sticky, raised_at, last_true_at, false_since, cleared_at, suppress_until, last_manual_clear_at, last_eval, last_eval_as_of, detail_json`). The state "clearing" is derived as `active=1 AND false_since IS NOT NULL`; `origin='human'` maps to `human=1`.

**B8 — Regime.** `regime_daily(day_ts INTEGER, spec_version, label, v_w REAL NOT NULL, source, run_id, computed_at, inputs_json, backfilled)`, key `(day_ts, spec_version)`; the label column is named `label` (not `market_regime`); `source ∈ {live, backfill}`. `label ∈ {TRENDING, CHOPPY, DISPERSED, DEAD, UNKNOWN}` (uppercase). There is no debounce, and a thin universe yields `UNKNOWN` (v_w 0.15). Live rows and backfilled rows use the same raw rule.

**B9 — `source` vocabulary** (both `metric_values.source` and `events.source`). One value per producer: `registry` (04), `chain` (05), `github` (06), `web` (07), `community` (08), `x` and `rss` (09), `llm` (10), `integrity` (11), `scoring` (12), `quant` (13), `api` (14, manual entry), and `runtime | storage | cost | ops` (01/03/16 system events; 16's ops emitters, i.e. backups, disk, integrity and unclean-boot recovery, use `ops`, and 03's catalog source column agrees, amended 2026-10-16). Backfills keep the producing spec's `source` and set `backfilled=1`. There is no `backfill_*` source and no `nlp` source. 03's CHECK lists equal exactly this set. *(Amended 2026-10-16.)* This vocabulary covers only `metric_values.source` and `events.source`. A table's own provenance column (`pool_history.source`, `price_bars.source`, `chain_hourly.source`, `occupants.source`, `identity_observations.source`, `team_members.source`, …) names the upstream feed (`blockmachine`, `tmc_candle`, `sse`, `rpc`, `legacy`, …) and has its own CHECK in 03. A new producer reuses the value of the spec whose data it derives from instead of adding a value (19 uses `chain`, §K3), because widening these CHECKs means rebuilding the two largest tables.

**B10 — Text retention.**
- X post text is stored for 90 d, then hash only (`social_posts`).
- Discord: TaoSentinel never reads third-party servers. Crossposts that arrive in the operator's **own aggregator server** (Followed announcement channels) may keep `announcements.text` when `DISCORD_STORE_TEXT=true` (default), with user ids redacted, for 90 d, then hash only.
- Telegram public posts are hash-only by default (`TELEGRAM_POSTS_STORE_TEXT=false`) and are classified in flight.
- *(Amended 2026-09-23.)* **"Classified in flight" is implemented as pending-classification text.** When a `*_STORE_TEXT` flag is false, the collector still writes the text into `announcements.text` with `text_purge_after = observed_at + LLM_PENDING_TEXT_MAX_DAYS × 86400` (default 7 d). 03's `storage_retention_daily` sets `text` to NULL (hash kept) as soon as a `post_labels` row exists for the post **or** `text_purge_after` has passed. With the flag true, text is kept 90 d. Spec 10 labels pending rows in its normal 6-h batch — there is no separate synchronous path — and spec 10's old "90 d Telegram text" allowance is replaced by this rule.

**B11 — Post routing.** Discord and Telegram posts go to `announcements` (08). X posts go to `social_posts` (09). 08 never writes `social_posts`. Message ids in `post_labels` are `x:{id}`, `discord:{source_message_id}` and `telegram:{handle_lower}/{n}` (lower-cased handle) — no `announcement:` prefix anywhere, and 03's column comments must match. `social_post_mentions` (post → occupant links) is written by both 08 and 09. Text hashing uses the shared `app/nlp/textnorm.py` (defined by 08: NFKC, lower, strip URLs/mentions, collapse whitespace), which 09 and 10 import.

**B12 — Pause and kill state.**
- `control_state(target, key, value, reason, set_at, set_by, expires_at)` is the only persistent store for operator pauses (API, Telegram and CLI all write it). `expires_at` NULL means until resumed; a row past `expires_at` is ignored, so a timed pause ends by itself.
- `job_state` holds runtime watermarks only.
- File sentinels:
  - `KILL_SWITCH` pauses paid collectors and the LLM; free LongTasks keep running.
  - `PAUSE_ALL` pauses every non-`ops_exempt` job.
  - `SCORING_KILLED` stops publishing new scores.
  - `data/control/<target>.paused` files (`<target>` = a job id or a `PAUSE_GROUPS` name) are an **emergency input only**: the app reads them and never writes them.
  - *(Amended 2026-10-16.)* The **integrity hold** (16 §2.6 step 8) is derived state, not a sentinel. While `data/run/integrity.json` reports `result ≠ "ok"`, the app pauses what `PAUSE_ALL` pauses, plus `storage_retention_daily`. It reports the reason `pause_all` and `degraded_reasons += db_integrity`, and it writes no pause file.
- *(Amended 2026-09-23.)* The sentinel list above is **closed**. Per-provider kill files and their path keys (`X_KILLED`, `GITHUB_KILLED`, `COMMUNITY_PAUSED`, 05's `TMC_PAUSED`/`TMC_PAUSE_SENTINEL`, `X_KILL_SWITCH_PATH`, `COMMUNITY_KILL_SWITCH_PATH`, …) are retired — the three global sentinels keep their path keys `KILL_SWITCH_PATH`, `PAUSE_ALL_PATH`, `SCORING_KILL_SWITCH_PATH` (16's `DEPLOY_PAUSED` is read only by the external `auto-update.sh`, never by the app, so it is outside this list); the emergency equivalent is `data/control/x.paused` etc.
- **Pause targets** are exact job ids or named groups from 16's `PAUSE_GROUPS` table (e.g. `discord` = {`discord_6h`, `discord_widget_1h`, `discord_gateway`}, `x` = all 09 X jobs, `llm` = all 10 jobs) — **never prefix matching**. `ops_exempt` jobs and 14's `telegram_bot`/`alert_dispatch` cannot be paused through a group target. *(Amended 2026-10-16; 14 and 16 disagreed.)* An exact-id pause of an `ops_exempt` job is allowed (16 §2.11). An exact-id pause of `alert_dispatch` or `telegram_bot` is refused with 409 `not_pausable` (`app/control.py:NOT_PAUSABLE`; 14 §2.4), because `alert_dispatch` is the only pager (§D3) and `telegram_bot` carries `/resume`. Use `ALERTS_ENABLED` / `TELEGRAM_COMMANDS_ENABLED` instead; an emergency file still applies.
- Pause-reason vocabulary (shown in `/health` and `status()`): `operator | kill_switch | pause_all | budget | emergency_file` (no `manual`/`flag`).
- `resume` deletes only the `control_state` row. If an emergency `.paused` file also exists, the reply carries `file_pause_active` (the file) and a note that the target stays paused until the operator deletes that file. If another row (e.g. a group) still covers a member, the reply carries `still_paused_by`.
- Every pause, and every resume that removed a row, emits `collector_paused` / `collector_resumed` (`source='api'`, alert `no`) as an audit trail.

---

## C. Metric keys (03 catalog = 12 registry = what producers write)

**C1 — P4 (owner 11, `source='integrity'`).**
- `team_doxxed`, `owner_identity`, `owner_commitment`, `owner_retention_30d`, `control_stability_90d` and `owner_overhang` are written as **final [0,1] scores** in `value`, with raw inputs in `value_json` using 12 §2.4's keys. 12 applies the identity (`prenorm`) normaliser to them.
- `tenure` is written by 11 as **raw days** (`tenure_days(head_block, network_registered_at)`). 12 normalises it with `zclip(days,120,365)` and a floor of 0.3; 05's `occupant_age_days` is the fallback. 11 writes `failed` with reason `no_registration_block` when the registration block is unknown.
- `owner_retention_30d` evaluates the **hard triggers first** (full-exit extrinsic `unstake_all_alpha`/`remove_stake_full_limit`, any 7-d sell impact > 2 % of pool TAO, `sell_ratio > 1`) and writes `0.0` with status `ok`. Only then does it apply the `owner_cut_enabled=false → 1.0` shortcut. *(Amended 2026-09-23.)* The `sell_ratio > 1` trigger applies only when `earned_30d ≥ OWNER_MIN_EARNED_ALPHA`. With the cut disabled, or earnings below that minimum, `sell_ratio` is undefined and only the full-exit and 7-d-impact triggers can fire.

**C2 — P4 aux trigger keys** (weight 0, read by 12's red-flag gates; written by 11):
- `owner_extraction_trigger` = 1 iff a non-whitelisted S1 hard trigger occurred in the last 30 d, **or** a non-whitelisted `perpetual_off` occurred in the last 90 d. A `perpetual_off` is either a perpetual→decaying transition of the owner's lock or `set_perpetual_lock(false)` by the owner coldkey. Otherwise the key is 0. It is `failed` when the owner data is unobservable.
- `owner_control_change_trigger` = 1 iff a non-whitelisted `coldkey_swap`, `owner_transfer`, `coldkey_change_unclassified` or `subnet_owner_changed` occurred in the last 30 d. `value_json.identity_churn = true` when the change is paired, within 14 d, with an owner-hotkey re-point or a **chain-sourced** identity change of `subnet_name`, `github_repo`, `subnet_url` or `discord` (the D4 warning set; 04's `identity_changed(field='owner_coldkey')` does not count); this makes the flag sticky (§D5).
- *(Amended 2026-09-23.)* Both trigger rows carry `value_json.trigger_ids`: a sorted list of the evidence ids that made the value 1, formatted `owner_changes:<id>` or `owner_stake_events:<extrinsic_id>` (empty when the value is 0). 12 copies them into `red_flag_state.detail_json` and uses them for D5's "new evidence" test.
- **Retired names:** `owner_lock_disabled_90d`, `owner_control_change_30d`, `owner_max_sell_impact_7d`, `identity_stability`, `owner_conviction`, `owner_retention`.
- **Evidence-only keys** (weight 0, in 03's catalog): `owner_lock_ratio`, `owner_dump_impact`, `owner_holding_share`, `owner_sell_impact_7d`, `owner_sell_ratio_30d`, `team_verified_count`, `team_candidate_count`.

**C3 — P5.**
- Producer metrics: `venue_reachable`, `discord_members`, `presence_ratio`, `discord_growth_30d` (08); `x_engagement_30d`, `x_followers` (09).
- **Derived by 12** from legs; 08 and 09 must not register producers for these: `announcement_cadence_30d`, `silence_days`.
- Legs: `discord_last_activity_days`, `tg_last_activity_days`, `x_last_activity_days`, `discord_announcements_30d`, `tg_posts_30d`, `x_cadence_30d`.
- Evidence keys: `discord_topology`, `inflated_days`, `tg_members`.
- **Retired:** `discord_last_announcement_days`, `tg_last_post_days` (and the never-catalogued `x_last_post_days`; spec 19 §2.5 reads the legs above).
- X absence: no team X handle, or a gone account, gives `x_followers = 0` (absent) and absent X legs. `silence_days = 0` (absent) only when no venue is reachable **and** no X account exists; otherwise it is observed from the legs.

**C4 — P6 (owner 10, `source='llm'`):** `event_impact_14d`, `polarity_7d`, `attention_7d`, `kol_coverage_30d`.

**C5 — Producer function contract (all registry producer fns, including 07, 08, 09, 10, 11):** `async def compute_<metric_key>(db, occupant_key: str, as_of: datetime) -> MetricResult`.
- `as_of` is 12's `T_eval` as a tz-aware UTC `datetime`; reads must use `observed_at ≤ as_of`.
- A verifiable absence returns `MetricResult(0.0, value_json, absence_reason='<reason>')`, for example `'no_x_account'`, `'x_account_gone'`, `'no_website'`, `'no_venue'`.
- Registry metrics register with `register_metric(...)`. Named appendix fns: `compute_news_mentions_7d`, `compute_video_uploads_90d`, `compute_video_mentions_30d`, `compute_blog_posts_90d`; their `source` is `rss` (the X fns use `x`). *(Amended 2026-10-16.)* The three evidence-only ones (`video_uploads_90d`, `video_mentions_30d`, `blog_posts_90d`) register as evidence functions (12's `EVIDENCE_FNS`, 09 §756), not as registry rows (§C8).

**C6 — Freshness windows** (frozen per metric in 12's registry; master §7.7 summarises them):

| Metrics | Window |
|---|---|
| chain | 1 d |
| GitHub | 2 d |
| web | 2 d |
| community fn metrics | 2 d |
| 12-derived P5 combiners | 1 d |
| X | 3 d |
| P6 | 1 d |
| `team_doxxed` | 90 d |
| other P4 (`owner_identity`, S1–S5, `tenure`) | 2 d |
| main-server Discord channel evidence (weekly re-check) | 8 d |

**C7 — `participation`** in ts-1.0 = (active validators + active miners) ÷ max_uids only. `vtrust_dispersion` is evidence with weight 0.

**C8 — Catalog vs registry.** 03's `METRIC_KEYS` = 12's registry ∪ pure evidence keys. The registry holds every weighted metric, the aux trigger keys, and the legs its derived metrics read. Pure evidence keys may live in the catalog only. Today there are 17: C2's 7, `vtrust_dispersion`, `owner_locked_share`, `sum_subnet_prices`, `tao_usd` (05); `discord_topology`, `inflated_days`, `tg_members` (08); and `video_uploads_90d`, `video_mentions_30d`, `blog_posts_90d` (09). Spec 19's 30 keys (§K4) joined them on 2026-10-16, so the catalog holds 47 evidence keys and 109 keys in all. Within the catalog only, producers may write them, and 12 does not score them.

---

## D. Events & alerts

**D1 — Catalog.** 03 §3.9 is the only catalog, and `emit_event` rejects unknown types and missing required payload keys (extra keys are allowed). Every emitter uses 03's names. Resolved renames:

| Old name (spec) | Canonical |
|---|---|
| `owner_coldkey_swap` (11) | `owner_control_change` |
| `owner_perpetual_lock_disabled` (11) | `owner_perpetual_off` |
| `owner_sell`, `owner_full_exit_call` (11) | `owner_extraction_detected` (payload `reasons[]` ⊇ `'full_exit_extrinsic'` when applicable) |
| `owner_cut_enabled_changed` (11) | `owner_cut_toggled` |
| 03 draft `invite_expiring`, `guild_changed`, `community_inflation_suspected`, `tg_handle_gone` | 08's emitted names: `invite_expired`, `invite_dead`, `inflated_community`, `community_venue_changed`, `silence_45d` (keep a draft name only if some spec emits it) |
| `spend_soft_cap` / `spend_hard_cap` (03), latch keys `budget_soft:`/`budget_hard:` (01) | `spend_80pct` / `spend_cap_reached` |

03 must also contain every type any spec emits, including:
- **16 (ops):** `service_recovered_unclean`, `spend_reset`, `budget_override_set`, `backup_failed`, `backup_stale`, `backup_mirror_missing`, `backup_mirror_failed`, `disk_low`, `disk_critical`, `db_integrity_failed`, `schema_warning`.
- **01:** `collector_failing`, `collector_recovered`.
- **11:** `owner_coldkey_swap_announced` plus its final owner/team list.
- **12:** `tier_change`, `red_flag_raised`, `red_flag_cleared`, `score_jump`, `insufficient_data`, `rank_instability`, `scoring_stalled` (system event via `AlertLatch`).
- **Also (amended 2026-09-23):** 08 `discord_gateway_fatal` (warning); 17 `backfill_done` (info) and `backfill_failed` (warning). Every `AlertLatch` key that pages must map to a catalogued type. 08's foreign-guild case is log + `/health` only, with no event.
- **14:** `dereg_watch`.

**D2 — Payload fixes** (the emitter adopts 03's required keys):

| Event | Required keys / fix |
|---|---|
| `chain_sse_recovered` | + `down_seconds` |
| `source_disagreement` (06) | severity `info`; `{netuid, occupant_key, field:'github_repo', groups:[{key, sources[], values[]}], resolved_source, pair_hash, repo_ids?}` (`repo_ids` required from 06 only; 04 emits the same type for its other identity fields without it); dedupe `{occupant_key}\|{field}\|{pair_hash}` |
| `silence_45d` | + `last_comm_at`, `last_comm_platform` |
| `collector_recovered` | `{collector, failures, down_seconds}` |
| `backup_failed` | `last_good_age_h` (not `last_ok_age_hours`) |
| `backup_stale` | `{age_hours, stale_hours}` |
| `red_flag_raised` | `{netuid, spec_version, as_of, flag, multiplier, human, sticky, detail, flag_multiplier_total}` |
| `gate_model_broken` (05) | *(added 2026-10-16)* dedupe `gate_model_broken\|{d7}` (05's weekly rule; 03 had `as_of`, which would page every slot) |
| `rank_instability` | severity `info`; `{spec_version, as_of, series, lag_days, n, rank_autocorr, quintile_turnover, top20_turnover}`; `series ∈ {health, composite_smoothed}` |

Dedupe keys follow 03's patterns.

**D3 — System events and delivery.** Runtime and ops conditions are `events` rows with `occupant_key NULL`. `AlertLatch.fire(key, text, *, event_type=None, severity='warning', payload=None, dedupe_key=None, source='runtime')` writes the row through `emit_event`; if the DB write fails, it falls back to a direct `send_alert(text)`. When `event_type=None` (process notices such as `service_online`), the latch sends the text directly and writes no row. *(Amended 2026-09-23.)* **`alert_dispatch` ships in Phase 0:** 01 implements the dispatcher job (`app/alerts/dispatch.py`), driven by 14 §2.4's routing table as data (unknown types → digest). 14's later phases add the API and Telegram commands but not the dispatcher, so alerts work from day one. Otherwise only 14's `alert_dispatch` job sends Telegram messages for `events` rows, and it sends only rows with `alerted_at IS NULL` (16's `ops_digest_daily` sends its own digest text). *(Amended 2026-10-16, BUILD-LOG Phase 0 item 2.)* There is **no `direct` route class** and no "dispatcher inactive → send direct" rule. 01 §2.11's `direct` types and 14 §2.4's `direct` marks for `disk_critical`/`db_integrity_failed` mean `immediate`. The latch's fallback runs when `emit_event` raises for any reason (DB error, or a validation error, which is logged at ERROR). Consequence: with `ALERTS_ENABLED=false` nothing pages, and that is the operator's choice.

**D4 — Routing** (14 §2.4 agrees row-for-row with 03's alert column):
- Detection events whose condition is also a red flag are **digest-only**: `emission_disabled`, `emission_reenabled`, `dereg_risk_entered`, `dereg_proximity`, `site_down_7d`, `site_recovered`, `owner_extraction_detected`, `owner_perpetual_off`, `owner_control_change`. The immediate page is `red_flag_raised`/`red_flag_cleared`.
- Watchlist dereg alerts are 14's `dereg_watch`.
- `payload.backfilled = true` goes to the digest regardless of type. *(Amended 2026-10-16.)* History rows written by 17's backfills, and 05's backfilled features, are inserted with `alerted_at` already set (§J1), so they are never delivered. This rule covers backfilled rows that arrive unalerted (e.g. with `BACKFILL_ALERTS_ENABLED=true`).
- Conditional routes (`identity_changed` "yes if warning", taxonomy types "yes if critical", `registry_anomaly` "3rd consecutive") are `yes` in the catalog's alert column, and the condition lives in `app/alerts/routing.py`. "Row-for-row agreement" means the same key set, with a conditional route counted as `yes`.
- `identity_changed` is immediate only when `severity='warning'`, i.e. `source='chain'` and `field ∈ {subnet_name, github_repo, subnet_url, discord}`; everything else goes to the digest. `owner_coldkey` identity changes are `info`/digest; owner swaps page through the red flag.
- `score_jump` is immediate; `insufficient_data` and `rank_instability` go to the digest.
- Provider-budget events other than `spend_80pct`/`spend_cap_reached` (`x_budget_paused`, `tmc_budget_degraded`, and any later `*_budget_*` type) go to the **digest**; the page for a budget stop is `spend_cap_reached`.
- Ops types with alert=yes in 03 are immediate.

**D5 — Manual flags.**
- `confirm` is allowed for `owner_extraction` and `key_compromise`. `clear` is allowed for `owner_extraction`, `key_compromise` and `owner_control_change` when the flag is active (auto-raised or confirmed); otherwise the response is HTTP 409 `flag_not_active`.
- A human-confirmed `owner_extraction_evidence` label from 10 is entered as flag `owner_extraction`.
- A manual clear sets `suppress_until = cleared_at + 30 d`, but any **new** trigger evidence (ids in the trigger row's `value_json.trigger_ids` that are not in the cleared flag's `detail_json`, §C2) re-raises immediately.
- `owner_control_change` becomes **sticky** when its trigger row has `value_json.identity_churn = true`: it stays active after the trigger returns to 0 until a manual clear. Otherwise it clears after the 30-d trigger window plus the normal 7-d hysteresis.

---

## E. Scheduler & jobs (mechanics owner 01; ids/cadence owner = collector spec)

**E1 — Trigger kinds.**
- `interval`.
- `cron` (UTC).
- `manual`: `IntervalTrigger(weeks=520)` with `next_run_time=None`, exempt from the first-run stagger and from liveness. Runs only via CLI/API `run-job`.
- `LongTask` for long-lived coroutines: `chain_sse` (05), `discord_gateway` (08), `telegram_bot` (14), and any SSE stream a spec adds.
- `startup_catchup: bool` on `JobSpec` (amended 2026-09-23): when true, the runtime runs the job once at boot if its last scheduled fire was missed while the process was down (keyed on the missed fire time, not a fixed age threshold). *(Amended 2026-10-16.)* Users: 04 `registry_identity`, 05 `tmc_daily`/`chain_features`, 06 `github_org_enum`/`github_activity`/`github_metrics`, 11 `owner_collect_daily`/`integrity_features`, 12 `scoring_6h`. Two optional `JobSpec` window fields restrict the catch-up run without changing the schedule:
  - `catchup_avoid_utc` (windows to wait out, e.g. `tmc_daily` avoids 11's TMC window `20:25-23:35`);
  - `catchup_only_utc` (the only window it may run in, e.g. `owner_collect_daily` `20:30-23:15`).
- `ops_exempt=True` marks ops jobs that `KILL_SWITCH`/`PAUSE_ALL` do not pause: 16's ops jobs, 01's `log_retention_daily`, 03's `storage_retention_daily`, and 14's `alert_dispatch` and `telegram_bot` (otherwise `PAUSE_ALL` would silence alerts and `/resume`).

**E2 — Job ids.** The owner spec's ids win, and 01's `JOB_TABLE` lists every job and LongTask exactly as the owners define them. The placeholder rows are replaced:

| 01 placeholder | Owner ids |
|---|---|
| `registry_sync` | 04 `registry_occupants`, `registry_identity` |
| `chain_rest_daily`, `chain_holders_weekly` | 05 `tmc_hourly`, `tmc_daily`, `tmc_weekly`, `chain_features`, `chain_rpc_hourly`, `chain_rpc_metagraph` (+ LongTask `chain_sse`) |
| `github_repos_6h`, `github_org_daily` | 06 `github_resolve`, `github_org_enum`, `github_activity`, `github_metrics`, `github_html_probe` |
| `web_daily` | 07 `web_check`, `web_metrics`, `web_spn2` |
| `x_teams_daily`, `x_kols_6h` | 09 `x_team_poll`, `x_kol_poll`, `x_mentions_poll`, `x_profile_refresh`, `x_ctf_crosscheck` |
| `rss_6h` | 09 appendix `rss_feeds_poll`, `news_google_poll`, `youtube_poll` |
| `llm_classify_6h` | 10 `llm_classify_submit`, `llm_batch_poll`, `llm_rules_fallback`, `llm_narrative_weekly` |

It also adds:
- 01: `log_retention_daily`.
- 08: `discord_6h` (cron), `discord_widget_1h`, `telegram_daily`, `discord_gateway`.
- 11: `owner_collect_daily`, `owner_watch_hourly`, `team_seed_daily`, `integrity_features`, `owner_rpc_seed`.
- 12: `features_hourly`, `scoring_6h`.
- 13: `quant_weekly`.
- 14: `alert_dispatch`, `needs_review_digest`, `dereg_watch`, `telegram_bot`.
- 16: `backup_daily`, `ops_guard_5m`, `ops_digest_daily`, `cost_ledger_hourly`.
- 03: `storage_retention_daily`.
- 17: manual `backfill_tmc`, `backfill_github`. The other 17 scripts run outside the service.

*(Amended 2026-10-16.)* That is the whole table: 57 = 54 jobs + 3 LongTasks (`chain_sse`, `discord_gateway`, `telegram_bot`), including §K6's four workbench jobs (built 2026-10-16). The placeholder ids above and `chain_prune` are kept in `app/jobs.py:RETIRED_JOB_IDS` and must never be declared. 01's verify strings (declared/available job counts per phase) are recomputed from the final table.

**E3 — Fixed UTC times.**

| Job / window | UTC time |
|---|---|
| `storage_retention_daily` | 03:00Z; runs every retention policy, including the `chain_ticks` prune. There is no `chain_prune` job; `STORAGE_RETENTION_CRON_MINUTE=0`. |
| `backup_daily` | 03:30Z; waits up to 20 min for `storage_retention_daily` (`DB_BACKUP_WAIT_FOR_JOBS=storage_retention_daily`) |
| unattended-upgrades reboot window | 04:30Z |
| `quant_weekly` | Sun 05:00Z |
| `ops_digest_daily` | 09:00Z |
| `scoring_6h` | 00:15Z official + 06:15/12:15/18:15Z provisional |
| `discord_6h` | cron 00/06/12/18:00Z |
| `owner_collect_daily` | 20:30Z, running until 23:30Z at most |

*(Amended 2026-10-16.)* **What the windows mean.**
- The 03:00–03:50Z maintenance window (retention, then backup) and the 04:30Z reboot are reserved for **heavy DB work**. A new cron job that writes many rows or holds a long transaction does not start between 02:55 and 03:55Z or between 04:20 and 04:45Z.
- Light network jobs may run inside the windows. SQLite's online backup and `busy_timeout` tolerate a few small writes, and a job cut off by the reboot is retried by its normal schedule or by `startup_catchup`. Example: 06's `github_html_probe` at 03:00Z.
- Interval jobs are timed from boot and may land anywhere.
- `backup_daily`'s timeout (90 min) can overlap the reboot. The backup is atomic (§I1), so a killed run leaves only a `*.tmp`, which the next run deletes.

**E4 — Liveness.** 16's max-age rules are canonical and 01 implements them. A job counts as stale only when it is enabled, not paused, not `manual`, and its `last_run` is older than its max-age.

---

## F. Configuration (inventory owner 01)

**F1 —** 01's `Settings` contains **every** key used by any spec (the union of every §5 table, including 03, 08, 11, 12, 16 and 17), grouped by owner spec. The owner spec's default wins for its own keys.

**F2 — Resolved keys and defaults.**

| Key | Value / rule |
|---|---|
| `TMC_RATE_PER_MIN` | 8 |
| `TMC_RATE_BURST` | 2; validator `TMC_RATE_BURST + TMC_RATE_PER_MIN ≤ 10` |
| `TMC_HOURLY_CAP` | 500 |
| `TMC_MONTHLY_CAP` | 90000 |
| `TMC_SSE_MAX_CONNECTIONS` | 2 (`SSE_MAX_CONNECTIONS` retired) |
| `TAOSTATS_MONTHLY_CAP_REQ` | `TAOSTATS_MONTHLY_CALL_CAP` accepted as a deprecated alias |
| `SCORING_SPEC_VERSION` | read-only alias property of `SPEC_VERSION` |
| `X_MONTHLY_CAP_USD` | 25 (set to 75 with `X_PROVIDER=official_v2`, see checklist C2) |
| `LLM_MONTHLY_CAP_USD` | 60 |
| cap semantics | **cap = 0 for a paid provider group means the provider is disabled (deny all, fail-closed).** |
| `CHAIN_TICKS_RETENTION_DAYS` | 30 |
| `CHAIN_TICKS_BLOCK_STRIDE` | 5 (one tick snapshot every 5 blocks ≈ 1 min) |
| `DB_BACKUP_MAX_TOTAL_GB` | 12 (local dir) |
| `DB_BACKUP_MIRROR_MAX_TOTAL_GB` | separate key |
| `DB_BACKUP_STALE_HOURS` | 36 |
| `DB_BACKUP_KEEP_UNCOMPRESSED` | 1 |
| `SCORING_OFFICIAL_HOUR` | 0 (pinned) |
| `DISCORD_STORE_TEXT` | true |
| `TELEGRAM_POSTS_STORE_TEXT` | false |
| `QUANT_WEEKLY_JOB_CRON` | `sun 05:00` |
| `LLM_PENDING_TEXT_MAX_DAYS` | 7 (owner 03, retention; 08 stamps `text_purge_after`; see B10) |
| `BACKFILL_TMC_QUIET_UTC` | `"02:05-02:45,20:25-23:35"` |
| `BACKFILL_TMC_MAX_PER_HOUR` | 180 |
| `TAOSTATS_RATE_PER_MIN` | 5 *(amended 2026-10-16: the free plan's limit)*; `TAOSTATS_MONTHLY_CAP_REQ` 10000 (free plan credits) |
| `BLOCKMACHINE_RATE_PER_SEC` | 12 (validator 0.1–16; plan 1,000 req/min) *(added 2026-10-16)* |
| `BLOCKMACHINE_MONTHLY_CAP_REQ` | 4500000 (plan 5 M RU/month at 1 RU per call) *(added 2026-10-16)* |
| cap semantics, request caps | *(Amended 2026-10-16.)* The fail-closed rule above also covers every **monthly request cap of a keyed/paid provider** (`TAOSTATS_MONTHLY_CAP_REQ`, `BLOCKMACHINE_MONTHLY_CAP_REQ`, `TMC_MONTHLY_CAP`): `0` = deny every request. There is no "0 = unlimited"; an operator who wants no practical cap sets a large number. |

---

## G. Providers & budgets

**G1 — One HTTP layer.** All outbound calls go through `app/http.py: Http.request(provider, method, url, *, ok_statuses=..., ...)` on top of `app/ratelimit.py: ProviderLimiter`, both owned by 01.
- There is no other limiter module. 05's `app/providers/ratelimit.py`, `get_bucket()` and the `provider_usage` table are retired; request counts live in `provider_spend.requests`.
- When the hourly or monthly cap is reached, the limiter raises `BudgetExhausted`, and the job's run note is `paused:budget <provider> <window> until <iso>` until the window resets. The pre-run budget check uses the job's **first** provider (its primary). For example, `chain_sse` = (`tmc_sse`, `tmc`) keeps streaming when the TMC REST budget is out (05 §7). There are no priority classes in v1. `EndpointSelector` (RPC endpoint failover, ported from old `app/chain/endpoints.py`) lives in `app/collectors/endpoints.py` (owner 05); 17's backfill imports it from there.
- *(Amended 2026-09-23.)* **Provider ids** are the same strings in the limiter, `provider_spend.provider`, the governor and `/health`: `tmc`, `tmc_sse`, `subnetalpha`, `subtensor_rpc`, `onfinality`, `opentensor_archive`, `github`, `github_html`, `web`, `wayback`, `discord`, `tme`, `rss`, `youtube`, `twitterapi_io`, `official_v2`, `anthropic`, `taostats`, `rufus`, `subnetradar`, `telegram` (Bot API sends/long-poll), `deadman` (16's optional healthchecks-style ping). Governor spend **groups** aggregate them: `x` = {`twitterapi_io`, `official_v2`}, `llm` = {`anthropic`}, `taostats` = {`taostats`}.
- *(Amended 2026-10-16.)* `blockmachine` (Blockmachine archive RPC, 17's primary history endpoint; `BLOCKMACHINE_*` keys, monthly request cap) is also a provider id. Archive endpoint order is `blockmachine` → keyed `onfinality` → public `onfinality` → `opentensor_archive`. 16's governor also has a request-only group `tmc` = {`tmc`} (monthly request cap `TMC_MONTHLY_CAP`, shown in `/health.spend`), so the groups are `x`, `llm`, `taostats`, `tmc`. A `JobSpec.providers` entry is a provider id or, for a job whose provider is chosen at run time, a spend group (`x`); budget pauses resolve a group through the governor. The limiter table, this list and `/health` agree; `tests/test_conventions.py` enforces it.
- On a 429, the limiter honours `Retry-After` (seconds or HTTP-date). Any status listed in `ok_statuses` is returned to the caller without retry.
- *(Amended 2026-10-16.)* There are two documented exceptions to "every call is an `Http.request`":
  - The TMC SSE stream (`tmc_sse`) opens through `Http.client("tmc_sse")` and is metered by **connection slots** (`TMC_SSE_MAX_CONNECTIONS`), not per request.
  - Archive/RPC websockets (17's `RpcGateway`, 05's optional `chain_rpc`) take one limiter token per JSON-RPC request. 17's standalone scripts book the counts to `provider_spend.requests` through `app/backfill/common.py:SpendBook`, which also seeds the script's limiter with this month's count, so monthly caps hold across processes.
  
  `deadman` is limited but never booked (`book_spend=False`).

**G2 — TMC access.** `TmcClient` (05, `app/collectors/chain_tmc.py`) is the only TMC caller, and other specs call its methods.
- **04's `registry_occupants`** (every 6 h) fetches `/subnets/?limit=200` (with `/subnets/table/` as fallback) and writes `chain_rest_snapshots` rows with `endpoint='/subnets/'` (or `'/subnets/table/'` when the fallback answered, because the payload shapes differ, so a reader asks for the endpoint whose shape it parses) (03's key is `endpoint, params, observed_at`; there is no `kind` column).
- **04's `registry_identity`** (02:40Z) fetches `/accounts/coldkey-identities/` daily and writes rows with `endpoint='/accounts/coldkey-identities/'`. `chain_rest_snapshots` has several writers (04, 05, 11 — whoever fetches the endpoint through `TmcClient`) and any reader.
- 05 and 11 **read** those snapshots and do not refetch them.
- Everything else TMC-related is fetched by 05, except 11's owner endpoints and 17's backfill, which call `TmcClient` methods.

**G3 — TMC budget.** 05's budget table lists every caller:

| Caller | Budget |
|---|---|
| 04 registry | ≤ 12 req/day + 1 coldkey-identities |
| 05 own tiers | per 05 |
| 06 `github-stats` cross-check | ≤ 21/day at 03:00Z |
| 11 `owner_collect_daily` | ≈ 390/day steady, ≤ 518 in detail mode, paced ≤ 5/min in 20:30–23:30Z |
| 17 `backfill_tmc` | one-off, ≤ 180/h, outside the quiet windows |

It states the totals: steady ≈ 716/day ≈ 21.5 k/month; worst ≈ 1.35 k/day ≈ 40 k/month; peak ≈ 311/h. All are under 8/min, 500/h and 90 k/month.

**G4 — GitHub.** 06's PAT budget table includes 17's backfill, which runs overnight and leaves ≥ 1,500 req/h of headroom for live jobs. `GitHubQuota` also records the `github-authentication-token-expiration` header (UNVERIFIED for fine-grained PATs) for the ops digest.

**G5 — Cost governor API** (16, `app/costs.py`; keyword-only after `usd`):
- `check(provider, est_usd) -> Decision`
- `reserve(provider, est_usd, *, job, run_id) -> Reservation | None`
- `commit(res, actual_usd, *, detail=None)`
- `release(res)`
- `record(provider, usd, *, requests=0, units=None, job=None, run_id=None, detail=None)` (`requests` defaults to 0 because `Http` already counts every request it sends; pass a count only for calls made outside `Http`, e.g. the Anthropic SDK)
- `snapshot() -> dict` for `/health.spend`

`provider_spend` has `requests` (not `calls`) and `units_json`. 09 and 10 call exactly these functions. 10 reserves before submitting a batch, commits after download, and releases on failure. Unreserved Sonnet/Opus calls use `record`.

**G6 — `/health`.** 01 builds it, 14 serves it, and 16's deploy gate reads only `status` and `version`, and passes on `status ∈ {ok, degraded}` with the expected version (it fails on `initializing` after its timeout, or when unreachable). 01 defines the `degraded_reasons` vocabulary and when each applies. The shape is 01's schema with:
- `status ∈ {ok, degraded, initializing}`;
- `degraded_reasons: list[str]` (vocabulary includes `spec_unknown`);
- `spend` as 16's group-keyed object `{group: {usd, cap_usd, pct, paused, reserved_usd}}`.

09, 10 and 14 reference this schema instead of restating it.

**G7 — TaoStats `/v1/` API.** *(Added 2026-10-12.)* TaoStats replaced its API; the old `/api/…/v1` routes stop answering on **2026-11-09**. Every TaoStats call (11 §C8, any future whale/pool-history port) follows these rules; source: `https://taostats.io/blog/api-migration-guide.md` (published 2026-10-08) and `https://api.taostats.io/openapi.json`.
- Host `TAOSTATS_BASE_URL` and the header `Authorization: <key>` (no `Bearer`) are unchanged, so 01's `Http`/limiter need no change. Paths drop `/api`, put the version first and use plural names (`/api/dtao/pool/history/v1` → `/v1/subnets/pools/history`); the migration guide's index maps every old route.
- Sorting is `order_by=<col>&order_dir=asc|desc` (most routes accept only `timestamp`). Unknown parameters, unknown sort columns, `limit > 200` and `network=testnet` are a **400**, not ignored; callers send only parameters listed in `openapi.json`.
- Default page size is 50, maximum 200. Callers pass `limit=200` and follow `pagination.next_page` until `null`; a route that used to return every subnet in one answer no longer does. Paging stops at row 1,000,000: narrow with `block_start`/`block_end` or `timestamp_start`/`timestamp_end` instead.
- Balances, amounts and prices arrive as **strings** (amounts in rao); parse with `Decimal`, never `float()` on the raw value. `null` means unknown, `"0"` means zero. Numbers inside `args` on `/v1/events` and `/v1/extrinsics` are JSON numbers.
- Addresses are plain SS58 strings (no `{ss58, hex}` objects); timestamps carry milliseconds (`…:36.000Z`).
- Retired with no replacement (do not plan on them): `dev_activity/*`, `dtao/tao_flow`, `dtao/hotkey_emission`, `dtao/slippage`, validator dividends and yield history, `identity/history` for coldkeys. `subnet/latest`'s `tao_flow`/`ema_tao_flow`/`net_flow_*` fields are gone too; 05's chain-sourced `subnet_ema_tao_flow` replaces them.
- Known data difference: `alpha_staked` (and so `market_cap`) on `/v1/subnets/pools*` is lower than the old API by a fixed per-subnet amount (12,362.75 α on SN64, checked 2026-10-12); price and reserves agree.

---

## H. API, feed, UI (owner 14)

**H1 — `GET /api/v1/scores/latest`** returns the latest row per occupant for `spec` (default `ts-1.0`) and drops `confidence < 0.4` unless `include_insufficient=1`.
- `as_of` and `computed_at` are ISO strings at the boundary; `red_flags` is a list of names.
- 15 consumes this endpoint unchanged and has no producer route of its own. Its `FeedPayload` has `as_of: datetime | None` and `scores` with `min_length=0`. A row with a null `composite`/`quality`/`tier` makes that netuid neutral instead of failing the batch. ETag is optional.

**H2 — Evidence panel** (14 subnet detail) reads `stage='norm'` rows: `value = m`, `raw = value_json.raw`, `raw_status = value_json.status`. Freshness comes from 12's registry per metric. Absence (`raw_status='absent'`) and failure (`failed`/`none`) are shown differently.

**H3 — Team and owner manual surfaces.**
- `team_members.status ∈ {candidate, verified, rejected}` and `source ∈ {subnetalpha, legacy, manual, llm}`. Verifying requires `verified_at`, `verified_by` and **≥ 2 independent evidence items** (`evidence_count ≥ 2`, 03's CHECK; the second may be an operator attestation note — 11's evidence count must accept it). 14's verify route either takes a list of ≥ 2 evidence URLs/notes or refuses with 409 `needs_second_source` until a second item has been added.
- 14 adds routes and Telegram commands for team verify/reject/disclosure, owner whitelist/compromise, and review list/resolve, all calling 11's domain functions.

---

## I. Disk, backups, operator items

**I1 — Disk.**
- **≥ 60 GB free is required** (peak use ≈ 4.7 × DB size + 5 GB during backup/VACUUM; the year-1 DB is ≈ 9 GB). Smaller disks are unsupported unless the operator lowers `CHAIN_TICKS_RETENTION_DAYS`, `DB_BACKUP_MAX_TOTAL_GB` and the weekly backup count, and adds a USB/NAS mirror.
- Backups are gzip'd: 7 daily + 8 weekly within `DB_BACKUP_MAX_TOTAL_GB`, never deleting the 2 newest. The newest copy is kept uncompressed for 13's snapshot.
- `Database.backup_to` writes `<dest>.tmp`, runs `quick_check`, then atomically renames.

**I2 — Operator checklist additions.**
- A deploy credential (fine-grained PAT for repo `taosentinel` with Contents read-only, or a read-only deploy key) in `~/taosentinel-secrets/deploy.env`.
- `API_AUTH_TOKEN` must be set before any `tailscale serve` exposure.
- System timezone UTC.
- BIOS "power on AC".
- No full-disk-encryption passphrase prompt (or TPM unlock), so the box recovers unattended.
- Discord Bot permissions may add *Manage Webhooks*.
- The B5 CSV columns are `netuid,channel_id,channel_name,guild_id,kind[,notes]`; 3-column rows still load.
- Checklist C2 sets `X_MONTHLY_CAP_USD=75` and `X_MENTIONS_ENABLED=false`.

---

## J. Backfill & quant contracts

**J1 — Backfill (17).**
- Backfilled rows carry `backfilled=1` and the producer's `source`. Historical events carry `payload.backfilled=true` and are written with `alerted_at` already set (suppressed).
- Legacy identity rows use `source='legacy'` with `source_ref='fundamentals.json@2026-06-02'`.
- `pool_history` stays a rowid table with nullable provenance columns. `price_bars` replaces a same-rank row when `excluded.ts_last > stored.ts_last`. It also recomputes in place when the **same source** brings the same close sample (`ts_last`, `close`) with different `open/high/low/volume/start_volume` (BUILD-LOG Phase 3; this makes rebuilds independent of run order).
- 05's `upsert_daily_from_analytics` uses 03's precedence: `analytics_1y` replaces `analytics_1w`, never the reverse, and a same-rank row is never overwritten. It honours a pre-set `occupant_key`.
- `occupants` has exact `registered_ts`/`deregistered_ts` (INTEGER) alongside `registered_at_est`.
- 04 exposes `app.registry.legacy.import_fundamentals(db, path, *, as_of, run_id, occupancy) -> LegacyReport`.

**J2 — Quant (13).**
- 13 reads the `v_quant_*` views (03), which already convert rao to τ, plus `metric_values WHERE stage='norm'`, `regime_daily.day_ts` and `scores.computed_at`.
- It uses master P4 names only and reads `data/quant/coverage.json` (17).
- `x_engagement_30d` and `polarity_7d` are flagged as look-ahead because engagement is refreshed in place.
- The snapshot comes from the newest **uncompressed** backup `.db`, never from the live DB or a `*.tmp` file.

---

## K. Workbench (spec 19) reconciliation *(added 2026-10-16)*

Spec 19 was written after this ledger (2026-10-09). Its operator questions (19 §11) stay open. The decisions below only settle names, owners and conventions, so 19 can be built without new conflicts. Where 19's text disagrees with this section, this section wins (§A1).

**K1 — Routes and page.** 19 owns `/api/v1/wb/*` and the `/wb` page (§A2). Both are served by 14's FastAPI app on `HEALTH_PORT` (8090), behind 14's `API_AUTH_TOKEN` rule and error shape. `WB_UI_PORT` is retired, so there is no second listener.

**K2 — DDL (owner 03, §B7).** 19 lists the columns it needs and 03 ships them in migration **0003** (0002 is `price_bars_blockmachine`):
- new tables `wb_presets` and `wb_uploads`;
- *(amended 2026-10-16, workbench build)* a third table `wb_dereg_network_hourly` (19 §2.4) for the network weather, instead of new columns on 05's `chain_network_hourly`, so the SSE recorder's `INSERT OR IGNORE` and `wb_dereg` never write the same row;
- one `manual_notes.kind` value, `chart_read`.

`chain_constants_probe` writes the existing `chain_constants` rows (`source='rpc_storage'`, `endpoint` = the probe URL), so it needs no DDL. `manual_notes` has no `source` column. A vision read is therefore `kind='chart_read'` with `created_by='wb_vision:<model>'` (the same pattern as `llm:<model>`), `evidence_url` = the stored image path, and `payload_json` = the structured read plus `{model, status ∈ {ok, unreadable}, cost_usd}`. 19's "`source='wb_vision'`" and "`classifier_version`" mean these fields.

**K3 — `source`.** Every `metric_values` and `events` row that 19 writes, technicals and dereg distance alike, uses `source='chain'`, except `repo_pushed_days`, which reads GitHub and so uses `source='github'` (§B9: reuse the value of the data's spec; amended 2026-10-16). They are pure functions of chain data, like 05's `chain_features`. There is no `technicals` or `wb` source (§B9). The workbench composites (`dev_activity_wb`, `social_activity_wb`, `rank_wb`) are computed in the browser and never stored.

**K4 — Metric keys.**
- Every 19 key is an evidence key: weight 0, in 03's catalog only (§C8), owner `19`, with no `spec_version` change.
- A key whose definition depends on a horizon H is catalogued once per value in `WB_DEREG_HORIZONS_DAYS`, with the suffix `_30d`/`_90d`: `dereg_rank_30d`, `dereg_safety_30d`, `mp_gap_to_cull_k_30d`, `cull_line_k_30d`, …. The k = 1 variants are `cull_line_1` and `mp_gap_to_cull_1`. In 19's tables, `_H` and `_kH` are placeholders, not key names.
- 05's weighted `dereg_safety` keeps its meaning until 19 §11 Q3 is decided. Until then 19's `dereg_safety_30d` is evidence only.
- Values must fit §B3, where an `ok` row with a NULL value is rejected:
  - an immune occupant's `dereg_rank*` rows are `not_applicable`, with `value_json.graduates_at` (not `ok`/NULL);
  - pre-launch occupants are `not_applicable`;
  - fewer than `WB_EMA_MIN_BARS` bars → `failed` with `reason: insufficient_bars`; more than 10 % missing buckets → `failed` with `reason: window_gaps`.
- `ema72_band_1h`/`_1d` store the band half-width in log units (`k·σ72`) in `value`, with `value_json.{upper, lower, mode}`.
- `as_of` follows §B4: 1 h keys use the hour bucket, and 1 d keys use the official `00:00Z` slot. `event_at` is the close time of the last **complete** bar.
- 19 §2.5's social legs are 02 §C3's `x_last_activity_days` and `tg_last_activity_days`. `x_last_post_days` and `tg_last_post_days` are not keys.
- *(Amended 2026-10-16, workbench build.)* The final list is 19 §3: 29 `chain` keys plus `repo_pushed_days` (`github`). `mp_vs_spot` is named `spot_vs_mp` (= spot/mp − 1), `spot_rank` is added, and `dereg_recovery_ratio` is deferred (its formula is unverified).

**K5 — Events** (03 catalogs them; all `source='chain'`; the four new types are alert `digest`, so they never page. `chain_constant_drift` is 05's existing type, and it keeps 05's immediate route, because a changed chain constant needs a config update; amended 2026-10-16):

| Type | Severity | Occupant | Dedupe |
|---|---|---|---|
| `dereg_rank_entered_k30` / `dereg_rank_cleared_k30` | info | yes | `occ\|{d7}` |
| `immunity_ends_7d` | warning | yes | `occupant_key` (once per occupant) |
| `chain_constant_drift` | warning | NULL (system event, §D3) | `name\|value` |
| `ema72_band_exit` | info | yes; emitted only with `WB_EVENTS_ENABLED=true` | `occ\|day` |

05's `dereg_risk_entered`/`dereg_risk_cleared` (the bottom-3 rule) stay unchanged until 19 §11 Q3 is decided.

**K6 — Jobs and providers.**

| Job id | Trigger | Provider |
|---|---|---|
| `wb_technicals` | cron hourly at :12 (the 00:12 run also writes the daily keys) | DB only |
| `wb_dereg` | hourly at :09 (covers the official slots) | DB only |
| `chain_constants_probe` | cron 01:20Z | `subtensor_rpc` (keyless finney entrypoint) |
| `wb_github_probe` | cron hourly at :25 (timeout 3,300 s); enabled only while `GITHUB_TOKEN` is empty | `github` (the limiter's no-token 50/h bucket; 06's budget table lists it as the no-token row) |

All four are declared with `phase = 3` (the phase whose data they read) and owner 19 (amended 2026-10-16).

- **`wb_discord_counts` is retired.** It would call the same keyless invite endpoint for the same counts as 08's `discord_6h`. 19's W4 builds 08's keyless half (`discord_6h`, `telegram_daily`) instead, so absence and topology rules stay 08's.
- The chart read is on-demand API work, not a job. It uses provider `anthropic` in spend group `llm` (`LLM_MONTHLY_CAP_USD`; 19 calls this "the `anthropic` group"), and calls `reserve` → `commit`/`release` (§G5).
- While `KILL_SWITCH` is set or `llm` is paused, `POST /api/v1/wb/chart-read` answers 409 `paused`.
- 16's `PAUSE_GROUPS` gains `wb` = {`wb_technicals`, `wb_dereg`, `chain_constants_probe`, `wb_github_probe`}. None of them is `ops_exempt`.

**K7 — Configuration and `/health`.**
- 01's `Settings` takes 19's `WB_*` keys, grouped under 19 (§F1), except `WB_UI_PORT` (K1).
- `WB_VISION_MODEL` defaults to `claude-opus-5-5`.
- 01's `degraded_reasons` vocabulary gains `chain_probe_stale`: `WB_ENABLED` is true and the last successful `chain_constants_probe` is older than 48 h.

**K8 — Scoring boundary.** 19 never writes `stage='norm'` rows, `scores` or `red_flag_state`. Its change requests to 05 and 12 (pool definition, `k_H` instead of bottom-3, `dereg_imminent` reading `dereg_rank_30d`) are open operator decisions (19 §11 Q3). Until a BUILD-LOG entry accepts them, 05's and 12's rules stand.

---

## Z. Changelog

- 2026-09-23 — first version; amendments marked inline (B10, B12, C1, C2, D1, D3, E1, G1).
- 2026-10-12 — §G7 TaoStats `/v1/` API.
- 2026-10-16 — Audited the built code (Phases 0–3) against this file and folded the results back in (BUILD-LOG 2026-10-16 entry).
  - New: A2 rows for 18 and 19; §K (spec 19 reconciliation); this changelog.
  - B: B4/B5 wording now matches 03's trigger and the PIT read. B9 now covers only `metric_values`/`events` (table provenance columns are separate), and 16's ops events use `ops`. B11 uses `handle_lower`. B12 lists `control_state.reason`/`expires_at`, group emergency files, the integrity hold, `not_pausable`, the resume reply and the audit events.
  - C: C3 retired leg names; C5 evidence functions; C8 lists all 17 evidence keys.
  - D: D2 `repo_ids?`, `gate_model_broken` dedupe. D3 drops the `direct` class and the dispatcher-inactive fallback. D4 drops `llm_budget_paused`, covers pre-alerted backfill rows, and explains conditional routes.
  - E: E1 complete catch-up users, window fields and `log_retention_daily`. E2 lists every job id (53). E3 says what the maintenance windows mean.
  - F: F2 adds the TaoStats free plan and the Blockmachine keys, and request caps of 0 now fail closed.
  - G: G1 adds `blockmachine`, the `tmc` group, `paused:budget`, the primary-provider budget check, the SSE/websocket exceptions and `SpendBook`. G2 covers the `/subnets/table/` fallback endpoint.
  - J: J1 adds the `price_bars` same-source recompute.
  - `tests/test_conventions.py` checks the machine-checkable rules.
- 2026-10-16 (later) — Spec 19 built (W2–W6, BUILD-LOG "Spec 19 workbench" entry). §K amended to the built design: K2 network weather in its own `wb_dereg_network_hourly` table; K3 `repo_pushed_days` uses `github`; K4 final key list (`spot_vs_mp`, `spot_rank`, `dereg_recovery_ratio` deferred); K5 `chain_constant_drift` keeps 05's immediate route; K6 triggers and phase 3. A2 lists the built modules. E2 = 57. C8 = 47 evidence keys.
