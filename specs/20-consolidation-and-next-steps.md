# 20 — Consolidation and next steps (after Phase 12)

**Status:** `proposed` · **Date:** 2026-10-18 · **Scope:** the as-built record and the ordered plan for what comes after the last master phase. This spec adds no conventions (02 › 00 › owner spec still bind); where it asks for code it names the owning spec and section. **Read with:** [`BUILD-LOG.md`](BUILD-LOG.md) (entry "2026-10-18 — Audit after Phase 12"), [`18-operator-checklist.md`](18-operator-checklist.md), [`INDEX.md`](INDEX.md).

---

## 0. Summary

Every master phase (0–12) and workbench step (W0–W7) is built. Since 2026-10-18 20:11Z the service runs the Phase-12 code and has written its first `ts-1.0` scores (128 occupants, slot 18:00Z). The pipeline is complete; the product is not yet trustworthy:

- **Nobody is paged.** No Telegram token → 8 immediate alerts and 35 critical events have never reached a human.
- **The host is not a 24/7 host yet.** Timezone is Europe/London, sleep targets are not masked, the lid switch still suspends, the unit file needs `daemon-reload`.
- **Eight phases of code are uncommitted** (193 files) on a public repo whose only commit is Phases 0–4.
- **About half of the 45 scored metrics are empty** at the first slot — for reasons that are free to fix (a 15-day `chain_daily` hole, a dry-run team seed, a Discord manual pass), or a budget decision (X, LLM).
- **`ts-1.0` is unvalidated.** The quant gates have never seen a historical score because 17 S9/S10 and 12's scoring backfill have not run.

Ten work packages N0–N9 (§2) order the fixes: safety → first-week verification → history-to-evidence → operator surface → free coverage → paid-data decisions → `ts-1.1` shadow → v2 bot → ops hardening → bloat removal. §3 is the operator's ordered to-do list, §4 the consolidated decision table, §5 the bloat inventory.

---

## 1. State of record (audit 2026-10-18 20:30Z)

### 1.1 Built vs spec

| Phase | Spec | Built | Live state on 2026-10-18 |
|---|---|---|---|
| 0 | 01, 03, 16 (subset), 14 routing | 2026-09-24 | running under systemd as `sqsq`, `MemoryMax=2G`, watchdog on |
| 1 | 04 registry | 2026-09-24 | 184 occupants (128 active, 55 deregistered, 1 pre-launch) |
| 2 | 05 chain | 2026-09-24 | SSE connected (block 9,312,699), gate fit converged (θ 0.00735, 0.18 pp) |
| 3 | 17 backfill | 2026-09-24/25, completed 2026-10-12/13 | S1–S8 done; **S9 `backfill_github` and S10 `replay_history` not built** (the one unavailable job) |
| 4 | 06 GitHub | 2026-10-17 | P2 populated 122–125/128; activity bootstrap runs nightly 20:30Z |
| 5 | 07 web | 2026-10-17 | 212 URLs, 162 checks/cycle; P3 partly populated |
| 6 | 08 community | 2026-10-17 (keyless half) | invites/widget/Telegram counts live; **gateway never run** (no bot token) |
| 7 | 11 integrity | 2026-10-18 | 4 owner spot snapshots; team seed dry-run only (184 candidates, 0 rows) |
| 8 | 12 scoring | 2026-10-18 | first run `sc-20261018T18-b24573bd` (provisional, reconstructed); first official slot 2026-10-19 00:00Z |
| 9 | 09 X/feeds, 10 LLM | 2026-10-18 (keyless parts) | RSS/News/YouTube/rules live; **no X, no LLM** (no keys) |
| 10 | 13 validation | 2026-10-17/18 | CLI works on chain factors; c1–c6 fail "no scores"; weekly job off |
| 11 | 14 API/Telegram/UI | 2026-10-18 | feed 200 at `/api/v1/scores/latest` (124 rows, `stale=false`); `/ui` served; bot idle (no token) |
| 12 | 15 bot integration | 2026-10-18 (consumer in `~/tao-terminal/app/sentinel/`) | consumer **disabled** (`[sentinel]` absent from `config.toml`); bot engine = v2 B1, unbuilt |
| W0–W7 | 19 workbench | 2026-10-16/17 | `wb_technicals`/`wb_dereg` hourly; terminal Screener/EMA band/checks live |
| — | 16 ops | Phase-0 subset only | built: units, backups, guard, digest, recovery, costs, control, security self-check. **Not built:** `deploy/auto-update.sh`, `scripts/with_bucket.py`; dead-man off; mirror off; drills §10 C/D/E/H never run |

### 1.2 Size and health

| Item | Value |
|---|---|
| App code | 52.9k lines / 206 files (collectors 13.2k, quant 5.1k, features 5.0k, integrity 4.5k, backfill 3.8k, nlp 3.5k, api 3.1k) |
| Tests | **1147 passed** in 8 min 07 s (20.4k lines, 141 files); `BUILT_PHASE = 11`, 56 available jobs |
| τ Terminal | 7.4k lines app, **169 passed** in 18 s |
| Job table | 57 declared, 56 available, 1 unavailable (`backfill_github`) |
| Schema / DB | v4; 5.39 GB + 25 MB WAL; 83 tables; `pool_history` 20.8 M rows; `metric_values` 119,895 raw + 5,760 norm; `events` 1,132 (35 critical / 137 warning / 960 info) |
| Disk | 194 GB free; `data/` 19.4 GB of which ≈ 10 GB reclaimable (§5.2) |
| Spend | $0 paid APIs this month; TMC 699 / 90,000 req; Blockmachine $9/mo subscription (backfill only) |
| Git | 1 commit (`33ffd7a`); 41 modified + 152 untracked files uncommitted; public remote |
| Config | 656 `Settings` keys; **45 never read anywhere** (§5.1) |

### 1.3 First scores (slot 2026-10-18 18:00Z, computed 20:16Z)

- Universe 128, scored 128, pre-launch 1, insufficient 3, events 13, errors 0, 6 s wall. Regime `DISPERSED` (v_w .25). Confidence 0.33–0.62, mean 0.56.
- Tiers: A 7 · B 27 · C 48 · D 26 · F 19 (+1 pre-launch untiered).
- Top: SN51 lium.io 84.1 A, SN4 80.7, SN28 80.3, SN75 78.6, SN64 78.1, SN19 77.0, SN9 75.3. Bottom: SN70 9.2, SN103 9.5, SN16 16.9, SN73 18.0, SN57 18.4.
- Red flags: `dereg_imminent` SN16/40/47/70/103/127; `stale_identity` SN35/39/59/70/103/113. 6 critical + 7 warning `red_flag_raised` events — **pending delivery, no channel**.
- P4 is `null` for every occupant in this slot (the `integrity_features` catch-up ran after the slot). Raw P4 rows now exist; the 00:00Z slot is the first real P4 reading.

### 1.4 Metric coverage at the first slot (norm stage, populated / 128)

| Pillar | Populated | Empty or thin — and why |
|---|---|---|
| P1 | emission_share 127, liquidity_depth 127, exit_capacity 127, gate_margin 127, dereg_safety 127, chain_buys 127, miner_burn_ok 100 | **`net_flow_30d` 1** (`chain_daily` hole 2026-09-25 → 10-09: TMC 1Y analytics S3 not re-run after W0); `holder_dispersion` 0 (`tmc_weekly` not yet run on new code); `participation` 0 (keyless; `TMC_NEURONS_ENABLED=false`) |
| P2 | 122–125 everywhere; review_practice_90d 96 | healthy |
| P3 | site_age 114, tls_ok 113, docs_present 104, site_freshness 96 | `site_uptime_30d` 13 (needs 30 d of checks); `product_status` 0 (manual D2) |
| P4 | — (slot artefact, see 1.3) | raw now: tenure ok 128; owner_commitment ok 117; control_stability_90d ok 4 / 124 `no_owner_snapshot` (first `owner_collect_daily` pending); **team_doxxed failed 118** (seed never applied: `not_seeded` 43, `unstructured` 63, `placeholder` 12) |
| P5 | venue_reachable 67, discord_members 26, presence_ratio 26, x_followers 25 | 61 × `main_vanity_unreviewed` (Discord pass B5, `DISCORD_CHANNEL_MAP_REVIEWED=false`); announcement_cadence_30d 0 and silence_days 11 (no Followed channels — B4/B5; X legs dead); discord_growth_30d 0 (30-d base); x_engagement 0 (`X_PROVIDER=null`, subnetalpha feeds gone) |
| P6 | event_impact_14d 128 (rules:v2) | attention_7d 0 (`baseline_short`, needs 21 d trending history); polarity_7d 0 and kol_coverage_30d 0 (X off) |

### 1.5 Findings that need action

| # | Finding | Fix (package) |
|---|---|---|
| F1 | `telegram_configured=false`; 8 immediates pending; `dereg_watch` watchlist empty | N0 (B3), N3 |
| F2 | Host: `Europe/London`, sleep targets `static`, lid switch default, sqlite3 CLI absent, `NeedDaemonReload=yes`, `unclean_boots_7d=4` | N0 |
| F3 | Phases 5–12 uncommitted; public repo | N0 |
| F4 | `chain_daily` has no rows 2026-09-25 → 2026-10-09 (service was down; S3 not re-run) → `net_flow_30d` fails on 126/128 and quant labels miss 15 days | N1 |
| F5 | Team seed dry-run only → `team_doxxed` empty, `team_members` 0 rows | N4 |
| F6 | `[sentinel]` not in the terminal's `config.toml` although the feed is now live | N1 |
| F7 | 45 inert `Settings` keys; dead unit/timer files; two static front-ends; stale README | N9 |
| F8 | `data/tmp` 3.1 GB + `data/backfill/tmp` 2.4 GB of superseded DB copies; 4.9 GB uncompressed backup twin | N0 |
| F9 | 62 occupants share `first_emission_block = 5228683` (12 §11 Q22) and legacy-only venue/X rows survive re-registration (08 Q27 / 09 Q16) | N4 (04 fix) |
| F10 | `wallet_dirs_found = 3` on the TaoSentinel host (the terminal's wallets) — accepted deviation from 16 §2.14; keep the terminal paper-only on this box | record only |

---

## 2. Work packages

Order rationale: safety before evidence before spend; nothing paid is proposed before the free fixes land. "Effort" = implementing-model sessions; calendar time is noted separately.

### N0 — Same-day housekeeping and safety (effort 0.5; operator 20 min + sudo)

**Goal:** the running system can reach a human, survives a closed lid, and its code is in git.

1. **Commit and push** everything (Phases 5–12, spec 19/20, this audit): `git add -A && git status` (`.gitignore` excludes `.env`, `data/`, `seed/` except the two whitelisted CSVs); scan the staged diff for secrets before the push (`git diff --cached | grep -nE 'ghp_|github_pat_|sk-ant-|[A-Za-z0-9]{40,}'`); tag `v0.1.0-phase12`. Add a pre-push secret scan (gitleaks or the grep) as a git hook — the repo is public.
2. `sudo systemctl daemon-reload` (unit changed on disk; no restart needed).
3. **Host prep** (16 §2.2, checklist A1): `sudo ops/laptop-prep.sh --user sqsq --dry-run`, then for real, then reboot once (both services are enabled; the terminal's user units have linger). Minimum if the script is skipped: `sudo timedatectl set-timezone UTC`; mask `sleep.target suspend.target hibernate.target hybrid-sleep.target`; `HandleLidSwitch=ignore`; `sudo apt install sqlite3`.
4. **Telegram** (checklist B3): `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` in `.env` → `sudo systemctl restart taosentinel` → expect `service_online` and the pending red-flag pages; `/health` then `/top` from the bot (14 commands: `/health /status /top /movers /sn /subnet /flags /flag /team /product /owner /review /needsreview /digest /spend /pause /resume`).
5. `GITHUB_TOKEN_EXPIRES_AT=YYYY-MM-DD` in `.env` (06 §11 item 16; the PAT sends no expiry header).
6. **Disk:** after `python -m app backup verify taosentinel-20261018T081800Z` exits 0, delete `data/tmp/pre_*.db` and `data/backfill/tmp/ledger.checkpointed.db*` (≈ 5.5 GB; both were flagged in BUILD-LOG Phase 3). Optional: `DB_BACKUP_KEEP_UNCOMPRESSED=0` (−4.9 GB; restore then needs a gunzip).
7. **Docs:** `README.md` build-status paragraph and command list rewritten to the as-built state (it still says Phases 0–2 / 138 tests); `INDEX.md` statuses corrected (done 2026-10-18); `CLAUDE.md` added (done 2026-10-18).

**Verify:** `git status` clean, `git log` shows the tag; `systemctl show taosentinel -p NeedDaemonReload` = no; `timedatectl` = UTC; `systemctl is-enabled sleep.target` = masked; `/health.telegram_configured` = true and `.alerts.pending_immediate` = 0 within one dispatch tick; `df` shows the space back.

### N1 — First week on the new code: verification backlog and the `chain_daily` hole (effort 1; calendar 7 days)

**Goal:** every `§10 Verify` item that was deferred "until the restart" is run and recorded; the first official slots are sane.

1. **Close the hole (F4):** `python -m scripts.backfill_tmc --tasks analytics --redo` (17 §2.4 S3; ≈ 128 TMC requests inside the hourly cap; the 1Y endpoint still covers the 15 days). Then confirm `select count(distinct day_ts) from chain_daily where day_ts between 1790294400 and 1791504000` = 15 (2026-09-25 … 2026-10-09) and that `net_flow_30d` is `ok` at the next slot.
2. **First official slot** (2026-10-19 00:00Z, computed ≈ 00:15Z): `/health.scores.latest_official_as_of` set; `python -m app.scoring explain --netuid 64`; P4 present; `score_jump` events on the 06:00Z slot only where justified (12 §10 run/explain/health/stability items; kill drill and power-loss item from 12 §10 after N0 step 3).
3. **Collectors on first cron:** `github_activity` bootstrap (≈ 12k requests, 20:30Z nights; check `x-ratelimit-remaining` ≥ 1,500 in the log), `github_metrics` 23:30Z, `owner_collect_daily` (→ `control_stability_90d`), `tmc_weekly` (→ `holder_dispersion`), `needs_review_digest` Monday 08:00Z (expect the two `rules:v1` false positives 1081/1082 — ignore), `ops_digest_daily` 09:00Z.
4. **Deferred checklists:** 06 §10 items 5, 8–11, 13–14; 07 §10 items 8–10, 13–14; 11 §10 live items (team seed, see N4); 14 §10 with real scores (`/ui`, SSE `/api/v1/stream`, `/api/v1/matrix.csv`, `/api/v1/export/scores.json`, bot commands); 16 §10 B (install checks), C (watchdog/crash/instance lock), E (kill switches, pauses, budget drill).
5. **Terminal consumer (F6):** add `[sentinel] enabled = true`, `mode = "observe"` to `~/tao-terminal/config.toml` (template in `config.example.toml`), `POST /api/sentinel/reload`, then 15 §10 T1–T3 (T2 saves the live fixture) and L1–L4. Observe mode only; nothing is sized until B1 exists.
6. Record everything as one BUILD-LOG entry "Week-1 live verification"; open items become rows in §4.

**Verify:** four consecutive official slots without `scoring_stalled`; `/health.status = ok` for 7 days with `degraded_reasons = []`; every collector in `/health.collectors` has `last_ok` within its liveness window; 15 L1–L4 pass.

### N2 — History → evidence: S9, S10, the scoring backfill and the first honest gate verdict (effort 3–4; calendar 1–2 weeks of nightly runs)

**Goal:** `ts-1.0` gets a point-in-time history from dTAO launch so 13's c1–c6 can say something; the result may be NO-GO, which is a valid outcome (log every trial, tune nothing to pass).

**Needs:** 12 §11 Q19 (run the backfill — recommended yes), 13 §11 items 3/4/13 (gate operationalisation, horizon 7 d, c6 union vs dereg-only — defaults below), 17 §11 item 15 (PR backfill on), Blockmachine key (present), GitHub PAT (present).

1. **Build 17 S9** `app/backfill/github_backfill.py` + `scripts/backfill_github.py` exactly per 17 §2.8, with the 2026-10-17 notes applied: `github_events.backfilled = 1` (the column exists), `/repositories/{id}/…` sub-paths (verified by the 06 build), PR dedupe key `pr_merged:{repo_id}:{number}`, the 1,500-remaining gate, the 00:30–08:00Z window. `AVAILABLE_BY_PHASE` 4→27 … 11→57 in `tests/conftest.py`; `python -m app check` → 57 available. Run over 1–2 nights; update `coverage.json` (caveat `github_org_membership_asof_today`).
   *Built 2026-10-22 (BUILD-LOG; 17 §2.8 as-built):* 627 repos / 114 occupants in scope, ≈ 15–25 k requests; needs the service restarted on the new code before the first trigger.
2. **Build 17 S10** `scripts/replay_history.py` per 17 §2.11: owners `chain` (05 `run_slot` with the historical tier — the `snapshot_historical` failure path already exists in `app/features/chain_features.py`), `github` (06 `P2Metrics.compute(now=D)`), `integrity` (11's `compute_owner_commitment` partial and `compute_control_stability_90d` in reconstructed mode, confirmed by 11 §11 item 12 → remove `control_stability_90d` from `coverage.json.forward_only`), then `scoring`. Resumable via `job_state('replay_history','last_day:<owner>')`. Range: chain from 2025-02-13 (pre-ledger era is in the DB), GitHub from `BACKFILL_GITHUB_SINCE`.
   *Built 2026-10-22 (BUILD-LOG):* `scripts/replay_history.py` (17 §2.11 as-built). Fixes it needed in owners' code: 05 tier 3 emission from `v_quant_chain_daily` + `block_times.ts_ms` + θ ≤ 0 → `theta_undefined` + replay gate events never page; 06 replay freshness by last commit (13 §2.5).
3. **Dead-occupant emission history** (13 §11 item 16, BUILD-LOG Phase 3 item 8): extend 17's RPC replay to persist the `DynamicInfo` emission share per occupant-day. `rpc_subnet_daily` already holds `emission_enabled`, `miner_burned`, `subnet_owner_cut`, `moving_price` per `(netuid, as_of, occupant_key)` — add `emission_share REAL` (migration 0005) and make `v_quant_chain_daily` a UNION that prefers `chain_daily` and falls back to the RPC row. Budget ≈ one cross-section per day since 2025-02-13 (≈ 600 archive calls). Re-run the chain factor study: c2's control then covers future-dead names.
   *Built 2026-10-22 (BUILD-LOG):* `scripts/backfill_rpc.py --emission-daily` (17 §2.3) + migration `0005_rpc_emission_share.sql`, run on the live DB before the Blockmachine plan lapsed. Two deviations: `emission_share` = 100·`tao_in_emission`/Σ (DynamicInfo has no other usable emission field; `emission` is 0), and the view also uses the RPC share **before 2025-11-17**, because TMC's `emission_percent` was truncated to whole percent until then (13 §11 item 7 corrected). The chain factor study re-run is left for item 5 (the first verdict).
4. **Scoring backfill** (12 §2.15 item 7): `python -m app.scoring backfill --spec ts-1.0 --from 2025-09-22` from the CLI (outside the unit's `MemoryMax=2G`), at night, service running (append-only, `backfilled = 1`, stops before the first live slot). Measure wall time (spec estimate 20 min is UNVERIFIED). Decide separately whether to extend to 2025-02-13 (chain-only history; P2 absent).
5. **First verdict:** `python -m app.quant snapshot` → `report --run-all` → `gate` for `quality_cg` and `P1` on both scopes; run c6 with the union (default) and the `death_ratio_dereg_only` diagnostic side by side; log everything in `trials.json`. Then `QUANT_WEEKLY_JOB_ENABLED=true` with `MemoryMax=3G` in the unit (16 §11 item 16) or keep the CLI.
6. Update 13 §2.5's backtestability matrix and `coverage.json`; BUILD-LOG entry with the verdict and its caveats (P2 history uses today's org membership — report with and without pre-`first_seen_at` rows, 06 §11 item 6; P3/P5/P6 forward-only; historical Health is the P1+P2 renormalisation).

**Verify:** `scores` holds ≥ 365 official `backfilled = 1` slots for `ts-1.0`; `report --run-all` returns no `insufficient_data` for `quality_cg` on the survivorship scope; `gate_verdict_quality_cg_*.json` has `pass` booleans (not null) for c1–c6 and `pending: [c7]`; `coverage.json.forward_only` no longer lists `control_stability_90d` or `chain_buys` (13 §11 item 14 resolved one way or the other).

### N3 — Operator surface: alerts that read well, a daily brief, scores where the operator looks (effort 2)

1. **Telegram in use** (after N0 step 4): walk the 14 §10 command checklist; set `NEEDS_REVIEW_DIGEST_DAY=*` only if Monday is too slow (14 §11 Q3); `WATCHLIST_NETUIDS` whenever a position is held (14 §11 Q6 — Telegram pages away from the desk, the terminal alerts at it; keep both).
2. **New job `market_brief_daily`** (owner 14, phase 11, cron `MARKET_BRIEF_AT_UTC` default `06:00`, flag `MARKET_BRIEF_ENABLED`, ops-exempt, a plain send — never a page): one message ≤ 20 lines — regime, top 5 by composite, five biggest 24-h movers (Δ composite vs the slot 24 h earlier), tier changes, flags raised/cleared, dereg watch (occupants whose `wb_dereg` distance to the next cull line is below one week), and one data-health line (populated metrics / 45, stale collectors). Sources: `v_latest_scores`, `scores` at `as_of − 86400`, `red_flag_state`, `wb_dereg_network_hourly`, `/health`. Reuse `app/notifications/format.py` (`bar/dot2/num/cut`). ≈ 150 lines + a test with the seeded API fixture.
3. **Sentinel tab in the τ Terminal** (`~/tao-terminal`, extends 19 W7 and the 15 consumer): a table of all subnets (composite, tier, confidence, pillar bars, red flags) from the consumer's TTL store (`/api/v1/scores/latest`, already polled when `[sentinel]` is enabled); a per-subnet drawer from `/api/v1/subnets/{netuid}` (explain and evidence) and a composite sparkline from `/api/v1/scores/history`; tier badge and flag markers on the subnet chart; desktop alert on `red_flag_raised` for held or watched subnets via `/api/v1/stream`. TaoSentinel's `/ui` stays the admin view.
4. **`python -m app status`** one-screen CLI (reads `/health` and the DB): latest slot, tier histogram, populated metrics per pillar, stale collectors, pending alerts, spend. Cheap, and it is what every future session runs first.

**Verify:** a brief arrives daily; the terminal shows the same composite as `/api/v1/scores/latest` for a sampled subnet; `python -m app status` runs in < 3 s.

### N4 — Free coverage: the empty metrics that cost nothing but time (effort 1; operator 2–3 h once)

| Step | Unlocks | How |
|---|---|---|
| Discord B4 (bot + aggregator server) and B5 (channel map pass, ≈ 1 h; `seed/discord_channels.csv` has 25 rows today) → `DISCORD_CHANNEL_MAP_REVIEWED=true` | `venue_reachable` for 61 occupants, `discord_announcements_30d`, `announcement_cadence_30d`, `silence_days` legs; gateway live test (08 §10 B4/B5) | checklist B4/B5 |
| `TMC_NEURONS_ENABLED=true` (≈ 18 req/day) | `participation` (05 §11 Q4) | `.env` + restart |
| Team seed for real: `python -m scripts.manual_entry team seed` without the dry-run flag (184 candidates) | `team_doxxed` moves from `failed` to candidate 0.3; `team list --status candidate` ordered by composite → verify tier A/B first (D1, two public sources each) | 11 §2.12 CLI or `/team add` |
| Product status for the top 30 by composite | `product_status` (D2) | `/product set <netuid> live <url>` |
| YouTube channel rows (09 §11 Q17), KOL roles (D3, `seed/kol_accounts.csv`) | `video_uploads_90d`, `kol_coverage_30d` once X exists | manual rows |
| Owner whitelist decision for SN64's treasury transfers (11 §11 Q20) | avoids a false `owner_extraction` flag on a tier-A subnet as soon as P4 is live | `owner whitelist add 64 --reason disclosed_transfer --applies-to stake_events --new-coldkey <dest> …` or accept the flag |
| 04 fix for legacy-only rows on re-registered netuids (08 Q27 / 09 Q16): apply the occupancy guard, retire rows whose declaring source is `legacy` only | removes predecessors' Discord/Telegram/X venues on SN10/20/53/89/94/109/113 | small change in `app/registry/`, test, BUILD-LOG |

**Verify:** populated metrics at the official slot rise from ≈ 24/45 to ≥ 33/45 without any paid key.

### N5 — Paid data: decide, then wire (effort 0.5 + smoke tests; operator decision)

| Source | Monthly cost | Unlocks | Recommendation |
|---|---|---|---|
| **LLM** (10 §11 Q1; `ANTHROPIC_API_KEY`, `LLM_ENABLED=true`) | ≈ $21–31 at 1,300 items/day; **≈ $3–8 today** because without X the item stream is news/RSS/YouTube/Discord only (≈ 50–150 items/day) | P6 quality (polarity on non-X text, entity attribution, `review_queue` candidates), replaces `rules:v2` | **yes now**, `LLM_MONTHLY_CAP_USD=15`, `LLM_NARRATIVE_ENABLED=false` until the budget is understood; run 10 §9 network smoke tests first (Haiku 5.5 `effort`+`format` inside Batches, `fallbacks` beta) |
| **X** (09 §11 Q1/Q15) | twitterapi.io ≈ $12.5 at the $25 cap; official v2 ≈ $70 | the only path to P5 X legs and P6 polarity/KOL; today there is no X data at all | **30-day trial on twitterapi.io** at `X_MONTHLY_CAP_USD=25`; record fixtures (`scripts/record_x_fixtures.py`, 09 §9.2); after 30 d measure the X metrics' IC with 13 before renewing |
| **TaoStats Standard** ($49) | owner_retention fidelity, stake-event history, whale revival | not now | no; revisit if 11's S1 proves noisy |
| Vision for chart uploads (19 §11 Q4) | ≈ $0.01–0.05 per image | `/wb` upload notes | with the LLM key: `WB_VISION_ENABLED=true` |

Also: golden-set labelling (checklist D6) after the first LLM week — `python -m app.nlp.eval bootstrap`; `TAOSTATS_ENABLED` is referenced only by tests — fold into `OWNER_TAOSTATS_ENABLED` or wire it (N9).

**Verify:** `/health.spend.llm.usd` grows and stays under the cap; `post_labels` carry `claude-haiku-5-5:e47d012b:1`; `x_*` raw rows turn `ok` for the 25 occupants with handles.

### N6 — `ts-1.1` as a shadow spec (effort 1; zero cost to run)

The specs recorded a dozen "ts-1.1 candidates". Put them in one `ScoringSpec` and run it as a shadow series (`SCORING_SHADOW_SPECS=ts-1.1`, `scores.shadow = 1`, 12 §2.13) so 13 can compare the two on the same slots; backfill it with N2's machinery (`backfill --spec ts-1.1`). Candidates, each a separate commit with its own golden test:

1. `rankg` tied zeros → a real zero scores 0.0 and only positive values are ranked (12 §11 Q6).
2. Velocity blend robust to missing legs (12 §11 Q6's V ≈ 42.7 artefact) — weight only present legs.
3. `x_engagement_30d` adds quotes at weight 2 (09 §11 Q12); team accounts weight 0.5 in `polarity_7d` (10 §11 Q9); mat-3 negatives count at materiality 2 until confirmed (10 §11 Q2).
4. P4: graded `commit_pct` (11 §11 Q6), S2 saturation at `lock_ratio ≥ 0.75` (12 §11 Q7).
5. P5: inflation exemption for presence ≥ 250 or guild age > 2 y (08 §11 Q7); growth denominator = current members (08 §11 Q6).
6. Workbench change requests (19 §11 Q3): pool definition including not-started subnets, `k_H` instead of bottom-3, `dereg_imminent` reading `dereg_rank_30d ≤ k_30`, no TMC badge in `dereg_risk_flag`.
7. Pillar renormalisation over *available* pillars only, so an X-less deployment is not scored against pillars it cannot have (check how `pillar_coverage` enters `confidence` and `composite` in 12 §2.9 before changing anything).

No promotion without 13's verdict; the switch is 15 §11 item 15 (new URL, whitelist entry, Phase B, verdict).

### N7 — SubnetTrader v2 B0–B5 in `~/tao-terminal` and spec 15 Phase B (effort 5–7; calendar ≥ 4 weeks of paper)

Plan of record: `~/tao-terminal/docs/SUBNETTRADER_V2_PLAN.md`. Blocked on its §9 answers; proposed defaults so "go with the defaults" is a complete answer:

| §9 | Default |
|---|---|
| 1 wallet | bot = `trader_wallet`, manual = `squiddy` (separate coldkeys) |
| 2 paper pot | 10 τ pot, 0.5 τ per trade, 5 open positions, ≤ 20 % of pot per subnet, 10 % drawdown pause, 3 % daily loss stop |
| 3 style | momentum/breakout on 4 h as primary, trend-pullback as the comparator; B2's walk-forward decides |
| 4 horizon | days (4 h bars); 1 h only if B2 shows it pays after costs |
| 5 sentiment | free money flow + GitHub via the TaoSentinel feed in **observe** mode; X only after N5's 30-day trial |
| 6 notifications | desktop now; Telegram later with its own bot token |

Build order: B1 engine as the `tao-bot` user service, paper, every entry through `rt.sizer.sized_preview(trader, req, strategy)` then `attach_trade` (15 §8.2) → B2 backtest gate with the terminal's `backtest.py` plus 13's AMM economics (`app/quant/economics.py`, `slippage.py`), rules and acceptance numbers written down before the run → B3 reason-labelled markers and Bot tab → B4 money-flow features logged with each decision → B5 ≥ 4 weeks / ≥ 30 paper trades. Spec 15 Phase B follows: `python -m app.sentinel.telemetry export` → c7 → `report --run-all` → `active` mode only on `promotable` (15 §2.7). Live money (B6) only on an explicit go.

### N8 — Ops hardening leftovers from 16 (effort 1; operator sudo)

1. Drills in order: 16 §10 D (backup verify + restore to `data/tmp/drill.db`), C (SIGKILL → `NRestarts=1`, `process_crash`), E (kill switches, pauses, budget drill), then H (simulated power cut) — H only after N0 step 3 and with Telegram live so the recovery page is seen.
2. `DEADMAN_ENABLED=true` + a free healthchecks.io URL (`DEADMAN_PING_URL`) — the only alert that fires when the laptop itself is dead.
3. Backup mirror (`DB_BACKUP_MIRROR_DIR` on a USB/NAS mount, 16 §2.7) — today `mirror = off` and every copy sits on the same disk.
4. Auto-update: recommend **not** building `deploy/auto-update.sh` for a one-operator box (manual `git pull && sudo systemctl restart taosentinel` after tests); delete `deploy/taosentinel-update.{service,timer}` in N9 unless the operator wants the timer, in which case build 16 §2.9 and run §10 F.
5. Battery charge limit (vendor tool), BIOS "power on with AC", `tailscale serve` only if remote access is wanted (API token is already set).

### N9 — Bloat and debt removal (effort 1–2) — the list is §5

Code-level steps: a test that fails when a `Settings` key is neither read in `app/`/`scripts/` nor listed in an explicit `RESERVED_KEYS` set (same whole-word rule as the audit, both cases); retire the keys in §5.1 that have no owner left; delete the dead files in §5.1; fold `frontend/wb` into `frontend/ui` or shrink `/wb` to the vision upload; wire `app/quant/dereg_events.py` as `python -m app.quant dereg-events` (13 §10 cites it) or delete it and fix 13; `pytest-xdist` (`-n auto`) or a `make test-fast` target; write `specs/AS-BUILT.md` (≤ 300 lines: module map, job table, metric coverage, open decisions) so a new session does not have to read 2.6 MB of specs; rewrite `README.md`.

---

## 3. Operator inputs, in order

1. **Today (no money):** `sudo systemctl daemon-reload`; Telegram bot token + chat id (B3); `GITHUB_TOKEN_EXPIRES_AT`; say "yes" to the commit + push and to deleting the superseded DB copies (§5.2).
2. **This week (sudo, 20 min + a reboot):** `sudo ops/laptop-prep.sh --user sqsq` (or the four manual steps in N0.3); BIOS "power on with AC".
3. **Decisions with defaults — "defaults" is a complete answer:** 12 Q19 backfill = yes; 13 §11 items 3/4 = as pre-registered, horizon 7 d; 13 item 13 = union + dereg-only diagnostic; 17 item 15 = PR backfill on; `DB_BACKUP_KEEP_UNCOMPRESSED` = keep 1; auto-update = not built; `DEADMAN` = on with a healthchecks.io URL.
4. **Free accounts and manual data (2–3 h once):** Discord bot + aggregator (B4) and the channel-map pass (B5); team verification for tier A/B (D1); product status for the top 30 (D2); KOL roles (D3); YouTube channel rows; the SN64 owner-transfer whitelist decision (11 Q20).
5. **Money:** LLM key with a $15 cap (yes recommended); twitterapi.io 30-day trial at $25 (recommended); TaoStats Standard (no); Blockmachine stays until N2 is done, then it can be paused between backfills.
6. **v2 bot §9** — "defaults" per N7, or your own numbers.

## 4. Consolidated decision table

| Spec · item | Question | Default if silent | Unlocks |
|---|---|---|---|
| 12 Q19 | run `ts-1.0` backfill from 2025-09-22 | yes | N2 |
| 13 §11 3/4/13 | gate operationalisation, horizon, c6 rule | pre-registered, 7 d, union + diagnostic | N2 verdict |
| 13 §11 16 | dead-occupant emission history | build (N2.3) | honest c2 |
| 17 §11 15 | PR/review backfill | on | `review_practice_90d` history |
| 09 Q1 / Q15 | X provider | twitterapi.io trial, $25 cap | P5 X legs, P6 polarity/KOL |
| 10 Q1 | LLM budget | on, $15 cap, narrative off | P6 quality |
| 11 Q20 | SN64 treasury transfers | operator must choose | avoids a false red flag |
| 11 Q4 / Q7 | S2 grace period; pre-2026-05-08 penalty | keep as built | — |
| 08 Q9 / B5 | Discord channel map reviewed | do the pass | 61 × `venue_reachable` |
| 05 Q4 | `TMC_NEURONS_ENABLED` | on | `participation` |
| 14 Q3 / Q6 | digest cadence; watchlist | weekly; set when holding | alerts |
| 19 Q1 / Q3 / Q8 | EMA band definition; 05/12 change requests; check thresholds | keep; → ts-1.1 (N6); keep advisory | — |
| 15 Q16–18 | paper entries in c7; min-trade floor; two pollers | as built | — |
| 16 §11 10–13 | FDE prompt, deploy creds, UTC, BIOS | UTC + BIOS now; no auto-update | N0/N8 |
| 00 §13 2/3 | TaoStats Standard; lite node | no; no | — |
| v2 §9 1–6 | bot decisions | N7 defaults | B1 |

## 5. Bloat and debt inventory

### 5.1 Code and docs

- **45 inert `Settings` keys** (declared in `app/config.py`, never read in `app/`, `scripts/`, `tests/` or `frontend/`, either case; a key read through a computed name would be a false positive — none was found by inspection of the `getattr(s, …)` sites): `BACKFILL_GITHUB_MAX_PAGES_PER_REPO BACKFILL_GITHUB_MAX_REPOS_PER_OCCUPANT BACKFILL_GITHUB_PRS_ENABLED BACKFILL_GITHUB_PR_MAX_PER_REPO BACKFILL_GITHUB_RESERVE_REMAINING BACKFILL_GITHUB_SINCE BACKFILL_GITHUB_TIMEOUT_SEC BACKFILL_GITHUB_WINDOW_UTC` (become live with N2 S9) · `BACKFILL_TMC_TIMEOUT_SEC DB_BACKUP_INTERVAL_HOURS DISCORD_INTERVAL_MIN FRONTEND_PORT GATE_HOURLY_ENABLED JOB_DEFAULT_TIMEOUT_SEC LEDGER_START_BLOCK LLM_BATCH_INTERVAL_MIN OWNER_AMOUNT_UNITS OWNER_S4_ENABLED OWNER_S4_SELF_WEIGHT_MAX OWNER_S4_VALI_STAKE_SHARE_MAX OWNER_SOURCE_DISAGREE_TOL OWNER_STAKE_REMOVED_ENABLED OWNER_STAKE_REMOVED_MAX_CALLS_PER_DAY OWNER_STREAM_ENABLED QUANT_GATE_LABEL REGISTRY_JUNK_TOKENS REGISTRY_STALE_SOURCE_DAYS SCORING_MIN_CONFIDENCE SCRIPT_RATE_SHARE SCRIPT_TMC_RATE_PER_MIN SECURITY_SELFCHECK_ENABLED SUBTENSOR_LOCAL_WS SUBTENSOR_RPC_CONNECT_TIMEOUT_SEC SUBTENSOR_RPC_MODE TAOAPP_API_KEY TMC_ANALYTICS_1W_ENABLED TMC_ANALYTICS_1W_FIELDS TMC_ANALYTICS_1Y_ENABLED TMC_CANDLES_ENABLED TMC_PRESSURE_WATCHLIST_ENABLED TMC_PRESSURE_WATCHLIST_SIZE TMC_SSE_BLOCKS_STREAM TMC_VALIDATORS_ENABLED WB_EMA_BAND_MODE X_AUTHOR_SHARE_CAP`; plus `TAOSTATS_ENABLED` read only by tests. Each is either a promise the operator may act on with no effect (`SECURITY_SELFCHECK_ENABLED`, `TMC_CANDLES_ENABLED`, `OWNER_STREAM_ENABLED`) or a leftover of a dropped design (`OWNER_STAKE_REMOVED_*` = 11's C5, `FRONTEND_PORT`, `SCRIPT_*` = the never-built `with_bucket.py`).
- **Dead files:** `ops/systemd/taosentinel-frontend.service` (UI moved to `/ui` 2026-10-18); `deploy/taosentinel-update.service` + `.timer` without `deploy/auto-update.sh`; `app/quant/dereg_events.py` (56 lines, no caller in code — only 13 §10's `python -c` cites it).
- **Dormant but referenced:** `app/whale/events.py` (used only by `app/integrity/taostats_c8.py`; `whale_cohort`/`whale_events` tables empty; alive only with a TaoStats key) — keep, label dormant.
- **Two static front-ends** on one server: `frontend/wb/index.html` (437 lines, inline CSS/JS) and `frontend/ui/` (634 lines). Since 19 W7 made the terminal the workbench UI, `/wb` earns its place only for the vision upload.
- **Stale docs:** `README.md` describes Phases 0–2, 138 tests and a 53-job table; `INDEX.md` statuses lagged five phases (fixed 2026-10-18). The 15 spec §11 sections mix hundreds of "settled" lines with the few still-open ones → `specs/AS-BUILT.md` (N9).
- **Tests:** 8 min serial, the `tests/quant` fixture alone 52 s; no parallelism.
- **Config surface:** 656 keys / 613 lines of `.env.example` for a one-operator service — freeze it: no new key without a reader and a test.

### 5.2 Data on disk (safe to reclaim, operator confirms)

| Path | Size | Why it exists | Action |
|---|---|---|---|
| `data/tmp/pre_phase3_2026-09-24.db`, `pre_price_repair_2026-09-25.db`, `pre_seed_import_2026-10-12.db` | 3.1 GB | pre-migration safety copies (BUILD-LOG Phase 3) | delete after `backup verify` |
| `data/backfill/tmp/ledger.checkpointed.db` (+ shm/wal) | 2.4 GB | WAL-checkpointed Pi ledger used by S1 | delete; `seed/pi/` keeps the source |
| `data/backups/taosentinel-20261018T081800Z.db` (uncompressed twin of the `.gz`) | 4.9 GB | `DB_BACKUP_KEEP_UNCOMPRESSED=1` (fast restore / quant snapshot) | keep, or set 0 |
| `seed/` (`ledger_backfill.db` 339 MB, `seed/pi/` ≈ 2.2 GB) | 2.6 GB | S1/S2/S5/S7 inputs, all imported | move to the backup mirror once N8.3 exists |
| `pool_history` 1-min rows | ≈ 2–3 GB inside the DB | 17 §11 item 3 (`LEDGER_IMPORT_THIN_SEC=300` would have saved 0.7 GB) | leave; retention policies own it |

## 6. This spec is done when

N0–N4 are complete and recorded in BUILD-LOG, N2 has produced one gate verdict (pass or fail) on `quality_cg`, the paid-data decisions of N5 are taken (either way), and §5.1's inert-key test is green. N6–N9 are tracked here but may be re-planned after the verdict.

## Changelog

- **2026-10-18** — written from the post-Phase-12 audit (live DB, `/health`, logs, both test suites, every spec's §11). First scores, metric coverage, inert-key scan and disk inventory recorded as of 20:30Z.
- 2026-10-22: N2 item 3 built and run (BUILD-LOG 2026-10-22); schema v5.
- 2026-10-22: N2 item 2 (S10) built; live run pending operator go-ahead (writes ≈ 1.3 M append-only rows).
