-- TaoSentinel schema version 1 (baseline) — the DDL of specs/03-storage-schema.md §3.1–§3.7, in document order.
-- Generated verbatim from the spec's SQL blocks; edit the spec and regenerate rather than hand-editing.

-- class: append (one row per applied migration). Owner: 03.
CREATE TABLE IF NOT EXISTS schema_version (
  version     INTEGER PRIMARY KEY,           -- 1 = models.sql baseline; ≥2 = migrations/NNNN_*.sql
  name        TEXT    NOT NULL,              -- 'baseline' | migration file stem
  checksum    TEXT    NOT NULL,              -- sha256 hex of the migration file bytes
  applied_at  INTEGER NOT NULL               -- unix s
) STRICT;

-- class: state (row inserted at job start, finished at end). Owner: 01 CollectorJob wrapper; scripts too.
CREATE TABLE IF NOT EXISTS collector_runs (
  run_id        TEXT PRIMARY KEY,            -- uuid4 hex | 'sse:<connected_at>' | 'script:<name>:<started_at>'
  job           TEXT NOT NULL,               -- JOB_TABLE id (01 §2.7) or script name
  started_at    INTEGER NOT NULL,            -- unix s; default observed_at of rows written by the run
  finished_at   INTEGER,                     -- NULL = in flight, or orphaned by a crash/power loss
  ok            INTEGER CHECK (ok IN (0,1)), -- NULL in flight
  rows          INTEGER NOT NULL DEFAULT 0,
  error         TEXT,                        -- "ExcType: message", ≤ 500 chars
  cost_usd      REAL    NOT NULL DEFAULT 0.0,
  duration_sec  REAL,
  note          TEXT,                        -- JobOutcome.note (e.g. watermark)
  meta_json     TEXT                         -- per-job detail (04: head_block, per-source ok/failed)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_collector_runs_job_started ON collector_runs(job, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_collector_runs_orphans     ON collector_runs(started_at) WHERE finished_at IS NULL;

-- class: state. Per-job runtime watermarks only (01 §3; 02 §B12). Pause state lives in control_state.
CREATE TABLE IF NOT EXISTS job_state (
  job         TEXT NOT NULL,                 -- job id or script name
  key         TEXT NOT NULL,                 -- 'last_ok' | collector-defined watermark keys
  value_json  TEXT NOT NULL,
  updated_at  INTEGER NOT NULL,
  PRIMARY KEY (job, key)
) STRICT, WITHOUT ROWID;

-- class: state. The ONLY persistent store for operator pauses (02 §B12): API, Telegram and CLI all write it
-- (16 app/control.py pause()/resume()); survives restarts; `resume` deletes the row (and only the row). The file
-- sentinel list is closed: KILL_SWITCH (paid collectors + LLM; free LongTasks keep running), PAUSE_ALL (every
-- non-ops_exempt job), SCORING_KILLED (no new scores published); data/control/<target>.paused files are an
-- emergency input the app reads and never writes; per-provider kill files are retired.
CREATE TABLE IF NOT EXISTS control_state (
  target      TEXT NOT NULL,                 -- exact job id (01 JOB_TABLE) or a 16 PAUSE_GROUPS name (e.g. discord, x,
                                             -- llm); never prefix matching; a group target never pauses ops_exempt jobs
                                             -- or 14's telegram_bot/alert_dispatch
  key         TEXT NOT NULL CHECK (key IN ('paused')),
  value       TEXT NOT NULL CHECK (value IN ('true','false')),
  reason      TEXT,
  set_by      TEXT NOT NULL,                 -- 'api' | 'telegram' | 'cli' | operator name
  set_at      INTEGER NOT NULL,
  expires_at  INTEGER,                       -- NULL = until resumed
  PRIMARY KEY (target, key)
) STRICT, WITHOUT ROWID;

-- class: state. Cost governor ledger (16 app/costs.py, 02 §G5). One row per spend provider per UTC month;
-- increments only (commit/record/flush), never overwritten.
CREATE TABLE IF NOT EXISTS provider_spend (
  provider    TEXT NOT NULL,                 -- canonical provider id (02 §G1, same string in limiter, governor, /health):
                                             -- tmc | github | twitterapi_io | official_v2 | anthropic | taostats | … (groups x/llm/taostats aggregate at read time)
  month       TEXT NOT NULL CHECK (month GLOB '[0-9][0-9][0-9][0-9]-[0-1][0-9]'),
  usd         REAL NOT NULL DEFAULT 0.0,
  requests    INTEGER NOT NULL DEFAULT 0,    -- request counts (02 §G1: replaces 05's provider_usage; not `calls`);
                                             -- shared across processes so a restart cannot reset TMC_MONTHLY_CAP (90k)
  units_json  TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(units_json)),
                                             -- provider units summed per key, e.g. {"posts": n, "profiles": n,
                                             -- "input_tokens": n, "output_tokens": n} (record(..., units=…), 02 §G5)
  updated_at  INTEGER NOT NULL,
  PRIMARY KEY (provider, month)
) STRICT, WITHOUT ROWID;

-- class: state (mutable). Red-flag hysteresis state machine of spec 12 §2.8 (one row per occupant × flag × spec,
-- created on first raise or manual action, updated every scoring run). History = red_flag_raised/_cleared events.
-- Read by 12 (next run), 14 (/flags). Owner: 12. Columns fixed by 02 §B7: the state "clearing" is derived as
-- active = 1 AND false_since IS NOT NULL; origin 'human' maps to human = 1. sticky = owner_control_change whose
-- trigger row has value_json.identity_churn = true (stays active until a manual clear, 02 §D5). A manual clear sets
-- suppress_until = cleared_at + 30 d, but new trigger evidence (trigger ids in detail differing from the cleared
-- ones) re-raises immediately (02 §D5).
CREATE TABLE IF NOT EXISTS red_flag_state (
  occupant_key          TEXT    NOT NULL,
  flag                  TEXT    NOT NULL,    -- master §7.5 flag name (catalog §3.9 RED_FLAGS)
  spec_version          TEXT    NOT NULL,
  active                INTEGER NOT NULL CHECK (active IN (0,1)),
  raised_at             INTEGER,             -- slot of the last activation
  last_true_at          INTEGER,             -- slot of the last True evaluation
  false_since           INTEGER,             -- slot of the first False after the last True (hysteresis clock)
  cleared_at            INTEGER,
  human                 INTEGER NOT NULL DEFAULT 0 CHECK (human IN (0,1)),    -- held by a manual 'confirmed'
  sticky                INTEGER NOT NULL DEFAULT 0 CHECK (sticky IN (0,1)),   -- owner_control_change + identity churn
  last_manual_clear_at  INTEGER,             -- manual_flags.set_at of the last applied manual clear
  suppress_until        INTEGER,             -- post-mortem override window (manual clear + 30 d)
  last_eval             TEXT    CHECK (last_eval IN ('true','false','none')),
  last_eval_as_of       INTEGER NOT NULL,
  detail_json           TEXT    CHECK (detail_json IS NULL OR json_valid(detail_json)),  -- trigger inputs
  updated_at            INTEGER NOT NULL,
  PRIMARY KEY (occupant_key, flag, spec_version)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_red_flag_state_active ON red_flag_state(spec_version, active);

-- class: state (status/last_seen move; rows never deleted). Every occupant ever seen (master §5). Owner: 04; 17 adds history.
CREATE TABLE IF NOT EXISTS occupants (
  occupant_key          TEXT PRIMARY KEY,    -- "{netuid}:{network_registered_at}"
  netuid                INTEGER NOT NULL CHECK (netuid BETWEEN 0 AND 1023),
  network_registered_at INTEGER NOT NULL,    -- registration block
  status                TEXT NOT NULL CHECK (status IN ('active','pre_launch','deregistered')),
  name                  TEXT,                -- latest TMC name (convenience)
  symbol                TEXT,
  first_seen_block      INTEGER NOT NULL,
  first_seen_at         INTEGER NOT NULL,
  last_seen_block       INTEGER NOT NULL,
  last_seen_at          INTEGER NOT NULL,
  first_emission_block  INTEGER,
  activated_at          INTEGER,
  deregistered_at       INTEGER,
  dereg_reason          TEXT CHECK (dereg_reason IN ('reoccupied','absent','manual')),
  predecessor_key       TEXT,
  successor_key         TEXT,
  registered_at_est     INTEGER,             -- unix s estimate of the registration block's time (04: head-block math)
  registered_ts         INTEGER,             -- exact unix s of the registration block (Timestamp.Now, 17 block_times; 02 §J1)
  deregistered_ts       INTEGER,             -- exact unix s of the successor's registration block (17); NULL if unknown
  missing_runs          INTEGER NOT NULL DEFAULT 0,
  source                TEXT NOT NULL DEFAULT 'tmc'
                        CHECK (source IN ('tmc','rpc','legacy_ledger','onfinality','manual')),
  CHECK (occupant_key = netuid || ':' || network_registered_at)
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS ux_occupants_netuid_reg ON occupants(netuid, network_registered_at);
CREATE INDEX IF NOT EXISTS ix_occupants_netuid_status ON occupants(netuid, status);

-- class: state. Current occupant per netuid + denormalised resolved identity (written by 04 after merge R7).
CREATE TABLE IF NOT EXISTS subnets (
  netuid                INTEGER PRIMARY KEY CHECK (netuid BETWEEN 0 AND 1023),
  occupant_key          TEXT NOT NULL REFERENCES occupants(occupant_key),
  network_registered_at INTEGER NOT NULL,
  name                  TEXT,
  symbol                TEXT,
  status                TEXT NOT NULL CHECK (status IN ('active','pre_launch','deregistered')),
  is_active             INTEGER CHECK (is_active IN (0,1)),       -- TMC is_active (emission started)
  immune                INTEGER CHECK (immune IN (0,1)),          -- local rule: head − reg < NETWORK_IMMUNITY_PERIOD_BLOCKS
  owner_coldkey         TEXT,
  owner_hotkey          TEXT,
  head_block            INTEGER,
  website_url           TEXT,                -- identity_resolved(subnet_url).value_norm
  github_url            TEXT,                -- identity_resolved(github_repo)
  docs_url              TEXT,                -- identity_resolved(docs_url)
  discord_url           TEXT,                -- identity_resolved(discord)
  telegram_url          TEXT,                -- identity_resolved(telegram)
  x_handle              TEXT,                -- handle of identity_resolved(x_url), no '@'
  updated_at            INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_subnets_occupant ON subnets(occupant_key);

-- class: append (trigger-enforced). Per source, per field, PIT (master §6). Owner: 04.
CREATE TABLE IF NOT EXISTS identity_observations (
  id                INTEGER PRIMARY KEY,
  occupant_key      TEXT NOT NULL REFERENCES occupants(occupant_key),
  netuid            INTEGER NOT NULL,
  source            TEXT NOT NULL CHECK (source IN ('chain','subnetalpha','tmc_coldkey','tmc','manual','legacy')),
  field             TEXT NOT NULL,         -- 04 RESOLVED_FIELDS (subnet_name, github_repo, subnet_url, discord, x_url,
                                           -- telegram, docs_url, description, short_desc, logo_url, subnet_contact,
                                           -- owner_name, owner_url, owner_github, owner_discord, youtube, owner_coldkey,
                                           -- slug, twitter_embed_id) or '<field>__extra_<i>'
  value             TEXT,                  -- raw trimmed; '' = source reports empty
  value_norm        TEXT,
  ref_kind          TEXT,
  value_hash        TEXT NOT NULL,         -- sha256(value)
  is_junk           INTEGER NOT NULL DEFAULT 0 CHECK (is_junk IN (0,1)),
  junk_reason       TEXT,
  reclassified_from TEXT,
  observed_at       INTEGER NOT NULL,
  event_at          INTEGER,               -- subnetalpha modified_gmt; legacy as-of; NULL for chain (use block)
  block_number      INTEGER,               -- chain read block / TMC snapshot block
  run_id            TEXT NOT NULL,
  source_ref        TEXT
) STRICT;
CREATE INDEX IF NOT EXISTS ix_idobs_latest ON identity_observations(occupant_key, source, field, observed_at DESC);
CREATE INDEX IF NOT EXISTS ix_idobs_field  ON identity_observations(field, observed_at);

-- class: state. Did source S cover occupant O in its last ok run? (04 §4 absence semantics)
CREATE TABLE IF NOT EXISTS identity_coverage (
  source              TEXT NOT NULL,
  occupant_key        TEXT NOT NULL REFERENCES occupants(occupant_key),
  has_row             INTEGER NOT NULL CHECK (has_row IN (0,1)),   -- 0 = verifiably no record
  last_ok_at          INTEGER NOT NULL,
  last_ok_run_id      TEXT NOT NULL,
  last_ok_block       INTEGER,
  consecutive_misses  INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (source, occupant_key)
) STRICT, WITHOUT ROWID;

-- class: append. Raw payload audit, written only when the payload hash changes. Owner: 04.
CREATE TABLE IF NOT EXISTS identity_raw_snapshots (
  id            INTEGER PRIMARY KEY,
  source        TEXT NOT NULL,
  scope         TEXT NOT NULL CHECK (scope IN ('subnet','coldkey')),
  netuid        INTEGER,
  occupant_key  TEXT,
  source_ref    TEXT,
  payload_json  TEXT NOT NULL,
  payload_hash  TEXT NOT NULL,
  observed_at   INTEGER NOT NULL,
  event_at      INTEGER,
  block_number  INTEGER,
  run_id        TEXT NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_idraw ON identity_raw_snapshots(source, occupant_key, observed_at DESC);

-- class: state. Per-field resolved winner (history = identity_observations + events). Owner: 04.
CREATE TABLE IF NOT EXISTS identity_resolved (
  occupant_key        TEXT NOT NULL REFERENCES occupants(occupant_key),
  field               TEXT NOT NULL,
  value               TEXT,
  value_norm          TEXT,
  ref_kind            TEXT,
  source              TEXT,
  source_observed_at  INTEGER,
  status              TEXT NOT NULL CHECK (status IN ('present','absent','junk_only','unknown')),
  candidates_json     TEXT NOT NULL,
  override_id         INTEGER,
  flags_json          TEXT,
  resolved_at         INTEGER NOT NULL,
  run_id              TEXT NOT NULL,
  PRIMARY KEY (occupant_key, field)
) STRICT, WITHOUT ROWID;

-- class: state (status transitions only). Identity overrides (master §5 "manual overrides"). Owner: 04.
CREATE TABLE IF NOT EXISTS manual_overrides (
  id                   INTEGER PRIMARY KEY,
  occupant_key         TEXT NOT NULL REFERENCES occupants(occupant_key),
  field                TEXT NOT NULL,      -- identity field only (product status lives in manual_notes)
  mode                 TEXT NOT NULL CHECK (mode IN ('fallback','force','suppress')),
  value                TEXT,
  value_hash           TEXT,
  suppress_source      TEXT,
  suppress_value_hash  TEXT,
  source               TEXT NOT NULL CHECK (source IN ('manual','legacy')),
  source_ref           TEXT,
  status               TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','needs_review','rejected','retired')),
  reason               TEXT,
  evidence_url         TEXT,
  resolves_event_id    INTEGER,
  created_at           INTEGER NOT NULL,
  created_by           TEXT NOT NULL,
  retired_at           INTEGER
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS ux_manual_overrides ON manual_overrides(occupant_key, field, mode, source,
  COALESCE(value_hash,''), COALESCE(suppress_source,''), COALESCE(suppress_value_hash,''));

-- class: state (retire, never delete). Venues/targets per occupant (master §6). Owners: 04 (identity rows),
-- 07 (web_autodiscover docs rows), 08 (discord guild resolution, verified_at), 09 (feeds.csv rows, youtube channel ids).
CREATE TABLE IF NOT EXISTS watch_channels (
  id                INTEGER PRIMARY KEY,
  occupant_key      TEXT REFERENCES occupants(occupant_key),   -- NULL = ecosystem-wide feed (09 feeds.csv)
  netuid            INTEGER,                                   -- denormalised from occupant_key (NULL if ecosystem)
  platform          TEXT NOT NULL CHECK (platform IN ('discord_invite','discord_channel','discord_handle','x','telegram',
                      'youtube','youtube_video','website','docs','rss','medium','substack','google_news')),
  handle            TEXT NOT NULL,         -- invite code | channel id | x handle | tg handle | UC… id | normalised URL
  url               TEXT,
  label             TEXT,                  -- feeds.csv label
  source            TEXT NOT NULL CHECK (source IN ('chain','subnetalpha','tmc','tmc_coldkey','manual','legacy',
                      'operator','web_autodiscover','seed_feeds')),
  priority          INTEGER NOT NULL DEFAULT 50,   -- lower wins: chain 10, subnetalpha 20, tmc/tmc_coldkey 30,
                                                   -- manual(force) 5, manual(fallback)/legacy 40, operator 45, autodiscover 90
  is_resolved       INTEGER NOT NULL DEFAULT 0 CHECK (is_resolved IN (0,1)),   -- equals the resolved winner
  is_main_guild     INTEGER NOT NULL DEFAULT 0 CHECK (is_main_guild IN (0,1)),
  guild_id          TEXT,
  needs_resolution  INTEGER NOT NULL DEFAULT 0 CHECK (needs_resolution IN (0,1)),
  filtered          INTEGER NOT NULL DEFAULT 0 CHECK (filtered IN (0,1)),     -- 09: keyword-filter publisher feed
  verified_at       INTEGER,               -- set only by the collector that validated the target
  first_seen_at     INTEGER NOT NULL,
  last_seen_at      INTEGER NOT NULL,
  retired_at        INTEGER,
  active            INTEGER GENERATED ALWAYS AS (retired_at IS NULL) VIRTUAL,
  meta_json         TEXT
) STRICT;
-- NULL occupant_key must still be unique per (platform, handle): writers use INSERT OR IGNORE, which honours
-- expression unique indexes, then UPDATE by id.
CREATE UNIQUE INDEX IF NOT EXISTS ux_watch_channels ON watch_channels(COALESCE(occupant_key,''), platform, handle);
CREATE INDEX IF NOT EXISTS ix_watch_platform ON watch_channels(platform, retired_at);
CREATE INDEX IF NOT EXISTS ix_watch_guild ON watch_channels(guild_id) WHERE guild_id IS NOT NULL;

-- class: state (status transitions; history = team_member_* events, which are the PIT audit trail). Manual,
-- evidence-linked team table (master §6, checklist D1). Owner: 11 (seed, verify/reject domain API); 14 is the
-- transport (API/Telegram), calling 11's domain functions (02 §H3). PIT: a member counts at t iff verified_at ≤ t and (rejected_at IS NULL or > t);
-- team_doxxed is forward-only (master §10), so in-place status edits are acceptable.
CREATE TABLE IF NOT EXISTS team_members (
  id                  INTEGER PRIMARY KEY,
  occupant_key        TEXT    NOT NULL REFERENCES occupants(occupant_key),
  netuid              INTEGER NOT NULL,
  name                TEXT    NOT NULL CHECK (length(name) <= 120),
  name_norm           TEXT    NOT NULL,      -- 11 §2.2.3 step 5 normalisation (dedupe key)
  role                TEXT    CHECK (role IS NULL OR length(role) <= 60),
  profile_url         TEXT,
  profile_kind        TEXT    CHECK (profile_kind IN ('linkedin','x','github','website','other')),
  evidence_url        TEXT,                  -- primary evidence; required once status = 'verified'
  evidence_note       TEXT,
  evidence_json       TEXT    NOT NULL DEFAULT '[]' CHECK (json_valid(evidence_json)),
                                             -- [{url, domain, class:'personal'|'attestation'|'excluded', added_by, added_at, note}]
  evidence_count      INTEGER NOT NULL DEFAULT 0,   -- distinct non-excluded registrable domains (profile_url ∪ evidence)
  status              TEXT    NOT NULL DEFAULT 'candidate' CHECK (status IN ('candidate','verified','rejected')),
  source              TEXT    NOT NULL CHECK (source IN ('subnetalpha','legacy','manual','llm')),
  source_ref          TEXT,                  -- subnetalpha link@modified_gmt | legacy <file>:<line> | api | telegram | cli
  verified_by         TEXT,
  verified_at         INTEGER,               -- first verification (unix s)
  rejected_by         TEXT,
  rejected_at         INTEGER,
  reject_reason       TEXT,
  first_seen_at       INTEGER NOT NULL,      -- row creation
  last_checked_at     INTEGER,               -- last human verify / re-verify
  source_last_seen_at INTEGER,               -- last seed run whose parse still listed the name
  created_by          TEXT    NOT NULL,      -- seed_subnetalpha | seed_legacy | cli:<by> | api | telegram
  verified            INTEGER GENERATED ALWAYS AS (status = 'verified') VIRTUAL,   -- spec 14 compatibility
  created_at          INTEGER GENERATED ALWAYS AS (first_seen_at) VIRTUAL,        -- spec 14 compatibility
  CHECK (status <> 'verified' OR (evidence_url IS NOT NULL AND evidence_count >= 2
                                  AND verified_at IS NOT NULL AND verified_by IS NOT NULL)),   -- checklist D1
  UNIQUE (occupant_key, name_norm)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_team_members_occ ON team_members(occupant_key, status);

-- class: state. KOL registry (09 seed; operator edits credibility/verified/active/notes — never overwritten by re-seed).
CREATE TABLE IF NOT EXISTS kol_accounts (
  handle              TEXT PRIMARY KEY,    -- case-preserved
  handle_lower        TEXT NOT NULL UNIQUE,
  platform            TEXT NOT NULL DEFAULT 'x' CHECK (platform IN ('x')),
  author_id           TEXT,                -- X user id once resolved (x_accounts)
  display_name        TEXT,
  category            TEXT NOT NULL CHECK (category IN ('official','founder','explorer','fund','media','analyst','community')),
  credibility         REAL NOT NULL CHECK (credibility BETWEEN 0 AND 1),
  verified            INTEGER NOT NULL DEFAULT 0 CHECK (verified IN (0,1)),
  active              INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
  team_occupant_keys  TEXT,                -- JSON array; KOLs that are also a subnet's team account
  added_at            INTEGER NOT NULL,
  added_by            TEXT NOT NULL CHECK (added_by IN ('seed','operator')),
  notes               TEXT,
  updated_at          INTEGER NOT NULL
) STRICT;

-- class: append (new row per change; latest row per (occupant_key, kind) is current). Notes, product status,
-- aliases, LLM narratives, seed edge briefs. Owners: 14 (API/Telegram), 04 (seed), 10 (narrative/alias), 11.
CREATE TABLE IF NOT EXISTS manual_notes (
  id            INTEGER PRIMARY KEY,
  occupant_key  TEXT REFERENCES occupants(occupant_key),     -- NULL = ecosystem-wide
  kind          TEXT NOT NULL CHECK (kind IN ('note','product_status','alias','llm_narrative','seed_edge','legacy',
                                              'team_disclosure')),
  value         TEXT,        -- product_status: none|waitlist|beta|live, NULL = cleared (→ None); alias: alias string;
                             -- team_disclosure (11): anonymous|clear (operator statement feeding team_doxxed absence)
  title         TEXT,
  body          TEXT,        -- free text: note, narrative (≤ 300 words), edge-brief excerpt
  evidence_url  TEXT,
  payload_json  TEXT,        -- llm_narrative: {risks[3], catalysts[3], confidence, model, evidence_sha256, tokens, cost_usd, ...}
  ref_event_id  INTEGER,     -- e.g. the netuid_reoccupied event a review note answers (04 needs-review)
  source_ref    TEXT,        -- 'research/subnet_edge/sn64_chutes.md', 'fundamentals.json@2026-06-02'
  created_at    INTEGER NOT NULL,
  created_by    TEXT NOT NULL,   -- 'api' | 'telegram' | 'cli' | 'seed_registry' | 'llm:<model>' | operator
  CHECK (kind <> 'product_status' OR value IS NULL OR value IN ('none','waitlist','beta','live')),
  CHECK (kind <> 'product_status' OR value IS NULL OR evidence_url IS NOT NULL),
  CHECK (kind <> 'team_disclosure' OR value IN ('anonymous','clear'))
) STRICT;
CREATE INDEX IF NOT EXISTS ix_manual_notes_occ_kind ON manual_notes(occupant_key, kind, created_at DESC);

-- class: append (trigger-enforced). Human-confirmed red flags and manual clears (14 /manual/flag, Telegram /flag,
-- 11 confirm_flag / key-compromise override). Latest row per (occupant_key, flag) with set_at ≤ cutoff is
-- authoritative; spec 12 §2.8 reads it (human hold, manual clear, 30-d suppression). 02 §D5: `confirmed` only for
-- owner_extraction and key_compromise; `cleared` for those two and owner_control_change, and only while the flag is
-- active (auto-raised or confirmed; 14 answers 409 flag_not_active otherwise). A human-confirmed
-- owner_extraction_evidence label (10) is entered here as flag owner_extraction.
CREATE TABLE IF NOT EXISTS manual_flags (
  id            INTEGER PRIMARY KEY,
  occupant_key  TEXT NOT NULL REFERENCES occupants(occupant_key),
  flag          TEXT NOT NULL CHECK (flag IN ('owner_extraction','key_compromise','owner_control_change')),
  state         TEXT NOT NULL CHECK (state IN ('confirmed','cleared')),
  evidence_url  TEXT NOT NULL,
  note          TEXT,
  set_by        TEXT NOT NULL,
  set_at        INTEGER NOT NULL,
  -- only owner_extraction / key_compromise are human-confirmable; owner_control_change may only be cleared (02 §D5)
  CHECK (state = 'cleared' OR flag IN ('owner_extraction','key_compromise'))
) STRICT;
CREATE INDEX IF NOT EXISTS ix_manual_flags_occ ON manual_flags(occupant_key, flag, set_at DESC);

-- class: state (status transitions). Items needing a human decision: 10 (LLM flag candidates), 11 (team candidates,
-- owner control/extraction/lock/source reviews; 11 owns the review domain functions, 02 §A2, and its kind
-- vocabulary). 04's needs-review list stays computed (no storage). 11's enqueue() merges into the OPEN row with the
-- same dedupe_base, else inserts dedupe_key = f"{dedupe_base}|{created_at}".
CREATE TABLE IF NOT EXISTS review_queue (
  id                 INTEGER PRIMARY KEY,
  occupant_key       TEXT,                 -- NULL only for ecosystem-wide items
  kind               TEXT NOT NULL CHECK (kind IN ('llm_flag_candidate',                          -- 10
                       'team_candidate','team_reverify','team_delisted',                            -- 11 team
                       'owner_extraction_candidate','owner_change_whitelist',                       -- 11 owner
                       'owner_source_disagreement','other')),
  suggested_flag     TEXT CHECK (suggested_flag IN ('owner_extraction','key_compromise','security_incident')),
  post_id            TEXT,                 -- 10: post_labels.post_id
  label_version      TEXT,                 -- 10: classifier_version of the label
  event_type         TEXT,
  materiality        INTEGER CHECK (materiality IS NULL OR materiality BETWEEN 1 AND 3),
  summary            TEXT,
  evidence_url       TEXT,
  ref_table          TEXT,                 -- 11: 'owner_changes' | 'team_members' | 'events'
  ref_id             INTEGER,
  priority           INTEGER NOT NULL DEFAULT 2 CHECK (priority BETWEEN 1 AND 3),   -- 1 urgent … 3 low
  payload_json       TEXT CHECK (payload_json IS NULL OR json_valid(payload_json)),
  dedupe_key         TEXT,                 -- 11: "{dedupe_base}|{created_at}"; 10: "{kind}|{post_id}|{label_version}"
  dedupe_base        TEXT,                 -- 11: e.g. "team_candidate|64:4123456", "owner_change_whitelist|<change_id>"
  created_at         INTEGER NOT NULL,
  status             TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','confirmed','rejected','resolved','expired')),
  resolved_by        TEXT,
  resolved_at        INTEGER,
  resolution_note    TEXT,
  resolution_action  TEXT CHECK (resolution_action IN ('confirm_flag','whitelist','key_compromise','dismiss','done',
                                                       'auto_cleared'))
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS ux_review_queue_post ON review_queue(kind, post_id, label_version)
  WHERE post_id IS NOT NULL;                                                    -- 10: one review per label, forever
CREATE UNIQUE INDEX IF NOT EXISTS ux_review_open_dedupe ON review_queue(dedupe_key)
  WHERE dedupe_key IS NOT NULL AND status = 'open';                             -- one OPEN item per key
CREATE INDEX IF NOT EXISTS ix_review_queue_open ON review_queue(status, priority, created_at);
CREATE INDEX IF NOT EXISTS ix_review_queue_base ON review_queue(dedupe_base, status) WHERE dedupe_base IS NOT NULL;

-- class: state (retire, never delete). Operator whitelist for disclosed multisig migrations, post-compromise
-- recoveries and false positives (master §7.3 control_stability_90d). PIT: effective for decisions with
-- t ≥ created_at and (retired_at IS NULL or t < retired_at). Owner: 11.
CREATE TABLE IF NOT EXISTS owner_event_whitelist (
  id            INTEGER PRIMARY KEY,
  occupant_key  TEXT    NOT NULL REFERENCES occupants(occupant_key),
  netuid        INTEGER NOT NULL,
  reason        TEXT    NOT NULL CHECK (reason IN ('multisig_migration','post_compromise_recovery','disclosed_transfer',
                                                   'hotkey_rotation','initial_alpha_accounted','false_positive','other')),
  applies_to    TEXT    NOT NULL CHECK (applies_to IN ('coldkey_change','hotkey_repoint','identity_churn','stake_events',
                                                       'perpetual_off','extraction','initial_alpha','all')),
  window_start  INTEGER NOT NULL,        -- unix s, inclusive
  window_end    INTEGER NOT NULL,        -- unix s, inclusive
  old_coldkey   TEXT,                    -- optional exact-match constraints
  new_coldkey   TEXT,
  owner_change_id INTEGER,               -- owner_changes.id (optional)
  evidence_url  TEXT    NOT NULL,
  note          TEXT,
  created_by    TEXT    NOT NULL,
  created_at    INTEGER NOT NULL,
  retired_by    TEXT,
  retired_at    INTEGER,
  CHECK (window_end >= window_start),
  CHECK (applies_to <> 'perpetual_off' OR reason IN ('post_compromise_recovery','false_positive'))
) STRICT;
CREATE INDEX IF NOT EXISTS ix_owner_wl_occ ON owner_event_whitelist(occupant_key, window_start);

-- class: state. X account registry (09). kind='kol' rows reference kol_accounts.
CREATE TABLE IF NOT EXISTS x_accounts (
  author_id            TEXT PRIMARY KEY,
  handle               TEXT NOT NULL,
  handle_lower         TEXT NOT NULL UNIQUE,
  kind                 TEXT NOT NULL CHECK (kind IN ('team','kol')),
  kol_handle           TEXT REFERENCES kol_accounts(handle),
  status               TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','gone','unresolved')),
  since_id             TEXT,
  resolved_at          INTEGER,
  last_polled_at       INTEGER,
  last_post_at         INTEGER,
  last_error           TEXT,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  provider             TEXT,
  created_at           INTEGER NOT NULL
) STRICT;

-- class: state. Team account ↔ occupant mapping (shared handles map to several occupants).
CREATE TABLE IF NOT EXISTS x_account_occupants (
  author_id     TEXT NOT NULL REFERENCES x_accounts(author_id),
  occupant_key  TEXT NOT NULL REFERENCES occupants(occupant_key),
  source        TEXT NOT NULL,
  PRIMARY KEY (author_id, occupant_key)
) STRICT, WITHOUT ROWID;

-- class: state. Mention-search watermarks (09).
CREATE TABLE IF NOT EXISTS x_search_state (
  query_key      TEXT PRIMARY KEY,
  since_id       TEXT,
  last_run_at    INTEGER,
  last_posts     INTEGER,
  last_cost_usd  REAL
) STRICT;

-- class: state. RSS/news/YouTube poller state (09 Appendix A).
CREATE TABLE IF NOT EXISTS feed_state (
  feed_url              TEXT PRIMARY KEY,
  kind                  TEXT NOT NULL,       -- watch_channels.platform
  occupant_key          TEXT,
  effective_url         TEXT,
  etag                  TEXT,
  last_modified         TEXT,                -- HTTP header value verbatim (not a timestamp column)
  last_fetched_at       INTEGER,
  last_status           INTEGER,
  consecutive_failures  INTEGER NOT NULL DEFAULT 0,
  items_seen            INTEGER NOT NULL DEFAULT 0,
  last_new_item_at      INTEGER,
  enabled               INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0,1))
) STRICT;

-- class: append. Per-block hot fields from the TMC SSE table (master §6 "12 hot fields per block"). Owner: 05.
-- Written every CHAIN_TICKS_BLOCK_STRIDE blocks (default 5 ≈ 1 min, + on flag changes). Kept
-- CHAIN_TICKS_RETENTION_DAYS (30; 02 §F2), then only chain_hourly + 5m bars survive (§2.7 chain_prune).
-- No event_at index: retention walks the PK per netuid (disk, §6).
CREATE TABLE IF NOT EXISTS chain_ticks (
  netuid                       INTEGER NOT NULL,
  block_number                 INTEGER NOT NULL,
  occupant_key                 TEXT    NOT NULL,
  event_at                     INTEGER NOT NULL,   -- payload block timestamp if present, else arrival (05 §2.1)
  observed_at                  INTEGER NOT NULL,
  source                       TEXT    NOT NULL CHECK (source IN ('sse','rest_table')),
  run_id                       TEXT,               -- 'sse:<connected_at>' or the REST-fallback run
  price                        REAL,               -- τ per α
  subnet_moving_price          REAL,               -- on-chain EMA price (drives pruning/emission)
  emission                     REAL,               -- % of total TAO emission, post-gate
  emission_enabled             INTEGER CHECK (emission_enabled IN (0,1)),
  immune                       INTEGER CHECK (immune IN (0,1)),              -- TMC value (quirk: false before start)
  deregistration_risk          INTEGER CHECK (deregistration_risk IN (0,1)),
  excess_tao                   REAL,               -- rao/block ("chain buys")
  subnet_ema_tao_flow          REAL,
  owner_personal_locked_alpha  REAL,               -- rao-scaled α as delivered (÷1e9 = α; SN64 613237766512173 ≈ 613 k α)
  total_locked_alpha           REAL,               -- rao-scaled α (÷1e9)
  total_subnet_stake           REAL,               -- rao-scaled α (÷1e9)
  tao_liquidity                REAL,               -- pool TAO in rao (÷1e9 = τ; SN64 199012885911739 ≈ 199 k τ)
  PRIMARY KEY (netuid, block_number)
) STRICT, WITHOUT ROWID;

-- class: precedence. Last state of each UTC hour for every TMC table field (master §6 "others hourly").
-- Named columns = ALL 44 fields of /subnets/table/ and the SSE `full`/`delta` rows (identical key set, verified
-- against the research probe dumps of 2026-09-22, block 9,123,732; `subnet` is stored as netuid) + the local
-- immune_local + analytics-only fields. Units are as delivered (mixed: see comments). A field TMC adds later is kept
-- verbatim in extras_json (and 05 logs tmc_schema_drift) until an additive migration promotes it. Owner: 05; 17 backfills.
CREATE TABLE IF NOT EXISTS chain_hourly (
  netuid                          INTEGER NOT NULL,  -- TMC `subnet`
  hour_ts                         INTEGER NOT NULL,
  occupant_key                    TEXT    NOT NULL,
  block_number                    INTEGER,           -- TMC `block_number`: last block of the hour seen (analytics: point block)
  event_at                        INTEGER NOT NULL,  -- time of the state written (last tick) / analytics point ts
  observed_at                     INTEGER NOT NULL,
  source                          TEXT    NOT NULL CHECK (source IN ('sse','rest_table','tick_rollup','analytics_1w','analytics_1m')),
  run_id                          TEXT,
  backfilled                      INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),
  name                            TEXT,
  symbol                          TEXT,
  logo_url                        TEXT,
  is_active                       INTEGER CHECK (is_active IN (0,1)),
  emission_enabled                INTEGER CHECK (emission_enabled IN (0,1)),
  immune                          INTEGER CHECK (immune IN (0,1)),   -- TMC value (quirk: False before start)
  immune_local                    INTEGER CHECK (immune_local IN (0,1)),   -- (block − reg) < NETWORK_IMMUNITY_PERIOD_BLOCKS (864000)
  deregistration_risk             INTEGER CHECK (deregistration_risk IN (0,1)),
  price                           REAL,              -- τ per α
  price_after                     REAL,              -- τ per α after all unlocked stake sells (exit capacity)
  price_difference_hour           REAL,              -- % change
  price_difference_day            REAL,
  price_difference_week           REAL,
  price_difference_month          REAL,
  subnet_moving_price             REAL,              -- τ per α (on-chain EMA)
  emission                        REAL,              -- % of total TAO emission, post-gate
  excess_tao                      REAL,              -- rao/block ("chain buys")
  subnet_ema_tao_flow             REAL,              -- rao, signed
  owner_personal_stake            REAL,              -- rao-scaled α (÷1e9)
  owner_personal_locked_alpha     REAL,              -- rao-scaled α (÷1e9)
  owner_personal_perpetual_alpha  REAL,              -- rao-scaled α (÷1e9)
  owner_personal_decaying_alpha   REAL,              -- rao-scaled α (÷1e9)
  total_locked_alpha              REAL,              -- rao-scaled α (÷1e9)
  total_subnet_stake              REAL,              -- rao-scaled α (÷1e9)
  tao_liquidity                   REAL,              -- pool TAO, rao (÷1e9 = τ)
  alpha_liquidity                 REAL,              -- pool α, rao-scaled (÷1e9)
  circulating_supply              REAL,              -- rao-scaled α (÷1e9)
  marketcap                       REAL,              -- τ (= price × circulating α; SN64 ≈ 384,841)
  marketcap_rank                  INTEGER,
  fdv                             REAL,              -- rao (÷1e9 = τ)
  volume                          REAL,              -- τ, window UNVERIFIED (likely 24 h)
  miners_tao_per_day              REAL,              -- τ/day
  neuron_regs_burned_24h          REAL,              -- rao burned by neuron registrations, 24 h (UNVERIFIED unit)
  root_prop                       REAL,              -- %
  adr                             REAL,              -- alpha distribution ratio
  network_registered_at           INTEGER,
  subnet_owner                    TEXT,              -- owner coldkey ss58 (root: 0x00…)
  commits_7d                      INTEGER,
  commits_30d                     INTEGER,
  commits_90d                     INTEGER,
  chain_buys_per_block            REAL,              -- legacy flow-model field; stored, not used
  subsidized_breakpoint           REAL,              -- legacy; stored, not used
  is_subsidized                   INTEGER CHECK (is_subsidized IN (0,1)),
  total_tao_buy_24h               REAL,              -- analytics-only fields below (NULL on SSE rows)
  total_tao_sell_24h              REAL,
  volume_24h                      REAL,
  tx_count_24h                    INTEGER,
  neuron_registrations_24h        INTEGER,
  rao_recycled_24h                REAL,
  tao_price_usd                   REAL,
  extras_json                     TEXT,              -- any delivered field without a named column, verbatim
  PRIMARY KEY (netuid, hour_ts)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_chain_hourly_occ ON chain_hourly(occupant_key, hour_ts);

-- class: precedence. TMC analytics daily EOD rows (span=1Y) + daily rows derived from the 1W hourly pull. Owner: 05.
CREATE TABLE IF NOT EXISTS chain_daily (
  netuid                         INTEGER NOT NULL,
  day_ts                         INTEGER NOT NULL,
  occupant_key                   TEXT    NOT NULL,
  block_number                   INTEGER,
  event_at                       INTEGER NOT NULL,  -- analytics point timestamp
  observed_at                    INTEGER NOT NULL,
  source                         TEXT    NOT NULL CHECK (source IN ('analytics_1y','analytics_1w')),
  run_id                         TEXT,
  backfilled                     INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),
  subnet_tao                     REAL,              -- pool depth, rao (÷1e9 = τ; research probe SN51 153227189355104)
  total_supply                   REAL,
  circulation_supply             REAL,
  emission_percent               REAL,              -- = emission share %
  daily_emission                 REAL,
  daily_inflation                REAL,
  total_locked                   REAL,
  owner_personal_locked_alpha    REAL,
  owner_personal_decaying_alpha  REAL,
  total_decaying                 REAL,
  total_tao_buy                  REAL,
  total_tao_sell                 REAL,
  total_tao_buy_24h              REAL,
  total_tao_sell_24h             REAL,
  volume_24h                     REAL,
  tx_count_24h                   INTEGER,
  tx_amount_24h                  REAL,
  neuron_registrations_24h       INTEGER,
  rao_recycled_24h               REAL,
  chain_buys                     REAL,
  tao_price_usd                  REAL,
  tao_price_eur                  REAL,
  extras_json                    TEXT,
  PRIMARY KEY (netuid, day_ts)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_chain_daily_occ ON chain_daily(occupant_key, day_ts);

-- class: append. /analytics/trending/subnets/?limit=50, one row per listed subnet per pull. Owner: 05.
CREATE TABLE IF NOT EXISTS chain_trending (
  observed_at        INTEGER NOT NULL,
  netuid             INTEGER NOT NULL,
  occupant_key       TEXT,
  rank               INTEGER,
  views              INTEGER,
  visitors           INTEGER,
  percentage_change  REAL,
  event_at           INTEGER,
  source             TEXT NOT NULL DEFAULT 'tmc_trending',
  run_id             TEXT,
  PRIMARY KEY (observed_at, netuid)
) STRICT, WITHOUT ROWID;

-- class: rollup. Daily trending views per occupant for P6 attention_7d (10). Written by 05's daily job from the
-- day's LAST successful chain_trending pull: views = listed value, 0 when the occupant is absent from a successful
-- pull (verifiable absence); no row when every pull that day failed (→ NULL for 10). Replaces 10's
-- chain_daily.tmc_trending_views.
CREATE TABLE IF NOT EXISTS chain_trending_daily (
  day_ts        INTEGER NOT NULL,
  occupant_key  TEXT    NOT NULL,
  netuid        INTEGER NOT NULL,
  views         INTEGER NOT NULL,
  visitors      INTEGER,
  n_pulls_ok    INTEGER NOT NULL,
  observed_at   INTEGER NOT NULL,           -- = observed_at of the pull used
  run_id        TEXT,
  PRIMARY KEY (day_ts, occupant_key)
) STRICT, WITHOUT ROWID;

-- class: append. /extrinsics/staking-activity/pressure/ bucket=1d. Owner: 05.
CREATE TABLE IF NOT EXISTS chain_pressure (
  day_ts       INTEGER NOT NULL,
  side         TEXT    NOT NULL CHECK (side IN ('buy','sell')),
  netuid       INTEGER NOT NULL DEFAULT -1,   -- -1 = network-wide
  value_rao    REAL,
  ema_price    REAL,
  event_at     INTEGER,                       -- timestamp_ms / 1000
  observed_at  INTEGER NOT NULL,
  source       TEXT NOT NULL DEFAULT 'tmc_pressure',
  run_id       TEXT,
  PRIMARY KEY (day_ts, side, netuid, observed_at)
) STRICT, WITHOUT ROWID;

-- class: append. Network-level regime inputs (Σ subnet prices, TAO/USD). Owner: 05; read by 12 regime.
CREATE TABLE IF NOT EXISTS chain_network_hourly (
  hour_ts            INTEGER PRIMARY KEY,
  block_number       INTEGER,
  sum_subnet_prices  REAL,
  n_active           INTEGER,
  total_subnet_tao   REAL,
  tao_usd            REAL,
  tao_usd_source     TEXT,
  event_at           INTEGER,
  observed_at        INTEGER NOT NULL,
  source             TEXT NOT NULL DEFAULT 'sse',
  run_id             TEXT
) STRICT, WITHOUT ROWID;

-- class: append. Chain constants: TMC /general/constants/ (runtime defaults), live RPC storage values, config
-- snapshot. Merges 05's rpc_constants (source='rpc_storage'). Owner: 05.
CREATE TABLE IF NOT EXISTS chain_constants (
  name          TEXT    NOT NULL,             -- e.g. NetworkImmunityPeriod, EmissionBarQuantile, SubnetOwnerCut
  source        TEXT    NOT NULL CHECK (source IN ('tmc_constants','rpc_storage','config')),
  observed_at   INTEGER NOT NULL,
  value         TEXT    NOT NULL,             -- decimal/JSON string as delivered
  block_number  INTEGER,
  endpoint      TEXT,                         -- RPC endpoint used (rpc_storage)
  event_at      INTEGER,
  run_id        TEXT,
  PRIMARY KEY (name, source, observed_at)
) STRICT, WITHOUT ROWID;

-- class: append (retention 90 d, newest per endpoint kept). Raw sink for endpoints whose shape is unverified or
-- consumed by other specs (/subnets/, /accounts/coldkey-identities/, /subnets/burn/, …). Writers: whichever spec fetches the
-- endpoint through TmcClient — 04 (/subnets/, /accounts/coldkey-identities/, 02 §G2), 05, 11; any spec may read. DDL owner: 03.
CREATE TABLE IF NOT EXISTS chain_rest_snapshots (
  endpoint      TEXT    NOT NULL,             -- path template, e.g. '/subnets/'
  params        TEXT    NOT NULL DEFAULT '',  -- canonical query string (sorted)
  observed_at   INTEGER NOT NULL,
  run_id        TEXT    NOT NULL,
  http_status   INTEGER,
  payload_json  TEXT,
  event_at      INTEGER,
  source        TEXT NOT NULL DEFAULT 'tmc',
  PRIMARY KEY (endpoint, params, observed_at)
) STRICT, WITHOUT ROWID;

-- class: append. Weekly holder concentration (/accounts/subnet-holders/{n}/). Owner: 05.
CREATE TABLE IF NOT EXISTS holders_snapshots (
  netuid         INTEGER NOT NULL,
  observed_at    INTEGER NOT NULL,
  occupant_key   TEXT    NOT NULL,
  run_id         TEXT    NOT NULL,
  source         TEXT    NOT NULL DEFAULT 'tmc_holders',
  event_at       INTEGER,
  holders_count  INTEGER,
  n_fetched      INTEGER,
  top1_share     REAL,
  top10_share    REAL,
  hhi_topn       REAL,
  gini_topn      REAL,
  rows_json      TEXT,                        -- [{rank, coldkey, percentage, total_alpha}] fetched top-N
  PRIMARY KEY (netuid, observed_at)
) STRICT, WITHOUT ROWID;

-- class: precedence (§3.3 ranks). OHLC(V) bars: SSE rollups from chain_ticks (period '5m' and the live hourly
-- rollup, period '1h', both source='sse_rollup'; the CHECKs below deliberately do not tie source to period),
-- TMC 1h candles (gap fill), imported ledger 1h/1d, archive-RPC backfill (OnFinality / opentensor archive).
-- Owner: 05 (5m + 1h sse_rollup, tmc 1h), 17 (ledger, RPC, enrichment).
-- Read by 13 panel builder (via v_quant_price_bars).
CREATE TABLE IF NOT EXISTS price_bars (
  netuid            INTEGER NOT NULL,
  period            TEXT    NOT NULL CHECK (period IN ('5m','1h','1d')),
  ts                INTEGER NOT NULL,         -- bucket start (aligned)
  occupant_key      TEXT,                     -- NULL = unattributable (seed era, §2.9)
  open              REAL,
  high              REAL,
  low               REAL,
  close             REAL    NOT NULL CHECK (close > 0),
  volume            REAL,                     -- tmc_candle: as delivered; ledger/RPC: Δ of pool_history.subnet_volume
                                              -- (§2.9, unit τ vs α UNVERIFIED); sse_rollup: NULL
  start_volume      REAL,                     -- TMC candle field
  tao_price_usd     REAL,                     -- TMC candle field
  ts_last           INTEGER,                  -- timestamp of the close sample; NULL for tmc_candle
  tao_in_pool       REAL,                     -- τ; NULL when the source lacks reserves
  alpha_in_pool     REAL,                     -- α (TAO units, alpha/1e9)
  moving_price      REAL,
  block_number      INTEGER,                  -- chain-accurate block of the close sample (ledger/sse/onfinality)
  tmc_block_number  INTEGER,                  -- TMC candle label; NOT chain-accurate, never join on it
  source            TEXT    NOT NULL CHECK (source IN ('sse_rollup','tmc_candle','ledger_import','onfinality',
                                                       'opentensor_archive','taostats')),
  event_at          INTEGER,                  -- = ts_last when known
  observed_at       INTEGER NOT NULL,         -- = ts_last only for ledger bars captured live (ts_last ≥ 1777468595,
                                              -- not enriched, §2.9); else write time
  backfilled        INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),   -- 1 = reconstructed after the fact (17)
  run_id            TEXT,
  PRIMARY KEY (netuid, period, ts)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_price_bars_occ ON price_bars(occupant_key, period, ts) WHERE occupant_key IS NOT NULL;

-- class: append. Optional RPC/lite-node daily reads (05 §2 optional collector). Owner: 05.
CREATE TABLE IF NOT EXISTS rpc_subnet_daily (
  netuid            INTEGER NOT NULL,
  as_of             INTEGER NOT NULL,
  occupant_key      TEXT    NOT NULL,
  block_number      INTEGER,
  observed_at       INTEGER NOT NULL,
  event_at          INTEGER,
  source            TEXT NOT NULL DEFAULT 'rpc_storage',
  run_id            TEXT,
  miner_burned      REAL,                     -- MinerBurned (U96F32 → float)
  subnet_owner_cut  REAL,
  emission_enabled  INTEGER CHECK (emission_enabled IN (0,1)),
  moving_price      REAL,
  endpoint          TEXT,
  PRIMARY KEY (netuid, as_of)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS rpc_metagraph_daily (
  netuid                    INTEGER NOT NULL,
  as_of                     INTEGER NOT NULL,
  occupant_key              TEXT    NOT NULL,
  block_number              INTEGER,
  observed_at               INTEGER NOT NULL,
  event_at                  INTEGER,
  source                    TEXT NOT NULL DEFAULT 'rpc_metagraph',
  run_id                    TEXT,
  n_uids                    INTEGER,
  max_uids                  INTEGER,
  active_validators         INTEGER,
  active_miners             INTEGER,
  permit_count              INTEGER,
  vtrust_mean               REAL,
  vtrust_pstdev             REAL,
  incentive_to_owner_share  REAL,
  endpoint                  TEXT,
  PRIMARY KEY (netuid, as_of)
) STRICT, WITHOUT ROWID;

-- class: append (one fit per as_of per model version). v440 gate reproduction (05 §2.3). Derived intermediate.
CREATE TABLE IF NOT EXISTS gate_fits (
  as_of          INTEGER NOT NULL,
  model_version  TEXT    NOT NULL,            -- 'gate-v440-1'
  block_number   INTEGER,
  n_eligible     INTEGER,
  bar_rank       INTEGER,
  exponent       REAL,
  theta          REAL,
  residual_pp    REAL,
  iterations     INTEGER,
  converged      INTEGER CHECK (converged IN (0,1)),
  degraded       INTEGER CHECK (degraded IN (0,1)),
  burn_source    TEXT CHECK (burn_source IN ('fit','rpc')),
  observed_at    INTEGER NOT NULL,
  run_id         TEXT    NOT NULL,
  PRIMARY KEY (as_of, model_version)
) STRICT, WITHOUT ROWID;

-- class: rollup (deterministic per (as_of, model_version)). Per-subnet gate outputs.
CREATE TABLE IF NOT EXISTS gate_subnet_daily (
  as_of                  INTEGER NOT NULL,
  model_version          TEXT    NOT NULL,
  netuid                 INTEGER NOT NULL,
  occupant_key           TEXT    NOT NULL,
  eligible               INTEGER NOT NULL CHECK (eligible IN (0,1)),
  demand_share           REAL,
  burn_adjusted_share    REAL,
  gate                   REAL,
  predicted_share        REAL,
  realised_share         REAL,
  predicted_gated_share  REAL,
  implied_miner_burn     REAL,
  fitted_burn            REAL,
  rpc_miner_burned       REAL,
  observed_at            INTEGER NOT NULL,
  run_id                 TEXT,
  PRIMARY KEY (as_of, model_version, netuid)
) STRICT, WITHOUT ROWID;

-- class: append. Exact block↔time anchors, ONLY from archive-RPC Timestamp.Now reads (never interpolated).
-- Owner: 17 (backfill_rpc, occupancy); read by 13 block_to_unix, 17 occupancy (registered_ts).
CREATE TABLE IF NOT EXISTS block_times (
  block_number  INTEGER PRIMARY KEY,
  ts_ms         INTEGER NOT NULL,           -- Timestamp.Now (ms)
  event_at      INTEGER GENERATED ALWAYS AS (ts_ms / 1000) VIRTUAL,   -- block time, unix s
  endpoint      TEXT    NOT NULL,           -- RPC endpoint used
  observed_at   INTEGER NOT NULL,
  source        TEXT    NOT NULL DEFAULT 'rpc_timestamp',
  run_id        TEXT
) STRICT, WITHOUT ROWID;

-- class: state (resume + audit ledger of the archive-RPC backfill; failed → done upsert). Owner: 17.
CREATE TABLE IF NOT EXISTS backfill_buckets (
  era           TEXT    NOT NULL CHECK (era IN ('gap','ledger','pre_ledger')),
  bucket_ts     INTEGER NOT NULL,           -- hour start (day start with --step 86400)
  status        TEXT    NOT NULL CHECK (status IN ('done','failed','skipped')),
  block_number  INTEGER,
  block_ts      INTEGER,
  n_subnets     INTEGER,
  rows_written  INTEGER,
  price_method  TEXT    CHECK (price_method IN ('swap_sqrt','reserve_ratio')),
  endpoint      TEXT,
  attempts      INTEGER NOT NULL DEFAULT 0,
  error         TEXT,
  observed_at   INTEGER NOT NULL,
  run_id        TEXT    NOT NULL,
  PRIMARY KEY (era, bucket_ts)
) STRICT, WITHOUT ROWID;

-- class: append (change-only log per netuid from archive-RPC buckets). Owner: 17; read by 11 (owner-key history,
-- S3 back-fill), 04 (name history).
CREATE TABLE IF NOT EXISTS rpc_owner_history (
  netuid         INTEGER NOT NULL,
  block_number   INTEGER NOT NULL,
  occupant_key   TEXT    NOT NULL,
  event_at       INTEGER NOT NULL,          -- block time (Timestamp.Now)
  observed_at    INTEGER NOT NULL,
  source         TEXT    NOT NULL DEFAULT 'rpc_archive',
  run_id         TEXT,
  owner_coldkey  TEXT,
  owner_hotkey   TEXT,
  subnet_name    TEXT,
  token_symbol   TEXT,
  change         TEXT    NOT NULL,          -- 'first' | comma list of changed fields
  PRIMARY KEY (netuid, block_number)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_rpc_owner_history_occ ON rpc_owner_history(occupant_key, block_number);

-- class: append + NULL-only enrichment (trigger-enforced, §2.5). Imported old-ledger pool series (7,362,854 rows,
-- 2025-09-22 → 2026-06-19, + Pi rows) and spec 17's archive-RPC samples. Owner: 17. Never pruned. Units: price τ/α,
-- tao_in_pool τ, alpha_in_pool α (never rao). The first nine columns are the old DDL; the rest are provenance.
CREATE TABLE IF NOT EXISTS pool_history (
    netuid       INTEGER NOT NULL,
    block_number INTEGER NOT NULL,
    timestamp    INTEGER NOT NULL,
    price        REAL    NOT NULL,
    tao_in_pool  REAL    NOT NULL,
    alpha_in_pool REAL   NOT NULL,
    moving_price          REAL,
    network_registered_at INTEGER,
    subnet_volume         REAL,
    occupant_key  TEXT,      -- NULL until stamped by 17's occupancy (seed era may stay NULL = unattributable)
    source        TEXT,      -- ledger_pc | ledger_pi | ledger_extra | onfinality | opentensor_archive (NULL: raw old copy)
    event_at      INTEGER,   -- rpc rows: exact block time; ledger rows: NULL (timestamp = capture wall clock)
    observed_at   INTEGER,   -- ledger rows ≥ LEDGER_LIVE_FROM_TS (1777468595): = timestamp (captured live); earlier
                             -- ledger rows: import run start (fetched later, 17 §2.1); rpc rows: read time
    price_method  TEXT,      -- rpc: swap_sqrt | reserve_ratio; ledger: NULL
    enriched_at   INTEGER,   -- set when a NULL observed value (moving_price, subnet_volume, network_registered_at)
                             -- was filled later (PIT: knowable from here); occupant stamping never sets it
    run_id        TEXT,      -- import/backfill run (collector_runs.run_id); NULL on raw old-ledger copies
    PRIMARY KEY (netuid, block_number)
);
CREATE INDEX IF NOT EXISTS idx_pool_history_subnet_ts
    ON pool_history(netuid, timestamp DESC);
-- no occupant_key index: stamping and bar derivation walk (netuid, block_number) / (netuid, timestamp); saves ≈ 250 MB

CREATE TRIGGER IF NOT EXISTS trg_pool_history_update BEFORE UPDATE ON pool_history
WHEN NEW.netuid IS NOT OLD.netuid OR NEW.block_number IS NOT OLD.block_number
  OR NEW.timestamp IS NOT OLD.timestamp OR NEW.price IS NOT OLD.price
  OR NEW.tao_in_pool IS NOT OLD.tao_in_pool OR NEW.alpha_in_pool IS NOT OLD.alpha_in_pool
  OR NEW.source IS NOT OLD.source OR NEW.observed_at IS NOT OLD.observed_at OR NEW.event_at IS NOT OLD.event_at
  OR NEW.price_method IS NOT OLD.price_method
  OR (OLD.moving_price IS NOT NULL AND NEW.moving_price IS NOT OLD.moving_price)
  OR (OLD.subnet_volume IS NOT NULL AND NEW.subnet_volume IS NOT OLD.subnet_volume)
  OR (OLD.network_registered_at IS NOT NULL AND NEW.network_registered_at IS NOT OLD.network_registered_at)
  OR (OLD.occupant_key IS NOT NULL AND NEW.occupant_key IS NOT OLD.occupant_key)
  OR (OLD.enriched_at IS NOT NULL AND NEW.enriched_at IS NOT OLD.enriched_at)
BEGIN SELECT RAISE(ABORT, 'pool_history: only NULL-only enrichment is allowed'); END;
CREATE TRIGGER IF NOT EXISTS trg_pool_history_no_delete BEFORE DELETE ON pool_history
BEGIN SELECT RAISE(ABORT, 'pool_history is append-only'); END;

-- Dormant (whale tracker port, activates with a TaoStats key).
CREATE TABLE IF NOT EXISTS whale_events (
    id            TEXT    PRIMARY KEY,
    coldkey       TEXT    NOT NULL,
    hotkey        TEXT,
    netuid        INTEGER NOT NULL,
    action        TEXT    NOT NULL,
    tao_amount    REAL    NOT NULL,
    alpha_amount  REAL    NOT NULL,
    price_tao     REAL,
    usd           REAL,
    fee_tao       REAL,
    slippage      REAL,
    is_transfer   INTEGER DEFAULT 0,
    size_vs_pool_pct REAL,
    block_number  INTEGER NOT NULL,
    timestamp     INTEGER NOT NULL,
    source        TEXT    DEFAULT 'taostats'
);
CREATE INDEX IF NOT EXISTS idx_whale_events_coldkey_ts ON whale_events(coldkey, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_whale_events_netuid_ts  ON whale_events(netuid, timestamp DESC);

CREATE TABLE IF NOT EXISTS whale_cohort (
    as_of_block   INTEGER NOT NULL,
    coldkey       TEXT    NOT NULL,
    rank          INTEGER,
    balance_total_tao REAL,
    label         TEXT,
    label_source  TEXT,
    attribution   TEXT DEFAULT 'heuristic',
    total_stake_tao REAL,    -- appended (17, whale_cohort.json)
    free_fraction   REAL,
    balance_rank    INTEGER,
    coldkey_swap    TEXT,
    observed_at     INTEGER, -- import time of the snapshot file
    source          TEXT,    -- 'whale_cohort_json' | 'taostats'
    PRIMARY KEY (as_of_block, coldkey)
);

-- class: state. Canonical repos keyed by GitHub numeric id (stable across rename/transfer). Owner: 06.
CREATE TABLE IF NOT EXISTS github_repos (
  repo_id                INTEGER PRIMARY KEY,
  full_name              TEXT NOT NULL,              -- canonical "owner/name" as last verified
  owner                  TEXT NOT NULL,              -- lower-cased login
  name                   TEXT NOT NULL,
  owner_type             TEXT CHECK (owner_type IN ('Organization','User')),
  is_fork                INTEGER NOT NULL DEFAULT 0 CHECK (is_fork IN (0,1)),
  parent_full_name       TEXT,
  archived               INTEGER NOT NULL DEFAULT 0 CHECK (archived IN (0,1)),
  disabled               INTEGER NOT NULL DEFAULT 0 CHECK (disabled IN (0,1)),
  visibility             TEXT,
  pushed_at              INTEGER,
  created_at             INTEGER,
  updated_at             INTEGER,
  default_branch         TEXT,
  stars                  INTEGER,
  forks                  INTEGER,
  open_issues            INTEGER,
  subscribers            INTEGER,
  license                TEXT,                       -- license.spdx_id
  size_kb                INTEGER,
  description            TEXT,
  topics_json            TEXT,
  etag                   TEXT,
  resolution_method      TEXT NOT NULL CHECK (resolution_method IN ('direct','redirect','onchain','org_enum','manual','html')),
  declared_by_sources    TEXT NOT NULL DEFAULT '[]', -- JSON [{occupant_key, source, raw}]
  status                 TEXT NOT NULL DEFAULT 'ok' CHECK (status IN ('ok','gone','blocked')),
  consecutive_404        INTEGER NOT NULL DEFAULT 0,
  gone_since             INTEGER,
  first_seen_at          INTEGER NOT NULL,           -- PIT membership start (look-ahead flag in 13)
  last_verified_at       INTEGER NOT NULL,
  last_ok_at             INTEGER,
  last_commit_seen_at    INTEGER,                    -- commits watermark (max committer date stored)
  force_push_suspected   INTEGER NOT NULL DEFAULT 0 CHECK (force_push_suspected IN (0,1)),
  participation_json     TEXT,
  contributors_json      TEXT,
  hygiene_json           TEXT,
  hygiene_checked_at     INTEGER,
  originality_json       TEXT,
  originality_checked_at INTEGER
) STRICT;
CREATE INDEX IF NOT EXISTS ix_github_repos_owner ON github_repos(owner, is_fork, archived, pushed_at);

-- class: append. Written when a tracked field changed, and at least daily per repo.
CREATE TABLE IF NOT EXISTS github_repo_snapshots (
  repo_id      INTEGER NOT NULL,
  observed_at  INTEGER NOT NULL,
  full_name    TEXT,
  pushed_at    INTEGER,
  stars        INTEGER,
  forks        INTEGER,
  open_issues  INTEGER,
  archived     INTEGER CHECK (archived IN (0,1)),
  size_kb      INTEGER,
  status       TEXT,
  event_at     INTEGER,                              -- = pushed_at
  source       TEXT NOT NULL DEFAULT 'github_api',
  run_id       TEXT,
  PRIMARY KEY (repo_id, observed_at)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS github_repo_aliases (       -- class: state
  repo_id        INTEGER NOT NULL,
  full_name      TEXT    NOT NULL,                   -- lower-cased
  source         TEXT    NOT NULL CHECK (source IN ('api','redirect','html','declared')),
  first_seen_at  INTEGER NOT NULL,
  last_seen_at   INTEGER NOT NULL,
  PRIMARY KEY (repo_id, full_name)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_github_repo_aliases_name ON github_repo_aliases(full_name);

CREATE TABLE IF NOT EXISTS github_orgs (               -- class: state
  owner                   TEXT PRIMARY KEY,          -- lower-cased
  login                   TEXT,
  kind                    TEXT NOT NULL CHECK (kind IN ('org','user','gone')),
  n_public                INTEGER,
  n_non_fork              INTEGER,
  n_archived              INTEGER,
  n_pushed_30d            INTEGER,
  max_pushed_at_non_fork  INTEGER,
  etag                    TEXT,
  first_seen_at           INTEGER NOT NULL,
  last_enumerated_at      INTEGER,
  last_ok_at              INTEGER,
  consecutive_failures    INTEGER NOT NULL DEFAULT 0
) STRICT;

CREATE TABLE IF NOT EXISTS github_declared_refs (      -- class: state; one row per (occupant, identity source)
  occupant_key       TEXT NOT NULL,
  source             TEXT NOT NULL CHECK (source IN ('chain','subnetalpha','tmc','manual','seed_fundamentals')),
  raw_value          TEXT NOT NULL,
  kind               TEXT CHECK (kind IN ('repo','owner','none')),
  owner              TEXT,
  repo               TEXT,
  ref_key            TEXT,
  is_placeholder     INTEGER NOT NULL DEFAULT 0 CHECK (is_placeholder IN (0,1)),
  state              TEXT NOT NULL CHECK (state IN ('new','resolved','gone_pending','gone','blocked','org_empty','placeholder','none')),
  repo_id            INTEGER,
  resolution_method  TEXT,
  first_seen_at      INTEGER NOT NULL,
  last_attempt_at    INTEGER,
  last_http_status   INTEGER,
  consecutive_404    INTEGER NOT NULL DEFAULT 0,
  first_404_at       INTEGER,
  resolved_at        INTEGER,
  observed_at        INTEGER NOT NULL,
  PRIMARY KEY (occupant_key, source)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS github_occupant_repos (     -- class: state; R(o) membership
  occupant_key         TEXT NOT NULL,
  repo_id              INTEGER NOT NULL,
  role                 TEXT NOT NULL CHECK (role IN ('declared','primary','org','declared_gone')),
  declared_source      TEXT,
  declared_by_sources  TEXT NOT NULL DEFAULT '[]',
  in_scope             INTEGER NOT NULL DEFAULT 0 CHECK (in_scope IN (0,1)),
  first_seen_at        INTEGER NOT NULL,
  last_seen_at         INTEGER NOT NULL,
  PRIMARY KEY (occupant_key, repo_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_github_occ_repos_repo ON github_occupant_repos(repo_id);

-- class: append. PIT: event_at = author date. sha is global (duplicates across repos counted once org-wide).
CREATE TABLE IF NOT EXISTS github_commits (
  sha                TEXT PRIMARY KEY,
  repo_id            INTEGER NOT NULL,
  author_key         TEXT NOT NULL,                  -- canonical author identity (github_author_identities)
  author_login       TEXT,
  author_name        TEXT,
  author_email_hash  TEXT,                           -- sha256(lower(email)); raw email never stored
  author_type        TEXT,
  event_at           INTEGER NOT NULL,
  committer_date     INTEGER,
  n_parents          INTEGER NOT NULL,
  message_head       TEXT,                           -- first line, ≤ 200 chars
  message_norm_hash  TEXT,
  verified           INTEGER CHECK (verified IN (0,1)),
  is_bot             INTEGER NOT NULL DEFAULT 0 CHECK (is_bot IN (0,1)),
  is_merge           INTEGER NOT NULL DEFAULT 0 CHECK (is_merge IN (0,1)),
  spam_flags         TEXT NOT NULL DEFAULT '[]',
  stats_total        INTEGER,
  whitespace_only    INTEGER CHECK (whitespace_only IN (0,1)),
  observed_at        INTEGER NOT NULL,
  source             TEXT NOT NULL DEFAULT 'github_api' CHECK (source IN ('github_api','github_atom')),
  run_id             TEXT NOT NULL,
  backfilled         INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1))   -- 17 backfill_github history pull
) STRICT;
CREATE INDEX IF NOT EXISTS ix_github_commits_repo_day ON github_commits(repo_id, event_at);
CREATE INDEX IF NOT EXISTS ix_github_commits_author   ON github_commits(author_key, event_at);

CREATE TABLE IF NOT EXISTS github_author_identities (  -- class: state
  alias_key      TEXT PRIMARY KEY,
  canonical_key  TEXT NOT NULL,
  first_seen_at  INTEGER NOT NULL
) STRICT;

-- class: rollup of github_commits (REPLACE per (repo_id, day_ts)).
CREATE TABLE IF NOT EXISTS github_activity_daily (
  repo_id        INTEGER NOT NULL,
  day_ts         INTEGER NOT NULL,                   -- UTC day of author date
  commits_all    INTEGER NOT NULL,
  commits_human  INTEGER NOT NULL,
  commits_bot    INTEGER NOT NULL,
  commits_merge  INTEGER NOT NULL,
  commits_spam   INTEGER NOT NULL,
  authors_human  INTEGER NOT NULL,
  observed_at    INTEGER NOT NULL,
  run_id         TEXT    NOT NULL,
  backfilled     INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),
  PRIMARY KEY (repo_id, day_ts)
) STRICT, WITHOUT ROWID;

-- class: rollup. Electric-Capital author classes, one row per author per occupant per run-day.
CREATE TABLE IF NOT EXISTS github_authors_daily (
  occupant_key    TEXT    NOT NULL,
  author_key      TEXT    NOT NULL,
  day_ts          INTEGER NOT NULL,
  commits_today   INTEGER NOT NULL,
  cd28            INTEGER NOT NULL,
  cd56            INTEGER NOT NULL,
  cd84_3          INTEGER NOT NULL,
  cd84            INTEGER NOT NULL,
  dev_class       TEXT    NOT NULL CHECK (dev_class IN ('full_time','part_time','one_time')),
  repos_json      TEXT    NOT NULL,
  observed_at     INTEGER NOT NULL,
  run_id          TEXT    NOT NULL,
  backfilled      INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),
  PRIMARY KEY (occupant_key, author_key, day_ts)
) STRICT, WITHOUT ROWID;

-- class: append. Raw repo-side events (not the occupant-level events table).
CREATE TABLE IF NOT EXISTS github_events (
  id           INTEGER PRIMARY KEY,
  repo_id      INTEGER NOT NULL,
  event_kind   TEXT NOT NULL CHECK (event_kind IN ('release','tag','pr_merged','archived','unarchived','renamed',
                 'transferred','gone','restored','default_branch_changed')),
  event_at     INTEGER,
  observed_at  INTEGER NOT NULL,
  dedupe_key   TEXT NOT NULL UNIQUE,
  payload_json TEXT NOT NULL,
  source       TEXT NOT NULL DEFAULT 'github_api',
  backfilled   INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),   -- 1 = written by 17's backfill (02 §J1)
  run_id       TEXT NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_github_events_repo_kind ON github_events(repo_id, event_kind, event_at);

CREATE TABLE IF NOT EXISTS github_http_cache (         -- class: state (cache; retention 30 d)
  url         TEXT PRIMARY KEY,
  etag        TEXT NOT NULL,
  status      INTEGER NOT NULL,
  body        BLOB NOT NULL,
  fetched_at  INTEGER NOT NULL
) STRICT;

-- class: append (content_text NULLed by retention). One row per (occupant, url, kind, UTC day). Owner: 07.
CREATE TABLE IF NOT EXISTS web_checks (
  id                     INTEGER PRIMARY KEY,
  occupant_key           TEXT    NOT NULL,
  netuid                 INTEGER NOT NULL,
  url                    TEXT    NOT NULL,           -- normalised declared URL
  kind                   TEXT    NOT NULL CHECK (kind IN ('website','docs')),
  is_primary             INTEGER NOT NULL DEFAULT 1 CHECK (is_primary IN (0,1)),
  source                 TEXT    NOT NULL,           -- watch_channels.source at check time
  event_at               INTEGER NOT NULL,           -- request start (07's checked_at)
  observed_at            INTEGER NOT NULL,           -- row write time
  check_day_ts           INTEGER NOT NULL,           -- UTC day of event_at
  run_id                 TEXT    NOT NULL,
  attempts               INTEGER NOT NULL DEFAULT 1,
  status_class           TEXT    NOT NULL CHECK (status_class IN ('up_2xx','redirect_ok','client_error','server_error',
                           'timeout','dns_fail','tls_fail','conn_error','too_many_redirects','bot_challenge','parked','hijacked_suspect')),
  http_status            INTEGER,
  final_url              TEXT,
  final_host             TEXT,
  redirect_hops          INTEGER NOT NULL DEFAULT 0,
  redirect_chain         TEXT,                       -- JSON [{url,status,location}]
  cross_domain           INTEGER NOT NULL DEFAULT 0 CHECK (cross_domain IN (0,1)),
  resolved_ip            TEXT,
  latency_ms             INTEGER,
  robots_allowed         INTEGER CHECK (robots_allowed IN (0,1)),
  tls_days_left          INTEGER,
  tls_ok                 INTEGER CHECK (tls_ok IN (0,1)),
  tls_hostname_match     INTEGER CHECK (tls_hostname_match IN (0,1)),
  tls_issuer             TEXT,
  tls_not_after          INTEGER,
  content_type           TEXT,
  extractor              TEXT CHECK (extractor IN ('trafilatura','readability','plain','raw','none')),
  is_spa                 INTEGER CHECK (is_spa IN (0,1)),
  content_hash           TEXT,                       -- sha256 of normalised text
  content_len            INTEGER,
  content_text           TEXT,                       -- only when hash changed (≤ WEB_STORE_TEXT_MAX_CHARS); retention §2.7
  change_ratio           REAL,
  parked                 INTEGER NOT NULL DEFAULT 0 CHECK (parked IN (0,1)),
  parked_signals         TEXT,
  hijack_signals         TEXT,
  docs_candidate_url     TEXT,
  wayback_first_capture  INTEGER,
  wayback_checked_at     INTEGER,
  error                  TEXT,
  UNIQUE (occupant_key, url, kind, check_day_ts)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_web_checks_url_day ON web_checks(url, kind, check_day_ts);
CREATE INDEX IF NOT EXISTS ix_web_checks_occ_day ON web_checks(occupant_key, kind, check_day_ts);

-- class: append (one row per probe attempt). Discord invite metadata (probe='invite'), widget.json samples
-- (probe='widget') and the per-occupant topology census (probe='census'). Owner: 08 (§3.2 DDL adopted).
-- Aggregate counts only: usernames/inviter/widget members[] are never stored (Developer Policy §16).
CREATE TABLE IF NOT EXISTS discord_snapshots (
  id                    INTEGER PRIMARY KEY,
  occupant_key          TEXT,                 -- NULL only for the shared main-guild vanity sample (one per cycle)
  netuid                INTEGER,              -- convenience copy
  probe                 TEXT    NOT NULL CHECK (probe IN ('invite','widget','census')),
  guild_id              TEXT,
  invite_code           TEXT,                 -- case-sensitive; NULL for widget rows and code-less census rows
  observed_at           INTEGER NOT NULL,     -- fetch time (census: classification time)
  event_at              INTEGER,              -- always NULL: counts describe "now" (kept for PIT uniformity)
  cycle_at              INTEGER NOT NULL,     -- scheduled slot start (6-h slot: invite/census; hour: widget)
  source                TEXT    NOT NULL DEFAULT 'discord_api',   -- 'discord_api' (keyless REST) | 'discord_bot'
  http_status           INTEGER,              -- NULL for census
  member_count          INTEGER,              -- approximate_member_count
  presence_count        INTEGER,              -- approximate_presence_count | widget presence_count
  presence_ratio        REAL,                 -- presence/member; NULL if either missing or member_count = 0
  invite_valid          INTEGER CHECK (invite_valid IN (0,1)),   -- 1 valid; 0 dead (404 10006/50270, expired,
                                                                 -- non-guild); NULL collection failure / n.a.
  invite_error_code     INTEGER,              -- Discord JSON error code on 404 (10006 unknown, 50270 expired)
  expires_at            INTEGER,              -- NULL = permanent invite
  landing_channel_id    TEXT,                 -- census main_channel rows: the main-server subnet channel id
  landing_channel_type  INTEGER,              -- 0 text, 2 voice, 5 announcement, 13 stage, 15 forum
  features_json         TEXT CHECK (features_json IS NULL OR json_valid(features_json)),   -- sorted JSON array
  boosts                INTEGER,              -- guild.premium_subscription_count
  verification_level    INTEGER,
  guild_created_at      INTEGER,              -- from the snowflake: ((id >> 22) + 1420070400000) / 1000
  widget_enabled        INTEGER CHECK (widget_enabled IN (0,1)),   -- widget rows: 1 (200), 0 (403/50004), NULL other
  topology_class        TEXT CHECK (topology_class IN ('own_server','main_channel','main_vanity','invalid',
                                                       'handle_only','none','unknown')),
  is_primary            INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0,1)),
  unknown_targets       INTEGER NOT NULL DEFAULT 0,   -- targets without a successful observation in their window
  source_agreement      TEXT CHECK (source_agreement IN ('agree','disagree','one_dead','single_source')),
  declared_by_json      TEXT CHECK (declared_by_json IS NULL OR json_valid(declared_by_json)),
                                              -- JSON array of watch_channels.source values declaring this code
  meta_json             TEXT CHECK (meta_json IS NULL OR json_valid(meta_json)),
                                              -- {guild_name, vanity_url_code, premium_tier, nsfw_level, invite_type,
                                              --  invite_flags, landing_channel_name, profile_visibility,
                                              --  instant_invite_code, channel_count, error}
  run_id                TEXT    NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_dsnap_occ_obs   ON discord_snapshots(occupant_key, observed_at);
CREATE INDEX IF NOT EXISTS ix_dsnap_guild_obs ON discord_snapshots(guild_id, probe, observed_at);
CREATE INDEX IF NOT EXISTS ix_dsnap_code_obs  ON discord_snapshots(invite_code, observed_at);
CREATE INDEX IF NOT EXISTS ix_dsnap_cycle     ON discord_snapshots(cycle_at, probe);

-- class: append. t.me profile/preview page samples, one row per (occupant, handle, UTC day). Owner: 08.
CREATE TABLE IF NOT EXISTS telegram_snapshots (
  id                 INTEGER PRIMARY KEY,
  occupant_key       TEXT    NOT NULL,
  netuid             INTEGER,
  handle             TEXT    NOT NULL,        -- public handle (case preserved) or '+hash'
  observed_at        INTEGER NOT NULL,
  event_at           INTEGER,                 -- always NULL
  cycle_day_ts       INTEGER NOT NULL,        -- UTC day start of the run
  source             TEXT    NOT NULL DEFAULT 'tme_page',
  http_status        INTEGER,                 -- profile page status; NULL on transport failure
  kind               TEXT CHECK (kind IN ('group','channel','private_group','private_channel','bot',
                                          'user_or_unknown','not_found')),
  title              TEXT,                    -- og:title of community kinds only (group/channel); NULL for user/bot accounts
  members            INTEGER,                 -- groups: members; channels: subscribers
  online             INTEGER,                 -- groups only
  online_ratio       REAL,                    -- online / members
  deprecated_marker  INTEGER NOT NULL DEFAULT 0 CHECK (deprecated_marker IN (0,1)),
  preview_available  INTEGER CHECK (preview_available IN (0,1)),   -- public channels: t.me/s rendered >= 1 post
  preview_posts      INTEGER,                 -- post blocks parsed this run
  last_post_at       INTEGER,                 -- newest <time datetime> in the preview
  parse_ok           INTEGER NOT NULL CHECK (parse_ok IN (0,1)),   -- 0 = HTML changed/unparseable (collection failure)
  error              TEXT,
  run_id             TEXT    NOT NULL,
  UNIQUE (occupant_key, handle, cycle_day_ts)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_tgsnap_occ_obs ON telegram_snapshots(occupant_key, observed_at);

-- class: append (annotation: deleted_at; text NULLed by §2.7 nlp_text_retention). Discord messages crossposted
-- (Follow) into the operator's aggregator server, and Telegram channel preview posts (02 §B11: all Discord and
-- Telegram posts land here; 08 never writes social_posts). Owner: 08. 02 §B10: TaoSentinel never reads third-party
-- servers; Discord text exists ONLY for crossposts received in the operator's own server (via_own_server = 1;
-- user/role mention ids redacted by 08; CHECK below). With the platform's *_STORE_TEXT flag true
-- (DISCORD_STORE_TEXT default true) text is kept 90 d; with it false (TELEGRAM_POSTS_STORE_TEXT default false)
-- the text is pending-classification only: written with text_purge_after = observed_at + LLM_PENDING_TEXT_MAX_DAYS
-- × 86400 and NULLed by §2.7 once a post_labels row exists or text_purge_after passes (amended 02 §B10).
CREATE TABLE IF NOT EXISTS announcements (
  msg_id             TEXT PRIMARY KEY,        -- 'discord:{source_message_id}' or 'telegram:{handle_lower}/{post_no}'
                                              -- = spec 10 item_id = post_labels.post_id (02 §B11)
  platform           TEXT    NOT NULL CHECK (platform IN ('discord','telegram')),
  occupant_key       TEXT,                    -- set iff exactly one occupant is mapped (all: social_post_mentions)
  netuid             INTEGER,
  source_guild_id    TEXT,                    -- discord: message.reference.guild_id; telegram: NULL
  source_channel_id  TEXT    NOT NULL,        -- discord: message.reference.channel_id; telegram: handle_lower
  message_id         TEXT    NOT NULL,        -- discord: original message id; telegram: post number
  local_channel_id   TEXT,                    -- aggregator channel the copy arrived in
  local_message_id   TEXT,                    -- id of the crosspost copy (delete events)
  via_own_server     INTEGER NOT NULL DEFAULT 0 CHECK (via_own_server IN (0,1)),   -- spec 10 privacy gate
  mapping            TEXT    NOT NULL CHECK (mapping IN ('channel','guild','guild_ambiguous','main_announce',
                                                         'unmapped','tg_handle')),
  author_is_team     INTEGER CHECK (author_is_team IN (0,1)),   -- 1 team channel, 0 Foundation/other, NULL unknown
  has_attachment     INTEGER NOT NULL DEFAULT 0 CHECK (has_attachment IN (0,1)),
  url_count          INTEGER NOT NULL DEFAULT 0,
  text               TEXT,                    -- <= DISCORD_TEXT_MAX_CHARS; NULLed by §2.7 (90 d, or pending rule below)
  text_purge_after   INTEGER,                 -- set iff written while the *_STORE_TEXT flag was false (pending text)
  text_hash          TEXT    NOT NULL,        -- sha256(app/nlp/textnorm.py(...)) (08, 02 §B11); empty: sha256('msg:' || msg_id)
  text_len           INTEGER,
  event_at           INTEGER NOT NULL,        -- discord: crosspost created_at; telegram: <time datetime>
  observed_at        INTEGER NOT NULL,
  deleted_at         INTEGER,                 -- annotation: MESSAGE_DELETE seen
  meta_json          TEXT CHECK (meta_json IS NULL OR json_valid(meta_json)),   -- {views, embed_count, candidates} (edits ignored)
  run_id             TEXT,                    -- gateway: 'gw:{session_start}'; telegram: job run_id
  -- read aliases (virtual, no storage) for spec 10 and the raw-row source convention
  source             TEXT GENERATED ALWAYS AS
                       (CASE platform WHEN 'discord' THEN 'discord_gateway' ELSE 'tme_preview' END) VIRTUAL,
  guild_id           TEXT GENERATED ALWAYS AS (source_guild_id) VIRTUAL,
  channel_id         TEXT GENERATED ALWAYS AS (source_channel_id) VIRTUAL,
  author_role_flag   TEXT GENERATED ALWAYS AS
                       (CASE author_is_team WHEN 1 THEN 'team' WHEN 0 THEN 'other' END) VIRTUAL,
  url                TEXT GENERATED ALWAYS AS (CASE platform
                       WHEN 'discord' THEN 'https://discord.com/channels/' || source_guild_id || '/'
                                           || source_channel_id || '/' || message_id
                       ELSE 'https://t.me/' || source_channel_id || '/' || message_id END) VIRTUAL,
  CHECK (text IS NULL OR via_own_server = 1 OR platform = 'telegram'),
  CHECK (text_purge_after IS NULL OR text_purge_after >= observed_at)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_ann_occ_event ON announcements(occupant_key, event_at);
CREATE INDEX IF NOT EXISTS ix_ann_purge     ON announcements(text_purge_after) WHERE text IS NOT NULL AND text_purge_after IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_ann_src_event ON announcements(platform, source_channel_id, event_at);
CREATE INDEX IF NOT EXISTS ix_ann_hash      ON announcements(text_hash, event_at);
CREATE INDEX IF NOT EXISTS ix_ann_obs       ON announcements(observed_at);
CREATE INDEX IF NOT EXISTS ix_ann_local     ON announcements(local_message_id);

-- class: state (engagement refreshed in place — accepted PIT deviation, 09 §11.4, flagged look-ahead by 13, 02 §J2;
-- text NULLed by retention after 90 d, then hash only, 02 §B10). One row per X post. Owner: 09. Discord and Telegram
-- posts live in announcements (02 §B11).
CREATE TABLE IF NOT EXISTS social_posts (
  platform                TEXT NOT NULL DEFAULT 'x' CHECK (platform IN ('x')),
  post_id                 TEXT NOT NULL,            -- native id (X snowflake)
  item_id                 TEXT GENERATED ALWAYS AS (platform || ':' || post_id) VIRTUAL,   -- 10: 'x:1969812345'
  author_id               TEXT NOT NULL,
  author_handle           TEXT,
  occupant_key            TEXT,                     -- source-bound occupant (team account); NULL for KOL/mentions
  is_team_account         INTEGER NOT NULL DEFAULT 0 CHECK (is_team_account IN (0,1)),
  is_kol                  INTEGER NOT NULL DEFAULT 0 CHECK (is_kol IN (0,1)),
  kol_handle              TEXT,                     -- kol_accounts.handle when is_kol=1
  author_kind             TEXT GENERATED ALWAYS AS
                          (CASE WHEN is_team_account = 1 THEN 'team' WHEN is_kol = 1 THEN 'kol' ELSE 'other' END) VIRTUAL,
  text                    TEXT,                     -- NULL after LLM_TEXT_RETENTION_DAYS (90 d, §2.7)
  text_hash               TEXT NOT NULL,            -- sha256(app/nlp/textnorm.py(text)) (02 §B11; kept forever)
  lang                    TEXT,
  url                     TEXT,
  like_count              INTEGER,
  repost_count            INTEGER,
  reply_count             INTEGER,
  quote_count             INTEGER,
  view_count              INTEGER,
  engagement_observed_at  INTEGER,
  has_url                 INTEGER CHECK (has_url IN (0,1)),
  has_media               INTEGER CHECK (has_media IN (0,1)),
  in_reply_to             TEXT,
  is_repost               INTEGER NOT NULL DEFAULT 0 CHECK (is_repost IN (0,1)),
  quoted_post_id          TEXT,
  author_credibility      REAL,
  credibility_flags       TEXT,                     -- JSON array
  bot_filtered            INTEGER NOT NULL DEFAULT 0 CHECK (bot_filtered IN (0,1)),   -- 10's spam_flag
  event_at                INTEGER NOT NULL,
  observed_at             INTEGER NOT NULL,
  source                  TEXT NOT NULL CHECK (source IN ('twitterapi_io','official_v2','subnetalpha_ctf')),
  run_id                  TEXT,
  raw_json_hash           TEXT,
  PRIMARY KEY (platform, post_id)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_social_posts_occ     ON social_posts(occupant_key, event_at);
CREATE INDEX IF NOT EXISTS ix_social_posts_author  ON social_posts(author_id, event_at);
CREATE INDEX IF NOT EXISTS ix_social_posts_hash    ON social_posts(text_hash, event_at);
CREATE INDEX IF NOT EXISTS ix_social_posts_obs     ON social_posts(observed_at);
CREATE INDEX IF NOT EXISTS ix_social_posts_item    ON social_posts(item_id);

-- class: append. Post ↔ occupant attribution (team/query/rule/entity). Written by 09 (X) and 08 (Discord/Telegram,
-- post_id = native id so platform || ':' || post_id = announcements.msg_id), plus 10's entity rows (02 §B11).
CREATE TABLE IF NOT EXISTS social_post_mentions (
  platform            TEXT NOT NULL,
  post_id             TEXT NOT NULL,
  occupant_key        TEXT NOT NULL,
  match_kind          TEXT NOT NULL CHECK (match_kind IN ('team','query','rule','entity')),
  classifier_version  TEXT,                         -- entity rows only
  event_at            INTEGER,                      -- the post's / announcement's event_at (copied; NULL if unknown)
  observed_at         INTEGER NOT NULL,
  source              TEXT GENERATED ALWAYS AS ('mention_' || match_kind) VIRTUAL,   -- raw-row convention alias
  run_id              TEXT,
  PRIMARY KEY (platform, post_id, occupant_key, match_kind)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_spm_occ ON social_post_mentions(occupant_key, observed_at);

-- class: append. Profile refreshes (followers etc.). Owner: 09.
CREATE TABLE IF NOT EXISTS x_account_snapshots (
  author_id      TEXT    NOT NULL,
  observed_at    INTEGER NOT NULL,
  handle         TEXT,
  followers      INTEGER,
  following      INTEGER,
  post_count     INTEGER,
  account_created_at INTEGER,
  verified       INTEGER CHECK (verified IN (0,1)),
  provider       TEXT,
  source         TEXT NOT NULL DEFAULT 'x_profile',
  event_at       INTEGER,
  run_id         TEXT,
  raw_json_hash  TEXT,
  PRIMARY KEY (author_id, observed_at)
) STRICT, WITHOUT ROWID;

-- class: append (summary NULLed by retention). RSS/Google News/Substack/Medium items. Owner: 09 Appendix A.
CREATE TABLE IF NOT EXISTS news_items (
  url_hash            TEXT PRIMARY KEY,             -- sha256(canonical_url) or sha256("t:"+title_norm+"|p:"+domain); 10's item_id
  canonical_url       TEXT,
  url                 TEXT NOT NULL,
  title               TEXT NOT NULL,
  summary             TEXT,                         -- HTML-stripped, ≤ 2,000 chars
  publisher           TEXT,
  publisher_domain    TEXT,
  feed_url            TEXT NOT NULL,
  feed_kind           TEXT,
  occupant_keys_json  TEXT NOT NULL DEFAULT '[]',   -- [{"occupant_key":…, "kind":"rule"|"query"}]
  lang                TEXT,
  text_hash           TEXT NOT NULL,                -- sha256(title_norm + "\n" + summary_norm)
  event_at            INTEGER NOT NULL,             -- published (or observed when the feed lacks a date)
  event_at_source     TEXT NOT NULL CHECK (event_at_source IN ('feed','observed')),
  observed_at         INTEGER NOT NULL,
  source              TEXT NOT NULL CHECK (source IN ('google_news','rss','substack','medium')),
  run_id              TEXT,
  raw_json_hash       TEXT
) STRICT;
CREATE INDEX IF NOT EXISTS ix_news_items_event ON news_items(event_at);
CREATE INDEX IF NOT EXISTS ix_news_items_obs   ON news_items(observed_at);

-- class: append (description NULLed by retention). YouTube channel/playlist RSS items. Owner: 09 Appendix A.
CREATE TABLE IF NOT EXISTS video_items (
  video_id            TEXT PRIMARY KEY,             -- 10's item_id
  channel_id          TEXT NOT NULL,
  channel_title       TEXT,
  occupant_key        TEXT,                         -- channel's occupant
  occupant_keys_json  TEXT NOT NULL DEFAULT '[]',   -- rule matches (playlist items)
  playlist_id         TEXT,
  title               TEXT NOT NULL,
  description         TEXT,
  url                 TEXT GENERATED ALWAYS AS ('https://www.youtube.com/watch?v=' || video_id) VIRTUAL,
  views               INTEGER,                      -- first observation only
  event_at            INTEGER NOT NULL,             -- published
  observed_at         INTEGER NOT NULL,
  source              TEXT NOT NULL CHECK (source IN ('channel_rss','playlist_rss')),
  run_id              TEXT,
  raw_json_hash       TEXT
) STRICT;
CREATE INDEX IF NOT EXISTS ix_video_items_occ ON video_items(occupant_key, event_at);

-- class: append (trigger-enforced). One label per (item, classifier_version). Owner: 10.
-- post_id (02 §B11) = the item id itself for posts: 'x:{id}' (= social_posts.item_id), 'discord:{source_message_id}'
--   and 'telegram:{handle_lower}/{n}' (= announcements.msg_id); '{item_kind}:{item_id}' for the other kinds:
--   'news_item:{news_items.url_hash}', 'video_item:{video_items.video_id}'.
CREATE TABLE IF NOT EXISTS post_labels (
  post_id             TEXT NOT NULL,
  item_kind           TEXT NOT NULL CHECK (item_kind IN ('social_post','announcement','news_item','video_item')),
  item_id             TEXT NOT NULL,
  classifier_version  TEXT NOT NULL,                -- '{model}:{rubric_sha8}:{schema_v}' | 'rules:v1'
  classifier_tier     TEXT NOT NULL CHECK (classifier_tier IN ('bulk','escalation','rules')),
  polarity            TEXT NOT NULL CHECK (polarity IN ('bullish','bearish','neutral')),
  intensity           INTEGER NOT NULL CHECK (intensity BETWEEN 1 AND 5),
  event_type          TEXT NOT NULL,                -- master §7.4 taxonomy (catalog §3.9)
  direction           INTEGER NOT NULL CHECK (direction IN (-1,0,1)),
  materiality         INTEGER NOT NULL CHECK (materiality BETWEEN 1 AND 3),
  entities_json       TEXT NOT NULL,
  is_spam             INTEGER NOT NULL CHECK (is_spam IN (0,1)),
  is_promo            INTEGER NOT NULL CHECK (is_promo IN (0,1)),
  summary             TEXT,                         -- model summary (not source text)
  confidence          REAL NOT NULL,
  event_at            INTEGER NOT NULL,             -- copied from the item
  observed_at         INTEGER NOT NULL,             -- label write time (PIT key)
  source              TEXT NOT NULL DEFAULT 'llm' CHECK (source IN ('llm','rules')),
  run_id              TEXT NOT NULL,
  batch_key           TEXT,
  model_resolved      TEXT,
  input_tokens        INTEGER,
  output_tokens       INTEGER,
  cost_usd            REAL,
  raw_json            TEXT,
  PRIMARY KEY (post_id, classifier_version)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_post_labels_event   ON post_labels(event_at);
CREATE INDEX IF NOT EXISTS ix_post_labels_ver_obs ON post_labels(classifier_version, observed_at);
CREATE INDEX IF NOT EXISTS ix_post_labels_item    ON post_labels(item_kind, item_id);

CREATE TABLE IF NOT EXISTS post_label_entities (    -- class: append; child rows of post_labels: event_at/run_id/source
                                                    -- are the parent label's (join on post_id, classifier_version)
  post_id             TEXT NOT NULL,
  classifier_version  TEXT NOT NULL,
  occupant_key        TEXT NOT NULL,
  netuid              INTEGER NOT NULL,
  confidence          REAL NOT NULL,
  via                 TEXT NOT NULL CHECK (via IN ('model','source','regex')),
  observed_at         INTEGER NOT NULL,
  PRIMARY KEY (post_id, classifier_version, occupant_key)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_ple_occ ON post_label_entities(occupant_key, classifier_version);

CREATE TABLE IF NOT EXISTS llm_batches (            -- class: state. Batch API ledger (survives restarts). Owner: 10.
  batch_key           TEXT PRIMARY KEY,
  provider_batch_id   TEXT,
  model               TEXT NOT NULL,
  classifier_version  TEXT NOT NULL,
  alias_sha256        TEXT,
  status              TEXT NOT NULL CHECK (status IN ('building','submitted','ended','applied','failed','expired','canceled')),
  n_requests INTEGER, n_items INTEGER, n_succeeded INTEGER, n_errored INTEGER, n_expired INTEGER, n_parse_errors INTEGER,
  est_input_tokens INTEGER, est_output_tokens INTEGER, est_cost_usd REAL,
  input_tokens INTEGER, output_tokens INTEGER, cache_read_tokens INTEGER, cost_usd REAL,
  created_at          INTEGER NOT NULL,
  submitted_at        INTEGER,
  ended_at            INTEGER,
  applied_at          INTEGER,
  last_polled_at      INTEGER,
  error               TEXT
) STRICT;

CREATE TABLE IF NOT EXISTS llm_batch_items (        -- class: state (retention 90 d after batch end)
  batch_key  TEXT NOT NULL,
  custom_id  TEXT NOT NULL,
  post_id    TEXT NOT NULL,
  attempt    INTEGER NOT NULL DEFAULT 1,
  status     TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','labelled','errored','requeued','fallback')),
  PRIMARY KEY (batch_key, post_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_lbi_custom ON llm_batch_items(batch_key, custom_id);

-- class: append. One row per occupant per owner_collect run: owner keys, cut, locks, position and the Rufus
-- conviction-table fields (master §7.3 P4; research 02-gapfill-owner-extraction-mechanics §2b/§2d). Owner: 11.
CREATE TABLE IF NOT EXISTS owner_snapshots (
  occupant_key              TEXT    NOT NULL,
  netuid                    INTEGER NOT NULL,
  observed_at               INTEGER NOT NULL,
  event_at                  INTEGER,         -- block time of block_number when known
  source                    TEXT    NOT NULL DEFAULT 'owner_collect',   -- composite of the C1..C8 fetches below
  run_id                    TEXT    NOT NULL,
  block_number              INTEGER,
  owner_coldkey             TEXT,
  owner_hotkey              TEXT,
  keys_source               TEXT CHECK (keys_source IN ('tmc_list','tmc_detail','chain_hourly','rufus')),
  owner_cut_per_block       REAL,            -- TMC subnet page / table (alpha per block)
  owner_cut_cumulative      REAL,
  pending_owner_cut         REAL,
  owner_cut_enabled         INTEGER CHECK (owner_cut_enabled IN (0,1)),
  auto_lock_enabled_tmc     INTEGER CHECK (auto_lock_enabled_tmc IN (0,1)),
  tmc_owner_personal_stake  REAL,
  tmc_owner_locked          REAL,            -- owner_personal_locked_alpha
  tmc_owner_perpetual       REAL,            -- owner_personal_perpetual_alpha
  tmc_owner_decaying        REAL,            -- owner_personal_decaying_alpha
  owner_total_alpha         REAL,            -- C2 /accounts/coldkey-stakes balance_alpha (owner position)
  owner_locked_alpha        REAL,            -- C2 locked_alpha
  owner_available_alpha     REAL,            -- total − locked
  owner_hotkeys_json        TEXT CHECK (owner_hotkeys_json IS NULL OR json_valid(owner_hotkeys_json)),
  owner_hk_alpha_stake      REAL,            -- C3 latest hour (hotkey stake incl. nominators: a trace, not the position)
  owner_hk_conviction       REAL,
  rufus_baked_at            INTEGER,         -- Rufus table.json baked_at_utc
  rufus_stale               INTEGER CHECK (rufus_stale IN (0,1)),
  rufus_auto_lock           INTEGER CHECK (rufus_auto_lock IN (0,1)),
  rufus_owner_perp          REAL,
  rufus_owner_decay         REAL,
  rufus_owner_pct           REAL,            -- owner bucket pct of eligible alpha
  rufus_to_owner_perp       REAL,
  rufus_to_owner_lockers    INTEGER,
  rufus_challenger_max_pct  REAL,
  rufus_days_to_king        REAL,
  total_subnet_stake        REAL,            -- copied from the chain_hourly row used
  tao_in                    REAL,
  price                     REAL,
  initial_alpha_est         REAL,            -- TaoStats only
  identity_name             TEXT,            -- owner coldkey identity name (04) at snapshot time
  fetch_status_json         TEXT    NOT NULL CHECK (json_valid(fetch_status_json)),
                                             -- {"c1":"ok|skip|fail:<err>", …, "c8":…}; per-input absence vs failure
  raw_json                  TEXT,            -- owner subset of the C1 snapshot + the C2 rows for this netuid
  PRIMARY KEY (occupant_key, observed_at)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_owner_snapshots_ck ON owner_snapshots(owner_coldkey, observed_at);

-- class: append. C3 hourly trace of the owner HOTKEY from /analytics/neuron/{n}/{hk}/?span=1W (INSERT OR IGNORE).
-- Owner: 11. ≈ 3 k rows/day; kept forever in v1 (rollup: §11).
CREATE TABLE IF NOT EXISTS owner_position_hourly (
  occupant_key                TEXT    NOT NULL,
  hotkey                      TEXT    NOT NULL,
  hour_ts                     INTEGER NOT NULL,
  block_number                INTEGER,
  alpha_stake                 REAL,
  conviction                  REAL,
  conviction_locked_alpha     REAL,
  conviction_perpetual_alpha  REAL,
  conviction_decaying_alpha   REAL,
  event_at                    INTEGER NOT NULL,   -- analytics point ts
  observed_at                 INTEGER NOT NULL,
  source                      TEXT    NOT NULL DEFAULT 'tmc_neuron_1w',
  run_id                      TEXT,
  PRIMARY KEY (occupant_key, hotkey, hour_ts)
) STRICT, WITHOUT ROWID;

-- class: append (annotations: superseded_by/superseded_at, whitelisted/whitelist_id display cache). Owner-coldkey
-- stake movements (S1 sells, S3 lock toggles), one row per (source event, occupant). PIT: a row is superseded at t
-- only if superseded_at <= t; metrics call is_whitelisted(., t), never the cache. Owner: 11.
CREATE TABLE IF NOT EXISTS owner_stake_events (
  event_uid          TEXT    NOT NULL,       -- source row id; else sha1(extrinsic_id|function|from_netuid|to_netuid|alpha)
  occupant_key       TEXT    NOT NULL,
  netuid             INTEGER NOT NULL,
  owner_coldkey      TEXT    NOT NULL,
  signer             TEXT,
  hotkey             TEXT,
  dest_coldkey       TEXT,
  function           TEXT,                   -- extrinsic call name as delivered (remove_stake_full_limit, …)
  kind               TEXT    NOT NULL CHECK (kind IN ('sell','sell_full','swap_out','transfer_out','move_internal','buy',
                               'swap_in','transfer_in','lock','lock_move','perpetual_on','perpetual_off','other')),
  counts_as_outflow  INTEGER NOT NULL CHECK (counts_as_outflow IN (0,1)),
  from_netuid        INTEGER,
  to_netuid          INTEGER,
  alpha              REAL,                   -- alpha amount (TAO units, never rao)
  tao                REAL,                   -- TAO received/paid
  price              REAL,
  slippage           REAL,
  is_proxy           INTEGER CHECK (is_proxy IN (0,1)),
  success            INTEGER NOT NULL DEFAULT 1 CHECK (success IN (0,1)),
  block_number       INTEGER,
  extrinsic_id       TEXT,                   -- 'block-idx'
  event_at           INTEGER NOT NULL,       -- block time
  observed_at        INTEGER NOT NULL,
  source             TEXT    NOT NULL CHECK (source IN ('tmc_staking_activity','tmc_stake_removed','tmc_stream',
                               'taostats_delegation','taostats_extrinsic','rpc_events')),
  run_id             TEXT    NOT NULL,
  superseded_by      TEXT,                   -- event_uid of the cross-source duplicate that wins
  superseded_at      INTEGER,
  whitelisted        INTEGER NOT NULL DEFAULT 0 CHECK (whitelisted IN (0,1)),   -- display cache only
  whitelist_id       INTEGER,
  raw_json           TEXT,
  PRIMARY KEY (event_uid, occupant_key)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_ose_occ_time ON owner_stake_events(occupant_key, event_at);
CREATE INDEX IF NOT EXISTS ix_ose_ck_block ON owner_stake_events(owner_coldkey, block_number);
CREATE INDEX IF NOT EXISTS ix_ose_extrinsic ON owner_stake_events(extrinsic_id) WHERE extrinsic_id IS NOT NULL;

-- class: append (annotations: whitelisted/whitelist_id, review_id, event_id, paired_change_id, identity_event_id).
-- Control/identity/lock changes of the owner (S3, owner_control_change and owner_extraction inputs). Owner: 11.
CREATE TABLE IF NOT EXISTS owner_changes (
  id                    INTEGER PRIMARY KEY,
  occupant_key          TEXT    NOT NULL,
  netuid                INTEGER NOT NULL,
  change_type           TEXT    NOT NULL CHECK (change_type IN ('coldkey_swap','owner_transfer','subnet_owner_changed',
                          'coldkey_change_unclassified','coldkey_swap_announced','hotkey_repoint',
                          'hotkey_repoint_identity_churn','perpetual_off','decaying_lock_added','lock_reduced',
                          'auto_lock_disabled','auto_lock_enabled','owner_cut_disabled','owner_cut_enabled',
                          'extraction_detected','key_compromise_override')),
  old_value             TEXT,
  new_value             TEXT,
  detected_via          TEXT    NOT NULL,    -- tmc_snapshot | chain_hourly | rufus | staking_activity | taostats |
                                             -- subnetradar | rpc_owner_history | features | operator
  source                TEXT    GENERATED ALWAYS AS (detected_via) VIRTUAL,   -- raw-row convention alias
  classification_basis  TEXT,                -- taostats_is_coldkey_swap | rufus_challenger | subnetradar |
                                             -- old_ck_retains_stake | old_ck_emptied | unclassified
  s3_base_penalty       REAL    NOT NULL DEFAULT 0,   -- 0.6 | 0.4 | 1.0 (hard, perpetual_off) | 0
  paired_change_id      INTEGER,
  identity_event_id     INTEGER,             -- events.id of the paired identity_changed row
  block_number          INTEGER,
  extrinsic_id          TEXT,                -- 'block-idx' when a signed extrinsic is known (D3, D9)
  event_at              INTEGER NOT NULL,
  observed_at           INTEGER NOT NULL,
  review_id             INTEGER,             -- review_queue.id
  event_id              INTEGER,             -- events.id emitted for this change
  evidence_json         TEXT    NOT NULL DEFAULT '{}' CHECK (json_valid(evidence_json)),
  whitelisted           INTEGER NOT NULL DEFAULT 0 CHECK (whitelisted IN (0,1)),   -- display cache only
  whitelist_id          INTEGER,
  dedupe_key            TEXT    NOT NULL UNIQUE,   -- f"{occupant_key}|{change_type}|{new_value or ''}|{event_at // 86400}"
  run_id                TEXT    NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_owner_changes_occ ON owner_changes(occupant_key, event_at);

-- The ONLY input to scoring (master §6) and the feature store of spec 12 (02 §B3–§B5; §2.5/§2.6 contract). One
-- table, no metric_norm table, no is_provisional column (a row is provisional iff as_of % 86400 <> 0).
-- stage='raw': written by producers (05–12) through put_metric_values, keyed by the scoring slot the observation
--   serves (00:00Z official, 06/12/18:00Z provisional; hourly producers the hour bucket); first success wins, only a
--   failed/none row may be upgraded by a later successful observation. value = the producer's value (a raw
--   measurement, or a final [0,1] score for 11's pre-normalised P4 metrics, 02 §C1); value_json = inputs/evidence.
-- stage='norm': written only by spec 12 at scoring time, insert-once per (slot, scoring spec); spec_version = the
--   scoring spec, value = m in [0,1] or NULL, observed_at = the run's computed_at; the PIT record 13 and 14 read.
-- Key (02 §B3) = (occupant_key, metric_key, as_of, spec_version, stage); the PK order also serves §2.6's lookup.
CREATE TABLE IF NOT EXISTS metric_values (
  occupant_key  TEXT    NOT NULL,
  metric_key    TEXT    NOT NULL,          -- catalog §3.8 (validated by put_metric_values)
  as_of         INTEGER NOT NULL,          -- raw: producer slot key; norm: scoring slot. Official = 00:00Z (as_of % 86400 = 0)
  spec_version  TEXT    NOT NULL,          -- raw: metric-definition version; norm: scoring spec version ('ts-1.0')
  stage         TEXT    NOT NULL DEFAULT 'raw' CHECK (stage IN ('raw','norm')),
  value         REAL,                      -- raw: producer value; norm: m in [0,1]; NULL = None
  value_json    TEXT CHECK (value_json IS NULL OR json_valid(value_json)),
                                           -- raw: {"status","reason",…inputs, evidence_url?}; norm: {"raw","status",
                                           --  "reason","raw_as_of","raw_observed_at","age_s","src","fallback","cohort_n"}
  status        TEXT GENERATED ALWAYS AS (COALESCE(json_extract(value_json, '$.status'), 'none')) VIRTUAL
                CHECK (status IN ('ok','absent','failed','not_applicable','none')),
                                           -- never written by producers; 'none' (key missing; on norm rows also "no
                                           -- raw row") is read as 'failed' (02 §B3)
  observed_at   INTEGER NOT NULL,          -- when this value became knowable (PIT key)
  source        TEXT    NOT NULL CHECK (source IN ('registry','chain','github','web','community','x','rss','llm',
                                                   'integrity','scoring','quant','api','runtime','storage','cost','ops')),
                                           -- exactly the 02 §B9 producer vocabulary (no 'nlp', no backfill_* source)
  run_id        TEXT    NOT NULL,
  backfilled    INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),   -- computed after the fact (12 catch-up, 17)
  CHECK (stage = 'norm' OR status <> 'ok' OR value IS NOT NULL),          -- a NULL is never an 'ok' measurement
  CHECK (stage = 'norm' OR status NOT IN ('failed','not_applicable','none') OR value IS NULL),
  CHECK (stage = 'raw' OR value IS NULL OR value BETWEEN 0 AND 1),
  PRIMARY KEY (occupant_key, metric_key, as_of, spec_version, stage)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_metric_values_xsec ON metric_values(metric_key, stage, spec_version, as_of);
CREATE INDEX IF NOT EXISTS ix_metric_values_slot ON metric_values(stage, spec_version, as_of);
CREATE INDEX IF NOT EXISTS ix_metric_values_obs  ON metric_values(observed_at);

-- Never UPDATEd or DELETEd (triggers). One row per occupant per slot per spec_version (02 §B6 key; run_kind is a
-- function of the slot hour, so spec 12's UNIQUE(..., run_kind) is implied). No observed_at column: computed_at
-- (run instant) with input_cutoff is the PIT key for backtests. Owner: 12 (§2.7-§2.10).
CREATE TABLE IF NOT EXISTS scores (
  id                  INTEGER PRIMARY KEY,
  occupant_key        TEXT    NOT NULL,
  netuid              INTEGER NOT NULL,
  as_of               INTEGER NOT NULL,      -- slot instant: 00:00Z official, 06/12/18:00Z provisional (run at hh:15Z)
  spec_version        TEXT    NOT NULL,
  run_kind            TEXT    NOT NULL CHECK (run_kind IN ('official','provisional')),
  run_id              TEXT    NOT NULL,      -- score_runs.run_id
  status              TEXT    NOT NULL CHECK (status IN ('active','pre_launch')),
  quality             REAL CHECK (quality IS NULL OR quality BETWEEN 0 AND 100),          -- Health (P1-P5)
  velocity            REAL CHECK (velocity IS NULL OR velocity BETWEEN 0 AND 100),
  composite_raw       REAL CHECK (composite_raw IS NULL OR composite_raw BETWEEN 0 AND 100),
  flag_multiplier     REAL    NOT NULL DEFAULT 1.0 CHECK (flag_multiplier BETWEEN 0.25 AND 1.0),   -- floor 0.25
  composite           REAL CHECK (composite IS NULL OR composite BETWEEN 0 AND 100),      -- composite_raw x flag_multiplier
  composite_smoothed  REAL CHECK (composite_smoothed IS NULL OR composite_smoothed BETWEEN 0 AND 100),  -- EMA span 7
  pillar_scores       TEXT    NOT NULL CHECK (json_valid(pillar_scores)),    -- {"P1":0.857835,...,"P6":null}, [0,1]|null
  pillar_coverage     TEXT    NOT NULL CHECK (json_valid(pillar_coverage)),  -- {"P1":0.95,...,"P6":1.0}
  confidence          REAL    NOT NULL CHECK (confidence BETWEEN 0 AND 1),
  insufficient_data   INTEGER NOT NULL DEFAULT 0 CHECK (insufficient_data IN (0,1)),  -- confidence < 0.4
  tier                TEXT CHECK (tier IN ('A','B','C','D','F')),           -- NULL for pre_launch
  prev_tier           TEXT CHECK (prev_tier IN ('A','B','C','D','F')),
  red_flags           TEXT    NOT NULL DEFAULT '[]' CHECK (json_valid(red_flags)),
                                             -- sorted JSON array of active flag NAMES, e.g. ["dereg_imminent","repo_gone"];
                                             -- multiplier/since/state/origin/detail live only in explain.flags (02 §B6)
  regime              TEXT,                  -- TRENDING|DISPERSED|CHOPPY|DEAD|UNKNOWN (12 regime)
  v_w                 REAL,                  -- velocity weight used
  explain             TEXT    NOT NULL DEFAULT '{}' CHECK (json_valid(explain)),   -- master §7.7, spec 12 §2.10
  computed_at         INTEGER NOT NULL,      -- run start T0 (13's computed_at); PIT key for score reads
  input_cutoff        INTEGER NOT NULL,      -- latest observed_at admitted (strict: T0; reconstructed: slot + 15 min)
  backfilled          INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),   -- reconstructed-mode (catch-up) row
  shadow              INTEGER NOT NULL DEFAULT 0 CHECK (shadow IN (0,1)),       -- SCORING_SHADOW_SPECS run
  UNIQUE (occupant_key, as_of, spec_version)
) STRICT;
CREATE INDEX IF NOT EXISTS ix_scores_latest ON scores(spec_version, occupant_key, as_of DESC);
CREATE INDEX IF NOT EXISTS ix_scores_asof   ON scores(spec_version, as_of);
CREATE INDEX IF NOT EXISTS ix_scores_occ    ON scores(occupant_key, spec_version, run_kind, as_of);   -- EMA/tier prev

-- Occupant-scoped and system events (master §6). Catalog §3.9. Alert dispatcher (14) sets alerted_at.
CREATE TABLE IF NOT EXISTS events (
  id            INTEGER PRIMARY KEY,
  occupant_key  TEXT,                        -- NULL = system event
  netuid        INTEGER,
  event_type    TEXT    NOT NULL,
  severity      TEXT    NOT NULL CHECK (severity IN ('info','warning','critical')),
  event_at      INTEGER,                     -- when it happened (NULL unknown → readers use observed_at)
  observed_at   INTEGER NOT NULL,            -- when we detected it
  source        TEXT    NOT NULL CHECK (source IN ('registry','chain','github','web','community','x','rss','llm',
                                                   'integrity','scoring','quant','api','runtime','storage','cost','ops')),
                                             -- emitter, exactly the 02 §B9 vocabulary; backfilled history keeps the
                                             -- producer's source and sets payload.backfilled = true (02 §J1)
  payload       TEXT    NOT NULL DEFAULT '{}' CHECK (json_valid(payload)),
  dedupe_key    TEXT,                        -- per catalog; NULL = never deduplicated
  run_id        TEXT,
  alerted_at    INTEGER,                     -- NULL = not (yet) alerted / not alertable
  CHECK (occupant_key IS NOT NULL OR netuid IS NULL)
) STRICT;
CREATE UNIQUE INDEX IF NOT EXISTS ux_events_dedupe   ON events(event_type, dedupe_key) WHERE dedupe_key IS NOT NULL;
CREATE INDEX        IF NOT EXISTS ix_events_observed ON events(observed_at DESC, id DESC);
CREATE INDEX        IF NOT EXISTS ix_events_occ      ON events(occupant_key, event_type, event_at);
CREATE INDEX        IF NOT EXISTS ix_events_type     ON events(event_type, observed_at);
CREATE INDEX        IF NOT EXISTS ix_events_unalerted ON events(id) WHERE alerted_at IS NULL;   -- 14 alert_dispatch queue (02 §D3)

-- Network regime per official slot (master §7.6, 02 §B8); written by 12 (live, INSERT OR IGNORE) and 17 (backfill);
-- read by 13 for tagging (day_ts). Live and backfilled rows use the same raw rule: no debounce; a thin universe
-- yields UNKNOWN with v_w 0.15. No observed_at column (computed_at, 02 §B6).
CREATE TABLE IF NOT EXISTS regime_daily (
  day_ts                INTEGER NOT NULL,    -- official slot as_of (00:00Z)
  spec_version          TEXT    NOT NULL,
  label                 TEXT    NOT NULL CHECK (label IN ('TRENDING','DISPERSED','CHOPPY','DEAD','UNKNOWN')),
  raw_regime            TEXT,
  vol_24h               REAL,
  directional_strength  REAL,
  dispersion            REAL,
  n_subnets             INTEGER,
  thin_universe         INTEGER NOT NULL DEFAULT 0 CHECK (thin_universe IN (0,1)),
  v_w                   REAL    NOT NULL,    -- 0.25 TRENDING/DISPERSED, 0.10 DEAD, else 0.15 (frozen per spec_version)
  tao_usd_ret_24h       REAL,                -- diagnostic only in ts-1.0
  sum_prices_ret_24h    REAL,
  inputs_json           TEXT CHECK (inputs_json IS NULL OR json_valid(inputs_json)),   -- 12: {raw_regime, vol_24h, …}
  source                TEXT    NOT NULL DEFAULT 'live' CHECK (source IN ('live','backfill')),
                                             -- row provenance (12 scoring run | 17 script), not the §B9 vocabulary
  computed_at           INTEGER NOT NULL,
  backfilled            INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),   -- 17 rows and 12 catch-up rows
  run_id                TEXT,
  PRIMARY KEY (day_ts, spec_version)
) STRICT, WITHOUT ROWID;

-- Rank churn monitor (master §7.7), after each official primary run. Owner: 12 (§2.12).
CREATE TABLE IF NOT EXISTS rank_stability_daily (
  as_of              INTEGER NOT NULL,       -- official slot
  spec_version       TEXT    NOT NULL,
  series             TEXT    NOT NULL CHECK (series IN ('health','composite_smoothed')),
  lag_days           INTEGER NOT NULL CHECK (lag_days IN (1,7)),
  n                  INTEGER NOT NULL,
  rank_autocorr      REAL,                   -- NULL when n < 10
  quintile_turnover  REAL,
  top20_turnover     REAL,
  computed_at        INTEGER NOT NULL,
  PRIMARY KEY (as_of, spec_version, series, lag_days)
) STRICT, WITHOUT ROWID;

-- One row per (scoring run, spec): catch-up planning, audit, performance timings. Owner: 12 (§2.2, §2.13).
CREATE TABLE IF NOT EXISTS score_runs (
  run_id          TEXT    NOT NULL,
  spec_version    TEXT    NOT NULL,
  as_of           INTEGER NOT NULL,          -- slot
  run_kind        TEXT    NOT NULL CHECK (run_kind IN ('official','provisional')),
  mode            TEXT    NOT NULL CHECK (mode IN ('strict','reconstructed')),
  trigger         TEXT    NOT NULL CHECK (trigger IN ('schedule','catchup','rescore','backfill','cli')),
  shadow          INTEGER NOT NULL DEFAULT 0 CHECK (shadow IN (0,1)),
  killed          INTEGER NOT NULL DEFAULT 0 CHECK (killed IN (0,1)),   -- SCORING_KILLED: computed, not written
  backfilled      INTEGER NOT NULL DEFAULT 0 CHECK (backfilled IN (0,1)),
  started_at      INTEGER NOT NULL,
  finished_at     INTEGER,
  ok              INTEGER CHECK (ok IN (0,1)),
  spec_hash       TEXT    NOT NULL,
  regime          TEXT,
  n_universe      INTEGER,
  n_scored        INTEGER,
  n_prelaunch     INTEGER,
  n_insufficient  INTEGER,
  n_events        INTEGER,
  n_errors        INTEGER,                   -- producer fn errors in the run (12 §3.1)
  timings_json    TEXT CHECK (timings_json IS NULL OR json_valid(timings_json)),
  error           TEXT,
  PRIMARY KEY (run_id, spec_version)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_score_runs_slot ON score_runs(spec_version, as_of, run_kind);

-- ── Enforcement triggers (§2.5) ──
CREATE TRIGGER IF NOT EXISTS trg_scores_no_update BEFORE UPDATE ON scores
BEGIN SELECT RAISE(ABORT, 'scores is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_scores_no_delete BEFORE DELETE ON scores
BEGIN SELECT RAISE(ABORT, 'scores is append-only'); END;

-- 02 §B4: only a raw failed/none row may be upgraded, by a later successful observation; norm rows are insert-once.
-- (Status read via json_extract, not the generated column, inside the trigger.)
CREATE TRIGGER IF NOT EXISTS trg_metric_values_update BEFORE UPDATE ON metric_values
WHEN OLD.stage = 'norm'
  OR COALESCE(json_extract(OLD.value_json, '$.status'), 'none') NOT IN ('failed','none')
  OR COALESCE(json_extract(NEW.value_json, '$.status'), 'none') IN ('failed','none')
  OR NEW.observed_at < OLD.observed_at
  OR NEW.occupant_key IS NOT OLD.occupant_key OR NEW.metric_key IS NOT OLD.metric_key
  OR NEW.as_of IS NOT OLD.as_of OR NEW.spec_version IS NOT OLD.spec_version OR NEW.stage IS NOT OLD.stage
BEGIN SELECT RAISE(ABORT, 'metric_values: only a failed raw row may be upgraded by a later successful observation'); END;
CREATE TRIGGER IF NOT EXISTS trg_metric_values_delete BEFORE DELETE ON metric_values
WHEN OLD.as_of % 86400 = 0
BEGIN SELECT RAISE(ABORT, 'metric_values: official rows are permanent'); END;

CREATE TRIGGER IF NOT EXISTS trg_events_update BEFORE UPDATE ON events
WHEN OLD.alerted_at IS NOT NULL
  OR NEW.occupant_key IS NOT OLD.occupant_key OR NEW.netuid IS NOT OLD.netuid
  OR NEW.event_type IS NOT OLD.event_type OR NEW.event_at IS NOT OLD.event_at
  OR NEW.observed_at IS NOT OLD.observed_at OR NEW.source IS NOT OLD.source
  OR NEW.dedupe_key IS NOT OLD.dedupe_key OR NEW.run_id IS NOT OLD.run_id
BEGIN SELECT RAISE(ABORT, 'events: only alerted_at/payload/severity may change, and only before alerting'); END;
CREATE TRIGGER IF NOT EXISTS trg_events_no_delete BEFORE DELETE ON events
BEGIN SELECT RAISE(ABORT, 'events is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_post_labels_no_update BEFORE UPDATE ON post_labels
BEGIN SELECT RAISE(ABORT, 'post_labels is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_post_labels_no_delete BEFORE DELETE ON post_labels
BEGIN SELECT RAISE(ABORT, 'post_labels is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_idobs_no_update BEFORE UPDATE ON identity_observations
BEGIN SELECT RAISE(ABORT, 'identity_observations is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_idobs_no_delete BEFORE DELETE ON identity_observations
BEGIN SELECT RAISE(ABORT, 'identity_observations is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_manual_flags_no_update BEFORE UPDATE ON manual_flags
BEGIN SELECT RAISE(ABORT, 'manual_flags is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_manual_flags_no_delete BEFORE DELETE ON manual_flags
BEGIN SELECT RAISE(ABORT, 'manual_flags is append-only'); END;

-- Latest observation per (occupant, source, field) = 06's "is_current" and 04's "t = now" read.
CREATE VIEW IF NOT EXISTS v_identity_current AS
SELECT io.* FROM identity_observations io
WHERE io.id = (SELECT i2.id FROM identity_observations i2
               WHERE i2.occupant_key = io.occupant_key AND i2.source = io.source AND i2.field = io.field
               ORDER BY i2.observed_at DESC, i2.id DESC LIMIT 1);

-- Latest score per occupant per spec_version (API/feed; filter by spec_version in the caller).
CREATE VIEW IF NOT EXISTS v_latest_scores AS
SELECT s.* FROM scores s
WHERE s.as_of = (SELECT MAX(s2.as_of) FROM scores s2
                 WHERE s2.occupant_key = s.occupant_key AND s2.spec_version = s.spec_version);

-- Spec 13 panel-builder names, preserved exactly (13 §3.1).
CREATE VIEW IF NOT EXISTS v_quant_price_bars AS
SELECT netuid, occupant_key, period, ts AS bucket_start, ts_last, open, high, low, close, volume,
       tao_in_pool, alpha_in_pool, moving_price, block_number, source, observed_at, backfilled
FROM price_bars;

-- Units: tao_liquidity is delivered in rao (research probe 2026-09-22: SN51 154321166411713 vs analytics
-- subnet_tao 153227189355104, both ≈ 153-154 k τ), so the view converts to τ. 05's live units probe re-checks;
-- a mismatch is fixed by a migration that redefines the view (DROP VIEW + CREATE VIEW is additive-policy compliant).
-- volume = analytics volume_24h (NULL on SSE rows; the table's own `volume` field has an UNVERIFIED window).
CREATE VIEW IF NOT EXISTS v_quant_chain_hourly AS
SELECT netuid, occupant_key, hour_ts AS hour, block_number, tao_liquidity / 1e9 AS tao_in_pool,
       subnet_moving_price AS moving_price, volume_24h AS volume, observed_at, backfilled, source
FROM chain_hourly;

-- 13 §3.1: emission_share = TMC emission_percent; tao_in_pool = subnet_tao / 1e9 (subnet_tao is rao — verified
-- against the research probe's 1Y analytics dump for SN51; same redefinition rule as above).
CREATE VIEW IF NOT EXISTS v_quant_chain_daily AS
SELECT occupant_key, netuid, day_ts AS day, event_at AS ts, block_number,
       emission_percent AS emission_share, subnet_tao / 1e9 AS tao_in_pool, observed_at, backfilled, source
FROM chain_daily;

-- registered_ts: exact (17 block_times) when known, else 04's estimate; registered_ts_exact says which.
CREATE VIEW IF NOT EXISTS v_quant_occupants AS
SELECT occupant_key, netuid, network_registered_at,
       COALESCE(registered_ts, registered_at_est) AS registered_ts, (registered_ts IS NOT NULL) AS registered_ts_exact,
       first_seen_at, last_seen_at, status, deregistered_at, deregistered_ts
FROM occupants;
