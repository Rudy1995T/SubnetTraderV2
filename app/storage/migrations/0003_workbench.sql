-- @allow-destructive widen manual_notes.kind CHECK with 'chart_read' (19 §2.7, 02 §K2): rebuild copies every row
-- Workbench tables (owner 19, DDL owner 03): presets, chart uploads, the hourly dereg "weather" row (19 §2.4; its own
-- table so 05's SSE recorder and wb_dereg never write the same chain_network_hourly row).
CREATE TABLE manual_notes_v3 (
  id            INTEGER PRIMARY KEY,
  occupant_key  TEXT REFERENCES occupants(occupant_key),
  kind          TEXT NOT NULL CHECK (kind IN ('note','product_status','alias','llm_narrative','seed_edge','legacy',
                                              'team_disclosure','chart_read')),
  value         TEXT,
  title         TEXT,
  body          TEXT,
  evidence_url  TEXT,
  payload_json  TEXT,
  ref_event_id  INTEGER,
  source_ref    TEXT,
  created_at    INTEGER NOT NULL,
  created_by    TEXT NOT NULL,
  CHECK (kind <> 'product_status' OR value IS NULL OR value IN ('none','waitlist','beta','live')),
  CHECK (kind <> 'product_status' OR value IS NULL OR evidence_url IS NOT NULL),
  CHECK (kind <> 'team_disclosure' OR value IN ('anonymous','clear'))
) STRICT;
INSERT INTO manual_notes_v3 SELECT id, occupant_key, kind, value, title, body, evidence_url, payload_json, ref_event_id,
  source_ref, created_at, created_by FROM manual_notes;
DROP TABLE manual_notes;
ALTER TABLE manual_notes_v3 RENAME TO manual_notes;
CREATE INDEX IF NOT EXISTS ix_manual_notes_occ_kind ON manual_notes(occupant_key, kind, created_at DESC);

CREATE TABLE IF NOT EXISTS wb_presets (
  id            INTEGER PRIMARY KEY,
  name          TEXT    NOT NULL UNIQUE,
  weights_json  TEXT    NOT NULL CHECK (json_valid(weights_json)),
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS wb_uploads (
  id            INTEGER PRIMARY KEY,
  netuid        INTEGER,
  occupant_key  TEXT,
  path          TEXT    NOT NULL,
  sha256        TEXT    NOT NULL,
  media_type    TEXT    NOT NULL,
  bytes         INTEGER NOT NULL,
  model         TEXT,
  status        TEXT    NOT NULL CHECK (status IN ('stored','ok','unreadable','failed','vision_disabled')),
  note_id       INTEGER,
  created_at    INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS ix_wb_uploads_netuid ON wb_uploads(netuid, created_at DESC);

CREATE TABLE IF NOT EXISTS wb_dereg_network_hourly (
  hour_ts                          INTEGER PRIMARY KEY,
  head_block                       INTEGER,
  immunity_period_blocks           INTEGER,
  immunity_source                  TEXT,     -- rpc_storage | config
  pool_size                        INTEGER,
  pool_size_30d                    INTEGER,
  pool_size_90d                    INTEGER,
  k_30                             INTEGER,
  k_90                             INTEGER,
  reg_gap_mean_days                REAL,
  n_regs_window                    INTEGER,
  last_registration_block          INTEGER,
  last_registration_ts             INTEGER,
  earliest_next_registration_block INTEGER,
  eta_next_registration_days       REAL,
  lock_cost_tau                    REAL,
  lock_cost_decay_tau_per_day      REAL,
  lock_cost_observed_at            INTEGER,
  graduates_30d                    INTEGER,
  graduates_90d                    INTEGER,
  cull_line_1                      REAL,
  observed_at                      INTEGER NOT NULL,
  run_id                           TEXT
) STRICT, WITHOUT ROWID;
