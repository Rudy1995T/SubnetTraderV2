-- GitHub collector clocks (owner 06 §2.5/§3.2, DDL owner 03; added 2026-10-17 at the Phase 4 build).
-- activity_ok_at: last successful activity pass for the repo (the activity_stale rule of 06 §2.8 reads it).
-- stats_checked_at: last /stats/participation + /stats/contributors refresh (weekly, GITHUB_STATS_INTERVAL_DAYS).
ALTER TABLE github_repos ADD COLUMN activity_ok_at INTEGER;
ALTER TABLE github_repos ADD COLUMN stats_checked_at INTEGER;
