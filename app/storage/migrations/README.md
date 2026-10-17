# Migrations

- Version 1 is `app/storage/models.sql` (the baseline). Versions ≥ 2 are `NNNN_<snake_name>.sql` here (4-digit, contiguous).
- **Additive only**: `CREATE TABLE/INDEX/VIEW/TRIGGER`, `ADD COLUMN` (nullable or constant default), `DROP/CREATE VIEW`,
  `DROP/CREATE TRIGGER`. No `DROP TABLE`, `DROP COLUMN`, `RENAME`, type changes or tightened constraints unless the
  file's first line is `-- @allow-destructive <reason>` (human review required).
- Forbidden in every file: `BEGIN`, `COMMIT`, `ROLLBACK`, `VACUUM`, `PRAGMA journal_mode`, `PRAGMA auto_vacuum`, `ATTACH`.
- An applied migration is never edited: a checksum change is reported as `checksum_drift:<v>` and never re-applied.
