# TaoSentinel — working notes for Claude Code sessions

Bittensor subnet health/sentiment scorer (0–100 per subnet, six pillars, point-in-time SQLite history, quant gates,
read-only score feed for a trading bot). Python 3.12, FastAPI, APScheduler, SQLite WAL. Runs 24/7 on this laptop as
the systemd unit `taosentinel` (127.0.0.1:8090). The repo is **public** (github.com/Rudy1995T/SubnetTraderV2).

## Read in this order
1. `specs/INDEX.md` (status map) → `specs/20-consolidation-and-next-steps.md` (as-built state + what to do next).
2. `specs/BUILD-LOG.md` — every deviation from a spec and every live finding lives here; append, never rewrite.
3. For any component: its spec `specs/NN-*.md`. Precedence on conflicts: `02-cross-cutting-conventions.md` › `00-MASTER-SPEC.md` › owner spec.
4. Operator to-dos (keys, accounts, sudo steps): `specs/18-operator-checklist.md` and spec 20 §3.

## Rules that have held since Phase 0
- Review the spec against the live data/APIs **before** building it; record what was wrong in the spec's Changelog and in BUILD-LOG.
- Do not add features that are not in a spec; if a spec is wrong, fix the spec first.
- A phase is done when its `§10 Verify` items pass on this laptop and `pytest` is green. Keep `tests/conftest.py` `BUILT_PHASE` / `AVAILABLE_BY_PHASE` in step with `python -m app check`.
- Absence is a signal (0.0); collection failure is `None`. `metric_values` is first-success-wins and append-only (triggers). `scores` is append-only.
- Keyless first; paid paths are flag-gated and governed (`app/costs.py`); never add a paid call outside the governor.

## Running things
```bash
.venv/bin/python -m app check                 # schema + job table (58 declared)
.venv/bin/python -m pytest -q                 # 1174 tests, ≈ 10 min; -m smoke for the fast path; tests/quant alone ≈ 1 min
make lint                                     # compileall
curl -s localhost:8090/health | python3 -m json.tool | head -40
curl -s 'localhost:8090/api/v1/scores/latest' | head -c 600
```
Service control needs sudo and is the operator's: `sudo systemctl restart taosentinel` after code changes (the running
process never reloads code). Logs: `data/logs/YYYY-MM-DD.jsonl` (`data/service.log` is root-only — do not try).

## Live-system cautions
- The live DB is `data/taosentinel.db` (≈ 5.4 GB). Inspect it read-only: `sqlite3.connect("file:data/taosentinel.db?mode=ro", uri=True)`. Do not run CLI jobs that write to it unless the spec's verify step says so — writes can page the operator through `alert_dispatch`.
- Shell env vars do not persist between Bash calls: pass `DB_PATH=/path/to/copy.db` inline on the same command line.
- Never print `.env` values; it is mode 600 and holds keys. `.env.example` is generated and has no real values. Scan a diff for secrets before any push.
- Long jobs (backfills, scoring backfill, quant) run from the CLI outside the unit (`MemoryMax=2G`), at night, and are resumable by design.
- Timezone on this host is still Europe/London (spec wants UTC); all app times are UTC.

## Sister project
`~/tao-terminal` (not a git repo): the τ Terminal (charts, paper trading, Screener = spec 19 W7, spec 15 consumer in
`app/sentinel/`, v2 bot plan in `docs/SUBNETTRADER_V2_PLAN.md`). Tests: `cd ~/tao-terminal && .venv/bin/python -m pytest -q`
(169). User services `tao-terminal` (127.0.0.1:8787) and `tao-collector`. Paper only unless the operator says otherwise.
