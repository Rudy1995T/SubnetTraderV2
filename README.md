# TaoSentinel — design package

**What this is:** the complete design for *TaoSentinel*, a 24/7 Bittensor subnet health + sentiment scoring service (0–100 per subnet, six evidence pillars, point-in-time history, quant validation, fail-safe feed for a trading bot). Produced 2026-09-22 by Claude Fable 5.1 (Ultracode multi-agent research + synthesis) for Rudolf. Nothing here is code yet; it is what a coding model (Claude Opus) implements on the Ubuntu laptop.

```
E:\TaoData\
├── README.md                    ← you are here
├── specs\
│   ├── INDEX.md                 ← status-tagged map + implementation order
│   ├── 00-MASTER-SPEC.md        ← binding design-of-record (read first)
│   ├── 02-cross-cutting-conventions.md ← binding tie-breaker for cross-spec names/contracts (read second)
│   ├── 01, 03…17-*.md           ← component specs, one per phase
│   └── 18-operator-checklist.md ← what Rudolf must provide/do (keys, accounts, Pi recovery)
└── research\
    ├── INDEX.md                 ← 16 research reports the design rests on
    ├── 01-*.md                  ← old-bot code maps + web research (APIs, ecosystem, quant methods)
    └── 02-gapfill-*.md          ← verified deep-dives (TMC keyless API, Discord topology, GitHub resolver, backfill, owner extraction)
```

## Build status and running it (laptop)

**Phase 0 is built** (2026-09-24): runtime, config (all 627 keys), SQLite schema v1, scheduler with the full 53-job table
(7 available in Phase 0), `/health`, alert dispatch, backups, cost governor, pause/kill switches, systemd units, 108 tests.
**Phase 1 is built** (subnet registry: occupants, three-source identity merge, review list) and **Phase 2 is built**
(TMC SSE recorder + REST jobs, v440 gate fit, dereg/immunity, holders, P1 metrics, first P1 matrix): 16 jobs available,
138 tests. Deviations and open operator steps are in [`specs/BUILD-LOG.md`](specs/BUILD-LOG.md).

```bash
make install-dev            # .venv + dependencies (Python 3.12)
.venv/bin/python -m app check   # schema + job registry sanity check
make test                   # pytest (138 tests)
make run                    # foreground service on 127.0.0.1:8090  (Ctrl-C stops it cleanly)
curl -s localhost:8090/health | python3 -m json.tool | head -30
.venv/bin/python -m app.registry review          # registry review list (Phase 1)
.venv/bin/python -m app.features.chain_features matrix   # ranked P1 matrix (Phase 2, after a slot has run)
sudo ops/install-systemd.sh && sudo systemctl start taosentinel   # 24/7 under systemd
```
Secrets and toggles go in `.env` (mode 600, already created with the keys you will need, in phase order);
`.env.example` lists every key with its default.

## How to use this package

1. **Operator (Rudolf):** work through `specs/18-operator-checklist.md` section A before the build; sections B–C as phases arrive. Copy this whole folder plus the old bot's seed data to the laptop (checklist A2–A3).
2. **Implementer (Opus on the laptop):** read `specs/00-MASTER-SPEC.md` in full, then `specs/02-cross-cutting-conventions.md`, then `specs/INDEX.md`, then implement phases in order. Each component spec ends with a *Verify checklist*; a phase is done when those commands pass and `pytest` is green. When a spec cites `research/…` or an old-bot `file:line`, read it — the citations are precise and were verified on 2026-09-22.
3. **Every collector ships `observe`-first, flag-gated, keyless where possible.** The first useful output (the on-chain matrix, Phase 2) needs no keys at all.

## Suggested kickoff prompt for the implementing model

> You are implementing TaoSentinel on this Ubuntu laptop from the specs in `~/taosentinel/specs/`. Read `specs/00-MASTER-SPEC.md` completely, then `specs/02-cross-cutting-conventions.md` (binding tie-breaker; it wins over any other spec on the points it covers), then `specs/INDEX.md`. Implement Phase 0 (`01-repo-skeleton-runtime.md` + `03-storage-schema.md`) now: create the repo layout, `pyproject.toml`, `app/config.py`, `app/runtime.py` (ported from `seed/app/main.py` per the spec's line references), `app/storage/`, `/health`, the scheduler registry, tests, and the systemd unit. Follow the specs literally; where a spec says UNVERIFIED, verify against the live API before relying on it and record what you found in `specs/BUILD-LOG.md`. Do not add features not in the specs. Stop after Phase 0's Verify checklist passes and report.

## Key design facts (so nobody re-derives them)

- Identity is `occupant_key = "{netuid}:{network_registered_at}"`, never netuid alone (slots are recycled; 42/128 names changed Jun→Sep 2026).
- The on-chain pillar is keyless via **taomarketcap.com's public API + SSE** (10 req/min, 100k/month, 2 SSE); a local RPC node is optional.
- Emission is gated (v440): rank ~32 is a cliff; ~40 subnets are zeroed by implied miner-burn — score on **realised** emission, not moving-price rank.
- `NetworkImmunityPeriod` live value = **864,000 blocks (120 d)**; docs still say 1,296,000 — stale.
- The owner cut (18 %) accrues as alpha stake on the owner hotkey/coldkey; auto-lock is **opt-in** (12/121); `set_perpetual_lock(false)` is the formal exit signal.
- GitHub must be resolved by **numeric repo id** (301 renames) and scored **org-wide** (Chutes' declared repo is idle while the org ships weekly).
- X data is pay-per-use only; Discord bots cannot join third-party servers → keyless invite metadata + Followed announcement channels.
- Absence is a signal (0.0); collection failure is not (None). Point-in-time storage from day one. No capital until the pre-registered evidence gate passes.

## Cost envelope
Keyless build: $0. Free keys (GitHub PAT, OnFinality, Telegram, Discord bot): $0. LLM classification ≈ $20–60/mo. X via twitterapi.io ≈ $5–20/mo. TaoStats Standard $49/mo optional. Ceiling ≈ $150/mo with everything on.
