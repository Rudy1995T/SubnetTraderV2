# [code] prior-sentiment-design

**Topic:** Prior sentiment-ingestion design in E:\SN_Trader\SubnetTrader (specs/feature-sentiment-ingestion.md + feature-sentiment-ingestion-phase-a.md): reusable concepts, API contract, phased plan, backtest gate, decisions to keep/reject for TaoSentinel

## Findings

## Status of what exists

Both specs are `Status: proposed` (dated 2026-06-19) and were **never implemented**: `Glob app/**/*sentiment*` and `tests/*sentiment*` return nothing. Only the design documents are reusable; there is no sentiment code, table, or test in the tree. `specs/INDEX.md:29-30` summarise both as proposed.

- `E:\SN_Trader\SubnetTrader\specs\feature-sentiment-ingestion.md` (427 lines, 61KB) — design-of-record, **Pi-side receive-only**.
- `E:\SN_Trader\SubnetTrader\specs\feature-sentiment-ingestion-phase-a.md` (609 lines) — tactical build plan for Phase A with full code listings.

## The "PC-side hype-funnel scorer" concept (what was planned, never designed)

The parent spec deliberately scopes the scorer OUT (Non-goals, lines 26-30): "All social scraping (Discord, Telegram, X, Reddit), hype-funnel feature extraction (github→website→socials→price), and sentiment scoring live in a **separate repository** that runs on the operator's PC. This spec never describes how the score is computed." So **there is no PC-side scoring math anywhere in these two files** — only the contract the scorer must satisfy.

What it did pin down about the scorer (lines 9, 104, 108-116):
- Sources: Discord, Telegram, X, Reddit; funnel order `github→website→socials→price`.
- Output: one compact score per subnet, pushed as a whole-universe batch ("the PC pushes the whole day's scored universe at once, not one request per subnet").
- **Two named components in one vector, not two endpoints** (line 104): `funnel` = "organic funnel quality" and `velocity` = "fast pump-velocity", "so the Pi stores both and the sizing hook can later weight them independently without an API change."
- Score range `[SENTIMENT_SCORE_MIN, SENTIMENT_SCORE_MAX]` = `[-1.0, 1.0]` (config.py block, lines 335-336).
- Provenance tag `model_version` (e.g. `"hype-funnel-2026.06"`, ≤64 chars) persisted verbatim for backtest grouping.
- `subnet-fundamentals.md:117-119` ("Future — community sentiment") is the origin: sentiment was to be "additional fields the off-Pi collector computes (it already harvests the discord/telegram/twitter handles per subnet)"; if cadence needs to beat git-commit transport, "graduate to a tiny polled HTTP service on a dev box."

The **only scorer actually built** in the old repo is the fundamentals quality score (`subnet-fundamentals.md:33-55`, code `app/strategy/quality.py`, collector `app/data/fundamentals_collect.py`, artifact `data/fundamentals.json`): 7 components weighted 0.30 github_active / 0.20 team_attributed / 0.15 has_github / 0.10 has_website / 0.10 has_description / 0.08 has_socials / 0.07 age, weight-renormalised average in [0,1], with explicit missing-data semantics (empty field = 0.0 real negative; unknowable = None dropped; absent subnet = None = neutral). Sources: subnetalpha.ai WP REST `GET /wp-json/wp/v2/subnet?per_page=100&page=N` (no key), on-chain `SubnetIdentity` + `network_registered_at`, GitHub `GET /repos/{owner}/{repo}` (`pushed_at`, `stargazers_count`, `archived`). This is the seed for TaoSentinel's GitHub/website/doxxed-team pillars.

## Scoring math on the consuming side (score → sizing factor)

- **Clamp**: factor ∈ `[SENTIMENT_FACTOR_FLOOR, SENTIMENT_FACTOR_CEIL]` = `[0.5, 1.0]` (lines 296-301). Ceil 1.0 = "de-risk-only": sentiment "can only ever shrink a position the base strategy already approved — never enlarge one or manufacture a new entry." Floor 0.5 so a bad score halves at most and "prevents sentiment from silently zeroing out an otherwise-valid base signal, which would be a de facto block."
- **TTLs**: `SENTIMENT_TTL_SEC=21600` (6h, chosen to survive the PC's overnight off-window); `SENTIMENT_FUTURE_SKEW_SEC=300` (reject `as_of` >5 min in future); parent payload also carries per-batch `ttl_seconds` bounded `1..86400`. TTL boundary is **inclusive** (`now - as_of > TTL` ⇒ stale; phase-a §4: "a read at as_of + TTL still applies").
- **Factor-curve idea — deferred, verbatim from phase-a §3.2 lines 143-148**:
  ```python
  def _score_to_factor(self, score: float) -> float:
      # Phase-A placeholder: maps every score to the neutral 1.0 (then clamps).
      # The real score→factor curve is DEFERRED to a later phase (see Open
      # Questions), so even active mode is a no-op until it lands. CEIL defaults
      # to 1.0, so this can only ever de-risk, never upsize.
      return self._clamp(1.0)
  ```
  Parent open question 3 (line 425): symmetric/upsize band "deferred to after Phase C proves the signal — and would need its own clamp-rationale and a tighter cap."
- **Fail-safe rule (verbatim, line 268)**: "**missing OR stale OR disabled OR killed → `sentiment_factor = 1.0` (no effect).**" `factor_for(netuid, now)` is "the single fail-safe choke point" (branches 1-5 at lines 272-276). Phase-a splits it into `would_be_factor` (honors enabled/kill/TTL, ignores mode; used for observe logging + GET) and `factor_for` (returns 1.0 unless `ENABLED and MODE=="active"`, else `== would_be_factor`), so "stored == applied" by construction.
- Hook placement: `amount_tao = round(amount_tao * applied, 6)` after the pool-depth clamp (`ema_manager.py:2040`) and before the balance check (`:2044`); observe mode logs + persists but does not multiply (phase-a §3.9c lines 431-466).

## Three-layer fail-safe (lines 51-57) — keep

1. Default-off (`SENTIMENT_ENABLED=False`) ⇒ dormant merge. 2. Observe-before-act (`SENTIMENT_MODE ∈ {off, observe, active}`, default `observe`, config not code, validated by `field_validator` lowercase-normalised). 3. Bounded effect (clamp + `SENTIMENT_KILLED` sentinel file `./SENTIMENT_KILL_SWITCH_PATH`, `is_killed = os.path.exists`, cleared by `rm` or `POST /api/control/sentiment-resume`; ingestion still accepted while killed so the audit stream is uninterrupted).

## API contract

**Auth**: existing `require_auth` in `E:\SN_Trader\SubnetTrader\app\api_auth.py:21-36` — `hmac.compare_digest` of `X-Auth-Token` header vs live `app_config.settings.API_AUTH_TOKEN` as UTF-8 bytes; empty token disables the gate; 401 `{"detail":"Missing or invalid auth token"}`. Whole router mounted with `app.include_router(sentiment_router, dependencies=[Depends(require_auth)])` so the debug GET is also authed (the stored vector "leak[s] the PC service's signal"). Transport: Tailscale/WireGuard tunnel, `HEALTH_HOST=127.0.0.1` loopback bind, never `0.0.0.0`; CORS irrelevant to a server-side poster; do NOT add an explicit OPTIONS handler (line 100).

**`POST /api/sentiment/score` — parent design payload (verbatim, lines 106-117)**:
```jsonc
{
  "as_of": "2026-06-19T14:32:00Z",         // ISO-8601 UTC; when the PC computed these scores
  "ttl_seconds": 21600,                     // freshness window for this batch (e.g. 6h)
  "model_version": "hype-funnel-2026.06",   // optional free-form provenance tag (audit)
  "scores": [
    { "netuid": 64, "funnel": 0.72, "velocity": 0.15 },   // funnel = organic quality, velocity = pump
    { "netuid": 18, "funnel": 0.40, "velocity": 0.88 }
    // ... one entry per scored subnet; subnets absent here simply have no score (→ neutral)
  ]
}
```
Validation table (lines 119-127): `as_of` required, 422 if > skew in future or already older than `ttl_seconds` at receipt; `ttl_seconds` 1..86400; `model_version` ≤64; `scores` 1..128 entries, empty ⇒ 422; `netuid` 0..1023, duplicates ⇒ 422; `funnel`/`velocity` in `[SCORE_MIN, SCORE_MAX]`. Whole batch validates before any write ("rejected and logged, never partially stored").

**200 response** (lines 133-141): `{"stored":2,"as_of":…,"expires_at":as_of+ttl,"enabled":false,"mode":"observe"}` — the `enabled/mode` echo is deliberate "so the PC learns the feed is dormant — no silent black hole." Errors: 401 / 422 / 503 (`db`/store not initialized).

**Phase-A concrete Pydantic models (phase-a §3.3 lines 210-241) DIVERGE from the parent**: `ScoreRow{netuid: int ge0 le1023, score: float}` (single `score`, not `funnel`/`velocity`), `ScorePost{as_of: float (epoch seconds), model_version: str="" max64, scores: list[ScoreRow] min1 max128}`, **no `ttl_seconds`** (TTL is Pi-side config). Response drops `expires_at`.

**`GET /api/sentiment[?netuid=N]`** (lines 157-175): returns every stored row including stale ones — `fresh`, `expires_at`, `would_apply_factor` — "the at-a-glance proof of the fail-safe."

## Storage decisions

- Live score: **in-memory `dict[int, ScoreEntry(score, as_of, received_at)]`**, TTL-on-read (Option A, lines 181-196), chosen because the score is "ephemeral, fast-decaying" and re-hydrating a near-TTL score after restart is "actively undesirable"; also avoids contention with the single auto-commit aiosqlite connection. The spec explicitly says (line 196): "A queryable score history for diagnostics is a separate append-only-log decision, orthogonal to the live-decision store."
- Last-write-wins per netuid; a partial batch never wipes another subnet's fresh score (staleness, not absence-from-batch, neutralises).
- Durable audit: four `ema_positions` columns `sentiment_factor REAL DEFAULT 1.0 / sentiment_score REAL NULL / sentiment_as_of TEXT NULL (ISO-8601) / sentiment_mode TEXT DEFAULT 'off'` via idempotent `ALTER TABLE` inside the existing try/except loop. `DEFAULT 1.0` is load-bearing ("A reviewer must not simplify this to a nullable column", line 230). Persistence rule: record what was **decided** (would-be factor in observe) distinct from what was **applied**.
- Time convention (phase-a §3.2 callout, line 83): epoch-seconds floats end-to-end; the parent's `factor_for(netuid, now=utc_now())` datetime snippet is flagged as wrong.

## Phased rollout A→D (lines 384-389)

- **A — land dormant**: endpoint + store + audit columns + hook wired inert; verify `compileall`/`import app.main` green, POST round-trips 200, `amount_tao` byte-identical when disabled.
- **B — observe**: `ENABLED=true, MODE=observe`; hook logs `SENTIMENT[observe] SN<id>: score=… factor=…` and persists; at night factor reads exactly 1.0; survives `systemctl restart`.
- **C — backtest correlation** (no Pi behaviour change): offline job joins persisted `sentiment_factor/score/as_of` against `pnl_pct/pnl_tao` on the same row, computes "an information-coefficient-style statistic", writes `factor_*.json`/`quality_*.json` into `data/backtest/results/` so the read-only explorer surfaces it with zero new code.
- **D — active**: `MODE=active`, clamp `[0.5,1.0]`, kill switch armed; unit test asserts clamp; `touch ./SENTIMENT_KILLED` returns factor to 1.0 instantly.
- Emergency fallback at any phase: sentinel file or config flip (hot-reloaded).
- Note: phase-a §1 (line 26) relabels — "go-live flip is Phase B, backtest/correlation is Phase C, score→factor tuning is Phase D" — slightly inconsistent with the parent (B=observe, D=active).

## The backtest gate (evidence bar before capital)

Verbatim (line 386): "*Verify:* a robust, sign-stable positive relationship between persisted sentiment and subsequent returns on held-out data **before any capital is risked**. A null/negative result halts the rollout here (the feature stays in observe, costing nothing)." Line 317: "No capital is risked until that artifact shows a robust positive signal."

Required artifact shape (verified in `E:\SN_Trader\SubnetTrader\app\backtest_api.py:173-195` `_rows_from_factor` and `FactorRow` at `:57-74`): top-level `scope` ("live"|"survivorship"), `span`, `timestamp`; `factors:{<name>:{<horizon>:{lookback, min_depth, ic:{mean_ic, ic_ir, t_stat, pct_positive, avg_names}, deciles:{mean_long_short, spread_t_stat}}}}`; filename prefix `factor_`/`quality_` with `_YYYYMMDD_HHMMSS` suffix (`_kind_for` at `:96`). The repo's wider evidence discipline (INDEX.md:26-31): cross-sectional factor test on the death-inclusive `pool_history` panel, within-size-band read to de-confound size, static-only features + current-occupant join to kill look-ahead, per-cell DSR gate, and a documented NO-GO (momentum candidate) when IC was not size-orthogonal.

## Decisions to keep vs reject for TaoSentinel

**Keep**: capture-vs-trade split (scorer never in the trading process); fail-safe-neutral with a single choke point; observe-before-act modes as config; de-risk-only clamp until evidence; file-sentinel kill switch; two-component vector (`funnel` quality vs `velocity` hype) — extend to N named pillars; `model_version` provenance; whole-batch atomic validation, future-skew reject, dup-netuid reject, netuid 0..1023; `enabled/mode` echo in the response; `DEFAULT 1.0` non-nullable audit column; decided-vs-applied persistence; epoch-seconds convention; IC-style backtest gate with the existing `factor_*.json` shape; no module-top I/O.

**Reject / revisit**: (1) the "PC off at night ⇒ intermittent feed" premise — the Ubuntu laptop is 24/7 and is the collector, so 6h TTL rationale is moot (a TTL still belongs, but sized to the collector cadence); (2) in-memory-only, non-durable score store — for TaoSentinel the score **history** is the product, so use the spec's own suggested append-only log as primary, with the dict-with-TTL only as the trading-side consumer cache; (3) Phase-A's regression to a single `score` field and dropping `ttl_seconds`/`expires_at` — prefer the parent's multi-component vector; (4) `[-1,1]` range vs the user's 0-100 scale — pick one and convert at the boundary; (5) subnetalpha.ai scraping of the fundamentals collector is editorial/fragile per `subnet-fundamentals.md:13` — plan for failure.

## Reusable assets

- `E:\SN_Trader\SubnetTrader\specs\feature-sentiment-ingestion.md` — Design-of-record: three-layer fail-safe, POST /api/sentiment/score schema (funnel+velocity vector, as_of, ttl_seconds, model_version), 200/401/422/503 contract, debug GET with fresh/expires_at/would_apply_factor, in-memory TTL store rationale, audit columns, phased A→D rollout, backtest gate, 8 safety properties table (lines 402-411), risks, open questions.
  - How to reuse: Lift the payload schema (lines 106-127), response (133-141), fail-safe rule (268-282), clamp rationale (296-301), phased plan (384-389) and safety-properties table nearly verbatim into the TaoSentinel 'trading-consumer contract' spec; replace the PC-off-at-night premise with the 24/7 laptop cadence.
- `E:\SN_Trader\SubnetTrader\specs\feature-sentiment-ingestion-phase-a.md` — Complete code listings: sentiment_kill.py (§3.1), SentimentStore with would_be_factor/factor_for/_clamp/_fresh_entry (§3.2 lines 86-170), Pydantic ScoreRow/ScorePost + router (§3.3 lines 179-277), config block + SENTIMENT_MODE validator (§3.4), .env block (§3.5), SQL migration (§3.6-3.8), sizing hook (§3.9c lines 431-466), test plan (§4), verification checklist (§5).
  - How to reuse: The store, kill-sentinel and router code are stdlib+FastAPI+Pydantic only and can be copied into the new repo as the bot-side consumer; swap the single `score` field back to a multi-component vector and add a durable append-only history table alongside the dict.
- `E:\SN_Trader\SubnetTrader\app\api_auth.py` — require_auth: constant-time hmac.compare_digest of X-Auth-Token vs live settings.API_AUTH_TOKEN; empty token disables; clean 401 (37 lines).
  - How to reuse: Copy verbatim for TaoSentinel's ingest/control API auth; mount router-wide with dependencies=[Depends(require_auth)].
- `E:\SN_Trader\SubnetTrader\app\portfolio\value_kill.py` — is_killed/engage_kill/clear_kill file-sentinel helpers (lines 178-199) that SENTIMENT_KILLED was modelled on.
  - How to reuse: Copy is_killed/clear_kill 1:1; use phase-a §3.1's single-arg engage_kill(path, reason) variant.
- `E:\SN_Trader\SubnetTrader\app\backtest_api.py` — Read-only factor-artifact reader: _kind_for (line 88-99, factor_/quality_ prefixes), FactorRow TypedDict (57-74), _rows_from_factor (173-195) defining the factors:{name:{horizon:{ic:{mean_ic,ic_ir,t_stat,pct_positive,avg_names},deciles:{mean_long_short,spread_t_stat},lookback,min_depth}}} shape.
  - How to reuse: TaoSentinel's Phase-C IC job should emit exactly this JSON shape so the existing explorer (and a ported copy) renders sentiment pillars as factors with no new code.
- `E:\SN_Trader\SubnetTrader\specs\subnet-fundamentals.md` — The only BUILT off-Pi scorer: 7-component weighted quality score [0,1] (github_active .30, team_attributed .20, has_github .15, has_website .10, has_description .10, has_socials .08, age .07), missing-data semantics, sources (subnetalpha.ai WP REST /wp-json/wp/v2/subnet, on-chain SubnetIdentity, GitHub /repos), data/fundamentals.json artifact schema (lines 59-81), FundamentalsStore read API.
  - How to reuse: Directly seeds TaoSentinel's GitHub/website/doxxed-team pillars; port app/strategy/quality.py + app/data/fundamentals_collect.py and extend with sentiment pillars per its own 'Future — community sentiment' section (lines 117-119).
- `E:\SN_Trader\SubnetTrader\app\strategy\quality.py` — Pure derive_components(record) → quality_score(components) → (score|None, coverage) scorer, no I/O.
  - How to reuse: Port as the fundamentals pillar; rescale [0,1] to 0-100 at the boundary.
- `E:\SN_Trader\SubnetTrader\app\data\fundamentals_collect.py` — Off-Pi collector for subnetalpha.ai + on-chain identity + GitHub API producing data/fundamentals.json; flags --no-onchain --no-github --limit --network.
  - How to reuse: Base of TaoSentinel's collector; it already harvests discord/twitter/telegram handles per subnet.
- `E:\SN_Trader\SubnetTrader\data\fundamentals.json` — Existing snapshot of ~130 subnets with links, age, github freshness, quality scores.
  - How to reuse: Seed dataset / backfill baseline for the new registry table; check generated_at before trusting.

## Gotchas

- Neither spec was implemented: no app/sentiment_api.py, sentiment_store.py, sentiment_kill.py or tests/test_sentiment_* exist; only the documents are reusable.
- Payload schema conflict: the parent design uses ISO-8601 `as_of`, per-batch `ttl_seconds`, and `funnel`+`velocity` components; the Phase-A code uses epoch-float `as_of`, no `ttl_seconds` (Pi-side SENTIMENT_TTL_SEC), and a single `score` field. Pick one explicitly (recommend the parent's multi-component vector with epoch seconds).
- Time convention: the parent's `factor_for(netuid, now=utc_now())` datetime snippet is wrong; Phase-A mandates epoch-seconds floats end-to-end (store does `now - entry.as_of`).
- The 6h TTL and the whole 'PC off at night ⇒ intermittent feed' invariant assume the scorer is on a daytime-only PC; TaoSentinel's collector is the 24/7 laptop, so re-derive TTL from collector cadence rather than copying 21600.
- The recommended in-memory store (Option A) deliberately discards score history; the spec itself notes a durable append-only log is a separate decision. TaoSentinel needs the history as its primary product.
- Phase labelling drifts between docs: parent = A dormant / B observe / C backtest / D active; phase-a §1 says D = 'score→factor tuning'.
- `SENTIMENT_MODE` defaults to 'observe' while `SENTIMENT_ENABLED` defaults false; the ENABLED gate lives at the read site (factor_for), not in a model_validator — do not add one.
- `sentiment_factor` audit column must be `REAL DEFAULT 1.0`, never nullable (no-op multiplier semantics for legacy/observe/stale rows).
- TTL boundary is inclusive: `now - as_of > TTL` ⇒ stale; exactly at TTL still applies.
- All ema_manager.py/main.py/db.py line anchors are as of 2026-06-19 and may have drifted; phase-a itself corrected two mislabels (init_services() not setup(); managers built conditionally inside `if settings.<X>_ENABLED:` blocks).
- Live-strategy ambiguity in the old bot: ops-hard-risk-controls.md claims trend retired 2026-06-02 while README/CLAUDE.md say trend live; confirm before wiring any sizing hook.
- Score range is [-1,1] in the old design vs the user's requested 0-100 scale — convert at the API boundary and document which is canonical.
- subnetalpha.ai collection is an editorial scrape (WP REST API, no key) described as fragile; Taostats/tao.app dev_activity was skipped in v1 to avoid a paid/keyed dependency.

## Open questions

- Which payload shape is canonical for TaoSentinel: parent (ISO as_of + ttl_seconds + funnel/velocity) or Phase-A (epoch as_of + single score)? Recommend parent shape generalised to N named pillars plus a composite.
- What TTL/cadence applies now that the collector runs 24/7 on the laptop (spec's 6h was sized to an overnight PC gap)?
- Does TaoSentinel need a market-wide regime score in addition to per-netuid scores (parent open question 2 deferred it)?
- Will the trading bot be revived on the same laptop (loopback POST) or a separate box (tunnel + X-Auth-Token)? This decides whether the ingest endpoint or a shared SQLite/JSON artifact is the seam.
- What forward-return panel will the Phase-C IC job use given the old bot had 0 closed value trades — pool_history cross-sectional panel (as subnet-fundamentals-phase2 did) or live ema_positions rows?
- Is the de-risk-only clamp [0.5,1.0] still the right first band, or should TaoSentinel's allocation model allow upsizing on strong composite scores once backtested?
