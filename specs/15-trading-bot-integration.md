# 15 — Trading-bot integration contract

**Status:** proposed · **Phase:** 12 · **Depends on:** 02 (ledger: §H1 feed contract, §B6 `scores` time semantics, §B1 time encoding, §B12 sentinels), 14 (the producer: `GET /api/v1/scores/latest`, Phase 11), 12 (writes `scores` at `spec_version = ts-1.0`), 13 (verdict JSON §3.6, gate criterion c7), 16 (`SCORING_KILLED`, restore and spec-promotion runbooks) · **Master spec refs:** §1.3 (evidence before capital, fail-safe ops), §2 (adopt `feature-sentiment-ingestion*.md`; the bot keeps its own `api_auth.py`), §5 (`occupant_key`), §6 (`scores`), §7.5 (red-flag names), §7.7 (confidence, tiers), §9 (trading contract shape), §10 (evidence gate), §11 (kill switches), §12 (Phase 12).

---

## 1. Purpose and scope

This spec is the **only seam** between TaoSentinel and capital. It defines (a) which fields of spec 14's versioned, fail-safe-neutral **score feed** the bot relies on and how it validates them (14 owns and serves the feed; 02 §H1), (b) the **consumer module** a revived SubnetTrader bot uses to read it, (c) the **score→factor curve** and the clamp band, (d) the **observe/active** ladder and the evidence gate that guards every step, and (e) the audit trail that lets Phase C prove or refute the signal before any position is shaped by it.

**In scope**
- TaoSentinel side: **nothing to build.** The bot consumes spec 14's `GET /api/v1/scores/latest` unchanged (02 §H1). This spec has no producer route, cache, SQL, job or config key of its own; §3.1 lists the response fields the bot relies on, and §10 T1–T3 check them against the live route.
- Bot side (revived SubnetTrader repo, *not* the TaoSentinel repo): `app/sentinel/` package (models, `ScoreStore`, curve, kill sentinel, poller, read-only router), `score_feed_log` table, eight audit columns on `ema_positions`, the single sizing hook in `EmaManager._enter`, `/health.sentinel`, control endpoints, the observe-telemetry file for 13's criterion c7, tests.
- The rollout ladder A → B → C → D → (E) with mechanical enforcement (active mode requires a gate-verdict artifact on disk).

**Out of scope**
- How scores are computed (spec 12), how the feed is served (spec 14 §3.5, incl. its SQL and `SCORE_FEED_TTL_SEC`), how the gate is computed (spec 13), any wallet/executor/strategy code (master §2: not ported into TaoSentinel).
- Any new entry signal. The factor is a **post-sizing multiplier**; it never creates, re-orders or (by default) vetoes an entry. The optional `SENTINEL_FLAG_BLOCK_ENTRY` opt-in (§2.6) is the single, explicit exception master §9 allows.
- Reviving the bot itself (its own repo, own systemd unit). This spec ships the bot-side change set as a patch plan against `E:\SN_Trader\SubnetTrader` with verified line anchors.

---

## 2. Design

### 2.1 Topology decision

Both services run on the **same Ubuntu laptop**, both bind loopback, the bot **pulls**.

```
 laptop (Ubuntu, systemd)
 ┌─────────────────────────────────────┐        ┌──────────────────────────────────────────┐
 │ taosentinel.service  127.0.0.1:8090 │        │ subnettrader.service  127.0.0.1:8081     │
 │                                     │  GET   │                                          │
 │  scores (SQLite) ─► 14's route ─────┼◄───────┼── SentinelClient.poll_once()  every 15 m │
 │  ▲     /api/v1/scores/latest        │  200   │         │                                │
 │  │     (open read tier, no cache)   │        │         ▼                                │
 │  scoring engine (spec 12)           │        │   ScoreStore (dict[netuid], per-row TTL) │
 │  slot as_of hh:00Z, run at hh:15Z   │        │         │   └─► score_feed_log (append)  │
 │                                     │        │         ▼                                │
 │  ./SCORING_KILLED → stale=true      │        │   EmaManager._enter hook, after every    │
 │                                     │        │   base gate, just before execute_swap:   │
 │                                     │        │   amount_tao = round(amount_tao*f, 6)    │
 └─────────────────────────────────────┘        │   ./SENTINEL_KILLED → factor 1.0         │
                                                └──────────────────────────────────────────┘
```

| Decision | Value | Rationale |
|---|---|---|
| Transport | Bot **pulls** `GET http://127.0.0.1:8090/api/v1/scores/latest?spec=ts-1.0&include_insufficient=1` (spec 14 §3.5) every `SENTINEL_POLL_INTERVAL_MIN` (15) | Same box, same clock, no tunnel; TaoSentinel needs no knowledge of the bot; a dead bot costs TaoSentinel nothing; a dead TaoSentinel costs the bot nothing (neutral). `include_insufficient=1` makes rows with `confidence < 0.4` arrive, so the bot records `low_confidence` rather than `missing` and still sees a block flag on such a row. `spec` is pinned, so a producer-side `SPEC_VERSION` promotion never silently switches the series. |
| Auth | 14 serves the route in its **open read tier** (master §9: auth on mutations only). The bot sends `X-Auth-Token: $SENTINEL_AUTH_TOKEN` only when that key is non-empty (harmless on an open route). The bot's own sentinel router and control routes always sit behind the bot's `require_auth`. | Loopback reads are open by master rule. Exposing `:8090` beyond loopback is 14's and 16's decision (02 §I2), not this spec's. |
| Push variant | **None in v1.** | 02 §H1: 15 has no producer-side component, and both services share the laptop. A bot on another host would pull the same route over the tailnet (deferred). |
| Ports | TaoSentinel `:8090` (API), `:3001` (UI, Phase 11); bot keeps `HEALTH_PORT=8081` (`config.py:494`), `HEALTH_HOST=127.0.0.1` (`config.py:501`), frontend `:3000`. | No collisions (answers research 02 open question). |
| systemd ordering | `subnettrader.service` gets `After=taosentinel.service` only: no `Wants=`, never `Requires=`. | The bot must start and trade normally when TaoSentinel is absent. `Wants=` would also *start* TaoSentinel whenever the bot (re)starts, for example on a bot `Restart=on-failure` or auto-update while the operator has TaoSentinel stopped for 16's restore runbook. |
| Clock | Shared system clock on loopback. 14's body carries `generated_at` (response build time, ISO); `skew_sec = generated_at − observed_at` is logged and drives the future-skew reject (§3.2; 02 §B6). | The skew tripwire (`SENTINEL_FUTURE_SKEW_SEC`) stays in force even though both services share one clock. |

### 2.2 Data flow and timing

1. Spec 12 writes one `scores` row per active occupant per run. `as_of` is the **slot instant** (00:00Z official; 06/12/18:00Z provisional). The run starts at hh:15Z and lands within its 1 200 s job timeout, and `computed_at` is the run instant (02 §B6). Nothing in this spec is called by the scoring engine.
2. Spec 14's `queries.latest_scores` serves the **latest row per occupant** for the requested `spec`, so per-row `as_of` may differ (an occupant skipped in a provisional run keeps its official row). The batch `as_of` is the max row `as_of`. `as_of`, `computed_at` and `generated_at` are ISO strings and `red_flags` are name strings. Pre-launch rows (null `composite`/`tier`) are excluded by default, and rows with `confidence < 0.4` are dropped unless `include_insufficient=1` (02 §H1; 14 §3.5). An empty DB returns `200 {as_of: null, count: 0, scores: [], stale: true}`.
3. 14 sets the batch flag `stale = (now − as_of) > ttl_seconds or exists(./SCORING_KILLED)`. `ttl_seconds` is 14's `SCORE_FEED_TTL_SEC` (21600 today; §11 Q1).
4. The bot polls. An ETag on this route is optional (02 §H1). The client sends `If-None-Match` only after a `200` that carried an ETag, and treats a `304` as an ok poll with no ingest. The client works identically whether or not 14 emits ETags. A `200` is validated **whole-batch** by `FeedPayload` (§3.2). `ScoreStore.ingest()` then applies the future-skew reject and the spec-version whitelist, and handles each row:
   - freshness is measured from the **row's own `as_of`**;
   - per occupant, an older row never replaces a newer one;
   - a row with a null `quality`, `composite` or `tier` is stored as a neutral entry.

   A `stale: true` batch marks every stored entry stale. Rows are appended to `score_feed_log` with `INSERT OR IGNORE` on `(spec_version, occupant_key, as_of)`, which is idempotent across polls and restarts.
5. At entry time, the hook in `EmaManager._enter` calls `store.decide(netuid, now)`. It sits after every base-strategy gate and just before `execute_swap` (§2.6). In effective `active` mode it multiplies; in `observe` it only logs and persists the would-be factor. Both branches write the eight `sentinel_*` audit columns on the new `ema_positions` row.

### 2.3 The three-layer fail-safe (kept from the old design)

1. **Default-off** — `SENTINEL_ENABLED=false`: the poller job early-returns, the hook does not read the store, `amount_tao` is byte-identical to today.
2. **Observe-before-act** — `SENTINEL_MODE` defaults to `observe`, and only `active` multiplies. `active` is downgraded to `observe` unless the gate-verdict artifact (§2.7) is present, passing, and for the same `spec_version` as the score being used. Mode is config, not code; the bot reads `app.config.settings` live (hot-reload safe).
3. **Bounded effect** — the factor is clamped to `[SENTINEL_FACTOR_FLOOR, effective CEIL]` = `[0.5, 1.0]`. A `SENTINEL_FACTOR_CEIL` above 1.0 takes effect only with an upsize verdict (§2.7). `./SENTINEL_KILLED` forces `1.0` everywhere instantly. The multiply is the last sizing step (§2.6), so a compromised feed can at most halve an order the base strategy already approved. `./SENTINEL_KILLED` is a file in the **bot's** working directory; it is not a TaoSentinel sentinel, so 02 §B12's closed list is unaffected.

**The load-bearing rule** (verbatim from the old spec, generalised): *missing OR stale OR disabled OR killed OR low-confidence OR null quality/composite/tier (02 §H1) OR unaccepted spec_version → factor = 1.0 (no effect).* `ScoreStore.decide()` is the single choke point where every branch lives.

### 2.4 Score → factor curve (Phase D v1, de-risk-only)

Inputs are the feed row fields; `quality` (the slow Health score) drives the curve because `composite` already embeds the red-flag multipliers and velocity, which the curve handles separately (no double counting).

```
FLOOR = SENTINEL_FACTOR_FLOOR  (0.5)      CEIL   = SENTINEL_FACTOR_CEIL if gate.cached().upsize_ok else 1.0   (§2.7)
Q_LO  = SENTINEL_Q_FLOOR_AT   (30.0)     Q_HI   = SENTINEL_Q_NEUTRAL_AT (70.0)   Q_UP = SENTINEL_Q_CEIL_AT (90.0)
C_MIN = SENTINEL_MIN_CONFIDENCE (0.4)    C_FULL = SENTINEL_FULL_CONFIDENCE (0.7)
w_v   = SENTINEL_VELOCITY_WEIGHT (0.0)   BLOCK  = set(SENTINEL_BLOCK_FLAGS)

score_to_factor(quality, velocity, confidence, red_flags) -> (factor, reason):
  1. if quality is None                            → (1.0,   "no_quality")        # 02 §H1: null row ⇒ neutral
  2. if BLOCK ∩ red_flags ≠ ∅                      → (FLOOR, "block_flag")        # hard, confidence-independent
  3. if confidence is None or confidence < C_MIN   → (1.0,   "low_confidence")   # unknown ≠ bad
  4. q   = quality if (velocity is None or w_v == 0) else (1 − w_v)·quality + w_v·velocity
     u   = clamp((q − Q_LO) / (Q_HI − Q_LO), 0, 1)
     raw = FLOOR + (1.0 − FLOOR) · u                                              # 0.5 at Q≤30 … 1.0 at Q≥70
     g   = clamp((confidence − C_MIN) / (C_FULL − C_MIN), 0, 1)
     f   = 1.0 − (1.0 − raw) · g                                                  # haircut scaled by confidence
  5. Phase E only (CEIL > 1.0): if q > Q_HI and confidence ≥ C_FULL and red_flags == []:
         f = 1.0 + (CEIL − 1.0) · clamp((q − Q_HI) / (Q_UP − Q_HI), 0, 1)
  6. return (clamp(f, FLOOR, CEIL), "curve")
```

Worked values (v1, `CEIL=1.0`): Q=30,c≥0.7 → 0.50 · Q=50,c≥0.7 → 0.75 · Q=70 → 1.00 · Q=30,c=0.55 → 0.75 · Q=90 → 1.00 · any block flag on a non-null row → 0.50 · quality null (with or without a block flag) → 1.00 · c=0.39 → 1.00. `decide()` also maps a null `composite` or `tier` to `(1.0, "no_quality")` before calling the curve (02 §H1). With `CEIL=1.0` step 5 never runs and the function is provably ≤ 1.0 (unit test). `SENTINEL_VELOCITY_WEIGHT` ships at 0.0; velocity is carried in the feed and audit so Phase C can test it, but it earns weight only through its own gate (§11).

### 2.5 Modes and the effective-mode matrix

| `SENTINEL_ENABLED` | `SENTINEL_MODE` | gate verdict (§2.7) | `SENTINEL_KILLED` | poller does work | hook reads store | persisted `sentinel_factor` | multiply |
|---|---|---|---|---|---|---|---|
| false | any | any | any | no (early return) | no | 1.0, reason `disabled`, mode `off` | no |
| true | off | any | any | yes | no | 1.0, reason `disabled`, mode `off` | no |
| true | observe | any | absent | yes | yes | would-be, mode `observe` | no |
| true | observe | any | present | yes | yes | 1.0, reason `killed`, mode `observe` | no |
| true | active | not `ok` (`missing`, `parse_error`, `fail`, `pending`, `not_promotable`, `wrong_factor`, `spec_not_accepted`) | absent | yes | yes | would-be, mode `observe` (downgraded; `/health.sentinel.mode_effective="observe"`, `gate=<detail>`) | no |
| true | active | `ok`, entry `spec_version` == verdict `spec_version` | absent | yes | yes | applied (== would-be), mode `active` | **yes** (skipped when the factor is 1.0) |
| true | active | `ok`, entry of another whitelisted `spec_version` | absent | yes | yes | would-be, mode `observe` (a verdict licenses only its own `spec_version`) | no |
| true | active | `ok` | present | yes | yes | 1.0, reason `killed`, mode `active` | no (factor 1.0) |

`effective_mode(settings, gate, spec_version=None)` is a pure function. It returns `off` when `SENTINEL_ENABLED` is false or `MODE=off`, and `observe` when `MODE=observe`. With `MODE=active` it returns `active` only when `gate.ok` and `spec_version` is `None` or equals `gate.spec_version`; otherwise it downgrades to `observe`. `/health` reports it with `spec_version=None`. `decide()` evaluates it with the entry's `spec_version` and returns the result as `FactorDecision.mode`, which the hook persists. The kill sentinel changes only the factor, never the mode. With `MODE=off` the poller still runs, so `/health.sentinel` shows feed liveness during Phase A bring-up, but the hook never reads the store. With `ENABLED=false` the registered jobs early-return and the hook short-circuits.

### 2.6 Hook placement (bot)

The hook goes in `E:\SN_Trader\SubnetTrader\app\portfolio\ema_manager.py` (repo HEAD 2026-06-29; anchors re-verified 2026-09-23), inside `async def _enter(self, netuid, cur, snap_data)` at **:1986**, which is the only entry path (the cycle calls it at :847). Base sizing ends with the pool-depth clamp (:2024-2040).

**Insert the hook between :2146 and :2148**: after the `if settings.EMA_FRESH_POOL_ON_TRADE:` block (:2096-2146), and before the `ENTER` log (:2148) and `execute_swap` (:2159). `netuid` is the first parameter and in scope. The multiply is exactly `amount_tao = round(amount_tao * applied, 6)`, and it is skipped when `applied == 1.0`. The `amount_tao` persisted on the position row (`open_ema_position`, :2187-2197) is the already-multiplied value, so cost basis and PnL stay self-consistent (old spec rationale, line 264).

**Why not :2040-2042 (the old design's anchor).** Three base-strategy gates read `amount_tao` after :2040:
- the balance guard (:2044-2057), which skips the entry when `available < amount_tao`;
- the just-in-time pool re-cap (:2103-2106), which re-floors at `full_amount * 0.25`;
- the pre-trade slippage reject (:2116-2128).

A haircut applied before these gates lets the sentinel *create* entries the base strategy rejects, because a halved order can pass a balance or slippage check that the full order failed. The re-cap can also lift a halved amount back to `0.25 × full_amount` while the audit row still records the haircut. Multiplying as the last sizing step keeps every base decision identical to the no-sentinel bot, makes `amount_tao ≤ base` exact, and makes `sentinel_factor` exactly the applied ratio.

**Opt-in block** (`SENTINEL_FLAG_BLOCK_ENTRY=true`, honoured only when `decision.mode == "active"`). If `decision.reason == "block_flag"`, the hook:
1. logs `SENTINEL BLOCK SN{netuid}: flags=[…]` with `data={netuid, flags, as_of, spec_version}`;
2. increments `_sentinel_blocked` (surfaced as `/health.sentinel.entries_blocked`);
3. skips the entry with `return None` before `execute_swap`, the only state-changing call.

The read-only chain calls above the hook have already run by then, which is harmless: price drift (:1989), balance (:2046), validator hotkey (:2063), orphan check (:2073) and fresh pool (:2097). A blocked entry writes no `ema_positions` row; the structured log line is its audit. The default is `false`, and Phase D ships with it off.

### 2.7 Gate-verdict artifact (mechanical enforcement of the ladder)

Spec 13's `report --run-all` writes `data/quant/results/gate_verdict_<factor>_<YYYYMMDD_HHMMSS>.json` (13 §3.6, §8.7). The operator copies the passing `gate_verdict_quality_<ts>.json` to the bot at `SENTINEL_GATE_VERDICT_PATH` (default `./data/sentinel_gate_verdict.json`). The bot loads it at `init_services()` and on `POST /api/control/sentinel-reload-gate`, and never reads the file on the trade path. The loader reads these keys from 13 §3.6 and ignores every other key:

```json
{"spec_version": "ts-1.0", "factor": "quality", "generated_at": 1796101964,
 "criteria": {"c1_ic_both_scopes": {"pass": true}, "…": {}, "c7_observe_telemetry": {"pass": true}, "u1_upsize": {"pass": false}},
 "gate_pass_return_route": true, "gate_pass_survival_route": false, "gate_pass": true, "pending": [],
 "upsize_gate_pass": false, "promotable": true}
```

`GateVerdict(ok, upsize_ok, spec_version, factor, detail)` is built as follows; the loader never raises.
- `ok = gate_pass is True and pending == [] and promotable is True and factor == "quality" and spec_version ∈ SENTINEL_ACCEPTED_SPEC_VERSIONS`. `promotable` is 13's own statement that the verdict's factor is a feed series eligible for promotion (13 §2.7: `gate_pass ∧ factor ∈ {quality, composite_smoothed}`); a missing or non-true `promotable` never yields `ok`. The curve's input is `quality` (§2.4), so a verdict on another factor (for example `quality_cg`, which 13 never marks promotable) does not license it. `effective_mode()` additionally requires the verdict's `spec_version` to equal the entry's own feed `spec_version` (§7.4 row 2); any failure keeps the entry in `observe`.
- `detail ∈ {pass, missing, parse_error, fail, pending, not_promotable, wrong_factor, spec_not_accepted}` (checked in that order; `not_promotable` = `gate_pass` true and nothing pending but `promotable` absent/false).
- `upsize_ok = ok and upsize_gate_pass is True`. 13 §3.6 emits `upsize_gate_pass` (mirror of `criteria.u1_upsize.pass`: `gate_pass` ∧ survival route ∧ pooled OOS IC t ≥ 3 on a scope with ≥ 4 folds), so Phase E is unblocked mechanically; it still needs a second verdict with it true (§8.4).

The config validator only range-checks `SENTINEL_FACTOR_CEIL`. `decide()` evaluates the curve's effective ceiling, `SENTINEL_FACTOR_CEIL if gate.cached().upsize_ok else 1.0`. A validator that consulted the verdict would crash the bot at start: `config.py:685` builds `Settings()` at import, before `init_services()` loads the verdict, so the cache would be empty.

**Observe telemetry (13 criterion c7).** The hourly `sentinel_coverage` job writes `SENTINEL_TELEMETRY_PATH` (default `./data/sentinel_observe_telemetry.json`) atomically (tmp file + `os.replace`). The same JSON is produced on demand by the export command `python -m app.sentinel.telemetry export --out PATH [--days 30] [--spec ts-1.0]` (bot repo, read-only on `data/ledger.db`, exit 0 on success, 2 on DB/IO error; `--out -` prints to stdout). This JSON file — never the bot's SQLite DB — is what 13's `--observe-telemetry` takes (13 refuses a SQLite path with exit 2). Schema `sentinel-telemetry-1`:

```json
{"schema": "sentinel-telemetry-1", "spec_version": "ts-1.0", "generated_at": 1793437203,
 "since": "2026-10-01T00:05:12Z", "days": 34, "window_days": 30,
 "n_entries": 58, "entry_coverage": 0.71, "live_signal_coverage": 0.71,
 "entries": [{"entry_id": 4411, "entry_ts": "2026-10-28T13:02:11Z", "netuid": 64,
              "occupant_key": "64:5F…", "as_of": 1793404800, "mode": "observe", "reason": "curve",
              "decided_factor": 0.75, "applied_factor": 1.0, "covered": true}]}
```

- `since` is the earliest accepted `score_feed_log.observed_at`; `days = floor((now − since) / 86400)`.
- `n_entries` and `entry_coverage` come from the §10 B7 query over the trailing `window_days` (30 d) (`null` and 0 when there are no entries); `live_signal_coverage` is the same number under its older name (13 accepts either).
- `entries[]` has one object per `ema_positions` row the B7 query counts (window, `sentinel_mode ∈ {observe, active}`; so `len(entries) == n_entries`), with `entry_id` = `ema_positions.id`, `entry_ts` as stored (ISO), `occupant_key` = `sentinel_occupant_key`, `mode` = `sentinel_mode`, `reason` = `sentinel_reason`: `decided_factor = sentinel_factor`, `applied_factor = sentinel_factor if sentinel_mode == 'active' else 1.0` (§2.8), `covered` = the row counts toward coverage in the B7 query (a fresh, accepted signal was present: `sentinel_reason ∈ {curve, block_flag}`), `as_of` = `sentinel_as_of` (INTEGER, null if none).
- `generated_at` is INTEGER unix s; a `schema` other than `sentinel-telemetry-1` makes 13's c7 `pass: null` (`reason: telemetry_schema_unknown`).

Before Phase C, the operator runs the export (or copies the hourly file) to `~/taosentinel/data/quant/observe_telemetry.json` (13's `QUANT_OBSERVE_TELEMETRY_PATH`) and passes that path as `--observe-telemetry` (13 §2.7 c7). 13's c7 rule (`days ≥ 30`, `entry_coverage > 0.60`, `n_entries ≥ 20`) reads only those three keys; `entries[]` and the rest are echoed/informational.

### 2.8 Decided-vs-applied rule (audit)

`sentinel_factor` always records **what the feature decided** (`decide().factor`). In `observe` it is the would-be factor and nothing was applied. In `active` it equals the applied multiplier by construction (`factor_for == decide().factor` whenever the active short-circuit passes). The reader reconstructs the applied value as `applied = sentinel_factor if sentinel_mode == 'active' else 1.0`. The identity is exact for three reasons:
1. The multiply is the last sizing step (§2.6), so every `active` row has `amount_tao == round(base × sentinel_factor, 6)`.
2. `amount_tao` is assigned as the last statement inside the hook's `try`, so a `hook_error` row (factor 1.0) never carries a multiplied amount.
3. `sentinel_mode` is the per-decision effective mode (`FactorDecision.mode`), not the configured one.

Blocked entries (opt-in) write no row. `sentinel_reason` says *why* (`curve|block_flag|disabled|killed|missing|stale|low_confidence|no_quality|hook_error`). `DEFAULT 1.0 NOT NULL` on `sentinel_factor` is load-bearing: legacy rows read as no-op multipliers with no backfill.

---

## 3. Data contract

### 3.1 Feed payload — spec 14's `GET /api/v1/scores/latest?spec=ts-1.0&include_insufficient=1` (consumed unchanged, 02 §H1)

```json
{
  "as_of": "2026-09-22T12:00:00Z",
  "run_kind": "provisional",
  "spec_version": "ts-1.0",
  "ttl_seconds": 21600,
  "generated_at": "2026-09-22T12:40:03Z",
  "stale": false,
  "regime": "TRENDING",
  "count": 118,
  "scores": [
    {"netuid": 64, "occupant_key": "64:4123456", "name": "Chutes", "status": "active",
     "quality": 84.1, "velocity": 71.0, "composite": 82.4, "composite_raw": 81.9,
     "confidence": 0.91, "tier": "A", "red_flags": [],
     "pillars": {"P1": 0.81, "P2": 0.90, "P3": 0.60, "P4": 0.70, "P5": 0.78, "P6": 0.62},
     "as_of": "2026-09-22T12:00:00Z"},
    {"netuid": 18, "occupant_key": "18:5120033", "name": "…", "status": "active",
     "quality": 41.2, "velocity": null, "composite": 38.0, "composite_raw": 42.2,
     "confidence": 0.52, "tier": "D", "red_flags": ["repo_gone", "silence_45d"],
     "pillars": {"P1": 0.40, "P2": null, "P3": 0.30, "P4": 0.50, "P5": 0.20, "P6": null},
     "as_of": "2026-09-22T00:00:00Z"}
  ]
}
```
The shape follows 14 §3.5, with `as_of` on the slot instant (02 §B6); 14 is authoritative. SN18's row is the older official row that 14 serves because the provisional run skipped that occupant. The bot ignores every key not listed below.

| Field | Wire type | When null or absent | Bot use |
|---|---|---|---|
| `as_of` | ISO-8601 string with offset, or null | null only with `scores: []` (empty DB): nothing stored, poll counts as ok | batch fallback for a row without `as_of`; logged as `batch_as_of` |
| `spec_version` | `^[a-z0-9][a-z0-9._-]{0,63}$` | required when `scores` is non-empty | whitelist check |
| `ttl_seconds` | int 1..86400 (14's `SCORE_FEED_TTL_SEC`) | required when `scores` is non-empty | effective TTL = `min(ttl_seconds, SENTINEL_TTL_MAX_SEC)` |
| `stale` | bool, default false | — | `true` ⇒ every stored entry is marked stale |
| `generated_at` | ISO string, optional | skew check falls back to `computed_at`, then `as_of` | `skew_sec`; future-skew reject |
| `computed_at` | ISO string, optional, batch or row (02 §H1) | ignored | future-skew reject; logged |
| `run_kind` | string, optional (`official`/`provisional`) | — | logged, so Phase C can separate provisional rows |
| `scores` | array, 0..1024 | `[]` ⇒ nothing stored | — |
| `scores[].netuid` | int 0..1023, unique in batch | required | store key |
| `scores[].occupant_key` | `^\d{1,4}:\d{1,12}$`, prefix == `netuid`, unique | required | identity; per-occupant monotonic ingest |
| `scores[].as_of` | ISO string, optional | falls back to batch `as_of` | **the row's freshness clock** |
| `scores[].quality` | float 0..100, or null | null ⇒ entry stored; `decide()` → `1.0, no_quality` | curve input (Health) |
| `scores[].velocity` | float 0..100, or null | curve uses quality only | carried and audited |
| `scores[].composite` | float 0..100, or null (= `composite_smoothed`) | null ⇒ `no_quality` | audited |
| `scores[].confidence` | float 0..1, or null | null ⇒ `low_confidence` | confidence haircut |
| `scores[].tier` | `A`–`F`, or null | null ⇒ `no_quality` | audited |
| `scores[].red_flags` | list of flag-name strings (02 §B6), ≤ 32, each `^[a-z0-9_]{1,64}$` | `[]` | block flags; unknown names are tolerated |

Rules:
- Unknown fields are ignored (`extra="ignore"`).
- Any of the following rejects the **whole batch**, because schema drift means a producer bug; everything goes neutral and an alert fires:
  - a malformed or out-of-range value;
  - a timestamp without an offset;
  - a duplicate `netuid` or `occupant_key`;
  - `red_flags` given as objects.
- A null `quality`, `composite`, `tier` or `confidence` makes only that netuid neutral (02 §H1).
- A semantic change requires a new `spec_version` string, which the bot ignores until it is whitelisted.
- An ETag is optional (02 §H1); the client uses one only if it is present (§2.2).
- 14's `503 {"error": "db_unavailable"}` and every other non-200 status take the failure path (§4).

### 3.2 Bot-side Pydantic models (consumer of 14's response)

`subnettrader/app/sentinel/models.py` depends only on Pydantic v2 and the stdlib, and does no I/O and no settings import (receive-time checks live in `ScoreStore.ingest`). 14 owns the producer's response model; this file is only the bot's tolerant reader of it and is **not** copied into TaoSentinel (02 §H1).

```python
"""Consumer model for TaoSentinel GET /api/v1/scores/latest (producer: spec 14; contract: 02 §H1).
Unknown keys are ignored; a null in a row makes that netuid neutral; anything malformed rejects the batch."""
from __future__ import annotations
import re
from datetime import datetime
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_OCC_RE = re.compile(r"^(\d{1,4}):(\d{1,12})$")
_SPEC_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_FLAG_RE = re.compile(r"^[a-z0-9_]{1,64}$")


def _aware(v: datetime | None) -> datetime | None:
    if v is not None and v.utcoffset() is None:
        raise ValueError("timestamp must be ISO-8601 with an offset (…Z)")
    return v


class ScoreRow(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)
    netuid: int = Field(ge=0, le=1023)
    occupant_key: str = Field(min_length=3, max_length=17)
    as_of: datetime | None = None                 # row slot instant (02 §B6); None -> batch as_of
    computed_at: datetime | None = None           # run instant, if 14 serves it
    quality: float | None = Field(default=None, ge=0.0, le=100.0)
    velocity: float | None = Field(default=None, ge=0.0, le=100.0)
    composite: float | None = Field(default=None, ge=0.0, le=100.0)
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    tier: Literal["A", "B", "C", "D", "F"] | None = None
    red_flags: tuple[str, ...] = Field(default=(), max_length=32)   # names only; objects fail validation

    @field_validator("as_of", "computed_at")
    @classmethod
    def _ts(cls, v: datetime | None) -> datetime | None:
        return _aware(v)

    @field_validator("occupant_key")
    @classmethod
    def _occ(cls, v: str) -> str:
        if not _OCC_RE.match(v):
            raise ValueError(f"occupant_key {v!r} must be '<netuid>:<network_registered_at>'")
        return v

    @field_validator("red_flags")
    @classmethod
    def _flags(cls, v: tuple[str, ...]) -> tuple[str, ...]:
        seen: list[str] = []
        for f in v:
            if not _FLAG_RE.match(f):
                raise ValueError(f"bad red_flag {f!r}")
            if f not in seen:
                seen.append(f)
        return tuple(seen)

    @model_validator(mode="after")
    def _key_matches(self) -> "ScoreRow":
        if int(self.occupant_key.split(":", 1)[0]) != self.netuid:
            raise ValueError("occupant_key prefix != netuid")
        return self

    @property
    def usable(self) -> bool:                     # 02 §H1: null composite/quality/tier -> neutral
        return self.quality is not None and self.composite is not None and self.tier is not None


class FeedPayload(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)
    as_of: datetime | None = None                 # null only for an empty DB
    spec_version: str | None = None
    ttl_seconds: int | None = Field(default=None, ge=1, le=86400)
    stale: bool = False
    generated_at: datetime | None = None          # 14's response build time (skew check)
    computed_at: datetime | None = None
    run_kind: str | None = Field(default=None, max_length=16)
    scores: tuple[ScoreRow, ...] = Field(default=(), min_length=0, max_length=1024)

    @field_validator("as_of", "generated_at", "computed_at")
    @classmethod
    def _ts(cls, v: datetime | None) -> datetime | None:
        return _aware(v)

    @field_validator("spec_version")
    @classmethod
    def _spec(cls, v: str | None) -> str | None:
        if v is not None and not _SPEC_RE.match(v):
            raise ValueError("bad spec_version")
        return v

    @model_validator(mode="after")
    def _batch(self) -> "FeedPayload":
        if self.scores:
            if self.as_of is None or self.spec_version is None or self.ttl_seconds is None:
                raise ValueError("non-empty batch needs as_of, spec_version and ttl_seconds")
            n = [r.netuid for r in self.scores]
            k = [r.occupant_key for r in self.scores]
            if len(set(n)) != len(n):
                raise ValueError("duplicate netuid in batch")
            if len(set(k)) != len(k):
                raise ValueError("duplicate occupant_key in batch")
        return self
```

The bot pins this model with `tests/fixtures/scores_latest.schema.json` (= `FeedPayload.model_json_schema()`). It also validates golden bodies:
- `scores_latest_14_example.json`: 14 §3.5's example, with `as_of` on the slot instant;
- `scores_latest_empty.json`: `{"as_of": null, "count": 0, "scores": [], "stale": true}`;
- a live capture saved in Phase A (§10 T2).

`python -m app.sentinel.models --validate-url "$SENTINEL_URL"` fetches the route once and exits 0 or 1; this is the live contract check.

### 3.3 TaoSentinel tables read / written

None (02 §H1). The bot reads no TaoSentinel table, and TaoSentinel writes nothing on the bot's behalf. 14's `queries.latest_scores` (14 §3.5) is the only producer code, and 02 §B6 fixes the column semantics it serves: `scores.as_of` is the slot instant, `computed_at` is the run instant, and `red_flags` is a sorted array of names. The earlier draft's `build_feed` SQL, `FeedCache`, `feed_push` job and `collector_runs` writes are withdrawn.

**metric_keys produced:** none. **events emitted:** none. Bot telemetry lives in the bot's `score_feed_log`, its logs and its `/health`.

### 3.4 Bot tables written (SubnetTrader `app/storage/models.sql` + `db.py` migration tuple at `db.py:87-114`)

```sql
-- Append-only audit of every feed row received. All times are INTEGER epoch seconds UTC (02 §B1 style).
-- One row per TaoSentinel score row, first receipt wins: UNIQUE(spec_version, occupant_key, as_of) is
-- 02 §B6's scores key, so repeated polls of an unchanged feed add nothing. A batch-level reject (schema,
-- spec_version, future_skew) writes ONE row with netuid/occupant_key NULL and reject_reason set (never deduped).
CREATE TABLE IF NOT EXISTS score_feed_log (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  observed_at    INTEGER NOT NULL,                 -- bot receive time
  batch_as_of    INTEGER,                          -- batch as_of (NULL: empty DB or unparsable body)
  as_of          INTEGER,                          -- row slot instant (row as_of, else batch as_of)
  computed_at    INTEGER,                          -- TaoSentinel run instant, when served (02 §B6)
  generated_at   INTEGER,                          -- 14's response build time, when served
  run_kind       TEXT,                             -- 'official' | 'provisional', when served
  spec_version   TEXT,
  ttl_seconds    INTEGER,
  stale          INTEGER NOT NULL DEFAULT 0,       -- batch stale flag, or row older than TTL at receipt
  accepted       INTEGER NOT NULL,                 -- 1 = written to the live ScoreStore
  reject_reason  TEXT,                             -- schema | spec_version | future_skew | older (NULL when accepted)
  netuid         INTEGER,
  occupant_key   TEXT,
  quality        REAL,
  velocity       REAL,
  composite      REAL,
  confidence     REAL,
  tier           TEXT,
  red_flags      TEXT,                             -- JSON array of flag names
  payload_sha256 TEXT    NOT NULL                  -- sha256 of the raw response body
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_score_feed_log_row ON score_feed_log(spec_version, occupant_key, as_of);
CREATE INDEX IF NOT EXISTS idx_score_feed_log_netuid ON score_feed_log(netuid, as_of);
```

Idempotent `ALTER` lines appended to the tuple at `db.py:87-114` (same try/except idiom):

```python
"ALTER TABLE ema_positions ADD COLUMN sentinel_factor REAL NOT NULL DEFAULT 1.0",      # load-bearing default
"ALTER TABLE ema_positions ADD COLUMN sentinel_quality REAL DEFAULT NULL",
"ALTER TABLE ema_positions ADD COLUMN sentinel_velocity REAL DEFAULT NULL",
"ALTER TABLE ema_positions ADD COLUMN sentinel_as_of INTEGER DEFAULT NULL",             # row slot instant, epoch s (02 §B6)
"ALTER TABLE ema_positions ADD COLUMN sentinel_mode TEXT NOT NULL DEFAULT 'off'",
"ALTER TABLE ema_positions ADD COLUMN sentinel_spec_version TEXT DEFAULT NULL",
"ALTER TABLE ema_positions ADD COLUMN sentinel_occupant_key TEXT DEFAULT NULL",         # master §5 identity
"ALTER TABLE ema_positions ADD COLUMN sentinel_reason TEXT NOT NULL DEFAULT 'disabled'",
```

`open_ema_position(...)` (`db.py:136-180`) gains eight defaulted keyword params (`sentinel_factor=1.0, sentinel_quality=None, sentinel_velocity=None, sentinel_as_of=None, sentinel_mode="off", sentinel_spec_version=None, sentinel_occupant_key=None, sentinel_reason="disabled"`) added to the INSERT column list and tuple. Retention: none (≈ 4 runs × ~130 rows ≈ 520 rows/day, ≈ 190 k rows/year). The legacy `ema_positions.entry_ts` stays ISO text (`utc_iso()`, `db.py:168`), so readers compare it through `julianday()` (§10 B7).

---

## 4. Failure modes (absence-vs-failure adapted for a consumer)

For a consumer the question is *what factor results*, never *what metric value*. Every row below yields **no exception on the trade path**.

| # | Condition | Feed (14) | Bot store | `decide()` → factor, reason | `/health.sentinel` | Alert |
|---|---|---|---|---|---|---|
| 1 | TaoSentinel process down / connection refused / timeout | — | unchanged; entries age out | fresh → curve; after TTL → `1.0, stale` | `consecutive_failures++`, `last_error` | once at `SENTINEL_ALERT_AFTER_FAILURES` (8 = 2 h), once on recovery |
| 2 | Scoring stalled (latest `as_of` old) | `stale=true` once `now − as_of > ttl_seconds` | every stored entry marked stale | `1.0, stale` | `stale_count = n` | TaoSentinel's own `scoring_stalled` (spec 12) |
| 3 | `./SCORING_KILLED` on TaoSentinel | last-good rows, `stale=true` | every stored entry marked stale | `1.0, stale` | `stale_count` | — |
| 4 | `503 {"error":"db_unavailable"}` or any other non-200 | 503/5xx | unchanged | as #1 | failure count, `last_http_status` | as #1 |
| 5 | Empty DB: `200 {as_of: null, count: 0, scores: [], stale: true}` | 200 | nothing new; the `stale` flag marks existing entries stale | `1.0, missing` or `1.0, stale` | `last_batch_rows=0`; poll counted ok | — |
| 6 | Schema drift: out-of-range value, timestamp without offset, duplicate netuid, `red_flags` as objects, non-empty batch without `spec_version`/`ttl_seconds` | 200 | whole batch rejected; log row `accepted=0, reject_reason='schema'` | as #1 | failure count | as #1 |
| 7 | `spec_version` ∉ `SENTINEL_ACCEPTED_SPEC_VERSIONS` | 200 | not stored; log row `accepted=0, reject_reason='spec_version'`; old entries age out | `1.0, stale` after TTL | `spec_version`, `spec_version_accepted=false` | once (latched) |
| 8 | Future skew: `generated_at` (else max `computed_at`, else batch `as_of`), or any row `as_of`, later than `observed_at + SENTINEL_FUTURE_SKEW_SEC` | 200 | whole batch rejected, `reject_reason='future_skew'` | as #1 | failure count, `skew_sec` | as #1 |
| 9 | Clock skew the other way (feed clock behind) | fresh | stored; rows look older ⇒ stale sooner | fail-safe direction (`1.0`) | `skew_sec` logged when \|skew\| > 60 s | — |
| 10 | Row already older than TTL at receipt (e.g. an occupant 14 still serves from an old run) | valid | stored with `stale_at_receipt` (warning logged); not rejected | `1.0, stale` | `stale_count` | — |
| 11 | Netuid absent from the batch (pre-launch, deregistered, or low confidence without `include_insufficient=1`) | — | no entry (a previous entry is **not** wiped; it ages out) | `1.0, missing` | `fresh_count` excludes it | — |
| 12 | `velocity == null` | valid | stored | curve uses quality only | — | — |
| 13 | `confidence < SENTINEL_MIN_CONFIDENCE` (the row is present because of `include_insufficient=1`) | valid row | stored | `1.0, low_confidence` | counted in `low_conf_count` | — |
| 14 | Any flag ∈ `SENTINEL_BLOCK_FLAGS` on a usable row | valid | stored | `FLOOR, block_flag` (the entry is skipped only with `SENTINEL_FLAG_BLOCK_ENTRY` in effective `active`) | `entries_blocked` | — |
| 15 | Occupant changed for a netuid (`occupant_key` differs from stored) | valid | the new occupant replaces the old one regardless of `as_of`; `sentinel_occupant_changed` info log | curve on the new occupant | — | — |
| 16 | Bot restart | — | empty until the prime poll (after `READY=1`, before the first cycle, §8.2) | `1.0, missing` until then | `fresh_count=0` briefly | — |
| 17 | `./SENTINEL_KILLED` present | — | unchanged (ingest continues) | `1.0, killed` | `killed=true` | Telegram on engage/clear via control endpoint |
| 18 | `SENTINEL_ENABLED=false` | — | poller early-returns | `1.0, disabled` (store not read) | `enabled=false` | — |
| 19 | `MODE=active` but the verdict is not `ok`, or licenses another `spec_version` | — | — | would-be logged, nothing applied; mode `observe` | `mode_effective`, `gate=<detail>` | once (latched) |
| 20 | `score_feed_log` write fails (DB error) | — | store already updated (store-first order) | unaffected | `log_failures++` | at 8 |
| 21 | Any exception inside the hook | — | — | `1.0, hook_error`; `amount_tao` untouched (it is assigned last); `logger.error` | `hook_errors++` | at 1 (latched) |
| 22 | Null `quality`, `composite` or `tier` in a row (02 §H1) | valid | stored as a neutral entry that replaces the occupant's older usable row | `1.0, no_quality` | `null_rows` | — |
| 23 | Row `as_of` older than the stored row of the same occupant (TaoSentinel DB restore, 16's runbook) | valid | row ignored; logged `accepted=0, reject_reason='older'` if unseen | the stored entry keeps ageing | `ignored_older++` | — |

---

## 5. Configuration

All keys `UPPER_SNAKE`, pydantic-settings, `extra="ignore"`, read through the live `app.config.settings` attribute (never `from app.config import settings`).

### 5.1 TaoSentinel (`app/config.py`)

This spec owns **no** TaoSentinel key (02 §H1, §F1). The bot depends on these keys, which other specs own:

| Key | Owner | Default | Why the bot cares |
|---|---|---|---|
| `SCORE_FEED_TTL_SEC` | 14 | `21600` | Advertised as `ttl_seconds` and used as the batch `stale` threshold (§11 Q1). |
| `SCORING_SPEC_VERSION` | 14/12 | `ts-1.0` | 14's default `spec`. It is a read-only alias of `SPEC_VERSION` (02 §F2). The bot does not rely on the default because it pins `spec=` in `SENTINEL_URL`. |
| `SCORING_KILL_SWITCH_PATH` | 16 (02 §B12) | `./SCORING_KILLED` | Present ⇒ last-good rows with `stale=true`. |
| `API_HOST`, `API_PORT`, `API_AUTH_TOKEN` | 14 §5 | `127.0.0.1`, `8090`, `""` | Where the bot polls. The feed route is in the open read tier, so the token is irrelevant to it. |

`./KILL_SWITCH` does not affect the feed, because the feed is not a paid collector.

### 5.2 Bot (`SubnetTrader app/config.py`, new `SENTINEL_*` block placed after `VALUE_KILL_SWITCH_PATH` at `config.py:447`)

| Key | Type | Default | Validator / semantics |
|---|---|---|---|
| `SENTINEL_ENABLED` | bool | `False` | Layer 1. Nothing runs when false. |
| `SENTINEL_MODE` | str | `"observe"` | `field_validator` lower-cases; ∈ {off, observe, active} (copy `validate_whale_signal_mode`, `config.py:646-653`). |
| `SENTINEL_URL` | str | `http://127.0.0.1:8090/api/v1/scores/latest?spec=ts-1.0&include_insufficient=1` | Absolute http(s) URL (§2.1). It pins one spec; the whitelist below guards against the producer serving another. |
| `SENTINEL_AUTH_TOKEN` | str | `""` | Sent as `X-Auth-Token` only when non-empty (14's read route is open). |
| `SENTINEL_POLL_INTERVAL_MIN` | int | `15` | 1..360. |
| `SENTINEL_HTTP_TIMEOUT_SEC` | float | `10.0` | 1..60. |
| `SENTINEL_TTL_MAX_SEC` | int | `86400` | Effective TTL = `min(ttl_seconds, this)`. |
| `SENTINEL_FUTURE_SKEW_SEC` | int | `300` | Reject the batch if `generated_at` (else max `computed_at`, else batch `as_of`), or any row `as_of`, is later than `observed_at + this` (02 §B6). |
| `SENTINEL_ACCEPTED_SPEC_VERSIONS` | str | `"ts-1.0"` | Comma-separated whitelist; parsed to a frozenset. |
| `SENTINEL_MIN_CONFIDENCE` | float | `0.4` | 0..1; neutral below (matches master §7.7 `insufficient_data`). |
| `SENTINEL_FULL_CONFIDENCE` | float | `0.7` | > `SENTINEL_MIN_CONFIDENCE`. |
| `SENTINEL_Q_FLOOR_AT` | float | `30.0` | 0..100; tier-D/F boundary. |
| `SENTINEL_Q_NEUTRAL_AT` | float | `70.0` | > `Q_FLOOR_AT`; tier-A boundary. |
| `SENTINEL_Q_CEIL_AT` | float | `90.0` | > `Q_NEUTRAL_AT`; Phase E only. |
| `SENTINEL_VELOCITY_WEIGHT` | float | `0.0` | 0..0.5. |
| `SENTINEL_FACTOR_FLOOR` | float | `0.5` | 0.1..1.0. |
| `SENTINEL_FACTOR_CEIL` | float | `1.0` | Range-check only (1.0..1.5), so the validator never fails on runtime state. The effective ceiling is `1.0` unless the cached verdict has `upsize_ok` (§2.7). |
| `SENTINEL_BLOCK_FLAGS` | str | `"emission_disabled,dereg_imminent,miner_burn_zeroed,owner_extraction,key_compromise"` | Comma-separated; must match `^[a-z0-9_]+$`. |
| `SENTINEL_FLAG_BLOCK_ENTRY` | bool | `False` | Opt-in hard entry block (active mode only). |
| `SENTINEL_KILL_SWITCH_PATH` | str | `./SENTINEL_KILLED` | Layer 3 sentinel. |
| `SENTINEL_GATE_VERDICT_PATH` | str | `./data/sentinel_gate_verdict.json` | A copy of 13's `gate_verdict_quality_<ts>.json` (must carry `promotable: true`). Required for `active` to be effective. |
| `SENTINEL_TELEMETRY_PATH` | str | `./data/sentinel_observe_telemetry.json` | Observe-telemetry file for 13's c7 (§2.7), written hourly. |
| `SENTINEL_STRATEGY_TAGS` | str | `""` | Empty = hook applies to every `EmaManager`; else comma list of `cfg.tag` values. |
| `SENTINEL_ALERT_AFTER_FAILURES` | int | `8` | Latched pager threshold. |

`.env.example` gets the same block with comments after the `VALUE_*` block. Kill/pause semantics: `touch ./SENTINEL_KILLED` (or `POST /api/control/sentinel-kill`) ⇒ every factor `1.0` immediately, ingestion continues; `rm` (or `POST /api/control/sentinel-resume`) clears. The `sentinel_poll` and `sentinel_coverage` jobs are registered **unconditionally** and check `SENTINEL_ENABLED` inside the job. `setup_scheduler()` (`main.py:1369-1578`) otherwise adds jobs only for flags set at start, and a config-API hot-reload (`config_api.py:482-483` rebinds `app.config.settings`) could then neither start nor stop polling. With this rule, setting `SENTINEL_ENABLED` either way via hot-reload takes effect at the next tick for the jobs and at the next entry for the hook.

---

## 6. Cadence, rate limits, cost

| Actor | Call | Cadence | Volume | Bucket | Cost |
|---|---|---|---|---|---|
| Bot poller | 14's `GET /api/v1/scores/latest` | every 15 min + one prime at startup | 96/day, ~50 KB (~130 rows incl. 14's extra keys), loopback | none: `max_instances=1` interval job; `poll_once` also refuses to start within 30 s of the previous poll. The old bot has no `TokenBucket` class. | $0 |
| Bot `sentinel_coverage` | local SQL + telemetry file write | hourly | 24/day | — | $0 |
| Scoring runs (spec 12) | — | 4/day | 4 distinct slot `as_of` per day ⇒ ~520 `score_feed_log` rows/day | — | — |

No paid provider is involved and `provider_spend` is untouched. The feed adds one SQL query per poll to TaoSentinel (14 serves it uncached) and no load on any external API.

---

## 7. Failure modes and fail-safe behaviour

### 7.1 Never raise into the scheduler
- Bot `sentinel_poll` job wrapper: flag early-return, `try/except Exception` swallow + `logger.error(..., data={...})`, latched `send_alert` at threshold and on recovery (port of `main.py:1294-1366` idiom and the `_health_alerted` latch at `main.py:1099-1145`).
- Bot `sentinel_coverage` job: same wrapper. It swallows SQL or file errors and keeps the last telemetry file.
- The bot's read-only sentinel router never touches the trade path. The hook is wrapped in its own `try/except` (row 21).

### 7.2 Last-good semantics
- TaoSentinel: last-good semantics are 14's. Under `SCORING_KILLED` it serves last-good rows with `stale: true`; it never falls back to another `spec_version` (14 §7).
- Bot: entries are never deleted; they become stale by the clock, or all at once when a batch says `stale: true`. Absence from a batch never evicts, so a partial batch cannot wipe another subnet's fresh score. Per occupant, an older row never replaces a newer one. After a TaoSentinel restore, `as_of` may step back (16's restore runbook); the bot ignores those rows and its entries age out. No rehydration from `score_feed_log` on restart (the prime poll replaces it within seconds; re-hydrating a near-TTL score is undesirable — old spec rationale kept).

### 7.3 `/health` fields

Bot (`GET /health`, extends the body at `main.py:2230-2283`; no DB read on this path):
```json
"sentinel": {"enabled": true, "mode": "observe", "mode_effective": "observe", "killed": false,
             "gate": "missing", "fresh_count": 118, "stale_count": 0, "low_conf_count": 9,
             "last_as_of": 1790056800, "last_generated_at": 1790058009, "last_poll_at": 1790058011,
             "last_ok_at": 1790058011, "last_batch_rows": 129, "last_error": null, "last_http_status": 200,
             "consecutive_failures": 0, "spec_version": "ts-1.0", "spec_version_accepted": true,
             "skew_sec": -2, "ceil_effective": 1.0, "null_rows": 0, "ignored_older": 0,
             "entries_blocked": 0, "hook_errors": 0, "log_failures": 0,
             "entry_coverage_30d": null, "entry_count_30d": 0}
```
- `last_as_of` is the newest row slot instant in the store (epoch seconds; the bot's own JSON keeps the bot's epoch convention).
- `entry_coverage_30d` and `entry_count_30d` are filled by the hourly `sentinel_coverage` job (SQL in §10 B7). They are the gate-7 numbers, also written to the telemetry file (§2.7).
- `entries_blocked` and `hook_errors` are summed over managers from `_sentinel_blocked` and `_sentinel_hook_errors`.
- Minimum required keys: `enabled, mode, fresh_count, stale_count, last_as_of, spec_version`.

TaoSentinel adds no `/health` key for the feed (02 §H1). Feed liveness on that side is 12's `scores` status and `scoring_stalled`.

### 7.4 The eight safety properties (adapted)

| # | Property | Enforced where |
|---|---|---|
| 1 | **Fail-safe neutral** — missing/stale/low-confidence/null-row/unaccepted version → `1.0` | `ScoreStore.decide()` is the single choke point. The hook multiplies only after the store returns, only in effective `active`, and only when the factor ≠ 1.0. The TTL is inclusive and per row. A `stale=true` batch marks every entry stale. |
| 2 | **Observe-before-act** — mode is config; `active` needs evidence | `SENTINEL_MODE` defaults to `observe`. `effective_mode()` downgrades `active` without an `ok` `GateVerdict` (13's JSON, factor `quality`) for the entry's own `spec_version`. |
| 3 | **Default off, dormant-safe merge** | `SENTINEL_ENABLED=False`; the jobs early-return and the hook short-circuits. Tests assert byte-identical `amount_tao` (disabled, observe, and active at factor 1.0). |
| 4 | **Deploy-gate safe** — stdlib + FastAPI + Pydantic + httpx (already bot dependencies); no module-top I/O; `compileall` + `import app.main` green; `/health` stays `status: ok`. | `app/sentinel/*` imports nothing heavy. `GateVerdict` is loaded in `init_services()`, not at import. No `Settings` validator depends on runtime state, so a bad `SENTINEL_FACTOR_CEIL` can never stop the bot from importing. |
| 5 | **Kill switch** — `1.0` everywhere, no redeploy | `./SENTINEL_KILLED` is checked in `decide()` right after the disabled/off short-circuit. Control via `POST /api/control/sentinel-kill` and `sentinel-resume`. `is_killed`/`engage_kill`/`clear_kill` are ported from `value_kill.py:178-199`. |
| 6 | **Private transport + whole-batch validation** | Loopback binds on both sides. 14's read route is open by design (master §9). The bot's `require_auth` (`api_auth.py:21-36`) guards its sentinel router and control routes. `FeedPayload` validates the whole batch before any write; skew reject; version whitelist. |
| 7 | **Bounded effect** | Final `clamp(f, FLOOR, CEIL)` with effective `CEIL = 1.0` unless an upsize verdict exists. The multiply is the last sizing step, after the impact cap (`ema_manager.py:2028`), the balance guard (:2044), the JIT re-cap (:2105) and the slippage reject (:2116), so the sentinel only shrinks entries the base strategy approved. |
| 8 | **Restart-invariant audit** | Eight `ema_positions` columns with `DEFAULT 1.0 NOT NULL` on the factor; `score_feed_log` append-only with `INSERT OR IGNORE`; ledger is gitignored so deploy rollback never clobbers it. |

---

## 8. Implementation plan

### 8.1 TaoSentinel repo (`~/taosentinel`)

Nothing (02 §H1). The producer is 14's `app/api/scores.py` + `queries.latest_scores` (14 §3.5, §8.1), tested by 14's `test_scores_latest_*`. This spec adds no file, job, key or test to TaoSentinel. §10 T1–T3 are consumer-side checks against the live route once 14 ships (Phase 11).

### 8.2 Bot repo (`E:\SN_Trader\SubnetTrader`, revived as `~/subnettrader`)

| File | Contents |
|---|---|
| `app/sentinel/__init__.py` | empty. |
| `app/sentinel/models.py` | §3.2 verbatim (the bot's consumer model; TaoSentinel has no copy), plus `python -m app.sentinel.models --validate-url URL`. |
| `app/sentinel/kill.py` | `is_killed(path) -> bool`, `clear_kill(path) -> bool` **verbatim** from `value_kill.py:178-180, 193-199`; `engage_kill(path: str, reason: str = "operator") -> None` writes `{"engaged_at": utc_iso(), "reason": reason}` (single-arg variant, no `KillDecision`). |
| `app/sentinel/factor.py` | `CurveConfig` dataclass (built per decision, with `ceil` = the effective ceiling) + `score_to_factor(quality, velocity, confidence, red_flags, cfg) -> tuple[float, str]` (§2.4) + `clamp`. Pure. |
| `app/sentinel/gate.py` | `GateVerdict`, `load_gate_verdict(path, accepted) -> GateVerdict` (13 §3.6 shape, §2.7), `cached() -> GateVerdict` (module-level; default `ok=False, detail="missing"`), `effective_mode(settings, gate, spec_version=None) -> str`. Imports nothing from `app.config`. |
| `app/sentinel/store.py` | `ScoreEntry`, `FactorDecision`, `IngestResult`, `IngestRejected`, `ScoreStore`. |
| `app/sentinel/client.py` | `SentinelClient` poller. |
| `app/sentinel/telemetry.py` | `coverage(db, now) -> dict` (the §10 B7 query), `entries(db, now, days) -> list[dict]` (per-entry decided/applied factors), `build_telemetry(db, now, days, spec) -> dict` and `write_telemetry(path, store, db, now)` (§2.7 `sentinel-telemetry-1` JSON, atomic); `__main__`-style `export --out PATH [--days 30] [--spec ts-1.0]` CLI (`python -m app.sentinel.telemetry export`) opening `data/ledger.db` read-only. |
| `app/sentinel/api.py` | `router = APIRouter(prefix="/api/sentinel", tags=["sentinel"])` with read-only `GET ""` and `GET "/{netuid}"`; there is no ingest route (02 §H1: pull only). Uses the `configure(store_getter, client_getter)` closure idiom (`metrics_api.py:21-27`). |
| `app/storage/models.sql`, `app/storage/db.py` | §3.4 DDL, ALTER tuple, `open_ema_position` params, `log_feed_rows(rows: list[dict]) -> int` (`executemany INSERT OR IGNORE`). |
| `app/portfolio/ema_manager.py` | Imports `import time`, `import app.config as app_config` (the file only has the captured `from app.config import StrategyConfig, settings` at :12, which a hot-reload does not update). In `__init__` after `self._fundamentals` (:158): `self._sentinel_store: typing.Any \| None = None`, `self._sentinel_blocked = 0`, `self._sentinel_hook_errors = 0`, plus a `_sentinel_applies()` method. The hook goes between :2146 and :2148 (§2.6). The eight locals are threaded into `open_ema_position` at :2187-2197. |
| `app/main.py` | `init_services()` (190-356; managers built at 243-263): construct `ScoreStore` and `SentinelClient`, call `load_gate_verdict`, and assign `mgr._sentinel_store` to every constructed manager. `setup_scheduler()` (1369-1578): register `sentinel_poll` (interval `SENTINEL_POLL_INTERVAL_MIN`) and `sentinel_coverage` (hourly) **unconditionally** (§5.2), both with `max_instances=1`. Prime poll: `await asyncio.wait_for(sentinel_client.poll_once(), SENTINEL_HTTP_TIMEOUT_SEC + 2)` inside `try`, when `SENTINEL_ENABLED`, placed after `sd_notify("READY=1")` and the watchdog task (:3421-3422) and **before** the first `run_ema_cycle()` (:3450-3451), so the first cycle after a restart sees scores. Also: the `/health` block (body at 2230-2283); the router mount `app.include_router(sentinel_router, dependencies=[Depends(require_auth)])` beside 2185/2189; and the control routes `POST /api/control/sentinel-kill`, `sentinel-resume` and `sentinel-reload-gate` beside `value-resume` (2492), each with `dependencies=[Depends(require_auth)]`. |
| `app/config.py` | §5.2 block + validators (range checks only; `SENTINEL_MODE` lower-cased). `.env.example` block. |

```python
# app/sentinel/store.py
@dataclasses.dataclass(frozen=True)
class ScoreEntry:
    netuid: int; occupant_key: str
    quality: float | None; velocity: float | None; composite: float | None
    confidence: float | None; tier: str | None; red_flags: tuple[str, ...]
    as_of: int              # row slot instant, epoch s (row as_of, else batch as_of)
    computed_at: int | None; ttl_seconds: int; spec_version: str; stale: bool; observed_at: int

@dataclasses.dataclass(frozen=True)
class FactorDecision:
    factor: float          # clamped would-be factor
    reason: str            # curve|block_flag|disabled|killed|missing|stale|low_confidence|no_quality
    entry: ScoreEntry | None
    mode: str              # effective mode for THIS decision: off|observe|active (§2.5)

@dataclasses.dataclass(frozen=True)
class IngestResult:
    accepted: bool; stored: int; reason: str | None; batch_as_of: int | None; spec_version: str | None
    stale_rows: int; null_rows: int; ignored_older: int; skew_sec: int | None

class IngestRejected(ValueError): ...   # future_skew → failure path

class ScoreStore:
    def __init__(self, settings_getter=lambda: app_config.settings) -> None: self._rows: dict[int, ScoreEntry] = {}
    def ingest(self, batch: FeedPayload, *, observed_at: float) -> IngestResult:
        """1. ref = generated_at or max(row/batch computed_at) or batch as_of; raise IngestRejected('future_skew')
              if ref, or any row as_of, > observed_at + SENTINEL_FUTURE_SKEW_SEC.
           2. empty scores -> nothing stored (accepted=True, stored=0); spec_version not whitelisted ->
              accepted=False, reason='spec_version', nothing written.
           3. batch.stale -> mark EVERY stored entry stale (replace with stale=True copies).
           4. per row (as_of = row.as_of or batch.as_of, epoch int): stored = self._rows.get(netuid);
              replace iff stored is None or stored.occupant_key != row.occupant_key (log
              sentinel_occupant_changed) or row as_of >= stored.as_of; else ignored_older += 1.
              Null quality/composite/tier rows are stored (decide() -> no_quality). stale flag = batch.stale;
              warn when observed_at - as_of > effective ttl (stale_at_receipt)."""
    def peek(self, netuid: int) -> ScoreEntry | None
    def keys(self) -> list[int]
    def effective_ttl(self, e: ScoreEntry) -> int              # min(e.ttl_seconds, SENTINEL_TTL_MAX_SEC)
    def is_fresh(self, e: ScoreEntry, now: float) -> bool      # (not e.stale) and (now - e.as_of <= effective_ttl)  ← inclusive, per row
    def counts(self, now: float) -> tuple[int, int, int]       # fresh, stale, low_confidence
    def decide(self, netuid: int, now: float) -> FactorDecision
    def would_be_factor(self, netuid: int, now: float) -> float   # decide(...).factor
    def factor_for(self, netuid: int, now: float) -> float
        """APPLIED factor: 1.0 unless decide().mode == 'active'; then == decide().factor."""
```

`decide()` order (with `s` = live settings and `g = gate.cached()`):
1. `not s.SENTINEL_ENABLED` or `effective_mode(s, g) == "off"` → `(1.0, disabled, None, "off")`, without touching the store or the file system.
2. `is_killed(path)` → `(1.0, killed, peek, mode)`.
3. `peek is None` → `(1.0, missing, None, mode)`.
4. `not is_fresh(e)` → `(1.0, stale, e, mode)`.
5. `e.quality`, `e.composite` or `e.tier` is None → `(1.0, no_quality, e, mode)`.
6. Otherwise `score_to_factor(..., cfg(ceil = s.SENTINEL_FACTOR_CEIL if g.upsize_ok else 1.0))` → `(clamp(f), reason, e, mode)`.

In steps 2–6, `mode = effective_mode(s, g, e.spec_version if e else None)`.

```python
# app/sentinel/client.py
class SentinelClient:
    def __init__(self, store: ScoreStore, db: Database | None,
                 settings_getter=lambda: app_config.settings, http: httpx.AsyncClient | None = None) -> None: ...
    status: dict   # keys of /health.sentinel minus the store counts
    async def poll_once(self, *, now: float | None = None) -> IngestResult | None:
        """Never raises. Returns None without a request if the previous poll started < 30 s ago.
        headers = {X-Auth-Token if SENTINEL_AUTH_TOKEN, If-None-Match only if the last 200 carried an ETag}.
        304 → ok, None. 200 → FeedPayload.model_validate_json → store.ingest → db.log_feed_rows
        (store-first; a log failure is counted, not raised) → status update (last_batch_rows, skew_sec, …).
        401/403/404/5xx/timeout/ValidationError/IngestRejected → failure path (consecutive_failures, last_error,
        latched alert; batch-level log row with reject_reason schema|future_skew).
        spec_version not accepted → log row accepted=0, status.spec_version_accepted=False, latched alert, NOT a failure."""
    async def close(self) -> None
```

Hook (insert between `ema_manager.py:2146` and `:2148`, §2.6):

```python
        # --- TaoSentinel size modulation (TaoData specs/15-trading-bot-integration.md §2.6) ---
        # Last sizing step: every base gate (balance :2044, JIT re-cap :2103, slippage reject :2116) has
        # already passed on the base amount, so this can only shrink an approved entry.
        # Fail-safe neutral: None store / disabled / missing / stale / killed / low-confidence / null row / error → 1.0.
        # observe = decide+log+persist, amount_tao untouched; active = multiply. amount_tao is assigned LAST (§2.8).
        sentinel_factor, sentinel_reason, sentinel_mode = 1.0, "disabled", "off"
        sentinel_quality = sentinel_velocity = sentinel_as_of = None
        sentinel_spec_version = sentinel_occupant_key = None
        try:
            s = app_config.settings
            store = self._sentinel_store
            if store is not None and s.SENTINEL_ENABLED and self._sentinel_applies():
                d = store.decide(netuid, time.time())      # mode is per decision (gate + spec binding)
                sentinel_mode = d.mode
                if d.mode != "off":
                    sentinel_factor, sentinel_reason = d.factor, d.reason
                    if d.entry is not None:
                        sentinel_quality, sentinel_velocity = d.entry.quality, d.entry.velocity
                        sentinel_as_of, sentinel_spec_version = d.entry.as_of, d.entry.spec_version
                        sentinel_occupant_key = d.entry.occupant_key
                    if d.mode == "active":
                        if s.SENTINEL_FLAG_BLOCK_ENTRY and d.reason == "block_flag":
                            self._sentinel_blocked += 1
                            logger.warning(f"EMA[{self._cfg.tag}] SENTINEL BLOCK SN{netuid}: flags={list(d.entry.red_flags)}",
                                           data={"netuid": netuid, "flags": list(d.entry.red_flags),
                                                 "as_of": sentinel_as_of, "spec_version": sentinel_spec_version})
                            return None
                        new_amount = round(amount_tao * d.factor, 6) if d.factor != 1.0 else amount_tao
                        logger.info(f"EMA[{self._cfg.tag}] SENTINEL APPLIED SN{netuid}: x{d.factor:.3f} → {new_amount:.4f} τ ({d.reason})")
                        amount_tao = new_amount                   # last statement: nothing after it can raise
                    else:
                        logger.info(f"EMA[{self._cfg.tag}] SENTINEL OBSERVE SN{netuid}: would x{d.factor:.3f} "
                                    f"(q={sentinel_quality}, v={sentinel_velocity}, as_of={sentinel_as_of}, reason={d.reason})")
        except Exception as exc:  # the hook must never fail an entry; amount_tao is still the base amount here
            sentinel_factor, sentinel_reason = 1.0, "hook_error"
            self._sentinel_hook_errors += 1
            logger.error(f"EMA[{self._cfg.tag}] SENTINEL HOOK ERROR SN{netuid}: {exc}")
```

`_sentinel_applies()` returns `True` when `SENTINEL_STRATEGY_TAGS` is empty or contains `self._cfg.tag` (tags: `meanrev`, `trend`, `flow`, `value`; `config.py:193, 259, 355, 412`). The eight locals are always defined, so the `open_ema_position(...)` call about 40 lines later is always safe.

### 8.3 Step order

1. **TaoSentinel**: nothing to build (02 §H1). Once 14's route is live (Phase 11), run §10 T1–T3 and save a live capture as a bot fixture.
2. **Bot, Phase A (dormant)**: models + golden fixtures → `kill.py` → `factor.py` + tests → `gate.py` → `store.py` + tests → DDL + migration + `open_ema_position` → `client.py` (mock transport) → `telemetry.py` → `api.py` + control routes → hook + byte-identical and placement tests → `main.py` wiring → `/health.sentinel`. Verify §10 B1–B4 with `SENTINEL_ENABLED=false`.
3. **Phase B onward** per §8.4.

### 8.4 Rollout ladder A → E

| Phase | Config state (bot) | Entry criterion | Verify | Exit criterion |
|---|---|---|---|---|
| **A dormant** | `SENTINEL_ENABLED=false` | bot code merged (nothing in TaoSentinel) | `compileall app` + `python -c 'import app.main'` green; `test_sentinel_sizing_hook::test_byte_identical_when_disabled`; `/health.sentinel.enabled=false`; §10 T1–T3 against 14's live route (when Phase 11 is live) | all §9 tests green on the laptop |
| **B observe** | `ENABLED=true`, `MODE=observe` | A done; TaoSentinel Phase 8 (scores) and Phase 11 (14's route) live | log lines `SENTINEL OBSERVE SN…`; `ema_positions` rows carry `sentinel_*` with `sentinel_mode='observe'`; `/health.sentinel.fresh_count > 0`; survives `systemctl restart subnettrader` | telemetry `days ≥ 30`, `live_signal_coverage > 0.60` with `n_entries ≥ 20` (§2.7, §10 B7), and no open sentinel failure alert. With fewer entries, extend B. |
| **C gate** | unchanged | B exit, and ≥ 90 d of forward collection for the full composite (master §10) | on the bot: `python -m app.sentinel.telemetry export --out ./data/sentinel_observe_telemetry.json` (§2.7, schema `sentinel-telemetry-1`), then copy that JSON to `~/taosentinel/data/quant/observe_telemetry.json`; on TaoSentinel take a snapshot and run `python -m app.quant report --run-all --factor quality --horizon-days 7 --observe-telemetry data/quant/observe_telemetry.json` (13 §8.7; horizon pre-registered as `QUANT_GATE_HORIZON_DAYS`; the flag takes the JSON export, never `data/ledger.db` — 13 exits 2 on a SQLite path), which writes `data/quant/results/gate_verdict_quality_<ts>.json` with criteria c1–c7 + `u1_upsize` (13 §3.6) | `gate_pass: true`, `pending: []` and `promotable: true`; otherwise **stay in B indefinitely at zero cost** (a valid outcome) |
| **D active (de-risk only)** | `MODE=active`, `FACTOR_CEIL=1.0`, verdict copied to `SENTINEL_GATE_VERDICT_PATH` | C pass | `/health.sentinel.mode_effective="active"`, `gate="pass"`; every new entry's `amount_tao ∈ [0.5, 1.0] × base`; `touch ./SENTINEL_KILLED` ⇒ next entry logs `x1.000 … (killed)`; TTL expiry ⇒ `x1.000 … (stale)` | 30 days without a kill-worthy incident; realised haircut telemetry reviewed |
| **E upsizing (optional)** | `FACTOR_CEIL=1.25`, `Q_CEIL_AT=90` | D + a **second** verdict with `upsize_gate_pass: true`, proposed as pooled OOS IC t ≥ 3 (walk-forward, ≥ 4 folds) **and** a survival-route pass (bottom-tercile 90-d death rate ≥ 2× top, n ≥ 10) — 13 §3.6's `criteria.u1_upsize` / `upsize_gate_pass`, on a verdict that is also `ok` (incl. `promotable`). | `/health.sentinel.ceil_effective == 1.25` only with that verdict; a test asserts `factor ≤ 1.25`, with upsizing only when `confidence ≥ 0.7` and `red_flags == []` | — |

Emergency fallback at any phase: `touch ./SENTINEL_KILLED` (instant) or `SENTINEL_ENABLED=false` (hot reload). Phase labels follow the parent spec (A dormant / B observe / C backtest / D active), not phase-a's drifted relabel.

---

## 9. Tests

All offline; `now` is injected; settings via `monkeypatch.setattr(app.config, "settings", Settings(...))`; kill file under `tmp_path`; HTTP via `httpx.MockTransport`; routes via `fastapi.testclient.TestClient`.

### 9.1 TaoSentinel (`tests/`)
None (02 §H1). 14's `test_scores_latest_contract`, `test_scores_latest_filters`, `test_scores_latest_killed` and `test_scores_latest_empty_db` cover the producer. The bot's golden fixtures (§3.2) are copied from 14's example and a live capture.

### 9.2 Bot (`tests/`)
| Test file | Cases |
|---|---|
| `test_sentinel_models.py` | Schema pin. Accepts 14's example body (ISO `as_of`; extra keys `name`, `status`, `pillars`, `composite_raw`, `run_kind`, `regime`, `count`), the empty-DB body `{as_of: null, count: 0, scores: [], stale: true}`, rows with null `quality`/`composite`/`tier`/`confidence` (`usable == False`), `velocity: null`, and an unknown field `foo`. Rejects: netuid 1024; `occupant_key` prefix mismatch; duplicate netuid; duplicate occupant_key; quality 100.1; confidence 1.1; tier `"E"`; flag `"Bad Flag"`; `red_flags` as objects `[{"flag": …}]`; an `as_of` without offset; a non-empty batch without `spec_version` or `ttl_seconds`; `ttl_seconds` 0 or 86401. |
| `test_sentinel_factor.py` | Curve table (§2.4 worked values, `pytest.approx`). `factor ≤ 1.0` for 10,000 random inputs when `CEIL=1.0`. Block flag ⇒ `FLOOR` regardless of confidence. `quality=None` + block flag ⇒ `1.0, no_quality`. `confidence < 0.4` ⇒ `1.0`. `w_v=0.3, velocity=None` ⇒ quality only. Phase E: `ceil=1.25`, Q=90, c=0.9, no flags ⇒ 1.25; Q=90 with a non-block flag ⇒ 1.0. Monotone in Q. |
| `test_sentinel_store.py` | **TTL boundary (per row)**: row `as_of=1000, ttl=21600`; `now=22600` ⇒ fresh (inclusive), `now=22601` ⇒ `stale`. A row whose own `as_of` is older than the batch `as_of` goes stale on its own clock. `SENTINEL_TTL_MAX_SEC=3600` caps a 21600 ttl. **Skew**: `generated_at = observed_at+301` ⇒ `IngestRejected`, `+300` accepted; a row `as_of = observed_at+301` ⇒ rejected; no `generated_at` ⇒ falls back to `computed_at`, then `as_of`. Stale at receipt: accepted with a warning. **Stale batch**: `stale=true` ⇒ every stored entry is `stale`, including netuids absent from the batch; the next `stale=false` batch with the same rows restores them. **spec_version** `ts-2.0` ⇒ `accepted=False`, store unchanged. **Monotonic**: an older row for the same occupant is ignored (`ignored_older=1`); an equal `as_of` overwrites; a different `occupant_key` replaces regardless of `as_of` and logs `sentinel_occupant_changed`; a partial batch leaves other netuids untouched. **Null row** replaces the older usable row ⇒ `1.0, no_quality`. **Empty batch** ⇒ nothing stored, accepted. `counts()`. **Kill**: file present ⇒ `1.0, killed` in observe and active; `clear_kill` restores. **Mode matrix**: every §2.5 row via `decide().mode` and `factor_for`, including the verdict for `ts-1.0` + entry `ts-1.1` ⇒ mode `observe`. **Ceiling**: `SENTINEL_FACTOR_CEIL=1.25` without `upsize_ok` ⇒ every factor ≤ 1.0. **Disabled** never touches the file system (`is_killed` not called). |
| `test_sentinel_kill.py` | `is_killed`; `engage_kill` writes `{engaged_at, reason}`; `clear_kill` returns True then False (mirrors the `value_kill` sentinel test). |
| `test_sentinel_gate.py` | Missing file ⇒ `ok=False, detail="missing"`. Bad JSON ⇒ `parse_error`. 13 §3.6 verdict with `gate_pass: true`, `pending: []`, factor `quality`, whitelisted version ⇒ `ok`. `pending: ["c7"]` ⇒ `pending`. Factor `quality_cg` with `gate_pass: true` ⇒ `not_promotable` (13 never marks it promotable); a hand-built `quality_cg` with `promotable: true` ⇒ `wrong_factor`. `promotable` false or missing on an otherwise passing `quality` verdict ⇒ `not_promotable`, `ok=False`, `effective_mode` = `observe`. Version not whitelisted ⇒ `spec_not_accepted`; verdict for `ts-1.0` with an entry on another `spec_version` ⇒ observe. `gate_pass: false` ⇒ `fail`. No `upsize_gate_pass` key ⇒ `upsize_ok=False`; `upsize_gate_pass: true` on an `ok` verdict ⇒ `upsize_ok=True`. `effective_mode` table. `Settings(SENTINEL_FACTOR_CEIL=1.25)` constructs without error (no runtime-state validator). |
| `test_sentinel_client.py` | MockTransport. 200 ⇒ ingest + `score_feed_log` rows; a second identical poll adds 0 rows (`INSERT OR IGNORE` on `(spec_version, occupant_key, as_of)`). 200 with ETag ⇒ the next request sends `If-None-Match`; 304 ⇒ no ingest, ok. Without an ETag no `If-None-Match` is sent. 401/503/timeout/invalid JSON/future skew ⇒ failure path, one batch-level log row, latched alert at `SENTINEL_ALERT_AFTER_FAILURES` and once on recovery. `X-Auth-Token` present iff the token is set. The request URL carries `spec=ts-1.0&include_insufficient=1`. A second poll within 30 s makes no request. A spec_version reject is logged with `accepted=0` and is not a failure. |
| `test_sentinel_api.py` | `GET /api/sentinel` returns 401 without a token when `API_AUTH_TOKEN` is set; with the token it returns rows with `fresh`, `expires_at`, `would_apply_factor`, `reason`. `POST /api/sentinel/scores` does not exist (404/405). `sentinel-kill`, `sentinel-resume` and `sentinel-reload-gate` are authed. |
| `test_sentinel_sizing_hook.py` | Construct `EmaManager` with a stub executor.<br>**Byte-identical `amount_tao` (`==`)**: `SENTINEL_ENABLED=false` (the store is a MagicMock asserting zero calls); `store=None`; `observe`; and `active` when the factor is 1.0 (killed, stale, missing, low_confidence).<br>**Observe** persists the would-be factor and `sentinel_mode='observe'`.<br>**Active + verdict**: `amount_tao == round(base × f, 6)`, within `[0.5, 1.0] × base`, `sentinel_factor == f`, `sentinel_mode='active'`, and `execute_swap` receives the multiplied amount. Active without a verdict behaves as observe.<br>**Placement**: a balance just below the *base* amount ⇒ entry skipped even at f = 0.5 (the sentinel never creates an entry); a slippage estimate over the limit at base ⇒ skipped; a JIT re-cap then f = 0.5 ⇒ final `== round(recapped × 0.5, 6)`.<br>**Errors**: store raising ⇒ `hook_error`, amount untouched; a logger raising after the factor is computed ⇒ amount untouched and `sentinel_factor == 1.0`.<br>**Other**: `SENTINEL_FLAG_BLOCK_ENTRY` in active ⇒ `_enter` returns `None` and `execute_swap` is never called; `SENTINEL_STRATEGY_TAGS="flow"` ⇒ trend manager untouched; columns survive a simulated restart (re-open DB, `SELECT sentinel_*`). |
| `test_sentinel_telemetry.py` | Fixture `ema_positions` with ISO `entry_ts` (`utc_iso()` format, including rows 29.9 d and 30.1 d old) ⇒ the B7 query counts exactly the rows inside 30 d; the file has `schema`, `since`, `days`, `n_entries`, `entry_coverage` (= `live_signal_coverage`) and `entries[]` with `len == n_entries`, `applied_factor == decided_factor` for `active` rows and `1.0` for `observe` rows; the write is atomic; coverage is null when `n_entries == 0`; `python -m app.sentinel.telemetry export --out -` prints the same JSON and exits 0, and exits 2 when the DB is missing. |
| `test_sentinel_db.py` | Migration is idempotent (run `_apply_schema` twice); `sentinel_factor` is NOT NULL DEFAULT 1.0 on a legacy INSERT; the `score_feed_log` unique index dedupes rows but not batch-level (NULL-key) rows. |

Smoke: `python -m compileall app && python -c "import app.main"` and `pytest -q` in the bot repo.

---

## 10. Verify checklist

**TaoSentinel (14's route, checked from the bot's side)**
- T1 `curl -s 'http://127.0.0.1:8090/api/v1/scores/latest?spec=ts-1.0&include_insufficient=1' | jq '{spec_version, as_of, generated_at, ttl_seconds, stale, n:(.scores|length), rf:(.scores[0].red_flags|type)}'` → `spec_version` `"ts-1.0"`; `as_of` an ISO string ending `T00:00:00Z`, `T06:00:00Z`, `T12:00:00Z` or `T18:00:00Z` (the slot instant, 02 §B6); `ttl_seconds` 21600; `stale` false; `n` ≥ 100; `rf` `"array"` of strings.
- T2 In the bot repo: `python -m app.sentinel.models --validate-url "$SENTINEL_URL"` → exit 0. Save the body as `tests/fixtures/scores_latest_live.json`.
- T3 `touch ~/taosentinel/SCORING_KILLED; curl -s '…/latest?spec=ts-1.0' | jq .stale` → `true`; after `rm` → `false`.

**Bot (Phase A, `SENTINEL_ENABLED=false`)**
- B1 `python -m compileall app && python -c "import app.main"` → exit 0.
- B2 `pytest tests/test_sentinel_*.py -q` → all pass; `test_byte_identical_when_disabled` present.
- B3 `curl -s http://127.0.0.1:8081/health | jq .sentinel` → `{"enabled":false,"mode":"observe","mode_effective":"off",...}`.
- B4 `sqlite3 data/ledger.db "PRAGMA table_info(ema_positions)" | grep -c sentinel_` → `8`; `sqlite3 data/ledger.db ".schema score_feed_log"` shows the table.

**Bot (Phase B, `SENTINEL_ENABLED=true`, `SENTINEL_MODE=observe`)**
- B5 within 1 min of restart: `curl -s -H "X-Auth-Token: $API_AUTH_TOKEN" http://127.0.0.1:8081/api/sentinel | jq '.scores | map(select(.fresh)) | length'` → ≥ 100; `/health.sentinel.fresh_count` matches.
- B6 `sqlite3 data/ledger.db "SELECT COUNT(*), MAX(as_of) FROM score_feed_log WHERE accepted=1"` grows by about 130 rows per new slot `as_of` (4 per day), not per poll.
- B7 coverage (gate item 7; `entry_ts` is ISO text, so compare with `julianday`, not string `>=`):
  ```sql
  SELECT ROUND(AVG(sentinel_reason IN ('curve','block_flag')),3) AS live_signal_coverage, COUNT(*) AS n_entries
  FROM ema_positions
  WHERE julianday(entry_ts) >= julianday('now','-30 days') AND sentinel_mode IN ('observe','active');
  ```
  → `live_signal_coverage > 0.60` with `n_entries ≥ 20`. The same numbers appear at `/health.sentinel.entry_coverage_30d` / `entry_count_30d` and in `data/sentinel_observe_telemetry.json` (`jq 'del(.entries)'` shows `schema, spec_version, generated_at, since, days, window_days, n_entries, entry_coverage, live_signal_coverage`; `jq '.entries | length'` equals `n_entries`).
- B8 `grep 'SENTINEL OBSERVE' data/logs/*.jsonl | tail -3` shows `would x0.xxx (q=…, reason=curve)`; `touch ./SENTINEL_KILLED` then next entry logs `would x1.000 … reason=killed`; `rm` restores.
- B9 `systemctl stop taosentinel` for longer than `ttl_seconds` (or, in a test env, set 14's `SCORE_FEED_TTL_SEC=60`) → `/health.sentinel.stale_count == fresh_count_before`, `consecutive_failures` climbs, one Telegram alert at 8 and one on recovery. The bot keeps trading at factor 1.0.

**Bot (Phase D)**
- D1 `cp ~/taosentinel/data/quant/results/gate_verdict_quality_<ts>.json ./data/sentinel_gate_verdict.json; curl -X POST -H "X-Auth-Token: $API_AUTH_TOKEN" http://127.0.0.1:8081/api/control/sentinel-reload-gate` → `/health.sentinel.gate == "pass"`, `mode_effective == "active"`, `ceil_effective == 1.0`.
- D2 `sqlite3 data/ledger.db "SELECT netuid, amount_tao, sentinel_factor, sentinel_mode, sentinel_reason FROM ema_positions ORDER BY id DESC LIMIT 5"` → every `sentinel_mode='active'` row has `0.5 ≤ sentinel_factor ≤ 1.0`.
- D3 `touch ./SENTINEL_KILLED` → next entry `SENTINEL APPLIED … x1.000 (killed)` with **no restart**.

---

## 11. Open questions / operator decisions

1. **TTL vs cadence (14's key).** `ttl_seconds` is 14's `SCORE_FEED_TTL_SEC` = 21600, measured from the **slot instant** (02 §B6). The next slot's run starts at hh:15Z and may take up to its 1 200 s timeout, and the bot polls every 15 min. So every row goes stale at hh:00Z, and fresh rows reach the bot only at about hh:30–hh:50Z. That leaves the bot neutral for about 30–50 min four times a day (≈ 10 % of entries; harmless but it dents gate-7 coverage). Recommendation for 14: `SCORE_FEED_TTL_SEC = 25200` (6 h + 15 min cron offset + 20 min job timeout + 15 min poll + margin). The bot cannot extend the TTL: it uses `min(ttl_seconds, SENTINEL_TTL_MAX_SEC)`, and 14's own `stale` flag uses the same TTL.
2. **Feed read auth — resolved.** 14 serves the route in the open read tier (master §9), and the earlier `FEED_AUTH_REQUIRED` key is withdrawn (02 §H1). Exposure beyond loopback is governed by 14/16 and 02 §I2.
3. **Spec 13 verdict — mostly resolved.** The loader reads 13 §3.6 as is (§2.7). Two items remain:
   - (a) *Resolved 2026-09-24:* 13 §3.6 emits `criteria.u1_upsize` / `upsize_gate_pass` (the §8.4 row E rule) and `promotable`; the loader requires `promotable` for `ok` (§2.7).
   - (b) *Resolved 2026-09-24:* the telemetry export is §2.7's `sentinel-telemetry-1` JSON (`python -m app.sentinel.telemetry export`), passed to 13 as `--observe-telemetry <json>`; 13's c7 requires `n_entries ≥ 20`.
4. **Spec 03 column types — resolved by 02 §B1/§B6.** `scores.as_of` is INTEGER in storage, ISO at the wire; `red_flags` are names. The bot reads only the wire format.
5. **Host/port/token key names — resolved.** They are 14 §5's `API_HOST`, `API_PORT` (8090) and `API_AUTH_TOKEN`; the bot keeps `HEALTH_PORT=8081`.
6. **Which bot strategy is live.** The hook is strategy-agnostic (shared `EmaManager` path), and the old repo disagrees on whether trend was retired 2026-06-02. Confirm from the Pi's `.env` + `/health` before Phase B; use `SENTINEL_STRATEGY_TAGS` if only one manager should be modulated.
7. **Audit columns beyond the brief's six.** `sentinel_occupant_key` (master §5 identity rule) and `sentinel_reason` (Phase C needs to separate `missing` from `stale` from `curve`) are added; confirm.
8. **Velocity.** Carried, logged, weight 0.0. Proposal: velocity earns weight only through its own gate (a 13 verdict with factor `velocity`); until then `SENTINEL_VELOCITY_WEIGHT` stays 0 and `gate.py` licenses only factor `quality`.
9. **Quality vs composite as the curve input.** Quality is chosen to avoid double-counting flags and velocity. Revisit after Phase 9 if the composite passes the gate and quality alone does not; that would need a verdict with factor `composite` and a `gate.py` change.
10. **Provisional runs.** The feed serves the 06/12/18:00Z provisional slots as well as the 00:00Z official one. 14 already carries `run_kind`, and the bot logs it in `score_feed_log`. If Phase B telemetry shows provisional rows flipping tiers, add an opt-in bot filter (no feed change).
11. **Pre-launch occupants** are excluded by 14's default (`include_prelaunch=0`) ⇒ neutral. The bot never requests them; if requested, their null `composite`/`tier` would be neutral anyway (02 §H1).
12. **Stale-at-receipt handling** diverges from the parent (parent: 422). Pull semantics make acceptance-with-warning more informative, and 14 may legitimately serve an old row for an occupant it no longer scores; confirm.
13. **Master §7.7 confidence threshold** (0.4) is reused as `SENTINEL_MIN_CONFIDENCE`; agree that the bot should not second-guess it with a different number.
14. **Telegram bot token.** Checklist B3 suggests reusing the old bot's `TELEGRAM_BOT_TOKEN` for TaoSentinel. If the revived bot runs its long-poll command bot with the same token on the same laptop, both processes call `getUpdates` and Telegram returns 409 Conflict to one of them. The revived bot needs its own token, or its command bot disabled.
15. **Shadow series after a promotion.** With `spec=ts-1.0` pinned, after 16's promotion runbook (`SPEC_VERSION=ts-1.1`, `SCORING_SHADOW_SPECS=ts-1.0`) the bot keeps receiving the ts-1.0 shadow rows; 14's SQL does not filter `scores.shadow`. This is intended (the old series stays warm), and an active verdict for ts-1.0 keeps applying. Moving to ts-1.1 means a new URL, whitelist entry, Phase B and verdict.

---

## 12. Sources

- `E:\TaoData\specs\02-cross-cutting-conventions.md` §A2, §B1, §B6, §B12, §F2, §H1, §I2 (binding).
- `E:\TaoData\specs\00-MASTER-SPEC.md` §1.3, §2, §5, §6, §7.5, §7.7, §9, §10, §11, §12.
- `E:\TaoData\specs\14-api-telegram-and-ui.md` §3.5 (`GET /api/v1/scores/latest` body, SQL, empty-DB response), §5 (`SCORE_FEED_TTL_SEC`, `API_*`) — read 2026-09-24 while 14 was being edited; 02 §H1/§B6 win where they differ.
- `E:\TaoData\specs\12-feature-store-and-scoring-engine.md` §2.2 (slot/`computed_at` time model, hh:15Z run, 1 200 s timeout); `03-storage-schema.md` `scores` DDL (`computed_at`, `shadow`, `red_flags` names).
- `E:\TaoData\specs\13-validation-harness.md` §2.7 (c1–c7, c7 file path), §3.6 (verdict JSON), §8.7 (CLI), §11 Q9.
- `E:\TaoData\specs\16-ops-deploy-and-observability.md` restore and spec-promotion runbooks (feed `as_of` may step back; ts-1.1 promotion).
- `E:\TaoData\specs\18-operator-checklist.md` (B3 Telegram token reuse; no feed keys defined there).
- `E:\TaoData\research\01-prior-sentiment-design.md` (full: payload, fail-safe, TTL inclusivity, store split, keep/reject list, gotchas).
- `E:\TaoData\research\01-factor-validation-tooling.md` §5 (evidence gates 1–6, rollout ladder), §8 (integration recipe, PIT rule).
- `E:\TaoData\research\02-gapfill-old-bot-runtime-skeleton.md` §1 (lifecycle anchors 190-356, 1294-1366, 1369-1578, 2144-2283, 3450-3462), §4 (auth + `configure()` injection), open question on same-box ports.
- `E:\SN_Trader\SubnetTrader\specs\feature-sentiment-ingestion.md` lines 1-140 (goal, payload, validation, 200 echo), 260-320 (fail-safe rule, modes, clamp rationale, kill switch, Phase C artifact), 380-427 (rollout A→D, verification, eight safety properties, risks, open questions).
- `E:\SN_Trader\SubnetTrader\specs\feature-sentiment-ingestion-phase-a.md` lines 80-180 (`SentimentStore`, epoch-seconds convention, `would_be_factor/factor_for`), 200-280 (Pydantic models, router, 503 on uninitialised store), 420-470 (`_sentiment_store` attribute, hook listing, `open_ema_position` threading).
- `E:\SN_Trader\SubnetTrader\app\portfolio\value_kill.py` lines 178-199 (`is_killed`, `engage_kill`, `clear_kill`).
- `E:\SN_Trader\SubnetTrader\app\api_auth.py` (whole file, copied verbatim).
- `E:\SN_Trader\SubnetTrader\app\portfolio\ema_manager.py` (HEAD 20e5761, 2026-06-29; re-verified 2026-09-23): :12 (captured `settings` import), :117-158 (`__init__`, `_regime`/`_fundamentals` idiom), :847 (only `_enter` call), :1220 (`_max_pool_impact_pct`), :1986 (`_enter`), :1989 (price-drift chain read), :2024-2040 (depth clamp), :2044-2057 (balance guard), :2063/:2073 (validator hotkey, orphan check), :2096-2146 (JIT fresh-pool block; re-cap :2103-2106, slippage reject :2116-2128), :2148 (`ENTER` log), :2159 (`execute_swap`), :2187-2197 (`open_ema_position` call).
- `E:\SN_Trader\SubnetTrader\app\storage\db.py` :82-119 (migration tuple idiom), :136-180 (`open_ema_position`; `entry_ts = utc_iso()` at :168).
- `E:\SN_Trader\SubnetTrader\app\config.py` :193/:259/:355/:412 (strategy tags), :447, :494, :501, :509-510, :646-653, :685 (`settings = Settings()` at import); `app\config_api.py` :482-483 (hot-reload rebinds `app.config.settings`); `app\main.py` :190-356 (managers 243-263), :1369-1578, :2185/:2189, :2230-2283, :2492, :3421-3422, :3450-3462; `app\metrics_api.py` :21-27; `ops\systemd\subnettrader.service` (Type=notify, `ProtectHome=false`).

## Changelog
- 2026-09-23 reconciliation + review:
  - H1: the producer side is withdrawn (route, `FeedCache`, `build_feed` SQL, `feed_push`, `FEED_*` keys, `/health.feed`, TaoSentinel tests). The bot consumes 14's `/api/v1/scores/latest?spec=ts-1.0&include_insufficient=1` unchanged, and pushing is dropped on both sides.
  - H1 model: ISO `as_of: datetime | None`, `scores` min 0, null `quality`/`composite`/`tier`/`confidence` ⇒ that netuid neutral, `red_flags` as names, empty-DB body accepted, ETag optional. Code checked against 14's example with Pydantic 2.12.
  - B6: `as_of` is the slot instant. Freshness is per row (14 serves the latest row per occupant, so rows can be old). The future-skew check uses `generated_at`, then `computed_at`, then `as_of`. `server_time` is removed.
  - B6/16: per-occupant monotonic ingest (an older row never overwrites, e.g. after a restore). A `stale` batch marks every entry stale.
  - B1: the bot's tables use INTEGER epoch seconds. `score_feed_log` is re-keyed to `(spec_version, occupant_key, as_of)`. The B7 coverage SQL uses `julianday()` because `entry_ts` is ISO text.
  - F2/B12: `SCORING_SPEC_VERSION` is noted as an alias. `./SENTINEL_KILLED` is noted as a bot-side file outside B12's closed list. Ports are :8090/:8081. The systemd unit uses `After=` only (`Wants=` could start TaoSentinel during a restore).
  - Hook moved from :2040 to between :2146 and :2148, after the balance guard, JIT re-cap and slippage reject. The old anchor let the sentinel create entries the base strategy rejected, and let the re-cap undo a haircut the audit had recorded. The multiply is skipped at 1.0 and `amount_tao` is assigned last. Anchors re-verified.
  - Gate aligned to 13 §3.6: `verdict_quality_<ts>.json` with `gate_pass`, `pending` and `factor == quality`. Active mode is bound to the verdict's `spec_version`. There is no upsize criterion in 13, so Phase E is blocked.
  - `SENTINEL_FACTOR_CEIL` is range-checked only, with the effective ceiling applied in `decide()`. The old validator would have crashed at import.
  - c7 telemetry file defined (`since`, `days`, `live_signal_coverage`, `n_entries`). The B exit needs `n_entries ≥ 20`, and the Phase C command is now 13's `report`.
  - Jobs are registered unconditionally so hot-reload works. The prime poll runs before the first cycle. The non-existent `TokenBucket` is removed.
  - Tests added for per-row TTL, skew, stale batch, monotonic ingest, null rows, spec-bound active mode, hook placement and telemetry. §11 updated: the TTL recommendation is 25200 for 14, plus new items on Telegram token sharing and shadow series.
- 2026-09-24 final pass: §2.7 `GateVerdict.ok` now also requires 13's `promotable is True` (new detail `not_promotable`) for factor `quality` and the feed's `spec_version`, else the entry stays in `observe`; verdict file renamed to 13's `gate_verdict_<factor>_<ts>.json`, INTEGER `generated_at`; Phase E unblocked (13 emits `upsize_gate_pass`/`u1_upsize`).
- 2026-09-24 final pass: c7 telemetry is a JSON export (`python -m app.sentinel.telemetry export --out PATH`, schema `sentinel-telemetry-1`: `n_entries`, `days`, `entry_coverage` (+ alias `live_signal_coverage`), per-entry `decided_factor`/`applied_factor`); §8.4 row C passes that JSON as `report --run-all --observe-telemetry`, never the SQLite ledger.
- 2026-09-24 final pass: tests (§9.2 gate/telemetry), verify D1/B7 and §11 Q3 updated accordingly.
