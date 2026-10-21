# 15 — Trading-bot integration contract

**Status:** built 2026-10-18 (consumer in `~/tao-terminal/app/sentinel/`; the v2 bot engine that calls the hook is that repo's phase B1, unbuilt) · **Phase:** 12 · **Depends on:** 02 (ledger: §H1 feed contract, §B6 `scores` time semantics, §B1 time encoding, §B12 sentinels), 14 (the producer: `GET /api/v1/scores/latest`, Phase 11), 12 (writes `scores` at `spec_version = ts-1.0`), 13 (verdict JSON §3.6, gate criterion c7), 16 (`SCORING_KILLED`, restore and spec-promotion runbooks), 19 §0.1b (the τ Terminal is the operator's UI) · **Master spec refs:** §1.3 (evidence before capital, fail-safe ops), §2 (the bot is not ported into TaoSentinel), §5 (`occupant_key`), §6 (`scores`), §7.5 (red-flag names), §7.7 (confidence, tiers), §9 (trading contract shape), §10 (evidence gate), §11 (kill switches), §12 (Phase 12).

---

## 0. What the 2026-10-18 revision changed (read first)

The 2026-09-24 draft patched the **old** SubnetTrader (`E:\SN_Trader\SubnetTrader`, `EmaManager._enter`, `ema_positions`, `.env` `SENTINEL_*` keys, `/health` on `:8081`). That bot will not be revived: it ran live 2026-06-05 → 2026-08-17, was auto-killed at a 21.95 % drawdown, and its strategy backtests at about breakeven after costs. The agreed plan (`~/tao-terminal/docs/SUBNETTRADER_V2_PLAN.md` §3.1) builds **SubnetTrader v2 inside `~/tao-terminal`** as a `tao-bot` service that reuses the terminal's `Trader` (limits, hotkey rules, paper/live, safe staking). This spec is retargeted at that host:

| Topic | 2026-09-24 draft | Now |
|---|---|---|
| Host repo | revived `~/subnettrader` | `~/tao-terminal` (not a git repo; backups in session scratchpads) |
| Hook | inline block in `EmaManager._enter` between :2146 and :2148 | `SentinelSizer.sized_preview()` around `Trader.preview` (§2.6): base preview → factor → re-preview |
| Audit | 8 `ALTER`ed columns on `ema_positions` | `sentinel_decisions` table (+ `base_tao`, `amount_tao`, confidence, tier, flags, `trade_id` → `app_trades`) |
| Config | `.env` `SENTINEL_*` via pydantic-settings, hot reload via `config_api` | `config.toml` `[sentinel]` (dataclass, like `[workbench]`); `POST /api/sentinel/reload` |
| Status | `/health.sentinel` on `:8081` | `GET /api/status` → `sentinel`, `GET /api/sentinel[/{netuid}]` on `:8787` |
| Controls | `/api/control/sentinel-{kill,resume,reload-gate}` behind `require_auth` | `POST /api/sentinel/{kill,resume,reload}` behind the terminal's per-launch token |
| Alerts | Telegram `send_alert` | desktop `notify-send` + the terminal's SSE `alert` event |
| Scheduler | APScheduler jobs registered unconditionally | two daemon threads (poller, hourly coverage) that check `enabled` each tick |
| systemd | `After=taosentinel.service` | none: `tao-terminal` is a **user** unit and `taosentinel` a **system** unit, which cannot be ordered; the consumer is neutral without the producer anyway |

Also fixed in this pass:
- **Producer ETag bug (14).** `/scores/latest`'s ETag hashed `spec|max_as_of|max_id|params|killed`, not `stale`. When the TTL passed without a new row, `stale` flipped to `true` in the body while the ETag stayed the same, so a consumer that sent `If-None-Match` kept getting `304` and never saw it. The tag now also hashes `stale` (`app/api/scores.py`, `tests/api/test_reads.py::test_scores_latest_killed_and_empty`). The consumer was already safe because it keeps its own per-row TTL clock.
- **`INSERT OR IGNORE` swallows NOT NULL violations.** A `score_feed_log` row without `stale` would vanish silently. `log_feed_rows` now defaults the NOT NULL columns.
- **Sizing can no longer go below the chain minimum.** `round(base × f, 6)` is floored at `[trading] min_tao_per_trade` (0.01 τ), because the stake would otherwise be refused by the second preview or swept by the chain. Since `base_tao` is stored, the applied ratio stays exact (§2.8).
- **TTL wire range.** `ttl_seconds` accepts 1..2 592 000 (30 d) instead of ≤ 86 400, so a producer TTL above a day no longer rejects every batch. The effective TTL is still capped by `ttl_max_sec` (86 400).
- §11 Q1 (TTL 25 200) is resolved: 14 ships `SCORE_FEED_TTL_SEC = 25200`.

---

## 1. Purpose and scope

This spec is the **only seam** between TaoSentinel and capital. It defines (a) which fields of spec 14's versioned, fail-safe-neutral **score feed** the bot relies on and how it validates them (14 owns and serves the feed; 02 §H1), (b) the **consumer package** the v2 bot uses to read it, (c) the **score→factor curve** and the clamp band, (d) the **observe/active** ladder and the evidence gate that guards every step, and (e) the audit trail that lets Phase C prove or refute the signal before any position is shaped by it.

**In scope**
- TaoSentinel side: **nothing new.** The bot consumes spec 14's `GET /api/v1/scores/latest` unchanged apart from the ETag fix above (02 §H1). This spec has no producer route, cache, SQL, job or config key of its own.
- Consumer side, in `~/tao-terminal`: `app/sentinel/` (models, kill, factor, gate, settings, store, client, storage, hook, telemetry, runtime), the `score_feed_log` and `sentinel_decisions` tables, the `[sentinel]` config section, the terminal routes and status block, the observe-only advice line in the manual trade ticket, and tests.
- The rollout ladder A → B → C → D → (E) with mechanical enforcement (active mode requires a gate-verdict artifact on disk).

**Out of scope**
- How scores are computed (12), how the feed is served (14 §3.5), how the gate is computed (13), and the v2 bot's strategy, universe, exits and scheduling (`SUBNETTRADER_V2_PLAN.md` B1).
- Any new entry signal. The factor is a **post-sizing multiplier**: it never creates, re-orders or (by default) vetoes an entry. The opt-in `flag_block_entry` (§2.6) is the single explicit exception master §9 allows.
- Exits. Sells are never modulated (`sized_preview` refuses a sell).
- Manual trades. The ticket shows the score and the would-be factor as advice; it never resizes or blocks a manual order and never writes a decision row.

---

## 2. Design

### 2.1 Topology decision

Both services run on the **same Ubuntu laptop** and both bind loopback. The consumer **pulls**.

```
 laptop (Ubuntu)
 ┌───────────────────────────────────────┐         ┌──────────────────────────────────────────────┐
 │ taosentinel.service (system) :8090    │         │ tao-terminal.service (user) 127.0.0.1:8787   │
 │                                       │  GET    │   SentinelRuntime(consumer="terminal")       │
 │  scores ─► 14's /api/v1/scores/latest ┼◄────────┼── SentinelClient: prime poll, then every 15 m │
 │            open read tier, ETag       │  200/304│        ▼                                     │
 │  scoring engine (12): slot hh:00Z,    │         │   ScoreStore (dict[netuid], per-row TTL)     │
 │  run at hh:15Z                        │         │        ├─► score_feed_log (terminal.db)      │
 │  ./SCORING_KILLED → stale=true        │         │        ▼                                     │
 └───────────────────────────────────────┘         │   /api/sentinel, /api/status.sentinel,       │
                                                   │   ticket advice line (manual buys)           │
                                                   │                                              │
                                                   │ tao-bot (v2 B1, unbuilt): its own            │
                                                   │   SentinelRuntime(consumer="bot") and        │
                                                   │   SentinelSizer.sized_preview(Trader, …)     │
                                                   │   ./SENTINEL_KILLED → factor 1.0             │
                                                   └──────────────────────────────────────────────┘
```

| Decision | Value | Rationale |
|---|---|---|
| Transport | Pull `GET http://127.0.0.1:8090/api/v1/scores/latest?spec=ts-1.0&include_insufficient=1` every `poll_interval_min` (15), plus one prime poll when the runtime starts | Same box and clock, no tunnel. TaoSentinel needs no knowledge of the consumer; a dead consumer costs TaoSentinel nothing; a dead TaoSentinel costs the bot nothing (neutral). `include_insufficient=1` makes rows with `confidence < 0.4` arrive, so the bot records `low_confidence` rather than `missing` and still sees a block flag on such a row. `spec` is pinned, so a producer-side `SPEC_VERSION` promotion never silently switches the series. |
| Consumers | One `SentinelRuntime` per process that needs scores. Today: the terminal (`consumer="terminal"`). With v2 B1: `tao-bot` (`consumer="bot"`), which owns the sizing decisions. | The store is in-memory and per process. `score_feed_log` is keyed per consumer, so two processes never dedupe each other's receipts. |
| Auth | 14 serves the route in its **open read tier** (master §9). The client sends `X-Auth-Token` only when `[sentinel] auth_token` is set. The terminal's own sentinel `POST` routes need its per-launch token (`X-Terminal-Token`); its `GET`s are open on loopback like every other terminal read. | Loopback reads are open by master rule. |
| Push variant | **None.** | 02 §H1. |
| Ports | TaoSentinel `:8090` (API and `/ui`); terminal `:8787`. | No collisions. |
| Service ordering | None. | `tao-terminal` runs under the user manager and `taosentinel` under the system manager, and systemd cannot order units across them. None is needed: without TaoSentinel every factor is 1.0. |
| Clock | Shared system clock. `skew_sec = generated_at − observed_at` is logged and drives the future-skew reject (§3.2; 02 §B6). | The tripwire stays in force even on one clock. |

### 2.2 Data flow and timing

1. Spec 12 writes one `scores` row per active occupant per run. `as_of` is the **slot instant** (00:00Z official; 06/12/18:00Z provisional); the run starts at hh:15Z and lands within its 1 200 s job timeout; `computed_at` is the run instant (02 §B6).
2. Spec 14's `queries.latest_scores` serves the **latest row per current occupant** for the requested `spec`, so per-row `as_of` may differ. The batch `as_of`/`computed_at`/`run_kind` are the newest served row's. Times are ISO (`…Z`), `red_flags` are name strings. Pre-launch rows are excluded by default; rows with `confidence < 0.4` are dropped unless `include_insufficient=1`. An empty result returns `200 {as_of: null, count: 0, scores: [], stale: true, …}`.
3. 14 sets the batch flag `stale = (now − newest as_of) > ttl_seconds or exists(./SCORING_KILLED)`, with `ttl_seconds = SCORE_FEED_TTL_SEC` (25 200).
4. The client polls. It sends `If-None-Match` only after a valid `200` that carried an ETag; a `304` is an ok poll with no ingest. A `200` is validated **whole-batch** by `FeedPayload` (§3.2). `ScoreStore.ingest()` then applies the future-skew reject and the spec-version whitelist, and handles each row:
   - freshness is measured from the **row's own `as_of`**;
   - per occupant, an older row never replaces a newer one (a different `occupant_key` for the netuid replaces regardless);
   - a row with a null `quality`, `composite` or `tier` is stored as a neutral entry.

   A `stale: true` batch marks every stored entry stale. Rows are appended to `score_feed_log` with `INSERT OR IGNORE` on `(consumer, spec_version, occupant_key, as_of)`, which is idempotent across polls and restarts.
5. At entry time the bot calls `SentinelSizer.sized_preview(trader, request, strategy)` (§2.6). In effective `active` mode the factor shrinks the order; in `observe` it is only logged. Both write one `sentinel_decisions` row.

### 2.3 The three-layer fail-safe

1. **Default-off.** `[sentinel] enabled = false`: the poller tick and the coverage job do nothing, the hook never reads the store, and the order is byte-identical to the no-sentinel order.
2. **Observe-before-act.** `mode` defaults to `observe`; only `active` multiplies. `active` is downgraded to `observe` unless the gate-verdict artifact (§2.7) is present, passing, promotable, and for the same `spec_version` as the score being used.
3. **Bounded effect.** The factor is clamped to `[factor_floor, effective ceiling]` = `[0.5, 1.0]`. A `factor_ceil` above 1.0 takes effect only with an upsize verdict (§2.7). `./SENTINEL_KILLED` (in the terminal directory) forces `1.0` everywhere instantly. The multiply happens after every base gate (§2.6), so a compromised feed can at most halve an order the base strategy already approved. The kill file belongs to the consumer, not TaoSentinel, so 02 §B12's closed list is unaffected.

**The load-bearing rule:** *missing OR stale OR disabled OR killed OR low-confidence OR null quality/composite/tier (02 §H1) OR unaccepted spec_version → factor = 1.0 (no effect).* `ScoreStore.decide()` is the single choke point where every branch lives.

### 2.4 Score → factor curve (Phase D v1, de-risk-only)

Inputs are the feed row fields. `quality` (the slow Health score) drives the curve, because `composite` already embeds the red-flag multipliers and velocity, which the curve handles separately (no double counting).

```
FLOOR = factor_floor   (0.5)      CEIL = factor_ceil if gate.cached().upsize_ok else 1.0   (§2.7)
Q_LO  = q_floor_at     (30.0)     Q_HI = q_neutral_at (70.0)     Q_UP = q_ceil_at (90.0)
C_MIN = min_confidence (0.4)      C_FULL = full_confidence (0.7)
w_v   = velocity_weight (0.0)     BLOCK = set(block_flags)

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

Worked values (v1, `CEIL=1.0`): Q=30,c≥0.7 → 0.50 · Q=50,c≥0.7 → 0.75 · Q=70 → 1.00 · Q=30,c=0.55 → 0.75 · Q=90 → 1.00 · any block flag on a non-null row → 0.50 · quality null (with or without a block flag) → 1.00 · c=0.39 → 1.00. `decide()` also maps a null `composite` or `tier` to `(1.0, "no_quality")` before calling the curve. With `CEIL=1.0` step 5 never runs, and the function stays ≤ 1.0 (tested over 10 000 random inputs). `velocity_weight` ships at 0.0: velocity is carried in the feed and the audit so Phase C can test it, but it earns weight only through its own gate (§11).

Default `block_flags`: `emission_disabled, dereg_imminent, miner_burn_zeroed, owner_extraction, key_compromise`. All five exist in ts-1.0's flag catalog (`app/scoring/spec.py FLAGS_TS_1_0`, checked 2026-10-18).

### 2.5 Modes and the effective-mode matrix

| `enabled` | `mode` | gate verdict (§2.7) | `SENTINEL_KILLED` | poller does work | hook reads store | decided `sentinel_factor` | multiply |
|---|---|---|---|---|---|---|---|
| false | any | any | any | no | no | 1.0, reason `disabled`, mode `off` | no |
| true | off | any | any | yes | no | 1.0, reason `disabled`, mode `off` | no |
| true | observe | any | absent | yes | yes | would-be, mode `observe` | no |
| true | observe | any | present | yes | yes | 1.0, reason `killed`, mode `observe` | no |
| true | active | not `ok` (`missing`, `parse_error`, `fail`, `pending`, `not_promotable`, `wrong_factor`, `spec_not_accepted`) | absent | yes | yes | would-be, mode `observe` (downgraded; status `mode_effective="observe"`, `gate=<detail>`) | no |
| true | active | `ok`, entry `spec_version` == verdict `spec_version` | absent | yes | yes | applied (== would-be), mode `active` | **yes** (skipped when the factor is 1.0) |
| true | active | `ok`, entry of another whitelisted `spec_version` | absent | yes | yes | would-be, mode `observe` (a verdict licenses only its own `spec_version`) | no |
| true | active | `ok` | present | yes | yes | 1.0, reason `killed`, mode `active` | no (factor 1.0) |

`gate.effective_mode(settings, gate, spec_version=None)` is pure: `off` when `enabled` is false or `mode=off`; `observe` when `mode=observe`; with `mode=active` it returns `active` only when `gate.ok` and `spec_version` is `None` or equals `gate.spec_version`, else `observe`. The status block reports it with `spec_version=None`; `decide()` evaluates it with the entry's `spec_version` and returns it as `FactorDecision.mode`, which the decision row persists. The kill file changes only the factor, never the mode. With `mode=off` the poller still runs, so the status block shows feed liveness during bring-up.

**Config changes without a restart:** edit `config.toml` `[sentinel]`, then `POST /api/sentinel/reload`. It re-reads the section and the verdict, re-checks the downgrade alert and wakes the poller; a bad section is refused and the old settings stay. The poller and coverage threads check `enabled` on every tick, so enabling or disabling works through a reload.

### 2.6 Hook placement (v2 bot, around `Trader`)

The terminal's `Trader.preview(TradeRequest)` is where every base gate lives: allow-list, `min/max_tao_per_trade`, the rolling `max_tao_per_day`, free balance plus fee reserve (refused live, warned in paper), hotkey choice, and the chain quote with its impact warning. `Trader.execute` re-checks the limits and refuses if spot moved past the worst price. The hook must therefore wrap the preview, not sit inside it:

```
sized_preview(trader, req(side=buy, amount=base), strategy):
  1. base_preview = trader.preview(req)                 # every base gate on the BASE amount; TradeError propagates
  2. sz = size(netuid, base, strategy, min_tao=trader.lim.min_tao_per_trade)    # never raises
  3. blocked (opt-in, active only)  → record, return (None, sz, id)             # no trade
  4. sz.amount_tao != base          → p = trader.preview(req with amount=sz.amount_tao)
                                       on any error: factor 1.0, reason hook_error, p = base_preview
  5. record one sentinel_decisions row; return (p, sz, decision_id)
the bot then: trade = trader.execute(p.id, "buy"); sizer.attach_trade(decision_id, trade["id"])
```

`size()` mirrors the old inline block:

```python
sz = EntrySizing(base_tao=base, amount_tao=base)                  # defaults: factor 1.0, disabled, off
try:
    if store is not None and s.enabled and applies(strategy):
        d = store.decide(netuid, now)                              # mode is per decision (gate + spec binding)
        sz.mode = d.mode
        if d.mode != "off":
            sz.factor, sz.reason = d.factor, d.reason; copy the entry snapshot
            if d.mode == "active":
                if s.flag_block_entry and d.reason == "block_flag": log BLOCK; return blocked
                new = round(base * d.factor, 6) if d.factor != 1.0 else base
                new = min(base, max(new, min_tao))                 # never under the chain minimum the base passed
                log "SENTINEL APPLIED [strategy] SN{n}: x{f} → {new} τ of {base} ({reason})"
                sz.amount_tao = new                                # LAST statement: nothing after it can raise
            else:
                log "SENTINEL OBSERVE [strategy] SN{n}: would x{f} (q=…, v=…, as_of=…, reason=…)"
except Exception:
    sz = (amount_tao=base, factor=1.0, reason="hook_error"); hook_errors += 1; latched alert
```

**Why around the preview and not inside the strategy's sizing.** A haircut applied before the gates lets the sentinel *create* entries the base strategy rejects: a halved order passes a per-trade cap, a daily cap or a balance check the full order failed. Applying it after a successful base preview keeps every base decision identical to the no-sentinel bot and makes `amount_tao ≤ base` exact. The second preview only re-prices a smaller order. Every gate is monotone in size, so the smaller order passes too, except `min_tao_per_trade`, which the floor handles. A second-preview failure (a chain hiccup) falls back to the base order, because neutral means "as if TaoSentinel did not exist".

**Opt-in block** (`flag_block_entry = true`, honoured only when `decision.mode == "active"`). On `reason == "block_flag"` the sizer logs `SENTINEL BLOCK [strategy] SN{n}: flags=[…]`, increments `entries_blocked`, records a decision row with `blocked = 1`, and returns no preview. The bot skips the entry, and nothing state-changing has run (previews are read-only chain calls). Default `false`; Phase D ships with it off.

**`strategy_tags`.** Empty = every v2 strategy is sized; otherwise only the listed strategy tags (a tag not listed gets reason `disabled`, mode `off`).

### 2.7 Gate-verdict artifact (mechanical enforcement of the ladder)

Spec 13's `report --run-all` writes `data/quant/results/gate_verdict_<factor>_<YYYYMMDD_HHMMSS>.json` (13 §3.6, §8.7). The operator copies the passing `gate_verdict_quality_<ts>.json` to `[sentinel] gate_verdict_path` (default `~/tao-terminal/data/sentinel_gate_verdict.json`). The runtime loads it at start and on `POST /api/sentinel/reload`, never on the trade path. The loader reads these keys from 13 §3.6 (matched against `app/quant/gate.py` 2026-10-18) and ignores every other key:

```json
{"spec_version": "ts-1.0", "factor": "quality", "generated_at": 1796101964,
 "gate_pass": true, "pending": [], "promotable": true, "upsize_gate_pass": false}
```

`GateVerdict(ok, upsize_ok, spec_version, factor, detail, generated_at)`; the loader never raises.
- `ok = gate_pass is True and pending == [] and promotable is True and factor == "quality" and spec_version ∈ accepted_spec_versions`. `promotable` is 13's own statement (`gate_pass ∧ factor ∈ {quality, composite_smoothed}`); a missing or non-true `promotable` never yields `ok`. The curve's input is `quality`, so a verdict on another factor does not license it. `effective_mode()` additionally requires the verdict's `spec_version` to equal the entry's feed `spec_version`.
- `detail ∈ {pass, missing, parse_error, fail, pending, not_promotable, wrong_factor, spec_not_accepted}`, checked in the order missing → parse_error → fail → pending → not_promotable → wrong_factor → spec_not_accepted.
- `upsize_ok = ok and upsize_gate_pass is True` (13 emits it as the mirror of `criteria.u1_upsize.pass`).

`[sentinel] factor_ceil` is only range-checked (1.0..1.5). `decide()` evaluates the effective ceiling `factor_ceil if gate.cached().upsize_ok else 1.0`, so no config value depends on runtime state.

**Observe telemetry (13 criterion c7).** The hourly coverage thread writes `[sentinel] telemetry_path` (default `~/tao-terminal/data/sentinel_observe_telemetry.json`) atomically (tmp file + `os.replace`). The same JSON comes from `python -m app.sentinel.telemetry export --out PATH|- [--days 30] [--spec ts-1.0] [--db PATH]` (opens `data/terminal.db` read-only; exit 0, or 2 on DB/IO error). This JSON, never the SQLite file, is what 13's `--observe-telemetry` takes (13 refuses a SQLite path). Schema `sentinel-telemetry-1`:

```json
{"schema": "sentinel-telemetry-1", "spec_version": "ts-1.0", "generated_at": 1793437203,
 "since": "2026-10-01T00:05:12Z", "days": 34, "window_days": 30,
 "n_entries": 58, "entry_coverage": 0.71, "live_signal_coverage": 0.71,
 "entries": [{"entry_id": 4411, "entry_ts": "2026-10-28T13:02:11Z", "netuid": 64, "strategy": "momentum",
              "occupant_key": "64:5228683", "as_of": 1793404800, "spec_version": "ts-1.0", "mode": "observe",
              "reason": "curve", "decided_factor": 0.75, "applied_factor": 1.0, "base_tao": 0.5,
              "amount_tao": 0.5, "trade_id": 812, "trade_mode": "paper", "covered": true}]}
```

- **An entry** is a `sentinel_decisions` row decided in the trailing `window_days` (30), `blocked = 0`, `sentinel_mode ∈ {observe, active}`, whose linked `app_trades` row has `status ∈ {success, paper}`, and whose `sentinel_spec_version` is the requested spec or NULL (a `missing` decision has no spec and counts as uncovered). Paper entries count: c7 measures whether a fresh signal was present at entry time, which does not depend on the order being live (§11 Q16).
- `covered` = `sentinel_reason ∈ {curve, block_flag}`; `entry_coverage` = covered / `n_entries` (null when there are none); `live_signal_coverage` is the same number under its older name (13 accepts either).
- `applied_factor = amount_tao / base_tao` for `active` rows, `1.0` for `observe` rows; `decided_factor = sentinel_factor`.
- `since` = earliest accepted `score_feed_log.observed_at` for the spec (any consumer); `days = floor((now − since) / 86400)`, 0 without one.
- 13's c7 rule (`days ≥ 30`, `entry_coverage > 0.60`, `n_entries ≥ 20`) reads only those three keys; `entries[]` is echoed and length-checked.

Before Phase C the operator runs the export (or copies the hourly file) to `~/Documents/TaoData/data/quant/observe_telemetry.json` (13's `QUANT_OBSERVE_TELEMETRY_PATH`).

### 2.8 Decided-vs-applied rule (audit)

`sentinel_factor` always records **what the feature decided** (`decide().factor`): the would-be factor in `observe`, the multiplier in `active`. The row also stores `base_tao` (what every base gate approved) and `amount_tao` (what went to the `Trader`), so the applied ratio is **`amount_tao / base_tao`**, exactly, with no reconstruction rule. In `active` it equals `sentinel_factor` except when the 0.01 τ floor lifted the order. In every other mode it is 1.0. `sentinel_reason` says *why* (`curve|block_flag|disabled|killed|missing|stale|low_confidence|no_quality|hook_error`). `sentinel_mode` is the per-decision effective mode, not the configured one. Blocked entries have a row (`blocked = 1`, no `trade_id`).

### 2.9 Terminal display (observe value before the bot exists)

- `GET /api/status` gains `sentinel` (§7.3).
- `GET /api/sentinel` returns `{health, scores: [...]}`; `GET /api/sentinel/{netuid}` returns one row (404 when none is stored). Each row: the stored score fields, `fresh`, `expires_at` (`as_of + effective TTL`), `would_apply_factor`, `applied_factor` (1.0 unless effective active), `reason`, `mode`.
- The manual ticket's checks (`/api/workbench/checks`, buys only, when `enabled` and `ticket_checks`) gain one line: `TaoSentinel: quality 41 · tier D · conf 0.52 · flags repo_gone → the bot would size ×0.66` (`warn` on a block flag or a factor ≤ 0.75, else `info`; "no score" / "score is stale" → ×1.00). Advice only: nothing is resized or blocked, and no decision row is written.
- Alerts (feed down at `alert_after_failures` and on recovery, spec_version not accepted, active downgraded, hook error, kill engaged/cleared, log failures at 8) go to `notify-send` and the terminal's SSE `alert` event.

---

## 3. Data contract

### 3.1 Feed payload — spec 14's `GET /api/v1/scores/latest?spec=ts-1.0&include_insufficient=1` (consumed unchanged)

```json
{
  "as_of": "2026-09-22T12:00:00Z",
  "computed_at": "2026-09-22T12:15:41Z",
  "generated_at": "2026-09-22T12:40:03Z",
  "run_kind": "provisional",
  "spec_version": "ts-1.0",
  "ttl_seconds": 25200,
  "stale": false,
  "regime": "TRENDING",
  "count": 2,
  "scores": [
    {"netuid": 64, "occupant_key": "64:4123456", "name": "Chutes", "status": "active",
     "quality": 84.1, "velocity": 71.0, "composite": 82.4, "composite_raw": 81.9,
     "confidence": 0.91, "tier": "A", "red_flags": [],
     "pillars": {"P1": 0.81, "P2": 0.90, "P3": 0.60, "P4": 0.70, "P5": 0.78, "P6": 0.62},
     "as_of": "2026-09-22T12:00:00Z", "computed_at": "2026-09-22T12:15:41Z"},
    {"netuid": 18, "occupant_key": "18:5120033", "name": "…", "status": "active",
     "quality": 41.2, "velocity": null, "composite": 38.0, "composite_raw": 42.2,
     "confidence": 0.52, "tier": "D", "red_flags": ["repo_gone", "silence_45d"],
     "pillars": {"P1": 0.40, "P2": null, "P3": 0.30, "P4": 0.50, "P5": 0.20, "P6": null},
     "as_of": "2026-09-22T00:00:00Z", "computed_at": "2026-09-22T00:16:02Z"}
  ]
}
```
This is the shape `app/api/scores.py build_feed` serves (checked 2026-10-18; a real body from 14's seeded test app is the consumer's fixture `scores_latest_producer.json`). SN18's row is an older official row that 14 still serves because the provisional run skipped that occupant. The consumer ignores every key not listed below.

| Field | Wire type | When null or absent | Consumer use |
|---|---|---|---|
| `as_of` | ISO-8601 with offset, or null | null only with `scores: []` | batch fallback for a row without `as_of`; logged as `batch_as_of` |
| `spec_version` | `^[a-z0-9][a-z0-9._-]{0,63}$` | required when `scores` is non-empty | whitelist check |
| `ttl_seconds` | int 1..2 592 000 (14's `SCORE_FEED_TTL_SEC`) | required when `scores` is non-empty | effective TTL = `min(ttl_seconds, ttl_max_sec)` |
| `stale` | bool, default false | — | `true` ⇒ every stored entry is marked stale |
| `generated_at` | ISO, optional | skew check falls back to max `computed_at`, then `as_of` | `skew_sec`; future-skew reject |
| `computed_at` | ISO, optional, batch or row | ignored | future-skew reject; logged |
| `run_kind` | string, optional (`official`/`provisional`) | — | logged, so Phase C can separate provisional rows |
| `scores` | array, 0..1024 | `[]` ⇒ nothing stored | — |
| `scores[].netuid` | int 0..1023, unique in batch | required | store key |
| `scores[].occupant_key` | `^\d{1,4}:\d{1,12}$`, prefix == `netuid`, unique | required | identity; per-occupant monotonic ingest |
| `scores[].as_of` | ISO, optional | falls back to batch `as_of` | **the row's freshness clock** |
| `scores[].quality` | float 0..100, or null | null ⇒ `no_quality` | curve input (Health) |
| `scores[].velocity` | float 0..100, or null | curve uses quality only | carried and audited |
| `scores[].composite` | float 0..100, or null (= `composite_smoothed`) | null ⇒ `no_quality` | audited |
| `scores[].confidence` | float 0..1, or null | null ⇒ `low_confidence` | confidence haircut |
| `scores[].tier` | `A`–`F` (no `E`), or null | null ⇒ `no_quality` | audited |
| `scores[].red_flags` | list of names, ≤ 32, each `^[a-z0-9_]{1,64}$` | `[]` | block flags; unknown names tolerated; duplicates collapsed |

Rules:
- Unknown fields are ignored (`extra="ignore"`).
- Any of the following rejects the **whole batch** (schema drift = producer bug; everything goes neutral and the failure path runs): a malformed or out-of-range value; a timestamp without an offset; a duplicate `netuid` or `occupant_key`; `red_flags` given as objects; a non-empty batch without `as_of`, `spec_version` or `ttl_seconds`.
- A null `quality`, `composite`, `tier` or `confidence` makes only that netuid neutral.
- A semantic change requires a new `spec_version` string, which the consumer ignores until it is whitelisted.
- 14's `503 {"error": "db_unavailable"}` and every other non-200/304 status take the failure path (§4).

### 3.2 Consumer model

`~/tao-terminal/app/sentinel/models.py` (`ScoreRow`, `FeedPayload`) depends only on Pydantic v2 and the stdlib and does no I/O. It is the 2026-09-24 model unchanged except `ttl_seconds ≤ TTL_WIRE_MAX = 30 × 86 400` (§0). Receive-time checks (skew, whitelist) live in `ScoreStore.ingest`. TaoSentinel keeps a mirror of it in `tests/api/test_reads.py` because the repos cannot import each other.

Pins: `tests/fixtures/scores_latest.schema.json` (= `FeedPayload.model_json_schema()`; a change fails `test_schema_pin` until regenerated deliberately) and the golden bodies `scores_latest_14_example.json`, `scores_latest_empty.json`, `scores_latest_producer.json` (real 14 output), plus `scores_latest_live.json` once verify step T2 saves it.

`python -m app.sentinel.models --validate-url URL [--save PATH]` fetches the route once and exits 0 (valid) or 1.

### 3.3 TaoSentinel tables read / written

None (02 §H1). The consumer reads no TaoSentinel table and TaoSentinel writes nothing on its behalf. **metric_keys produced:** none. **events emitted:** none.

### 3.4 Consumer tables (`~/tao-terminal/data/terminal.db`, created by `termdb.connect` from `app/sentinel/storage.py SCHEMA`)

```sql
CREATE TABLE IF NOT EXISTS score_feed_log(        -- append-only; INTEGER epoch seconds
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  consumer TEXT NOT NULL,                         -- process that polled: terminal | bot
  observed_at INTEGER NOT NULL,                   -- receive time
  batch_as_of INTEGER, as_of INTEGER,             -- batch / row slot instant
  computed_at INTEGER, generated_at INTEGER, run_kind TEXT,
  spec_version TEXT, ttl_seconds INTEGER,
  stale INTEGER NOT NULL DEFAULT 0,               -- batch stale flag, or row older than TTL at receipt
  accepted INTEGER NOT NULL,                      -- 1 = written to the live store
  reject_reason TEXT,                             -- schema|spec_version|future_skew|older|http_<code>|transport
  netuid INTEGER, occupant_key TEXT,
  quality REAL, velocity REAL, composite REAL, confidence REAL, tier TEXT,
  red_flags TEXT,                                 -- JSON array of flag names
  payload_sha256 TEXT NOT NULL                    -- sha256 of the raw body ('' body on transport errors)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_score_feed_log_row ON score_feed_log(consumer, spec_version, occupant_key, as_of);
CREATE INDEX IF NOT EXISTS ix_score_feed_log_netuid ON score_feed_log(netuid, as_of);

CREATE TABLE IF NOT EXISTS sentinel_decisions(    -- one row per sized bot entry (§2.8)
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  decided_at INTEGER NOT NULL, strategy TEXT NOT NULL, netuid INTEGER NOT NULL,
  base_tao REAL NOT NULL, amount_tao REAL NOT NULL, blocked INTEGER NOT NULL DEFAULT 0,
  sentinel_factor REAL NOT NULL DEFAULT 1.0,      -- DECIDED factor
  sentinel_mode TEXT NOT NULL DEFAULT 'off', sentinel_reason TEXT NOT NULL DEFAULT 'disabled',
  sentinel_quality REAL, sentinel_velocity REAL, sentinel_confidence REAL, sentinel_tier TEXT,
  sentinel_red_flags TEXT, sentinel_as_of INTEGER, sentinel_spec_version TEXT, sentinel_occupant_key TEXT,
  trade_id INTEGER                                -- app_trades.id once executed (paper or live)
);
CREATE INDEX IF NOT EXISTS ix_sentinel_decisions_at ON sentinel_decisions(decided_at);
```

- One `score_feed_log` row per received score row, first receipt per consumer wins. A batch-level reject or a failed poll writes one row with `netuid`/`occupant_key` NULL, which is never deduped (one per failed poll). `log_feed_rows` defaults the NOT NULL columns, because `OR IGNORE` would otherwise drop a violating row silently.
- Retention: none. Accepted rows ≈ 4 slots × ~130 rows ≈ 520/day per consumer; a fully down feed adds ≤ 96 failure rows/day.

---

## 4. Failure modes (absence-vs-failure adapted for a consumer)

For a consumer the question is *what factor results*. Every row yields **no exception on the trade path**.

| # | Condition | Feed (14) | Consumer store | `decide()` → factor, reason | status (`/api/status.sentinel`) | Alert |
|---|---|---|---|---|---|---|
| 1 | TaoSentinel down / refused / timeout | — | unchanged; entries age out | fresh → curve; after TTL → `1.0, stale` | `consecutive_failures++`, `last_error`; log row `transport` | once at `alert_after_failures` (8 = 2 h), once on recovery |
| 2 | Scoring stalled | `stale=true` once `now − as_of > ttl_seconds` (ETag now changes with it) | every entry marked stale | `1.0, stale` | `stale_count = n` | TaoSentinel's own `scoring_stalled` (12) |
| 3 | `./SCORING_KILLED` on TaoSentinel | last-good rows, `stale=true` | every entry marked stale | `1.0, stale` | `stale_count` | — |
| 4 | `503 db_unavailable` or other non-200/304 | 5xx/4xx | unchanged | as #1 | `last_http_status`; log row `http_<code>` | as #1 |
| 5 | Empty DB `200 {as_of: null, scores: [], stale: true}` | 200 | nothing new; existing entries marked stale | `1.0, missing` or `1.0, stale` | `last_batch_rows=0`; poll ok | — |
| 6 | Schema drift (§3.1) | 200 | whole batch rejected; log row `schema` | as #1 | failure count | as #1 |
| 7 | `spec_version` ∉ `accepted_spec_versions` | 200 | not stored; log row `spec_version`; old entries age out | `1.0, stale` after TTL | `spec_version`, `spec_version_accepted=false` | once (latched) |
| 8 | Future skew: `generated_at` (else max `computed_at`, else batch `as_of`), or any row `as_of`, later than `observed_at + future_skew_sec` | 200 | whole batch rejected, log row `future_skew` | as #1 | failure count, `skew_sec` | as #1 |
| 9 | Feed clock behind | fresh | stored; rows look older ⇒ stale sooner | fail-safe direction | `skew_sec` logged when \|skew\| > 60 s | — |
| 10 | Row older than its TTL at receipt | valid | stored, `stale_at_receipt` warning | `1.0, stale` | `stale_count` | — |
| 11 | Netuid absent from the batch | — | no entry (an old entry is **not** wiped; it ages out) | `1.0, missing` | not in `fresh_count` | — |
| 12 | `velocity == null` | valid | stored | quality only | — | — |
| 13 | `confidence < min_confidence` | valid row | stored | `1.0, low_confidence` | `low_conf_count` | — |
| 14 | Block flag on a usable row | valid | stored | `FLOOR, block_flag` (skipped only with `flag_block_entry` in effective `active`) | `entries_blocked` | — |
| 15 | Occupant changed for a netuid | valid | new occupant replaces regardless of `as_of`; `sentinel_occupant_changed` log | curve on the new occupant | — | — |
| 16 | Consumer restart | — | empty until the prime poll (seconds after start) | `1.0, missing` until then | `fresh_count=0` briefly | — |
| 17 | `./SENTINEL_KILLED` present | — | unchanged (ingest continues) | `1.0, killed` | `killed=true` | on engage/clear via the routes |
| 18 | `enabled = false` | — | poller tick idle | `1.0, disabled` (store not read) | `enabled=false` | — |
| 19 | `mode=active`, verdict not `ok` or for another spec | — | — | would-be logged, nothing applied; mode `observe` | `mode_effective`, `gate=<detail>` | once (latched) |
| 20 | DB write fails (`score_feed_log` or `sentinel_decisions`) | — | store already updated (store first) | unaffected | `log_failures++` | at 8 (feed log) |
| 21 | Any exception in the hook or in the re-preview | — | — | `1.0, hook_error`; base order used | `hook_errors++` | at 1 (latched) |
| 22 | Null `quality`/`composite`/`tier` | valid | stored as neutral; replaces the occupant's older usable row | `1.0, no_quality` | `null_rows` | — |
| 23 | Row `as_of` older than the stored row of the same occupant (TaoSentinel restore) | valid | ignored; log row `older` if unseen | the stored entry keeps ageing | `ignored_older++` | — |
| 24 | Bad `[sentinel]` config at terminal start | — | runtime starts **disabled** with defaults; message on stdout/journal | `1.0, disabled` | `enabled=false` | — |

---

## 5. Configuration

### 5.1 TaoSentinel (`app/config.py`)

This spec owns **no** TaoSentinel key (02 §H1, §F1). The consumer depends on:

| Key | Owner | Default | Why the consumer cares |
|---|---|---|---|
| `SCORE_FEED_TTL_SEC` | 14 | `25200` | Advertised as `ttl_seconds` and the batch `stale` threshold. 6 h + 15 min cron offset + 20 min job timeout + poll margin, so rows stay fresh until the next slot lands (resolved former Q1). |
| `SCORING_SPEC_VERSION` | 14/12 | `ts-1.0` | 14's default `spec`, alias of `SPEC_VERSION` (02 §F2). The consumer pins `spec=` in its URL. |
| `SCORING_KILL_SWITCH_PATH` | 16 (02 §B12) | `./SCORING_KILLED` | Present ⇒ last-good rows with `stale=true`. |
| `API_HOST`, `API_PORT`, `API_AUTH_TOKEN` | 14 §5 | `127.0.0.1`, `8090`, `""` | Where the consumer polls. The feed route is open, so the token is irrelevant to it. |

### 5.2 Consumer (`~/tao-terminal/config.toml` `[sentinel]`, `app/sentinel/settings.py SentinelSettings`)

Every key is optional; unknown keys are refused (the terminal's `_section` idiom); `validate()` does range checks only. Relative paths resolve against `~/tao-terminal`.

| Key | Type | Default | Validation / semantics |
|---|---|---|---|
| `enabled` | bool | `false` | Layer 1. |
| `mode` | str | `"observe"` | lower-cased; ∈ {off, observe, active}. |
| `url` | str | `http://127.0.0.1:8090/api/v1/scores/latest?spec=ts-1.0&include_insufficient=1` | absolute http(s). |
| `auth_token` | str | `""` | `X-Auth-Token` only when set. |
| `poll_interval_min` | int | `15` | 1..360. |
| `http_timeout_sec` | float | `10.0` | 1..60. |
| `ttl_max_sec` | int | `86400` | 60..2 592 000; effective TTL = `min(ttl_seconds, this)`. |
| `future_skew_sec` | int | `300` | 0..3600. |
| `accepted_spec_versions` | list[str] | `["ts-1.0"]` | each `^[a-z0-9][a-z0-9._-]{0,63}$`. The first is the telemetry spec. |
| `min_confidence` | float | `0.4` | 0..1 (master §7.7). |
| `full_confidence` | float | `0.7` | 0..1, > `min_confidence`. |
| `q_floor_at` / `q_neutral_at` / `q_ceil_at` | float | `30` / `70` / `90` | strictly increasing, ≤ 100. |
| `velocity_weight` | float | `0.0` | 0..0.5. |
| `factor_floor` | float | `0.5` | 0.1..1.0. |
| `factor_ceil` | float | `1.0` | 1.0..1.5, range only; effective 1.0 without `upsize_ok`. |
| `block_flags` | list[str] | the five in §2.4 | each `^[a-z0-9_]+$`. |
| `flag_block_entry` | bool | `false` | opt-in skip, effective active only. |
| `kill_switch_path` | path | `SENTINEL_KILLED` | layer 3. |
| `gate_verdict_path` | path | `data/sentinel_gate_verdict.json` | copy of 13's verdict; needed for `active`. |
| `telemetry_path` | path | `data/sentinel_observe_telemetry.json` | c7 file, hourly. |
| `strategy_tags` | list[str] | `[]` | empty = all v2 strategies. |
| `alert_after_failures` | int | `8` | 1..1000. |
| `ticket_checks` | bool | `true` | advice line in the manual ticket (§2.9). |

`config.example.toml` carries the block with comments. Kill semantics: `touch ~/tao-terminal/SENTINEL_KILLED` (or `POST /api/sentinel/kill`) ⇒ every factor `1.0` immediately, ingestion continues; `rm` (or `POST /api/sentinel/resume`) clears.

---

## 6. Cadence, rate limits, cost

| Actor | Call | Cadence | Volume | Guard | Cost |
|---|---|---|---|---|---|
| Poller (per consumer) | 14's `GET /api/v1/scores/latest` | every 15 min + one prime at start + one per reload | 96/day, ~50 KB, loopback; mostly `304` between slots | single thread; `poll_once` refuses a request within 30 s of the previous one | $0 |
| Coverage thread | local SQL + telemetry file | 5 s after start, then hourly | 24/day | — | $0 |
| Bot entry (B1) | 1 or 2 `Trader.preview` (each one `sim_swap` quote) | per entry | ≤ a few per hour | Trader's own limits | $0 |

No paid provider is involved and `provider_spend` is untouched. The feed adds one SQL query per `200` to TaoSentinel.

---

## 7. Failure modes and fail-safe behaviour

### 7.1 Never raise into a thread or the trade path
- `SentinelClient.run`: catches everything per tick, logs to the terminal's `log` table (`component='sentinel'`), latched alerts at the threshold and on recovery.
- Coverage thread: swallows SQL/file errors and keeps the last telemetry file.
- `SentinelSizer.size` and the re-preview are wrapped (row 21); `record`/`attach_trade` count failures instead of raising.

### 7.2 Last-good semantics
- TaoSentinel: 14's. Under `SCORING_KILLED` it serves last-good rows with `stale: true` and never falls back to another `spec_version`.
- Consumer: entries are never deleted; they go stale by the clock or all at once when a batch says `stale: true`. Absence from a batch never evicts. Per occupant, an older row never replaces a newer one. No rehydration from `score_feed_log` on restart: the prime poll replaces it within seconds, and rehydrating a near-TTL score is undesirable.

### 7.3 Status block (`GET /api/status` → `sentinel`, also `GET /api/sentinel` → `health`)

```json
{"enabled": true, "mode": "observe", "mode_effective": "observe", "killed": false,
 "gate": "missing", "gate_spec_version": null, "fresh_count": 118, "stale_count": 0, "low_conf_count": 9,
 "last_as_of": 1790056800, "last_poll_at": 1790058011, "last_ok_at": 1790058011, "last_error": null,
 "last_http_status": 200, "consecutive_failures": 0, "last_batch_rows": 129, "last_generated_at": 1790058009,
 "spec_version": "ts-1.0", "spec_version_accepted": true, "skew_sec": -2, "run_kind": "official",
 "null_rows": 0, "ignored_older": 0, "ceil_effective": 1.0, "consumer": "terminal", "url": "http://127.0.0.1:8090/…",
 "entries_blocked": 0, "hook_errors": 0, "log_failures": 0, "entry_coverage_30d": null, "entry_count_30d": 0}
```
`last_as_of` is the newest row slot instant in the store (epoch s). `entry_coverage_30d`/`entry_count_30d` come from the hourly coverage thread (the c7 numbers, also in the telemetry file). No DB read on this path.

### 7.4 The eight safety properties

| # | Property | Enforced where |
|---|---|---|
| 1 | **Fail-safe neutral**: missing/stale/low-confidence/null-row/unaccepted version → `1.0` | `ScoreStore.decide()` is the single choke point; the sizer multiplies only in effective `active` and only when the factor ≠ 1.0. TTL is inclusive and per row. A `stale=true` batch marks every entry stale. |
| 2 | **Observe-before-act** | `mode` defaults to `observe`; `effective_mode()` downgrades `active` without an `ok` `GateVerdict` (13's JSON, factor `quality`, promotable) for the entry's own `spec_version`. |
| 3 | **Default off** | `enabled = false`; threads idle, the sizer short-circuits. Tests assert byte-identical amounts (disabled, no store, observe, and active at factor 1.0: killed, stale, missing, low confidence). |
| 4 | **Safe to merge** | stdlib + Pydantic + httpx (already terminal dependencies); no module-top I/O; a bad `[sentinel]` section starts the runtime disabled instead of stopping the terminal; no setting depends on runtime state. |
| 5 | **Kill switch**: `1.0` everywhere, no restart | `SENTINEL_KILLED` is checked in `decide()` right after the disabled/off short-circuit; `POST /api/sentinel/kill` / `resume`. |
| 6 | **Private transport + whole-batch validation** | Loopback on both sides; terminal POSTs need its per-launch token and same origin; `FeedPayload` validates the whole batch before any write; skew reject; version whitelist. |
| 7 | **Bounded effect** | Final `clamp(f, FLOOR, CEIL)` with effective `CEIL = 1.0` unless an upsize verdict exists. The factor is applied only after a successful base preview, so the sentinel only shrinks entries the base strategy approved; `amount_tao ∈ [max(0.5·base, 0.01), base]`. |
| 8 | **Restart-invariant audit** | `sentinel_decisions` (base, amount, decided factor, snapshot, trade link) and append-only `score_feed_log` in `data/terminal.db` (gitignored). |

---

## 8. Implementation (as built 2026-10-18)

### 8.1 TaoSentinel repo (`~/Documents/TaoData`)

Only the ETag fix (§0) in `app/api/scores.py` with its test, and the mirror-model comment in `tests/api/test_reads.py`. No file, job, key or route of this spec's own.

### 8.2 Consumer (`~/tao-terminal`)

| File | Contents |
|---|---|
| `app/sentinel/__init__.py` | package docstring. |
| `app/sentinel/models.py` | §3.2 model + `--validate-url` CLI. |
| `app/sentinel/kill.py` | `is_killed`, `engage_kill(path, reason)` (writes `{engaged_at, reason}`), `clear_kill` → bool. |
| `app/sentinel/factor.py` | `CurveConfig`, `score_to_factor`, `clamp` (§2.4). Pure. |
| `app/sentinel/gate.py` | `GateVerdict`, `load_gate_verdict`, process-wide `cached()/set_cached()/reload()`, `effective_mode()`. |
| `app/sentinel/settings.py` | `SentinelSettings` + `validate()`, `load_settings(path)`. |
| `app/sentinel/store.py` | `ScoreEntry`, `FactorDecision`, `IngestResult`, `IngestRejected`, `ScoreStore` (thread-safe; `ingest`, `peek`, `keys`, `entries`, `effective_ttl`, `is_fresh`, `counts`, `last_as_of`, `curve_config`, `decide`, `would_be_factor`, `factor_for`). |
| `app/sentinel/client.py` | `SentinelClient` (sync httpx; `poll_once`, `run(stop)` with a `wake` event, `status`, latched alerts, `score_feed_log` rows). |
| `app/sentinel/storage.py` | §3.4 DDL, `log_feed_rows`, `record_decision`, `attach_trade`, `sha256`. |
| `app/sentinel/hook.py` | `EntrySizing`, `SentinelSizer` (`applies`, `size`, `sized_preview`, `record`, `attach_trade`; counters `blocked`, `hook_errors`, `log_failures`). |
| `app/sentinel/telemetry.py` | `entries`, `coverage`, `build_telemetry`, `write_telemetry` (atomic), `export` CLI. |
| `app/sentinel/runtime.py` | `SentinelRuntime(db_path, settings, consumer, alert, http, user_agent, config_path)`: per-thread connections, `start(stop)`, `coverage_job`, `reload`, `engage_kill`, `clear_kill`, `health`, `row(s)`, `ticket_check`. |
| `app/termdb.py` | `connect()` also runs `sentinel.storage.SCHEMA`. |
| `app/server.py` | `Ctx.sentinel` (bad config ⇒ disabled defaults), `Ctx.sentinel_alert`, started with the other threads; `/api/status.sentinel`; `GET /api/sentinel`, `GET /api/sentinel/{netuid}`, `POST /api/sentinel/{kill,resume,reload}`; the ticket advice line in `/api/workbench/checks`. |
| `config.example.toml`, `README.md` | `[sentinel]` block; "TaoSentinel scores" section. |

**Integration contract for v2 B1 (`tao-bot`).** Build one `SentinelRuntime(db_path, consumer="bot", alert=…)` and `start(stop)` it. For each entry candidate, after the strategy has chosen netuid and base size: `p, sz, did = rt.sizer.sized_preview(trader, TradeRequest(wallet, netuid, "buy", amount=base), strategy=tag)`; a `TradeError` means the base gates refused (no entry, exactly as without the sentinel); `p is None` means blocked; otherwise `trade = trader.execute(p.id, "buy")` and `rt.sizer.attach_trade(did, trade["id"])`. Exits call `Trader` directly. While both processes run, the terminal's runtime keeps showing scores; only the bot's runtime writes decisions.

### 8.3 Step order

1. **TaoSentinel**: nothing to build. After `sudo systemctl restart taosentinel` and the first scoring slot, run §10 T1–T3.
2. **Consumer Phase A (dormant)**: done 2026-10-18 (§9; 169 terminal tests green). Restart `tao-terminal` to load it: `systemctl --user restart tao-terminal`.
3. **Phase B** needs v2 B1 (the bot engine) to produce entries; until then `enabled = true` in the terminal gives feed liveness and ticket advice only.

### 8.4 Rollout ladder A → E

| Phase | Config state | Entry criterion | Verify | Exit criterion |
|---|---|---|---|---|
| **A dormant** | `enabled = false` | consumer merged (done) | `pytest -q` green incl. `test_byte_identical_amount_when_neutral`; `/api/status.sentinel.enabled=false`; §10 T1–T3 once 14's route is live | all §9 tests green on the laptop |
| **B observe** | `enabled = true`, `mode = "observe"` (terminal **and** bot runtimes) | A done; TaoSentinel Phases 8 + 11 live (scores written, route up); **v2 B1 running (paper is fine)** | `SENTINEL OBSERVE` log lines; `sentinel_decisions` rows with `sentinel_mode='observe'`; `fresh_count > 0`; survives restarts | telemetry `days ≥ 30`, `entry_coverage > 0.60` with `n_entries ≥ 20`, no open feed-down alert. Fewer entries ⇒ extend B. |
| **C gate** | unchanged | B exit, and ≥ 90 d of forward collection for the full composite (master §10) | `python -m app.sentinel.telemetry export --out ~/Documents/TaoData/data/quant/observe_telemetry.json`, then in TaoSentinel take a snapshot and `python -m app.quant report --run-all --factor quality --horizon-days 7 --observe-telemetry data/quant/observe_telemetry.json` → `data/quant/results/gate_verdict_quality_<ts>.json` | `gate_pass: true`, `pending: []`, `promotable: true`; otherwise **stay in B indefinitely at zero cost** (a valid outcome) |
| **D active (de-risk only)** | `mode = "active"`, `factor_ceil = 1.0`, verdict at `gate_verdict_path`, `POST /api/sentinel/reload` | C pass | `mode_effective="active"`, `gate="pass"`; every new decision's `amount_tao ∈ [max(0.5·base, 0.01), base]`; kill ⇒ next entry `x1.000 … (killed)`; TTL expiry ⇒ `(stale)` | 30 days without a kill-worthy incident; realised haircut reviewed |
| **E upsizing (optional)** | `factor_ceil = 1.25`, `q_ceil_at = 90` | D + a **second** verdict with `upsize_gate_pass: true` that is also `ok` | `ceil_effective == 1.25` only with that verdict; upsizing only when `confidence ≥ 0.7` and `red_flags == []` | — |

Live trading is a separate operator decision (v2 plan B6, "only when you say go"). The ladder runs on paper entries; nothing here enables live mode.

Emergency fallback at any phase: `touch ~/tao-terminal/SENTINEL_KILLED` (instant) or `enabled = false` + `POST /api/sentinel/reload`.

---

## 9. Tests (as built; `~/tao-terminal/tests/`, all offline)

`now` is injected; settings via `sentinel_util.settings(tmp_path, …)` and a mutable `Holder` (a reload); the kill file and verdict under `tmp_path`; HTTP via `httpx.MockTransport`; routes via `TestClient` on a stub context; the gate cache is reset by an autouse fixture in `tests/conftest.py`.

| Test file | Cases |
|---|---|
| `test_sentinel_models.py` (22) | schema pin; golden bodies (14 example, empty, **real producer output**, live capture if saved); nulls ⇒ `usable` False; unknown keys; 17 whole-batch rejects (netuid 1024, prefix mismatch, duplicate netuid, quality 100.1, confidence 1.1, tier E, `"Bad Flag"`, flag objects, offset-less `as_of`/`generated_at`, missing `spec_version`/`ttl_seconds`/`as_of`, TTL 0 and > 30 d, bad spec name); duplicate flags collapse; `--validate-url` exit 0/1 and `--save`. |
| `test_sentinel_factor.py` (15) | worked values; block flag regardless of confidence; null quality + block ⇒ `no_quality`; low/None confidence; ≤ 1.0 over 10 000 random inputs; velocity weight; monotone in Q; Phase E 1.25/1.125 and no upsize with a flag or c < 0.7; kill file; every gate `detail` incl. missing `promotable`/`upsize_gate_pass`; effective-mode table incl. spec binding; settings validation (no runtime-state validator, `factor_ceil=1.25` constructs), toml loading, unknown key refused. |
| `test_sentinel_store.py` (12) | inclusive per-row TTL boundary (`now = as_of + ttl` fresh, +1 stale) and `ttl_max_sec` cap; skew ±300/301 with `generated_at` → `computed_at` → `as_of` fallback, row `as_of` in the future; stale at receipt accepted; stale batch marks absent netuids and recovers; spec whitelist; monotonic ingest, equal `as_of` overwrites, occupant change replaces + event; null row replaces usable; empty batch; `counts()`; kill in observe and active; every §2.5 row incl. verdict for another spec; ceiling needs `upsize_ok`; disabled never touches the file system. |
| `test_sentinel_client.py` (8) | 200 ⇒ ingest + log rows, re-poll adds 0; URL carries `spec` and `include_insufficient`; ETag ⇒ `If-None-Match`, 304 ⇒ ok without ingest, never sent without an ETag; auth header only when set; 30 s gap; 401/503/timeout/bad JSON/future skew ⇒ one batch row each with its reason, latched alert at the threshold and on recovery; spec reject is not a failure (latched alert, `accepted=0` rows); log failure counted, store still updated; run loop idle when disabled, prime poll when enabled. |
| `test_sentinel_hook.py` (17) | byte-identical `amount_in` for disabled, no store, observe, killed, stale, missing, low confidence; disabled never calls the store; active + verdict ⇒ `round(base × f, 6)` through `execute`, decision row linked to the trade; active without verdict ⇒ observe; **never creates an entry** (per-trade cap and daily cap refused at base although 0.5 × base would pass, no decision row); 0.01 τ floor; store raising and logger raising ⇒ `hook_error`, base amount; re-preview failure ⇒ base preview; opt-in block (active only, observe never blocks); strategy tags; sells refused; rows survive reopen. |
| `test_sentinel_telemetry.py` (3) | window 29.9 d in / 30.1 d out; off / failed / unexecuted / blocked / other-spec rows excluded; coverage 0.75; applied 1.0 for observe, `amount/base` for active; `since`/`days`; empty ⇒ coverage null; atomic write; export `--out -` exit 0, missing DB exit 2. |
| `test_sentinel_api.py` (5) | `/api/sentinel` rows (`fresh`, `expires_at`, `would_apply_factor`, `applied_factor`, `reason`); 404 for an unknown netuid; no ingest route; kill/resume/reload need the terminal token; reload picks up config + verdict and keeps old settings on a bad section; ticket advice line levels; coverage job writes the telemetry file. |

TaoSentinel: `tests/api/test_reads.py::test_scores_latest_killed_and_empty` now also asserts that the TTL passing alone changes the ETag (fails without the fix).

---

## 10. Verify checklist

**TaoSentinel (14's route, checked from the consumer's side; needs `sudo systemctl restart taosentinel` and a first scoring slot)**
- T1 `curl -s 'http://127.0.0.1:8090/api/v1/scores/latest?spec=ts-1.0&include_insufficient=1' | jq '{spec_version, as_of, generated_at, ttl_seconds, stale, n:(.scores|length), rf:(.scores[0].red_flags|type)}'` → `"ts-1.0"`; `as_of` ends `T00:00:00Z`/`T06…`/`T12…`/`T18…`; `ttl_seconds` 25200; `stale` false; `n` ≥ 100; `rf` `"array"`.
- T2 `cd ~/tao-terminal && .venv/bin/python -m app.sentinel.models --validate-url 'http://127.0.0.1:8090/api/v1/scores/latest?spec=ts-1.0&include_insufficient=1' --save tests/fixtures/scores_latest_live.json` → exit 0 (the fixture is then validated by `test_golden_bodies`).
- T3 `touch ~/Documents/TaoData/SCORING_KILLED; curl -s '…/latest?spec=ts-1.0' | jq .stale` → `true`; after `rm` → `false`.

**Consumer Phase A (`enabled = false`)**
- A1 `cd ~/tao-terminal && .venv/bin/python -m pytest -q` → all pass (169 on 2026-10-18).
- A2 `systemctl --user restart tao-terminal; curl -s http://127.0.0.1:8787/api/status | jq .sentinel` → `{"enabled": false, "mode": "observe", "mode_effective": "off", …}`.
- A3 `.venv/bin/python -c "import sqlite3; c=sqlite3.connect('data/terminal.db'); print([r[1] for r in c.execute('PRAGMA table_info(sentinel_decisions)')]); print(c.execute('SELECT COUNT(*) FROM score_feed_log').fetchone())"` → the §3.4 columns; a count (0 while disabled).

**Feed liveness in the terminal (`enabled = true`, `mode = "observe"`, then `POST /api/sentinel/reload` with the page token, or a restart)**
- L1 within a minute: `curl -s http://127.0.0.1:8787/api/sentinel | jq '[.scores[] | select(.fresh)] | length'` → ≥ 100, equal to `.health.fresh_count`.
- L2 `score_feed_log` grows by about 130 accepted rows per new slot `as_of` (4 per day), not per poll.
- L3 the ticket's Checks for a buy show the `TaoSentinel: …` line.
- L4 `systemctl stop taosentinel` for longer than `ttl_seconds` (or set 14's `SCORE_FEED_TTL_SEC=60`) → `stale_count` = the former `fresh_count`, `consecutive_failures` climbs, one desktop alert at 8 and one on recovery.

**Phase B (needs v2 B1)**
- B1 `journalctl --user -u tao-bot | grep 'SENTINEL OBSERVE' | tail -3` shows `would x0.xxx (q=…, reason=curve)`; `touch SENTINEL_KILLED` ⇒ next entry `would x1.000 … reason=killed`; `rm` restores.
- B2 coverage = `python -m app.sentinel.telemetry export --out - | jq 'del(.entries)'` → `entry_coverage > 0.60`, `n_entries ≥ 20`, `days ≥ 30`; the same numbers at `/api/status.sentinel.entry_coverage_30d` / `entry_count_30d`.

**Phase D**
- D1 `cp ~/Documents/TaoData/data/quant/results/gate_verdict_quality_<ts>.json ~/tao-terminal/data/sentinel_gate_verdict.json`, set `mode = "active"`, reload → `gate == "pass"`, `mode_effective == "active"`, `ceil_effective == 1.0`.
- D2 `SELECT netuid, base_tao, amount_tao, sentinel_factor, sentinel_mode, sentinel_reason FROM sentinel_decisions ORDER BY id DESC LIMIT 5` → every `active` row has `max(0.5·base, 0.01) ≤ amount_tao ≤ base_tao`.
- D3 `touch SENTINEL_KILLED` → next entry `SENTINEL APPLIED … x1.000 … (killed)` with no restart.

---

## 11. Open questions / operator decisions

1. ~~TTL vs cadence~~ — **resolved**: 14 ships `SCORE_FEED_TTL_SEC = 25200`.
2. ~~Feed read auth~~ — resolved: open read tier (master §9).
3. ~~Spec 13 verdict~~ — resolved: 13 emits `promotable`, `upsize_gate_pass`, and reads `sentinel-telemetry-1` (verified against `app/quant/gate.py` 2026-10-18).
4. ~~Spec 03 column types~~ — resolved by 02 §B1/§B6.
5. ~~Host/port/token names~~ — resolved (14 §5; terminal `:8787`).
6. **Which v2 strategies are sized.** v2 B1 picks its strategies after the B2 walk-forward. The hook is strategy-agnostic; use `strategy_tags` if only some should be modulated.
7. **Audit columns.** `sentinel_decisions` adds `base_tao`, `amount_tao`, `sentinel_confidence`, `sentinel_tier`, `sentinel_red_flags` and `trade_id` beyond the brief's six; confirm.
8. **Velocity.** Carried, logged, weight 0.0; earns weight only through its own verdict (factor `velocity`) and a `gate.py` change.
9. **Quality vs composite** as the curve input: revisit if `composite_smoothed` passes the gate and quality does not (needs a `gate.py` change; 13 already marks it promotable).
10. **Provisional runs.** `run_kind` is logged; add an opt-in "official rows only" filter if Phase B shows provisional rows flipping tiers.
11. **Pre-launch occupants** are excluded by 14's default ⇒ neutral.
12. **Stale-at-receipt** is accepted with a warning (parent spec: 422); confirm.
13. **Confidence threshold** 0.4 reused from master §7.7; confirm.
14. ~~Telegram token sharing~~ — moot: the terminal alerts on the desktop. If v2 later adds Telegram it needs its own bot token (two `getUpdates` pollers on one token get 409).
15. **Shadow series after a promotion.** With `spec=ts-1.0` pinned, the consumer keeps receiving ts-1.0 shadow rows after a ts-1.1 promotion (14 does not filter `scores.shadow`), and an active ts-1.0 verdict keeps applying. Moving to ts-1.1 = new URL, whitelist entry, Phase B and verdict.
16. **Paper entries in c7** — *new.* Counted (§2.7), because c7 measures signal availability at entry time. If you want c7 to wait for live entries, the query gains `t.mode = 'live'`.
17. **Minimum-trade floor** — *new.* A halved order below 0.01 τ is lifted to 0.01 τ rather than skipped (skipping would be a de-risk beyond the 0.5 floor). With v2's planned 0.5 τ orders it never binds.
18. **Terminal vs bot consumer** — *new.* Until B1 the terminal is the only consumer. Once `tao-bot` runs, both poll (2 × 96 requests/day on loopback, mostly `304`). If one poller is preferred, the bot could read the terminal's `/api/sentinel` instead, at the cost of making the terminal a dependency of the bot; not recommended.

---

## 12. Sources

- `~/Documents/TaoData/specs/02-cross-cutting-conventions.md` §A2, §B1, §B6, §B12, §F2, §H1, §I2 (binding).
- `00-MASTER-SPEC.md` §1.3, §2, §5, §6, §7.5, §7.7, §9, §10, §11, §12.
- `14-api-telegram-and-ui.md` §3.5, §5; code `app/api/scores.py` (`feed_row`, `build_feed`, `etag`), `app/api/queries.py latest_scores/feed_meta`, `app/config.py SCORE_FEED_TTL_SEC = 25200` — read 2026-10-18.
- `12-feature-store-and-scoring-engine.md` §2.2; `app/scoring/spec.py FLAGS_TS_1_0`.
- `13-validation-harness.md` §2.7, §3.6, §8.7; code `app/quant/gate.py` (verdict keys, c7 rule), `app/quant/__main__.py load_telemetry`, `app/quant/backtestability.py PROMOTABLE_FACTORS`.
- `16-ops-deploy-and-observability.md` restore and spec-promotion runbooks.
- `19-investment-workbench.md` §0.1b, §2.9 (the terminal as the operator UI; `WorkbenchClient` idiom).
- `~/tao-terminal/docs/SUBNETTRADER_V2_PLAN.md` (v2 inside the terminal, phases B0–B7, paper first); `~/tao-terminal/app/trading.py` (`Trader.preview/execute`, `check_limits`), `app/config.py` (`_section`), `app/server.py` (security middleware, `Ctx`), `app/termdb.py`, `app/db.py log`.
- `research/01-prior-sentiment-design.md`, `research/01-factor-validation-tooling.md` §5, §8 (fail-safe rule, TTL inclusivity, rollout ladder).
- Superseded host: `E:\SN_Trader\SubnetTrader` (`ema_manager.py`, `db.py`, `config.py`, `main.py`, `value_kill.py`, `api_auth.py`; anchors as of HEAD 2026-06-29) — the 2026-09-24 draft, kept in git history.

## Changelog
- 2026-09-23 reconciliation + review: producer side withdrawn (H1); ISO model with null-neutral rows; per-row TTL; monotonic ingest; INTEGER epoch tables; hook moved after every base gate; gate aligned to 13 §3.6; ceiling range-check only; c7 telemetry file; unconditional jobs for hot reload.
- 2026-09-24 final pass: `promotable` required for `ok`; verdict file `gate_verdict_<factor>_<ts>.json`; Phase E unblocked (`upsize_gate_pass`); c7 telemetry is the `sentinel-telemetry-1` JSON export.
- 2026-10-18 review + build (operator: "improve it, then implement it"):
  - Retargeted from the old SubnetTrader (not revived) to the v2 bot inside `~/tao-terminal` (§0 table): `Trader`-level hook via `sized_preview` (base preview → factor → re-preview), `sentinel_decisions` audit with `base_tao`/`amount_tao`/`trade_id`, `[sentinel]` toml config with `POST /api/sentinel/reload`, terminal status/routes/alerts, ticket advice line, per-consumer `score_feed_log`, no systemd ordering.
  - Bugs fixed: 14's ETag ignored `stale` (304 hid a TTL flip); `INSERT OR IGNORE` dropped NOT NULL-violating log rows; halved orders could fall under the 0.01 τ chain minimum; `ttl_seconds > 86400` rejected the whole batch.
  - Q1 resolved (TTL 25 200); Q14 moot; new Q16–Q18. Contract checked against a real 14 body (fixture `scores_latest_producer.json`) and 13's code.
  - Built: 11 modules, 82 tests (169 terminal tests green), TaoSentinel API tests green.
