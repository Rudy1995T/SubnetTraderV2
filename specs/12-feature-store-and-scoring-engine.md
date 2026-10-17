# 12 — Feature store and the `ts-1.0` scoring engine

**Status:** proposed (reconciled to 02 on 2026-09-23) · **Phase:** 8 · **Binding tie-breaker:** `02-cross-cutting-conventions.md` (ids B1–J2 cited below) › master › this spec for the metric registry (02 §A2) · **Depends on:** 01 (Settings, `JOB_TABLE` ids `features_hourly`/`scoring_6h`, `CollectorJob`, `AlertLatch`, `/health`, `app/util/time.py`), 03 (owns **all** DDL and both catalogs: `metric_values` with `stage ∈ {raw, norm}` and the generated `status` column, `scores`, `events`, `red_flag_state`, `regime_daily`, `rank_stability_daily`, `score_runs`, `manual_flags`; the single writer `put_metric_values` and the reader `metric_at` — 02 §B3–B7; this spec ships no DDL), 04 (`occupants`, `subnets`, `watch_channels`, identity events), 05 (P1 + aux flag inputs, `chain_hourly` for regime), 06 (P2), 07 (P3 + site events), 08 (P5 Discord/TG feature fns `app/features/community.py`), 09 (P5 X legs), 10 (P6), 11 (P4 pre-normalised values, raw `tenure` days + trigger aux, written by its `integrity_features` job), 14 (`manual_flags`, alert routing, rescore endpoint) · **Consumers:** 13 (PIT panel), 14 (API/Telegram/UI), 15 (feed) · **Master spec refs:** §1.3, §2 (rows `quality.py`, `regime.py`, `utils/math.py`), §3, §5, §6, §7.1–§7.7, §9, §10, §11, §12

All numbers in this spec that define `ts-1.0` (weights, normaliser parameters, flag multipliers, thresholds, EMA span, regime parameters) are **code constants frozen in `app/scoring/spec.py` / `app/features/registry.py`**, never config (spec 01 §5). Changing any of them is a new `spec_version`.

---

## 1. Purpose and scope

**In scope (Phase 8 deliverable):**
1. The **metric registry** `app/features/registry.py`: one declarative, import-validated row per `ts-1.0` metric — 45 scored metrics (P1…P6), 9 *legs* (inputs to derived metrics) and 8 *aux* flag inputs — with pillar, weight, normaliser + params, freshness window, producing spec, producer kind and absence rule.
2. The **normalisers** `app/features/normalise.py` (exact code, §2.5).
3. The **feature store** contract: `stage='raw'` `metric_values` rows (spec 03's table and writer, read by this spec), the normalised values this spec writes as `stage='norm'` rows of the same table (02 §B3), the point-in-time (PIT) row-selection rule, the derived metrics (`announcement_cadence_30d`, `silence_days` — 02 §C3; the `kol_coverage_30d` fallback), and the hourly `features_hourly` job.
4. The **scoring engine** `app/scoring/` run by `scoring_6h` (00:15Z official, 06/12/18:15Z provisional): pillar kernel with coverage renormalisation, Health, Velocity, regime-scaled `v_w`, `composite_raw`, red-flag gates with floor and hysteresis, `composite`, EMA smoothing over official rows, confidence, tiers with ±3 hysteresis, `insufficient_data`, `pre_launch`, explain JSON, events, immutable `scores` rows, catch-up after power loss.
5. Ports: `app/quant/regime.py` (old `strategy/regime.py`) and `app/util/mathutil.py` (old `utils/math.py:1-131`) — created here in Phase 8 because scoring needs them; spec 13 later extends `mathutil.py`.
6. Rank-stability monitors, the `spec_version` mechanism with shadow runs, the rescore backend called by spec 14.
7. A golden worked example (SN64 and a tail occupant) that is also the golden test.
8. The PIT contract spec 13 consumes and the read contract for 14/15.

**Out of scope:** collecting or computing producer metrics (05–11 own them; this spec consumes the raw rows exactly as spec 03 stores them and as 08/11 define their P5/P4 values — §2.3, §2.4); the DDL, the metric-key catalog and the events catalog (03, 02 §A2/B7/D1); Telegram delivery and alert routing (14); the feed (served by 14 §3.5 `GET /api/v1/scores/latest`, consumed unchanged by 15, 02 §H1); factor validation (13); re-weighting (Phase C, master §10); UI.

---

## 2. Design

### 2.1 Components and data flow

```
 producers (collector jobs)                        producer feature fns (called by FeatureEngine)
 05 chain_features_job, 06 github metrics,         08 app/features/community.py (compute_<key>)
 07 web_metrics, 11 integrity_features             09 app/features/social_x.py, social_feeds.py
        │ put_metric_values (02 §B4)                10 app/features/sentiment.py
        ▼                                                       │ put_metric_values
 ┌─────────────── metric_values stage='raw' (spec 03 DDL; first success per key wins, 02 §B4) ───────────────┐
 └────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        ▲ derived raw rows (announcement_cadence_30d, silence_days)
        │
 features_hourly (every 60 min): FeatureEngine.compute_registered(T) + derive(T)   (as_of key = floor_hour(T), provisional unless 00:00Z)
 scoring_6h (cron hh:15Z, hh ∈ SCORING_CRON_HOURS):
   0 plan slots: missed slots first (catch-up, oldest first), then the current slot
   for each slot, for each spec in [SPEC_VERSION, *SCORING_SHADOW_SPECS]:
   1 compute_registered(T_eval) + derive(T_eval)    (once per slot, shared by all specs; rows keyed as_of = slot)
   2 FeatureEngine.snapshot(spec, slot, cutoff, mode)  → Snapshot (normalised m per occupant×metric, rankg cohorts)
   3 regime = classify(chain_hourly, 24 h window)   → v_w
   4 ScoreCalculator: pillars → Health; P6 + blend → Velocity; composite_raw
   5 RedFlagEngine: triggers → state machine → flag_multiplier (floor 0.25)
   6 EMA + tier hysteresis from the previous official scores row
   7 explain; ONE transaction: metric_values stage='norm', scores, red_flag_state, regime_daily, events, score_runs
   8 official + primary only: rank-stability monitors; prune provisional stage='norm' rows > 14 d
```

The engine supports both producer kinds (§2.3): collector jobs that write their own raw rows (05, 06, 07, 11) and read-only feature fns it calls and whose results it writes on their behalf (08, 09, 10).

### 2.2 Time model (binding)

| term | definition |
|---|---|
| encoding | Every timestamp is **INTEGER unix seconds, UTC** (02 §B1; helpers `now_ts`, `floor_hour`, `floor_day`, `to_iso`, `parse_iso`, `parse_ts` from `app/util/time.py`). The run's computation instant `computed_at` (= `T0`) is stored in `scores.computed_at`, `regime_daily.computed_at`, `rank_stability_daily.computed_at` and as the `observed_at` of the run's `stage='norm'` rows; `scores.input_cutoff` stores `cutoff` (02 §B3, §B6 — `scores` and `regime_daily` have no `observed_at`). |
| slot | A scoring instant `hh:00Z` with `hh ∈ SCORING_CRON_HOURS` (default 0,6,12,18). The run at `hh:15Z` scores slot `hh:00Z`. |
| official / provisional | Slot hour `== SCORING_OFFICIAL_HOUR` (pinned to 0, 02 §B6) → `run_kind='official'`, else `'provisional'`. |
| `as_of` (score) | The slot instant. The official score for 2026-09-23 has `as_of = 1790121600` (2026-09-23T00:00:00Z). |
| `T0` / `computed_at` | Wall-clock start of the run (`RunContext.started_at`). |
| `cutoff` | Latest `observed_at` admitted as input. **strict** mode: `cutoff = T0`. **reconstructed** mode: `cutoff = slot + SCORING_CRON_MINUTE·60` (the nominal run time), and rows with `backfilled = 1` are admitted regardless of `observed_at`. |
| mode choice | strict iff `T0 − (slot + SCORING_CRON_MINUTE·60) ≤ SCORING_LATE_GRACE_MIN·60`; otherwise reconstructed. Every reconstructed score row carries `backfilled = 1`. |
| catch-up | At the start of each run: `last_done` = the latest slot with a primary-spec `score_runs` row that is `ok = 1` or `killed = 1` (a failed `ok = 0` attempt does not count, so it is retried). The official slots strictly between `last_done` and the current slot are missed; the most recent `SCORING_CATCHUP_MAX_SLOTS` of them are scored oldest-first in reconstructed mode before the current slot (older gaps stay empty). Missed provisional slots are skipped (superseded). No `last_done` (first ever run) → no catch-up. |
| raw `as_of` key | 02 §B3/§B4: a row is provisional iff `as_of % 86400 ≠ 0` (there is no `is_provisional` column); producers key a row by the scoring slot it serves — `00:00Z` (official) or `06/12/18:00Z` (provisional) — and hourly producers use the hour bucket. 05 and 11 write the 00:00Z official row and 06/12/18:00Z provisional rows; 06/07 write the 00:00Z row; rows this spec writes (08/09/10 fn results, derived metrics) use `as_of = slot` inside a scoring run (so a strict run up to `SCORING_LATE_GRACE_MIN` late still keys its own slot) and `as_of = floor_hour(now)` in `features_hourly` — so the official run (or a `features_hourly` run in the 00:xx hour) writes the official day row and other runs write provisional hour rows. |
| age | `age_s = slot − raw.as_of` (clamped at 0). A raw row is **fresh** iff `age_s ≤ freshness_s` (inclusive). Stale rows are not used (no forward-fill) and count as missing for coverage. |

**Raw-row selection (PIT rule; the semantics of 02 §B5 `metric_at`, evaluated with the `as_of` bound at `slot` and the `observed_at` bound at `cutoff`).** For scoring spec `S` at `slot` with `cutoff` and `mode`, the candidate rows for `(occupant_key, metric_key)` are those with `stage = 'raw'`, `spec_version = S.raw_spec_version`, `slot − freshness_s ≤ as_of ≤ slot`, and (`observed_at ≤ cutoff` **or** (`mode='reconstructed'` **and** `backfilled = 1`)). The selected row is the candidate with the greatest `as_of` whose `status ∉ {'failed', 'none'}` (generated `none` — no `value_json.status` key — is read as `failed`, B3; a failure is *unobserved*, so an older observation inside the freshness window is used — this is the window doing its job, not a forward-fill: nothing older than `freshness_s` is ever used). If every candidate is `failed`, the metric is `None` with the latest failed row's `reason`; no candidate at all → `None` with `reason='no_fresh_row'`.

Registered fns and derived metrics computed inside a run are evaluated at instant `T_eval = cutoff` (they read only rows with `observed_at ≤ T_eval` — their own PIT contract, specs 09/10) and their rows are stamped `observed_at = T_eval`, so they pass the selection rule of the same run. Rows written later by other jobs (`observed_at > cutoff`) are invisible to that run.

Raw rows follow 02 §B4 (first success wins): a row that is `ok`, `absent` or `not_applicable` for `(occupant_key, metric_key, as_of, spec_version, stage='raw')` is never replaced, so a later recomputation for the same key is silently ignored; only a `failed` row may be replaced by a later observation, which carries the later `observed_at`. Intraday refreshes therefore land on provisional hour keys, and a PIT replay sees exactly what a live run saw. The exact inputs a score used are also preserved in the slot's `stage='norm'` rows and `scores.explain` (§2.6, §2.10).

### 2.3 Raw-row contract (every producer)

A raw row is spec 03's `metric_values(occupant_key, metric_key, as_of, spec_version, stage='raw', value, value_json, observed_at, source, run_id, backfilled)` (03 §3.6; key `(occupant_key, metric_key, as_of, spec_version, stage)`), written **only** through the single writer `app/storage/pit.py::put_metric_values(rows)` (02 §B4; `upsert_metric_values` is its alias, e.g. spec 05's call sites). The writer validates every `metric_key` against 03's `METRIC_KEYS` catalog and applies first-success-wins: an `ok`/`absent`/`not_applicable` row is never replaced; a `failed` row may be replaced by a later observation. Spec 05's `INSERT OR REPLACE` draft must not be used (a REPLACE deletes the official row and 03's delete trigger aborts it). `spec_version` on raw rows is the **metric-definition version** (`ts-1.0`). `source` is the producing spec's single value (02 §B9).

**`status` lives in `value_json.status`** (02 §B3): the table's `status` column is **generated** from it and producers never write a `status` column. Vocabulary `ok | absent | failed | not_applicable`; a missing key generates `none`, which this engine reads as `failed`. 03's CHECKs: `ok` ⇒ `value IS NOT NULL`; `failed`/`not_applicable` ⇒ `value IS NULL`. **`absent` must be explicit.** `value_json.reason` (short token, e.g. `no_github_url`, `repo_gone`, `no_token`, `pre_launch`, `invite_invalid`) is recommended for every non-`ok` row. `value_json.status` is reserved for this enum; domain enums use other keys (07's product enum is `value_json.product_status`).

| status | meaning (master §1.3) | normalised `m` |
|---|---|---|
| `absent` | verifiable absence (no repo, no website, anonymous team, no venue) — `value` may be `0.0` or `NULL` | **0.0** regardless of normaliser |
| `ok` | a real observation, including a real zero (emission 0, 0 commits) | normaliser(`value`, `value_json`) — may still be `None` if a multi-input normaliser lacks its decisive input |
| `failed` | collection failure, not yet observed, producer disabled | **None** (dropped, lowers confidence) |
| `not_applicable` | metric undefined for this occupant (0 merged PRs → `review_practice_90d`) | **None** |

**Registered producer fns** (02 §C5; specs 08/09/10 today, and any 07/11 fn registered the same way; collectors 05/06/07/11 otherwise write their own rows):

```python
# app/features/types.py
@dataclass(frozen=True, slots=True)
class MetricResult:
    value: float | None
    value_json: dict = field(default_factory=dict)
    absence_reason: str | None = None        # not None → value_json.status='absent', value_json.reason = absence_reason

# Contract (02 §C5), one coroutine per metric key:
#   async def compute_<metric_key>(db, occupant_key: str, as_of: datetime) -> MetricResult
#   as_of = this engine's T_eval as a tz-aware UTC datetime; every read uses observed_at <= as_of.
#   Verifiable absence: return MetricResult(0.0, value_json, absence_reason='<reason>')
#   (e.g. 'no_x_account', 'x_account_gone', 'no_website', 'no_venue').
ProducerFn = Callable[[Database, str, datetime], Awaitable[MetricResult]]
```

Return mapping (into `value_json.status`, which the engine sets before writing): `absence_reason` set → `absent` (value as returned, normally `0.0`; `value_json.reason = absence_reason`); `value=None` → `not_applicable` if `value_json.get("status") == "not_applicable"` else `failed`; otherwise `ok`. A return that is not a `MetricResult` is written as `failed` with `reason='bad_return'` (logged once per fn and run). X absence follows 02 §C3: no team X handle, or a gone account, gives `x_followers = 0` and absent X legs (`absence_reason` `no_x_account` / `x_account_gone`). A fn that raises or exceeds `FEATURE_FN_TIMEOUT_SEC` produces `failed` rows with `reason='fn_error'`/`'fn_timeout'` for the affected occupants; it never aborts the run. A fn whose module does not import yet (later phase) produces no rows (`reason` logged once as `producer_unavailable`).

### 2.4 The metric registry (`app/features/registry.py`)

```python
@dataclass(frozen=True, slots=True)
class MetricDef:
    key: str                       # ^[a-z0-9_]{3,48}$
    pillar: str | None             # "P1".."P6"; None for legs/aux
    weight: float                  # within-pillar weight; 0.0 for legs/aux
    normaliser: str                # key of NORMALISERS; "none" for legs/aux
    params: Mapping[str, float]    # frozen normaliser parameters (MappingProxyType)
    freshness_days: float
    source_spec: str               # "05".."11", or "12" for derived
    producer: str                  # "collector" | "fn" | "derived"
    role: str = "scored"           # "scored" | "leg" | "aux"
    fn: str | None = None          # dotted "module:callable" of a producer fn (lazy import)
    fallback_key: str | None = None
    absence_rule: str = ""         # human-readable mirror of the producer's absence table
    @property
    def freshness_s(self) -> int: return int(self.freshness_days * 86400)
    @property
    def cross_sectional(self) -> bool: return self.normaliser == "rankg"

PILLAR_WEIGHTS: Final = MappingProxyType({"P1": 30.0, "P2": 20.0, "P3": 10.0, "P4": 18.0, "P5": 12.0})  # Health; P6 → Velocity only
METRICS: Final[Mapping[str, MetricDef]]          # built from _TABLE below, validated at import
PILLAR_OF: Final[Mapping[str, str | None]]       # metric_key → pillar (spec 14 reads this)
def scored(pillar: str) -> tuple[MetricDef, ...] # registry order
def register_metric(key: str, *, fn: ProducerFn, pillar: str | None = None,
                    freshness_days: float | None = None, **declared) -> None
    # 02 §C5 registration entry point for producer fns (08/09/10 today; 07/11 if they register fns).
    # (a) key in METRICS → attaches fn (overrides the dotted path of the table).
    # (b) key is an *evidence* key of 03's METRIC_KEYS catalog (w 0, not in METRICS: e.g. 08's discord_topology,
    #     tg_members, inflated_days; 09's video_uploads_90d, video_mentions_30d, blog_posts_90d) → stored in
    #     EVIDENCE_FNS; compute_registered calls it and writes its raw rows; it is never normalised or scored.
    # (c) anything else → RegistryError. Never adds keys to METRICS.
    # Checks when the argument is not None: pillar == row.pillar, or for a leg the pillar of the metric
    # it feeds (09 passes pillar="P5" for x_cadence_30d); freshness_days == row.freshness_days;
    # declared["weight"] == row.weight (0 for legs, aux and evidence) and declared["component_of"] names the
    # consuming derived metric. declared["normaliser"] is ignored (this registry is authoritative). Mismatch → RegistryError.
def register_many(rows: Iterable[Mapping]) -> None   # spec 08's REGISTRY list-of-dicts → register_metric per row
EVIDENCE_FNS: Final[dict[str, ProducerFn]]           # (b) above; outside registry_hash
def registry_hash() -> str      # sha256 of the canonical JSON of _TABLE (fn paths excluded)
```

**The table (ts-1.0)** = master §7.3 plus 02 §C1–C7, exactly. `rankg` = cross-sectional over active occupants at the slot (§2.5). Freshness days are frozen per 02 §C6 (= master §7.7): chain 1; GitHub 2; web 2; community fn metrics 2; 12-derived P5 combiners 1 (recomputed hourly); X 3; P6 1; `team_doxxed` 90; the other P4 metrics (`owner_identity`, S1–S5, `tenure`) and the two owner trigger aux rows 2 (11 writes them for the 00:00Z official slot and the 06/12/18:00Z provisional slots; 2 d tolerates one missed day). C6's 8-d window for weekly-rechecked main-server Discord channel evidence is applied by 08's fns when they select that evidence (08 §2.4), not to a registry row. **P4** (02 §C1): `team_doxxed`, `owner_identity`, `owner_commitment`, `owner_retention_30d`, `control_stability_90d` and `owner_overhang` arrive as final [0,1] scores in `value` and are scored through `prenorm`; their `value_json` carries the raw inputs under the keyword names of the §2.5 reference function (listed per row); `tenure` arrives as raw days.

| # | metric_key | P | w | normaliser (params) | fresh d | src | producer | absence → 0.0 / failure → None (producer decides; summary) |
|---|---|---|---|---|---|---|---|---|
| 1 | `emission_share` | P1 | .20 | `rankg` | 1 | 05 | collector | real 0 is *ranked* (status ok) / no 00:00Z row, field null |
| 2 | `gate_margin` | P1 | .15 | `zclip_log2(lo=-2, hi=1)` | 1 | 05 | collector | ratio 0 → 0.0 / not eligible, fit missing, θ undefined |
| 3 | `dereg_safety` | P1 | .15 | `identity` (custom, computed by 05) | 1 | 05 | collector | bottom-3 non-immune → raw 0.0 ok / moving price null, reg block unknown |
| 4 | `liquidity_depth` | P1 | .10 | `rankg` (master "rankg(log)": log is rank-invariant, applied to raw τ) | 1 | 05 | collector | drained pool ranked lowest / null, units unresolved |
| 5 | `exit_capacity` | P1 | .05 | `zclip(lo=0.05, hi=0.6)` | 1 | 05 | collector | never / price ≤ 0, not started |
| 6 | `net_flow_30d` | P1 | .10 | `zclip(lo=-0.5, hi=0.5)` | 1 | 05 | collector | real 0 / too few daily rows |
| 7 | `chain_buys` | P1 | .05 | `rankg` | 1 | 05 | collector | `excess_tao=0` real 0 ranked / null, mcap not computable |
| 8 | `holder_dispersion` | P1 | .05 | `zclip(lo=0.3, hi=0.9)` | 1 | 05 | collector | never / no holders snapshot ≤ 14 d |
| 9 | `participation` | P1 | .05 | `zclip(lo=0.2, hi=0.9)` on (active validators + active miners) ÷ max_uids **only** (02 §C7; `vtrust_dispersion` is evidence, w 0) | 1 | 05 | collector | — / no metagraph source (keyless default) |
| 10 | `miner_burn_ok` | P1 | .10 | `zclip(lo=0.0, hi=0.8)` | 1 | 05 | collector | fully burned → raw 0.0 ok / deep tail, fit degraded, not eligible |
| 11 | `active_authors_28d` | P2 | .20 | `logcap(cap=10)` | 2 | 06 | collector | P2-wide: no GitHub ref / placeholder / gone beyond grace → absent / no token, activity stale |
| 12 | `fte_devs_84d` | P2 | .15 | `logcap(cap=8)` | 2 | 06 | collector | as 11 |
| 13 | `commit_days_90d` | P2 | .15 | `logcap(cap=90)` | 2 | 06 | collector | as 11 |
| 14 | `declared_repo_freshness` | P2 | .10 | `recency(full_at=14, zero_at=180)` | 2 | 06 | collector | `reason='repo_gone'` → absent (also drives `repo_gone` flag) / verify not ok ≤ 2 d |
| 15 | `org_freshness` | P2 | .10 | `recency(full_at=7, zero_at=90)` | 2 | 06 | collector | as 11; `value_json.{all_archived, declared_archived, successor_repo_id}` drive `all_repos_archived` |
| 16 | `release_cadence_90d` | P2 | .05 | `logcap(cap=6)` | 2 | 06 | collector | as 11 |
| 17 | `review_practice_90d` | P2 | .05 | `identity` | 2 | 06 | collector | as 11; 0 merged PRs → `not_applicable` (None) |
| 18 | `bus_factor` | P2 | .10 | `bus_factor` (≥3→1, 2→.6, 1→.3, 0→0) | 2 | 06 | collector | as 11 |
| 19 | `repo_hygiene` | P2 | .05 | `checklist` (`value_json.items` if present) | 2 | 06 | collector | as 11; < 3 of 5 items observed → failed |
| 20 | `originality` | P2 | .05 | `identity` (raw is already `1 − template share`) | 2 | 06 | collector | as 11 |
| 21 | `site_uptime_30d` | P3 | .30 | `identity` | 2 | 07 | collector | no website → absent / < 3 definitive check days |
| 22 | `tls_ok` | P3 | .10 | `binary` | 2 | 07 | collector | expired/mismatch → raw 0 ok / handshake never evaluated |
| 23 | `site_age` | P3 | .10 | `age_ramp(young_at=30, mature_at=365, floor=0.2)` | 2 | 07 | collector | never / no website, Wayback never answered |
| 24 | `site_freshness` | P3 | .15 | `recency(full_at=90, zero_at=365)` | 2 | 07 | collector | never / SPA shell, robots HEAD-only |
| 25 | `docs_present` | P3 | .15 | `binary` | 2 | 07 | collector | no docs link on a parsed landing page → 0 / site down |
| 26 | `product_status` | P3 | .20 | `identity` (07 writes the mapped none 0 / waitlist .3 / beta .6 / live 1; the enum itself is `value_json.product_status`, 02 §B3) | 2 | 07 | collector | manual `none` → 0 / unset → None |
| 27 | `team_doxxed` | P4 | .25 | `prenorm` (11 writes the final map value 1.0/.6/.3/0.0; `value_json.{n_verified, n_named, disclosure}`) | 90 | 11 | collector | anonymous disclosure (`_ANON_TOKENS` port, 11 §2.2.6) → 0.0 written by 11 / no record, placeholder, all rejected → None |
| 28 | `owner_identity` | P4 | .05 | `prenorm` (02 §C1; 11 writes 1.0/0.0; `value_json.{owner_coldkey, name, url, github}`) | 2 | 11 | collector | no coldkey identity row, or name without url/github → 0.0 / coldkey unknown, identities never fetched, owner-key mismatch |
| 29 | `owner_commitment` | P4 | .20 | `prenorm` (11 writes final S2; `value_json.{lock_ratio, auto_lock, commit_pct, perpetual_locked, age_days}`; reference formula §2.5) | 2 | 11 | collector | zero lock ∧ auto off ∧ age > 90 d → 0.0 / perp or total unavailable, `grace_period` |
| 30 | `owner_retention_30d` | P4 | .20 | `prenorm` (11 writes final S1; `value_json.{sell_ratio, max_7d_sell_impact, full_exit, owner_cut_enabled, earned_alpha}`) | 2 | 11 | collector | 02 §C1 order: a hard trigger (full-exit extrinsic, any 7-d sell impact > 2 % of pool TAO, `sell_ratio > 1`) → 0.0 `ok` first; only then `owner_cut_enabled = false` → 1.0 / pre-launch, activity coverage short, earned unknown |
| 31 | `control_stability_90d` | P4 | .15 | `prenorm` (11 writes final S3; `value_json.{events: [{kind, event_at, whitelisted}], observed_since}`) | 2 | 11 | collector | non-whitelisted `perpetual_off` in 90 d → 0.0 / no owner snapshot ≤ 36 h |
| 32 | `owner_overhang` | P4 | .05 | `prenorm` (11 writes final S5 with the −0.2 legacy adjustment; `value_json.{dump_impact, legacy_unaccounted}`) | 2 | 11 | collector | dump_impact ≥ 25 % → 0.0 / owner position or pool unknown |
| 33 | `tenure` | P4 | .10 | `tenure(lo=120, hi=365, floor=0.3)` on raw days (02 §C1: 11 writes `tenure_days(head_block, network_registered_at)`); `fallback_key='occupant_age_days'` (05's aux, same unit) | 2 | 11 | collector | never 0 (floor .3) / registration block unknown → `failed`, `reason='no_registration_block'` |
| 34 | `venue_reachable` | P5 | .10 | `binary` | 2 | 08 | fn `app.features.community:compute_venue_reachable` | all targets observed, none reachable, or no venue in any source → 0.0 / class `unknown`, a target failed (08 §4.2) |
| 35 | `discord_members` | P5 | .15 | `logcap(cap=5000)` | 2 | 08 | fn `…community:compute_discord_members` | — / not `own_server`, primary invite not observed OK in 2 d |
| 36 | `presence_ratio` | P5 | .10 | `presence_ratio(low=.05, high=.35, inflate_below=.02, inflate_members=1000)` (`value_json.members`; = 08's `norm_presence_ratio`) | 2 | 08 | fn `…community:compute_presence_ratio` | — / not `own_server`, < 2 samples in 24 h |
| 37 | `discord_growth_30d` | P5 | .10 | `zclip(lo=-0.1, hi=0.3)` | 2 | 08 | fn `…community:compute_discord_growth_30d` | — / < 30 ± 3 d of counts, guild changed |
| 38 | `announcement_cadence_30d` | P5 | .25 | `logcap(cap=12)` | 1 | 12 | derived | Σ ok legs 46–48; no ok leg → None (master: no venue → None) |
| 39 | `x_engagement_30d` | P5 | .10 | `rankg` | 3 | 09 | fn `app.features.social_x:compute_x_engagement_30d` | never / 0 posts, provider null |
| 40 | `x_followers` | P5 | .05 | `logcap(cap=50000)` | 3 | 09 | fn `…social_x:compute_x_followers` | no handle, account gone → absent / no snapshot ≤ 10 d |
| 41 | `silence_days` | P5 | .15 | `recency(full_at=7, zero_at=45)` | 1 | 12 | derived | min over ok legs 49–51; 02 §C3: absent (→ 0.0) **only** when no ok leg ∧ no reachable venue ∧ no X account (X legs absent); otherwise observed from the legs, else failed |
| 42 | `event_impact_14d` | P6 | .40 | `event_impact(scale=2)` | 1 | 10 | fn `app.features.sentiment:compute_event_impact_14d` | no events with coverage ≥ .5 → raw 0 ok (→ .5) / labels delayed |
| 43 | `polarity_7d` | P6 | .25 | `identity` (0.5 = neutral) | 1 | 10 | fn `…sentiment:compute_polarity_7d` | — / < 5 posts |
| 44 | `attention_7d` | P6 | .20 | `zclip(lo=-2, hi=3)` | 1 | 10 | fn `…sentiment:compute_attention_7d` (inputs per 10: TMC trending views `chain_trending_daily.views` + labelled mention count from `post_labels`; `x_mentions_7d` is not read) | — / baseline < 21 d on both components |
| 45 | `kol_coverage_30d` | P6 | .15 | `logcap(cap=5)`; `fallback_key='x_kol_posts_30d'` | 1 | 10 | fn `…sentiment:compute_kol_coverage_30d` | KOL run ok, 0 mentions → raw 0 ok / provider null, no KOL run in 30 d |
| 46 | `x_cadence_30d` | — | 0 | `none` (leg of 38) | 3 | 09 | fn `…social_x:compute_x_cadence_30d` | no handle / gone → absent |
| 47 | `discord_announcements_30d` | — | 0 | `none` (leg of 38) | 2 | 08 | fn `…community:compute_discord_announcements_30d` | 0 = followed channel + fresh gateway heartbeat, no posts / None = no followed channel, gateway stale (08 §4.2) |
| 48 | `tg_posts_30d` | — | 0 | `none` (leg of 38) | 2 | 08 | fn `…community:compute_tg_posts_30d` | 0 = fresh preview, no posts / None = no public channel, preview unavailable |
| 49 | `x_last_activity_days` | — | 0 | `none` (leg of 41) | 3 | 09 | fn `…social_x:compute_x_last_activity_days` | no handle / gone → absent (02 §C3) / None = no team post observed yet |
| 50 | `discord_last_activity_days` | — | 0 | `none` (leg of 41; `value_json.lower_bound`) | 2 | 08 | fn `…community:compute_discord_last_activity_days` | None = no followed channel, heartbeat stale (never absent) |
| 51 | `tg_last_activity_days` | — | 0 | `none` (leg of 41; `value_json.lower_bound`) | 2 | 08 | fn `…community:compute_tg_last_activity_days` | None = no public channel with preview (never absent) |
| 52 | `x_mentions_7d` | — | 0 | `none` (evidence only; **not** an input of 10's `attention_7d`, which reads TMC trending views + the labelled mention count, 10 §3.3) | 3 | 09 | fn `…social_x:compute_x_mentions_7d` | per spec 09 |
| 53 | `news_mentions_7d` | — | 0 | `none` (evidence) | 3 | 09 | fn `app.features.social_feeds:compute_news_mentions_7d` (fn name per 02 §C5; `register_metric` overrides the path) | per spec 09 A.4 |
| 54 | `x_kol_posts_30d` | — | 0 | `none` (fallback leg of 45, normalised with 45's normaliser) | 3 | 09 | fn `…social_x:compute_x_kol_posts_30d` | per spec 09 |
| 55 | `emission_enabled_flag` | — | 0 | `none` (aux → `emission_disabled`) | 1 | 05 | collector | 1/0 |
| 56 | `dereg_risk_flag` | — | 0 | `none` (aux → `dereg_imminent`) | 1 | 05 | collector | 1/0 |
| 57 | `immune_flag` | — | 0 | `none` (aux → `dereg_imminent`) | 1 | 05 | collector | 1/0 (local immunity: `NETWORK_IMMUNITY_PERIOD_BLOCKS` = 864000) |
| 58 | `implied_miner_burn` | — | 0 | `none` (aux → `miner_burn_zeroed`) | 1 | 05 | collector | [0,1] or None |
| 59 | `dereg_hazard` | — | 0 | `none` (evidence) | 1 | 05 | collector | [0,1] |
| 60 | `occupant_age_days` | — | 0 | `none` (evidence; `tenure` itself comes from 11) | 1 | 05 | collector | days since registration block |
| 61 | `owner_extraction_trigger` | — | 0 | `none` (aux → `owner_extraction`) | 2 | 11 | collector | 02 §C2: 1.0 iff a non-whitelisted S1 hard trigger (full-exit extrinsic `unstake_all_alpha`/`remove_stake_full_limit`, 7-d sell impact > 2 % of pool TAO, `sell_ratio > 1`) in the last 30 d, **or** a non-whitelisted `perpetual_off` (perpetual→decaying transition of the owner's lock, or `set_perpetual_lock(false)` by the owner coldkey) in the last 90 d; 0.0 otherwise; `failed` iff the owner data is unobservable. `value_json.{reasons[], trigger_ids[]}` (§2.8) |
| 62 | `owner_control_change_trigger` | — | 0 | `none` (aux → `owner_control_change`) | 2 | 11 | collector | 02 §C2: 1.0 iff a non-whitelisted `coldkey_swap`, `owner_transfer`, `coldkey_change_unclassified` or `subnet_owner_changed` in the last 30 d; 0.0 otherwise; `failed` iff the owner data is unobservable. `value_json.identity_churn = true` when the change is paired with an owner-hotkey re-point or an identity change within 14 d (→ sticky flag, 02 §D5); `value_json.{kind, trigger_ids[]}` |

Per-pillar sums: P1 = P2 = P3 = P4 = P5 = P6 = 1.00 (checked at import).

**Keys outside this table.** Producers also write evidence-only keys (weight 0) that the scoring math never reads (05: `vtrust_dispersion` (02 §C7), `owner_locked_share`, `sum_subnet_prices`, `tao_usd`; 08: `discord_topology`, `tg_members`, `inflated_days` (02 §C3); 11: `owner_lock_ratio`, `owner_dump_impact`, `owner_holding_share`, `owner_sell_impact_7d`, `team_verified_count`, `team_candidate_count` (02 §C2); 09: `video_uploads_90d`, `video_mentions_30d`, `blog_posts_90d`). They are admitted by spec 03's catalog (`app/storage/catalog.py: METRIC_KEYS`, owned by 03 — 02 §A2), not by `METRICS`; they carry no weight and no normaliser and never get `stage='norm'` rows. Fn-produced evidence keys (08, 09) are attached through `register_metric` (case (b) above) so that `compute_registered` writes them. Retired names that must not be written (02 §C2/C3): `owner_lock_disabled_90d`, `owner_control_change_30d`, `owner_max_sell_impact_7d`, `identity_stability`, `owner_conviction`, `owner_retention`, `discord_last_announcement_days`, `tg_last_post_days`. `announcement_cadence_30d` and `silence_days` are **derived here** from the C3 legs (§2.6); 08 and 09 must not register producers for them, and any such fn is never called.

**Import-time validation** (`_validate()`; any violation raises `RegistryError`, so `JobRegistry.build` fails loudly at boot per spec 01 §2.7):
1. Keys unique and match `^[a-z0-9_]{3,48}$`; exactly 62 rows; exactly the master §7.3 scored set per pillar (P1 10, P2 10, P3 6, P4 7, P5 8, P6 4).
2. For each of P1…P6: `abs(math.fsum(w) − 1.0) ≤ 1e-9` over `role='scored'`.
3. `PILLAR_WEIGHTS == {P1:30, P2:20, P3:10, P4:18, P5:12}`; `"P6" not in PILLAR_WEIGHTS`.
4. `role='scored'` ⇒ `pillar in P1..P6 and weight > 0 and normaliser ≠ 'none'`; `role ∈ {leg, aux}` ⇒ `pillar is None and weight == 0 and normaliser == 'none'`.
5. `normaliser ∈ NORMALISERS`; `inspect.signature(NORMALISERS[n]).bind(0.5, **params)` succeeds for scalar normalisers (the multi-input ones are bound with their documented `value_json` keys set to `None`).
6. `freshness_days > 0`; `source_spec ∈ {05,06,07,08,09,10,11,12}`; `producer ∈ {collector, fn, derived}`; `producer == 'fn' ⇒ fn` set, named `compute_<key>` (02 §C5); `normaliser == 'prenorm'` exactly on the rows whose producer writes a final [0,1] score (27–32, 02 §C1).
7. `fallback_key`, when set, names a `role ∈ {leg, aux}` row measured in the same unit as the primary (ts-1.0: `x_kol_posts_30d` for 45, `occupant_age_days` for 33); the blend keys of the spec (`net_flow_30d`, `chain_buys`, `attention_7d`) are scored rows.
8. `registry_hash()` equals `GOLDEN_REGISTRY_HASH` stored in `app/scoring/spec.py` for `ts-1.0` — an accidental edit fails CI and boot (§2.13).
9. Every key is in 03's `METRIC_KEYS` with the same pillar and weight, and no key is a retired name (02 §C2/C3). This registry never adds catalog keys (03 owns the catalog, 02 §A2).

### 2.5 Normalisers (`app/features/normalise.py`, exact code)

Pure, stdlib-only, no I/O, no settings. Every function maps `None → None`. Anchors verified with Python on 2026-09-23 (§9 tests pin them).

```python
"""ts-1.0 normalisers. Pure, stdlib only. Every function maps None -> None."""
from __future__ import annotations
import math
from statistics import NormalDist
from typing import Mapping

_ND = NormalDist()

def _clip01(x: float) -> float:                                   # port quality.py:91-92
    return 0.0 if x < 0.0 else 1.0 if x > 1.0 else x

def _average_ranks(xs: list[float]) -> list[float]:               # port utils/math.py:28-42 verbatim
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    ranks = [0.0] * len(xs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        shared = (i + j) / 2.0
        for k in range(i, j + 1):
            ranks[order[k]] = shared
        i = j + 1
    return ranks

def identity(x: float | None) -> float | None:
    return None if x is None else _clip01(float(x))

def binary(x: float | None) -> float | None:
    return None if x is None else (1.0 if x >= 0.5 else 0.0)

def recency(days: float | None, full_at: float, zero_at: float) -> float | None:   # port quality.py:113-122
    if days is None:
        return None
    if days <= full_at:
        return 1.0
    if days >= zero_at:
        return 0.0
    span = zero_at - full_at
    return _clip01(1.0 - (days - full_at) / span) if span > 0 else 0.0

def age_ramp(days: float | None, young_at: float, mature_at: float, floor: float) -> float | None:
    """Master 'recency inverted': <= young_at -> floor, >= mature_at -> 1.0, linear between."""
    if days is None:
        return None
    return floor + (1.0 - floor) * _clip01((days - young_at) / (mature_at - young_at))

def logcap(x: float | None, cap: float) -> float | None:          # OpenSSF criticality style
    if x is None:
        return None
    return _clip01(math.log1p(max(float(x), 0.0)) / math.log1p(cap))

def zclip(x: float | None, lo: float, hi: float) -> float | None:
    if x is None:
        return None
    return _clip01((float(x) - lo) / (hi - lo))

def zclip_inv(x: float | None, lo: float, hi: float) -> float | None:
    z = zclip(x, lo, hi)
    return None if z is None else 1.0 - z

def zclip_log2(ratio: float | None, lo: float, hi: float) -> float | None:
    """gate_margin: ratio <= 0.25 -> 0, 1.0 -> 2/3, >= 2 -> 1."""
    if ratio is None:
        return None
    if ratio <= 0.0:
        return 0.0
    return zclip(math.log2(ratio), lo, hi)

def checklist(x: float | None, items: Mapping[str, float | None] | None = None) -> float | None:
    if items:
        vals = [1.0 if v else 0.0 for v in items.values() if v is not None]
        return (sum(vals) / len(vals)) if vals else None
    return identity(x)

def bus_factor(k: float | None) -> float | None:
    if k is None:
        return None
    k = int(round(k))
    return 1.0 if k >= 3 else 0.6 if k == 2 else 0.3 if k == 1 else 0.0

def tenure(days: float | None, lo: float, hi: float, floor: float) -> float | None:
    z = zclip(days, lo, hi)
    return None if z is None else max(floor, z)

def event_impact(x: float | None, scale: float) -> float | None:
    """tanh(x/2) -> [-1,1] -> [0,1]; x = 0 (no events) -> 0.5 neutral."""
    return None if x is None else (math.tanh(float(x) / scale) + 1.0) / 2.0

def presence_ratio(r: float | None, members: float | None, *, low: float, high: float,
                   inflate_below: float, inflate_members: float) -> float | None:
    """= spec 08's norm_presence_ratio (08 §2.9), with the 08 constants passed as frozen params."""
    if r is None or members is None:
        return None
    if members > inflate_members and r < inflate_below:
        return 0.0                                                # inflation (also inflated_community flag)
    if r < low:
        return _clip01(r / low)                                   # 0 -> 0.0 ... 0.05 -> 1.0
    if r <= high:
        return 1.0                                                # healthy band 0.05-0.35
    return max(0.5, 1.0 - (r - high) / 0.7)                       # tiny / bot-heavy servers, floor 0.5

def prenorm(x: float | None) -> float | None:
    """Producer already wrote the final [0,1] value (spec 11 P4 metrics). Out of range = producer bug -> None."""
    if x is None:
        return None
    x = float(x)
    return None if (x < -1e-9 or x > 1.0 + 1e-9) else _clip01(x)

# --- Reference implementations of master §7.3 P4 (S1/S2/S3/S5 and the team map). ----------------------
# Production values are computed by spec 11 (with its extra edge rules: grace_period, OWNER_MIN_EARNED_ALPHA,
# whitelists, lock_ratio when total = 0) and scored here through `prenorm` (02 §C1). These functions are NOT in
# the ts-1.0 dispatch; they are the executable statement of the master formulas that spec 11's tests must agree
# with on the shared anchors (§9 test_normalise), and the oracle for spec 13's reconstructed P4 history. The
# `value_json` of registry rows 27, 29-32 carries their keyword arguments under the same names (§2.4).

def team_map(n_verified: float | None, n_named: int | None) -> float | None:
    if n_verified is None:
        return None
    if n_verified >= 2:
        return 1.0
    if n_verified >= 1:
        return 0.6
    return 0.3 if (n_named or 0) >= 1 else 0.0

def owner_commitment(lock_ratio: float | None, auto_lock: bool | None, commit_pct: float | None,
                     perpetual_locked: float | None, age_days: float | None) -> float | None:
    """S2: 0.60*lock_ratio + 0.25*auto_lock + 0.15*[commit_pct >= 5 %]; renormalised over known parts."""
    if lock_ratio is None:
        return None
    if perpetual_locked == 0 and auto_lock is False and age_days is not None and age_days > 90:
        return 0.0
    parts = [(0.60, _clip01(lock_ratio))]
    if auto_lock is not None:
        parts.append((0.25, 1.0 if auto_lock else 0.0))
    if commit_pct is not None:
        parts.append((0.15, 1.0 if commit_pct >= 0.05 else 0.0))
    return math.fsum(w * s for w, s in parts) / math.fsum(w for w, _ in parts)

def owner_retention(sell_ratio: float | None, max_7d_sell_impact: float | None,
                    full_exit: bool | None, owner_cut_enabled: bool | None) -> float | None:
    """S1: 1 - clip(sell_ratio); hard 0 on a full-exit extrinsic, a 7-d sell_impact > 2 % or sell_ratio > 1.
    The hard triggers are evaluated first; only then the owner_cut_enabled=False -> 1.0 shortcut (02 §C1)."""
    if (full_exit or (max_7d_sell_impact is not None and max_7d_sell_impact > 0.02)
            or (sell_ratio is not None and sell_ratio > 1.0)):
        return 0.0
    if owner_cut_enabled is False:
        return 1.0
    if sell_ratio is None:
        return None
    return 1.0 - _clip01(sell_ratio)

def control_stability(events: list[dict], as_of_ts: int) -> float:
    """S3: formal exit -> 0; swap -0.6, hotkey re-point + identity change -0.4; halves every 45 d; 90-d lookback."""
    pen = 0.0
    for e in events:
        if e.get("whitelisted"):
            continue
        age_d = (as_of_ts - int(e["event_at"])) / 86400.0
        if age_d < 0 or age_d > 90:
            continue
        if e["kind"] == "perpetual_lock_disabled":
            return 0.0
        base = {"coldkey_swap_unannounced": 0.6, "hotkey_repoint_identity_change": 0.4}.get(e["kind"], 0.0)
        pen += base * 0.5 ** (age_d / 45.0)
    return _clip01(1.0 - pen)

def owner_overhang(dump_impact: float | None, legacy_unaccounted: bool | None) -> float | None:
    """S5: < 5 % -> 1, 25 % -> 0, linear; -0.2 for pre-2026-05-08 registrations with unaccounted owner alpha."""
    z = zclip_inv(dump_impact, 0.05, 0.25)
    if z is None:
        return None
    return _clip01(z - (0.2 if legacy_unaccounted else 0.0))

def rankg(values: Mapping[str, float], *, clip_z: float = 2.5, min_n: int = 10) -> dict[str, float | None]:
    """Cross-sectional rank -> percentile (r0 + 0.5)/n -> inverse normal -> clip +/-clip_z -> [0,1].
    Ties share their average rank. Cohort smaller than min_n -> None for everyone."""
    keys = list(values)
    n = len(keys)
    if n < min_n:
        return {k: None for k in keys}
    ranks0 = _average_ranks([float(values[k]) for k in keys])
    out: dict[str, float | None] = {}
    for k, r0 in zip(keys, ranks0):
        z = _ND.inv_cdf((r0 + 0.5) / n)
        z = -clip_z if z < -clip_z else clip_z if z > clip_z else z
        out[k] = (z + clip_z) / (2.0 * clip_z)
    return out
```

**Dispatch** (`NORMALISERS` + `normalise_value(defn, value, status, value_json) -> float | None`). `NORMALISERS` (ts-1.0) = `{identity, binary, recency, age_ramp, logcap, zclip, zclip_inv, zclip_log2, checklist, bus_factor, tenure, event_impact, presence_ratio, prenorm, rankg}`; `team_map`, `owner_commitment`, `owner_retention`, `control_stability`, `owner_overhang` are reference functions outside the dispatch. Scalar normalisers are called as `f(value, **params)`; the two multi-input ones read their extra input from `value_json` — `checklist(value, items=vj.get("items"))`, `presence_ratio(value, vj.get("members"), **params)`. `rankg` is never dispatched per row; `snapshot()` applies it to the cohort (below). Status handling wraps every call: `absent → 0.0`, `failed | not_applicable | none → None` (generated `none` = no `value_json.status` key, read as `failed`, 02 §B3), `ok →` the function. A `prenorm` value outside [0,1] yields `None` with `reason='prenorm_out_of_range'` and one warning log per (metric, run).

**`rankg` cohort.** For metric `k` at slot `A`: the occupants whose status at `A` is `active` (not `pre_launch`) and whose selected raw row is fresh with `status='ok'` and `value IS NOT NULL`. `absent` rows get 0.0 and are excluded from the cohort; `pre_launch` occupants get `None` with `reason='pre_launch'`. Cohort size `< RANKG_MIN_N` (10, frozen) → `None` for the whole cohort (`reason='cohort_too_small'`). Consequences to know: all-tied cohort → 0.5 each; 37 tied zeros in a cohort of 122 each get `m = 0.2941` on `emission_share` (the worked-example fixture, §2.14; research `02-gapfill-keyless-onchain-pillar-source.md` §2 counts 37 of 129 TMC table rows at exactly 0 emission on 2026-09-22 — that count includes root and not-started rows, which are outside the cohort, so the live tie is somewhat smaller; §11 Q6).

### 2.6 Feature engine (`app/features/engine.py`)

```python
@dataclass(frozen=True, slots=True)
class Occupant:
    occupant_key: str; netuid: int; network_registered_at: int
    status: str                        # status at the slot: 'active' | 'pre_launch'

@dataclass(frozen=True, slots=True)
class RawRow:                          # one spec-03 metric_values row with stage='raw'
    occupant_key: str; metric_key: str; as_of: int; value: float | None; value_json: dict
    status: str; observed_at: int; source: str; run_id: str | None; backfilled: int   # status = generated column
    @property
    def provisional(self) -> bool: return self.as_of % 86400 != 0                        # 02 §B3

@dataclass(slots=True)
class NormValue:
    m: float | None; raw: float | None; status: str; reason: str | None
    age_s: int | None; raw_as_of: int | None; raw_observed_at: int | None; src: str | None
    fallback: bool = False; cohort_n: int | None = None

Snapshot = dict[str, dict[str, NormValue]]       # occupant_key → metric_key → NormValue (all 45 scored keys present)

class FeatureEngine:
    def __init__(self, db: Database, *, registry: Mapping[str, MetricDef] = METRICS) -> None
    async def universe(self, slot: int) -> list[Occupant]
    async def compute_registered(self, occ: list[Occupant], t_eval: int, key_as_of: int, run_id: str, *, mode: str) -> int
    async def derive(self, occ: list[Occupant], slot: int, cutoff: int, key_as_of: int, run_id: str, *, mode: str) -> int
    # key_as_of = the slot in scoring runs, floor_hour(now) in features_hourly (§2.2 "raw as_of key")
    async def select_raw(self, keys: Sequence[str], slot: int, cutoff: int, mode: str, raw_sv: str,
                         fresh_override_s: int | None = None) -> dict[tuple[str, str], RawRow]
    async def select_series(self, key: str, as_ofs: Sequence[int], cutoff: int, mode: str,
                            raw_sv: str) -> dict[str, dict[int, RawRow]]          # exact as_of matches (flags)
    async def snapshot(self, spec: ScoringSpec, occ: list[Occupant], slot: int, cutoff: int, mode: str) -> Snapshot
    def norm_rows(self, spec: ScoringSpec, snap: Snapshot, slot: int, run_kind: str, computed_at: int,
                  run_id: str, backfilled: int) -> list[dict]                       # stage='norm' rows (§3.1)
```

**`universe(slot)`** — occupants from `occupants` (spec 04; DDL 03 — all times INTEGER unix seconds, no conversion) with `netuid ≠ 0` (the root subnet has no alpha pool and is never scored), `first_seen_at ≤ slot` and (`deregistered_at IS NULL` or `> slot`); `status = 'active'` if `activated_at` is set and `≤ slot`, else `'pre_launch'`. Deregistered occupants are never scored.

**`compute_registered(occ, t_eval, key_as_of, run_id, mode)`** — for every distinct producer fn (dotted `fn` path, attached via `register_metric`, or in `EVIDENCE_FNS`) and every occupant: `await asyncio.wait_for(fn(db, occupant_key, datetime.fromtimestamp(t_eval, UTC)), FEATURE_FN_TIMEOUT_SEC)`, sequentially (SQLite single writer; the fns are read-only). Results become raw rows `(stage = 'raw', as_of = key_as_of, value, value_json with status per §2.3, observed_at = t_eval, source = SOURCE_OF[key], run_id, backfilled = 0)` written through `put_metric_values` in one transaction (02 §B4 first success wins: an existing `ok`/`absent`/`not_applicable` row for the same key is left untouched, a `failed` one is replaced). In **reconstructed** mode rows are stamped `observed_at = now`, `backfilled = 1`; because 03's writer never overwrites a successful row, a catch-up can never replace a live observation. If a live row already occupies the key but is invisible to the reconstructed cutoff (e.g. `features_hourly` ran in the 00:xx hour after 00:15Z while scoring was paused), the reconstructed run's own row is ignored and the PIT rule falls back to the previous fresh row — conservative, never look-ahead. `SOURCE_OF` = the producing spec's single `source` (02 §B9): 07 → `web`, 08 keys → `community`, 09 X keys → `x`, 09 appendix keys (`news_mentions_7d`, `video_uploads_90d`, `video_mentions_30d`, `blog_posts_90d`) → `rss`, 10 keys → `llm`, 11 → `integrity`, derived keys → `scoring` (03's CHECK lists exactly the B9 set; there is no `nlp` or `features` source).

**`derive(occ, slot, cutoff, key_as_of, run_id, mode)`** — evaluates the two 02 §C3 derived metrics from the selected leg rows (same PIT rule, each leg with its own freshness) and writes raw rows (`source='scoring'`, keys and stamping as for fns); 08 and 09 register no producer for them:

| derived key | rule | `value_json` |
|---|---|---|
| `announcement_cadence_30d` | legs L = (`x_cadence_30d`, `discord_announcements_30d`, `tg_posts_30d`). If ≥ 1 leg is `ok`: `value = Σ value over ok legs` (absent legs add 0, failed/missing legs ignored), `status='ok'`. Else `value=NULL`, `status='failed'`, `reason='no_ok_leg'` (master P5: Discord/TG metrics None without a venue; never absent). | `{"legs": {key: value \| status}}` |
| `silence_days` | legs L = (`x_last_activity_days`, `discord_last_activity_days`, `tg_last_activity_days`). Exact legs E = ok legs without `value_json.lower_bound`; lower-bound legs B = ok legs with it (08 returns "days since observation start" when no post was ever seen). If E ≠ ∅: `value = min(E)`, `ok`. Elif B ≠ ∅: `value = min(B)`, `ok`, `value_json.lower_bound = true`. Else, 02 §C3: if no venue is reachable — the selected `venue_reachable` row has value 0.0 (status ok or absent) — **and** no X account exists — the selected `x_last_activity_days` or `x_cadence_30d` row is `absent` (09's `no_x_account` / `x_account_gone`), or the occupant has no `watch_channels(platform='x')` row with `first_seen_at ≤ cutoff` and (`retired_at` NULL or `> cutoff`) (no team handle in any source) — then `status='absent'`, `reason='no_reachable_channel'` (→ 0.0). Else `failed`, `reason='no_ok_leg'` (an X account exists or a venue is reachable, so silence must be observed from the legs). | `{"legs": {key: value \| status}, "min_leg": key, "lower_bound": bool}` |

**`snapshot(spec, occ, slot, cutoff, mode)`** — the only producer of normalised values:
1. `raws = select_raw(all scored + leg + aux keys, slot, cutoff, mode, spec.raw_spec_version)` — one statement per key (served by 03's `ix_metric_values_xsec`):
   ```sql
   SELECT occupant_key, as_of, value, status, value_json, observed_at, source, run_id, backfilled
   FROM metric_values
   WHERE metric_key = :k AND stage = 'raw' AND spec_version = :raw_sv
     AND as_of <= :slot AND as_of >= :slot - :fresh_s
     AND (observed_at <= :cutoff OR (:reconstructed = 1 AND backfilled = 1))
   ORDER BY occupant_key, as_of DESC;
   -- per occupant: the first row with status NOT IN ('failed','none') wins; if every row is failed/none, the first
   -- (latest) one is returned so its reason reaches the norm row (02 §B5); the table key makes (occupant, as_of) unique
   ```
2. For each occupant × scored metric: pre-launch occupant and `pillar == 'P1'` → `NormValue(None, reason='pre_launch')`. No selectable row (none fresh, or all fresh rows `failed`/`none`) → if `fallback_key` has a selectable row, use it with `fallback=True` (normalised with the primary's normaliser); else `None` with `reason='no_fresh_row'` or the failed row's reason. Row status `absent` → `m = 0.0`; `failed | not_applicable | none` → `None` (reason copied); `ok` → `normalise_value(...)` unless the metric is cross-sectional.
3. Cross-sectional metrics (`rankg`): build the cohort (§2.5) and assign `m` and `cohort_n`.
4. `norm_rows()` materialises one `stage='norm'` `metric_values` row (02 §B3) per universe occupant × scored metric: `metric_key`, `spec_version = spec.version` (the scoring spec), `as_of = slot` (so run kind = `as_of % 86400 ≠ 0` ⇒ provisional), `value = m` (NULL when None — written explicitly so coverage is auditable), `value_json = {raw, status, reason, raw_as_of, raw_observed_at, age_s, src, fallback, cohort_n}` (`status` = the selected raw row's status, omitted when no raw row was selected so the generated column reads `none`; `src` = that row's `source`; `cohort_n` for `rankg` only), `observed_at = computed_at` (T0), `source = 'scoring'`, `backfilled` of the run, `run_id`.

**`features_hourly`** (`app/features/engine.py::FeaturesJob(CollectorJob)`, spec 01 job table: interval `FEATURES_INTERVAL_MIN`, flag `FEATURES_ENABLED`): `occ = universe(now)`; `compute_registered(occ, now, floor_hour(now), run_id, mode='strict')`; `derive(occ, slot=now, cutoff=now, key_as_of=floor_hour(now), run_id, mode='strict')`; returns `JobOutcome(rows=n_written)`. It writes **`stage='raw'` rows only** (never `stage='norm'`), keyed `floor_hour(now)` (provisional except in the 00:xx hour), so the API evidence stays fresh between scoring runs. Idempotent: a re-run in the same hour inserts nothing new (first success wins) and only replaces rows that were `failed`.

### 2.7 Scoring: pillars, Health, Velocity, regime, composite (`app/scoring/engine.py`)

**Pillar kernel** — port of `quality.py:207-233` (`quality_score`), generalised from 7 fixed components to registry items:

```python
def pillar_score(items: Iterable[tuple[float, float | None]], min_coverage: float) -> tuple[float | None, float]:
    """Coverage-renormalised weighted mean in [0,1]; None != 0 (a None item is dropped, a 0.0 item counts)."""
    items = list(items)
    total_w = math.fsum(w for w, _ in items)
    avail_w = math.fsum(w for w, m in items if m is not None)
    coverage = avail_w / total_w if total_w > 0 else 0.0
    if coverage + 1e-9 < min_coverage or avail_w <= 0:
        return None, coverage
    return _clip01(math.fsum(w * m for w, m in items if m is not None) / avail_w), coverage
```

Per occupant `o` (spec constants in parentheses):

```
for k in P1..P6:  P_k, cov_k = pillar_score([(d.weight, snap[o][d.key].m) for d in scored(k)], 0.3)
                  (pre_launch: P1 is not computed → P1 = None, cov_1 excluded below)
K      = [P1..P5] if active else [P2..P5]
H      = [k ∈ K with P_k is not None]
Health = 100 · Σ_{k∈H} W_k·P_k / Σ_{k∈H} W_k                  (None if H empty; W = 30/20/10/18/12)
confidence = Σ_{k∈K} W_k·cov_k / Σ_{k∈K} W_k                   (master §7.7; absent (0.0) metrics count as covered)
blend  = mean(m for m in (m[net_flow_30d], m[chain_buys], m[attention_7d]) if m is not None)   (None if none)
Velocity = 100·(0.6·P6 + 0.4·blend)  if P6 is not None and blend is not None
         = 100·P6                      if blend is None           (renormalised)
         = 100·blend                   if P6 is None              (spec 10: "Velocity falls back to the blend")
         = None                        if both None
v_w    = {TRENDING: .25, DISPERSED: .25, CHOPPY: .15, DEAD: .10, UNKNOWN: .15}[regime]
composite_raw = None if Health is None else (Health if Velocity is None else Health·(1−v_w) + Velocity·v_w)
flag_multiplier = max(0.25, Π multiplier_f over flags with red_flag_state.active = 1)   (1.0 when none; §2.8)
composite = composite_raw · flag_multiplier
insufficient_data = (active and confidence < 0.4)
pre_launch: velocity, composite_raw, composite, composite_smoothed, tier = NULL; quality (Health) = NULL;
            pillar_scores P2..P6 and confidence (over P2..P5) are stored; flags are evaluated and stored.
```

Values are stored unrounded (`REAL`); tiers and events use the unrounded values; explain rounds to 6 decimals.

**Regime** (`app/quant/regime.py`, port — §8): `regime_for_slot(db, slot, cutoff, mode, params) -> RegimeResult(label, raw_regime, metrics: dict, v_w)` (`label` is stored in `regime_daily.label` and `scores.regime`).
1. `SELECT netuid, hour_ts, price FROM chain_hourly WHERE hour_ts > :slot - 86400 AND hour_ts <= :slot AND netuid != 0 AND is_active = 1 AND price > 0 AND (observed_at <= :cutoff OR (:recon = 1 AND backfilled = 1))`.
2. Group into `{netuid: [{"ts": hour_ts, "price": price}, …]}` (INTEGER `ts`) and call the ported `compute_regime_metrics(per_netuid, params)` (old `regime.py:159-221`) → `{vol_24h, directional_strength, dispersion, raw_regime, n_subnets, thin_universe}`. One line of the port changes (§8.2): the old `_bucket_snapshots` does `parse_iso(ts_raw).timestamp()` on a `datetime`, but 02 §B1's `parse_iso` returns `int`, so the port uses `ts = float(parse_ts(ts_raw))` (accepts int, float, ISO string or aware datetime).
3. `label = "UNKNOWN" if thin_universe else raw_regime` (02 §B8: uppercase labels, no debounce; the old code returns `DEAD` for a thin universe; here a data gap must not masquerade as a dead market — `UNKNOWN` keeps the baseline `v_w = 0.15`).
4. Diagnostics only (not used for classification in ts-1.0): `tao_usd_ret_24h = ln(tao_usd(slot)/tao_usd(slot−86400))` and `sum_prices_ret_24h` from `chain_network_hourly` (NULL when either end is missing).

`RegimeParams` (frozen in `spec.py`) keeps the old attribute names so the ported functions are unchanged: `REGIME_VOL_WINDOW_HOURS=24, REGIME_BUCKET_HOURS=4, REGIME_MIN_BUCKETS=6, REGIME_MIN_SUBNETS=10, REGIME_VOL_TREND_THRESHOLD=0.30, REGIME_VOL_CHOP_FLOOR=0.10, REGIME_VOL_DEAD_THRESHOLD=0.05, REGIME_DIR_THRESHOLD=0.02, REGIME_DISP_THRESHOLD=0.015` (old `config.py:454-464`). The old debounce (`REGIME_DEBOUNCE_CYCLES`) is not ported: consecutive 24-h windows 6 h apart overlap 75 % and the score is EMA-smoothed (§11 Q10). The regime is computed once per slot per distinct `RegimeParams` and shared by specs that use the same params.

### 2.8 Red-flag gates (`app/scoring/red_flags.py`)

```python
@dataclass(frozen=True, slots=True)
class FlagDef:
    name: str; multiplier: float; clear_days: int     # multiplier == 03 catalog.RED_FLAGS[name] (checked at import)
    human_confirmable: bool          # manual 'confirmed' raises and holds (02 §D5: owner_extraction, key_compromise)
    manual_clearable: bool           # manual 'cleared' clears an active flag (02 §D5: + owner_control_change)
    sticky_on: str | None            # detail key that makes the flag sticky (owner_control_change: "identity_churn")
    evaluate: Callable[[FlagInputs], tuple[bool | None, dict]]   # (trigger, detail)
```

Triggers are evaluated at every run (official and provisional) from the run's selected raw rows, `events` (`observed_at ≤ cutoff`, or backfilled in reconstructed mode) and `manual_flags` (03 DDL, written by 14 and 11: latest row per `(occupant_key, flag)` with `set_at ≤ cutoff`; its CHECK admits `confirmed` for `owner_extraction`/`key_compromise` and `cleared` for those two plus `owner_control_change`, 02 §D5). `A0` = day start of the slot. **`detail`** is the evaluated inputs; for the two owner flags it copies the trigger row's `value_json.{reasons, trigger_ids, kind, identity_churn}` — `trigger_ids` (sorted evidence ids, e.g. `owner_changes:<id>`, `owner_stake_events:<id>`) is what 02 §D5 compares to decide whether evidence is new after a manual clear.

| flag | × | trigger **True** | trigger **False** | **None** (no state change) | clear |
|---|---|---|---|---|---|
| `emission_disabled` | .50 | aux `emission_enabled_flag` = 0 ∧ occupant `active` | = 1 | row missing | 7 d |
| `dereg_imminent` | .60 | `dereg_risk_flag` = 1 ∧ `immune_flag` = 0 | `dereg_risk_flag` = 0 ∨ `immune_flag` = 1 | missing | 7 d |
| `miner_burn_zeroed` | .60 | `implied_miner_burn` > 0.8 on each of the exact-`as_of` rows `A0`, `A0−1d`, `A0−2d` | any of the three present and ≤ 0.8 | otherwise | 7 d |
| `owner_extraction` | .70 | aux `owner_extraction_trigger` raw = 1 (02 §C2: non-whitelisted S1 hard trigger in 30 d — full-exit extrinsic `unstake_all_alpha`/`remove_stake_full_limit`, 7-d sell impact > 2 % of pool TAO, `sell_ratio > 1` — or non-whitelisted `perpetual_off` in 90 d); **or** a manual `confirmed` row (incl. a human-confirmed `owner_extraction_evidence` label from 10, entered as this flag — 02 §D5) | = 0 | row missing or `failed` | 7 d auto; human-held until manual clear; manual clear → 30-d suppression unless new `trigger_ids` (D5) |
| `owner_control_change` | .85 | aux `owner_control_change_trigger` raw = 1 (02 §C2: non-whitelisted `coldkey_swap`, `owner_transfer`, `coldkey_change_unclassified` or `subnet_owner_changed` in 30 d); **sticky** when the row has `value_json.identity_churn = true` | = 0 | row missing or `failed` | 7 d after the trigger returns to 0 (= the 30-d trigger window + 7-d hysteresis); sticky → manual clear only; manual clear allowed while active (D5); spec 11 whitelisting a disclosed migration zeroes the trigger |
| `key_compromise` | .50 | manual `confirmed` only | — | — | manual |
| `repo_gone` | .80 | `declared_repo_freshness` raw `status='absent'` ∧ `reason='repo_gone'` | row present with any other status/reason | missing | 7 d |
| `all_repos_archived` | .70 | `org_freshness` raw `value_json.all_archived` ∨ (`declared_archived` ∧ `successor_repo_id` is null) | row present, neither | missing | 7 d |
| `site_down_7d` | .85 | latest of {`site_down_7d`, `site_recovered`} events for the occupant is `site_down_7d` | latest is `site_recovered`, or no such event and a fresh `site_uptime_30d` row exists | no such event and no fresh uptime row | 7 d |
| `stale_identity` | .90 | `dead_venue` ∧ `name_turnover` ∧ `no_comms_30d` | any term False | otherwise | 7 d |
| `silence_45d` | .85 | `silence_days` raw ok with value ≥ 45 | ok with value < 45, or `absent` (no channel — P5 already scores it) | failed/missing | 7 d |
| `inflated_community` | .90 | `presence_ratio` raw ok with value < 0.02 ∧ `value_json.members` > 1000 on each exact-`as_of` row `A0−6d … A0` | any of the 7 present and not satisfying | otherwise | 7 d |

`stale_identity` terms: `dead_venue` = `venue_reachable` raw value 0 (ok or absent) → True, 1 → False, missing → None. `name_turnover` = an `identity_changed` event (`payload.field='subnet_name'`, `payload.kind='changed'`) or an `identity_junk` event (`payload.junk_reason='abandonment_marker'`) with `event_at ∈ (A0−90d, slot]` → True; none → False. `no_comms_30d` = `silence_days` raw ok ≥ 30 or `absent` → True; ok < 30 → False; failed → None.

**State machine** (`red_flag_state` with exactly 03's columns, 02 §B7: `active, human, sticky, raised_at, last_true_at, false_since, cleared_at, suppress_until, last_manual_clear_at, last_eval, last_eval_as_of, detail_json, updated_at`; one row per `(occupant_key, flag, spec_version)`, created when a flag is first raised and updated on every later run; `s` = that row or None; `trig ∈ {True, False, None}` and `detail` come from `fd.evaluate`; `ids(d) = set(d.get("trigger_ids") or [])`):

```
1  h = latest manual_flags row for (occupant, flag) with set_at ≤ cutoff                        (None if none)
   confirmed = fd.human_confirmable and h is not None and h.state == 'confirmed'
2  new_clear = s is not None and fd.manual_clearable and h is not None and h.state == 'cleared'
               and h.set_at > (s.last_manual_clear_at or 0)
   if new_clear: s.last_manual_clear_at = h.set_at; s.suppress_until = slot + 30·86400    # D5: cleared_at + 30 d
                 s.detail_json.cleared_trigger_ids = sorted(ids(s.detail_json))
3  eff = trig
   if trig is True and s and s.suppress_until and slot < s.suppress_until
        and ids(detail) <= set(s.detail_json.cleared_trigger_ids or []):
        eff = False; detail.suppressed = True        # same evidence stays cleared; NEW trigger ids re-raise now (D5)
4  if s is None or s.active == 0:
       if eff is True or confirmed:                                                              → raised
           s.active = 1; s.raised_at = slot; s.cleared_at = NULL; s.false_since = NULL
           s.human = int(confirmed)
           s.sticky = int(eff is True and fd.sticky_on is not None and detail.get(fd.sticky_on) is True)
           if eff is True: s.last_true_at = slot
           s.detail_json = detail + {trigger_event_id: <events.id behind the trigger, e.g. site_down_7d, else null>}
                                   (cleared_trigger_ids carried over; INSERT, or UPDATE of the inactive row)
   else:                                                                                         (active)
       s.human = int(confirmed)
       if eff is True:
           s.last_true_at = slot; s.false_since = NULL; s.detail_json = detail (cleared_trigger_ids carried over)
           if fd.sticky_on is not None and detail.get(fd.sticky_on) is True: s.sticky = 1
       elif eff is False and s.false_since is NULL: s.false_since = slot     # None leaves the clock untouched
       if s.human or s.sticky: s.false_since = NULL                          # held flags never run the clock
       if new_clear:                                                                             → cleared (manual)
       elif s.false_since is not NULL and slot − s.false_since ≥ fd.clear_days·86400:           → cleared (hysteresis)
       on cleared: s.active = 0; s.cleared_at = slot; s.human = 0; s.sticky = 0
5  if s: s.last_eval = 'true'|'false'|'none' (of trig); s.last_eval_as_of = slot; s.updated_at = computed_at
```

Derived, never stored (02 §B7): `state = 'clearing'` iff `active = 1 ∧ false_since IS NOT NULL`, else `'active'`; `origin = 'human'` iff `human = 1`, else `'auto'`; `clear_streak_days = (slot − false_since) // 86400` (0 when NULL); the multiplier is `fd.multiplier` (= 03's `catalog.RED_FLAGS`).

"Clear when the trigger clears for 7 consecutive days" is measured in slot time from the first False evaluation after the last True; a True resets it, a None neither resets nor advances it. Human-confirmed and sticky flags hold until a manual clear. A manual `cleared` row for a flag with no state row is ignored (14 refuses it with HTTP 409 `flag_not_active`, D5); one that arrives after the flag already auto-cleared still sets `last_manual_clear_at`/`suppress_until`, so the cleared evidence cannot re-raise it for 30 d. `manual_clearable` is True for `owner_extraction`, `key_compromise` and `owner_control_change`; `human_confirmable` for the first two (02 §D5). `flag_multiplier = max(0.25, Π multiplier)` over rows with `active = 1`; **`scores.red_flags`** = the names of those flags as a JSON array sorted by name, e.g. `["dereg_imminent","stale_identity"]` (02 §B6); multiplier, since (= `raised_at`), state, origin and detail live only in `explain.flags` (§2.10).

### 2.9 Smoothing, tiers, confidence, insufficient data, pre-launch

- **Previous official row** `prev` = latest `scores` row for `(occupant_key, spec_version)` with `run_kind='official'`, `as_of < slot`, `as_of ≥ slot − 7·86400` (`EMA_MAX_GAP_DAYS`), `composite_smoothed IS NOT NULL`. Always read from the table — no in-memory state — so a restart yields byte-identical results (EMA restart invariance).
- **EMA** (span 7, `α = 2/(7+1) = 0.25`): `composite_smoothed = composite` if `prev` is None (seed/reseed after a > 7-d gap); else `α·composite + (1−α)·prev.composite_smoothed`; `None` if `composite` is None (no forward-fill; the next valid composite continues from the last non-null smoothed within the gap). Provisional rows compute the same formula against the same `prev` and are **never** used as `prev`.
- **Tier** (A ≥ 75, B 60–74, C 45–59, D 30–44, F < 30; ±3 hysteresis):

```python
ORDER = ("F", "D", "C", "B", "A")
LOWER = {"A": 75.0, "B": 60.0, "C": 45.0, "D": 30.0, "F": float("-inf")}

def assign_tier(s: float | None, prev: str | None, h: float = 3.0) -> str | None:
    if s is None:
        return None
    if prev is None:                                                   # seed: plain thresholds
        return next((t for t in ("A", "B", "C", "D") if s >= LOWER[t]), "F")
    ip = ORDER.index(prev)
    up = max(i for i, t in enumerate(ORDER) if s >= LOWER[t] + h)      # enter a higher tier at >= threshold + 3
    if up > ip:
        return ORDER[up]
    if prev != "F" and s <= LOWER[prev] - h:                            # leave at <= threshold - 3
        return ORDER[max(i for i, t in enumerate(ORDER) if i <= ip and s > LOWER[t] - h)]
    return prev
```
  `prev` tier = `prev.tier` (official only). Provisional rows get `assign_tier(smoothed_prov, prev.tier)` for display; only official rows emit `tier_change`.
- **Confidence / `insufficient_data`** as in §2.7; rows with `insufficient_data = 1` keep composite, smoothed and tier (the EMA and hysteresis stay continuous) but are excluded from rank tables by default (14) and from the feed by `min_confidence` (15), and emit no `tier_change`.
- **Pre-launch** (`status='pre_launch'`): no composite/tier; P2–P6 shown; `confidence` over P2..P5; excluded from rank-stability and the feed.

### 2.10 Explain JSON (`scores.explain`, rendered verbatim by 14)

```json
{"v": 1, "spec_version": "ts-1.0", "spec_hash": "…", "as_of": 1790121600, "run_kind": "official", "mode": "strict",
 "status": "active", "regime": {"name": "CHOPPY", "raw": "CHOPPY", "vol_24h": 0.21, "directional_strength": 0.011,
 "dispersion": 0.009, "n_subnets": 118}, "v_w": 0.15,
 "health": 90.82934, "velocity": {"value": 68.838615, "P6": 0.751816, "blend": 0.593242,
 "blend_parts": {"net_flow_30d": 0.54, "chain_buys": 0.679725, "attention_7d": 0.56}},
 "composite_raw": 87.530731, "flag_multiplier": 1.0, "composite": 87.530731, "composite_smoothed": 81.957683,
 "prev": {"as_of": 1790035200, "composite_smoothed": 80.1, "tier": "A"}, "tier": "A",
 "confidence": 0.983333, "insufficient_data": false, "coverage": {"P1": 0.95, "P2": 1.0, "P3": 1.0, "P4": 1.0, "P5": 1.0, "P6": 1.0},
 "pillars": {"P1": {"score": 0.857835, "weight": 30, "points": 28.594504, "metrics": {
     "emission_share": {"m": 0.949556, "raw": 9.649, "w": 0.2, "st": "ok", "age_s": 0, "cohort_n": 122, "impact": 3.15478}, "…": {}}}},
 "top_positive": [{"metric": "emission_share", "pillar": "P1", "impact": 3.15478}],
 "top_negative": [{"metric": "discord_growth_30d", "pillar": "P5", "impact": -0.2}],
 "flags": [],
 "missing": ["participation"]}
```
(SN64 row of the worked example, §2.14. A flagged row lists e.g. `{"flag": "dereg_imminent", "multiplier": 0.6, "since": 1789948800, "state": "active", "origin": "auto", "human": false, "sticky": false, "clear_streak_days": 0, "detail": {"dereg_risk_flag": 1, "immune_flag": 0}}` — `explain.flags` is the only place multiplier/since/state/origin/detail are stored, 02 §B6.)


- `points_k = 100 · W_k·P_k / Σ_{H} W` (sums to Health). **Impact** of metric `i` in Health pillar `k ∈ H`: `impact_i = 100 · (W_k / Σ_H W) · (w_i / A_k) · (m_i − 0.5)`, `A_k` = available within-pillar weight. Identity (tested): `Σ impact_i = Health − 50`. P6 metrics carry `impact_v = 100·0.6·(w_i/A_6)·(m_i − 0.5)` and are excluded from the top lists.
- `top_positive` = up to 3 metrics with `impact > 0`, sorted by `(−impact, metric_key)`; `top_negative` = up to 3 with `impact < 0`, sorted by `(impact, metric_key)`. `missing` = scored keys with `m = None` (sorted).
- Official rows carry the full `pillars.*.metrics` block; provisional rows omit it (the provisional `stage='norm'` rows hold the same data for 14 days).

### 2.11 Events emitted (`events`, primary spec only)

Written in the run transaction via `app/scoring/events.py::emit()`, a thin wrapper over 03's `app/storage/events.py::emit_event` (02 §D1: it rejects an unknown `event_type` or a missing required payload key — extra keys are allowed — and inserts `ON CONFLICT DO NOTHING` on `ux_events_dedupe (event_type, dedupe_key)`; a nested `db.transaction()` joins the run's transaction). Columns (03 §3.6): `occupant_key, netuid, event_type, severity ∈ {info, warning, critical}` (02 §B2), `event_at = slot, observed_at = computed_at, source = 'scoring'` (02 §B9), `payload (JSON), dedupe_key, run_id, alerted_at = NULL`. System events (`rank_instability`) have `occupant_key` and `netuid` NULL (03's CHECK). Not emitted for shadow specs, when `SCORING_EVENTS_ENABLED=false`, or when killed. Reconstructed (catch-up) runs emit with `payload.backfilled = true`. Names, required payload keys and dedupe patterns are 03 §3.9's (02 §D1/D2); `{occ}` = `occupant_key`.

| event_type | when | severity | payload (required keys first; extras after `+`) | dedupe_key |
|---|---|---|---|---|
| `tier_change` | official, `tier ≠ prev.tier`, both non-null, neither row `insufficient_data` | `info` up / `warning` down | `{netuid, spec_version, as_of, from, to, direction, composite_smoothed, prev_composite_smoothed, composite, threshold, top_positive, top_negative, red_flags (names), backfilled?}` | `{occ}\|tier_change\|{spec}\|{as_of}` |
| `red_flag_raised` | transition to `active = 1` (any run kind) | `critical` if multiplier ≤ 0.6 else `warning` | `{netuid, spec_version, as_of, flag, multiplier, human, sticky, detail, flag_multiplier_total}` (02 §D2; `detail` = the state row's `detail_json`) + `since` (= `raised_at`), `trigger_event_id`, `origin`, `backfilled?` | `{occ}\|red_flag_raised\|{flag}\|{raised_at}` |
| `red_flag_cleared` | transition to `active = 0` | `info` | `{netuid, spec_version, as_of, flag, multiplier, raised_at, reason: "hysteresis"\|"manual"}` + `cleared_at`, `clear_streak_days`, `origin`, `backfilled?` | `{occ}\|red_flag_cleared\|{flag}\|{as_of}` |
| `score_jump` | official, `prev_official.composite` non-null, `abs(composite − prev) ≥ SCORE_JUMP_ALERT_POINTS` | `warning` | `{netuid, spec_version, as_of, prev_as_of, prev_composite, composite, delta, pillar_deltas: {P1..P6}, flags_added, flags_removed, coverage_delta}` | `{occ}\|score_jump\|{spec}\|{as_of}` |
| `insufficient_data` | official, row enters `insufficient_data` (previous official row was sufficient or absent) | `info` | `{netuid, spec_version, as_of, confidence, coverage: {P1..P5}, missing}` | `{occ}\|insufficient_data\|{spec}\|{as_of}` |
| `rank_instability` | official, series `health`, lag 1, `rank_autocorr < RANK_AUTOCORR_ALERT_BELOW` (§2.12) | `info`, `occupant_key` NULL | `{spec_version, as_of, series, lag_days, n, rank_autocorr, quintile_turnover, top20_turnover}` (02 §D2) + `threshold` | `system\|rank_instability\|{spec}\|{as_of}` |

Routing is spec 14's and follows 02 §D4: `tier_change`, `red_flag_raised`, `red_flag_cleared` and `score_jump` are immediate; `insufficient_data` and `rank_instability` go to the digest; any event with `payload.backfilled = true` goes to the digest regardless of type. The producers' detection events for flag conditions (`emission_disabled`, `dereg_risk_entered`, `site_down_7d`, `owner_extraction_detected`, `owner_perpetual_off`, `owner_control_change`, …) are digest-only; the page is `red_flag_raised`/`red_flag_cleared`. Runtime conditions go through `AlertLatch.fire(...)` (02 §D3), which writes a system `events` row via `emit_event` (falling back to a direct `send_alert` if the DB write fails): `scoring_stalled` when `now − latest primary as_of > SCORING_STALE_AFTER_MIN·60` — `event_type='scoring_stalled'`, `severity='critical'`, `source='scoring'`, payload exactly 03 §3.9's row `{slot_as_of, last_ok_as_of, missed_slots, last_error}` (`slot_as_of` = the first scheduled primary slot after `last_ok_as_of` with no `scores` row, as unix s; `last_ok_as_of` = the latest primary `as_of`; `missed_slots` = scheduled slots in `(last_ok_as_of, now]` without a row; `last_error` = the newest failed `scoring_6h` run's error text, or `null`; `spec_version`/`age_min` may be added as extras), dedupe `{slot_as_of}` (catalogued in 03 §3.9, alert yes); `clear()` sends the recovery text when a run lands. The wrapper's `collector_failing:scoring_6h` is 01's.

### 2.12 Rank-stability monitors (`app/scoring/stability.py`, step `rank_autocorr_health`)

After each official primary run (not shadow, not killed), for `series ∈ {health, composite_smoothed}` (02 §D2 and 03's CHECK; `health` = the `scores.quality` column) and `lag_days ∈ {1, 7}`: take occupants with rows at both `slot` and `slot − lag` (official, same spec, `status='active'`, `insufficient_data = 0`, value non-null), **sorted by `netuid`** before any call (the ported `quantile_buckets` breaks ties by input position, so the order must be fixed for determinism); `n < 10` → NULL stats. Compute with `app/util/mathutil.py` (ported `utils/math.py`):
- `rank_autocorr = spearman_r(values_t, values_t−lag)` (old `math.py:45-54`; returns 0.0 when n < 5 or all tied);
- `quintile_turnover` = share of the common occupants whose `quantile_buckets(values, 5)` (old `math.py:73-88`) bucket differs;
- `top20_turnover = |top20_t \ top20_{t−lag}| / min(20, n)` (top by value, ties by `netuid`).
One `rank_stability_daily` row per (series, lag) with 03's columns `(as_of = slot, spec_version, series, lag_days, n, rank_autocorr, quintile_turnover, top20_turnover, computed_at)`, key `(as_of, spec_version, series, lag_days)` — four rows per official run, `INSERT OR IGNORE`. Master §7.7 target: `rank_autocorr ≥ 0.9` on Health, lag 1 — below it emits `rank_instability` (series `health`, `lag_days = 1`, §2.11). The offline equivalent is spec 13's `rank-stability` CLI.

### 2.13 `spec_version`, shadow runs, rescore (`app/scoring/spec.py`)

```python
@dataclass(frozen=True)
class ScoringSpec:
    version: str                                   # "ts-1.0"
    raw_spec_version: str                          # raw metric definitions it reads ("ts-1.0")
    metrics: Mapping[str, MetricDef]               # registry used (ts-1.0: registry.METRICS)
    pillar_weights: Mapping[str, float]            # {P1:30, P2:20, P3:10, P4:18, P5:12}
    pillar_min_coverage: float = 0.3
    velocity_weights: tuple[float, float] = (0.6, 0.4)          # (P6, blend)
    blend_keys: tuple[str, ...] = ("net_flow_30d", "chain_buys", "attention_7d")
    v_w_by_regime: Mapping[str, float] = MappingProxyType({"TRENDING": .25, "DISPERSED": .25, "CHOPPY": .15, "DEAD": .10, "UNKNOWN": .15})
    flags: Mapping[str, FlagDef] = FLAGS_TS_1_0
    flag_floor: float = 0.25
    ema_span: int = 7
    ema_max_gap_days: int = 7
    tier_lower: Mapping[str, float] = MappingProxyType({"A": 75.0, "B": 60.0, "C": 45.0, "D": 30.0})
    tier_hysteresis: float = 3.0
    insufficient_below: float = 0.4
    rankg_clip_z: float = 2.5
    rankg_min_n: int = 10
    manual_clear_suppress_days: int = 30
    regime: RegimeParams = REGIME_TS_1_0

SPECS: Final[Mapping[str, ScoringSpec]] = MappingProxyType({"ts-1.0": TS_1_0})
GOLDEN_SPEC_HASH: Final = {"ts-1.0": "<sha256 filled at implementation time; test pins it>"}
GOLDEN_REGISTRY_HASH: Final = {"ts-1.0": "<sha256>"}
def get_spec(version: str) -> ScoringSpec            # KeyError → SpecUnknown
def spec_hash(spec: ScoringSpec) -> str              # sha256 of canonical JSON (sorted keys, floats repr, fns by qualified name)
```

Rules:
1. `ts-1.0` is frozen once released: `spec_hash(TS_1_0) == GOLDEN_SPEC_HASH["ts-1.0"]` is asserted at import and in CI. Any change to weights, params, flags, thresholds, EMA or regime params — or to the registry — requires a new `ScoringSpec` with a new `version` (`ts-1.1`, or `ts-1.0.1` for a bug fix that changes outputs). Code refactors that keep outputs identical keep the version (the golden test proves it).
2. A new spec whose metric definitions change sets `raw_spec_version` to a new string; the affected producers then write raw rows under both versions until the old spec is retired. Specs that only change weights/params reuse `raw_spec_version='ts-1.0'`. Producer-side formulas are part of the raw definition: the pre-normalised P4 values (spec 11, whose ts-1.0 constants are pinned by its Settings validators) and 08's presence constants — changing any of them requires a new `raw_spec_version` under which the producer writes. `spec_hash`/`registry_hash` cannot detect such a change, so producer specs must treat their ts-1.0 constants as frozen (11 validates them in Settings; 08 keeps them in `spec.py`).
3. **Shadow runs.** `SCORING_SHADOW_SPECS="ts-1.1"` makes every scheduled run also score each listed spec after the primary, sharing `compute_registered`/`derive` and (for equal params) the regime; shadow rows have `scores.shadow = 1`, their own `stage='norm'` rows (keyed by the shadow `spec_version`), `red_flag_state` rows and `score_runs` rows, and **emit no events and no alerts**. The feed (15) and API (14) serve `SPEC_VERSION` unless `?spec=` names another version.
4. **Promotion**: set `SPEC_VERSION=ts-1.1` (restart; the old version may stay in `SCORING_SHADOW_SPECS`). The new series already has history from its shadow period; the bot ignores it until whitelisted (15).
5. **Rescore backend** (`ScoringService.rescore(as_of: date | None, spec_version: str, shadow: bool) -> str`, called by spec 14's `POST /control/rescore` as a background task; `run_id = "rs-YYYYMMDD-HHMMSS"`): `as_of=None` → the current slot; a date → that day's official slot, reconstructed mode. Refused (the endpoint maps to 409/422/423): a run in flight; `SCORING_KILLED` present; the slot already has rows for that spec (immutability — use a new version); the slot is older than the spec's latest completed slot (state would regress). CLI `python -m app.scoring backfill --spec ts-1.1 --from 2026-06-01 [--to …]` scores a new shadow spec's official slots oldest-first (reconstructed), which is how a candidate spec gets comparable history for spec 13.

### 2.14 Worked numerical example (golden fixture; arithmetic re-verified in Python 3.14.3 on 2026-09-23 after the 02 reconciliation)

All inputs are **synthetic fixture values** chosen to exercise every rule (they are not claims about the live subnets); the golden test (§9) loads exactly these raw rows and asserts every number below to ±1e-4 (scores to ±1e-3). Official slot `as_of = 1790121600` (2026-09-23T00:00Z), strict mode, `computed_at = 1790122504`; regime `CHOPPY` → `v_w = 0.15`. P1 `rankg` cohort n = 122 active occupants; `x_engagement_30d` cohort n = 58. Occupants: **SN64** `64:4213977` (age 600 d) and **TAIL** — fixture occupant `200:6970000` (age 300 d, non-immune, dead Discord invite as its only venue, no X/TG, GitHub org with no commit in 214 d, anonymous team).

`rankg` detail: `m = (clip(Φ⁻¹((r0+0.5)/n), ±2.5) + 2.5)/5`, `r0` = 0-based ascending average rank.

**P1 (W = 30)**

| metric (w) | SN64 raw → m | TAIL raw → m |
|---|---|---|
| `emission_share` (.20) | 9.649 %, r0 = 120 → p .98770, z 2.2478 → **.94956** | 0.0, tied with 36 others (r0 = 18) → p .15164, z −1.0294 → **.29411** |
| `gate_margin` (.15) | ratio 12.18, log2 3.606 → **1.0** | ratio 0.12, log2 −3.059 → **0.0** |
| `dereg_safety` (.15) | rank-from-bottom 108 of 110 → 107/109 = **.98165** | rank 3 (bottom-3) → **0.0** |
| `liquidity_depth` (.10) | r0 = 119 → p .97951, z 2.0437 → **.90874** | r0 = 4 → p .03689, z −1.7880 → **.14239** |
| `exit_capacity` (.05) | 0.359 → **.56182** | 0.41 → **.65455** |
| `net_flow_30d` (.10) | +0.04 → **.54** | −0.20 → **.30** |
| `chain_buys` (.05) | r0 = 99 → p .81557, z .8986 → **.67972** | 0.0, tied ×41 (r0 = 20) → p .16803, z −.9620 → **.30761** |
| `holder_dispersion` (.05) | 0.55 → **.41667** | 0.14 → **0.0** |
| `participation` (.05) | failed (keyless) → None | None |
| `miner_burn_ok` (.10) | 0.98 → **1.0** | predicted share < 1e-4 → None |
| **P1** | Σw·m .81494 / cov .95 = **.857835** | Σw·m .151170 / cov .85 = **.177847** |

**P2 (W = 20)** — SN64: authors 14 → 1.0; FTE 5 → .81546; commit-days 84 → .98488; declared push 1 d → 1.0; org push 0 d → 1.0; releases 4 → .82709; review .62 → .62; bus factor 3 → 1.0; hygiene 4/5 → .80; originality .97 → .97 ⇒ **P2 = .930906** (cov 1.0). TAIL: 0, 0, 0; declared 214 d → 0; org 214 d → 0; releases 0 → 0; review `not_applicable` (0 merged PRs) → None; bus 0 → 0; hygiene .40; originality .35 ⇒ Σ .0375 / cov .95 = **P2 = .039474**.

**P3 (W = 10)** — SN64: uptime 29/30 = .96667, TLS 1, age 700 d → 1.0, freshness 12 d → 1.0, docs 1, product `live` 1.0 ⇒ **P3 = .99**. TAIL: uptime .60, TLS 1, age 410 d → 1.0, freshness 300 d → 1 − 210/275 = .23636, docs 0, product unset → None ⇒ Σ .41545 / cov .80 = **P3 = .519318**.

**P4 (W = 18)** — spec 11 writes the final values (normaliser `prenorm` on rows 27–32; `tenure` is raw days — 02 §C1); they are shown with the inputs that produce them. SN64 (fixture: auto-lock off): team 2 verified → 1.0; owner identity 1; commitment `.6·.85 + .25·0 + .15·[.225 ≥ .05]` = .66; retention sell_ratio .10 → .90; control stability (no events) 1.0; overhang .03 → 1.0; tenure 600 d → 1.0 ⇒ **P4 = .912**. TAIL: team absent → 0; owner identity absent → 0; commitment (perpetual lock 0 ∧ auto-lock off ∧ age 300 > 90) → 0; retention .80 → .20; control 1.0; overhang .18 → 1 − .13/.20 = .35 (fixture `legacy_unaccounted = false`, i.e. no −0.2 legacy penalty); tenure 300 d → (300−120)/245 = .73469 ⇒ **P4 = .280969**.

**P5 (W = 12)** — SN64: venue 1; members 9 000 → 1.0; presence .12 → 1.0; growth +.04 → .35; announcements = x 18 + discord 4 (tg leg None: no public channel, ignored) = 22 → 1.0; x engagement r0 = 56 of 58 → p .97414, z 1.9454 → .88908; followers 42 000 → .98389; silence .5 d → 1.0 ⇒ **P5 = .923103**. TAIL: venue 0 (invite 404, `absent`); members/presence/growth None (invite invalid); announcements: no `ok` leg → None; x engagement None; followers absent (`no_x_account`: no `watch_channels(platform='x')` row; X legs absent — 02 §C3) → 0; silence `absent` (no reachable venue ∧ no X account, 02 §C3) → 0 ⇒ coverage = .10 + .05 + .15 = **.30 (not < .30 → kept)**, **P5 = 0.0**.

**Health and confidence**

| | SN64 | TAIL |
|---|---|---|
| W·P for P1…P5 | 25.73505, 18.61812, 9.90000, 16.41600, 11.07723 | 5.33540, 0.78947, 5.19318, 5.05745, 0.00000 |
| **Health** = 100·ΣW·P / 90 | 100·81.74641/90 = **90.8293** | 100·16.37551/90 = **18.1950** |
| confidence = ΣW·cov / 90 | (30·.95+20+10+18+12)/90 = **.98333** | (30·.85+20·.95+10·.80+18+12·.30)/90 = 74.1/90 = **.82333** |

**P6, blend, Velocity**

| | SN64 | TAIL |
|---|---|---|
| `event_impact_14d` (.40) | x = 2·0.5^(3/7) + 2·0.5^(9/7) = 1.48599 + 0.82034 = 2.30633 → (tanh(1.15317)+1)/2 = **.90940** | x = 0 (coverage ≥ .5) → **.5** |
| `polarity_7d` (.25) | **.64** | < 5 posts → None |
| `attention_7d` (.20) | z .8 → **.56** | z −.5 → **.30** |
| `kol_coverage_30d` (.15) | 3 KOLs → **.77371** | X provider null → None |
| **P6** | **.751816** (cov 1.0) | (.4·.5 + .2·.3)/.6 = **.433333** (cov .6) |
| blend = mean(net_flow, chain_buys, attention) | (.54 + .679725 + .56)/3 = **.593242** | (.30 + .307606 + .30)/3 = **.302535** |
| **Velocity** = 100·(.6·P6 + .4·blend) | 45.10895 + 23.72967 = **68.8386** | 26.00000 + 12.10142 = **38.1014** |

**Composite, flags, EMA, tier, events**

| | SN64 | TAIL |
|---|---|---|
| composite_raw = .85·H + .15·V | 77.2049 + 10.3258 = **87.5307** | 15.4658 + 5.7152 = **21.1810** |
| active flags | none | `dereg_imminent` ×.60 (raised at slot 2026-09-21, still True); `stale_identity` ×.90 (raised now: dead venue ∧ `identity_changed` subnet_name 40 d ago ∧ silence absent) |
| flag_multiplier | **1.0** | max(.25, .60·.90) = **.54** |
| composite | **87.5307** | 21.1810·.54 = **11.4377** |
| prev official (2026-09-22) | smoothed 80.10, tier A | smoothed 31.40, tier D, composite 19.02 |
| composite_smoothed = .25·C + .75·prev | 21.8827 + 60.0750 = **81.9577** | 2.8594 + 23.5500 = **26.4094** |
| tier | A (81.96 > 72 → stays) | 26.41 ≤ 30 − 3 → **F** |
| `scores.red_flags` | `[]` | `["dereg_imminent","stale_identity"]` (names only, 02 §B6) |
| events | none | `red_flag_raised{flag: stale_identity, multiplier: .9, human: false, sticky: false, detail, flag_multiplier_total: .54}` (warning), `tier_change D→F` (warning); `score_jump` not emitted (abs(11.44 − 19.02) = 7.58 < 15) |
| explain top_positive | `emission_share` +3.155, `gate_margin` +2.632, `dereg_safety` +2.535 | `control_stability_90d` +1.500, `site_age` +0.694, `tls_ok` +0.694 |
| explain top_negative | `discord_growth_30d` −0.200, `holder_dispersion` −0.146 (only two < 0) | `silence_days` −3.333, `dereg_safety` −2.941, `gate_margin` −2.941 |
| Σ impacts (= Health − 50) | 40.8293 | −31.8050 |

Reading the example: TAIL's zero emission still scores `.294` on `emission_share` because 37 occupants tie at zero and share an average rank (master rule; §11 Q6), but `gate_margin`, `dereg_safety`, the P2 zeros and the two flags drive it to F. SN64's only drags are growth and holder concentration.

### 2.15 PIT contract (for spec 13) and read contract (for 14/15)

1. **No look-ahead by construction.** Every `stage='norm'` row and `scores` row of a strict run was computed only from raw rows with `observed_at ≤ cutoff = computed_at`, events/manual rows with `observed_at`/`set_at ≤ cutoff`, and `chain_hourly` rows with `observed_at ≤ cutoff`. `scores` stores the run instant in `computed_at` and the admitted cutoff in `input_cutoff` (together the PIT key, 02 §B6); `stage='norm'` rows carry `observed_at = computed_at` (02 §B3).
2. **Reconstructed rows are marked.** Catch-up, rescore and `backfill` runs write `backfilled = 1` on `stage='norm'` rows, `scores` rows and `regime_daily` rows (and on fn/derived raw rows). Their effective observation time for spec 13's `strict` PIT mode is unknown-late → spec 13 drops them in `strict` and uses `eff = as_of + lag_s` in `reconstructed` mode (its §2.5 `RECONSTRUCTIBLE_KEYS`).
3. **Normalised factor** `k` at decision time `t` (spec 13 "any metric_key"; 02 §J2): `SELECT occupant_key, as_of, value AS m, json_extract(value_json,'$.raw') AS raw, observed_at, backfilled FROM metric_values WHERE stage='norm' AND spec_version=:S AND metric_key=:k AND value IS NOT NULL` → per occupant the row with the greatest `as_of` among rows with `observed_at ≤ t` (strict) and `t − as_of ≤ METRICS[k].freshness_s`. **`raw:<k>`** = `value_json.raw` of the **same norm row**. `stage='raw'` rows are PIT-safe too (first success wins, 02 §B4) and remain the source for keys this spec does not normalise, but factor studies of scored metrics read the norm rows so that the value is exactly what scoring used.
4. **Scores factors**: `scores` with `spec_version=:S AND shadow` as intended (`shadow=0` for the primary series), `eff = computed_at` (02 §B6, §J2), `run_kind='official'` for the 24 h grid, all run kinds for the 4 h grid; `max_age = QUANT_SCORE_MAX_AGE_S` (spec 13). `pillar_scores` keys `"P1"…"P6"`, values in [0,1] or null.
5. **Regime**: `regime_daily(day_ts, spec_version, label)` (02 §B8/§J2), one row per official primary slot (`day_ts = as_of` at 00:00Z).
6. **Freshness windows**: `METRICS[k].freshness_s` (§2.4). **Pillar membership**: `PILLAR_OF[k]`.
7. **History before Phase 8**: produced only by `python -m app.scoring backfill --spec ts-1.0 --from <date>` over reconstructible raw rows (05/06/17 backfills). Those rows are `backfilled = 1`; forward-only metrics are `None` in them, so historical Health is effectively the P1+P2 renormalisation (= spec 13's `quality_cg` intent).
8. **Read contract for 14/15** (02 §H1/H2): latest score per occupant = max `as_of` for the spec (03's `ix_scores_latest`, `v_latest_scores`); `red_flags` is a JSON array of flag **names** sorted by name (02 §B6) that 14 and 15 read directly (multiplier/since/state/detail come from `explain.flags`); `composite` in the API/feed = `composite_smoothed`; `quality` = Health; `as_of` (slot instant) and `computed_at` are INTEGER unix seconds, converted to ISO only at the API boundary; evidence = the latest `stage='norm'` row per metric (`value` = m, `value_json.raw` = raw, `value_json.status` = raw status — `absent` shown differently from `failed`/`none`), freshness from `METRICS[k]`.

---

## 3. Data contract

Spec 03 owns the final DDL (master §6, 02 §B7): `metric_values`, `scores`, `events`, `red_flag_state`, `regime_daily`, `rank_stability_daily`, `score_runs` and `manual_flags` (03 §3.1, §3.2, §3.6, all in schema v1). This spec uses those tables **as 03 defines them** and ships **no** `CREATE TABLE`/`ALTER TABLE`; §3.1 lists the columns it writes, by 03's names. There is no `metric_norm` table (02 §B3). All times INTEGER unix seconds UTC (02 §B1).

### 3.1 Tables written

Columns this spec writes (03's DDL is authoritative; no DDL here, 02 §B7):

| table (03 §) | columns written by 12 | rules |
|---|---|---|
| `metric_values`, `stage='raw'` (§3.6) | `occupant_key, metric_key, as_of, spec_version (ts-1.0), stage='raw', value, value_json {status, reason, …inputs}, observed_at, source, run_id, backfilled` | only for the 08/09/10 (and any registered 07/11) fn results and the two derived keys; through `put_metric_values` (02 §B4 first success wins); `source` per `SOURCE_OF` (02 §B9); generated `status` never written |
| `metric_values`, `stage='norm'` (§3.6) | `occupant_key, metric_key (45 scored keys), as_of = slot, spec_version = scoring spec, stage='norm', value = m ∈ [0,1] or NULL, value_json {raw, status, reason, raw_as_of, raw_observed_at, age_s, src, fallback, cohort_n}, observed_at = computed_at, source='scoring', run_id, backfilled` | 02 §B3; insert-once (`ON CONFLICT DO NOTHING`, 02 §B4); provisional rows (`as_of % 86400 ≠ 0`) pruned after `METRIC_NORM_PROVISIONAL_RETENTION_DAYS`, official rows kept forever |
| `scores` (§3.6) | `occupant_key, netuid, as_of = slot, spec_version, run_kind, run_id, status (active\|pre_launch), quality (= Health), velocity, composite_raw, flag_multiplier, composite, composite_smoothed, pillar_scores {P1..P6}, pillar_coverage {P1..P6}, confidence, insufficient_data, tier, prev_tier, red_flags (sorted names), regime (label), v_w, explain, computed_at = T0, input_cutoff = cutoff, backfilled, shadow` | 02 §B6: key `(occupant_key, as_of, spec_version)`, `INSERT` only (03's triggers abort UPDATE/DELETE: 'scores is append-only'); no `observed_at`; EMA prev lookup uses 03's `ix_scores_occ` |
| `red_flag_state` (§3.1) | `occupant_key, flag, spec_version, active, raised_at, last_true_at, false_since, cleared_at, human, sticky, last_manual_clear_at, suppress_until, last_eval, last_eval_as_of, detail_json, updated_at` | 02 §B7; UPSERT (03 class `state`); `clearing`, `origin`, `clear_streak_days` and the multiplier are derived (§2.8) |
| `regime_daily` (§3.6) | `day_ts = official slot, spec_version, label, raw_regime, vol_24h, directional_strength, dispersion, n_subnets, thin_universe, v_w, tao_usd_ret_24h, sum_prices_ret_24h, inputs_json, source='live', computed_at, backfilled, run_id` — `label` per 02 §B8 (03's draft calls it `market_regime`) | `INSERT OR IGNORE`, official primary slots only; labels uppercase `TRENDING`, `CHOPPY`, `DISPERSED`, `DEAD`, `UNKNOWN`; no `observed_at` (02 §B6) |
| `rank_stability_daily` (§3.6) | `as_of, spec_version, series (health\|composite_smoothed), lag_days (1\|7), n, rank_autocorr, quintile_turnover, top20_turnover, computed_at` | §2.12; `INSERT OR IGNORE` |
| `score_runs` (§3.6) | `run_id, spec_version, as_of, run_kind, mode, trigger, shadow, killed, backfilled, started_at, finished_at, ok, spec_hash, regime, n_universe, n_scored, n_prelaunch, n_insufficient, n_events, timings_json {compute_registered_s, snapshot_s, score_s, write_s, fn_errors, n_errors}, error` | one row per (run, spec); inserted at start (`ok` NULL), finished in the run transaction (or by a separate `ok=0` update after a rollback) |

Also written: `events` (§2.11; 03 §3.6 columns incl. `source='scoring'`, `dedupe_key`, `run_id`), `collector_runs` (by the spec-01 wrapper for `features_hourly` and `scoring_6h`).

**Write discipline.** One `BEGIN IMMEDIATE … COMMIT` per (slot, spec) through 03's `Database.transaction()` (nested helper calls join it): `stage='norm'` rows (`put_metric_values`, insert-once), `scores` (`INSERT`; a UNIQUE conflict aborts the transaction and the run records `ok=0, error='slot_already_scored'` — the pre-check in §7 makes this unreachable in normal operation), `red_flag_state` (UPSERT — 03 class `state`), `regime_daily` and `rank_stability_daily` (`INSERT OR IGNORE`), `events` (`emit_event`, `ON CONFLICT DO NOTHING`), `score_runs` finish update. A crash mid-transaction leaves nothing; the slot is retried by catch-up. Provisional `stage='norm'` rows older than `METRIC_NORM_PROVISIONAL_RETENTION_DAYS` are deleted (`DELETE FROM metric_values WHERE stage='norm' AND as_of % 86400 <> 0 AND as_of < ?`, 10 000-row batches — 03's delete trigger permits only provisional rows) after the official run commits (derived data; `scores.explain` keeps what was used). Raw rows written by `compute_registered`/`derive` go through `put_metric_values` in their own transaction before the snapshot (they are observations, valid even if the scoring transaction later fails).

### 3.2 Tables read

| table (owner) | columns | use |
|---|---|---|
| `metric_values` (03; `stage='raw'` rows from 05–11, 12) | `occupant_key, metric_key, as_of, spec_version, stage, value, value_json, status (generated), observed_at, source, run_id, backfilled` | §2.2 selection |
| `occupants` (04; DDL 03) | `occupant_key, netuid, network_registered_at, status, first_seen_at, activated_at, deregistered_at` (INTEGER) | universe, pre-launch |
| `watch_channels` (04; DDL 03) | `occupant_key, platform, first_seen_at, retired_at` | "no X account" test for `silence_days` (§2.6, 02 §C3) |
| `events` (03; rows from 04, 07) | `id, occupant_key, event_type, event_at, observed_at, payload` for `identity_changed`, `identity_junk`, `site_down_7d`, `site_recovered` | flag triggers, `detail.trigger_event_id` |
| `manual_flags` (03; rows from 14, 11) | `occupant_key, flag, state, evidence_url, set_by, set_at` | human-confirmed flags, manual clears (02 §D5) |
| `chain_hourly` (05) | `netuid, hour_ts, price, is_active, observed_at, backfilled` | regime |
| `chain_network_hourly` (05) | `hour_ts, tao_usd, sum_subnet_prices` | regime diagnostics |
| `scores`, `red_flag_state`, `score_runs` (own) | all | EMA/tier `prev`, flag state, catch-up planning |

### 3.3 metric_keys produced

- **Raw (this spec as producer, `source='scoring'`, 02 §B9)**: `announcement_cadence_30d`, `silence_days` (§2.6, 02 §C3). It also writes, on their behalf, the raw rows of the registered feature fns (08/09/10, incl. attached evidence keys) with their producers' `source` (`community`, `x`, `rss`, `llm`).
- **Normalised (`stage='norm'`, 02 §B3)**: all 45 scored keys of §2.4 for every universe occupant at every slot, per scoring spec. Normalisers and weights exactly as the §2.4 table (= master §7.3 + 02 §C1–C7); pillar weights P1 30, P2 20, P3 10, P4 18, P5 12; P6 → Velocity only.

### 3.4 Events emitted

`tier_change`, `red_flag_raised`, `red_flag_cleared`, `score_jump`, `insufficient_data`, `rank_instability` (02 §D1, catalogued in 03 §3.9), plus the system event `scoring_stalled` via `AlertLatch` (catalogued in 03 §3.9: payload `{slot_as_of, last_ok_as_of, missed_slots, last_error}`) — schemas in §2.11.

## 4. Failure-mode table (feature store and scoring)

The collector-level absence-vs-failure decisions live in 05–11; this table fixes how the engine turns every condition into numbers, rows and alerts.

| # | condition | effect on metrics / scores | persisted trace | alert |
|---|---|---|---|---|
| 1 | Producer row `value_json.status='absent'` | `m = 0.0`, counts as covered | norm row `value_json.status='absent'` | — |
| 2 | Latest fresh producer row `failed`, an older fresh row observed | the older row is used (02 §B5 `metric_at` semantics; inside the freshness window only) | norm row `value_json.raw_as_of` < latest producer `as_of` | — |
| 2a | Every fresh row `failed`/`none`, or the row is `not_applicable` | `m = None`, dropped, coverage ↓ | `value_json.status` + `reason` in the norm row | — |
| 3 | No raw row within the freshness window (producer down, job disabled, laptop off) | `m = None` (`reason='no_fresh_row'`); **never forward-filled** | norm row with `value` NULL (status key omitted → `none`) | producer's own `collector_failing` (01) |
| 3a | `prenorm` value outside [0,1] (producer bug, spec 11) | `m = None` (`reason='prenorm_out_of_range'`) | norm row `value_json.reason` | log warning once per (metric, run) |
| 4 | Pillar coverage < 0.3 | `P_k = None`, Health renormalises over remaining pillars | `pillar_scores.Pk = null`, `pillar_coverage` | — |
| 5 | All Health pillars None | Health, composite, smoothed, tier = NULL; `confidence` ≈ 0 → `insufficient_data = 1` | score row still written | `insufficient_data` event (digest) |
| 6 | Only P1 available (06–11 down) | Health = P1 only, confidence ≤ .333 → `insufficient_data` | idem | idem |
| 7 | `rankg` cohort < 10 | all cohort members `None` (`cohort_too_small`) | reason in the norm rows | log warning once per run |
| 8 | Producer fn raises / times out | `failed` raw rows for affected occupants (`fn_error`/`fn_timeout`); run continues | `score_runs.timings_json.fn_errors` | log error; none |
| 9 | Producer module not importable (later phase) | no rows → case 3 | log `producer_unavailable` once per process | — |
| 10 | `chain_hourly` thin (< 10 subnets with 6 buckets) | regime `UNKNOWN`, `v_w = 0.15` | `regime_daily.inputs_json.thin_universe = true` | — |
| 11 | P6 and blend both None | Velocity None → `composite_raw = Health` | `velocity = NULL` | — |
| 12 | `manual_flags` empty (spec 14 not built yet; 03 creates the table up front) | human flags inactive; manual clears impossible | — | — |
| 13 | `events` rows of 04/07 missing | `site_down_7d` → None or False per §2.8; `name_turnover` False | — | — |
| 14 | `./SCORING_KILLED` exists (02 §B12) | everything computed in memory; **no** `stage='norm'`, `scores`, `red_flag_state`, `regime_daily`, `rank_stability_daily` or `events` rows — only `score_runs(killed=1)` (raw fn/derived observations are still written, as `features_hourly` would); catch-up never backfills killed slots | `score_runs`, `/health.scoring_killed` | — (operator action) |
| 15 | `SCORING_ENABLED=false` | job skipped (`disabled`) | none | — |
| 16 | Slot already scored for the spec | skip (idempotent) | wrapper `rows=0, note='already_scored'` | — |
| 17 | Laptop off across slots | catch-up of ≤ `SCORING_CATCHUP_MAX_SLOTS` official slots, reconstructed, `backfilled=1`; older gaps stay empty | `score_runs.trigger='catchup'` | — |
| 18 | DB locked / write error | transaction rolled back; `score_runs.ok=0`; the slot is retried by the next run's catch-up | `collector_runs.ok=0` | `collector_failing:scoring_6h` on the 5th |
| 19 | Exception while scoring one occupant | that occupant is skipped (no row), `timings_json.n_errors + 1` (03's `score_runs` has no `n_errors` column); if > 10 % of the universe fails the whole slot aborts (rollback) | `score_runs.error` | wrapper alert if repeated |
| 20 | Registry invalid or `spec_hash` ≠ golden | `RegistryError`/`SpecHashMismatch` at import → boot fails loudly (spec 01 §2.7: only `ImportError`/`ModuleNotFoundError`/`AttributeError` are downgraded to "unavailable", so both classes subclass plain `Exception`, never `ImportError`) | journal | systemd restart loop visible in `/health` absence |
| 21 | `SPEC_VERSION` not in `SPECS` | `scoring_6h` refuses every run (`error='spec_unknown'`) | `/health.degraded_reasons += spec_unknown` | wrapper alert on 5th |
| 22 | Clock behind the slot (`T0 < slot`) | run refused (`clock_before_slot`) | `score_runs.error` | — |
| 23 | No new primary score for > `SCORING_STALE_AFTER_MIN` | feed/API report `stale` (14/15) | `/health.scores.stale = true`, `degraded_reasons += scores_stale` (01's name) | `AlertLatch.fire("scoring_stalled", …, event_type="scoring_stalled")` (02 §D3) + recovery text |

---

## 5. Configuration

Keys from spec 01's inventory are marked (01) and keep their defaults; the rest are new. All read dynamically via `app_config.settings` at run time (spec 01 dynamic-settings rule). **Weights, normaliser params, flag multipliers, tier thresholds, EMA span, regime params and the insufficient-data threshold are not config** — they are frozen in `spec.py`/`registry.py` per `spec_version`.

| key | type | default | validator / notes |
|---|---|---|---|
| `FEATURES_ENABLED` (01) | bool | `True` | gates `features_hourly` only; scoring always runs its own `compute_registered` |
| `FEATURES_INTERVAL_MIN` (01) | int | `60` | 10–1440 |
| `SCORING_ENABLED` (01) | bool | `True` | |
| `SCORING_CRON_HOURS` (01) | str | `"0,6,12,18"` | comma ints 0–23, unique, ≥ 1 |
| `SCORING_CRON_MINUTE` (01) | int | `15` | 0–59 |
| `SCORING_OFFICIAL_HOUR` | int | `0` | **pinned to 0**: the validator rejects any other value (02 §B6, §F2); must be in `SCORING_CRON_HOURS` |
| `SPEC_VERSION` (01) | str | `"ts-1.0"` | must be a key of `SPECS` (checked by the scoring module; case 21) |
| `SCORING_SPEC_VERSION` | read-only `@property` | `= SPEC_VERSION` | alias for specs 14/15, which name it; an env var of that name is ignored with a warning if it differs |
| `SCORING_SHADOW_SPECS` | str | `""` | comma list; entries not in `SPECS` or equal to `SPEC_VERSION` are dropped with a warning (fail-safe, not fail-boot) |
| `SCORING_MIN_CONFIDENCE` (01) | float | `0.4` | 0–1; default `min_confidence` for rank tables/Telegram (14); the stored `insufficient_data` uses the frozen spec value |
| `SCORING_KILL_SWITCH_PATH` (01) | path | `"./SCORING_KILLED"` | case 14; the sentinel `SCORING_KILLED` is in 02 §B12's closed list, but B12's amendment retires "any `*_KILL_SWITCH_PATH` key" — if 01 drops this key, the sentinel lives at 01's fixed path (§11 Q21) |
| `SCORING_EVENTS_ENABLED` | bool | `True` | `false` → scores/state still written, no `events` rows |
| `SCORING_CATCHUP_ENABLED` | bool | `True` | |
| `SCORING_CATCHUP_MAX_SLOTS` | int | `8` | 0–14 official slots (14 × ≤ 60 s + the current slot stays inside the 1 200 s job timeout); a run also stops starting catch-up slots once 900 s have elapsed — the rest are planned by the next run |
| `SCORING_LATE_GRACE_MIN` | int | `45` | 0–360; strict vs reconstructed (§2.2) |
| `SCORING_STALE_AFTER_MIN` | int | `420` | ≥ 360 |
| `SCORE_JUMP_ALERT_POINTS` | float | `15.0` | 1–100 |
| `RANK_STABILITY_ENABLED` | bool | `True` | |
| `RANK_AUTOCORR_ALERT_BELOW` | float | `0.9` | 0–1 (master §7.7 target) |
| `METRIC_NORM_PROVISIONAL_RETENTION_DAYS` | int | `14` | 1–365; age limit of provisional `stage='norm'` rows (this spec prunes them; 03 §2.7 prunes provisional raw rows) |
| `FEATURE_FN_TIMEOUT_SEC` | float | `20.0` | 1–120, per fn call per occupant |
| `FEATURE_COMPUTE_BUDGET_SEC` | int | `600` | 60–1100; remaining fn calls in a run are skipped as `failed` (`fn_budget`) once exceeded (keeps `scoring_6h` under its 1 200 s timeout) |

**Kill switch / pause semantics.** `SCORING_KILL_SWITCH_PATH` present → case 14 (computed, not written; `/control/rescore` returns 423 per spec 14). `KILL_SWITCH` (paid collectors) does not affect this spec — it makes no external calls. Operator pause of `scoring_6h` or `features_hourly` (API, Telegram or CLI; the target is the exact job id or a 16 `PAUSE_GROUPS` name, never a prefix) is stored in `control_state` (02 §B12) and persists across restarts; resuming triggers catch-up bounded by `SCORING_CATCHUP_MAX_SLOTS`. `PAUSE_ALL` pauses both jobs (neither is `ops_exempt`); an emergency `data/control/scoring_6h.paused` file is read, never written.

---

## 6. Cadence, rate limits, cost, performance budget

| job / step | cadence | external calls | notes |
|---|---|---|---|
| `features_hourly` | every `FEATURES_INTERVAL_MIN` (60) | **none** | local SQL only |
| `scoring_6h` | cron `SCORING_CRON_HOURS` at `:SCORING_CRON_MINUTE` UTC; official 00:15Z | **none** | + catch-up slots after downtime |
| rank-stability, pruning | inside the official run | none | |

No provider token bucket is touched (the process-wide TMC bucket, ≤ 8 req/min, is not used here; producer fns called by `compute_registered` are DB-only by contract — a fn that needs the network belongs in its producer's collector job); no `bittensor`/`bittensor-wallet` import; monthly cost **$0**.

**Storage growth** (130 occupants): `stage='norm'` rows 45 × 130 = 5 850 per slot; after pruning provisional rows, ≈ 2.1 M official rows/year ≈ 350 MB/year incl. indexes, plus ≤ 246 k provisional rows at any time (3 provisional slots/day × 14 d × 5 850). Raw rows written here (fn results + 2 derived keys, ≈ 16 keys × 130 × up to 24 hourly provisional keys/day) fall under 03's provisional-metric retention (03 §2.7). `scores`: 520 rows/day; official `explain` ≈ 4 KB, provisional ≈ 1 KB → ≈ 330 MB/year. `red_flag_state` < 2 k rows; `score_runs`, `regime_daily`, `rank_stability_daily` negligible. Spec 16's backup sizing must include this.

**Performance budget** (per slot per spec, laptop, 130 occupants; measured into `score_runs.timings_json`, a warning is logged above 2× budget):

| step | budget (p95) | load |
|---|---|---|
| `compute_registered` + `derive` | 30 s | ≈ 13 fns × 130 calls of local SQL |
| `snapshot` | 5 s | ≤ 62 indexed selects, ≤ 25 k rows read |
| regime | 1 s | ≤ 3 100 `chain_hourly` rows |
| score calculation + flags + explain | 2 s CPU | pure Python |
| write transaction | 3 s | ≈ 6 k `stage='norm'` inserts + 130 `scores` inserts + flag-state upserts |
| stability + pruning (official) | 3 s | |
| **total** | **≤ 60 s** (shadow spec + ≤ 15 s each) | peak RSS increase ≤ 150 MB |

---

## 7. Failure modes and fail-safe behaviour

1. **Never raise into the scheduler.** `ScoringJob`/`FeaturesJob` are `CollectorJob`s; the wrapper (spec 01 §2.6) records failures and alerts on the 5th consecutive one. Inside a run, per-occupant and per-fn errors are contained (§4 cases 8, 19).
2. **Never publish partial or look-ahead data.** One transaction per (slot, spec); strict mode admits only `observed_at ≤ cutoff`; reconstructed rows are marked `backfilled = 1`; slot pre-check `SELECT 1 FROM scores WHERE spec_version=? AND as_of=? AND run_kind=? LIMIT 1` skips already-scored slots.
3. **Fail-safe neutral.** Missing inputs become `None` (lower confidence), never fabricated zeros; a missing regime becomes `UNKNOWN` (baseline `v_w`); a missing manual table disables only human flags. Consumers treat low confidence, missing and stale as neutral (15).
4. **Immutability.** `scores` has 03's UPDATE/DELETE triggers; history is changed only by a new `spec_version`. `stage='norm'` rows are inserted once per (slot, spec) (`ON CONFLICT DO NOTHING`, 02 §B4) and deleted only by provisional pruning (14 d); `stage='raw'` rows follow 02 §B4 first-success-wins, guarded by 03's triggers.
5. **Restart invariance.** EMA, tiers and catch-up are driven by table state (`scores`, `score_runs`, `red_flag_state`), never process memory; the same inputs replayed in a fresh process produce identical rows (tested).
6. **Kill switch.** `SCORING_KILLED` stops all writes of this spec within one run; `/health.scoring_killed` and the feed's `stale` (14/15) make it visible.
7. **Bounded work.** `FEATURE_COMPUTE_BUDGET_SEC` and per-call timeouts keep a run under the 1 200 s job timeout; catch-up is capped at `SCORING_CATCHUP_MAX_SLOTS` (default 8) official slots and stops starting new slots after 900 s.
8. **Loud on design corruption.** Registry/spec-hash mismatches fail the boot on purpose — silently scoring with edited weights would corrupt a frozen series.

---

## 8. Implementation plan

### 8.1 Files

| file | contents (signatures) |
|---|---|
| `app/features/types.py` | `MetricResult`, `RawRow`, `NormValue`, `Occupant`, `ProducerFn` (§2.3, §2.6) |
| `app/features/normalise.py` | §2.5 functions; `NORMALISERS: Mapping[str, Callable]`; `status_of(result) -> str` (fn-result → `value_json.status`, §2.3); `normalise_value(defn: MetricDef, value: float \| None, status: str, vj: dict) -> float \| None` (status wrapper + dispatch); imports `_average_ranks` from `app.quant.mathutil` (shown inline in §2.5 for completeness) |
| `app/features/registry.py` | `MetricDef`, `_TABLE`, `METRICS`, `PILLAR_OF`, `PILLAR_WEIGHTS`, `scored(pillar)`, `register_metric(...)`, `register_many(rows)`, `EVIDENCE_FNS`, `registry_hash()`, `_validate()`, `class RegistryError(Exception)` (plain `Exception`, never `ImportError` — §4 case 20); validates every key against 03's `app/storage/catalog.py: METRIC_KEYS` and never adds catalog keys (03 owns the catalog, 02 §A2) |
| `app/features/store.py` | re-exports `put_metric_values` / `upsert_metric_values` from `app.storage.pit` (02 §B4; spec 05's `INSERT OR REPLACE` body is not used); `SELECT_RAW_SQL`; `select_series_sql()`; `write_norm_rows(db, rows) -> int` (`stage='norm'` via `put_metric_values`, insert-once); `prune_norm_provisional(db, before_as_of, batch=10_000) -> int`; `metric_at` is 03's reader (02 §B5), not redefined here |
| `app/features/derived.py` | `derive_announcement_cadence(legs: dict[str, RawRow \| None]) -> MetricResult`; `derive_silence_days(legs: dict[str, RawRow \| None], venue: RawRow \| None, x_absent: bool) -> MetricResult` (`x_absent` = an X leg row is `absent` or no active `watch_channels(platform='x')` row, 02 §C3) |
| `app/features/__main__.py` | CLI `python -m app.features --metric k1,k2 --as-of YYYY-MM-DD [--netuid N] [--write]`: evaluates fns/derived metrics at the date's 00:15Z (or now for `today`) and prints raw value/status/reason per occupant; `--write` goes through `put_metric_values` (spec 09's verify step 11 calls this) |
| `app/features/engine.py` | `FeatureEngine` (§2.6); `class FeaturesJob(CollectorJob)`: `id="features_hourly"`, `phase=8`, `enabled_flag="FEATURES_ENABLED"`, `interval_flag="FEATURES_INTERVAL_MIN"`, `timeout_sec=1200` |
| `app/util/mathutil.py` | verbatim port of `utils/math.py:1-131` (spec 13 later appends `monotone_score`) |
| `app/quant/regime.py` | ported pure functions + `@dataclass(frozen=True) RegimeParams`; `@dataclass RegimeResult(label, raw_regime, metrics, v_w)`; `async def regime_for_slot(db, slot: int, cutoff: int, mode: str, params: RegimeParams, v_w_by_regime: Mapping[str,float]) -> RegimeResult` |
| `app/scoring/spec.py` | `ScoringSpec`, `REGIME_TS_1_0`, `TS_1_0`, `SPECS`, `get_spec()`, `spec_hash()`, `GOLDEN_SPEC_HASH`, `GOLDEN_REGISTRY_HASH`, `class SpecHashMismatch(Exception)` |
| `app/scoring/tiers.py` | `ORDER`, `LOWER`, `assign_tier(s, prev, h=3.0)`, `ema(composite, prev_smoothed, span=7) -> float \| None` |
| `app/scoring/red_flags.py` | `FlagDef`, `FLAGS_TS_1_0`, `@dataclass FlagInputs(raw: dict[str, RawRow \| None], series: dict[str, dict[int, RawRow]], events: list[dict], manual: dict[str, dict], occupant: Occupant, slot: int, cutoff: int)`, `evaluate(fd, inputs) -> tuple[bool \| None, dict]`, `step(state: FlagState \| None, fd, trig, detail, manual_row, slot, cutoff) -> tuple[FlagState, str \| None]`, `multiplier(active: Iterable[FlagDef], floor=0.25) -> float` |
| `app/scoring/explain.py` | `impacts(pillars, snap_o, spec) -> list[Impact]`; `build_explain(...) -> dict` (§2.10) |
| `app/scoring/events.py` | `emit(db, *, occupant_key, netuid, event_type, severity, event_at, observed_at, payload, dedupe_key, run_id, source="scoring") -> bool` (wraps 03's `emit_event`, 02 §D1) and builders `tier_change_event(...)`, `flag_event(...)`, `score_jump_event(...)`, `insufficient_event(...)`, `instability_event(...)` |
| `app/scoring/stability.py` | `async def rank_autocorr_health(db, spec_version, slot) -> list[dict]` (§2.12; one dict per (series `health`/`composite_smoothed`, lag 1/7)) |
| `app/scoring/engine.py` | `pillar_score()`; `@dataclass Slot(as_of, run_kind, mode, cutoff, trigger, backfilled)`; `slot_for(now, hours) -> int` (latest `hh:00 ≤ now`); `plan_slots(last_done, now, cfg) -> list[Slot]`; `ScoreCalculator(spec)` (pure: `pillars`, `health`, `confidence`, `velocity`, `composite_raw`, `score(...) -> ScoreRow`); `ScoringService(rt)` (`run_scheduled`, `run_slot`, `rescore`, `backfill`, `scores_status`); `class ScoringJob(CollectorJob)`: `id="scoring_6h"`, `phase=8`, `enabled_flag="SCORING_ENABLED"`, cron from settings, `min_gap_factor=0`, `timeout_sec=1200` |
| `app/scoring/__main__.py` | CLI: `registry [--check] [--hash]`, `run [--slot ISO] [--dry-run] [--spec V]`, `explain <netuid> [--slot ISO]`, `rescore --slot YYYY-MM-DD --spec V [--shadow]`, `backfill --spec V --from D [--to D]`, `stability [--days N]` |
**No DDL and no migration in this spec** (02 §B7). Every table and column in §3.1 exists in 03's schema v1 (`metric_values` with `stage` and the generated `status`, `scores` with `computed_at`/`input_cutoff`/`flag_multiplier`/`pillar_coverage`/`prev_tier`/`shadow`, `red_flag_state`, `regime_daily`, `rank_stability_daily`, `score_runs`, index `ix_scores_occ`). Earlier drafts' `metric_norm` table, `ALTER TABLE` statements, `metric_values_v2` rebuild and `is_provisional` column must not be applied.

### 8.2 What is ported (verbatim unless noted)

| old file:lines | new location | change |
|---|---|---|
| `app/strategy/quality.py:91-92` `_clamp01` | `normalise._clip01` | rename |
| `quality.py:113-122` `_recency_score` | `normalise.recency` | params renamed `full_at`/`zero_at` |
| `quality.py:207-233` `quality_score` | `engine.pillar_score` | generalised to `(w, m)` items; `min_coverage` 0.25 → 0.3 (master); `math.fsum`; `1e-9` tolerance |
| `quality.py:33`, `108-110` `_ANON_TOKENS`, `_looks_anonymous` | `features/text.py` | ported by spec 11 (Phase 7, §8.1 there); not used by this spec because `team_doxxed` arrives pre-normalised |
| `quality.py:125-204` `derive_components`, `236-249` `static_quality_score`, `252-255` `is_low_quality`, `258-288` `_CATEGORY_KEYWORDS`/`classify_category` | not ported | replaced by the registry, PIT `metric_values`, and tiers respectively; `classify_category` is not a score input |
| `app/utils/math.py:1-131` | `app/util/mathutil.py` | none (`_average_ranks` 28-42, `spearman_r` 45-54, `quantile_buckets` 73-88 used here; `pearson_r` 9-25 is `spearman_r`'s kernel; the unused `datetime` import is harmless) |
| `app/strategy/regime.py:22-27` constants, `48-106` `_bucket_snapshots`, `109-119` `_log_returns`, `122-128` `_std`, `131-156` `classify_regime`, `159-221` `compute_regime_metrics` | `app/quant/regime.py` | imports: drop `logger`, `send_alert`, `utc_now`, `utc_iso`, `timedelta` (used only by `RegimeFilter`); import `parse_ts` from `app.util.time` (01, 02 §B1) and change the one `_bucket_snapshots` line `ts = parse_iso(ts_raw).timestamp()` to `ts = float(parse_ts(ts_raw))` (B1's `parse_iso` returns `int`, which has no `.timestamp()`; the old `parse_iso` returned a `datetime`); `settings` argument receives `RegimeParams` (same attribute names); `RegimeFilter` (224-393) with its debounce (≈ 253-310) not ported (02 §B8: no debounce) |
| `app/config.py:454-464` `REGIME_*` defaults | `spec.REGIME_TS_1_0` | frozen constants, not config |

### 8.3 Step order

1. `app/util/mathutil.py` + `tests/quant/test_mathutil.py` (copy of the old expectations for `spearman_r`, `quantile_buckets`, `zscore` from `E:\SN_Trader\SubnetTrader\tests\test_factor_backtest.py` "Phase 1 — stats primitives"; regime fixtures may reuse `tests/test_regime_filter.py`).
2. `normalise.py` + `test_normalise.py` (anchors, monotonicity, None/absent).
3. `registry.py` + `test_registry.py`; compute and pin `GOLDEN_REGISTRY_HASH`.
4. `store.py` over 03's schema v1 (no DDL here); `test_store.py` (first-success-wins through `put_metric_values`, a `failed` row replaced by a later observation, `ok`/`absent`/`not_applicable` never replaced, reconstructed rows never overwrite, `stage='norm'` insert-once and provisional prune, official norm rows undeletable).
5. `app/quant/regime.py` + `test_regime.py`.
6. `spec.py`, `tiers.py`, `red_flags.py` (pure) + tests; pin `GOLDEN_SPEC_HASH`.
7. `features/engine.py` + `derived.py` (`select_raw`, `snapshot`, `derive`, `compute_registered`) + tests on an in-memory DB.
8. `ScoreCalculator`, `explain.py` + **golden worked-example test** (§2.14).
9. `ScoringService` (planning, transaction, events, flags state, stability, pruning) + integration tests (catch-up, kill, immutability, shadow, restart invariance).
10. Wire `FeaturesJob`/`ScoringJob` into spec 01's already-declared `JOB_TABLE` ids; `/health.scores` block from `scores_status()` (`{latest_as_of, latest_official_as_of, stale, spec_version, shadow_specs, last_run_ok}`); CLI.
11. After spec 17's imports: `python -m app.scoring backfill --spec ts-1.0 --from 2025-09-22` to give spec 13 reconstructed history.

---

## 9. Tests (`tests/features/`, `tests/scoring/`, `tests/quant/`; no network)

| file | assertions |
|---|---|
| `test_registry.py` | P1…P6 weight sums `≤ 1e-9` from 1.0; `PILLAR_WEIGHTS == {P1:30,P2:20,P3:10,P4:18,P5:12}` and no P6; scored key sets per pillar equal the master §7.3 lists (hard-coded in the test, incl. the updated P4 keys `team_doxxed, owner_identity, owner_commitment, owner_retention_30d, control_stability_90d, owner_overhang, tenure`); 62 rows = 45 scored + 9 legs + 8 aux; `prenorm` exactly on rows 27–32 (02 §C1); `tenure` uses `tenure(120, 365, .3)` with fallback `occupant_age_days`; `participation` is `zclip(.2,.9)`; freshness per 02 §C6 (chain 1, GitHub/web/community fns 2, derived 1, X 3, P6 1, `team_doxxed` 90, other P4 and trigger aux 2); aux names `owner_extraction_trigger`, `owner_control_change_trigger` and the six C3 legs present; no retired name (02 §C2/C3) in the registry; every key in 03's `METRIC_KEYS` with the same pillar/weight; every fn path named `compute_<key>`; a monkeypatched weight/normaliser/pillar makes `_validate()` raise `RegistryError` (and `RegistryError` is not an `ImportError`); `register_metric("event_impact_14d", pillar="P6", fn=f, freshness_days=1)` attaches; spec 09's `register_metric("x_cadence_30d", pillar="P5", component_of="announcement_cadence_30d", normaliser=None, weight=None, freshness_days=3, fn=f)` attaches; `register_metric("video_uploads_90d", fn=f)` lands in `EVIDENCE_FNS` (catalogued evidence key) and not in `METRICS`; wrong pillar/freshness or an uncatalogued key raises; `registry_hash() == GOLDEN_REGISTRY_HASH["ts-1.0"]` |
| `test_normalise.py` | anchors: `recency(97,14,180)=0.5`; `logcap(10,10)=1`; `zclip_log2(1)=2/3, (0.25)=0, (2)=1, (0)=0`; `age_ramp(30)=.2, (197.5)=.6, (365)=1`; `tenure(100)=.3, (300)=.734694, (400)=1`; `event_impact(0)=.5, (2.30633)=.909400`; `presence_ratio(.01, 2000)=0, (.01, 500)=.2, (.2, 500)=1, (1.0, 500)=.5, (.3513, 390)=.998143, (.2, None)=None` (08's reference values); `prenorm(.66)=.66, (1.2)=None, (None)=None`; reference functions (shared anchors with spec 11's tests): `owner_commitment(.85,False,.225,1e5,600)=.66, (0,False,0,0,300)=0, (.5,None,None,10,20)=.5`; `owner_retention(.1,.001,False,True)=.9, (1.5,None,False,True)=0, (.1,.03,False,True)=0, (.5,None,False,False)=1, (.5,None,True,False)=0, (.1,.03,False,False)=0, (1.5,None,False,False)=0` (02 §C1: all three hard triggers before the cut-disabled shortcut); `control_stability` swap at 45 d = .7, lock disabled = 0; `owner_overhang(.03)=1, (.18)=.35, (.10, legacy)=.55`; `bus_factor` 0/1/2/3 → 0/.3/.6/1. **Monotonicity** sweeps x ∈ [−5, 500] step 0.1: `recency`, `zclip_inv`, `owner_overhang` non-increasing; `logcap`, `zclip`, `zclip_log2`, `tenure`, `age_ramp`, `event_impact`, `prenorm` (on [0,1]) non-decreasing; every output ∈ [0,1]. Every function returns `None` for `None`; `normalise_value` maps `absent → 0.0` and `failed/not_applicable → None` for every registry normaliser |
| `test_rankg.py` | 37 zeros + 85 distinct at n = 122 → all zeros share `m = .29411`; all-tied → 0.5; n = 9 → all None, n = 10 → values; n = 1 000 extremes → exactly 0.0/1.0 (clip); `rankg(x) == rankg(log x)` for positive x; cohort excludes `absent` and `pre_launch` occupants |
| `test_pillar.py` | None dropped / 0.0 counted; coverage .10+.05+.15 kept (`math.fsum` gives exactly 0.3, a plain `sum` 0.30000000000000004 — both kept by the 1e-9 tolerance), .25 → None; port fidelity: a 7-component case from old `quality_score` with coverage ≥ 0.3 gives the same score |
| `test_golden_worked_example.py` | in-memory DB (03 schema v1 only — no migration from this spec) with the §2.14 `stage='raw'` rows for SN64 + TAIL written through `put_metric_values` with explicit `value_json.status` (P4 rows 27–32 as spec 11's final values with the §2.4 `value_json` keys, `tenure` as raw days, `owner_extraction_trigger`/`owner_control_change_trigger` = 0), 120 synthetic occupants positioned to produce the stated `r0` ranks (and 56 more `x_engagement_30d` values), a root occupant `0:0` that must not enter any cohort, a CHOPPY `chain_hourly` fixture, the prev official rows, the `dereg_imminent` state row, a TAIL `venue_reachable` absent row, TAIL `x_followers`/`x_cadence_30d`/`x_last_activity_days` absent rows (`no_x_account`) and no `watch_channels(platform='x')` row for TAIL, and one `identity_changed(subnet_name)` event; `run_slot(official, strict)` → every §2.14 number (±1e-4 metrics/pillars, ±1e-3 scores), 45 × 122 `stage='norm'` rows with `observed_at = computed_at`, TAIL tier F, events exactly `{red_flag_raised(stale_identity), tier_change(D→F)}` with `source='scoring'` and 03's required payload keys (`red_flag_raised` has `human=false, sticky=false, detail, flag_multiplier_total=.54`), `scores.red_flags` = `[]` / `["dereg_imminent","stale_identity"]` (names), `scores.computed_at = 1790122504` and `input_cutoff = computed_at`, explain top lists and tie-break order, Σ impacts = Health − 50 |
| `test_velocity.py` | P6 ∧ blend → formula; blend None → 100·P6; P6 None → 100·blend; both None → None and `composite_raw = Health`; `v_w` per regime incl. UNKNOWN |
| `test_flags.py` | state rows use exactly 03's `red_flag_state` columns (02 §B7); floor: flags ×.5 ×.6 ×.6 ×.7 → product .126 → **.25**; hysteresis timeline: True d0, False from d1 → `active=1` with `false_since=d1` (derived `clearing`, `clear_streak_days` 0…6) through d7, `active=0` at the first slot ≥ d8; a True on d4 resets `false_since`; None on d2–d3 neither resets nor advances; human-confirmed stays `active=1, human=1, false_since NULL` 30 d with trigger False; manual clear → immediate `red_flag_cleared(reason manual)` and `suppress_until = cleared_at + 30 d`; within 30 d the same `trigger_ids` do not re-raise, a **new** trigger id re-raises at once, after 30 d the old evidence re-raises (02 §D5); a manual clear with no state row is ignored; `owner_extraction` raised by `owner_extraction_trigger = 1` or a manual `confirmed` row; `owner_control_change`: trigger 1 → raised, back to 0 → cleared 7 d later; with `identity_churn = true` → `sticky=1`, stays active with trigger 0 for 60 d, cleared only by a manual `cleared` row; manual clear of a non-sticky `owner_control_change` works; `miner_burn_zeroed` needs 3 exact-as_of rows > 0.8 (0.8 exactly → False); `inflated_community` needs 7; `stale_identity` three-term logic; `site_down_7d` from latest event and `detail.trigger_event_id` = that event's id |
| `test_tiers.py` | `(62,B)→B, (77.9,B)→B, (78,B)→A, (72.1,A)→A, (72,A)→B, (40,A)→D, (33,F)→D, (32.9,F)→F, (27,D)→F, (27.01,D)→D, (57,B)→C, (57.01,B)→B, (42,C)→D, (44,C)→C, (90,D)→A, (50,None)→C, (None,A)→None` |
| `test_ema.py` | **restart invariance**: slots 1–10 in one process vs 1–5, new process, 6–10 → identical `scores` rows (ignoring `run_id`, `observed_at`); gap > 7 d → reseed; `composite = None` → smoothed None, next valid continues from the last non-null within 7 d; provisional rows never used as `prev` |
| `test_pit.py` | raw row with `observed_at > cutoff` invisible; a second successful `put_metric_values` for the same key is ignored (02 §B4 first success wins; also for `absent`/`not_applicable`) and a `failed` row is replaced with the later `observed_at`, so a replay before the replacement selects the previous fresh `as_of`; latest fresh row `failed` (or with no `value_json.status` → generated `none`) + older fresh `ok` → the older is used (02 §B5); freshness boundary (age = window → fresh, +1 s → None); `backfilled=1` rows admitted only in reconstructed mode; `stage='norm'` rows never selected as raw input; fn/derived rows keyed `floor_hour(t_eval)` (provisional iff `as_of % 86400 ≠ 0`), stamped `observed_at = t_eval` (strict) or `now` + `backfilled=1` (reconstructed, never overwriting a live row) |
| `test_immutability.py` | `UPDATE`/`DELETE` on `scores` raise `sqlite3.IntegrityError` (03's message "scores is append-only"); re-running a scored slot inserts nothing and returns `note='already_scored'`; a second `stage='norm'` insert for the same (occupant, metric, spec, slot) is ignored (`ON CONFLICT DO NOTHING`, 02 §B4) and leaves the first row unchanged; deleting an official norm row aborts |
| `test_catchup.py` | last done D−3 00:00Z, now D 00:20Z → plan `[D−2, D−1]` reconstructed (`backfilled=1`) then D strict; cap `SCORING_CATCHUP_MAX_SLOTS`; killed slots never replanned; missed provisional slots skipped; events carry `payload.backfilled=true` |
| `test_kill.py` | `SCORING_KILLED` present → only `score_runs(killed=1)`; zero new `stage='norm'`, `scores`, `events`, `red_flag_state`, `regime_daily`, `rank_stability_daily` rows |
| `test_shadow.py` | a monkeypatched spec `ts-test` in `SCORING_SHADOW_SPECS` → rows with `shadow=1` and their own `stage='norm'` (`spec_version='ts-test'`)/state rows, no events; primary rows byte-identical to a run without the shadow |
| `test_prelaunch.py` | pre-launch occupant: P1 not computed, P2–P6 stored, `quality/velocity/composite*/tier` NULL, confidence over P2–P5, excluded from `rankg` cohorts, rank-stability and events except flags; netuid 0 (root) is never in the universe |
| `test_regime.py` | ported functions reproduce the old module's outputs on a fixture (expected values generated once from `~/taosentinel/seed/app/strategy/regime.py` when present, pinned in the test); thin universe → `UNKNOWN`/.15; TRENDING/DISPERSED → .25; DEAD → .10; INTEGER `ts` inputs bucket identically to the old ISO-string inputs (the `parse_ts` port change) |
| `test_stability.py` | identical rankings → autocorr 1.0, turnovers 0; reversed → −1.0; n < 10 → NULL stats; input order shuffled → identical `quintile_turnover` (sorted by netuid first); four rows per official run, one per (series `health`/`composite_smoothed`, `lag_days` 1/7), with 03's columns; `health` lag-1 autocorr .85 → one `rank_instability` event (severity `info`, payload `{spec_version, as_of, series: "health", lag_days: 1, n, rank_autocorr, quintile_turnover, top20_turnover}`, dedupe `system\|rank_instability\|{spec}\|{as_of}`); `composite_smoothed` below .9 emits nothing |
| `test_explain.py` | Σ impacts = Health − 50 over 200 random fixtures; top lists ≤ 3, sign-filtered, tie-broken by key; provisional explain omits `pillars.*.metrics` |
| `test_events.py` | every row goes through 03's `emit_event` (unknown type or missing required key → `ValueError`, so each builder is checked against 03 §3.9's required keys) and has `source='scoring'`; a re-run emits no duplicates (03's exact dedupe patterns, e.g. `{occ}\|red_flag_raised\|{flag}\|{raised_at}`); `red_flag_raised` carries `human`, `sticky`, `detail`, `flag_multiplier_total` and has severity critical iff multiplier ≤ .6; catch-up events carry `payload.backfilled=true`; `scoring_stalled` fires once through `AlertLatch` and clears on the next run; `score_jump` at exactly 15.0 fires, 14.99 does not; `insufficient_data` only on transition; no events for shadow specs or with `SCORING_EVENTS_ENABLED=false` |
| `test_spec.py` | `spec_hash(TS_1_0) == GOLDEN_SPEC_HASH`; unknown `SPEC_VERSION` → run refused `spec_unknown`; `settings.SCORING_SPEC_VERSION == settings.SPEC_VERSION`; `SCORING_OFFICIAL_HOUR=6` rejected by the validator (02 §B6); every `FlagDef.multiplier` equals 03's `catalog.RED_FLAGS` |
| `test_features_job.py` | writes only `stage='raw'` rows (never `stage='norm'`) keyed `floor_hour(now)`, each with `value_json.status` and `source` per 02 §B9 (`scoring` for the two derived keys, `llm` for P6); a re-run in the same hour adds nothing; a raising fn → `failed` rows, job ok; a non-`MetricResult` return → `failed`/`bad_return`; a missing producer module → no rows, no exception; budget exhaustion → `fn_budget` rows; an `EVIDENCE_FNS` fn writes its evidence key; any registered `compute_announcement_cadence_30d`/`compute_silence_days` is never called |

---

## 10. Verify checklist (Phase 8 done when all pass on the laptop)

```bash
cd ~/taosentinel && . .venv/bin/activate
python -m app.scoring registry --check
#  → "registry ts-1.0: 62 metrics (45 scored: P1 10, P2 10, P3 6, P4 7, P5 8, P6 4; 9 legs; 8 aux); pillar sums OK; hash … == golden"  (exit 0)
pytest tests/features tests/scoring tests/quant/test_mathutil.py -q          # all pass, incl. test_golden_worked_example
sqlite3 data/taosentinel.db "SELECT COUNT(*) FROM sqlite_master WHERE name='metric_norm'"   # 0 — no such table (02 §B3)
sqlite3 data/taosentinel.db "PRAGMA table_info(scores)" | grep -E "computed_at|input_cutoff|flag_multiplier|shadow"   # 03's columns, no observed_at
sqlite3 data/taosentinel.db "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='scores'"  # trg_scores_no_update, trg_scores_no_delete
python -m app run-job features_hourly                                         # {"ok": true, "rows": >0}
sqlite3 data/taosentinel.db "SELECT metric_key, status, COUNT(*) FROM metric_values WHERE stage='raw' AND source='scoring' AND as_of=(strftime('%s','now')/3600)*3600 GROUP BY 1,2"
#  → announcement_cadence_30d / silence_days, ≈ universe size each (netuid 0 excluded)
python -m app.features --metric venue_reachable,x_cadence_30d --as-of today --netuid 64   # raw value/status per fn, nothing written
python -m app.scoring run --dry-run          # table: netuid tier composite_smoothed health velocity confidence flags; writes nothing
python -m app run-job scoring_6h             # ok; then:
sqlite3 data/taosentinel.db "SELECT run_kind, COUNT(*), SUM(insufficient_data), SUM(status='pre_launch') FROM scores WHERE spec_version='ts-1.0' AND as_of=(SELECT MAX(as_of) FROM scores WHERE spec_version='ts-1.0') GROUP BY 1"
sqlite3 data/taosentinel.db "SELECT COUNT(*) FROM metric_values WHERE stage='norm' AND spec_version='ts-1.0' AND as_of=(SELECT MAX(as_of) FROM scores WHERE spec_version='ts-1.0')"   # = 45 × universe size
sqlite3 data/taosentinel.db "SELECT red_flags FROM scores WHERE red_flags <> '[]' LIMIT 3"   # sorted name arrays, e.g. ["dereg_imminent","stale_identity"]
sqlite3 data/taosentinel.db "SELECT COUNT(*) FROM scores WHERE netuid=0"      # 0 (root never scored)
python -m app run-job scoring_6h             # ok, rows=0, note already_scored
sqlite3 data/taosentinel.db "UPDATE scores SET tier='A' WHERE id=1"          # → Error: scores is append-only
python -m app.scoring explain 64             # pillars, top ± metrics, flags, regime, confidence for the current SN64 occupant
python -m app.scoring explain 64 --json | jq '.pillars.P1.metrics.emission_share.raw'   # equals TMC table `emission` for SN64 at the slot (±0.001)
python -m app.scoring explain 64 --json | jq '.pillars.P4.metrics'           # P4 m values equal spec 11's written values (prenorm)
curl -s localhost:8090/health | jq .scores   # {"latest_as_of": …, "stale": false, "spec_version": "ts-1.0", …}
# kill drill on a past, not-yet-scored provisional slot (it stays unscored: killed slots are never backfilled)
touch SCORING_KILLED && python -m app.scoring run --slot <yesterday>T18:00:00Z; rm SCORING_KILLED   # → killed=true, no new scores/stage='norm' rows
sqlite3 data/taosentinel.db "SELECT day_ts, label, v_w, thin_universe, computed_at FROM regime_daily ORDER BY day_ts DESC LIMIT 3"   # label per 02 §B8
# after two official runs:
sqlite3 data/taosentinel.db "SELECT series, lag_days, n, rank_autocorr, quintile_turnover, top20_turnover FROM rank_stability_daily ORDER BY as_of DESC LIMIT 4"
# power-loss drill: sudo systemctl stop taosentinel across a 00:15Z, start it again →
sqlite3 data/taosentinel.db "SELECT as_of, trigger, mode, backfilled FROM score_runs ORDER BY started_at DESC LIMIT 5"   # a 'catchup' row, reconstructed, backfilled=1
```

---

## 11. Open questions / operator decisions (reconciliation list)

1. **Storage (resolved by 02 §B3/B7).** One `metric_values` table with `stage ∈ {raw, norm}`, the generated `status` column (from `value_json.status`) and no `is_provisional` column; no `metric_norm` table; this spec ships no DDL. Spec 13 reads normalised factors from `stage='norm'` rows (`value` = m, `value_json.raw` = raw, 02 §J2).
2. **Time encoding / `as_of` (resolved by 02 §B1/B6).** All INTEGER unix seconds. `scores.as_of` is the **slot instant** (00:00Z official); the run instant is `scores.computed_at` (≈ D 00:15Z) and `input_cutoff` completes the PIT key. Producer fns receive `as_of` as a tz-aware UTC `datetime` (02 §C5); 14 converts to ISO only at the boundary.
3. **`scores.red_flags`** = sorted JSON array of flag names (02 §B6); multiplier/since/state/origin/detail live only in `explain.flags`.
4. **`scores` has no `observed_at`** (02 §B6); `computed_at` is the column 13 reads (02 §J2).
5. **`SCORING_SPEC_VERSION`** (named by 14/15) is a read-only alias of 01's `SPEC_VERSION`.
6. **Tied zeros under `rankg`**: master's "ties averaged" gives every zero-emission occupant `m ≈ 0.294` on `emission_share` (≈ 0.308 on `chain_buys`) in the 122-occupant fixture (§2.14). Implemented as specified; proposal for `ts-1.1`: map a real zero to 0.0 and rank only positive values.
7. **P4 is pre-normalised by spec 11** (02 §C1): `team_doxxed`, `owner_identity`, `owner_commitment`, `owner_retention_30d`, `control_stability_90d`, `owner_overhang` arrive as final [0,1] values and are scored through `prenorm`, with raw inputs in `value_json` under the §2.4 keys (= the reference functions' keyword names); `tenure` arrives as raw days; the §2.5 reference functions state the master formulas and share test anchors with spec 11. Research S2 saturates at `lock_ratio ≥ 0.75` while master/11 use `0.6·lock_ratio` linearly — `ts-1.1` candidate. Spec 11's edge rules (`grace_period` → None, hard triggers before the cut-disabled shortcut) are accepted as the production semantics.
8. **`presence_ratio`** uses spec 08's `norm_presence_ratio` (linear ramp below 0.05, `max(0.5, 1 − (r − 0.35)/0.7)` above 0.35, None without a member count) — master gives only the healthy band.
9. **Freshness (resolved by 02 §C6)**: P4 owner metrics, `owner_identity`, `tenure` and the owner trigger aux 2 d; `team_doxxed` 90 d; P6 1 d; derived combiners 1 d; X 3 d; the 8-d main-server Discord evidence window is internal to 08's fns.
10. **Regime (02 §B8)**: no debounce; thin universe → `UNKNOWN` (`v_w` .15) instead of the old `DEAD` (.10); live and backfilled rows use the same raw rule, but live rows read `chain_hourly.price` (active subnets) while 17's backfill reads `price_bars` closes — 13 should split `by_regime` results at the first live day. Master mentions TAO/USD as an input — stored as a diagnostic only because the ported classifier uses subnet prices; using it is a `ts-1.1` change. **Open for 03:** B8 names the column `label`; 03's DDL still says `market_regime`.
11. **X absence (resolved by 02 §C3/C5)**: 09's fns return `MetricResult(0.0, …, absence_reason='no_x_account' | 'x_account_gone')`, giving `x_followers = 0` and absent X legs; the `watch_channels(platform='x')` test in `derive()` is a PIT guard for occupants with no handle in any source. `x_cadence_30d = None` for an account that exists but never posted keeps `announcement_cadence_30d` from the other legs. Open for 09: whether `x_last_activity_days` becomes "days since resolution" (a lower bound, like 08's legs) after the first successful poll with zero posts (09 §11 Q7); 12 already honours `value_json.lower_bound`.
12. **Routing (resolved by 02 §D4)**: `score_jump` immediate; `insufficient_data`, `rank_instability` and any `payload.backfilled=true` event to the digest. **Resolved (2026-09-24):** 03 §3.9 catalogues the system event `scoring_stalled` (`sys`, critical, alert yes, payload `{slot_as_of, last_ok_as_of, missed_slots, last_error}`, dedupe `{slot_as_of}`); §2.11 emits exactly that row.
13. **Threshold edges**: `miner_burn_zeroed` uses master's `> 0.8` (spec 05 wrote `≥`); `owner_extraction` is raised from `owner_extraction_trigger` (02 §C2; its hard trigger `sell_ratio > 1`, while S1 already reaches 0 at 1.0). **Still open for 11:** the trigger rows must carry `value_json.trigger_ids` (sorted evidence ids) for 02 §D5's "new evidence re-raises" test; without them a manual clear suppresses re-raising for the full 30 d.
14. **Key names (resolved by 02 §C1–C3)**: legs `discord_last_activity_days`, `tg_last_activity_days`, `x_last_activity_days`, `discord_announcements_30d`, `tg_posts_30d`, `x_cadence_30d`; aux `owner_extraction_trigger`, `owner_control_change_trigger`; `tenure` produced by 11 as raw days with 05's `occupant_age_days` as fallback. Retired names are listed in §2.4. `announcement_cadence_30d`/`silence_days` are derived here; 08/09 register no producer for them; 08's lower-bound legs are honoured (§2.6).
15. **Spec 13** (02 §J2): master P4 names only; score reads use `scores.computed_at` (e.g. the official score of day D has `computed_at ≈ D 00:15Z`); normalised factors come from `stage='norm'` rows.
16. **Manual flags (resolved by 02 §D5)**: `confirm` for `owner_extraction` and `key_compromise`; `clear` for those two and `owner_control_change` while active (else 14 returns 409 `flag_not_active`). A human-confirmed `owner_extraction_evidence` is entered as flag `owner_extraction`. Spec 11 whitelisting a disclosed migration remains the non-manual way to zero `owner_control_change_trigger`.
17. **Interpretations recorded** (implemented as stated, flag if wrong): floor 0.25 applies to the product of multipliers; `silence_45d` is not raised for occupants with no reachable channel; "dead invite" in `stale_identity` = `venue_reachable == 0`; `insufficient_data` rows keep composite/EMA/tier; a manual clear suppresses auto re-raise for 30 d unless new `trigger_ids` appear (02 §D5; post-mortem override to `key_compromise`); `owner_control_change` clears 7 d after its 30-d trigger returns to 0 and is sticky (manual clear only) when paired with identity churn (02 §D5 — master's "auto-clears after 30 d" becomes 30 + 7 d); a failed latest raw row falls back to an older row inside the freshness window (02 §B5); `tenure = max(0.3, zclip(days, 120, 365))` read literally from master's "zclip …, floor .3" (flat 0.3 until ≈ 194 d, then linear; spec 11 agrees) rather than a 0.3→1 ramp starting at 120 d.
18. **Writers (resolved by 02 §B3/B4)**: every producer writes through `put_metric_values` (first success wins; spec 05's `INSERT OR REPLACE` would trip 03's delete trigger). P3 remains collector-produced by 07's `web_metrics` job; an 07 fn registered under 02 §C5 is also accepted (first success wins). 07's product enum moves to `value_json.product_status`; `value_json.status` is the status enum.
19. **Operator decisions**: `SCORE_JUMP_ALERT_POINTS=15`, `SCORING_CATCHUP_MAX_SLOTS=8` and `SCORING_LATE_GRACE_MIN=45` are starting values — revisit after 30 days of telemetry. Whether to run the `ts-1.0` backfill from 2025-09-22 (reconstructed P1+P2 history, ≈ 365 official slots, ≈ 20 min — UNVERIFIED estimate, re-measure on the laptop) before spec 13's first study — recommended yes.
20. **Keys outside the registry** (§2.4; 02 §C2/C3/C7): 03's catalog admits the evidence keys (08's `discord_topology`, `inflated_days`, `tg_members`; 11's `owner_lock_ratio`, `owner_dump_impact`, `owner_holding_share`, `owner_sell_impact_7d`, `team_verified_count`, `team_candidate_count`; 05's `vtrust_dispersion`; 09's video/blog keys) and the 62 registry keys, with `source` values exactly 02 §B9's set. **Open:** 11 and 03's draft also carry `owner_sell_ratio_30d`, which 02 §C2 neither lists as evidence nor retires; this registry treats it as an uncatalogued key until 02/03 decide (S1's `sell_ratio` is already in `owner_retention_30d.value_json`).
21. **`SCORING_KILL_SWITCH_PATH` (open, 01/02):** 02 §B12 keeps the `SCORING_KILLED` sentinel but its amendment retires "any `*_KILL_SWITCH_PATH` key", which literally includes 01's `SCORING_KILL_SWITCH_PATH`. This spec only needs "sentinel present → compute, do not publish"; 01 decides whether the path stays configurable.

---

## 12. Sources

- `E:\TaoData\specs\02-cross-cutting-conventions.md` (binding ledger: A2, B1–B9, B12, C1–C7, D1–D5, E2–E3, F2, H1–H2, J2) — read 2026-09-23
- `E:\TaoData\specs\00-MASTER-SPEC.md` v0.2 (reconciled 2026-09-23) §1.3, §2, §3, §5, §6, §7.1–§7.7, §9, §10, §11, §12
- `E:\TaoData\specs\INDEX.md`; `E:\TaoData\specs\18-operator-checklist.md` (A3 seed app tree for regime test fixtures; D1–D4 manual data feeding P3/P4 and human flags)
- `E:\TaoData\specs\01-repo-skeleton-runtime.md` §2.4 (dynamic settings), §2.6 (`CollectorJob`), §2.7 (job table: `features_hourly`, `scoring_6h`), §2.9 (`/health.scores`), §2.11 (`AlertLatch`), §5 (config inventory: features/scoring keys; weights not config)
- `E:\TaoData\specs\03-storage-schema.md` §2.3 (`Database.transaction`, nested calls join), §2.5 (triggers), §2.6 (`metric_at`, `cross_section`), §3.1 (`red_flag_state`), §3.2 (`occupants`, `watch_channels`, `manual_flags`), §3.6 (`metric_values` with `stage` and generated `status`, `scores`, `events`, `regime_daily`, `rank_stability_daily`, `score_runs`), §3.8 (metric catalog), §3.9 (events catalog, `RED_FLAGS`), §8 (`emit_event`), Appendix A — DDL re-read 2026-09-23
- `E:\TaoData\specs\04-subnet-registry-and-identity.md` §3.1 (`occupants`, `events.dedupe_key`), §3.3, event catalogue (`identity_changed`, `identity_junk`)
- `E:\TaoData\specs\05-chain-collector.md` §2.3–§2.8, §3.1 (`chain_hourly`, `chain_network_hourly`), §3.3 (P1 + aux keys, `upsert_metric_values`), §4, §8 (`normalise.py`/`store.py` Phase-2 subsets)
- `E:\TaoData\specs\06-github-collector.md` §2.8, §3.3, §4 (status contract, `repo_gone`/archive inputs)
- `E:\TaoData\specs\07-web-presence-collector.md` §2.8–§2.9, §3.3–§3.5, §4, §8.2
- `E:\TaoData\specs\08-community-collector.md` §2.9 (P5 fns `compute_<key>`, `norm_presence_ratio`, lower-bound legs), §3.4 (key registration), §4.2 (absence vs failure)
- `E:\TaoData\specs\09-x-collector-and-kol.md` §3.3, §3.5, §4, Appendix A.4, §8.3 step 10 (`register` call shape)
- `E:\TaoData\specs\10-llm-classification.md` §3.3 (P6 keys; `source='llm'` per 02 §B9), §3.4 (`register_metric`, `MetricResult`), §4
- `E:\TaoData\specs\11-team-and-integrity.md` §2.2.6, §2.5–§2.7, §2.9 (S3, trigger aux, `identity_churn`), §2.11 (`tenure`), §3.3 (P4 rows), §4 — read 2026-09-23 before its 02 alignment (still on retired key names and raw-input P4 rows; 02 §C1/C2 govern)
- `E:\TaoData\specs\13-validation-harness.md` §2.3, §2.5, §3.1 (`metric_values`, `scores`, `regime_daily` columns), §11
- `E:\TaoData\specs\14-api-telegram-and-ui.md` §2.4 (alert routing), §3.1 (`scores`, `manual_flags`), §3.5 (`/scores/latest`, `/subnets/{netuid}` evidence, `/control/rescore`)
- `E:\TaoData\specs\15-trading-bot-integration.md` §3.1 (feed payload, consumed unchanged from 14's `GET /api/v1/scores/latest`, 02 §H1), §5 (`SCORING_SPEC_VERSION`); the feed itself is served by 14 §3.5 (`/api/v1/scores/latest`) — 15 has no producer route of its own
- `E:\TaoData\research\01-web-quant-scoring-methods.md` (all: rank-Gaussianisation for ~130 names, log-cap counts, coverage/absence semantics, EMA span 7–14 + ±3 hysteresis, rank autocorrelation/turnover, regime-conditioned sentiment weight, PIT)
- `E:\TaoData\research\01-fundamentals-scorer.md` §5 (`quality_score` semantics, None ≠ 0, coverage gate)
- `E:\TaoData\research\01-prior-sentiment-design.md` "Decisions to keep vs reject" (fail-safe neutral, version provenance, epoch seconds, score history as the product)
- `E:\TaoData\research\02-gapfill-keyless-onchain-pillar-source.md` §2 (realised post-gate emission vs moving price; θ = 0.739 %, gate .59/.50/.32/.16/.10 at ranks 28/32/35/40/45; 37 zero-emission subnets; immunity 864 000 blocks)
- `E:\TaoData\research\02-gapfill-owner-extraction-mechanics.md` §5 (S1–S5 owner metrics)
- `E:\SN_Trader\SubnetTrader\app\strategy\quality.py` 1-288 (ported: 91-92, 113-122, 207-233; 33/108-110 ported by spec 11; not ported: 125-204, 236-249, 252-255, 258-288) — line numbers re-verified 2026-09-23
- `E:\SN_Trader\SubnetTrader\app\strategy\regime.py` 1-120 (+ 120-221 classifier core; `RegimeFilter` 224-393 with debounce ≈ 253-310 not ported) — re-verified 2026-09-23
- `E:\SN_Trader\SubnetTrader\app\utils\math.py` 1-131 (`_average_ranks` 28-42, `spearman_r` 45-54, `quantile_buckets` 73-88 re-verified 2026-09-23)
- `E:\SN_Trader\SubnetTrader\app\utils\time.py` (`utc_iso()` takes no argument; its `parse_iso` returns a `datetime`, which `_bucket_snapshots` calls `.timestamp()` on — hence the `parse_ts` port change, §8.2; re-verified 2026-09-23)
- `E:\SN_Trader\SubnetTrader\app\config.py` 453-469 (`REGIME_*` defaults; the ported values are 454-464, re-verified 2026-09-23)

---

## Changelog
- 2026-09-23 reconciliation:
  - B3/B7: dropped the `metric_norm` table and every `CREATE`/`ALTER TABLE`; normalised values are `metric_values` `stage='norm'` rows with the B3 `value_json` keys; §3.1 now lists the columns written.
  - B3/B4/B5: `status` is generated from `value_json.status` (`none` read as failed); no `is_provisional` column; single writer `put_metric_values` with first-success-wins; norm rows are insert-once; the PIT selection follows `metric_at`.
  - B6/J2/H1–H2: `scores.as_of` = slot; `computed_at` + `input_cutoff` (no `observed_at`); `red_flags` = sorted names; details only in `explain.flags`; `SCORING_OFFICIAL_HOUR` pinned to 0; 13/14 read contracts updated.
  - B7/D5: red-flag state machine rewritten on 03's columns (`active/human/sticky/…`). Manual clear gives 30-d suppression, but new `trigger_ids` re-raise. `owner_control_change` is sticky on `identity_churn`, clears 7 d after its 30-d trigger returns to 0, and can be cleared manually.
  - C1: P4 rows 27–32 use `prenorm` (incl. `owner_identity`), with `value_json` keys = the reference-function arguments; `tenure` is raw days (`no_registration_block`); `owner_retention` checks all hard triggers (incl. `sell_ratio > 1`) before the cut-disabled shortcut.
  - C2/C3/C7: trigger definitions (30-d S1 hard trigger / 90-d `perpetual_off`; 30-d control change + `identity_churn`), retired names, evidence lists, the X-absence and `silence_days` rules, and `participation` = counts ÷ max_uids only.
  - C5/C6: `compute_<key>` producer contract (`MetricResult`, tz-aware `T_eval`), P6 fn names, `register_metric` for catalogued evidence keys (`EVIDENCE_FNS`), freshness per C6.
  - B9: `SOURCE_OF` — 10 → `llm`, derived and norm rows → `scoring`, appendix feeds → `rss`.
  - D1–D4: event payloads and dedupe keys copied from 03 §3.9. `red_flag_raised` gains `human/sticky/detail`; `rank_instability` is `info` on series `health`, with rows per lag in 03's `rank_stability_daily`. Routing follows D4. `scoring_stalled` goes through `AlertLatch` (still to be catalogued).
  - B1/B8: regime port uses `parse_ts` (B1's `parse_iso` returns `int`); the regime column is `label`; no debounce.
  - §2.14 re-verified in Python 3.14.3: only TAIL P1 Σw·m changed (.151169 → .151170). The explain JSON now shows its 6-decimal values, and the golden test asserts names-only `red_flags` and 03 payload keys.
  - B12: pauses are stored in `control_state` with exact targets; §11 Q10/Q12/Q13/Q20/Q21 list the questions still open for 01/03/11.
- 2026-09-24 final pass: `scoring_stalled` (§2.11, §3.4, §11 Q12) now emits exactly 03 §3.9's row — critical, payload `{slot_as_of, last_ok_as_of, missed_slots, last_error}`, dedupe `{slot_as_of}`; Q12 closed.
- 2026-09-24 final pass: removed the stale "15 §3.3 feed query" citation — the feed is 14 §3.5 `GET /api/v1/scores/latest`, consumed unchanged by 15 (02 §H1); §1 scope line and §12 sources updated.
- 2026-09-24 final pass: registry rows 44/52 — `attention_7d` inputs are TMC trending views + labelled mention count (spec 10); `x_mentions_7d` is evidence only, not an `attention_7d` input. No netuid-27 re-occupancy fixture exists (checked).
