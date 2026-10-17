# 04 — Subnet registry and identity merge

**Status:** proposed (reconciled to 02 and adversarially reviewed 2026-09-23) · **Phase:** 1 · **Binding tie-breaker:** `02-cross-cutting-conventions.md` (ids cited inline) › master › owner specs; this spec owns its job ids and cadence (A2, E2) · **Depends on:** 01 (`Settings`, `CollectorJob`/`RunContext`/`JobOutcome`, `JOB_TABLE`, `Http.request` + `ProviderLimiter` with provider ids `tmc`, `subtensor_rpc`, `subnetalpha` (G1), `job_state`, `/health`, `app/util/time.py`), 03 (all DDL (B7), `emit_event`, events catalog §3.9), 05 (`TmcClient`, `write_rest_snapshot`/`latest_rest_snapshot`, the TMC budget table §6.3 — G2/G3), 16 (liveness E4, `PAUSE_GROUPS` B12), 17 (`OccupancyIndex` passed to `import_fundamentals`, exact `registered_ts` — J1) · **Master spec refs:** §1.3 (identity = occupant), §2 (reuse table: `fundamentals_collect.py`), §4 (tier-0 sources: TMC REST, public RPC, subnetalpha), §5 (identity rules), §6 (reference tables), §7.3 P4 inputs (`owner_identity`, `control_stability_90d` identity-churn pairing, `tenure`), §7.5 `stale_identity`, §8 row "Registry & identity", §11, §12 Phase 1.

---

## 1. Purpose and scope

The registry is the **first collector** and the **identity backbone** of TaoSentinel. Every other collector (chain, GitHub, web, community, X) reads its targets from here and keys its rows by the `occupant_key` it mints.

**In scope**
1. **Occupant discovery and lifecycle** from TMC `/public/v1/subnets/`: mint `occupant_key = f"{netuid}:{network_registered_at}"`, track `status ∈ {pre_launch, active, deregistered}`, detect slot re-registration and emit `netuid_reoccupied`.
2. **Three-source identity ingestion**, per field, point-in-time: on-chain `SubnetIdentitiesV3` (public JSON-RPC over HTTPS, no SDK), subnetalpha.ai WP-REST ACF, TMC coldkey identities for the owner coldkey. Plus manual/legacy candidates.
3. **Normalisation** of every URL/handle into a canonical `ref` (GitHub owner/repo, X handle, Discord invite / main-server channel id / handle, Telegram handle, YouTube channel, website, docs, email).
4. **Per-field merge** with precedence chain > subnetalpha > TMC coldkey > manual (`fallback` mode), producing `identity_resolved` and the `watch_channels` target list.
5. **Events**: `identity_changed`, `source_disagreement`, `netuid_reoccupied`, `identity_junk`, plus lifecycle events `occupant_activated`, `registry_anomaly`.
6. **Seed script** `scripts/seed_registry.py`: first population, legacy import of `seed/fundamentals.json` and `seed/subnet_edge/*.md` as `source='legacy'` fallback candidates with a re-occupancy guard. The fundamentals import is the library function `app.registry.legacy.import_fundamentals(...)` that 17 also calls (J1).
7. **"Needs review" read-model** feeding the weekly Telegram digest (rendered by spec 14).
8. **Raw TMC snapshots** (G2): every `registry_occupants` run stores its `/subnets/` payload (kind `subnets`) and every `registry_identity` run its `/accounts/coldkey-identities/` payload (kind `coldkey_identities`) in `chain_rest_snapshots` through 05's `write_rest_snapshot`; 05 and 11 read them and never refetch.

**Out of scope** (owned elsewhere): the TMC HTTP client (05's `TmcClient`, the only TMC caller, G2); HTTP routes and Telegram commands (14, A2 — this spec exposes domain functions, §3.3); resolving GitHub URLs to numeric repo ids and **every** `source_disagreement` on `github_repo` (06, compared by repo id — D2); resolving Discord invites to guild ids and validating channel ids (08); YouTube `@handle` → channel id (09 appendix); parsing `team_info` into `team_members` candidates (11); computing any `metric_values` row (11/12 — this spec produces **no metrics**, only their inputs, see §3.4); immunity/tenure arithmetic (05/11); the Telegram digest rendering (14).

---

## 2. Design

### 2.1 Components and data flow

```
                 ┌──────────────── registry_occupants (every 6 h) ────────────────┐
 TMC /subnets/ ──►│ OccupantTracker.sync()  ──► occupants, subnets                 │──► events: netuid_reoccupied,
 (TmcClient,     │   (network_registered_at diff, pre_launch→active)              │    occupant_activated, registry_anomaly
  1–3 req)       │ write_rest_snapshot('subnets') ──► chain_rest_snapshots (G2)   │
                 └────────────────────────────────────────────────────────────────┘
                 ┌──────────────── registry_identity (daily 02:40Z) ──────────────┐
 chain RPC ──────►│ ChainIdentityReader  ─┐                                        │
 (5 calls)       │                        │  ObservationWriter (hash-dedupe) ──► identity_observations, identity_raw_snapshots,
 subnetalpha ────►│ SubnetalphaReader ────┼─► normalise.classify()                 │    identity_coverage
 (2 req)         │                        │                                        │──► events: identity_changed, identity_junk
 TMC coldkey ────►│ TmcColdkeyReader ─────┘  (+ write_rest_snapshot('coldkey_identities'))
 (1 req)         │            ▼                                                    │
 manual_overrides│ Merger.resolve(occupant, field) ──► identity_resolved, subnets.*_url │──► events: source_disagreement
                 │            ▼                                                    │
                 │ WatchChannelsBuilder ──► watch_channels                         │
                 │ compute_needs_review() ──► 14 (digest + routes, A2)             │
                 └────────────────────────────────────────────────────────────────┘
```

**Job table (E2; 01's `JOB_TABLE` mirrors it verbatim and replaces its `registry_sync` placeholder and the `REGISTRY_INTERVAL_MIN` key).** Both are `CollectorJob`s (01 §2.6): `max_instances=1`, `coalesce=True`, `paid=False` (so `KILL_SWITCH` never pauses them), not `ops_exempt` (so `PAUSE_ALL` does), phase 1, events `source='registry'` (B9).

| id | factory (`app.…`) | trigger | UTC time / interval | misfire grace s | timeout s | enabled flag | provider (budget key) | `startup_catchup` | `min_gap_factor` | liveness max-age (E4, 16's formula) | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `registry_occupants` | `registry.jobs:RegistryOccupantsJob` | interval | `REGISTRY_OCCUPANTS_INTERVAL_MIN` = 360 (every 6 h, G2) | 3600 | 600 | `REGISTRY_ENABLED` | `tmc` | no (interval jobs are run at boot by 01's stagger) | 0.5 (01 default: a restart within 3 h costs no request) | 360·60·2 + 600 + 120 = 43,920 s | `JOB_TABLE` index 0, so it is the first job at boot (16 boot step 7); 1 × `/subnets/` (+ `/subnets/table/` only on failure) |
| `registry_identity` | `registry.jobs:RegistryIdentityJob` | cron | `REGISTRY_IDENTITY_AT_UTC` = 02:40Z daily (inside 17's TMC quiet window, after 05's 02:10Z batch) | 21600 | 1800 | `REGISTRY_ENABLED` | none at job level: HTTP providers `subtensor_rpc`, `subnetalpha`, `tmc`; a TMC `BudgetExhausted` ends only the coldkey step (§7), so a TMC cap never blocks the keyless sources | **yes** (E1) | 0 (cron) | 1440·60·2 + 1800 + 120 = 174,720 s | calls the occupant sync inline first when `subnets.updated_at` is older than 12 h (`ensure_occupants_fresh`) |

There are no other 04 jobs; the seed script (§2.11) and 17's `import_fundamentals` are scripts. Pause targets are the exact ids above or 16's `PAUSE_GROUPS` name `registry` = {`registry_occupants`, `registry_identity`}; 16's `tmc` group should also list both (each calls TMC) — requested from 16 (B12).

### 2.2 Occupant lifecycle (the identity rule, master §5)

Source (G2): `resp = await tmc.fetch_subnets()` — 05's `TmcClient` (`app/collectors/chain_tmc.py`, built per run as `TmcClient(run.http, lambda: settings)`), i.e. `GET {TMC_BASE_URL}/subnets/?limit={TMC_SUBNETS_PAGE_LIMIT}` (200) paged by `offset` inside 05's `get_paginated` until `count`/a short page. The server's maximum `limit` is **UNVERIFIED** (research saw the default page of 25; §6 gives the request count for each case). Never follow the `next` URL: TMC returns it with an `http://` scheme (verified in the 2026-09-22 dump). Row shape **verified** from the research dump of `/subnets/?netuid=51&limit=1` (2026-09-22): the row has 22 keys (`netuid`, `is_active`, `registered_at`, `created_at_block`, `latest_snapshot`, owner stake fields, …) and **no** `name`, `symbol` or `subnet_owner`; the 87-key `latest_snapshot` carries the chain fields, several as **strings**:

| field | where (verified) | use |
|---|---|---|
| `netuid` | row (int) | key |
| `network_registered_at` | `latest_snapshot` (string, e.g. `'3966206'`; coerce with `int()`, the idiom of old `chain_client.py:738-742`) | occupant key |
| `name` | `latest_snapshot.subnet_identities_v3.subnetName` (NULL when the identity is missing; the `/subnets/table/` `name` shows `Unknown` for those) | convenience column |
| `symbol` | `latest_snapshot.token_symbol` | convenience column |
| `is_active` | row (bool) | pre_launch vs active |
| `first_emission_block_number` | `latest_snapshot` (string → `int()`, NULL allowed) | activation block |
| `subnet_owner`, `subnet_owner_hotkey` | `latest_snapshot` (ss58 strings) | `subnets.owner_coldkey`/`owner_hotkey`; owner coldkey → TMC coldkey identity lookup |
| `registered_at` | row (ISO, minute precision, e.g. SN51 `2024-10-04T01:55:00+00:00` for block 3,966,206; semantics **UNVERIFIED**) | `registered_at_est` (below) |
| `subnet_identities_v3` | `latest_snapshot` (camelCase; all 8 keys present on 2026-09-22) | chain-identity **fallback** only (§2.3) |
| `block_number` | `latest_snapshot` (int) | `head_block` = max over rows |

**Fallback `/subnets/table/`** (`tmc.fetch_subnets_table()`, 1 request, 129 rows × 44 fields, verified to carry `subnet` (= netuid), `name`, `symbol`, `is_active`, `network_registered_at` (int), `subnet_owner`, `block_number`): used only when `/subnets/` fails or is insane. It has no `first_emission_block_number` (activation then uses `is_active` alone and `first_emission_block` stays NULL until a later `/subnets/` run back-fills it), no `subnet_owner_hotkey` and no identities (the §2.3 snapshot fallback is then unavailable).

**Snapshots (G2).** After the sanity gate passes, the run calls `write_rest_snapshot(db, 'subnets', resp, run_id=run.run_id)` (or kind `subnets_table` when the fallback was used); 05 merges pages into one row keyed `(endpoint, params, observed_at)`. An insane payload is **not** stored, so 05/11 never read it; its row count and a 2-row sample go to `collector_runs.meta_json`.

**Sanity gate before any transition:** a run is *sane* iff `100 ≤ len(rows) ≤ 300`, every row has an int `netuid` and an int-coercible `network_registered_at`, and `head_block` is an int ≥ the previous sane run's `head_block` (watermark `job_state('registry_occupants','head_block')`, B12). An insane run (after the table fallback was also tried) mutates nothing, feeds the `registry_anomaly(sanity_failed)` counter (§3.5) and then raises `RegistryRunError('sanity:…')`, so 01's wrapper records `ok=0` and counts the failure (`collector_failing` at `JOB_FAIL_ALERT_THRESHOLD`).

**Transition rules** (evaluated per netuid, inside one transaction):

| # | condition | action | event |
|---|---|---|---|
| T1 | netuid not in `subnets` | insert `occupants` row (`source='tmc'`): `status = 'active'` if `is_active` (and, on `/subnets/` rows, `first_emission_block_number` is not null), else `'pre_launch'`; `first_seen_block = last_seen_block = head_block`, `first_seen_at = last_seen_at = observed_at`; insert `subnets` row | `occupant_activated` only if inserted directly as `active` **and** the registry already has ≥ 1 prior sane run (first seed run emits nothing) |
| T2 | netuid in `subnets` and `network_registered_at` **equal** to the current occupant's | update `last_seen_block = head_block`, `last_seen_at = observed_at`, `name`, `symbol`, `missing_runs = 0`; back-fill `first_emission_block` if NULL and now known; if current status is `pre_launch` and the T1 activity condition holds → `status = 'active'`, `activated_at = observed_at`, `first_emission_block = first_emission_block_number` | `occupant_activated` |
| T3 | netuid in `subnets` and incoming `network_registered_at` **greater** than the current occupant's | close old: `status='deregistered'`, `deregistered_at=observed_at`, `dereg_reason='reoccupied'`, `last_seen_block` **unchanged** (= last run where it was seen), `successor_key = new_key`; insert new occupant per T1 with `predecessor_key = old_key`; repoint `subnets.occupant_key`. `registered_ts`/`deregistered_ts` are never written here (17, J1) | `netuid_reoccupied` (severity `warning`, immediate — 03 §3.9) |
| T4 | netuid in `subnets` and incoming `network_registered_at` **smaller** than the current occupant's | no mutation; per-netuid failure, not a run failure | `registry_anomaly(reason='reg_block_went_backwards')` (3-consecutive gate, §3.5) |
| T5 | netuid in `subnets` with status ≠ `deregistered` but **absent** from a sane run | `missing_runs += 1`; when `missing_runs ≥ REGISTRY_MISSING_RUNS_TO_DEREG (3)` → `status='deregistered'`, `dereg_reason='absent'`, `deregistered_at=observed_at` | `registry_anomaly(reason='netuid_absent')` at the 3rd consecutive miss (= the dereg run); `netuid_reoccupied` is **not** emitted (no successor) |
| T6 | `is_active` flips to `False` for an `active` occupant with unchanged registration | no status change here (emission_disabled is spec 05's red flag); log only | — |

Every sane run also rewrites the `subnets` row of each current occupant (03 §3.2): `network_registered_at`, `name`, `symbol`, `status`, `is_active`, `immune = (head_block − network_registered_at) < NETWORK_IMMUNITY_PERIOD_BLOCKS` (05's key, 864,000; master §5), `owner_coldkey`, `owner_hotkey`, `head_block`, `updated_at = observed_at`. The resolved-identity columns (`website_url`, `github_url`, `docs_url`, `discord_url`, `telegram_url`, `x_handle`) are written by the identity run after the merge (§2.7 R7).

`network_registered_at` never decreases for a live slot (T4 is a data glitch). A slot can be re-registered again before we ever saw the intermediate occupant; T3 handles it because it compares against whatever we last stored. Old occupant rows are never deleted (survival analysis, master §5).

**Re-registration vs owner change (verified 2026-09-23).** Only a new `network_registered_at` is a re-occupancy. Real examples from the old ledger `pool_history` (read-only query) confirmed by the 2026-09-22 TMC table: SN40 3,372,582 → **8,409,860** (≈ 2026-06-15, Chunking → Ralph) and SN92 7,013,758 → **8,352,006** (≈ 2026-06-07). The research's "27 Nodexo → SILX Orion, Nodexo now at 106" (research 02-gapfill-github §1/§3, which also labels 20, 53 and 119 "re-occupied") is **not** a re-registration: SN27 kept block 1,727,132 and SN106 kept 5,558,480 in both sources (likewise 20: 1,970,929, 53: 4,203,869, 119: 5,740,660). Such same-registration identity/owner changes keep the occupant key (master §5) and surface as `identity_changed` (chain `subnet_name`, warning) plus 11's owner-change path, never as `netuid_reoccupied`.

`registered_at_est` (INTEGER unix s, B1) is set at insert to `parse_ts(row.registered_at)` when present and within 7 days of the block estimate, else to the block estimate `observed_at − (head_block − network_registered_at) × BLOCK_SECONDS` (12 s/block drifts ≈ 2.5 d over 716 d: SN51's estimate is 2024-10-06 vs TMC's 2024-10-04). It is an estimate. Exact `registered_ts`/`deregistered_ts` are written only by 17 (J1); 04 preserves them on every update, and consumers use `COALESCE(registered_ts, registered_at_est)`.

### 2.3 On-chain `SubnetIdentitiesV3` reader (public JSON-RPC over HTTPS)

No `bittensor` SDK, no websockets. Every call is `await run.http.request('subtensor_rpc', 'POST', url, json={"jsonrpc":"2.0","id":n,"method":…,"params":[…]}, timeout=SUBTENSOR_RPC_TIMEOUT_SEC)` (G1: 01's `Http` sets the User-Agent, applies the `subtensor_rpc` limiter and retries 5xx/timeouts). A 200 body with an `error` member raises `RpcError`. Five calls today:

1. `chain_getFinalizedHead` → `block_hash`.
2. `chain_getHeader [block_hash]` → `int(result.number, 16)` = `block`.
3. `state_getKeysPaged [PREFIX_HEX, 1000, null, block_hash]` → list of storage keys; loop with `start_key = batch[-1]` while `len(batch) == 1000` (121 keys on 2026-09-22 → 1 call).
4. `state_queryStorageAt [keys, block_hash]` in chunks of `REGISTRY_CHAIN_QUERY_CHUNK` = 100 keys (the chunk size the research script used successfully; a larger chunk is **UNVERIFIED** on the public node) → `[{block, changes: [[key, value_hex|null], …]}]`; 121 keys → 2 calls.

The research's 3-call variant (no pinning, research script `chain_identities.py`) works; we spend one extra call to pin every read to the same finalised block so the observation is honestly point-in-time. Endpoint failover: after `Http` raises `ProviderError` on `SUBTENSOR_RPC_URL`, the whole read (steps 1–4) restarts on the next URL of `SUBTENSOR_RPC_FALLBACK_URLS`; steps are never mixed across nodes, because a pinned hash from one node may be pruned on another.

**Storage key.** `PREFIX = twox128("SubtensorModule") ‖ twox128("SubnetIdentitiesV3")`, where `twox128(s) = xxhash.xxh64_intdigest(s.encode(), seed=0).to_bytes(8, "little") + xxhash.xxh64_intdigest(s.encode(), seed=1).to_bytes(8, "little")` (≡ the research script's `xxh64(b, seed).digest()[::-1]`; dependency `xxhash>=3.4`). Verified constant (recomputed 2026-09-23 with xxhash 4.0.1; must be a unit-test vector):

```
twox128("SubtensorModule")    = 658faa385070e074c85bf6b568cf0555
twox128("SubnetIdentitiesV3") = c7cb9786b286b680ca204a6c0920ee52
PREFIX_HEX = "0x658faa385070e074c85bf6b568cf0555c7cb9786b286b680ca204a6c0920ee52"
```

Each full key is `0x` + 64 hex (prefix) + 16 hex (Twox64Concat hash of the SCALE-encoded `u16` netuid) + 4 hex (the raw `u16` netuid, little-endian) = 86 characters. `netuid = int.from_bytes(bytes.fromhex(key[-4:]), 'little')`. Test vector: the suffix for netuid 64 is `62cf9476b03dd865` + `4000`. Reject keys whose length ≠ 86 or whose prefix ≠ `PREFIX_HEX` (count in `chain_bad_keys`).

**Value decoding.** The value is a SCALE struct of eight `Vec<u8>` fields in this order: `subnet_name, github_repo, subnet_contact, subnet_url, discord, description, logo_url, additional` (pallet bounds 256/1024/1024/1024/256/1024/1024/1024 bytes). Each field = compact length + bytes:

```python
def compact_len(b: bytes, i: int) -> tuple[int, int]:
    m = b[i] & 3
    if m == 0: return b[i] >> 2, i + 1                                   # 0..63
    if m == 1: return int.from_bytes(b[i:i+2], "little") >> 2, i + 2     # 64..16383
    if m == 2: return int.from_bytes(b[i:i+4], "little") >> 2, i + 4     # 16384..2**30-1
    raise ScaleError("big-int compact not expected")                      # m == 3

def decode_identity_v3(value_hex: str) -> dict[str, str]:
    b = bytes.fromhex(value_hex[2:]); i = 0; out = {}
    for name in IDENTITY_V3_FIELDS:
        n, i = compact_len(b, i)
        if i + n > len(b): raise ScaleError(f"{name}: length {n} overruns value")
        out[name] = b[i:i+n].decode("utf-8", "replace"); i += n
    if i != len(b): raise ScaleError(f"{len(b)-i} trailing bytes")
    return out
```

Test vector (SN64, re-encoded 2026-09-23 from the research decode `chain_identities_v3.json`): the value `0x1843687574657388…` decodes to `subnet_name="Chutes"` (`0x18 = 6<<2`), then the 34-byte `github_repo` `https://github.com/chutesai/chutes` (`0x88 = 34<<2`), …, and the 141-byte `additional` with the two-byte compact `0x3502` (`(141<<2)|1 = 0x0235` LE). SN27's 102-byte `description` (`0x9901`) and 134-byte `logo_url` (`0x1902`) are further two-byte cases. A decode error for one netuid is a per-netuid failure (`identity_coverage.consecutive_misses += 1`, raw hex still stored in `identity_raw_snapshots`), never a run failure. `null` values (should not occur for returned keys) are treated the same way.

**Sanity gate:** `len(keys) ≥ REGISTRY_CHAIN_MIN_KEYS (90)`; otherwise the chain run is stored with `ok=0, error='sanity:low_key_count'` and **no** chain observations are written (prevents a partial read from emitting 100 "field removed" changes). Netuids with no key at all (verified 2026-09-22: root 0 and 35, 57, 59, 70, 84, 86, 108; of these 59, 86, 108 are not yet started) are covered by the run (`identity_coverage` updated with `has_row=0`) so their absence is *verifiable*.

**Fallback (chain unreachable or insane):** use `latest_snapshot.subnet_identities_v3` from `latest_rest_snapshot(db, 'subnets', max_age_s=12·3600)` (the occupant job's stored payload, G2; zero extra requests, survives restarts). Map camelCase → snake_case (`subnetName→subnet_name, githubRepo→github_repo, subnetContact→subnet_contact, subnetUrl→subnet_url, discord, description, logoUrl→logo_url, additional`). A key missing from a netuid's dict means *field not covered this run* (no observation, so no false `removed` event), not `''`. Observations from this path carry `source='chain'` but `source_ref='tmc_snapshot'` and `block_number = latest_snapshot.block_number`; `identity_changed` events from a fallback read have `payload.via='tmc_snapshot'`. Strings from TMC are already SCALE-decoded; the compact-prefix leak documented for subnetalpha's `subnet-stats` endpoint (master §5) does not apply because that endpoint is **not used**.

### 2.4 subnetalpha.ai WP-REST ACF reader

Port of `fundamentals_collect.py:73-111` (verified: `range(1, 4)` page loop, 400-break, empty-break, `< 100`-break, `_fields=slug,title,acf`, an 11-key projection) with these changes: `_fields` extended, logger instead of `print`, all calls through 01's `Http` with provider `subnetalpha` (G1; limiter `SUBNETALPHA_RATE_PER_SEC` = 1.0), sanity gate, raw snapshot storage.

```
await run.http.request('subnetalpha', 'GET', SUBNETALPHA_BASE_URL,
    params={"per_page": 100, "page": n, "_fields": "id,slug,title,modified_gmt,link,acf"},
    headers={"Accept": "application/json"}, timeout=SUBNETALPHA_TIMEOUT_SEC, ok_statuses=(200, 400))
```

`ok_statuses=(200, 400)` makes 01's `Http` hand the past-the-end 400 back without retrying (G1). Page loop: `n = 1, 2, …` until (a) HTTP **400** (WordPress `rest_post_invalid_page_number` — expected, not an error), (b) empty list, (c) `len(items) < 100`, or (d) `n > REGISTRY_SUBNETALPHA_MAX_PAGES (5)`. A non-JSON body (e.g. a Cloudflare challenge page) fails the source for this run. Read `X-WP-Total` / `X-WP-TotalPages` on page 1 and log them. Verified 2026-09-22: 128 records, 2 pages, netuids 1–128.

Per record: `netuid = int(re.search(r"(\d+)", acf["subnet_number"]).group(1))` (`_parse_netuid`, port of lines 62-70; skip and count records with no match); `wp_id = id`; `slug`; `name = title.rendered` (HTML-unescape); `event_at = parse_iso(modified_gmt + "Z")` (WordPress `modified_gmt` is GMT **without** a zone suffix, e.g. `2026-08-13T10:01:03`; a naive parse would apply local time); `link`. Two records for one netuid (a stale duplicate post): keep the newest `modified_gmt`, count `duplicate_netuid` in `meta_json`, store both raw snapshots. The slug is editorial and changes on rebrand — kept as `source_ref` and as the field `slug` (identity observations, info-level changes), never as a key.

The 32 ACF keys are stored verbatim in `identity_raw_snapshots` (one row per record when the ACF JSON hash changes) and mapped to identity fields as follows:

| ACF key(s) | identity `field` | note |
|---|---|---|
| `subnet_number` | — (key) | `"Subnet 01"…"Subnet 128"` |
| `title.rendered` | `subnet_name` | compare with chain name → name turnover |
| `subnet_owner` | `owner_name_editorial` | 15 are `"Unknown"` → junk |
| `github_url` | `github_repo` | 106 non-empty |
| `website_url` | `subnet_url` | 104 |
| `twitterx_url` | `x_url` | 102 (100 x.com, 2 twitter.com) |
| `discord_url` | `discord` | 112 (87 are the main vanity `discord.com/invite/bittensor`) |
| `telegram_url` | `telegram` | 7 |
| `short_desc` | `short_desc` | text |
| `what_exactly_does_it_do` | `description_long` | text (chain `description` stays separate) |
| `what_exactly_is_the_productbuild` | `purpose` | text |
| `team_info` | `team_info_html` | HTML; consumed by spec 11; stored on hash change only |
| `roadmap` | `roadmap_html` | HTML |
| `twitter_embed_id` | `twitter_embed_id` (03's field name) | numeric string; evidence only, never a candidate |
| `video_url`, `additional_video_1..8` | `video_url_0..8` | YouTube discovery (§2.6) |
| `video_description`, `video_description_1..8` | — | stored in raw snapshot only |
| `infographic` | — | WP media id; raw only |
| `modified_gmt` | `event_at` on every subnetalpha observation | no per-field timestamps exist; a bump does not say which field changed → the hash-dedupe per field is what detects the change |

Sanity gate: `len(records) ≥ REGISTRY_SUBNETALPHA_MIN_RECORDS (100)`. Netuid 0 (root) is never on subnetalpha; `identity_coverage` records `has_row=0` for it every run so its absence is verifiable, not unknown. A record whose `netuid` maps to a netuid whose current occupant was registered **after** `modified_gmt` is still stored (it is the site's current statement) but the merge marks its candidates `stale_for_occupant=1` and they lose precedence to TMC coldkey values (§2.7 rule R4) — this is the mechanism that stops a pre-June "Chunking" post from labelling SN40's re-registered occupant (Ralph, block 8,409,860). R4's identity-epoch clause does the same for same-registration owner changes such as SN27 Nodexo → Orion (subnetalpha was already current for 27 and 106 on 2026-09-22: `Orion`, modified 2026-08-13; `Nodexo`, 2026-08-27).

### 2.5 TMC coldkey identities (owner identity)

`resp = await tmc.fetch_coldkey_identities()` (05's `TmcClient`, G2) once per identity run, then `write_rest_snapshot(db, 'coldkey_identities', resp, run_id=run.run_id)` when `resp.ok`. Verified 2026-09-22: a **bare JSON list** of 1,088 rows (240,748 bytes, keys `additional, coldkey, description, discord, github_repo, image, name, url`); should TMC ever switch to `{count, results}`, 05's `get_paginated` pages it by `offset` (never via the `next` URL). Index by `coldkey`. For each current occupant whose `subnets.owner_coldkey` (from §2.2) has a row, write observations with `source='tmc_coldkey'`, `source_ref=coldkey`. On 2026-09-22 86 of 129 owner coldkeys had a row; 31 of those are named `Owner<n>` (placeholders → junk, §2.6) and 45 carry a `url` or `github_repo`; some values are a single space (`' '`, SN27's owner) and trim to empty.

| coldkey-identity column | identity `field` |
|---|---|
| `name` | `owner_name` |
| `url` | `owner_url` — also a **lower-precedence candidate** for `subnet_url` |
| `github_repo` | `owner_github` — also a candidate for `github_repo` |
| `discord` | `owner_discord` — also a candidate for `discord` |
| `image` | `owner_image` |
| `description` | `owner_description` |
| `additional` | `owner_additional` |

The owner coldkey itself is written as field `owner_coldkey`, `source='tmc'` (its change is `identity_changed` **info**/digest — owner swaps page through 11's red flag, D4), and the TMC name (`subnetName` of the stored `/subnets/` snapshot, else the table `name`) as field `subnet_name`, `source='tmc'` (the lowest automatic `subnet_name` candidate, §2.7 R1; TMC's `Unknown` is junk). An occupant whose coldkey has no identity row gets `identity_coverage(source='tmc_coldkey', has_row=0)` → verifiable absence → spec 11's `owner_identity = 0.0`. A `BudgetExhausted` or `ProviderError` from the TMC call marks the `tmc_coldkey` source not-ok for this run (last observations stand, §4) and the chain/subnetalpha steps continue.

### 2.6 Normalisation and classification (`app/registry/normalise.py`)

`classify(raw: str, expected_field: str) -> list[Ref]` is pure and total (never raises; `[]` for empty input); the first `Ref` is the primary. `Ref = {kind, canonical, handle, extra, is_junk, junk_reason, reclassified_from}`. Public helpers other specs import (one parser for the whole system):

```python
def normalise_x_handle(s: str | None) -> str | None
    # s = an X/Twitter URL, "@handle" or bare "handle". Returns the handle WITHOUT '@', original case
    # (callers compare with .lower()), or None when s is empty, junk, a non-profile path (i/…, intent/…,
    # search…, home, hashtag/…, share…) or fails ^[A-Za-z0-9_]{1,15}$. Hosts per the X rule below;
    # "…/status/{id}" returns the first path segment. Used by classify() for x_url and by 09's
    # app/collectors/x_handles.py (09 adds its own checks on top).
def canonical_website(url: str | None) -> str | None
    # the Website rule below (used by 07 for URLs it discovers itself); None if junk/not a web URL
def compare_key(ref: Ref) -> str       # R6 grouping key
```

**Common pre-processing:** strip whitespace, surrounding quotes/angle brackets, trailing punctuation `.,;)`; collapse internal whitespace; lower-case scheme and host; strip fragment; strip `utm_*`, `ref`, `si`, `feature` query params; strip a single trailing `/`; add `https://` when a known host appears without scheme (`github.com/…`, `x.com/…`, `discord.gg/…`, `t.me/…`, `www.…`) **and**, in URL-typed fields (`github_repo, subnet_url, website_url, url, discord, logo_url, twitterx_url, telegram_url, video_url*`), when the whole value is a bare domain `^[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+(/\S*)?$` (verified case: SN51's chain `subnet_url = "lium.io"`, `subnetUrl: 'lium.io'` in TMC).

**Several URLs in one value / secondary refs.** Text fields (`additional`, `description`, a non-URL `subnet_contact`) and multi-URL values are tokenised; every URL token is classified. In a URL-typed field the first token whose kind matches the field is the primary; every other ref — and every ref found in a text field — is stored as a **secondary** observation under `"{target_field}__extra_{i}"`, where `target_field` is the field of the ref's kind (website → `subnet_url`, github → `github_repo`, x → `x_url`, discord_* → `discord`, telegram → `telegram`, youtube* → `youtube`) and `i` is the 0-based order of that kind within the source value. Example (verified SN106): `additional = "API: https://nodexo.ai/api Docs: https://nodexo.ai/docs"` → `subnet_url__extra_0 = https://nodexo.ai/api` and the derived `docs_url`. A **reclassified** primary (a social URL in the wrong field, e.g. SN106's `subnet_contact = https://discord.com/channels/799672011265015819/1174835090539433994`) keeps its own field's observation (with `ref_kind='discord_channel'`) and adds `discord__extra_0` with `reclassified_from='subnet_contact'`. A reclassified ref is **never** written under the target's primary field name: two same-run rows for one `(occupant, source, field)` would make the hash-dedupe flip every run and emit a spurious `identity_changed` each time.

**Junk (`is_junk=1`, the candidate is dropped)** — any of: trimmed value matches, case-insensitively, `^(none|null|nil|n/?a|-+|—|~|tbd|tba|todo|pending\.{0,3}|coming soon|soon|deprecated|inactive|for sale|parked|placeholder|unknown|awaiting data\.{0,3}|awating data\.{0,3}|test|example|xxx|\.)$` (`~` verified: SN117's legacy `website`/`discord`) → `token`; `owner_name` matching `^owner\s*\d+$` (31 of the 86 subnet-owner coldkey identities on 2026-09-22 are `Owner<n>`) → `token`; an email whose local part is one of the tokens (SN39 `deprecated@gmail.com`) → `token`; a URL-typed field whose value has no `.` and no `/` and is < 3 chars → `too_short`; host ∈ `{example.com, localhost, deprecated.com, www.deprecated.com, deprecated.png, test.com}` or host matches `^(deprecated|pending|placeholder)\b` → `junk_host`; GitHub placeholder (below) → `placeholder_repo`; a bare Discord handle in a field expecting a URL is **not** junk (it is `discord_handle`). **Flags that keep the value** (`is_junk=0`, `junk_reason` set, still a valid candidate, still emit `identity_junk`): `abandonment_marker` — the `subnet_name` tokens `deprecated|for sale|parked|inactive|pending` (the name still resolves; verified chain names: SN39 `deprecated`, SN73 `Parked`, SN112 `for sale`); `leaked_secret` (GitHub table). Spec 12's `stale_identity` gate reads the `abandonment_marker` events (12 §2.8).

**GitHub** (`kind ∈ {github_repo, github_owner}`; port and extend `parse_repo` semantics — do **not** copy its regex, which rejected `/orgs/…` and org-only URLs):

| input form | result |
|---|---|
| `github.com/{o}/{r}[.git][/tree/…\|/blob/…\|/releases…][?tab=…][/]` | `github_repo`, `handle="{o}/{r}"` (original case), `canonical="https://github.com/{o}/{r}"`, `extra.key="{o.lower()}/{r.lower()}"` |
| `github.com/{o}[/]`, `github.com/orgs/{o}/repositories`, `github.com/orgs/{o}` | `github_owner`, `handle="{o}"` |
| owner ∈ `{orgs, sponsors, topics, settings, features, marketplace, apps, login, about, site, explore, search, collections, trending}` with no valid remainder, or `(o,r) ∈ {(username,repo),(deprecated,deprecated),(user,repo),(owner,repo),(org,repo),(your-org,your-repo)}` | junk `placeholder_repo` |
| `raw.githubusercontent.com/…`, `gist.github.com/…`, `{o}.github.io` | `website` (reclassified) |
| a URL with `?token=GHSAT…` / `ghp_` / `github_pat_` anywhere (verified: SN27's chain `logo_url` `https://raw.githubusercontent.com/troy12x/silx-web/…/orion.png?token=GHSAT0AAAA…`) | `is_junk=0`, `junk_reason='leaked_secret'`; `value` and `value_norm` store the **redacted** form (`?token=<redacted>`), `value_hash` is over the raw string (so a rotated token is still a change); emit `identity_junk` (severity `warning`) — the string is public on-chain, but the operator should know and it must not travel into Telegram text |

`extra.key` (lower-cased `o/r`) is the string key recorded in `candidates_json`. The registry emits **no** `source_disagreement` for `github_repo`: equivalence is by numeric repo id (verified: `rayonlabs/chutes` → 301 → `chutesai/chutes`, id 884216433; `CortexLM/platform` and `baseintelligence/base` both → id 972956783), which only 06 resolves, and 06 is the `github_repo` emitter (D2). A string mismatch between github candidates is recorded as `flags_json` `github_string_mismatch` on the resolved row only.

**X** (implemented by `normalise_x_handle`): hosts `x.com, twitter.com, mobile.twitter.com, www.x.com, www.twitter.com, fxtwitter.com, vxtwitter.com`; `handle` = first path segment without `@`, must match `^[A-Za-z0-9_]{1,15}$`; canonical `https://x.com/{handle}` (handle kept in original case, compared lower-cased). Paths `i/…`, `intent/…`, `search…`, `home`, `hashtag/…`, `share…` → junk `token`. `…/status/{id}` → handle from the first segment plus `extra.status_id`. A bare `@handle` or bare `handle` in an X-typed field → kind `x_handle` with the same canonical URL. Cases pinned by test 20: `https://twitter.com/Foo/` → `Foo`; `x.com/@Foo` → `Foo`; `https://mobile.twitter.com/foo/status/123` → `foo` (+ `status_id`); `@chutes_ai` → `chutes_ai`; `https://x.com/i/communities/1` → None; `https://x.com/this_handle_is_too_long` → None.

**Discord:**

| input | kind | handle | extra |
|---|---|---|---|
| `discord.gg/{code}`, `discord.com/invite/{code}`, `discordapp.com/invite/{code}`, `discord.com/invite/{code}?…` | `discord_invite` | `code` (case-sensitive) | `is_main_vanity = (code.lower() == "bittensor")` |
| `discord.com/channels/{guild_id}/{channel_id}[/{message_id}]` | `discord_channel` | `channel_id` | `guild_id`, `is_main_guild = (guild_id == DISCORD_MAIN_GUILD_ID)`, `message_id` |
| `discord.com/users/{id}` | `discord_handle` | `id` | `is_user_id=1` |
| `(?i)^@?[a-z0-9_.]{2,32}$` (no leading/trailing `.`, not a junk token; case-insensitive because legacy names are mixed-case — verified chain values `CreativeBuilds` SN77, `Tag101` SN101, `@unarbos` SN3, `p383_54249` SN51) | `discord_handle` | handle without `@`, lower-cased | — |
| `discord.com`, `discord.gg` alone | junk `too_short` | | |

**Telegram:** `t.me/{h}`, `telegram.me/{h}`, `telegram.dog/{h}`, `t.me/s/{h}` → `telegram_public`, `handle=h` (regex `^[A-Za-z0-9_]{5,32}$`); `t.me/+{hash}`, `t.me/joinchat/{hash}` → `telegram_private`, `handle="+{hash}"`; `@handle` → public.

**YouTube:** `youtube.com/channel/{UC[\w-]{22}}` → `youtube_channel` (handle = id, canonical `https://www.youtube.com/channel/{id}`); `youtube.com/@{h}`, `youtube.com/c/{h}`, `youtube.com/user/{h}` → `youtube_handle` (`needs_resolution=1`); `youtube.com/watch?v={id}`, `youtu.be/{id}`, `youtube.com/shorts/{id}`, `youtube.com/embed/{id}` → `youtube_video` (`needs_resolution=1`; the channel is recoverable keylessly via `https://www.youtube.com/oembed?url=…&format=json` → `author_url`, done by 09). Anything else on `youtube.com` → `website`.

**Website** (implemented by `canonical_website`): scheme `http|https`, host contains a dot and is not one of the social hosts above; canonical = scheme + host (lower) + path (case preserved, trailing `/` stripped) + retained query. If the host **is** a social host, the ref is **reclassified** into the matching kind and stored as a secondary per the rule above (e.g. SN106's `subnet_contact` deep link becomes the `discord__extra_0` candidate with `reclassified_from='subnet_contact'`).

**Docs URL:** `docs_url` candidates are any classified `website` ref whose host starts with `docs.` or whose first path segment ∈ `{docs, documentation, api-docs, developers, dev, wiki}` or that appears in `additional` after the token `Docs:`; also `gitbook.io`, `readthedocs.io`, `mintlify.app`, `notion.site` hosts. Docs is a derived field (`field='docs_url'`, same source as its parent value).

**Email:** `^[^@\s]+@[^@\s]+\.[^@\s]+$` → `email` (from `subnet_contact`; stored, never published to the UI without the operator's say — it is public on-chain but treat as contact data).

**Text fields** (`subnet_name, description, short_desc, …`): `canonical = unicodedata.normalize("NFKC", value).strip()`; comparisons use `casefold()` and collapsed whitespace. `subnet_name` additionally strips a trailing ` (SNnn)` / ` SN nn` and Greek/Unicode symbol suffixes for the turnover comparison only.

### 2.7 Per-field merge (`app/registry/merge.py`)

For each `(occupant_key, field)` in `RESOLVED_FIELDS = [subnet_name, github_repo, subnet_url, discord, x_url, telegram, docs_url, description, short_desc, logo_url, subnet_contact, owner_name, owner_url, owner_github, owner_discord, youtube]`, gather **candidates**: the latest `identity_observations` row per source for that field (plus mapped alternates: `owner_url`→`subnet_url`, `owner_github`→`github_repo`, `owner_discord`→`discord`, `video_url_*`→`youtube`, `{field}__extra_*`→`field`; `twitter_embed_id` is evidence only), plus `manual_overrides` rows with `status='active'` for the field. Within one source the primary observation outranks its `__extra_i` rows (ascending `i`), which outrank mapped alternates. Then:

| rule | statement |
|---|---|
| R1 precedence | `chain (source_ref≠tmc_snapshot) = chain (tmc_snapshot) > subnetalpha > tmc_coldkey > tmc (subnet_name only: the TMC name) > manual(mode=fallback, source=manual) > manual(mode=fallback, source=legacy)`. The first candidate that is non-empty, non-junk (`is_junk=0`; flagged values such as `abandonment_marker`/`leaked_secret` stay eligible) and not suppressed wins. |
| R2 force | A `manual_overrides` row with `mode='force'` wins over everything; `resolved.source='manual'`, `override_id` set. Requires `reason` and `evidence_url` (validated by `add_override`, §3.3). This answers 06's open question 1 (an operator repo correction can beat a dead chain repo). |
| R3 suppress | A `mode='suppress'` row hides one specific `(suppress_source, suppress_value_hash)` candidate; the chain falls through. Examples: SN117's dead chain repo `everyframe-studios/everyframe-miner` (404, research 02-gapfill-github §2/§3); SN55's chain invite that lands on a look-alike guild (08 review note). 06 and 08 write no overrides; only the operator (via 14) and the seed import do. |
| R4 stale-for-occupant | A subnetalpha candidate whose `event_at` (`modified_gmt`) < `identity_epoch(occ) − 2 d` is demoted below `tmc_coldkey`, `tmc` and `manual`; if it still wins it is marked `resolved.flags += ['stale_editorial']`. `identity_epoch(occ, t)` = max of (a) `COALESCE(registered_ts, registered_at_est)` (re-registration, e.g. SN40) and (b) the `observed_at` of the latest **change** (a row whose `value_hash` differs from the previous row of the same key) among the occupant's chain `subnet_name` and tmc `owner_coldkey` observations with `observed_at ≤ t` (same-registration owner/identity change, e.g. SN27). Clause (a) alone is what 11's team-seed guard calls "rule R4". |
| R5 status | `present` if a winner exists; `junk_only` if every non-empty candidate is junk; `absent` if all *covering* sources (those with `identity_coverage.has_row` for this occupant in their last ok run, or a structural non-coverage like netuid 0 on subnetalpha) reported empty; `unknown` if at least one enabled source has never covered this occupant **and** no candidate exists. |
| R6 disagreement | For every field **except `github_repo`** (06's, D2): among non-junk, non-suppressed candidates from **different** sources, excluding Discord invites with `is_main_vanity=1` (a generic pointer to the main server, not a venue claim — the same exclusion 08 applies to `source_agreement`; 87 of 112 subnetalpha `discord_url` values are that vanity), group by `compare_key` (`handle.lower()` for X/Telegram/Discord invite, `channel_id` for Discord channel, canonical for website, `casefold` name for `subnet_name`). If > 1 group → `source_disagreement` with `pair_hash = sha256("|".join(sorted(group_keys)).encode()).hexdigest()[:16]`, emitted once per `(occupant_key, field, pair_hash)` (03 dedupe `occ|field|pair_hash`). Discord invite vs channel deep link is **not** a disagreement (different kinds); two different non-vanity invite codes are (verified example: SN106 chain `fwjBwxUcdX` vs subnetalpha `t7BMee4w`, the latter dead). 08 does not suppress them; its `source_agreement` is the guild-level view. |
| R7 write | Upsert `identity_resolved` (PK `occupant_key, field`) with `value, value_norm, ref_kind, source, source_observed_at, status, candidates_json, override_id, flags_json, resolved_at, run_id` (times INTEGER, B1). `candidates_json` lists every candidate (source, field, value, hash, is_junk, junk_reason, demoted, suppressed) so the UI can show the evidence. Then update the denormalised `subnets` columns (03): `website_url` ← `subnet_url`, `github_url` ← `github_repo`, `docs_url`, `discord_url` ← `discord` canonical URL (NULL for a `discord_handle`), `telegram_url` ← `https://t.me/{handle}`, `x_handle` ← X handle without `@` (all NULL unless `status='present'`). If the winning `value_norm` differs from the previous resolved row, set `payload.resolved_changed=true` on the triggering `identity_changed` event (or emit a synthetic `identity_changed` with payload `source='resolved'`, severity `info`, when the change came from an override). |

`_pick` (fundamentals_collect.py:226-231, "first non-empty trimmed string") is the ancestor of R1; the new version is `pick(candidates: list[Candidate]) -> Candidate | None` operating on classified refs, not strings.

### 2.8 `identity_changed` and `identity_junk`

`ObservationWriter.write(occ, source, field, ref, raw, *, event_at, block_number, source_ref)` (all instants INTEGER unix s; `observed_at` is the writer's, = `run.started_at` for live runs):
1. Compute `value_hash = sha256(raw_trimmed.encode()).hexdigest()` (`''` for empty → hash of the empty string).
2. Load the latest row for `(occupant_key, source, field)`.
3. If none → insert; **no event** (first observation of a field for an occupant/source is a baseline, not a change).
4. If `value_hash` equal → insert a heartbeat row only when `observed_at − latest.observed_at ≥ REGISTRY_REOBSERVE_DAYS (30) × 86400`; no event. (Legacy writes use `observed_at = as_of`, so re-running an import never adds rows.)
5. If different → insert; emit `identity_changed` (`kind` = `added` when the old value was `''`, `removed` when the new one is `''`, else `changed`). Severity (D4, 03 §3.9): `warning` only when `source='chain'` and `field ∈ {subnet_name, github_repo, subnet_url, discord}` (owner-signed identity moved — the set 11 pairs for `control_stability_90d` and `owner_control_change_trigger.value_json.identity_churn`, C2); everything else `info`, including `owner_coldkey` (owner swaps page through 11's red flag, D4).
6. If `ref.junk_reason` is set and no `identity_junk` event exists for `dedupe_key = f"{occupant_key}|{source}|{field}|{value_hash}"` → emit `identity_junk` (`info`; `warning` for `abandonment_marker`/`leaked_secret`).

No events at all are emitted for `source='legacy'`, for `{field}__extra_*` and the derived `docs_url`, for the coverage marker field `_coverage` (§3.3 PIT replay), or for `twitter_embed_id` (evidence); `slug` changes are ordinary `info` events. Every event goes through 03's `emit_event(db, …, source='registry', run_id=run.run_id)` (B9) inside the source's write transaction.

Chain observations from a run flagged `chain_suspect` (key count between `REGISTRY_CHAIN_MIN_KEYS` and 110; 121 on 2026-09-22) are written but **`removed` transitions are not emitted** for netuids missing from that run; the `registry_anomaly(reason='chain_low_key_count')` counter is advanced instead (3-consecutive gate, §3.5).

### 2.9 `watch_channels`

After the merge, `WatchChannelsBuilder.rebuild(occupant_key)` upserts one row per distinct `(occupant_key, platform, handle)` (03's `ux_watch_channels`; `INSERT OR IGNORE` then `UPDATE` by id) over **all non-junk, non-suppressed candidates** (not just winners) for the fields `discord, x_url, telegram, youtube, subnet_url, docs_url` plus the owner alternates. `netuid` is denormalised; `source` = the highest-precedence source currently declaring the target and `priority` = 03's value for it (manual force 5 · chain 10 · subnetalpha 20 · tmc/tmc_coldkey 30 · manual fallback/legacy 40); `meta_json.declared_by` = sorted list of declaring sources. On INSERT the builder sets `needs_resolution` and `first_seen_at`; on UPDATE it touches only `last_seen_at`, `is_resolved`, `source`, `priority`, `meta_json.declared_by` and clears `retired_at` — never `handle`, `verified_at`, `guild_id`, `needs_resolution` or other `meta_json` keys, which the validating collectors own:

| platform | handle | url | notes |
|---|---|---|---|
| `discord_invite` | invite code | `https://discord.gg/{code}` | `is_main_guild=1` for the `bittensor` vanity; 08 polls own-server invites every 6 h and the vanity once per cycle |
| `discord_channel` | channel id | `https://discord.com/channels/{guild}/{channel}` | `guild_id` set; 18 subnets via chain deep links (verified 2026-09-22); a bare channel id cannot be validated keylessly, so 08 trusts it while the main vanity is alive and re-checks only the ≈ 5 main-channel invite codes weekly (08 §11 Q23); the operator's `seed/discord_channels.csv` (checklist B5) is loaded by 08 into the same rows with `source='operator'`, `priority=45` |
| `discord_handle` | handle | — | contact signal only; never polled |
| `x` | handle | `https://x.com/{handle}` | 09 reads timelines |
| `telegram` | handle or `+hash` | `https://t.me/{handle}` | 08 scrapes counts |
| `youtube` | `UC…` id or `@handle` | channel URL | `needs_resolution=1` for handles/videos; 09's appendix resolves them into `meta_json.channel_id` + `verified_at` + `needs_resolution=0` on the same row (it must not rewrite `handle`: the rebuild keys on it and would re-insert the input row) |
| `youtube_video` | video id | `https://www.youtube.com/watch?v={id}` | resolution seed only |
| `website` | canonical URL | same | 07 checks liveness; `docs` likewise |
| `docs` | canonical URL | same | 07 `docs_present` |

`is_resolved=1` marks the row that equals the resolved winner for its field. The retirement sweep (end of each identity run) covers rows with `source ∈ {chain, subnetalpha, tmc, tmc_coldkey, manual, legacy, operator}` (08 touches its `operator` rows every `discord_6h` run and relies on this sweep); rows not seen for `REGISTRY_WATCH_RETIRE_DAYS (14)` days get `retired_at` set (never deleted; consumers filter the generated `active`/`retired_at IS NULL`). Rows of `web_autodiscover` (07), `seed_feeds` (09) and every `occupant_key IS NULL` row are never retired by 04. Rows of deregistered occupants are retired at the T3/T5 transition. `verified_at` is written only by the collector that actually validated the target (08/09/07), never by the registry.

### 2.10 "Needs review" (`app/registry/review.py`)

`compute_needs_review(db, now: int) -> list[ReviewItem]`, no storage (03: "04's needs-review list stays computed"), each item `{occupant_key, netuid, name, reason, detail, since, event_id}` (`since` INTEGER; ISO only when 14 renders it); reasons, in priority order:

1. `new_occupant` — inserted by T3, or by T1 after the first sane run (`first_seen_at > job_state('registry_occupants','first_sane_at')`), with `first_seen_at` within 7 d. (Without the watermark every occupant would be "new" for a week after a fresh install.)
2. `reoccupied_unreviewed` — `netuid_reoccupied` event with no `manual_notes` row whose `ref_event_id` = that event id (03 column).
3. `source_disagreement_open` — latest `source_disagreement` per `(occupant, field)` (04's non-GitHub fields and 06's `github_repo`) with no active `manual_overrides` row whose `resolves_event_id` = that event id.
4. `abandonment_marker` — `identity_junk` with `junk_reason='abandonment_marker'` on `subnet_name` for a non-deregistered occupant (verified chain names 2026-09-22: SN39 "deprecated", SN73 "Parked", SN112 "for sale").
5. `name_turnover` — resolved chain `subnet_name` ≠ subnetalpha `subnet_name` (casefold, symbol-stripped) — 25 netuids per research 02-gapfill-discord-telegram (a raw, unstripped comparison of the 2026-09-22 dumps gives 38; the symbol stripping of §2.6 accounts for part of the gap — expected count **UNVERIFIED**).
6. `identity_void` — `github_repo`, `subnet_url`, `discord`, `x_url` all `absent`/`junk_only` for an active occupant other than root 0.
7. `chain_identity_missing` — **active** occupant (other than root 0) with `identity_coverage(chain).has_row=0` (2026-09-22: 35, 57, 70, 84; 59, 86, 108 are pre_launch).
8. `legacy_needs_review` — `manual_overrides` rows with `status='needs_review'` (seed/legacy import, §2.11).
9. `never_activated` — `pre_launch` for > 120 d.
10. `unresolved_watch_target` — `watch_channels.needs_resolution=1` older than 14 d.
11. `leaked_secret` — `identity_junk(leaked_secret)` whose value is still the current chain value.

Transport is 14's (A2): 14's `needs_review()`/`/needsreview` digest and any route must include this list (a new cross-spec item: 14 §8.3's six buckets do not call it yet). For Phase 1 (before 14) the list is printed by `python -m app.registry review [--reason R] [--json]`.

### 2.11 Seed script `scripts/seed_registry.py`

```
python -m scripts.seed_registry [--dry-run] [--no-network] [--legacy-only]
       [--fundamentals seed/fundamentals.json] [--edge-notes seed/subnet_edge]
       [--legacy-as-of 2026-06-02T11:03:41Z] [--edge-as-of 2026-06-18T14:37:08Z]
       [--report data/seed_registry_report.json]
```

Steps (idempotent; safe to re-run; `collector_runs.job='seed_registry'`, `run_id='script:seed_registry:<started_at>'`, 03). The script builds a runtime without scheduler/HTTP server (01's `run-job` code path), so its HTTP calls use the same provider ids and limiter; while the service is running, run it under 16's `scripts/with_bucket.py` (the TMC per-IP bucket is shared).
1. Run `RegistryOccupantsJob` once through 01's wrapper → `occupants`/`subnets`. Requires network unless `--no-network` (then only legacy import against existing occupants).
2. Run `RegistryIdentityJob` once.
3. **Legacy fundamentals import** = `import_fundamentals(db, path, as_of=parse_ts(LEGACY_AS_OF), run_id=run_id, occupancy=None)` (J1; 17's `scripts/import_fundamentals.py` calls the same function with its `OccupancyIndex`). Input verified: schema_version 2, `generated_at 2026-06-02T11:03:41.944123Z` (unix 1780398221), 129 records keyed by netuid string; used columns `name, owner, website, github, twitter, discord, telegram, short_desc`. For each netuid:
   - **Occupant then.** With `occupancy` given: `occ_then = occupancy.occupant_at_ts(netuid, as_of)`; observations are written to `occ_then` (must exist in `occupants`, FK), overrides only if `occ_then` is the current occupant. With `occupancy=None` (or `occ_then` unknown): the **re-occupancy guard** — skip if `COALESCE(registered_ts, registered_at_est) > as_of` (report `skipped_reoccupied`); otherwise `occ_then` = the current occupant. On 2026-09-22 this skips 14 netuids registered after the snapshot (TMC table: 16, 35, 36, 40, 58, 59, 70, 76, 86, 90, 92, 99, 103, 108).
   - **Name guard:** if `casefold(legacy.name) ≠ casefold(resolved subnet_name)` and neither contains the other → import with `status='needs_review'`. Verified cases: the same-registration identity/owner changes SN27 Nodexo→Orion, SN106 Liquidity Provisioning→Nodexo, SN20 GroundLayer→Witness, SN53 Efficient Frontier→engy, SN119 Satori→CapabilityForge Next, and the same-owner rebrand SN100 Platform→Cortex. Else `status='active'`.
   - Each non-empty, non-junk link becomes a `manual_overrides` row: `mode='fallback'`, `source='legacy'`, `source_ref='fundamentals.json@2026-06-02'` (J1), `field` per column (`github→github_repo, website→subnet_url, twitter→x_url, discord→discord, telegram→telegram, owner→owner_name_editorial, short_desc→short_desc`), `value` = canonical ref, `created_by='seed_registry'` (17: `'import_fundamentals'`). Also written as `identity_observations` rows with `source='legacy'`, the same `source_ref`, `observed_at = event_at = as_of` (point-in-time honest: the old bot held these values then) so backtests can see them; plus one `identity_raw_snapshots(source='legacy', scope='subnet')` row per record.
   - Junk legacy values (verified: SN3 `https://github.com/username/repo`, `www.deprecated.com`, `deprecated`; SN39 `deprecated/deprecated`; SN117 `~`) are **not** imported; counted.
   - `LegacyReport(imported: int, needs_review: int, skipped_reoccupied: int, junk: int, attributed_via_occupancy: int, attributed_via_guard: int, observations: int, overrides: int, per_netuid: dict[int, str])`.
4. **Edge-notes import** `import_edge_notes(db, dir, *, as_of, run_id) -> LegacyReport` (`seed/subnet_edge/sn{NN}_{slug}.md`; verified: 25 such files plus `README.md`, five `_*.md` files and a `synth_backtest/` directory, all skipped): netuid from the filename regex `^sn(\d+)_`; name from the first `# SN\d+ (.+)` heading; every `https?://[^\s)>\]"']+` in the file is classified; GitHub/X/Discord/Telegram/website/docs refs become `manual_overrides`/`identity_observations` rows (`source='legacy'`, `source_ref='subnet_edge/sn64_chutes.md'`) under the same guards with `as_of = --edge-as-of` (default = the old checkout's file mtime 2026-06-18T14:37:08Z; authoring dates are **UNVERIFIED** — some notes say "as of mid-2025"); `subnetalpha.ai`, `taostats.io`, `taomarketcap.com`, `backprop.finance` links are ignored. The note's `**Verdict:**` line is stored in `manual_notes` (`kind='seed_edge'`, `body` = the line, `source_ref` = file, `created_by='seed_registry'`; 03's kind vocabulary) — it is trading-edge, not health (master §2), so it never feeds a metric.
5. Rebuild `watch_channels`, compute the needs-review list, write the JSON report with `write_atomic` (port of `fundamentals_collect.py:388-393`, verified: write `path.tmp`, `json.dump(indent=2)`, `os.replace`) into `app/util/fs.py` (11 reuses it).

Report shape: `{seeded_at (ISO), occupants: {active, pre_launch, deregistered}, observations_written: {chain, subnetalpha, tmc_coldkey, tmc, legacy}, legacy: LegacyReport, edge_notes: LegacyReport, events: {…}, needs_review_count}`.

---

## 3. Data contract

Table names follow master §6. **The final DDL lives only in 03 §3.2/§3.3/§3.6 (B7); this spec ships no `CREATE TABLE`.** Every instant below is `INTEGER` unix seconds UTC (B1); ISO appears only in `/health`, CLI output and Telegram text.

### 3.1 Tables written

| table (03) | class | columns this spec writes | notes |
|---|---|---|---|
| `occupants` | state | `occupant_key, netuid, network_registered_at, status, name, symbol, first_seen_block, first_seen_at, last_seen_block, last_seen_at, first_emission_block, activated_at, deregistered_at, dereg_reason, predecessor_key, successor_key, registered_at_est, missing_runs, source='tmc'` | never writes `registered_ts`/`deregistered_ts` (17, J1) and preserves them; 17 inserts historical rows and may update `first_seen_*`, `registered_ts`, `predecessor_key`; the status of a current occupant is only ever changed by 04 |
| `subnets` | state | all 14 columns (§2.2 + §2.7 R7) | one row per netuid; `updated_at` = last sane occupant run |
| `identity_observations` | append | `occupant_key, netuid, source ∈ {chain, subnetalpha, tmc_coldkey, tmc, manual, legacy}, field, value, value_norm, ref_kind, value_hash, is_junk, junk_reason, reclassified_from, observed_at, event_at, block_number, run_id, source_ref` | `field` = §2.7 `RESOLVED_FIELDS`, the raw-only fields (`owner_coldkey`, `slug`, `twitter_embed_id`, `owner_name_editorial`, `description_long`, `purpose`, `team_info_html`, `roadmap_html`, `video_url_0..8`, `additional`, `owner_image`, `owner_description`, `owner_additional`), `{field}__extra_{i}`, and the coverage marker `_coverage` (§3.3) |
| `identity_coverage` | state | all columns | per `(source, occupant_key)`; `last_ok_*` from the source's last ok run |
| `identity_raw_snapshots` | append | all columns (`scope ∈ {subnet, coldkey}`, `block_number`) | written only when `payload_hash` changes; chain rows keep the raw value hex |
| `identity_resolved` | state | all columns | R7 |
| `manual_overrides` | state | all columns | legacy rows from §2.11; operator rows via `add_override` (§3.3); unique index is 03's `COALESCE` expression index |
| `watch_channels` | state | `occupant_key, netuid, platform, handle, url, source, priority, is_resolved, is_main_guild, guild_id, needs_resolution, first_seen_at, last_seen_at, retired_at, meta_json` | §2.9 rules; never `verified_at`, `label`, `filtered` |
| `chain_rest_snapshots` | append | via 05's `write_rest_snapshot` only: kinds `subnets` (`endpoint='/subnets/'`), `subnets_table`, `coldkey_identities` (`endpoint='/accounts/coldkey-identities/'`) | G2; 03 key `(endpoint, params, observed_at)`, no `kind` column (05's `REST_SNAPSHOT_KINDS` maps kind → endpoint) |
| `events` | append | via 03's `emit_event(..., source='registry')` | §3.5 |
| `collector_runs` | state | the wrapper (01) inserts/finishes the row; 04 adds `meta_json` for `run.run_id` (`{head_block, rows_in, fallback_used, sources: {chain|subnetalpha|tmc_coldkey: {ok, error, via, n}}, anomalies}`) and `note` via `JobOutcome` | seed script: `job='seed_registry'` |
| `job_state` | state | `registry_occupants`: `head_block`, `first_sane_at`, `anomaly:{reason}:{netuid}` counters; `registry_identity`: `anomaly:*`, `source_fail:{source}` counters | runtime watermarks only (B12) |
| `manual_notes` | append | seed script only: `occupant_key, kind='seed_edge', body, source_ref, created_at, created_by` | 03 vocabulary |

### 3.2 Tables read
`occupants, subnets, identity_observations, identity_coverage, identity_resolved, manual_overrides, watch_channels, events, manual_notes` (needs-review), `job_state` (watermarks and anomaly counters), `chain_rest_snapshots` (kind `subnets`, ≤ 12 h old, for the chain fallback §2.3 and the TMC name).

### 3.3 Public API of the package (read API used by 05/06/07/08/09/11/12; domain functions called by 14 and 17)

```python
# app/registry/__init__.py re-exports these
async def current_occupant(db, netuid: int) -> Occupant | None
async def occupant_at(db, netuid: int, block: int) -> Occupant | None      # PIT: reg ≤ block < successor.reg
async def active_occupants(db, include_pre_launch: bool = False) -> list[Occupant]
async def resolved(db, occupant_key: str, field: str, as_of: int | datetime | None = None) -> Resolved | None
       # as_of normalised with parse_ts (B1; C5 producers pass their tz-aware T_eval).
       # Fast path: as_of None or ≥ identity_resolved.resolved_at → the stored row.
       # Replay (as_of < resolved_at): recompute R1–R6 from identity_observations with observed_at ≤ as_of and
       #   manual_overrides with created_at ≤ as_of < COALESCE(retired_at, ∞) (their current status is used —
       #   status transitions carry no timestamp); coverage at as_of = the latest `_coverage` observation
       #   (value '1'/'0', written per (occupant, source) through the hash-dedupe each ok run, so only changes and
       #   30-d heartbeats add rows). No forward-fill; before the first observation → None.
async def identity_epoch(db, occupant_key: str, t: int) -> int              # §2.7 R4
async def watch_targets(db, platform: str, only_active: bool = True) -> list[WatchChannel]
def normalise_x_handle(s: str | None) -> str | None                         # §2.6 (09 wraps it)
def canonical_website(url: str | None) -> str | None                        # §2.6 (07 uses it)

# domain functions (14 exposes routes/Telegram commands for them, A2; errors → 14 maps ValueError to 422)
async def compute_needs_review(db, now: int) -> list[ReviewItem]            # §2.10
async def add_override(db, *, occupant_key: str, field: str, mode: str, value: str | None = None,
                       suppress_source: str | None = None, suppress_value: str | None = None,
                       reason: str | None, evidence_url: str | None, resolves_event_id: int | None = None,
                       created_by: str) -> int      # force/suppress require reason + evidence_url; re-resolves the field
async def set_override_status(db, override_id: int, status: str, *, by: str) -> None   # active|rejected|retired
# legacy import (J1; 17 passes its OccupancyIndex, the seed script passes None)
class OccupancyLookup(Protocol):
    def occupant_at_ts(self, netuid: int, ts: int) -> str | None
async def import_fundamentals(db, path: str | Path, *, as_of: int, run_id: str,
                              occupancy: OccupancyLookup | None) -> LegacyReport   # app.registry.legacy, §2.11
```

### 3.4 Metrics produced
**None.** This spec writes no `metric_values` and registers no producer fns (C5 does not apply; B9's `registry` source is used for its events). It is the source of truth for these downstream inputs (owner spec computes them): `tenure` (11, raw days from `occupants.network_registered_at` and the head block; `reg_ts = COALESCE(registered_ts, registered_at_est)`, C1); `control_stability_90d` S3 identity-churn pairing and `owner_control_change_trigger.value_json.identity_churn` (11, from chain-sourced `identity_changed` in the D4 warning set — never `owner_coldkey`, C2); `owner_identity` (11, from `resolved()` of `owner_name` + `owner_url`/`owner_github`: `present` → 1.0, `absent`/`junk_only` → 0.0, `unknown` → None); the `stale_identity` gate (12 §2.8: `identity_changed(field='subnet_name', kind='changed')` or `identity_junk(abandonment_marker)`); P2 targets (06, `github_repo` candidates), P3 targets (07, `subnet_url`, `docs_url`), P5 targets (08/09, `watch_channels`). `identity_stability` is retired (C2). The absence/failure semantics those metrics need are exactly `identity_resolved.status` (§4).

### 3.5 Events emitted

All via `emit_event` with `events.source='registry'` (B9); severities `info|warning` only (B2); required payload keys and dedupe patterns are 03 §3.9's (D1/D2); routing is 14's per D4 (this spec never sends Telegram, D3).

| event_type | severity | route (03 alert column, D4) | payload (JSON) | dedupe_key |
|---|---|---|---|---|
| `netuid_reoccupied` | warning | immediate | `{netuid, old_occupant_key, new_occupant_key, old_network_registered_at, new_network_registered_at, old_name, new_name, old_status_before, old_last_seen_block, head_block, new_status}` (17's history rows use 03's backfill key set) | `new_occupant_key` |
| `occupant_activated` | info | stored only | `{netuid, occupant_key, first_emission_block, head_block, days_pre_launch}` | `occupant_key` |
| `identity_changed` | `warning` iff payload `source='chain'` and `field ∈ {subnet_name, github_repo, subnet_url, discord}`; else `info` (incl. `owner_coldkey`) | immediate if warning, else digest | `{netuid, occupant_key, source, field, kind: 'changed'\|'added'\|'removed', old_value, new_value, old_hash, new_hash, old_observed_at, via: 'rpc'\|'tmc_snapshot'\|null, resolved_changed: bool}` | `f"{occupant_key}|{source}|{field}|{new_hash}"` |
| `source_disagreement` | info | digest | `{netuid, occupant_key, field, groups: [{key, sources: [...], values: [...]}], resolved_source, pair_hash}` (never `field='github_repo'` from 04, R6) | `f"{occupant_key}|{field}|{pair_hash}"` |
| `identity_junk` | info; warning for `abandonment_marker`/`leaked_secret` | digest | `{netuid, occupant_key, source, field, value, junk_reason}` (`value` redacted for `leaked_secret`) | `f"{occupant_key}|{source}|{field}|{value_hash}"` |
| `registry_anomaly` | warning | immediate — the emitter itself applies 03's "3rd consecutive" | `{reason, netuid?, detail}`; `reason ∈ {reg_block_went_backwards, netuid_absent, chain_low_key_count, sanity_failed, acf_key_missing, source_failed, clock_skew}` | `f"{reason}|{netuid or ''}|{run_date}"`, `run_date = floor(run.started_at / 86400)` |

**3-consecutive gate.** For each `(reason, netuid)` the job keeps a counter in `job_state` (+1 on a run showing the condition, reset to 0 on a run without it); the row is emitted only when the counter reaches `REGISTRY_ANOMALY_CONSECUTIVE_RUNS` (3) and, while the condition persists, at most once per UTC day (dedupe). Per-netuid reasons set `occupant_key`/`netuid` of the current occupant; run-level reasons are system rows (`occupant_key` and `netuid` NULL). `source_failed` (`detail = {source, consecutive, last_error, last_ok_at}`) is how a single identity source that keeps failing (e.g. subnetalpha behind a Cloudflare challenge) reaches the operator; whole-job failures are 01's `collector_failing`/`collector_recovered`.

`event_at` (INTEGER) = `observed_at` for all registry events except `identity_changed` from subnetalpha (`event_at = parse_iso(modified_gmt + "Z")`) and `netuid_reoccupied` (`event_at = registered_at_est` of the new occupant; `payload.head_block` records when we noticed).

---

## 4. Absence-vs-failure table

The registry is a collector, so this is the binding table. "Covered" means `identity_coverage.has_row` exists for `(source, occupant)` from that source's last **ok** run (or the source structurally cannot cover the occupant, e.g. netuid 0 on subnetalpha, which is recorded as `has_row=0`).

| resolved field | **0.0-equivalent: `status='absent'` (verifiable absence)** | **None-equivalent: `status='unknown'` (collection failure)** | `junk_only` |
|---|---|---|---|
| `github_repo` | chain covered with `github_repo=''` **and** subnetalpha covered with `github_url=''` (or no record) **and** coldkey covered with `github_repo=''` (or owner has no identity) **and** no manual/legacy fallback | chain never covered this occupant **and** subnetalpha never covered **and** coldkey never covered (all three enabled sources have no ok run touching it) | every candidate is junk (SN39 `deprecated/deprecated` — the only such netuid in the 2026-09-22 dumps; SN3's `username/repo` exists only in the legacy file, which is not imported, and SN3's chain now says `unarbos/teutonic`) → downstream treats as **absent** (0.0), plus `identity_junk` |
| `subnet_url` | same pattern over chain `subnet_url`, subnetalpha `website_url`, coldkey `url` | same | same |
| `discord` | chain `discord=''` + subnetalpha `discord_url=''` + coldkey `discord=''` all covered; a bare handle counts as **present** (kind `discord_handle`) so 08 can still set `venue_reachable=0` itself | all sources uncovered | `"None"`, `"deprecated"`, `"pending..."` (69 chain rows today are empty or junk) |
| `x_url` | subnetalpha covered and `twitterx_url=''`; chain has no X field (structural), coldkey has none (structural) | subnetalpha never covered the occupant (and the occupant is not netuid 0) | junk token |
| `telegram` | subnetalpha covered and `telegram_url=''` | subnetalpha never covered | — |
| `docs_url` | no candidate anywhere while `subnet_url` is `present` or `absent` (derived; it inherits `unknown` only when `subnet_url` is unknown) | `subnet_url` unknown | — |
| `subnet_name` | chain, subnetalpha and tmc all covered and empty/`Unknown` (e.g. an unnamed not-started subnet) | no source ever covered the occupant | abandonment marker → still `present`, flagged; only-`Unknown` candidates → `junk_only` |
| `owner_name` / `owner_url` / `owner_github` | `owner_coldkey` known **and** coldkey-identities run ok **and** no row for that coldkey → `absent` (→ `owner_identity = 0.0`) | `owner_coldkey` unknown (`subnet_owner` missing from both `/subnets/` `latest_snapshot` and the table fallback) **or** the coldkey-identities call has never succeeded | `owner_name = Owner<n>` placeholder → `junk_only` (11 maps it to 0.0) |
| `youtube` | subnetalpha covered, all `video_url*` empty | subnetalpha never covered | non-YouTube video hosts → not junk, kind `website` |
| occupant `status` (T-rules) | a netuid absent for ≥ 3 sane runs → `deregistered` (verifiable) | an insane run → nothing changes; `/health.collectors.registry_occupants.last_error` set | — |

Rule of thumb enforced in code: **a source that failed this run contributes its last successful observation (with its own `observed_at`), never a synthesized empty.** A source that has never succeeded for an occupant contributes nothing and forces `unknown` only when no other source has a value.

---

## 5. Configuration

All keys on `app.config.Settings` (pydantic-settings, `UPPER_SNAKE`, `extra="ignore"`); consumers read `app.config.settings` dynamically. 01's inventory carries every key (F1); this spec's defaults win for its own keys. Keys marked *shared* are owned by 01/05 and only consumed here. Retired by this spec: `REGISTRY_INTERVAL_MIN` (01 placeholder), `REGISTRY_OCCUPANT_INTERVAL_MINUTES`, `REGISTRY_IDENTITY_CRON_UTC`, `REGISTRY_IDENTITY_INTERVAL_MINUTES` (no `SCHEDULER_MODE` exists), `REGISTRY_SUBNETALPHA_ENABLED`, `SUBNETALPHA_TIMEOUT_S`, `SUBTENSOR_RPC_TIMEOUT_S`, `TMC_RATE_LIMIT_PER_MIN`, `TMC_RATE_LIMIT_PER_HOUR`, `TMC_PAGE_SIZE`, `SUBNETALPHA_RATE_LIMIT_PER_MIN`, `SUBTENSOR_RPC_RATE_LIMIT_PER_MIN`, `REGISTRY_ALERT_AFTER_FAILURES` (01's `JOB_FAIL_ALERT_THRESHOLD` alerts on job failure).

| key | type | default | validator / semantics |
|---|---|---|---|
| `REGISTRY_ENABLED` | bool | `True` | enabled flag of both jobs (the wrapper re-checks it every run) |
| `REGISTRY_OCCUPANTS_INTERVAL_MIN` | int | `360` | `60 ≤ x ≤ 1440`; G2 says every 6 h — changing it changes 05's budget row |
| `REGISTRY_IDENTITY_AT_UTC` | str `HH:MM` | `"02:40"` | identity run time; must stay inside 17's quiet window `BACKFILL_TMC_QUIET_UTC` 02:05–02:45 and after 05's `TMC_DAILY_AT_UTC` batch |
| `REGISTRY_CHAIN_ENABLED` | bool | `True` | if False → chain source = TMC snapshot fallback only |
| `SUBTENSOR_RPC_URL` *(01 inventory, used only here)* | str | `"https://entrypoint-finney.opentensor.ai"` | must be `https://` (no `wss://`; no SDK) |
| `SUBTENSOR_RPC_FALLBACK_URLS` | list[str] | `[]` | tried in order, each for a whole read (§2.3); the archive host `https://archive.chain.opentensor.ai` is **UNVERIFIED** over HTTPS JSON-RPC — operator may add it |
| `SUBTENSOR_RPC_TIMEOUT_SEC` *(01 inventory, used only here)* | float | `15.0` | per call |
| `SUBTENSOR_RPC_RATE_PER_SEC` *(shared, 01)* | float | `2.0` | `subtensor_rpc` limiter (05's optional RPC collector shares it) |
| `REGISTRY_CHAIN_QUERY_CHUNK` | int | `100` | keys per `state_queryStorageAt`; > 100 **UNVERIFIED** on the public node |
| `REGISTRY_CHAIN_MIN_KEYS` | int | `90` | sanity gate; `< x` → chain run insane |
| `SUBNETALPHA_ENABLED` *(01 inventory)* | bool | `True` | subnetalpha source of the identity run |
| `SUBNETALPHA_BASE_URL` *(01 inventory)* | str | `"https://subnetalpha.ai/wp-json/wp/v2/subnet"` | |
| `SUBNETALPHA_RATE_PER_SEC` *(shared, 01)* | float | `1.0` | `subnetalpha` limiter (research: 1 req/s is polite) |
| `SUBNETALPHA_TIMEOUT_SEC` | float | `30` | |
| `REGISTRY_SUBNETALPHA_MAX_PAGES` | int | `5` | |
| `REGISTRY_SUBNETALPHA_MIN_RECORDS` | int | `100` | sanity gate |
| `REGISTRY_TMC_COLDKEY_ENABLED` | bool | `True` | coldkey-identities step |
| `TMC_BASE_URL`, `TMC_API_KEY`, `TMC_REQUEST_TIMEOUT_SEC`, `TMC_MAX_RETRIES` *(shared, 05)* | | | used through `TmcClient` only (G2) |
| `TMC_SUBNETS_PAGE_LIMIT` *(shared, 05)* | int | `200` | `/subnets/` page size (05's `fetch_subnets`) |
| `TMC_RATE_PER_MIN` / `TMC_RATE_BURST` / `TMC_HOURLY_CAP` / `TMC_MONTHLY_CAP` *(shared, F2)* | int | `8` / `2` / `500` / `90000` | 01's `ProviderLimiter['tmc']`; never read by this spec's code |
| `NETWORK_IMMUNITY_PERIOD_BLOCKS` *(shared, 05)* | int | `864000` | `subnets.immune` (§2.2) |
| `HTTP_USER_AGENT` *(shared, 01)* | str | `"TaoSentinel/0.1 (+https://github.com/<operator>/taosentinel)"` | set by 01's `Http` on every request |
| `BLOCK_SECONDS` | float | `12.0` | `registered_at_est`; also read by 11 |
| `DISCORD_MAIN_GUILD_ID` *(shared with 08)* | str | `"799672011265015819"` | verified main guild id (research 02-gapfill-discord-telegram) |
| `REGISTRY_MISSING_RUNS_TO_DEREG` | int | `3` | T5 |
| `REGISTRY_ANOMALY_CONSECUTIVE_RUNS` | int | `3` | `registry_anomaly` gate (§3.5, 03's "3rd consecutive") |
| `REGISTRY_REOBSERVE_DAYS` | int | `30` | heartbeat row cadence (also `_coverage` markers) |
| `REGISTRY_WATCH_RETIRE_DAYS` | int | `14` | §2.9 retirement sweep (08 relies on it) |
| `REGISTRY_JUNK_TOKENS` | list[str] | the regex alternatives from §2.6 | lower-cased, exact match after trim |
| `REGISTRY_STALE_SOURCE_DAYS` | int | `3` | source counted "stale" in the job's `/health` block |
| `LEGACY_FUNDAMENTALS_PATH` | str | `"seed/fundamentals.json"` | seed script |
| `LEGACY_EDGE_NOTES_DIR` | str | `"seed/subnet_edge"` | seed script |
| `LEGACY_AS_OF` | str ISO | `"2026-06-02T11:03:41Z"` | `generated_at` of the old snapshot (unix 1780398221), parsed with `parse_ts`; 11's `TEAM_LEGACY_AS_OF` defaults to it |

**Pause / kill semantics (B12).** Operator pauses are `control_state` rows (API, Telegram and CLI all write them) for the exact targets `registry_occupants`, `registry_identity`, or 16's `PAUSE_GROUPS` name `registry` (requested; never prefix matching); `data/control/<target>.paused` files are an emergency input the app reads and never writes. 01's wrapper checks `paused_reason()` before the run starts, so a paused job writes no `collector_runs` row; `/health.collectors.<id>.paused` uses B12's vocabulary (`operator | kill_switch | pause_all | budget | emergency_file`). `KILL_SWITCH` never pauses these jobs (`paid=False`, keyless sources); `PAUSE_ALL` and the integrity hold do (not `ops_exempt`). `resume` deletes only the `control_state` row. A TMC budget stop skips `registry_occupants` as `paused: budget` (its provider is `tmc`); `registry_identity` carries no job-level provider and only loses its coldkey step (§7). `REGISTRY_ENABLED=False` makes both jobs skip as `disabled`. `SCORING_KILLED` is irrelevant here.

---

## 6. Cadence, rate limits, cost

All requests go through 01's `Http.request(provider, …)` on `ProviderLimiter` (G1); TMC requests go through 05's `TmcClient` (G2). The TMC rows below are exactly 05 §6.3's "04 registry" row (G3).

| provider id | calls per run | runs/day | req/day | note |
|---|---|---|---|---|
| `tmc` (`registry_occupants`) | `/subnets/`: 1 page if the server honours `limit=200`, 2 at a cap of 100 (the budget case); `/subnets/table/`: 0, or 1 only when `/subnets/` failed or was insane | 4 | ≤ 12 | if the server caps `limit` at its default 25 (research saw only the default; **UNVERIFIED**), `/subnets/` costs 6 pages → 7/run, 28/day, and 05's row must be raised — the live network test (§9) settles it |
| `tmc` (`registry_identity`) | `/accounts/coldkey-identities/`: 1 (a bare list on 2026-09-22) | 1 | 1 (≤ 3 if it ever paginates) | 240 KB; at 02:40Z, inside 17's quiet window, after 05's 02:10Z batch ends (≈ 02:27Z) |
| **TMC total** | | | **≤ 15** (steady 5–9) | = 05 §6.3 "≤ 15 (≤ 12 + 1, + pages)"; ≤ 3/h per occupant run (4 in the one 02:00Z hour where an interval run may coincide with the identity call; 11's 20:30–23:30Z window sees ≤ 3) |
| `subtensor_rpc` | 5 (`getFinalizedHead`, `getHeader`, `getKeysPaged`, 2 × `queryStorageAt`) | 1 | 5 | < 2 s total per research; no key; limiter 2/s |
| `subnetalpha` | 2 (+1 when page 2 is exactly 100 long, answered by the past-the-end 400) | 1 | 2–3 | 1.3 MB + 0.5 MB; limiter 1/s |
| **total** | | | **≈ 12–23 req/day, $0.00** | seed script: one occupant run + one identity run (≈ 9 requests) |

HTTP behaviour is 01's (G1): the limiter honours `Retry-After` on 429 and `X-RateLimit-*`; 5xx/timeouts are retried by `Http` (`HTTP_MAX_RETRIES`, `TMC_MAX_RETRIES` = 2); 404/401/403 come back unretried and are never retried within a run; any status in `ok_statuses` returns without retry (subnetalpha's 400); `BudgetExhausted` at a cap. The registry never builds its own `httpx.AsyncClient`, bucket or retry loop.

---

## 7. Failure modes and fail-safe behaviour

**Run semantics (01 §2.6).** `RegistryOccupantsJob.collect(run)` and `RegistryIdentityJob.collect(run)` return a `JobOutcome(rows, note)` on success and raise on a whole-run failure; 01's wrapper records `collector_runs`, counts `consecutive_failures`, and fires `collector_failing`/`collector_recovered` through `AlertLatch` at `JOB_FAIL_ALERT_THRESHOLD` (D1, D3). The registry never calls `send_alert`. A whole-run failure is: occupants — both `/subnets/` and the table fallback failed or were insane; identity — all three sources failed, or no sane occupant run exists (`RegistryRunError('no_occupants')`). A single identity source failing is not a job failure: it is recorded in `meta_json.sources` and, after 3 consecutive runs, `registry_anomaly(source_failed)`.

| failure | behaviour | `/health` (job `status()` block) |
|---|---|---|
| TMC `/subnets/` down / 429 exhausted / insane | table fallback; if that fails too: no occupant mutation, last-good `subnets` stands, raise (job failure); the identity run still runs against existing occupants (≤ 12 h old, or it syncs inline) | `last_error`, `consecutive_failures` |
| TMC `BudgetExhausted` | occupants: propagates → `paused: budget` (not a failure, G1); identity: caught in the coldkey step only → `sources.tmc_coldkey = {ok:false, error:'budget'}` | `paused` / `sources` |
| TMC page shape changed (no coercible `network_registered_at`) | sanity fail as above + `registry_anomaly(sanity_failed)` after 3 runs | |
| chain RPC unreachable / bad JSON / low key count | chain source = TMC snapshot fallback for this run (`via='tmc_snapshot'`); if the snapshot is also unavailable, chain contributes nothing and prior chain observations remain the belief | `sources.chain = {ok:false, via:'tmc_snapshot'|null, keys, block_number}` |
| per-netuid SCALE decode error | that netuid: `identity_coverage.consecutive_misses += 1`, raw hex stored; others proceed | `chain_decode_errors` |
| subnetalpha 5xx / Cloudflare challenge / non-JSON / < 100 records | source skipped this run; prior observations stand; no `removed` events | `sources.subnetalpha.ok=false` |
| subnetalpha ACF key renamed (e.g. `twitterx_url` disappears) | if a mapped ACF key is missing from **every** record, the key is *not covered* this run (no observations for that field); `registry_anomaly(acf_key_missing)` after 3 runs | |
| coldkey-identities down | owner fields keep last observation; `owner_*` status never flips to `absent` on a failed run | `sources.tmc_coldkey.ok` |
| DB locked / write error | the transaction rolls back (occupant transitions + their events are one transaction; identity writes are one transaction per source); the error propagates → job failure | |
| clock skew (`run.started_at` < previous run's) | run proceeds; `registry_anomaly(clock_skew)` after 3 runs | |
| `registry_identity` before any sane occupant run | `ensure_occupants_fresh()` runs the occupant sync inline (same code, counted in this run's `meta_json`); if that fails → `RegistryRunError('no_occupants')` | |

**Alerting** (D3/D4; no direct Telegram from this spec): `netuid_reoccupied` and warning-level `identity_changed` (chain × {subnet_name, github_repo, subnet_url, discord}) are immediate through 14's dispatcher; `registry_anomaly` is immediate once its 3-consecutive gate lets it through; `identity_changed` (info, incl. `owner_coldkey`), `source_disagreement` and `identity_junk` go to the digest; job failure/recovery pages are 01's `collector_failing`/`collector_recovered`.

**Job `status()` extras** (01 builds `/health`, G6; ISO strings at this boundary only):
```json
"registry_occupants": {"last_run":"…","last_ok":"…","last_error":null,"consecutive_failures":0,"rows":129,
                       "head_block":9123662,"active":126,"pre_launch":3,"deregistered":0,"fallback_used":false,"paused":null},
"registry_identity":  {"last_run":"…","last_ok":"…","last_error":null,"consecutive_failures":0,"rows":1104,
                       "sources":{"chain":{"ok":true,"via":"rpc","keys":121,"block_number":9123650,"decode_errors":0,"last_ok":"…"},
                                  "subnetalpha":{"ok":true,"records":128,"pages":2,"last_ok":"…"},
                                  "tmc_coldkey":{"ok":true,"rows":1088,"matched_owners":86,"last_ok":"…"}},
                       "stale_sources":[], "events_emitted":{"identity_changed":3,"source_disagreement":1,"identity_junk":0},
                       "needs_review_count":37, "paused":null}
```
Liveness is 16's rule (E4): a job is stale only when enabled, unpaused and `now − last_run` exceeds its max-age (§2.1 table: 43,920 s and 174,720 s).

---

## 8. Implementation plan

### 8.1 Files

```
app/registry/
  __init__.py          re-exports the §3.3 API
  __main__.py          CLI: python -m app.registry review [--reason R] [--json]
  keys.py              occupant_key(netuid, reg) / parse_occupant_key(key) -> (netuid, reg)
  models.py            dataclasses: Occupant, OccupantInput, Candidate, Ref, Resolved, ReviewItem, WatchChannel,
                       SourceRunResult, LegacyReport; errors RegistryRunError, RpcError, ScaleError
  occupants.py         OccupantTracker (T1–T6), subnets row writer, ensure_occupants_fresh()
  sources/tmc.py       thin wrappers over 05's TmcClient + pure parsers (no HTTP of its own, G2)
  sources/chain_rpc.py SubtensorRpc, twox128(), PREFIX_HEX, compact_len(), decode_identity_v3(), netuid_from_key()
  sources/subnetalpha.py fetch_subnetalpha(), parse_netuid(), ACF_FIELD_MAP
  normalise.py         classify(), normalise_x_handle(), canonical_website(), normalise_github/discord/telegram/youtube(),
                       is_junk(), extract_docs(), compare_key()
  observations.py      ObservationWriter (hash-dedupe, identity_changed, identity_junk, coverage + _coverage, raw snapshots)
  merge.py             Merger.resolve(), pick(), disagreement(), identity_epoch(), subnets resolved-column writer
  overrides.py         add_override(), set_override_status()
  watch.py             WatchChannelsBuilder.rebuild(), retire_stale()
  review.py            compute_needs_review()
  legacy.py            import_fundamentals(), import_edge_notes(), OccupancyLookup, LegacyReport
  anomalies.py         consecutive-run gate for registry_anomaly (job_state counters)
  jobs.py              RegistryOccupantsJob, RegistryIdentityJob (01 CollectorJob subclasses)
app/util/fs.py         write_atomic (ported, §8.4)
scripts/seed_registry.py
tests/registry/…  (see §9)
```
No HTTP routes here: 14 owns routes and Telegram commands and calls the §3.3 domain functions (A2).

### 8.2 Signatures

```python
# keys.py
def occupant_key(netuid: int, network_registered_at: int) -> str            # f"{netuid}:{network_registered_at}"
def parse_occupant_key(key: str) -> tuple[int, int]

# sources/tmc.py   (tmc = TmcClient(run.http, lambda: app_config.settings), 05 §8)
async def fetch_occupant_inputs(tmc: TmcClient) -> tuple[list[OccupantInput], TmcResponse, str]
    # '/subnets/' first, '/subnets/table/' on failure/insanity; returns (inputs, resp, kind 'subnets'|'subnets_table')
def subnets_row_to_input(row: dict) -> OccupantInput | None   # §2.2 table: int() coercion of the snapshot strings,
                                                               # name = subnet_identities_v3.subnetName, symbol = token_symbol
def table_row_to_input(row: dict) -> OccupantInput | None     # 'subnet' → netuid; first_emission_block None
def parse_coldkey_identities(resp: TmcResponse) -> dict[str, dict]   # bare list or {results}; keyed by coldkey
# OccupantInput(netuid, network_registered_at, name, symbol, is_active, first_emission_block, owner_coldkey,
#               owner_hotkey, registered_at_ts, block_number, identities_v3: dict | None)

# sources/chain_rpc.py
PREFIX_HEX = "0x658faa385070e074c85bf6b568cf0555c7cb9786b286b680ca204a6c0920ee52"
IDENTITY_V3_FIELDS = ("subnet_name","github_repo","subnet_contact","subnet_url","discord","description","logo_url","additional")
def twox128(s: str) -> bytes
def compact_len(b: bytes, i: int) -> tuple[int, int]
def decode_identity_v3(value_hex: str) -> dict[str, str]
def netuid_from_key(key_hex: str) -> int                                     # ValueError on bad length/prefix
class SubtensorRpc:
    def __init__(self, urls: list[str], http: Http, timeout_s: float): ...  # provider 'subtensor_rpc'
    async def call(self, url: str, method: str, params: list) -> Any        # RpcError on a JSON-RPC error member
    async def read_subnet_identities_v3(self, chunk: int = 100) -> ChainReadResult   # whole read per URL, §2.3
        # ChainReadResult(block_number: int, block_hash: str, url: str, rows: dict[int, dict[str,str]],
        #                 raw: dict[int, str], decode_errors: dict[int, str], bad_keys: int, n_keys: int)

# sources/subnetalpha.py
def parse_netuid(subnet_number: str | None) -> int | None                    # port of fundamentals_collect.py:62-70
async def fetch_subnetalpha(http: Http, *, max_pages: int) -> SubnetalphaResult
    # SubnetalphaResult(records: dict[int, SaRecord], pages: int, wp_total: int | None,
    #                   missing_acf_keys: set[str], duplicate_netuids: list[int])
    # SaRecord(netuid, wp_id, slug, name, modified_ts: int, link, acf: dict)

# normalise.py
@dataclass(frozen=True)
class Ref: kind: str; canonical: str; handle: str | None; extra: dict; is_junk: bool; junk_reason: str | None; reclassified_from: str | None
def classify(raw: str, expected_field: str) -> list[Ref]                     # [] for empty; first = primary (§2.6)
def normalise_x_handle(s: str | None) -> str | None                          # §2.6 (09 wraps it)
def canonical_website(url: str | None) -> str | None                         # §2.6 (07 uses it)
def is_junk(raw: str, field: str) -> str | None                              # junk_reason or None
def extract_docs(refs: list[Ref], text: str) -> Ref | None
def compare_key(ref: Ref) -> str                                             # R6 grouping key

# observations.py   (all instants int unix s, B1)
class ObservationWriter:
    def __init__(self, db, run_id: str, observed_at: int): ...               # events via 03 emit_event(source='registry')
    async def write(self, occ: Occupant, source: str, field: str, ref: Ref | None, raw: str,
                    *, event_at: int | None, block_number: int | None, source_ref: str | None,
                    suspect: bool = False, via: str | None = None) -> bool   # True = row inserted
    async def mark_coverage(self, source: str, occ: Occupant, has_row: bool, block_number: int | None) -> None
        # upserts identity_coverage AND writes the '_coverage' observation ('1'/'0') through write()
    async def write_raw(self, source: str, scope: str, occ: Occupant | None, payload: dict, *,
                        source_ref: str | None, event_at: int | None, block_number: int | None) -> bool

# merge.py
class Merger:
    def __init__(self, db, run_id: str, now: int): ...
    async def resolve(self, occ: Occupant, field: str, *, as_of: int | None = None) -> Resolved
    async def resolve_all(self, occ: Occupant) -> dict[str, Resolved]       # writes identity_resolved + subnets columns
def pick(cands: list[Candidate], overrides: list[Override]) -> tuple[Candidate | None, list[str]]   # winner, flags
def disagreement(field: str, cands: list[Candidate]) -> Disagreement | None  # None for github_repo (R6)
async def identity_epoch(db, occupant_key: str, t: int) -> int

# occupants.py
class OccupantTracker:
    def __init__(self, db, run_id: str, observed_at: int): ...
    async def sync(self, inputs: list[OccupantInput], head_block: int) -> SyncResult
        # SyncResult(inserted, activated, reoccupied, anomalies, missing, sane: bool, error: str | None)
async def ensure_occupants_fresh(db, run: RunContext, max_age_h: float = 12) -> bool

# jobs.py (factories in 01's JOB_TABLE, §2.1)
class RegistryOccupantsJob(CollectorJob):
    id = "registry_occupants"; phase = 1; enabled_flag = "REGISTRY_ENABLED"
    interval_flag = "REGISTRY_OCCUPANTS_INTERVAL_MIN"; provider = "tmc"; paid = False
    timeout_sec = 600; misfire_grace_sec = 3600
    async def collect(self, run: RunContext) -> JobOutcome                   # raises RegistryRunError on a failed run
class RegistryIdentityJob(CollectorJob):
    id = "registry_identity"; phase = 1; enabled_flag = "REGISTRY_ENABLED"; interval_flag = None
    cron = {"hour": 2, "minute": 40}   # from REGISTRY_IDENTITY_AT_UTC
    provider = None; paid = False; timeout_sec = 1800; misfire_grace_sec = 21600; min_gap_factor = 0
    startup_catchup = True
    async def collect(self, run: RunContext) -> JobOutcome
```

### 8.3 Step order (identity run)

1. `run.run_id` (uuid4 hex from 01's wrapper), `observed_at = int(run.started_at.timestamp())`.
2. `ensure_occupants_fresh()`; load `active_occupants(include_pre_launch=True)` and the current `subnets` map (netuid → occupant).
3. Chain: `read_subnet_identities_v3()`; sanity; per netuid → occupant via `subnets`; for each of the 8 fields: `classify(raw, field)` → `writer.write(...)` (primary + secondaries, §2.6); `mark_coverage('chain', occ, has_row)` for **all** current occupants (has_row=0 for netuids without a key). On failure → TMC snapshot fallback from `latest_rest_snapshot(db, 'subnets', max_age_s=43200)` else skip.
4. subnetalpha: `fetch_subnetalpha()`; sanity; `missing_acf_keys` guard; per record → netuid → occupant; `write_raw` on ACF hash change; `slug` + per mapped field `write(...)` with `event_at = modified_ts`; coverage for every occupant (`has_row=0` for those with no record, including netuid 0).
5. TMC: `tmc.fetch_coldkey_identities()` → `write_rest_snapshot(db, 'coldkey_identities', …)`; for each occupant with `owner_coldkey`: `write('tmc','owner_coldkey',…)`, `write('tmc','subnet_name',…)` (TMC name), then the 7 `owner_*` fields (`has_row = coldkey in identities`).
6. Merge: for each occupant `resolve_all()` → `identity_resolved`, `subnets` resolved columns; `source_disagreement` events.
7. `WatchChannelsBuilder.rebuild()` per occupant; `retire_stale()`.
8. Anomaly gate update (`source_failed`, `chain_low_key_count`, `acf_key_missing`, `clock_skew`); `UPDATE collector_runs SET meta_json=… WHERE run_id=run.run_id`; return `JobOutcome(rows=observations written, note=…)`.

Steps 3–5 are each wrapped individually (each its own transaction); one source failing never skips the others or the merge. The occupant run is: fetch (+ fallback) → sanity → `OccupantTracker.sync()` in one transaction (occupants, subnets, events) → `write_rest_snapshot` → anomaly gate → `job_state` watermarks.

### 8.4 What to port verbatim vs rewrite

| old code (all line ranges re-verified 2026-09-23) | action |
|---|---|
| `fundamentals_collect.py:62-70` `_NETUID_RE`, `_parse_netuid` | port verbatim as `parse_netuid` |
| `fundamentals_collect.py:73-111` `fetch_subnetalpha` | port the loop structure (page range, 400-break, empty-break, `< 100`-break, `_fields` param); replace `print` with `logger.warning`, extend `_fields`, use 01's `Http` (`ok_statuses=(200, 400)`) instead of `client.get` + `raise_for_status`, return `SaRecord` objects with the full `acf` dict instead of the 11-key projection, add sanity + duplicate-netuid rule |
| `fundamentals_collect.py:226-231` `_pick` | semantics only → `merge.pick()` over classified candidates |
| `fundamentals_collect.py:234-319` `build_dataset` | **do not port**; the universe rule `set(alpha) | set(onchain)` is replaced by the TMC occupant universe, and the netuid-keyed record shape by `identity_resolved`. Keep its `sources` bookkeeping idea as `candidates_json`. |
| `fundamentals_collect.py:388-393` `write_atomic` | port verbatim into `app/util/fs.py` (seed report; 11 reuses it) |
| `fundamentals_collect.py:117-158` `fetch_onchain` (bittensor SDK) | **do not port** (no SDK); replaced by `chain_rpc.py` |
| `fundamentals_collect.py:161-220` `parse_repo`, `fetch_github` | **do not port** (defective: `_GH_RE` needs `/{o}/{r}` and line 175 rejects owner `orgs`, so org-only and `/orgs/…/repositories` URLs return None; 06 owns GitHub) — `normalise_github` re-implements the URL forms from research 02-gapfill-github §7.1 |
| `chain_client.py:711-765` `normalise_dynamic_info` | port only the `network_registered_at` int-coercion idiom (738-742) into `subnets_row_to_input`; the comment at 734-735 (slot-identity marker that changes on reuse) is the T3 rule |

---

## 9. Tests

`tests/registry/`, pytest + `pytest-asyncio`, in-memory aiosqlite with the 03 DDL, `respx` for httpx mocking (behind 01's `Http` with a no-op limiter), no network. Fixture values are the real 2026-09-22 rows (research dumps `chain_identities_v3.json`, `tmc_table.json`, `subnets_51.json`, `coldkey_identities.json`, `cpt1/2.json`) unless marked *synthetic*. Fixtures in `tests/fixtures/registry/`:

| fixture | content |
|---|---|
| `tmc_subnets_run1.json` | `/subnets/` `{count, results}` shape with the verified row layout (§2.2: strings in `latest_snapshot`, identities in `subnet_identities_v3`), 9 rows: netuid 0 (root, reg `'0'`), 27 `Nodexo` reg `'1727132'` owner ck A, 40 `Chunking` reg `'3372582'`, 64 `Chutes` reg `'4531295'`, 39 `deprecated` reg `'3280104'`, 106 `Liquidity Provisioning` reg `'5558480'`, 112 `for sale` reg `'5633022'`, 117 `everyframe.studio` reg `'5710859'`, 51 copied from `subnets_51.json`; 108 absent; `block_number` 8_400_000 (*synthetic head*) |
| `tmc_subnets_run2.json` | 27 → `Orion`, **same reg** 1727132, owner ck B (same-registration owner change); 40 → `Ralph` **reg `'8409860'`**, `is_active=false`, `first_emission_block_number=null` (*synthetic pre-launch state*); 106 → `Nodexo` (same reg); 108 reg `'9111253'` pre_launch; `block_number` 9_123_662 |
| `tmc_subnets_run3.json` | run2 with 40 `is_active=true`, `first_emission_block_number='8420000'` |
| `tmc_subnets_insane.json` | 12 rows (sanity fail) |
| `tmc_subnets_table.json` | 129-row `/subnets/table/` subset copied from `tmc_table.json` (fallback path) |
| `chain_keys_run1.json` / `chain_values_run1.json` | RPC responses built by `tests/registry/gen_chain_fixture.py` from a dict of the 8 fields (real rows for 27 Orion incl. the `?token=GHSAT…` logo, 106 Nodexo incl. the deep-link contact + `Docs:` additional, 64 with `github_repo="https://github.com/rayonlabs/chutes"` in run1 (*synthetic*) and the real `chutesai/chutes` in run2, 39 all-`deprecated`, 112 `for sale`, 117 dead repo, 51 `subnet_url="lium.io"`, `discord="p383_54249"`, 40 Ralph); keys built with the real prefix + Twox64Concat; one malformed value (trailing bytes) for netuid 117 in run2 |
| `subnetalpha_page1.json`, `subnetalpha_page2.json` | 3 + 3 records with all 32 ACF keys: "Subnet 64" chutes (real: `github_url=https://github.com/rayonlabs/chutes`, `discord_url=https://discord.gg/bittensor`, `twitterx_url=https://x.com/chutes_ai`, `modified_gmt=2025-10-16T08:58:23`, plus *synthetic* `video_url=https://www.youtube.com/watch?v=abc123DEF45`), "Subnet 27" *synthetic* stale Nodexo post (`modified_gmt=2025-10-01T00:00:00`, `github_url=https://github.com/neuralinternet/SN27`, `twitterx_url=https://x.com/nodex0_`), "Subnet 40" *synthetic* stale Chunking post (`modified_gmt=2025-10-01T00:00:00`, `github_url=https://github.com/Inference-xyz`), "Subnet 106" Nodexo (real: `discord_url=https://discord.gg/t7BMee4w`), "Subnet 3" placeholders (`Awaiting Data...`, `www.deprecated.com`), "Subnet 117" `glyph-research/glyph-subnet`; page 3 returns HTTP 400 |
| `tmc_coldkey_identities.json` | bare list, 4 rows: SN64 owner (`name`, `url`, `github_repo`), SN106 owner (real `Nodexo` row), an `Owner51` placeholder, SN27 owner B `Orion` with `' '` values; none for owner ck A |
| `fundamentals_legacy.json` | schema 2 subset (real values): 27 Nodexo `neuralinternet/SN27`, 40 Chunking `Inference-xyz`, 64 Chutes `rayonlabs/chutes`, 100 Platform `CortexLM/platform?tab=readme-ov-file`, 3 `username/repo` + `www.deprecated.com`, 117 `~` |
| `subnet_edge/sn64_chutes.md` | real file copy |

Unit tests (name → assertion):

1. `test_twox128_prefix_constant` — `twox128("SubtensorModule").hex() == "658faa385070e074c85bf6b568cf0555"`, `twox128("SubnetIdentitiesV3").hex() == "c7cb9786b286b680ca204a6c0920ee52"`, `PREFIX_HEX` equals §2.3.
2. `test_key_suffix_netuid` — `netuid_from_key(PREFIX_HEX + "62cf9476b03dd865" + "4000") == 64`; wrong length or prefix raises `ValueError`.
3. `test_scale_decode_roundtrip` — the real SN64 row re-encoded (`0x1843687574657388…`, `additional` compact `0x3502`) and SN27 (`0x9901`, `0x1902`) decode exactly; an empty field and a 300-byte field round-trip; trailing bytes → `ScaleError`; overrun → `ScaleError`.
4. `test_chain_read_pins_block` — respx: 5 calls (2 `queryStorageAt` chunks of 100 for 121 keys), `state_getKeysPaged` and `state_queryStorageAt` receive the hash from `chain_getFinalizedHead`; a `ProviderError` on the primary restarts steps 1–4 on the fallback URL (no mixed nodes).
5. `test_chain_sanity_low_keys_no_writes` — 40 keys → chain source not ok, zero `identity_observations` rows with `source='chain'`; `registry_anomaly(chain_low_key_count)` only on the 3rd consecutive such run, exactly once.
6. `test_chain_fallback_tmc_snapshot` — RPC 503 ×3 → observations written with `source_ref='tmc_snapshot'` from the stored `subnets` snapshot, `via='tmc_snapshot'` on events; a netuid whose dict lacks `logoUrl` writes no `logo_url` observation (no `removed` event).
7. `test_subnetalpha_page_loop_400` — 2 pages, page 3 = 400 (returned via `ok_statuses`, not retried) → 6 records, `pages == 2`; `event_at == parse_iso("2025-10-16T08:58:23Z")`; `parse_netuid("Subnet 07") == 7`; `"Subnet-85"` → 85; `None` → None; two records for one netuid → newest kept, `duplicate_netuids` reported.
8. `test_subnetalpha_missing_acf_key_guard` — remove `twitterx_url` from every record → no `x_url` observations; `registry_anomaly(acf_key_missing)` on the 3rd consecutive run.
9. `test_occupant_first_run_no_events` — run1 on empty DB → 9 occupants (strings coerced to int, `name` from `subnetName`, `symbol` from `token_symbol`, `owner_hotkey` set), 0 events, `chain_rest_snapshots` has one `endpoint='/subnets/'` row; `subnets[0].immune == 0`, `subnets[40].immune == 0` at head 8_400_000.
10. `test_reoccupancy_40_chunking_to_ralph` — run1 then run2: `occupants["40:3372582"].status == "deregistered"`, `dereg_reason == "reoccupied"`, `last_seen_block == 8_400_000`, `successor_key == "40:8409860"`; `occupants["40:8409860"].status == "pre_launch"`, `predecessor_key` set, `registered_ts IS NULL`; `subnets[40].occupant_key == "40:8409860"`; exactly one `netuid_reoccupied` (`severity='warning'`, `events.source='registry'`) with `old_name="Chunking", new_name="Ralph"`; `occupant_at(40, 8_400_000)` returns the old occupant and `occupant_at(40, 8_420_000)` the new one.
11. `test_same_registration_owner_change_27` — run1 then run2 (identity runs after each): **no** `netuid_reoccupied` for 27 or 106; `occupant_key` stays `27:1727132`; chain `identity_changed(field='subnet_name')` `Nodexo → Orion` with severity `warning`; `identity_changed(source='tmc', field='owner_coldkey')` with severity `info`; 106 keeps `106:5558480` with `subnet_name` changed to `Nodexo`.
12. `test_prelaunch_activation` — run3 after run2 → `40:8409860` becomes `active`, `first_emission_block == 8_420_000`, one `occupant_activated`; a table-fallback run activates on `is_active` alone and back-fills `first_emission_block` at the next `/subnets/` run.
13. `test_reg_block_backwards_anomaly` — run2 then run1 → no mutation; `registry_anomaly(reg_block_went_backwards)` for netuid 40 only after the 3rd consecutive such run.
14. `test_missing_netuid_three_runs` — remove 39 from three consecutive sane runs → `deregistered`, `dereg_reason='absent'`, one `registry_anomaly(netuid_absent)` at the 3rd run; two runs → still active with `missing_runs == 2` and no event.
15. `test_insane_run_no_mutation` — `tmc_subnets_insane.json` with the table fallback also failing → `collect()` raises `RegistryRunError`, the wrapper records `ok=0`, tables and `chain_rest_snapshots` unchanged; with the table fallback working → sane run from `tmc_subnets_table.json`, snapshot kind `subnets_table`.
16. `test_rename_chutes_and_disagreements` — chain run1 `rayonlabs/chutes`, run2 `chutesai/chutes`; subnetalpha stays `rayonlabs/chutes` → one `identity_changed` (`source='chain'`, `field='github_repo'`, severity `warning`, `old_value` rayonlabs, `resolved_changed=True`); **no** `source_disagreement` for `github_repo` from 04 (06's, by repo id), `flags_json` contains `github_string_mismatch`; `identity_resolved[64,'github_repo'].value_norm == "https://github.com/chutesai/chutes"`; SN106 chain `fwjBwxUcdX` vs subnetalpha `t7BMee4w` → one `source_disagreement(field='discord')`, `resolved_source='chain'`; SN64 subnetalpha main vanity vs chain → no disagreement; a third identical run emits **no** new events (dedupe).
17. `test_stale_editorial_demoted` — (a) subnetalpha "Subnet 40" post (`modified_gmt` 2025-10) vs occupant `40:8409860` → `stale_for_occupant`, resolved `github_repo` = chain `RalphLabsAI/ralph`; (b) "Subnet 27" post (2025-10) vs `27:1727132` after run2's chain `subnet_name` change → demoted via `identity_epoch`; its `x_url` (subnetalpha-only field) still wins but `flags` contains `stale_editorial`.
18. `test_junk_detection` — `"None"`, `"deprecated"`, `"pending..."`, `"Awaiting Data..."`, `"~"`, `https://github.com/username/repo`, `https://github.com/deprecated/deprecated`, `www.deprecated.com`, `https://deprecated.png`, `deprecated@gmail.com`, owner name `Owner51` → `is_junk=1`, reasons as specified; `identity_junk` emitted once per value; `subnet_name="for sale"` → `is_junk=0`, resolved `present` **and** `identity_junk(abandonment_marker)` with severity `warning`.
19. `test_leaked_token_flag` — SN27 `logo_url` with `?token=GHSAT…` → `is_junk=0`, `value`/`value_norm` redacted (`?token=<redacted>`), `value_hash` over the raw string, `junk_reason='leaked_secret'`, event severity `warning`, payload value redacted.
20. `test_normalise_forms` — GitHub table-driven: `.git` (SN47), trailing `/` (SN20, SN126), `?tab=readme-ov-file` (SN6, SN13, SN100), `/tree/main/x`, `github.com/orgs/Beam-Network/repositories` → owner `Beam-Network`, `github.com/BitMind-AI` → owner, `raw.githubusercontent.com/...` → website, case-insensitive `compare_key`; `normalise_x_handle`: the §2.6 X cases plus `None`/`""` → None; website: `lium.io` → `https://lium.io`, `canonical_website("https://Example.org/Path/?utm_source=x")` → `https://example.org/Path`; Discord: `discord.com/invite/bittensor` → `is_main_vanity`; `discord.com/channels/799672011265015819/1174835090539433994` → `discord_channel`, `is_main_guild`; `@tl_arrash`, `qbittensorlabs`, `CreativeBuilds` (→ `creativebuilds`), `p383_54249` → `discord_handle`; Telegram `t.me/+ug7C1J7Apf8wNzc1` → `telegram_private`, `t.me/s/NATIXNetwork` → public `NATIXNetwork`; YouTube `youtube.com/channel/UC…` → channel, `youtu.be/abc123DEF45` → video `needs_resolution`.
21. `test_reclassify_and_extras` — SN106 `subnet_contact` deep link → the `subnet_contact` row keeps `ref_kind='discord_channel'` and a `discord__extra_0` candidate with `reclassified_from='subnet_contact'`; the chain `discord` primary stays `fwjBwxUcdX` and a second identical run inserts no new row and no event (no flip-flop); `additional` → `docs_url` = `https://nodexo.ai/docs`, `subnet_url__extra_0 = https://nodexo.ai/api`; extras emit no `identity_changed`.
22. `test_absence_vs_unknown` — occupant with chain `github_repo=''`, subnetalpha covered & empty, coldkey covered & empty → `status='absent'`; same with subnetalpha never run → `unknown`; after one subnetalpha run with no record for that netuid (`has_row=0`) → `absent`. Netuid 0 with no subnetalpha record → `absent` (structural). Owner with only `Owner51` → `owner_name` `junk_only`.
23. `test_source_failure_keeps_last_good` — run A writes chain values; run B chain fails and fallback unavailable → `identity_resolved` unchanged, `source_observed_at` = run A, no `removed` events; three such runs → one `registry_anomaly(source_failed, detail.source='chain')`; the job itself stays `ok=1`.
24. `test_manual_override_modes` — `force` beats chain; `suppress` of SN117's dead chain repo lets subnetalpha `glyph-research/glyph-subnet` win; `fallback` only fills an otherwise-absent field; `add_override(mode='force')` without `evidence_url` raises `ValueError` (14 maps it to 422).
25. `test_watch_channels_population` — from the fixtures: `discord_invite` `bittensor` (`is_main_guild=1`, priority 20) and `fwjBwxUcdX` (priority 10, `meta_json.declared_by=['chain','tmc_coldkey']`), `discord_channel` `1174835090539433994` (guild set), `x` `chutes_ai`, `youtube_video` `abc123DEF45` (`needs_resolution=1`), `website`, `docs`; a 09-style update (`meta_json.channel_id`, `verified_at`, `needs_resolution=0`) survives the next rebuild; second run with the X handle removed from every source → row still present; `last_seen_at` aged 15 d → `retired_at` set, never deleted; a `web_autodiscover` row aged 15 d is not retired.
26. `test_seed_legacy_guards` — `import_fundamentals(..., occupancy=None)`: legacy 40 Chunking skipped (`skipped_reoccupied`, reg 8,409,860 > as_of), 27 Nodexo imported with `status='needs_review'` (same reg, name ≠ Orion), 64 imported `active` as `mode='fallback', source='legacy', source_ref='fundamentals.json@2026-06-02'`, 100 Platform vs resolved `Cortex` → `needs_review`, SN3 `username/repo`/`www.deprecated.com` and SN117 `~` counted as junk, not imported; legacy `identity_observations.observed_at == 1780398221`; a re-run inserts nothing. With a fake `OccupancyLookup` returning `40:3372582` for netuid 40 → observations land on the old occupant, no overrides.
27. `test_seed_edge_note_urls` — `sn64_chutes.md` → `https://chutes.ai` website and `rayonlabs/chutes` GitHub fallback rows with `observed_at` = the edge as_of; `subnetalpha.ai` links ignored; verdict stored in `manual_notes(kind='seed_edge')`; `README.md`, `_*.md` and the `synth_backtest/` directory skipped.
28. `test_needs_review_reasons` — after run1 (first sane run) + run2 + seeds: contains `new_occupant` (40:8409860, 108 — not the 9 run1 occupants), `reoccupied_unreviewed` (40), `source_disagreement_open` (106 discord; **not** 64 — `rayonlabs/chutes` → `chutesai/chutes` is a same-id rename, id 884216433, for which 06 emits `repo_renamed` and no disagreement, and 04 emits no `github_repo` disagreement at all), `abandonment_marker` (39, 112), `name_turnover` (117), `legacy_needs_review` (27, 100), `chain_identity_missing` not for 108 (pre_launch); a `manual_notes` row with `ref_event_id` = the reoccupied event clears `reoccupied_unreviewed`.
29. `test_pit_resolved_as_of` — `resolved(db, "64:4531295", "github_repo", as_of=<int between run1 and run2>)` returns `rayonlabs/chutes`; `as_of` before run1 returns `None`; `as_of` = a tz-aware `datetime` works (C5); owner absent via `_coverage` markers replays as `absent`, not `unknown`.
30. `test_run_semantics` — `/subnets/` and the table both raise `ProviderError` → `collect()` raises `RegistryRunError` (01 wrapper: `ok=0`, `consecutive_failures == 1`); `BudgetExhausted` in `registry_identity`'s coldkey step → run returns ok with `sources.tmc_coldkey.error == 'budget'`; `BudgetExhausted` in `registry_occupants` propagates; `grep -r send_alert app/registry` is empty (D3).
31. `test_rest_snapshots_and_budget` — one occupant run = exactly 1 `/subnets/` request (limit 200 honoured) and 0 table requests; one identity run = 1 coldkey request, 5 RPC calls, 2 subnetalpha calls (+ the 400); `chain_rest_snapshots` rows keyed `endpoint='/subnets/'` and `'/accounts/coldkey-identities/'`; an insane payload writes no snapshot.

Integration/smoke (marked `@pytest.mark.network`, skipped in CI; run once on the laptop): `test_live_chain_read` — real RPC returns ≥ 90 keys, decodes with 0 errors, SN64 `subnet_name == "Chutes"`; `test_live_subnetalpha_shape` — page 1 has `X-WP-TotalPages`, every record has all 32 ACF keys (fails loudly when the site renames a key); `test_live_tmc_subnets_fields` — `latest_snapshot` carries `network_registered_at`, `first_emission_block_number`, `subnet_owner`, `subnet_owner_hotkey`, `subnet_identities_v3` with 8 keys, and prints the page size actually honoured for `limit=200` (settles §6's UNVERIFIED page cap).

Mocked always: httpx (respx), clock (`freezegun` or an injected `now()`), the limiter (no-op in tests).

---

## 10. Verify checklist (Phase 1 done when all pass on the laptop)

Expected numbers are from the 2026-09-22 dumps; drift since then is normal.

```bash
cd ~/taosentinel && source .venv/bin/activate
pytest tests/registry -q                                  # expected: 31 passed, network tests skipped
pytest tests/registry -q -m network                       # once, on the laptop; note the honoured /subnets/ page size
python -m scripts.seed_registry --report data/seed_registry_report.json
jq '.occupants, .legacy, .needs_review_count' data/seed_registry_report.json
#   occupants: active ≈ 126, pre_launch ≈ 3 (SN59/86/108 on 2026-09-22), deregistered 0 on a fresh DB
#   legacy.skipped_reoccupied ≈ 14 (+ any slot re-registered since), legacy.needs_review ≥ 6 (27, 106, 20, 53, 119, 100 …),
#   legacy.imported ≥ 60, legacy.junk ≥ 3
sqlite3 data/taosentinel.db "select status, count(*) from occupants group by status;"
sqlite3 data/taosentinel.db "select occupant_key,name from subnets s join occupants using(occupant_key) where netuid in (27,106,64,39,40);"
#   27:1727132 Orion · 106:5558480 Nodexo · 64:4531295 Chutes · 39:3280104 deprecated · 40:8409860 Ralph
sqlite3 data/taosentinel.db "select source, count(*) from identity_observations group by source;"
#   chain ≥ 960 (121 netuids × 8 fields, empties included) · subnetalpha ≥ 1,900 · tmc_coldkey ≥ 500 · tmc ≥ 250 · legacy ≥ 200
sqlite3 data/taosentinel.db "select field,status,count(*) from identity_resolved group by 1,2 order by 1,2;"
#   github_repo present ≈ 117, absent ≈ 11, junk_only ≈ 1 (SN39), unknown = 0 (computed from the dumps)
sqlite3 data/taosentinel.db "select value_norm from identity_resolved where occupant_key like '64:%' and field='github_repo';"
#   https://github.com/chutesai/chutes
sqlite3 data/taosentinel.db "select endpoint, count(*) from chain_rest_snapshots group by 1;"
#   /subnets/ ≥ 1 · /accounts/coldkey-identities/ ≥ 1
sqlite3 data/taosentinel.db "select platform,count(*) from watch_channels where retired_at is null group by 1;"
#   discord_channel ≥ 18 · discord_invite ≥ 40 (incl. the bittensor vanity per subnet that lists it) · x ≥ 100 · telegram ≥ 6 · website ≥ 100 · docs ≥ 10 · youtube/youtube_video ≥ 50
sqlite3 data/taosentinel.db "select event_type,severity,source,count(*) from events group by 1,2,3;"
#   all source='registry'; no severity 'warn'; identity_junk ≥ 60 · abandonment_marker warning = 3 (SN39/73/112) · netuid_reoccupied = 0 on a fresh DB
curl -s localhost:8090/health | jq '.collectors.registry_identity.sources'
#   chain.ok true, via "rpc", keys ≥ 118 · subnetalpha.ok true, records 128 · tmc_coldkey.ok true, rows ≥ 1000
python -m app.registry review --json | jq 'group_by(.reason)|map({reason:.[0].reason,n:length})'
#   name_turnover ≈ 25 (UNVERIFIED, §2.10) · abandonment_marker 3 · chain_identity_missing ≈ 4 · legacy_needs_review ≥ 6
# Re-occupancy drill (no waiting for a real dereg):
python -m scripts.seed_registry --dry-run --simulate-reoccupy 40   # replays the T3 path against the live DB in a rolled-back txn;
#   prints: would close 40:8409860 (Ralph) → open 40:<reg+1> pre_launch, 1 netuid_reoccupied event
journalctl -u taosentinel --since "1 day ago" | grep -c '"job":"registry_'   # ≥ 5 (4 occupant + 1 identity)
```

---

## 11. Open questions / operator decisions

1. **Manual precedence.** Master §5 lists manual override *last* (lowest precedence). Implemented as the default `mode='fallback'`. The `force` and `suppress` modes are additions so the operator can beat a stale/dead owner-signed value (SN117's dead chain repo; 06's open question 1 asks for exactly this). Confirm this reading of "manual override" or move `manual(force)` into the master's precedence line.
2. **TMC `/subnets/` page cap** — field placement is now verified (§2.2); the maximum honoured `limit` is still **UNVERIFIED** (default 25). At a cap below 100 the registry spends up to 28 TMC requests/day instead of ≤ 12 and 05 §6.3's row must be raised. The network smoke test prints it.
3. **Coldkey-identities pagination** — resolved: a bare list on 2026-09-22; 05's pager covers a future `{count, results}` shape.
4. **Occupant cadence** — 6 h occupant sync means a re-registration is noticed up to 6 h late; 05's SSE table stream carries `network_registered_at` (a 44-field table column), so 05 could call `OccupantTracker.sync()` on such a delta rather than wait. Left to 05 (not required for v1).
5. **`registered_at_est` accuracy** — improved by TMC's row `registered_at` (§2.2, semantics UNVERIFIED); exact `registered_ts` comes from 17's backfill (J1) and only for occupants that existed then; later occupants keep the estimate.
6. **GitHub disagreements** — resolved: 04 emits none for `github_repo` (06 compares repo ids, D2). Before Phase 4 (06) they are visible only as `flags_json` `github_string_mismatch`.
7. **Email storage** — `subnet_contact` emails are on-chain public; stored in `identity_observations` but excluded from the UI/API by default. Confirm.
8. **subnetalpha maintainer consent** — 2 requests/day; research suggests one email to the obfuscated contact on the index page. Operator decision.
9. **Archive RPC fallback** — `SUBTENSOR_RPC_FALLBACK_URLS` defaults empty; the OnFinality endpoint (checklist B2) is an obvious keyed fallback once available: `https://bittensor-finney.api.onfinality.io/rpc?apikey=…` (**UNVERIFIED** path).
10. **Netuid 0 (root)** — an occupant like any other (`0:0`), never on subnetalpha, no identity; kept so 05 can key root rows; excluded from `identity_void` and `chain_identity_missing`.
11. **Same-registration owner changes** (SN27 Nodexo→Orion, SN106, SN20, SN53, SN119 since June) keep their occupant key by master §5's definition, so GitHub/community history continues across owners. The page is 11's `owner_control_change` red flag plus 04's chain `subnet_name` `identity_changed` (warning). Confirm that continuing history across a subnet sale is intended.

---

## 12. Sources

- `E:\TaoData\specs\02-cross-cutting-conventions.md` (A2, B1, B2, B7, B9, B12, C1, C2, C5, D1–D4, E1–E4, F1, F2, G1–G3, J1)
- `E:\TaoData\specs\00-MASTER-SPEC.md` §1.3, §2, §4, §5, §6, §7.3, §7.5, §8, §11, §12
- `E:\TaoData\specs\03-storage-schema.md` §3.2 (occupants, subnets, identity_*, manual_overrides, watch_channels, manual_notes, review_queue), §3.3 (`chain_rest_snapshots`), §3.1 (`collector_runs`, `job_state`, `control_state`), §3.6 (`events`), §3.9 (catalog rows for 04's events), Appendix A (04 renames)
- `E:\TaoData\specs\05-chain-collector.md` §6.1, §6.2, §6.3 (TMC budget, 04 row), §8 (`TmcClient`, `write_rest_snapshot`, `latest_rest_snapshot`, `REST_SNAPSHOT_KINDS`), §5 (`TMC_*` keys)
- `E:\TaoData\specs\01-repo-skeleton-runtime.md` §2.6 (`CollectorJob`), §2.7 (`JOB_TABLE`), §2.12 (`Http`, `ProviderLimiter`), §5–§6 (keys, limiter table); `16-ops-deploy-and-observability.md` §2.5 (liveness), §2.11 (pauses); `08-community-collector.md` §2.4, §2.8, §11 Q23; `09-x-collector-and-kol.md` (`normalise_x_handle`, YouTube resolution); `11-team-and-integrity.md` (consumers); `12-feature-store-and-scoring-engine.md` §2.8 (`stale_identity`); `14-api-telegram-and-ui.md` §2.4, §8.3; `17-data-migration-and-backfill.md` §2.2, §2.x fundamentals import
- `E:\TaoData\specs\18-operator-checklist.md` (A3 seed paths, B5 `seed/discord_channels.csv`, C4 `TMC_API_KEY`, D5 needs-review digest)
- `E:\TaoData\research\01-web-subnetalpha.md` (WP-REST shape, 400 past the last page, 32 ACF keys and counts, slug rule, `modified_gmt`, SCALE-prefix leak on `subnet-stats`, TMC coldkey-identities)
- `E:\TaoData\research\02-gapfill-discord-telegram-topology.md` § Data pulled (JSON-RPC read, 121 keys, 8-field SCALE decode, 87 vanity invites, 18 deep links, 18 bare handles, 69 empty/junk discord rows, 25 name-turnover netuids, netuids without identity rows)
- `E:\TaoData\research\02-gapfill-github-canonical-repo-resolution.md` §1–§3 (URL quirks `/orgs/…/repositories`, `.git`, `?tab=`, trailing `/`; rayonlabs→chutesai id 884216433; SN117 dead chain repo) — its "re-occupied" label for 20/27/53/119 is corrected in §2.2
- `E:\TaoData\research\02-gapfill-keyless-onchain-pillar-source.md` §1 (`/subnets/` + `latest_snapshot`, default page 25, coldkey-identities 1,088 rows; rate limits)
- `E:\SN_Trader\SubnetTrader\app\data\fundamentals_collect.py` lines 52-57 (constants), 62-70 (`_NETUID_RE`, `_parse_netuid`), 73-111 (`fetch_subnetalpha`), 117-158 (`fetch_onchain`), 163-177 (`_GH_RE`, `parse_repo`; owner denylist line 175), 226-231 (`_pick`), 234-319 (`build_dataset`), 388-393 (`write_atomic`) — re-read 2026-09-23
- `E:\SN_Trader\SubnetTrader\app\data\chain_client.py` lines 711-765 (`normalise_dynamic_info`; slot-identity comment 734-735, int coercion 738-742) — re-read 2026-09-23
- `E:\SN_Trader\SubnetTrader\data\fundamentals.json` (schema 2, 2026-06-02, records for 3, 27, 40, 64, 100, 106, 117) and `data\ledger.db` `pool_history` (read-only query 2026-09-23: registration blocks of 16, 20, 27, 40, 53, 64, 92, 100, 106, 119)
- `E:\SN_Trader\SubnetTrader\research\subnet_edge\` (25 `sn*.md` notes, `README.md`, `_*.md`, `synth_backtest/`; `sn64_chutes.md` format)
- Research scratchpad dumps 2026-09-22 (`chain_identities.py`, `chain_identities_v3.json`, `tmc_table.json`, `subnets_51.json`, `coldkey_identities.json`, `cpt1.json`/`cpt2.json`): fixture values; twox128 constants and the netuid-64 suffix recomputed 2026-09-23 with xxhash 4.0.1

## Changelog
- 2026-09-23 reconciliation + review:
  - B1/B7/J1: all times INTEGER; §3.1 now a column table against 03's DDL (no `CREATE TABLE`); `registered_ts`/`deregistered_ts` left to 17; `import_fundamentals(db, path, *, as_of, run_id, occupancy) -> LegacyReport` exposed, `source='legacy'`, `source_ref='fundamentals.json@2026-06-02'`.
  - E1/E2/E4: one job table (`registry_occupants` interval 6 h, `registry_identity` cron 02:40Z + `startup_catchup`) with factories, timeouts, flags, providers and 16's liveness numbers; 01 `CollectorJob` semantics.
  - G1/G2/G3/F1/F2: TMC only via 05's `TmcClient`; `/subnets/` and coldkey-identities stored in `chain_rest_snapshots` (kinds `subnets`, `coldkey_identities`); RPC/subnetalpha via `Http` with provider ids; call counts = 05's row (≤ 15/day); 01/05 key names adopted, private limiter/retry text removed.
  - B2/B9/D1/D2/D3/D4: `warning` everywhere; `events.source='registry'`; 03's payloads; no direct `send_alert` (package finding applied); `owner_coldkey` changes info; `registry_anomaly` 3-consecutive gate implemented by the emitter, new `source_failed` reason.
  - C2: `identity_stability` removed; downstream now `control_stability_90d`/`identity_churn` and 12's `stale_identity`. B12: pause via `control_state`/`PAUSE_GROUPS` `registry`.
  - Review: SN27/106 "re-occupancy" is false (same registration blocks in old ledger and TMC); real examples SN40/SN92; tests, verify numbers and R4 (identity epoch) corrected; legacy skip count ≈ 14, not ≥ 25.
  - Review: `/subnets/` row layout verified (snapshot strings, `subnetName`, `token_symbol`, `subnet_owner_hotkey`); SCALE test vector fixed (`0x3502`, no 70-byte field); query chunk 100; failover restarts the whole read.
  - Review: normaliser gaps fixed (bare domains, mixed-case Discord handles, `~`, `Owner<n>`, placeholder emails, leaked-secret redaction, secondary/reclassified refs never reuse a primary field → no flip-flop events); `normalise_x_handle`/`canonical_website` defined for 09/07.
  - Review: 04 emits no `github_repo` `source_disagreement` (06's, by repo id); main vanity excluded from R6; `watch_channels` priority/`declared_by`, update-only columns, retirement scope; PIT replay via `_coverage` markers; needs-review `new_occupant` watermark; manual_notes `seed_edge`; routes moved to 14.
