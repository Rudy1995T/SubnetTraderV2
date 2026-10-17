# [web] owner-extraction-mechanics

**Topic:** Subnet-owner extraction under dTAO — on-chain mechanics, observability (Taostats / TaoMarketCap / chain / dashboards), 2026 cases, and a computable "Owner Integrity" sub-score design (as of 2026-09-22)

Verification legend: **[CODE]** = read directly from `RaoFoundation/subtensor` `main` (spec_version 469; `opentensor/subtensor` now redirects there). **[DOC]** = official docs (bittensor.com/docs, docs.taostats.io). **[DATA]** = live data pulled 2026-09-22. **[PRESS]** = tao.media / news. **UNVERIFIED** where flagged.

## Findings

## 1. Owner cut: exact mechanics

**Rate.** `SubnetOwnerCut` = global `StorageValue<u16>`, default `11_796/65_535 = 18%` (`runtime/src/lib.rs: SubtensorInitialSubnetOwnerCut`). Root-settable only (`SubnetOwnerCutSet` event). Owners cannot raise it. **[CODE]**

**Accrual (per block, in alpha).** In `coinbase/run_coinbase.rs::emit_to_subnets()`: if `OwnerCutEnabled(netuid)` (per-subnet bool, default `true`, owner/root settable via admin-utils `sudo_set_owner_cut_enabled`, call_index 92) then `owner_cut = alpha_out × 0.18`, deducted from alpha_out and accumulated in `PendingOwnerCut(netuid)`. If disabled, the 18% stays with miners/validators (owner forgoes cut — a positive signal). **[CODE]**

**Payout (per tempo, as staked alpha).** In `drain_pending()` each epoch (`tempo`, default 360 blocks ≈ 72 min): `PendingOwnerCut` is drained and `distribute_dividends_and_incentives()` calls
`increase_stake_for_hotkey_and_coldkey_on_subnet(SubnetOwnerHotkey(netuid), SubnetOwner(netuid), netuid, owner_cut)`.
So the cut lands as **alpha stake on (hotkey = `SubnetOwnerHotkey`, coldkey = `SubnetOwner`)** — never as free TAO. To "sell", the owner must `remove_stake`/`remove_stake_limit`/`remove_stake_full_limit`/`unstake_all_alpha` (alpha→TAO via the pool, recorded as TAO outflow) or `swap_stake`/`move_stake` to another subnet, or `transfer_stake` to another coldkey. **[CODE]**
- Storage: `SubnetOwner(netuid) -> coldkey`, `SubnetOwnerHotkey(netuid) -> hotkey` (settable by owner/root via `sudo_set_sn_owner_hotkey`, rate-limited 1/week; emits `SubnetOwnerHotkeySet(netuid, hotkey)`). **[CODE]**
- Leased subnets (crowdloan): if `SubnetUidToLeaseId(netuid)` exists, part of the cut is paid to lease contributors first (`distribute_leased_network_dividends`). **[CODE]**
- Position query: `Alpha(hotkey, coldkey, netuid)` share map; SDK `get_stake_for_coldkey_and_hotkey(owner_ck, owner_hk, netuid)`. **[CODE/SDK]**

**Registration "free alpha" (closed).** Until hotfix v3.3.15-402 (2026-05-08), a founder received a pre-minted alpha position = (lock − min floor)/median alpha price, minted to the owner coldkey at registration and not surfaced on Taostats; now `owner_alpha_stake = AlphaBalance::ZERO` in `subnets/subnet.rs`. Subnets registered **before 2026-05-08** may hold a hidden initial owner position — estimate from `NetworkRegistrationQueued{lock_amount, median_subnet_alpha_price}` event. **[PRESS + CODE event]**

**Conviction / lock layer (shipped 2026-05-13; "Conviction v2").** **[CODE + DOC]**
- Extrinsics (pallet `SubtensorModule`): `lock_stake(hotkey, netuid, amount)` (call 136), `move_lock(destination_hotkey, netuid)` (137), `set_perpetual_lock(netuid, enabled: bool)` (138). **There is no `unlock_stake` extrinsic**; "unlocking" = `set_perpetual_lock(false)` which returns the lock to exponential decay. Rustdoc: "Locks decay by default."
- Storage: `Lock(coldkey, netuid, hotkey) -> LockState{locked_mass, conviction, last_update}`; aggregates `HotkeyLock(netuid,hotkey)`, `DecayingHotkeyLock(netuid,hotkey)`, `OwnerLock(netuid)`, `DecayingOwnerLock(netuid)`; `DecayingLock(coldkey,netuid)` (present-and-`false` ⇒ perpetual; absent ⇒ decaying); `LockingColdkeys(netuid,hotkey,coldkey)` reverse index; `UnlockRate`, `MaturityRate` (code default 934,866 blocks ≈ 130 d time constant, "50% in ~90 days"; taostats doc states τ = 1,142,108 blocks / half-life ≈ 110 d — **live values UNVERIFIED, query `SubtensorModule.UnlockRate`/`MaturityRate`**).
- Note: `OwnerLock(netuid)` aggregates *all* coldkeys locked to the owner hotkey (owner + community "to-owner"); isolate the owner's own commitment via `Lock(SubnetOwner, netuid, *)`.
- Owner locks get instant conviction (conviction == locked_mass; confirmed in live data, e.g. SN79 1,269,236.8 == 1,269,236.8).
- Events: `StakeLocked{coldkey,hotkey,netuid,amount}`, `PerpetualLockUpdated{coldkey,netuid,enabled}`, `LockMoved{...}`, `SubnetOwnerChanged{netuid,old_coldkey,new_coldkey}`. (`StakeUnlocked` is declared but never emitted.)
- **Critical nuance — auto-lock is OPT-IN.** `auto_lock_owner_cut()` (`staking/lock.rs:1848`) returns immediately unless `OwnerCutAutoLockEnabled(netuid)` is true; default `false` since PR #2695 "No default auto-lock" (merged 2026-05-27, spec 410). Set via admin-utils `sudo_set_owner_cut_auto_lock_enabled(netuid, bool)` (call 95, owner or root, blocked during `AdminFreezeWindow`). Exposed in runtime API `get_subnet_hyperparams_v3` as `owner_cut_auto_lock_enabled` and `owner_cut_enabled`. The press narrative "every owner emission is auto-locked since May 13" is **no longer true**: live snapshot 2026-09-22 shows **12 of 121 subnets** with auto-lock on (SN17, 31, 51, 53, 64 Chutes, 73, 87, 90, 96, 106, 109, 122). **[CODE + DATA]**
- Subnet king (ownership transfer by conviction): `change_subnet_owner_if_needed(netuid)` is called every epoch in `run_coinbase.rs:455`; requires subnet age ≥ `ONE_YEAR` (2,629,800 blocks) and the top hotkey's rolled conviction > 18% of eligible alpha (`SubnetAlphaOut − SubnetProtocolAlpha − AlphaBurned`) — spec ≥447 uses the single-hotkey bar, not subnet-wide sum. No `SubnetOwnerChanged` transfer observed in the leaderboard (`days_to_king` null for all 121 subnets) — **UNVERIFIED whether any has fired on mainnet**.

**Emission share / TAO flow.** `SubnetTaoFlow(netuid)` (i64, TAO in minus out via stake/unstake swaps) → `SubnetEmaTaoFlow(netuid) -> (block, ema)` with `FlowEmaSmoothingFactor`; `NetTaoFlowEnabled` gates flow-based share; `TaoFlowCutoff` caps it. An owner dump is recorded as TAO outflow (`record_tao_outflow` in `stake_utils.rs`) and depresses the subnet's own emission share — second-order damage. `SubnetMovingPrice` (EMA price, `ema_price_halving_blocks` = 201,600) drives pruning. **[CODE]**

## 2. Observability

### 2a. Taostats API (`https://api.taostats.io`, `Authorization` header) **[DOC]**
| Purpose | Endpoint | Key params / fields |
|---|---|---|
| Owner coldkey + history | `GET /api/subnet/owner/v1` | `netuid`, `owner`, `is_coldkey_swap`, block/timestamp range → `owner.ss58`, `is_coldkey_swap`, `block_number` |
| Owner sell/buy events | `GET /api/delegation/v1` | `nominator=<owner_ck>`, `netuid`, `action=undelegate` → `action` DELEGATE/UNDELEGATE, `amount` (TAO rao), `alpha`, `alpha_price_in_tao`, `slippage`, `extrinsic_id`, `is_transfer`, `transfer_address` |
| Owner position now | `GET /api/dtao/stake_balance/latest/v1` | `coldkey`, `hotkey`, `netuid` → `balance` (alpha rao), `balance_as_tao` |
| Owner position daily | `GET /api/dtao/stake_balance/history/v1` | same keys; snapshots at 00:00 UTC (block-level also queryable) |
| Locks / conviction | `GET /api/conviction/latest/v1` (+ `/api/conviction/history/v1`) | `coldkey`, `hotkey`, `netuid` → `amount_locked`, `conviction`, `perpetual`, `is_owner_hotkey`, `is_owner_coldkey` |
| Raw extrinsics | `GET /api/extrinsic/v1` | `signer_address=<owner_ck>`, `full_name` e.g. `SubtensorModule.remove_stake`, `.remove_stake_full_limit`, `.unstake_all_alpha`, `.swap_stake`, `.transfer_stake`, `.set_perpetual_lock`, `.lock_stake`, `.announce_coldkey_swap`, `.swap_coldkey_announced`, `.swap_hotkey`/`swap_hotkey_v2`, `.set_subnet_identity`, `AdminUtils.sudo_set_owner_cut_auto_lock_enabled`; `call_args`, `success` |
| Events | `GET /api/event/v1` | filter `StakeRemoved`, `ColdkeySwapAnnounced`, `ColdkeySwapped`, `SubnetOwnerChanged`, `PerpetualLockUpdated`, `SubnetIdentitySet`, `SubnetOwnerHotkeySet` — **exact filter param names UNVERIFIED** |
| Pool state | `GET /api/dtao/pool/latest/v1`, `/history/v1` | `tao_in`, `alpha_in`, `alpha_out`, `alpha_staked`, price |
| TAO flow | `GET /api/dtao/tao_flow/v1` | `netuid` → `tao_flow` (EMA field name UNVERIFIED) |
| Pruning risk | `GET /api/subnet/pruning/latest/v1` | `pruning_rank`, `moving_price`, `is_immune`, `immunity_blocks_remaining`, `registered_at_block` |
| Holder concentration | `GET /api/subnet/distribution/coldkey/v1` | per-subnet coldkey distribution |
| Identity | `GET /api/subnet/identity/v1` | name/repo/contact; diff over time |

### 2b. TaoMarketCap **[DATA, scraped 2026-09-22]**
Subnet page (`taomarketcap.com/subnets/<n>`) embeds per-subnet JSON with: `subnet_owner`, `subnet_owner_hotkey`, `owner_personal_stake`, `owner_personal_locked_alpha`, `owner_personal_perpetual_alpha`, `owner_personal_decaying_alpha`, `total_locked_alpha`, `owner_cut_per_block`, `owner_cut_cumulative`, `pending_owner_cut`, `subnet_ema_tao_flow` (signed string), `subnet_locked`, `largest_locked`, `subnet_moving_price`, `first_emission_block_number`, `ema_price_halving_blocks`, `root_prop`, `recycle_mode`. These are served by internal endpoints (e.g. SSE `/public/v1/sse/subnets/{netuid}/`) — not all in the public OAS.
Public API (`api.taomarketcap.com`, OAS at `/api/public-oas3.json`, 129 paths):
- `GET /public/v1/accounts/coldkey-stakes/{coldkey}/` → per subnet `balance_alpha`, `locked_alpha`, `is_subnet_owner`, `role`, `realized`, `dividends`.
- `GET /public/v1/analytics/neuron/{netuid}/{hotkey}/?span=1D|1W|1M|1Y|ALL` → hourly `conviction`, `conviction_locked_alpha`, `conviction_perpetual_alpha`, `conviction_decaying_alpha`, `alpha_stake` (docstring: "for rug-pull and owner-commitment analysis").
- `GET /public/v1/extrinsics/staking-activity/` → filters `coldkey`, `signer`, `function`, `exclude_function`, `netuid_relation=same|different`, `is_proxy`, `alpha_gte`; `/pressure/` and `/histogram/` variants. `GET /public/v1/transactions/stakes/stake-removed/`. `GET /public/v1/subnets/historical/{field_name}/`.

### 2c. Direct chain (bittensor SDK / substrate-interface) **[CODE/SDK]**
- `SubtensorModule.SubnetOwner(netuid)`, `SubnetOwnerHotkey(netuid)`, `OwnerCutEnabled(netuid)`, `OwnerCutAutoLockEnabled(netuid)`, `PendingOwnerCut(netuid)`, `Alpha(hotkey,coldkey,netuid)`, `StakingHotkeys(coldkey)`, `OwnedHotkeys(coldkey)`, `Lock(coldkey,netuid,hotkey)`, `OwnerLock(netuid)`, `DecayingOwnerLock(netuid)`, `DecayingLock(coldkey,netuid)`, `MinerBurned(netuid)`, `SubnetEmaTaoFlow(netuid)`, `SubnetMovingPrice(netuid)`, `NetworkRegisteredAt(netuid)`, `NetworkImmunityPeriod`, `SubnetEmissionEnabled(netuid)`, `SubnetUidToLeaseId(netuid)`.
- SDK: `get_subnet_owner_hotkey(netuid)`, `get_stake_for_coldkey_and_hotkey`, `get_stake_info_for_coldkey`, `get_stake_lock(ck, netuid, hk)`, `get_stake_locks(ck, netuid)`, `get_stake_availability_for_coldkeys([ck], [netuid])` → `{total, locked, available}` (runtime API `StakeInfoRuntimeApi.get_stake_availability_for_coldkeys`), `get_hotkey_conviction(hk, netuid)`, `get_subnet_hyperparameters(netuid)` (v3 list includes `owner_cut_enabled`, `owner_cut_auto_lock_enabled`), `swap_coldkey_announced`, `lock_stake`, `set_perpetual_lock`.
- Block-by-block: Δ`Alpha(owner_hk, owner_ck, netuid)` per tempo minus drained `PendingOwnerCut` = net owner disposal; sign of `StakeRemoved(coldkey, hotkey, tao, alpha, netuid, …)` events signed by owner coldkey.

### 2d. Public dashboards
- Rufus/Doug conviction table: `https://s3.hippius.com/rufus/public/conviction-table/` — **machine-readable** `data/table.json` (per subnet: `auto_lock_enabled`, `owner`/`to_owner`/`owner_side`/`challenger` buckets with `locked_perp`, `locked_decay`, `conviction`, `pct` of eligible alpha, per-row coldkey/hotkey/`perpetual`, `days_to_king`, `meta.owner_coldkey/owner_hotkey/registration_block`) and `data/baked-at.json` (`baked_at_utc`, `owner_emission_lock_count`). Refreshed 2026-09-22 22:30 UTC. **[DATA]**
- `subnetradar.com/swaps` — coldkey swaps & registrations feed (netuid, old/new coldkey, block; 22 recent owner changes incl. SN112, SN117, SN47 on Sep 21–22). **[DATA]**
- `taostats.io/subnets/<n>`, `taomarketcap.com/subnets/<n>` (owner block), `tao.app/subnets/<n>`.

## 3. Other owner-side red flags with on-chain signatures **[CODE unless noted]**
1. **Exit-by-decay:** owner coldkey `set_perpetual_lock(netuid, false)` → `PerpetualLockUpdated{enabled:false}`; then `owner_personal_decaying_alpha` > 0 and `available` rising in `get_stake_availability_for_coldkeys`. This is the only formal "unlock" path — a public 30–90-day pre-sell warning (press figures "63% at 30 d / 95% at 90 d" are inconsistent with code defaults; treat as UNVERIFIED).
2. **Coldkey swap:** user path = `announce_coldkey_swap(hash)` → ≥ `ColdkeySwapAnnouncementDelay` (5 days) → `swap_coldkey_announced`; events `ColdkeySwapAnnounced{who,new_coldkey_hash}`, `ColdkeySwapped{old,new}`, `ColdkeySwapDisputed`. Root-only `swap_coldkey` (call 71) needs no announcement. Taostats `/api/subnet/owner/v1` flags `is_coldkey_swap`. Locks/conviction follow the swap. Legit uses: multisig migration (docs recommend), post-compromise (ORO). Red flag when not preceded by a public notice or when identity changes at the same time.
3. **Hotkey swap:** `swap_hotkey`/`swap_hotkey_v2(hotkey,new_hotkey,netuid?,keep_stake)` → `HotkeySwapped`/`HotkeySwappedOnSubnet`; `SubnetOwnerHotkeySet` when owner re-points the cut hotkey (1/week limit).
4. **Identity churn:** `set_subnet_identity`, `update_symbol` → `SubnetIdentitySet(netuid)`; compare `github_repo`/`subnet_contact` over time (rebrand after scandal, e.g. SN3→Teutonic, SN39→"deprecated").
5. **Hyperparameter self-dealing:** `owner_cut` is NOT owner-settable. Owner-settable and relevant: `owner_cut_enabled` (turning it *off* is positive), `owner_cut_auto_lock_enabled`, `immunity_period`, `immune_owner_uids_limit`, `transfers_enabled`, `registration_allowed`/`min_burn`/`max_burn`, `commit_reveal_weights_enabled`, `liquid_alpha_enabled`, `yuma_version`, `activity_cutoff`. Owner changes are rate-limited (`OwnerHyperparamRateLimit` default 2 epochs) and blocked in the `AdminFreezeWindow` (default 10 blocks before epoch). Track `AdminUtils.*` extrinsics signed by the owner coldkey.
6. **Emission off:** `SubnetEmissionEnabled(netuid)` is **root-only** (`sudo_set_subnet_emission_enabled`, call 94); new subnets start off until `start_call` (`FirstEmissionBlockNumberSet`). Not an owner lever.
7. **Owner as validator/miner:** miner incentive to any hotkey in `OwnedHotkeys(SubnetOwner)` (plus `SubnetOwnerHotkey`) is **withheld** and burned/recycled; the withheld fraction is stored per tempo in `MinerBurned(netuid)` (U96F32). Owner hotkeys are dereg-immune up to `ImmuneOwnerUidsLimit`. Remaining vector: owner validator setting weights to *non-owner-coldkey* affiliated miners — detect via `Weights(netuid, owner_vali_uid)` concentration and owner validator stake share.
8. **Self-referential "owner lock":** community-looking locks to the owner hotkey from a second coldkey controlled by the team (observed on SN24: 150k "owner_side" from a non-owner coldkey). Weight `to_owner` locks by coldkey diversity (count, Gini).
9. **Registration/reset:** `register_network`/`register_network_with_identity` by a coldkey that previously owned a pruned subnet (join on `NetworkRegistrationQueued.coldkey`); `dissolve_network` is now **root-only**.
10. **Pruning rules in force:** cap 128 subnets; on new registration `get_network_to_prune()` picks the non-immune subnet with the lowest `SubnetMovingPrice` (ties → oldest); `NetworkImmunityPeriod` code default = 1,296,000 blocks (~180 d) from `NetworkRegisteredAt` — **NOTE: the keyless-onchain report verified the LIVE storage value is 864,000 blocks (120 d) via TaoStats `immunity_blocks_remaining` arithmetic; the master spec uses 864,000 and re-verifies quarterly.** Lock cost doubles per registration, decays over 100,800 blocks (~2 wk), floor 1,000 TAO, fully deposited as the pool's initial TAO. **[CODE + DOC]**

## 4. Known 2026 cases
| Case | Netuids | Date | What happened / on-chain evidence | Trackers |
|---|---|---|---|---|
| **Covenant AI (Sam Dare)** | SN3 Templar, SN39 Basilica, SN81 Grail | 2026-04-09/10 | ~37,000 TAO (~$10–11 M) of alpha sold across the three subnets before/around the exit post; TAO −25–30% in hours (~$900 M mcap), Templar alpha −50%+. Const admitted selling <1% of his own alpha on those subnets ("near 100% burn code"). Community reconstructed the timeline from unstake events; no single canonical dashboard. Triggered BIT-0011/Conviction (mainnet 2026-05-13) and the May 8 free-alpha hotfix. As of Sep 22: SN3 owner lock 0, community to-owner 386k; SN39 "deprecated"; SN81 "Reliquary". | tao.media, The Block, The Defiant, Rufus table |
| **ORO** | SN15 | 2026-07-13 | Owner coldkey compromised (DPRK-attributed phishing); **147,000 alpha drained and sold over ~10 h** — on-chain indistinguishable from an owner dump. Post-mortem Jul 21; coldkey swapped to multisig. Owner lock today = 0. | abittensorjourney.com, Protos |
| **Connito AI** | SN102 | 2026-07-13 | Owner wallet drained of 2,500 TAO same day, same vector; subnet kept running. | same |
| **Quasar** | SN24 | early Aug 2026 | Model plagiarism (98% inherited weights) exposed days after a Novelty Search pitch; alpha −75%; owner banned, mining paused, coldkey swap announced, emissions cut 86% in v440 gate reshuffle. Today: owner lock 0, "owner_side" 150k from a non-owner coldkey, challengers 549k perpetual (max single hotkey 16.9% < 18% bar). | abittensorjourney.com, X threads |
| **Tenex / Tenexium** | SN67 | 2026-01-02 | EVM liquidity contract exploited, ~$2.8 M drained; community alleged the owner "left the exploit in" (unproven). Now re-used as "Harnyx". | SimplyTao, X |
| (Context) Const's alpha sales on SN3/39/81, and Rao Foundation/Latent 11 takeover of core dev (Apr 23) | | Apr 2026 | Whale/insider selling by non-owner insiders is a related but distinct signal. | bex.co, The Block |

Leaderboard state (2026-09-22): 121 subnets; **63 with zero owner lock**; top owner locks: SN79 MVTRX 1.27 M α (50.8% of eligible), SN88 920k, SN2 845k, SN60 640k, SN64 Chutes 614k (auto-lock on), SN32 600k, SN8 600k. **[DATA]**

## 5. Metric proposal — "Owner Extraction / Integrity" sub-score (0–100)

All amounts in alpha unless noted; `owner_ck = SubnetOwner(netuid)`, `owner_hk = SubnetOwnerHotkey(netuid)`; recompute daily.

**S1. Owner net-sell ratio (30 d) — weight 30**
- Data: Taostats `/api/delegation/v1?nominator=owner_ck&netuid=n&action=undelegate` (+ `move_stake`/`swap_stake`/`transfer_stake` out via `/api/extrinsic/v1?signer_address=owner_ck` or TMC `staking-activity?coldkey=…&netuid_relation=different`); earned = Σ drained `PendingOwnerCut` ≈ `owner_cut_per_block × 7200 × 30` (TMC) or `0.18 × alpha_out_emitted_30d` if `owner_cut_enabled`.
- Formula: `sell_ratio = Σ alpha_out_of_position(owner_ck, n, 30d) / owner_cut_earned(n, 30d)`; also `sell_impact = Σ TAO_received / tao_in_pool_avg`.
- Thresholds: `sell_ratio ≤ 0.25` → 0 penalty; `0.25–1.0` → linear to 15; `> 1.0` (selling principal) → 25; any 7-day window with `sell_impact > 2%` of pool TAO or a single `remove_stake_full_limit`/`unstake_all_alpha` → 30 (max). Owner-cut disabled (`owner_cut_enabled=false`) → −5 penalty (bonus).

**S2. Owner commitment ratio — weight 25**
- Data: `Lock(owner_ck, n, *)` via SDK `get_stake_locks` / `get_stake_availability_for_coldkeys`; Taostats `/api/conviction/latest/v1?coldkey=owner_ck&netuid=n` (`amount_locked`, `perpetual`, `is_owner_coldkey`); TMC `owner_personal_perpetual_alpha`, `owner_personal_stake`; hyperparam `owner_cut_auto_lock_enabled`; Rufus `table.json` `owner.pct`.
- Formula: `lock_ratio = owner_perpetual_locked / owner_total_stake`; `commit_pct = owner_perpetual_locked / eligible_alpha_out`.
- Score: auto-lock on: +10; `lock_ratio ≥ 0.75`: +10; `commit_pct ≥ 5%`: +5 (cap 25, invert to penalty = 25 − score). Zero owner lock AND auto-lock off AND subnet age > 90 d → full 25 penalty. Discount `to_owner`/`owner_side` locks whose coldkeys have < 3 distinct lockers or that also appear as owner-adjacent (S4).

**S3. Exit / control-change events (90 d lookback, decay by age) — weight 25**
- Data: events `PerpetualLockUpdated{coldkey=owner_ck, enabled:false}`, `ColdkeySwapAnnounced{who=owner_ck}`, `ColdkeySwapped`, `SubnetOwnerChanged`, `SubnetOwnerHotkeySet`, `SubnetIdentitySet`; Taostats `/api/subnet/owner/v1?netuid=n` (`is_coldkey_swap`); TMC `owner_personal_decaying_alpha` trend; subnetradar.com/swaps.
- Score: owner toggled to decaying or `owner_personal_decaying_alpha > 0` → 25 (hard red, this is the formal exit signal); coldkey swap without prior identity/announcement continuity → 15; owner hotkey re-pointed + identity change in same 14 d → 10; each halves every 45 d. Manual whitelist for disclosed multisig migrations / post-compromise recoveries (ORO pattern).

**S4. Owner self-dealing & concentration — weight 10**
- Data: `OwnedHotkeys(owner_ck)` ∩ `Uids(n)`; validator permit/stake from metagraph; `Weights(n, owner_vali_uid)`; `MinerBurned(n)`; `/api/subnet/distribution/coldkey/v1`.
- Formula: `owner_vali_stake_share`; `self_weight = Σ w(owner_vali → hotkeys whose coldkey ∈ owner cluster) / Σ w(owner_vali)`; `owner_holding_share = owner_total_alpha / alpha_out`.
- Thresholds: `owner_vali_stake_share > 30%` → 4; `self_weight > 20%` → 4; `owner_holding_share > 25%` with `lock_ratio < 0.5` → 2. (High `MinerBurned` is neutral — protocol already withholds it — but signals owner keys mining.)

**S5. Liquid overhang vs pool depth — weight 10**
- Data: `available` from `get_stake_availability_for_coldkeys([owner_ck],[n])`; pool `tao_in`, `alpha_out` from `/api/dtao/pool/latest/v1`; price.
- Formula: `overhang = owner_available_alpha / alpha_out`; `dump_impact = owner_available_alpha × price / tao_in`.
- Thresholds: `dump_impact < 5%` → 0; `5–25%` → linear to 6; `> 25%` → 10. Add +2 if subnet registered before 2026-05-08 and estimated initial owner alpha (`lock_amount − floor)/median price`) is still unaccounted for.

`OwnerIntegrity = 100 − (S1 + S2 + S3 + S4 + S5)`. Suggested bands: ≥ 80 clean; 60–79 watch; 40–59 elevated; < 40 red. Flag the whole subnet as "extraction event" if S1 = 30 or S3 ≥ 15 regardless of total. Caveat: a wallet compromise (ORO/Connito) is indistinguishable from a dump in S1; keep the penalty but allow a post-mortem override.

## Sources
- Code: https://github.com/RaoFoundation/subtensor — `pallets/subtensor/src/{lib.rs, coinbase/run_coinbase.rs, coinbase/subnet_emissions.rs, coinbase/root.rs, staking/lock.rs, macros/dispatches.rs, macros/events.rs, rpc_info/subnet_info.rs, utils/misc.rs}`, `pallets/admin-utils/src/lib.rs`, `runtime/src/lib.rs`; PR #2695 https://github.com/RaoFoundation/subtensor/pull/2695; PRs #2632/#2687/#2699/#2800/#3183.
- SDK: https://github.com/opentensor/bittensor/blob/master/bittensor/core/subtensor.py
- Docs: https://www.bittensor.com/docs/guides/subnets ; https://www.bittensor.com/docs/concepts/emissions ; https://docs.taostats.io/docs/conviction-v2.md ; Taostats reference pages: get-subnet-owner-1, get-stakingdelegation-events, get-stake-balance, get-historical-stake-balance, get-conviction, get-conviction-history, get-extrinsics, get-event-1, get-subnet-tao-flow, subnet-deregistration-ranking, get-subnet-coldkey-distribution-1 (all under https://docs.taostats.io/reference/); index https://docs.taostats.io/llms.txt
- TaoMarketCap: https://taomarketcap.com/subnets/1 (embedded owner fields); OAS https://api.taomarketcap.com/api/public-oas3.json
- Dashboards: https://s3.hippius.com/rufus/public/conviction-table/ (+ `data/table.json`, `data/baked-at.json`); https://subnetradar.com/swaps
- Press/cases: https://www.tao.media/the-conviction-upgrade-bittensor-just-made-subnet-owner-exits-a-public-event/ ; https://www.tao.media/covenant-ais-bittensor-exit-what-happened-how-bittensor-responded-and-whats-next-for-the-network/ ; https://www.tao.media/bittensors-hotfix-ends-subnet-owners-free-alpha-at-registration/ ; https://www.tao.media/which-bittensor-subnets-have-locked-alpha-the-full-conviction-leaderboard/ ; https://www.theblock.co/post/397028/bittensor-co-founder-denies-suspending-subnet-emissions-after-covenant-ai-exit ; https://thedefiant.io/news/markets/tao-tanks-20-as-major-subnet-developer-accuses-bittensor-founder-of-decentralization-theatre ; https://www.abittensorjourney.com/p/navigating-bittensor-july-2026 ; https://www.abittensorjourney.com/p/navigating-bittensor-august-2026 ; https://protos.com/ai-firm-oro-says-north-korean-hacker-stole-600k-worth-of-crypto/ ; https://simplytao.ai/blog/subnet-67-crash-what-happened-in-tenex ; https://simplytao.ai/blog/bittensor-locked-stake-the-conviction-mechanism-explained ; https://bex.co/blog/2026/04/23/latent-11-bittensor-core-development-handoff-opentensor-decentralization ; https://x.com/learnbittensor/status/2043629276692402561 (BIT-0011)
