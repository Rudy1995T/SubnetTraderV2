# [code] fundamentals-scorer

**Topic:** SN_Trader Phase-1 subnet fundamentals/quality scorer + Phase-2 validation: exact scraping/chain/GitHub mechanics, scoring formula, look-ahead caveats, Pi read-only consumption, and what is portable into TaoSentinel

## Findings

## 1. Architecture in one line
Off-Pi collector (`app/data/fundamentals_collect.py`) -> pure scorer (`app/strategy/quality.py`) -> single committed JSON artifact (`data/fundamentals.json`, git as transport) -> read-only, never-raises loader on the Pi (`app/data/fundamentals.py::FundamentalsStore`). Spec: `E:\SN_Trader\SubnetTrader\specs\subnet-fundamentals.md` (Phase 1, dated 2026-06-02, "BUILT, NOT wired into any trade decision"). Phase 2: `specs\subnet-fundamentals-phase2.md` (validate-first backtest; result = SHELVE the gate).

## 2. subnetalpha.ai scrape — exact mechanics (`fundamentals_collect.py:52-111`)
- NOT an HTML scrape. It hits the site's **WordPress REST API**, no key: `WP_API = "https://subnetalpha.ai/wp-json/wp/v2/subnet"` (line 52), custom post type `subnet`.
- `fetch_subnetalpha(client)` (73-111): `GET` with `params={"per_page":100,"page":N,"_fields":"slug,title,acf"}`, loops `page in range(1,4)`; breaks on HTTP 400 (WP "invalid page number" past the last page), empty list, or `len(items)<100`.
- Fields come from the **ACF (Advanced Custom Fields)** block. Exact ACF keys read (96-108): `acf.subnet_number` ("Subnet 75" -> `_parse_netuid` regex `(\d+)`, line 62-70), `acf.subnet_owner` -> owner, `acf.team_info`, `acf.short_desc`, `acf.what_exactly_does_it_do` -> long_desc, `acf.website_url`, `acf.github_url`, `acf.twitterx_url`, `acf.discord_url`, `acf.telegram_url`; plus `title.rendered` -> name, `slug`.
- User-Agent: `"SubnetTrader-fundamentals/1.0 (+https://github.com/Rudy1995T)"` (line 55). Spec notes `robots.txt` fully permissive. Timeout default 30s (`--timeout`).
- Coverage in the shipped artifact: 128 of 129 records carry `"subnetalpha"` in `sources` (only netuid 0 root is on-chain-only).

## 3. On-chain SubnetIdentity read — exact SDK calls (`fundamentals_collect.py:117-158`)
- `fetch_onchain(network)`: lazy `import bittensor`; `sub = bittensor.AsyncSubtensor(network=network)`; `await sub.initialize()`; `current_block = int(await sub.get_current_block())`; `infos = await sub.all_subnets()` (one call returns every `DynamicInfo`); `await sub.close()` in `finally`.
- Per `info`: `info.netuid`, `info.subnet_name`, `info.subnet_identity` (the `SubnetIdentity` object; attrs used: `github_repo`, `subnet_url`, `discord`, `description`, `subnet_name` (used as owner)), `info.network_registered_at` (registration block).
- Age: `age_days = (current_block - reg_at) * BLOCK_SECONDS / 86400` with `BLOCK_SECONDS = 12.0` (line 57, 144).
- Default endpoint `DEFAULT_ARCHIVE = "wss://archive.chain.opentensor.ai:443"` (line 54) — the free public archive node; `--network` overrides. `--no-onchain` skips it for hosts without the SDK. SDK pin: `bittensor>=10.1,<11.0` (`requirements.txt:10`).
- The live chain client (`app/data/chain_client.py::normalise_dynamic_info`, lines 711-760) deliberately **discards** `subnet_identity`; it only keeps `network_registered_at` (line 738-742) as an age / slot-identity marker. Note the docstring at 734-735: `network_registered_at` **changes when a deregistered netuid is reused** — this is the slot-reuse detector.
- `backfill_metadata.py` (`E:\SN_Trader\SubnetTrader\app\data\backfill_metadata.py`) is a different thing: historical backfill of `moving_price`/`subnet_volume` into `pool_history` via `sub.all_subnets(block=N)` against the archive node, one query per 24h bucket (`representative_blocks`, lines 38-50; `_apply`, 53-65; UPDATE gated on `moving_price IS NULL`, resumable per bucket). Proves `all_subnets(block=N)` returns real historical state on the archive node — reusable for point-in-time reconstruction.
- Coverage: 129/129 records have `"onchain"` in `sources`.

## 4. GitHub query — exact mechanics (`fundamentals_collect.py:161-220`)
- One endpoint only: `GITHUB_API = "https://api.github.com/repos/{owner}/{repo}"` (line 53). No commits, contributors, issues, or releases endpoints in Phase 1.
- Metrics captured (205-217): `pushed_at` -> `github_pushed_days_ago` (float, vs collector `now`), `stargazers_count` -> `github_stars`, `archived` -> `github_archived` (bool). That's it — no commit counts, no contributor counts.
- Auth: header `Accept: application/vnd.github+json`; `Authorization: Bearer <token>` where token = `--github-token` arg or env `GITHUB_PAT` or `GITHUB_TOKEN` (line 342). Without a token the whole GitHub step is skipped (`github_active` -> None, `has_github` static signal still applies). 5000 req/hr with a PAT.
- Concurrency: `asyncio.Semaphore(5)`; 404 -> silently skipped; 403 -> prints "rate-limited", that request dropped (no retry/backoff); non-200 -> skipped.
- Repo URL parsing `parse_repo` (166-177): regex `github\.com/([^/\s]+)/([^/\s#?]+)`, strips `.git`, rejects owner in `("orgs","sponsors","topics","settings")`. Only the FIRST repo URL per subnet is used (`_pick(alpha.github, onchain.github)`), so org-level URLs like `github.com/macrocosm-os` without a repo yield no match.
- Result: 89 of 129 records have a numeric `github_pushed_days_ago` / `"github"` in `sources`; only 1 repo `archived: true`.
- Phase-2 §4 (`phase2.md:201`) sketches the deferred point-in-time variant: `/repos/{o}/{r}/commits?until=<bar_date>&per_page=1` -> "days since last push as of date", cached per repo into a `github_history.json` sidecar. Never built.

## 5. Scoring formula and weights (`app/strategy/quality.py`, pure, tested)
`QualityConfig` (lines 36-67): weights sum to 1.0 — `w_github_active=0.30`, `w_team=0.20`, `w_has_github=0.15`, `w_website=0.10`, `w_description=0.10`, `w_socials=0.08`, `w_age=0.07`; thresholds `github_active_days=30`, `github_stale_days=365`, `age_mature_days=180`, `min_desc_chars=80`, `min_coverage=0.25`, `low_quality_at=0.35`. The config is embedded in the JSON under `"config"` for provenance (`fundamentals.json:5-19`).

`derive_components(record, cfg)` (125-204) -> `QualityComponents` (7 fields, each float|None):
- `team_attributed`: `owner+team_info` blob empty -> None; matches `_ANON_TOKENS=("anonymous","anon","unknown","undisclosed","tbd","n/a","none")` as whole-ish words -> 0.0; owner present -> 1.0; team text but no owner -> 0.7.
- `has_website`: empty -> 0.0; http(s) URL whose host is NOT in `x.com, twitter.com, discord.com, discord.gg, t.me, github.com, youtube.com` -> 1.0; else 0.0 (so bare `www.deprecated.com` without scheme -> 0.0).
- `has_github`: `"github.com/" in url.lower()` -> 1.0 else 0.0.
- `github_active`: None if has_github<1 (not applicable); 0.0 if archived; else `_recency_score(days_ago, 30, 365)`: 1.0 at <=30d, linear to 0.0 at 365d; None if `pushed_days_ago` is None.
- `has_socials`: count of non-empty discord/twitter/telegram: >=2 -> 1.0, 1 -> 0.5, 0 -> 0.0.
- `has_description`: `short_desc + long_desc` empty -> 0.0; len>=80 -> 1.0; else 0.5.
- `age`: None if unknown; else `clamp01(age_days/180)`.

`quality_score(components, cfg)` (207-233): `coverage = sum(w for non-None)/sum(all w)`; if `coverage < 0.25` -> `(None, coverage)`; else `score = sum(w*c for non-None)/avail_w`, clamped to [0,1]. Missing-data semantics: empty field = real 0.0 (absence of evidence IS the signal); None only when genuinely unknowable (dropped from average, renormalised).

`static_quality_score` (236-249): `quality_score(replace(components, github_active=None, age=None))` — the 0.63-weight look-ahead-clean editorial core. `is_low_quality(score)` (252-255): `score is not None and score <= 0.35`; None never low.

`classify_category(name, short_desc)` (263-288): first-match keyword table with 13 labels ordered distinctive-vertical-first (gaming, science, storage, finance, media, privacy, security, oracle, data, inference, training, compute, agents; else "other"). Deliberately NOT in the score. Classifies on short_desc only (long prose trips spurious matches — `fundamentals_collect.py:285-288`). In the shipped file, ~75 of 129 land in "other" — the keyword table is weak.

Shipped-file distribution: 129 scored (0 `quality: null`), 18 with quality < 0.40.

## 6. Merge logic (`fundamentals_collect.py:234-319`)
`build_dataset`: universe = `set(alpha) | set(onchain)`; per field `_pick(alpha_value, onchain_value)` = first non-empty (subnetalpha wins, chain fills gaps); `long_desc = _pick(alpha.long_desc, onchain.description)`; twitter/telegram/short_desc/team_info are subnetalpha-only; `age_days` chain-only; GitHub metrics merged by `parse_repo(github_url)`. `sources` list records which of the three contributed. `rescore_dataset` (363-385) recomputes scores from stored `components` with no network (`--rescore IN.json`) — reusable whenever only weights change. `write_atomic` (388-393): tmp + `os.replace`.

## 7. JSON schema (`data/fundamentals.json`, schema_version 2, generated 2026-06-02T11:03:41Z, `count: 129`)
Top level: `generated_at` (ISO Z), `generated_at_unix`, `schema_version`, `config` (QualityConfig asdict), `count`, `subnets` (dict keyed by netuid string). Per record: `netuid`, `name`, `slug`, `owner`, `category`, `short_desc`, `website`, `github`, `twitter`, `discord`, `telegram`, `age_days`, `github_pushed_days_ago`, `github_stars`, `github_archived`, `components{team_attributed,has_website,has_github,github_active,has_socials,has_description,age}`, `quality`, `coverage`, `sources[]`, `quality_static`, `coverage_static`. Examples: SN1 Apex (Macrocosmos; quality 1.0, coverage 0.7 because github_active None — repo not resolved), SN2 DSperse (Inference Labs; 2114 stars, pushed 0.7d ago, quality 1.0, coverage 1.0), SN3 "Unknown" (deprecated placeholder: owner "Unknown", website `www.deprecated.com`, github `https://github.com/username/repo` -> still gets `has_github=1.0`, quality 0.4429). Netuid range 0..~128 (129 keys, root included).

## 8. Look-ahead / confounding caveats (Phase 2, `phase2.md` §1-2, §2a.6; results `data/backtest/results/quality_overlay_2026-06-02.md`)
- **Fact A**: 0 closed value trades -> no trade backtest possible; validation is a cross-sectional factor test on `pool_history` (4.17M rows, 128 netuids, 2025-09-22..2026-06-02).
- **Fact B, size confound**: big subnets are mechanically more "legit". Full-universe quality IC was negative (t -2.1/-2.9/-3.2 at h1/6/12) and **vanished under `--controls size`** (t -0.5/-1.2/-1.0) => quality is a size proxy. Within the live value band (1500-3000 TAO, 13 names) IC is noise; wider band (500-5000, 36 names) ~zero.
- **Slot reuse (wrong-subnet)**: a snapshot keyed by netuid labels subnet B's history with subnet A's quality after dereg/re-registration (4 in-window resets: netuids 82, 96, 115, 119). Fix = current-occupant join: return None for bars before `_reg_unix(reg_block)` (`factor_backtest.py:111-124`); `Bar.quality` uses sentinel `-1.0` (never 0.0, which is a real score).
- **Stale-value (time travel)**: `github_active` (0.30) and `age` (0.07) measured today are look-ahead when back-applied; hence `quality_static`. Point-in-time reconstruction (§4) deferred.
- **Survival test**: zero in-band deaths in every tercile/band/horizon/scope — the depth-band floor already shields rugs (deaths happen at 1-5 TAO depth). So no death-rate for quality to predict.
- Other caveats: DSR uninformative (every factor clears 0.95 on ~200-obs panels), dense panel only ~6 weeks, <=12-25 independent dates. Verdict: gate stays inert (`VALUE_QUALITY_GATE_MIN=-1.0`, `config.py:421`); dataset kept as analyst reference; telemetry logs point-in-time quality for a forward A/B.

## 9. How the Pi consumed it (read-only)
- `FundamentalsStore.load()` (`fundamentals.py:61-96`): default path repo-root `data/fundamentals.json`; FileNotFound / OSError / ValueError -> empty store + one warning, never raises. Accessors `quality(netuid)`, `quality_static(netuid)`, `is_low_quality`, `age_hours`, `is_stale(max_age_hours=168)`, `__len__`, `__contains__`.
- `main.py:257-268`: on startup, `ema_value._fundamentals = FundamentalsStore.load()` (value manager only). `main.py:1334-1349` `refresh_fundamentals()` re-reads daily, keeps last-good store if reload comes back empty; scheduled via APScheduler interval 24h, `id="fundamentals_refresh"`, `misfire_grace_time=3600` (`main.py:1568-1577`). `/health` (1641-1646) and status endpoint (2244-2252) expose `subnets`, `stale`, `age_hours`, `quality_gate_min`.
- `ema_manager.py:555-593`: per in-band candidate, reads `fstore.quality(netuid)` / `quality_static`, applies inert `_quality_blocks(quality, cfg.quality_gate_min)` (None never blocks), emits a structured `value_eval` JSONL event with `quality`, `quality_static`, `quality_gate_min`, `admitted`, `reject_reason`; `quality_blocked` counter in `value_stats` (line 805).
- Backtest join: `factor_panel.py:274-287` builds `quality_by_netuid` from `store.quality_static(nu)` and stamps it on every Bar.

## 10. Portability into a standalone TaoSentinel service
Directly portable (copy files, drop the `app.` import prefix): `quality.py` entire (zero deps beyond dataclasses); `fundamentals_collect.py` functions `_parse_netuid`, `fetch_subnetalpha`, `fetch_onchain`, `parse_repo`, `fetch_github`, `_pick`, `build_dataset`, `rescore_dataset`, `write_atomic` (deps: httpx, optional bittensor); `fundamentals.py` loader (drop the `app.strategy.quality` import or vendor it); `tests/test_fundamentals.py` (24 tests, lines 72-308) as a regression suite; `backfill_metadata.py`'s `all_subnets(block=N)` pattern for point-in-time reconstruction; the JSON schema as-is for a v1 snapshot format. Must be rewritten/extended: GitHub coverage (add `/commits`, `/contributors`, `/releases`, `/issues`, rate-limit backoff, org-URL handling), the weak keyword category table, a time-series store (the current design is a single snapshot keyed by netuid with no history — TaoSentinel needs per-netuid per-day rows plus a `network_registered_at`-keyed occupant id), and everything Discord/X/website-liveness, which does not exist here.

## Reusable assets

- `E:\SN_Trader\SubnetTrader\app\strategy\quality.py` — Pure, dependency-free quality scorer: QualityConfig (weights/thresholds), QualityComponents, derive_components, quality_score (coverage-renormalised weighted mean), static_quality_score, is_low_quality, classify_category keyword table.
  - How to reuse: Copy verbatim as the seed of TaoSentinel's fundamentals sub-score; rescale to 0-100 by multiplying; keep the None-vs-0.0 semantics; extend QualityComponents with new signals and add weights to QualityConfig; replace the keyword category table.
- `E:\SN_Trader\SubnetTrader\app\data\fundamentals_collect.py` — Async collector: subnetalpha.ai WP REST (ACF field map lines 96-108), on-chain SubnetIdentity via AsyncSubtensor.all_subnets() (117-158), GitHub /repos/{o}/{r} with PAT + Semaphore(5) (180-220), merge with subnetalpha-wins _pick (234-319), no-network --rescore (363-385), atomic write (388-393).
  - How to reuse: Lift fetch_subnetalpha, fetch_onchain, parse_repo, fetch_github, build_dataset into a `collectors/` package; swap print() for logging; add retry/backoff on 403 and secondary GitHub endpoints; keep CLI flags --no-onchain/--no-github/--limit/--network/--rescore.
- `E:\SN_Trader\SubnetTrader\app\data\fundamentals.py` — Read-only never-raises FundamentalsStore loader with quality/quality_static accessors and staleness (is_stale default 168h, age_hours).
  - How to reuse: Reuse as the read side contract for any downstream consumer (trading bot re-activation); the neutral-on-missing invariant is the exact interface a future bot weight should honour.
- `E:\SN_Trader\SubnetTrader\data\fundamentals.json` — Committed snapshot dated 2026-06-02: 129 netuids (0-128 incl. root), schema_version 2, embedded QualityConfig, 89 with GitHub metrics, 128 from subnetalpha, 129 from chain.
  - How to reuse: Seed TaoSentinel's first history row (as-of 2026-06-02) and as a fixture for schema tests; use its ACF-derived link fields (website/github/twitter/discord/telegram) as the initial crawl targets.
- `E:\SN_Trader\SubnetTrader\tests\test_fundamentals.py` — 24 pytest cases covering scorer semantics (Hippius high, junk low, anonymous team, social-link-not-website, recency/archive, sparse-scores-low-not-unknown, static score, category, loader missing/malformed/stale).
  - How to reuse: Copy into the new repo's test suite; adjust import paths; they encode the missing-data invariants that must survive any re-scaling to 0-100.
- `E:\SN_Trader\SubnetTrader\app\data\backfill_metadata.py` — Pattern for historical chain reads: AsyncSubtensor(network=archive).all_subnets(block=N), one call per time bucket, resumable UPDATE gated on NULL.
  - How to reuse: Template for reconstructing point-in-time age / occupant-id / moving_price history per netuid on the Linux box; use the free archive endpoint wss://archive.chain.opentensor.ai:443.
- `E:\SN_Trader\SubnetTrader\app\data\chain_client.py` — normalise_dynamic_info (lines 711-760) projects DynamicInfo -> dict incl. network_registered_at (slot-reuse marker), moving_price, subnet_volume, tao_in/alpha_in, emission.
  - How to reuse: Reuse the field projection for the on-chain signal collector; pair network_registered_at with netuid to form a stable subnet identity key (netuid + reg_block) so history survives dereg/reuse.
- `E:\SN_Trader\SubnetTrader\specs\subnet-fundamentals-phase2.md` — Look-ahead hazard analysis (slot reuse, stale-value), size-confound reasoning, static-vs-time-varying component classification, go/no-go decision tree, point-in-time GitHub reconstruction sketch (§4).
  - How to reuse: Adopt §2 verbatim as TaoSentinel's backtest hygiene rules: current-occupant join on reg_block, store every signal as-of its observation date, and always test `--controls size` before believing a factor.
- `E:\SN_Trader\SubnetTrader\data\backtest\results\quality_overlay_2026-06-02.md` — Documented negative result: static quality is a size proxy (IC vanishes under size control), zero in-band deaths, gate shelved.
  - How to reuse: Cite as prior evidence; it argues the new tracker needs time-varying signals (commit velocity, social cadence, milestone deltas) rather than static presence flags to find edge orthogonal to size.

## Gotchas

- subnetalpha.ai is read via the WordPress REST API (`/wp-json/wp/v2/subnet?per_page=100&page=N&_fields=slug,title,acf`), not HTML; the ACF key names (subnet_number, subnet_owner, team_info, short_desc, what_exactly_does_it_do, website_url, github_url, twitterx_url, discord_url, telegram_url) are site-specific and may change without notice — verify they still exist before porting.
- GitHub freshness is pushed_at only; `pushed_at` bumps on ANY branch push (incl. bot/dependabot), so it overstates human activity. No commits/contributors/issues endpoints were queried.
- Placeholder repo URLs pass `has_github` (e.g. SN3 `https://github.com/username/repo` scores has_github=1.0) — 404s are silently dropped, leaving github_active=None rather than 0.0, which renormalises the penalty away.
- The shared `discord.gg/bittensor` invite counts as a subnet-specific social (has_socials), inflating community signal for subnets without their own server.
- GitHub 403 (rate limit) is not retried; only 5 concurrent requests; token read from `GITHUB_PAT` or `GITHUB_TOKEN` env or `--github-token`.
- Age uses a hard-coded 12s block time from `network_registered_at`; netuid slots are reused after dereg, so `netuid` alone is NOT a stable identity — key history on (netuid, network_registered_at).
- The dataset is a single snapshot keyed by netuid with no time dimension; Phase-2 proved back-applying today's github_active/age is look-ahead. Any new tracker must store as-of-date rows from day one.
- Phase-2 result: static quality is confounded with pool size (IC vanishes under `--controls size`); do not expect presence-flag scores alone to carry alpha.
- Coverage < 0.25 -> quality None (neutral); None must never be treated as 'bad' downstream (loader/gate invariant enforced in tests).
- classify_category matches on short_desc only and yields 'other' for ~75/129 subnets — the keyword table is too weak to use for sector allocation.
- Running the collector on Windows: the subnetalpha + GitHub paths are pure httpx and work anywhere; only `fetch_onchain` needs the bittensor SDK (use `--no-onchain` on Windows).

## Open questions

- Does subnetalpha.ai's WP REST endpoint and ACF schema still exist unchanged as of 2026-09-22 (the collector was last run 2026-06-02)?
- Is the free archive node `wss://archive.chain.opentensor.ai:443` still open and quota-free, and does `bittensor>=10.1` still expose `SubnetIdentity.github_repo/subnet_url/discord/description` with those attribute names?
- How many netuids exist now (the snapshot had 129 incl. root; new registrations since June are absent)?
- Which of the 40 subnets without resolved GitHub metrics had org-only URLs (github.com/org) vs 404 placeholders — matters for deciding whether to crawl org repo lists in the new collector.
