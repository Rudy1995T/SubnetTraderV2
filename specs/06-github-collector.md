# 06 — GitHub collector: canonical repo resolver, org-level activity, builder metrics

**Status:** built 2026-10-17 (reconciled to 02 and adversarially reviewed 2026-09-23; pre-build review + live PAT verification 2026-10-17, see Changelog) · **Phase:** 4 · **Depends on:** 02 (binding ledger: B1–B5, B7, B9, B12, C5, C6, D1–D4, E1–E4, F1, G1, G2, G4, J1), 01-repo-skeleton-runtime (Settings, `CollectorJob`/`JOB_TABLE`, `Http`/`ProviderLimiter`, `/health`, `AlertLatch`), 03-storage-schema (DDL owner, `put_metric_values`, `emit_event`, events + metric catalogs), 04-subnet-registry-and-identity (`resolved(…, 'github_repo')` candidates, `manual_overrides`, `active_occupants`), 05 (`TmcClient`, `chain_hourly.commits_30d`, startup catch-up attribute), 16 (liveness, pause groups, ops digest), 17 (`backfill_github` caller of this spec's client) · **Consumed by:** 12-feature-store-and-scoring-engine (P2 pillar, `repo_gone` / `all_repos_archived` gates), 13-validation-harness (PIT P2 factor study), 17 (replay) · **Master spec refs:** §1.3 (absence ≠ failure; identity = occupant), §2 (replace `fetch_github`/`parse_repo`), §4 (tier 0 GitHub HTML, tier 1 GitHub REST + PAT), §5, §6, §7.2, §7.3 P2, §7.5 (`repo_gone`, `all_repos_archived`), §7.7 (GitHub freshness window 2 d), §8 row 06, §10 (PIT note), §11, §12 (Phase 4).

---

## 1. Purpose and scope

The GitHub collector turns the free-text GitHub strings that subnet owners publish (on-chain `SubnetIdentitiesV3.github_repo`, subnetalpha ACF `github_url`, TMC identity, seed `fundamentals.json`) into a **numeric-id-keyed, org-wide, point-in-time record of who is building what**, and produces the ten P2 `metric_keys` of `ts-1.0`. It exists because the old collector was wrong in a way that inverted the signal: a 301 (rename) or 404 (gone) dropped the repo, `github_active` became `None`, was renormalised away, and the *largest* teams (Apex, Chutes, Gradients, Synth — all renamed; 13 of the 20 null netuids in all) scored a perfect `quality=1.0, coverage=0.7` (research `02-gapfill-github-canonical-repo-resolution.md` §6). This spec makes that impossible by construction: a repo is either **resolved** (numeric id), **verifiably absent** (placeholder / gone > grace → metric `0.0`), or **unobservable this cycle** (API failure → `None`).

**In scope**
- URL normaliser for every observed form, placeholder denylist.
- Resolver state machine keyed on numeric `repo_id` (200/301/404/403/429/5xx paths), alias history, source-disagreement detection.
- Org / user enumeration, primary-repo selection when the declared ref is org-only or gone.
- Activity collection per non-fork repo: commits (author identity, bot + merge filters), participation/contributors stats, releases + tags, merged PRs + reviews, community profile + CI/dependabot hygiene, template-originality check.
- Commit-spam filter; author identity dedupe; Electric-Capital author classification (`github_authors_daily`).
- Exact computation of the ten P2 metrics into `metric_values` with absence/failure encoding.
- Events `repo_renamed`, `repo_moved`, `repo_gone`, `repo_restored`, `repo_archived`, `org_repo_archived`, `identity_repo_changed`, `source_disagreement`, `commit_spam_suspected` (03 §3.9 names).
- The **GitHub PAT budget table** (02 §A2, §G4, including 17's backfill), the `github`/`github_html` provider rows for 01's limiter, `GitHubQuota`, ETag caching, zero-quota HTML rename detector, TMC `github-stats` cross-check via 05's `TmcClient`.

**Out of scope**
- Normalising metrics to [0,1] and pillar aggregation (spec 12 — this spec writes raw values + status).
- Reading identity strings from chain/subnetalpha/TMC and merging them (spec 04 does; this spec reads 04's `resolved(…, 'github_repo')` candidates, §3.1).
- GitHub *Search* API, GraphQL (optional optimisation, §11), GitHub Apps, private repos, org member lists (public members are a weak team-size proxy; deferred).
- Any write to GitHub, any token with scopes beyond public read.

---

## 2. Design

### 2.1 Components and data flow

```
04 resolved('github_repo')           04 active_occupants()
   ordered candidates per source           status ∈ {active, pre_launch}
          │                                       │
          ▼                                       ▼
 ┌──────────────┐   DeclaredRef    ┌─────────────────────────┐   repo_id   ┌─────────────────┐
 │ urlnorm.py   │ ───────────────► │ resolver.py             │ ──────────► │ github_repos     │
 │ normalise +  │                  │ 200/301/404/403/429/5xx │             │ github_repo_     │
 │ denylist     │                  │ keyed on numeric id     │             │   aliases        │
 └──────────────┘                  └───────────┬─────────────┘             │ github_declared_ │
                                               │ owner (canonical)         │   refs           │
                                               ▼                           │ github_occupant_ │
                                   ┌─────────────────────────┐             │   repos          │
                                   │ orgs.py                 │ ──────────► │ github_orgs      │
                                   │ /orgs|/users enumerate  │             └────────┬────────┘
                                   │ primary-repo selection  │                      │ activity scope R(o)
                                   └───────────┬─────────────┘                      ▼
                                               │                    ┌───────────────────────────┐
 ┌──────────────┐  every call      ┌────────────▼────────────┐       │ github_commits            │
 │ client.py    │ ◄─────────────── │ activity.py             │ ────► │ github_events (raw)       │
 │ via 01 Http  │                  │ commits/stats/releases/ │       │ github_activity_daily     │
 │ ETag cache   │                  │ pulls/reviews/hygiene/  │       │ github_repos.hygiene_json │
 │ 403/429/5xx  │                  │ originality             │       └───────────┬───────────────┘
 └──────────────┘                  └─────────────────────────┘                   │
                                               spam.py + authors.py              ▼
                                   ┌─────────────────────────┐       ┌───────────────────────────┐
                                   │ metrics.py              │ ────► │ github_authors_daily      │
                                   │ ten P2 metric_keys      │       │ metric_values (source=    │
                                   │ status ok/absent/failed │       │   'github')  · events     │
                                   └─────────────────────────┘       └───────────────────────────┘
 html_probe.py  (github.com HTML, provider github_html) ──► rename hints → resolver re-verify queue
 tmc_crosscheck.py (05 TmcClient, G2; chain_hourly.commits_30d) ──► collector_runs.meta_json only
```

**Jobs (01's `JOB_TABLE` mirrors this table verbatim, 02 §A2/§E2).** It replaces 01's placeholders `github_repos_6h` and `github_org_daily` (and their keys `GITHUB_REPOS_INTERVAL_MIN`/`GITHUB_ORG_INTERVAL_MIN`). All rows: `CollectorJob` subclasses (01 §2.6) run by 01's never-raising wrapper, `max_instances=1`, `coalesce=True`, `phase=4`, `paid=False`; cron jobs `min_gap_factor=0`. 17's manual `backfill_github` (provider `github`) is 17's row, budgeted in §6.

| id | factory (`app.…`) | trigger | cadence / UTC time | misfire grace s | timeout s | enabled flag | provider | notes |
|---|---|---|---|---|---|---|---|---|
| `github_resolve` | `collectors.github.jobs:GithubResolveJob` | interval | `GITHUB_RESOLVE_INTERVAL_MIN` = 360; first run after 01's boot stagger; `min_gap_factor` 0.5 | 1800 | 1800 | `GITHUB_ENABLED` | `github` | ≤ 190 req/run: resolve/re-verify every ref by id, alias/gone/archived events, source disagreement (§2.3). Soft deadline `timeout − 120 s`; unfinished refs go first next run (`last_attempt_at ASC`). Liveness 45,120 s (16, E4) |
| `github_org_enum` | `collectors.github.jobs:GithubOrgEnumJob` | cron | `GITHUB_ORG_ENUM_CRON` = 20:00 | 3600 | 1800 | `GITHUB_ENABLED` | `github` | ≈ 155 req: enumerate canonical owners, primary selection, `org_repo_archived`. Startup catch-up |
| `github_activity` | `collectors.github.jobs:GithubActivityJob` | cron | `GITHUB_ACTIVITY_CRON` = 20:30 | 3600 | 10500 | `GITHUB_ENABLED` | `github` | ≈ 3.5 k req ≈ 53 min: commits, releases/tags, PRs + reviews, stats (weekly due), hygiene (weekly due), originality (monthly due), rollups. Soft deadline = min(start + timeout − 300 s, next `GITHUB_METRICS_CRON` − 600 s); leftover repos go first next run (`note="partial: N repos left"`). Startup catch-up |
| `github_metrics` | `collectors.github.jobs:GithubMetricsJob` | cron | `GITHUB_METRICS_CRON` = 23:30 | 3600 | 1800 | `GITHUB_ENABLED` | — (DB only) | author classification + ten P2 rows (§2.8, the 00:00Z official key the 00:15Z scoring run reads); `commit_spam_suspected`. Startup catch-up with `catchup_avoid_utc=["18:00-23:30"]` (a catch-up there would take the next official key with stale inputs) |
| `github_html_probe` | `collectors.github.jobs:GithubHtmlProbeJob` | cron | `GITHUB_HTML_PROBE_CRON` = 03:00 | 3600 | 3600 | `GITHUB_ENABLED` (inside: `GITHUB_HTML_PROBE_ENABLED`, `GITHUB_TMC_CROSSCHECK_ENABLED`) | `github_html`; `tmc` via `TmcClient` | ≤ 600 HTML GETs at 1/s (0 PAT quota) + ≤ 21 TMC req (G3). No catch-up (advisory). Member of 16's `github` **and** `tmc` pause groups |

**Startup catch-up** uses the `JobSpec` attributes `startup_catchup` / `catchup_avoid_utc` that 05 §6.1 asks 01 to add: at boot, a cron job whose `job_state.last_ok` predates its most recent scheduled fire runs at boot + 300 s (shifted past its avoid window); without it a power-off loses APScheduler's in-memory schedule (master §11). Ordering guarantees the official 00:15Z scoring run (master §3) sees metrics observed ≤ 45 min earlier; all jobs are watermark-based and idempotent. Pause and liveness: §5, §7.

### 2.2 URL normaliser (`urlnorm.py`)

`normalise(raw: str) -> list[DeclaredRef]` — returns every GitHub reference found in the string (owners publish comma/space-separated lists), deduplicated by `key`, first occurrence first.

```python
@dataclass(frozen=True)
class DeclaredRef:
    kind: Literal["repo", "owner"]
    owner: str            # as written (case preserved for display)
    repo: str | None      # None for owner-only
    key: str              # f"{owner.lower()}/{repo.lower()}" or owner.lower()
    raw: str
    placeholder: bool     # True → verifiable absence
```

Algorithm (order matters):
1. Match case-insensitively (owner/repo keep their original case for display; only `key` is lower-cased) and strip the host part: accept `https?://`, no scheme, `www.`, `git@github.com:`, `api.github.com/repos/`, `raw.githubusercontent.com/`. `{o}.github.io[/…]` → `owner` ref for `{o}`. Anything without `github.com`/`githubusercontent.com` → no ref (empty list).
2. Split path on `/`, drop empty segments, strip `?…` and `#…` from every segment (`?tab=readme-ov-file` — netuids 6, 13, 100), strip a trailing `.git` from the second segment (netuid 47), tolerate trailing slashes (20, 126).
3. Route reserved first segments: `orgs/{o}[/repositories|/projects|/…]` → `owner` ref `{o}` (netuids 105, 118); `users/{u}` → `owner`; `topics`, `sponsors`, `settings`, `features`, `marketplace`, `explore`, `login`, `apps`, `about`, `pricing`, `site`, `enterprise`, `collections`, `trending`, `events`, `security`, `search`, `new`, `notifications` → no ref.
4. One segment → `owner` ref. Two or more → `repo` ref from the first two; segments after (`tree/main/…`, `blob`, `releases`, `issues`, `wiki`, `pulls`) are ignored.
5. Validate: owner `^[a-z0-9](?:[a-z0-9-]{0,38})$`, repo `^[a-z0-9._-]{1,100}$` (on the lower-cased forms); invalid → no ref.
6. Placeholder denylist (exact match on lower-cased parts) → `placeholder=True`: full pairs `username/repo`, `owner/repo`, `deprecated/deprecated`, `org/repo`, `user/repo`, `example/example`; owners `username`, `user`, `owner`, `org`, `yourname`, `your-org`, `example`, `deprecated`, `placeholder`, `tbd`, `n-a`; repos `repo`, `repository`, `deprecated`, `tbd`, `todo`, `placeholder`, `example`, `coming-soon`, `your-repo`, `subnet-template-placeholder`. `GITHUB_EXTRA_DENYLIST` (comma-separated keys) is appended at runtime. A candidate that 04 marked `is_junk` (04 §2.6: e.g. `deprecated/deprecated`, `username/repo`) is a placeholder too.

Port from the old bot only the *idea* of `_GH_RE` (`fundamentals_collect.py:163-177`); the old function's rejection of `orgs` as an owner (line 175) is the defect that dropped netuids 105 and 118 and must not be reproduced.

### 2.3 Resolver state machine (`resolver.py`)

One row per (`occupant_key`, `source`) in `github_declared_refs`; state transitions run every `github_resolve` cycle. Only occupants with `status ∈ {active, pre_launch}` are processed; on `netuid_reoccupied` the new `occupant_key` starts with no rows.

```
                 ┌──────────── placeholder ────────────┐ (terminal; absence)
 declared ──►normalise──► no ref ──► none (terminal; absence)
                 │
                 ├── kind=repo ──► GET /repos/{o}/{r}  (follow_redirects=False, If-None-Match)
                 │       200 / 304 ──► resolved(repo_id)     method=direct
                 │       301/302/307/308 ──► Location=/repositories/{id} ──► GET /repositories/{id}
                 │                       200 ──► resolved(repo_id)   method=redirect
                 │                              alias(from,to); repo_renamed | repo_moved
                 │                       404 ──► gone_pending
                 │       404 ──► gone_pending(first_404_at) ──► org enumeration of {o}
                 │       403 rate-limited ──► (client sleeps to reset, retries; state unchanged)
                 │       403 other / 451 ──► blocked (retry next cycle; counts as gone for grace)
                 │       429 / 5xx / network ──► unchanged (failure counted; last-good kept)
                 │
                 └── kind=owner ──► org enumeration ──► primary selected ──► resolved(repo_id) method=org_enum
                                                   └──► org has 0 usable repos ──► org_empty (absence after grace)

 resolved(repo_id) ──every cycle──► GET /repositories/{id}
         200: full_name changed ──► alias + repo_renamed/repo_moved; archived flip ──► repo_archived
         404 ──► gone_pending on the id (deleted or made private)
 gone_pending ──2 consecutive cycles 404──► gone (repo_gone event) ──► org fallback → primary, else absence after GITHUB_GONE_GRACE_DAYS
 gone ──any later 200 on the id──► resolved (event repo_restored, info)
```

Rules:
- **Never resolve by name once an id is known.** Re-verification is always `GET /repositories/{id}`; the name is only used for the first contact and for HTML probing.
- **301 handling** (302/307/308 treated the same). `Location` must match `^https://api\.github\.com/repositories/(\d+)$`; anything else is logged `resolver.unexpected_location` and treated as a failure (state unchanged). `repo_renamed` when `owner.lower()` is unchanged, `repo_moved` otherwise (netuid 64 `rayonlabs/chutes → chutesai/chutes`, id 884216433 constant).
- **404 is not final on first sight.** `gone_pending` requires the 404 to be reproduced on the next cycle (≥ 6 h later) before `repo_gone` is emitted; `first_404_at` is recorded at the first 404 and drives the > 14 d absence rule. Never retry a 404 within the same cycle (master §11).
- **403 with `x-ratelimit-remaining: 0`** is handled by the client through `GitHubQuota` (sleep to `x-ratelimit-reset` + 2 s, capped at 3,700 s, then retry the same request). The batch is never dropped; only the run's soft deadline (§2.1) ends a cycle early — a wait that would pass it raises `QuotaDeferred`, the run returns ok with a `partial` note, and the remaining refs go first next cycle (`last_attempt_at ASC`, so nothing starves).
- **Declared ref = 04's precedence, not resolvability.** Input is 04's `resolved(db, occupant_key, 'github_repo')` (04 §3.3): the winner plus `candidates` (04 R7 `candidates_json`: `source, value, hash, is_junk, demoted, suppressed`) in 04's order — a `force` override first (R2), then R1 `chain > subnetalpha > tmc_coldkey > manual > legacy` with R4's stale-editorial demotion (master §5). The declared ref is the first candidate that is not junk, not suppressed (R3) and that `urlnorm` accepts as a non-placeholder ref — normally 04's winner. A **dead** declared repo is *not* replaced by a lower-precedence source's live repo (editorial sources lag and, after re-occupancy, can name the previous team's repo): it falls back only to org enumeration of its own owner (§2.4), then to absence after `GITHUB_GONE_GRACE_DAYS` (master §7.3). The operator redirects a dead owner-signed value with a 04 `suppress` or `force` override (SN117's 404 chain repo today). Sources map to `github_declared_refs.source` as `chain, subnetalpha, tmc_coldkey→tmc, manual, legacy→seed_fundamentals` (03's CHECK); per source the first candidate (main field before `__extra_*`) and, within a multi-URL string, its first `repo` ref (else first `owner` ref) is used. `declared_source` is recorded in `github_occupant_repos`.
- **Comparison is by resolved `repo_id`**, never by string: `datura-ai/desearch` (netuid 22's chain string, the pre-rename name) and `Desearch-ai/subnet-22` are one id and agree. Every non-junk source ref is resolved (≤ 176 refs, §6) for this purpose.
  - *Same id, different strings* (a source lagging a rename): no disagreement and nothing to write — 06 is the only emitter of `source_disagreement` for `github_repo` (04 R6 skips that field, 02 §D2).
  - *Different ids* among ≥ 2 resolved sources: emit `source_disagreement` (§3.4; netuid 20 today: `witnessvision/witness_subnet` 1360186689 vs `chronoseek/bittensor-subnet` 1183382351). An unresolvable source is not a group (SN117's 404 chain repo is a `repo_gone` case, not a disagreement). A group backed **only by `legacy`** (the frozen old-bot `fundamentals.json`) is not a group either (added 2026-10-17: on the first live run 17 of 25 disagreements were legacy-only — pre-rename or superseded repos of the same team, which 04 R4 already treats as stale editorial); `legacy` still joins a group that a live source backs.
  - *Owner-only refs of non-declared sources* on the canonical owner (subnetalpha's `github.com/Beam-Network` beside chain's `orgs/Beam-Network/repositories`) take the owner listing's outcome (`resolved` with the primary / declared id, `org_empty`, `gone`) without events; owner refs naming *another* owner are never enumerated and stay `new`.
- **`identity_repo_changed`** fires when the declared `repo_id` changes between cycles (a new string resolving to another id, or the declared source switching after a 04 override). A string change that resolves to the same id (e.g. `CortexLM/platform → CortexLM/cortex`, id 972956783) is *not* an identity change here; 04's `identity_changed` still records the string.
- **`repo_gone` and its grace clock** apply to the declared ref only; lower-precedence refs keep their state for the disagreement check without events.
- **Canonical owner** for org enumeration = owner of the declared repo's canonical `full_name` (`gradients-ai`, not `rayonlabs`), or the declared ref's owner as written when it is owner-only or never resolved (`qbittensor-labs`). Owners of other sources are *not* enumerated (would merge two teams' activity).
- **Reading 04's candidates** (`identity_resolved.candidates_json`; verified against the live DB 2026-10-17). The JSON carries `source, field, value, value_norm, ref_kind, hash, is_junk, demoted, suppressed, observed_at, override_id` but not 04's in-memory `order`, so 06 derives it from `field`: `github_repo` → 0, `github_repo__extra_{i}` → 1 + i, an alternate field (TMC's `owner_github`) → 100. Ranking is 04's `pick()`: a `force` override first, then (`demoted` → last, else 04's `SOURCE_RANK`), `order`, newest `observed_at`. A legacy value appears twice (the observation and its `fallback` override, e.g. override 369); candidates are deduplicated per (source, `value_norm`). `raw` = `value` (the owner's string, e.g. `…/orgs/Beam-Network/repositories`) is what `urlnorm` parses.
- **First sight is not a change** (prevents a day-1 page storm: `identity_repo_changed` routes *immediately* and ≈ 120 occupants resolve on the first run). `identity_repo_changed` needs a previous non-null declared `repo_id` for the occupant; `repo_archived` and `org_repo_archived` need a previous observation of the repo with `archived=0`. A redirect on first contact still records the alias and emits `repo_renamed`/`repo_moved` (stored / digest only), because that is evidence the published string is stale.

### 2.4 Org enumeration and primary selection (`orgs.py`)

`GET /orgs/{owner}/repos?type=public&sort=pushed&per_page=100` (page 2 only when `Link: rel="next"` and `GITHUB_ORG_MAX_REPOS` allows); on 404 fall back to `GET /users/{owner}/repos?type=owner&sort=pushed&per_page=100`; both 404 → `github_orgs.kind='gone'`. Once `github_orgs.kind` is known, the matching endpoint is called directly (the fallback runs only when the kind is unknown or its endpoint 404s), so user owners cost no daily `/orgs` 404. Every listed repo is upserted into `github_repos` (`resolution_method='org_enum'` on new rows only, `first_seen_at` preserved) and linked to the occupant in `github_occupant_repos` with `role='org'`. Per owner, store `n_public, n_non_fork, n_archived, n_pushed_30d, max_pushed_at_non_fork` (over non-fork ∧ non-archived).

**Primary-repo selection** (only when the chosen declared ref is owner-only, `gone`, `blocked`, or `org_empty` after a later listing shows repos):

```python
def pick_primary(repos: list[RepoRow], netuid: int, subnet_name: str, now: int) -> RepoRow | None:
    cands = [r for r in repos if not r.is_fork and not r.archived]
    if not cands: return None
    def score(r):
        text = f"{r.name} {r.description or ''}"
        b = 0
        m = re.search(r"\bsn\s?(\d+)\b|\bnetuid\s?(\d+)\b|\bsubnet[\s-]?(\d+)\b", text, re.I)
        if m and int(next(g for g in m.groups() if g)) == netuid: b += 4      # "SN 48" vs "SN 63" (qbittensor-labs)
        if re.search(r"subnet|bittensor|validator|miner|netuid", text, re.I): b += 2
        tok = _name_token(subnet_name)                                        # ≥4 alnum chars, lower
        if tok and tok in text.lower(): b += 1
        age_d = (now - r.pushed_at) / 86400 if r.pushed_at else 10**6     # INTEGER unix s (02 §B1); now injected
        fresh = 3 if age_d <= 7 else 2 if age_d <= 30 else 1 if age_d <= 90 else 0
        return (b + fresh, r.pushed_at or 0, r.stars or 0, -r.repo_id)   # None-safe, deterministic tie-break
    return max(cands, key=score)
```

The selected repo gets `role='primary'` in `github_occupant_repos`; the previous primary (if different) keeps `role='org'` and an `info` log line records the switch. `org_repo_archived` is emitted (severity `warning`, one row per occupant linked to the owner) when a repo of the owner flips `archived` 0→1 between listings, payload flag `is_subnet_related` = name/description matches the bonus regex (Ditto's `ditto-platform/ditto-screener/dittobench-starter-kit` on 2026-08-11).

**Activity scope** `R(o)` for occupant `o` = {declared repo if resolved and alive} ∪ {primary} ∪ top-`(GITHUB_ACTIVITY_MAX_REPOS_PER_OCCUPANT − |declared ∪ primary|)` non-fork, non-archived org repos ordered by `pushed_at DESC` with `pushed_at ≥ now − GITHUB_ACTIVITY_LOOKBACK_DAYS`. Forks are excluded (Santiment/Electric-Capital rationale: forks inherit history), *except* a declared repo that is itself a fork (e.g. the legacy ref `helionlink/Chi`, which 301s to the fork `meowjet/Chi` 1225878339 — research §3 row 117), which is included with commits counted only when `author_date > repo.created_at` and `originality = 0.0`. `github_occupant_repos.in_scope` is rewritten each org-enum run. **User owners** (`github_orgs.kind='user'`, decided 2026-10-17 — §11 item 14): a personal account's other repos enter `R(o)` only when their name/description matches the subnet bonus regex of `pick_primary` (`subnet|bittensor|validator|miner|netuid|sn\d+`) or the subnet-name token; a developer's unrelated hobby repos must not count as subnet building. Organisations keep the org-wide scope.

### 2.5 Activity collection (`activity.py`)

Per repo in any occupant's scope, one pass per `github_activity` run. **All per-repo paths are id-based**, `…` = `/repositories/{repo_id}` (verified 2026-10-17: `/repositories/884216433/{commits,releases,pulls,tags,community/profile,readme,stats/participation,git/trees/HEAD}` all 200, while the pre-rename `/repos/rayonlabs/chutes/commits` answers 301 → `/repositories/884216433/commits`), so a rename between two resolve cycles cannot break or redirect activity collection.

| call | params | stored → | notes |
|---|---|---|---|
| `GET …/commits` | `since={watermark}&per_page=100`, pages ≤ `GITHUB_ACTIVITY_MAX_COMMIT_PAGES` | `github_commits` (INSERT OR IGNORE by sha) | watermark = `last_commit_seen_at − 2 d` (first run: `now − GITHUB_ACTIVITY_LOOKBACK_DAYS`). Default branch only. Fields: `sha`, `commit.author.{name,email,date}`, `commit.committer.date`, `author.login`, `author.type`, `len(parents)`, first line of `commit.message`, `commit.verification.verified`. `409` (empty repo) → 0 rows, not a failure. A 404 → the repo is queued for re-verify (`github_declared_refs.last_attempt_at = NULL` for its refs) and counts as a failed pass. |
| `GET …/stats/participation` | — | `github_repos.participation_json` (52 weekly `all`/`owner`) | **weekly** (`stats_checked_at` older than `GITHUB_STATS_INTERVAL_DAYS` = 7). `202` → wait `GITHUB_STATS_202_WAIT_SEC`, retry ≤ `GITHUB_STATS_202_RETRIES`; still 202 → keep last-good, not a failure; `204` (empty repo) → empty. Cross-check only (no metric reads it), hence weekly: daily refresh cost ≈ 1,260 req/day for no metric effect. |
| `GET …/stats/contributors` | — | `github_repos.contributors_json` (top-100 `{login, total}`) | weekly with participation; same 202 rule; cross-check for bus factor on repos where commit pages were capped. |
| `GET …/releases` | `per_page=50` | `github_events` kind `release` (`event_at=published_at`, payload `{tag_name, prerelease, draft}`; drafts skipped) | dedupe key `release:{repo_id}:{tag_name}` |
| `GET …/tags` | `per_page=50` | `github_events` kind `tag` (`event_at` from `GET /commits/{sha}` **only** if the tag is new and not already covered by a release with the same `tag_name`; else omitted) | tags carry no date; cap 5 commit look-ups per repo per run (the rest wait for the next run); dedupe key `tag:{repo_id}:{tag_name}` |
| `GET …/pulls` | `state=closed&sort=updated&direction=desc&per_page=50` | `github_events` kind `pr_merged` (`event_at=merged_at`, payload `{number, author_login, review_count, reviewers}`) | only PRs with `merged_at ≥ now − GITHUB_PR_LOOKBACK_DAYS`, ≤ `GITHUB_PR_MAX_PER_REPO` newest; `review_count` from `GET …/pulls/{n}/reviews` (states `APPROVED`, `CHANGES_REQUESTED`, `COMMENTED` by a user ≠ PR author, non-bot) — fetched once per PR (stored PRs are not re-fetched). Dedupe key `pr_merged:{repo_id}:{number}` (02 §J1; identical to 17's backfill key, so neither writer duplicates a PR). |
| `GET …/community/profile` | — | `hygiene_json.{license, readme, contributing, code_of_conduct}` from `files.*` non-null; `security` from `GET …/contents/SECURITY.md`, then `…/contents/.github/SECURITY.md` (404 → false), **only when `contributing` is false** (the item is `security_or_contributing`) | weekly (`hygiene_checked_at` older than `GITHUB_HYGIENE_INTERVAL_DAYS`); declared + primary repos only. Verified 2026-10-17: `files` = `{code_of_conduct, code_of_conduct_file, contributing, issue_template, license, pull_request_template, readme}` — there is no `security` key |
| `GET …/readme` | — | `hygiene_json.readme_bytes = size` | 404 → 0 |
| `GET …/contents/.github/workflows` | — | `hygiene_json.ci = any(name endswith .yml/.yaml)` | 404 → false |
| `GET …/contents/.github/dependabot.yml`, `GET …/contents/renovate.json` (+ `.github/renovate.json`) | — | `hygiene_json.dep_update = any 200` | 404 → false; stop at first 200 |
| `GET /repos/{template}/git/trees/{default_branch}?recursive=1` (once per run, cached) and `GET …/git/trees/{default_branch}?recursive=1` | — | `originality_json = {repo_files, identical_files, identical_share, truncated, template_sha}` | monthly (`originality_checked_at`), primary/declared only; `truncated=true` → compute on returned entries. If `repo.fork` and `parent.full_name == template` also `GET …/compare/{template_owner}:{template_default}...{default_branch}` → `ahead_by` |
| sampled `GET …/commits/{sha}` | ≤ `GITHUB_COMMIT_STATS_SAMPLE` per repo per run | `github_commits.stats_total`, `whitespace_only` | fetched at insert time, only for new commits that the insert batch already flags `burst` or `identical_msg` (§2.6), so both columns are part of the INSERT (`github_commits` is append-only, 03) |

Every raw row carries `observed_at = now` and `run_id` (INTEGER times, 02 §B1; GitHub ISO strings → `parse_iso`); commit rows carry `event_at = author_date`, `source='github_api'`, `backfilled=0`. A repo's pass is **successful** when its commit page(s) answered (200/409); then `github_repos.activity_ok_at = now` and the watermark advance commit together — `activity_ok_at` is what the `activity_stale` rule of §2.8 reads (migration 0004, §3.2). After the pass, `github_activity_daily` is rebuilt for every (`repo_id`, `day_ts`) touched by new commits (REPLACE — it is a rollup; the PIT record is `github_commits`; `commits_spam` uses the read-time flags of §2.6). Repo-level `github_events` kinds (`archived, unarchived, renamed, transferred, gone, restored, default_branch_changed`) are written by the resolver/org enum on transitions with dedupe key `{event_kind}:{repo_id}:{detail}` (detail = new `full_name`, new default branch, or the UTC `day_ts`).

### 2.6 Commit-spam filter (`spam.py`) and author identity (`authors.py`)

`flag_commits(commits: list[CommitRow], repo: RepoRow) -> list[CommitRow]` is pure and deterministic over stored fields. At insert time it sets `is_bot`, `is_merge` (intrinsic, final) and `spam_flags` as seen within the fetch batch; `github_commits` is append-only (03 `APPEND_ONLY_TABLES`), so these columns are **never updated**. The authoritative `burst`/`identical_msg`/`tiny`/`whitespace_only`/`pre_fork` classification is recomputed by `flag_commits` at read time over each window (metrics, rollups, `commit_spam_suspected`), so late-arriving commits can still complete a burst or an identical-message group; commits without sampled stats are never `tiny`/`whitespace_only`.

| flag | rule | effect on counts |
|---|---|---|
| `is_bot` | `author.type == "Bot"`, or login ends with `[bot]`, or login ∈ `BOT_LOGINS` ∪ `GITHUB_BOT_LOGINS_EXTRA` (`dependabot`, `dependabot-preview`, `github-actions`, `renovate`, `imgbot`, `snyk-bot`, `codecov`, `allcontributors`, `pre-commit-ci`, `mergify`, `github-merge-queue`, `semantic-release-bot`, `devin-ai-integration`, `copilot-swe-agent`), or author name contains `[bot]`, or email matches `^\d+\+.*\[bot\]@users\.noreply\.github\.com$` | excluded from all human counts |
| `is_merge` | `len(parents) ≥ 2` | excluded (mergers are not authors — Electric Capital) |
| `burst` | > `GITHUB_SPAM_BURST_N` (10) commits by the same `author_key` inside any `GITHUB_SPAM_BURST_SEC` (60 s) window, sorted by `author_date`; the first commit of each burst stays unflagged | flagged commits excluded from `commits_human`; the day still counts as a commit-day via the unflagged first commit |
| `identical_msg` | same `author_key` and same normalised message (lower, whitespace-collapsed, digits stripped) ≥ 5 times within 7 d; first per day unflagged | as above |
| `tiny` | sampled `stats.total < GITHUB_SPAM_MIN_STATS_TOTAL` (5) | excluded from `commits_human` |
| `whitespace_only` | sampled: every `files[].patch` is empty after stripping whitespace-only hunks (lines whose `+`/`−` content differs only in whitespace) | excluded |
| `pre_fork` | declared-fork repo and `author_date ≤ repo.created_at` | excluded |
| repo-level `force_push_suspected` | `pushed_at − max(committer_date on default branch) > 6 h` **and** the commit page returned 0 new shas since the last run while `pushed_at` advanced | logged on `github_repos`, no count effect (pushed_at also bumps on other branches); feeds `commit_spam_suspected` payload only |

`commit_spam_suspected` (severity `warning`) is emitted by `github_metrics` when, over the trailing 28 d and across `R(o)`, flagged human commits ≥ 50 % of human commits **and** human commits ≥ 20; dedupe key `occ|{d30}` (03 §3.9).

**Author identity** `author_key(commit) -> str`:
1. `author.login.lower()` when present;
2. else the login parsed from a GitHub noreply email `^(?:\d+\+)?([a-z0-9-]+)@users\.noreply\.github\.com$`;
3. else `email:` + first 16 hex of `sha1(email.strip().lower())` — raw emails are **never stored** (`author_email_hash` = sha256 of the lower-cased email, 03).

Whenever one commit carries both a login and an email, `github_author_identities(alias_key=email:…, canonical_key=login)` is upserted, and all keys are mapped through this table at read time (`resolve_author_keys`). This collapses the common "same person, git email vs web-flow login" split without any external identity service.

### 2.7 Electric-Capital classification (`github_authors_daily`)

For occupant `o`, run day `D = floor_day(now)` (INTEGER), human commits `C(o)` = rows of `github_commits` with `repo_id ∈ R(o)`, `is_bot=0`, `is_merge=0` and no read-time spam flag (§2.6; the kept first commit of a burst or identical-message group carries none), keys resolved through `github_author_identities`:

- `cd(a, [t0,t1))` = number of distinct UTC dates of `author_date` for author `a` in the window.
- `w1 = cd(a,[D−28,D))`, `w2 = cd(a,[D−56,D−28))`, `w3 = cd(a,[D−84,D−56))`, `cd84 = cd(a,[D−84,D))`.
- `dev_class = 'full_time'` if at least two of `{w1,w2,w3} ≥ GITHUB_FTE_COMMIT_DAYS_28D` (10); else `'part_time'` if `cd84 ≥ 3`; else `'one_time'`.

This is the operationalisation of master's "≥ 10 commit-days in a rolling 28-d window, sustained over 84 d" that 03's columns encode. One row per (`occupant_key`, `author_key`, `day_ts = D`) for every author with `cd84 ≥ 1`, columns `commits_today, cd28=w1, cd56=w2, cd84_3=w3, cd84, dev_class, repos_json, observed_at, run_id, backfilled` (0 live; 17 sets 1). Rows are append-only per day (re-running the same day REPLACEs the same-day rows only).

### 2.8 Metric computation (`metrics.py`)

Windows end at `now` (the run instant = `observed_at`, INTEGER); `D = floor_day(now)`; all reads use rows with `observed_at ≤ now` (trivially true live). **`as_of` = the first scoring slot ≥ `now`** (slots 00/06/12/18:00Z, 02 §B4; `slot_for(now) = -(-now // 21600) * 21600`): the scheduled 23:30Z run keys the next day's official 00:00Z row (age 0 at the 00:15Z run); a catch-up or manual run keys the next provisional slot. Universe: 04's active and pre-launch occupants, netuid 0 excluded. Human commit set `C(o)` and repo set `R(o)` as in §2.7; `declared` = the `github_occupant_repos.role='declared'` row whose repo is alive (`status='ok'`), else `primary`.

**Replay** (17's `replay_history`, `now` in the past, `replay=True`): `github_commits`/`github_events` rows with `backfilled=1` are admitted regardless of `observed_at` (12's reconstructed-mode rule); `pushed_at`/`archived` come from the latest `github_repo_snapshots` row with `observed_at ≤ now` and `hygiene_json`/`originality_json` only when checked ≤ `now` — otherwise that metric is `failed`, `reason='no_pit_snapshot'` — except the two freshness keys, which then use 13 §2.5's approximation: days since the last `github_commits.event_at ≤ now` over the declared repo (`declared_repo_freshness`) or over `R(o)` incl. a declared-gone repo (`org_freshness`, always in replay: its live path reads today's `pushed_at`, which is look-ahead), `value_json.approx = 'last_commit'`, `failed`/`no_pit_commit` when there is none (amended 2026-10-22, 20 N2 item 2); if 17's `coverage(repo_id, window_start) -> bool` is False for any repo in `R(o)`, the five activity metrics are `failed`, `reason='backfill_incomplete'` (17 §6 replay contract). Replay rows carry `backfilled=1` and `value_json.backfilled=true` (02 §J1); membership is today's (13/17 caveat `github_org_membership_asof_today`).

| metric_key | raw `value` | status rules |
|---|---|---|
| `active_authors_28d` | count of distinct author keys with `w1 ≥ 1` | absent→`0.0` (§4); failed→None (`activity_stale`) when `github_repos.activity_ok_at` of every repo in `R(o)` is NULL or older than `now − GITHUB_ACTIVITY_STALE_DAYS` |
| `fte_devs_84d` | count of `dev_class='full_time'` rows for `D` | as above |
| `commit_days_90d` | count of distinct UTC dates with ≥ 1 commit in `C(o)` over `[D−90, D)` (org-wide days, max 90) | as above |
| `declared_repo_freshness` | `(now − declared.pushed_at) / 86400` (float days), using the latest verified `pushed_at` (`last_verified_at` = last *definitive* answer, §3.2) | absent→`0.0` when no ref / placeholder / `gone` with `first_404_at ≤ now − GITHUB_GONE_GRACE_DAYS` and no primary; when a primary exists the primary's `pushed_at` is used and `value_json.fallback='org_primary'`; failed→None when `last_verified_at < now − 2 d` |
| `org_freshness` | `(now − max pushed_at over github_repos where owner ∈ owners(o), is_fork=0, archived=0) / 86400`; if every non-fork repo is archived, the max over the archived ones (status `ok`, `all_archived=true`); if the owner is a user with no other repos, the declared repo's `pushed_at` | absent→`0.0` when no ref/placeholder/org gone/empty; failed→None when `github_orgs.last_ok_at < now − 2 d` for every owner. `value_json` carries `{n_non_fork, n_archived, all_archived, declared_archived, successor_repo_id}` for 12's `all_repos_archived` gate: `all_archived` = `n_non_fork ≥ 1` and every non-fork repo of `owners(o)` archived; `successor_repo_id` = `pick_primary` over the non-archived non-fork repos of `owners(o)` when the declared repo is archived, else null |
| `release_cadence_90d` | count of `github_events` kind ∈ {release, tag} across `R(o)` with `event_at ≥ now − 90 d`, deduped by `(repo_id, tag_name)`; a tag whose date was never looked up (`event_at` NULL, §2.5 cap) is not counted | absent→`0.0`; failed→None when activity stale |
| `review_practice_90d` | `merged_with_review / merged`, both over `pr_merged` events in `R(o)` with `event_at ≥ now − 90 d`, excluding PRs whose `author_login` is a bot (`authors.is_bot`) | `merged = 0` → `status='not_applicable'` (None, master: "0 PRs → None"); absent→`0.0`; failed→None when activity stale |
| `bus_factor` | smallest `k` such that the top-`k` authors by human commits in 90 d reach ≥ 50 % of human commits (`k = 0` when no commits) | absent→`0.0`; failed→None when activity stale. Spec 12 maps ≥3→1, 2→0.6, 1→0.3, 0→0 |
| `repo_hygiene` | mean of the observed items among `{license, readme_bytes ≥ 500, ci, security_or_contributing, dep_update}` on the declared/primary repo; `value_json.items` = `{license, readme_500, ci, security_or_contributing, dep_update}` → `bool` or `null` (unobserved), the dict 12's `checklist` reads | absent→`0.0`; `< 3 of 5` items observed → None (failed); `hygiene_checked_at < now − 14 d` → None |
| `originality` | `1 − identical_share` where `identical_share = identical_files / max(repo_files, 1)` from `originality_json`; if the repo is a fork of the template with `ahead_by == 0` → `0.0`; declared-fork (non-template) → `0.0` | absent→`0.0`; `originality_checked_at < now − 45 d` or missing → None |

**Workbench hand-off (19 §2.5, added 2026-10-17).** 19's `wb_github_probe` writes the evidence key `repo_pushed_days` only while `GITHUB_TOKEN` is empty, so setting the PAT would leave the workbench's "Push d" column blank. `github_metrics` therefore also writes `repo_pushed_days` (role `evidence`, `source='github'`, `value_json.def='wb-1'`) for every occupant in the same `put_metric_values` call, with 19's semantics and 06's resolver data: `as_of = floor_day(now)` (19's key), value = days since the declared repo's (else the primary's) verified `pushed_at`, `value_json.{repo, pushed_at (ISO), archived, stars, kind: "repo"|"org_latest"}`; no ref/placeholder → `absent` (`reason='no_github_link'`); declared repo gone → `failed` (`reason='http_404'`, 19's vocabulary). First success wins against the probe's row of the same day.

Rows go **only** through `app/storage/pit.py::put_metric_values(rows)` (02 §B4), one call per run: `(occupant_key, metric_key, as_of, spec_version='ts-1.0', stage='raw', value, value_json, observed_at=now, source='github', run_id, backfilled)`. **First success wins**: a key already `ok`/`absent`/`not_applicable` is never replaced (a re-run for the same slot is silently ignored); only a `failed` row is replaced by a later observation. `status` lives only in `value_json.status ∈ {ok, absent, failed, not_applicable}` (02 §B3; never a `status` column) with `value_json.reason` on every non-`ok` row (`no_github_url`, `placeholder`, `repo_gone`, `gone_grace`, `api_outage`, `no_token`, `bad_token`, `activity_stale`, `never_observed`, `no_pit_snapshot`, `backfill_incomplete`) and metric-specific detail; 03's CHECKs: `ok` ⇒ `value` NOT NULL, `failed`/`not_applicable` ⇒ `value` NULL. **Contract with spec 12 (§2.3):** `absent` → 0.0; `ok` → the master §7.2 normaliser of `value`; `failed`/`not_applicable` → None. 06 registers no producer fns (02 §C5 does not apply: 12 lists 06 as a collector that writes its own rows). The collector never writes a fabricated number to obtain a 0.

---

## 3. Data contract

### 3.1 Tables read

| table / API (owner) | columns / call | use |
|---|---|---|
| 04 `active_occupants(db, include_pre_launch=True)` | `occupant_key, netuid, name, status` | universe = `status ∈ {active, pre_launch}`, netuid 0 (root) excluded; `netuid`/`name` for primary selection |
| 04 `resolved(db, occupant_key, 'github_repo')` (over `identity_resolved`, `identity_observations`, `manual_overrides`; read only) | winner `value, source, status, override_id`; ordered `candidates` (`source, value, hash, is_junk, demoted, suppressed`) | declared ref and per-source refs (§2.3). The field is `github_repo` (04's vocabulary: chain `github_repo`, subnetalpha `github_url`, TMC coldkey `github_repo`/`owner_github`, manual, legacy `fundamentals.json`). `status ∈ {absent, junk_only}` → verifiable absence; `unknown` → `failed`, `never_observed` |
| `chain_hourly` (05) | latest `commits_30d` per netuid | TMC cross-check (§8 `tmc_crosscheck.py`) without a TMC request |

`netuid_reoccupied` needs no handling: a new `occupant_key` simply has no GitHub rows yet; the old occupant's rows are frozen.

### 3.2 Tables written (DDL owner: 03 §3.4)

03 holds the final DDL (schema v1) for every table below; this spec ships no `CREATE TABLE`/`ALTER TABLE` (02 §B7). All timestamps are INTEGER unix seconds UTC (02 §B1): GitHub's ISO strings are converted with `parse_iso` when a response is parsed; ISO appears only in `/health`, logs and CLI output. Column names are 03's (`size_kb`, `day_ts`, `payload_json`, `source`, `backfilled`).

| table (03 class) | key | writers | rules |
|---|---|---|---|
| `github_repos` (state) | `repo_id` | resolver, org enum, activity (stats/hygiene/originality JSON) | all repo fields of the API payload (`full_name, owner` lower-cased, `name, owner_type, is_fork, parent_full_name, archived, disabled, visibility, pushed_at, created_at, updated_at, default_branch, stars, forks, open_issues, subscribers, license, size_kb, description, topics_json, etag`); `resolution_method ∈ {direct, redirect, onchain, org_enum, manual, html}` set on first contact and never overwritten; `status ∈ {ok, gone, blocked}`, `consecutive_404`, `gone_since`; `last_verified_at` = last **definitive** answer (200/304/404/451/blocked 403 — transport/5xx failures do not advance it); `last_ok_at` = last 200/304; `first_seen_at` = PIT membership start; `last_commit_seen_at` = commits watermark; `force_push_suspected`; `participation_json, contributors_json, hygiene_json, hygiene_checked_at, originality_json, originality_checked_at`; **migration 0004 (03, additive, 2026-10-17):** `activity_ok_at` (last successful activity pass, §2.5 — the `activity_stale` clock, which no v1 column carried) and `stats_checked_at` (weekly `/stats/*`, §2.5) |
| `github_repo_snapshots` (append) | `(repo_id, observed_at)` | resolver, org enum | written when a tracked field changed and at least daily per repo; `event_at = pushed_at`; the PIT source of `pushed_at`/`archived` for replays (§2.8) |
| `github_repo_aliases` (state) | `(repo_id, full_name)` | resolver, HTML probe | `full_name` lower-cased; `source ∈ {api, redirect, html, declared}` |
| `github_orgs` (state) | `owner` (lower) | org enum | `kind ∈ {org, user, gone}`; `n_public, n_non_fork, n_archived, n_pushed_30d, max_pushed_at_non_fork` (over non-fork ∧ non-archived), `etag`, `last_enumerated_at, last_ok_at, consecutive_failures` |
| `github_declared_refs` (state) | `(occupant_key, source)` | resolver | `source` mapped per §2.3; `state ∈ {new, resolved, gone_pending, gone, blocked, org_empty, placeholder, none}`; `first_404_at` drives the 14-d grace |
| `github_occupant_repos` (state) | `(occupant_key, repo_id)` | resolver, org enum | `role ∈ {declared, primary, org, declared_gone}`, `declared_source`, `in_scope` (rewritten each org-enum run), `first_seen_at` = look-ahead marker for 13/17 |
| `github_commits` (append-only) | `sha` (global: a commit in two repos is stored once) | activity; 17 (`backfilled=1`) | `INSERT OR IGNORE`, never updated (§2.6); `event_at` = author date; `message_head` ≤ 200 chars; `source='github_api'`; `backfilled=0` live |
| `github_author_identities` (state) | `alias_key` | activity | email-key → login-key map (§2.6) |
| `github_activity_daily` (rollup) | `(repo_id, day_ts)` | activity; 17 | REPLACE per key; `day_ts` = UTC day of the author date; `backfilled` |
| `github_authors_daily` (rollup) | `(occupant_key, author_key, day_ts)` | metrics; 17 | §2.7; same-day re-runs REPLACE that day's rows only |
| `github_events` (append) | `id`; `dedupe_key` UNIQUE | activity, resolver, org enum; 17 | `INSERT OR IGNORE`; `event_kind` per 03's CHECK; dedupe keys `release:{repo_id}:{tag_name}`, `tag:{repo_id}:{tag_name}`, `pr_merged:{repo_id}:{number}` (02 §J1, shared with 17), other kinds per §2.5; `payload_json`; `source='github_api'`; `backfilled` = 0 live (03 added the column, §11 item 15) |
| `github_http_cache` (state) | `url` | client | ETag bodies (§6); 03's retention deletes rows older than `GITHUB_HTTP_CACHE_RETENTION_DAYS` (30, 03's key) |

Also written: `metric_values` through `put_metric_values` only (§2.8; key `(occupant_key, metric_key, as_of, spec_version, stage)`, first success wins — no `run_id` in the key); `events` through `emit_event` (§3.4); `collector_runs` by 01's wrapper (one row per run; 06 supplies `JobOutcome.note` = watermark/`partial` and `meta_json` = queue counts and cross-check results).

### 3.3 Metric keys produced (P2, master §7.3, pillar weight 20)

| metric_key | raw unit | master normaliser (12 registry) | w | freshness window (02 §C6) |
|---|---|---|---|---|
| `active_authors_28d` | count | `logcap(x, 10)` | .20 | 2 d |
| `fte_devs_84d` | count | `logcap(x, 8)` | .15 | 2 d |
| `commit_days_90d` | days | `logcap(x, 90)` | .15 | 2 d |
| `declared_repo_freshness` | days since push | `recency(d, 14, 180)` | .10 | 2 d |
| `org_freshness` | days since push | `recency(d, 7, 90)` | .10 | 2 d |
| `release_cadence_90d` | count | `logcap(x, 6)` | .05 | 2 d |
| `review_practice_90d` | ratio | identity | .05 | 2 d |
| `bus_factor` | k (authors) | custom ≥3→1, 2→0.6, 1→0.3 (12's `bus_factor`, 0→0) | .10 | 2 d |
| `repo_hygiene` | ratio | `checklist` (12 reads `value_json.items`) | .05 | 2 d (recomputed daily from a ≤ 14-d-old check) |
| `originality` | ratio | custom (`1 − template share`, computed here; 12 applies `identity`) | .05 | 2 d (from a ≤ 45-d-old check) |

### 3.4 Events emitted (`events` table, occupant-level)

Names, severities, required payload keys and dedupe patterns are 03 §3.9's (02 §D1/§D2); every payload timestamp is INTEGER unix s (02 §B1). Route = 03's alert column (02 §D4; 14 §2.4 mirrors it) — a routing hint only: this collector writes rows and never sends Telegram messages; any row with `payload.backfilled=true` goes to the digest.

| event_type | severity | route | payload (required keys) | dedupe_key (03) |
|---|---|---|---|---|
| `repo_renamed` | info | stored only | `{repo_id, from, to, detected_at, method: "api"\|"html"}` | `sha1(occ\|type\|repo_id\|to)` |
| `repo_moved` | warning | digest | `{repo_id, from, to, from_owner, to_owner, detected_at}` | `sha1(occ\|type\|repo_id\|to)` |
| `repo_gone` | warning | digest only (D4: the page is 12's `red_flag_raised(repo_gone)`) | `{repo_id\|null, declared: ref_key, source, first_404_at, consecutive_404, reason: "404"\|"blocked"\|"unresolved"\|"org_empty", fallback_primary_repo_id\|null}` | `sha1(occ\|type\|declared)\|{d30}` |
| `repo_restored` | info | stored only | `{repo_id, full_name}` | `sha1(occ\|type\|repo_id)\|{d30}` |
| `repo_archived` | warning | digest | `{repo_id, full_name, role}` (declared or primary) | `sha1(occ\|type\|repo_id)` |
| `org_repo_archived` | warning | digest | `{owner, repo_id, full_name, is_subnet_related}` | `sha1(occ\|type\|repo_id)` |
| `identity_repo_changed` | warning | immediate | `{source, from_ref, to_ref, from_repo_id, to_repo_id}` | `sha1(occ\|type\|to_ref)` |
| `source_disagreement` | info | digest | `{netuid, occupant_key, field: "github_repo", groups: [{key: "<repo_id>", sources: [04 source…], values: [raw ref…]}], resolved_source, pair_hash, repo_ids: [int…]}` | `{occupant_key}\|github_repo\|{pair_hash}` |
| `commit_spam_suspected` | warning | digest | `{repos: [repo_id], human_commits_28d, flagged_28d, flags_histogram, force_push_repos}` | `occ\|{d30}` |

All rows go through 03's `emit_event(db, *, event_type, occupant_key, netuid, severity, event_at, observed_at, payload, dedupe_key, source='github', run_id)` (unknown types or missing required keys raise; extra keys are allowed; a dedupe hit returns None and is not an error). `event_at = observed_at` (detection events). For `source_disagreement`: `repo_ids` = the sorted distinct resolved ids, `pair_hash = sha256("|".join(sorted(str(i) for i in repo_ids)).encode()).hexdigest()[:16]` (04 R6's formula over 06's group keys; 04's needs-review `source_disagreement_open` lists 06's rows until an override resolves them), `groups` sorted by key, `sources` in 04's vocabulary (`chain, subnetalpha, tmc_coldkey, manual, legacy`), `resolved_source` = the declared source (§2.3).

---

## 4. Absence-vs-failure table

"Absence" = `value_json.status='absent'` → normalised 0.0 (raw `value=0.0` for counts, `NULL` for day/ratio metrics). "Failure" = `status='failed'`, `value=NULL` → None (dropped from the pillar, lowers confidence).

| metric_key | verifiable absence → 0.0 | collection failure → None |
|---|---|---|
| all ten | no GitHub reference in *any* source for the occupant (04 `status='absent'`, or `kind=none` everywhere); every candidate is junk or a placeholder (`username/repo`, `deprecated/deprecated`; 04 `junk_only`) | 04 `status='unknown'` or resolve never completed a cycle for the occupant (`never_observed`); 401 on every call (`bad_token`). `GITHUB_ENABLED=false` writes **no** rows (12 reads a missing row as None, `no_fresh_row`) |
| `active_authors_28d`, `fte_devs_84d`, `commit_days_90d`, `release_cadence_90d`, `bus_factor` | scope `R(o)` resolved and every repo's commit page returned 0 human commits in the window (genuine inactivity is `0.0` with `status='ok'`, not absent — absence here means no ref/placeholder/gone-beyond-grace with no org fallback) | no PAT (`no_token`); last successful activity pass for every repo in `R(o)` older than `GITHUB_ACTIVITY_STALE_DAYS` (`activity_stale`); commit endpoint 5xx/network for every repo this run; replay without complete backfill (`backfill_incomplete`) |
| `declared_repo_freshness` | declared ref `gone`/`blocked`/`unresolved` for ≥ `GITHUB_GONE_GRACE_DAYS` (14) **and** no primary from org enumeration → `0.0`, reason `repo_gone` (spec 12 raises the `repo_gone` gate from the same row) | no definitive answer from the declared repo verify (`/repositories/{id}`) within 2 d (rate-limit starvation, 5xx) |
| `org_freshness` | owner `gone` or `org_empty` (0 public non-fork repos) for ≥ 14 d and no resolved declared repo | org listing not successful within 2 d for every owner |
| `review_practice_90d` | no ref/placeholder (absent); **0 merged (non-bot) PRs → `not_applicable` (None)** per master | activity stale / PR endpoint failed |
| `repo_hygiene` | no ref/placeholder/gone-beyond-grace | fewer than 3 of 5 items observed; check older than 14 d |
| `originality` | no ref/placeholder/gone-beyond-grace; declared repo is a non-template fork → `0.0` with `status='ok'` | tree endpoint failed or check older than 45 d |

Transitional states are neither: `gone_pending` and `gone` inside the grace period keep the last-good `pushed_at` (which ages naturally) with `status='ok'`, `value_json.reason='gone_grace'` (a 404 is a definitive answer, so `last_verified_at` advances and the 2-d failure rule does not fire).

---

## 5. Configuration

All in 01's `Settings` (pydantic-settings, UPPER_SNAKE, read dynamically via `app.config.settings`); 01 lists every key below and the defaults here win (02 §F1). Retired with this revision: `GITHUB_USER_AGENT` (01's `HTTP_USER_AGENT`), `GITHUB_BUCKET_BURST` (01's burst 10), `GITHUB_JOB_MAX_RUNTIME_SEC` (per-job timeouts, §2.1), `GITHUB_RESOLVE_INTERVAL_HOURS` (→ `_MIN`), `GITHUB_ALERT_AFTER_FAILURES` (01's `JOB_FAIL_ALERT_THRESHOLD`), `GITHUB_KILL_SWITCH_PATH` (02 §B12), and 01's placeholder keys `GITHUB_REPOS_INTERVAL_MIN`/`GITHUB_ORG_INTERVAL_MIN`.

| key | type | default | validator / notes |
|---|---|---|---|
| `GITHUB_ENABLED` | bool | `True` | master flag; false → every 06 job skips (`last_skip_reason="disabled"`; 01 registers jobs regardless) |
| `GITHUB_TOKEN` | str | `""` | PAT (checklist B1: fine-grained, public repositories read-only); alias `GITHUB_PAT` (01). **An empty `GITHUB_TOKEN=` line falls back to a non-empty `GITHUB_PAT`** (the operator `.env` template ships `GITHUB_TOKEN=` empty; pydantic's alias lookup takes the first key present, so before 2026-10-17 a PAT entered as `GITHUB_PAT` was silently ignored). Empty after the fallback → degraded mode (§6) |
| `GITHUB_TOKEN_EXPIRES_AT` | str | `""` | `YYYY-MM-DD` expiry of the PAT, entered by the operator. Fine-grained PATs send **no** `github-authentication-token-expiration` header (verified 2026-10-17), so this is the only source of 16's PAT-expiry warning when the header is absent; `GitHubQuota.token_expires_at` = header if present, else this date 00:00Z |
| `GITHUB_STATS_INTERVAL_DAYS` | int | `7` | `/stats/participation` + `/stats/contributors` refresh (§2.5) |
| `GITHUB_API_BASE` | str | `https://api.github.com` | |
| `GITHUB_HTML_BASE` | str | `https://github.com` | |
| `GITHUB_RATE_PER_HOUR` | int | `4000` | 01's `github` limiter rate (burst 10; hourly cap = this + 500, §6); ≤ 4,500 with a PAT; 01's validator clamps the effective rate to 50/h without a token |
| `GITHUB_MAX_CONCURRENCY` | int | `4` | 1–8 (≤ 01's `max_connections=8`) |
| `GITHUB_RESERVE_REMAINING` | int | `300` | when `x-ratelimit-remaining` < reserve, only `github_resolve` may spend; others wait for reset. 17's `backfill_github` passes its own `reserve=` (1,500, §6) |
| `GITHUB_RESOLVE_INTERVAL_MIN` | int | `360` | 60–1440; minutes (01's `interval_flag`) |
| `GITHUB_ORG_ENUM_CRON` | str | `"20:00"` | `HH:MM` UTC → the job's `cron={"hour": H, "minute": M}` |
| `GITHUB_ACTIVITY_CRON` | str | `"20:30"` | |
| `GITHUB_METRICS_CRON` | str | `"23:30"` | ≥ activity cron + 2 h and ≤ 23:40 (rows must be observed before the 00:15Z run) |
| `GITHUB_HTML_PROBE_CRON` | str | `"03:00"` | |
| `GITHUB_ACTIVITY_LOOKBACK_DAYS` | int | `90` | first-run commit window; ≥ 90 (bus factor / commit_days need 90) |
| `GITHUB_ACTIVITY_MAX_REPOS_PER_OCCUPANT` | int | `12` | 1–30 |
| `GITHUB_ACTIVITY_MAX_COMMIT_PAGES` | int | `5` | per repo per run (500 commits) |
| `GITHUB_ACTIVITY_STALE_DAYS` | int | `2` | = the GitHub freshness window (02 §C6); activity metrics → `failed` beyond this; 1–3 |
| `GITHUB_PR_LOOKBACK_DAYS` | int | `90` | |
| `GITHUB_PR_MAX_PER_REPO` | int | `20` | merged PRs whose reviews are fetched, per repo per run |
| `GITHUB_COMMIT_STATS_SAMPLE` | int | `10` | `/commits/{sha}` look-ups per flagged repo per run |
| `GITHUB_HYGIENE_INTERVAL_DAYS` | int | `7` | |
| `GITHUB_ORIGINALITY_INTERVAL_DAYS` | int | `30` | |
| `GITHUB_TEMPLATE_REPO` | str | `opentensor/bittensor-subnet-template` | `owner/name` |
| `GITHUB_STATS_202_RETRIES` | int | `3` | |
| `GITHUB_STATS_202_WAIT_SEC` | float | `3.0` | |
| `GITHUB_ETAG_ENABLED` | bool | `True` | |
| `GITHUB_ORG_MAX_REPOS` | int | `200` | pagination cap per owner |
| `GITHUB_GONE_GRACE_DAYS` | int | `14` | master §7.3/§7.5 ("> 14 d"); part of the ts-1.0 definition — validator pins 14 while `SPEC_VERSION='ts-1.0'` |
| `GITHUB_SPAM_BURST_N` | int | `10` | |
| `GITHUB_SPAM_BURST_SEC` | int | `60` | |
| `GITHUB_SPAM_MIN_STATS_TOTAL` | int | `5` | |
| `GITHUB_FTE_COMMIT_DAYS_28D` | int | `10` | Electric Capital threshold (master §7.3; pinned for ts-1.0) |
| `GITHUB_BOT_LOGINS_EXTRA` | str | `""` | comma-separated, merged into `BOT_LOGINS` |
| `GITHUB_EXTRA_DENYLIST` | str | `""` | comma-separated ref keys |
| `GITHUB_HTML_PROBE_ENABLED` | bool | `True` | |
| `GITHUB_HTML_PROBE_PACING_SEC` | float | `1.0` | ≥ 0.8 (183 requests at 0.8 s were clean; higher rates UNVERIFIED); sets 01's `github_html` rate |
| `GITHUB_TMC_CROSSCHECK_ENABLED` | bool | `True` | calls 05's `TmcClient` (G2) |

Failure alerts use 01's `JOB_FAIL_ALERT_THRESHOLD` (5) → `collector_failing` / `collector_recovered` (02 §D1). 03 owns `GITHUB_HTTP_CACHE_RETENTION_DAYS` (30).

**Pause semantics** (02 §B12; mechanics 01/16, `app/control.py::effective_pause`). A 06 job is skipped when `PAUSE_ALL` exists (`pause_all`), when an emergency file `data/control/<id>.paused` or a group file (`github.paused`; for the probe also `tmc.paused`) exists (`emergency_file`), or when a `control_state` row targets its exact id or a `PAUSE_GROUPS` group containing it (`operator`) — 16's `github` group = the five 06 jobs + 17's `backfill_github`; `github_html_probe` is also in 16's `tmc` group (TMC cross-check). `budget` = `BudgetExhausted` from the `github` limiter (G1). There is no GitHub kill file (`GITHUB_KILLED` retired). The global `KILL_SWITCH` pauses *paid* collectors only; GitHub is free and keeps running under it. `SCORING_KILLED` does not affect this collector. Waiting on GitHub's own primary limit is **not** a pause: it shows as `quota.sleeping_until` (§7).

---

## 6. Cadence, rate limits, cost

**Provider limits (02 §G1).** Every outbound call goes through 01's `Http.request(provider, 'GET', url, …)` on the process-wide `ProviderLimiter`; this spec owns no limiter and constructs no `httpx` client. Provider ids and the rows 01's limiter table must carry (06 owns the values):

| provider | used for | bucket rate | burst | hourly cap | auth / headers (01's `Http.client(provider)`) |
|---|---|---|---|---|---|
| `github` | REST API (`GITHUB_API_BASE`) | `GITHUB_RATE_PER_HOUR`/3600 (4,000/h; 50/h without a token, 01 validator) | 10 | `GITHUB_RATE_PER_HOUR + 500` (4,500). It must exceed the bucket's hourly throughput (rate + burst): a cap equal to the rate raises `BudgetExhausted` near the end of any busy hour and would end `github_activity` early (G1) | `Authorization: Bearer {GITHUB_TOKEN}` when set, `Accept: application/vnd.github+json`, `X-GitHub-Api-Version: 2022-11-28`, `User-Agent` = `HTTP_USER_AGENT` |
| `github_html` | `github.com` HTML probe (`GITHUB_HTML_BASE`) | 1/`GITHUB_HTML_PROBE_PACING_SEC` (1.0/s) | 1 | — | none (the token is never sent to github.com) |

TMC cross-check calls use 05's `TmcClient` (the only TMC caller, G2) on provider `tmc`, inside 05's TMC budget table (G3: ≤ 21/day at 03:00Z). 01's `Http` counts every request into the limiter windows and `provider_spend.requests`, retries 429 (honouring `Retry-After`) and 5xx/transport errors, and returns 301/302/401/403/404 unchanged; the other statuses 06 must see are passed in `ok_statuses` (§8). `GitHubQuota` (§8) additionally mirrors GitHub's own window from every response — `x-ratelimit-{limit,remaining,used,reset,resource}` and the `github-authentication-token-expiration` header (02 §G4; kept verbatim and parsed with `parse_ts`; verified 2026-10-17 that GitHub does **not** send it for the fine-grained PAT — `token_expires_at` then comes from `GITHUB_TOKEN_EXPIRES_AT`, §5). 01's limiter already reacts to the same headers: `ProviderLimiter.observe` penalises the `github` bucket until `x-ratelimit-reset` when `remaining` hits 0, so after a primary-limit 403 the client only decides *whether* to wait (deadline check → `QuotaDeferred`) and retries; the bucket does the sleeping, never both. 16's `ops_digest_daily` reads `GitHubQuota.snapshot()` in-process for the PAT-expiry warning. Any job other than `github_resolve` waits for `reset` when `remaining < GITHUB_RESERVE_REMAINING` (300). Secondary limits (≤ 100 concurrent, 900 points/min/endpoint, research 01-web-social §3) are respected by `GITHUB_MAX_CONCURRENCY` 4 and the ≈ 1.1 req/s bucket.

**GitHub PAT budget table** (owner 06, 02 §A2/§G4). PAT 5,000/h per GitHub window = 120,000/day; 01's `github` bucket 4,000/h; ~130 active occupants, ~176 distinct declared keys, ~140 distinct owners, ~450 repos in scope at cap 12 / avg 3.5.

| caller | calls per run | runs/day | calls/day | when (UTC) |
|---|---|---|---|---|
| `github_resolve` | 176 refs → ≤ 176 verifies (+ ≤ 11 redirect follow-ups on first contact) | 4 | ≈ 720 | every 6 h |
| `github_org_enum` | 140 owners × 1.1 pages (endpoint kind remembered, §2.4) | 1 | ≈ 155 | 20:00 |
| `github_activity` | per repo: commits 1.3 (watermarked) + releases 1 + tags 1 (+≤ 5 tag dates) + pulls 1 + reviews ≤ 20 (avg 3) + stats (2.8 ÷ 7 d) ≈ 7.7 → 450 repos | 1 | ≈ 3,500 (≈ 53 min at 4,000/h) | 20:30 → ≤ 23:20 |
| hygiene (inside activity) | 5–7 calls × ≤ 260 declared/primary repos ÷ 7 d | — | ≈ 200 | 20:30 |
| originality (inside activity) | 2 × 260 ÷ 30 d (+1 template tree) | — | ≈ 20 | 20:30 |
| commit stats sampling | ≤ 10 × flagged repos (typ. ≤ 30) | — | ≈ 300 (cap 1,000) | 20:30 |
| **live total** | | | **≈ 4,900/day ≈ 4 % of quota (less in practice: 304s are free, below); peak ≤ 4,000/h (+ burst 10) in 20:30–21:30** | |
| first `github_activity` run (bootstrap) | 90-d commit history + ≤ 20 PR reviews per repo | once | ≈ 12,000 (≈ 3 h; completes over 2 runs via the soft deadline) | trigger once with `python -m app run-job github_activity` outside 20:00–23:30 |
| 17 `backfill_github` (manual, one-off) | ≈ 5–6 k without PR reviews; + 1 per merged PR ≈ 20–40 k (UNVERIFIED, 17 §6) | 2–4 triggers (1–2 nights) | ≈ 2 h / ≈ 6–13 h at ≤ 3,500/h (≈ 3,000/h effective) | only inside 17's `BACKFILL_GITHUB_WINDOW_UTC` **00:30–08:00Z** (overnight, after the 20:00–23:30 live batch; live use in the window is `github_resolve` ≈ 180/6 h only) |
| `github_html_probe` | 176–600 HTML GETs (all known `full_name`s) | 1 | 0 PAT quota (`github_html`); 3–10 min at 1 req/s | 03:00 |
| TMC cross-check | 0–1 × `/subnets/table/` (only when `chain_hourly` is > 2 h old) + ≤ 20 × `/subnets/github-stats/{n}/` on mismatch | 1 | ≤ 21 TMC req (05's table, G3) | 03:00 |

**Headroom rule (G4).** `backfill_github` works only inside `BACKFILL_GITHUB_WINDOW_UTC` (00:30–08:00Z; 17 enforces it and stops at the next repo boundary when the window closes) and passes `reserve=BACKFILL_GITHUB_RESERVE_REMAINING` (1,500) and its soft deadline to every `GitHubClient.get`/`get_all_pages` call, so `GitHubQuota.wait_if_reserved('backfill_github', reserve=1500, deadline=…)` sleeps to `reset_at` whenever `x-ratelimit-remaining < 1,500` (`QuotaDeferred` if that passes the deadline; 17 then returns `partial`). Live jobs therefore always keep **≥ 1,500 req/h** of every GitHub window (they themselves wait only below 300; `github_resolve` never waits) and, inside the window, need ≤ ≈ 200/h of 01's FIFO bucket; the backfill's share is ≤ 3,500 req/h of the PAT's 5,000/h and ≤ 4,000/h of the bucket. Do not start the §6 bootstrap run inside 00:30–08:00Z while a backfill is pending. The sub-paths `/repositories/{id}/commits|releases|pulls` that 17 prefers work (verified 2026-10-17; 06's live activity uses them too), so 17's `/repos/{full_name}/…` fallback should never fire.

The old bot's `Semaphore(5)` + no backoff (`fundamentals_collect.py:187,199-201`) is replaced by 01's limiter + quota-aware sleeping; a 403 never drops the remainder of a batch.

**ETag / conditional requests.** `If-None-Match` is sent for `GET /repositories/{id}`, `/repos/{o}/{r}`, `/orgs/{o}/repos` page 1, `/community/profile` (bodies stored in `github_http_cache`, ≤ ~10 MB total). On 304 the cached body is used. **Authenticated 304s are free** (verified 2026-10-17 with the operator's fine-grained PAT: three `If-None-Match` re-requests of `/repositories/884216433` returned 304 with `x-ratelimit-used` unchanged at 4; research had shown that *unauthenticated* 304s do cost one). They still take one token from 01's `github` bucket and hourly cap (01 counts requests, not GitHub quota), so the budget table keeps counting them; the PAT window gains the headroom. ETags are never used for paginated commit/PR lists.

**Degraded (no PAT) mode.** 01 clamps the `github` rate to 50/h; `github_resolve` (≤ 176 calls, so one cycle spans several 30-min runs via the soft deadline) and `github_html_probe` run; `github_org_enum` and `github_activity` skip their API work; `github_metrics` still writes: `declared_repo_freshness` from resolve data, verifiable absences (no ref/placeholder), and `status='failed', reason='no_token'` for everything else, so confidence drops honestly. At startup one process notice `github: no GITHUB_TOKEN — builder pillar degraded` via `AlertLatch.fire('github_no_token', text)` with `event_type=None` (02 §D3: sent directly, no row). In this mode 19's `wb_github_probe` (≤ 45/h) shares the same 50/h bucket, so a resolve cycle can take a day; that is accepted (degraded mode is a fallback, not an operating point).

**Cost:** $0. `collector_runs.cost_usd = 0`; `provider_spend` rows for `github`/`github_html` carry request counts only (usd 0), written by 01's `Http`/governor (G1, G5).

---

## 7. Failure modes and fail-safe behaviour

| failure | handling | metric effect | `/health` |
|---|---|---|---|
| 403/429 with `x-ratelimit-remaining: 0` | `GitHubQuota` sleeps to `reset + 2 s` (≤ 3,700 s), retries the same request; `QuotaDeferred` if that passes the run's soft deadline → run ends ok with `partial`; remaining refs next run | none (last-good kept) | `quota.sleeping_until`, `quota.reset_at` (not a pause) |
| 403 secondary (`retry-after` or body mentions "secondary rate limit") | sleep `Retry-After` (default 60 s), ×2 backoff, ≤ 3 tries, then the call fails; concurrency drops to 1 for the rest of the run. A 429 is retried by 01's `Http` first (`Retry-After`, G1), then `ProviderError` | per-repo `failed` if final | `last_error`, `rate_limits.github.penalized_until` |
| 403 that is neither primary nor secondary (e.g. "Repository access blocked") / 451 (DMCA) | ref → `blocked`; retried next cycle; counts toward the 14-d grace like a 404 | after grace: absent (`repo_gone`, detail `blocked`) | `queue.blocked` |
| 401 (bad or expired PAT) | `GitHubAuthError`: the run fails at once (16 §7.3.3 runbook) | `failed`, `bad_token` once rows go stale | `token_state: "bad"`, `consecutive_failures` |
| 404 on `/repos/{o}/{r}` or `/repositories/{id}` | `gone_pending` → confirmed next cycle → `gone` + event; org fallback | see §4 | `queue.gone` |
| 5xx / timeout / connection error | 01's `Http` retries with backoff, then `ProviderError` → `GitHubUnavailable` for this call; state and last-good unchanged. A run raises `GitHubRunDegraded` at its end (after committing all work) when > 50 % of its calls failed, so 01 counts it as a failure | `failed` for metrics whose data goes stale beyond windows | `consecutive_failures`, `last_error` |
| `BudgetExhausted` (01's `github` hourly cap) | propagates; the job ends `paused: budget` until the window resets (G1); work done so far is committed per repo | none | `paused: "budget"` |
| `202` on `/stats/*` after retries | keep last-good JSON; not a failure | none (stats are cross-checks) | — |
| `409 Git Repository is empty` | 0 commits, `status='ok'` | counts `0.0` (genuine) | — |
| `Location` on 301 not `/repositories/{id}` | log, treat as failure, state unchanged | none | `last_error` |
| HTML probe markup change (meta missing on 200) | log `html_probe_parse_miss`, infer nothing; if > 50 % of pages miss, or on a final 429, disable the probe for 24 h (log + `/health` only; no event type exists for it, 02 §D3) | none (probe is advisory) | `parse_miss_rate`, `disabled_until` |
| TMC cross-check 404 (`GitHub stats not found`) / 429 | treated as "no data"; never a failure | none | `tmc_crosscheck.mismatches` |
| GitHub `pushed_at` older than our last observation (clock skew / repo restored from backup) | store as observed; `github_repo_snapshots` keeps both | recency uses the latest observed | — |
| Job raises anywhere | 01's wrapper catches, writes `collector_runs.ok=0`, increments `consecutive_failures`; `collector_failing` at `JOB_FAIL_ALERT_THRESHOLD` (5), `collector_recovered` once on recovery | none | `consecutive_failures` |
| Power loss mid-run | commits are `INSERT OR IGNORE` by sha, watermark advances only after a repo's pass commits; refs ordered by `last_attempt_at ASC`; startup catch-up (§2.1) | none | `last_ok` |
| Occupant re-registered mid-window | new `occupant_key` has no refs → fresh resolve next cycle; old key's rows frozen | old occupant's metrics stop being written | — |

**Never raise into the scheduler.** Each job is a `CollectorJob` whose `collect(run) -> JobOutcome` may raise; 01's wrapper `run()` never raises, records the run and counts failures. Soft deadlines return `JobOutcome(rows, note="partial: N left")` instead of letting `asyncio.wait_for` cancel the run.

**`/health`** (01 builds it, G6; ISO strings only at this boundary). `collectors.<id>` for the five jobs is 01's standard block; `paused` uses B12's closed vocabulary `operator | kill_switch | pause_all | budget | emergency_file`. 06's `status()` overrides add:
```json
{"github_resolve": {"token_state": "ok | missing | bad",
   "quota": {"limit": 5000, "remaining": 4712, "used": 288, "reset_at": "…", "resource": "core",
             "sleeping_until": null, "token_expires_at": "…", "token_expiration_raw": "…"},
   "queue": {"refs": 176, "resolved": 158, "gone_pending": 2, "gone": 9, "blocked": 0, "placeholder": 1, "org_empty": 1, "none": 3}},
 "github_activity": {"scope": {"occupants": 130, "owners": 140, "repos_in_scope": 447}, "repos_left": 0, "rate_limit_sleeps_24h": 0},
 "github_html_probe": {"parse_miss_rate": 0.0, "rename_hints": 0, "disabled_until": null,
   "tmc_crosscheck": {"checked": 128, "mismatches": 3}}}
```
`rate_limits.github` and `rate_limits.github_html` are 01's limiter snapshots. Liveness is 16's rule (E4: `last_run`-based; `github_resolve` 45,120 s; daily crons ≈ 48–51 h).

---

## 8. Implementation plan

Package `app/collectors/github/` (the master's `collectors/github.py` slot, refined into a package; `app/collectors/github/__init__.py` exports the five job classes, `get_client`, `cli`).

| file | contents |
|---|---|
| `urlnorm.py` | `DeclaredRef`, `normalise(raw) -> list[DeclaredRef]`, `PLACEHOLDER_PAIRS/OWNERS/REPOS`, `RESERVED_FIRST_SEGMENTS`, `_name_token(name) -> str\|None` |
| `client.py` | `class GitHubClient` (below; one per Runtime via `get_client(rt) -> GitHubClient`, shared with 17's backfill), `class GitHubQuota` (process-wide: `limit, remaining, used, reset_at, resource, sleeping_until, token_expires_at, token_expiration_raw` — INTEGER times; `update(headers)`; `async wait_if_reserved(job, *, reserve: int \| None = None, deadline: float \| None = None)` — effective reserve = `reserve`, else 0 for `github_resolve`, else `GITHUB_RESERVE_REMAINING`; raises `QuotaDeferred` when the wait would pass `deadline`; `snapshot() -> dict`), exceptions `GitHubUnavailable`, `GitHubAuthError`, `QuotaDeferred`, `GitHubRunDegraded`; `parse_link_next(headers) -> str\|None`, `parse_redirect_id(location) -> int\|None` |
| `resolver.py` | `class Resolver`: `async run(now, deadline) -> ResolveReport`; `async resolve_ref(ref_row) -> RefState`; `async verify_by_id(repo_id) -> RepoRow\|Gone\|Failed`; `declared_ref(resolved: Resolved) -> Candidate\|None` (§2.3 order); `emit_alias_events(old_full_name, repo)`; `upsert_repo(payload, method, occupant_key, source, raw)`; `detect_disagreement(occupant_key, refs) -> dict\|None` (§3.4 payload) |
| `orgs.py` | `async enumerate_owner(owner) -> OrgListing\|Failed` (kind-aware org/user endpoint), `pick_primary(repos, netuid, subnet_name, now)`, `build_scope(occupant_key) -> list[int]`, `detect_archived_flips(owner, before, after) -> list[Event]` |
| `activity.py` | `async collect_repo(repo: RepoRow, now, deadline) -> RepoActivityResult`; `async fetch_commits(repo, since) -> list[CommitRow]`; `async fetch_stats(repo)`; `async fetch_releases_tags(repo)`; `async fetch_merged_prs_with_reviews(repo, since)`; `async check_hygiene(repo)`; `async check_originality(repo, template_tree)`; `rebuild_activity_daily(repo_id, day_tss)` |
| `spam.py` | `flag_commits(commits, repo) -> list[CommitRow]` (pure; insert-time and read-time use, §2.6), `detect_force_push(repo, commits_new, pushed_at) -> bool`, `normalise_message(msg) -> str` |
| `authors.py` | `author_key(commit_json) -> str`, `record_identity(db, alias_key, canonical_key)`, `resolve_author_keys(db, keys) -> dict[str,str]`, `is_bot(commit_json_or_login) -> bool`, `BOT_LOGINS` |
| `metrics.py` | `class P2Metrics`: `compute(occupant_key, now, *, replay: bool = False, coverage: Callable[[int, int], bool] \| None = None) -> list[MetricObservation]` with one pure function per key (`active_authors_28d(C)`, `fte_devs_84d(rows)`, `commit_days_90d(C)`, `declared_repo_freshness(...)`, `org_freshness(...)`, `release_cadence_90d(events)`, `review_practice_90d(events)`, `bus_factor(C)`, `repo_hygiene(hj)`, `originality(oj, repo)`); `classify_authors(occupant_key, commits, day_ts) -> list[AuthorDailyRow]`; `slot_for(now) -> int` (§2.8); `write_metric_values(db, obs, *, now, run_id, backfilled=False) -> int` (one `put_metric_values` call); `MetricObservation(metric_key, value: float\|None, status, reason, detail: dict)`. 17's `replay_history` calls `compute(…, replay=True, coverage=…)` with `now = D 23:59:59Z`, then `write_metric_values(…, backfilled=True)` |
| `html_probe.py` | `async probe(full_name) -> HtmlProbeResult(status, repo_id, nwo, is_fork, location)`; `parse_octolytics(html) -> dict` (regex on `<meta name="octolytics-dimension-repository_id" content="…">`, `…repository_nwo`, `…repository_is_fork`; the network-root meta name `…repository_network_root_nwo` is UNVERIFIED and optional); `async run(now, deadline)` — queues API re-verify (`github_declared_refs.last_attempt_at = NULL`) for any nwo mismatch or a 301 to a name other than the stored canonical `full_name` (a declared stale string such as `rayonlabs/chutes` 301-ing to the known `chutesai/chutes` is no news and would otherwise re-queue daily) |
| `tmc_crosscheck.py` | `async run(now, tmc: TmcClient)`: our declared-repo `commits_all` over 30 d vs the latest `chain_hourly.commits_30d` per netuid (05 stores it; `tmc.fetch_subnets_table()` only when that row is > 2 h old); mismatch ratio > 2× or absent-vs-present → up to 20 `tmc.get('/subnets/github-stats/{netuid}/')` (`owner, repo, last_pushed_at, commits_30d, is_valid`) compared with `github_repos` by *verbatim on-chain string* (TMC keys on that, not the canonical); results in the probe run's `collector_runs.meta_json.tmc_crosscheck` and log lines `github.crosscheck.mismatch` (no metric effect, no events) |
| `jobs.py` | `GithubResolveJob`, `GithubOrgEnumJob`, `GithubActivityJob`, `GithubMetricsJob`, `GithubHtmlProbeJob` — `CollectorJob` subclasses (01 §2.6) with the §2.1 class attributes (`id, phase, enabled_flag, interval_flag \| cron, provider, timeout_sec, misfire_grace_sec, min_gap_factor, startup_catchup, catchup_avoid_utc`), `collect(run) -> JobOutcome`, `status()` extras (§7), `soft_deadline(run) -> float` |
| `cli.py` | `python -m app.collectors.github {normalise <url>\|resolve [--netuid N] [--dry-run]\|org <owner>\|activity [--netuid N]\|metrics [--netuid N] [--now ISO] [--write]\|probe [--netuid N]\|crosscheck}` (`metrics` prints by default; `--write` writes with the §2.8 slot rule) |
| (elsewhere) | config keys of §5 live in 01's `Settings` (02 §F1); DDL is 03's schema v1 (no migration file here); `JOB_TABLE` rows and limiter rows are 01's copies of §2.1 and §6 |

**`GitHubClient`** (`client.py`):
```python
GH_OK = (200, 202, 204, 301, 302, 304, 307, 308, 401, 403, 404, 409, 422, 451)   # passed to Http as ok_statuses

class GitHubClient:
    def __init__(self, http: Http, quota: GitHubQuota, db, settings_getter, clock=time.time): ...
    async def get(self, path: str, *, params: dict | None = None, etag_key: str | None = None, job: str = "",
                  reserve: int | None = None, deadline: float | None = None,
                  accept: str = "application/vnd.github+json") -> GHResponse
    # GHResponse(status: int, json: Any | None, headers: dict, from_cache: bool, location: str | None)
    async def get_all_pages(self, path, *, params, max_pages: int, job, reserve=None, deadline=None) -> list[Any]
    async def get_html(self, url: str) -> GHResponse           # provider 'github_html', no auth, redirects not followed
```
`get` implements: `quota.wait_if_reserved(job, reserve=…, deadline=…)` → `http.request('github', 'GET', GITHUB_API_BASE + path, params=…, headers={'Accept': accept, 'If-None-Match': <etag> when etag_key and a cache hit}, follow_redirects=False, ok_statuses=GH_OK)` (01's `Http` acquires the `github` limiter token, retries 429 honouring `Retry-After` and 5xx/transport errors with backoff, then raises `ProviderError`; `BudgetExhausted` propagates) → `quota.update(response.headers)` → dispatch: 200 (store ETag + body), 304 (cached body, `from_cache=True`), 301/302/307/308 (return `location`, never followed), 202/204/404/409/422/451 (returned), 401 → `GitHubAuthError`, 403 with `x-ratelimit-remaining: 0` → sleep to `reset + 2 s` via the quota (≤ 3,700 s; `QuotaDeferred` past `deadline`) and retry, 403 secondary (`retry-after` header or a body message containing "secondary rate limit") → sleep `Retry-After` (default 60 s) ×2 backoff ≤ 3 tries with concurrency → 1, then `GitHubUnavailable`; any other 403 → returned (caller: `blocked`); `ProviderError` → `GitHubUnavailable` (caught by callers as `Failed`).

**Port from the old bot:** only the regex spirit of `parse_repo` (`fundamentals_collect.py:163-177`), the `Accept`/`Authorization: Bearer` header construction (`fundamentals_collect.py:184-186`, now 01's `Http.client('github')` base headers) and the env fallback `GITHUB_PAT`→`GITHUB_TOKEN` (line 342; `GITHUB_TOKEN` canonical, `GITHUB_PAT` an alias in 01's `Settings`). Do **not** port `fetch_github` (lines 180-219: no `follow_redirects` control, 404 silently dropped, 403 dropped, non-200 dropped) or the client construction at lines 325-326 (no per-provider limiter). All cited lines re-verified 2026-09-23.

**Step order (Phase 4):**
1. `urlnorm.py` with unit tests (pure, no I/O); confirm 01 carries the §5 keys and the §6 limiter rows, and 03's schema v1 the §3.2 tables.
2. `client.py` + `GitHubQuota` over 01's `Http` against recorded fixtures (respx); verify 403-sleep, 301-no-follow, `QuotaDeferred`.
3. `resolver.py` end-to-end on the fixtures (chutes 301, apex 301, `username/repo` placeholder, `qbittensor-labs/quantum` 404 → org enum, same-id no-op, netuid-20 disagreement); events; run `cli resolve --dry-run` on the laptop with the PAT.
4. `orgs.py` (+ `pick_primary` tests on the qbittensor-labs and Beam-Network listings); scope build.
5. `activity.py` + `spam.py` + `authors.py`; run `cli activity --netuid 64`; inspect `github_commits`.
6. `metrics.py` + `github_authors_daily`; run `cli metrics --netuid 64 --write` and `--netuid 1`; check `metric_values`.
7. `html_probe.py`, `tmc_crosscheck.py`, `jobs.py` (01's `JOB_TABLE` rows), `/health` extras, CLI; bootstrap `run-job github_activity`; systemd restart; 24 h soak.

---

## 9. Tests

Fixtures live in `tests/fixtures/github/` as recorded JSON/HTML with headers (`{status, headers, body}`), replayed through `respx` (httpx mock transport). Time is injected (`clock=`/`now=`), never patched globally.

**Unit — `urlnorm`** (parametrised, ≥ 40 cases): every form in §2.2 including `https://github.com/orgs/Beam-Network/repositories` → owner `Beam-Network`; `https://github.com/ditto-assistant/` → owner; `…/chutes.git`, `…/subnet?tab=readme-ov-file`, `…/repo#readme`, `git@github.com:o/r.git`, `www.github.com/o/r/tree/main/x`, `https://o.github.io`, two URLs in one string, `username/repo` → placeholder, `deprecated/deprecated` → placeholder, `github.com/topics/bittensor` → none, invalid owner `-foo` → none, `GITHUB_EXTRA_DENYLIST` honoured.

**Unit — `client`** (stub 01 `Http`): every call goes through `Http.request('github', …, ok_statuses=GH_OK)` and HTML through provider `github_html` without an `Authorization` header; 200 stores ETag and body; 304 returns cached body with `from_cache=True` and `GitHubQuota` follows the 304's `x-ratelimit-*` headers; 301 returns `location` unfollowed; 403 with `x-ratelimit-remaining: 0, x-ratelimit-reset: now+5` sleeps (fake clock) and retries once → 200, and raises `QuotaDeferred` when `deadline < reset`; 403 secondary with `Retry-After: 2` backs off ≤ 3 times then raises `GitHubUnavailable`; 401 → `GitHubAuthError`; `ProviderError` → `GitHubUnavailable`; `BudgetExhausted` propagates; `wait_if_reserved` blocks non-resolve jobs when `remaining < 300`, never blocks `github_resolve`, and blocks a `reserve=1500` caller at 1,499; `github-authentication-token-expiration: 2027-09-01 12:00:00 UTC` → `token_expiration_raw` verbatim and `token_expires_at` parsed (header absent → both `None`).

**Unit — `resolver`** (recorded fixtures):
- `rayonlabs/chutes` → 301 `Location: https://api.github.com/repositories/884216433` → `/repositories/884216433` → `chutesai/chutes`; asserts `github_repos.repo_id=884216433`, alias rows for both names, event `repo_moved` (owner changed), `resolution_method='redirect'`.
- `macrocosm-os/prompting` → 301 → `741209532` `macrocosm-os/apex`; event `repo_renamed` (same owner).
- `username/repo` → placeholder never requested (assert zero HTTP calls).
- `qbittensor-labs/quantum` → 404 ×2 cycles → `gone`, `repo_gone` event with `first_404_at`, org enumeration fixture (`quantum-compute` "SN 48", `enigma` "SN 63") → primary for netuid 48 is `quantum-compute`, for netuid 63 is `enigma` (netuid bonus).
- Re-verify by id: fixture where `/repositories/884216433` now returns `full_name='chutesai/chutes-v2'` → alias + `repo_renamed`; fixture returning 404 → `gone_pending`.
- Chain says `chutesai/chutes`, subnetalpha says `rayonlabs/chutes` → same id → **no** `source_disagreement` and no write to 04's tables. Netuid-20 fixture (`witnessvision/witness_subnet` 1360186689 vs `chronoseek/bittensor-subnet` 1183382351) → one `source_disagreement` with severity `info`, `field='github_repo'`, two groups keyed by id, `repo_ids=[1183382351, 1360186689]`, `pair_hash` and `dedupe_key` per §3.4, `resolved_source='chain'`; only `witnessvision` is enumerated; the payload passes 03's `emit_event` validator.
- SN117-style fixture: chain `everyframe-studios/everyframe-miner` 404 ×2, subnetalpha `glyph-research/glyph-subnet` 200 → the declared ref stays chain (`repo_gone`, no disagreement); owner listing 404 → after 14 d `declared_repo_freshness` is `absent`/`repo_gone`; a 04 `suppress` on the chain value → subnetalpha becomes declared (no `identity_repo_changed`: the chain ref never had an id, so this is first sight — §2.3 — and the operator made the change).
- `identity_repo_changed` fires when the declared id changes between two runs; does not fire for `CortexLM/platform → CortexLM/cortex` (same id 972956783).

**Unit — `orgs`**: `chutesai` listing (12 repos: `sek8s`, `tdx-measure` fork, `chutes-audit`, `chutes-api`, `chutes-miner`, `e2ee-proxy`, `chutes` pushed 07-23) → `max_pushed_at_non_fork` = sek8s's 09-20, scope excludes the fork, includes the declared `chutes` although idle; `ditto-assistant` listings before/after with three repos flipping archived → three `org_repo_archived` events, `is_subnet_related=true`; `Beam-Network` (3 repos, `mls-go` fork) → primary `beam`; `/orgs/{u}` 404 → `/users/{u}/repos` used; both 404 → `kind='gone'`.

**Unit — `spam` / `authors`**: `github_commits` rows are never UPDATEd (a burst completed by a later batch is flagged at read time only); synthetic commit lists: 15 commits in 40 s → 14 flagged `burst`, first kept; 6 identical "update" messages over 3 d → flagged except first per day; `dependabot[bot]`, `author.type='Bot'`, `12345+renovate[bot]@users.noreply.github.com` → `is_bot`; 2-parent commit → `is_merge`; noreply email → login key; email-only commit later mapped to a login through `github_author_identities`; force-push heuristic true/false cases.

**Unit — `metrics`** (pure, in-memory rows): an 84-day synthetic history for three authors (A: 12/11/10 commit-days per sub-window → full_time; B: 12/0/0 → part_time; C: one commit → one_time) → `fte_devs_84d=1`, `active_authors_28d=2`; bus factor with commits 60/30/10 → k=1 (60 ≥ 50 %), 40/35/25 → k=2; `review_practice_90d` with 0 merged → `not_applicable`; hygiene with 2 of 5 items observed → `failed`; originality with 20 identical of 30 files → 0.333; absence encoding: occupant with only a placeholder → all ten rows `status='absent'` with raw `0.0` for counts and `NULL` for day/ratio metrics; stale activity (> 2 d) → `failed` for the five activity metrics while freshness metrics stay `ok`; `slot_for(23:30Z)` = next 00:00Z and `slot_for(10:00Z)` = 12:00Z; a second write for the same slot stores 0 rows (first success wins) unless the stored row is `failed`; `review_practice_90d` ignores dependabot-authored PRs; an all-archived org → `org_freshness` `ok` with `all_archived=true`; replay admits `backfilled=1` commits, a False `coverage` → `failed`/`backfill_incomplete`, no snapshot ≤ `now` → `no_pit_snapshot`; every stored timestamp is an INTEGER.

**Unit — events / `github_events`**: severities only `info`/`warning`; dedupe keys equal 03's patterns; `pr_merged:{repo_id}:{number}` and `tag:{repo_id}:{tag_name}` keys; a PR already written by 17's backfill is not duplicated.

**Unit — `html_probe`**: recorded `github.com/chutesai/chutes` HTML → `repo_id=884216433`, `nwo='chutesai/chutes'`, `is_fork=false`; recorded `github.com/rayonlabs/chutes` 301 → `location`; HTML without meta → `parse_miss`.

**Integration / smoke** (marked `@pytest.mark.integration`, skipped without `GITHUB_TOKEN`): resolve netuids 1, 64, 105, 118 live and assert canonical ids `741209532, 884216433, 1238170662, 1224630318`; enumerate `chutesai`; collect activity for 884216433 with `GITHUB_ACTIVITY_MAX_COMMIT_PAGES=1`; run metrics for the same occupant and assert ten `metric_values` rows with `status ∈ {ok, not_applicable}`; assert the quota tracker's `used` increased by the number of non-cached calls. Mocked always: Telegram (`send_alert` spy), 05's `TmcClient` transport, clock.

---

## 10. Verify checklist (Phase 4 done when all pass on the laptop)

1. `pytest tests/collectors/github -q` → all green, ≥ 120 tests; `pytest -m integration tests/collectors/github` green with the PAT in `.env`.
2. `python -m app.collectors.github normalise "https://github.com/orgs/Beam-Network/repositories"` → `[DeclaredRef(kind='owner', owner='Beam-Network', repo=None, key='beam-network', placeholder=False)]`; `… normalise "https://github.com/username/repo"` → `placeholder=True`.
3. `python -m app.collectors.github resolve --netuid 64 --dry-run` → prints `chain: chutesai/chutes → 200 id=884216433 (direct)`, `subnetalpha: rayonlabs/chutes → 301 → id=884216433 (redirect) alias`; no `source_disagreement`. `--netuid 1` → `741209532 macrocosm-os/apex (redirect) repo_renamed`.
4. After one full `github_resolve` run: `sqlite3 data/taosentinel.db "select state,count(*) from github_declared_refs group by 1"` shows `resolved ≥ 150`, `placeholder ≥ 1` (SN39 `deprecated/deprecated`; 04 skips junk legacy rows), `gone_pending/gone` ≈ 10–16, and `select count(*) from github_repos where resolution_method='redirect'` ≥ 8.
5. `curl -s localhost:8090/health | jq '.collectors.github_resolve'` → `enabled:true, paused:null, token_state:"ok", quota.remaining > 0, queue.refs ≥ 170`; `jq .rate_limits.github` → `hour_cap = 4500`; `jq .rate_limits.github_html` present.
6. ~~ETag / id sub-path / expiry-header check~~ — **done 2026-10-17** (pre-build, operator PAT): three 304s left `x-ratelimit-used` at 4 (free); `/repositories/884216433/{commits,releases,pulls,tags,community/profile,readme,stats/participation,git/trees/HEAD}` all 200; no `github-authentication-token-expiration` header; community-profile `files` has no `security`. Answers written into §2.5, §5, §6 and §11.
7. After `github_org_enum`: `select owner,kind,n_public,n_non_fork,max_pushed_at_non_fork from github_orgs where owner in ('chutesai','macrocosm-os','ditto-assistant')` → chutesai `n_public ≥ 12` (54 on 2026-10-17), `max_pushed_at_non_fork ≥ 2026-09-20`; `select role,count(*) from github_occupant_repos group by 1` shows `primary` rows for the owner-only on-chain refs (105 `orgs/Beam-Network`, 118 `orgs/ditto-assistant`).
8. After the bootstrap `run-job github_activity` (§6; may end `partial` and need a second run): `select count(*) from github_commits` > 10,000; `select count(*) from github_commits where is_bot=1` > 0; `select count(*) from github_events where event_kind='pr_merged'` > 0; a scheduled run's `collector_runs.duration_sec` < 10,500 and it finishes before 23:20Z; `rate_limit.remaining` never reached 0 in logs (`grep '"rate_limit_sleep"' data/logs/*.jsonl | wc -l` == 0 in normal operation).
9. After the 23:30Z `github_metrics`: `select metric_key, status, count(*) from metric_values where source='github' and stage='raw' and as_of=(select max(as_of) from metric_values where source='github') group by 1,2` → ten keys, ≥ 120 occupants each, and `as_of % 86400 = 0`; for netuid 64's occupant `org_freshness` well below `declared_repo_freshness` while the Chutes pattern holds (research §4: org pushed 09-20, declared repo 07-23); for netuid 39 (`deprecated/deprecated`) all ten `status='absent'`; `select count(*) from github_authors_daily where day_ts=strftime('%s','now','start of day') and dev_class='full_time'` > 0. Re-running `github_metrics` for the same slot adds 0 rows.
10. Pause drill (02 §B12): `python -m app ops pause github` → `/health.collectors.github_resolve.paused == "operator"` (all five jobs and `backfill_github`); `resume github` clears it. `touch data/control/github.paused` → `"emergency_file"` until the file is removed. `touch KILL_SWITCH` → the GitHub jobs keep running (free). No `GITHUB_KILLED` file is read.
11. Power-loss drill: `sudo systemctl restart taosentinel` during `github_activity` → on restart no duplicate `github_commits` (sha PK), watermark resumes, `collector_runs` shows the aborted row `ok=0` and a later `ok=1`.
12. HTML probe: `python -m app.collectors.github probe --netuid 64` → `chutesai/chutes 200 id=884216433 nwo=chutesai/chutes fork=false`; `rayonlabs/chutes → 301 → chutesai/chutes`.
13. Cross-check: `python -m app.collectors.github crosscheck` → `checked ≥ 120, mismatches ≤ 15`, ≤ 21 `tmc` requests (`rate_limits.tmc.hour_used` delta), and netuid 22's mismatch explanation shows TMC key `datura-ai/desearch` vs canonical `Desearch-ai/subnet-22`.
14. Backfill headroom (G4, during 17's S9): no `backfill_github` request outside 00:30–08:00Z, and `quota.remaining` never below 1,500 because of it (`grep '"backfill_github"' data/logs/*.jsonl`).

---

## 11. Open questions / operator decisions

1. **Manual override precedence** — resolved: 04's `force` and `suppress` overrides (04 R2/R3) let the operator beat or hide a stale owner-signed repo (SN117); 06 follows 04's candidate order (§2.3).
2. **304 quota with a PAT** — resolved 2026-10-17: free (§6).
3. **HTML probe rate at production scale** (≤ 600 req/day at 1 req/s) — secondary abuse thresholds UNVERIFIED; the probe auto-disables on a 429 for 24 h.
4. **Private repos.** 5 of the 16 dead keys may be private, not deleted; GitHub cannot distinguish. This spec treats both as `gone` (master §7.3). Should a *declared* repo that goes private count as stronger evidence (deliberate withdrawal) than deletion? Needs operator view; no data to separate them.
5. **Declared-vs-org weighting.** Master splits `declared_repo_freshness` (.10) and `org_freshness` (.10); Chutes/Apex evidence supports this. Whether `active_authors_28d` should be org-wide (as specified) or capped at the primary repo is a Phase-C IC question.
6. **Look-ahead in org membership.** Repos are attributed to an occupant from `first_seen_at` (today for the initial universe). Backfilled P2 rows (spec 13/17) carry `backfilled=1` and `value_json.backfilled=true` (02 §J1, §2.8); private→public flips expose commit history that was unobservable at the time — a second, unflaggable look-ahead; the validation harness should report the P2 study with and without pre-`first_seen_at` rows.
7. **`metric_values` key** — resolved (02 §B3/§B4): key `(…, as_of, spec_version, stage)`, first success wins, slot `as_of` (§2.8).
8. **Extra tables** — resolved: 03's schema v1 carries all twelve GitHub tables (§3.2); the identity field is `github_repo`, read through 04's `resolved()`.
9. **GraphQL for PR reviews.** One GraphQL query per repo (`pullRequests(states:MERGED, last:20){ reviews{totalCount} }`) would replace up to 20 REST calls; deferred until the REST budget (§6) proves tight.
10. **Ditto-style "infrastructure archived" penalty.** `org_repo_archived` with `is_subnet_related=true` is emitted; whether it should feed the `all_repos_archived` gate short of *all* repos being archived is a spec-12 decision.
11. **Netuid 20 / 117 occupancy.** Netuid 20 raises `source_disagreement` on day 1 (two resolved ids); netuid 117's chain repo is 404, so it follows the `repo_gone` path (flag after 14 d unless the owner org has repos). The operator resolves both with a 04 `force`/`suppress` once `network_registered_at` confirms who holds the slot.
12. **Template drift.** `opentensor/bittensor-subnet-template` default branch moves; `originality_json.template_sha` is recorded per check so a later template change does not silently re-score history. **The template itself moved** (seen 2026-10-17: `GET /repos/opentensor/bittensor-subnet-template` → 301 → `/repositories/676155963` = `RaoFoundation/bittensor-subnet-template`): activity follows the redirect by id, uses `/repositories/{template_id}/…` for the tree and compare, and records `originality_json.template_full_name`; a declared fork whose parent is either name counts as a template fork. `GITHUB_TEMPLATE_REPO` keeps the old name (the redirect is followed), the operator may set the new one.
13. **Package vs module.** Master §3 lists `collectors/github.py`; this spec uses the package `collectors/github/` for size. No behavioural difference.
14. **Shared and user owners.** *User owners* — resolved 2026-10-17: only subnet-related repos of a personal account enter `R(o)` (§2.4). *Shared org owners* stay open: occupants sharing one organisation (e.g. `macrocosm-os`) share activity metrics; restricting `R(o)` to repos carrying the occupant's netuid/name there is a Phase-C refinement.
15. **Cross-spec (03/17).** Resolved: 03's `github_events` has a `backfilled` column (live rows 0, 17's rows 1); 17 §2.8 still describes the payload-key stand-in and should switch to the column when it is built.
16. **PAT expiry.** The fine-grained PAT carries no expiry header (§6), so 16's warning works only if the operator sets `GITHUB_TOKEN_EXPIRES_AT`. Operator: enter the expiry date shown on github.com → Settings → Developer settings → Fine-grained tokens.

---

## 12. Sources

- `E:\TaoData\specs\00-MASTER-SPEC.md` §1.3, §2, §3, §4, §5, §6, §7.2, §7.3 (P2), §7.5, §7.7, §8, §10, §11, §12.
- `E:\TaoData\specs\02-cross-cutting-conventions.md` (B1–B5, B7, B9, B12, C5, C6, D1–D4, E1–E4, F1, G1, G2, G4, J1); `03-storage-schema.md` §2.5–§2.6, §3.4, §3.9, Appendix A; `04-subnet-registry-and-identity.md` §2.6–§2.7, §3.3–§3.5; `01-repo-skeleton-runtime.md` §2.6–§2.7, §2.12, §6; `05-chain-collector.md` §6.1 (startup catch-up, `TmcClient`), budget table; `12-feature-store-and-scoring-engine.md` §2.2–§2.4, §2.8; `16-ops-deploy-and-observability.md` (liveness, `PAUSE_GROUPS`, PAT digest); `17-data-migration-and-backfill.md` §2.8, §6, §11 Q21.
- `E:\TaoData\specs\INDEX.md`; `E:\TaoData\specs\18-operator-checklist.md` (B1 `GITHUB_TOKEN`, D5 unresolved-repo digest).
- `E:\TaoData\research\02-gapfill-github-canonical-repo-resolution.md` (all sections: URL quirks, universe scan 156/11/16, resolution table, Chutes org pattern, TMC github-stats trust test, null-flow defect, resolver spec, 304-quota verification, open questions).
- `E:\TaoData\research\01-web-social-apis.md` §3 (limits, endpoints, fork/spam heuristics, OpenSSF/CHAOSS).
- `E:\TaoData\research\01-web-quant-scoring-methods.md` §1 (Santiment event method, Electric Capital classification, OpenSSF criticality normalisation, CHAOSS bus factor), §2 (missing-data and PIT rules).
- `E:\TaoData\research\01-fundamentals-scorer.md` §4 (old GitHub mechanics), §5 (`None ≠ 0` semantics), Gotchas.
- `E:\TaoData\research\02-gapfill-old-bot-runtime-skeleton.md` (job wrapper, `/health` shape, Settings conventions).
- `E:\SN_Trader\SubnetTrader\app\data\fundamentals_collect.py` lines 52-57 (constants), 163-219 (`parse_repo`, `fetch_github` — defective, regex idea only), 322-360 (client construction, token env fallback); re-verified 2026-09-23 (175 rejects `orgs`, 184-187 headers + `Semaphore(5)`, 197-203 404/403/non-200 drops, 326 client, 342 token fallback).

## Changelog
- 2026-10-17 pre-build review (operator PAT live; verified facts are dated in the text):
  - **Bug:** `GITHUB_TOKEN=` (empty, as the `.env` template ships it) shadowed `GITHUB_PAT` through pydantic's first-match alias lookup, so the PAT was ignored → §5 fallback rule; implemented in 01's `Settings` validator.
  - Settled UNVERIFIED items: authenticated 304s are free (§6); `/repositories/{id}/…` sub-paths work → activity is id-keyed and the "follow a 301 on a `/repos/{full_name}/…` sub-path" rule is dropped (§2.5); no expiry header for fine-grained PATs → new `GITHUB_TOKEN_EXPIRES_AT` (§5, §11 item 16); community profile has no `files.security` → contents check, only when `contributing` is false.
  - `/stats/*` are cross-checks only → weekly (`GITHUB_STATS_INTERVAL_DAYS`); activity budget 4.7 k → 3.5 k req/day.
  - `activity_stale` had no clock (v1 has no per-repo "last activity pass" column) → migration 0004 adds `github_repos.activity_ok_at`, `stats_checked_at`.
  - First sight is not a change (§2.3): no `identity_repo_changed` (immediate route) / `repo_archived` / `org_repo_archived` on first observation — otherwise ≈ 120 Telegram pages on the first run.
  - Declared-ref derivation from 04's stored `candidates_json` made exact (order from `field`, legacy double rows, alternate `owner_github`).
  - Workbench hand-off: `github_metrics` writes 19's `repo_pushed_days` once the PAT disables `wb_github_probe` (§2.8).
  - User-owned owners: only subnet-related repos in `R(o)` (§2.4, §11 item 14 partly resolved). §11 item 15 resolved (03 has `github_events.backfilled`). Stale verify numbers updated (chutesai 54 public repos).
- 2026-09-23 reconciliation + review:
  - B1/B7: DDL block replaced by a column contract on 03 §3.4 (INTEGER times, `size_kb`, `day_ts`, `payload_json`, `backfilled`); `github_commits` never UPDATEd, spam flags authoritative at read time.
  - B3/B4/C5: writes only via `put_metric_values`, `stage='raw'`, first success wins, `as_of` = first scoring slot ≥ now; no producer fns registered; 12's absent/ok/failed contract restated.
  - B2/D1/D2/D4: severities `warning`/`info`; `source_disagreement` → 03/04 payload, `field='github_repo'`, `pair_hash` over repo ids, `occ|github_repo|pair_hash`; 03 dedupe patterns and routing hints per event.
  - E2/E3/E4: one job table (factory, trigger, UTC time, misfire, timeout, flag, provider); per-job timeouts and soft deadlines; startup catch-up via 05's `startup_catchup`; liveness per 16.
  - G1/G2: all calls through 01 `Http` (`github`, `github_html` rows specified; hourly cap = rate + 500); TMC cross-check via `TmcClient` + `chain_hourly.commits_30d`.
  - G4: PAT budget table gains bootstrap run and 17's `backfill_github` (00:30–08:00Z, reserve 1,500 → ≥ 1,500 req/h for live jobs); `wait_if_reserved(reserve=, deadline=)`; `GitHubQuota` records token-expiration header (UNVERIFIED for fine-grained PATs).
  - J1: PR key `pr_merged:{repo_id}:{number}`, tag key, replay mode (`backfilled=1`, 17 coverage predicate, `no_pit_snapshot`).
  - B12/F1: `GITHUB_KILLED`, `GITHUB_USER_AGENT`, `GITHUB_BUCKET_BURST`, `GITHUB_JOB_MAX_RUNTIME_SEC`, `GITHUB_ALERT_AFTER_FAILURES` retired; `GITHUB_RESOLVE_INTERVAL_MIN`; `GITHUB_RATE_PER_HOUR` 4000; stale days 2 (C6); pause per 16's groups.
  - 04 alignment: input is `resolved(…,'github_repo')`; declared ref follows 04 precedence (force/suppress/demotion), a dead declared repo no longer falls to a lower source (master §7.3); 06 is the sole `github_repo` disagreement emitter, `pair_hash` in 04's format.
  - Review fixes: Cortex quality=1.0 and SN117 fork/disagreement claims corrected; netuid 34 dropped from primary check; `last_verified_at` = last definitive answer (gone-grace vs 2-d rule); None-safe `pick_primary`; bot PRs excluded; all-archived/successor defined; 401 handling; `files.security` UNVERIFIED.
- 2026-10-22 (20 N2 item 2): replay freshness — `declared_repo_freshness` falls back to, and `org_freshness` always uses, the last commit ≤ `now` (13 §2.5 "approx"), instead of `no_pit_snapshot` / today's `pushed_at` (look-ahead).
