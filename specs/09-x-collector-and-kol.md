# 09 — X/Twitter collector, KOL registry, and the RSS/YouTube/news appendix

**Status:** proposed (reconciled with 02 and adversarially reviewed 2026-09-23; pre-build review + live probes 2026-10-18) · **Phase:** 9 · **Depends on:** 02 (binding reconciliation ledger — it overrides this spec; ids such as `C3`, `G5` are cited inline), 01 (`Settings`, `CollectorJob`/`JobOutcome`/`JOB_TABLE`, `Http` + `ProviderLimiter`, `/health` builder), 03 (all DDL, the events and metric-key catalogs, text retention), 04 (registry, `watch_channels`, the X/YouTube URL normalisers), 08 (`app/nlp/textnorm.py`; co-writer of `social_post_mentions`), 12 (`MetricResult`, `register_metric`, the derived P5 combiners), 16 (cost governor `app/costs.py`, `PAUSE_GROUPS`, kill switches) · **Feeds:** 10 (posts, news and videos for P6), 12 (P5 X metrics and legs) · **Master spec refs:** §1.3, §4 (tier 0 RSS/YouTube, tier 2 twitterapi.io / official v2), §5, §6, §7.3 P5/P6, §8 row "X / KOL" and "News / RSS / YouTube", §11 (cost governor, kill switches), §12 Phase 9, §13 decision 1.

---

## 1. Purpose and scope

**Purpose.** Collect what subnet teams say on X, what credible ecosystem voices (KOLs) say about subnets, and what the wider web (news, blogs, YouTube) says — as append-only, point-in-time rows. Spec 10 classifies them into P6 (`event_impact_14d`, `polarity_7d`, `attention_7d`, `kol_coverage_30d` are 10's metrics, 02 §C4). This spec's producer fns supply P5 (02 §C3): the scored `x_engagement_30d` and `x_followers`, and the legs `x_cadence_30d` and `x_last_activity_days`, from which spec 12 derives `announcement_cadence_30d` and `silence_days` (09 registers no producer for those two). Every paid call is estimated and checked with the cost governor before it is made (`X_MONTHLY_CAP_USD`, 02 §G5).

**In scope**
- `XProvider` protocol and three implementations: `twitterapi_io` (primary, unofficial), `official_v2` (ToS-clean fallback), `null` (no-op; X metrics of occupants with a handle are `None`).
- Team-account map (occupant → X handle) built from `watch_channels` (`platform='x'`, written by spec 04 from subnetalpha `twitterx_url`, on-chain `additional` and manual overrides).
- Polling plan (§2.5): team timelines daily with `since_id`; KOL accounts 4×/day; mention search for the top-40 occupants by emission daily; weekly profile refresh.
- Normalised `social_posts` rows (X only, 02 §B11), `x_accounts` state, `x_account_snapshots` (followers PIT), `social_post_mentions` (post → occupant links; 08 writes its own platforms).
- Cost governor integration (estimate → `check` → call → `record`; no reservation — 02 §G5). Pacing, the 80 % alert (`spend_80pct`), the hard stop and `/health.spend` belong to spec 16.
- Bot/shill heuristics → `author_credibility` ∈ [0,1]; the single-account cap helper `cap_author_share` that spec 10 may import.
- `kol_accounts` seed list (`seed/kol_accounts.csv`) with operator-editable `credibility`.
- Free cross-check: daily parse of the ~12 tweets on subnetalpha profile pages (`.ctf-item`) — the only X data when `X_PROVIDER=null` (the default), a wrong-handle detector otherwise. **Live 2026-10-18 the pages render no tweets any more (§2.9); the job is kept as a detector and its fallback is gated on feed presence.**
- **Appendix A:** RSS/news/YouTube collector (`feedparser` + ETag/Last-Modified; Google News RSS query set; publisher feeds; ecosystem newsletters; Medium/Substack per occupant; YouTube channel + Novelty Search playlist RSS) → `news_items`, `video_items`.

**Out of scope**
- LLM classification, `post_labels`, and the computation of `polarity_7d` / `attention_7d` / `kol_coverage_30d` / `event_impact_14d` (spec 10, 02 §C4), and all normalisation (spec 12).
- The P5 combiners `announcement_cadence_30d` and `silence_days` (derived by spec 12 from the legs, 02 §C3), and text retention (03's `storage_retention_daily`, 02 §B10).
- Discord/Telegram (spec 08). Reddit. Nitter/RSS bridges (dead, master §4 "Not used").
- Scraping x.com directly or with the operator's account (master §1.2). xAI `x_search` (ad-hoc only; not a collector).
- SN13 Data Universe / SN22 Desearch as X data paths — recorded as **future options** in §11 only.

---

## 2. Design

### 2.1 Components and data flow

```
 watch_channels (platform='x')  kol_accounts (verified=1)   top-40 occupants by emission_share
          │                          │                             │
          ▼                          ▼                             ▼
   x_team_poll (01:30Z)      x_kol_poll (xx:30, 4×/day)   x_mentions_poll (02:00Z)
   timeline w/ since_id      packed search from:a OR..   query per occupant, since_id
          │                          │                             │
          └──────────────┬───────────┴─────────────┬───────────────┘
                         ▼                         │
        costs.check(provider, est) (16, 02 §G5)    │   deny → stop the run (note paused:budget)
                         ▼                         │
                XProvider (twitterapi_io | official_v2 | null)
                  ── Http.request(provider_id, …) over ProviderLimiter (01, 02 §G1) ──
                         ▼
             textnorm → XPost → heuristics(author_credibility)   → costs.record(provider, usd, requests=0, …)
                         ▼
   social_posts (X only; engagement refreshed)   x_account_snapshots   social_post_mentions
                         ▼
        producer fns (app/features/social_x.py, via register_metric) ──► 12 writes metric_values (P5 + legs)
        spec 10 reads social_posts + kol_accounts ──► post_labels ──► P6 (10)

 x_profile_refresh (Sun 02:30Z)   x_ctf_crosscheck (07:00Z, free)   events: x_account_gone, x_budget_paused…

 Appendix A:  feed registry (watch_channels platform rss|medium|substack|youtube; google_news queries generated)
              rss_feeds_poll (6 h) / news_google_poll (05:00Z) / youtube_poll (05:30Z)
              → news_items / video_items (dedupe by url_hash / video_id) → match.match_occupants → spec 10 / 12
```

One asyncio process. Every box is a `CollectorJob` (01 §2.6) listed in §2.5's job table: `collect(run)` returns `JobOutcome(rows, cost_usd, note)`, a whole-run failure raises into 01's wrapper (which records `ok=0` and never raises into the scheduler), and per-account errors are caught and counted.

### 2.2 XProvider protocol

```python
# app/collectors/x_providers/base.py
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol

@dataclass(frozen=True)
class XUser:
    id: str                  # provider-stable numeric id as string
    handle: str              # case as returned by provider
    display_name: str | None
    followers: int | None
    following: int | None
    post_count: int | None
    created_at: datetime | None   # tz-aware UTC
    verified: bool | None         # blue/legacy verified flag; None if not exposed
    raw: dict = field(default_factory=dict, compare=False)

@dataclass(frozen=True)
class XPost:
    post_id: str
    author_id: str
    author_handle: str
    text: str
    lang: str | None
    event_at: datetime                 # created_at, UTC
    like_count: int | None
    repost_count: int | None
    reply_count: int | None
    quote_count: int | None
    view_count: int | None
    has_url: bool
    has_media: bool
    in_reply_to: str | None            # post id replied to
    is_repost: bool                    # pure retweet
    quoted_post_id: str | None
    author: XUser | None               # populated when the provider embeds author metrics (search results)
    raw: dict = field(default_factory=dict, compare=False)

@dataclass(frozen=True)
class XPage:
    posts: list[XPost]
    next_cursor: str | None
    newest_id: str | None              # max post_id on the page (str compare via int)
    billed_posts: int                  # posts the provider bills for this page (twitterapi_io: len(page); official: result_count)

@dataclass(frozen=True)
class MentionQuery:
    terms: list[str]                   # OR-ed terms, already quoted where needed
    required_coterms: list[str]        # AND-ed OR-group, may be empty
    from_handles: list[str]            # for packed KOL/team search (from:a OR from:b); exclusive with terms
    exclude_reposts: bool = True
    lang: str | None = None

@dataclass(frozen=True)
class XCall:
    kind: Literal["resolve_user", "user_timeline", "search_recent"]
    expected_posts: int                # collector's estimate (see §6.2)
    pages: int = 1

class XProvider(Protocol):
    name: Literal["twitterapi_io", "official_v2", "null"]   # = the 02 §G1 provider id passed to Http.request, costs.check/record,
                                                            #   provider_spend.provider and social_posts.source (governor group 'x')
    async def resolve_user(self, handle: str) -> XUser | None: ...          # None = verifiably not found/suspended; raises XProviderError on failure
    async def resolve_user_by_id(self, user_id: str) -> XUser | None: ...
    async def user_timeline(self, user_id: str, since_id: str | None, max_posts: int, cursor: str | None = None) -> XPage: ...
    async def search_recent(self, query: MentionQuery, since_id: str | None, since_at: datetime | None, max_posts: int, cursor: str | None = None) -> XPage: ...
    def cost_estimate(self, call: XCall) -> float: ...                       # USD, before the call
    def cost_actual(self, call: XCall, page: XPage) -> float: ...            # USD, after the call
    def render_query(self, q: MentionQuery, since_at: datetime | None) -> str: ...   # provider syntax
```

`XProviderError(kind ∈ {auth, rate_limited, budget, transient, parse, not_found})` is the only exception type providers raise. Providers are constructed with 01's `Http` and send every request as `http.request(self.name, method, url, …, ok_statuses=(200, 400, 402))` (02 §G1): 401/403/404 are returned unchanged by `Http`, 400/402 are returned so the body can be inspected, and 429/5xx/timeouts are retried by `Http` (429 honours `Retry-After`/`x-rate-limit-reset` through `ProviderLimiter.observe`). Providers add no retry loop, sleep and client of their own. A `ProviderError` left after `Http`'s retries maps to `rate_limited` (last status 429) or `transient`; the collector then ends the run early with `JobOutcome.note='partial:<kind>'`.

### 2.3 Provider implementations

| | `twitterapi_io` | `official_v2` | `null` |
|---|---|---|---|
| Base URL | `TWITTERAPI_IO_BASE_URL` = `https://api.twitterapi.io` | `X_OFFICIAL_BASE_URL` = `https://api.x.com/2` | — |
| Auth header | `X-API-Key: <TWITTERAPI_IO_KEY>` | `Authorization: Bearer <X_BEARER_TOKEN>` | — |
| `resolve_user` | `GET /twitter/user/info?userName={handle}` → `data{id, userName, name, followers, following, statusesCount, createdAt, isBlueVerified}` (field names as observed in vendor docs at research time — **UNVERIFIED**; the parser reads them through a `FIELD_MAP` dict so a rename is a one-line fix) | `GET /users/by/username/{handle}?user.fields=created_at,public_metrics,verified` → `data{id, username, name, created_at, public_metrics{followers_count, following_count, tweet_count}, verified}`; `errors[]` with `title="Not Found Error"` → `None` | returns `None` |
| `resolve_user_by_id` | `GET /twitter/user/info?userId={id}` (**UNVERIFIED** param) | `GET /users/{id}?user.fields=…` | `None` |
| `user_timeline` | `GET /twitter/user/last_tweets?userId={id}&cursor={cursor}&includeReplies=true` (**UNVERIFIED** param names). Page ≈ 20 tweets, `has_next_page`, `next_cursor`. **No server-side since_id:** the provider pages until the page's minimum id ≤ `since_id` or `pages == X_TIMELINE_MAX_PAGES`, then returns the union filtered to `id > since_id`. Pinned tweets may appear first, so `newest_id = max(ids)` not `ids[0]`. First poll (`since_id` NULL): one page only. | `GET /users/{id}/tweets?since_id={since_id}&max_results=20&tweet.fields=created_at,public_metrics,entities,lang,referenced_tweets,in_reply_to_user_id,conversation_id&expansions=referenced_tweets.id&exclude={X_OFFICIAL_EXCLUDE}&pagination_token={cursor}`; ≤ `X_TIMELINE_MAX_PAGES` pages, so both providers stop at 60 posts per account per run. **First poll** (`since_id` NULL): `max_results=5` (the API minimum), one page — ≈ $0.025 per account instead of $0.50 for a 100-post page. `meta{newest_id, oldest_id, result_count, next_token}`; `result_count=0` ⇒ no `data` key. A run that hits the page bound still advances `since_id` to the newest id, so posts older than the last page are skipped (a long outage undercounts cadence). | empty page |
| `search_recent` | `GET /twitter/tweet/advanced_search?query={rendered}&queryType=Latest&cursor={cursor}`; rendered syntax is web-search syntax: `(a OR b) -filter:retweets since:2026-09-21_03:00:00_UTC lang:en`; `since:` with `_HH:MM:SS_UTC` suffix is per vendor docs (**UNVERIFIED** — fixture test §9.2 must prove it). Filtered client-side to `id > since_id`. | `GET /tweets/search/recent?query={rendered}&since_id={since_id}&max_results=100&sort_order=recency&tweet.fields=…&expansions=author_id&user.fields=created_at,public_metrics,username,verified`; rendered syntax: `(a OR b) -is:retweet lang:en`; query ≤ 512 chars; ≤ 25 `from:` handles per query (each `from:x OR ` ≈ 20 chars). Recent search covers 7 days only; a stored `since_id` whose snowflake time is older than 6 d 23 h is dropped and replaced by `start_time = now − 6 d 23 h` (the API is believed to reject an older `since_id` with HTTP 400 — **UNVERIFIED**; a 400 naming `since_id` triggers the same fallback once). | empty page |
| `cost_estimate` | `max(min_call, per_tweet × expected_posts)` per page; `resolve_user` = `per_profile`. Prices live in `app/collectors/x_providers/prices.py` (`per_tweet` 0.00015 = $0.15/1k, `per_profile` 0.00018, `min_call` 0.00015 = 15 credits; research §1) and can be overridden by `X_PRICES_JSON` without a code change. The vendor's "list calls $0.0015 from Oct 1" rule is not known to cover `last_tweets`/`advanced_search` (**UNVERIFIED**); if it does, set `min_call` to 0.0015. For `user_timeline` `expected_posts = 20 × pages` because a page bills every returned tweet, not only new ones. | `0.005 × max(1, expected_posts)` per page; `resolve_user` = `0.010` ($0.005/post read, $0.010/user read, research §1). Zero-result calls are assumed to bill `$0.005` (**UNVERIFIED**; overestimating is safe). | `0.0` |
| `cost_actual` | `max(min_call, per_tweet × billed_posts)`; profile `per_profile` | `0.005 × max(1, result_count)` (until zero-result billing is verified; then `0.005 × result_count`) | `0.0` |
| Rate limit (01 `ProviderLimiter`, 02 §G1 provider id) | `twitterapi_io`: `X_RATE_PER_SEC` = 1.0, burst 2 (the vendor advertises 1000+ QPS; we self-limit) | `official_v2`: `X_OFFICIAL_RATE_PER_SEC` = 0.25 (15/min, under the tightest app window: user lookup 300/15 min; search 450/15 min; timeline 10,000/15 min — research §1), burst 2; `x-rate-limit-remaining/reset` are honoured through `ProviderLimiter.observe` | — |
| HTTP errors | 401/403 → `auth` (the run stops at the first one and fails, §7); 402/insufficient credits → `budget` (run fails; `x_budget_paused` with reason `provider_balance`); 429/5xx/timeout → retried by `Http`, then `rate_limited`/`transient`; 404 on user → `None` | 401/403 → `auth`; 400 on an old `since_id` → start_time fallback (above), any other 400 → `parse`; 429/5xx/timeout → retried by `Http`, then `rate_limited`/`transient`; `errors[]` with `title="Not Found Error"` → `None` | — |
| Raw archive | `data/raw/x/{provider}/{YYYY-MM-DD}.jsonl.gz` when `X_RAW_ARCHIVE_ENABLED` (one line per raw post; lets a field rename be re-parsed without re-buying) | same | — |

All three are constructed by `app/collectors/x_providers/__init__.py::make_provider(settings, http) -> XProvider`; an unknown `X_PROVIDER` fails config validation, while a missing key disables X (01's fail-safe validator: `x_active()` is false; §5).

### 2.4 Team-account map and handle normalisation

`watch_channels` rows with `platform='x'` and `active=1` (generated `retired_at IS NULL`) are the input (spec 04 writes them from subnetalpha ACF `twitterx_url` — 102/128 records, research 01-web-subnetalpha — plus on-chain `additional` JSON and manual overrides; `handle` is already normalised by 04). `x_account_occupants` is re-synced from these rows at the start of every `x_team_poll` (new pairs inserted, pairs whose row is retired deleted — it is a routing table; PIT attribution lives in `social_posts.occupant_key` and `social_post_mentions`). `normalise_x_handle(url_or_handle) -> str | None` in `app/collectors/x_handles.py` delegates URL parsing to 04's `app/registry/normalise.py` X normaliser (one parser for the whole system) and adds the checks below; 09 applies it to KOL seed rows, ctf profile links and author handles in search results:

- Accept `https://x.com/H`, `https://twitter.com/H`, `x.com/#!/H`, `@H`, `H`; strip query/fragment/trailing slash; path segment 1 only.
- Reject reserved paths: `i, intent, share, home, search, hashtag, explore, settings, login, messages, compose, notifications`; reject if a status URL (`/status/`) unless a `/H/status/…` form — then take `H`.
- Validate `^[A-Za-z0-9_]{1,15}$`; return the handle with original case; compare and index on `lower()`.
- A handle mapping to ≥ 2 occupants (Rayon Labs SN64/56/19, Macrocosmos SN1/9/13/25/37 — shared accounts) is legal: `x_accounts.kind='team'`, `x_account_occupants` holds the fan-out, and posts are attributed per §2.6.
- **`legacy` X rows never count** (stricter than 08 §2.2's legacy-only rule / 08 §11 Q27): a stale legacy row can sit next to a current one — live SN25 (UR) carries `MacrocosmosAI/legacy` beside `urnetwork/subnetalpha`, which would route Macrocosmos posts to UR. Live 2026-10-18: 3 occupants have only `legacy` X rows (SN20 `GroundLayerHQ`, SN89 `infinitehash89`, SN113 `TensorUSD` — the last is a *previous* occupant's account, the 08 finding again). Such occupants have no team account here (→ `absent` `no_x_account`, §4) until 04 or a `manual` row confirms one.
- **One row per account, team wins.** `x_accounts.handle_lower` is UNIQUE (03), so an account that is both a team handle and a KOL seed row (live: `MacrocosmosAI` is the team handle of 4 occupants and a KOL row; `const_reborn` if 04 ever maps it to Affine) has a single row with `kind='team'` and `kol_handle` set. KOL polling includes it through `kol_accounts.author_id`; its posts get `is_team_account=1` **and** `is_kol=1` (03's generated `author_kind` is then `team`), and `kol_coverage_30d`/`x_kol_posts_30d` exclude it for its own occupants through `team_occupant_keys` (§3.3).

### 2.5 Jobs and polling plan

**Job table (02 §E2 — spec 01's `JOB_TABLE` mirrors these rows verbatim; they replace 01's placeholders `x_teams_daily`, `x_kols_6h`, `rss_6h`).** All are phase 9, `max_instances=1`, `coalesce=True`, not `ops_exempt`. Cron jobs set `min_gap_factor=0`; `rss_feeds_poll` keeps 01's default 0.5. Liveness max-age is 16 §2.5's formula (`interval_min·60·2 + timeout + 120`). Times avoid the 02 §E3 windows (03:00 retention, 03:30 backup, 04:30 reboot, `hh:15` scoring). The paid X jobs override `CollectorJob.enabled()` to require every listed flag; their `provider` is the `X_PROVIDER` value read when the job is built (changing the provider needs a restart), which the governor maps to group `x`.

| id | factory (`app.…`) | trigger | UTC time / interval | misfire grace s | timeout s | enabled flag (all must hold) | provider (02 §G1) | paid | pause group (16 `PAUSE_GROUPS`) | liveness max-age s |
|---|---|---|---|---|---|---|---|---|---|---|
| `x_team_poll` | `collectors.x:XTeamPollJob` | cron | daily 01:30Z (`X_TEAM_POLL_CRON`) | 3600 | 1800 | `X_ENABLED`, `X_TEAM_ENABLED`, `settings.x_active()` | `twitterapi_io` \| `official_v2` | yes | `x` | 174,720 |
| `x_kol_poll` | `collectors.x:XKolPollJob` | cron | 00:30, 06:30, 12:30, 18:30Z (`X_KOL_POLL_CRON_HOURS`, minute 30) | 1800 | 900 | `X_ENABLED`, `X_KOL_ENABLED`, `x_active()` | same | yes | `x` | 44,220 |
| `x_mentions_poll` | `collectors.x:XMentionsPollJob` | cron | daily 02:00Z (`X_MENTIONS_POLL_CRON`) | 3600 | 1800 | `X_ENABLED`, `X_MENTIONS_ENABLED`, `x_active()` | same | yes | `x` | 174,720 |
| `x_profile_refresh` | `collectors.x:XProfileRefreshJob` | cron | Sun 02:30Z (`X_PROFILE_REFRESH_CRON`) | 21600 | 1800 | `X_ENABLED`, `x_active()` | same | yes | `x` | 1,211,520 |
| `x_ctf_crosscheck` | `collectors.x_ctf:XCtfCrosscheckJob` | cron | daily 07:00Z (`X_CTF_CROSSCHECK_CRON`) | 3600 | 900 | `X_CTF_CROSSCHECK_ENABLED` (independent of `X_ENABLED`) | `subnetalpha` | no | `x` | 173,820 |
| `rss_feeds_poll` | `collectors.rss:RssFeedsPollJob` | interval | every 6 h (`RSS_POLL_INTERVAL_HOURS`) | 1800 | 900 | `RSS_ENABLED` | `rss` | no | `rss` (proposed to 16) | 44,220 |
| `news_google_poll` | `collectors.news:NewsGooglePollJob` | cron | daily 05:00Z (`RSS_GOOGLE_NEWS_POLL_CRON`) | 3600 | 1800 | `RSS_ENABLED`, `RSS_GOOGLE_NEWS_ENABLED` | `rss` | no | `rss` (proposed) | 174,720 |
| `youtube_poll` | `collectors.youtube:YoutubePollJob` | cron | daily 05:30Z (`YOUTUBE_POLL_CRON`) | 3600 | 900 | `RSS_ENABLED`, `YOUTUBE_ENABLED` | `youtube` | no | `rss` (proposed) | 173,820 |

There is no `x_resolve_new` job: resolution is the first step of `x_team_poll`/`x_kol_poll` (below).

**Per-job behaviour**

| Job id | Input set | Provider call | Watermark | Writes |
|---|---|---|---|---|
| `x_team_poll` | all `x_accounts.kind='team'` with `status IN ('active','unresolved')` | `X_TEAM_POLL_MODE=timeline` (default): `user_timeline(id, since_id, …)` (§2.3 page bounds); `=search`: packed `from:` queries of ≤ 25 handles with `since_at = last_run_at − 1h` | `x_accounts.since_id` = newest id seen (first poll: §2.3) | `social_posts`, `social_post_mentions(match_kind='team')` for shared handles, `x_accounts.last_polled_at/last_post_at/consecutive_failures` |
| `x_kol_poll` | `kol_accounts.verified=1 AND active=1 AND credibility>0` | `X_KOL_POLL_MODE=search` (default): packed `from:` queries, `since_at = last_run_at − 1h`, client-side `id > x_search_state.since_id`; `=timeline`: per account, as team | `x_search_state.since_id` per query key `kol:{chunk_index}` | `social_posts(is_kol=1, kol_handle)`, `social_post_mentions(match_kind='rule')` |
| `x_mentions_poll` | top-`X_MENTIONS_TOP_N` (40) active occupants by the latest `stage='raw'` `emission_share` row (`status='ok'`, `as_of ≥ now − 2 d`); fallback when fewer exist (before Phase 8): the latest `chain_hourly.emission` per occupant (05) | `search_recent(build_mention_query(occupant), since_id, max_posts=X_SEARCH_MAX_POSTS_PER_QUERY)`; pages until `max_posts` or exhaustion | `x_search_state.since_id` per query key `mention:{occupant_key}` | `social_posts`, `social_post_mentions` (`match_kind` `query` + `rule`) |
| `x_profile_refresh` | team + verified KOL accounts whose newest `x_account_snapshots` row is older than `X_PROFILE_REFRESH_DAYS − 1` days | `resolve_user_by_id(id)` (fallback `resolve_user(handle)` when the provider has no by-id lookup) | — | `x_account_snapshots`; `x_accounts.handle` on rename (+ `x_handle_changed`); `status='unresolved'` after 1 `None`, `'gone'` after 2 consecutive `None` (+ `x_account_gone`); a later success sets `'active'` again |
| resolution step (inside `x_team_poll`/`x_kol_poll`) | handles in active `watch_channels(platform='x')` / `kol_accounts` with no `x_accounts` row, not in retry back-off | `resolve_user(handle)` | back-off state in `job_state(job, 'unresolved:{handle_lower}')` = `{reason, consecutive, next_retry_at}` (a runtime watermark, 02 §B12) | `x_accounts` (+ `kol_accounts.author_id`), `x_account_snapshots`; `None` or error → `x_account_unresolved`, retry after `X_UNRESOLVED_RETRY_DAYS`; 2 consecutive `not_found`/`suspended` results ≥ `X_UNRESOLVED_RETRY_DAYS` apart count as a gone account for §4 |
| `x_ctf_crosscheck` | subnetalpha profile pages with `twitter_embed_id` set (63 live 2026-10-18; the value is the plugin feed id) from 04's `identity_observations(source='subnetalpha', field IN ('slug','twitter_embed_id'))` | free `GET https://subnetalpha.ai/subnet/{slug}/` (provider `subnetalpha`, 1 req/s, shared with 04) | none (idempotent by post id) | `social_posts(source='subnetalpha_ctf')` (§2.9), `x_ctf_discrepancy` events |
| Appendix A jobs | see §A.6 | | | |

**Mention query construction** (`app/nlp/match.py::build_mention_query(occupant) -> MentionQuery`): terms = `["\"{name}\"", "SN{netuid}", "\"subnet {netuid}\"", "$SN{netuid}"]` + `"@{team_handle}"` for each team handle (the on-chain token symbol is a Unicode glyph and is useless as a search term); if `name` is in the ambiguity stoplist (`AMBIGUOUS_NAMES`: any name ≤ 4 chars or in a curated list such as `Apex, Score, Nova, Templar, Gradients, Vanta, Synth, Zeus, Grail, Forge, Base, Its AI…`; the list ships in `app/nlp/ambiguous_names.txt` and is regenerated by `scripts/build_kol_list.py --names`) then `required_coterms = ["bittensor", "TAO", "subnet", "dTAO"]`, else empty. Names that are placeholders (`subnetNN`, `Unknown`) contribute no name term. Example rendered for twitterapi.io: `("Chutes" OR SN64 OR "subnet 64" OR $SN64 OR @chutes_ai) -filter:retweets since:2026-09-21_04:00:00_UTC`.

### 2.6 Post normalisation and attribution

For every `XPost` returned:
1. `text_hash = app.nlp.textnorm.text_hash(text)` — the shared normaliser defined by 08 (NFKC → lower → strip URLs → strip mentions → collapse whitespace, 02 §B11), so X hashes match Discord/Telegram hashes for 08's cross-platform dedupe; empty normalised text → `sha256('msg:x:' + post_id)` (08's convention), so media-only posts never collapse into one hash. `raw_json_hash = sha256(canonical_json(raw))`. Ids: `social_posts.post_id` is the native snowflake; the item id seen by 10 and `post_labels.post_id` is `x:{id}` (03's generated `social_posts.item_id`, 02 §B11). `url = https://x.com/{author_handle}/status/{post_id}`. All instants are INTEGER unix seconds (02 §B1).
2. Flags: `is_team_account = author_id ∈ x_accounts(kind='team')`; `is_kol = author_handle_lower ∈ kol_accounts(verified=1, active=1)`, with `kol_handle` set.
3. Attribution → `social_posts.occupant_key` (source-bound occupant only, 03) and `social_post_mentions(platform='x', post_id, occupant_key, match_kind, event_at, observed_at, run_id)`:
   - team post from a single-occupant handle → `occupant_key` = that occupant;
   - team post from a shared handle → `match.match_occupants(text)` restricted to the handle's occupant set; exactly one hit → that occupant; otherwise `NULL` and one `match_kind='team'` row per occupant of the handle;
   - mention-search hit → `occupant_key NULL`; one `match_kind='query'` row for the queried occupant, plus `'rule'` rows for further `match_occupants` hits;
   - KOL post → `occupant_key NULL`; `match_occupants` hits become `'rule'` rows; spec 10's `entities[netuids]` adds `'entity'` rows later.
4. Credibility (§2.7) → `author_credibility`, `credibility_flags`, `bot_filtered`.
5. Upsert rule: `INSERT … ON CONFLICT(platform, post_id) DO UPDATE SET like_count=…, repost_count=…, reply_count=…, quote_count=…, view_count=…, engagement_observed_at=excluded.observed_at, source=CASE WHEN social_posts.source='subnetalpha_ctf' THEN excluded.source ELSE social_posts.source END, author_id=CASE WHEN social_posts.author_id LIKE 'h:%' THEN excluded.author_id ELSE social_posts.author_id END` — the first `observed_at`, `text`, `event_at` and `occupant_key` never change (PIT). Engagement is "latest observed" with its own timestamp — the accepted deviation from append-only (03 class `state`; 13 flags `x_engagement_30d` as look-ahead, 02 §J2). The `h:{handle_lower}` placeholder author id exists only on ctf rows written before the account was resolved (§2.9). `social_post_mentions` is append-only (`INSERT OR IGNORE`). Text retention is 03's: `text` is NULLed after 90 d, the hash is kept (02 §B10).

### 2.7 Bot/shill heuristics → `author_credibility`

`app/nlp/kol.py::author_credibility(user: XUser | None, post: XPost, batch: list[XPost], db_dupes: int, kol: KolRow | None, is_team: bool) -> tuple[float, list[str]]`. Pure function; all thresholds are module constants (frozen with `spec_version`, not config):

| Factor | Condition | Multiplier | Flag |
|---|---|---|---|
| Team account | `is_team` | fixed **1.0** account-level (post-level factors still apply) | `team` |
| KOL | `kol is not None` | account-level = `kol.credibility` (operator-editable), heuristics below skipped except post-level | `kol` |
| Account age | `created_at` known: `< 30 d` → ×0.3; `30–180 d` → ×0.7 | | `young`, `newish` |
| Follower ratio | `following > 1000 and followers/(following+1) < 0.1` → ×0.5; `followers < 50` → ×0.6 | | `follow_ratio`, `tiny` |
| Posting velocity | `post_count/max(age_days,1) > 100` → ×0.2; `30–100` → ×0.6 | | `firehose`, `fast` |
| Handle pattern | `^[A-Za-z]+\d{5,}$` (auto-generated) → ×0.7 | | `autohandle` |
| Duplicate text (post-level) | same `text_hash` from the same author within ±`X_DUPLICATE_WINDOW_MIN=10` min (batch ∪ DB) → ×0.3; same `text_hash` from ≥ 2 *different* authors within 10 min → ×0.3 for all of them | | `dup_self`, `dup_coord` |
| Promo/giveaway (post-level) | ≥ 1 match of `PROMO_RE` (`giveaway|airdrop|whitelist|presale|100x|retweet to win|rt to win|dm me|send \d+|claim your|free mint|🎁|💸|🚀{3,}`) → ×0.2 | | `promo` |
| URL-only | `has_url and len(text_norm) < 20` → ×0.5 | | `link_only` |
| Unknown account metrics | `user is None` (no profile embedded, not yet resolved) → account-level factors skipped, `flags += ['unprofiled']`, account-level = 0.7 | | `unprofiled` |

`author_credibility = clamp(account_level × Π post_level, 0, 1)`. Posts with `author_credibility < X_MIN_CREDIBILITY (0.3)` are **kept** (audit) but flagged `bot_filtered=1` (10's `spam_flag`) and excluded from every metric and from LLM classification (spec 10 saves the tokens). **Team-account posts are never `bot_filtered`**: a team's giveaway post (`promo` ×0.2) is still team communication for `x_cadence_30d`/`x_last_activity_days`; the flags stay in `credibility_flags` for 10. Authors seen in mention search are profiled from the embedded author object (official: `expansions=author_id` → `includes.users`; twitterapi.io: per-tweet `author` object, **UNVERIFIED** until the §9.2 fixtures exist); accounts with `author_credibility ≥ 0.6` seen ≥ 5 times in 30 d are candidates for the operator's KOL list (listed by `python -m app.collectors.x --kol-candidates`; proposed input to 14's `needs_review_digest`).

### 2.8 Single-account 25 % engagement cap (shared helper)

```python
# app/nlp/kol.py
def cap_author_share(weights: dict[str, float], cap: float = 0.25) -> dict[str, float]:
    """Water-fill. S = Σ weights, n = authors with weight > 0, c = max(cap, 1/n) (cap·n < 1 is unattainable).
    Returns w' with Σ w' = S and w'_a ≤ c·S for every author: capped authors get exactly c·S, the remaining mass is
    shared by the uncapped authors in proportion to their original weights, repeated until no uncapped author exceeds
    c·S (≤ n rounds). Pure; zero weights stay 0; an empty dict returns {}."""
```

Spec 10 owns `polarity_7d` and its per-item weight formula (02 §C4, 10 §3.4, frozen in `ts-1.0`), and 09 only supplies the inputs:
- The weight is `w = cred × (1 + log1p(engagement))`.
- `cred` = `social_posts.author_credibility` (§2.7; NULL → 0.7, the unprofiled level) for X posts, and 1.0 for team announcements.
- `engagement` = `likes + 2·reposts + replies + quotes` (NULL → 0).
- Weights are summed per `author_id`, and the single-account cap of 25 % (master §7.3) is applied by this helper's water-fill. 10 §3.4 calls `cap_author_share` (a one-shot `min(W_a, 0.25·ΣW)` would leave a 70 % author at 45 %).

09 computes no polarity weights of its own. `X_AUTHOR_SHARE_CAP` is the default `cap`. Unit-tested in §9.1.

### 2.9 Free cross-check: subnetalpha `.ctf` tweet feeds

Profile HTML (`https://subnetalpha.ai/subnet/{slug}/`, ~230 KB) server-renders ~12 latest tweets inside `div.ctf-item` when `twitter_embed_id` is set (74 pages at research time, 63 on 2026-10-18; research 01-web-subnetalpha §1 — **no longer true live, see the 2026-10-18 probe at the end of this section**). Parser (`app/collectors/x_ctf.py::parse_ctf(html) -> list[CtfTweet]`, `selectolax` or `lxml`):
- `.ctf-item a[href*="/status/"]` → `post_id` = the `/status/(\d+)` group (the only reliable id); `event_at = snowflake_to_datetime(post_id)` = `(id >> 22) + 1288834974657` ms — exact, so the year-less `.ctf-tweet-date` ("16 Sep") is only a sanity check.
- `.ctf-tweet-text` → `text`; the author handle comes from the item's profile link (`x.com/{H}`, parsed by `normalise_x_handle`); the `.ctf-author-name` selector is **UNVERIFIED** (research names only `.ctf-tweet-text`/`.ctf-tweet-date`); engagement counts absent → `NULL`.
- **Handle match:** an item is kept only when its author handle equals (case-insensitively) one of the page occupant's active `watch_channels(platform='x')` handles. Other items (retweets of other accounts, or a feed configured for a different account) get no row; a page with items but no match emits `x_ctf_discrepancy` with extra payload keys `reason='handle_mismatch'`, `ctf_handle`.
- Row: `social_posts(platform='x', post_id, source='subnetalpha_ctf', occupant_key=<page's occupant>, is_team_account=1, author_credibility=1.0, bot_filtered=0, author_id = x_accounts.author_id if the handle is resolved else 'h:{handle_lower}', event_at = snowflake time, observed_at = run time)`, engagement `NULL`; §2.6's conflict rule lets a paid provider later enrich it and replace the placeholder author id.
- Outputs: (a) with X inactive (`X_PROVIDER=null`, the default) and `X_CTF_FALLBACK_METRICS=true`, the producer fns accept `source='subnetalpha_ctf'` rows for `x_cadence_30d`/`x_last_activity_days`, but only when the occupant's page was fetched successfully within the last 3 d — otherwise the leg is `failed` (§4). This is why the job runs daily: at a monthly cadence the X leg would report up to ~30 d of silence for accounts that post daily (a false `silence_45d` risk). The 12-item window truncates cadence above 12 (§4); (b) with a paid provider, the job compares `max(event_at)` from ctf vs provider for the same handle; a gap > 7 d in either direction emits `x_ctf_discrepancy` (`warning`, `reason='gap'`) — catches a wrong handle in `watch_channels` or a stale plugin cache; (c) handle liveness: a feed whose newest tweet is > 90 d old lowers nothing by itself — cadence already reflects it.

Cost: 63 GETs/day (live count 2026-10-18; ≈ 10 MB/day at ~160 KB/page) through provider `subnetalpha` at 1 req/s (shared with spec 04). Cache-Control is public; a plugin-cached feed may lag hours.

**Live probe 2026-10-18 (pre-build review) — the feeds are no longer server-rendered.** Of the 132 subnetalpha records 63 have a non-empty `twitter_embed_id` (its value is the plugin's feed id, e.g. `106` for Beam, rendered as `data-ctf-style="106"`), not 74. On 10 of those pages (beam, connitoai, nodexo, minos, green-compute, claims, soma, ditto, affine, sundae_bar) the HTML carries only the plugin's CSS (`.ctf-feed-106 .ctf-item{…}`): **zero** `div.ctf-item` elements and zero `/status/` links. The research-time markup (Sept 2026) is gone; whether the plugin now loads tweets client-side through `admin-ajax.php` was not probed (an undocumented AJAX action is out of scope, master §1.2). Consequences, binding for the build:
- **Feed presence gates the fallback.** Every fetch writes `job_state('x_ctf_crosscheck', 'page:{occupant_key}') = {fetched_at, http_status, n_items, n_matched, newest_event_at}` (a runtime watermark, 02 §B12). Output (a) accepts ctf rows only when a fetch within the last 3 d had `http_status = 200` **and `n_matched ≥ 1`**. A page fetched OK with no feed items is "no feed" — never a measured silence — so the X legs stay `failed` (`reason='ctf_no_feed'`). Without this rule every handle-holder would read `x_cadence_30d = 0` and drift into `silence_45d` on the default keyless path.
- With today's site the fallback therefore never fires: with `X_PROVIDER=null` the X legs are `failed` for occupants with a handle (lower `confidence`, no penalty) and `absent` for occupants without one — 02 §C3's intended keyless behaviour.
- The job stays scheduled (63 cheap GETs/day) as the detector for the markup coming back; `JobOutcome.note` reports `pages=…, with_feed=…, items=…`, and the parser keeps the research selectors (`.ctf-item`, `a[href*="/status/"]`, `.ctf-tweet-text`; `.ctf-author-name` and the author link stay **UNVERIFIED** — no live item exists to verify them). A test fixture of the current markup (CSS only) must parse to 0 items without error.

---

## 3. Data contract

All DDL lives in 03 (02 §B7); 03 adopted this spec's tables with the renames in its Appendix A (e.g. `published_at` → `event_at`, `calls` → `requests`, `created_at` → `account_created_at`). This section names the columns 09 reads and writes, using 03's names. Every timestamp is INTEGER unix seconds UTC (02 §B1); ids are TEXT (X snowflakes exceed 2^53).

### 3.1 Tables read

| Table | Columns used | Owner |
|---|---|---|
| `subnets` | `netuid, occupant_key, name, status, is_active, x_handle` | 04 |
| `occupants` | `occupant_key, status` | 04 |
| `watch_channels` | `id, occupant_key` (NULL = ecosystem-wide feed), `netuid, platform ∈ {x, youtube, youtube_video, rss, medium, substack}, handle, url, label, filtered, source, verified_at, meta_json` (YouTube: `channel_id`), `needs_resolution, first_seen_at, last_seen_at, retired_at`, generated `active` | 04 (identity rows); 09 writes `seed_feeds` rows, YouTube resolution and X `verified_at` (§3.2) |
| `identity_observations` | `occupant_key, source='subnetalpha', field ∈ {'slug','twitter_embed_id'}, value, observed_at` | 04 |
| `metric_values` | `stage='raw'` rows of `emission_share` (top-N selection, §2.5) | 05 (producer), 03 (DDL) |
| `chain_hourly` | `occupant_key, hour_ts, emission` (top-N fallback) | 05 |
| `collector_runs`, `feed_state`, `x_accounts`, `x_account_snapshots`, `social_posts`, `social_post_mentions`, `kol_accounts`, `news_items`, `video_items` | freshness and PIT inputs of the §3.3/§A.4 producer fns | 01 / 09 |

### 3.2 Tables written

DDL: 03 §3.2 (`x_accounts`, `x_account_occupants`, `x_search_state`, `feed_state`, `kol_accounts`, `watch_channels`) and 03 §3.5 (`social_posts`, `social_post_mentions`, `x_account_snapshots`, `news_items`, `video_items`).

| Table | What 09 writes | Notes |
|---|---|---|
| `x_accounts` | one row per resolved account: `author_id, handle, handle_lower, kind ('team'\|'kol'), kol_handle, status ('active'\|'unresolved'\|'gone'), since_id, resolved_at, last_polled_at, last_post_at, last_error, consecutive_failures, provider (= XProvider.name), created_at` | state. A handle that never resolved has no row (no `author_id` exists); its back-off lives in `job_state` (§2.5) |
| `x_account_occupants` | `(author_id, occupant_key, source='watch_channels')` | routing table, re-synced by every `x_team_poll` (§2.4) |
| `x_account_snapshots` | `author_id, observed_at, handle, followers, following, post_count, account_created_at, verified, provider, source='x_profile', event_at NULL, run_id, raw_json_hash` | append-only; the PIT source of `x_followers` |
| `x_search_state` | `query_key ('kol:{i}' \| 'mention:{occupant_key}'), since_id, last_run_at, last_posts, last_cost_usd` | state |
| `social_posts` | every column of 03's DDL except the generated `item_id`/`author_kind`; `platform='x'` only (08 never writes it, 02 §B11); `source ∈ {twitterapi_io, official_v2, subnetalpha_ctf}` | §2.6 upsert rule; indexes are 03's |
| `social_post_mentions` | `platform='x'` rows, `match_kind ∈ {team, query, rule}`, `event_at` copied from the post, `observed_at`, `run_id` | append-only; 08 writes its platforms, 10 writes the `entity` rows (02 §B11) |
| `kol_accounts` | seed rows (`added_by='seed'`, `INSERT OR IGNORE`), `author_id` on resolution, `updated_at` | operator edits to `credibility`, `verified`, `active`, `notes` are never overwritten by a re-seed |
| `watch_channels` | `seed_feeds` rows (§A.1); YouTube `meta_json.channel_id = UC…` + `verified_at` (§A.2; `handle` unchanged); `verified_at` of X rows when the handle resolves | 04 owns the identity rows |
| `news_items`, `video_items`, `feed_state` | Appendix A §A.3 | |
| `events` | §3.5, through 03's `emit_event` | |
| `metric_values` | nothing directly: 12's `compute_registered` writes the rows returned by the §3.3/§A.4 producer fns (`source` `x` / `rss`, 02 §B9) | |
| `provider_spend` | nothing directly: only 16's governor writes it, via the `record` calls of §6.4 (`provider, month, usd, requests, units_json {"posts", "profiles"}, updated_at`; `requests`, not `calls`, 02 §G5) | |
| `collector_runs` | via 01's wrapper; `note` carries `partial:<kind>` / `paused:budget reason=<r>` and the account counts (§6.4) | |

### 3.3 Metric contributions (producer fns in `app/features/social_x.py`)

Contract (02 §C5, 12 §2.3): `async def compute_<metric_key>(db, occupant_key: str, as_of: datetime) -> MetricResult`, where `as_of` is 12's `T_eval` (tz-aware UTC) and every read uses `observed_at ≤ as_of` (PIT). A verifiable absence returns `MetricResult(0.0, value_json, absence_reason='no_x_account' | 'x_account_gone')` (→ `absent`); `value=None` with `value_json.status='not_applicable'` means undefined; any other `None` is `failed`; every non-`ok` result carries `value_json.reason`. Every fn is registered through `register_metric(...)` (§8.3 step 10), and 12 writes its rows with `source='x'` (02 §B9). **09 registers no producer for `announcement_cadence_30d` or `silence_days`**: 12 derives both from the legs (02 §C3, 12 §2.6). "Team posts" of O = `social_posts` rows with `is_team_account=1` and (`occupant_key = O` or a `social_post_mentions(platform='x', match_kind='team', occupant_key=O)` row with `observed_at ≤ as_of`); team posts are never `bot_filtered` (§2.7), and every other count reads `bot_filtered = 0` only. "Original" = `is_repost=0 AND in_reply_to IS NULL` (quotes count as original). Input-freshness constants (module constants, frozen with `spec_version`): `POLL_MAX_AGE_S` = 3 d (last successful poll of the account, or ctf fetch of its page), `SNAPSHOT_MAX_AGE_S` = (`X_PROFILE_REFRESH_DAYS` + 3) d (10 d by default). `x_accounts.status`/`last_polled_at` are state, not PIT: in 12's reconstructed mode they can leak across the catch-up window — accepted, the same class of deviation as the engagement refresh (02 §J2).

| metric_key | Definition (raw value written) | Role (12 registry) | Normaliser (spec 12) | Weight | Freshness |
|---|---|---|---|---|---|
| `x_cadence_30d` | count of original team posts with `event_at ∈ (as_of−30d, as_of]` | leg of `announcement_cadence_30d` (12 sums the `ok` legs; absent legs add 0; 12 §2.6) | none (the derived sum gets `logcap(x, 12)`) | 0 (the derived metric carries .25 of P5) | 3 d |
| `x_engagement_30d` | median over original team posts with `event_at` in 30 d of `likes + 2·reposts + replies` (master §7.3; quotes and views are collected but unused). Engagement is the latest observed value: re-read daily for twitterapi.io timelines (the latest 20 tweets come back every run); under `official_v2` it is observed once, 0–24 h after posting (no re-read budget), so lower and noisier — the bias is shared by the cross-sectional `rankg`. Look-ahead-flagged by 13 (02 §J2) | scored P5 | `rankg` cross-sectional | .10 | 3 d |
| `x_followers` | latest `x_account_snapshots.followers` with `observed_at ≤ as_of`, not older than `SNAPSHOT_MAX_AGE_S`; shared handle → the same value for each occupant; ≥ 2 team accounts → max | scored P5 | `logcap(x, 50_000)` | .05 | 3 d |
| `x_last_activity_days` | days from the newest team post of any kind (incl. fetched replies/reposts; `official_v2` excludes them by default) to `as_of`; a lower bound when none was ever observed (§4) | leg of `silence_days` (12 takes the min over exact legs, then over lower-bound legs; 12 §2.6) | none (derived: `recency(d, 7, 45)`) | 0 (the derived metric carries .15 of P5) | 3 d |
| `x_mentions_7d` | count of distinct non-team posts with `bot_filtered=0`, a `social_post_mentions` row for the occupant (any `match_kind`, `observed_at ≤ as_of`) and `event_at` in 7 d | **evidence only** (weight 0, 12 row 52). It is not an input to `attention_7d`. Spec 10 §3.4 builds `attention_7d` from TMC trending views (05's `chain_trending_daily.views`) plus its own labelled, entity-linked mention count (`log1p` of the daily items in `post_labels`), averaged as two z-scores. | none | 0 | 3 d |
| `x_kol_posts_30d` | count of distinct `kol_accounts.handle` (`verified=1`, `active=1`, `credibility ≥ 0.5`) with a mention row for the occupant and `event_at` in 30 d, excluding KOLs whose `team_occupant_keys` contains this occupant | label-free fallback leg of 10's `kol_coverage_30d` (12 `fallback_key`) | `logcap(x, 5)` (the primary's) | 0 | 3 d |
| (inputs only) | `social_posts` (credibility, engagement, `is_team_account`, `is_kol`, `kol_handle`, `bot_filtered`), `social_post_mentions`, `kol_accounts.credibility`, `cap_author_share()` | read by 10 for `polarity_7d`, `attention_7d`, `kol_coverage_30d`, `event_impact_14d` (02 §C4) | 10 / 12 | — | — |

### 3.4 KOL seed list (`seed/kol_accounts.csv`, written by `scripts/build_kol_list.py`)

Columns: `handle,display_name,category,credibility,verified,team_occupant_netuids,notes`. `verified=1` in the seed means the handle was confirmed by search in research 01-web-ecosystem-2026 §4 (checked 2026-09-23: 32 confirmed, 5 marked UNVERIFIED there — matching the table); it does not vouch for the role, credibility or team mapping. `verified=0` rows are **not polled** and do not count for `kol_coverage_30d` until the operator flips `verified=1` (their `credibility` is 0.0 in the seed so the `≥ 0.5` gate excludes them even if `verified` is toggled without reviewing credibility). Netuid → `occupant_key` resolution happens at load time via `subnets`. Team mappings backed by research: `const_reborn` → Affine SN120, `webuildscore` → Score SN44, `QuasarModels` → Quasar SN24; the others (Macrocosmos, Manifold → SN4, Taoshi → SN8, RedTeam → SN61) are the operator's to confirm at load (checklist D3).

| handle | display_name | category | cred. | verified | team SN | notes |
|---|---|---|---|---|---|---|
| opentensor | OpenTensor Foundation | official | 0.90 | 1 | | protocol foundation |
| bittensor_ | Bittensor | official | 0.90 | 1 | | protocol account |
| taostats | Taostats | explorer | 0.85 | 1 | | canonical explorer |
| learnbittensor | Learn Bittensor | official | 0.80 | 1 | | docs |
| const_reborn | Jacob Steeves (Const) | founder | 0.85 | 1 | 120 | ex-OTF CEO; builds Affine SN120 — own-subnet posts excluded from KOL coverage |
| mogmachine | mogmachine | founder | 0.75 | 1 | | Taostats founder |
| MacrocosmosAI | Macrocosmos | founder | 0.70 | 1 | 1,9,13 | multi-subnet operator; 25/37 dropped 2026-10-18 (re-registered as UR / Aurelius) |
| manifoldlabs | Manifold Labs | founder | 0.70 | 1 | 4 | Targon |
| taoshiio | Taoshi | founder | 0.60 | 1 | 8 | legacy SN8 operator; SN8 now Vanta — verify mapping |
| webuildscore | Score | founder | 0.60 | 1 | 44 | |
| _redteam_ | RedTeam | founder | 0.60 | 1 | 61 | |
| CrucibleLabs | Crucible Labs | founder | 0.55 | 1 | | subnet mapping unknown → operator |
| QuasarModels | Quasar | founder | 0.30 | 1 | 24 | 2026-07-31 SOTA claim, audited days later as 98 % inherited weights (early Aug 2026); low credibility by design |
| YumaGroup | Yuma (DCG) | fund | 0.75 | 1 | | validator + fund; conflicted but informed |
| DCGco | Digital Currency Group | fund | 0.60 | 1 | | |
| BarrySilbert | Barry Silbert | fund | 0.60 | 1 | | |
| xtaohq | xTAO | fund | 0.65 | 1 | | treasury co |
| latentholdings | Latent Holdings | fund | 0.70 | 1 | | owns tao.app, Latent 11 |
| gtaoventures | GTAO Ventures | fund | 0.55 | 1 | | |
| TrustedStake | TrustedStake | community | 0.55 | 1 | | validator |
| 0xSammy | 0xSammy | analyst | 0.80 | 1 | | TAO Institute; 89k followers |
| KeithSingery | Keith Singery | analyst | 0.75 | 1 | | Bittensor Guru podcast; validator |
| here4impact | Michael D. White | analyst | 0.60 | 1 | | |
| CryptoZPunisher | CryptoZPunisher | analyst | 0.55 | 1 | | |
| bittingthembits | Andy ττ | analyst | 0.60 | 1 | | resource list maintainer |
| tzedonn | tzedonn | analyst | 0.55 | 1 | | |
| taomedia_ | tao.media | media | 0.65 | 1 | | ecosystem outlet |
| BSCNews | BSC News | media | 0.40 | 1 | | general crypto media; PR-heavy |
| TAOCommunityHub | TAO Community Hub | community | 0.50 | 1 | | |
| TAOFlows | TAO Flows | community | 0.50 | 1 | | |
| proofoftalk | Proof of Talk | media | 0.35 | 1 | | conference account; verified per research §4, not in the brief's core list |
| JesusMartinez | (unknown) | analyst | 0.35 | 1 | | listed verified in research §4 but role unclear → operator to confirm before raising credibility |
| yuma_ai | Yuma (alt) | fund | 0.00 | 0 | | **UNVERIFIED** handle; may duplicate YumaGroup |
| taoapp_ | tao.app | explorer | 0.00 | 0 | | **UNVERIFIED** handle |
| markjeffrey | Mark Jeffrey (Stillcore) | analyst | 0.00 | 0 | | **UNVERIFIED** handle |
| TaoPortal | TaoPortal | community | 0.00 | 0 | | **UNVERIFIED** handle |
| jaltucher | James Altucher | analyst | 0.00 | 0 | | **UNVERIFIED** handle (BlueTAO.ai) |

37 rows (32 verified, 5 unverified). Default credibility by category when the operator adds a row without one: official 0.9, founder 0.8, explorer 0.8, fund 0.7, analyst 0.6, media 0.6, community 0.5.

### 3.5 Events emitted (catalog = 03 §3.9: names, required payload keys, dedupe keys, alert column; severity `info | warning | critical`, 02 §B2)

| event_type | `source` (02 §B9) | severity | routing (02 §D4 / 03) | occupant_key | payload (03's required keys; extra keys allowed) |
|---|---|---|---|---|---|
| `x_account_unresolved` | `x` | warning | digest | the handle's occupant; NULL for a KOL | `{handle, kind, reason: 'not_found'\|'suspended'\|'error', consecutive, provider}` |
| `x_account_gone` | `x` | warning | immediate (03) | the occupant when exactly one is mapped, else NULL + extra `occupant_keys[]`; NULL for a KOL | `{handle, author_id, last_post_at, since}` — after 2 consecutive `None` profile refreshes; 09's fns then return `absent` (`x_account_gone`) |
| `x_handle_changed` | `x` | info | stored only | as above | `{author_id, old_handle, new_handle}` |
| `x_budget_paused` | `x` | warning | **digest** for every reason (02 §D4: the page for a budget stop is 16's `spend_cap_reached`; 03's "yes if pacing/provider_balance" is superseded) | NULL | `{provider, usd_mtd, cap_usd, projected_usd, reason: 'hard_cap'\|'pacing'\|'zero_cap'\|'provider_balance'}` — at most once per provider per month (dedupe `provider\|month`) |
| `x_budget_resumed` | `x` | info | stored only | NULL | `{provider, month}` — first allowed `check` after an `x_budget_paused` of the same month |
| `x_ctf_discrepancy` | `x` | warning | digest | occupant | `{handle, ctf_last_post_at, provider_last_post_at, gap_days}` + extras `reason: 'gap'\|'handle_mismatch'`, `ctf_handle` |
| `feed_dead` | `rss` | warning | digest | occupant / NULL | `{feed_url, kind, consecutive_failures, last_status}` — at `RSS_DEAD_AFTER_FAILURES=10` (≈ 2.5 d at 6 h) |
| `youtube_channel_resolved` | `rss` | info | stored only | occupant | `{input_url, channel_id, method}` |

`observed_at` = job time; `event_at` = the timestamp of the underlying observation (for `x_account_gone`: the second failed refresh). Spend events (`spend_80pct`, `spend_cap_reached`, `spend_reset`) are emitted by 16's governor, never by 09; a whole-run failure pages through 01's `collector_failing`.

---

## 4. Absence-vs-failure table

| metric_key | **0.0 `absent`** — verifiable absence (`absence_reason`) | **None** — `failed` (collection failure) or `not_applicable` | value — `ok` (a measured 0 is `ok`, not absent) |
|---|---|---|---|
| `x_cadence_30d` (leg) | `no_x_account`: no active `watch_channels(platform='x')` row for the occupant with `first_seen_at ≤ as_of` and (`retired_at` NULL or `> as_of`) — the PIT test 12 §2.6 also uses — other than `legacy` rows (§2.4); `x_account_gone`: every mapped handle is gone (`x_accounts.status='gone'`, or a declared handle that resolved as `not_found`/`suspended` twice ≥ `X_UNRESOLVED_RETRY_DAYS` apart, §2.5) | `failed`: (a) X inactive (`X_PROVIDER=null`, key missing, `X_ENABLED=false`) and no usable ctf rows (page not fetched OK within 3 d, or `X_CTF_FALLBACK_METRICS=false`); (b) a mapped handle not yet resolved or failing for a non-404 reason; (c) no successful poll of the account within 3 d (429s, budget pause, auth failure, kill switch) | count; 0 when the account was polled OK within 3 d and posted no original post in 30 d. ctf-only source with all 12 feed items inside the window → **12**, `value_json.truncated=true` (a lower bound) |
| `x_engagement_30d` | never absent | `not_applicable` (reason `no_x_account` / `x_account_gone` / `no_posts_30d`): no account, gone account, or 0 original posts in 30 d; `failed`: any `failed` condition of `x_cadence_30d`, or ctf-only source (no engagement fields) | median; **0.0 is legitimate** when posts exist and all counts are 0 |
| `x_followers` | `no_x_account` / `x_account_gone` (as `x_cadence_30d`; 02 §C3: `x_followers = 0`) | `failed`: no snapshot with `observed_at ≤ as_of` within `SNAPSHOT_MAX_AGE_S` (never resolved, X inactive, refresh failing) | followers |
| `x_last_activity_days` (leg) | `no_x_account` / `x_account_gone` (as `x_cadence_30d`) | `failed`: conditions (a)–(c) of `x_cadence_30d` | exact: days since the newest team post; **lower bound** when the account was polled OK within 3 d but no team post was ever observed: days since `x_accounts.resolved_at`, `value_json.lower_bound=true` (12 §2.6 uses exact legs first; this settles old Q7) |
| `x_mentions_7d` (evidence) | — | `failed`: X inactive; the mention search of a top-N occupant failed or was paused for > 3 d | count; tail occupants (not searched) get the count seen through KOL/team rule matches with `value_json.searched=false` |
| `x_kol_posts_30d` (fallback leg) | — | `failed`: X inactive; no successful `x_kol_poll` within 3 d; fewer than 5 `verified=1` KOL rows | count; 0 when KOL polling ran and no credible KOL mentioned the occupant |
| `news_mentions_7d` (evidence) | — | `failed`: `RSS_ENABLED=false`; or ≥ 50 % of enabled feeds failed in the last 24 h **and** (for a top-N occupant) its own Google News query failed | count; 0 when the feeds ran and nothing matched (tail occupants: global/publisher/ecosystem feeds only) |
| `video_uploads_90d` (evidence) | `no_youtube_channel`: no YouTube channel in any source | `failed`: channel known but `channel_id` unresolved; RSS failing > 7 d | count; 0 when RSS was fetched OK within 7 d and nothing was uploaded in 90 d |
| `blog_posts_90d` (evidence) | `no_blog_feed`: no Medium/Substack/blog feed in any source | `failed`: feed failing > 7 d | count (0 is `ok`) |
| `video_mentions_30d` (evidence) | — | `failed`: Novelty Search playlist feed failing > 7 d | count (0 is `ok`) |

General rule enforced in code: a producer fn first decides *absence* from reference tables (`watch_channels` PIT columns, `x_accounts.status`, the §2.5 back-off state), then *failure* from input freshness (`x_accounts.last_polled_at`, ctf fetch time, `feed_state`), and only then computes. An inactive provider is a *failure to collect* (None) for occupants **with** a handle, never absence — a keyless build shows their X metrics as missing with lowered `confidence` instead of penalising them — while occupants **without** a handle are `absent` regardless of the provider (02 §C3).

---

## 5. Configuration

All in `app/config.py::Settings` (pydantic-settings, `UPPER_SNAKE`, `extra="ignore"`); consumers read `app.config.settings` dynamically. Validators follow the old bot's pattern (`E:\SN_Trader\SubnetTrader\app\config.py:637-662`).

| Key | Type | Default | Validator / notes |
|---|---|---|---|
| `X_ENABLED` | bool | `False` | master switch; 01 registers every job regardless, and the paid X jobs report `disabled` while it is False |
| `X_PROVIDER` | str | `"null"` | `∈ {null, twitterapi_io, official_v2}` (lower-cased; = the 02 §G1 provider id); an unknown value fails validation; a missing key (`TWITTERAPI_IO_KEY` / `X_BEARER_TOKEN`) makes X inactive with a warning (01's fail-safe validator; `settings.x_active()` is False) — the boot never fails over an optional key |
| `TWITTERAPI_IO_KEY` | str | `""` | secret; never logged |
| `TWITTERAPI_IO_BASE_URL` | str | `https://api.twitterapi.io` | |
| `X_BEARER_TOKEN` | str | `""` | secret |
| `X_OFFICIAL_BASE_URL` | str | `https://api.x.com/2` | |
| `X_OFFICIAL_EXCLUDE` | str | `"retweets,replies"` | passed to `exclude=`; saves per-post billing; `"retweets"` to keep replies |
| `X_MONTHLY_CAP_USD` | float | `25.0` | `≥ 0`; read by 16's governor for group `x`. **0 disables every paid X call** (governor `zero_cap`, fail-closed, 02 §F2). Checklist C1: 25 for twitterapi.io; C2: 75 with `official_v2` → boot warning when `X_PROVIDER=official_v2` and `0 < cap < 75` (§6.3's lean plan needs ≈ $70) |
| `X_SOFT_ALERT_PCT` | float | `0.80` | `0 < x < 1`; threshold of 16's `spend_80pct` for group `x` (16 §2.10 reads it instead of `COST_SOFT_ALERT_PCT`); 09 sends no soft alert of its own |
| `X_PACING_ENABLED` | bool | `True` | 16's pacing flag for group `x` (16 §2.10 decision rule 7) |
| `X_PRICES_JSON` | str | `""` | optional JSON override of `x_providers/prices.py`, e.g. `{"twitterapi_io":{"per_tweet":0.00015,"per_profile":0.00018,"min_call":0.00015},"official_v2":{"per_post":0.005,"per_user":0.010}}`; unknown keys fail validation |
| `X_TEAM_ENABLED` | bool | `True` | |
| `X_TEAM_POLL_CRON` | str | `"01:30"` | `HH:MM` UTC, daily |
| `X_TEAM_POLL_MODE` | str | `"timeline"` | `∈ {timeline, search}` |
| `X_TIMELINE_MAX_PAGES` | int | `3` | `1..10`; pages per account per run (twitterapi.io 20/page; official `max_results=20`) |
| `X_KOL_ENABLED` | bool | `True` | |
| `X_KOL_POLL_CRON_HOURS` | str | `"0,6,12,18"` | comma-separated UTC hours 0–23; minute fixed at 30 |
| `X_KOL_POLL_MODE` | str | `"search"` | `∈ {timeline, search}` |
| `X_MENTIONS_ENABLED` | bool | `True` | checklist C2 sets `False` with `official_v2` at cap 75 (§6.3; 02 §I2) |
| `X_MENTIONS_TOP_N` | int | `40` | `1..128` |
| `X_MENTIONS_POLL_CRON` | str | `"02:00"` | `HH:MM` UTC, daily |
| `X_SEARCH_MAX_POSTS_PER_QUERY` | int | `200` | hard page-out per query per run |
| `X_PROFILE_REFRESH_CRON` | str | `"sun 02:30"` | `dow HH:MM` UTC (same format as `QUANT_WEEKLY_JOB_CRON`) |
| `X_PROFILE_REFRESH_DAYS` | int | `7` | `1..30`; an account is re-read when its newest snapshot is older than this − 1 d, so 30 gives a monthly refresh from the weekly job; `x_followers` accepts snapshots up to this + 3 d old |
| `X_UNRESOLVED_RETRY_DAYS` | int | `7` | |
| `X_MIN_CREDIBILITY` | float | `0.30` | `bot_filtered` threshold; changing it does **not** rewrite stored rows (flag is stamped at observe time; spec 12 may re-derive from `author_credibility`) |
| `X_AUTHOR_SHARE_CAP` | float | `0.25` | `0 < x ≤ 1`; default `cap` of `cap_author_share` (§2.8) |
| `X_DUPLICATE_WINDOW_MIN` | int | `10` | |
| `X_RATE_PER_SEC` | float | `1.0` | limiter provider `twitterapi_io` (01 §6 table, burst 2) |
| `X_OFFICIAL_RATE_PER_SEC` | float | `0.25` | limiter provider `official_v2` (burst 2) |
| `X_HTTP_TIMEOUT_SEC` | float | `20.0` | passed as `timeout=` to `Http.request` |
| `X_RAW_ARCHIVE_ENABLED` | bool | `True` | gz JSONL under `data/raw/x/` |
| `X_CTF_CROSSCHECK_ENABLED` | bool | `True` | free; independent of `X_ENABLED` (runs with X inactive) |
| `X_CTF_CROSSCHECK_CRON` | str | `"07:00"` | `HH:MM` UTC, daily (daily is required for the fallback metrics, §2.9) |
| `X_CTF_FALLBACK_METRICS` | bool | `True` | ctf rows feed `x_cadence_30d`/`x_last_activity_days` only while X is inactive |
| `KOL_SEED_PATH` | str | `"seed/kol_accounts.csv"` | loaded at boot and by `scripts/build_kol_list.py --load` |
| `RSS_ENABLED` | bool | `True` | Appendix A master switch |
| `RSS_POLL_INTERVAL_HOURS` | int | `6` | |
| `RSS_GOOGLE_NEWS_ENABLED` | bool | `True` | |
| `RSS_GOOGLE_NEWS_TOP_N` | int | `40` | per-subnet query count |
| `RSS_GOOGLE_NEWS_POLL_CRON` | str | `"05:00"` | |
| `RSS_GOOGLE_NEWS_DECODE_URLS` | bool | `False` | when True, try to resolve `news.google.com/rss/articles/…` to the publisher URL (§A.2); every attempt is a request to Google, so the default dedupes by title + publisher instead |
| `RSS_FEEDS_SEED_PATH` | str | `"seed/feeds.csv"` | |
| `RSS_EXTRA_FEEDS` | str | `""` | comma-separated extra feed URLs (ecosystem-wide) |
| `RSS_RATE_PER_SEC` | float | `0.5` | limiter provider `rss` (all feeds, including Google News; 02 §G1 has no separate Google provider) |
| `GOOGLE_NEWS_MIN_INTERVAL_SEC` | float | `10.0` | per-host spacing for `news.google.com`, enforced inside the collector on top of the `rss` limiter |
| `RSS_HTTP_TIMEOUT_SEC` | float | `20.0` | |
| `RSS_DEAD_AFTER_FAILURES` | int | `10` | → `feed_dead` event; feed keeps being retried daily |
| `NEWS_KEYWORDS` | str | `"bittensor,dtao,subnet,$tao,taostats,opentensor"` | publisher-feed keyword filter (case-insensitive, any) + registry names |
| `YOUTUBE_ENABLED` | bool | `True` | |
| `YOUTUBE_POLL_CRON` | str | `"05:30"` | daily |
| `YOUTUBE_NOVELTY_PLAYLIST_ID` | str | `"PLN0WJc6io8zqEknIutfyEjiJViazrxrzU"` | Novelty Search playlist (research 01-web-ecosystem-2026 sources) |
| `YOUTUBE_RATE_PER_SEC` | float | `0.33` | limiter provider `youtube` (RSS + channel-id discovery) |
| `YOUTUBE_API_KEY` | str | `""` | optional secret; when set, channel-id discovery first uses Data API v3 `channels.list?forHandle=` / `videos.list` (1 unit each; 10,000 units/day free, research §4), which avoids the EU consent wall |

**Pause semantics** (01's wrapper + 16's `effective_pause()`, 02 §B12; the first match is the reported reason): disabled flag (§2.5; `last_skip_reason="disabled"`, not a pause) → `pause_all` (`./PAUSE_ALL`) → `kill_switch` (`./KILL_SWITCH`; paid X jobs only — the governor also denies every paid `check`) → `operator` (a `control_state` row for the job id or for the group `x`/`rss`, written by API, Telegram or CLI) → `emergency_file` (`data/control/<job>.paused`, `data/control/x.paused` or `data/control/rss.paused`; read-only inputs the app never writes; there is no `X_KILLED` file or `X_KILL_SWITCH_PATH`) → `budget` (group `x` exhausted or its cap is 0). A paused job makes no call and writes no `collector_runs` row; `/health.collectors.<id>.paused` shows the reason. Inside a run, a governor denial ends the run early (`note='paused:budget reason=<hard_cap|pacing|zero_cap>'`, not a failure), and a provider `auth` error fails the run (§7); neither is a separate pause state. Pause targets are exact job ids or 16's named `PAUSE_GROUPS` (`x` = the five X jobs of §2.5; `rss` = the three appendix jobs, proposed to 16), never prefixes.

---

## 6. Cadence, rate limits, cost

### 6.1 Limiter providers (01 `ProviderLimiter`; ids per 02 §G1)

| Provider id | Rate | Used by |
|---|---|---|
| `twitterapi_io` | `X_RATE_PER_SEC` = 1.0/s, burst 2 | all twitterapi.io calls |
| `official_v2` | `X_OFFICIAL_RATE_PER_SEC` = 0.25/s (15/min), burst 2; `x-rate-limit-*` honoured via `ProviderLimiter.observe` | all official v2 calls (one id for every endpoint family) |
| `subnetalpha` | 1.0/s (01) | spec 04 (WP-REST) + `x_ctf_crosscheck` |
| `rss` | `RSS_RATE_PER_SEC` = 0.5/s; plus `GOOGLE_NEWS_MIN_INTERVAL_SEC` (10 s) per-host spacing for `news.google.com` inside the collector | publisher, ecosystem, Medium/Substack and Google News feeds |
| `youtube` | `YOUTUBE_RATE_PER_SEC` = 0.33/s | channel/playlist RSS, oEmbed, channel-id discovery, optional Data API |

Every request goes through `Http.request(provider_id, …)` (01, 02 §G1); no module builds its own client or limiter, and `Http` counts every request into `provider_spend.requests`.

### 6.2 Volume assumptions

`N_team ≈ 100` resolved team accounts (102 `twitterx_url` minus dead/dupes), `N_kol = 32` verified, `N_mention = 40`. Baseline **3 posts/account/day** (brief). Mentions: mean 15 credible matches/subnet/day (top subnets 50+, tail 0–2). Provider page sizes: twitterapi.io 20/page (bills every returned tweet); official `max_results=20` for timelines (5 on the first poll) and 100 for search (bills returned posts only).

### 6.3 Monthly cost (30 days) by provider and plan

| Component | twitterapi_io (default plan) | official_v2 (default plan) | official_v2 "lean" |
|---|---|---|---|
| Team timelines | 100 calls/day × 20 billed × $0.00015 = **$0.30/day → $9.00** (search mode instead: 4 packed queries returning ≈ 300 posts in total = $0.045/day → $1.35) | 300 posts/day × $0.005 = **$1.50/day → $45.00** (`exclude=retweets,replies`) | $45.00 |
| KOL (search, 6 h) | 2 packed queries × 4/day; 32 × 5 posts = 160 posts/day → $0.024/day → **$0.72** | 160 × $0.005 = $0.80/day → **$24.00** (cadence does not change the bill; per-post) | $24.00 |
| Mentions (top-40 daily) | 600 posts/day → $0.09/day → **$2.70** | 600 × $0.005 = $3.00/day → **$90.00** | disabled (`X_MENTIONS_ENABLED=false`) → $0 |
| Profiles | 132 × $0.00018 × 4.33 weeks → **$0.10** | 132 × $0.010 × 4.33 → **$5.72** | monthly (`X_PROFILE_REFRESH_DAYS=30`) → $1.32 |
| One-off first poll | 100 × 20 × $0.00015 = $0.30 | 100 × 5 × $0.005 = **$2.50** (§2.3; was $50 with a 100-post first page) | $2.50 |
| **Total** | **≈ $12.5 / month** (cap 25) | **≈ $165 / month** — does not fit cap 75 | **≈ $70 / month** (cap 75); up to ≈ $75 if zero-result calls bill $0.005 (≈ 30 empty team calls/day, **UNVERIFIED**) — pacing may then pause the last days of a month |
| Worst case | teams 3 pages each ($0.90/day) + mentions at `X_SEARCH_MAX_POSTS_PER_QUERY=200` × 40 ($1.20/day) + KOL 8 × 200 ($0.24/day) = **$2.34/day → $70** → governor halts ≈ day 10 (earlier with pacing) | teams 100 × 60 posts (page bound) × $0.005 = $30/day → governor halts on day 3 | same |

Hard cap `X_MONTHLY_CAP_USD` binds in every row; the plan tables only say what fits without pauses. Under `official_v2` the boot warning (§5) tells the operator to set `X_MENTIONS_ENABLED=false` and cap 75 (checklist C2, 02 §I2).

### 6.4 Cost governor integration (`app/costs.py`, owned by spec 16; API = 02 §G5)

09 calls exactly two governor functions, keyword-only after the USD argument (16 §2.10); it never reserves, commits or releases (no hold — 02 §G5):

```python
d = await costs.check(provider.name, est)        # Decision: allowed, reason ∈ {None, hard_cap, pacing, kill_switch, zero_cap},
                                                 #           usd_mtd, reserved_usd, cap_usd, projected_usd
await costs.record(provider.name, actual, requests=0, units={"posts": n} | {"profiles": n},
                   job=job_id, run_id=run.run_id)  # requests=0: Http already counted the request (= the G5 default; explicit on purpose)
```

Collector sequence per paid call: `est = provider.cost_estimate(call)` → `d = await costs.check(...)` → if `not d.allowed`: stop making paid calls, finish writing what was fetched, emit `x_budget_paused` (reasons `hard_cap | pacing | zero_cap`; nothing for `kill_switch`) and return `JobOutcome(rows, cost_usd, note='paused:budget reason=<r>')`; else call → `actual = provider.cost_actual(call, page)` → `record(...)`. `JobOutcome.cost_usd` = Σ `actual` of the run, so 16's wrapper reconciliation (`run_total(run_id)`) books nothing extra. The decision rule (hard cap, cap 0, pacing with `X_PACING_ENABLED`), the hard latch, `spend_cap_reached`, the 80 % `spend_80pct` alert (threshold `X_SOFT_ALERT_PCT`), month rollover and overrides are 16's — 09 restates none of them and sends no soft alert. `x_budget_resumed` is emitted by the first allowed `check` of a month that has an `x_budget_paused` for the provider. **Degradation under pressure:** when a `Decision.projected_usd > 0.9 × cap_usd`, `x_mentions_poll` uses top-20 instead of `X_MENTIONS_TOP_N` for the rest of the month (logged). A twitterapi.io 402 (vendor balance empty) is not a governor decision: the run fails and `x_budget_paused(reason='provider_balance')` is emitted. Balance reconciliation through a vendor "my info"/credits endpoint is **UNVERIFIED** — if it exists, `x_profile_refresh` logs the vendor balance next to `usd_mtd`.

`/health` is 01's schema (02 §G6); 09 adds nothing to it. X spend appears as 16's `spend.x` group object (`usd, cap_usd, pct, paused, reserved_usd`, + optional extras), job state as `collectors.<job_id>` (`paused`, `last_skip_reason`, `last_error`, …). Account counts (team / KOL / unresolved / gone, posts in 24 h) go into the run's `JobOutcome.note` and `python -m app.collectors.x --status`.

### 6.5 Appendix A cadence

`rss_feeds_poll` every 6 h over ~25 ecosystem/publisher feeds + ~30 per-occupant blog feeds: ≤ 60 conditional GETs per run, most 304 → ~10 KB/run. `news_google_poll` daily: 2 global + 40 per-subnet queries = 42 GETs at ≥ 10 s spacing ≈ 7 min (URL decoding is off by default; when on, ≤ 42 × ~10 more Google requests at the same spacing — too slow for the 1,800-s timeout, so decoding is capped at 60 items/run). `youtube_poll` daily: ~40 channel RSS + 1 playlist. All free; `Http` counts the requests, no USD is recorded. Wayback/GitHub are not touched here.

---

## 7. Failure modes and fail-safe behaviour

| Failure | Behaviour | Metric effect | `/health` / alert |
|---|---|---|---|
| Provider raises `auth` (401/403) | the run stops at the first one and raises (`ok=0`, error `auth:<status>`; call-count = 1); the next scheduled run tries once more | None for accounts not polled (3-d input freshness) | 01's `collector_failing` at `JOB_FAIL_ALERT_THRESHOLD` (5) consecutive failures; `collectors.<job>.last_error` |
| 402 (twitterapi.io balance empty) | the run raises (`ok=0`, error `provider_balance`); `x_budget_paused(reason='provider_balance')` | as above | digest now; `collector_failing` after 5 runs |
| 429 / `rate_limited` | `Http` penalises by `Retry-After`/`x-rate-limit-reset` and retries (≤ `HTTP_MAX_RETRIES`); still failing → end the run with `note='partial:rate_limited'`, remaining accounts next run | affected accounts stale → None after 3 d | partial ≠ failure; logged |
| 5xx / timeout | retried by `Http` (1 s, 4 s); then the account's `consecutive_failures += 1`, continue with the next account | as above | `ok=1` with `note='partial:transient'`, unless > 50 % of accounts failed → raise (`ok=0`) |
| Governor denial (`hard_cap`, `pacing`, `zero_cap`) | the run ends early (`note='paused:budget reason=…'`); hard cap/cap 0 → later runs skip `paused: budget` until rollover, a raised cap or an override (16); pacing is re-evaluated per call | None after 3 d, **never 0** | 16's `spend_cap_reached` (page, hard cap) + `x_budget_paused` (digest, 02 §D4) |
| Official search `since_id` older than 7 d (laptop off > 1 week) | dropped and replaced by `start_time = now − 6 d 23 h` (§2.3, **UNVERIFIED** API behaviour) | mentions older than 7 d are lost | logged |
| Provider parse error (field rename) | `XProviderError(parse)`; raw archived; the run raises (`ok=0`) | None | `collector_failing` at 5; fix `FIELD_MAP` |
| Account 404 / suspended | `resolve_user → None` → `status='unresolved'` (1st) / `'gone'` (2nd refresh) | `gone` → `absent` (`x_account_gone`) | `x_account_gone` (`warning`, immediate per 03) |
| Handle in `watch_channels` is wrong (points at a fan account) | not detectable automatically; ctf cross-check gap or handle mismatch → `x_ctf_discrepancy` | — | digest |
| Kill switch / `PAUSE_ALL` / operator pause / emergency file | the wrapper skips the job (`paused:<reason>`, §5); `KILL_SWITCH` leaves `x_ctf_crosscheck` and Appendix A running (free) | last-good within freshness, then None | `kill_switch_active`, `collectors.<job>.paused` |
| Laptop off at a cron time | 01/APScheduler misfire grace (§2.5: 3,600 s daily, 1,800 s for `x_kol_poll`): a run missed by less runs on boot, otherwise at the next fire time; `since_id` guarantees no gap (timeline, up to the page bound) — search mode uses `since_at = last_run_at − 1h`, so a 2-day outage back-fills 2 days (bounded by `X_SEARCH_MAX_POSTS_PER_QUERY`) | — | — |
| ctf fetch failing > 3 d with X inactive | fallback legs become `failed` (never a stale "silence" value, §2.9) | `x_cadence_30d`/`x_last_activity_days` None | `collector_failing` after 5 runs |
| Provider vendor disappears (Nitter-style C&D) | `X_PROVIDER=official_v2` or `null`; `x_accounts.provider` column records which provider resolved each id — ids are X-native and portable across providers; `since_id` remains valid | — | — |
| Duplicate posts across jobs (team post also hit by mention search) | PK `(platform, post_id)`; second observation refreshes engagement only | — | — |
| Feed 404/410/DNS | `feed_state.consecutive_failures += 1`; after `RSS_DEAD_AFTER_FAILURES` → `feed_dead`; retried daily thereafter | `blog_posts_90d` None after 7 d failing | digest |
| Google News 429/503 | back off, skip remaining queries this run | `news_mentions_7d` None only if the day's queries mostly failed (§4) | logged |
| YouTube consent/JS wall on channel-id discovery | `channel_id` stays NULL, retried weekly; operator can set `watch_channels.meta_json.channel_id` to the `UC…` id | `video_uploads_90d` None | digest |

Invariants: a job raises only for a whole-run failure, into 01's wrapper, which never raises into the scheduler; a job never writes a metric (spec 12 does); a job never forward-fills — a missing observation stays missing and `confidence` drops. `last-good` semantics live entirely in spec 12's freshness windows (X 3 d).

---

## 8. Implementation plan

### 8.1 Files

```
app/collectors/x.py                     XCollector + CollectorJob subclasses XTeamPollJob, XKolPollJob, XMentionsPollJob, XProfileRefreshJob; __main__ (--once, --status, --kol-candidates)
app/collectors/x_handles.py             normalise_x_handle() (wraps 04's app/registry/normalise.py), chunk_handles(handles, max_per_query=25, max_chars=512)
app/collectors/x_ctf.py                 XCtfCrosscheckJob, fetch_ctf_pages(), parse_ctf(html), snowflake_to_datetime(id)
app/collectors/x_providers/__init__.py  make_provider(settings, http)
app/collectors/x_providers/prices.py    PRICES (+ X_PRICES_JSON override)
app/collectors/x_providers/base.py      XUser, XPost, XPage, MentionQuery, XCall, XProvider, XProviderError
app/collectors/x_providers/twitterapi_io.py
app/collectors/x_providers/official_v2.py
app/collectors/x_providers/null.py
app/collectors/rss.py                   FeedPoller (feedparser + ETag), feed registry loader, RssFeedsPollJob
app/collectors/news.py                  Google News query builder, publisher keyword filter, URL canonicalisation/optional decoding, news_items writer, NewsGooglePollJob
app/collectors/youtube.py               channel-id discovery, channel/playlist RSS, video_items writer, YoutubePollJob
app/nlp/kol.py                          KolRegistry (load seed, lookup), author_credibility(), cap_author_share()
app/nlp/match.py                        match_occupants(), build_mention_query(), PROMO_RE, AMBIGUOUS_NAMES, url_canonical()
app/features/social_x.py                compute_x_cadence_30d, compute_x_engagement_30d, compute_x_followers, compute_x_last_activity_days,
                                        compute_x_mentions_7d, compute_x_kol_posts_30d; register_producers() (§8.3 step 10)
app/features/social_feeds.py            compute_news_mentions_7d, compute_video_uploads_90d, compute_video_mentions_30d, compute_blog_posts_90d
(imported, not defined here: app/nlp/textnorm.py (08), app/http.py + app/ratelimit.py (01), app/costs.py (16), app/features/types.py + registry.py (12))
scripts/build_kol_list.py               --write seed/kol_accounts.csv | --load | --names (regenerate ambiguous_names.txt from registry)
scripts/record_x_fixtures.py            one-shot recorder (redacts keys) for tests/fixtures/x/*
seed/kol_accounts.csv, seed/feeds.csv, app/nlp/ambiguous_names.txt
tests/collectors/test_x_providers.py test_x_collector.py test_x_handles.py test_x_ctf.py test_x_costs.py
tests/nlp/test_kol.py test_match.py   tests/collectors/test_rss.py test_news.py test_youtube.py   tests/features/test_social_x.py
```

### 8.2 Key signatures

```python
# app/collectors/x.py
class XCollector:
    def __init__(self, db: Database, provider: XProvider, costs: CostGovernor, http: Http, registry: SubnetRegistry, kols: KolRegistry, log): ...
    async def run_team_poll(self, run: RunContext) -> JobOutcome      # called by XTeamPollJob.collect(run); 01 §2.6
    async def run_kol_poll(self, run: RunContext) -> JobOutcome
    async def run_mentions_poll(self, run: RunContext) -> JobOutcome
    async def run_profile_refresh(self, run: RunContext) -> JobOutcome
    async def _resolve_new_accounts(self, kind: Literal['team','kol'], run_id: str) -> int
    async def _poll_timeline(self, acct: XAccountRow, run_id: str) -> tuple[int, float]      # (rows, usd) — one governor check per page
    async def _poll_packed_search(self, handles: list[str], query_key: str, run_id: str) -> tuple[int, float]
    async def _poll_mention(self, occupant: OccupantRow, run_id: str) -> tuple[int, float]
    async def _ingest(self, posts: list[XPost], *, source: str, run_id: str, occupant_hint: str | None, match_kind: str) -> int   # normalise + credibility + upsert + mentions
    def _top_n_occupants(self, n: int) -> list[OccupantRow]
    # pauses are 01's wrapper + 16's effective_pause(); the collector only reacts to Decision.allowed inside a run

# app/nlp/match.py
def match_occupants(text: str, registry: SubnetRegistry, restrict_to: set[str] | None = None) -> list[tuple[str, str]]   # [(occupant_key, 'name'|'sn'|'cashtag'|'handle')]
def build_mention_query(occupant: OccupantRow, team_handles: list[str]) -> MentionQuery
def url_canonical(url: str) -> str    # lower host, strip utm_*/ref/fbclid/gclid/mc_*, drop fragment, trailing slash

# app/collectors/rss.py
class FeedPoller:
    async def fetch(self, feed: FeedRow) -> FeedFetch          # FeedFetch(status, entries, etag, last_modified, bozo)
    async def poll_all(self, kinds: set[str], run: RunContext) -> JobOutcome

# app/collectors/youtube.py
async def discover_channel_id(url_or_handle: str, http: Http) -> tuple[str | None, str]
    # (channel_id, method ∈ 'path','api','oembed','externalId','canonical','video_page','none')

# app/features/social_x.py (every producer fn, 02 §C5)
async def compute_x_cadence_30d(db, occupant_key: str, as_of: datetime) -> MetricResult   # likewise for the other §3.3 / §A.4 keys
```

### 8.3 Step order

1. **Config + provider skeleton** (`base.py`, `null.py`, `prices.py`, `make_provider`, settings + validators). Boot with `X_PROVIDER=null` must make no paid call (the paid X jobs report `disabled`) and pass `/health`.
2. **No DDL here**: 03's schema v1 already creates `x_accounts`, `x_account_occupants`, `x_account_snapshots`, `x_search_state`, `social_posts`, `social_post_mentions`, `kol_accounts`, `feed_state`, `news_items`, `video_items` (02 §B7). Write through `Database` (03 §2.3); `social_post_mentions`, `x_account_snapshots`, `news_items`, `video_items` are append-only (`INSERT OR IGNORE`, never `upsert()`).
3. **KOL registry**: `scripts/build_kol_list.py --write` (embedded seed table §3.4) → `--load`; `KolRegistry` in-memory index by `handle_lower`.
4. **Handles + rules**: `normalise_x_handle`, `chunk_handles`, `match_occupants`, `build_mention_query`, `url_canonical`; tests.
5. **Heuristics**: `author_credibility`, `cap_author_share`; tests.
6. **twitterapi_io provider** with `FIELD_MAP` over 01's `Http` (02 §G1): no retry loop, limiter or client of its own — `Http` retries 429/5xx and honours `Retry-After`.
7. **official_v2 provider** (`since_id`, first-poll `max_results=5`, `pagination_token`, `meta`, `includes.users` join for search, the 7-day `since_id` fallback).
8. **XCollector** jobs + governor calls (§6.4); `python -m app.collectors.x --once team|kol|mentions|profiles|ctf [--dry-run]` runs the job once through 01's wrapper (flags, pauses and governor apply); `--dry-run` prints the estimate and makes no call.
9. **ctf cross-check** (`x_ctf.py`), runs regardless of `X_ENABLED`.
10. **Producer fns** (`social_x.py`), registered with spec 12 (02 §C5): `register_metric("x_cadence_30d", pillar="P5", component_of="announcement_cadence_30d", normaliser=None, weight=None, freshness_days=3, fn=compute_x_cadence_30d)`; likewise `x_last_activity_days` (`component_of="silence_days"`), `x_engagement_30d`/`x_followers` (`pillar="P5"`), `x_mentions_7d`, `x_kol_posts_30d`. Nothing is registered for `announcement_cadence_30d` or `silence_days`. Before Phase 8 (`app.features.registry` missing) `register_producers()` returns 0.
11. **Appendix A**: `rss.py` → `news.py` → `youtube.py` → `social_feeds.py` (`register_metric` for the four `rss` keys; the three evidence keys land in 12's `EVIDENCE_FNS`); `seed/feeds.csv`.
12. **Scheduler rows** for 01's `JOB_TABLE` = §2.5's job table verbatim; `PAUSE_GROUPS` entries `x`, `rss` for 16.
13. **Fixtures**: `scripts/record_x_fixtures.py` once a key exists; until then synthetic fixtures marked `"_synthetic": true`.

### 8.4 What is ported from the old bot

| Old path | Lines | Use |
|---|---|---|
| `app/data/fundamentals_collect.py` | 73-111 (`fetch_subnetalpha`), 105 (`twitterx_url`) | spec 04 owns the port; this spec consumes `watch_channels`; the WP-REST pagination pattern (`400` past last page) is reused verbatim for slug/embed-id lookup if `identity_observations` lacks them |
| `app/data/taostats_client.py` | 72-81 (limiter), 93-113 (retry/429 loop) | **not ported**: 01's `Http`/`ProviderLimiter` own limiting and retries (02 §G1) |
| `app/config.py` | 637-662 (enum validators) | `X_PROVIDER`, `X_TEAM_POLL_MODE`, `X_KOL_POLL_MODE` validators (no sentinel path: `X_KILL_SWITCH_PATH` is retired, 02 §B12) |
| `research/subnet_edge/sn13_data_universe.md`, `sn22_desearch.md` | whole | future-option notes only (§11 Q9); no code |

Nothing else from the old bot touches X — it had no social ingestion (`feature-sentiment-ingestion.md` is the *trading-side* contract, spec 15).

---

## 9. Tests

All tests run offline; `httpx` is mocked with `respx` (or `httpx.MockTransport`); no test may hit the network (a session-scoped fixture patches `httpx.AsyncClient` to raise on unmocked hosts).

### 9.1 Unit

| Test file | Fixture | Asserts |
|---|---|---|
| `test_x_handles.py` | table of 25 URL/handle forms incl. `https://x.com/#!/Foo_Bar?s=20`, `twitter.com/intent/follow?screen_name=x`, `x.com/foo/status/1`, `@Foo`, `Foo` (16 chars), `x.com/i/flow/login` | correct handle or `None`; case preserved; `chunk_handles` respects 25/512 with a synthetic 30-handle list of 15-char handles |
| `test_kol.py` | `XUser` variants (age 5 d / 90 d / 3 y; ratios; velocity 150/day), batch with 3 identical texts within 4 min from one author and 2 from different authors; promo text; KOL row credibility 0.3; team flag | multipliers and flags exactly as §2.7; team → account-level 1.0; KOL overrides heuristics; a team post with promo text keeps `bot_filtered=0` (flag `promo` recorded); `cap_author_share` (§2.8 water-fill): `{a: 60, b: 10, c: 10, d: 10, e: 10}` → `{25, 18.75 ×4}`; `{a: 50, b: 40, c: 5, d: 5}` → all 25; `{a: 90, b: 5, c: 5}` → c = 1/3, all 33.33; 2 authors → 50/50; every output share ≤ max(cap, 1/n) + 1e-9; sums preserved to 1e-9; `{}` → `{}` |
| `test_match.py` | registry stub with names `Chutes (64, $SN64)`, `Score (44)`, `subnet103 (103)`, handles | `match_occupants("Score just won…")` → `[]` (ambiguous, no co-term); `"…Score on bittensor…"` → SN44; `"SN64 is cooking"` → 64; `"$SN64"` → 64; `build_mention_query` for 103 has no name term; rendered twitterapi.io and official strings ≤ 512 chars and contain `-filter:retweets` / `-is:retweet` respectively; `url_canonical` strips `utm_*`, keeps path case |
| `test_x_providers.py` | `tests/fixtures/x/twitterapi_io/{user_info.json, last_tweets_p1.json, last_tweets_p2.json (has_next_page=false), advanced_search.json, err_401.json, err_402.json, err_429.json}`; `official_v2/{users_by_username.json, users_tweets_p1.json, users_tweets_empty.json, search_recent.json, err_429_headers.json, not_found.json}` | normalisation to `XPost` (all fields incl. `is_repost`, `in_reply_to`, `quoted_post_id`, `has_media`); since_id filtering across 2 pages, `newest_id = max`; `cost_estimate`/`cost_actual` exact values (`20 tweets → 0.003`; official empty → `0.005`; official first poll `max_results=5`); `X_PRICES_JSON` override applied; error mapping to `XProviderError.kind` (402 → `budget`, 400 on old `since_id` → one retry with `start_time`); every request goes through a spy `Http.request` with provider id `twitterapi_io` / `official_v2` and `ok_statuses=(200, 400, 402)`; providers never sleep or retry themselves |
| `test_x_costs.py` | `FakeProvider(posts_per_call, cost_per_call)` + a fake governor with 16's exact signatures (keyword-only after the USD argument; a positional extra raises `TypeError`) | `check` is called before every paid call and `record(provider, usd, requests=0, units=…, job=…, run_id=…)` after it; `Decision(allowed=False, reason='hard_cap')` → no further calls, `JobOutcome.note` starts `paused:budget`, exactly one `x_budget_paused` (dedupe `provider\|month`); `reason='kill_switch'` → no event; next month's first allowed check → `x_budget_resumed`; `projected_usd > 0.9 × cap` → mentions top-N 20; the collector sends **no** soft alert (16's `spend_80pct`); `JobOutcome.cost_usd` = Σ recorded USD |
| `test_x_collector.py` | FakeProvider returning scripted pages; DB in tmp | `since_id` persisted per account; second run polls with it; shared-handle attribution (§2.6); team post also returned by mention search → single row, engagement refreshed, `observed_at` unchanged; `text_hash` equals `app.nlp.textnorm.text_hash(text)`; provider raising `transient` on one account → `ok=1`, `note='partial:transient'`, other accounts processed; `auth` → the run raises (`ok=0`), call-count spy = 1; `KILL_SWITCH` / `data/control/x.paused` present → zero calls, `/health.collectors.x_team_poll.paused` = `kill_switch` / `emergency_file`; `X_PROVIDER=null` → job `disabled`, zero calls; `x_account_occupants` re-synced after a `watch_channels` row is retired |
| `test_x_ctf.py` | `tests/fixtures/ctf/apex.html` (trimmed real page, 12 `.ctf-item`), `no_feed.html`, a page whose items belong to another handle | 12 rows, ids from `/status/`, `event_at` from snowflake matches `.ctf-tweet-date` day/month; engagement NULL; `source='subnetalpha_ctf'`; `author_id='h:{handle}'` before resolution; later provider upsert replaces `source` and `author_id` and fills engagement without changing `observed_at`; mismatched handle → no rows + `x_ctf_discrepancy(reason='handle_mismatch')` |
| `test_social_x.py` | seeded `social_posts`/`x_accounts`/`watch_channels` scenarios: no handle; gone; declared handle not found twice; polled-with-0-posts; polled but never a post; ctf-only 12 posts in window; ctf page last fetched 4 d ago; posts with zero engagement; bot_filtered posts | §4 matrix row by row: every fn returns a `MetricResult`; `absent` with reasons `no_x_account`/`x_account_gone`; `not_applicable` vs `failed`; `lower_bound=true` for never-posted; stale ctf → `failed`; PIT: a post or mention row with `observed_at > as_of` is invisible; no fn exists for `announcement_cadence_30d`/`silence_days` |
| `test_rss.py` | `fixtures/rss/{substack.xml, medium.xml, wordpress.xml, bozo.xml}`; mocked 200 then 304 | ETag/Last-Modified sent on the second fetch; 304 → no rows, `feed_state.last_status=304`; bozo with 0 entries → failure; `items_seen` increments; dedupe by `url_hash` |
| `test_news.py` | `fixtures/rss/google_news.xml` (redirect links + `<source>`), `cointelegraph.xml` | keyword filter keeps only matching items; decoding off (default) → `url_hash = sha256("t:"+title_norm+"\|p:"+domain)` with Google's " - Publisher" title suffix stripped; decode mocked success/failure when on; `occupant_keys_json` = `[{"occupant_key", "kind"}]`; `news.google.com` requests ≥ `GOOGLE_NEWS_MIN_INTERVAL_SEC` apart (fake clock); `compute_news_mentions_7d` counts a story seen via Google and a publisher feed once |
| `test_youtube.py` | `fixtures/youtube/{channel_page.html (externalId), video_page.html (channelId), oembed.json, channels_forhandle.json, channel_rss.xml, playlist_rss.xml}` | discovery methods (API first when `YOUTUBE_API_KEY` is set); `video_items` rows with `views` from `media:statistics`; playlist items attributed by title rules |

### 9.2 Integration / smoke (need a key; skipped without `TWITTERAPI_IO_KEY` / `X_BEARER_TOKEN`; marked `@pytest.mark.live`)

- `resolve_user("opentensor")` returns an id and followers > 10,000; `user_timeline` one page; `search_recent(MentionQuery(terms=['"Chutes"','SN64']), since_at=now−1d)` returns ≥ 1 post with `since:` honoured (all `event_at ≥ since_at`) — this is the test that de-UNVERIFIES the `since:` operator and page size; the recorder script saves these responses as the fixtures for §9.1.
- Official v2: `users/by/username`, one `users/:id/tweets` page with `since_id`, one search with a 25-handle packed query (≤ 512 chars, HTTP 200).
- Cost ledger after the smoke run: `provider_spend.usd` equals Σ `collector_runs.cost_usd` for the month (±1e-6), and `provider_spend.requests` equals the number of HTTP requests sent (no double count: 09 records `requests=0`).

What must be mocked in unit tests: every HTTP call, the clock (`freezegun` or an injected `now()` for since_at/month rollover), Telegram alerts (spy), the limiter (01's `fake_clock` fixture), file sentinels (tmp path).

---

## 10. Verify checklist

Run on the laptop from `~/taosentinel` with the venv active.

1. `pytest tests/collectors/test_x_*.py tests/nlp tests/features/test_social_x.py tests/collectors/test_rss.py tests/collectors/test_news.py tests/collectors/test_youtube.py -q` → all green, 0 skipped except `live`.
2. `python -m scripts.build_kol_list --write seed/kol_accounts.csv --load && sqlite3 data/taosentinel.db "select count(*), sum(verified) from kol_accounts"` → `37|32`.
3. `X_ENABLED=true X_PROVIDER=null python -m app.collectors.x --once team` → log line `x_team_poll disabled (X inactive)`, no HTTP request; `sqlite3 … "select count(*) from x_accounts"` → 0; `curl -s :8090/health | jq '.collectors.x_team_poll.enabled'` → `false`.
4. `python -m app.collectors.x --once ctf` → `sqlite3 … "select count(distinct occupant_key) from social_posts where source='subnetalpha_ctf'"` → ≈ 60 if the feeds render again; **0 on the 2026-10-18 site** (then the run note shows `pages=63 with_feed=0` and every `job_state … 'page:%'` row has `n_items 0`); a second run adds 0 rows.
5. With `TWITTERAPI_IO_KEY` set and `X_PROVIDER=twitterapi_io`: `python -m app.collectors.x --once team --dry-run` prints `estimate_usd≈0.32 requests≈200` (100 profile resolutions + 100 first pages); `python -m app.collectors.x --once team` → `select count(*) from x_accounts where kind='team' and status='active'` ≈ 100; `select count(*) from social_posts where source='twitterapi_io'` > 500; `select usd, requests from provider_spend where provider='twitterapi_io'` ≈ `0.30–0.35` USD and ≈ 200 requests (the CLI flushes the governor on exit); `select count(*) from x_accounts where since_id is not null` ≈ 100.
6. `python -m app.collectors.x --once kol && python -m app.collectors.x --once mentions` → `select match_kind, count(*) from social_post_mentions group by 1` shows `team`, `query`, `rule` rows; `select count(*) from social_posts where bot_filtered=1` > 0 (mention noise exists).
7. `curl -s :8090/health | jq '.spend.x'` (16's snapshot, 02 §G6) → `cap_usd 25`, `usd` matches step 5, `paused false`.
8. Governor drill (it really pages once): `X_MONTHLY_CAP_USD=0.10 python -m app.collectors.x --once mentions` → the run ends with `note` starting `paused:budget reason=hard_cap`, no search request sent; `select event_type from events where event_type in ('spend_cap_reached','x_budget_paused') order by id desc limit 2` → both; one Telegram page (`spend_cap_reached`), `x_budget_paused` only in the next digest (02 §D4). With the normal cap (25 > the latch's 0.10, so 16 clears the latch) the next run emits `x_budget_resumed`.
9. `mkdir -p data/control && touch data/control/x.paused && python -m app.collectors.x --once team` → no request (`provider_spend.requests` unchanged), `paused:emergency_file`; `rm data/control/x.paused`. Same with `touch KILL_SWITCH` → `paused:kill_switch` (also pauses the LLM; `rm KILL_SWITCH` afterwards).
10. `python -m app.collectors.rss --once && python -m app.collectors.news --once && python -m app.collectors.youtube --once` → `select count(*) from news_items` > 20, `select count(*) from video_items` > 10, `select count(*) from feed_state where last_status=200` > 15; rerun `rss --once` → `select count(*) from feed_state where last_status=304` > 10.
11. Feature check after ≥ 1 day: `python -m app.features --metric x_cadence_30d,x_last_activity_days,x_engagement_30d,x_followers,x_mentions_7d --as-of today` (spec 12 CLI) → non-None for ≥ 80 % of occupants with a team handle; `absent` (`x_account_gone`) for gone accounts and (`no_x_account`) for occupants without a handle; `failed` for unresolved handles. With X inactive, `x_cadence_30d`/`x_last_activity_days` come from ctf rows only for occupants whose page had feed items within 3 d (none on the 2026-10-18 site → `failed`, `reason='ctf_no_feed'`).
12. Scheduler: `journalctl -u taosentinel --since today | grep -E 'x_team_poll|x_kol_poll|x_ctf_crosscheck|rss_feeds_poll'` shows runs at 01:30Z / xx:30Z every 6 h / 07:00Z / every 6 h; `curl :8090/health | jq '.collectors | keys'` includes `x_team_poll, x_kol_poll, x_mentions_poll, x_profile_refresh, x_ctf_crosscheck, rss_feeds_poll, news_google_poll, youtube_poll`.

---

## 11. Open questions / operator decisions

1. **Which X plan (master §13 decision 1)?** Default `null`. twitterapi.io at ≈ $12.5/mo fits the $25 cap with every job on; official v2 needs the "lean" plan (no mentions) at a $75 cap. Operator picks; the governor makes either safe.
2. **twitterapi.io specifics are UNVERIFIED** (exact param/field names, 20-tweet page size, `since:` operator with time suffix, `userId` lookup, balance endpoint). Step 6/§9.2 records fixtures on first live use; `FIELD_MAP` isolates renames.
3. **Official v2 zero-result billing** UNVERIFIED (research §1 open question). Estimator assumes `$0.005` per empty call; if empty calls are free, KOL/team timeline polling under official becomes ~30 % cheaper and the estimate is relaxed.
4. **Engagement refresh vs strict append-only — resolved (02 §J2):** accepted as a `state`-class deviation; 13 flags `x_engagement_30d` as look-ahead.
5. **Table/column additions — resolved:** 03 adopted every table with renames (INTEGER instants, `event_at`, `account_created_at`, `requests`, `platform`, `item_id`, `kol_handle`); §3 uses 03's names.
6. **Shared-handle attribution** (Rayon Labs, Macrocosmos): a generic post from a shared account counts toward every mapped occupant's cadence. Alternative: divide by the number of occupants. Master §7.3 is silent; keep full credit, revisit after 30 d of data.
7. **`silence_days` X leg with zero posts ever — resolved:** after a successful poll the leg is "days since `resolved_at`" with `value_json.lower_bound=true`, matching 08's legs; 12 §2.6 prefers exact legs.
8. **Unverified KOL handles** (`@yuma_ai`, `@taoapp_`, `@markjeffrey`, `TaoPortal`, James Altucher) and the roles of `@JesusMartinez`, `@CrucibleLabs`: operator confirms in `seed/kol_accounts.csv` (checklist D3). Multi-subnet netuids for Macrocosmos/Taoshi mappings to be confirmed against the registry at load.
9. **Future X data paths via subnets (from the old bot's research; not built):** SN13 Data Universe (Macrocosmos "Gravity" jobs / Hugging Face dumps — bulk historical X for back-filling a sentiment classifier; API status was "planned" in mid-2025) and SN22 Desearch (real-time X/Reddit/Google search with AI summary; niche-subnet signal-to-noise poor at n < 10 tweets/week). Both would be new `XProvider` implementations if their pricing beats twitterapi.io or if X shuts the scrape-backed vendors; re-evaluate when Phase 9 starts.
10. **Google News URL decoding** (`news.google.com/rss/articles/…` no longer base64-decodable; community decoders POST to `batchexecute`) UNVERIFIED maintenance; off by default, v1 dedupes by `(title, publisher)`. `when:7d` in the query is UNVERIFIED. Google News items are stale (research: July-2026 median item age 6.6 d), so a PIT `news_mentions_7d` undercounts recent news.
11. **Exact publisher and newsletter feed URLs** (cointelegraph `/rss`, decrypt `/feed`, theblock `/rss.xml`, tao.media `/feed/`, simplytao, ownyourmind, bex.co, Subnet Edge, 0xsammy.com) UNVERIFIED — `seed/feeds.csv` ships them `enabled=1`; a dead one just emits `feed_dead`.
12. **Master §7.3 `x_engagement_30d` formula** uses `likes + 2·reposts + replies`; quotes and views are collected but unused. Suggest adding quotes at weight 2 in `ts-1.1` — recorded, not implemented.
13. **HTTP layer — resolved (02 §G1):** 01's `Http` + `ProviderLimiter` with provider ids `twitterapi_io`, `official_v2`, `subnetalpha`, `rss`, `youtube`.
14. **Cross-spec items for the coordinator:** (a) 16 `PAUSE_GROUPS` needs `x` = §2.5's five X ids and a new `rss` group for the three appendix jobs; (b) 10 §3.4's single-pass author cap vs §2.8's water-fill (10 decides); (c) 03 §3.9's `x_budget_paused` alert column ("yes if pacing/provider_balance") contradicts 02 §D4 (digest); (d) 04 and 09 both describe an X handle normaliser — 09 now delegates to 04's.
15. **subnetalpha tweet feeds gone (2026-10-18, §2.9):** with the default `X_PROVIDER=null` no X data exists at all, so every occupant with a team handle has `failed` X legs (P5 `confidence` lower, no penalty). Choosing a provider (Q1) is now the only way to get X-based P5/P6 inputs. Re-probe a few pages if the plugin ever renders again; the job reports `with_feed` in every run note.
16. **Legacy-only X rows (§2.4)** on re-registered netuids — SN20 `GroundLayerHQ`, SN89 `infinitehash89`, SN113 `TensorUSD` (a previous occupant's account). Same fix as 08 Q27 belongs in 04; until then 09 ignores them. The operator may add a `manual` row for a handle that is in fact current.
17. **YouTube channels of subnets (Appendix A.2):** 04 has no declared channel rows; own channels can only come from video ids that pass the own-channel test. Expect `video_uploads_90d` to be `absent` for most occupants until the operator adds channel rows (`manual_overrides`/`watch_channels` with `platform='youtube'`).

---

## 12. Sources

Research: `E:\TaoData\research\01-web-social-apis.md` (§1 X options/prices/limits/legal; §4 news/YouTube/Medium/Substack; §6 run-rate), `E:\TaoData\research\01-web-ecosystem-2026.md` (§3 rating efforts; §4 KOL list, newsletters, podcasts, Novelty Search playlist), `E:\TaoData\research\01-web-quant-scoring-methods.md` (§3 bot/shill heuristics, engagement weighting, single-account cap; §2 time decay), `E:\TaoData\research\01-web-subnetalpha.md` (§1 `.ctf-item` tweet feeds, `twitter_embed_id`, WP-REST fields, polite rate).
Specs: `E:\TaoData\specs\02-cross-cutting-conventions.md` (B1–B12, C3–C6, D1–D4, E1–E4, F2, G1, G5, G6, I2, J2), `E:\TaoData\specs\00-MASTER-SPEC.md` (§1.3, §4, §5, §6, §7.3, §7.7, §8, §9, §11, §12, §13), `E:\TaoData\specs\01-repo-skeleton-runtime.md` (§2.6–§2.9, §6), `E:\TaoData\specs\03-storage-schema.md` (§3.2, §3.5, §3.8, §3.9, Appendix A), `E:\TaoData\specs\04-subnet-registry-and-identity.md` (normalisers, `watch_channels`), `E:\TaoData\specs\08-community-collector.md` (`textnorm`), `E:\TaoData\specs\10-llm-classification.md` (§3.4), `E:\TaoData\specs\12-feature-store-and-scoring-engine.md` (§2.3, §2.4, §2.6), `E:\TaoData\specs\16-ops-deploy-and-observability.md` (§2.5, §2.10, §2.11), `E:\TaoData\specs\INDEX.md`, `E:\TaoData\specs\18-operator-checklist.md` (C1, C2, D3, E).
Old bot: `E:\SN_Trader\SubnetTrader\app\data\fundamentals_collect.py:52-111`, `E:\SN_Trader\SubnetTrader\app\data\taostats_client.py:72-113`, `E:\SN_Trader\SubnetTrader\app\config.py:440-662`, `E:\SN_Trader\SubnetTrader\research\subnet_edge\sn13_data_universe.md`, `E:\SN_Trader\SubnetTrader\research\subnet_edge\sn22_desearch.md`.

---

## Appendix A — RSS / news / YouTube collector

### A.1 Feed registry (`seed/feeds.csv` → `watch_channels`, `occupant_key` NULL for ecosystem-wide)

Columns: `kind,label,url,occupant_netuid,enabled,filtered,notes`; each row is loaded (`INSERT OR IGNORE`, 03's `ux_watch_channels` handles the NULL occupant) as `watch_channels(platform=kind, handle=url_canonical(url), url, label, filtered, source='seed_feeds', occupant_key NULL unless occupant_netuid is set)`; `enabled=0` rows are written retired. Per-occupant Medium/Substack/YouTube rows come from spec 04 (subnetalpha `video_url`, on-chain `additional`, manual). Ecosystem-wide seed:

| kind | label | url | notes |
|---|---|---|---|
| `google_news` | Bittensor subnet | `https://news.google.com/rss/search?q=%22Bittensor%22+subnet&hl=en-US&gl=US&ceid=US:en` | global query 1 |
| `google_news` | Bittensor | `https://news.google.com/rss/search?q=Bittensor&hl=en-US&gl=US&ceid=US:en` | global query 2. Both verified 2026-10-18: 100 items each, `<source url>` present, titles end in " - {Publisher}"; items span **months** (2025-11 … 2026-10), so the first run back-fills old stories — harmless (`event_at` windows), but 10 never sends items older than `LLM_LOOKBACK_DAYS` to the LLM |
| `google_news` | per-subnet (generated) | `…?q=%22{name}%22+Bittensor&…` for the top-`RSS_GOOGLE_NEWS_TOP_N` by `emission_share`; placeholder names → `%22SN{netuid}%22+Bittensor` | generated daily, not seeded |
| `rss` | Cointelegraph | `https://cointelegraph.com/rss` | keyword-filtered; verified 2026-10-18 (30 items; links carry `?utm_…` → `url_canonical`) |
| `rss` | Decrypt | `https://decrypt.co/feed` | keyword-filtered; verified 2026-10-18 (37 items) |
| `rss` | The Block | `https://www.theblock.co/rss.xml` | keyword-filtered; verified 2026-10-18 (20 items) |
| `substack` | A Bittensor Journey | `https://www.abittensorjourney.com/feed` | monthly digest; not filtered; verified 2026-10-18 (20 items, ~730 KB body) |
| `rss` | tao.media | `https://www.tao.media/feed/` | **dead: 404 on 2026-10-18 → seeded `enabled=0`** (operator re-enables with a working URL) |
| `rss` | SimplyTao | `https://simplytao.ai/blog/rss` | **dead: 500 on 2026-10-18 → `enabled=0`** |
| `substack` | Pine Analytics | `https://pineanalytics.substack.com/feed` | **`filtered=1`**: general crypto research (latest item 2026-10-16 is a Jupiter report), keyword-filtered like a publisher; same Cloudflare 403 as Subnet Edge from the app client |
| `substack` | Subnet Edge | `https://subnetedge.substack.com/feed` | paid; feed carries titles/teasers; 20 items, newest 2026-02-09 — dormant. **`*.substack.com` answers 403 `cf-mitigated: challenge` to the app's Python client** (curl gets 200; the challenge keys on the TLS fingerprint, not headers — verified 2026-10-18) → this feed fails and reaches `feed_dead`; TaoSentinel does not evade bot challenges. Custom-domain Substacks are unaffected |
| `substack` | 0xSammy — The Agentic Future | `https://www.0xsammy.com/feed` | verified 2026-10-18 (20 items, ~1.8 MB body — under `HTTP_MAX_BODY_BYTES` 5 MB) |
| `rss` | Own Your Mind | `https://ownyourmind.ai/feed` | **dead: 404 on 2026-10-18 → `enabled=0`** |
| `rss` | bex.co blog | `https://bex.co/blog/feed` | **dead: 500 on 2026-10-18 → `enabled=0`** |
| `youtube` | Novelty Search playlist | `https://www.youtube.com/feeds/videos.xml?playlist_id=PLN0WJc6io8zqEknIutfyEjiJViazrxrzU` | `YOUTUBE_NOVELTY_PLAYLIST_ID`; items attributed by `match_occupants(title + description)` |

Per-occupant patterns: Medium `https://medium.com/feed/@{user}` or `https://medium.com/feed/{publication}`; Substack `https://{pub}.substack.com/feed` or `https://{customdomain}/feed`; YouTube `https://www.youtube.com/feeds/videos.xml?channel_id={UC…}` (last 15 uploads, no key). Mirror.xyz/Paragraph feeds UNVERIFIED — operator adds via `RSS_EXTRA_FEEDS`.

### A.2 Fetch and parse

`FeedPoller.fetch`: `http.request('rss', 'GET', url, headers={If-None-Match: <etag>, If-Modified-Since: <last_modified>}, timeout=RSS_HTTP_TIMEOUT_SEC, follow_redirects=True, ok_statuses=(200, 304))` (01's UA; record the final URL; a permanent redirect updates `feed_state.effective_url`; `news.google.com` requests are additionally spaced by `GOOGLE_NEWS_MIN_INTERVAL_SEC`). `304` → no entries. `200` → `feedparser.parse(resp.content)`; `bozo=1 and len(entries)=0` → failure; `bozo=1` with entries → warn + proceed. Per entry: `title`, `link`, `id/guid`, `published_parsed`/`updated_parsed` → `event_at` (UTC; missing → `observed_at` with `event_at_source='observed'`), `summary`/`content[0].value` → `summary` (HTML stripped, ≤ 2,000 chars), `author`, `source.href/title` (Google News `<source>`).

Canonical URL: `match.url_canonical(link)`. Google News links (`news.google.com/rss/articles/…`): Google appends " - {Publisher}" to titles — strip it; `publisher_domain` comes from `<source url>`. With `RSS_GOOGLE_NEWS_DECODE_URLS=false` (default) `canonical_url=NULL` and `url_hash = sha256("t:" + title_norm + "|p:" + publisher_domain)` (`title_norm` = `textnorm.text_norm(title)`). When true, at most 60 items per run try (1) a `GET` with `follow_redirects=True` and take the final URL if its host ≠ `news.google.com`; (2) a decoder module (`app/collectors/gnews_decode.py`, port of the community `batchexecute` approach — UNVERIFIED longevity); on failure the title hash above. Other feeds: `url_hash = sha256(canonical_url)`. `text_hash = sha256(title_norm + "\n" + summary_norm)` (03). Because a story can arrive both through Google (title hash) and a publisher feed (URL hash), `compute_news_mentions_7d` counts distinct `(title_norm, publisher_domain)` pairs, not rows.

Publisher feeds (`watch_channels.filtered=1`) keep an item only when `NEWS_KEYWORDS` or any registry name/`SN\d+` matches `title + summary` (case-insensitive; ambiguous names require a co-term as in §2.5). Ecosystem feeds (`substack`, tao.media, etc.) keep every item. Attribution: `occupant_keys_json = [{"occupant_key": k, "kind": "rule"} for k, _ in match_occupants(title + " " + summary)]`; per-subnet Google News query results additionally get `{"occupant_key": <queried>, "kind": "query"}` (03's format). `news_items.source` ∈ {`google_news`, `rss`, `substack`, `medium`} = the feed's platform.

YouTube channel-id discovery (`discover_channel_id`, provider `youtube`): `/channel/UC…` in the path → done (`method='path'`); with `YOUTUBE_API_KEY`, `channels.list?part=id&forHandle=@h` or `videos.list?part=snippet&id=` (1 unit each; `method='api'`); for video URLs, keylessly `https://www.youtube.com/oembed?url=…&format=json` → `author_url` (04 §2 points 09 at this; `method='oembed'`; the `author_url` form — `/channel/UC…` vs `/@handle` — is UNVERIFIED, an `@handle` continues with the page step); else GET the page with a desktop UA and cookie `CONSENT=YES+cb` (EU consent interstitial UNVERIFIED for server IPs) and regex, in order, `"externalId":"(UC[0-9A-Za-z_-]{22})"`, `<link rel="canonical" href="https://www.youtube.com/channel/(UC…)">`, `<meta itemprop="identifier" content="(UC…)">`; for `watch?v=`/`youtu.be` URLs (subnetalpha `video_url`) regex `"channelId":"(UC…)"` on the video page (`method='video_page'`). The resolved id is stored in `watch_channels.meta_json.channel_id` (a JSON merge that keeps any other keys) together with `verified_at` and `meta_json.channel_id_method`. `handle` keeps the declared input (`@handle`, normalised URL or `UC…`), which 03's `ux_watch_channels` key and 04's identity rows rely on, so it is never overwritten. The feed URL is built from `meta_json.channel_id`. Failures are retried weekly.

**Own channel vs a channel a video happens to sit on (pre-build review 2026-10-18).** Live, 04 holds **no** `platform='youtube'` rows — only 145 `youtube_video` rows (subnetalpha `video_url`, 127 active) — and those videos are often not the team's: SN1 Apex's `XsNSghevAAQ` is a *Novelty Search* episode on the taostats channel (oEmbed `author_url` = `https://www.youtube.com/@taostats`, video page `"externalChannelId":"UCxLqly5YxULobLJwcYjMhJQ"`; both keyless lookups verified). Treating every video's channel as the occupant's own channel would make taostats the YouTube channel of each subnet that presented there. Rule: a channel id resolved from a `youtube_video` row is the occupant's **own channel** only when (a) it is not in `YOUTUBE_ECOSYSTEM_CHANNELS` (module constant: taostats `UCxLqly5YxULobLJwcYjMhJQ` and the Opentensor Foundation `UCq-UgxLUK1mYJBxrmOfP7aA`, which owns the Novelty Search playlist; operator-extendable through `meta_json.ecosystem=true` on any row) and (b) no other active occupant's rows resolve to the same channel id. Otherwise `meta_json.channel_role='ecosystem'|'shared'` and the channel is not polled as that occupant's channel (Novelty Search uploads still arrive through the playlist feed and are attributed by rules). `platform='youtube'` rows (a declared channel) are own channels without the test. `video_uploads_90d` is `absent` (`no_youtube_channel`) when no own channel remains. Discovery order for a video id: Data API (key set) → video page `"externalChannelId"`/`"channelId"` (`method='video_page'`) → oEmbed `author_url` (an `@handle` → channel page step).

### A.3 Rows

DDL is 03's (§3.5 `news_items`, `video_items`; §3.2 `feed_state`); all instants INTEGER unix seconds (02 §B1).
**`news_items`** (append-only): `url_hash, canonical_url, url, title, summary (≤ 2,000 chars; NULLed after 90 d by 03), publisher, publisher_domain, feed_url, feed_kind, occupant_keys_json, lang, text_hash, event_at, event_at_source ('feed'|'observed'), observed_at, source, run_id, raw_json_hash`.
**`video_items`** (append-only): `video_id, channel_id, channel_title, occupant_key (the channel's occupant), occupant_keys_json (rule matches for playlist items, same object format), playlist_id, title, description (NULLed after 90 d), views (media:statistics, first observation only), event_at (published), observed_at, source ('channel_rss'|'playlist_rss'), run_id, raw_json_hash`; `url` is generated.
**`feed_state`** (state): `feed_url, kind (= watch_channels.platform), occupant_key, effective_url, etag, last_modified (header verbatim), last_fetched_at, last_status, consecutive_failures, items_seen, last_new_item_at, enabled`.
Dedupe: `INSERT OR IGNORE` on the PK; a re-seen item never updates. 10 labels these items as `news_item:{url_hash}` / `video_item:{video_id}` (03's `post_labels` convention).

### A.4 Metrics (producer fns in `app/features/social_feeds.py`; none scored in ts-1.0)

Same contract as §3.3 (02 §C5): `compute_news_mentions_7d`, `compute_video_uploads_90d`, `compute_video_mentions_30d`, `compute_blog_posts_90d`, each `async def …(db, occupant_key, as_of: datetime) -> MetricResult`, registered with `register_metric` (the first is a 12 registry row, the other three are catalogued evidence keys that land in 12's `EVIDENCE_FNS`); 12 writes their rows with `source='rss'` (02 §B9).

| metric_key | definition | consumer |
|---|---|---|
| `news_mentions_7d` | distinct `(title_norm, publisher_domain)` among `news_items` with the occupant in `occupant_keys_json` (`json_each(...).occupant_key`), `event_at ∈ (as_of−7d, as_of]`, `observed_at ≤ as_of` | evidence (12 row 53, weight 0); 10's `attention_7d` counts entity-linked items itself (10 §3.4); 10 classifies news titles/summaries for `event_impact_14d` |
| `video_uploads_90d` | uploads on the occupant's own channel(s), `event_at` in 90 d | evidence; candidate P5 input for `ts-1.1` |
| `video_mentions_30d` | playlist/other-channel videos whose rules match the occupant, 30 d | evidence; 10 candidate input |
| `blog_posts_90d` | Medium/Substack/blog items on the occupant's own feeds in 90 d | evidence; `ts-1.1` |

### A.5 Absence vs failure — see §4 rows `news_mentions_7d`, `video_uploads_90d`, `video_mentions_30d`, `blog_posts_90d`.

### A.6 Jobs

Ids, triggers, times, timeouts, flags and providers are in §2.5's job table (the one table 01 mirrors, 02 §E2). Behaviour: `rss_feeds_poll` polls platforms `rss|substack|medium`; `news_google_poll` generates the per-subnet query URLs from the top-`RSS_GOOGLE_NEWS_TOP_N` occupants each run, then polls global + generated; `youtube_poll` polls platform `youtube` plus the Novelty Search playlist, and runs channel-id discovery for unresolved rows on Sundays. All are free (`paid=false`): `KILL_SWITCH` does not stop them; `PAUSE_ALL`, operator pauses (group `rss`, proposed) and emergency files do.

## Changelog
- 2026-10-18 pre-build review (Phase 9), against the live DB (schema 4) and ~30 keyless probes:
  - §2.9: the subnetalpha `.ctf-item` tweets are no longer server-rendered (0 of 10 pages with a feed id; 63 embed ids, not 74; the value is the plugin feed id). The fallback now needs a fetch with ≥ 1 matched item within 3 d (`job_state 'page:{occupant_key}'`), so an empty page can never read as silence; the job stays as a detector (Q15).
  - §2.4/§4: `legacy` X rows never count (3 legacy-only occupants live, incl. SN113's previous occupant `TensorUSD`; SN25 UR carries a stale `MacrocosmosAI` legacy row beside its own; Q16); a team+KOL account (`MacrocosmosAI`) is one `x_accounts` row (`kind='team'`, `kol_handle` set) because `handle_lower` is UNIQUE.
  - §8: 09's matching helpers live in `app/nlp/match.py` (`app/nlp/rules.py` is 10's classifier module — `nlp.rules:LlmRulesFallbackJob` in 01's job table; both specs had claimed the file); test file `test_match.py`.
  - Build (same day): `legacy` X rows never count (SN25 UR's stale `MacrocosmosAI` row); Opentensor Foundation added to the ecosystem channels; `*.substack.com` feeds blocked by a Cloudflare TLS challenge for the app client (not evaded); `MacrocosmosAI` team netuids 1/9/13. Build deviations: BUILD-LOG Phase 9.
  - Appendix A: seed feeds probed — Cointelegraph/Decrypt/The Block/A Bittensor Journey/Subnet Edge/0xSammy/Google News/Novelty Search verified; tao.media, SimplyTao, Own Your Mind, bex.co dead (404/500) → seeded `enabled=0`; Pine Analytics is general crypto → `filtered=1`. A.2: own-channel test for channels resolved from `youtube_video` rows (SN1's video is on the taostats channel; Q17); keyless oEmbed returns `@handle`, the video page returns `externalChannelId` (both verified).
- 2026-09-24 final pass: Appendix A now stores the resolved YouTube channel id in `watch_channels.meta_json.channel_id` (with `verified_at`). `handle` is never overwritten, and §3.1, §3.2 and §7 were updated to match.
- 2026-09-24 final pass: §2.8 defers to spec 10's polarity weight `cred × (1 + log1p(engagement))`, with engagement including quotes and the water-filled cap. The stale single-pass-clip note is removed.
- 2026-09-24 final pass: §3.3 states that `attention_7d` = TMC trending views + labelled mention count (spec 10) and that `x_mentions_7d` is evidence only.
- 2026-09-23 reconciliation + review:
  - C3/C5: every producer fn is `compute_<key>(db, occupant_key, as_of) -> MetricResult`, registered with `register_metric`; `absent` with `no_x_account`/`x_account_gone`; no producer for `announcement_cadence_30d`/`silence_days`; appendix fns `compute_news_mentions_7d` etc. with `source='rss'` (B9); `x_mentions_7d`/`news_mentions_7d` are evidence-only (10 owns `attention_7d`, C4).
  - §4 rebuilt: measured zeros are `ok`; `x_last_activity_days` gets a lower bound after a successful poll with no posts (old Q7 settled, 12 §2.6).
  - B1/B7/B11: INTEGER instants and 03's column names; DDL left to 03; `app/nlp/textnorm.py` imported; post ids `x:{id}`; `social_posts` is X-only; mentions rows carry `event_at`/`run_id`.
  - G1: `Http.request` + `ProviderLimiter` with provider ids `twitterapi_io`, `official_v2`, `subnetalpha`, `rss`, `youtube`; own buckets, retries and the `google_news` bucket removed.
  - G5/G6: exact `check` + `record(..., requests=0, units=…, job=…, run_id=…)`; no collector soft alert (16's `spend_80pct` uses `X_SOFT_ALERT_PCT`); `/health` is 01's schema + 16's `spend.x`; `provider_spend.requests`.
  - F2/D1/D4/B2: cap 0 disables X; `x_budget_paused` is digest-only; severities `warning`.
  - B12: `X_KILLED`/`X_KILL_SWITCH_PATH`, `paused:auth`, `paused:provider_null` removed; pause groups `x`/`rss`; emergency file `data/control/x.paused`; auth errors fail the run.
  - E2: one job table (8 jobs, factory/trigger/time/misfire/timeout/flags/provider/pause group/liveness); times moved off the E3 windows (team 01:30Z, mentions 02:00Z, KOL xx:30).
  - Review: ctf cross-check moved monthly → daily with a 3-day freshness gate (monthly data would report false X silence on the default null-provider path); handle-match rule and the `h:` placeholder author id for ctf rows.
  - Review: official first poll `max_results=5` ($2.50 instead of ≈ $50); page bound 3 × 20; 7-day `since_id` fallback for recent search (UNVERIFIED); cost table recomputed; `X_PRICES_JSON` added; twitterapi.io Oct-1 list-call price flagged.
  - Review: team posts are never `bot_filtered`; the `cap_author_share` water-fill is defined precisely (its old test contradicted it); `x_followers` snapshot age follows `X_PROFILE_REFRESH_DAYS`; top-N fallback uses `chain_hourly.emission` (`subnets.emission` does not exist); unresolved handles back off via `job_state`.
  - Review: KOL seed checked against the research (32 verified / 5 UNVERIFIED; Quasar date corrected; unconfirmed team mappings named); Google News URL decoding off by default with title+publisher dedupe; optional `YOUTUBE_API_KEY`/oEmbed discovery; Subnet Edge and 0xSammy feed URLs marked UNVERIFIED.
