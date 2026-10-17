# [web] web-social-apis

**Topic:** Social/dev signal ingestion options for TaoSentinel (X/Twitter, Discord, Telegram, GitHub, web/news/video/LLM classification): costs, rate limits, ToS, as of 2026-09-22

## Findings

## 0. Reusable assets already in E:/SN_Trader/SubnetTrader (verified today)

- `app/data/fundamentals_collect.py:52` `WP_API = "https://subnetalpha.ai/wp-json/wp/v2/subnet"`; `fetch_subnetalpha()` lines 73-111 pages 100/page with `_fields=slug,title,acf`. Live fetch today returns JSON with ACF keys: `subnet_number, subnet_owner, team_info, short_desc, what_exactly_does_it_do, what_exactly_is_the_productbuild, roadmap, infographic, website_url, github_url, twitterx_url, twitter_embed_id, discord_url, telegram_url, video_url, video_description, additional_video_1..8, video_description_1..8`. No `doxxed`/`linkedin`/`team_members` fields exist; doxxed status must come from `team_info` free text plus manual entry. This IS the subnetalpha.ai link directory the user referenced; no HTML scraping needed.
- Lines 114-160 read on-chain `SubnetIdentity` via `wss://archive.chain.opentensor.ai:443` (line 54). Dataclass (`bittensor/core/chain_data/subnet_identity.py`): `subnet_name, github_repo, subnet_contact, subnet_url, logo_url, discord, description, additional` (all str). Authoritative owner-declared links; cross-check vs subnetalpha.
- Lines 161-220 `fetch_github()` with `GITHUB_PAT` -> `pushed_at`, `stargazers_count`, `archived`. Line 163 `_GH_RE` parses owner/repo.
- `app/strategy/quality.py:37-67` `QualityConfig` weights: github_active .30, team .20, has_github .15, website .10, description .10, socials .08, age .07; `low_quality_at=0.35`. Seed for the new 0-100 model.
- `app/data/taostats_client.py:27` `TaostatsClient` (httpx, TTL cache, per-minute limiter; key sent as raw `Authorization` header, line 59). `app/notifications/telegram.py:114` `TelegramBot` (Bot API long-poll, send_message/send_document) reusable for alerts.
- Taostats API tiers (taostats.io/pro): Free $0 = 5 calls/min, 10k calls/month; Standard $49/mo = 60/min, 50k/month; Pro $199/mo (limits UNVERIFIED). Quarterly -10%, annual -15%.
- `taostat/subnets-infos/subnets.json` (GitHub) has only 75 subnets with fields `bittensor_id,name,owner,github,hw_requirements,image_url,description`; stale, no socials. Low value.

## 1. X / Twitter

**Official X API (docs.x.com/x-api/getting-started/pricing).** Tiers are gone. Since 2026-02-06 all new developers get pay-per-usage; legacy Basic ($200/mo) force-migrated after 2026-06-01, legacy Pro ($5,000/mo) force-migrated after 2026-09-01. Free tier discontinued (only "for-good" apps case-by-case). Prices: post read $0.005/resource; user read $0.010/resource; post create $0.015 ($0.200 if it contains a URL); "owned reads" (authenticated user's own timelines/bookmarks/followers/lists) $0.001/resource. Cap 3,000,000 post reads per billing month (some blogs say 2M; official doc says 3M). No minimum, no contract; spend >= $200 earns xAI credits (up to 20% back at $1,000+). Enterprise via form (~$42k/mo per third parties). Full-archive search needs upgrade; default is 7-day recent search.
Rate limits (docs.x.com/x-api/fundamentals/rate-limits): `GET /2/users/:id/tweets` 10,000/15min app, 900/15min user, max_results 100; `GET /2/tweets/search/recent` 450/15min app, 300/15min user, max_results 100, query <= 512 chars; `GET /2/users/by/username/:username` and `/2/users/:id` 300/15min app, 900/15min user. Rate limits are independent of billing; 429 + `x-rate-limit-reset`.
Cost model for 150 accounts: billing is per post RETURNED. Using `since_id` and assuming ~3 new posts/account/day = 450 posts/day = $2.25/day = ~$68/mo; one-time ID resolution 150 x $0.010 = $1.50. UNVERIFIED: whether a zero-result call is $0. Search-recent alternative: `from:a OR from:b ...` packs ~25 handles per 512-char query -> 6 queries per poll. Owned-read hack (UNVERIFIED that reverse_chronological home timeline qualifies at $0.001): create a dedicated X account that follows all 150 targets and poll `GET /2/users/:id/timelines/reverse_chronological` -> ~$14/mo.

**xAI Grok x_search (docs.x.ai/developers/tools/x-search).** From 2026-09-21 12:00 PT billed $5 per 1k posts fetched (parents/quotes count) + $10 per 1k profiles, plus model tokens (grok-4.7 in examples). Params `allowed_x_handles`/`excluded_x_handles` (max 20 each, mutually exclusive), `from_date`/`to_date` ISO8601, image/video understanding flags. Same per-post price as official API, so NOT a bulk-ingest path; useful only for on-demand semantic questions ("what are KOLs saying about SN64 this week?").

**Third-party read APIs (unofficial, scrape-backed).** twitterapi.io: $0.15/1k tweets, $0.18/1k profiles, min $0.00015 (15 credits) per call, list calls $0.0015 from Oct 1; $0.10 free credits; 75+ endpoints incl. `user/last_tweets`, `advanced_search`, webhooks/WebSocket monitoring, MCP server, 1000+ QPS. SocialData.tools: $0.20/1k results, 3 req/min free, failed calls free; Monitoring API user-tweets webhooks $0.0069/hr (1-10), $0.0055/hr (101-300) => 150 monitors ~ $594/mo (too expensive; use polling instead). GetXAPI: $0.001/call (~$0.05/1k tweets), $0.10 free. Apify actors: xquik $0.15/1k, kaitoeasyapi $0.18-0.25/1k, apidojo $0.40/1k (free plan demo only). Bright Data ~$1.50/1k. ScrapingDog $40/mo 200k credits. Estimated cost for daily polling of 150 accounts via twitterapi.io returning ~20 tweets/call: 3,000 tweets/day x $0.15/1k = $0.45/day ~ $14/mo; hourly polling ~24x that in min-call fees (~$16/mo floor) unless cursoring.

**Nitter / RSS bridges.** zedeus/nitter GitHub repo archived 2026-09-11 after X Corp cease-and-desist (2026-08-24); nitter.net offline; project says it "will continue" but public instances are dead. Self-hosting needs real X account tokens. RSS-Bridge `TwitterV2Bridge` requires your own X bearer token (i.e. pays official per-post prices). RSS.app / TwitRSS.me are paid or 2-4h stale. Treat all of these as non-viable.

**Legal.** X v. Bright Data (N.D. Cal., Alsup, 2024-05-10) dismissed X's scraping claims (Copyright Act preemption); Bright Data antitrust counterclaims survived April 2025; settlement in principle June 2025. The Nitter C&D shows X is again pursuing scrapers in 2026. Risk to the user is vendor shutdown (not personal liability) unless the user's own X account is used for scraping (violates X user ToS). Do not scrape with the user's account.

**Recommendation:** Build an `XProvider` interface; primary = twitterapi.io (cheapest reliable, cursor/`since` support, webhooks) ~$5-20/mo; secondary = official pay-per-use with a hard $75/mo spend cap (compliance-safe fallback, needs developer account + credit card); tertiary = xAI x_search for ad-hoc semantic KOL queries. Daily poll cadence for 130 subnet accounts, 2-4x/day for 20 KOLs.

## 2. Discord and Telegram

**What a bot can read.** Bot token (Developer Portal app) receives `MESSAGE_CREATE` gateway events and can call `GET /channels/{id}/messages` (limit 1-100, `before`/`after`/`around`; community-cited bucket ~5 req/5s per channel, UNVERIFIED) in any channel where it has `VIEW_CHANNEL` + `READ_MESSAGE_HISTORY`. `MESSAGE_CONTENT` is a privileged intent: toggle in the portal for <100 servers, verification required at >=100 (`content`, `embeds`, `attachments` are empty without it). Threads: `GET /channels/{id}/threads/archived/public`, `GET /guilds/{id}/threads/active`; forum posts are threads. Announcement channels (type 5) work the same.
**Critical constraint:** a bot can only be added to a server by someone with MANAGE_GUILD there. You cannot add your bot to the official Bittensor Discord (discord.gg/bittensor, 50k+ members) or to subnet project servers you don't admin. Legitimate workaround: Discord "Follow" on announcement channels crossposts published messages into YOUR server via webhook, where your bot reads them (only works for channels the source server marks as Announcement and only for posts the team "publishes"; per-channel availability UNVERIFIED). Everything else in third-party servers is manual/curated (or ask subnet teams to invite the bot).
**Rate limits (docs.discord.com):** REST global 50 req/s per token; per-route buckets via `X-RateLimit-Bucket/Limit/Remaining/Reset-After/Scope`; invalid-request (401/403/429) ban at 10,000 per 10 min per IP. Gateway: 120 events/60s per connection; identify limited by `max_concurrency` per 5s; 1,000 session starts/day default; 2,500 guilds per shard.
**ToS:** Self-bots (automating a user token) are explicitly forbidden (support.discord.com "Automated User Accounts (Self-Bots)") -> warning then permanent ban. Developer Policy: do not mine/scrape Discord; do not use message content obtained via API to train ML/AI models without express permission (LLM inference for classification is not training but is a gray area; store message IDs + classification, not full text, to be safe; UNVERIFIED legal reading).
**IDs:** User Settings > Advanced > Developer Mode, right-click > Copy ID; or `GET /users/@me/guilds` then `GET /guilds/{id}/channels`.
**Library:** discord.py 2.x (`discord.Intents.default(); intents.message_content = True`; `channel.history(after=dt, limit=None)`), MIT, actively maintained (exact latest version UNVERIFIED).
**Landscape:** one big official Bittensor Discord with per-subnet channels (e.g. Tatsu had "tatsu-identity - 192" under [TESTNET], plus dual channels after mainnet) AND many subnets run their own servers (`discord_url` in subnetalpha ACF / on-chain `SubnetIdentity.discord`). Expect a mix; ingest is realistically: (a) announcement-channel follows into your own server, (b) manual milestone entry for the rest.

**Telegram.** Bot API: bot must be an admin of a channel to receive `channel_post` updates (`allowed_updates=["channel_post"]`), cannot read history, cannot read channels it is not in -> useless for third-party channels. MTProto user session (api_id/api_hash from my.telegram.org + phone number): reads any public channel history (`GetHistory`), needs a persistent session file (a credential), hits `FloodWaitError` when aggressive; Telegram ToS prohibits abusive automation and its 2026 ban system is behavioral; use a dedicated SIM/account, low rate, read-only. Telethon moved to Codeberg (`codeberg.org/Lonami/Telethon`; GitHub archived 2026-02-21; last commit 2026-09-14, still on PyPI). Pyrogram is unmaintained (community fork Kurigram, UNVERIFIED). Zero-auth option: `https://t.me/s/<channel>` server-rendered HTML (~20 posts/page, `?before=<id>` paging, ~500-1,000 messages reachable; no comments/reactions; only if channel preview enabled). Recommend t.me/s/ first, Telethon fallback.

## 3. GitHub

**Limits (docs.github.com).** REST: 60/hr unauthenticated per IP; 5,000/hr per PAT; GitHub App installation 5,000-12,500/hr. Secondary: <=100 concurrent, 900 points/min/endpoint (GET=1pt), 90s CPU per 60s. Headers `x-ratelimit-limit/remaining/used/reset/resource`. GraphQL: 5,000 points/hr, <=500,000 nodes/query, `first/last` 1-100, query `rateLimit{cost remaining}`. Search API ~30 req/min authenticated (UNVERIFIED). 130 subnets x ~10 calls/day = ~1,300/day, well inside one PAT; use conditional requests (`ETag`/`If-None-Match` returns 304 and costs 0).
**Endpoints/fields:** `GET /repos/{o}/{r}` -> `stargazers_count, forks_count, open_issues_count (issues+PRs), subscribers_count, pushed_at, updated_at, archived, disabled, fork, parent, source, default_branch, license, size, topics`. `/stats/commit_activity` -> 52 x `{week, total, days[7]}` (202 on cold cache; retry after a few seconds; cache keyed by default-branch SHA). `/stats/contributors` -> per-author `{total, weeks[{w,a,d,c}]}` (top 100; a/d zero for >10k-commit repos). `/stats/participation` -> `{all[52], owner[52]}`. `/stats/code_frequency` 422 if >=10k commits. `/commits?since&until&per_page=100` (author, committer, `verification.verified`), `/commits/{sha}` for `stats.total`. `/releases` (`tag_name, published_at, prerelease`), `/tags`. `/pulls?state=all&sort=updated`, `/issues?state=all&since=` (filter `pull_request` key). CI: `/commits/{ref}/check-runs`, `/actions/runs?branch=<default>&per_page=5` (`status, conclusion`). Dependabot: `/dependabot/alerts` requires repo write; instead check `/contents/.github/dependabot.yml` or `renovate.json`. `/community/profile` -> `health_percentage`, presence of README/LICENSE/CONTRIBUTING/SECURITY. Org enumeration: `/orgs/{org}/repos?type=public&sort=pushed&per_page=100`, `/users/{u}/repos`, `/orgs/{org}/members` (public members = team-size proxy).
**Fork/vendored/spam detection:** `fork:true` + `parent`/`source` identifies declared forks; for template-derived subnets compare against `opentensor/bittensor-subnet-template` via `/repos/{o}/{r}/compare/{template_sha}...{head}` (`ahead_by`, `behind_by`, `total_commits`) and `/stats/contributors` (if >80% of lines come from one huge initial commit -> vendored). Commit-spam heuristics: commits with `stats.total < 5`, repeated identical messages, bursts (>10 commits within 60s), README/whitespace-only churn, authors ending in `[bot]` (dependabot, github-actions, renovate), merge-only activity, unverified signatures from unknown authors, `pushed_at` newer than latest commit date (force-push). Score humans-only: unique non-bot authors/90d, merged-PR count with >=1 review (`/pulls/{n}/reviews`). Nesbitt (2026-05) warns AI agents now mass-generate low-effort issues/PRs, so raw counts overstate health.
**Borrowable frameworks:** OpenSSF Scorecard (api.securityscorecards.dev/projects/github.com/{o}/{r}; weekly over ~1M repos; small subnet repos may be absent -> run `scorecard` CLI with PAT). Checks 0-10: Maintained (>=1 commit/week over 90d = 10; <90d-old repo unevaluated), Code-Review, CI-Tests (~30 recent commits), Dependency-Update-Tool (Dependabot/Renovate), Security-Policy, License, Binary-Artifacts, Branch-Protection, Signed-Releases, Vulnerabilities (OSV), Contributors (multi-org), Packaging, SAST, Pinned-Dependencies, Token-Permissions, Dangerous-Workflow, Webhooks, SBOM, Fuzzing, CII-Best-Practices. CHAOSS Starter Project Health: Time to First Response, Change Request Closure Ratio, Contributor Absence Factor (min contributors covering 50% of commits; old name Bus Factor), Release Frequency. ecosyste.ms repos API gives free repo metadata (rate limit UNVERIFIED).

## 4. Generic sources

**Website liveness/change:** per site daily: GET with 10s timeout -> status, latency, redirect chain, final host; TLS via `ssl.get_server_certificate` + `cryptography` `not_valid_after` (days-to-expiry); content fingerprint = sha256 of extracted main text (trafilatura/readability, strip scripts/nonces/dates) + `difflib` ratio for change magnitude. changedetection.io (self-hosted Docker, Playwright sidecar for JS sites, XPath/CSS/jq selectors, Apprise notifications to Discord/Telegram, ~32k stars; SaaS $8.99/mo) is viable but a ~200-line custom poller integrates better with the DB. Wayback: Availability API `https://archive.org/wayback/available?url=<u>&timestamp=<YYYYMMDD>` (free, no key); CDX `https://web.archive.org/cdx/search/cdx?url=<u>&output=json&collapse=digest&from=2023` (first capture = site age; ~1 req/s community limit, 429/503 on abuse); Save Page Now 2 `POST https://web.archive.org/save/<u>` with IA S3 keys (free account) and `if_not_archived_within=1d` to snapshot each subnet site weekly.
**LinkedIn:** no automated access. User Agreement bans crawlers/manual scraping; hiQ v. LinkedIn ended (Dec 2022) with permanent injunction + $500k against hiQ on contract grounds; CFAA does not criminalize public scraping but ToS breach is actionable. Official APIs do not expose arbitrary profile lookup. Design: `team_members(subnet_id, name, role, linkedin_url, github_login, x_handle, verified_by, verified_at, evidence_note)` manual table + UI; supplementary public signals: GitHub profile `company`/`bio`, X bio, conference talks, company registries (OpenCorporates free tier UNVERIFIED).
**News:** Bing News/Search APIs retired 2025-08-11 (HTTP 410). Google Custom Search JSON API closed to new customers, shuts down 2027-01-01. Google News RSS is free/no key: `https://news.google.com/rss/search?q=%22Bittensor%22+%22subnet%22&hl=en-US&gl=US&ceid=US:en` (stale: July-2026 sample median item age 6.6 days). Paid REST alternatives: GNews, NewsData.io, APITube, Perigon (free tiers UNVERIFIED). Add direct RSS from cointelegraph/decrypt/theblock and CryptoPanic (free tier UNVERIFIED).
**YouTube:** RSS `https://www.youtube.com/feeds/videos.xml?channel_id=UC...` (last 15 uploads, no key, updates in minutes; channel_id from page source `externalId`). Data API v3: 10,000 units/day free; `search.list`=100 units (avoid), `channels.list`=1 (`forHandle=` resolves @handles; `contentDetails.relatedPlaylists.uploads` = `UU`+id suffix), `playlistItems.list`=1/page, `videos.list`=1 (`statistics.viewCount/likeCount/commentCount`, `liveStreamingDetails`), `activities.list`=1 (UNVERIFIED). 130 channels daily ~ 300 units. Transcripts via `youtube-transcript-api` (unofficial) for AMA summarization.
**Medium/Substack:** `https://medium.com/feed/@user`, `https://medium.com/feed/<publication>`; `https://<pub>.substack.com/feed` or `<customdomain>/feed`. Mirror.xyz/Paragraph also expose feeds (URL patterns UNVERIFIED). Poll every 6h with `feedparser` + ETag.

## 5. LLM classification cost (Anthropic first-party prices, skill-cached 2026-06-24)

Model IDs/prices per MTok in/out: `claude-haiku-4-5` $1/$5 (200K ctx; uses `thinking:{type:"enabled",budget_tokens}`; min cacheable prefix 4,096 tokens); `claude-sonnet-5` $2/$10 (1M ctx, adaptive thinking, 512-token cache min); `claude-sonnet-4-6` $3/$15; `claude-opus-5` $5/$25. Batch API = 50% off; cache write 1.25x (5-min TTL) / 2x (1h), cache read 0.1x. Use `output_config.format` structured outputs (no prefill on 4.6+ models).
Workload: 2,000 posts/day, ~150 tokens each, ~600-token rubric, ~80 output tokens (JSON: sentiment, category, milestone flag, entities, confidence). Batch 20 posts per request -> 100 requests/day: input 100 x 3,600 = 360k, output 100 x 1,600 = 160k.
- Haiku 4.5: $0.36 + $0.80 = $1.16/day = ~$35/mo; via Batch API ~$17/mo. (Rubric too short to cache on Haiku unless padded to 4,096 tokens.)
- Sonnet 5: $0.72 + $1.60 = $2.32/day = ~$70/mo; Batch ~$35/mo; rubric cacheable (saves ~$0.1/day).
- One-call-per-post (no batching) roughly doubles input cost: Haiku ~$69/mo, Sonnet ~$135/mo.
- Weekly Opus 5 analyst pass per subnet (130 x ~8k in / 1k out) ~ $8/week.
Recommendation: Haiku 4.5 via Batch API for bulk (~$17-35/mo), escalate the ~5% milestone/anomaly hits to Sonnet 5, weekly Opus 5 narrative. Total LLM budget ~$40-80/mo.

## 6. Total monthly run-rate estimate (excluding chain RPC)
twitterapi.io $5-20; official X fallback capped $75 (optional); Discord/Telegram $0 (own bot + t.me/s); GitHub $0 (PAT); Taostats Free $0 (Standard $49 if >10k calls/mo); YouTube/RSS/Wayback $0; LLM $40-80. Baseline ~$50-100/mo; ~$175/mo with the official X fallback and Taostats Standard.

## Data sources catalog

| Name | URL | What it gives | Access | Reliability |
|---|---|---|---|---|
| X API v2 (official, pay-per-use) | https://docs.x.com/x-api/getting-started/pricing | User timelines (GET /2/users/:id/tweets), recent search (7-day), user lookup, metrics; owned-read discount on own timelines | Pay-per-use only since 2026-02-06: $0.005/post read, $0.010/user read, $0.001 owned reads, $0.015 post create; cap 3M post reads/mo; no free tier; rate limits 10k/15min timeline (app), 450/15min search (app), 300/15min user lookup; needs developer account + card | Most stable and ToS-clean; ~$70/mo for 150 accounts at 3 posts/day; enterprise ~$42k/mo for full archive |
| twitterapi.io | https://twitterapi.io/ | user/last_tweets, advanced_search, user info, lists, webhooks/WebSocket monitors, MCP server; 75+ endpoints | $0.15/1k tweets, $0.18/1k profiles, min $0.00015/call, list calls $0.0015 (from Oct 1); $0.10 free credit; no card to start; 1000+ QPS | Unofficial scrape-backed; cheapest per tweet; vendor-shutdown risk given X's 2026 C&D against Nitter; keep behind a provider interface |
| SocialData.tools | https://docs.socialdata.tools/getting-started/pricing/ | Tweets, profiles, search; Monitoring API (user-tweets webhooks) | $0.20/1k results; 3 req/min free; monitors $0.0069-0.0035/hr each (150 monitors ~ $594/mo) | Unofficial; polling is cheap, monitors are not |
| GetXAPI | https://www.getxapi.com/twitter-api-pricing | user/tweets, tweet/advanced_search, user/info | $0.001/call (~$0.05/1k tweets), $0.10 free credit | Unofficial; rate limits unpublished; UNVERIFIED reliability |
| Apify X actors (xquik, kaitoeasyapi, apidojo) | https://apify.com/xquik/x-tweet-scraper | Batch tweet scraping by handle/search/URL | $0.15-0.40/1k tweets pay-per-result; Apify free plan is demo-only (5 runs/mo) | Unofficial; batch-oriented, min 50 tweets/query on some actors |
| xAI Grok x_search tool | https://docs.x.ai/developers/tools/x-search | Keyword/semantic/user search + thread fetch on X inside a Grok completion; allowed_x_handles max 20; date filters | From 2026-09-21: $5/1k posts fetched + $10/1k profiles + model tokens; needs xAI API key | Official-ish; same per-post price as X API so only for ad-hoc semantic queries, not bulk ingest |
| Nitter / RSS-Bridge / TwitRSS | https://github.com/zedeus/nitter | RSS of X timelines | Nitter repo archived 2026-09-11 after X C&D; nitter.net offline; RSS-Bridge TwitterV2 needs your own paid X bearer token | Non-viable in 2026 |
| Discord Bot API + Gateway (discord.py 2.x) | https://docs.discord.com/developers/topics/rate-limits | Message history and live messages in channels the bot can VIEW + READ_MESSAGE_HISTORY; threads; announcement-channel follows crossposted into your own server | Free; bot token; MESSAGE_CONTENT privileged intent (portal toggle <100 servers); REST 50 req/s global, ~5 req/5s per channel messages, 100 msgs/page; gateway 120 events/60s, 1000 session starts/day | Bot can only join servers where an admin invites it; cannot be added to Bittensor official or third-party subnet servers; self-bots banned; do not train models on content |
| Telegram MTProto user session (Telethon, Codeberg) | https://codeberg.org/Lonami/Telethon | Full history and live posts of any public channel | Free; api_id/api_hash from my.telegram.org + dedicated phone number; FloodWait throttling | GitHub repo archived 2026-02-21, maintained on Codeberg (commit 2026-09-14); ToS gray zone, behavioral ban system; use dedicated account, read-only, low rate |
| Telegram t.me/s/<channel> HTML preview | https://t.me/s/telegram | Last ~500-1000 posts of public channels with views/media; ?before= paging | Free, no auth | No comments/reactions; only channels with preview enabled; HTML may change |
| Telegram Bot API | https://core.telegram.org/bots/api | channel_post updates only for channels where bot is admin; sending alerts | Free bot token | Useless for reading third-party channels; reuse old bot's app/notifications/telegram.py for alerts |
| GitHub REST + GraphQL API | https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api | Repo metadata, /stats/commit_activity, /stats/contributors, /stats/participation, commits, releases, PRs/issues, check-runs, actions runs, community/profile, org repos/members | Free; 60/hr unauth, 5,000/hr with PAT (classic or fine-grained, public_repo read); GraphQL 5,000 pts/hr; 100 concurrent; ETag 304s free | Stats endpoints return 202 on cold cache (retry); code_frequency 422 for >=10k-commit repos |
| OpenSSF Scorecard API / CLI | https://api.securityscorecards.dev/projects/github.com/ossf/scorecard | 20 checks scored 0-10 (Maintained, Code-Review, CI-Tests, Dependency-Update-Tool, Security-Policy, License, Vulnerabilities, Branch-Protection, Signed-Releases...) | Free public API (weekly, ~1M repos); CLI needs GITHUB_AUTH_TOKEN | Small subnet repos may be absent from public dataset; run CLI locally |
| CHAOSS metrics (Starter Project Health) | https://chaoss.community/kb/metrics-model-starter-project-health/ | Definitions: Time to First Response, Change Request Closure Ratio, Contributor Absence Factor (bus factor), Release Frequency | Free spec; compute from GitHub API | 2026 caveat: AI-agent issues/PRs inflate counts; weight human authors |
| ecosyste.ms repos API | https://repos.ecosyste.ms/ | Cross-host repo metadata, maintenance status, dependency counts | Free; rate limits UNVERIFIED | UNVERIFIED coverage of subnet repos |
| subnetalpha.ai WordPress REST | https://subnetalpha.ai/wp-json/wp/v2/subnet | Per-subnet editorial: name, owner, team_info, descriptions, roadmap, website_url, github_url, twitterx_url, discord_url, telegram_url, video_url(s) | Free, no key; 100/page; verified live 2026-09-22 | Already wrapped in old bot fundamentals_collect.py:73; unofficial endpoint could be closed any time; cache aggressively |
| On-chain SubnetIdentity (subtensor) | https://raw.githubusercontent.com/opentensor/bittensor/master/bittensor/core/chain_data/subnet_identity.py | Owner-declared subnet_name, github_repo, subnet_contact, subnet_url, logo_url, discord, description, additional | Free via public archive node wss://archive.chain.opentensor.ai:443 or own RPC | Authoritative but often blank/outdated; old bot reads it at fundamentals_collect.py:114-160 |
| Taostats API | https://docs.taostats.io/docs/the-taostats-api | Subnet pages, pools/prices, metagraph, validators, dTAO history | Free: 5/min, 10k/mo; Standard $49/mo: 60/min, 50k/mo; Pro $199/mo (limits UNVERIFIED) | Old bot TaostatsClient reusable (raw key in Authorization header) |
| changedetection.io (self-hosted) | https://github.com/dgtlmoon/changedetection.io | Website change detection with selectors, JS rendering (Playwright sidecar), Apprise notifications | Free Docker; SaaS $8.99/mo | Alternative to a custom poller; heavier to integrate with scoring DB |
| Internet Archive Wayback (Availability, CDX, SPN2) | https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server | First-capture date (site age), snapshot history, on-demand archiving | Free; Availability/CDX no key (~1 req/s community limit); SPN2 needs free IA S3 keys | 429/503 under load; cache responses (immutable) |
| Google News RSS | https://news.google.com/rss/search?q=Bittensor&hl=en-US&gl=US&ceid=US:en | Keyword news feed | Free, no key | Stale (median 6.6 days in July 2026 sample); Bing News API retired 2025-08-11; Google CSE JSON API closed to new customers, ends 2027-01-01 |
| YouTube channel RSS + Data API v3 | https://www.youtube.com/feeds/videos.xml?channel_id=UCxxxx | RSS: last 15 uploads no key; API: playlistItems.list (1 unit), videos.list stats (1 unit), channels.list forHandle (1 unit) | Free; API 10,000 units/day; avoid search.list (100 units) | Transcripts via unofficial youtube-transcript-api |
| Medium / Substack RSS | https://www.wprssaggregator.com/substack-rss-feed/ | Latest posts per author/publication | Free: medium.com/feed/@user, medium.com/feed/<pub>, <pub>.substack.com/feed | Stable |
| LinkedIn | https://www.linkedin.com/legal/user-agreement | Team verification (manual only) | No scraping (User Agreement; hiQ injunction); no public profile API | Design a manual team_members table with evidence + verified_at |
| Anthropic Claude API (Haiku 4.5 / Sonnet 5 / Opus 5) | https://docs.anthropic.com/en/docs/about-claude/pricing | Post classification (sentiment, milestone, category), weekly narrative | claude-haiku-4-5 $1/$5 per MTok; claude-sonnet-5 $2/$10; claude-opus-5 $5/$25; Batch API -50%; cache read 0.1x (Haiku min cacheable prefix 4096 tokens) | 2,000 posts/day batched 20/request: Haiku ~$35/mo ($17 via Batch), Sonnet ~$70/mo ($35 via Batch) |

## Sources

- https://docs.x.com/x-api/getting-started/pricing
- https://docs.x.com/x-api/fundamentals/rate-limits
- https://www.medianama.com/2026/02/223-x-developer-api-pricing-pay-per-use-model/
- https://www.socialcrawl.dev/blog/x-twitter-api-2026
- https://postproxy.dev/blog/x-api-pricing-2026/
- https://www.blotato.com/blog/twitter-api-pricing
- https://twitterapi.io/
- https://www.socialcrawl.dev/blog/best-twitter-x-data-apis-2026
- https://socialdata.tools/
- https://docs.socialdata.tools/monitoring/pricing/
- https://docs.socialdata.tools/getting-started/pricing/
- https://www.getxapi.com/twitter-api-pricing
- https://apify.com/xquik/x-tweet-scraper
- https://apify.com/kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest/api
- https://apify.com/apidojo/tweet-scraper
- https://docs.x.ai/developers/tools/x-search
- https://github.com/zedeus/nitter
- https://status.d420.de/
- https://rss-bridge.github.io/rss-bridge/Bridge_Specific/TwitterV2.html
- https://blog.ericgoldman.org/archives/2024/05/x-corp-v-bright-data-is-the-decision-weve-been-waiting-for-guest-blog-post.htm
- https://www.mlex.com/mlex/articles/2357003/x-corp-nears-settlement-with-bright-data-in-us-case-over-data-scraping
- https://docs.discord.com/developers/topics/rate-limits
- https://docs.discord.com/developers/events/gateway
- https://discord.com/developers/docs/resources/message
- https://support.discord.com/hc/en-us/articles/115002192352-Automated-User-Accounts-Self-Bots
- https://support-dev.discord.com/hc/en-us/articles/8563934450327-Discord-Developer-Policy
- https://support-dev.discord.com/hc/en-us/articles/6207308062871-What-are-Privileged-Intents
- https://discord.com/invite/bittensor
- https://x.com/TatsuEcosystem/status/1851951549091594499
- https://dev.to/starnikov/the-telegram-bot-api-cannot-read-a-channel-your-bot-is-not-in-4jme
- https://dev.to/starnikovoleg/telethon-is-alive-on-codeberg-its-github-repo-has-been-archived-since-february-4lnj
- https://codeberg.org/Lonami/Telethon
- https://core.telegram.org/bots/api
- https://dev.to/odeeb/how-to-scrape-a-telegram-channel-without-login-no-api-key-no-phone-number-54dp
- https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api
- https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api
- https://docs.github.com/en/rest/metrics/statistics
- https://github.com/ossf/scorecard/blob/main/docs/checks.md
- https://scorecard.dev/
- https://chaoss.community/kb/metrics-model-starter-project-health/
- https://www.chaoss.community/kb/metric-contributor-absence-factor/
- https://nesbitt.io/2026/05/27/chaoss-metrics-in-2026.html
- https://www.chainguard.dev/unchained/what-the-fork-imposter-commits-in-github-actions-and-ci-cd
- https://github.com/dgtlmoon/changedetection.io
- https://www.bigiron.cc/guides/wayback-machine-apis-cdx-save-page-now-and-availability
- https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server
- https://www.fbm.com/publications/what-recent-rulings-in-hiq-v-linkedin-and-other-cases-say-about-the-legality-of-data-scraping/
- https://learn.microsoft.com/en-us/lifecycle/announcements/bing-search-api-retirement
- https://developers.google.com/custom-search/v1/overview
- https://keirolabs.cloud/blog/google-search-apis-2026
- https://cloro.dev/blog/google-news-rss/
- https://www.wprssaggregator.com/youtube-rss-feed/
- https://www.socialcrawl.dev/blog/youtube-data-api-2026
- https://www.wprssaggregator.com/substack-rss-feed/
- https://finder.wprssaggregator.com/rss-feeds/platform/medium
- https://raw.githubusercontent.com/opentensor/bittensor/master/bittensor/core/chain_data/subnet_identity.py
- https://github.com/taostat/subnets-infos/blob/main/subnets.json
- https://taostats.io/pro/api-keys
- https://docs.taostats.io/docs/the-taostats-api
- https://subnetalpha.ai/wp-json/wp/v2/subnet?per_page=3&_fields=slug,title,acf
- E:/SN_Trader/SubnetTrader/app/data/fundamentals_collect.py
- E:/SN_Trader/SubnetTrader/app/strategy/quality.py
- E:/SN_Trader/SubnetTrader/app/data/taostats_client.py
- E:/SN_Trader/SubnetTrader/app/notifications/telegram.py
- claude-api skill (bundled, model table cached 2026-06-24) and shared/prompt-caching.md

## Open questions

- Does an official X API timeline/search call that returns zero posts cost $0 (per-resource billing) and does GET /2/users/:id/timelines/reverse_chronological count as an 'owned read' at $0.001/post? (UNVERIFIED; would make a dedicated follower account ~5x cheaper)
- How many of the ~130 subnet channels in the official Bittensor Discord are Announcement-type channels that can be Followed into the user's own server, versus plain text channels (requires joining the server and inspecting; per-channel availability UNVERIFIED)
- Exact per-channel rate-limit bucket for GET /channels/{id}/messages (community cites 5 req/5s; not in official docs)
- Taostats Pro ($199/mo) limits and whether a subnet-identity/metadata endpoint exists in the API (docs returned 403 to fetch)
- Whether Discord's Developer Policy prohibition on using message content to 'train ML/AI models' is read by Discord to cover LLM inference-only classification (legal gray area)
- Telegram's current stance on read-only MTProto user sessions polling public channels (2026 behavioral ban system); which subnet Telegram channels have t.me/s/ preview disabled
- Is the subnetalpha.ai WP REST endpoint sanctioned by the site owner (unofficial; could be closed); does subnetalpha list all current subnets (site had ~130 posts previously)
- OpenSSF Scorecard public API coverage of small subnet repos and API-call cost per repo when running the CLI locally (a few hundred calls/repo UNVERIFIED)
- GitHub Search API rate limit (30/min auth UNVERIFIED) and ecosyste.ms rate limits
- Current discord.py version, Kurigram (Pyrogram fork) maintenance status, youtube-transcript-api breakage frequency
- Free-tier limits of GNews / NewsData.io / CryptoPanic as Google-News-RSS supplements
- Whether Haiku 4.5's 4,096-token minimum cacheable prefix justifies padding the rubric or simply batching 20+ posts per request (cost model above assumes batching, no caching)
