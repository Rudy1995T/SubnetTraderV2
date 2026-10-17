# [web] discord-telegram-topology

**Topic:** discord-telegram-topology: Bittensor subnet Discord/Telegram community topology (128 netuids) via keyless invite metadata, on-chain identities, widget.json and t.me previews

## Findings

## Data pulled (all keyless, 2026-09-22 ~14:10-14:25 UTC)
- subnetalpha: `GET https://subnetalpha.ai/wp-json/wp/v2/subnet?per_page=100&page={1,2}&_fields=slug,title,acf` -> 128 records, netuids 1-128 (no 0). `acf.discord_url` present on 112, `acf.telegram_url` on 7, `acf.twitterx_url` on ~110. Only **26 distinct invite codes** among the 112 discord URLs: **87 are the vanity `discord.com/invite/bittensor`**; 25 are unique.
- On-chain identities: taomarketcap per-subnet endpoint 429'd after 4 calls ("Request was throttled", shared IP with sibling agents), so I read the chain directly: `POST https://entrypoint-finney.opentensor.ai` JSON-RPC (HTTP works, no key): `state_getKeysPaged(twox128("SubtensorModule")+twox128("SubnetIdentitiesV3"))` -> 121 keys, then `state_queryStorageAt` -> SCALE-decode 8 compact-prefixed Vec<u8> fields `subnet_name, github_repo, subnet_contact, subnet_url, discord, description, logo_url, additional` (Twox64Concat key suffix = last 2 bytes LE netuid). 3 RPC calls total, <2 s. Script: `C:\Users\rudis\AppData\Local\Temp\claude\e--TaoData\bdf78d80-2adb-4f86-b60c-1068ddf9e98d\scratchpad\chain_identities.py`; output `chain_identities_v3.json`. Netuids with NO identity row: 35, 57, 59, 70, 84, 86, 108.
- On-chain `discord` field shapes (121 rows): 69 empty/junk ("", " ", "None", "deprecated", "pending..."), 23 invite URLs, **18 deep links `discord.com/channels/799672011265015819/<channel_id>`** (SN 23,24,32,38,54,61,67,72,81,83,87,105,107,111,115,117,119,128 — these hand us the subnet's channel id inside the main server with zero auth), 18 bare usernames (SN3 @unarbos, 6 amedeo_ma, 8 tl_arrash, 18 wouter_orpheusai, 25 xcolwell, 45 canti_dev, 48+63 qbittensorlabs, 51 p383_54249, 60 yubo, 66 wejh, 76 ormasheroncovelabs_43871, 77 CreativeBuilds, 91 christensor_49068, 97 @arbos, 101 Tag101, 102 isabella618033, 120 consttt) = owner-contact handle signal.
- Discord invites: `GET https://discord.com/api/v10/invites/{code}?with_counts=true&with_expiration=true`, 1.05 s spacing, UA "TaoSentinel-research/0.1". 44 distinct codes resolved (26 subnetalpha + 18 chain-only). Latency ~0.3 s. Raw JSON: `scratchpad\discord_sa.json`, `discord_chain.json`; merged table `topology.json`.
  - Response headers: **NO `X-RateLimit-*` headers unauthenticated**; `Cache-Control: public, max-age=300`, `cf-cache-status: EXPIRED|HIT`, `x-discord-features: invites`. Counts are edge-cached 5 min, so polling faster than 5 min is pointless.
  - Body fields confirmed: `type`, `code`, `expires_at`, `flags` (2 on vanity), `guild{id,name,features[],verification_level,vanity_url_code,premium_subscription_count,premium_tier,nsfw_level}`, `channel{id,type,name}`, `inviter{...}` (only on user-created invites), `profile{member_count,online_count,visibility,badge}`, `approximate_member_count`, `approximate_presence_count`. `with_expiration` is no longer documented but `expires_at` is always present.
  - Error codes: 404 `{"code":10006,"message":"Unknown Invite"}` (revoked/never existed) and 404 `{"code":50270,"message":"Invite is expired."}` — both 404, neither counts toward the invalid-request ban (docs: only 401/403/429 count).

## Topology result (128 netuids; precedence own_server > main channel-link > main vanity > invalid > handle > none)
Counts: **own_server 25 | main_bittensor_channel 22 | main_bittensor_vanity 64 | invalid 3 | handle_only 1 | none 13**. So 86/128 (67%) live inside the main Bittensor guild 799672011265015819 (50,905 members / 5,571-5,578 online, ratio 0.11, verification_level 4, 63 boosts, tier 3, features NEWS+COMMUNITY+DISCOVERABLE+MEMBER_VERIFICATION_GATE_ENABLED+PREVIEW_ENABLED, created 2021-01-15). Widget disabled (403 code 50004). `/guilds/{id}`, `/guilds/{id}/preview`, `/guilds/{id}/channels` all 401 without a token even though DISCOVERABLE. `https://discord.com/discovery/servers/bittensor` returns 200 but is a JS SPA (no SSR data); `/servers/<slug>` 404.

Own servers (netuid -> guild id, name, members/online, ratio, landing channel(type), notable):
- 52 Dojo -> 1186416652955430932 Tensorplex Labs 20428/358 r=0.018 team-chat(0), widget ON
- 122 CookingTAO -> 692175473198759938 Cooking 10719/1307 r=0.122 DISCOVERABLE, widget ON (guild created 2020, pre-dates subnet)
- 64 Chutes -> 1397250390851977419 Chutes 5885/1245 r=0.212 DISCOVERABLE vanity=chutes (chain only; subnetalpha says vanity bittensor)
- 106 Nodexo -> 1039241590092353596 1986/142 r=0.072 announcements(5); subnetalpha code t7BMee4w is dead (10006), chain code fwjBwxUcdX works
- 101 Tag101 -> 1419378005230485587 1905/9 **r=0.005** '》rules', created 2025-09-21 17:41
- 34 BitMind -> 1233808620039311441 1757/122 r=0.069 (Telegram marked "Deprecated")
- 44 Score -> 1267467479614099600 SIRE 1619/146 r=0.090
- 35 Cartha -> 1415076289626509375 0xMarkets Community 1441/159 r=0.110 announcements(5)
- 4 Targon -> 1169311853495713792 Manifold 1429/162 r=0.113
- 62 Ridges -> 1432764037418057879 1323/262 r=0.198 no COMMUNITY/NEWS
- 17 404-GEN -> 1065924238550237194 "404" 1279/180 r=0.141 broadcasts(5), widget ON (instant_invite VrwtK2kC)
- 22 Desearch -> 1328375514410516501 1091/55 r=0.050
- 72 NATIX -> 1523680729748471919 666/104 r=0.156 (verify gate channel)
- 65 True Performance Network -> 1227933622841905253 615/79 r=0.128 'announcements-not-public'(5); subnetalpha 'taofu' dead
- 85 Vidaio -> 1362092201491828766 606/91 r=0.150
- 75 Hippius -> 1298001698874327131 496/89 r=0.179
- 49 Nepher Robotics -> 1520879565906645263 390/137 r=0.351 (chain points to a DIFFERENT "Nepher Robotics" 1438956431104807024 155/31)
- 113 LongShort -> 1451203260982497406 TensorUSD 251/50 r=0.199 widget ON
- 121 sundae_bar -> 1377297591313633390 225/25 r=0.111
- 80 OpenRoboto -> 1531867586701299764 194/67 r=0.345 **invite expires 2026-10-08T13:53:04Z**
- 55 NIOME -> 1438840748328878100 182/28 r=0.154 (chain invite 7mJkaJZX -> lookalike guild 1419380124024836297 "NIOME" 1922/9 r=0.005, created 2025-09-21 17:50 — 8 min after Tag101's guild; both land on '》rules' with 9 online: template/inflated-member pattern)
- 78 Umi -> 1471703293599416333 172/54 r=0.314 **invite expires 2026-09-22T22:53:28Z** (will be invalid tomorrow)
- 20 Witness -> 1493981293686296578 ChronoSeek 69/15 r=0.217 announcement(5), widget ON
- 90 KubeTEE -> 1456253162242703464 39/10 r=0.256
- 46 Instant -> 1531896984342892554 30/9 r=0.300
Main-server per-subnet channels are named `{netuid}・{name}・{symbol}` (observed: 15・oro・ο id 1466080859164643389, 69・herald・ع 1349118519198875778, 98・neverplayalone・ბ 1509596888628723865, 114・soma・є 1460644425813328004, 118・ditto・ⲁ 1499448472246419608, plus 18 ids from chain deep links; all type 0 = GUILD_TEXT, i.e. NOT followable). Global announce channel: 830075335084474390 '📣・announce' type 5 (followable).
Invalid: SN8 Vanta (2XSw62p9Fj expired; chain has handle tl_arrash), SN36 Epago (4TzhwgdsAf expired), SN100 Cortex (TTFXK3pYD expired). Also dead but rescued by chain: SN65, SN78, SN106. Chain-side dead invites: SN1 Apex bvBDat3Gy and SN9/SN13 (Macrocosmos) adsQPnFRY both 50270 expired = stale on-chain identity signal.
No reachable Discord at all (17): 3, 8, 36, 39(deprecated), 42, 47, 57, 70, 73(Parked), 82, 84, 86(inactive), 94(pending), 100, 103, 116, 125. Of these only 8, 36, 100 have an X account on subnetalpha; 82/103/125/3 have only a website. Name turnover (subnetalpha title != chain name) on 25 netuids (3,12,16,17,20,25,29,32,36,47,54,65,76,78,89,92,100,104,112,113,115,117,119,125,126) — dead invite + name mismatch = netuid re-registered by a new owner; subnetalpha is stale for these.
Source disagreement (subnetalpha guild != chain guild): SN20, 46, 49, 55, 62, 64, 80, 90 — chain is the more specific/current source in 6 of 8.

## Widget endpoint
`GET /guilds/{id}/widget.json` (keyless): 200 for 5 guilds (Cooking presence 1311; Tensorplex 364; 404 188; TensorUSD 48 + channel list; ChronoSeek 16 + instant_invite), returns `{id,name,instant_invite,channels[{id,name,position}],members[<=100 {username,status,avatar_url}],presence_count}`. 403 code 50004 "Widget Disabled" for the other 21 (incl. main). **403 counts toward the 10k/10-min invalid-request limit**, so probe disabled widgets weekly, not hourly. `widget.png` 404.

## Telegram (7 URLs, 6 distinct)
`t.me/s/<handle>` server-side preview exists ONLY for broadcast channels (validated: t.me/s/durov -> 20 `tgme_widget_message_wrap` blocks, newest 2026-09-11). All 7 subnet links redirect back to the profile page (0 message blocks) because they are groups/private/users: SN72 NATIXNetwork group "23 831 members, 619 online"; SN2 inference_labs group "7 393 members, 219 online"; SN51 & SN22 daturavalidator (a validator's group, not the subnet) "175 members, 15 online"; SN52 `+ug7C1J7Apf8wNzc1` private channel "Tensorplex Announcements, 255 subscribers" (og:title only); SN34 `+izbzDva6idMzZTYx` private "BitMind AI (Deprecated)" 472 members/29 online; SN30 wahoopredict_support is a user/bot account ("You can contact ... right away") — no counts. So Telegram yields a member/online time series for 4 groups + 2 private counts, but no post feed anywhere; a post feed would require a Telegram user session (Telethon/MTProto) joined to each group — out of scope for keyless.

## Metrics enabled
Per guild (dedupe main server to one sample/cycle): `member_count`, `presence_count`, `presence_ratio`=presence/members sampled at fixed UTC hours (daily median; healthy own-servers observed 0.07-0.35; <0.02 with >1000 members = inflated flag: Tag101 0.005, NIOME-lookalike 0.005, Tensorplex 0.018), `member_growth_7d/30d` from 00:00Z snapshots, `invite_valid` + `invite_error_code` (10006 vs 50270) + `invite_expires_at` (temporary invite in identity = hygiene flag), `guild_age` from snowflake ((id>>22)+1420070400000 ms), `landing_channel_type` (5 = followable announcements), `features` NEWS/COMMUNITY/DISCOVERABLE, `boosts`, `verification_level`, `widget_enabled` (+ hourly `presence_count` from widget when on), `topology_class`, `main_server_channel_id`, `source_agreement` (subnetalpha vs chain), `name_turnover`, `owner_discord_handle` (chain). Telegram: `tg_members`, `tg_online`, `tg_kind` {group, private, user, channel}.

## Cadence & limits
Discord rules (docs.discord.com/developers/topics/rate-limits): IP-scoped when unauthenticated; "10,000 per 10 minutes" invalid (401/403/429) requests -> temporary Cloudflare IP ban; global 50 req/s applies to bots. Proposed: invites for 25 own-servers + 1 main vanity every 6 h at 00/06/12/18Z (~104 req/day); 22 main-channel codes re-validated weekly (channel rename/delete detection); 3-9 dead codes daily; widget.json hourly for the 5 enabled (120/day), disabled ones weekly; chain identities re-read daily (3 RPC calls) to pick up new invite strings; t.me profile pages daily (6/day). Total ≈ 250 req/day ≈ 0.003 req/s, >5000x under the ban threshold; enforce ≥1 s spacing, honor `Retry-After` on 429, never retry a 404 inside the same cycle.

## ToS position
`GET /invites/{invite.code}` is a documented endpoint (docs.discord.com/developers/resources/invite) that Discord itself serves unauthenticated for link previews, but: Discord Terms of Service (effective 2025-09-29) forbid "scraping our services without our written consent, including by using any robot, spider, crawler, scraper, or other automatic device"; Developer Policy (effective 2024-07-08) §20 "Do not mine or scrape any data, content, or information available on or through Discord services", §15 API Data only "as necessary to provide your Application's stated functionality", §16 no profiling users, §18 no selling/commercializing API Data, §13 no engagement inflation, "API Limits: ...will not attempt to circumvent such limitations". Developer Terms page returned 403 to curl (UNVERIFIED verbatim). Practical posture: low volume, aggregate counts only, private non-commercial; strip and never store `inviter`/widget `members[]` usernames (§16); register a Discord application and send `Authorization: Bot <token>` on the same endpoint so calls are attributed and rate-limit headers appear (expected, UNVERIFIED that counts are identical) — this puts you under Developer Terms you have accepted rather than the anonymous-scraper grey zone. Selfbots (automating the user's account to read main-server channels) are a ToS violation and must not be specified.

## What the user must do manually (main server)
1. Join discord.gg/bittensor; User Settings -> Advanced -> Developer Mode ON; for each `NN・name・symbol` channel right-click -> Copy Channel ID; fill a CSV netuid,channel_id for the 64 main_bittensor_vanity subnets (22 already known from invites/chain). 2. In the main server, open '📣・announce' (830075335084474390) -> Follow -> target = a channel in the user's own aggregator server; the bot (MESSAGE_CONTENT intent, <100 guilds so no verification) reads crossposts there. Per-subnet channels are type 0 and cannot be followed; milestone posts there stay manual/CSV. 3. Join own-servers with type-5 landing channels (SN35, 17, 65, 106, 20) and Follow them likewise; for other own-servers, locate their announcements channel after joining. 4. Create the bot application (Developer Portal), invite it only to the aggregator server, hand over the token.

## Data sources catalog

| Name | URL | What it gives | Access | Reliability |
|---|---|---|---|---|
| Discord Get Invite (keyless) | https://discord.com/api/v10/invites/{code}?with_counts=true | guild id/name/features/verification/boosts/vanity, landing channel id/type/name, approximate_member_count, approximate_presence_count, expires_at, inviter; 404 code 10006 (unknown) / 50270 (expired) | free, no token; IP-scoped; no X-RateLimit headers unauthenticated; Cache-Control max-age=300 (edge cached 5 min); 10,000 invalid (401/403/429) req per 10 min per IP -> temp ban; 404s do not count | Documented endpoint, ~0.3 s latency; counts are approximate and identical for every invite of the same guild; ToS grey zone for automated polling (Terms anti-scraping clause, Dev Policy §15/§20) — keep low volume, aggregate only, prefer Bot-token attribution |
| Discord Guild Widget | https://discord.com/api/v10/guilds/{guild_id}/widget.json | presence_count, instant_invite, channels[] (voice/visible), up to 100 online members with status | free, no token; 403 code 50004 when server owner has widget disabled (counts as invalid request) | Enabled on only 5/26 subnet guilds (Cooking, Tensorplex, 404, TensorUSD, ChronoSeek); main Bittensor guild disabled; poll disabled guilds weekly at most |
| Bittensor chain SubnetIdentitiesV3 (public RPC) | https://entrypoint-finney.opentensor.ai (JSON-RPC over HTTPS; also wss://entrypoint-finney.opentensor.ai:443) | Per-netuid owner-declared subnet_name, github_repo, subnet_contact, subnet_url, discord (invite URL \| discord.com/channels/799672011265015819/<channel_id> \| bare username \| empty), description, logo_url, additional | free, no key; 3 calls for full map (getKeysPaged + queryStorageAt); Opentensor public node, undocumented fair-use limits | 121/128 netuids have rows (missing 35,57,59,70,84,86,108); strings are owner-set and can be stale (SN1/9/13 invites expired) or junk ('None','pending...'); decoded with xxhash twox128 prefix + SCALE compact lengths (script chain_identities.py) |
| subnetalpha.ai WP REST subnet directory | https://subnetalpha.ai/wp-json/wp/v2/subnet?per_page=100&page={1,2}&_fields=slug,title,acf | 128 records: acf.subnet_number, discord_url (112), telegram_url (7), twitterx_url, github_url, website_url, subnet_owner, team_info, roadmap, short_desc | free, no auth, standard WP REST (per_page<=100) | 87/112 discord_url are the generic bittensor vanity invite; 6 codes dead; titles stale for 25 re-registered netuids (e.g. SN100 'Base' vs chain 'Cortex'); manually curated, no netuid 0 |
| taomarketcap public subnets API | https://api.taomarketcap.com/public/v1/subnets/table/ and /public/v1/subnets/{netuid}/ | table: 129 rows netuid 0-128 with price/mcap/emission/is_active/commits_7d etc (no identity); per-subnet: latest_snapshot.subnet_identities_v3 {subnetName, githubRepo, subnetContact, subnetUrl, discord, description, logoUrl, additional} | free, keyless, ~10 req/60 s per IP; 429 {"detail":"Request was throttled."} | Throttled after 4 per-subnet calls in this session (shared IP); use chain RPC for the full identity map instead and TMC only for the table |
| Telegram public profile / channel preview | https://t.me/<handle> and https://t.me/s/<channel> | Groups: 'N members, M online' in div.tgme_page_extra + og:title/description; broadcast channels: server-rendered last ~20 posts (tgme_widget_message_wrap, <time datetime>); private +invite links: title + subscriber count only | free, no auth, HTML scrape; ~1 req/s polite | None of the 7 subnet links is a public channel, so no post feed; 4 groups give member/online series (NATIX 23,831/619; inference_labs 7,393/219; daturavalidator 175/15); SN30 link is a support account (no counts); SN34 group titled 'Deprecated' |
| tao.app beta API (identity history) | https://api.tao.app/api/beta/subnets/identity-changes?netuid=N and /api/beta/subnets/owner-content?netuid=N | Historical identity changes per netuid (block-ordered) and decoded owner 'additional' payload (tagline, links, About) | requires APIKeyHeader even on '(Free)' routes | Not exercised (no key); useful later for identity-change event detection |

## Sources

- https://subnetalpha.ai/wp-json/wp/v2/subnet?per_page=100&page=1&_fields=slug,title,acf
- https://subnetalpha.ai/wp-json/wp/v2/subnet?per_page=100&page=2&_fields=slug,title,acf
- https://api.taomarketcap.com/public/v1/subnets/table/
- https://api.taomarketcap.com/public/v1/subnets/1/
- https://entrypoint-finney.opentensor.ai (JSON-RPC state_getKeysPaged / state_queryStorageAt on SubtensorModule.SubnetIdentitiesV3)
- https://discord.com/api/v10/invites/{code}?with_counts=true&with_expiration=true (44 codes)
- https://discord.com/api/v10/guilds/{id}/widget.json (26 guilds)
- https://discord.com/api/v10/guilds/799672011265015819/preview (401)
- https://discord.com/api/v10/guilds/799672011265015819/channels (401)
- https://discord.com/discovery/servers/bittensor
- https://t.me/s/NATIXNetwork, /s/daturavalidator, /s/wahoopredict_support, /s/inference_labs, https://t.me/+ug7C1J7Apf8wNzc1, https://t.me/+izbzDva6idMzZTYx, control https://t.me/s/durov
- https://docs.discord.com/developers/resources/invite
- https://docs.discord.com/developers/topics/rate-limits
- https://docs.discord.com/developers/resources/channel (Follow Announcement Channel, channel types)
- https://support-dev.discord.com/hc/en-us/articles/8563934450327-Discord-Developer-Policy
- https://discord.com/terms
- https://tao.app openapi (identity-changes / owner-content endpoints require APIKeyHeader)
- Local artifacts: C:\Users\rudis\AppData\Local\Temp\claude\e--TaoData\bdf78d80-2adb-4f86-b60c-1068ddf9e98d\scratchpad\{sa_dir.json,chain_identities_v3.json,chain_identities.py,discord_invites.py,discord_sa.json,discord_chain.json,discord_widgets.json,telegram_probe.json,topology.json}

## Open questions

- Do Bot-token-authenticated calls to GET /invites/{code} return identical approximate counts and proper X-RateLimit-* headers (expected yes, UNVERIFIED)?
- Which of the 64 main_bittensor_vanity subnets actually have a `NN・name・symbol` channel in the main server, and which have none (requires the user's manual Copy-Channel-ID pass)?
- Discord Developer Terms of Service verbatim clauses (page 403'd to curl); does Discord consider ~250 req/day of documented public-endpoint polling 'scraping' under the user Terms?
- Does Cloudflare edge caching (max-age=300) mean presence counts lag by up to 5 min, and do different CF POPs return different cached snapshots (affects sampling design)?
- Own-servers: which have an announcement (type 5) channel not exposed by the invite landing channel — only discoverable after the user joins each server
- Are the Tag101 (1419378005230485587) and lookalike NIOME (1419380124024836297) guilds operated by the same party (created 8 min apart, both 1.9k members / 9 online)? Worth a manual look before penalizing SN55
- Telegram post feeds: is a Telethon user session (MTProto) acceptable to the user for reading group messages, or stay counts-only?
- Netuids 35, 57, 59, 70, 84, 86, 108 have no on-chain identity row; taomarketcap marks 59, 86, 108 inactive — confirm dereg status via chain before scoring
- Will Discord ever expose per-channel data of a DISCOVERABLE guild without membership (guild preview requires a token; not tested with a bot token — bots can call /guilds/{id}/preview for discoverable guilds per docs, UNVERIFIED here)
