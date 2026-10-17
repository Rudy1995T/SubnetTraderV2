# 07 — Web presence collector

**Status:** proposed (reconciled to 02 and reviewed 2026-09-23) · **Phase:** 5 · **Binding tie-breaker:** `02-cross-cutting-conventions.md` (ids cited inline) › master › owner specs; this spec owns its job ids and cadence (02 §A2, §E2) · **Depends on:** 01 (`CollectorJob`/`JOB_TABLE`, `Http.request` + `ProviderLimiter` with provider ids `web` and `wayback` (G1), `Settings`, `/health`, `app/util/time.py`), 03 (all DDL (B7): `web_checks`, `watch_channels`, `manual_notes`, `metric_values` + `put_metric_values`, `events` + `emit_event`; metric-key and events catalogs §3.8/§3.9), 04 (populates `watch_channels` platforms `website`/`docs`; URL canonicaliser `app/registry/normalise.py`), 12 (`MetricResult`, registry rows 21–26, the `site_down_7d` flag rule), 14 (product-status entry into `manual_notes`, alert routing), 16 (liveness, pause groups) · **Master spec refs:** §1.3 (absence ≠ failure), §4 (tier-0 Wayback, no-key), §5 (`occupant_key`), §6 (storage rules), §7.3 P3 table, §7.5 `site_down_7d`, §7.7 web freshness window 2 d, §8 row "Web presence", §11 ops principles, §12 Phase 5.

---

## 1. Purpose and scope

The web collector answers, once a day per subnet, five questions the scoring engine cannot answer from chain or GitHub data: *is the declared website alive, is its TLS hygiene sane, how old is the site, does its content ever change, and is there developer documentation?* It produces the automated P3 metrics (`site_uptime_30d`, `tls_ok`, `site_age`, `site_freshness`, `docs_present`), joins the manual `product_status` entry (`manual_notes`) into the same metric batch, and emits the eight web events of 03 §3.9 (`site_down_7d`, `site_recovered`, `tls_expiring_14d`, `site_parked`, `site_hijack_suspect`, `site_content_reset`, `site_cross_domain_redirect`, `docs_gone`). `site_down_7d`/`site_recovered` are digest-only detection events; the page for the flag is 12's `red_flag_raised`/`red_flag_cleared` (02 §D4).

**In scope**
- One daily check of every active `website` and `docs` URL per occupant (from `watch_channels`): DNS, TLS, HTTPS GET with a bounded redirect chain, status classification, text extraction + normalised content hash + diff ratio, parked/hijack heuristics, robots.txt compliance.
- First-time (then monthly-until-found) Wayback first-capture lookup for site age; optional weekly Save-Page-Now (SPN2) when IA keys are present.
- Docs-link auto-discovery from the landing page (a candidate URL, checked like any other docs URL, never asserted as verified).
- P3 metric rows into `metric_values` (spec_version `ts-1.0`, through 03's `put_metric_values`, 02 §B4) and web events into `events` (through 03's `emit_event`, 02 §D1).

**Out of scope**
- JavaScript rendering (no Playwright/headless browser in v1; SPA shells are detected and handled as a measurement limitation, §4).
- Crawling beyond the landing page and the docs page(s). No sitemap walks, no link following, no forms, no logins.
- Any social/repo host: `x.com`, `discord.*`, `t.me`, `github.com`, `youtube.com` are *not websites* (host exclusion, §2.2) and are covered by specs 06/08/09.
- Editing identity sources. The registry (04) owns `watch_channels`; this collector only appends `web_autodiscover` docs candidates and refreshes their `last_seen_at` (§3.1) — everything else is read-only.
- Scoring, tiering, red-flag multiplication (spec 12 reads the events/metrics this spec writes).

---

## 2. Design

### 2.1 Components and data flow

```
watch_channels (platform ∈ website|docs, retired_at NULL)    manual_notes (kind='product_status')
        │ due = no web_checks row for the target in last 23.5 h         │
        ▼                                                               │
┌──────────────── web_check job (every 30 min, max_instances=1) ─────┐   │
│ 0 network preflight (DNS + TCP probe; fail ⇒ skip run, §7)         │   │
│ for each due target (concurrency 4, ≥10 s gap per host):           │   │
│  (URL normalised, §2.2) 1 robots.txt (7-d cache) → 2 DNS → 3 TLS   │   │
│  4 GET chain (≤5 hops, 10 s, 2 MB cap) → 5 classify status         │   │
│  6 extract (trafilatura→readability→plain) → 7 normalise → sha256  │   │
│  8 diff vs previous stored text → change_ratio                     │   │
│  9 parked / hijack / bot-challenge heuristics                      │   │
│ 10 docs-link discovery (website kind only)                         │   │
│ 11 Wayback CDX first capture (primary website; first time / 30 d)  │   │
│ 12 INSERT web_checks row ; per-check events via emit_event         │   │
└────────────────────────────────────────────────────────────────────┘   │
        │ raw rows                                                       │
        ▼                                                                ▼
┌──────── web_metrics job (00:05Z official; again 06/12/18:05Z) ─────────┐
│ per active/pre_launch occupant: the six C5 fns (§2.8) → put_metric_   │
│ values (as_of = today 00:00Z, first success wins) ; streak events     │
│ (site_down_7d, site_recovered, docs_gone) via emit_event              │
└───────────────────────────────────────────────────────────────────────┘
                     web_spn2 job (weekly, optional, IA keys) ──► archive.org
```

All outbound HTTP goes through 01's `Http.request(provider, method, url, …)` on top of `ProviderLimiter` (02 §G1), with provider id **`web`** for subnet sites and robots.txt and **`wayback`** for CDX, Availability and SPN2 (§6). 07 never constructs its own `httpx.AsyncClient`. Per request it passes `follow_redirects=False` (manual hop loop, §2.3 step 4), `retries=0` (a 5xx or timeout is an observation, not something to retry within seconds), `ok_statuses` = every status 100–599 (the caller classifies), and `headers={'User-Agent': WEB_USER_AGENT}`. Two things 07 needs from 01's `Http` that 01 does not yet define (§11 Q15): a body cap (`max_body_bytes`: stream, truncate, mark truncated) and no bucket-wide `Retry-After` penalty for the multi-host provider `web` (one slow site must not stall every other site; 07 applies `Retry-After` per host itself). The raw TLS handshake of §2.3 step 3 is not an HTTP call: it takes one `web` token from the limiter first, and the run adds its handshake count to `provider_spend.requests` via `costs.record('web', 0.0, requests=n)` (02 §G5).

### 2.2 URL selection

Input rows (03 §3.2 columns): `SELECT w.id, w.occupant_key, w.netuid, w.platform, w.handle, w.url, w.source, w.priority, w.is_resolved, w.first_seen_at FROM watch_channels w JOIN occupants o USING (occupant_key) WHERE w.platform IN ('website','docs') AND w.retired_at IS NULL AND o.status IN ('active','pre_launch')`. The target URL is `handle` (04's canonical URL); it is stored verbatim as `web_checks.url`, so the two join.

1. **Normalise** (`normalise_url`): 04 already canonicalises `handle` (lower scheme/host, strip fragment, `utm_*`/`ref`/`si`/`feature` params and one trailing `/`, add `https://` when missing); 07 calls 04's `app/registry/normalise.py` website canonicaliser for the URLs it discovers itself (§2.3 step 10) so its handles match 04's. Before fetching, drop default ports and reject anything that is not `http`/`https` or whose host is an IP literal or `localhost` (→ target skipped, logged `invalid_url`). Junk placeholders (`deprecated.com`, …) never reach 07: 04 drops them as `junk_host`.
2. **Host exclusion** (`is_excluded_host`): `registrable_domain(host)` must not be in `WEB_EXCLUDED_HOSTS` (so `docs.github.com` and `www.x.com` are excluded too). The default list ports the old bot's `has_website` rule verbatim (`E:\SN_Trader\SubnetTrader\app\strategy\quality.py:153-156`: `x.com, twitter.com, discord.com, discord.gg, t.me, github.com, youtube.com`) and adds directory/aggregator hosts that are never the subnet's own product site: `taostats.io, taomarketcap.com, subnetalpha.ai, tao.app, linktr.ee, linkin.bio`. Excluded URLs are skipped, and an occupant whose *every* website candidate is excluded counts as **no website** (verifiable absence).
3. **Primary website**: the non-excluded `website` row with `is_resolved = 1` (04's resolved winner); if none survives exclusion, the one with the lowest `priority` (03: manual `force` 5, chain 10, subnetalpha 20, tmc 30, manual `fallback`/legacy 40, operator 45); ties → earliest `first_seen_at`, then `id`. At most `WEB_MAX_WEBSITES_PER_OCCUPANT` (2) website rows are checked (`web_checks.is_primary` = 1 for the primary, 0 for the second); metrics use the primary only.
4. **Dedupe across occupants**: identical normalised URLs shared by several occupants (e.g. one studio running several subnets) are fetched once per day and the resulting row is written once per occupant (same `content_hash`, `run_id`).
5. **Due**: a target `(occupant_key, url, kind)` is due when it has no `web_checks` row with `event_at > now_ts() − WEB_PER_URL_MIN_INTERVAL_H·3600` (INTEGER unix s, 02 §B1). Due targets are grouped by `(url, kind)` so each URL is fetched once per run, and processed oldest-first. This watermark makes the job idempotent across restarts and power loss (master §11). A URL newly declared by a second occupant after today's fetch is fetched once more that day (rare).

### 2.3 Per-URL pipeline

| Step | What | Detail |
|---|---|---|
| 1 | robots.txt | `Http.request('web', 'GET', f'https://{host}/robots.txt', timeout=5, …)` (≤ 64 KB), parsed with `urllib.robotparser.RobotFileParser.parse(lines)`; cached in memory per host for `WEB_ROBOTS_CACHE_DAYS`. Unreachable/4xx/5xx robots ⇒ allowed. The test is `can_fetch('TaoSentinel', url)` with the **bare token**: `robotparser` compares a rule's agent with the part of the given UA before the first `/`, so passing the browser-like `WEB_USER_AGENT` would match `Mozilla`, not us; `*` rules apply when there is no `TaoSentinel` group. Disallowed ⇒ **HEAD-only** liveness request (no body, no extraction; `robots_allowed=0`, `content_hash=NULL`); a HEAD answered 405/501 is classed `bot_challenge` (indeterminate: we may not look). Crawl-delay is honoured up to 30 s. |
| 2 | DNS | `await loop.getaddrinfo(host, port, type=SOCK_STREAM)` under `asyncio.timeout(WEB_DNS_TIMEOUT_S)`. `socket.gaierror` with `EAI_NONAME` (or `EAI_NODATA` where defined: the name does not exist) ⇒ `dns_fail`; `EAI_AGAIN` (temporary resolver failure) or a timeout ⇒ `timeout` (indeterminate: our resolver, not their site). First address stored in `resolved_ip`. |
| 3 | TLS | Against the **declared** host on 443 (the one the subnet published — an expired cert there is the red flag: research `01-web-ecosystem-2026.md` §2, Score's dking.bot, and §5 item 12). For an `http://` URL the probe runs after step 4, against the first `https` hop's host, else the declared host; 443 refused or no TLS while http answers ⇒ `tls_ok=0` (`no_tls`). One `web` limiter token, then `await asyncio.to_thread(ssl.get_server_certificate, (host, 443), timeout=WEB_TIMEOUT_S)` (unverified context with SNI, so an expired or mismatched cert is still read; `timeout` exists since Python 3.10) → PEM → `cryptography.x509.load_pem_x509_certificate` → `not_valid_after_utc` (cryptography ≥ 42), issuer (`O` then `CN`), SAN DNSNames (no SAN ⇒ mismatch; browsers ignore CN). `tls_days_left = floor((not_valid_after − now)/86400)`; `tls_not_after` INTEGER. Hostname match implemented locally (`ssl.match_hostname` was removed in Python 3.12): exact SAN match, or one-label wildcard `*.example.com` matching `sub.example.com` only, compared lower-case in A-label form. Row `tls_ok` = 1 iff the GET's handshake with the declared host verified (certifi chain) ∧ hostname matches ∧ `tls_days_left > WEB_TLS_WARN_DAYS`; 0 on `tls_fail`, ≤ 14 d left, mismatch or `no_tls`; NULL when not evaluated (no TLS response reached). Handshake errors here do **not** decide `status_class`; the GET does. |
| 4 | GET chain | `Http.request('web', 'GET', url, …)` (§2.1 arguments) with headers `User-Agent: WEB_USER_AGENT`, `Accept: text/html,application/xhtml+xml;q=0.9,*/*;q=0.5`, `Accept-Language: en` and **no explicit `Accept-Encoding`** (httpx advertises only codecs it can decode; forcing `br` without the `brotli` package yields undecodable bodies). `timeout=httpx.Timeout(WEB_TIMEOUT_S, connect=WEB_CONNECT_TIMEOUT_S)`. Manual redirect loop: on 301/302/303/307/308 resolve `Location` against the current URL, append `{url, status, location}` to `redirect_chain`, stop after `WEB_MAX_REDIRECTS` hops (⇒ `too_many_redirects`); the hops of one check form one fetch (§6's per-host gap applies between checks, not between hops). Body truncated at `WEB_MAX_BODY_BYTES` (01's `max_body_bytes`, §2.1); non-HTML content types (`application/pdf`, images) keep the status but skip extraction (`content_hash` of the raw bytes' first 1 MB, `extractor='raw'`). `latency_ms` = the first hop's `response.elapsed` (includes the body read). Transport errors are classified from the exception chain (`ProviderError.__cause__`; 01's `Http` must raise `… from exc`). |
| 5 | Classify | §2.4 decision table. |
| 6 | Extract | `trafilatura.extract(html, url=final_url, include_comments=False, include_tables=True, include_links=False, favor_precision=True, output_format='txt')` in `asyncio.to_thread`. If `None` or `< 200` chars → `readability.Document(html).summary()` → tags stripped with `lxml.html.fromstring(...).text_content()`. If still `< 200` chars → plain text: drop `<script>`, `<style>`, `<noscript>`, `<svg>`, `<template>` subtrees, then `text_content()`. Always prepend `<title>` and `<meta name=description>` / `og:description` so SPA shells hash something meaningful. Record `extractor ∈ {trafilatura, readability, plain, raw, none}`. |
| 7 | Normalise + hash | `normalise(text)`: NFKC, lowercase; delete ISO dates `\d{4}-\d{2}-\d{2}(t[\d:]+z?)?`, month-name dates `\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+\d{1,2},?\s+\d{2,4}\b` and `\d{1,2}\s+(jan|…|dec)[a-z]*\s+\d{4}`, clock times `\b\d{1,2}:\d{2}(:\d{2})?\s*(am|pm)?\b`, relative times `\b\d+\s+(second|minute|hour|day|week|month|year)s?\s+ago\b`, `(©|\(c\)|copyright)\s*(\d{4}(\s*[-–]\s*\d{4})?)?` (removes the year after `©` as well — the earlier form `©|\(c\)|copyright\s*\d{4}…` left `2026` behind and changed every hash on 1 January), numbers ≥ 7 digits with optional separators `\b\d[\d,.]{6,}\b`, hex tokens ≥ 16 chars `\b[0-9a-f]{16,}\b`, UUIDs, base64-looking tokens ≥ 24 chars `\b[a-z0-9+/=_-]{24,}\b` (nonces, asset hashes); collapse whitespace to single spaces; strip. `content_hash = sha256(norm.encode()).hexdigest()`, `content_len = len(norm)`. |
| 8 | Diff | `prev` = latest row for the same `(occupant_key, url)` with `content_text IS NOT NULL`. `change_ratio = difflib.SequenceMatcher(None, prev.split()[:5000], cur.split()[:5000]).ratio()` (word tokens: linear-ish on 5k tokens; character diffs on 32 KB strings are too slow). `1.0` = identical. When `content_hash` differs from `prev.content_hash` the row stores `content_text` (normalised, truncated to `WEB_STORE_TEXT_MAX_CHARS`); otherwise `content_text = NULL` (text is only stored on change, so storage grows with churn, not with days). First observation ever: `content_text` stored, `change_ratio = NULL`. |
| 9 | Heuristics | Parked, hijack and bot-challenge scoring, §2.5. |
| 10 | Docs discovery | `website` kind only, only when outcome is `up`. Parse `<a href>` (stdlib `html.parser`), match anchor text or href path against `(?i)\b(docs?|documentation|developer(s)?|api reference|whitepaper|litepaper|gitbook|readthedocs|mintlify)\b`; resolve relative to `final_url`; exclude hosts in `WEB_EXCLUDED_HOSTS` plus `docs.bittensor.com`, `docs.learnbittensor.org`; first match → `docs_candidate_url` (canonicalised by 04's website canonicaliser). If `WEB_DOCS_AUTODISCOVER_ENABLED` and the occupant has **no** active `docs` row in `watch_channels`, `INSERT OR IGNORE` one (`platform='docs'`, `handle` = `url` = the canonical URL, `source='web_autodiscover'`, `priority=90`, `verified_at=NULL`, `first_seen_at = last_seen_at = now`; `active` is generated from `retired_at` and never inserted). While the landing page still links it, 07 sets `last_seen_at = now` on that row — its only UPDATE — so 04's 14-d not-seen retirement never retires a live candidate. The new target is checked like any docs URL on the next tick. |
| 11 | Wayback | §2.6. |
| 12 | Write | One `web_checks` row per due target (§3.2, `INSERT OR IGNORE`); per-check events through `emit_event` (§3.5). Never raise; any unexpected exception in steps 1–11 becomes a row with `status_class='conn_error'`, `error='internal:<ExcClass>: <msg[:200]>'`, counted in the run summary (§7). Rows whose `error` starts with `internal:` or `network_suspect` are **always indeterminate** (§2.4 footnote): a bug in our code or our own network must never read as a site outage or advance a `site_down_7d` streak. |

### 2.4 Status classification

`status_class` is decided in this order (first match wins):

| # | Condition | `status_class` | Uptime outcome |
|---|---|---|---|
| 1 | DNS `gaierror` `EAI_NONAME`/`EAI_NODATA` (§2.3 step 2) | `dns_fail` | down |
| 2 | Any `httpx.TimeoutException` (connect/read/write/pool) on any hop, a DNS timeout or `EAI_AGAIN` | `timeout` | indeterminate¹ |
| 3 | `httpx.ConnectError` whose cause is `ssl.SSLError` / `SSLCertVerificationError` (expired, untrusted CA, hostname mismatch, protocol failure) | `tls_fail` | down |
| 4 | `httpx.ConnectError` (refused/reset/unreachable, not DNS, not TLS), `RemoteProtocolError`, `ReadError`, `DecodingError`, internal exception | `conn_error` | indeterminate¹ |
| 5 | Redirect hop count > `WEB_MAX_REDIRECTS` (still 3xx) | `too_many_redirects` | down |
| 6 | Final status 429 (any body: the server is alive and throttling us; its `Retry-After` is honoured for that host), or 403/503 **and** `is_bot_challenge()` true (§2.5.3), or 405/501 to a robots-mandated HEAD | `bot_challenge` | indeterminate (never converts to down) |
| 7 | Final status 2xx and `hijack_score ≥ 3` | `hijacked_suspect` | down |
| 8 | Final status 2xx and (`parked_score ≥ 3` or final registrable domain ∈ `WEB_PARKING_HOSTS`) | `parked` | down |
| 9 | Final status 2xx, `redirect_hops = 0` | `up_2xx` | up |
| 10 | Final status 2xx, `redirect_hops ≥ 1` (incl. http→https, apex↔www, and cross-domain to a non-parking domain — recorded via `cross_domain=1` and the `site_cross_domain_redirect` event) | `redirect_ok` | up |
| 11 | Final status 4xx | `client_error` | down |
| 12 | Final status 5xx (or 3xx with no `Location`) | `server_error` | down |

¹ **Indeterminate rule** (brief): a `timeout`/`conn_error` day counts as *failed* only when the previous check day for that URL was also not `up`; otherwise it is excluded from both numerator and denominator. Within a run, a first `timeout`/`conn_error` triggers **one** retry after `WEB_RETRY_DELAY_S` (default 600 s, scheduled inside the same run) before the row is written; the row records `attempts=2` and `event_at` = the start of the first attempt. Rows whose `error` starts with `internal:` or `network_suspect` (§2.3 step 12, §7) are indeterminate unconditionally and do not count as "not up" for the next day's rule.

The classes `conn_error`, `too_many_redirects`, `bot_challenge` are additions to the brief's list; they are necessary because "connection refused" is neither DNS nor timeout, a redirect loop is not a 4xx, and Cloudflare/Vercel challenge pages return 403/503 to non-browser clients and would otherwise mark live sites as down.

**Cross-domain**: `registrable_domain(host)` uses `tldextract.TLDExtract(suffix_list_urls=())` (offline PSL snapshot bundled with the package; no network) with `include_psl_private_domains=False` (the default), so `x.vercel.app` → `vercel.app` — §2.6's shared-platform test relies on this; it falls back to "last two labels" if the import fails. (Newer tldextract releases rename `.registered_domain` to `.top_domain_under_public_suffix`; use whichever exists — version UNVERIFIED.) `cross_domain = registrable_domain(final_host) != registrable_domain(declared_host)`.

### 2.5 Heuristics (pure functions in `web_classify.py`)

#### 2.5.1 Parked page (`parked_signals(html, norm_text, final_host, links, cfg) -> list[str]`, `parked_score = Σ weights`)

| Signal id | Weight | Test |
|---|---|---|
| `parking_host` | 3 | `registrable_domain(final_host)` ∈ `WEB_PARKING_HOSTS` |
| `strong_phrase` | 3 | `<title>` or first 600 chars of `norm_text` contain any `WEB_PARKED_STRONG_PHRASES` |
| `weak_keywords_2` | 2 | ≥ 2 **distinct** `WEB_PARKED_WEAK_KEYWORDS` anywhere in `norm_text` |
| `parking_script` | 1 | any `<script src>`/`<iframe src>`/`<meta generator>` host ∈ `WEB_PARKING_HOSTS` |
| `ad_template` | 1 | `norm_text` contains `related searches` or `sponsored listings` or `privacy policy` + `related links` |
| `tiny_body_links` | 1 | `content_len < WEB_TINY_BODY_CHARS` **and** `is_spa = 0` **and** ≥ 1 `<a href>` to an external registrable domain |

`parked = parked_score ≥ 3`. Defaults:
`WEB_PARKING_HOSTS = ["sedoparking.com","sedo.com","hugedomains.com","dan.com","afternic.com","godaddy.com","parkingcrew.net","bodis.com","above.com","undeveloped.com","domainmarket.com","sav.com","dynadot.com","namecheap.com","porkbun.com","squadhelp.com","atom.com","brandbucket.com","namesilo.com","parklogic.com","ndsplus.com","cashparking.com"]` (matched on registrable domain of the **final** host only; a site merely *registered* at GoDaddy is unaffected).
`WEB_PARKED_STRONG_PHRASES = ["domain for sale","this domain is for sale","buy this domain","domain is parked","parked free","parked domain","domain parking","domain name for sale","this webpage was generated by the domain owner","website expired","this account has been suspended","account suspended","this site can't be reached","hosting account has expired","is for sale!","welcome to nginx!","apache2 debian default page","apache2 ubuntu default page","test page for the apache http server"]`.
`WEB_PARKED_WEAK_KEYWORDS = ["make an offer","get this domain","for sale","sedo","sedoparking","godaddy","hugedomains","afternic","dan.com","namecheap","parkingcrew","bodis","domain owner","buy now","inquire","coming soon","under construction","placeholder page","default web page"]`.
`coming soon`/`under construction` are deliberately *weak* — a legitimate pre-launch subnet may show them; they only tip a page into `parked` together with another signal. Default web-server pages are *strong*: as weak keywords a stock nginx page scored only `tiny_body_links` (1) and counted as up; `it works!` is dropped (it matches "how it works!"). These lists and weights are design heuristics, not taken from the research corpus (UNVERIFIED against a labelled sample); tune them with the §9 fixtures and the first week's `parked_signals`.

#### 2.5.2 Hijack suspect (`hijack_signals(...)`, `hijack_score = Σ weights`)

| Signal id | Weight | Test |
|---|---|---|
| `cross_domain_new` | 2 | `cross_domain=1` to a non-parking domain **and** some previous row for this URL was `up_2xx`/`redirect_ok` with `cross_domain=0` |
| `scam_keywords_2` | 2 | ≥ 2 distinct `WEB_HIJACK_KEYWORDS` in `norm_text` |
| `content_reset` | 1 | `change_ratio IS NOT NULL AND change_ratio < WEB_CONTENT_RESET_RATIO` |
| `issuer_and_ip_change` | 1 | `resolved_ip` **and** `tls_issuer` both differ from the previous row |

`hijacked_suspect = hijack_score ≥ 3` (i.e. needs a cross-domain jump or scam text plus corroboration; a plain rebrand redirect alone is `redirect_ok`). Default `WEB_HIJACK_KEYWORDS = ["casino","slot machine","betting","sportsbook","viagra","cialis","escort","xxx","porn","adult content","crypto giveaway","airdrop claim","connect your wallet","claim your reward","payday loan","essay writing","replica watches","free spins"]`. A transition into `hijacked_suspect` (previous row of the URL not `hijacked_suspect`) emits `site_hijack_suspect` (§3.5; an immediate alert, so not re-sent every day the state persists) — human review, never auto-cleared.

#### 2.5.3 Bot challenge (`is_bot_challenge(status, headers, html) -> bool`)

True when status ∈ {403, 429, 503} and any of: header `cf-mitigated: challenge`; `server: cloudflare` with body containing `Just a moment...`, `_cf_chl_opt`, `challenge-platform`, `Attention Required! | Cloudflare`; body containing `Vercel Security Checkpoint`; header `x-akamai-*` with body `Access Denied` + `Reference #`; `x-sucuri-id` header; body containing `Enable JavaScript and cookies to continue`. Outcome is indeterminate for uptime; TLS inspection still counts (the handshake succeeded).

#### 2.5.4 SPA detection (`detect_spa(html) -> bool`)

True when the plain-text body is `< WEB_TINY_BODY_CHARS` and the HTML contains any of `<div id="root">`, `<div id="__next">`, `<div id="app">`, `data-reactroot`, `__NUXT__`, `<noscript>` with `enable JavaScript`, `<script type="module" src="/assets/index-`, `_next/static`. SPA rows have `is_spa=1`; the tiny-body parked signal is disabled and `site_freshness` is `None` when `content_len < 300` (limitation, not absence).

### 2.6 Wayback (site age) and SPN2

**When**: only for **primary website** targets (site age is a website metric; docs are never looked up), when no row of that `(occupant_key, url)` carries `wayback_first_capture` **and** (`wayback_checked_at` is NULL or older than `WEB_WAYBACK_RECHECK_DAYS`·86400). Once a first capture is known it is copied into every later daily row of that URL (no re-query). All archive.org calls use provider `wayback` (G1): `WAYBACK_RATE_PER_SEC` 0.5, burst 1 — half the ~1 req/s community limit in research `01-web-social-apis.md` §4.

**Target selection**: if `registrable_domain(host)` ∈ `WEB_SHARED_PLATFORM_HOSTS` (`github.io, vercel.app, netlify.app, pages.dev, notion.site, webflow.io, framer.website, framer.app, gitbook.io, mintlify.app, readthedocs.io, super.site, carrd.co, wixsite.com, squarespace.com, substack.com, medium.com, super.so, softr.app, bubbleapps.io`) the target is the declared URL itself, host + path (`proj.vercel.app/`, `medium.com/@proj`; the platform root would give the platform's age); otherwise it is the root `{registrable_domain}/` (the domain's age, not the current path's). CDX keys captures by SURT, which drops the scheme and a leading `www.`, so one exact-match query covers `http`/`https` and apex/`www.` (IA canonicalisation; UNVERIFIED for the live service, hence the `www.` retry in step 1). Do **not** use `matchType=domain&limit=1`: the CDX server sorts by SURT key then timestamp, so `limit=1` under `matchType=domain` returns the first capture of the lexicographically first URL, not the earliest capture (documented CDX ordering; a known footgun). Known limitation: a domain with an earlier owner reports that owner's first capture (§11 Q3).

1. **CDX first capture** (primary): `GET https://web.archive.org/cdx/search/cdx?url={target}&output=json&fl=timestamp,original,statuscode&filter=statuscode:200&limit=1` → `[["timestamp","original","statuscode"],["20210305123456","https://example.com/","200"]]`. An exact-URL query returns captures in timestamp order, so the single row is the earliest 200 capture. Empty list ⇒ retry once without the filter (roots that only ever redirected), then once, unfiltered, with `www.{registrable_domain}/`; still empty ⇒ no capture known: `wayback_first_capture = NULL`, `wayback_checked_at = now` (re-query after 30 d). `YYYYMMDDhhmmss` → unix seconds UTC.
2. **Availability** (fallback, only when CDX answered 5xx or timed out): `GET https://archive.org/wayback/available?url={target}&timestamp=19960101` → `{"archived_snapshots": {"closest": {"available": true, "timestamp": "20210305123456", "status": "200", "url": "…"}}}` or `{"archived_snapshots": {}}`. The `timestamp` parameter makes `closest` the capture nearest 1996-01-01, i.e. the earliest; without it `closest` is the most recent capture — the earlier draft used that as a first-capture fallback, which was wrong. An empty fallback answer is not trusted as "no capture" (the Availability API is reported to return empty results for archived URLs — UNVERIFIED): both fields stay NULL and the lookup is retried at the URL's next check.
3. Store `wayback_first_capture` and `wayback_checked_at` (INTEGER). Responses are immutable; the cache is the table itself. A 429/503/timeout on every attempt ⇒ both fields stay NULL (a failure, not absence).

**Budget**: ≈ 130 primary website URLs × 1–2 requests (≤ 4) on the first run ≈ 200 requests ≈ 7 min at 0.5 req/s; steady state ≤ ~30/month (new URLs, 30-d rechecks of uncaptured sites). A `429`/`503` from archive.org sets `wayback_backoff_until = now + max(Retry-After, 3600)` (a runtime watermark in `job_state`, B12) and skips Wayback lookups until then; the affected URLs are looked up at their next daily check. The web check itself is unaffected.

**SPN2 (optional)**: job `web_spn2` runs weekly (Sun 07:00Z, §2.7) only when `WEB_SPN2_ENABLED` and both `IA_ACCESS_KEY`/`IA_SECRET_KEY` are set (18-operator-checklist B7). For each primary website URL whose last check was `up`: `Http.request('wayback', 'POST', 'https://web.archive.org/save', headers={'Accept': 'application/json', 'Authorization': f'LOW {IA_ACCESS_KEY}:{IA_SECRET_KEY}'}, data={'url': url, 'if_not_archived_within': '7d', 'skip_first_archive': '1'})`. The response `{"url": …, "job_id": "…"}` is logged (`data={"job_id": …}`); no status polling in v1. Captures are spaced `WEB_SPN2_MIN_INTERVAL_S` (10 s) apart inside the job, on top of the `wayback` bucket (there is no separate `spn2` provider id, G1); ≈ 130 URLs ≈ 22 min. The research corpus gives only `POST https://web.archive.org/save/<url>` with IA S3 keys and `if_not_archived_within=1d`; the form-body variant, the `LOW` auth scheme, `skip_first_archive` and SPN2's per-account limits come from the SPN2 public API document and are UNVERIFIED here (§11 Q10). Failures are logged and counted, never alerted (optional feature, $0).

### 2.7 Job schedule

**The complete job list of this spec. 01's `JOB_TABLE` mirrors it verbatim (02 §A2, §E2)** and replaces the placeholder `web_daily` (and its `WEB_INTERVAL_MIN` key) with these three rows. All rows: `max_instances=1`, `coalesce=True` (01), not `paid`, not `ops_exempt`.

| id | factory (`app.…`) | trigger | UTC time / interval | misfire grace s | timeout s | enabled flag | phase | provider | paid | liveness max-age s (16 §2.5 formula) |
|---|---|---|---|---|---|---|---|---|---|---|
| `web_check` | `collectors.web:WebCheckJob` | interval (`interval_flag=WEB_TICK_MINUTES`), 01 first-run stagger, `min_gap_factor` 0.5 | every 30 min | 900 | 1500 (a run that hits it leaves its remaining targets due for the next tick) | `WEB_ENABLED` | 5 | `web`, `wayback` | no | 5,220 (30·60·2 + 1,500 + 120) |
| `web_metrics` | `collectors.web:WebMetricsJob` | cron (UTC), `min_gap_factor=0` | **00:05Z** (official, before `scoring_6h` at 00:15Z, E3) and 06:05, 12:05, 18:05Z (`WEB_METRICS_CRON_HOURS`, `WEB_METRICS_CRON_MINUTE`). Every run keys the current UTC day's 00:00Z slot (§2.8), so later runs only fill keys that are missing or `failed` (B4) — this is the power-loss catch-up (a restart loses APScheduler's in-memory schedule) — and re-evaluate streak events | 3600 | 600 | `WEB_ENABLED` | 5 | — (DB only) | no | 43,920 (360·60·2 + 600 + 120) |
| `web_spn2` | `collectors.web:WebSpn2Job` | cron (UTC), `min_gap_factor=0` | Sun 07:00Z (`WEB_SPN2_WEEKDAY`, `WEB_SPN2_HOUR_UTC`; clear of the 04:30Z reboot window and 05:00Z `quant_weekly`, E3) | 21600 | 3600 | `WEB_SPN2_ENABLED` (validator forces false without IA keys) | 5 | `wayback` | no | 1,213,320 (10,080·60·2 + 3,600 + 120) |

All three are 01 `CollectorJob` subclasses; the wrapper never raises into APScheduler, writes `collector_runs`, feeds `/health`, and fires `collector_failing` after `JOB_FAIL_ALERT_THRESHOLD` consecutive failed runs (01; D1). Each run reads live settings (01 hot-reload rule). Pauses follow 02 §B12: `KILL_SWITCH` does not pause these free jobs; `PAUSE_ALL` does; an operator pause is a `control_state` row whose target is a job id or the group `web` (16's `PAUSE_GROUPS` row `web` = {`web_check`, `web_metrics`, `web_spn2`}); there is no WEB-specific kill sentinel (B12's closed list); `data/control/<target>.paused` is a read-only emergency input. A paused job writes nothing, so P3 goes `None` after 12's 2-d window — never `0.0`.

### 2.8 Metric derivation (`app/features/web_metrics.py`)

**Producer fns (02 §C5).** One coroutine per key — `compute_site_uptime_30d`, `compute_tls_ok`, `compute_site_age`, `compute_site_freshness`, `compute_docs_present`, `compute_product_status` — each `async def compute_<key>(db, occupant_key: str, as_of: datetime) -> MetricResult` (12's `app/features/types.py::MetricResult`). `as_of` is the evaluation instant `T_eval` (tz-aware UTC); every read uses `observed_at ≤ T_eval`. The six share one cached loader (`load_web_inputs`, one query set per `(occupant_key, T_eval)`, §8.4) and a pure core (`derive_web_metrics`). With `D = floor_day(T_eval)`: the uptime window is the 30 check days `D−30 … D−1` (complete UTC days before the slot; today's partial day is ignored, so every run of the day computes the same value), and **"latest"** means the newest row with `check_day_ts ∈ {D−2, D−1}` (`WEB_LATEST_MAX_AGE_DAYS` = 2). An older row is never promoted into today's metric (`failed`, reason `no_recent_check`), so a paused or broken `web_check` cannot pass stale observations through 12's 2-d window (C6). The return mapping is 12 §2.3's: `absence_reason` set → `absent`; `value=None` → `not_applicable` when `value_json["status"] == "not_applicable"`, else `failed`; otherwise `ok`. **No website** = no `website` target with `first_seen_at ≤ T_eval` and (`retired_at` NULL or > `T_eval`) that survives §2.2's host exclusion.

| metric_key | Computation (U = rows with `is_primary=1` in the window, whichever URL was primary that day; P = rows of the current primary URL; D = docs rows) | status / reason · `value_json` | Normaliser · w (master §7.3; 12 rows 21–26) |
|---|---|---|---|
| `site_uptime_30d` | Outcomes (§2.4) over U; `value = up/(up+down)` | no website → `absent` 0.0, `no_website` (`value_json.excluded` lists excluded candidates); `up+down < WEB_UPTIME_MIN_CHECKS` (3) → `failed` `insufficient_checks`; no primary row in the last 2 check days → `failed` `no_recent_check`. `{up, down, indeterminate, url}` | identity · .30 |
| `tls_ok` | Latest P row: `1.0` if `tls_ok=1`; `0.0` if `tls_ok=0` | `ok` (0.0 is an observation: `reason` ∈ `expiring`, `expired`, `hostname_mismatch`, `tls_fail`, `no_tls`); no website → `not_applicable`; latest row `tls_ok IS NULL` (timeout, conn_error, dns_fail, challenge without cert) → `failed` `tls_not_evaluated`; no recent row → `failed` `no_recent_check`. `{tls_days_left, tls_not_after, tls_issuer, url}` | binary · .10 |
| `site_age` | `(D − wayback_first_capture)/86400` days, from the newest P row carrying it (any age: age only grows). If Wayback answered "no capture" (`wayback_checked_at` set, capture NULL) → `(D − first up event_at of this URL for this occupant)/86400` with `value_json.source='self_observed'` (a floor on the true age; §11 Q3), else `'wayback'` | no website → `not_applicable`; Wayback never answered → `failed` `wayback_unavailable`; no capture and no up row ever → `failed` `no_capture` | `age_ramp` (≤30 d→.2, ≥365 d→1, linear) · .10 |
| `site_freshness` | Days from D to the `event_at` of the newest P row whose text changed **materially** (`change_ratio ≤ 1 − WEB_FRESHNESS_MIN_DELTA`, 0.03), else to the first stored text (`value_json.baseline=true`) | no website, latest up row `is_spa=1 AND content_len<300` (`spa_shell`), or every recent row HEAD-only (`robots_head_only`) → `not_applicable`; no up P row in the last 2 check days → `failed` `no_recent_up` | `recency(d, 90, 365)` · .15 |
| `docs_present` | `1.0` if any D row in the last 2 check days is `up_2xx`/`redirect_ok` with `content_len ≥ 200` or `is_spa=1`; `0.0` if every docs URL that was ever up is definitively down for ≥ `WEB_DOCS_GONE_STREAK_DAYS` (2) consecutive checks | 1.0 and the docs-down 0.0 are `ok` (`reason='docs_down'`); no docs target while the latest P row is `up` with `extractor ≠ 'none'` (the landing page was parsed and links no docs) → `absent` 0.0 `no_docs_link`; no website and no docs target → `not_applicable` `no_website`; otherwise (website down/indeterminate, docs timeout/challenge, docs target not yet checked) → `failed` `docs_undetermined` | binary · .15 |
| `product_status` | §2.9 | §2.9 | identity on the mapped value (12 row 26) · .20 |

**Writer.** 12's registry lists P3 as `producer='collector'`, and 07 writes the 00:00Z row (12 §2.2). The `web_metrics` job calls the six fns for every occupant with `status IN ('active','pre_launch')` at `T_eval` = run start and writes the results through 03's `put_metric_values` (02 §B4): `stage='raw'`, `as_of = D` (the official 00:00Z slot), `spec_version='ts-1.0'`, `source='web'` (B9), `observed_at = T_eval`, `run_id`, `backfilled=0`. `value_json.status` comes from the mapping above; a status column is never written (B3). First success wins: a later run the same day replaces only keys whose row is `failed`; an `ok`/`absent`/`not_applicable` row is never replaced (B4 — the old "append-only, reader takes the latest `observed_at`" rule is withdrawn). A CLI replay for a past day (`--as-of YYYY-MM-DD`) evaluates at `T_eval` = that day 00:05Z and writes `observed_at = now`, `backfilled=1` (J1). A late row (e.g. after power loss) is invisible to that day's official scoring run, and 12 falls back to the previous day's row inside the 2-d window. Because the fns follow C5, 12 may also attach them with `register_metric` (first success wins; 12 §11 Q18); in ts-1.0 it does not.

### 2.9 `product_status` join

```sql
SELECT id, value, evidence_url, created_at, created_by
FROM manual_notes
WHERE occupant_key = :ok AND kind = 'product_status' AND created_at <= :t_eval
ORDER BY created_at DESC, id DESC LIMIT 1;
```
The latest `manual_notes` row per `(occupant_key, kind)` is current (03 §3.2; product status does not live in `manual_overrides`). `value ∈ {none, waitlist, beta, live}` → `0.0 / 0.3 / 0.6 / 1.0`, status `ok`, `value_json = {"product_status": value, "evidence_url": …, "set_at": created_at, "note_id": id}`. The enum goes in `value_json.product_status`, never in `value_json.status` (02 §B3; 03's generated `status` CHECK would reject `waitlist`/`beta`/`live`). No row → `failed` `unset` (a missing manual entry is not evidence of "no product"); latest row `value IS NULL` (operator cleared it) → `failed` `cleared`; an unknown value (03's CHECK prevents it; defensive) → `failed` `invalid` + `logger.warning("product_status_invalid")`. Entry: 14's `POST /api/v1/manual/product` and Telegram `/product set <netuid> <status> <url>` (18-D2) write `manual_notes(kind='product_status')` for the *current* occupant; a `netuid_reoccupied` event (04) leaves the old occupant's rows untouched and the new occupant unset. 14's needs-review digest has no "product unset" bucket yet (§11 Q17).

---

## 3. Data contract

### 3.1 Tables read

| Table | Columns needed (exact) | Notes |
|---|---|---|
| `watch_channels` | `id, occupant_key, netuid, platform, handle, url, source, priority, is_resolved, verified_at, first_seen_at, last_seen_at, retired_at` (03 §3.2; `active` is generated from `retired_at`) | `platform ∈ {'website','docs'}`; `handle` = 04's canonical URL (stored as `web_checks.url`); `priority` lower wins (03's values, §2.2 step 3); `source` ∈ 03's list (`chain`, `subnetalpha`, `tmc`, `tmc_coldkey`, `manual`, `legacy`, `operator`, `web_autodiscover`, `seed_feeds`). **Written by this spec only for its own rows** `source='web_autodiscover', platform='docs'` (§2.3 step 10): `INSERT OR IGNORE` (`priority=90, verified_at=NULL, first_seen_at=last_seen_at=now`) and `UPDATE … SET last_seen_at` on those rows; never DELETE, never other rows. |
| `occupants` | `occupant_key, netuid, status` | `status IN ('active','pre_launch')` filter |
| `manual_notes` | `id, occupant_key, kind, value, evidence_url, created_at, created_by` | `kind='product_status'` (03 §3.2; entry path owned by 14) |
| `web_checks` | §3.2 | previous rows: diff, streaks, Wayback cache, PrevState |
| `events` | `id, occupant_key, event_type, event_at, payload` | the latest `site_down_7d`/`site_recovered` per `(occupant_key, payload.url)` = open-streak state (§3.5); dedupe itself is `emit_event`'s |

### 3.2 Table written: `web_checks` (raw, append-only, PIT; DDL owned by 03 §3.4)

03 §3.4 holds the final DDL (02 §B7; 03 adopted this spec's draft with 02 §B1's INTEGER times), `STRICT`, `UNIQUE (occupant_key, url, kind, check_day_ts)`, listed in 03's `APPEND_ONLY_TABLES`; 07 writes with plain `INSERT OR IGNORE` (never `Database.upsert()`) and never UPDATEs a row. Columns 07 fills: `occupant_key, netuid, url` (= `watch_channels.handle`), `kind` (`website`|`docs`), `is_primary` (1 = primary website and every docs row, 0 = second website), `source` (the target's `watch_channels.source`), **`event_at`** (INTEGER; start of the first attempt — this spec's former `checked_at`), `observed_at` (INTEGER; row write), **`check_day_ts`** (`floor_day(event_at)`; the former TEXT `check_day`), `run_id`, `attempts`, `status_class` (the 12 classes of §2.4; 03's CHECK), `http_status, final_url, final_host, redirect_hops, redirect_chain` (JSON `[{url,status,location}]`), `cross_domain, resolved_ip, latency_ms, robots_allowed, tls_days_left, tls_ok, tls_hostname_match, tls_issuer`, **`tls_not_after`** (INTEGER), `content_type, extractor, is_spa, content_hash, content_len, content_text` (normalised text, only when the hash changed, ≤ `WEB_STORE_TEXT_MAX_CHARS`), `change_ratio` (NULL on the first observation), `parked, parked_signals` (JSON), `hijack_signals` (JSON), `docs_candidate_url`, **`wayback_first_capture`**, **`wayback_checked_at`** (INTEGER; the capture is copied forward once known), `error` (incl. the `internal:` and `network_suspect` markers of §2.4). Retention is 03's `web_text` policy: `content_text` is nulled after `WEB_TEXT_RETENTION_DAYS` (365) except the newest non-NULL text per `(occupant_key, url, kind)`, which step 8 diffs against. Volume ≈ 200 rows/day, < 100 KB/day without text.

### 3.3 Table written: `metric_values` (through `put_metric_values`, 02 §B3–§B4)

Six `stage='raw'` keys per occupant per official slot (`site_uptime_30d, tls_ok, site_age, site_freshness, docs_present, product_status`, all in 03's `METRIC_KEYS`) with 03 §3.6's columns: `occupant_key, metric_key, as_of` (INTEGER, `D` = 00:00Z), `spec_version='ts-1.0', stage='raw', value, value_json, observed_at, source='web', run_id, backfilled` — values and statuses as in §2.8/§2.9. `value_json.status ∈ {ok, absent, failed, not_applicable}` with a `reason` on every non-`ok` row; `status` is 03's generated column and is never written. `value` is NULL on `failed`/`not_applicable` rows (03's CHECKs), so coverage stays measurable. There is no `is_provisional` column (07 writes only official rows).

### 3.4 Table written: `collector_runs`

Written by 01's wrapper; 07 returns `JobOutcome(rows, cost_usd=0.0, note)`: `rows` = checks written (`web_check`), metric rows (`web_metrics`), captures requested (`web_spn2`); `note` carries `network_suspect` / `internal_errors=n` when set (§7).

### 3.5 Events written (through 03's `emit_event`; catalog 03 §3.9, 02 §D1)

Every emission is `emit_event(db, event_type=…, occupant_key, netuid, severity=<03's>, event_at, observed_at, payload, dedupe_key, source='web', run_id)`. A dedupe hit returns `None` (03's `ux_events_dedupe(event_type, dedupe_key)`), so there is no separate SELECT. Times are INTEGER unix s (B1); `day`/`check_day`/`streak_start_day` = `floor(ts/86400)` of the event / the check / the first down check; `last_up_day` = a day-start unix ts.

| event_type | severity | alert (03 §3.9, 02 §D4) | emitted by | trigger | payload (03's required keys; extras allowed) | dedupe_key (03) |
|---|---|---|---|---|---|---|
| `site_down_7d` | critical | **digest-only** detection event; the page is 12's `red_flag_raised(flag='site_down_7d')` | `web_metrics` | the current primary website has ≥ `WEB_DOWN_STREAK_DAYS` (7) consecutive checks with outcome `down` (indeterminate days and days without a row neither break nor extend the streak); once per streak; `event_at` = the 7th down check | `url, streak_days, last_status_class, last_http_status, secondary_up` | `occ\|url\|streak_start_day` |
| `site_recovered` | info | digest-only; 12 then clears the flag (`red_flag_cleared`) | `web_metrics` | an open streak (the latest `site_down_7d`/`site_recovered` of the occupant for that URL is `site_down_7d`) ends: the URL is `up` again (`reason='up'`, `event_at` = that check), or it stops being the occupant's primary website (`reason='target_changed'` / `'target_retired'`, `event_at = T_eval`). Without the second case 12's latest-event rule (12 §2.8) would keep the flag raised forever | `url, down_days` (+ `reason`) | `occ\|url\|day` |
| `tls_expiring_14d` | warning | digest | `web_check` | primary website row with `tls_days_left ≤ WEB_TLS_WARN_DAYS` (incl. ≤ 0 = expired); once per certificate | `url, tls_days_left, tls_not_after, tls_issuer, expired` | `occ\|url\|tls_not_after` |
| `site_parked` | warning | yes (immediate) | `web_check` | transition into `parked` (previous row not parked) | `url, final_url, parked_signals` | `occ\|url\|check_day` |
| `site_hijack_suspect` | critical | yes (immediate; human review, never auto-cleared) | `web_check` | transition into `hijacked_suspect` (previous row of the URL not `hijacked_suspect`) | `url, final_url, hijack_signals, change_ratio` | `occ\|url\|check_day` |
| `site_content_reset` | warning | digest | `web_check` | `change_ratio < WEB_CONTENT_RESET_RATIO` (0.2) ∧ previous `content_len ≥ 300` ∧ current outcome `up` | `url, prev_hash, new_hash, prev_len, new_len, change_ratio` | `occ\|url\|new_hash` |
| `site_cross_domain_redirect` | info | digest | `web_check` | first row with `cross_domain=1` to a given final registrable domain | `url, final_url, final_domain` | `occ\|url\|final_domain` |
| `docs_gone` | info | digest | `web_metrics` | a docs URL that was `up` at least once is definitively `down` for ≥ `WEB_DOCS_GONE_STREAK_DAYS` (2) consecutive checks | `url, last_up_day, streak_days` | `occ\|url\|streak_start_day` |

Routing is 14's `alert_dispatch` over 03's alert column (02 §D3/§D4); this collector never sends messages itself. The streak re-emission "every 30 d" of the earlier draft is withdrawn: 03's key allows one event per streak, and 12's flag is state-based on the latest event. **Per-occupant keys (resolved 2026-09-24).** In every dedupe key, `occ` is the event's `occupant_key`, and all eight keys start with it (03 §3.9 is adopting the same `occ|…` patterns). `ux_events_dedupe(event_type, dedupe_key)` is global, so a key that began with `url` would give only the first of several occupants sharing one URL (§2.2 step 4, e.g. one studio's site for several subnets) each event, and 12's per-occupant `site_down_7d` flag would miss the others. With the `occ|` prefix, each sharing occupant gets its own event.

---

## 4. Absence-vs-failure table

| Metric | `0.0` — verifiable absence (`absent`, C5 `absence_reason`) | `None` — `failed` (unobserved) or `not_applicable` (undefined) | Normal value (`ok`) |
|---|---|---|---|
| `site_uptime_30d` | No website target in any source, or every candidate is an excluded host (social/repo/directory) — `no_website` | `failed`: < 3 definitive (up/down) check days in the window (e.g. all `bot_challenge`/indeterminate); no primary check in the last 2 days | `up/(up+down)` — `dns_fail` (NXDOMAIN), `tls_fail`, 4xx/5xx (not 429), `parked`, `hijacked_suspect`, `too_many_redirects` are **down**; `timeout`/`conn_error` are down only after a non-up previous check; `internal:`/`network_suspect` rows never count |
| `tls_ok` | — (expired or ≤ 14 d left, hostname mismatch, `tls_fail`, no TLS on 443 while http answers are `ok` observations with value 0.0, 12 row 22) | `not_applicable`: no website. `failed`: handshake never evaluated (latest row `timeout`/`conn_error`/`dns_fail`/challenge without cert), no recent check | 1.0 when the GET verified the chain, hostname matches, > 14 d left |
| `site_age` | — (never 0.0 by absence; a young site is a low value, not zero) | `not_applicable`: no website. `failed`: Wayback never answered (429/503/timeouts on every attempt); no capture and no `up` observation yet | days since first capture; `self_observed` floor when Wayback has no capture |
| `site_freshness` | — | `not_applicable`: no website, SPA shell < 300 chars, robots HEAD-only. `failed`: no `up` row in the last 2 check days | days since last material change (`Δ ≥ 3 %` of word tokens) or since first stored text (`baseline=true`) |
| `docs_present` | No docs target (identity, manual or auto-discovered) **while** the latest primary check was `up` with a parsed landing page (`extractor ≠ none`) — `no_docs_link`. (Every docs URL definitively down ≥ 2 consecutive checks is an `ok` 0.0, `docs_down`.) | `not_applicable`: no website and no docs target. `failed`: website down/indeterminate with no docs target (we could not look), docs `timeout`/`conn_error`/`bot_challenge`, docs target not yet checked | 1.0 |
| `product_status` | — (manual `none` is an `ok` entry with value 0.0) | `failed`: no `manual_notes` row (`unset`), latest row cleared (`cleared`), invalid value | mapped enum |
| Transient-timeout rule (all) | — | A single `timeout` day writes a row but yields no metric change for that day (`site_uptime_30d` recomputed without it). A `failed` row is skipped by 12's reader in favour of the previous fresh row inside the 2-d window (02 §B5); this spec never forward-fills | — |

Rule of thumb applied throughout: **the collector decides**; 12 maps `absent → 0.0` and `failed`/`not_applicable` → `None` (master §7.2, 12 §2.3).

---

## 5. Configuration

All keys live in 01's `Settings` (pydantic-settings, `extra="ignore"`; 01 lists them under the 07 group and 07's defaults win, 02 §F1), read dynamically at each run. Lists are JSON arrays in `.env`. Retired: `WEB_KILL_SENTINEL_PATH`/`WEB_KILLED` (02 §B12: the sentinel list is closed), `WEB_ALERT_AFTER_FAILURES` (01's `JOB_FAIL_ALERT_THRESHOLD` + `collector_failing`), `WEB_METRICS_CRON_UTC`, `WAYBACK_RATE_PER_S`, and 01's placeholders `WEB_INTERVAL_MIN` and `WEB_SPN_ENABLED` (→ `WEB_SPN2_ENABLED`).

| Key | Type | Default | Validator / semantics |
|---|---|---|---|
| `WEB_ENABLED` | bool | `true` | enabled flag of `web_check` and `web_metrics`; precondition of `web_spn2` |
| `WEB_TICK_MINUTES` | int | `30` | 5–360; `interval_flag` of `web_check` |
| `WEB_RATE_PER_SEC` | float | `1.0` | provider `web` bucket (01 limiter table), all sites combined; burst `WEB_RATE_BURST` |
| `WEB_RATE_BURST` | int | `2` | |
| `WEB_PER_URL_MIN_INTERVAL_H` | float | `23.5` | ≥ 12; politeness: one request per URL per day |
| `WEB_SAME_HOST_GAP_S` | float | `10` | ≥ 1; minimum spacing between two checks of the same registrable host (not between the hops of one check) |
| `WEB_NETWORK_PROBE_HOSTS` | list[str] | `["archive.org","github.com"]` | preflight: DNS + TCP connect to 443, no HTTP request (§7) |
| `WEB_LATEST_MAX_AGE_DAYS` | int | `2` | 1–7; "latest row" horizon of §2.8 |
| `WEB_CONCURRENCY` | int | `4` | 1–16 parallel URLs |
| `WEB_TIMEOUT_S` | float | `10` | read timeout and TLS/DNS ceilings; 3–30 |
| `WEB_CONNECT_TIMEOUT_S` | float | `5` | 1–15 |
| `WEB_DNS_TIMEOUT_S` | float | `5` | 1–15 |
| `WEB_RETRY_DELAY_S` | int | `600` | in-run retry after a first `timeout`/`conn_error`; 0 disables |
| `WEB_MAX_REDIRECTS` | int | `5` | 1–10 |
| `WEB_MAX_BODY_BYTES` | int | `2_000_000` | body cap, passed to 01's `Http.request(max_body_bytes=…)` (§11 Q15) |
| `WEB_USER_AGENT` | str | `Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36 TaoSentinel/1.0 (+https://github.com/<operator>/taosentinel; monitoring)` | must contain `TaoSentinel/` (robots token, honest identification) |
| `WEB_ROBOTS_ENABLED` | bool | `true` | disallow ⇒ HEAD-only (§2.3 step 1) |
| `WEB_ROBOTS_CACHE_DAYS` | int | `7` | |
| `WEB_EXCLUDED_HOSTS` | list[str] | `["x.com","twitter.com","discord.com","discord.gg","t.me","github.com","youtube.com","taostats.io","taomarketcap.com","subnetalpha.ai","tao.app","linktr.ee","linkin.bio"]` | first seven verbatim from old bot `quality.py:153-156` |
| `WEB_MAX_WEBSITES_PER_OCCUPANT` | int | `2` | |
| `WEB_PARKING_HOSTS`, `WEB_PARKED_STRONG_PHRASES`, `WEB_PARKED_WEAK_KEYWORDS`, `WEB_HIJACK_KEYWORDS`, `WEB_SHARED_PLATFORM_HOSTS` | list[str] | §2.5 / §2.6 defaults | lowercase; matched against lowercase text/hosts |
| `WEB_TINY_BODY_CHARS` | int | `300` | |
| `WEB_CONTENT_RESET_RATIO` | float | `0.2` | 0–1; below ⇒ `site_content_reset` |
| `WEB_FRESHNESS_MIN_DELTA` | float | `0.03` | 0–0.5; material-change threshold for `site_freshness` |
| `WEB_STORE_TEXT_MAX_CHARS` | int | `32_000` | |
| `WEB_TLS_WARN_DAYS` | int | `14` | master §7.3 `tls_ok` threshold; also the `tls_expiring_14d` trigger |
| `WEB_UPTIME_WINDOW_DAYS` | int | `30` | frozen with `ts-1.0` (changing it is a new spec_version) |
| `WEB_UPTIME_MIN_CHECKS` | int | `3` | |
| `WEB_DOWN_STREAK_DAYS` | int | `7` | master §7.5 |
| `WEB_DOCS_GONE_STREAK_DAYS` | int | `2` | |
| `WEB_DOCS_AUTODISCOVER_ENABLED` | bool | `true` | |
| `WEB_WAYBACK_ENABLED` | bool | `true` | |
| `WEB_WAYBACK_RECHECK_DAYS` | int | `30` | re-query when no capture known |
| `WAYBACK_RATE_PER_SEC` | float | `0.5` | provider `wayback` bucket (burst 1; 01's limiter-table value), shared by CDX, Availability and SPN2 and by any other spec using archive.org |
| `WEB_SPN2_ENABLED` | bool | `false` | requires `IA_ACCESS_KEY` and `IA_SECRET_KEY` (18-B7) else validator forces `false` + warning |
| `IA_ACCESS_KEY`, `IA_SECRET_KEY` | str | `""` | secrets; never logged |
| `WEB_SPN2_WEEKDAY` | int | `6` | 0 = Monday … 6 = Sunday |
| `WEB_SPN2_HOUR_UTC` | int | `7` | validator: not 3–5 (03:00–05:59Z holds retention, backup, the 04:30Z reboot window and `quant_weekly`, E3) |
| `WEB_SPN2_MIN_INTERVAL_S` | float | `10` | sleep between SPN2 captures inside `web_spn2` (on top of the `wayback` bucket) |
| `WEB_METRICS_CRON_HOURS` | str | `"0,6,12,18"` | must contain 0 |
| `WEB_METRICS_CRON_MINUTE` | int | `5` | validator: `< SCORING_CRON_MINUTE` (15), so the official row precedes the 00:15Z run |

**Pause semantics (02 §B12)**: `WEB_ENABLED=false` ⇒ the wrapper skips with `last_skip_reason='disabled'`. Pauses are `control_state` rows (API `POST /api/v1/control/pause/{target}`, Telegram, CLI) with target `web_check`/`web_metrics`/`web_spn2` or the group `web` (§2.7); `data/control/<target>.paused` is an emergency input the app only reads. `/health.collectors.<id>.paused` uses B12's vocabulary (`operator | pause_all | emergency_file`; `kill_switch` and `budget` never apply to these free jobs). Paused jobs are excluded from liveness (02 §E4). Resume = delete the `control_state` row (and any emergency file); the next tick processes everything that became due meanwhile (oldest first, still ≤ 1 request per URL per day).

---

## 6. Cadence, rate limits, cost

Jobs, triggers and timeouts: the single table in §2.7 (02 §E2).

| Provider id (02 §G1) | Rate | Daily volume (129 occupants) | Notes |
|---|---|---|---|
| `web` — subnet sites, robots.txt, TLS handshakes | `WEB_RATE_PER_SEC` 1 req/s, burst 2; per host ≥ 10 s between checks; per URL ≤ 1/day (+1 in-run retry) | ≈ 130 website + ≈ 60 docs fetches (+ ≈ 100 redirect hops), ≈ 190 TLS handshakes, ≈ 25 robots fetches (7-d cache) ≈ 500 tokens | ≈ 9–12 min of wall time (the bucket is the bottleneck), + ≥ 10 min when an in-run retry is pending; worst case bounded by the 1,500-s timeout, leftovers stay due for the next tick |
| `wayback` — CDX, Availability, SPN2 | `WAYBACK_RATE_PER_SEC` 0.5 req/s, burst 1; on 429/503 back off ≥ 1 h (`wayback_backoff_until`) | first run ≈ 200 requests (≈ 7 min); steady state ≤ ~1/day; SPN2 ≤ 130/week (Sun 07:00Z, ≈ 22 min) | community limit ~1 req/s (research `01-web-social-apis.md` §4); responses cached in `web_checks`; SPN2 per-account limits UNVERIFIED |
| Cost | — | **$0.00/month** | `collector_runs.cost_usd = 0`; request counts only in `provider_spend.requests` (01's `Http` counts HTTP; the run records TLS handshakes via `costs.record('web', 0.0, requests=n)`, G5) |

Limiter rules (02 §G1): one process-wide `ProviderLimiter` (01); no collector builds its own client or bucket. `Retry-After` on a site's 429/503 is honoured **per host** by 07 (that host is skipped for the rest of the run, ≤ 1 h), not as a bucket-wide penalty (§11 Q15); a 404 or a `bot_challenge` is never retried within a run. Bandwidth: 2 MB per page cap ⇒ ≤ 400 MB/day worst case, typically < 60 MB.

---

## 7. Failure modes and fail-safe behaviour

| Failure | Behaviour |
|---|---|
| Any exception inside a single URL's pipeline | caught per URL; row written with `status_class='conn_error'`, `error='internal:…'` (always indeterminate, §2.4); run continues; the URL is retried on the next day, not the next tick; `note` counts `internal_errors` |
| Whole run raises (DB locked, settings invalid) | 01's wrapper logs, increments `consecutive_failures`, sets `last_error`; `collector_failing` at `JOB_FAIL_ALERT_THRESHOLD`, `collector_recovered` on the next success (D1); never propagates into APScheduler |
| Laptop off for N days | ticks resume; all URLs are due; processed oldest-first over the next ticks; the next `web_metrics` run (≤ 6 h later, §2.7) writes today's slot. Past slots are **not** recomputed automatically (the CLI `--as-of` replay writes `backfilled=1` rows, §2.8); missing days are never forward-filled and read as `None` in 12 once the 2-d window passes — not as `absent` |
| archive.org down | Wayback fields stay NULL, `site_age` stays `failed` (or the `self_observed` fallback once Wayback has answered "no capture"); back off ≥ 1 h. No event (no catalogued type, D1): `/health` shows `wayback_last_ok` and the log warns once when it is > 7 d old |
| Laptop network down | **preflight** at run start: resolve `WEB_NETWORK_PROBE_HOSTS` and TCP-connect to 443 of the first (no HTTP request); both fail ⇒ the run ends at once with `note='network_suspect'`, writes no rows, and every target stays due. **Mid-run degradation:** rows are written in batches of 10; if ≥ 60 % of a batch (≥ 5 rows) are `timeout`/`conn_error`, the failing rows of that batch get `error='network_suspect'` (always indeterminate, §2.4), the in-run retry pass is skipped and `note='network_suspect'`. No down streak can advance from our own outage; 16's watchdog/dead-man covers the box itself |
| One host blocks us (403 challenge, 429) | `bot_challenge` rows, indeterminate forever (`site_uptime_30d` → `failed` once < 3 definitive days). 14's needs-review has no "persistently indeterminate" bucket yet (§11 Q17); the operator can read `/health.collectors.web_check.status_counts` and set a manual `product_status` or docs target |
| Extraction library failure (trafilatura raises) | fall through readability → plain; `extractor='none'` ⇒ `content_hash=NULL`, freshness `None`; never fails the row |
| Duplicate insert (same URL/day, e.g. overlapping manual run) | `INSERT OR IGNORE` on the UNIQUE key; counted as `skipped_duplicate` |

**Last-good semantics**: this spec never forward-fills. "Last good" is a *reader* concept: the fns read only rows ≤ 2 check days old (§2.8), and 12's 2-day web freshness window (02 §C6, master §7.7) tolerates a missed day by skipping a `failed` row in favour of the previous fresh one (02 §B5).

**`/health` fields**: 01's standard `collectors.<id>` block (`last_run, last_ok, last_error, consecutive_failures, last_rows, paused` with B12's vocabulary, …) for `web_check`, `web_metrics`, `web_spn2`; `WebCheckJob.status()` extends it with `urls_total, urls_due, urls_checked, status_counts{status_class: n}, network_suspect, internal_errors, wayback_last_ok, wayback_backoff_until`. Alerts: only 01's `collector_failing`/`collector_recovered` (D1); `network_suspect` and Wayback staleness are `/health` + log only (no catalogued event type).

---

## 8. Implementation plan

Nothing is ported from the old bot except the host-exclusion tuple and the `_is_url`/`_host` helpers (`E:\SN_Trader\SubnetTrader\app\strategy\quality.py:95-105`, copied into `normalise_url`). Everything else is new code. Dependencies: `trafilatura>=2.0,<3.0` (already in 01's pin), plus `readability-lxml`, `lxml[html_clean]` (lxml ≥ 5.2 moved `lxml.html.clean`, which readability-lxml imports, into the separate `lxml_html_clean` package), `cryptography>=42` (`not_valid_after_utc`), `tldextract` (offline PSL) — added to 01's `pyproject` under this spec — and the already-present `httpx`, `aiosqlite`, `APScheduler`. Tests use 01's `respx`.

### 8.1 Files

```
app/collectors/web.py            WebCheckJob / WebMetricsJob / WebSpn2Job (01 CollectorJob) + orchestration + CLI
app/collectors/web_fetch.py      DNS / TLS / GET-chain (I/O)
app/collectors/web_classify.py   status, parked, hijack, bot-challenge, SPA, registrable_domain (pure)
app/collectors/web_content.py    extraction, normalisation, hash, diff, docs discovery (pure)
app/collectors/web_robots.py     robots cache (I/O + pure parse)
app/collectors/web_wayback.py    Availability, CDX, SPN2 clients (I/O)
app/features/web_metrics.py      six C5 producer fns, loader, pure core, streak events
tests/collectors/test_web_fetch.py test_web_classify.py test_web_content.py test_web_wayback.py
tests/features/test_web_metrics.py
tests/fixtures/web/*.html        parked_sedo.html parked_godaddy.html spa_shell.html landing_ok.html
                                 landing_docs_link.html cf_challenge.html hijacked_casino.html tiny_links.html
```

### 8.2 Signatures

```python
# web_fetch.py
@dataclass(frozen=True)
class Hop: url: str; status: int; location: str | None
@dataclass(frozen=True)
class TlsInfo:
    days_left: int | None; not_after: int | None; issuer: str | None      # not_after: unix s (B1)
    hostname_match: bool | None; error: str | None
@dataclass(frozen=True)
class FetchResult:
    url: str; final_url: str | None; final_host: str | None; status: int | None
    chain: tuple[Hop, ...]; hops: int; latency_ms: int | None
    headers: dict[str, str]; content_type: str | None; body: str | None; body_bytes: int
    exc_kind: Literal["dns","timeout","tls","conn","too_many_redirects","protocol","internal"] | None
    error: str | None; attempts: int

async def resolve_dns(host: str, port: int, timeout_s: float) -> str            # raises DnsError(kind='nxdomain'|'temporary') | TimeoutError
async def inspect_tls(limiter: ProviderLimiter, host: str, port: int, timeout_s: float) -> TlsInfo   # takes one 'web' token; never raises
async def fetch_chain(http: Http, url: str, *, method: str, max_redirects: int, max_body: int, ua: str,
                      timeout: httpx.Timeout) -> FetchResult                       # provider 'web'; never raises
async def network_preflight(hosts: list[str], timeout_s: float) -> bool           # DNS + TCP 443, no HTTP
def hostname_matches(host: str, san_dns_names: list[str]) -> bool

# web_classify.py
def registrable_domain(host: str) -> str
def is_excluded_host(host: str, excluded: list[str]) -> bool
def is_bot_challenge(status: int | None, headers: dict[str, str], html: str | None) -> bool
def detect_spa(html: str, plain_len: int) -> bool
def parked_signals(html: str | None, norm_text: str, final_host: str | None, links: list[str],
                   is_spa: bool, cfg: WebHeuristicsConfig) -> list[str]
def hijack_signals(norm_text: str, cross_domain: bool, prev: PrevState | None, change_ratio: float | None,
                   resolved_ip: str | None, tls_issuer: str | None, cfg: WebHeuristicsConfig) -> list[str]
def classify(fr: FetchResult, tls: TlsInfo | None, parked: list[str], hijack: list[str],
             bot_challenge: bool, max_redirects: int) -> str                        # status_class

# web_content.py
def extract_text(html: str, url: str) -> tuple[str, str]                            # (text, extractor)
def normalise(text: str) -> str
def content_hash(norm: str) -> str
def change_ratio(prev_norm: str, cur_norm: str, max_tokens: int = 5000) -> float
def outbound_links(html: str, base_url: str) -> list[str]
def find_docs_candidate(html: str, base_url: str, excluded_hosts: list[str]) -> str | None

# web_robots.py
class RobotsCache:
    def __init__(self, http: Http, ttl_days: int, ua_token: str = "TaoSentinel"): ...
    async def allowed(self, url: str) -> tuple[bool, float | None]                  # (allowed, crawl_delay_s); never raises

# web_wayback.py
class WaybackClient:
    def __init__(self, http: Http): ...                                          # provider 'wayback'
    async def first_capture(self, target: str) -> tuple[int | None, str]         # (unix_ts|None, 'found'|'none'|'error'|'backoff'); §2.6 steps 1-2
    async def spn2_save(self, url: str, access_key: str, secret_key: str) -> str | None            # job_id

# web.py
@dataclass
class WebCheckRow: ...                                                              # mirrors 03's web_checks columns (§3.2)
class WebCheckJob(CollectorJob):   id = "web_check";   async def collect(self, run: RunContext) -> JobOutcome
class WebMetricsJob(CollectorJob): id = "web_metrics"; async def collect(self, run: RunContext) -> JobOutcome
class WebSpn2Job(CollectorJob):    id = "web_spn2";    async def collect(self, run: RunContext) -> JobOutcome
async def check_url(run: RunContext, target: WatchTarget, prev: PrevState | None) -> WebCheckRow
async def run_web_metrics(run: RunContext, t_eval: int, *, backfilled: bool = False) -> JobOutcome
async def main(argv: list[str]) -> int          # python -m app.collectors.web --once [--netuid N] [--url U] [--metrics] [--as-of YYYY-MM-DD] [--force]

# features/web_metrics.py  (MetricResult = 12's app/features/types.py)
async def compute_site_uptime_30d(db, occupant_key: str, as_of: datetime) -> MetricResult   # 02 §C5
async def compute_tls_ok(db, occupant_key: str, as_of: datetime) -> MetricResult
async def compute_site_age(db, occupant_key: str, as_of: datetime) -> MetricResult
async def compute_site_freshness(db, occupant_key: str, as_of: datetime) -> MetricResult
async def compute_docs_present(db, occupant_key: str, as_of: datetime) -> MetricResult
async def compute_product_status(db, occupant_key: str, as_of: datetime) -> MetricResult
async def load_web_inputs(db, occupant_key: str, t_eval: int) -> WebInputs      # §8.4 queries; cached per (occupant_key, t_eval)
def derive_web_metrics(inp: WebInputs, cfg: WebMetricsConfig) -> dict[str, MetricResult]   # pure
def outcome(row: Mapping, prev_outcome: str | None) -> Literal["up","down","indeterminate"]  # reads status_class + error
def streak_events(inp: WebInputs, t_eval: int, cfg: WebMetricsConfig) -> list[EventSpec]     # site_down_7d, site_recovered, docs_gone
def to_raw_row(key: str, res: MetricResult, *, as_of: int, observed_at: int, run_id: str,
               backfilled: int) -> dict        # 12 §2.3 status mapping; source='web', spec_version='ts-1.0', stage='raw'
```

`RunContext` is 01's (`run_id, started_at, db, http, settings, log`); the limiter is `self.rt.limiter`. `PrevState` = the previous row's `status_class, outcome, error, content_hash, content_text, resolved_ip, tls_issuer, cross_domain, parked, wayback_first_capture, wayback_checked_at`, loaded with one query per target (`ORDER BY event_at DESC LIMIT 1`, plus the latest `content_text IS NOT NULL` row). `EventSpec` holds the `emit_event` keyword arguments.

### 8.3 Step order

1. `web_content.py` + `web_classify.py` (pure) with their tests (fixtures first).
2. `web_fetch.py` against `respx` routes (01's harness); TLS via monkeypatched `ssl.get_server_certificate` returning in-test self-signed PEMs (`cryptography` generates them with chosen SAN and `not_valid_after`).
3. `web_robots.py`, `web_wayback.py` with `respx`.
4. `web.py` orchestration: `SELECT` targets → dedupe → due filter → `asyncio.Semaphore(WEB_CONCURRENCY)` + per-host `asyncio.Lock` with gap → `check_url` → batched `INSERT OR IGNORE` (network gate, §7) → per-check events via `emit_event`; `JobOutcome` → `collector_runs`.
5. `features/web_metrics.py` (C5 fns, pure core) + `run_web_metrics` writer via `put_metric_values`; streak events via `emit_event`.
6. Add the three rows of §2.7 to 01's `JOB_TABLE`; extend `status()` with the `/health` fields; add the CLI.
7. `web_spn2` last (optional path).

### 8.4 SQL used by `load_web_inputs` (per occupant, parameterised, PIT-bounded; `:d` = `floor_day(t_eval)`)

```sql
-- website rows (U and P), incl. today's partial day for streak events; metrics use check_day_ts < :d
SELECT check_day_ts, event_at, observed_at, url, is_primary, status_class, error, http_status, parked,
       tls_ok, tls_days_left, tls_not_after, tls_issuer, tls_hostname_match, content_hash, content_len,
       change_ratio, is_spa, robots_allowed, extractor, wayback_first_capture, wayback_checked_at
FROM web_checks
WHERE occupant_key = :ok AND kind = 'website'
  AND check_day_ts >= :d - 45*86400 AND check_day_ts <= :d AND observed_at <= :t_eval
ORDER BY check_day_ts, is_primary DESC;
-- docs rows: same columns, kind = 'docs', check_day_ts >= :d - 10*86400
-- targets: SELECT platform, handle, source, priority, is_resolved, first_seen_at FROM watch_channels
--   WHERE occupant_key = :ok AND platform IN ('website','docs') AND first_seen_at <= :t_eval
--     AND (retired_at IS NULL OR retired_at > :t_eval)
-- first up (site_age fallback): SELECT MIN(event_at) FROM web_checks WHERE occupant_key = :ok AND url = :primary
--   AND kind = 'website' AND status_class IN ('up_2xx','redirect_ok') AND observed_at <= :t_eval
-- open streaks: SELECT id, event_type, event_at, payload FROM events WHERE occupant_key = :ok
--   AND event_type IN ('site_down_7d','site_recovered') AND observed_at <= :t_eval ORDER BY event_at DESC, id DESC
-- product: §2.9
```
The 45-day window (not 30) is needed so `outcome()` can see the day before the window start and so the freshness "last material change" can be found; if no change is found in 45 d the query is widened to the latest row with `change_ratio <= 0.97` or the earliest `content_text IS NOT NULL` row.

---

## 9. Tests

All network I/O goes through 01's `Http` and is mocked with `respx` (01's harness), routed on host + path; `ssl.get_server_certificate` and `loop.getaddrinfo` are monkeypatched; the clock is frozen. DB tests apply 03's schema (`Database.apply_schema()`) to a temp database — no local DDL — and write through `put_metric_values`/`emit_event`, so 03's CHECKs (status vocabulary, severities, `source='web'`) are exercised.

**Unit — `test_web_classify.py`** (one test per status class):
- `dns_fail`: `getaddrinfo` raises `gaierror(EAI_NONAME)` → `status_class='dns_fail'`, outcome `down`; `gaierror(EAI_AGAIN)` → `timeout`.
- `timeout`: transport raises `httpx.ReadTimeout` twice → `timeout`, `attempts=2`; outcome `indeterminate` when previous day `up`, `down` when previous day `timeout`.
- `tls_fail`: `httpx.ConnectError` with `__cause__ = ssl.SSLCertVerificationError` → `tls_fail`, `tls_ok=0`.
- `conn_error`: `httpx.ConnectError("Connection refused")`.
- `too_many_redirects`: handler that always 302s to itself+`?n` → 6 hops → class; chain length 5 recorded.
- `bot_challenge`: fixture `cf_challenge.html` with 403 + `cf-mitigated: challenge` → class; outcome indeterminate.
- `parked`: `parked_sedo.html` (strong phrase + parking script), `parked_godaddy.html` (final host `parked-content.godaddy.com`), `tiny_links.html` (tiny body + 2 external links + weak keyword) → all `parked`; `spa_shell.html` (tiny body, `<div id="root">`) → **not** parked, `is_spa=1`.
- `hijacked_suspect`: previous row `up_2xx` same domain; now 301 → `casino-win.example` with `hijacked_casino.html` → score 4 → class; `site_hijack_suspect` event.
- `up_2xx` / `redirect_ok`: `landing_ok.html`; http→https 301 then 200 → `redirect_ok`, `cross_domain=0`; 301 to `newbrand.example` with a normal page → `redirect_ok`, `cross_domain=1`, `site_cross_domain_redirect` event, no hijack.
- `client_error` (404), `server_error` (502), 3xx without `Location` → `server_error`; plain 429 → `bot_challenge`; robots-HEAD answered 405 → `bot_challenge`.
- `internal:` rows: an exception in extraction → `conn_error` with `error='internal:…'`, outcome `indeterminate` even after a non-up previous day.

**Unit — `test_web_content.py`**: normalisation removes each pattern class (parametrised: ISO date, "September 22, 2026", "3 hours ago", "© 2026" — the year must go too, 9-digit number, 40-hex nonce, UUID); same page fetched twice with different nonces/dates hashes identically; `change_ratio` of identical = 1.0, wholesale replacement < 0.2, one-paragraph edit ≈ 0.9; SPA shell hashes title + meta description; `find_docs_candidate` picks `/docs` over a `docs.bittensor.com` link and resolves relative hrefs; extraction fallback chain (trafilatura → readability → plain) exercised by monkeypatching `trafilatura.extract` to return `None`.

**Unit — `test_web_fetch.py`**: `hostname_matches` (exact, wildcard one label, no multi-label wildcard, no partial); `inspect_tls` with generated certs: 10 d left → `days_left=10`, 400 d → 400, expired → negative; issuer extraction (`O` preferred over `CN`); body cap truncates at 2 MB without error; non-HTML content type sets `extractor='raw'`; no `Accept-Encoding` header is set by 07; `http://` URL with 443 refused → `tls_ok=0`, reason `no_tls`; `inspect_tls` consumes one `web` limiter token; robots `Disallow` under `User-agent: TaoSentinel` is honoured (bare-token `can_fetch`) and ignored for other agents.

**Unit — `test_web_wayback.py`**: CDX row → unix timestamp; CDX empty with filter → retried without filter, then with `www.` → `(None,'none')`; CDX 503 → Availability called **with `timestamp=19960101`** and its `closest.timestamp` used; Availability empty after a CDX error → `(None,'error')` (not "no capture"); 429 → `(None,'backoff')` and `wayback_backoff_until` ≥ now + 3600; shared-platform host → the declared URL (host + path) is the target; every call uses provider `wayback`; SPN2 posts form `url=…` with `Authorization: LOW k:s` and returns `job_id`.

**Unit — `test_web_metrics.py`**: fixture generator `rows(days: str)` where the string encodes a day per char (`U` up, `D` down, `T` timeout, `B` bot, `.` no check):
- `"UUUUUUUUUUUUUUUUUUUUUUUUUUUDDD"` → `site_uptime_30d = 27/30`.
- `"UUUUUUUUUUUUUUUUUUUUUUUUUUUUTU"` → single timeout excluded → 29/29 = 1.0; `"…UUTTU"` → second T counts as down → 28/29.
- `"UU"` → `failed` `insufficient_checks`; no website target → `absent` 0.0 `no_website` (and `tls_ok`/`site_age`/`site_freshness` `not_applicable`); all `B` → `failed`; last row 5 days old (web_check paused) → every latest-state key `failed` `no_recent_check`.
- `DDDDDDD` at the tail → `site_down_7d` emitted once (`streak_days=7`, severity `critical`, `source='web'`, dedupe `occ|url|streak_start_day`); two occupants sharing that URL → one `site_down_7d` each; `DDDDDDDU` → `site_recovered` (`reason='up'`); `DDDTDDDD` → streak continues across the T and reaches 7 on the 8th char; open streak + primary URL replaced → `site_recovered(reason='target_changed')` for the old URL; running `web_metrics` twice → no duplicate events (`emit_event` returns None).
- `tls_ok`: latest row `tls_days_left=10` → `ok` 0.0 `expiring`; `tls_fail` → `ok` 0.0; latest `timeout` → `failed` `tls_not_evaluated`.
- `site_age`: first capture 400 d ago → 400 (`source='wayback'`); no capture, first up 20 d ago, `wayback_checked_at` set → 20 with `source='self_observed'`; Wayback never answered → `failed`.
- `site_freshness`: last material change 120 d ago → 120; only baseline text 5 d ago → 5 with `baseline=true`; SPA tiny → `not_applicable`.
- `docs_present`: docs `up` → 1.0; no docs target + website up + `extractor='trafilatura'` → `absent` 0.0 `no_docs_link`; docs `D D` → `ok` 0.0 + `docs_gone`; docs `T` → `failed`; website `D` and no docs → `failed`.
- `product_status` (`manual_notes`): latest `live` → 1.0 with `value_json.product_status='live'` and `value_json.status='ok'`; `none` → `ok` 0.0; latest `value NULL` → `failed` `cleared`; no row → `failed` `unset`; `created_at > T_eval` → ignored (PIT).
- Writer: rows land with `as_of = floor_day(T_eval)`, `stage='raw'`; a second run the same day leaves `ok`/`absent`/`not_applicable` rows untouched and upgrades a `failed` one (B4); a `--as-of` replay writes `backfilled=1`.

**Integration / smoke — `test_web_job.py`**: seed `watch_channels` with 3 occupants (one excluded-host-only, one with website+docs, two occupants sharing one URL) and run `WebCheckJob.collect` against `respx` → exactly one GET per distinct URL, 4 `web_checks` rows (one per occupant/kind), `collector_runs` row `ok=1`, `/health` counts; run twice in the same day → 0 new rows (`skipped_duplicate`); freeze clock +24 h → all due again. Robots test: `robots.txt` with `Disallow: /` for `TaoSentinel` → HEAD only, `robots_allowed=0`, `content_hash IS NULL`. Wayback: first run performs CDX once per primary website URL (none for docs), second run performs none (cached), `wayback_first_capture` copied forward. Network gate: preflight probe failing → 0 rows, all targets still due; a batch with ≥ 60 % timeouts → those rows carry `error='network_suspect'`. Autodiscovered docs row: inserted once with 03's columns, `last_seen_at` refreshed on the next up check. Pause: a `control_state` row for target `web` (or `data/control/web_check.paused`) → skip, no `collector_runs` row.

Must be mocked: every host on the internet (`respx` routes keyed on host + path), `ssl.get_server_certificate`, `loop.getaddrinfo`, the clock, the `ProviderLimiter` (a no-op limiter with a per-provider call counter to assert `web`/`wayback` usage), Telegram (spec 01 test double).

---

## 10. Verify checklist

Run on the laptop after Phase 5 is deployed (Phases 0–1 required: registry populated, `watch_channels` has website rows).

| # | Command | Expected |
|---|---|---|
| 1 | `pytest tests/collectors/test_web_*.py tests/features/test_web_metrics.py -q` | all green, ≥ 60 tests, < 30 s |
| 2 | `python -m app.collectors.web --once --netuid 64 --force` | prints one JSON row per URL with `status_class ∈ {up_2xx, redirect_ok}`, non-null `tls_days_left`, `content_hash`, `latency_ms`; exit 0 |
| 3 | `python -m app.collectors.web --once --force` (all due) | finishes in < 15 min; log line `web_check_done` with `urls_checked ≥ 100`, `status_counts` |
| 4 | `sqlite3 data/taosentinel.db "select status_class, count(*) from web_checks where check_day_ts=unixepoch(date('now')) group by 1"` | ≥ 100 rows; `up_2xx`+`redirect_ok` ≥ 70 % (Sept-2026 ecosystem: expect a handful of `dns_fail`/`parked`; placeholders like `www.deprecated.com` never arrive — 04 drops them as junk) |
| 5 | `sqlite3 data/taosentinel.db "select count(*) from web_checks where wayback_first_capture is not null and kind='website' and is_primary=1 and check_day_ts=unixepoch(date('now'))"` | ≥ 60 % of primary website rows after the first full run (rest re-queried in 30 d) |
| 6 | `sqlite3 data/taosentinel.db "select count(*) from watch_channels where source='web_autodiscover'"` | > 0 (docs candidates found on landing pages) |
| 7 | After the 00:05Z `web_metrics` run: `sqlite3 data/taosentinel.db "select metric_key, status, count(*) from metric_values where source='web' and stage='raw' and as_of=unixepoch(date('now')) group by 1,2"` | 6 metric keys × active/pre-launch occupants; `site_uptime_30d` is `failed` (`insufficient_checks`) until the 3rd complete check day, then `ok`; occupants with no website show `site_uptime_30d` `absent` (value 0.0) and `not_applicable` for `tls_ok`/`site_age`/`site_freshness`; `product_status` `failed` (`unset`) unless set; no `status` outside `ok/absent/failed/not_applicable` |
| 8 | After 3 complete check days: same query | `site_uptime_30d` `ok` for every occupant with a reachable website |
| 9 | `curl -s localhost:8090/health | jq '.collectors.web_check'` | `last_run` within 30 min, `consecutive_failures: 0`, `paused: null`, `urls_due: 0` after a run |
| 10 | `touch data/control/web_check.paused; sleep 1900; curl -s localhost:8090/health | jq '.collectors.web_check.paused'` → `"emergency_file"`; `rm data/control/web_check.paused`; then `POST /api/v1/control/pause/web` → all three jobs `"operator"`, `…/resume/web` clears | pause/resume works without restart (02 §B12) |
| 11 | `sqlite3 data/taosentinel.db "select event_type, count(*) from events where event_type like 'site_%' or event_type='docs_gone' or event_type='tls_expiring_14d' group by 1"` | rows exist for at least `site_cross_domain_redirect`/`tls_expiring_14d`/`site_parked` where applicable; no `site_down_7d` before day 7 |
| 12 | `sqlite3 data/taosentinel.db "select url, tls_days_left from web_checks where check_day_ts=unixepoch(date('now')) and tls_days_left <= 14"` | lists any expiring cert (the dking.bot-style red flag) with a matching `tls_expiring_14d` event |
| 13 | `sqlite3 data/taosentinel.db "select count(*) from web_checks where check_day_ts=unixepoch(date('now')) and content_text is not null"` on day 2 | ≪ row count (text stored only on change) |
| 14 | `grep -c '"event":"wayback_backoff"' data/logs/$(date -u +%F).jsonl` | 0 in steady state |

---

## 11. Open questions / operator decisions

1. **`watch_channels.priority`** — *settled by 03 §3.2*: manual `force` 5 outranks chain 10; manual `fallback`/legacy 40; 07 prefers 04's `is_resolved` winner (§2.2 step 3).
2. **Added status classes** `conn_error`, `too_many_redirects`, `bot_challenge` — *settled*: 03's `web_checks` CHECK lists all 12 classes.
3. **`site_age` fallback to self-observed age** when Wayback has no capture (§2.8): master defines site_age strictly as "days since first Wayback capture". The fallback avoids permanent `None` for sites Wayback never crawls (small new domains) and is flagged in `value_json.source`. Operator/master decision: keep or drop. Related limitation: a re-registered domain reports its previous owner's first capture (the metric is "domain age", not "team's site age").
4. **`site_freshness` material-change threshold** (`WEB_FRESHNESS_MIN_DELTA = 0.03`): master says "content-hash change"; a 3 % word-token threshold de-noises surviving volatile fragments. Keep, or use strict hash inequality?
5. **`content_text` stored inside `web_checks`** — *settled by 03 §3.4* (kept in `web_checks`, only on change).
6. **Docs auto-discovery writes to `watch_channels`** (`source='web_autodiscover'`, unverified). Alternative: keep candidates only in `web_checks.docs_candidate_url` and require operator confirmation (`/docs set <netuid> <url>`). Auto-adding gives `docs_present` coverage immediately; false positives are bounded by the "same team's landing page links it" premise.
7. **`docs_present = 0.0` when no docs link is found on the landing page**: this treats "the team links no docs from its front page" as verifiable absence. Some projects keep docs only in the GitHub README. Alternative: `None` unless the operator confirms absence. Current choice follows master's "absence is a signal".
8. **Extended `WEB_EXCLUDED_HOSTS`** (directory hosts `taostats.io, taomarketcap.com, subnetalpha.ai, tao.app, linktr.ee, linkin.bio`) beyond the old bot's list. Linktree pages are a grey zone (some teams use them as the only landing) — currently excluded ⇒ "no website".
9. **UA honesty vs. browser-likeness**: the default UA is browser-like with an appended `TaoSentinel/1.0 (+repo url)` token. Some WAFs treat any non-standard token as a bot. If the `bot_challenge` share exceeds ~10 % after a week, consider a pure browser UA (less honest) or a Playwright sidecar (Phase 11+).
10. **SPN2 exact form parameters and limits** are UNVERIFIED in the research corpus (it gives only `POST /save/<url>`, IA S3 keys and `if_not_archived_within`). The optional job is default-off; verify the form body, the `LOW` auth header, `skip_first_archive` and per-account limits against the SPN2 public API doc before enabling.
11. **`product_status` decay**: no expiry in v1. Should a `live` override whose `evidence_url` goes dead (this collector could check it monthly) drop to `None` and surface for review?
12. **Retention of `content_text`** — *settled by 03 §2.7* (`web_text`, `WEB_TEXT_RETENTION_DAYS` 365, newest text per `(occupant_key, url, kind)` kept).
13. **Shared URL across occupants** yields identical P3 rows for each; a studio's landing page says little about an individual subnet's product. Consider a `shared_site` flag in `value_json` so the UI can show it (no scoring change in `ts-1.0`).
14. *(Closed 2026-09-24: the events dedupe keys now start with `occ|` (§3.5), matching 03 §3.9.)*
15. **NEW — 01's `Http` gaps for a multi-host provider (01 §2.12, 02 §G1).** (a) `Http.request` has no body cap/streaming: add `max_body_bytes` (stream, truncate, flag) — without it a large file at a subnet URL is downloaded whole; (b) `limiter.observe()` turns one site's `Retry-After` into a bucket-wide penalty that stalls every site: skip bucket-level penalties for provider `web` (07 applies them per host); (c) `ProviderError` must chain the transport exception (`raise … from exc`) so 07 can classify timeouts vs TLS vs refused; (d) 01's limiter table should read `WEB_RATE_PER_SEC`/`WEB_RATE_BURST` and `WAYBACK_RATE_PER_SEC` (07 keys).
16. *(Closed 2026-09-24: 16's `PAUSE_GROUPS` has the row `web` = {`web_check`, `web_metrics`, `web_spn2`}.)*
17. **NEW — 14 needs-review buckets.** 14 §3.10's `needs_review` has no bucket for unset `product_status` or persistently indeterminate (`bot_challenge`) websites, which 07's earlier draft assumed; 14 may add `product_unset` and `web_indeterminate`.
18. **NEW — `site_recovered` on target change (12 §2.8).** 12 raises `site_down_7d` from the latest `site_down_7d`/`site_recovered` event. 07 therefore also emits `site_recovered` (`payload.reason='target_changed'|'target_retired'`) when a URL with an open streak stops being the primary website; 14's digest wording should show the reason.

---

## 12. Sources

- `E:\TaoData\specs\02-cross-cutting-conventions.md` — A2, B1–B5, B7, B9, B12, C5, C6, D1–D4, E1–E4, F1, G1, G5, J1 (binding).
- `E:\TaoData\specs\03-storage-schema.md` — §2.5–§2.7 (`put_metric_values`, `metric_at`, `web_text` retention), §3.2 (`watch_channels`, `manual_notes`), §3.4 (`web_checks` DDL), §3.6 (`metric_values`, `events`), §3.8–§3.9 (catalogs), Appendix A.
- `E:\TaoData\specs\12-feature-store-and-scoring-engine.md` — §2.2–§2.4 (raw-row contract, `MetricResult`, registry rows 21–26), §2.8 (`site_down_7d` flag), §11 Q18.
- `E:\TaoData\specs\01-repo-skeleton-runtime.md` §2.6–§2.7, §2.12, §6; `04-subnet-registry-and-identity.md` §2.6, §2.9; `14-api-telegram-and-ui.md` §2.6, §3.10 (`/manual/product`, `/product set`); `16-ops-deploy-and-observability.md` §2.5, §2.11, §6.2.
- `E:\TaoData\specs\00-MASTER-SPEC.md` — §1.3, §4 (Wayback tier 0), §5, §6, §7.2–7.3 (P3), §7.5 (`site_down_7d`), §7.7, §8, §11, §12.
- `E:\TaoData\specs\INDEX.md`, `E:\TaoData\specs\18-operator-checklist.md` (B7 `IA_ACCESS_KEY`/`IA_SECRET_KEY`; D2 `/product set`).
- `E:\TaoData\research\01-web-social-apis.md` — §4 (liveness pipeline, TLS via `ssl.get_server_certificate` + `cryptography`, trafilatura/readability + difflib, Wayback Availability/CDX/SPN2 URLs and ~1 req/s limit, changedetection.io rejected), §1 legal notes (no personal-account scraping; vendor risk), Medium/Substack feeds (out of scope here, spec 09 appendix).
- `E:\TaoData\research\01-web-ecosystem-2026.md` — §2 (Score's dking.bot expired SSL) and §5 red-flag 12 (expired SSL, dead website as cheap hygiene checks).
- `E:\TaoData\research\01-fundamentals-scorer.md` — §5 `has_website` host-exclusion rule and the `www.deprecated.com` placeholder case; §8 slot-reuse caveat (occupant keying).
- `E:\TaoData\research\02-gapfill-discord-telegram-topology.md` — "Parked"/"deprecated" occupants list (expected `parked`/`dns_fail` cases for the verify checklist).
- `E:\SN_Trader\SubnetTrader\app\strategy\quality.py:95-105` (`_is_url`, `_host`) and `:149-159` (`has_website` exclusion tuple) — the only ported code.

## Changelog
- 2026-09-24 final pass: all eight event dedupe keys now use `occ|url|…` (occupant_key first), so occupants that share a URL each get their own events. §11 Q14 is closed.
- 2026-09-24 final pass: confirmed that no WEB-specific kill sentinel remains (B12's closed list). Pausing uses only `control_state` or 16's `web` PAUSE_GROUP, which now exists, so the Q16 request is met.
- 2026-09-23 reconciliation + review:
  - B3/C5: six `compute_<key>(db, occupant_key, as_of) -> MetricResult` fns; statuses `ok|absent|failed|not_applicable` with reasons (`no_website`, `no_docs_link`, …); product enum moved to `value_json.product_status`; source `manual_notes` (03), not `manual_overrides`.
  - B4/B5: rows via `put_metric_values`, `as_of` = official 00:00Z slot, first success wins; append-only/latest-`observed_at` rule withdrawn; "latest" limited to 2 check days so stale checks cannot pass 12's freshness window.
  - B1/B7: INTEGER times (`event_at`, `check_day_ts`, `tls_not_after`, `wayback_*`); local `web_checks` DDL replaced by a column contract on 03 §3.4; `watch_channels` columns per 03 (`platform`, `handle`, `retired_at`, `is_resolved`).
  - B2/D1/D2/D4: events through `emit_event`, `source='web'` (B9), 03's severities (`critical`, not `high`) and dedupe column; 30-d re-emission removed; `site_down_7d`/`site_recovered` digest-only; `site_recovered` also on target change; hijack event on transition only.
  - E2: one job table (`web_check`, `web_metrics` 00:05Z + 06/12/18:05Z catch-up, `web_spn2` Sun 07:00Z) with factory, trigger, misfire, timeout, flag, provider, liveness.
  - G1/G5/F1/B12: `Http.request` with provider ids `web`/`wayback` (no `web_sites`/`spn2` buckets); TLS handshakes counted; `WEB_KILLED` sentinel and `WEB_ALERT_AFTER_FAILURES` retired; pauses via `control_state`/`PAUSE_GROUPS`.
  - Review fixes: the Availability fallback returned the *latest* capture (now `timestamp=19960101`, CDX first); © regex left the year behind (annual false freshness); forced `Accept-Encoding: br` without brotli; robots `can_fetch` must get the bare token; EAI_AGAIN and plain 429 no longer count as down; internal and network-outage rows never count as down (preflight + batch gate); default-server pages now parked; `http://` sites get `no_tls`.
  - Deps/tests: `trafilatura` per 01 pin, `lxml[html_clean]`, `cryptography>=42`; `respx` + 03's schema in tests; verify SQL on INTEGER columns; the `www.deprecated.com` expectation removed (04 drops junk).
  - New cross-spec asks: §11 Q14 (03 `occ|` dedupe prefix), Q15 (01 `Http` body cap / per-host `Retry-After`), Q16 (16 `web` pause group), Q17 (14 needs-review buckets), Q18 (12/14 `site_recovered` reasons).
