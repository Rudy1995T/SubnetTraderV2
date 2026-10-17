# [web] web-quant-scoring-methods

**Topic:** How quant/fundamental shops build composite health/quality/sentiment scores — inputs, normalization, weighting, NLP, and validation, to inform a 0-100 Bittensor subnet score (TaoSentinel)

## Findings

## 1. Crypto fundamentals scoring — inputs and normalization tricks (verified from vendor docs unless marked)

**Santiment Development Activity** (academy.santiment.net/metrics/development-activity/). Counts *GitHub events*, not commits. Rationale: forking inherits all commits (Bitcoin fork = 40k+ "commits") but emits a single `ForkEvent`; commit counts are also trivially spammable. "Development Activity" = subset of event types; "GitHub Activity" = all events. Explicitly EXCLUDED from Dev Activity: issue comments, issues opened/closed, forks, commit comments, issue subscribes, release downloads, watching/starring, project-management events. INCLUDED (documented only as "pure dev events"; PushEvent and PullRequestEvent confirmed, rest UNVERIFIED): PushEvent, PullRequestEvent, PR review events, CreateEvent/DeleteEvent (branches/tags), ReleaseEvent. Scope: public repos only; private→public flips emit no back-history; tracked per GitHub *organization* and rolled up per asset/ecosystem. Metrics: `dev_activity` (5-min), `dev_activity_1d`, `dev_activity_contributors_count` (unique actors), `30d_moving_avg_dev_activity_change_1d`, `ecosystem_dev_activity`. CAVEAT: academy page carries a notice (Aug 2026) that GitHub-based metrics are "currently broken … expected to remain unreliable for a while" — do not depend on it; replicate the method with your own GitHub collector.

**Electric Capital Developer Report** (developerreport.com/about). Identity dedupe (multi-account committers collapsed); 483M commits fingerprinted (file, lines changed, message, committer, author, dates) to drop duplicated/merged upstream code — "only new code counts"; bots removed; only *original commit authors* count (PR mergers don't). Classification over an 84-day rolling window: **Full-time = ≥10 commit-days in a rolling 28-day period and sustained across multiple weeks; Part-time = intermittent regular activity; One-time = minimal/sporadic**. Monthly active devs use a 28-day rolling window "for stability". Ecosystem eligibility: top-200 network value, or >$500M private valuation, or >50 monthly active devs. Sub-ecosystems attributed recursively via their open taxonomy (github.com/electric-capital/crypto-ecosystems). Takeaway for subnets: score *commit-days per author* (10/28 threshold) and *count of full-time-equivalent authors*, not commit volume.

**Token Terminal** "Core developers" = unique GitHub users with ≥1 commit to the project's *main-org public repos* in the past 30 days (peripheral repos excluded for comparability). Fees = total user payments from on-chain events; Revenue = fees × take-rate retained by protocol; P/F (FD) = fully-diluted mcap / annualized fees; P/S (FD) = FD mcap / annualized revenue.

**DefiLlama data definitions** (docs.llama.fi/analysts/data-definitions). Accounting identity: `Revenue = Fees − Supply-side revenue`; `Earnings = Revenue − token incentives emitted`; Holders revenue = buyback/burn/direct staker distributions (only in the governance token; burns need real receipts). TVL excludes borrowed, own governance token, self-minted, vesting, double-counted LP/receipt tokens, native staking; categories tracked separately (staking, pool2, borrowed, vesting). Accrual basis; unverifiable self-reports excluded; realized losses booked as negative fees. Bittensor analogue: subnet "fees" ≈ external revenue/API sales, "incentives" ≈ alpha emissions; earnings-style metric = revenue − emissions value.

**Messari Disruption Factor** (messari.io/report/the-disruption-factor). Formula `DF = Σ_i W_i · P_i · (1 − G_i)` where P_i = metric's percentile rank (0–1) *relative to sector peers*, G_i = "gamability" discount (higher for metrics easy to manipulate, e.g., raw tx counts), W_i = weight; rolled into pillar scores then summed to 0–100. 7 pillars / 28 metrics (names partially UNVERIFIED — includes an "Economic Longevity" pillar; press coverage cites usage/economics/developer/decentralization areas). L2 debut: Arbitrum 69.5, Base 67.1, OP 58.2. Key ideas to copy: peer-relative percentile, explicit gamability haircut per metric.

**LunarCrush** (github.com/lunarcrush/api, lunarcrush.com/metrics/galaxy-score). Galaxy Score = 0–100 "proprietary score based on technical indicators of price, average social sentiment, relative social activity, and a factor of how closely social indicators correlate with price and volume" (i.e., price score + sentiment + social engagement + correlation); measures asset vs *itself*; >50 = healthy. AltRank = rank vs all supported assets on social+price (lower is better). **Sentiment = % of posts, weighted by interactions, that are positive** (each post scored 1–5, 3 neutral). Social Dominance = share of total social volume, rolling 24h. Interactions = views+likes+comments+shares etc. Exact weights proprietary since Oct 2021. API v4: Bearer key; tiers Hobby free 4 req/min, 100/day (market data, Galaxy/AltRank only); Individual 10/min, 2k/day (+social/creator); Builder 100/min, 20k/day; Scale 500/min, 100k/day (all endpoints, full time series); hourly buckets <365d, daily beyond.

**CoinGecko Trust Score** (exchanges, 1–10): components Liquidity (weight 4/10; volume, order-book depth ±2%, spread, trade frequency, outlier checks, SimilarWeb traffic), Cybersecurity 2/10, Scale 1/10, plus Regulation, Past Incidents, Team Presence, Proof of Reserves (3.0). Pair-level trust is Green/Yellow/Red. Not a coin-fundamentals score; its structural lesson: heaviest weight on the hardest-to-fake input (liquidity), explicit incident/team-presence penalties.

**Kaito**: mindshare = share of voice (% of category attention over trailing 7D/30D/3M windows — windows UNVERIFIED), explicitly *not* sentiment-dependent; sentiment is separate; attention weighted by "Smart Followers" (accounts identified via social-graph analysis) and bot filtering via "proprietary ML". No public formula; paid/enterprise.

**CryptoRank**: fundamental data (funding rounds, investors, vesting/unlock schedules, tokenomics) exposed via API; Blockunity builds 3 core scores × 6 sub-scores from it (details UNVERIFIED). **CER.live** security rating: 11 binary/tiered 0–10 items (audited, auditor >50 audits, findings remediated, findings published, verified contracts, audited code == deployed, bug bounty, public team, insurance, audit scope, incidents), weighted (audit items 0.55 for tokens; incidents/insurance 0.30) → AAA…D bands (≥9 AAA, 8.5 AA, 8 A, 7.5 BBB, 6.5 BB, 6 B, 5 CCC, 4.5 CC, 4 C, 3 DDD, 2.5 DD, <2.5 D). Good template for a "doxxed-team / security" checklist pillar.

**OpenSSF Scorecard** (0–10 per check, 18–20 checks; risk weights Critical=10, High=7.5, Medium=5, Low=2.5; aggregate = risk-weighted mean). Directly reusable checks: Maintained (High; 10 if ≥1 commit/week over 90 days, 0 if archived), Code-Review (High; −7 for one unreviewed change, −3 more for multiple), Contributors (Low; ≥3 orgs with ≥5 commits in last 30 commits), Branch-Protection (High; tiered 3–10), CI-Tests (Low), Security-Policy (Medium; 6+3+1 tiers), License (Low; 6+3+1), Signed-Releases (High; 8 sigs / 10 SLSA), Vulnerabilities (High; OSV). Public REST: `https://api.securityscorecards.dev/projects/github.com/{org}/{repo}` (free, no auth).

**OpenSSF Criticality Score** — the best off-the-shelf *normalization recipe*: `C = Σ_i α_i · log(1+S_i) / log(1+max(S_i,T_i)) / Σ_i α_i` ∈ [0,1]. Defaults (weight, cap): created_since months (1, 120), updated_since months (−1, 120), contributor_count (2, 5000), org_count (1, 10), commit_frequency/week last yr (1, 1000), recent_releases last yr (0.5, 26), closed_issues 90d (0.5, 5000), updated_issues 90d (0.5, 5000), comment_frequency per issue 90d (1, 15), dependents (2, 500000). Log(1+x) + cap = robust to heavy tails and spam.

**CHAOSS** definitions: Bus Factor / Contributor Absence Factor = smallest number of contributors producing 50% of contributions; Elephant Factor = same over employers; Time to First Response (issue/PR → first human reply); Issue Resolution Duration / Time to Close; Release Frequency (count all point releases). arXiv 2309.12120 warns single context-free health indicators fail to identify sustainability — use composites.

Academic evidence on dev metrics: MSR'19 (cmustrudel "Striking Gold") found *stars* (popularity) associated with market cap, activity/QA metrics added no incremental info; dev activity is a lagging price indicator but leading viability indicator. Treat GitHub health as a *survival/quality* filter, not a return predictor.

## 2. Composite construction best practice

**Normalization**: (a) cross-sectional z-score with winsorization at ±3σ (Barra USE4; MSCI Momentum winsorizes z at ±3 then maps z→score via `1+z` if z≥0 else `1/(1−z)` to keep scores positive/asymmetric); Barra re-standardizes to cap-weighted mean 0, equal-weighted std 1. (b) Percentile/rank (Messari; Alphalens rank-IC) — robust, bounded, but loses magnitude and inflates tiny differences in small universes (~128 subnets → 0.8-pt percentile gaps). (c) log(1+x) with cap (OpenSSF) for counts. (d) Absolute thresholds for checklist items (Piotroski/CER). Recommendation for ~100–130 subnets: rank-Gaussianize (inverse-normal of percentile) for continuous metrics, log-cap for counts, binary for checklist; report both peer-relative and absolute.

**Weighting**: OECD/JRC Handbook + COINr: equal weights are the default (experts converge near-equal; results insensitive; transparent); PCA weights maximize variance but overweight correlated indicators and ignore importance; budget allocation for expert priors; "effective weight" ≠ nominal weight because of correlations and hierarchy. Quant practice (Barra/Qian): ICIR-weighted `w_k ∝ IC_k / σ(IC_k)`; or max-ICIR `w = Σ⁻¹ · IC̄` (Σ = IC covariance). Bayesian shrinkage (Clemen & Winkler 1986; James–Stein): posterior weights = matrix-weighted average of LS/IC weights and prior (equal) weights; practical intensity `λ = N/(N+κ)` so weights move toward data only as sample N grows. Given tiny history, start equal-within-pillar, pillar weights by budget allocation, and only shrink toward IC weights once ≥~100 cross-sectional periods exist.

**Aggregation**: arithmetic (fully compensatory) vs geometric (penalizes any pillar near zero — appropriate for "any red flag kills it" health scores). Hybrid used by rating shops: geometric across pillars, arithmetic within.

**Missing data**: Barra USE4 imputes at factor level by regressing available exposures on peer attributes (industry/size) — not naive zero. Alternatives: neutral (median/0-z) imputation + a coverage/confidence flag; explicit penalty only for *verifiable absence* (no public repo, no website) vs *collection failure* (API down). Cross-sectional PCA imputation (Bryzgalova et al.) for panels. Never let a data outage change a rank silently — carry forward last value with decay and mark stale.

**Time decay of stale signals**: exponential weights `w(t)=0.5^{Δt/h}`; RavenPack smooths entity-level Event Sentiment Score with decay windows 1–21 days and gives repeat stories a novelty score decaying by count in prior 24h; S&P/RavenPack index uses 1/3/9 quarterly multipliers. Barra uses half-lives (e.g., 84–504 days) for covariance. Suggested: sentiment h≈3–7d, milestone/news h≈14–30d, GitHub 30–90d rolling, team/security checklist no decay but re-verify quarterly.

**Smoothing & rank stability**: MSCI Momentum uses rank buffers (target 500: add if rank ≤250, retain existing if rank ≤750) and turnover buffer `w_new = x + (y−x)/2`; semi-annual rebalance with conditional trigger. MSCI ESG committees "lean against" threshold crossings (MIT "traffic light effect", 2026) — beware: suppressed downgrades cost investors ~2.8% next-year. Do: EMA the 0–100 score (span 7–14 obs), assign tiers with hysteresis (enter tier at ≥T+3, exit at ≤T−3), publish both raw and smoothed, log every tier change with reason. Measure churn via Alphalens *factor rank autocorrelation* and *quantile turnover*.

**Regime conditioning**: crypto evidence — technical/sentiment predictability stronger in "greed"/high-vol regimes; sentiment causal effects concentrated in bear/turmoil states; extreme sentiment states carry most information (arXiv 2602.07018 "Extremity Premium"). Condition sentiment weight on TAO/BTC trend & vol regime; keep fundamentals weight constant.

**Point-in-time**: Fama–French forms portfolios each July using December-FY accounting data (≥6-month lag) so only *published* information is used; PIT universes avoid survivorship. For TaoSentinel: store every signal with `observed_at` (when you fetched it) and `event_at`; backtests may use a row only if observed_at ≤ decision time; keep deregistered subnets in history; never recompute past scores with today's classifier without versioning (LLM output drift = look-ahead; cf. Look-Ahead-Bench arXiv 2601.13770).

**Slow + fast combination**: Piotroski F-score = 9 binary tests summed 0–9 (author: binary chosen for simplicity; loses magnitude; suggested multifactor rank alternative). OpenSSF = risk-weighted 0–10. Pattern: `Score = 100 · [ w_F · F_slow (fundamental, monthly half-life) + w_S · S_fast (sentiment/attention, days) ]` with w_S regime-scaled; signals should be traded at their own half-life (IC decay curve: horizon where IC peaks = natural holding period; where IC halves = half-life). Sentiment decays in days–weeks; quality/value persist months–years.

## 3. Sentiment NLP for crypto

Model comparisons: on crypto social text, CryptoBERT (ElKulako/cryptobert, HF) F1 55.6 vs FinBERT 52.7 vs BERT 53.7 (Frasincar/sentic ACSA 2023 — 3-class, noisy data); "LLMs and NLP Models in Cryptocurrency Sentiment Analysis" (Applied-AI-Research-Lab, 2024): GPT-4 best F1 on positive class, BERT/FinBERT better on neutral; fine-tuned GPT-4 gave most balanced per-class performance vs FinBERT/BERT/Flan-T5/Gemma-7B (Springer 2025). Gurgul et al. 2025 used facebook/bart-large-mnli zero-shot bullish/bearish. Practical stack: LLM zero/few-shot with JSON schema for *event type + direction + confidence*, cheap local model (CryptoBERT) for bulk polarity, LLM only on high-engagement or team-account posts.

Standard label sets: polarity {bullish, bearish, neutral} (+ intensity 1–5 as LunarCrush). Event taxonomies from finance NLP (Stock Sonar 8 types: Legal, Analyst, Financial, Price, Deals, M&A, Partnerships, Product; SentiFM 10 types; DocFEE 9 types) → crypto/subnet taxonomy: product_launch, release/upgrade, partnership/integration, revenue/customer, token_buyback/burn, emissions/tokenomics_change, team_change, funding, security_incident/exploit, delay/missed_milestone, governance/registration, regulatory, giveaway/promo (spam). Each with direction and materiality.

Bot/shill detection heuristics: follow ratio, posting velocity, profile completeness, username patterns, account age × tweet count, duplicate/near-duplicate text within minutes, co-retweet clusters, synchronized creation dates, shared hashtags; Botometer (1000+ features; crypto/promo accounts skew high). arXiv 2001.10289: >56% of accounts sharing Telegram/Discord invites were bots/suspended; 93% of bot-shared links pointed to pump-and-dump channels. arXiv 2209.02911 (48 coins): engagement *coefficient* is non-linear — very low = no interest, very high = artificial; bot-heavy coins had lower future returns; strategy worked for holding periods of a few months. Engagement-weighted (LunarCrush, Kaito smart-follower weighting) beats count-weighted; add per-account credibility weight and cap any single account's share.

Predictive-value evidence: Liu & Tsyvinski (RFS 2021): 1σ rise in Google "Bitcoin" search → +2.3% 2-week-ahead return; Twitter post counts also predict; strong 1-week momentum (3.16%/σ). LASSO-VAR (Applied Economics 2023, 10 coins, 2018–22 daily): sentiment/attention improve forecasts *only for less-capitalized coins* (relevant: subnet alpha tokens are small caps). Finance Research Letters 2024: engagement-weighted tweet sentiment moves prices. 2020–25 daily index: 1-unit lagged sentiment → +0.24–0.25% next-day BTC. Gurgul 2025: daily zero-shot sentiment improved Sharpe vs buy-and-hold (BTC 2011–2023) but no regime split; Wei 2023 XGBoost significant but 295-model selection = overfit risk; Yae & Tian 2022: attention/volume predictors fail out-of-sample. Survey arXiv 2606.00071 (2026): evidence is thin, publication-biased, mostly intraday-to-daily; attention/engagement effects last ~1–2 weeks (Liu–Tsyvinski) to a few months (engagement-threshold selection). Sentiment-aware MVO (arXiv 2508.16378, 5 coins 2020–25): sentiment+technical Sharpe 0.71 vs 0.70 technical-only, 0.60 sentiment-only, 0.55 BTC B&H — marginal. Conclusion: use sentiment as a *timing/overlay weight with half-life of days*, attention/engagement for 1–4 week horizons, fundamentals for allocation.

## 4. Validation workflow

Pipeline (standard A-share/Barra practice): winsorize (3σ or MAD) → impute → standardize → neutralize (regress out size/sector; for subnets: regress out market-cap/emission share and category) → single-factor test → combine → portfolio → evaluate. Metrics: IC = cross-sectional corr(factor_t, r_{t+1}); Rank-IC (Spearman) preferred; ICIR = mean(IC)/std(IC); t-stat = ICIR·√N_periods. Thresholds: |IC|>0.02–0.03 meaningful, Rank-IC >0.05 good, IC>0 in >55% of periods, ICIR >0.5 deployable, >1.0 excellent. Harvey–Liu–Zhu (RFS 2016): new factors need t>3.0 given the factor zoo. Sample size: N ≥ (t_target/ICIR)² periods → ICIR 0.3 needs 45 weekly periods for t=2, 100 for t=3. Quantile analysis (Alphalens): mean period-wise return by quantile, top-minus-bottom spread, cumulative Q5–Q1, quantile turnover, factor rank autocorrelation, IC decay across horizons (1d/1w/2w/4w/12w). Deflated Sharpe (Bailey & López de Prado 2014): `PSR(SR*) = Φ[ (ŜR − SR*)·√(T−1) / √(1 − γ̂₃·ŜR + ((γ̂₄−1)/4)·ŜR²) ]`; `E[max_N ŜR] ≈ E[ŜR] + √V[ŜR]·( (1−γ)·Φ⁻¹(1−1/N) + γ·Φ⁻¹(1−1/(N·e)) )`, γ=0.5772; `DSR = PSR(SR₀)` with SR₀ = E[max]; N = *effective* independent trials. MinBTL ≈ `[(1−γ)Φ⁻¹(1−1/N) + γΦ⁻¹(1−1/(Ne))]² / E[max SR]²` (< 2ln N / E[max]² years). Walk-forward: rolling train (e.g., 180 days as in arXiv 2508.16378) / test; rolling-window CV folds; Diebold–Mariano vs naive baseline; report by regime. Log every variant tried (N for DSR). Grinold: `IR = IC·√BR·TC` — with ~120 subnets and weekly decisions breadth is modest; small IC (0.03–0.05) still useful.

Rank-stability recipe for a 0–100 index: EMA score; hysteresis tiers; MSCI-style buffers; minimum data-coverage gate before a subnet is rankable; freeze weights for a quarter; version the scoring spec and store scores under `spec_version`.

## Data sources catalog

| Name | URL | What it gives | Access | Reliability |
|---|---|---|---|---|
| GitHub REST/GraphQL API (self-collected dev activity) | https://docs.github.com/en/rest | Commits (author, date), PRs, reviews, issues, releases, stars, forks, contributors per repo — raw material to replicate Santiment event-based dev activity, Electric Capital full-time-dev classification (>=10 commit-days per 28d), CHAOSS bus factor, time-to-first-response, release frequency | Free with personal access token; REST 5,000 req/hr per token; GraphQL 5,000 points/hr; public Events API only returns last 300 events/90 days so use commits/pulls/issues endpoints instead | Authoritative but only public repos; dedupe multi-account authors; strip bot accounts ([bot] suffix, dependabot, renovate); exclude merge commits from upstream; track org-level not single repo |
| OpenSSF Scorecard public API | https://api.securityscorecards.dev/projects/github.com/{org}/{repo} | 0-10 per check (Maintained, Code-Review, Branch-Protection, CI-Tests, Security-Policy, License, Signed-Releases, Vulnerabilities, Contributors...) and risk-weighted aggregate score | Free, no auth; only repos already scanned by the weekly cron (top ~1M repos); run `scorecard` CLI with a GitHub token for others | Deterministic and reproducible; small subnet repos may be absent from public API; run CLI locally |
| OpenSSF Criticality Score CLI | https://github.com/ossf/criticality_score | log(1+S)/log(1+max(S,T)) weighted-average normalization with default weights/caps for created_since, updated_since, contributor_count, org_count, commit_frequency, recent_releases, closed/updated issues, comment_frequency, dependents | Free; needs GitHub token; dependents signal needs BigQuery (optional) | Formula is directly reusable for a subnet GitHub pillar; caps must be re-tuned for small crypto repos (e.g., contributor cap 50 not 5000) |
| Santiment SanAPI (dev activity, social volume, sentiment) | https://academy.santiment.net/sanapi/ | dev_activity, dev_activity_contributors_count, github_activity, social_volume, sentiment metrics via GraphQL; per-asset and per-GitHub-org | Free: 1,000 calls/month, 100/min, 500/hr, 1 year history minus last 30 days; Pro/Max remove lag; 20% off paying in SAN | Academy notice (Aug 2026): GitHub-based metrics currently broken/unreliable; TAO listed but subnets are not separate assets — use only as cross-check |
| LunarCrush API v4 | https://lunarcrush.com/api4 | Galaxy Score (0-100), AltRank, sentiment (% positive interaction-weighted), social dominance, interactions_24h, social_volume, contributors, posts, creators, topic time series (hourly <365d) | Bearer API key; Hobby free 4 req/min, 100/day (market data only); Individual 10/min, 2k/day; Builder 100/min, 20k/day; Scale 500/min, 100k/day | Proprietary weights; topics exist for TAO and some subnets (UNVERIFIED coverage per subnet); good for engagement-weighted sentiment benchmark |
| Electric Capital Open Dev Data / crypto-ecosystems taxonomy | https://github.com/electric-capital/crypto-ecosystems | Open taxonomy mapping repos to ecosystems/sub-ecosystems (Bittensor ecosystem entry lists subnet repos), plus aggregate developer counts on developerreport.com | Free, open source (taxonomy); aggregate dashboards free; raw commit-level data not public | Use taxonomy as seed list of subnet repos; verify Bittensor sub-ecosystem completeness (UNVERIFIED) |
| DefiLlama API + dimension-adapters | https://docs.llama.fi/ | Fees/revenue/holders-revenue/earnings definitions and open adapters; TVL rules | Free, no auth, fair-use rate limits | Bittensor subnets largely absent; value is the accounting framework (Revenue = Fees − supply-side; Earnings = Revenue − emissions) to apply to subnet revenue disclosures |
| Messari (Disruption Factor, metrics) | https://messari.io/report/the-disruption-factor | DF = Σ W_i·P_i·(1−G_i) peer-percentile, gamability-discounted composite; 100+ quant metrics; classifications | Reports partially free; API/Pro paid (pricing UNVERIFIED) | Methodology documented in report; exact pillar/gamability table not retrieved (429s); DF not per-subnet |
| Token Terminal | https://tokenterminal.com/docs/explorer/metrics | Core developers (unique committers 30d, main-org repos), fees, revenue, P/F, P/S, earnings, token incentives | Paid Pro/API (pricing UNVERIFIED); docs index at tokenterminal.com/docs/llms.txt | Bittensor subnet coverage minimal; borrow definitions |
| Kaito AI | https://kaito.ai/ | Mindshare (share of voice), sentiment, smart-follower-weighted attention, narrative tracking | Paid/enterprise; no public API pricing; methodology proprietary | Concept reusable (smart-follower weighting, bot filtering) but not reproducible; UNVERIFIED subnet-level coverage |
| CER.live cryptocurrency security rating methodology | http://cer.live/methodology/cryptocurrencies/ | 11-item audit/team/incident checklist with weights and AAA-D bands — template for a doxxed-team/security pillar | Free methodology page; ratings site free | Exchange/token oriented; adapt items (public team, audits, incidents, bug bounty) to subnets |
| CHAOSS metrics definitions (Augur / GrimoireLab) | https://chaoss.community/kb/metrics-model-starter-project-health/ | Standard definitions: bus factor, elephant factor, time to first response, issue resolution duration, release frequency, change request closure ratio | Free, open source tooling | Implementation-agnostic definitions; compute from GitHub API |
| Alphalens (alphalens-reloaded) | https://alphalens.ml4trading.io/api-reference.html | IC, rank-IC, ICIR, quantile returns, quantile turnover, factor rank autocorrelation, IC decay by horizon tear sheets | Free, pip install alphalens-reloaded (Python 3.13 compatibility UNVERIFIED) | Standard factor validation; needs pricing panel with forward returns for subnet alpha tokens |
| Deflated Sharpe Ratio reference implementation | https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf | PSR/DSR/MinBTL formulas; skfolio PR #255 adds deflated_sharpe_ratio and expected_max_sharpe_ratio (merge status UNVERIFIED) | Free | Requires logging effective number of trials N |
| CryptoBERT / FinBERT / BART-MNLI (HuggingFace) | https://huggingface.co/ElKulako/cryptobert | Local 3-class crypto social sentiment (CryptoBERT), finance sentiment (ProsusAI/finbert), zero-shot bullish/bearish/event labels (facebook/bart-large-mnli) | Free, run locally on CPU | F1 ~0.53-0.56 on noisy crypto tweets; use LLM (Claude) for event-type extraction on high-value posts, CryptoBERT for bulk polarity |
| Botometer / bot heuristics | https://botometer.osome.iu.edu/ | Bot probability from 1000+ features; heuristic fallbacks: follow ratio, posting velocity, account age×tweets, duplicate text, synchronized clusters | RapidAPI, availability post-X-API changes UNVERIFIED | X API restrictions make third-party bot scoring unreliable; implement heuristics locally |

## Sources

- https://academy.santiment.net/metrics/development-activity/
- https://academy.santiment.net/metrics/development-activity/development-activity/
- https://medium.com/santiment/tracking-github-activity-of-crypto-projects-introducing-a-better-approach-9fb1af3f1c32
- https://www.developerreport.com/about
- https://www.developerreport.com/developer-report
- https://tokenterminal.com/explorer/metrics/active-developers
- https://tokenterminal.com/resources/articles/token-terminal-key-metrics-faq
- https://docs.llama.fi/analysts/data-definitions
- https://messari.io/report/the-disruption-factor
- https://messari.io/newsletter/unqualified-opinions/the-disruption-factor-a-new-framework-for-ranking-real-crypto-impact-1
- https://lunarcrush.com/metrics/galaxy-score
- https://github.com/lunarcrush/api
- https://lunarcrush.com/about/api
- https://www.coingecko.com/en/methodology
- https://support.coingecko.com/hc/en-us/articles/36442561461657-Trust-Score-Methodology
- https://kaito-ai.gitbook.io/product-docs/overview/use-cases-project-teams
- https://cryptorank.io/insights/analytics/blockunity-scoring-api-cryptorank
- http://cer.live/methodology/cryptocurrencies/
- https://github.com/ossf/scorecard/blob/main/docs/checks.md
- https://github.com/ossf/criticality_score
- https://www.chaoss.community/kb/metric-elephant-factor/
- https://chaoss.community/kb/metrics-model-starter-project-health/
- https://arxiv.org/pdf/2309.12120
- https://cmustrudel.github.io/papers/msr19crypto.pdf
- https://www.top1000funds.com/wp-content/uploads/2011/09/USE4_Methodology_Notes_August_2011.pdf
- https://www.msci.com/indexes/documents/methodology/2_MSCI_Momentum_Indexes_Methodology_20250725.pdf
- https://mitsloan.mit.edu/press/study-suggests-mscis-esg-ratings-favor-stability-over-frequent-updates
- https://www.hbs.edu/ris/Publication%20Files/26-072_c2450c13-b5b3-4444-b434-fe283a05cd3b.pdf
- https://bluefoxr.github.io/COINrDoc/weighting-1.html
- https://www.oecd.org/content/dam/oecd/en/publications/reports/2008/08/handbook-on-constructing-composite-indicators-methodology-and-user-guide_g1gh9301/9789264043466-en.pdf
- https://arxiv.org/pdf/0707.3482
- https://arxiv.org/pdf/2207.13071
- https://arxiv.org/pdf/2601.13770
- https://www.ravenpack.com/products/edge/factors/forex-sentiment
- https://www.spglobal.com/spdji/en/documents/methodologies/methodology-sp-500-rvnpck-ai-sentiment-indices.pdf
- https://en.wikipedia.org/wiki/Piotroski_F-score
- https://dev.to/linou518/quant-factor-research-in-practice-ic-ir-and-the-barra-multi-factor-model-1h8k
- https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf
- https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551
- https://academic.oup.com/rfs/article/29/1/5/1843824
- https://alphalens.ml4trading.io/api-reference.html
- https://www.nber.org/papers/w24877
- https://www.tandfonline.com/doi/abs/10.1080/00036846.2023.2289930
- https://www.sciencedirect.com/science/article/abs/pii/S1544612324006287
- https://arxiv.org/html/2606.00071v1
- https://arxiv.org/html/2508.16378
- https://arxiv.org/pdf/2204.05781
- https://arxiv.org/pdf/2209.02911
- https://arxiv.org/abs/2001.10289
- https://arxiv.org/pdf/2602.07018
- https://www.sciencedirect.com/science/article/pii/S3050700625000672
- https://personal.eur.nl/frasincar/papers/ACSA2023/acsa2023.pdf
- https://github.com/Applied-AI-Research-Lab/LLM-and-NLP-models-in-Cryptocurrency-Sentiment-Analysis
- https://link.springer.com/chapter/10.1007/978-3-031-96235-6_23
- https://arxiv.org/pdf/1911.10783
- https://scrapebadger.com/blog/how-to-detect-spam-or-bots-on-twitter-and-what-to-do-with-that-data
- https://academy.santiment.net/sanapi/rate-limits/
- https://academy.santiment.net/sanapi/historical-and-realtime-data-restrictions/
- https://microalphas.com/information-coefficient/
- https://backtestbrewery.github.io/posts/alpha-decay-trading.html

## Open questions

- Santiment GitHub dev-activity metrics are flagged broken as of Aug 2026 — is there a restoration date, or should TaoSentinel fully self-host the event-based collector?
- Messari Disruption Factor: exact 7 pillar names, 28 metrics, per-metric gamability discounts G_i and weights W_i were not retrievable (HTTP 429); needs manual read of the report.
- Kaito mindshare time windows (7D/30D/3M) and smart-follower weighting formula are proprietary/UNVERIFIED; can subnet-level mindshare be pulled at all without an enterprise contract?
- LunarCrush topic coverage per Bittensor subnet (does each subnet have a 'topic'?) and whether the free Hobby tier exposes enough to benchmark our own engagement-weighted sentiment.
- Does Electric Capital's crypto-ecosystems taxonomy enumerate all current Bittensor subnet repos (coverage gap likely for newer subnets)?
- Current X/Twitter API pricing and whether pulling ~130 subnet team accounts' timelines fits Basic tier read caps (2026 pricing not verified here).
- Which forward-return horizon on subnet alpha tokens is tradable given thin liquidity — evidence says attention/sentiment works at 1-day to 2-week horizons for small caps, but subnet AMM slippage may erase it; needs IC-decay curve on actual TAO/alpha price panel.
- Minimum viable history: with ~120-130 subnets and weekly rebalancing, t>3 requires ~100 periods (~2 years) at ICIR 0.3 — is there enough on-chain alpha price history since dTAO (Feb 2025) to validate anything beyond IC>0 sign tests?
- Point-in-time integrity of LLM-labeled sentiment: how to freeze classifier versions so historical scores are not silently rewritten (spec_version per row)?
- Whether alphalens-reloaded and skfolio DSR helpers run under Python 3.13 on the Ubuntu box (not tested).
