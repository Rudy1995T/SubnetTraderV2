# [web] web-ecosystem-2026

**Topic:** Bittensor subnet ecosystem landscape as of Sept 2026: what drives long-term subnet value, existing health-scoring efforts, KOLs/channels, and red-flag taxonomy (input to TaoSentinel scoring model)

## Findings

## 1. Landscape (Sept 2026)

**Counts & caps.** 128-129 active subnets (ownyourmind.ai recorded 129 on 2026-09-18; TAO Institute/bex.co say 128, up 97% from 65 at start of 2025). Protocol cap doubled 128→256 ("Robin τ" expansion, May 2026, per BSCNews/openpr) — UNVERIFIED whether all 256 slots are registrable yet; tao.media mentions a "planned limit of 1,028" (UNVERIFIED). Deregistration mechanism live since 2025-09-17: when cap is hit, the new registration prunes the non-immune subnet with the lowest **EMA alpha price**; immunity reduced 6→4 months (Oct 2025). taostats.io/subnets exposes a live "Pruning Rank / EMA Price / Remaining Immunity" table (on 2026-09-22 bottom of list: SN86, SN59, SN108 at EMA 0; then SN16, SN99, SN92, SN76, SN103, SN113, SN116). On deregistration, alpha is swapped back to TAO and returned to holders; the netuid is recycled.

**Halving.** First halving 2025-12-12/14: 7,200→3,600 TAO/day (1→0.5 TAO/block). Daily emission measured 3,559 TAO on 2026-08-18. ~70-72.5% of staked TAO sits on root (SN0); alpha staking ≈27.5%; total staked ≈7.35M TAO. Academic finding (Maymin, arXiv 2603.29751, 2026-03-31): a small-minus-big size premium in alpha tokens of 1.17%/day pre-halving fell to 0.51%/day post-halving; only implementable below ~$10k AUM due to AMM slippage.

**Emission-mechanism timeline (critical for scoring — the rules changed 5 times in 10 months):**
- 2025-02-14 dTAO launch (price-based emissions via per-subnet AMM).
- Nov 2025 **Taoflow**: emissions weighted by *net TAO inflow* (stake − unstake), not raw price.
- 2026-05-08 hotfix v3.3.15-402: owner's free alpha at registration set to zero (`owner_alpha_stake = AlphaBalance::ZERO`).
- 2026-05-13 **Conviction / BIT-0011 (Locked Stake)** on mainnet: owner emissions auto-lock; conviction = stake × time, EMA re-averaged every 30 days; anyone can challenge ownership by locking alpha; exits require public on-chain unlock tx. First onboarded: SN3, SN39, SN81 (the Covenant subnets). 25/128 subnets had owner-locked alpha by 2026-06-11.
- June 2026 price-based `get_shares` formula with **miner-burn penalty** shipped; 2026-06-22 emissions switched off for **57 subnets** ("no active miner mechanism or no code"); weekly Monday review, case-by-case reinstatement (Const, Discord). tao.media's netuid list for this cut is UNVERIFIED (it includes SN5/14/53/62/80 which are now top-20 — likely reinstated or mis-extracted).
- 2026-07-28 **v440 "Emission Gate"**: subnets ranked by demand share; bar set where subnets above carry q=0.61 of demand (~rank 32); `gate(s)=s^h/(s^h+θ^h)`, h=3; top-8 emission share 32.8%→52.7%; 94 tail subnets 38.4%→12.5%. Params `EmissionBarQuantile`, `EmissionGateExponent` root-sudo adjustable. Explicit statement: tail alpha "earns materially less."
- 2026-08-03 **Root Reborn v4.4.1**: root yield no longer auto-liquidates alpha; validators direct yield into "baskets" of subnet tokens (validators become allocators). Yuma (DCG) published a critique 2026-06-19 citing moral hazard.
- Subtensor spec 464 merged to main 2026-09-17 (unifies 462 baskets + 463 security; `BasketTradingEnabled` stays OFF; unstake_all 16-leg admission envelope; StakingHotkeys capped at 256). Mainnet/Finney still spec 459 at that date. v437 (2026-07-09) added air-gapped signing. Spec 466 (fee cap) PR open.

**Governance shocks.** 2026-02-12 Jacob Steeves (Const) stepped down as OTF CEO (role eliminated); builds Affine (SN120). 2026-04-02 OTF transferred 9 core repos (SDK `latent-to/bittensor`, `latent-to/btcli`) to **Latent 11** (single-client concentration risk). 2026-04-09/10 **Covenant AI exit** (Sam Dare): abandoned SN3 Templar, SN39 Basilica, SN81 Grail, sold ~37,000 TAO (~$10-11M); TAO −25-30% (~$650-900M mcap), Templar −50%+. SN3 continued as Teutonic (80B run May; Teutonic-II-110B announced 2026-08-24). 2026-06-22 Const published 18-month decentralization roadmap (target Dec 2027).

**Capital/markets.** Grayscale GTAO (OTC) S-1 filed 2025-12-30, Amendment 1 2026-04-02 (no staking); Bitwise spot TAO ETF filed late Apr 2026; SEC decision was expected Aug 2026 — **no approval/rejection found as of 2026-09-22 (UNVERIFIED status)**. STAO staked ETP on Euronext Paris 2026-06-02. Treasury cos: TAO Synergies (Nasdaq TAOX, 87.7% of treasury in TAO at 2026-06-30), xTAO (61,135 TAO at 2026-03-27), TaoWeave. Yuma Total Market Fund (2026-06-25) offers institutional alpha exposure; Yuma Composite Fund −31.9% since 2025-09-04 vs TAO −46.1%. Claimed Nvidia $420M / Polychain $200M TAO positions (Q1 2026) — **UNVERIFIED, no on-chain proof published**. Kraken native dTAO integration 2026-07-03; roadmap: Targon SN4, Vanta SN8, Score SN44, Lium SN51, Ridges SN62, Chutes SN64, Hippius SN75; **Vanta SN8 went live 2026-07-16** (first alpha on a tier-1 CEX). TAO on Solana via Wormhole 2026-05-05; TAO→Base via Chainlink CCIP 2026-08-20. Forge (borrow TAO against alpha) testnet 2026-06-25.

## 2. What distinguishes winners

**Top-20 by mcap (CoinGecko, category total $1.39B, fetched 2026-09-22):** 1 Chutes SN64 $131M; 2 Lium SN51 $127M; 3 Targon SN4 $99M; 4 Score SN44 $63M; 5 Affine SN120 $62M; 6 EfficientFrontier/Engy SN53 $61M; 7 Templar SN3 $55M; 8 Vanta SN8 $47.5M; 9 IOTA SN9 $40M; 10 NOVA SN68 $34M; 11 Gradients SN56 $27M; 12 Hippius SN75; 13 Bitcast SN93; 14 Tiger Alpha SN107 $24M; 15 BitMind SN34; 16 Ridges SN62 $18M; 17 OpenKaito SN5; 18 RedTeam SN61; 19 404-GEN SN17; 20 OpenRoboto SN80. Note: Apex/SN1 and Taoshi/SN8-legacy are NOT in top-10 now; SN8 is Vanta (Prop-Trading rebrand). The avark.agency claim that "Tiger Alpha was deregistered Feb 2026" is CONTRADICTED by SN107 sitting at #14.

**Revenue reality (SubConnect Bittensor Revenue Index, Aug 2026 edition reported 2026-09-21/22):** 24-25 subnets with commercial revenue, $28-35M annualized network-wide (15 "high-confidence" estimates backed by dashboards/on-chain/disclosures); 14 subnets run revenue-funded alpha buybacks (expected 20-25 by YE); compute/infra = 78-82% of verified revenue; external revenue covers ~9-12% of ~$300M/yr emissions; two subnets earn more from customers than from emissions. Named: **Lium SN51 $8-10M**, **Targon SN4 $5.5-6M** (self-reports $10.4M ARR, Manifold Labs $10.5M Series A, Dippy 4-8.6M users, TargonOS H200 $3.29/hr), **Chutes SN64 $4-5M** (Pine Analytics: $1.3-2.4M external vs ~$52M/yr emissions = 22-40:1 subsidy; OpenRouter peak 42B tok/day 2026-02-07, 8-12B/day by March; protocol-level buyback of SN64 alpha tied to usage). The widely quoted "Chutes/Bittensor $43M Q1 real AI revenue" conflicts with all independent estimates — treat as gross/unaudited (UNVERIFIED). Other buybacks: **Synth SN50** $100k over 8 weeks (July 2026, reported Q2 profitability); Bitstarter raises route 50% of TAO to buybacks (Quasar SN24 400 TAO, Djinn 600, Handshake58 220). Clients cited: PwC France, Dropbox, NYSE-listed REIT, Reading FC (Score), DKING/SIRE "$300M AUM" (Score).

**Signals that preceded rises:** (a) verifiable third-party throughput (OpenRouter for Chutes; nineteen.ai latency records); (b) enterprise/consumer integrations announced with named counterparties (Dippy→Targon six-figure deal; Kaito); (c) CEX listing roadmap (Kraken list = 7 names, all top-16); (d) doxxed, VC-backed operators (Manifold Labs, Rayon Labs — which runs SN64/56/19 = 23.7% of emissions, itself a concentration risk); (e) owner-locked alpha/conviction (leaderboard 2026-06-11: MVTRX SN79 1.27M, DSperse SN2 845k, Investing SN88 705k, Bitsec SN60 640k, Desearch SN22 501k); (f) inclusion in Yuma/TAO Institute coverage; (g) surviving the June-22 cut and the v440 gate (being above rank ~32 is now the dominant emission determinant); (h) net TAO inflow (Taoflow) and "Chain Buys" (taomarketcap column). GitHub velocity is cited (chutes-api 1,077 commits; Metanova 451; Zeus 97; Sportsensor 116) but is a weak discriminator alone (see Quasar).

**Collapses & red flags that preceded them:** Covenant (SN3/39/81) — public dispute with core team, moderator timeout, founder open letter, then 37k TAO dump within hours (pre-Conviction, exits were silent). **Quasar SN24** — Novelty Search appearance 2026-07-31 claiming 120B/5M-context SOTA; reproducible audit days later showed 98% inherited weights; −75%; team ousted, owner banned, mining paused; v440 cut its emissions ~86% — despite a 2026-04-30 Adaption Labs (Sara Hooker, $50M seed) partnership announcement, i.e. big-name partnership ≠ validation. **SN281 "LOL subnet"** (2025) — miners ran no code, validator scored by alpha holdings; reached #7 mcap before OTF intervention. **ORO SN15 / Connito SN102** (2026-07-13) — owner wallets social-engineered (147k alpha dumped over 10h; 2,500 TAO), attributed to DPRK; coldkey swap + multisig post-mortem 2026-07-21. 57 subnets zero-emission on 2026-06-22 for "no miner mechanism or no code." Templar carried $98M mcap with zero external revenue (Pine). Score's product domain dking.bot had an expired SSL cert (ownyourmind) — a cheap automated check.

## 3. Existing rating/health efforts

- **TAO Institute SRI (app.taoinstitute.io/sri)** — launched 2026-04-15 (0xSammy + Jack Ai-Leung); open-source; daily; 4 dimensions (Emission Viability, Market Structure, Economic Sustainability, Governance & Operations), 12 on-chain metrics, Green/Amber/Red tiers; plus sell-side-style analyst coverage and monthly operator interviews. Page returned 503 — metric list UNVERIFIED.
- **SubConnect Bittensor Revenue Index** (monthly from Aug 2026) — revenue estimates with confidence tiers; URL not found.
- **Yuma (DCG) "State of Bittensor"** (2nd annual, Mar 2026) — Total Subnets Price (TSP) 1.18; top-20% of subnets = 55% of TSP; subnet mcap = 27% of TAO mcap; qualitative highlights (Ridges, Score, Targon, Yanez).
- **taostats.io** — official explorer; deregistration/pruning rank table; conviction table (s3.hippius.com/rufus/public/conviction-table/index.html); API via taostats.io/pro (docs.taostats.io; tiers/rate limits UNVERIFIED — 403 on fetch).
- **taomarketcap.com** — columns: Emission, Price, 1h/1d/7d, Chain Buys, Mcap, Circ. Supply, Owner Lock, Total Lock; has API Doc.
- **backprop.finance** — screener, dTAO terminal, TNV/FDV/age/emissions; no published score.
- **tao.app** (Latent Holdings) — metrics tracker (403 on fetch; fields UNVERIFIED).
- **subnetradar.com/swaps** — coldkey swaps & registrations feed (e.g., SN117 & SN47 owner swaps 2026-09-21, SN25 2026-09-07).
- **Pine Analytics** (bear case 2026-03-23) — subsidy-ratio method: daily emission $ ÷ daily throughput vs market price/M tokens.
- **Maymin arXiv 2603.29751** — factor model (size premium).
- **BlueTAO.ai** (James Altucher) — inference benchmarks (Chutes vs Engy).
- **CoinGecko** "bittensor-subnets" category; **subnetalpha.ai** — descriptions only, no links/scores, often stale.
No one publishes a composite 0-100 combining GitHub + socials + on-chain + revenue; SRI is the closest (on-chain only).

## 4. KOLs / official channels (handles verified via search unless marked)

@opentensor, @bittensor_, @const_reborn (Jacob Steeves), @taostats, @mogmachine (Taostats founder), @MacrocosmosAI, @YumaGroup (@yuma_ai UNVERIFIED), @DCGco, @BarrySilbert, @xtaohq, @latentholdings, @taoapp_ (UNVERIFIED), @0xSammy (89k; TAO Institute; "The Agentic Future" newsletter), @KeithSingery (Bittensor Guru podcast; Discord mod; validator), @markjeffrey (Stillcore Capital; TWiST guest; UNVERIFIED handle), @taomedia_, @learnbittensor, @TAOFlows, @TrustedStake, @here4impact (Michael D. White), @CryptoZPunisher, @bittingthembits (Andy ττ resource list), @tzedonn, @manifoldlabs, @taoshiio, @webuildscore, @_redteam_, @gtaoventures, @CrucibleLabs, @QuasarModels, @JesusMartinez, @proofoftalk, @BSCNews, @TAOCommunityHub, TaoPortal (handle UNVERIFIED), James Altucher (UNVERIFIED handle). Discord: official Bittensor Discord hosts Const's announcements and the weekly **Novelty Search** community call (YouTube playlist exists); invite link UNVERIFIED. Telegram: UNVERIFIED. Newsletters/podcasts: A Bittensor Journey (monthly "Navigating Bittensor"), Subnet Edge (paid Substack), SimplyTao, tao.media, Own Your Mind, bex.co, ourcryptotalk, Bittensor India, Revenue Search: Inside Bittensor (Spotify), Manifold Labs Substack, Pine Analytics Substack, TWiST.

## 5. Red-flag taxonomy (observed patterns → detectable signals)

1. **Owner extraction / silent exit** (Covenant): owner coldkey alpha→TAO swaps, unlock txs post-Conviction, zero owner-locked alpha, TO-OWNER lock = 0. 2. **Ghost/no-code subnet**: no miner distribution, empty/forked repo, zero-emission flag on weekly Monday list, EMA price→0, pruning rank top-10. 3. **Fake ML claims** (Quasar): unverifiable SOTA claims, weights near-identical to open models, no reproducible eval; partnership PR without artifacts. 4. **Holdings-based scoring / meme mechanics** (SN281): validator code weights by token balance. 5. **Weight-copying validators**: mitigated by commit-reveal; detect via validator vTrust dispersion. 6. **Owner swaps/coldkey swaps** (subnetradar feed) esp. right after immunity; multi-owner "extractor" operators. 7. **Key compromise** (ORO/Connito): sudden large owner-wallet outflows. 8. **Emission-subsidy dependence**: revenue/emission ratio <5%, no buyback. 9. **Team concentration**: one operator controlling multiple top subnets (Rayon 23.7%). 10. **Governance dispute with core team** preceding exit. 11. **Tail position under v440** (rank >32 → emissions 1-5% of linear). 12. **Cheap hygiene fails**: expired SSL, dead website, stale Discord milestones, X silence >30d.

## Data sources catalog

| Name | URL | What it gives | Access | Reliability |
|---|---|---|---|---|
| Taostats API | https://docs.taostats.io/ | Official explorer API: subnets, alpha prices/EMA, emissions, validators, stake flows, identities, deregistration/pruning rank, conviction table (also at s3.hippius.com/rufus/public/conviction-table/index.html) | API key via taostats.io/pro; tiers/rate limits UNVERIFIED (docs returned 403); 'Get API usage' endpoint exists | Canonical on-chain source; used by most other dashboards. Best for EMA price, pruning rank, owner/coldkey and stake data. |
| TaoMarketCap | https://taomarketcap.com/ | Subnet table: emission, price, 1h/1d/7d, Chain Buys, mcap, circulating supply, Owner Lock, Total Lock, sparkline; validators APY | Free web; 'API Doc' and developer dashboard exist, pricing UNVERIFIED | Owner Lock / Total Lock columns map directly to the Conviction signal; Chain Buys proxies net inflow. |
| TAO Institute Subnet Risk Index (SRI) | https://app.taoinstitute.io/sri | Daily Green/Amber/Red structural risk score per subnet across 4 dimensions and 12 on-chain metrics; analyst coverage | Open-source methodology; web app; page returned 503 during research | Closest existing analogue to TaoSentinel's on-chain pillar; on-chain only (no GitHub/social). |
| SubConnect Bittensor Revenue Index | UNVERIFIED (reported via cryptobriefing.com and panews.io) | Monthly per-subnet external revenue estimates with confidence tiers; buyback program list (14 subnets Aug 2026) | Unknown; likely PDF/newsletter | First systematic revenue index; 15/24 high-confidence; still partly self-reported. |
| SubnetRadar coldkey swaps | https://subnetradar.com/swaps | Feed of subnet owner coldkey swaps and new registrations with block numbers and old/new owner addresses | Free web; no advertised API (scrapeable) | Direct detector for owner-swap red flag. |
| Backprop Finance | https://backprop.finance/ | Subnet screener, dTAO trade terminal, TNV/FDV/age/emissions, leaderboards, portfolio | Free web; API UNVERIFIED | Good for volume and screener; no published health score. |
| tao.app (Latent Holdings) | https://www.tao.app/subnets/24 | Per-subnet metrics pages | Web; returned 403 to automated fetch (bot-blocked) | Fields UNVERIFIED; owned by Latent Holdings which also controls the SDK (Latent 11). |
| CoinGecko Bittensor Subnets category | https://www.coingecko.com/en/categories/bittensor-subnets | Alpha token prices, mcap, 24h/7d/30d changes, category total mcap | Free web; CoinGecko API (free tier with rate limits, Pro paid) | Mcap uses circulating alpha; convenient cross-check vs taostats. |
| OpenRouter provider stats | https://openrouter.ai/ | Third-party throughput (tokens/day) for Chutes and other inference providers | Free web/API | Only independent usage signal for inference subnets; Chutes peaked 42B tok/day 2026-02-07. |
| subtensor GitHub (RaoFoundation/subtensor; opentensor/subtensor) | https://github.com/RaoFoundation/subtensor | Runtime spec versions, PRs for baskets/torsion/emission gate, migrations, parameter defaults | Free; GitHub API 60 req/h unauth, 5,000/h with token | Authoritative for rule changes that alter scoring assumptions. |
| Bittensor SDK/btcli (latent-to/bittensor, latent-to/btcli) | https://github.com/latent-to/bittensor | Python SDK v10+ for direct chain queries (subnet info, stake, owner, hyperparams) via own RPC node | Free; needs archive/RPC endpoint (own node or paid provider) | Repos moved from opentensor org on 2026-04-02; single-maintainer concentration. |
| subnetalpha.ai | https://subnetalpha.ai/ | Per-subnet name, team, one-line description, profile page | Free web | No external links/scores; admits data can be stale; several 'Unknown' entries. |
| A Bittensor Journey (Navigating Bittensor monthly) | https://www.abittensorjourney.com/ | Monthly dated digest of protocol upgrades, subnet events, listings, scandals | Free Substack | Best single chronological source found; secondary. |
| tao.media | https://www.tao.media/ | News, conviction leaderboard, hotfix explainers, deregistration coverage | Free; X @taomedia_ | Ecosystem-aligned outlet; verify against chain. |
| Yuma (DCG) reports | https://www.theblock.co/news/web3/2026-03-05-dcg-yuma-bittensor-report-392351 | Annual State of Bittensor; TSP metric; Root Reborn critique; Yuma Total Market Fund | Free PDFs | Yuma is a top-3 validator and fund manager: informed but conflicted. |
| Own Your Mind | https://ownyourmind.ai/tokenomics/bittensor-subnets-where-the-revenue-is/ | Cross-sourced revenue rankings, GitHub commit counts, dates, contradictions flagged | Free | Aggregator; useful as a fact map, verify primaries. |
| Pine Analytics bear case | https://pineanalytics.substack.com/p/the-bear-case-for-bittensor-tao | Subsidy-ratio methodology, revenue vs emission estimates | Free Substack | Bearish; methodology reusable for revenue/emission ratio metric. |
| arXiv 2603.29751 (Maymin) | https://arxiv.org/abs/2603.29751 | Factor model of alpha returns; size premium; slippage constraints | Free | Peer-review status unknown; useful for backtest design. |
| Kraken listings roadmap | https://blog.kraken.com/product/new-features/bittensor-subnet-tokens | Which alpha tokens are on CEX roadmap / live (Vanta SN8 live 2026-07-16) | Free | Listing = strong liquidity/legitimacy signal. |
| SEC EDGAR (GTAO, TAOX) | https://www.sec.gov/Archives/edgar/data/0002029297/000202929726000006/gtao-20260630.htm | Audited TAO holdings of Grayscale trust and TAO Synergies; ETF filing status | Free; EDGAR full-text search API | Only audited numbers in the ecosystem. |
| Bittensor Discord (Novelty Search weekly call; Const announcements) | UNVERIFIED invite (official Bittensor Discord) | Weekly Monday emission-pause list, Const posts, subnet announcement channels, Novelty Search demos | Free; requires user's Discord account/bot token; scraping subject to Discord ToS | Primary source for the weekly dead-subnet list and team milestone posts. |
| X/Twitter KOL set | https://x.com/const_reborn | Announcements from @opentensor, @const_reborn, @taostats, @MacrocosmosAI, @YumaGroup, @xtaohq, @latentholdings, @0xSammy, @KeithSingery, @taomedia_, @learnbittensor, @TAOFlows, @TrustedStake, @CryptoZPunisher, @here4impact, @bittingthembits, @tzedonn, @manifoldlabs, @taoshiio, @webuildscore, @_redteam_, @QuasarModels | X API v2 Basic ~$100/mo (read tweets) or Pro $5k/mo; free tier is write-only — UNVERIFIED current pricing | Fetching x.com directly returned HTTP 402; plan for paid API or alternative (RSS bridges, Nitter mirrors unreliable). |

## Sources

- https://ownyourmind.ai/tokenomics/bittensor-subnets-where-the-revenue-is/
- https://panews.io/articles/01a0c6e8-30d0-7272-ac2f-9195bbaa5e50
- https://cryptobriefing.com/bittensor-subnets-annualized-revenue/
- https://hackernoon.com/bittensor-cleans-house-inactive-subnets-to-no-longer-get-weekly-emissions-says-const
- https://www.tao.media/bittensor-ceases-emissions-for-dozens-of-inactive-subnets-in-cleanup-push/
- https://www.bittensor.com/releases/v440-upgrade
- https://www.abittensorjourney.com/p/navigating-bittensor-august-2026
- https://www.abittensorjourney.com/p/navigating-bittensor-july-2026
- https://www.abittensorjourney.com/p/navigating-bittensor-june-2026
- https://www.abittensorjourney.com/p/navigating-bittensor-may-2026
- https://github.com/RaoFoundation/subtensor/pull/3178
- https://github.com/RaoFoundation/subtensor/pull/3182
- https://bex.co/blog/2026/04/23/latent-11-bittensor-core-development-handoff-opentensor-decentralization
- https://www.tao.media/covenant-ais-bittensor-exit-what-happened-how-bittensor-responded-and-whats-next-for-the-network/
- https://coinmarketcap.com/top-stories/69dd25af14cadf4840f348f2/
- https://simplytao.ai/blog/jacob-steeves-steps-down-as-opentensor-ceo
- https://www.tao.media/bittensors-hotfix-ends-subnet-owners-free-alpha-at-registration/
- https://www.tao.media/which-bittensor-subnets-have-locked-alpha-the-full-conviction-leaderboard/
- https://simplytao.ai/blog/bittensor-locked-stake-the-conviction-mechanism-explained
- https://docs.taostats.io/docs/conviction
- https://www.globenewswire.com/news-release/2026/04/15/3274675/0/en/TAO-Institute-Launches-Research-Platform-to-Accelerate-Institutional-Capital-Formation-in-Bittensor.html
- https://app.taoinstitute.io/sri
- https://www.theblock.co/news/web3/2026-03-05-dcg-yuma-bittensor-report-392351
- https://www.coindesk.com/tech/2026/06/17/a-new-bittensor-proposal-would-turn-validators-into-something-like-fund-managers
- https://crypto.news/bittensor-validator-warns-root-reborn-proposal-carries-substantial-risks/
- https://arxiv.org/abs/2603.29751
- https://pineanalytics.substack.com/p/the-bear-case-for-bittensor-tao
- https://www.coingecko.com/en/categories/bittensor-subnets
- https://taostats.io/subnets
- https://taomarketcap.com/
- https://backprop.finance/
- https://subnetradar.com/swaps
- https://subnetalpha.ai/
- https://blog.kraken.com/product/new-features/bittensor-subnet-tokens
- https://cryptobriefing.com/kraken-lists-bittensor-subnet-alpha-tokens/
- https://www.sec.gov/Archives/edgar/data/0002029297/000202929726000006/gtao-20260630.htm
- https://www.sec.gov/Archives/edgar/data/0001571934/000110465926095966/taox-20260630x10q.htm
- https://bex.co/blog/2026/05/02/grayscale-bitwise-tao-etf-filings-decentralized-ai-token-wall-street
- https://www.techi.com/spot-tao-etf-bittensor-subnet-exposure-gap/
- https://www.kucoin.com/news/flash/bittensor-launches-taoflow-restructures-emission-model-around-real-time-staking-flows
- https://www.kucoin.com/news/flash/bittensor-halving-in-december-2025-could-cut-tao-emissions-in-half
- https://research.grayscale.com/reports/bittensor-on-the-eve-of-the-first-halving
- https://docs.learnbittensor.org/subnets/subnet-deregistration
- https://learnbittensor.org/concepts/tokenomics/miner-burn
- https://www.chaincatcher.com/en/article/2173294
- https://www.coingecko.com/learn/what-is-bittensor-tao-decentralized-ai
- https://x.com/proofoftalk/status/1930634404084265284
- https://x.com/const_reborn/article/2068760333787808043
- https://x.com/latentholdings
- https://x.com/CryptoZPunisher/status/2085087113695952915
- https://x.com/here4impact/status/2085122199602470928
- https://x.com/learnbittensor/status/2043629276692402561
- https://x.com/twistartups/status/2043856152782385387
- https://podcasts.apple.com/us/podcast/bittensor-guru/id1715552186
- https://www.youtube.com/playlist?list=PLN0WJc6io8zqEknIutfyEjiJViazrxrzU
- https://www.tao.media/the-ultimate-guide-to-bittensor-2026/
- https://www.kucoin.com/news/insight/TAO/69f41c952f8d6c0007318445
- https://www.mexc.com/news/1056144
- https://avark.agency/learn/how-to-build-on-bittensor-in-2026-the-complete-guide-to-launching-on-the-decentralized-ai-network
- https://subnetedge.substack.com/p/bittensor-update-network-cleanup
- https://www.0xsammy.com/p/the-agentic-future-040726-bittensor
- https://www.tao.media/kraken-adds-bittensor-subnet-tokens-to-exchange-listings-roadmap/
- https://x.com/BSCNews/status/2051055448254554558

## Open questions

- SEC outcome on Grayscale GTAO / Bitwise spot TAO ETF (decision expected Aug 2026): approved, delayed, or withdrawn? No source found as of 2026-09-22.
- Exact 12 on-chain metrics and weights in TAO Institute's SRI (page 503'd) — worth replicating/ingesting rather than re-inventing.
- Taostats API pricing tiers and rate limits (docs 403'd); whether free tier suffices for 128 subnets polled hourly.
- Is the 128→256 subnet cap live on mainnet (spec 459) or pending a runtime upgrade? Which spec is Finney on today and is BasketTradingEnabled still off?
- Authoritative list of the 57 subnets zero-emissioned on 2026-06-22 and which were reinstated (tao.media's list conflicts with current top-20).
- Which 14 subnets run revenue-funded buybacks (SubConnect index) — names and on-chain buyback addresses to monitor.
- Reconcile Chutes revenue: $1.3-2.4M (Pine) vs $4-5M (SubConnect) vs '$43M Q1' (press) — what does each measure?
- Verification of Nvidia $420M / Polychain $200M TAO positions (no on-chain proof published).
- Was 'Tiger Alpha' (SN107, now #14) ever deregistered in Feb 2026, or did avark.agency confuse a different subnet?
- Current status of Basilica SN39 and Grail SN81 after Covenant's exit and Conviction onboarding (new owners? emissions?).
- Official Bittensor Discord invite and Telegram channels; whether Novelty Search publishes transcripts/recordings systematically.
- X API tier needed for ~40 accounts + subnet team accounts; alternative if user's X access is read-limited.
- Does tao.app expose a public API or per-subnet 'rating' fields (fetch blocked)?
- Post-v440: what is the current 'bar' rank and which subnets sit just above/below it (largest emission cliff)?
