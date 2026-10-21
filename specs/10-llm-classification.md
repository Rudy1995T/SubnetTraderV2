# 10 — LLM classification pipeline and the Narrative/Sentiment pillar (P6)

**Status:** proposed (reconciled with 02 and reviewed 2026-09-23; pre-build review + model re-verification 2026-10-18) · **Phase:** 9 · **Binding tie-breaker:** `02-cross-cutting-conventions.md` (ids B1…J2 cited inline win over this text) › master › this spec for P6 and the classification pipeline (02 §A2, §C4) · **Depends on:** 01 (runtime, `JOB_TABLE`, `CollectorJob`, `/health`, `ProviderLimiter`, `app/util/time.py`), 03 (owns **all** DDL this spec reads or writes, the events catalog §3.9 and the metric-key catalog §3.8; the text-retention policy `nlp_text_retention` — this spec ships no DDL, B7), 04 (registry/aliases, `occupants`), 05 (`chain_trending_daily`), 08 (`announcements`, `app/nlp/textnorm.py`), 09 (X/KOL/RSS/YouTube/news collectors, `bot_filtered`, `cap_author_share`), 11 (review-queue domain functions, `manual_flags` hand-off, D5), 12 (producer-fn contract C5, feature engine, registry rows 42–45, scoring), 14 (routes/Telegram that call 11's review functions), 16 (cost governor G5, kill switches, pause groups) · **Master spec refs:** §1.3, §3, §4 (tier 1 Anthropic), §5, §6, §7.1, §7.3 (P6, `flow_attention_blend`), §7.4, §7.5, §7.7, §8 (row "LLM classification"), §11, §12 (Phase 9), §13 (decision 4).

---

## 1. Purpose and scope

This spec turns unlabelled text items (X posts, crossposted Discord announcements and Telegram channel posts, RSS/news items, YouTube video metadata) into **point-in-time, versioned labels** (`post_labels`), materialises **events** from those labels, and provides the four **P6 producer fns** (02 §C4/§C5, `source='llm'`) that feed Velocity (master §7.1: `Velocity = 100·[0.6·P6 + 0.4·flow_attention_blend]`); spec 12 calls them and writes the rows. It also runs a weekly narrative job whose output is *display-only* (never a scoring input).

**In scope**
- Input queue selection, privacy/ToS gating, text assembly, item packing (20 items per request).
- The classification rubric (system prompt, ≥ 600 tokens), the per-item JSON output schema, entity linking against a per-batch alias table from the registry.
- Anthropic **Batch API** job lifecycle (submit every 6 h, poll, download, partial results, retries, resume after power loss).
- **Escalation** of materiality-3 negatives and `owner_extraction_evidence` / `security_incident` to `claude-sonnet-5-5` (single call), both labels stored, Sonnet wins.
- Weekly `claude-opus-5-5` narrative for tier A/B + top movers → `manual_notes(kind='llm_narrative')`.
- `classifier_version` rule, label-resolution precedence pinned by `spec_version`, **no in-place relabelling ever**.
- Rule-based fallback classifier (`rules:v2`) when `LLM_ENABLED=false`, kill switch, budget exhausted, or LLM stalled.
- Cost-governor integration exactly per 02 §G5 (per-batch estimate → `reserve` before submit → `commit` after download → `release` on submit failure; `check` + `record` for the unreserved Sonnet/Opus calls); the 80 %/100 % alerts are 16's `spend_80pct`/`spend_cap_reached`.
- Event materialisation (materiality ≥ 2; the 16 taxonomy `event_type`s of 03 §3.9) and `review_queue` rows of kind `llm_flag_candidate`; resolution runs through 11's domain functions (02 §A2, §D5).
- P6 producer fns exactly per master §7.3 and 02 §C4/§C5: `compute_event_impact_14d`, `compute_polarity_7d`, `compute_attention_7d`, `compute_kol_coverage_30d` (spec 12 calls them and writes the `stage='raw'` rows); `attention_7d` is also a `flow_attention_blend` input.
- Evaluation harness (200-item golden set, per-event-type precision/recall, drift check on rubric change).
- Labelling **pending-classification text** (02 §B10 amended) inside the normal 6-h batch, so it is labelled before 03's retention purges it.

**Out of scope**
- Collecting the items (spec 09 for X/KOL/RSS/YouTube/news; spec 08 for announcements).
- All DDL (03, 02 §B7) and text retention: 03's `storage_retention_daily` policy `nlp_text_retention` NULLs text (hash kept) after `LLM_TEXT_RETENTION_DAYS`, and pending text once a `post_labels` row exists or `text_purge_after` passes (02 §B10). This spec has no retention job.
- The normaliser implementations, pillar aggregation and the `metric_values` writes (spec 12); this spec's fns return `MetricResult`s and state the normaliser each one expects.
- Red-flag multipliers and the flag state machine (spec 12), `manual_flags` writes (11/14, 02 §D5) and owner-analytics detectors (spec 11) — this spec only feeds them labels, events and review-queue rows.
- Telegram/HTTP surfaces (spec 14) — they call 11's review functions, which call back into this spec (§8.2).
- Any local ML model (CryptoBERT etc.). Research 01-web-quant-scoring-methods §3 shows F1 ≈ 0.55 on noisy crypto text; the rules fallback is deliberately simple and cheap instead.

---

## 2. Design

### 2.1 Components and data flow

```
 social_posts ─┐                                            ┌─► post_labels (bulk, classifier_version A)
 announcements ┼─► queue.select() ─► pack 20/request ─► Anthropic Batch API (Haiku 5.5) ─► download ─┤
 news_items    │        │  privacy gate, bot_filtered=0,          every 6 h · poll 5 min                │  escalate?  ──► Sonnet 5.5 single call ─► post_labels (escalation, version B)
 video_items ──┘        │  dedup by text_hash, lookback 3 d,                                             │
                        │  pending text (text_purge_after) first                                         └─► events (materiality ≥ 2, dedupe 48 h) ─► review_queue (llm_flag_candidate) ─► 11 resolve_review ─► manual_flags (D5)
                        └─► rules.py (rules:v2) when LLM off / budget out / stalled ─► post_labels (fallback)
                                                                                                 │
 registry (subnets, occupants, watch_channels, manual_notes alias) ─► aliases.py ─► alias table   │
                                                                                                 ▼
 scores (tier, movers) + events + labels + metrics ─► narrative.py (Opus 5.5 weekly) ─► manual_notes(kind='llm_narrative')
                                                                                                 │
 resolve.py (LABEL_POLICY pinned in scoring/spec.py) ─► features/sentiment.py compute_<key>(db, occupant_key, as_of) ─► 12 compute_registered ─► metric_values (stage='raw', source='llm')
```

One asyncio process; every step is a `JOB_TABLE` job (§6.1; `max_instances=1`, 01's `CollectorJob` wrapper: swallow + log + `collector_runs` + `collector_failing` after repeated failures). All Anthropic calls use the official `anthropic` SDK (`AsyncAnthropic`) — the one sanctioned caller outside `Http` (02 §G1/§G5): each call first takes a token from 01's `ProviderLimiter` bucket `anthropic` (`await limiter.acquire("anthropic")`) and its spend/requests are booked through 16's cost governor (§6.3).

### 2.2 Algorithms

**Item ids** (02 §B11; = `post_labels.post_id`): X `x:{id}` (= `social_posts.item_id`), Discord `discord:{source_message_id}` and Telegram `telegram:{handle_lower}/{n}` (= `announcements.msg_id`), news `news_item:{news_items.url_hash}`, video `video_item:{video_items.video_id}`. No `social_post:`/`announcement:` prefix exists anywhere. All times are INTEGER unix seconds (02 §B1).

**Queue selection** (`nlp/queue.py`): an item is eligible when (a) it has classifiable text — `social_posts.text` (X); `announcements.text` (Discord crossposts received in the operator's aggregator server, and Telegram posts — including **pending-classification text** written with `text_purge_after` while the platform's `*_STORE_TEXT` flag is false, 02 §B10); `title + summary` for news; `title + description` for videos — (b) `event_at ≥ now − LLM_LOOKBACK_DAYS·86400`, (c) no `post_labels` row exists for the *current bulk* `classifier_version` (a `rules:v2`-only item stays eligible while its text exists; pending text is purged at the next 03:00Z retention run once *any* label exists, so a rules label on pending text is effectively final), (d) privacy gate passes (§2.5), (e) X posts: `bot_filtered = 0` (09 §2.7: `author_credibility < X_MIN_CREDIBILITY` is never classified), (f) `attempt < 3`. Items with identical `text_hash` (08's `app/nlp/textnorm.py`, B11; news uses 09's title+summary hash) inside one cycle are sent once and the label is copied to the duplicates (`raw_json.dedup_of = post_id`). Ordering: pending-classification rows first (earliest `text_purge_after`), then team + KOL, then engagement desc, then `event_at` desc; cap `LLM_BATCH_MAX_REQUESTS × LLM_BATCH_ITEMS_PER_REQUEST` per cycle, remainder waits.

**Packing**: `LLM_BATCH_ITEMS_PER_REQUEST = 20` items per request (validator 5–40). Each request = shared system prompt (rubric + schema reminder + alias table) + one user message holding the items in `<item>` tags (§2.4). Inside a request items are ordered by `post_id` and the prompt `id` is the 1-based position (`"1"…"20"`); the parser maps positions back through the request's `llm_batch_items` rows (sorted by `post_id`), so 64-hex news ids are never echoed or mistyped. `max_tokens = 8192` (Haiku 5.5 thinks adaptively; thinking counts toward the cap, §2.3), `output_config.effort = BULK_EFFORT`. On a parse failure the request's items are re-queued once at 10 items per request, then fall back to rules.

**Escalation** (`nlp/llm_classify.py::escalate`, run inside `llm_batch_poll` right after a batch is applied, while pending text still exists): select resolved bulk labels where `(direction = −1 AND materiality = 3) OR event_type IN ('owner_extraction_evidence','security_incident')`, not yet labelled by the current *escalation* version, within `LLM_ESCALATION_MAX_PER_DAY` (overflow → review queue only). Before each call `governor.check("anthropic", est_usd)`; a denial (`kill_switch`, `hard_cap`, `zero_cap`) leaves the label pending **without** consuming one of its 3 tries. One `messages.create` per item on `claude-sonnet-5` with the same rubric, single-item schema, independent re-assessment (the Haiku label is **not** shown, to avoid anchoring). Both rows stay; `resolve.py` prefers the escalation version. A label still unescalated `LLM_FALLBACK_AFTER_H` after its bulk `observed_at`, or whose text has been purged, goes to the review queue (the operator opens `url`).

**Label resolution** (`nlp/resolve.py`): `LABEL_POLICY` is a frozen part of `scoring/spec.py` for `ts-1.0` (build note 2026-10-18: a module-level `LABEL_POLICIES = {"ts-1.0": LabelPolicy(...)}` beside 08's frozen constants, **not** a `ScoringSpec` field — adding a field would change 12's pinned `GOLDEN_SPEC_HASH`; the rubric-hash guard test of §9.3 protects it instead): an ordered list of accepted `classifier_version`s per tier (`escalation`, `bulk`, `fallback`). `resolve(post_id, as_of)` returns the highest-precedence row whose `observed_at ≤ as_of`. A rubric/schema/model change produces new version strings → new rows → a new `spec_version` (`ts-1.1`) pins them. Old `metric_values`/`scores` are never recomputed under the new labels, and no label row is ever updated (03's trigger aborts `UPDATE`/`DELETE` on `post_labels`). `submit_cycle` refuses (`skipped: version_unpinned`) when the computed bulk version is not in `LABEL_POLICY.bulk` of `SPEC_VERSION` or of any `SCORING_SHADOW_SPECS` entry, so no money is spent on labels no scoring spec reads.

**Events** (`nlp/events.py`): from resolved labels with `materiality ≥ 2` and at least one entity with `confidence ≥ LLM_ENTITY_MIN_CONFIDENCE`; one row per cluster `(occupant_key, event_type, direction, floor(event_at / 172800))` with `dedupe_key = f"{occupant_key}|{event_type}|{event_at // 172800}|{d}"`, `d ∈ {pos, neg, neu}` (the "topic" slot of 03 §3.9's pattern; `event_at` = the earliest label's); materiality-3 negatives are written only when the resolved label is an escalation-tier row or a human confirmed the review item (master §7.4: "before counting"). Events are notifications and UI/narrative evidence; `event_impact_14d` is computed from the resolved labels themselves (§3.4), never from `events` rows, whose payload may still change before `alerted_at` (03 §2.5).

**P6 metrics** (`features/sentiment.py`): §3.4 gives the exact formulas. `as_of` is spec 12's `T_eval` as a tz-aware UTC `datetime` (02 §C5); every computation reads only label rows with `observed_at ≤ as_of` and item rows with `observed_at ≤ as_of` (PIT by construction).

### 2.3 Models, ids, prices (re-verified 2026-10-18 at build time against the bundled `claude-api` skill v2.1.301, model table cached 2026-10-06, and the installed SDK `anthropic==1.15.0`)

The 2026-09-23 table pinned `claude-haiku-4-5-20251001` / `claude-sonnet-5` / `claude-opus-5`. Since then the current generation is Haiku 5.5 / Sonnet 5.5 / Opus 5.5 — cheaper or equal per token — and nothing has been labelled yet (live `post_labels` = 0), so the defaults move to it now at no relabel cost. The older ids stay valid config values; changing a model changes the version string (§2.2) and needs a new `LABEL_POLICY` entry.

| Role | Model id | Input / output $ per MTok | Request rules (all from the skill) |
|---|---|---|---|
| Bulk classification | `claude-haiku-5-5` — a fixed id with no date suffix and no separate alias, so the reason for pinning a dated id (a snapshot roll must not change labels under one `classifier_version`) still holds | $0.10 / $0.50 for prompts ≤ 100K tokens ($0.50 / $2.50 above — never reached: a request is ≈ 8–10K); Batch API −50 % → **$0.05 / $0.25**; cache read 0.1×, 5-min write 1.25× | 1M context, 128K output. **Adaptive thinking is on by default**; thinking counts toward `max_tokens` and bills as output → send `output_config.effort = BULK_EFFORT = "low"` explicitly (Haiku 5.5's default is `medium`; `low` is the skill's level for high-volume classification) and keep thinking adaptive (the skill's lever is effort, not `{"type": "disabled"}`); `max_tokens = 8192` so thinking cannot crowd out the 20-item JSON. Read content blocks by `type` (a response can start with `thinking` blocks). No `temperature`/`top_p`/`top_k` (400), no prefill (400). Min cacheable prefix **512** (Haiku 4.5: 4,096) → the ≈ 4K-token rubric + alias prefix can now cache (best-effort inside a batch). Tokenizer ≈ +30 % vs Haiku 4.5 → system tokens are counted with this model id. Safety classifiers may refuse (`stop_reason: "refusal"`); Haiku 5.5 has **no server-side fallback** and `fallbacks` is rejected on the Batches API anyway → a refused request is a parse failure (§7: requeue at 10/request once, then rules). `BULK_EFFORT` is a frozen constant in `nlp/rubric.py`, not config: changing it means bumping `LLM_SCHEMA_V` |
| Escalation | `claude-sonnet-5-5` | $2 / $10 (as Sonnet 5) | adaptive thinking on by default; `output_config.effort = "medium"` sent explicitly (Sonnet 5.5's default is `high`; levels recalibrated); never send `thinking` (`{"type":"disabled"}` is a 400); forced `tool_choice` is a 400 (not used); safety classifiers in five categories → **server-side refusal fallback on by default**: `client.beta.messages.create(..., betas=["server-side-fallback-2026-07-01"], fallbacks="default")` (`LLM_REFUSAL_FALLBACK`; the `fallbacks` parameter is in the SDK 1.15 signature); `model_resolved = message.model` records who answered. Min cacheable prefix 512 → the shared prefix caches across the sequential calls of one poll run (5-min TTL) |
| Weekly narrative | `claude-opus-5-5` | $4 / $20 (Opus 5 was $5 / $25) | thinking cannot be disabled (never send `thinking`); effort default `medium`, sent explicitly; `max_tokens = 8000`; same `fallbacks="default"` beta; check `stop_reason == "refusal"` (with `stop_details.category`) before reading content; min cacheable prefix 512 |

Structured outputs: `output_config={"format": {"type": "json_schema", "schema": …}}` (next to `effort`) on `messages.create`, on the beta create and on batch `params`; the skill lists Haiku 5.5, Sonnet 5.5 and Opus 5.5 as supported and structured outputs as working with Batches, streaming and token counting. Schema limits: `additionalProperties: false` on every object; `enum`/`const` (integer enums included) supported; no `minimum`/`maximum`/`minLength`/`maxLength` (ranges and the 140-char summary are checked client-side); array length cannot be constrained. The client-side pydantic validation runs regardless (it also covers a `refusal` or `max_tokens` stop, where the output may not match the schema). Never use assistant prefill and never send `temperature`/`top_p`/`top_k` (all three models reject them).

Batch API facts used here (skill `python/claude-api/batches.md`): ≤ 100,000 requests or 256 MB per batch; most batches end within 1 h, at most 24 h (unfinished requests become `expired`); results downloadable for 29 d after creation; results arrive in **any order** — key by `custom_id`; result types `succeeded | errored | canceled | expired`, where `errored` with `error.type == "invalid_request"` must not be retried unchanged. `messages.count_tokens` counts are model-specific (pass the same model id); its price is not stated in the skill (believed free, rate-limited — UNVERIFIED).

### 2.4 The rubric (system prompt) — file `app/nlp/prompts/classify_v1.md`

Copy verbatim; `rubric_sha256 = sha256(file bytes with CRLF→LF, trailing whitespace stripped)`. The alias table is **not** part of the hashed file (it is a second system block appended per batch). Approx. 1,150 tokens.

```
You are TaoSentinel's classifier for text about Bittensor subnets. Bittensor is a
network of ~130 "subnets" (numbered by netuid, each with an alpha token, an owner
team, miners and validators). You receive up to 40 short items (X posts, Discord
announcements, news headlines, video titles) and must return ONE JSON object per
item, following the schema exactly. Never invent facts; classify only what the text
says. Output JSON only, no prose.

## Fields per item
- id: echo the item id exactly.
- polarity: bullish | bearish | neutral — the stance toward the mentioned subnet(s)
  or, if none, toward Bittensor. Sarcasm counts as its intended stance.
- intensity: 1–5 strength of that stance (1 = barely, 3 = clear, 5 = extreme). Use 1
  when polarity is neutral.
- event_type: exactly one of the taxonomy below; "other" when nothing fits.
- direction: +1 if the event is good for the subnet's prospects, −1 if bad, 0 if
  informational. Must stay inside the allowed set for the event_type.
- materiality: 1 = routine or vague; 2 = specific and notable, would change an
  informed holder's expectations; 3 = major AND verifiable in the text (numbers,
  URLs, tx hashes, named counterparties, dates) or a security/ownership crisis.
- entities: subnets the item is ABOUT, as [{netuid, confidence}]. Rules:
  * "SN64", "subnet 64", "netuid 64", "sn64", "$SN64" → netuid 64 (confidence ≥ 0.9).
  * A name or symbol from the ALIASES table → that netuid, but ONLY if the item is
    clearly about Bittensor (mentions Bittensor, TAO, subnet, SN, alpha, miner,
    validator, emission, netuid, or comes from a Bittensor-scoped source). Generic
    words that are also subnet names (Score, Gradients, Nova, Apex, Templar, Synth,
    Affine, Vanta…) get NO entity without Bittensor context.
  * Bare "#64", prices, or dates are not netuids.
  * If the item is about Bittensor/TAO in general, or you cannot tell which
    subnet, entities = [] . Ambiguity → empty, never a guess.
  * Confidence: 1.0 explicit netuid or cashtag; 0.7–0.9 unambiguous alias with
    context; below 0.6 do not include.
- is_spam: true for bot-like, scam, copy-paste shill, airdrop/"send TAO" scams,
  engagement bait unrelated to the subnet, or non-Bittensor content that only
  keyword-matched.
- is_promo: true for giveaways, contests, referral pushes, "like & RT" campaigns,
  paid promotion; may be true while is_spam is false.
- summary: ≤ 140 characters, factual, names the subnet if known, no hashtags.
- confidence: 0–1 your confidence in event_type + direction + materiality.

## Event taxonomy (allowed direction, materiality guidance)
- product_launch (+1): a usable product/app/API/model goes live or a launch date is
  announced. 3 = live with URL or usage evidence; 2 = dated launch or public beta;
  1 = "coming soon".
- release_upgrade (+1): software version, model update, validator/miner code change.
  1 default; 2 = major version with changelog or breaking mechanism change.
- partnership_integration (+1): integration or partnership. 2 = named counterparty
  with what is integrated; 1 = unnamed or "exploring". Never 3 on the announcement
  alone (partnership PR without artifacts is not validation).
- revenue_customer (+1): paying customers, revenue, ARR, usage volumes from third
  parties. 3 = numbers given (USD, ARR, paying-customer count, tokens/day from an
  independent dashboard); 2 = customer named without numbers; 1 = vague.
- token_buyback_burn (+1): alpha buybacks or burns funded by revenue. 3 = amounts or
  tx references; 2 = program announced; 1 = intention.
- exchange_listing (+1): 3 = tier-1 CEX (Kraken, Coinbase, Binance, OKX, Bybit)
  live listing; 2 = other CEX live or tier-1 roadmap; 1 = DEX/aggregator/bridge.
- funding (+1): equity or token raise. 2 default; 3 if ≥ $10M with named lead.
- emissions_tokenomics_change (0): protocol/subnet economics changes. Always
  direction 0 (informational). 1 default; 2 when the text says THIS subnet's
  emissions or tokenomics changed.
- team_change (−1 exit, +1 hire): founder/core exit, ousting, hand-over −1 mat 2
  (3 if the whole team abandons the subnet); hire/advisor +1 mat 1.
- security_incident (−1): exploit, key/coldkey compromise, funds drained, validator
  attack. 3 = losses or compromised owner keys; 2 = vulnerability disclosed and
  patched without loss; 1 = phishing warning.
- delay_missed_milestone (−1): a publicly promised date/roadmap item slipped or was
  missed. 2 default; 1 minor.
- governance_dispute (−1): public conflict with the foundation/core team/validators,
  moderator action, bans, ousting. 2 default; 3 = owner banned, mining paused, or
  team removed.
- regulatory (−1): enforcement, legal threat. 1 default; 2 if directly against the
  team.
- owner_extraction_evidence (−1, always 3): evidence that the owner is selling
  emissions/stake, unlocking, dumping, or moving alpha out while promoting; large
  owner-wallet outflows. Cite what the text shows in summary.
- giveaway_promo (0, mat 1): set is_promo = true.
- other (0, mat 1).

## Materiality discipline
Big names ≠ proof. A partnership with a famous lab is 2, not 3. Claims of "SOTA"
without an eval link are 1. Do not raise materiality because the tone is loud.
Retweets/reposts of another subnet's news get entities of the subnet discussed.

## Examples (abbreviated)
1. "SN8 Vanta alpha is now live for spot trading on Kraken: kraken.com/…" →
   {polarity:bullish, intensity:4, event_type:exchange_listing, direction:1,
    materiality:3, entities:[{netuid:8, confidence:1.0}], is_spam:false,
    is_promo:false, summary:"SN8 Vanta alpha listed on Kraken spot", confidence:0.95}
2. "We're stepping away from SN3/39/81 effective today. Thanks all." (team account)
   → team_change, direction −1, materiality 3, entities 3, 39, 81 (1.0), bearish 4.
3. "Coldkey of the SN15 owner was socially engineered; 147k alpha dumped over 10h.
   Post-mortem soon." → security_incident, −1, 3, entities [15:1.0], bearish 5.
4. "Synth SN50 buyback week 8: 12.4k alpha bought with $18k API revenue, txs in
   thread" → token_buyback_burn, +1, 3, entities [50:1.0], bullish 3.
5. "Score just dropped 3 goals in the second half" (no Bittensor context) →
   other, 0, 1, entities [], neutral 1, is_spam true (keyword-only match).
6. "🎁 Giveaway! RT + follow + tag 3 friends to win 5 $SN64" → giveaway_promo, 0, 1,
   entities [{netuid:64, confidence:1.0}], neutral 1, is_promo true, is_spam false.
7. "Independent audit shows the SN24 checkpoint is 98% inherited weights; owner
   banned from the Discord, mining paused" → governance_dispute, −1, 3, entities
   [24:1.0], bearish 5.
8. "Targon (SN4) reports $10.4M ARR; Manifold Labs closes $10.5M Series A" →
   revenue_customer, +1, 3, entities [4:1.0], bullish 4 (funding is secondary; pick
   the more material type).

Return: {"schema_v": 1, "items": [ …one object per input item, same order… ]}
```

Second system block (per batch, `cache_control: {"type": "ephemeral"}` on it — on Haiku a bonus only if the total prefix crosses the 4,096-token minimum, never required; batch requests run in parallel, so hits are best-effort anyway; on the sequential Sonnet escalation calls of one poll run the ≈ 4k-token prefix does cache, 5-min TTL):

```
ALIASES (netuid|name|symbol|x_handle|extra). Use for entity linking only.
64|Chutes|CHUTES|@chutes_ai|chutes.ai;Rayon Labs
4|Targon|TARGON|@manifoldlabs|targon.com;Manifold Labs
…
```

Built by `nlp/aliases.py::build_alias_table(db) -> tuple[str, str]` from `subnets` (current occupants only: `netuid, name, symbol`, host of `website_url`), X handles from `watch_channels(platform='x', retired_at IS NULL)`, and manual aliases (the latest `manual_notes(kind='alias')` row per occupant, `value` = `;`-separated aliases). ≈ 20 tokens per line, ~130 lines ≈ 2,000–2,600 tokens. Regenerated per cycle; its sha256 is stored in `llm_batches.alias_sha256` (diagnostic only, not part of `classifier_version`).

User message per request:

```
<items n="20">
<item id="1" platform="x" author_kind="team" source_netuid="64" date="2026-09-20" engagement="likes=120 reposts=30 replies=8">
…text (≤ LLM_MAX_ITEM_CHARS, whitespace-collapsed, URLs kept)…
</item>
<item id="2" platform="news" author_kind="other" source_netuid="" date="2026-09-21" engagement="">
Title
Summary…
</item>
…
</items>
Classify every item. Output JSON only.
```

`id` = the item's 1-based position in the request (§2.2 Packing). `author_kind ∈ {team, kol, other}` (X: 03's generated `social_posts.author_kind`; announcements: `author_role_flag`, `team` for Telegram and team channels); `source_netuid` is filled when the item is source-bound (team X account, own announcement channel, subnet YouTube channel). Discord authors are never named; X handles may appear inside the text but the attribute carries only `author_kind`.

### 2.5 Privacy / ToS rules (binding)

| Source | Sent to the LLM? | Stored text (03's retention policy `nlp_text_retention`, 02 §B10) | Rationale |
|---|---|---|---|
| X posts (team, KOL, mention search), `bot_filtered = 0` | yes | `social_posts.text` 90 d, then NULL, labelled or not (summary kept in `post_labels.summary`) | provider ToS covers reads; X text allowed by master §6 |
| Discord crossposts received in the operator's own aggregator server (Follow) — `announcements.via_own_server = 1`, user ids redacted by 08 | yes | `DISCORD_STORE_TEXT=true` (default): 90 d; `=false`: pending text (`text_purge_after`), NULLed once labelled or after `LLM_PENDING_TEXT_MAX_DAYS` (7 d) | operator's own server; Developer Policy "no training" — inference only |
| Any other Discord text | **never** | never stored — TaoSentinel never reads third-party servers; 03's CHECK forbids Discord text with `via_own_server = 0`, and `social_posts` is X-only (B11) | Developer Policy |
| Telegram `t.me/s` previews (public channels) | yes | `TELEGRAM_POSTS_STORE_TEXT=false` (default): pending text, NULLed once labelled or after 7 d; `=true`: 90 d | public, no auth |
| RSS/news/YouTube titles + summaries | yes | summary/description 90 d | public feeds |

`queue.select()` enforces the table in SQL (`text IS NOT NULL AND (platform = 'telegram' OR via_own_server = 1)` for `announcements`; `bot_filtered = 0` for `social_posts`) and a unit test asserts no forbidden row can reach `build_batch_request()`. The spec's old "90 d Telegram text" allowance is withdrawn (B10 amended).

---

## 3. Data contract

Spec 03 owns every table, column, index and trigger named here (02 §A2, §B7); this spec ships **no DDL** and lists only the columns it **reads (R)** or **writes (W)**, with 03's names (03 Appendix A rows "10" record the renames). Every instant is INTEGER unix seconds (02 §B1).

### 3.1 Read

| Table | Columns needed | Notes |
|---|---|---|
| `social_posts` (R; 09, X only — B11) | `platform` (= `'x'`), `post_id` (native), `item_id` (generated `'x:{id}'` = item id), `author_id`, `author_handle`, `occupant_key` (source-bound team account, else NULL), `is_team_account`, `is_kol`, `kol_handle`, `author_kind` (generated `team\|kol\|other`), `author_credibility`, `bot_filtered`, `text`, `text_hash`, `lang`, `like_count, repost_count, reply_count, quote_count, view_count`, `is_repost`, `in_reply_to`, `url`, `event_at`, `observed_at` | 09 sets `author_credibility`/`bot_filtered` (09 §2.7); engagement is refreshed in place (02 §J2 look-ahead) |
| `announcements` (R; 08) | `msg_id` (= item id, `discord:{source_message_id}` / `telegram:{handle_lower}/{n}`), `platform`, `occupant_key`, `netuid`, `source_channel_id`, `message_id`, `via_own_server`, `author_is_team` / `author_role_flag` (generated), `text`, `text_purge_after`, `text_hash`, `url` (generated), `event_at`, `observed_at`, `deleted_at` | text only for own-server crossposts and Telegram (03 CHECK); pending text per 02 §B10 |
| `news_items` (R; 09 A) | `url_hash` (item id), `title`, `summary`, `url`, `feed_url`, `occupant_keys_json`, `text_hash`, `event_at`, `observed_at` | |
| `video_items` (R; 09 A) | `video_id` (item id), `channel_id`, `occupant_key`, `occupant_keys_json`, `title`, `description`, `url` (generated), `views`, `event_at`, `observed_at` | |
| `social_post_mentions` (R) | `platform`, `post_id`, `occupant_key`, `match_kind`, `observed_at` | label-free attribution for the coverage denominators (§3.4) |
| `kol_accounts` (R) | `handle` (PK), `handle_lower`, `platform`, `author_id`, `credibility`, `verified`, `active`, `team_occupant_keys` | operator-edited state (checklist D3), read as-is (accepted PIT deviation) |
| `subnets`, `occupants` (R; 04) | `netuid`, `occupant_key`, `name`, `symbol`, `website_url`, `status`; `occupants.{network_registered_at, registered_ts, first_seen_at, deregistered_at}` | occupant of netuid `n` at `event_at`: the `occupants` row with `netuid = n` and the greatest `COALESCE(registered_ts, first_seen_at) ≤ event_at`; none → link dropped (`raw_json.unmapped`) |
| `watch_channels` (R) | `occupant_key`, `platform='x'`, `handle`, `retired_at` | alias table |
| `chain_trending_daily` (R; 05) | `day_ts`, `occupant_key`, `views`, `observed_at` | `views = 0` when absent from a successful pull (verifiable); **no row** = every pull that day failed (missing). Replaces the old `chain_daily.tmc_trending_views` |
| `collector_runs` (R) | `job`, `ok`, `finished_at` | `x_kol_poll` recency for `kol_coverage_30d` |
| `events` (R) | own rows (`source='llm'`): `event_type`, `dedupe_key`, `payload`, `alerted_at`, `observed_at`; any `occupant_key` rows of the last 14 d for the narrative | confirmations (`payload.confirmed_by`) are read with `observed_at ≤ t` |
| `scores` (R) | `occupant_key`, `as_of`, `spec_version`, `run_kind`, `composite_smoothed`, `tier`, `pillar_scores`, `explain`, `red_flags`, `computed_at` | narrative selection + evidence |
| `metric_values` (R) | `stage='norm'` rows of the latest official slot (`value` = m, `value_json.raw`) | narrative evidence only |
| `manual_notes` (R) | `kind='alias'` (`value`); the last 3 `kind='note'` rows per occupant | alias table; narrative |
| `job_state` (R/W) | `('llm_classify_submit', 'resv:{batch_key}')` → `{reservation_id, est_usd}` | runtime state only (02 §B12); deleted after commit/release |

### 3.2 Write

No `CREATE`/`ALTER TABLE` here (02 §B7): `post_labels`, `post_label_entities`, `llm_batches`, `llm_batch_items` and `review_queue` are 03 §3.2/§3.5 DDL (STRICT, INTEGER times, CHECKs). Columns this spec writes:

| Table (03 class) | Columns written | Rule |
|---|---|---|
| `post_labels` (append; trigger aborts `UPDATE`/`DELETE`) | `post_id` (§2.2 item ids), `item_kind ∈ {social_post, announcement, news_item, video_item}`, `item_id` (the item table's key: `social_posts.item_id`, `announcements.msg_id`, `news_items.url_hash`, `video_items.video_id`), `classifier_version` (`'{model_id}:{rubric_sha256[:8]}:{schema_v}'` or `'rules:v2'`), `classifier_tier ∈ {bulk, escalation, rules}`, `polarity`, `intensity`, `event_type`, `direction`, `materiality`, `entities_json` (`[{"netuid":64,"occupant_key":"64:5423011","confidence":0.9,"via":"model\|source\|regex"}]`), `is_spam`, `is_promo`, `summary`, `confidence`, `event_at` (copied from the item), `observed_at` (label write time — the PIT key), `source` (`'llm'` bulk/escalation, `'rules'` for `rules:v2`), `run_id`, `batch_key` (NULL for rules/escalation), `model_resolved` (`message.model`), `input_tokens`, `output_tokens`, `cost_usd`, `raw_json` (the model's item object verbatim, + `dedup_of`, `clamped`, `unmapped`) | `INSERT OR IGNORE` on `(post_id, classifier_version)`; a relabel is a new row under a new version, never an update |
| `post_label_entities` (append; child rows) | `post_id`, `classifier_version`, `occupant_key`, `netuid`, `confidence`, `via ∈ {model, source, regex}`, `observed_at` (= the parent label's) | only links with `confidence ≥ LLM_ENTITY_MIN_CONFIDENCE`; lower ones stay in `raw_json` |
| `social_post_mentions` (append) | `platform` (`x`/`discord`/`telegram`), `post_id` (native id: `msg_id` without its platform prefix), `occupant_key`, `match_kind='entity'`, `classifier_version`, `event_at`, `observed_at`, `run_id` | X posts and announcements only, links with `via ∈ {model, regex}` (03 §3.5 and 09 §2.6 expect these rows); `INSERT OR IGNORE` — the first labelling version wins the row |
| `llm_batches` (state) | every column of 03's DDL; `status` walks `building → submitted → ended → applied`, or `failed/expired/canceled` | the restart ledger; `provider_batch_id` = Anthropic `msgbatch_…` |
| `llm_batch_items` (state) | `batch_key`, `custom_id`, `post_id`, `attempt`, `status ∈ {pending, labelled, errored, requeued, fallback}` | maps results back (key by `custom_id`, then position, §2.2); 03 prunes rows 90 d after the batch ends |

Rows written to other tables:

| Table | Columns written | Rule |
|---|---|---|
| `events` (W, via 03's `emit_event`) | `occupant_key`, `netuid`, `event_type` (one of the 16 taxonomy values, 03 §3.9), `severity ∈ {info, warning, critical}` (02 §B2), `event_at` (earliest label of the cluster), `observed_at`, `source='llm'` (02 §B9), `dedupe_key` (§2.2), `run_id`, `payload` `{post_ids[], label_version, direction, materiality, summary, url, confidence, n_supporting, confirmed_by?}` (03's required keys, 02 §D1/§D2) | severity: positive/neutral → `info`; negative mat 2 → `warning`; negative mat 3 confirmed (Sonnet or human) → `critical`, which 14's `alert_dispatch` pages (03 alert "yes if critical", 02 §D4); non-critical rows go to the digest. Idempotent through `ux_events_dedupe(event_type, dedupe_key)`. A later label joining the cluster updates `payload.{n_supporting, post_ids, materiality, confirmed_by}` and `severity` only while `alerted_at IS NULL` (03 §2.5); after that nothing is updated (the metric does not read events, §2.2) |
| `review_queue` (state; 03 §3.2) | `kind='llm_flag_candidate'`, `occupant_key`, `suggested_flag ∈ {owner_extraction, key_compromise, security_incident, NULL}`, `post_id`, `label_version`, `event_type`, `materiality`, `summary`, `evidence_url` (item `url`), `ref_table='events'` + `ref_id` when a cluster event exists, `priority` (1 for owner_extraction/key_compromise, else 2), `payload_json` `{reason, direction, confidence}`, `dedupe_key = f"llm_flag_candidate\|{post_id}\|{label_version}"`, `created_at`, `status='open'` | `INSERT OR IGNORE` (03's partial unique index `ux_review_queue_post(kind, post_id, label_version)`: one review per label, forever). Enqueued when the resolved label is (1) `owner_extraction_evidence` (mat ≥ 2) → `owner_extraction` (master "always human-reviewed"); (2) `security_incident` mat 3 → `key_compromise` when text/summary match the key-compromise regex (`coldkey\|hotkey\|private key\|seed phrase\|mnemonic\|compromis`), else `security_incident`; (3) a negative mat 3 that cannot be escalated (daily cap, `LLM_FALLBACK_AFTER_H`, text purged) → NULL. Status changes are made only by 11's `resolve_review` (below) |
| `manual_notes` (append) | `occupant_key`, `kind='llm_narrative'`, `title` (`'Narrative {as_of ISO} ({model})'`), `body` (≤ 300 words), `payload_json` `{risks[3], catalysts[3], confidence, model, evidence_sha256, input_tokens, output_tokens, cost_usd, spec_version, score_as_of}`, `created_by='llm:{model_resolved}'` (03: `created_by`, not `author`), `created_at` | UI/Telegram show the latest |
| `collector_runs`, `provider_spend` | — | written by 01's wrapper and 16's governor only (§6.3) |

**Not written by this spec:** `metric_values` (12's `compute_registered` writes the rows from this spec's `MetricResult`s: `stage='raw'`, `source='llm'`, `observed_at = T_eval`, first success wins, 02 §B3/§B4); `manual_flags` (11/14, 02 §D5); item text (03's retention policy `nlp_text_retention`, 02 §B10).

**Review hand-off (02 §A2/§D5).** 14's review routes and Telegram commands call 11's `resolve_review(db, review_id, action, *, by, note, evidence_url)`. For an `llm_flag_candidate` row: `confirm_flag` with `suggested_flag ∈ {owner_extraction, key_compromise}` → 11 writes `manual_flags(occupant_key, flag=suggested_flag, state='confirmed', evidence_url, note, set_by, set_at)` **and** calls this spec's `confirm_review(db, review_id, by=…, note=…)`, which writes/upgrades the cluster event with `payload.confirmed_by = by` (critical when negative mat 3) so the label counts in `event_impact_14d` from the event's `observed_at` on. A human-confirmed `owner_extraction_evidence` therefore reaches spec 12 only as the `owner_extraction` manual flag; `confirm_review` itself never writes `manual_flags`, and 11's `sync_confirmed_llm_flags` (official `integrity_features` run) back-fills the flag if `confirm_review` was ever called outside `resolve_review`. `key_compromise` → 11's `override_key_compromise`; `dismiss` → `rejected`, no effect here (the label stays uncounted if it needed confirmation). For `suggested_flag ∈ {security_incident, NULL}` confirmation must call `confirm_review` only, without a `manual_flags` row — 11 §2.10 currently restricts `confirm_flag` to the two flag kinds (open item for 11, §11 Q12).

### 3.3 Metric keys produced (master §7.3, P6; 02 §C4, `source='llm'`; raw value returned, normaliser applied by spec 12)

| metric_key | raw value returned | normaliser (master; 12 registry row) | w | freshness window (02 §C6) |
|---|---|---|---|---|
| `event_impact_14d` | `x = Σ_c m_c · d_c · 0.5^((t − τ_c) / 7 d)` over counted label clusters with `event_at ∈ (t − 14 d, t]` | `tanh(x/2)` → [−1,1] → `(v+1)/2` ∈ [0,1] (`event_impact(scale=2)`, row 42) | .40 | 1 d |
| `polarity_7d` | credibility- and engagement-weighted bullish share ∈ [0,1], 0.5 neutral, 25 % author cap | identity (row 43) | .25 | 1 d |
| `attention_7d` | z-score vs own 90-d baseline | `zclip(z, −2, +3)` (row 44) | .20 | 1 d |
| `kol_coverage_30d` | count of distinct credible KOLs | `logcap(x, 5)`; fallback `x_kol_posts_30d` (row 45) | .15 | 1 d |

`flow_attention_blend` = mean of the normalised `net_flow_30d`, `chain_buys` (spec 05) and `attention_7d` (this spec) — computed by spec 12, which calls `compute_attention_7d` for every universe occupant in every run (value or None).

### 3.4 Exact metric computations (`app/features/sentiment.py`)

**Common.** Each fn is `async def compute_<metric_key>(db, occupant_key: str, as_of: datetime) -> MetricResult` (02 §C5; 12 §2.3). `as_of` is spec 12's `T_eval` as a tz-aware UTC `datetime` (the run cutoff in `scoring_6h`, `now` in `features_hourly`); `t = parse_ts(as_of)` (01's `app/util/time.py`). **Every** read has `observed_at ≤ t` (labels, entities, items, trending rows, events, collector runs). `L(t)` = per item, the label resolved at `t` under the running scoring spec's `LABEL_POLICY` (§2.2), kept when its `post_label_entities` rows link it to `occupant_key` with `confidence ≥ 0.6` and `is_spam = 0`. `A(t, W)` = the occupant's *attributable* items with `event_at` in window `W` by label-free attribution (source-bound `occupant_key`, `social_post_mentions`, `news_items.occupant_keys_json`, `video_items.occupant_key`/`occupant_keys_json`) that pass §2.2 (a), (d), (e); `coverage(W) = |A ∩ labelled at t| / |A|` (1.0 when `A` is empty). **P6 fns never return `absence_reason`**: a P6 `absent` row normalises to 0.0 (12 §2.3) — for `event_impact_14d` the most negative value. "Nothing happened" is `ok` with value 0; an undefined metric is `MetricResult(None, {"status": "not_applicable", "reason": …})`; unmeasurable is `MetricResult(None, {"reason": …})` (12 writes `failed`).

**`event_impact_14d`** — clusters over `L(t)` restricted to `materiality ≥ 2` and `event_at ∈ (t − 14 d, t]`, keyed `(event_type, direction, floor(event_at / 172800))` (= the events' `dedupe_key`, §2.2). A resolved **bulk** label with `direction = −1` and `materiality = 3` counts only after a human confirmation visible at `t` (an `events` row with `source='llm'`, `payload.confirmed_by` set by `confirm_review`, `observed_at ≤ t`, whose `post_ids` contain the item); an escalation-tier resolved label counts as it stands (Sonnet's pass is master §7.4's second pass — this includes `owner_extraction_evidence`, whose human review gates only the red flag, §3.2). Per cluster: `m_c` = max counted materiality, `d_c` its direction, `τ_c` = min `event_at`. `x = Σ_c m_c · d_c · 0.5^((t − τ_c) / 604800)`. The metric is computed from append-only label rows, never from `events` payloads (which may change before `alerted_at`), so a replay at `t` reproduces the live value. `value_json = {n_events, top: [{event_type, direction, materiality, event_at, post_id}] (≤ 5 by |contribution|), coverage, pending_confirmation}`. Status: `x ≠ 0` → `ok` (any coverage; recorded); `x = 0` and `coverage ≥ 0.5` → `ok`, value `0.0` (→ 0.5 via `event_impact(scale=2)`); `x = 0` and `coverage < 0.5` → None, reason `labels_pending`.

**`polarity_7d`** — items `I` = X posts (`bot_filtered = 0`) and announcements in `L(t)` with `is_promo = 0` and `event_at ∈ (t − 7 d, t]` (master "posts … team + KOL + mentions"; news/video are not posts). Per item: credibility `c` = `author_credibility` (X; NULL → 0.7, 09's unprofiled level) or 1.0 (announcements: team channels); engagement `e = like_count + 2·repost_count + reply_count + quote_count` (NULL → 0; announcements 0); weight `w = c · (1 + log1p(e))` (a zero-engagement post still counts); score `s = 0.5 + 0.1·p·intensity` with `p = +1` bullish, `0` neutral, `−1` bearish (`s ∈ [0,1]`, 0.5 neutral — master's "bullish share" read as intensity-weighted net bullishness mapped to [0,1]). Author key `a` = `social_posts.author_id` or `announcements.source_channel_id`; `W_a = Σ_{i∈a} w_i`; `W' = cap_author_share({a: W_a}, cap=0.25)` (09 §2.8 water-filling: no author above 25 % of the **final** total; with fewer than 4 authors the cap becomes `1/n`) — a one-shot `min(W_a, 0.25·ΣW)` would leave a 70 % author at 45 %; item weight `w'_i = w_i · W'_a / W_a`; `value = Σ w'·s / Σ w'`. `|I| < 5` → None: reason `labels_pending` (failed) when `|I|` plus the unlabelled attributable posts of the window reach 5, else `status='not_applicable'`, reason `lt_5_posts` (master "< 5 posts → None"). `value_json = {n_items, n_authors, capped_authors, bullish_share, bearish_share, weight_total}`. 09 refreshes engagement in place (02 §J2), so `polarity_7d` carries the same look-ahead caveat as `x_engagement_30d`.

**`attention_7d`** — two daily series on UTC days before `D0 = floor_day(t)`: recent window `R` = the 7 days `[D0 − 7 d, D0)`, baseline `B` = the 90 days `[D0 − 97 d, D0 − 7 d)` (non-overlapping). `views_d = chain_trending_daily.views` (05; 0 = absent from a successful pull; no row = missing). `ment_d = log1p(#items in L(t) with event_at on day d, not source-bound to this occupant)` (its own posts, announcements and channel videos excluded), valid from the UTC day of the first label of a pinned version onward. Per component with ≥ 21 valid days in `B` and ≥ 4 in `R`: `z_c = (mean_R − mean_B) / max(std_B, floor_c)`, `floor_views = max(1, 0.1·mean_B)`, `floor_ment = 0.5`; `z = mean(z_c over available components)` (master "TMC trending views + log mentions": two z's averaged, because summing raw views with log counts would let views dominate); none available → None, reason `baseline_short`. Day `D0 − 1`'s trending row is written by 05's `tmc_daily` at 02:10Z, so the 00:15Z official run sees 6 recent view days. `value_json = {z_views, z_mentions, baseline_days_views, baseline_days_mentions, mentions_7d, views_7d}`. 09's label-free `x_mentions_7d`/`news_mentions_7d` are not read (§11 Q13).

**`kol_coverage_30d`** — `COUNT(DISTINCT kol_accounts.handle)` over X posts in `L(t)` with `is_kol = 1`, `kol_handle = handle`, `verified = 1 AND active = 1 AND credibility ≥ 0.5` (operator-edited state read as-is — accepted PIT deviation), excluding KOLs whose `team_occupant_keys` contains this occupant (09's rule), `bot_filtered = 0`, `event_at ∈ (t − 30 d, t]`. `ok` (0 allowed) when an `x_kol_poll` run with `ok = 1` and `finished_at ∈ (t − 3 d, t]` exists and ≥ 5 KOLs are verified; otherwise None, reason `kol_poll_stale` (incl. X provider null) / `kol_list_short`; also None (`labels_pending`) when no KOL post of the window is labelled but ≥ 1 is unlabelled. On None, 12 falls back to 09's label-free `x_kol_posts_30d` (registry row 45 `fallback_key`). `value_json = {kol_handles[], n_posts, unlabelled_kol_posts}`.

**Registration:** `register_metric("event_impact_14d", pillar="P6", fn=compute_event_impact_14d, freshness_days=1)` and likewise for `compute_polarity_7d`, `compute_attention_7d`, `compute_kol_coverage_30d` (12 §2.4; dotted paths `app.features.sentiment:compute_<key>`). 12's `compute_registered` writes each result as a `stage='raw'` row (`source='llm'`, `observed_at = T_eval`, `as_of` = the slot in scoring runs or `floor_hour(now)` in `features_hourly`; first success wins, 02 §B4). The fns run for every universe occupant hourly, so every query is served by 03's indexes (`ix_ple_occ`, `ix_post_labels_ver_obs`, `ix_social_posts_occ`, `ix_ann_occ_event`).

---

## 4. Absence-vs-failure table

| Metric / output | `ok` with a measured zero / neutral | None (`failed` or `not_applicable`) | Notes |
|---|---|---|---|
| `event_impact_14d` | no counted cluster in 14 d **and** `coverage ≥ 0.5` → `0.0` (→ 0.5, neutral) | `x = 0` with `coverage < 0.5` (`labels_pending`) | `x ≠ 0` is written at any coverage; never `absent` (would normalise to 0.0) — 12 row 42 |
| `polarity_7d` | — (0.5 is the neutral value, not a zero) | `< 5` labelled posts: `not_applicable` (`lt_5_posts`), or `failed` (`labels_pending`) when unlabelled posts would reach 5 | no posts at all → `not_applicable` |
| `attention_7d` | zero-mention days and `views = 0` days are observations → z computed (may be negative) | < 21 baseline or < 4 recent valid days on both components (`baseline_short`) | view days without a `chain_trending_daily` row are missing, never 0 |
| `kol_coverage_30d` | ok `x_kol_poll` within 3 d, ≥ 5 verified KOLs, no credible KOL mention → `0.0` | `kol_poll_stale`, `kol_list_short`, `labels_pending` → 12 uses `x_kol_posts_30d` | absence is only verifiable when the KOL collector actually ran |
| Labels for an item | `rules:v2` label written (fallback) | no row for the pinned versions → item invisible to metrics until labelled | never a placeholder label |
| Events | no mat ≥ 2 labels → no rows | — | mat-3 negatives pending escalation/review are not events and do not count |
| LLM down (5xx / 429 storms / batch expired) | — | labels delayed; metrics from the labelled subset with lower `coverage`; after `LLM_FALLBACK_AFTER_H` the rules classifier labels the backlog | `/health.collectors.llm_batch_poll.oldest_unlabelled_h` |
| No posts collected at all (X provider null, no announcements) | `event_impact_14d` 0 (→ 0.5) when nothing attributable exists | `polarity_7d` (`not_applicable`), `kol_coverage_30d` (→ fallback); `attention_7d` from views only | P6 coverage < 0.3 → P6 None → Velocity = blend only (12 §2.7) |

---

## 5. Configuration (`app/config.py`, pydantic-settings, `extra="ignore"`; consumers read `app.config.settings` dynamically; 01's `Settings` mirrors every row, this spec's defaults win — 02 §F1)

| Key | Type | Default | Validator / semantics |
|---|---|---|---|
| `LLM_ENABLED` | bool | `false` | master flag of the paid jobs; false → only `rules:v2` runs (if `LLM_RULES_ENABLED`); a disabled flag is reported `last_skip_reason="disabled"`, not a pause |
| `ANTHROPIC_API_KEY` | str | `""` | required when `LLM_ENABLED` (01's `llm_active()`); a 401 fails the run (§7) |
| `LLM_MONTHLY_CAP_USD` | float | `60` | ≥ 0; cap of 16's spend group `llm` = {`anthropic`} (02 §F2/§G1; checklist B6); **0 = provider disabled** (fail-closed) |
| `LLM_BULK_MODEL` | str | `claude-haiku-5-5` | fixed id (§2.3, verified 2026-10-18); part of `classifier_version` |
| `LLM_ESCALATION_MODEL` | str | `claude-sonnet-5-5` | `model_resolved` recorded (a refusal fallback may answer with another model) |
| `LLM_REFUSAL_FALLBACK` | bool | `true` | escalation and narrative calls use the beta `fallbacks="default"` (`server-side-fallback-2026-07-01`); false → plain `messages.create`, and a refusal skips the item/subject |
| `LLM_NARRATIVE_MODEL` | str | `claude-opus-5-5` | |
| `LLM_SCHEMA_V` | int | `1` | bump only with `nlp/schema.py` changes |
| `LLM_RUBRIC_PATH` | str | `app/nlp/prompts/classify_v1.md` | file must exist; hash computed at startup |
| `LLM_BATCH_INTERVAL_MIN` | int | `360` | 60–1440; with the offset it defines the §6.1 cron |
| `LLM_BATCH_OFFSET_MIN` | int | `20` | submit at HH:20 for HH ∈ {00,06,12,18} so results usually precede the next HH:15 scoring run |
| `LLM_POLL_INTERVAL_MIN` | int | `5` | 1–60 |
| `LLM_BATCH_ITEMS_PER_REQUEST` | int | `20` | 5–40 (replaces 01's placeholder `LLM_BATCH_SIZE`) |
| `LLM_BATCH_MAX_REQUESTS` | int | `250` | per cycle (≤ 5,000 items; far below the 100,000-request / 256 MB batch limit) |
| `LLM_BATCH_STALE_H` | int | `26` | a submitted batch not `ended` after this (the API itself expires work at 24 h) → `batches.cancel`, status `expired`, items requeued |
| `LLM_LOOKBACK_DAYS` | int | `3` | items older than this are never sent to the LLM (rules may still label them); < 03's `LLM_PENDING_TEXT_MAX_DAYS` (7) |
| `LLM_MAX_ITEM_CHARS` | int | `1200` | truncation, `…` appended |
| `LLM_TOKENS_PER_CHAR` | float | `0.29` | estimate (≈ 1/3.5) for budget math |
| `LLM_PRICES_JSON` | str | `""` | optional override of `costs.PRICES` (`{"model": [in, out]}` USD/MTok) |
| `LLM_ENTITY_MIN_CONFIDENCE` | float | `0.6` | ≤ 0.6 (the frozen metric threshold); links below are stored in `raw_json` only |
| `LLM_ESCALATION_ENABLED` | bool | `true` | |
| `LLM_ESCALATION_MAX_PER_DAY` | int | `40` | beyond → review queue only; bounds the Sonnet spend (§6.3) |
| `LLM_NARRATIVE_ENABLED` | bool | `true` | flag of `llm_narrative_weekly` |
| `LLM_NARRATIVE_DAY_OF_WEEK` / `LLM_NARRATIVE_HOUR_UTC` | str / int | `sun` / `1` | cron; 01:00Z = after the 00:15Z official score, clear of the 03:00–04:30Z retention/backup/reboot window |
| `LLM_NARRATIVE_MAX_SUBNETS` | int | `40` | |
| `LLM_NARRATIVE_EVIDENCE_TOKENS` | int | `8000` | bundle budget (estimated from chars; Opus 5.5 counts may be higher — trim with `count_tokens` when available) |
| `LLM_FALLBACK_AFTER_H` | int | `18` | unlabelled item (or unescalated mat-3 negative) older than this → rules label (resp. review queue) |
| `LLM_RULES_ENABLED` | bool | `true` | flag of `llm_rules_fallback` |
| `LLM_TEXT_RETENTION_DAYS` | int | `90` | 1–90; applied by 03's retention policy `nlp_text_retention` (02 §B10) |
| `LLM_REQUEST_TIMEOUT_S` | int | `180` | 30–600; direct Sonnet/Opus calls (adaptive thinking lengthens them); SDK `max_retries=3` |
| `LLM_RATE_PER_SEC` | float | `0.5` | 01's `ProviderLimiter` bucket `anthropic` (burst 2) for every SDK call (replaces this spec's old `ANTHROPIC_RPM`) |
| `LLM_EVAL_ALLOW_NETWORK` | bool | `false` | eval harness may call the API |

Read, not owned: `KILL_SWITCH_PATH`, `PAUSE_ALL_PATH` (01/16), `LLM_PENDING_TEXT_MAX_DAYS` (03), `X_MIN_CREDIBILITY` (09), `SPEC_VERSION`, `SCORING_SHADOW_SPECS` (01/12). **Retired:** `ANTHROPIC_RPM`, `LLM_MIN_AUTHOR_CREDIBILITY` (09's `bot_filtered` gates instead), and 01's placeholders `LLM_INTERVAL_MIN`, `LLM_BATCH_SIZE`, `LLM_CLASSIFIER_VERSION` (the version is computed from model, rubric and schema and pinned by `LABEL_POLICY`).

**Frozen in `scoring/spec.py` (`ts-1.0`), not config:** half-life 7 d, window 14 d, cluster width 48 h, `tanh(x/2)`, polarity weight `c·(1 + log1p(e))` with unprofiled `c = 0.7`, single-account cap 0.25 (water-filled), min 5 posts, attention windows 7 d / 90 d with ≥ 4 / ≥ 21 valid days and the two floors, KOL credibility ≥ 0.5 with ≥ 5 verified KOLs and a 3-d poll recency, entity confidence ≥ 0.6, coverage threshold 0.5, `LABEL_POLICY` version strings. Changing any = new `spec_version`.

**Kill-switch / pause semantics (02 §B12, 16 §2.11).** Reasons shown in `/health.collectors.<id>.paused`: `operator | kill_switch | pause_all | budget | emergency_file`.
- `KILL_SWITCH` pauses the **paid** jobs `llm_classify_submit` and `llm_narrative_weekly`, and the governor denies every paid call (so no Sonnet escalation runs inside `llm_batch_poll` either).
- `PAUSE_ALL` pauses all four jobs (none is `ops_exempt`).
- Operator pauses are `control_state` rows for an exact job id or 16's pause group `llm` (= all four jobs of §6.1; never prefix matching); `resume` deletes only the row. `data/control/<job>.paused` is an emergency input the app reads and never writes (`emergency_file`); the reply to a `resume` says the job stays paused until the operator deletes it.
- Budget: the governor's hard latch for group `llm` pauses the jobs whose `provider` is `anthropic` (`budget`); `reserve` returning `None` ends a submit run `skipped: budget` and the rules fallback labels the backlog immediately (no `LLM_FALLBACK_AFTER_H` wait).
- `llm_batch_poll` and `llm_rules_fallback` are free jobs (`paid=False`, no provider): under `KILL_SWITCH` or a budget stop they keep downloading already-submitted (already billed) batches — so the reservation can be committed (02 §G5) — and keep labelling with rules.

---

## 6. Cadence, rate limits, cost

### 6.1 Jobs (owner of these ids per 02 §A2/§E2; 01's `JOB_TABLE` mirrors this table verbatim, replacing its placeholder `llm_classify_6h`)

| id | factory (`app.…`) | trigger | UTC time / interval | misfire grace s | timeout s | enabled flag | phase | provider | paid | `min_gap_factor` | liveness max-age s (16 §2.5) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| `llm_classify_submit` | `nlp.llm_classify:LlmClassifySubmitJob` | cron `hour=0,6,12,18 minute=LLM_BATCH_OFFSET_MIN` | 00:20 / 06:20 / 12:20 / 18:20Z | 3600 | 600 | `LLM_ENABLED` | 9 | `anthropic` | yes | 0 | 43,920 |
| `llm_batch_poll` | `nlp.llm_classify:LlmBatchPollJob` | interval `minutes=LLM_POLL_INTERVAL_MIN` | every 5 min: retrieve → download → apply → escalate → materialise events → commit | 300 | 1800 | `LLM_ENABLED` | 9 | anthropic (free: not paused by KILL_SWITCH) | no | 0 | 2,520 |
| `llm_rules_fallback` | `nlp.rules:LlmRulesFallbackJob` | interval `minutes=60` | hourly: rules-label items unlabelled > `LLM_FALLBACK_AFTER_H` (all eligible when the LLM is off or budget-stopped), then `materialise_events(since_hours=72)` (crash safety net) | 1800 | 600 | `LLM_RULES_ENABLED` | 9 | — | no | 0 | 7,920 |
| `llm_narrative_weekly` | `nlp.narrative:LlmNarrativeWeeklyJob` | cron `day_of_week=LLM_NARRATIVE_DAY_OF_WEEK hour=LLM_NARRATIVE_HOUR_UTC minute=0` | Sun 01:00Z | 21600 | 5400 | `LLM_NARRATIVE_ENABLED` | 9 | `anthropic` | yes | 0 | ≈ 1.21 M (weekly; never trips) |

All four: `max_instances=1`, `coalesce=True`, not `ops_exempt`. There is no `nlp_text_retention` job (03's policy) and no separate events job (events are materialised inline after every label write, plus the hourly safety net). Spec 12's `features_hourly` and `scoring_6h` call §3.4's fns.

### 6.2 Rate limiting and request counting

Every SDK call — `messages.batches.create/retrieve/results/cancel`, Sonnet and Opus `messages.create`, `messages.count_tokens` — first awaits `limiter.acquire("anthropic")` (01's `ProviderLimiter`, `LLM_RATE_PER_SEC` 0.5/s, burst 2). 429/5xx/connection errors are retried by the SDK (honouring `retry-after`); when retries are exhausted on a 429 the caller calls `limiter.penalize("anthropic", retry_after)` and the run fails. Because these calls bypass `Http`, each one books its request count with `governor.record("anthropic", 0.0, requests=1, job=…, run_id=…)` (buffered by 16 until `flush()`; 02 §G5); the Sonnet/Opus calls book usd and the request in one `record` (§6.3). Individual requests inside a batch are not separate API calls.

### 6.3 Budget math (prices per §2.3, skill cache 2026-06-24 — verify at build time)

Assumed realistic volume: 130 team accounts × 3 posts + 30 KOLs × 10 + mention search ~500 + announcements 50 + news/video 40 ≈ **1,300 items/day** (research §5 modelled 2,000). All three models think adaptively by default; thinking tokens bill as output, so output figures include an **UNVERIFIED** thinking allowance (bulk at effort `low`: +100 % of the JSON; Sonnet/Opus at `medium` as before). Token counts carry Haiku 5.5's ≈ +30 % tokenizer.

| Component | Per day | Cost/day | Cost/month |
|---|---|---|---|
| Haiku 5.5 bulk, Batch: 65 requests × (system ≈ 4,300 tok + 20 × 220 tok) ≈ 8,700 in; (20 × 95 + 30) × 2 ≈ 3,860 out incl. thinking | 566k in / 251k out | 0.566×$0.05 + 0.251×$0.25 = **$0.09** (cache reads would lower the input half) | **≈ $2.7** |
| At 2,000 items/day | 870k / 386k | $0.14 | ≈ $4.2 |
| Sonnet 5.5 escalation: 10–40 calls/day (cap `LLM_ESCALATION_MAX_PER_DAY` = 40) × (≈ 4,300-tok prefix, cached within a poll run: 1 write at 1.25× then reads at 0.1×; + 250 item tokens; ≈ 1,000 out incl. thinking) | — | $0.15 (10/day) – $0.49 (40/day) | ≈ $5 – $15 |
| Opus 5.5 narrative weekly: 40 subnets × (≈ 9k in + ≈ 2k out incl. thinking) | — | 40 × (0.009×$4 + 0.002×$20) = $3.04/week | ≈ $13.2 |
| `count_tokens` | price not stated in the skill (believed free, rate-limited — UNVERIFIED) | | |
| **Total** | | | **≈ $21–31/month** at 1,300 items/day (≈ $22–32 at 2,000) — was ≈ $37–48 with Haiku 4.5 / Opus 5; inside the $60 cap, which the governor enforces |

Levers if the run-rate nears the cap: narrative effort `low` or fewer subjects (`LLM_NARRATIVE_MAX_SUBNETS`), a lower escalation cap, fewer mention-search items.

**Estimates** (`app/costs.py`, 16 §2.10; this spec owns the formula and the price values): `PRICES = {"claude-haiku-5-5": (0.10, 0.50), "claude-sonnet-5-5": (2.0, 10.0), "claude-opus-5-5": (4.0, 20.0), "claude-haiku-4-5": (1.0, 5.0), "claude-sonnet-5": (2.0, 10.0), "claude-opus-5": (5.0, 25.0)}`` — a dated id (`claude-haiku-4-5-20251001`) is looked up by its alias (trailing `-YYYYMMDD` stripped); `LLM_PRICES_JSON` overrides. `estimate_batch(texts, *, model, system_tokens, n_req) -> float`: `est_in = n_req × system_tokens + Σ (len(text)·LLM_TOKENS_PER_CHAR + 40)`, `est_out = len(texts) × 95 + n_req × 30`, `est_usd = estimate_llm_usd(model, est_in, est_out, batch=True)`. `system_tokens` is measured per model (token counts are model-specific) with `client.messages.count_tokens(model=…, system=…, messages=[…])` once per rubric/alias change. `actual_usd` of a batch = Σ over succeeded results of `estimate_llm_usd(model, usage.input_tokens, usage.output_tokens, batch=True, cache_read_tokens=usage.cache_read_input_tokens, cache_write_tokens=usage.cache_creation_input_tokens)` (cache read ×0.1, write ×1.25).

**Governor protocol (02 §G5; keyword-only after the amount):**
- Submit: `res = await governor.reserve("anthropic", est_usd, job="llm_classify_submit", run_id=run_id)`; `None` → `skipped: budget` (the reason is `governor.check(...)`'s if needed). `res.id` is persisted in `job_state('llm_classify_submit', f'resv:{batch_key}')` before `batches.create`.
- `batches.create` fails (or the batch is canceled before any result): `await governor.release(res_id)`.
- After download and apply (batch `ended`): `await governor.commit(res_id, actual_usd, detail={"model": …, "batch_key": …})`, then `await governor.record("anthropic", 0.0, requests=0, units={"in": …, "out": …, "cache_read": …, "cache_write": …}, job="llm_batch_poll", run_id=run_id)`; delete the `job_state` row. `UnknownReservation` (expired and purged, 16) → book `actual_usd` with `record("anthropic", actual_usd, requests=0, job=…, run_id=…, detail=…)` instead.
- Unreserved Sonnet/Opus calls: `d = await governor.check("anthropic", est_usd)`; `not d.allowed` → skip (escalation stays pending, narrative subject skipped); after the call `await governor.record("anthropic", usd, requests=1, units={…}, job=…, run_id=…, detail={"model": message.model, "post_id"|"occupant_key": …})`.
- 80 % / 100 % alerts are 16's `spend_80pct` / `spend_cap_reached` (group `llm`); this spec emits no budget event.

---

## 7. Failure modes and fail-safe behaviour

Rules: no job raises into the scheduler (01's wrapper); every job writes `collector_runs`; `consecutive_failures` increments per job and 01's `collector_failing` pages at `JOB_FAIL_ALERT_THRESHOLD`. This spec emits **no system events** of its own (02 §D1/§D3): non-fatal degradations are logged, written to `collector_runs.note` (16's ops digest shows them) and exposed in `/health`.

| Failure | Behaviour |
|---|---|
| API key missing/invalid (401) | the submit run fails (`auth_failed`) each cycle — never a silent disable — so `collector_failing` pages; rules fallback keeps labelling |
| `batches.create` 4xx `invalid_request` | batch `failed`, error stored, reservation released; items requeued at 10/request once; if it fails again → rules label + `label_error` counter; run fails (schema/rubric bug) |
| 429 / 5xx / network | SDK retries; then `limiter.penalize` (429) and run failure; next cycle retries; open batch ids persist in `llm_batches` |
| Batch `processing_status` never `ended` | after `LLM_BATCH_STALE_H` → `batches.cancel`, status `expired`, items requeued (attempt + 1); results that did arrive are applied and committed |
| Result `errored` (per request) | `invalid_request` → requeue at 10/request (never resubmitted unchanged); other → requeue same size; after attempt 3 → rules label |
| Result `expired` / `canceled` | requeue |
| Parse failure (invalid JSON, count/position mismatch, enum violation, `stop_reason` `max_tokens`/`refusal`) | salvage per-item objects whose position matches; unmatched items requeued; `n_parse_errors` per batch; > 10 % of requests in a batch → `collector_runs.note="classifier_drift"` and `/health` `parse_error_rate_24h` |
| Sonnet call fails (after retries) | bulk label stays; retried next poll (max 3 tries; governor denials do not count), then review queue with `suggested_flag` (§3.2) — the label does **not** count as confirmed |
| Pending text purged before escalation / relabel | escalation → review queue (operator opens `url`); a rules-labelled pending item stays rules-labelled |
| Opus `stop_reason == 'refusal'` or parse failure | narrative skipped for that occupant this week, logged with `stop_details.category`; no retry the same week |
| Registry unavailable (alias table empty) | submit run fails (`alias_table_empty`; entity linking would be blind) |
| Bulk version not in `LABEL_POLICY` | submit `skipped: version_unpinned` (no spend; `/health` shows it) |
| Cost cap hit | §5 kill/pause semantics and §6.3; 16 pages `spend_cap_reached` |
| Power loss mid-cycle | `llm_batches.status='submitted'` rows resume polling on start; reservation ids survive in `job_state`; apply is idempotent (`INSERT OR IGNORE` on `(post_id, classifier_version)`, events on `dedupe_key`); batches still `building` are marked `failed`, their reservation released and items requeued |
| Model snapshot change (`message.model` ≠ last seen for a Sonnet/Opus alias) | stored in `model_resolved`, logged `classifier_snapshot_changed` (log only); the bulk model is a pinned dated id; no automatic version bump (§11 Q3) |
| Last-good semantics | metrics are computed only from stored labels; a stalled classifier lowers `coverage`, never substitutes stale labels for new items; `scores` reads last good per spec 12 |

`/health` (01 builds it, 02 §G6): each job has 01's standard `collectors.<id>` block; `LlmBatchPollJob.status()` extends it with `queue_depth, open_batches, oldest_unlabelled_h, labelled_24h, escalations_24h, escalation_pending, parse_error_rate_24h, fallback_active, review_open, classifier_version_bulk, classifier_version_escalation, rubric_sha256`. Spend is 16's group-keyed `/health.spend.llm = {usd, cap_usd, pct, paused, reserved_usd}` (02 §G1/§G6). Pages come only from 01 (`collector_failing`), 16 (`spend_*`) and critical taxonomy events (§3.2).

---

## 8. Implementation plan

Nothing is ported from the old bot (brief: none). Reuse: `CollectorJob`/`JOB_TABLE`, `ProviderLimiter`, `app/util/time.py`, `StructuredLogger` (01); `CostGovernor`, `PRICES`, `estimate_llm_usd`, `estimate_batch` in `app/costs.py` (16; formula and prices owned here); `app/nlp/textnorm.py` (08); `cap_author_share` in `app/nlp/kol.py` (09); `review.resolve_review` (11). Dependency: the official `anthropic` SDK, `>=1,<2` — 1.x runs on `httpx2` and needs Python ≥ 3.10, so 01's `httpx<1.0`/`respx` pin is unaffected (confirm a published 1.x on PyPI at build time; otherwise pin the newest 0.x and use its `httpx`).

### 8.1 Files

```
app/nlp/__init__.py
app/nlp/schema.py         # pydantic models + JSON schema export
app/nlp/prompts/classify_v1.md   # §2.4 verbatim
app/nlp/prompts/narrative_v1.md
app/nlp/rubric.py         # load_rubric(), rubric_sha256(), classifier_version(model, schema_v)
app/nlp/aliases.py        # build_alias_table(db) -> (text, sha256)
app/nlp/queue.py          # select_unlabelled(), assemble_text(), pack_requests()
app/nlp/llm_classify.py   # LlmClassifier: submit_cycle(), poll_cycle(), apply_batch(), escalate(); LlmClassifySubmitJob, LlmBatchPollJob
app/nlp/rules.py          # RulesClassifier (rules:v2); LlmRulesFallbackJob
app/nlp/lexicon.py        # polarity lexicon, event regexes
app/nlp/resolve.py        # LABEL_POLICY resolution
app/nlp/events.py         # materialise_events(), enqueue_review(), confirm_review() (called by 11)
app/nlp/narrative.py      # weekly Opus job; LlmNarrativeWeeklyJob
app/nlp/eval.py           # python -m app.nlp.eval
app/features/sentiment.py # compute_<key> P6 producer fns + registration (no retention module: 03 owns it)
app/scoring/spec.py       # add LABEL_POLICY for ts-1.0 (spec 12 owns the file)
tests/nlp/…  tests/fixtures/golden_labels_v1.jsonl  tests/fixtures/llm/*.json  tests/fixtures/rubric_hashes.json
```

### 8.2 Signatures

```python
# app/nlp/schema.py
class Entity(BaseModel): netuid: int; confidence: float  # 0..1
class LabelV1(BaseModel):
    id: str; polarity: Literal["bullish","bearish","neutral"]; intensity: int  # 1..5
    event_type: Literal["product_launch","release_upgrade","partnership_integration","revenue_customer",
        "token_buyback_burn","exchange_listing","funding","emissions_tokenomics_change","team_change",
        "security_incident","delay_missed_milestone","governance_dispute","regulatory",
        "owner_extraction_evidence","giveaway_promo","other"]
    direction: Literal[-1,0,1]; materiality: Literal[1,2,3]; entities: list[Entity]
    is_spam: bool; is_promo: bool; summary: str; confidence: float
    # validators: len(summary) <= 140 (truncate, do not fail); direction ∈ ALLOWED_DIRECTION[event_type] (clamp to default, flag raw_json.clamped=True)
class BatchOutV1(BaseModel): schema_v: Literal[1]; items: list[LabelV1]
class NarrativeV1(BaseModel): narrative: str; risks: list[str]; catalysts: list[str]; confidence: float  # 3 each, ≤ 300 words
def json_schema_batch() -> dict   # only type/enum/required/additionalProperties keywords; ranges checked client-side
def json_schema_single() -> dict
def json_schema_narrative() -> dict
ALLOWED_DIRECTION = {"product_launch": {1}, "release_upgrade": {1}, "partnership_integration": {1}, "revenue_customer": {1},
    "token_buyback_burn": {1}, "exchange_listing": {1}, "funding": {1}, "emissions_tokenomics_change": {0},   # master §7.4: informational
    "team_change": {-1,1}, "security_incident": {-1}, "delay_missed_milestone": {-1}, "governance_dispute": {-1},
    "regulatory": {-1}, "owner_extraction_evidence": {-1}, "giveaway_promo": {0}, "other": {0}}
DEFAULT_DIRECTION = {k: (next(iter(v)) if len(v)==1 else 0) for k, v in ALLOWED_DIRECTION.items()}; DEFAULT_DIRECTION["team_change"] = -1

# app/nlp/rubric.py
def load_rubric(path: str) -> str
def rubric_sha256(text: str) -> str            # normalise CRLF→LF, rstrip lines
def classifier_version(model_id: str, rubric_sha: str, schema_v: int) -> str   # f"{model_id}:{rubric_sha[:8]}:{schema_v}"
RULES_VERSION = "rules:v2"

# app/nlp/aliases.py
async def build_alias_table(db) -> tuple[str, str]   # (text, sha256); current occupants only

# app/nlp/queue.py
@dataclass class QueueItem: post_id: str; item_kind: str; item_id: str; occupant_key: str|None; platform: str
    author_kind: str; author_key: str|None; kol_handle: str|None; text: str; text_hash: str
    event_at: int; text_purge_after: int|None                     # unix s (02 §B1)
    like_count: int|None; repost_count: int|None; reply_count: int|None; quote_count: int|None; url: str|None
async def select_unlabelled(db, *, bulk_version: str, now: int, lookback_days: int, limit: int) -> list[QueueItem]
def assemble_text(row: dict, item_kind: str, max_chars: int) -> str | None
def pack_requests(items: list[QueueItem], per_request: int) -> list[list[QueueItem]]
def render_items_block(items: list[QueueItem]) -> str

# app/nlp/llm_classify.py
class LlmClassifier:
    def __init__(self, db, client: anthropic.AsyncAnthropic, governor: CostGovernor, limiter: ProviderLimiter, settings_getter)
    async def submit_cycle(self, run_id: str) -> dict          # {batch_key, n_requests, n_items, est_cost_usd} or {skipped: budget|version_unpinned|empty_queue}
    async def poll_cycle(self, run_id: str) -> dict            # per submitted batch: retrieve → if ended: download+apply+escalate+events → commit
    async def apply_batch(self, batch_key: str, results: AsyncIterable) -> dict
    def parse_request_result(self, custom_id: str, message, post_ids: list[str]) -> tuple[list[tuple[str, LabelV1]], list[str]]
        # post_ids = the request's llm_batch_items sorted by post_id; prompt id "k" ↔ post_ids[k-1]; returns (parsed, unmatched)
    async def write_labels(self, labels: list[LabelV1], items_by_id: dict[str, QueueItem], *, version: str, tier: str, batch_key: str|None, usage, run_id: str) -> int
    async def escalate(self, run_id: str) -> dict               # single-item Sonnet calls
    async def link_entities(self, label: LabelV1, item: QueueItem) -> list[dict]   # adds source-bound entity (via='source', 1.0), maps netuid→occupant_key at event_at
def build_batch_request(custom_id: str, model: str, system_blocks: list[dict], items_text: str) -> Request  # anthropic.types.messages.batch_create_params.Request

# app/nlp/rules.py
class RulesClassifier:
    VERSION = "rules:v2"
    def classify(self, item: QueueItem, aliases: AliasIndex) -> LabelV1
async def rules_fallback_cycle(db, run_id: str) -> dict

# jobs (01 CollectorJob subclasses; class attributes = their §6.1 rows)
class LlmClassifySubmitJob(CollectorJob)   # llm_classify_submit
class LlmBatchPollJob(CollectorJob)        # llm_batch_poll; status() extends 01's /health block (§7)
class LlmRulesFallbackJob(CollectorJob)    # llm_rules_fallback (app/nlp/rules.py)
class LlmNarrativeWeeklyJob(CollectorJob)  # llm_narrative_weekly (app/nlp/narrative.py)

# app/nlp/resolve.py
@dataclass(frozen=True) class LabelPolicy: escalation: tuple[str, ...]; bulk: tuple[str, ...]; fallback: tuple[str, ...]
async def resolve_labels(db, policy: LabelPolicy, *, occupant_key: str|None, since: int, until: int, as_of: int) -> list[ResolvedLabel]
#   SQL: pick, per post_id, the row with min(tier_rank) among rows whose classifier_version ∈ policy and observed_at ≤ as_of

# app/nlp/events.py
async def materialise_events(db, policy: LabelPolicy, run_id: str, *, since_hours: int = 72) -> dict
async def enqueue_review(db, *, occupant_key: str, post_id: str, label_version: str, event_type: str, materiality: int,
                         summary: str | None, evidence_url: str | None, suggested_flag: str | None, reason: str,
                         priority: int, ref_id: int | None = None) -> int | None   # INSERT OR IGNORE (ux_review_queue_post); kind 'llm_flag_candidate' only
async def confirm_review(db, review_id: int, *, by: str, note: str | None = None) -> int | None   # 11's review.py calls it with by= only
    # called by 11's resolve_review (never by 14 directly); writes/upgrades the cluster event with payload.confirmed_by = by
    # (critical when negative mat 3); returns events.id; never writes manual_flags (11/14, 02 §D5). No reject hook: 11's dismiss suffices.

# app/nlp/narrative.py
async def select_subjects(db, *, max_subnets: int) -> list[str]   # tier A/B ∪ top-10 |Δ composite_smoothed 7d| ∪ new red flags this week
async def build_evidence(db, occupant_key: str, *, token_budget: int) -> tuple[str, str]   # (bundle_text, sha256)
async def run_weekly(db, client, governor, limiter, run_id: str) -> dict

# app/features/sentiment.py  (02 §C5; as_of = 12's T_eval, tz-aware UTC)
async def compute_event_impact_14d(db, occupant_key: str, as_of: datetime) -> MetricResult
async def compute_polarity_7d(db, occupant_key: str, as_of: datetime) -> MetricResult
async def compute_attention_7d(db, occupant_key: str, as_of: datetime) -> MetricResult
async def compute_kol_coverage_30d(db, occupant_key: str, as_of: datetime) -> MetricResult
def register(registry) -> None     # register_metric(key, pillar="P6", fn=…, freshness_days=1) × 4
```

### 8.3 Batch API calls (Python SDK, `anthropic.AsyncAnthropic`, per skill `python/claude-api/batches.md`)

```python
from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
from anthropic.types.messages.batch_create_params import Request

def build_batch_request(custom_id, model, system_blocks, items_text):
    return Request(custom_id=custom_id, params=MessageCreateParamsNonStreaming(
        model=model, max_tokens=8192, system=system_blocks,
        messages=[{"role": "user", "content": items_text}],
        output_config={"format": {"type": "json_schema", "schema": json_schema_batch()}},  # + "effort": BULK_EFFORT; supported on Haiku 5.5 and in Batches (skill)
    ))
# every SDK call below is preceded by: await limiter.acquire("anthropic")   (§6.2)
# submit (after governor.reserve, §6.3)
batch = await client.messages.batches.create(requests=requests)        # store batch.id → llm_batches.provider_batch_id, status='submitted'
# poll
b = await client.messages.batches.retrieve(provider_batch_id)          # b.processing_status == "ended"; b.request_counts.{succeeded,errored,expired,canceled}
# download (results in ANY order — key by custom_id)
async for r in await client.messages.batches.results(provider_batch_id):   # verified 2026-10-18: SDK 1.15 AsyncBatches.results is a coroutine returning AsyncJSONLDecoder
    match r.result.type:
        case "succeeded": msg = r.result.message; text = next(b.text for b in msg.content if b.type == "text"); usage = msg.usage
        case "errored":   err = r.result.error.type   # 'invalid_request' → requeue at 10/request; else requeue
        case "expired" | "canceled": requeue
```

`system_blocks = [{"type":"text","text": rubric}, {"type":"text","text": alias_table, "cache_control": {"type":"ephemeral"}}]`. `custom_id = f"{batch_key[:8]}-{index:04d}"`; the `llm_batch_items` table maps it back to post_ids (then prompt position → `post_id`, §2.2).

Escalation call (after `governor.check`, §6.3): `await client.messages.create(model=LLM_ESCALATION_MODEL, max_tokens=4096, system=system_blocks, messages=[{"role":"user","content": single_item_block + "\nThis item was pre-flagged as potentially material and negative. Assess independently and cite the evidence in summary."}], output_config={"effort": "medium", "format": {"type":"json_schema","schema": json_schema_single()}})` — `max_tokens` covers adaptive thinking plus the JSON (1,024 could truncate it); no `thinking`, `temperature` or prefill. Narrative call: same shape on `LLM_NARRATIVE_MODEL` with `max_tokens=8000`, `timeout=LLM_REQUEST_TIMEOUT_S`, `narrative_v1.md` as system, bundle as user, `format` = `json_schema_narrative()`; check `response.stop_reason` before reading content (`refusal` → skip, `max_tokens` → parse failure). **Refusal fallback (verified 2026-10-18):** with `LLM_REFUSAL_FALLBACK=true` both calls go through `client.beta.messages.create(..., betas=["server-side-fallback-2026-07-01"], fallbacks="default")` — the skill recommends it by default for Sonnet 5.5 (only the `"default"` form, Claude API only) and Opus 5.5; it is rejected in Batches and Haiku 5.5 has none. `model_resolved = message.model` records which model answered; a final `stop_reason == "refusal"` means the whole chain declined (escalation: the label goes to the review queue without counting as confirmed; narrative: subject skipped). The escalation call is `client.messages.create` with the same arguments when the flag is false.

### 8.4 Rules classifier (`rules:v2`)

- Polarity: lexicon `lexicon.POS/NEG` (~220 crypto-tuned terms with weights ±1..±3, e.g. `ship`, `live`, `revenue`, `buyback`, `listed`, `partnership` / `exploit`, `drained`, `rug`, `dump`, `delay`, `abandon`, `banned`, `paused`), negation window 3 tokens (`not`, `no`, `never`, `isn't`), booster words (`huge`, `massive` ×1.5), exclamation/caps bonus ≤ +0.5, emoji map (🚀 +2, 🔥 +1, 💀 −2, 🚩 −2). `score = (Σ pos − Σ neg) / sqrt(n_terms² + 15)` (VADER-style normalisation) → `bullish` if > 0.15, `bearish` if < −0.15, else neutral; intensity = `min(5, 1 + round(4·|score|))`.
- Event regexes (first match in priority order; **widened at build 2026-10-18** — the patterns as listed missed "socially engineered", "now live for spot trading on Kraken", "stepping away", "unstaked … moved to an exchange", "integrates with", "Wells notice" and had no rule for emissions/regulatory/partnership; `app/nlp/lexicon.py::EVENT_RULES` is authoritative, order: security, owner extraction, buyback, exchange listing (incl. DEX/aggregator), emissions/tokenomics, revenue, delay, team change, governance, regulatory, product launch, partnership, release, funding, giveaway). Original list: security_incident (`exploit|hack|compromis|drained|phish|key.?(leak|stolen)|social.?engineer`), owner_extraction_evidence (`owner.{0,40}(dump|sold|unlock|extract)|(dump|sold).{0,40}owner`), token_buyback_burn (`buy.?back|burn(ed|ing)?\b`), exchange_listing (`(list(ed|ing)|now (live|trading)) on (kraken|coinbase|binance|okx|bybit|gate|kucoin|mexc)`), revenue_customer (`\$[\d.,]+ ?(k|m)? (arr|revenue|mrr)|paying customers|revenue`), product_launch (`(is|now) live|launch(ed|ing)?\b|mainnet|public beta`), release_upgrade (`v\d+\.\d+|release notes|changelog|upgrade`), team_change (`step(ping)? down|leav(e|ing) the team|joins? as|new (cto|ceo|lead)`), delay_missed_milestone (`delay(ed)?|postpon|pushed back|slipp`), governance_dispute (`banned|ousted|dispute|timeout|removed from`), funding (`raise[sd]?|series [a-c]|seed round`), giveaway_promo (`giveaway|airdrop|rt (and|&) follow|tag \d friends`), else other. Materiality: 2 if the text contains a number, URL or `$`, else 1; **rules never emit 3** (so no escalation from rules). Direction = `DEFAULT_DIRECTION[event_type]`.
- Entities: netuid regex (`\b(?:sn|subnet|netuid)\s?#?(\d{1,3})\b|\$sn(\d{1,3})\b`, case-insensitive) → confidence 1.0; exact alias/symbol match (word-bounded, ≥ 4 chars, not a generic-word alias unless Bittensor context regex `bittensor|\btao\b|subnet|netuid|\bsn\d` matches) → 0.7; source-bound → 1.0.
- `is_spam`: URL shorteners + "send TAO", or zero Bittensor context and no source binding; `is_promo` from the giveaway regex. `confidence = 0.4` (0.5 when a netuid was explicit). `summary = first 140 chars`.

### 8.5 Narrative job

`select_subjects`: latest official `scores` rows (`spec_version = current`) with `tier IN ('A','B')` ∪ top-10 by `|composite_smoothed(as_of) − composite_smoothed(as_of − 7 d)|` ∪ occupants with a new `red_flags` entry in the last 7 d; cap `LLM_NARRATIVE_MAX_SUBNETS`, priority A/B by composite desc, then movers. `build_evidence` (≤ 8,000 estimated tokens, sections in this order, truncating from the bottom): identity header (name, netuid, occupant_key, age, owner identity flag); latest `explain` JSON; pillar values + confidence; `events` last 14 d (type, direction, materiality, summary, date); top-15 resolved labels by `materiality desc, engagement desc` (summaries only, never full text); P1 highlights (`emission_share`, `gate_margin`, `dereg_safety`, `net_flow_30d`); P4 highlights (`owner_commitment`, `owner_retention_30d` — `owner_conviction` is a retired name, 02 §C2); P2 highlights (`active_authors_28d`, `commit_days_90d`, `org_freshness`); last 3 `manual_notes` (non-narrative); previous narrative truncated to 150 words. `narrative_v1.md` instructs: "≤ 300 words, past-tense factual synthesis, no price prediction, every risk/catalyst must cite an item in the bundle by date; if evidence is thin say so." Output `NarrativeV1` → `manual_notes`. Governor (unreserved call, 02 §G5): `check` the estimated cost per subject before each call, `record(…, requests=1, …)` after it (§6.3); a denial skips the remaining subjects.

### 8.6 Step order

1. `schema.py`, `rubric.py`, prompts, `tests/fixtures/rubric_hashes.json` (hash of the shipped rubric) — unit tests green offline.
2. `aliases.py`, `queue.py` (SQL + privacy gate), `rules.py` + `lexicon.py`; `llm_rules_fallback` job; `resolve.py` + `LABEL_POLICY` added to `scoring/spec.py` with the three version strings.
3. `events.py` + `enqueue_review`/`confirm_review` (wired to 11's `resolve_review`); `features/sentiment.py` + registration; run scoring with rules-only labels → P6 appears.
4. `llm_classify.py`: submit/poll/apply with a fake client fixture; then real key smoke test with `LLM_BATCH_MAX_REQUESTS=1`.
5. Escalation; cost-governor wiring (reserve/commit/release, check/record); `/health` fields.
6. `narrative.py`.
7. `eval.py` + golden set bootstrap (§9.3).

---

## 9. Tests

All tests offline by default; the Anthropic client is a fake (`tests/nlp/fakes.py::FakeAnthropic`) replaying fixtures from `tests/fixtures/llm/`.

**Unit**
- `test_schema.py`: valid batch fixture parses; unknown `event_type` → item rejected (not the batch); `summary` > 140 chars truncated; `direction` outside `ALLOWED_DIRECTION` clamped with `clamped=True`; `schema_v ≠ 1` → whole request rejected; count/position mismatch → salvage matched positions, return unmatched post_ids; `emissions_tokenomics_change` with direction −1 clamped to 0.
- `test_rubric.py`: sha of shipped `classify_v1.md` equals `rubric_hashes.json`; CRLF file yields the same hash; `classifier_version("claude-haiku-5-5", sha, 1) == "claude-haiku-5-5:" + sha[:8] + ":1"`; rubric token estimate ≥ 600.
- `test_queue.py` (sqlite fixture on 03's DDL with 40 rows across the four tables): only `announcements` rows with text are selected (Discord text with `via_own_server=0` cannot exist — 03 CHECK); `bot_filtered=1` X posts skipped; pending Telegram rows (`text_purge_after` set) ordered first; items already labelled by the bulk version excluded, `rules:v2`-only included while text exists; post ids are `x:…`/`discord:…`/`telegram:h/n`/`news_item:…`; text assembly for news = title+summary; truncation; dedup by `text_hash` copies labels with `dedup_of`.
- `test_pack.py`: 47 items → 3 requests (20/20/7); items ordered by `post_id` with prompt ids `1..n`; rendered block escapes `</item>` inside text.
- `test_rules.py`: 30-item mini golden subset → each expected event_type/polarity; never materiality 3; generic "Score" without context → no entity; `$SN64` → 64@1.0.
- `test_resolve.py`: rows for bulk v1, escalation v1, rules → escalation wins; escalation `observed_at > as_of` → bulk wins at that `as_of`; a version not in the policy is invisible.
- `test_escalation.py`: bulk labels (−1, mat 3), (`security_incident`, mat 2), (+1, mat 3), (−1, mat 2) → exactly the first two escalate; daily cap respected (overflow → review row); a governor `check` denial leaves the label pending without consuming a try; pending > `LLM_FALLBACK_AFTER_H` → review row; Sonnet fixture returns mat 2 → the resolved label is mat 2 and **no critical event** exists; Sonnet returns mat 3 → critical event written with `payload.confirmed_by='claude-sonnet-5-5'`.
- `test_events.py`: 6 labels about SN64 `product_launch` within 30 h of one 48-h bucket → one event, `n_supporting=6`, `event_at` = earliest, `dedupe_key = "{occ}|product_launch|{bucket}|pos"`, severity `info`; negative mat 2 → `warning` (never `warn`); `source='llm'`; opposite directions → separate rows; mat 1 → no event; negative mat 3 pending (bulk only) → review row, no event; `confirm_review` → critical event with `payload.confirmed_by`, and **no** `manual_flags` row; a second `enqueue_review` for the same `(post_id, label_version)` is a no-op; an alerted event's payload is never updated.
- `test_cost.py` (governor fake with 16's keyword-only signatures — a positional argument after the amount raises `TypeError`): `estimate_batch` on 20 items ≈ expected tokens ±10 %; `reserve` returning `None` → `submit_cycle` returns `{skipped:"budget"}`, makes no Anthropic call, and the fallback job labels the backlog; `create` raising → `release(res_id)`; after apply → `commit(res_id, usage-derived USD (batch ×0.5, cache read ×0.1), detail=…)` then `record(…, 0.0, requests=0, units=…)`; `commit` raising `UnknownReservation` → `record(actual)`; Sonnet/Opus: `check` then `record(…, requests=1, …)`; every SDK call awaited `limiter.acquire("anthropic")`; unpinned bulk version → `{skipped:"version_unpinned"}`.
- `test_batch_lifecycle.py`: submit → `llm_batches.status='submitted'`; retrieve `in_progress` → no change; `ended` with 1 succeeded + 1 errored(invalid_request) + 1 expired → labels written for the first, requeue at 10/request for the second, requeue for the third; simulate restart (new `LlmClassifier`) → `poll_cycle` resumes the same `provider_batch_id`; apply twice → row count unchanged (idempotent).
- `test_sentiment_metrics.py`: fns are called as `compute_<key>(db, occ, as_of=datetime(…, tzinfo=UTC))` (a `str` raises); hand-computed fixtures — `event_impact_14d`: clusters (+3 @ 0 d, −2 @ 7 d, +2 @ 13 d) → `x = 3 − 1 + 2·0.5^(13/7) = 2.552045` (norm .927711), and an extra +2 @ exactly 14 d is **outside** the half-open window (`(t − 14 d, t]`: x unchanged); no clusters with coverage 1.0 → `ok` 0.0, never `absent`; coverage 0.4 and x = 0 → None `labels_pending`; an unconfirmed bulk mat-3 negative does not count, the same after a `confirm_review` event observed before t counts; `polarity_7d`: 6 authors, one with 70 % of the weight → `cap_author_share` gives it exactly 25 % and the others 15 % each (a one-shot cap would leave 45 %), expected value to 1e-6; 4 posts → `not_applicable`; `attention_7d`: 97-d synthetic series, z to 1e-6, baseline excludes the 7 recent days, < 21 baseline days → None, no trending rows + mentions present → mentions-only z; `kol_coverage_30d`: credibility 0.4 KOL excluded, KOL that is the occupant's own team excluded, no ok `x_kol_poll` in 3 d → None.
- `test_pit_invariance.py`: build labels v1 at T0, compute all four metrics at `as_of=T1` → snapshot; insert relabels under `claude-haiku-5-5:deadbeef:2` with different polarities/materialities at T2 > T1; recompute at `as_of=T1` with the same policy → **identical**; recompute at T1 with a policy pinning the new version → still identical because those rows have `observed_at > T1`; recompute at T3 with the new policy → differs. An unalerted event's payload upgraded at T2 does not change `event_impact_14d` at T1. Also: no code path issues `UPDATE post_labels`.
- `test_privacy.py`: no `announcements` row without text, no Discord row with `via_own_server=0` and no `bot_filtered=1` X post appears in `build_batch_request` payloads (assert on the fake client's captured requests). Text NULLing is 03's retention (tested in 03).
- `test_narrative.py`: subject selection with fixture scores (A/B tiers, movers, new flag); evidence bundle ≤ budget and section order; Opus fake returns `NarrativeV1` → `manual_notes` row with `kind='llm_narrative'`; `stop_reason='refusal'` fixture → no row, no raise.

**Integration / smoke (need `ANTHROPIC_API_KEY`, `LLM_EVAL_ALLOW_NETWORK=1`, marked `@pytest.mark.network`)**
- Submit one 5-item batch on Haiku, poll to `ended`, parse — confirms structured-output acceptance on Haiku in Batches (skill says supported; if a 400 appears anyway, drop `format` and rely on pydantic parsing).
- One Sonnet escalation call with fixture item 3 (key compromise) → `security_incident`, −1, 3.
- `count_tokens` on the rubric ≥ 600.

### 9.3 Evaluation harness and golden set

`tests/fixtures/golden_labels_v1.jsonl` — 200 items, one JSON per line: `{id, item_kind, platform, author_kind, source_netuid, event_at, text, gold:{polarity, event_type, direction, materiality, entities:[netuid…], is_spam, is_promo}, labeller, labelled_at, note}`. Composition targets: ≥ 8 per event_type where the ecosystem provides examples (≥ 3 for rare ones), ≥ 40 spam/promo, ≥ 30 entity-ambiguity cases (generic names, multi-subnet, network-level), ≥ 20 network-level items with `entities=[]`, ≥ 25 from announcements/news/video kinds. **Bootstrap:** the implementer ships 40 synthetic items modelled on research 01-web-ecosystem-2026 §2/§5 (Covenant exit, Quasar audit, ORO/Connito key compromise, Vanta Kraken listing, Synth buyback, Targon ARR, Score SSL expiry, June-22 emission cut, giveaways) and a script `python -m app.nlp.eval bootstrap --n 160` that samples real collected items after week 1, pre-labels them with the bulk model, and writes them with `labeller='llm-prelabel'` for the operator to correct (checklist: add as D6 — ~2 h once). Items keep `labeller` so pre-labelled-but-unreviewed rows can be excluded (`--reviewed-only`).

`python -m app.nlp.eval run --golden tests/fixtures/golden_labels_v1.jsonl --classifier rules|bulk|escalation [--reviewed-only]` prints: event_type precision/recall/F1 per class + macro-F1, polarity accuracy and bullish/bearish confusion, direction accuracy, materiality MAE and exact-match, entity precision/recall (per netuid), spam/promo precision/recall, tokens and USD (LLM modes, via the governor with `reserve` of the full run). Targets for accepting a rubric: Haiku macro-F1 ≥ 0.70, polarity accuracy ≥ 0.80, entity precision ≥ 0.90 (a wrong subnet is worse than a miss), spam precision ≥ 0.85; rules: macro-F1 ≥ 0.45, entity precision ≥ 0.90. Results saved to `data/eval/{classifier_version}.json`.

**Drift check:** `python -m app.nlp.eval drift --old <classifier_version> --new <classifier_version>` compares the two saved results and, on the same 200 items, prints per-class deltas and the label agreement rate; a rubric change is accepted only with agreement ≥ 0.85 on non-`other` classes or an explicit note in the `ts-1.x` changelog. `pytest` includes a test that fails when `classify_v1.md`'s hash changes without `rubric_hashes.json` and `LABEL_POLICY` being updated together (guards accidental relabels).

---

## 10. Verify checklist

```
# 0. offline tests
pytest tests/nlp -q                                   # all green, no network
python -m app.nlp.rubric --print-version              # → claude-haiku-5-5:<sha8>:1 ; token estimate ≥ 600

# 1. rules-only P6 (LLM_ENABLED=false)
python -m app.jobs run llm_rules_fallback             # collector_runs.ok=1, rows>0
sqlite3 data/taosentinel.db "select classifier_version,source,count(*) from post_labels group by 1,2"   # rules:v2|rules N
python -m app.jobs run scoring_6h
sqlite3 data/taosentinel.db "select metric_key,count(*),sum(value is null) from metric_values where source='llm' and stage='raw' and as_of >= strftime('%s','now','start of day') group by 1"
#   → four metric_keys, one row per universe occupant each; polarity_7d NULL where n<5; no status 'absent' on P6 rows

# 2. batch lifecycle (LLM_ENABLED=true, LLM_BATCH_MAX_REQUESTS=1)
python -m app.jobs run llm_classify_submit            # log: submitted batch msgbatch_… n_items=20 est_cost_usd≈0.01
sqlite3 data/taosentinel.db "select key from job_state where job='llm_classify_submit' and key like 'resv:%'"   # one reservation row
python -m app.jobs run llm_batch_poll                 # repeat until log: applied batch … n_succeeded=1 ; reservation committed, job_state row gone
sqlite3 data/taosentinel.db "select status,n_items,cost_usd,input_tokens,output_tokens from llm_batches"
sqlite3 data/taosentinel.db "select classifier_tier,count(*) from post_labels group by 1"      # bulk ≥ 20
sqlite3 data/taosentinel.db "select substr(post_id,1,instr(post_id,':')),count(*) from post_labels group by 1"   # x: discord: telegram: news_item: video_item: only
curl -s localhost:8090/health | jq '.collectors.llm_batch_poll, .spend.llm'               # queue_depth, open_batches=0; spend usd>0, cap_usd 60

# 3. escalation + events + review
sqlite3 data/taosentinel.db "select count(*) from post_labels where classifier_tier='escalation'"   # ≥0; fixture-driven in tests
sqlite3 data/taosentinel.db "select event_type,severity,count(*) from events where source='llm' group by 1,2"   # severities info|warning|critical only
sqlite3 data/taosentinel.db "select id,occupant_key,suggested_flag,status,dedupe_key from review_queue where kind='llm_flag_candidate' order by id desc limit 5"
# confirm one owner_extraction candidate through 11 (CLI/API/Telegram) → manual_flags row owner_extraction/confirmed + event payload.confirmed_by

# 4. PIT + version rules
pytest tests/nlp/test_pit_invariance.py -q
grep -rn "UPDATE post_labels" app/ | wc -l            # → 0

# 5. cost governor
LLM_MONTHLY_CAP_USD=0.001 python -m app.jobs run llm_classify_submit   # log: skipped reason=budget ; no Anthropic call
touch KILL_SWITCH; python -m app.jobs run llm_batch_poll; rm KILL_SWITCH   # poll still runs (free job); escalations denied (kill_switch)

# 6. narrative (Sunday 01:00Z or forced)
python -m app.jobs run llm_narrative_weekly --force   # manual_notes kind='llm_narrative' rows for tier A/B ∪ movers, ≤ LLM_NARRATIVE_MAX_SUBNETS
sqlite3 data/taosentinel.db "select occupant_key,created_by,length(body),json_extract(payload_json,'$.cost_usd') from manual_notes where kind='llm_narrative' order by created_at desc limit 3"

# 7. privacy (text retention itself is 03's storage_retention_daily policy nlp_text_retention)
sqlite3 data/taosentinel.db "select count(*) from announcements where platform='discord' and via_own_server=0 and text is not null"   # → 0 (CHECK)
sqlite3 data/taosentinel.db "select count(*) from announcements a where text_purge_after is not null and text is not null and exists (select 1 from post_labels p where p.post_id=a.msg_id) and a.observed_at < strftime('%s','now','-1 day')"   # → 0 after a retention run

# 8. eval
python -m app.nlp.eval run --classifier rules --golden tests/fixtures/golden_labels_v1.jsonl        # macro-F1 printed, entity precision ≥ 0.90
LLM_EVAL_ALLOW_NETWORK=1 python -m app.nlp.eval run --classifier bulk --reviewed-only               # ≤ $0.30, macro-F1 ≥ 0.70
```

Phase 9 (this spec's half) is done when steps 0–5 and 7–8 pass on the laptop and P6 values appear in `/api/v1/subnets/{netuid}` explain output for occupants with ≥ 5 labelled posts.

---

## 11. Open questions / operator decisions

1. **LLM budget (master §13.4):** default `LLM_MONTHLY_CAP_USD=60` and `LLM_ENABLED=false` until the key exists (checklist B6). Approve or lower; projected run-rate ≈ $21–31/month at 1,300 items/day with the 2026-10-18 model defaults (was ≈ $37–48 with Haiku 4.5 / Opus 5), including an UNVERIFIED thinking-token allowance (§6.3).
2. **Mat-3 negatives while escalation is unavailable** — this spec implements master §7.4 literally (not counted until Sonnet or a human confirms). Alternative considered: count at materiality 2 until confirmed, so a real incident is not invisible during a Sonnet outage or budget stop. Recommend the alternative for `ts-1.1`; kept out of `ts-1.0` to conform. Likewise, a Sonnet-confirmed `owner_extraction_evidence` counts in `event_impact_14d` before the human review, which gates only the red flag — confirm this reading of "always human-reviewed".
3. **Model snapshot drift:** resolved. The bulk default `claude-haiku-5-5` is a fixed id (no dated snapshot exists or is needed — the skill says to never append a date suffix); Sonnet 5.5 and Opus 5.5 are recorded per call in `model_resolved`, and a change is only logged (the escalation version string does not change). **Model choice (2026-10-18):** defaults moved to the current generation (§2.3); the operator may pin the previous ids in `.env` — any change is a new `classifier_version`.
4. **`output_config.format` + `effort` on Haiku 5.5 inside the Batch API** — the skill lists Haiku 5.5 for structured outputs, structured outputs as working with Batches, and its batch example sends `output_config={"effort": "low"}` to `claude-haiku-5-5`; the network smoke test confirms the combination.
5. **Trending views** — resolved: 05 writes `chain_trending_daily` (0 vs no-row). Until a 21-day history exists, `attention_7d` is mentions-only.
6. **Tables** — resolved: 03 adopted `post_labels`, `post_label_entities`, `llm_batches`, `llm_batch_items` and `review_queue` (kind `llm_flag_candidate`); this spec ships no DDL. The batch reservation id lives in `job_state` (no `llm_batches` column needed).
7. **Golden-set labelling effort** (~2 h operator time after week 1) — add to checklist D as D6 (not yet in 18).
8. **Cross-subnet operator concentration** (Rayon Labs runs SN64/56/19): the rubric links entities per subnet; a team-account post about a sibling subnet is attributed to the sibling by the model and to the source subnet via `via='source'`. Acceptable? Alternative: drop the source-bound link when the model links a different netuid with confidence ≥ 0.9.
9. **Polarity from team accounts:** teams are always bullish about themselves; including them in `polarity_7d` biases toward 0.5–0.7. Master says "team + KOL + mentions", implemented as such; consider a team weight of 0.5 in `ts-1.1`.
10. **Discord inference-only use of crossposted announcement text** — legal gray area per research 01-web-social-apis §2; mitigations here: own-server only, 90-d (or pending-only) retention, summaries not text. Operator to confirm comfort.
11. **Minimum cacheable prefix** — resolved: 512 tokens for Haiku 5.5, Sonnet 5.5 and Opus 5.5 (skill, "check the docs before relying on it"); the ≈ 4K-token prefix caches on all three.
12. **Review hand-off gaps (for 11):** 11 §2.10's `confirm_flag` accepts `llm_flag_candidate` rows only with `suggested_flag ∈ {owner_extraction, key_compromise}`; rows with `security_incident`/NULL need a confirmation that calls `confirm_review` without a `manual_flags` row. 11's `review.enqueue` lacks the `post_id`/`label_version`/`event_type`/`materiality` columns, so this spec inserts its own kind directly (03's DDL anticipates it).
13. **Mentions leg of `attention_7d`:** this spec counts entity-linked labelled items (spam-filtered, LLM-attributed). 09 §3.3 and 12 registry row 52 describe 09's label-free `x_mentions_7d` (+ `news_mentions_7d`) as the input. Either keep this definition (09/12 text to be corrected) or switch to the label-free counts in `ts-1.1`; the label-based count lags labelling by ≤ 6 h (≤ 18 h on fallback).
14. **Look-ahead caveats:** `polarity_7d` weights use engagement refreshed in place (as `x_engagement_30d`, 02 §J2) and `kol_coverage_30d` reads operator-edited `kol_accounts.credibility`; 13 already treats P6 as forward-only.

---

## 12. Sources

- `E:\TaoData\specs\02-cross-cutting-conventions.md` (binding ledger, incl. the 2026-09-23 amendments: A2, B1–B4, B7, B9–B12, C4–C6, D1–D5, E1–E4, F1–F2, G1, G5–G6, J2).
- `E:\TaoData\specs\00-MASTER-SPEC.md` §1.3, §3, §4, §5, §6, §7.1–7.7, §8, §11, §12, §13.
- `E:\TaoData\specs\03-storage-schema.md` §2.5 (triggers), §2.7 (`nlp_text_retention`, `llm_batch_items` retention), §3.2 (`manual_notes`, `manual_flags`, `review_queue`, `kol_accounts`, `watch_channels`), §3.3 (`chain_trending_daily`), §3.5 (`announcements`, `social_posts`, `social_post_mentions`, `news_items`, `video_items`, `post_labels`, `post_label_entities`, `llm_batches`, `llm_batch_items`), §3.6 (`metric_values`, `events`), §3.8, §3.9, §5, Appendix A rows "10".
- `E:\TaoData\specs\12-feature-store-and-scoring-engine.md` §2.2–§2.6 (T_eval, `MetricResult`, registry rows 42–45/52–54, `fallback_key`, `event_impact` normaliser, `compute_registered`); `11-team-and-integrity.md` §2.10 (`resolve_review`, `sync_confirmed_llm_flags`); `16-ops-deploy-and-observability.md` §2.5, §2.10, §2.11, §6.2; `09-x-collector-and-kol.md` §2.6–§2.8, §3.3; `08-community-collector.md` §2.6 (textnorm, `msg_id`); `05-chain-collector.md` §3.1 (`chain_trending_daily`); `01-repo-skeleton-runtime.md` §2.6–§2.9, §6 (limiter row `anthropic`).
- `E:\TaoData\specs\INDEX.md`; `E:\TaoData\specs\18-operator-checklist.md` (B6 `ANTHROPIC_API_KEY`, `LLM_MONTHLY_CAP_USD=60`; D3 KOL credibility; D4 human-confirmed flags; D5 needs-review digest).
- `E:\TaoData\research\01-web-social-apis.md` §2 (Discord Developer Policy, own-server Follow crossposts, no third-party text), §5 (LLM cost model — its "Sonnet 5 512-token cache minimum" is superseded by the skill's 1,024), open questions (Discord inference gray area).
- `E:\TaoData\research\01-web-quant-scoring-methods.md` §2 (time decay `0.5^(Δt/h)`, half-lives, point-in-time, classifier-drift = look-ahead, `spec_version`), §3 (label sets polarity+intensity 1–5, event taxonomy, bot/shill heuristics, engagement weighting with per-account cap, predictive-value evidence).
- `E:\TaoData\research\01-web-ecosystem-2026.md` §1–2, §5 — few-shot material and golden-set seeds.
- Bundled `claude-api` skill v2.1.301 (model table cached 2026-10-06, re-read 2026-10-18: §2.3's ids, prices, thinking/effort table, Haiku 5.5 migration notes, caching minimums, structured-output models, `fallbacks="default"`; earlier: v2.1.280, cached 2026-06-24, read 2026-09-23): SKILL.md model table and thinking/effort table; `shared/models.md` (full id `claude-haiku-4-5-20251001`; Sonnet 5/Opus 5 alias-only; Sonnet 5 tokenizer); `python/claude-api/batches.md` (100k requests / 256 MB, ≤ 24 h, results 29 d, result types, key by `custom_id`, caching in batches); `shared/tool-use-concepts.md` § Structured Outputs (supported models incl. Haiku 4.5, works with Batches, schema limits); `shared/prompt-caching.md` (minimums: Opus 5 512, Sonnet 5 1,024, Haiku 4.5 4,096; write 1.25×, read 0.1×); `shared/token-counting.md`; `python/claude-api/README.md` (refusal/`stop_details`, server-side fallbacks, SDK retries); `python/claude-api/sdk-upgrade.md` (1.x: `httpx2`, Python ≥ 3.10, sampling params removed).
- Old-bot files: none (brief). Runtime/job-wrapper/config conventions are inherited via specs 01 and 16.

## Changelog
- 2026-10-18 pre-build review (Phase 9), against the `claude-api` skill v2.1.301 (cached 2026-10-06), the installed `anthropic==1.15.0` and the built 03/11/12 code:
  - §2.3/§5/§6.3: model defaults → `claude-haiku-5-5` (bulk; $0.10/$0.50, Batch $0.05/$0.25, fixed id), `claude-sonnet-5-5` (escalation), `claude-opus-5-5` (narrative, $4/$20). Haiku 5.5 thinks adaptively by default → `effort="low"` (frozen `BULK_EFFORT`), `max_tokens` 8192, blocks read by type, refusals = parse failures; cache minimum 512 on all three; Sonnet/Opus calls use `fallbacks="default"` (new `LLM_REFUSAL_FALLBACK`). Budget ≈ $21–31/month (was $37–48). `PRICES` lists the new ids.
  - §8.3: the async results form is verified (`async for r in await client.messages.batches.results(id)`).
  - §2.2: `LABEL_POLICY` lives in `scoring/spec.py` as a module constant, not a `ScoringSpec` field (12's golden spec hash stays valid).
  - §8.2: `confirm_review(..., note=None)` — 11's built `review.py` calls it with `by=` only.
  - Build (same day): §8.4 regexes widened, incident wording required for `security_incident`, ordinary-word names linked at 0.5 unless a subnet cue is adjacent; the rules version is **`rules:v2`** (72 `rules:v1` labels written by the first live run stay in `post_labels`, outside `LABEL_POLICY`). Build deviations: BUILD-LOG Phase 9.
- 2026-09-23 reconciliation + review:
  - B7/B1/B11: all DDL removed (03 owns `post_labels`, `llm_batches`, `review_queue`, …); INTEGER times; item ids `x:{id}`, `discord:{source_message_id}`, `telegram:{handle}/{n}`; 03 Appendix A names (`bot_filtered`, `kol_handle`, `like_count`, `chain_trending_daily`, `created_by`); textnorm import.
  - B10 (amended): pending-classification text labelled in the normal 6-h batch (queued first, escalated in the same poll run); 90-d Telegram allowance and the `nlp_text_retention` job dropped (03 policy).
  - C4/C5/B9: fns `compute_<key>(db, occupant_key, as_of: datetime)`, `as_of` = 12's T_eval; `source='llm'`; 12 writes the rows; P6 never `absent` (x=0 → `ok` 0.0 → 0.5).
  - D1/D2/B2/D4: events via `emit_event`, `dedupe_key` per 03's pattern, `warning` severity; no 10 system events (failures page via 01, budget via 16).
  - D5/A2: review hand-off through 11's `resolve_review` → `manual_flags(owner_extraction)`; `confirm_review` never writes flags; `reject_review` dropped.
  - G5/G1: reserve → commit → release (+ `UnknownReservation` → `record`), `check`+`record(requests=1)` for Sonnet/Opus, keyword args, `limiter.acquire("anthropic")`; `/health.spend.llm`.
  - E2/B12: one job table (`llm_classify_submit`, `llm_batch_poll`, `llm_rules_fallback`, `llm_narrative_weekly` Sun 01:00Z); poll/rules free so paid batches still commit; pause reasons per B12.
  - Review: event_impact from append-only labels (events payload mutable → PIT leak); water-filled 25 % cap (one-shot cap left 45 %); 14-d boundary fixture fixed (x = 2.552045); non-overlapping attention baseline.
  - Review: `emissions_tokenomics_change` fixed at direction 0 (master §7.4); `owner_conviction` → P4 `owner_commitment`/`owner_retention_30d`.
  - Review (skill): Haiku dated id default, no `effort` on Haiku, Sonnet cache min 1,024, structured outputs in Batches confirmed, max_tokens 4096/8000 for thinking models, cost math incl. thinking (≈ $37–48/mo), escalation cap 40/day.
