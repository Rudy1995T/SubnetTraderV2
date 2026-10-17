# 11 — Team & governance integrity (manual team evidence, owner analytics S1–S5, control-change detectors, P4 metrics)

**Status:** proposed (reconciled with 02 on 2026-09-23) · **Phase:** 7 · **Depends on:** 02 (binding ledger — B1–B4, B7, B9, B12, C1, C2, C5, C6, D1–D5, E1–E4, F2, G1–G3, H3 are applied here and win over any sibling text), 01 (Settings, `CollectorJob` wrapper, `JOB_TABLE`, `/health`, `Http`/`ProviderLimiter`), 03 (owns every DDL this spec touches, the events catalog §3.9 and the metric-key catalog §3.8), 04 (`occupants`, `subnets.owner_coldkey`, `identity_observations` field `team_info_html`, `identity_resolved` `owner_*` fields, `identity_changed` events, `manual_notes`, the `chain_rest_snapshots` kinds `subnets` and `coldkey_identities` — 02 §G2), 05 (`TmcClient`, the only TMC caller; `chain_hourly` owner/pool fields; units probe; the TMC budget table — 02 §G2/§G3), 10 (`review_queue` kind `llm_flag_candidate`, `confirm_review`), 12 (raw-row contract, `prenorm`/`binary`/`tenure` scoring of the P4 rows, the red-flag state machine — this spec writes the **final** P4 scores, raw `tenure` days and the two trigger rows, 02 §C1/§C2), 14 (API/Telegram transport that calls the domain functions defined here; `manual_flags`), 17 (`rpc_owner_history`, keyless owner-key history for S3 back-fill) · **Master spec refs:** §1.2, §1.3, §2 (`quality.py` `_ANON_TOKENS`, `app/whale/events.py` port), §4 (TMC keyless tier-0, TaoStats tier-2), §5, §6 (`team_members`, `metric_values`, `events`), §7.2, §7.3 P4, §7.5 (`owner_extraction`, `owner_control_change`, `key_compromise`), §7.7 (team freshness 90 d), §8 row "Team & integrity", §10, §11, §12 Phase 7, §13 item 2.

---

## 1. Purpose and scope

Produce the `stage='raw'` rows of all seven **P4 · Team & Governance Integrity** metrics (pillar weight 18) for every non-root occupant: the six scores `team_doxxed`, `owner_identity`, `owner_commitment`, `owner_retention_30d`, `control_stability_90d`, `owner_overhang` as **final [0,1] values** in `value` (spec 12 passes them through `prenorm`, `owner_identity` through `binary`), with their raw inputs in `value_json` under spec 12 §2.4's keys, and `tenure` as **raw days** (spec 12 applies `zclip(days, 120, 365)`, floor 0.3) — 02 §C1. Plus the two aux trigger rows spec 12's red-flag gates read, `owner_extraction_trigger` (→ `owner_extraction`) and `owner_control_change_trigger` (→ `owner_control_change`) (02 §C2), the evidence-only keys, the `key_compromise` human override, and the human-review queue behind all of them. The master §7.3 formulas are computed here; spec 12's reference functions (`team_map`, `owner_commitment`, `owner_retention`, `control_stability`, `owner_overhang`) are the test oracle on shared anchors (§9 T43).

**In scope**
- **A. Team evidence.** `team_members` (occupant-keyed, evidence-linked, `candidate → verified | rejected`), the subnetalpha `team_info` HTML seed parser and the `research/subnet_edge` legacy seed (candidates only — **never auto-verified**), the two-source verification rule, domain functions consumed by spec 14 (API + Telegram) and the CLI `python -m scripts.manual_entry team add|verify|reject|list|anon`, 90-day re-verify reminders, `team_doxxed`.
- **B. Owner analytics.** Owner coldkey/hotkey and owner-cut fields from TMC `/subnets/{id}/` `latest_snapshot`; owner position and locks from `/accounts/coldkey-stakes/{owner_ck}/` and `/analytics/neuron/{netuid}/{owner_hk}/?span=1W`; owner lock fields from spec 05's `chain_hourly`; the Rufus conviction table JSON; owner sells from `/extrinsics/staking-activity/?coldkey=` (+ `/transactions/stakes/stake-removed/` reconciliation) with watermarks; S1 `owner_retention_30d`, S2 `owner_commitment`, S3 `control_stability_90d`, S5 `owner_overhang` exactly per master §7.3; S4 as a red-flag **event only** (metric design for ts-1.1); the pre-2026-05-08 hidden-initial-alpha adjustment; TaoStats-keyed upgrades behind `TAOSTATS_API_KEY`; subnetradar.com/swaps daily cross-check.
- **C. Detectors.** Coldkey swap / owner transfer, owner-hotkey re-point + identity churn within 14 d, perpetual-lock-off (formal exit), extraction (S1 hard triggers), auto-lock/owner-cut toggles, source disagreement, the `key_compromise` override, and review-queue items for each.
- **D.** `owner_identity` (from spec 04's resolved TMC coldkey identity) and `tenure` (raw days from `tenure_days(head_block, network_registered_at)`; spec 05's `occupant_age_days` is spec 12's fallback key; §2.11).
- **E.** The writes to 03's tables `team_members`, `owner_snapshots`, `owner_position_hourly`, `owner_stake_events`, `owner_changes`, `owner_event_whitelist` and this spec's kinds on the shared `review_queue` (03 owns all DDL — 02 §B7; §3.1 lists the columns this spec relies on).

**Out of scope**
- Applying red-flag multipliers and their 7-day hysteresis (spec 12 reads the trigger metrics and `manual_flags` defined here). Alert routing/digest rendering (14). LLM classification (10 — this spec only consumes its confirmed reviews). Identity merge (04).
- **Fetching any profile URL.** No LinkedIn/X/GitHub profile is ever requested by this spec (master §1.2: no LinkedIn automation). URLs are stored as evidence; verification is a human act.
- Any wallet, key or signing code; `bittensor` is not imported here.
- S4 as a scored metric (ts-1.1), owner-activity history older than `OWNER_ACTIVITY_BACKFILL_DAYS` (optional CLI backfill, §8), the TaoStats subscription decision (operator, master §13 item 2).
- Writing `stage='norm'` rows and normalising `tenure` (spec 12 owns `prenorm`/`binary`/`tenure` and their frozen parameters).

---

## 2. Design

### 2.1 Components and data flow

```
 identity_observations (04: subnetalpha team_info_html) ─┐
 seed/subnet_edge/sn*.md (legacy, one-shot) ─────────────┼─► team_seed_daily (DB-only, 04:20Z) ─► team_members(candidate) ─► review_queue(team_candidate)
 CLI scripts.manual_entry | API/Telegram (14) ──► app/integrity/team.py ─► team_members(verified|rejected) ─► events team_member_*

 TmcClient (05; 01 ProviderLimiter 'tmc' ≤ 8/min)                 Rufus table.json / baked-at.json   subnetradar /swaps   TaoStats (keyed only)
   C1 /subnets/{n}/  (or 04's kind='subnets' snapshot: 0 req)               │                          │                   │
   C2 /accounts/coldkey-stakes/{owner_ck}/   (1 per distinct owner coldkey) │                          │                   │
   C3 /analytics/neuron/{n}/{owner_hk}/?span=1W                             │                          │                   │
   C4 /extrinsics/staking-activity/?coldkey=…&FROM&TO  (1+ per owner ck)    │                          │                   │
   C5 /transactions/stakes/stake-removed/?hotkey=…&subnet=n  (≤ 20/day)     │                          │                   │
        └─────────────────────────► owner_collect_daily (20:30Z) ◄──────────┴──────────────────────────┴───────────────────┘
                                     │ owner_snapshots · owner_position_hourly · owner_stake_events · chain_rest_snapshots(raw)
                                     ▼
 chain_hourly (05 SSE) ─► owner_watch_hourly (DB-only, :12) ─► detectors D1–D9 ─► owner_changes ─► events + review_queue
 identity_changed events (04) ───────────────────────────────►        ▲
 owner_event_whitelist · manual_flags(key_compromise) ────────────────┘  (exclusions, PIT by created_at)
                                     ▼
 integrity_features (00:07Z official; 06/12/18:07Z provisional) ─► metric_values (raw): 7 P4 keys + 2 triggers + evidence ─► spec 12 (prenorm/tenure, flags)
 rpc_owner_history (17, one-shot + after each backfill) ─► owner_changes (detected_via='rpc_owner_history', S3 history)
```

**Identity rule.** Every row carries `occupant_key = f"{netuid}:{network_registered_at}"`. Owner detectors compare snapshots **within one occupant only**; a re-occupied netuid starts with empty team and owner history (a new owner coldkey after `netuid_reoccupied` is not a "change"). Netuid 0 (root) is excluded everywhere.

**Point-in-time rule.** Metric computation at decision time `t` reads only rows with `observed_at ≤ t` (owner tables), whitelist/override rows with `created_at ≤ t`, and team state reconstructed as of `t` (§2.2.6). **Replay exception (spec 17 / 12 reconstructed mode):** when a `compute_<metric_key>` entry point is called with `mode='reconstructed'` (17's history replay, 13's reconstruction), rows of `owner_changes`, `owner_snapshots` and `owner_stake_events` that are **backfilled** use `event_at` as their PIT time — they are admitted iff `event_at ≤ t` (for `owner_snapshots` with NULL `event_at`, `observed_at` still applies) — while every live row keeps the `observed_at ≤ t` bound and whitelist/override rows keep `created_at ≤ t`. "Backfilled" = `backfilled = 1` where 03 carries the column; 03 v1 has none on these three tables, so the marker is `json_extract(evidence_json, '$.backfilled') = 1` on `owner_changes` (always set by `owner_rpc_seed`, and on TaoStats owner-history imports) and `json_extract(raw_json, '$.backfilled') = 1` on `owner_snapshots`/`owner_stake_events` (set by 17's back-fill and by C4/C8 history pages older than the coldkey's first live watermark) (§11 Q2 (g)). This is what lets `control_stability_90d` (from the seeded `rpc_owner_history` changes) and `owner_commitment` be reconstructed; rows computed this way are written with `backfilled = 1`. Strict mode (the default, and every live run) never uses `event_at` as the PIT time. Stale inputs become `None`; nothing is forward-filled — every "latest value" read in §2.5–§2.11 carries an explicit maximum age (`OWNER_SNAPSHOT_MAX_AGE_HOURS` for owner snapshots, 2 h for `chain_hourly`, `OWNER_RUFUS_MAX_AGE_HOURS` for Rufus).

**Time encoding.** Every stored time this spec reads or writes is `INTEGER` unix seconds UTC (02 §B1 — including 04's `occupants.registered_at_est`/`registered_ts` and `identity_observations.event_at`); ISO-8601 appears only at the CLI/API/Telegram boundary (`app/util/time.py::to_iso/parse_ts`, owner 01). `reg_ts(O) = COALESCE(occupants.registered_ts, occupants.registered_at_est)` (exact block time when 17 has it, else 04's estimate).

### 2.2 Team evidence (A)

#### 2.2.1 Status machine

| from → to | function | preconditions |
|---|---|---|
| (new) → `candidate` | `add_member(..., verify=False)`, seeders | occupant exists and is not deregistered; `(occupant_key, name_norm)` unique (a duplicate returns the existing id with `error='duplicate_member'`) |
| (new) → `verified` | `add_member(..., verify=True)` | two-source rule (§2.2.2) passes; else the row is created as `candidate` and the result carries `warning='two_source_rule'` |
| `candidate` / `rejected` → `verified` | `verify_member` | ≥ 1 new evidence item (an `evidence_url`, or an operator attestation `note`, §2.2.2); two-source rule passes on `profile_url` ∪ all evidence ∪ attestation notes |
| `verified` → `verified` (re-verify) | `verify_member` | same; refreshes `last_checked_at`, appends evidence, keeps `verified_at` |
| `candidate` / `verified` → `rejected` | `reject_member` | `reason` required (1..300 chars) |

Seeders never change `status`, never touch `verified_*`/`rejected_*`, and never resurrect a rejected row (the unique key blocks re-insert; they only bump `source_last_seen_at`). Every transition to `verified` sets `evidence_url` (the first evidence URL when none is stored), `verified_at` (first verification only) and `verified_by` (02 §H3); 03's CHECK additionally requires `evidence_count ≥ 2`.

#### 2.2.2 Two-source rule (whale_labels.json policy, generalised)

"Never assert an identity without two independent public sources" (old `data/whale_labels.json` `_README` array, lines 2-11; the POLICY sentence is lines 8-9; `verified` = "two independent reputable sources", line 6). Implemented as `check_two_source(profile_url, evidence_urls, *, attestation_notes=(), org_handles, x_handles) -> TwoSourceResult(ok, domains, attestations, evidence_count, reason)`:

```
urls         = [u for u in [profile_url, *evidence_urls] if u]          # all must be http(s), ≤ 500 chars
dom(u)       = registrable_domain(u)  # lower host, strip "www."/"m."/"mobile.", last 2 labels; last 3 when the last 2 ∈
                                      # {co.uk, com.au, co.jp, com.br, co.in, github.io, vercel.app, netlify.app, pages.dev, substack.com, medium.com}
excluded     = TEAM_EVIDENCE_EXCLUDED_DOMAINS (default {"subnetalpha.ai"})     # the seed source is not evidence
personal(u)  = linkedin.com/(in|pub)/…  |  (x|twitter).com/<h> with h ∉ x_handles (occupant's resolved X handles, spec 04)
             | github.com/<user> with user ∉ org_handles (owners of the occupant's resolved github_repo candidates, spec 04)
             | t.me/<h> | medium.com/@<h> | <h>.substack.com
attest(u)    = not personal(u) and dom(u) ∉ excluded    # official site/docs page, subnet X account post, org GitHub page, press, identity page
notes        = distinct stripped operator attestation notes (10..300 chars; the `note` / `evidence_note` a human
               caller — CLI, API, Telegram — passes to verify_member / add_member(verify=True); seeders never pass one)
evidence_count = |{dom(u) : u ∈ urls} − excluded| + |notes|                 # 02 §H3: independent items; a note is one
ok  ⇔  len(evidence_urls) + |notes| ≥ 1
     ∧ |{dom(u) : u ∈ urls} − excluded| ≥ 1                                 # at least one public URL source
     ∧ evidence_count ≥ TEAM_VERIFY_MIN_SOURCES (2)
     ∧ |{u : attest(u)}| + |notes| ≥ 1
```

`evidence_count` counts evidence **URLs** (distinct non-excluded registrable domains over `profile_url` ∪ evidence) **and** operator attestation notes (02 §H3: ≥ 2 independent items, the second may be an attestation note); it is the value written to `team_members.evidence_count` and checked by 03's CHECK (`≥ 2` for `verified`). A note is stored as an `evidence_json` item `{url: null, domain: null, class: 'attestation', added_by, added_at, note}` and counts once. Examples: LinkedIn profile + the same LinkedIn URL as evidence → 1 → fail; LinkedIn + personal X → 2 domains but no attestation → fail; LinkedIn + `https://macrocosmos.ai/team` → pass (2); LinkedIn + operator note "confirmed on the team's Discord AMA 2026-09-20, role CTO" → pass (1 domain + 1 note = 2); a note alone (no public URL) → fail; anything + `subnetalpha.ai/...` → subnetalpha does not count. There is no override flag beyond the attestation note, which is a recorded, attributed evidence item.

#### 2.2.3 subnetalpha `team_info` seed parser (`app/integrity/team_parse.py`, pure)

Input: the latest `identity_observations` row with `source='subnetalpha'`, `field='team_info_html'` for the occupant (spec 04 stores it on hash change; `event_at = modified_gmt`). Research facts: median 1,572 chars; 70/128 records mark names as `<strong>Name</strong> – Role` (354 names); 16/128 contain LinkedIn URLs (162 distinct); 7 embed X profile links; ~15 are placeholders (`Awaiting Data...`, `Awating Data...`, `No team members have been publicly disclosed`, `The core team behind ORO is kept private`); Aug-2026 LLM prose uses heading-like `<strong>Public Team Members and Handles:</strong>` and leaks `data-start=`/`class="text-token-…"` attributes.

`parse_team_info(html: str) -> TeamInfoParse(disclosure, candidates: list[Candidate], text_len, html_hash, anon_marker)`:

1. **Tokenise** with a stdlib `html.parser.HTMLParser` subclass: text split into *lines* at `p, li, br, div, tr, h1–h6`; inside a line keep ordered spans `('strong', text)` (for `<strong>`/`<b>`), `('text', text)`, `('a', href, text)`. All attributes except `a[href]` are ignored (this discards the leaked ChatGPT markup). `html.unescape` on text.
2. **Candidates from strong spans.** For each strong span `S` in a line: `S' = S.strip(" :–—-|")`; role `R` = the following text in the same line up to the next strong span, stripped of a leading `[–—\-:|,]` separator, truncated to 60 chars.
   - If `is_plausible_name(S')` → candidate `(name=S', role=R or None)`.
   - Else if `S'` matches `ROLE_WORDS` (`co-?founder|founder|ceo|cto|coo|cfo|lead|engineer|developer|researcher|head of \w+|director|advisor|product|marketing|community`) and the text after the separator (up to `,`/`;`/`(`/line end) is a plausible name → candidate `(name=that, role=S')` (role-first form `<strong>Founder:</strong> Jane Doe`).
   - Else skip (heading).
3. **Candidates/profiles from links.** For each `('a', href, text)` whose `href` matches a profile pattern: if a strong-derived candidate exists on the same line → attach as its `profile_url` (preference linkedin > x > github > website, first match per kind); else if `is_plausible_name(text)` → candidate `(name=text, profile_url=href)`; else if the href is a personal X/GitHub/LinkedIn URL → candidate `(name=<handle from URL>, profile_url=href)`.
   Profile patterns → `profile_kind`: `linkedin` = `^https?://([a-z]{2,3}\.)?(www\.)?linkedin\.com/(in|pub|company)/[^/?#]+`; `x` = `^https?://(www\.|mobile\.)?(x|twitter)\.com/([A-Za-z0-9_]{1,15})(?:[/?#]|$)` with handle ∉ {home, i, intent, share, search, hashtag}; `github` = `^https?://(www\.)?github\.com/([A-Za-z0-9-]{1,39})/?$`; any other http(s) → `website`; non-http → `other`.
4. `is_plausible_name(s)`: `2 ≤ len(s) ≤ 60`; `1 ≤ tokens ≤ TEAM_NAME_MAX_TOKENS (5)`; contains a letter; no email; no run of ≥ 3 digits; not `_looks_anonymous(s)`; does not match `\b(team|members?|contributors?|activity|handles|overview|roadmap|leadership|advisors|partners|investors|community|github|website|twitter|linkedin|discord|public|core|key|about)\b` (case-insensitive).
5. **Dedupe** within the parse on `name_norm(s)` = NFKD, strip combining marks, `casefold`, strip leading `@`, non-word → space, collapse whitespace.
6. **Disclosure** on the plain text `t` (lower-cased, whitespace-collapsed), first match wins:

| disclosure | rule | team_doxxed consequence (if no verified/candidate members) |
|---|---|---|
| `empty` | `len(t) == 0` | no editorial data → `None` |
| `placeholder` | `t` fully matches `(awaiting\|awating) (data\|content)\W*\|coming soon\W*\|tb[ad]\W*\|pending\W*`, or `len(t) < 20` with no candidates | `None` |
| `named` | ≥ 1 candidate (an anonymous phrase elsewhere sets `anon_marker=true` but does not override) | via members |
| `anonymous` | no candidates **and** (`ANON_PHRASES` anywhere **or** (`len(t) ≤ TEAM_ANON_MAX_CHARS (200)` and `_looks_anonymous(t)`)) | `0.0` |
| `unstructured` | text present, no candidates, no anonymous marker | `None` |

`ANON_PHRASES` = `kept private|not (been )?(publicly )?disclosed|no team members have been publicly disclosed|(team|founders?|developers?|contributors) (is|are|remains?) (anonymous|pseudonymous|undisclosed|private)|anonymous team|pseudonymous team|undisclosed team|prefers? to remain anonymous`. `_ANON_TOKENS` and `_looks_anonymous` are **ported verbatim** from `quality.py` line 33 and lines 108-110 into `app/features/text.py` (spec 12 §8 lists the same port as `looks_anonymous`; this spec, Phase 7, creates the module and spec 12 imports it — one copy); the length gate stops an incidental "none"/"unknown" inside 1,500-char prose from zeroing a named team (refinement recorded in §11).

#### 2.2.4 Seeding (`team_seed_daily`, DB-only)

For each non-deregistered occupant `O` (netuid ≠ 0):
1. `obs` = latest `identity_observations(occupant_key=O, source='subnetalpha', field='team_info_html')`. None → nothing to seed (`disclosure='no_record'`).
2. **Stale-for-occupant guard** (spec 04 rule R4): if `obs.event_at < reg_ts(O) − 2·86400` → do not seed; `disclosure='stale_for_occupant'` (→ `None`).
3. `p = parse_team_info(obs.value)`; for each candidate: `INSERT … ON CONFLICT(occupant_key, name_norm) DO UPDATE SET source_last_seen_at=:now, profile_url=COALESCE(team_members.profile_url, excluded.profile_url), profile_kind=COALESCE(team_members.profile_kind, excluded.profile_kind), role=COALESCE(team_members.role, excluded.role) WHERE team_members.status='candidate'` (verified/rejected rows only get `source_last_seen_at`, in a separate UPDATE). New rows: `status='candidate'`, `source='subnetalpha'`, `source_ref=f"{obs.source_ref}@{obs.event_at}"`, `evidence_url=NULL`, `evidence_note='subnetalpha team_info (unverified seed)'`, `created_by='seed_subnetalpha'`; event `team_member_added`.
4. If ≥ 1 new candidate: `review.enqueue(kind='team_candidate', occupant_key=O, dedupe_base=f"team_candidate|{O}", ref_table='team_members')` — if an **open** row with that `dedupe_base` exists its `payload_json={n_new, member_ids}` is merged; otherwise a new row is inserted with `dedupe_key = f"{dedupe_base}|{created_at}"` (03's DDL comment; the unique index on `dedupe_key` covers open rows only, so a later batch after resolution opens a fresh item under the same base).
5. **Delisting:** a `verified` member with `source='subnetalpha'` whose `name_norm` is absent from a `named` parse → event `team_member_delisted` + review `kind='team_delisted'`, `dedupe_base=f"team_delisted|{member_id}"`, `ref_table='team_members'`, `ref_id=member_id` (status unchanged — the operator decides).
6. A disclosure different from the previous observation's parse → event `team_disclosure_changed`.

**Legacy seed** (`python -m scripts.manual_entry team seed --legacy`, idempotent; also run once by the job when `TEAM_SEED_LEGACY_ENABLED` and `job_state(team_seed_daily, legacy_done)` is unset): files `{TEAM_LEGACY_NOTES_DIR}/sn{NN}_*.md` (skip `README.md`, `_*.md`); netuid from `^sn(\d+)_`; re-occupancy guard identical to spec 04 §2.11 with `as_of = TEAM_LEGACY_AS_OF` (default: spec 04's `LEGACY_AS_OF`, 2026-06-02T11:03:41Z; skip when the current occupant's `reg_ts(O) > as_of`). Per line (candidate names trimmed of trailing spaces/punctuation): `Founded by ([A-Z][\w .'-]{1,60})` → candidate role `founder`; `\b(CEO|CTO|co-?founder|founder|lead)\b[:\s]+([A-Z][\w .'-]{1,60})` → candidate with that role; `(?i)\banonymous team\b` (**case-insensitive** — the SN67 note says "Anonymous team", twice) → at most **one** `manual_notes(kind='team_disclosure', value='anonymous', evidence_url='file:seed/subnet_edge/<file>#L<first line>', body=<the matched line>, source_ref='<file>:<line>', created_by='seed_legacy')` per file. `source='legacy'`, `source_ref='<file>:<line>'`. Expected yield on the 25 `sn*.md` notes (regexes run against the files 2026-09-23): exactly SN120 "Const" (founder, `sn120_affine.md:14`) and one SN67 anonymity note (`sn67_harnyx.md:14`; the note already describes Harnyx, the post-Tenex re-use, so it imports iff the current SN67 occupant predates `TEAM_LEGACY_AS_OF`); SN88's "Awaiting content" → nothing.

`source='llm'` is reserved for a later spec-10 extension (team names from prose); such rows would enter only as `candidate` under the same rules. Not built in v1.

#### 2.2.5 Manual disclosure

`set_disclosure(netuid, status: 'anonymous'|'clear', evidence_url, note, by)` writes `manual_notes(kind='team_disclosure', value=status, evidence_url, body=note, created_at=now_ts, created_by)` (03 columns; free text lives in `body`; `team_disclosure` with `value ∈ {anonymous, clear}` is in 03's CHECK). The latest row per occupant wins; `clear` cancels a manual/legacy anonymity assertion.

#### 2.2.6 `team_doxxed` (weight .25; master §7.3 map; `team.compute_team_doxxed`)

```
state as of t: a member counts with its status as of t (first_seen_at ≤ t; verified iff a team_member_verified event with event_at ≤ t
               exists and no later team_member_rejected event ≤ t); check_time = checked_at of the latest verified event ≤ t
V      = #{verified ∧ check_time ≥ t − TEAM_VERIFY_STALE_DAYS (180)}
Vstale = #{verified ∧ check_time <  t − TEAM_VERIFY_STALE_DAYS}          # stale verifications degrade to "named"
C      = #{candidate} + Vstale
D      = latest manual team_disclosure (created_at ≤ t) if any, else parse(latest subnetalpha team_info observed ≤ t).disclosure;
         a legacy 'anonymous' note applies only when the subnetalpha disclosure ∈ {empty, placeholder, unstructured, no_record, stale_for_occupant}
team_doxxed = 1.0 if V ≥ 2 ; 0.6 if V = 1 ; 0.3 if C ≥ 1 ; 0.0 if D = 'anonymous' ; else None
```

**Row written (02 §C1; spec 12 row 27, `prenorm`):** `value` = the final `team_doxxed` — 1.0 / 0.6 / 0.3 with `status='ok'` whenever `V ≥ 1` or `C ≥ 1`; `V = C = 0 ∧ D = 'anonymous'` → `value = 0.0`, `status='absent'`, `reason='anonymous'`; every other `V = C = 0` case → `value = NULL`, `status='failed'`, `reason ∈ {no_record, empty, placeholder, unstructured, stale_for_occupant, all_rejected}`. `value_json` = spec 12's input keys `{verified: V, candidates: C, disclosure: D}` plus evidence `{stale_verified: Vstale, rejected, disclosure_source: 'manual'|'subnetalpha'|'legacy', anon_marker, member_ids}`; the evidence-only keys `team_verified_count = V` and `team_candidate_count = C` (always `ok`) are written in the same run (02 §C2). The `None` reasons surface in spec 14's `team_unknown` bucket (`all_rejected` = named text but every candidate rejected). **Re-verify reminders:** each daily official `integrity_features` run enqueues `kind='team_reverify'` (`dedupe_base=f"team_reverify|{member_id}"`, `ref_table='team_members'`, `ref_id=member_id`; `enqueue` merges only into an open row, so each 90-day cycle opens a new row once the previous one is resolved) for verified members with `last_checked_at < now − TEAM_REVERIFY_DAYS·86400`; the item auto-resolves when `verify_member` refreshes the member.

### 2.3 Owner data acquisition (B)

`owner_collect_daily` processes every occupant with `status ∈ {active, pre_launch}`, netuid ≠ 0, in ascending netuid, with a per-occupant `try/except` (one failure never aborts the run). All TMC calls go through spec 05's `TmcClient` (`app/collectors/chain_tmc.py`, the only TMC caller — 02 §G2; methods `get(path, params)` / `get_paginated(path, params, page_limit, max_pages)` without the retired `priority` argument) on top of 01's `Http.request('tmc', …)` / `ProviderLimiter` (8/min, 500/h, 90 k/month, no priority classes — 02 §G1/§F2). This job additionally paces itself to ≤ `OWNER_TMC_MAX_PER_MIN` (5) and stops at `OWNER_TMC_MAX_PER_RUN` (518) requests, which keeps it inside its row of 05's budget table (02 §G3, §6). Field names marked † are verified on the TMC subnet-page embed but **UNVERIFIED on the REST shapes**; the `owner_personal_{locked,perpetual,decaying}_alpha` names are verified on the SSE table (research 02-keyless §2) and `pending_owner_cut`, `network_registered_at`, `first_emission_block_number` on the `/subnets/` `latest_snapshot`, but their placement inside the `/subnets/{id}/` response is also UNVERIFIED (research 02-continuity only states "owner keys, hyperparams" are present for SN64). `scripts/owner_probe.py` (§10 step 1) prints actual keys and freezes a mapping in `data/owner_field_map.json`, which the mappers read (fallback key lists are built in).

| step | request | when | read → column |
|---|---|---|---|
| **C1** owner keys + cut | none if the newest `chain_rest_snapshots` row of kind `subnets` (written every 6 h by 04's `registry_occupants`, 02 §G2; 03 key `endpoint='/subnets/'`) is ≤ 12 h old and carries both owner keys for `n` (`OWNER_KEYS_SOURCE=auto`); else `GET /subnets/{n}/` | every occupant | `latest_snapshot` (then row): `subnet_owner`† → `owner_coldkey`; `subnet_owner_hotkey`† → `owner_hotkey`; `owner_cut_per_block`†, `owner_cut_cumulative`†, `pending_owner_cut`; `owner_personal_stake`†, `owner_personal_locked_alpha`, `owner_personal_perpetual_alpha`, `owner_personal_decaying_alpha`; hyperparams `owner_cut_enabled`†, `owner_cut_auto_lock_enabled`† (names from `get_subnet_hyperparams_v3`); `network_registered_at` (must equal `O`'s, else skip `O` — the registry will re-occupy); `block_number` |
| **C2** position | `GET /accounts/coldkey-stakes/{owner_ck}/` (list or `{results,next}`; follow `next`) | once per **distinct** owner coldkey per run (cached; several subnets share owners) | rows whose subnet id (`netuid`\|`subnet`\|`subnet_id`) = `n`: `owner_total_alpha = Σ balance_alpha`, `owner_locked_alpha = Σ locked_alpha`, `owner_available_alpha = max(0, total − locked)`, hotkeys → `owner_hotkeys_json`; `is_subnet_owner` cross-check |
| **C3** owner-hotkey series | `GET /analytics/neuron/{n}/{owner_hk}/?span=1W` | every occupant with `owner_hk` | hourly `ts`, `block_number`, `alpha_stake`, `conviction`, `conviction_locked_alpha`, `conviction_perpetual_alpha`, `conviction_decaying_alpha` → `owner_position_hourly` (`INSERT OR IGNORE`); latest hour → `owner_snapshots.owner_hk_alpha_stake/owner_hk_conviction`. This is the **hotkey's** stake (owner + nominators) — a trace, not the owner position |
| **C4** owner activity | `GET /extrinsics/staking-activity/?coldkey={owner_ck}&FROM={from}&TO={now}&limit={OWNER_ACTIVITY_PAGE_LIMIT}&offset={k}` (documented filters: `FROM, TO, coldkey, hotkey, from_netuid, to_netuid, function, exclude_function, alpha_gte/lte, block_number, is_evm, is_proxy`; `limit/offset` paging UNVERIFIED) | once per distinct owner coldkey; pages until `< limit` rows, `next` null, or `OWNER_ACTIVITY_MAX_PAGES` | each row → `owner_stake_events(source='tmc_staking_activity')` via §2.4; rows touching no netuid owned by this coldkey are dropped (counted in run stats) |
| **C5** reconcile | `GET /transactions/stakes/stake-removed/?hotkey={owner_hk}&subnet={n}&limit=100&offset={k}` (the OAS filters are `block, event, hotkey, id, subnet, limit, offset` — there is **no coldkey filter**, research 02-keyless §1) | only if C4 failed for this coldkey, or the reconciliation trigger fires; ≤ `OWNER_STAKE_REMOVED_MAX_CALLS_PER_DAY` | rows whose coldkey (`coldkey`\|`nominator`, UNVERIFIED) = `owner_ck` → `owner_stake_events(source='tmc_stake_removed', kind='sell')`; sees only removals from the owner hotkey |
| **C6** Rufus | `GET https://s3.hippius.com/rufus/public/conviction-table/data/baked-at.json`, then `…/data/table.json` only if `baked_at_utc` changed | daily, provider `rufus` | see below |
| **C7** subnetradar | `GET https://subnetradar.com/swaps` | daily, provider `subnetradar` | see below |
| **C8** TaoStats | see below | only if `settings.taostats_active()` and `OWNER_TAOSTATS_ENABLED` | |

**C4 watermarks.** `job_state(job='owner_collect_daily', key=f"activity_wm:{owner_ck}")` = `{ts, block, coverage_start}`. `from = wm.ts − OWNER_ACTIVITY_OVERLAP_SEC`; on first sight of a coldkey `from = now − OWNER_ACTIVITY_BACKFILL_DAYS·86400` and `coverage_start = from`. The watermark advances only to the newest row of a complete page sequence; if `OWNER_ACTIVITY_MAX_PAGES` is hit (`truncated`), it advances to the last row received and the next run continues. Rows are requested ascending (sort param name UNVERIFIED; if only descending exists, the job pages backwards from `now` and stores the oldest reached as the resume point). First-run backfill calls are capped at `OWNER_BACKFILL_MAX_CALLS_PER_RUN` and resume on later runs. `FROM`/`TO` encoding (epoch ms vs s vs ISO) is UNVERIFIED — `OWNER_ACTIVITY_TIME_FORMAT=auto` uses the probe's result. Duplicates across overlaps are absorbed by the `event_uid` key.

**Reconciliation trigger** (per occupant, after C2 and C4): `unexplained = prev_total + Σinflow − Σoutflow − curr_total` over `(prev_snapshot, now]` (accrual is ignored: it only makes the position larger). `unexplained > max(OWNER_RECONCILE_MIN_ALPHA, 0.02 · prev_total)` → C5 for that occupant and event `owner_position_unexplained` (info). S1 counts only event rows, never the residual.

**C6 Rufus parser** (tolerant; nesting UNVERIFIED): top level is a list of subnet objects or a dict keyed by netuid string. Per subnet read `netuid`, `auto_lock_enabled`, `days_to_king`, `meta.{owner_coldkey, owner_hotkey, registration_block}`, buckets `owner`, `to_owner`, `owner_side`, `challenger`, each with `locked_perp`, `locked_decay`, `conviction`, `pct` and `rows[]` of `{coldkey, hotkey, perpetual, locked…}`; keys are matched case-insensitively one level deep and the discovered schema is logged once per process. Use for `O` only if `meta.registration_block` is absent or equals `O.network_registered_at`. `baked_at_utc` older than `OWNER_RUFUS_MAX_AGE_HOURS` → stored with `rufus_stale=1`, not used for S2. A `pct` > 1 is read as percent. Raw bodies → `chain_rest_snapshots(endpoint='rufus/table.json'|'rufus/baked-at.json', source='rufus')`.

**C7 subnetradar parser** (structure UNVERIFIED): try JSON (`Accept: application/json`, else a `<script id="__NEXT_DATA__">` blob); else regex rows containing `(?:SN|netuid\s*)(\d{1,3})` and two SS58 strings `5[1-9A-HJ-NP-Za-km-z]{47}`. Output `[{netuid, old_coldkey, new_coldkey, block|date, type}]`. Used only by D1 (classification) and D8 (miss check). A parse failure sets `/health…subnetradar.parse_ok=false` and is never a job failure. Raw → `chain_rest_snapshots(endpoint='subnetradar/swaps', source='subnetradar')`.

**C8 TaoStats upgrades** (provider `taostats` in spec 01's limiter, header `Authorization: <key>`; skipped while `./KILL_SWITCH` exists). *(Amended 2026-10-12: TaoStats retires the old `/api/…/v1` routes on 2026-11-09; every call below uses the new `/v1/` API under 02 §G7. Paths and filters probed live 2026-10-12.)*
- `GET /v1/subnets/stake-events?coldkey={owner_ck}&timestamp_start={wm}&order_by=timestamp&order_dir=asc&limit=200&page={k}` per owner coldkey (the ported `build_delegation_params`, old `events.py:135-158`, with `nominator` → `coldkey`, `delegate` → `hotkey`, `action` values `stake|unstake|all`, `order=timestamp_asc` split into `order_by`/`order_dir`) → parsed with the ported `app/whale/events.py::classify_event` (old lines 83-132), adapted to the new response: `nominator` → `coldkey`, `delegate` → `hotkey`, `delegate_name` → `hotkey_name`, `action` `stake`/`unstake` instead of `DELEGATE`/`UNDELEGATE`, `id` in a new format (old ids do not match) → `owner_stake_events(source='taostats_delegation')` (`unstake` → outflow; `is_transfer` true → `transfer_out`). The move-stake phantom-pair rule (old `whale_impact.py:158-178`: same coldkey, netuid and block holding both BUY and SELL among non-transfer rows) → both legs `kind='move_internal'`; the new API also flags these directly (`validator_swap = true` on each half of a within-subnet validator move), and `is_transfer` is now always `true`/`false`, never `NULL` (the old research 01-whale §1 NULL case no longer occurs). Cross-netuid swaps stay real trades. Pre-dTAO events (before block 4,920,351) live only on `/v1/historic/stake-events`, which has no `netuid`/`alpha` and is not needed here.
- `GET /v1/subnets/owners?netuid={n}` (or unfiltered, paginated) daily → owner history with `is_coldkey_swap` (response unchanged from the old route) → D1 classification and 90-day S3 backfill (`owner_changes.detected_via='taostats'`).
- `GET /v1/extrinsics?signer_address={owner_ck}&pallet=SubtensorModule&name=set_perpetual_lock&include_args=true` weekly per owner coldkey → exact `perpetual_off`/`perpetual_on` from `args` (was `call_args`; omitted unless `include_args=true`); the same weekly sweep with `name=announce_coldkey_swap` → `owner_changes(change_type='coldkey_swap_announced')` (≥ 5-day on-chain warning before `swap_coldkey_announced`, research 02-owner §3 item 2; info event + review item, S3 penalty 0 — the penalty attaches to the executed swap). `signer_address` is SS58 in both the filter and the response (the old API returned hex).
- `GET /v1/events?pallet=SubtensorModule&name=NetworkRegistrationQueued` (filters verified 2026-10-12) for pre-cutoff occupants → `initial_alpha_est` (§2.7); once per occupant. `args` carries `coldkey`, `hotkey`, `lock_amount` (rao, JSON number), `median_subnet_alpha_price` (`{"bits": "…"}`, fixed-point — decode and verify the scale against the subnet's price at `registration_block` before use) and `registration_block`.

**Source ranking** for duplicate stake events (same `owner_ck`, netuid, `block_number`, direction, alpha within 0.1 %): `taostats_delegation > tmc_staking_activity > tmc_stake_removed > tmc_stream`. The lower-ranked row gets `superseded_by`/`superseded_at`; PIT queries treat it as superseded only if `superseded_at ≤ t`.

**Units.** TMC amounts are converted with spec 05's units decision file; the staking-activity/stake-removed/coldkey-stakes/neuron amount units are UNVERIFIED — `OWNER_AMOUNT_UNITS=auto` uses the probe (the SN64 owner position must land in 10⁵–10⁶ α). `chain_hourly`'s `owner_personal_*`, `total_subnet_stake` and `tao_liquidity` columns are rao-scaled (03 column comments: ÷ 1e9 → α / τ); every read in this spec goes through one helper that applies that conversion. All amounts this spec stores are α or τ floats.

**Optional real-time stream** (`OWNER_STREAM_ENABLED=false`): the second SSE slot on `GET /public/v1/extrinsics/staking-activity/stream/` (the OAS lists it among the 11 SSE paths without the `/sse/` prefix; anonymous access is **UNVERIFIED** — only `/sse/subnets/table/` was tested keyless, and research 02-continuity notes stream endpoints may require a key), filtered client-side to the current owner-coldkey set → `owner_stake_events(source='tmc_stream')`, cutting extraction-detection latency from ≤ 24 h to minutes. Allowed only when spec 05 is not using its second slot: config validator refuses `true` when 05's `TMC_SSE_BLOCKS_STREAM=true`, and with `TMC_SSE_BLOCKS_STREAM=auto` the stream task checks spec 05's live SSE connection count before connecting and yields (disconnects) if spec 05 opens `/sse/blocks/`; the process never exceeds `TMC_SSE_MAX_CONNECTIONS` (2; 02 §F2). **v1 does not register this stream:** 02 §E2 lists exactly five 11 jobs (§6), so the key is inert until a later version adds a `LongTask` `owner_stake_stream` to 01's `JOB_TABLE` (02 §E1).

### 2.4 Stake-event classification (`app/integrity/classify.py::classify_stake_event`)

Function names are `SubtensorModule.*` (prefix stripped). `n` = the occupant's netuid.

| function | condition | `kind` | `counts_as_outflow` |
|---|---|---|---|
| `remove_stake`, `remove_stake_limit` | origin netuid = n | `sell` | 1 |
| `remove_stake_full_limit`, `unstake_all`, `unstake_all_alpha` | origin netuid = n (or unspecified) | `sell_full` | 1 (hard trigger H1) |
| `swap_stake`, `swap_stake_limit`, `move_stake` | origin n, destination netuid ≠ n | `swap_out` | 1 |
| `move_stake` | origin = destination = n | `move_internal` | 0 |
| `transfer_stake` | destination coldkey ≠ owner_ck | `transfer_out` | 1 |
| `transfer_stake` | destination coldkey = owner_ck, netuids differ | `swap_out` | 1 |
| `add_stake`, `add_stake_limit` | destination n | `buy` | 0 |
| any swap/move/transfer | destination n from another netuid / coldkey | `swap_in` / `transfer_in` | 0 |
| `lock_stake` / `move_lock` | | `lock` / `lock_move` | 0 |
| `set_perpetual_lock` | `enabled=false` / `true` | `perpetual_off` / `perpetual_on` | 0 (→ D3) |
| anything else | | `other` | 0 |

Rows with `success = 0` are stored and never counted. A proxy signer (`signer ≠ owner_ck`) still counts (the coldkey's position moved), `is_proxy=1`. Outflows are **gross** — buys are recorded but not netted (master: "Σ owner alpha out of position").

### 2.5 S1 `owner_retention_30d` (weight .20)

```
first_emission_ts(O) = reg_ts(O) + (occupants.first_emission_block − network_registered_at) × BLOCK_SECONDS (12)   # NULL first_emission_block ⇒ pre_launch
W        = [max(t − 30 d, first_emission_ts(O)), t);   days_W = max(1, len(W)/86400)
E        = owner_stake_events of O with event_at ∈ W, observed_at ≤ t, success = 1, not superseded as of t, not whitelisted as of t (§2.9)
out      = Σ alpha(e), e ∈ E, counts_as_outflow = 1
tao_rcv(a,b) = Σ tao(e), e ∈ E, kind ∈ {sell, sell_full, swap_out}, event_at ∈ [a,b)
earned   = Σ_{hours h ∈ W} cut_per_block(h) × 300            if hourly owner_cut_per_block (chain_hourly.extras_json or owner_snapshots) covers ≥ 80 % of W's hours
         = mean(owner_snapshots.owner_cut_per_block in W) × 7200 × days_W   elif ≥ 1 snapshot in W   (master: ≈ owner_cut_per_block × 7200 × 30)
         = None                                              otherwise
           (hours/snapshots with owner_cut_enabled = 0 contribute a known 0: no cut accrues while the cut is off)
impact7(d) = tao_rcv(d − 7 d, d) / mean(chain_hourly.tao_liquidity over [d − 7 d, d))    for each daily boundary d in W (windows clipped at W.start)
sell_ratio = out / max(earned, OWNER_MIN_EARNED_ALPHA)          # None iff earned is None; the floor makes "sales while ~nothing
                                                                  # was earned" a principal sale without a separate branch
H1 = ∃ e ∈ E: kind = 'sell_full'                                 # token full_exit_extrinsic
H2 = max_impact7 > SELL_IMPACT_MAX (0.02), max_impact7 = max_d impact7(d)   # token sell_impact_7d; §5 constants (frozen, = spec 12's reference)
H3 = sell_ratio > 1                                              # token sell_ratio
# order fixed by 02 §C1: every hard trigger first, the owner_cut_enabled shortcut only afterwards
pre_launch (no first_emission_block)                                          → None ('pre_launch')
H1 ∨ H2 ∨ H3 found in the covered part of W (H3 there: earned known ∧ covered out > max(earned, OWNER_MIN_EARNED_ALPHA))
                                                                              → 0.0 (a dump is a dump even while coverage is still incomplete)
activity coverage for owner_ck does not reach W.start, or last good C4/C8 for owner_ck older than OWNER_SNAPSHOT_MAX_AGE_HOURS → None ('activity_coverage')
earned is None                                                                → None ('earned_unknown')
owner_cut_enabled = False (latest snapshot ≤ t and ≤ OWNER_SNAPSHOT_MAX_AGE_HOURS old) → 1.0 ('owner_cut_disabled')
S1 = 1 − clip(sell_ratio, 0, 1)
```
**Row written (02 §C1; spec 12 row 30, `prenorm`):** `value = S1` (final). `value_json` = spec 12's input keys `{sell_ratio, max_impact7, hard: [...], earned_alpha: earned}` with `hard ⊆ {full_exit_extrinsic, sell_impact_7d, sell_ratio}` (H1/H2/H3; the same tokens as `owner_extraction_detected.payload.reasons[]`), plus evidence `{owner_cut_enabled, out_alpha, earned_method, tao_rcv_30d, n_events, n_whitelisted_excluded, trigger_ids[], sources[], coverage_start}`. `status`: `not_applicable`/`pre_launch`; `failed`/`activity_coverage` or `failed`/`earned_unknown` with `value = NULL`; any hard trigger writes `value = 0.0` with `status='ok'` (03's CHECK forbids `ok` with NULL) — even with incomplete coverage and even with the cut disabled. The evidence-only key `owner_sell_impact_7d` = `max_impact7` is written alongside (02 §C2). With the cut disabled for the whole window `earned = 0`, so any outflow above `OWNER_MIN_EARNED_ALPHA` is H3 (§11 Q5).

### 2.6 S2 `owner_commitment` (weight .20)

```
perp   = Rufus owner.locked_perp (fresh, occupant match) → else chain_hourly.owner_personal_perpetual_alpha (latest ≤ t, ≤ 2 h old) → else owner_snapshots.tmc_owner_perpetual (≤ 36 h)
total  = owner_snapshots.owner_total_alpha (C2) → else tmc_owner_personal_stake (C1)
auto   = Rufus auto_lock_enabled (fresh) → else owner_snapshots.auto_lock_enabled_tmc → else None
commit_pct = Rufus owner.pct (share of eligible alpha = AlphaOut − ProtocolAlpha − Burned) → else perp / chain_hourly.total_subnet_stake (proxy; value_json.eligible_proxy = true)
lock_ratio = clip(perp / total, 0, 1) if total > 0 else (0.0 if perp = 0 else 1.0)
if owner_cut_enabled = False: auto := 1            (nothing to auto-lock; the owner forgoes the cut)
perp or total unavailable                                                        → None
perp = 0 ∧ auto ∈ {False, None} ∧ age_days ≤ COMMITMENT_ZERO_AGE_DAYS (90)       → None ('grace_period')
perp = 0 ∧ auto = False ∧ age_days > 90                                          → 0.0 (master zero rule)
S2 = Σ_known w·s / Σ_known w  over the parts {0.60·lock_ratio (always); 0.25·auto (if auto is not None);
                                             0.15·[commit_pct ≥ COMMIT_PCT_MIN (0.05)] (if commit_pct is not None)}   # = spec 12's reference
```
Only the owner coldkey's own perpetual lock counts; `to_owner`/`owner_side` locks from other coldkeys (the SN24 pattern: 150k "owner_side" from a non-owner coldkey) are shown in `value_json` but never credited. `age_days = tenure_days(head_block, network_registered_at)` (§2.11).

**Row written (02 §C1; spec 12 row 29, `prenorm`):** `value = S2` (final); `value_json` = spec 12's input keys `{perp, total, lock_ratio, auto (after the owner-cut-disabled substitution), commit_pct, age_days}` plus evidence `{perp_source, auto_source, eligible_proxy, rufus_to_owner_perp, rufus_to_owner_lockers}`; `status='ok'`; `grace_period` → `value = NULL`, `status='not_applicable'`, `reason='grace_period'` (the bare master formula would score a young zero-lock subnet 0.0; §11 Q4); `perp`/`total` unavailable → `status='failed'`. `perp` is written as an exact `0.0` when no perpetual lock exists (the zero rule compares exactly). The evidence-only key `owner_lock_ratio` = `lock_ratio` is written alongside (02 §C2). Outside `grace_period` and the `total = 0` rule, `value` equals spec 12's reference `owner_commitment(lock_ratio, auto, commit_pct, perp, age_days)` (T43).

### 2.7 S5 `owner_overhang` (weight .05) and the pre-2026-05-08 adjustment

```
avail       = owner_snapshots.owner_available_alpha (C2) → else max(0, tmc_owner_personal_stake − tmc_owner_locked)
price, tao_in = chain_hourly.price (τ/α), chain_hourly.tao_liquidity (τ), latest ≤ t, ≤ 2 h old
dump_impact = avail × price / tao_in            (tao_in ≤ 0, price None or avail None → None)
base        = 1 − zclip(dump_impact, OVERHANG_FULL (0.05), OVERHANG_ZERO (0.25))    # < 5 % → 1, ≥ 25 % → 0, linear between
hidden(O)   = DTAO_LAUNCH_UTC ≤ reg_ts(O) < FREE_ALPHA_CUTOFF_UTC (2026-05-08T00:00Z)
              ∧ no whitelist (kind='initial_alpha_accounted', applies_to='initial_alpha') effective at t for O
              ∧ ¬(initial_alpha_est known ∧ initial_alpha_est × price < 0.01 × tao_in)
S5          = clip(base − FREE_ALPHA_PENALTY (0.2)·[hidden(O)], 0, 1)
initial_alpha_est (TaoStats only) = (lock_amount − 1,000 τ floor) / median_subnet_alpha_price   from NetworkRegistrationQueued
```
**Row written (02 §C1; spec 12 row 32, `prenorm`):** `value = S5` (final, including the −0.2); `value_json` = spec 12's input keys `{dump_impact, hidden_alpha: hidden(O)}` plus evidence `{avail, price, tao_in, initial_alpha_est}`; `status='ok'`; the `None` cases above → `value = NULL`, `status='failed'`, `reason ∈ {no_position, no_pool, not_started}`. The evidence-only key `owner_dump_impact` = `dump_impact` is written alongside (02 §C2). `value` equals spec 12's reference `owner_overhang(dump_impact, hidden_alpha)` (T43).

### 2.8 S4 owner self-dealing — event only in ts-1.0

- **v1 keyless piece (always on):** `owner_holding_share = owner_total_alpha / total_subnet_stake`; `> OWNER_S4_HOLDING_SHARE_MAX (0.25)` with `lock_ratio < OWNER_S4_LOCK_RATIO_MIN (0.5)` → event `owner_concentration` (info, digest; 03 dedupe `occ|conc|<iso_week>`); evidence-only key `owner_holding_share` (weight 0, 02 §C2).
- **ts-1.1 detector design** (`OWNER_S4_ENABLED=false`; needs spec 05's RPC metagraph pass, or TMC `/subnets/neurons/{n}/` + `/subnets/weights/{n}/` (+256 req/week), or TaoStats): `owner_cluster = {owner_ck} ∪ coldkeys of OwnedHotkeys(owner_ck)`; `owner_vali_uids` = permitted validators whose hotkey ∈ `OwnedHotkeys(owner_ck) ∪ {owner_hk}`; `owner_vali_stake_share = Σ stake(owner validators) / Σ stake(permitted validators)`; `self_weight = Σ w(owner_vali → uids whose coldkey ∈ owner_cluster) / Σ w(owner_vali)`. Thresholds `owner_vali_stake_share > 0.30`, `self_weight > 0.20`, `owner_holding_share > 0.25 ∧ lock_ratio < 0.5` → event `owner_self_dealing_suspected` (warning, components in payload; not yet in 03 §3.9 — 03 must add it before `OWNER_S4_ENABLED` may be set, because `emit_event` rejects unknown types). ts-1.1 turns this into metric `owner_self_dealing` (research §5 penalty points 4/4/2 of 10); its weight is a new-`spec_version` decision.

### 2.9 Detectors (C), whitelist, S3 and triggers

| id | runs in | condition | `owner_changes.change_type` (S3 base) | event (severity) | review kind |
|---|---|---|---|---|---|
| D1 | collect, watch, rpc-seed | `owner_ck` differs from the previous observation of the same occupant (previous `owner_snapshots` row; hourly: 03's named column `chain_hourly.subnet_owner`; one-shot seed: spec 17 `rpc_owner_history` rows whose `change` lists `owner_coldkey`) | classify, first match wins: TaoStats `is_coldkey_swap` → `coldkey_swap`; the new coldkey was a Rufus `challenger`-bucket locker at the previous snapshot (or `days_to_king ≤ 1`) → `subnet_owner_changed` (conviction takeover = `SubnetOwnerChanged`; no mainnet occurrence known, research 02-owner §1); subnetradar row type; heuristic (1 extra C2 on the old coldkey: still holds ≥ 50 % of its last observed position in n → `owner_transfer`; emptied → `coldkey_swap`; C2 failed → `coldkey_change_unclassified`) — all four at base 0.6 | `owner_control_change` (warning, digest — 02 §D4) | `owner_change_whitelist` (`event_type='owner_control_change'`, priority 1) |
| D2 | collect, rpc-seed | different `owner_hk` for the occupant (also when D1 fires in the same observation) | `hotkey_repoint` (0); if a spec-04 `identity_changed` with `payload.source='chain'`, `field ∈ {subnet_name, github_repo, subnet_url, discord}` has `abs(Δevent_at) ≤ REPOINT_CHURN_WINDOW_DAYS (14)` → extra row `hotkey_repoint_identity_churn` (0.4; `event_at` = the later of the two; `paired_change_id`, `identity_event_id`); re-evaluated daily for 14 d | `owner_hotkey_repointed` (info, digest) / `owner_identity_churn` (warning) | `owner_change_whitelist` (`event_type='owner_identity_churn'`) for the pair only (a plain re-point has penalty 0 and no review item) |
| D3 | watch (hourly), collect | consecutive `chain_hourly` rows h−1, h on the **same unit basis** (`perp` = `owner_personal_perpetual_alpha`; on `analytics_1w` gap-repair rows, which carry only locked + decaying, `perp = owner_personal_locked_alpha − owner_personal_decaying_alpha`; a pair spanning two sources is skipped unless spec 05's units probe marks them identical): `Δp = perp_h − perp_{h−1} ≤ −max(OWNER_PERP_OFF_MIN_ALPHA, 0.01·perp_{h−1})` and `Δd = decay_h − decay_{h−1} ≥ (1 − OWNER_PERP_OFF_MATCH_TOL)·abs(Δp)`; **or** Rufus owner row `perpetual` true→false; **or** a `perpetual_off` stake event signed by owner_ck (C4/C8). Dedupe 1/occupant/24 h | `perpetual_off` (hard: S3 = 0) | `owner_perpetual_off` (critical, digest — 02 §D4) | `owner_extraction_candidate` (`suggested_flag='owner_extraction'`, priority 1) |
| D3b | watch | `Δd > 0` with `abs(Δp) < OWNER_PERP_OFF_MIN_ALPHA` | `decaying_lock_added` (0) | `owner_decaying_lock_added` (info) | — |
| D3c | watch | `Δp` beyond the threshold with `Δd` below the match (there is no unlock extrinsic; a coldkey swap or `move_lock` to a non-owner hotkey are the known causes — UNVERIFIED which TMC aggregates follow `move_lock`) | `lock_reduced` (0) | `owner_lock_reduced` (warning, digest) | `owner_change_whitelist` (`event_type='owner_lock_reduced'`) |
| D4 | features | S1 hard trigger H1/H2/H3 true in W (non-whitelisted); at most one `extraction_detected` change per occupant per 7 d (a D4b row counts) | `extraction_detected` (0) | `owner_extraction_detected` (critical, digest — 02 §D1/§D4; `reasons[]` = the §2.5 `hard` tokens) | `owner_extraction_candidate` (`suggested_flag='owner_extraction'`, priority 1) |
| D4b | collect (at ingest) | a newly inserted, non-whitelisted stake event with `kind='sell_full'` (H1: `unstake_all*`/`remove_stake_full_limit`) | `extraction_detected` (0; `detected_via='staking_activity'`) | `owner_extraction_detected` with `reasons=['full_exit_extrinsic']` (critical, digest; 02 §D1 folds the old full-exit event into it; the ISO-week dedupe makes a later D4 event of the same week a no-op) | `owner_extraction_candidate` (`suggested_flag='owner_extraction'`, priority 1) |
| D5 | operator | key-compromise override (§2.10) | `key_compromise_override` (0) | `owner_key_compromise_override` (warning) | resolves open owner items |
| D6 | collect | Rufus/TMC `auto_lock_enabled` or `owner_cut_enabled` toggled | `auto_lock_disabled`/`auto_lock_enabled`/`owner_cut_disabled`/`owner_cut_enabled` (0) | `owner_auto_lock_changed` / `owner_cut_toggled` (info, digest) | — |
| D7 | collect | fresh Rufus `meta.owner_coldkey` or spec-04 `subnets.owner_coldkey` ≠ C1 owner_ck for > 24 h | — | `owner_source_disagreement` (info) | `owner_source_disagreement` |
| D8 | collect | subnetradar lists an owner change for a current occupant (after its registration, within 7 d) not observed by D1 within 48 h | — | `owner_source_disagreement` (info) | same |
| D9 | collect (weekly C8 sweep; TaoStats only) | `SubtensorModule.announce_coldkey_swap` signed by owner_ck | `coldkey_swap_announced` (0) | `owner_coldkey_swap_announced` (warning, digest; 02 §D1 — 03 must add the type) | `owner_change_whitelist` (`event_type='owner_coldkey_swap_announced'`, priority 2; the operator can pre-whitelist the coming swap) |

A decaying owner lock that already exists at first observation produces **no** event (research counts 16 decaying-only owners; a decaying lock is weaker commitment scored by S2, not an exit — §11).

**Whitelist matching** (`app/integrity/whitelist.py::is_whitelisted(item, t) -> int | None`): an `owner_changes` row or stake event of occupant `O` with `event_at = x` is whitelisted as of `t` iff an `owner_event_whitelist` row exists with the same `occupant_key`, `created_at ≤ t`, `retired_at IS NULL OR retired_at > t`, `window_start ≤ x ≤ window_end`, `applies_to` ∈ {the item's class, `'all'`} (classes: `coldkey_change` ⊇ {coldkey_swap, owner_transfer, coldkey_change_unclassified, subnet_owner_changed, coldkey_swap_announced}; `hotkey_repoint` ⊇ {hotkey_repoint, hotkey_repoint_identity_churn}; `identity_churn` ⊇ {hotkey_repoint_identity_churn}; `stake_events` ⊇ all stake events; `perpetual_off` ⊇ {perpetual_off, lock_reduced}; `extraction` ⊇ {extraction_detected}; `initial_alpha`), and — when set — `old_coldkey`/`new_coldkey` equal the change's values. `perpetual_off` may be whitelisted only with `reason ∈ {post_compromise_recovery, false_positive}` (03 CHECK on its column `reason`; the linked change is stored in `owner_change_id`). The `whitelisted` columns are a display cache refreshed on insert; metric computation always calls `is_whitelisted(·, t)`.

**S3 `control_stability_90d` (weight .15)**
```
C  = owner_changes of O with event_at ∈ [t − 90 d, t), observed_at ≤ t, not whitelisted as of t
∃ c ∈ C with change_type = 'perpetual_off'                 → 0.0 ('formal_exit')
S3 = clip(1 − Σ_{c ∈ C} s3_base_penalty(c) × 0.5^(age_days(c) / S3_HALF_LIFE_DAYS (45)), 0, 1)
None iff no owner_snapshots row for O within OWNER_SNAPSHOT_MAX_AGE_HOURS; value_json.observed_since = first snapshot
(changes before observed_since are invisible unless TaoStats or spec 17's rpc_owner_history back-filled them)
```
**Row written (02 §C1; spec 12 row 31, `prenorm`):** `value = S3` (final); `value_json` = spec 12's input key `{observed_since}` plus evidence `events = [{kind, event_at, whitelisted}]` over every penalising change of the occupant in the last 90 d — `kind` in spec 12's reference vocabulary (`perpetual_off` → `perpetual_lock_disabled`; `coldkey_swap | owner_transfer | coldkey_change_unclassified | subnet_owner_changed` → `coldkey_swap_unannounced`; `hotkey_repoint_identity_churn` → `hotkey_repoint_identity_change`), `whitelisted = is_whitelisted(·, t) is not None` — so that `value == control_stability(events, t)` (T43), and `change_ids[]`; `status='ok'`. Unobservable → `value = NULL`, `status='failed'`, `reason='no_owner_snapshot'`. "Unannounced" = not operator-whitelisted (§11 Q8).

**Trigger rows** (aux, weight 0, `stage='raw'`; names, windows and semantics fixed by 02 §C2; spec 12 raises/clears the flags under 02 §D5):
- **`owner_extraction_trigger`** = 1.0 iff a non-whitelisted S1 hard trigger (H1 full-exit extrinsic, H2 7-d sell impact > 2 % of pool TAO, H3 `sell_ratio > 1`; §2.5, found in the covered part of the window) occurred in `[t − 30 d, t)`, **or** a non-whitelisted `perpetual_off` — a perpetual→decaying transition of the owner lock or `set_perpetual_lock(false)` signed by the owner coldkey (D3) — has `event_at ∈ [t − 90 d, t)`; else 0.0. `value_json = {reasons[] ⊆ {full_exit_extrinsic, sell_impact_7d, sell_ratio, perpetual_off}, trigger_ids[], last_event_at, detected_via[]}`. `value = NULL`, `status='failed'` iff the owner data is unobservable (no owner snapshot within `OWNER_SNAPSHOT_MAX_AGE_HOURS` **and** no `chain_hourly` owner-lock row within 2 h); missing activity coverage alone never yields 1.0. A human-confirmed `owner_extraction` lives in `manual_flags` and is read by spec 12 directly.
- **`owner_control_change_trigger`** = 1.0 iff a non-whitelisted `coldkey_swap | owner_transfer | coldkey_change_unclassified | subnet_owner_changed` has `event_at ∈ [t − 30 d, t)`; else 0.0. `value_json = {kind, event_at, change_id, trigger_ids[], identity_churn}` where `identity_churn = true` iff that change is paired (either order, ≤ `REPOINT_CHURN_WINDOW_DAYS` = 14 d apart) with an owner-hotkey re-point (`hotkey_repoint` or `hotkey_repoint_identity_churn` row) or with a chain-source `identity_changed` event for `field ∈ {subnet_name, github_repo, subnet_url, discord}` — never 04's `owner_*` identity fields, which change with every swap. `identity_churn = true` makes spec 12's flag **sticky** until a manual clear; otherwise it clears after the 30-d window plus the 7-d hysteresis (02 §D5). `status='failed'` iff no owner snapshot within `OWNER_SNAPSHOT_MAX_AGE_HOURS`.
- `trigger_ids` = the sorted stable ids of the evidence behind a 1.0 (`chg:<owner_changes.id>`, `ose:<event_uid>`), so spec 12 can tell new evidence after a manual clear: new ids re-raise immediately, the 30-d suppression covers only the cleared ones (02 §D5).
- **Evidence-only keys** (weight 0; `status='ok'` whenever the input exists, else `failed`; exactly 02 §C2's list): `owner_lock_ratio` (§2.6), `owner_dump_impact` (§2.7), `owner_holding_share` (§2.8), `owner_sell_impact_7d` (§2.5 `max_impact7`), `team_verified_count`, `team_candidate_count` (§2.2.6). Retired names are never written (`owner_lock_disabled_90d`, `owner_control_change_30d`, `owner_max_sell_impact_7d`, `identity_stability`, `owner_conviction`, `owner_retention`); `sell_ratio` stays inside `owner_retention_30d.value_json` (02 §C2 has no `owner_sell_ratio_30d`).

### 2.10 Operator actions: review queue, whitelist, key-compromise override

`review.resolve_review(db, review_id, action, *, by, note, evidence_url=None, whitelist_reason=None, window=None) -> ActionResult`. Review kinds are exactly 03's CHECK list: `team_candidate`, `team_reverify`, `team_delisted` (team); `owner_extraction_candidate` (D3/D4/D4b), `owner_change_whitelist` (every owner change awaiting a whitelist-or-accept decision — D1, the D2 churn pair, D3c, D9 — with the detector event in 03's `event_type` column: `owner_control_change`, `owner_identity_churn`, `owner_lock_reduced`, `owner_coldkey_swap_announced`), `owner_source_disagreement` (D7/D8); 10's `llm_flag_candidate`; `other` is unused by 11. Statuses are 03's `open | confirmed | rejected | resolved | expired`. Row columns are 03's: `ref_table ∈ {owner_changes, team_members, events}` + `ref_id`; `dedupe_base = f"{kind}|{ref}"` (`ref` = `occupant_key` for `team_candidate`, member id for `team_reverify`/`team_delisted`, change id for `owner_change_whitelist`/`owner_extraction_candidate`, `f"{occupant_key}:{field}"` for `owner_source_disagreement`); `enqueue()` merges `payload_json` into the **open** row with the same `dedupe_base`, else inserts with `dedupe_key = f"{dedupe_base}|{created_at}"` (03's open-only unique index).

**`enqueue` for spec 10.** `review.enqueue(db, kind='llm_flag_candidate', occupant_key=…, post_id=…, label_version=…, event_type=…, materiality=…, summary=…, evidence_url=…, suggested_flag ∈ {owner_extraction, key_compromise, security_incident, None}, priority=…, payload={reason, direction, confidence}, ref_table='events', ref_id=…)` takes 10's post/label columns exactly as 10 §3.2 writes them (`post_id` = 10's item id, e.g. `discord:{source_message_id}` = `announcements.msg_id`; there is no `msg_id` column on `review_queue`). It writes `dedupe_key = f"llm_flag_candidate|{post_id}|{label_version}"`, `dedupe_base` NULL, `INSERT OR IGNORE` against 03's `ux_review_queue_post(kind, post_id, label_version)` (one review per label, forever; returns `None` when ignored) and never merges. 10's `enqueue_review` delegates to it, so 10 inserts no rows of its own (closes 10 §11 item 12). `post_id` with any other kind, or `llm_flag_candidate` without `post_id`/`label_version`, or a kind outside 03's list, raises `ValueError("invalid_kind")` (a programming error, not user input).

| action | allowed kinds | effect |
|---|---|---|
| `confirm_flag` | `owner_extraction_candidate`; `llm_flag_candidate` with `suggested_flag ∈ {owner_extraction, key_compromise}` | `manual_flags(flag, state='confirmed', evidence_url, set_by, set_at)` with `flag = 'owner_extraction'` for an owner item, else the row's `suggested_flag` — `confirm` exists only for `owner_extraction` and `key_compromise` (02 §D5); for 10's rows also calls `app.nlp.events.confirm_review`; `status='confirmed'` |
| `whitelist` | `owner_change_whitelist`, `owner_extraction_candidate` | `owner_admin.add_whitelist(reason=whitelist_reason, applies_to=<class of the item>, window=window or [event_at − 86400, event_at + 86400], change_id=ref_id` (stored as `owner_change_id`)`, evidence_url)`; `status='confirmed'` |
| `key_compromise` | `owner_extraction_candidate`, `owner_change_whitelist`, `llm_flag_candidate` with `suggested_flag ∈ {owner_extraction, key_compromise}` | `owner_admin.override_key_compromise(netuid, window_start, window_end, evidence_url, by)`; `status='confirmed'` |
| `done` (acknowledge) | `team_*`, `owner_source_disagreement`; `llm_flag_candidate` with `suggested_flag='security_incident'` | `status='resolved'` (operator acted). For a `security_incident` row this is the human **acknowledge**: it calls 10's `confirm_review` (the incident then counts in `event_impact_14d`) and writes **no** `manual_flags` row — `security_incident` is not a confirmable flag (02 §D5; 03's `manual_flags` CHECK) |
| `dismiss` | any; the **only** action for an `llm_flag_candidate` whose `suggested_flag` is NULL | `status='rejected'`; no data effect (auto triggers keep firing until whitelisted; 10's label stays uncounted) |

Any other (action, kind, `suggested_flag`) combination returns `invalid_transition` and writes nothing: a `security_incident` candidate goes to human review only (acknowledge or dismiss), a NULL-candidate item can only be dismissed, and only `owner_extraction`/`key_compromise` candidates can be confirmed.

Every resolution sets `resolved_by`, `resolved_at` (INTEGER), `resolution_note`, `resolution_action`; `evidence_url` is mandatory for `confirm_flag`, `whitelist` and `key_compromise`. Items auto-resolve with `status='expired'`, `resolution_action='auto_cleared'`: `team_candidate` when the occupant has no `candidate` rows; `team_reverify` when the member's `last_checked_at` is fresh; `team_delisted` when the member is rejected or re-listed; `owner_source_disagreement` when the sources agree again for 48 h.

**Confirmed LLM evidence without a queue action.** If the operator confirms a spec-10 item through spec 10's own `confirm_review` path (not through `resolve_review`), the official `integrity_features` run finds confirmed `owner_extraction_evidence` events (`payload.confirmed_by` set, `observed_at ≤ t`) that have no `manual_flags(owner_extraction, confirmed)` row set at or after the event, and writes one (`set_by='sync:llm_confirmed'`, `evidence_url` = the event's `payload.url`) so master §7.5's "human-confirmed `owner_extraction_evidence` event" always reaches spec 12 through `manual_flags`.

**`override_key_compromise(netuid, window_start, window_end, evidence_url, by, note)`** — master §7.5's post-mortem override (ORO/Connito pattern: the on-chain dump is indistinguishable from an owner exit):
1. `owner_event_whitelist(reason='post_compromise_recovery', applies_to='all', window=[window_start, window_end])` — the attacker's sells, any lock toggle and the recovery coldkey swap to a multisig inside the window stop counting for S1, S3 and both trigger rows for decisions after `created_at` (earlier decisions are unchanged: PIT).
2. `manual_flags(occupant_key, flag='key_compromise', state='confirmed', evidence_url, set_by=by, set_at=now)` → spec 12 applies `key_compromise ×0.50` until the operator clears it. In the same transaction a `manual_flags(flag, state='cleared', evidence_url, note='superseded by key_compromise override')` row is appended for each of `owner_extraction` and `owner_control_change` that is active in 12's `red_flag_state` (`active = 1`, current `SPEC_VERSION`); an inactive flag gets no row and the result carries `warning='flag_not_active:<flag>'` (02 §D5: a clear applies only to an active flag). Spec 12 clears it at the next run and suppresses re-raise for 30 d unless new trigger ids appear (02 §D5); the whitelist of step 1 already makes both triggers read 0 for the window — master §7.5 "allow a post-mortem override to `key_compromise`".
3. `owner_changes(change_type='key_compromise_override', detected_via='operator')`, event `owner_key_compromise_override`, and every open owner review item of the occupant whose `event_at` lies in the window is resolved with `resolution_action='key_compromise'`.
Constraint: `window_end − window_start ≤ OWNER_OVERRIDE_MAX_WINDOW_DAYS (45)` days (`window_too_long` otherwise).

### 2.11 `owner_identity` (weight .05) and `tenure` (weight .10) (D)

- **`owner_identity`**: via spec 04 `registry.resolved(db, O, field, as_of=t)` for `owner_name`, `owner_url`, `owner_github` (TMC `/accounts/coldkey-identities/`, fetched once a day by 04's `registry_identity` into `chain_rest_snapshots` kind `coldkey_identities` and merged by 04 for `subnets.owner_coldkey`; this spec never refetches it — 02 §G2). `owner_name.status = 'unknown'` → `None`; `owner_name` present ∧ (`owner_url` or `owner_github` present) → 1.0; otherwise (`absent`/`junk_only`, or a name without url/github) → 0.0. If today's C1 `owner_coldkey` ≠ `subnets.owner_coldkey` (D7 open) → `None` (`owner_key_mismatch`): the identity would describe the wrong key. Row (spec 12 row 28, `binary`, freshness 2 d): 1.0 → `status='ok'`; 0.0 → `value = 0.0`, `status='absent'`, `reason ∈ {no_identity_row, no_url_or_github, junk_only}`; `None` → `status='failed'`, `reason ∈ {owner_unknown, identities_never_ok, owner_key_mismatch}`.
- **`tenure`** (02 §C1; spec 12 row 33): written by this spec as **raw days**, `value = tenure_days(head_block, network_registered_at) = (head_block − network_registered_at) × 12 / 86400`, with `head_block` = latest `chain_hourly.block_number` for n ≤ 2 h old, else `occupants.last_seen_block`; `status='ok'`, `value_json = {head_block, head_source: 'chain_hourly'|'occupants', network_registered_at}`. Registration block unknown (NULL or ≤ 0) → `value = NULL`, `status='failed'`, `reason='no_registration_block'`; no head block → `failed`/`no_head_block`. Spec 12 normalises it with `max(0.3, zclip(days, 120, 365))` (master: `< 120 d (immune) → 0.3 … ≥ 365 d → 1`; 120 d = `NETWORK_IMMUNITY_PERIOD_BLOCKS` 864,000 × 12 s) and falls back to spec 05's `occupant_age_days` when this row is missing or stale. The same helper gives S2's `age_days`; a verify-time cross-check compares it with `occupant_age_days` (|Δ| ≤ 0.1 d).

### 2.12 Domain API for spec 14 and the CLI

Spec 14 owns transport (routes, auth, Telegram parsing); this spec owns semantics. All functions are `async`, never raise on user error, and return `ActionResult(ok: bool, id: int | None, occupant_key: str | None, status: str | None, error: str | None, warning: str | None, event_id: int | None)`. Error codes: `no_current_occupant`, `not_found`, `evidence_required`, `invalid_url`, `two_source_rule`, `duplicate_member`, `invalid_transition`, `window_too_long`, `invalid_kind`.

| spec-14 surface | domain call |
|---|---|
| `POST /api/v1/manual/team` `{netuid, name, role, profile_url, evidence_url, verified, note}` (existing) | `team.add_member(db, netuid=…, name, role, profile_url, evidence_url, evidence_note=note, source='manual', created_by='api', verify=verified)` |
| *new* `POST /api/v1/manual/team/{id}/verify` `{evidence_urls[], note}` · `POST …/{id}/reject` `{reason, evidence_url?}` | `team.verify_member` / `team.reject_member` |
| *new* `POST /api/v1/manual/team/disclosure` `{netuid, status: anonymous\|clear, evidence_url, note}` | `team.set_disclosure` |
| `POST /api/v1/manual/flag` (existing) | spec 14 writes `manual_flags` under 02 §D5: `confirm` only for `owner_extraction`/`key_compromise`; `clear` for those two and `owner_control_change` only while the flag is active, else HTTP 409 `flag_not_active`; for `key_compromise` the UI should prefer the override route so the window is whitelisted too |
| *new* `POST /api/v1/manual/owner/whitelist` `{netuid, reason, applies_to, window_start, window_end, evidence_url, change_id?, old_coldkey?, new_coldkey?, note}` (stored in 03's columns `reason` and, for `change_id`, `owner_change_id`; window bounds ISO at the API boundary, stored INTEGER) · `DELETE …/whitelist/{id}` (sets `retired_at`, never deletes) | `owner_admin.add_whitelist` / `owner_admin.retire_whitelist` |
| *new* `POST /api/v1/manual/owner/compromise` `{netuid, window_start, window_end, evidence_url, note}` | `owner_admin.override_key_compromise` |
| *new* `GET /api/v1/review?kind=&status=open` · `POST /api/v1/review/{id}/resolve` | `review.list_open` / `review.resolve_review` |
| Telegram `/team add <netuid> "<name>" "<role>" <url>` (existing; url = `profile_url`, row is a candidate) · *new* `/team verify <id> <url> [<url2>]` · `/team reject <id> "<reason>"` · `/team list <netuid>` · `/review [kind]` · `/owner whitelist <review_id> <reason> <url>` · `/owner compromise <netuid> <from_iso> <to_iso> <url>` | same functions, `by='telegram'` |

CLI (`scripts/manual_entry.py`, argparse; opens the DB through spec 03's `Database` at `settings.DB_PATH` unless the global `--db PATH` option is given (used by the verify drill on a copy); prints the JSON `ActionResult`; exit 0 iff `ok`; ISO arguments are parsed with `parse_ts()` and stored as INTEGER):
```
python -m scripts.manual_entry team add <netuid> --name "Jane Doe" --role "CTO" [--profile URL] [--evidence URL] [--note TEXT] [--verify] --by rudolf
python -m scripts.manual_entry team verify <member_id> --evidence URL [--evidence URL2 …] [--note TEXT] --by rudolf
python -m scripts.manual_entry team reject <member_id> --reason "not on team page" [--evidence URL] --by rudolf
python -m scripts.manual_entry team list [<netuid>] [--status candidate|verified|rejected] [--stale]
python -m scripts.manual_entry team anon <netuid> --evidence URL [--note TEXT] [--clear] --by rudolf
python -m scripts.manual_entry team seed [--legacy] [--dry-run]
python -m scripts.manual_entry owner whitelist add <netuid> --reason multisig_migration --applies-to coldkey_change --from ISO --to ISO --evidence URL [--change-id N] [--old-coldkey SS58] [--new-coldkey SS58] --by rudolf
python -m scripts.manual_entry owner whitelist list [<netuid>]  |  owner whitelist retire <id> --by rudolf
python -m scripts.manual_entry owner compromise <netuid> --from ISO --to ISO --evidence URL [--note TEXT] --by rudolf
python -m scripts.manual_entry review list [--kind K]  |  review resolve <id> --action confirm_flag|whitelist|key_compromise|dismiss|done [--reason R] [--from ISO --to ISO] [--evidence URL] --by rudolf
# every sub-command also accepts the global option --db PATH (default settings.DB_PATH)
```
Running the CLI while the service runs is safe (WAL, short transactions, no external calls).

---

## 3. Data contract

Timestamps: every stored time is `INTEGER` unix seconds UTC (02 §B1); amounts are α or τ `REAL` after unit conversion; JSON columns end in `_json`. 03 owns every DDL this section names (02 §B7): this spec ships no `CREATE TABLE`/`ALTER TABLE`; it states what it reads and writes, and the remaining deltas with 03's current DDL are §11 Q2.

### 3.1 Tables written

The 03 tables this spec writes (03 §3.2 reference tables, §3.5 owner tables) and the columns and semantics it relies on:

| table (03 §) | class | written by | columns and semantics this spec relies on |
|---|---|---|---|
| `team_members` (§3.2) | state; history = `team_member_*` events | seeders (candidates only), domain functions §2.12 | `status ∈ {candidate, verified, rejected}`, `source ∈ {subnetalpha, legacy, manual, llm}` (02 §H3); `name_norm` (§2.2.3 step 5) with `UNIQUE(occupant_key, name_norm)`; `name` ≤ 120 and `role` ≤ 60 chars; `profile_url`, `profile_kind ∈ {linkedin, x, github, website, other}`; `evidence_url` (primary), `evidence_note`, `evidence_json` `[{url, domain, class: personal\|attestation\|excluded, added_by, added_at, note}]` (an operator attestation note is an item with `url`/`domain` null and `class='attestation'`), `evidence_count` (distinct non-excluded registrable domains over `profile_url` ∪ evidence **plus** operator attestation notes, §2.2.2 / 02 §H3); `verified_by`, `verified_at` (first verification), `last_checked_at` (latest verify/re-verify), `rejected_by`, `rejected_at`, `reject_reason`, `first_seen_at`, `source_last_seen_at`, `source_ref`, `created_by ∈ {seed_subnetalpha, seed_legacy, cli:<by>, api, telegram}`; generated `verified`/`created_at` for spec 14. 03 CHECK: `verified` ⇒ `evidence_url`, `evidence_count ≥ 2`, `verified_at`, `verified_by` |
| `owner_snapshots` (§3.5) | append; PK `(occupant_key, observed_at)` | `owner_collect_daily`, one row per occupant per run | `event_at` = the C1 block time if the payload carries one (UNVERIFIED), else `reg_ts(O) + (block_number − network_registered_at)·12`, else `observed_at`; `source='owner_collect'`; `run_id`, `block_number`; `owner_coldkey`, `owner_hotkey`, `keys_source ∈ {tmc_list, tmc_detail, chain_hourly, rufus}`; C1 `owner_cut_per_block`, `owner_cut_cumulative`, `pending_owner_cut`, `owner_cut_enabled`, `auto_lock_enabled_tmc`, `tmc_owner_personal_stake`, `tmc_owner_locked`, `tmc_owner_perpetual`, `tmc_owner_decaying`; C2 `owner_total_alpha`, `owner_locked_alpha`, `owner_available_alpha`, `owner_hotkeys_json`; C3 latest hour `owner_hk_alpha_stake`, `owner_hk_conviction`; the `rufus_*` columns (baked_at, stale, auto_lock, owner perp/decay/pct, to_owner perp/lockers, challenger max pct, days_to_king); `total_subnet_stake`, `tao_in`, `price` (τ-converted copies of the `chain_hourly` row used); `initial_alpha_est`; `identity_name`; `fetch_status_json` `{"c1":"ok\|skip\|fail:<err>", …, "c8": …}`; `raw_json` = the owner subset of C1 + the C2 rows for n + `rufus_challenger_coldkeys[]` (D1 classification; 03 has no column for it) |
| `owner_position_hourly` (§3.5) | append, `INSERT OR IGNORE`; PK `(occupant_key, hotkey, hour_ts)` | C3 | `block_number`, `alpha_stake`, `conviction`, `conviction_locked_alpha`, `conviction_perpetual_alpha`, `conviction_decaying_alpha`, `event_at`, `observed_at`, `run_id`, `source='tmc_neuron_1w'` — a trace of the owner **hotkey** (owner + nominators), not the owner position |
| `owner_stake_events` (§3.5) | append; annotations `superseded_by`/`superseded_at` and the display cache `whitelisted`/`whitelist_id` | C4, C5, C8 | PK `(event_uid, occupant_key)`; `event_uid` = the source row id, else `sha1(extrinsic_id\|function\|from_netuid\|to_netuid\|alpha)`; `owner_coldkey`, `signer`, `hotkey`, `dest_coldkey`, `function`, `kind`/`counts_as_outflow` (§2.4), `from_netuid`, `to_netuid`, `alpha`, `tao`, `price`, `slippage`, `is_proxy`, `success`, `block_number`, `extrinsic_id`, `event_at` (block time), `observed_at`, `source ∈ {tmc_staking_activity, tmc_stake_removed, tmc_stream, taostats_delegation, taostats_extrinsic}` (03's `rpc_events` is 17's), `run_id`, `raw_json`; PIT: superseded only if `superseded_at ≤ t`; metrics call `is_whitelisted(·, t)`, never the cache |
| `owner_changes` (§3.5) | append; annotations `whitelisted`/`whitelist_id`, `review_id`, `event_id`, `paired_change_id`, `identity_event_id` | detectors §2.9, override §2.10, `owner_rpc_seed` | `change_type` (03's list = §2.9's), `old_value`, `new_value`, `detected_via ∈ {tmc_snapshot, chain_hourly, rufus, staking_activity, taostats, subnetradar, rpc_owner_history, features, operator}` (generated alias `source`), `classification_basis ∈ {taostats_is_coldkey_swap, rufus_challenger, subnetradar, old_ck_retains_stake, old_ck_emptied, unclassified}`, `s3_base_penalty` (0.6 / 0.4 / 1.0 for `perpetual_off` / 0 — mirrors the frozen constants), `block_number`, `event_at`, `observed_at`, `evidence_json` (includes `extrinsic_id` — 03 has no column for it), `dedupe_key = f"{occupant_key}\|{change_type}\|{new_value or ''}\|{event_at // 86400}"`, `run_id` |
| `owner_event_whitelist` (§3.2) | state (retire, never delete) | `owner_admin` | `reason` ∈ {multisig_migration, post_compromise_recovery, disclosed_transfer, hotkey_rotation, initial_alpha_accounted, false_positive, other}; `applies_to ∈ {coldkey_change, hotkey_repoint, identity_churn, stake_events, perpetual_off, extraction, initial_alpha, all}`; inclusive `window_start`/`window_end`; optional `old_coldkey`, `new_coldkey`, `owner_change_id` (the API/CLI parameter `change_id`); `evidence_url` NOT NULL; `note`; `created_by`/`created_at` (effective for decisions with `t ≥ created_at`); `retired_by`/`retired_at`. 03 CHECK: `applies_to='perpetual_off'` ⇒ `reason ∈ {post_compromise_recovery, false_positive}` |
| `review_queue` (§3.2) | state | `review.enqueue` / `resolve_review` / `auto_clear` | kinds (exactly 03's CHECK list) and statuses §2.10; `dedupe_base = f"{kind}\|{ref}"`, `dedupe_key = f"{dedupe_base}\|{created_at}"` (unique among open rows only) — for 10's rows `dedupe_key = f"llm_flag_candidate\|{post_id}\|{label_version}"`; `ref_table ∈ {owner_changes, team_members, events}` + `ref_id`; `post_id`, `label_version`, `event_type`, `materiality`; `suggested_flag`; `priority` 1 (urgent) … 3; `summary`, `evidence_url`, `payload_json`; `created_at`; `resolved_by`, `resolved_at`, `resolution_note`, `resolution_action ∈ {confirm_flag, whitelist, key_compromise, dismiss, done, auto_cleared}` |

`review.enqueue()` looks up an `open` row with the same `dedupe_key` first (single-writer SQLite, same transaction) and merges `payload_json` into it; only when none is open does it insert a new row.

Also written: `manual_notes` (`kind='team_disclosure'`, `value ∈ {anonymous, clear}`, `body`, `evidence_url`, `source_ref` for legacy notes, `created_at`, `created_by`; 03 columns), `manual_flags` (append-only; `confirmed` rows for `owner_extraction`/`key_compromise` from `confirm_flag`, the override and the LLM-confirmation sync; `cleared` rows only for an active flag — 02 §D5; columns `occupant_key, flag, state, evidence_url, note, set_by, set_at`), `chain_rest_snapshots` (raw C6/C7 bodies with `source='rufus'|'subnetradar'`; 03's DDL of 05's table), `job_state` (runtime watermarks only: `activity_wm:*`, `legacy_done`, `watch_wm`, `rpc_seed_done`; operator pauses live in `control_state`, 02 §B12), `events`, `metric_values`, `collector_runs`.

### 3.2 Tables read

`occupants` (`status`, `network_registered_at`, `registered_at_est`, `registered_ts`, `first_emission_block`, `activated_at`, `last_seen_block`), `subnets.owner_coldkey`, `identity_observations` (subnetalpha `team_info_html`), `identity_resolved` via `registry.resolved()` (`owner_name`, `owner_url`, `owner_github`, `x_url`, `github_repo`, `subnet_url`, `docs_url`), `identity_coverage`, `events` (`identity_changed`, `netuid_reoccupied`), `chain_hourly` (named columns `owner_personal_perpetual_alpha`, `owner_personal_decaying_alpha`, `owner_personal_locked_alpha`, `total_subnet_stake`, `tao_liquidity` — rao-scaled, §2.3 Units — `price`, `block_number`, `source`, `subnet_owner`; `owner_cut_per_block` from `extras_json` when present — not in the verified SSE field list, so the hourly `earned` path is expected to be rare), `chain_rest_snapshots` (kind `subnets` rows written by 04's `registry_occupants`, 02 §G2), `rpc_owner_history` (spec 17: `occupant_key, block_number, event_at, owner_coldkey, owner_hotkey, change`), `manual_flags`, `manual_notes`, `red_flag_state` (12; `active` only, for the override's clears — §2.10), `job_state`, spec 10's confirmed `owner_extraction_evidence` events.

### 3.3 `metric_values` produced (`stage='raw'`; 02 §B3/§B4, §C1/§C2)

Rows are written through the single writer `app/storage/pit.py::put_metric_values` (02 §B4; `upsert_metric_values` is an alias; keys are validated against 03's `METRIC_KEYS`) with `stage='raw'`, `spec_version = settings.SPEC_VERSION` (`ts-1.0`), `source='integrity'` (02 §B9), `observed_at` = run time, `run_id`, `backfilled` (1 only for catch-up rows). **Status lives only in `value_json.status`** ∈ {`ok`, `absent`, `failed`, `not_applicable`} — 03 generates the `status` column from it and this spec never writes a status column (02 §B3) — with `value_json.reason` on every non-`ok` row; 03's CHECKs: `ok` ⇒ `value` NOT NULL, `failed`/`not_applicable` ⇒ `value` NULL. `as_of` = 00:00Z for the official 00:07Z run and the slot hour (06:00/12:00/18:00Z) for the provisional runs; a row is provisional iff `as_of % 86400 ≠ 0` (there is no `is_provisional` column). First success wins (02 §B4): an `ok`/`absent`/`not_applicable` row is never replaced, a `failed` row may be replaced by a later observation for the same `as_of`. One row per key per non-deregistered occupant (netuid ≠ 0). This spec never writes `stage='norm'` rows.

| metric_key | `value` | `value_json` inputs (spec 12 §2.4 keys) | spec 12 normaliser | w (P4 = 18) | fresh d (02 §C6) |
|---|---|---|---|---|---|
| `team_doxxed` | final 1.0 / 0.6 / 0.3; 0.0 with `status='absent'` when anonymous | `verified`, `candidates`, `disclosure` | `prenorm` | .25 | 90 |
| `owner_identity` | 1.0 / 0.0 (`absent`) / NULL (`failed`) | — | `binary` | .05 | 2 |
| `owner_commitment` | final S2 | `perp`, `total`, `lock_ratio`, `auto`, `commit_pct`, `age_days` | `prenorm` | .20 | 2 |
| `owner_retention_30d` | final S1 (0.0 `ok` on any hard trigger) | `sell_ratio`, `max_impact7`, `hard[]`, `earned_alpha` | `prenorm` | .20 | 2 |
| `control_stability_90d` | final S3 | `observed_since` (evidence `events[]`) | `prenorm` | .15 | 2 |
| `owner_overhang` | final S5 | `dump_impact`, `hidden_alpha` | `prenorm` | .05 | 2 |
| `tenure` | raw days `tenure_days(head_block, network_registered_at)` | (evidence `head_block`, `head_source`, `network_registered_at`) | `tenure(120, 365, floor .3)`; fallback key 05 `occupant_age_days` | .10 | 2 |
| *aux:* `owner_extraction_trigger`, `owner_control_change_trigger` (1.0 / 0.0 / NULL → flags `owner_extraction` / `owner_control_change`) | | §2.9 (`reasons[]` or `kind`, `trigger_ids[]`, `identity_churn`) | `none` | 0 | 2 |
| *evidence:* `owner_lock_ratio`, `owner_dump_impact`, `owner_holding_share`, `owner_sell_impact_7d`, `team_verified_count`, `team_candidate_count` | | | — (in 03's catalog, not in 12's registry) | 0 | — |

Because `value` is the final score, spec 12's `prenorm` returns it unchanged and T43 checks the six scores against spec 12's reference functions on the shared anchors (12 §9 `test_normalise`). The edge rules where this spec is stricter than the bare master formula are documented, not divergences: `grace_period` (S2 → None, §11 Q4), the `OWNER_MIN_EARNED_ALPHA` floor (S1), whitelists (S1/S3) and `lock_ratio` when `total = 0` (S2).

### 3.4 Events emitted

Every event goes through 03's `emit_event(db, *, event_type, occupant_key, netuid, severity, event_at, observed_at, payload, dedupe_key, source='integrity', run_id)` (02 §B9; 02 §D1: unknown types and missing required payload keys are rejected, extra keys are allowed; `ON CONFLICT DO NOTHING` on `(event_type, dedupe_key)`). Types, severities (`info | warning | critical`, 02 §B2), required payload keys and dedupe patterns are 03 §3.9's; this table is this spec's **final owner/team list** (02 §D1). Two rows are not yet in 03 §3.9: `owner_coldkey_swap_announced` (02 §D1 requires it) and the ts-1.1 `owner_self_dealing_suspected`. Payload keys in parentheses after the required set are extras.

| event_type | severity | trigger | payload (required, 03) | dedupe_key |
|---|---|---|---|---|
| `owner_control_change` | warning | D1 | `{netuid, change_id, change_type, old_coldkey, new_coldkey, classification_basis, detected_via, whitelisted, review_id}` (+ `swapped_at`) | `occ\|ck\|<new_coldkey>` |
| `owner_coldkey_swap_announced` | warning | D9 | `{netuid, change_id, extrinsic_id, block_number, announced_at}` | `occ\|announce\|<extrinsic_id>` (proposed to 03) |
| `owner_hotkey_repointed` | info | D2 | `{netuid, change_id, old_hotkey, new_hotkey}` (+ `block_number`) | `occ\|hk\|<new_hotkey>` |
| `owner_identity_churn` | warning | D2 pair | `{netuid, change_id, repoint_change_id, identity_event_id, field, days_apart}` | `occ\|churn\|<change_id>` |
| `owner_perpetual_off` | critical | D3 | `{netuid, change_id, perp_before, perp_after, decay_before, decay_after, detected_via, hour_ts}` (+ `coldkey, block_number, extrinsic_id`; `hour_ts = floor_hour(event_at)` for Rufus/extrinsic detections) | `occ\|perpoff\|<day>` |
| `owner_decaying_lock_added` / `owner_lock_reduced` | info / warning | D3b / D3c | `{netuid, delta_perp, delta_decay, hour_ts}` | `occ\|<event_type>\|<day>` |
| `owner_extraction_detected` | critical | D4 (features) and D4b (ingest of a full-exit extrinsic) | `{netuid, change_id, reasons[], sell_ratio, max_impact7, tao_rcv_30d, out_alpha_30d, earned_30d, top_event_uids[≤5]}`; `reasons ⊆ {full_exit_extrinsic, sell_impact_7d, sell_ratio}`, ⊇ `full_exit_extrinsic` for D4b, whose not-yet-computed numbers are `null` | `occ\|extract\|<iso_week>` |
| `owner_key_compromise_override` | warning | D5 | `{netuid, window_start, window_end, whitelist_id, evidence_url, by}` | `occ\|kc\|<whitelist_id>` |
| `owner_auto_lock_changed` / `owner_cut_toggled` | info | D6 | `{netuid, old, new, source}` (+ `block_number`) | `occ\|<event_type>\|<day>` |
| `owner_source_disagreement` | info | D7/D8 | `{netuid, field, values:{source: value}, since}` | `occ\|srcdis\|<field>\|<day>` |
| `owner_position_unexplained` | info | reconcile | `{netuid, prev_total, curr_total, inflow, outflow, unexplained}` | `occ\|unexpl\|<day>` |
| `owner_concentration` | info | S4 v1 | `{netuid, owner_holding_share, lock_ratio}` | `occ\|conc\|<iso_week>` |
| `owner_self_dealing_suspected` | warning | S4 ts-1.1 only (`OWNER_S4_ENABLED`) | `{owner_vali_stake_share, self_weight, owner_holding_share, lock_ratio}` | `occ\|{d30}` (proposed to 03) |
| `owner_data_degraded` | warning | features | `{n_none, n_total, reasons{}}` when > 25 % of occupants have S1, S2 and S3 all None | `owner_degraded\|<day>` (system event: occupant and netuid NULL) |
| `team_member_added` / `team_member_verified` / `team_member_rejected` / `team_member_delisted` | info | §2.2 | `{member_id, name, role, source, status, checked_at?, evidence_count?, domains[]?, reason?, by}` | `team\|<member_id>\|<type>\|<checked_at or day>` |
| `team_disclosure_changed` | info | seed / manual | `{old, new, source}` | `occ\|disc\|<new>\|<day>` |

`occ` = `occupant_key`; `<day>` = `event_at // 86400`; `<iso_week>` = the ISO year-week of `event_at` (e.g. `2026-W39`); `{d30}` = 03's re-emission bucket. `event_at` = the change's `event_at` (hour of the chain row, the event's block time, or the operator action time); `observed_at` = detection time. **Routing (02 §D4; owner 14):** `owner_extraction_detected`, `owner_perpetual_off` and `owner_control_change` are digest-only trigger evidence — the immediate page is spec 12's `red_flag_raised`/`red_flag_cleared`; 04's `identity_changed(field='owner_coldkey')` is `info`/digest, so an owner swap pages once, through the red flag. The other types keep 03 §3.9's alert column (immediate: `owner_identity_churn`, `owner_key_compromise_override`, `owner_data_degraded`). The `team_member_verified` events are also the PIT audit trail used by §2.2.6.

---

## 4. Absence-vs-failure table

Rule: a value the source actually reported (including zero, an empty team field, a missing identity row from a successful call) is an observation; a value we could not obtain, or cannot attribute to this occupant, is `None`. `0.0` is written only after a **successful** fetch that proves absence — never from an `except` branch. Columns below describe the final value this spec writes (spec 12's `prenorm`/`binary` pass it through; only `tenure` is normalised by 12); encoding in `value_json.status`: verifiable absence → `absent` (12 maps to 0.0), real observation → `ok` (possibly 0.0), collection failure → `failed`, undefined for this occupant → `not_applicable` (both → `None`).

| metric | `0.0` (verifiable absence / real zero) | `None` (collection failure / not identifiable) |
|---|---|---|
| `team_doxxed` | no verified/candidate members **and** disclosure `anonymous` (subnetalpha text such as "kept private" / "No team members have been publicly disclosed", a short `_ANON_TOKENS` match, a manual `team anon`, or a legacy "Anonymous team" note when subnetalpha says nothing usable) → `status='absent'`, `reason='anonymous'` | no subnetalpha record; `team_info` empty, placeholder ("Awaiting Data…"), unstructured prose or stale-for-occupant; all named candidates rejected. (Never `None` when ≥ 1 member counts: those map to 0.3/0.6/1.0) |
| `owner_identity` | owner coldkey known, coldkey-identities call succeeded, and no row for that coldkey (spec 04 `status='absent'`), or a row with a name but neither url nor github | owner coldkey unknown; coldkey-identities never succeeded (`unknown`); C1 owner key ≠ `subnets.owner_coldkey` |
| `owner_commitment` | perpetual owner lock = 0 ∧ auto-lock off ∧ age > 90 d (master zero rule; `value = 0.0`, `ok`); otherwise the formula can legitimately give low values | `perp` or `total` unavailable (C2 and C1 fallback both failed, chain_hourly stale > 2 h and no snapshot ≤ 36 h) → `failed`; zero lock with unknown/off auto-lock and age ≤ 90 d → `not_applicable`/`grace_period` |
| `owner_retention_30d` | any hard trigger H1/H2/H3 — evaluated before the cut-disabled shortcut (02 §C1) and even with incomplete coverage; `sell_ratio > 1` includes sales while < `OWNER_MIN_EARNED_ALPHA` was earned and any outflow above it while the cut is off — all `value = 0.0`, `status='ok'` | pre-launch (`not_applicable`); activity coverage does not reach the window start (first days of collection, C4 failures > 36 h) or `earned` unknown (no cut data), with no hard trigger (`failed`) |
| `control_stability_90d` | a non-whitelisted `perpetual_off` in 90 d; penalties summing to ≥ 1 | no owner snapshot in 36 h (control not observable) → `failed` |
| `owner_overhang` | `dump_impact ≥ 25 %` (or ≥ 5 % plus the −0.2 hidden-alpha penalty reaching 0) | C2 and the C1 fallback failed (no `avail`); `price`/`tao_liquidity` missing or `tao_in ≤ 0` (not started) |
| `tenure` | never 0 (raw days > 0; 12's normaliser floors at 0.3) | registration block unknown (`no_registration_block`) or no head block (`no_head_block`) → `failed`; spec 12 then uses 05's `occupant_age_days` |
| `owner_extraction_trigger` / `owner_control_change_trigger` | no qualifying condition (real "clear", `value = 0.0`, `ok`) | owner data unobservable (as defined in §2.9) → `failed` (spec 12: no flag state change) |

Collector-level mapping (what the job writes on each outcome):

| outcome | rows written | effect on metrics |
|---|---|---|
| C1 OK, owner keys present | `owner_snapshots` row | normal |
| C1 OK but keys null/blank (TMC field missing) | row with `owner_coldkey NULL`, `fetch_status_json.c1="ok:no_keys"` | all owner metrics `None`; `owner_data_degraded` if widespread |
| C1 HTTP failure, no list fallback | no snapshot for `O` this run | owner metrics `None` once the previous snapshot is > 36 h old |
| C2 `200` with an empty list / no row for n | snapshot with `owner_total_alpha = 0`, `owner_available_alpha = 0` (TMC says no stakes) | real zero |
| C2 `404` for the coldkey | if C1's `owner_personal_stake` is also 0/absent → treated as the empty list above; otherwise C2 columns `NULL`, `c2="fail:404_inconsistent"` (an owner that received a cut should have a position — an indexer gap is likelier than a real zero) | real zero / `None` accordingly |
| C2 failure (5xx/timeout/429 exhausted) | snapshot with C2 columns `NULL`, `c2="fail:<err>"` | S2/S5 fall back to TMC snapshot fields, else `None` |
| C3 failure | no `owner_position_hourly` rows | no metric depends solely on C3 |
| C4 OK, zero rows | watermark advances; no events | real "no activity" → S1 can be 1.0 |
| C4 failure | watermark unchanged; C5 attempted within its cap | S1 `None` once coverage is > 36 h stale |
| Rufus unreachable / stale | `rufus_*` `NULL` or `rufus_stale=1` | S2 uses TMC fields; `auto` may become `None` (renormalised) |
| subnetradar parse failure | raw stored only | none (cross-check only) |
| TaoStats disabled/429 | nothing | keyless path only |

---

## 5. Configuration

All keys live on spec 01's `Settings` (pydantic-settings, `extra="ignore"`), are read dynamically (`app_config.settings.X`), and appear in `.env.example`. Validators are listed in the last column. **Normaliser parameters and master-fixed thresholds are not env keys** (spec 01 §5: "weights, normaliser parameters and red-flag multipliers are not config: they are frozen in `app/scoring/spec.py` per `SPEC_VERSION`"); they are listed separately below as constants.

| key | type | default | validator / notes |
|---|---|---|---|
| `TEAM_ENABLED` | bool | `true` | master switch for team seeding, features and reminders |
| `TEAM_SEED_ENABLED` | bool | `true` | `team_seed_daily` job |
| `TEAM_SEED_SUBNETALPHA_ENABLED` | bool | `true` | |
| `TEAM_SEED_LEGACY_ENABLED` | bool | `true` | one-shot, guarded by `job_state.legacy_done` |
| `TEAM_LEGACY_NOTES_DIR` | path | `"seed/subnet_edge"` | resolved with `settings.path()` (checklist A3 copies the notes there) |
| `TEAM_LEGACY_AS_OF` | str ISO | `""` → spec 04's `LEGACY_AS_OF` (`2026-06-02T11:03:41Z`) | as-of of the edge notes for the re-occupancy guard; parsed with `parse_ts()` |
| `TEAM_VERIFY_MIN_SOURCES` | int | `2` | ≥ 2 (policy floor — values < 2 rejected) |
| `TEAM_EVIDENCE_EXCLUDED_DOMAINS` | list[str] | `["subnetalpha.ai"]` | lower-cased registrable domains |
| `TEAM_REVERIFY_DAYS` | int | `90` | 30..365; reminder age (master §7.7 team freshness 90 d) |
| `TEAM_VERIFY_STALE_DAYS` | int | `180` | ≥ `TEAM_REVERIFY_DAYS`; after this a verification counts as "named" (0.3) |
| `TEAM_ANON_MAX_CHARS` | int | `200` | 20..2000; length gate for `_looks_anonymous` |
| `TEAM_NAME_MAX_TOKENS` | int | `5` | 2..8 |
| `OWNER_ENABLED` | bool | `true` | master switch for `owner_collect_daily` |
| `OWNER_COLLECT_CRON` | str `HH:MM` | `"20:30"` | UTC; **pinned** — the validator rejects any other value (02 §E3: `owner_collect_daily` runs 20:30Z until 23:30Z at most; 02 §G3 paces it inside that window, clear of the 00:07Z features run and 05's 02:10Z batch) |
| `OWNER_KEYS_SOURCE` | enum `auto\|list\|detail` | `auto` | `list` = never call C1; `detail` = always call C1 |
| `OWNER_TMC_MAX_PER_MIN` | int | `5` | 1..5 (02 §G3 pacing); job-local ceiling on top of 01's `ProviderLimiter('tmc')` (8/min), leaving ≥ 3/min for 05's hourly tier and 04 |
| `OWNER_TMC_MAX_PER_RUN` | int | `518` | 100..518 (02 §G3 detail-mode ceiling); TMC requests per `owner_collect_daily` run; at the cap the run issues no further TMC request (C3 and back-fill pages yield first, §6) |
| `OWNER_ACTIVITY_PAGE_LIMIT` | int | `100` | 10..200 (effective page size decided by the probe; the client just pages) |
| `OWNER_ACTIVITY_MAX_PAGES` | int | `10` | per coldkey per run |
| `OWNER_ACTIVITY_OVERLAP_SEC` | int | `3600` | ≥ 600 (old `OVERLAP_SEC` was 600 for the global feed) |
| `OWNER_ACTIVITY_BACKFILL_DAYS` | int | `35` | ≥ 31 so the first S1 window is complete |
| `OWNER_BACKFILL_MAX_CALLS_PER_RUN` | int | `128` | extra first-run C4 back-fill pages per run; they only use headroom under `OWNER_TMC_MAX_PER_RUN` and resume on the next run |
| `OWNER_ACTIVITY_TIME_FORMAT` | enum `auto\|ms\|s\|iso` | `auto` | `auto` reads `data/owner_field_map.json` |
| `OWNER_AMOUNT_UNITS` | enum `auto\|rao\|unit` | `auto` | |
| `OWNER_STAKE_REMOVED_ENABLED` | bool | `true` | C5 |
| `OWNER_STAKE_REMOVED_MAX_CALLS_PER_DAY` | int | `20` | 0..100 |
| `OWNER_RECONCILE_MIN_ALPHA` | float | `500.0` | α |
| `OWNER_NEURON_SPAN` | enum `1D\|1W\|1M` | `1W` | C3 span |
| `OWNER_RUFUS_ENABLED` | bool | `true` | C6 |
| `OWNER_RUFUS_TABLE_URL` | str | `"https://s3.hippius.com/rufus/public/conviction-table/data/table.json"` | |
| `OWNER_RUFUS_BAKED_AT_URL` | str | `"https://s3.hippius.com/rufus/public/conviction-table/data/baked-at.json"` | |
| `OWNER_RUFUS_MAX_AGE_HOURS` | int | `48` | |
| `OWNER_SUBNETRADAR_ENABLED` | bool | `true` | C7 |
| `OWNER_SUBNETRADAR_URL` | str | `"https://subnetradar.com/swaps"` | |
| `OWNER_TAOSTATS_ENABLED` | bool | `true` | effective only when `settings.taostats_active()` (`TAOSTATS_ENABLED` and `TAOSTATS_API_KEY`, spec 01) |
| `OWNER_TAOSTATS_EXTRINSIC_WEEKDAY` | int | `0` | 0=Mon; weekly `set_perpetual_lock` + `announce_coldkey_swap` sweep |
| `OWNER_STREAM_ENABLED` | bool | `false` | optional 2nd SSE slot (§2.3); **inert in v1** (no `LongTask` registered, 02 §E2); validator refuses `true` when 05's `TMC_SSE_BLOCKS_STREAM=true` (enum `auto|true|false`; with `auto` the stream task checks 05's live SSE connection count first, §2.3; `TMC_SSE_MAX_CONNECTIONS` = 2) |
| `OWNER_WATCH_HOURLY_ENABLED` | bool | `true` | DB-only hourly D1/D3 |
| `OWNER_RPC_HISTORY_SEED_ENABLED` | bool | `true` | one-shot import of spec 17's `rpc_owner_history` into `owner_changes` (guarded by `job_state(job='owner_rpc_seed', key='rpc_seed_done')`; an **absent** row = armed. Re-armed by deleting that row: automatically by 17's `backfill_rpc` when an era inserted `rpc_owner_history` rows, or manually with `python -m app ops rearm owner_rpc_seed` (16's ops CLI; deletes exactly that row, then the next 05:20Z run or `python -m app run-job owner_rpc_seed` imports; the import is idempotent)) |
| `OWNER_SNAPSHOT_MAX_AGE_HOURS` | int | `36` | staleness → `None` (also bounds the `owner_cut_enabled` read) |
| `OWNER_MIN_EARNED_ALPHA` | float | `1.0` | α; floor of the S1 denominator (dust guard, not a scoring threshold) |
| `OWNER_PERP_OFF_MIN_ALPHA` | float | `100.0` | α; D3 noise floor (detector sensitivity, not a score parameter) |
| `OWNER_PERP_OFF_MATCH_TOL` | float | `0.10` | 0..0.5 |
| `OWNER_OVERRIDE_MAX_WINDOW_DAYS` | int | `45` | |
| `OWNER_SOURCE_DISAGREE_TOL` | float | `0.05` | relative tolerance for Rufus vs TMC perpetual-lock cross-check (info only) |
| `OWNER_S4_ENABLED` | bool | `false` | ts-1.1 detector; the keyless `owner_concentration` event is always on |
| `OWNER_S4_HOLDING_SHARE_MAX` / `OWNER_S4_LOCK_RATIO_MIN` | float | `0.25` / `0.5` | event thresholds only (research §5 S4; no score effect in ts-1.0) |
| `OWNER_S4_VALI_STAKE_SHARE_MAX` / `OWNER_S4_SELF_WEIGHT_MAX` | float | `0.30` / `0.20` | ts-1.1 |
| `DTAO_LAUNCH_UTC` | str ISO | `"2025-02-13T00:00:00Z"` | = old `DTAO_LAUNCH_TS` 1739404800 (`events.py:22`) |
| `INTEGRITY_FEATURES_ENABLED` | bool | `true` | `integrity_features` job |
| `INTEGRITY_FEATURES_AT_UTC` | list[str] | `["00:07","06:07","12:07","18:07"]` | 00:07 is official (after spec 05's 00:05 `chain_features`, before spec 12's 00:15 scoring) |
| `INTEGRITY_FEATURES_CATCHUP_DAYS` | int | `7` | recompute missing official `as_of` days on start (`backfilled=1`, `value_json.backfilled=true`) |

**Frozen constants (not env keys; `app/integrity/constants.py`, `ts-1.0`).** Master §7.3/§7.5 fix these; spec 12's reference functions (`app/features/normalise.py`) hard-code the same values and `tests/integrity/test_constants.py` asserts equality. Changing any of them is a new `spec_version`.

| constant | value | used by |
|---|---|---|
| `RETENTION_WINDOW_DAYS` / `SELL_IMPACT_WINDOW_DAYS` / `SELL_IMPACT_MAX` | 30 / 7 / 0.02 | S1 window, `max_impact7`, H2 |
| `COMMIT_PCT_MIN` / `COMMITMENT_ZERO_AGE_DAYS` | 0.05 / 90 | S2 (also the `grace_period` boundary) |
| `S3_WINDOW_DAYS` / `S3_HALF_LIFE_DAYS` / `S3_PENALTY_SWAP` / `S3_PENALTY_REPOINT_CHURN` | 90 / 45 / 0.6 / 0.4 | S3, `owner_changes.s3_base_penalty` |
| `REPOINT_CHURN_WINDOW_DAYS` | 14 | D2 pairing (master "within 14 d") |
| `CONTROL_CHANGE_WINDOW_DAYS` / `EXTRACTION_HARD_WINDOW_DAYS` / `PERPETUAL_OFF_WINDOW_DAYS` | 30 / 30 / 90 | `owner_control_change_trigger`; `owner_extraction_trigger` (hard triggers / `perpetual_off`) — 02 §C2 |
| `OVERHANG_FULL` / `OVERHANG_ZERO` / `FREE_ALPHA_PENALTY` | 0.05 / 0.25 / 0.2 | S5 |
| `FREE_ALPHA_CUTOFF_UTC` | `2026-05-08T00:00:00Z` (hotfix v3.3.15-402) | S5 `hidden(O)`; should become the hotfix block number (§11 Q7) |
| `TEAM_MAP` | ≥2 → 1.0 · 1 → 0.6 · named → 0.3 · anonymous → 0 | `team_doxxed` (= spec 12's reference `team_map`) |

Existing keys reused, not redefined: `TAOSTATS_ENABLED`, `TAOSTATS_API_KEY` (checklist C3), `TAOSTATS_RATE_PER_MIN`, `TAOSTATS_MONTHLY_CAP_REQ`, `TMC_RATE_PER_MIN` (8), `TMC_RATE_BURST` (2), `TMC_HOURLY_CAP` (500), `TMC_MONTHLY_CAP` (90,000) (02 §F2), `TMC_SSE_MAX_CONNECTIONS` (2; `SSE_MAX_CONNECTIONS` is retired), 05's `TMC_SSE_BLOCKS_STREAM` (`SSE_BLOCKS_STREAM_ENABLED` and `TMC_PAUSE_SENTINEL` are retired, 02 §B12), `SPEC_VERSION` (`ts-1.0`), `SEED_DIR`, `KILL_SWITCH_PATH`, `HTTP_USER_AGENT`, `BLOCK_SECONDS` (12.0) and `LEGACY_AS_OF` (spec 04), `NETWORK_IMMUNITY_PERIOD_BLOCKS` (864000; only used to explain the 120-d tenure knee).

**Pause / kill semantics (02 §B12).** Operator pauses of the five jobs of §6 are rows in `control_state` (API, Telegram and CLI all write it; `job_state` holds only this spec's watermarks); `data/control/<job>.paused` is honoured as an emergency input only. `PAUSE_ALL` pauses all five (none is `ops_exempt`). `KILL_SWITCH` skips only the TaoStats sub-steps (paid provider); the keyless steps continue because the job is `paid=False`. `SCORING_KILLED` does not affect this spec. 05's `TMC_PAUSED` sentinel is retired (02 §B12's closed list); a TMC stop is a pause of 16's `tmc` group (a `control_state` row or the emergency file `data/control/tmc.paused`), which contains `owner_collect_daily`, so 01's wrapper skips the whole run (`paused:operator` / `paused:emergency_file`) — Rufus/subnetradar/TaoStats are skipped too, a paused run writes nothing, and owner metrics decay to `None` after 36 h instead of being computed from partial data. A limiter `BudgetExhausted` (02 §G1) ends the run `paused: budget` with every finished coldkey's watermark committed. Manual domain functions and the CLI work regardless of pauses.

---

## 6. Cadence, rate limits, cost

**Job table** — the single source of this spec's rows for 01's `JOB_TABLE` (02 §E2: owner ids win; 01 mirrors them verbatim). All five are `CollectorJob`s with `max_instances=1`, `coalesce=True`, `min_gap_factor=0` (cron, UTC), `interval_flag=None`, `phase=7`, `ops_exempt=False`; `liveness_max_age_sec` as listed (02 §E4: a job is stale only when enabled, not paused and older than this).

| id | factory (`app.…`) | trigger (UTC) | time | misfire grace s | timeout s | enabled flag | provider | paid | liveness max age |
|---|---|---|---|---|---|---|---|---|---|
| `owner_collect_daily` | `integrity.owner_collect:OwnerCollectJob` | cron `{"hour": 20, "minute": 30}` (`OWNER_COLLECT_CRON`, pinned) | 20:30Z; hard stop 23:30Z, soft deadline 23:15Z (02 §E3) | 3600 | 10800 | `OWNER_ENABLED` | `tmc` (+ `rufus`, `subnetradar`, `taostats`) | no (TaoStats sub-steps honour `KILL_SWITCH`) | 30 h |
| `owner_watch_hourly` | `integrity.owner_watch:OwnerWatchJob` | cron `{"minute": 12}` | hh:12Z | 1800 | 300 | `OWNER_WATCH_HOURLY_ENABLED` | — (DB only) | no | 3 h |
| `team_seed_daily` | `integrity.team_seed:TeamSeedJob` | cron `{"hour": 4, "minute": 20}` | 04:20Z | 21600 | 600 | `TEAM_SEED_ENABLED` | — (DB only) | no | 30 h |
| `integrity_features` | `integrity.features:IntegrityFeaturesJob` | cron `{"hour": "0,6,12,18", "minute": 7}` (`INTEGRITY_FEATURES_AT_UTC`) | 00:07Z official; 06:07/12:07/18:07Z provisional | 10800 | 900 | `INTEGRITY_FEATURES_ENABLED` | — (DB only) | no | 8 h |
| `owner_rpc_seed` | `integrity.owner_history:OwnerRpcSeedJob` | cron `{"hour": 5, "minute": 20}` (no-op unless armed) | 05:20Z | 21600 | 600 | `OWNER_RPC_HISTORY_SEED_ENABLED` | — (DB only) | no | 01 default (daily run keeps `last_run` fresh) |

`owner_collect_daily` never runs outside 20:30–23:30Z (02 §E3/§G3): `soft_deadline = min(started_at + timeout − 900 s, 23:15Z)` — after it no new coldkey is started and every finished coldkey's watermark is already committed — and the loop stops issuing requests at 23:30Z. Startup catch-up is 01's `JobSpec.startup_catchup=True` with `catchup_only_utc="20:30-23:15"` (02 §E1 missed-fire rule: it runs once at boot + `STARTUP_CATCHUP_DELAY_SEC` (300 s) iff `job_state.last_ok` is older than the previous scheduled 20:30Z fire — never a fixed age — and only when that instant falls inside 20:30–23:15Z); otherwise it waits for the next 20:30Z (owner rows turn `failed` once the last snapshot is > 36 h old; nothing is forward-filled). `integrity_features` recomputes missing official `as_of` days within `INTEGRITY_FEATURES_CATCHUP_DAYS` from stored raw data (`backfilled = 1`; spec 12's reconstructed mode admits them; events emitted for those days carry `payload.backfilled = true`, which routes them to the digest — 02 §D4).

**Freshness chain.** `owner_collect_daily` finishes by ≈ 21:50Z steady (≈ 22:15Z at the 518-request cap; never after 23:30Z) → `integrity_features` at 00:07Z uses a 0.6–3.6 h-old snapshot, well inside `OWNER_SNAPSHOT_MAX_AGE_HOURS` (36), and writes `as_of` 00:00Z rows that spec 12's 00:15Z official scoring selects (P4 owner freshness 2 d, 02 §C6). Perpetual-lock-off and owner-key changes are seen hourly by `owner_watch_hourly` from the SSE-backed `chain_hourly` at zero REST cost. Owner sells are seen daily (≤ 24 h latency); a full-exit extrinsic ingested at 20:30–23:30Z reaches `owner_extraction_trigger` at the next 00:07Z run and the red flag at 00:15Z.

**TMC budget** — this job's row of 05's TMC budget table (02 §G3): ≈ 390/day steady, ≤ 518 in detail mode, paced ≤ 5/min inside 20:30–23:30Z; all requests via `TmcClient` → `ProviderLimiter('tmc')` (8/min, `TMC_HOURLY_CAP` 500, `TMC_MONTHLY_CAP` 90,000 — 02 §F2; anonymous per-IP limits 10 req/60 s, 600/h, 100,000/month). Let `N` = non-root occupants ≈ 128 and `K` = distinct owner coldkeys ≈ 110 (UNVERIFIED; worst case `K = N = 128`):

| call | per day | note |
|---|---|---|
| C1 `/subnets/{n}/` | 0 (list carries keys) or `N` = 128 | probe decides; `OWNER_KEYS_SOURCE` |
| C2 `/accounts/coldkey-stakes/{ck}/` | `K` ≈ 110 (+ ≤ 5 D1 classification calls) | cached per coldkey |
| C3 `/analytics/neuron/{n}/{hk}/?span=1W` | `N` = 128 | |
| C4 `/extrinsics/staking-activity/?coldkey=` | `K × 1.15` ≈ 127 | ~15 % extra pages for active owners |
| C5 `/transactions/stakes/stake-removed/` | ≤ 20 | cap |
| **spec 11 steady** | **≈ 390** (C2 + C3 + C4 + C5 ≈ 3 per occupant) | **≤ 518 with C1** (detail mode) = the hard per-run cap `OWNER_TMC_MAX_PER_RUN` |

Totals across all TMC callers (05's table, 02 §G3): steady ≈ 716/day ≈ 21.5 k/month; worst ≈ 1.35 k/day ≈ 40 k/month; peak hour ≈ 311/h — all under 8/min, 500/h and 90 k/month. **First-run back-fill** (C4 pages until every coldkey's coverage reaches 35 d; worst 110 × 10 pages) never raises the daily ceiling: back-fill pages (≤ `OWNER_BACKFILL_MAX_CALLS_PER_RUN`, 128) use only the headroom under the 518 cap — the unused C1 budget in list mode; in detail mode they displace that day's C3 trace calls, which the next run's 1W span repairs — so the back-fill completes in ≈ 9 runs. **Wall time at 5/min:** 390 calls ≈ 78 min, 518 ≈ 104 min — done by ≈ 22:15Z; TaoStats sub-steps run as a concurrent task on their own limiter (≈ 29 min at 5/min), so they do not add to it. **Hourly:** ≤ 5/min ⇒ ≤ 300/h from this job; with 05's hourly tier and 04's `registry_occupants` the peak hour stays ≈ 311/h < 500; 05's 02:10Z batch (≈ 140 req) never overlaps this window. There are no priority classes (02 §G1); nothing here bypasses `TmcClient`/`ProviderLimiter`.

**Other providers:** Rufus (S3 static JSON) 2 req/day, provider `rufus` 1 req/s; subnetradar 1 req/day, provider `subnetradar` 0.2 req/s, `User-Agent = HTTP_USER_AGENT`; both must be added to spec 01's `ProviderLimiter` table. TaoStats (only when keyed): stake events ≈ `K` = 110/day + `/v1/subnets/owners` ≈ 2/day + weekly extrinsic sweeps 2 × 110/week (`set_perpetual_lock`, `announce_coldkey_swap`; ≈ 31/day) + one-off registration events ≈ **143/day ≈ 4.3 k/month** — fits the free tier (5/min, 10 k/month; ≈ 29 min of wall time at 5/min) and Standard (60/min, 50 k/month). TaoStats tier limits are themselves UNVERIFIED (research 01-subnetalpha §2B); spec 01's `TAOSTATS_MONTHLY_CAP_REQ` enforces the cap.

**Cost:** $0 keyless. TaoStats $0 (free tier, if the old key still works) or $49/month (Standard) — operator decision (master §13 item 2, checklist C3). DB growth: `owner_position_hourly` ≈ 128 × 24 rows/day ≈ 3 k rows/day (~0.5 MB/day); `owner_snapshots` 128 rows/day; `owner_stake_events` tens to hundreds/day. No retention pruning needed in year 1 (spec 03 may add a 400-day rollup for `owner_position_hourly`).

---

## 7. Failure modes and fail-safe behaviour

| failure | detection | behaviour |
|---|---|---|
| Job exception | spec 01 wrapper | never raises into the scheduler; `collector_runs.ok=0`; `collector_failing` after `JOB_FAIL_ALERT_THRESHOLD` consecutive failures |
| One occupant's fetches fail | per-occupant `try/except` | `fetch_status_json` records it; other occupants continue; run `ok=0` if ≥ 25 % of occupants have C1 or C2 failures (successful rows kept) |
| TMC 429 / `X-RateLimit-Remaining: 0` | `TmcClient` / 01 limiter | the limiter honours `Retry-After` (02 §G1); calls wait; past the soft deadline (`min(started_at + timeout − 900 s, 23:15Z)`) the job starts no new coldkey and returns `JobOutcome(note='soft_deadline')` with every finished coldkey's watermark committed — the wrapper's hard `asyncio.wait_for` timeout is never the normal stop path |
| TMC hourly or monthly cap reached | limiter raises `BudgetExhausted` (02 §G1) | the run ends `paused: budget` (finished coldkeys' watermarks committed) until the window resets; owner metrics become `None` after 36 h; no forward-fill |
| TMC field rename / missing owner keys | mappers + probe map | `owner_schema_drift` log + `owner_data_degraded` event when > 25 % of occupants lack keys; metrics `None` |
| Staking-activity pagination truncated | `truncated` flag | watermark advances only over received rows; next run resumes; S1 coverage stays honest (`activity_coverage`) |
| Duplicate / overlapping sources | `event_uid` PK + source ranking | idempotent re-ingest; superseded rows excluded PIT-correctly |
| Occupant changes mid-run (`network_registered_at` in C1 ≠ occupant) | C1 check | skip that occupant this run; next run keys under the new occupant (spec 04 transition) |
| Rufus JSON unreachable / schema drift / stale | HTTP error, parser, `baked_at` | fall back to TMC fields; `auto` may be `None` (S2 renormalises); `rufus.schema_ok=false` on `/health` |
| subnetradar down / unparseable | parser | ignored (cross-check only) |
| TaoStats 401/429/cap | spec 01 limiter | TaoStats sub-steps skipped this run; keyless path unaffected |
| False-positive detector hit (multisig migration, hotkey rotation) | operator review | `whitelist` resolution; effective from `created_at` (PIT), historical scores unchanged |
| Wallet compromise (ORO/Connito) | D4/D3/D1 fire as for an exit | flag stays until the operator runs the key-compromise override (§2.10) |
| Operator error in manual data | evidence URLs + events audit trail | `reject_member`, `retire_whitelist`; every change is an `events` row with `by` |
| DB locked / write failure | wrapper | run marked failed; watermarks are written in the same transaction as the events they cover, so no gap is created |
| Laptop power loss | startup catch-up | `owner_collect_daily` runs at the next 20:30Z, or 300 s after boot via `startup_catchup` when its last scheduled 20:30Z fire was missed and the catch-up instant falls inside 20:30–23:15Z (02 §E1, §6); C4 resumes from watermarks (overlap 1 h); C3 1W span repairs up to 7 days of hourly trace; `owner_watch_hourly` re-scans `chain_hourly` since its own watermark (`job_state.watch_wm`), including backfilled hours — `source='analytics_1w'` rows carry `owner_personal_locked_alpha` and `owner_personal_decaying_alpha` but **not** the perpetual column (spec 05 `TMC_ANALYTICS_1W_FIELDS`), so D3 derives `perp = locked − decaying` on them (identity verified on the live table, research 02-keyless §2) and skips pairs that straddle an SSE↔analytics source change unless the units probe marks both sources identical |
| Rows attributed to the wrong occupant after a re-registration inside a run | C1 `network_registered_at` check; spec 04 `netuid_reoccupied` | snapshot skipped; stake events are keyed by the occupant whose `owner_coldkey` matched at `event_at` (PIT `occupant_at(netuid, block)`), never by netuid alone |
| Hidden hard-trigger in a coverage gap (C4 truncated for days) | `activity_coverage` reason | S1 `failed`, not 1.0; `owner_position_unexplained` + C5 reconcile still surface large drops; `/health.integrity.owner.activity_coverage_ok` counts affected coldkeys |

**Never:** forward-fill a snapshot, score a stale snapshot, infer a sell from the unexplained residual, verify a team member automatically, fetch a profile URL, or let a whitelist created today change a score already published for an earlier `as_of`.

**`/health` block** (`collectors.owner_collect_daily` etc. from the wrapper, plus `integrity`): `{owner: {occupants, snapshots_today, keys_missing, c1_fail, c2_fail, c3_fail, c4_fail, c4_truncated, activity_coverage_ok, tmc_requests_today, last_ok}, rufus: {baked_at, stale, schema_ok}, subnetradar: {parse_ok, rows}, taostats: {active, requests_today}, team: {candidates, verified, rejected, reverify_due, unknown}, review: {open_by_kind}, triggers: {owner_extraction: n, owner_control_change: n}}`.

---

## 8. Implementation plan

### 8.1 Files (all under `~/taosentinel/`)

| file | contents |
|---|---|
| `app/integrity/__init__.py` | empty |
| `app/integrity/models.py` | frozen dataclasses: `Candidate`, `TeamInfoParse`, `TeamMemberState`, `TwoSourceResult`, `ActionResult`, `OwnerKeys`, `OwnerPosition`, `StakeEvent`, `OwnerChange`, `WhitelistRow`, `RawMetric(key: str, value: float \| None, status: Literal['ok','absent','failed','not_applicable'], reason: str \| None, value_json: dict)` with `to_metric_result()` → spec 12's `app/features/types.py::MetricResult` (`absence_reason` set iff `status='absent'`; `value_json.status` always set) |
| `app/integrity/constants.py` | the frozen `ts-1.0` constants of §5 |
| `app/features/text.py` | `_ANON_TOKENS` (old `quality.py:33`), `_looks_anonymous` (old `quality.py:108-110`) ported verbatim, plus the public alias `looks_anonymous` spec 12 §8 expects — created here (Phase 7), imported by spec 12 |
| `app/integrity/team_parse.py` | imports `_ANON_TOKENS`/`_looks_anonymous` from `app/features/text.py`; `ANON_PHRASES`, `ROLE_WORDS`, `parse_team_info`, `is_plausible_name`, `name_norm`, `classify_profile_url`, `registrable_domain`, `is_personal_profile`, `check_two_source` — all pure |
| `app/integrity/team.py` | domain functions §2.12, `compute_team_doxxed`, `team_state_as_of`, `due_reverify` |
| `app/integrity/team_seed.py` | `TeamSeedJob(CollectorJob)`, `seed_subnetalpha(db, now)`, `seed_legacy(db, notes_dir, as_of)` |
| `app/integrity/owner_sources.py` | C1–C8 fetchers + tolerant mappers (`map_snapshot`, `map_coldkey_stakes`, `map_neuron_rows`, `map_activity_row`, `map_stake_removed_row`, `parse_rufus`, `parse_subnetradar`), `load_field_map()` |
| `app/integrity/classify.py` | `classify_stake_event`, `mark_phantom_moves`, `SOURCE_RANK`, `supersede_duplicates(db, rows, now)` |
| `app/integrity/owner_collect.py` | `OwnerCollectJob(CollectorJob)` (run loop, pacing, watermarks, reconciliation, D1/D2/D3(Rufus)/D6/D7/D8 calls) |
| `app/integrity/owner_watch.py` | `OwnerWatchJob(CollectorJob)` (hourly D1 from `chain_hourly.subnet_owner`, D3/D3b/D3c) |
| `app/integrity/owner_history.py` | `OwnerRpcSeedJob(CollectorJob)`, `seed_from_rpc_owner_history(db, since_block)` — D1/D2 over spec 17's `rpc_owner_history` change rows (`detected_via='rpc_owner_history'`, `classification_basis='unclassified'` unless TaoStats/subnetradar confirm, `evidence_json.backfilled = true` — the backfill marker the §2.1 replay exception reads); its events carry `payload.backfilled = true` and are written with `alerted_at` preset (02 §J1), and it opens review items only for changes inside the 90-d S3 window |
| `app/integrity/owner_metrics.py` | pure `s1_owner_retention`, `s2_owner_commitment`, `s3_control_stability`, `s5_owner_overhang`, `owner_identity_value`, `tenure_days`, `tenure_row`, `extraction_trigger`, `control_change_trigger`, `holding_share`, helpers `earned_alpha`, `impact7_series` |
| `app/integrity/detectors.py` | `detect_coldkey_change`, `classify_coldkey_change`, `detect_hotkey_repoint`, `pair_identity_churn`, `detect_lock_transitions`, `detect_extraction`, `detect_toggles`, `detect_source_disagreement`, `record_change`; CLI `drill` (dry run) |
| `app/integrity/whitelist.py` | `item_class`, `is_whitelisted`, `refresh_whitelist_cache` |
| `app/integrity/owner_admin.py` | `add_whitelist`, `retire_whitelist`, `override_key_compromise` |
| `app/integrity/review.py` | `enqueue`, `list_open`, `resolve_review`, `auto_clear` |
| `app/integrity/features.py` | `IntegrityFeaturesJob(CollectorJob)`, `compute_all(db, as_of, decision_time, official) -> list[RawMetric]`, `sync_confirmed_llm_flags(db, t)`; writes through 03's `app/storage/pit.py::put_metric_values` (02 §B4); exposes the C5-shaped entry points `compute_<metric_key>(db, occupant_key, as_of: datetime, *, mode: Literal['strict', 'reconstructed'] = 'strict') -> MetricResult` for the 7 P4 keys and the 2 trigger keys (02 §C5 — the keyword-only `mode` keeps the C5 call shape; used by 13/17 reconstruction, which pass `mode='reconstructed'` so backfilled owner rows are read by `event_at`, §2.1 PIT rule — 12's registry treats these keys as collector rows and never calls them) |
| `app/whale/events.py` | **ported verbatim** from `E:\SN_Trader\SubnetTrader\app\whale\events.py` lines 1-170 (stdlib only; master §2 "port, dormant until a TaoStats key exists"). No other spec ports it — spec 17 lists `whale_events` as TaoStats-only and does not import the module — so this spec owns the port |
| `scripts/manual_entry.py` | argparse CLI §2.12 (`team`, `owner`, `review` sub-commands); prints `ActionResult` JSON |
| `scripts/owner_probe.py` | live probe (§10 step 1): 1 call each of C1–C5 for given netuids, Rufus, subnetradar; prints key sets, time format, units, page size; writes `data/owner_field_map.json` with `write_atomic` |
| `tests/integrity/…` | §9 |

### 8.2 Signatures

```python
# team_parse.py
def parse_team_info(html: str) -> TeamInfoParse
def is_plausible_name(s: str) -> bool
def name_norm(s: str) -> str
def classify_profile_url(url: str) -> Literal["linkedin", "x", "github", "website", "other"]
def registrable_domain(url: str) -> str | None
def is_personal_profile(url: str, *, org_handles: set[str], x_handles: set[str]) -> bool
def check_two_source(profile_url: str | None, evidence_urls: list[str], *, attestation_notes: Sequence[str] = (),
                     org_handles: set[str], x_handles: set[str], excluded: set[str], min_sources: int) -> TwoSourceResult
                     # evidence_count = distinct non-excluded URL domains + distinct attestation notes (02 §H3)

# team.py
async def add_member(db, *, netuid: int | None = None, occupant_key: str | None = None, name: str, role: str | None,
                     profile_url: str | None, evidence_url: str | None, evidence_note: str | None, source: str,
                     created_by: str, verify: bool = False) -> ActionResult
async def verify_member(db, member_id: int, *, evidence_urls: list[str], note: str | None, by: str) -> ActionResult
                                          # evidence_urls may be empty when a stored URL + this note make 2 items; a non-empty
                                          # human `note` (10..300 chars) is recorded as an operator attestation item (§2.2.2)
async def reject_member(db, member_id: int, *, reason: str, evidence_url: str | None, by: str) -> ActionResult
async def set_disclosure(db, *, netuid: int, status: Literal["anonymous", "clear"], evidence_url: str, note: str | None, by: str) -> ActionResult
async def list_members(db, *, netuid: int | None = None, occupant_key: str | None = None, status: str | None = None,
                       stale_only: bool = False) -> list[dict]
async def team_state_as_of(db, occupant_key: str, t: datetime) -> list[TeamMemberState]
async def compute_team_doxxed(db, occupant_key: str, t: datetime) -> RawMetric          # value = final map value; value_json {verified, candidates, disclosure}
async def due_reverify(db, now: datetime) -> list[int]

# owner_sources.py   (TmcClient from app/collectors/chain_tmc.py, spec 05 — 02 §G2; never raise — return (data | None, status_str))
async def fetch_owner_keys(tmc, db, netuid: int, occupant: Occupant, *, source_mode: str) -> tuple[OwnerKeys | None, str]
async def fetch_coldkey_stakes(tmc, coldkey: str) -> tuple[list[dict] | None, str]
async def fetch_neuron_series(tmc, netuid: int, hotkey: str, span: str) -> tuple[list[dict] | None, str]
async def fetch_activity(tmc, coldkey: str, from_ts: int, to_ts: int, *, page_limit: int, max_pages: int,
                         time_format: str) -> tuple[list[dict], str, bool]            # rows, status, truncated
async def fetch_stake_removed(tmc, hotkey: str, netuid: int, *, max_pages: int) -> tuple[list[dict] | None, str]
async def fetch_rufus(http, *, table_url: str, baked_url: str, last_baked_at: int | None) -> tuple[RufusTable | None, str]
async def fetch_subnetradar(http, url: str) -> tuple[list[dict], str]
async def fetch_taostats_delegations(http, coldkey: str, since_ts: int) -> tuple[list[dict] | None, str]
async def fetch_taostats_owner_history(http, netuid: int | None) -> tuple[list[dict] | None, str]
def parse_rufus(payload: Any) -> dict[int, RufusSubnet]
def parse_subnetradar(body: str, content_type: str) -> list[dict]

# classify.py
def classify_stake_event(row: dict, *, occupant_netuid: int, owner_coldkey: str) -> StakeEvent | None
def mark_phantom_moves(events: list[StakeEvent]) -> list[StakeEvent]          # whale_impact.py:158-178 rule
SOURCE_RANK: dict[str, int] = {"taostats_delegation": 0, "tmc_staking_activity": 1, "tmc_stake_removed": 2, "tmc_stream": 3}

# owner_metrics.py  (pure; inputs already filtered PIT by the caller)
def earned_alpha(cut_hourly: Mapping[int, float], cut_snapshots: Sequence[float], *, window: tuple[int, int]) -> tuple[float | None, str]
def impact7_series(events: Sequence[StakeEvent], tao_in_hourly: Mapping[int, float], *, window: tuple[int, int], days: int = 7) -> list[float]
# each returns RawMetric: value = the final score (tenure: raw days), value_json = spec 12 §2.4 input keys + evidence (§3.3)
def s1_owner_retention(events, *, earned: float | None, impact7: Sequence[float], owner_cut_enabled: bool | None,
                       pre_launch: bool, activity_covered: bool, min_earned: float) -> RawMetric   # value = S1 (H1–H3 before the cut-disabled shortcut)
def s2_owner_commitment(*, perp: float | None, total: float | None, auto: bool | None, commit_pct: float | None,
                        owner_cut_enabled: bool | None, age_days: float) -> RawMetric            # value = S2
def s3_control_stability(changes: Sequence[OwnerChange], *, t: int, observable: bool,
                         whitelisted: Callable[[OwnerChange], bool]) -> RawMetric               # value = S3; value_json {observed_since, events[]}
def s5_owner_overhang(*, avail: float | None, price: float | None, tao_in: float | None,
                      hidden_alpha: bool) -> RawMetric                                           # value = S5
def owner_identity_value(name_status: str, has_url: bool, has_github: bool, key_match: bool) -> RawMetric
def extraction_trigger(hard_hits: Sequence[StakeEvent | OwnerChange], changes: Sequence[OwnerChange], *, t: int,
                       observable: bool, whitelisted: Callable) -> RawMetric    # owner_extraction_trigger: hard ≤ 30 d, perpetual_off ≤ 90 d
def control_change_trigger(changes: Sequence[OwnerChange], identity_events: Sequence[dict], *, t: int, observable: bool,
                       whitelisted: Callable) -> RawMetric
def tenure_days(head_block: int, network_registered_at: int, block_seconds: int = 12) -> float
def tenure_row(head_block: int | None, network_registered_at: int | None, *, head_source: str) -> RawMetric   # raw days / no_registration_block

# detectors.py
async def detect_coldkey_change(db, occupant_key: str, prev: OwnerKeys | None, curr: OwnerKeys, *, run_id: str, now: int,
                                classify: Callable[[], Awaitable[tuple[str, str]]]) -> OwnerChange | None
async def detect_hotkey_repoint(db, occupant_key: str, prev: OwnerKeys | None, curr: OwnerKeys, *, run_id: str, now: int) -> OwnerChange | None
async def pair_identity_churn(db, occupant_key: str, *, now: int, window_days: int) -> list[OwnerChange]
def detect_lock_transitions(rows: Sequence[ChainHourlyOwnerRow], *, min_alpha: float, match_tol: float) -> list[tuple[str, dict]]
async def detect_extraction(db, occupant_key: str, s1: RawMetric, *, now: int, run_id: str) -> OwnerChange | None
async def emit_full_exit_extraction(db, new_events: Sequence[StakeEvent], *, now: int, run_id: str) -> int   # D4b: owner_extraction_detected(reasons=['full_exit_extrinsic'])
async def record_change(db, change: OwnerChange, *, event_type: str, severity: str, review_kind: str | None,
                        suggested_flag: str | None = None, priority: int = 2) -> int    # owner_changes + events + review (one transaction)

# whitelist.py
def item_class(change_type_or_kind: str) -> set[str]
async def is_whitelisted(db, occupant_key: str, *, item_class: set[str], event_at: int, t: datetime,
                         old_coldkey: str | None = None, new_coldkey: str | None = None) -> int | None

# owner_admin.py
async def add_whitelist(db, *, netuid: int, reason: str, applies_to: str, window_start: int, window_end: int,
                        evidence_url: str, by: str, change_id: int | None = None, old_coldkey: str | None = None,
                        new_coldkey: str | None = None, note: str | None = None) -> ActionResult
async def retire_whitelist(db, whitelist_id: int, *, by: str) -> ActionResult
async def override_key_compromise(db, *, netuid: int, window_start: int, window_end: int, evidence_url: str, by: str,
                                  note: str | None = None) -> ActionResult

# review.py
async def enqueue(db, *, occupant_key: str | None, kind: str, dedupe_base: str | None = None,
                  ref_table: str | None = None, ref_id: int | None = None, priority: int = 2,
                  suggested_flag: str | None = None, summary: str | None = None, payload: dict | None = None,
                  evidence_url: str | None = None, event_type: str | None = None, materiality: int | None = None,
                  post_id: str | None = None, label_version: str | None = None) -> int | None
    # kind ∈ 03's CHECK list; 11 kinds need dedupe_base (merge into the open row, else insert with
    # dedupe_key = f"{dedupe_base}|{created_at}"); kind='llm_flag_candidate' needs post_id + label_version
    # (10 §3.2; dedupe_key = f"llm_flag_candidate|{post_id}|{label_version}", INSERT OR IGNORE, None if ignored)
async def list_open(db, *, kind: str | None = None, occupant_key: str | None = None, limit: int = 100) -> list[dict]
async def resolve_review(db, review_id: int, action: str, *, by: str, note: str | None, evidence_url: str | None = None,
                         whitelist_reason: str | None = None, window: tuple[int, int] | None = None) -> ActionResult
async def auto_clear(db, now: datetime) -> int

# jobs (CollectorJob subclasses; collect() returns JobOutcome(rows, cost_usd=0.0, note)); every class attribute
# (id, cron, timeout_sec, misfire_grace_sec, enabled_flag, provider, paid, liveness_max_age_sec) = its row of the §6 job table
class OwnerCollectJob(CollectorJob)         # owner_collect_daily
class OwnerWatchJob(CollectorJob)           # owner_watch_hourly
class TeamSeedJob(CollectorJob)             # team_seed_daily
class IntegrityFeaturesJob(CollectorJob)    # integrity_features
class OwnerRpcSeedJob(CollectorJob)         # owner_rpc_seed
```

`OwnerCollectJob.collect(run)` order: (1) pauses (`control_state` rows for the job or its `tmc`/`integrity` group, emergency `.paused` files, `PAUSE_ALL`) are checked by 01's wrapper before `collect()`; compute `soft_deadline = min(run.started_at + timeout_sec − 900, 23:15Z)` and the request budget `OWNER_TMC_MAX_PER_RUN`; (2) load `active_occupants(include_pre_launch=True)` minus netuid 0; (3) C6 Rufus and C7 subnetradar once; (4) per occupant C1 (or list) → keys; group occupants by `owner_ck`; (5) per coldkey (skipped once `now > soft_deadline`): C2, C4 (watermarked; D4b `owner_extraction_detected(reasons=['full_exit_extrinsic'])` for newly inserted `sell_full` rows; back-fill pages only within the remaining budget), optional C8 delegation on a concurrent TaoStats task; (6) per occupant: C3 (skipped once the budget is spent), compose `owner_snapshots` row (C2 rows for n, chain_hourly copy, Rufus fields), `INSERT`; D1/D2/D6/D7 vs the previous snapshot; D3 via Rufus perpetual flag; reconciliation → C5; (7) D8 subnetradar miss check; (8) `supersede_duplicates`; (9) `job_state` watermarks committed per coldkey in the same transaction as its events; (10) `JobOutcome(rows=snapshots + events inserted, note=f"tmc={n_req} truncated={n_trunc}")`.

`IntegrityFeaturesJob.collect(run)`: `decision_time = run.started_at`; `as_of` = 00:00Z for the official run, else the slot hour (provisional iff `as_of % 86400 ≠ 0`, 02 §B3); for each non-deregistered occupant (netuid ≠ 0) compute the 7 P4 rows + 2 trigger rows + 6 evidence rows (§2.2.6, §2.5–§2.11, §3.3) reading only rows `observed_at ≤ decision_time`; D2 pairing and D4 extraction detection; `team_reverify` enqueue, `review.auto_clear` and `sync_confirmed_llm_flags` (official run only); `put_metric_values` in one transaction per occupant; `owner_data_degraded` check. It never writes `stage='norm'` rows.

### 8.3 Step order

1. Check that 03's migrations contain every §3.1 table and column (03 owns the DDL, 02 §B7; this spec adds none) and that 03 §3.8/§3.9 list this spec's metric keys (§3.3) and event types (§3.4); open deltas are §11 Q2.
2. `team_parse.py` + fixtures/tests (§9 T1–T7) — pure, no DB.
3. `team.py` domain functions + `scripts/manual_entry.py team …` + tests T8–T12.
4. `team_seed.py` (subnetalpha + legacy) + T13–T15; register `team_seed_daily`.
5. `scripts/owner_probe.py`; **run it on the laptop** and commit `data/owner_field_map.json` (resolves every † and UNVERIFIED shape in §2.3).
6. `owner_sources.py`, `classify.py` (+ the `app/whale/events.py` port), `owner_collect.py`; register `owner_collect_daily`; T16–T20, T33–T37.
7. `owner_metrics.py` pure functions + T21–T31.
8. `whitelist.py`, `review.py`, `owner_admin.py`, `detectors.py`; `owner_watch.py`; register `owner_watch_hourly`; T24–T28, T38.
9. `constants.py` + `features.py` (final P4 values, raw `tenure` days, trigger and evidence rows — §3.3); register `integrity_features`; T39–T41, T43–T44.
9b. `owner_history.py` (spec 17 `rpc_owner_history` seed); register `owner_rpc_seed`; T45.
10. C8 TaoStats path (dormant without a key) + T42.
11. `/health` block, `ProviderLimiter` entries `rufus` and `subnetradar`, spec-14 hooks (new routes/commands call §2.12 functions).
12. Verify checklist §10 on the laptop.

### 8.4 Port verbatim (file:line) vs rewrite

| old asset | action |
|---|---|
| `E:\SN_Trader\SubnetTrader\app\strategy\quality.py` line 33 (`_ANON_TOKENS`) and lines 108-110 (`_looks_anonymous`) (verified 2026-09-23) | copy verbatim into `app/features/text.py` (shared with spec 12) |
| `quality.py` lines 137-147 (`team_attributed`: blob empty → None, anonymous → 0.0, owner → 1.0, team text → 0.7) | **do not port** — replaced by the evidence-based map §2.2.6 (kept as the reference for the None/0.0 semantics) |
| `app\whale\events.py` lines 1-170 (`DELEGATION_PATH`, `PAGE_LIMIT`, `DTAO_LAUNCH_TS` 20-22; `ss58_of` 27; `rao_to_tao` 42; `opt_float` 53; `parse_ts` 60; `_ACTION_MAP` 80; `classify_event` 83-132; `build_delegation_params` 135-158; `parse_delegation_response` 161-170) | copy verbatim to `app/whale/events.py` (stdlib-only); owner code calls `classify_event` and maps `BUY/SELL` into §2.4 kinds |
| `app\backtest\whale_impact.py` lines 158-178 (`move_groups` set 160-167: non-transfer rows, `netuid > 0`, same coldkey/netuid/block with both BUY and SELL = phantom move pair; dropped at 175-177) | re-express as `classify.mark_phantom_moves` (no SQL dependency; `is_transfer` NULL counts as non-transfer) |
| `app\whale\collector.py` lines 36-38 and 91-121 (overlap watermark, never-raise, `_status`, alert on 5th failure) | pattern only — superseded by spec 01's wrapper; `OWNER_ACTIVITY_OVERLAP_SEC` mirrors `OVERLAP_SEC` |
| `data\whale_labels.json` `_README` lines 2-11 (POLICY sentence lines 8-9: two-independent-sources) | policy → `check_two_source` docstring + §2.2.2 |
| `research\subnet_edge\*.md` (template in research 01-whale-research-ops §2) | legacy team seed input (§2.2.4); verdicts are not imported |
| `app\data\fundamentals_collect.py` lines 388-393 (`write_atomic`) | reuse (spec 04 ports it) for `data/owner_field_map.json` |

---

## 9. Tests

All unit tests run offline (`httpx.MockTransport`, in-memory `Database(":memory:")` with spec 03's migrations, injectable clock). Fixtures live in `tests/integrity/fixtures/`. Where a test states a P4 score ("S2 = 0.926286", "team_doxxed = 0.6"), it asserts the row's `value` — the final score, which spec 12's `prenorm` passes through unchanged; agreement with spec 12's reference functions on the shared anchors is T43 (a local copy of those functions is used until spec 12 lands).

**Team (pure + DB)**
- T1 `test_parse_strong_pairs` — Apex-style HTML (`<p><strong>Will Squires</strong> – CEO <a href="https://www.linkedin.com/in/willsquires/">LinkedIn</a></p>` × 4) → 4 candidates, roles set, LinkedIn attached, `profile_kind='linkedin'`, disclosure `named`.
- T2 `test_parse_role_first` — `<strong>Founder:</strong> Jane Doe, <strong>CTO:</strong> John Roe` → names Jane Doe/John Roe with roles.
- T3 `test_parse_llm_prose_headings` — `<strong data-start="12" class="text-token-text-primary">Public Team Members and Handles:</strong>` and `<strong>GitHub Contributors and Activity:</strong>` produce no candidates; an X link `https://x.com/jondurbin` inside yields candidate `jondurbin`.
- T4 `test_parse_kept_private` — `<p>The core team behind ORO is kept private.</p>` → `anonymous`; with no members `team_doxxed = 0.0`.
- T5 `test_parse_placeholders` — `Awaiting Data...`, `Awating Data...`, `` (empty) → `placeholder`/`empty` → `team_doxxed = None`; `No team members have been publicly disclosed` → `anonymous` → 0.0.
- T6 `test_parse_anon_length_gate` — 1,500-char prose containing "none of the …" and "unknown" plus 3 strong names → `named`, `anon_marker=false`; a 30-char "Team: anonymous" → `anonymous`.
- T7 `test_two_source_rule` — table: (LinkedIn, same LinkedIn) fail 1 domain; (LinkedIn, personal X) fail no attestation; (LinkedIn, `https://macrocosmos.ai/team`) pass, `evidence_count=2`; (LinkedIn, `https://subnetalpha.ai/subnet/apex/`) fail (excluded); (profile None, evidence `https://chutes.ai/about` + `https://www.theblock.co/post/1`) pass; subnet's own X account post counts as attestation when its handle is in `x_handles`; (LinkedIn, no URL, attestation note) pass, `evidence_count=2`, `evidence_json` holds a `url: null, class: 'attestation'` item; (no profile, no URL, note only) fail (no public URL).
- T8 `test_state_machine` — add → `verify_member` with one same-domain URL → `two_source_rule`, still candidate; with an attestation → verified, `verified_at` set, `team_member_verified` event with `checked_at`; re-verify 100 d later → `verified_at` unchanged, `last_checked_at` updated; reject → `rejected`; direct SQL `UPDATE … status='verified'` with `evidence_count=1` violates the CHECK.
- T9 `test_team_doxxed_map` — 2 verified → `value = 1.0` (`ok`, `value_json.verified = 2`); 1 → 0.6; candidates only → 0.3 (`verified = 0`, `candidates ≥ 1`); one verified 200 d ago (stale) and nothing else → 0.3; manual `anon` → 0.0 (`status='absent'`, `reason='anonymous'`); manual `anon` then `clear` with placeholder text → None (`status='failed'`, `reason='placeholder'`); all candidates rejected → None (`all_rejected`); the evidence rows `team_verified_count`/`team_candidate_count` equal V/C in every case.
- T10 `test_team_pit_replay` — verified at T+1 d: `compute_team_doxxed(t=T)` → 0.3, `(t=T+2 d)` → 0.6; rejection at T+5 d replays correctly.
- T11 `test_seed_idempotent_no_resurrection` — two seed runs → no duplicate rows; a rejected name stays rejected with `source_last_seen_at` bumped; exactly one open `team_candidate` review per occupant with merged payload; after that item expires, a later new candidate opens a second row under the same `dedupe_base` (new `dedupe_key` with its own `created_at`; 03's unique index covers open rows only), while a second **open** row with the same `dedupe_key` is refused by the index.
- T12 `test_domain_errors` — unknown netuid → `no_current_occupant`; `verify_member` without URLs → `evidence_required`; `ftp://` → `invalid_url`; duplicate add → `duplicate_member` with existing id; `add_member(verify=True)` failing the rule → ok, `status='candidate'`, `warning='two_source_rule'`.
- T13 `test_seed_stale_for_occupant` — SN40 team_info (Chunking post) with `modified_gmt` before the Ralph occupant's `reg_ts(O) − 2 d` (registration 8,409,860; SN27 kept its registration block, so it is no re-occupancy example) → no candidates; `team_doxxed = None` reason `stale_for_occupant`.
- T14 `test_seed_delisted` — verified subnetalpha member missing from a new `named` parse → `team_member_delisted` + `team_delisted` review; status unchanged.
- T15 `test_legacy_seed` — over copies of the 25 real `sn*.md` files: exactly one candidate (`Const`, role founder, `source='legacy'`, `source_ref='sn120_affine.md:14'`); `sn67_harnyx.md` ("Anonymous team" on lines 14 and 22, capital A) skipped when the SN67 occupant's `reg_ts(O) > TEAM_LEGACY_AS_OF`, otherwise exactly **one** `team_disclosure` note (`#L14`); second run imports nothing (`legacy_done`).

**Owner acquisition**
- T16 `test_classify_stake_event` — table-driven over §2.4 (incl. `success=0` never counted, proxy signer counted, `transfer_stake` to self across netuids → `swap_out`).
- T17 `test_phantom_move_pairs` — TaoStats BUY+SELL same coldkey/netuid/block → both `move_internal`, excluded from outflow.
- T18 `test_activity_watermark_resume` — 3 pages with `OWNER_ACTIVITY_MAX_PAGES=2` → truncated, watermark at the last received row; next run fetches page 3 only (overlap duplicates absorbed); no gaps.
- T19 `test_source_supersede_pit` — TMC row at t0, TaoStats duplicate observed t1 → at `t < t1` the TMC row counts, at `t ≥ t1` only the TaoStats row counts.
- T20 `test_reconcile_trigger` — position drop of 5,000 α with no events → C5 called once, `owner_position_unexplained` emitted, S1 unchanged.

**Owner metrics (the brief's scenarios)**
- T21 `test_s2_chutes_sn64` — Rufus `auto_lock_enabled=true`, `owner.locked_perp=614,000`, `owner.pct=0.225`; C2 total 700,000 → `lock_ratio = 0.877143`, S2 = 0.6·0.877143 + 0.25 + 0.15 = **0.926286** (±1e-6).
- T22 `test_s2_mvtrx_sn79` — perp 1,269,236.8, total 1,300,000, auto false, `pct = 0.508` → `lock_ratio = 0.976336`, S2 = 0.585802 + 0 + 0.15 = **0.735802**.
- T23 `test_s2_zero_rule_and_grace` — perp 0, auto false, age 200 d → 0.0; age 40 d → None (`status='not_applicable'`, `reason='grace_period'`; the bare formula would give 0.0); auto unknown, perp 50k, total 100k, pct 0.1 → (0.3 + 0.15)/0.75 = 0.6; auto and `commit_pct` unknown, perp 10, total 20, age 20 → 0.5 (renormalised over `lock_ratio` only; = 12's anchor `owner_commitment(.5, None, None, 10, 20)`); `owner_cut_enabled=false`, perp 0, total 0, age 200 → auto := 1 → (0 + 0.25 + 0)/1.0 = 0.25.
- T24 `test_s1_covenant_dump` — `owner_cut_per_block = 0.1 α` hourly for 30 d → earned 21,600 α; events: `remove_stake` 60,000 α → 5,000 τ (day −3), `unstake_all_alpha` 90,000 α → 7,000 τ (day −2); `tao_liquidity` 300,000 τ → `impact7 = 0.04` → H1, H2, H3 (`sell_ratio = 6.944`) → `value = 0.0` (`ok`; `value_json.sell_ratio = 6.944`, `max_impact7 = 0.04`, `hard = [full_exit_extrinsic, sell_impact_7d, sell_ratio]`) and `owner_extraction_trigger = 1` with the same `reasons`; at ingest D4b writes the `extraction_detected` change, event `owner_extraction_detected` (critical, `reasons=['full_exit_extrinsic']`) and an `owner_extraction_candidate` review; the D4 hit at the next features run adds no second event in that ISO week; S2 = 0.0 (no lock, auto off, age > 90 d).
- T25 `test_s1_partial_and_disabled` — earned 21,600, `sell` 5,400 α → 0.75; cut disabled all window (`earned = 0`): no sells → 1.0; 0.5 α sold → 1.0 (H3 not reached because 0.5 ≤ `OWNER_MIN_EARNED_ALPHA`, then the shortcut); one `unstake_all_alpha` → 0.0 (hard trigger before the shortcut, 02 §C1; = 12's anchor `owner_retention(.5, None, True, False) = 0`) and `owner_extraction_trigger = 1`; 500 α sold by `remove_stake` → H3 → 0.0; coverage starting after W.start with no hard trigger → None (`failed`/`activity_coverage`); the same with an `unstake_all_alpha` inside the covered part → 0.0 (`ok`); earned 0.5 α (cut on) with 3 α sold → `sell_ratio = 3.0` (floor `OWNER_MIN_EARNED_ALPHA`) → 0.0.
- T26 `test_oro_compromise_override` — SN15 fixture: 147,000 α sold via 30 `remove_stake` rows over 10 h (Σ 6,000 τ vs `tao_liquidity` 150,000 τ → impact 4 %), coldkey swap 8 d later (old coldkey emptied → `coldkey_swap`). Before the override: S1 = 0 (hard), `owner_extraction_trigger = 1`, `owner_control_change_trigger = 1`, S3 = 1 − 0.6 = 0.4 on the swap day. Override created at T with window [dump − 1 h, swap + 1 d] (9 d ≤ 45): for `t ≥ T` → S1 = 1.0 (no remaining sells), S3 = 1.0 (`events[].whitelisted = true`), both triggers 0, `manual_flags(key_compromise, confirmed)` appended plus a `cleared` row for each of `owner_extraction`/`owner_control_change` that the fixture's `red_flag_state` has active (an inactive one → no row, `warning='flag_not_active:<flag>'`), open owner reviews resolved (`status='confirmed'`, `resolution_action='key_compromise'`), event `owner_key_compromise_override`; recomputing `t < T` yields the pre-override values (PIT); a 60-day window → `window_too_long`.
- T27 `test_perpetual_off_detection` — hourly perp 500,000 → 20,000 and decay 0 → 480,000 → `perpetual_off`, event `owner_perpetual_off` (dedupe `occ|perpoff|<day>`), S3 = 0.0, `owner_extraction_trigger = 1` (`reasons=['perpetual_off']`) for 90 d, then 0; the same drop across an `sse` → `analytics_1w` row pair with `perp = locked − decaying` detected identically, and a pair straddling unverified units skipped; a decaying-only owner at first observation (SN36 Epago pattern, 58k decaying) → no event; decay +10,000 with perp unchanged → `decaying_lock_added` only; perp −10,000 with decay 0 → `lock_reduced` + an `owner_lock_reduced` review.
- T28 `test_repoint_identity_churn_decay` — repoint at d0, chain `identity_changed(github_repo)` at d10 → pair row (event_at d10, base 0.4): S3(d10) = 0.6, S3(d55) = 1 − 0.4·0.5 = 0.8; identity change at d20 → no pair; a coldkey swap paired (≤ 14 d) with a re-point or that identity change → `owner_control_change_trigger = 1` with `identity_churn = true` for 30 d, then 0 (the flag stays until a manual clear, 02 §D5); an unpaired swap → 1 with `identity_churn = false` for 30 d; 04's `identity_changed(field='owner_coldkey')` for the swap itself never sets `identity_churn`.
- T29 `test_s3_decay_whitelist_pit` — swap at −45 d → 0.7; at −91 d → 1.0; whitelist (`kind='multisig_migration'`, window covering the swap) created at T → 1.0 for `t ≥ T`, 0.7 for `t < T`; retired whitelist no longer applies after `retired_at`; a `perpetual_off` whitelist with `kind='multisig_migration'` is rejected by 03's CHECK.
- T30 `test_s5_overhang` — avail 100,000 α, price 0.05, tao_in 100,000 → 0.05 → 1.0; 0.25 → 0.0; 0.15 → 0.5; registered 2026-03-01 (pre-cutoff, post-dTAO) → 0.3 at 0.15; `initial_alpha_accounted` whitelist → 0.5; registered 2026-06-01 → no penalty; tao_in 0 → None.
- T31 `test_owner_identity_and_tenure` — present name + url → 1.0 (`ok`); name only → 0.0 (`absent`); `absent` → 0.0; `unknown` → None (`failed`); key mismatch → None; `tenure_days(9_123_662, 8_693_284)` = 59.77 d (SN86's registration block, research 02-keyless §2), written by `integrity_features` as the `tenure` row (`value ≈ 59.77`, `ok`); registration block unknown → `failed`/`no_registration_block`; |Δ| against 05's `occupant_age_days` ≤ 0.1 d.

**Detectors, sources, jobs**
- T32 `test_reoccupancy_isolation` — netuid 92 re-occupied (7,013,758 → 8,352,006, 04 §2.2; not SN27, which kept its registration block 1,727,132) with a new owner coldkey → no D1 change for the new occupant; old occupant rows untouched.
- T33 `test_rufus_parser_tolerant` — list-shaped and dict-shaped fixtures parse to the same `RufusSubnet`; `registration_block` mismatch → ignored for the occupant; `baked_at` 72 h old → `rufus_stale=1`, S2 falls back to TMC fields.
- T34 `test_subnetradar_parser` — HTML fixture with 3 swap rows → 3 dicts; garbage HTML → `[]`, no exception.
- T35 `test_coldkey_change_classification` — TaoStats `is_coldkey_swap=true` wins; else the new coldkey in the previous snapshot's `raw_json.rufus_challenger_coldkeys` → `subnet_owner_changed`; else subnetradar row; else old coldkey still holding 60 % → `owner_transfer`; emptied → `coldkey_swap`; C2 failing → `coldkey_change_unclassified` (penalty still 0.6); each emits `owner_control_change` and opens one `owner_control_change` review.
- T36 `test_budget_and_pacing` — MockTransport counts calls for 128 occupants / 110 coldkeys: list-keys mode ≤ 390 TMC requests, detail mode ≤ 518, and never more than `OWNER_TMC_MAX_PER_RUN` even with first-run back-fill pending (back-fill pages and C3 yield); a fake clock shows ≤ `OWNER_TMC_MAX_PER_MIN` requests in any 60 s and none outside 20:30–23:30Z; every request went through `TmcClient` → `ProviderLimiter('tmc')` (limiter counter == transport counter).
- T37 `test_collect_failure_isolation` — C2 500 for one coldkey → its occupants get `c2="fail:…"`, others complete; ≥ 25 % failures → run `ok=0`; `data/control/tmc.paused` present → the wrapper skips the run, no request made (`paused:emergency_file`); a `control_state` pause → the wrapper skips the run; `BudgetExhausted` mid-run → `paused: budget` with the finished coldkeys' watermarks committed.
- T38 `test_review_resolution` — every enqueued `kind` passes 03's CHECK (`team_candidate`, `team_reverify`, `team_delisted`, `owner_extraction_candidate`, `owner_change_whitelist`, `owner_source_disagreement`, `llm_flag_candidate`), `team_candidates`/`owner_control_change` as a kind abort; `whitelist` action on an `owner_change_whitelist` item creates the row (`reason` = the given reason, `applies_to` = the item's class, default ±1 d window, `owner_change_id` = the item's `ref_id`); `confirm_flag` on an `owner_extraction_candidate` item writes `manual_flags(owner_extraction, confirmed)`; `llm_flag_candidate` rows via `enqueue(post_id=…, label_version=…)`: a second enqueue of the same label returns None; `confirm_flag` with `suggested_flag='key_compromise'` writes `manual_flags(key_compromise, confirmed)` + `confirm_review`; `suggested_flag='security_incident'`: `confirm_flag` → `invalid_transition`, `done` → `resolved` + `confirm_review`, no `manual_flags` row; `suggested_flag` NULL: only `dismiss` succeeds; `dismiss` → `rejected`, triggers unchanged; `done` on a `team_delisted` item → `resolved`; `team_candidate` auto-clears (`expired`, `auto_cleared`) when the last candidate is verified/rejected; a spec-10 `confirm_review` done outside this module is mirrored into `manual_flags` by `sync_confirmed_llm_flags` exactly once.
- T39 `test_features_rows_and_pit` — `integrity_features` official run writes the 7 P4 keys + 2 trigger keys + 6 evidence keys per non-root occupant with `stage='raw'`, `source='integrity'`, `spec_version='ts-1.0'`, `as_of` 00:00Z and `value_json.status` on every row (03's generated `status` column equals it; no status column is written); provisional runs write slot-hour rows (`as_of % 86400 ≠ 0`) and never touch the official row; re-running the official slot keeps an `ok` row and replaces a `failed` one (02 §B4); no `stage='norm'` rows; a snapshot observed after `decision_time` is ignored; a 40-h-old snapshot → owner rows `failed`.
- T40 `test_absence_vs_failure` — table-driven over §4 (each row asserts `0.0` vs `None`).
- T41 `test_catchup_backfilled` — service down 2 days → catch-up writes the two missing official `as_of` rows with `value_json.backfilled=true`, using only rows observed ≤ each decision time.
- T42 `test_taostats_path_dormant` — no key → zero TaoStats requests; with a key + `KILL_SWITCH` → zero; with a key → delegation rows parsed through `classify_event`, supersede TMC duplicates; phantom legs with `is_transfer = NULL` paired; an `announce_coldkey_swap` extrinsic → `coldkey_swap_announced` + `owner_coldkey_swap_announced`.
- T43 `test_spec12_contract` — for every fixture occupant of T21–T31: each of the six score rows has `value ∈ [0,1]` or NULL and `prenorm(value) == value`; `value_json` carries spec 12 §2.4's input keys for rows 27, 29–32 (plus evidence); on 12's shared anchors (12 §9 `test_normalise`: `owner_commitment(.85, False, .225, 1e5, 600) = .66`, `(0, False, 0, 0, 300) = 0`, `(.5, None, None, 10, 20) = .5`; `owner_retention(.1, .001, False, True) = .9`, `(1.5, None, False, True) = 0`, `(.1, .03, False, True) = 0`, `(.5, None, False, False) = 1`, `(.5, None, True, False) = 0`; `control_stability` swap at 45 d = .7, lock disabled = 0; `owner_overhang(.03) = 1`, `(.18) = .35`, `(.10, legacy) = .55`) this spec's functions give the same values on equivalent inputs (±1e-9); every row has `value_json.status` and a `reason` when not `ok`; the aux keys are exactly `owner_extraction_trigger`, `owner_control_change_trigger`, the evidence keys exactly 02 §C2's six, and no retired key is ever written.
- T44 `test_constants` — `app/integrity/constants.py` equals the constants hard-coded in spec 12's reference functions (0.02, 0.05, 0.05/0.25, 0.2, 0.6/0.4, 45, 90) and the trigger windows 30/30/90 (02 §C2); no `OWNER_*` env key shadows them.
- T45 `test_rpc_owner_history_seed` — spec 17 fixture rows (`change='owner_coldkey'` at block B, `change='owner_hotkey'` at B′) → one `coldkey_change_unclassified` and one `hotkey_repoint` `owner_changes` row with `detected_via='rpc_owner_history'`, `event_at` from the row, `observed_at` = import time and `evidence_json.backfilled = true` (PIT: invisible to strict decisions before the import; `compute_control_stability_90d(db, O, as_of=D, mode='reconstructed')` for a D between `event_at` and the import sees them by `event_at` and returns the penalised S3); re-run inserts nothing; changes before the occupant's `network_registered_at` are ignored; the seeded changes' events carry `payload.backfilled = true` with `alerted_at` set.

**Live (`@pytest.mark.live`, skipped in CI):** `owner_probe` against netuids 64, 79, 3, 15 (≈ 20 TMC calls): owner keys present for all four; SN79 perpetual owner lock ≈ 1.27 M α; a staking-activity page for SN64's owner coldkey parses without unknown `kind`s.

---

## 10. Verify checklist (Phase 7 is done when all pass on the laptop)

```bash
# 0. offline
pytest tests/integrity -q                                   # green
python -m compileall -q app scripts                          # clean
python -m app jobs | grep -E 'owner_collect_daily|owner_watch_hourly|team_seed_daily|integrity_features|owner_rpc_seed'
#   → 5 rows, available=yes, phase 7

# 1. probe (≈ 20 TMC calls, 2 Rufus, 1 subnetradar) — resolves every † / UNVERIFIED shape in §2.3
python -m scripts.owner_probe --netuids 64,79,3,15 --out data/owner_field_map.json
#   prints: C1 keys source list|detail, owner_coldkey/owner_hotkey field names, owner_cut_per_block units (α or rao),
#           C2 row keys + subnet-id key, C3 row keys, C4 page size / FROM-TO format (ms|s|iso) / sort / amount units,
#           C5 coldkey key, Rufus top-level shape + baked_at, subnetradar parse rows ≥ 1
#   expected: SN79 perpetual owner lock ≈ 1.27e6 α; SN64 auto_lock_enabled = true; commit data/owner_field_map.json

# 2. team seed
python -m app run-job team_seed_daily
sqlite3 data/taosentinel.db "select status, source, count(*) from team_members group by 1,2"
#   → candidate|subnetalpha ≈ 250–360 (354 strong names minus headings/duplicates), no verified rows
sqlite3 data/taosentinel.db "select count(*) from review_queue where kind='team_candidate' and status='open'"   # ≈ 60–75 (≤ 70 records use <strong> names)
python -m scripts.manual_entry team seed --legacy --dry-run  # lists exactly SN120 'Const' (founder) + at most one SN67 anonymity note (per guard)

# 3. two-source rule round trip (clean up afterwards)
python -m scripts.manual_entry team add 64 --name "Probe Person" --role CTO --profile https://www.linkedin.com/in/probe-person \
       --evidence https://www.linkedin.com/in/probe-person --verify --by rudolf
#   → {"ok": true, "status": "candidate", "warning": "two_source_rule", "id": N}
python -m scripts.manual_entry team verify N --evidence https://chutes.ai/ --by rudolf        # → status "verified", evidence_count 2
python -m scripts.manual_entry team reject N --reason "verify-checklist probe" --by rudolf    # → status "rejected"

# 4. owner collection (≈ 78 min at 5/min; ≤ 104 min at the 518-request cap, also on back-fill days)
python -m app run-job owner_collect_daily
curl -s localhost:8090/health | jq '.integrity.owner'
#   → snapshots_today ≈ 128, keys_missing ≤ 3, c2_fail/c4_fail ≤ 5, tmc_requests_today ≤ 390 (≤ 518 in detail mode or while back-filling)
sqlite3 data/taosentinel.db "select count(*), count(distinct owner_coldkey) from owner_snapshots where observed_at > strftime('%s','now')-7200"
#   → ≈ 128 | ≈ 100–128
sqlite3 data/taosentinel.db "select netuid, rufus_owner_perp, tmc_owner_perpetual, rufus_auto_lock, owner_total_alpha from owner_snapshots
                             where netuid in (64,79) order by observed_at desc limit 2"
#   → SN64 ≈ 6.1e5 α, auto 1; SN79 ≈ 1.27e6 α, auto 0 (2026-09-22 values; drift expected)
sqlite3 data/taosentinel.db "select source, kind, count(*) from owner_stake_events group by 1,2"   # tmc_staking_activity rows, kinds from §2.4 only

# 5. features
python -m app run-job integrity_features
sqlite3 data/taosentinel.db "select metric_key, count(*), sum(value is null) from metric_values
                             where source='integrity' and as_of=<today 00:00Z> group by 1"
#   → the 7 P4 keys (tenure in days) + 2 trigger keys + 6 evidence keys, ≈ 128 rows each; owner_retention_30d nulls fall to ≤ ~15 %
#     once every coldkey's 35-day coverage is complete   (<today 00:00Z> = strftime('%s','now','start of day'))
sqlite3 data/taosentinel.db "select occupant_key, metric_key, round(value,3), round(json_extract(value_json,'$.lock_ratio'),3), json_extract(value_json,'$.status')
                             from metric_values where source='integrity' and stage='raw'
                             and metric_key in ('owner_commitment','team_doxxed') and (occupant_key like '64:%' or occupant_key like '79:%' or occupant_key like '3:%') and as_of=<today 00:00Z>"
#   → owner_commitment value (final S2): SN64 ≥ 0.85; SN79 ≈ 0.70–0.75; SN3 = 0.0 (owner lock 0, auto off, age > 90 d)
#   → after spec 12 is live, the stage='norm' rows of the six scores equal `value` (prenorm) — T43

# 6. detectors (dry-run drill on a temp copy — never on the live DB)
sqlite3 data/taosentinel.db ".backup /tmp/ts-copy.db"
python -m app.integrity.detectors drill --db /tmp/ts-copy.db --netuid 64 --fake-coldkey-change
#   → prints D1 classification path, the owner_changes row, event owner_control_change (warning), an owner_change_whitelist review (event_type owner_control_change),
#     owner_control_change_trigger = 1
python -m scripts.manual_entry --db /tmp/ts-copy.db owner compromise 15 --from 2026-07-13T00:00:00Z --to 2026-07-22T00:00:00Z \
       --evidence https://protos.com/ai-firm-oro-says-north-korean-hacker-stole-600k-worth-of-crypto/ --by rudolf
#   → whitelist row (kind post_compromise_recovery), manual_flags(key_compromise, confirmed) (+ cleared rows only for flags active in the copy),
#     event owner_key_compromise_override (the ORO dump predates collection, so no owner_changes fall in the window — the drill only
#     exercises the write path)

# 7. hourly watch + budget after 24 h of normal running
curl -s localhost:8090/health | jq '.collectors.owner_watch_hourly.last_ok, .providers.tmc'
#   → last_ok < 70 min old; TMC requests_today ≈ 716 steady (≤ ~1,350 worst, 02 §G3); no hour above ≈ 311
sqlite3 data/taosentinel.db "select change_type, count(*) from owner_changes group by 1"      # only real transitions
sqlite3 data/taosentinel.db "select kind, status, count(*) from review_queue group by 1,2"

# 8. history seed + contract (after spec 17's backfill_rpc and spec 12 are in place)
python -m app run-job owner_rpc_seed
sqlite3 data/taosentinel.db "select change_type, count(*) from owner_changes where detected_via='rpc_owner_history' group by 1"   # a few per 90 d
pytest tests/integrity/test_spec12_contract.py -q            # final values == spec 12 reference anchors
```

---

## 11. Open questions / operator decisions

1. **`team_members` shape vs spec 14 (settled, 02 §H3 + 03 §3.2).** `status ∈ {candidate, verified, rejected}`, `source ∈ {subnetalpha, legacy, manual, llm}`, generated `verified`/`created_at` for 14; 14's `unconfirmed_candidates` bucket reads `status='candidate'` (old labels `seed_subnetalpha → subnetalpha`, `seed_research → legacy`), and 14 adds the §2.12 routes and commands.
2. **Remaining deltas with 03** (03 owns the DDL and both catalogs; its Appendix A adopted this spec's table shapes). (a) `owner_snapshots` has no `rufus_challenger_coldkeys_json` column → stored in `raw_json.rufus_challenger_coldkeys`; (b) `owner_changes` has no `extrinsic_id` column → `evidence_json.extrinsic_id`; (c) 03 §3.9 lacks `owner_coldkey_swap_announced` (02 §D1 requires it) and the ts-1.1 `owner_self_dealing_suspected`; (d) 03 §3.8 still lists `owner_lock_disabled_90d`/`owner_control_change_30d` as flag inputs, `owner_sell_ratio_30d` as evidence and `tenure` as `12 / features` — superseded by 02 §C1/§C2 (§3.3 is the list this spec writes); (e) 03 §3.9 marks `owner_control_change`, `owner_perpetual_off` and `owner_extraction_detected` alert=yes, which 02 §D4 makes digest-only; (f) `chain_rest_snapshots` is keyed by `endpoint`/`params` while 02 §G2 speaks of a `kind` (`subnets` ≡ `endpoint='/subnets/'`, `coldkey_identities` ≡ `endpoint='/accounts/coldkey-identities/'`), and its table comment names 05 as the only writer although 04 (02 §G2) and this spec (Rufus/subnetradar raw bodies) also write it; (g) `owner_changes`, `owner_snapshots` and `owner_stake_events` have no `backfilled` column, so the §2.1 replay marker lives in `evidence_json`/`raw_json.backfilled` until 03 adds the column (then `backfilled = 1` is read directly); 03's `team_members.evidence_count` comment says "distinct domains" — 02 §H3 makes it domains **plus** operator attestation notes (§2.2.2).
3. **Decaying owner alpha ≠ exit.** Research §5 S3 treats `owner_personal_decaying_alpha > 0` as the formal exit signal; live data has 16 decaying-only owners (e.g. SN36) that never had a perpetual lock. This spec fires `perpetual_off` only on a *transition* (perpetual → decaying, or an explicit `set_perpetual_lock(false)`), per master §7.3/§7.5's wording (`PerpetualLockUpdated{enabled:false}`). Confirm.
4. **S2 grace period.** Master's zero rule ("zero lock ∧ auto-lock off ∧ age > 90 d → 0") implies young subnets are not zeroed; the master *formula* alone gives such a subnet 0.0 anyway (lock_ratio 0, auto 0, commit 0), so the age clause only has meaning as a grace. Implemented as a `not_applicable` row (`reason='grace_period'`) → `None`, which lowers confidence rather than scoring 0. Alternative: drop the grace and write 0.0. Operator decision; spec 12 passes through whatever this spec writes.
5. **`owner_cut_enabled = false`.** S1 evaluates H1–H3 before the master's "→ 1.0" shortcut (02 §C1). Hours with the cut off earn a known 0, so with the cut disabled for the whole window H3 fires on any outflow above `OWNER_MIN_EARNED_ALPHA` (1 α): every principal sale scores 0 and raises `owner_extraction`, and the shortcut only rescues dust (T25). This follows the ledger's order and is recorded for the operator; a larger floor for cut-disabled windows would need a ledger change. S2: the auto-lock component counts as satisfied (written as `auto = true`, `auto_source='owner_cut_disabled'`).
6. **S2 `commit_pct` is binary** (≥ 5 % → .15) per master; a graded version (`zclip(commit_pct, 0, 0.05)`) would reduce cliff effects — ts-1.1 candidate.
7. **Pre-2026-05-08 −0.2 penalty** applies to every occupant registered between dTAO launch and the cutoff unless TaoStats shows a negligible estimate or the operator whitelists `initial_alpha_accounted`; keyless it is effectively a uniform −0.2 on S5 (≈ −0.01 of P4) for ~110 older subnets. Master-mandated; revisit with TaoStats data. The cutoff is a date (compared with `reg_ts(O)`), not a block number — the hotfix block (v3.3.15-402) should replace it once known.
8. **"Unannounced" coldkey swap** = any swap not whitelisted by the operator. On-chain every user swap is *announced* (`announce_coldkey_swap` → ≥ 5-day `ColdkeySwapAnnouncementDelay` → `swap_coldkey_announced`; only root's `swap_coldkey` skips it), so the master's "unannounced" can only mean "without public notice" (research 02-owner §3 item 2) — a human judgement. With a TaoStats key, D9 surfaces the on-chain announcement ≥ 5 days early so the operator can pre-whitelist a disclosed migration. `SubnetOwnerChanged` (conviction takeover) is classified `subnet_owner_changed` and counts for the flag with the same 0.6 S3 penalty — an interpretation (master lists it only under the flag). Expect ~2–4 review items/week (subnetradar listed 22 owner changes/registrations incl. SN112, SN117, SN47 on 2026-09-21/22; registrations of new occupants are not changes).
9. **UNVERIFIED shapes** resolved by the probe: `subnet_owner_hotkey`/`owner_cut_per_block`/hyperparameter names on REST `/subnets/{id}/`; units of owner fields; staking-activity row keys, `FROM`/`TO` format, page size, sort param and whether `set_perpetual_lock` appears there; stake-removed coldkey key and pagination depth (spec 13 open item 8 — history deeper than 35 d is forward-only until TaoStats or a backfill run proves depth); coldkey-stakes row keys; Rufus JSON nesting and `pct` scale; subnetradar structure; TaoStats event filter names (settled 2026-10-12: `/v1/events?pallet=SubtensorModule&name=NetworkRegistrationQueued`, §C8); live `UnlockRate`/`MaturityRate` (not needed by ts-1.0 formulas).
10. **Owner position scope.** S1/S5 use the owner **coldkey's** position in the subnet (all its hotkeys); community `to_owner` locks are never credited (SN24 pattern). A team-controlled second coldkey is invisible to v1 (S4 ts-1.1 `owner_cluster` is the path).
11. **Extraction latency.** Daily collection means ≤ 24 h to detect a sell-off (ORO drained 147k α in ~10 h). `OWNER_STREAM_ENABLED` (2nd SSE slot) would give minutes but competes with spec 05's optional blocks stream and is not registered in v1 (§2.3, 02 §E2). Operator choice for a later version.
12. **Metric names (settled, 02 §C2/§J2).** `identity_stability`, `owner_conviction` and `owner_retention` are retired; 13 uses the master P4 names (+ 05's aux `owner_locked_share` for the reconstructible lock history). Reconstructibility (via the §8.1 `compute_<metric_key>` entry points): `owner_commitment` partially (lock history only, from `chain_daily.owner_personal_locked_alpha` ≥ 2026-05-13, no auto-lock/total-stake history), `control_stability_90d` keylessly from spec 17's `rpc_owner_history` (owner coldkey/hotkey changes; no identity-churn pairing before spec 04's history; **confirmed** for replays: `mode='reconstructed'` reads the seeded changes by `event_at`, §2.1 — answers 17 §11 item 26, so 17 may drop the key from `forward_only`) or with TaoStats `/v1/subnets/owners`, `owner_retention_30d` only with TaoStats stake-event history or a proven staking-activity backfill depth, `tenure` fully; `team_doxxed`, `owner_identity` forward-only.
13. **Final values, not raw inputs (settled, 02 §C1).** The six P4 scores are written as final [0,1] values (12 scores them with `prenorm`/`binary`), with raw inputs in `value_json` under 12 §2.4's keys, and `tenure` as raw days; the earlier `value_json.s11` copy is gone (it would duplicate `value`). `perp` is an exact `0.0` when no perpetual lock exists (the zero rule compares exactly).
14. **Anonymous-token length gate** (`TEAM_ANON_MAX_CHARS = 200`) refines master's "team_info matches anonymous tokens → 0": the old `_looks_anonymous` matched incidental "none"/"unknown" in long prose. Explicit phrases ("kept private", "not publicly disclosed") still match at any length.
15. **Team verification workload.** Fully manual by policy (no profile fetching, two independent sources, 90-day re-verify). ~70 subnets have seeded candidates; until verified they score 0.3. The operator should prioritise tier A/B subnets (`team list --status candidate` ordered by composite in spec 14's digest).
16. **Alert routing (settled, 02 §D4).** `owner_extraction_detected`, `owner_perpetual_off` and `owner_control_change` are digest-only; the page is 12's `red_flag_raised`/`red_flag_cleared`; 04's `identity_changed(field='owner_coldkey')` is `info`/digest, so a swap pages once. Other owner/team types follow 03 §3.9's alert column.
17. **Job registry (02 §E1/§E2).** 01's `JOB_TABLE` mirrors the five rows of the §6 job table verbatim (cron triggers are standard, E1); 01's `ProviderLimiter` must add providers `rufus` (1.0/s, burst 1) and `subnetradar` (0.2/s, burst 1). `TMC_HOURLY_CAP` = 500 (02 §F2).
18. **`tenure` producer (settled, 02 §C1).** This spec writes `tenure` as raw days (§2.11); 05's `occupant_age_days` is 12's fallback key; 12 has no derived `tenure` row.
19. **LLM-confirmed extraction evidence.** Spec 10's `confirm_review` writes a confirmed event but no `manual_flags` row; this spec mirrors it into `manual_flags` (`sync_confirmed_llm_flags`, §2.10) because spec 12 reads human gates only from `manual_flags`. Spec 10/14 may instead write the flag directly — then the sync becomes a no-op.

---

## 12. Sources

- `E:\TaoData\specs\02-cross-cutting-conventions.md` (binding; applied: B1–B4, B7, B9, B12, C1, C2, C5, C6, D1–D5, E1–E4, F2, G1–G3, H3).
- `E:\TaoData\specs\00-MASTER-SPEC.md` §1.2–1.3, §2, §4, §5, §6, §7.2, §7.3 (P4 table and mechanics paragraph), §7.5, §7.7, §8, §10, §11, §12, §13; `INDEX.md`; `18-operator-checklist.md` (C3 `TAOSTATS_API_KEY`, D1 team entry, D4 flag confirm, D5 digest).
- Sibling specs: `01-repo-skeleton-runtime.md` (§2.4 dynamic settings, §2.6 `CollectorJob`, §2.7 job table, §2.12 limiter, §3 `job_state`/`collector_runs`, §5 TaoStats keys); `04-subnet-registry-and-identity.md` (§2.2 occupant lifecycle and `subnet_owner`, §2.4 subnetalpha ACF incl. `team_info → team_info_html`, §2.5 coldkey identities, §2.7 rule R4, §2.10 needs-review, §2.11 legacy guard, §3.1 DDL, §3.3 read API, §3.5 `identity_changed`); `05-chain-collector.md` (§2 `TmcClient`, §2.9 owner fields, §3.1 `chain_hourly`/`chain_rest_snapshots`, §5 `TMC_*`/`SSE_*`, §6 TMC budget table, §8 `TmcClient` interface); `10-llm-classification.md` (§3.2 `review_queue`, §8.2 `confirm_review`); `14-api-telegram-and-ui.md` (§2.3 mutation flow, §2.4 routing, §3.1 `team_members`/`manual_flags`/`manual_notes`, §3.10 needs-review buckets, §8.4 Telegram commands); `03-storage-schema.md` (§2.1 conventions, §2.5 write disciplines, §3.2 and §3.5 DDL of `team_members`/`manual_notes`/`manual_flags`/`review_queue`/`owner_event_whitelist`/`owner_*`, §3.6 `metric_values`/`events`, §3.7 views, §3.8–§3.9 catalogs, Appendix A renames); `12-feature-store-and-scoring-engine.md` (§2.3 raw-row contract and `MetricResult`, §2.4 registry rows 27–33 and 61–62, §2.5 `prenorm`/`tenure` and the P4 reference functions, §2.8 flag state machine, §9 `test_normalise` anchors, §8 `app/features/text.py`); `17-data-migration-and-backfill.md` (`rpc_owner_history` DDL and semantics).
- Research: `E:\TaoData\research\02-gapfill-owner-extraction-mechanics.md` (all: owner-cut mechanics, locks/auto-lock opt-in 12/121, `set_perpetual_lock`, events, TMC/TaoStats/Rufus/subnetradar observability, 2026 cases Covenant/ORO/Connito/Quasar/Tenex, S1–S5 proposal); `02-gapfill-keyless-onchain-pillar-source.md` (§0 limits, §1 endpoint enumeration incl. `coldkey-stakes`, `analytics/neuron`, `staking-activity`, `stake-removed`, `coldkey-identities`; §2 owner lock facts 61/128, SN79/SN64; §6 bucket plan); `02-gapfill-data-continuity-and-backfill.md` (`/subnets/{id}/` carries owner keys and hyperparams); `01-web-subnetalpha.md` §1 (`team_info` structure: 70/128 `<strong>` names, 16 LinkedIn, placeholders, LLM prose); `01-fundamentals-scorer.md` §5 (`team_attributed`, `_ANON_TOKENS`); `01-whale-research-ops.md` §1 (Taostats delegation feed, phantom move pairs, two-source attribution policy), §2 (subnet_edge template and qualitative signals); `01-web-ecosystem-2026.md` §2 (collapses: Covenant, Quasar, ORO/Connito), §5 (red-flag taxonomy).
- Old bot (line numbers re-checked 2026-09-23): `E:\SN_Trader\SubnetTrader\app\strategy\quality.py` line 33 (`_ANON_TOKENS`), 108-110 (`_looks_anonymous`), 137-147 (`team_attributed`); `app\whale\events.py` lines 20-22, 27, 42, 53, 60, 80, 83-132, 135-158, 161-170; `app\whale\collector.py` 36-38, 91-121; `app\backtest\whale_impact.py` 158-178; `app\data\fundamentals_collect.py` 388-393; `data\whale_labels.json` lines 2-11; `research\subnet_edge\README.md`, `sn64_chutes.md`, `sn120_affine.md` (line 14), `sn67_harnyx.md` (lines 14, 22), `sn88_investing.md` (line 26).
- External (UNVERIFIED shapes noted inline): `https://api.taomarketcap.com/public/v1` (OAS `https://api.taomarketcap.com/api/public-oas3.json`); `https://s3.hippius.com/rufus/public/conviction-table/data/table.json`, `…/data/baked-at.json`; `https://subnetradar.com/swaps`; `https://api.taostats.io` (old routes `/api/delegation/v1`, `/api/subnet/owner/v1`, `/api/extrinsic/v1`, `/api/event/v1`, `/api/conviction/latest/v1`, now `/v1/subnets/stake-events`, `/v1/subnets/owners`, `/v1/extrinsics`, `/v1/events`, `/v1/subnets/conviction` — migration guide `https://taostats.io/blog/api-migration-guide.md`, probed 2026-10-12); subtensor code `https://github.com/RaoFoundation/subtensor` (`staking/lock.rs`, `coinbase/run_coinbase.rs`, PR #2695).

## Changelog
- 2026-09-23 reconciliation:
  - C1: the six P4 scores are written as final [0,1] `value` (12: `prenorm`/`binary`), inputs under 12 §2.4's `value_json` keys; `value_json.s11` dropped; T43 checks 12's reference anchors.
  - C1: S1 evaluates H1–H3 (incl. `sell_ratio > 1`) before the cut-disabled shortcut; a hard trigger writes 0.0 `ok`; cut-off hours earn a known 0 (§11 Q5).
  - C1: `tenure` is written here as raw days (`failed`/`no_registration_block`); 05's `occupant_age_days` is 12's fallback.
  - C2/D5: triggers renamed to `owner_extraction_trigger` (hard 30 d, `perpetual_off` 90 d) and `owner_control_change_trigger` (30 d; `identity_churn` → sticky), with `trigger_ids[]`; evidence keys = C2's six; retired names never written.
  - D1/D2/B2/B9: events use 03's names, payloads and dedupe (`owner_control_change`, `owner_perpetual_off`, `owner_extraction_detected` with `reasons[]`, `owner_cut_toggled`, `owner_hotkey_repointed` info), `source='integrity'`; §3.4 is the final owner/team list (+ `owner_coldkey_swap_announced`).
  - D4: owner detection events are digest-only; the page is `red_flag_raised`.
  - D5/H3: manual-flag semantics; the override clears only active flags; `verified` rows carry `evidence_url`, `verified_at`, `verified_by`.
  - B7/B1/B3/B4: §3.1 DDL replaced by a column contract on 03's tables (review kinds, open-only `dedupe_key`, `source_table/source_id`, whitelist `kind`/`change_id`, `rpc_owner_history`); status only in `value_json`; no `is_provisional`; first success wins; `reg_ts` INTEGER.
  - G1/G2/G3/F2: `TmcClient` only (no priorities, `BudgetExhausted`); C1 and identities from 04's snapshots; ≈ 390/day, ≤ 518 per run incl. back-fill, ≤ 5/min in 20:30–23:30Z; totals 716/day, 1.35 k worst, 311/h peak.
  - E1–E4/B12: one job table for 01 (five ids; `owner_collect_daily` pinned 20:30Z, stop 23:30Z, window-only catch-up); pauses in `control_state`; optional stream not registered.
  - C6: P4 freshness 2 d (`team_doxxed` 90 d).
- 2026-09-24 final pass: H3 — `evidence_count` = distinct non-excluded URL domains **plus** operator attestation notes (a note may be the second item, never the only public source); review kinds and columns now exactly 03's DDL (`team_candidate`, `owner_extraction_candidate`, `owner_change_whitelist` + `event_type`, `ref_table`/`ref_id`, `dedupe_base`), whitelist columns `reason`/`owner_change_id`.
- 2026-09-24 final pass: D5 — `confirm_flag` only for `owner_extraction`/`key_compromise` candidates; `security_incident` candidates are acknowledge (`done` → 10's `confirm_review`, no flag) or dismiss; NULL candidates dismiss-only; `enqueue` takes 10's `post_id`/`label_version`/`event_type`/`materiality` so 10 inserts no rows itself.
- 2026-09-24 final pass: J1/E1 — replay (`mode='reconstructed'`) reads backfilled `owner_changes`/`owner_snapshots`/`owner_stake_events` by `event_at`; `python -m app ops rearm owner_rpc_seed` deletes `rpc_seed_done`; `owner_collect_daily` catch-up = `startup_catchup` missed-fire rule; `TMC_SSE_BLOCKS_STREAM` replaces `SSE_BLOCKS_STREAM_ENABLED`, `TMC_PAUSED` dropped; T32 uses netuid 92, T13 SN40.
