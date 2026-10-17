# 18 — Operator checklist (what Rudolf must provide or do)

**Status:** `design-of-record` · **Date:** 2026-09-22 (updated 2026-09-23 per 02 §I1–I2) · Referenced by master spec §13.

Everything below is something the implementing model cannot do on its own. Items are grouped by *when* they are needed. Nothing in **Phase 0–2** needs a paid key — the first on-chain matrix runs keyless.

---

## A. Before the laptop build (Phase 0)

| # | Item | Why | How |
|---|---|---|---|
| A1 | **Ubuntu laptop ready**: Ubuntu 22.04+ (systemd ≥ 240), Python 3.12, git, **≥ 60 GB free disk required** (the DB reaches ≈ 9 GB in year 1; peak use during backup/VACUUM is ≈ 4.7 × DB + 5 GB; smaller disks only work if you lower `CHAIN_TICKS_RETENTION_DAYS`, `DB_BACKUP_MAX_TOTAL_GB` and the weekly backup count and add a USB/NAS mirror — 02 §I1), power settings set to *never sleep on lid close / AC*; system timezone **UTC**; BIOS/UEFI *power on when AC is restored* (vendor-specific); **no full-disk-encryption passphrase prompt at boot** (or TPM unlock) so the box recovers unattended after a power cut; auto-login not required. | 24/7 host | `sudo apt install python3.12 python3.12-venv git sqlite3`; `sudo systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target`; in `/etc/systemd/logind.conf` set `HandleLidSwitch=ignore`; `sudo timedatectl set-timezone UTC`. Full prep list in spec 16 §laptop preparation. |
| A2 | **Copy this folder** (`E:\TaoData\specs`, `E:\TaoData\research`) to the laptop at `~/taosentinel/specs` and `~/taosentinel/research`. | The implementer works from the specs. | USB or `scp`. |
| A3 | **Copy the old bot's seed data** to `~/taosentinel/seed/`: `E:\SN_Trader\SubnetTrader\data\ledger.db` (868 MB), `data\backups\ledger_backfill.db` (339 MB — put it directly in `seed/`, i.e. `~/taosentinel/seed/ledger_backfill.db`), `data\fundamentals.json`, `data\whale_cohort.json`, `data\whale_labels.json` (labels live under its `labels` key), `research\subnet_edge\*.md`, and the whole `E:\SN_Trader\SubnetTrader\app\` tree (read-only reference for porting). | Spec 17 imports 7.36M rows of pool history and the fundamentals snapshot; ported modules are copied from `app/`. | USB (≈1.3 GB). Do **not** copy `.env` into the repo; keep it outside (`~/taosentinel-secrets/old-pi.env`). |
| A4 | **Recover the Pi** (`pi@192.168.0.62`, password SSH). Run: `sudo systemctl stop subnettrader`, then copy `~/Desktop/SN_Bot/SubnetTrader/data/ledger.db` (+ `-wal`/`-shm`), `data/backups/*.db`, `data/logs/*.jsonl` from 2026-06-19 on, `data/backtest/**`, `.env`, any `VALUE_KILLED`/`KILL_SWITCH` files, `journalctl -u subnettrader --since 2026-06-19 > pi-journal.txt`, and `~/.bittensor/wallets/trader_wallet/` (keep offline!). Report: last `pool_history` timestamp, and whether the 10 OPEN 1-τ value positions (netuids 31, 47, 67, 94, 99, 108, 109, 113, 116, 122) were ever unstaked (give the coldkey SS58 so it can be checked keylessly via TMC `/accounts/coldkey-stakes/{coldkey}/`). | The Pi likely holds ~1.9M more 1-minute rows than the PC copy and may still hold alpha. | `ssh pi@192.168.0.62`; `sqlite3 data/ledger.db "select datetime(max(timestamp),'unixepoch') from pool_history"`. |
| A5 | **Decide the open decisions** in master §13 (X access, TaoStats, lite node, LLM budget, Pi, repo hosting, disk size, Discord crosspost text, pre-ledger backfill). Defaults if you say nothing: twitterapi.io deferred (null provider), TaoStats keyless, no node, LLM on at $60/mo cap, private GitHub repo with manual `git pull`, ≥ 60 GB disk, crosspost text kept 90 d, pre-ledger backfill on. | They gate Phases 7, 9, 12. | Reply in chat. |
| A6 | **Create the private GitHub repo** `taosentinel` (empty) — or say "local git only". | Version control + optional auto-update. | github.com → New repository → Private. |
| A7 | **Deploy credential** (only if you want the auto-update timer): a fine-grained PAT scoped to repo `taosentinel` with *Contents: read-only*, or a read-only deploy key. Store it in `~/taosentinel-secrets/deploy.env` (mode 600), **not** in the app's `.env`. Auto-update stays off until it exists. | Spec 16 auto-update gate. | github.com → Settings → Developer settings → Fine-grained tokens (or repo → Settings → Deploy keys). |
| A8 | **API token** before exposing the API/UI beyond the laptop (e.g. `tailscale serve`): `openssl rand -hex 32` → `API_AUTH_TOKEN=` in `.env` (and `NEXT_PUBLIC_API_TOKEN` for the UI build). Loopback-only use needs nothing. | Authenticated mutations (spec 14). | shell |

## B. Free keys and accounts (needed from Phase 4 onward)

| # | Item | Needed by | How to get it | Where it goes |
|---|---|---|---|---|
| B1 | **GitHub Personal Access Token** (fine-grained, *Public repositories (read-only)*, no other scopes, 1-year expiry). Check whether the old `GITHUB_PAT` in the Pi `.env` still works first. | Phase 4 (06) | github.com → Settings → Developer settings → Fine-grained tokens. | `GITHUB_TOKEN=` in `~/taosentinel/.env` (a `GITHUB_PAT=` line also works) + `GITHUB_TOKEN_EXPIRES_AT=YYYY-MM-DD` (fine-grained PATs send no expiry header; 06 §5) |
| B2 | **OnFinality free API key** for Bittensor Finney (raises 5 → 40 req/s, archive-capable). | Phase 3 (17) | app.onfinality.io → sign up → API Service → Bittensor Finney → key. | `ONFINALITY_API_KEY=` |
| B3 | **Telegram bot** (BotFather `/newbot`) + your **chat id** (message the bot, then `https://api.telegram.org/bot<token>/getUpdates`). The old `TELEGRAM_BOT_TOKEN/CHAT_ID` may still be valid — reuse it for TaoSentinel **only if the old trading bot will not run its own command bot**; two processes long-polling one token collide (HTTP 409 on getUpdates), so a revived SubnetTrader needs its own bot token. | Phase 0 alerts | t.me/BotFather | `TELEGRAM_BOT_TOKEN=`, `TELEGRAM_CHAT_ID=` |
| B4 | **Discord application + Bot** (discord.com/developers → New Application → Bot → enable *Message Content Intent*; copy token). Create your **own aggregator server** ("TaoSentinel Feed"), invite the bot to it only (OAuth2 URL generator: scopes `bot`, permissions *View Channels*, *Read Message History* (integer 66560); optionally also *Manage Webhooks* (integer 536937472) so the bot can see which Followed channel a crosspost came from — spec 08). | Phase 6 (08) | Developer portal | `DISCORD_BOT_TOKEN=`, `DISCORD_AGGREGATOR_GUILD_ID=` |
| B5 | **Discord manual pass (one-time, ~1 h):** join `discord.gg/bittensor`; enable *Developer Mode* (User Settings → Advanced); in the main server open `📣・announce` → **Follow** → target a channel in your aggregator server; for each `NN・name・symbol` subnet channel you can see, right-click → *Copy Channel ID* and paste into `seed/discord_channels.csv` with columns `netuid,channel_id,channel_name,guild_id,kind[,notes]` (`kind` = `announce` or `subnet_text`; old 3-column rows still load) — 22 rows are pre-filled by spec 08's `--export-csv` command from on-chain identities, ~64 remain; join the own-servers listed in `research/02-gapfill-discord-telegram-topology.md` that have announcement channels (SN35, 17, 65, 106, 20, …) and **Follow** each announcements channel into your aggregator server too. When done, set `DISCORD_CHANNEL_MAP_REVIEWED=true` (until then subnets that only link the generic Bittensor invite score `venue_reachable` as unknown, not 0). | Phase 6 (08) | Discord client | `seed/discord_channels.csv` |
| B6 | **Anthropic API key** with a spending limit set in the console (recommend $60/month). | Phase 9 (10) | console.anthropic.com → API keys; Settings → Limits. | `ANTHROPIC_API_KEY=`, `LLM_MONTHLY_CAP_USD=60` |
| B7 | **Internet Archive account** (free) → S3-like keys, only if you want weekly Save-Page-Now snapshots of subnet websites (optional). | Phase 5 (07) optional | archive.org/account/s3.php | `IA_ACCESS_KEY=`, `IA_SECRET_KEY=` |

## C. Paid / optional (Phase 7, 9, 12)

| # | Item | Cost | Unlocks | Where it goes |
|---|---|---|---|---|
| C1 | **twitterapi.io** account + API key (no card to start; $0.10 free credit). Set `X_MONTHLY_CAP_USD=25`. | ~$5–20/mo | P5 X cadence/engagement, P6 polarity & KOL coverage. | `X_PROVIDER=twitterapi_io`, `TWITTERAPI_IO_KEY=` |
| C2 | **X Developer account** (official pay-per-use) as ToS-clean fallback; add a card; set the console spend cap to $75. | $0.005/post read (≈ $70/mo on spec 09's lean plan) | Same as C1 via `X_PROVIDER=official_v2`. | `X_PROVIDER=official_v2`, `X_BEARER_TOKEN=`, `X_MONTHLY_CAP_USD=75`, `X_MENTIONS_ENABLED=false` |
| C3 | **TaoStats API key** — check whether the old key (49 chars in the Pi `.env`) is still valid on the free tier (5 req/min, 10k/mo); upgrade to Standard ($49/mo, 60/min, 50k/mo) when Phase 7 needs owner stake events at full fidelity. First test to run with it: `GET /v1/subnets/pools/history?netuid=64&frequency=by_hour&limit=5` (new API, 02 §G7; passed 2026-10-12 with the current free key). | $0–49/mo | `owner_retention` fidelity, hourly pool history, delegation feed (whale tracker revival, `/v1/subnets/stake-events`), `/v1/subnets/owners`. | `TAOSTATS_API_KEY=` |
| C4 | **TaoMarketCap Pro** ($49/mo) — only if the keyless 10 req/min / 2 SSE limits bite (they should not). | $49/mo | More SSE slots, 150 req/min. | `TMC_API_KEY=` |
| C5 | **tao.app API key** (pricing unknown; likely via wallet login on tao.app). Worth one email/DM: their `/api/beta/analytics/subnets/social/{netuid}/latest` is ready-made Discord+X analytics. | ? | Shortcut for P5/P6. | `TAOAPP_API_KEY=` |
| C6 | **Blockmachine** — the old key returns 401 (subscription lapsed). Not needed; OnFinality replaces it. | — | — | — |
| C7 | **Local lite subtensor node** (`docker compose up -d mainnet-lite`, ~128 GB NVMe, 16 GB RAM) — the "TAO RPC node". Only required for per-block `MinerBurned`/constants; defer until the laptop has the disk. | hardware | Spec 05 optional collector. | `SUBTENSOR_LOCAL_WS=ws://127.0.0.1:9944` |

## D. Manual data you maintain (ongoing, ~30 min/week)

| # | Item | Tool |
|---|---|---|
| D1 | **Team members** with evidence (name, role, profile URL, how you verified). Seeded as *unverified candidates* from subnetalpha `team_info`; you confirm or reject. Never assert an identity without two independent public sources. | `python -m scripts.manual_entry team …` or Telegram `/team add` |
| D2 | **Product status** per subnet (`none/waitlist/beta/live` + evidence URL). | `/product set <netuid> live <url>` |
| D3 | **KOL credibility** edits (`kol_accounts.credibility` 0–1). Seed list is in spec 09. | CSV in `seed/kol_accounts.csv` |
| D4 | **Human-confirmed red flags** (`owner_extraction`, `key_compromise`) and their clearance. | `/flag confirm <netuid> <flag> "<evidence url>"` |
| D5 | Review the weekly **"needs review"** Telegram digest (new occupants, source disagreements, dead invites, unresolved repos). | Telegram |
| D6 | **Golden-set labelling (one-off, ~2 h, before Phase 9 goes live):** hand-label ~200 posts/announcements in `tests/fixtures/llm_golden.jsonl` (event type, direction, materiality, netuids) so spec 10 can measure classifier precision/recall and detect drift when its rubric changes. | spec 10 eval CLI |

## E. Things you should *not* do
- Do not run any scraper with your personal X or Discord account (self-bots) — ToS bans.
- Do not commit `.env`, `data/`, or the Pi wallet anywhere.
- Do not point any collector at `backprop.finance` (ToS forbids automation) — manual reference only.
- Do not start a `mainnet-archive` node on the laptop (3.5 TB+); use OnFinality for history.
