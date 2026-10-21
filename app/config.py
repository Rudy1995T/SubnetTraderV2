"""Settings — the complete v1 configuration inventory (spec 01 §5, 02 §F1/§F2).

Dynamic-settings rule (binding): read configuration through the module attribute at call time —
``import app.config as app_config; app_config.settings.FOO`` or ``get_settings().FOO``.
Importing the ``settings`` name directly from this module is forbidden (tests/test_config.py
enforces it): a hot reload rebinds the attribute and a captured object would keep stale values.
"""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any

from pydantic import AliasChoices, Field, PrivateAttr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

APP_ROOT: Path = Path(os.environ.get("TAOSENTINEL_ROOT") or Path(__file__).resolve().parents[1])

_log = logging.getLogger("taosentinel.config")

_SECRET_SUFFIXES = ("_TOKEN", "_KEY", "_SECRET", "_PAT")
_SECRET_EXPLICIT = frozenset({"DEADMAN_PING_URL", "IA_SECRET_KEY"})
MASK = "••••••••"

# Keys whose change needs a process restart (trigger keys, bind address, paths) — 01 §2.4.
_RESTART_SUFFIXES = ("_PATH", "_DIR", "_INTERVAL_MIN", "_INTERVAL_SEC", "_INTERVAL_HOURS", "_CRON", "_CRON_HOUR",
                     "_CRON_MINUTE", "_CRON_HOURS", "_AT_UTC", "_HOUR_UTC", "_MINUTE", "_DAY", "_DAY_OF_WEEK",
                     "_WEEKDAY", "_TICK_MINUTES")
_RESTART_EXPLICIT = frozenset({"HEALTH_HOST", "HEALTH_PORT", "DB_PATH", "JSONL_DIR", "DATA_DIR", "SEED_DIR"})

_HHMM_KEYS = ("REGISTRY_IDENTITY_AT_UTC", "TMC_DAILY_AT_UTC", "TMC_WEEKLY_AT_UTC", "GITHUB_ORG_ENUM_CRON",
              "GITHUB_ACTIVITY_CRON", "GITHUB_METRICS_CRON", "GITHUB_HTML_PROBE_CRON", "OWNER_COLLECT_CRON",
              "X_TEAM_POLL_CRON", "X_MENTIONS_POLL_CRON", "X_CTF_CROSSCHECK_CRON", "RSS_GOOGLE_NEWS_POLL_CRON",
              "YOUTUBE_POLL_CRON")
_DOW_HHMM_KEYS = ("QUANT_WEEKLY_JOB_CRON", "X_PROFILE_REFRESH_CRON")
_HHMM_LIST_KEYS = ("CHAIN_FEATURES_AT_UTC", "INTEGRITY_FEATURES_AT_UTC")
_HOURS_LIST_KEYS = ("SCORING_CRON_HOURS", "DISCORD_CRON_HOURS", "WEB_METRICS_CRON_HOURS", "X_KOL_POLL_CRON_HOURS")


def _is_secret(key: str) -> bool:
    return key in _SECRET_EXPLICIT or key.endswith(_SECRET_SUFFIXES)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=os.environ.get("TAOSENTINEL_ENV_FILE", str(APP_ROOT / ".env")),
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    _warnings: list[str] = PrivateAttr(default_factory=list)

    # ── Runtime, alerts transport, shared providers (01) ────────────────────────────────────────────────────
    SPEC_VERSION: str = "ts-1.0"
    LOG_LEVEL: str = "INFO"
    LOG_RETENTION_ENABLED: bool = True
    LOG_RETENTION_DAYS: int = 14
    JSONL_DIR: str = "data/logs"
    DATA_DIR: str = "data"
    SEED_DIR: str = "seed"
    DB_PATH: str = "data/taosentinel.db"
    DB_BUSY_TIMEOUT_MS: int = 5000
    HEALTH_HOST: str = Field(default="127.0.0.1", validation_alias=AliasChoices("HEALTH_HOST", "API_HOST"))
    HEALTH_PORT: int = Field(default=8090, validation_alias=AliasChoices("HEALTH_PORT", "API_PORT"))
    API_AUTH_TOKEN: str = ""
    CONTROL_ALLOWED_ORIGINS: str = "http://localhost:3001,http://127.0.0.1:3001"
    KILL_SWITCH_PATH: str = "./KILL_SWITCH"
    SCORING_KILL_SWITCH_PATH: str = "./SCORING_KILLED"
    SYSTEMD_WATCHDOG_ENABLED: bool = True
    SHUTDOWN_DB_TIMEOUT_SEC: float = 5.0
    SHUTDOWN_HTTP_TIMEOUT_SEC: float = 3.0
    SHUTDOWN_DEADLINE_SEC: float = 10.0
    HTTP_USER_AGENT: str = "TaoSentinel/0.1 (+https://github.com/<operator>/taosentinel)"
    HTTP_TIMEOUT_SEC: float = 20.0
    HTTP_MAX_RETRIES: int = 2
    HTTP_MAX_BODY_BYTES: int = 5242880
    JOB_FAIL_ALERT_THRESHOLD: int = 5
    JOB_DEFAULT_TIMEOUT_SEC: int = 1800
    JOB_START_STAGGER_SEC: int = 20
    STARTUP_CATCHUP_DELAY_SEC: int = 300
    STARTUP_PRIME_ENABLED: bool = True
    ALERT_REPEAT_MIN: int = 360
    DISK_FREE_MIN_GB: float = 2.0
    TELEGRAM_BOT_TOKEN: str = ""
    TELEGRAM_CHAT_ID: str = ""
    TELEGRAM_COMMANDS_ENABLED: bool = True
    TELEGRAM_ALLOWED_CHAT_IDS: str = ""
    DB_BACKUP_ENABLED: bool = True
    DB_BACKUP_DIR: str = "data/backups"
    DB_BACKUP_MIRROR_DIR: str = ""
    DB_BACKUP_RETENTION: int = 7
    DB_BACKUP_WEEKLY_RETENTION: int = 8
    DB_BACKUP_INTERVAL_HOURS: int = 24
    DB_BACKUP_MIN_AGE_HOURS: float = 12.0
    BLOCK_SECONDS: float = 12.0
    SUBTENSOR_RPC_RATE_PER_SEC: float = 2.0
    SUBNETALPHA_RATE_PER_SEC: float = 1.0
    ONFINALITY_API_KEY: str = ""
    ONFINALITY_WS_URL: str = "wss://bittensor-finney.api.onfinality.io/public-ws"
    ONFINALITY_RATE_PER_SEC: float = 20.0
    # Blockmachine archive RPC (operator's Standard plan, 2026-09-24: 1,000 req/min, 5 M RU/month, 1 RU per call;
    # BUILD-LOG Phase 3). The .env spelling ``BLOCKMACHINE_API_kEY`` is accepted (settings are case-insensitive).
    BLOCKMACHINE_API_KEY: str = ""
    BLOCKMACHINE_WS_URL: str = "wss://rpc.blockmachine.io"
    BLOCKMACHINE_RATE_PER_SEC: float = 12.0
    BLOCKMACHINE_MONTHLY_CAP_REQ: int = 4_500_000
    TAOSTATS_ENABLED: bool = False
    TAOSTATS_API_KEY: str = ""
    TAOSTATS_BASE_URL: str = "https://api.taostats.io"
    TAOSTATS_RATE_PER_MIN: int = 5
    TAOSTATS_MONTHLY_CAP_REQ: int = Field(default=10000, validation_alias=AliasChoices("TAOSTATS_MONTHLY_CAP_REQ", "TAOSTATS_MONTHLY_CALL_CAP"))
    TAOSTATS_MONTHLY_CAP_USD: float = 0.0
    TAOAPP_API_KEY: str = ""

    # ── Storage (03) ────────────────────────────────────────────────────────────────────────────────────────
    LLM_PENDING_TEXT_MAX_DAYS: int = 7
    DB_READER_ENABLED: bool = True
    DB_CACHE_SIZE_MB: int = 64
    DB_MMAP_SIZE_MB: int = 256
    DB_INCREMENTAL_VACUUM_PAGES: int = 25600
    STORAGE_RETENTION_ENABLED: bool = True
    STORAGE_RETENTION_CRON_HOUR: int = 3
    STORAGE_RETENTION_CRON_MINUTE: int = 0
    RETENTION_REQUIRE_BACKUP_HOURS: float = 30.0
    RETENTION_DELETE_BATCH: int = 50000
    PRICE_BARS_5M_RETENTION_DAYS: int = 90
    METRIC_PROVISIONAL_RETENTION_DAYS: int = 90
    CHAIN_REST_SNAPSHOTS_RETENTION_DAYS: int = 90
    GITHUB_HTTP_CACHE_RETENTION_DAYS: int = 30
    LLM_BATCH_ITEMS_RETENTION_DAYS: int = 90
    WEB_TEXT_RETENTION_DAYS: int = 365

    # ── Registry / identity (04) ────────────────────────────────────────────────────────────────────────────
    REGISTRY_ENABLED: bool = True
    REGISTRY_OCCUPANTS_INTERVAL_MIN: int = 360
    REGISTRY_IDENTITY_AT_UTC: str = "02:40"
    REGISTRY_CHAIN_ENABLED: bool = True
    SUBTENSOR_RPC_URL: str = "https://entrypoint-finney.opentensor.ai"
    SUBTENSOR_RPC_FALLBACK_URLS: list[str] = Field(default_factory=lambda: [])
    SUBTENSOR_RPC_TIMEOUT_SEC: float = 15.0
    REGISTRY_CHAIN_QUERY_CHUNK: int = 100
    REGISTRY_CHAIN_MIN_KEYS: int = 90
    SUBNETALPHA_ENABLED: bool = True
    SUBNETALPHA_BASE_URL: str = "https://subnetalpha.ai/wp-json/wp/v2/subnet"
    SUBNETALPHA_TIMEOUT_SEC: float = 30.0
    REGISTRY_SUBNETALPHA_MAX_PAGES: int = 5
    REGISTRY_SUBNETALPHA_MIN_RECORDS: int = 100
    REGISTRY_TMC_COLDKEY_ENABLED: bool = True
    DISCORD_MAIN_GUILD_ID: str = "799672011265015819"
    REGISTRY_MISSING_RUNS_TO_DEREG: int = 3
    REGISTRY_ANOMALY_CONSECUTIVE_RUNS: int = 3
    REGISTRY_REOBSERVE_DAYS: int = 30
    REGISTRY_WATCH_RETIRE_DAYS: int = 14
    REGISTRY_JUNK_TOKENS: list[str] = Field(default_factory=lambda: [])
    REGISTRY_STALE_SOURCE_DAYS: int = 3
    LEGACY_FUNDAMENTALS_PATH: str = "seed/fundamentals.json"
    LEGACY_EDGE_NOTES_DIR: str = "seed/subnet_edge"
    LEGACY_AS_OF: str = "2026-06-02T11:03:41Z"

    # ── Chain / TMC (05) ────────────────────────────────────────────────────────────────────────────────────
    TMC_ENABLED: bool = True
    TMC_BASE_URL: str = "https://api.taomarketcap.com/public/v1"
    TMC_API_KEY: str = ""
    TMC_RATE_PER_MIN: int = 8
    TMC_RATE_BURST: int = 2
    TMC_HOURLY_CAP: int = 500
    TMC_MONTHLY_CAP: int = 90000
    TMC_REQUEST_TIMEOUT_SEC: int = 30
    TMC_MAX_RETRIES: int = 2
    TMC_SSE_ENABLED: bool = True
    TMC_SSE_URL: str = "https://api.taomarketcap.com/public/v1/sse/subnets/table/"
    TMC_SSE_READ_TIMEOUT_SEC: int = 90
    TMC_SSE_SILENCE_SEC: int = 180
    TMC_SSE_BACKOFF_MIN_SEC: float = 1.0
    TMC_SSE_BACKOFF_MAX_SEC: float = 60.0
    TMC_SSE_ALERT_AFTER_SEC: int = 1800
    TMC_SSE_UNAUTHORIZED_RETRY_SEC: int = 3600
    TMC_SSE_MAX_CONNECTIONS: int = 2
    TMC_SSE_BLOCKS_STREAM: str = "auto"
    TMC_TABLE_FALLBACK_ENABLED: bool = True
    TMC_TABLE_FALLBACK_SEC: int = 600
    CHAIN_TICKS_BLOCK_STRIDE: int = 5
    CHAIN_TICKS_RETENTION_DAYS: int = 30
    TMC_HOURLY_ENABLED: bool = True
    TMC_HOURLY_MINUTE: int = 7
    TMC_TRENDING_ENABLED: bool = True
    TMC_MARKET_ENABLED: bool = True
    TMC_DAILY_ENABLED: bool = True
    TMC_DAILY_AT_UTC: str = "02:10"
    TMC_SUBNETS_PAGE_LIMIT: int = 200
    TMC_ANALYTICS_1W_ENABLED: bool = True
    TMC_ANALYTICS_1W_FIELDS: str = "ts,block_number,emission_percent,chain_buys,subnet_tao,owner_personal_locked_alpha,owner_personal_decaying_alpha,total_locked,total_decaying,total_tao_buy_24h,total_tao_sell_24h,volume_24h,tx_count_24h,neuron_registrations_24h,rao_recycled_24h,tao_price_usd"
    TMC_PRESSURE_ENABLED: bool = True
    TMC_PRESSURE_WATCHLIST_ENABLED: bool = False
    TMC_PRESSURE_WATCHLIST_SIZE: int = 40
    TMC_WEEKLY_ENABLED: bool = True
    TMC_WEEKLY_AT_UTC: str = "01:10"
    TMC_HOLDERS_ENABLED: bool = True
    TMC_HOLDERS_PAGE_LIMIT: int = 100
    TMC_HOLDERS_MAX_PAGES: int = 2
    TMC_HOLDERS_MAX_AGE_DAYS: int = 14
    TMC_CANDLES_ENABLED: bool = True
    TMC_ANALYTICS_1Y_ENABLED: bool = True
    TMC_VALIDATORS_ENABLED: bool = True
    TMC_NEURONS_ENABLED: bool = False
    NETWORK_IMMUNITY_PERIOD_BLOCKS: int = 864000
    GATE_MODEL_VERSION: str = "gate-v440-1"
    GATE_BAR_RANK: int = 32
    GATE_EXPONENT: float = 3.0
    GATE_FIT_MAX_ITER: int = 50
    GATE_FIT_TOL: float = 0.0001
    GATE_FIT_TOLERANCE_PP: float = 6.0
    GATE_MIN_PRED_SHARE: float = 0.0001
    GATE_HOURLY_ENABLED: bool = False
    GATE_BROKEN_AFTER_DAYS: int = 7
    DEREG_STARVED_RATIO: float = 0.2
    DEREG_EMISSION_DECLINE_PCT: float = 30.0
    DEREG_PRICE_DECLINE_PCT: float = 40.0
    DEREG_DEPTH_DECLINE_PCT: float = 30.0
    DEREG_W_PROXIMITY: float = 0.45
    DEREG_W_STARVED: float = 0.25
    DEREG_W_DECLINE: float = 0.3
    DEREG_HIGH_RISK_AT: float = 0.6
    DEREG_HIST_DAYS: int = 30
    CHAIN_FEATURES_ENABLED: bool = True
    CHAIN_FEATURES_AT_UTC: list[str] = Field(default_factory=lambda: ["00:05","06:05","12:05","18:05"])
    CHAIN_FEATURES_CATCHUP_DAYS: int = 7
    SUBTENSOR_RPC_ENABLED: bool = False
    SUBTENSOR_RPC_MODE: str = "jsonrpc"
    SUBTENSOR_LOCAL_WS: str = "ws://127.0.0.1:9944"
    SUBTENSOR_RPC_ENDPOINTS: list[str] = Field(default_factory=lambda: ["ws://127.0.0.1:9944", "wss://bittensor-finney.api.onfinality.io/public-ws", "wss://entrypoint-finney.opentensor.ai:443"])
    SUBTENSOR_ENDPOINT_COOLDOWN_SEC: int = 300
    SUBTENSOR_RPC_CONNECT_TIMEOUT_SEC: int = 30
    SUBTENSOR_RPC_READ_TIMEOUT_SEC: int = 45
    SUBTENSOR_METAGRAPH_ENABLED: bool = False

    # ── GitHub (06) ─────────────────────────────────────────────────────────────────────────────────────────
    GITHUB_ENABLED: bool = True
    GITHUB_TOKEN: str = Field(default="", validation_alias=AliasChoices("GITHUB_TOKEN", "GITHUB_PAT"))
    GITHUB_API_BASE: str = "https://api.github.com"
    GITHUB_HTML_BASE: str = "https://github.com"
    GITHUB_RATE_PER_HOUR: int = 4000
    GITHUB_MAX_CONCURRENCY: int = 4
    GITHUB_RESERVE_REMAINING: int = 300
    GITHUB_RESOLVE_INTERVAL_MIN: int = 360
    GITHUB_ORG_ENUM_CRON: str = "20:00"
    GITHUB_ACTIVITY_CRON: str = "20:30"
    GITHUB_METRICS_CRON: str = "23:30"
    GITHUB_HTML_PROBE_CRON: str = "03:00"
    GITHUB_ACTIVITY_LOOKBACK_DAYS: int = 90
    GITHUB_ACTIVITY_MAX_REPOS_PER_OCCUPANT: int = 12
    GITHUB_ACTIVITY_MAX_COMMIT_PAGES: int = 5
    GITHUB_ACTIVITY_STALE_DAYS: int = 2
    GITHUB_PR_LOOKBACK_DAYS: int = 90
    GITHUB_PR_MAX_PER_REPO: int = 20
    GITHUB_COMMIT_STATS_SAMPLE: int = 10
    GITHUB_HYGIENE_INTERVAL_DAYS: int = 7
    GITHUB_ORIGINALITY_INTERVAL_DAYS: int = 30
    GITHUB_TEMPLATE_REPO: str = "opentensor/bittensor-subnet-template"
    GITHUB_STATS_202_RETRIES: int = 3
    GITHUB_STATS_202_WAIT_SEC: float = 3.0
    GITHUB_ETAG_ENABLED: bool = True
    GITHUB_ORG_MAX_REPOS: int = 200
    GITHUB_GONE_GRACE_DAYS: int = 14
    GITHUB_SPAM_BURST_N: int = 10
    GITHUB_SPAM_BURST_SEC: int = 60
    GITHUB_SPAM_MIN_STATS_TOTAL: int = 5
    GITHUB_FTE_COMMIT_DAYS_28D: int = 10
    GITHUB_BOT_LOGINS_EXTRA: str = ""
    GITHUB_EXTRA_DENYLIST: str = ""
    GITHUB_HTML_PROBE_ENABLED: bool = True
    GITHUB_HTML_PROBE_PACING_SEC: float = 1.0
    GITHUB_TMC_CROSSCHECK_ENABLED: bool = True
    GITHUB_STATS_INTERVAL_DAYS: int = 7
    GITHUB_TOKEN_EXPIRES_AT: str = ""

    # ── Web presence (07) ───────────────────────────────────────────────────────────────────────────────────
    WEB_ENABLED: bool = True
    WEB_TICK_MINUTES: int = 30
    WEB_RATE_PER_SEC: float = 1.0
    WEB_RATE_BURST: int = 2
    WEB_PER_URL_MIN_INTERVAL_H: float = 23.5
    WEB_SAME_HOST_GAP_S: float = 10.0
    WEB_NETWORK_PROBE_HOSTS: list[str] = Field(default_factory=lambda: ["archive.org","github.com"])
    WEB_LATEST_MAX_AGE_DAYS: int = 2
    WEB_CONCURRENCY: int = 4
    WEB_TIMEOUT_S: float = 10.0
    WEB_CONNECT_TIMEOUT_S: float = 5.0
    WEB_DNS_TIMEOUT_S: float = 5.0
    WEB_RETRY_DELAY_S: int = 600
    WEB_MAX_REDIRECTS: int = 5
    WEB_MAX_BODY_BYTES: int = 2000000
    WEB_USER_AGENT: str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36 TaoSentinel/1.0 (uptime monitor)"
    WEB_ROBOTS_ENABLED: bool = True
    WEB_ROBOTS_CACHE_DAYS: int = 7
    WEB_EXCLUDED_HOSTS: list[str] = Field(default_factory=lambda: ["x.com", "twitter.com", "discord.com", "discord.gg", "t.me", "github.com", "youtube.com", "taostats.io", "taomarketcap.com", "subnetalpha.ai", "tao.app", "linktr.ee", "linkin.bio", "vimeo.com", "huggingface.co", "youtu.be", "discordapp.com", "telegram.me"])
    WEB_MAX_WEBSITES_PER_OCCUPANT: int = 2
    WEB_PARKING_HOSTS: list[str] = Field(default_factory=lambda: ["sedoparking.com", "sedo.com", "hugedomains.com", "dan.com", "afternic.com", "godaddy.com", "parkingcrew.net", "bodis.com", "above.com", "undeveloped.com", "domainmarket.com", "sav.com", "dynadot.com", "namecheap.com", "porkbun.com", "squadhelp.com", "atom.com", "brandbucket.com", "namesilo.com", "parklogic.com", "ndsplus.com", "cashparking.com"])
    WEB_PARKED_STRONG_PHRASES: list[str] = Field(default_factory=lambda: ["domain for sale", "this domain is for sale", "buy this domain", "domain is parked", "parked free", "parked domain", "domain parking", "domain name for sale", "this webpage was generated by the domain owner", "website expired", "this account has been suspended", "account suspended", "this site can't be reached", "hosting account has expired", "is for sale!", "welcome to nginx!", "apache2 debian default page", "apache2 ubuntu default page", "test page for the apache http server"])
    WEB_PARKED_WEAK_KEYWORDS: list[str] = Field(default_factory=lambda: ["make an offer", "get this domain", "for sale", "sedo", "sedoparking", "godaddy", "hugedomains", "afternic", "dan.com", "namecheap", "parkingcrew", "bodis", "domain owner", "buy now", "inquire", "coming soon", "under construction", "placeholder page", "default web page"])
    WEB_HIJACK_KEYWORDS: list[str] = Field(default_factory=lambda: ["casino", "slot machine", "betting", "sportsbook", "viagra", "cialis", "escort", "xxx", "porn", "adult content", "crypto giveaway", "airdrop claim", "payday loan", "essay writing", "replica watches", "free spins"])
    WEB_SHARED_PLATFORM_HOSTS: list[str] = Field(default_factory=lambda: ["github.io", "vercel.app", "netlify.app", "pages.dev", "notion.site", "webflow.io", "framer.website", "framer.app", "gitbook.io", "mintlify.app", "readthedocs.io", "super.site", "carrd.co", "wixsite.com", "squarespace.com", "substack.com", "medium.com", "super.so", "softr.app", "bubbleapps.io"])
    WEB_TINY_BODY_CHARS: int = 300
    WEB_CONTENT_RESET_RATIO: float = 0.2
    WEB_FRESHNESS_MIN_DELTA: float = 0.03
    WEB_STORE_TEXT_MAX_CHARS: int = 32000
    WEB_TLS_WARN_DAYS: int = 14
    WEB_UPTIME_WINDOW_DAYS: int = 30
    WEB_UPTIME_MIN_CHECKS: int = 3
    WEB_DOWN_STREAK_DAYS: int = 7
    WEB_DOCS_GONE_STREAK_DAYS: int = 2
    WEB_DOCS_AUTODISCOVER_ENABLED: bool = True
    WEB_WAYBACK_ENABLED: bool = True
    WEB_WAYBACK_RECHECK_DAYS: int = 30
    WAYBACK_RATE_PER_SEC: float = 0.5
    WEB_SPN2_ENABLED: bool = False
    IA_ACCESS_KEY: str = ""
    IA_SECRET_KEY: str = ""
    WEB_SPN2_WEEKDAY: int = 6
    WEB_SPN2_HOUR_UTC: int = 7
    WEB_SPN2_MIN_INTERVAL_S: float = 10.0
    WEB_METRICS_CRON_HOURS: str = "0,6,12,18"
    WEB_METRICS_CRON_MINUTE: int = 5

    # ── Community (08) ──────────────────────────────────────────────────────────────────────────────────────
    DISCORD_ENABLED: bool = True
    DISCORD_INTERVAL_MIN: int = 360
    DISCORD_WIDGET_ENABLED: bool = True
    DISCORD_WIDGET_INTERVAL_MIN: int = 60
    DISCORD_RATE_PER_SEC: float = 1.0
    DISCORD_BOT_TOKEN: str = ""
    DISCORD_AGGREGATOR_GUILD_ID: str = ""
    DISCORD_GATEWAY_ENABLED: bool = False
    DISCORD_CHANNELS_SEED_PATH: str = "seed/discord_channels.csv"
    TELEGRAM_COUNTS_ENABLED: bool = True
    TELEGRAM_COUNTS_INTERVAL_MIN: int = 1440
    TELEGRAM_COUNTS_RATE_PER_SEC: float = 0.5
    DISCORD_CRON_HOURS: str = "0,6,12,18"
    DISCORD_CRON_MINUTE: int = 0
    DISCORD_API_BASE: str = "https://discord.com/api/v10"
    DISCORD_USER_AGENT: str = "DiscordBot (https://github.com/<operator>/taosentinel, 0.1.0)"
    DISCORD_INVITES_USE_BOT_TOKEN: bool = True
    DISCORD_MAIN_VANITY_CODE: str = "bittensor"
    DISCORD_MAIN_ANNOUNCE_CHANNEL_ID: str = "830075335084474390"
    DISCORD_MAIN_CHANNEL_RECHECK_DAYS: int = 7
    DISCORD_DEAD_WEEKLY_AFTER_DAYS: int = 30
    DISCORD_INVITE_EXPIRY_WARN_DAYS: int = 7
    DISCORD_INVALID_GUARD_PER_10MIN: int = 100
    DISCORD_WIDGET_DISABLED_RECHECK_DAYS: int = 7
    DISCORD_WIDGET_SKIP_GUILD_IDS: str = "799672011265015819"
    DISCORD_CHANNEL_MAP_REVIEWED: bool = False
    DISCORD_PRESENCE_MIN_SAMPLES: int = 2
    DISCORD_AGGREGATOR_CHANNEL_IDS: str = ""
    DISCORD_GATEWAY_ENUMERATE_FOLLOWS: bool = True
    DISCORD_BACKFILL_DAYS: int = 14
    DISCORD_BACKFILL_OVERLAP_SEC: int = 600
    DISCORD_BACKFILL_MAX_PER_CHANNEL: int = 2000
    DISCORD_TEXT_MAX_CHARS: int = 4000
    TELEGRAM_BASE_URL: str = "https://t.me"
    TELEGRAM_PREVIEW_ENABLED: bool = True
    TELEGRAM_POSTS_STORE_TEXT: bool = False
    DISCORD_STORE_TEXT: bool = True
    COMMUNITY_EVENTS_ENABLED: bool = True
    COMMUNITY_VENUE_CHANGE_DEBOUNCE: int = 2

    # ── X / RSS / news / YouTube (09) ───────────────────────────────────────────────────────────────────────
    X_ENABLED: bool = False
    X_PROVIDER: str = "null"
    TWITTERAPI_IO_KEY: str = ""
    TWITTERAPI_IO_BASE_URL: str = "https://api.twitterapi.io"
    X_BEARER_TOKEN: str = ""
    X_OFFICIAL_BASE_URL: str = "https://api.x.com/2"
    X_OFFICIAL_EXCLUDE: str = "retweets,replies"
    X_MONTHLY_CAP_USD: float = 25.0
    X_SOFT_ALERT_PCT: float = 0.8
    X_PACING_ENABLED: bool = True
    X_PRICES_JSON: str = ""
    X_TEAM_ENABLED: bool = True
    X_TEAM_POLL_CRON: str = "01:30"
    X_TEAM_POLL_MODE: str = "timeline"
    X_TIMELINE_MAX_PAGES: int = 3
    X_KOL_ENABLED: bool = True
    X_KOL_POLL_CRON_HOURS: str = "0,6,12,18"
    X_KOL_POLL_MODE: str = "search"
    X_MENTIONS_ENABLED: bool = True
    X_MENTIONS_TOP_N: int = 40
    X_MENTIONS_POLL_CRON: str = "02:00"
    X_SEARCH_MAX_POSTS_PER_QUERY: int = 200
    X_PROFILE_REFRESH_CRON: str = "sun 02:30"
    X_PROFILE_REFRESH_DAYS: int = 7
    X_UNRESOLVED_RETRY_DAYS: int = 7
    X_MIN_CREDIBILITY: float = 0.3
    X_AUTHOR_SHARE_CAP: float = 0.25
    X_DUPLICATE_WINDOW_MIN: int = 10
    X_RATE_PER_SEC: float = 1.0
    X_OFFICIAL_RATE_PER_SEC: float = 0.25
    X_HTTP_TIMEOUT_SEC: float = 20.0
    X_RAW_ARCHIVE_ENABLED: bool = True
    X_CTF_CROSSCHECK_ENABLED: bool = True
    X_CTF_CROSSCHECK_CRON: str = "07:00"
    X_CTF_FALLBACK_METRICS: bool = True
    KOL_SEED_PATH: str = "seed/kol_accounts.csv"
    RSS_ENABLED: bool = True
    RSS_POLL_INTERVAL_HOURS: int = 6
    RSS_GOOGLE_NEWS_ENABLED: bool = True
    RSS_GOOGLE_NEWS_TOP_N: int = 40
    RSS_GOOGLE_NEWS_POLL_CRON: str = "05:00"
    RSS_GOOGLE_NEWS_DECODE_URLS: bool = False
    RSS_FEEDS_SEED_PATH: str = "seed/feeds.csv"
    RSS_EXTRA_FEEDS: str = ""
    RSS_RATE_PER_SEC: float = 0.5
    GOOGLE_NEWS_MIN_INTERVAL_SEC: float = 10.0
    RSS_HTTP_TIMEOUT_SEC: float = 20.0
    RSS_DEAD_AFTER_FAILURES: int = 10
    NEWS_KEYWORDS: str = "bittensor,dtao,subnet,$tao,taostats,opentensor"
    YOUTUBE_ENABLED: bool = True
    YOUTUBE_POLL_CRON: str = "05:30"
    YOUTUBE_NOVELTY_PLAYLIST_ID: str = "PLN0WJc6io8zqEknIutfyEjiJViazrxrzU"
    YOUTUBE_RATE_PER_SEC: float = 0.33
    YOUTUBE_API_KEY: str = ""

    # ── LLM classification (10) ─────────────────────────────────────────────────────────────────────────────
    LLM_ENABLED: bool = False
    ANTHROPIC_API_KEY: str = ""
    LLM_MONTHLY_CAP_USD: float = 60.0
    LLM_BULK_MODEL: str = "claude-haiku-5-5"
    LLM_ESCALATION_MODEL: str = "claude-sonnet-5-5"
    LLM_NARRATIVE_MODEL: str = "claude-opus-5-5"
    LLM_REFUSAL_FALLBACK: bool = True
    LLM_SCHEMA_V: int = 1
    LLM_RUBRIC_PATH: str = "app/nlp/prompts/classify_v1.md"
    LLM_BATCH_INTERVAL_MIN: int = 360
    LLM_BATCH_OFFSET_MIN: int = 20
    LLM_POLL_INTERVAL_MIN: int = 5
    LLM_BATCH_ITEMS_PER_REQUEST: int = 20
    LLM_BATCH_MAX_REQUESTS: int = 250
    LLM_BATCH_STALE_H: int = 26
    LLM_LOOKBACK_DAYS: int = 3
    LLM_MAX_ITEM_CHARS: int = 1200
    LLM_TOKENS_PER_CHAR: float = 0.29
    LLM_PRICES_JSON: str = ""
    LLM_ENTITY_MIN_CONFIDENCE: float = 0.6
    LLM_ESCALATION_ENABLED: bool = True
    LLM_ESCALATION_MAX_PER_DAY: int = 40
    LLM_NARRATIVE_ENABLED: bool = True
    LLM_NARRATIVE_DAY_OF_WEEK: str = "sun"
    LLM_NARRATIVE_HOUR_UTC: int = 1
    LLM_NARRATIVE_MAX_SUBNETS: int = 40
    LLM_NARRATIVE_EVIDENCE_TOKENS: int = 8000
    LLM_FALLBACK_AFTER_H: int = 18
    LLM_RULES_ENABLED: bool = True
    LLM_TEXT_RETENTION_DAYS: int = 90
    LLM_REQUEST_TIMEOUT_S: int = 180
    LLM_RATE_PER_SEC: float = 0.5
    LLM_EVAL_ALLOW_NETWORK: bool = False

    # ── Team & integrity (11) ───────────────────────────────────────────────────────────────────────────────
    TEAM_ENABLED: bool = True
    TEAM_SEED_ENABLED: bool = True
    TEAM_SEED_SUBNETALPHA_ENABLED: bool = True
    TEAM_SEED_LEGACY_ENABLED: bool = True
    TEAM_LEGACY_NOTES_DIR: str = "seed/subnet_edge"
    TEAM_LEGACY_AS_OF: str = ""
    TEAM_VERIFY_MIN_SOURCES: int = 2
    TEAM_EVIDENCE_EXCLUDED_DOMAINS: list[str] = Field(default_factory=lambda: ["subnetalpha.ai"])
    TEAM_REVERIFY_DAYS: int = 90
    TEAM_VERIFY_STALE_DAYS: int = 180
    TEAM_ANON_MAX_CHARS: int = 200
    TEAM_NAME_MAX_TOKENS: int = 5
    OWNER_ENABLED: bool = True
    OWNER_COLLECT_CRON: str = "20:30"
    OWNER_KEYS_SOURCE: str = "auto"
    OWNER_TMC_MAX_PER_MIN: int = 5
    OWNER_TMC_MAX_PER_RUN: int = 518
    OWNER_ACTIVITY_PAGE_LIMIT: int = 100
    OWNER_ACTIVITY_MAX_PAGES: int = 10
    OWNER_ACTIVITY_OVERLAP_SEC: int = 3600
    OWNER_ACTIVITY_BACKFILL_DAYS: int = 35
    OWNER_BACKFILL_MAX_CALLS_PER_RUN: int = 128
    OWNER_ACTIVITY_TIME_FORMAT: str = "auto"
    OWNER_AMOUNT_UNITS: str = "auto"
    OWNER_STAKE_REMOVED_ENABLED: bool = False     # 11 §2.3.1: stake-removed rows carry no coldkey
    OWNER_STAKE_REMOVED_MAX_CALLS_PER_DAY: int = 20
    OWNER_RECONCILE_MIN_ALPHA: float = 500.0
    OWNER_NEURON_SPAN: str = "1W"
    OWNER_RUFUS_ENABLED: bool = True
    OWNER_RUFUS_TABLE_URL: str = "https://s3.hippius.com/rufus/public/conviction-table/data/table.json"
    OWNER_RUFUS_BAKED_AT_URL: str = "https://s3.hippius.com/rufus/public/conviction-table/data/baked-at.json"
    OWNER_RUFUS_MAX_AGE_HOURS: int = 48
    OWNER_SUBNETRADAR_ENABLED: bool = True
    OWNER_SUBNETRADAR_URL: str = "https://subnetradar.com/swaps"
    OWNER_TAOSTATS_ENABLED: bool = True
    OWNER_TAOSTATS_EXTRINSIC_WEEKDAY: int = 0
    OWNER_STREAM_ENABLED: bool = False
    OWNER_WATCH_HOURLY_ENABLED: bool = True
    OWNER_RPC_HISTORY_SEED_ENABLED: bool = True
    OWNER_SNAPSHOT_MAX_AGE_HOURS: int = 36
    OWNER_MIN_EARNED_ALPHA: float = 1.0
    OWNER_PERP_OFF_MIN_ALPHA: float = 100.0
    OWNER_PERP_OFF_MATCH_TOL: float = 0.1
    OWNER_OVERRIDE_MAX_WINDOW_DAYS: int = 45
    OWNER_SOURCE_DISAGREE_TOL: float = 0.05
    OWNER_S4_ENABLED: bool = False
    OWNER_S4_HOLDING_SHARE_MAX: float = 0.25
    OWNER_S4_LOCK_RATIO_MIN: float = 0.5
    OWNER_S4_VALI_STAKE_SHARE_MAX: float = 0.3
    OWNER_S4_SELF_WEIGHT_MAX: float = 0.2
    DTAO_LAUNCH_UTC: str = "2025-02-13T00:00:00Z"
    INTEGRITY_FEATURES_ENABLED: bool = True
    INTEGRITY_FEATURES_AT_UTC: list[str] = Field(default_factory=lambda: ["00:07","06:07","12:07","18:07"])
    INTEGRITY_FEATURES_CATCHUP_DAYS: int = 7

    # ── Features / scoring (12) ─────────────────────────────────────────────────────────────────────────────
    FEATURES_ENABLED: bool = True
    FEATURES_INTERVAL_MIN: int = 60
    SCORING_ENABLED: bool = True
    SCORING_CRON_HOURS: str = "0,6,12,18"
    SCORING_CRON_MINUTE: int = 15
    SCORING_OFFICIAL_HOUR: int = 0
    SCORING_SHADOW_SPECS: str = ""
    SCORING_MIN_CONFIDENCE: float = 0.4
    SCORING_EVENTS_ENABLED: bool = True
    SCORING_CATCHUP_ENABLED: bool = True
    SCORING_CATCHUP_MAX_SLOTS: int = 8
    SCORING_LATE_GRACE_MIN: int = 45
    SCORING_STALE_AFTER_MIN: int = 420
    SCORE_JUMP_ALERT_POINTS: float = 15.0
    RANK_STABILITY_ENABLED: bool = True
    RANK_AUTOCORR_ALERT_BELOW: float = 0.9
    METRIC_NORM_PROVISIONAL_RETENTION_DAYS: int = 14
    FEATURE_FN_TIMEOUT_SEC: float = 20.0
    FEATURE_COMPUTE_BUDGET_SEC: int = 600

    # ── Validation harness (13) ─────────────────────────────────────────────────────────────────────────────
    QUANT_ENABLED: bool = True
    QUANT_RESULTS_DIR: str = "data/quant/results"
    QUANT_SNAPSHOT_DIR: str = "data/quant/snapshots"
    QUANT_SNAPSHOT_KEEP: int = 2
    QUANT_DB_PATH: str = ""
    QUANT_OBSERVE_TELEMETRY_PATH: str = "data/quant/observe_telemetry.json"
    QUANT_OBSERVE_MIN_DAYS: int = 30
    QUANT_OBSERVE_MIN_COVERAGE: float = 0.6
    QUANT_OBSERVE_MIN_ENTRIES: int = 20
    QUANT_UPSIZE_MIN_T: float = 3.0
    QUANT_ZERO_RUN_DAYS: int = 7
    QUANT_ZERO_EMISSION_EPS_PCT: float = 0.0001
    QUANT_SEED: int = 0
    QUANT_SPEC_VERSION: str = "ts-1.0"
    QUANT_PIT_MODE: str = "strict"
    QUANT_RESET_DROP_RATIO: float = 0.3
    QUANT_SURVIVORSHIP_START: str = "2025-09-22"
    QUANT_LIVE_ERA_START: str = "2026-04-22"
    QUANT_GRID_SURVIVORSHIP_S: int = 86400
    QUANT_GRID_LIVE_S: int = 14400
    QUANT_HORIZONS_24H: str = "7,28,84"
    QUANT_HORIZONS_4H: str = "6,12,42"
    QUANT_LOOKBACK_BARS: int = 6
    QUANT_MIN_DEPTH_TAO: float = 3000.0
    QUANT_MIN_NAMES: int = 10
    QUANT_MIN_IC_DATES: int = 10
    QUANT_CONTROLS: str = "size,emission_share"
    QUANT_TRADE_TAO: float = 2.0
    QUANT_FEE_RATE: float = 0.003
    QUANT_IMPACT_SCALE: float = 1.0
    QUANT_DEATH_FLOOR_RETURN: float = 0.01
    QUANT_SCORE_MAX_AGE_S: int = 129600
    QUANT_PIT_MAX_AGE_DEFAULT_S: int = 172800
    QUANT_GATE_FACTOR: str = "quality_cg"
    QUANT_GATE_LABEL: str = "ret_log"
    QUANT_GATE_HORIZON_DAYS: int = 7
    QUANT_WF_TEST_DAYS: int = 28
    QUANT_WF_TRAIN_MIN_FRACTION: float = 0.5
    QUANT_MIN_FOLDS: int = 4
    QUANT_DSR_THRESHOLD: float = 0.95
    QUANT_DECAY_MIN: float = 0.25
    QUANT_MONOTONE_MIN: float = 0.6
    QUANT_SURVIVAL_HORIZON_DAYS: int = 90
    QUANT_SURVIVAL_MIN_EVENTS: int = 10
    QUANT_SURVIVAL_RATIO_MIN: float = 2.0
    QUANT_GATE_TOP_N: int = 32
    QUANT_GATE_CROSS_DAYS: int = 28
    QUANT_EMISSION_REGIME_BOUNDARIES: str = "taoflow:2025-09-22,price_burn:2026-06-25,gate_v440:2026-07-28"
    QUANT_WEEKLY_JOB_ENABLED: bool = False
    QUANT_WEEKLY_JOB_CRON: str = "sun 05:00"
    QUANT_WEEKLY_JOB_TIMEOUT_S: int = 1800
    QUANT_ALERT_AFTER_FAILURES: int = 2

    # ── API / Telegram / alerts (14) ────────────────────────────────────────────────────────────────────────
    UI_BASE_URL: str = "http://127.0.0.1:8090/ui"           # 14 §8.6: the UI is served by this app at /ui
    SCORE_FEED_TTL_SEC: int = 25200
    STREAM_ENABLED: bool = True
    STREAM_MAX_CONNECTIONS: int = 8
    STREAM_HEARTBEAT_SEC: int = 15
    STREAM_POLL_SEC: float = 5.0
    STREAM_REPLAY_MAX: int = 1000
    STREAM_HEALTH_EVERY_SEC: int = 60
    EXPORT_CACHE_MAX_AGE_SEC: int = 300
    HEALTH_PROBE_TIMEOUT_SEC: float = 10.0
    HEALTH_SERVICES_CACHE_SEC: int = 60
    TELEGRAM_ENABLED: bool = True
    TELEGRAM_POLL_TIMEOUT_SEC: int = 20
    TELEGRAM_MAX_MSG_PER_MIN: int = 20
    TELEGRAM_CONFIRM_TTL_SEC: int = 300
    TERMINAL_API_URL: str = "http://127.0.0.1:8787"     # 14 §8.4a: τ Terminal read API (loopback only; empty = off)
    ALERTS_ENABLED: bool = True
    ALERT_DISPATCH_INTERVAL_SEC: int = 60
    ALERT_MIN_SEVERITY: str = "info"
    ALERT_LATE_AFTER_HOURS: int = 24
    ALERT_BATCH_THRESHOLD: int = 5
    ALERT_MAX_ROWS_PER_TICK: int = 200
    WATCHLIST_NETUIDS: list[int] = Field(default_factory=lambda: [])
    DEREG_WATCH_ENABLED: bool = True
    DEREG_WATCH_INTERVAL_MIN: int = 60
    DEREG_WATCH_SAFETY_MAX: float = 0.1
    DEREG_WATCH_IMMUNITY_WARN_DAYS: int = 7
    NEEDS_REVIEW_DIGEST_ENABLED: bool = True
    NEEDS_REVIEW_DIGEST_DAY: str = "mon"
    NEEDS_REVIEW_DIGEST_HOUR_UTC: int = 8
    BLOCKED_SITE_DAYS: int = 7
    FRONTEND_PORT: int = 3001                               # retired 2026-10-18 (14 §5); nothing reads it

    # ── Ops, backups, cost governor (16) ────────────────────────────────────────────────────────────────────
    PAUSE_ALL_PATH: str = "./PAUSE_ALL"
    CONTROL_DIR: str = "data/control"
    RUN_DIR: str = "data/run"
    DB_BACKUP_CRON_HOUR: int = 3
    DB_BACKUP_CRON_MINUTE: int = 30
    DB_BACKUP_COMPRESS: bool = True
    DB_BACKUP_GZIP_LEVEL: int = 6
    DB_BACKUP_MAX_TOTAL_GB: float = 12.0
    DB_BACKUP_MIN_FREE_FACTOR: float = 1.3
    DB_BACKUP_KEEP_UNCOMPRESSED: int = 1
    DB_BACKUP_MANUAL_RETENTION: int = 5
    DB_BACKUP_MIRROR_MARKER: str = ".taosentinel-mirror"
    DB_BACKUP_MIRROR_MAX_TOTAL_GB: float = 0.0
    DB_BACKUP_STARTUP_DELAY_SEC: int = 600
    DB_BACKUP_WAIT_FOR_JOBS: str = "storage_retention_daily"
    DB_BACKUP_WAIT_MAX_SEC: int = 1200
    DB_BACKUP_STALE_HOURS: float = 36.0
    DB_QUICK_CHECK_ON_UNCLEAN: bool = True
    DB_QUICK_CHECK_DELAY_SEC: int = 300
    DB_WAL_WARN_MB: int = 1024
    DISK_FREE_CRITICAL_GB: float = 0.5
    OPS_GUARD_ENABLED: bool = True
    OPS_GUARD_INTERVAL_MIN: int = 5
    OPS_DIGEST_ENABLED: bool = True
    OPS_DIGEST_HOUR_UTC: int = 9
    OPS_DIGEST_MINUTE: int = 0
    DEADMAN_ENABLED: bool = False
    DEADMAN_PING_URL: str = ""
    WATCHDOG_API_SELF_PROBE_ENABLED: bool = True
    WATCHDOG_API_PROBE_FAILS: int = 3
    COST_LEDGER_ENABLED: bool = True
    COST_LEDGER_INTERVAL_MIN: int = 60
    COST_SOFT_ALERT_PCT: float = 0.8
    COST_HARD_STOP_PCT: float = 1.0
    COST_RESERVATION_TTL_SEC: int = 86400
    COST_OVERRIDE_MAX_USD: float = 100.0
    PAT_EXPIRY_WARN_DAYS: int = 14
    SECURITY_SELFCHECK_ENABLED: bool = True
    SCRIPT_TMC_RATE_PER_MIN: int = 2
    SCRIPT_RATE_SHARE: float = 0.25

    # ── Backfill (17) ───────────────────────────────────────────────────────────────────────────────────────
    BACKFILL_ENABLED: bool = True
    BACKFILL_DIR: str = "data/backfill"
    BACKFILL_ALERTS_ENABLED: bool = True
    BACKFILL_MIN_FREE_GB: float = 3.0
    LEDGER_SEED_PATH: str = "seed/ledger.db"
    LEDGER_BACKFILL_SEED_PATH: str = "seed/ledger_backfill.db"
    LEDGER_PI_SEED_PATH: str = "seed/pi/ledger.db"
    LEDGER_EXTRA_SEED_PATHS: list[str] = Field(default_factory=lambda: [])
    LEDGER_NRA_TRUST_FROM_TS: int = 1780182493
    LEDGER_LIVE_FROM_TS: int = 1777468595
    LEDGER_IMPORT_THIN_SEC: int = 0
    LEDGER_IMPORT_BATCH_ROWS_ONLINE: int = 20000
    LEDGER_EXPECTED_ROWS: int = 7362854
    LEDGER_EXPECTED_NETUIDS: int = 129
    LEDGER_BACKFILL_EXPECTED_MERGE: int = 23237
    WHALE_COHORT_SEED_PATH: str = "seed/whale_cohort.json"
    WHALE_LABELS_SEED_PATH: str = "seed/whale_labels.json"
    ARCHIVE_WS_URL: str = "wss://archive.chain.opentensor.ai:443"
    ARCHIVE_RATE_PER_SEC: float = 2.0
    ARCHIVE_MIN_BUCKET_INTERVAL_SEC: float = 1.5
    ARCHIVE_BACKOFF_MAX_SEC: int = 120
    BACKFILL_RPC_ENDPOINTS: list[str] = Field(default_factory=lambda: [])
    BACKFILL_RPC_ERAS: list[str] = Field(default_factory=lambda: ["gap","ledger","pre_ledger"])
    BACKFILL_RPC_STEP_SEC: int = 3600
    BACKFILL_RPC_SAMPLE_OFFSET_SEC: int = 60
    BACKFILL_RPC_MIN_NETUIDS_PER_HOUR: int = 100
    BACKFILL_RPC_PUBLIC_MAX_RPS: float = 4.0
    BACKFILL_RPC_CONCURRENCY: int = 0
    BACKFILL_RPC_MAX_ATTEMPTS: int = 6
    BACKFILL_RPC_TIMEOUT_SEC: float = 45.0
    BACKFILL_RPC_PRICE_METHOD: str = "auto"
    BACKFILL_RPC_MODE: str = "runtime_api"
    DTAO_LAUNCH_BLOCK: int = 4920351
    LEDGER_START_BLOCK: int = 6502594
    BACKFILL_TMC_ENABLED: bool = True
    BACKFILL_TMC_TIMEOUT_SEC: int = 14400
    BACKFILL_TMC_MAX_PER_HOUR: int = 180
    BACKFILL_TMC_QUIET_UTC: str = "02:05-02:45,20:25-23:35"
    BACKFILL_TMC_CANDLE_TIMEOUT_SEC: int = 90
    BACKFILL_TMC_CANDLE_CHUNK_DAYS: int = 30
    BACKFILL_TMC_MIN_5M_PER_1H: int = 6
    BACKFILL_TMC_MIN_5M_PER_1D: int = 144
    PI_TRADER_COLDKEY: str = ""
    BACKFILL_GITHUB_ENABLED: bool = True
    BACKFILL_GITHUB_SINCE: str = "2025-02-13"
    BACKFILL_GITHUB_MAX_PAGES_PER_REPO: int = 50
    BACKFILL_GITHUB_MAX_REPOS_PER_OCCUPANT: int = 30
    BACKFILL_GITHUB_PRS_ENABLED: bool = True
    BACKFILL_GITHUB_PR_MAX_PER_REPO: int = 200
    BACKFILL_GITHUB_RESERVE_REMAINING: int = 1500
    BACKFILL_GITHUB_TIMEOUT_SEC: int = 14400
    BACKFILL_GITHUB_WINDOW_UTC: str = "00:30-08:00"
    REGIME_BACKFILL_START: str = ""
    COVERAGE_PATH: str = "data/quant/coverage.json"

    # ── Investment workbench (19) ───────────────────────────────────────────────────────────────────────────
    WB_ENABLED: bool = True
    WB_EMA_PERIOD: int = 72
    WB_EMA_WINDOW_BARS: int = 432
    WB_EMA_MIN_BARS: int = 72
    WB_EMA_TIMEFRAMES: list[str] = Field(default_factory=lambda: ["1h", "1d"])
    WB_EMA_BAND_MODE: str = "sigma"
    WB_EMA_BAND_K: float = 2.0
    WB_STALE_BARS_1H: int = 3
    WB_DEREG_HORIZONS_DAYS: list[int] = Field(default_factory=lambda: [30, 90])
    WB_DEREG_REG_GAP_WINDOW_DAYS: int = 90
    WB_DEREG_REG_GAP_FALLBACK_DAYS: float = 8.0
    WB_CHAIN_PROBE_URL: str = "wss://entrypoint-finney.opentensor.ai:443"
    WB_CHAIN_PROBE_TIMEOUT_SEC: int = 20
    WB_GITHUB_PROBE_ENABLED: bool = True
    WB_GITHUB_PROBE_PER_RUN: int = 45
    WB_VISION_ENABLED: bool = False
    WB_VISION_MODEL: str = "claude-opus-5-5"
    WB_VISION_EST_USD: float = 0.10
    WB_VISION_MAX_IMAGE_MB: int = 5
    WB_UPLOAD_DIR: str = "data/wb/uploads"
    WB_EVENTS_ENABLED: bool = False
    WB_STUDY_IMMUNITY_BLOCKS: int = 864000

    # ── validators (owner §5 tables; a raise fails the boot loudly, 01 §7) ──────────────────────────────────
    @field_validator("LOG_LEVEL")
    @classmethod
    def _v_log_level(cls, v: str) -> str:
        v = str(v).upper()
        if v not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError(f"LOG_LEVEL must be one of DEBUG/INFO/WARNING/ERROR/CRITICAL, got {v!r}")
        return v

    @field_validator("SCORING_OFFICIAL_HOUR")
    @classmethod
    def _v_official_hour(cls, v: int) -> int:
        if v != 0:
            raise ValueError("SCORING_OFFICIAL_HOUR is pinned to 0 (02 §B6)")
        return v

    @field_validator("SUBTENSOR_RPC_MODE")
    @classmethod
    def _v_rpc_mode(cls, v: str) -> str:
        v = str(v).lower()
        if v == "sdk":
            raise ValueError("SUBTENSOR_RPC_MODE=sdk needs the bittensor package, which v1 forbids (01 §2.3)")
        if v != "jsonrpc":
            raise ValueError(f"SUBTENSOR_RPC_MODE must be 'jsonrpc', got {v!r}")
        return v

    @field_validator("TMC_SSE_BLOCKS_STREAM")
    @classmethod
    def _v_blocks_stream(cls, v: str) -> str:
        v = str(v).lower()
        if v not in {"auto", "true", "false"}:
            raise ValueError("TMC_SSE_BLOCKS_STREAM must be auto|true|false")
        return v

    @field_validator("OWNER_KEYS_SOURCE")
    @classmethod
    def _v_owner_keys(cls, v: str) -> str:
        if v not in {"auto", "list", "detail"}:
            raise ValueError("OWNER_KEYS_SOURCE must be auto|list|detail")
        return v

    @field_validator("OWNER_ACTIVITY_TIME_FORMAT")
    @classmethod
    def _v_owner_tf(cls, v: str) -> str:
        if v not in {"auto", "ms", "s", "iso"}:
            raise ValueError("OWNER_ACTIVITY_TIME_FORMAT must be auto|ms|s|iso")
        return v

    @field_validator("OWNER_AMOUNT_UNITS")
    @classmethod
    def _v_owner_units(cls, v: str) -> str:
        if v not in {"auto", "rao", "unit"}:
            raise ValueError("OWNER_AMOUNT_UNITS must be auto|rao|unit")
        return v

    @field_validator("OWNER_NEURON_SPAN")
    @classmethod
    def _v_owner_span(cls, v: str) -> str:
        if v not in {"1D", "1W", "1M"}:
            raise ValueError("OWNER_NEURON_SPAN must be 1D|1W|1M")
        return v

    @field_validator("X_PROVIDER")
    @classmethod
    def _v_x_provider(cls, v: str) -> str:
        if v not in {"null", "twitterapi_io", "official_v2"}:
            raise ValueError("X_PROVIDER must be null|twitterapi_io|official_v2")
        return v

    @field_validator("X_PRICES_JSON")
    @classmethod
    def _v_x_prices(cls, v: str) -> str:
        from app.collectors.x_providers.prices import parse_override   # 09 §5: unknown keys fail validation
        parse_override(v)
        return v

    @field_validator("ALERT_MIN_SEVERITY")
    @classmethod
    def _v_alert_sev(cls, v: str) -> str:
        if v not in {"info", "warning", "critical"}:
            raise ValueError("ALERT_MIN_SEVERITY must be info|warning|critical (02 §B2)")
        return v

    @field_validator("TERMINAL_API_URL")
    @classmethod
    def _v_terminal_url(cls, v: str) -> str:
        from urllib.parse import urlsplit
        v = v.strip()
        if v and (urlsplit(v).scheme not in ("http", "https")
                  or urlsplit(v).hostname not in ("127.0.0.1", "::1", "localhost")):
            raise ValueError("TERMINAL_API_URL must be an http(s) loopback URL or empty (14 §8.4a: wallet data stays local)")
        return v

    @field_validator("CONTROL_ALLOWED_ORIGINS")
    @classmethod
    def _v_origins(cls, v: str) -> str:
        if any(o.strip() == "*" for o in v.split(",")):
            raise ValueError("CONTROL_ALLOWED_ORIGINS must not contain '*' (14)")
        return v

    @field_validator("HEALTH_PORT")
    @classmethod
    def _v_port(cls, v: int) -> int:
        if v != 0 and not (1024 <= v <= 65535):
            raise ValueError("HEALTH_PORT must be 1024-65535 (0 = pick a free port, tests only)")
        return v

    @model_validator(mode="after")
    def _v_model(self) -> "Settings":
        from app.util.time import dow_hhmm, hhmm

        w = self._warnings
        # TMC budget (02 §F2)
        if not (1 <= self.TMC_RATE_PER_MIN <= 10):
            raise ValueError("TMC_RATE_PER_MIN must be 1..10")
        if self.TMC_RATE_BURST < 1 or self.TMC_RATE_BURST + self.TMC_RATE_PER_MIN > 10:
            raise ValueError("TMC_RATE_BURST + TMC_RATE_PER_MIN must be <= 10 (02 §F2)")
        if self.TMC_HOURLY_CAP > 600 or self.TMC_HOURLY_CAP < 0:
            raise ValueError("TMC_HOURLY_CAP must be 0..600")
        if self.TMC_MONTHLY_CAP > 100_000 or self.TMC_MONTHLY_CAP < 0:
            raise ValueError("TMC_MONTHLY_CAP must be 0..100000")
        if not (1 <= self.TMC_SSE_MAX_CONNECTIONS <= 2):
            raise ValueError("TMC_SSE_MAX_CONNECTIONS must be 1..2")
        if self.TMC_RATE_PER_MIN + self.SCRIPT_TMC_RATE_PER_MIN > 10:
            raise ValueError("TMC_RATE_PER_MIN + SCRIPT_TMC_RATE_PER_MIN must be <= 10 (16 §5.1)")
        # shutdown budget (01 §5)
        if not (self.SHUTDOWN_DB_TIMEOUT_SEC + self.SHUTDOWN_HTTP_TIMEOUT_SEC + 2 <= self.SHUTDOWN_DEADLINE_SEC < 30):
            raise ValueError("SHUTDOWN_DB + SHUTDOWN_HTTP + 2 <= SHUTDOWN_DEADLINE_SEC < 30 required")
        # caps (F2: 0 = provider disabled, never negative)
        for k in ("X_MONTHLY_CAP_USD", "LLM_MONTHLY_CAP_USD", "TAOSTATS_MONTHLY_CAP_REQ", "TAOSTATS_MONTHLY_CAP_USD"):
            if getattr(self, k) < 0:
                raise ValueError(f"{k} must be >= 0 (0 = disabled)")
        # storage (03 §5)
        if not (2 <= self.CHAIN_TICKS_RETENTION_DAYS <= 365):
            raise ValueError("CHAIN_TICKS_RETENTION_DAYS must be 2..365")
        if not (1 <= self.LLM_TEXT_RETENTION_DAYS <= 90):
            raise ValueError("LLM_TEXT_RETENTION_DAYS must be 1..90")
        if not (1 <= self.LLM_PENDING_TEXT_MAX_DAYS <= self.LLM_TEXT_RETENTION_DAYS):
            raise ValueError("LLM_PENDING_TEXT_MAX_DAYS must be 1..LLM_TEXT_RETENTION_DAYS")
        if not (8 <= self.DB_CACHE_SIZE_MB <= 1024):
            raise ValueError("DB_CACHE_SIZE_MB must be 8..1024")
        if not (0 <= self.DB_MMAP_SIZE_MB <= 4096):
            raise ValueError("DB_MMAP_SIZE_MB must be 0..4096")
        if not (1000 <= self.RETENTION_DELETE_BATCH <= 200_000):
            raise ValueError("RETENTION_DELETE_BATCH must be 1000..200000")
        if not (0 <= self.STORAGE_RETENTION_CRON_HOUR <= 23 and 0 <= self.STORAGE_RETENTION_CRON_MINUTE <= 59):
            raise ValueError("STORAGE_RETENTION_CRON_HOUR/_MINUTE out of range")
        if not (1000 <= self.WEB_STORE_TEXT_MAX_CHARS <= 200_000):
            raise ValueError("WEB_STORE_TEXT_MAX_CHARS must be 1000..200000")
        if self.PRICE_BARS_5M_RETENTION_DAYS < 7 or self.METRIC_PROVISIONAL_RETENTION_DAYS < 14:
            raise ValueError("PRICE_BARS_5M_RETENTION_DAYS >= 7 and METRIC_PROVISIONAL_RETENTION_DAYS >= 14 required")
        if (self.STORAGE_RETENTION_CRON_HOUR, self.STORAGE_RETENTION_CRON_MINUTE) >= (
                self.DB_BACKUP_CRON_HOUR, self.DB_BACKUP_CRON_MINUTE):
            w.append("retention_after_backup: storage_retention_daily does not precede backup_daily")
        # ops / backups (16 §5.1)
        if not (0 <= self.DB_BACKUP_CRON_HOUR <= 23 and 0 <= self.DB_BACKUP_CRON_MINUTE <= 59):
            raise ValueError("DB_BACKUP_CRON_HOUR/_MINUTE out of range")
        if not (1 <= self.DB_BACKUP_GZIP_LEVEL <= 9):
            raise ValueError("DB_BACKUP_GZIP_LEVEL must be 1..9")
        if not (1 <= self.DB_BACKUP_KEEP_UNCOMPRESSED <= max(1, self.DB_BACKUP_RETENTION)):
            raise ValueError("DB_BACKUP_KEEP_UNCOMPRESSED must be 1..DB_BACKUP_RETENTION")
        if self.DB_BACKUP_MIN_FREE_FACTOR < 1.0:
            raise ValueError("DB_BACKUP_MIN_FREE_FACTOR must be >= 1.0")
        if self.DB_BACKUP_MAX_TOTAL_GB < 0 or self.DB_BACKUP_MIRROR_MAX_TOTAL_GB < 0:
            raise ValueError("backup size budgets must be >= 0")
        if self.DB_BACKUP_STALE_HOURS <= self.DB_BACKUP_INTERVAL_HOURS:
            raise ValueError("DB_BACKUP_STALE_HOURS must exceed DB_BACKUP_INTERVAL_HOURS")
        if not (0 <= self.DB_BACKUP_STARTUP_DELAY_SEC <= 3600 and 0 <= self.DB_BACKUP_WAIT_MAX_SEC <= 3600):
            raise ValueError("DB_BACKUP_STARTUP_DELAY_SEC / DB_BACKUP_WAIT_MAX_SEC must be 0..3600")
        if self.DISK_FREE_CRITICAL_GB >= self.DISK_FREE_MIN_GB:
            raise ValueError("DISK_FREE_CRITICAL_GB must be < DISK_FREE_MIN_GB")
        if not (1 <= self.OPS_GUARD_INTERVAL_MIN <= 15):
            raise ValueError("OPS_GUARD_INTERVAL_MIN must be 1..15")
        if not (0 <= self.OPS_DIGEST_HOUR_UTC <= 23 and 0 <= self.OPS_DIGEST_MINUTE <= 59):
            raise ValueError("OPS_DIGEST_HOUR_UTC/_MINUTE out of range")
        if not (1 <= self.WATCHDOG_API_PROBE_FAILS <= 10):
            raise ValueError("WATCHDOG_API_PROBE_FAILS must be 1..10")
        if not (5 <= self.COST_LEDGER_INTERVAL_MIN <= 240):
            raise ValueError("COST_LEDGER_INTERVAL_MIN must be 5..240")
        if not (0.5 <= self.COST_SOFT_ALERT_PCT <= 0.99):
            raise ValueError("COST_SOFT_ALERT_PCT must be 0.5..0.99")
        if not (0.5 <= self.COST_HARD_STOP_PCT <= 1.0):
            raise ValueError("COST_HARD_STOP_PCT must be 0.5..1.0")
        if self.COST_RESERVATION_TTL_SEC < 3600:
            raise ValueError("COST_RESERVATION_TTL_SEC must be >= 3600")
        if self.COST_OVERRIDE_MAX_USD <= 0:
            raise ValueError("COST_OVERRIDE_MAX_USD must be > 0")
        if not (0.05 <= self.SCRIPT_RATE_SHARE <= 0.5):
            raise ValueError("SCRIPT_RATE_SHARE must be 0.05..0.5")
        # time formats
        for k in _HHMM_KEYS:
            hhmm(getattr(self, k))
        for k in _DOW_HHMM_KEYS:
            dow_hhmm(getattr(self, k))
        for k in _HHMM_LIST_KEYS:
            for x in getattr(self, k):
                hhmm(x)
        for k in _HOURS_LIST_KEYS:
            for x in str(getattr(self, k)).split(","):
                if not x.strip().isdigit() or not (0 <= int(x) <= 23):
                    raise ValueError(f"{k}: bad hour list {getattr(self, k)!r}")
        if self.WEB_METRICS_CRON_MINUTE >= self.SCORING_CRON_MINUTE:
            raise ValueError("WEB_METRICS_CRON_MINUTE must be < SCORING_CRON_MINUTE (07)")
        # fail-safe downgrades (each logged once at warning, 01 §2.4)
        if self.DEADMAN_ENABLED and not self.DEADMAN_PING_URL:
            self.DEADMAN_ENABLED = False
            w.append("DEADMAN_ENABLED forced off: DEADMAN_PING_URL is empty")
        if self.DISCORD_GATEWAY_ENABLED and not self.DISCORD_BOT_TOKEN:
            self.DISCORD_GATEWAY_ENABLED = False
            w.append("DISCORD_GATEWAY_ENABLED forced off: DISCORD_BOT_TOKEN is empty")
        if self.WEB_SPN2_ENABLED and not (self.IA_ACCESS_KEY and self.IA_SECRET_KEY):
            self.WEB_SPN2_ENABLED = False
            w.append("WEB_SPN2_ENABLED forced off: IA_ACCESS_KEY/IA_SECRET_KEY missing")
        if not self.ONFINALITY_API_KEY and self.ONFINALITY_RATE_PER_SEC > 4.0:
            self.ONFINALITY_RATE_PER_SEC = 4.0
        # backfill (17 §5)
        if not (0.1 <= self.BLOCKMACHINE_RATE_PER_SEC <= 16.0):
            raise ValueError("BLOCKMACHINE_RATE_PER_SEC must be 0.1..16 (plan: 1,000 req/min)")
        if self.BLOCKMACHINE_MONTHLY_CAP_REQ < 0:
            raise ValueError("BLOCKMACHINE_MONTHLY_CAP_REQ must be >= 0 (0 = disabled)")
        if not (0.1 <= self.ARCHIVE_RATE_PER_SEC <= 3.0):
            raise ValueError("ARCHIVE_RATE_PER_SEC must be 0.1..3.0")
        if self.ARCHIVE_MIN_BUCKET_INTERVAL_SEC < 0.5:
            raise ValueError("ARCHIVE_MIN_BUCKET_INTERVAL_SEC must be >= 0.5")
        for u in self.BACKFILL_RPC_ENDPOINTS:
            if "lite." in u or "entrypoint-finney" in u:
                raise ValueError(f"BACKFILL_RPC_ENDPOINTS: {u.split('?')[0]!r} is not an archive (state discarded)")
        bad = set(self.BACKFILL_RPC_ERAS) - {"gap", "ledger", "pre_ledger"}
        if bad:
            raise ValueError(f"BACKFILL_RPC_ERAS: unknown eras {sorted(bad)}")
        if self.BACKFILL_RPC_STEP_SEC not in (900, 1800, 3600, 14400, 86400):
            raise ValueError("BACKFILL_RPC_STEP_SEC must be one of 900, 1800, 3600, 14400, 86400")
        if not (0 <= self.BACKFILL_RPC_SAMPLE_OFFSET_SEC <= 600):
            raise ValueError("BACKFILL_RPC_SAMPLE_OFFSET_SEC must be 0..600")
        if not (0.5 <= self.BACKFILL_RPC_PUBLIC_MAX_RPS <= 5.0):
            raise ValueError("BACKFILL_RPC_PUBLIC_MAX_RPS must be 0.5..5.0")
        if not (0 <= self.BACKFILL_RPC_CONCURRENCY <= 8):
            raise ValueError("BACKFILL_RPC_CONCURRENCY must be 0..8")
        if self.BACKFILL_RPC_PRICE_METHOD not in ("auto", "swap_sqrt", "reserve_ratio"):
            raise ValueError("BACKFILL_RPC_PRICE_METHOD must be auto|swap_sqrt|reserve_ratio")
        if self.BACKFILL_RPC_MODE not in ("runtime_api", "storage"):
            raise ValueError("BACKFILL_RPC_MODE must be runtime_api|storage")
        if self.LEDGER_IMPORT_THIN_SEC != 0 and not (60 <= self.LEDGER_IMPORT_THIN_SEC <= 3600):
            raise ValueError("LEDGER_IMPORT_THIN_SEC must be 0 or 60..3600")
        if not (1 <= self.BACKFILL_TMC_MAX_PER_HOUR <= max(1, self.TMC_HOURLY_CAP - 320)):
            raise ValueError("BACKFILL_TMC_MAX_PER_HOUR must be 1..TMC_HOURLY_CAP-320 (05 §6.3)")
        if not (1 <= self.BACKFILL_TMC_MIN_5M_PER_1H <= 12 and 1 <= self.BACKFILL_TMC_MIN_5M_PER_1D <= 288):
            raise ValueError("BACKFILL_TMC_MIN_5M_PER_1H 1..12 and _PER_1D 1..288 required")
        if self.PI_TRADER_COLDKEY and not re.match(r"^5[1-9A-HJ-NP-Za-km-z]{47}$", self.PI_TRADER_COLDKEY):
            raise ValueError("PI_TRADER_COLDKEY must be an SS58 address (public key only)")
        if self.DISCORD_BOT_TOKEN and not self.DISCORD_USER_AGENT.startswith("DiscordBot ("):
            w.append("DISCORD_USER_AGENT should start with 'DiscordBot (' for Bot-token calls (08)")
        if "TaoSentinel/" not in self.WEB_USER_AGENT:
            raise ValueError("WEB_USER_AGENT must contain 'TaoSentinel/' (07)")
        # web presence (07 §5)
        for key, lo, hi in (("WEB_TICK_MINUTES", 5, 360), ("WEB_LATEST_MAX_AGE_DAYS", 1, 7), ("WEB_CONCURRENCY", 1, 16),
                            ("WEB_TIMEOUT_S", 3, 30), ("WEB_CONNECT_TIMEOUT_S", 1, 15), ("WEB_DNS_TIMEOUT_S", 1, 15),
                            ("WEB_MAX_REDIRECTS", 1, 10), ("WEB_CONTENT_RESET_RATIO", 0, 1),
                            ("WEB_FRESHNESS_MIN_DELTA", 0, 0.5), ("WEB_SPN2_WEEKDAY", 0, 6), ("WEB_SPN2_HOUR_UTC", 0, 23)):
            if not (lo <= getattr(self, key) <= hi):
                raise ValueError(f"{key} must be {lo}..{hi} (07 §5)")
        if self.WEB_PER_URL_MIN_INTERVAL_H < 12 or self.WEB_SAME_HOST_GAP_S < 1 or self.WEB_RETRY_DELAY_S < 0:
            raise ValueError("WEB_PER_URL_MIN_INTERVAL_H >= 12, WEB_SAME_HOST_GAP_S >= 1, WEB_RETRY_DELAY_S >= 0 (07 §5)")
        if 3 <= self.WEB_SPN2_HOUR_UTC <= 5:
            raise ValueError("WEB_SPN2_HOUR_UTC must not be 3..5 (retention/backup/reboot/quant_weekly window, 07 §5)")
        if "0" not in [x.strip() for x in str(self.WEB_METRICS_CRON_HOURS).split(",")]:
            raise ValueError("WEB_METRICS_CRON_HOURS must contain 0 (the official 00:05Z run, 07 §2.7)")
        for key in ("WEB_EXCLUDED_HOSTS", "WEB_PARKING_HOSTS", "WEB_PARKED_STRONG_PHRASES", "WEB_PARKED_WEAK_KEYWORDS",
                    "WEB_HIJACK_KEYWORDS", "WEB_SHARED_PLATFORM_HOSTS"):
            setattr(self, key, [str(x).strip().lower() for x in getattr(self, key) if str(x).strip()])
        # validation harness (13 §5)
        if self.QUANT_MIN_NAMES < 10:
            raise ValueError("QUANT_MIN_NAMES must be >= 10 (13 §5: spearman_r degenerates below 5 names)")
        if self.QUANT_MIN_IC_DATES < 2 or self.QUANT_SNAPSHOT_KEEP < 1 or self.QUANT_ZERO_RUN_DAYS < 1:
            raise ValueError("QUANT_MIN_IC_DATES >= 2, QUANT_SNAPSHOT_KEEP >= 1, QUANT_ZERO_RUN_DAYS >= 1 (13 §5)")
        if self.QUANT_PIT_MODE not in ("strict", "reconstructed"):
            raise ValueError("QUANT_PIT_MODE must be strict|reconstructed (13 §5)")
        for key in ("QUANT_RESET_DROP_RATIO", "QUANT_DEATH_FLOOR_RETURN"):
            if not (0 < getattr(self, key) < 1):
                raise ValueError(f"{key} must be in (0, 1) (13 §5)")
        if self.QUANT_GRID_SURVIVORSHIP_S % 3600 or self.QUANT_GRID_LIVE_S % 3600:
            raise ValueError("QUANT_GRID_*_S must be multiples of 3600 (13 §5)")
        if self.QUANT_LOOKBACK_BARS < 1 or self.QUANT_MIN_DEPTH_TAO < 0 or self.QUANT_TRADE_TAO < 0:
            raise ValueError("QUANT_LOOKBACK_BARS >= 1, QUANT_MIN_DEPTH_TAO >= 0, QUANT_TRADE_TAO >= 0 (13 §5)")
        for key in ("QUANT_HORIZONS_24H", "QUANT_HORIZONS_4H"):
            try:
                hz = [int(x) for x in str(getattr(self, key)).split(",") if x.strip()]
            except ValueError:
                hz = []
            if not hz or min(hz) < 1:
                raise ValueError(f"{key} must be comma-separated ints >= 1 (13 §5)")
        # workbench (19 §4)
        if self.WB_EMA_PERIOD < 2 or self.WB_EMA_WINDOW_BARS < 2 * self.WB_EMA_PERIOD:
            raise ValueError("WB_EMA_WINDOW_BARS must be >= 2 * WB_EMA_PERIOD (19 §2.2)")
        if not (2 <= self.WB_EMA_MIN_BARS <= self.WB_EMA_WINDOW_BARS):
            raise ValueError("WB_EMA_MIN_BARS must be 2..WB_EMA_WINDOW_BARS")
        if not set(self.WB_EMA_TIMEFRAMES) <= {"1h", "1d"}:
            raise ValueError("WB_EMA_TIMEFRAMES must be a subset of ['1h','1d'] (19 §4)")
        if self.WB_EMA_BAND_MODE != "sigma":
            raise ValueError("WB_EMA_BAND_MODE: only 'sigma' is implemented in v1 (19 §2.3)")
        if not (0.1 <= self.WB_EMA_BAND_K <= 10):
            raise ValueError("WB_EMA_BAND_K must be 0.1..10")
        if not set(self.WB_DEREG_HORIZONS_DAYS) <= {30, 90}:
            raise ValueError("WB_DEREG_HORIZONS_DAYS must be a subset of [30, 90] (one catalog key per horizon)")
        if not (7 <= self.WB_DEREG_REG_GAP_WINDOW_DAYS <= 365) or self.WB_DEREG_REG_GAP_FALLBACK_DAYS <= 0:
            raise ValueError("WB_DEREG_REG_GAP_WINDOW_DAYS 7..365 and WB_DEREG_REG_GAP_FALLBACK_DAYS > 0 required")
        if not (1 <= self.WB_GITHUB_PROBE_PER_RUN <= 50):
            raise ValueError("WB_GITHUB_PROBE_PER_RUN must be 1..50 (no-token bucket 50/h)")
        # GitHub (06 §5)
        env_text = _env_file_text()
        if not self.GITHUB_TOKEN:
            # an empty GITHUB_TOKEN= line wins the alias lookup; fall back to a non-empty GITHUB_PAT (06 §5)
            pat = os.environ.get("GITHUB_PAT") or _env_value(env_text, "GITHUB_PAT")
            if pat:
                self.GITHUB_TOKEN = pat
        if not (1 <= self.GITHUB_MAX_CONCURRENCY <= 8):
            raise ValueError("GITHUB_MAX_CONCURRENCY must be 1..8 (01 max_connections=8)")
        if not (1 <= self.GITHUB_RATE_PER_HOUR <= 4500):
            raise ValueError("GITHUB_RATE_PER_HOUR must be 1..4500 (PAT window 5,000/h, 06 §6)")
        if not (60 <= self.GITHUB_RESOLVE_INTERVAL_MIN <= 1440):
            raise ValueError("GITHUB_RESOLVE_INTERVAL_MIN must be 60..1440")
        if self.GITHUB_ACTIVITY_LOOKBACK_DAYS < 90:
            raise ValueError("GITHUB_ACTIVITY_LOOKBACK_DAYS must be >= 90 (bus factor / commit_days need 90 d)")
        if not (1 <= self.GITHUB_ACTIVITY_MAX_REPOS_PER_OCCUPANT <= 30):
            raise ValueError("GITHUB_ACTIVITY_MAX_REPOS_PER_OCCUPANT must be 1..30")
        if not (1 <= self.GITHUB_ACTIVITY_STALE_DAYS <= 3):
            raise ValueError("GITHUB_ACTIVITY_STALE_DAYS must be 1..3 (02 §C6 freshness window)")
        if self.SPEC_VERSION == "ts-1.0" and (self.GITHUB_GONE_GRACE_DAYS != 14 or self.GITHUB_FTE_COMMIT_DAYS_28D != 10):
            raise ValueError("GITHUB_GONE_GRACE_DAYS=14 and GITHUB_FTE_COMMIT_DAYS_28D=10 are pinned for ts-1.0 (06 §5)")
        if self.GITHUB_HTML_PROBE_PACING_SEC < 0.8:
            raise ValueError("GITHUB_HTML_PROBE_PACING_SEC must be >= 0.8 (06 §5)")
        mh, mm = hhmm(self.GITHUB_METRICS_CRON)
        ah, am = hhmm(self.GITHUB_ACTIVITY_CRON)
        if not ((mh * 60 + mm) - (ah * 60 + am) >= 120 and mh * 60 + mm <= 23 * 60 + 40):
            raise ValueError("GITHUB_METRICS_CRON must be >= GITHUB_ACTIVITY_CRON + 2 h and <= 23:40 (06 §5)")
        if self.GITHUB_TOKEN_EXPIRES_AT and not re.match(r"^\d{4}-\d{2}-\d{2}$", self.GITHUB_TOKEN_EXPIRES_AT):
            raise ValueError("GITHUB_TOKEN_EXPIRES_AT must be YYYY-MM-DD (the PAT's expiry date) or empty")
        if not (1 <= self.WB_VISION_MAX_IMAGE_MB <= 20) or self.WB_VISION_EST_USD <= 0:
            raise ValueError("WB_VISION_MAX_IMAGE_MB 1..20 and WB_VISION_EST_USD > 0 required")
        # 11 §5 (Phase 7) validators
        if self.TEAM_VERIFY_MIN_SOURCES < 2:
            raise ValueError("TEAM_VERIFY_MIN_SOURCES must be >= 2 (two-source policy floor, 11 §2.2.2)")
        if not (30 <= self.TEAM_REVERIFY_DAYS <= 365) or self.TEAM_VERIFY_STALE_DAYS < self.TEAM_REVERIFY_DAYS:
            raise ValueError("TEAM_REVERIFY_DAYS must be 30..365 and TEAM_VERIFY_STALE_DAYS >= TEAM_REVERIFY_DAYS")
        if not (20 <= self.TEAM_ANON_MAX_CHARS <= 2000) or not (2 <= self.TEAM_NAME_MAX_TOKENS <= 8):
            raise ValueError("TEAM_ANON_MAX_CHARS must be 20..2000 and TEAM_NAME_MAX_TOKENS 2..8")
        if self.OWNER_COLLECT_CRON != "20:30":
            raise ValueError("OWNER_COLLECT_CRON is pinned to 20:30 (02 §E3/§G3)")
        if not (1 <= self.OWNER_TMC_MAX_PER_MIN <= 5) or not (100 <= self.OWNER_TMC_MAX_PER_RUN <= 518):
            raise ValueError("OWNER_TMC_MAX_PER_MIN must be 1..5 and OWNER_TMC_MAX_PER_RUN 100..518 (02 §G3)")
        if not (10 <= self.OWNER_ACTIVITY_PAGE_LIMIT <= 200) or self.OWNER_ACTIVITY_OVERLAP_SEC < 600 \
                or self.OWNER_ACTIVITY_BACKFILL_DAYS < 31 or self.OWNER_ACTIVITY_MAX_PAGES < 1:
            raise ValueError("OWNER_ACTIVITY_PAGE_LIMIT 10..200, _OVERLAP_SEC >= 600, _BACKFILL_DAYS >= 31, _MAX_PAGES >= 1")
        if self.OWNER_STAKE_REMOVED_ENABLED:
            raise ValueError("OWNER_STAKE_REMOVED_ENABLED must stay false: stake-removed rows carry no coldkey (11 §2.3.1)")
        if not (0 <= self.OWNER_PERP_OFF_MATCH_TOL <= 0.5):
            raise ValueError("OWNER_PERP_OFF_MATCH_TOL must be 0..0.5")
        if self.OWNER_STREAM_ENABLED and self.TMC_SSE_BLOCKS_STREAM == "true":
            raise ValueError("OWNER_STREAM_ENABLED needs the second SSE slot; TMC_SSE_BLOCKS_STREAM=true holds it (11 §2.3)")
        # deprecated alias / read-only alias
        if "TAOSTATS_MONTHLY_CALL_CAP" in os.environ or "TAOSTATS_MONTHLY_CALL_CAP=" in env_text:
            w.append("TAOSTATS_MONTHLY_CALL_CAP is deprecated; use TAOSTATS_MONTHLY_CAP_REQ (02 §F2)")
        sv = os.environ.get("SCORING_SPEC_VERSION") or _env_value(env_text, "SCORING_SPEC_VERSION")
        if sv and sv != self.SPEC_VERSION:
            w.append(f"SCORING_SPEC_VERSION={sv!r} ignored: it is a read-only alias of SPEC_VERSION ({self.SPEC_VERSION!r})")
        for msg in w:
            _log.warning(msg)
        return self

    # ── aliases & derived helpers (not env keys) ─────────────────────────────────────────────────────────────
    @property
    def SCORING_SPEC_VERSION(self) -> str:  # noqa: N802 — read-only alias (F2)
        return self.SPEC_VERSION

    @property
    def config_warnings(self) -> list[str]:
        return list(self._warnings)

    def path(self, rel: str | Path) -> Path:
        """Relative → APP_ROOT/rel; absolute unchanged (never cwd-relative)."""
        p = Path(rel)
        return p if p.is_absolute() else (APP_ROOT / p)

    def x_active(self) -> bool:
        if not self.X_ENABLED or self.X_PROVIDER == "null" or self.X_MONTHLY_CAP_USD <= 0:
            return False
        if self.X_PROVIDER == "twitterapi_io":
            return bool(self.TWITTERAPI_IO_KEY)
        if self.X_PROVIDER == "official_v2":
            return bool(self.X_BEARER_TOKEN)
        return False

    def llm_active(self) -> bool:
        return bool(self.LLM_ENABLED and self.ANTHROPIC_API_KEY and self.LLM_MONTHLY_CAP_USD > 0)

    def taostats_active(self) -> bool:
        return bool(self.TAOSTATS_ENABLED and self.TAOSTATS_API_KEY and self.TAOSTATS_MONTHLY_CAP_REQ > 0)

    def telegram_configured(self) -> bool:
        return bool(self.TELEGRAM_ENABLED and self.TELEGRAM_BOT_TOKEN and self.TELEGRAM_CHAT_ID)

    def public_dict(self) -> dict[str, Any]:
        """Every field; secrets masked (empty secrets stay empty so 'unset' is visible)."""
        out: dict[str, Any] = {}
        for k in type(self).model_fields:
            v = getattr(self, k)
            out[k] = (MASK if v else v) if _is_secret(k) else v
        out["SCORING_SPEC_VERSION"] = self.SCORING_SPEC_VERSION
        return out


def _env_file_text() -> str:
    try:
        p = Path(os.environ.get("TAOSENTINEL_ENV_FILE", str(APP_ROOT / ".env")))
        return p.read_text(encoding="utf-8") if p.is_file() else ""
    except OSError:
        return ""


def _env_value(text: str, key: str) -> str | None:
    for line in text.splitlines():
        line = line.strip()
        if line.startswith(key + "="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def restart_required(key: str) -> bool:
    return key in _RESTART_EXPLICIT or key.endswith(_RESTART_SUFFIXES)


def is_secret(key: str) -> bool:
    return _is_secret(key)


settings: Settings = Settings()


def get_settings() -> Settings:
    return settings


def reload_settings() -> tuple[Settings, list[str]]:
    """Rebind the module attribute to a fresh Settings(); return (new, changed_keys)."""
    global settings
    old, new = settings, Settings()
    changed = [k for k in Settings.model_fields if getattr(old, k) != getattr(new, k)]
    settings = new
    try:
        from app.logging.logger import logger
        logger.set_level(new.LOG_LEVEL)
    except Exception:  # pragma: no cover - logger import problems must not break a reload
        pass
    try:
        from app.runtime import runtime
        if runtime is not None and runtime.limiter is not None:
            runtime.limiter.reload()
    except Exception:  # pragma: no cover
        pass
    return new, changed
