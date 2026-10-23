"""Jobs: ``JobSpec``/``JOB_TABLE``, the never-raise ``CollectorJob`` wrapper, ``LongTask``, scheduler, liveness.

Spec 01 §2.6–§2.8 (mechanics), 02 §E1–§E4, 16 §2.5 (liveness rules). ``JOB_TABLE`` mirrors every owner spec's job
rows verbatim (57 entries = 54 jobs + 3 LongTasks; 19's workbench rows added 2026-10-16); a later phase only adds modules, never rows.
"""
from __future__ import annotations

import asyncio
import importlib
import json
import random
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Literal

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

import app.config as app_config
from app.logging.logger import logger
from app.util.time import dow_hhmm, hhmm, in_utc_windows, now_ts, parse_dt, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.config import Settings
    from app.logging.logger import StructuredLogger
    from app.runtime import Runtime

UTC = timezone.utc
TriggerFn = Callable[["Settings"], BaseTrigger]


# ── outcomes & context ───────────────────────────────────────────────────────────────────────────────────────
@dataclass(slots=True)
class JobOutcome:
    rows: int = 0
    cost_usd: float = 0.0
    note: str | None = None
    meta: dict | None = None


@dataclass(slots=True)
class RunContext:
    run_id: str
    started_at: int
    trigger: str
    rt: "Runtime"
    settings: "Settings"
    log: "StructuredLogger"
    rows_committed: int = 0

    @property
    def started_dt(self) -> datetime:
        return parse_dt(self.started_at)


# ── JobSpec ──────────────────────────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class JobSpec:
    id: str
    owner: str
    factory: str
    kind: Literal["interval", "cron", "manual", "longtask"]
    phase: int
    trigger: TriggerFn | None = None
    enabled_flags: tuple[str, ...] = ()
    enabled_fn: Callable[["Settings"], bool] | None = None
    misfire_grace_sec: int | None = 300
    timeout_sec: int | None = 1800
    providers: tuple[str, ...] = ()
    paid: bool = False
    ops_exempt: bool = False
    min_gap_factor: float = 0.5
    startup_catchup: bool = False
    catchup_avoid_utc: tuple[str, ...] = ()
    catchup_only_utc: str | None = None
    first_run: Literal["stagger", "ops"] | int = "stagger"
    jitter_sec: int | None = None
    liveness_max_age_sec: int | None = None
    fail_alert_threshold: int | None = None
    fail_alert_key: str | None = None
    timeout_key: str | None = None        # Settings key overriding timeout_sec at run time (13 §8.8)
    trigger_desc: str = ""


class JobSpecMismatch(RuntimeError):
    pass


# ── trigger helpers (UTC; Settings keys are read when setup_scheduler runs) ───────────────────────────────────
_UNITS = {"sec": "seconds", "min": "minutes", "hour": "hours"}


def every(key: str, unit: Literal["sec", "min", "hour"]) -> TriggerFn:
    return lambda s: IntervalTrigger(**{_UNITS[unit]: int(getattr(s, key))}, timezone=UTC)


def fixed_every(minutes: int) -> TriggerFn:
    return lambda s: IntervalTrigger(minutes=minutes, timezone=UTC)


def daily_at(key: str) -> TriggerFn:
    def _t(s: "Settings") -> BaseTrigger:
        h, m = hhmm(getattr(s, key))
        return CronTrigger(hour=h, minute=m, timezone=UTC)
    return _t


def times_at(key: str) -> TriggerFn:
    """List of HH:MM sharing one minute (e.g. ["00:05","06:05",…]) → CronTrigger(hour="0,6,…", minute=…)."""
    def _t(s: "Settings") -> BaseTrigger:
        pairs = [hhmm(x) for x in getattr(s, key)]
        minutes = {m for _, m in pairs}
        if len(minutes) != 1:
            raise ValueError(f"{key}: all times must share one minute")
        return CronTrigger(hour=",".join(str(h) for h, _ in pairs), minute=minutes.pop(), timezone=UTC)
    return _t


def hours_at(hours_key: str, minute: int | str) -> TriggerFn:
    def _t(s: "Settings") -> BaseTrigger:
        mm = int(getattr(s, minute)) if isinstance(minute, str) else int(minute)
        return CronTrigger(hour=str(getattr(s, hours_key)), minute=mm, timezone=UTC)
    return _t


def weekly_at(key: str) -> TriggerFn:
    def _t(s: "Settings") -> BaseTrigger:
        d, h, m = dow_hhmm(getattr(s, key))
        return CronTrigger(day_of_week=d, hour=h, minute=m, timezone=UTC)
    return _t


def cron(**fields: int | str) -> TriggerFn:
    def _t(s: "Settings") -> BaseTrigger:
        f = {k: (getattr(s, v[1:]) if isinstance(v, str) and v.startswith("$") else v) for k, v in fields.items()}
        return CronTrigger(timezone=UTC, **f)
    return _t


def manual_trigger() -> IntervalTrigger:
    return IntervalTrigger(weeks=520, timezone=UTC)


# ── JOB_TABLE (01 §2.7; owner rows win, 02 §E2) ──────────────────────────────────────────────────────────────
def _x_active(s: "Settings") -> bool:
    return s.x_active()


def _no_github_token(s: "Settings") -> bool:
    return not s.GITHUB_TOKEN


J = JobSpec
JOB_TABLE: tuple[JobSpec, ...] = (
    # Phase 0 — 01, 03, 16, 14's alert_dispatch (01 body)
    J("log_retention_daily", "01", "logging.retention:LogRetentionJob", "interval", 0, fixed_every(1440),
      ("LOG_RETENTION_ENABLED",), misfire_grace_sec=3600, timeout_sec=120, ops_exempt=True, first_run="ops",
      trigger_desc="every 1440 min"),
    J("storage_retention_daily", "03", "storage.retention:RetentionJob", "cron", 0,
      cron(hour="$STORAGE_RETENTION_CRON_HOUR", minute="$STORAGE_RETENTION_CRON_MINUTE"),
      ("STORAGE_RETENTION_ENABLED",), misfire_grace_sec=3600, timeout_sec=3600, ops_exempt=True, min_gap_factor=0,
      trigger_desc="03:00Z"),
    J("backup_daily", "16", "storage.backup:BackupJob", "cron", 0,
      cron(hour="$DB_BACKUP_CRON_HOUR", minute="$DB_BACKUP_CRON_MINUTE"), ("DB_BACKUP_ENABLED",),
      misfire_grace_sec=3600, timeout_sec=5400, ops_exempt=True, min_gap_factor=0, trigger_desc="03:30Z"),
    J("ops_guard_5m", "16", "ops.guard:OpsGuardJob", "interval", 0, every("OPS_GUARD_INTERVAL_MIN", "min"),
      ("OPS_GUARD_ENABLED",), misfire_grace_sec=120, timeout_sec=60, ops_exempt=True, min_gap_factor=0,
      first_run="ops", trigger_desc="every 5 min"),
    J("ops_digest_daily", "16", "ops.digest:OpsDigestJob", "cron", 0,
      cron(hour="$OPS_DIGEST_HOUR_UTC", minute="$OPS_DIGEST_MINUTE"), ("OPS_DIGEST_ENABLED",),
      misfire_grace_sec=3600, timeout_sec=120, ops_exempt=True, min_gap_factor=0, trigger_desc="09:00Z"),
    J("cost_ledger_hourly", "16", "costs:CostLedgerJob", "interval", 0, every("COST_LEDGER_INTERVAL_MIN", "min"),
      ("COST_LEDGER_ENABLED",), misfire_grace_sec=600, timeout_sec=120, ops_exempt=True, min_gap_factor=0,
      first_run="ops", trigger_desc="every 60 min"),
    J("alert_dispatch", "14", "alerts.dispatch:AlertDispatchJob", "interval", 0,
      every("ALERT_DISPATCH_INTERVAL_SEC", "sec"), ("ALERTS_ENABLED",), misfire_grace_sec=120, timeout_sec=50,
      providers=("telegram",), ops_exempt=True, min_gap_factor=0, first_run="ops", trigger_desc="every 60 s"),
    # Phase 1 — 04
    J("registry_occupants", "04", "registry.jobs:RegistryOccupantsJob", "interval", 1,
      every("REGISTRY_OCCUPANTS_INTERVAL_MIN", "min"), ("REGISTRY_ENABLED",), misfire_grace_sec=3600,
      timeout_sec=600, providers=("tmc",), trigger_desc="every 360 min"),
    J("registry_identity", "04", "registry.jobs:RegistryIdentityJob", "cron", 1, daily_at("REGISTRY_IDENTITY_AT_UTC"),
      ("REGISTRY_ENABLED",), misfire_grace_sec=21600, timeout_sec=1800, providers=("subtensor_rpc", "tmc"),
      min_gap_factor=0, startup_catchup=True, trigger_desc="02:40Z"),
    # Phase 2 — 05
    J("chain_sse", "05", "collectors.chain_tmc:ChainSseTask", "longtask", 2, None, ("TMC_ENABLED", "TMC_SSE_ENABLED"),
      misfire_grace_sec=None, timeout_sec=None, providers=("tmc_sse", "tmc"), min_gap_factor=0,
      liveness_max_age_sec=900, trigger_desc="continuous"),
    J("tmc_hourly", "05", "collectors.chain_tmc_rest:TmcHourlyJob", "cron", 2, cron(minute="$TMC_HOURLY_MINUTE"),
      ("TMC_ENABLED", "TMC_HOURLY_ENABLED"), misfire_grace_sec=1800, timeout_sec=300, providers=("tmc",),
      min_gap_factor=0, trigger_desc="hourly :07"),
    J("tmc_daily", "05", "collectors.chain_tmc_rest:TmcDailyJob", "cron", 2, daily_at("TMC_DAILY_AT_UTC"),
      ("TMC_ENABLED", "TMC_DAILY_ENABLED"), misfire_grace_sec=3600, timeout_sec=5400, providers=("tmc",),
      min_gap_factor=0, startup_catchup=True, catchup_avoid_utc=("20:25-23:35",), trigger_desc="02:10Z"),
    J("tmc_weekly", "05", "collectors.chain_tmc_rest:TmcWeeklyJob", "cron", 2, daily_at("TMC_WEEKLY_AT_UTC"),
      ("TMC_ENABLED", "TMC_WEEKLY_ENABLED"), misfire_grace_sec=3600, timeout_sec=3600, providers=("tmc",),
      min_gap_factor=0, trigger_desc="01:10Z daily (1/7 of netuids)"),
    J("chain_features", "05", "features.chain_features:ChainFeaturesJob", "cron", 2, times_at("CHAIN_FEATURES_AT_UTC"),
      ("CHAIN_FEATURES_ENABLED",), misfire_grace_sec=10800, timeout_sec=900, min_gap_factor=0, startup_catchup=True,
      trigger_desc="00:05/06:05/12:05/18:05Z"),
    J("chain_rpc_hourly", "05", "collectors.chain_rpc:ChainRpcHourlyJob", "cron", 2, cron(minute=1),
      ("SUBTENSOR_RPC_ENABLED",), misfire_grace_sec=1800, timeout_sec=600, providers=("subtensor_rpc",),
      min_gap_factor=0, trigger_desc="hourly :01"),
    J("chain_rpc_metagraph", "05", "collectors.chain_rpc:ChainRpcMetagraphJob", "cron", 2,
      cron(hour="0,6,12,18", minute=2), ("SUBTENSOR_RPC_ENABLED", "SUBTENSOR_METAGRAPH_ENABLED"),
      misfire_grace_sec=3600, timeout_sec=1800, providers=("subtensor_rpc",), min_gap_factor=0,
      trigger_desc="00:02/06:02/12:02/18:02Z"),
    # Phase 3 — 17
    J("backfill_tmc", "17", "backfill.tmc_backfill:BackfillTmcJob", "manual", 3, None, ("BACKFILL_TMC_ENABLED",),
      misfire_grace_sec=None, timeout_sec=14400, providers=("tmc",), min_gap_factor=0, trigger_desc="manual"),
    # Phase 4 — 06 (+17)
    # Workbench track (19 §5): declared at phase 3, the phase whose data they read
    J("wb_technicals", "19", "features.technicals:TechnicalsJob", "cron", 3, cron(minute=12), ("WB_ENABLED",),
      misfire_grace_sec=1800, timeout_sec=600, min_gap_factor=0, trigger_desc="hourly :12"),
    J("wb_dereg", "19", "features.dereg_distance:DeregDistanceJob", "cron", 3, cron(minute=9), ("WB_ENABLED",),
      misfire_grace_sec=1800, timeout_sec=600, min_gap_factor=0, trigger_desc="hourly :09"),
    J("chain_constants_probe", "19", "collectors.chain_probe:ChainConstantsProbeJob", "cron", 3,
      cron(hour=1, minute=20), ("WB_ENABLED",), misfire_grace_sec=21600, timeout_sec=300,
      providers=("subtensor_rpc",), min_gap_factor=0, trigger_desc="01:20Z"),
    J("wb_github_probe", "19", "collectors.wb_github_probe:WbGithubProbeJob", "cron", 3, cron(minute=25),
      ("WB_ENABLED", "WB_GITHUB_PROBE_ENABLED"), _no_github_token, misfire_grace_sec=1800, timeout_sec=3300,
      providers=("github",), min_gap_factor=0, trigger_desc="hourly :25 (only while GITHUB_TOKEN is empty)"),
    J("github_resolve", "06", "collectors.github.jobs:GithubResolveJob", "interval", 4,
      every("GITHUB_RESOLVE_INTERVAL_MIN", "min"), ("GITHUB_ENABLED",), misfire_grace_sec=1800, timeout_sec=1800,
      providers=("github",), trigger_desc="every 360 min"),
    J("github_org_enum", "06", "collectors.github.jobs:GithubOrgEnumJob", "cron", 4, daily_at("GITHUB_ORG_ENUM_CRON"),
      ("GITHUB_ENABLED",), misfire_grace_sec=3600, timeout_sec=1800, providers=("github",), min_gap_factor=0,
      startup_catchup=True, trigger_desc="20:00Z"),
    J("github_activity", "06", "collectors.github.jobs:GithubActivityJob", "cron", 4, daily_at("GITHUB_ACTIVITY_CRON"),
      ("GITHUB_ENABLED",), misfire_grace_sec=3600, timeout_sec=10500, providers=("github",), min_gap_factor=0,
      startup_catchup=True, trigger_desc="20:30Z"),
    J("github_metrics", "06", "collectors.github.jobs:GithubMetricsJob", "cron", 4, daily_at("GITHUB_METRICS_CRON"),
      ("GITHUB_ENABLED",), misfire_grace_sec=3600, timeout_sec=1800, min_gap_factor=0, startup_catchup=True,
      catchup_avoid_utc=("18:00-23:30",), trigger_desc="23:30Z"),
    J("github_html_probe", "06", "collectors.github.jobs:GithubHtmlProbeJob", "cron", 4,
      daily_at("GITHUB_HTML_PROBE_CRON"), ("GITHUB_ENABLED",), misfire_grace_sec=3600, timeout_sec=3600,
      providers=("github_html", "tmc"), min_gap_factor=0, trigger_desc="03:00Z"),
    J("backfill_github", "17", "backfill.github_backfill:BackfillGithubJob", "manual", 4, None,
      ("BACKFILL_GITHUB_ENABLED",), misfire_grace_sec=None, timeout_sec=14400, providers=("github",),
      min_gap_factor=0, trigger_desc="manual"),
    # Phase 5 — 07
    J("web_check", "07", "collectors.web:WebCheckJob", "interval", 5, every("WEB_TICK_MINUTES", "min"),
      ("WEB_ENABLED",), misfire_grace_sec=900, timeout_sec=1500, providers=("web", "wayback"),
      trigger_desc="every 30 min"),
    J("web_metrics", "07", "collectors.web:WebMetricsJob", "cron", 5,
      hours_at("WEB_METRICS_CRON_HOURS", "WEB_METRICS_CRON_MINUTE"), ("WEB_ENABLED",), misfire_grace_sec=3600,
      timeout_sec=600, min_gap_factor=0, trigger_desc="00:05/06:05/12:05/18:05Z"),
    J("web_spn2", "07", "collectors.web:WebSpn2Job", "cron", 5,
      cron(day_of_week="$WEB_SPN2_WEEKDAY", hour="$WEB_SPN2_HOUR_UTC", minute=0), ("WEB_SPN2_ENABLED",),
      misfire_grace_sec=21600, timeout_sec=3600, providers=("wayback",), min_gap_factor=0,
      trigger_desc="Sun 07:00Z"),
    # Phase 6 — 08
    J("discord_6h", "08", "collectors.discord:DiscordInvitesJob", "cron", 6,
      hours_at("DISCORD_CRON_HOURS", "DISCORD_CRON_MINUTE"), ("DISCORD_ENABLED",), misfire_grace_sec=1800,
      timeout_sec=1800, providers=("discord",), min_gap_factor=0, liveness_max_age_sec=45120,
      trigger_desc="00/06/12/18:00Z"),
    J("discord_widget_1h", "08", "collectors.discord:DiscordWidgetJob", "interval", 6,
      every("DISCORD_WIDGET_INTERVAL_MIN", "min"), ("DISCORD_WIDGET_ENABLED",), misfire_grace_sec=600,
      timeout_sec=600, providers=("discord",), trigger_desc="every 60 min"),
    J("telegram_daily", "08", "collectors.telegram:TelegramCountsJob", "interval", 6,
      every("TELEGRAM_COUNTS_INTERVAL_MIN", "min"), ("TELEGRAM_COUNTS_ENABLED",), misfire_grace_sec=3600,
      timeout_sec=1800, providers=("tme",), trigger_desc="every 1440 min"),
    J("discord_gateway", "08", "collectors.discord_gateway:DiscordGatewayTask", "longtask", 6, None,
      ("DISCORD_GATEWAY_ENABLED",), misfire_grace_sec=None, timeout_sec=None, min_gap_factor=0,
      liveness_max_age_sec=600, trigger_desc="continuous"),
    # Phase 7 — 11
    J("owner_collect_daily", "11", "integrity.owner_collect:OwnerCollectJob", "cron", 7, daily_at("OWNER_COLLECT_CRON"),
      ("OWNER_ENABLED",), misfire_grace_sec=3600, timeout_sec=10800,
      providers=("tmc", "rufus", "subnetradar", "taostats"), min_gap_factor=0, startup_catchup=True,
      catchup_only_utc="20:30-23:15", liveness_max_age_sec=108000, trigger_desc="20:30Z"),
    J("owner_watch_hourly", "11", "integrity.owner_watch:OwnerWatchJob", "cron", 7, cron(minute=12),
      ("OWNER_WATCH_HOURLY_ENABLED",), misfire_grace_sec=1800, timeout_sec=300, min_gap_factor=0,
      liveness_max_age_sec=10800, trigger_desc="hourly :12"),
    J("team_seed_daily", "11", "integrity.team_seed:TeamSeedJob", "cron", 7, cron(hour=4, minute=20),
      ("TEAM_SEED_ENABLED",), misfire_grace_sec=21600, timeout_sec=600, min_gap_factor=0,
      liveness_max_age_sec=108000, trigger_desc="04:20Z"),
    J("integrity_features", "11", "integrity.features:IntegrityFeaturesJob", "cron", 7,
      times_at("INTEGRITY_FEATURES_AT_UTC"), ("INTEGRITY_FEATURES_ENABLED",), misfire_grace_sec=10800,
      timeout_sec=900, min_gap_factor=0, startup_catchup=True, liveness_max_age_sec=28800,
      trigger_desc="00:07/06:07/12:07/18:07Z"),
    J("owner_rpc_seed", "11", "integrity.owner_history:OwnerRpcSeedJob", "cron", 7, cron(hour=5, minute=20),
      ("OWNER_RPC_HISTORY_SEED_ENABLED",), misfire_grace_sec=21600, timeout_sec=600, min_gap_factor=0,
      trigger_desc="05:20Z"),
    # Phase 8 — 12
    J("features_hourly", "12", "features.engine:FeaturesJob", "interval", 8, every("FEATURES_INTERVAL_MIN", "min"),
      ("FEATURES_ENABLED",), misfire_grace_sec=600, timeout_sec=1200, trigger_desc="every 60 min"),
    J("scoring_6h", "12", "scoring.engine:ScoringJob", "cron", 8, hours_at("SCORING_CRON_HOURS", "SCORING_CRON_MINUTE"),
      ("SCORING_ENABLED",), misfire_grace_sec=1800, timeout_sec=1200, min_gap_factor=0, startup_catchup=True,
      trigger_desc="00:15/06:15/12:15/18:15Z"),
    # Phase 9 — 09, 10
    J("x_team_poll", "09", "collectors.x:XTeamPollJob", "cron", 9, daily_at("X_TEAM_POLL_CRON"),
      ("X_ENABLED", "X_TEAM_ENABLED"), _x_active, misfire_grace_sec=3600, timeout_sec=1800, providers=("x",),
      paid=True, min_gap_factor=0, trigger_desc="01:30Z"),
    J("x_kol_poll", "09", "collectors.x:XKolPollJob", "cron", 9, hours_at("X_KOL_POLL_CRON_HOURS", 30),
      ("X_ENABLED", "X_KOL_ENABLED"), _x_active, misfire_grace_sec=1800, timeout_sec=900, providers=("x",),
      paid=True, min_gap_factor=0, trigger_desc="00:30/06:30/12:30/18:30Z"),
    J("x_mentions_poll", "09", "collectors.x:XMentionsPollJob", "cron", 9, daily_at("X_MENTIONS_POLL_CRON"),
      ("X_ENABLED", "X_MENTIONS_ENABLED"), _x_active, misfire_grace_sec=3600, timeout_sec=1800, providers=("x",),
      paid=True, min_gap_factor=0, trigger_desc="02:00Z"),
    J("x_profile_refresh", "09", "collectors.x:XProfileRefreshJob", "cron", 9, weekly_at("X_PROFILE_REFRESH_CRON"),
      ("X_ENABLED",), _x_active, misfire_grace_sec=21600, timeout_sec=1800, providers=("x",), paid=True,
      min_gap_factor=0, trigger_desc="Sun 02:30Z"),
    J("x_ctf_crosscheck", "09", "collectors.x_ctf:XCtfCrosscheckJob", "cron", 9, daily_at("X_CTF_CROSSCHECK_CRON"),
      ("X_CTF_CROSSCHECK_ENABLED",), misfire_grace_sec=3600, timeout_sec=900, providers=("subnetalpha",),
      min_gap_factor=0, trigger_desc="07:00Z"),
    J("rss_feeds_poll", "09", "collectors.rss:RssFeedsPollJob", "interval", 9, every("RSS_POLL_INTERVAL_HOURS", "hour"),
      ("RSS_ENABLED",), misfire_grace_sec=1800, timeout_sec=900, providers=("rss",), trigger_desc="every 6 h"),
    J("news_google_poll", "09", "collectors.news:NewsGooglePollJob", "cron", 9, daily_at("RSS_GOOGLE_NEWS_POLL_CRON"),
      ("RSS_ENABLED", "RSS_GOOGLE_NEWS_ENABLED"), misfire_grace_sec=3600, timeout_sec=1800, providers=("rss",),
      min_gap_factor=0, trigger_desc="05:00Z"),
    J("youtube_poll", "09", "collectors.youtube:YoutubePollJob", "cron", 9, daily_at("YOUTUBE_POLL_CRON"),
      ("RSS_ENABLED", "YOUTUBE_ENABLED"), misfire_grace_sec=3600, timeout_sec=900, providers=("youtube",),
      min_gap_factor=0, trigger_desc="05:30Z"),
    J("llm_classify_submit", "10", "nlp.llm_classify:LlmClassifySubmitJob", "cron", 9,
      cron(hour="0,6,12,18", minute="$LLM_BATCH_OFFSET_MIN"), ("LLM_ENABLED",), misfire_grace_sec=3600,
      timeout_sec=600, providers=("anthropic",), paid=True, min_gap_factor=0, trigger_desc="00/06/12/18:20Z"),
    J("llm_batch_poll", "10", "nlp.llm_classify:LlmBatchPollJob", "interval", 9, every("LLM_POLL_INTERVAL_MIN", "min"),
      ("LLM_ENABLED",), misfire_grace_sec=300, timeout_sec=1800, min_gap_factor=0, trigger_desc="every 5 min"),
    J("llm_rules_fallback", "10", "nlp.rules:LlmRulesFallbackJob", "interval", 9, fixed_every(60),
      ("LLM_RULES_ENABLED",), misfire_grace_sec=1800, timeout_sec=600, min_gap_factor=0, trigger_desc="every 60 min"),
    J("llm_narrative_weekly", "10", "nlp.narrative:LlmNarrativeWeeklyJob", "cron", 9,
      cron(day_of_week="$LLM_NARRATIVE_DAY_OF_WEEK", hour="$LLM_NARRATIVE_HOUR_UTC", minute=0),
      ("LLM_NARRATIVE_ENABLED",), misfire_grace_sec=21600, timeout_sec=5400, providers=("anthropic",), paid=True,
      min_gap_factor=0, trigger_desc="Sun 01:00Z"),
    # Phase 10 — 13
    J("quant_weekly", "13", "quant.weekly_job:QuantWeeklyJob", "cron", 10, weekly_at("QUANT_WEEKLY_JOB_CRON"),
      ("QUANT_ENABLED", "QUANT_WEEKLY_JOB_ENABLED"), misfire_grace_sec=3600, timeout_sec=1800, min_gap_factor=0,
      fail_alert_key="QUANT_ALERT_AFTER_FAILURES", timeout_key="QUANT_WEEKLY_JOB_TIMEOUT_S",
      trigger_desc="Sun 05:00Z"),
    # Phase 11 — 14
    J("dereg_watch", "14", "notifications.watch:DeregWatchJob", "interval", 11,
      every("DEREG_WATCH_INTERVAL_MIN", "min"), ("DEREG_WATCH_ENABLED",), misfire_grace_sec=600, timeout_sec=120,
      min_gap_factor=0, trigger_desc="every 60 min"),
    J("needs_review_digest", "14", "notifications.digest:NeedsReviewDigestJob", "cron", 11,
      cron(day_of_week="$NEEDS_REVIEW_DIGEST_DAY", hour="$NEEDS_REVIEW_DIGEST_HOUR_UTC", minute=0),
      ("NEEDS_REVIEW_DIGEST_ENABLED",), misfire_grace_sec=3600, timeout_sec=300, providers=("telegram",),
      min_gap_factor=0, trigger_desc="Mon 08:00Z"),
    J("market_brief_daily", "14", "notifications.brief:MarketBriefJob", "cron", 11, daily_at("MARKET_BRIEF_AT_UTC"),
      ("MARKET_BRIEF_ENABLED",), misfire_grace_sec=3600, timeout_sec=120, providers=("telegram",), ops_exempt=True,
      min_gap_factor=0, trigger_desc="06:00Z"),
    J("telegram_bot", "14", "notifications.bot:TelegramBotTask", "longtask", 11, None, ("TELEGRAM_COMMANDS_ENABLED",),
      misfire_grace_sec=None, timeout_sec=None, providers=("telegram",), ops_exempt=True, min_gap_factor=0,
      trigger_desc="continuous long-poll"),
)
SPEC_BY_ID: dict[str, JobSpec] = {s.id: s for s in JOB_TABLE}

RETIRED_JOB_IDS = frozenset({
    "registry_sync", "chain_rest_daily", "chain_holders_weekly", "github_repos_6h", "github_org_daily", "web_daily",
    "x_teams_daily", "x_kols_6h", "rss_6h", "llm_classify_6h", "chain_prune", "nlp_rules_fallback",
    "nlp_events_materialise", "nlp_text_retention",
})


def build_trigger(spec: JobSpec, s: "Settings | None" = None) -> BaseTrigger | None:
    s = s or app_config.settings
    if spec.kind == "manual":
        return manual_trigger()
    if spec.kind == "longtask" or spec.trigger is None:
        return None
    return spec.trigger(s)


def next_fire_times(trigger: BaseTrigger, n: int, start: datetime | None = None) -> list[datetime]:
    now = start or datetime.now(UTC)
    out: list[datetime] = []
    prev = None
    cur = now
    for _ in range(n):
        nxt = trigger.get_next_fire_time(prev, cur)
        if nxt is None:
            break
        out.append(nxt)
        prev = nxt
        cur = nxt + timedelta(microseconds=1)
    return out


def trigger_interval_min(spec: JobSpec, s: "Settings | None" = None) -> float:
    s = s or app_config.settings
    if spec.kind in ("manual", "longtask"):
        return float("inf")
    trig = build_trigger(spec, s)
    if isinstance(trig, IntervalTrigger):
        return trig.interval.total_seconds() / 60.0
    times = next_fire_times(trig, 9, datetime(2026, 1, 5, 0, 0, 30, tzinfo=UTC))   # a fixed Monday: deterministic
    gaps = [(b - a).total_seconds() / 60.0 for a, b in zip(times, times[1:])]
    return max(gaps) if gaps else float("inf")


def spec_liveness_max_age(spec: JobSpec, s: "Settings | None" = None) -> int | None:
    if spec.kind == "manual":
        return None
    if spec.liveness_max_age_sec is not None:
        return spec.liveness_max_age_sec
    if spec.kind == "longtask":
        return None
    return int(trigger_interval_min(spec, s) * 60 * 2 + (spec.timeout_sec or 0) + 120)


def previous_fire_time(trigger: BaseTrigger, now: datetime) -> datetime | None:
    """Step get_next_fire_time from now − 35 d; the last fire ≤ now."""
    cur = now - timedelta(days=35)
    prev = None
    last = None
    for _ in range(100000):
        nxt = trigger.get_next_fire_time(prev, cur)
        if nxt is None or nxt > now:
            return last
        last = nxt
        prev = nxt
        cur = nxt + timedelta(microseconds=1)
    return last


# ── status ───────────────────────────────────────────────────────────────────────────────────────────────────
@dataclass
class _Status:
    last_run: int | None = None
    last_ok: int | None = None
    last_error: str | None = None
    last_skip_reason: str | None = None
    consecutive_failures: int = 0
    first_failure_at: int | None = None
    last_rows: int | None = None
    last_duration_sec: float | None = None
    last_cost_usd: float | None = None
    paused: str | None = None
    running: bool = False
    failing_latched: bool = False


def _enabled(spec: JobSpec) -> bool:
    s = app_config.settings
    if not all(bool(getattr(s, f, False)) for f in spec.enabled_flags):
        return False
    if spec.enabled_fn is not None and not spec.enabled_fn(s):
        return False
    return True


# ── CollectorJob ─────────────────────────────────────────────────────────────────────────────────────────────
class CollectorJob(ABC):
    spec: JobSpec

    def __init__(self, rt: "Runtime", spec: JobSpec) -> None:
        self.rt = rt
        self.spec = spec
        self._status = _Status()
        self._state_cache: dict[str, Any] = {}

    @property
    def id(self) -> str:
        return self.spec.id

    @property
    def status_raw(self) -> dict:
        return {"last_run": self._status.last_run, "last_ok": self._status.last_ok,
                "running": self._status.running}

    @property
    def running(self) -> bool:
        return self._status.running

    def enabled(self) -> bool:
        return _enabled(self.spec)

    def interval_min(self) -> float:
        return trigger_interval_min(self.spec)

    def effective_liveness_max_age(self) -> int | None:
        return spec_liveness_max_age(self.spec)

    def paused_reason(self) -> str | None:
        from app.control import effective_pause
        return effective_pause(self, self.rt)

    def fail_alert_threshold(self) -> int:
        s = app_config.settings
        if self.spec.fail_alert_key:
            return max(1, int(getattr(s, self.spec.fail_alert_key)))
        if self.spec.fail_alert_threshold:
            return max(1, int(self.spec.fail_alert_threshold))
        return max(1, int(s.JOB_FAIL_ALERT_THRESHOLD))

    def effective_timeout(self) -> int | None:
        """``timeout_key``'s setting when the spec names one (live config), else the static ``timeout_sec``."""
        if self.spec.timeout_key:
            return max(1, int(getattr(app_config.settings, self.spec.timeout_key)))
        return self.spec.timeout_sec

    @abstractmethod
    async def collect(self, run: RunContext) -> JobOutcome: ...

    # job_state (runtime watermarks only — never pauses, 02 §B12)
    async def get_state(self, key: str, default: Any = None) -> Any:
        if key in self._state_cache:
            return self._state_cache[key]
        db = self.rt.db
        if db is None:
            return default
        row = await db.fetchone("SELECT value_json FROM job_state WHERE job = ? AND key = ?", (self.id, key),
                                writer=True)
        val = json.loads(row["value_json"]) if row else default
        self._state_cache[key] = val
        return val

    async def set_state(self, key: str, value: Any) -> None:
        self._state_cache[key] = value
        db = self.rt.db
        if db is None:
            return
        await db.upsert("job_state", [{"job": self.id, "key": key, "value_json": json.dumps(value, default=str),
                                       "updated_at": now_ts()}], conflict=("job", "key"))

    async def _safe(self, coro: Any, what: str) -> Any:
        try:
            return await coro
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(f"{what} failed", data={"job": self.id, "error": f"{type(exc).__name__}: {exc}"})
            return None

    async def run(self, trigger: str = "schedule") -> None:
        """The wrapper (01 §2.6). Never raises (except CancelledError)."""
        st = self._status
        if st.running:
            st.last_skip_reason = "in_flight"
            return
        if not self.enabled():
            st.last_skip_reason = "disabled"
            st.paused = None
            return
        reason = self.paused_reason()
        if reason:
            st.paused = reason
            st.last_skip_reason = f"paused:{reason}"
            return
        st.paused = None
        if self.spec.kind == "interval" and trigger == "schedule" and self.spec.min_gap_factor > 0:
            last_ok = await self._safe(self.get_state("last_ok"), "job_state read")
            if last_ok and now_ts() - int(last_ok) < self.interval_min() * 60 * self.spec.min_gap_factor:
                st.last_skip_reason = "recent"
                return
        st.running = True
        st.last_skip_reason = None
        run_id = uuid.uuid4().hex
        t0 = time.monotonic()
        started = now_ts()
        db = self.rt.db
        if db is not None:
            await self._safe(db.execute("INSERT INTO collector_runs (run_id, job, started_at) VALUES (?,?,?)",
                                        (run_id, self.id, started)), "collector_runs insert")
        ctx = RunContext(run_id=run_id, started_at=started, trigger=trigger, rt=self.rt,
                         settings=app_config.settings, log=logger)
        try:
            outcome = await asyncio.wait_for(self.collect(ctx), timeout=self.effective_timeout())
        except asyncio.CancelledError:
            st.running = False
            raise
        except Exception as exc:
            from app.ratelimit import BudgetExhausted
            dur = round(time.monotonic() - t0, 3)
            if isinstance(exc, BudgetExhausted):
                note = f"paused:budget {exc.provider} {exc.window} until {to_iso(exc.until)}"
                if db is not None:
                    await self._safe(db.execute(
                        "UPDATE collector_runs SET finished_at=?, ok=1, rows=?, note=?, duration_sec=? WHERE run_id=?",
                        (now_ts(), ctx.rows_committed, note, dur, run_id)), "collector_runs update")
                st.last_skip_reason = None
                logger.info(f"{self.id} ended on budget", data={"note": note})
            else:
                err = ("timeout after %ss" % self.effective_timeout()) if isinstance(exc, asyncio.TimeoutError) \
                    else f"{type(exc).__name__}: {exc}"
                err = err[:500]
                st.last_error = err
                if st.consecutive_failures == 0:
                    st.first_failure_at = now_ts()
                st.consecutive_failures += 1
                logger.error(f"{self.id} failed", data={"job": self.id, "run_id": run_id, "error": err,
                                                        "consecutive_failures": st.consecutive_failures})
                if db is not None:
                    await self._safe(db.execute(
                        "UPDATE collector_runs SET finished_at=?, ok=0, error=?, duration_sec=? WHERE run_id=?",
                        (now_ts(), err, dur, run_id)), "collector_runs update")
                if st.consecutive_failures == self.fail_alert_threshold() and self.rt.alerts is not None:
                    st.failing_latched = True
                    await self._safe(self.rt.alerts.fire(
                        f"collector_failing:{self.id}",
                        f"⚠️ <b>Collector failing</b>: {self.id} ×{st.consecutive_failures}\n<code>{_esc(err)}</code>",
                        event_type="collector_failing", severity="warning",
                        payload={"collector": self.id, "consecutive_failures": st.consecutive_failures,
                                 "last_error": err, "job": self.id},
                        dedupe_key=f"{self.id}|{st.first_failure_at}", source="runtime"), "alert fire")
        else:
            now = now_ts()
            dur = round(time.monotonic() - t0, 3)
            st.last_ok = now
            st.last_rows = outcome.rows
            run_total = 0.0
            costs = self.rt.costs
            if costs is not None:
                run_total = costs.run_total(run_id)
                extra = (outcome.cost_usd or 0.0) - run_total
                if extra > 1e-9 and self.spec.providers:
                    from app.costs import spend_provider_for
                    await self._safe(costs.record(spend_provider_for(self.spec.providers[0]), extra, job=self.id,
                                                  run_id=run_id), "cost reconciliation")
            cost = max(outcome.cost_usd or 0.0, run_total)
            st.last_cost_usd = cost
            if db is not None:
                await self._safe(db.execute(
                    "UPDATE collector_runs SET finished_at=?, ok=1, rows=?, note=?, meta_json=?, duration_sec=?, "
                    "cost_usd=? WHERE run_id=?",
                    (now, outcome.rows, outcome.note, json.dumps(outcome.meta, default=str) if outcome.meta else None,
                     dur, cost, run_id)), "collector_runs update")
            await self._safe(self.set_state("last_ok", now), "job_state write")
            if st.consecutive_failures and st.failing_latched and self.rt.alerts is not None:
                n_before = st.consecutive_failures
                ffa = st.first_failure_at or now
                await self._safe(self.rt.alerts.clear(
                    f"collector_failing:{self.id}", f"✅ {self.id} recovered", event_type="collector_recovered",
                    severity="info", payload={"collector": self.id, "failures": n_before,
                                              "down_seconds": now - ffa, "job": self.id},
                    dedupe_key=f"{self.id}|{ffa}"), "alert clear")
            st.consecutive_failures = 0
            st.first_failure_at = None
            st.failing_latched = False
            st.last_error = None
        finally:
            st.last_run = now_ts()
            st.last_duration_sec = round(time.monotonic() - t0, 3)
            st.running = False

    def health_extra(self) -> dict | None:
        return None

    def status(self) -> dict:
        st = self._status
        spec = self.spec
        next_run = None
        sched = self.rt.scheduler
        if sched is not None:
            try:
                j = sched.get_job(spec.id)
                if j is not None and j.next_run_time is not None:
                    next_run = to_iso(j.next_run_time.timestamp())
            except Exception:  # pragma: no cover
                pass
        max_age = self.effective_liveness_max_age()
        enabled = self.enabled()
        paused = self.paused_reason() if enabled else None
        last = st.last_run or self.rt.started_at
        stale = bool(max_age is not None and enabled and not paused and now_ts() - last > max_age)
        out = {
            "owner": spec.owner, "phase": spec.phase, "kind": spec.kind, "trigger": spec.trigger_desc,
            "enabled": enabled, "paused": paused, "paid": spec.paid, "ops_exempt": spec.ops_exempt,
            "providers": list(spec.providers), "next_run": next_run, "last_run": to_iso(st.last_run),
            "last_ok": to_iso(st.last_ok), "last_error": st.last_error,
            "last_skip_reason": "disabled" if not enabled else st.last_skip_reason,
            "consecutive_failures": st.consecutive_failures, "last_rows": st.last_rows,
            "last_duration_sec": st.last_duration_sec, "last_cost_usd": st.last_cost_usd,
            "liveness_max_age_sec": max_age, "stale": stale,
        }
        if paused:
            out["paused_reason"] = paused
        extra = None
        try:
            extra = self.health_extra()
        except Exception as exc:  # pragma: no cover
            extra = {"error": str(exc)}
        if extra:
            out["extra"] = extra
        return out


def _esc(s: str) -> str:
    import html
    return html.escape(s, quote=False)


# ── LongTask ─────────────────────────────────────────────────────────────────────────────────────────────────
class LongTask(ABC):
    spec: JobSpec
    backoff_min_sec: float = 5.0
    backoff_max_sec: float = 300.0

    def __init__(self, rt: "Runtime", spec: JobSpec) -> None:
        self.rt = rt
        self.spec = spec
        self.last_beat: int | None = None
        self.last_attempt: int | None = None
        self.connected = False
        self.reconnects = 0
        self.last_error: str | None = None
        self.fatal_error: str | None = None
        self.connected_since: int | None = None
        self._paused: str | None = None

    @property
    def id(self) -> str:
        return self.spec.id

    def enabled(self) -> bool:
        return _enabled(self.spec)

    def paused_reason(self) -> str | None:
        from app.control import effective_pause
        return effective_pause(self, self.rt)

    @abstractmethod
    async def run_once(self, stop: asyncio.Event) -> None: ...

    def beat(self) -> None:
        self.last_beat = now_ts()

    def attempt(self) -> None:
        self.last_attempt = now_ts()

    def mark_connected(self, on: bool) -> None:
        self.connected = on
        self.connected_since = now_ts() if on else None

    async def _wait(self, stop: asyncio.Event, seconds: float) -> None:
        try:
            await asyncio.wait_for(stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def run_forever(self, stop: asyncio.Event) -> None:
        backoff = self.backoff_min_sec
        while not stop.is_set():
            if not self.enabled() or self.paused_reason():
                self._paused = self.paused_reason() if self.enabled() else None
                self.mark_connected(False)
                await self._wait(stop, 15)
                continue
            self._paused = None
            started = time.monotonic()
            child = asyncio.create_task(self.run_once(stop))
            try:
                while not child.done():
                    done, _ = await asyncio.wait({child}, timeout=15)
                    if done:
                        break
                    if stop.is_set() or not self.enabled() or self.paused_reason():
                        child.cancel()
                        try:
                            await child
                        except (asyncio.CancelledError, Exception):
                            pass
                        break
                if child.done() and not child.cancelled() and child.exception() is not None:
                    exc = child.exception()
                    self.last_error = f"{type(exc).__name__}: {exc}"[:500]
                    logger.warning(f"{self.id} run_once raised", data={"error": self.last_error})
            except asyncio.CancelledError:
                child.cancel()
                raise
            self.mark_connected(False)
            self.last_attempt = now_ts()
            self.reconnects += 1
            if stop.is_set():
                break
            healthy = time.monotonic() - started >= 60
            backoff = self.backoff_min_sec if healthy else min(self.backoff_max_sec, backoff * 2)
            await self._wait(stop, backoff * random.uniform(0.8, 1.2))

    def status(self) -> dict:
        enabled = self.enabled()
        paused = self.paused_reason() if enabled else None
        max_age = self.spec.liveness_max_age_sec
        clocks = [x for x in (self.last_beat, self.last_attempt, self.rt.started_at) if x is not None]
        stale = bool(max_age is not None and enabled and not paused and now_ts() - max(clocks) > max_age)
        out = {"enabled": enabled, "available": True, "paused": paused, "connected": self.connected,
               "last_beat": to_iso(self.last_beat), "last_attempt": to_iso(self.last_attempt),
               "reconnects": self.reconnects, "last_error": self.last_error, "liveness_max_age_sec": max_age,
               "stale": stale}
        if self.fatal_error:
            out["fatal_error"] = self.fatal_error
        return out


# ── registry & scheduler ─────────────────────────────────────────────────────────────────────────────────────
_CHECK_ATTRS = ("id", "phase", "timeout_sec", "kind", "paid", "ops_exempt")


class JobRegistry:
    @staticmethod
    def build(rt: "Runtime") -> None:
        rt.jobs.clear()
        rt.tasks.clear()
        rt.unavailable.clear()
        seen: set[str] = set()
        for spec in JOB_TABLE:
            if spec.id in seen:
                raise JobSpecMismatch(f"duplicate job id {spec.id}")
            seen.add(spec.id)
            mod_name, _, cls_name = spec.factory.partition(":")
            try:
                mod = importlib.import_module(f"app.{mod_name}")
                cls = getattr(mod, cls_name)
            except (ModuleNotFoundError, ImportError, AttributeError) as exc:
                rt.unavailable[spec.id] = f"{type(exc).__name__}: {exc}"
                logger.debug("job unavailable", data={"job": spec.id, "phase": spec.phase, "error": str(exc)})
                continue
            want = LongTask if spec.kind == "longtask" else CollectorJob
            if not (isinstance(cls, type) and issubclass(cls, want)):
                raise JobSpecMismatch(f"{spec.id}: factory {spec.factory} is not a {want.__name__}")
            for attr in _CHECK_ATTRS:
                declared = cls.__dict__.get(attr)
                if declared is not None and not isinstance(declared, property) and declared != getattr(spec, attr):
                    raise JobSpecMismatch(f"{spec.id}: class attribute {attr}={declared!r} != JOB_TABLE "
                                          f"{getattr(spec, attr)!r}")
            obj = cls(rt, spec)
            if spec.kind == "longtask":
                rt.tasks[spec.id] = obj
            else:
                rt.jobs[spec.id] = obj
        if rt.unavailable:
            by_phase: dict[int, int] = {}
            for jid in rt.unavailable:
                ph = SPEC_BY_ID[jid].phase
                by_phase[ph] = by_phase.get(ph, 0) + 1
            logger.info("jobs unavailable (later phases not built yet)",
                        data={"count": len(rt.unavailable), "by_phase": dict(sorted(by_phase.items()))})


def setup_scheduler(rt: "Runtime", *, now: datetime | None = None) -> AsyncIOScheduler:
    s = app_config.settings
    sched = AsyncIOScheduler(timezone=UTC)
    boot = now or datetime.now(UTC)
    ops_delay = 5 if s.STARTUP_PRIME_ENABLED else 30
    k = 0
    for spec in JOB_TABLE:
        job = rt.jobs.get(spec.id)
        if job is None:
            continue
        trig = build_trigger(spec, s)
        kwargs: dict[str, Any] = dict(id=spec.id, name=spec.id, max_instances=1, coalesce=True,
                                      misfire_grace_time=spec.misfire_grace_sec, replace_existing=True)
        if spec.kind == "manual":
            sched.add_job(job.run, trig, next_run_time=None, **kwargs)
            continue
        if spec.kind == "interval":
            if spec.first_run == "ops":
                first = boot + timedelta(seconds=ops_delay)
            elif spec.first_run == "stagger":
                first = boot + timedelta(seconds=30 + s.JOB_START_STAGGER_SEC * k)
                k += 1
            else:
                first = boot + timedelta(seconds=int(spec.first_run))
            sched.add_job(job.run, trig, next_run_time=first, **kwargs)
            continue
        sched.add_job(job.run, trig, **kwargs)                            # cron: natural next fire
        if spec.startup_catchup:
            _plan_catchup(rt, sched, job, trig, boot)
    rt.scheduler = sched
    return sched


def _catchup_time(spec: JobSpec, boot: datetime) -> datetime | None:
    s = app_config.settings
    at = boot + timedelta(seconds=s.STARTUP_CATCHUP_DELAY_SEC)
    ts = int(at.timestamp())
    if spec.catchup_avoid_utc:
        inside, end = in_utc_windows(ts, spec.catchup_avoid_utc)
        if inside and end is not None:
            at = datetime.fromtimestamp(end, UTC)
            ts = end
    if spec.catchup_only_utc:
        inside, _ = in_utc_windows(ts, spec.catchup_only_utc)
        if not inside:
            return None
    return at


def _plan_catchup(rt: "Runtime", sched: AsyncIOScheduler, job: CollectorJob, trig: BaseTrigger,
                  boot: datetime) -> None:
    """E1 (amended): run once at boot when the last scheduled fire was missed while the process was down."""
    async def _check_and_schedule() -> None:
        try:
            last_ok = await job.get_state("last_ok")
        except Exception:
            last_ok = None
        missed = previous_fire_time(trig, boot)
        if missed is None:
            return
        if last_ok is not None and int(last_ok) >= int(missed.timestamp()):
            return
        at = _catchup_time(job.spec, boot)
        if at is None:
            return
        natural = trig.get_next_fire_time(None, boot)
        if natural is not None and at >= natural:
            return
        sched.add_job(job.run, DateTrigger(run_date=at, timezone=UTC), args=("catchup",), id=f"{job.id}:catchup",
                      name=f"{job.id}:catchup", max_instances=1, misfire_grace_time=3600, replace_existing=True)
        logger.info("startup catch-up planned", data={"job": job.id, "at": to_iso(at.timestamp()),
                                                      "missed": to_iso(missed.timestamp())})
    rt.pending_setup.append(_check_and_schedule)


# ── liveness (16 §2.5 rules) ─────────────────────────────────────────────────────────────────────────────────
def stale_ids(rt: "Runtime", now: int) -> list[str]:
    out: list[str] = []
    h = rt.task_handles.get("health")
    if h is not None and h.done():
        out.append("api_task")
    s = app_config.settings
    if s.WATCHDOG_API_SELF_PROBE_ENABLED and rt.api_probe_fails >= s.WATCHDOG_API_PROBE_FAILS:
        out.append("api_probe")
    for job in rt.jobs.values():
        max_age = job.effective_liveness_max_age()
        if max_age is None or not job.enabled() or job.paused_reason():
            continue
        last = job.status_raw["last_run"] or rt.started_at
        if now - last > max_age:
            out.append(job.id)
    for task in rt.tasks.values():
        max_age = task.spec.liveness_max_age_sec
        if max_age is None or not task.enabled() or task.paused_reason():
            continue
        last = max(x for x in (task.last_beat, task.last_attempt, rt.started_at) if x is not None)
        if now - last > max_age:
            out.append(task.id)
    return out


def _jobs_live() -> bool:
    from app.runtime import get_runtime
    return not stale_ids(get_runtime(), now_ts())


def declared_counts() -> dict[int, int]:
    out: dict[int, int] = {}
    for s in JOB_TABLE:
        out[s.phase] = out.get(s.phase, 0) + 1
    return out
