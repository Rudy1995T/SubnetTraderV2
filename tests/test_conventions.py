"""02 cross-cutting conventions — the machine-checkable rules, checked against the code (02 header, amended
2026-10-16). One test per 02 clause id; a failure names the clause to read before changing either side."""
import re
from pathlib import Path

import pytest

import app.config as app_config
from app import control
from app.costs import SPEND_GROUPS
from app.jobs import JOB_TABLE, RETIRED_JOB_IDS
from app.ratelimit import PROVIDERS, ProviderLimiter, limits_from_settings
from app.storage import catalog

MODELS_SQL = Path(__file__).resolve().parents[1] / "app" / "storage" / "models.sql"

# 02 §G1 (+ amendment 2026-10-16: blockmachine)
G1_PROVIDERS = {
    "tmc", "tmc_sse", "subnetalpha", "subtensor_rpc", "onfinality", "opentensor_archive", "blockmachine", "github",
    "github_html", "web", "wayback", "discord", "tme", "rss", "youtube", "twitterapi_io", "official_v2", "anthropic",
    "taostats", "rufus", "subnetradar", "telegram", "deadman",
}
G1_GROUPS = {"x": {"twitterapi_io", "official_v2"}, "llm": {"anthropic"}, "taostats": {"taostats"}, "tmc": {"tmc"}}
B9_SOURCES = {"registry", "chain", "github", "web", "community", "x", "rss", "llm", "integrity", "scoring", "quant",
              "api", "runtime", "storage", "cost", "ops"}


def _check_list(table: str) -> set[str]:
    sql = MODELS_SQL.read_text()
    body = sql[sql.index(f"CREATE TABLE IF NOT EXISTS {table} ("):]
    m = re.search(r"source\s+TEXT\s+NOT NULL CHECK \(source IN \(([^)]*)\)", body)
    assert m, table
    return set(re.findall(r"'([a-z_]+)'", m.group(1)))


def test_g1_provider_ids_one_vocabulary():
    assert set(PROVIDERS) == G1_PROVIDERS
    assert set(limits_from_settings()) == G1_PROVIDERS
    assert {g: set(sg.members) for g, sg in SPEND_GROUPS.items()} == G1_GROUPS


def test_g1_job_providers_are_ids_or_groups():
    allowed = G1_PROVIDERS | set(SPEND_GROUPS)
    bad = {s.id: p for s in JOB_TABLE for p in s.providers if p not in allowed}
    assert not bad


def test_b9_source_vocabulary_and_checks():
    assert set(catalog.SOURCES) == B9_SOURCES
    assert _check_list("metric_values") == B9_SOURCES
    assert _check_list("events") == B9_SOURCES
    assert {m.source for m in catalog.METRIC_KEYS.values()} <= B9_SOURCES
    assert {e.source for e in catalog.EVENT_TYPES.values() if e.source} <= B9_SOURCES


def test_b9_ops_emitters_catalogued_as_ops():
    for t in ("service_recovered_unclean", "backup_failed", "backup_mirror_missing", "backup_mirror_failed",
              "backup_stale", "disk_low", "disk_critical", "db_integrity_failed"):
        assert catalog.EVENT_TYPES[t].source == "ops", t


def test_b2_severities():
    assert catalog.SEVERITIES == ("info", "warning", "critical")
    assert {e.severity for e in catalog.EVENT_TYPES.values()} <= set(catalog.SEVERITIES)


def test_d1_catalog_names():
    retired = {"owner_coldkey_swap", "owner_perpetual_lock_disabled", "owner_sell", "owner_full_exit_call",
               "owner_cut_enabled_changed", "invite_expiring", "guild_changed", "community_inflation_suspected",
               "tg_handle_gone", "spend_soft_cap", "spend_hard_cap"}
    assert not retired & set(catalog.EVENT_TYPES)
    required = {
        "owner_control_change", "owner_perpetual_off", "owner_extraction_detected", "owner_cut_toggled",
        "invite_expired", "invite_dead", "inflated_community", "community_venue_changed", "silence_45d",
        "spend_80pct", "spend_cap_reached",
        "service_recovered_unclean", "spend_reset", "budget_override_set", "backup_failed", "backup_stale",
        "backup_mirror_missing", "backup_mirror_failed", "disk_low", "disk_critical", "db_integrity_failed",
        "schema_warning", "collector_failing", "collector_recovered", "owner_coldkey_swap_announced",
        "tier_change", "red_flag_raised", "red_flag_cleared", "score_jump", "insufficient_data", "rank_instability",
        "scoring_stalled", "discord_gateway_fatal", "backfill_done", "backfill_failed", "dereg_watch",
    }
    assert required <= set(catalog.EVENT_TYPES), required - set(catalog.EVENT_TYPES)
    assert catalog.EVENT_TYPES["discord_gateway_fatal"].severity == "warning"
    assert catalog.EVENT_TYPES["backfill_failed"].severity == "warning"


def test_d2_payload_keys():
    E = catalog.EVENT_TYPES
    assert "down_seconds" in E["chain_sse_recovered"].payload_keys
    assert E["source_disagreement"].severity == "info" and "repo_ids" in E["source_disagreement"].optional
    assert {"last_comm_at", "last_comm_platform"} <= set(E["silence_45d"].payload_keys)
    assert set(E["collector_recovered"].payload_keys) == {"collector", "failures", "down_seconds"}
    assert "last_good_age_h" in E["backup_failed"].payload_keys
    assert set(E["backup_stale"].payload_keys) == {"age_hours", "stale_hours"}
    assert set(E["red_flag_raised"].payload_keys) >= {"netuid", "spec_version", "as_of", "flag", "multiplier",
                                                      "human", "sticky", "detail", "flag_multiplier_total"}
    assert E["rank_instability"].severity == "info"
    assert E["gate_model_broken"].dedupe == "gate_model_broken|{d7}"


def test_d4_detection_events_digest_only():
    for t in ("emission_disabled", "emission_reenabled", "dereg_risk_entered", "dereg_proximity", "site_down_7d",
              "site_recovered", "owner_extraction_detected", "owner_perpetual_off", "owner_control_change",
              "insufficient_data", "rank_instability", "x_budget_paused", "tmc_budget_degraded"):
        assert catalog.EVENT_TYPES[t].alert == "digest", t
    for t in ("score_jump", "red_flag_raised", "red_flag_cleared", "spend_80pct", "spend_cap_reached",
              "disk_critical", "db_integrity_failed"):
        assert catalog.EVENT_TYPES[t].alert == "yes", t
    assert "direct" not in {e.alert for e in catalog.EVENT_TYPES.values()}


def test_c2_c3_retired_metric_keys():
    retired = {"owner_lock_disabled_90d", "owner_control_change_30d", "owner_max_sell_impact_7d",
               "identity_stability", "owner_conviction", "owner_retention", "discord_last_announcement_days",
               "tg_last_post_days", "x_last_post_days", "announcement_cadence", "metric_norm"}
    assert not retired & set(catalog.METRIC_KEYS)
    for k in ("owner_extraction_trigger", "owner_control_change_trigger", "announcement_cadence_30d", "silence_days",
              "x_last_activity_days", "tg_last_activity_days", "discord_last_activity_days"):
        assert k in catalog.METRIC_KEYS, k


def test_c8_evidence_keys():
    ev = {k for k, m in catalog.METRIC_KEYS.items() if m.weight == 0 and m.role == "evidence"}
    assert ev == {"owner_lock_ratio", "owner_dump_impact", "owner_holding_share", "owner_sell_impact_7d",
                  "owner_sell_ratio_30d", "team_verified_count", "team_candidate_count", "vtrust_dispersion",
                  "owner_locked_share", "sum_subnet_prices", "tao_usd", "discord_topology", "inflated_days",
                  "tg_members", "video_uploads_90d", "video_mentions_30d", "blog_posts_90d"} | WB_KEYS


WB_KEYS = {"ema72_1h", "ema72_dist_1h", "ema72_band_1h", "ema72_band_pos_1h", "px_vs_moving", "ema72_1d",
           "ema72_dist_1d", "ema72_band_1d", "ema72_band_pos_1d", "ema72_band_rank_1d", "rv_30d", "dd_90d",
           "ema72_cross_days_1d", "immunity_remaining_days", "dereg_rank", "dereg_rank_30d", "dereg_rank_90d",
           "cull_line_1", "cull_line_k_30d", "cull_line_k_90d", "mp_gap_to_cull_1", "mp_gap_to_cull_k_30d",
           "mp_gap_to_cull_k_90d", "dereg_safety_30d", "dereg_safety_90d", "hours_in_bottom_k_30d", "spot_rank",
           "spot_vs_mp", "days_to_cull_static", "repo_pushed_days"}


def test_k4_workbench_keys_and_k5_events():
    """02 §K3–§K6: 30 evidence keys owned by 19 (chain, except the GitHub probe), digest-only events, wb pause group."""
    wb = {k: m for k, m in catalog.METRIC_KEYS.items() if m.producer_spec == "19"}
    assert set(wb) == WB_KEYS
    assert all(m.weight == 0 and m.role == "evidence" for m in wb.values())
    assert {k for k, m in wb.items() if m.source != "chain"} == {"repo_pushed_days"}
    assert wb["repo_pushed_days"].source == "github"
    for t in ("dereg_rank_entered_k30", "dereg_rank_cleared_k30", "immunity_ends_7d", "ema72_band_exit"):
        e = catalog.EVENT_TYPES[t]
        assert e.source == "chain" and e.alert == "digest", t
    assert catalog.EVENT_TYPES["immunity_ends_7d"].severity == "warning"
    assert control.PAUSE_GROUPS["wb"] == {"wb_technicals", "wb_dereg", "chain_constants_probe", "wb_github_probe"}
    ids = {s.id: s for s in JOB_TABLE}
    for j in control.PAUSE_GROUPS["wb"]:
        assert ids[j].owner == "19" and ids[j].phase == 3 and not ids[j].ops_exempt and not ids[j].startup_catchup


def test_e1_trigger_kinds_and_exemptions():
    assert {s.id for s in JOB_TABLE if s.kind == "longtask"} == {"chain_sse", "discord_gateway", "telegram_bot"}
    assert {s.id for s in JOB_TABLE if s.ops_exempt} == {
        "backup_daily", "ops_guard_5m", "ops_digest_daily", "cost_ledger_hourly", "log_retention_daily",
        "storage_retention_daily", "alert_dispatch", "telegram_bot"}
    assert {s.id for s in JOB_TABLE if s.startup_catchup} == {
        "registry_identity", "tmc_daily", "chain_features", "github_org_enum", "github_activity", "github_metrics",
        "owner_collect_daily", "integrity_features", "scoring_6h"}


def test_e2_job_table_complete():
    ids = [s.id for s in JOB_TABLE]
    assert len(ids) == len(set(ids)) == 57
    assert not RETIRED_JOB_IDS & set(ids)
    assert {"wb_discord_counts"}.isdisjoint(ids)                          # §K6 retired before it was built


def test_b12_pause_vocabulary_and_closed_sentinels():
    assert control.PAUSE_REASONS == ("operator", "kill_switch", "pause_all", "budget", "emergency_file")
    assert control.NOT_PAUSABLE == {"alert_dispatch", "telegram_bot"}
    fields = set(app_config.Settings.model_fields)
    assert {"KILL_SWITCH_PATH", "PAUSE_ALL_PATH", "SCORING_KILL_SWITCH_PATH"} <= fields
    retired = {"X_KILLED", "GITHUB_KILLED", "COMMUNITY_PAUSED", "TMC_PAUSED", "TMC_PAUSE_SENTINEL",
               "X_KILL_SWITCH_PATH", "COMMUNITY_KILL_SWITCH_PATH", "GITHUB_KILL_SWITCH_PATH", "SSE_MAX_CONNECTIONS",
               "WB_UI_PORT"}
    assert not retired & fields
    for g in control.PAUSE_GROUPS:
        assert not set(control.resolve_targets(g)) & control.never_grouped(), g


@pytest.mark.asyncio
async def test_b12_not_pausable(rt):
    for t in ("alert_dispatch", "telegram_bot"):
        with pytest.raises(control.NotPausable):
            await control.pause(t, by="test", db=rt.db)
    assert await rt.db.fetchval("SELECT COUNT(*) FROM control_state", writer=True) == 0


@pytest.mark.asyncio
async def test_b12_pause_resume_audit_events(rt):
    await control.pause("web", by="test", reason="r", db=rt.db)
    await control.resume("web", by="test", db=rt.db)
    await control.resume("web", by="test", db=rt.db)                     # no row → no second event
    rows = await rt.db.fetchall("SELECT event_type, source FROM events ORDER BY id", writer=True)
    assert [(r["event_type"], r["source"]) for r in rows] == [("collector_paused", "api"),
                                                              ("collector_resumed", "api")]


def test_f2_defaults():
    s = app_config.Settings()
    assert (s.TMC_RATE_PER_MIN, s.TMC_RATE_BURST, s.TMC_HOURLY_CAP, s.TMC_MONTHLY_CAP) == (8, 2, 500, 90000)
    assert s.TMC_SSE_MAX_CONNECTIONS == 2
    assert (s.X_MONTHLY_CAP_USD, s.LLM_MONTHLY_CAP_USD) == (25, 60)
    assert (s.CHAIN_TICKS_RETENTION_DAYS, s.CHAIN_TICKS_BLOCK_STRIDE) == (30, 5)
    assert (s.DB_BACKUP_MAX_TOTAL_GB, s.DB_BACKUP_STALE_HOURS, s.DB_BACKUP_KEEP_UNCOMPRESSED) == (12, 36, 1)
    assert s.SCORING_OFFICIAL_HOUR == 0
    assert (s.DISCORD_STORE_TEXT, s.TELEGRAM_POSTS_STORE_TEXT, s.LLM_PENDING_TEXT_MAX_DAYS) == (True, False, 7)
    assert s.QUANT_WEEKLY_JOB_CRON == "sun 05:00"
    assert (s.BACKFILL_TMC_QUIET_UTC, s.BACKFILL_TMC_MAX_PER_HOUR) == ("02:05-02:45,20:25-23:35", 180)
    assert (s.TAOSTATS_RATE_PER_MIN, s.TAOSTATS_MONTHLY_CAP_REQ) == (5, 10000)
    assert (s.BLOCKMACHINE_RATE_PER_SEC, s.BLOCKMACHINE_MONTHLY_CAP_REQ) == (12.0, 4_500_000)


@pytest.mark.parametrize("provider,key", [("blockmachine", "BLOCKMACHINE_MONTHLY_CAP_REQ"),
                                          ("taostats", "TAOSTATS_MONTHLY_CAP_REQ"), ("tmc", "TMC_MONTHLY_CAP")])
def test_f2_request_cap_zero_fails_closed(settings_override, provider, key):
    settings_override(**{key: 0})
    lim = ProviderLimiter(limits_from_settings())
    assert lim.exhausted(provider) is not None


def test_g1_retry_after_http_date():
    from email.utils import format_datetime
    from datetime import datetime, timezone
    wall = 1_800_000_000
    lim = ProviderLimiter(limits_from_settings(), wall=lambda: wall)
    lim.observe("web", {"Retry-After": format_datetime(datetime.fromtimestamp(wall + 120, timezone.utc),
                                                       usegmt=True)}, host="h")
    left = lim._host_until[("web", "h")] - lim._mono()
    assert 110 < left <= 120


def test_g1_spend_book_persists_requests(tmp_root):
    import sqlite3
    from app.backfill.common import SpendBook
    db = tmp_root / "spend.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE provider_spend (provider TEXT, month TEXT, usd REAL, requests INTEGER, "
                "units_json TEXT, updated_at INTEGER, PRIMARY KEY (provider, month))")
    con.close()
    lim = ProviderLimiter(limits_from_settings())
    book = SpendBook(db, flush_every=3)
    book.seed(lim, ["blockmachine"])
    for _ in range(4):
        book.add("blockmachine")
    book.flush()
    lim2 = ProviderLimiter(limits_from_settings())
    SpendBook(db).seed(lim2, ["blockmachine"])
    assert lim2.month_requests("blockmachine") == 4                       # a second process sees the first's count


def test_b1_parse_iso_milliseconds():
    from app.util.time import parse_iso, parse_ts
    assert parse_iso("2026-10-12T10:00:36.000Z") == parse_iso("2026-10-12T10:00:36Z") == 1791799236
    assert parse_ts("2026-10-12T10:00:36.000Z") == 1791799236
