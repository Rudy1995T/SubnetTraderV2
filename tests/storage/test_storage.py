"""Spec 03 §9: schema, migrate, db, triggers, constraints, pit, events, precedence, catalog."""
import asyncio
import json
import sqlite3

import pytest

from app.storage import catalog
from app.storage.db import AppendOnlyViolation, Database
from app.storage.events import emit_event, mark_alerted
from app.storage.migrate import CODE_SCHEMA_VERSION, MigrationError, SchemaError, discover, lint, migrate, \
    split_statements
from app.storage.pit import cross_section, latest_scores, metric_at, put_metric_values



# ── schema ──────────────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_schema_counts(db):
    assert db.schema_version == 5 == CODE_SCHEMA_VERSION   # 0005 rpc_subnet_daily.emission_share + view (20 N2.3)
    q = "SELECT COUNT(*) FROM sqlite_schema WHERE type = ? AND name NOT LIKE 'sqlite_%'"
    assert await db.fetchval(q, ("table",)) == 83   # 80 + 0003 wb_presets/wb_uploads/wb_dereg_network_hourly
    assert await db.fetchval(q, ("view",)) == 6
    assert await db.fetchval(q, ("trigger",)) == 14
    assert await db.fetchval("PRAGMA user_version", writer=True) == CODE_SCHEMA_VERSION
    assert await db.fetchval("PRAGMA auto_vacuum", writer=True) == 2
    nonstrict = {r["name"] for r in await db.fetchall(
        "SELECT name FROM pragma_table_list WHERE schema='main' AND strict=0 AND type='table' AND name NOT LIKE 'sqlite_%'")}
    assert nonstrict == {"pool_history", "whale_cohort", "whale_events"}
    res = await migrate(db)
    assert res.applied == [] and res.version == CODE_SCHEMA_VERSION


@pytest.mark.asyncio
async def test_pool_history_legacy_columns(db):
    cols = [r["name"] for r in await db.fetchall("PRAGMA table_info(pool_history)", writer=True)]
    assert cols[:9] == ["netuid", "block_number", "timestamp", "price", "tao_in_pool", "alpha_in_pool",
                        "moving_price", "network_registered_at", "subnet_volume"]


# ── migrate ─────────────────────────────────────────────────────────────────────────────────────────────────
def test_split_statements_keeps_trigger():
    sql = "CREATE TABLE a(x);\n-- c\nCREATE TRIGGER t BEFORE DELETE ON a BEGIN SELECT RAISE(ABORT,'no'); END;\n"
    st = split_statements(sql)
    assert len(st) == 2 and st[1].startswith("CREATE TRIGGER") and st[1].rstrip().endswith("END;")


def test_lint_rules():
    assert lint("DROP TABLE x;", version=2)
    assert not lint("-- @allow-destructive reason\nDROP TABLE x;", version=2)
    for bad in ("BEGIN;", "VACUUM;", "PRAGMA journal_mode=DELETE;", "ATTACH 'x' AS y;"):
        assert lint(bad, version=2), bad
    assert not lint("CREATE TRIGGER t BEFORE DELETE ON a BEGIN SELECT 1; END;", version=2)


@pytest.mark.asyncio
async def test_migrations_apply_skip_drift_ahead(tmp_path):
    mdir = tmp_path / "migrations"
    mdir.mkdir()
    (mdir / "0002_add_col.sql").write_text("ALTER TABLE subnets ADD COLUMN note TEXT;\n"
                                           "ALTER TABLE subnets ADD COLUMN note TEXT;\n")
    d = Database(tmp_path / "m.db")
    await d.connect()
    try:
        migs = discover(mdir)
        res = await migrate(d, migrations=migs)
        assert res.applied == [1, 2] and res.version == 2
        (mdir / "0002_add_col.sql").write_text("ALTER TABLE subnets ADD COLUMN note TEXT; -- edited\n")
        res = await migrate(d, migrations=discover(mdir))
        assert res.applied == [] and "checksum_drift:2" in res.warnings
        res = await migrate(d, migrations=discover(tmp_path / "none"))
        assert any(w.startswith("schema_ahead:2>1") for w in res.warnings)
        assert await d.fetchval("SELECT COUNT(*) FROM events WHERE event_type='schema_warning'") >= 1
    finally:
        await d.close()


@pytest.mark.asyncio
async def test_failing_migration_rolls_back(tmp_path):
    mdir = tmp_path / "migrations"
    mdir.mkdir()
    (mdir / "0002_bad.sql").write_text("CREATE TABLE zz(a INTEGER);\nINSERT INTO nope VALUES (1);\n")
    d = Database(tmp_path / "f.db")
    await d.connect()
    try:
        with pytest.raises(MigrationError):
            await migrate(d, migrations=discover(mdir))
        assert await d.fetchval("SELECT MAX(version) FROM schema_version") == 1
        assert await d.fetchval("SELECT COUNT(*) FROM sqlite_schema WHERE name='zz'") == 0
    finally:
        await d.close()


def test_numbering_gap(tmp_path):
    (tmp_path / "0003_x.sql").write_text("SELECT 1;")
    with pytest.raises(SchemaError):
        discover(tmp_path)


# ── db ──────────────────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_pragmas_and_strict(db):
    assert await db.fetchval("PRAGMA journal_mode", writer=True) == "wal"
    assert await db.fetchval("PRAGMA synchronous", writer=True) == 1
    assert await db.fetchval("PRAGMA foreign_keys", writer=True) == 1
    assert await db.fetchval("PRAGMA busy_timeout", writer=True) == 5000
    with pytest.raises(sqlite3.Error):
        await db.execute("INSERT INTO collector_runs(run_id, job, started_at) VALUES ('x','t','2026-09-22T00:00Z')")


@pytest.mark.asyncio
async def test_old_sqlite_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(sqlite3, "sqlite_version_info", (3, 36, 0))
    with pytest.raises(SchemaError):
        await Database(tmp_path / "o.db").connect()


@pytest.mark.asyncio
async def test_transaction_rollback_and_isolation(db):
    with pytest.raises(RuntimeError):
        async with db.transaction():
            await db.execute("INSERT INTO job_state (job, key, value_json, updated_at) VALUES ('a','k','1',1)")
            raise RuntimeError("boom")
    assert await db.fetchval("SELECT COUNT(*) FROM job_state", writer=True) == 0
    seen = {}

    async def task_a():
        async with db.transaction():
            await db.execute("INSERT INTO job_state (job, key, value_json, updated_at) VALUES ('a','k','1',1)")
            seen["owner"] = await db.fetchval("SELECT COUNT(*) FROM job_state")
            seen["reader_during"] = await db._r.execute("SELECT COUNT(*) AS n FROM job_state")
            seen["reader_during"] = (await seen["reader_during"].fetchone())["n"]
            await asyncio.sleep(0.05)
            raise RuntimeError("rollback A")

    async def task_b():
        await asyncio.sleep(0.01)
        await db.execute("INSERT INTO job_state (job, key, value_json, updated_at) VALUES ('b','k','1',1)")

    ra, rb = await asyncio.gather(task_a(), task_b(), return_exceptions=True)
    assert isinstance(ra, RuntimeError) and rb is None
    assert seen["owner"] == 1 and seen["reader_during"] == 0
    rows = await db.fetchall("SELECT job FROM job_state", writer=True)
    assert [r["job"] for r in rows] == ["b"]


@pytest.mark.asyncio
async def test_insert_upsert_guards(db):
    with pytest.raises(ValueError):
        await db.insert_or_ignore("job_state", [{"job": "a", "nope": 1}])
    with pytest.raises(ValueError):
        await db.insert_or_ignore("job_state", [{"job": "a", "key": "k", "value_json": "1", "updated_at": 1},
                                                {"job": "a", "key": "k2", "value_json": "1"}])
    with pytest.raises(AppendOnlyViolation):
        await db.upsert("scores", [{"id": 1}], conflict=("id",))
    with pytest.raises(AppendOnlyViolation):
        await db.upsert("pool_history", [{"netuid": 1}], conflict=("netuid",))
    rows = [{"job": "a", "key": f"k{i}", "value_json": "1", "updated_at": 1} for i in range(1200)]
    assert await db.insert_or_ignore("job_state", rows) == 1200
    assert await db.insert_or_ignore("job_state", rows) == 0
    assert await db.fetchval("SELECT src_rank('sse_rollup') > src_rank('ledger_import')") == 1
    assert await db.fetchval("SELECT src_rank('sse_rollup') > src_rank('ledger_import')", writer=True) == 1


@pytest.mark.asyncio
async def test_backup_to(db, tmp_path, monkeypatch):
    await db.execute("INSERT INTO job_state (job, key, value_json, updated_at) VALUES ('a','k','1',1)")
    res = await db.backup_to(tmp_path / "b" / "x.db")
    assert res["integrity"] == "ok" and not (tmp_path / "b" / "x.db.tmp").exists()
    c = sqlite3.connect(tmp_path / "b" / "x.db")
    assert c.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    assert c.execute("SELECT COUNT(*) FROM job_state").fetchone()[0] == 1
    c.close()
    import app.storage.db as dbmod
    monkeypatch.setattr(dbmod, "_quick_check", lambda p: "row 1 missing")
    with pytest.raises(RuntimeError):
        await db.backup_to(tmp_path / "b" / "y.db")
    assert not (tmp_path / "b" / "y.db").exists() and not (tmp_path / "b" / "y.db.tmp").exists()
    st = await db.stats()
    assert {"path", "size_bytes", "wal_bytes", "schema_version", "journal_mode"} <= set(st)


# ── triggers & constraints ─────────────────────────────────────────────────────────────────────────────────
async def _score_row(db):
    await db.execute("INSERT INTO occupants (occupant_key, netuid, network_registered_at, status, first_seen_block, "
                     "first_seen_at, last_seen_block, last_seen_at) VALUES ('64:1',64,1,'active',1,0,1,0)")
    cols = [r["name"] for r in await db.fetchall("PRAGMA table_info(scores)", writer=True)]
    return cols


@pytest.mark.asyncio
async def test_scores_append_only(db):
    await _score_row(db)
    await db.execute(
        "INSERT INTO scores (occupant_key, netuid, as_of, spec_version, run_kind, run_id, status, pillar_scores, "
        "pillar_coverage, confidence, computed_at, input_cutoff) VALUES "
        "('64:1', 64, 86400, 'ts-1.0', 'official', 'r', 'active', '{}', '{}', 0.9, 86500, 86500)")
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        await db.execute("UPDATE scores SET netuid = 65")
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        await db.execute("DELETE FROM scores")
    rows = await latest_scores(db, "ts-1.0")
    assert len(rows) == 1 and rows[0]["occupant_key"] == "64:1"


def _mv(**kw):
    base = dict(occupant_key="64:1", metric_key="emission_share", as_of=86400, spec_version="ts-1.0", stage="raw",
                value=0.5, value_json={"status": "ok"}, observed_at=100, source="chain", run_id="r")
    base.update(kw)
    return base


@pytest.mark.asyncio
async def test_metric_values_rules(db):
    assert await put_metric_values(db, [_mv(value=None, value_json={"status": "failed"})]) == 1
    assert await put_metric_values(db, [_mv(value=None, value_json={"status": "failed"}, observed_at=150)]) == 0
    assert await put_metric_values(db, [_mv(observed_at=50)]) == 0            # earlier observation never upgrades
    assert await put_metric_values(db, [_mv(observed_at=200)]) == 1           # failed → ok upgrade
    assert await put_metric_values(db, [_mv(value=0.9, observed_at=300)]) == 0  # first success wins
    assert await db.fetchval("SELECT value FROM metric_values") == 0.5
    assert await put_metric_values(db, [_mv(stage="norm", value=0.7, value_json={"raw": 0.5, "status": "ok"})]) == 1
    assert await put_metric_values(db, [_mv(stage="norm", value=0.1, value_json={"raw": 0.5, "status": "ok"})]) == 0
    with pytest.raises(sqlite3.IntegrityError):
        await db.execute("UPDATE metric_values SET value = 0.2 WHERE stage = 'norm'")
    with pytest.raises(sqlite3.IntegrityError):
        await db.execute("DELETE FROM metric_values WHERE as_of = 86400")
    await put_metric_values(db, [_mv(as_of=86400 + 6 * 3600)])
    await db.execute("DELETE FROM metric_values WHERE as_of = ?", (86400 + 6 * 3600,))
    with pytest.raises(sqlite3.IntegrityError):
        await db.execute("INSERT INTO metric_values (occupant_key, metric_key, as_of, spec_version, stage, value, "
                         "value_json, observed_at, source, run_id) VALUES ('a','emission_share',1,'v','raw',NULL,"
                         "'{\"status\":\"ok\"}',1,'chain','r')")
    with pytest.raises(sqlite3.IntegrityError):
        await db.execute("INSERT INTO metric_values (occupant_key, metric_key, as_of, spec_version, stage, value, "
                         "value_json, observed_at, source, run_id) VALUES ('a','emission_share',1,'v','raw',1.0,"
                         "'{\"status\":\"ok\"}',1,'nlp','r')")


@pytest.mark.asyncio
async def test_put_metric_values_validation(db):
    with pytest.raises(ValueError, match="unknown"):
        await put_metric_values(db, [_mv(metric_key="nope")])
    with pytest.raises(ValueError, match="owner_extraction_trigger"):
        await put_metric_values(db, [_mv(metric_key="owner_lock_disabled_90d")])
    with pytest.raises(ValueError, match="discord_last_activity_days"):
        await put_metric_values(db, [_mv(metric_key="discord_last_announcement_days")])
    with pytest.raises(ValueError, match="status"):
        await put_metric_values(db, [_mv(value_json={})])
    with pytest.raises(ValueError, match="role"):
        await put_metric_values(db, [_mv(metric_key="x_cadence_30d", stage="norm", source="x")])


@pytest.mark.asyncio
async def test_metric_at_pit(db):
    await put_metric_values(db, [_mv(as_of=86400, observed_at=86500, value=0.4)])
    await put_metric_values(db, [_mv(as_of=2 * 86400, observed_at=2 * 86400 + 100, value=None,
                                     value_json={"status": "failed", "reason": "x"})])
    t = 2 * 86400 + 200
    r = await metric_at(db, "64:1", "emission_share", t, spec_version="ts-1.0", freshness_s=2 * 86400)
    assert r.value == 0.4 and r.status == "ok"                               # newer failed row skipped
    r = await metric_at(db, "64:1", "emission_share", t, spec_version="ts-1.0", freshness_s=300)
    assert r.status == "failed"                                              # only a failed row in the window
    assert await metric_at(db, "64:1", "emission_share", 86450, spec_version="ts-1.0", freshness_s=86400) is None
    r = await metric_at(db, "64:1", "emission_share", 86400, spec_version="ts-1.0", freshness_s=0)
    assert r is None                                                         # observed after t
    # upgrade at t2 does not change what a replay at t < t2 sees
    await put_metric_values(db, [_mv(as_of=2 * 86400, observed_at=3 * 86400, value=0.8)])
    r = await metric_at(db, "64:1", "emission_share", t, spec_version="ts-1.0", freshness_s=2 * 86400)
    assert r.value == 0.4
    xs = await cross_section(db, "emission_share", 86400, spec_version="ts-1.0", stage="raw")
    assert set(xs) == {"64:1"}


# ── events ──────────────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_emit_event_rules(db):
    base = dict(occupant_key=None, netuid=None, event_at=1, observed_at=1, source="runtime", run_id=None)
    for bad in ("owner_sell", "spend_soft_cap"):
        with pytest.raises(ValueError):
            await emit_event(db, event_type=bad, payload={}, dedupe_key=None, **base)
    with pytest.raises(ValueError, match="missing"):
        await emit_event(db, event_type="collector_failing", payload={"collector": "x"}, dedupe_key=None, **base)
    p = {"collector": "x", "consecutive_failures": 5, "last_error": "e", "extra": 1}
    i = await emit_event(db, event_type="collector_failing", payload=p, dedupe_key="x|1", **base)
    assert i and await emit_event(db, event_type="collector_failing", payload=p, dedupe_key="x|1", **base) is None
    assert await db.fetchval("SELECT severity FROM events WHERE id = ?", (i,)) == "warning"
    assert await emit_event(db, event_type="collector_failing", payload=p, dedupe_key=None, **base)
    assert await emit_event(db, event_type="collector_failing", payload=p, dedupe_key=None, **base)
    occ = dict(base, occupant_key="64:2", netuid=64, source="registry")
    back = {"old_key": "64:1", "new_key": "64:2", "old_name": "a", "new_name": "b", "reg_block": 2, "backfilled": True}
    assert await emit_event(db, event_type="netuid_reoccupied", payload=back, dedupe_key="r1", alerted_at=5, **occ)
    with pytest.raises(ValueError):
        await emit_event(db, event_type="netuid_reoccupied", payload={**back, "backfilled": False},
                         dedupe_key="r2", **occ)
    assert await mark_alerted(db, i) and not await mark_alerted(db, i)
    with pytest.raises(sqlite3.IntegrityError):
        await db.execute("UPDATE events SET severity = 'critical' WHERE id = ?", (i,))
    with pytest.raises(sqlite3.IntegrityError):
        await db.execute("DELETE FROM events")


# ── precedence ──────────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_price_bar_precedence(db):
    from app.storage.precedence import precedence_upsert_sql
    cols = ["netuid", "period", "ts", "close", "ts_last", "tao_in_pool", "source", "observed_at"]
    sql = precedence_upsert_sql("price_bars", cols, ("netuid", "period", "ts"), coalesce=("tao_in_pool",))

    async def put(src, close, ts_last=None, tip=None):
        await db.execute(sql, dict(netuid=1, period="1h", ts=0, close=close, ts_last=ts_last, tao_in_pool=tip,
                                   source=src, observed_at=1))
        return await db.fetchone("SELECT source, close, tao_in_pool FROM price_bars", writer=True)

    assert (await put("onfinality", 1.0))["source"] == "onfinality"
    assert (await put("tmc_candle", 2.0))["close"] == 2.0
    assert (await put("tmc_candle", 3.0))["close"] == 2.0                   # same rank, NULL ts_last → never
    assert (await put("ledger_import", 4.0, ts_last=10))["close"] == 4.0
    assert (await put("ledger_import", 5.0, ts_last=20))["close"] == 5.0     # later close sample
    assert (await put("ledger_import", 6.0, ts_last=15))["close"] == 5.0
    assert (await put("sse_rollup", 7.0, ts_last=1))["source"] == "sse_rollup"
    assert (await put("tmc_candle", 8.0))["close"] == 7.0                    # lower rank never overwrites


@pytest.mark.asyncio
async def test_chain_daily_precedence(db):
    from app.storage.precedence import precedence_upsert_sql
    cols = ["netuid", "day_ts", "occupant_key", "event_at", "observed_at", "source", "emission_percent"]
    sql = precedence_upsert_sql("chain_daily", cols, ("netuid", "day_ts"), keep=("occupant_key",))

    async def put(src, occ, v):
        await db.execute(sql, dict(netuid=1, day_ts=0, occupant_key=occ, event_at=0, observed_at=1, source=src,
                                   emission_percent=v))
        return await db.fetchone("SELECT source, occupant_key, emission_percent FROM chain_daily", writer=True)

    await put("analytics_1w", "1:5", 1.0)
    assert (await put("analytics_1w", "1:6", 2.0))["emission_percent"] == 1.0
    r = await put("analytics_1y", "1:7", 3.0)
    assert r["source"] == "analytics_1y" and r["occupant_key"] == "1:5" and r["emission_percent"] == 3.0
    assert (await put("analytics_1w", "1:5", 4.0))["emission_percent"] == 3.0


# ── catalog ─────────────────────────────────────────────────────────────────────────────────────────────────
def test_catalog():
    mk = catalog.METRIC_KEYS
    assert len(mk) == 109 and not set(mk) & set(catalog.RETIRED_METRIC_KEYS)
    for p in ("P1", "P2", "P3", "P4", "P5", "P6"):
        assert sum(m.weight for m in mk.values() if m.pillar == p) == pytest.approx(1.0)
    assert {k: mk[k].weight for k in ("team_doxxed", "owner_identity", "owner_commitment", "owner_retention_30d",
                                      "control_stability_90d", "owner_overhang", "tenure")} == \
        {"team_doxxed": .25, "owner_identity": .05, "owner_commitment": .20, "owner_retention_30d": .20,
         "control_stability_90d": .15, "owner_overhang": .05, "tenure": .10}
    assert all(m.source in catalog.SOURCES for m in mk.values())
    roles = [m.role for m in mk.values()]
    assert (roles.count("scored"), roles.count("leg"), roles.count("aux"), roles.count("evidence")) == (45, 9, 8, 47)   # 17 + 30 workbench keys (19 §3)
    for et, e in catalog.EVENT_TYPES.items():
        assert e.severity in ("info", "warning", "critical") and e.alert in ("yes", "digest", "no", "direct"), et
    for et in ("emission_disabled", "emission_reenabled", "dereg_risk_entered", "dereg_proximity", "site_down_7d",
               "site_recovered", "owner_extraction_detected", "owner_perpetual_off", "owner_control_change"):
        assert catalog.EVENT_TYPES[et].alert == "digest", et
    assert catalog.RED_FLAGS == {"emission_disabled": .5, "dereg_imminent": .6, "miner_burn_zeroed": .6,
                                 "owner_extraction": .7, "owner_control_change": .85, "key_compromise": .5,
                                 "repo_gone": .8, "all_repos_archived": .7, "site_down_7d": .85,
                                 "stale_identity": .9, "silence_45d": .85, "inflated_community": .9}


def test_routing_matches_catalog():
    from app.alerts.routing import ROUTES
    assert set(ROUTES) == set(catalog.EVENT_TYPES)
    for et, e in catalog.EVENT_TYPES.items():
        r = ROUTES[et]
        if isinstance(r, str):
            want = {"yes": "immediate", "digest": "digest", "no": "none"}[e.alert]
            assert r == want, et
