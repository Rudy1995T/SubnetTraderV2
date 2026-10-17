"""Spec 03 §9 test_retention (subset): tick rollup + prune, backup gate, privacy text retention, dry-run."""
import pytest

import app.config as app_config
from app.storage.retention import run_retention

NOW = 1790121600          # 2026-09-23T00:00Z


async def _ticks(db, netuid, start, n, step=60, base=1000):
    rows = [dict(netuid=netuid, block_number=base + i, occupant_key=f"{netuid}:1", event_at=start + i * step,
                 observed_at=start + i * step, source="sse", run_id="sse:1", price=1.0 + i / 100, tao_liquidity=5e12,
                 subnet_moving_price=1.0) for i in range(n)]
    await db.insert_or_ignore("chain_ticks", rows)


@pytest.mark.asyncio
async def test_chain_prune_rollup_and_gate(db, monkeypatch):
    old = NOW - 40 * 86400
    await _ticks(db, 1, old, 30)
    await _ticks(db, 2, old, 30)
    await _ticks(db, 1, NOW - 3600, 10, base=900000)
    import app.storage.backup as bk
    monkeypatch.setattr(bk, "latest_backup_age_hours", lambda d: 40.0)
    res = await run_retention(db, now=NOW)
    assert "chain_prune" not in res and res["_skipped"] >= 1
    assert await db.fetchval("SELECT COUNT(*) FROM events WHERE event_type='retention_skipped'") == 1
    monkeypatch.setattr(bk, "latest_backup_age_hours", lambda d: 10.0)
    res = await run_retention(db, now=NOW)
    assert res["chain_prune"] > 0
    assert await db.fetchval("SELECT COUNT(*) FROM chain_ticks WHERE event_at < ?", (NOW - 30 * 86400,)) == 0
    assert await db.fetchval("SELECT COUNT(*) FROM chain_ticks") == 10
    assert await db.fetchval("SELECT COUNT(*) FROM chain_hourly WHERE source='tick_rollup'") == 2
    bars = await db.fetchall("SELECT * FROM price_bars WHERE period='5m' ORDER BY netuid, ts")
    assert bars and all(b["source"] == "sse_rollup" and b["close"] > 0 for b in bars)
    first = bars[0]
    assert first["open"] == pytest.approx(1.0) and first["close"] == pytest.approx(1.04)
    res2 = await run_retention(db, now=NOW, only=["chain_prune"])
    assert res2["chain_prune"] == 0


@pytest.mark.asyncio
async def test_text_retention_and_dry_run(db, monkeypatch):
    import app.storage.backup as bk
    monkeypatch.setattr(bk, "latest_backup_age_hours", lambda d: None)
    for i, age in enumerate((91, 89)):
        await db.execute(
            "INSERT INTO social_posts (post_id, author_id, text, text_hash, event_at, observed_at, source) "
            "VALUES (?,?,?,?,?,?,'twitterapi_io')", (f"p{i}", "a", "hello", "h", NOW - age * 86400, NOW))
    await db.execute(
        "INSERT INTO announcements (msg_id, platform, source_channel_id, message_id, mapping, text, text_purge_after, "
        "text_hash, event_at, observed_at) VALUES ('telegram:x/1','telegram','x','1','tg_handle','pending',?,'h',?,?)",
        (NOW - 3 * 86400 + 7 * 86400, NOW - 3 * 86400, NOW - 3 * 86400))
    dry = await run_retention(db, now=NOW, dry_run=True, only=["nlp_text_retention"])
    assert dry["nlp_text_retention"] == 1
    assert await db.fetchval("SELECT COUNT(*) FROM social_posts WHERE text IS NULL") == 0
    await run_retention(db, now=NOW, only=["nlp_text_retention"])
    assert await db.fetchval("SELECT text FROM social_posts WHERE post_id='p0'") is None
    assert await db.fetchval("SELECT text_hash FROM social_posts WHERE post_id='p0'") == "h"
    assert await db.fetchval("SELECT text FROM social_posts WHERE post_id='p1'") == "hello"
    assert await db.fetchval("SELECT text FROM announcements") == "pending"      # day 3, unlabelled → kept
    await run_retention(db, now=NOW + 5 * 86400, only=["nlp_text_retention"])
    assert await db.fetchval("SELECT text FROM announcements") is None           # purge time passed
