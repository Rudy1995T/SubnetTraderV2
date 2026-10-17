"""Log retention: delete JSONL files older than LOG_RETENTION_DAYS (spec 01 §2.10)."""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

import app.config as app_config
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger

_NAME = re.compile(r"^(\d{4}-\d{2}-\d{2})\.jsonl(?:\.\d+)?$")


def prune_old_logs() -> int:
    """Remove ``YYYY-MM-DD.jsonl[.N]`` files whose day is older than the retention window; returns count."""
    s = app_config.settings
    d = s.path(s.JSONL_DIR)
    if not d.is_dir():
        return 0
    cutoff = (datetime.now(timezone.utc) - timedelta(days=s.LOG_RETENTION_DAYS)).strftime("%Y-%m-%d")
    removed = 0
    for p in d.iterdir():
        m = _NAME.match(p.name)
        if not m or m.group(1) >= cutoff:
            continue
        try:
            p.unlink()
            removed += 1
        except OSError as exc:
            logger.warning("log prune failed", data={"file": p.name, "error": str(exc)})
    if removed:
        logger.info("old logs pruned", data={"removed": removed, "retention_days": s.LOG_RETENTION_DAYS})
    return removed


class LogRetentionJob(CollectorJob):
    async def collect(self, run: RunContext) -> JobOutcome:
        return JobOutcome(rows=prune_old_logs())
