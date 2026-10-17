"""Path resolution against APP_ROOT (never the cwd — old bot's cwd-relative trap)."""
from __future__ import annotations

from pathlib import Path

import app.config as app_config


def resolve(rel: str | Path) -> Path:
    return app_config.settings.path(rel)


def ensure_dirs() -> None:
    """Create the runtime directories (systemd's ExecStartPre does this under the unit)."""
    s = app_config.settings
    for rel in (s.DATA_DIR, s.JSONL_DIR, s.DB_BACKUP_DIR, s.RUN_DIR):
        s.path(rel).mkdir(parents=True, exist_ok=True)
    s.path(s.DB_BACKUP_DIR).joinpath("manual").mkdir(parents=True, exist_ok=True)
    for sub in ("tmp", "cache", "quarantine"):
        s.path(s.DATA_DIR).joinpath(sub).mkdir(parents=True, exist_ok=True)
