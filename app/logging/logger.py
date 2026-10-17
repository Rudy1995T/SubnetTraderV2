"""Structured logging: console + daily JSONL files (spec 01 §2.10).

Contract: ``logger.info(msg, data={...})`` — no printf args. Each JSONL entry is
``{ts, level, logger, message, data?}``; files are ``<JSONL_DIR>/YYYY-MM-DD.jsonl`` (UTC day),
line-buffered, rotated at ``MAX_BYTES`` to ``.1``, ``.2`` …
"""
from __future__ import annotations

import json
import logging
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import app.config as app_config

MAX_BYTES = 10 * 1024 * 1024


def _utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class JsonlFileHandler(logging.Handler):
    """Writes one JSON object per line into the current UTC day's file."""

    def __init__(self, directory: str | Path | None = None) -> None:
        super().__init__()
        self._dir_override = Path(directory) if directory else None
        self._lock_io = threading.Lock()
        self._fh = None
        self._day: str | None = None
        self._path: Path | None = None

    def _dir(self) -> Path:
        if self._dir_override is not None:
            return self._dir_override
        s = app_config.settings
        return s.path(s.JSONL_DIR)

    def _open(self, day: str) -> None:
        if self._fh:
            try:
                self._fh.close()
            except OSError:
                pass
        d = self._dir()
        d.mkdir(parents=True, exist_ok=True)
        self._path = d / f"{day}.jsonl"
        self._fh = open(self._path, "a", encoding="utf-8", buffering=1)
        self._day = day

    def _rotate(self) -> None:
        assert self._path is not None
        if self._fh:
            self._fh.close()
            self._fh = None
        n = 1
        while self._path.with_name(f"{self._path.name}.{n}").exists():
            n += 1
        for i in range(n, 1, -1):
            self._path.with_name(f"{self._path.name}.{i - 1}").rename(self._path.with_name(f"{self._path.name}.{i}"))
        self._path.rename(self._path.with_name(f"{self._path.name}.1"))
        self._fh = open(self._path, "a", encoding="utf-8", buffering=1)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            entry: dict[str, Any] = {
                "ts": datetime.fromtimestamp(record.created, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            data = getattr(record, "data", None)
            if data is not None:
                entry["data"] = data
            if record.exc_info:
                entry.setdefault("data", {})
                if isinstance(entry["data"], dict):
                    entry["data"]["exc"] = logging.Formatter().formatException(record.exc_info)
            line = json.dumps(entry, default=str, ensure_ascii=False)
            with self._lock_io:
                day = _utc_day()
                if self._fh is None or day != self._day:
                    self._open(day)
                self._fh.write(line + "\n")
                if self._path is not None and self._path.stat().st_size > MAX_BYTES:
                    self._rotate()
        except Exception:  # pragma: no cover - logging must never raise
            self.handleError(record)

    def close(self) -> None:
        with self._lock_io:
            if self._fh:
                try:
                    self._fh.close()
                except OSError:
                    pass
                self._fh = None
        super().close()


class _ConsoleFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        base = f"{ts} {record.levelname:<7} {record.name}: {record.getMessage()}"
        data = getattr(record, "data", None)
        if data is not None:
            base += " " + json.dumps(data, default=str, ensure_ascii=False)[:2000]
        if record.exc_info:
            base += "\n" + self.formatException(record.exc_info)
        return base


class StructuredLogger:
    """Thin wrapper over a stdlib logger with the ``data=`` convention."""

    def __init__(self, name: str = "taosentinel", *, console: bool = True, jsonl: bool = True) -> None:
        self._logger = logging.getLogger(name)
        self._logger.propagate = False
        s = app_config.settings
        self._logger.setLevel(getattr(logging, s.LOG_LEVEL, logging.INFO))
        if not self._logger.handlers:
            if console:
                ch = logging.StreamHandler(sys.stderr)
                ch.setFormatter(_ConsoleFormatter())
                self._logger.addHandler(ch)
            if jsonl:
                self._logger.addHandler(JsonlFileHandler())
        for noisy in ("uvicorn", "uvicorn.error", "uvicorn.access", "apscheduler", "httpx", "httpcore",
                      "aiosqlite", "trafilatura"):
            logging.getLogger(noisy).setLevel(logging.WARNING)

    @property
    def raw(self) -> logging.Logger:
        return self._logger

    def set_level(self, level: str) -> None:
        self._logger.setLevel(getattr(logging, str(level).upper(), logging.INFO))

    def child(self, suffix: str) -> "StructuredLogger":
        c = StructuredLogger.__new__(StructuredLogger)
        c._logger = self._logger.getChild(suffix)
        return c

    def _log(self, level: int, msg: str, data: Any = None, exc_info: bool = False) -> None:
        self._logger.log(level, msg, extra={"data": data} if data is not None else None, exc_info=exc_info)

    def debug(self, msg: str, data: Any = None) -> None:
        self._log(logging.DEBUG, msg, data)

    def info(self, msg: str, data: Any = None) -> None:
        self._log(logging.INFO, msg, data)

    def warning(self, msg: str, data: Any = None) -> None:
        self._log(logging.WARNING, msg, data)

    def error(self, msg: str, data: Any = None, exc_info: bool = False) -> None:
        self._log(logging.ERROR, msg, data, exc_info)

    def critical(self, msg: str, data: Any = None) -> None:
        self._log(logging.CRITICAL, msg, data)

    def exception(self, msg: str, data: Any = None) -> None:
        self._log(logging.ERROR, msg, data, True)


logger = StructuredLogger()


def get_logger(name: str | None = None) -> StructuredLogger:
    return logger if not name else logger.child(name)
