"""Atomic file writes (port of the old ``fundamentals_collect.write_atomic``: tmp + ``os.replace``)."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def write_atomic(path: str | Path, data: Any) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, p)
