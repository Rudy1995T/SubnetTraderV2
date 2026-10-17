"""Copy the old SubnetTrader backtest result files read-only to ``data/quant/legacy/`` with an index (spec 19 §2.8, W1).

``python -m scripts.import_legacy_results [--src <SubnetTrader>/data/backtest/results] [--dest data/quant/legacy]``
Idempotent: a file already present with the same size and sha256 is skipped; nothing is deleted.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
import sys
from pathlib import Path

import app.config as app_config

DEFAULT_SRC = Path("/home/sqsq/USB_Recovery/rootfs/home/pi/Desktop/SN_Bot/SubnetTrader/data/backtest/results")


def _sha(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _describe(p: Path) -> dict:
    kind, title = p.suffix.lstrip(".") or "file", None
    if p.suffix == ".json":
        try:
            d = json.loads(p.read_text())
            if isinstance(d, dict):
                title = d.get("name") or d.get("strategy") or d.get("title") or d.get("study")
                kind = d.get("kind") or d.get("type") or "json"
        except (OSError, ValueError):
            kind = "json_unreadable"
    return {"kind": str(kind)[:40], "title": str(title)[:120] if title else None}


def run(src: Path, dest: Path) -> dict:
    if not src.is_dir():
        raise SystemExit(f"source not found: {src}")
    dest.mkdir(parents=True, exist_ok=True)
    files, copied, skipped = [], 0, 0
    for p in sorted(x for x in src.rglob("*") if x.is_file()):
        rel = p.relative_to(src)
        out = dest / rel
        sha = _sha(p)
        if out.exists() and out.stat().st_size == p.stat().st_size and _sha(out) == sha:
            skipped += 1
        else:
            out.parent.mkdir(parents=True, exist_ok=True)
            if out.exists():
                os.chmod(out, stat.S_IWUSR | stat.S_IRUSR)
            shutil.copy2(p, out)
            os.chmod(out, stat.S_IRUSR | stat.S_IRGRP)
            copied += 1
        files.append({"file": str(rel), "size": p.stat().st_size, "mtime": int(p.stat().st_mtime), "sha256": sha,
                      **_describe(p)})
    index = {"source": str(src), "n": len(files), "files": files,
             "note": "old SubnetTrader backtest results (Apr–Aug 2026), read-only reference (19 §2.8)"}
    (dest / "index.json").write_text(json.dumps(index, indent=1))
    return {"n": len(files), "copied": copied, "skipped": skipped, "dest": str(dest)}


def main(argv: list[str] | None = None) -> int:
    s = app_config.settings
    ap = argparse.ArgumentParser(prog="python -m scripts.import_legacy_results")
    ap.add_argument("--src", default=str(DEFAULT_SRC))
    ap.add_argument("--dest", default=str(s.path(s.DATA_DIR) / "quant" / "legacy"))
    a = ap.parse_args(argv)
    print(json.dumps(run(Path(a.src), Path(a.dest))))
    return 0


if __name__ == "__main__":
    sys.exit(main())
