"""Security self-check (spec 16 §2.14): permissions, bind address, auth token, wallet material. Never fatal."""
from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
from pathlib import Path

from app.config import APP_ROOT


def _mode(p: Path) -> int | None:
    try:
        return stat.S_IMODE(p.stat().st_mode)
    except OSError:
        return None


def selfcheck(settings) -> dict:
    env = APP_ROOT / ".env"
    secrets = Path.home() / "taosentinel-secrets"
    data = settings.path(settings.DATA_DIR)
    loopback = settings.HEALTH_HOST in ("127.0.0.1", "::1", "localhost")
    token = bool(settings.API_AUTH_TOKEN)
    env_mode = _mode(env)
    out = {
        "env_mode_ok": env_mode is None or env_mode & 0o077 == 0,
        "env_owner_ok": (not env.exists()) or env.stat().st_uid == os.getuid(),
        "secrets_dir_ok": (not secrets.exists()) or (_mode(secrets) or 0) & 0o077 == 0,
        "data_dir_ok": (not data.exists()) or (_mode(data) or 0) & 0o077 == 0,
        "loopback_bind": loopback,
        "auth_token_set": token,
        "bind_ok": loopback or token,
        "serve_exposes_api": False,
        "env_tracked": False,
        "wallet_dirs_found": 0,
    }
    wallets = Path.home() / ".bittensor" / "wallets"
    n = 0
    if wallets.is_dir():
        n += sum(1 for p in wallets.iterdir() if p.is_dir())
    try:
        for p in APP_ROOT.rglob("*"):
            if p.is_file() and p.name in ("coldkey", "hotkey", "coldkeypub.txt") and ".venv" not in p.parts:
                n += 1
    except OSError:
        pass
    out["wallet_dirs_found"] = n
    if shutil.which("tailscale"):
        try:
            r = subprocess.run(["tailscale", "serve", "status", "--json"], capture_output=True, text=True, timeout=5)
            if r.returncode == 0 and str(settings.HEALTH_PORT) in r.stdout:
                out["serve_exposes_api"] = True
        except (OSError, subprocess.SubprocessError):
            pass
    if (APP_ROOT / ".git").exists() and shutil.which("git"):
        try:
            r = subprocess.run(["git", "-C", str(APP_ROOT), "ls-files", ".env"], capture_output=True, text=True,
                               timeout=5)
            out["env_tracked"] = bool(r.stdout.strip())
        except (OSError, subprocess.SubprocessError):
            pass
    if out["serve_exposes_api"] and not token:
        out["bind_ok"] = False
    out["ok"] = all([out["env_mode_ok"], out["secrets_dir_ok"], out["bind_ok"], not out["env_tracked"]])
    return out


def selfcheck_json(settings) -> str:
    return json.dumps(selfcheck(settings), indent=1)
