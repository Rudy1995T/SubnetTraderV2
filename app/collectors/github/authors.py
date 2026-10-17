"""Author identity and bot detection (spec 06 §2.6). Raw emails are never stored."""
from __future__ import annotations

import hashlib
import re
from typing import TYPE_CHECKING, Iterable

import app.config as app_config

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

BOT_LOGINS = frozenset({
    "dependabot", "dependabot-preview", "github-actions", "renovate", "imgbot", "snyk-bot", "codecov",
    "allcontributors", "pre-commit-ci", "mergify", "github-merge-queue", "semantic-release-bot",
    "devin-ai-integration", "copilot-swe-agent",
})
_NOREPLY = re.compile(r"^(?:\d+\+)?([a-z0-9-]+)@users\.noreply\.github\.com$")
_BOT_NOREPLY = re.compile(r"^\d+\+.*\[bot\]@users\.noreply\.github\.com$")


def _bot_logins() -> frozenset[str]:
    extra = app_config.settings.GITHUB_BOT_LOGINS_EXTRA
    return BOT_LOGINS | {x.strip().lower() for x in extra.split(",") if x.strip()}


def is_bot(login: str | None = None, *, author_type: str | None = None, name: str | None = None,
           email: str | None = None) -> bool:
    if author_type == "Bot":
        return True
    lg = (login or "").lower()
    if lg:
        if lg.endswith("[bot]") or lg in _bot_logins() or lg.removesuffix("[bot]") in _bot_logins():
            return True
    if name and "[bot]" in name.lower():
        return True
    if email and _BOT_NOREPLY.match(email.strip().lower()):
        return True
    return False


def email_hash(email: str | None) -> str | None:
    """sha256 of the lower-cased email (03 ``author_email_hash``)."""
    if not email or not email.strip():
        return None
    return hashlib.sha256(email.strip().lower().encode()).hexdigest()


def email_key(email: str) -> str:
    return "email:" + hashlib.sha1(email.strip().lower().encode()).hexdigest()[:16]


def author_key(login: str | None, email: str | None) -> str:
    """login → noreply login → ``email:<sha1[:16]>`` → ``unknown``."""
    if login:
        return login.lower()
    e = (email or "").strip().lower()
    if e:
        m = _NOREPLY.match(e)
        if m:
            return m.group(1)
        return email_key(e)
    return "unknown"


def alias_for(login: str | None, email: str | None) -> tuple[str, str] | None:
    """(alias_key, canonical_key) when one commit carries both a login and a (non-noreply) email."""
    if not login or not email or not email.strip():
        return None
    e = email.strip().lower()
    if _NOREPLY.match(e):
        return None
    return email_key(e), login.lower()


async def record_identities(db: "Database", pairs: Iterable[tuple[str, str]], now: int) -> int:
    rows = [{"alias_key": a, "canonical_key": c, "first_seen_at": int(now)} for a, c in dict(pairs).items()]
    if not rows:
        return 0
    return await db.upsert("github_author_identities", rows, conflict=("alias_key",), update=("canonical_key",))


async def resolve_author_keys(db: "Database", keys: Iterable[str]) -> dict[str, str]:
    """alias → canonical for every ``email:`` key that has been seen next to a login."""
    ks = sorted({k for k in keys if k.startswith("email:")})
    out: dict[str, str] = {}
    for i in range(0, len(ks), 500):
        part = ks[i:i + 500]
        ph = ",".join("?" * len(part))
        for r in await db.fetchall(f"SELECT alias_key, canonical_key FROM github_author_identities "
                                   f"WHERE alias_key IN ({ph})", part):
            out[r["alias_key"]] = r["canonical_key"]
    return out
