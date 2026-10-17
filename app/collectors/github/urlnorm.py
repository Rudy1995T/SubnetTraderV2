"""GitHub reference normaliser (spec 06 §2.2): every GitHub ref in a free-text string → ``DeclaredRef``.

Pure (no I/O). Ports only the *idea* of the old bot's ``_GH_RE``; ``orgs/{o}`` is an owner ref, not a rejection
(the old defect that dropped netuids 105 and 118).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Literal
from urllib.parse import urlsplit

_OWNER_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,38})$")
_REPO_RE = re.compile(r"^[a-z0-9._-]{1,100}$")
_SPLIT_RE = re.compile(r"[\s,;|<>()\[\]{}\"'`]+")

RESERVED_FIRST_SEGMENTS = frozenset({
    "topics", "sponsors", "settings", "features", "marketplace", "explore", "login", "apps", "about", "pricing",
    "site", "enterprise", "collections", "trending", "events", "security", "search", "new", "notifications",
})
PLACEHOLDER_PAIRS = frozenset({"username/repo", "owner/repo", "deprecated/deprecated", "org/repo", "user/repo",
                               "example/example"})
PLACEHOLDER_OWNERS = frozenset({"username", "user", "owner", "org", "yourname", "your-org", "example", "deprecated",
                                "placeholder", "tbd", "n-a"})
PLACEHOLDER_REPOS = frozenset({"repo", "repository", "deprecated", "tbd", "todo", "placeholder", "example",
                               "coming-soon", "your-repo", "subnet-template-placeholder"})


@dataclass(frozen=True)
class DeclaredRef:
    kind: Literal["repo", "owner"]
    owner: str            # as written (case preserved for display)
    repo: str | None      # None for owner-only
    key: str              # f"{owner.lower()}/{repo.lower()}" or owner.lower()
    raw: str
    placeholder: bool     # True → verifiable absence

    @property
    def full_name(self) -> str | None:
        return f"{self.owner}/{self.repo}" if self.repo else None


def _denylist(extra: str | Iterable[str] | None) -> frozenset[str]:
    if extra is None:
        import app.config as app_config
        extra = app_config.settings.GITHUB_EXTRA_DENYLIST
    if isinstance(extra, str):
        extra = extra.split(",")
    return frozenset(x.strip().lower() for x in extra if x and x.strip())


def _is_placeholder(owner_l: str, repo_l: str | None, extra: frozenset[str]) -> bool:
    key = f"{owner_l}/{repo_l}" if repo_l else owner_l
    if key in extra or owner_l in PLACEHOLDER_OWNERS:
        return True
    if repo_l is not None and (key in PLACEHOLDER_PAIRS or repo_l in PLACEHOLDER_REPOS):
        return True
    return False


def _clean(seg: str) -> str:
    return seg.split("?", 1)[0].split("#", 1)[0]


def _segments(token: str) -> tuple[str, list[str]] | None:
    """(host, path segments) of one token, or None when it is not a GitHub-family URL."""
    t = token.strip().rstrip(".:!")
    low = t.lower()
    if low.startswith("git@github.com:"):
        return "github.com", [s for s in (_clean(x) for x in t[len("git@github.com:"):].split("/")) if s]
    if "github.com" not in low and "githubusercontent.com" not in low and "github.io" not in low:
        return None
    if "://" not in t:
        t = "https://" + t.lstrip("/")
    try:
        parts = urlsplit(t)
    except ValueError:
        return None
    host = (parts.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    segs = [s for s in (_clean(x) for x in parts.path.split("/")) if s]
    return host, segs


def _ref(owner: str, repo: str | None, raw: str, extra: frozenset[str]) -> DeclaredRef | None:
    owner_l = owner.lower()
    if not _OWNER_RE.match(owner_l):
        return None
    repo_l = None
    if repo is not None:
        if repo.lower().endswith(".git"):
            repo = repo[:-4]
        repo_l = repo.lower()
        if not _REPO_RE.match(repo_l) or repo_l in (".", ".."):
            return None
    key = f"{owner_l}/{repo_l}" if repo_l else owner_l
    return DeclaredRef("repo" if repo else "owner", owner, repo, key, raw, _is_placeholder(owner_l, repo_l, extra))


def _token_ref(token: str, extra: frozenset[str]) -> DeclaredRef | None:
    hs = _segments(token)
    if hs is None:
        return None
    host, segs = hs
    if host.endswith(".github.io"):
        return _ref(host[: -len(".github.io")], None, token, extra)
    if host == "api.github.com":
        if len(segs) >= 3 and segs[0].lower() == "repos":
            return _ref(segs[1], segs[2], token, extra)
        if len(segs) >= 2 and segs[0].lower() in ("orgs", "users"):
            return _ref(segs[1], None, token, extra)
        return None
    if host == "raw.githubusercontent.com":
        return _ref(segs[0], segs[1], token, extra) if len(segs) >= 2 else None
    if host != "github.com" or not segs:
        return None
    first = segs[0].lower()
    if first in ("orgs", "users"):
        return _ref(segs[1], None, token, extra) if len(segs) >= 2 else None
    if first in RESERVED_FIRST_SEGMENTS:
        return None
    if len(segs) == 1:
        return _ref(segs[0], None, token, extra)
    return _ref(segs[0], segs[1], token, extra)


def normalise(raw: str | None, *, extra_denylist: str | Iterable[str] | None = None) -> list[DeclaredRef]:
    """Every GitHub ref in ``raw`` (comma/space-separated lists allowed), deduplicated by key, first first."""
    if not raw:
        return []
    extra = _denylist(extra_denylist)
    out: list[DeclaredRef] = []
    seen: set[str] = set()
    for tok in _SPLIT_RE.split(raw):
        if not tok:
            continue
        ref = _token_ref(tok, extra)
        if ref is not None and ref.key not in seen:
            seen.add(ref.key)
            out.append(ref)
    return out


def first_ref(raw: str | None, *, extra_denylist: str | Iterable[str] | None = None) -> DeclaredRef | None:
    """Within one string: its first ``repo`` ref, else its first ``owner`` ref (06 §2.3)."""
    refs = normalise(raw, extra_denylist=extra_denylist)
    return next((r for r in refs if r.kind == "repo"), None) or (refs[0] if refs else None)


def _name_token(name: str | None) -> str | None:
    """The subnet name's longest alphanumeric run of ≥ 4 chars, lower-cased (pick_primary's +1 bonus)."""
    if not name:
        return None
    toks = [t for t in re.findall(r"[a-z0-9]+", name.lower()) if len(t) >= 4]
    return max(toks, key=len) if toks else None
