"""URL / handle normalisation and classification (spec 04 §2.6). Pure and total: never raises.

``classify(raw, expected_field)`` returns ``[primary, *secondaries]`` (``[]`` for empty input). Each ``Ref`` carries the
field its kind belongs to (``target_field``) so the writer can store secondaries as ``{target}__extra_{i}``.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field, replace
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import app.config as app_config


@dataclass(frozen=True)
class Ref:
    kind: str
    canonical: str
    handle: str | None = None
    extra: dict = field(default_factory=dict)
    is_junk: bool = False
    junk_reason: str | None = None
    reclassified_from: str | None = None
    target_field: str | None = None

    def with_(self, **kw) -> "Ref":
        return replace(self, **kw)


# ── vocabularies ─────────────────────────────────────────────────────────────────────────────────────────────
_JUNK_TOKEN_RE = re.compile(
    r"^(none|null|nil|n/?a|-+|—|~|tbd|tba|todo|pending\.{0,3}|coming soon|soon|deprecated|inactive|for sale|parked|"
    r"placeholder|unknown|awaiting data\.{0,3}|awating data\.{0,3}|test|example|xxx|\.)$", re.I)
_ABANDON_RE = re.compile(r"^(deprecated|for sale|parked|inactive|pending\.{0,3})$", re.I)
_OWNER_PLACEHOLDER_RE = re.compile(r"^owner\s*\d+$", re.I)
_JUNK_HOSTS = {"example.com", "localhost", "deprecated.com", "www.deprecated.com", "deprecated.png", "test.com"}
_JUNK_HOST_RE = re.compile(r"^(deprecated|pending|placeholder)\b", re.I)
_GH_RESERVED = {"orgs", "sponsors", "topics", "settings", "features", "marketplace", "apps", "login", "about", "site",
                "explore", "search", "collections", "trending"}
_GH_PLACEHOLDERS = {("username", "repo"), ("deprecated", "deprecated"), ("user", "repo"), ("owner", "repo"),
                    ("org", "repo"), ("your-org", "your-repo")}
X_HOSTS = {"x.com", "twitter.com", "mobile.twitter.com", "www.x.com", "www.twitter.com", "fxtwitter.com",
           "vxtwitter.com"}
_X_BAD_FIRST = {"i", "intent", "home", "hashtag", "search", "share", "explore", "settings", "messages",
                "notifications", "compose"}
_X_HANDLE_RE = re.compile(r"^[A-Za-z0-9_]{1,15}$")
DISCORD_HOSTS = {"discord.gg", "discord.com", "discordapp.com", "www.discord.com", "www.discordapp.com",
                 "www.discord.gg", "discord.io"}
TELEGRAM_HOSTS = {"t.me", "telegram.me", "telegram.dog", "www.t.me"}
YOUTUBE_HOSTS = {"youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be", "www.youtu.be"}
GITHUB_HOSTS = {"github.com", "www.github.com"}
_SOCIAL_HOSTS = X_HOSTS | DISCORD_HOSTS | TELEGRAM_HOSTS | YOUTUBE_HOSTS | GITHUB_HOSTS
_DISCORD_HANDLE_RE = re.compile(r"^@?[a-z0-9_.]{2,32}$", re.I)
_TG_HANDLE_RE = re.compile(r"^[A-Za-z0-9_]{5,32}$")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_BARE_DOMAIN_RE = re.compile(r"^[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+(/\S*)?$")
_KNOWN_NOSCHEME_RE = re.compile(r"^(www\.|github\.com/|x\.com/|twitter\.com/|discord\.gg/|discord\.com/|t\.me/|"
                                r"youtube\.com/|youtu\.be/)", re.I)
_URL_TOKEN_RE = re.compile(r"(https?://[^\s<>\"')\]]+|www\.[^\s<>\"')\]]+|(?:github\.com|x\.com|twitter\.com|"
                           r"discord\.gg|discord\.com|t\.me|youtu\.be|youtube\.com)/[^\s<>\"')\]]+)", re.I)
_SECRET_RE = re.compile(r"(token=GHSAT[^&\s]*|ghp_[A-Za-z0-9]+|github_pat_[A-Za-z0-9_]+)")
_DOCS_SEGMENTS = {"docs", "documentation", "api-docs", "developers", "dev", "wiki"}
_DOCS_HOST_SUFFIX = ("gitbook.io", "readthedocs.io", "mintlify.app", "notion.site")
_TRACKING = re.compile(r"^(utm_.*|ref|si|feature)$", re.I)

URL_FIELDS = {"github_repo", "subnet_url", "website_url", "url", "discord", "logo_url", "twitterx_url", "telegram_url",
              "x_url", "telegram", "youtube", "owner_url", "owner_github", "owner_discord", "github_url", "discord_url",
              "docs_url", "owner_image"}
TEXT_WITH_URLS = {"additional", "description", "subnet_contact", "owner_additional", "owner_description"}

KIND_FIELD = {
    "website": "subnet_url", "github_repo": "github_repo", "github_owner": "github_repo", "x": "x_url",
    "x_handle": "x_url", "discord_invite": "discord", "discord_channel": "discord", "discord_handle": "discord",
    "telegram_public": "telegram", "telegram_private": "telegram", "youtube_channel": "youtube",
    "youtube_handle": "youtube", "youtube_video": "youtube", "email": "subnet_contact", "docs": "docs_url",
}
# expected kind family per field (for choosing the primary among URL tokens)
FIELD_FAMILY = {
    "github_repo": "github", "owner_github": "github", "github_url": "github",
    "subnet_url": "website", "owner_url": "website", "website_url": "website", "url": "website", "logo_url": "website",
    "docs_url": "website", "owner_image": "website",
    "discord": "discord", "owner_discord": "discord", "discord_url": "discord",
    "x_url": "x", "twitterx_url": "x", "telegram": "telegram", "telegram_url": "telegram", "youtube": "youtube",
}


def _family(kind: str) -> str:
    if kind.startswith("github"):
        return "github"
    if kind in ("x", "x_handle"):
        return "x"
    if kind.startswith("discord"):
        return "discord"
    if kind.startswith("telegram"):
        return "telegram"
    if kind.startswith("youtube"):
        return "youtube"
    if kind in ("website", "docs"):
        return "website"
    return kind


def _field_family(field: str) -> str | None:
    if field.startswith("video_url"):
        return "youtube"
    return FIELD_FAMILY.get(field)


# ── helpers ──────────────────────────────────────────────────────────────────────────────────────────────────
def _preclean(s: str) -> str:
    s = s.strip().strip("\"'").strip()
    if s.startswith("<") and s.endswith(">"):
        s = s[1:-1].strip()
    s = s.rstrip(".,;)")
    return re.sub(r"\s+", " ", s)


def is_junk(raw: str, field: str) -> str | None:
    """junk_reason for values that are dropped (``token``/``too_short``); None otherwise."""
    s = _preclean(raw or "")
    if not s:
        return None
    if _JUNK_TOKEN_RE.match(s):
        return "token"
    if field in ("owner_name", "owner_name_editorial") and _OWNER_PLACEHOLDER_RE.match(s):
        return "token"
    if (field in URL_FIELDS or field.startswith("video_url")) and "." not in s and "/" not in s and len(s) < 3:
        return "too_short"
    return None


def _add_scheme(tok: str, url_field: bool) -> str:
    if re.match(r"^[a-z][a-z0-9+.-]*://", tok, re.I):
        return tok
    if _KNOWN_NOSCHEME_RE.match(tok) or (url_field and _BARE_DOMAIN_RE.match(tok)):
        return "https://" + tok
    return tok


def _split_url(url: str):
    try:
        p = urlsplit(url)
    except ValueError:
        return None
    if p.scheme.lower() not in ("http", "https") or not p.hostname:
        return None
    host = p.hostname.lower().rstrip(".")
    query = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if not _TRACKING.match(k)]
    path = p.path
    if path.endswith("/") and len(path) > 1:
        path = path[:-1]
    if path == "/":
        path = ""
    return p.scheme.lower(), host, path, query


def canonical_website(url: str | None) -> str | None:
    if not url:
        return None
    s = _add_scheme(_preclean(url), True)
    parts = _split_url(s)
    if not parts:
        return None
    scheme, host, path, query = parts
    if "." not in host or host in _JUNK_HOSTS or _JUNK_HOST_RE.match(host) or host in _SOCIAL_HOSTS:
        return None
    return urlunsplit((scheme, host, path, urlencode(query), ""))


def normalise_x_handle(s: str | None) -> str | None:
    if not s:
        return None
    t = _preclean(s)
    if not t or _JUNK_TOKEN_RE.match(t):
        return None
    if t.startswith("@") and "/" not in t:
        h = t[1:]
        return h if _X_HANDLE_RE.match(h) else None
    if "/" not in t and "." not in t:
        return t if _X_HANDLE_RE.match(t) else None
    parts = _split_url(_add_scheme(t, True))
    if not parts or parts[1] not in X_HOSTS:
        return None
    segs = [x for x in parts[2].split("/") if x]
    if not segs:
        return None
    first = segs[0].lstrip("@")
    if first.lower() in _X_BAD_FIRST:
        return None
    return first if _X_HANDLE_RE.match(first) else None


def _redact(s: str) -> str:
    s = re.sub(r"token=[^&\s]*", "token=<redacted>", s)
    s = re.sub(r"ghp_[A-Za-z0-9]+", "ghp_<redacted>", s)
    return re.sub(r"github_pat_[A-Za-z0-9_]+", "github_pat_<redacted>", s)


def _classify_url(tok: str, url_field: bool) -> Ref | None:
    secret = bool(_SECRET_RE.search(tok))
    s = _add_scheme(tok, url_field)
    parts = _split_url(s)
    if not parts:
        return None
    scheme, host, path, query = parts
    segs = [x for x in path.split("/") if x]
    jr = "leaked_secret" if secret else None

    def site(kind="website", **extra) -> Ref:
        canon = urlunsplit((scheme, host, path, urlencode(query), ""))
        if secret:
            canon = _redact(canon)
        junk = host in _JUNK_HOSTS or bool(_JUNK_HOST_RE.match(host)) or "." not in host
        return Ref(kind, canon, None, dict(extra), junk, "junk_host" if junk else jr, target_field=KIND_FIELD[kind])

    if host in GITHUB_HOSTS:
        if not segs:
            return Ref("github_owner", "https://github.com", None, {}, True, "too_short", target_field="github_repo")
        if segs[0].lower() == "orgs" and len(segs) >= 2:
            o = segs[1]
            return Ref("github_owner", f"https://github.com/{o}", o, {"key": o.lower()}, False, jr,
                       target_field="github_repo")
        o = segs[0]
        if o.lower() in _GH_RESERVED:
            return Ref("github_owner", f"https://github.com/{o}", o, {}, True, "placeholder_repo",
                       target_field="github_repo")
        if len(segs) == 1:
            return Ref("github_owner", f"https://github.com/{o}", o, {"key": o.lower()}, False, jr,
                       target_field="github_repo")
        r = segs[1]
        if r.lower().endswith(".git"):
            r = r[:-4]
        if (o.lower(), r.lower()) in _GH_PLACEHOLDERS:
            return Ref("github_repo", f"https://github.com/{o}/{r}", f"{o}/{r}", {"key": f"{o.lower()}/{r.lower()}"},
                       True, "placeholder_repo", target_field="github_repo")
        return Ref("github_repo", f"https://github.com/{o}/{r}", f"{o}/{r}", {"key": f"{o.lower()}/{r.lower()}"},
                   False, jr, target_field="github_repo")
    if host in ("raw.githubusercontent.com", "gist.github.com") or host.endswith(".github.io"):
        return site()
    if host in X_HOSTS:
        h = normalise_x_handle(s)
        if not h:
            return Ref("x", s, None, {}, True, "token", target_field="x_url")
        extra = {}
        if len(segs) >= 3 and segs[1] == "status":
            extra["status_id"] = segs[2]
        return Ref("x", f"https://x.com/{h}", h, extra, False, jr, target_field="x_url")
    if host in DISCORD_HOSTS:
        if host.endswith("discord.gg") or host == "discord.io":
            if not segs:
                return Ref("discord_invite", s, None, {}, True, "too_short", target_field="discord")
            code = segs[0]
            return Ref("discord_invite", f"https://discord.gg/{code}", code,
                       {"is_main_vanity": code.lower() == "bittensor"}, False, jr, target_field="discord")
        if segs and segs[0] == "invite" and len(segs) >= 2:
            code = segs[1]
            return Ref("discord_invite", f"https://discord.gg/{code}", code,
                       {"is_main_vanity": code.lower() == "bittensor"}, False, jr, target_field="discord")
        if segs and segs[0] == "channels" and len(segs) >= 3:
            gid, cid = segs[1], segs[2]
            main = app_config.settings.DISCORD_MAIN_GUILD_ID
            return Ref("discord_channel", f"https://discord.com/channels/{gid}/{cid}", cid,
                       {"guild_id": gid, "is_main_guild": gid == main,
                        "message_id": segs[3] if len(segs) > 3 else None}, False, jr, target_field="discord")
        if segs and segs[0] == "users" and len(segs) >= 2:
            return Ref("discord_handle", f"https://discord.com/users/{segs[1]}", segs[1], {"is_user_id": 1}, False,
                       jr, target_field="discord")
        return Ref("discord_invite", s, None, {}, True, "too_short", target_field="discord")
    if host in TELEGRAM_HOSTS:
        if not segs:
            return Ref("telegram_public", s, None, {}, True, "too_short", target_field="telegram")
        if segs[0] == "s" and len(segs) >= 2:
            segs = segs[1:]
        if segs[0] == "joinchat" and len(segs) >= 2:
            h = "+" + segs[1]
            return Ref("telegram_private", f"https://t.me/{h}", h, {}, False, jr, target_field="telegram")
        if segs[0].startswith("+"):
            return Ref("telegram_private", f"https://t.me/{segs[0]}", segs[0], {}, False, jr, target_field="telegram")
        h = segs[0]
        if _TG_HANDLE_RE.match(h):
            return Ref("telegram_public", f"https://t.me/{h}", h, {}, False, jr, target_field="telegram")
        return Ref("telegram_public", f"https://t.me/{h}", h, {}, True, "token", target_field="telegram")
    if host in YOUTUBE_HOSTS:
        q = dict(query)
        if host.endswith("youtu.be") and segs:
            vid = segs[0]
            return Ref("youtube_video", f"https://www.youtube.com/watch?v={vid}", vid, {"needs_resolution": 1}, False,
                       jr, target_field="youtube")
        if segs and segs[0] == "channel" and len(segs) >= 2 and re.match(r"^UC[\w-]{22}$", segs[1]):
            return Ref("youtube_channel", f"https://www.youtube.com/channel/{segs[1]}", segs[1], {}, False, jr,
                       target_field="youtube")
        if segs and (segs[0].startswith("@") or segs[0] in ("c", "user")):
            h = segs[0] if segs[0].startswith("@") else (f"@{segs[1]}" if len(segs) >= 2 else None)
            if h:
                return Ref("youtube_handle", f"https://www.youtube.com/{h}", h, {"needs_resolution": 1}, False, jr,
                           target_field="youtube")
        if segs and segs[0] == "watch" and q.get("v"):
            v = q["v"]
            return Ref("youtube_video", f"https://www.youtube.com/watch?v={v}", v, {"needs_resolution": 1}, False, jr,
                       target_field="youtube")
        if segs and segs[0] in ("shorts", "embed", "live") and len(segs) >= 2:
            return Ref("youtube_video", f"https://www.youtube.com/watch?v={segs[1]}", segs[1],
                       {"needs_resolution": 1}, False, jr, target_field="youtube")
        return site()
    return site()


def _is_docs(ref: Ref) -> bool:
    if ref.kind != "website" or ref.is_junk:
        return False
    parts = _split_url(ref.canonical)
    if not parts:
        return False
    _, host, path, _ = parts
    first = next((x for x in path.split("/") if x), "")
    return host.startswith("docs.") or first.lower() in _DOCS_SEGMENTS or host.endswith(_DOCS_HOST_SUFFIX)


def extract_docs(refs: list[Ref], text: str = "") -> Ref | None:
    for r in refs:
        if _is_docs(r):
            return r.with_(kind="docs", target_field="docs_url")
    m = re.search(r"Docs:\s*(\S+)", text or "", re.I)
    if m:
        r = _classify_url(m.group(1).rstrip(".,;)"), True)
        if r and r.kind == "website" and not r.is_junk:
            return r.with_(kind="docs", target_field="docs_url")
    return None


def _text_ref(s: str, field: str) -> Ref:
    canon = unicodedata.normalize("NFKC", s).strip()
    jr = None
    junk = False
    if field in ("subnet_name",) and _ABANDON_RE.match(canon):
        jr = "abandonment_marker"
    elif _JUNK_TOKEN_RE.match(canon) or (field in ("owner_name", "owner_name_editorial") and
                                         _OWNER_PLACEHOLDER_RE.match(canon)):
        junk, jr = True, "token"
    return Ref("text", canon, None, {}, junk, jr, target_field=field)


def classify(raw: str | None, expected_field: str) -> list[Ref]:
    try:
        return _classify(raw, expected_field)
    except Exception:  # total by contract
        return [Ref("text", str(raw or "").strip(), None, {}, False, None, target_field=expected_field)]


def _classify(raw: str | None, field: str) -> list[Ref]:
    s = _preclean(raw or "")
    if not s:
        return []
    url_field = field in URL_FIELDS or field.startswith("video_url")
    fam = _field_family(field)
    jt = is_junk(s, field)
    if field == "subnet_name" or (not url_field and field not in TEXT_WITH_URLS):
        return [_text_ref(s, field)]
    if jt:
        if field == "subnet_contact" and _EMAIL_RE.match(s):
            pass
        else:
            return [Ref(fam or "text", s, None, {}, True, jt, target_field=field)]
    # email (contact)
    if _EMAIL_RE.match(s) and " " not in s:
        local = s.split("@", 1)[0]
        junk = bool(_JUNK_TOKEN_RE.match(local))
        return [Ref("email", s.lower(), s.lower(), {}, junk, "token" if junk else None, target_field="subnet_contact")]
    if not url_field:  # free text with embedded URLs
        refs = []
        for tok in _URL_TOKEN_RE.findall(s):
            r = _classify_url(tok.rstrip(".,;)"), True)
            if r:
                refs.append(r)
        primary = _text_ref(s, field)
        if field == "subnet_contact" and refs and len(s.split()) == 1:
            primary = refs[0]
            if refs[0].target_field != field:
                refs = [refs[0].with_(reclassified_from=field)] + refs[1:]
            else:
                refs = refs[1:]
        docs = extract_docs(refs, s)
        if docs:
            refs = [r for r in refs if not (r.kind == "website" and r.canonical == docs.canonical)]
        out = [primary] + refs
        if docs:
            out.append(docs)
        return out
    # URL-typed field: tokenise
    tokens = s.split(" ")
    refs: list[Ref] = []
    for tok in tokens:
        tok = tok.strip().rstrip(".,;)")
        if not tok:
            continue
        r = _classify_url(tok, True)
        if r is None:
            if fam == "discord" and _DISCORD_HANDLE_RE.match(tok) and not tok.strip("@").startswith(".") \
                    and not tok.endswith("."):
                h = tok.lstrip("@").lower()
                r = Ref("discord_handle", h, h, {}, False, None, target_field="discord")
            elif fam == "x" and normalise_x_handle(tok):
                h = normalise_x_handle(tok)
                r = Ref("x_handle", f"https://x.com/{h}", h, {}, False, None, target_field="x_url")
            elif fam == "telegram" and _TG_HANDLE_RE.match(tok.lstrip("@")):
                h = tok.lstrip("@")
                r = Ref("telegram_public", f"https://t.me/{h}", h, {}, False, None, target_field="telegram")
            else:
                continue
        refs.append(r)
    if not refs:
        return [Ref(fam or "text", s, None, {}, True, "too_short" if len(s) < 3 else "unparsed", target_field=field)]
    primary_idx = next((i for i, r in enumerate(refs) if _family(r.kind) == fam), None)
    if primary_idx is None:
        primary = refs[0]
        secondaries = [primary.with_(reclassified_from=field)] + refs[1:]
    else:
        primary = refs[primary_idx]
        secondaries = refs[:primary_idx] + refs[primary_idx + 1:]
    out = [primary] + secondaries
    if field in ("subnet_url", "owner_url", "website_url"):
        docs = extract_docs([primary] + secondaries, s)
        if docs:
            out.append(docs)
    return out


def compare_key(ref_kind: str | None, value_norm: str | None, handle: str | None = None,
                extra: dict | None = None) -> str:
    k = ref_kind or ""
    extra = extra or {}
    if k in ("x", "x_handle", "telegram_public", "telegram_private", "discord_invite", "discord_handle"):
        return (handle or value_norm or "").lower()
    if k == "discord_channel":
        return handle or ""
    if k.startswith("github"):
        return extra.get("key") or (value_norm or "").lower()
    if k == "text":
        return name_key(value_norm or "")
    if k in ("website", "docs"):
        parts = _split_url(value_norm or "")
        if parts:
            host = parts[1][4:] if parts[1].startswith("www.") else parts[1]
            return host + parts[2].lower()
    return (value_norm or "").lower()


def strip_name(s: str) -> str:
    """Casefold + collapse whitespace; drop a trailing ``(SNnn)``/``SN nn`` and symbol suffixes (turnover compare)."""
    t = unicodedata.normalize("NFKC", s or "").casefold()
    t = re.sub(r"\s*\(?\s*sn\s*\d+\s*\)?\s*$", "", t)
    t = re.sub(r"[^\w\s.-]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def name_key(s: str) -> str:
    """Name equality key: ``strip_name`` then letters/digits only ("Compute Horde" == "ComputeHorde")."""
    return re.sub(r"[\W_]+", "", strip_name(s))
