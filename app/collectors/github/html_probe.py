"""Zero-PAT-quota rename detector (spec 06 §2.1, §8): ``github.com/{full_name}`` HTML on provider ``github_html``.

Advisory only: a 301 or an ``octolytics`` nwo that differs from the stored ``full_name`` queues the API re-verify
(``github_declared_refs.last_attempt_at = NULL``) and records an ``html`` alias; it never changes state by itself.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from app.collectors.github.client import GitHubUnavailable
from app.logging.logger import logger
from app.util.time import to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.collectors.github.client import GitHubClient
    from app.storage.db import Database

_META = re.compile(r'<meta\s+name="octolytics-dimension-([a-z_]+)"\s+content="([^"]*)"', re.I)
DISABLE_S = 86400


@dataclass(slots=True)
class HtmlProbeResult:
    status: int
    repo_id: int | None = None
    nwo: str | None = None
    is_fork: bool | None = None
    location: str | None = None
    parse_miss: bool = False


def parse_octolytics(html: str) -> dict[str, str]:
    return {m.group(1).lower(): m.group(2) for m in _META.finditer(html or "")}


async def probe(client: "GitHubClient", full_name: str) -> HtmlProbeResult:
    r = await client.get_html(full_name)
    if r.status != 200:
        return HtmlProbeResult(r.status, location=r.location)
    meta = parse_octolytics(r.text or "")
    rid = meta.get("repository_id")
    if not rid or not rid.isdigit():
        return HtmlProbeResult(200, parse_miss=True)
    fork = meta.get("repository_is_fork")
    return HtmlProbeResult(200, int(rid), (meta.get("repository_nwo") or "").lower() or None,
                           None if fork is None else fork.lower() == "true")


async def run(db: "Database", client: "GitHubClient", *, now: int, deadline: float | None,
              netuids: set[int] | None = None, disabled_until: int | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"checked": 0, "rename_hints": 0, "parse_miss": 0, "errors": 0, "disabled_until": None,
                           "lines": []}
    if disabled_until and disabled_until > now:
        out["disabled_until"] = disabled_until
        return out
    q = ("SELECT DISTINCT g.repo_id, g.full_name FROM github_declared_refs d JOIN github_repos g ON g.repo_id = "
         "d.repo_id JOIN occupants o ON o.occupant_key = d.occupant_key WHERE d.kind = 'repo' AND g.status = 'ok'")
    params: list[Any] = []
    if netuids:
        q += f" AND o.netuid IN ({','.join('?' * len(netuids))})"
        params += sorted(netuids)
    rows = await db.fetchall(q + " ORDER BY g.repo_id", params)
    # declared strings that differ from the canonical name (e.g. rayonlabs/chutes) are probed too
    q2 = ("SELECT DISTINCT d.repo_id, lower(d.owner || '/' || d.repo) AS name FROM github_declared_refs d JOIN "
          "occupants o ON o.occupant_key = d.occupant_key WHERE d.kind = 'repo' AND d.repo_id IS NOT NULL")
    if netuids:
        q2 += f" AND o.netuid IN ({','.join('?' * len(netuids))})"
    extra = await db.fetchall(q2, sorted(netuids) if netuids else [])
    targets = [(int(r["repo_id"]), r["full_name"]) for r in rows]
    canon = {rid: name for rid, name in targets}
    names = {n for _, n in targets}
    targets += [(int(r["repo_id"]), r["name"]) for r in extra if r["name"] not in names]
    for rid, name in targets[:600]:
        if deadline is not None and client.clock() >= deadline:
            break
        try:
            res = await probe(client, name)
        except GitHubUnavailable as exc:
            out["errors"] += 1
            logger.debug("github.html_probe.error", data={"name": name, "error": str(exc)[:200]})
            continue
        out["checked"] += 1
        if res.status == 429:
            out["disabled_until"] = now + DISABLE_S
            logger.warning("github.html_probe.disabled", data={"until": to_iso(now + DISABLE_S), "why": "429"})
            break
        hint = None
        if res.status in (301, 302, 307, 308):
            to = (res.location or "").split("github.com/")[-1].strip("/").lower()
            out["lines"].append(f"{name} → {res.status} → {to}")
            if to != canon.get(rid):                     # a redirect to the known canonical name is no news
                hint = res.location
        elif res.parse_miss:
            out["parse_miss"] += 1
            logger.info("html_probe_parse_miss", data={"name": name})
        elif res.status == 200:
            out["lines"].append(f"{name} 200 id={res.repo_id} nwo={res.nwo} fork={str(res.is_fork).lower()}")
            if res.nwo and res.nwo != name.lower():
                hint = res.nwo
            if res.repo_id and res.repo_id != rid:
                hint = hint or f"id {res.repo_id}"
        if hint:
            out["rename_hints"] += 1
            await db.execute("UPDATE github_declared_refs SET last_attempt_at = NULL WHERE repo_id = ?", (rid,))
            if res.nwo:
                await db.execute(
                    "INSERT INTO github_repo_aliases (repo_id, full_name, source, first_seen_at, last_seen_at) "
                    "VALUES (?,?,?,?,?) ON CONFLICT(repo_id, full_name) DO UPDATE SET last_seen_at = "
                    "excluded.last_seen_at", (rid, res.nwo, "html", now, now))
    if out["checked"] >= 10 and out["parse_miss"] * 2 > out["checked"]:
        out["disabled_until"] = now + DISABLE_S
        logger.warning("github.html_probe.disabled", data={"until": to_iso(now + DISABLE_S), "why": "parse_miss"})
    out["parse_miss_rate"] = round(out["parse_miss"] / out["checked"], 3) if out["checked"] else 0.0
    return out
