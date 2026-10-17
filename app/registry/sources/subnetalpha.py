"""subnetalpha.ai WP-REST ACF reader (spec 04 §2.4; port of the old ``fetch_subnetalpha`` loop structure)."""
from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import app.config as app_config
from app.logging.logger import logger
from app.util.time import parse_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.http import Http

_NETUID_RE = re.compile(r"(\d+)")

# ACF key → identity field (§2.4 table); text/raw-only fields are observations too, never candidates for URLs
ACF_FIELD_MAP: dict[str, str] = {
    "subnet_owner": "owner_name_editorial", "github_url": "github_repo", "website_url": "subnet_url",
    "twitterx_url": "x_url", "discord_url": "discord", "telegram_url": "telegram", "short_desc": "short_desc",
    "what_exactly_does_it_do": "description_long", "what_exactly_is_the_productbuild": "purpose",
    "team_info": "team_info_html", "roadmap": "roadmap_html", "twitter_embed_id": "twitter_embed_id",
    "video_url": "video_url_0", **{f"additional_video_{i}": f"video_url_{i}" for i in range(1, 9)},
}
EXPECTED_ACF_KEYS = frozenset(ACF_FIELD_MAP) | {"subnet_number"}


def parse_netuid(subnet_number: str | None) -> int | None:
    if not subnet_number:
        return None
    m = _NETUID_RE.search(str(subnet_number))
    return int(m.group(1)) if m else None


@dataclass
class SaRecord:
    netuid: int
    wp_id: int | None
    slug: str | None
    name: str
    modified_ts: int | None
    link: str | None
    acf: dict


@dataclass
class SubnetalphaResult:
    records: dict[int, SaRecord] = field(default_factory=dict)
    pages: int = 0
    wp_total: int | None = None
    missing_acf_keys: set[str] = field(default_factory=set)
    duplicate_netuids: list[int] = field(default_factory=list)
    unparsed: int = 0
    all_records: list[SaRecord] = field(default_factory=list)


class SubnetalphaError(RuntimeError):
    pass


async def fetch_subnetalpha(http: "Http", *, max_pages: int | None = None) -> SubnetalphaResult:
    s = app_config.settings
    max_pages = max_pages or s.REGISTRY_SUBNETALPHA_MAX_PAGES
    res = SubnetalphaResult()
    items_all: list[dict] = []
    for n in range(1, max_pages + 1):
        r = await http.request("subnetalpha", "GET", s.SUBNETALPHA_BASE_URL,
                               params={"per_page": 100, "page": n, "_fields": "id,slug,title,modified_gmt,link,acf"},
                               headers={"Accept": "application/json"}, timeout=s.SUBNETALPHA_TIMEOUT_SEC,
                               ok_statuses=(200, 400))
        if r.status_code == 400:
            break
        if r.status_code != 200:
            raise SubnetalphaError(f"HTTP {r.status_code} on page {n}")
        try:
            items = r.json()
        except ValueError as exc:
            raise SubnetalphaError(f"non-JSON body on page {n} (challenge page?)") from exc
        if not isinstance(items, list):
            raise SubnetalphaError(f"unexpected body type on page {n}")
        if n == 1:
            tot = r.headers.get("x-wp-total")
            res.wp_total = int(tot) if tot and tot.isdigit() else None
            logger.debug("subnetalpha totals", data={"total": tot, "pages": r.headers.get("x-wp-totalpages")})
        res.pages = n
        if not items:
            break
        items_all.extend(items)
        if len(items) < 100:
            break
    present: set[str] = set()
    for it in items_all:
        acf = it.get("acf") or {}
        if isinstance(acf, list):       # WP returns [] for posts without ACF
            acf = {}
        present |= set(acf)
        netuid = parse_netuid(acf.get("subnet_number"))
        if netuid is None:
            res.unparsed += 1
            continue
        mod = it.get("modified_gmt")
        try:
            mts = parse_iso(mod + "Z") if mod and not mod.endswith("Z") else (parse_iso(mod) if mod else None)
        except ValueError:
            mts = None
        title = (it.get("title") or {}).get("rendered") if isinstance(it.get("title"), dict) else it.get("title")
        rec = SaRecord(netuid, it.get("id"), it.get("slug"), html.unescape(title or ""), mts, it.get("link"), acf)
        res.all_records.append(rec)
        old = res.records.get(netuid)
        if old is not None:
            res.duplicate_netuids.append(netuid)
            if (old.modified_ts or 0) >= (mts or 0):
                continue
        res.records[netuid] = rec
    if items_all:
        res.missing_acf_keys = set(EXPECTED_ACF_KEYS) - present
    return res
