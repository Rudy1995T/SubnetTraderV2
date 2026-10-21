"""The six P3 metrics of ts-1.0 (spec 07 §2.8–§2.9, §4) and 07's streak events (§3.5).

One cached loader (``load_web_inputs``) per (occupant, T_eval), a pure core (``derive_web_metrics``), six C5 producer
fns, and the ``web_metrics`` writer (``run_web_metrics``) through 03's ``put_metric_values``. ``D = floor_day(T_eval)``;
the uptime window is the complete check days D−30 … D−1 and "latest" means a row with ``check_day_ts`` in D−2 … D−1,
so every run of a UTC day computes the same values and a stale check never passes 12's 2-day window.
"""
from __future__ import annotations

import json
from collections import Counter, OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Mapping
from urllib.parse import urlsplit

import app.config as app_config
from app.collectors.web import classify as C
from app.features.types import MetricResult
from app.util.time import floor_day, parse_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

DAY = 86400
SPEC_VERSION = "ts-1.0"
P3_KEYS = ("site_uptime_30d", "tls_ok", "site_age", "site_freshness", "docs_present", "product_status")
PRODUCT_MAP = {"none": 0.0, "waitlist": 0.3, "beta": 0.6, "live": 1.0}
WINDOW_LOAD_DAYS = 45
DOCS_LOAD_DAYS = 10
_ROW_COLS = ("id, check_day_ts, event_at, observed_at, url, is_primary, status_class, error, http_status, parked, "
             "tls_ok, tls_days_left, tls_not_after, tls_issuer, tls_hostname_match, content_hash, content_len, "
             "change_ratio, is_spa, robots_allowed, extractor, docs_candidate_url, wayback_first_capture, "
             "wayback_checked_at")


@dataclass(frozen=True, slots=True)
class WebMetricsConfig:
    excluded_hosts: tuple[str, ...]
    max_websites: int = 2
    latest_days: int = 2
    window_days: int = 30
    min_checks: int = 3
    tls_warn_days: int = 14
    freshness_min_delta: float = 0.03
    tiny_body_chars: int = 300
    down_streak_days: int = 7
    docs_gone_days: int = 2

    @classmethod
    def from_settings(cls, s=None) -> "WebMetricsConfig":
        s = s or app_config.settings
        return cls(tuple(s.WEB_EXCLUDED_HOSTS), int(s.WEB_MAX_WEBSITES_PER_OCCUPANT), int(s.WEB_LATEST_MAX_AGE_DAYS),
                   int(s.WEB_UPTIME_WINDOW_DAYS), int(s.WEB_UPTIME_MIN_CHECKS), int(s.WEB_TLS_WARN_DAYS),
                   float(s.WEB_FRESHNESS_MIN_DELTA), int(s.WEB_TINY_BODY_CHARS), int(s.WEB_DOWN_STREAK_DAYS),
                   int(s.WEB_DOCS_GONE_STREAK_DAYS))


@dataclass
class WebInputs:
    occupant_key: str
    netuid: int | None
    t_eval: int
    D: int
    websites: list = field(default_factory=list)          # check.WatchTarget (primary first)
    excluded: list[str] = field(default_factory=list)
    docs_targets: list = field(default_factory=list)
    docs_excluded: list[str] = field(default_factory=list)
    web_rows: list[dict] = field(default_factory=list)    # window rows, kind website, event_at order
    primary_all: list[dict] = field(default_factory=list)  # every row of the current primary URL (streaks)
    docs_rows: list[dict] = field(default_factory=list)
    docs_ever_up: dict[str, int] = field(default_factory=dict)   # url -> last up event_at
    capture: int | None = None
    wayback_checked_at: int | None = None
    first_up_at: int | None = None
    material_change_at: int | None = None
    baseline_at: int | None = None
    streak_events: list[dict] = field(default_factory=list)       # site_down_7d / site_recovered, newest first
    product: dict | None = None

    @property
    def primary_url(self) -> str | None:
        return self.websites[0].url if self.websites else None

    @property
    def second_url(self) -> str | None:
        return self.websites[1].url if len(self.websites) > 1 else None


# ── loader (§8.4) ────────────────────────────────────────────────────────────────────────────────────────────
_CACHE: "OrderedDict[tuple[int, str, int], WebInputs]" = OrderedDict()
_CACHE_MAX = 512


async def load_web_inputs(db: "Database", occupant_key: str, t_eval: int, cfg: WebMetricsConfig | None = None
                          ) -> WebInputs:
    from app.collectors.web.check import select_occupant_targets
    cfg = cfg or WebMetricsConfig.from_settings()
    key = (id(db), occupant_key, int(t_eval))
    hit = _CACHE.get(key)
    if hit is not None:
        return hit
    t = int(t_eval)
    D = floor_day(t)
    occ = await db.fetchone("SELECT netuid FROM occupants WHERE occupant_key = ?", (occupant_key,))
    inp = WebInputs(occupant_key, int(occ["netuid"]) if occ else None, t, D)
    targets = await db.fetchall(
        "SELECT id, occupant_key, netuid, platform, handle, source, priority, is_resolved, first_seen_at "
        "FROM watch_channels WHERE occupant_key = ? AND platform IN ('website','docs') AND first_seen_at <= ? "
        "AND (retired_at IS NULL OR retired_at > ?) ORDER BY id", (occupant_key, t, t))
    sel, excl, _ = select_occupant_targets(targets, list(cfg.excluded_hosts), cfg.max_websites)
    inp.websites = [x for x in sel if x.kind == "website"]
    inp.docs_targets = [x for x in sel if x.kind == "docs"]
    inp.excluded = excl
    inp.docs_excluded = [r["handle"] for r in targets if r["platform"] == "docs"
                         and C.is_excluded_host(urlsplit(r["handle"]).hostname or "", cfg.excluded_hosts)]
    inp.web_rows = await db.fetchall(
        f"SELECT {_ROW_COLS} FROM web_checks WHERE occupant_key = ? AND kind = 'website' AND check_day_ts >= ? "
        "AND check_day_ts <= ? AND observed_at <= ? ORDER BY event_at, id",
        (occupant_key, D - WINDOW_LOAD_DAYS * DAY, D, t))
    inp.docs_rows = await db.fetchall(
        f"SELECT {_ROW_COLS} FROM web_checks WHERE occupant_key = ? AND kind = 'docs' AND check_day_ts >= ? "
        "AND check_day_ts <= ? AND observed_at <= ? ORDER BY event_at, id",
        (occupant_key, D - DOCS_LOAD_DAYS * DAY, D, t))
    for r in await db.fetchall(
            "SELECT url, MAX(event_at) AS at FROM web_checks WHERE occupant_key = ? AND kind = 'docs' AND "
            "status_class IN ('up_2xx','redirect_ok') AND observed_at <= ? AND check_day_ts < ? GROUP BY url",
            (occupant_key, t, D)):
        inp.docs_ever_up[r["url"]] = int(r["at"])
    pu = inp.primary_url
    if pu:
        args = (occupant_key, pu, t, D)
        base = "FROM web_checks WHERE occupant_key = ? AND url = ? AND kind = 'website' AND observed_at <= ? " \
               "AND check_day_ts < ?"
        inp.primary_all = await db.fetchall(
            f"SELECT {_ROW_COLS} FROM web_checks WHERE occupant_key = ? AND url = ? AND kind = 'website' "
            "AND observed_at <= ? ORDER BY event_at, id", (occupant_key, pu, t))
        r = await db.fetchone(f"SELECT wayback_first_capture AS c {base} AND wayback_first_capture IS NOT NULL "
                              "ORDER BY event_at DESC LIMIT 1", args)
        inp.capture = int(r["c"]) if r else None
        r = await db.fetchone(f"SELECT wayback_checked_at AS c {base} AND wayback_checked_at IS NOT NULL "
                              "ORDER BY event_at DESC LIMIT 1", args)
        inp.wayback_checked_at = int(r["c"]) if r else None
        v = await db.fetchval(f"SELECT MIN(event_at) {base} AND status_class IN ('up_2xx','redirect_ok')", args)
        inp.first_up_at = int(v) if v is not None else None
        v = await db.fetchval(f"SELECT MAX(event_at) {base} AND change_ratio IS NOT NULL AND change_ratio <= ?",
                              (*args, 1.0 - cfg.freshness_min_delta))
        inp.material_change_at = int(v) if v is not None else None
        v = await db.fetchval(f"SELECT MIN(event_at) {base} AND content_hash IS NOT NULL", args)
        inp.baseline_at = int(v) if v is not None else None
    inp.streak_events = await db.fetchall(
        "SELECT id, event_type, event_at, payload FROM events WHERE occupant_key = ? AND event_type IN "
        "('site_down_7d','site_recovered') AND observed_at <= ? ORDER BY event_at DESC, id DESC", (occupant_key, t))
    inp.product = await db.fetchone(
        "SELECT id, value, evidence_url, created_at, created_by FROM manual_notes WHERE occupant_key = ? AND "
        "kind = 'product_status' AND created_at <= ? ORDER BY created_at DESC, id DESC LIMIT 1", (occupant_key, t))
    _CACHE[key] = inp
    while len(_CACHE) > _CACHE_MAX:
        _CACHE.popitem(last=False)
    return inp


def clear_cache() -> None:
    _CACHE.clear()


# ── pure core ────────────────────────────────────────────────────────────────────────────────────────────────
def outcome(row: Mapping, prev_outcome: str | None) -> str:
    """§2.4 for one row given the previous considered row's outcome ('up' / anything else / None)."""
    return C.outcome(row["status_class"], row.get("error"), None if prev_outcome is None else prev_outcome == "up")


def _by_url(rows: list[dict]) -> dict[str, list[tuple[dict, str]]]:
    groups: dict[str, list[dict]] = {}
    for r in rows:
        groups.setdefault(r["url"], []).append(r)
    return {u: list(zip(rs, C.outcomes(rs))) for u, rs in groups.items()}


def _recent(rows: list[dict], D: int, days: int) -> list[dict]:
    return [r for r in rows if D - days * DAY <= int(r["check_day_ts"]) <= D - DAY]


def _ok(v: float, **vj: Any) -> MetricResult:
    return MetricResult(float(v), vj)


def _failed(reason: str, **vj: Any) -> MetricResult:
    return MetricResult(None, {"reason": reason, **vj})


def _na(reason: str, **vj: Any) -> MetricResult:
    return MetricResult(None, {"status": "not_applicable", "reason": reason, **vj})


def _absent(reason: str, **vj: Any) -> MetricResult:
    return MetricResult(0.0, vj, absence_reason=reason)


def tls_reason(r: Mapping, warn_days: int) -> str:
    """Most specific first: a cert read with ≤ 0 days left is ``expired`` even when the GET failed on it."""
    dl = r.get("tls_days_left")
    if dl is not None and dl <= 0:
        return "expired"
    if dl is not None and r.get("tls_hostname_match") == 0:
        return "hostname_mismatch"
    if dl is not None and dl <= warn_days:
        return "expiring"
    if r["status_class"] == "tls_fail":
        return "tls_fail"
    return "untrusted" if dl is not None else "no_tls"


def _days(D: int, ts: int) -> float:
    return round(max(0.0, (D - int(ts)) / DAY), 2)


def derive_web_metrics(inp: WebInputs, cfg: WebMetricsConfig) -> dict[str, MetricResult]:
    D = inp.D
    out: dict[str, MetricResult] = {}
    has_web = bool(inp.websites)
    pu = inp.primary_url
    rows = [r for r in inp.web_rows if int(r["check_day_ts"]) < D]
    per_url = _by_url(rows)
    p_rows = [r for r in rows if r["url"] == pu]
    p_recent = _recent(p_rows, D, cfg.latest_days)
    latest = p_recent[-1] if p_recent else None

    # site_uptime_30d
    if not has_web:
        out["site_uptime_30d"] = _absent("no_website", excluded=inp.excluded)
    else:
        lo = D - cfg.window_days * DAY
        cnt = Counter()
        for _, pairs in per_url.items():
            for r, o in pairs:
                if r["is_primary"] == 1 and lo <= int(r["check_day_ts"]) <= D - DAY:
                    cnt[o] += 1
        vj = {"up": cnt["up"], "down": cnt["down"], "indeterminate": cnt["indeterminate"], "url": pu}
        any_recent = any(r["is_primary"] == 1 for r in _recent(rows, D, cfg.latest_days))
        if not any_recent:
            out["site_uptime_30d"] = _failed("no_recent_check", **vj)
        elif cnt["up"] + cnt["down"] < cfg.min_checks:
            out["site_uptime_30d"] = _failed("insufficient_checks", **vj)
        else:
            out["site_uptime_30d"] = _ok(cnt["up"] / (cnt["up"] + cnt["down"]), **vj)

    # tls_ok
    if not has_web:
        out["tls_ok"] = _na("no_website")
    elif latest is None:
        out["tls_ok"] = _failed("no_recent_check", url=pu)
    else:
        vj = {"tls_days_left": latest["tls_days_left"], "tls_not_after": latest["tls_not_after"],
              "tls_issuer": latest["tls_issuer"], "url": pu}
        if latest["tls_ok"] is None:
            out["tls_ok"] = _failed("tls_not_evaluated", **vj)
        elif latest["tls_ok"] == 1:
            out["tls_ok"] = _ok(1.0, **vj)
        else:
            out["tls_ok"] = _ok(0.0, reason=tls_reason(latest, cfg.tls_warn_days), **vj)

    # site_age
    if not has_web:
        out["site_age"] = _na("no_website")
    elif inp.capture is not None:
        out["site_age"] = _ok(_days(D, inp.capture), source="wayback", first_capture=inp.capture, url=pu)
    elif inp.wayback_checked_at is not None:
        if inp.first_up_at is not None:
            out["site_age"] = _ok(_days(D, inp.first_up_at), source="self_observed", first_up=inp.first_up_at,
                                  url=pu)
        else:
            out["site_age"] = _failed("no_capture", url=pu)
    else:
        out["site_age"] = _failed("wayback_unavailable", url=pu)

    # site_freshness
    if not has_web:
        out["site_freshness"] = _na("no_website")
    else:
        up_recent = [r for r in p_recent if r["status_class"] in C.UP_CLASSES]
        if p_recent and all(r["robots_allowed"] == 0 for r in p_recent):
            out["site_freshness"] = _na("robots_head_only", url=pu)
        elif not up_recent:
            out["site_freshness"] = _failed("no_recent_up", url=pu)
        elif up_recent[-1]["is_spa"] == 1 and (up_recent[-1]["content_len"] or 0) < cfg.tiny_body_chars:
            out["site_freshness"] = _na("spa_shell", url=pu)
        elif inp.material_change_at is not None:
            out["site_freshness"] = _ok(_days(D, inp.material_change_at), baseline=False,
                                        changed_at=inp.material_change_at, url=pu)
        elif inp.baseline_at is not None:
            out["site_freshness"] = _ok(_days(D, inp.baseline_at), baseline=True, changed_at=inp.baseline_at, url=pu)
        else:
            out["site_freshness"] = _failed("no_text", url=pu)

    # docs_present
    drows = [r for r in inp.docs_rows if int(r["check_day_ts"]) < D]
    d_recent = _recent(drows, D, cfg.latest_days)
    good = [r for r in d_recent if r["status_class"] in C.UP_CLASSES
            and ((r["content_len"] or 0) >= 200 or r["is_spa"] == 1)]
    if good:
        out["docs_present"] = _ok(1.0, url=good[-1]["url"])
    elif inp.docs_targets:
        gone = []
        d_by = _by_url(drows)
        for u in inp.docs_ever_up:
            defin = [o for _, o in d_by.get(u, []) if o != "indeterminate"]
            n = 0
            for o in reversed(defin):
                if o != "down":
                    break
                n += 1
            gone.append(n >= cfg.docs_gone_days)
        if inp.docs_ever_up and all(gone):
            out["docs_present"] = _ok(0.0, reason="docs_down", urls=sorted(inp.docs_ever_up))
        else:
            out["docs_present"] = _failed("docs_undetermined", urls=[t.url for t in inp.docs_targets])
    elif inp.docs_excluded:
        out["docs_present"] = _failed("docs_undetermined", excluded=inp.docs_excluded)
    elif not has_web:
        out["docs_present"] = _na("no_website")
    elif latest is not None and latest["status_class"] in C.UP_CLASSES and latest["extractor"] not in (None, "none") \
            and not latest.get("docs_candidate_url"):
        out["docs_present"] = _absent("no_docs_link", url=pu)
    else:
        out["docs_present"] = _failed("docs_undetermined", url=pu)

    out["product_status"] = product_status(inp.product)
    return out


def product_status(row: Mapping | None) -> MetricResult:
    if row is None:
        return _failed("unset")
    v = row.get("value")
    if v is None:
        return _failed("cleared", note_id=row["id"], set_at=row["created_at"])
    if v not in PRODUCT_MAP:
        from app.logging.logger import logger
        logger.warning("product_status_invalid", data={"note_id": row["id"], "value": v})
        return _failed("invalid", note_id=row["id"])
    return _ok(PRODUCT_MAP[v], product_status=v, evidence_url=row.get("evidence_url"), set_at=row["created_at"],
               note_id=row["id"])


# ── streak events (§3.5: site_down_7d, site_recovered, docs_gone) ────────────────────────────────────────────
def _trailing(pairs: list[tuple[dict, str]], want: str) -> list[dict]:
    run: list[dict] = []
    for r, o in reversed(pairs):
        if o == "indeterminate":
            continue
        if o != want:
            break
        run.append(r)
    return list(reversed(run))


def streak_events(inp: WebInputs, t_eval: int, cfg: WebMetricsConfig) -> list[dict]:
    ev: list[dict] = []
    occ, pu = inp.occupant_key, inp.primary_url

    def add(event_type: str, event_at: int, payload: dict, dedupe: str) -> None:
        ev.append({"event_type": event_type, "occupant_key": occ, "netuid": inp.netuid, "event_at": int(event_at),
                   "payload": payload, "dedupe_key": dedupe})

    # open streaks: the latest down/recovered event per URL is site_down_7d
    latest_by_url: dict[str, dict] = {}
    for e in inp.streak_events:
        u = json.loads(e["payload"] or "{}").get("url")
        if u and u not in latest_by_url:
            latest_by_url[u] = e
    open_urls = {u: e for u, e in latest_by_url.items() if e["event_type"] == "site_down_7d"}

    if pu:
        pairs = list(zip(inp.primary_all, C.outcomes(inp.primary_all)))
        downs = _trailing(pairs, "down")
        if len(downs) >= cfg.down_streak_days:
            first, nth, last = downs[0], downs[cfg.down_streak_days - 1], downs[-1]
            sec_up = None
            if inp.second_url:
                srows = [r for r in inp.web_rows if r["url"] == inp.second_url]
                so = [o for o in C.outcomes(srows) if o != "indeterminate"]
                sec_up = (so[-1] == "up") if so else None
            add("site_down_7d", nth["event_at"],
                {"url": pu, "streak_days": len(downs), "last_status_class": last["status_class"],
                 "last_http_status": last["http_status"], "secondary_up": sec_up,
                 "streak_start_day": int(first["event_at"]) // DAY},
                f"{occ}|{pu}|{int(first['event_at']) // DAY}")
        oe = open_urls.get(pu)
        if oe is not None:
            defin = [(r, o) for r, o in pairs if o != "indeterminate" and int(r["event_at"]) > int(oe["event_at"])]
            if defin and defin[-1][1] == "up":
                up_row = next(r for r, o in reversed(defin) if o == "up")
                before = [p for p in pairs if int(p[0]["event_at"]) < int(up_row["event_at"])]
                n_down = len(_trailing(before, "down"))
                add("site_recovered", up_row["event_at"], {"url": pu, "down_days": n_down, "reason": "up"},
                    f"{occ}|{pu}|{int(up_row['event_at']) // DAY}")
    for u, oe in open_urls.items():
        if u == pu:
            continue
        reason = "target_retired" if pu is None else "target_changed"      # no website left vs another primary
        down_days = json.loads(oe["payload"] or "{}").get("streak_days")
        add("site_recovered", t_eval, {"url": u, "down_days": down_days, "reason": reason},
            f"{occ}|{u}|{int(t_eval) // DAY}")

    # docs_gone
    d_by = _by_url(inp.docs_rows)
    for u, last_up in inp.docs_ever_up.items():
        downs = _trailing(d_by.get(u, []), "down")
        if len(downs) >= cfg.docs_gone_days and int(downs[0]["event_at"]) > last_up:
            add("docs_gone", downs[-1]["event_at"],
                {"url": u, "last_up_day": floor_day(last_up), "streak_days": len(downs)},
                f"{occ}|{u}|{int(downs[0]['event_at']) // DAY}")
    return ev


# ── C5 producer fns (02 §C5, 12 §2.3) ────────────────────────────────────────────────────────────────────────
async def _compute(db: "Database", occupant_key: str, as_of: datetime, key: str) -> MetricResult:
    cfg = WebMetricsConfig.from_settings()
    inp = await load_web_inputs(db, occupant_key, parse_ts(as_of), cfg)
    return derive_web_metrics(inp, cfg)[key]


async def compute_site_uptime_30d(db, occupant_key: str, as_of: datetime) -> MetricResult:
    return await _compute(db, occupant_key, as_of, "site_uptime_30d")


async def compute_tls_ok(db, occupant_key: str, as_of: datetime) -> MetricResult:
    return await _compute(db, occupant_key, as_of, "tls_ok")


async def compute_site_age(db, occupant_key: str, as_of: datetime) -> MetricResult:
    return await _compute(db, occupant_key, as_of, "site_age")


async def compute_site_freshness(db, occupant_key: str, as_of: datetime) -> MetricResult:
    return await _compute(db, occupant_key, as_of, "site_freshness")


async def compute_docs_present(db, occupant_key: str, as_of: datetime) -> MetricResult:
    return await _compute(db, occupant_key, as_of, "docs_present")


async def compute_product_status(db, occupant_key: str, as_of: datetime) -> MetricResult:
    return await _compute(db, occupant_key, as_of, "product_status")


# ── writer ───────────────────────────────────────────────────────────────────────────────────────────────────
def to_raw_row(key: str, res: MetricResult, *, occupant_key: str, as_of: int, observed_at: int, run_id: str,
               backfilled: int = 0) -> dict:
    vj = res.mapped_json()
    if backfilled:
        vj["backfilled"] = True
    st = vj["status"]
    return {"occupant_key": occupant_key, "metric_key": key, "as_of": int(as_of), "spec_version": SPEC_VERSION,
            "stage": "raw", "value": res.value if st in ("ok", "absent") else None, "value_json": vj,
            "observed_at": int(observed_at), "source": "web", "run_id": run_id, "backfilled": 1 if backfilled else 0}


async def run_web_metrics(db: "Database", *, t_eval: int, run_id: str, observed_at: int | None = None,
                          backfilled: bool = False, netuids: set[int] | None = None, write: bool = True
                          ) -> dict[str, Any]:
    from app.registry import active_occupants
    from app.storage.events import emit_event
    from app.storage.pit import put_metric_values
    cfg = WebMetricsConfig.from_settings()
    occs = [o for o in await active_occupants(db, include_pre_launch=True) if o.netuid != 0
            and (netuids is None or o.netuid in netuids)]
    D = floor_day(t_eval)
    obs_at = int(observed_at if observed_at is not None else t_eval)
    rows: list[dict] = []
    status: Counter = Counter()
    results: dict[str, dict[str, MetricResult]] = {}
    events: list[dict] = []
    for o in occs:
        inp = await load_web_inputs(db, o.occupant_key, t_eval, cfg)
        res = derive_web_metrics(inp, cfg)
        results[o.occupant_key] = res
        for k in P3_KEYS:
            rows.append(to_raw_row(k, res[k], occupant_key=o.occupant_key, as_of=D, observed_at=obs_at,
                                   run_id=run_id, backfilled=int(backfilled)))
            status[f"{k}:{res[k].status}"] += 1
        if not backfilled:
            events += streak_events(inp, t_eval, cfg)
    clear_cache()
    out: dict[str, Any] = {"occupants": len(occs), "as_of": D, "rows": 0, "events": 0, "status": dict(status),
                           "results": results, "pending_events": events}
    if not write:
        return out
    out["rows"] = await put_metric_values(db, rows)
    for e in events:
        try:
            eid = await emit_event(db, severity=None, observed_at=obs_at, source="web", run_id=run_id, **e)
            out["events"] += 1 if eid else 0
        except Exception as exc:
            from app.logging.logger import logger
            logger.warning("web.event_failed", data={"event_type": e["event_type"], "error": str(exc)})
    return out


def as_datetime(ts: int) -> datetime:
    return datetime.fromtimestamp(int(ts), timezone.utc)
