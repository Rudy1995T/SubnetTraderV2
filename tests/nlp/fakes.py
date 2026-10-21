"""Spec 10 test doubles: ``FakeAnthropic`` (the SDK call shapes used by app/nlp: batches create/retrieve/results/
cancel, count_tokens, messages.create, beta.messages.create), a keyword-only ``FakeGovernor``, a recording limiter, and
DB seed helpers on 03's DDL. Fixtures replayed from ``tests/fixtures/llm/``."""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

import app.config as app_config
from app.util.time import parse_iso

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "llm"
DAY = 86400
HOUR = 3600
T0 = parse_iso("2026-10-01T00:00:00Z")
REG_TS = T0 - 400 * DAY
BULK_V = "claude-haiku-5-5:e47d012b:1"
ESC_V = "claude-sonnet-5-5:e47d012b:1"
RULES_V = "rules:v2"


def fx(name: str) -> Any:
    return json.loads((FIX / name).read_text(encoding="utf-8"))


def nlp_settings(**kw):
    base = dict(LLM_ENABLED=True, ANTHROPIC_API_KEY="sk-test", LLM_MONTHLY_CAP_USD=60.0, X_ENABLED=False)
    base.update(kw)
    return app_config.Settings(_env_file=None, **base)


# ── fake SDK objects ─────────────────────────────────────────────────────────────────────────────────────────
def usage(i: int = 1000, o: int = 300, cr: int = 0, cw: int = 0) -> SimpleNamespace:
    return SimpleNamespace(input_tokens=i, output_tokens=o, cache_read_input_tokens=cr, cache_creation_input_tokens=cw)


def message(text: str, *, model: str = "claude-haiku-5-5", stop_reason: str = "end_turn", u: Any = None,
            thinking: bool = True, category: str | None = None) -> SimpleNamespace:
    blocks = ([SimpleNamespace(type="thinking", thinking="…")] if thinking else []) + \
        [SimpleNamespace(type="text", text=text)]
    return SimpleNamespace(content=blocks, usage=u or usage(), stop_reason=stop_reason, model=model,
                           stop_details=SimpleNamespace(category=category) if category else None)


def item_obj(pos: int, **kw) -> dict:
    d = {"id": str(pos), "polarity": "neutral", "intensity": 1, "event_type": "other", "direction": 0,
         "materiality": 1, "entities": [], "is_spam": False, "is_promo": False, "summary": f"item {pos}",
         "confidence": 0.8}
    d.update(kw)
    return d


def batch_text(items: list[dict]) -> str:
    return json.dumps({"schema_v": 1, "items": items})


def ok_result(custom_id: str, items: list[dict], **mkw) -> SimpleNamespace:
    return SimpleNamespace(custom_id=custom_id, result=SimpleNamespace(type="succeeded",
                                                                       message=message(batch_text(items), **mkw)))


def err_result(custom_id: str, etype: str = "invalid_request") -> SimpleNamespace:
    return SimpleNamespace(custom_id=custom_id, result=SimpleNamespace(
        type="errored", error=SimpleNamespace(type="error", error=SimpleNamespace(type=etype, message="bad"))))


def gone_result(custom_id: str, kind: str = "expired") -> SimpleNamespace:
    return SimpleNamespace(custom_id=custom_id, result=SimpleNamespace(type=kind))


def n_items(params: dict) -> int:
    content = params["messages"][0]["content"]
    return int(content.split('n="', 1)[1].split('"', 1)[0])


def default_results(req: dict) -> SimpleNamespace:
    """Every request succeeds; every item neutral/other with no entities."""
    p = req["params"]
    return ok_result(req["custom_id"], [item_obj(i + 1) for i in range(n_items(p))])


class _AsyncIter:
    def __init__(self, items: list) -> None:
        self._it = iter(items)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration from None


class FakeError(Exception):
    def __init__(self, status_code: int, msg: str = "fake") -> None:
        super().__init__(msg)
        self.status_code = status_code
        self.response = SimpleNamespace(headers={"retry-after": "7"})


class _Batches:
    def __init__(self, owner: "FakeAnthropic") -> None:
        self.o = owner

    async def create(self, *, requests: list) -> SimpleNamespace:
        self.o.calls.append(("batches.create", len(requests)))
        if self.o.create_error is not None:
            raise self.o.create_error
        bid = f"msgbatch_{len(self.o.batches) + 1:03d}"
        self.o.batches[bid] = {"requests": [dict(r) for r in requests], "status": "in_progress"}
        self.o.submitted.extend(dict(r) for r in requests)
        return SimpleNamespace(id=bid, processing_status="in_progress")

    async def retrieve(self, bid: str) -> SimpleNamespace:
        self.o.calls.append(("batches.retrieve", bid))
        b = self.o.batches[bid]
        return SimpleNamespace(id=bid, processing_status=b["status"],
                               request_counts=SimpleNamespace(succeeded=len(b["requests"]), errored=0, expired=0,
                                                              canceled=0, processing=0))

    async def results(self, bid: str) -> _AsyncIter:
        self.o.calls.append(("batches.results", bid))
        b = self.o.batches[bid]
        out = [self.o.results_fn(r) for r in b["requests"]]
        return _AsyncIter([x for x in reversed(out) if x is not None])           # any order

    async def cancel(self, bid: str) -> SimpleNamespace:
        self.o.calls.append(("batches.cancel", bid))
        self.o.batches[bid]["status"] = "canceling"
        return SimpleNamespace(id=bid, processing_status="canceling")


class _Messages:
    def __init__(self, owner: "FakeAnthropic", beta: bool = False) -> None:
        self.o = owner
        self.beta = beta
        if not beta:
            self.batches = _Batches(owner)

    async def create(self, **kw) -> Any:
        if not self.beta:
            assert "betas" not in kw and "fallbacks" not in kw
        else:
            assert kw.get("betas") == ["server-side-fallback-2026-07-01"] and kw.get("fallbacks") == "default"
        for bad in ("thinking", "temperature", "top_p", "top_k"):
            assert bad not in kw, bad
        assert kw["messages"][-1]["role"] == "user"                                  # never a prefill
        self.o.calls.append(("beta.messages.create" if self.beta else "messages.create", kw["model"]))
        self.o.direct.append(kw)
        r = self.o.direct_fn(kw)
        if isinstance(r, BaseException):
            raise r
        return r

    async def count_tokens(self, **kw) -> SimpleNamespace:
        self.o.calls.append(("count_tokens", kw["model"]))
        return SimpleNamespace(input_tokens=4321)


class FakeAnthropic:
    def __init__(self, results_fn: Callable[[dict], Any] | None = None,
                 direct_fn: Callable[[dict], Any] | None = None) -> None:
        self.calls: list[tuple] = []
        self.batches: dict[str, dict] = {}
        self.submitted: list[dict] = []
        self.direct: list[dict] = []
        self.results_fn = results_fn or default_results
        self.direct_fn = direct_fn or (lambda kw: message(json.dumps(item_obj(1)), model=kw["model"]))
        self.create_error: BaseException | None = None
        self.messages = _Messages(self)
        self.beta = SimpleNamespace(messages=_Messages(self, beta=True))

    def end_all(self) -> None:
        for b in self.batches.values():
            b["status"] = "ended"

    def sdk_calls(self) -> int:
        return len(self.calls)


# ── governor / limiter doubles ──────────────────────────────────────────────────────────────────────────────
class FakeGovernor:
    """02 §G5 signatures, keyword-only after the amount (a positional extra raises TypeError)."""

    def __init__(self, *, allow: bool = True, reserve_ok: bool = True, commit_unknown: bool = False) -> None:
        self.allow = allow
        self.reserve_ok = reserve_ok
        self.commit_unknown = commit_unknown
        self.log: list[tuple] = []
        self._n = 0

    async def check(self, provider: str, est_usd: float):
        self.log.append(("check", provider, est_usd))
        return SimpleNamespace(allowed=self.allow, reason=None if self.allow else "kill_switch")

    async def reserve(self, provider: str, est_usd: float, *, job: str, run_id: str | None):
        self.log.append(("reserve", provider, est_usd, job))
        if not self.reserve_ok:
            return None
        self._n += 1
        return SimpleNamespace(id=f"resv-{self._n}", est_usd=est_usd)

    async def commit(self, res: Any, actual_usd: float, *, detail: dict | None = None) -> None:
        self.log.append(("commit", res, actual_usd, detail))
        if self.commit_unknown:
            from app.costs import UnknownReservation
            raise UnknownReservation(res)

    async def release(self, res: Any) -> None:
        self.log.append(("release", res))

    async def record(self, provider: str, usd: float, *, requests: int = 0, units: dict | None = None,
                     job: str | None = None, run_id: str | None = None, detail: dict | None = None) -> None:
        self.log.append(("record", provider, usd, requests, units, job, detail))

    def paused_reason(self, name: str) -> str | None:
        return None

    def of(self, kind: str) -> list[tuple]:
        return [x for x in self.log if x[0] == kind]


class FakeLimiter:
    def __init__(self) -> None:
        self.acquired: list[str] = []
        self.penalized: list[tuple[str, float]] = []

    async def acquire(self, provider: str, **kw) -> bool:
        self.acquired.append(provider)
        return True

    def penalize(self, provider: str, seconds: float) -> None:
        self.penalized.append((provider, seconds))


# ── DB seeds (03 DDL) ───────────────────────────────────────────────────────────────────────────────────────
def okey(netuid: int, reg: int = 1000) -> str:
    return f"{netuid}:{reg}"


async def seed_occupant(db, netuid: int, *, reg: int = 1000, reg_ts: int = REG_TS, name: str | None = None,
                        symbol: str | None = None, website: str | None = None, current: bool = True,
                        handle: str | None = None) -> str:
    k = okey(netuid, reg)
    await db.execute("INSERT OR IGNORE INTO occupants (occupant_key, netuid, network_registered_at, status, name, "
                     "symbol, first_seen_block, first_seen_at, last_seen_block, last_seen_at, registered_ts) "
                     "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                     (k, netuid, reg, "active", name or f"SN{netuid}", symbol, reg, reg_ts, reg + 10, T0, reg_ts))
    if current:
        await db.execute("INSERT OR REPLACE INTO subnets (netuid, occupant_key, network_registered_at, name, symbol, "
                         "status, website_url, updated_at) VALUES (?,?,?,?,?,?,?,?)",
                         (netuid, k, reg, name or f"SN{netuid}", symbol, "active", website, T0))
    if handle:
        await db.execute("INSERT OR IGNORE INTO watch_channels (occupant_key, netuid, platform, handle, source, "
                         "first_seen_at, last_seen_at) VALUES (?,?,?,?,?,?,?)",
                         (k, netuid, "x", handle, "chain", T0 - DAY, T0))
    return k


async def seed_x(db, pid: str, text: str | None, *, event_at: int, observed_at: int | None = None,
                 author_id: str = "a1", occupant_key: str | None = None, is_team: int = 0, is_kol: int = 0,
                 kol_handle: str | None = None, credibility: float | None = None, bot_filtered: int = 0,
                 likes: int | None = 0, reposts: int | None = 0, replies: int | None = 0, quotes: int | None = 0,
                 mention: str | None = None, text_hash: str | None = None) -> str:
    from app.nlp.textnorm import text_hash as th
    await db.execute(
        "INSERT INTO social_posts (platform, post_id, author_id, author_handle, occupant_key, is_team_account, is_kol, "
        "kol_handle, text, text_hash, url, like_count, repost_count, reply_count, quote_count, author_credibility, "
        "bot_filtered, event_at, observed_at, source) VALUES ('x',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'twitterapi_io')",
        (pid, author_id, f"h{author_id}", occupant_key, is_team, is_kol, kol_handle, text,
         text_hash or th(text or f"x{pid}"), f"https://x.com/i/status/{pid}", likes, reposts, replies, quotes,
         credibility, bot_filtered, event_at, observed_at if observed_at is not None else event_at + 60))
    if mention:
        await db.execute("INSERT OR IGNORE INTO social_post_mentions (platform, post_id, occupant_key, match_kind, "
                         "event_at, observed_at) VALUES ('x',?,?,'query',?,?)",
                         (pid, mention, event_at, observed_at if observed_at is not None else event_at + 60))
    return f"x:{pid}"


async def seed_ann(db, msg_id: str, text: str | None, *, event_at: int, observed_at: int | None = None,
                   occupant_key: str | None = None, via_own_server: int = 1, purge_after: int | None = None,
                   author_is_team: int | None = 1, channel: str = "c1", deleted_at: int | None = None) -> str:
    from app.nlp.textnorm import text_hash as th
    plat, native = msg_id.split(":", 1)
    obs = observed_at if observed_at is not None else event_at + 60
    await db.execute(
        "INSERT INTO announcements (msg_id, platform, occupant_key, netuid, source_guild_id, source_channel_id, "
        "message_id, via_own_server, mapping, author_is_team, text, text_purge_after, text_hash, text_len, event_at, "
        "observed_at, deleted_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (msg_id, plat, occupant_key, int(occupant_key.split(":")[0]) if occupant_key else None,
         "g1" if plat == "discord" else None, channel, native.split("/")[-1], via_own_server,
         "channel" if plat == "discord" else "tg_handle", author_is_team, text, purge_after,
         th(text) if text else th("msg:" + msg_id), len(text) if text else None, event_at, obs, deleted_at))
    return msg_id


async def seed_news(db, url_hash: str, title: str, summary: str | None, *, event_at: int,
                    occupant_keys: list[str] | None = None, observed_at: int | None = None) -> str:
    from app.nlp.textnorm import text_hash as th
    await db.execute(
        "INSERT INTO news_items (url_hash, url, title, summary, feed_url, occupant_keys_json, text_hash, event_at, "
        "event_at_source, observed_at, source) VALUES (?,?,?,?,?,?,?,?,'feed',?,'rss')",
        (url_hash, f"https://news.example/{url_hash}", title, summary, "https://news.example/feed",
         json.dumps([{"occupant_key": k, "kind": "rule"} for k in occupant_keys or []]),
         th(title + "\n" + (summary or "")),
         event_at, observed_at if observed_at is not None else event_at + 60))
    return f"news_item:{url_hash}"


async def seed_video(db, vid: str, title: str, desc: str | None, *, event_at: int, occupant_key: str | None = None,
                     observed_at: int | None = None) -> str:
    await db.execute(
        "INSERT INTO video_items (video_id, channel_id, occupant_key, title, description, views, event_at, "
        "observed_at, source) VALUES (?,?,?,?,?,?,?,?,'channel_rss')",
        (vid, "UC1", occupant_key, title, desc, 10, event_at,
         observed_at if observed_at is not None else event_at + 60))
    return f"video_item:{vid}"


async def seed_label(db, post_id: str, *, occ: str | None, event_at: int, observed_at: int, version: str = BULK_V,
                     tier: str = "bulk", event_type: str = "other", direction: int = 0, materiality: int = 1,
                     polarity: str = "neutral", intensity: int = 1, conf: float = 0.9, is_spam: int = 0,
                     is_promo: int = 0, summary: str | None = None, model_resolved: str | None = None,
                     item_kind: str | None = None, via: str = "model") -> None:
    kind = item_kind or ("social_post" if post_id.startswith("x:") else "announcement"
                         if post_id.startswith(("discord:", "telegram:")) else post_id.split(":", 1)[0])
    item_id = post_id if kind in ("social_post", "announcement") else post_id.split(":", 1)[1]
    ents = [{"netuid": int(occ.split(":")[0]), "occupant_key": occ, "confidence": conf, "via": via}] if occ else []
    await db.execute(
        "INSERT INTO post_labels (post_id, item_kind, item_id, classifier_version, classifier_tier, polarity, "
        "intensity, event_type, direction, materiality, entities_json, is_spam, is_promo, summary, confidence, "
        "event_at, observed_at, source, run_id, model_resolved) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (post_id, kind, item_id, version, tier, polarity, intensity, event_type, direction, materiality,
         json.dumps(ents), is_spam, is_promo, summary or f"{event_type} {post_id}", 0.8, event_at, observed_at,
         "rules" if tier == "rules" else "llm", "seed", model_resolved))
    if occ and conf >= 0.6:
        await db.execute("INSERT INTO post_label_entities (post_id, classifier_version, occupant_key, netuid, "
                         "confidence, via, observed_at) VALUES (?,?,?,?,?,?,?)",
                         (post_id, version, occ, int(occ.split(":")[0]), conf, via, observed_at))
