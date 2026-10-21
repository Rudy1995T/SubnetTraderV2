"""Anthropic Batch API classification, escalation and the two LLM jobs (spec 10 §2.2, §2.3, §6, §7, §8.2, §8.3).

Lifecycle (``llm_batches.status``): ``building → submitted → ended → applied`` (or ``failed``/``expired``). Submit:
queue → dedup by ``text_hash`` → pack 20/request → ``governor.reserve`` (id persisted in ``job_state``) →
``batches.create``. Poll: ``retrieve`` → when ended download results (any order, keyed by ``custom_id``; prompt id
``k`` ↔ the request's k-th ``post_id``) → apply (``INSERT OR IGNORE``, idempotent) → escalate (single Sonnet calls) →
materialise events → ``governor.commit`` → ``applied``. Every SDK call first awaits ``limiter.acquire("anthropic")``
and books its request with ``governor.record`` (§6.2); this module is the one sanctioned API caller outside ``Http``.

The anthropic SDK is imported lazily so ``nlp.rules`` (free) keeps working without the ``llm`` extra.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, AsyncIterable, Callable

import app.config as app_config
from app.costs import UnknownReservation, estimate_batch, estimate_llm_usd
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.nlp import rubric as R
from app.nlp.aliases import OccupantsAt, build_alias_table
from app.nlp.events import enqueue_review, materialise_events, suggest_flag
from app.nlp.queue import (QueueItem, dedup, load_items, native_mention_id, pack_requests, render_items_block,
                           render_single, select_unlabelled)
from app.nlp.resolve import pinned_bulk_versions, policy_for
from app.nlp.schema import LabelV1, RequestRejected, json_schema_batch, json_schema_single, parse_batch_items, \
    parse_single
from app.util.time import floor_day, now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.costs import CostGovernor
    from app.ratelimit import ProviderLimiter
    from app.storage.db import Database

PROVIDER = "anthropic"
SUBMIT_JOB = "llm_classify_submit"
POLL_JOB = "llm_batch_poll"
FALLBACK_BETA = "server-side-fallback-2026-07-01"
ESCALATION_SUFFIX = ("\nThis item was pre-flagged as potentially material and negative. Assess independently and cite "
                     "the evidence in summary.")
BUILDING_GRACE_S = 900               # a 'building' batch older than this is an interrupted submit (power loss)
ESC_STATE_DAYS = 8
PARSE_DRIFT = 0.10                   # > 10 % of a batch's requests failing to parse → note classifier_drift


def make_client(s: Any = None) -> Any:
    """``AsyncAnthropic`` with SDK retries (429/5xx/connection, honouring retry-after)."""
    import anthropic
    s = s or app_config.settings
    return anthropic.AsyncAnthropic(api_key=s.ANTHROPIC_API_KEY, max_retries=3,
                                    timeout=float(s.LLM_REQUEST_TIMEOUT_S))


def system_blocks(rubric: str, alias_text: str) -> list[dict]:
    blocks: list[dict] = [{"type": "text", "text": rubric}]
    if alias_text:
        blocks.append({"type": "text", "text": alias_text, "cache_control": {"type": "ephemeral"}})
    return blocks


def bulk_params(model: str, system: list[dict], items_text: str) -> dict:
    """§8.3 bulk request params — never ``thinking``/``temperature``/``top_p``/``top_k`` or a prefill."""
    return {"model": model, "max_tokens": R.BULK_MAX_TOKENS, "system": system,
            "messages": [{"role": "user", "content": items_text}],
            "output_config": {"effort": R.BULK_EFFORT,
                              "format": {"type": "json_schema", "schema": json_schema_batch()}}}


def build_batch_request(custom_id: str, model: str, system_blocks: list[dict], items_text: str) -> Any:
    from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
    from anthropic.types.messages.batch_create_params import Request
    return Request(custom_id=custom_id,
                   params=MessageCreateParamsNonStreaming(**bulk_params(model, system_blocks, items_text)))


def message_text(message: Any) -> str:
    """Concatenated ``text`` blocks (adaptive thinking may put ``thinking`` blocks first)."""
    return "".join(getattr(b, "text", "") or "" for b in (getattr(message, "content", None) or [])
                   if getattr(b, "type", None) == "text")


def usage_of(message: Any) -> dict[str, int]:
    u = getattr(message, "usage", None)
    g = (lambda k: int(getattr(u, k, 0) or 0)) if u is not None else (lambda k: 0)
    return {"in": g("input_tokens"), "out": g("output_tokens"), "cache_read": g("cache_read_input_tokens"),
            "cache_write": g("cache_creation_input_tokens")}


def usage_usd(model: str, u: dict[str, int], *, batch: bool) -> float:
    return estimate_llm_usd(model, u["in"], u["out"], batch=batch, cache_read_tokens=u["cache_read"],
                            cache_write_tokens=u["cache_write"])


def error_kind(exc: BaseException) -> str:
    code = getattr(exc, "status_code", None)
    if code == 401 or type(exc).__name__ == "AuthenticationError":
        return "auth_failed"
    if code == 400 or type(exc).__name__ == "BadRequestError":
        return "invalid_request"
    if code == 429 or type(exc).__name__ == "RateLimitError":
        return "rate_limited"
    return "error"


def retry_after(exc: BaseException) -> float:
    try:
        h = exc.response.headers  # type: ignore[attr-defined]
        return float(h.get("retry-after") or 60.0)
    except Exception:
        return 60.0


def result_error_type(err: Any) -> str | None:
    """SDK 1.15: ``result.error`` is an ``ErrorResponse`` whose ``.error.type`` is the API error type."""
    inner = getattr(err, "error", None)
    if inner is not None and getattr(inner, "type", None):
        return inner.type
    t = getattr(err, "type", None)
    return None if t == "error" else t


# ── label writing (shared with rules:v2) ─────────────────────────────────────────────────────────────────────
@dataclass
class LabelEntry:
    item: QueueItem
    label: LabelV1
    via: str = "model"                       # model | regex
    model_resolved: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    cost_usd: float | None = None
    dedup_of: str | None = None


def link_entities(label: LabelV1, item: QueueItem, occ_at: OccupantsAt, via: str = "model"
                  ) -> tuple[list[dict], list[int]]:
    """Model/regex links mapped netuid → occupant at ``event_at`` (unmapped netuids returned), plus the source-bound
    occupant (``via='source'``, 1.0). One link per occupant, highest confidence wins."""
    links: dict[str, dict] = {}
    unmapped: list[int] = []
    for e in label.entities:
        occ = occ_at.at(e.netuid, item.event_at)
        if occ is None:
            unmapped.append(int(e.netuid))
            continue
        cur = links.get(occ)
        if cur is None or e.confidence > cur["confidence"]:
            links[occ] = {"netuid": int(e.netuid), "occupant_key": occ, "confidence": round(float(e.confidence), 4),
                          "via": via}
    if item.occupant_key:
        n = occ_at.netuid_of(item.occupant_key)
        if n is not None:
            links[item.occupant_key] = {"netuid": n, "occupant_key": item.occupant_key, "confidence": 1.0,
                                        "via": "source"}
    return sorted(links.values(), key=lambda x: (-x["confidence"], x["netuid"])), sorted(set(unmapped))


async def write_label_rows(db: "Database", entries: list[LabelEntry], *, version: str, tier: str, source: str,
                           run_id: str, batch_key: str | None, occ_at: OccupantsAt, now: int) -> int:
    """``INSERT OR IGNORE`` labels (+ entity rows ≥ ``LLM_ENTITY_MIN_CONFIDENCE`` and X/announcement entity mentions);
    an existing ``(post_id, classifier_version)`` is never touched. Returns rows inserted."""
    min_conf = float(app_config.settings.LLM_ENTITY_MIN_CONFIDENCE)
    n = 0
    async with db.transaction():
        for e in entries:
            it, lab = e.item, e.label
            links, unmapped = link_entities(lab, it, occ_at, e.via)
            raw = lab.raw_dict()
            if lab.clamped:
                raw["clamped"] = True
            if e.dedup_of:
                raw["dedup_of"] = e.dedup_of
            if unmapped:
                raw["unmapped"] = unmapped
            res = await db.execute(
                "INSERT OR IGNORE INTO post_labels (post_id, item_kind, item_id, classifier_version, classifier_tier, "
                "polarity, intensity, event_type, direction, materiality, entities_json, is_spam, is_promo, summary, "
                "confidence, event_at, observed_at, source, run_id, batch_key, model_resolved, input_tokens, "
                "output_tokens, cost_usd, raw_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (it.post_id, it.item_kind, it.item_id, version, tier, lab.polarity, int(lab.intensity), lab.event_type,
                 int(lab.direction), int(lab.materiality), json.dumps(links, separators=(",", ":")),
                 int(lab.is_spam), int(lab.is_promo), lab.summary, float(lab.confidence), int(it.event_at), int(now),
                 source, run_id, batch_key, e.model_resolved, e.input_tokens, e.output_tokens, e.cost_usd,
                 json.dumps(raw, separators=(",", ":"), ensure_ascii=False, default=str)))
            if res.rowcount <= 0:
                continue
            n += 1
            for ln in links:
                if ln["confidence"] < min_conf:
                    continue
                await db.execute(
                    "INSERT OR IGNORE INTO post_label_entities (post_id, classifier_version, occupant_key, netuid, "
                    "confidence, via, observed_at) VALUES (?,?,?,?,?,?,?)",
                    (it.post_id, version, ln["occupant_key"], ln["netuid"], ln["confidence"], ln["via"], int(now)))
                nm = native_mention_id(it.post_id)
                if nm and ln["via"] in ("model", "regex"):
                    await db.execute(
                        "INSERT OR IGNORE INTO social_post_mentions (platform, post_id, occupant_key, match_kind, "
                        "classifier_version, event_at, observed_at, run_id) VALUES (?,?,?,'entity',?,?,?,?)",
                        (nm[0], nm[1], ln["occupant_key"], version, int(it.event_at), int(now), run_id))
    return n


# ── the classifier ───────────────────────────────────────────────────────────────────────────────────────────
class LlmClassifier:
    def __init__(self, db: "Database", client: Any, governor: "CostGovernor", limiter: "ProviderLimiter | None",
                 settings_getter: Callable[[], Any] | None = None, *, clock: Callable[[], int] = now_ts) -> None:
        self.db = db
        self.client = client
        self.governor = governor
        self.limiter = limiter
        self._s = settings_getter or (lambda: app_config.settings)
        self.clock = clock

    # helpers ------------------------------------------------------------------------------------------------
    @property
    def s(self) -> Any:
        return self._s()

    def versions(self) -> dict[str, str]:
        s = self.s
        text = R.load_rubric(s.LLM_RUBRIC_PATH)
        sha = R.rubric_sha256(text)
        return {"rubric": text, "sha": sha, "bulk": R.classifier_version(s.LLM_BULK_MODEL, sha, s.LLM_SCHEMA_V),
                "escalation": R.classifier_version(s.LLM_ESCALATION_MODEL, sha, s.LLM_SCHEMA_V)}

    def _need_client(self) -> Any:
        if self.client is None:
            raise RuntimeError("auth_failed: ANTHROPIC_API_KEY is empty")
        return self.client

    async def _sdk(self, job: str, run_id: str, what: str, fn: Callable[[], Any], *, book_request: bool = True) -> Any:
        """limiter → request booking → call; a 429 that survived the SDK's retries penalises the bucket."""
        self._need_client()
        if self.limiter is not None:
            await self.limiter.acquire(PROVIDER)
        if book_request:
            await self.governor.record(PROVIDER, 0.0, requests=1, job=job, run_id=run_id)
        try:
            return await fn()
        except Exception as exc:
            if error_kind(exc) == "rate_limited" and self.limiter is not None:
                self.limiter.penalize(PROVIDER, retry_after(exc))
            logger.warning(f"anthropic {what} failed", data={"error": f"{type(exc).__name__}: {exc}"[:300]})
            raise

    async def _state_get(self, job: str, key: str) -> Any:
        v = await self.db.fetchval("SELECT value_json FROM job_state WHERE job = ? AND key = ?", (job, key),
                                   writer=True)
        return json.loads(v) if v else None

    async def _state_put(self, job: str, key: str, value: Any) -> None:
        await self.db.upsert("job_state", [{"job": job, "key": key, "value_json": json.dumps(value, default=str),
                                            "updated_at": self.clock()}], conflict=("job", "key"))

    async def _state_del(self, job: str, key: str) -> None:
        await self.db.execute("DELETE FROM job_state WHERE job = ? AND key = ?", (job, key))

    async def _system_tokens(self, model: str, sha: str, alias_sha: str, rubric: str, alias_text: str) -> int:
        st = await self._state_get(SUBMIT_JOB, "system_tokens")
        if isinstance(st, dict) and st.get("key") == f"{model}|{sha}|{alias_sha}":
            return int(st["tokens"])
        return R.token_estimate(rubric + "\n" + alias_text)

    async def _measure_system_tokens(self, run_id: str, model: str, sha: str, alias_sha: str,
                                     system: list[dict]) -> None:
        """``count_tokens`` once per rubric/alias change (model-specific counts, §6.3); best effort."""
        key = f"{model}|{sha}|{alias_sha}"
        st = await self._state_get(SUBMIT_JOB, "system_tokens")
        if isinstance(st, dict) and st.get("key") == key:
            return
        try:
            r = await self._sdk(SUBMIT_JOB, run_id, "count_tokens", lambda: self.client.messages.count_tokens(
                model=model, system=system, messages=[{"role": "user", "content": "<items n=\"0\">\n</items>"}]))
            await self._state_put(SUBMIT_JOB, "system_tokens", {"key": key, "tokens": int(r.input_tokens)})
        except Exception:
            pass

    # submit -------------------------------------------------------------------------------------------------
    async def recover_building(self) -> int:
        """Power loss mid-submit: stale ``building`` batches → failed, reservation released, items requeued."""
        now = self.clock()
        rows = await self.db.fetchall("SELECT batch_key FROM llm_batches WHERE status = 'building' AND created_at < ?",
                                      (now - BUILDING_GRACE_S,), writer=True)
        for r in rows:
            await self._release(r["batch_key"])
            await self.db.execute("UPDATE llm_batches SET status = 'failed', error = 'building_interrupted' "
                                  "WHERE batch_key = ?", (r["batch_key"],))
            await self.db.execute("UPDATE llm_batch_items SET status = 'requeued' WHERE batch_key = ? AND "
                                  "status = 'pending'", (r["batch_key"],))
        return len(rows)

    async def _release(self, batch_key: str) -> None:
        st = await self._state_get(SUBMIT_JOB, f"resv:{batch_key}")
        if st and st.get("reservation_id"):
            await self.governor.release(st["reservation_id"])
        await self._state_del(SUBMIT_JOB, f"resv:{batch_key}")

    async def submit_cycle(self, run_id: str) -> dict:
        s = self.s
        now = self.clock()
        v = self.versions()
        if v["bulk"] not in pinned_bulk_versions():
            logger.warning("llm submit skipped: bulk version not pinned", data={"version": v["bulk"]})
            return {"skipped": "version_unpinned", "classifier_version": v["bulk"]}
        await self.recover_building()
        alias_text, alias_sha = await build_alias_table(self.db)
        if not alias_text:
            raise RuntimeError("alias_table_empty")
        per = int(s.LLM_BATCH_ITEMS_PER_REQUEST)
        items = await select_unlabelled(self.db, bulk_version=v["bulk"], now=now,
                                        lookback_days=int(s.LLM_LOOKBACK_DAYS),
                                        limit=int(s.LLM_BATCH_MAX_REQUESTS) * per)
        if not items:
            return {"skipped": "empty_queue"}
        prim, dups = dedup(items)
        reqs = pack_requests(prim, per)[:int(s.LLM_BATCH_MAX_REQUESTS)]
        sent = {q.post_id for r in reqs for q in r}
        model = s.LLM_BULK_MODEL
        system = system_blocks(v["rubric"], alias_text)
        sys_tok = await self._system_tokens(model, v["sha"], alias_sha, v["rubric"], alias_text)
        texts = [q.text or "" for r in reqs for q in r]
        est = estimate_batch(texts, model=model, system_tokens=sys_tok, n_req=len(reqs))
        res = await self.governor.reserve(PROVIDER, est, job=SUBMIT_JOB, run_id=run_id)
        if res is None:
            await self._state_put(SUBMIT_JOB, "budget_stop", {"at": now})
            return {"skipped": "budget", "est_cost_usd": round(est, 6)}
        batch_key = uuid.uuid4().hex
        tpc = float(s.LLM_TOKENS_PER_CHAR)
        est_in = sum(int(len(t) * tpc) + 40 for t in texts) + sys_tok * len(reqs)
        est_out = 95 * len(texts) + 30 * len(reqs)
        n_items = len(sent) + sum(len(d) for p, d in dups.items() if p in sent)
        bitems: list[dict] = []
        api_reqs = []
        for i, r in enumerate(reqs):
            cid = f"{batch_key[:8]}-{i:04d}"
            for q in r:
                bitems.append({"batch_key": batch_key, "custom_id": cid, "post_id": q.post_id, "attempt": q.attempt,
                               "status": "pending"})
                for d in dups.get(q.post_id, []):
                    bitems.append({"batch_key": batch_key, "custom_id": f"dup:{q.post_id}", "post_id": d.post_id,
                                   "attempt": q.attempt, "status": "pending"})
            api_reqs.append(build_batch_request(cid, model, system, render_items_block(r)))
        async with self.db.transaction():
            await self.db.execute(
                "INSERT INTO llm_batches (batch_key, model, classifier_version, alias_sha256, status, n_requests, "
                "n_items, est_input_tokens, est_output_tokens, est_cost_usd, created_at) "
                "VALUES (?,?,?,?,'building',?,?,?,?,?,?)",
                (batch_key, model, v["bulk"], alias_sha, len(reqs), n_items, est_in, est_out, est, now))
            await self.db.insert_or_ignore("llm_batch_items", bitems)
            await self._state_put(SUBMIT_JOB, f"resv:{batch_key}", {"reservation_id": res.id, "est_usd": est})
        try:
            batch = await self._sdk(SUBMIT_JOB, run_id, "batches.create",
                                    lambda: self.client.messages.batches.create(requests=api_reqs))
        except Exception as exc:
            kind = error_kind(exc)
            await self._release(batch_key)
            await self.db.execute("UPDATE llm_batches SET status = 'failed', error = ? WHERE batch_key = ?",
                                  (f"{kind}: {type(exc).__name__}: {exc}"[:500], batch_key))
            await self.db.execute("UPDATE llm_batch_items SET status = ? WHERE batch_key = ? AND status = 'pending'",
                                  ("errored" if kind == "invalid_request" else "requeued", batch_key))
            if kind == "auth_failed":
                raise RuntimeError(f"auth_failed: {exc}") from exc
            raise
        await self.db.execute("UPDATE llm_batches SET provider_batch_id = ?, status = 'submitted', submitted_at = ? "
                              "WHERE batch_key = ?", (batch.id, self.clock(), batch_key))
        await self._state_del(SUBMIT_JOB, "budget_stop")
        await self._measure_system_tokens(run_id, model, v["sha"], alias_sha, system)
        logger.info("llm batch submitted", data={"batch_key": batch_key, "provider_batch_id": batch.id,
                                                 "n_requests": len(reqs), "n_items": n_items, "est_cost_usd": est})
        return {"batch_key": batch_key, "provider_batch_id": batch.id, "n_requests": len(reqs), "n_items": n_items,
                "est_cost_usd": round(est, 6)}

    # poll ---------------------------------------------------------------------------------------------------
    async def poll_cycle(self, run_id: str) -> dict:
        await self.recover_building()
        out: dict[str, Any] = {"batches": [], "cost_usd": 0.0}
        rows = await self.db.fetchall("SELECT * FROM llm_batches WHERE status IN ('submitted','ended') "
                                      "ORDER BY created_at", writer=True)
        to_commit: list[dict] = []
        for b in rows:
            r = await self._poll_one(b, run_id)
            out["batches"].append(r)
            if r.get("applied"):
                to_commit.append(r)
        out["escalation"] = await self.escalate(run_id)
        out["events"] = await materialise_events(self.db, policy_for(), run_id, since_hours=72, now=self.clock())
        for r in to_commit:
            await self._commit(r, run_id)
            out["cost_usd"] += r["actual_usd"]
        out["cost_usd"] += out["escalation"].get("cost_usd", 0.0)
        return out

    async def _poll_one(self, b: dict, run_id: str) -> dict:
        key, pid = b["batch_key"], b["provider_batch_id"]
        now = self.clock()
        if b["status"] == "submitted":
            mb = await self._sdk(POLL_JOB, run_id, "batches.retrieve",
                                 lambda: self.client.messages.batches.retrieve(pid))
            await self.db.execute("UPDATE llm_batches SET last_polled_at = ? WHERE batch_key = ?", (now, key))
            if getattr(mb, "processing_status", None) != "ended":
                stale = now - int(b["submitted_at"] or b["created_at"]) > int(self.s.LLM_BATCH_STALE_H) * 3600
                if stale and b["error"] != "cancel_requested":
                    await self._sdk(POLL_JOB, run_id, "batches.cancel",
                                    lambda: self.client.messages.batches.cancel(pid))
                    await self.db.execute("UPDATE llm_batches SET error = 'cancel_requested' WHERE batch_key = ?",
                                          (key,))
                    return {"batch_key": key, "status": "cancel_requested"}
                return {"batch_key": key, "status": getattr(mb, "processing_status", "in_progress")}
            rc = getattr(mb, "request_counts", None)
            g = (lambda k: int(getattr(rc, k, 0) or 0)) if rc is not None else (lambda k: None)
            await self.db.execute("UPDATE llm_batches SET status = 'ended', ended_at = ?, n_succeeded = ?, "
                                  "n_errored = ?, n_expired = ? WHERE batch_key = ?",
                                  (now, g("succeeded"), g("errored"), g("expired"), key))
        results = await self._sdk(POLL_JOB, run_id, "batches.results",
                                  lambda: self.client.messages.batches.results(pid))
        applied = await self.apply_batch(key, results, run_id=run_id)
        applied.update(batch_key=key, status="ended", applied=True)
        return applied

    async def _commit(self, r: dict, run_id: str) -> None:
        key = r["batch_key"]
        b = await self.db.fetchone("SELECT model, error FROM llm_batches WHERE batch_key = ?", (key,), writer=True)
        actual = float(r["actual_usd"])
        detail = {"model": b["model"] if b else None, "batch_key": key}
        st = await self._state_get(SUBMIT_JOB, f"resv:{key}")
        booked = False
        if st and st.get("reservation_id"):
            try:
                await self.governor.commit(st["reservation_id"], actual, detail=detail)
                booked = True
            except UnknownReservation:
                pass
        if not booked:
            await self.governor.record(PROVIDER, actual, requests=0, job=POLL_JOB, run_id=run_id, detail=detail)
        await self._state_del(SUBMIT_JOB, f"resv:{key}")
        await self.governor.record(PROVIDER, 0.0, requests=0, units=r["units"], job=POLL_JOB, run_id=run_id)
        final = "expired" if b and b["error"] == "cancel_requested" else "applied"
        await self.db.execute("UPDATE llm_batches SET status = ?, applied_at = ? WHERE batch_key = ?",
                              (final, self.clock(), key))
        logger.info("llm batch applied", data={"batch_key": key, "status": final, "actual_usd": actual,
                                               "n_labelled": r.get("n_labelled"), "n_succeeded": r.get("n_succeeded")})

    def parse_request_result(self, custom_id: str, message: Any, post_ids: list[str]
                             ) -> tuple[list[tuple[str, LabelV1]], list[str]]:
        """Prompt id ``k`` ↔ ``post_ids[k-1]`` (sorted). Returns (parsed, unmatched post_ids)."""
        if getattr(message, "stop_reason", None) == "refusal":
            return [], list(post_ids)
        try:
            ok, _bad = parse_batch_items(message_text(message))
        except RequestRejected as exc:
            logger.info("llm request parse failure", data={"custom_id": custom_id, "error": str(exc),
                                                           "stop_reason": getattr(message, "stop_reason", None)})
            return [], list(post_ids)
        parsed: dict[str, LabelV1] = {}
        for _raw, lab in ok:
            sid = lab.id.strip()
            if not sid.isdigit():
                continue
            k = int(sid)
            if 1 <= k <= len(post_ids) and post_ids[k - 1] not in parsed:
                parsed[post_ids[k - 1]] = lab
        return [(p, parsed[p]) for p in post_ids if p in parsed], [p for p in post_ids if p not in parsed]

    async def apply_batch(self, batch_key: str, results: AsyncIterable, *, run_id: str = "apply") -> dict:
        """Write labels for every succeeded request (idempotent); set per-item statuses; tally usage and USD."""
        b = await self.db.fetchone("SELECT * FROM llm_batches WHERE batch_key = ?", (batch_key,), writer=True)
        if b is None:
            raise KeyError(batch_key)
        rows = await self.db.fetchall("SELECT custom_id, post_id FROM llm_batch_items WHERE batch_key = ?",
                                      (batch_key,), writer=True)
        by_cid: dict[str, list[str]] = {}
        dups: dict[str, list[str]] = {}
        for r in rows:
            if r["custom_id"].startswith("dup:"):
                dups.setdefault(r["custom_id"][4:], []).append(r["post_id"])
            else:
                by_cid.setdefault(r["custom_id"], []).append(r["post_id"])
        for v in by_cid.values():
            v.sort()
        items = await load_items(self.db, [r["post_id"] for r in rows])
        occ_at = await OccupantsAt.load(self.db)
        model, version = b["model"], b["classifier_version"]
        status: dict[str, str] = {}
        tot = {"in": 0, "out": 0, "cache_read": 0, "cache_write": 0}
        n = {"succeeded": 0, "errored": 0, "expired": 0, "parse_errors": 0, "labelled": 0}
        actual = 0.0
        seen: set[str] = set()
        async for res in results:
            cid = getattr(res, "custom_id", None)
            pids = by_cid.get(cid or "")
            if not pids or cid in seen:
                continue
            seen.add(cid)
            r = res.result
            rtype = getattr(r, "type", None)
            if rtype == "succeeded":
                n["succeeded"] += 1
                msg = r.message
                u = usage_of(msg)
                for k in tot:
                    tot[k] += u[k]
                usd = usage_usd(model, u, batch=True)
                actual += usd
                parsed, unmatched = self.parse_request_result(cid, msg, pids)
                if unmatched:
                    n["parse_errors"] += 1
                share = max(1, len(parsed))
                entries = []
                for p, lab in parsed:
                    it = items.get(p)
                    if it is None:
                        unmatched.append(p)
                        continue
                    kw = dict(model_resolved=getattr(msg, "model", None) or model, input_tokens=u["in"] // share,
                              output_tokens=u["out"] // share, cost_usd=round(usd / share, 8))
                    entries.append(LabelEntry(it, lab, **kw))
                    status[p] = "labelled"
                    for d in dups.get(p, []):
                        if d in items:
                            entries.append(LabelEntry(items[d], lab, dedup_of=p, **kw))
                            status[d] = "labelled"
                n["labelled"] += await write_label_rows(self.db, entries, version=version, tier="bulk", source="llm",
                                                        run_id=run_id, batch_key=batch_key, occ_at=occ_at,
                                                        now=self.clock())
                for p in unmatched:
                    status[p] = "errored"                 # parse failure → once more at 10/request, then rules
            elif rtype == "errored":
                n["errored"] += 1
                et = result_error_type(getattr(r, "error", None))
                for p in pids:
                    status[p] = "errored" if et == "invalid_request" else "requeued"
            else:                                         # expired | canceled
                n["expired"] += 1
                for p in pids:
                    status[p] = "requeued"
        for cid, pids in by_cid.items():                 # requests with no result line at all
            for p in pids:
                status.setdefault(p, "requeued")
        for prim, ds in dups.items():
            for d in ds:
                status.setdefault(d, status.get(prim, "requeued"))
        async with self.db.transaction():
            for st in ("labelled", "errored", "requeued"):
                ids = [p for p, x in status.items() if x == st]
                for i in range(0, len(ids), 400):
                    part = ids[i:i + 400]
                    await self.db.execute(
                        f"UPDATE llm_batch_items SET status = ? WHERE batch_key = ? AND post_id IN "
                        f"({','.join('?' * len(part))})", (st, batch_key, *part))
            await self.db.execute(
                "UPDATE llm_batches SET n_succeeded = ?, n_errored = ?, n_expired = ?, n_parse_errors = ?, "
                "input_tokens = ?, output_tokens = ?, cache_read_tokens = ?, cost_usd = ? WHERE batch_key = ?",
                (n["succeeded"], n["errored"], n["expired"], n["parse_errors"], tot["in"], tot["out"],
                 tot["cache_read"], round(actual, 8), batch_key))
        n_req = max(1, int(b["n_requests"] or len(by_cid) or 1))
        drift = n["parse_errors"] / n_req > PARSE_DRIFT
        if drift:
            logger.warning("classifier_drift", data={"batch_key": batch_key, "n_parse_errors": n["parse_errors"],
                                                     "n_requests": n_req})
        return {"n_succeeded": n["succeeded"], "n_errored": n["errored"], "n_expired": n["expired"],
                "n_parse_errors": n["parse_errors"], "n_labelled": n["labelled"], "actual_usd": round(actual, 8),
                "units": tot, "classifier_drift": drift}

    async def write_labels(self, labels: list[LabelV1], items_by_id: dict[str, QueueItem], *, version: str, tier: str,
                           batch_key: str | None, usage: Any, run_id: str) -> int:
        """§8.2 convenience wrapper: ``labels[i].id`` is the item's ``post_id``."""
        occ_at = await OccupantsAt.load(self.db)
        entries = [LabelEntry(items_by_id[x.id], x, model_resolved=getattr(usage, "model", None))
                   for x in labels if x.id in items_by_id]
        return await write_label_rows(self.db, entries, version=version, tier=tier,
                                      source="rules" if tier == "rules" else "llm", run_id=run_id,
                                      batch_key=batch_key, occ_at=occ_at, now=self.clock())

    async def link_entities(self, label: LabelV1, item: QueueItem) -> list[dict]:
        return link_entities(label, item, await OccupantsAt.load(self.db))[0]

    # escalation ---------------------------------------------------------------------------------------------
    async def call_direct(self, *, job: str, run_id: str, model: str, system: Any, user: str, max_tokens: int,
                          schema: dict, effort: str, timeout: float | None = None) -> Any:
        """One unreserved Sonnet/Opus call (request booked with the usd by the caller, §6.3)."""
        kw: dict[str, Any] = {"model": model, "max_tokens": max_tokens, "system": system,
                              "messages": [{"role": "user", "content": user}],
                              "output_config": {"effort": effort, "format": {"type": "json_schema", "schema": schema}}}
        if timeout:
            kw["timeout"] = timeout
        if self.s.LLM_REFUSAL_FALLBACK:
            fn = lambda: self.client.beta.messages.create(**kw, betas=[FALLBACK_BETA], fallbacks="default")  # noqa
        else:
            fn = lambda: self.client.messages.create(**kw)  # noqa: E731
        return await self._sdk(job, run_id, f"messages.create {model}", fn, book_request=False)

    async def _snapshot_check(self, job: str, alias: str, resolved: str | None) -> None:
        if not resolved:
            return
        key = f"last_model:{alias}"
        last = await self._state_get(job, key)
        if last != resolved:
            if last is not None:
                logger.warning("classifier_snapshot_changed", data={"alias": alias, "was": last, "now": resolved})
            await self._state_put(job, key, resolved)

    async def escalate(self, run_id: str) -> dict:
        """Single-item Sonnet re-assessment of escalation candidates (§2.2); dead ends go to the review queue."""
        s = self.s
        now = self.clock()
        out: dict[str, Any] = {"candidates": 0, "escalated": 0, "denied": 0, "failed": 0, "reviews": 0,
                               "cost_usd": 0.0}
        policy = policy_for()
        v = self.versions()
        esc_v = v["escalation"]
        if not policy.bulk:
            return out
        ph = ",".join("?" * len(policy.bulk))
        cands = await self.db.fetchall(
            f"SELECT * FROM post_labels l WHERE l.classifier_tier = 'bulk' AND l.classifier_version IN ({ph}) "
            "AND l.observed_at >= ? AND ((l.direction = -1 AND l.materiality = 3) OR l.event_type IN "
            "('owner_extraction_evidence','security_incident')) AND NOT EXISTS (SELECT 1 FROM post_labels e WHERE "
            "e.post_id = l.post_id AND e.classifier_version = ?) AND NOT EXISTS (SELECT 1 FROM review_queue q WHERE "
            "q.kind = 'llm_flag_candidate' AND q.post_id = l.post_id) ORDER BY l.observed_at, l.post_id",
            [*policy.bulk, now - ESC_STATE_DAYS * 86400, esc_v], writer=True)
        esc_states = {r["key"][4:]: json.loads(r["value_json"]) for r in await self.db.fetchall(
            "SELECT key, value_json FROM job_state WHERE job = ? AND key LIKE 'esc:%'", (POLL_JOB,), writer=True)}
        cands = [c for c in cands if not (esc_states.get(c["post_id"]) or {}).get("done")]
        out["candidates"] = len(cands)
        if not cands:
            return out
        enabled = bool(s.LLM_ESCALATION_ENABLED) and esc_v in policy.escalation
        items = await load_items(self.db, [c["post_id"] for c in cands])
        day = floor_day(now)
        calls = int(await self._state_get(POLL_JOB, f"esc_calls:{day}") or 0)
        cap = int(s.LLM_ESCALATION_MAX_PER_DAY)
        occ_at: OccupantsAt | None = None
        system = None
        denied = False
        for c in cands:
            pid = c["post_id"]
            st = esc_states.get(pid) or {"tries": 0}
            it = items.get(pid)
            reason = None
            if not enabled:
                reason = "escalation_disabled"
            elif it is None or it.text is None:
                reason = "text_purged"
            elif now - int(c["observed_at"]) > int(s.LLM_FALLBACK_AFTER_H) * 3600:
                reason = "escalation_timeout"
            elif int(st.get("tries", 0)) >= 3:
                reason = "escalation_failed"
            elif calls >= cap:
                reason = "daily_cap"
            if reason is None and (denied or self.client is None):
                continue                               # stays pending (no try consumed) until timeout → review
            if reason:
                out["reviews"] += await self._dead_end(c, it, reason)
                await self._state_put(POLL_JOB, f"esc:{pid}", {**st, "done": True, "reason": reason, "at": now})
                continue
            assert it is not None
            if system is None:
                alias_text, _ = await build_alias_table(self.db)
                system = system_blocks(v["rubric"], alias_text)
            user = render_single(it) + ESCALATION_SUFFIX
            est_in = R.token_estimate(v["rubric"]) + 2600 + int(len(user) * float(s.LLM_TOKENS_PER_CHAR))
            est = estimate_llm_usd(s.LLM_ESCALATION_MODEL, est_in, 1000)
            d = await self.governor.check(PROVIDER, est)
            if not d.allowed:                          # kill_switch / hard_cap / zero_cap: no try consumed
                out["denied"] += 1
                denied = True
                continue
            calls += 1
            await self._state_put(POLL_JOB, f"esc_calls:{day}", calls)
            try:
                msg = await self.call_direct(job=POLL_JOB, run_id=run_id, model=s.LLM_ESCALATION_MODEL, system=system,
                                             user=user, max_tokens=R.ESCALATION_MAX_TOKENS,
                                             schema=json_schema_single(), effort=R.ESCALATION_EFFORT,
                                             timeout=float(s.LLM_REQUEST_TIMEOUT_S))
            except Exception as exc:
                await self.governor.record(PROVIDER, 0.0, requests=1, job=POLL_JOB, run_id=run_id)
                st = {"tries": int(st.get("tries", 0)) + 1, "error": f"{type(exc).__name__}"[:80], "at": now}
                await self._state_put(POLL_JOB, f"esc:{pid}", st)
                out["failed"] += 1
                if error_kind(exc) == "auth_failed":
                    raise RuntimeError(f"auth_failed: {exc}") from exc
                continue
            u = usage_of(msg)
            usd = usage_usd(s.LLM_ESCALATION_MODEL, u, batch=False)
            out["cost_usd"] += usd
            resolved = getattr(msg, "model", None) or s.LLM_ESCALATION_MODEL
            await self.governor.record(PROVIDER, usd, requests=1, units=u, job=POLL_JOB, run_id=run_id,
                                       detail={"model": resolved, "post_id": pid})
            await self._snapshot_check(POLL_JOB, s.LLM_ESCALATION_MODEL, resolved)
            if getattr(msg, "stop_reason", None) == "refusal":
                cat = getattr(getattr(msg, "stop_details", None), "category", None)
                logger.info("escalation refused", data={"post_id": pid, "category": cat})
                out["reviews"] += await self._dead_end(c, it, "refused")
                await self._state_put(POLL_JOB, f"esc:{pid}", {**st, "done": True, "reason": "refused", "at": now})
                continue
            try:
                lab = parse_single(message_text(msg))
            except RequestRejected:
                st = {"tries": int(st.get("tries", 0)) + 1, "error": "parse_failure", "at": now}
                await self._state_put(POLL_JOB, f"esc:{pid}", st)
                out["failed"] += 1
                continue
            if occ_at is None:
                occ_at = await OccupantsAt.load(self.db)
            await write_label_rows(self.db, [LabelEntry(it, lab, model_resolved=resolved, input_tokens=u["in"],
                                                        output_tokens=u["out"], cost_usd=round(usd, 8))],
                                   version=esc_v, tier="escalation", source="llm", run_id=run_id, batch_key=None,
                                   occ_at=occ_at, now=self.clock())
            await self._state_del(POLL_JOB, f"esc:{pid}")
            out["escalated"] += 1
        await self.db.execute("DELETE FROM job_state WHERE job = ? AND (key LIKE 'esc:%' OR key LIKE 'esc_calls:%') "
                              "AND updated_at < ?", (POLL_JOB, now - ESC_STATE_DAYS * 86400))
        return out

    async def _dead_end(self, c: dict, it: QueueItem | None, reason: str) -> int:
        """An unescalatable candidate → review row (§2.2; never counted as confirmed). The flag follows §3.2; with
        escalation disabled only §3.2's own rules enqueue (as ``materialise_events`` does)."""
        sug = suggest_flag(c["event_type"], int(c["direction"]), int(c["materiality"]),
                           f"{c['summary'] or ''}\n{(it.text if it else '') or ''}")
        if sug is None:
            if reason == "escalation_disabled":
                return 0
            sug = (None, 2)
        occ = await self.db.fetchval("SELECT occupant_key FROM post_label_entities WHERE post_id = ? AND "
                                     "classifier_version = ? ORDER BY confidence DESC LIMIT 1",
                                     (c["post_id"], c["classifier_version"]), writer=True)
        rid = await enqueue_review(self.db, occupant_key=occ, post_id=c["post_id"],
                                   label_version=c["classifier_version"], event_type=c["event_type"],
                                   materiality=int(c["materiality"]), summary=c["summary"],
                                   evidence_url=it.url if it else None, suggested_flag=sug[0], reason=reason,
                                   priority=sug[1], direction=int(c["direction"]), confidence=float(c["confidence"]),
                                   now=self.clock())
        return 1 if rid else 0


# ── /health extras (§7) ──────────────────────────────────────────────────────────────────────────────────────
async def health_snapshot(db: "Database", now: int | None = None) -> dict:
    s = app_config.settings
    now = int(now if now is not None else now_ts())
    v = R.current_versions()
    queue = await select_unlabelled(db, bulk_version=v["bulk"], now=now, lookback_days=int(s.LLM_LOOKBACK_DAYS),
                                    limit=10 ** 7)
    oldest = max((now - q.observed_at for q in queue), default=None)
    day = now - 86400
    open_b = await db.fetchval("SELECT COUNT(*) FROM llm_batches WHERE status IN ('building','submitted','ended')")
    lab24 = await db.fetchval("SELECT COUNT(*) FROM post_labels WHERE classifier_tier = 'bulk' AND observed_at >= ?",
                              (day,))
    esc24 = await db.fetchval("SELECT COUNT(*) FROM post_labels WHERE classifier_tier = 'escalation' AND "
                              "observed_at >= ?", (day,))
    pending = await db.fetchval(
        "SELECT COUNT(*) FROM post_labels l WHERE l.classifier_tier = 'bulk' AND l.observed_at >= ? AND "
        "((l.direction = -1 AND l.materiality = 3) OR l.event_type IN ('owner_extraction_evidence',"
        "'security_incident')) AND NOT EXISTS (SELECT 1 FROM post_labels e WHERE e.post_id = l.post_id AND "
        "e.classifier_tier = 'escalation') AND NOT EXISTS (SELECT 1 FROM review_queue q WHERE "
        "q.kind = 'llm_flag_candidate' AND q.post_id = l.post_id)", (now - ESC_STATE_DAYS * 86400,))
    pe = await db.fetchone("SELECT COALESCE(SUM(n_parse_errors), 0) AS e, COALESCE(SUM(n_requests), 0) AS r FROM "
                           "llm_batches WHERE ended_at >= ?", (day,))
    review_open = await db.fetchval("SELECT COUNT(*) FROM review_queue WHERE kind = 'llm_flag_candidate' AND "
                                    "status = 'open'")
    from app.nlp.rules import fallback_mode
    return {"queue_depth": len(queue), "open_batches": int(open_b or 0),
            "oldest_unlabelled_h": round(oldest / 3600, 2) if oldest is not None else None,
            "labelled_24h": int(lab24 or 0), "escalations_24h": int(esc24 or 0),
            "escalation_pending": int(pending or 0),
            "parse_error_rate_24h": round(pe["e"] / pe["r"], 4) if pe and pe["r"] else 0.0,
            "fallback_active": (await fallback_mode(db, None, now)) is not None, "review_open": int(review_open or 0),
            "classifier_version_bulk": v["bulk"], "classifier_version_escalation": v["escalation"],
            "rubric_sha256": v["rubric_sha256"]}


# ── jobs ─────────────────────────────────────────────────────────────────────────────────────────────────────
def _client_or_none(s: Any) -> Any:
    return make_client(s) if s.ANTHROPIC_API_KEY else None


class LlmClassifySubmitJob(CollectorJob):
    """llm_classify_submit (paid): one Batch API submission per 6-h cycle."""

    async def collect(self, run: RunContext) -> JobOutcome:
        s = run.settings
        if not s.ANTHROPIC_API_KEY:
            raise RuntimeError("auth_failed: ANTHROPIC_API_KEY is empty")
        clf = LlmClassifier(run.rt.db, make_client(s), run.rt.costs, run.rt.limiter)
        res = await clf.submit_cycle(run.run_id)
        if "skipped" in res:
            return JobOutcome(rows=0, note=f"skipped: {res['skipped']}", meta=res)
        return JobOutcome(rows=int(res["n_items"]), note=f"submitted {res['provider_batch_id']} "
                                                         f"n_items={res['n_items']}", meta=res)


class LlmBatchPollJob(CollectorJob):
    """llm_batch_poll (free): download → apply → escalate → events → commit; caches the /health extras."""
    _health: dict | None = None

    async def collect(self, run: RunContext) -> JobOutcome:
        clf = LlmClassifier(run.rt.db, _client_or_none(run.settings), run.rt.costs, run.rt.limiter)
        res = await clf.poll_cycle(run.run_id)
        try:
            self._health = await health_snapshot(run.rt.db)
        except Exception as exc:  # pragma: no cover
            logger.warning("llm health snapshot failed", data={"error": str(exc)})
        rows = sum(int(b.get("n_labelled") or 0) for b in res["batches"]) + int(res["escalation"].get("escalated", 0))
        drift = any(b.get("classifier_drift") for b in res["batches"])
        note = "classifier_drift" if drift else (f"applied={sum(1 for b in res['batches'] if b.get('applied'))} "
                                                 f"escalated={res['escalation'].get('escalated', 0)}")
        return JobOutcome(rows=rows, cost_usd=round(res["cost_usd"], 8), note=note, meta={
            "batches": res["batches"], "escalation": res["escalation"], "events": res["events"]})

    def health_extra(self) -> dict | None:
        return self._health


__all__ = ["LlmClassifier", "LlmClassifySubmitJob", "LlmBatchPollJob", "LabelEntry", "write_label_rows",
           "link_entities", "build_batch_request", "bulk_params", "system_blocks", "make_client", "health_snapshot",
           "message_text", "usage_of", "usage_usd"]
