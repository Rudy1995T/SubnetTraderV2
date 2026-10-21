"""Weekly Opus narrative (spec 10 §8.5; display-only, never a scoring input) and ``LlmNarrativeWeeklyJob``.

Subjects: latest official scores with tier A/B ∪ top-10 movers by |Δ composite_smoothed 7 d| ∪ occupants with a new red
flag this week, capped at ``LLM_NARRATIVE_MAX_SUBNETS``. The evidence bundle (≤ ``LLM_NARRATIVE_EVIDENCE_TOKENS``,
estimated from characters) is truncated from the bottom; it carries label summaries only, never item text. Each call is
an unreserved governor ``check`` → call → ``record(…, requests=1)``; a denial skips the remaining subjects; a refusal or
parse failure skips the subject for the week.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.costs import estimate_llm_usd
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.nlp import rubric as R
from app.nlp.schema import RequestRejected, json_schema_narrative, parse_narrative
from app.util.time import now_ts, to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

DAY = 86400
JOB = "llm_narrative_weekly"
MOVERS = 10
P1_KEYS = ("emission_share", "gate_margin", "dereg_safety", "net_flow_30d")
P4_KEYS = ("owner_commitment", "owner_retention_30d")
P2_KEYS = ("active_authors_28d", "commit_days_90d", "org_freshness")
EST_OUT_TOKENS = 2000


def _s() -> Any:
    return app_config.settings


def _date(ts: int | None) -> str:
    return datetime.fromtimestamp(int(ts), timezone.utc).strftime("%Y-%m-%d") if ts else "?"


async def _latest_official(db: "Database") -> int | None:
    return await db.fetchval("SELECT MAX(as_of) FROM scores WHERE spec_version = ? AND run_kind = 'official' "
                             "AND shadow = 0", (_s().SPEC_VERSION,))


async def select_subjects(db: "Database", *, max_subnets: int) -> list[str]:
    as_of = await _latest_official(db)
    if as_of is None:
        return []
    sv = _s().SPEC_VERSION
    cur = {r["occupant_key"]: r for r in await db.fetchall(
        "SELECT occupant_key, tier, composite, composite_smoothed, red_flags FROM scores WHERE spec_version = ? "
        "AND as_of = ? AND shadow = 0", (sv, as_of))}
    prev_as_of = await db.fetchval("SELECT MAX(as_of) FROM scores WHERE spec_version = ? AND run_kind = 'official' "
                                   "AND shadow = 0 AND as_of <= ?", (sv, as_of - 7 * DAY))
    prev = {} if prev_as_of is None else {r["occupant_key"]: r for r in await db.fetchall(
        "SELECT occupant_key, composite_smoothed, red_flags FROM scores WHERE spec_version = ? AND as_of = ? "
        "AND shadow = 0", (sv, prev_as_of))}
    ab = sorted((k for k, r in cur.items() if r["tier"] in ("A", "B")),
                key=lambda k: (-(cur[k]["composite"] or 0.0), k))
    movers = []
    for k, r in cur.items():
        p = prev.get(k)
        if p and r["composite_smoothed"] is not None and p["composite_smoothed"] is not None:
            movers.append((abs(r["composite_smoothed"] - p["composite_smoothed"]), k))
    movers = [k for _, k in sorted(movers, key=lambda z: (-z[0], z[1]))[:MOVERS]]
    flagged = []
    for k, r in sorted(cur.items()):
        now_f = set(json.loads(r["red_flags"] or "[]"))
        was = set(json.loads(prev[k]["red_flags"] or "[]")) if k in prev else set()
        if now_f - was:
            flagged.append(k)
    out: list[str] = []
    for k in [*ab, *movers, *flagged]:
        if k not in out:
            out.append(k)
    return out[:max(0, int(max_subnets))]


async def _norm_values(db: "Database", occupant_key: str, keys: tuple[str, ...], as_of: int | None) -> list[str]:
    if as_of is None:
        return []
    rows = await db.fetchall(
        f"SELECT metric_key, value, value_json FROM metric_values WHERE occupant_key = ? AND stage = 'norm' AND "
        f"spec_version = ? AND as_of = ? AND metric_key IN ({','.join('?' * len(keys))})",
        (occupant_key, _s().SPEC_VERSION, as_of, *keys))
    out = []
    for r in sorted(rows, key=lambda r: keys.index(r["metric_key"])):
        raw = json.loads(r["value_json"] or "{}").get("raw")
        v = "n/a" if r["value"] is None else f"{float(r['value']):.3f}"
        out.append(f"- {r['metric_key']}: m={v} raw={raw}")
    return out


async def build_evidence(db: "Database", occupant_key: str, *, token_budget: int) -> tuple[str, str]:
    """(bundle_text, sha256). Sections in §8.5 order; whole sections dropped / the last one cut to fit the budget."""
    from app.nlp.resolve import policy_for, resolve_labels
    s = _s()
    now = now_ts()
    as_of = await _latest_official(db)
    occ = await db.fetchone("SELECT * FROM occupants WHERE occupant_key = ?", (occupant_key,))
    score = await db.fetchone("SELECT * FROM scores WHERE occupant_key = ? AND spec_version = ? AND as_of = ? "
                              "AND shadow = 0", (occupant_key, s.SPEC_VERSION, as_of)) if as_of else None
    sections: list[tuple[str, list[str]]] = []
    reg = (occ or {}).get("registered_ts") or (occ or {}).get("first_seen_at")
    sections.append(("IDENTITY", [
        f"name: {(occ or {}).get('name')}", f"netuid: {(occ or {}).get('netuid')}", f"occupant_key: {occupant_key}",
        f"age_days: {int((now - reg) / DAY) if reg else '?'}", f"status: {(occ or {}).get('status')}",
        f"red_flags: {(score or {}).get('red_flags', '[]')}"]))
    if score:
        sections.append(("SCORE_EXPLAIN", [(score.get("explain") or "{}")[:4000]]))
        sections.append(("PILLARS", [f"as_of: {to_iso(score['as_of'])} tier: {score['tier']} composite: "
                                     f"{score['composite']} confidence: {score['confidence']}",
                                     f"pillar_scores: {score['pillar_scores']}",
                                     f"pillar_coverage: {score['pillar_coverage']}"]))
    evs = await db.fetchall("SELECT event_type, severity, event_at, observed_at, payload FROM events WHERE "
                            "occupant_key = ? AND COALESCE(event_at, observed_at) > ? ORDER BY "
                            "COALESCE(event_at, observed_at) DESC LIMIT 40", (occupant_key, now - 14 * DAY))
    ev_lines = []
    for e in evs:
        p = json.loads(e["payload"] or "{}")
        ev_lines.append(f"- {_date(e['event_at'] or e['observed_at'])} {e['event_type']} ({e['severity']}) "
                        f"dir={p.get('direction', '')} mat={p.get('materiality', '')} "
                        f"{p.get('summary') or ''}".rstrip())
    sections.append(("EVENTS_14D", ev_lines or ["(none)"]))
    labs = await resolve_labels(db, policy_for(), occupant_key=occupant_key, since=now - 14 * DAY, until=now,
                                as_of=now, min_conf=float(s.LLM_ENTITY_MIN_CONFIDENCE))
    labs = [x for x in labs if not x.is_spam and x.summary]
    eng = await _engagement(db, [x.post_id for x in labs])
    labs.sort(key=lambda x: (-x.materiality, -eng.get(x.post_id, 0), -x.event_at))
    sections.append(("TOP_LABELS", [f"- {_date(x.event_at)} {x.event_type} {x.polarity} mat={x.materiality}: "
                                    f"{x.summary}" for x in labs[:15]] or ["(none)"]))
    for title, keys in (("P1", P1_KEYS), ("P4", P4_KEYS), ("P2", P2_KEYS)):
        sections.append((f"{title}_HIGHLIGHTS", await _norm_values(db, occupant_key, keys, as_of) or ["(none)"]))
    notes = await db.fetchall("SELECT created_at, title, body FROM manual_notes WHERE occupant_key = ? AND "
                              "kind = 'note' ORDER BY created_at DESC, id DESC LIMIT 3", (occupant_key,))
    sections.append(("OPERATOR_NOTES", [f"- {_date(n['created_at'])} {n['title'] or ''} {(n['body'] or '')[:600]}"
                                        for n in notes] or ["(none)"]))
    prev = await db.fetchone("SELECT created_at, body FROM manual_notes WHERE occupant_key = ? AND "
                             "kind = 'llm_narrative' ORDER BY created_at DESC, id DESC LIMIT 1", (occupant_key,))
    if prev:
        words = (prev["body"] or "").split()
        sections.append(("PREVIOUS_NARRATIVE", [f"{_date(prev['created_at'])}: " + " ".join(words[:150])]))
    budget_chars = int(int(token_budget) / max(1e-6, float(s.LLM_TOKENS_PER_CHAR)))
    text = ""
    for title, lines in sections:
        block = f"## {title}\n" + "\n".join(lines) + "\n\n"
        if len(text) + len(block) > budget_chars:
            room = budget_chars - len(text)
            if room > 200:
                text += block[:room - 1] + "…"
            break
        text += block
    text = text.rstrip()
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()


async def _engagement(db: "Database", post_ids: list[str]) -> dict[str, int]:
    xs = [p[2:] for p in post_ids if p.startswith("x:")]
    out: dict[str, int] = {}
    for i in range(0, len(xs), 400):
        part = xs[i:i + 400]
        for r in await db.fetchall(
                "SELECT post_id, COALESCE(like_count,0) + 2*COALESCE(repost_count,0) + COALESCE(reply_count,0) + "
                f"COALESCE(quote_count,0) AS e FROM social_posts WHERE platform = 'x' AND post_id IN "
                f"({','.join('?' * len(part))})", part):
            out["x:" + r["post_id"]] = int(r["e"])
    return out


async def run_weekly(db: "Database", client: Any, governor: Any, limiter: Any, run_id: str, *,
                     force: bool = False) -> dict:
    from app.nlp.llm_classify import LlmClassifier, message_text, usage_of, usage_usd
    s = _s()
    now = now_ts()
    model = s.LLM_NARRATIVE_MODEL
    subjects = await select_subjects(db, max_subnets=int(s.LLM_NARRATIVE_MAX_SUBNETS))
    clf = LlmClassifier(db, client, governor, limiter)
    system = R.load_narrative_prompt()
    as_of = await _latest_official(db)
    out: dict[str, Any] = {"subjects": len(subjects), "written": 0, "skipped": 0, "refused": 0, "denied": 0,
                           "failed": 0, "cost_usd": 0.0}
    for occ in subjects:
        if not force and await db.fetchval("SELECT 1 FROM manual_notes WHERE occupant_key = ? AND kind = "
                                           "'llm_narrative' AND created_at > ? LIMIT 1", (occ, now - 6 * DAY)):
            out["skipped"] += 1
            continue
        bundle, sha = await build_evidence(db, occ, token_budget=int(s.LLM_NARRATIVE_EVIDENCE_TOKENS))
        est = estimate_llm_usd(model, R.token_estimate(system + bundle), EST_OUT_TOKENS)
        d = await governor.check("anthropic", est)
        if not d.allowed:
            out["denied"] += 1
            logger.info("narrative denied by governor", data={"reason": d.reason, "occupant_key": occ})
            break
        try:
            msg = await clf.call_direct(job=JOB, run_id=run_id, model=model, system=system, user=bundle,
                                        max_tokens=R.NARRATIVE_MAX_TOKENS, schema=json_schema_narrative(),
                                        effort=R.NARRATIVE_EFFORT, timeout=float(s.LLM_REQUEST_TIMEOUT_S))
        except Exception as exc:
            await governor.record("anthropic", 0.0, requests=1, job=JOB, run_id=run_id)
            out["failed"] += 1
            logger.warning("narrative call failed", data={"occupant_key": occ, "error": f"{type(exc).__name__}"})
            continue
        u = usage_of(msg)
        usd = usage_usd(model, u, batch=False)
        out["cost_usd"] += usd
        resolved = getattr(msg, "model", None) or model
        await governor.record("anthropic", usd, requests=1, units=u, job=JOB, run_id=run_id,
                              detail={"model": resolved, "occupant_key": occ})
        await clf._snapshot_check(JOB, model, resolved)
        if getattr(msg, "stop_reason", None) == "refusal":
            out["refused"] += 1
            logger.info("narrative refused", data={"occupant_key": occ, "category": getattr(
                getattr(msg, "stop_details", None), "category", None)})
            continue
        try:
            nar = parse_narrative(message_text(msg))
        except RequestRejected:
            out["failed"] += 1
            logger.info("narrative parse failure", data={"occupant_key": occ,
                                                         "stop_reason": getattr(msg, "stop_reason", None)})
            continue
        payload = {"risks": nar.risks, "catalysts": nar.catalysts, "confidence": nar.confidence, "model": resolved,
                   "evidence_sha256": sha, "input_tokens": u["in"], "output_tokens": u["out"],
                   "cost_usd": round(usd, 6), "spec_version": s.SPEC_VERSION, "score_as_of": as_of}
        await db.execute("INSERT INTO manual_notes (occupant_key, kind, title, body, payload_json, created_at, "
                         "created_by) VALUES (?, 'llm_narrative', ?, ?, ?, ?, ?)",
                         (occ, f"Narrative {to_iso(as_of or now)} ({resolved})", nar.narrative,
                          json.dumps(payload, ensure_ascii=False), now_ts(), f"llm:{resolved}"))
        out["written"] += 1
    return out


class LlmNarrativeWeeklyJob(CollectorJob):
    """llm_narrative_weekly (paid, Sun 01:00Z)."""

    async def collect(self, run: RunContext) -> JobOutcome:
        s = run.settings
        if not s.ANTHROPIC_API_KEY:
            raise RuntimeError("auth_failed: ANTHROPIC_API_KEY is empty")
        from app.nlp.llm_classify import make_client
        res = await run_weekly(run.rt.db, make_client(s), run.rt.costs, run.rt.limiter, run.run_id,
                               force=run.trigger in ("cli", "manual"))
        return JobOutcome(rows=res["written"], cost_usd=round(res["cost_usd"], 8),
                          note=f"written={res['written']} refused={res['refused']} denied={res['denied']}", meta=res)


__all__ = ["select_subjects", "build_evidence", "run_weekly", "LlmNarrativeWeeklyJob"]
