"""Event materialisation and the LLM review hand-off (spec 10 §2.2 "Events", §3.2 events/review_queue rows, 02 §D1/§D5).

One ``events`` row per cluster ``(occupant_key, event_type, direction, floor(event_at / 172800))`` with
``dedupe_key = f"{occ}|{event_type}|{bucket}|{pos|neg|neu}"``, built from resolved labels with materiality ≥ 2, a linked
entity (confidence ≥ ``LLM_ENTITY_MIN_CONFIDENCE``) and ``is_spam = 0``. A bulk-tier negative of materiality 3 joins a
cluster only once Sonnet re-labelled it (escalation tier) or a human confirmed it (``confirm_review``). Payloads are
updated only while ``alerted_at IS NULL`` (03 trigger); P6 metrics never read event payloads except the confirmation
marker, which carries its own ``confirmed_at`` (PIT: a later confirmation never changes an earlier ``as_of``).

``confirm_review`` is called by 11's ``resolve_review`` (never by 14 directly) and never writes ``manual_flags``.
"""
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Iterable

import app.config as app_config
from app.logging.logger import logger
from app.nlp.lexicon import KEY_COMPROMISE_RE
from app.nlp.resolve import LabelPolicy, ResolvedLabel, resolve_labels
from app.storage.events import emit_event
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

BUCKET_S = 172800                    # 48-h cluster width (frozen: scoring/spec.P6_CLUSTER_WIDTH_S)
EVENT_MAX_AGE_S = 14 * 86400         # labels of older items notify nothing (they are history, not news)
ESCALATE_TYPES = ("owner_extraction_evidence", "security_incident")
DIR_NAME = {1: "pos", -1: "neg", 0: "neu"}


def _settings():
    return app_config.settings


def dedupe_key(occupant_key: str, event_type: str, event_at: int, direction: int) -> str:
    return f"{occupant_key}|{event_type}|{int(event_at) // BUCKET_S}|{DIR_NAME[int(direction)]}"


def is_escalation_candidate(event_type: str, direction: int, materiality: int) -> bool:
    """§2.2 Escalation: negative materiality-3, or an owner-extraction / security label of any materiality."""
    return (int(direction) == -1 and int(materiality) == 3) or event_type in ESCALATE_TYPES


def needs_confirmation(lab: ResolvedLabel) -> bool:
    """Master §7.4: a bulk negative of materiality 3 counts only after Sonnet or a human confirms it."""
    return lab.tier == "bulk" and lab.direction == -1 and lab.materiality == 3


def severity_of(direction: int, materiality: int) -> str:
    if direction < 0 and materiality >= 3:
        return "critical"
    if direction < 0 and materiality == 2:
        return "warning"
    return "info"


def suggest_flag(event_type: str, direction: int, materiality: int, text: str | None) -> tuple[str | None, int] | None:
    """(suggested_flag, priority) of a label that needs a human, or None (§3.2 review_queue rules 1–3)."""
    if event_type == "owner_extraction_evidence" and materiality >= 2:
        return "owner_extraction", 1
    if event_type == "security_incident" and materiality == 3:
        return ("key_compromise", 1) if KEY_COMPROMISE_RE.search(text or "") else ("security_incident", 2)
    if direction == -1 and materiality == 3:
        return None, 2
    return None


def _occ_netuid(occupant_key: str | None) -> int | None:
    try:
        return int(str(occupant_key).split(":", 1)[0]) if occupant_key else None
    except ValueError:
        return None


async def confirmed_posts(db: "Database", occupant_key: str | None, t: int) -> set[str]:
    """Post ids human/Sonnet-confirmed by an ``llm`` event visible at ``t`` (``observed_at ≤ t`` and
    ``COALESCE(payload.confirmed_at, observed_at) ≤ t``)."""
    where = ["source = 'llm'", "observed_at <= ?", "json_extract(payload, '$.confirmed_by') IS NOT NULL",
             "COALESCE(json_extract(payload, '$.confirmed_at'), observed_at) <= ?"]
    args: list[Any] = [int(t), int(t)]
    if occupant_key is not None:
        where.insert(0, "occupant_key = ?")
        args.insert(0, occupant_key)
    rows = await db.fetchall(f"SELECT payload FROM events WHERE {' AND '.join(where)}", args)
    out: set[str] = set()
    for r in rows:
        try:
            p = json.loads(r["payload"] or "{}")
        except ValueError:
            continue
        ids = p.get("confirmed_post_ids")
        if ids is None:
            ids = p.get("post_ids") or []
        out.update(str(x) for x in ids)
    return out


async def enqueue_review(db: "Database", *, occupant_key: str | None, post_id: str, label_version: str,
                         event_type: str, materiality: int, summary: str | None, evidence_url: str | None,
                         suggested_flag: str | None, reason: str, priority: int, ref_id: int | None = None,
                         direction: int | None = None, confidence: float | None = None,
                         now: int | None = None) -> int | None:
    """``INSERT OR IGNORE`` one ``llm_flag_candidate`` row per label (``ux_review_queue_post``) via 11's ``enqueue``."""
    from app.integrity.review import enqueue
    return await enqueue(db, occupant_key=occupant_key, kind="llm_flag_candidate", ref_table="events", ref_id=ref_id,
                         priority=int(priority), suggested_flag=suggested_flag, summary=summary,
                         payload={"reason": reason, "direction": direction, "confidence": confidence},
                         evidence_url=evidence_url, event_type=event_type, materiality=int(materiality),
                         post_id=post_id, label_version=label_version, now=now)


async def _has_review(db: "Database", post_id: str) -> bool:
    return bool(await db.fetchval("SELECT 1 FROM review_queue WHERE kind = 'llm_flag_candidate' AND post_id = ? "
                                  "LIMIT 1", (post_id,), writer=True))


async def _event_row(db: "Database", event_type: str, key: str) -> dict | None:
    return await db.fetchone("SELECT id, payload, severity, alerted_at FROM events WHERE event_type = ? AND "
                             "dedupe_key = ?", (event_type, key), writer=True)


async def _urls(db: "Database", post_ids: Iterable[str]) -> dict[str, str | None]:
    from app.nlp.queue import load_items
    items = await load_items(db, post_ids)
    return {k: v.url for k, v in items.items()}


def _top(labels: list[ResolvedLabel]) -> ResolvedLabel:
    return sorted(labels, key=lambda x: (-x.materiality, -x.confidence, x.event_at, x.post_id))[0]


async def _upsert_cluster(db: "Database", occ: str, labels: list[ResolvedLabel], run_id: str, now: int,
                          *, confirmed: dict[str, str]) -> tuple[str, int | None]:
    """Write/upgrade one cluster event. ``confirmed`` = {post_id: confirmed_by} for labels confirmed by escalation."""
    top = _top(labels)
    d = top.direction
    m = max(x.materiality for x in labels)
    ev_at = min(x.event_at for x in labels)
    key = dedupe_key(occ, top.event_type, ev_at, d)
    post_ids = [x.post_id for x in sorted(labels, key=lambda x: (x.event_at, x.post_id))]
    url = (await _urls(db, [top.post_id])).get(top.post_id)
    payload: dict[str, Any] = {"post_ids": post_ids, "label_version": top.classifier_version, "direction": d,
                               "materiality": m, "summary": top.summary, "url": url,
                               "confidence": round(top.confidence, 4), "n_supporting": len(post_ids)}
    conf = {p: by for p, by in confirmed.items() if p in post_ids}
    if conf:
        payload["confirmed_by"] = next(iter(conf.values()))
        payload["confirmed_at"] = now
        payload["confirmed_post_ids"] = sorted(conf)
    sev = severity_of(d, m)
    cur = await _event_row(db, top.event_type, key)
    if cur is None:
        eid = await emit_event(db, event_type=top.event_type, occupant_key=occ, netuid=_occ_netuid(occ),
                               severity=sev, event_at=ev_at, observed_at=now, payload=payload, dedupe_key=key,
                               source="llm", run_id=run_id)
        return ("new" if eid is not None else "same"), eid
    if cur["alerted_at"] is not None:
        return "alerted", int(cur["id"])
    old = json.loads(cur["payload"] or "{}")
    merged = dict(payload)
    merged["post_ids"] = list(dict.fromkeys([*(old.get("post_ids") or []), *post_ids]))
    merged["n_supporting"] = len(merged["post_ids"])
    merged["materiality"] = max(int(old.get("materiality") or 0), m)
    for k in ("confirmed_by", "confirmed_at", "confirm_note"):        # an earlier confirmation is kept as it was
        if old.get(k) is not None:
            merged[k] = old[k]
    cp = list(dict.fromkeys([*(old.get("confirmed_post_ids") or []), *(payload.get("confirmed_post_ids") or [])]))
    if cp:
        merged["confirmed_post_ids"] = cp
        merged.setdefault("confirmed_at", now)
    sev = severity_of(d, merged["materiality"]) if (d >= 0 or merged["materiality"] < 3 or cp) else cur["severity"]
    if merged == old and sev == cur["severity"]:
        return "same", int(cur["id"])
    await db.execute("UPDATE events SET payload = ?, severity = ? WHERE id = ? AND alerted_at IS NULL",
                     (json.dumps(merged, separators=(",", ":"), sort_keys=True, default=str), sev, cur["id"]))
    return "updated", int(cur["id"])


async def _item_texts(db: "Database", post_ids: Iterable[str]) -> dict[str, str | None]:
    from app.nlp.queue import load_items
    return {k: v.text for k, v in (await load_items(db, post_ids)).items()}


async def materialise_events(db: "Database", policy: LabelPolicy, run_id: str, *, since_hours: int = 72,
                             now: int | None = None) -> dict:
    """Events + review rows for labels written in the last ``since_hours`` (idempotent; the hourly rules job reruns
    it as a crash safety net)."""
    s = _settings()
    now = int(now if now is not None else now_ts())
    min_conf = float(s.LLM_ENTITY_MIN_CONFIDENCE)
    versions = policy.versions()
    ph = ",".join("?" * len(versions))
    rows = await db.fetchall(
        f"SELECT DISTINCT post_id FROM post_labels WHERE classifier_version IN ({ph}) AND observed_at >= ? "
        "AND observed_at <= ? AND event_at > ?", [*versions, now - int(since_hours) * 3600, now, now - EVENT_MAX_AGE_S],
        writer=True)
    pids = [r["post_id"] for r in rows]
    out = {"labels": len(pids), "events_new": 0, "events_updated": 0, "reviews": 0}
    if not pids:
        return out
    labels = await resolve_labels(db, policy, occupant_key=None, since=now - EVENT_MAX_AGE_S, until=now, as_of=now,
                                  post_ids=pids)
    texts = await _item_texts(db, [x.post_id for x in labels if suggest_flag(x.event_type, x.direction,
                                                                              x.materiality, None) is not None])
    human = await confirmed_posts(db, None, now)
    clusters: set[tuple[str, str, int, int]] = set()
    for lab in labels:
        await _maybe_review(db, lab, texts.get(lab.post_id), now=now, out=out)
        if lab.materiality < 2 or lab.is_spam:
            continue
        if needs_confirmation(lab) and lab.post_id not in human:
            continue
        for e in lab.entities:
            if float(e["confidence"]) >= min_conf:
                clusters.add((e["occupant_key"], lab.event_type, lab.direction, lab.event_at // BUCKET_S))
    for occ, et, d, b in sorted(clusters):
        members = await resolve_labels(db, policy, occupant_key=occ, since=b * BUCKET_S - 1,
                                       until=(b + 1) * BUCKET_S - 1, as_of=now, min_conf=min_conf)
        members = [x for x in members if x.event_type == et and x.direction == d and x.materiality >= 2
                   and not x.is_spam and (not needs_confirmation(x) or x.post_id in human)]
        if not members:
            continue
        confirmed = {x.post_id: (x.model_resolved or x.classifier_version.split(":", 1)[0]) for x in members
                     if x.tier == "escalation" and x.direction == -1 and x.materiality == 3}
        what, _ = await _upsert_cluster(db, occ, members, run_id, now, confirmed=confirmed)
        if what == "new":
            out["events_new"] += 1
        elif what == "updated":
            out["events_updated"] += 1
    return out


async def _maybe_review(db: "Database", lab: ResolvedLabel, text: str | None, *, now: int, out: dict) -> None:
    s = _settings()
    sug = suggest_flag(lab.event_type, lab.direction, lab.materiality, f"{lab.summary or ''}\n{text or ''}")
    if sug is None:
        return
    if lab.tier == "escalation" and sug[0] is None:
        return                                       # Sonnet-confirmed negative: counts, no human needed
    if lab.tier == "bulk" and is_escalation_candidate(lab.event_type, lab.direction, lab.materiality) \
            and s.LLM_ESCALATION_ENABLED and now - lab.observed_at <= int(s.LLM_FALLBACK_AFTER_H) * 3600 \
            and text is not None:
        return                                       # escalation still pending: escalate() decides (§2.2)
    if await _has_review(db, lab.post_id):
        return
    occ = (lab.top_entity or {}).get("occupant_key")
    url = (await _urls(db, [lab.post_id])).get(lab.post_id)
    reason = "always_reviewed" if sug[0] == "owner_extraction" else (
        "security_incident" if lab.event_type == "security_incident" else "not_escalated")
    rid = await enqueue_review(db, occupant_key=occ, post_id=lab.post_id, label_version=lab.classifier_version,
                               event_type=lab.event_type, materiality=lab.materiality, summary=lab.summary,
                               evidence_url=url, suggested_flag=sug[0], reason=reason, priority=sug[1],
                               direction=lab.direction, confidence=lab.confidence, now=now)
    if rid:
        out["reviews"] = out.get("reviews", 0) + 1


async def confirm_review(db: "Database", review_id: int, *, by: str, note: str | None = None,
                         now: int | None = None) -> int | None:
    """Human confirmation of an ``llm_flag_candidate``: write/upgrade the cluster event with
    ``payload.confirmed_by = by`` (+ ``confirmed_at``, ``confirmed_post_ids``); critical when negative mat 3.
    Returns ``events.id`` (None when the row/label is unknown). Never writes ``manual_flags`` (02 §D5)."""
    now = int(now if now is not None else now_ts())
    row = await db.fetchone("SELECT * FROM review_queue WHERE id = ?", (int(review_id),), writer=True)
    if row is None or row["kind"] != "llm_flag_candidate" or not row["post_id"]:
        return None
    lab = await db.fetchone("SELECT * FROM post_labels WHERE post_id = ? AND classifier_version = ?",
                            (row["post_id"], row["label_version"]), writer=True)
    if lab is None:
        logger.warning("confirm_review: label not found", data={"review_id": review_id, "post_id": row["post_id"],
                                                                "label_version": row["label_version"]})
        return None
    occ = row["occupant_key"]
    if not occ:
        occ = await db.fetchval("SELECT occupant_key FROM post_label_entities WHERE post_id = ? AND "
                                "classifier_version = ? ORDER BY confidence DESC LIMIT 1",
                                (row["post_id"], row["label_version"]), writer=True)
    if not occ:
        return None
    et, d, m = lab["event_type"], int(lab["direction"]), int(lab["materiality"])
    key = dedupe_key(occ, et, int(lab["event_at"]), d)
    cur = await _event_row(db, et, key)
    pid = row["post_id"]
    if cur is not None and cur["alerted_at"] is None:
        p = json.loads(cur["payload"] or "{}")
        p["post_ids"] = list(dict.fromkeys([*(p.get("post_ids") or []), pid]))
        p["n_supporting"] = len(p["post_ids"])
        p["materiality"] = max(int(p.get("materiality") or 0), m)
        p["confirmed_by"] = by
        p["confirmed_at"] = now
        p["confirmed_post_ids"] = list(dict.fromkeys([*(p.get("confirmed_post_ids") or []), pid]))
        if note:
            p["confirm_note"] = note
        sev = severity_of(d, p["materiality"])
        await db.execute("UPDATE events SET payload = ?, severity = ? WHERE id = ? AND alerted_at IS NULL",
                         (json.dumps(p, separators=(",", ":"), sort_keys=True, default=str), sev, cur["id"]))
        return int(cur["id"])
    url = row["evidence_url"] or (await _urls(db, [pid])).get(pid)
    payload = {"post_ids": [pid], "label_version": row["label_version"], "direction": d, "materiality": m,
               "summary": lab["summary"], "url": url, "confidence": float(lab["confidence"]), "n_supporting": 1,
               "confirmed_by": by, "confirmed_at": now, "confirmed_post_ids": [pid]}
    if note:
        payload["confirm_note"] = note
    if cur is not None:                              # the cluster event was already alerted: a separate row
        key = f"{key}|confirmed:{pid}"
    eid = await emit_event(db, event_type=et, occupant_key=occ, netuid=_occ_netuid(occ), severity=severity_of(d, m),
                           event_at=int(lab["event_at"]), observed_at=now, payload=payload, dedupe_key=key,
                           source="llm", run_id=f"review:{review_id}")
    if eid is None:
        eid = await db.fetchval("SELECT id FROM events WHERE event_type = ? AND dedupe_key = ?", (et, key),
                                writer=True)
    return int(eid) if eid is not None else None


__all__ = ["materialise_events", "enqueue_review", "confirm_review", "confirmed_posts", "dedupe_key",
           "is_escalation_candidate", "needs_confirmation", "severity_of", "suggest_flag", "BUCKET_S"]
