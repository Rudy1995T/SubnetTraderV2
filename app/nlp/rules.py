"""Rule-based fallback classifier ``rules:v2`` and the hourly ``llm_rules_fallback`` job (spec 10 §8.4, §6.1).

Runs when the LLM is off (``LLM_ENABLED=false`` / no key / zero cap), budget- or kill-switch-stopped (all eligible
items, immediately), and otherwise for items left unlabelled more than ``LLM_FALLBACK_AFTER_H`` (or exhausted by LLM
retries, or too old for the LLM lookback). Rules never emit materiality 3 (so nothing they write escalates). After labelling it
reruns ``materialise_events(since_hours=72)`` as the crash safety net.
"""
from __future__ import annotations

import math
import re
from typing import TYPE_CHECKING

import app.config as app_config
from app.jobs import CollectorJob, JobOutcome, RunContext
from app.logging.logger import logger
from app.nlp import lexicon as LX
from app.nlp.aliases import AliasIndex, OccupantsAt
from app.nlp.match import KIND_RANK, has_context, is_ambiguous, match_occupants, norm
from app.nlp.queue import QueueItem, _candidates, attempt_stats, llm_exhausted
from app.nlp.rubric import RULES_VERSION, current_versions
from app.nlp.schema import DEFAULT_DIRECTION, LabelV1
from app.util.time import now_ts

if TYPE_CHECKING:  # pragma: no cover
    from app.costs import CostGovernor
    from app.storage.db import Database

DAY = 86400
RULES_MAX_AGE_DAYS = 30              # P6's longest window (kol_coverage_30d); older items are history
RULES_MAX_CHARS = 4000
BUDGET_STOP_TTL_S = 12 * 3600        # a submit `skipped: budget` keeps the fallback in "all" mode for two cycles
_EMOJI_RE = re.compile("|".join(re.escape(e) for e in sorted(LX.EMOJI, key=len, reverse=True)))
_CAPS_RE = re.compile(r"\b[A-Z]{3,}\b")
_NETUID_TOKENS = re.compile(r"\$?\b(?:sn|subnet|netuid)\s?#?\d{1,3}\b|\$sn\d{1,3}\b", re.I)
_NUMBER_DOLLAR = re.compile(r"\d|\$(?!sn\d|tao\b)", re.I)


AMBIGUOUS_CONF = 0.5          # < LLM_ENTITY_MIN_CONFIDENCE: stored in raw_json, never counted
_CUE = r"(?:subnet|sn ?\d{1,3}|\(sn ?\d{1,3}\)|netuid ?\d{1,3})"


def _cued(name: str, text: str) -> bool:
    """An ordinary-word name directly next to a subnet cue: "Score subnet", "subnet Score", "Score (SN44)"."""
    n = re.escape(norm(name).strip())
    return bool(re.search(rf"(?<!\w){n}(?:'s)?\W{{0,3}}{_CUE}|{_CUE}\W{{0,3}}{n}(?!\w)", norm(text)))


class RulesClassifier:
    VERSION = RULES_VERSION

    # polarity ---------------------------------------------------------------------------------------------------
    @staticmethod
    def polarity(text: str) -> tuple[str, int, float]:
        toks = LX.TOKEN_RE.findall(text.lower())
        pos = neg = 0.0
        n_terms = 0
        for i, tok in enumerate(toks):
            w = LX.POS.get(tok)
            if w is None:
                w = -LX.NEG[tok] if tok in LX.NEG else None
            if not w:
                continue
            n_terms += 1
            if i > 0 and toks[i - 1] in LX.BOOSTERS:
                w *= LX.BOOSTERS[toks[i - 1]]
            if any(t in LX.NEGATORS for t in toks[max(0, i - LX.NEG_WINDOW):i]):
                w = -w
            if w > 0:
                pos += w
            else:
                neg += -w
        for m in _EMOJI_RE.finditer(text):
            w = LX.EMOJI[m.group(0)]
            n_terms += 1
            if w > 0:
                pos += w
            else:
                neg += -w
        raw = pos - neg
        caps = [c for c in _CAPS_RE.findall(text) if not re.fullmatch(r"SN\d*|TAO|DTAO|USD|API|AI|ARR|MRR", c)]
        bonus = min(0.5, 0.1 * min(text.count("!"), 4) + (0.2 if caps else 0.0))
        if raw > 0:
            raw += bonus
        elif raw < 0:
            raw -= bonus
        score = raw / math.sqrt(n_terms ** 2 + LX.VADER_ALPHA) if n_terms else 0.0
        if score > LX.POS_THRESHOLD:
            pol = "bullish"
        elif score < -LX.POS_THRESHOLD:
            pol = "bearish"
        else:
            return "neutral", 1, score
        return pol, min(5, 1 + round(4 * abs(score))), score

    @staticmethod
    def event_type(text: str) -> str:
        for et, rx in LX.EVENT_RULES:
            if rx.search(text):
                return et
        return "other"

    @staticmethod
    def materiality(text: str, event_type: str) -> int:
        if event_type in ("other", "giveaway_promo"):
            return 1                                    # rubric: (0, mat 1)
        stripped = _NETUID_TOKENS.sub(" ", text)        # "SN64" is a reference, not a number
        return 2 if (LX.URL_RE.search(stripped) or _NUMBER_DOLLAR.search(stripped)) else 1

    @staticmethod
    def entities(text: str, aliases: AliasIndex | None) -> tuple[list[dict], bool]:
        """([{netuid, confidence}], explicit_netuid). Netuid regex → 1.0; registry name/alias/handle → 0.7; an
        ordinary-word name or alias (``is_ambiguous``) → 0.5, below the 0.6 counting threshold — Bittensor context alone
        is not enough in long-form text ("liquidity injections" in a TAO explainer is not SN77 Liquidity; live
        2026-10-18)."""
        found: dict[int, float] = {}
        for m in LX.NETUID_RE.finditer(text):
            n = int(m.group(1) or m.group(2))
            if 0 < n <= 1023:
                found[n] = 1.0
        explicit = bool(found)
        if aliases is not None:
            for occ, kind in match_occupants(text, aliases):
                if KIND_RANK[kind] == 0:
                    continue                            # sn / cashtag already handled by the netuid regex
                ref = aliases.by_key.get(occ)
                if ref is None:
                    continue
                if kind == "name" and len(norm(ref.name or "").strip()) < 4:
                    continue
                terms = [ref.name or ""] if kind == "name" else [a for a in ref.aliases if norm(a) in norm(text)]
                generic = kind in ("name", "alias") and all(is_ambiguous(t) and not _cued(t, text) for t in terms)
                found.setdefault(ref.netuid, AMBIGUOUS_CONF if generic else 0.7)
        return [{"netuid": n, "confidence": c} for n, c in sorted(found.items())], explicit

    def classify(self, item: QueueItem, aliases: AliasIndex | None) -> LabelV1:
        text = item.text or ""
        pol, intensity, _ = self.polarity(text)
        et = self.event_type(text)
        ents, explicit = self.entities(text, aliases)
        context = bool(LX.CONTEXT_RE.search(text)) or has_context(text) or bool(ents)
        spam = bool((LX.SHORTENER_RE.search(text) and LX.SEND_TAO_RE.search(text))
                    or (not context and not item.occupant_key))
        summary = " ".join(text.split())[:140]
        return LabelV1.model_validate({
            "id": item.post_id, "polarity": pol, "intensity": intensity, "event_type": et,
            "direction": DEFAULT_DIRECTION[et], "materiality": self.materiality(text, et), "entities": ents,
            "is_spam": spam, "is_promo": bool(LX.GIVEAWAY_RE.search(text)), "summary": summary,
            "confidence": 0.5 if explicit else 0.4})


async def fallback_mode(db: "Database", governor: "CostGovernor | None", now: int) -> str | None:
    """'llm_off' | 'budget' | 'kill_switch' → label everything eligible now; None → only stale/exhausted items."""
    s = app_config.settings
    if not s.llm_active():
        return "llm_off"
    if governor is not None:
        r = governor.paused_reason("anthropic")
        if r:
            return r
    else:
        from app.control import kill_switch_active
        if kill_switch_active():
            return "kill_switch"
    v = await db.fetchval("SELECT value_json FROM job_state WHERE job = 'llm_classify_submit' AND key = "
                          "'budget_stop'", writer=True)
    if v:
        import json
        try:
            at = int(json.loads(v).get("at", 0))
        except (ValueError, AttributeError, TypeError):
            at = 0
        if now - at <= BUDGET_STOP_TTL_S:
            return "budget"
    return None


async def select_for_rules(db: "Database", *, now: int, mode: str | None) -> list[QueueItem]:
    s = app_config.settings
    bulk = current_versions()["bulk"]
    cands = await _candidates(db, exclude_versions=(bulk, RULES_VERSION), min_event_at=now - RULES_MAX_AGE_DAYS * DAY,
                              max_chars=RULES_MAX_CHARS)
    if mode is not None:
        return cands
    stats, _open = await attempt_stats(db, bulk)
    stale = now - int(s.LLM_FALLBACK_AFTER_H) * 3600
    too_old = now - int(s.LLM_LOOKBACK_DAYS) * DAY
    out = []
    for q in cands:
        e, f = stats.get(q.post_id, (0, 0))
        if q.observed_at < stale or q.event_at < too_old or llm_exhausted(e, f):
            out.append(q)
    return out


async def rules_fallback_cycle(db: "Database", run_id: str, *, governor: "CostGovernor | None" = None,
                               now: int | None = None) -> dict:
    from app.nlp.events import materialise_events
    from app.nlp.llm_classify import LabelEntry, write_label_rows
    from app.nlp.resolve import policy_for
    now = int(now if now is not None else now_ts())
    mode = await fallback_mode(db, governor, now)
    items = await select_for_rules(db, now=now, mode=mode)
    labelled = 0
    if items:
        idx = await AliasIndex.load(db)
        occ_at = await OccupantsAt.load(db)
        clf = RulesClassifier()
        for i in range(0, len(items), 500):
            part = items[i:i + 500]
            entries = [LabelEntry(q, clf.classify(q, idx), via="regex") for q in part]
            labelled += await write_label_rows(db, entries, version=RULES_VERSION, tier="rules", source="rules",
                                               run_id=run_id, batch_key=None, occ_at=occ_at, now=now)
        ids = [q.post_id for q in items]
        for i in range(0, len(ids), 400):
            part = ids[i:i + 400]
            await db.execute("UPDATE llm_batch_items SET status = 'fallback' WHERE status IN ('errored','requeued') "
                             f"AND post_id IN ({','.join('?' * len(part))})", part)
    ev = await materialise_events(db, policy_for(), run_id, since_hours=72, now=now)
    res = {"mode": mode or "stale_only", "eligible": len(items), "labelled": labelled, "events": ev}
    if labelled:
        logger.info("rules fallback labelled", data=res)
    return res


class LlmRulesFallbackJob(CollectorJob):
    """llm_rules_fallback (free, hourly)."""

    async def collect(self, run: RunContext) -> JobOutcome:
        res = await rules_fallback_cycle(run.rt.db, run.run_id, governor=run.rt.costs, now=run.started_at)
        return JobOutcome(rows=res["labelled"], note=f"mode={res['mode']} labelled={res['labelled']}", meta=res)


def _main(argv: list[str] | None = None) -> int:  # pragma: no cover - operator convenience
    import argparse
    import json

    ap = argparse.ArgumentParser(prog="python -m app.nlp.rules")
    ap.add_argument("text", help="classify one text with rules:v2 (no DB)")
    a = ap.parse_args(argv)
    q = QueueItem(post_id="cli:1", item_kind="social_post", item_id="cli:1", occupant_key=None, platform="x",
                  author_kind="other", author_key=None, kol_handle=None, text=a.text, text_hash="", event_at=0,
                  text_purge_after=None)
    print(json.dumps(RulesClassifier().classify(q, None).api_dict(), ensure_ascii=False))
    return 0


__all__ = ["RulesClassifier", "rules_fallback_cycle", "LlmRulesFallbackJob", "fallback_mode", "select_for_rules",
           "RULES_VERSION"]

if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_main())
