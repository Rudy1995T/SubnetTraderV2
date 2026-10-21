"""Evaluation harness (spec 10 §9.3): ``python -m app.nlp.eval run|bootstrap|drift``.

``run`` scores a classifier (``rules`` offline; ``bulk``/``escalation`` need ``LLM_EVAL_ALLOW_NETWORK=1`` and a key) on
the golden set: event_type P/R/F1 per class + macro-F1 (over gold classes), polarity accuracy and confusion, direction
accuracy, materiality MAE/exact, entity P/R (micro + per netuid), spam/promo P/R; results and per-item predictions go
to ``data/eval/{classifier_version}.json``. ``drift`` compares two saved results (per-class deltas, agreement on
non-``other`` items; acceptance ≥ 0.85). ``bootstrap`` pre-labels sampled real items with the bulk model
(``labeller='llm-prelabel'``) for the operator to correct.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

import app.config as app_config
from app.nlp.queue import QueueItem
from app.nlp.rubric import RULES_VERSION, classifier_version, load_rubric, rubric_sha256
from app.nlp.schema import EVENT_TYPES, LabelV1

DEFAULT_GOLDEN = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "golden_labels_v1.jsonl"
TARGETS = {"llm": {"macro_f1": 0.70, "polarity_accuracy": 0.80, "entity_precision": 0.90, "spam_precision": 0.85},
           "rules": {"macro_f1": 0.45, "entity_precision": 0.90}}
AGREEMENT_MIN = 0.85


def load_golden(path: str | Path, *, reviewed_only: bool = False) -> list[dict]:
    out = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        g = json.loads(line)
        if reviewed_only and g.get("labeller") == "llm-prelabel":
            continue
        out.append(g)
    return out


def golden_item(g: dict) -> QueueItem:
    sn = g.get("source_netuid")
    occ = f"{int(sn)}:0" if sn not in (None, "") else None
    plat = g.get("platform") or "x"
    return QueueItem(post_id=str(g["id"]), item_kind=g.get("item_kind") or "social_post", item_id=str(g["id"]),
                     occupant_key=occ, platform=plat, author_kind=g.get("author_kind") or "other", author_key=None,
                     kol_handle=None, text=g["text"], text_hash=str(g["id"]), event_at=int(g.get("event_at") or 0),
                     text_purge_after=None)


def predicted_netuids(label: LabelV1, item: QueueItem, min_conf: float = 0.6) -> set[int]:
    s = {int(e.netuid) for e in label.entities if e.confidence >= min_conf}
    if item.source_netuid is not None:
        s.add(item.source_netuid)
    return s


def _prf(tp: int, fp: int, fn: int) -> dict[str, float]:
    p = tp / (tp + fp) if tp + fp else 0.0
    r = tp / (tp + fn) if tp + fn else 0.0
    f = 2 * p * r / (p + r) if p + r else 0.0
    return {"precision": round(p, 4), "recall": round(r, 4), "f1": round(f, 4), "support": tp + fn}


def score(golden: list[dict], preds: dict[str, dict]) -> dict:
    """``preds[id] = {polarity, event_type, direction, materiality, entities:[netuid], is_spam, is_promo}``."""
    rows = [(g, preds[str(g["id"])]) for g in golden if str(g["id"]) in preds]
    n = len(rows)
    out: dict[str, Any] = {"n": n}
    if not n:
        return out
    tp: Counter = Counter()
    fp: Counter = Counter()
    fn: Counter = Counter()
    for g, p in rows:
        ge, pe = g["gold"]["event_type"], p["event_type"]
        if ge == pe:
            tp[ge] += 1
        else:
            fp[pe] += 1
            fn[ge] += 1
    gold_classes = sorted({g["gold"]["event_type"] for g, _ in rows})
    per = {c: _prf(tp[c], fp[c], fn[c]) for c in sorted(set(gold_classes) | set(fp))}
    out["event_type"] = per
    out["macro_f1"] = round(sum(per[c]["f1"] for c in gold_classes) / len(gold_classes), 4)
    conf = Counter((g["gold"]["polarity"], p["polarity"]) for g, p in rows)
    out["polarity_accuracy"] = round(sum(v for (a, b), v in conf.items() if a == b) / n, 4)
    out["polarity_confusion"] = {f"{a}->{b}": v for (a, b), v in sorted(conf.items())}
    out["bullish_bearish_swaps"] = conf[("bullish", "bearish")] + conf[("bearish", "bullish")]
    out["direction_accuracy"] = round(sum(1 for g, p in rows if g["gold"]["direction"] == p["direction"]) / n, 4)
    out["materiality_mae"] = round(sum(abs(g["gold"]["materiality"] - p["materiality"]) for g, p in rows) / n, 4)
    out["materiality_exact"] = round(sum(1 for g, p in rows if g["gold"]["materiality"] == p["materiality"]) / n, 4)
    etp = efp = efn = 0
    per_n: dict[int, Counter] = defaultdict(Counter)
    for g, p in rows:
        gs, ps = set(map(int, g["gold"]["entities"])), set(map(int, p["entities"]))
        etp += len(gs & ps)
        efp += len(ps - gs)
        efn += len(gs - ps)
        for x in gs & ps:
            per_n[x]["tp"] += 1
        for x in ps - gs:
            per_n[x]["fp"] += 1
        for x in gs - ps:
            per_n[x]["fn"] += 1
    ent = _prf(etp, efp, efn)
    out["entity_precision"], out["entity_recall"] = ent["precision"], ent["recall"]
    out["entity_per_netuid"] = {str(k): _prf(v["tp"], v["fp"], v["fn"]) for k, v in sorted(per_n.items())}
    for flag in ("is_spam", "is_promo"):
        t_ = sum(1 for g, p in rows if g["gold"][flag] and p[flag])
        f_ = sum(1 for g, p in rows if not g["gold"][flag] and p[flag])
        m_ = sum(1 for g, p in rows if g["gold"][flag] and not p[flag])
        r = _prf(t_, f_, m_)
        key = flag[3:]
        out[f"{key}_precision"], out[f"{key}_recall"] = r["precision"], r["recall"]
    return out


def pred_dict(label: LabelV1, item: QueueItem) -> dict:
    return {"polarity": label.polarity, "event_type": label.event_type, "direction": int(label.direction),
            "materiality": int(label.materiality), "entities": sorted(predicted_netuids(label, item)),
            "is_spam": bool(label.is_spam), "is_promo": bool(label.is_promo)}


def predict_rules(golden: list[dict], index: Any = None) -> dict[str, dict]:
    from app.nlp.rules import RulesClassifier
    clf = RulesClassifier()
    out = {}
    for g in golden:
        it = golden_item(g)
        out[str(g["id"])] = pred_dict(clf.classify(it, index), it)
    return out


async def predict_llm(golden: list[dict], *, tier: str) -> tuple[dict[str, dict], dict]:  # pragma: no cover - network
    """Direct (non-batch) calls of the bulk/escalation model on the golden items; requires the network flag."""
    from app.nlp.llm_classify import (bulk_params, make_client, message_text, system_blocks, usage_of, usage_usd)
    from app.nlp.queue import render_items_block, render_single
    from app.nlp.schema import RequestRejected, json_schema_single, parse_batch_items, parse_single
    from app.ratelimit import ProviderLimiter
    s = app_config.settings
    client = make_client(s)
    limiter = ProviderLimiter.from_settings()
    rubric = load_rubric()
    system = system_blocks(rubric, "")
    items = [golden_item(g) for g in golden]
    preds: dict[str, dict] = {}
    usage = {"in": 0, "out": 0, "usd": 0.0}
    if tier == "bulk":
        for i in range(0, len(items), 20):
            chunk = sorted(items[i:i + 20], key=lambda q: q.post_id)
            await limiter.acquire("anthropic")
            msg = await client.messages.create(**bulk_params(s.LLM_BULK_MODEL, system, render_items_block(chunk)))
            u = usage_of(msg)
            usage["in"] += u["in"]
            usage["out"] += u["out"]
            usage["usd"] += usage_usd(s.LLM_BULK_MODEL, u, batch=False)
            try:
                ok, _ = parse_batch_items(message_text(msg))
            except RequestRejected:
                continue
            for _raw, lab in ok:
                if lab.id.isdigit() and 1 <= int(lab.id) <= len(chunk):
                    it = chunk[int(lab.id) - 1]
                    preds[it.post_id] = pred_dict(lab, it)
    else:
        for it in items:
            await limiter.acquire("anthropic")
            msg = await client.messages.create(
                model=s.LLM_ESCALATION_MODEL, max_tokens=4096, system=system,
                messages=[{"role": "user", "content": render_single(it)}],
                output_config={"effort": "medium", "format": {"type": "json_schema", "schema": json_schema_single()}})
            u = usage_of(msg)
            usage["in"] += u["in"]
            usage["out"] += u["out"]
            usage["usd"] += usage_usd(s.LLM_ESCALATION_MODEL, u, batch=False)
            try:
                preds[it.post_id] = pred_dict(parse_single(message_text(msg)), it)
            except RequestRejected:
                continue
    return preds, usage


def results_dir() -> Path:
    s = app_config.settings
    return Path(s.path(s.DATA_DIR)) / "eval"


def save_results(version: str, metrics: dict, preds: dict[str, dict], *, out_dir: Path | None = None) -> Path:
    d = out_dir or results_dir()
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{version.replace(':', '_')}.json"
    p.write_text(json.dumps({"classifier_version": version, "metrics": metrics, "predictions": preds}, indent=1,
                            sort_keys=True), encoding="utf-8")
    return p


def drift(old: dict, new: dict) -> dict:
    """Per-class F1 deltas and the label agreement rate on items where either side is not ``other``."""
    po, pn = old.get("predictions", {}), new.get("predictions", {})
    common = sorted(set(po) & set(pn))
    rel = [k for k in common if po[k]["event_type"] != "other" or pn[k]["event_type"] != "other"]
    agree = sum(1 for k in rel if po[k]["event_type"] == pn[k]["event_type"] and po[k]["direction"] ==
                pn[k]["direction"]) / len(rel) if rel else 1.0
    eo, en = old.get("metrics", {}).get("event_type", {}), new.get("metrics", {}).get("event_type", {})
    deltas = {c: round(en.get(c, {}).get("f1", 0.0) - eo.get(c, {}).get("f1", 0.0), 4) for c in EVENT_TYPES
              if c in eo or c in en}
    return {"n_common": len(common), "n_non_other": len(rel), "agreement": round(agree, 4),
            "accepted": agree >= AGREEMENT_MIN, "f1_deltas": deltas}


def _print(d: dict) -> None:
    print(json.dumps(d, indent=1, sort_keys=True))


def run(golden_path: str | Path, classifier: str, *, reviewed_only: bool = False, index: Any = None,
        out_dir: Path | None = None) -> dict:
    s = app_config.settings
    golden = load_golden(golden_path, reviewed_only=reviewed_only)
    usage = None
    if classifier == "rules":
        version = RULES_VERSION
        preds = predict_rules(golden, index)
    elif classifier in ("bulk", "escalation"):
        if not s.LLM_EVAL_ALLOW_NETWORK or not s.ANTHROPIC_API_KEY:
            raise SystemExit("LLM eval needs LLM_EVAL_ALLOW_NETWORK=1 and ANTHROPIC_API_KEY")
        sha = rubric_sha256(load_rubric())
        model = s.LLM_BULK_MODEL if classifier == "bulk" else s.LLM_ESCALATION_MODEL
        version = classifier_version(model, sha, s.LLM_SCHEMA_V)
        preds, usage = asyncio.run(predict_llm(golden, tier=classifier))
    else:
        raise SystemExit(f"unknown classifier {classifier!r}")
    m = score(golden, preds)
    if usage:
        m["usage"] = usage
    m["targets"] = TARGETS["rules" if classifier == "rules" else "llm"]
    m["path"] = str(save_results(version, m, preds, out_dir=out_dir))
    m["classifier_version"] = version
    return m


async def bootstrap(n: int, out_path: Path) -> int:  # pragma: no cover - network + live DB
    """Sample ``n`` recent real items, pre-label them with the bulk model, append with ``labeller='llm-prelabel'``."""
    import random

    from app.nlp.queue import _candidates
    from app.storage.db import Database
    from app.util.time import now_ts, to_iso
    s = app_config.settings
    if not s.LLM_EVAL_ALLOW_NETWORK or not s.ANTHROPIC_API_KEY:
        raise SystemExit("bootstrap needs LLM_EVAL_ALLOW_NETWORK=1 and ANTHROPIC_API_KEY")
    db = Database(s.path(s.DB_PATH))
    await db.connect()
    try:
        cands = await _candidates(db, exclude_versions=("__none__",), min_event_at=now_ts() - 30 * 86400,
                                  max_chars=int(s.LLM_MAX_ITEM_CHARS))
    finally:
        await db.close()
    pick = random.Random(7).sample(cands, min(n, len(cands)))
    golden = [{"id": q.post_id, "item_kind": q.item_kind, "platform": q.platform, "author_kind": q.author_kind,
               "source_netuid": q.source_netuid, "event_at": q.event_at, "text": q.text} for q in pick]
    preds, _ = await predict_llm([{**g, "gold": {}} for g in golden], tier="bulk")
    now = to_iso(now_ts())
    with out_path.open("a", encoding="utf-8") as f:
        for g in golden:
            p = preds.get(str(g["id"]))
            if not p:
                continue
            f.write(json.dumps({**g, "gold": p, "labeller": "llm-prelabel", "labelled_at": now,
                                "note": "pre-labelled; operator to correct"}, ensure_ascii=False) + "\n")
    return len(golden)


def main(argv: Iterable[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m app.nlp.eval")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--golden", default=str(DEFAULT_GOLDEN))
    r.add_argument("--classifier", choices=("rules", "bulk", "escalation"), default="rules")
    r.add_argument("--reviewed-only", action="store_true")
    b = sub.add_parser("bootstrap")
    b.add_argument("--n", type=int, default=160)
    b.add_argument("--out", default=str(DEFAULT_GOLDEN))
    d = sub.add_parser("drift")
    d.add_argument("--old", required=True)
    d.add_argument("--new", required=True)
    a = ap.parse_args(list(argv) if argv is not None else None)
    if a.cmd == "run":
        _print(run(a.golden, a.classifier, reviewed_only=a.reviewed_only))
    elif a.cmd == "bootstrap":
        print(asyncio.run(bootstrap(a.n, Path(a.out))))
    else:
        rd = results_dir()
        old = json.loads((rd / f"{a.old.replace(':', '_')}.json").read_text())
        new = json.loads((rd / f"{a.new.replace(':', '_')}.json").read_text())
        _print(drift(old, new))
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
