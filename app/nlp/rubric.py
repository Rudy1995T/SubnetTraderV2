"""Rubric loading, hashing and the ``classifier_version`` rule (spec 10 §2.2, §2.3, §2.4).

``classifier_version = f"{model}:{rubric_sha256[:8]}:{schema_v}"``; ``rules:v2`` for the rules fallback. The rubric hash
is over the file with CRLF→LF and trailing whitespace stripped per line (and trailing blank lines dropped), so an
editor's line-ending change is not a relabel. ``BULK_EFFORT`` is frozen here, not config: changing it means bumping
``LLM_SCHEMA_V``.

CLI: ``python -m app.nlp.rubric --print-version`` → the bulk/escalation versions, the sha and the token estimate.
"""
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

import app.config as app_config

RULES_VERSION = "rules:v2"     # v1 (2026-10-18 build) wrote 72 live labels before the security/ambiguous-name fixes
BULK_EFFORT = "low"                 # §2.3: Haiku 5.5 thinks adaptively; "low" bounds thinking on bulk classification
ESCALATION_EFFORT = "medium"
NARRATIVE_EFFORT = "medium"
BULK_MAX_TOKENS = 8192
ESCALATION_MAX_TOKENS = 4096
NARRATIVE_MAX_TOKENS = 8000
NARRATIVE_PROMPT = Path(__file__).with_name("prompts") / "narrative_v1.md"


def normalise(text: str) -> str:
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    return "\n".join(ln.rstrip() for ln in lines).rstrip("\n")


def _resolve(path: str | Path) -> Path:
    p = Path(path)
    if p.is_absolute() or p.exists():
        return p
    return Path(__file__).resolve().parents[2] / p                     # repo-relative (LLM_RUBRIC_PATH default)


def load_rubric(path: str | Path | None = None) -> str:
    """The normalised rubric text (what is sent and what is hashed)."""
    p = _resolve(path or app_config.settings.LLM_RUBRIC_PATH)
    return normalise(p.read_text(encoding="utf-8"))


def rubric_sha256(text: str) -> str:
    return hashlib.sha256(normalise(text).encode("utf-8")).hexdigest()


def classifier_version(model_id: str, rubric_sha: str, schema_v: int) -> str:
    return f"{model_id}:{rubric_sha[:8]}:{int(schema_v)}"


def token_estimate(text: str) -> int:
    return int(len(text) * float(app_config.settings.LLM_TOKENS_PER_CHAR))


def current_versions() -> dict[str, str]:
    """{rubric_sha256, bulk, escalation} for the configured models, rubric and schema."""
    s = app_config.settings
    sha = rubric_sha256(load_rubric())
    return {"rubric_sha256": sha, "bulk": classifier_version(s.LLM_BULK_MODEL, sha, s.LLM_SCHEMA_V),
            "escalation": classifier_version(s.LLM_ESCALATION_MODEL, sha, s.LLM_SCHEMA_V)}


def load_narrative_prompt() -> str:
    return normalise(NARRATIVE_PROMPT.read_text(encoding="utf-8"))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m app.nlp.rubric")
    ap.add_argument("--print-version", action="store_true")
    a = ap.parse_args(argv)
    v = current_versions()
    text = load_rubric()
    if a.print_version:
        print(v["bulk"])
    print(f"escalation={v['escalation']} rubric_sha256={v['rubric_sha256']} token_estimate={token_estimate(text)}")
    try:
        from app.scoring.spec import LABEL_POLICIES
        pol = LABEL_POLICIES.get(app_config.settings.SPEC_VERSION)
        pinned = bool(pol and v["bulk"] in pol.bulk)
    except Exception:  # pragma: no cover
        pinned = False
    print(f"pinned_in_{app_config.settings.SPEC_VERSION}={'yes' if pinned else 'NO'}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
