"""Spec 10 §9 test_rubric + the §9.3 guard: the shipped rubric's hash, CRLF invariance, version strings, and the
rubric hash ↔ rubric_hashes.json ↔ LABEL_POLICIES pairing (an accidental rubric edit must fail here)."""
from __future__ import annotations

import json
from pathlib import Path

from app.nlp import rubric as R
from app.scoring.spec import LABEL_POLICIES, TS_1_0, spec_hash
from app.scoring.golden import GOLDEN_SPEC_HASH

ROOT = Path(__file__).resolve().parents[2]
HASHES = json.loads((ROOT / "tests" / "fixtures" / "rubric_hashes.json").read_text())


def test_shipped_rubric_hash_matches_fixture():
    text = R.load_rubric(ROOT / "app" / "nlp" / "prompts" / "classify_v1.md")
    assert R.rubric_sha256(text) == HASHES["app/nlp/prompts/classify_v1.md"]


def test_crlf_and_trailing_ws_same_hash(tmp_path):
    raw = (ROOT / "app" / "nlp" / "prompts" / "classify_v1.md").read_text(encoding="utf-8")
    p = tmp_path / "r.md"
    p.write_bytes(("  \r\n".join(raw.split("\n")) + "\r\n\r\n").encode("utf-8"))
    assert R.rubric_sha256(R.load_rubric(p)) == HASHES["app/nlp/prompts/classify_v1.md"]
    assert R.rubric_sha256(raw + " changed") != HASHES["app/nlp/prompts/classify_v1.md"]


def test_classifier_version_and_policy_pairing():
    sha = HASHES["app/nlp/prompts/classify_v1.md"]
    assert R.classifier_version("claude-haiku-5-5", sha, 1) == "claude-haiku-5-5:" + sha[:8] + ":1"
    pol = LABEL_POLICIES["ts-1.0"]
    want = HASHES["label_policy_ts-1.0"]
    assert list(pol.bulk) == want["bulk"] and list(pol.escalation) == want["escalation"]
    assert list(pol.fallback) == want["fallback"] == [R.RULES_VERSION]
    v = R.current_versions()
    assert v["bulk"] in pol.bulk and v["escalation"] in pol.escalation
    assert R.BULK_EFFORT == "low"


def test_rubric_token_estimate_and_golden_spec_hash_untouched():
    assert R.token_estimate(R.load_rubric()) >= 600
    assert spec_hash(TS_1_0) == GOLDEN_SPEC_HASH["ts-1.0"]          # LABEL_POLICIES is not a ScoringSpec field


def test_print_version_cli(capsys):
    assert R.main(["--print-version"]) == 0
    out = capsys.readouterr().out.splitlines()
    assert out[0] == "claude-haiku-5-5:e47d012b:1" and "pinned_in_ts-1.0=yes" in out[-1]
