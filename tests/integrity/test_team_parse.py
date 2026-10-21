"""Spec 11 §9 T1–T7: the subnetalpha team_info parser and the two-source rule (pure)."""
from __future__ import annotations

from app.features.text import _ANON_TOKENS, _looks_anonymous, looks_anonymous
from app.integrity.team_parse import (check_two_source, classify_profile_url, is_plausible_name, name_norm,
                                      parse_team_info, registrable_domain, split_name_role)

APEX = "".join(f'<p><strong>{n}</strong> – {r} <a href="https://www.linkedin.com/in/{h}/">LinkedIn</a></p>'
               for n, r, h in [("Will Squires", "CEO", "willsquires"), ("Steffen Cruz", "CTO", "steffencruz"),
                               ("Elena Nesterova", "Head of Research", "elena"), ("Alex Williams", "Engineer", "alexw")])


def test_parse_strong_pairs():                                                    # T1
    p = parse_team_info(APEX)
    assert p.disclosure == "named"
    assert [c.name for c in p.candidates] == ["Will Squires", "Steffen Cruz", "Elena Nesterova", "Alex Williams"]
    assert p.candidates[0].role == "CEO" and p.candidates[2].role == "Head of Research"
    assert all(c.profile_kind == "linkedin" and c.profile_url.startswith("https://www.linkedin.com/in/")
               for c in p.candidates)


def test_parse_role_first():                                                      # T2
    p = parse_team_info("<p><strong>Founder:</strong> Jane Doe, <strong>CTO:</strong> John Roe</p>")
    assert [(c.name, c.role) for c in p.candidates] == [("Jane Doe", "Founder"), ("John Roe", "CTO")]


def test_parse_llm_prose_headings():                                              # T3
    html = ('<p><strong data-start="12" class="text-token-text-primary">Public Team Members and Handles:</strong></p>'
            '<p><strong>GitHub Contributors and Activity:</strong> lots of commits</p>'
            '<p><strong>Subnet ownership</strong> Subnet 3 now carries the identity</p>'
            '<p>Lead dev <a href="https://x.com/jondurbin">@jondurbin</a></p>')
    p = parse_team_info(html)
    assert [c.name for c in p.candidates] == ["jondurbin"]
    assert p.candidates[0].profile_kind == "x"


def test_parse_kept_private():                                                    # T4
    p = parse_team_info("<p>The core team behind ORO is kept private.</p>")
    assert p.disclosure == "anonymous" and not p.candidates


def test_parse_placeholders():                                                    # T5
    assert parse_team_info("Awaiting Data...").disclosure == "placeholder"
    assert parse_team_info("<p>Awating Data...</p>").disclosure == "placeholder"
    assert parse_team_info("").disclosure == "empty"
    assert parse_team_info("No team members have been publicly disclosed").disclosure == "anonymous"


def test_parse_anon_length_gate():                                                # T6
    prose = ("<p>" + "The project builds tooling and none of the prior launches went unknown to the market. " * 18
             + "</p>" + "".join(f"<p><strong>{n}</strong> – Engineer</p>" for n in ("Ann Lee", "Bob Ray", "Cy Tan")))
    p = parse_team_info(prose)
    assert len(p.candidates) == 3 and p.disclosure == "named" and p.anon_marker is False
    assert parse_team_info("<p>Team: anonymous</p>").disclosure == "anonymous"


def test_name_rules_and_split():
    assert not is_plausible_name("Founder") and not is_plausible_name("Rebranding and Evolution")
    assert not is_plausible_name("subnet82.com") and not is_plausible_name("Subnet ownership")
    assert is_plausible_name("Tom A. Lynch") and is_plausible_name("Will O'Brien")
    assert split_name_role("Xavier Lyu, Founder") == ("Xavier Lyu", "Founder")
    assert split_name_role("Founder/CEO – Bob Wold") == ("Bob Wold", "Founder/CEO")
    assert name_norm("@José  Núñez") == "jose nunez"
    assert classify_profile_url("https://twitter.com/home") == "website"
    assert registrable_domain("https://www.foo.co.uk/x") == "foo.co.uk"
    assert registrable_domain("https://a.substack.com/p/1") == "a.substack.com"


def test_anon_tokens_port_verbatim():
    assert _ANON_TOKENS == ("anonymous", "anon", "unknown", "undisclosed", "tbd", "n/a", "none")
    assert _looks_anonymous("Team: anon") and looks_anonymous is _looks_anonymous
    assert not _looks_anonymous("Anonymity research lab")


def test_two_source_rule():                                                       # T7
    li = "https://www.linkedin.com/in/jane"
    kw = dict(org_handles={"macrocosmos-ai"}, x_handles={"macrocosmosai"}, excluded={"subnetalpha.ai"},
              min_sources=2)
    r = check_two_source(li, [li], **kw)
    assert not r.ok and r.evidence_count == 1
    r = check_two_source(li, ["https://x.com/janedoe"], **kw)
    assert not r.ok and r.reason == "no_attestation"
    r = check_two_source(li, ["https://macrocosmos.ai/team"], **kw)
    assert r.ok and r.evidence_count == 2
    assert not check_two_source(li, ["https://subnetalpha.ai/subnet/apex/"], **kw).ok
    assert check_two_source(None, ["https://chutes.ai/about", "https://www.theblock.co/post/1"], **kw).ok
    r = check_two_source(li, ["https://x.com/macrocosmosai/status/1"], **kw)
    assert r.ok                                          # the subnet's own X account counts as attestation
    r = check_two_source(li, [], attestation_notes=["confirmed on the team's Discord AMA 2026-09-20, role CTO"], **kw)
    assert r.ok and r.evidence_count == 2
    assert any(i["url"] is None and i["class"] == "attestation" for i in r.items)
    r = check_two_source(None, [], attestation_notes=["confirmed on the team's Discord AMA, role CTO"], **kw)
    assert not r.ok and r.reason == "no_public_url"
