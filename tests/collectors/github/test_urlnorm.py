"""Spec 06 §2.2 / §9: URL normaliser (pure)."""
import pytest

from app.collectors.github.urlnorm import _name_token, first_ref, normalise

CASES = [
    # (raw, kind, owner, repo, placeholder)
    ("https://github.com/orgs/Beam-Network/repositories", "owner", "Beam-Network", None, False),
    ("https://github.com/orgs/ditto-assistant/repositories", "owner", "ditto-assistant", None, False),
    ("https://github.com/orgs/foo/projects", "owner", "foo", None, False),
    ("https://github.com/users/alice", "owner", "alice", None, False),
    ("https://github.com/ditto-assistant/", "owner", "ditto-assistant", None, False),
    ("https://github.com/BitMind-AI", "owner", "BitMind-AI", None, False),
    ("https://github.com/rayonlabs/chutes.git", "repo", "rayonlabs", "chutes", False),
    ("https://github.com/macrocosm-os/subnet?tab=readme-ov-file", "repo", "macrocosm-os", "subnet", False),
    ("https://github.com/CortexLM/platform?tab=readme-ov-file", "repo", "CortexLM", "platform", False),
    ("https://github.com/o/repo-x#readme", "repo", "o", "repo-x", False),
    ("git@github.com:o/r.git", "repo", "o", "r", False),
    ("www.github.com/o/r/tree/main/x", "repo", "o", "r", False),
    ("github.com/sportstensor/sn41", "repo", "sportstensor", "sn41", False),
    ("http://github.com/a/b", "repo", "a", "b", False),
    ("https://github.com/a/b/blob/main/README.md", "repo", "a", "b", False),
    ("https://github.com/a/b/releases", "repo", "a", "b", False),
    ("https://github.com/a/b/issues/12", "repo", "a", "b", False),
    ("https://github.com/a/b/wiki", "repo", "a", "b", False),
    ("https://github.com/a/b/pulls", "repo", "a", "b", False),
    ("https://github.com/attelierai/attelierai_subnet/", "repo", "attelierai", "attelierai_subnet", False),
    ("https://github.com/witnessvision/witness_subnet/", "repo", "witnessvision", "witness_subnet", False),
    ("https://o.github.io", "owner", "o", None, False),
    ("https://myorg.github.io/docs/page", "owner", "myorg", None, False),
    ("https://api.github.com/repos/a/b", "repo", "a", "b", False),
    ("https://raw.githubusercontent.com/a/b/main/x.py", "repo", "a", "b", False),
    ("https://github.com/username/repo", "repo", "username", "repo", True),
    ("https://github.com/deprecated/deprecated", "repo", "deprecated", "deprecated", True),
    ("https://github.com/owner/repo", "repo", "owner", "repo", True),
    ("https://github.com/acme/coming-soon", "repo", "acme", "coming-soon", True),
    ("https://github.com/yourname", "owner", "yourname", None, True),
    ("https://github.com/acme/TBD", "repo", "acme", "TBD", True),
    ("https://GitHub.com/Foo/Bar", "repo", "Foo", "Bar", False),
    ("https://github.com/a/b.", "repo", "a", "b", False),
    ("[repo](https://github.com/a/b)", "repo", "a", "b", False),
    ("Code: https://github.com/a/b, docs elsewhere", "repo", "a", "b", False),
    ("https://github.com/a-b-c/x.y_z", "repo", "a-b-c", "x.y_z", False),
]


@pytest.mark.parametrize("raw,kind,owner,repo,ph", CASES)
def test_forms(raw, kind, owner, repo, ph):
    refs = normalise(raw, extra_denylist="")
    assert len(refs) == 1, refs
    r = refs[0]
    assert (r.kind, r.owner, r.repo, r.placeholder) == (kind, owner, repo, ph)
    assert r.key == (f"{owner.lower()}/{repo.lower()}" if repo else owner.lower())


@pytest.mark.parametrize("raw", [
    "", None, "https://example.com/a/b", "https://github.com/topics/bittensor", "https://github.com/sponsors/x",
    "https://github.com/-foo/bar", "https://github.com/", "https://github.com/settings/tokens",
    "https://github.com/marketplace/x", "https://notgithub.org/a/b", "~", "unconst", "https://github.com/a/b~c",
    "https://gitlab.com/a/b",
])
def test_no_ref(raw):
    assert normalise(raw, extra_denylist="") == []


def test_two_urls_in_one_string_dedupe_and_order():
    refs = normalise("https://github.com/a/b https://github.com/c, https://github.com/A/B", extra_denylist="")
    assert [r.key for r in refs] == ["a/b", "c"]


def test_first_ref_prefers_repo_over_owner():
    assert first_ref("https://github.com/org, https://github.com/org/main", extra_denylist="").key == "org/main"
    assert first_ref("https://github.com/org", extra_denylist="").kind == "owner"


def test_extra_denylist_honoured():
    assert normalise("https://github.com/acme/demo", extra_denylist="acme/demo")[0].placeholder
    assert normalise("https://github.com/Acme", extra_denylist=" acme ,x/y")[0].placeholder
    assert not normalise("https://github.com/acme/demo", extra_denylist="")[0].placeholder


def test_extra_denylist_from_settings(settings_override):
    settings_override(GITHUB_EXTRA_DENYLIST="foo/bar")
    assert normalise("https://github.com/foo/bar")[0].placeholder


def test_owner_case_kept_key_lowered():
    r = normalise("https://github.com/Desearch-ai/subnet-22", extra_denylist="")[0]
    assert r.owner == "Desearch-ai" and r.key == "desearch-ai/subnet-22" and r.full_name == "Desearch-ai/subnet-22"


def test_name_token():
    assert _name_token("Chutes") == "chutes"
    assert _name_token("SN 1 Apex") == "apex"
    assert _name_token("ab") is None and _name_token(None) is None
