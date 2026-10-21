"""Spec 14 §9 fixtures: a seeded DB (SN64 healthy, SN47 flagged, SN130 pre-launch, SN9 insufficient + sticky flag)
on top of the root ``rt``/``tmp_root``/``app_client`` fixtures. Slots are anchored on yesterday so they are always in
the past; ``SCORE_FEED_TTL_SEC`` is widened per test where freshness matters."""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
import pytest_asyncio

import app.config as app_config
from app.util.time import floor_day, now_ts

SPEC = "ts-1.0"
NOW = now_ts()
D0 = floor_day(NOW) - 86400                 # yesterday 00:00Z (official)
P0 = D0 + 12 * 3600                         # yesterday 12:00Z (provisional, SN64's latest)
OCC64, OCC47, OCC130, OCC9 = "64:4123456", "47:4000001", "130:4590000", "9:3000000"


@pytest.fixture(autouse=True)
def _reset_api_state():
    from app.api import health, hub
    health._services_cache = None
    hub.reset()
    yield
    health._services_cache = None
    hub.reset()


@pytest.fixture
def cfg(tmp_root, monkeypatch):
    """Override settings on top of tmp_root's (keeps every path in tmp)."""
    def _apply(**kw):
        new = app_config.settings.model_copy(update=kw)
        monkeypatch.setattr(app_config, "settings", new)
        return new
    return _apply


async def _occ(db, key: str, netuid: int, status: str, name: str, *, current: bool = True, head: int = 7_000_000,
               **links) -> None:
    reg = int(key.split(":")[1])
    await db.execute("INSERT INTO occupants (occupant_key, netuid, network_registered_at, status, name, "
                     "first_seen_block, first_seen_at, last_seen_block, last_seen_at) VALUES (?,?,?,?,?,?,?,?,?)",
                     (key, netuid, reg, status, name, reg, NOW - 400 * 86400, head, NOW))
    if current:
        await db.execute("INSERT INTO subnets (netuid, occupant_key, network_registered_at, name, symbol, status, "
                         "immune, head_block, website_url, github_url, discord_url, x_handle, owner_coldkey, "
                         "updated_at) VALUES (?,?,?,?,?,?,0,?,?,?,?,?,?,?)",
                         (netuid, key, reg, name, f"SN{netuid}", status, head, links.get("web"), links.get("gh"),
                          links.get("discord"), links.get("x"), "5Fowner", NOW))


async def _score(db, key: str, netuid: int, as_of: int, *, comp, quality=None, velocity=None, conf=0.9, tier=None,
                 flags=(), status="active", run_kind=None, pillars=None, explain=None, run_id="sc-test") -> None:
    pillars = pillars or {"P1": 0.81, "P2": 0.9, "P3": 0.6, "P4": 0.7, "P5": 0.78, "P6": 0.62}
    run_kind = run_kind or ("official" if as_of % 86400 == 0 else "provisional")
    await db.execute(
        "INSERT INTO scores (occupant_key, netuid, as_of, spec_version, run_kind, run_id, status, quality, velocity, "
        "composite_raw, composite, composite_smoothed, pillar_scores, pillar_coverage, confidence, insufficient_data, "
        "tier, red_flags, regime, explain, computed_at, input_cutoff) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
        "?,?)",
        (key, netuid, as_of, SPEC, run_kind, run_id, status, quality, velocity,
         None if comp is None else min(100.0, comp + 0.5), comp, comp, json.dumps(pillars),
         json.dumps({k: 1.0 for k in pillars}), conf, int(conf < 0.4), tier, json.dumps(sorted(flags)), "TRENDING",
         json.dumps(explain or {}), as_of + 900, as_of + 900))


@pytest_asyncio.fixture
async def seeded(rt, cfg):
    from app.features.registry import PILLARS, scored
    cfg(SCORE_FEED_TTL_SEC=10 * 86400)
    db = rt.db
    await _occ(db, OCC64, 64, "active", "Chutes", web="https://chutes.ai", gh="https://github.com/rayonlabs/chutes",
               discord="https://discord.gg/chutes", x="chutes_ai")
    await _occ(db, OCC47, 47, "active", "Foo")
    await _occ(db, OCC130, 130, "pre_launch", "Newbie")
    await _occ(db, OCC9, 9, "active", "Baz")
    ex64 = {"top_positive": [{"metric": "emission_share", "pillar": "P1", "impact": 3.2},
                             {"metric": "gate_margin", "pillar": "P1", "impact": 2.6}],
            "top_negative": [{"metric": "discord_growth_30d", "pillar": "P5", "impact": -0.2}], "flags": []}
    for k in range(8):                                                          # 8 official rows, oldest first
        d = D0 - (7 - k) * 86400
        await _score(db, OCC64, 64, d, comp=75.0 + k, quality=78.0 + k, velocity=60.0 + k, conf=0.9, tier="B" if k < 4
                     else "A", explain=ex64)
    await _score(db, OCC64, 64, P0, comp=82.4, quality=84.1, velocity=71.0, conf=0.91, tier="A",
                 pillars={"P1": 0.81, "P2": 0.9, "P3": None, "P4": 0.7, "P5": 0.78, "P6": 0.62}, explain=ex64)
    ex47 = {"top_positive": [], "top_negative": [{"metric": "dereg_safety", "pillar": "P1", "impact": -4.0}],
            "flags": [{"flag": "dereg_imminent", "multiplier": 0.6, "since": D0 - 86400, "state": "active",
                       "origin": "auto", "human": False, "sticky": False}]}
    for d, c in ((D0 - 86400, 22.0), (D0, 18.7)):
        await _score(db, OCC47, 47, d, comp=c, quality=31.0, velocity=22.4, conf=0.55, tier="D",
                     flags=["dereg_imminent"], explain=ex47)
    await _score(db, OCC130, 130, D0, comp=None, quality=None, velocity=None, conf=0.5, tier=None, status="pre_launch")
    await _score(db, OCC9, 9, D0, comp=40.0, quality=41.0, velocity=30.0, conf=0.3, tier="C",
                 flags=["owner_control_change"])
    await db.execute("INSERT INTO red_flag_state (occupant_key, flag, spec_version, active, raised_at, human, sticky, "
                     "last_eval_as_of, updated_at) VALUES (?,?,?,1,?,0,0,?,?)",
                     (OCC47, "dereg_imminent", SPEC, D0 - 86400, D0, NOW))
    await db.execute("INSERT INTO red_flag_state (occupant_key, flag, spec_version, active, raised_at, human, sticky, "
                     "last_eval_as_of, updated_at) VALUES (?,?,?,1,?,0,1,?,?)",
                     (OCC9, "owner_control_change", SPEC, D0 - 3 * 86400, D0, NOW))
    # norm rows of SN64's latest slot: every scored metric; one absent, one failed
    keys = [d.key for p in PILLARS for d in scored(p)]
    absent_key, failed_key = keys[-3], keys[1]
    for k in keys:
        if k == absent_key:
            v, vj = 0.0, {"raw": 0, "status": "absent", "reason": "no_x_account", "raw_as_of": D0, "age_s": 43200}
        elif k == failed_key:
            v, vj = None, {"raw": None, "status": "failed", "reason": "timeout", "raw_as_of": None}
        else:
            v, vj = 0.6, {"raw": 9, "status": "ok", "raw_as_of": D0, "age_s": 43200, "src": "test"}
        await db.execute("INSERT INTO metric_values (occupant_key, metric_key, as_of, spec_version, stage, value, "
                         "value_json, observed_at, source, run_id) VALUES (?,?,?,?, 'norm', ?,?,?, 'scoring', 'sc')",
                         (OCC64, k, P0, SPEC, v, json.dumps(vj), P0 + 900))
    ev_key = keys[0]
    await db.execute("INSERT INTO metric_values (occupant_key, metric_key, as_of, spec_version, stage, value, "
                     "value_json, observed_at, source, run_id) VALUES (?,?,?,?, 'raw', 9, ?, ?, 'chain', 'r')",
                     (OCC64, ev_key, D0, SPEC, json.dumps({"status": "ok", "evidence_url": "https://example.com/ev"}),
                      D0 + 60))
    # events
    from app.storage.events import emit_event
    await emit_event(db, event_type="site_recovered", occupant_key=OCC64, netuid=64, event_at=D0, observed_at=D0,
                     payload={"url": "https://chutes.ai", "down_days": 8, "reason": "target_changed"},
                     dedupe_key="s1", source="web", run_id=None, alerted_at=D0)
    for i, et in enumerate(("tier_change", "repo_moved")):
        payload = {"netuid": 64, "spec_version": SPEC, "as_of": D0, "from": "B", "to": "A", "direction": "up",
                   "composite_smoothed": 76.8, "prev_composite_smoothed": 74.0, "composite": 76.8, "threshold": 75,
                   "top_positive": [], "top_negative": [], "red_flags": []} if et == "tier_change" else \
            {"repo_id": 1, "from": "a/b", "to": "c/d", "from_owner": "a", "to_owner": "c", "detected_at": D0}
        await emit_event(db, event_type=et, occupant_key=OCC64, netuid=64, event_at=D0 + i, observed_at=D0 + i,
                         payload=payload, dedupe_key=f"t{i}", source="scoring" if et == "tier_change" else "github",
                         run_id=None, alerted_at=D0 + 60)
    # team + product
    await db.execute("INSERT INTO team_members (occupant_key, netuid, name, name_norm, role, profile_url, evidence_url, "
                     "evidence_count, status, source, verified_by, verified_at, first_seen_at, last_checked_at, "
                     "created_by) VALUES (?,?,?,?,?,?,?,2,'verified','manual','rudolf',?,?,?,'api')",
                     (OCC64, 64, "Jon Durbin", "jon durbin", "founder", "https://x.com/jondurbin",
                      "https://chutes.ai/team", D0, D0, D0))
    await db.execute("INSERT INTO manual_notes (occupant_key, kind, value, evidence_url, created_at, created_by) "
                     "VALUES (?, 'product_status', 'live', 'https://chutes.ai/app', ?, 'telegram')", (OCC64, D0))
    # chain ticks for dereg_watch
    for n, key, risk, price in ((47, OCC47, 1, 0.001), (64, OCC64, 0, 0.5), (9, OCC9, 0, 0.002)):
        await db.execute("INSERT INTO chain_ticks (netuid, block_number, occupant_key, event_at, observed_at, source, "
                         "subnet_moving_price, deregistration_risk, immune) VALUES (?,?,?,?,?, 'sse', ?, ?, 0)",
                         (n, 9_500_000, key, NOW - 60, NOW - 60, price, risk))
    return SimpleNamespace(db=db, rt=rt, absent_key=absent_key, failed_key=failed_key, ev_key=ev_key, n_keys=len(keys))
