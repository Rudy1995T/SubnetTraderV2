"""Spec 12 §2.14 golden fixture + small builders shared by the scoring tests (synthetic values, not live claims)."""
from __future__ import annotations

import json
import math

from app.storage.events import emit_event
from app.storage.pit import put_metric_values

DAY = 86400
SLOT = 1790121600                      # 2026-09-23T00:00Z (official)
T0 = 1790122504                        # computed_at (00:15:04Z, strict)
SN64 = "64:4213977"
TAIL = "200:6970000"
ROOT = "0:0"
OBS = SLOT + 300                       # producers' observed_at (00:05Z)


async def add_occupant(db, okey: str, *, status: str = "active", activated_at: int | None = 1700000000,
                       first_seen_at: int = 1700000000, deregistered_at: int | None = None,
                       first_emission_block: int | None = None) -> None:
    netuid, reg = (int(x) for x in okey.split(":"))
    await db.execute(
        "INSERT INTO occupants (occupant_key, netuid, network_registered_at, status, first_seen_block, first_seen_at, "
        "last_seen_block, last_seen_at, activated_at, deregistered_at, first_emission_block) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (okey, netuid, reg, status, reg, first_seen_at, reg, first_seen_at, activated_at, deregistered_at,
         first_emission_block))


def raw(okey: str, key: str, value, status: str = "ok", *, as_of: int = SLOT, observed_at: int = OBS,
        backfilled: int = 0, **vj) -> dict:
    from app.storage.catalog import METRIC_KEYS
    body = {"status": status, **vj}
    if status != "ok" and "reason" not in body:
        body["reason"] = "fixture"
    if status in ("failed", "not_applicable"):
        value = None
    return {"occupant_key": okey, "metric_key": key, "as_of": as_of, "spec_version": "ts-1.0", "stage": "raw",
            "value": value, "value_json": body, "observed_at": observed_at, "source": METRIC_KEYS[key].source,
            "run_id": "fixture", "backfilled": backfilled}


async def choppy_chain_hourly(db, slot: int, n: int = 12, a: float = 0.0045, *, observed_at: int | None = None) -> None:
    """``n`` subnets alternating ±a every 4 h over the 24 h before ``slot`` → CHOPPY (vol ≈ .2, dir ≈ a, disp 0)."""
    rows = []
    for netuid in range(1, n + 1):
        for h in range(24):
            ts = slot - 86400 + (h + 1) * 3600
            k = (ts - (slot - 86400)) // (4 * 3600)
            price = math.exp(a * (k % 2))
            rows.append((netuid, ts, f"{netuid}:1", ts, observed_at if observed_at is not None else ts, "sse", 1, price))
    await db.executemany("INSERT INTO chain_hourly (netuid, hour_ts, occupant_key, event_at, observed_at, source, "
                         "is_active, price) VALUES (?,?,?,?,?,?,?,?)", rows)


async def prev_score(db, okey: str, as_of: int, *, smoothed=None, tier=None, composite=None, insufficient=0,
                     confidence=0.9, spec="ts-1.0") -> None:
    netuid = int(okey.split(":")[0])
    await db.execute(
        "INSERT INTO scores (occupant_key, netuid, as_of, spec_version, run_kind, run_id, status, composite, "
        "composite_smoothed, pillar_scores, pillar_coverage, confidence, insufficient_data, tier, red_flags, explain, "
        "computed_at, input_cutoff) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (okey, netuid, as_of, spec, "official" if as_of % DAY == 0 else "provisional", "prev", "active", composite,
         smoothed, "{}", "{}", confidence, insufficient, tier, "[]", "{}", as_of + 900, as_of + 900))


def _spread(lo: float, hi: float, k: int) -> list[float]:
    return [lo + (hi - lo) * (i + 1) / (k + 1) for i in range(k)]


async def build_golden(db) -> None:
    """The §2.14 worked example: SN64 + TAIL + 120 synthetic active occupants + root, official slot 2026-09-23."""
    await add_occupant(db, ROOT)
    await add_occupant(db, SN64)
    await add_occupant(db, TAIL)
    synth = [f"{n}:{1000 + n}" for n in range(1, 122) if n != 64]
    assert len(synth) == 120
    for o in synth:
        await add_occupant(db, o)
    rows: list[dict] = []
    # P1 cross-sectional cohorts (n = 122): emission 37 zeros incl. TAIL, SN64 r0 = 120
    em = [0.0] * 36 + _spread(0.01, 9.0, 83) + [12.0]
    liq = _spread(1.0, 4.0, 4) + _spread(10.0, 900.0, 114) + [5000.0, 6000.0]       # TAIL r0 = 4, SN64 r0 = 119
    cb = [0.0] * 40 + _spread(0.01, 0.5, 58) + _spread(2.0, 9.0, 22)                 # 41 zeros, SN64 r0 = 99
    for o, e, l_, c in zip(synth, em, liq, cb):
        rows += [raw(o, "emission_share", e), raw(o, "liquidity_depth", l_), raw(o, "chain_buys", c)]
    rows.append(raw(ROOT, "emission_share", 50.0))
    # x_engagement_30d cohort n = 58: SN64 r0 = 56 (57 others: 56 below, 1 above)
    for i, o in enumerate(synth[:57]):
        rows.append(raw(o, "x_engagement_30d", 100.0 + i if i < 56 else 9999.0))
    # previous official rows for the synthetic occupants (insufficient → no insufficient_data transition event)
    for o in synth:
        await prev_score(db, o, SLOT - DAY, insufficient=1, confidence=0.1)

    def p1(o, es, gm, ds, lq, ec, nf, cbv, hd, mb):
        out = [raw(o, "emission_share", es), raw(o, "gate_margin", gm), raw(o, "dereg_safety", ds),
               raw(o, "liquidity_depth", lq), raw(o, "exit_capacity", ec), raw(o, "net_flow_30d", nf),
               raw(o, "chain_buys", cbv), raw(o, "holder_dispersion", hd),
               raw(o, "participation", None, "failed", reason="no_metagraph_source")]
        out.append(raw(o, "miner_burn_ok", mb) if mb is not None else
                   raw(o, "miner_burn_ok", None, "failed", reason="deep_tail"))
        return out
    rows += p1(SN64, 9.649, 12.18, 107 / 109, 4000.0, 0.359, 0.04, 1.0, 0.55, 0.98)
    rows += p1(TAIL, 0.0, 0.12, 0.0, 5.0, 0.41, -0.20, 0.0, 0.14, None)
    # P2
    rows += [raw(SN64, "active_authors_28d", 14), raw(SN64, "fte_devs_84d", 5), raw(SN64, "commit_days_90d", 84),
             raw(SN64, "declared_repo_freshness", 1), raw(SN64, "org_freshness", 0),
             raw(SN64, "release_cadence_90d", 4), raw(SN64, "review_practice_90d", 0.62), raw(SN64, "bus_factor", 3),
             raw(SN64, "repo_hygiene", 0.8), raw(SN64, "originality", 0.97)]
    rows += [raw(TAIL, "active_authors_28d", 0), raw(TAIL, "fte_devs_84d", 0), raw(TAIL, "commit_days_90d", 0),
             raw(TAIL, "declared_repo_freshness", 214), raw(TAIL, "org_freshness", 214),
             raw(TAIL, "release_cadence_90d", 0),
             raw(TAIL, "review_practice_90d", None, "not_applicable", reason="no_merged_prs"),
             raw(TAIL, "bus_factor", 0), raw(TAIL, "repo_hygiene", 0.40), raw(TAIL, "originality", 0.35)]
    # P3
    rows += [raw(SN64, "site_uptime_30d", 29 / 30), raw(SN64, "tls_ok", 1), raw(SN64, "site_age", 700),
             raw(SN64, "site_freshness", 12), raw(SN64, "docs_present", 1),
             raw(SN64, "product_status", 1.0, product_status="live")]
    rows += [raw(TAIL, "site_uptime_30d", 0.60), raw(TAIL, "tls_ok", 1), raw(TAIL, "site_age", 410),
             raw(TAIL, "site_freshness", 300), raw(TAIL, "docs_present", 0),
             raw(TAIL, "product_status", None, "failed", reason="unset")]
    # P4 (spec 11's final values; tenure = raw days)
    rows += [raw(SN64, "team_doxxed", 1.0, n_verified=2, n_named=3, disclosure="named"),
             raw(SN64, "owner_identity", 1.0), raw(SN64, "owner_commitment", 0.66, lock_ratio=0.85, auto_lock=False,
                                                    commit_pct=0.225, perpetual_locked=1e5, age_days=600),
             raw(SN64, "owner_retention_30d", 0.90, sell_ratio=0.10), raw(SN64, "control_stability_90d", 1.0, events=[]),
             raw(SN64, "owner_overhang", 1.0, dump_impact=0.03, legacy_unaccounted=False), raw(SN64, "tenure", 600)]
    rows += [raw(TAIL, "team_doxxed", 0.0, "absent", reason="anonymous"),
             raw(TAIL, "owner_identity", 0.0, "absent", reason="no_identity"),
             raw(TAIL, "owner_commitment", 0.0, lock_ratio=0.0, auto_lock=False, perpetual_locked=0, age_days=300),
             raw(TAIL, "owner_retention_30d", 0.20, sell_ratio=0.80), raw(TAIL, "control_stability_90d", 1.0),
             raw(TAIL, "owner_overhang", 0.35, dump_impact=0.18, legacy_unaccounted=False), raw(TAIL, "tenure", 300)]
    # P5 (08 fn rows + 09 X rows as their producers would write them; derived keys come from derive())
    rows += [raw(SN64, "venue_reachable", 1.0), raw(SN64, "discord_members", 9000),
             raw(SN64, "presence_ratio", 0.12, members=9000), raw(SN64, "discord_growth_30d", 0.04),
             raw(SN64, "x_engagement_30d", 9000.0), raw(SN64, "x_followers", 42000),
             raw(SN64, "x_cadence_30d", 18), raw(SN64, "discord_announcements_30d", 4),
             raw(SN64, "tg_posts_30d", None, "failed", reason="no_public_channel"),
             raw(SN64, "x_last_activity_days", 0.5), raw(SN64, "discord_last_activity_days", 2.0),
             raw(SN64, "tg_last_activity_days", None, "failed", reason="no_public_channel")]
    rows += [raw(TAIL, "venue_reachable", 0.0, "absent", reason="invite_invalid"),
             raw(TAIL, "discord_members", None, "failed", reason="invite_invalid"),
             raw(TAIL, "presence_ratio", None, "failed", reason="invite_invalid"),
             raw(TAIL, "discord_growth_30d", None, "failed", reason="invite_invalid"),
             raw(TAIL, "x_followers", 0.0, "absent", reason="no_x_account"),
             raw(TAIL, "x_cadence_30d", 0.0, "absent", reason="no_x_account"),
             raw(TAIL, "x_last_activity_days", 0.0, "absent", reason="no_x_account"),
             raw(TAIL, "discord_announcements_30d", None, "failed", reason="no_followed_channel"),
             raw(TAIL, "discord_last_activity_days", None, "failed", reason="no_followed_channel")]
    # P6
    rows += [raw(SN64, "event_impact_14d", 2 * 0.5 ** (3 / 7) + 2 * 0.5 ** (9 / 7)), raw(SN64, "polarity_7d", 0.64),
             raw(SN64, "attention_7d", 0.8), raw(SN64, "kol_coverage_30d", 3)]
    rows += [raw(TAIL, "event_impact_14d", 0.0), raw(TAIL, "polarity_7d", None, "failed", reason="lt_5_posts"),
             raw(TAIL, "attention_7d", -0.5), raw(TAIL, "kol_coverage_30d", None, "failed", reason="provider_null")]
    # aux flag inputs
    for o, risk in ((SN64, 0.0), (TAIL, 1.0)):
        rows += [raw(o, "emission_enabled_flag", 1.0), raw(o, "dereg_risk_flag", risk), raw(o, "immune_flag", 0.0),
                 raw(o, "owner_extraction_trigger", 0.0, reasons=[], trigger_ids=[]),
                 raw(o, "owner_control_change_trigger", 0.0, kind=None, trigger_ids=[], identity_churn=False)]
    await put_metric_values(db, rows)
    # history: prev official scores, the dereg_imminent state row, one identity_changed(subnet_name) 40 d ago
    await prev_score(db, SN64, SLOT - DAY, smoothed=80.10, tier="A", composite=80.0)
    await prev_score(db, TAIL, SLOT - DAY, smoothed=31.40, tier="D", composite=19.02)
    await db.execute(
        "INSERT INTO red_flag_state (occupant_key, flag, spec_version, active, raised_at, last_true_at, false_since, "
        "human, sticky, last_eval, last_eval_as_of, detail_json, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (TAIL, "dereg_imminent", "ts-1.0", 1, SLOT - 2 * DAY, SLOT - DAY + 18 * 3600, None, 0, 0, "true",
         SLOT - DAY + 18 * 3600, json.dumps({"dereg_risk_flag": 1.0, "immune_flag": 0.0}), SLOT - DAY + 18 * 3600))
    t = SLOT - 40 * DAY
    await emit_event(db, event_type="identity_changed", occupant_key=TAIL, netuid=200, event_at=t, observed_at=t,
                     payload={"netuid": 200, "occupant_key": TAIL, "source": "chain", "field": "subnet_name",
                              "kind": "changed", "old_value": "Old", "new_value": "New", "old_hash": "a",
                              "new_hash": "b", "old_observed_at": t - DAY, "resolved_changed": True},
                     dedupe_key=f"{TAIL}|chain|subnet_name|b", source="registry", run_id="fixture")
    await choppy_chain_hourly(db, SLOT)
