"""Operator CLI for team evidence, owner whitelists / key-compromise overrides and the review queue (spec 11 §2.12).

    python -m scripts.manual_entry [--db PATH] team add <netuid> --name N [--role R] [--profile URL] [--evidence URL]
                                                    [--note T] [--verify] --by WHO
    python -m scripts.manual_entry team verify <member_id> [--evidence URL …] [--note T] --by WHO
    python -m scripts.manual_entry team reject <member_id> --reason R [--evidence URL] --by WHO
    python -m scripts.manual_entry team list [<netuid>] [--status S] [--stale]
    python -m scripts.manual_entry team anon <netuid> --evidence URL [--note T] [--clear] --by WHO
    python -m scripts.manual_entry team seed [--legacy] [--dry-run]
    python -m scripts.manual_entry owner whitelist add <netuid> --reason R --applies-to A --from ISO --to ISO
                                                    --evidence URL [--change-id N] [--old-coldkey K] [--new-coldkey K]
                                                    [--note T] --by WHO
    python -m scripts.manual_entry owner whitelist list [<netuid>] | owner whitelist retire <id> --by WHO
    python -m scripts.manual_entry owner compromise <netuid> --from ISO --to ISO --evidence URL [--note T] --by WHO
    python -m scripts.manual_entry review list [--kind K]
    python -m scripts.manual_entry review resolve <id> --action A [--reason R] [--from ISO --to ISO] [--evidence URL]
                                                    [--note T] --by WHO

Prints JSON; exit 0 iff ok. Safe while the service runs (WAL, short transactions, no external calls).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m scripts.manual_entry", description="spec 11 §2.12 operator CLI")
    p.add_argument("--db", help="database path (default settings.DB_PATH); used by the verify drill on a copy")
    sub = p.add_subparsers(dest="area", required=True)

    team = sub.add_parser("team").add_subparsers(dest="cmd", required=True)
    a = team.add_parser("add")
    a.add_argument("netuid", type=int)
    a.add_argument("--name", required=True)
    a.add_argument("--role")
    a.add_argument("--profile")
    a.add_argument("--evidence")
    a.add_argument("--note")
    a.add_argument("--verify", action="store_true")
    a.add_argument("--by", required=True)
    v = team.add_parser("verify")
    v.add_argument("member_id", type=int)
    v.add_argument("--evidence", action="append", default=[])
    v.add_argument("--note")
    v.add_argument("--by", required=True)
    r = team.add_parser("reject")
    r.add_argument("member_id", type=int)
    r.add_argument("--reason", required=True)
    r.add_argument("--evidence")
    r.add_argument("--by", required=True)
    ls = team.add_parser("list")
    ls.add_argument("netuid", type=int, nargs="?")
    ls.add_argument("--status", choices=("candidate", "verified", "rejected"))
    ls.add_argument("--stale", action="store_true")
    an = team.add_parser("anon")
    an.add_argument("netuid", type=int)
    an.add_argument("--evidence", required=True)
    an.add_argument("--note")
    an.add_argument("--clear", action="store_true")
    an.add_argument("--by", required=True)
    sd = team.add_parser("seed")
    sd.add_argument("--legacy", action="store_true")
    sd.add_argument("--dry-run", action="store_true")

    owner = sub.add_parser("owner").add_subparsers(dest="cmd", required=True)
    wl = owner.add_parser("whitelist").add_subparsers(dest="wcmd", required=True)
    wa = wl.add_parser("add")
    wa.add_argument("netuid", type=int)
    wa.add_argument("--reason", required=True)
    wa.add_argument("--applies-to", required=True)
    wa.add_argument("--from", dest="frm", required=True)
    wa.add_argument("--to", required=True)
    wa.add_argument("--evidence", required=True)
    wa.add_argument("--change-id", type=int)
    wa.add_argument("--old-coldkey")
    wa.add_argument("--new-coldkey")
    wa.add_argument("--note")
    wa.add_argument("--by", required=True)
    wli = wl.add_parser("list")
    wli.add_argument("netuid", type=int, nargs="?")
    wr = wl.add_parser("retire")
    wr.add_argument("id", type=int)
    wr.add_argument("--by", required=True)
    c = owner.add_parser("compromise")
    c.add_argument("netuid", type=int)
    c.add_argument("--from", dest="frm", required=True)
    c.add_argument("--to", required=True)
    c.add_argument("--evidence", required=True)
    c.add_argument("--note")
    c.add_argument("--by", required=True)

    rev = sub.add_parser("review").add_subparsers(dest="cmd", required=True)
    rl = rev.add_parser("list")
    rl.add_argument("--kind")
    rr = rev.add_parser("resolve")
    rr.add_argument("id", type=int)
    rr.add_argument("--action", required=True,
                    choices=("confirm_flag", "whitelist", "key_compromise", "dismiss", "done"))
    rr.add_argument("--reason")
    rr.add_argument("--from", dest="frm")
    rr.add_argument("--to")
    rr.add_argument("--evidence")
    rr.add_argument("--note")
    rr.add_argument("--by", required=True)
    return p


def _out(obj: Any) -> int:
    from app.integrity.models import ActionResult
    if isinstance(obj, ActionResult):
        print(json.dumps(obj.to_dict(), default=str))
        return 0 if obj.ok else 1
    print(json.dumps(obj, indent=2, default=str))
    return 0


async def _main(a: argparse.Namespace) -> int:
    import app.config as app_config
    from app.integrity import owner_admin, review, team, team_seed
    from app.storage.db import Database
    from app.util.time import now_ts, parse_ts
    s = app_config.settings
    db = Database(a.db or s.path(s.DB_PATH))
    await db.connect()
    try:
        if a.area == "team":
            if a.cmd == "add":
                return _out(await team.add_member(db, netuid=a.netuid, name=a.name, role=a.role, profile_url=a.profile,
                                                  evidence_url=a.evidence, evidence_note=a.note, source="manual",
                                                  created_by=f"cli:{a.by}", verify=a.verify))
            if a.cmd == "verify":
                return _out(await team.verify_member(db, a.member_id, evidence_urls=a.evidence, note=a.note,
                                                     by=f"cli:{a.by}"))
            if a.cmd == "reject":
                return _out(await team.reject_member(db, a.member_id, reason=a.reason, evidence_url=a.evidence,
                                                     by=f"cli:{a.by}"))
            if a.cmd == "list":
                return _out(await team.list_members(db, netuid=a.netuid, status=a.status, stale_only=a.stale))
            if a.cmd == "anon":
                return _out(await team.set_disclosure(db, netuid=a.netuid, status="clear" if a.clear else "anonymous",
                                                      evidence_url=a.evidence, note=a.note, by=f"cli:{a.by}"))
            if a.cmd == "seed":
                now = now_ts()
                if a.legacy:
                    out = await team_seed.seed_legacy(db, s.path(s.TEAM_LEGACY_NOTES_DIR),
                                                      as_of=team_seed.legacy_as_of(), now=now, dry_run=a.dry_run)
                else:
                    out = await team_seed.seed_subnetalpha(db, now=now, dry_run=a.dry_run)
                    if not a.dry_run:
                        out.pop("candidates", None)
                return _out(out)
        if a.area == "owner":
            if a.cmd == "whitelist" and a.wcmd == "add":
                return _out(await owner_admin.add_whitelist(
                    db, netuid=a.netuid, reason=a.reason, applies_to=a.applies_to, window_start=parse_ts(a.frm),
                    window_end=parse_ts(a.to), evidence_url=a.evidence, by=f"cli:{a.by}", change_id=a.change_id,
                    old_coldkey=a.old_coldkey, new_coldkey=a.new_coldkey, note=a.note))
            if a.cmd == "whitelist" and a.wcmd == "list":
                return _out(await owner_admin.list_whitelist(db, netuid=a.netuid))
            if a.cmd == "whitelist" and a.wcmd == "retire":
                return _out(await owner_admin.retire_whitelist(db, a.id, by=f"cli:{a.by}"))
            if a.cmd == "compromise":
                return _out(await owner_admin.override_key_compromise(
                    db, netuid=a.netuid, window_start=parse_ts(a.frm), window_end=parse_ts(a.to),
                    evidence_url=a.evidence, by=f"cli:{a.by}", note=a.note))
        if a.area == "review":
            if a.cmd == "list":
                return _out(await review.list_open(db, kind=a.kind))
            if a.cmd == "resolve":
                window = (parse_ts(a.frm), parse_ts(a.to)) if a.frm and a.to else None
                return _out(await review.resolve_review(db, a.id, a.action, by=f"cli:{a.by}", note=a.note,
                                                        evidence_url=a.evidence, whitelist_reason=a.reason,
                                                        window=window))
        print("unknown command", file=sys.stderr)
        return 2
    finally:
        await db.close()


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_main(_parser().parse_args(argv)))


if __name__ == "__main__":
    raise SystemExit(main())
