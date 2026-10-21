"""One-shot recorder for the spec 09 §9.1 provider fixtures (§8.3 step 13, §9.2).

    python -m scripts.record_x_fixtures --provider twitterapi_io|official_v2 [--handle opentensor]
                                        [--query '"Chutes" OR SN64'] [--out tests/fixtures/x] [--yes]

Makes ≈ 5 paid requests (≈ $0.002 on twitterapi.io, ≈ $0.05 on official v2) through 01's ``Http`` (so the requests
are counted) and books their USD with the governor. Each response body is saved as ``{out}/{provider}/{name}.json``
with ``"_recorded_at"`` added; API keys, bearer tokens and the ``X-API-Key`` value are redacted from everything written
(bodies never carry them, but request echoes and error messages might). Recorded fixtures replace the synthetic ones
(``"_synthetic": true``); re-run the provider tests afterwards and fix ``FIELD_MAP`` if a field was renamed.
It needs a key, so it was never run at build time (2026-10-18).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PLAN = {
    "twitterapi_io": [
        ("user_info", "/twitter/user/info", lambda a, uid: {"userName": a.handle}),
        ("last_tweets_p1", "/twitter/user/last_tweets", lambda a, uid: {"userId": uid, "includeReplies": "true"}),
        ("advanced_search", "/twitter/tweet/advanced_search", lambda a, uid: {
            "query": f"({a.query}) -filter:retweets since:"
                     f"{(datetime.now(timezone.utc) - timedelta(days=1)).strftime('%Y-%m-%d_%H:%M:%S_UTC')}",
            "queryType": "Latest"}),
    ],
    "official_v2": [
        ("users_by_username", "/users/by/username/{handle}", lambda a, uid: {
            "user.fields": "created_at,public_metrics,verified,username"}),
        ("users_tweets_p1", "/users/{uid}/tweets", lambda a, uid: {
            "max_results": 5, "tweet.fields": "created_at,public_metrics,entities,lang,referenced_tweets,"
            "in_reply_to_user_id,conversation_id,attachments,author_id", "expansions": "referenced_tweets.id"}),
        ("search_recent", "/tweets/search/recent", lambda a, uid: {
            "query": f"({a.query}) -is:retweet", "max_results": 10, "sort_order": "recency",
            "tweet.fields": "created_at,public_metrics,entities,lang,referenced_tweets,author_id",
            "expansions": "author_id", "user.fields": "created_at,public_metrics,username,verified"}),
        ("not_found", "/users/by/username/{missing}", lambda a, uid: {"user.fields": "created_at"}),
    ],
}


def redact(obj: Any, secrets: list[str]) -> Any:
    if isinstance(obj, dict):
        return {k: ("<redacted>" if k.lower() in ("x-api-key", "authorization", "api_key", "token") else
                    redact(v, secrets)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [redact(v, secrets) for v in obj]
    if isinstance(obj, str):
        for s in secrets:
            if s:
                obj = obj.replace(s, "<redacted>")
        return re.sub(r"Bearer\s+[A-Za-z0-9%._~+/=-]+", "Bearer <redacted>", obj)
    return obj


async def record(args: argparse.Namespace) -> int:
    import app.config as app_config
    from app.costs import CostGovernor
    from app.http import Http
    from app.ratelimit import ProviderLimiter
    from app.storage.db import Database
    s = app_config.settings
    prov = args.provider
    key = s.TWITTERAPI_IO_KEY if prov == "twitterapi_io" else s.X_BEARER_TOKEN
    if not key:
        print(f"no key for {prov}; nothing recorded", file=sys.stderr)
        return 1
    base = (s.TWITTERAPI_IO_BASE_URL if prov == "twitterapi_io" else s.X_OFFICIAL_BASE_URL).rstrip("/")
    out = Path(args.out) / prov
    out.mkdir(parents=True, exist_ok=True)
    db = Database(s.path(s.DB_PATH))
    await db.connect()
    await db.apply_schema()
    limiter = ProviderLimiter.from_settings()
    costs = CostGovernor(db, limiter=limiter)
    await costs.load()
    http = Http(limiter, costs)
    uid = None
    try:
        for name, path, params in PLAN[prov]:
            url = base + path.format(handle=args.handle, uid=uid or "", missing="zz_no_such_user_09")
            d = await costs.check(prov, 0.01)
            if not d.allowed:
                print(f"governor denied ({d.reason}); stopping", file=sys.stderr)
                break
            r = await http.request(prov, "GET", url, params=params(args, uid), ok_statuses=(200, 400, 402, 404))
            body = r.json() if r.content else {}
            if uid is None and isinstance(body, dict) and isinstance(body.get("data"), dict):
                uid = str(body["data"].get("id") or "") or None
            body = redact(body, [key])
            if isinstance(body, dict):
                body["_recorded_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                body["_http_status"] = r.status_code
            (out / f"{name}.json").write_text(json.dumps(body, indent=1, ensure_ascii=False), encoding="utf-8")
            await costs.record(prov, 0.0005 if prov == "twitterapi_io" else 0.01, requests=0, job="record_x_fixtures")
            print(f"saved {name} (HTTP {r.status_code})")
        await costs.flush()
    finally:
        await http.aclose()
        await db.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m scripts.record_x_fixtures")
    ap.add_argument("--provider", required=True, choices=sorted(PLAN))
    ap.add_argument("--handle", default="opentensor")
    ap.add_argument("--query", default='"Chutes" OR SN64')
    ap.add_argument("--out", default="tests/fixtures/x")
    ap.add_argument("--yes", action="store_true", help="confirm the paid requests")
    args = ap.parse_args(argv)
    if not args.yes:
        print("this makes paid X requests; re-run with --yes", file=sys.stderr)
        return 2
    return asyncio.run(record(args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
