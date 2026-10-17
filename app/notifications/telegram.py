"""Telegram transport (spec 01 §2.11). The long-poll command bot (``telegram_bot`` LongTask) is spec 14 / Phase 11.

``send_alert`` returns True iff Telegram answered 200; it is a no-op (False) when ``TELEGRAM_ENABLED`` is false or the
token/chat id is empty. Every call goes through ``Http.request("telegram", …)``; the token is never logged.
"""
from __future__ import annotations

import re
from typing import Any

import app.config as app_config
from app.logging.logger import logger

_TAG = re.compile(r"<[^>]+>")


def telegram_configured() -> bool:
    return app_config.settings.telegram_configured()


def strip_html(text: str) -> str:
    import html
    return html.unescape(_TAG.sub("", text))


async def send_message(text: str, *, parse_mode: str | None = "HTML", reply_markup: Any = None,
                       chat_id: str | None = None) -> tuple[bool, int | None]:
    """Send one message. Returns (ok, http_status); status None means a network error / 5xx after the attempt."""
    s = app_config.settings
    if not telegram_configured():
        return False, None
    from app.http import ProviderError, get_http

    body: dict[str, Any] = {"chat_id": chat_id or s.TELEGRAM_CHAT_ID, "text": text,
                            "disable_web_page_preview": True}
    if parse_mode:
        body["parse_mode"] = parse_mode
    if reply_markup is not None:
        body["reply_markup"] = reply_markup
    try:
        resp = await get_http().request(
            "telegram", "POST", f"https://api.telegram.org/bot{s.TELEGRAM_BOT_TOKEN}/sendMessage", json=body,
            timeout=5, retries=0, ok_statuses=(200, 400, 403, 429))
    except ProviderError as exc:
        logger.debug("telegram send failed", data={"error": str(exc).replace(s.TELEGRAM_BOT_TOKEN, "***"),
                                                   "status": exc.status})
        return False, exc.status
    except Exception as exc:  # BudgetExhausted etc. — never raise into callers
        logger.debug("telegram send failed", data={"error": type(exc).__name__})
        return False, None
    if resp.status_code != 200:
        logger.debug("telegram send rejected", data={"status": resp.status_code, "body": resp.text[:300]})
    return resp.status_code == 200, resp.status_code


async def send_alert(text: str, *, reply_markup: Any = None) -> bool:
    ok, _ = await send_message(text, reply_markup=reply_markup)
    return ok


async def get_me() -> dict:
    s = app_config.settings
    from app.http import get_http
    resp = await get_http().request("telegram", "GET", f"https://api.telegram.org/bot{s.TELEGRAM_BOT_TOKEN}/getMe",
                                    timeout=5, retries=0, ok_statuses=(200, 401, 404))
    return resp.json() if resp.content else {}
