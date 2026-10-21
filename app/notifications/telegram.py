"""Telegram transport (spec 01 §2.11) and the long-poll command bot (spec 14 §8.4, ``telegram_bot`` LongTask).

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


# ── long-poll command bot (spec 14 §8.4; ported from the old bot's TelegramBot) ─────────────────────────────────
def allowed_chat_ids() -> set[str]:
    s = app_config.settings
    ids = {x.strip() for x in (s.TELEGRAM_ALLOWED_CHAT_IDS or "").split(",") if x.strip()}
    return ids or ({str(s.TELEGRAM_CHAT_ID)} if s.TELEGRAM_CHAT_ID else set())


def _btn(text: str, data: str) -> dict:
    return {"text": text, "callback_data": data}


HOME = ("🏠 Home", "act:home")


def _grid(buttons: list[dict], per_row: int) -> list[list[dict]]:
    return [buttons[i:i + per_row] for i in range(0, len(buttons), per_row)]


def _kb(*rows: list[dict]) -> dict:
    return {"inline_keyboard": [r for r in rows if r]}


def main_menu_keyboard() -> dict:
    """The home screen's keyboard (14 §8.4: every screen leads back here through 🏠 Home)."""
    return _kb([_btn("🏆 Top", "act:top"), _btn("🚩 Flags", "act:flags"), _btn("📈 Movers", "act:movers")],
               [_btn("👛 Wallets", "w:l"), _btn("🩺 Health", "act:health"), _btn("🗂 Reviews", "act:review")],
               [_btn("💸 Spend", "act:spend"), _btn("🧭 To do", "act:needs"), _btn("📬 Digest", "act:digest")])


def back_keyboard(*extra: tuple[str, str]) -> dict:
    """``[extra…] [🔁 Refresh?] [🏠 Home]`` — one row; ``extra`` are (label, callback_data) pairs."""
    return _kb([_btn(t, d) for t, d in extra] + [_btn(*HOME)])


def sn_buttons(netuids: list[int], per_row: int = 5, limit: int = 10) -> list[list[dict]]:
    return _grid([_btn(f"SN{n}", f"sn:{n}") for n in netuids[:limit]], per_row)


def top_keyboard(netuids: list[int], *, everything: bool = False, offer_all: bool = False) -> dict:
    refresh = "act:top:all" if everything else "act:top"
    extra = [_btn("👀 Show anyway", "act:top:all")] if offer_all else []
    return _kb(*sn_buttons(netuids), extra, [_btn("🔁 Refresh", refresh), _btn(*HOME)])


def sn_keyboard(netuid: int) -> dict:
    return _kb([_btn("📜 Events", f"ev:{netuid}"), _btn("🔁 Refresh", f"sn:{netuid}")],
               [_btn("🏆 Top", "act:top"), _btn(*HOME)])


def flags_keyboard(netuids: list[int]) -> dict:
    return _kb(*sn_buttons(netuids), [_btn("🔁 Refresh", "act:flags"), _btn(*HOME)])


def movers_keyboard(days: int, netuids: list[int]) -> dict:
    periods = [_btn(("✓ " if d == days else "") + f"{d}d", f"act:mv:{d}") for d in (1, 7, 30)]
    return _kb(*sn_buttons(netuids), periods, [_btn(*HOME)])


def health_keyboard(*, all_jobs: bool = False) -> dict:
    toggle = _btn("🩺 Summary", "act:health") if all_jobs else _btn("📋 All jobs", "act:jobs")
    return _kb([toggle, _btn("🔁 Refresh", "act:jobs" if all_jobs else "act:health")], [_btn(*HOME)])


def confirm_keyboard(yes_data: str, no_data: str = "no:x") -> dict:
    return {"inline_keyboard": [[_btn("✅ Confirm", yes_data), _btn("✖ Cancel", no_data)]]}


# ── wallet view (14 §8.4a): ``w:<view>:<arg>:<wallet>``; the wallet (name, else coldkey) goes last ────────────
def wallet_ref(w: dict) -> str:
    """Name when it fits the 64-byte callback budget, else the coldkey (the terminal resolves either)."""
    name = str(w.get("name") or "")
    return name if name and len(name.encode()) <= 40 and ":" not in name else str(w.get("coldkey") or "all")


def wcb(view: str, ref: str, arg: object = "") -> str:
    return f"w:{view}:{arg}:{ref}"


def wallets_keyboard(items: list[tuple[str, str]]) -> dict:
    """``items`` = (button label, wallet ref)."""
    rows = [[_btn(label, wcb("o", ref))] for label, ref in items]
    return _kb(*rows, [_btn("👥 All wallets", wcb("o", "all"))] if len(items) > 1 else [],
               [_btn("🔁 Refresh", "w:l"), _btn(*HOME)])


def wallet_keyboard(ref: str) -> dict:
    return _kb([_btn("📂 Positions", wcb("p", ref)), _btn("📊 Performance", wcb("f", ref, 30))],
               [_btn("💡 Insights", wcb("i", ref, 0)), _btn("🧾 Trades", wcb("t", ref, 0))],
               [_btn("🔁 Refresh", wcb("o", ref)), _btn("👛 Wallets", "w:l"), _btn(*HOME)])


def wallet_sub_keyboard(ref: str, *, refresh: str, top: list[list[dict]] = ()) -> dict:
    """A wallet sub-screen: optional rows, then ``[⬅️ Wallet] [🔁 Refresh] [🏠 Home]``."""
    return _kb(*top, [_btn("⬅️ Wallet", wcb("o", ref)), _btn("🔁 Refresh", refresh), _btn(*HOME)])


def positions_keyboard(ref: str, netuids: list[int]) -> dict:
    return wallet_sub_keyboard(ref, refresh=wcb("p", ref),
                               top=_grid([_btn(f"SN{n}", wcb("s", ref, n)) for n in netuids[:15]], 5))


def position_keyboard(ref: str, netuid: int) -> dict:
    return _kb([_btn("📂 Positions", wcb("p", ref)), _btn(f"🛰 SN{netuid} score", f"sn:{netuid}")],
               [_btn("🔁 Refresh", wcb("s", ref, netuid)), _btn(*HOME)])


def performance_keyboard(ref: str, period: int) -> dict:
    labels = {1: "1D", 7: "1W", 30: "1M", 365: "1Y"}
    row = [_btn(("✓ " if p == period else "") + lab, wcb("f", ref, p)) for p, lab in labels.items()]
    return wallet_sub_keyboard(ref, refresh=wcb("f", ref, period), top=[row])


def pager_keyboard(view: str, ref: str, page: int, pages: int, *, newer_left: bool = False) -> dict:
    nav = []
    if page > 0:
        nav.append(_btn("◀ Newer" if newer_left else "◀ Prev", wcb(view, ref, page - 1)))
    if pages:
        nav.append(_btn(f"{page + 1}/{pages}", wcb(view, ref, page)))
    if page + 1 < pages:
        nav.append(_btn("Older ▶" if newer_left else "Next ▶", wcb(view, ref, page + 1)))
    return wallet_sub_keyboard(ref, refresh=wcb(view, ref, page), top=[nav])


def _toast(text: str) -> str:
    """Collapse an HTML message into a short single-line callback toast."""
    plain = re.sub(r"<[^>]+>", "", text)
    plain = " ".join(plain.split())
    return plain[:190]


class TelegramBot:
    """Long-polling command bot (port of the old ``TelegramBot``; HTTP through ``Http.request("telegram", …)``)."""

    def __init__(self, dispatch, *, token: str | None = None, poll_timeout_sec: int | None = None,
                 retry_delay_sec: float = 3.0, on_poll=None, transport=None) -> None:
        s = app_config.settings
        self._dispatch = dispatch                    # object with on_command(text, chat_id) / on_callback(...)
        self._token = token if token is not None else s.TELEGRAM_BOT_TOKEN
        self._poll_timeout_sec = int(poll_timeout_sec if poll_timeout_sec is not None else s.TELEGRAM_POLL_TIMEOUT_SEC)
        self._retry_delay_sec = retry_delay_sec
        self._offset: int | None = None
        self._on_poll = on_poll
        self._transport = transport                  # tests: async (method, payload, timeout) -> dict

    @property
    def enabled(self) -> bool:
        return bool(app_config.settings.TELEGRAM_ENABLED and self._token and allowed_chat_ids())

    def _redact(self, text: str) -> str:
        """Strip the bot token from text before logging (it appears in API URLs)."""
        return text.replace(self._token, "***") if self._token else text

    async def run(self, stop_event) -> None:
        """Poll until ``stop_event`` is set (old ``run``)."""
        import asyncio
        if not self.enabled:
            return
        await self._set_my_commands()
        await self._discard_pending_updates()
        logger.info("Telegram command bot polling started")
        try:
            while not stop_event.is_set():
                try:
                    updates = await self._fetch_updates(timeout=self._poll_timeout_sec)
                    if self._on_poll:
                        self._on_poll()
                    for update in updates:
                        await self._handle_update(update)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if self._on_poll:
                        self._on_poll()
                    logger.warning("Telegram polling error", data={"error": self._redact(str(exc))[:300]})
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=self._retry_delay_sec)
                    except asyncio.TimeoutError:
                        pass
        finally:
            logger.info("Telegram command bot stopped")

    # ── Bot API calls ──────────────────────────────────────────────────────────────────────────────────────
    async def _post(self, method: str, payload: dict, *, timeout: float = 15.0) -> dict:
        if self._transport is not None:
            return await self._transport(method, payload, timeout)
        from app.http import get_http
        resp = await get_http().request("telegram", "POST", self._url(method), json=payload, timeout=timeout,
                                        retries=0, ok_statuses=(200, 400, 403, 409))
        body = resp.json() if resp.content else {}
        if not body.get("ok"):
            raise RuntimeError(self._redact(f"Telegram API error {resp.status_code}: {str(body)[:300]}"))
        return body

    def _url(self, method: str) -> str:
        return f"https://api.telegram.org/bot{self._token}/{method}"

    async def send_message(self, chat_id: str, text: str, reply_markup: dict | None = None) -> None:
        payload: dict = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        try:
            await self._post("sendMessage", payload)
        except Exception as exc:
            if "can't parse entities" in str(exc) or " 400" in str(exc):
                payload.pop("parse_mode", None)
                payload["text"] = strip_html(text)
                await self._post("sendMessage", payload)
            else:
                raise

    async def edit_message(self, chat_id: str, message_id: int, text: str, reply_markup: dict | None = None) -> None:
        payload: dict = {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML",
                         "disable_web_page_preview": True}
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        try:
            await self._post("editMessageText", payload)
        except Exception as exc:
            # "message is not modified" (e.g. Refresh with no change) is benign.
            logger.debug("Telegram editMessageText failed", data={"error": self._redact(str(exc))[:200]})

    async def answer_callback(self, callback_id: str, text: str = "") -> None:
        if not callback_id:
            return
        payload: dict = {"callback_query_id": callback_id}
        if text:
            payload["text"] = text
        try:
            await self._post("answerCallbackQuery", payload)
        except Exception as exc:
            logger.debug("Telegram answerCallbackQuery failed", data={"error": self._redact(str(exc))[:200]})

    async def _discard_pending_updates(self) -> None:
        """Skip stale commands so a restart does not replay old chat actions."""
        for _ in range(50):
            if not await self._fetch_updates(timeout=0):
                return

    async def _fetch_updates(self, *, timeout: int) -> list[dict]:
        import json as _json
        payload: dict = {"timeout": timeout, "allowed_updates": _json.dumps(["message", "callback_query"])}
        if self._offset is not None:
            payload["offset"] = self._offset
        body = await self._post("getUpdates", payload, timeout=timeout + 10)
        updates = body.get("result", []) or []
        if updates:
            self._offset = max(int(u["update_id"]) for u in updates) + 1
        return updates

    async def _set_my_commands(self) -> None:
        from app.notifications.commands import BOT_COMMANDS
        try:
            await self._post("setMyCommands", {"commands": [{"command": c, "description": d}
                                                            for c, d in BOT_COMMANDS]})
        except Exception as exc:
            logger.debug("Telegram setMyCommands failed", data={"error": self._redact(str(exc))[:200]})

    # ── updates ────────────────────────────────────────────────────────────────────────────────────────────
    async def _handle_update(self, update: dict) -> None:
        if "callback_query" in update:
            await self._handle_callback(update["callback_query"])
            return
        message = update.get("message") or {}
        text = (message.get("text") or "").strip()
        if not text:
            return
        chat_id = str(message.get("chat", {}).get("id", ""))
        if chat_id not in allowed_chat_ids():
            logger.warning("Ignoring Telegram message from unauthorized chat", data={"chat_id": chat_id})
            return
        if not text.startswith("/"):
            return
        await self._run_safely(chat_id, None, self._dispatch.on_command(self, chat_id, text))

    async def _handle_callback(self, callback: dict) -> None:
        message = callback.get("message") or {}
        chat_id = str(message.get("chat", {}).get("id", ""))
        callback_id = str(callback.get("id", ""))
        if chat_id not in allowed_chat_ids():
            logger.warning("Ignoring Telegram callback from unauthorized chat", data={"chat_id": chat_id})
            await self.answer_callback(callback_id)
            return
        data = str(callback.get("data") or "")
        logger.info("Telegram button tapped", data={"data": data})
        await self._run_safely(chat_id, callback_id,
                               self._dispatch.on_callback(self, chat_id, message.get("message_id"), callback_id, data))

    async def _run_safely(self, chat_id: str, callback_id: str | None, coro) -> None:
        """A handler exception never kills the loop: the chat gets ``⚠️ command failed: <exc class>``."""
        try:
            await coro
        except Exception as exc:
            logger.error("Telegram handler failed", data={"error": self._redact(f"{type(exc).__name__}: {exc}")[:300]})
            try:
                if callback_id:
                    await self.answer_callback(callback_id, f"command failed: {type(exc).__name__}")
                await self.send_message(chat_id, f"⚠️ command failed: {type(exc).__name__}")
            except Exception:  # pragma: no cover
                pass

    async def emit(self, chat_id: str, text: str, keyboard: dict | None, message_id: int | None) -> None:
        """Edit the message in place for a button tap, else send new message(s) (chunked; keyboard on the first)."""
        from app.notifications.format import chunk
        parts = chunk(text) or [""]
        if message_id is not None and len(parts) == 1:
            await self.edit_message(chat_id, message_id, parts[0], reply_markup=keyboard)
            return
        for i, part in enumerate(parts):
            await self.send_message(chat_id, part, reply_markup=keyboard if i == 0 else None)
