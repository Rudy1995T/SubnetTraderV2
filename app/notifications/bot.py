"""``telegram_bot`` LongTask (spec 14 §6.1, §8.4): long-polls ``getUpdates`` and answers the operator's commands.

``ops_exempt`` and not pausable (02 §B12), not liveness-gated (16 §2.5). Without a token / allowed chat it idles
(re-checking every 60 s, so a token added via ``reload-config`` is picked up) instead of spinning the reconnect loop.
"""
from __future__ import annotations

import asyncio

from app.jobs import LongTask
from app.notifications.commands import build_dispatcher
from app.notifications.telegram import TelegramBot


class TelegramBotTask(LongTask):
    async def run_once(self, stop: asyncio.Event) -> None:
        bot = TelegramBot(build_dispatcher(lambda: self.rt), on_poll=self.beat)
        self.attempt()
        if not bot.enabled:
            self.mark_connected(False)
            try:
                await asyncio.wait_for(stop.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass
            return
        self.mark_connected(True)
        try:
            await bot.run(stop)
        finally:
            self.mark_connected(False)
