"""Spec 14 §9 Telegram tests: reply formats (phone layout, §2.7/§8.5 amended 2026-10-20), navigation, the wallet view
(§8.4a), confirmations, pause two-step, chat allowlist, chunking, containment, token redaction. The Bot API is a fake
transport recording ``(method, payload)``; the τ Terminal is a fake ``terminal_get``."""
from __future__ import annotations

import pytest

from app.notifications import wallet as W
from app.notifications.commands import CommandDispatcher, render_sn, render_top
from app.notifications.format import PRE_WIDTH, utf16_len
from app.notifications.telegram import TelegramBot


pytestmark = pytest.mark.asyncio
CHAT = "42"
TOKEN = "123456:SECRETTOKEN"


class Fake:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.updates: list[list[dict]] = []

    async def __call__(self, method: str, payload: dict, timeout: float) -> dict:
        import asyncio
        self.calls.append((method, payload))
        await asyncio.sleep(0.001)                    # a real transport yields; never starve the loop
        if method == "getUpdates":
            return {"ok": True, "result": self.updates.pop(0) if self.updates else []}
        return {"ok": True, "result": {}}

    def sent(self) -> list[dict]:
        return [p for m, p in self.calls if m in ("sendMessage", "editMessageText")]


@pytest.fixture
def bot(seeded, cfg):
    cfg(TELEGRAM_BOT_TOKEN=TOKEN, TELEGRAM_CHAT_ID=CHAT, TELEGRAM_ALLOWED_CHAT_IDS="")
    fake = Fake()
    disp = CommandDispatcher(lambda: seeded.rt)
    b = TelegramBot(disp, transport=fake)
    b.fake = fake  # type: ignore[attr-defined]
    b.disp = disp  # type: ignore[attr-defined]
    return b


def msg(text: str, chat: str = CHAT) -> dict:
    return {"update_id": 1, "message": {"text": text, "chat": {"id": int(chat)}}}


def cb(data: str, chat: str = CHAT) -> dict:
    return {"update_id": 2, "callback_query": {"id": "cbid", "data": data,
                                               "message": {"message_id": 7, "chat": {"id": int(chat)}}}}


async def test_top_format(bot, seeded):
    await bot._handle_update(msg("/top 50"))
    out = bot.fake.sent()
    assert out and all(utf16_len(p["text"]) <= 4096 for p in out)
    text = out[0]["text"]
    assert "<pre>" in text and " #  SN name        T  comp conf" in text and "1 hidden (confidence &lt; 0.4)" in text
    rows = [ln for ln in text.split("\n") if ln[:3].strip().isdigit()]
    assert rows[0].split()[1] == "64" and rows[1].split()[1] == "47" and rows[1].rstrip().endswith("!")
    assert "🟢 A 1" in text and "🟠 D 1" in text
    kb = out[0]["reply_markup"]["inline_keyboard"]
    assert kb[0][0]["callback_data"] == "sn:64" and kb[-1][-1]["callback_data"] == "act:home"
    text2, nets, offer = await render_top(seeded.db, 10, "A")
    assert nets == [64] and offer is False
    text3, nets3, offer3 = await render_top(seeded.db, 10, "C")             # only a low-confidence row
    assert nets3 == [] and offer3 and "None of the 1 scored subnets reaches confidence 0.4" in text3
    text4, nets4, _ = await render_top(seeded.db, 10, "C", everything=True)
    assert nets4 == [9] and "incl. low-confidence rows" in text4


async def test_sn_card(bot, seeded):
    text = await render_sn(seeded.db, 64)
    assert "Chain     ████████░░ .81" in text and "Product   ░░░░░░░░░░ n/a" in text
    assert "👍 emission_share +3.2" in text and "👎 discord_growth_30d -0.2" in text
    assert "🟢 <b>82.4</b> composite · tier <b>A</b>" in text
    pre_end = text.index("</pre>")
    assert text.index('<a href="https://chutes.ai">web</a>') > pre_end           # links outside <pre>
    assert "/ui/subnet/64" in text
    await bot._handle_update(msg("/sn 64"))
    kb = bot.fake.sent()[-1]["reply_markup"]["inline_keyboard"]
    assert [[b["callback_data"] for b in row] for row in kb] == [["ev:64", "sn:64"], ["act:top", "act:home"]]
    assert "<code>dereg_imminent</code> ×0.60" in await render_sn(seeded.db, 47)


async def test_flags_movers_health_spend(bot, seeded):
    for c in ("/flags", "/movers 400", "/health", "/spend", "/review", "/needsreview", "/digest"):
        await bot._handle_update(msg(c))
    texts = [p["text"] for p in bot.fake.sent()]
    flags, movers, health, spend = texts[:4]
    assert "Red flags · 2 active" in flags and "📌" in flags and "(clearing)" not in flags
    assert "Movers · 90d" in movers
    assert "🩺 <b>Health</b>" in health and "jobs ·" in health
    assert "Spend ·" in spend and "tmc" in spend
    assert "Open reviews" in texts[4] and "Needs review" in texts[5] and "Digest events" in texts[6]


async def test_team_commands(bot, seeded):
    await bot._handle_update(msg('/team add 64 "Ann Lee" "cto" https://x.com/annlee'))
    t = bot.fake.sent()[-1]["text"]
    assert t.startswith("✅ team_members #") and "candidate" in t
    mid = int(t.split("#")[1].split()[0])
    row = await seeded.db.fetchone("SELECT name, role, created_by FROM team_members WHERE id = ?", (mid,), writer=True)
    assert row == {"name": "Ann Lee", "role": "cto", "created_by": "telegram"}
    await bot._handle_update(msg(f"/team verify {mid} https://x.com/annlee/status/9"))
    assert "needs a second independent source" in bot.fake.sent()[-1]["text"]
    await bot._handle_update(msg("/team list 64"))
    assert "Ann Lee" in bot.fake.sent()[-1]["text"]
    await bot._handle_update(msg("/team frobnicate"))
    assert bot.fake.sent()[-1]["text"].startswith("Usage:")


async def test_product_command(bot, seeded):
    await bot._handle_update(msg("/product set 64 beta https://chutes.ai/beta"))
    assert bot.fake.sent()[-1]["text"] == "✅ SN64 product_status = beta (was live)"


async def test_confirmations(bot, seeded):
    await bot._handle_update(msg("/flag confirm 64 owner_extraction https://example.com/e"))
    kb = bot.fake.sent()[-1]["reply_markup"]["inline_keyboard"][0]
    data = kb[0]["callback_data"]
    assert data.startswith("do:x:") and len(data.encode()) <= 64 and kb[1]["callback_data"] == "no:x"
    await bot._handle_update(cb(data))
    assert "CONFIRMED" in bot.fake.sent()[-1]["text"]
    assert await seeded.db.fetchval("SELECT state FROM manual_flags", writer=True) == "confirmed"
    await bot._handle_update(cb(data))                                             # reused nonce
    assert "expired" in bot.fake.sent()[-1]["text"]
    await bot._handle_update(msg("/owner compromise 64 2026-09-01T00:00:00Z 2026-09-02T00:00:00Z https://e.com/x"))
    data = bot.fake.sent()[-1]["reply_markup"]["inline_keyboard"][0][0]["callback_data"]
    bot.disp.pending[data[5:]].expires = 0                                         # expired nonce
    await bot._handle_update(cb(data))
    assert "expired" in bot.fake.sent()[-1]["text"]
    await bot._handle_update(msg("/flag clear 64 owner_control_change https://example.com/e"))
    data = bot.fake.sent()[-1]["reply_markup"]["inline_keyboard"][0][0]["callback_data"]
    await bot._handle_update(cb(data))
    assert "flag_not_active" in bot.fake.sent()[-1]["text"]


async def test_pause_two_step(bot, seeded):
    await bot._handle_update(msg("/pause x"))
    kb = bot.fake.sent()[-1]["reply_markup"]["inline_keyboard"][0]
    assert kb[0]["callback_data"] == "do:pause:x"
    n_before = len(bot.fake.calls)
    await bot._handle_update(cb("do:pause:x"))
    assert bot.fake.calls[n_before][0] == "answerCallbackQuery"                    # answered first
    assert await seeded.db.fetchval("SELECT set_by FROM control_state WHERE target = 'x'", writer=True) == "telegram"
    assert "paused by telegram (5 jobs)" in bot.fake.sent()[-1]["text"]
    await bot._handle_update(msg("/pause telegram_bot"))
    assert "not_pausable" in bot.fake.sent()[-1]["text"]
    await bot._handle_update(msg("/pause githu"))
    assert "unknown target" in bot.fake.sent()[-1]["text"]
    await bot._handle_update(msg("/pause x 2"))
    assert bot.fake.sent()[-1]["reply_markup"]["inline_keyboard"][0][0]["callback_data"].startswith("do:x:")


async def test_unauthorized_chat_ignored(bot, cfg):
    await bot._handle_update(msg("/top", chat="999"))
    assert bot.fake.sent() == []
    cfg(TELEGRAM_ALLOWED_CHAT_IDS="999,1000")
    await bot._handle_update(msg("/help", chat="999"))
    assert bot.fake.sent()[-1]["chat_id"] == "999"


async def test_chunking(bot):
    big = "<b>t</b>\n<pre>\n" + "\n".join("x" * 60 for _ in range(100)) + "\n</pre>\nend"
    await bot.emit(CHAT, big, {"inline_keyboard": []}, None)
    parts = bot.fake.sent()
    assert len(parts) == 2 and all(utf16_len(p["text"]) <= 4096 for p in parts)
    for p in parts:
        assert p["text"].count("<pre>") == p["text"].count("</pre>")
    assert "reply_markup" in parts[0] and "reply_markup" not in parts[1]


async def test_handler_exception_contained(bot, monkeypatch):
    async def boom(*a, **k):
        raise ValueError("kaboom")
    monkeypatch.setattr(CommandDispatcher, "c_flags", boom)
    await bot._handle_update(msg("/flags"))
    assert bot.fake.sent()[-1]["text"] == "⚠️ command failed: ValueError"
    await bot._handle_update(msg("/nonsense"))
    assert bot.fake.sent()[-1]["text"] == "Unknown command. /help"


async def test_run_loop_and_token_redacted(bot, seeded, caplog):
    import asyncio
    stop = asyncio.Event()
    polls = []
    bot._on_poll = lambda: polls.append(1)
    bot.fake.updates = [[], [msg("/help")]]                                        # discard round, then one command

    async def stopper():
        while not bot.fake.sent():
            await asyncio.sleep(0.01)
        stop.set()
    await asyncio.wait_for(asyncio.gather(bot.run(stop), stopper()), timeout=5)
    methods = [m for m, _ in bot.fake.calls]
    assert methods[0] == "setMyCommands" and "getUpdates" in methods and polls
    assert TOKEN not in bot._redact(f"https://api.telegram.org/bot{TOKEN}/getUpdates failed")
    from app.http import redact_path
    assert redact_path(f"/bot{TOKEN}/sendMessage") == "/bot***/sendMessage"


def _pre_lines(text: str) -> list[str]:
    out, inside = [], False
    for ln in text.split("\n"):
        if "<pre>" in ln:
            inside = True
            continue
        if "</pre>" in ln:
            inside = False
            continue
        if inside:
            out.append(ln)
    return out


async def test_home_navigation_and_phone_width(bot, seeded, monkeypatch):
    monkeypatch.setattr(W, "terminal_get", fake_terminal())
    await bot._handle_update(msg("/start"))
    home = bot.fake.sent()[-1]
    assert "🛰 <b>TaoSentinel</b>" in home["text"] and "red flags" in home["text"]
    datas = [b["callback_data"] for row in home["reply_markup"]["inline_keyboard"] for b in row]
    assert {"act:top", "act:flags", "act:movers", "w:l", "act:health", "act:review", "act:spend", "act:needs",
            "act:digest"} <= set(datas)
    screens = datas + ["act:top:all", "act:jobs", "act:mv:1", "sn:64", "ev:64", "w:o::Main", "w:p::Main",
                       "w:s:71:Main", "w:f:7:Main", "w:i:1:Main", "w:t:0:Main", "w:o::all"]
    for d in screens:
        n = len(bot.fake.sent())
        await bot._handle_update(cb(d))
        sent = bot.fake.sent()[n:]
        assert sent, d
        p = sent[-1]
        wide = [ln for ln in _pre_lines(p["text"]) if len(ln) > PRE_WIDTH]
        assert not wide, (d, wide)                                                 # 14 §2.7: phone width
        kb = [b["callback_data"] for row in p["reply_markup"]["inline_keyboard"] for b in row]
        assert all(len(x.encode()) <= 64 for x in kb)
        assert "act:home" in kb or d in ("act:home",), d                          # every screen leads home
        assert p.get("message_id") == 7                                            # edited in place


# ── wallet view (14 §8.4a) ─────────────────────────────────────────────────────────────────────────────────────
WALLETS = [
    {"name": "squiddy", "coldkey": "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY", "local": True, "free": 0.0467,
     "staked_tao": 0.0, "n_positions": 0, "events": 3, "first_event": 1774275744, "trading": True,
     "sync": {"ok": True, "at": 1792532621}},
    {"name": "Main", "coldkey": "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty", "local": False, "free": 0.29,
     "staked_tao": 94.07, "n_positions": 3, "events": 5, "first_event": 1739482884, "trading": False,
     "sync": {"ok": True, "at": 1792532647}},
]


def _pos(netuid, name, held, cost, exit_value, unreal, realized, **k):
    return {"netuid": netuid, "name": name, "occupant": "current", "held": held, "cost": cost, "exit_value": exit_value,
            "value_spot": exit_value * 1.01, "unrealized_exit": unreal, "realized": realized,
            "avg_entry": cost / held if held else 0, "spot": exit_value / held if held else 0, "buys": 3, "sells": 1,
            "first_ts": 1751551176, "last_ts": 1792505568, "emissions_alpha": 10.0, "warnings": [], **k}


POSITIONS = {
    "positions": [_pos(64, "Chutes", 1000.0, 50.0, 56.2, 6.2, -36.0),
                  _pos(47, "Foo", 500.0, 20.0, 17.8, -2.2, 0.0),
                  _pos(0, None, 0.9, 0.9, 0.94, 0.0, 5.8),
                  _pos(9, "Dust", 0.2, 0.0, 0.0, 0.0, -4.8),
                  {**_pos(12, "Gone", 0.0, 0.0, 0.0, 0.0, -8.0)}],
    "totals": {"cost": 70.9, "unrealized_exit": 4.0, "realized": -122.2, "total": -118.2, "emissions_value": 13.3,
               "fees_tao": 2.74, "warnings": 0, "free_tao": 0.29, "portfolio_tao": 95.3, "portfolio_exit_tao": 94.6,
               "tao_usd": 276.9},
}
INSIGHTS = {
    "empty": False,
    "persona": {"emoji": "🧳", "title": "The Subnet Tourist", "blurb": "Why pick one?", "traits": ["32% win rate"]},
    "stats": [{"label": "Trades", "value": "1,417"}, {"label": "Volume", "value": "11,519.8 τ"},
              {"label": "Win rate", "value": "32%"}, {"label": "Total PnL", "value": "-112.63 τ"},
              {"label": "Fees", "value": "2.739 τ"}],
    "cards": [{"id": "momentum", "emoji": "📈", "title": "Heating up", "headline": "Score 44 → 52", "detail": "x"}]
             + [{"id": f"c{i}", "emoji": "✨", "title": f"Card {i}", "headline": f"-{i}.00 τ", "detail": f"detail {i}"}
                for i in range(8)]
             + [{"id": "worst_month", "emoji": "🌧️", "title": "Worst month", "headline": "2025-10: -65.23 τ"}],
    "curve": [[1, 0.0], [2, 1.0], [3, -2.0], [4, -5.0]],
    "score": {"period": 30, "score": 52.2, "grade": "C", "prev": 43.5, "prev_grade": "D", "delta": 8.8,
              "sells": 15, "realized": 2.416, "prev_sells": 7, "history": [[1, 40.0], [2, None], [3, 52.0]],
              "parts": [{"id": "profit", "label": "Profit", "value": 57.0, "delta": 18.9, "tip": "cut losers"},
                        {"id": "wins", "label": "Win rate", "value": 29.4, "delta": -15.0, "tip": "fewer trades"}]},
}
TRADES = [{"ts": 1792505568 - i * 86400, "kind": "buy" if i % 2 else "sell", "netuid": 64, "tao": 9.98,
           "old_occupant": False, "source": "taostats"} for i in range(20)] + \
         [{"ts": 1760000000, "kind": "liquidation", "netuid": 12, "tao": 15.4, "old_occupant": True, "source": "taostats"}]


def fake_terminal(calls: list | None = None, fail: bool = False):
    async def get(path, **params):
        if calls is not None:
            calls.append((path, params))
        if fail:
            raise W.TerminalUnavailable("ConnectError")
        return {"/api/wallets": WALLETS, "/api/positions": POSITIONS, "/api/insights": INSIGHTS,
                "/api/trades": TRADES}[path]
    return get


async def test_wallet_list_and_overview(bot, seeded, monkeypatch):
    calls: list = []
    monkeypatch.setattr(W, "terminal_get", fake_terminal(calls))
    await bot._handle_update(msg("/wallet"))
    p = bot.fake.sent()[-1]
    assert "👛 <b>Wallets</b>" in p["text"] and "👁 <b>Main</b> · <i>watch-only</i>" in p["text"]
    kb = [b["callback_data"] for row in p["reply_markup"]["inline_keyboard"] for b in row]
    assert kb[:3] == ["w:o::squiddy", "w:o::Main", "w:o::all"]
    await bot._handle_update(msg("/wallet main"))                                  # case-insensitive name
    t = bot.fake.sent()[-1]["text"]
    assert "💰 <b>95.30 τ</b>" in t and "94.60 τ if sold now" in t and "3 positions + 1 dust" in t
    assert "+4.00 τ" in t and "−122.20 τ" in t and "−118.20 τ" in t and "+5.6%" in t
    assert ("/api/positions", {"wallet": "Main"}) in calls
    assert "1,722" not in t and "11,519" not in t                                  # never a turnover total
    await bot._handle_update(msg("/wallet nobody"))
    assert "No wallet called" in bot.fake.sent()[-1]["text"]


async def test_wallet_positions_join_sentinel(bot, seeded, monkeypatch):
    monkeypatch.setattr(W, "terminal_get", fake_terminal())
    text, kb = await W.screen(seeded.db, "p", "", "Main")
    rows = [ln for ln in _pre_lines(text) if ln[:3].strip().isdigit()]
    assert rows[0].split()[:3] == ["64", "Chutes", "A"] and rows[0].endswith("+12.4%")   # tier from TaoSentinel
    assert rows[1].split()[:3] == ["47", "Foo", "D"] and rows[2].split()[:2] == ["0", "Root"]
    assert "🪙 dust: SN9" in text and "SN12" not in text                          # closed positions are not listed
    assert "🚩 SN47 Foo: <code>dereg_imminent</code>" in text
    assert [b["callback_data"] for b in kb["inline_keyboard"][0]] == ["w:s:64:Main", "w:s:47:Main", "w:s:0:Main"]
    card, kb = await W.screen(seeded.db, "s", "64", "Main")
    assert "📌 <b>SN64 · Chutes</b>" in card and "open PnL +6.20 τ +12.4%" in card and "🛰 Sentinel: 🟢 <b>A</b>" in card
    assert ["sn:64"] == [b["callback_data"] for b in kb["inline_keyboard"][0] if b["callback_data"].startswith("sn:")]


async def test_wallet_performance_insights_trades(bot, seeded, monkeypatch):
    calls: list = []
    monkeypatch.setattr(W, "terminal_get", fake_terminal(calls))
    text, kb = await W.screen(seeded.db, "f", "7", "Main")
    assert ("/api/insights", {"wallet": "Main", "period": 7}) in calls
    assert "Trader score <b>52</b> (C) · was 44 (D) · ▲ +9" in text and "Realised this week: <b>+2.42 τ</b>" in text
    assert "Σ Lifetime PnL: <b>−112.63 τ</b>" in text and "11,519" not in text   # volume (turnover) is not shown
    assert "🌧 worst month: 2025-10: −65.23 τ" in text and "💡 <i>fewer trades</i>" in text
    assert [b["text"] for b in kb["inline_keyboard"][0]] == ["1D", "✓ 1W", "1M", "1Y"]
    text, kb = await W.screen(seeded.db, "i", "0", "Main")
    assert "The Subnet Tourist" in text and "Card 0" in text and "Heating up" not in text and "Card 6" not in text
    assert [b["callback_data"] for b in kb["inline_keyboard"][0]] == ["w:i:0:Main", "w:i:1:Main"]
    text, _ = await W.screen(seeded.db, "i", "9", "Main")                          # clamped to the last page
    assert "Card 6" in text and "−7.00 τ" in text
    text, kb = await W.screen(seeded.db, "t", "0", "Main")
    rows = _pre_lines(text)
    assert rows[0] == "── 2026 ──" and "sell" in rows[1] and "Chutes" in rows[1] and len(rows) == 16
    text, _ = await W.screen(seeded.db, "t", "1", "Main")
    assert "liq   12 (old)" in text


async def test_wallet_terminal_down_and_off(bot, seeded, monkeypatch, cfg):
    monkeypatch.setattr(W, "terminal_get", fake_terminal(fail=True))
    text, kb = await W.screen(seeded.db, "l", "", "")
    assert "did not answer" in text and kb["inline_keyboard"][0][0]["callback_data"] == "w:l"
    monkeypatch.undo()
    cfg(TERMINAL_API_URL="")
    text, _ = await W.screen(seeded.db, "o", "", "Main")
    assert "wallet view is off" in text

