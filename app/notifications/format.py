"""Telegram message formatting helpers (spec 14 §2.7; shipped in Phase 0 with the dispatcher)."""
from __future__ import annotations

import html


def esc(x: object) -> str:
    return html.escape("" if x is None else str(x), quote=False)


def utf16_len(s: str) -> int:
    return len(s.encode("utf-16-le")) // 2


def chunk(text: str, limit: int = 4000) -> list[str]:
    """Split on line boundaries; close/reopen ``<pre>`` around a cut; length counted in UTF-16 code units."""
    lines = text.split("\n")
    out: list[str] = []
    cur: list[str] = []
    cur_len = 0
    in_pre = False
    for line in lines:
        add = utf16_len(line) + (1 if cur else 0)
        closing = utf16_len("</pre>") + 1 if in_pre else 0
        if cur and cur_len + add + closing > limit:
            body = "\n".join(cur)
            if in_pre:
                body += "\n</pre>"
            out.append(body)
            cur = ["<pre>"] if in_pre else []
            cur_len = utf16_len("<pre>") if in_pre else 0
            add = utf16_len(line) + (1 if cur else 0)
        while utf16_len(line) > limit:            # a single overlong line is hard-cut
            head = line[: limit - 10]
            out.append(head)
            line = line[limit - 10:]
            add = utf16_len(line)
        cur.append(line)
        cur_len += add
        opens = line.count("<pre>")
        closes = line.count("</pre>")
        if opens > closes:
            in_pre = True
        elif closes > opens:
            in_pre = False
    if cur:
        out.append("\n".join(cur))
    return [c for c in out if c.strip()]


def fmt_age(seconds: float | None) -> str:
    if seconds is None:
        return "-"
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m"
    if s < 86400:
        return f"{s // 3600}h{(s % 3600) // 60:02d}m"
    return f"{s // 86400}d {(s % 86400) // 3600:02d}h"


def bar(v: float | None, cells: int = 10) -> str:
    """``████████░░ .81``; ``None`` → ``░░░░░░░░░░ n/a`` (spec 14 §8.5)."""
    if v is None:
        return "░" * cells + " n/a"
    v = max(0.0, min(1.0, float(v)))
    n = int(round(v * cells))
    return "█" * n + "░" * (cells - n) + " " + dot2(v)


def dot2(v: float | None) -> str:
    """``0.91`` → ``.91`` (``1.0`` → ``1.0``); ``None`` → ``-``."""
    if v is None:
        return "-"
    s = f"{float(v):.2f}"
    return s[1:] if s.startswith("0.") else s


def num(v: float | None, spec: str = ".1f", width: int = 0) -> str:
    s = "—" if v is None else format(float(v), spec)
    return s.rjust(width) if width else s


def cut(s: object, width: int) -> str:
    s = "" if s is None else str(s)
    return s if len(s) <= width else s[: max(1, width - 1)] + "…"


# ── phone-width helpers (14 §2.7 amended 2026-10-20: `<pre>` lines ≤ 32 characters) ─────────────────────────
PRE_WIDTH = 32
_SPARK = "▁▂▃▄▅▆▇█"
_TIER_DOT = {"A": "🟢", "B": "🔵", "C": "🟡", "D": "🟠", "F": "🔴"}
_STATUS_DOT = {"ok": "🟢", "healthy": "🟢", "degraded": "🟡"}


def tier_dot(tier: str | None) -> str:
    return _TIER_DOT.get(tier or "", "⚪")


def status_dot(status: str | None) -> str:
    return _STATUS_DOT.get(status or "", "🔴")


def cells(v: float | None, n: int = 10) -> str:
    """``bar`` without the number: ``█`` × round(v·n), ``░`` for the rest (``None`` → all ``░``)."""
    if v is None:
        return "░" * n
    k = int(round(max(0.0, min(1.0, float(v))) * n))
    return "█" * k + "░" * (n - k)


def spark(values: list[float | None], width: int = 24) -> str:
    """Unicode sparkline of the last ``width`` points, resampled evenly; ``None`` points are dropped."""
    vs = [float(v) for v in values if v is not None]
    if len(vs) < 2:
        return ""
    if len(vs) > width:
        vs = [vs[round(i * (len(vs) - 1) / (width - 1))] for i in range(width)]
    lo, hi = min(vs), max(vs)
    if hi - lo < 1e-12:
        return _SPARK[3] * len(vs)
    return "".join(_SPARK[int((v - lo) / (hi - lo) * (len(_SPARK) - 1))] for v in vs)


def tao(v: float | None, sign: bool = False, nd: int = 2) -> str:
    """``12.34 τ`` / ``+12.34 τ`` with a real minus sign; ``None`` → ``—``."""
    if v is None:
        return "—"
    s = f"{float(v):{'+' if sign else ''},.{nd}f}".replace("-", "−")
    return f"{s} τ"


def pct(v: float | None, sign: bool = True, nd: int = 1) -> str:
    """Fraction → ``+12.6%`` (``None`` → ``—``)."""
    if v is None:
        return "—"
    return f"{float(v) * 100:{'+' if sign else ''}.{nd}f}%".replace("-", "−")


def day_month(ts: int | None) -> str:
    """``20 Oct`` (UTC)."""
    import time
    return "-" if ts is None else time.strftime("%d %b", time.gmtime(int(ts))).lstrip("0")


def day_month_hm(ts: int | None) -> str:
    """``20 Oct 18:00Z`` (UTC)."""
    import time
    return "-" if ts is None else time.strftime("%d %b %H:%MZ", time.gmtime(int(ts))).lstrip("0")
