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
