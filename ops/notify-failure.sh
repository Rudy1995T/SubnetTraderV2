#!/usr/bin/env bash
# OnFailure= notifier (spec 16 §2.4): Telegram message when taosentinel.service enters `failed`. Always exits 0.
UNIT="${1:-taosentinel.service}"
ROOT="${SENTINEL_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
kv() {  # kv KEY FILE — value of KEY=… (quotes and `export` tolerated)
  sed -n -E "s/^(export[[:space:]]+)?$1=[\"']?([^\"']*)[\"']?[[:space:]]*$/\2/p" "$2" 2>/dev/null | tail -1
}
TOKEN="$(kv TELEGRAM_BOT_TOKEN "$ROOT/.env")"
CHAT="$(kv TELEGRAM_CHAT_ID "$ROOT/.env")"
[[ -n "$TOKEN" && -n "$CHAT" ]] || exit 0
esc() { sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g'; }
INFO="$(systemctl show "$UNIT" -p Result -p NRestarts -p ExecMainStatus -p ActiveEnterTimestamp 2>/dev/null | tr '\n' ' ')"
RESULT="$(systemctl show "$UNIT" -p Result --value 2>/dev/null)"
RESTARTS="$(systemctl show "$UNIT" -p NRestarts --value 2>/dev/null)"
TAIL="$( { journalctl -u "$UNIT" --no-pager -n 15 2>/dev/null; echo '--- service.log'; tail -n 15 "$ROOT/data/service.log" 2>/dev/null; } | esc | tail -c 3500)"
TEXT="🛑 <b>TaoSentinel unit FAILED</b> ($UNIT) result=${RESULT} restarts=${RESTARTS}
<code>$(echo "$INFO" | esc)</code>
<pre>${TAIL}</pre>"
curl -fsS -m 15 "https://api.telegram.org/bot${TOKEN}/sendMessage" \
  --data-urlencode "chat_id=${CHAT}" --data-urlencode "parse_mode=HTML" --data-urlencode "text=${TEXT}" >/dev/null 2>&1 || true
exit 0
