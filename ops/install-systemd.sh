#!/usr/bin/env bash
# Install TaoSentinel's systemd units (spec 16 §2.4). Idempotent. Does NOT start anything.
#   sudo [SENTINEL_USER=<login>] [SENTINEL_ROOT=<path>] [SENTINEL_SECRETS=<path>] [SENTINEL_MIRROR_DIR=<path>] \
#        ops/install-systemd.sh [--with-frontend] [--with-update-timer] [--render-only <outdir>] [--uninstall]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"   # unit templates live in this checkout
WITH_FRONTEND=0; WITH_TIMER=0; RENDER_ONLY=""; UNINSTALL=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-frontend) WITH_FRONTEND=1 ;;
    --with-update-timer) WITH_TIMER=1 ;;
    --render-only) RENDER_ONLY="$2"; shift ;;
    --uninstall) UNINSTALL=1 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

UNITS=(taosentinel.service taosentinel-notify-failure@.service taosentinel-frontend.service
       taosentinel-update.service taosentinel-update.timer)

if [[ $UNINSTALL -eq 1 ]]; then
  [[ $EUID -eq 0 ]] || { echo "Must run as root (try: sudo $0)"; exit 1; }
  systemctl disable --now taosentinel-update.timer taosentinel-frontend.service taosentinel.service 2>/dev/null || true
  for u in "${UNITS[@]}"; do rm -f "/etc/systemd/system/$u"; done
  rm -f /etc/sudoers.d/taosentinel /etc/logrotate.d/taosentinel
  systemctl daemon-reload
  echo "uninstalled (data/ untouched)"
  exit 0
fi

# 1. root (not needed for --render-only)
if [[ -z "$RENDER_ONLY" && $EUID -ne 0 ]]; then echo "Must run as root (try: sudo $0)"; exit 1; fi

# 2. resolve
SENTINEL_USER="${SENTINEL_USER:-${SUDO_USER:-}}"
if [[ -z "$SENTINEL_USER" || "$SENTINEL_USER" == "root" ]]; then
  echo "SENTINEL_USER must be a normal login (run with sudo from your account, or set SENTINEL_USER)"; exit 1
fi
if [[ -n "$RENDER_ONLY" ]]; then
  SENTINEL_GROUP="${SENTINEL_GROUP:-$SENTINEL_USER}"
else
  SENTINEL_GROUP="$(id -gn "$SENTINEL_USER")"
fi
SENTINEL_ROOT="${SENTINEL_ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
SENTINEL_SECRETS="${SENTINEL_SECRETS:-/home/$SENTINEL_USER/taosentinel-secrets}"
SENTINEL_MIRROR_DIR="${SENTINEL_MIRROR_DIR:-}"

# 3. validate
if [[ -z "$RENDER_ONLY" ]]; then
  "$SENTINEL_ROOT/.venv/bin/python" -c 'import sys; assert sys.version_info[:2]==(3,12)' \
    || { echo "$SENTINEL_ROOT/.venv must be a Python 3.12 venv (make install)"; exit 1; }
  [[ -f "$SENTINEL_ROOT/.env" ]] || { echo "$SENTINEL_ROOT/.env missing (cp .env.example .env)"; exit 1; }
fi
if [[ -n "$SENTINEL_MIRROR_DIR" ]]; then
  [[ "$SENTINEL_MIRROR_DIR" == /* ]] || { echo "SENTINEL_MIRROR_DIR must be absolute"; exit 1; }
  [[ "$SENTINEL_MIRROR_DIR" != /home/* ]] || { echo "SENTINEL_MIRROR_DIR must not live under /home (ProtectHome)"; exit 1; }
  MIRROR_RW_LINE="ReadWritePaths=-$SENTINEL_MIRROR_DIR"
else
  MIRROR_RW_LINE=""
fi

NODE_DIR="/usr/bin"
if [[ -z "$RENDER_ONLY" && ( $WITH_FRONTEND -eq 1 || $WITH_TIMER -eq 1 ) ]]; then
  NODE_BIN="$(sudo -u "$SENTINEL_USER" bash -lc 'command -v node' || true)"
  if [[ -n "$NODE_BIN" ]]; then NODE_DIR="$(dirname "$NODE_BIN")"
  elif [[ $WITH_FRONTEND -eq 1 ]]; then echo "node not found in $SENTINEL_USER's login shell (install Node 20+ first)"; exit 1; fi
fi

if [[ -z "$RENDER_ONLY" ]]; then
  # 4. directories and permissions
  for d in data data/logs data/backups data/backups/manual data/run data/control data/tmp data/quarantine data/cache; do
    install -d -m 700 -o "$SENTINEL_USER" -g "$SENTINEL_GROUP" "$SENTINEL_ROOT/$d"
  done
  chown "$SENTINEL_USER": "$SENTINEL_ROOT/.env" && chmod 600 "$SENTINEL_ROOT/.env"
  if [[ -f "$SENTINEL_SECRETS/deploy.env" ]]; then chmod 700 "$SENTINEL_SECRETS"; chmod 600 "$SENTINEL_SECRETS/deploy.env"; fi
  # 5. bytecode (the unit cannot write __pycache__ under ProtectHome=read-only)
  sudo -u "$SENTINEL_USER" "$SENTINEL_ROOT/.venv/bin/python" -m compileall -q "$SENTINEL_ROOT/app" "$SENTINEL_ROOT/scripts"
fi

# 7. render
OUT="${RENDER_ONLY:-$(mktemp -d)}"
mkdir -p "$OUT"
render() {  # src dst
  sed -e "s#@SENTINEL_USER@#$SENTINEL_USER#g" -e "s#@SENTINEL_GROUP@#$SENTINEL_GROUP#g" \
      -e "s#@SENTINEL_ROOT@#$SENTINEL_ROOT#g" -e "s#@SENTINEL_SECRETS@#$SENTINEL_SECRETS#g" \
      -e "s#@NODE_DIR@#$NODE_DIR#g" -e "s#@MIRROR_RW_LINE@#$MIRROR_RW_LINE#g" "$1" > "$2"
  if grep -q '@[A-Z_]\+@' "$2"; then echo "unrendered placeholder in $2"; exit 1; fi
}
render "$REPO_DIR/ops/systemd/taosentinel.service" "$OUT/taosentinel.service"
render "$REPO_DIR/ops/systemd/taosentinel-notify-failure@.service" "$OUT/taosentinel-notify-failure@.service"
render "$REPO_DIR/ops/systemd/taosentinel-frontend.service" "$OUT/taosentinel-frontend.service"
render "$REPO_DIR/deploy/taosentinel-update.service" "$OUT/taosentinel-update.service"
render "$REPO_DIR/deploy/taosentinel-update.timer" "$OUT/taosentinel-update.timer"
if [[ -n "$RENDER_ONLY" ]]; then echo "rendered into $OUT"; exit 0; fi

# 8. install units
install -m 644 "$OUT/taosentinel.service" /etc/systemd/system/taosentinel.service
install -m 644 "$OUT/taosentinel-notify-failure@.service" /etc/systemd/system/taosentinel-notify-failure@.service
[[ $WITH_FRONTEND -eq 1 ]] && install -m 644 "$OUT/taosentinel-frontend.service" /etc/systemd/system/
if [[ $WITH_TIMER -eq 1 ]]; then
  install -m 644 "$OUT/taosentinel-update.service" /etc/systemd/system/
  install -m 644 "$OUT/taosentinel-update.timer" /etc/systemd/system/
fi

# 9. sudoers (validated before install)
TMP_SUDO="$(mktemp)"
echo "$SENTINEL_USER ALL=(root) NOPASSWD: /usr/bin/systemctl restart taosentinel, /usr/bin/systemctl restart taosentinel-frontend" > "$TMP_SUDO"
visudo -cf "$TMP_SUDO" >/dev/null && install -m 440 "$TMP_SUDO" /etc/sudoers.d/taosentinel
rm -f "$TMP_SUDO"

# 10. logrotate for the append: stdout/stderr logs
cat > /etc/logrotate.d/taosentinel <<EOF
$SENTINEL_ROOT/data/service.log $SENTINEL_ROOT/data/frontend.log {
  daily
  rotate 14
  maxsize 50M
  missingok
  notifempty
  compress
  delaycompress
  copytruncate
  su $SENTINEL_USER $SENTINEL_GROUP
}
EOF

# 11. enable (never start)
systemctl daemon-reload
systemctl enable taosentinel.service
[[ $WITH_FRONTEND -eq 1 ]] && systemctl enable taosentinel-frontend.service
[[ $WITH_TIMER -eq 1 ]] && systemctl enable taosentinel-update.timer

# 12. next steps
cat <<EOF
Installed. Next:
  sudo systemctl start taosentinel
  systemctl status taosentinel
  journalctl -u taosentinel -f
  tail -f $SENTINEL_ROOT/data/service.log
EOF
