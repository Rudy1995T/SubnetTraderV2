#!/usr/bin/env bash
# One-time host preparation for a 24/7 TaoSentinel laptop (spec 16 §2.2). Idempotent; run as root:
#   sudo ops/laptop-prep.sh --user <login> [--dry-run] [--skip 5,11] [--mirror-label TSMIRROR]
# Steps that need a human (BIOS "power on with AC", Tailscale key-expiry toggle, Wi-Fi PSK) are printed, not done.
set -uo pipefail

USER_LOGIN=""; DRY=0; SKIP=","; MIRROR_LABEL=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --user) USER_LOGIN="$2"; shift ;;
    --dry-run) DRY=1 ;;
    --skip) SKIP=",$2," ; shift ;;
    --mirror-label) MIRROR_LABEL="$2"; shift ;;
    *) echo "unknown argument $1"; exit 2 ;;
  esac
  shift
done
[[ $EUID -eq 0 || $DRY -eq 1 ]] || { echo "Must run as root (try: sudo $0 --user \$USER)"; exit 1; }
[[ -n "$USER_LOGIN" ]] || { echo "--user <login> is required"; exit 2; }

declare -a SUMMARY
run() { if [[ $DRY -eq 1 ]]; then echo "  + $*"; else eval "$@"; fi; }
step() {  # step N "name" function
  local n="$1" name="$2" fn="$3"
  if [[ "$SKIP" == *",$n,"* ]]; then SUMMARY+=("$n|$name|skipped"); return 0; fi
  echo "== step $n: $name"
  if "$fn"; then SUMMARY+=("$n|$name|ok"); else SUMMARY+=("$n|$name|FAILED"); print_summary; exit 1; fi
}
print_summary() { printf '\n%-3s %-28s %s\n' "#" "step" "state"; for l in "${SUMMARY[@]}"; do IFS='|' read -r a b c <<<"$l"; printf '%-3s %-28s %s\n' "$a" "$b" "$c"; done; }

s0() {  # prerequisite checks (old install.sh idea): OS + tools
  . /etc/os-release; echo "  $PRETTY_NAME $(uname -m)"
  command -v python3.12 >/dev/null || echo "  python3.12 missing (step 1 installs it)"
  return 0
}
s1() { run "apt-get update -qq && apt-get install -y python3.12 python3.12-venv python3.12-dev git sqlite3 curl jq openssl ufw logrotate build-essential unattended-upgrades"; }
s2() { run "timedatectl set-timezone Etc/UTC"; }
s3() {
  run "timedatectl set-ntp true"; run "systemctl enable --now systemd-timesyncd"
  run "systemctl enable systemd-time-wait-sync.service"
  run "mkdir -p /etc/systemd/system/systemd-time-wait-sync.service.d"
  run "printf '[Service]\nTimeoutStartSec=180\n' > /etc/systemd/system/systemd-time-wait-sync.service.d/50-timeout.conf"
}
s4() {
  run "systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target"
  run "mkdir -p /etc/systemd/logind.conf.d"
  run "printf '[Login]\nHandleLidSwitch=ignore\nHandleLidSwitchExternalPower=ignore\nHandleLidSwitchDocked=ignore\nHandleSuspendKey=ignore\nHandleHibernateKey=ignore\nIdleAction=ignore\n' > /etc/systemd/logind.conf.d/50-taosentinel.conf"
  if command -v gsettings >/dev/null; then
    run "sudo -u $USER_LOGIN dbus-launch gsettings set org.gnome.settings-daemon.plugins.power sleep-inactive-ac-type nothing || true"
    run "sudo -u $USER_LOGIN dbus-launch gsettings set org.gnome.settings-daemon.plugins.power sleep-inactive-battery-type nothing || true"
  fi
}
s5() {
  [[ -f /etc/UPower/UPower.conf ]] || return 0
  for kv in UsePercentageForPolicy=true PercentageLow=20 PercentageCritical=8 PercentageAction=5 CriticalPowerAction=PowerOff; do
    k="${kv%%=*}"; run "sed -i -E 's/^#?${k}=.*/${kv}/' /etc/UPower/UPower.conf"
  done
  run "systemctl restart upower || true"
}
s6() {
  run "sed -i -E 's/^GRUB_TIMEOUT=.*/GRUB_TIMEOUT=3/' /etc/default/grub"
  run "grep -q '^GRUB_RECORDFAIL_TIMEOUT=' /etc/default/grub && sed -i -E 's/^GRUB_RECORDFAIL_TIMEOUT=.*/GRUB_RECORDFAIL_TIMEOUT=5/' /etc/default/grub || echo GRUB_RECORDFAIL_TIMEOUT=5 >> /etc/default/grub"
  run "update-grub"
  echo "  OPERATOR: in BIOS/UEFI enable 'Power on with AC attach' / 'AC power recovery = On'."
}
s7() {
  run "systemctl is-enabled NetworkManager-wait-online.service || systemctl enable NetworkManager-wait-online.service"
  echo "  OPERATOR (Wi-Fi only): nmcli connection modify \"<SSID>\" connection.permissions \"\" 802-11-wireless-security.psk-flags 0 802-11-wireless-security.psk \"<password>\""
}
s8() {
  run "printf 'Unattended-Upgrade::Automatic-Reboot \"true\";\nUnattended-Upgrade::Automatic-Reboot-WithUsers \"true\";\nUnattended-Upgrade::Automatic-Reboot-Time \"04:30\";\n' > /etc/apt/apt.conf.d/52taosentinel"
  run "dpkg-reconfigure -f noninteractive unattended-upgrades"
}
s9() {
  local have need
  have=$(swapon --show --bytes --noheadings 2>/dev/null | awk '{s+=$3} END{print s+0}')
  need=$((4*1024*1024*1024))
  if (( have < need )); then
    local gib=$(( (need - have + 1073741823) / 1073741824 ))
    run "fallocate -l ${gib}G /swapfile-ts && chmod 600 /swapfile-ts && mkswap /swapfile-ts && swapon /swapfile-ts"
    run "grep -q swapfile-ts /etc/fstab || echo '/swapfile-ts none swap sw 0 0' >> /etc/fstab"
  fi
  run "echo vm.swappiness=10 > /etc/sysctl.d/60-taosentinel.conf && sysctl --system >/dev/null"
}
s10() {
  run "ufw default deny incoming"; run "ufw default allow outgoing"
  run "ufw allow in on tailscale0 || true"; run "ufw --force enable"
}
s11() {
  command -v tailscale >/dev/null || run "curl -fsSL https://tailscale.com/install.sh | sh"
  echo "  OPERATOR: run 'sudo tailscale up', then disable key expiry for this machine in the admin console."
  echo "  Only after API_AUTH_TOKEN is set: tailscale serve --bg --https=8443 http://127.0.0.1:8090"
}
s12() {
  run "mkdir -p /etc/systemd/journald.conf.d"
  run "printf '[Journal]\nSystemMaxUse=500M\nMaxRetentionSec=30day\n' > /etc/systemd/journald.conf.d/50-taosentinel.conf"
  run "systemctl restart systemd-journald"
}
s13() {
  [[ -n "$MIRROR_LABEL" ]] || { echo "  (no --mirror-label: skipped)"; return 0; }
  run "mkdir -p /mnt/taosentinel-mirror"
  run "grep -q 'LABEL=$MIRROR_LABEL' /etc/fstab || echo 'LABEL=$MIRROR_LABEL /mnt/taosentinel-mirror ext4 nofail,noatime,x-systemd.device-timeout=10s 0 2' >> /etc/fstab"
  run "mount -a"; run "chown $USER_LOGIN: /mnt/taosentinel-mirror && chmod 700 /mnt/taosentinel-mirror"
  run "sudo -u $USER_LOGIN touch /mnt/taosentinel-mirror/.taosentinel-mirror"
}
s14() { run "usermod -aG systemd-journal $USER_LOGIN"; }

step 0 "prerequisites" s0
step 1 "packages" s1
step 2 "UTC clock" s2
step 3 "time sync before start" s3
step 4 "never sleep" s4
step 5 "battery -> clean poweroff" s5
step 6 "unattended boot (GRUB)" s6
step 7 "network without login" s7
step 8 "unattended upgrades" s8
step 9 "swap >= 4 GiB" s9
step 10 "firewall (UFW)" s10
step 11 "tailscale" s11
step 12 "journald cap" s12
step 13 "backup mirror mount" s13
step 14 "journal read access" s14
print_summary
echo "Reboot once, then: sudo ops/install-systemd.sh"
