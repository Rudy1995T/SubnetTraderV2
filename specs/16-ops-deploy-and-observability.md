# 16 — Ops: laptop prep, systemd, watchdog, backups/restore, auto-update, cost governor, kill switches, power-loss recovery, observability, runbooks

**Status:** proposed · **Phase:** — (continuous; the Phase-0 subset ships with spec 01, the rest lands as each phase needs it) · **Depends on:** 02 (reconciliation ledger — binding over everything below; cited as `02 §<id>`), 01 (runtime, `CollectorJob`/`LongTask`, `_jobs_live`, `/health`, `AlertLatch`, `ProviderLimiter`, `Http`, tables `job_state`/`provider_spend`/`collector_runs`), 03 (`Database.backup_to`/`transaction()`, `migrate()`/`discover()`, 03 §2.8, which assigns the backup/restore pipeline to this spec and fixes the `backup_to` primitive (03 §2.3 item 7) and `CODE_SCHEMA_VERSION` (03 §2.3 item 9), DDL of `control_state`/`provider_spend`/`job_state`/`events` + the event catalog 03 §3.9, job `storage_retention_daily`), 14 (`events` + `alert_dispatch`, `app/control.py`, `/api/health/services`, `/api/v1/control/*`), 09 + 10 (cost-governor callers), 12 (scoring kill switch, `SCORING_SHADOW_SPECS`, `python -m app.scoring backfill`), 15 (feed honours `SCORING_KILLED`) · **Master spec refs:** §1.3 (fail-safe ops), §2 (ops rows of the reuse table), §3 (process model, layout), §4 (TMC per-IP limits), §6 (`collector_runs`, `provider_spend`), §9 (`/health`, Telegram alerts), §11 (operations — binding), §12 (phase plan: "16 — 0, continuous"), §13.6 (repo hosting)

---

## 1. Purpose and scope

TaoSentinel runs unattended, 24/7, on one Ubuntu laptop that is occasionally powered off. This spec makes that safe: the host is prepared so it boots and reconnects without a human, the process is supervised and restarted when it wedges, the SQLite file is backed up and restorable, code updates cannot take the service down, paid APIs cannot overspend, every subsystem can be paused from a file or a command, and the operator is told — once, in Telegram — when something needs attention.

**In scope**
- Laptop preparation (`ops/laptop-prep.sh`): Ubuntu 22.04+/24.04, Python 3.12 venv, sleep/lid masking, battery policy, unattended boot (GRUB, BIOS, Wi-Fi), UTC + time sync, unattended-upgrades reboot window, UFW (loopback + Tailscale only), swap, journald cap, mirror mount.
- Service user and path layout; permissions.
- systemd units **verbatim**: `taosentinel.service`, `taosentinel-frontend.service` (Phase 11), `taosentinel-notify-failure@.service`, `taosentinel-update.service` + `.timer`; `ops/install-systemd.sh` parameterised by `SENTINEL_USER` / `SENTINEL_ROOT`; sudoers and logrotate drop-ins.
- Liveness max-age rules (canonical per 02 §E4; spec 01 implements them with the five amendments of §2.5).
- Power-loss detection (instance lock + heartbeat) and the recovery order.
- Backups (`backup_daily` at 03:30Z: `VACUUM INTO` tmp → `quick_check` → rename → gzip → mirror → GFS retention 7 daily + 8 weekly; this spec owns `app/storage/backup.py` beyond the `backup_to` primitive, per spec 03 §2.8), restore CLI and runbook.
- `deploy/auto-update.sh` (ported): PAT from a separate secrets file, compileall + import smoke + `app check` on a scratch DB + optional `pytest -m smoke`, restart, `/health` poll 60 s, rollback.
- Cost governor `app/costs.py` (full; supersedes the spec 01 §2.13 subset; API = 02 §G5): check / reserve → commit / release, record, snapshot; `provider_spend` ledger, caps (0 = provider disabled, 02 §F2), 80 % alert once, 100 % hard stop with collector auto-pause, UTC month reset, override.
- Kill switches (`KILL_SWITCH`, `SCORING_KILLED`, `PAUSE_ALL` as operator files — 02 §B12's closed sentinel list; per-target pauses only in `control_state`; `data/control/<target>.paused` as read-only emergency input), the `PAUSE_GROUPS` table (pause targets = exact job ids or group names, never prefixes), the `ops_exempt` job list and the pause precedence.
- Rate-limit hygiene across processes (`scripts/with_bucket.py`, shared 429 penalty files).
- Monitoring: ops alert catalogue and routing, the ops jobs `backup_daily`, `ops_guard_5m`, `ops_digest_daily` (09:00Z) and `cost_ledger_hourly` (one table, §6.1), optional dead-man ping, `OnFailure=` notifier, log retention.
- Security baseline; runbooks (§7.3).

**Out of scope:** collector logic (04–11), scoring math and the shadow-series writer (12), the Telegram command surface and `alert_dispatch` internals (14), DDL of non-ops tables (03), Docker (the old `Dockerfile`/`docker-compose.yml` are **not** ported — systemd only), frontend code (14), historical backfill (17), anything touching wallets or keys of the old bot.

---

## 2. Design

### 2.1 Overview

```
 systemd (PID 1) — starts without any user login
  ├─ taosentinel.service   Type=notify · WatchdogSec=120 · Restart=on-failure · OnFailure=notify
  │    python -m app serve  (spec 01 runtime)
  │      ├─ pre_init: instance flock → read previous heartbeat → classify boot (clean / crash / unclean reboot)
  │      ├─ READY=1 after scheduler start · WATCHDOG=1 every 60 s iff _jobs_live() ∧ API task alive
  │      ├─ ops jobs (§6.1): backup_daily 03:30Z · ops_guard_5m · ops_digest_daily 09:00Z · cost_ledger_hourly
  │      ├─ CostGovernor (02 §G5): check | reserve → call → commit/release | record   ◄── x_* / llm_* jobs, scripts via with_bucket
  │      └─ alerts (02 §D3): AlertLatch.fire → emit_event row → alert_dispatch (14's id; 01 ships it in Phase 0, ops_exempt);
  │                          direct send_alert only as the fallback when the DB write fails
  │    on failed state ──► taosentinel-notify-failure@taosentinel.service ──► ops/notify-failure.sh ──► Telegram (curl)
  ├─ taosentinel-frontend.service (Phase 11; Wants=, not Requires=) ── node next start -p 3001 -H 127.0.0.1
  └─ taosentinel-update.timer (opt-in, every 5 min) ──► taosentinel-update.service ──► deploy/auto-update.sh
        flock · DEPLOY_PAUSED? · fetch (PAT from ~/taosentinel-secrets/deploy.env) · quiet window? · [manual-deploy]?
        pre-deploy backup if app/storage/ changed · reset --hard · pip if deps changed
        gate: compileall → import smoke → `app check` on scratch DB → [pytest -m smoke] → restart → /health status ok|degraded + version = new sha ≤ 60 s
        any failure → rollback to previous sha + restart + Telegram

 data/taosentinel.db ──VACUUM INTO──► data/backups/taosentinel-<stamp>.db (+ .db.gz + .json manifest)
                                        └─ copy .db.gz + .json ─► /mnt/taosentinel-mirror/ (only if marker file present)
 Remote access: Windows PC ─Tailscale─► tailscale serve ─► 127.0.0.1:3001 (UI) / 127.0.0.1:8090 (API); UFW drops all other inbound
```

### 2.2 Laptop preparation (`ops/laptop-prep.sh`)

Run once as root: `sudo ops/laptop-prep.sh --user <login>` (flags: `--dry-run` prints commands only; `--skip <n,n>`; `--mirror-label TSMIRROR`). Every step first checks current state and is a no-op when already satisfied; the script stops at the first failing step and prints a summary table `step | state (ok/changed/skipped/FAILED)`. Ubuntu **24.04 LTS** is recommended (ships `python3.12`); on 22.04 step 1 first adds `ppa:deadsnakes/ppa`. Step 0 ports the old `install.sh` 1-80 prerequisite checks (OS/arch detect, `check_cmd git/curl`, Python gate) with the gate changed to **exactly 3.12** (`>=3.12,<3.13`, matching `pyproject.toml`).

| # | Step | Commands / file contents | Why |
|---|---|---|---|
| 1 | Packages | `apt-get install -y python3.12 python3.12-venv python3.12-dev git sqlite3 curl jq openssl ufw logrotate build-essential unattended-upgrades` | runtime; gate tools (`curl`, `jq`, `flock` from util-linux) |
| 2 | UTC clock | `timedatectl set-timezone Etc/UTC` | every cron time in these specs is UTC; unattended-upgrades' reboot time is *local* time |
| 3 | Time sync before start | `timedatectl set-ntp true`; `systemctl enable --now systemd-timesyncd`; `systemctl enable systemd-time-wait-sync.service`; drop-in `/etc/systemd/system/systemd-time-wait-sync.service.d/50-timeout.conf` = `[Service]` / `TimeoutStartSec=180` | `time-sync.target` only waits for NTP when `systemd-time-wait-sync` is enabled; the 180 s cap lets an offline boot proceed on the RTC clock |
| 4 | Never sleep | `systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target`; `/etc/systemd/logind.conf.d/50-taosentinel.conf` = `[Login]` / `HandleLidSwitch=ignore` / `HandleLidSwitchExternalPower=ignore` / `HandleLidSwitchDocked=ignore` / `HandleSuspendKey=ignore` / `HandleHibernateKey=ignore` / `IdleAction=ignore`; if a GNOME session exists: `sudo -u <login> dbus-launch gsettings set org.gnome.settings-daemon.plugins.power sleep-inactive-ac-type nothing` (and `sleep-inactive-battery-type nothing`) | lid close / idle must never suspend (checklist A1) |
| 5 | Battery → clean shutdown | `/etc/UPower/UPower.conf`: `UsePercentageForPolicy=true`, `PercentageLow=20`, `PercentageCritical=8`, `PercentageAction=5`, `CriticalPowerAction=PowerOff`; `systemctl restart upower` | on a long power cut the battery is the UPS; UPower's default `HybridSleep` (→ `Hibernate` → `PowerOff` fallback chain) depends on logind reporting the masked sleep targets as unavailable (**UNVERIFIED** per release), so the action is set explicitly to a clean power-off |
| 6 | Unattended boot | `/etc/default/grub`: `GRUB_TIMEOUT=3`, `GRUB_RECORDFAIL_TIMEOUT=5`; `update-grub`. Operator, in BIOS/UEFI (vendor-specific): "Power on with AC attach" / "AC power recovery = On". No LUKS passphrase prompt on the system disk (§11). | Ubuntu's recordfail logic otherwise waits **forever** at the GRUB menu after an unclean shutdown; after the battery empties the laptop must power on by itself when AC returns |
| 7 | Network without login | Prefer Ethernet. Wi-Fi as a system connection: `nmcli connection modify "<SSID>" connection.permissions "" 802-11-wireless-security.psk-flags 0 802-11-wireless-security.psk "<password>"` (the PSK must be re-entered: a keyring-owned secret is not copied when the flag changes; it then lives in `/etc/NetworkManager/system-connections/`, mode 600); confirm `systemctl is-enabled NetworkManager-wait-online.service` = `enabled` | a Wi-Fi password kept in the user keyring only connects after a desktop login |
| 8 | Unattended upgrades | `/etc/apt/apt.conf.d/52taosentinel`: `Unattended-Upgrade::Automatic-Reboot "true";` `Unattended-Upgrade::Automatic-Reboot-WithUsers "true";` `Unattended-Upgrade::Automatic-Reboot-Time "04:30";`; `dpkg-reconfigure -f noninteractive unattended-upgrades` | security patches; reboot at 04:30Z = after the 03:30Z backup, before the 06:15Z scoring run |
| 9 | Swap ≥ 4 GiB | if `swapon --show --bytes` total < 4 GiB: create an **additional** file `/swapfile-ts` of `ceil((4 GiB − current)/1 GiB)` GiB (never re-`fallocate` Ubuntu's own active `/swapfile` or `/swap.img` — that fails with "Text file busy"): `fallocate -l <n>G /swapfile-ts && chmod 600 /swapfile-ts && mkswap /swapfile-ts && swapon /swapfile-ts`, append `/swapfile-ts none swap sw 0 0` to `/etc/fstab`; `/etc/sysctl.d/60-taosentinel.conf` = `vm.swappiness=10`; `sysctl --system` | `VACUUM INTO`, gzip and a Next.js build can spike memory on an 8 GB laptop |
| 10 | Firewall | `ufw default deny incoming`; `ufw default allow outgoing`; `ufw allow in on tailscale0`; optional LAN SSH `ufw allow from 192.168.0.0/24 to any port 22 proto tcp`; `ufw --force enable` | API/UI bind to loopback anyway; UFW's `before.rules` already accept `lo` |
| 11 | Tailscale | `curl -fsSL https://tailscale.com/install.sh \| sh`; `tailscale up`; operator: admin console → this machine → **Disable key expiry**. Optional exposure — **only after `API_AUTH_TOKEN` is set** (02 §I2; §2.14): `tailscale serve --bg --https=443 http://127.0.0.1:3001` and `tailscale serve --bg --https=8443 http://127.0.0.1:8090` (flag syntax differs between Tailscale releases — **UNVERIFIED** for the installed version; check `tailscale serve --help`) | remote read access from the Windows PC with no open ports; node keys otherwise expire (default 180 d) and the laptop silently drops off the tailnet |
| 12 | Journald cap | `/etc/systemd/journald.conf.d/50-taosentinel.conf` = `[Journal]` / `SystemMaxUse=500M` / `MaxRetentionSec=30day`; `systemctl restart systemd-journald` | bounded disk |
| 13 | Mirror mount (optional) | USB disk formatted ext4, label `TSMIRROR`; `/etc/fstab`: `LABEL=TSMIRROR /mnt/taosentinel-mirror ext4 nofail,noatime,x-systemd.device-timeout=10s 0 2`; `mkdir -p /mnt/taosentinel-mirror && mount -a`; `chown <login>: /mnt/taosentinel-mirror && chmod 700 /mnt/taosentinel-mirror`; `sudo -u <login> touch /mnt/taosentinel-mirror/.taosentinel-mirror`. NAS alternative: a CIFS/NFS fstab line with `nofail,x-systemd.automount`. | off-device copy; the **marker file** is how the backup job tells a mounted mirror from an empty mount point |
| 14 | Journal read access | `usermod -aG systemd-journal <login>` | `notify-failure.sh` and the digest read `journalctl -u taosentinel` without sudo |

### 2.3 User, paths, permissions

`SENTINEL_USER` is the operator's normal login (not a dedicated system user): the operator runs git, the CLI, seed edits and manual entry as the same user, and the unit's sandboxing (§2.4) provides the isolation. `SENTINEL_ROOT` defaults to `/home/$SENTINEL_USER/taosentinel`. The unit sets `UMask=0077`, so every file the service creates is `600`/`700`.

| path | mode | written by | content |
|---|---|---|---|
| `~/taosentinel/` | 755 | git, operator | repo, `.venv/`, `pyproject.toml`, `requirements.lock` |
| `~/taosentinel/.env` | **600** | operator | Settings (spec 01 §5) incl. app secrets; never committed |
| `~/taosentinel/{KILL_SWITCH,SCORING_KILLED,PAUSE_ALL}` | 600 | operator (`touch`) only | 02 §B12's **closed** sentinel list (path keys `KILL_SWITCH_PATH`, `SCORING_KILL_SWITCH_PATH`, `PAUSE_ALL_PATH`); gitignored; the service reads them and cannot write them (`ProtectHome=read-only`). Per-provider kill files (`X_KILLED`, `GITHUB_KILLED`, `COMMUNITY_PAUSED`, …) are retired: use `data/control/<group>.paused` (§2.11) |
| `~/taosentinel/DEPLOY_PAUSED` | 600 | operator (`touch`) only | input of the external `deploy/auto-update.sh` only (§2.9); never read by the app, so outside B12's list; gitignored |
| `~/taosentinel/data/` | 700 | service | gitignored runtime root (`DATA_DIR`) |
| `data/taosentinel.db` (+`-wal`, `-shm`) | 600 | service | the database (`DB_PATH`) |
| `data/logs/YYYY-MM-DD.jsonl[.N]` | 600 | service | structured logs (spec 01 §2.10) |
| `data/backups/taosentinel-<stamp>.{db,db.gz,json}` | 600 | `backup_daily` | nightly snapshots + manifest |
| `data/backups/manual/taosentinel-<stamp>-<tag>.{db,db.gz,json}` | 600 | CLI / auto-update | tagged snapshots (`predeploy-<sha7>`, `pre-migration`, `compact`) |
| `data/run/` | 700 | service, auto-update | `taosentinel.lock` (instance flock), `heartbeat.json`, `backup.lock`, `deploy.lock`, `deploy-state.json`, `deploy-history.jsonl`, `deploy-pat.json`, `penalty-<provider>.json`, `integrity.json` (§2.6 step 8) |
| `data/control/<target>.paused` | 600 | operator (shell) only | emergency per-target pause input (immediate, needs neither API nor DB); the app reads these files and **never creates or deletes them** (02 §B12); the persisted operator pause is the `control_state` row (§2.11) |
| `data/cache/` | 700 | service | `XDG_CACHE_HOME` for libraries that cache under `~/.cache` (read-only for the unit) |
| `data/tmp/` | 700 | service, gate | `SQLITE_TMPDIR`, restore staging, gate scratch DBs |
| `data/quarantine/<utc-stamp>/` | 700 | restore CLI | the DB (+wal/shm) moved aside by a restore — never auto-deleted |
| `data/service.log`, `data/frontend.log` | 600 | systemd `append:` | stdout/stderr (logrotate, §2.13.6) |
| `~/taosentinel-secrets/` | **700** | operator | outside the repo |
| `~/taosentinel-secrets/deploy.env` | **600** | operator | auto-update secrets/config (§5.3); never read by the Python service |
| `~/taosentinel-secrets/old-pi.env` | 600 | operator | archived old-bot `.env` (checklist A3) |
| `/mnt/taosentinel-mirror/` (+`.taosentinel-mirror`) | 700 | `backup_daily` | mirror copies; must not live under `/home` (`ProtectHome=read-only`) |

`.gitignore` additions to spec 01's list: `PAUSE_ALL DEPLOY_PAUSED` (`data/control/` is covered by `data/`).

### 2.4 systemd units (verbatim; `@…@` placeholders rendered by `install-systemd.sh`)

**`ops/systemd/taosentinel.service`** — port of `subnettrader.service` 1-52: every directive kept, paths parameterised, hardening extended.

```ini
[Unit]
Description=TaoSentinel subnet scoring service (FastAPI on 127.0.0.1:8090)
Documentation=file://@SENTINEL_ROOT@/specs/16-ops-deploy-and-observability.md
After=network-online.target time-sync.target
Wants=network-online.target
# Crash recovery rate limit: 5 restarts in 60 s, then give up (the OnFailure notifier fires).
StartLimitIntervalSec=60
StartLimitBurst=5
OnFailure=taosentinel-notify-failure@%n.service

[Service]
# Watchdog: the app sends WATCHDOG=1 every WATCHDOG_USEC/2 (60 s) only while _jobs_live()
# is true (spec 01 §2.8 + spec 16 §2.5). A withheld ping -> SIGABRT -> Restart=on-failure.
# A watchdog restart recurs no faster than ~120 s apart, so it never trips the 5-in-60s limit.
Type=notify
NotifyAccess=main
WatchdogSec=120
# READY=1 is sent after the scheduler starts; nothing slow (backup, quick_check) precedes it.
TimeoutStartSec=120
User=@SENTINEL_USER@
Group=@SENTINEL_GROUP@
WorkingDirectory=@SENTINEL_ROOT@
Environment=TAOSENTINEL_ROOT=@SENTINEL_ROOT@
Environment=PYTHONUNBUFFERED=1
Environment=SQLITE_TMPDIR=@SENTINEL_ROOT@/data/tmp
# ~/.cache is read-only under ProtectHome=read-only; libraries that cache there write here instead.
Environment=XDG_CACHE_HOME=@SENTINEL_ROOT@/data/cache
ExecStartPre=/usr/bin/mkdir -p @SENTINEL_ROOT@/data/logs @SENTINEL_ROOT@/data/backups/manual @SENTINEL_ROOT@/data/run @SENTINEL_ROOT@/data/control @SENTINEL_ROOT@/data/tmp @SENTINEL_ROOT@/data/quarantine @SENTINEL_ROOT@/data/cache
ExecStart=@SENTINEL_ROOT@/.venv/bin/python -u -m app serve
# append: needs systemd >= 240 and an existing data/ directory (the installer creates it).
StandardOutput=append:@SENTINEL_ROOT@/data/service.log
StandardError=append:@SENTINEL_ROOT@/data/service.log
Restart=on-failure
RestartSec=10
# Graceful shutdown: the app owns SIGTERM (uvicorn capture_signals neutralised), closes the DB
# under SHUTDOWN_DEADLINE_SEC=10 and _hard_exit(0)s; 30 s is only the outer guard.
TimeoutStopSec=30
KillSignal=SIGTERM
UMask=0077
# Hardening. The service writes only under data/ (+ the mirror, which lives outside /home).
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=read-only
ReadWritePaths=@SENTINEL_ROOT@/data
@MIRROR_RW_LINE@
PrivateTmp=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
RestrictRealtime=true
LockPersonality=true
MemoryMax=2G
TasksMax=512

[Install]
WantedBy=multi-user.target
```
`@MIRROR_RW_LINE@` renders to `ReadWritePaths=-<SENTINEL_MIRROR_DIR>` when a mirror is configured, else to an empty line. With `ProtectHome=read-only` the process can write **only** under `data/` (and the mirror): Python cannot write `__pycache__` under `app/` at runtime (silently skipped; `install-systemd.sh` and `auto-update.sh` run `compileall` as the user instead), and the service can never create or delete repo-root sentinels (`./KILL_SWITCH`, `./SCORING_KILLED`, `./PAUSE_ALL`). It writes no pause file anywhere (02 §B12): its only self-imposed pause, the integrity hold, is derived from `data/run/integrity.json` (§2.6 step 8, §2.11). `MemoryMax=2G` assumes spec 13's in-process `quant_weekly` job stays off (its default); enabling it on a year-old DB may need `MemoryMax=3G` (§11).

**`ops/systemd/taosentinel-notify-failure@.service`** — new; fires when the main unit enters `failed` (start limit hit, or a failure `Restart=` does not cover). Whether a unit passing through `activating (auto-restart)` also triggers `OnFailure=` differs between systemd releases (**UNVERIFIED** for 249/255); if it does, the operator gets one extra message per crash, which is acceptable:

```ini
[Unit]
Description=TaoSentinel failure notifier for %i

[Service]
Type=oneshot
User=@SENTINEL_USER@
Group=@SENTINEL_GROUP@
Environment=SENTINEL_ROOT=@SENTINEL_ROOT@
ExecStart=/usr/bin/env bash @SENTINEL_ROOT@/ops/notify-failure.sh %i
TimeoutStartSec=30
```
`ops/notify-failure.sh <unit>` reads `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` from `$SENTINEL_ROOT/.env` with the `kv()` parser of §2.9; collects `systemctl show <unit> -p Result -p NRestarts -p ExecMainStatus -p ActiveEnterTimestamp`, the last 15 lines of `journalctl -u <unit> --no-pager` and of `data/service.log` (HTML-escaped, ≤ 3,500 chars); sends `🛑 <b>TaoSentinel unit FAILED</b> (<unit>) result=… restarts=…` + a `<pre>` tail; always exits 0.

**`ops/systemd/taosentinel-frontend.service`** (Phase 11) — port of `subnettrader-frontend.service` 1-29. `Wants=` (not `Requires=`) so a backend crash never tears the UI down; npm replaced by a direct `node` exec so SIGTERM reaches Next.js and npm never writes `~/.npm/_logs`; logs moved out of `/tmp` (which is private per unit).

```ini
[Unit]
Description=TaoSentinel UI (Next.js on 127.0.0.1:3001)
After=taosentinel.service
Wants=taosentinel.service
StartLimitIntervalSec=60
StartLimitBurst=5

[Service]
Type=simple
User=@SENTINEL_USER@
Group=@SENTINEL_GROUP@
WorkingDirectory=@SENTINEL_ROOT@/frontend
# The installer rewrites @NODE_DIR@ to the node bin dir the user's login shell resolves.
# Re-run sudo ops/install-systemd.sh --with-frontend after a node upgrade.
Environment=PATH=@NODE_DIR@:/usr/local/bin:/usr/bin:/bin
Environment=NODE_ENV=production
Environment=NEXT_TELEMETRY_DISABLED=1
# Equivalent to `npm run start` (package.json "next start -p 3001") plus a loopback bind.
ExecStart=@NODE_DIR@/node @SENTINEL_ROOT@/frontend/node_modules/next/dist/bin/next start -p 3001 -H 127.0.0.1
StandardOutput=append:@SENTINEL_ROOT@/data/frontend.log
StandardError=append:@SENTINEL_ROOT@/data/frontend.log
Restart=on-failure
RestartSec=10
TimeoutStopSec=15
UMask=0077
NoNewPrivileges=true
ProtectSystem=full
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

**`deploy/taosentinel-update.service`** — port of `subnettrader-update.service` 1-16. **No** `NoNewPrivileges` (the script needs `sudo -n systemctl restart`).

```ini
[Unit]
Description=TaoSentinel git auto-deploy (fetch, gate, restart on change, rollback on failure)
Documentation=file://@SENTINEL_ROOT@/specs/16-ops-deploy-and-observability.md
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=@SENTINEL_USER@
Group=@SENTINEL_GROUP@
WorkingDirectory=@SENTINEL_ROOT@
Environment=SENTINEL_ROOT=@SENTINEL_ROOT@
Environment=SENTINEL_SECRETS=@SENTINEL_SECRETS@
Environment=PATH=@NODE_DIR@:/usr/local/bin:/usr/bin:/bin
# Defense-in-depth against leaking the token-bearing fetch URL into journald.
Environment=GIT_TRACE=0 GIT_TRACE_PACKET=0 GIT_TRACE_CURL=0 GIT_CURL_VERBOSE=0
ExecStart=/usr/bin/env bash @SENTINEL_ROOT@/deploy/auto-update.sh
# Bound a hung fetch / pip / build so a stuck run cannot block the next tick.
TimeoutStartSec=900
Nice=10
```

**`deploy/taosentinel-update.timer`** — port of `subnettrader-update.timer`; 2 min → 5 min, jittered.

```ini
[Unit]
Description=Check GitHub for TaoSentinel updates every 5 minutes

[Timer]
OnBootSec=5min
OnUnitActiveSec=5min
RandomizedDelaySec=30
AccuracySec=15s
Unit=taosentinel-update.service

[Install]
WantedBy=timers.target
```

**`ops/install-systemd.sh`** — port of the old installer 1-41, parameterised and idempotent:

```
sudo [SENTINEL_USER=<login>] [SENTINEL_ROOT=<path>] [SENTINEL_SECRETS=<path>] [SENTINEL_MIRROR_DIR=<path>] \
     ops/install-systemd.sh [--with-frontend] [--with-update-timer] [--render-only <outdir>] [--uninstall]
```
Steps (`set -euo pipefail`; `--render-only` skips steps 1 and 8-11 and writes the rendered files into `<outdir>` without root — used by tests):
1. Require root (`$EUID -eq 0`), else print `Must run as root (try: sudo $0)` and exit 1 (old 9-12).
2. Resolve `SENTINEL_USER=${SENTINEL_USER:-$SUDO_USER}` (refuse empty or `root`), `SENTINEL_GROUP=$(id -gn "$SENTINEL_USER")`, `SENTINEL_ROOT=${SENTINEL_ROOT:-<repo root from script dir>}` (old 6-7), `SENTINEL_SECRETS=${SENTINEL_SECRETS:-/home/$SENTINEL_USER/taosentinel-secrets}`.
3. Validate: `$SENTINEL_ROOT/.venv/bin/python -c 'import sys; assert sys.version_info[:2]==(3,12)'`; `.env` exists; `SENTINEL_MIRROR_DIR` (if set) is absolute and not under `/home`.
4. `install -d -m 700 -o "$SENTINEL_USER" -g "$SENTINEL_GROUP"` for `data/` and every §2.3 subdirectory; `chown "$SENTINEL_USER": .env && chmod 600 .env`; if `$SENTINEL_SECRETS/deploy.env` exists: `chmod 700 "$SENTINEL_SECRETS" && chmod 600 "$SENTINEL_SECRETS/deploy.env"`.
5. `sudo -u "$SENTINEL_USER" "$SENTINEL_ROOT/.venv/bin/python" -m compileall -q "$SENTINEL_ROOT/app" "$SENTINEL_ROOT/scripts"`.
6. With `--with-frontend` / `--with-update-timer`: `NODE_BIN="$(sudo -u "$SENTINEL_USER" bash -lc 'command -v node' || true)"` (old 15-21) — required for the frontend (exit 1 with the old message if empty), optional for the timer (`@NODE_DIR@` falls back to `/usr/bin`).
7. Render with one `sed` per file (`s#@SENTINEL_USER@#…#g`, `@SENTINEL_GROUP@`, `@SENTINEL_ROOT@`, `@SENTINEL_SECRETS@`, `@NODE_DIR@`, `@MIRROR_RW_LINE@`); fail if any placeholder remains (`grep -q '@[A-Z_]\+@'`).
8. `install -m 644` the rendered units into `/etc/systemd/system/` (`taosentinel.service`, `taosentinel-notify-failure@.service`; frontend / update units only with their flags).
9. Sudoers `/etc/sudoers.d/taosentinel` (mode 440), validated with `visudo -cf <tmp>` before `mv`: `<SENTINEL_USER> ALL=(root) NOPASSWD: /usr/bin/systemctl restart taosentinel, /usr/bin/systemctl restart taosentinel-frontend`.
10. Logrotate `/etc/logrotate.d/taosentinel`: `<root>/data/service.log <root>/data/frontend.log { daily; rotate 14; maxsize 50M; missingok; notifempty; compress; delaycompress; copytruncate; su <user> <group> }` (`copytruncate` because systemd holds the `append:` fd open; `create` is omitted because logrotate ignores it with `copytruncate`; the rotated copies inherit mode 600 from the source).
11. `systemctl daemon-reload`; `systemctl enable taosentinel.service` (+ `taosentinel-frontend.service` / `taosentinel-update.timer` with their flags). It **does not start** anything.
12. Print next steps: `sudo systemctl start taosentinel`, `systemctl status taosentinel`, `journalctl -u taosentinel -f`, `tail -f data/service.log` (old 36-41).

`--uninstall`: `systemctl disable --now` all units and the timer, remove unit, sudoers and logrotate files, `daemon-reload`; never touches `data/`.

### 2.5 Liveness rules (canonical per 02 §E4; spec 01 implements them)

The max-age rules below are canonical (02 §E4); spec 01 implements them in `_jobs_live()`, `systemd_watchdog_loop()` (verbatim port of `ops-chain-resilience.md` 160-227 / `main.py` 3364-3382) and `effective_liveness_max_age() = liveness_max_age_sec or (interval_min*60*2 + timeout_sec + 120)`. A job counts as stale only when it is enabled, not paused (`paused_reason() is None`), not `manual`, and `now − last_run > max-age` (02 §E4). Warm-up: a job whose `last_run is None` is measured from the process start (`rt.started_at`), so a scheduler that never fires anything still trips after one max-age instead of being skipped forever. Five amendments to spec 01, implemented in `app/jobs.py` / `app/runtime.py` by the spec-01 implementer:

1. **Cron jobs** (`backup_daily`, `ops_digest_daily`, `scoring_6h`, `discord_6h`, …): `interval_min()` returns the largest gap between consecutive fire times among the trigger's next 8 fire times (daily → 1,440; hours `0,6,12,18` → 360; weekly → 10,080; monthly → ≤ 44,640), so the formula applies unchanged. `manual` triggers (02 §E1: `IntervalTrigger(weeks=520)`, `next_run_time=None`) are never liveness-checked.
2. **Pause-aware tasks:** `LongTask` gains `paused_reason()` with the §2.11 precedence; `_jobs_live()` skips any job **or task** whose `paused_reason()` is not `None` (spec 01 checked only `task.enabled()`). Required so `PAUSE_ALL` cannot cause a restart loop.
3. **API task:** spec 01 `main()` stores its `health_task` as `rt.task_handles["health"]`; if that task is `done()` (uvicorn exited or crashed) `_jobs_live()` returns `False` — the process restarts instead of running headless.
4. **Optional self-probe** (`WATCHDOG_API_SELF_PROBE_ENABLED`, default `True`): on each watchdog tick (60 s) a raw `httpx.AsyncClient` (no limiter, 5 s timeout) GETs `http://127.0.0.1:{HEALTH_PORT}/health`; `WATCHDOG_API_PROBE_FAILS` (3) consecutive failures → not live. Answers spec 14 open question 4.
5. **Attempt-based task liveness:** `LongTask` gains `last_attempt: datetime | None`, set by `run_forever()` after every `run_once()` return or exception (before the backoff sleep) and by every REST-fallback poll *attempt*, successful or not; `_jobs_live()` compares `max(last_beat, last_attempt)` with `liveness_max_age_sec`. Reason: with beats only on received events, a TMC outage (SSE and REST both failing) makes `chain_sse` stale after 900 s and systemd would restart the whole process every ≈ 15 min, killing long jobs mid-run (e.g. spec 11's `owner_collect_daily`, up to 3 h) — and a restart cannot fix a provider outage. `backoff_max_sec` (300) < 900 keeps a supervisor that is still looping live; a wedged loop (no events, no attempts) still trips.

Liveness uses `last_run` (last completed **attempt**), never `last_ok`: a provider outage is not a stall and a restart would not fix it (it surfaces as `collector_failing`). `chain_sse` declares `liveness_max_age_sec = 900` (spec 05) and calls `beat()` on every SSE event and every successful REST-fallback poll (data freshness, shown on `/health`), while amendment 5's `last_attempt` is what keeps it live through an outage.

**Liveness table** (the one place these numbers live; 01 mirrors it, owners' own `liveness_max_age_sec` values win). Sibling examples use the owner rows as of 2026-09-23 — recompute from the final `JOB_TABLE`:

| kind | clock compared | max-age | examples (s) |
|---|---|---|---|
| interval job | `last_run` (last completed attempt, ok or not) | owner's `liveness_max_age_sec`, else `interval_min·60·2 + timeout_sec + 120` | `ops_guard_5m` 780 (600 + 60 + 120) · `cost_ledger_hourly` 7,440 · `discord_widget_1h` 7,920 · `features_hourly` 8,520 · 06 `github_resolve` (6 h, 1,800 s) 45,120 |
| cron job | `last_run` | same formula, `interval_min()` per amendment 1 | `scoring_6h` 360 min → 43,200 + timeout + 120 ≈ 12.4 h · `discord_6h` 45,120 (declared by 08) · `backup_daily` 178,320 (≈ 49.5 h) · `ops_digest_daily` 173,040 · other daily crons ≈ 48–51 h · weekly ≈ 14 d (never trip in practice) |
| `manual` job (02 §E1; 17's backfills) | — | exempt | `backfill_tmc`, `backfill_github` |
| LongTask with declared max-age | `max(last_beat, last_attempt)` (amendment 5) | owner's `liveness_max_age_sec` | `chain_sse` 900 (05) · `discord_gateway` 600 (08) |
| LongTask without declared max-age | — | not liveness-gated (a provider outage must never restart the process) | `telegram_bot` (14) |
| API task | `task_handles["health"].done()` + self-probe (amendments 3–4) | 3 consecutive probe failures (≈ 3 min) | — |
| disabled / paused | — | skipped | any job under `PAUSE_ALL`, the integrity hold or a budget pause |
| never ran since start (`last_run is None`) | `rt.started_at` | the job's normal max-age (warm-up is not a skip) | a scheduler that never fires trips after one max-age |

Every withheld ping is logged at `error` as `watchdog_withheld` with the stale ids and counted for the digest. `WatchdogSec=120` with pings every 60 s tolerates one missed tick.

### 2.6 Power-loss detection and recovery order

**Instance lock.** `app/ops/recovery.py::pre_init(rt)` runs first in `init_services()` (before `Database.connect`). It opens `data/run/taosentinel.lock` and takes `fcntl.flock(fd, LOCK_EX | LOCK_NB)`, holding the fd for the process lifetime; failure → log `another instance holds data/run/taosentinel.lock` and `sys.exit(3)`. This forbids a manual `python -m app serve` next to the systemd instance (two writers, doubled TMC traffic). `python -m app check/jobs/run-job` and scripts do **not** take the lock.

**Heartbeat.** `data/run/heartbeat.json` = `{"pid", "boot_id", "started_at", "ts", "version"}` (`started_at`/`ts` are unix seconds; `boot_id` from `/proc/sys/kernel/random/boot_id`), written atomically (`tmp` + `os.replace`) right after READY and then by every `ops_guard_5m` run; **deleted** by `shutdown_services()` as its last step before `_hard_exit(0)` (spec 01 amendment: one `Path.unlink(missing_ok=True)`).

**Boot classification** (in `pre_init`, after the lock is held):

| previous `heartbeat.json` | `boot_id` vs now | kind | meaning |
|---|---|---|---|
| absent, boot log empty | — | `first_boot` | fresh install |
| absent | — | `clean` | previous process shut down cleanly (SIGTERM path) |
| present | same | `process_crash` | SIGKILL, OOM, watchdog abort, `_hard_exit` deadline |
| present | different | `host_unclean` | power cut, kernel panic, hard reset, battery died |

`downtime_sec = now − prev.ts` (upper bound; error ≤ `OPS_GUARD_INTERVAL_MIN`). The boot is appended to `job_state(job='_ops', key='boots')` — a JSON list capped at 30 entries of `{at, kind, prev_ts, downtime_sec, version, orphan_runs}` (`at`/`prev_ts` unix seconds; `orphan_runs` = `collector_runs` rows with `finished_at IS NULL AND started_at < boot_at`, counted after `apply_schema`; spec 01 leaves those rows untouched).

**Recovery order** (who does what, in order, after any boot):

| # | Stage | Actor | Detail |
|---|---|---|---|
| 0 | Power-on, boot, fsck journal replay | firmware/OS | AC-attach power-on (BIOS), `GRUB_RECORDFAIL_TIMEOUT=5`, ext4 journal replay |
| 1 | `network-online.target`, `time-sync.target` | systemd | NetworkManager-wait-online; `systemd-time-wait-sync` capped at 180 s |
| 2 | Start `taosentinel.service` | systemd | enabled in `multi-user.target`, no login needed |
| 3 | `pre_init` | app | lock → classify → new heartbeat |
| 4 | DB open | SQLite | WAL recovery is automatic on the first connection; `apply_schema` idempotent |
| 5 | `READY=1` | app | nothing heavy before it (`TimeoutStartSec=120`) |
| 6 | Live data first | `chain_sse` LongTask | starts immediately (not staggered) |
| 7 | Staggered catch-up | scheduler | JOB_TABLE order, first at +30 s then +`JOB_START_STAGGER_SEC` each: 04 `registry_occupants` → 05 `tmc_daily` (its `analytics ?span=1W` pull repairs `chain_hourly` over the gap, spec 05) → the rest; `min_gap_factor` skips jobs whose `last_ok` is still recent, so short outages cost no API budget; a job declaring `startup_catchup=True` (02 §E1 amended: 05, 11, 12) runs once at boot when its last scheduled fire was missed while the process was down (keyed on the missed fire time, not an age threshold); `manual` jobs are never started (02 §E1) |
| 8 | Integrity check (unclean kinds only) | `recovery.post_boot_check()` | at +`DB_QUICK_CHECK_DELAY_SEC` (300 s): `PRAGMA quick_check` on a separate read-only connection (`file:<db>?mode=ro`, `uri=True`) in the background. `ok` → log and write `data/run/integrity.json` = `{at, result: "ok"}`; anything else → write `integrity.json` with the first 5 result lines, alert `db_integrity_failed` (critical, direct), runbook §7.3.9. While `integrity.json.result ≠ "ok"` the **integrity hold** is active: it pauses exactly what `PAUSE_ALL` pauses (every non-`ops_exempt` job, reported `pause_all`, §2.11) without any pause file being written (02 §B12), `/health.degraded_reasons` contains `db_integrity`, and `backup_daily` skips retention (§2.7 step 8) so pre-corruption snapshots are never rotated out. The file is deleted by a live restore (§2.8 step 6) or rewritten by `python -m app ops integrity-check` (read-only `quick_check`) |
| 9 | Startup backup | spec 01 `run_startup_backup()` | delayed by `DB_BACKUP_STARTUP_DELAY_SEC` (600 s); only if the newest backup is older than `DB_BACKUP_MIN_AGE_HOURS` and step 8 (if scheduled) returned `ok` |
| 10 | Missed scores | spec 12 | 12's slot catch-up (in the boot-time `startup_catchup` run, 02 §E1, or the next scoring cron) re-scores missed `as_of` from stored raw data (`backfilled=true`) |
| 11 | Report | `recovery.report()` | for `process_crash`/`host_unclean` with `downtime_sec ≥ 60`: event `service_recovered_unclean` (warning, `source='ops'`) — kind, downtime, orphan runs, SSE gap ≈ downtime/12 blocks, missed cron fires (backup/scoring/digest) computed from the triggers between `prev.ts` and now |

**Permanently lost across an outage (documented, never back-filled by forward-fill):** per-block `chain_ticks` (hourly rows are repaired from TMC analytics), SSE deltas, daily web checks, Discord/Telegram count snapshots, X posts older than the provider's since-id window. Point-in-time rule: a missing observation stays missing.

### 2.7 Backups

**Job.** `backup_daily` (`app.storage.backup:BackupJob`; row in §6.1, which 01's `JOB_TABLE` mirrors, 02 §E2): **cron `hour=DB_BACKUP_CRON_HOUR minute=DB_BACKUP_CRON_MINUTE` UTC (03:30Z, 02 §E3)**, `misfire_grace_sec=3600`, `min_gap_factor=0`, `timeout_sec=5400`, `ops_exempt=True` (runs under `PAUSE_ALL` and the integrity hold). Never in the start path: the only boot-time backup is spec 01's backgrounded `run_startup_backup()` (old `main.py` 323-340 guard, plus the §2.6 step-9 delay). Manual: `python -m app backup now [--tag T]` (same code path, may run while the service runs; spec 03 ceded its `db backup`/`db restore` spellings to this CLI). A CLI backup never calls `connect()`/`apply_schema()` on the live file (old `scripts/backup_db.py:35` rule: `backup_to` opens its own connection) and never takes the instance lock.

**Pipeline** — `app/storage/backup.py::create_backup(db, *, backup_dir, mirror_dir, retention, weekly_retention, compress, max_total_gb, tag=None) -> BackupResult` (spec 01's signature extended; all keyword-only and defaulting from Settings; `BackupResult.as_dict()` returns the `{name, path, size_bytes, integrity, mirror_error, pruned}` dict spec 01 §3 callers expect):

1. **Lock:** `fcntl.flock(data/run/backup.lock, LOCK_EX|LOCK_NB)` (the same flock(2) lock `auto-update.sh` tests with `flock -n`); busy → `BackupBusy` → `BackupJob` returns `JobOutcome(rows=0, note="busy")` (not a failure). Delete `*.tmp` leftovers older than 1 h in the backup and mirror dirs (the verbatim `backup_to` writes `<dest>.tmp`, i.e. `<name>.db.tmp`, 02 §I1; gzip and mirror copies also end in `.tmp`).
2. **Wait for heavy writers:** sleep 30 s first (so a job fired in the same scheduler second is registered), then while any job id in `DB_BACKUP_WAIT_FOR_JOBS` (default `storage_retention_daily`, cron 03:00Z, which runs every retention policy including the `chain_ticks` prune — there is no `chain_prune` job, 02 §E3) is in flight, poll every 10 s up to `DB_BACKUP_WAIT_MAX_SEC` (1,200); then proceed anyway (logged) — pruning first keeps the snapshot small and avoids WAL contention (the old bot phased its retention 12 h away from the backup for the same reason, `main.py` 1522-1524). *In flight* = inside the service: the job's wrapper is executing (`rt.jobs[id]` running flag); from the CLI: a `collector_runs` row for that job with `finished_at IS NULL AND started_at > now − timeout_sec`.
3. **Preflight** (disk guard; spec 03 ceded the key to this spec): `need = (size(db) + size(db-wal)) × DB_BACKUP_MIN_FREE_FACTOR (1.3) + (0.5 × size(db) if DB_BACKUP_COMPRESS else 0) + DISK_FREE_MIN_GB·2³⁰` (spec 03 §6: gzip output ≈ 40–50 % of the DB, written while the `.db` still exists); if `shutil.disk_usage(backup_dir).free < need`: run retention with the minimum set (newest 2 stamps) and re-check; still short → raise `BackupError("insufficient_disk: need X GB, free Y GB")`.
4. **Snapshot:** `name = f"taosentinel-{stamp}"` (+ `f"-{tag}"`; `stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(now_ts()))` (`now_ts` from `app/util/time.py`, 02 §B1), lexicographic = chronological, old `backup.py` 21-24); `await db.backup_to(dir / f"{name}.db")` — spec 03's primitive, a verbatim port of old `db.py` 38-80: dedicated connection → `VACUUM INTO '<dir>/<name>.db.tmp'` (old `db.py:53`) → `PRAGMA quick_check` on the tmp file → `os.replace` into place; integrity ≠ `ok` → tmp deleted, `RuntimeError`. Tagged snapshots go to `backups/manual/`.
5. **Manifest** `name.json` (atomic): `{name, stamp, tag, created_at, app_version, spec_version, schema_version (MAX(version) FROM schema_version of the snapshot, read-only connection), size_bytes, sha256 (of the .db, streamed in asyncio.to_thread), quick_check: "ok", gz: {size_bytes} | null, counts: {scores, metric_values, events, collector_runs} (a table that does not exist yet in an early phase → `null`), duration_sec}`; `created_at` is unix seconds.
6. **Compress** (`DB_BACKUP_COMPRESS=True`): stream `.db` → `<name>.db.gz.tmp` (1 MiB chunks) with `gzip.open(level=DB_BACKUP_GZIP_LEVEL)`, `os.fsync`; verify by streaming decompression and comparing SHA-256 with the manifest; `os.replace` → `name.db.gz`; update manifest `gz`. Runs in `asyncio.to_thread` (CPU ≈ 30–120 s per GB; the event loop is never blocked). A gzip failure keeps the verified `.db` as the day's backup and logs a warning.
7. **Mirror** (`DB_BACKUP_MIRROR_DIR` non-empty): the directory must exist **and** contain `DB_BACKUP_MIRROR_MARKER` (`.taosentinel-mirror`); otherwise `mirror_error = "mirror_missing"` → alert `backup_mirror_missing`. The mirror root is **never** `mkdir`-ed (old `backup.py:77` did, silently writing to the internal disk when the USB was unmounted). Copy `name.db.gz` (or `.db` when compression is off) and `name.json` via `.tmp` + `os.fsync` + `os.replace`; size mismatch or `OSError` → `mirror_error=str(exc)` → alert `backup_mirror_failed`. A mirror problem never fails the job (old semantics).
8. **Retention** `apply_retention(dir, daily=DB_BACKUP_RETENTION, weekly=DB_BACKUP_WEEKLY_RETENTION, keep_uncompressed=DB_BACKUP_KEEP_UNCOMPRESSED)`: group files by stamp; keep K = the newest `daily` stamps ∪ the newest stamp of each of the newest `weekly` ISO weeks (`date.isocalendar()[:2]`); delete every file of every other stamp; inside K delete the `.db` of all but the newest `keep_uncompressed` stamps when a verified `.db.gz` exists (spec 13's quant snapshot reads the newest uncompressed `.db`). Same on the mirror (no uncompressed copies there). `backups/manual/` keeps the newest `DB_BACKUP_MANUAL_RETENTION`. Then the size budget (local dir: `DB_BACKUP_MAX_TOTAL_GB`, default 12; mirror: the separate `DB_BACKUP_MIRROR_MAX_TOTAL_GB` — 02 §F2; `0` = no size budget, i.e. only the 7 + 8 rotation): while total bytes > budget and more than 2 stamps remain, delete the oldest weekly-only stamp, then the oldest daily; the 2 newest stamps are never deleted (logged `backup_budget_pruned`, counted in the digest). **Skipped entirely** while `data/run/integrity.json` reports a non-`ok` result (§2.6 step 8), so a corrupt-but-VACUUMable DB can never rotate the good snapshots out. A retention error is logged (`backup_retention_failed`) and does not fail the job.
9. **Record:** `job_state('backup_daily','last_result')` = manifest + `{mirror: "ok"|"missing"|"failed"|"off", pruned}`; return `JobOutcome(rows=1, note=f"{name} {mb} MB mirror={…}")`.

Any exception in steps 1-6 → job failure (`ok=0`) + alert `backup_failed` (critical, `source='ops'`; payload `{error, last_good_name, last_good_age_h}` = 03 §3.9's required keys, 02 §D2, plus the extra key `backup_dir`; text ported from old `main.py` 1323-1331: `⚠️ <b>DB backup FAILED</b>\n<code>{exc}</code>\nThe SQLite DB is unprotected until this recovers.`). `latest_backup_age_hours(dir)` (old `backup.py` 27-36, glob changed to `taosentinel-*.json`, age from manifest `created_at`, fallback mtime) feeds `/health.db.last_backup_age_hours`, the spec 14 `backup` probe and `backup_stale` (> `DB_BACKUP_STALE_HOURS`).

**Size** (spec 03 §6 owns DB sizing; backup sizing is this spec's). Year-1 DB ≈ 7.8 GB at the default `CHAIN_TICKS_BLOCK_STRIDE=5`, ≈ 11 GB at stride 1 (03 §6, `CHAIN_TICKS_RETENTION_DAYS=30`; **UNVERIFIED** estimates); gzip ≈ 40–50 % of the DB (ratio **UNVERIFIED** until the first real backup). The local set never shrinks below the 2 newest stamps, the newest kept both as `.db` (spec 13's snapshot, 02 §J2) and `.db.gz` (the mirror copy) → ≈ 1.9 × DB steady; the preflight (step 3) additionally needs ≈ 1.8 × DB + 2 GB free at 03:30Z. Peak disk need ≈ **4.7 × DB + 5 GB** (live DB, WAL ≤ 1 GB, local set, preflight reserve, logs + journal ≈ 1 GB) → ≈ 42 GB at month 12 on stride 5, ≈ 57 GB on stride 1: hence **≥ 60 GB free is required** (02 §I1, amended). The 12 GB default budget (02 §F2) keeps the full 7 + 8 rotation locally only while the DB is ≲ 1.5 GB; after that the local dir holds the newest few stamps and the USB/NAS mirror (§2.2 step 13; `DB_BACKUP_MIRROR_MAX_TOTAL_GB=0` = unbudgeted) carries the rotation (≈ 14–15 stamps × 3.5 GB ≈ 50 GB of `.gz` by month 12 at stride 5). Smaller disks are unsupported (02 §I1) unless the operator lowers `CHAIN_TICKS_RETENTION_DAYS`, `DB_BACKUP_MAX_TOTAL_GB` and the weekly backup count (`DB_BACKUP_WEEKLY_RETENTION`) and adds a USB/NAS mirror; even then, e.g., 20 GB free lasts only while the DB is ≤ ≈ 3.1 GB, i.e. roughly the first 1–3 months by 03 §6's growth figures (**UNVERIFIED**); after that `backup_daily` fails its preflight (`backup_failed`, `error="insufficient_disk: …"`) until disk is added. Keep the 12 GB default on a 60 GB disk; each extra local stamp costs ≈ 0.45 × DB.

### 2.8 Restore

CLI (`app/ops/cli.py`, mounted by `app/__main__.py` as `python -m app backup …`; spec 03 ceded its `db backup` / `db restore` spellings to this CLI, so there are no aliases; one implementation, `app/storage/backup.py::restore_snapshot`):

| command | behaviour |
|---|---|
| `backup list [--mirror] [--manual]` | table: stamp · tag · forms (`db`/`gz`) · size · schema_version · age · sha recorded |
| `backup verify <stamp\|path> [--full]` | recompute SHA-256 (decompressing `.gz`) vs manifest; open a copy read-only; `PRAGMA quick_check` (`integrity_check` with `--full`); exit 0/1 |
| `backup now [--tag T]` | §2.7 pipeline; prints the manifest |
| `backup prune [--keep-daily N] [--keep-weekly M]` | retention with overrides (disk emergencies) |
| `backup restore <stamp> [--from local\|mirror] [--target PATH] [--yes]` | below |

`restore` algorithm:
1. **Refuse while the service runs:** if `flock(data/run/taosentinel.lock, LOCK_EX|LOCK_NB)` fails **or** a TCP connect to `HEALTH_HOST:HEALTH_PORT` succeeds (catches a service started without the lock, e.g. an old build), and `--target` is absent or equals `DB_PATH` → exit 2 with `stop the service first: sudo systemctl stop taosentinel`. With `--target <other path>` (drill mode) the live DB is never touched and the service may keep running.
2. **Materialise** into `data/tmp/restore-<stamp>.db` (copy `.db`, or gunzip `.db.gz`); verify SHA-256 against the manifest; `PRAGMA integrity_check` must return `ok`.
3. **Schema guard:** `MAX(version) FROM schema_version` of the snapshot `≤ CODE_SCHEMA_VERSION` (module constant of `app/storage/migrate.py`, re-exported by `db.py`, spec 03 §2.3 item 9). Older → allowed, the next boot migrates it forward. Newer → refuse with exit 3 (`snapshot is newer than this code — check out a newer release`): a restore is a deliberate act and must match the code (spec 03 §2.8 item 2), unlike a *live* DB left ahead of the code by an auto-update rollback, which boot tolerates as `schema_ahead`.
4. **Spend carry-over** (live restore only): if the current DB opens, then for months ∈ {current, previous}: `provider_spend.usd/requests := max(snapshot, current)` per `(provider, month)`, and every `job_state` row with `job='_costs'` is copied from the current DB. If it does not open: print the providers with caps and the command `python -m app costs set <provider> --usd X` to reconcile from provider dashboards. (Without this a restore would forget money already spent this month.)
5. **Quarantine:** move `taosentinel.db`, `-wal`, `-shm` to `data/quarantine/<utc-stamp>/` (never delete).
6. **Swap:** `os.replace(staged, DB_PATH)`; assert no `-wal`/`-shm` remain beside it; delete `data/run/integrity.json` (the suspect DB is gone; this lifts the integrity hold, §2.11).
7. Print the post-restore checklist (§7.3.9) and exit 0.

### 2.9 Auto-update (`deploy/auto-update.sh`)

Opt-in (`--with-update-timer`); the checklist default (A5) is manual `git pull`. Port of old `auto-update.sh` 1-179 keeping its safety properties (quiet no-op when up to date; `git reset --hard` only touches tracked files; the body is a function called at EOF so a reset replacing the script mid-run cannot change the executing code; rollback to the last known-good commit on any gate failure; PAT never logged). Changes: paths from env, PAT from `~/taosentinel-secrets/deploy.env` (not the app `.env`), SSH deploy-key alternative, instance/backup/quiet-window deferrals, `[manual-deploy]` refusal, pre-deploy backup on storage changes, stronger gate (`app check` against a scratch DB validates the **live** `.env` against the new `Settings`), health poll reads `/health.status` ∈ {`ok`, `degraded`} and the new SHA in `/health.version` (nothing else, 02 §G6), Telegram notifications, deploy history for the digest (unix-second `at`, 02 §B1).

```bash
#!/usr/bin/env bash
# TaoSentinel git auto-deploy. Port of SubnetTrader deploy/auto-update.sh (1-179). See spec 16 §2.9.
set -euo pipefail

REPO="${SENTINEL_ROOT:?SENTINEL_ROOT not set}"
SECRETS="${SENTINEL_SECRETS:-$HOME/taosentinel-secrets}/deploy.env"
SERVICE="taosentinel"; FRONTEND_SERVICE="taosentinel-frontend"
SYSTEMCTL="${DEPLOY_SYSTEMCTL:-sudo -n /usr/bin/systemctl}"      # tests inject a stub
PY="$REPO/.venv/bin/python"; RUN="$REPO/data/run"
export GIT_TRACE=0 GIT_TRACE_PACKET=0 GIT_TRACE_CURL=0 GIT_CURL_VERBOSE=0 GIT_TERMINAL_PROMPT=0

log() { echo "[$(date -u +%FT%TZ)] auto-update: $*"; }

kv() {  # kv FILE KEY -> value; drops CR and wrapping quotes, never alters token bytes (old 62-72)
  local line v
  line="$(grep -E "^$2=" "$1" 2>/dev/null | head -1)" || true
  v="${line#"$2"=}"; v="${v%$'\r'}"; v="${v#[\"\']}"; v="${v%[\"\']}"
  printf '%s' "$v"
}

notify() {  # best-effort Telegram; never fails the script; token only inside the URL argument
  [ "$(kv "$SECRETS" DEPLOY_NOTIFY)" = "0" ] && return 0
  local tok chat; tok="$(kv "$REPO/.env" TELEGRAM_BOT_TOKEN)"; chat="$(kv "$REPO/.env" TELEGRAM_CHAT_ID)"
  [ -n "$tok" ] && [ -n "$chat" ] || return 0
  curl -fsS -m 5 -o /dev/null --data-urlencode "chat_id=$chat" --data-urlencode "parse_mode=HTML" \
       --data-urlencode "text=$1" "https://api.telegram.org/bot${tok}/sendMessage" || true
}

hist() {  # hist RESULT FROM TO DETAIL   (DETAIL: a snake_case word)
  printf '{"at":%s,"result":"%s","from":"%s","to":"%s","detail":"%s"}\n' \
    "$(date -u +%s)" "$1" "${2:0:7}" "${3:0:7}" "$4" >> "$RUN/deploy-history.jsonl"      # at = unix s (02 §B1)
  tail -n 500 "$RUN/deploy-history.jsonl" > "$RUN/.dh.tmp" && mv "$RUN/.dh.tmp" "$RUN/deploy-history.jsonl"
}

fetch_failures() {  # fetch_failures inc|reset ; alerts once at 12 consecutive (= 1 h at 5-min cadence)
  local n=0 f="$RUN/deploy-state.json"
  [ -f "$f" ] && n="$(jq -r '.fetch_failures // 0' "$f" 2>/dev/null || echo 0)"
  if [ "$1" = reset ]; then n=0; else n=$((n + 1)); fi
  printf '{"fetch_failures":%d,"updated_at":%s}\n' "$n" "$(date -u +%s)" > "$f"
  if [ "$n" -eq 12 ]; then notify "⚠️ <b>auto-update</b>: git fetch failing for 1 h (deploy PAT expired or network). Runbook §7.3.3"; fi
  return 0
}

in_quiet_window() {  # DEPLOY_QUIET_WINDOWS="HH:MM-HH:MM,..." UTC; a window must not cross 00:00 (split it)
  local now w s e; now="$(date -u +%H%M)"
  for w in ${QUIET//,/ }; do
    s="${w%-*}"; e="${w#*-}"; s="${s/:/}"; e="${e/:/}"
    if [ "$now" -ge "$s" ] && [ "$now" -lt "$e" ]; then return 0; fi
  done
  return 1
}

pat_expiry_probe() {  # once per 24 h: record GitHub's token-expiration header for the digest
  local tok="$1" f="$RUN/deploy-pat.json" repo hdr exp
  [ -n "$tok" ] || return 0
  if [ -f "$f" ] && [ -z "$(find "$f" -mmin +1440)" ]; then return 0; fi
  repo="${URL_BASE#github.com/}"; repo="${repo%.git}"
  hdr="$(curl -fsS -m 10 -o /dev/null -D - -H "Authorization: Bearer $tok" "https://api.github.com/repos/$repo" 2>/dev/null)" || return 0
  exp="$(grep -i '^github-authentication-token-expiration:' <<<"$hdr" | cut -d' ' -f2- | tr -d '\r')" || true
  printf '{"checked_at":%s,"expires_at":"%s"}\n' "$(date -u +%s)" "$exp" > "$f"   # expires_at = GitHub's header text verbatim
}

rollback() {  # rollback PREV CHANGED WHY TARGET
  local prev="$1" changed="$2" why="$3" target="$4"
  log "ROLLBACK ($why): reverting to ${prev:0:8} (last known good)"
  git reset --hard "$prev" || log "CRITICAL: rollback reset failed (index.lock / disk full?) — manual intervention required"
  if grep -qxE 'pyproject.toml|requirements.lock' <<<"$changed"; then
    { "$REPO/.venv/bin/pip" install -q -r requirements.lock && "$REPO/.venv/bin/pip" install -q --no-deps -e . ; } \
      || log "WARN: rollback pip install failed"
  fi
  "$PY" -m compileall -q "$REPO/app" >/dev/null 2>&1 || true
  $SYSTEMCTL restart "$SERVICE" || log "CRITICAL: rollback restart failed — service may be DOWN"
  hist rolled_back "$prev" "$target" "$why"
  notify "🛑 <b>Deploy rolled back</b> (${why}): ${target:0:7} → live on ${prev:0:7}"
  exit 1   # non-zero so `systemctl status taosentinel-update` / journald flag the run
}

main() {
  cd "$REPO"; mkdir -p "$RUN" "$REPO/data/tmp"
  exec 9>"$RUN/deploy.lock"; flock -n 9 || { log "another deploy is running"; exit 0; }
  [ -f "$REPO/DEPLOY_PAUSED" ] && { log "DEPLOY_PAUSED present — skipping"; exit 0; }
  command -v curl >/dev/null && command -v jq >/dev/null || { log "ERROR: curl and jq are required"; exit 1; }
  [ -f "$SECRETS" ] || { log "ERROR: $SECRETS missing"; exit 1; }
  [ "$(stat -c %a "$SECRETS")" = "600" ] || { log "ERROR: $SECRETS must be mode 600"; exit 1; }

  local branch tok key url
  branch="$(kv "$SECRETS" DEPLOY_BRANCH)"; branch="${branch:-main}"
  URL_BASE="$(kv "$SECRETS" DEPLOY_GIT_URL_BASE)"
  QUIET="$(kv "$SECRETS" DEPLOY_QUIET_WINDOWS)"
  QUIET="${QUIET:-00:10-00:35,06:10-06:35,12:10-12:35,18:10-18:35,02:55-04:45,20:25-23:35}"
  key="$(kv "$SECRETS" DEPLOY_SSH_KEY)"; tok=""
  if [ -n "${DEPLOY_GIT_URL:-}" ]; then url="$DEPLOY_GIT_URL"                      # tests: file:///…/bare.git
  elif [ -n "$key" ]; then
    export GIT_SSH_COMMAND="ssh -i $key -o IdentitiesOnly=yes -o BatchMode=yes"; url="git@${URL_BASE/\//:}"
  else
    tok="$(kv "$SECRETS" DEPLOY_GITHUB_PAT)"
    [ -n "$tok" ] || { log "ERROR: DEPLOY_GITHUB_PAT empty in $SECRETS"; exit 1; }
    url="https://x-access-token:${tok}@${URL_BASE}"
  fi

  if ! git fetch --quiet "$url" "$branch" 2>/dev/null; then
    log "ERROR: git fetch failed (network or auth)"; fetch_failures inc; exit 1
  fi
  fetch_failures reset; pat_expiry_probe "$tok"

  local local_sha remote_sha changed warn=0
  local_sha="$(git rev-parse HEAD)"; remote_sha="$(git rev-parse FETCH_HEAD)"
  [ "$local_sha" = "$remote_sha" ] && exit 0                                        # up to date — quiet no-op
  if in_quiet_window; then log "quiet window — deferring ${remote_sha:0:8}"; exit 0; fi
  if ! flock -n "$RUN/backup.lock" true; then log "backup running — deferring"; exit 0; fi
  if git log --format=%B "${local_sha}..${remote_sha}" | grep -qF '[manual-deploy]'; then
    if [ ! -f "$RUN/refused-${remote_sha:0:7}" ]; then
      touch "$RUN/refused-${remote_sha:0:7}"; hist refused "$local_sha" "$remote_sha" manual_deploy_marker
      notify "✋ <b>Manual deploy required</b> for ${remote_sha:0:7} ([manual-deploy] marker). Runbook §7.3.6"
    fi
    exit 0
  fi

  changed="$(git diff --name-only "$local_sha" "$remote_sha")"
  log "deploy: $branch ${local_sha:0:8} -> ${remote_sha:0:8}"
  if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
    log "WARN: discarding unexpected local tracked changes:"; git status --porcelain --untracked-files=no | sed 's/^/  /'
  fi
  if grep -q '^app/storage/' <<<"$changed"; then                                   # schema may change: back up with the OLD code first
    log "app/storage changed -> blocking pre-deploy backup"
    "$PY" -m app backup now --tag "predeploy-${remote_sha:0:7}" \
      || { log "ERROR: pre-deploy backup failed — deploy aborted, tree untouched"; hist aborted "$local_sha" "$remote_sha" predeploy_backup_failed
           notify "⚠️ <b>Deploy aborted</b>: pre-deploy backup failed for ${remote_sha:0:7}"; exit 1; }
  fi

  local prev_sha="$local_sha"
  git checkout -q "$branch" 2>/dev/null || git checkout -qB "$branch"
  git reset --hard "$remote_sha"
  if grep -qxE 'pyproject.toml|requirements.lock' <<<"$changed"; then
    log "dependencies changed -> pip install"
    { "$REPO/.venv/bin/pip" install -q -r requirements.lock && "$REPO/.venv/bin/pip" install -q --no-deps -e . ; } \
      || rollback "$prev_sha" "$changed" pip_failed "$remote_sha"
  fi

  # ── Safety gate: all offline; nothing touches the live DB ──
  local out scratch
  log "gate 1/4: compileall"
  "$PY" -m compileall -q "$REPO/app" "$REPO/scripts" >/dev/null || rollback "$prev_sha" "$changed" compileall "$remote_sha"
  log "gate 2/4: import smoke"
  out="$("$PY" -c 'import app.config, app.runtime, app.jobs, app.api.app, app.costs, app.ops.recovery' 2>&1)" \
    || { tail -n 12 <<<"$out" | sed 's/^/  /' >&2; rollback "$prev_sha" "$changed" import_smoke "$remote_sha"; }
  log "gate 3/4: app check on a scratch DB (live .env vs new Settings, schema on empty DB, job registry imports)"
  scratch="$(mktemp -d "$REPO/data/tmp/gate.XXXXXX")"
  out="$(DB_PATH="$scratch/gate.db" JSONL_DIR="$scratch/logs" DB_BACKUP_DIR="$scratch/b" TELEGRAM_BOT_TOKEN= \
         timeout 120 "$PY" -m app check 2>&1)" \
    || { tail -n 12 <<<"$out" | sed 's/^/  /' >&2; rm -rf "$scratch"; rollback "$prev_sha" "$changed" check_failed "$remote_sha"; }
  rm -rf "$scratch"
  if [ "$(kv "$SECRETS" DEPLOY_RUN_SMOKE_TESTS)" = "1" ]; then
    log "gate 4/4: pytest -m smoke"
    timeout 300 "$PY" -m pytest -q -m smoke -p no:cacheprovider >/dev/null 2>&1 || rollback "$prev_sha" "$changed" smoke_tests "$remote_sha"
  else
    log "gate 4/4: skipped (DEPLOY_RUN_SMOKE_TESTS != 1)"
  fi

  log "restarting $SERVICE"
  $SYSTEMCTL restart "$SERVICE" || rollback "$prev_sha" "$changed" restart_failed "$remote_sha"
  local hurl healthy=0 body st ver i
  hurl="$(kv "$SECRETS" DEPLOY_HEALTH_URL)"; hurl="${hurl:-http://127.0.0.1:8090/health}"
  log "probing $hurl (up to ~60 s) for status ok|degraded and version ${remote_sha:0:7}"
  for (( i = 0; i < 20; i++ )); do                                  # reads only .status and .version (02 §G6)
    if body="$(curl -fsS --max-time 3 "$hurl" 2>/dev/null)"; then
      st="$(jq -r '.status // empty' <<<"$body" 2>/dev/null || true)"
      ver="$(jq -r '.version // empty' <<<"$body" 2>/dev/null || true)"
      if [[ "$st" == ok || "$st" == degraded ]] && [[ "$ver" == *"${remote_sha:0:7}"* ]]; then healthy=1; break; fi
    fi
    sleep 3
  done
  [ "$healthy" -eq 1 ] || rollback "$prev_sha" "$changed" health_timeout "$remote_sha"

  if grep -q '^frontend/' <<<"$changed" && [ -d "$REPO/frontend" ]; then           # non-critical: warn, never roll back the backend
    log "frontend changed -> npm ci + build + restart"
    { ( cd frontend && npm ci --silent && npm run build ) && $SYSTEMCTL restart "$FRONTEND_SERVICE"; } \
      || { log "WARN: frontend update failed"; warn=1; }
  fi
  hist deployed "$prev_sha" "$remote_sha" "$([ "$warn" -eq 0 ] && echo ok || echo frontend_warn)"
  notify "🚀 <b>Deployed</b> ${prev_sha:0:7} → ${remote_sha:0:7}$([ "$warn" -eq 0 ] || echo ' (frontend update failed)')"
  log "done (now at ${remote_sha:0:8})"
}

main "$@"
```

Notes: (a) The gate parses `/health` with `jq` and reads only `status` and `version` (02 §G6). `ok` and `degraded` both mean the new process is up and serving — a standing degraded reason (`mirror_missing`, a budget pause, one failing collector) must not turn every deploy into a rollback; `initializing` (DB not connected or scheduler not running) keeps polling until the ≈ 60-s timeout → rollback. `version` carries the short SHA (spec 01: `0.1.0+<sha7>`); the substring test accepts either form. (b) Because `Type=notify`, `systemctl restart` returns only after `READY=1` (or fails after `TimeoutStartSec`), so the probe never sees the old process. (c) Rollback after a schema change relies on the additive-migration rule (§7.3.6). (d) The deploy PAT is a **separate** fine-grained token (single repository `taosentinel`, *Contents: read-only*), not checklist B1's public-repos token, which cannot read a private repo.

### 2.10 Cost governor (`app/costs.py`; owner per 02 §A2, public API = 02 §G5; supersedes spec 01 §2.13)

**Model.** Spend is ledgered per *canonical provider id* (`provider_spend.provider`) and capped per *group*. Provider ids are the same strings in the limiter, `provider_spend.provider`, the governor and `/health` (02 §G1, amended: `tmc`, `tmc_sse`, `subnetalpha`, `subtensor_rpc`, `onfinality`, `opentensor_archive`, `github`, `github_html`, `web`, `wayback`, `discord`, `tme`, `rss`, `youtube`, `twitterapi_io`, `official_v2`, `anthropic`, `taostats`, `rufus`, `subnetradar`); the governor's spend groups aggregate them (G1: `x`, `llm`, `taostats`).

| group | members (canonical provider ids = `provider_spend.provider`) | USD cap key | request cap key | paid (KILL_SWITCH applies) | pacing flag |
|---|---|---|---|---|---|
| `x` | `twitterapi_io`, `official_v2` | `X_MONTHLY_CAP_USD` (25; 75 with `X_PROVIDER=official_v2`, checklist C2 — 02 §F2) | — | yes | `X_PACING_ENABLED` (spec 09) |
| `llm` | `anthropic` | `LLM_MONTHLY_CAP_USD` (60) | — | yes | — |
| `taostats` | `taostats` | `TAOSTATS_MONTHLY_CAP_USD` (display only: flat subscription, never enforced) | `TAOSTATS_MONTHLY_CAP_REQ` (alias `TAOSTATS_MONTHLY_CALL_CAP`) | no | — |
| `tmc` | `tmc` | — | `TMC_MONTHLY_CAP` (90,000) | no | — |
| *(implicit)* | any other canonical id (`github`, `discord`, `web`, …) | — | — | no | — |

`tmc` is a single-provider entry keyed by its own provider id: it adds only the monthly request cap (and its 80 %/100 % alerts and `/health.spend.tmc`) to G1's three spend groups; the limiter enforces the same cap (02 §G1).

**Cap semantics (02 §F2):** a cap of **0 for a paid group (`x`, `llm`) disables that provider** — every `check`/`reserve` denies with `zero_cap` (fail-closed); there is no "unlimited" value for a paid group. A request cap of 0 likewise denies every request of that group.

`group_of(name)` accepts a group key or a canonical provider id and returns its `SpendGroup` (a member resolves to its group; any other canonical id to an implicit uncapped group); there are no aliases. `spend_provider_for(name)` returns the id to book against: a canonical id maps to itself; a group key used as a job's `provider` maps to its active member (`x` → `settings.X_PROVIDER`, i.e. `twitterapi_io`/`official_v2`; `llm` → `anthropic`). `Http.request()` (spec 01 §2.12) books every wire request as `record(p, 0.0, requests=1)` under its canonical id `p` (buffered until `flush()`). `record()` defaults to **`requests=0`** (02 §G5, amended), so a caller booking the USD of a call that already went through `Http` (09's per-page `record`, the wrapper reconciliation below) passes no count and nothing is double-counted; only calls made outside `Http` — 10's Anthropic SDK calls — pass their own `requests=n`. `commit()` never counts requests.

```python
# app/costs.py
@dataclass(frozen=True, slots=True)
class SpendGroup:
    name: str; members: tuple[str, ...]; usd_cap_key: str | None; req_cap_key: str | None
    paid: bool; pacing_flag: str | None = None

SPEND_GROUPS: dict[str, SpendGroup]          # table above: x, llm, taostats (02 §G1) + tmc (request cap only)
def group_of(name: str) -> SpendGroup        # group key or canonical provider id (02 §G1); no aliases
def spend_provider_for(name: str) -> str     # canonical id -> itself; group key x -> X_PROVIDER, llm -> anthropic

@dataclass(slots=True)
class Reservation:                           # returned only when granted
    id: str                                  # "resv-<uuid4 hex[:12]>"; persisted as job_state('_costs', f'resv:{id}')
    group: str; provider: str; est_usd: float
    job: str; run_id: str | None
    created_at: int; expires_at: int         # unix s (02 §B1); expires_at = created_at + COST_RESERVATION_TTL_SEC

@dataclass(slots=True)
class Decision:                              # returned by check(); 09 reads allowed / reason / usd_mtd / cap_usd / projected_usd
    allowed: bool; reason: str | None        # None | "hard_cap" | "pacing" | "kill_switch" | "zero_cap"
    usd_mtd: float; reserved_usd: float; cap_usd: float | None; projected_usd: float | None

class CostGovernor:
    def __init__(self, db: "Database", *, limiter: "ProviderLimiter | None" = None,
                 alerts: "AlertLatch | None" = None, clock: Callable[[], int] = now_ts) -> None
    async def load(self) -> None                         # read month state + latches; limiter.seed_month(p, requests)

    # ── public API — exactly 02 §G5; everything after the leading positional parameters is keyword-only ──
    async def check(self, provider: str, est_usd: float) -> Decision                  # no hold (09)
    async def reserve(self, provider: str, est_usd: float, *, job: str,
                      run_id: str | None) -> Reservation | None                        # hold (10); None = denied
    async def commit(self, res: Reservation | str, actual_usd: float, *, detail: dict | None = None) -> None
    async def release(self, res: Reservation | str) -> None                            # res: object, or its id after a restart
    async def record(self, provider: str, usd: float, *, requests: int = 0, units: dict[str, int] | None = None,
                     job: str | None = None, run_id: str | None = None, detail: dict | None = None) -> None
    def snapshot(self) -> dict                                                         # /health.spend (02 §G6)

    # ── ops surface (runtime, CLI, override route); 09/10 never call these ──
    def is_exhausted(self, name: str) -> bool            # sync — CollectorJob.paused_reason() is sync
    def paused_reason(self, name: str) -> str | None     # "kill_switch" | "budget" | None
    def month_usd(self, name: str) -> float
    def cap_usd(self, name: str) -> float | None         # effective = configured + active override
    def run_total(self, run_id: str) -> float            # Σ usd committed/recorded with this run_id (this process)
    async def daily_run_rate(self, name: str, days: int = 7) -> float
    async def refresh(self) -> None                      # re-read DB state; month rollover; reservation expiry
    async def set_override(self, group: str, extra_usd: float, *, by: str, reason: str) -> None
    async def clear_override(self, group: str) -> None
    async def flush(self) -> int                         # persist buffered request counts; returns rows written
# module-level estimation helpers (callers own the price inputs)
PRICES: dict[str, tuple[float, float]]                   # USD per MTok (in, out); spec 10 owns the values (+ LLM_PRICES_JSON override)
def estimate_llm_usd(model: str, in_tokens: int, out_tokens: int, *, batch: bool = False,
                     cache_read_tokens: int = 0, cache_write_tokens: int = 0) -> float
                                                         # batch halves the price; cache read ×0.1, cache write ×1.25
def estimate_batch(texts: Sequence[str], *, model: str, system_tokens: int, n_req: int) -> float
                                                         # spec 10 §6.3 formula (tokens/char, +40/item, 95 out/item, 30 out/req);
                                                         # spec 10 owns the formula and may extend this signature
```
Spec 09 (`XProvider.cost_estimate`) and spec 10 (`estimate_batch`, which lives in this module and uses `PRICES`) own their estimators; the governor only sees USD. **Call protocol (02 §G5; 09 and 10 call exactly these functions, with keywords — a positional argument after `usd`/`est_usd`/`actual_usd` raises `TypeError`):**
- 09 (no hold): `d = await costs.check(provider, est)` before each paid call; `if not d.allowed:` stop; after the call `await costs.record(provider, usd, units={…}, job=…, run_id=…)` — no `requests`: the call went through `Http`, which already counted it (02 §G5).
- 10 (hold): `res = await costs.reserve("anthropic", est, job=…, run_id=…)` before submitting a batch — `None` means denied (budget fallback); persist `res.id` with the batch. After download `await costs.commit(res or res_id, actual_usd, detail={…})`; on submit failure or cancel `await costs.release(res or res_id)`. Token units of a committed batch and its Anthropic SDK request count (create/retrieve/results — outside `Http`) are booked with `record("anthropic", 0.0, requests=n, units={…})`.
- 10, unreserved Sonnet/Opus calls (Anthropic SDK, outside `Http`): `record("anthropic", usd, requests=1, job=…, run_id=…, detail={…})`.
- `detail` goes to the structured log line `spend_booked` only, never to the DB.

**Decision rule** (`check` and `reserve`; month = UTC `YYYY-MM` of `clock()`):
1. `g = group_of(provider)`. Implicit/uncapped group → allow.
2. `g.paid` and `KILL_SWITCH_PATH` exists → deny `kill_switch` (not latched). Catches ad-hoc scripts too.
3. `cap = cap_usd(g)`; for a paid group `cap ≤ 0` → deny `zero_cap` (cap 0 = provider disabled, 02 §F2).
4. Hard latch `job_state('_costs', f'hard:{g}:{month}')` present and `cap ≤ latch.cap` → deny `hard_cap`. (A raised cap or an override clears the latch automatically on the next decision; the rollover makes it irrelevant because the key is month-scoped.)
5. `usd_mtd` = `SELECT COALESCE(SUM(usd),0) FROM provider_spend WHERE provider IN (<members>) AND month=?` (read from the DB every time → multi-process safe); `reserved` = Σ `est_usd` of unexpired reservations of the group (`job_state` rows `job='_costs' AND key LIKE 'resv:%'`, also DB-read).
6. `usd_mtd ≥ cap × COST_HARD_STOP_PCT` **or** `usd_mtd + reserved + est_usd > cap` → write the hard latch `{at, usd_mtd, cap}`, alert `spend_cap_reached` once per (group, month), deny `hard_cap`.
7. Pacing (only if `g.pacing_flag` names a true setting): `projected = usd_mtd + reserved + est_usd + daily_run_rate(g, 7) × days_left_in_month` > cap → deny `pacing` (not latched; re-evaluated per call).
8. Allow. `reserve` additionally persists the reservation (`job_state('_costs', f'resv:{id}')`, `expires_at = now + COST_RESERVATION_TTL_SEC`) and returns it. On any deny `reserve` returns `None` and logs `spend_denied {provider, group, reason, est_usd}`; a caller that needs the reason calls `check()`.

**Commit / record** — a `record()` carrying only a request count (`usd == 0`, no `units`) is buffered for `flush()`; every other `record()` and every `commit()` is written immediately (not hourly) and atomically inside `async with db.transaction(immediate=True)` (spec 03 §2.3 item 1: writer lock + `BEGIN IMMEDIATE`, so a concurrent script waits on `busy_timeout` instead of interleaving): read the row's `units_json` (`{}` when the row does not exist yet), merge (sum per key), then `INSERT INTO provider_spend(provider, month, usd, requests, units_json, updated_at) VALUES (…) ON CONFLICT(provider, month) DO UPDATE SET usd = usd + excluded.usd, requests = requests + excluded.requests, units_json = excluded.units_json, updated_at = excluded.updated_at` (`updated_at` unix seconds). Increments, never overwrites, so a script and the service can both write. `commit` books `actual_usd` to the reservation's provider (with its `job`/`run_id`, `requests=0`) and deletes the row; `release` deletes it without spending. `refresh()` marks a reservation past `expires_at` as expired (it stops counting in `reserved`; `warning` log `reservation_expired`) and deletes it 7 d later, so a batch that finishes after its TTL is still committed to the right provider; `commit` of an unknown id raises `UnknownReservation` (the caller then books the spend with `record`); `release` of an unknown id is a logged no-op. After every commit/record: if `usd_mtd ≥ COST_SOFT_ALERT_PCT × cap` and `soft:{g}:{month}` is absent → write it, alert `spend_80pct` (**once per group per month, across restarts**). Request-only groups (`tmc`, `taostats`) use the same 80 % rule on `requests / request_cap` and are exhausted at 100 % (the limiter raises `BudgetExhausted` at the same point, spec 01 §2.12).

**Request counts** from `Http` (`record(p, 0.0, requests=1)`, 02 §G5) accumulate in memory and are flushed as increments by `flush()` (hourly `cost_ledger_hourly`, §6.1, and at shutdown per spec 01); `load()` seeds `ProviderLimiter.seed_month()` from the DB so a restart never resets the monthly TMC count (`TMC_MONTHLY_CAP` 90,000 under the 100k/month per-IP limit).

**Exhaustion and auto-pause.** `is_exhausted(name)` is true when the group's hard latch is active for the current month, or its request cap is reached, or it is paid with `cap ≤ 0`. `CollectorJob.paused_reason()` (spec 01) returns `"budget"` for a job whose `provider` maps to an exhausted group, so every job of that provider skips (`/health.collectors.<id>.paused = "budget"`; the block also carries `paused_reason` for spec 09's field name). A `BudgetExhausted` raised by the limiter inside a run (hourly or monthly request cap) ends that run as `paused: budget`, not as a failure, until the window resets (02 §G1). `is_exhausted` checks the month key on every call: after the UTC rollover it returns `False` and schedules `refresh()` — collectors resume at their next tick with no operator action, and a `spend_reset` info event is written for each group that had been stopped.

**Override** (one month only): `python -m app costs override <group> --usd <extra> --reason "<text>"` or `POST /api/v1/control/budget/override` (authed; body `{group, extra_usd, reason}`) writes `job_state('_costs', f'override:{g}:{month}') = {extra_usd, by, reason, at}` (validator: `0 < extra_usd ≤ COST_OVERRIDE_MAX_USD`), emits `budget_override_set` (info), and the effective cap becomes `configured + extra_usd` until the month ends. The API path updates the in-process governor immediately; a CLI override (another process) reaches the service's cached `is_exhausted()`/`paused_reason()` at its next `governor.refresh()`, which `ops_guard_5m` runs every 5 min (§2.13.3). A permanent change is a `.env` edit + `POST /api/v1/control/reload-config` (cap keys are read dynamically). `python -m app costs clear-override <group>` removes it.

**Wrapper reconciliation** (amends spec 01 §2.6 step 8, which would otherwise double-count jobs that already commit per call): `extra = outcome.cost_usd − rt.costs.run_total(run_id)`; if `extra > 1e-9` → `record(spend_provider_for(job.provider), extra, job=id, run_id=run_id)` (default `requests=0`); `collector_runs.cost_usd = max(outcome.cost_usd, run_total(run_id))`.

**`/health.spend`** = `snapshot()` (02 §G6; 01 builds `/health`, 14 serves it, 09/10/14 reference this shape). Keyed by group; every group object carries the five required keys `usd, cap_usd, pct, paused, reserved_usd`; the remaining keys are optional extras for the digest and CLI:
```json
"spend": {"x": {"usd": 3.21, "cap_usd": 25.0, "pct": 0.128, "paused": false, "reserved_usd": 0.0,
                "month": "2026-09", "requests": 812, "request_cap": null, "paused_reason": null,
                "override_usd": 0.0, "soft_alerted": false, "run_rate_7d": 0.41, "projected_usd": 12.4},
          "llm": {"…": "…"}, "taostats": {"…": "…"},
          "tmc": {"usd": 0.0, "cap_usd": null, "pct": 0.226, "paused": false, "reserved_usd": 0.0,
                  "requests": 20311, "request_cap": 90000, "…": "…"}}
```
`pct` = `usd / cap_usd` for USD-capped groups and `requests / request_cap` for request-only groups; `paused` is a bool (the reason is in `paused_reason` and in `collectors.<id>.paused`).

**CLI** (`python -m app costs …`): `status` (the snapshot as a table), `override <group> --usd X --reason R`, `clear-override <group>`, `set <provider> --usd X [--month YYYY-MM]` (absolute correction after a restore; writes an info log and a `manual_entry`-style `budget_override_set` event with `payload.kind="set"`), `record <provider> --usd X` (drills on a scratch `DB_PATH` only — refuses when `DB_PATH` is the live DB and the service lock is held). The CLI opens `Database(DB_PATH)` and runs `apply_schema()` first (a fresh drill file works), and passes an `AlertLatch` whose sink prints alert texts to stdout instead of sending them, so drills never reach Telegram.

### 2.11 Kill switches and pause precedence

| control | key / location | scope | effect | cleared by |
|---|---|---|---|---|
| `KILL_SWITCH` | `KILL_SWITCH_PATH` = `./KILL_SWITCH` (operator file) | every `paid=True` job (09's X jobs; 10's `llm_classify_submit` and `llm_narrative_weekly`) + the governor. 10's free `llm_batch_poll` and `llm_rules_fallback` (`paid=False`) are **not** paused: they keep downloading already-billed batches so reservations commit, and keep rules-labelling (10 §6.1; the governor still denies their Sonnet escalations) | paid jobs skip `paused:kill_switch`; `reserve`/`check` deny every paid group (also in scripts); free jobs and free LongTasks keep running (02 §B12) | `rm KILL_SWITCH` (next tick) |
| `SCORING_KILLED` | `SCORING_KILL_SWITCH_PATH` = `./SCORING_KILLED` | scoring publish (spec 12) + the feed endpoint `GET /api/v1/scores/latest` (served by 14 §3.5, 02 §H1; spec 15 only consumes it) | scores computed, not inserted; feed serves last good with `stale=true`; `/control/rescore` → 423 | `rm SCORING_KILLED` |
| `PAUSE_ALL` | `PAUSE_ALL_PATH` = `./PAUSE_ALL` (operator file; the service never creates it) | every job and LongTask except the `ops_exempt` set below (02 §B12) | skip `paused:pause_all`; liveness ignores them; API, Telegram bot and alert dispatch stay up | `rm PAUSE_ALL` |
| integrity hold | `data/run/integrity.json` with `result ≠ "ok"` (§2.6 step 8; no pause file is written) | same scope as `PAUSE_ALL`, plus `storage_retention_daily` (a suspect DB is never pruned) | reported `paused:pause_all`; `degraded_reasons += db_integrity` | live restore (§2.8 step 6) or `python -m app ops integrity-check` returning `ok` |
| per-target pause (the only persistent pause store, 02 §B12) | `control_state(target, key='paused', value='true', reason, set_by, set_at, expires_at)` (spec 03 DDL; `reason` is the operator's free-text note); API, Telegram and CLI all write it | an exact job/LongTask id or a `PAUSE_GROUPS` name (resolution below; never a prefix or provider match) | skip `paused:operator` | `POST /api/v1/control/resume/{target}`, Telegram `/resume`, `python -m app ops resume <target>`, or `expires_at` passing |
| per-target pause (emergency input) | `data/control/<target>.paused` — created and deleted by the operator's shell only; the app reads it and never writes it (02 §B12) | same resolution | skip `paused:emergency_file` | `rm` only. A resume deletes only the `control_state` row; if the file still exists the target stays paused and the API/CLI/Telegram reply carries `file_pause_active: data/control/<target>.paused` and says the target stays paused until the operator deletes that file |
| budget | governor latch (§2.10) | spend group | skip `paused:budget` | rollover, higher cap, override |
| `DEPLOY_PAUSED` | `./DEPLOY_PAUSED` — an input of the external `deploy/auto-update.sh` only; the app never reads it, so it is outside 02 §B12's closed list | auto-update only | timer runs are no-ops | `rm DEPLOY_PAUSED` |

**Closed sentinel list** (02 §B12, amended): the app reads exactly `KILL_SWITCH`, `PAUSE_ALL`, `SCORING_KILLED` (path keys `KILL_SWITCH_PATH`, `PAUSE_ALL_PATH`, `SCORING_KILL_SWITCH_PATH`) and `data/control/<target>.paused`. Retired, with their path keys: `X_KILLED` (`X_KILL_SWITCH_PATH`, 09), `GITHUB_KILLED` (`GITHUB_KILL_SWITCH_PATH`, 06), `COMMUNITY_PAUSED` (`COMMUNITY_KILL_SWITCH_PATH`, 08), 05's `TMC_PAUSED`/`TMC_PAUSE_SENTINEL` and any other per-component sentinel (e.g. 17's `BACKFILL_PAUSE_PATH`, 07's `WEB` kill sentinel). The emergency equivalents are the group files `data/control/x.paused`, `github.paused`, `community.paused` (or `discord.paused`), `tmc.paused`, `backfill.paused`, `web.paused`, … (`PAUSE_GROUPS` below).

**`ops_exempt` set** (02 §E1: never paused by `KILL_SWITCH`, `PAUSE_ALL` or any `PAUSE_GROUPS` target; an operator pause by the **exact** id — row or emergency file — and the integrity hold's `storage_retention_daily` rule still apply): this spec's `backup_daily`, `ops_guard_5m`, `ops_digest_daily`, `cost_ledger_hourly` (§6.1); 03's `storage_retention_daily`; 01's `log_retention_daily`; 14's `alert_dispatch` job (01 ships it in Phase 0, 02 §D3) and `telegram_bot` LongTask — so alerts and Telegram `/resume` keep working under `PAUSE_ALL` (02 §E1 binds the flag). Pausing `telegram_bot` by its exact id disables Telegram `/resume`; undo it from the CLI or API. Everything else pauses, including scoring and manual backfills.

**Precedence** (first match is the reported reason; implemented once in `app/control.py::effective_pause(job_or_task, rt) -> str | None` and used by both `CollectorJob.paused_reason()` and `LongTask.paused_reason()`): disabled flag (reported as `last_skip_reason="disabled"`, not a pause) → `pause_all` (`./PAUSE_ALL` or the integrity hold) → `kill_switch` (paid) → `emergency_file` (a `data/control/<t>.paused` file whose `<t>` is the job's own id or a group containing it) → `operator` (a live `control_state` row whose target is the job's own id or a group containing it) → `budget`. The vocabulary of `/health.collectors.<id>.paused` and `status()` is closed (02 §B12): `operator | kill_switch | pause_all | budget | emergency_file`. 10's former `manual`/`flag` report `operator`; a collector-local stop such as 09's `auth` is a failure (`last_error`), not a pause reason. `effective_pause()` is synchronous (spec 01's `paused_reason()` is sync): `control_state` rows are loaded into `app/control.py`'s in-memory cache at boot — each row's target expanded once with `resolve_targets()` (a row naming an unknown target is ignored and logged `unknown_pause_target`) — updated in place by `pause()`/`resume()` inside the service, and re-read by every `ops_guard_5m` run (`control.refresh_cache(db)`), so a row written by the CLI or `sqlite3` from another process takes effect within 5 min; rows with `expires_at < now` are ignored; file checks are an `os.path.exists` per run (immediate).

**Pause targets** (02 §B12, amended). A target — the `control_state.target` of an API/Telegram/CLI pause, or the `<target>` of an emergency file — is either an **exact** job/LongTask id of 01's `JOB_TABLE` (declared, whether or not available in the current phase) or a group name from `PAUSE_GROUPS` below. There is **no prefix, provider or substring matching** (the former `github_*`/`tmc`-provider rule is retired; it also made `telegram` hit 14's `telegram_bot`). A group never reaches the `ops_exempt` set or 14's `alert_dispatch`/`telegram_bot`; group names and job ids are disjoint.

**`PAUSE_GROUPS`** (owned here; members are the owner specs' job ids, 02 §E2; a job may belong to several groups):

| group | members (exact ids) | source / note |
|---|---|---|
| `registry` | `registry_occupants`, `registry_identity` | 04 §2.1 |
| `chain` | `chain_sse` (LongTask), `tmc_hourly`, `tmc_daily`, `tmc_weekly`, `chain_features`, `chain_rpc_hourly`, `chain_rpc_metagraph` | 05 §6.1 (all 05 jobs + the SSE LongTask) |
| `tmc` | `registry_occupants`, `registry_identity`, `tmc_hourly`, `tmc_daily`, `tmc_weekly`, `github_html_probe`, `owner_collect_daily`, `backfill_tmc` | every job that calls `TmcClient` (02 §G2/§G3): 04; 05; 06's TMC cross-check runs inside `github_html_probe`; 11; 17. **Not** `chain_sse`: its stream (`tmc_sse`) is unmetered and its ≤ 6/h REST fallback is the live-data path — stop it with `chain_sse` or `chain` |
| `github` | `github_resolve`, `github_org_enum`, `github_activity`, `github_metrics`, `github_html_probe`, `backfill_github` | 06 §2.1; 17 (same PAT) |
| `web` | `web_check`, `web_metrics`, `web_spn2` | 07 §2.7 |
| `discord` | `discord_6h`, `discord_widget_1h`, `discord_gateway` (LongTask) | 08 §6 (the 02 §B12 example) |
| `telegram_counts` | `telegram_daily` | 08 §6 (t.me counts; never 14's `telegram_bot`) |
| `community` | `discord_6h`, `discord_widget_1h`, `discord_gateway`, `telegram_daily` | all of 08; replaces the retired `COMMUNITY_PAUSED` |
| `x` | `x_team_poll`, `x_kol_poll`, `x_mentions_poll`, `x_profile_refresh`, `x_ctf_crosscheck` | 09 §2.5 (all 09 X jobs, 02 §B12) |
| `rss` | `rss_feeds_poll`, `news_google_poll`, `youtube_poll` | 09 §2.5 job table (appendix feeds, news, YouTube) |
| `llm` | `llm_classify_submit`, `llm_batch_poll`, `llm_rules_fallback`, `llm_narrative_weekly` | 10 §6.1 job table (exactly 10's four jobs, 02 §B12/§E2; `nlp_*` ids do not exist — event materialisation runs inside `llm_batch_poll`/`llm_rules_fallback`, text retention inside 03's `storage_retention_daily`) |
| `integrity` | `owner_collect_daily`, `owner_watch_hourly`, `team_seed_daily`, `integrity_features`, `owner_rpc_seed` | 11 §6 |
| `scoring` | `features_hourly`, `scoring_6h` | 12 §6 (`SCORING_KILLED` only stops publication; this group stops the jobs) |
| `quant` | `quant_weekly` | 13 §5 |
| `notify` | `dereg_watch`, `needs_review_digest` | 14 §2.5–§2.6 (never `alert_dispatch`/`telegram_bot`) |
| `backfill` | `backfill_tmc`, `backfill_github` | 17 §8.1 (manual in-service jobs; 17's standalone scripts are not jobs) |

In no group (exact id only): the `ops_exempt` set above. A new job joins the table in the same change that adds it to `JOB_TABLE` (static test, §9).

```python
# app/control.py
PAUSE_GROUPS: dict[str, frozenset[str]]            # the table above, verbatim
PAUSE_REASONS = ("operator", "kill_switch", "pause_all", "budget", "emergency_file")   # closed (02 §B12)
class UnknownTarget(ValueError): ...

def declared_ids() -> frozenset[str]               # every JOB_TABLE id (jobs and LongTasks), available or not
def never_grouped() -> frozenset[str]              # {s.id for s in JOB_TABLE if s.ops_exempt} | {"alert_dispatch", "telegram_bot"}

def resolve_targets(name: str) -> list[str]:
    """Exact JOB_TABLE id -> [name]; PAUSE_GROUPS name -> its members minus never_grouped();
    anything else -> UnknownTarget. Never a prefix, provider or substring match."""
    if name in declared_ids():
        return [name]
    if name in PAUSE_GROUPS:
        return sorted(PAUSE_GROUPS[name] - never_grouped())
    raise UnknownTarget(name)
```

`pause(name, *, by, reason, hours=None)` validates `name` with `resolve_targets()` (`UnknownTarget` → API 404, CLI exit 2 listing the group names, Telegram "unknown target") and upserts one row `(target=name, key='paused', value='true', reason, set_by=by, set_at=now, expires_at=now + hours·3600 or NULL)` — a group pause is stored under the group name, so `resume <group>` undoes it. `resume(name, *, by)` deletes only the row whose `target = name` (never a file, never another row); its reply (API, CLI, Telegram) lists every covered job that is still paused: by an emergency file → `file_pause_active: data/control/<t>.paused` with the text "stays paused until the operator deletes this file" (02 §B12); by another row (e.g. its group) → `still_paused_by: <target>`. A file whose name is neither a job id nor a group (e.g. `github_.paused`, `telegram.paused`) pauses nothing and is logged once as `unknown_pause_file`. The existence checks are an `os.path.exists` per run (µs), so shell-created files work with the service running.

### 2.12 Rate-limit hygiene across processes

Rules (binding for every process on the laptop):
1. One `ProviderLimiter` per process (spec 01); every outbound call goes through `Http.request(provider, …)`; no module builds its own `httpx.AsyncClient`.
2. **The TMC anonymous limits are per public IP** (10/60 s, 600/h, 100k/month — research 02-gapfill-keyless §0). Every host behind the same NAT shares them: the Windows PC, the old Pi if SubnetTrader is ever revived, and any script on the laptop. The service budgets 8/min and 500/h; ad-hoc TMC work gets the remainder (2/min, 100/h). These are *average* rates: a token bucket admits `burst + rate` requests in any 60-s window, so the service alone (`TMC_RATE_BURST` 2 + `TMC_RATE_PER_MIN` 8; validator `burst + rate ≤ 10`, 02 §F2) can already touch 10; concurrent scripts therefore rely on the shared 429 penalty (rule 3), and an ad-hoc script larger than a handful of calls uses `--exclusive tmc`. Spec 17's backfills are not scripts: `backfill_tmc`/`backfill_github` run **in-service** as `manual` jobs inside the service bucket, paced by `BACKFILL_TMC_MAX_PER_HOUR` (180) outside `BACKFILL_TMC_QUIET_UTC` (02 §F2, §G3), and are never wrapped in `with_bucket`.
3. 429s are shared: `ProviderLimiter.penalize(p, s)` also writes `data/run/penalty-<p>.json` = `{"until": <epoch>, "pid": …}` (atomic replace); `acquire(p)` honours `max(in-memory, file)` penalty, re-reading the file at most every 5 s (mtime-cached). Amendment to spec 01 §2.12.
4. Monthly request counters are shared through `provider_spend.requests` increments (§2.10); a script flushes its governor on exit.
5. Never retry a 404 within a cycle; 403s on Discord count toward the invalid-request ban (10,000 per 10 min per IP) — widget probes of disabled guilds are weekly (spec 08).

**`scripts/with_bucket.py`** — the only sanctioned way to run ad-hoc network scripts next to the service:

```
python -m scripts.with_bucket [--providers tmc,github,…] [--exclusive tmc] -- <command …>
```
1. Detects the service (`GET http://127.0.0.1:{HEALTH_PORT}/health`, 1 s timeout).
2. Service running → the child's environment gets reduced rates (pydantic-settings env beats `.env`): `TMC_RATE_PER_MIN=SCRIPT_TMC_RATE_PER_MIN` (2), `TMC_HOURLY_CAP = 600 − <service TMC_HOURLY_CAP>` (100 with the default 500 — the anonymous per-IP cap is 600/h, research 02-gapfill-keyless §0), bucket burst 1, and for every other listed provider its `*_RATE_PER_SEC`/`*_RATE_PER_MIN`/`*_RATE_PER_HOUR` key × `SCRIPT_RATE_SHARE` (0.25). Not running → rates unchanged. Always sets `TAOSENTINEL_PROCESS_ROLE=script` (the runtime refuses `serve` in that role).
3. `--exclusive tmc` (large ad-hoc scripts outside the service only — never for spec 17's in-service backfills): reads `API_AUTH_TOKEN` from `.env`, lists `/api/v1/jobs`, `POST /api/v1/control/pause/{id}` (a `control_state` row per **exact** id) for every member of `PAUSE_GROUPS["tmc"]` (§2.11) that is not a `manual` job (so never `backfill_tmc`; `chain_sse` is not a member) and not already paused, runs the child at `TMC_RATE_PER_MIN=7` / `TMC_HOURLY_CAP=500`, and resumes exactly those ids in `finally` (also on SIGINT/SIGTERM).
4. Library form for Python scripts: `async with script_runtime(providers=("tmc",)) as rt:` → `rt.http`, `rt.costs`, `rt.db` built from Settings with the same shares applied; flushes the governor and closes on exit.

### 2.13 Monitoring, alerts, digest, logs

**2.13.1 Probes.** (a) systemd watchdog — in-process liveness (§2.5); (b) `/health` — polled by the deploy gate (reads only `status` and `version`, 02 §G6), the UI NavBar (30 s) and the operator (`python -m app health`); (c) `/api/health/services` — spec 14 provider probes; (d) `OnFailure=` notifier — covers "systemd gave up"; (e) optional external dead-man (`DEADMAN_PING_URL`, e.g. a free healthchecks.io check; **UNVERIFIED** current free-tier terms) — `ops_guard_5m` GETs the URL; the external service alerts when pings stop, which is the only way to learn that the whole laptop is off. Ops tooling (runbooks, digest cross-checks) depends on these `/health` fields only — shape owned by 01 (02 §G6): `status` (`ok | degraded | initializing`), `degraded_reasons`, `version`, `kill_switch_active`, `scoring_killed`, `collectors.*.{paused,last_run,consecutive_failures}`, `db.{last_backup,last_backup_age_hours}`, `spend.*`, and the new `ops` block:

```json
"ops": {"boot_kind": "host_unclean", "started_at": "…Z", "downtime_before_boot_sec": 11820, "unclean_boots_7d": 1,
        "pause_all": false, "watchdog": {"enabled": true, "withheld_24h": 0, "api_probe_fails": 0},
        "disk": {"free_gb": 41.2, "min_gb": 2.0, "critical_gb": 0.5},
        "backup": {"last_name": "taosentinel-20260923T033012Z", "mirror": "ok", "stale": false},
        "deploy": {"last_at": "…Z", "last_result": "deployed", "sha": "3fa2c1d", "pat_expires_at": "2027-08-01 00:00:00 UTC"},
        "security": {"env_mode_ok": true, "secrets_dir_ok": true, "loopback_bind": true, "serve_exposes_api": false, "auth_token_set": true, "wallet_dirs_found": 0}}
```
`ops.pause_all` is true while `./PAUSE_ALL` exists or the integrity hold is active. `degraded_reasons` (spec 01's vocabulary, which also holds 01's own reasons such as `spec_unknown`) gains `pause_all`, `mirror_missing`, `db_integrity`, `disk_critical`; `backup_stale` uses `DB_BACKUP_STALE_HOURS` (36, 02 §F2) instead of spec 01's literal 48.

**2.13.2 Ops alert catalogue and routing.** `AlertLatch.fire(key, text, *, event_type=None, severity='warning', payload=None, dedupe_key=None, source='runtime')` (02 §D3; spec 01's latching semantics unchanged). With `event_type` set it writes the row through 03's `emit_event` (`occupant_key=NULL`, `netuid=NULL`, `event_at=observed_at=now` unix s, `source` ∈ {`runtime`, `cost`, `ops`} per 02 §B9, `payload`, `dedupe_key` from the table; `ux_events_dedupe` makes a repeat a no-op; `emit_event` rejects unknown types and missing required payload keys, extra keys allowed — 02 §D1). Delivery (02 §D3, amended): the `alert_dispatch` job sends rows with `alerted_at IS NULL` (exactly-once, restart-safe). It ships in **Phase 0** — 01 implements it (`app/alerts/dispatch.py`) with 14 §2.4's routing table as data (unknown types → digest); 14's later phases add only the API and Telegram commands — and it is `ops_exempt` (§2.11), so ops alerts work from day one and under `PAUSE_ALL`. `fire()` calls `send_alert(text)` itself in exactly one case: the DB write fails (the D3 fallback — the text is sent and nothing is stored). There is no other direct path for an event type: every ops row, `disk_critical` and `db_integrity_failed` included, is inserted with `alerted_at = NULL` and paged by the dispatcher (a failed insert — likely precisely when the disk is full or the DB damaged — takes the fallback); 03 §3.9's `direct` marking of those two and `backup_failed`'s "+ direct send if the dispatcher is down" are superseded by D3 (§11 item 22). Without `event_type` (a process notice such as `service_online`) `fire()` sends directly and stores nothing (D3). Dedupe tokens follow 03 §3.9: `{day}` = `floor(event_at/86400)`, `{h6}` = `floor(event_at/21600)`, `{month}` = `YYYY-MM`, `{boot}` = `{boot_id}|{started_at}` (more specific than 03's `boot_id`, so each process crash inside one kernel boot reports; 03 lets the emitter's more specific pattern win).

| event_type / key | trigger | severity | route | `source` · `dedupe_key` | payload: 03 §3.9 required keys (+ extras) |
|---|---|---|---|---|---|
| `service_online` | after READY | info | direct | — (not stored; `fire()` without `event_type`) | — (spec 01 text + `boot_kind`) |
| `service_recovered_unclean` | §2.6 step 11 | warning | immediate | ops · `{boot}` | `{kind, downtime_sec, prev_ts, orphan_runs, sse_gap_blocks_est, missed: [job ids]}` |
| `collector_failing` | spec 01 wrapper, `JOB_FAIL_ALERT_THRESHOLD`-th consecutive failure | warning | immediate | runtime · `{collector}\|{first_failure_at}` | `{collector, consecutive_failures, last_error}` (+ `job`, the same id) |
| `collector_recovered` | spec 01 wrapper, first success after `collector_failing` | info | immediate | runtime · `{collector}\|{first_failure_at}` | `{collector, failures, down_seconds}` — `failures` = consecutive failures before the success, `down_seconds` = `now − first_failure_at` (02 §D2) (+ `job`) |
| `spend_80pct` | §2.10 soft latch | warning | immediate | cost · `{group}\|{month}\|80` | `{group, month, usd_mtd, cap_usd, pct, unit: "usd"\|"requests"}` (+ `provider` = group key, the field 14 dedupes on) |
| `spend_cap_reached` | §2.10 hard latch | critical | immediate | cost · `{group}\|{month}\|cap` | `{group, month, usd_mtd, cap_usd, reserved_usd, denied_est_usd, paused_jobs: [ids]}` (+ `provider`) |
| `spend_reset` | UTC rollover of a stopped group | info | digest | cost · `{group}\|{month}\|reset` | `{group, month}` (+ `provider`) |
| `budget_override_set` | CLI/API override or `costs set` | info | digest | cost · — (NULL, every change recorded) | `{group, month, extra_usd, by, reason, kind: "override"\|"set"}` (+ `provider`) |
| `backup_failed` | §2.7 | critical | immediate | ops · `{day}` | `{error, last_good_name, last_good_age_h}` (+ `backup_dir`) |
| `backup_stale` | §2.13.3: newest backup older than `DB_BACKUP_STALE_HOURS` | warning | immediate | ops · `{day}` | `{age_hours, stale_hours}` (`stale_hours` = `DB_BACKUP_STALE_HOURS`) |
| `backup_mirror_missing` | marker absent | warning | immediate | ops · `{day}` | `{mirror_dir, marker}` |
| `backup_mirror_failed` | copy error | warning | immediate | ops · `{day}` | `{mirror_dir, error}` |
| `disk_low` | free < `DISK_FREE_MIN_GB` | warning | immediate | ops · `{day}` | `{free_gb, min_gb, db_gb, backups_gb, logs_gb}` (+ `path`) |
| `disk_critical` | free < `DISK_FREE_CRITICAL_GB` | critical | immediate (direct only via the failed-insert fallback) | ops · `{day}\|{h6}` | same |
| `db_integrity_failed` | §2.6 step 8 | critical | immediate (direct only via the failed-insert fallback) | ops · `{boot}` | `{quick_check: "<first 5 lines>"}` |
| `schema_warning` (emitted by 03's `migrate()` at boot; listed for the operator) | `schema_ahead` after an auto-update rollback across a migration, or `checksum_drift` | warning | immediate | storage · `{kind}\|{version}` | `{kind, version, detail}` — runbook §7.3.6 |
| `watchdog_withheld` | §2.5 | — | log only (count in digest) | — | — |
| deploy applied / rolled back / refused / fetch failing | `auto-update.sh` | info / critical / warning | direct (curl) | not stored (bash) · per SHA / 12 failures | `deploy-history.jsonl` |
| unit failed | `OnFailure=` | critical | direct (curl) | not stored · per failure | — |
| ops digest | 09:00Z | — | direct message | not stored · daily | — |

Routing follows 02 §D4: every ops type with alert = yes in 03 §3.9 is immediate (`service_recovered_unclean`, `collector_*`, `spend_80pct`, `spend_cap_reached`, `backup_*`, `disk_low`, `schema_warning`), and so are `disk_critical` and `db_integrity_failed` (D3 leaves no direct path, see above); `spend_reset` and `budget_override_set` go to the digest. Provider-budget events of other specs (09's `x_budget_paused`, 05's `tmc_budget_degraded`, 10's `llm_budget_paused`, …) also go to the **digest**: the page for a budget stop is `spend_cap_reached` (02 §D4, amended).

**2.13.3 `ops_guard_5m`** (`app.ops.guard:OpsGuardJob`; row in §6.1): (1) rewrite `heartbeat.json`; (2) disk: `shutil.disk_usage(DATA_DIR)` → fire/clear `disk_low` / `disk_critical` latches; (3) once per hour: mirror presence (configured ∧ marker present) → fire/clear `backup_mirror_missing`; newest backup age (`latest_backup_age_hours`) > `DB_BACKUP_STALE_HOURS` while `DB_BACKUP_ENABLED` → fire/clear `backup_stale` (`{age_hours, stale_hours}`); (4) every run: `governor.refresh()` (rollover, expired reservations, overrides/latches written by other processes), `control.refresh_cache(db)` and a re-read of `data/run/integrity.json` (§2.11) — a few indexed SELECTs; (5) dead-man ping if `DEADMAN_ENABLED` (GET `DEADMAN_PING_URL`, 10 s timeout, provider `deadman`, failures only logged); (6) WAL size: if `db-wal` > `DB_WAL_WARN_MB` (1,024) log `warning wal_large` (checkpointing is spec 03's). Returns `JobOutcome(rows=0, note="free=41.2GB mirror=ok")`.

**2.13.4 `ops_digest_daily`** (`app.ops.digest:OpsDigestJob`; row in §6.1, 09:00Z per 02 §E3): builds the text in-process (DB + runtime; no HTTP), HTML-escaped, ≤ 4,000 chars (spec 14 chunker), sent direct:
```
🛰 TaoSentinel ops · 2026-09-23 09:00Z · v0.1.0+3fa2c1d · up 2d 04h
boots 7d: 2 (1 host_unclean, down 3h17m on 09-22) · watchdog withheld 24h: 0
jobs 24h  ok/fail/skip  rows    last error
<pre>tmc_daily          1/0/0    3,871
github_activity    1/0/0    1,204
x_team_poll        0/0/1    —       paused:budget</pre>
failing now: none · paused: x (budget) · PAUSE_ALL: no · KILL_SWITCH: no · SCORING_KILLED: no
spend 2026-09: x $25.00/$25 (100 %, stopped 09-21) · llm $31.40/$60 (52 %, proj $44) · tmc 20,311/90,000 req
scores: latest official 2026-09-23T00:15Z (129 rows, conf≥0.4: 112) · provisional 06:15Z
db 2.41 GB (+38 MB/24h) · WAL 12 MB · disk free 41.2 GB · logs 212 MB
backup taosentinel-20260923T033012Z 2.40 GB→0.81 GB gz · 9 kept locally (9.7/12 GB) · mirror ok (15 kept)
deploy: last 09-21 18:02Z deployed 1a2b3c4→3fa2c1d · deploy PAT expires in 312 d · GitHub PAT expires in 280 d
security: .env 600 ✓ · secrets 700 ✓ · bind 127.0.0.1 ✓ · auth token ✓ · wallet dirs 0 ✓
```
Warnings are prefixed `⚠️` (PAT < `PAT_EXPIRY_WARN_DAYS`, mirror not configured, backup stale, disk < 2 × `DISK_FREE_MIN_GB`, any job with 0 ok runs in 24 h while enabled and unpaused). The GitHub collector PAT expiry comes from the `github-authentication-token-expiration` response header that spec 06's `GitHubQuota` records for the digest (02 §G4). Fine-grained PATs send **no** such header (verified 2026-10-17), so `GitHubQuota.expires_at()` falls back to the operator's `GITHUB_TOKEN_EXPIRES_AT` (06 §5); with neither, the digest prints `GitHub PAT expiry unknown (set GITHUB_TOKEN_EXPIRES_AT)`. Built in `app/ops/digest.py` 2026-10-17.

**2.13.5 Telegram budget.** Typical day: 1 digest + 0–5 ops alerts + spec 14 traffic; all through the `telegram` bucket (spec 14: 20 msg/min).

**2.13.6 Logs and retention.**

| log | where | rotation / retention | owner |
|---|---|---|---|
| structured app log | `data/logs/YYYY-MM-DD.jsonl` | 10 MB → `.N`; `LOG_RETENTION_DAYS=14` via `log_retention_daily` | spec 01 |
| stdout/stderr | `data/service.log`, `data/frontend.log` | logrotate daily, 14 files, 50 MB max, gzip, `copytruncate` | this spec |
| systemd / deploy / notifier | journald | `SystemMaxUse=500M`, 30 days | this spec |
| deploy history | `data/run/deploy-history.jsonl` | last 500 lines | `auto-update.sh` |
| audit tables | `collector_runs`, `events`, `provider_spend` | kept forever (small: ≈ 300 `collector_runs` rows/day) | spec 03 |
| backups | `data/backups/` | GFS 7 + 8, budget `DB_BACKUP_MAX_TOTAL_GB` (12 GB, 02 §F2; the 2 newest never pruned) | §2.7 |

### 2.14 Security baseline

- **Secrets at rest:** `.env` 600, `~/taosentinel-secrets/` 700 with files 600; the deploy PAT lives only in `deploy.env` (the service never reads it; the auto-updater never reads app keys except the Telegram pair for notifications). `.env`, `data/`, `seed/*` (except `*.example`) and all sentinel files are gitignored; `test_no_secrets_tracked` asserts `git ls-files` contains none of them.
- **API token:** `API_AUTH_TOKEN=$(openssl rand -hex 32)` from Phase 0 (64 hex chars; `require_auth` compares with `hmac.compare_digest`). Mutating routers are authed (spec 14); read routers stay open on loopback.
- **Network:** `HEALTH_HOST=127.0.0.1` (default) and the UI bound to `127.0.0.1`; remote access only through Tailscale (`tailscale serve` proxies to loopback), with `CONTROL_ALLOWED_ORIGINS` extended by the tailnet UI origin (e.g. `https://<host>.<tailnet>.ts.net`). If the operator instead binds `HEALTH_HOST` to the Tailscale IP, `API_AUTH_TOKEN` must be non-empty (spec 01 warns; the digest flags it). The same holds for `tailscale serve`: it makes loopback-bound routes reachable from every tailnet device (the API sees `127.0.0.1` as the client), so exposing `:8090` that way also requires a non-empty `API_AUTH_TOKEN`; `selfcheck()` runs `tailscale serve status --json` when the binary exists and reports `serve_exposes_api`. The UI must then be built with `NEXT_PUBLIC_API_URL=https://<host>.<tailnet>.ts.net:8443` — spec 14's default `http://<page host>:8090` is neither served over the tailnet nor callable from an https page (mixed content).
- **No wallet material:** no `bittensor`/`bittensor-wallet` packages (spec 01 `test_deps.py`); `app/ops/security.py::selfcheck()` counts `~/.bittensor/wallets` directories and any `coldkey`/`hotkey` JSON files under `SENTINEL_ROOT` and reports them in `/health.ops.security.wallet_dirs_found` and the digest (checklist A4: the old Pi wallet stays **offline**, never on this laptop).
- **Self-check** (boot + digest): `.env` mode 600 and owned by the service user; secrets dir 700; `HEALTH_HOST` loopback or token set; API exposed by `tailscale serve` ⇒ token set; `.env` not tracked by git; `data/` mode 700. Failures are warnings (never fatal) shown in `/health.ops.security`.
- **Least privilege:** the service has no sudo (`NoNewPrivileges=true`); the updater's sudo is limited to two exact `systemctl restart` commands; Telegram commands are chat-id allowlisted (spec 14).

---

## 3. Data contract

No `metric_values` are produced (ops output is diagnostics, alerts and files), so no `occupant_key` rows and no `metric_key`s. Ops rows follow spec 03 §2.1: every `*_at` column is `INTEGER` unix seconds UTC (the tables are `STRICT` and reject ISO strings); JSON values written here (`job_state.value_json`, `events.payload`, manifests, `heartbeat.json`, `integrity.json`) also use unix seconds for `at`/`*_at`/`ts` keys; ISO-8601 appears only in `/health`, CLI output and Telegram text. Absence vs failure for ops: a run that verified there was nothing to do is `ok=1, rows=0`; a failure is `ok=0` with `error` and never writes a zero-valued substitute; nothing is forward-filled (a missed heartbeat, backup or digest stays missing and is reported as such).

### 3.1 Tables written (master §6 names; DDL owned by spec 03 §3.1 — quoted, not redefined)

```sql
-- class: state. Cost governor ledger (01 §2.13, 16). One row per provider per UTC month.  (spec 03 columns/constraints; comments adapted)
CREATE TABLE IF NOT EXISTS provider_spend (
  provider    TEXT NOT NULL,                 -- spend provider: twitterapi_io | official_v2 | anthropic | taostats | tmc | github | …
  month       TEXT NOT NULL CHECK (month GLOB '[0-9][0-9][0-9][0-9]-[0-1][0-9]'),
  usd         REAL NOT NULL DEFAULT 0.0,     -- incremented at commit/record, never overwritten (§2.10)
  requests    INTEGER NOT NULL DEFAULT 0,    -- incremented by flush(); spec 09's "calls" is this column
  units_json  TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(units_json)),
                                             -- {"posts": n, "profiles": n, "input_tokens": n, …} summed per key
  updated_at  INTEGER NOT NULL,              -- unix s
  PRIMARY KEY (provider, month)
) STRICT, WITHOUT ROWID;

-- class: state. Operator control plane (pause/resume), survives restarts.  (spec 03 columns/constraints; comments adapted)
CREATE TABLE IF NOT EXISTS control_state (
  target      TEXT NOT NULL,                 -- exact job id (01 JOB_TABLE) or a PAUSE_GROUPS name (§2.11); a group never
                                             -- pauses ops_exempt jobs or 14's telegram_bot/alert_dispatch
  key         TEXT NOT NULL CHECK (key IN ('paused')),
  value       TEXT NOT NULL CHECK (value IN ('true','false')),
  reason      TEXT,                          -- operator's free-text note (the reported pause reason is 'operator')
  set_by      TEXT NOT NULL,                 -- 'api' | 'telegram' | 'cli' | operator name
  set_at      INTEGER NOT NULL,
  expires_at  INTEGER,                       -- NULL = until resumed
  PRIMARY KEY (target, key)
) STRICT, WITHOUT ROWID;
```
Writers of `control_state` (the only persistent pause store, 02 §B12): `app/control.py::pause()/resume()` (API, Telegram, `python -m app ops pause|resume`), which write `value='true'` rows under the exact target name; `resume()` deletes only that row and never touches a `data/control/<target>.paused` file (it reports `file_pause_active` when one exists, §2.11).

`job_state` (spec 03 DDL: `job, key, value_json, updated_at INTEGER`) — runtime state only; reserved pseudo-job names start with `_`; no pause is ever kept here (02 §B12):

| `job` | `key` | `value_json` | writer |
|---|---|---|---|
| `_costs` | `resv:<id>` | `{group, provider, est_usd, job, run_id, created_at, expires_at, expired_at}` (`expired_at` NULL until `refresh()` marks it) | `reserve` / `commit` / `release` / `refresh` |
| `_costs` | `soft:<group>:<YYYY-MM>` | `{at, usd_mtd, cap, unit}` | governor (80 % latch, persisted) |
| `_costs` | `hard:<group>:<YYYY-MM>` | `{at, usd_mtd, cap, denied_est_usd}` | governor (100 % latch, persisted) |
| `_costs` | `override:<group>:<YYYY-MM>` | `{extra_usd, by, reason, at}` | CLI / API |
| `_ops` | `boots` | `[{at, kind, prev_ts, downtime_sec, version, orphan_runs}]` (≤ 30) | `recovery.pre_init` |
| `backup_daily` | `last_result` | manifest + `{mirror, pruned}` | `BackupJob` |

`events` (spec 03 §3.6 DDL) — ops rows are written with `INSERT OR IGNORE`, `occupant_key = NULL`, `netuid = NULL` (the table's CHECK requires both NULL together), `event_at = observed_at = now` (unix s), `severity ∈ {info, warning, critical}` (02 §B2), `source ∈ {runtime, cost, ops}` (02 §B9), `payload` JSON and `dedupe_key` per the catalogue in §2.13.2, `run_id` of the emitting job run when there is one, `alerted_at = NULL` always (the dispatcher sets it; a notice sent by the failed-insert fallback has no row, §2.13.2). `collector_runs` — one row per run of `backup_daily`, `ops_guard_5m`, `ops_digest_daily`, `cost_ledger_hourly` via the spec 01 wrapper.

### 3.2 Tables read
`collector_runs` (`job, started_at, finished_at, ok, rows, error, cost_usd` — digest, run rate, orphan count), `scores` (`as_of, spec_version, run_kind, confidence` — digest), `events` (`event_type, severity, observed_at` — 24 h counts in the digest), `schema_version` (`version` — manifest, restore check), `provider_spend`, `job_state`, `control_state` (pause cache, §2.11).

### 3.3 Files
Written: everything under `data/run/` (incl. `heartbeat.json`, `integrity.json`), `data/backups/`, `data/quarantine/`, `data/tmp/`, `data/cache/`, the mirror — never a pause or sentinel file (02 §B12). Read: `.env` (Settings; the bash scripts read `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` only), `~/taosentinel-secrets/deploy.env` (auto-update only), sentinel files at `SENTINEL_ROOT`, `data/control/*.paused` (read-only emergency input), `/proc/sys/kernel/random/boot_id`.

### 3.4 Events emitted
See §2.13.2 (payload schemas). 03 §3.9 is the only catalog (02 §D1); every name below is in it with the payload keys used here. Emitted by this spec: `service_recovered_unclean`, `spend_80pct`, `spend_cap_reached`, `spend_reset`, `budget_override_set`, `backup_failed`, `backup_stale`, `backup_mirror_missing`, `backup_mirror_failed`, `disk_low`, `disk_critical`, `db_integrity_failed`. Emitted by spec 01's wrapper through this spec's `AlertLatch` routing: `collector_failing`, `collector_recovered`. `disk_critical` and `db_integrity_failed` are inserted like every other type and paged by the dispatcher; when the insert itself fails (the DB or the disk may be the problem), `fire()` sends the text direct and nothing is stored (02 §D3 fallback).

---

## 4. Status semantics (absence vs failure) for ops jobs and the governor

| condition | `collector_runs` | `/health` | liveness | alert |
|---|---|---|---|---|
| backup ok, mirror ok | `ok=1, rows=1, note="<name> <MB> mirror=ok"` | `db.last_backup` fresh; `ops.backup.mirror="ok"` | fresh | clears mirror latches |
| backup ok, mirror marker absent | `ok=1, rows=1, note="… mirror=missing"` | `mirror="missing"`, `degraded_reasons += mirror_missing` | fresh | `backup_mirror_missing` (24 h) |
| backup ok, mirror not configured | `ok=1, note="… mirror=off"` | `mirror="off"` | fresh | digest ⚠️ only |
| backup skipped (lock busy) | `ok=1, rows=0, note="busy"` | unchanged | fresh | none |
| insufficient disk / snapshot `quick_check` ≠ ok / VACUUM error | `ok=0, error=…` | `backup_stale` once > 36 h | fresh | `backup_failed` |
| startup backup skipped (recent snapshot) | none (log) | — | — | none |
| governor deny `hard_cap` | caller: spec 09 (`check().allowed` false) ends `partial`, spec 10 (`reserve()` → `None`) skips with `paused: budget` | `spend.<g>.paused=true, paused_reason="budget"`; affected jobs `paused="budget"` | ignored (paused) | `spend_cap_reached` once per (group, month) |
| governor deny `pacing` | caller ends early (`ok=1`, `note="partial:pacing"`) | `spend.<g>.paused_reason="pacing"` while it lasts | fresh | none |
| governor deny `kill_switch` / `zero_cap` | caller skipped | `kill_switch_active` / `paused_reason="zero_cap"` | ignored | none |
| request cap reached (tmc/taostats) | limiter raises `BudgetExhausted` → that run ends `paused: budget` (not a failure, 02 §G1); later runs skip `budget` until the window resets | `spend.tmc.pct ≥ 1`, `paused=true` | ignored (paused) | `spend_cap_reached` (USD keys `0`/`null`; extras `unit="requests"`, `requests`, `request_cap`) |
| `ops_guard_5m` normal | `ok=1, rows=0, note="free=… mirror=…"` | `ops.disk`, heartbeat | fresh | — |
| disk below min / critical | `ok=1` | `degraded_reasons += disk_low / disk_critical` | fresh | `disk_low` (24 h) / `disk_critical` (6 h) |
| dead-man GET fails | `ok=1, note="deadman=fail"` | — | fresh | none (external service alerts on silence) |
| digest send fails | `ok=0, error=…` | — | fresh | `collector_failing` after 5 (i.e. five days) |
| unclean boot | — | `ops.boot_kind` | — | `service_recovered_unclean` |
| DB `quick_check` ≠ ok after unclean boot | — | `degraded_reasons += db_integrity`, `ops.pause_all=true` (integrity hold from `integrity.json`; no pause file); backups continue, backup retention and `storage_retention_daily` suspended | paused jobs ignored | `db_integrity_failed` (immediate; direct only if its insert fails) |
| TMC outage (SSE + REST) | TMC jobs `ok=0`; no SSE rows written (no zero rows, no forward fill) | `tasks.chain_sse.connected=false` | live via `last_attempt` | `collector_failing` after 5 |

---

## 5. Configuration

### 5.1 New `Settings` keys (owned by this spec; added to spec 01's inventory)

| key | type | default | validator / notes |
|---|---|---|---|
| `PAUSE_ALL_PATH` | path | `"./PAUSE_ALL"` | resolved with `settings.path()` |
| `CONTROL_DIR` | path | `"data/control"` | read-only emergency `<target>.paused` inputs (02 §B12) |
| `RUN_DIR` | path | `"data/run"` | lock, heartbeat, penalty files |
| `DB_BACKUP_CRON_HOUR` | int | `3` | 0–23 (UTC) |
| `DB_BACKUP_CRON_MINUTE` | int | `30` | 0–59 |
| `DB_BACKUP_COMPRESS` | bool | `True` | ceded by spec 03 (its Appendix A); gzip copies, §2.7 step 6 |
| `DB_BACKUP_GZIP_LEVEL` | int | `6` | 1–9 |
| `DB_BACKUP_MAX_TOTAL_GB` | float | `12.0` | ≥ 0 (02 §F2); local backup dir only; 0 = no size budget; the 2 newest stamps are never pruned; ceded by spec 03 |
| `DB_BACKUP_MIN_FREE_FACTOR` | float | `1.3` | ≥ 1.0; preflight §2.7 step 3; ceded by spec 03 |
| `DB_BACKUP_KEEP_UNCOMPRESSED` | int | `1` | 02 §F2; 1 ≤ x ≤ `DB_BACKUP_RETENTION`; ≥ 1 because spec 13 snapshots the newest uncompressed `.db` (02 §J2) |
| `DB_BACKUP_MANUAL_RETENTION` | int | `5` | ≥ 1 |
| `DB_BACKUP_MIRROR_MARKER` | str | `".taosentinel-mirror"` | plain file name |
| `DB_BACKUP_MIRROR_MAX_TOTAL_GB` | float | `0.0` | ≥ 0; separate key (02 §F2); 0 = no size budget on the mirror (the USB disk holds the full 7 + 8 rotation) |
| `DB_BACKUP_STARTUP_DELAY_SEC` | int | `600` | 0–3600 |
| `DB_BACKUP_WAIT_FOR_JOBS` | str | `"storage_retention_daily"` | comma-separated job ids (unknown ids ignored); 02 §E3 — there is no `chain_prune` job |
| `DB_BACKUP_WAIT_MAX_SEC` | int | `1200` | 0–3600 |
| `DB_BACKUP_STALE_HOURS` | float | `36.0` | > `DB_BACKUP_INTERVAL_HOURS` |
| `DB_QUICK_CHECK_ON_UNCLEAN` | bool | `True` | |
| `DB_QUICK_CHECK_DELAY_SEC` | int | `300` | |
| `DB_WAL_WARN_MB` | int | `1024` | |
| `DISK_FREE_CRITICAL_GB` | float | `0.5` | < `DISK_FREE_MIN_GB` |
| `OPS_GUARD_ENABLED` | bool | `True` | |
| `OPS_GUARD_INTERVAL_MIN` | int | `5` | 1–15 |
| `OPS_DIGEST_ENABLED` | bool | `True` | |
| `OPS_DIGEST_HOUR_UTC` | int | `9` | 0–23 |
| `OPS_DIGEST_MINUTE` | int | `0` | 0–59 |
| `DEADMAN_ENABLED` | bool | `False` | model validator forces `False` when `DEADMAN_PING_URL` is empty |
| `DEADMAN_PING_URL` | str | `""` | treated as a secret: added to `public_dict()`'s explicit mask list (spec 01's suffix rule covers only `*_TOKEN/*_KEY/*_SECRET`) |
| `WATCHDOG_API_SELF_PROBE_ENABLED` | bool | `True` | |
| `WATCHDOG_API_PROBE_FAILS` | int | `3` | 1–10 |
| `COST_LEDGER_ENABLED` | bool | `True` | flag of `cost_ledger_hourly` (§6.1); listed by spec 01 until now — the governor and this job are 16's (02 §A2, §E2), default unchanged |
| `COST_LEDGER_INTERVAL_MIN` | int | `60` | 5–240; same provenance |
| `COST_SOFT_ALERT_PCT` | float | `0.8` | 0.5–0.99; `spend_80pct` threshold (group `x` uses spec 09's `X_SOFT_ALERT_PCT`); same provenance |
| `COST_HARD_STOP_PCT` | float | `1.0` | 0.5–1.0 |
| `COST_RESERVATION_TTL_SEC` | int | `86400` | ≥ 3600 (a Batch API job may take 24 h) |
| `COST_OVERRIDE_MAX_USD` | float | `100.0` | > 0 |
| `PAT_EXPIRY_WARN_DAYS` | int | `14` | |
| `SECURITY_SELFCHECK_ENABLED` | bool | `True` | |
| `SCRIPT_TMC_RATE_PER_MIN` | int | `2` | model validator: `TMC_RATE_PER_MIN + SCRIPT_TMC_RATE_PER_MIN ≤ 10` |
| `SCRIPT_RATE_SHARE` | float | `0.25` | 0.05–0.5 |

### 5.2 Existing keys this spec consumes (owners in parentheses; names unchanged)

`KILL_SWITCH_PATH`, `SCORING_KILL_SWITCH_PATH`, `DATA_DIR`, `DB_PATH`, `DB_BACKUP_ENABLED`, `STORAGE_RETENTION_CRON_HOUR`/`_MINUTE` (spec 03: 03:00Z, 02 §E3), `DB_BACKUP_DIR`, `DB_BACKUP_MIRROR_DIR` (= the mirror mount, e.g. `/mnt/taosentinel-mirror`), `DB_BACKUP_RETENTION` (7 daily), `DB_BACKUP_WEEKLY_RETENTION` (8 weekly), `DB_BACKUP_INTERVAL_HOURS` (24; now only staleness/probe math), `DB_BACKUP_MIN_AGE_HOURS` (12), `DISK_FREE_MIN_GB` (2.0), `X_MONTHLY_CAP_USD` (25; 75 with official v2, 02 §F2), `LLM_MONTHLY_CAP_USD` (60), `TAOSTATS_MONTHLY_CAP_USD` (0), `TAOSTATS_MONTHLY_CAP_REQ` (10,000; `TAOSTATS_MONTHLY_CALL_CAP` accepted as a deprecated alias, 02 §F2), `TMC_MONTHLY_CAP` (90,000), `TMC_RATE_PER_MIN` (8), `TMC_RATE_BURST` (2), `TMC_HOURLY_CAP` (500), `HEALTH_HOST`, `HEALTH_PORT` (8090), `API_AUTH_TOKEN`, `CONTROL_ALLOWED_ORIGINS`, `SYSTEMD_WATCHDOG_ENABLED`, `SHUTDOWN_*`, `LOG_RETENTION_DAYS` (14), `JOB_FAIL_ALERT_THRESHOLD` (5), `ALERT_REPEAT_MIN`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` (all spec 01); `X_PACING_ENABLED`, `X_SOFT_ALERT_PCT` (group `x` uses it instead of `COST_SOFT_ALERT_PCT`), `X_PROVIDER` (spec 09); `ALERTS_ENABLED`, `TELEGRAM_ENABLED` (spec 14); no per-provider kill-file path key is consumed (retired, 02 §B12, §2.11); `CHAIN_TICKS_BLOCK_STRIDE` (5), `CHAIN_TICKS_RETENTION_DAYS` (30) (02 §F2; disk runbook); `SPEC_VERSION` (spec 01; `SCORING_SPEC_VERSION` is its read-only alias, 02 §F2); `SCORING_SHADOW_SPECS` (spec 12, §7.3.8); `QUANT_WEEKLY_JOB_ENABLED`, `QUANT_WEEKLY_JOB_CRON` (`sun 05:00`, spec 13, 02 §F2).

### 5.3 `~/taosentinel-secrets/deploy.env` (read only by `auto-update.sh`; not `Settings` keys)

| key | default | notes |
|---|---|---|
| `DEPLOY_GIT_URL_BASE` | — (required) | `github.com/<owner>/taosentinel.git` |
| `DEPLOY_GITHUB_PAT` | `""` | fine-grained PAT: *Only select repositories* = `taosentinel`, *Contents: Read-only*, 1-year expiry. Separate from `GITHUB_TOKEN` |
| `DEPLOY_SSH_KEY` | `""` | path to a read-only GitHub deploy key; when set it wins over the PAT |
| `DEPLOY_BRANCH` | `main` | |
| `DEPLOY_RUN_SMOKE_TESTS` | `0` | `1` requires `pip install -e ".[dev]"` in the venv |
| `DEPLOY_HEALTH_URL` | `http://127.0.0.1:8090/health` | |
| `DEPLOY_QUIET_WINDOWS` | `00:10-00:35,06:10-06:35,12:10-12:35,18:10-18:35,02:55-04:45,20:25-23:35` | UTC (02 §E3 times); covers the scoring runs, `storage_retention_daily` (03:00), the 03:30 backup, the 04:30 unattended-upgrades reboot and spec 11's `owner_collect_daily` (20:30–23:30, so a restart never kills its ≤ 3-h run); windows must not cross 00:00 |
| `DEPLOY_NOTIFY` | `1` | `0` silences deploy Telegram messages |

Test-only environment variables: `DEPLOY_GIT_URL` (e.g. `file:///tmp/bare.git`, bypasses auth), `DEPLOY_SYSTEMCTL` (stub command).

### 5.4 Installer / prep environment
`SENTINEL_USER` (default `$SUDO_USER`), `SENTINEL_ROOT` (default repo root), `SENTINEL_SECRETS` (default `/home/$SENTINEL_USER/taosentinel-secrets`), `SENTINEL_MIRROR_DIR` (optional; must equal `DB_BACKUP_MIRROR_DIR`). Process environment (not `Settings` keys): `TAOSENTINEL_ROOT` (unit, spec 01), `XDG_CACHE_HOME` (unit, §2.4), `TAOSENTINEL_PROCESS_ROLE=script` (set by `scripts/with_bucket.py`; `serve` refuses to start in that role).

### 5.5 Kill-switch / pause semantics
As §2.11 (02 §B12). Summary: the app reads a closed set of operator files — `KILL_SWITCH`, `PAUSE_ALL`, `SCORING_KILLED` at the repo root (next tick, no restart) and `data/control/<target>.paused` — and no per-provider kill file; `KILL_SWITCH` stops spend (paid jobs + governor, including scripts; free LongTasks keep running); `PAUSE_ALL` stops every non-`ops_exempt` job but keeps ops jobs, API, Telegram bot and alert dispatch; `SCORING_KILLED` stops publication; per-target pauses (an exact job id or a `PAUSE_GROUPS` name, never a prefix) persist across restarts only as `control_state` rows (`data/control/<target>.paused` is a read-only emergency input the app never writes, reported `emergency_file`); budget pauses clear themselves at the UTC month rollover; the integrity hold clears when `integrity.json` returns to `ok`. `DEPLOY_PAUSED` is read only by `auto-update.sh`.

---

## 6. Cadence, rate limits, cost

### 6.1 Ops jobs (owned here per 02 §E2; spec 01's `JOB_TABLE` mirrors these rows verbatim)

| id | factory (`app.…`) | trigger | UTC time / interval | misfire grace s | timeout s | `ops_exempt` | enabled flag | phase | provider | paid | `min_gap_factor` | liveness max-age s (§2.5) | cost |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `backup_daily` | `storage.backup:BackupJob` | cron `hour=DB_BACKUP_CRON_HOUR minute=DB_BACKUP_CRON_MINUTE` | 03:30Z daily; waits ≤ `DB_BACKUP_WAIT_MAX_SEC` (1,200 s) for `storage_retention_daily` (03:00Z) | 3600 | 5400 (wait + VACUUM + `quick_check` + gzip/verify of a ≈ 11 GB year-1 DB) | yes | `DB_BACKUP_ENABLED` | 0 | — | no | 0 | 178,320 | ≈ 3 × DB of disk I/O; gzip ≈ 30–120 s CPU per GB (**UNVERIFIED**) |
| `ops_guard_5m` | `ops.guard:OpsGuardJob` | interval `minutes=OPS_GUARD_INTERVAL_MIN` | every 5 min | 120 | 60 | yes | `OPS_GUARD_ENABLED` | 0 | — | no | 0 | 780 | local; +288 GETs/day via `Http` provider `deadman` when enabled |
| `ops_digest_daily` | `ops.digest:OpsDigestJob` | cron `hour=OPS_DIGEST_HOUR_UTC minute=OPS_DIGEST_MINUTE` | 09:00Z daily | 3600 | 120 | yes | `OPS_DIGEST_ENABLED` | 0 | — | no | 0 | 173,040 | 1 Telegram message |
| `cost_ledger_hourly` | `costs:CostLedgerJob` | interval `minutes=COST_LEDGER_INTERVAL_MIN` | every 60 min | 600 | 120 | yes | `COST_LEDGER_ENABLED` | 0 | — | no | 0 | 7,440 | local: `flush()` + `refresh()` + latch alerts |

Other `ops_exempt` jobs, owned elsewhere (§2.11; 02 §E1): 03 `storage_retention_daily` (cron 03:00Z), 01 `log_retention_daily`, 14 `alert_dispatch` (Phase 0, implemented by 01 — 02 §D3) and the `telegram_bot` LongTask. None of them is in any `PAUSE_GROUPS` group. This spec adds `ops_guard_5m` and `ops_digest_daily` and now defines `backup_daily` (cron, replacing 01's interval row) and `cost_ledger_hourly` (01's row plus `ops_exempt`, `min_gap_factor=0`); 01 recomputes its declared/available job counts from the final table (02 §E2).

### 6.2 Nightly schedule (UTC) and deploy quiet windows

| time (UTC; 02 §E3) | what | owner |
|---|---|---|
| 00:00 (+06/12/18:00) | `discord_6h` | 08 |
| 00:15 (+06/12/18:15) | `scoring_6h` (official 00:15; provisional others) | 12 |
| 00:20 (+06/12/18:20) | LLM batch submit (`llm_classify_submit`) | 10 |
| Sun 01:00 | `llm_narrative_weekly` | 10 |
| 03:00 | `storage_retention_daily` — every retention policy, incl. the `chain_ticks` prune (there is no `chain_prune` job) | 03 |
| 03:30 | `backup_daily` (waits ≤ 20 min for a running `storage_retention_daily`) | 16 |
| 04:30 | unattended-upgrades reboot window (only when a package requires it) | OS |
| Sun 05:00 | `quant_weekly` (`QUANT_WEEKLY_JOB_CRON=sun 05:00`) | 13 |
| Mon 08:00 | needs-review digest | 14 |
| 09:00 | `ops_digest_daily` | 16 |
| 20:30–23:30 | `owner_collect_daily` (TMC-paced, ends by 23:30 at most) | 11 |

Deploys are deferred inside `DEPLOY_QUIET_WINDOWS` and while `backup.lock` is held.

### 6.3 Rate limits
No new external budgets except the optional dead-man ping (provider `deadman`, 1 req / 5 min). Cross-process sharing per §2.12: service TMC 8/min + scripts 2/min = the 10/min per-IP cap on average (bursts are absorbed by the shared 429 penalty); hourly 500 + script 100 = 600; monthly counters shared via `provider_spend.requests`.

### 6.4 Monthly run-rate (all providers; research 01-web-social-apis §6, specs 05/09/10)

| provider | volume | USD / month | enforced cap |
|---|---|---|---|
| TMC REST (all callers, 05's budget table) | steady ≈ 716 req/day ≈ 21.5 k/month; worst ≈ 40 k/month; peak ≈ 311/h (02 §G3) | 0 | 90,000 req (limiter + governor) |
| TMC SSE | 1 of 2 anonymous connections | 0 | 2 connections |
| GitHub REST (PAT) | ≤ 4,000/h budget | 0 | limiter 4,000/h |
| Discord, t.me, RSS, Wayback, web, subtensor RPC | ≈ 1–2 k req/day | 0 | limiter |
| twitterapi.io (optional) | teams + KOLs + mentions | ≈ 5–20 (spec 09 plan ≈ 12.5) | `X_MONTHLY_CAP_USD` 25 hard |
| Anthropic (optional) | ≈ 2,000 posts/day via Batch | ≈ 17–80 estimated (research 01-web-social-apis §5–§6; spec 10 plan ≈ 40) | `LLM_MONTHLY_CAP_USD` 60 hard |
| official X v2 (optional fallback) | lean plan, team timelines | ≈ 45–75 (spec 09 §6) | same group `x`: only fits if `X_MONTHLY_CAP_USD` is raised to 75 (checklist C2) |
| TaoStats (optional) | owner/whale events | 0 free / 49 Standard | `TAOSTATS_MONTHLY_CAP_REQ` |
| Telegram, Tailscale (personal), dead-man (free tier) | — | 0 | — |
| **Total** | | **0** keyless · **≤ ≈ 85** with X + LLM · **≤ ≈ 134** with TaoStats Standard · **≤ ≈ 184** with official X at 75 + TaoStats Standard (research §6: ≈ 175) | master target ≤ 150 — the last combination exceeds it (operator decision, master §13.1) |

Local resources: process RSS ≤ 300 MB (spec 01) under `MemoryMax=2G`; disk per §2.7 **Size**: peak ≈ 4.7 × DB + 5 GB (DB ≈ 7.8 GB at stride 5 / ≈ 11 GB at stride 1 by month 12, 03 §6, **UNVERIFIED**) → **≥ 60 GB free is required** (02 §I1, amended); smaller disks are unsupported unless tick retention, `DB_BACKUP_MAX_TOTAL_GB` and the weekly backup count are lowered and a USB/NAS mirror is added (20 GB then lasts roughly the first 1–3 months); swap (4 GiB) comes on top. Electricity: a laptop at 15–30 W ≈ 11–22 kWh/month.

---

## 7. Failure modes and fail-safe behaviour

### 7.1 Failure-mode table

| failure | detection | automatic behaviour | operator sees | runbook |
|---|---|---|---|---|
| Event loop wedged / job never returns | stale `last_run` in `_jobs_live()` | ping withheld → SIGABRT at 120 s → restart (10 s) | journal `watchdog timeout`; `service_recovered_unclean` (`process_crash`) | §7.3.10 |
| uvicorn task died | `health_task.done()` / 3 failed self-probes | not live → restart | same | — |
| Crash loop (5 starts in 60 s) | systemd start limit | unit `failed`; `OnFailure=` notifier | Telegram "unit FAILED" + log tail | §7.3.10 |
| Power cut / battery death | stale heartbeat, new `boot_id` | recovery order §2.6; `quick_check` at +5 min | `service_recovered_unclean` (`host_unclean`) | §7.3.11 |
| DB corrupt after crash | `quick_check` ≠ ok | integrity hold from `integrity.json` (no pause file): every non-`ops_exempt` job and `storage_retention_daily` paused, backup retention suspended; nothing else written by collectors | `db_integrity_failed` (immediate; direct only if its insert fails) | §7.3.9 |
| TMC outage (SSE and REST both failing) | reconnect attempts fail; TMC jobs `ok=0` | liveness stays true through `last_attempt` (§2.5 amendment 5) — no restart loop; affected metrics go `None` after their freshness window and confidence drops | `collector_failing:<chain job>`, `/health.tasks.chain_sse.connected=false` | §7.3.1 |
| Second instance started by hand | instance flock | new process exits 3 | stderr message | — |
| Backup fails (disk, VACUUM, integrity) | `BackupJob` exception | primary untouched; retried next night (and by the boot-time backup when > 12 h old) | `backup_failed`, `backup_stale` > 36 h | §7.3.5 |
| USB/NAS mirror unmounted | marker absent | primary backup kept; mirror skipped; mount point never written | `backup_mirror_missing` (24 h) | remount; `touch` marker only on the real disk |
| Disk nearly full | `ops_guard_5m` | backups prune to the minimum set first; SQLite fails writes safely (`SQLITE_FULL` rolls back) | `disk_low` / `disk_critical` | §7.3.5 |
| Paid provider overspend risk | governor decision | deny + latch + auto-pause of that provider's jobs; LLM falls back to rules (spec 10) | `spend_80pct`, `spend_cap_reached` | §2.10 override |
| Restore would forget month-to-date spend | restore step 4 | `max(snapshot, current)` carry-over | CLI output | §7.3.9 |
| Bad deploy (syntax, import, config, schema on empty DB, smoke, restart, health) | gate step | rollback to previous SHA + restart | Telegram "Deploy rolled back (<why>)" | §7.3.6 |
| Deploy PAT expired / network down | `git fetch` fails | nothing changes; 12 consecutive failures → one alert | Telegram after 1 h | §7.3.3 |
| Non-additive migration pushed | `[manual-deploy]` in commit messages | auto-deploy refused once per SHA | "Manual deploy required" | §7.3.6 |
| TMC 429 storm | `Http` 429 → `penalize` (+ penalty file) | backoff, retries ≤ 2, then job failure | `rate_limits.tmc.penalized_until`, `collector_failing` | §7.3.2 |
| Discord IP ban | 429/403 on every Discord call | jobs fail; limiter's 200/h cap keeps our own contribution tiny | `collector_failing:discord_*` | §7.3.4 |
| NTP unreachable at boot | `systemd-time-wait-sync` timeout 180 s | start on RTC time | digest shows `NTPSynchronized=no` via `timedatectl show` | check network |
| Telegram down | `send_alert` exception | dispatched alerts retried (`alerted_at` stays NULL; 14's 10-tick poison rule); failed-insert fallback sends and process notices are dropped | `/api/health/services.telegram` fail | — |
| Laptop fully off | nothing local can alert | optional dead-man service alerts on missing pings | external email/Telegram | §7.3.11 |

### 7.2 Fail-safe principles applied here
1. Nothing heavy runs before `READY=1`; backups, integrity checks and catch-up are backgrounded or staggered.
2. Ops code never raises into the scheduler (spec 01 wrapper) and never writes to the live DB from a second process except through `backup_to` (read-only snapshot) and the governor's increments.
3. A missing mirror, missing dead-man URL or missing Telegram token degrades to logging — never to a crash.
4. Money is fail-closed (deny on doubt: zero cap, kill switch, latch), data collection is fail-open (keyless collectors keep running under `KILL_SWITCH`).
5. The restore path never deletes anything (quarantine) and refuses to run under the live service.
6. The deploy path never leaves unverified code running: every failure after `reset --hard` rolls back.

### 7.3 Runbooks

Conventions: `cd ~/taosentinel`; `API="-H X-Auth-Token:$API_AUTH_TOKEN"` after `set -a; . ./.env; set +a`; `TODAY=$(date -u +%F)`.

**7.3.1 Collector failing** (`collector_failing:<id>` alert)
1. `curl -s localhost:8090/health | jq '.collectors["<id>"]'` → `last_error`, `consecutive_failures`, `paused`.
2. `sqlite3 data/taosentinel.db "select started_at, ok, rows, error from collector_runs where job='<id>' order by started_at desc limit 10"`.
3. `grep '"<id>' data/logs/$TODAY.jsonl | tail -20`.
4. Classify `last_error`: `401`/`403 Bad credentials` → key expired (§7.3.3 / §7.3.7); `429` → rate (§7.3.2 / §7.3.4); `5xx`/`timeout`/`ConnectError` → provider outage: do nothing — the metrics go `None` after their freshness window and confidence drops (master §1.3); `KeyError`/`ValidationError`/parse errors → provider changed its schema: pause to stop noise (`curl -s -X POST $API localhost:8090/api/v1/control/pause/<id>`) and fix the code; `database is locked` → look for an ad-hoc script holding a write transaction (`fuser data/taosentinel.db`).
5. Test a fix without touching the service's budget: `python -m scripts.with_bucket --providers <p> -- python -m app run-job <id>`.
6. Resolution: the next successful run sends `collector_recovered`; resume if paused (`…/control/resume/<id>`).

**7.3.2 TMC 429 storm**
Symptoms: 05's `tmc_*` REST jobs (e.g. `tmc_daily`, `tmc_weekly`) failing with 429, `/health.rate_limits.tmc.penalized_until` repeatedly set; SSE usually unaffected (not metered) unless a third SSE connection is attempted (2 per IP).
1. Count: `grep '"provider":"tmc"' data/logs/$TODAY.jsonl | grep -c '"status":429'`; check `X-RateLimit-Remaining` in the logged `observe` lines.
2. Find other consumers on the same public IP: `ps aux | grep -E 'scripts\.|app (run-job|serve)'` on the laptop; any TMC script on the Windows PC; a revived Pi bot; open TMC connections (≤ 2 SSE allowed per IP; `ss -n` prints addresses, not host names): `for ip in $(getent ahostsv4 api.taomarketcap.com | awk '{print $1}' | sort -u); do ss -Htn state established dst "$ip"; done | wc -l`.
3. Stop them (or re-run them through `scripts.with_bucket`).
4. Let the window reset: `touch data/control/tmc.paused` (operator emergency file for group `tmc`, §2.11: every job that calls `TmcClient`; `chain_sse` keeps streaming; the service reads the file, never removes it; jobs report `paused: emergency_file`); wait 60 min (hourly cap 600); `rm data/control/tmc.paused`.
5. If it persists: set `TMC_RATE_PER_MIN=6` in `.env` and `sudo systemctl restart taosentinel`; check `python -m app costs status` for the monthly request count — near 90 k, `python -m app ops pause tmc_weekly --reason "tmc monthly cap"` (exact id; reported `operator`) until the 1st (UTC), then `ops resume tmc_weekly`.
6. Persistent even at 6/min with nothing else on the IP → TMC changed its limits: re-read `https://api.taomarketcap.com/llms.txt`; consider TMC Pro (checklist C4).

**7.3.3 GitHub PAT expired**
*Collector token* (`GITHUB_TOKEN`, checklist B1): `github_*` jobs fail with 401; `/api/health/services.github` fails.
1. Confirm: `curl -s -H "Authorization: Bearer $GITHUB_TOKEN" https://api.github.com/rate_limit | jq .message` → `"Bad credentials"`.
2. github.com → Settings → Developer settings → Fine-grained tokens → generate: *Public repositories (read-only)*, 1-year expiry.
3. Edit `.env` `GITHUB_TOKEN=…`; `sudo systemctl restart taosentinel` (provider clients build auth headers at start — a restart is the safe path).
4. Verify: `curl -s localhost:8090/api/health/services | jq .services.github.detail` → `…/5000 remaining`.
*Deploy token* (`DEPLOY_GITHUB_PAT`): alert "git fetch failing for 1 h"; `journalctl -u taosentinel-update -n 20`. Create a fine-grained PAT (repository `taosentinel` only, *Contents: read-only*), write it to `~/taosentinel-secrets/deploy.env`, keep mode 600, `sudo systemctl start taosentinel-update.service`, expect `journalctl -u taosentinel-update -n 3` without `ERROR`. The digest warns `PAT_EXPIRY_WARN_DAYS` (14) before either token expires when GitHub returns the expiration header.

**7.3.4 Discord invalid-request (Cloudflare) ban**
Symptoms: every Discord call (invites, widgets, and possibly the bot gateway and the operator's own Discord client on the same public IP) returns 429/403, often an HTML Cloudflare page instead of JSON. Cause: ≥ 10,000 401/403/429 responses in 10 min from the IP (research 02-gapfill-discord §Discord rules). The limiter caps our own traffic at 200/h, so a ban usually means a loop in new code or another tool on the IP.
1. Stop all Discord traffic at once: `touch data/control/discord.paused` (group `discord`, §2.11: `discord_6h`, `discord_widget_1h` and the `discord_gateway` task; `community.paused` also stops `telegram_daily`).
2. Inspect: `grep '"provider":"discord"' data/logs/$TODAY.jsonl | jq -r '.data.status' | sort | uniq -c` — look for thousands of 403 (widget-disabled guilds probed too often) or 429.
3. Fix the cause (e.g. `DISCORD_WIDGET_ENABLED=false` until spec 08's weekly rule is honoured), restart if code or config changed.
4. Wait ≥ 1 h (ban duration **UNVERIFIED**; typically about an hour), then probe once: `curl -s -o /dev/null -w '%{http_code}\n' https://discord.com/api/v10/gateway` → `200`.
5. `rm data/control/discord.paused`; watch the next `discord_6h` run.

**7.3.5 Disk full**
1. Measure: `df -h ~/taosentinel/data`; `du -sh data/* data/backups/* | sort -h | tail`; `journalctl --disk-usage`; biggest tables: `sqlite3 data/taosentinel.db "select name, sum(pgsize)/1e6 mb from dbstat group by name order by 2 desc limit 10"` (needs `dbstat`; otherwise `sqlite3_analyzer`).
2. Stop growth: `touch PAUSE_ALL` (ops jobs, API and Telegram keep running).
3. Free space, in order: `python -m app backup prune --keep-daily 2 --keep-weekly 2`; `find data/logs -name '*.jsonl*' -mtime +3 -delete`; `sudo journalctl --vacuum-size=200M`; `sudo apt-get clean`; `rm -rf data/tmp/*` (service stopped or no restore in progress).
4. Reduce the tick table: `.env` `CHAIN_TICKS_BLOCK_STRIDE=10` (default 5) and/or `CHAIN_TICKS_RETENTION_DAYS=7` (default 30); restart; prune now with `python -m app db retention --only chain_prune` (spec 03 CLI; `chain_prune` is 03 §2.7's id of the `chain_ticks` retention **policy** inside `storage_retention_daily` — there is no `chain_prune` job, 02 §E3; below `DISK_FREE_MIN_GB` the destructive-policy backup gate is bypassed).
5. Shrink the WAL (safe while running): `sqlite3 data/taosentinel.db "PRAGMA wal_checkpoint(TRUNCATE)"`. Never delete `-wal`/`-shm` files by hand.
6. Reclaim free pages (needs free space ≥ DB size): `python -m app backup now --tag compact` → `sudo systemctl stop taosentinel` → `python -m app backup restore <compact stamp> --from local --yes` (the snapshot is already vacuumed) → start.
7. `rm PAUSE_ALL`; confirm `disk_low` cleared in `/health.degraded_reasons`.

**7.3.6 Schema migration**
Rule: every migration shipped through auto-update is **additive** (new tables, new nullable/defaulted columns, new indexes) so the previous release runs on the new schema — rollback depends on it. Anything else (rename, drop, type change, large backfill) ships in two releases (add + backfill; later drop), and the destructive release carries `[manual-deploy]` in a commit message.
Manual deploy procedure:
1. `touch DEPLOY_PAUSED` (freeze the timer).
2. `python -m app backup now --tag pre-migration` → `python -m app backup verify <stamp>` exit 0.
3. Rehearse on a copy: `python -m app backup restore <stamp> --target data/tmp/migtest.db`; `git fetch && git checkout <new sha>` in a separate worktree (`git worktree add ../ts-next <sha>`); in it: `DB_PATH=$PWD/../taosentinel/data/tmp/migtest.db ../taosentinel/.venv/bin/python -m app check` → exit 0; note the duration.
4. `sudo systemctl stop taosentinel`; `git pull` (or `git reset --hard <sha>`); `pip install -r requirements.lock && pip install --no-deps -e .` if deps changed; `sudo systemctl start taosentinel` (migrations run in `apply_schema`; if they may exceed `TimeoutStartSec=120`, run `python -m app check` against the live DB first while stopped).
5. Verify: `curl -s localhost:8090/health | jq .db.schema_version` = new version; `pytest -q -m smoke`.
6. `rm DEPLOY_PAUSED`; `git worktree remove ../ts-next`.
Rollback: stop → `python -m app backup restore <pre-migration stamp> --yes` → `git reset --hard <previous sha>` → start.

**7.3.7 Rotate a token**
| token | steps | restart? |
|---|---|---|
| `API_AUTH_TOKEN` | `openssl rand -hex 32` → `.env`; `curl -X POST -H "X-Auth-Token: <old>" localhost:8090/api/v1/control/reload-config` (`require_auth` reads the module attribute dynamically — the new token is live immediately); UI: update `NEXT_PUBLIC_API_TOKEN`, `cd frontend && npm run build`, `sudo systemctl restart taosentinel-frontend` (the token is baked at build time); trading bot (spec 15): nothing to rotate — it reads the open read-tier feed `GET /api/v1/scores/latest` served by 14 §3.5 (02 §H1; `FEED_AUTH_REQUIRED` is retired) | backend no |
| `TELEGRAM_BOT_TOKEN` | BotFather `/revoke` → new token → `.env` | yes (long-poll bot holds the old token) |
| `GITHUB_TOKEN`, `TWITTERAPI_IO_KEY`, `X_BEARER_TOKEN`, `ANTHROPIC_API_KEY`, `TAOSTATS_API_KEY`, `DISCORD_BOT_TOKEN`, `ONFINALITY_API_KEY` | issue new key at the provider → `.env` → revoke the old one | yes (`sudo systemctl restart taosentinel`) |
| `DEPLOY_GITHUB_PAT` | new fine-grained PAT → `deploy.env` (mode 600) → revoke old | no |
| `DEADMAN_PING_URL` | new check → `.env` → reload-config | no |
After any rotation: `python -m app check` exit 0; `/api/health/services` all expected `ok`.

**7.3.8 `spec_version` bump + shadow run**
Weights, normalisers and gate multipliers are frozen per `spec_version` (master §7.3); any change is a new version and old rows are never rewritten (master §6).
1. Implement `ts-1.1` as an additional registered spec in `app/scoring/spec.py` (never edit `ts-1.0`); unit tests pin both.
2. Deploy normally (additive; no migration).
3. Shadow forward: `.env` `SCORING_SHADOW_SPECS="ts-1.1"` (spec 12 §2.13) + `reload-config` (or restart) → every scoring run also writes `ts-1.1` rows (`scores.shadow = 1`) that the feed never serves and that emit no events.
4. Shadow history: `python -m app.scoring backfill --spec ts-1.1 --from <date>` (spec 12 §2.13 item 5; local DB work only, no provider traffic) → scores the official slots oldest-first in reconstructed mode from stored `metric_values`, rows marked `backfilled = 1`. The rescore endpoint (`POST /api/v1/control/rescore` with `{"as_of":"YYYY-MM-DD","spec":"ts-1.1","shadow":true}`, spec 14) scores one slot only and refuses slots that already have rows.
5. Observe ≥ 14 days (≥ 56 runs): Spearman(`ts-1.0`, `ts-1.1`) composite per `as_of`, tier churn, rank autocorrelation (target ≥ 0.9), newly flagged occupants; `python -m app.quant factor --spec ts-1.1 …` and the gates of master §10 / spec 13 if the change is meant to improve predictive power.
6. Promote: `.env` `SPEC_VERSION=ts-1.1` (feed default), `SCORING_SHADOW_SPECS=ts-1.0` (keep the old series warm for rollback), `sudo systemctl restart taosentinel`. The trading bot treats `ts-1.1` as neutral until it is added to its `SENTINEL_ACCEPTED_SPEC_VERSIONS` (spec 15) — fail-safe by construction.
7. After 7 clean days, drop `ts-1.0` from the shadow list. Rollback at any point = set `SPEC_VERSION` back and restart. The same procedure applies to `LLM_CLASSIFIER_VERSION` bumps (spec 10).

**7.3.9 Restore from backup**
1. `sudo systemctl stop taosentinel-update.timer taosentinel` (the frontend may keep running).
2. `python -m app backup list` (add `--mirror` if local backups are gone); choose the newest snapshot that predates the damage; `python -m app backup verify <stamp> --full` → exit 0.
3. `python -m app backup restore <stamp> [--from mirror] --yes` → quarantines the current DB, carries over month-to-date spend, swaps the snapshot in.
4. The restore deleted `data/run/integrity.json`, which lifts the integrity hold (§2.11); `rm ./PAUSE_ALL` if you created it; `sudo systemctl start taosentinel taosentinel-update.timer`.
5. Verify: `/health` `status=ok`, `db.schema_version` expected, `python -m app costs status` matches provider dashboards (else `costs set`).
6. Expect: collectors re-fetch from their restored watermarks (idempotent `INSERT OR IGNORE`); chain hourly rows repaired from TMC analytics for up to the analytics window; per-block ticks, web checks and count snapshots between snapshot and now are lost; scores for missed `as_of` are recomputed by spec 12 (`backfilled=true`); the bot's feed `as_of` may step backwards once (spec 15 ignores older batches).
7. Keep `data/quarantine/<stamp>/` until the next weekly backup has run, then delete it by hand.

**7.3.10 Unit failed / restart loop**
1. `systemctl status taosentinel`; `journalctl -u taosentinel -n 100 --no-pager`; `tail -100 data/service.log`.
2. Watchdog restarts: journal `Watchdog timeout` + log `watchdog_withheld` naming the stale job ids → that job's code hangs outside its `timeout_sec` (e.g. a blocking call in a coroutine); pause it and fix.
3. Start-limit (`start-limit-hit`): usually `.env` validation (`python -m app check` shows the pydantic error), DB locked/corrupt (§7.3.9), or a second instance holding the lock (`fuser data/run/taosentinel.lock`).
4. After fixing: `sudo systemctl reset-failed taosentinel && sudo systemctl start taosentinel`.

**7.3.11 After a power cut (checklist)**
`systemctl is-active taosentinel` = `active` without login · `/health.ops.boot_kind` = `host_unclean` with a plausible `downtime_before_boot_sec` · Telegram got `service_recovered_unclean` · `journalctl -u taosentinel | grep -i quick_check` → `ok` and `jq -r .result data/run/integrity.json` → `ok` · `chain_sse` last event < 60 s old (`/health.collectors.chain_sse.last_event_at`, spec 05) · `tmc_daily` ran after boot (or was skipped as recent) · `timedatectl show -p NTPSynchronized` = `yes` · mirror still mounted (`ls /mnt/taosentinel-mirror/.taosentinel-mirror`) · if the outage spanned 03:30Z, the delayed startup backup exists in `data/backups/`.

---

## 8. Implementation plan

### 8.1 Files

| file | content | port from (old bot `E:\SN_Trader\SubnetTrader\…`) |
|---|---|---|
| `app/costs.py` | `SpendGroup`, `SPEND_GROUPS` (`x`, `llm`, `taostats` per 02 §G1 + `tmc`), `group_of`, `spend_provider_for`, `Reservation`, `Decision`, `UnknownReservation`, `CostGovernor` (public API = 02 §G5, §2.10), `CostLedgerJob` (§6.1: `flush()` + `refresh()` + latch alerts), `PRICES`, `estimate_llm_usd`, `estimate_batch` | — |
| `app/control.py` | `PAUSE_GROUPS`, `PAUSE_REASONS`, `UnknownTarget`, `declared_ids()`, `never_grouped()`, `resolve_targets(name) -> list[str]` (exact id or group name only, §2.11), `effective_pause(obj, rt) -> str \| None`, `file_paused(job_id) -> bool` (read-only; `data/control/<id>.paused` or `<group>.paused` for a group containing the job), `pause(name, *, by, reason, hours=None) -> dict` / `resume(name, *, by) -> dict` (spec 14's names; async; write/delete the `control_state` row, update the cache; never create or delete a `.paused` file — `resume` returns `file_pause_active` / `still_paused_by` when the target stays paused, 02 §B12), `is_paused(name) -> bool` (sync, cache + file), `refresh_cache(db) -> None`, `kill_switch_active()`, `integrity_hold_active()` (`data/run/integrity.json` result ≠ `ok`), `pause_all_active()` (`./PAUSE_ALL` or the integrity hold) | sentinel-path convention of `app/config.py` 447/510/574 |
| `app/ops/recovery.py` | `InstanceLock`, `pre_init(rt)`, `write_heartbeat(rt)`, `remove_heartbeat()`, `classify_boot(prev: dict \| None, boot_id: str, boots: list) -> BootInfo`, `count_orphan_runs(db, boot_at) -> int`, `post_boot_check(rt)`, `report(rt, info)` | — |
| `app/ops/guard.py` | `OpsGuardJob(CollectorJob)` | — |
| `app/ops/digest.py` | `async def build_digest(rt) -> str`, `OpsDigestJob(CollectorJob)` | — |
| `app/ops/security.py` | `def selfcheck(settings) -> dict` | — |
| `app/ops/cli.py` | `register(subparsers)` for `backup`, `costs`, `ops {digest --print, selfcheck, boots, integrity-check, pause <target> [--reason R] [--hours H], resume <target>, rearm owner_rpc_seed}` — `rearm owner_rpc_seed` deletes `job_state(job='owner_rpc_seed', key='rpc_seed_done')` (absent row = armed; 11 §6, the same delete 17's `backfill_rpc` does) and accepts no other job id (spec 03 ceded `db backup`/`db restore`; no aliases) | `scripts/backup_db.py` 1-53 (CLI shape, `--dir/--mirror/--retention`, no `connect()` at 35) |
| `app/storage/backup.py` | `create_backup`, `select_keep(stamps, daily, weekly) -> set[str]`, `apply_retention`, `write_manifest`, `compress_and_verify`, `mirror_copy`, `verify_snapshot`, `restore_snapshot`, `latest_backup_age_hours`, `BackupJob`, `BackupResult`, `BackupError` | `app/storage/backup.py` 1-97 (`_stamp` 21-24, `latest_backup_age_hours` 27-36, prune loop 39-52, mirror `.tmp` + `replace` 74-85 minus the `mkdir`), `main.py` 1294-1331 (job body + alert texts), 323-340 (startup guard); `Database.backup_to` = `db.py` 38-80 verbatim (spec 03) |
| `app/notifications/alerts.py` | `AlertLatch.fire(key, text, *, event_type=None, severity='warning', payload=None, dedupe_key=None, source='runtime')` (02 §D3: row via 03's `emit_event`; direct `send_alert` only when that DB write fails, or for a notice without `event_type`; the dispatcher itself is 01's `app/alerts/dispatch.py`, Phase 0) | spec 01 |
| `app/ratelimit.py` | penalty-file sharing in `penalize()` / `acquire()` | spec 01 |
| `app/jobs.py`, `app/runtime.py` | amendments: cron `interval_min()`, liveness warm-up from `rt.started_at`, `manual` jobs exempt, `LongTask.paused_reason()`, `ops_exempt` attribute (set on the §2.11 set), API-task liveness + self-probe, `remove_heartbeat()` at the end of `shutdown_services()`, cost reconciliation in the wrapper, JOB_TABLE rows of §6.1, `pre_init` first in `init_services()`, delayed startup backup | spec 01 (itself ported from `main.py` 359-447, 3308-3382) |
| `scripts/with_bucket.py` | CLI + `script_runtime()` | — |
| `ops/laptop-prep.sh` | §2.2 | `install.sh` 1-80 (checks) |
| `ops/install-systemd.sh` | §2.4 | `ops/install-systemd.sh` 1-41 |
| `ops/notify-failure.sh` | §2.4 | — |
| `ops/systemd/taosentinel.service`, `taosentinel-frontend.service`, `taosentinel-notify-failure@.service` | §2.4 verbatim | `ops/systemd/subnettrader.service` 1-52, `subnettrader-frontend.service` 1-29 |
| `deploy/auto-update.sh`, `deploy/taosentinel-update.service`, `deploy/taosentinel-update.timer` | §2.9 / §2.4 verbatim | `deploy/auto-update.sh` 1-179, `deploy/subnettrader-update.service` 1-16, `.timer` 1-12 |
| `tests/ops/…` | §9 | `tests/test_graceful_shutdown.py` 25-31 (monkeypatch `_hard_exit`) |

### 8.2 Key signatures not shown above

```python
# app/storage/backup.py
@dataclass(slots=True)
class BackupResult:
    name: str; path: str; size_bytes: int; sha256: str; gz_path: str | None; gz_size_bytes: int | None
    schema_version: int; mirror: str            # "ok" | "missing" | "failed" | "off"
    mirror_error: str | None; pruned: int; duration_sec: float
    def as_dict(self) -> dict                 # spec 01 §3 result keys (name, path, size_bytes, integrity, mirror_error, pruned)
async def create_backup(db, *, backup_dir: Path, mirror_dir: str = "", retention: int = 7,
                        weekly_retention: int = 8, compress: bool = True, max_total_gb: float = 12.0,
                        tag: str | None = None) -> BackupResult
def select_keep(stamps: list[str], daily: int, weekly: int) -> set[str]      # pure; GFS rule of §2.7 step 8
def apply_retention(dir: Path, *, daily: int, weekly: int, keep_uncompressed: int, budget_gb: float | None) -> list[str]
async def verify_snapshot(path: Path, *, full: bool = False) -> tuple[bool, str]
async def restore_snapshot(stamp_or_path: str, *, source: Literal["local", "mirror"], target: Path | None, yes: bool) -> int   # exit code 0/2/3
def latest_backup_age_hours(backup_dir: str | Path) -> float | None
# app/ops/recovery.py
@dataclass(slots=True)
class BootInfo:
    kind: Literal["first_boot", "clean", "process_crash", "host_unclean"]
    prev_ts: int | None; downtime_sec: int | None; boot_id: str; orphan_runs: int = 0     # unix s (02 §B1)
async def pre_init(rt: "Runtime") -> BootInfo
async def post_boot_check(rt: "Runtime", info: BootInfo) -> None     # background task; quick_check on unclean kinds
# app/control.py
def effective_pause(obj: "CollectorJob | LongTask", rt: "Runtime") -> str | None   # §2.11 precedence
async def refresh_cache(db: "Database") -> None      # SELECT target, value, expires_at FROM control_state WHERE key='paused'; expand via resolve_targets()
def pause_all_active() -> bool                                                        # PAUSE_ALL_PATH exists or integrity_hold_active()
# app/jobs.py (spec 01 amendment)
class LongTask:  last_attempt: datetime | None                                        # §2.5 amendment 5
```

### 8.3 Step order
1. **Phase 0 (with spec 01, which also ships the `alert_dispatch` job, `ops_exempt` — 02 §D3):** `control.py` (with `PAUSE_GROUPS`) → `costs.py` (full) → `storage/backup.py` (with spec 03's `backup_to`) → `ops/recovery.py` → `ops/guard.py`, `ops/digest.py` (every digest section tolerates missing later-phase tables: `sqlite3.OperationalError` → section omitted) → `ops/security.py` → `ops/cli.py` → spec 01 amendments (§8.1 rows) → units, `install-systemd.sh`, `notify-failure.sh`, `laptop-prep.sh` → tests §9 green.
2. **Laptop:** `sudo ops/laptop-prep.sh --user <login>` → reboot → `sudo ops/install-systemd.sh` → §10 A–E.
3. **When the private repo exists and the operator opts in (checklist A6):** create `deploy.env` → `sudo ops/install-systemd.sh --with-update-timer` → §10 F.
4. **Phase 9:** specs 09/10 call exactly the 02 §G5 API (`check` / `reserve` → `commit` | `release` / `record`) with keyword arguments → §10 G.
5. **Phase 11:** `--with-frontend` → §10 H. (Alert delivery needs nothing here: `alert_dispatch` has run since Phase 0; 14's phases add only its API and Telegram commands, 02 §D3.)

---

## 9. Tests

All under `tests/ops/`; fixtures from spec 01 (`db`, `rt`, `alerts_sink`, `respx_mock`, `fake_clock`). Filesystem work uses `tmp_path`; the bash tests are `@pytest.mark.skipif(sys.platform != "linux")`.

| file | cases |
|---|---|
| `test_backup.py` | snapshot produces `.db` + `.db.gz` + `.json`; manifest sha256 equals the `.db` and the decompressed `.gz`; a `quick_check` failure (monkeypatched) leaves no final file and raises; leftover `<name>.db.tmp` / `.gz.tmp` older than 1 h removed, younger kept; `busy` lock → `rows=0, note="busy"`; preflight with a monkeypatched `disk_usage` below need → `BackupError("insufficient_disk…")` and `backup_failed` fired with 03's required keys `error, last_good_name, last_good_age_h` (accepted by `emit_event`); waits for a fake in-flight `storage_retention_daily` then proceeds after `DB_BACKUP_WAIT_MAX_SEC`; gzip failure (monkeypatched) → verified `.db` kept, job ok; CLI path never calls `apply_schema()` on the source DB; manifest `counts` null for a missing table |
| `test_retention.py` | `select_keep`: 60 consecutive daily stamps ending 2026-09-23 → exactly the 7 newest ∪ newest-of-week for the 8 newest ISO weeks (expected set hard-coded); irregular stamps (gaps, two per day); `keep_uncompressed=1` deletes `.db` only where a verified `.gz` exists; budget pruning removes weekly-only stamps first and never the 2 newest; `max_total_gb=0` = unlimited; `integrity.json` with `result≠ok` → no file deleted; `manual/` retention independent |
| `test_mirror.py` | mirror dir absent → `mirror="missing"`, no directory created, `backup_mirror_missing` fired once across two runs (latch), primary ok; dir present without marker → `missing`; marker present → files copied via `.tmp` and sizes equal; copy `OSError` → `failed` + `backup_mirror_failed`; not configured → `off`, no alert |
| `test_restore.py` | refuses (exit 2) while the instance lock is held and no `--target`; drill `--target` works while locked; sha mismatch → refuse; `integrity_check` fail → refuse; refuses (exit 2) when a listener accepts on `HEALTH_PORT` even without the lock; newer `schema_version` than `CODE_SCHEMA_VERSION` → refuse (exit 3); live restore quarantines db/wal/shm, swaps and deletes `integrity.json`; spend carry-over takes `max()` per (provider, month) and copies `_costs` latches |
| `test_costs.py` | `reserve` returns a `Reservation` and persists `resv:*`, returns `None` when denied (`check()` gives the reason); a second `CostGovernor` on the same DB (simulated restart) commits by `res.id` to the reservation's provider; a reservation past its TTL stops counting in `reserved` but can still be committed; `record(p, 0.0, requests=1)` counts 1 request (buffered until `flush()`), `record(p, 0.5)` books USD without a request (default `requests=0`, 02 §G5); `commit` increments `usd` (never overwrites: two governors on one DB each commit 1.0 → 2.0); deny when `usd_mtd + reserved + est > cap` → `hard_cap`, latch persisted, `is_exhausted("x")` true for group key `x` and member `twitterapi_io`; `group_of("anthropic").name == "llm"` and `group_of("llm")` is the same group (02 §G1); soft alert fires exactly once when crossing 80 % and **not again after a new `CostGovernor` instance loads**; paid group with cap 0 → `zero_cap`; `KILL_SWITCH` file → `kill_switch` for paid, allowed for `tmc`; pacing on `x` with run-rate fixture (spec 09 numbers: mtd 0.50, est 0.02, 0.10/day × 20 d, cap 1 → deny) and allowed with `X_PACING_ENABLED=false`; fake clock to next month → `is_exhausted` false, `spend_reset` event; override raises effective cap and clears the latch; expired reservation dropped by `refresh()`; `run_total` reconciliation: job commits 0.30 and returns `cost_usd=0.30` → no extra record; returns 0.50 → extra 0.20 recorded; TMC request cap: 80 % alert with `unit="requests"`; a positional argument after `usd`/`est_usd`/`actual_usd` raises `TypeError` for `record`, `reserve` and `commit` (02 §G5 keyword-only contract); `units_json` of a new row starts from `{}` and is summed per key; `updated_at` is an int; `spend_80pct`/`spend_cap_reached` payloads pass 03's required keys (`group, month, usd_mtd, cap_usd, …`) plus `provider` = group, `source='cost'`, `dedupe_key='x\|<month>\|80'`/`'…\|cap'`, and a second insert with the same key is a no-op; `snapshot()` has `usd, cap_usd, pct, paused, reserved_usd` for every group (02 §G6) |
| `test_control.py` | precedence table of §2.11 (parametrised); `./PAUSE_ALL` and the integrity hold (`integrity.json` result ≠ `ok`) each pause a collector but none of the §2.11 `ops_exempt` set (`backup_daily`, `ops_guard_5m`, `ops_digest_daily`, `cost_ledger_hourly`, `alert_dispatch`, `telegram_bot`); the hold also pauses `storage_retention_daily`, `PAUSE_ALL` does not; `KILL_SWITCH` pauses a `paid=True` job but not a free LongTask; a `control_state` row `target='github'` (reported `operator`) and the file `data/control/github.paused` (reported `emergency_file`) each pause exactly `PAUSE_GROUPS["github"]`, not `tmc_daily`; `resolve_targets`: an exact id → `[id]`, a group → its members minus `never_grouped()`, anything else (`github_`, `tmc_d`, `telegram`, a provider name such as `tme`) → `UnknownTarget` (API 404, CLI exit 2) — no prefix, provider or substring match; `tmc` pauses `owner_collect_daily`, `github_html_probe` and `backfill_tmc` but never `chain_sse`; `chain` pauses `chain_sse`; static checks over `PAUSE_GROUPS`: every member is a `JOB_TABLE` id, no member is `ops_exempt`, `alert_dispatch` or `telegram_bot`, group names and job ids are disjoint, and every non-exempt `JOB_TABLE` id is in at least one group; an exact-id row still pauses an `ops_exempt` job (e.g. `backup_daily`); a row written through a second connection is honoured after `refresh_cache()`; `expires_at` in the past → not paused; resume deletes only the row, leaves the file, returns `file_pause_active`, and the target stays paused until the file is `rm`-ed; resuming `github_resolve` while a `github` row exists returns `still_paused_by: github`; an unknown `data/control/foo.paused` pauses nothing; every reported reason ∈ `PAUSE_REASONS`; no code path creates or deletes a file under `data/control/` |
| `test_liveness_ops.py` | `LongTask` with `last_beat` 20 min old but `last_attempt` 1 min old → live; both stale → not live; paused `LongTask` with stale beat → live; `PAUSE_ALL` with every collector stale → live; `health` task done → not live; self-probe failing 3× (respx) → not live, 2× → live; cron job `interval_min()` = 1440 for `backup_daily`, 360 for `scoring_6h`, 10,080 for a weekly cron; a `manual` job with `last_run=None` is never stale; a non-manual job with `last_run=None` becomes stale once `now − rt.started_at` exceeds its max-age; a LongTask without `liveness_max_age_sec` is never stale |
| `test_recovery.py` | no heartbeat + empty boots → `first_boot`; no heartbeat + boots → `clean`; heartbeat with same `boot_id` → `process_crash`; different `boot_id` → `host_unclean` with `downtime_sec = now − ts`; boots list capped at 30; orphan count; second `InstanceLock` in the same test process on the same file → raises; `report` emits `service_recovered_unclean` only for unclean kinds with downtime ≥ 60 s; `post_boot_check` with a corrupted copy → `integrity.json.result ≠ "ok"`, `pause_all_active()` true, no file created under `data/control/` or the repo root, `db_integrity_failed` sent direct |
| `test_guard_digest.py` | guard rewrites heartbeat, fires/clears `disk_low` via monkeypatched `disk_usage`; dead-man GET called once when enabled (respx), never when URL empty; digest ≤ 4,000 chars with all section headers on a fixture DB, sections omitted without later-phase tables, `⚠️` for a PAT expiring in 5 days |
| `test_alert_routing.py` | every `fire(event_type=…)` → one `events` row via `emit_event` (`occupant_key IS NULL`, `netuid IS NULL`, `source` ∈ {runtime, cost, ops}, `dedupe_key`, integer `observed_at`, `alerted_at IS NULL`) and **no** direct send — also with `alert_dispatch` disabled, paused or stale, and for `disk_critical`/`db_integrity_failed` (02 §D3); `emit_event` raising (monkeypatched DB error) → `send_alert` called once, nothing stored; `fire()` without `event_type` → sent, no row; repeating the same `dedupe_key` inserts nothing; `alert_dispatch` is declared in the Phase-0 `JOB_TABLE` with `ops_exempt=True`; parametrised over every §2.13.2 row: the payload passes 03's `EVENT_TYPES` required-key check and the severity matches 03 |
| `test_units.py` | `bash ops/install-systemd.sh --render-only $tmp` (env `SENTINEL_USER=tester SENTINEL_ROOT=/home/tester/taosentinel`) → no `@…@` left; parse `taosentinel.service` (`configparser(strict=False, interpolation=None)`) and assert `Type=notify, NotifyAccess=main, WatchdogSec=120, TimeoutStartSec=120, Restart=on-failure, RestartSec=10, StartLimitIntervalSec=60, StartLimitBurst=5, TimeoutStopSec=30, KillSignal=SIGTERM, After` contains both targets, `StandardOutput=append:/home/tester/taosentinel/data/service.log`, `ProtectHome=read-only`, `ReadWritePaths=/home/tester/taosentinel/data`, `Environment` contains `XDG_CACHE_HOME=/home/tester/taosentinel/data/cache`; frontend has `Wants=taosentinel.service` and no `Requires=`; timer `OnUnitActiveSec=5min`; `systemd-analyze verify` on the rendered files when available |
| `test_auto_update.py` | builds a bare repo + clone in `tmp_path`, `DEPLOY_GIT_URL=file://…`, `DEPLOY_SYSTEMCTL=true`, a tiny `http.server` thread serving `{"status":"ok","version":"0.1.0+<sha7>"}`, a stub venv python (`PY` path to the test interpreter, app package importable): (1) up to date → exit 0, no history line; (2) good commit → `deployed` line, HEAD = new sha; (3) commit with a syntax error in `app/` → exit 1, HEAD back to previous, `rolled_back … compileall`; (4) health server returning the old sha → `rolled_back … health_timeout`; (5) `[manual-deploy]` commit → exit 0, HEAD unchanged, `refused` line written once across two runs; (6) `DEPLOY_PAUSED` → no fetch; (7) `deploy.env` mode 644 → exit 1; (8) time inside a quiet window (`faketime` if installed, else skip) → deferred; (9) health server returning `{"status":"degraded","version":…<new sha7>}` → `deployed`; returning `initializing` throughout → `rolled_back … health_timeout`; (10) history `at` is an integer |
| `test_with_bucket.py` | service reachable (respx) → child env has `TMC_RATE_PER_MIN=2`, `TMC_HOURLY_CAP=100`, `TAOSENTINEL_PROCESS_ROLE=script`; unreachable → unchanged rates; penalty file written by one `ProviderLimiter` blocks `acquire` of a second instance until `until`; `--exclusive tmc` pauses/resumes, by exact id, exactly the non-`manual` members of `PAUSE_GROUPS["tmc"]` that were not already paused (never `chain_sse` or `backfill_tmc`), even when the child fails |
| `test_security.py` | `selfcheck` flags `.env` mode 644, non-loopback host without token, a `~/.bittensor/wallets` dir under a fake HOME; `git ls-files` contains no `.env`/`data/` (skipped outside a git checkout) |

---

## 10. Verify checklist (on the laptop)

**A. Host prep** (after `sudo ops/laptop-prep.sh --user $USER` and a reboot)
```bash
systemctl is-enabled sleep.target suspend.target hibernate.target hybrid-sleep.target   # masked ×4
grep -h HandleLidSwitch /etc/systemd/logind.conf.d/50-taosentinel.conf                 # HandleLidSwitch=ignore …
timedatectl show -p Timezone -p NTPSynchronized                                          # Timezone=Etc/UTC, NTPSynchronized=yes
systemctl is-enabled systemd-time-wait-sync.service                                      # enabled
grep CriticalPowerAction /etc/UPower/UPower.conf                                         # CriticalPowerAction=PowerOff
grep GRUB_RECORDFAIL_TIMEOUT /etc/default/grub                                           # GRUB_RECORDFAIL_TIMEOUT=5
swapon --show --bytes | awk 'NR>1{s+=$3} END{print s>=4294967296}'                       # 1
sudo ufw status verbose | head -8                                                        # Status: active; deny (incoming); tailscale0 ALLOW
tailscale status | head -2                                                               # this host listed, online
python3.12 --version                                                                     # Python 3.12.x
```

**B. Install and service**
```bash
cd ~/taosentinel && stat -c '%a' .env                                    # 600
sudo ops/install-systemd.sh && sudo systemctl start taosentinel && sleep 15
systemctl is-active taosentinel                                          # active
systemctl show taosentinel -p Type -p WatchdogUSec -p NRestarts          # Type=notify WatchdogUSec=2min NRestarts=0
ss -ltnp | grep -E ':8090|:3001'                                         # 127.0.0.1:8090 only (3001 after Phase 11)
curl -s localhost:8090/health | jq -r '.status, .version'                 # ok (or degraded — then read .degraded_reasons), 0.1.0+<sha7>
curl -s localhost:8090/health | jq '.ops.boot_kind, .ops.security'       # "first_boot"/"clean", all true, wallet_dirs_found 0
python -m app jobs | grep -E 'backup_daily|ops_guard_5m|ops_digest_daily|cost_ledger_hourly'   # 4 rows, available=yes, ops_exempt=yes; backup_daily cron 03:30, ops_digest_daily cron 09:00
sudo -k; sudo -n /usr/bin/systemctl restart taosentinel && echo sudo-ok # sudo-ok (NOPASSWD entry; -k drops cached credentials)
sudo -k; sudo -n /usr/bin/systemctl stop taosentinel; echo $?           # 1 "a password is required" (outside the sudoers scope)
```

**C. Watchdog, crash, instance lock**
```bash
sudo systemctl kill -s SIGKILL taosentinel; sleep 20; systemctl show taosentinel -p NRestarts   # NRestarts=1
curl -s localhost:8090/health | jq -r .ops.boot_kind                     # process_crash (+ Telegram service_recovered_unclean if down ≥ 60 s)
sudo kill -STOP $(systemctl show -p MainPID --value taosentinel); sleep 150
journalctl -u taosentinel -n 30 | grep -i watchdog                       # "Watchdog timeout" then restart
.venv/bin/python -m app serve; echo $?                                   # "another instance holds data/run/taosentinel.lock", 3
touch PAUSE_ALL; sleep 400; systemctl show taosentinel -p NRestarts      # unchanged (paused jobs ignored by liveness)
curl -s localhost:8090/health | jq '.ops.pause_all'; rm PAUSE_ALL        # true
```

**D. Backups and restore drill**
```bash
python -m app backup now                                                 # prints manifest; quick_check "ok"; mirror ok|missing|off
ls data/backups/                                                         # taosentinel-<stamp>.db  .db.gz  .json
python -m app backup verify <stamp> --full; echo $?                      # 0
python -m app ops integrity-check; jq -r .result data/run/integrity.json # ok
python -m app backup restore <stamp> --target data/tmp/drill.db; echo $? # 0, live DB untouched (service running)
sqlite3 data/tmp/drill.db "select max(version) from schema_version"      # = /health .db.schema_version
python -m app backup restore <stamp> --yes; echo $?                      # 2 "stop the service first"
sudo umount /mnt/taosentinel-mirror; python -m app backup now            # mirror=missing; Telegram backup_mirror_missing; nothing written under the mount point
ls -A /mnt/taosentinel-mirror; sudo mount -a                             # empty (unmounted), then remounted
# next morning: journalctl/collector_runs show backup_daily at 03:30Z ok=1
sqlite3 data/taosentinel.db "select started_at, ok, note from collector_runs where job='backup_daily' order by started_at desc limit 1"
```

**E. Kill switches, pauses, budget**
```bash
touch KILL_SWITCH; sleep 2; curl -s localhost:8090/health | jq .kill_switch_active; rm KILL_SWITCH   # true
touch data/control/github.paused; curl -s localhost:8090/health | jq '.collectors.github_resolve.paused'   # "emergency_file" (group github, §2.11)
touch data/control/github_.paused; python -m app ops pause github_ ; rm data/control/github_.paused       # unknown_pause_file logged, nothing paused; CLI exit 2 (UnknownTarget — no prefix matching)
python -m app ops resume github; ls data/control/github.paused; rm data/control/github.paused   # prints file_pause_active; the file is still there (the app never deletes it)
python -m app ops pause github --reason drill && sleep 310 && curl -s localhost:8090/health | jq '.collectors.github_resolve.paused'   # "operator" (control_state row, picked up by ops_guard_5m)
sqlite3 data/taosentinel.db "select target, key, value, set_by from control_state"; python -m app ops resume github   # github|paused|true|cli ; then row gone
DB_PATH=data/tmp/drill-costs.db X_MONTHLY_CAP_USD=1 python -m app costs record twitterapi_io --usd 0.85   # soft alert fired (spend_80pct)
DB_PATH=data/tmp/drill-costs.db X_MONTHLY_CAP_USD=1 python -m app costs record twitterapi_io --usd 0.20   # hard latch; "x: paused_reason=budget"
DB_PATH=data/tmp/drill-costs.db X_MONTHLY_CAP_USD=1 python -m app costs status                         # x 1.05/1.00 paused budget
DB_PATH=data/tmp/drill-costs.db X_MONTHLY_CAP_USD=1 python -m app costs override x --usd 1 --reason drill && \
DB_PATH=data/tmp/drill-costs.db X_MONTHLY_CAP_USD=1 python -m app costs status                         # cap 2.00, paused false
rm data/tmp/drill-costs.db
```

**F. Auto-update (after `--with-update-timer`)**
```bash
stat -c '%a' ~/taosentinel-secrets ~/taosentinel-secrets/deploy.env     # 700 600
systemctl list-timers taosentinel-update.timer                           # next run ≤ 5 min
sudo systemctl start taosentinel-update.service; journalctl -u taosentinel-update -n 5   # no ERROR; quiet when up to date
# push a commit that breaks app/ (e.g. `def x(:`) to DEPLOY_BRANCH from the PC, wait ≤ 5 min:
tail -1 data/run/deploy-history.jsonl                                    # {"result":"rolled_back",…,"detail":"compileall"}
git -C ~/taosentinel rev-parse --short HEAD                              # previous sha; Telegram "Deploy rolled back"
# push the fix: history "deployed", /health .version contains the new sha7
```

**G. Governor wiring (Phase 9, keys present)**
```bash
curl -s localhost:8090/health | jq '.spend'                              # groups x, llm, taostats, tmc, each with usd, cap_usd, pct, paused, reserved_usd; caps 25 / 60 / – / 90000 req
sqlite3 data/taosentinel.db "select provider, month, round(usd,4), requests from provider_spend order by provider"
# spec 09 §10 step 8 and spec 10 §10 step 5 drills pass with the governor of this spec
```

**H. Simulated power cut** (the real test; do it once before trusting the box)
```bash
sudo sysctl kernel.sysrq=1; echo b | sudo tee /proc/sysrq-trigger       # immediate reboot, no sync, no clean shutdown
# do NOT log in; from the Windows PC over Tailscale after ~2 min:
ssh <laptop> 'systemctl is-active taosentinel; curl -s localhost:8090/health | jq -c "{s:.status,k:.ops.boot_kind,d:.ops.downtime_before_boot_sec}"'
                                                                         # active; {"s":"ok" (or "degraded"),"k":"host_unclean","d":<seconds>}
# Telegram: service_recovered_unclean (kind host_unclean, downtime, missed jobs)
ssh <laptop> 'journalctl -u taosentinel -b | grep -i quick_check'        # quick_check ok (≈ 5 min after boot)
# then the §7.3.11 checklist. Optional physical drill: unplug AC, let UPower power off at 5 %, replug → the laptop boots by itself (BIOS AC-on).
```

---

## 11. Open questions / operator decisions

1. **Nightly timing — settled** (02 §E3, §F2): `storage_retention_daily` 03:00Z (`STORAGE_RETENTION_CRON_MINUTE=0`, includes the `chain_ticks` prune; no `chain_prune` job) → `backup_daily` 03:30Z waiting ≤ 20 min for it (`DB_BACKUP_WAIT_FOR_JOBS=storage_retention_daily`) → 04:30Z reboot window → `quant_weekly` Sun 05:00Z; digest 09:00Z. Retention's destructive-policy backup gate (≤ 30 h) passes with the previous 03:30Z backup.
2. **Naming — settled:** `TAOSTATS_MONTHLY_CALL_CAP` is a deprecated alias of `TAOSTATS_MONTHLY_CAP_REQ` and `SCORING_SPEC_VERSION` a read-only alias of `SPEC_VERSION` (02 §F2); per-target pauses live only in `control_state` (02 §B12); `provider_spend` has `requests` + `units_json` (02 §G5); governor/ops runtime state stays in `job_state` pseudo-jobs `_costs`/`_ops` (runtime state, never pauses).
3. **Cost-governor API — settled** (02 §G5, §2.10): 09 and 10 call exactly `check` / `reserve` / `commit` / `release` / `record` / `snapshot`, keyword-only after `usd`/`est_usd`; a test enforces it.
4. **Cap = 0 — settled** (02 §F2): 0 disables a paid provider (fail-closed); spec 01's "0 = no USD cap" is superseded. `TAOSTATS_MONTHLY_CAP_USD` is display-only (flat subscription, not a paid group).
5. **`/health` shape — settled** (02 §G6): 01 builds it, 14 serves it; `status ∈ {ok, degraded, initializing}`, `degraded_reasons[]`, `spend` = §2.10's group object. The deploy gate reads only `status` and `version` and accepts `degraded` (§2.9 note a).
6. **Alert routing and delivery — settled** (02 §D3 amended: `alert_dispatch` ships in Phase 0; §D4 amended: other specs' budget events go to the digest).
7. **Additions beyond master §11:** backup gzip + size budget, instance lock, dead-man ping, `OnFailure=` notifier, `ops_guard_5m`, `ops_digest_daily`, the integrity hold (`PAUSE_ALL` is now in master §11). 01 recomputes its job counts from the final table (02 §E2).
8. **Liveness — settled** (02 §E4; §2.5 table). Spec 05's suggested `tmc_daily.last_ok` (30 h) liveness is not adopted (a restart cannot fix a provider outage; failures surface as `collector_failing`); its `chain_sse` 900 s max-age is.
9. **Disk — settled as a requirement** (02 §I1, amended): ≥ 60 GB free is required; smaller disks are unsupported unless the operator lowers `CHAIN_TICKS_RETENTION_DAYS`, `DB_BACKUP_MAX_TOTAL_GB` and the weekly backup count and adds a USB/NAS mirror (§2.7: 20 GB then lasts ≈ 1–3 months).
10. **Full-disk encryption — operator item** (02 §I2): no passphrase prompt on the system disk (or TPM-backed unlock), so a power cut recovers unattended.
11. **Deploy credentials — operator item** (02 §I2): a fine-grained PAT (repo `taosentinel`, Contents read-only) or a read-only deploy key in `~/taosentinel-secrets/deploy.env`; checklist B1's token cannot read a private repo; auto-update stays off until it exists (checklist A5 default: manual `git pull`).
12. **UTC system timezone — operator item** (02 §I2); the operator reads Telegram in local time.
13. **BIOS "power on AC" — operator item** (02 §I2; vendor-specific). Battery charge limits (e.g. 80 % for a permanently plugged laptop, vendor tools/TLP) remain an operator choice per model.
14. **UNVERIFIED facts:** `tailscale serve` flag syntax for the installed version; Discord ban duration; gzip ratio for this DB; 03 §6's DB growth estimates (they drive §2.7's disk arithmetic); `github-authentication-token-expiration` header on fine-grained PATs; healthchecks.io free-tier terms; `needrestart` behaviour under unattended-upgrades (a restart of `taosentinel` by it is harmless — graceful SIGTERM); whether `OnFailure=` also fires on each auto-restart (systemd 249/255); UPower's critical-action fallback chain with masked sleep targets; whether a `ReadWritePaths=` mirror on an `x-systemd.automount` NAS mount triggers the automount inside the unit's mount namespace.
15. **Shadow series** follow spec 12 §2.13 (`SCORING_SHADOW_SPECS`, `scores.shadow`, `python -m app.scoring backfill`); the `SCORING_SPEC_VERSION` alias is settled (02 §F2).
16. **Hardening risk:** `ProtectHome=read-only` + `ReadWritePaths=data` is stricter than the old unit (`ProtectHome=false`). Consequences handled here: the service writes no pause file at all (the integrity hold is derived from `integrity.json`, 02 §B12), library caches go to `XDG_CACHE_HOME=data/cache`. A later wish to toggle a sentinel from Telegram/API must write `control_state` instead (sentinels are operator-only files, 02 §B12) — never relax `ProtectHome`. `MemoryMax=2G` may be too small for spec 13's in-process `quant_weekly` job on a year-old DB (≈ 2.8 M `metric_values` rows in Python objects) — raise to 3G or run the quant CLI outside the unit when that job is enabled.
17. **Spend event names — settled** (02 §D1): `spend_80pct` / `spend_cap_reached` with 03 §3.9's payload keys (plus `provider` = group for 14's dedupe); 01's `budget_soft:`/`budget_hard:` latch keys are retired.
18. **Backup ownership with spec 03 — settled:** 03 keeps `backup_to` + `CODE_SCHEMA_VERSION`; this spec owns the pipeline, the `backup …` CLI (no `db backup`/`db restore` aliases), restore, and the keys of §5.1 including those 03 ceded (`DB_BACKUP_COMPRESS`, `DB_BACKUP_MAX_TOTAL_GB` = 12, `DB_BACKUP_MIN_FREE_FACTOR`). 03 §5/§6 still quote a 40 GB budget and 2 uncompressed copies — superseded by 02 §F2 (12 GB, 1).
19. **`TMC_RATE_BURST` — settled** (02 §F2: 2; validator `burst + rate ≤ 10`).
20. **`/health` spend field names — settled** (02 §G6): 09's `budgets.<provider>` and 10's `spend.anthropic.{usd_month, cap}` read §2.10's group object instead (key `spend.llm`, 02 §G1).
21. **Spec 01 amendments requested here:** cron `interval_min()` over the next 8 fire times, liveness warm-up from `rt.started_at`, `manual` exemption, `LongTask.paused_reason()` + `last_attempt`, `ops_exempt` on the §2.11 set, `rt.task_handles["health"]`, API self-probe, `pre_init` first in `init_services()`, heartbeat removal in `shutdown_services()`, wrapper cost reconciliation (default `requests=0`), `Http` request counting via `record(p, 0.0, requests=1)`, pauses only via `control_state` with `PAUSE_GROUPS` targets, `AlertLatch.fire` per 02 §D3 (direct only on a failed DB write), the Phase-0 `alert_dispatch` row with `ops_exempt=True`, the §6.1 rows, and `COST_LEDGER_*`/`COST_SOFT_ALERT_PCT` regrouped under 16.

*Cross-spec issues the ledger does not settle (raised 2026-09-23):*

22. **Alert delivery — settled** (02 §D3, amended): `alert_dispatch` ships in Phase 0 (implemented by 01), so §2.13.2's former "no fresh dispatcher → direct send" rule is removed; `fire()` sends direct only when the DB write fails. Still open for 03: its §3.9 alert column marks `disk_critical`/`db_integrity_failed` `direct` and `backup_failed` "+ direct send if the dispatcher is down" — superseded by D3; 03 should mark them `yes`. Other direct senders that are not events and stay outside D3: the ops digest (a report), `notify-failure.sh` and `auto-update.sh` (the service may be down).
23. **`ops_exempt` for 14's `alert_dispatch` and `telegram_bot` — settled** (02 §E1): both carry the flag, like 03's `storage_retention_daily`; no `PAUSE_GROUPS` group contains them (02 §B12).
24. **Request double counting — settled** (02 §G5, amended): `record()` defaults to `requests=0`; `Http` passes `requests=1` per wire request; only calls outside `Http` (10's Anthropic SDK) pass a count.
25. **Disk — settled** (02 §I1, amended): ≥ 60 GB free is required; smaller disks are unsupported unless retention, backup budget and weekly count are lowered and a mirror is added.
26. **Duplicate budget pages — settled** (02 §D4, amended): `x_budget_paused`, `tmc_budget_degraded`, `llm_budget_paused`, … go to the digest; `spend_cap_reached` is the page. Still open for 03: §3.9 pages `x_budget_paused` for `reason ∈ {pacing, provider_balance}`.
27. **Pause-reason vocabulary — settled** (02 §B12, amended): `operator | kill_switch | pause_all | budget | emergency_file`; 10's `manual`/`flag` → `operator`; 09's `auth` is a failure, not a pause; 13's `sentinel`/`manual` and 17's `paused:sentinel` note must follow.
28. **Provider ids — settled** (02 §G1, amended): one canonical id per provider in limiter, `provider_spend`, governor and `/health`; spend groups `x`, `llm`, `taostats` (+ this spec's request-capped `tmc`). G1's list lacks this spec's `deadman` (dead-man GET, §2.13.3) and 14's `telegram` bucket — uncapped local ids; the ledger should list them.
29. **Retention policy id.** 02 §E3 speaks of "the `chain_ticks` prune"; 03 §2.7's policy id is `chain_prune` (used by §7.3.5's `--only chain_prune`). If 03 renames the policy, the runbook command follows.

---

## 12. Sources

- `E:\TaoData\specs\02-cross-cutting-conventions.md` (binding ledger; applied: A2, B1, B2, B9, B12, D1–D4, E1–E4, F2, G1, G3–G6, I1, I2, J2)
- `E:\TaoData\specs\00-MASTER-SPEC.md` §1.3, §2, §3, §4, §6, §9, §11, §12, §13
- `E:\TaoData\specs\INDEX.md`; `E:\TaoData\specs\18-operator-checklist.md` (A1, A3-A6, B1, B3, B6, C1-C4, E)
- `E:\TaoData\specs\01-repo-skeleton-runtime.md` §2.1, §2.4-§2.9, §2.11-§2.14, §3, §4, §5, §6, §7, §11 (Settings inventory, JOB_TABLE, `CollectorJob`/`LongTask`, liveness, `/health`, `AlertLatch`, limiter, governor subset)
- `E:\TaoData\specs\03-storage-schema.md` §1 (backup/restore delegated here), §2.1 (INTEGER timestamps, STRICT), §2.3 (`Database` API: item 1 writer lock/`transaction()`, item 7 `backup_to`, item 9 `CODE_SCHEMA_VERSION`), §2.4 (`migrate`/`discover`, `schema_ahead`, additive policy), §2.7 (`storage_retention_daily` 03:00Z, backup gate, `ops_exempt`), §2.8 (backup/restore ownership: spec 16; newer-snapshot refusal), §3.1 (`job_state`, `control_state`, `provider_spend` DDL), §3.6 (`events` DDL, `ux_events_dedupe`), §3.9 (event catalog), §5 (`DB_BACKUP_COMPRESS`, `DB_BACKUP_MAX_TOTAL_GB`, `DB_BACKUP_MIN_FREE_FACTOR`, `STORAGE_RETENTION_CRON_*`), §6 (sizing), §11 Q1/Q8/Q9/Q12, Appendix A
- `E:\TaoData\specs\12-feature-store-and-scoring-engine.md` §2.13 (`SCORING_SHADOW_SPECS`, promotion, rescore, `python -m app.scoring backfill`)
- `E:\TaoData\specs\05-chain-collector.md` §2.2, §5 (`CHAIN_TICKS_*`; its former `chain_prune` job is now a policy of 03's `storage_retention_daily`), §health (SSE 900 s, `tmc_budget_degraded`), job ids `tmc_hourly/daily/weekly`
- `E:\TaoData\specs\06-github-collector.md` §2.1, §5 (job ids, pause semantics — its kill-file key is retired by 02 §B12 — rate-limit headers)
- `E:\TaoData\specs\09-x-collector-and-kol.md` §2.5, §5, §6.3-§6.4, Appendix A.6 (job ids, governor interface, pacing, `X_SOFT_ALERT_PCT`; its kill-file key is retired by 02 §B12)
- `PAUSE_GROUPS` membership: 04 §2.1, 05 §6.1, 07 §2.7, 08 §6, 10 §6.1, 11 §6, 12 §6, 13 §5, 14 §2.5–§2.6, 17 §8.1 (job ids as of 2026-09-23)
- `E:\TaoData\specs\10-llm-classification.md` §5, §6.3 (reserve/record protocol, `PRICES`, budget fallback)
- `E:\TaoData\specs\13-validation-harness.md` (backup snapshot as quant input; Sunday 03:30 job)
- `E:\TaoData\specs\14-api-telegram-and-ui.md` §2.4 (routing), §3.1-§3.5 (`/health`, services probes, control endpoints, `data/control/*.paused`), §7, §11
- `E:\TaoData\specs\15-trading-bot-integration.md` (`SCORING_KILLED`, `SENTINEL_ACCEPTED_SPEC_VERSIONS`, ETag)
- `E:\TaoData\research\01-whale-research-ops.md` §3 (systemd units, watchdog, git auto-deploy, backups, Telegram, logging) and Gotchas
- `E:\TaoData\research\02-gapfill-old-bot-runtime-skeleton.md` §6 (hardcoded paths/lines), Gotchas (READY before backup, watchdog max-age, cwd-relative paths, PAT in `.env`, `append:` needs `data/`)
- `E:\TaoData\research\02-gapfill-keyless-onchain-pillar-source.md` §0 (per-IP TMC limits, SSE unmetered, shared-IP bucket exhaustion), §6 (≈ 600-700 req/day)
- `E:\TaoData\research\02-gapfill-discord-telegram-topology.md` (invalid-request ban rule, 403 counting)
- `E:\TaoData\research\01-web-social-apis.md` §6 (monthly run-rate)
- Old bot: `E:\SN_Trader\SubnetTrader\ops\systemd\subnettrader.service` 1-52, `ops\systemd\subnettrader-frontend.service` 1-29, `ops\install-systemd.sh` 1-41, `deploy\auto-update.sh` 1-179, `deploy\subnettrader-update.service` 1-16, `deploy\subnettrader-update.timer` 1-12, `app\storage\backup.py` 1-97, `app\storage\db.py` 38-80 (tmp name at 53), `scripts\backup_db.py` 1-53 (no-`connect()` rule at 35), `app\main.py` 318-341, 1294-1331, 1510-1520 (interval backup job), 1522-1524 (retention phased away from the backup), `specs\ops-chain-resilience.md` 160-230, `install.sh` 1-80


## Changelog
- 2026-09-23 reconciliation:
  - G5: governor API written out exactly (`check→Decision`, `reserve(*, job, run_id)→Reservation|None`, `commit(res, actual_usd, *, detail)`, `release`, `record(…, requests=1, …)`, `snapshot`); `requests=0` rule for USD bookings of `Http`-made calls; expired reservations stay committable.
  - G6: `/health.spend` group object with required `usd, cap_usd, pct, paused, reserved_usd`; deploy gate parses only `status`/`version` (jq) and accepts `ok|degraded`.
  - B12: `control_state` is the only persistent pause store; the app never writes `data/control/*.paused`; the app-written `data/control/PAUSE_ALL` is replaced by the integrity hold derived from `integrity.json`; resume reports `file_pause_active`.
  - E1/B12: explicit `ops_exempt` set (16's four jobs, 03 `storage_retention_daily`, 01 `log_retention_daily`, 14 `alert_dispatch`/`telegram_bot`); `KILL_SWITCH` spares free LongTasks.
  - D1/D2/B9: catalogue aligned to 03 §3.9 (`collector_recovered {collector, failures, down_seconds}`, `backup_failed.last_good_age_h`, `backup_stale {age_hours, stale_hours}`, spend dedupe `|80`/`|cap`, `usd_month` dropped, sources `runtime|cost|ops`, `{day}` = `floor(event_at/86400)`).
  - D3/D4: `AlertLatch.fire` writes through `emit_event`; direct send only on DB failure, for 03's direct types, or with no fresh dispatcher (pre-Phase-11 gap, §11 item 22); routing per D4.
  - E2/E3: single §6.1 ops-job table (`backup_daily` timeout 5,400 s; `cost_ledger_hourly` + `COST_*` keys now owned here); `DB_BACKUP_WAIT_FOR_JOBS=storage_retention_daily`; no `chain_prune` job (runbook `db retention --only chain_prune`); quant Sun 05:00Z; deploy quiet window 20:25–23:35.
  - E4: one liveness table; `manual` jobs exempt; warm-up measured from process start; cron `interval_min()` over the next 8 fire times.
  - F2: cap 0 disables a paid provider; `DB_BACKUP_MAX_TOTAL_GB` 12 (+ `DB_BACKUP_COMPRESS`, `DB_BACKUP_MIN_FREE_FACTOR` owned here), separate mirror budget, stale 36 h, 1 uncompressed; `TMC_RATE_BURST` 2; aliases settled.
  - I1/I2: sizing redone from 03 §6 (peak ≈ 4.7 × DB + 5 GB → ≥ 60 GB; the 20 GB floor lasts ≈ 1–3 months); `API_AUTH_TOKEN` before `tailscale serve`; operator items marked settled.
  - B1/B2: unix-second ints in `Reservation`, `BootInfo`, deploy history/state files; severities `info|warning|critical`.
  - Package 16: A1–A3 applied (= D2); A4 applied (17's backfills run in-service; `--exclusive` skips `backfill_*`/`manual`); reviewer-03 notes applied; retired 01 placeholder job ids replaced by owner ids (E2); §11 items 22–29 list ledger-silent conflicts.
- 2026-09-23 follow-up (B12, E1): closed sentinel list — `X_KILLED`/`GITHUB_KILLED`/`COMMUNITY_PAUSED` and their `*_KILL_SWITCH_PATH` keys retired (emergency equivalent `data/control/<group>.paused`; `DEPLOY_PAUSED` only for `auto-update.sh`); new `PAUSE_GROUPS` table; `resolve_targets` accepts exact ids or group names only (no prefix/provider match), groups never reach `ops_exempt`/`alert_dispatch`/`telegram_bot`; reasons `operator|kill_switch|pause_all|budget|emergency_file`; `resume` deletes only its row and reports `file_pause_active`/`still_paused_by`.
- 2026-09-23 follow-up (03 DDL): `control_state` quoted exactly (`value` 'true'|'false', no `value_json`, target = exact id or group); verify command and `refresh_cache` fixed.
- 2026-09-23 follow-up (D3, D4): `alert_dispatch` ships in Phase 0 (01); `fire()` sends direct only on a failed DB write (rule c and 03's "direct" types dropped); other specs' budget events go to the digest.
- 2026-09-23 follow-up (G1, G5, I1, §11): governor groups `x`/`llm`/`taostats` (+ request-capped `tmc`), canonical provider ids, no aliases; `record(…, requests=0)` default, `Http` passes `requests=1`, SDK calls pass their own count; ≥ 60 GB disk required; §11 items 6, 9, 22–28 settled; `startup_catchup` in the recovery order.
- 2026-09-24 final pass: `PAUSE_GROUPS.llm` = exactly 10's four jobs (`llm_classify_submit`, `llm_batch_poll`, `llm_rules_fallback`, `llm_narrative_weekly`; phantom `nlp_*` ids removed); `rss` sourced from 09 §2.5; `KILL_SWITCH` row states that 10's free `llm_batch_poll`/`llm_rules_fallback` keep running; `llm_narrative_weekly` (Sun 01:00Z) added to §6.2.
- 2026-09-24 final pass: `FEED_AUTH_REQUIRED` retired; the feed endpoint `GET /api/v1/scores/latest` is served by 14 §3.5 (02 §H1), spec 15 only consumes it. Governor group `llm` and `record(…, requests=0)` confirmed; `resolve_targets` has no prefix matching.
- 2026-09-24 final pass: `python -m app ops rearm owner_rpc_seed` added to `app/ops/cli.py` (deletes `job_state('owner_rpc_seed','rpc_seed_done')`, 11 §6).
