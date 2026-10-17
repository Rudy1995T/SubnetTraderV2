#!/usr/bin/env bash
# Foreground start for manual runs (systemd uses ExecStart directly).
cd "$(dirname "$0")"
. .venv/bin/activate
export TAOSENTINEL_ROOT="$PWD"
exec python -u -m app serve
