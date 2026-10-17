# TaoSentinel — common tasks (spec 01 §8). Run from the repo root.
PY := .venv/bin/python
PIP := .venv/bin/pip

.PHONY: venv install install-dev check jobs run test smoke lint logs status health backup-now

venv:
	python3.12 -m venv .venv

install: venv
	$(PIP) install -q --upgrade pip
	@if [ -f requirements.lock ]; then $(PIP) install -q -r requirements.lock && $(PIP) install -q --no-deps -e .; \
	 else $(PIP) install -q -e .; fi

install-dev: venv
	$(PIP) install -q --upgrade pip
	$(PIP) install -q -e ".[dev]"

check:
	$(PY) -m app check

jobs:
	$(PY) -m app jobs

run:
	TAOSENTINEL_ROOT=$(CURDIR) $(PY) -u -m app serve

test:
	$(PY) -m pytest -q

smoke:
	$(PY) -m pytest -q -m smoke

lint:
	$(PY) -m compileall -q app scripts

logs:
	tail -n 50 -f data/logs/$$(date -u +%F).jsonl

status:
	systemctl status taosentinel --no-pager || true

health:
	$(PY) -m app health

backup-now:
	$(PY) -m app backup now
