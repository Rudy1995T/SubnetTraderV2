"""test_deps / test_time / test_mathutil / test_logging (spec 01 §9)."""
import importlib.util
import json
import math
import os
from datetime import datetime, timezone

import pytest

from app.util import mathutil as m
from app.util import time as t


# ── deps ────────────────────────────────────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("mod", ["bittensor", "bittensor_wallet", "substrateinterface"])
def test_no_forbidden_deps(mod):
    assert importlib.util.find_spec(mod) is None


def test_quant_mathutil_absent():
    assert importlib.util.find_spec("app.quant.mathutil") is None


# ── time ────────────────────────────────────────────────────────────────────────────────────────────────────
def test_time_helpers():
    assert t.to_iso(1790121600) == "2026-09-23T00:00:00Z"
    a = t.parse_iso("2026-09-23T00:00:00Z")
    assert a == t.parse_iso("2026-09-23T00:00:00+00:00") == t.parse_iso("2026-09-23T00:00:00") == 1790121600
    assert isinstance(a, int)
    assert t.parse_ts(1790121600) == t.parse_ts(1790121600.7) == 1790121600
    assert t.parse_ts("2026-09-23T00:00:00Z") == 1790121600
    assert t.parse_ts(datetime(2026, 9, 23, tzinfo=timezone.utc)) == 1790121600
    assert t.parse_dt(1790121600).tzinfo == timezone.utc
    assert t.floor_hour(1790125199) == 1790121600 and t.floor_hour(1790125200) == 1790125200
    assert t.floor_day(1790125199) == 1790121600
    assert t.month_key(1790121600) == "2026-09"
    assert t.hhmm("02:10") == (2, 10)
    with pytest.raises(ValueError):
        t.hhmm("24:00")
    assert t.dow_hhmm("sun 05:00") == ("sun", 5, 0)
    assert t.to_iso(None) is None


def test_in_utc_windows():
    day = 1790121600
    inside, end = t.in_utc_windows(day + 21 * 3600, "20:25-23:35")
    assert inside and end == day + 23 * 3600 + 35 * 60
    assert t.in_utc_windows(day + 3600, "20:25-23:35") == (False, None)


# ── mathutil ────────────────────────────────────────────────────────────────────────────────────────────────
def test_mathutil():
    assert m.spearman_r([1, 2, 3, 4, 5], [5, 6, 7, 8, 7]) == pytest.approx(0.8207826816681233)
    assert m.spearman_r([1, 2, 3, 4, 5], [5, 4, 3, 2, 1]) == pytest.approx(-1.0)
    assert m.spearman_r([1, 2, 3], [3, 2, 1]) == 0.0           # verbatim port: n < 5 → 0.0
    assert m._average_ranks([10, 20, 20, 5]) == [1.0, 2.5, 2.5, 0.0]   # 0-based, ties averaged
    assert m.pearson_r([9, 9, 1, 2, 3, 4, 5], [1, 2, 3, 4, 5]) == pytest.approx(1.0)   # tail-aligned
    assert m.quantile_buckets([1, 2, 3, 4], 2) == [0, 0, 1, 1]
    z = m.zscore([1, 2, 3])
    assert z[1] == 0 and z[0] == pytest.approx(-z[2])
    assert m.zscore([5, 5]) == [0.0, 0.0]
    assert m.log_returns([1, math.e]) == pytest.approx([1.0])
    assert m.returns_pearson_r([1, 2, 4, 8], [1, 2, 4, 8]) == 0.0  # constant returns → zero variance
    assert m.returns_pearson_r([1, 2, 3, 5, 4, 6], [2, 4, 6, 10, 8, 12]) == pytest.approx(1.0)
    assert m.returns_pearson_r([1, 2, 3, 5, 4], [2, 4, 6, 10, 8]) == 0.0   # < 6 prices
    assert m.effective_bets(-0.5, 10) == float("inf")
    assert m.effective_bets(0.0, 10) == 10 and m.effective_bets(1.0, 10) == 1
    assert m.gini_coefficient([1, 2, 3, 4]) == pytest.approx(0.25)
    assert m.gini_coefficient([5, 5, 5]) == pytest.approx(0.0)
    assert m.gini_coefficient([]) == 0.0
    assert m.gini_coefficient([0, -1, 1, 2, 3, 4]) == pytest.approx(0.25)
    assert m.monotone_score([1, 2, 3, 4, 5]) == pytest.approx(1.0)
    assert m.monotone_score([5, 4, 3, 2, 1]) == pytest.approx(-1.0)
    assert m.monotone_score([1, 2, None, 3]) == 0.0
    assert m.rolling_volatility([1.0] * 30) == 0.0
    assert m.rolling_volatility([1.0]) is None
    assert m.rolling_volatility([1.0, 2.0] * 13) == pytest.approx(
        __import__("statistics").stdev([math.log(2), -math.log(2)] * 12) * math.sqrt(2190))   # annualised (4h bars)


# ── logging ─────────────────────────────────────────────────────────────────────────────────────────────────
def test_jsonl_entry_and_rotation(tmp_path, monkeypatch):
    from app.logging import logger as lg
    h = lg.JsonlFileHandler(tmp_path)
    import logging
    log = logging.getLogger("t.jsonl")
    log.handlers[:] = [h]
    log.propagate = False
    log.setLevel(logging.INFO)
    log.info("hello", extra={"data": {"a": 1}})
    files = list(tmp_path.glob("*.jsonl"))
    assert len(files) == 1
    e = json.loads(files[0].read_text().splitlines()[0])
    assert set(e) == {"ts", "level", "logger", "message", "data"} and e["data"] == {"a": 1}
    monkeypatch.setattr(lg, "MAX_BYTES", 1024)
    for i in range(50):
        log.info("x" * 100)
    assert list(tmp_path.glob("*.jsonl.1"))
    h.close()


def test_prune_old_logs(tmp_root):
    import app.config as app_config
    from app.logging.retention import prune_old_logs
    d = app_config.settings.path(app_config.settings.JSONL_DIR)
    d.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for n in ("2020-01-01.jsonl", "2020-01-02.jsonl.3", f"{today}.jsonl"):
        (d / n).write_text("{}\n")
    assert prune_old_logs() == 2
    assert (d / f"{today}.jsonl").exists()
