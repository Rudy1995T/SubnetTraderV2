"""Death-inclusive, occupant-aware cross-sectional panel (spec 13 §2.1, §8.2).

Port of the old SubnetTrader ``app/backtest/factor_panel.py`` with the ``settings``/``FundamentalsStore``/
``DeregRiskConfig`` coupling removed (13 §8.1):

* ``segment_series`` — positional slot-reset segmentation (the collapse bar is the *terminal* bar of the dying
  segment), unchanged; ``segment_deaths`` adds the occupant boundary rule and the death kind of each segment.
* ``build_panel`` — pure: groups already-downsampled rows into per-netuid ``Bar`` series + a bucket index + the sorted
  date axis. The old ``(bucket, price, depth[, moving_price, volume])`` rows are a prefix of ``PanelRow``.
* ``forward_return`` / ``forward_log_return`` — segment-respecting forward returns with death realisation.
* ``block_to_unix`` — piecewise-linear block → unix over exact anchors (17's ``block_times``, chain_daily).

The DB I/O (old ``load_factor_panel``) lives in :mod:`app.quant.panel_builder`. Read-only, stdlib only.
"""
from __future__ import annotations

import bisect
import math
import sqlite3
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal, Mapping, NamedTuple

RESET_DROP_RATIO = 0.30          # 13 §5 QUANT_RESET_DROP_RATIO default; same value as 05's DeregRiskConfig (literal)
DEATH_RETURN = -1.0              # a `reoccupied` death is realised at −100 % (13 §2.1, §11 item 6)
DeathKind = Literal["price_reset", "reoccupied"]

# Old constant block anchor (factor_backtest.py:33-40) — the fallback of block_to_unix without anchors.
_BLOCK_ANCHOR_BLOCK = 6_502_594
_BLOCK_ANCHOR_TS = 1_758_585_588
_BLOCK_TIME_S = 12.0

_EMPTY: Mapping[str, float] = MappingProxyType({})


class Bar(NamedTuple):
    """Old positional order kept (``factor_panel.py:49-56``); ``quality`` dropped, ``ts``/``occ``/``pit`` appended."""
    bucket: int                 # bucket-start epoch — the PIT decision instant (13 §2.1)
    price: float
    depth: float                # τ in pool; 0.0 = not observed in bucket (ineligible)
    seg: int                    # slot-lifetime index within this netuid
    reg_block: int = 0          # occupant registration block (0 = unknown)
    moving_price: float = 0.0   # chain emission-weight EMA (0 = not observed)
    volume: float = 0.0         # Δ subnet_volume sum over the bucket (13 §2.3); 0.0 = unusable
    ts: int = 0                 # timestamp of the close sample (entry instant, ≥ bucket); 0 → treated as `bucket`
    occ: str | None = None      # occupant_key of the close sample
    pit: Mapping[str, float] = _EMPTY   # factor -> value with eff ≤ bucket (absent = None); "_reg_ts" internal


# (bucket, price, depth, moving_price, volume, ts, occ, reg_block, pit) — any prefix of length ≥ 3 is accepted.
PanelRow = tuple


@dataclass
class Panel:
    """Date-indexed cross-section stored as per-netuid segmented series (old shape + deaths, PIT keys, regimes)."""
    scope: str
    grid_seconds: int
    series: dict[int, list[Bar]]                         # netuid -> ordered bars
    index_of: dict[int, dict[int, int]]                  # netuid -> {bucket_epoch: position}
    seg_end: dict[int, list[int]]                        # netuid -> position -> segment terminal position
    dates: list[int]                                     # sorted union of bucket epochs
    seg_death: dict[int, dict[int, str]] = field(default_factory=dict)   # netuid -> {seg_id: DeathKind}
    pit_keys: tuple[str, ...] = ()
    regime_of: dict[int, dict[str, str]] = field(default_factory=dict)  # date -> {"emission","market","market_source"}
    meta: dict = field(default_factory=dict)                             # builder provenance (panel_builder)

    @property
    def n_netuids(self) -> int:
        return len(self.series)

    @property
    def n_bars(self) -> int:
        return sum(len(s) for s in self.series.values())


def segment_deaths(prices: list[float], reset_ratio: float = RESET_DROP_RATIO,
                   occupants: list[str | None] | None = None) -> tuple[list[int], dict[int, str]]:
    """Segment ids per bar plus the death kind of every segment that ended (13 §8.2).

    Price rule (ported): ``price[i]/price[i-1] < reset_ratio`` ⇒ bar ``i`` is the terminal bar of the dying segment
    (``price_reset``) — but only when the two bars' occupants are not both known and equal: a one-step crash of a
    known living occupant is a price move, not a death (13 §2.1). Occupant rule: both occupants known and different ⇒
    bar ``i-1`` is the terminal bar and the old segment died ``reoccupied``. Unknown → known is not a boundary; both
    rules at the same bar → one ``price_reset``.
    """
    n = len(prices)
    segs = [0] * n
    death: dict[int, str] = {}
    s = 0
    for i in range(1, n):
        prev = prices[i - 1]
        both_known = bool(occupants and occupants[i - 1] and occupants[i])
        occ_fire = both_known and occupants[i - 1] != occupants[i]
        price_fire = prev > 0 and prices[i] / prev < reset_ratio and (occ_fire or not both_known)
        if price_fire:
            segs[i] = s          # collapse bar is the terminal bar of the dying segment
            death[s] = "price_reset"
            s += 1               # the fresh registration starts at the next bar
        elif occ_fire:
            death[s] = "reoccupied"
            s += 1
            segs[i] = s
        else:
            segs[i] = s
    return segs, death


def segment_series(prices: list[float], reset_ratio: float = RESET_DROP_RATIO) -> list[int]:
    """Ported signature: positional ``seg_id`` per bar from one-step price collapses only."""
    return segment_deaths(prices, reset_ratio)[0]


def _segment_ends(segs: list[int]) -> list[int]:
    """For each position, the terminal position of the segment it belongs to."""
    n = len(segs)
    ends = [0] * n
    run_end = n - 1
    for i in range(n - 1, -1, -1):
        if i == n - 1 or segs[i] != segs[i + 1]:
            run_end = i
        ends[i] = run_end
    return ends


def _opt(r: tuple, j: int) -> float:
    return float(r[j]) if len(r) > j and r[j] is not None else 0.0


def build_panel(rows_by_netuid: dict[int, list[PanelRow]], *, grid_seconds: int,
                reset_ratio: float = RESET_DROP_RATIO, scope: str = "",
                regime_of: dict[int, dict[str, str]] | None = None, reg_blocks: dict[int, int] | None = None,
                pit_keys: tuple[str, ...] = ()) -> Panel:
    """Build a :class:`Panel` from already-downsampled rows (pure, no DB).

    Rows are ``(bucket, price, depth[, moving_price, volume, ts, occ, reg_block, pit])`` sorted or not; one row per
    (netuid, bucket). Non-positive prices are dropped before segmentation so a bad sample can't fake a reset.
    ``reg_blocks`` is the per-netuid fallback when a row carries no registration block (ported kwarg).
    """
    regs = reg_blocks or {}
    series: dict[int, list[Bar]] = {}
    index_of: dict[int, dict[int, int]] = {}
    seg_end: dict[int, list[int]] = {}
    seg_death: dict[int, dict[int, str]] = {}
    all_dates: set[int] = set()
    for nu, rows in rows_by_netuid.items():
        clean = [r for r in rows if r[1] and r[1] > 0]
        if not clean:
            continue
        clean.sort(key=lambda r: r[0])
        prices = [float(r[1]) for r in clean]
        occs = [(r[6] if len(r) > 6 else None) for r in clean]
        segs, death = segment_deaths(prices, reset_ratio, occs if any(occs) else None)
        fallback_reg = int(regs.get(nu, 0))
        bars = []
        for i, r in enumerate(clean):
            bucket = int(r[0])
            ts = int(r[5]) if len(r) > 5 and r[5] else bucket
            reg = int(r[7]) if len(r) > 7 and r[7] else fallback_reg
            pit = r[8] if len(r) > 8 and r[8] is not None else _EMPTY
            bars.append(Bar(bucket, float(r[1]), float(r[2] or 0.0), segs[i], reg, _opt(r, 3), _opt(r, 4), ts,
                            occs[i], pit))
        series[nu] = bars
        index_of[nu] = {b.bucket: i for i, b in enumerate(bars)}
        seg_end[nu] = _segment_ends(segs)
        seg_death[nu] = death
        all_dates.update(b.bucket for b in bars)
    return Panel(scope=scope, grid_seconds=grid_seconds, series=series, index_of=index_of, seg_end=seg_end,
                 dates=sorted(all_dates), seg_death=seg_death, pit_keys=tuple(pit_keys),
                 regime_of=dict(regime_of or {}))


def forward_return(panel: Panel, netuid: int, i: int, horizon: int) -> float | None:
    """Forward simple return over ``horizon`` bars **within the bar's segment** (ported + death kinds).

    Inside the segment → ``price[i+h]/price[i] − 1``; overshoot into a segment that died by ``price_reset`` → realised
    at the collapse price; died ``reoccupied`` → ``DEATH_RETURN``; alive but the data ran out → ``None``.
    """
    bars = panel.series[netuid]
    end = panel.seg_end[netuid][i]
    p0 = bars[i].price
    if p0 <= 0:
        return None
    target = i + horizon
    if target <= end:
        return bars[target].price / p0 - 1.0
    kind = panel.seg_death.get(netuid, {}).get(bars[i].seg)
    if kind is None and not panel.seg_death and end < len(bars) - 1:
        kind = "price_reset"     # panels built without seg_death (hand-made in tests): ported `died` rule
    if kind == "price_reset":
        return bars[end].price / p0 - 1.0
    if kind == "reoccupied":
        return DEATH_RETURN
    return None


def forward_log_return(panel: Panel, netuid: int, i: int, horizon: int, *, death_floor: float = 0.01) -> float | None:
    """``log(max(1 + r, death_floor))`` of :func:`forward_return` (death → log(0.01) ≈ −4.6)."""
    r = forward_return(panel, netuid, i, horizon)
    if r is None:
        return None
    return math.log(max(1.0 + r, death_floor))


def block_to_unix(block: int, anchors: list[tuple[int, int]] | None = None) -> float:
    """Unix time of ``block``: piecewise-linear over sorted ``(block_number, unix)`` anchors, extrapolated at 12 s/block
    from the nearest anchor outside their range; the old constant anchor when no anchors are given (= old
    ``_reg_unix``)."""
    if not anchors:
        return _BLOCK_ANCHOR_TS + _BLOCK_TIME_S * (block - _BLOCK_ANCHOR_BLOCK)
    blocks = [a[0] for a in anchors]
    j = bisect.bisect_left(blocks, block)
    if j < len(anchors) and anchors[j][0] == block:
        return float(anchors[j][1])
    if j == 0:
        b0, t0 = anchors[0]
        return t0 + _BLOCK_TIME_S * (block - b0)
    if j == len(anchors):
        b1, t1 = anchors[-1]
        return t1 + _BLOCK_TIME_S * (block - b1)
    (b0, t0), (b1, t1) = anchors[j - 1], anchors[j]
    return t0 + (t1 - t0) * (block - b0) / (b1 - b0)


def _ro_conn(db_path: str) -> sqlite3.Connection:
    """Read-only connection (``mode=ro``); a path is required (13 §8.1: no settings fallback)."""
    if not db_path:
        raise ValueError("db_path is required")
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    return con
