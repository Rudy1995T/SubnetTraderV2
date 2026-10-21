"""``StreamHub`` — SSE fan-out over SQLite rowid watermarks (spec 14 §2.1, §3.5 ``/api/v1/stream``).

The DB is the only source: the hub polls ``scores``/``events`` by rowid every ``STREAM_POLL_SEC`` (or at once on
``nudge()``) and fans committed rows out to subscriber queues. Frame ids are ``<scores.id>-<events.id>`` — the pair of
watermarks after that frame — so a reconnect with ``Last-Event-ID`` replays straight from SQLite, also across a
restart. A slow subscriber (queue full) loses its oldest frame and gets one ``resync``.
"""
from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import app.config as app_config
from app.logging.logger import logger
from app.util.time import to_iso

if TYPE_CHECKING:  # pragma: no cover
    from app.storage.db import Database

QUEUE_MAX = 256
_ID = re.compile(r"^(\d+)-(\d+)$")


@dataclass(slots=True)
class Frame:
    event: str
    data: dict
    id: str | None = None

    def encode(self) -> str:
        head = f"id: {self.id}\n" if self.id else ""
        return f"{head}event: {self.event}\ndata: {json.dumps(self.data, separators=(',', ':'), default=str)}\n\n"


@dataclass(eq=False)
class Subscriber:
    types: frozenset[str]
    queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=QUEUE_MAX))
    resync_pending: bool = False

    def offer(self, f: Frame) -> None:
        if f.event not in self.types and f.event != "resync":
            return
        if self.queue.full():
            for _ in range(1 if self.resync_pending else 2):    # drop the oldest frame(s)
                self.queue.get_nowait()
            if not self.resync_pending:
                self.resync_pending = True
                self.queue.put_nowait(Frame("resync", {"reason": "slow_consumer"}))
        self.queue.put_nowait(f)


def parse_last_id(v: str | None) -> tuple[int, int] | None:
    if not v:
        return None
    m = _ID.match(v.strip())
    return (int(m.group(1)), int(m.group(2))) if m else None


def score_frame_data(r: dict) -> dict:
    def r2(x: Any) -> float | None:
        return None if x is None else round(float(x), 2)
    try:
        flags = json.loads(r["red_flags"]) if isinstance(r["red_flags"], str) else list(r["red_flags"] or [])
    except ValueError:
        flags = []
    return {"occupant_key": r["occupant_key"], "netuid": r["netuid"], "as_of": to_iso(r["as_of"]),
            "computed_at": to_iso(r["computed_at"]), "run_kind": r["run_kind"], "composite": r2(r["composite_smoothed"]),
            "quality": r2(r["quality"]), "velocity": r2(r["velocity"]), "tier": r["tier"],
            "confidence": r2(r["confidence"]), "red_flags": flags, "spec_version": r["spec_version"]}


def event_frame_data(r: dict) -> dict:
    try:
        payload = json.loads(r["payload"]) if isinstance(r["payload"], str) else (r["payload"] or {})
    except ValueError:
        payload = {}
    return {"id": r["id"], "netuid": r["netuid"], "occupant_key": r["occupant_key"], "event_type": r["event_type"],
            "severity": r["severity"], "event_at": to_iso(r["event_at"]), "observed_at": to_iso(r["observed_at"]),
            "payload": payload}


class StreamHub:
    def __init__(self, db: "Database") -> None:
        self.db = db
        self.subs: set[Subscriber] = set()
        self.sid = 0
        self.eid = 0
        self._ready = False
        self._lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._task: asyncio.Task | None = None

    # ── rows ───────────────────────────────────────────────────────────────────────────────────────────────
    async def _max_ids(self) -> tuple[int, int]:
        s = await self.db.fetchval("SELECT MAX(id) FROM scores") or 0
        e = await self.db.fetchval("SELECT MAX(id) FROM events") or 0
        return int(s), int(e)

    async def _scores_between(self, lo: int, hi: int, limit: int) -> list[dict]:
        return await self.db.fetchall(
            "SELECT id, occupant_key, netuid, as_of, computed_at, run_kind, composite_smoothed, quality, velocity, "
            "tier, confidence, red_flags, spec_version FROM scores WHERE id > ? AND id <= ? AND spec_version = ? "
            "AND shadow = 0 ORDER BY id LIMIT ?", (lo, hi, app_config.settings.SPEC_VERSION, limit))

    async def _events_between(self, lo: int, hi: int, limit: int) -> list[dict]:
        return await self.db.fetchall(
            "SELECT id, netuid, occupant_key, event_type, severity, event_at, observed_at, payload FROM events "
            "WHERE id > ? AND id <= ? ORDER BY id LIMIT ?", (lo, hi, limit))

    @staticmethod
    def _frames(scores: list[dict], events: list[dict], s0: int, e0: int, s1: int, e1: int) -> list[Frame]:
        out: list[Frame] = []
        for r in scores:
            out.append(Frame("score", score_frame_data(r), f"{r['id']}-{e0}"))
        for r in events:
            out.append(Frame("event", event_frame_data(r), f"{s1}-{r['id']}"))
        return out

    # ── lifecycle ──────────────────────────────────────────────────────────────────────────────────────────
    async def _init(self) -> None:
        if not self._ready:
            self.sid, self.eid = await self._max_ids()
            self._ready = True

    async def subscribe(self, types: frozenset[str], last_id: tuple[int, int] | None) -> Subscriber:
        """Register (replaying from ``last_id`` first); the caller checks the connection cap."""
        sub = Subscriber(types)
        async with self._lock:
            await self._init()
            if last_id is not None:
                await self._replay(sub, last_id)
            self.subs.add(sub)
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run())
        return sub

    def unsubscribe(self, sub: Subscriber) -> None:
        self.subs.discard(sub)
        if not self.subs:
            self._wake.set()

    async def _replay(self, sub: Subscriber, last: tuple[int, int]) -> None:
        cap = int(app_config.settings.STREAM_REPLAY_MAX)
        ls, le = min(last[0], self.sid), min(last[1], self.eid)
        scores = await self._scores_between(ls, self.sid, cap + 1) if "score" in sub.types else []
        events = await self._events_between(le, self.eid, cap + 1) if "event" in sub.types else []
        if len(scores) + len(events) > cap:
            sub.offer(Frame("resync", {"reason": "replay_too_large"}))
            return
        for f in self._frames(scores, events, ls, le, self.sid, self.eid):
            sub.offer(f)

    def nudge(self) -> None:
        self._wake.set()

    async def poll_once(self) -> int:
        """One poll: fan out rows above the watermarks; returns the number of frames sent."""
        async with self._lock:
            await self._init()
            s1, e1 = await self._max_ids()
            if s1 <= self.sid and e1 <= self.eid:
                return 0
            cap = 5000
            scores = await self._scores_between(self.sid, s1, cap) if s1 > self.sid else []
            events = await self._events_between(self.eid, e1, cap) if e1 > self.eid else []
            if scores and len(scores) == cap:
                s1 = int(scores[-1]["id"])
            if events and len(events) == cap:
                e1 = int(events[-1]["id"])
            frames = self._frames(scores, events, self.sid, self.eid, s1, e1)
            self.sid, self.eid = s1, e1
            for sub in list(self.subs):
                for f in frames:
                    sub.offer(f)
            return len(frames)

    async def _run(self) -> None:
        while self.subs:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=float(app_config.settings.STREAM_POLL_SEC))
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            if not self.subs:
                break
            try:
                await self.poll_once()
            except Exception as exc:
                logger.warning("stream hub poll failed", data={"error": f"{type(exc).__name__}: {exc}"})


_hub: StreamHub | None = None


def get_hub(db: "Database") -> StreamHub:
    global _hub
    if _hub is None or _hub.db is not db:
        _hub = StreamHub(db)
    return _hub


def nudge() -> None:
    """Wake the hub for an immediate poll; a no-op when no stream is open."""
    if _hub is not None:
        _hub.nudge()


def reset() -> None:  # tests
    global _hub
    _hub = None
