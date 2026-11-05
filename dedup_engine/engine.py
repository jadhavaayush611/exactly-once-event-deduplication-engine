"""Exactly-once (effect) event de-duplication engine.

Pipeline per event:
  1. late check      event.ts < watermark - window  -> DLQ (dedup state already evicted)
  2. atomic claim    KV put_if_absent(dedup:<id>, PENDING + payload)   <- write-ahead log
  3. reorder buffer  min-heap on event time, released when ts <= watermark
  4. deliver         sink.deliver(event) with retries (sink dedups on event.id)
  5. commit          KV set(dedup:<id>, COMMITTED, score=event.ts)  -> becomes evictable
  6. maintenance     checkpoint max_ts, evict records older than watermark - window

watermark = max_event_ts_seen - allowed_lateness
Crash anywhere -> recover(): re-load PENDING records from the KV store and re-deliver.
"""
from __future__ import annotations

import asyncio
import heapq
import itertools
import json
import math
import time
import uuid
from dataclasses import asdict, dataclass
from typing import Callable, Optional

from .kv import KVStore
from .models import Event, KVError, SinkError, Status
from .sink import Sink

PREFIX = "dedup:"
META_MAX_TS = "meta:max_ts"


@dataclass
class Metrics:
    received: int = 0
    accepted: int = 0
    duplicates: int = 0
    too_old: int = 0
    late_emitted: int = 0
    delivered: int = 0
    kv_retries: int = 0
    sink_retries: int = 0
    flush_failures: int = 0
    evicted: int = 0
    recovered: int = 0
    adopted: int = 0
    invalid_timestamp: int = 0
    too_far_future: int = 0
    buffer_overflow: int = 0

    def snapshot(self) -> dict:
        return asdict(self)


class DedupEngine:
    def __init__(self, kv: KVStore, sink: Sink, *, window: float = 300.0, lateness: float = 5.0,
                 max_retries: int = 6, backoff: float = 0.0005, batch_size: int = 256, checkpoint_step: float = 1.0,
                 max_future_skew: Optional[float] = None, max_buffer_size: Optional[int] = None,
                 max_evict_per_cycle: Optional[int] = None, recovery_batch_size: int = 256,
                 time_source: Optional[Callable[[], float]] = None,
                 crash_hook: Optional[Callable[[str], None]] = None, dlq: Optional[list] = None):
        self.kv, self.sink = kv, sink
        self.window, self.lateness = window, lateness
        self.max_retries, self.backoff = max_retries, backoff
        self.batch_size = batch_size
        self.checkpoint_step = checkpoint_step
        self.evict_step = window / 20.0
        self.max_future_skew = max_future_skew
        self.max_buffer_size = max_buffer_size
        self.max_evict_per_cycle = max_evict_per_cycle
        self.recovery_batch_size = recovery_batch_size
        self.time_source = time_source or time.time
        self.crash_hook = crash_hook
        self.dlq = dlq if dlq is not None else []
        self.m = Metrics()
        self._heap: list[tuple[float, int, Event]] = []
        self._seq = itertools.count()
        self._max_ts: Optional[float] = None
        self._lock = asyncio.Lock()
        self._capacity_lock = asyncio.Lock()
        self._reserved: int = 0
        self._live: set[str] = set()        # ids buffered / being delivered by THIS process
        self._claiming: dict[str, int] = {}  # ids with a claim currently in flight here
        self._last_ckpt = float("-inf")
        self._last_evict = float("-inf")

    # ---------------------------------------------------------------- helpers
    @property
    def watermark(self) -> float:
        return float("-inf") if self._max_ts is None else self._max_ts - self.lateness

    def _crash(self, point: str) -> None:
        if self.crash_hook:
            self.crash_hook(point)

    async def _retry(self, fn, exc, counter: str):
        delay = self.backoff
        for attempt in range(self.max_retries + 1):
            try:
                return await fn()
            except exc:
                if attempt == self.max_retries:
                    raise
                setattr(self.m, counter, getattr(self.m, counter) + 1)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 0.05)

    # ----------------------------------------------------------------- ingest
    async def ingest(self, event: Event) -> Status:
        m = self.m
        m.received += 1

        # 1. Non-finite or invalid timestamp check
        if not isinstance(event.ts, (int, float)) or math.isnan(event.ts) or math.isinf(event.ts):
            m.invalid_timestamp += 1
            self.dlq.append(event)
            return Status.INVALID_TIMESTAMP

        # 2. Clock-skew / future timestamp validation
        if self.max_future_skew is not None:
            now = self.time_source()
            if event.ts > now + self.max_future_skew:
                m.too_far_future += 1
                self.dlq.append(event)
                return Status.TOO_FAR_FUTURE

        # 3. Sliding window expiration boundary check
        wm = self.watermark
        if event.ts < wm - self.window:
            m.too_old += 1
            self.dlq.append(event)
            return Status.TOO_OLD

        # 4. Capacity reservation check before initiating write-ahead claim
        # If the event is already known (in-flight or buffered locally), it won't consume new buffer space.
        reserved = False
        if self.max_buffer_size is not None and event.id not in self._live and event.id not in self._claiming:
            async with self._capacity_lock:
                if len(self._heap) + self._reserved >= self.max_buffer_size:
                    await self._flush()
                if len(self._heap) + self._reserved >= self.max_buffer_size:
                    # Before rejecting, check if event is already in the KV store (a duplicate)
                    # so that duplicates are correctly recognized as DUPLICATE rather than BUFFER_FULL.
                    try:
                        cur = await self._retry(lambda: self.kv.get(PREFIX + event.id), KVError, "kv_retries")
                        if cur is not None:
                            m.duplicates += 1
                            return Status.DUPLICATE
                    except KVError:
                        pass
                    m.buffer_overflow += 1
                    return Status.BUFFER_FULL
                self._reserved += 1
                reserved = True

        try:
            token = uuid.uuid4().hex
            rec = json.dumps({"state": "pending", "ts": event.ts, "token": token, "event": event.to_dict()})
            self._claiming[event.id] = self._claiming.get(event.id, 0) + 1
            try:
                owned, existing = await self._claim(PREFIX + event.id, rec, token)
            finally:
                self._claiming[event.id] -= 1
                if not self._claiming[event.id]:
                    del self._claiming[event.id]
            if not owned:
                m.duplicates += 1
                # Orphan: a PENDING record nobody in this process is working on (an earlier claim
                # whose ack was lost for good). Adopt it, otherwise the event would be stranded.
                if (existing and existing.get("state") == "pending" and event.id not in self._live
                        and event.id not in self._claiming):
                    if not reserved and self.max_buffer_size is not None:
                        async with self._capacity_lock:
                            if len(self._heap) + self._reserved >= self.max_buffer_size:
                                await self._flush()
                            if len(self._heap) + self._reserved >= self.max_buffer_size:
                                m.buffer_overflow += 1
                                return Status.BUFFER_FULL
                            self._reserved += 1
                            reserved = True
                    m.adopted += 1
                    self._buffer(Event.from_dict(existing["event"]))
                    if reserved:
                        self._reserved -= 1
                        reserved = False
                    self._crash("after_buffer")
                    await self._flush()
                return Status.DUPLICATE

            m.accepted += 1
            self._crash("after_claim")
            if event.ts < wm:
                m.late_emitted += 1
            self._buffer(event)
            if reserved:
                self._reserved -= 1
                reserved = False
            self._crash("after_buffer")
            await self._flush()
            return Status.ACCEPTED
        finally:
            if reserved:
                self._reserved -= 1

    def _buffer(self, event: Event) -> None:
        # the watermark advances only on claimed events, so in-flight concurrent
        # requests cannot push it ahead of events that are still being claimed
        if self._max_ts is None or event.ts > self._max_ts:
            self._max_ts = event.ts
        self._live.add(event.id)
        heapq.heappush(self._heap, (event.ts, next(self._seq), event))

    async def _claim(self, key: str, rec: str, token: str):
        """Atomic claim -> (owned, existing_record). Survives 'applied but ack lost' via an
        ownership token stored inside the record."""
        async def attempt():
            if await self.kv.put_if_absent(key, rec, None):
                return True, None
            cur = await self.kv.get(key)
            if cur is None:
                raise KVError("record vanished during claim")
            cur = json.loads(cur)
            return cur.get("token") == token, cur   # our own earlier write?
        return await self._retry(attempt, KVError, "kv_retries")

    # ------------------------------------------------------------------ flush
    async def _flush(self, drain: bool = False) -> int:
        """Release buffered events. Sink delivery is sequential (preserves event-time order);
        KV commits of one batch run concurrently (hides KV round-trip latency)."""
        async with self._lock:
            while True:
                wm = self.watermark
                batch = []
                while self._heap and (drain or self._heap[0][0] <= wm) and len(batch) < self.batch_size:
                    batch.append(heapq.heappop(self._heap))
                if not batch:
                    break
                delivered, failed = [], False
                for i, item in enumerate(batch):
                    try:
                        await self._deliver(item[2])
                        delivered.append(item)
                    except (KVError, SinkError):
                        self.m.flush_failures += 1
                        for rest in batch[i:]:        # keep them; still PENDING in the KV store
                            heapq.heappush(self._heap, rest)
                        failed = True
                        break
                results = await asyncio.gather(*[self._commit(it[2]) for it in delivered],
                                               return_exceptions=True)
                for it, r in zip(delivered, results):
                    if isinstance(r, KVError):
                        self.m.flush_failures += 1
                        heapq.heappush(self._heap, it)  # redelivered later; sink is idempotent
                        failed = True
                    elif isinstance(r, BaseException):
                        raise r                         # e.g. SimulatedCrash
                if failed:
                    return len(self._heap)
            await self._maintenance()
            return len(self._heap)

    async def _deliver(self, ev: Event) -> None:
        self._crash("before_deliver")
        await self._retry(lambda: self.sink.deliver(ev), SinkError, "sink_retries")
        self._crash("after_deliver")

    async def _commit(self, ev: Event) -> None:
        rec = json.dumps({"state": "committed", "ts": ev.ts})
        await self._retry(lambda: self.kv.set(PREFIX + ev.id, rec, ev.ts), KVError, "kv_retries")
        self._live.discard(ev.id)
        self._crash("after_commit")
        self.m.delivered += 1

    async def _maintenance(self) -> None:
        if self._max_ts is None:
            return
        try:
            if self._max_ts - self._last_ckpt >= self.checkpoint_step:
                await self.kv.set(META_MAX_TS, repr(self._max_ts))
                self._last_ckpt = self._max_ts
            cutoff = self.watermark - self.window
            if cutoff - self._last_evict >= self.evict_step:
                evicted = await self.kv.evict_before(cutoff, limit=self.max_evict_per_cycle)
                self.m.evicted += evicted
                if self.max_evict_per_cycle is None or evicted < self.max_evict_per_cycle:
                    self._last_evict = cutoff
        except KVError:
            pass  # best effort; retried on the next flush

    async def drain(self) -> int:
        """End-of-stream: release everything buffered. Returns events still stuck."""
        return await self._flush(drain=True)

    # --------------------------------------------------------------- recovery
    async def recover(self) -> int:
        """Rebuild the reorder buffer from PENDING write-ahead records after a crash.
        Uses bounded incremental batch retrieval from the dedicated pending index,
        strictly respecting max_buffer_size and in-flight capacity reservations."""
        raw = await self._retry(lambda: self.kv.get(META_MAX_TS), KVError, "kv_retries")
        if raw:
            self._max_ts = float(raw)

        total_recovered = 0
        seen_keys: set[str] = set()

        while True:
            # Determine available capacity
            async with self._capacity_lock:
                current_used = len(self._heap) + self._reserved
                if self.max_buffer_size is not None:
                    avail = max(0, self.max_buffer_size - current_used)
                    if avail == 0:
                        # Reorder buffer is full; cannot recover more until drained/flushed
                        break
                    limit = min(avail, self.recovery_batch_size)
                else:
                    limit = self.recovery_batch_size

                if limit <= 0:
                    break
                self._reserved += limit
                reserved_for_batch = limit

            batch_recovered = 0
            new_items_seen = False
            try:
                # Fetch pending batch from KV store (fetch extra to skip already seen/queued items if needed)
                fetch_limit = reserved_for_batch + len(seen_keys)
                items = await self._retry(lambda: self.kv.get_pending_batch(fetch_limit),
                                          KVError, "kv_retries")
                if not items:
                    # No pending records left in index
                    break

                queued = {e.id for _, _, e in self._heap}
                batch_candidates = []

                for k, v in items:
                    if k in seen_keys:
                        continue
                    seen_keys.add(k)
                    new_items_seen = True

                    if v is None:
                        # Stale pending index entry (referenced key does not exist in store)
                        if hasattr(self.kv, "cleanup_stale_pending"):
                            try:
                                await self.kv.cleanup_stale_pending(k)
                            except KVError:
                                pass
                        continue
                    try:
                        rec = json.loads(v)
                    except Exception:
                        # Malformed pending record: clean up index entry
                        if hasattr(self.kv, "cleanup_stale_pending"):
                            try:
                                await self.kv.cleanup_stale_pending(k)
                            except KVError:
                                pass
                        continue

                    if not isinstance(rec, dict) or rec.get("state") != "pending" or "event" not in rec:
                        # Stale index entry pointing to committed or non-pending record
                        if hasattr(self.kv, "cleanup_stale_pending"):
                            try:
                                await self.kv.cleanup_stale_pending(k)
                            except KVError:
                                pass
                        continue

                    try:
                        ev = Event.from_dict(rec["event"])
                    except Exception:
                        continue

                    if ev.id in queued or ev.id in self._live:
                        continue

                    queued.add(ev.id)
                    batch_candidates.append(ev)
                    if len(batch_candidates) >= reserved_for_batch:
                        break

                # Buffer recovered events
                for ev in batch_candidates:
                    self._buffer(ev)
                    batch_recovered += 1

                total_recovered += batch_recovered
                self.m.recovered += batch_recovered

            finally:
                async with self._capacity_lock:
                    self._reserved -= reserved_for_batch

            if not new_items_seen or len(items) < fetch_limit:
                break

        await self._flush()
        return total_recovered

    def stats(self) -> dict:
        d = self.m.snapshot()
        d.update(buffered=len(self._heap), reserved=self._reserved,
                 watermark=None if self._max_ts is None else self.watermark,
                 dlq=len(self.dlq))
        return d
