"""Deterministic tests for sliding timestamp window semantics, boundary conditions,
clock-skew guarding, memory bounding, and expiration behavior (Phase 2)."""
import asyncio
import math

import pytest
from helpers import ids, mk, run

from dedup_engine import DedupEngine, Event, MemoryKV, MemorySink, Status


def test_normal_in_order_events():
    """Requirement 6.1: In-order events advance watermark and release when ts <= watermark."""
    async def go():
        sink = MemorySink()
        kv = MemoryKV()
        e = DedupEngine(kv, sink, window=60.0, lateness=2.0)

        # Ingest t=10.0: wm = 8.0 -> t=10 > 8.0, held in buffer
        assert await e.ingest(mk(1, 10.0)) == Status.ACCEPTED
        assert e.watermark == 8.0
        assert ids(sink) == []
        assert e.stats()["buffered"] == 1

        # Ingest t=12.0: wm = 10.0 -> t=10.0 <= 10.0, releases e1!
        assert await e.ingest(mk(2, 12.0)) == Status.ACCEPTED
        assert e.watermark == 10.0
        assert ids(sink) == ["e1"]
        assert e.stats()["buffered"] == 1  # e2 held

        # Ingest t=15.0: wm = 13.0 -> releases e2 (ts 12.0 <= 13.0)
        assert await e.ingest(mk(3, 15.0)) == Status.ACCEPTED
        assert e.watermark == 13.0
        assert ids(sink) == ["e1", "e2"]

        # Drain releases remaining e3 (ts 15.0)
        assert await e.drain() == 0
        assert ids(sink) == ["e1", "e2", "e3"]
    run(go())


def test_valid_out_of_order_events():
    """Requirement 6.2: Out-of-order arrivals within lateness are released in event-time order."""
    async def go():
        sink = MemorySink()
        e = DedupEngine(MemoryKV(), sink, window=60.0, lateness=5.0)

        # Sequence of timestamps: [10, 14, 12, 11, 13, 16]
        # Bounded disorder: max arrival ts is 14 when 11 arrives (diff = 3 <= lateness 5)
        for i, ts in enumerate([10.0, 14.0, 12.0, 11.0, 13.0, 16.0], 1):
            assert await e.ingest(Event(f"ev{i}", ts)) == Status.ACCEPTED

        await e.drain()
        # Downstream sink must receive events sorted strictly by event time
        assert [x.ts for x in sink.effects] == [10.0, 11.0, 12.0, 13.0, 14.0, 16.0]
        assert [x.id for x in sink.effects] == ["ev1", "ev4", "ev3", "ev5", "ev2", "ev6"]
    run(go())


def test_late_events_within_accepted_lateness():
    """Requirement 6.3: Late events arriving with cutoff <= ts < watermark are accepted and emitted."""
    async def go():
        sink = MemorySink()
        e = DedupEngine(MemoryKV(), sink, window=50.0, lateness=5.0)

        # Establish watermark at 100.0 (max_ts=105.0)
        await e.ingest(Event("anchor", 105.0))
        assert e.watermark == 100.0

        # Event arrives with ts=98.0: cutoff is 100 - 50 = 50.0. 50 <= 98 < 100 -> late!
        st = await e.ingest(Event("late-1", 98.0))
        assert st == Status.ACCEPTED
        assert e.m.late_emitted == 1

        # Resending late event inside window is identified as duplicate
        assert await e.ingest(Event("late-1", 98.0)) == Status.DUPLICATE

        await e.drain()
        assert "late-1" in ids(sink)
        assert ids(sink).count("late-1") == 1
    run(go())


def test_events_beyond_lateness_limit():
    """Requirement 6.4: Late arrival vs expired: events older than window are sent to DLQ."""
    async def go():
        sink = MemorySink()
        e = DedupEngine(MemoryKV(), sink, window=40.0, lateness=10.0)

        # Anchor at ts=110.0 -> watermark = 100.0. Cutoff = 100.0 - 40.0 = 60.0.
        await e.ingest(Event("anchor", 110.0))
        assert e.watermark == 100.0

        # Event A: ts=75.0 (late relative to watermark 100, but inside retention window 60..100)
        assert await e.ingest(Event("late-inside", 75.0)) == Status.ACCEPTED
        assert e.m.late_emitted == 1

        # Event B: ts=59.0 (older than cutoff 60.0 -> expired)
        assert await e.ingest(Event("expired-dlq", 59.0)) == Status.TOO_OLD
        assert e.m.too_old == 1
        assert [x.id for x in e.dlq] == ["expired-dlq"]

        await e.drain()
        assert "late-inside" in ids(sink)
        assert "expired-dlq" not in ids(sink)
    run(go())


def test_far_future_event_does_not_advance_watermark_or_evict():
    """Requirement 6.5: Implausibly future timestamps are rejected without advancing watermark."""
    async def go():
        sink = MemorySink()
        kv = MemoryKV()
        # Simulated wall-clock time fixed at 1000.0
        clock = 1000.0
        e = DedupEngine(kv, sink, window=100.0, lateness=5.0,
                        max_future_skew=60.0, time_source=lambda: clock)

        # 1. Ingest normal baseline event at current time
        assert await e.ingest(Event("normal-1", 1000.0)) == Status.ACCEPTED
        assert e.watermark == 995.0
        assert e._max_ts == 1000.0

        # 2. Ingest malicious / malformed event far in future (ts=2000.0, skew=1000s > max 60s)
        st_future = await e.ingest(Event("future-bomb", 2000.0))
        assert st_future == Status.TOO_FAR_FUTURE
        assert e.m.too_far_future == 1
        assert [x.id for x in e.dlq] == ["future-bomb"]

        # CRITICAL ASSERTION: watermark and max_ts must NOT advance!
        assert e._max_ts == 1000.0
        assert e.watermark == 995.0

        # 3. Subsequent normal event at 1005.0 must be accepted normally
        # (In the vulnerable baseline, future-bomb advanced watermark to 1995.0,
        # which would have caused normal-2 to be rejected as TOO_OLD and normal-1 evicted!)
        st_normal = await e.ingest(Event("normal-2", 1005.0))
        assert st_normal == Status.ACCEPTED
        assert e.watermark == 1000.0

        await e.drain()
        assert sorted(ids(sink)) == ["normal-1", "normal-2"]
    run(go())


def test_invalid_and_non_finite_timestamps():
    """Requirement 6.6: NaN, +/-inf, and non-numeric timestamps are rejected with INVALID_TIMESTAMP."""
    async def go():
        sink = MemorySink()
        kv = MemoryKV()
        e = DedupEngine(kv, sink)

        # NaN
        assert await e.ingest(Event("nan-evt", float("nan"))) == Status.INVALID_TIMESTAMP
        # +inf
        assert await e.ingest(Event("inf-evt", float("inf"))) == Status.INVALID_TIMESTAMP
        # -inf
        assert await e.ingest(Event("neginf-evt", float("-inf"))) == Status.INVALID_TIMESTAMP
        # Non-numeric
        assert await e.ingest(Event("str-evt", "not_a_float")) == Status.INVALID_TIMESTAMP

        assert e.m.invalid_timestamp == 4
        assert len(e.dlq) == 4
        assert e.watermark == float("-inf")
        assert e.stats()["buffered"] == 0
        assert await kv.size() == 0
        assert ids(sink) == []
    run(go())


def test_events_at_exact_expiration_boundary_and_either_side():
    """Requirement 6.7: Exact boundary semantics: [W - window, inf) accepted; (-inf, W - window) TOO_OLD."""
    async def go():
        sink = MemorySink()
        e = DedupEngine(MemoryKV(), sink, window=100.0, lateness=0.0)

        # Set watermark at exactly 200.0
        await e.ingest(Event("anchor", 200.0))
        assert e.watermark == 200.0
        # Cutoff = 200.0 - 100.0 = 100.0

        # 1. Boundary minus epsilon (99.999): strictly < cutoff -> TOO_OLD
        assert await e.ingest(Event("below-boundary", 99.999)) == Status.TOO_OLD

        # 2. Exact boundary (100.0): not < cutoff -> ACCEPTED (inside window)
        assert await e.ingest(Event("exact-boundary", 100.0)) == Status.ACCEPTED

        # 3. Boundary plus epsilon (100.001): strictly > cutoff -> ACCEPTED
        assert await e.ingest(Event("above-boundary", 100.001)) == Status.ACCEPTED

        await e.drain()
        assert "below-boundary" in [x.id for x in e.dlq]
        assert "below-boundary" not in ids(sink)
        assert "exact-boundary" in ids(sink)
        assert "above-boundary" in ids(sink)
    run(go())


def test_duplicate_ids_with_matching_timestamps():
    """Requirement 6.8: Duplicate IDs with identical timestamps yield DUPLICATE status and 1 sink effect."""
    async def go():
        sink = MemorySink()
        e = DedupEngine(MemoryKV(), sink, window=50.0, lateness=2.0)

        ev = Event("order-101", 50.0, {"amount": 99.95})
        assert await e.ingest(ev) == Status.ACCEPTED
        assert await e.ingest(ev) == Status.DUPLICATE
        assert await e.ingest(ev) == Status.DUPLICATE

        await e.drain()
        assert ids(sink) == ["order-101"]
        assert sink.attempts == 1
    run(go())


def test_duplicate_ids_with_conflicting_timestamps_or_payloads():
    """Requirement 6.9: Repeated event IDs with modified timestamps or payloads are rejected on ID claim."""
    async def go():
        sink = MemorySink()
        e = DedupEngine(MemoryKV(), sink, window=50.0, lateness=2.0)

        # Original submission
        assert await e.ingest(Event("tx-1", 50.0, {"v": 1})) == Status.ACCEPTED

        # Conflicting duplicate with higher timestamp and changed payload
        assert await e.ingest(Event("tx-1", 55.0, {"v": 999})) == Status.DUPLICATE

        # Conflicting duplicate with earlier timestamp
        assert await e.ingest(Event("tx-1", 45.0, {"v": 0})) == Status.DUPLICATE

        await e.drain()
        # Exactly one effect with original timestamp and payload
        assert len(sink.effects) == 1
        assert sink.effects[0].ts == 50.0
        assert sink.effects[0].data == {"v": 1}
    run(go())


def test_state_eviction_and_replay_after_expiration():
    """Requirement 6.10: State is evicted after window; replays with original ts are DLQ'd (no double effect)."""
    async def go():
        sink = MemorySink()
        kv = MemoryKV()
        # Small window=20s, lateness=0s
        e = DedupEngine(kv, sink, window=20.0, lateness=0.0)

        # Ingest e1 at ts=10.0
        assert await e.ingest(Event("e1", 10.0)) == Status.ACCEPTED
        await e.drain()
        assert ids(sink) == ["e1"]

        # Advance stream so watermark reaches 40.0 -> cutoff = 40.0 - 20.0 = 20.0
        assert await e.ingest(Event("e2", 40.0)) == Status.ACCEPTED
        await e.drain()

        # State for e1 (ts=10.0 < cutoff 20.0) is evicted
        assert await kv.get("dedup:e1") is None
        assert e.m.evicted >= 1

        # Replay e1 with its original timestamp 10.0:
        # Since ts=10.0 < cutoff 20.0, engine classifies it as TOO_OLD!
        # It does NOT re-claim or re-deliver it!
        assert await e.ingest(Event("e1", 10.0)) == Status.TOO_OLD
        assert [x.id for x in e.dlq] == ["e1"]
        assert sink.attempts == 2  # e1 and e2 each delivered once

        # Documented Boundary: If a producer re-stamps e1 with new timestamp 45.0 after eviction,
        # the engine accepts it (since KV dedup key was evicted) and attempts sink delivery.
        assert await e.ingest(Event("e1", 45.0)) == Status.ACCEPTED
        await e.drain()
        assert sink.attempts == 3  # Engine attempted delivery 3 times
        # Idempotent MemorySink protects downstream from duplicate side effect:
        assert ids(sink) == ["e1", "e2"]
    run(go())


def test_watermark_behavior_after_rejected_events():
    """Requirement 6.11: Rejected events (TOO_OLD, TOO_FAR_FUTURE, INVALID_TIMESTAMP, DUPLICATE) do not alter watermark."""
    async def go():
        clock = 100.0
        e = DedupEngine(MemoryKV(), MemorySink(), window=50.0, lateness=5.0,
                        max_future_skew=30.0, time_source=lambda: clock)

        # Baseline: anchor at ts=100.0 -> max_ts=100.0, wm=95.0
        await e.ingest(Event("anchor", 100.0))
        assert e._max_ts == 100.0
        assert e.watermark == 95.0

        # 1. TOO_OLD
        await e.ingest(Event("old", 10.0))
        assert e._max_ts == 100.0
        assert e.watermark == 95.0

        # 2. TOO_FAR_FUTURE
        await e.ingest(Event("future", 500.0))
        assert e._max_ts == 100.0
        assert e.watermark == 95.0

        # 3. INVALID_TIMESTAMP
        await e.ingest(Event("nan", float("nan")))
        assert e._max_ts == 100.0
        assert e.watermark == 95.0

        # 4. DUPLICATE
        await e.ingest(Event("anchor", 100.0))
        assert e._max_ts == 100.0
        assert e.watermark == 95.0
    run(go())


def test_bounded_cleanup_and_buffer_overflow():
    """Requirement 6.12: Bounded maintenance batching and observable reorder-buffer capacity limit."""
    async def go():
        # Part A: Bounded cleanup per cycle
        kv = MemoryKV()
        sink = MemorySink()
        e = DedupEngine(kv, sink, window=10.0, lateness=0.0, max_evict_per_cycle=3)

        # Populate KV with 10 committed keys with score < 5.0
        for i in range(10):
            await kv.set(f"dedup:old-{i}", '{"state":"committed"}', score=float(i * 0.1))

        # Advance watermark to 20.0 -> cutoff = 10.0 (all 10 keys eligible for eviction)
        await e.ingest(Event("advancer", 20.0))
        # Maintenance evicted exactly max_evict_per_cycle = 3 keys
        assert e.m.evicted == 3

        # Next flush evicts the next batch of 3
        await e.drain()
        assert e.m.evicted >= 6

        # Part B: Buffer overflow capacity enforcement
        e_buf = DedupEngine(MemoryKV(), MemorySink(), window=100.0, lateness=50.0, max_buffer_size=3)
        # Ingest 3 events that are held (ts > watermark)
        assert await e_buf.ingest(Event("b1", 10.0)) == Status.ACCEPTED
        assert await e_buf.ingest(Event("b2", 11.0)) == Status.ACCEPTED
        assert await e_buf.ingest(Event("b3", 12.0)) == Status.ACCEPTED
        assert e_buf.stats()["buffered"] == 3

        # Ingest 4th event: heap is full and cannot release (wm = 12 - 50 = -38 < ts 10)
        st_overflow = await e_buf.ingest(Event("b4", 13.0))
        assert st_overflow == Status.BUFFER_FULL
        assert e_buf.m.buffer_overflow == 1
        assert e_buf.stats()["buffered"] == 3

        # Drain frees buffer
        assert await e_buf.drain() == 0
        assert e_buf.stats()["buffered"] == 0
    run(go())
