"""Phase 3A Regression Tests: Event Lifecycle, Buffer Capacity Reservation, Retry, and Recovery."""
import asyncio
import pytest

from helpers import ids, mk, run
from dedup_engine import (
    DedupEngine, Event, MemoryKV, MemorySink, Status, KVError,
    FaultyKV, FaultySink, SimulatedCrash
)


def test_full_buffer_without_drainable_event():
    """Buffer is full and events are held behind watermark (cannot drain) -> BUFFER_FULL.
    No KV record is created for the rejected event."""
    async def go():
        kv, sink = MemoryKV(), MemorySink()
        # lateness=10 keeps events held until watermark >= ts
        e = DedupEngine(kv, sink, window=100.0, lateness=10.0, max_buffer_size=2)

        assert await e.ingest(mk(1, 10.0)) == Status.ACCEPTED
        assert await e.ingest(mk(2, 11.0)) == Status.ACCEPTED
        assert len(e._heap) == 2

        # 3rd event exceeds capacity and cannot drain
        st = await e.ingest(mk(3, 12.0))
        assert st == Status.BUFFER_FULL
        assert e.m.buffer_overflow == 1
        assert len(e._heap) == 2

        # Verify no stranded KV claim was created for e3
        raw_e3 = await kv.get("dedup:e3")
        assert raw_e3 is None, f"Expected no KV claim for rejected event, found {raw_e3}"
    run(go())


def test_full_buffer_with_successful_flush():
    """Buffer is at capacity, but events are eligible for release -> flush succeeds, new event accepted."""
    async def go():
        kv, sink = MemoryKV(), MemorySink()
        # lateness=0 allows events to drain immediately if ts <= watermark
        e = DedupEngine(kv, sink, window=100.0, lateness=0.0, max_buffer_size=2)

        assert await e.ingest(mk(1, 10.0)) == Status.ACCEPTED
        assert await e.ingest(mk(2, 20.0)) == Status.ACCEPTED
        # Watermark is now 20.0, so e1 (ts 10.0) can drain on next flush
        # Now submit e3 with capacity=2:
        st = await e.ingest(mk(3, 30.0))
        assert st == Status.ACCEPTED
        assert "e1" in ids(sink)
    run(go())


def test_full_buffer_with_failed_flush():
    """Buffer is full, flush is attempted but fails (e.g. KV commit error) -> BUFFER_FULL without claim."""
    async def go():
        inner_kv = MemoryKV()
        kv = FaultyKV(inner_kv)
        sink = MemorySink()
        # lateness=10 keeps events held in heap
        e = DedupEngine(kv, sink, window=100.0, lateness=10.0, max_buffer_size=2, max_retries=0)

        # Ingest 2 events held in buffer
        assert await e.ingest(mk(1, 10.0)) == Status.ACCEPTED
        assert await e.ingest(mk(2, 10.0)) == Status.ACCEPTED
        assert len(e._heap) == 2

        # Advance watermark with a large event that triggers release of e1 and e2, but make KV commit fail
        kv.fail_rate = 1.0  # Make flush fail
        # Ingest 3rd event: buffer is full (len=2), flush attempted, fails, buffer remains full -> BUFFER_FULL
        st = await e.ingest(mk(3, 10.0))
        assert st == Status.BUFFER_FULL
        assert e.m.buffer_overflow == 1

        # e3 must NOT have a KV claim
        assert await inner_kv.get("dedup:e3") is None
    run(go())


def test_retry_after_capacity_becomes_available():
    """Rejected event returns BUFFER_FULL, freed capacity allows retry to return ACCEPTED,
    and subsequent retry returns DUPLICATE with exactly one downstream effect."""
    async def go():
        kv, sink = MemoryKV(), MemorySink()
        e = DedupEngine(kv, sink, window=100.0, lateness=10.0, max_buffer_size=2)

        assert await e.ingest(mk(1, 10.0)) == Status.ACCEPTED
        assert await e.ingest(mk(2, 10.0)) == Status.ACCEPTED

        # Rejected under full capacity
        assert await e.ingest(mk(3, 10.0)) == Status.BUFFER_FULL
        assert await kv.get("dedup:e3") is None

        # Free capacity by draining
        assert await e.drain() == 0
        assert ids(sink) == ["e1", "e2"]

        # Retry returns ACCEPTED
        assert await e.ingest(mk(3, 10.0)) == Status.ACCEPTED

        # Subsequent retry returns DUPLICATE (both while buffered and after commit)
        assert await e.ingest(mk(3, 10.0)) == Status.DUPLICATE
        assert await e.drain() == 0
        assert await e.ingest(mk(3, 10.0)) == Status.DUPLICATE

        # Exactly 1 downstream effect for each event
        assert ids(sink) == ["e1", "e2", "e3"]
    run(go())


def test_concurrent_submissions_while_buffer_is_nearly_full():
    """20 coroutines submit concurrently when buffer capacity is 5.
    Exactly 5 are ACCEPTED, 15 return BUFFER_FULL, and buffer limit is strictly respected."""
    async def go():
        kv, sink = MemoryKV(latency=0.0005), MemorySink()
        e = DedupEngine(kv, sink, window=100.0, lateness=10.0, max_buffer_size=5)

        tasks = [e.ingest(Event(f"ev_{i}", 10.0)) for i in range(20)]
        results = await asyncio.gather(*tasks)

        assert results.count(Status.ACCEPTED) == 5
        assert results.count(Status.BUFFER_FULL) == 15
        assert len(e._heap) == 5
        assert e._reserved == 0

        # Verify only the 5 accepted events have KV claims
        claimed = 0
        for i in range(20):
            if await kv.get(f"dedup:ev_{i}") is not None:
                claimed += 1
        assert claimed == 5
    run(go())


def test_kv_claim_failure_releases_reservation():
    """When KV claim fails with an exception, capacity reservation is cleanly released."""
    async def go():
        kv = FaultyKV(MemoryKV(), fail_rate=1.0)
        e = DedupEngine(kv, MemorySink(), window=100.0, lateness=10.0, max_buffer_size=2, max_retries=0)

        with pytest.raises(KVError):
            await e.ingest(mk(1, 10.0))

        assert e._reserved == 0
        assert len(e._heap) == 0

        # After KV recovers, full buffer capacity is usable
        kv.fail_rate = 0.0
        assert await e.ingest(mk(2, 10.0)) == Status.ACCEPTED
        assert await e.ingest(mk(3, 10.0)) == Status.ACCEPTED
        assert len(e._heap) == 2
    run(go())


def test_ambiguous_claim_acknowledgement_with_capacity():
    """Write lands in KV but ACK is lost: retry recognizes token, returns ACCEPTED,
    and capacity reservations are safely updated without leaking."""
    async def go():
        inner_kv = MemoryKV()
        kv = FaultyKV(inner_kv)
        sink = MemorySink()
        kv.once["put_if_absent"] = "after"  # write committed, client saw error

        e = DedupEngine(kv, sink, window=100.0, lateness=10.0, max_buffer_size=2)
        # Ingest retries internally via _retry and succeeds via token recognition
        st = await e.ingest(mk(1, 10.0))
        assert st == Status.ACCEPTED
        assert e._reserved == 0
        assert len(e._heap) == 1
    run(go())


def test_process_restart_after_buffer_full_no_resurrection():
    """A rejected BUFFER_FULL event must never be recovered or delivered after process restart."""
    async def go():
        kv = MemoryKV()
        sink1 = MemorySink()
        e1 = DedupEngine(kv, sink1, window=100.0, lateness=10.0, max_buffer_size=1)

        assert await e1.ingest(mk(1, 10.0)) == Status.ACCEPTED
        assert await e1.ingest(mk(2, 10.0)) == Status.BUFFER_FULL

        # Crash happens: simulate restart with fresh engine on same KV
        sink2 = MemorySink()
        e2 = DedupEngine(kv, sink2, window=100.0, lateness=10.0, max_buffer_size=1)
        recovered = await e2.recover()

        assert recovered == 1  # Only e1 recovered
        await e2.drain()
        assert ids(sink2) == ["e1"]
        assert "e2" not in ids(sink2)
    run(go())


def test_recovery_of_legitimately_accepted_events():
    """Events legitimately accepted and pending are correctly recovered across engine instances."""
    async def go():
        kv = MemoryKV()
        sink1 = MemorySink()
        e1 = DedupEngine(kv, sink1, window=100.0, lateness=10.0, max_buffer_size=5)

        for i in range(1, 4):
            assert await e1.ingest(mk(i, 10.0)) == Status.ACCEPTED

        # Restart engine
        sink2 = MemorySink()
        e2 = DedupEngine(kv, sink2, window=100.0, lateness=10.0, max_buffer_size=5)
        recovered = await e2.recover()
        assert recovered == 3

        await e2.drain()
        assert sorted(ids(sink2)) == ["e1", "e2", "e3"]
    run(go())


def test_cancellation_and_exception_cleanup_of_capacity_reservations():
    """Cancellation during ingestion releases capacity reservation."""
    async def go():
        kv = MemoryKV(latency=0.05)
        e = DedupEngine(kv, MemorySink(), window=100.0, lateness=10.0, max_buffer_size=2)

        task = asyncio.create_task(e.ingest(mk(1, 10.0)))
        await asyncio.sleep(0.01)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        assert e._reserved == 0
        # Buffer remains fully usable
        assert await e.ingest(mk(2, 10.0)) == Status.ACCEPTED
        assert await e.ingest(mk(3, 10.0)) == Status.ACCEPTED
        assert len(e._heap) == 2
    run(go())
