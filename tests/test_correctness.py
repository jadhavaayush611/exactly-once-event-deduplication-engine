"""Algorithm correctness: dedup, sliding window, watermark, out-of-order, eviction."""
import asyncio

import pytest
from helpers import ids, make_stream, mk, run

from dedup_engine import DedupEngine, Event, MemoryKV, MemorySink, Status


def engine(window=100.0, lateness=2.0, kv=None):
    kv, sink = kv or MemoryKV(), MemorySink()
    return DedupEngine(kv, sink, window=window, lateness=lateness), kv, sink


def test_basic_dedup():
    async def go():
        e, _, sink = engine()
        r = [await e.ingest(mk(1)) for _ in range(3)]
        await e.drain()
        assert r == [Status.ACCEPTED, Status.DUPLICATE, Status.DUPLICATE]
        assert ids(sink) == ["e1"]
    run(go())


def test_concurrent_duplicates_exactly_one_wins():
    async def go():
        e, _, sink = engine(kv=MemoryKV(latency=0.0005))
        res = await asyncio.gather(*[e.ingest(mk(7)) for _ in range(200)])
        await e.drain()
        assert res.count(Status.ACCEPTED) == 1 and res.count(Status.DUPLICATE) == 199
        assert ids(sink) == ["e7"]
    run(go())


def test_out_of_order_is_released_in_event_time_order():
    async def go():
        e, _, sink = engine(lateness=2.0)
        for t in [1, 3, 2, 5, 4, 6]:
            await e.ingest(mk(t))
        await e.drain()
        assert [x.ts for x in sink.effects] == [1, 2, 3, 4, 5, 6]
    run(go())


def test_events_are_held_until_watermark_passes():
    async def go():
        e, _, sink = engine(lateness=5.0)
        await e.ingest(mk(10))
        assert sink.effects == []           # ts 10 > watermark 5 -> buffered
        await e.ingest(mk(16))              # watermark 11 -> releases ts 10
        assert ids(sink) == ["e10"]
    run(go())


def test_late_event_inside_window_is_emitted_and_still_deduplicated():
    async def go():
        e, _, sink = engine(window=50.0, lateness=2.0)
        await e.ingest(mk(1, 100))
        assert await e.ingest(mk(2, 90)) == Status.ACCEPTED   # late (wm=98) but inside window
        assert await e.ingest(mk(2, 90)) == Status.DUPLICATE
        await e.drain()
        assert e.m.late_emitted == 1 and ids(sink).count("e2") == 1
    run(go())


def test_event_older_than_window_goes_to_dlq():
    async def go():
        e, _, sink = engine(window=50.0, lateness=2.0)
        await e.ingest(mk(1, 200))
        assert await e.ingest(mk(2, 100)) == Status.TOO_OLD   # < 198-50
        await e.drain()
        assert [x.id for x in e.dlq] == ["e2"] and ids(sink) == ["e1"]
    run(go())


def test_eviction_keeps_state_bounded():
    async def go():
        e, kv, _ = engine(window=10.0, lateness=1.0)
        for i in range(2000):
            await e.ingest(mk(i))
        assert await kv.size() < 60          # ~window worth of keys, not 2000
        assert e.m.evicted > 1900
    run(go())


def test_pending_records_are_never_evicted():
    async def go():
        e, kv, _ = engine(window=5.0, lateness=100.0)   # nothing is released
        for i in range(50):
            await e.ingest(mk(i))
        assert await kv.size() >= 50
        assert await e.drain() == 0
    run(go())


@pytest.mark.parametrize("seed", range(10))
def test_random_stream_exactly_once_and_ordered(seed):
    async def go():
        base, stream = make_stream(300, seed, dup_rate=0.5, lateness=1.0)
        e, _, sink = engine(window=1000.0, lateness=1.0)
        res = await asyncio.gather(*[e.ingest(x) for x in stream])
        await e.drain()
        assert res.count(Status.ACCEPTED) == len(base)
        assert sorted(ids(sink)) == sorted(x.id for x in base)
        ts = [x.ts for x in sink.effects]
        assert ts == sorted(ts)
    run(go())
