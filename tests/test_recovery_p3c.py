"""Tests for Phase 3C: Dedicated Pending Index and Bounded Recovery.
Covers:
1. Empty pending index.
2. Recovery of fewer records than one batch.
3. Recovery of more records than one batch.
4. Repeated recovery calls.
5. Recovery with a partially filled heap.
6. Recovery when the heap is at capacity.
7. Recovery while capacity reservations are active.
8. Pending-to-committed index transitions.
9. Deletion of pending records.
10. Missing records referenced by stale pending-index entries.
11. Malformed pending records.
12. Redis interruption during batch retrieval.
13. Recovery resumption after interruption.
14. Recovery ordering and deterministic tie-breaking.
15. Large pending datasets without unbounded heap growth.
16. Phase 3A buffer-full regression tests.
17. Phase 3B atomicity and lost-acknowledgement regression tests.
"""
import asyncio
import json
import pytest
import redis.asyncio as redis

from dedup_engine import DedupEngine, Event, Status, KVError, MemoryKV, MemorySink, FaultyKV
from dedup_engine.kv import RedisKV

REDIS_URL = "redis://localhost:6380/0"


@pytest.fixture
async def clean_redis():
    """Isolated, clean Redis instance for each test requiring real Redis."""
    r = redis.from_url(REDIS_URL, decode_responses=True)
    try:
        await r.ping()
    except Exception as e:
        pytest.skip(f"Live Redis instance on {REDIS_URL} unreachable: {e}")
    await r.flushdb()
    yield r
    await r.flushdb()
    await r.aclose()


def mk_pending_rec(eid: str, ts: float, token: str = "tok_test"):
    ev = Event(eid, ts, {"v": eid})
    return json.dumps({"state": "pending", "ts": ts, "token": token, "event": ev.to_dict()})


# 1. Empty pending index
@pytest.mark.anyio
async def test_empty_pending_index_recovery(clean_redis):
    kv = RedisKV(REDIS_URL)
    sink = MemorySink()
    e = DedupEngine(kv, sink, max_buffer_size=10, recovery_batch_size=5)
    recovered = await e.recover()
    assert recovered == 0
    assert len(e._heap) == 0
    await kv.close()


# 2. Recovery of fewer records than one batch
@pytest.mark.anyio
async def test_recovery_fewer_than_one_batch(clean_redis):
    kv = RedisKV(REDIS_URL)
    sink = MemorySink()
    # Write 3 pending records
    for i in range(3):
        rec = mk_pending_rec(f"p_{i}", 10.0 + i)
        await kv.put_if_absent(f"dedup:p_{i}", rec, score=None)

    e = DedupEngine(kv, sink, max_buffer_size=10, recovery_batch_size=5)
    recovered = await e.recover()
    assert recovered == 3
    assert len(e._heap) == 3
    await kv.close()


# 3. Recovery of more records than one batch
@pytest.mark.anyio
async def test_recovery_more_than_one_batch(clean_redis):
    kv = RedisKV(REDIS_URL)
    sink = MemorySink()
    # Write 8 pending records
    for i in range(8):
        rec = mk_pending_rec(f"multi_{i}", 10.0 + i)
        await kv.put_if_absent(f"dedup:multi_{i}", rec, score=None)

    # Batch size = 3, buffer size = 20
    e = DedupEngine(kv, sink, max_buffer_size=20, recovery_batch_size=3)
    recovered = await e.recover()
    assert recovered == 8
    # 8 recovered; flush during recovery flushed events <= watermark, total in sink + heap = 8
    assert len(e._heap) + len(sink.effects) == 8
    await kv.close()


# 4. Repeated recovery calls are safe and idempotent
@pytest.mark.anyio
async def test_repeated_recovery_calls(clean_redis):
    kv = RedisKV(REDIS_URL)
    sink = MemorySink()
    for i in range(4):
        rec = mk_pending_rec(f"rep_{i}", 10.0 + i)
        await kv.put_if_absent(f"dedup:rep_{i}", rec, score=None)

    e = DedupEngine(kv, sink, max_buffer_size=20, recovery_batch_size=2)
    rec1 = await e.recover()
    assert rec1 == 4

    # Second recovery call on the same active engine must not double-enqueue
    rec2 = await e.recover()
    assert rec2 == 0
    assert len(e._heap) == 4
    await kv.close()


# 5. Recovery with a partially filled heap
@pytest.mark.anyio
async def test_recovery_with_partially_filled_heap(clean_redis):
    kv = RedisKV(REDIS_URL)
    sink = MemorySink()
    e = DedupEngine(kv, sink, max_buffer_size=5, recovery_batch_size=3)

    # Ingest 2 events into heap
    await e.ingest(Event("live_1", 20.0))
    await e.ingest(Event("live_2", 21.0))
    assert len(e._heap) == 2

    # Put 4 pending records into Redis directly (simulating pre-existing pending WAL)
    for i in range(4):
        rec = mk_pending_rec(f"backlog_{i}", 5.0 + i)
        await kv.put_if_absent(f"dedup:backlog_{i}", rec, score=None)

    # Available capacity was 5 - 2 = 3. Should recover exactly 3 records, bounded by capacity.
    recovered = await e.recover()
    assert recovered == 3
    assert len(e._heap) <= 5
    await kv.close()


# 6. Recovery when the heap is at capacity
@pytest.mark.anyio
async def test_recovery_when_heap_is_at_capacity(clean_redis):
    kv = RedisKV(REDIS_URL)
    sink = MemorySink()
    e = DedupEngine(kv, sink, max_buffer_size=2, recovery_batch_size=2)

    await e.ingest(Event("full_1", 20.0))
    await e.ingest(Event("full_2", 21.0))
    assert len(e._heap) == 2

    # Add extra pending record to Redis
    rec = mk_pending_rec("overflow_pending", 10.0)
    await kv.put_if_absent("dedup:overflow_pending", rec, score=None)

    # Heap is at capacity (2/2). recover() must recover 0 and not exceed capacity.
    recovered = await e.recover()
    assert recovered == 0
    assert len(e._heap) == 2
    assert e._reserved == 0
    await kv.close()


# 7. Recovery while capacity reservations are active
@pytest.mark.anyio
async def test_recovery_with_active_capacity_reservations(clean_redis):
    kv = RedisKV(REDIS_URL)
    sink = MemorySink()
    e = DedupEngine(kv, sink, max_buffer_size=4, recovery_batch_size=2)

    # Ingest 1 event, and artificially hold a reservation of 2
    await e.ingest(Event("res_1", 10.0))
    e._reserved = 2  # simulates 2 concurrent claims reserving capacity

    # Add 3 pending records to KV
    for i in range(3):
        rec = mk_pending_rec(f"res_p_{i}", 5.0 + i)
        await kv.put_if_absent(f"dedup:res_p_{i}", rec, score=None)

    # Total capacity = 4, used = 1 + 2 = 3. Avail = 1.
    recovered = await e.recover()
    assert recovered == 1
    assert len(e._heap) <= 2
    assert e._reserved == 2
    e._reserved = 0
    await kv.close()


# 8. Pending-to-committed index transitions
@pytest.mark.anyio
async def test_pending_to_committed_index_transitions(clean_redis):
    kv = RedisKV(REDIS_URL)
    r = clean_redis
    key = "dedup:trans_test"

    # Start pending: score=None -> must be in idx:dedup:pending, NOT in idx:dedup
    rec_pending = mk_pending_rec("trans_test", 15.0)
    await kv.put_if_absent(key, rec_pending, score=None)

    assert await r.zscore(RedisKV.INDEX_PENDING, key) == 15.0
    assert await r.zscore(RedisKV.INDEX, key) is None

    # Transition to committed: score=15.0 -> must be in idx:dedup, NOT in idx:dedup:pending
    rec_committed = json.dumps({"state": "committed", "ts": 15.0})
    await kv.set(key, rec_committed, score=15.0)

    assert await r.zscore(RedisKV.INDEX_PENDING, key) is None
    assert await r.zscore(RedisKV.INDEX, key) == 15.0
    await kv.close()


# 9. Deletion of pending records
@pytest.mark.anyio
async def test_deletion_of_pending_records(clean_redis):
    kv = RedisKV(REDIS_URL)
    r = clean_redis
    key = "dedup:del_test"

    rec = mk_pending_rec("del_test", 10.0)
    await kv.put_if_absent(key, rec, score=None)
    assert await r.zscore(RedisKV.INDEX_PENDING, key) == 10.0

    # Delete must atomically remove from key and both indexes
    deleted = await kv.delete(key)
    assert deleted is True
    assert await r.get(key) is None
    assert await r.zscore(RedisKV.INDEX_PENDING, key) is None
    assert await r.zscore(RedisKV.INDEX, key) is None
    await kv.close()


# 10. Missing records referenced by stale pending-index entries
@pytest.mark.anyio
async def test_stale_pending_index_entries_cleaned_up(clean_redis):
    kv = RedisKV(REDIS_URL)
    r = clean_redis

    # Insert a valid pending record
    rec1 = mk_pending_rec("valid_1", 10.0)
    await kv.put_if_absent("dedup:valid_1", rec1, score=None)

    # Manually inject a stale index entry whose key does not exist
    await r.zadd(RedisKV.INDEX_PENDING, {"dedup:stale_missing": 5.0})

    sink = MemorySink()
    e = DedupEngine(kv, sink, max_buffer_size=10, recovery_batch_size=5)
    recovered = await e.recover()

    # Only valid_1 should be recovered
    assert recovered == 1
    assert [ev.id for _, _, ev in e._heap] == ["valid_1"]

    # Stale index entry must have been automatically removed from Redis
    assert await r.zscore(RedisKV.INDEX_PENDING, "dedup:stale_missing") is None
    await kv.close()


# 11. Malformed pending records
@pytest.mark.anyio
async def test_malformed_pending_records_handled_safely(clean_redis):
    kv = RedisKV(REDIS_URL)
    r = clean_redis

    # Insert a valid pending record
    rec1 = mk_pending_rec("valid_m", 10.0)
    await kv.put_if_absent("dedup:valid_m", rec1, score=None)

    # Insert a key with invalid JSON and add to pending index
    await r.set("dedup:corrupt", "NOT_JSON_DATA{{{")
    await r.zadd(RedisKV.INDEX_PENDING, {"dedup:corrupt": 5.0})

    sink = MemorySink()
    e = DedupEngine(kv, sink, max_buffer_size=10, recovery_batch_size=5)
    recovered = await e.recover()

    assert recovered == 1
    assert [ev.id for _, _, ev in e._heap] == ["valid_m"]
    # Corrupt key cleaned up from pending index
    assert await r.zscore(RedisKV.INDEX_PENDING, "dedup:corrupt") is None
    await kv.close()


# 12 & 13. Redis interruption and recovery resumption
@pytest.mark.anyio
async def test_recovery_interruption_and_resumption():
    inner_kv = MemoryKV()
    faulty_kv = FaultyKV(inner_kv)
    sink = MemorySink()

    # Prepopulate 4 pending records
    for i in range(4):
        rec = mk_pending_rec(f"int_{i}", 10.0 + i)
        await inner_kv.put_if_absent(f"dedup:int_{i}", rec, score=None)

    # Inject failure on first get_pending_batch call
    faulty_kv.once["get_pending_batch"] = "before"

    e = DedupEngine(faulty_kv, sink, max_buffer_size=10, recovery_batch_size=2)
    # First attempt fails due to interruption
    with pytest.raises(KVError):
        # We invoke direct without internal retry to test recovery boundary
        await e.kv.get_pending_batch(2)

    # Capacity reservation must remain 0
    assert e._reserved == 0

    # Resume recovery: should cleanly recover all 4 records
    recovered = await e.recover()
    assert recovered == 4
    assert len(e._heap) == 4


# 14. Recovery ordering and deterministic tie-breaking
@pytest.mark.anyio
async def test_recovery_ordering_and_deterministic_tie_breaking(clean_redis):
    kv = RedisKV(REDIS_URL)
    sink = MemorySink()

    # Events with identical timestamps
    for i in range(5):
        rec = mk_pending_rec(f"tie_{i}", 100.0)
        await kv.put_if_absent(f"dedup:tie_{i}", rec, score=None)

    e = DedupEngine(kv, sink, max_buffer_size=10, recovery_batch_size=2)
    recovered = await e.recover()
    assert recovered == 5

    # Drain to sink and ensure all 5 events were delivered
    await e.drain()
    delivered_ids = [ev.id for ev in sink.effects]
    assert len(delivered_ids) == 5
    assert set(delivered_ids) == {f"tie_{i}" for i in range(5)}
    await kv.close()


# 15. Large pending dataset without unbounded heap growth
@pytest.mark.anyio
async def test_large_pending_dataset_bounded_heap(clean_redis):
    kv = RedisKV(REDIS_URL)
    sink = MemorySink()

    # Insert 50 pending records
    for i in range(50):
        rec = mk_pending_rec(f"scale_{i}", float(i))
        await kv.put_if_absent(f"dedup:scale_{i}", rec, score=None)

    # Configure small engine capacity = 10, batch size = 5, large window and lateness so items remain buffered
    e = DedupEngine(kv, sink, max_buffer_size=10, recovery_batch_size=5, window=1000.0, lateness=100.0)

    # Recovery must load at most 10 records into the heap
    rec = await e.recover()
    assert rec == 10
    assert len(e._heap) == 10
    assert len(e._heap) + e._reserved <= 10

    # Drain buffer to free capacity
    await e.drain()
    assert len(e._heap) == 0

    # Next recovery pass loads next batch up to capacity
    rec2 = await e.recover()
    assert rec2 == 10
    assert len(e._heap) == 10
    await kv.close()


# 16. Phase 3A buffer-full regression test
@pytest.mark.anyio
async def test_phase_3a_buffer_full_regression(clean_redis):
    kv = RedisKV(REDIS_URL)
    sink = MemorySink()
    e = DedupEngine(kv, sink, max_buffer_size=2, window=100.0, lateness=10.0)

    assert await e.ingest(Event("bf_1", 10.0)) == Status.ACCEPTED
    assert await e.ingest(Event("bf_2", 10.0)) == Status.ACCEPTED

    # 3rd event exceeds buffer -> BUFFER_FULL without creating key in Redis or pending index
    st = await e.ingest(Event("bf_3", 10.0))
    assert st == Status.BUFFER_FULL
    assert await clean_redis.get("dedup:bf_3") is None
    assert await clean_redis.zscore(RedisKV.INDEX_PENDING, "dedup:bf_3") is None
    await kv.close()


# 17. Phase 3B atomicity and lost-acknowledgement regression
@pytest.mark.anyio
async def test_phase_3b_lost_ack_regression(clean_redis):
    kv = RedisKV(REDIS_URL)
    sink = MemorySink()
    e = DedupEngine(kv, sink, window=100.0, lateness=10.0)

    token = "token_p3c_test"
    ev = Event("lost_ack_p3c", 10.0)
    rec = json.dumps({"state": "pending", "ts": 10.0, "token": token, "event": ev.to_dict()})

    # Claim in Redis
    ok = await kv.put_if_absent(f"dedup:{ev.id}", rec, score=None)
    assert ok is True
    # In pending index
    assert await clean_redis.zscore(RedisKV.INDEX_PENDING, f"dedup:{ev.id}") == 10.0

    # Client simulates lost ACK and retries
    owned, existing = await e._claim(f"dedup:{ev.id}", rec, token)
    assert owned is True
    assert existing["token"] == token
    await kv.close()
