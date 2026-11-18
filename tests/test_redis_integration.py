"""Integration tests for RedisKV against a real, isolated Redis 7 instance.
Covers atomic mutations, concurrent claims, window boundaries, pending protection,
connection failures, lost ACKs, and Phase 3A lifecycle compatibility.
"""
import asyncio
import json
import pytest
import redis.asyncio as redis

from dedup_engine import DedupEngine, Event, Status, KVError, MemorySink
from dedup_engine.kv import RedisKV

REDIS_URL = "redis://localhost:6380/0"
_redis_available: bool | None = None


@pytest.fixture(autouse=True)
async def clean_redis():
    """Ensure an isolated, clean Redis keyspace for every test."""
    global _redis_available
    if _redis_available is None:
        try:
            probe = redis.from_url(
                REDIS_URL, decode_responses=True, socket_connect_timeout=0.2, retry=None
            )
            await probe.ping()
            await probe.aclose()
            _redis_available = True
        except Exception as e:
            _redis_available = False
            pytest.skip(f"Live Redis 7 instance on {REDIS_URL} is unreachable: {e}")
    elif not _redis_available:
        pytest.skip(f"Live Redis 7 instance on {REDIS_URL} is unreachable")

    r = redis.from_url(REDIS_URL, decode_responses=True)
    await r.flushdb()
    yield r
    await r.flushdb()
    await r.aclose()


@pytest.mark.anyio
async def test_atomic_creation_of_record_and_index(clean_redis):
    """1. Creating a record and updating idx:dedup occurs atomically."""
    r = clean_redis
    kv = RedisKV(REDIS_URL)

    # Put with score (e.g. committed record)
    ok = await kv.put_if_absent("dedup:e1", '{"state":"committed"}', score=100.5)
    assert ok is True
    assert await r.get("dedup:e1") == '{"state":"committed"}'
    assert await r.zscore(RedisKV.INDEX, "dedup:e1") == 100.5

    # Second put must fail atomically without modifying value or index
    ok2 = await kv.put_if_absent("dedup:e1", '{"state":"overwrite_attempt"}', score=200.0)
    assert ok2 is False
    assert await r.get("dedup:e1") == '{"state":"committed"}'
    assert await r.zscore(RedisKV.INDEX, "dedup:e1") == 100.5
    await kv.close()


@pytest.mark.anyio
async def test_atomic_put_if_absent_under_concurrent_submissions(clean_redis):
    """2. Concurrent put_if_absent calls for distinct IDs all succeed atomically."""
    kv = RedisKV(REDIS_URL)

    async def worker(i):
        return await kv.put_if_absent(f"dedup:conc_{i}", f'{{"n":{i}}}', score=float(i))

    results = await asyncio.gather(*[worker(i) for i in range(50)])
    assert all(results)
    assert await kv.size() == 51  # 50 records + 1 sorted set index (idx:dedup)
    await kv.close()


@pytest.mark.anyio
async def test_concurrent_claims_for_same_event_id(clean_redis):
    """3. Concurrent claims for the same event ID: exactly 1 winner, N-1 losers."""
    kv = RedisKV(REDIS_URL)
    eid = "race_event_1"

    async def claim_coro(worker_id):
        rec = json.dumps({"state": "pending", "token": f"tok_{worker_id}", "ts": 50.0})
        return await kv.put_if_absent(f"dedup:{eid}", rec, score=None)

    results = await asyncio.gather(*[claim_coro(i) for i in range(30)])
    assert results.count(True) == 1
    assert results.count(False) == 29
    # Pending record must not have an entry in idx:dedup
    assert await clean_redis.zscore(RedisKV.INDEX, f"dedup:{eid}") is None
    await kv.close()


@pytest.mark.anyio
async def test_updating_existing_record_and_score(clean_redis):
    """4. Updating an existing record and its score via set() is atomic."""
    kv = RedisKV(REDIS_URL)
    key = "dedup:score_update"

    await kv.set(key, '{"v":1}', score=10.0)
    assert await clean_redis.get(key) == '{"v":1}'
    assert await clean_redis.zscore(RedisKV.INDEX, key) == 10.0

    # Update to new value and new score
    await kv.set(key, '{"v":2}', score=25.5)
    assert await clean_redis.get(key) == '{"v":2}'
    assert await clean_redis.zscore(RedisKV.INDEX, key) == 25.5
    await kv.close()


@pytest.mark.anyio
async def test_transitioning_between_pending_and_committed_state(clean_redis):
    """5. Transitioning record between pending (score=None) and committed (score=ts)."""
    kv = RedisKV(REDIS_URL)
    key = "dedup:transition_ev"

    # Start pending: score=None
    await kv.put_if_absent(key, '{"state":"pending"}', score=None)
    assert await clean_redis.get(key) == '{"state":"pending"}'
    assert await clean_redis.zscore(RedisKV.INDEX, key) == None

    # Transition to committed: score=15.0
    await kv.set(key, '{"state":"committed"}', score=15.0)
    assert await clean_redis.get(key) == '{"state":"committed"}'
    assert await clean_redis.zscore(RedisKV.INDEX, key) == 15.0

    # Transition back to pending (e.g. uncommit/revert): removes from index
    await kv.set(key, '{"state":"reverted_pending"}', score=None)
    assert await clean_redis.get(key) == '{"state":"reverted_pending"}'
    assert await clean_redis.zscore(RedisKV.INDEX, key) is None
    await kv.close()


@pytest.mark.anyio
async def test_eviction_at_exact_cutoff_boundary(clean_redis):
    """6. Eviction drops score < cutoff; score == cutoff is preserved."""
    kv = RedisKV(REDIS_URL)

    # Insert keys with scores around boundary 100.0:
    # 99.9 (< 100), 100.0 (== 100), 100.1 (> 100)
    await kv.set("dedup:t99", '{"v":99}', score=99.9)
    await kv.set("dedup:t100", '{"v":100}', score=100.0)
    await kv.set("dedup:t101", '{"v":101}', score=100.1)

    # Cutoff at 100.0 (boundary check: < 100.0 evicted)
    evicted = await kv.evict_before(100.0)
    assert evicted == 1

    assert await clean_redis.get("dedup:t99") is None
    assert await clean_redis.zscore(RedisKV.INDEX, "dedup:t99") is None

    assert await clean_redis.get("dedup:t100") is not None
    assert await clean_redis.zscore(RedisKV.INDEX, "dedup:t100") == 100.0

    assert await clean_redis.get("dedup:t101") is not None
    assert await clean_redis.zscore(RedisKV.INDEX, "dedup:t101") == 100.1
    await kv.close()


@pytest.mark.anyio
async def test_protection_of_pending_records_from_eviction(clean_redis):
    """7. Pending records (score=None) are never evicted regardless of age."""
    kv = RedisKV(REDIS_URL)

    await kv.put_if_absent("dedup:p1", '{"state":"pending","ts":1.0}', score=None)
    await kv.set("dedup:c1", '{"state":"committed","ts":1.0}', score=1.0)

    # Large eviction cutoff: 1,000,000
    evicted = await kv.evict_before(1000000.0)
    assert evicted == 1

    assert await clean_redis.get("dedup:c1") is None
    assert await clean_redis.get("dedup:p1") is not None
    assert await clean_redis.zscore(RedisKV.INDEX, "dedup:p1") is None
    await kv.close()


@pytest.mark.anyio
async def test_deletion_of_expired_records_and_index_entries(clean_redis):
    """8. Atomic delete() removes both key and index entry."""
    kv = RedisKV(REDIS_URL)
    key = "dedup:to_delete"

    await kv.set(key, '{"v":"del"}', score=50.0)
    assert await clean_redis.get(key) is not None
    assert await clean_redis.zscore(RedisKV.INDEX, key) == 50.0

    deleted = await kv.delete(key)
    assert deleted is True
    assert await clean_redis.get(key) is None
    assert await clean_redis.zscore(RedisKV.INDEX, key) is None

    # Deleting again returns False
    assert await kv.delete(key) is False
    await kv.close()


@pytest.mark.anyio
async def test_redis_connection_interruption(clean_redis):
    """9. KVError raised cleanly on bad host/port connection without leaking unhandled exceptions."""
    bad_kv = RedisKV("redis://127.0.0.1:65534/0")
    with pytest.raises(KVError) as exc_info:
        await bad_kv.put_if_absent("k", "v")
    assert "Error" in str(exc_info.value) or "refused" in str(exc_info.value).lower()
    await bad_kv.close()


@pytest.mark.anyio
async def test_script_execution_followed_by_lost_acknowledgement(clean_redis):
    """10. Script executes in Redis but client ACK is lost: retry recognizes token safely."""
    kv = RedisKV(REDIS_URL)
    sink = MemorySink()
    e = DedupEngine(kv, sink, window=100.0, lateness=10.0)

    token = "fixed_token_abc123"
    ev = Event("lost_ack_redis_event", 10.0, {"val": 42})
    rec = json.dumps({"state": "pending", "ts": 10.0, "token": token, "event": ev.to_dict()})

    # 1. First execution writes to Redis
    ok = await kv.put_if_absent(f"dedup:{ev.id}", rec, score=None)
    assert ok is True

    # 2. Client simulates lost response and retries _claim with same token
    owned, existing = await e._claim(f"dedup:{ev.id}", rec, token)
    assert owned is True
    assert existing["token"] == token
    assert existing["state"] == "pending"

    # 3. Third-party with DIFFERENT token gets owned=False
    owned_other, existing_other = await e._claim(f"dedup:{ev.id}", rec, "different_token_xyz")
    assert owned_other is False
    assert existing_other["token"] == token
    await kv.close()


@pytest.mark.anyio
async def test_repeated_operations_and_idempotent_retries(clean_redis):
    """11. Engine end-to-end against live RedisKV handles duplicate submissions idempotently."""
    kv = RedisKV(REDIS_URL)
    sink = MemorySink()
    e = DedupEngine(kv, sink, window=100.0, lateness=2.0)

    # Submit e1 5 times
    statuses = [await e.ingest(Event("e1", 10.0)) for _ in range(5)]
    assert statuses == [Status.ACCEPTED, Status.DUPLICATE, Status.DUPLICATE, Status.DUPLICATE, Status.DUPLICATE]
    await e.drain()

    assert [ev.id for ev in sink.effects] == ["e1"]
    # Check that in Redis it is committed and indexed
    assert await clean_redis.zscore(RedisKV.INDEX, "dedup:e1") == 10.0
    await kv.close()


@pytest.mark.anyio
async def test_phase_3a_lifecycle_compatibility_with_redis(clean_redis):
    """12. Phase 3A capacity reservations and buffer overflow lifecycle work identically on RedisKV."""
    kv = RedisKV(REDIS_URL)
    sink = MemorySink()
    e = DedupEngine(kv, sink, max_buffer_size=2, lateness=10.0, window=100.0)

    # Fill buffer to capacity = 2
    assert await e.ingest(Event("r1", 10.0)) == Status.ACCEPTED
    assert await e.ingest(Event("r2", 10.0)) == Status.ACCEPTED
    assert len(e._heap) == 2

    # 3rd event exceeds buffer -> BUFFER_FULL without creating Redis key
    st_overflow = await e.ingest(Event("r3", 10.0))
    assert st_overflow == Status.BUFFER_FULL
    assert await clean_redis.get("dedup:r3") is None
    assert await clean_redis.zscore(RedisKV.INDEX, "dedup:r3") is None

    # Restart engine instance on same Redis store
    sink2 = MemorySink()
    e_restarted = DedupEngine(kv, sink2, max_buffer_size=2, lateness=10.0, window=100.0)
    recovered = await e_restarted.recover()
    assert recovered == 2
    await e_restarted.drain()

    # r3 must NOT be resurrected
    effects = [ev.id for ev in sink2.effects]
    assert "r3" not in effects
    assert sorted(effects) == ["r1", "r2"]

    # Now retry r3 on free capacity
    assert await e_restarted.ingest(Event("r3", 10.0)) == Status.ACCEPTED
    assert await e_restarted.ingest(Event("r3", 10.0)) == Status.DUPLICATE
    await e_restarted.drain()
    assert [ev.id for ev in sink2.effects].count("r3") == 1
    await kv.close()
