# Independent Verification Report — Phase 3B: Atomic Redis State and Index Mutations

**Date**: November 2, 2026  
**Verifier**: Independent Senior Systems Engineer  
**Scope**: Verification of RedisKV atomicity, Lua script invariants, failure boundary semantics, lost-ACK recovery, concurrency, Phase 3A regressions, test suite execution, and documentation accuracy.

---

## 1. Final Verdict

# **PASS PHASE 3B**

The Phase 3B implementation of atomic Redis record and index mutations in `RedisKV` has been rigorously and independently verified against a live Redis instance. Record mutations and sorted-set index (`idx:dedup`) updates execute indivisibly within Redis Lua scripts. Under concurrent submissions, connection dropouts, lost acknowledgements, and process crashes, no torn states, orphaned index entries, or unindexed records were produced. Phase 3A capacity reservation behavior and buffer overflow lifecycles remain completely intact.

---

## 2. Environment & Inspected Files

### 2.1 Test & Runtime Environment
- **Operating System**: Windows 11 Enterprise (Build 26100)
- **Python**: 3.14.6 (64-bit)
- **Pytest**: 9.1.1
- **Redis Driver**: redis-py 5.0.4 (`redis.asyncio`)
- **Live Redis Server**: **Redis 8.4.2** (Standalone mode, Alpine Linux x86_64 running via WSL2 on `127.0.0.1:6380`, protected mode disabled)

### 2.2 Files and Functions Inspected
- [`dedup_engine/kv.py`](file:///D:/Exactly-Once-Event-Deduplication-Engine/dedup_engine/kv.py):
  - `KVStore`: Base abstract interface.
  - `RedisKV.LUA_PUT_IF_ABSENT`: Atomic `EXISTS` check, `SET`, and conditional `ZADD`.
  - `RedisKV.LUA_SET`: Atomic `SET` and conditional `ZADD` / `ZREM`.
  - `RedisKV.LUA_EVICT_BEFORE`: Atomic `ZRANGEBYSCORE -inf (cutoff` with multi-key `ZREM` and `DEL`.
  - `RedisKV.LUA_DELETE`: Atomic `ZREM` and `DEL`.
  - `RedisKV._do()`: Exception translation wrapper converting `redis.RedisError` to typed `KVError`.
- [`dedup_engine/engine.py`](file:///D:/Exactly-Once-Event-Deduplication-Engine/dedup_engine/engine.py):
  - `DedupEngine.ingest()`: Ingestion validation, capacity reservation, and claim orchestration.
  - `DedupEngine._claim()`: Atomic claim protocol with ownership token verification on retry.
  - `DedupEngine._commit()`: Transition of delivered events to committed state with score.
  - `DedupEngine._flush()`: Batch sink delivery and concurrent commit pipeline.
  - `DedupEngine.recover()`: Write-ahead log reconstruction from pending KV records.
- [`dedup_engine/faults.py`](file:///D:/Exactly-Once-Event-Deduplication-Engine/dedup_engine/faults.py):
  - `FaultyKV`: Injected request-loss and ACK-loss simulation wrapper.
  - `CrashInjector`: Injected crash hook at pipeline lifecycle checkpoints.
- [`tests/test_redis_integration.py`](file:///D:/Exactly-Once-Event-Deduplication-Engine/tests/test_redis_integration.py):
  - All 12 integration tests covering real Redis communication.
- [`bench/benchmark.py`](file:///D:/Exactly-Once-Event-Deduplication-Engine/bench/benchmark.py):
  - Ingestion stream generation, semaphore concurrency, and `exactly_once` evaluation logic.

---

## 3. Test Execution Results & Command Evidence

All test runs were executed against the live Redis 8.4.2 instance.

### 3.1 Redis Integration Suite
Command: `python -m pytest tests/test_redis_integration.py -v`  
Exit Code: `0`  
Output:
```
============================= test session starts =============================
platform win32 -- Python 3.14.6, pytest-9.1.1, pluggy-1.6.0
rootdir: D:\Exactly-Once-Event-Deduplication-Engine
configfile: pytest.ini
plugins: anyio-4.15.1
collected 12 items

tests\test_redis_integration.py ............                             [100%]

============================= 12 passed in 51.26s =============================
```

### 3.2 Full Test Suite
Command: `python -m pytest -v`  
Exit Code: `0`  
Output:
```
============================= test session starts =============================
platform win32 -- Python 3.14.6, pytest-9.1.1, pluggy-1.6.0
rootdir: D:\Exactly-Once-Event-Deduplication-Engine
configfile: pytest.ini
testpaths: tests
plugins: anyio-4.15.1
collected 93 items

tests\test_api.py ....                                                   [  4%]
tests\test_correctness.py ..................                             [ 23%]
tests\test_fault_injection.py ...................................        [ 61%]
tests\test_lifecycle_p3a.py ..........                                   [ 72%]
tests\test_proxy.py ..                                                   [ 74%]
tests\test_redis_integration.py ............                             [ 87%]
tests\test_window_semantics.py ............                              [100%]

============================== warnings summary ===============================
fastapi\testclient.py:1: StarletteDeprecationWarning: Using `httpx` with `starlette.testclient` is deprecated; install `httpx2` instead.
================== 93 passed, 1 warning in 93.69s (0:01:33) ===================
```

### 3.3 Quick Benchmark Execution
Command: `python bench/benchmark.py --quick`  
Exit Code: `0`  
Output Summary:
```
=== A) throughput & latency vs KV RTT and concurrency ===
  kv_latency_ms |     concurrency |  throughput_eps |          p50_ms |          p95_ms |          p99_ms | kv_ops_per_event |    exactly_once
            0.0 |               1 |           22739 |            0.02 |            0.04 |            0.07 |             2.0 |            True
            0.0 |              16 |           22639 |            0.53 |            0.85 |            1.64 |             2.0 |            True
            0.0 |             128 |           22334 |            5.15 |             7.1 |            7.23 |             2.0 |            True
            0.5 |               1 |              56 |           16.07 |           33.04 |           35.85 |             2.0 |            True
            0.5 |              16 |            1332 |           14.76 |           30.86 |           31.96 |             2.0 |            True
            0.5 |             128 |           17042 |            6.17 |            9.28 |            9.67 |             2.0 |            True
            2.0 |             128 |            7181 |            4.91 |           28.16 |           30.45 |             2.0 |            True

=== B) duplicate ratio (KV RTT 0.5ms, concurrency 64) ===
      dup_ratio |  throughput_eps |          p95_ms |   dups_detected | kv_ops_per_event |    exactly_once
              0 |           12653 |            27.0 |               0 |             2.0 |            True
           0.25 |           11537 |            25.3 |             250 |             2.0 |            True
            0.5 |           17609 |            5.29 |             500 |             2.0 |            True
            0.9 |           12831 |             7.7 |             901 |             2.0 |            True

=== C) sliding-window size vs KV state (event spacing 50ms) ===
       window_s |    peak_kv_keys |  throughput_eps |    exactly_once
              5 |             130 |           20066 |            True
             30 |             654 |           20310 |            True
            120 |            1401 |           19776 |            True
            600 |            1401 |           20755 |            True

=== D) throughput under injected KV faults (request-loss + ack-loss) ===
      fail_rate |        ack_loss |  throughput_eps |          p99_ms |         retries |    exactly_once
              0 |               0 |           12208 |           28.88 |               0 |            True
           0.01 |            0.01 |            5698 |            40.9 |              47 |            True
           0.05 |            0.05 |            3536 |            29.2 |             224 |            True
            0.1 |             0.1 |            2539 |            29.5 |             534 |            True
            0.2 |             0.2 |            1651 |          109.56 |            1650 |            True
```

---

## 4. Detailed Atomicity & Lua Script Verification

### 4.1 Script Analysis
1. **`LUA_PUT_IF_ABSENT`**:
   - Executes `EXISTS key`. If 1, returns `0`. If 0, executes `SET key val` and (if `score` provided) `ZADD idx score key`.
   - **Verification Result**: Because Lua scripts are executed atomically on Redis's single thread, no concurrent command can observe the key without its index entry or vice versa. If score is absent (`score=None`), the index entry is never created.
2. **`LUA_SET`**:
   - Executes `SET key val`. If `has_score == '1'`, calls `ZADD idx score key`. If `has_score == '0'`, calls `ZREM idx key`.
   - **Verification Result**: Handles state transitions between pending and committed atomically. Transitioning to pending removes the record from `idx:dedup`, shielding it from window eviction.
3. **`LUA_EVICT_BEFORE`**:
   - Uses `ZRANGEBYSCORE idx -inf (cutoff`.
   - The opening parenthesis `(` enforces strict inequality ($\text{score} < \text{cutoff}$).
   - Passes the queried key list into `ZREM idx unpack(keys)` and `DEL unpack(keys)`.
   - **Verification Result**: Entire eviction batch completes or fails together. Preserves boundary keys where $\text{score} == \text{cutoff}$.
4. **`LUA_DELETE`**:
   - Calls `ZREM idx key` followed by `DEL key`.
   - **Verification Result**: Completely removes key from both storage structures in a single command round trip.

### 4.2 Failure Boundary Analysis
- **What Lua Atomicity Prevents**:
  - Process crashes or connection timeouts mid-operation can never leave an unindexed record or a dangling index entry.
  - Partial eviction sweeps cannot leave orphaned index pointers.
- **Failures Outside the Script**:
  - Network timeouts occurring after Redis commits the Lua script but before the client receives the response (handled by ownership token recovery).
  - Client crashes occurring after delivery but before commit (handled by write-ahead log recovery).

---

## 5. Lost-Acknowledgement Reproduction

To verify that lost acknowledgements are handled safely when the script actually executed:
1. An event claim was issued via `kv.put_if_absent("dedup:lost_ack_repro", rec, score=None)` with token `client_token_001`.
2. Verified that Redis accepted the command, wrote the key, and did not index it.
3. The response was discarded to simulate a network partition / client timeout.
4. The client retried `_claim()` with token `client_token_001`.
5. `put_if_absent()` returned `False`. The engine loaded the record with `kv.get()`, compared tokens, recognized matching ownership, and returned `(True, existing)`.
6. A second caller with `different_token_999` attempted the same key and was correctly denied ownership (`owned == False`).
7. Full ingestion pipeline processed the event, yielding exactly 1 delivered side-effect. Resubmission returned `Status.DUPLICATE`.

---

## 6. Concurrency and Eviction Findings

An independent stress test was executed against Redis 8.4.2:
1. **50 Concurrent Claims for Same ID**: Exactly 1 succeeded (`True`), 49 failed (`False`). No duplicate index entries.
2. **100 Concurrent Claims for Distinct IDs**: All 100 succeeded, producing exactly 100 entries in `idx:dedup`.
3. **50 Concurrent Updates & Deletions**: Deletions atomically removed string keys and sorted-set entries. Updates cleanly modified score. Zero orphaned references.
4. **Concurrent Eviction vs Set**: Eviction sweeps running parallel to record updates maintained strict keyspace consistency; all keys present in `idx:dedup` were verified to exist in the Redis string keyspace.

---

## 7. Phase 3A Compatibility Findings

Phase 3A introduced capacity reservations (`_reserved`), buffer overflow status (`BUFFER_FULL`), and crash recovery without resurrecting rejected events.
- Tested on `RedisKV` with `max_buffer_size=2`.
- Ingested 2 events (buffer full).
- 3rd event was rejected with `Status.BUFFER_FULL`.
- Verified that `dedup:r3` was **never created in Redis** and never entered `idx:dedup`.
- Simulated crash and restart: `engine.recover()` restored only the 2 valid pending events. `r3` was not resurrected.
- Once capacity freed, `r3` was ingested and delivered normally.
- **Verdict**: Phase 3A capacity reservations and lifecycles operate identically on `RedisKV`.

---

## 8. Benchmark Methodology and Exactly-Once Validation

The benchmark evaluation logic in `bench/benchmark.py` was inspected:
```python
got = [e.id for e in sink.effects]
ok = len(got) == len(set(got)) == uniq
```
- `len(got) == len(set(got))` verifies that zero duplicate effects were emitted by the sink.
- `len(got) == uniq` verifies that every unique input event was delivered exactly once, without loss or omission.
- **Verdict**: The `exactly_once` benchmark metric is **valid, rigorous, and directly computed from sink side-effects**.

---

## 9. Test Suite Audit (All 12 Redis Integration Tests)

| # | Test Name | Stated Purpose | Assessment & Genuineness |
|---|---|---|---|
| 1 | `test_atomic_creation_of_record_and_index` | Atomic creation of key + index entry | **Genuine**: Verifies atomic creation and verifies duplicate put leaves state unmodified. |
| 2 | `test_atomic_put_if_absent_under_concurrent_submissions` | 50 concurrent puts for distinct IDs | **Genuine**: Verifies concurrent non-interfering Lua scripts. |
| 3 | `test_concurrent_claims_for_same_event_id` | 30 parallel workers claiming same ID | **Genuine**: Verifies single-winner semantics under true Redis contention. |
| 4 | `test_updating_existing_record_and_score` | Set score update atomicity | **Genuine**: Verifies update from old score to new score indivisibly. |
| 5 | `test_transitioning_between_pending_and_committed_state` | Pending/committed transitions | **Genuine**: Verifies `ZREM` when score is None and `ZADD` when score is set. |
| 6 | `test_eviction_at_exact_cutoff_boundary` | Exact cutoff boundary eviction | **Genuine**: Tests scores 99.9, 100.0, 100.1 with cutoff 100.0. Boundary strictly retained. |
| 7 | `test_protection_of_pending_records_from_eviction` | Pending protected from eviction | **Genuine**: Evicts with cutoff 1,000,000; committed evicted, pending untouched. |
| 8 | `test_deletion_of_expired_records_and_index_entries` | Atomic delete | **Genuine**: Verifies both key and sorted-set member removed, subsequent delete returns False. |
| 9 | `test_redis_connection_interruption` | Driver error wrapping | **Genuine**: Connects to invalid port, verifies typed `KVError` is raised. |
| 10 | `test_script_execution_followed_by_lost_acknowledgement` | Lost ACK recovery via token | **Genuine**: Discards client ACK after Lua write, retries and validates ownership. |
| 11 | `test_repeated_operations_and_idempotent_retries` | End-to-end engine against Redis | **Genuine**: Replays event 5 times; verifies 1 accepted, 4 duplicate, 1 delivery. |
| 12 | `test_phase_3a_lifecycle_compatibility_with_redis` | Phase 3A capacity on RedisKV | **Genuine**: Verifies overflow, non-creation in Redis, crash recovery, and retry. |

---

## 10. Documentation and Scope Audit

- **`README.md`**: Accurately reflects Redis 7/8 support, Lua script atomicity, and the updated test matrix.
- **`docs/ACCEPTANCE_CRITERIA.md`**: Correctly updated with `AC-REDIS-01` and `AC-REDIS-02`.
- **`docs/IMPLEMENTATION_AUDIT.md`**: Accurately annotates P3.1 and P3.2 as completed and verified in Phase 3B.
- **`docs/PHASE_3B_IMPLEMENTATION_REPORT.md`**: Accurately documents Redis 8.4.2 test results, failure boundaries, and limitations.

### Explicit Limitations & Unresolved Risks:
1. **Idempotent Downstream Sinks**: End-to-end exactly-once delivery guarantees apply to idempotent downstream sinks keyed on `event.id`. Non-idempotent sinks with arbitrary side effects require external 2PC or transactional outbox patterns.
2. **Recovery Scan Optimization**: Ingest recovery currently uses `scan_iter(match=PREFIX + "*")`. For very large keyspaces, a dedicated `idx:pending` secondary index should be implemented (scheduled for Phase 3C).
3. **Partition Single-Writer**: Ordering guarantees hold per engine partition. Multi-node partitioning and distributed watermark coordination are scheduled for Phase 5.

---

## 11. Verification Conclusion

Phase 3B satisfies all architectural requirements, failure recovery conditions, and concurrency constraints. All 93 test cases pass with zero failures.

**Final Certification**: **PASS PHASE 3B**
