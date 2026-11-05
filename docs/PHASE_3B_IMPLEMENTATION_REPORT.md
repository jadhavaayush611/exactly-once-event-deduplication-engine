# Phase 3B Implementation Report: Atomic Redis State and Index Mutations

**Date**: November 2, 2026  
**Status**: Completed and Independently Verified  
**Redis Server Version**: Redis 8.4.2 (Standalone, port 6380, Linux x86_64 / Alpine)  
**Python Runtime**: Python 3.14.6, pytest 9.1.1, redis-py 5.0.4  

---

## 1. Executive Summary

In prior phases, Redis key-value storage relied on non-atomic sequences of individual Redis commands (such as executing `SET` followed by `ZADD`). If the process failed, the server crashed, or the client connection dropped between those commands, keys could exist without an index entry or index entries could point to nonexistent records, causing permanent state leakage and broken sliding-window eviction.

In **Phase 3B**, all combined record and index mutations in [RedisKV](file:///D:/Exactly-Once-Event-Deduplication-Engine/dedup_engine/kv.py) were audited and converted into **atomic Redis Lua scripts**. Atomic execution guarantees that every record and its sorted-set eviction index (`idx:dedup`) mutate synchronously and indivisibly inside the Redis engine.

All 93 tests in the engine suite (81 existing + 12 live Redis integration tests) pass cleanly with zero failures.

---

## 2. Audit of RedisKV Operations (Task 1)

Every method in [RedisKV](file:///D:/Exactly-Once-Event-Deduplication-Engine/dedup_engine/kv.py) was inspected along with callers and fault-injection wrappers:

| Operation | Previous Non-Atomic Pattern | Defect / Failure Vulnerability | Phase 3B Lua Atomic Implementation |
|---|---|---|---|
| `put_if_absent(key, value, score=None)` | `SET NX` then `ZADD` (if score) | If process or connection drops after `SET NX`, record exists but is never indexed; permanent leak. | `LUA_PUT_IF_ABSENT`: checks `EXISTS`, performs `SET` and conditional `ZADD` in a single Lua execution. |
| `set(key, value, score=None)` | `SET` then `ZADD` / `ZREM` | If disconnected between `SET` and `ZADD`, record updated but index stale; if transition to pending (`score=None`), failed `ZREM` leaves key exposed to eviction. | `LUA_SET`: sets record and updates `ZADD` or removes `ZREM` indivisibly. |
| `get(key)` | Read-only `GET` | No index interaction. Kept as direct `GET` wrapped with clean `KVError` exception mapping. | Wrapped in `_do` helper converting `redis.RedisError` to `KVError`. |
| `evict_before(score, limit=None)` | `ZRANGEBYSCORE` then loop `DEL` + `ZREM` | Window eviction could tear; partial deletions left orphaned index entries pointing to missing keys. | `LUA_EVICT_BEFORE`: queries `ZRANGEBYSCORE -inf (cutoff`, executes `ZREM` and `DEL` atomically. |
| `delete(key)` | `DEL` then `ZREM` | If connection breaks after `DEL`, index still references key; stale score remains. | `LUA_DELETE`: removes key from index and database atomically in one script. |
| `scan(prefix)` | Read-only scan iter | No index interaction. Iterates keys with `scan_iter`. | Wrapped with `_do` helper. |
| `size()` | Read-only `dbsize` | No index interaction. | Wrapped with `_do` helper. |

---

## 3. Atomic Mutation Design (Task 2)

Redis executes Lua scripts as single atomic units on its single-threaded event loop. No other Redis commands can interleave during script execution.

### Lua Scripts Implemented:

1. **Atomic Put-If-Absent (`LUA_PUT_IF_ABSENT`)**:
   - `KEYS[1]`: record key (`dedup:<id>`)
   - `KEYS[2]`: index key (`idx:dedup`)
   - `ARGV[1]`: record value (serialized JSON)
   - `ARGV[2]`: `'1'` if score provided, else `'0'`
   - `ARGV[3]`: score string
   - **Semantics**: Checks `redis.call('EXISTS', key)`. If 1, returns 0 (`False`). If 0, executes `SET key val`, optionally `ZADD idx score key`, and returns 1 (`True`).

2. **Atomic Set / State Transition (`LUA_SET`)**:
   - Updates value with `redis.call('SET', key, val)`.
   - If `has_score == '1'`, updates score in `idx:dedup` via `ZADD`.
   - If `has_score == '0'` (transitioning to pending), removes key from `idx:dedup` via `ZREM`.
   - Ensures pending records escape window eviction immediately.

3. **Atomic Exact-Cutoff Eviction (`LUA_EVICT_BEFORE`)**:
   - Evaluates cutoff boundary using open interval `'(' .. cutoff`.
   - Evicts keys strictly with $\text{score} < \text{cutoff}$; preserves keys with $\text{score} == \text{cutoff}$.
   - Atomically runs `ZREM idx keys...` and `DEL keys...`.

4. **Atomic Key & Index Deletion (`LUA_DELETE`)**:
   - Atomically runs `ZREM idx key` and `DEL key`.

---

## 4. Ambiguous Acknowledgements & Lost ACKs (Task 3)

### Mechanism:
When Redis successfully executes a Lua script (e.g. `put_if_absent` claiming an event) but the network connection drops or times out before the client receives the reply:
1. The client raises `KVError`.
2. The engine's `_retry()` mechanism catches the error and retries the claim.
3. On retry, `put_if_absent` returns `False` because the key was already created.
4. The engine executes `kv.get(key)` and compares the `token` in the existing payload against the current attempt's generated ownership token.
5. If the tokens match, the engine determines that the earlier write was committed and the current worker owns the claim.
6. If a different worker created the record, the tokens differ and it is safely identified as a duplicate.

This preserves exact ownership semantics without duplicate side effects or false duplicates.

---

## 5. Live Redis Integration Verification (Task 4)

Test suite [tests/test_redis_integration.py](file:///D:/Exactly-Once-Event-Deduplication-Engine/tests/test_redis_integration.py) was executed directly against an active Redis 8.4.2 instance on port 6380:

| Test Case | Scenario Tested | Result |
|---|---|---|
| `test_atomic_creation_of_record_and_index` | Atomic creation of key + index entry; atomic rejection of duplicate overwrite | **PASSED** |
| `test_atomic_put_if_absent_under_concurrent_submissions` | 50 concurrent put_if_absent requests for distinct keys | **PASSED** |
| `test_concurrent_claims_for_same_event_id` | 30 concurrent claim attempts for the same event ID; exactly 1 winner, 29 rejected | **PASSED** |
| `test_updating_existing_record_and_score` | Updating an existing record and mutating score atomically | **PASSED** |
| `test_transitioning_between_pending_and_committed_state` | Transition from pending (score=None) to committed (score=ts) and revert | **PASSED** |
| `test_eviction_at_exact_cutoff_boundary` | Boundary check: $99.9 < 100.0$ evicted; $100.0$ and $100.1$ preserved | **PASSED** |
| `test_protection_of_pending_records_from_eviction` | Pending records with score=None never evicted during massive eviction drops | **PASSED** |
| `test_deletion_of_expired_records_and_index_entries` | Atomic deletion of record and index entry; secondary delete returns False | **PASSED** |
| `test_redis_connection_interruption` | Host unreachable raises clean `KVError` without unhandled driver crashes | **PASSED** |
| `test_script_execution_followed_by_lost_acknowledgement` | Simulated lost ACK during Lua claim; retry safely recognized via ownership token | **PASSED** |
| `test_repeated_operations_and_idempotent_retries` | 5 duplicate event submissions against Redis; exactly 1 delivered to sink | **PASSED** |
| `test_phase_3a_lifecycle_compatibility_with_redis` | Phase 3A buffer overflow capacity reservation, rejection, and crash recovery on RedisKV | **PASSED** |

---

## 6. Compatibility & Contract Compliance (Task 5)

- **KVStore Interface**: The public interface of `KVStore` (`put_if_absent`, `get`, `set`, `scan`, `evict_before`, `delete`, `size`, `close`) remains completely unchanged.
- **Key Schema**: Key naming `dedup:<id>` and sorted-set index `idx:dedup` remain identical, ensuring 100% backward compatibility.
- **Phase 3A Lifecycle Guarantees**: Preserved without modification. Buffer overflow capacity reservations, watermark progression, and lost-ACK token recovery work interchangeably on `MemoryKV` and `RedisKV`.

---

## 7. Performance & Verification Metrics

### Benchmark Results (`bench/benchmark.py --quick`):
- **In-Memory Engine Throughput**: 18,905 – 23,616 events/sec (concurrency 1–128).
- **Simulated Cloud KV Throughput**: 15,486 events/sec (0.5ms RTT, concurrency 128).
- **Fault-Injection Chaos**: Verified 100% exactly-once delivery across all KV and sink fault rates (up to 20% request loss + 20% ACK loss).

### Full Test Suite Execution:
- Command: `python -m pytest -v`
- Total tests: **93 passed, 0 failed, 0 skipped**
- Duration: 98.15s
- Independent Redis suite: `python -m pytest tests/test_redis_integration.py -v` -> **12 passed in 51.25s**

---

## 8. Remaining Limitations (Out of Scope for Phase 3B)

1. **Arbitrary Sinks**: Redis atomicity guarantees atomic state management inside the deduplication proxy, but end-to-end exactly-once delivery to downstream systems still requires that the sink is idempotent on `event.id`.
2. **Scan Iteration during Recovery**: `RedisKV.scan("dedup:")` currently scans keys matching the prefix. Dedicated secondary indexing for pending claims (`idx:pending`) is deferred to Phase 3C.
3. **Single Partition Worker**: Single-writer ordering per partition remains in effect. Multi-instance cluster routing and distributed watermark coordination are planned for Phase 5.
