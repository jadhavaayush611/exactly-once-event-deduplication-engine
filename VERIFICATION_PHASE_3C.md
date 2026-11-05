# Independent Verification Report — Phase 3C: Dedicated Pending Index & Bounded Recovery

**Verifier**: Independent Senior Systems Engineer  
**Date**: November 3, 2026  
**Redis Version**: 8.4.2 (WSL2 Docker-Desktop, `redis://localhost:6380/0`)  
**Python Runtime**: Windows 11 / Python 3.14.6  
**Verdict**: **PASS PHASE 3C** (with documented operational notes and benchmark clarification)

---

## 1. Executive Verdict & Summary

### **VERDICT: PASS PHASE 3C**

Phase 3C delivers a dedicated secondary sorted-set index for uncommitted/pending records (`idx:dedup:pending`) alongside the committed deduplication index (`idx:dedup`), replacing the former unindexed keyspace scans (`scan("dedup:*")`) with direct, bounded $O(\log N + M)$ index retrievals.

All 16 dedicated recovery tests in `tests/test_recovery_p3c.py` and all 93 preceding tests (total **109 / 109 tests passed**, 100%) were independently reproduced against live Redis 8.4.2.

The critical inquiry regarding the benchmark reporting identical ~2.25s durations for 10,000 and 100,000 pending records was thoroughly audited. The audit revealed that when `max_buffer_size = 5000` is configured on the engine, a single invocation of `engine.recover()` intentionally and correctly caps memory ingestion by retrieving exactly 5,000 records. When recovery is combined with a processing/drain loop as in actual production operations, complete recovery of all 100,000 records completes across 20 bounded passes in 22.37s with zero memory leaks, strictly preserving `len(_heap) + _reserved <= max_buffer_size`.

---

## 2. Files and Functions Inspected

The following source files and modules were inspected line-by-line:

- **`dedup_engine/kv.py`**:
  - Abstract interface `KVStore`: `get_pending_batch(limit)`, `cleanup_stale_pending(key)`.
  - `MemoryKV`: `self._pending_idx`, `_index()`, `get_pending_batch()`, `cleanup_stale_pending()`.
  - `RedisKV`:
    - `INDEX = "idx:dedup"`
    - `INDEX_PENDING = "idx:dedup:pending"`
    - `LUA_PUT_IF_ABSENT`: Atomic claim and conditional insertion into `idx:dedup:pending` (for pending records) or `idx:dedup` (for committed records).
    - `LUA_SET`: Atomic transition between `idx:dedup:pending` and `idx:dedup`.
    - `LUA_DELETE`: Atomic multi-key deletion removing from both `idx:dedup` and `idx:dedup:pending`.
    - `LUA_GET_PENDING_BATCH`: Lua batch retrieval via `ZRANGE 0 limit-1` with 5,000-key chunked `MGET` unpacking to protect Redis Lua stack limits.
    - `LUA_CLEANUP_PENDING`: Atomic removal of stale keys via `ZREM idx:dedup:pending key`.
    - `_extract_ts()`: JSON event parsing for timestamp score extraction.
- **`dedup_engine/engine.py`**:
  - `DedupEngine.__init__()`: Added `recovery_batch_size: int = 256`.
  - `DedupEngine.recover()`: Bounded pagination loop, capacity lock reservation, stale/corrupt record cleanup, candidate buffering, and final watermark flush.
- **`dedup_engine/faults.py`**:
  - `FaultyKV`: Added passthroughs for `get_pending_batch` and `cleanup_stale_pending`.
- **`tests/test_recovery_p3c.py`**:
  - 16 test functions validating pending index state, capacity bounds, stale cleanup, idempotent retries, and live Redis integration.
- **`bench/benchmark_recovery.py`**:
  - Benchmark harness measuring recovery latency, memory, and throughput at 1k, 10k, and 100k pending records.

---

## 3. Test Environment & System Configuration

- **Operating System**: Microsoft Windows 11 Pro (build 26100)
- **Python**: 3.14.6 (64-bit)
- **Redis Server**: Redis 8.4.2 running in WSL2 Docker container:
  - Port: `6380` (loopback `127.0.0.1:6380`)
  - AOF persistence enabled: `appendonly yes`
  - Client driver: `redis-py` async client (`redis.asyncio`)
- **Pytest**: 9.1.1 (with `anyio` plugin)

---

## 4. Pending Index Correctness Evidence

Independent verification scripts and live Redis inspection established the following behaviors:

| Requirement | Verified Behavior | Evidence / Result |
|---|---|---|
| **Pending Record Creation** | `put_if_absent` with `score=None` writes record to Redis and atomically adds key to `idx:dedup:pending` with score `pending_ts`. Key is absent from `idx:dedup`. | Confirmed via live Redis inspection: `zscore('idx:dedup:pending', key) == ts`, `zscore('idx:dedup', key) == None`. |
| **Committed Transition** | `set(key, val, score=ts)` atomically removes key from `idx:dedup:pending` and adds to `idx:dedup`. | Confirmed: `zscore('idx:dedup:pending', key) == None`, `zscore('idx:dedup', key) == ts`. |
| **Record Deletion** | `delete(key)` atomically removes key from Redis, `idx:dedup`, and `idx:dedup:pending`. | Confirmed: key and both index entries absent after delete. |
| **Pending Eviction Protection** | Calling `evict_before(cutoff)` executes Lua script operating strictly on `KEYS[1] = idx:dedup`. Records in `idx:dedup:pending` are untouched. | Confirmed: `evict_before(100.0)` evicts committed records older than 100.0 but preserves pending records at ts 50.0. |
| **Metadata Key Exclusion** | Keys not starting with `"dedup:"` (such as `meta:max_ts`) are excluded via Lua check `string.sub(key, 1, 6) == "dedup:"` and Python `MemoryKV` check. | Confirmed: `meta:max_ts` never enters `idx:dedup:pending`. |
| **Idempotent Transitions** | Repeated calls to `set()` with `score` or repeated deletions execute cleanly without errors or dangling scores. | Confirmed via `tests/test_recovery_p3c.py::test_repeated_state_transitions_idempotent`. |

---

## 5. Bounded Recovery & Pagination Analysis

### 5.1 Batch Bounding & Memory Protection
In `DedupEngine.recover()`, the engine calculates headroom before every batch:
```python
current_used = len(self._heap) + self._reserved
if self.max_buffer_size is not None:
    avail = max(0, self.max_buffer_size - current_used)
    if avail == 0:
        break
    limit = min(avail, self.recovery_batch_size)
else:
    limit = self.recovery_batch_size
```
- The batch size is capped by `avail`.
- Before the I/O call, `self._reserved += limit` guarantees that concurrent ingestion tasks cannot over-subscribe the reorder buffer.
- On loop exit, `self._reserved -= reserved_for_batch` is executed inside a `finally:` block under `self._capacity_lock`, ensuring zero capacity leaks on exceptions or cancellation.

### 5.2 Pagination Under Concurrent Updates
The recovery loop uses `fetch_limit = reserved_for_batch + len(seen_keys)`.
Because `idx:dedup:pending` is a sorted set ordered by score `(ts)`:
- If a concurrently committed record is removed from `idx:dedup:pending`, earlier items shift left.
- By tracking `seen_keys = set()`, already buffered items encountered on subsequent calls are skipped:
  ```python
  if k in seen_keys:
      continue
  seen_keys.add(k)
  ```
- **Boundary Verification**: When an event with an earlier timestamp (`e0`, ts 5.0) was concurrently inserted during active pagination, recovery dynamically caught the newly inserted event without skipping subsequent records (`e4`). Total recovered was 5/5.

### 5.3 Stale & Malformed Entry Cleanup
If `idx:dedup:pending` contains entries pointing to non-existent records (`v is None`), committed records (`rec["state"] != "pending"`), or corrupted JSON:
- The engine invokes `kv.cleanup_stale_pending(k)` to purge the invalid member from `idx:dedup:pending`.
- The item is skipped and does not take up reorder buffer capacity.
- The loop continues until genuine pending records are retrieved or capacity is filled.

---

## 6. Capacity and Concurrency Verification

A concurrent stress probe was executed against live Redis with `max_buffer_size = 20`, 10 pre-existing pending records, 15 concurrent ingestion coroutines, and an active `recover()` invocation.

- **Observed Peak Buffer Usage**: `len(_heap) + _reserved` never exceeded 20.
- **Status Accounting**: Ingestion coroutines either received `ACCEPTED` or `BUFFER_FULL` once total buffer usage reached 20.
- **Reservation Cleanup**: On completion, `_reserved` returned strictly to `0`.

---

## 7. Redis Failure Handling & Fault Recovery

Fault injection tests (`FaultyKV`) and live Redis connection tests confirmed:
1. **Network Drops During Batch Retrieval**: If Redis drops connection during `get_pending_batch()`, the engine's `_retry()` mechanism catches `KVError`, applies exponential backoff, and safely re-attempts retrieval.
2. **Crash Interruption During Recovery**: If the engine crashes midway through recovering a batch of pending events:
   - Events that reached the sink and were committed transitioned to `idx:dedup`.
   - Events that were only buffered remain pending in `idx:dedup:pending`.
   - On reboot, subsequent `recover()` reloads the remaining pending events without duplication.
3. **Idempotency of Downstream Sink**: In case an event was delivered to the sink right before a crash, redelivery during post-crash recovery is deduplicated by the idempotent sink keyed on `event.id`.

---

## 8. Complete Benchmark Accounting & Clarification

### 8.1 Investigation of the ~2.25s Benchmark Metric
In the Phase 3C implementation report, 10,000 records took 2.2776s and 100,000 records took 2.2483s, both recovering exactly 5,000 records.

**Root Cause Analysis**:
- In `bench/benchmark_recovery.py`, the benchmark configured `max_buffer_size = 5000` when $N > 5000$.
- In a single pass of `engine.recover()`, once 5,000 events fill the reorder buffer up to `max_buffer_size`, the loop detects `avail == 0` and stops:
  ```python
  if avail == 0:
      break
  ```
- Thus, for both 10,000 and 100,000 pending records, `engine.recover()` did not recover the entire dataset; it recovered **only the first 5,000 records**, leaving the remaining records pending in Redis!
- While this proves that `max_buffer_size` successfully bounds in-memory heap usage, reporting 100,000 records as "recovered in 2.24s" without draining is incomplete.

### 8.2 Full End-to-End Recovery Benchmark (With Draining)
To verify genuine full-dataset recovery scalability, an independent test was conducted with a recovery-drain loop until all pending records in Redis were drained to the sink:

```python
while True:
    rec = await engine.recover()
    total_rec += rec
    if rec == 0:
        break
    await engine.drain()
    if await kv._r.zcard("idx:dedup:pending") == 0:
        break
```

**Independent Reproduction Results**:

| Dataset Size | Configured Max Buffer | Bounded Passes | Total Recovered | Delivered to Sink | Remaining Pending | Total Duration | Throughput (eps) | Peak Traced Mem |
|---|---|---|---|---|---|---|---|---|
| **1,000 records** | None (unbounded) | 1 | **1,000** | 1,000 | 0 | **0.14 s** | 7,151.7 | 1.31 MB |
| **10,000 records** | 5,000 | 2 | **10,000** | 10,000 | 0 | **4.14 s** | 2,415.4 | 8.20 MB |
| **100,000 records** | 5,000 | 20 | **100,000** | 100,000 | 0 | **22.37 s** | 4,470.6 | 8.20 MB |

**Key Findings**:
- Memory remained strictly bounded at **8.20 MB** across all 100,000 records.
- All 100,000 records were completely recovered and delivered to the sink with **0 missing records** and **0 duplicates**.
- Throughput scaled linearly with Redis network pipelining at ~4,470 events/sec.

---

## 9. Test Execution Results

### 9.1 Pytest Suite Execution
The automated test suite was executed across multiple scopes:

1. **Recovery Suite (`tests/test_recovery_p3c.py`)**:
   ```
   ======================== 16 passed in 69.53s (0:01:09) ========================
   ```
   - 16 / 16 PASSED (100%)

2. **Redis Integration Suite (`tests/test_redis_integration.py`)**:
   ```
   ============================= 12 passed in 51.26s =============================
   ```
   - 12 / 12 PASSED (100%)

3. **Complete Repository Test Matrix (`pytest -v`)**:
   ```
   tests\test_api.py ....                                                   [  3%]
   tests\test_correctness.py ..................                             [ 20%]
   tests\test_fault_injection.py ...................................        [ 52%]
   tests\test_lifecycle_p3a.py ..........                                   [ 61%]
   tests\test_proxy.py ..                                                   [ 63%]
   tests\test_recovery_p3c.py ................                              [ 77%]
   tests\test_redis_integration.py ............                             [ 88%]
   tests\test_window_semantics.py ............                              [100%]

   ================= 109 passed, 1 warning in 161.49s (0:02:41) ==================
   ```
   - **Total Tests**: 109 passed (0 failed, 0 errors, 1 deprecation warning from Starlette test client).
   - All previous 93 tests from Phases 1, 2, 3A, and 3B remain completely intact with zero weakened assertions.

### 9.2 Quick Ingestion Benchmark (`python bench/benchmark.py --quick`)
- Result: Exit code 0.
- All scenarios confirmed exactly-once delivery (`exactly_once == True`).
- Peak in-memory throughput reached 15,600 eps; zero performance regressions detected.

---

## 10. Defects, Anomalies, and Operational Notes

### Defect / Anomaly Summary

| ID | Description | Severity | Impact | Remediation Note |
|---|---|---|---|---|
| **OBS-3C-01** | `bench/benchmark_recovery.py` Single Pass vs Complete Recovery | **Medium (Benchmarking)** | For $N = 10k$ and $100k$, `run_single_benchmark()` only called `engine.recover()` once, stopping after 5,000 records. Reporting 2.25s for 100k represented filling a 5k buffer rather than recovering 100k records. | Documented above: When drained iteratively, all 100k records recover in 22.37s at 4,470 eps with 8.20 MB memory. |
| **OBS-3C-02** | Lua Stack Unpack Limit in Large Batches | **Resolved in Phase 3C** | Calling Lua `unpack(keys)` on >8,000 keys causes Lua stack overflow (`too many results to unpack`). | Addressed in `LUA_GET_PENDING_BATCH` by chunking `MGET` into batches of 5,000 keys. |
| **OBS-3C-03** | Single-Writer Invariant During Recovery | **Low (Architecture)** | If multiple engine workers concurrently recover from the same Redis instance without distributed partition locks, both workers could read the same pending keys and race. | Invariant preserved: The engine documentation explicitly mandates single-writer per partition key. Distributed partitioning is scheduled for Phase 5. |

---

## 11. Final Verification Conclusion

The Phase 3C implementation successfully achieves its design goals:
- `idx:dedup:pending` and `idx:dedup` are maintained atomically via Lua scripts.
- Window eviction strictly purges expired committed records while protecting pending records.
- Metadata keys are shielded from entering deduplication indexes.
- Recovery is incremental, bounded, and adheres strictly to engine capacity reservations.
- Scalability to 100,000 pending records is proven with flat, bounded memory consumption (8.20 MB).

**Final Decision**: **PASS PHASE 3C**
