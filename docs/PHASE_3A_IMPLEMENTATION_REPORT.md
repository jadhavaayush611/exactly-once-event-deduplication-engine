# Phase 3A Implementation Report: Critical Event Lifecycle Repair

**Implementation Date**: October 30, 2026  
**Auditor & Implementer**: Antigravity (Google DeepMind)  
**Target Repository**: `D:\Exactly-Once-Event-Deduplication-Engine`  
**Execution Environment**: Windows 11 (win32) / Python 3.14.6 / pytest 9.1.1 / anyio 4.15.1  

---

## 1. Executive Summary

In Milestone Phase 3A, we identified and repaired the critical event lifecycle defect (**DEFECT-P2-01**) where `DedupEngine.ingest()` previously created write-ahead KV claims before verifying reorder buffer capacity headroom. 

With this repair:
- A clear, formal **Event Acceptance Contract** is defined and enforced.
- A **reorder-buffer capacity reservation mechanism** (`_reserved` counter under `_capacity_lock`) was introduced.
- Rejected `BUFFER_FULL` events **never** receive a write-ahead claim in KV, eliminating stranded claims.
- Client retries after capacity is freed return `Status.ACCEPTED` without false `DUPLICATE` status.
- Engine restarts and `recover()` **never** resurrect or redeliver events that returned `BUFFER_FULL`.
- All capacity reservations are guaranteed to be released on duplicate detection, validation failure, KV claim failure, unhandled exceptions, and asyncio cancellations.
- **10 new deterministic regression tests** were added in `tests/test_lifecycle_p3a.py`.
- The full test suite expanded from 71 to **81 tests**, passing with 100% success rate (0 failures).

---

## 2. Root Cause Analysis (DEFECT-P2-01)

### The Pre-Fix Vulnerability
In the previous implementation of `DedupEngine.ingest()`:
1. When an event arrived, the engine immediately called `self._claim(PREFIX + event.id, rec, token)`, creating a `pending` write-ahead log record in the KV store.
2. Only *after* the claim succeeded did the engine check `len(self._heap) >= self.max_buffer_size`.
3. If the buffer was full and could not drain (e.g. events held behind watermark lateness), the engine incremented `m.buffer_overflow` and returned `Status.BUFFER_FULL`.
4. However, the `pending` claim remained persisted in the KV store without rollback.

### Impact
- **False Duplicate on Retry**: When capacity was subsequently freed, a client retrying the rejected event encountered its own prior claim. Because it had not been buffered into `_live`, it was treated as an orphan adoption and returned `Status.DUPLICATE` rather than `Status.ACCEPTED`.
- **Unexpected Resurrection on Crash Recovery**: If the process crashed before any client retry, `DedupEngine.recover()` scanned all `pending` KV records, restored the rejected event into the reorder buffer, and delivered it to the sink, violating the guarantee given to the producer that the event was rejected.

---

## 3. Formal Event Acceptance Contract

An event transitions through distinct lifecycle phases governed by the following contract:

| Ingestion Status / State | Definition & Guarantees | KV Store State | Downstream Sink Effect |
|---|---|---|---|
| **`Status.BUFFER_FULL`** | The event was **not accepted** due to exhausted buffer capacity. Capacity was evaluated prior to KV claim. | **No claim created** (`None`). If KV already had a record from an earlier accepted run, that record remains untouched. | Zero effects. Safe for client to backoff and retry later. |
| **`Status.ACCEPTED`** | The event was **durably claimed** and accepted into the engine's processing lifecycle. Space in the reorder buffer is reserved. | Key is durably set to `pending` with the event payload and a unique ownership token. | Guaranteed at-least-once delivery; exactly-once logical effect via idempotent sink. |
| **`Status.DUPLICATE`** | The `event.id` was already accepted or committed. No additional buffer space or KV claim is allocated. | Existing `pending` or `committed` record is preserved; conflicting payloads/timestamps ignored. | Zero duplicate effects. Idempotent response to caller. |
| **`Status.TOO_OLD`** | Arrived older than sliding cutoff (`event.ts < watermark - window`). State already evicted. | No KV claim created. Diverted to DLQ. | Zero sink effects. |
| **`Status.TOO_FAR_FUTURE`** | Exceeds `time_source() + max_future_skew`. Clock skew guard. | No KV claim created. Diverted to DLQ. | Zero sink effects. |
| **`Status.INVALID_TIMESTAMP`** | Non-numeric or non-finite (`NaN`, `+inf`, `-inf`). | No KV claim created. Diverted to DLQ. | Zero sink effects. |
| **`KVError` (KV Failure)** | Communication with state store failed after `max_retries`. Acceptance cannot be confirmed. | Capacity reservation is released. Key may or may not exist. | If claim wrote but ACK lost, caller resend will resolve via ownership token. |

---

## 4. Design & Implementation Changes

### Files Modified:
- `dedup_engine/engine.py`
- `tests/test_lifecycle_p3a.py` (New test suite)

### Technical Details of the Fix

#### 1. Ingestion Capacity Reservation Mechanism
In `DedupEngine.__init__`:
```python
self._capacity_lock = asyncio.Lock()
self._reserved: int = 0
```
`_reserved` tracks in-flight claims that have secured buffer capacity but have not yet completed their KV claim or been inserted into `_heap`.

#### 2. Pre-Claim Capacity Verification
In `DedupEngine.ingest()`:
```python
# 4. Capacity reservation check before initiating write-ahead claim
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
```
- **Concurrency Safety**: Buffer capacity is verified and reserved under `self._capacity_lock`, ensuring that $N$ concurrent coroutines cannot race to exceed `max_buffer_size`.
- **Known Event Headroom**: If `event.id` is already in `self._live` or `self._claiming`, it will not consume new buffer capacity and bypasses reservation.
- **Duplicate Awareness on Overflow**: If the buffer is full, the engine checks whether the event is already in KV; if so, it returns `Status.DUPLICATE` rather than a spurious `Status.BUFFER_FULL`.
- **Resource Cleanup Guarantee**: The entire claim block is enclosed in a `try...finally` block that decrements `self._reserved` if the event is not accepted or encounters an error/cancellation:
```python
try:
    ...
    # on success:
    self._buffer(event)
    if reserved:
        self._reserved -= 1
        reserved = False
    ...
finally:
    if reserved:
        self._reserved -= 1
```

#### 3. Stats & Monitoring
`DedupEngine.stats()` was updated to expose `reserved=self._reserved` alongside `buffered=len(self._heap)`.

---

## 5. Regression Test Cases (`tests/test_lifecycle_p3a.py`)

Ten comprehensive, deterministic test cases were written to validate all requirements:

| Test Name | Scenario & Coverage | Verdict |
|---|---|---|
| `test_full_buffer_without_drainable_event` | Buffer full and held behind lateness -> returns `BUFFER_FULL`, zero KV claims created. | **PASS** |
| `test_full_buffer_with_successful_flush` | Buffer full but drainable -> flush frees space, new event successfully accepted. | **PASS** |
| `test_full_buffer_with_failed_flush` | Buffer full, flush attempted but KV commit fails -> returns `BUFFER_FULL`, no KV claim created. | **PASS** |
| `test_retry_after_capacity_becomes_available` | Rejected event returns `BUFFER_FULL`, drain frees buffer, retry returns `ACCEPTED`, duplicate retry returns `DUPLICATE`, 1 downstream effect. | **PASS** |
| `test_concurrent_submissions_while_buffer_is_nearly_full` | 20 concurrent coroutines race for 5 buffer slots -> exactly 5 `ACCEPTED`, 15 `BUFFER_FULL`, exactly 5 KV claims created. | **PASS** |
| `test_kv_claim_failure_releases_reservation` | Injected total KV failure raises `KVError` and cleanly releases `_reserved` to 0. | **PASS** |
| `test_ambiguous_claim_acknowledgement_with_capacity` | Write lands in KV but ACK lost; retry recognizes token, accepts, and updates reservations cleanly. | **PASS** |
| `test_process_restart_after_buffer_full_no_resurrection` | Engine rejected `BUFFER_FULL`, crashes; fresh engine `recover()` recovers only accepted events; rejected event is NOT resurrected. | **PASS** |
| `test_recovery_of_legitimately_accepted_events` | 3 accepted pending events survive crash and are safely recovered by new engine instance. | **PASS** |
| `test_cancellation_and_exception_cleanup_of_capacity_reservations` | Asyncio task cancelled mid-claim; reservation is cleanly decremented to 0 and buffer capacity preserved. | **PASS** |

---

## 6. Exact Command & Test Output

### 6.1 Pytest Suite Execution
**Command**:
```powershell
python -m pytest -v
```

**Output**:
```
============================= test session starts =============================
platform win32 -- Python 3.14.6, pytest-9.1.1, pluggy-1.6.0
rootdir: D:\Exactly-Once-Event-Deduplication-Engine
configfile: pytest.ini
testpaths: tests
plugins: anyio-4.15.1
collected 81 items

tests\test_api.py ....                                                   [  4%]
tests\test_correctness.py ..................                             [ 27%]
tests\test_fault_injection.py ...................................        [ 70%]
tests\test_lifecycle_p3a.py ..........                                   [ 82%]
tests\test_proxy.py ..                                                   [ 85%]
tests\test_window_semantics.py ............                              [100%]

============================== warnings summary ===============================
C:\Users\NWNB0000\AppData\Roaming\Python\Python314\site-packages\fastapi\testclient.py:1
  C:\Users\NWNB0000\AppData\Roaming\Python\Python314\site-packages\fastapi\testclient.py:1: StarletteDeprecationWarning: Using `httpx` with `starlette.testclient` is deprecated; install `httpx2` instead.
    from starlette.testclient import TestClient as TestClient  # noqa

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
======================= 81 passed, 1 warning in 46.69s ========================
```
- **Exit Code**: `0`
- **Total Tests**: `81` (Prior 71 tests + 10 Phase 3A tests)
- **Passed**: `81` (100%)
- **Failed**: `0`

---

### 6.2 Benchmark Throughput & Latency Execution
**Command**:
```powershell
python bench/benchmark.py --quick
```

**Output**:
```
=== A) throughput & latency vs KV RTT and concurrency ===
  kv_latency_ms |     concurrency |  throughput_eps |          p50_ms |          p95_ms |          p99_ms | kv_ops_per_event |    exactly_once
            0.0 |               1 |           16008 |            0.03 |            0.07 |            0.11 |             2.0 |            True
            0.0 |              16 |           19422 |            0.57 |            1.03 |             2.2 |             2.0 |            True
            0.0 |             128 |           21034 |            5.61 |            7.19 |            7.86 |             2.0 |            True
            0.5 |               1 |              50 |           15.69 |           31.63 |           32.22 |             2.0 |            True
            0.5 |              16 |            1075 |           14.94 |           30.85 |           31.88 |             2.0 |            True
            0.5 |             128 |           18274 |            6.14 |            8.05 |            8.27 |             2.0 |            True
            2.0 |               1 |              49 |            15.7 |           31.56 |           32.32 |             2.0 |            True
            2.0 |              16 |             742 |           15.55 |           31.15 |           43.08 |             2.0 |            True
            2.0 |             128 |            7123 |            6.33 |           27.73 |            28.7 |             2.0 |            True

=== B) duplicate ratio (KV RTT 0.5ms, concurrency 64) ===
      dup_ratio |  throughput_eps |          p95_ms |   dups_detected | kv_ops_per_event |    exactly_once
              0 |           11978 |           25.71 |               0 |             2.0 |            True
           0.25 |           14066 |             5.6 |             250 |             2.0 |            True
            0.5 |           15035 |           10.18 |             500 |             2.0 |            True
            0.9 |           11900 |           18.34 |             901 |             2.0 |            True

=== C) sliding-window size vs KV state (event spacing 50ms) ===
       window_s |    peak_kv_keys |  throughput_eps |    exactly_once
              5 |             130 |           17907 |            True
             30 |             654 |           16261 |            True
            120 |            1401 |           16906 |            True
            600 |            1401 |           18417 |            True

=== D) throughput under injected KV faults (request-loss + ack-loss) ===
      fail_rate |        ack_loss |  throughput_eps |          p99_ms |         retries |    exactly_once
              0 |               0 |           12796 |            18.8 |               0 |            True
           0.01 |            0.01 |            6433 |           16.45 |              47 |            True
           0.05 |            0.05 |            3767 |            14.3 |             224 |            True
            0.1 |             0.1 |            2303 |           32.64 |             530 |            True
            0.2 |             0.2 |            1504 |          137.54 |            1643 |            True

All benchmark scenarios kept exactly-once effects. CSVs in bench/results/
chart: bench/results/benchmarks.png
```
All four benchmark suites completed with `exactly_once == True` and sustained throughput up to 21,034 events/s.

---

## 7. Remaining Limitations (Out of Scope for Phase 3A)

In accordance with Phase 3A objectives, the following items remain scheduled for subsequent Phase 3 milestones:
1. **Redis Atomicity (Phase 3B)**: `RedisKV.set()` continues to use split `SET` and `ZADD` commands without atomic Lua scripting or multi-exec pipelines.
2. **Recovery Scalability (Phase 3C)**: `RedisKV.scan()` still performs $O(N)$ sequential individual `GET` requests, and `DedupEngine.recover()` does not paginate or bound heap loading during crash recovery.
3. **Downstream Sink Idempotency Hardening (Phase 3D)**: `StdoutSink` remains non-idempotent, and `FileSink` defaults to `fsync=False` without file locking.
4. **Single-Writer Partitioning**: The engine assumes single-writer semantics per partition for reorder buffer correctness.

---

## 8. Conclusion

**DEFECT-P2-01 is completely resolved and verified**. The event lifecycle is now fully consistent with the documented contract: buffer capacity is established prior to write-ahead claims, no stranded claim can cause false `DUPLICATE` responses upon client retry, and no rejected event can be resurrected upon process crash recovery.
