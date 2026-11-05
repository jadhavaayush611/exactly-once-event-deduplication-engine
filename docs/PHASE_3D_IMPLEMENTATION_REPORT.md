# Phase 3D Implementation Report: Downstream Sink Reliability, Idempotency & Delivery Guarantees

**Engineer**: Implementation Engineer  
**Date**: November 3, 2026  
**Status**: COMPLETE & VERIFIED  
**Baseline Test Matrix**: 124 / 124 Passed (100%) across unit, correctness, fault injection, lifecycle P3A, Redis integration P3B, recovery P3C, and sink reliability P3D test suites.

---

## 1. Executive Summary & Objective

In stream processing architectures, true exactly-once *network delivery* is mathematically impossible over an asynchronous unreliable network (the Two Generals' Problem). The Exactly-Once Event Deduplication Engine guarantees **exactly-once logical effects** through a combination of:
1. **At-least-once producer delivery** with deduplication tokens.
2. **Atomic Write-Ahead Log (WAL) state store claims** (`put_if_absent`).
3. **Event-time ordered reorder buffering** and watermark gating.
4. **Idempotent downstream sink applications** keyed on the immutable `event.id`.

The objective of Phase 3D is to strengthen downstream sink reliability, eliminate silent corruption risks, establish explicit persistence and locking contracts, and verify the engine-to-sink commit lifecycle.

---

## 2. Sink Guarantee Matrix

| Sink Implementation | Idempotent? | Durability Level | Restart-Safe? | Multi-Process Safe? | Exactly-Once Logical Effect? |
|---|---|---|---|---|---|
| **MemorySink** | Yes (keyed on `event.id`) | No (in-memory) | No (lost on process exit) | No | **Yes (in-process only)** |
| **FileSink (`fsync=True`)** | Yes (keyed on `event.id`) | **Durable** (`os.fsync`) | **Yes** (recovers seen IDs, prunes torn tails) | **Yes** (`lock_file=True`) | **Yes (across process crashes and power loss)** |
| **FileSink (`fsync=False`)** | Yes (keyed on `event.id`) | OS page cache | **Yes** (recovers seen IDs, prunes torn tails) | **Yes** (`lock_file=True`) | **Yes (across normal process crashes; vulnerable to power loss)** |
| **NaiveSink** | **No** (appends on every deliver attempt) | No (in-memory) | No | No | **No (creates duplicate side-effects on retry/recovery)** |
| **StdoutSink (`dedup=False`)** | **No** (prints on every deliver attempt) | No (ephemeral terminal stream) | No | No | **No (duplicates printed on retry/recovery)** |
| **StdoutSink (`dedup=True`)** | Partial (process-local bounded memory cache) | No (ephemeral terminal stream) | No | No | Best-effort process-local only (not crash-safe) |

---

## 3. Detailed Sink Audits & Implementations

### 3.1 FileSink Hardening
Before Phase 3D, `FileSink` had no dedicated unit tests, defaulted to `fsync=False`, had no process locking, and handled malformed lines with a simple silent pass.

In Phase 3D, `FileSink` in `dedup_engine/sink.py` was enhanced:
1. **Torn-Tail Crash Recovery (`_recover_state`)**:
   - On initialization, `FileSink` scans the target file line-by-line.
   - If a crash occurred during a write (leaving an unclosed, truncated JSON string or corrupted trailing bytes), `FileSink` detects the torn tail, rebuilds the file cleanly with all verified preceding records using an atomic temp-file replace (`os.replace`), and rebuilds the in-memory `_seen` set.
2. **Explicit Fsync Durability**:
   - Added `fsync: bool = False` configuration parameter.
   - When enabled, `os.fsync(fd)` is called on the file descriptor immediately after writing and flushing each event line. Delivery is only acknowledged once bits are safely committed to non-volatile storage.
3. **Advisory File Locking (`lock_file: bool = False`)**:
   - Supports cross-platform advisory file locking (`fcntl.flock` on Unix / macOS, `msvcrt.locking` on Windows).
   - Allows multiple local processes or instances to append safely without overlapping or interleaved line writes.
4. **Clean Resource Lifecycle**:
   - Added asynchronous `close()` method that flushes buffers, synchronizes to disk, and closes the underlying file descriptor.
   - Subsequent writes to a closed `FileSink` immediately raise `SinkError`.

### 3.2 MemorySink & NaiveSink
- **MemorySink**: Strictly idempotent in-memory sink storing unique `Event` instances in `self.effects` keyed on `event.id`. Used as the standard reference sink for deterministic correctness tests and benchmarks.
- **NaiveSink**: Intentionally non-idempotent sink that appends to `self.effects` on every invocation of `deliver()`. Serves as an educational counter-example proving that without an idempotent sink, retry and recovery mechanisms inevitably create duplicate downstream side-effects.

### 3.3 StdoutSink Demarcation
- `StdoutSink` is documented as an external ephemeral stream. Standard output lacks transactional rollback, two-phase commit, or persistence semantics.
- Added optional `dedup: bool = False` and `max_history: int = 10000` parameters. When enabled, a process-local bounded set filters out immediate in-process duplicates. It is clearly documented that `StdoutSink` cannot guarantee crash-safe or cross-process exactly-once delivery.

---

## 4. Engine-to-Sink Commit Lifecycle

The relationship between sink delivery and KV state store commit was audited in `DedupEngine._flush()`:

```
[ Reorder Buffer ]
        |
        v
  1. sink.deliver(event) ---------------------------> (Sink Failure)
        | (Success)                                         |
        v                                                   v
  2. kv.set("dedup:<id>", COMMITTED, score=ts)      Re-queue in _heap;
        | (Success)                                  State remains PENDING in KV;
        v                                            No premature commit.
  Record becomes evictable;
  Metrics delivered += 1
```

### Crash Invariants:
1. **Sink Failure**: If `sink.deliver(event)` raises `SinkError`, the event is kept in the reorder buffer `_heap`. Its record in the KV store remains `state == "pending"`. It is never prematurely marked `committed`.
2. **Crash Before Commit**: If the engine process crashes after `sink.deliver(event)` succeeds but before `kv.set(COMMITTED)` is applied:
   - On restart, `recover()` reloads the pending event from `idx:dedup:pending` and re-queues it into `_heap`.
   - The engine flushes the event to the downstream sink again.
   - If the sink is **idempotent** (`MemorySink`, `FileSink`), the redelivery is recognized as a duplicate by `event.id` and produces zero duplicate effects.
   - If the sink is **non-idempotent** (`NaiveSink`), the redelivery produces a duplicate downstream effect.

---

## 5. Verification & Test Suite (`tests/test_sinks_p3d.py`)

A dedicated 15-test suite was implemented covering all required reliability aspects:

1. `test_duplicate_event_delivery_to_idempotent_sinks`: Repeated deliveries of identical event IDs to `MemorySink` and `FileSink` produce exactly 1 effect.
2. `test_distinct_event_ids_delivered`: Distinct event IDs are independently delivered and preserved.
3. `test_retry_after_sink_exceptions`: Transient sink exceptions trigger engine retries with backoff without losing events.
4. `test_filesink_restart_and_reconstruction`: Re-opening an existing `FileSink` reloads known IDs and prevents duplicate writes across restarts.
5. `test_filesink_torn_tail_recovery`: Truncated or corrupted trailing records from hard crashes are cleanly pruned on startup without losing valid records.
6. `test_filesink_close_behavior`: Operations on closed sinks raise `SinkError`.
7. `test_filesink_fsync_flag`: Validates `fsync=True` and `fsync=False` code paths.
8. `test_filesink_corrupted_lines_safely_ignored`: Internal corrupted records are safely skipped during recovery.
9. `test_filesink_concurrent_writers`: Concurrent coroutines writing with advisory file locking produce clean, non-interleaved JSONL files.
10. `test_exactly_once_logical_effects_with_idempotent_sink`: Validates end-to-end exactly-once logical effects under duplicate ingestion.
11. `test_naive_sink_shows_duplicate_effects_under_retry`: Validates that `NaiveSink` demonstrates duplicate side effects when redelivery occurs.
12. `test_stdout_sink_behavior`: Tests `dedup=False` vs `dedup=True` bounded filtering.
13. `test_sink_failure_prevents_premature_commit`: Confirms KV record remains `pending` when sink fails.
14. `test_crash_between_deliver_and_commit_redelivers_safely`: Simulates crash at `after_deliver`, asserts post-crash recovery redelivery produces zero duplicate file records with `FileSink`.
15. `test_filesink_creates_parent_directories_cleanly`: Confirms automatic nested directory creation.

---

## 6. Regression & Benchmark Results

### 6.1 Pytest Suite
- `tests/test_sinks_p3d.py` $\to$ **15 / 15 passed** in 0.22s.
- Full regression suite (`python -m pytest -v`) $\to$ **124 / 124 passed** in 167.69s (100% pass rate).

### 6.2 Ingestion Benchmark (`python bench/benchmark.py --quick`)
- All scenarios confirmed exactly-once logical effects (`exactly_once == True`).
- Peak throughput reached **22,500 eps** in memory.
- Injected KV faults (up to 20% request and ack loss) maintained 100% exactly-once compliance with graceful retry degradation.

---

## 7. Conclusion

Phase 3D establishes clear, verifiable boundaries for downstream sink reliability. By hardening `FileSink` with fsync persistence, advisory file locking, and crash torn-tail recovery, the engine guarantees durable exactly-once logical effects across unexpected process termination.
