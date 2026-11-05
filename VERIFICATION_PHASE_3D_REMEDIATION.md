# Independent Verification Report — Phase 3D Remediation: FileSink Cross-Process Idempotency & Corruption Safety

**Date**: 2026-11-03  
**Auditor**: Independent Senior Systems Verification Engineer  
**Scope**: Verification of Phase 3D remediation addressing DEF-3D-01 (Cross-Process Deduplication Failure) and DEF-3D-02 (Mid-File Corruption Truncation Data Loss).  
**Repository**: `D:\Exactly-Once-Event-Deduplication-Engine`  
**Target Component**: `dedup_engine/sink.py` (`FileSink`, `_file_lock`, `_recover_state_locked`, `_is_event_on_disk_locked`) and sink documentation.  
**Verdict**: **PASS PHASE 3D**

---

## 1. Executive Summary

Phase 3D initial verification issued a **FAIL** rating due to two critical defects in [dedup_engine/sink.py](file:///D:/Exactly-Once-Event-Deduplication-Engine/dedup_engine/sink.py):
1. **DEF-3D-01 (High)**: `FileSink.deliver()` tested membership against a stale in-memory set (`event.id in self._seen`) *before* acquiring the inter-process lock. Concurrent OS processes each passed the check and appended duplicate records.
2. **DEF-3D-02 (High)**: `_recover_state_locked()` scanned line-by-line and unconditionally truncated the file at the first invalid line without distinguishing EOF partial writes from mid-file or historical line corruptions, silently discarding subsequent valid committed events.

### Verification Outcome
The implementation agent submitted a remediation package. We conducted an independent adversarial audit, multi-process stress testing, corruption fault injection, regression testing across all 127 tests, and benchmark invariant verification.

- **DEF-3D-01 is VERIFIED FIXED**: `FileSink.deliver()` with `lock_file=True` now acquires an exclusive file lock on a dedicated lock file (`<path>.lock`) *prior* to querying authoritative disk state. Authoritative disk checks are executed within the locked critical section via `_is_event_on_disk_locked(event.id)`. In-memory `_seen` cache is strictly updated after disk confirmation or write. Multi-process concurrency tests with 2 and 4 independent OS processes demonstrated zero duplicate appends.
- **DEF-3D-02 is VERIFIED FIXED**: `_recover_state_locked()` now classifies unparseable records by file position. If unparseable bytes occur strictly at the end of the file (trailing torn-tail from process crash or power loss), it cleanly truncates the trailing unparseable fragment via atomic temp-file replacement. If unparseable bytes appear prior to any valid subsequent lines (mid-file or historic byte corruption), `FileSink` immediately aborts initialization, raises `SinkError`, preserves the original file byte-for-byte, and creates a forensic timestamped `.quarantine.<timestamp>` copy.
- **Durability & Platform Semantics are ACCURATELY QUALIFIED**: `FileSink(fsync=True)` executes `os.fsync(fd)` per record delivery for durable on-disk persistence. `FileSink(fsync=False)` is correctly documented as surviving process crashes via OS page cache but vulnerable to OS/power loss. Directory sync (`_sync_parent_directory()`) is implemented on POSIX systems via `os.open(parent_dir, os.O_RDONLY | os.O_DIRECTORY)` and gracefully skipped on Windows NTFS where directory metadata is journaling-backed.
- **Regression Suite**: 127/127 tests passed cleanly (including 18 sink tests, 12 live Redis tests, 35 fault-injection tests, 16 recovery tests, and 10 lifecycle tests).
- **Benchmark Invariants**: Quick benchmark completed with `exactly_once == True` across all test matrices (0.0ms-2.0ms latency, up to 90% duplicates, sliding windows, and 20% request/ack loss).

---

## 2. Test Environment & Setup

- **Host OS**: Windows 11 Pro (win32, x64)
- **Python**: 3.14.6 (`pytest-9.1.1`, `anyio-4.15.1`)
- **Redis Server**: Redis 8.4.2 running in WSL2, accessible at `redis://localhost:6380/0`
- **Locking Implementation**: Platform-appropriate OS-level locking:
  - Windows: `msvcrt.locking(fd, msvcrt.LK_LOCK, 1)` / `msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)`
  - POSIX: `fcntl.flock(fd, fcntl.LOCK_EX)` / `fcntl.flock(fd, fcntl.LOCK_UN)`

---

## 3. Defect Remediation Verification

### 3.1 DEF-3D-01: Cross-Process Deduplication & Lock Order

#### Source Code Audit
Inspecting [dedup_engine/sink.py](file:///D:/Exactly-Once-Event-Deduplication-Engine/dedup_engine/sink.py#L318-L368):
```python
def deliver(self, event: Event) -> None:
    # Process-local cache short-circuit only when locking is disabled
    if not self._lock_file and event.id in self._seen:
        return

    lock_fd = self._acquire_lock()
    try:
        # Check disk state while holding lock
        if self._lock_file and self._is_event_on_disk_locked(event.id):
            self._seen.add(event.id)
            return

        # Double check in-memory seen
        if event.id in self._seen:
            return

        ...  # Format, write, and fsync
        self._seen.add(event.id)
    finally:
        self._release_lock(lock_fd)
```

Key Architectural Properties Confirmed:
1. **Lock Prior to Decision**: When `lock_file=True`, the lock on `<path>.lock` is acquired *before* evaluating whether the event exists.
2. **Authoritative Disk Query**: `_is_event_on_disk_locked(event.id)` scans disk records backwards from current file EOF up to `_last_scan_pos`, ensuring records committed by concurrent processes are immediately detected.
3. **Lock Release Guarantee**: `_release_lock(lock_fd)` is enclosed in a strict `finally` block, ensuring no leaked file locks on formatting errors, I/O errors, or duplicate exits.
4. **Dedicated Lock File**: Locking uses a separate `.lock` descriptor instead of locking the append-only data stream, preventing lock-offset collision with file pointers.

#### Independent Adversarial Multi-Process Reproduction
We executed independent multi-process stress tests using `multiprocessing.Process` spawning genuine independent OS processes:

1. **Two-Process Race (5 Staggered Rounds)**:
   - Setup: Two distinct processes initialized against the same `FileSink` path.
   - Execution: Both processes attempt to deliver the identical event (`event_shared_round_X`) simultaneously across 5 separate rounds.
   - Result:
     - Output file contained exactly 1 JSONL entry per round.
     - Total records on disk: 5 lines for 5 unique events.
     - Zero duplicate appends observed across all runs.

2. **Four-Process High-Contention (40 Operations / 10 Unique Events)**:
   - Setup: 4 independent OS processes started simultaneously.
   - Workload: Each process attempts to deliver 10 shared events (`evt_0` through `evt_9`) concurrently (40 deliveries total).
   - Result:
     - Output file contained exactly 10 lines.
     - `set(recorded_ids)` matched the 10 unique IDs exactly.
     - File was valid JSONL without interleaving or torn lines.

3. **Fallback Mode Audit (`lock_file=False`)**:
   - Accurately documented in docstrings and report: when `lock_file=False`, `FileSink` relies solely on in-memory deduplication and POSIX `O_APPEND` atomic block appends, which does NOT guarantee cross-process deduplication across independent runtimes.

---

### 3.2 DEF-3D-02: Corruption Safety & Crash Repair

#### Source Code Audit
Inspecting [dedup_engine/sink.py](file:///D:/Exactly-Once-Event-Deduplication-Engine/dedup_engine/sink.py#L225-L315):
```python
def _recover_state_locked(self) -> None:
    ...
    # Two-pass validation:
    # Pass 1: Parse all lines, identify valid lines and unparseable positions
    # If unparseable lines exist:
    #   If unparseable lines are strictly trailing at EOF (no subsequent valid lines):
    #       Cleanly truncate to last_valid_offset via atomic temp-file replace.
    #   Else (unparseable lines occur mid-file or before valid lines):
    #       Quarantine corrupted file to path.quarantine.<timestamp>
    #       Raise SinkError with diagnostic information (fail-closed).
```

Key Architectural Properties Confirmed:
1. **Trailing Torn-Tail Truncation**: Partial writes occurring at EOF (the standard signature of a crash midway through writing a line) are safely removed. The file is truncated to the end of the last fully valid JSON line.
2. **Fail-Closed on Historical / Mid-File Corruption**: Corrupted lines that precede valid data cannot be proven to be torn writes. Rather than discarding data, the engine:
   - Leaves the primary data file untouched or preserved.
   - Writes a timestamped quarantine file (`<target>.quarantine.<timestamp>`).
   - Raises `SinkError` to block the engine from operating on corrupted historical state.

#### Independent Fault-Injection Verification
We evaluated 4 corruption scenarios using isolated files:

- **Scenario 1 (Mid-File Corruption)**:
  - File: `Line 1 (Valid) \n CORRUPTED_BYTES \n Line 2 (Valid) \n`
  - Result: Raised `SinkError("Corrupted records detected prior to EOF... quarantined to ...")`. Primary file remained completely intact with all 3 lines preserved. Quarantine file created on disk. **PASS**.
- **Scenario 2 (Trailing Torn-Tail / Crash at EOF)**:
  - File: `Line 1 (Valid) \n {"id": "evt_partia` (EOF without newline)
  - Result: Trailing torn line truncated. Recovered state contains `Line 1`. Initialization succeeded. **PASS**.
- **Scenario 3 (Empty File)**:
  - File: 0 bytes.
  - Result: Recovered cleanly, `_seen` empty, `_last_scan_pos == 0`. **PASS**.
- **Scenario 4 (Line 0 Corruption with subsequent Valid Line)**:
  - File: `CORRUPTED_LINE_0 \n Line 1 (Valid) \n`
  - Result: Raised `SinkError`. Failed closed. Original file untouched. **PASS**.

---

## 4. Durability & Platform Considerations

| Dimension | `fsync=True` | `fsync=False` |
| :--- | :--- | :--- |
| **Flush Target** | OS page cache + Storage Controller (`os.fsync(fd)`) | OS page cache only |
| **Crash Protection** | Process crash, OS kernel panic, sudden power loss | Process crash (OS flushes cache upon process termination) |
| **Throughput Impact** | Bounded by storage device sync IOPS (~100-2,000 eps) | High throughput (OS write buffering, >15,000 eps) |
| **Parent Directory Sync** | POSIX: `fsync` on directory fd; Windows: NTFS journaled | Skipped |
| **Cross-Process Deduplication** | Guaranteed with `lock_file=True` | Guaranteed with `lock_file=True` |

---

## 5. Test Suite & Benchmark Execution

### 5.1 Full Test Suite Execution
```
pytest -v
================= 127 passed, 1 warning in 173.67s =================
```
- `tests/test_sinks_p3d.py`: 18/18 passed.
- `tests/test_redis_integration.py`: 12/12 passed (Live Redis 8.4.2).
- `tests/test_fault_injection.py`: 35/35 passed.
- `tests/test_recovery_p3c.py`: 16/16 passed.
- `tests/test_lifecycle_p3a.py`: 10/10 passed.
- `tests/test_correctness.py`: 18/18 passed.
- `tests/test_window_semantics.py`: 12/12 passed.
- `tests/test_api.py`: 4/4 passed.
- `tests/test_proxy.py`: 2/2 passed.

### 5.2 Benchmark Verification (`python bench/benchmark.py --quick`)
- **A) Latency & Concurrency**: 0.0ms to 2.0ms KV latency; `exactly_once == True` maintained across all concurrency levels (1, 16, 128).
- **B) Duplicate Ratio**: Duplicate ratios from 0.0 to 0.9 (90% dups); `exactly_once == True` maintained across all workloads.
- **C) Sliding Window Size**: Windows from 5s to 600s; `exactly_once == True` maintained.
- **D) Injected KV Faults**: Up to 20% request-loss + ack-loss; `exactly_once == True` maintained with retries.

---

## 6. Verification Verdict

```
===============================================================================
                            FINAL VERDICT: PASS PHASE 3D
===============================================================================
```

Both defects (`DEF-3D-01` and `DEF-3D-02`) have been completely resolved and verified through independent adversarial tests. Sink guarantees are formally documented, multi-process serialization is enforced at the disk level, crash recovery properly protects historical data, and all 127 regression tests pass. Phase 3D is certified complete.
