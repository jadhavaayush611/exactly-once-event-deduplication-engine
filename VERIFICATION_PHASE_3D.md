# Independent Verification Report — Phase 3D: Downstream Sink Reliability, Idempotency & Delivery Guarantees

**Verifier**: Independent Senior Systems Engineer  
**Date**: November 3, 2026  
**Python Runtime**: Windows 11 Pro / Python 3.14.6 (64-bit)  
**Redis Version**: 8.4.2 (port 6380, AOF enabled)  
**Test Matrix**: 124 / 124 Passed (100%)  
**Verdict**: **FAIL PHASE 3D** (Defect identified in claimed cross-process idempotency and internal corrupted line handling)

---

## 1. Executive Verdict & Summary

### **VERDICT: FAIL PHASE 3D**

While Phase 3D significantly strengthens the project's sink implementations—providing explicit in-memory deduplication contracts, torn-tail recovery on startup, configurable `fsync` flushing, and thorough engine-to-sink commit ordering tests—an independent audit and reproduction revealed **two critical defects** where the implementation report and documentation overclaim capabilities that fail under independent multi-process and corruption testing:

1. **Defect DEF-3D-01 (High Severity - Cross-Process Deduplication Failure)**:  
   The Phase 3D report, `README.md`, and docstrings claim `FileSink(lock_file=True)` is **"Multi-Process Safe: Yes"** and provides **"durable exactly-once logical effects across processes"**.  
   However, an independent multi-process reproduction proved that `FileSink.deliver()` checks its duplicate filter `if event.id in self._seen:` **outside and prior to acquiring the file lock**, relying solely on an in-memory `set` populated at process startup. When two independent OS processes initialize before a given event ID is written and subsequently attempt to deliver the same event concurrently with `lock_file=True`, the advisory lock merely serializes their raw write operations: **both processes write duplicate records to the file**, resulting in duplicate downstream logical effects.

2. **Defect DEF-3D-02 (Medium Severity - Silent Erasure of Deduplication Evidence During Corrupted Line Pruning)**:  
   `FileSink._recover_state()` treats any malformed line anywhere in the JSONL file as a "torn tail" and triggers a full file rewrite that silently discards the corrupted line. If a previously acknowledged record is partially corrupted in the middle of the file and its event ID is subsequently redelivered by an upstream producer, `FileSink` no longer has any record of that ID and will append a second entry, violating exactly-once logical effects.

Per the strict verdict criteria:  
> *"Issue PASS PHASE 3D only if: Sink guarantees match actual behavior; any claimed cross-process safety is actually demonstrated; corruption cannot silently erase deduplication evidence and create duplicate effects; no critical or high-severity defect remains."*

Because cross-process deduplication fails and corruption can silently discard deduplication evidence, Phase 3D receives a **FAIL** verdict until these claims and implementations are reconciled.

---

## 2. Repository Files & Functions Inspected

- **`dedup_engine/sink.py`**:
  - `_file_lock()`: Platform-specific advisory locking (`msvcrt.locking` on Windows, `fcntl.flock` on Unix).
  - `MemorySink.deliver()`: In-memory set deduplication.
  - `NaiveSink.deliver()`: Deliberately non-idempotent append.
  - `FileSink.__init__()`, `_recover_state()`, `deliver()`, `close()`: File initialization, torn-tail recovery, locking, and fsync behavior.
  - `StdoutSink.deliver()`: Standard output streaming with optional bounded deduplication.
- **`dedup_engine/engine.py`**:
  - `DedupEngine._flush()`, `_deliver()`, `_commit()`: Delivery order, commit ordering, and retry mechanics.
- **`tests/test_sinks_p3d.py`**:
  - All 15 unit and integration tests.
- **`bench/benchmark.py`**:
  - Ingestion benchmark `exactly_once` evaluation logic.
- **Documentation**:
  - `README.md`, `docs/ACCEPTANCE_CRITERIA.md`, `docs/IMPLEMENTATION_AUDIT.md`, `docs/PHASE_3D_IMPLEMENTATION_REPORT.md`.

---

## 3. Test Environment & Execution Outputs

- **Operating System**: Microsoft Windows 11 Pro (build 26100)
- **Python**: 3.14.6
- **Redis Server**: Redis 8.4.2 (port 6380)

### 3.1 Pytest Execution Outputs
```
python -m pytest tests/test_sinks_p3d.py -v
============================= 15 passed in 0.22s ==============================

python -m pytest -v
================= 124 passed, 1 warning in 167.90s (0:02:47) ==================
```

### 3.2 Quick Benchmark Execution Output
```
python bench/benchmark.py --quick
All benchmark scenarios kept exactly-once effects. CSVs in bench/results/
chart: bench/results/benchmarks.png
```

All 124 tests pass cleanly. However, unit test passing alone does not validate claims where tests did not exercise the true multi-process failure modes.

---

## 4. Sink Guarantee Matrix: Claimed vs. Independently Established

| Sink Configuration | Idempotent? | Durable? | Restart-Safe? | Multi-Process Safe? | Claim Status |
|---|---|---|---|---|---|
| **MemorySink** | Yes (in-memory) | No | No | No | **VERIFIED** |
| **NaiveSink** | No (appends every try) | No | No | No | **VERIFIED** |
| **StdoutSink (`dedup=False`)** | No | No | No | No | **VERIFIED** |
| **StdoutSink (`dedup=True`)** | Partial (process-local) | No | No | No | **VERIFIED** |
| **FileSink (`lock_file=False`)** | Yes (single process) | Yes (`fsync=True`) | Yes | No | **VERIFIED** |
| **FileSink (`lock_file=True`)** | **Single process only** | Yes (`fsync=True`) | Yes | **NO (Fails duplicate suppression across processes)** | **OVERCLAIM / DEFECT** |

---

## 5. Cross-Process Concurrency Findings & Reproduction

### 5.1 Code Inspection of `FileSink.deliver()`
Lines 207–236 of `dedup_engine/sink.py`:
```python
async def deliver(self, event: Event) -> None:
    async with self._lock:
        # 1. In-memory fast path check (STALE ACROSS PROCESSES!)
        if event.id in self._seen:
            return

        if self._f is None or self._f.closed:
            raise SinkError(f"FileSink({self.path}) is closed")

        try:
            fd = self._f.fileno()
            line = event.to_json() + "\n"

            if self.lock_file:
                with _file_lock(fd, exclusive=True):
                    # Re-check seen on disk if multi-process locking is active <-- COMMENT ONLY!
                    self._f.seek(0, os.SEEK_END)
                    self._f.write(line)
                    self._f.flush()
                    if self.fsync:
                        os.fsync(fd)
            ...
            self._seen.add(event.id)
```

**Flaw Identified**:  
Inside `with _file_lock(fd, exclusive=True):`, despite a comment stating `# Re-check seen on disk if multi-process locking is active`, **no disk check or re-scan of the file is performed!** The code immediately writes `self._f.write(line)`.

### 5.2 Independent Multi-Process Reproduction
An independent multi-process script was executed:
- Two separate OS processes (PID 2904 and PID 14228) opened the same `FileSink` with `lock_file=True, fsync=True`.
- A multiprocessing barrier ensured both processes opened the file and initialized their in-memory `_seen` sets before either wrote the chosen event ID (`evt-mp-1`).
- Both processes called `fs.deliver(Event(id='evt-mp-1', ...))`.

**Actual Output**:
```
LINES COUNT ON DISK: 2
Line 1: {"id":"evt-mp-1","ts":100.0,"data":{"pid":2904}}
Line 2: {"id":"evt-mp-1","ts":100.0,"data":{"pid":14228}}
```
Under 4-process contention writing 10 shared IDs, **40 records** were written to disk for 10 unique IDs.  
**Conclusion**: `FileSink(lock_file=True)` provides mutual exclusion for writing bytes (preventing torn ASCII interspersing), but **fails completely as an idempotent sink across multiple processes**. Claiming multi-process safety and exactly-once effects for `FileSink` across processes is invalid.

---

## 6. Crash Consistency & Durability Findings

### 6.1 Process Crash vs. Sudden Power Loss
- **Process Crash**: `FileSink(fsync=False)` flushes via `self._f.flush()`, writing user-space data to the OS page cache. In the event of a Python process crash (`SIGKILL`), the OS kernel flushes the page cache to disk.
- **Sudden Power Loss**: `FileSink(fsync=True)` calls `os.fsync(fd)`. This forces drive cache writeback for the file contents.
- **Directory Metadata Sync Caveat**: In `_recover_state()`, when an atomic temp file replace is performed (`os.replace(temp_path, self.path)`), `os.fsync` is called on the temp file descriptor, but on POSIX systems, `os.fsync` is not called on the parent directory descriptor (`os.open(parent_dir)`). On POSIX filesystems, sudden power loss immediately after `os.replace` can cause directory entry loss. On Windows/NTFS, metadata commits are handled by the NTFS log journal.

---

## 7. Corruption & Recovery Findings

### 7.1 Mid-File Corruption vs. Torn Tail
`FileSink._recover_state()` iterates over all lines in the file. If *any* line fails `json.loads` or lacks an `"id"` key, `has_torn_tail` is set to `True`:
```python
with open(self.path, "r", encoding="utf-8", errors="replace") as f:
    for line in f:
        stripped = line.strip()
        try:
            record = json.loads(stripped)
            if isinstance(record, dict) and "id" in record:
                self._seen.add(str(record["id"]))
                valid_lines.append(stripped + "\n")
            else:
                has_torn_tail = True
        except (ValueError, KeyError):
            has_torn_tail = True

if has_torn_tail:
    temp_path = self.path + ".recovery.tmp"
    with open(temp_path, "w", encoding="utf-8") as tf:
        for vl in valid_lines:
            tf.write(vl)
    os.replace(temp_path, self.path)
```

**Reproduction Analysis**:
1. When corruption occurs at the very end of the file (a true torn write from a crash during `write()`), pruning the trailing incomplete fragment and allowing the engine to recover and redeliver that event is sound.
2. However, if a byte in the *middle* of the file is corrupted (e.g., bit flip, disk block corruption, manual tampering), `_recover_state()` **deletes that corrupted record from the file permanently**.
3. Because the record is deleted, its `id` is never added to `self._seen`.
4. If an upstream producer later retransmits that event ID, `FileSink` treats it as a brand new event and appends it again, creating a duplicate downstream effect.
5. In addition, silently rewriting and removing historical acknowledged records without raising an error or quarantining the file violates fail-closed data integrity principles.

---

## 8. Engine-to-Sink Lifecycle Reproduction

The engine-to-sink commit lifecycle in `DedupEngine._flush()` was independently verified:

1. **Sink Failure Isolation**:
   - When a sink raises `SinkError`, the event remains in the engine's `_heap`.
   - The corresponding key in the KV store remains `state == "pending"`.
   - The engine never marks an unacknowledged event as `committed`.
2. **Crash After Sink Delivery, Before KV Commit**:
   - When a simulated crash occurs at `after_deliver` (after `sink.deliver()` succeeded but before `kv.set(COMMITTED)`):
   - The record on disk exists.
   - The record in KV remains `pending`.
   - On engine reboot and `recover()`, the engine re-queues the pending event and re-delivers it to the sink.
   - For `MemorySink` and single-process `FileSink`, the redelivery is recognized as a duplicate and suppressed.
   - For `NaiveSink`, the redelivery appends a duplicate effect, demonstrating why sink idempotency is required.

---

## 9. Benchmark Methodology & Limitations

In `bench/benchmark.py`:
- `exactly_once` is calculated as `len(got) == len(set(got)) == uniq`, checking whether the sink received exactly the expected unique event IDs without duplicates.
- **Limitation**: The benchmark suite exclusively instantiates `MemorySink()`.
- It does **not** exercise `FileSink`, disk I/O, `fsync` overhead, file locking, or multi-process contention.
- Therefore, the benchmark figures (e.g. 22,500 eps) reflect pure in-memory coroutine throughput and do not benchmark durable file sink performance.

---

## 10. Defects, Severity & Remediation Plan

### Defect Table

| Defect ID | Description | Severity | Impact | Remediation Required |
|---|---|---|---|---|
| **DEF-3D-01** | `FileSink(lock_file=True)` does not check disk for duplicates across processes | **High** | Multi-process writes of identical IDs produce duplicate file entries. Violates claimed cross-process idempotency. | **Option A**: Remove the multi-process idempotency claim from `README.md`, docstrings, and report, clarifying that `FileSink` is single-writer only; **Option B**: When `lock_file=True`, re-scan or tail-check the file for the ID inside the lock, or use a shared lock-coordinated index. |
| **DEF-3D-02** | `_recover_state()` silently deletes internal corrupted lines, losing deduplication history | **Medium** | Corrupted mid-file records are permanently pruned; retransmitted events are processed as new, causing duplicate logical effects. | Fail closed on mid-file corruption (raise `SinkError` or quarantine corrupted file to `.corrupt` backup) rather than silently truncating historical records. Only prune trailing incomplete lines if at EOF. |
| **DEF-3D-03** | `README.md` and Report claim "Durable across power loss" based solely on `os.fsync(fd)` | **Low** | On POSIX filesystems, directory metadata synchronization (`fsync` on parent directory) is required after atomic rename to prevent lost dirents. | Clarify documentation to state file descriptor data sync vs. filesystem directory journal semantics. |

---

## 11. Final Verification Conclusion

The Phase 3D implementation provides solid improvements for single-process file logging, torn-tail truncation, and commit ordering. However, because `FileSink(lock_file=True)` does not prevent duplicate writes across independent OS processes despite explicit claims in the report and documentation, and because internal corruption silently discards deduplication state:

**Final Decision**: **FAIL PHASE 3D**

Remediation must either align the documentation and guarantee matrix to reflect single-writer process boundaries or implement genuine cross-process deduplication.
