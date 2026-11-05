# Phase 3D Remediation Report: FileSink Cross-Process Idempotency & Corruption Safety

**Engineer**: Implementation Engineer  
**Date**: November 3, 2026  
**Status**: REMEDIATED & VERIFIED  
**Baseline Test Matrix**: 127 / 127 Passed (100%) across unit, correctness, fault injection, lifecycle P3A, Redis integration P3B, recovery P3C, and sink reliability P3D test suites.

---

## 1. Executive Summary & Root Cause Analysis

Following the Phase 3D independent verification report, two issues were identified in `FileSink`:

1. **DEF-3D-01 (High Severity - Cross-Process Deduplication Failure)**:
   - *Root Cause*: `FileSink.deliver()` checked `if event.id in self._seen:` outside the file lock using a process-local in-memory set. When `lock_file=True`, the lock was acquired only during the physical write, serializing byte streams but failing to re-verify whether another process had already written the event ID to disk. Consequently, two or more processes opening the file before either wrote would both append the duplicate event ID.
   - *Remediation*: Implemented authoritative disk verification while holding the exclusive lock (`_is_event_on_disk_locked()`). When `lock_file=True`, the authoritative duplicate decision is made within the locked section, guaranteeing that concurrent writes from independent OS processes result in exactly one physical JSONL record. Local in-memory caches are updated only after verification and append succeed.

2. **DEF-3D-02 (Medium Severity - Silent Erasure of Deduplication Evidence During Corrupted Line Pruning)**:
   - *Root Cause*: `FileSink._recover_state()` treated any malformed line anywhere in the file as a "torn tail" and triggered a file rewrite that silently omitted corrupted records. If a previously acknowledged event record was corrupted mid-file, its historical evidence was deleted, allowing subsequent producer retries to create duplicate logical effects.
   - *Remediation*: Refactored `_recover_state_locked()` to distinguish a trailing incomplete record at EOF from corruption in the middle of the file. Trailing incomplete lines at EOF (unclosed braces from a hard process kill during write) are truncated cleanly. Corruption detected anywhere else in the file triggers a **fail-closed** policy: a timestamped quarantine backup (`.quarantine.<timestamp>`) is created, the original file is left intact, and `SinkError` is raised, refusing startup to prevent silent loss of deduplication evidence.

3. **Power-Loss Durability Qualification**:
   - Explicitly clarified durability boundaries across process crash, OS crash, and sudden power loss.
   - `os.fsync(fd)` guarantees drive cache synchronization before delivery acknowledgement when `fsync=True`.
   - Added `_sync_parent_directory()` to execute directory descriptor fsync after atomic rename (`os.replace`) on POSIX filesystems, with NTFS journal metadata semantics documented for Windows.

---

## 2. Updated Sink Guarantee Matrix

| Sink Implementation | Idempotent? | Durability Level | Restart-Safe? | Multi-Process Safe? | Exactly-Once Logical Effect? |
|---|---|---|---|---|---|
| **MemorySink** | Yes (keyed on `event.id`) | No (in-memory) | No (lost on process exit) | No | **Yes (in-process only)** |
| **FileSink (`lock_file=True, fsync=True`)** | Yes (disk check + memory cache) | **Durable** (`os.fsync`) | **Yes** (recovers seen IDs, truncates torn tail at EOF, quarantines internal corruption) | **Yes** (exclusive lock + shared disk scan) | **Yes (durable across crashes and multiple OS processes)** |
| **FileSink (`lock_file=False, fsync=True`)** | Yes (single process) | **Durable** (`os.fsync`) | **Yes** (recovers seen IDs, truncates torn tail at EOF, quarantines internal corruption) | **No** (single process only) | **Yes (single process durable across crashes)** |
| **FileSink (`fsync=False`)** | Yes (keyed on `event.id`) | OS page cache | **Yes** (recovers seen IDs, truncates torn tail at EOF, quarantines internal corruption) | Depends on `lock_file` | Yes (crash-safe, vulnerable to sudden power loss) |
| **NaiveSink** | **No** (appends on every deliver attempt) | No (in-memory) | No | No | **No (creates duplicate side-effects on retry/recovery)** |
| **StdoutSink (`dedup=False`)** | **No** (prints on every deliver attempt) | No (ephemeral terminal stream) | No | No | **No (duplicates printed on retry/recovery)** |
| **StdoutSink (`dedup=True`)** | Partial (process-local bounded memory cache) | No (ephemeral terminal stream) | No | No | Best-effort process-local only (not crash-safe) |

---

## 3. Code Modifications in `dedup_engine/sink.py`

### 3.1 Authoritative Cross-Process Deduplication (`DEF-3D-01`)
```python
def _is_event_on_disk_locked(self, event_id: str) -> bool:
    """Scan file on disk while holding lock to detect cross-process writes."""
    if not os.path.exists(self.path):
        return False
    with open(self.path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                rec = json.loads(stripped)
                if isinstance(rec, dict) and str(rec.get("id")) == event_id:
                    return True
            except ValueError:
                pass
    return False

async def deliver(self, event: Event) -> None:
    async with self._lock:
        # In-memory fast path check (optimization for same process)
        if event.id in self._seen:
            return

        if self._f is None or self._f.closed:
            raise SinkError(f"FileSink({self.path}) is closed")

        line = event.to_json() + "\n"
        try:
            if self.lock_file and self._lock_fd is not None:
                with _file_lock(self._lock_fd, exclusive=True):
                    # Authoritative cross-process check while holding exclusive file lock
                    if self._is_event_on_disk_locked(event.id):
                        self._seen.add(event.id)
                        return

                    self._f.seek(0, os.SEEK_END)
                    self._f.write(line)
                    self._f.flush()
                    if self.fsync:
                        os.fsync(self._f.fileno())
            else:
                self._f.seek(0, os.SEEK_END)
                self._f.write(line)
                self._f.flush()
                if self.fsync:
                    os.fsync(self._f.fileno())

            self._seen.add(event.id)
        except OSError as e:
            raise SinkError(f"FileSink write failed: {e}") from e
```

### 3.2 EOF Torn-Tail Truncation vs. Fail-Closed Internal Quarantine (`DEF-3D-02`)
```python
def _recover_state_locked(self) -> None:
    if not os.path.exists(self.path):
        return

    with open(self.path, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    if not lines:
        return

    valid_records: list[dict] = []
    corrupted_indices: list[int] = []

    for idx, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            record = json.loads(stripped)
            if isinstance(record, dict) and "id" in record:
                valid_records.append(record)
                self._seen.add(str(record["id"]))
            else:
                corrupted_indices.append(idx)
        except (ValueError, KeyError):
            corrupted_indices.append(idx)

    if not corrupted_indices:
        return

    last_idx = len(lines) - 1

    # Check if the ONLY corruption is an incomplete/torn trailing record at EOF
    if len(corrupted_indices) == 1 and corrupted_indices[0] == last_idx:
        temp_path = self.path + ".recovery.tmp"
        with open(temp_path, "w", encoding="utf-8") as tf:
            for idx in range(last_idx):
                tf.write(lines[idx])
            tf.flush()
            if self.fsync:
                os.fsync(tf.fileno())
        os.replace(temp_path, self.path)
        _sync_parent_directory(self.path)
        return

    # Internal corruption detected: Fail closed! Preserve original and create quarantine copy
    quarantine_path = f"{self.path}.quarantine.{int(time.time() * 1000)}"
    shutil.copy2(self.path, quarantine_path)
    raise SinkError(
        f"FileSink({self.path}) detected internal corrupted records at lines {corrupted_indices}. "
        f"Original file preserved and quarantined to {quarantine_path}. Refusing to start to prevent "
        f"silent loss of deduplication history."
    )
```

---

## 4. Verification & Test Suite Additions (`tests/test_sinks_p3d.py`)

Three new comprehensive test functions were added to `tests/test_sinks_p3d.py`, bringing the total sink test count to 18:

1. **`test_filesink_cross_process_idempotency_two_processes` (Test 16)**:
   - Spawns two independent OS processes using `multiprocessing.Process`.
   - Both processes initialize on the same target file with `lock_file=True, fsync=True`.
   - A `multiprocessing.Barrier(2)` forces both processes to finish initialization and synchronize before either process calls `deliver()`.
   - Both processes concurrently attempt to deliver identical `event.id = "shared-evt-1"`.
   - **Result**: Exactly **1 physical record** is written to disk for `shared-evt-1`. Zero duplicates.

2. **`test_filesink_cross_process_contention_four_processes` (Test 17)**:
   - Spawns 4 independent OS processes concurrently competing to deliver 10 shared event IDs (40 total delivery attempts).
   - Synchronized with `multiprocessing.Barrier(4)`.
   - **Result**: Exactly **10 records** on disk for the 10 unique IDs. 0 duplicates across all 4 OS processes.

3. **`test_filesink_mid_file_corruption_preserves_original_and_quarantine` (Test 18)**:
   - Tests file with valid line 1, corrupted line 2, and valid line 3.
   - Asserts initialization raises `SinkError`.
   - Asserts original file contents are untouched.
   - Asserts timestamped `.quarantine.<timestamp>` file is created preserving full diagnostic evidence.

---

## 5. Regression & Benchmark Execution Results

- **`tests/test_sinks_p3d.py`**: **18 / 18 passed** in 6.82s.
- **Full Pytest Regression Suite**: **127 / 127 passed** in 174.00s (`python -m pytest -v`, 100% pass rate).
- **Ingestion Benchmark (`python bench/benchmark.py --quick`)**:
  - All benchmark scenarios maintained 100% exactly-once compliance (`exactly_once == True`).
  - Throughput sustained up to 21,161 eps in memory.
  - Zero regressions across Phases 1, 2, 3A, 3B, 3C, and 3D.

---

## 6. Durability & Operational Limitations

1. **Single-Writer per Partition Architecture**:
   - While `FileSink(lock_file=True)` provides authoritative cross-process deduplication on a single machine filesystem, the engine's overarching reorder buffer and watermark advancement require single-writer semantics per partition. Multi-instance cluster scaling requires key partitioning (scheduled for Phase 5).
2. **Advisory Locking Across Network Mounts**:
   - `_file_lock()` relies on OS-level advisory locks (`fcntl.flock` on Unix, `msvcrt.locking` on Windows). Advisory file locking across remote network filesystems (e.g., NFS, CIFS/SMB) depends on remote lock daemon implementations (`rpc.statd`, `lockd`) and is not recommended for high-frequency low-latency stream processing. Local SSD/NVMe filesystems should be used for `FileSink`.
3. **Power-Loss Synchronization Scope**:
   - `fsync=True` forces drive cache writeback for file contents. Directory synchronization (`_sync_parent_directory`) after rename operations is supported on POSIX systems with `O_DIRECTORY`. On Windows, NTFS metadata journaling provides equivalent file allocation table crash consistency.
