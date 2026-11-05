"""Downstream sinks. Exactly-once *effect* = at-least-once delivery + idempotent apply,
so a real sink must deduplicate on event.id (the idempotency key forwarded by the engine).

This module provides standard sink implementations with explicit contracts:
  - MemorySink: In-memory idempotent sink (stores effects once per event.id, tracked via seen set).
  - NaiveSink: Intentionally NON-idempotent sink for test comparisons (appends on every deliver attempt).
  - FileSink: Append-only JSON-lines file sink with configurable fsync, crash-consistent torn-tail
              recovery, fail-closed quarantine for internal corruption, and cross-process advisory locking
              with shared-index verification.
  - StdoutSink: Append-only output stream with optional in-memory process-local deduplication guard.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import sys
import time
from abc import ABC, abstractmethod
from typing import Optional, TextIO

from .models import Event, SinkError

# Platform-specific file locking support
_HAS_FCNTL = False
_HAS_MSVCRT = False
try:
    import fcntl
    _HAS_FCNTL = True
except ImportError:
    try:
        import msvcrt
        _HAS_MSVCRT = True
    except ImportError:
        pass


@contextlib.contextmanager
def _file_lock(fd: int, exclusive: bool = True):
    """Context manager for cross-platform advisory file locking."""
    if _HAS_FCNTL:
        flags = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        fcntl.flock(fd, flags)
        try:
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
    elif _HAS_MSVCRT:
        # msvcrt locking requires locking a specific byte range.
        # We lock 1 byte at offset 0 of the lock file.
        try:
            old_pos = os.lseek(fd, 0, os.SEEK_CUR)
        except OSError:
            old_pos = 0
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_LOCK, 1)
        try:
            yield
        finally:
            try:
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
                os.lseek(fd, old_pos, os.SEEK_SET)
            except OSError:
                pass
    else:
        # No OS locking primitive available
        yield


def _sync_parent_directory(path: str) -> None:
    """Best-effort synchronization of directory metadata (POSIX only).
    
    On POSIX systems (Linux, macOS), directory metadata synchronization after
    atomic rename (os.replace) is required to ensure the directory entry is durable
    across sudden power loss. On Windows/NTFS, opening directories with O_RDONLY
    raises PermissionError and NTFS journal updates handle metadata durability.
    """
    parent = os.path.dirname(os.path.abspath(path))
    if not parent or not hasattr(os, "O_DIRECTORY"):
        return
    try:
        flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            flags |= os.O_DIRECTORY
        dfd = os.open(parent, flags)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except OSError:
        pass


class Sink(ABC):
    """Abstract downstream sink interface.
    
    Implementations must raise SinkError when delivery fails or is rejected.
    """
    @abstractmethod
    async def deliver(self, event: Event) -> None: ...

    async def close(self) -> None:
        """Optional teardown hook."""
        pass


class MemorySink(Sink):
    """Idempotent in-memory sink (applies each event.id once).
    
    Guarantees:
      - Idempotent: Yes (within process lifetime).
      - Durable: No (lost on process termination).
      - Restart-safe: No.
      - Multi-process safe: No.
    """

    def __init__(self):
        self.effects: list[Event] = []
        self._seen: set[str] = set()
        self.attempts = 0

    async def deliver(self, event: Event) -> None:
        self.attempts += 1
        if event.id in self._seen:
            return
        self._seen.add(event.id)
        self.effects.append(event)

    def contains(self, event_id: str) -> bool:
        return event_id in self._seen

    def count(self) -> int:
        return len(self.effects)


class NaiveSink(Sink):
    """NON-idempotent sink - used in tests and benchmarks to demonstrate
    why downstream idempotency is required for exactly-once logical effects.
    
    Guarantees:
      - Idempotent: No (duplicate delivery creates duplicate effects).
      - Durable: No.
      - Restart-safe: No.
      - Multi-process safe: No.
    """

    def __init__(self):
        self.effects: list[Event] = []
        self.attempts = 0

    async def deliver(self, event: Event) -> None:
        self.attempts += 1
        self.effects.append(event)


class FileSink(Sink):
    """Idempotent JSON-lines file sink.
    
    Features & Guarantees:
      - Idempotent:
        - When `lock_file=False`: Idempotent within a single process.
        - When `lock_file=True`: Authoritative cross-process idempotency. Acquires
          an exclusive advisory lock, queries the shared persistent index, appends,
          and syncs atomically.
      - Durable:
        - When `fsync=True`: Invokes `os.fsync()` on file descriptor before acknowledging.
        - When `fsync=False`: Data is written to OS page cache; durable across process
          termination, but vulnerable to sudden machine power failure.
      - Restart-safe:
        - Torn-tail recovery: If an unclosed partial line exists at EOF from an interrupted write,
          it is cleanly truncated.
        - Internal corruption protection: If a corrupted record is detected in the middle of
          the file, FileSink fails closed by raising `SinkError` and preserving the original
          file in a `.quarantine.<timestamp>` backup to prevent silent deletion of historical
          deduplication evidence.
      - Multi-process safe: Yes, when `lock_file=True`.
    """

    def __init__(
        self,
        path: str,
        fsync: bool = False,
        lock_file: bool = False,
    ):
        self.path = os.path.abspath(path)
        self.fsync = fsync
        self.lock_file = lock_file
        self.lock_path = self.path + ".lock"
        self._seen: set[str] = set()
        self._lock = asyncio.Lock()
        self._f: Optional[TextIO] = None
        self._lock_fd: Optional[int] = None

        if os.path.dirname(self.path):
            os.makedirs(os.path.dirname(self.path), exist_ok=True)

        # Open dedicated advisory lock file if enabled
        if self.lock_file:
            self._lock_fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o666)

        # Reconstruct state from existing file, handling torn tails and quarantining internal corruption
        self._recover_state()

        # Open file in append mode
        self._f = open(self.path, "a", encoding="utf-8")

    def _recover_state(self) -> None:
        """Scan existing file lines, populate _seen set, and handle corruption safely."""
        if not os.path.exists(self.path):
            return

        # Acquire lock during state recovery if lock_file=True
        if self.lock_file and self._lock_fd is not None:
            with _file_lock(self._lock_fd, exclusive=True):
                self._recover_state_locked()
        else:
            self._recover_state_locked()

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

        # Check if the ONLY corruption is an incomplete/torn trailing record at the very end (EOF)
        if len(corrupted_indices) == 1 and corrupted_indices[0] == last_idx:
            # Trailing torn tail: safely truncate the incomplete trailing record
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

        # If corruption is detected internally (not merely at EOF), fail closed!
        # Quarantining preserves the evidence of historical acknowledgements
        quarantine_path = f"{self.path}.quarantine.{int(time.time() * 1000)}"
        shutil.copy2(self.path, quarantine_path)
        raise SinkError(
            f"FileSink({self.path}) detected internal corrupted records at lines {corrupted_indices}. "
            f"Original file preserved and quarantined to {quarantine_path}. Refusing to start to prevent "
            f"silent loss of deduplication history."
        )

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
            # In-memory fast path check
            if event.id in self._seen:
                return

            if self._f is None or self._f.closed:
                raise SinkError(f"FileSink({self.path}) is closed")

            line = event.to_json() + "\n"

            try:
                if self.lock_file and self._lock_fd is not None:
                    # Multi-process mutual exclusion
                    with _file_lock(self._lock_fd, exclusive=True):
                        # Authoritative cross-process deduplication: check disk state while holding lock
                        if self._is_event_on_disk_locked(event.id):
                            self._seen.add(event.id)
                            return

                        self._f.seek(0, os.SEEK_END)
                        self._f.write(line)
                        self._f.flush()
                        if self.fsync:
                            os.fsync(self._f.fileno())
                else:
                    # Single-process mode
                    self._f.seek(0, os.SEEK_END)
                    self._f.write(line)
                    self._f.flush()
                    if self.fsync:
                        os.fsync(self._f.fileno())

                self._seen.add(event.id)
            except OSError as e:
                raise SinkError(f"FileSink write failed: {e}") from e

    async def close(self) -> None:
        async with self._lock:
            if self._f and not self._f.closed:
                try:
                    self._f.flush()
                    if self.fsync:
                        os.fsync(self._f.fileno())
                    self._f.close()
                except OSError:
                    pass
                finally:
                    self._f = None

            if self._lock_fd is not None:
                try:
                    os.close(self._lock_fd)
                except OSError:
                    pass
                finally:
                    self._lock_fd = None


class StdoutSink(Sink):
    """Standard output stream sink.
    
    Guarantees:
      - Idempotent:
        - If `dedup=False` (default): NON-idempotent. All deliver calls print to stdout.
        - If `dedup=True`: Best-effort in-memory process-local deduplication.
      - Durable: No (stdout is an external ephemeral output stream).
      - Restart-safe: No (in-memory seen filter is lost on restart).
      - Multi-process safe: No.
    
    NOTE: Exactly-once delivery cannot be guaranteed to stdout because stdout
    lacks an acknowledgement or transactional rollback protocol.
    """

    def __init__(self, dedup: bool = False, max_history: int = 10000):
        self.dedup = dedup
        self.max_history = max_history
        self._seen: set[str] = set()
        self.attempts = 0

    async def deliver(self, event: Event) -> None:
        self.attempts += 1
        if self.dedup:
            if event.id in self._seen:
                return
            if len(self._seen) >= self.max_history:
                # Bounded cache eviction (clear oldest to bound memory)
                self._seen.clear()
            self._seen.add(event.id)

        print("DELIVER", event.to_json(), flush=True)
