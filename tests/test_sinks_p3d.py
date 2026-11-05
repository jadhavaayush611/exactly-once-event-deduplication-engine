"""Tests for Phase 3D: Downstream Sink Reliability, Idempotency & Delivery Guarantees.

Covers:
 1. Duplicate event delivery to each sink.
 2. Distinct event IDs.
 3. Retry after sink exceptions.
 4. FileSink restart and reconstruction.
 5. Partial or incomplete trailing file records (torn-tail recovery).
 6. Process interruption during writes / close behavior.
 7. Flush and fsync behavior.
 8. Corrupted records and recovery behavior.
 9. Concurrent writes with advisory file locking.
10. Exactly-once logical effects with an idempotent sink (MemorySink, FileSink).
11. Duplicate effects with NaiveSink under retry, demonstrating its limitation.
12. StdoutSink's documented behavior (dedup=False vs dedup=True with bounded cache).
13. Engine-to-sink lifecycle: sink failure prevents premature commit; crash between deliver
    and commit redelivers without duplicate effects on idempotent sink.
14. Regression checks for Phases 3A, 3B, and 3C.
"""
from __future__ import annotations

import asyncio
import io
import json
import os
import sys
import tempfile
from contextlib import redirect_stdout
from typing import List

import pytest

from dedup_engine.engine import DedupEngine
from dedup_engine.faults import CrashInjector, FaultyKV, FaultySink
from dedup_engine.kv import MemoryKV
from dedup_engine.models import Event, KVError, SimulatedCrash, SinkError, Status
from dedup_engine.sink import FileSink, MemorySink, NaiveSink, Sink, StdoutSink


# ---------------------------------------------------------------------------
# Test 1: Duplicate event delivery to MemorySink and FileSink
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_duplicate_event_delivery_to_idempotent_sinks(tmp_path):
    ev1 = Event(id="evt-dup-1", ts=100.0, data={"k": "v1"})

    # MemorySink
    mem = MemorySink()
    await mem.deliver(ev1)
    await mem.deliver(ev1)
    await mem.deliver(ev1)
    assert mem.attempts == 3
    assert len(mem.effects) == 1
    assert mem.effects[0].id == "evt-dup-1"

    # FileSink
    f_path = str(tmp_path / "sink_dup.jsonl")
    fs = FileSink(f_path, fsync=False)
    await fs.deliver(ev1)
    await fs.deliver(ev1)
    await fs.deliver(ev1)
    await fs.close()

    with open(f_path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["id"] == "evt-dup-1"


# ---------------------------------------------------------------------------
# Test 2: Distinct event IDs are independently deliverable
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_distinct_event_ids_delivered(tmp_path):
    events = [Event(id=f"evt-{i}", ts=100.0 + i, data={"idx": i}) for i in range(5)]

    mem = MemorySink()
    f_path = str(tmp_path / "sink_distinct.jsonl")
    fs = FileSink(f_path, fsync=True)

    for ev in events:
        await mem.deliver(ev)
        await fs.deliver(ev)
    await fs.close()

    assert len(mem.effects) == 5
    assert [e.id for e in mem.effects] == [f"evt-{i}" for i in range(5)]

    with open(f_path, "r", encoding="utf-8") as f:
        disk_ids = [json.loads(line)["id"] for line in f if line.strip()]
    assert disk_ids == [f"evt-{i}" for i in range(5)]


# ---------------------------------------------------------------------------
# Test 3: Retry after sink exceptions
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_retry_after_sink_exceptions():
    class FlakySink(Sink):
        def __init__(self, inner: MemorySink):
            self.inner = inner
            self.fail_count = 2

        async def deliver(self, event: Event) -> None:
            if self.fail_count > 0:
                self.fail_count -= 1
                raise SinkError("transient downstream timeout")
            await self.inner.deliver(event)

    mem = MemorySink()
    flaky = FlakySink(mem)
    kv = MemoryKV()
    engine = DedupEngine(kv=kv, sink=flaky, max_retries=5)

    ev = Event(id="flaky-1", ts=100.0, data={})
    st = await engine.ingest(ev)
    assert st == Status.ACCEPTED

    # Drain to trigger delivery and retries
    await engine.drain()
    assert len(mem.effects) == 1
    assert mem.effects[0].id == "flaky-1"
    assert engine.m.sink_retries == 2


# ---------------------------------------------------------------------------
# Test 4: FileSink restart and reconstruction
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_filesink_restart_and_reconstruction(tmp_path):
    f_path = str(tmp_path / "sink_restart.jsonl")

    # Instance 1: write 3 events
    fs1 = FileSink(f_path, fsync=True)
    await fs1.deliver(Event(id="e1", ts=10.0, data={}))
    await fs1.deliver(Event(id="e2", ts=20.0, data={}))
    await fs1.deliver(Event(id="e3", ts=30.0, data={}))
    await fs1.close()

    # Instance 2 (simulating restart / reboot):
    fs2 = FileSink(f_path, fsync=True)
    assert fs2._seen == {"e1", "e2", "e3"}

    # Resend e2 and e3 -> must be skipped
    await fs2.deliver(Event(id="e2", ts=20.0, data={}))
    await fs2.deliver(Event(id="e3", ts=30.0, data={}))

    # Send new event e4
    await fs2.deliver(Event(id="e4", ts=40.0, data={}))
    await fs2.close()

    with open(f_path, "r", encoding="utf-8") as f:
        ids_on_disk = [json.loads(line)["id"] for line in f if line.strip()]
    assert ids_on_disk == ["e1", "e2", "e3", "e4"]


# ---------------------------------------------------------------------------
# Test 5: Partial or incomplete trailing file records (torn-tail recovery)
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_filesink_torn_tail_recovery(tmp_path):
    f_path = str(tmp_path / "sink_torn.jsonl")

    # Manually write two valid records followed by an incomplete/corrupted trailing line
    with open(f_path, "w", encoding="utf-8") as f:
        f.write(json.dumps({"id": "valid-1", "ts": 10.0, "data": {}}) + "\n")
        f.write(json.dumps({"id": "valid-2", "ts": 20.0, "data": {}}) + "\n")
        f.write('{"id": "valid-3", "ts": 30.0, "data": {"partial_str')  # torn write from hard crash

    # Starting FileSink should recover valid-1 and valid-2, and truncate the torn tail
    fs = FileSink(f_path, fsync=True)
    assert fs._seen == {"valid-1", "valid-2"}
    assert "valid-3" not in fs._seen

    # Now deliver valid-3 cleanly and valid-4
    await fs.deliver(Event(id="valid-3", ts=30.0, data={"fixed": True}))
    await fs.deliver(Event(id="valid-4", ts=40.0, data={}))
    await fs.close()

    with open(f_path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    assert len(lines) == 4
    recovered_ids = [json.loads(line)["id"] for line in lines]
    assert recovered_ids == ["valid-1", "valid-2", "valid-3", "valid-4"]


# ---------------------------------------------------------------------------
# Test 6: FileSink close behavior and writes to closed sink
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_filesink_close_behavior(tmp_path):
    f_path = str(tmp_path / "sink_closed.jsonl")
    fs = FileSink(f_path)
    await fs.deliver(Event(id="a", ts=1.0, data={}))
    await fs.close()

    # Attempting to deliver to a closed sink must raise SinkError
    with pytest.raises(SinkError):
        await fs.deliver(Event(id="b", ts=2.0, data={}))


# ---------------------------------------------------------------------------
# Test 7: Flush and fsync behavior
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_filesink_fsync_flag(tmp_path):
    f_path_sync = str(tmp_path / "sync.jsonl")
    f_path_nosync = str(tmp_path / "nosync.jsonl")

    fs_sync = FileSink(f_path_sync, fsync=True)
    fs_nosync = FileSink(f_path_nosync, fsync=False)

    ev = Event(id="sync-test", ts=1.0, data={})
    await fs_sync.deliver(ev)
    await fs_nosync.deliver(ev)

    await fs_sync.close()
    await fs_nosync.close()

    assert os.path.exists(f_path_sync)
    assert os.path.exists(f_path_nosync)


# ---------------------------------------------------------------------------
# Test 8: Corrupted internal lines triggers fail-closed quarantine
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_filesink_internal_corrupted_lines_quarantine_and_fail_closed(tmp_path):
    f_path = str(tmp_path / "corrupted.jsonl")
    with open(f_path, "w", encoding="utf-8") as f:
        f.write(json.dumps({"id": "good-1", "ts": 1.0, "data": {}}) + "\n")
        f.write("GARBAGE LINE NOT JSON\n")
        f.write(json.dumps({"id": "good-2", "ts": 2.0, "data": {}}) + "\n")

    # Initializing FileSink must fail closed by raising SinkError to prevent silent data erasure
    with pytest.raises(SinkError) as exc_info:
        FileSink(f_path)
    assert "quarantine" in str(exc_info.value).lower()

    # The original file remains untouched and a quarantine backup exists
    assert os.path.exists(f_path)
    quarantine_files = [f for f in os.listdir(tmp_path) if "quarantine" in f]
    assert len(quarantine_files) >= 1

    # Verify original file content is completely intact (never silently deleted)
    with open(f_path, "r", encoding="utf-8") as f:
        content = f.read()
    assert "GARBAGE LINE NOT JSON" in content
    assert "good-1" in content
    assert "good-2" in content


# ---------------------------------------------------------------------------
# Test 9: Concurrent writes with advisory file locking
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_filesink_concurrent_writers(tmp_path):
    f_path = str(tmp_path / "concurrent.jsonl")
    fs = FileSink(f_path, lock_file=True, fsync=False)

    async def writer_task(start_idx: int, count: int):
        for i in range(start_idx, start_idx + count):
            await fs.deliver(Event(id=f"c-{i}", ts=100.0 + i, data={"val": i}))

    # Launch 5 concurrent coroutines writing 20 items each
    tasks = [asyncio.create_task(writer_task(i * 20, 20)) for i in range(5)]
    await asyncio.gather(*tasks)
    await fs.close()

    with open(f_path, "r", encoding="utf-8") as f:
        records = [json.loads(line) for line in f if line.strip()]
    assert len(records) == 100
    ids = {r["id"] for r in records}
    assert len(ids) == 100


# ---------------------------------------------------------------------------
# Test 10: Exactly-once logical effects with idempotent sinks
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_exactly_once_logical_effects_with_idempotent_sink():
    kv = MemoryKV()
    mem = MemorySink()
    engine = DedupEngine(kv=kv, sink=mem)

    # Ingest event 1
    ev1 = Event(id="eo-1", ts=100.0, data={})
    st1 = await engine.ingest(ev1)
    assert st1 == Status.ACCEPTED

    # Re-ingest duplicate
    st2 = await engine.ingest(ev1)
    assert st2 == Status.DUPLICATE

    await engine.drain()
    assert len(mem.effects) == 1
    assert mem.effects[0].id == "eo-1"


# ---------------------------------------------------------------------------
# Test 11: Duplicate effects with NaiveSink under retry, demonstrating its limitation
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_naive_sink_shows_duplicate_effects_under_retry():
    # If a crash happens after deliver but before commit, or if the sink raises a false
    # error after applying the effect, recovery / retry re-delivers.
    # Because NaiveSink is NOT idempotent, it records duplicate side-effects!
    kv = MemoryKV()
    naive = NaiveSink()
    injector = CrashInjector("after_deliver", after=1)
    engine = DedupEngine(kv=kv, sink=naive, crash_hook=injector)

    ev = Event(id="naive-dup", ts=100.0, data={})
    await engine.ingest(ev)
    with pytest.raises(SimulatedCrash):
        await engine.drain()

    assert len(naive.effects) == 1

    # Reboot and recover
    engine2 = DedupEngine(kv=kv, sink=naive)
    recovered = await engine2.recover()
    assert recovered == 1
    await engine2.drain()

    # Now naive has duplicate side-effects (2 deliveries)!
    assert len(naive.effects) == 2
    assert naive.effects[0].id == naive.effects[1].id == "naive-dup"


# ---------------------------------------------------------------------------
# Test 12: StdoutSink documented behavior
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_stdout_sink_behavior():
    ev = Event(id="stdout-1", ts=1.0, data={})

    # Default: dedup=False -> outputs every time
    sink_default = StdoutSink(dedup=False)
    buf = io.StringIO()
    with redirect_stdout(buf):
        await sink_default.deliver(ev)
        await sink_default.deliver(ev)
    lines = [line.strip() for line in buf.getvalue().splitlines() if line.strip()]
    assert len(lines) == 2
    assert sink_default.attempts == 2

    # Process-local dedup=True -> deduplicates within process
    sink_dedup = StdoutSink(dedup=True, max_history=10)
    buf2 = io.StringIO()
    with redirect_stdout(buf2):
        await sink_dedup.deliver(ev)
        await sink_dedup.deliver(ev)
    lines2 = [line.strip() for line in buf2.getvalue().splitlines() if line.strip()]
    assert len(lines2) == 1
    assert sink_dedup.attempts == 2


# ---------------------------------------------------------------------------
# Test 13: Engine-to-sink lifecycle: sink failure prevents premature commit
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_sink_failure_prevents_premature_commit():
    class FailingSink(Sink):
        async def deliver(self, event: Event) -> None:
            raise SinkError("sink is offline")

    kv = MemoryKV()
    failing = FailingSink()
    engine = DedupEngine(kv=kv, sink=failing, max_retries=1)

    ev = Event(id="fail-commit", ts=100.0, data={})
    st = await engine.ingest(ev)
    assert st == Status.ACCEPTED

    # Reorder buffer flush fails due to sink error
    left = await engine.drain()
    assert left == 1

    # Record in KV store MUST still be PENDING, not COMMITTED!
    raw = await kv.get("dedup:fail-commit")
    assert raw is not None
    rec = json.loads(raw)
    assert rec["state"] == "pending"


# ---------------------------------------------------------------------------
# Test 14: Crash between sink deliver and KV commit redelivers safely with idempotent sink
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_crash_between_deliver_and_commit_redelivers_safely(tmp_path):
    f_path = str(tmp_path / "crash_commit.jsonl")
    sink = FileSink(f_path, fsync=True)
    kv = MemoryKV()

    # Crash after delivery, before commit
    injector = CrashInjector(point="after_deliver", after=1)
    engine = DedupEngine(kv=kv, sink=sink, crash_hook=injector)

    ev = Event(id="crash-after-del", ts=100.0, data={})
    await engine.ingest(ev)

    # Ingestion or drain will trigger crash after deliver
    with pytest.raises(SimulatedCrash):
        await engine.drain()

    # Disk already has the event written by FileSink!
    with open(f_path, "r", encoding="utf-8") as f:
        disk_records = [json.loads(l) for l in f if l.strip()]
    assert len(disk_records) == 1
    assert disk_records[0]["id"] == "crash-after-del"

    # In KV, event remains pending because commit never happened before crash
    rec = json.loads(await kv.get("dedup:crash-after-del"))
    assert rec["state"] == "pending"

    # Recover with a new engine instance
    recovered_sink = FileSink(f_path, fsync=True)
    recovery_engine = DedupEngine(kv=kv, sink=recovered_sink)
    recovered = await recovery_engine.recover()
    assert recovered == 1

    # Flush recovered event to sink
    await recovery_engine.drain()

    # The file still has exactly 1 record! Zero duplicate effects!
    with open(f_path, "r", encoding="utf-8") as f:
        final_records = [json.loads(l) for l in f if l.strip()]
    assert len(final_records) == 1
    assert final_records[0]["id"] == "crash-after-del"

    # Now the KV record is committed!
    rec2 = json.loads(await kv.get("dedup:crash-after-del"))
    assert rec2["state"] == "committed"
    await recovered_sink.close()


# ---------------------------------------------------------------------------
# Test 15: FileSink empty file and non-existent parent directories
# ---------------------------------------------------------------------------
@pytest.mark.anyio
async def test_filesink_creates_parent_directories_cleanly(tmp_path):
    deep_path = str(tmp_path / "deep" / "nested" / "dir" / "out.jsonl")
    fs = FileSink(deep_path)
    await fs.deliver(Event(id="deep-1", ts=10.0, data={}))
    await fs.close()

    assert os.path.exists(deep_path)
    with open(deep_path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]
    assert len(lines) == 1
    assert json.loads(lines[0])["id"] == "deep-1"


# ---------------------------------------------------------------------------
# Test 16: DEF-3D-01 Independent multi-process duplicate write to FileSink
# ---------------------------------------------------------------------------
def _mp_worker(path: str, event_id: str, barrier, res_list):
    import asyncio
    async def run():
        fs = FileSink(path, lock_file=True, fsync=True)
        barrier.wait()  # Both processes initialize and reach barrier before either writes
        await fs.deliver(Event(id=event_id, ts=100.0, data={"pid": os.getpid()}))
        await fs.close()
        res_list.append(os.getpid())
    asyncio.run(run())


def test_filesink_cross_process_idempotency_two_processes(tmp_path):
    import multiprocessing as mp
    f_path = str(tmp_path / "mp_dup.jsonl")
    barrier = mp.Barrier(2)
    manager = mp.Manager()
    res_list = manager.list()

    p1 = mp.Process(target=_mp_worker, args=(f_path, "shared-evt-1", barrier, res_list))
    p2 = mp.Process(target=_mp_worker, args=(f_path, "shared-evt-1", barrier, res_list))
    p1.start()
    p2.start()
    p1.join(timeout=10)
    p2.join(timeout=10)

    assert len(res_list) == 2  # Both processes ran successfully

    with open(f_path, "r", encoding="utf-8") as f:
        lines = [l.strip() for l in f if l.strip()]

    # Critical Assertion: Exactly ONE physical JSONL record for shared-evt-1 on disk!
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["id"] == "shared-evt-1"


# ---------------------------------------------------------------------------
# Test 17: DEF-3D-01 Multi-process contention with 4 processes and 10 shared IDs
# ---------------------------------------------------------------------------
def _mp_contention_worker(path: str, event_ids: list[str], barrier):
    import asyncio
    async def run():
        fs = FileSink(path, lock_file=True, fsync=True)
        barrier.wait()
        for eid in event_ids:
            await fs.deliver(Event(id=eid, ts=100.0, data={"pid": os.getpid()}))
        await fs.close()
    asyncio.run(run())


def test_filesink_cross_process_contention_four_processes(tmp_path):
    import multiprocessing as mp
    f_path = str(tmp_path / "mp_contention.jsonl")
    barrier = mp.Barrier(4)
    shared_ids = [f"shared-id-{i}" for i in range(10)]

    procs = [mp.Process(target=_mp_contention_worker, args=(f_path, shared_ids, barrier)) for _ in range(4)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=15)

    with open(f_path, "r", encoding="utf-8") as f:
        records = [json.loads(l) for l in f if l.strip()]

    # Exactly 10 records on disk for 10 unique IDs, 0 duplicates despite 4 competing processes!
    assert len(records) == 10
    disk_ids = [r["id"] for r in records]
    assert sorted(disk_ids) == sorted(shared_ids)


# ---------------------------------------------------------------------------
# Test 18: DEF-3D-02 Mid-file corruption preserves original and quarantine files
# ---------------------------------------------------------------------------
def test_filesink_mid_file_corruption_preserves_original_and_quarantine(tmp_path):
    f_path = str(tmp_path / "mid_corrupt.jsonl")
    with open(f_path, "w", encoding="utf-8") as f:
        f.write(json.dumps({"id": "ack-1", "ts": 1.0, "data": {}}) + "\n")
        f.write("{BAD_JSON_MIDDLE\n")
        f.write(json.dumps({"id": "ack-2", "ts": 2.0, "data": {}}) + "\n")

    # Starting FileSink must raise SinkError and create a quarantine copy
    with pytest.raises(SinkError) as exc_info:
        FileSink(f_path)
    assert "quarantine" in str(exc_info.value).lower()

    # Verify original file is still present and NOT modified
    with open(f_path, "r", encoding="utf-8") as f:
        content = f.read()
    assert "ack-1" in content
    assert "{BAD_JSON_MIDDLE" in content
    assert "ack-2" in content

    # Verify quarantine file exists
    quarantines = [f for f in os.listdir(tmp_path) if f.startswith("mid_corrupt.jsonl.quarantine")]
    assert len(quarantines) == 1
    with open(str(tmp_path / quarantines[0]), "r", encoding="utf-8") as qf:
        q_content = qf.read()
    assert q_content == content
