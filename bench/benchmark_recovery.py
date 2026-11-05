"""Recovery performance benchmark for Phase 3C (Dedicated Pending Index & Bounded Recovery).

Measures recovery time, round trips, memory/heap usage, and throughput across:
  - 1,000 pending records
  - 10,000 pending records
  - 100,000 pending records
"""

from __future__ import annotations

import asyncio
import gc
import json
import os
import sys
import time
import tracemalloc

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dedup_engine.engine import DedupEngine
from dedup_engine.kv import RedisKV
from dedup_engine.sink import MemorySink

REDIS_URL = "redis://localhost:6380/0"


async def populate_pending_records(kv: RedisKV, n: int, batch_size: int = 5000):
    print(f"Populating {n:,} pending records into Redis...")
    for batch_start in range(0, n, batch_size):
        batch_end = min(batch_start + batch_size, n)
        pipe = kv._r.pipeline(transaction=False)
        for i in range(batch_start, batch_end):
            key = f"dedup:bench-evt-{i}"
            val = json.dumps({
                "id": f"bench-evt-{i}",
                "ts": 1000.0 + (i * 0.001),
                "token": f"tok-{i}",
                "state": "pending",
                "claimed_at": 1000.0 + (i * 0.001),
                "event": {
                    "id": f"bench-evt-{i}",
                    "ts": 1000.0 + (i * 0.001),
                    "payload": {"seq": i, "data": "payload-x" * 5},
                },
            })
            pipe.set(key, val)
            pipe.zadd("idx:dedup:pending", {key: 1000.0 + (i * 0.001)})
        await pipe.execute()
    print(f"Finished populating {n:,} records.")


async def run_single_benchmark(n: int, recovery_batch_size: int = 512):
    kv = RedisKV(REDIS_URL)
    try:
        await kv._r.flushdb()
        await populate_pending_records(kv, n)

        initial_pending = await kv._r.zcard("idx:dedup:pending")
        assert initial_pending == n, f"Expected {n} pending keys in index, got {initial_pending}"

        # Bound buffer size to test bounded heap ingestion
        max_buf = 5000 if n > 5000 else None
        sink = MemorySink()
        engine = DedupEngine(
            kv=kv,
            sink=sink,
            window=3600.0,
            max_buffer_size=max_buf,
            recovery_batch_size=recovery_batch_size,
        )

        gc.collect()
        tracemalloc.start()
        start_time = time.perf_counter()

        # Run recovery
        recovered_count = await engine.recover()

        duration = time.perf_counter() - start_time
        current_mem, peak_mem = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        heap_size = len(engine._heap)
        eps = recovered_count / duration if duration > 0 else 0

        # Drain heap to sink to verify integrity
        await engine.drain()
        sink_count = len(sink.effects)

        result = {
            "n_pending": n,
            "recovered": recovered_count,
            "sink_delivered": sink_count,
            "duration_s": round(duration, 4),
            "throughput_eps": round(eps, 1),
            "peak_mem_mb": round(peak_mem / (1024 * 1024), 2),
            "peak_heap_len": heap_size,
            "batch_size": recovery_batch_size,
            "max_buffer_size": max_buf,
        }
        return result
    finally:
        await kv.close()


async def main():
    print("=" * 75)
    print("PHASE 3C: BOUNDED INCREMENTAL RECOVERY BENCHMARK")
    print("=" * 75)

    test_sizes = [1000, 10000, 100000]
    results = []

    for size in test_sizes:
        print(f"\n--- Testing Recovery with {size:,} Pending Records ---")
        res = await run_single_benchmark(size, recovery_batch_size=512)
        results.append(res)
        print(f"Results for {size:,} records:")
        print(f"  Duration:         {res['duration_s']} s")
        print(f"  Throughput:       {res['throughput_eps']:,} events/sec")
        print(f"  Recovered Count:  {res['recovered']:,}")
        print(f"  Delivered Count:  {res['sink_delivered']:,}")
        print(f"  Peak Heap Size:   {res['peak_heap_len']:,} (Max Buffer: {res['max_buffer_size']})")
        print(f"  Peak Traced Mem:  {res['peak_mem_mb']} MB")

    print("\n" + "=" * 75)
    print("SUMMARY RESULTS TABLE")
    print("=" * 75)
    header = f"{'Pending Records':>16} | {'Batch':>6} | {'Duration (s)':>13} | {'Throughput (eps)':>16} | {'Peak Heap':>10} | {'Peak Mem (MB)':>14}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['n_pending']:>16,}"
            f" | {r['batch_size']:>6}"
            f" | {r['duration_s']:>13.4f}"
            f" | {r['throughput_eps']:>16,.1f}"
            f" | {r['peak_heap_len']:>10,}"
            f" | {r['peak_mem_mb']:>14.2f}"
        )
    print("=" * 75)

    os.makedirs("bench/results", exist_ok=True)
    with open("bench/results/recovery_benchmark.json", "w") as f:
        json.dump(results, f, indent=2)
    print("\nSaved benchmark metrics to bench/results/recovery_benchmark.json")


if __name__ == "__main__":
    asyncio.run(main())
