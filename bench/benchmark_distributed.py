"""
Distributed Architecture Benchmark: Single vs 2 vs 4 Multi-Partition Instances.
Measures:
- Throughput (events/sec)
- Latency percentiles (p50, p95, p99)
- Duplicate detection & exactly-once preservation
- Partition distribution uniformity
- Buffer utilization
"""
from __future__ import annotations

import argparse
import asyncio
import os
import random
import statistics
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import redis.asyncio as redis

from dedup_engine import (
    Event,
    MemorySink,
    PartitionManager,
    RedisKV,
    Status,
    compute_partition,
)

REDIS_URL = "redis://127.0.0.1:6380/0"


async def run_distributed_workload(
    num_instances: int,
    num_partitions: int,
    total_events: int = 5000,
    dup_ratio: float = 0.3,
) -> dict:
    # 1. Clean Redis
    r = redis.from_url(REDIS_URL)
    await r.flushdb()
    await r.aclose()

    # 2. Setup sinks and instances
    sinks: Dict[int, MemorySink] = {p: MemorySink() for p in range(num_partitions)}
    kvs = [RedisKV(REDIS_URL) for _ in range(num_instances)]
    managers = [
        PartitionManager(
            instance_id=f"inst-{i}",
            kv=kvs[i],
            sink_factory=lambda p_id: sinks[p_id],
            num_partitions=num_partitions,
            lease_ttl_ms=4000,
            heartbeat_interval_ms=1000,
            lateness=0.0,
            window=60.0,
        )
        for i in range(num_instances)
    ]

    # Register in-process peer registry so co-located instances route directly
    peer_reg = {pm.instance_id: pm for pm in managers}
    for pm in managers:
        pm._in_process_registry = peer_reg

    # Start all managers
    for pm in managers:
        await pm.start()

    # Wait for partition distribution to stabilize
    await asyncio.sleep(1.0)

    # 3. Generate workload
    unique_count = max(1, int(total_events * (1.0 - dup_ratio)))
    now = time.time()
    base_events = [Event(f"dist-evt-{i}", now + i * 0.001, {"seq": i}) for i in range(unique_count)]
    rng = random.Random(42)
    stream = base_events + [rng.choice(base_events) for _ in range(total_events - unique_count)]
    rng.shuffle(stream)

    # 4. Execute ingestion across instances (round-robin submission to simulate load balancer)
    latencies = []
    t_start = time.perf_counter()

    for idx, ev in enumerate(stream):
        target_pm = managers[idx % num_instances]
        t0 = time.perf_counter()
        st, p_id, _ = await target_pm.ingest_event(ev)
        lat = (time.perf_counter() - t0) * 1000.0
        latencies.append(lat)

    # 5. Drain all partitions
    for pm in managers:
        for p_state in pm.partitions.values():
            if p_state.is_owner and p_state.engine:
                await p_state.engine.drain()

    elapsed = time.perf_counter() - t_start

    # 6. Stop managers
    for pm in managers:
        await pm.stop()
    for kv in kvs:
        await kv.close()

    # 7. Collect results
    all_effects = []
    partition_counts = {}
    for p, s in sinks.items():
        eff = [x.id for x in s.effects]
        all_effects.extend(eff)
        partition_counts[p] = len(eff)

    latencies.sort()
    p50 = statistics.median(latencies)
    p95 = latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))]
    p99 = latencies[min(len(latencies) - 1, int(len(latencies) * 0.99))]
    throughput = round(total_events / max(elapsed, 1e-9), 1)

    exactly_once = len(all_effects) == len(set(all_effects)) == unique_count

    return {
        "instances": num_instances,
        "partitions": num_partitions,
        "total_events": total_events,
        "unique_expected": unique_count,
        "sink_effects": len(all_effects),
        "exactly_once": exactly_once,
        "throughput_eps": throughput,
        "p50_ms": round(p50, 3),
        "p95_ms": round(p95, 3),
        "p99_ms": round(p99, 3),
        "partition_distribution": partition_counts,
    }


async def main():
    print("=" * 78)
    print("           DISTRIBUTED ENGINE BENCHMARK (1 vs 2 vs 4 Instances)")
    print("=" * 78)

    results = []
    for num_inst in [1, 2, 4]:
        print(f"Running benchmark with {num_inst} instance(s) across 4 partitions...")
        res = await run_distributed_workload(num_instances=num_inst, num_partitions=4, total_events=2000)
        results.append(res)
        print(f"  -> Throughput: {res['throughput_eps']} eps | p50: {res['p50_ms']}ms | p95: {res['p95_ms']}ms | Exactly-Once: {res['exactly_once']}")

    print("\nSummary Table:")
    print(f"{'Instances':>10} | {'Partitions':>10} | {'Throughput (eps)':>16} | {'p50 (ms)':>10} | {'p95 (ms)':>10} | {'Exactly-Once':>12}")
    print("-" * 78)
    for r in results:
        print(f"{r['instances']:>10} | {r['partitions']:>10} | {r['throughput_eps']:>16} | {r['p50_ms']:>10} | {r['p95_ms']:>10} | {str(r['exactly_once']):>12}")


if __name__ == "__main__":
    asyncio.run(main())
