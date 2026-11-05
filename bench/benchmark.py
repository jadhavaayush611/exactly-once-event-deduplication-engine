"""Benchmarks: throughput/latency vs KV RTT & concurrency, duplicate ratio, window state size,
and the cost of fault injection.   python bench/benchmark.py [--quick] [--n 4000]"""
import argparse
import asyncio
import csv
import os
import random
import statistics
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dedup_engine import DedupEngine, Event, FaultyKV, KVError, MemoryKV, MemorySink  # noqa: E402

OUT = os.path.join(os.path.dirname(__file__), "results")


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))]


async def run_case(*, n, dup_ratio=0.3, latency=0.0, concurrency=64, window=60.0, lateness=1.0,
                   fail=0.0, ack_loss=0.0, spacing=0.001, seed=1):
    rng = random.Random(seed)
    uniq = max(1, int(n * (1 - dup_ratio)))
    base = [Event(f"ev-{i}", i * spacing, i) for i in range(uniq)]
    stream = base + [rng.choice(base) for _ in range(n - uniq)]
    stream.sort(key=lambda e: e.ts + rng.uniform(0, lateness * 0.5))
    mem = MemoryKV(latency)
    kv = FaultyKV(mem, fail, ack_loss, seed) if (fail or ack_loss) else mem
    sink = MemorySink()
    eng = DedupEngine(kv, sink, window=window, lateness=lateness)
    sem, lat, peak = asyncio.Semaphore(concurrency), [], 0

    async def one(e):
        nonlocal peak
        async with sem:
            t0 = time.perf_counter()
            while True:
                try:
                    await eng.ingest(e)
                    break
                except KVError:
                    pass
            lat.append(time.perf_counter() - t0)
            peak = max(peak, len(mem._d))

    t0 = time.perf_counter()
    await asyncio.gather(*[asyncio.create_task(one(e)) for e in stream])
    for _ in range(200):               # under heavy faults one drain pass may leave stragglers
        if await eng.drain() == 0:
            break
    dt = time.perf_counter() - t0
    got = [e.id for e in sink.effects]
    ok = len(got) == len(set(got)) == uniq
    return dict(events=n, dup_ratio=dup_ratio, kv_latency_ms=latency * 1000, concurrency=concurrency,
                window_s=window, fail_rate=fail, ack_loss=ack_loss, seconds=round(dt, 3),
                throughput_eps=round(n / dt), p50_ms=round(pct(lat, .5) * 1000, 2),
                p95_ms=round(pct(lat, .95) * 1000, 2), p99_ms=round(pct(lat, .99) * 1000, 2),
                kv_ops_per_event=round(mem.ops / n, 2), peak_kv_keys=peak,
                dups_detected=eng.m.duplicates, retries=eng.m.kv_retries, exactly_once=ok)


def table(title, rows, cols):
    print(f"\n=== {title} ===")
    print(" | ".join(f"{c:>15}" for c in cols))
    for r in rows:
        print(" | ".join(f"{str(r[c]):>15}" for c in cols))


def save(name, rows):
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, name + ".csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


async def main(n, quick):
    cols = ["kv_latency_ms", "concurrency", "throughput_eps", "p50_ms", "p95_ms", "p99_ms",
            "kv_ops_per_event", "exactly_once"]
    # A: KV round-trip x concurrency
    A = []
    for lat in [0.0, 0.0005, 0.002]:
        for c in [1, 16, 128]:
            nn = min(n, 1500) if (lat > 0 and c == 1) else n
            A.append(await run_case(n=nn, latency=lat, concurrency=c))
    table("A) throughput & latency vs KV RTT and concurrency", A, cols)
    save("A_latency_concurrency", A)
    # B: duplicate ratio
    B = [await run_case(n=n, dup_ratio=d, latency=0.0005, concurrency=64) for d in [0, .25, .5, .9]]
    table("B) duplicate ratio (KV RTT 0.5ms, concurrency 64)", B,
          ["dup_ratio", "throughput_eps", "p95_ms", "dups_detected", "kv_ops_per_event", "exactly_once"])
    save("B_duplicate_ratio", B)
    # C: window vs state
    C = [await run_case(n=n * 2, window=w, spacing=0.05, latency=0.0, concurrency=64)
         for w in [5, 30, 120, 600]]
    table("C) sliding-window size vs KV state (event spacing 50ms)", C,
          ["window_s", "peak_kv_keys", "throughput_eps", "exactly_once"])
    save("C_window_state", C)
    # D: fault overhead
    D = []
    for f, a in [(0, 0), (.01, .01), (.05, .05), (.10, .10), (.20, .20)]:
        D.append(await run_case(n=n, latency=0.0005, concurrency=64, fail=f, ack_loss=a))
    table("D) throughput under injected KV faults (request-loss + ack-loss)", D,
          ["fail_rate", "ack_loss", "throughput_eps", "p99_ms", "retries", "exactly_once"])
    save("D_fault_overhead", D)
    assert all(r["exactly_once"] for r in A + B + C + D), "EXACTLY-ONCE VIOLATED"
    print("\nAll benchmark scenarios kept exactly-once effects. CSVs in bench/results/")
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(1, 3, figsize=(15, 4))
        for lat in sorted({r["kv_latency_ms"] for r in A}):
            rs = [r for r in A if r["kv_latency_ms"] == lat]
            ax[0].plot([r["concurrency"] for r in rs], [r["throughput_eps"] for r in rs], "o-", label=f"RTT {lat} ms")
        ax[0].set(xscale="log", xlabel="concurrency", ylabel="events/s", title="Throughput vs concurrency")
        ax[0].legend()
        ax[1].plot([r["window_s"] for r in C], [r["peak_kv_keys"] for r in C], "o-")
        ax[1].set(xscale="log", yscale="log", xlabel="window (s)", ylabel="peak KV keys", title="State vs window")
        ax[2].plot([r["fail_rate"] + r["ack_loss"] for r in D], [r["throughput_eps"] for r in D], "o-")
        ax[2].set(xlabel="total injected fault rate", ylabel="events/s", title="Throughput under faults")
        fig.tight_layout()
        fig.savefig(os.path.join(OUT, "benchmarks.png"), dpi=120)
        print("chart: bench/results/benchmarks.png")
    except ImportError:
        print("(matplotlib not installed - skipping chart)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=4000)
    ap.add_argument("--quick", action="store_true")
    a = ap.parse_args()
    asyncio.run(main(1000 if a.quick else a.n, a.quick))
