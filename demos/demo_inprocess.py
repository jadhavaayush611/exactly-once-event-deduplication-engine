"""Narrated demo: duplicates, out-of-order, late events, a crash and recovery.
   python demos/demo_inprocess.py"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dedup_engine import CrashInjector, DedupEngine, Event, MemoryKV, MemorySink, SimulatedCrash  # noqa: E402


def ev(i, ts):
    return Event(f"order-{i}", ts, {"amount": i * 10})


async def main():
    kv, sink = MemoryKV(), MemorySink()
    mk = lambda hook=None: DedupEngine(kv, sink, window=60, lateness=2, crash_hook=hook)

    print("1) duplicates + out-of-order inside the lateness bound")
    e = mk()
    for i, ts in [(1, 1), (3, 3), (2, 2), (2, 2), (1, 1), (5, 5), (4, 4), (9, 9)]:
        print(f"   ingest order-{i} ts={ts:>2} -> {(await e.ingest(ev(i, ts))).value}")
    print("   delivered so far:", [x.id for x in sink.effects], "(event-time order, no dups)")

    print("\n2) very old event (older than window) -> dead-letter queue")
    await e.ingest(ev(99, 500))
    print("   ingest order-7 ts=100 ->", (await e.ingest(ev(7, 100))).value, "| dlq:", [x.id for x in e.dlq])

    print("\n3) process crash right after the sink applied an event but before the KV commit")
    e2 = mk(CrashInjector("after_deliver", after=1))
    try:
        await e2.ingest(ev(600, 600))
        await e2.ingest(ev(601, 603))
    except SimulatedCrash as c:
        print("   *** CRASH at", c)
    e3 = mk()
    print("   restart: recovered", await e3.recover(), "pending events from the KV write-ahead records")
    for x in (ev(600, 600), ev(601, 603)):                 # producer replays (at-least-once)
        print(f"   replay {x.id} ->", (await e3.ingest(x)).value)
    await e3.drain()
    got = [x.id for x in sink.effects]
    print("\nFinal sink:", got)
    print("Duplicates in sink:", len(got) - len(set(got)), "| stats:", e3.stats())


asyncio.run(main())
