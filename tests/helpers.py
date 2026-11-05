import asyncio
import random

from dedup_engine import Event


def run(coro):
    return asyncio.run(coro)


def mk(i, ts=None):
    return Event(f"e{i}", float(i if ts is None else ts), {"n": i})


def ids(sink):
    return [e.id for e in sink.effects]


def make_stream(n, seed, dup_rate=0.4, lateness=1.0, spacing=0.1):
    """Unique events + duplicates, shuffled with bounded disorder (< lateness)."""
    rng = random.Random(seed)
    base = [Event(f"e{i}", i * spacing, i) for i in range(n)]
    stream = base + [rng.choice(base) for _ in range(int(n * dup_rate))]
    stream.sort(key=lambda e: e.ts + rng.uniform(0, lateness * 0.9))
    return base, stream
