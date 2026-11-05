"""Fault injection: flaky KV, flaky sink, and process-crash points."""
from __future__ import annotations

import random
from typing import Optional

from .kv import KVStore
from .models import KVError, SimulatedCrash, SinkError
from .sink import Sink

CRASH_POINTS = ["after_claim", "after_buffer", "before_deliver", "after_deliver", "after_commit"]


class _Faulty:
    def _init_faults(self, fail_rate, ack_loss_rate, seed):
        self.fail_rate, self.ack_loss_rate = fail_rate, ack_loss_rate
        self.rng = random.Random(seed)
        self.once: dict[str, str] = {}  # op name -> "before" | "after" (one-shot, deterministic)
        self.calls = 0
        self.injected = 0

    def _decide(self, name: str) -> Optional[str]:
        self.calls += 1
        if name in self.once:
            return self.once.pop(name)
        r = self.rng.random()
        if r < self.fail_rate:
            return "before"        # request lost: nothing applied
        if r < self.fail_rate + self.ack_loss_rate:
            return "after"         # applied, but the ack was lost
        return None

    async def _call(self, name, fn, exc):
        mode = self._decide(name)
        if mode == "before":
            self.injected += 1
            raise exc(f"injected: {name} request lost")
        res = await fn()
        if mode == "after":
            self.injected += 1
            raise exc(f"injected: {name} applied but ack lost")
        return res


class FaultyKV(_Faulty, KVStore):
    def __init__(self, inner: KVStore, fail_rate=0.0, ack_loss_rate=0.0, seed=0):
        self.inner = inner
        self._init_faults(fail_rate, ack_loss_rate, seed)

    async def put_if_absent(self, key, value, score=None):
        return await self._call("put_if_absent", lambda: self.inner.put_if_absent(key, value, score), KVError)

    async def get(self, key):
        return await self._call("get", lambda: self.inner.get(key), KVError)

    async def set(self, key, value, score=None):
        return await self._call("set", lambda: self.inner.set(key, value, score), KVError)

    async def scan(self, prefix):
        return await self._call("scan", lambda: self.inner.scan(prefix), KVError)

    async def get_pending_batch(self, limit):
        return await self._call("get_pending_batch", lambda: self.inner.get_pending_batch(limit), KVError)

    async def cleanup_stale_pending(self, key):
        if hasattr(self.inner, "cleanup_stale_pending"):
            return await self.inner.cleanup_stale_pending(key)

    async def evict_before(self, score, limit=None):
        return await self._call("evict_before", lambda: self.inner.evict_before(score, limit), KVError)

    async def delete(self, key):
        return await self._call("delete", lambda: self.inner.delete(key), KVError)

    async def size(self):
        return await self.inner.size()


class FaultySink(_Faulty, Sink):
    def __init__(self, inner: Sink, fail_rate=0.0, ack_loss_rate=0.0, seed=0):
        self.inner = inner
        self._init_faults(fail_rate, ack_loss_rate, seed)

    async def deliver(self, event):
        return await self._call("deliver", lambda: self.inner.deliver(event), SinkError)


class CrashInjector:
    """Callable used as DedupEngine(crash_hook=...). Raises SimulatedCrash at a named point."""

    def __init__(self, point: Optional[str] = None, after: int = 1, rate: float = 0.0, seed: int = 0):
        self.point, self.after, self.rate = point, after, rate
        self.rng = random.Random(seed)
        self.hits = 0
        self.crashes = 0

    def __call__(self, point: str) -> None:
        if self.point == point:
            self.hits += 1
            if self.hits == self.after:
                self.crashes += 1
                raise SimulatedCrash(point)
        elif self.point is None and self.rate and self.rng.random() < self.rate:
            self.crashes += 1
            raise SimulatedCrash(point)
