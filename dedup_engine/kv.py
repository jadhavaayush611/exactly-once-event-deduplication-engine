"""Key-value store abstraction.

Contract (all a backend must provide):
  * put_if_absent  - ATOMIC set-if-not-exists  (Redis SET NX / DynamoDB conditional put)
  * set            - overwrite, optional event-time `score` that makes the key evictable
  * evict_before   - drop every scored key with score < cutoff (sliding-window expiry)
Keys written with score=None (pending records) are never evicted.
"""
from __future__ import annotations

import asyncio
import heapq
from abc import ABC, abstractmethod
from typing import Optional

from .models import KVError


class KVStore(ABC):
    @abstractmethod
    async def put_if_absent(self, key: str, value: str, score: Optional[float] = None) -> bool: ...
    @abstractmethod
    async def get(self, key: str) -> Optional[str]: ...
    @abstractmethod
    async def set(self, key: str, value: str, score: Optional[float] = None) -> None: ...
    @abstractmethod
    async def scan(self, prefix: str) -> list[tuple[str, str]]: ...
    @abstractmethod
    async def get_pending_batch(self, limit: int) -> list[tuple[str, Optional[str]]]: ...
    @abstractmethod
    async def evict_before(self, score: float, limit: Optional[int] = None) -> int: ...
    @abstractmethod
    async def delete(self, key: str) -> bool: ...
    @abstractmethod
    async def size(self) -> int: ...

    async def close(self) -> None:
        pass


class MemoryKV(KVStore):
    """In-process store. `latency` simulates a cloud round trip per operation."""

    def __init__(self, latency: float = 0.0):
        self.latency = latency
        self._d: dict[str, str] = {}
        self._score: dict[str, float] = {}
        self._heap: list[tuple[float, str]] = []
        self._pending_idx: dict[str, float] = {}  # key -> ts (for dedicated pending index)
        self.ops = 0

    async def _rtt(self) -> None:
        self.ops += 1
        await asyncio.sleep(self.latency)  # sleep(0) still yields to the loop

    async def put_if_absent(self, key, value, score=None) -> bool:
        await self._rtt()
        if key in self._d:  # check+write is one synchronous block => atomic
            return False
        self._d[key] = value
        self._index(key, score)
        return True

    async def get(self, key):
        await self._rtt()
        return self._d.get(key)

    async def set(self, key, value, score=None) -> None:
        await self._rtt()
        self._d[key] = value
        self._index(key, score)

    def _index(self, key, score):
        if score is None:
            self._score.pop(key, None)
            # Only dedup records enter the pending index; metadata (e.g. meta:max_ts) does not
            if key.startswith("dedup:"):
                ts = 0.0
                try:
                    import json
                    parsed = json.loads(self._d.get(key, ""))
                    if isinstance(parsed, dict) and "ts" in parsed and isinstance(parsed["ts"], (int, float)):
                        ts = float(parsed["ts"])
                except Exception:
                    pass
                self._pending_idx[key] = ts
            else:
                self._pending_idx.pop(key, None)
        else:
            self._score[key] = score
            self._pending_idx.pop(key, None)
            heapq.heappush(self._heap, (score, key))

    async def cleanup_stale_pending(self, key: str) -> None:
        self._pending_idx.pop(key, None)

    async def scan(self, prefix):
        await self._rtt()
        return [(k, v) for k, v in self._d.items() if k.startswith(prefix)]

    async def get_pending_batch(self, limit: int) -> list[tuple[str, Optional[str]]]:
        await self._rtt()
        # Sort pending by (ts, key)
        sorted_keys = sorted(self._pending_idx.keys(), key=lambda k: (self._pending_idx[k], k))[:limit]
        return [(k, self._d.get(k)) for k in sorted_keys]

    async def evict_before(self, score: float, limit: Optional[int] = None) -> int:
        await self._rtt()
        n = 0
        while self._heap and self._heap[0][0] < score and (limit is None or n < limit):
            s, k = heapq.heappop(self._heap)
            if self._score.get(k) == s:  # skip stale heap entries
                del self._score[k]
                self._d.pop(k, None)
                self._pending_idx.pop(k, None)
                n += 1
        return n

    async def delete(self, key: str) -> bool:
        await self._rtt()
        self._score.pop(key, None)
        self._pending_idx.pop(key, None)
        return self._d.pop(key, None) is not None

    async def size(self) -> int:
        return len(self._d)


class RedisKV(KVStore):
    """Redis 7 backend (pip install redis). Expiry index lives in a sorted set.
    All record-and-index mutations are executed atomically via Lua scripts."""

    INDEX = "idx:dedup"
    INDEX_PENDING = "idx:dedup:pending"

    # Atomic PUT-IF-ABSENT:
    # KEYS[1] = record key
    # KEYS[2] = committed index key (idx:dedup)
    # KEYS[3] = pending index key (idx:dedup:pending)
    # ARGV[1] = record value
    # ARGV[2] = '1' if score is provided else '0'
    # ARGV[3] = score (string float)
    # ARGV[4] = pending timestamp score (string float)
    LUA_PUT_IF_ABSENT = """
local key = KEYS[1]
local idx_committed = KEYS[2]
local idx_pending = KEYS[3]
local val = ARGV[1]
local has_score = ARGV[2]
local score = ARGV[3]
local pending_ts = tonumber(ARGV[4]) or 0.0

if redis.call('EXISTS', key) == 1 then
    return 0
end

redis.call('SET', key, val)
if has_score == '1' then
    redis.call('ZADD', idx_committed, score, key)
    redis.call('ZREM', idx_pending, key)
else
    redis.call('ZREM', idx_committed, key)
    if string.sub(key, 1, 6) == "dedup:" then
        redis.call('ZADD', idx_pending, pending_ts, key)
    else
        redis.call('ZREM', idx_pending, key)
    end
end
return 1
"""

    # Atomic SET (create or overwrite record and update/remove index):
    # KEYS[1] = record key
    # KEYS[2] = committed index key (idx:dedup)
    # KEYS[3] = pending index key (idx:dedup:pending)
    # ARGV[1] = record value
    # ARGV[2] = '1' if score is provided else '0'
    # ARGV[3] = score (string float)
    # ARGV[4] = pending timestamp score (string float)
    LUA_SET = """
local key = KEYS[1]
local idx_committed = KEYS[2]
local idx_pending = KEYS[3]
local val = ARGV[1]
local has_score = ARGV[2]
local score = ARGV[3]
local pending_ts = tonumber(ARGV[4]) or 0.0

redis.call('SET', key, val)
if has_score == '1' then
    redis.call('ZADD', idx_committed, score, key)
    redis.call('ZREM', idx_pending, key)
else
    redis.call('ZREM', idx_committed, key)
    if string.sub(key, 1, 6) == "dedup:" then
        redis.call('ZADD', idx_pending, pending_ts, key)
    else
        redis.call('ZREM', idx_pending, key)
    end
end
return 1
"""

    # Atomic EVICT_BEFORE (drop every scored key with score < cutoff):
    # KEYS[1] = committed index key (idx:dedup)
    # ARGV[1] = cutoff score
    # ARGV[2] = '1' if limit is provided else '0'
    # ARGV[3] = limit count
    LUA_EVICT_BEFORE = """
local idx = KEYS[1]
local cutoff = ARGV[1]
local has_limit = ARGV[2]
local limit = tonumber(ARGV[3])

local keys
if has_limit == '1' then
    keys = redis.call('ZRANGEBYSCORE', idx, '-inf', '(' .. cutoff, 'LIMIT', 0, limit)
else
    keys = redis.call('ZRANGEBYSCORE', idx, '-inf', '(' .. cutoff)
end

if #keys == 0 then
    return 0
end

redis.call('ZREM', idx, unpack(keys))
redis.call('DEL', unpack(keys))
return #keys
"""

    # Atomic DELETE (delete record key and remove from both indexes):
    # KEYS[1] = record key
    # KEYS[2] = committed index key (idx:dedup)
    # KEYS[3] = pending index key (idx:dedup:pending)
    LUA_DELETE = """
local key = KEYS[1]
local idx_committed = KEYS[2]
local idx_pending = KEYS[3]

redis.call('ZREM', idx_committed, key)
redis.call('ZREM', idx_pending, key)
local deleted = redis.call('DEL', key)
return deleted
"""

    # Atomic GET_PENDING_BATCH:
    # KEYS[1] = pending index key (idx:dedup:pending)
    # ARGV[1] = limit count
    LUA_GET_PENDING_BATCH = """
local idx_pending = KEYS[1]
local limit = tonumber(ARGV[1])

local keys = redis.call('ZRANGE', idx_pending, 0, limit - 1)
if #keys == 0 then
    return {}
end

local res = {}
local batch_size = 5000
for i = 1, #keys, batch_size do
    local batch_keys = {}
    local end_idx = math.min(i + batch_size - 1, #keys)
    for j = i, end_idx do
        batch_keys[#batch_keys + 1] = keys[j]
    end
    local vals = redis.call('MGET', unpack(batch_keys))
    for k = 1, #batch_keys do
        res[#res + 1] = {batch_keys[k], vals[k] or false}
    end
end
return res
"""

    # Cleanup stale member from pending index:
    # KEYS[1] = pending index key (idx:dedup:pending)
    # ARGV[1] = key to remove
    LUA_CLEANUP_PENDING = """
local idx_pending = KEYS[1]
local key = ARGV[1]
return redis.call('ZREM', idx_pending, key)
"""

    def __init__(self, url: str = "redis://localhost:6379/0"):
        import redis.asyncio as redis  # lazy: only needed for this backend
        self._redis = redis
        self._r = redis.from_url(url, decode_responses=True)

    async def _do(self, coro):
        try:
            return await coro
        except self._redis.RedisError as e:
            raise KVError(str(e)) from e

    def _extract_ts(self, value: str) -> float:
        try:
            import json
            parsed = json.loads(value)
            if isinstance(parsed, dict) and "ts" in parsed and isinstance(parsed["ts"], (int, float)):
                return float(parsed["ts"])
        except Exception:
            pass
        return 0.0

    async def put_if_absent(self, key, value, score=None) -> bool:
        has_score = "1" if score is not None else "0"
        score_str = str(score) if score is not None else ""
        pending_ts = str(self._extract_ts(value)) if score is None else "0.0"
        res = await self._do(
            self._r.eval(self.LUA_PUT_IF_ABSENT, 3, key, self.INDEX, self.INDEX_PENDING,
                         value, has_score, score_str, pending_ts)
        )
        return bool(res)

    async def get(self, key):
        return await self._do(self._r.get(key))

    async def set(self, key, value, score=None) -> None:
        has_score = "1" if score is not None else "0"
        score_str = str(score) if score is not None else ""
        pending_ts = str(self._extract_ts(value)) if score is None else "0.0"
        await self._do(
            self._r.eval(self.LUA_SET, 3, key, self.INDEX, self.INDEX_PENDING,
                         value, has_score, score_str, pending_ts)
        )

    async def get_pending_batch(self, limit: int) -> list[tuple[str, Optional[str]]]:
        if limit <= 0:
            return []
        raw = await self._do(
            self._r.eval(self.LUA_GET_PENDING_BATCH, 1, self.INDEX_PENDING, str(limit))
        )
        # raw is a list of [key, val_or_false]
        out: list[tuple[str, Optional[str]]] = []
        for item in raw:
            k = item[0]
            v = item[1] if item[1] is not False and item[1] is not None else None
            out.append((k, v))
        return out

    async def cleanup_stale_pending(self, key: str) -> None:
        await self._do(
            self._r.eval(self.LUA_CLEANUP_PENDING, 1, self.INDEX_PENDING, key)
        )

    async def scan(self, prefix):
        out = []
        async for k in self._r.scan_iter(match=prefix + "*", count=1000):
            v = await self._do(self._r.get(k))
            if v is not None:
                out.append((k, v))
        return out

    async def evict_before(self, score: float, limit: Optional[int] = None) -> int:
        has_limit = "1" if limit is not None else "0"
        limit_str = str(limit) if limit is not None else "0"
        return await self._do(
            self._r.eval(self.LUA_EVICT_BEFORE, 1, self.INDEX, str(score), has_limit, limit_str)
        )

    async def delete(self, key: str) -> bool:
        res = await self._do(
            self._r.eval(self.LUA_DELETE, 3, key, self.INDEX, self.INDEX_PENDING)
        )
        return bool(res)

    async def size(self) -> int:
        return await self._do(self._r.dbsize())

    async def close(self) -> None:
        await self._r.aclose()
