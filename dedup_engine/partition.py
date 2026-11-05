"""
Distributed partition management, ownership leasing, and event routing.

Key Concepts:
- Deterministic Partitioning: Events are hashed by partition key (or event.id if absent)
  using CRC32 across a configurable number of partitions (default: 4).
- Distributed Ownership: Redis-backed lease acquisition using atomic Lua scripts.
- Fencing Tokens (Epochs): Monotonically increasing epoch counters prevent split-brain
  processing by stale owners.
- Ownership Lifecycles: Automated background heartbeat renewals, expiry detection,
  and graceful release.
- Partition Routing: Automatic forward routing to the current partition owner (via
  internal HTTP endpoint or in-process dispatch if locally owned).
"""
from __future__ import annotations

import asyncio
import logging
import time
import zlib
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import httpx

from .engine import DedupEngine
from .kv import KVStore, MemoryKV, RedisKV
from .models import Event, KVError, Status
from .sink import Sink

logger = logging.getLogger("dedup_engine.partition")

ACQUIRE_LEASE_LUA = """
-- KEYS[1]: meta:partition:<p_id>:owner
-- KEYS[2]: meta:partition:<p_id>:epoch
-- KEYS[3]: meta:partition:<p_id>:endpoint
-- ARGV[1]: instance_id
-- ARGV[2]: ttl_ms
-- ARGV[3]: internal_endpoint
local cur_owner = redis.call('GET', KEYS[1])
if not cur_owner or cur_owner == ARGV[1] then
    local epoch = redis.call('INCR', KEYS[2])
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
    if ARGV[3] and ARGV[3] ~= '' then
        redis.call('SET', KEYS[3], ARGV[3], 'PX', ARGV[2])
    end
    return {1, epoch}
else
    local cur_epoch = tonumber(redis.call('GET', KEYS[2])) or 0
    return {0, cur_epoch}
end
"""

RENEW_LEASE_LUA = """
-- KEYS[1]: meta:partition:<p_id>:owner
-- KEYS[2]: meta:partition:<p_id>:epoch
-- KEYS[3]: meta:partition:<p_id>:endpoint
-- ARGV[1]: instance_id
-- ARGV[2]: epoch
-- ARGV[3]: ttl_ms
-- ARGV[4]: internal_endpoint
local cur_owner = redis.call('GET', KEYS[1])
local cur_epoch = tonumber(redis.call('GET', KEYS[2])) or 0
if cur_owner == ARGV[1] and cur_epoch == tonumber(ARGV[2]) then
    redis.call('PEXPIRE', KEYS[1], ARGV[3])
    if ARGV[4] and ARGV[4] ~= '' then
        redis.call('PEXPIRE', KEYS[3], ARGV[3])
    end
    return 1
else
    return 0
end
"""

RELEASE_LEASE_LUA = """
-- KEYS[1]: meta:partition:<p_id>:owner
-- KEYS[2]: meta:partition:<p_id>:epoch
-- KEYS[3]: meta:partition:<p_id>:endpoint
-- ARGV[1]: instance_id
-- ARGV[2]: epoch
local cur_owner = redis.call('GET', KEYS[1])
local cur_epoch = tonumber(redis.call('GET', KEYS[2])) or 0
if cur_owner == ARGV[1] and cur_epoch == tonumber(ARGV[2]) then
    redis.call('DEL', KEYS[1])
    redis.call('DEL', KEYS[3])
    return 1
else
    return 0
end
"""


def compute_partition(key: str, num_partitions: int) -> int:
    """Deterministic, platform-independent hash partitioning using CRC32."""
    if num_partitions <= 1:
        return 0
    return (zlib.crc32(key.encode("utf-8")) & 0xFFFFFFFF) % num_partitions


@dataclass
class PartitionState:
    partition_id: int
    is_owner: bool = False
    epoch: int = 0
    last_heartbeat: float = 0.0
    engine: Optional[DedupEngine] = None
    failovers: int = 0


class PartitionManager:
    """
    Manages partition assignment, lease coordination, heartbeats, and event routing.
    """

    def __init__(
        self,
        instance_id: str,
        kv: KVStore,
        sink_factory: Callable[[int], Sink],
        num_partitions: int = 4,
        lease_ttl_ms: int = 4000,
        heartbeat_interval_ms: int = 1000,
        internal_endpoint: str = "",
        window: float = 60.0,
        lateness: float = 2.0,
        max_buffer_size: Optional[int] = None,
        max_future_skew: Optional[float] = None,
    ):
        self.instance_id = instance_id
        self.kv = kv
        self.sink_factory = sink_factory
        self.num_partitions = num_partitions
        self.lease_ttl_ms = lease_ttl_ms
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.internal_endpoint = internal_endpoint

        self.window = window
        self.lateness = lateness
        self.max_buffer_size = max_buffer_size
        self.max_future_skew = max_future_skew

        self.partitions: Dict[int, PartitionState] = {
            p: PartitionState(partition_id=p) for p in range(num_partitions)
        }

        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running = False
        self._lock = asyncio.Lock()

        # In-process instance registry for direct co-located routing
        self._in_process_registry: Optional[Dict[str, PartitionManager]] = None

        # In-memory coordination fallback (for tests using MemoryKV)
        self._mem_leases: Dict[int, Tuple[str, int, float, str]] = {}  # p -> (owner, epoch, expire_at, endpoint)
        self._mem_epoch: Dict[int, int] = {p: 0 for p in range(num_partitions)}

    # -------------------------------------------------------------------------
    # Lifecycle: Start, Heartbeat Loop, Stop
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """Starts partition management, attempts initial lease acquisition, and runs heartbeat loop."""
        async with self._lock:
            if self._running:
                return
            self._running = True

            # Register Lua scripts if RedisKV
            await self._register_scripts()

            # Initial acquisition sweep
            await self._reconcile_ownership()

            # Start background heartbeat & acquisition loop
            self._heartbeat_task = asyncio.create_task(self._loop())
            logger.info("PartitionManager %s started with %d partitions", self.instance_id, self.num_partitions)

    async def stop(self) -> None:
        """Gracefully releases all partition leases and stops heartbeat loop."""
        async with self._lock:
            if not self._running:
                return
            self._running = False

            if self._heartbeat_task:
                self._heartbeat_task.cancel()
                try:
                    await self._heartbeat_task
                except asyncio.CancelledError:
                    pass

            # Gracefully release all owned partitions
            for p_id, p_state in self.partitions.items():
                if p_state.is_owner:
                    try:
                        await self._release_lease(p_id, p_state.epoch)
                    except Exception as e:
                        logger.warning("Error releasing lease for partition %d: %s", p_id, e)
                    p_state.is_owner = False
                    p_state.engine = None

            logger.info("PartitionManager %s stopped and released all leases", self.instance_id)

    async def _register_scripts(self) -> None:
        raw_r = None
        if isinstance(self.kv, RedisKV):
            raw_r = self.kv._r
        elif hasattr(self.kv, "inner") and isinstance(self.kv.inner, RedisKV):
            raw_r = self.kv.inner._r

        if raw_r is not None:
            self._sha_acquire = await raw_r.script_load(ACQUIRE_LEASE_LUA)
            self._sha_renew = await raw_r.script_load(RENEW_LEASE_LUA)
            self._sha_release = await raw_r.script_load(RELEASE_LEASE_LUA)

    async def _loop(self) -> None:
        while self._running:
            try:
                await self._reconcile_ownership()
            except Exception as e:
                logger.error("Error in partition reconciliation loop: %s", e)
            await asyncio.sleep(self.heartbeat_interval_ms / 1000.0)

    # -------------------------------------------------------------------------
    # Lease Management: Acquire, Renew, Release
    # -------------------------------------------------------------------------

    async def _reconcile_ownership(self) -> None:
        """Renews existing owned leases, checks for expired leases, and acquires available partitions."""
        for p_id in range(self.num_partitions):
            p_state = self.partitions[p_id]
            if p_state.is_owner:
                # Renew lease
                renewed = await self._renew_lease(p_id, p_state.epoch)
                if not renewed:
                    # Lost lease (fencing detected or expired)
                    logger.warning("Lost ownership lease for partition %d (epoch %d)", p_id, p_state.epoch)
                    p_state.is_owner = False
                    p_state.engine = None
                    p_state.failovers += 1
                else:
                    p_state.last_heartbeat = time.time()
            else:
                # Attempt to acquire partition
                acquired, epoch = await self._acquire_lease(p_id)
                if acquired:
                    logger.info("Acquired ownership of partition %d (epoch %d)", p_id, epoch)
                    p_state.is_owner = True
                    p_state.epoch = epoch
                    p_state.last_heartbeat = time.time()

                    # Initialize per-partition DedupEngine & perform pending recovery
                    sink = self.sink_factory(p_id)
                    engine = DedupEngine(
                        self.kv,
                        sink,
                        window=self.window,
                        lateness=self.lateness,
                        max_buffer_size=self.max_buffer_size,
                        max_future_skew=self.max_future_skew,
                    )
                    try:
                        recovered = await engine.recover()
                        logger.info("Partition %d recovered %d pending events", p_id, recovered)
                    except Exception as e:
                        logger.error("Error during recovery of partition %d: %s", p_id, e)

                    p_state.engine = engine

    async def _acquire_lease(self, p_id: int) -> Tuple[bool, int]:
        raw_r = None
        if isinstance(self.kv, RedisKV):
            raw_r = self.kv._r
        elif hasattr(self.kv, "inner") and isinstance(self.kv.inner, RedisKV):
            raw_r = self.kv.inner._r

        if raw_r is not None:
            if not hasattr(self, "_sha_acquire"):
                await self._register_scripts()
            try:
                keys = [
                    f"meta:partition:{p_id}:owner",
                    f"meta:partition:{p_id}:epoch",
                    f"meta:partition:{p_id}:endpoint",
                ]
                args = [self.instance_id, self.lease_ttl_ms, self.internal_endpoint]
                res = await self.kv._do(raw_r.evalsha(self._sha_acquire, len(keys), *keys, *args))
                return bool(res[0]), int(res[1])
            except Exception as e:
                logger.debug("Redis acquire_lease error for partition %d: %s", p_id, e)
                return False, 0
        else:
            # MemoryKV / Mock fallback
            now = time.time()
            cur = self._mem_leases.get(p_id)
            if not cur or cur[0] == self.instance_id or cur[2] <= now:
                self._mem_epoch[p_id] = self._mem_epoch.get(p_id, 0) + 1
                epoch = self._mem_epoch[p_id]
                expire_at = now + (self.lease_ttl_ms / 1000.0)
                self._mem_leases[p_id] = (self.instance_id, epoch, expire_at, self.internal_endpoint)
                return True, epoch
            return False, cur[1]

    async def _renew_lease(self, p_id: int, epoch: int) -> bool:
        raw_r = None
        if isinstance(self.kv, RedisKV):
            raw_r = self.kv._r
        elif hasattr(self.kv, "inner") and isinstance(self.kv.inner, RedisKV):
            raw_r = self.kv.inner._r

        if raw_r is not None:
            if not hasattr(self, "_sha_renew"):
                await self._register_scripts()
            try:
                keys = [
                    f"meta:partition:{p_id}:owner",
                    f"meta:partition:{p_id}:epoch",
                    f"meta:partition:{p_id}:endpoint",
                ]
                args = [self.instance_id, epoch, self.lease_ttl_ms, self.internal_endpoint]
                res = await self.kv._do(raw_r.evalsha(self._sha_renew, len(keys), *keys, *args))
                return bool(res)
            except Exception as e:
                logger.debug("Redis renew_lease error for partition %d: %s", p_id, e)
                return False
        else:
            now = time.time()
            cur = self._mem_leases.get(p_id)
            if cur and cur[0] == self.instance_id and cur[1] == epoch and cur[2] > now:
                expire_at = now + (self.lease_ttl_ms / 1000.0)
                self._mem_leases[p_id] = (self.instance_id, epoch, expire_at, self.internal_endpoint)
                return True
            return False

    async def _release_lease(self, p_id: int, epoch: int) -> bool:
        raw_r = None
        if isinstance(self.kv, RedisKV):
            raw_r = self.kv._r
        elif hasattr(self.kv, "inner") and isinstance(self.kv.inner, RedisKV):
            raw_r = self.kv.inner._r

        if raw_r is not None:
            if not hasattr(self, "_sha_release"):
                await self._register_scripts()
            try:
                keys = [
                    f"meta:partition:{p_id}:owner",
                    f"meta:partition:{p_id}:epoch",
                    f"meta:partition:{p_id}:endpoint",
                ]
                args = [self.instance_id, epoch]
                res = await self.kv._do(raw_r.evalsha(self._sha_release, len(keys), *keys, *args))
                return bool(res)
            except Exception as e:
                logger.warning("Redis release_lease error for partition %d: %s", p_id, e)
                return False
        else:
            cur = self._mem_leases.get(p_id)
            if cur and cur[0] == self.instance_id and cur[1] == epoch:
                del self._mem_leases[p_id]
                return True
            return False

    # -------------------------------------------------------------------------
    # Partition Queries & Owner Endpoint Discovery
    # -------------------------------------------------------------------------

    async def get_partition_owner_info(self, p_id: int) -> Tuple[Optional[str], int, Optional[str]]:
        """Returns (owner_instance_id, epoch, endpoint) for a given partition."""
        if isinstance(self.kv, RedisKV):
            try:
                owner = await self.kv.get(f"meta:partition:{p_id}:owner")
                epoch_str = await self.kv.get(f"meta:partition:{p_id}:epoch")
                endpoint = await self.kv.get(f"meta:partition:{p_id}:endpoint")
                epoch = int(epoch_str) if epoch_str else 0
                return owner, epoch, endpoint
            except Exception:
                return None, 0, None
        else:
            now = time.time()
            cur = self._mem_leases.get(p_id)
            if cur and cur[2] > now:
                return cur[0], cur[1], cur[3]
            return None, self._mem_epoch.get(p_id, 0), None

    # -------------------------------------------------------------------------
    # Ingestion & Routing
    # -------------------------------------------------------------------------

    async def ingest_event(
        self,
        event: Event,
        partition_key: Optional[str] = None,
        fencing_epoch: Optional[int] = None,
        is_routed: bool = False,
    ) -> Tuple[Status, int, Optional[str]]:
        """
        Routes and ingests an event.

        Returns (Status, partition_id, error_message_or_owner).
        """
        routing_key = partition_key if partition_key is not None else event.id
        p_id = compute_partition(routing_key, self.num_partitions)
        p_state = self.partitions[p_id]

        # Case 1: Locally owned partition
        if p_state.is_owner and p_state.engine is not None:
            # If request specified fencing_epoch, validate it matches current epoch
            if fencing_epoch is not None and fencing_epoch != p_state.epoch:
                return Status.BUFFER_FULL, p_id, f"Fencing token mismatch (expected {p_state.epoch}, got {fencing_epoch})"

            try:
                status = await p_state.engine.ingest(event)
                return status, p_id, None
            except KVError as e:
                raise e

        # Case 2: If this call is already a forwarded request, do not forward again (avoid loop)
        if is_routed:
            return Status.BUFFER_FULL, p_id, f"Partition {p_id} not owned by targeted instance {self.instance_id}"

        # Case 3: Partition owned by another instance -> forward to remote endpoint or in-process peer
        owner, epoch, endpoint = await self.get_partition_owner_info(p_id)
        if not owner:
            return Status.BUFFER_FULL, p_id, f"Partition {p_id} is temporarily leaderless; retryable"

        # Check in-process peer registry first (for in-process multi-instance benchmarks and tests)
        if self._in_process_registry and owner in self._in_process_registry:
            peer = self._in_process_registry[owner]
            return await peer.ingest_event(event, partition_key=routing_key, fencing_epoch=epoch, is_routed=True)

        if not endpoint:
            # Temporary state: partition is currently leaderless or has no endpoint configured
            return Status.BUFFER_FULL, p_id, f"Partition {p_id} has no reachable endpoint; retryable"

        # Forward request over HTTP
        forward_url = f"{endpoint.rstrip('/')}/events/internal"
        req_data = {
            "event_id": event.id,
            "timestamp": event.ts,
            "payload": event.data,
            "partition_key": routing_key,
            "partition_id": p_id,
            "fencing_epoch": epoch,
        }

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(forward_url, json=req_data)
                if resp.status_code == 202:
                    return Status.ACCEPTED, p_id, None
                elif resp.status_code == 200:
                    return Status.DUPLICATE, p_id, None
                elif resp.status_code == 429:
                    return Status.BUFFER_FULL, p_id, "Partition buffer capacity full on owner"
                elif resp.status_code == 410:
                    return Status.TOO_OLD, p_id, "Event older than window on partition owner"
                elif resp.status_code == 422:
                    return Status.TOO_FAR_FUTURE, p_id, "Future skew rejected on partition owner"
                elif resp.status_code == 400:
                    return Status.INVALID_TIMESTAMP, p_id, "Invalid timestamp"
                elif resp.status_code == 503:
                    return Status.BUFFER_FULL, p_id, "Storage error on partition owner; retryable"
                else:
                    return Status.BUFFER_FULL, p_id, f"Forwarding failed with status {resp.status_code}"
        except Exception as e:
            logger.warning("Failed forwarding event to owner %s (%s): %s", owner, forward_url, e)
            return Status.BUFFER_FULL, p_id, f"Routing failure to partition owner {owner}; retryable"

    # -------------------------------------------------------------------------
    # Telemetry & Operational Metrics
    # -------------------------------------------------------------------------

    def get_stats(self) -> Dict[str, Any]:
        """Collects per-partition and aggregate telemetry."""
        owned_partitions = []
        partition_details = {}
        total_buffered = 0
        total_reserved = 0
        total_dlq = 0

        for p_id, p_state in self.partitions.items():
            details: Dict[str, Any] = {
                "partition_id": p_id,
                "is_owner": p_state.is_owner,
                "epoch": p_state.epoch,
                "failovers": p_state.failovers,
            }
            if p_state.is_owner and p_state.engine:
                owned_partitions.append(p_id)
                eng_stats = p_state.engine.stats()
                details.update(
                    watermark=eng_stats.get("watermark"),
                    buffered=eng_stats.get("buffered", 0),
                    reserved=eng_stats.get("reserved", 0),
                    accepted=eng_stats.get("accepted", 0),
                    duplicates=eng_stats.get("duplicates", 0),
                    too_old=eng_stats.get("too_old", 0),
                    too_far_future=eng_stats.get("too_far_future", 0),
                    buffer_overflow=eng_stats.get("buffer_overflow", 0),
                    delivered=eng_stats.get("delivered", 0),
                    dlq=eng_stats.get("dlq", 0),
                )
                total_buffered += eng_stats.get("buffered", 0)
                total_reserved += eng_stats.get("reserved", 0)
                total_dlq += eng_stats.get("dlq", 0)
            partition_details[p_id] = details

        return {
            "instance_id": self.instance_id,
            "num_partitions": self.num_partitions,
            "owned_partitions": owned_partitions,
            "total_buffered": total_buffered,
            "total_reserved": total_reserved,
            "total_dlq": total_dlq,
            "partitions": partition_details,
        }
