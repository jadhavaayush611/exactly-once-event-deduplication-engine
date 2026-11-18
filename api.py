"""HTTP API, operational endpoints, and dashboard host for the event deduplication engine."""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import statistics
import time
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from dedup_engine import (
    DedupEngine,
    Event,
    FaultyKV,
    FaultySink,
    KVError,
    MemoryKV,
    MemorySink,
    PartitionManager,
    RedisKV,
    SinkError,
    Status,
    compute_partition,
)
from dedup_engine.sink import FileSink, StdoutSink

logger = logging.getLogger("dedup_engine.api")

BASE = Path(__file__).resolve().parent
WEB = BASE / "web"
log = deque(maxlen=500)
state: dict[str, Any] = {}

MAX_CONTENT_LENGTH = 1_000_000  # 1MB limit for request payloads
REQUEST_TIMEOUT_SECONDS = 15.0   # Default operational request timeout


def build(
    window: float = 60.0,
    lateness: float = 2.0,
    max_buffer_size: Optional[int] = None,
    max_future_skew: Optional[float] = None,
    kv_store=None,
    sink_target=None,
):
    kv = kv_store if kv_store is not None else MemoryKV()
    sink = sink_target if sink_target is not None else MemorySink()
    return DedupEngine(
        kv,
        sink,
        window=window,
        lateness=lateness,
        max_buffer_size=max_buffer_size,
        max_future_skew=max_future_skew,
    ), kv, sink


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Read environment configurations for partitioning
    instance_id = os.environ.get("ENGINE_INSTANCE_ID", f"inst-{os.getpid()}")
    redis_url = os.environ.get("ENGINE_REDIS_URL", "")
    num_partitions = int(os.environ.get("ENGINE_NUM_PARTITIONS", "4"))
    enable_partitions = os.environ.get("ENGINE_ENABLE_PARTITIONS", "0") in ("1", "true", "True")
    internal_endpoint = os.environ.get("ENGINE_INTERNAL_ENDPOINT", "")

    kv = RedisKV(redis_url) if redis_url else MemoryKV()
    sink = MemorySink()
    e = DedupEngine(kv, sink, window=60.0, lateness=2.0)
    start_time = time.time()

    pm: Optional[PartitionManager] = None
    if enable_partitions:
        pm = PartitionManager(
            instance_id=instance_id,
            kv=kv,
            sink_factory=lambda p_id: MemorySink(),
            num_partitions=num_partitions,
            internal_endpoint=internal_endpoint,
            window=60.0,
            lateness=2.0,
        )
        await pm.start()

    state.update(
        engine=e,
        kv=kv,
        sink=sink,
        config={"window": 60.0, "lateness": 2.0},
        ready=True,
        start_time=start_time,
        instance_id=instance_id,
        partition_manager=pm,
    )

    yield

    if pm:
        await pm.stop()
    if isinstance(kv, RedisKV):
        await kv.close()
    state.clear()


app = FastAPI(
    title="Exactly-Once Event Deduplication Engine",
    version="1.0.0",
    lifespan=lifespan,
)
app.mount("/assets", StaticFiles(directory=WEB), name="assets")


# -----------------------------------------------------------------------------
# Middleware: Request Size Enforcement and Global Exception Handling
# -----------------------------------------------------------------------------

@app.middleware("http")
async def limit_request_size(request: Request, call_next):
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > MAX_CONTENT_LENGTH:
                return JSONResponse(
                    {
                        "status": "error",
                        "error": "payload_too_large",
                        "message": f"Request body exceeds maximum allowed size of {MAX_CONTENT_LENGTH} bytes.",
                    },
                    status_code=413,
                )
        except ValueError:
            return JSONResponse(
                {"status": "bad_request", "error": "invalid_content_length"},
                status_code=400,
            )
    return await call_next(request)


@app.middleware("http")
async def handle_uncaught_exceptions(request: Request, call_next):
    try:
        return await call_next(request)
    except Exception as exc:
        logger.exception("Unhandled error processing request %s", request.url.path)
        return JSONResponse(
            {
                "status": "error",
                "error": "internal_server_error",
                "message": "An unexpected server error occurred.",
            },
            status_code=500,
        )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = []
    for err in exc.errors():
        errors.append({
            "loc": err.get("loc"),
            "msg": err.get("msg"),
            "type": err.get("type"),
        })
    return JSONResponse(
        {
            "status": "bad_request",
            "error": "validation_error",
            "message": "Malformed request or invalid fields.",
            "details": errors,
        },
        status_code=422,
    )


# -----------------------------------------------------------------------------
# Request Models
# -----------------------------------------------------------------------------

class EventIn(BaseModel):
    """Legacy endpoint model."""
    id: str = Field(min_length=1, max_length=256)
    ts: float
    data: Any = None


class ConfigIn(BaseModel):
    window: float = Field(default=60, gt=0, le=86400)
    lateness: float = Field(default=2, ge=0, le=3600)


class BatchIn(BaseModel):
    count: int = Field(default=100, ge=1, le=5000)
    duplicate_ratio: float = Field(default=0.35, ge=0, le=0.95)


def current():
    if not state or not state.get("engine"):
        raise HTTPException(503, "Engine starting or unavailable")
    return state["engine"], state.get("kv"), state.get("sink")


# -----------------------------------------------------------------------------
# UI and Dashboard Endpoints
# -----------------------------------------------------------------------------

@app.get("/")
async def index():
    return FileResponse(WEB / "index.html")


@app.get("/api/events")
async def list_events(limit: int = 100):
    return list(log)[-max(1, min(limit, 300)):][::-1]


@app.post("/api/events")
async def ingest_legacy(p: EventIn):
    """Legacy ingestion endpoint preserving backward compatibility with dashboard."""
    pm: Optional[PartitionManager] = state.get("partition_manager")
    ev = Event(p.id, p.ts, p.data)
    partition_id = None
    try:
        if pm is not None:
            status, partition_id, error_msg = await asyncio.wait_for(
                pm.ingest_event(ev),
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            p_state = pm.partitions.get(partition_id)
            wm = p_state.engine.watermark if (p_state and p_state.is_owner and p_state.engine) else None
        else:
            e, _, _ = current()
            status = await asyncio.wait_for(
                e.ingest(ev),
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            wm = None if e.watermark == float("-inf") else e.watermark
    except KVError as x:
        raise HTTPException(503, str(x)) from x
    except (SinkError, asyncio.TimeoutError) as x:
        raise HTTPException(503, str(x)) from x
    rec = {
        "id": ev.id,
        "ts": ev.ts,
        "data": ev.data,
        "status": status.value,
        "at": time.time(),
        "watermark": wm,
    }
    if partition_id is not None:
        rec["partition_id"] = partition_id
    log.append(rec)
    return rec


@app.post("/api/drain")
async def drain_legacy():
    e, _, _ = current()
    pm: Optional[PartitionManager] = state.get("partition_manager")
    if pm:
        total_left = 0
        for p_id, p_state in pm.partitions.items():
            if p_state.is_owner and p_state.engine:
                total_left += await p_state.engine.drain()
        return {"remaining": total_left, "stats": pm.get_stats()}
    try:
        left = await e.drain()
    except Exception as x:
        raise HTTPException(503, str(x)) from x
    return {"remaining": left, "stats": e.stats()}


@app.post("/api/demo")
async def demo():
    e, _, _ = current()
    now = time.time()
    seq = [
        Event(f"purchase-{int(now)}-1", now - 5, {"type": "purchase", "amount": 49.99}),
        Event(f"purchase-{int(now)}-2", now - 3, {"type": "purchase", "amount": 120}),
        Event(f"purchase-{int(now)}-1", now - 5, {"type": "purchase", "amount": 49.99}),
        Event(f"shipment-{int(now)}-3", now - 1, {"type": "shipment", "order": "ORD-103"}),
        Event(f"purchase-{int(now)}-2", now - 3, {"type": "purchase", "amount": 120}),
    ]
    out = []
    for ev in seq:
        st = await e.ingest(ev)
        r = {
            "id": ev.id,
            "ts": ev.ts,
            "data": ev.data,
            "status": st.value,
            "at": time.time(),
            "watermark": e.watermark,
        }
        log.append(r)
        out.append(r)
    await e.drain()
    return {"events": out, "stats": e.stats()}


@app.post("/api/config")
async def config(p: ConfigIn):
    e, kv, sink = build(p.window, p.lateness)
    state.update(
        engine=e,
        kv=kv,
        sink=sink,
        config={"window": p.window, "lateness": p.lateness},
    )
    log.clear()
    return {"window": p.window, "lateness": p.lateness, "reset": True}


@app.post("/api/reset")
async def reset():
    cfg = state.get("config", {"window": 60.0, "lateness": 2.0})
    e, kv, sink = build(**cfg)
    pm = state.get("partition_manager")
    if pm:
        for p_state in pm.partitions.values():
            if p_state.is_owner and p_state.engine:
                p_state.engine = DedupEngine(pm.kv, pm.sink_factory(p_state.partition_id), **cfg)
    state.update(engine=e, kv=kv, sink=sink)
    log.clear()
    return {"reset": True}


@app.post("/api/benchmark")
async def benchmark(p: BatchIn):
    n = p.count
    unique = max(1, int(n * (1 - p.duplicate_ratio)))
    now = time.time()
    e, kv, sink = build(86400, 0)
    rng = random.Random(17)
    base = [Event(f"bench-{i}", now + i * 0.001, {"sequence": i}) for i in range(unique)]
    stream = base + [rng.choice(base) for _ in range(n - unique)]
    rng.shuffle(stream)
    lat = []
    start = time.perf_counter()
    for ev in stream:
        t = time.perf_counter()
        await e.ingest(ev)
        lat.append((time.perf_counter() - t) * 1000)
    await e.drain()
    elapsed = time.perf_counter() - start
    ordered = sorted(lat)
    return {
        "events": n,
        "unique_expected": unique,
        "sink_effects": len(sink.effects),
        "duplicates_detected": e.m.duplicates,
        "throughput_eps": round(n / max(elapsed, 1e-9), 1),
        "elapsed_ms": round(elapsed * 1000, 2),
        "p50_ms": round(statistics.median(lat), 3),
        "p95_ms": round(ordered[min(n - 1, int(n * 0.95))], 3),
        "p99_ms": round(ordered[min(n - 1, int(n * 0.99))], 3),
        "exactly_once": len(sink.effects) == unique == len({x.id for x in sink.effects}),
        "note": "In-process MemoryKV benchmark; measure on target hardware for capacity claims.",
    }


@app.post("/api/fault-lab")
async def fault_lab(p: BatchIn):
    n = min(p.count, 1000)
    unique = max(1, int(n * (1 - p.duplicate_ratio)))
    now = time.time()
    base = [Event(f"fault-{i}", now + i * 0.001, {"sequence": i}) for i in range(unique)]
    rng = random.Random(2026)
    stream = base + [rng.choice(base) for _ in range(n - unique)]
    rng.shuffle(stream)
    mem, sink = MemoryKV(), MemorySink()
    kv = FaultyKV(mem, fail_rate=0.06, ack_loss_rate=0.06, seed=2026)
    fs = FaultySink(sink, fail_rate=0.04, ack_loss_rate=0.04, seed=2027)
    e = DedupEngine(kv, fs, window=86400, lateness=0, max_retries=5, backoff=0.0001)
    client_retries = 0
    for ev in stream:
        for _ in range(8):
            try:
                await e.ingest(ev)
                break
            except KVError:
                client_retries += 1
        else:
            raise HTTPException(503, "Fault run exhausted client retries")
    for _ in range(20):
        try:
            if await e.drain() == 0:
                break
        except Exception:
            await asyncio.sleep(0)
    ids = [x.id for x in sink.effects]
    return {
        "events_submitted": n,
        "unique_expected": unique,
        "sink_effects": len(ids),
        "duplicates_detected": e.m.duplicates,
        "kv_retries": e.m.kv_retries,
        "sink_retries": e.m.sink_retries,
        "client_retries": client_retries,
        "fault_rates": {
            "kv_request_loss": 0.06,
            "kv_ack_loss": 0.06,
            "sink_failure": 0.04,
            "sink_ack_loss": 0.04,
        },
        "exactly_once": len(ids) == unique and len(ids) == len(set(ids)) and set(ids) == {x.id for x in base},
    }


# -----------------------------------------------------------------------------
# Phase 4 & Phase 5 HTTP Ingestion Protocol: POST /events & POST /events/internal
# -----------------------------------------------------------------------------

def _reject_constants(token: str):
    raise ValueError(f"Disallowed JSON constant: {token}")


@app.post("/events")
@app.post("/events/internal")
async def post_event(request: Request):
    """
    HTTP Ingestion Endpoint for Exactly-Once Deduplication with Partition Routing.

    Expects:
    {
      "event_id": "evt-1001",
      "timestamp": 1793700000.0,
      "payload": { ... },
      "partition_key": "optional-key"
    }
    """
    raw_body = await request.body()
    if not raw_body:
        return JSONResponse(
            {
                "status": "bad_request",
                "error": "empty_request_body",
                "message": "Request body cannot be empty.",
            },
            status_code=400,
        )

    try:
        body = json.loads(raw_body, parse_constant=_reject_constants)
    except Exception as exc:
        return JSONResponse(
            {
                "status": "bad_request",
                "error": "malformed_json",
                "message": f"Malformed JSON: {exc}",
            },
            status_code=400,
        )

    if not isinstance(body, dict):
        return JSONResponse(
            {
                "status": "bad_request",
                "error": "invalid_body_format",
                "message": "Root JSON payload must be an object.",
            },
            status_code=400,
        )

    # 1. Validate event_id
    if "event_id" not in body:
        return JSONResponse(
            {
                "status": "bad_request",
                "error": "missing_event_id",
                "message": "Field 'event_id' is required.",
            },
            status_code=422,
        )

    event_id = body["event_id"]
    if not isinstance(event_id, str) or not (1 <= len(event_id) <= 256):
        return JSONResponse(
            {
                "status": "bad_request",
                "error": "invalid_event_id",
                "message": "Field 'event_id' must be a non-empty string with length <= 256.",
            },
            status_code=422,
        )

    # 2. Validate timestamp
    if "timestamp" not in body:
        return JSONResponse(
            {
                "event_id": event_id,
                "status": "bad_request",
                "error": "missing_timestamp",
                "message": "Field 'timestamp' is required.",
            },
            status_code=422,
        )

    ts_raw = body["timestamp"]
    if isinstance(ts_raw, bool) or not isinstance(ts_raw, (int, float)):
        return JSONResponse(
            {
                "event_id": event_id,
                "status": Status.INVALID_TIMESTAMP.value,
                "error": "invalid_timestamp",
                "message": "Field 'timestamp' must be a valid numeric timestamp.",
            },
            status_code=400,
        )

    ts = float(ts_raw)
    if math.isnan(ts) or math.isinf(ts):
        return JSONResponse(
            {
                "event_id": event_id,
                "status": Status.INVALID_TIMESTAMP.value,
                "error": "invalid_timestamp",
                "message": "Field 'timestamp' must be a finite numeric timestamp.",
            },
            status_code=400,
        )

    # 3. Preserve payload and extract optional partition metadata
    payload = body.get("payload", None)
    partition_key = body.get("partition_key", None)
    fencing_epoch = body.get("fencing_epoch", None)
    is_internal = request.url.path.endswith("/internal")

    # 4. Ingest via PartitionManager if active, else single-process DedupEngine
    pm: Optional[PartitionManager] = state.get("partition_manager")
    event = Event(id=event_id, ts=ts, data=payload)
    partition_id = None

    try:
        if pm is not None:
            status, partition_id, error_msg = await asyncio.wait_for(
                pm.ingest_event(
                    event,
                    partition_key=partition_key,
                    fencing_epoch=fencing_epoch,
                    is_routed=is_internal,
                ),
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            if error_msg and status == Status.BUFFER_FULL:
                return JSONResponse(
                    {
                        "event_id": event_id,
                        "partition_id": partition_id,
                        "status": Status.BUFFER_FULL.value,
                        "error": "routing_or_capacity_unavailable",
                        "message": error_msg,
                        "retryable": True,
                    },
                    status_code=429 if "capacity" in error_msg.lower() else 503,
                )
            p_state = pm.partitions[partition_id]
            wm = p_state.engine.watermark if (p_state.is_owner and p_state.engine) else None
        else:
            e, _, _ = current()
            status = await asyncio.wait_for(
                e.ingest(event),
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            wm = None if e.watermark == float("-inf") else e.watermark
    except asyncio.TimeoutError:
        logger.error("Timeout ingesting event %s after %ss", event_id, REQUEST_TIMEOUT_SECONDS)
        return JSONResponse(
            {
                "event_id": event_id,
                "status": "error",
                "error": "gateway_timeout",
                "message": f"Engine ingestion timed out after {REQUEST_TIMEOUT_SECONDS}s.",
            },
            status_code=504,
        )
    except (KVError, SinkError) as exc:
        logger.warning("Downstream store error ingesting event %s: %s", event_id, exc)
        return JSONResponse(
            {
                "event_id": event_id,
                "status": "error",
                "error": "service_unavailable",
                "message": "Underlying storage or sink error; producer may retry.",
                "details": str(exc),
            },
            status_code=503,
        )
    except Exception as exc:
        logger.exception("Unexpected error ingesting event %s: %s", event_id, exc)
        return JSONResponse(
            {
                "event_id": event_id,
                "status": "error",
                "error": "internal_server_error",
                "message": "An unexpected server error occurred.",
            },
            status_code=500,
        )

    # 5. Map engine Status to explicit HTTP responses
    log.append({
        "id": event.id,
        "ts": event.ts,
        "data": event.data,
        "status": status.value,
        "at": time.time(),
        "watermark": wm,
        "partition_id": partition_id,
    })

    resp_data: dict[str, Any] = {"event_id": event_id, "status": status.value}
    if partition_id is not None:
        resp_data["partition_id"] = partition_id

    if status == Status.ACCEPTED:
        resp_data.update({
            "message": "Event claimed and write-ahead logged; delivery guaranteed.",
            "watermark": wm,
        })
        return JSONResponse(resp_data, status_code=202)

    if status == Status.DUPLICATE:
        resp_data.update({
            "message": "Event ID already recognized inside the deduplication window; ignored.",
            "watermark": wm,
        })
        return JSONResponse(resp_data, status_code=200)

    if status == Status.BUFFER_FULL:
        resp_data.update({
            "error": "buffer_full",
            "message": "Reorder buffer capacity exceeded. Temporary backpressure; producer may retry.",
            "retryable": True,
        })
        return JSONResponse(resp_data, status_code=429)

    if status == Status.TOO_OLD:
        resp_data.update({
            "error": "too_old",
            "message": "Event timestamp is older than sliding retention window; routed to DLQ.",
            "watermark": wm,
        })
        return JSONResponse(resp_data, status_code=410)

    if status == Status.TOO_FAR_FUTURE:
        resp_data.update({
            "error": "too_far_future",
            "message": "Event timestamp exceeds maximum permitted future clock skew; routed to DLQ.",
            "watermark": wm,
        })
        return JSONResponse(resp_data, status_code=422)

    if status == Status.INVALID_TIMESTAMP:
        resp_data.update({
            "error": "invalid_timestamp",
            "message": "Non-finite, non-numeric, or malformed timestamp.",
        })
        return JSONResponse(resp_data, status_code=400)

    return JSONResponse(resp_data, status_code=200)


# -----------------------------------------------------------------------------
# Phase 4 & Phase 5 Operational Endpoints: /health, /ready, /stats
# -----------------------------------------------------------------------------

@app.get("/health")
@app.get("/api/health")
async def health():
    """
    Liveness probe.
    Reports if the HTTP process is running and initialized.
    """
    if not state or not state.get("engine"):
        return JSONResponse(
            {"status": "starting", "message": "Service process initializing"},
            status_code=503,
        )
    e = state["engine"]
    pm = state.get("partition_manager")
    return {
        "status": "ok",
        "instance_id": state.get("instance_id", "local"),
        "watermark": None if e.watermark == float("-inf") else e.watermark,
        "partitions_active": pm.get_stats()["owned_partitions"] if pm else [],
        "uptime": time.time() - state.get("start_time", time.time()),
    }


@app.get("/ready")
async def ready():
    """
    Readiness probe.
    Actively checks connection to the backing KV store and sink availability.
    """
    if not state or not state.get("engine"):
        return JSONResponse(
            {"ready": False, "status": "unavailable", "message": "Engine not initialized"},
            status_code=503,
        )
    kv = state.get("kv")
    e = state.get("engine")
    if kv is None or e is None:
        return JSONResponse(
            {"ready": False, "status": "unavailable", "message": "Backing store not configured"},
            status_code=503,
        )

    try:
        # Probe backing KV store with a lightweight non-blocking query (get a probe key)
        await asyncio.wait_for(kv.get("health:probe"), timeout=2.0)
    except Exception as exc:
        logger.warning("Readiness probe failed backing store check: %s", exc)
        return JSONResponse(
            {
                "ready": False,
                "status": "unhealthy",
                "error": "backing_store_unreachable",
                "message": "Backing KV store check failed.",
            },
            status_code=503,
        )

    pm: Optional[PartitionManager] = state.get("partition_manager")
    pm_stats = pm.get_stats() if pm else None

    return {
        "ready": True,
        "status": "ready",
        "instance_id": state.get("instance_id", "local"),
        "buffered": pm_stats["total_buffered"] if pm_stats else len(e._heap),
        "reserved": pm_stats["total_reserved"] if pm_stats else e._reserved,
        "owned_partitions": pm_stats["owned_partitions"] if pm_stats else [0],
    }


@app.get("/stats")
@app.get("/api/stats")
async def stats():
    """
    Telemetry and statistics endpoint.
    Exposes metrics for ingestion, deduplication, rejection, buffer, and delivery,
    plus per-partition distributed telemetry.
    """
    e, kv, sink = current()
    d = e.stats()
    try:
        kv_sz = await asyncio.wait_for(kv.size(), timeout=2.0)
    except Exception:
        kv_sz = None

    sink_effects = len(sink.effects) if hasattr(sink, "effects") else None
    cfg = state.get("config", {})

    pm: Optional[PartitionManager] = state.get("partition_manager")
    pm_stats = pm.get_stats() if pm else None

    failed_count = (
        d.get("too_old", 0)
        + d.get("invalid_timestamp", 0)
        + d.get("too_far_future", 0)
        + d.get("buffer_overflow", 0)
        + d.get("flush_failures", 0)
    )

    d.update(
        instance_id=state.get("instance_id", "local"),
        watermark=None if e.watermark == float("-inf") else e.watermark,
        window=cfg.get("window", e.window),
        lateness=cfg.get("lateness", e.lateness),
        kv_keys=kv_sz,
        sink_effects=sink_effects,
        buffered=pm_stats["total_buffered"] if pm_stats else len(e._heap),
        reserved=pm_stats["total_reserved"] if pm_stats else e._reserved,
        dlq_size=pm_stats["total_dlq"] if pm_stats else len(e.dlq),
        failed=failed_count,
        distributed=pm_stats,
    )
    return d
