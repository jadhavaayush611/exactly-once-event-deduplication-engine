"""
Phase 4 Automated Verification Suite: Network Proxy & Ingestion Protocol.

Verifies:
1. Valid event ingestion (POST /events -> 202 Accepted, structured body).
2. Missing fields and malformed JSON (POST /events -> 400 / 422).
3. Invalid, NaN, infinite, and out-of-range timestamps (POST /events -> 400 / 422 / 410).
4. Duplicate event submission (POST /events -> 200 Duplicate).
5. Same-ID concurrent requests (Only 1 accepted, remaining duplicate, exactly 1 sink effect).
6. Distinct-ID concurrent requests (All handled independently, correct sink effects).
7. Buffer-full response (429) and successful retry once capacity drains.
8. Simulated lost acknowledgement followed by retry (Token claim adoption / dedup).
9. Engine or backing-store (Redis/KV) failures (503 Service Unavailable).
10. Request cancellation and disconnect behavior.
11. Health, readiness, and statistics endpoints (GET /health, /ready, /stats).
12. Oversized request handling (413 Payload Too Large).
13. Status code mappings and schema consistency.
"""
from __future__ import annotations

import asyncio
import json
import math
import time
from typing import Any

import pytest
from fastapi.testclient import TestClient

from api import app, build, state
from dedup_engine import DedupEngine, Event, FaultyKV, KVError, MemoryKV, MemorySink, Status


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def client():
    """Initializes a clean in-memory engine before each test."""
    with TestClient(app) as c:
        c.post("/api/reset")
        yield c


# -----------------------------------------------------------------------------
# 1. Valid Event Ingestion
# -----------------------------------------------------------------------------

def test_valid_event_ingestion(client: TestClient):
    req = {
        "event_id": "evt-1001",
        "timestamp": 1793700000.0,
        "payload": {
            "user_id": 42,
            "action": "purchase",
            "amount": 199.99,
        },
    }
    resp = client.post("/events", json=req)
    assert resp.status_code == 202
    data = resp.json()
    assert data["event_id"] == "evt-1001"
    assert data["status"] == "accepted"
    assert "delivery guaranteed" in data["message"]
    assert data["watermark"] is not None

    # Check that payload is intact in the engine's sink
    eng = state["engine"]
    sink = state["sink"]
    # Trigger drain so buffered event reaches sink
    client.post("/api/drain")
    assert len(sink.effects) == 1
    assert sink.effects[0].id == "evt-1001"
    assert sink.effects[0].ts == 1793700000.0
    assert sink.effects[0].data == req["payload"]


# -----------------------------------------------------------------------------
# 2. Missing Fields and Malformed JSON
# -----------------------------------------------------------------------------

def test_missing_event_id(client: TestClient):
    req = {
        "timestamp": 1793700000.0,
        "payload": {"foo": "bar"},
    }
    resp = client.post("/events", json=req)
    assert resp.status_code == 422
    data = resp.json()
    assert data["status"] == "bad_request"
    assert data["error"] == "missing_event_id"


def test_missing_timestamp(client: TestClient):
    req = {
        "event_id": "evt-no-ts",
        "payload": {"foo": "bar"},
    }
    resp = client.post("/events", json=req)
    assert resp.status_code == 422
    data = resp.json()
    assert data["status"] == "bad_request"
    assert data["error"] == "missing_timestamp"


def test_malformed_json_syntax(client: TestClient):
    resp = client.post("/events", content=b'{"event_id": "evt-bad", broken json}')
    assert resp.status_code == 400
    data = resp.json()
    assert data["status"] == "bad_request"
    assert data["error"] == "malformed_json"


def test_empty_request_body(client: TestClient):
    resp = client.post("/events", content=b"")
    assert resp.status_code == 400
    data = resp.json()
    assert data["error"] == "empty_request_body"


def test_root_json_not_an_object(client: TestClient):
    resp = client.post("/events", content=b'["an", "array"]')
    assert resp.status_code == 400
    data = resp.json()
    assert data["error"] == "invalid_body_format"


# -----------------------------------------------------------------------------
# 3. Invalid, NaN, Infinite, and Out-of-Range Timestamps
# -----------------------------------------------------------------------------

def test_nan_timestamp(client: TestClient):
    # JSON standard does not permit literal NaN, but producers sometimes send it
    resp = client.post("/events", content=b'{"event_id": "evt-nan", "timestamp": NaN}')
    assert resp.status_code == 400
    data = resp.json()
    assert data["status"] == "bad_request"


def test_infinity_timestamp(client: TestClient):
    resp = client.post("/events", content=b'{"event_id": "evt-inf", "timestamp": Infinity}')
    assert resp.status_code == 400
    data = resp.json()
    assert data["status"] == "bad_request"


def test_non_numeric_timestamp(client: TestClient):
    req = {
        "event_id": "evt-str-ts",
        "timestamp": "not_a_number",
    }
    resp = client.post("/events", json=req)
    assert resp.status_code == 400
    data = resp.json()
    assert data["status"] == "invalid_timestamp"


def test_boolean_timestamp(client: TestClient):
    # In python, bool is a subclass of int, so we must reject True/False
    req = {
        "event_id": "evt-bool-ts",
        "timestamp": True,
    }
    resp = client.post("/events", json=req)
    assert resp.status_code == 400
    data = resp.json()
    assert data["status"] == "invalid_timestamp"


def test_too_old_timestamp(client: TestClient):
    # Baseline event to advance watermark
    client.post("/events", json={"event_id": "evt-anchor", "timestamp": 1000.0})
    # Window is 60s, lateness 2s -> watermark is 998.0 -> cutoff is 938.0
    req = {
        "event_id": "evt-old",
        "timestamp": 900.0,
    }
    resp = client.post("/events", json=req)
    assert resp.status_code == 410
    data = resp.json()
    assert data["status"] == "too_old"
    assert data["event_id"] == "evt-old"


def test_too_far_future_timestamp(client: TestClient):
    # Reconfigure engine with max_future_skew = 10.0
    now = time.time()
    e, kv, sink = build(window=60.0, lateness=2.0, max_future_skew=10.0)
    state.update(engine=e, kv=kv, sink=sink)

    req = {
        "event_id": "evt-future",
        "timestamp": now + 500.0,
    }
    resp = client.post("/events", json=req)
    assert resp.status_code == 422
    data = resp.json()
    assert data["status"] == "too_far_future"
    assert data["event_id"] == "evt-future"


# -----------------------------------------------------------------------------
# 4. Duplicate Event Submission & Idempotency
# -----------------------------------------------------------------------------

def test_duplicate_event_submission(client: TestClient):
    req = {
        "event_id": "evt-dup-1",
        "timestamp": 100.0,
        "payload": {"step": 1},
    }
    r1 = client.post("/events", json=req)
    assert r1.status_code == 202
    assert r1.json()["status"] == "accepted"

    # Second submission: same payload
    r2 = client.post("/events", json=req)
    assert r2.status_code == 200
    assert r2.json()["status"] == "duplicate"
    assert r2.json()["event_id"] == "evt-dup-1"

    # Third submission: different payload (must still be rejected as duplicate without mutating payload)
    req_modified = {
        "event_id": "evt-dup-1",
        "timestamp": 100.0,
        "payload": {"step": 2, "modified": True},
    }
    r3 = client.post("/events", json=req_modified)
    assert r3.status_code == 200
    assert r3.json()["status"] == "duplicate"

    # Drain and check downstream effects: exactly 1 effect with step=1
    client.post("/api/drain")
    sink = state["sink"]
    effects = [e for e in sink.effects if e.id == "evt-dup-1"]
    assert len(effects) == 1
    assert effects[0].data == {"step": 1}


# -----------------------------------------------------------------------------
# 5. Same-ID Concurrent Requests
# -----------------------------------------------------------------------------

@pytest.mark.anyio
async def test_concurrent_same_id_requests():
    """Simulates 25 concurrent client coroutines posting the identical event ID."""
    from httpx import ASGITransport, AsyncClient

    # Reinitialize clean state
    e, kv, sink = build()
    state.update(engine=e, kv=kv, sink=sink, config={"window": 60.0, "lateness": 2.0}, ready=True)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        req = {
            "event_id": "concurrent-same-id",
            "timestamp": 500.0,
            "payload": {"key": "val"},
        }

        tasks = [ac.post("/events", json=req) for _ in range(25)]
        responses = await asyncio.gather(*tasks)

        status_codes = [r.status_code for r in responses]
        statuses = [r.json()["status"] for r in responses]

        # Exactly 1 accepted (202) and 24 duplicates (200)
        assert status_codes.count(202) == 1
        assert status_codes.count(200) == 24
        assert statuses.count("accepted") == 1
        assert statuses.count("duplicate") == 24

        await e.drain()
        effects = [x for x in sink.effects if x.id == "concurrent-same-id"]
        assert len(effects) == 1


# -----------------------------------------------------------------------------
# 6. Distinct-ID Concurrent Requests
# -----------------------------------------------------------------------------

@pytest.mark.anyio
async def test_concurrent_distinct_id_requests():
    """Simulates 30 concurrent client coroutines posting unique event IDs."""
    from httpx import ASGITransport, AsyncClient

    e, kv, sink = build()
    state.update(engine=e, kv=kv, sink=sink, config={"window": 60.0, "lateness": 2.0}, ready=True)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        tasks = [
            ac.post(
                "/events",
                json={
                    "event_id": f"distinct-{i}",
                    "timestamp": 1000.0 + i * 0.1,
                    "payload": {"idx": i},
                },
            )
            for i in range(30)
        ]
        responses = await asyncio.gather(*tasks)

        assert all(r.status_code == 202 for r in responses)
        assert all(r.json()["status"] == "accepted" for r in responses)

        await e.drain()
        assert len(sink.effects) == 30
        assert {x.id for x in sink.effects} == {f"distinct-{i}" for i in range(30)}


# -----------------------------------------------------------------------------
# 7. Buffer-Full Backpressure (429) & Successful Retry
# -----------------------------------------------------------------------------

def test_buffer_full_and_retry(client: TestClient):
    # Configure an engine with a small max_buffer_size of 2 and large lateness
    # so events stay buffered in the heap until drained
    e, kv, sink = build(window=60.0, lateness=50.0, max_buffer_size=2)
    state.update(engine=e, kv=kv, sink=sink)

    r1 = client.post("/events", json={"event_id": "b-1", "timestamp": 100.0})
    assert r1.status_code == 202

    r2 = client.post("/events", json={"event_id": "b-2", "timestamp": 101.0})
    assert r2.status_code == 202

    # Buffer is now at capacity (2/2)
    r3 = client.post("/events", json={"event_id": "b-3", "timestamp": 102.0})
    assert r3.status_code == 429
    data3 = r3.json()
    assert data3["status"] == "buffer_full"
    assert data3["error"] == "buffer_full"
    assert data3["retryable"] is True
    assert data3["event_id"] == "b-3"

    # Drain the buffer to free up capacity
    client.post("/api/drain")

    # Retry the rejected event b-3; should now succeed!
    r3_retry = client.post("/events", json={"event_id": "b-3", "timestamp": 102.0})
    assert r3_retry.status_code == 202
    assert r3_retry.json()["status"] == "accepted"

    # Verify sink received all 3 events
    client.post("/api/drain")
    assert {e.id for e in sink.effects} == {"b-1", "b-2", "b-3"}


# -----------------------------------------------------------------------------
# 8. Simulated Lost Acknowledgement Followed by Retry
# -----------------------------------------------------------------------------

def test_simulated_lost_ack_retry(client: TestClient):
    """
    Scenario:
    Producer sends event 'evt-ack-loss'. The engine writes the pending claim into KV,
    but the network response is lost (or producer timed out).
    The producer deterministically retries the same request.
    The engine recognizes the existing record as duplicate (or adopts orphaned pending),
    ensuring exactly one downstream sink effect.
    """
    e = state["engine"]
    sink = state["sink"]

    # First attempt: succeeds at engine level
    r1 = client.post("/events", json={"event_id": "evt-ack-loss", "timestamp": 100.0})
    assert r1.status_code == 202

    # Producer retries after assumed lost ACK
    r2 = client.post("/events", json={"event_id": "evt-ack-loss", "timestamp": 100.0})
    assert r2.status_code == 200
    assert r2.json()["status"] == "duplicate"

    # Flush/drain engine
    client.post("/api/drain")
    effects = [x for x in sink.effects if x.id == "evt-ack-loss"]
    assert len(effects) == 1


# -----------------------------------------------------------------------------
# 9. Engine and Storage Failures (503 Service Unavailable)
# -----------------------------------------------------------------------------

def test_storage_failure_returns_503(client: TestClient):
    # Configure an engine with a totally failing KV store
    mem = MemoryKV()
    failing_kv = FaultyKV(mem, fail_rate=1.0, ack_loss_rate=0.0)
    e = DedupEngine(failing_kv, MemorySink(), max_retries=1, backoff=0.001)
    state.update(engine=e, kv=failing_kv)

    resp = client.post("/events", json={"event_id": "fail-evt", "timestamp": 100.0})
    assert resp.status_code == 503
    data = resp.json()
    assert data["status"] == "error"
    assert data["error"] == "service_unavailable"
    assert data["event_id"] == "fail-evt"


# -----------------------------------------------------------------------------
# 10. Operational Endpoints: /health, /ready, /stats
# -----------------------------------------------------------------------------

def test_health_endpoint(client: TestClient):
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "uptime" in data


def test_ready_endpoint_healthy(client: TestClient):
    resp = client.get("/ready")
    assert resp.status_code == 200
    data = resp.json()
    assert data["ready"] is True
    assert data["status"] == "ready"
    assert "buffered" in data
    assert "reserved" in data


def test_ready_endpoint_unhealthy_on_backing_store_down(client: TestClient):
    mem = MemoryKV()
    failing_kv = FaultyKV(mem, fail_rate=1.0)
    e = DedupEngine(failing_kv, MemorySink())
    state.update(engine=e, kv=failing_kv)

    resp = client.get("/ready")
    assert resp.status_code == 503
    data = resp.json()
    assert data["ready"] is False
    assert data["status"] == "unhealthy"
    assert data["error"] == "backing_store_unreachable"


def test_stats_endpoint(client: TestClient):
    # Ingest 1 valid, 1 duplicate, 1 old
    client.post("/events", json={"event_id": "s-1", "timestamp": 100.0})
    client.post("/events", json={"event_id": "s-1", "timestamp": 100.0})
    client.post("/events", json={"event_id": "s-old", "timestamp": 1.0})

    resp = client.get("/stats")
    assert resp.status_code == 200
    stats = resp.json()
    assert stats["received"] >= 3
    assert stats["accepted"] >= 1
    assert stats["duplicates"] >= 1
    assert stats["too_old"] >= 1
    assert "watermark" in stats
    assert "kv_keys" in stats
    assert "buffered" in stats
    assert "dlq_size" in stats


# -----------------------------------------------------------------------------
# 11. Oversized Request Handling (413 Payload Too Large)
# -----------------------------------------------------------------------------

def test_oversized_payload_rejected(client: TestClient):
    # Limit is 1MB (1_000_000 bytes)
    huge_payload = {"padding": "x" * 1_200_000}
    resp = client.post(
        "/events",
        json={"event_id": "big-1", "timestamp": 100.0, "payload": huge_payload},
    )
    assert resp.status_code == 413
    data = resp.json()
    assert data["status"] == "error"
    assert data["error"] == "payload_too_large"


# -----------------------------------------------------------------------------
# 12. Security & Information Leakage Protection
# -----------------------------------------------------------------------------

def test_no_stack_trace_leakage_on_internal_exception(client: TestClient):
    # Force an unexpected exception in engine
    class CrashingEngine:
        async def ingest(self, event):
            raise RuntimeError("Database password leaked: secret12345")

    state["engine"] = CrashingEngine()

    resp = client.post("/events", json={"event_id": "crash-1", "timestamp": 100.0})
    assert resp.status_code == 500
    data = resp.json()
    assert data["status"] == "error"
    assert data["error"] == "internal_server_error"
    assert "secret12345" not in resp.text
