"""Tests covering the live event stream demo, automatic UUID idempotency IDs,
duplicate detection, retry idempotency, stream stopping, and regression of existing functionality.
"""
import uuid
import pytest
from fastapi.testclient import TestClient

from api import app, state


@pytest.fixture(autouse=True)
def reset_engine():
    """Ensure engine is clean before each test."""
    with TestClient(app) as client:
        client.post("/api/reset")
        yield client


def test_automatic_uuid_generation_and_ingestion(reset_engine):
    """Test generating UUIDv4 IDs and submitting them to the engine."""
    client = reset_engine
    
    # Generate 5 unique UUIDv4 idempotency keys
    events = [
        {
            "id": str(uuid.uuid4()),
            "ts": 1700000000.0 + i,
            "data": {
                "domain": "fintech",
                "type": "payment_settled",
                "amount": 25.50 * (i + 1),
                "currency": "USD"
            }
        }
        for i in range(5)
    ]
    
    # Ingest all unique events
    for ev in events:
        resp = client.post("/api/events", json=ev)
        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == ev["id"]
        assert data["status"] == "accepted"
        
    stats = client.get("/api/stats").json()
    assert stats["received"] == 5
    assert stats["duplicates"] == 0
    assert stats["delivered"] >= 0  # may be buffered or delivered depending on watermark


def test_duplicate_detection_reusing_uuid_and_payload(reset_engine):
    """Test that reusing the exact same UUID and payload is visibly flagged as DUPLICATE
    and produces strictly ONE side-effect in the sink."""
    client = reset_engine
    
    logical_event_id = str(uuid.uuid4())
    payload = {
        "domain": "logistics",
        "type": "package_routed",
        "carrier": "FedEx",
        "tracking": "TRK-987654"
    }
    
    ev1 = {"id": logical_event_id, "ts": 1700000000.0, "data": payload}
    
    # First delivery attempt
    res1 = client.post("/api/events", json=ev1)
    assert res1.status_code == 200
    assert res1.json()["status"] == "accepted"
    
    # Simulated duplicate delivery: exactly same ID and payload
    ev2 = {"id": logical_event_id, "ts": 1700000001.0, "data": payload}
    res2 = client.post("/api/events", json=ev2)
    assert res2.status_code == 200
    assert res2.json()["status"] == "duplicate"
    
    # Third duplicate delivery attempt
    res3 = client.post("/api/events", json=ev2)
    assert res3.status_code == 200
    assert res3.json()["status"] == "duplicate"
    
    # Drain buffer and verify sink effects
    client.post("/api/drain")
    stats = client.get("/api/stats").json()
    assert stats["received"] == 3
    assert stats["duplicates"] == 2
    assert stats["sink_effects"] == 1, "Only one effect must be recorded in the sink"


def test_client_retry_behavior_is_strictly_idempotent(reset_engine):
    """Simulate a network disconnect or client timeout where the client retries the same event."""
    client = reset_engine
    
    retry_id = str(uuid.uuid4())
    payload = {"domain": "inventory", "action": "reserve", "sku": "SKU-99"}
    event_data = {"id": retry_id, "ts": 1700000010.0, "data": payload}
    
    # Attempt 1: succeeds at server
    attempt1 = client.post("/api/events", json=event_data)
    assert attempt1.status_code == 200
    assert attempt1.json()["status"] == "accepted"
    
    # Client did not receive ACK (or retries intentionally), sends identical event again
    attempt2 = client.post("/api/events", json=event_data)
    assert attempt2.status_code == 200
    # Engine recognizes write-ahead log claim and returns duplicate
    assert attempt2.json()["status"] == "duplicate"
    
    # Flush reorder buffer
    client.post("/api/drain")
    sink = state["sink"]
    effects_with_id = [e for e in sink.effects if e.id == retry_id]
    assert len(effects_with_id) == 1, "Retried event must execute exactly once in sink"


def test_phase4_phase5_events_endpoint_with_uuid(reset_engine):
    """Verify POST /events endpoint with UUID and HTTP 202 / 200 status mapping."""
    client = reset_engine
    
    ev_id = str(uuid.uuid4())
    body = {
        "event_id": ev_id,
        "timestamp": 1700000020.0,
        "payload": {"type": "order_created", "total": 120.00}
    }
    
    # New event returns 202 Accepted
    r1 = client.post("/events", json=body)
    assert r1.status_code == 202
    assert r1.json()["status"] == "accepted"
    
    # Replay returns 200 Duplicate
    r2 = client.post("/events", json=body)
    assert r2.status_code == 200
    assert r2.json()["status"] == "duplicate"


def test_stats_metrics_and_failed_tracking(reset_engine):
    """Verify /api/stats accurately exposes received, duplicates, delivered, and failed."""
    client = reset_engine
    
    # Submit 1 accepted, 1 duplicate
    uid = str(uuid.uuid4())
    client.post("/api/events", json={"id": uid, "ts": 1700000050.0, "data": {}})
    client.post("/api/events", json={"id": uid, "ts": 1700000051.0, "data": {}})
    
    # Submit 1 too old (watermark advances)
    client.post("/api/events", json={"id": str(uuid.uuid4()), "ts": 1700000200.0, "data": {}})
    # Now watermark is ~1700000198; submit event older than 1700000198 - 60 = 1700000138
    r_old = client.post("/events", json={
        "event_id": str(uuid.uuid4()),
        "timestamp": 1700000010.0,  # too old
        "payload": {}
    })
    assert r_old.status_code == 410
    
    stats = client.get("/api/stats").json()
    assert "received" in stats
    assert "duplicates" in stats
    assert "delivered" in stats
    assert "failed" in stats
    assert stats["failed"] >= 1  # at least the too_old event


def test_dashboard_ui_and_static_assets(reset_engine):
    """Verify that index.html and its static assets are served properly."""
    client = reset_engine
    
    # Check index.html
    resp = client.get("/")
    assert resp.status_code == 200
    html = resp.text
    assert "Continuous Live Event Stream Simulator" in html
    assert "AUTO UUID" in html
    assert "Live event feed" in html
    assert "TOTAL EVENTS RECEIVED" in html
    assert "UNIQUE EVENTS PROCESSED" in html
    assert "DUPLICATES DETECTED" in html
    assert "FAILED EVENTS" in html
    assert "status-badge" in html
    
    # Check assets
    css = client.get("/assets/styles.css")
    assert css.status_code == 200
    assert ".decision.duplicate" in css.text
    assert ".decision.accepted" in css.text
    assert ".decision.pending" in css.text
    
    js = client.get("/assets/app.js")
    assert js.status_code == 200
    assert "StreamProducer" in js.text
    assert "generateEventId" in js.text


def test_regression_existing_endpoints(reset_engine):
    """Ensure all existing operational and lab endpoints remain functional."""
    client = reset_engine
    
    # Health probe
    h = client.get("/health")
    assert h.status_code == 200
    assert h.json()["status"] == "ok"
    
    # Ready probe
    r = client.get("/ready")
    assert r.status_code == 200
    assert r.json()["ready"] is True
    
    # Events listing
    ev_list = client.get("/api/events?limit=50")
    assert ev_list.status_code == 200
    assert isinstance(ev_list.json(), list)
    
    # Drain
    dr = client.post("/api/drain", json={})
    assert dr.status_code == 200
    assert "remaining" in dr.json()
    
    # Demo
    dm = client.post("/api/demo", json={})
    assert dm.status_code == 200
    assert len(dm.json()["events"]) == 5
    
    # Benchmark
    bench = client.post("/api/benchmark", json={"count": 50, "duplicate_ratio": 0.4})
    assert bench.status_code == 200
    assert bench.json()["exactly_once"] is True
    
    # Fault lab
    fault = client.post("/api/fault-lab", json={"count": 50, "duplicate_ratio": 0.4})
    assert fault.status_code == 200
    assert fault.json()["exactly_once"] is True
