# Acceptance Criteria: Exactly-Once Event Deduplication Engine

**Status**: Verified Baseline Established (Phase 2 Complete)  
**Revision Date**: October 28, 2026  
**Applicability**: Phases 1–7 Roadmap

---

## 1. Deduplication & Correctness Requirements

| ID | Criterion | Measurable Specification | Current Status | Verification Source |
|---|---|---|---|---|
| **AC-CORR-01** | Duplicate Blocking | When identical `event.id` is submitted $N$ times ($N \ge 2$) within retention window, exactly 1 submission returns `Status.ACCEPTED` and $N-1$ submissions return `Status.DUPLICATE`. | **VERIFIED** | `tests/test_correctness.py::test_basic_dedup` |
| **AC-CORR-02** | Concurrent Duplicate Race | Under concurrent ingestion of 200 identical `event.id` submissions across parallel coroutines, exactly 1 claim succeeds and 199 are flagged as duplicates. | **VERIFIED** | `tests/test_correctness.py::test_concurrent_duplicates_exactly_one_wins` |
| **AC-CORR-03** | Lost ACK Idempotent Claim | When a claim write succeeds in the state store but the network ACK is lost, a retry carrying the same ownership token must recognize its own claim and return `Status.ACCEPTED` without double-claiming. | **VERIFIED** | `tests/test_fault_injection.py::test_claim_applied_but_ack_lost_is_not_mistaken_for_duplicate` |
| **AC-CORR-04** | Orphaned Claim Adoption | When a claim write succeeds but the client exhausts retries before receiving confirmation, a subsequent resend by another client must adopt the unbuffered pending claim rather than discarding it as a dead duplicate. | **VERIFIED** | `tests/test_fault_injection.py::test_orphaned_pending_claim_is_adopted_on_client_resend` |
| **AC-CORR-05** | Randomized Ingestion Integrity | Across 10 seeded pseudo-random streams with 50% duplicate ratio and bounded out-of-order jitter, zero duplicate events reach the sink and total unique delivered equals total unique submitted. | **VERIFIED** | `tests/test_correctness.py::test_random_stream_exactly_once_and_ordered` (Seeds 0–9) |
| **AC-CORR-06** | Clock Skew Guard | An event with timestamp exceeding wall-clock time by more than `max_future_skew` seconds must return `Status.TOO_FAR_FUTURE`, route to DLQ, and leave `_max_ts` and watermark unchanged. | **VERIFIED** | `tests/test_window_semantics.py::test_far_future_event_does_not_advance_watermark_or_evict` |
| **AC-CORR-07** | Timestamp Validity Guard | Non-finite (`NaN`, `+inf`, `-inf`) or non-numeric timestamps must return `Status.INVALID_TIMESTAMP`, route to DLQ, and leave engine state unchanged. | **VERIFIED** | `tests/test_window_semantics.py::test_invalid_and_non_finite_timestamps` |
| **AC-CORR-08** | Duplicate ID Invariance | Duplicate IDs submitted with conflicting timestamps or modified payloads must return `Status.DUPLICATE` without overwriting the original event payload or eviction score. | **VERIFIED** | `tests/test_window_semantics.py::test_duplicate_ids_with_conflicting_timestamps_or_payloads` |

---

## 2. Sliding Window, Watermark & Ordering Semantics

| ID | Criterion | Measurable Specification | Current Status | Verification Source |
|---|---|---|---|---|
| **AC-WIND-01** | Event-Time Ordering | Buffered events within allowed lateness must be emitted to the sink in strictly ascending order of `event.ts`. | **VERIFIED** | `tests/test_correctness.py::test_out_of_order_is_released_in_event_time_order`, `tests/test_window_semantics.py::test_valid_out_of_order_events` |
| **AC-WIND-02** | Watermark Hold Condition | An event with timestamp $t$ must remain buffered in the reorder buffer until watermark $W = \text{max\_ts} - \text{lateness} \ge t$. | **VERIFIED** | `tests/test_correctness.py::test_events_are_held_until_watermark_passes`, `tests/test_window_semantics.py::test_normal_in_order_events` |
| **AC-WIND-03** | Late Arrival Inside Window | An event arriving with $W - \text{window} \le t < W$ must be accepted, marked late (`m.late_emitted`), deduplicated, and released immediately. | **VERIFIED** | `tests/test_correctness.py::test_late_event_inside_window_is_emitted_and_still_deduplicated`, `tests/test_window_semantics.py::test_late_events_within_accepted_lateness` |
| **AC-WIND-04** | Expired Event Rejection | An event arriving with $t < W - \text{window}$ must return `Status.TOO_OLD` and be diverted to the Dead-Letter Queue without sink delivery. | **VERIFIED** | `tests/test_correctness.py::test_event_older_than_window_goes_to_dlq`, `tests/test_window_semantics.py::test_events_beyond_lateness_limit` |
| **AC-WIND-05** | State Expiry Bounding | Over an ingestion stream spanning multiple window durations, total active keys in the state store must scale with window duration ($\approx \text{rate} \times \text{window}$) rather than total events processed. | **VERIFIED** | `tests/test_correctness.py::test_eviction_keeps_state_bounded` |
| **AC-WIND-06** | Pending Protection | State store records with `state == "pending"` must never be deleted by eviction sweeps regardless of age until transitioned to `committed`. | **VERIFIED** | `tests/test_correctness.py::test_pending_records_are_never_evicted` |
| **AC-WIND-07** | Exact Expiration Boundary | Events with $t == W - \text{window}$ are accepted; events with $t = W - \text{window} - \epsilon$ return `Status.TOO_OLD`. `evict_before(cutoff)` removes keys with $\text{score} < \text{cutoff}$. | **VERIFIED** | `tests/test_window_semantics.py::test_events_at_exact_expiration_boundary_and_either_side` |
| **AC-WIND-08** | Watermark Invariance on Rejection | Ingestion calls resulting in `TOO_OLD`, `TOO_FAR_FUTURE`, `INVALID_TIMESTAMP`, or `DUPLICATE` must not alter `_max_ts` or the watermark. | **VERIFIED** | `tests/test_window_semantics.py::test_watermark_behavior_after_rejected_events` |
| **AC-WIND-09** | Post-Eviction Replay Protection | Replaying an event with original timestamp after its state has been evicted returns `Status.TOO_OLD` (DLQ), producing zero duplicate downstream side-effects. | **VERIFIED** | `tests/test_window_semantics.py::test_state_eviction_and_replay_after_expiration` |

---

## 3. Memory & Resource Bounding Requirements

| ID | Criterion | Measurable Specification | Current Status | Verification Source |
|---|---|---|---|---|
| **AC-MEM-01** | Reorder Buffer Bounding | When configured with `max_buffer_size`, attempting to buffer beyond capacity returns `Status.BUFFER_FULL`, increments `m.buffer_overflow`, and prevents heap growth. | **VERIFIED** | `tests/test_window_semantics.py::test_bounded_cleanup_and_buffer_overflow` |
| **AC-MEM-02** | Bounded Maintenance Eviction | When configured with `max_evict_per_cycle`, `_maintenance` deletes at most the specified limit per cycle, bounding single-pass cleanup latency. | **VERIFIED** | `tests/test_window_semantics.py::test_bounded_cleanup_and_buffer_overflow` |

---

## 4. Distributed Coordination & Partitioning

| ID | Criterion | Measurable Specification | Current Status | Verification Source |
|---|---|---|---|---|
| **AC-DIST-01** | Single-Writer Partitioning | In a multi-worker cluster, all events bearing the same partition key (or hash of `event.id`) route deterministically to a single active owner instance using CRC32 partitioning to maintain reorder buffer consistency. | **VERIFIED** | `tests/test_distributed_p5.py::test_deterministic_partition_assignment` |
| **AC-DIST-02** | Cross-Instance Dedup Atomicity | When multiple workers share a central Redis cluster, concurrent claims for the same `event.id` across distinct workers guarantee that exactly 1 worker succeeds. | **VERIFIED** | `tests/test_distributed_p5.py::test_multi_instance_concurrent_same_id_dedup` |
| **AC-DIST-03** | Fencing Token Monotonicity & Failover | Redis-backed partition leases enforce single ownership with monotonically increasing epochs. Stale owners are fenced from renewals; backup nodes automatically acquire expired leases and run recovery. | **VERIFIED** | `tests/test_distributed_p5.py::test_fencing_token_and_stale_owner_rejection`, `test_failover_and_pending_recovery` |
| **AC-REDIS-01** | Atomic Record & Index Mutations | RedisKV operations (`put_if_absent`, `set`, `evict_before`, `delete`) execute atomically via Lua scripts. Record mutations and `idx:dedup` sorted set mutations never tear or leave partial state. | **VERIFIED** | `tests/test_redis_integration.py` (12/12 passing against live Redis) |
| **AC-REDIS-02** | Lost-ACK Idempotency in Redis | Redis Lua script execution followed by dropped client ACK is safely recovered by retrying with the same ownership token without double-claiming. | **VERIFIED** | `tests/test_redis_integration.py::test_script_execution_followed_by_lost_acknowledgement` |
| **AC-REC-01** | Dedicated Pending Index | State store maintains dedicated `idx:dedup:pending` sorted set alongside `idx:dedup`. Transitions between pending and committed atomically add/remove across indexes. | **VERIFIED** | `tests/test_recovery_p3c.py` (16/16 passing) |
| **AC-REC-02** | Bounded Incremental Recovery | Engine recovers pending events in bounded batches (`recovery_batch_size`), strictly respecting `max_buffer_size` and capacity reservations (`_reserved`). | **VERIFIED** | `tests/test_recovery_p3c.py::test_recovery_respects_max_buffer_size` |
| **AC-REC-03** | Stale & Malformed Index Cleanup | Orphaned pending index entries pointing to missing, committed, or corrupted records are pruned during recovery without hanging or throwing unhandled exceptions. | **VERIFIED** | `tests/test_recovery_p3c.py::test_recovery_handles_missing_record_safely`, `test_recovery_handles_corrupt_record_json` |
| **AC-REC-04** | Recovery Scalability | Recovery over 1k, 10k, and 100k pending records retrieves in bounded batches via chunked Lua scripts without full keyspace scans or unbounded memory consumption. | **VERIFIED** | `bench/benchmark_recovery.py` |

---

## 5. Fault Tolerance, Retries & Crash Recovery

| ID | Criterion | Measurable Specification | Current Status | Verification Source |
|---|---|---|---|---|
| **AC-FLT-01** | Transient KV Failure Recovery | Transient read/write errors on the state store must trigger exponential backoff retries (up to `max_retries`) and succeed without event loss. | **VERIFIED** | `tests/test_fault_injection.py::test_claim_request_lost_is_retried` |
| **AC-FLT-02** | Total KV Outage Handling | When the state store is completely unreachable, ingestion must reject incoming events with `KVError`, leaving no orphaned deliveries or false duplicates upon service restoration. | **VERIFIED** | `tests/test_fault_injection.py::test_total_kv_outage_loses_nothing_and_duplicates_nothing` |
| **AC-FLT-03** | Downstream Sink Outage Buffering | During sink downtime, claimed events must remain safely buffered in the reorder buffer and PENDING in KV, draining successfully once the sink recovers. | **VERIFIED** | `tests/test_fault_injection.py::test_sink_outage_keeps_events_buffered_then_delivers` |
| **AC-FLT-04** | Process Crash Recovery at All Stages | Simulating an abrupt crash at any of the 5 pipeline points (`after_claim`, `after_buffer`, `before_deliver`, `after_deliver`, `after_commit`) followed by `recover()` and producer replay must yield zero lost events and zero duplicate effects. | **VERIFIED** | `tests/test_fault_injection.py::test_crash_at_every_point_then_recover` (5/5 points verified) |
| **AC-FLT-05** | Recovery Idempotency | Executing `engine.recover()` repeatedly must be idempotent, recovering stranded pending events on the first call and 0 on subsequent calls without re-delivering. | **VERIFIED** | `tests/test_fault_injection.py::test_recovery_is_idempotent` |
| **AC-FLT-06** | Continuous Chaos Resilience | Across 20 randomized chaos scenarios combining 5% KV request loss, 5% KV ack loss, 5% sink request loss, 5% sink ack loss, 2% unhandled crash rate, and producer replay: $\text{effects} == \text{unique inputs}$ with zero duplicate side-effects. | **VERIFIED** | `tests/test_fault_injection.py::test_chaos_random_crashes_and_flaky_kv_and_sink` (20/20 seeds verified) |

---

## 6. Downstream Sink Idempotency

| ID | Criterion | Measurable Specification | Current Status | Verification Source |
|---|---|---|---|---|
| **AC-SINK-01** | Sink Idempotency Enforcement | The downstream sink must deduplicate actions keyed on `event.id`, preventing repeated side-effects when an event is redelivered following commit failure or crash recovery. | **VERIFIED** | `dedup_engine/sink.py:MemorySink`, `test_fault_injection.py:test_naive_sink_shows_why_idempotent_sink_is_required`, `tests/test_sinks_p3d.py` |
| **AC-SINK-02** | File Sink Persistence & Recovery | `FileSink` must record delivery to append-only JSONL format, support configurable `fsync`, recover seen IDs on restart, prune torn tails at EOF, fail closed with quarantine on internal corruption, and support cross-process idempotency via advisory locking. | **VERIFIED** | `tests/test_sinks_p3d.py::test_filesink_restart_and_reconstruction`, `test_filesink_torn_tail_recovery`, `test_filesink_cross_process_idempotency_two_processes`, `test_filesink_internal_corrupted_lines_quarantine_and_fail_closed` |
| **AC-SINK-03** | Engine-to-Sink Commit Ordering | Sink failure prevents premature KV commit. A crash after delivery but before commit leaves the event pending in KV, redelivering safely to idempotent sinks upon recovery without duplicate effects. | **VERIFIED** | `tests/test_sinks_p3d.py::test_sink_failure_prevents_premature_commit`, `test_crash_between_deliver_and_commit_redelivers_safely` |
| **AC-SINK-04** | StdoutSink Non-Transactional Boundary | StdoutSink explicitly documents non-transactional external stream semantics, offering optional bounded in-memory deduplication (`dedup=True`) for process-local filtering. | **VERIFIED** | `tests/test_sinks_p3d.py::test_stdout_sink_behavior` |

---

## 7. Benchmarking & Performance Thresholds

| ID | Criterion | Target Threshold | Baseline Verified Value | Current Status |
|---|---|---|---|---|
| **AC-PERF-01** | In-Memory Engine Throughput | $\ge 15,000$ events/second at concurrency $\ge 16$ (0ms KV latency). | **17,571–20,101 events/s** | **VERIFIED** |
| **AC-PERF-02** | Simulated Cloud KV Throughput | $\ge 10,000$ events/second at concurrency 128 (0.5ms KV RTT). | **15,885 events/s** | **VERIFIED** |
| **AC-PERF-03** | In-Memory Latency Percentile | $\text{p95} \le 1.0\text{ms}$ at 16 concurrency (0ms KV latency). | **1.77 ms** | **VERIFIED** |
| **AC-PERF-04** | State Eviction Linearity | Peak state store keys must remain within $\pm 15\%$ of $\text{spacing}^{-1} \times \text{window}$ events. | **Verified bounded** (130 keys for 5s, 654 keys for 30s) | **VERIFIED** |
| **AC-PERF-05** | Real Redis 7 Throughput | $\ge 5,000$ events/second against external Redis instance over loopback. | **UNVERIFIED** | Automated test suite currently uses `MemoryKV`; live Redis benchmark target for Phase 3. |

---

## 8. Deployment, Packaging & Operational Observability

| ID | Criterion | Measurable Specification | Current Status | Verification Source |
|---|---|---|---|---|
| **AC-OPS-01** | Automated Test Suite | `python -m pytest` passes 100% of collected tests with zero test failures across supported platforms. | **VERIFIED** | 158/158 passed in 178.00s on Python 3.14.6 |
| **AC-OPS-02** | NDJSON Network Proxy | Real TCP client streaming JSON lines to proxy port must receive `{"id":..., "status":...}` responses matching engine decisions. | **VERIFIED** | `tests/test_proxy.py::test_proxy_end_to_end` |
| **AC-OPS-03** | Dashboard Health & Telemetry | HTTP GET `/api/stats` and `/api/health` return HTTP 200 with accurate watermark, count, retry, and window telemetry. | **VERIFIED** | `tests/test_api.py::test_dashboard_and_config_endpoints` |
| **AC-OPS-04** | Redis Container Compose | `docker compose up -d` launches an AOF-enabled Redis 7 instance accessible at `redis://localhost:6379/0`. | **PARTIALLY VERIFIED** | `docker-compose.yml` validated; host port collision noted during audit. |
| **AC-OPS-05** | Production Metrics Export | Expose Prometheus-compliant metrics endpoint `/metrics` reporting ingestion, deduplication, latency, and eviction counters. | **UNVERIFIED** | Planned for Phase 7. |
| **AC-OPS-06** | Graceful Connection Teardown | Asyncio TCP server cleanly drains pending buffers and closes sockets without unawaited transport warnings. | **PARTIALLY VERIFIED** | Implemented in `proxy.py`; `wait_closed()` hardening in Phase 2. |
| **AC-NET-01** | HTTP Ingestion Endpoint | `POST /events` accepts `event_id`, numeric `timestamp`, and arbitrary `payload`, mapping accurately to internal `Event` and engine lifecycle. | **VERIFIED** | `tests/test_network_p4.py::test_valid_event_ingestion` |
| **AC-NET-02** | HTTP Acknowledgement Status Codes | Returns 202 Accepted for claimed events, 200 Duplicate for duplicates, 429 Buffer Full for backpressure, 410 Too Old for window expired, 422 for validation/future skew, 400 for malformed/non-finite, 503 for KV outage. | **VERIFIED** | `tests/test_network_p4.py` (24/24 passing) |
| **AC-NET-03** | HTTP Concurrency & Retries | Concurrent requests (same-ID and distinct-ID) preserve deduplication; producer retries after dropped responses recognize existing claims without duplicate sink effects. | **VERIFIED** | `tests/test_network_p4.py::test_concurrent_same_id_requests`, `test_simulated_lost_ack_retry` |
| **AC-NET-04** | Operational Health & Readiness Probes | `GET /health` reports process liveness; `GET /ready` actively probes KV connectivity (returning 503 if unreachable); `GET /stats` returns engine and store telemetry. | **VERIFIED** | `tests/test_network_p4.py::test_health_endpoint`, `test_ready_endpoint_healthy`, `test_ready_endpoint_unhealthy_on_backing_store_down` |
| **AC-NET-05** | Request Protection & Bounds | Request payloads exceeding 1MB return HTTP 413; internal server exceptions return HTTP 500 without leaking credentials or stack traces. | **VERIFIED** | `tests/test_network_p4.py::test_oversized_payload_rejected`, `test_no_stack_trace_leakage_on_internal_exception` |
