# Exactly-Once Event De-duplication Engine

Stream-processing **de-duplication proxy** with **sliding timestamp windows**, an **out-of-order reorder buffer**,
a pluggable **cloud key-value store** (in-memory for tests, Redis 7 for real), **fault-injection tests** and **benchmarks**.
Python 3.11+ / asyncio. The core has zero dependencies (`redis` and `matplotlib` are optional).

## Quick start (Windows PowerShell / VS Code terminal)

```powershell
python -m venv .venv ; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

python -m pytest                       # 158 tests: correctness + faults + proxy + api + window semantics + P3A + P3B + P3C + P3D + P4 + P5
python demos/demo_inprocess.py         # narrated demo (dups, out-of-order, crash, recovery)
python bench/benchmark.py              # benchmarks -> bench/results/*.csv + benchmarks.png
python bench/benchmark_distributed.py  # multi-instance distributed partitioning benchmark
```

Run the proxy as a real network service:

```powershell
python -m dedup_engine.proxy --sink file:out.jsonl            # terminal 1 (memory KV)
python demos\demo_client.py                                    # terminal 2

docker compose up -d                                           # optional: Redis 7 as KV store
python -m dedup_engine.proxy --kv redis://localhost:6379/0 --sink file:out.jsonl
```

VS Code: use *Run and Debug* (launch configs for demo / proxy / benchmarks) or *Terminal > Run Task* (tests, benchmarks).


## Deployment on Render (free demo)

This repository includes a `Dockerfile` and `render.yaml` for a free, single-instance Render web service.
The service enables the partition manager locally and can connect to a Redis-compatible URL using
`ENGINE_REDIS_URL`. If Redis is omitted, it falls back to process-local `MemoryKV`.

**Free-tier limitations:** this is not the full multi-instance failover deployment. Render free web services
cannot receive private-network traffic, so owner-to-owner HTTP forwarding is not available. Free Key Value
instances are temporary and non-persistent, so their state may be lost on restart. The configured demo uses
`MemorySink`, which is also process-local and non-durable. Do not claim crash-safe exactly-once effects for
this hosted free demo. Use the local Docker Compose distributed setup and a durable shared sink for the
project's stronger distributed verification scenario.

### Deploy

1. Push this repository to GitHub.
2. In Render, choose **New > Blueprint** and select this repository.
3. When prompted, create a free Key Value instance or provide a Redis-compatible connection URL for
   `ENGINE_REDIS_URL`. Use the **private/internal** connection URL when the web service and Key Value
   instance are in the same Render region and the selected plan supports it; otherwise use the URL Render
   explicitly provides for the service's access path. Never commit credentials.
4. Deploy the Blueprint. Wait for `/health` to return HTTP 200.
5. Test `POST /events`, then repeat the same `event_id` and verify the duplicate response.

For a local multi-instance demonstration, use Docker Compose with Redis persistence and multiple app
containers. The Render free Blueprint intentionally provisions only one API instance.

## Render free-tier deployment (single-instance demo)

This repository includes a Dockerfile and Render Blueprint. The free-tier configuration runs one API instance with partition-aware local processing. It is not a distributed multi-instance failover deployment: Render free web services cannot receive private-network traffic, and free Key Value storage is non-persistent. The default demo sink is process-local. Do not claim crash-safe exactly-once effects for this hosted configuration.

1. Push the repository to GitHub.
2. In Render, select **New > Blueprint** and connect this repository.
3. Supply `ENGINE_REDIS_URL` using the Redis-compatible URL provided by your Key Value service. Do not commit this URL.
4. Deploy and wait for `/health` to return HTTP 200.
5. POST an event to `/events`, then retry the same `event_id` and verify a duplicate response.

For the full distributed behavior and failover tests, use the local multi-instance Docker setup with persistent Redis and a shared durable, idempotent sink.

## Architecture

```
 producers (at-least-once, may resend / reorder)
        |  NDJSON over TCP  {"id","ts","data"}
        v
+--------------------------- Dedup Proxy (proxy.py) ----------------------------+
|  1 late check  ts < watermark - window ?  --------------------> DLQ (too_old)  |
|  2 atomic claim  KV put_if_absent("dedup:<id>", PENDING+payload)  <- WAL       |
|        exists?  -> duplicate (or adopt orphan PENDING record)                  |
|  3 reorder buffer (min-heap on event time)                                     |
|        release when ts <= watermark = max_ts - allowed_lateness                |
|  4 deliver to sink (idempotent on event.id, retried)                           |
|  5 commit  KV set(COMMITTED, score=ts)  -> record becomes evictable            |
|  6 maintenance: checkpoint max_ts, evict records with ts < watermark - window  |
+---------------------------------------------------------------------------------+
        |                                   ^
        v                                   |  recover(): re-load PENDING records
   Sink (idempotent)                  KV store (Redis / DynamoDB-like / MemoryKV)
```

### Sliding timestamp window

* `watermark = max_event_ts_seen - allowed_lateness` (advances only on claimed events).
* Dedup state for an event is kept until `event.ts < watermark - window`, then evicted (bounded memory).
* Event older than `watermark - window` -> **TOO_OLD -> dead-letter queue** (we can no longer prove it is new).
* Event between `watermark - window` and `watermark` -> **late**: still deduplicated, emitted immediately (out of order).
* Event within lateness -> buffered and released in **event-time order**.

### How "exactly-once" is achieved

True exactly-once *delivery* is impossible over an unreliable network; the engine gives exactly-once **effect**:
at-least-once delivery + atomic de-dup claim + idempotent sink keyed by `event.id`.

| Failure | Mechanism |
|---|---|
| Duplicate / concurrent duplicate sent by producers | KV `put_if_absent` is the single arbiter; exactly one caller wins |
| KV request lost | retry with exponential backoff |
| KV write applied but **ack lost** | claim record carries an ownership **token**; a retry that finds its own token knows it won (not a false "duplicate") |
| Ack lost on the *last* retry (orphan PENDING record) | resend finds PENDING record nobody owns -> **adopted** into the buffer |
| Process crash after claim / before delivery | PENDING record holds the full payload (write-ahead) -> `recover()` re-queues it |
| Crash after sink applied, before commit | recovery redelivers; **idempotent sink** makes it a no-op |
| Sink down / ack lost | retries; events stay buffered + PENDING; flushed later |
| KV down | ingest returns `error`; nothing delivered, nothing lost; client resends |

`ACCEPTED` means *durably claimed* - delivery is then guaranteed even across crashes.

### KV store recommendation

**Redis 7 / 8** (AOF on): atomic Lua scripts for combined record & sorted-set index mutations, sub-ms latency. In AWS use **DynamoDB** (conditional `PutItem` + TTL), same `KVStore` interface (6 methods).

## Known limitations (be upfront in the viva)

* Guarantee holds **within the window**; a duplicate arriving after eviction is DLQ'd (`too_old`) rather than silently re-processed.
* Dedup is by `event.id`; a retry that re-stamps a different `ts` is still caught only while the record is retained.
* One engine instance per partition (single writer for the reorder buffer). Scale out by hashing `id` -> partition.
* Future timestamps are bounded via `max_future_skew` and reference `time_source` to guard watermark advancement.
* The sink must be idempotent (see `NaiveSink` test proving duplicate side effects otherwise).
* End-to-end exactly-once guarantees apply to idempotent sinks; non-idempotent arbitrary side-effect sinks require two-phase commit or transactional outboxes.
* `RedisKV` atomic mutations and dedicated pending index are verified via dedicated live Redis integration and recovery tests (`tests/test_redis_integration.py`, `tests/test_recovery_p3c.py`).
* Bounded incremental recovery operates with configurable batch sizes (`recovery_batch_size`) respecting capacity reservations and `max_buffer_size`.
* Downstream sink contracts explicitly distinguish idempotent logical effects from physical delivery. `FileSink` provides durable JSON-lines persistence with `fsync`, torn-tail crash recovery, and advisory locking. `StdoutSink` is explicitly documented as non-transactional with optional process-local deduplication.

## Sink Guarantee Matrix

| Sink | Idempotent? | Durable? | Restart-Safe? | Multi-Process Safe? | Exactly-Once Logical Effect? |
|---|---|---|---|---|---|
| **MemorySink** | Yes (keyed on `event.id`) | No (in-memory) | No (lost on exit) | No | Yes (in-process only) |
| **FileSink** (`lock_file=True, fsync=True`) | Yes (disk scan + cache) | **Durable** (`os.fsync`) | **Yes** (recovers seen IDs, truncates torn tail, quarantines corrupted files) | **Yes** (exclusive lock + shared disk scan) | **Yes (durable across crashes and across processes)** |
| **FileSink** (`lock_file=False, fsync=True`) | Yes (single-process) | **Durable** (`os.fsync`) | **Yes** (recovers seen IDs, truncates torn tail) | **No** (single process only) | **Yes (single-process durable)** |
| **FileSink** (`fsync=False`) | Yes (keyed on `event.id`) | Partial (OS buffer) | **Yes** (recovers seen IDs, truncates torn tail) | Depends on `lock_file` | Yes (crash-safe, vulnerable to sudden power loss) |
| **NaiveSink** | **No** (appends every try) | No (in-memory) | No | No | **No (creates duplicate side-effects on retry)** |
| **StdoutSink** (`dedup=False`) | **No** (prints every try) | No (ephemeral) | No | No | **No (duplicates printed on retry)** |
| **StdoutSink** (`dedup=True`) | Partial (process-local) | No (ephemeral) | No | No | Best-effort in-process only |

*Note on Power-Loss Durability*: When `fsync=True`, `os.fsync(fd)` guarantees that file data blocks are committed to persistent media before delivery is acknowledged. During torn-tail recovery (`os.replace`), `_sync_parent_directory` issues directory metadata synchronization where supported (POSIX). On Windows, NTFS metadata journaling ensures atomic directory updates.

## Project layout

```
dedup_engine/  models.py kv.py sink.py engine.py faults.py proxy.py partition.py
tests/         test_correctness.py  test_fault_injection.py  test_proxy.py  test_api.py  test_window_semantics.py  test_lifecycle_p3a.py  test_redis_integration.py  test_recovery_p3c.py  test_sinks_p3d.py  test_network_p4.py  test_distributed_p5.py
bench/         benchmark.py  benchmark_recovery.py  benchmark_distributed.py  (results/ CSV + JSON + chart)
demos/         demo_inprocess.py  demo_client.py
api.py / web/  FastAPI ingestion service, operational probes & monitoring UI
docs/          IMPLEMENTATION_AUDIT.md  ACCEPTANCE_CRITERIA.md  PHASE_2_IMPLEMENTATION_REPORT.md  PHASE_3B_IMPLEMENTATION_REPORT.md  PHASE_3C_IMPLEMENTATION_REPORT.md  PHASE_3D_IMPLEMENTATION_REPORT.md  PHASE_3D_REMEDIATION_REPORT.md  PHASE_4_IMPLEMENTATION_REPORT.md  PHASE_5_IMPLEMENTATION_REPORT.md
EXPERIMENT_REPORT.md   aim / theory / procedure / observations / conclusion
```

## Test matrix

| Group | What it proves |
|---|---|
| correctness (18) | dedup, 200 concurrent duplicates -> 1 winner, event-time ordering, watermark hold, late-in-window, TOO_OLD -> DLQ, bounded state, pending never evicted, 10 randomized streams |
| fault injection (35) | request-loss & ack-loss on KV, sink failures, KV/sink outages, crash at all 5 points + recovery, orphan adoption, naive-sink counter-example, **20 seeded chaos runs** (random crashes + flaky KV + flaky sink + producer replay) |
| lifecycle P3A (10) | Phase 3A buffer overflow lifecycle, capacity reservations under concurrency, crash recovery without resurrecting rejected events, lost-ACK ownership preservation |
| proxy (2) | NDJSON protocol end-to-end, 50 concurrent clients sending the same event |
| api (4) | Dashboard UI endpoints, demo trigger, benchmark endpoint, and config updates |
| network P4 (24) | Phase 4 HTTP ingestion protocol (`POST /events`), status code mappings (202/200/429/410/422/400/503/413), deterministic producer retry & lost ACK resilience, concurrent same-ID and distinct-ID submissions, buffer backpressure & retry, operational probes (`/health`, `/ready`, `/stats`), security bounds & payload limits |
| distributed P5 (7) | Phase 5 deterministic partition hashing (CRC32), Redis lease acquisition/renewal, fencing token monotonicity & stale owner rejection, unannounced crash failover, cross-instance concurrent same-ID deduplication, partition-isolated backpressure, distributed operational telemetry |
| window semantics (12) | Phase 2 sliding window boundaries, clock-skew guard, non-finite timestamps, buffer capacity limit, batch eviction, watermark invariance |
| redis integration (12) | Phase 3B atomic record & sorted-set mutations via Redis Lua, concurrent claims, score updates, pending/committed transitions, cutoff boundaries, lost ACKs, Phase 3A lifecycle compatibility |
| recovery P3C (16) | Phase 3C dedicated pending index (`idx:dedup:pending`), bounded incremental batch recovery, buffer capacity adherence, stale/malformed cleanup, idempotent resume, live Redis integration |
| sink reliability P3D (18) | Phase 3D sink idempotency, FileSink fsync durability, torn-tail recovery, fail-closed quarantine for internal corruption, independent multi-process deduplication (2 & 4 OS processes), crash redelivery, StdoutSink bounds |
