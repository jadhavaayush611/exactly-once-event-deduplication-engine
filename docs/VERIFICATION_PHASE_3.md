# Phase 3 Independent Verification Report: Exactly-Once Event Deduplication Engine

**Verification Date**: October 30, 2026  
**Auditor**: Independent Verification Agent  
**Target Repository**: `D:\Exactly-Once-Event-Deduplication-Engine`  
**Execution Environment**:
- **Operating System**: Windows 11 (win32)
- **Python**: 3.14.6
- **Test Framework**: pytest 9.1.1, pluggy 1.6.0, anyio 4.15.1
- **Live Redis Instance**: Redis 7.4.11 (`redis:7-alpine`, container `dedup-test-redis`, AOF enabled `--appendonly yes`, port 6380)
- **Prior Baseline**: 71 tests (Phase 2 baseline)

---

## 1. Executive Summary & Final Verdict

| Verification Area | Status | Key Findings & Evidence |
|---|---|---|
| **1. DEFECT-P2-01 Verification** | **FAIL** | **UNRESOLVED DEFECT CONFIRMED**. `DedupEngine.ingest()` still executes KV write-ahead claim before verifying reorder buffer capacity. Rejected `BUFFER_FULL` events leave stranded `pending` records in KV, causing subsequent retries to return `DUPLICATE` (via orphan adoption) and causing process restarts (`recover()`) to **resurrect and deliver** events previously rejected downstream. |
| **2. Redis Atomicity & Key/Index Consistency** | **FAIL** | `RedisKV.set()` executes non-atomic sequential writes: `SET` followed by un-pipelined `ZADD idx:dedup`. Connection drops or process crashes between the two leave committed records orphaned outside the sorted set index, causing permanent memory leaks that can never be evicted. Concurrent claims via `SET NX` pass, but multi-key atomicity is not implemented. |
| **3. Recovery Scalability & Correctness** | **FAIL** | `RedisKV.scan()` suffers from an $O(N)$ sequential N+1 query antipattern (`scan_iter` followed by individual `GET` per key). Recovery over 2,550 keys required 1.188s (~0.47ms/key sequential round trips). Furthermore, `DedupEngine.recover()` executes unbounded loading into heap memory, completely ignoring `max_buffer_size`, and concurrent unpartitioned recovery duplicates sink effects. |
| **4. Downstream Sink Testing & Guarantees** | **FAIL** | **Zero automated tests exist in `tests/` for `FileSink` or `StdoutSink`**. `StdoutSink` provides no deduplication, idempotency, or crash safety (merely prints). `FileSink` reloads IDs on single-process restart but defaults to `fsync=False` (vulnerable to OS crash data loss) and lacks multi-process concurrency protection. |
| **5. Regression Test Suite & Benchmarks** | **PASS** | `pytest -v` passed 71/71 tests in 41.64s (zero tests removed, skipped, or weakened). `bench/benchmark.py --quick` passed all 4 test suites (up to 25,450 eps in-memory, 15,661 eps cloud KV RTT). |
| **6. Documentation & Guarantee Alignment** | **FAIL** | Documentation in `README.md` and `ACCEPTANCE_CRITERIA.md` fails to disclose known non-atomic split writes in Redis, $N+1$ recovery bottlenecks, lack of buffer bounds during recovery, and false duplicate returns on `BUFFER_FULL` retries. |

---

### Final Verdict

```
================================================================================
FINAL VERDICT: DO NOT PROCEED
================================================================================
```

**Reasoning**: Phase 3 cannot pass because core reliability and durability invariants fail reproduction:
1. **DEFECT-P2-01 remains active in the engine**: Events rejected with `BUFFER_FULL` leave stranded claims in the KV store, causing client retries to return `Status.DUPLICATE` instead of `Status.ACCEPTED`, and causing crash recovery (`recover()`) to resurrect and deliver events that the producer was told were rejected.
2. **Redis backend lacks atomic record/index mutations**: `RedisKV.set()` relies on non-atomic split commands (`SET` followed by `ZADD`), resulting in permanent state leakage if interrupted.
3. **Recovery does not scale and lacks bounded memory safety**: Sequential N+1 reads in `RedisKV.scan()` create high latency bottlenecks, and `DedupEngine.recover()` loads unbounded records into memory ignoring `max_buffer_size`.
4. **Sink guarantees are untested**: No automated test coverage exists in `tests/` for `FileSink` or `StdoutSink`, and `StdoutSink` is demonstrably non-idempotent.

---

## 2. Detailed Verification by Section

### Section 1: Verification of DEFECT-P2-01

#### 1.1 Inspection of Implementation
In [dedup_engine/engine.py](file:///D:/Exactly-Once-Event-Deduplication-Engine/dedup_engine/engine.py#L131-L162), `DedupEngine.ingest()` executes the following sequence:
```python
131: token = uuid.uuid4().hex
132: rec = json.dumps({"state": "pending", "ts": event.ts, "token": token, "event": event.to_dict()})
133: self._claiming[event.id] = self._claiming.get(event.id, 0) + 1
134: try:
135:     owned, existing = await self._claim(PREFIX + event.id, rec, token)
136: finally:
137:     ...
...
157: if self.max_buffer_size is not None and len(self._heap) >= self.max_buffer_size:
158:     await self._flush()
159:     if len(self._heap) >= self.max_buffer_size:
160:         m.buffer_overflow += 1
161:         return Status.BUFFER_FULL
```
The write-ahead claim (`self._claim`) occurs at line 135 **before** checking if the reorder buffer has room at line 157. When the buffer is full and cannot drain (e.g. events are held behind watermark lateness), line 161 returns `Status.BUFFER_FULL` without removing or rolling back the `pending` record in KV.

#### 1.2 Reproduction Sequence Execution
We executed the 7-step reproduction script against the live codebase:
1. Configured engine with `max_buffer_size=2`, `lateness=10.0`, `window=100.0`.
2. Ingested `e1` and `e2` (buffer capacity filled to 2).
3. Submitted `e3`: returned `Status.BUFFER_FULL`.
4. Inspected KV store: `dedup:e3` exists as `{"state": "pending", "token": "...", "event": {"id": "e3", ...}}`.
5. Drained buffer to free capacity (`drain()` emptied heap; `e1` and `e2` delivered to sink).
6. Retried rejected event `e3`: returned `Status.DUPLICATE` (via orphan adoption logic in line 155) instead of `Status.ACCEPTED`.
7. Tested crash and recovery without retry:
   - Ingested `e1` (accepted), `e2` (accepted), `e3` (rejected with `BUFFER_FULL`).
   - Simulated process restart (`DedupEngine(kv, new_sink)`).
   - Executed `recover()`: returned `recovered_count=3`!
   - Executed `drain()`: `sink.effects` delivered `['e1', 'e2', 'e3']`.

**Defect Reproduction Output**:
```
=== TEST 1.1: DEFECT-P2-01 Sequence Verification ===
Step 1 - Fill buffer: e1=Status.ACCEPTED, e2=Status.ACCEPTED, heap_len=2
Step 2 - Submit e3: status=Status.BUFFER_FULL, heap_len=2
Step 2 - KV record for e3 after BUFFER_FULL: {"state": "pending", "ts": 10.0, "token": "439f9e4799a049858f6f9dbd017842e4", "event": {"id": "e3", "ts": 10.0, "data": null}}
Step 3 - Free capacity via drain(): remaining_heap=0, sink_effects=['e1', 'e2']
Step 4 - Retry e3: status=Status.DUPLICATE
Step 5 - Downstream sink effects: ['e1', 'e2', 'e3']

=== TEST 1.2: Crash & Recovery after BUFFER_FULL ===
Step 6 - Restart & recover(): recovered_count=3
Step 7 - Sink effects after recovery & drain: ['e1', 'e2', 'e3']
CRITICAL DEFECT DETECTED: e3 was rejected with BUFFER_FULL but RESURRECTED by recover() and delivered downstream!
```
- **Result**: **FAIL**. The contract is violated: rejected events are neither reportable as `ACCEPTED` upon legitimate client retry nor safe from resurrection upon crash recovery.

---

### Section 2: Verification of Redis Atomicity

#### 2.1 Test Environment & Isolation
- Tested against a live isolated Redis container:
  - Image: `redis:7-alpine`
  - Container ID / Name: `776572e64137 / dedup-test-redis`
  - Port: `127.0.0.1:6380 -> 6379/tcp`
  - Configuration: `redis-server --appendonly yes`
  - Version: Redis 7.4.11 standalone

#### 2.2 Inspection of Implementation
In [dedup_engine/kv.py](file:///D:/Exactly-Once-Event-Deduplication-Engine/dedup_engine/kv.py#L110-L146):
```python
110: async def put_if_absent(self, key, value, score=None) -> bool:
111:     ok = await self._do(self._r.set(key, value, nx=True))
112:     if ok and score is not None:
113:         await self._do(self._r.zadd(self.INDEX, {key: score}))
114:     return bool(ok)
...
119: async def set(self, key, value, score=None) -> None:
120:     await self._do(self._r.set(key, value))
121:     if score is None:
122:         await self._do(self._r.zrem(self.INDEX, key))
123:     else:
124:         await self._do(self._r.zadd(self.INDEX, {key: score}))
```
1. **Split-write non-atomicity**: Lines 120 and 124 execute two independent round trips without `MULTI/EXEC` pipeline or a Lua script. If a network interruption, timeout, or crash occurs between line 120 and line 124, the key is written to Redis but never indexed in `idx:dedup`.
2. **Permanent state leak on failure**: Any key missing from `idx:dedup` will never be found by `evict_before(score)` because eviction solely queries `zrangebyscore(self.INDEX, ...)`. We reproduced this in test 2.2:
   ```
   Normal set: in_kv exists=True, score in idx=100.0
   Eviction sweep cutoff 200.0: evicted count=1
   Orphaned key after eviction: still exists=True
   DEMONSTRATED: Split key/index mutation leaves committed records permanently leaked in Redis if not atomic!
   ```

#### 2.3 Other Redis Behaviors Verified
- **Concurrent claims for same ID**: 20 coroutines racing on `put_if_absent("dedup:concurrent-event-1")` -> exactly 1 winner, 19 losers. (PASS)
- **Lost ACK / Ambiguous outcome**: Client retrying with same token recognizes its ownership and returns `owned=True`. (PASS)
- **Window boundary eviction**: `evict_before(100.0)` dropped score 99.9, preserved score 100.0 and 100.1. (PASS)
- **Pending record protection**: Record with `score=None` was not added to `idx:dedup` and survived `evict_before(1000.0)`. (PASS)
- **Recovery after process restart**: Recovered pending record from Redis and delivered downstream. (PASS)
- **Result**: **FAIL** due to lack of demonstrated atomic design for record/index mutations (Criterion 2).

---

### Section 3: Verification of Recovery Scalability and Correctness

#### 3.1 Inspection of Recovery Code
In [dedup_engine/kv.py](file:///D:/Exactly-Once-Event-Deduplication-Engine/dedup_engine/kv.py#L126-L132) and [dedup_engine/engine.py](file:///D:/Exactly-Once-Event-Deduplication-Engine/dedup_engine/engine.py#L263-L283):
```python
# RedisKV.scan
126: async def scan(self, prefix):
127:     out = []
128:     async for k in self._r.scan_iter(match=prefix + "*", count=1000):
129:         v = await self._do(self._r.get(k))
130:         if v is not None:
131:             out.append((k, v))
132:     return out
```
Line 129 issues a sequential individual `GET` request for every single key returned by `scan_iter`. This is a classic $N+1$ query antipattern.

Furthermore, in `DedupEngine.recover()`:
- `items = await self._retry(lambda: self.kv.scan(PREFIX), ...)` loads every single item in the entire keyspace matching `dedup:*` into memory at once without pagination or chunking.
- Python filters in-memory: `if rec["state"] != "pending": continue`. There is no dedicated `idx:pending` index.
- For all pending items, `self._buffer(ev)` pushes onto `self._heap` without checking `max_buffer_size`, completely ignoring buffer capacity limits.

#### 3.2 Live Benchmark over Meaningful Datasets
We tested recovery over varying keyspace sizes against live Redis 7.4.11:

| Committed Keys in Redis | Pending Keys | Total Keys Scanned | Recovery Time (s) | Effective Keys / sec | Avg Latency / Key |
|---|---|---|---|---|---|
| 100 | 50 | 150 | 0.194s | 771.4 keys/s | 1.30 ms |
| 500 | 50 | 550 | 0.274s | 2,005.3 keys/s | 0.50 ms |
| 1,000 | 50 | 1,050 | 0.456s | 2,301.9 keys/s | 0.43 ms |
| 2,500 | 50 | 2,550 | 1.188s | 2,146.0 keys/s | 0.47 ms |

- **Sequential N+1 query overhead**: In a moderate database of 2,550 keys, recovery spent 1.188 seconds executing 2,550 sequential GET round trips over loopback. In a production cloud setting with 1ms network RTT, 50,000 keys would require ~50+ seconds just to complete `recover()`.
- **Buffer Bounding**: Engine configured with `max_buffer_size=10` recovered all 50 pending records into memory with `m.buffer_overflow == 0`. Bounded batch behavior does not exist in recovery.
- **Concurrent Recovery**: Two engine instances running concurrent recovery on the same Redis store without partition locking both scanned and recovered all 20 records, delivering 20 duplicate effects across sinks.
- **Duplicate Downstream Effects with Naive Sink**: When using `NaiveSink` (non-idempotent), crash recovery caused `p-naive-0` to be delivered twice (2 effects).
- **Result**: **FAIL** on recovery scalability and bounded memory safety.

---

### Section 4: Verification of Sink Tests

#### 4.1 Test Suite Inventory
An exhaustive search of `tests/` confirmed:
- `tests/test_api.py` (4 tests)
- `tests/test_correctness.py` (18 tests)
- `tests/test_fault_injection.py` (35 tests)
- `tests/test_proxy.py` (2 tests)
- `tests/test_window_semantics.py` (12 tests)
- **Total tests**: 71 tests.
- **`FileSink` tests in `tests/`**: **0**
- **`StdoutSink` tests in `tests/`**: **0**

#### 4.2 Behavior & Guarantee Audit
In [dedup_engine/sink.py](file:///D:/Exactly-Once-Event-Deduplication-Engine/dedup_engine/sink.py#L45-L72):
1. **`StdoutSink`**:
   ```python
   class StdoutSink(Sink):
       async def deliver(self, event: Event) -> None:
           print("DELIVER", event.to_json(), flush=True)
   ```
   `StdoutSink` has no state, no idempotency checks, and no crash recovery. Every delivery attempt prints to stdout. Claiming `StdoutSink` is idempotent or crash-safe is invalid.
2. **`FileSink`**:
   - Reloads known IDs from JSONL on single-process restart: verified working for single-writer process.
   - Torn line tolerance: verified (`try ... json.loads(line) ... except (ValueError, KeyError): pass`).
   - Crash safety limitation: defaults to `fsync=False`. Standard Python `.flush()` writes to OS page cache, not non-volatile storage. An OS crash or power cut can lose recent deliveries despite the caller receiving success.
   - Concurrency limitation: lacks file locking (`fcntl` / `LockFileEx`), rendering it unsafe for multi-worker deployments.
- **Result**: **FAIL** due to absence of regression test coverage in the suite and inaccurate or unsubstantiated idempotency claims.

---

### Section 5: Regression Checks & Baseline Comparison

#### 5.1 Pytest Execution Output
**Command**: `python -m pytest -v`  
**Execution Time**: 41.64s  
**Exit Code**: `0`

```
============================= test session starts =============================
platform win32 -- Python 3.14.6, pytest-9.1.1, pluggy-1.6.0
rootdir: D:\Exactly-Once-Event-Deduplication-Engine
configfile: pytest.ini
testpaths: tests
plugins: anyio-4.15.1
collected 71 items

tests\test_api.py ....                                                   [  5%]
tests\test_correctness.py ..................                             [ 30%]
tests\test_fault_injection.py ...................................        [ 80%]
tests\test_proxy.py ..                                                   [ 83%]
tests\test_window_semantics.py ............                              [100%]

============================== warnings summary ===============================
fastapi\testclient.py:1: StarletteDeprecationWarning: Using `httpx` with `starlette.testclient` is deprecated; install `httpx2` instead.
======================= 71 passed, 1 warning in 41.64s ========================
```

#### 5.2 Benchmark Execution Output
**Command**: `python bench/benchmark.py --quick`  
**Exit Code**: `0`

```
=== A) throughput & latency vs KV RTT and concurrency ===
  kv_latency_ms |     concurrency |  throughput_eps |          p50_ms |          p95_ms |          p99_ms | kv_ops_per_event |    exactly_once
            0.0 |               1 |           21174 |            0.02 |            0.05 |            0.07 |             2.0 |            True
            0.0 |              16 |           23044 |             0.5 |            0.71 |            2.03 |             2.0 |            True
            0.0 |             128 |           25450 |            4.73 |            6.09 |            6.55 |             2.0 |            True
            0.5 |               1 |              63 |           15.54 |           31.55 |           32.22 |             2.0 |            True
            0.5 |              16 |            1535 |           10.97 |           29.59 |            30.6 |             2.0 |            True
            0.5 |             128 |           15661 |            6.09 |            8.64 |            8.94 |             2.0 |            True
            2.0 |               1 |              52 |           15.84 |           32.06 |            32.8 |             2.0 |            True
            2.0 |              16 |             743 |           15.85 |            31.9 |           42.92 |             2.0 |            True
            2.0 |             128 |            6723 |            5.72 |           29.74 |           31.28 |             2.0 |            True

=== B) duplicate ratio (KV RTT 0.5ms, concurrency 64) ===
      dup_ratio |  throughput_eps |          p95_ms |   dups_detected | kv_ops_per_event |    exactly_once
              0 |           14151 |            10.5 |               0 |             2.0 |            True
           0.25 |           13581 |            7.23 |             250 |             2.0 |            True
            0.5 |           11107 |           31.94 |             500 |             2.0 |            True
            0.9 |           13311 |            6.16 |             901 |             2.0 |            True

=== C) sliding-window size vs KV state (event spacing 50ms) ===
       window_s |    peak_kv_keys |  throughput_eps |    exactly_once
              5 |             130 |           19536 |            True
             30 |             654 |           20286 |            True
            120 |            1401 |           19540 |            True
            600 |            1401 |           17539 |            True

=== D) throughput under injected KV faults (request-loss + ack-loss) ===
      fail_rate |        ack_loss |  throughput_eps |          p99_ms |         retries |    exactly_once
              0 |               0 |           14193 |            6.46 |               0 |            True
           0.01 |            0.01 |            6593 |           15.97 |              47 |            True
           0.05 |            0.05 |            4062 |           12.53 |             224 |            True
            0.1 |             0.1 |            2546 |           38.69 |             535 |            True
            0.2 |             0.2 |             997 |           126.8 |            1654 |            True

All benchmark scenarios kept exactly-once effects.
```

#### 5.3 Baseline Comparison
- **Prior Baseline**: 71 tests.
- **Current Suite**: 71 tests.
- **Difference**: 0 tests added, 0 removed, 0 skipped, 0 weakened.
- **Observation**: The Phase 2 test suite remains completely intact and passing, but **no new Phase 3 tests** (Redis integration tests or sink tests) have been committed to `tests/`.

---

### Section 6: Review of Documentation & Guarantee Boundaries

| Guarantee Dimension | Code Reality | Documentation Status | Alignment Verdict |
|---|---|---|---|
| **Claim Lifecycle & Statuses** | `BUFFER_FULL` leaves write-ahead claim stranded in KV; retries return `DUPLICATE`. | Docs describe `BUFFER_FULL` as simple buffer overflow without noting stranded claims. | **MISALIGNED** |
| **Atomicity Guarantees** | `RedisKV.set()` uses two split commands (`set` then `zadd`). Not atomic. | Docs advertise "atomic de-dup claim" without detailing backend split-write caveats. | **MISALIGNED** |
| **Recovery Behavior** | `recover()` runs $N+1$ queries on Redis and ignores `max_buffer_size`. | Docs claim `recover()` reloads pending records without detailing scalability/memory bounds. | **MISALIGNED** |
| **Redis Index Consistency** | Interrupted `set()` leaks unindexed keys that escape sliding-window eviction. | Docs state sliding window bounds memory, omitting leak risk on un-indexed writes. | **MISALIGNED** |
| **Buffer Overflow Semantics** | Claims occur before buffer check; recovery resurrects rejected events. | Docs state buffer bounds memory; does not document resurrection risk. | **MISALIGNED** |
| **Window Expiration & Replay** | Strict cutoff $t < W - \text{window}$ routes to in-memory DLQ. | Accurately documented in README.md and ACCEPTANCE_CRITERIA.md. | **ALIGNED** |
| **Sink Idempotency** | Required for exactly-once effect. `StdoutSink` is naive; `FileSink` has `fsync=False`. | Docs correctly state sinks must be idempotent, but fails to clarify `StdoutSink` limitations. | **PARTIALLY ALIGNED** |
| **Single-Writer Limitations** | Multi-instance recovery or concurrent writing corrupts state/reorder buffer. | Correctly noted in README: "One engine instance per partition". | **ALIGNED** |
| **DLQ Persistence Limitations** | `self.dlq` is an in-memory list; lost on process crash. | Known limitation; not durably persisted. | **ALIGNED** |

---

## 3. Acceptance Criteria Evaluation Matrix

| ID | Criterion | Measurable Specification | Phase 3 Verdict | Evidence / Reference |
|---|---|---|---|---|
| **AC-CORR-01** | Duplicate Blocking | 1 accepted, $N-1$ duplicates within window | **PASS** | `test_correctness.py::test_basic_dedup` |
| **AC-CORR-02** | Concurrent Duplicate Race | 200 coroutines, exactly 1 wins | **PASS** | `test_correctness.py::test_concurrent_duplicates` |
| **AC-CORR-03** | Lost ACK Idempotent Claim | Claim applied, retry recognizes token | **PASS** | `test_fault_injection.py::test_claim_applied_but_ack_lost` |
| **AC-CORR-04** | Orphaned Claim Adoption | Resend adopts unbuffered pending claim | **PASS** | `test_fault_injection.py::test_orphaned_pending_claim` |
| **AC-CORR-05** | Randomized Ingestion Integrity | 10 seeds, zero duplicate downstream effects | **PASS** | `test_correctness.py::test_random_stream` |
| **AC-CORR-06** | Clock Skew Guard | Future timestamp $> \text{now} + \text{skew} \to$ DLQ | **PASS** | `test_window_semantics.py` |
| **AC-CORR-07** | Timestamp Validity Guard | NaN/Inf timestamps rejected | **PASS** | `test_window_semantics.py` |
| **AC-CORR-08** | Duplicate ID Invariance | Duplicate ID with altered data rejected | **PASS** | `test_window_semantics.py` |
| **AC-WIND-01** | Event-Time Ordering | Released in ascending `event.ts` | **PASS** | `test_correctness.py::test_out_of_order` |
| **AC-WIND-02** | Watermark Hold Condition | Held until watermark passes $t$ | **PASS** | `test_correctness.py` |
| **AC-WIND-03** | Late Arrival Inside Window | Emitted late, deduplicated | **PASS** | `test_correctness.py` |
| **AC-WIND-04** | Expired Event Rejection | $t < W - D \to \text{TOO\_OLD} \to \text{DLQ}$ | **PASS** | `test_correctness.py` |
| **AC-WIND-05** | State Expiry Bounding | Key count scales with window duration | **PASS** | Benchmark Suite C & `test_correctness.py` |
| **AC-WIND-06** | Pending Protection | Records with `score=None` never evicted | **PASS** | Verified against live Redis 7.4.11 |
| **AC-WIND-07** | Exact Expiration Boundary | $t == W - D$ accepted, $< W - D$ rejected | **PASS** | `test_window_semantics.py` |
| **AC-WIND-08** | Watermark Invariance on Rejection | Rejections leave watermark unchanged | **PASS** | `test_window_semantics.py` |
| **AC-WIND-09** | Post-Eviction Replay Protection | Evicted replay returns `TOO_OLD` | **PASS** | `test_window_semantics.py` |
| **AC-MEM-01** | Reorder Buffer Bounding | Buffer overflow bounds memory, safe retry | **FAIL** | **DEFECT-P2-01 active**: claim before buffer check causes stranded claims & resurrection |
| **AC-MEM-02** | Bounded Maintenance Eviction | Eviction respects `max_evict_per_cycle` | **PASS** | `test_window_semantics.py` |
| **AC-DIST-01** | Single-Writer Partitioning | Single deterministic partition worker | **UNVERIFIED** | Phase 5 requirement |
| **AC-DIST-02** | Cross-Instance Dedup Atomicity | Cross-worker claim atomicity via Redis | **FAIL** | Split `set`/`zadd` creates inconsistent state; concurrent recovery duplicates delivery |
| **AC-DIST-03** | Distributed Watermark Alignment | Watermark alignment across partitions | **UNVERIFIED** | Phase 5 requirement |
| **AC-FLT-01** | Transient KV Failure Recovery | Backoff retry succeeds | **PASS** | `test_fault_injection.py` |
| **AC-FLT-02** | Total KV Outage Handling | Unreachable KV raises `KVError` cleanly | **PASS** | `test_fault_injection.py` |
| **AC-FLT-03** | Downstream Sink Outage Buffering | Events stay buffered until sink recovers | **PASS** | `test_fault_injection.py` |
| **AC-FLT-04** | Process Crash Recovery at All Stages | Crash recovery yields zero lost/duplicate | **FAIL** | Crash after `BUFFER_FULL` resurrects rejected events upon recovery |
| **AC-FLT-05** | Recovery Idempotency | Repeated recovery recovers 0 subsequent | **PASS** | Single-instance verified; concurrent multi-instance duplicates |
| **AC-FLT-06** | Continuous Chaos Resilience | 20 chaos scenarios with 0 duplicate effects | **PASS** | `test_fault_injection.py` (20/20 seeds) |
| **AC-SINK-01** | Sink Idempotency Enforcement | Sinks deduplicate on `event.id` | **FAIL** | `StdoutSink` has zero deduplication; no regression tests in suite |
| **AC-SINK-02** | File Sink Persistence & Recovery | Append-only JSONL reloads on start | **FAIL** | Untested in test suite; `fsync=False` by default; no file locking |
| **AC-PERF-01** | In-Memory Engine Throughput | $\ge 15,000$ eps at concurrency 16 | **PASS** | 23,044 eps at c=16, 25,450 eps at c=128 |
| **AC-PERF-02** | Simulated Cloud KV Throughput | $\ge 10,000$ eps at c=128 (0.5ms RTT) | **PASS** | 15,661 eps |
| **AC-PERF-03** | In-Memory Latency Percentile | $\text{p95} \le 1.0\text{ms}$ at c=16 | **PASS** | 0.71 ms |
| **AC-PERF-04** | State Eviction Linearity | Peak keys scale linearly with window | **PASS** | 130 keys (5s) vs 654 keys (30s) |
| **AC-PERF-05** | Real Redis 7 Throughput | $\ge 5,000$ eps against external Redis | **FAIL** | $N+1$ query bottleneck in `scan` bounds recovery throughput to ~2,100 keys/sec |
| **AC-OPS-01** | Automated Test Suite | 100% tests pass | **PASS** | 71/71 passed in 41.64s |
| **AC-OPS-02** | NDJSON Network Proxy | Real TCP client streaming JSON lines | **PASS** | `test_proxy.py` |
| **AC-OPS-03** | Dashboard Health & Telemetry | GET `/api/stats` and `/api/health` HTTP 200 | **PASS** | `test_api.py` |
| **AC-OPS-04** | Redis Container Compose | Redis 7 instance with AOF | **PASS** | Verified on port 6380 (Redis 7.4.11 AOF) |
| **AC-OPS-05** | Zero External Core Dependencies | Core runnable without third-party pkgs | **PASS** | Core uses standard library only |
| **AC-OPS-06** | Graceful Connection Teardown | Asyncio TCP server drains pending buffers | **PASS** | `proxy.py` clean shutdown |

---

## 4. Exit Criteria Audit

| Exit Criterion | Status | Evidence |
|---|---|---|
| **DEFECT-P2-01 is fixed and regression-tested** | **FAIL** | Code still executes `_claim` before buffer capacity check. Reproduced rejected event returning `DUPLICATE` on retry and resurrected on recovery. |
| **Redis record/index mutations have a demonstrated atomic design** | **FAIL** | `RedisKV.set()` uses non-atomic split commands `SET` and `ZADD` without transaction/Lua script. |
| **Real Redis integration tests pass** | **FAIL** | No formal Redis integration tests exist in `tests/`. Live Redis testing uncovered N+1 query bottlenecks and state leakage on split failures. |
| **Recovery does not duplicate logical sink effects in tested failure scenarios** | **FAIL** | Recovery resurrects `BUFFER_FULL` events, duplicates effects across unpartitioned concurrent instances, and duplicates side effects with non-idempotent sinks. |
| **The full existing suite passes without unexplained regressions** | **PASS** | 71/71 tests pass in 41.64s; quick benchmarks pass. |
| **Documentation accurately describes actual guarantees and limitations** | **FAIL** | Unaddressed edge case behaviors, split writes, and recovery limits are not documented. |

---

## 5. Remediation Roadmap Required Before Phase 4

1. **Fix DEFECT-P2-01 in `dedup_engine/engine.py`**:
   - Check reorder buffer headroom *prior* to executing `_claim()`.
   - If buffer is full, attempt `_flush()`; if still full, increment `m.buffer_overflow` and return `Status.BUFFER_FULL` *without* creating any write-ahead KV entry.
2. **Implement Atomic Multi-Key Mutations in `RedisKV`**:
   - Use a Redis Lua script or `MULTI/EXEC` pipeline for `set(key, value, score)` and `put_if_absent(key, value, score)` to ensure key write and `idx:dedup` sorted set indexing occur atomically.
3. **Eliminate N+1 Query in `RedisKV.scan()` and Bound `recover()`**:
   - Maintain a dedicated secondary sorted set `idx:pending` containing only uncommitted pending records.
   - Refactor `recover()` to query `idx:pending` directly in bounded batches (e.g. 256 keys), respecting `max_buffer_size`.
4. **Add Dedicated Test Suites for Sinks and Redis**:
   - Add `tests/test_sinks.py` verifying `FileSink` recovery, torn line recovery, and documenting `StdoutSink` non-idempotency.
   - Add `tests/test_redis_integration.py` running against live Redis with automated teardown.
