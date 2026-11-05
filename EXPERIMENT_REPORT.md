# Experiment Report - Exactly-Once Event De-duplication Engine

## 1. Aim
Design and implement a stream-processing de-duplication proxy that gives exactly-once *effects* using sliding
timestamp windows, handles out-of-order events, stores state in a cloud-style KV store, and prove correctness with
fault-injection tests and benchmarks.

## 2. Theory
* Exactly-once delivery is impossible over lossy links (Two Generals); practical systems combine **at-least-once delivery + idempotent processing**.
* **Event time vs processing time**: ordering uses the event timestamp; a **watermark** (`max_ts - allowed_lateness`) says when no earlier event is expected.
* **Sliding window**: dedup keys are retained for `window` seconds of event time, bounding memory; older events are dead-lettered.
* **Write-ahead claim**: the atomic `put_if_absent` that also stores the payload is both the dedup decision and the recovery log.
* **Ownership token**: distinguishes "my write whose ack was lost" from "someone else's earlier write".

## 3. Procedure
1. Implement `KVStore` (MemoryKV, RedisKV), `Sink` (idempotent Memory/File, NaiveSink), `DedupEngine`, TCP `DedupProxy`.
2. Implement fault injectors: `FaultyKV` / `FaultySink` (request loss, ack loss, outage) and `CrashInjector` (5 crash points).
3. Run `python -m pytest` (55 tests) and `python bench/benchmark.py`.

## 4. Fault-injection results
All 55 tests pass, including crash at every pipeline point followed by recovery and 20 seeded chaos runs
(random crashes + 5%/5% KV request/ack loss + 5%/5% sink request/ack loss + producer replay) that assert
**zero duplicate side-effects and zero lost events**.
One real bug was found by the benchmarks and fixed: when a claim's ack was lost on its *last* retry the record stayed
PENDING but was never buffered, so the client's resend was dropped as a duplicate (event stranded). Fix: orphan
adoption (regression test `test_orphaned_pending_claim_is_adopted_on_client_resend`).
`NaiveSink` test demonstrates that without an idempotent sink a crash between delivery and commit duplicates the effect.

## 5. Benchmarks (in-process MemoryKV with simulated round-trip; sandbox machine, 4000 events unless stated)
**A. Throughput / latency vs KV round-trip and concurrency**

| KV RTT (ms) | concurrency | events/s | p50 ms | p95 ms | p99 ms |
|---|---|---|---|---|---|
| 0.0 | 1 | 18907 | 0.02 | 0.06 | 0.36 |
| 0.0 | 16 | 19787 | 0.61 | 1.25 | 1.63 |
| 0.0 | 128 | 18082 | 7.0 | 10.9 | 27.84 |
| 0.5 | 1 | 628 | 1.21 | 2.4 | 2.53 |
| 0.5 | 16 | 6604 | 1.68 | 3.88 | 5.43 |
| 0.5 | 128 | 10967 | 10.1 | 22.6 | 24.84 |
| 2.0 | 1 | 342 | 2.23 | 4.46 | 4.59 |
| 2.0 | 16 | 3641 | 2.99 | 6.49 | 9.29 |
| 2.0 | 128 | 8812 | 11.53 | 25.53 | 28.73 |

**B. Duplicate ratio** (RTT 0.5 ms, concurrency 64)

| dup ratio | events/s | p95 ms | duplicates detected |
|---|---|---|---|
| 0 | 6997 | 13.53 | 0 |
| 0.25 | 9967 | 10.98 | 1000 |
| 0.5 | 12545 | 11.31 | 2000 |
| 0.9 | 18646 | 8.05 | 3601 |

**C. Sliding-window size vs state** (event spacing 50 ms)

| window (s) | peak KV keys | events/s |
|---|---|---|
| 5 | 132 | 14018 |
| 30 | 655 | 16658 |
| 120 | 2544 | 11122 |
| 600 | 5601 | 16800 |

**D. Throughput under injected KV faults** (RTT 0.5 ms, concurrency 64)

| request-loss | ack-loss | events/s | p99 ms | KV retries | exactly-once |
|---|---|---|---|---|---|
| 0 | 0 | 10294 | 13.71 | 0 | True |
| 0.01 | 0.01 | 8425 | 20.56 | 183 | True |
| 0.05 | 0.05 | 6541 | 26.54 | 957 | True |
| 0.1 | 0.1 | 4891 | 35.91 | 2242 | True |
| 0.2 | 0.2 | 2394 | 99.13 | 6548 | True |

## 6. Observations
* With near-zero KV latency the engine is CPU-bound (~19-20k events/s in pure Python); with a realistic cloud RTT, throughput is latency-bound and scales with concurrency (0.5 ms RTT: 628 -> ~11k events/s from 1 -> 128 in flight).
* Only 2 KV ops per accepted event (claim + commit); duplicates cost 2 (claim attempt + read) but no sink work, so throughput rises with duplicate ratio.
* KV state is proportional to the window (peak keys grow ~linearly with window), confirming eviction bounds memory.
* Faults cost throughput (retries, backoff) but never correctness: every scenario kept exactly-once effects.

## 7. Conclusion
The engine delivers exactly-once effects inside a configurable sliding window, survives KV/sink faults and process
crashes, keeps memory bounded, and its limits (window bound, idempotent sink, single writer per partition) are explicit.
