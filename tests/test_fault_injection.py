"""Fault injection: KV failures, lost acks, sink failures, process crashes, chaos."""
import pytest
from helpers import ids, make_stream, mk, run

from dedup_engine import (CRASH_POINTS, CrashInjector, DedupEngine, FaultyKV, FaultySink,
                          KVError, MemoryKV, MemorySink, NaiveSink, SimulatedCrash, Status)


def eng(kv, sink, hook=None, **kw):
    kw.setdefault("window", 1e6)
    kw.setdefault("lateness", 0.0)
    return DedupEngine(kv, sink, crash_hook=hook, **kw)


# ------------------------------------------------------------ KV faults
def test_claim_request_lost_is_retried():
    async def go():
        kv, sink = FaultyKV(MemoryKV()), MemorySink()
        kv.once["put_if_absent"] = "before"
        e = eng(kv, sink)
        assert await e.ingest(mk(1)) == Status.ACCEPTED
        assert ids(sink) == ["e1"] and e.m.kv_retries >= 1
    run(go())


def test_claim_applied_but_ack_lost_is_not_mistaken_for_duplicate():
    async def go():
        kv, sink = FaultyKV(MemoryKV()), MemorySink()
        kv.once["put_if_absent"] = "after"          # write landed, client saw an error
        e = eng(kv, sink)
        assert await e.ingest(mk(1)) == Status.ACCEPTED   # ownership token proves it is ours
        assert ids(sink) == ["e1"]
    run(go())


def test_commit_failure_causes_redelivery_but_single_effect():
    async def go():
        kv, sink = FaultyKV(MemoryKV()), MemorySink()
        e = eng(kv, sink, max_retries=0)
        kv.once["set"] = "before"                    # commit fails, no retries left
        await e.ingest(mk(1))
        assert e.m.flush_failures == 1 and ids(sink) == ["e1"]
        await e.drain()                              # redelivers the event
        assert sink.attempts == 2 and ids(sink) == ["e1"]
    run(go())


def test_total_kv_outage_loses_nothing_and_duplicates_nothing():
    async def go():
        kv, sink = FaultyKV(MemoryKV(), fail_rate=1.0), MemorySink()
        e = eng(kv, sink, max_retries=2)
        with pytest.raises(KVError):
            await e.ingest(mk(1))                    # client sees an error, must resend
        assert sink.effects == []
        kv.fail_rate = 0.0                           # outage over
        assert await e.ingest(mk(1)) == Status.ACCEPTED
        assert await e.ingest(mk(1)) == Status.DUPLICATE
        assert ids(sink) == ["e1"]
    run(go())


# ---------------------------------------------------------- sink faults
@pytest.mark.parametrize("mode", ["before", "after"])
def test_sink_failure_modes(mode):
    async def go():
        inner = MemorySink()
        sink = FaultySink(inner)
        sink.once["deliver"] = mode                  # "after" = applied, ack lost
        e = eng(MemoryKV(), sink)
        await e.ingest(mk(1))
        assert ids(inner) == ["e1"] and e.m.sink_retries == 1
    run(go())


def test_sink_outage_keeps_events_buffered_then_delivers():
    async def go():
        inner = MemorySink()
        sink = FaultySink(inner, fail_rate=1.0)
        e = eng(MemoryKV(), sink, max_retries=1)
        for i in range(5):
            await e.ingest(mk(i))
        assert inner.effects == [] and e.stats()["buffered"] == 5
        sink.fail_rate = 0.0
        assert await e.drain() == 0
        assert ids(inner) == [f"e{i}" for i in range(5)]
    run(go())


# --------------------------------------------------------- process crashes
@pytest.mark.parametrize("point", CRASH_POINTS)
def test_crash_at_every_point_then_recover(point):
    async def go():
        kv, sink = MemoryKV(), MemorySink()
        events = [mk(i) for i in range(10)]
        e = eng(kv, sink, CrashInjector(point, after=4))
        crashed = False
        for ev in events:
            try:
                await e.ingest(ev)
            except SimulatedCrash:
                crashed = True
                break
        assert crashed
        e2 = eng(kv, sink)                           # "restart the process"
        await e2.recover()
        for ev in events:                            # at-least-once producer replays everything
            await e2.ingest(ev)
        await e2.drain()
        assert sorted(ids(sink)) == sorted(x.id for x in events)
        assert len(ids(sink)) == len(set(ids(sink))) == 10
    run(go())


def test_recovery_is_idempotent():
    async def go():
        kv, sink = MemoryKV(), MemorySink()
        e = eng(kv, sink, CrashInjector("after_buffer", after=1))
        with pytest.raises(SimulatedCrash):
            await e.ingest(mk(1))
        e2 = eng(kv, sink)
        assert await e2.recover() == 1
        assert await e2.recover() == 0
        assert ids(sink) == ["e1"]
    run(go())


def test_naive_sink_shows_why_idempotent_sink_is_required():
    async def go():
        kv, sink = MemoryKV(), NaiveSink()
        e = eng(kv, sink, CrashInjector("after_deliver", after=1))
        with pytest.raises(SimulatedCrash):
            await e.ingest(mk(1))
        e2 = eng(kv, sink)
        await e2.recover()
        assert len(sink.effects) == 2                # duplicate side effect: documented limit
    run(go())


# ------------------------------------------------------------------ chaos
@pytest.mark.parametrize("seed", range(20))
def test_chaos_random_crashes_and_flaky_kv_and_sink(seed):
    async def go():
        base, stream = make_stream(150, seed, dup_rate=0.5, lateness=1.0)
        mem, inner = MemoryKV(), MemorySink()
        kv = FaultyKV(mem, fail_rate=0.05, ack_loss_rate=0.05, seed=seed)
        sink = FaultySink(inner, fail_rate=0.05, ack_loss_rate=0.05, seed=seed + 1)
        inj = CrashInjector(rate=0.02, seed=seed)
        mk_e = lambda: DedupEngine(kv, sink, window=1e6, lateness=1.0, crash_hook=inj)
        e, i, crashes = mk_e(), 0, 0
        while i < len(stream):
            try:
                await e.ingest(stream[i])
                i += 1
            except KVError:
                continue                              # client retries same event
            except SimulatedCrash:
                crashes += 1
                e = mk_e()
                while True:
                    try:
                        await e.recover()
                        break
                    except (SimulatedCrash, KVError):
                        e = mk_e()
                i = max(0, i - 20)                    # producer replays recent events
        inj.rate = kv.fail_rate = kv.ack_loss_rate = sink.fail_rate = sink.ack_loss_rate = 0.0
        assert await e.drain() == 0
        got = ids(inner)
        assert len(got) == len(set(got)), "duplicate side effect!"
        assert set(got) == {x.id for x in base}, "lost event!"
    run(go())


def test_orphaned_pending_claim_is_adopted_on_client_resend():
    """Regression: claim applied, ack lost, retries exhausted -> record PENDING but never buffered.
    The client's resend must not be dropped as a plain duplicate."""
    async def go():
        kv, sink = FaultyKV(MemoryKV()), MemorySink()
        e = eng(kv, sink, max_retries=0)
        kv.once["put_if_absent"] = "after"
        with pytest.raises(KVError):
            await e.ingest(mk(1))
        assert sink.effects == []
        assert await e.ingest(mk(1)) == Status.DUPLICATE   # resend
        await e.drain()
        assert ids(sink) == ["e1"] and e.m.adopted == 1
    run(go())
