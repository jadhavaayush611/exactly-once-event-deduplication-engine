import asyncio
import json

from helpers import run

from dedup_engine import DedupEngine, DedupProxy, MemoryKV, MemorySink


async def talk(port, lines):
    r, w = await asyncio.open_connection("127.0.0.1", port)
    out = []
    for ln in lines:
        w.write((ln if isinstance(ln, str) else json.dumps(ln)).encode() + b"\n")
        await w.drain()
        out.append(json.loads(await r.readline()))
    w.close()
    return out


def test_proxy_end_to_end():
    async def go():
        sink = MemorySink()
        eng = DedupEngine(MemoryKV(), sink, window=100, lateness=1)
        proxy = DedupProxy(eng, port=0)
        await proxy.start()
        ev = {"id": "a", "ts": 1.0, "data": 1}
        res = await talk(proxy.port, [ev, ev, {"id": "b", "ts": 2.0}, "not json", {"cmd": "drain"}])
        await proxy.stop()
        assert [r["status"] for r in res[:4]] == ["accepted", "duplicate", "accepted", "bad_request"]
        assert res[4]["remaining"] == 0
        assert [e.id for e in sink.effects] == ["a", "b"]
    run(go())


def test_proxy_many_concurrent_clients_same_event():
    async def go():
        sink = MemorySink()
        eng = DedupEngine(MemoryKV(latency=0.0002), sink, window=100, lateness=0)
        proxy = DedupProxy(eng, port=0)
        await proxy.start()
        results = await asyncio.gather(*[talk(proxy.port, [{"id": "x", "ts": 1.0}]) for _ in range(50)])
        await proxy.stop()
        st = [r[0]["status"] for r in results]
        assert st.count("accepted") == 1 and st.count("duplicate") == 49
        assert len(sink.effects) == 1
    run(go())
