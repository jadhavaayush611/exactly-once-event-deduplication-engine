"""Send a duplicate-heavy, out-of-order NDJSON stream to the running proxy.
   Terminal 1:  python -m dedup_engine.proxy --sink file:out.jsonl
   Terminal 2:  python demos/demo_client.py"""
import asyncio
import json
import random

HOST, PORT = "127.0.0.1", 9000


async def main():
    rng = random.Random(7)
    base = [{"id": f"evt-{i}", "ts": float(i), "data": {"n": i}} for i in range(30)]
    stream = base + [rng.choice(base) for _ in range(30)]
    stream.sort(key=lambda e: e["ts"] + rng.uniform(0, 3))
    r, w = await asyncio.open_connection(HOST, PORT)
    counts = {}
    for e in stream:
        w.write((json.dumps(e) + "\n").encode())
        await w.drain()
        st = json.loads(await r.readline())["status"]
        counts[st] = counts.get(st, 0) + 1
    w.write(b'{"cmd":"drain"}\n')
    await w.drain()
    print("responses:", counts)
    print("drain    :", json.loads(await r.readline()))
    w.close()


asyncio.run(main())
