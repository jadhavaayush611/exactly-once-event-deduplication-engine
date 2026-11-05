"""Stream-processing de-duplication proxy: newline-delimited JSON over TCP.

  client -> {"id":"a1","ts":12.5,"data":{...}}\n          <- {"id":"a1","status":"accepted"}\n
  client -> {"cmd":"stats"}\n / {"cmd":"drain"}\n         <- {"status":"ok",...}\n
Statuses: accepted | duplicate | too_old | error (transient, client should resend) | bad_request
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from typing import Optional

from .engine import DedupEngine
from .kv import MemoryKV, RedisKV
from .models import Event, KVError, SinkError
from .sink import FileSink, MemorySink, StdoutSink


class DedupProxy:
    def __init__(self, engine: DedupEngine, host="127.0.0.1", port=9000, idle_drain: float = 0.0):
        self.engine, self.host, self.port, self.idle_drain = engine, host, port, idle_drain
        self._server: Optional[asyncio.AbstractServer] = None
        self._idle_task: Optional[asyncio.Task] = None
        self._last = time.monotonic()

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]
        if self.idle_drain > 0:
            self._idle_task = asyncio.create_task(self._idle_loop())

    async def stop(self) -> None:
        if self._idle_task:
            self._idle_task.cancel()
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _idle_loop(self) -> None:
        # processing-time timer: if the stream goes quiet, flush the reorder buffer
        while True:
            await asyncio.sleep(self.idle_drain)
            if time.monotonic() - self._last >= self.idle_drain:
                try:
                    await self.engine.drain()
                except (KVError, SinkError):
                    pass

    async def _handle(self, reader, writer) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                resp = await self._process(line)
                writer.write((json.dumps(resp) + "\n").encode())
                await writer.drain()
        except ConnectionError:
            pass
        finally:
            writer.close()

    async def _process(self, line: bytes) -> dict:
        try:
            msg = json.loads(line)
        except ValueError:
            return {"status": "bad_request"}
        try:
            if "cmd" in msg:
                if msg["cmd"] == "drain":
                    left = await self.engine.drain()
                    return {"status": "ok", "remaining": left, "stats": self.engine.stats()}
                if msg["cmd"] == "stats":
                    return {"status": "ok", "stats": self.engine.stats()}
                return {"status": "bad_request"}
            ev = Event.from_dict(msg)
        except (KeyError, TypeError, ValueError):
            return {"status": "bad_request"}
        except (KVError, SinkError) as e:
            return {"status": "error", "error": str(e)}
        try:
            st = await self.engine.ingest(ev)
        except KVError as e:
            return {"id": ev.id, "status": "error", "error": str(e)}
        self._last = time.monotonic()
        return {"id": ev.id, "status": st.value}


async def _amain(a) -> None:
    kv = RedisKV(a.kv) if a.kv.startswith("redis") else MemoryKV()
    sink = StdoutSink() if a.sink == "stdout" else FileSink(a.sink[5:]) if a.sink.startswith("file:") else MemorySink()
    engine = DedupEngine(kv, sink, window=a.window, lateness=a.lateness)
    n = await engine.recover()
    proxy = DedupProxy(engine, a.host, a.port, a.idle_drain)
    await proxy.start()
    print(f"dedup proxy on {a.host}:{proxy.port} | kv={a.kv} sink={a.sink} window={a.window}s "
          f"lateness={a.lateness}s | recovered {n} pending events", flush=True)
    await asyncio.Event().wait()


def main() -> None:
    p = argparse.ArgumentParser(description="Exactly-once event de-duplication proxy")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--kv", default="memory", help="memory | redis://localhost:6379/0")
    p.add_argument("--sink", default="stdout", help="stdout | memory | file:out.jsonl")
    p.add_argument("--window", type=float, default=300.0)
    p.add_argument("--lateness", type=float, default=5.0)
    p.add_argument("--idle-drain", type=float, default=2.0)
    try:
        asyncio.run(_amain(p.parse_args()))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
