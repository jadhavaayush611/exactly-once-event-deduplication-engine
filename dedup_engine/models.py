"""Core data types shared by every layer of the engine."""
from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any


class Status(str, Enum):
    ACCEPTED = "accepted"                  # durably claimed (write-ahead); delivery is guaranteed
    DUPLICATE = "duplicate"                # already seen inside the sliding window
    TOO_OLD = "too_old"                    # older than the window -> dedup state evicted -> DLQ
    TOO_FAR_FUTURE = "too_far_future"      # exceeds future-skew threshold -> DLQ / rejected
    INVALID_TIMESTAMP = "invalid_timestamp"# non-finite, missing, or malformed timestamp -> DLQ
    BUFFER_FULL = "buffer_full"            # reorder buffer capacity exceeded -> backpressure


class KVError(Exception):
    """Transient failure talking to the key-value store."""


class SinkError(Exception):
    """Transient failure talking to the downstream sink."""


class SimulatedCrash(Exception):
    """Raised by the crash injector to emulate a process kill."""


@dataclass(frozen=True)
class Event:
    id: str          # idempotency / dedup key
    ts: float        # event time (seconds)
    data: Any = None

    def to_dict(self) -> dict:
        return {"id": self.id, "ts": self.ts, "data": self.data}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"))

    @staticmethod
    def from_dict(d: dict) -> "Event":
        return Event(id=str(d["id"]), ts=float(d["ts"]), data=d.get("data"))
