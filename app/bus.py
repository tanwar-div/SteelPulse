"""Event bus with at-least-once delivery.

Redis Streams with consumer groups is the deployment target: publishing
appends to the stream, each consumer group tracks its own cursor, and a
message stays in the group's pending-entries list until it is acked. A
consumer that dies mid-message leaves its entry pending, and another worker
claims it once it goes stale - that is the fault-recovery path.

An in-process implementation with the same semantics (pending list, ack,
stale claim) takes over when Redis is unreachable, so the pipeline, the tests
and the load scripts behave identically with no broker running.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Protocol

from app.config import get_settings

log = logging.getLogger(__name__)


@dataclass
class Message:
    """One delivery of one event."""

    id: str
    payload: dict
    delivery_count: int = 1

    @property
    def event_key(self) -> str:
        return str(self.payload.get("event_key", ""))


class EventBus(Protocol):
    name: str

    async def publish(self, payload: dict) -> str: ...
    async def publish_many(self, payloads: list[dict]) -> list[str]: ...
    async def consume(self, consumer: str, count: int, block_ms: int) -> list[Message]: ...
    async def ack(self, message_ids: list[str]) -> int: ...
    async def claim_stale(self, consumer: str, min_idle_ms: int, count: int) -> list[Message]: ...
    async def pending_count(self) -> int: ...
    async def depth(self) -> int: ...
    async def close(self) -> None: ...


# --------------------------------------------------------------------------- #
# In-process implementation
# --------------------------------------------------------------------------- #


@dataclass
class _PendingEntry:
    message: Message
    consumer: str
    delivered_at: float = field(default_factory=time.monotonic)


class MemoryBus:
    """Single-process stand-in for Redis Streams, with the same guarantees."""

    name = "memory"

    def __init__(self, maxlen: int = 1_000_000):
        self._queue: list[Message] = []
        self._pending: dict[str, _PendingEntry] = {}
        self._sequence = 0
        self._maxlen = maxlen
        self._lock = asyncio.Lock()
        self._arrival = asyncio.Event()
        self._delivered_total = 0

    async def publish(self, payload: dict) -> str:
        return (await self.publish_many([payload]))[0]

    async def publish_many(self, payloads: list[dict]) -> list[str]:
        async with self._lock:
            ids = []
            for payload in payloads:
                self._sequence += 1
                message_id = f"{int(time.time() * 1000)}-{self._sequence}"
                self._queue.append(Message(id=message_id, payload=payload))
                ids.append(message_id)
            if len(self._queue) > self._maxlen:
                del self._queue[: len(self._queue) - self._maxlen]
        self._arrival.set()
        return ids

    async def consume(self, consumer: str, count: int, block_ms: int) -> list[Message]:
        deadline = time.monotonic() + block_ms / 1000
        while True:
            async with self._lock:
                if self._queue:
                    batch = self._queue[:count]
                    del self._queue[: len(batch)]
                    for message in batch:
                        self._pending[message.id] = _PendingEntry(message, consumer)
                    self._delivered_total += len(batch)
                    return batch
                self._arrival.clear()
            if time.monotonic() >= deadline:
                return []
            try:
                await asyncio.wait_for(
                    self._arrival.wait(), timeout=max(0.001, deadline - time.monotonic())
                )
            except TimeoutError:
                return []

    async def ack(self, message_ids: list[str]) -> int:
        async with self._lock:
            return sum(1 for mid in message_ids if self._pending.pop(mid, None) is not None)

    async def claim_stale(self, consumer: str, min_idle_ms: int, count: int) -> list[Message]:
        """Take over entries whose original consumer has gone quiet."""
        cutoff = time.monotonic() - min_idle_ms / 1000
        claimed: list[Message] = []
        async with self._lock:
            for entry in list(self._pending.values()):
                if len(claimed) >= count:
                    break
                if entry.delivered_at <= cutoff:
                    entry.consumer = consumer
                    entry.delivered_at = time.monotonic()
                    entry.message.delivery_count += 1
                    claimed.append(entry.message)
        return claimed

    async def pending_count(self) -> int:
        async with self._lock:
            return len(self._pending)

    async def depth(self) -> int:
        async with self._lock:
            return len(self._queue)

    async def close(self) -> None:
        return None


# --------------------------------------------------------------------------- #
# Redis Streams implementation
# --------------------------------------------------------------------------- #


class RedisStreamBus:
    """Redis Streams consumer group with explicit acks and stale claiming."""

    name = "redis"

    def __init__(self, client, stream: str, group: str, maxlen: int):
        self._redis = client
        self._stream = stream
        self._group = group
        self._maxlen = maxlen

    @classmethod
    async def connect(cls, url: str, stream: str, group: str, maxlen: int):
        import redis.asyncio as aioredis

        client = aioredis.from_url(url, decode_responses=True)
        await client.ping()
        try:
            await client.xgroup_create(stream, group, id="0", mkstream=True)
        except Exception as exc:  # group already exists
            if "BUSYGROUP" not in str(exc):
                raise
        return cls(client, stream, group, maxlen)

    @staticmethod
    def _encode(payload: dict) -> dict:
        return {"data": json.dumps(payload, separators=(",", ":"))}

    @staticmethod
    def _decode(fields: dict) -> dict:
        return json.loads(fields["data"])

    async def publish(self, payload: dict) -> str:
        return await self._redis.xadd(
            self._stream, self._encode(payload), maxlen=self._maxlen, approximate=True
        )

    async def publish_many(self, payloads: list[dict]) -> list[str]:
        pipe = self._redis.pipeline(transaction=False)
        for payload in payloads:
            pipe.xadd(self._stream, self._encode(payload), maxlen=self._maxlen, approximate=True)
        return await pipe.execute()

    async def consume(self, consumer: str, count: int, block_ms: int) -> list[Message]:
        response = await self._redis.xreadgroup(
            self._group, consumer, {self._stream: ">"}, count=count, block=block_ms
        )
        if not response:
            return []
        _stream, entries = response[0]
        return [Message(id=mid, payload=self._decode(fields)) for mid, fields in entries]

    async def ack(self, message_ids: list[str]) -> int:
        if not message_ids:
            return 0
        return await self._redis.xack(self._stream, self._group, *message_ids)

    async def claim_stale(self, consumer: str, min_idle_ms: int, count: int) -> list[Message]:
        _cursor, entries, _deleted = await self._redis.xautoclaim(
            self._stream, self._group, consumer, min_idle_time=min_idle_ms, count=count
        )
        return [
            Message(id=mid, payload=self._decode(fields), delivery_count=2)
            for mid, fields in entries
            if fields
        ]

    async def pending_count(self) -> int:
        summary = await self._redis.xpending(self._stream, self._group)
        return int(summary["pending"]) if summary else 0

    async def depth(self) -> int:
        info = await self._redis.xinfo_groups(self._stream)
        for group in info:
            if group["name"] == self._group:
                return int(group.get("lag") or 0)
        return 0

    async def close(self) -> None:
        await self._redis.aclose()


_bus: EventBus | None = None


async def get_bus() -> EventBus:
    """Process-wide bus, Redis if reachable and in-process otherwise."""
    global _bus
    if _bus is not None:
        return _bus
    settings = get_settings()
    try:
        _bus = await RedisStreamBus.connect(
            settings.redis_url,
            settings.telemetry_stream,
            settings.consumer_group,
            settings.stream_maxlen,
        )
        log.info("event bus: redis streams (%s)", settings.telemetry_stream)
    except Exception as exc:  # pragma: no cover - depends on local services
        log.info("event bus: in-process (redis unreachable: %s)", exc.__class__.__name__)
        _bus = MemoryBus(maxlen=settings.stream_maxlen)
    return _bus


def set_bus(bus: EventBus | None) -> None:
    """Override the process-wide bus (tests, load harness)."""
    global _bus
    _bus = bus
