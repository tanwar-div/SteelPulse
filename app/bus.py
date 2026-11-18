"""Event bus with at-least-once delivery.

Three backends behind one protocol, chosen by BUS_BACKEND:

    rabbitmq  a durable queue on a direct exchange, consumed with a prefetch
              window and explicit acks. Unacked work is redelivered by the
              broker, which is AMQP's answer to a dead consumer.
    redis     Redis Streams with consumer groups: a message stays in the
              group's pending-entries list until acked, and a stale entry is
              claimed by another worker (XAUTOCLAIM).
    memory    the same semantics in-process - pending list, ack, stale claim -
              so the pipeline, the tests and the load scripts behave
              identically with no broker running.

The guarantee is the same in all three: nothing is lost, something may arrive
twice, and the consumer is idempotent so that is safe.
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


# --------------------------------------------------------------------------- #
# RabbitMQ implementation
# --------------------------------------------------------------------------- #


class RabbitStreamBus:
    """AMQP queue with a prefetch window, explicit acks and broker redelivery.

    The mapping from the Redis Streams model is close but not identical, and
    the difference is the interesting part:

    * a consumer group becomes one durable queue that every worker consumes,
      so the broker round-robins deliveries instead of each worker reading a
      private cursor;
    * acking is per delivery tag rather than per stream id;
    * there is no client-visible pending-entries list to sweep. An unacked
      message is the broker's responsibility: it goes back on the queue when
      the channel drops. ``claim_stale`` therefore requeues work that has sat
      unacked too long (basic.nack, requeue=true) and lets it arrive again
      through the normal consume path.

    Deliveries are pushed into a local buffer rather than pulled one at a time
    with basic.get, because a round trip per message would cap throughput an
    order of magnitude below the prefetch-window approach.
    """

    name = "rabbitmq"

    def __init__(self, connection, channel, queue, exchange, routing_key: str, prefetch: int):
        self._connection = connection
        self._channel = channel
        self._queue = queue
        self._exchange = exchange
        self._routing_key = routing_key
        self._prefetch = prefetch
        self._buffer: asyncio.Queue = asyncio.Queue()
        self._unacked: dict[str, tuple[object, float]] = {}
        self._consumer_tag: str | None = None
        self.requeued_total = 0

    @classmethod
    async def connect(
        cls,
        url: str,
        exchange_name: str,
        queue_name: str,
        prefetch: int,
        timeout: float,
    ) -> RabbitStreamBus:
        import aio_pika

        connection = await asyncio.wait_for(aio_pika.connect_robust(url), timeout=timeout)
        channel = await connection.channel()
        # Bound in-flight work: the broker will not push more than this many
        # unacked messages, which is what keeps a burst from exhausting memory.
        await channel.set_qos(prefetch_count=prefetch)

        exchange = await channel.declare_exchange(
            exchange_name, aio_pika.ExchangeType.DIRECT, durable=True
        )
        queue = await channel.declare_queue(queue_name, durable=True)
        await queue.bind(exchange, routing_key=queue_name)

        bus = cls(connection, channel, queue, exchange, queue_name, prefetch)
        bus._consumer_tag = await queue.consume(bus._on_delivery, no_ack=False)
        return bus

    async def _on_delivery(self, message) -> None:
        """Buffer a delivery. Nothing is acked until the pipeline says so."""
        await self._buffer.put(message)

    def _encode(self, payload: dict):
        import aio_pika

        return aio_pika.Message(
            body=json.dumps(payload, separators=(",", ":")).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
        )

    async def publish(self, payload: dict) -> str:
        return (await self.publish_many([payload]))[0]

    async def publish_many(self, payloads: list[dict]) -> list[str]:
        await asyncio.gather(
            *(
                self._exchange.publish(self._encode(payload), routing_key=self._routing_key)
                for payload in payloads
            )
        )
        # AMQP has no broker-assigned message id, so the event key stands in.
        return [str(payload.get("event_key", "")) for payload in payloads]

    async def consume(self, consumer: str, count: int, block_ms: int) -> list[Message]:
        """Take up to ``count`` buffered deliveries, waiting at most block_ms."""
        batch: list[Message] = []
        try:
            first = await asyncio.wait_for(self._buffer.get(), timeout=block_ms / 1000)
        except TimeoutError:
            return batch

        delivery = first
        while True:
            batch.append(self._track(delivery))
            if len(batch) >= count:
                break
            try:
                delivery = self._buffer.get_nowait()
            except asyncio.QueueEmpty:
                break
        return batch

    def _track(self, delivery) -> Message:
        message_id = str(delivery.delivery_tag)
        self._unacked[message_id] = (delivery, time.monotonic())
        return Message(
            id=message_id,
            payload=json.loads(delivery.body),
            delivery_count=2 if delivery.redelivered else 1,
        )

    async def ack(self, message_ids: list[str]) -> int:
        acked = 0
        for message_id in message_ids:
            entry = self._unacked.pop(message_id, None)
            if entry is None:
                continue
            try:
                await entry[0].ack()
                acked += 1
            except Exception:  # pragma: no cover - channel died mid-ack
                # Not acking is safe: the broker redelivers it.
                log.warning("ack failed for delivery %s", message_id)
        return acked

    async def claim_stale(self, consumer: str, min_idle_ms: int, count: int) -> list[Message]:
        """Requeue work that has sat unacked too long.

        Returns an empty list by design: the redelivery arrives through the
        normal consume path with ``redelivered`` set, rather than being handed
        straight back to the caller the way XAUTOCLAIM does.
        """
        cutoff = time.monotonic() - min_idle_ms / 1000
        stale = [(mid, entry) for mid, entry in list(self._unacked.items()) if entry[1] <= cutoff][
            :count
        ]

        for message_id, (delivery, _delivered_at) in stale:
            self._unacked.pop(message_id, None)
            try:
                await delivery.nack(requeue=True)
                self.requeued_total += 1
            except Exception:  # pragma: no cover - depends on broker state
                log.warning("requeue failed for delivery %s", message_id)
        if stale:
            log.info("requeued %d stale deliveries for redelivery", len(stale))
        return []

    async def pending_count(self) -> int:
        return len(self._unacked)

    async def depth(self) -> int:
        """Queue backlog, read on a throwaway channel so a failed passive
        declare cannot take the consumer's channel down with it."""
        try:
            async with self._connection.channel() as channel:
                queue = await channel.declare_queue(self._queue.name, passive=True)
                return int(queue.declaration_result.message_count or 0)
        except Exception:  # pragma: no cover - depends on broker state
            return 0

    async def close(self) -> None:
        try:
            if self._consumer_tag is not None:
                await self._queue.cancel(self._consumer_tag)
        except Exception:  # pragma: no cover
            pass
        await self._connection.close()


# --------------------------------------------------------------------------- #
# Backend selection
# --------------------------------------------------------------------------- #

_bus: EventBus | None = None

BACKEND_ORDER = {
    "auto": ("rabbitmq", "redis", "memory"),
    "rabbitmq": ("rabbitmq", "memory"),
    "redis": ("redis", "memory"),
    "memory": ("memory",),
}


async def _connect(backend: str, settings) -> EventBus | None:
    if backend == "rabbitmq":
        return await RabbitStreamBus.connect(
            settings.rabbitmq_url,
            settings.rabbitmq_exchange,
            settings.rabbitmq_queue,
            settings.rabbitmq_prefetch,
            settings.rabbitmq_connect_timeout,
        )
    if backend == "redis":
        return await RedisStreamBus.connect(
            settings.redis_url,
            settings.telemetry_stream,
            settings.consumer_group,
            settings.stream_maxlen,
        )
    return MemoryBus(maxlen=settings.stream_maxlen)


async def get_bus() -> EventBus:
    """Process-wide bus: the configured backend, or the next one that answers.

    Falling back rather than failing is deliberate - the suite and the load
    scripts must run with no services - but the chosen backend is reported by
    ``/health`` and the startup log, so a silent downgrade is still visible.
    """
    global _bus
    if _bus is not None:
        return _bus

    settings = get_settings()
    requested = settings.bus_backend
    order = BACKEND_ORDER.get(requested, BACKEND_ORDER["auto"])

    for backend in order:
        try:
            _bus = await _connect(backend, settings)
        except Exception as exc:  # pragma: no cover - depends on local services
            level = log.warning if backend == requested else log.info
            level("event bus: %s unavailable (%s)", backend, exc.__class__.__name__)
            continue
        if _bus is not None:
            log.info("event bus: %s", _bus.name)
            return _bus

    _bus = MemoryBus(maxlen=settings.stream_maxlen)
    return _bus


def current_bus() -> EventBus | None:
    """The resolved bus, or None if nothing has asked for one yet.

    Sync, and does no I/O - for reporting which backend is in use from code
    that is not already inside the event loop.
    """
    return _bus


def set_bus(bus: EventBus | None) -> None:
    """Override the process-wide bus (tests, load harness)."""
    global _bus
    _bus = bus
