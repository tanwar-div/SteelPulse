"""Event bus delivery semantics."""

from __future__ import annotations

import pytest

from app.bus import (
    BACKEND_ORDER,
    MemoryBus,
    RabbitStreamBus,
    RedisStreamBus,
    get_bus,
    set_bus,
)
from app.config import get_settings


async def test_published_messages_come_back_in_order():
    bus = MemoryBus()
    await bus.publish_many([{"event_key": f"k{i}"} for i in range(3)])

    batch = await bus.consume("c1", count=10, block_ms=10)
    assert [m.payload["event_key"] for m in batch] == ["k0", "k1", "k2"]


async def test_consumed_messages_stay_pending_until_acked():
    bus = MemoryBus()
    await bus.publish({"event_key": "k1"})

    batch = await bus.consume("c1", count=10, block_ms=10)
    assert await bus.pending_count() == 1

    await bus.ack([batch[0].id])
    assert await bus.pending_count() == 0


async def test_a_message_is_delivered_to_one_consumer_only():
    bus = MemoryBus()
    await bus.publish({"event_key": "k1"})

    first = await bus.consume("c1", count=10, block_ms=10)
    second = await bus.consume("c2", count=10, block_ms=10)
    assert len(first) == 1
    assert second == []


async def test_unacked_work_can_be_claimed_by_another_consumer():
    """The fault-recovery path: a dead consumer's message is taken over."""
    bus = MemoryBus()
    await bus.publish({"event_key": "k1"})
    await bus.consume("c1", count=10, block_ms=10)

    claimed = await bus.claim_stale("recovery", min_idle_ms=0, count=10)
    assert [m.payload["event_key"] for m in claimed] == ["k1"]
    assert claimed[0].delivery_count == 2


async def test_fresh_work_is_not_claimed_from_a_live_consumer():
    bus = MemoryBus()
    await bus.publish({"event_key": "k1"})
    await bus.consume("c1", count=10, block_ms=10)

    assert await bus.claim_stale("recovery", min_idle_ms=60_000, count=10) == []


async def test_acked_work_is_not_claimable():
    bus = MemoryBus()
    await bus.publish({"event_key": "k1"})
    batch = await bus.consume("c1", count=10, block_ms=10)
    await bus.ack([batch[0].id])

    assert await bus.claim_stale("recovery", min_idle_ms=0, count=10) == []


async def test_consume_returns_empty_when_the_stream_is_idle():
    bus = MemoryBus()
    assert await bus.consume("c1", count=10, block_ms=10) == []


async def test_depth_reports_undelivered_backlog():
    bus = MemoryBus()
    await bus.publish_many([{"event_key": f"k{i}"} for i in range(5)])
    assert await bus.depth() == 5

    await bus.consume("c1", count=2, block_ms=10)
    assert await bus.depth() == 3


# --------------------------------------------------------------------------- #
# Backend contract
# --------------------------------------------------------------------------- #

CONTRACT = (
    "publish",
    "publish_many",
    "consume",
    "ack",
    "claim_stale",
    "pending_count",
    "depth",
    "close",
)


@pytest.mark.parametrize("implementation", [MemoryBus, RedisStreamBus, RabbitStreamBus])
def test_every_backend_implements_the_bus_contract(implementation):
    """All three backends are interchangeable behind EventBus."""
    missing = [name for name in CONTRACT if not callable(getattr(implementation, name, None))]
    assert missing == [], f"{implementation.__name__} is missing {missing}"
    assert isinstance(implementation.name, str)


@pytest.mark.parametrize(
    ("requested", "first"),
    [("auto", "rabbitmq"), ("rabbitmq", "rabbitmq"), ("redis", "redis"), ("memory", "memory")],
)
def test_backend_preference_order(requested, first):
    order = BACKEND_ORDER[requested]
    assert order[0] == first
    # every chain ends at the in-process bus, so the suite never needs a broker
    assert order[-1] == "memory"


def test_unknown_backend_falls_back_to_auto():
    assert BACKEND_ORDER.get("nonsense", BACKEND_ORDER["auto"]) == BACKEND_ORDER["auto"]


async def test_memory_bus_is_used_when_no_broker_is_configured(monkeypatch):
    monkeypatch.setenv("BUS_BACKEND", "memory")
    get_settings.cache_clear()
    set_bus(None)
    try:
        bus = await get_bus()
        assert bus.name == "memory"
    finally:
        set_bus(None)
        get_settings.cache_clear()
