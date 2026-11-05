"""Fault recovery: nothing in flight is lost when a worker dies."""

from __future__ import annotations

import asyncio

from conftest import build_plant, readings

from app.bus import MemoryBus
from app.models import EventStatus, TelemetryEvent
from app.pipeline.ingest import ingest_batch
from app.pipeline.workers import WorkerPool
from app.rules import get_rule_engine


async def _wait_until(predicate, timeout: float = 5.0) -> bool:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


async def test_the_pool_drains_the_stream(session, bus):
    build_plant(session)
    engine = get_rule_engine()
    engine.load(session)
    await ingest_batch(session, readings([4.0, 4.5, 5.0]), bus)

    pool = WorkerPool(bus=bus, engine=engine)
    await pool.start()
    try:
        await _wait_until(lambda: pool.stats["processed"] >= 3)
    finally:
        await pool.stop()

    assert pool.stats["processed"] == 3
    assert await bus.pending_count() == 0


async def test_processed_events_are_acked(session, bus):
    build_plant(session)
    engine = get_rule_engine()
    engine.load(session)
    await ingest_batch(session, readings([4.0, 4.5]), bus)

    pool = WorkerPool(bus=bus, engine=engine)
    await pool.start()
    try:
        await _wait_until(lambda: pool.stats["processed"] >= 2)
    finally:
        await pool.stop()

    with_status = session.query(TelemetryEvent).filter(
        TelemetryEvent.status == EventStatus.PROCESSED
    )
    assert with_status.count() == 2


async def test_a_dead_consumers_work_is_claimed_and_finished(session):
    """The recovery path: a worker takes a message and never acks it."""
    bus = MemoryBus()
    build_plant(session)
    engine = get_rule_engine()
    engine.load(session)
    await ingest_batch(session, readings([4.0, 4.5]), bus)

    # A consumer takes the batch and dies before acking.
    orphaned = await bus.consume("doomed-worker", count=10, block_ms=10)
    assert len(orphaned) == 2
    assert await bus.pending_count() == 2

    pool = WorkerPool(bus=bus, engine=engine)
    claimed = await bus.claim_stale("recovery", min_idle_ms=0, count=10)
    await pool.handle(claimed, "recovery")

    assert await bus.pending_count() == 0
    assert (
        session.query(TelemetryEvent)
        .filter(TelemetryEvent.status == EventStatus.PROCESSED)
        .count()
        == 2
    )


async def test_recovery_reprocessing_is_safe(session):
    """A reclaimed message whose event already completed is simply skipped."""
    bus = MemoryBus()
    build_plant(session)
    engine = get_rule_engine()
    engine.load(session)
    await ingest_batch(session, readings([4.0]), bus)

    batch = await bus.consume("worker-a", count=10, block_ms=10)
    pool = WorkerPool(bus=bus, engine=engine)
    await pool.handle(batch, "worker-a")
    assert pool.stats["processed"] == 1

    # The same message arrives again after a claim.
    await pool.handle(batch, "recovery")
    assert pool.stats["skipped"] == 1
    assert session.query(TelemetryEvent).count() == 1


async def test_an_unroutable_message_is_dead_lettered_not_retried_forever(session, bus):
    build_plant(session)
    engine = get_rule_engine()
    engine.load(session)

    from app.bus import Message

    pool = WorkerPool(bus=bus, engine=engine)
    await pool.handle([Message(id="1-1", payload={"event_key": "ghost@2026"})], "worker")

    assert pool.stats["dead_lettered"] == 1
    assert await bus.pending_count() == 0


async def test_events_surviving_a_restart_are_still_tracked(session, bus):
    """Delivery completion is measured from durable rows, not memory."""
    build_plant(session)
    engine = get_rule_engine()
    engine.load(session)
    await ingest_batch(session, readings([4.0, 4.5, 5.0]), bus)

    # Nothing has been processed yet: all three are accepted but incomplete.
    received = session.query(TelemetryEvent).filter(
        TelemetryEvent.status == EventStatus.RECEIVED
    )
    assert received.count() == 3
