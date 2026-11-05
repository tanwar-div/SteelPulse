"""Worker pool consuming the telemetry stream.

Each worker reads a batch, processes it, and acks only what succeeded.
Anything left unacked stays in the consumer group's pending list, and the
recovery loop claims entries whose worker has gone quiet - that is how a
killed worker's in-flight messages get finished rather than lost.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from app.bus import EventBus, Message, get_bus
from app.config import get_settings
from app.db import session_scope
from app.pipeline.processor import process_message
from app.retry import PermanentError
from app.rules import RuleEngine, get_rule_engine

log = logging.getLogger(__name__)


class WorkerPool:
    """Runs N consumers plus one recovery loop as asyncio tasks."""

    def __init__(self, bus: EventBus | None = None, engine: RuleEngine | None = None):
        self._bus = bus
        self._engine = engine
        # A pool that resolved its own dependencies must drop them when it
        # stops, so a restart binds to the current bus and event loop rather
        # than to the ones a previous run left behind.
        self._owns_bus = bus is None
        self._owns_engine = engine is None
        self._tasks: list[asyncio.Task] = []
        self._stopping = asyncio.Event()
        self.stats = {
            "processed": 0,
            "skipped": 0,
            "failed": 0,
            "dead_lettered": 0,
            "alerts_opened": 0,
            "work_orders_created": 0,
            "recovered": 0,
        }

    async def start(self) -> None:
        settings = get_settings()
        self._bus = self._bus or await get_bus()
        if self._engine is None:
            self._engine = get_rule_engine()
            if not self._engine.loaded:
                with session_scope() as session:
                    self._engine.load(session)

        self._stopping.clear()
        for index in range(settings.worker_count):
            self._tasks.append(asyncio.create_task(self._run(f"worker-{index}")))
        self._tasks.append(asyncio.create_task(self._recover()))
        log.info("worker pool started with %d consumers", settings.worker_count)

    async def stop(self) -> None:
        self._stopping.set()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()
        if self._owns_bus:
            self._bus = None
        if self._owns_engine:
            self._engine = None
        log.info("worker pool stopped: %s", self.stats)

    @property
    def running(self) -> bool:
        return bool(self._tasks) and not self._stopping.is_set()

    async def _run(self, consumer: str) -> None:
        settings = get_settings()
        assert self._bus is not None
        while not self._stopping.is_set():
            try:
                batch = await self._bus.consume(
                    consumer, settings.batch_size, settings.block_ms
                )
                if not batch:
                    continue
                await self.handle(batch, consumer)
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - a consumer must not die
                log.exception("consumer %s hit an unexpected error", consumer)
                await asyncio.sleep(0.1)

    async def handle(self, batch: list[Message], consumer: str) -> None:
        """Process a batch, acking only what reached a terminal state."""
        assert self._bus is not None
        acked: list[str] = []
        for message in batch:
            try:
                with session_scope() as session:
                    outcome = await process_message(session, message, self._engine)
                if outcome.skipped:
                    self.stats["skipped"] += 1
                else:
                    self.stats["processed"] += 1
                    self.stats["alerts_opened"] += outcome.alerts_opened
                    self.stats["work_orders_created"] += outcome.work_orders_created
                acked.append(message.id)
            except PermanentError as exc:
                # Nothing a redelivery can fix: ack it so the stream drains,
                # and leave the event row parked in dead-letter for an operator.
                self.stats["dead_lettered"] += 1
                log.warning("dead-lettering %s: %s", message.id, exc)
                acked.append(message.id)
            except Exception as exc:
                # Leave it pending; the recovery loop will re-deliver it.
                self.stats["failed"] += 1
                log.warning("deferring %s for retry: %s", message.id, exc)

        if acked:
            await self._bus.ack(acked)

    async def _recover(self) -> None:
        """Claim entries whose consumer stopped acking, then run them again."""
        settings = get_settings()
        assert self._bus is not None
        interval = max(0.5, settings.stale_claim_ms / 1000 / 3)
        while not self._stopping.is_set():
            try:
                await asyncio.sleep(interval)
                claimed = await self._bus.claim_stale(
                    "recovery", settings.stale_claim_ms, settings.batch_size
                )
                if claimed:
                    self.stats["recovered"] += len(claimed)
                    log.info("recovery claimed %d stale messages", len(claimed))
                    await self.handle(claimed, "recovery")
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover
                log.exception("recovery loop error")


_pool: WorkerPool | None = None


def get_pool() -> WorkerPool:
    global _pool
    if _pool is None:
        _pool = WorkerPool()
    return _pool


def set_pool(pool: WorkerPool | None) -> None:
    global _pool
    _pool = pool
