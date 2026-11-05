"""Duplicate protection on maintenance order generation."""

from __future__ import annotations

import asyncio

from conftest import build_plant, readings

from app import workorders as wo
from app.db import session_scope
from app.idempotency import get_store, work_order_key
from app.models import ProcessedKey, Role, Severity, WorkOrder, WorkOrderStatus
from app.pipeline.ingest import ingest_batch
from app.pipeline.processor import process_message
from app.rules import get_rule_engine


def _generate(session, **overrides):
    kwargs = {
        "asset_code": "MILL-01",
        "rule_id": "rule-1",
        "severity": Severity.CRITICAL,
        "title": "Mill vibration above alarm band",
        "part_number": None,
    }
    kwargs.update(overrides)
    return wo.generate(session, **kwargs)


def test_the_key_is_the_maintenance_need_not_the_request():
    """Same asset and rule is the same job, however it is spelled."""
    assert work_order_key("mill-01", "rule-1") == work_order_key("MILL-01 ", "rule-1")


def test_different_rules_on_one_asset_are_different_jobs():
    assert work_order_key("MILL-01", "rule-1") != work_order_key("MILL-01", "rule-2")


def test_generating_twice_creates_one_order(plant):
    first, created_first = _generate(plant)
    second, created_second = _generate(plant)

    assert created_first is True
    assert created_second is False
    assert second.id == first.id
    assert plant.query(WorkOrder).count() == 1


def test_fifty_replays_create_one_order(plant):
    first, _ = _generate(plant)
    for _ in range(50):
        again, created = _generate(plant)
        assert created is False
        assert again.number == first.number

    assert plant.query(WorkOrder).count() == 1


def test_the_durable_ledger_records_the_order(plant):
    order, _ = _generate(plant)
    row = plant.get(ProcessedKey, work_order_key("MILL-01", "rule-1"))

    assert row is not None
    assert row.result_ref == order.id
    assert row.scope == "work_order"


def test_cache_loss_does_not_cost_the_guarantee(plant):
    """Only the processed-key table stands between a flushed cache and a duplicate."""
    first, _ = _generate(plant)
    get_store().clear()

    second, created = _generate(plant)
    assert created is False
    assert second.id == first.id
    assert plant.query(WorkOrder).count() == 1


def test_a_recurring_fault_can_be_raised_again_once_the_job_is_closed(plant):
    """Idempotency must not mean the same machine can never break twice."""
    first, _ = _generate(plant)
    wo.complete(plant, first, actor="tech", actor_role=Role.TECHNICIAN)
    plant.commit()

    second, created = _generate(plant)
    assert created is True
    assert second.id != first.id
    assert plant.query(WorkOrder).count() == 2


def test_a_cancelled_job_also_reopens_the_need(plant):
    first, _ = _generate(plant)
    wo.cancel(plant, first, actor="super", actor_role=Role.SUPERVISOR, reason="wrong call")
    plant.commit()

    _second, created = _generate(plant)
    assert created is True


def test_concurrent_generation_yields_one_order(plant):
    """Eight workers racing the same fault must agree on one job."""

    async def worker():
        with session_scope() as handle:
            order, created = _generate(handle)
            return (order.number if order else None), created

    async def run():
        return await asyncio.gather(*(worker() for _ in range(8)))

    results = asyncio.run(run())
    numbers = {number for number, _ in results if number}
    assert len(numbers) == 1
    assert sum(1 for _, created in results if created) == 1
    assert plant.query(WorkOrder).count() == 1


async def test_replayed_events_do_not_stack_orders(session, bus):
    """End to end: the same telemetry replayed yields one job."""
    build_plant(session, consecutive=2)
    engine = get_rule_engine()
    engine.load(session)

    batch = readings([14.0, 14.5, 15.0])
    await ingest_batch(session, batch, bus)
    messages = await bus.consume("test", count=50, block_ms=10)
    for message in messages:
        await process_message(session, message, engine)

    assert session.query(WorkOrder).count() == 1

    # The bus redelivers everything, as an at-least-once bus does.
    for _ in range(20):
        for message in messages:
            await process_message(session, message, engine)

    assert session.query(WorkOrder).count() == 1


def test_a_completed_order_is_not_resurrected_by_a_replay(plant):
    order, _ = _generate(plant)
    number = order.number
    wo.complete(plant, order, actor="tech", actor_role=Role.TECHNICIAN)
    plant.commit()

    # A late replay of the original event arrives after the job was closed.
    again, created = _generate(plant)
    assert created is True
    assert again.number != number
    assert plant.get(WorkOrder, order.id).status is WorkOrderStatus.COMPLETED
