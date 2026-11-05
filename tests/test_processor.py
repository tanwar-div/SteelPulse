"""Event processing: detection, alerting, work orders, energy."""

from __future__ import annotations

from datetime import timedelta

import pytest
from conftest import BASE_TIME, build_plant, readings

from app.bus import Message
from app.models import (
    Alert,
    AlertStatus,
    EnergyRollup,
    EventStatus,
    Severity,
    TelemetryEvent,
    WorkOrder,
    WorkOrderStatus,
)
from app.pipeline.ingest import Reading, ingest_batch
from app.pipeline.processor import process_message
from app.retry import PermanentError
from app.rules import get_rule_engine


async def _run(session, bus, values, metric="vibration_mm_s", quality=1.0):
    """Ingest a run of readings and process every published message."""
    engine = get_rule_engine()
    if not engine.loaded:
        engine.load(session)

    await ingest_batch(
        session,
        readings(
            values,
            sensor_tag=f"MILL-01.{metric}",
            metric=metric,
            quality=quality,
        ),
        bus,
    )
    outcomes = []
    for message in await bus.consume("test", count=500, block_ms=10):
        outcomes.append(await process_message(session, message, engine))
    return outcomes


async def test_a_normal_run_processes_without_alerting(session, bus):
    build_plant(session, consecutive=2)
    outcomes = await _run(session, bus, [4.0, 4.5, 5.0])

    assert sum(o.alerts_opened for o in outcomes) == 0
    assert session.query(Alert).count() == 0
    assert all(
        event.status is EventStatus.PROCESSED for event in session.query(TelemetryEvent).all()
    )


async def test_a_sustained_breach_opens_one_alert(session, bus):
    build_plant(session, consecutive=2)
    outcomes = await _run(session, bus, [14.0, 14.5, 15.0, 15.5])

    assert sum(o.alerts_opened for o in outcomes) == 1
    alert = session.query(Alert).one()
    assert alert.severity is Severity.CRITICAL
    assert alert.status is AlertStatus.OPEN
    assert alert.occurrences == 3  # three samples past the two-sample streak


async def test_a_critical_alert_generates_a_work_order(session, bus):
    build_plant(session, consecutive=2)
    await _run(session, bus, [14.0, 14.5, 15.0])

    order = session.query(WorkOrder).one()
    assert order.asset_code == "MILL-01"
    assert order.severity is Severity.CRITICAL
    assert order.part_number == "BRG-6208"
    assert order.alert_id == session.query(Alert).one().id


async def test_continuing_breaches_do_not_stack_work_orders(session, bus):
    """The point of the idempotency key: one fault is one job."""
    build_plant(session, consecutive=2)
    await _run(session, bus, [14.0] * 25)

    assert session.query(WorkOrder).count() == 1
    assert session.query(Alert).count() == 1


async def test_a_medium_alert_does_not_auto_generate_work(session, bus):
    """Only severities on the auto list become jobs on their own."""
    build_plant(session)
    await _run(session, bus, [120.0, 121.0], metric="temperature_c")

    assert session.query(Alert).count() == 1
    assert session.query(WorkOrder).count() == 0


async def test_processing_marks_the_event_complete(session, bus):
    build_plant(session)
    await _run(session, bus, [4.0])

    event = session.query(TelemetryEvent).one()
    assert event.status is EventStatus.PROCESSED
    assert event.processed_at is not None
    assert event.attempts == 1
    assert event.latency_ms >= 0


async def test_an_already_processed_event_is_skipped_on_redelivery(session, bus):
    build_plant(session)
    engine = get_rule_engine()
    engine.load(session)
    await ingest_batch(session, readings([4.0]), bus)

    batch = await bus.consume("test", count=10, block_ms=10)
    first = await process_message(session, batch[0], engine)
    second = await process_message(session, batch[0], engine)

    assert first.skipped is False
    assert second.skipped is True
    assert session.query(TelemetryEvent).one().attempts == 1


async def test_a_message_with_no_event_row_is_permanent(session, bus):
    build_plant(session)
    engine = get_rule_engine()
    engine.load(session)

    with pytest.raises(PermanentError):
        await process_message(
            session, Message(id="1-1", payload={"event_key": "nope@2026"}), engine
        )


async def test_power_readings_accumulate_into_energy(session, bus):
    build_plant(session)
    # 3600 kW for three one-second samples is 1 kWh per sample.
    await _run(session, bus, [3600.0, 3600.0, 3600.0], metric="power_kw")

    rollup = session.query(EnergyRollup).one()
    assert rollup.asset_code == "MILL-01"
    assert rollup.sample_count == 3
    assert rollup.kwh == pytest.approx(3.0, rel=1e-6)
    assert rollup.peak_kw == 3600.0


async def test_energy_splits_across_hour_buckets(session, bus):
    build_plant(session)
    engine = get_rule_engine()
    engine.load(session)

    batch = [
        Reading("MILL-01.power_kw", "power_kw", 1800.0, BASE_TIME),
        Reading("MILL-01.power_kw", "power_kw", 1800.0, BASE_TIME + timedelta(hours=1)),
    ]
    await ingest_batch(session, batch, bus)
    for message in await bus.consume("test", count=10, block_ms=10):
        await process_message(session, message, engine)

    assert session.query(EnergyRollup).count() == 2


async def test_non_power_metrics_do_not_create_energy_rows(session, bus):
    build_plant(session)
    await _run(session, bus, [4.0, 4.5])
    assert session.query(EnergyRollup).count() == 0


async def test_a_work_order_reserves_its_part(session, bus):
    build_plant(session, consecutive=2, on_hand=5.0)
    await _run(session, bus, [14.0, 14.5, 15.0])

    order = session.query(WorkOrder).one()
    assert order.parts_reserved is True
    assert order.status is WorkOrderStatus.SCHEDULED


async def test_a_work_order_with_no_stock_waits_for_parts(session, bus):
    build_plant(session, consecutive=2, on_hand=0.0)
    await _run(session, bus, [14.0, 14.5, 15.0])

    order = session.query(WorkOrder).one()
    assert order.parts_reserved is False
    assert order.status is WorkOrderStatus.AWAITING_PARTS
