"""Telemetry ingestion."""

from __future__ import annotations

from conftest import BASE_TIME, build_plant, readings

from app.models import EventStatus, Sensor, TelemetryEvent
from app.pipeline.ingest import Reading, ingest_batch


async def test_accepted_readings_become_durable_events(session, bus):
    build_plant(session)
    result = await ingest_batch(session, readings([4.0, 4.5]), bus)

    assert (result.accepted, result.duplicates, result.rejected) == (2, 0, 0)
    assert session.query(TelemetryEvent).count() == 2
    assert all(
        event.status is EventStatus.RECEIVED for event in session.query(TelemetryEvent).all()
    )


async def test_accepted_readings_are_published(session, bus):
    build_plant(session)
    await ingest_batch(session, readings([4.0, 4.5, 5.0]), bus)
    assert await bus.depth() == 3


async def test_an_unknown_sensor_tag_is_rejected(session, bus):
    build_plant(session)
    result = await ingest_batch(
        session, readings([4.0], sensor_tag="GHOST-99.vibration_mm_s"), bus
    )

    assert (result.accepted, result.rejected) == (0, 1)
    assert result.errors[0]["error"] == "unknown sensor tag"
    assert session.query(TelemetryEvent).count() == 0


async def test_a_mixed_batch_takes_the_good_readings(session, bus):
    build_plant(session)
    batch = readings([4.0, 4.5]) + readings([9.0], sensor_tag="GHOST-99.vibration_mm_s")
    result = await ingest_batch(session, batch, bus)

    assert (result.accepted, result.rejected) == (2, 1)


async def test_resending_a_batch_takes_each_reading_once(session, bus):
    """A gateway retrying after a timeout must not double-count."""
    build_plant(session)
    batch = readings([4.0, 4.5, 5.0])

    first = await ingest_batch(session, batch, bus)
    second = await ingest_batch(session, batch, bus)

    assert (first.accepted, first.duplicates) == (3, 0)
    assert (second.accepted, second.duplicates) == (0, 3)
    assert session.query(TelemetryEvent).count() == 3


async def test_a_duplicate_inside_one_batch_is_collapsed(session, bus):
    build_plant(session)
    one = readings([4.0])
    result = await ingest_batch(session, one + one, bus)

    assert (result.accepted, result.duplicates) == (1, 1)


async def test_the_same_instant_on_different_sensors_is_not_a_duplicate(session, bus):
    build_plant(session)
    batch = [
        Reading("MILL-01.vibration_mm_s", "vibration_mm_s", 4.0, BASE_TIME),
        Reading("MILL-01.power_kw", "power_kw", 2400.0, BASE_TIME),
    ]
    result = await ingest_batch(session, batch, bus)
    assert result.accepted == 2


async def test_an_empty_batch_is_a_no_op(session, bus):
    build_plant(session)
    result = await ingest_batch(session, [], bus)
    assert result.total == 0
    assert await bus.depth() == 0


async def test_ingestion_records_sensor_liveness(session, bus):
    """Last-seen is what makes an offline sensor detectable."""
    build_plant(session)
    await ingest_batch(session, readings([4.0]), bus)

    sensor = session.query(Sensor).filter_by(tag="MILL-01.vibration_mm_s").one()
    assert sensor.last_seen_at is not None


async def test_the_event_carries_its_asset_and_metric(session, bus):
    build_plant(session)
    await ingest_batch(session, readings([7.5]), bus)

    event = session.query(TelemetryEvent).one()
    assert event.asset_code == "MILL-01"
    assert event.metric == "vibration_mm_s"
    assert event.value == 7.5
