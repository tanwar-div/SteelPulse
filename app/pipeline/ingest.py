"""Telemetry ingestion.

Acceptance is deliberately thin: validate, persist a durable record of every
reading, publish to the bus, return. Detection, maintenance and inventory work
all happen downstream in the worker pool, so a burst of sensor traffic is
bounded by an insert and an append rather than by the slowest consumer.

Ingestion is idempotent on (sensor, observation instant). A gateway that
retries a batch after a timeout re-sends the same readings; those are
recognised and dropped rather than double-counted.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.bus import EventBus
from app.idempotency import event_key
from app.models import EventStatus, Sensor, TelemetryEvent, utcnow

log = logging.getLogger(__name__)


@dataclass
class Reading:
    """One sensor observation as it arrives from a gateway."""

    sensor_tag: str
    metric: str
    value: float
    observed_at: datetime
    asset_code: str | None = None
    quality: float = 1.0


@dataclass
class IngestResult:
    accepted: int = 0
    duplicates: int = 0
    rejected: int = 0
    event_ids: list[str] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.accepted + self.duplicates + self.rejected


def _aware(moment: datetime) -> datetime:
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def _sensor_index(session: Session, tags: set[str]) -> dict[str, tuple[str, str]]:
    """Map sensor tag -> (asset code, metric) for the tags in this batch."""
    from app.models import Asset

    rows = session.execute(
        select(Sensor.tag, Asset.code, Sensor.metric)
        .join(Asset, Asset.id == Sensor.asset_id)
        .where(Sensor.tag.in_(tags))
    ).all()
    return {tag: (asset_code, metric) for tag, asset_code, metric in rows}


async def ingest_batch(
    session: Session, readings: list[Reading], bus: EventBus, *, source: str = "gateway"
) -> IngestResult:
    """Accept a batch of readings: persist, publish, acknowledge."""
    result = IngestResult()
    if not readings:
        return result

    index = _sensor_index(session, {r.sensor_tag for r in readings})

    candidates: list[tuple[str, Reading, str, str]] = []
    seen_in_batch: set[str] = set()
    for reading in readings:
        known = index.get(reading.sensor_tag)
        if known is None:
            result.rejected += 1
            result.errors.append(
                {"sensor_tag": reading.sensor_tag, "error": "unknown sensor tag"}
            )
            continue

        asset_code, metric = known
        observed_at = _aware(reading.observed_at)
        key = event_key(reading.sensor_tag, observed_at.isoformat())
        if key in seen_in_batch:
            result.duplicates += 1
            continue
        seen_in_batch.add(key)
        candidates.append((key, reading, asset_code, reading.metric or metric))

    if not candidates:
        return result

    # One query settles which of these readings the system has already taken.
    known_keys = set(
        session.scalars(
            select(TelemetryEvent.event_key).where(
                TelemetryEvent.event_key.in_([key for key, _, _, _ in candidates])
            )
        ).all()
    )

    received_at = utcnow()
    rows: list[TelemetryEvent] = []
    payloads: list[dict] = []
    for key, reading, asset_code, metric in candidates:
        if key in known_keys:
            result.duplicates += 1
            continue

        observed_at = _aware(reading.observed_at)
        event = TelemetryEvent(
            event_key=key,
            sensor_tag=reading.sensor_tag,
            asset_code=asset_code,
            metric=metric,
            value=reading.value,
            quality=reading.quality,
            observed_at=observed_at,
            received_at=received_at,
            status=EventStatus.RECEIVED,
        )
        rows.append(event)
        payloads.append(
            {
                "event_key": key,
                "sensor_tag": reading.sensor_tag,
                "asset_code": asset_code,
                "metric": metric,
                "value": reading.value,
                "quality": reading.quality,
                "observed_at": observed_at.isoformat(),
                "received_at": received_at.isoformat(),
                "source": source,
            }
        )

    if not rows:
        return result

    session.add_all(rows)
    session.flush()

    for event, payload in zip(rows, payloads, strict=True):
        payload["event_id"] = event.id

    # Persist before publishing: an event on the bus must already be a durable
    # record, or a crash between the two would lose it with no way to notice.
    session.commit()

    stream_ids = await bus.publish_many(payloads)
    for event, stream_id in zip(rows, stream_ids, strict=True):
        event.stream_id = stream_id
    session.commit()

    _touch_sensors(session, {r.sensor_tag for _, r, _, _ in candidates}, received_at)
    session.commit()

    result.accepted = len(rows)
    result.event_ids = [event.id for event in rows]
    return result


def _touch_sensors(session: Session, tags: set[str], moment: datetime) -> None:
    """Record liveness so an offline sensor is detectable."""
    from sqlalchemy import update

    session.execute(update(Sensor).where(Sensor.tag.in_(tags)).values(last_seen_at=moment))
