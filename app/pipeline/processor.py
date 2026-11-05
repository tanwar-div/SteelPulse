"""Per-message processing: detect, alert, raise work, account for energy.

Handling one message is idempotent end to end. The event row is the record of
record: a message whose event is already PROCESSED is acked and dropped, which
is what makes at-least-once delivery safe to build on. Within a message,
work-order generation is keyed independently, so even a redelivery that slips
past the status check cannot produce a second maintenance order.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app import alerts, energy
from app import workorders as wo
from app.bus import Message
from app.config import get_settings
from app.models import EventStatus, Severity, TelemetryEvent, utcnow
from app.retry import PermanentError, TransientError
from app.rules import RuleEngine

log = logging.getLogger(__name__)


class ProcessOutcome:
    __slots__ = ("event_id", "alerts_opened", "work_orders_created", "skipped", "latency_ms")

    def __init__(self) -> None:
        self.event_id: str | None = None
        self.alerts_opened = 0
        self.work_orders_created = 0
        self.skipped = False
        self.latency_ms = 0.0


def _aware(moment: datetime) -> datetime:
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)


def _parse(value: str) -> datetime:
    return _aware(datetime.fromisoformat(value))


async def process_message(
    session: Session, message: Message, engine: RuleEngine
) -> ProcessOutcome:
    """Process one telemetry message. Raises TransientError to request a retry."""
    outcome = ProcessOutcome()
    payload = message.payload
    settings = get_settings()

    event = session.scalar(
        select(TelemetryEvent).where(TelemetryEvent.event_key == payload.get("event_key", ""))
    )
    if event is None:
        # The bus is ahead of the database, which happens if a publish raced a
        # rollback. Nothing durable to work from, so do not retry forever.
        raise PermanentError(f"no event row for {payload.get('event_key')!r}")

    outcome.event_id = event.id

    if event.status is EventStatus.PROCESSED:
        outcome.skipped = True
        return outcome

    event.attempts += 1
    if event.attempts > settings.max_delivery_attempts:
        event.status = EventStatus.DEAD_LETTER
        event.last_error = "max delivery attempts exceeded"
        session.commit()
        raise PermanentError(f"event {event.id} exceeded {settings.max_delivery_attempts} attempts")

    try:
        detections = engine.evaluate(
            sensor_tag=event.sensor_tag,
            asset_code=event.asset_code,
            metric=event.metric,
            value=event.value,
            quality=event.quality,
        )

        now = utcnow()
        received_at = _aware(event.received_at)
        latency_ms = (now - received_at).total_seconds() * 1000

        for detection in detections:
            alert, opened = alerts.raise_alert(
                session, detection, event_id=event.id, detection_latency_ms=latency_ms
            )
            if opened:
                outcome.alerts_opened += 1

            if detection.severity.value in settings.auto_work_order_severities:
                order, created = wo.generate(
                    session,
                    asset_code=detection.asset_code,
                    rule_id=detection.rule.id,
                    severity=_work_order_severity(detection.severity),
                    title=detection.message or detection.rule.description,
                    alert_id=alert.id,
                    part_number=detection.rule.part_number,
                )
                if created and order is not None:
                    outcome.work_orders_created += 1

        if event.metric == energy.POWER_METRIC:
            energy.accumulate(
                session,
                asset_code=event.asset_code,
                observed_at=_aware(event.observed_at),
                kw=event.value,
            )

        event.status = EventStatus.PROCESSED
        event.processed_at = now
        event.latency_ms = round(latency_ms, 3)
        event.last_error = None
        outcome.latency_ms = event.latency_ms
        session.commit()
        return outcome

    except PermanentError:
        session.rollback()
        _mark_failed(session, event.id, "permanent failure", EventStatus.DEAD_LETTER)
        raise
    except Exception as exc:
        session.rollback()
        _mark_failed(session, event.id, f"{exc.__class__.__name__}: {exc}", EventStatus.FAILED)
        raise TransientError(str(exc)) from exc


def _mark_failed(session: Session, event_id: str, error: str, status: EventStatus) -> None:
    """Record the failure without losing it to the rollback that preceded it."""
    event = session.get(TelemetryEvent, event_id)
    if event is None:
        return
    event.status = status
    event.last_error = error[:1000]
    session.commit()


def _work_order_severity(severity: Severity) -> Severity:
    return severity
