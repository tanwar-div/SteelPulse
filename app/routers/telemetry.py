"""Telemetry ingestion and event inspection."""

from __future__ import annotations

import time

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import Principal, current_principal, require_roles
from app.bus import get_bus
from app.db import get_session
from app.models import EventStatus, Role, TelemetryEvent
from app.pipeline.ingest import Reading, ingest_batch
from app.schemas import EventOut, IngestResponse, TelemetryBatch

router = APIRouter(prefix="/api/v1/telemetry", tags=["telemetry"])


def _to_out(event: TelemetryEvent) -> EventOut:
    return EventOut(
        id=event.id,
        event_key=event.event_key,
        sensor_tag=event.sensor_tag,
        asset_code=event.asset_code,
        metric=event.metric,
        value=event.value,
        quality=event.quality,
        status=event.status.value,
        attempts=event.attempts,
        last_error=event.last_error,
        observed_at=event.observed_at,
        received_at=event.received_at,
        processed_at=event.processed_at,
        latency_ms=event.latency_ms,
    )


@router.post("", response_model=IngestResponse, status_code=202)
async def ingest(
    batch: TelemetryBatch,
    session: Session = Depends(get_session),
    principal: Principal = Depends(require_roles(Role.SYSTEM, Role.OPERATOR, Role.SUPERVISOR)),
) -> IngestResponse:
    """Accept a batch of sensor readings.

    Acceptance persists each reading and publishes it to the bus; detection
    and downstream work happen in the worker pool. Safe to retry: readings
    already taken are reported as duplicates and processed only once.
    """
    started = time.perf_counter()
    bus = await get_bus()
    readings = [
        Reading(
            sensor_tag=item.sensor_tag,
            metric=item.metric or "",
            value=item.value,
            observed_at=item.observed_at,
            quality=item.quality,
        )
        for item in batch.readings
    ]
    result = await ingest_batch(session, readings, bus, source=batch.source)
    return IngestResponse(
        accepted=result.accepted,
        duplicates=result.duplicates,
        rejected=result.rejected,
        errors=result.errors[:20],
        ingest_ms=round((time.perf_counter() - started) * 1000, 3),
    )


@router.get("/events", response_model=list[EventOut])
def list_events(
    status: EventStatus | None = None,
    sensor_tag: str | None = None,
    asset_code: str | None = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> list[EventOut]:
    stmt = (
        select(TelemetryEvent)
        .order_by(TelemetryEvent.received_at.desc())
        .limit(limit)
        .offset(offset)
    )
    if status is not None:
        stmt = stmt.where(TelemetryEvent.status == status)
    if sensor_tag:
        stmt = stmt.where(TelemetryEvent.sensor_tag == sensor_tag)
    if asset_code:
        stmt = stmt.where(TelemetryEvent.asset_code == asset_code)
    return [_to_out(event) for event in session.scalars(stmt).all()]


@router.get("/events/dead-letter", response_model=list[EventOut])
def dead_letter_queue(
    limit: int = Query(100, ge=1, le=500),
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> list[EventOut]:
    """Events the pipeline gave up on, kept for inspection and replay."""
    stmt = (
        select(TelemetryEvent)
        .where(TelemetryEvent.status == EventStatus.DEAD_LETTER)
        .order_by(TelemetryEvent.received_at.desc())
        .limit(limit)
    )
    return [_to_out(event) for event in session.scalars(stmt).all()]


@router.get("/events/{event_id}", response_model=EventOut)
def get_event(
    event_id: str,
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> EventOut:
    event = session.get(TelemetryEvent, event_id)
    if event is None:
        raise HTTPException(404, "event not found")
    return _to_out(event)
