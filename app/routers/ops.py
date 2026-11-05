"""Pipeline operations: status, fault recovery, dead-letter replay."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import Principal, current_principal, require_roles
from app.bus import get_bus
from app.config import get_settings
from app.db import get_session
from app.models import EventStatus, Role, TelemetryEvent
from app.pipeline.workers import get_pool
from app.schemas import PipelineStatus

router = APIRouter(prefix="/api/v1/ops", tags=["ops"])


@router.get("/pipeline", response_model=PipelineStatus)
async def pipeline_status(
    principal: Principal = Depends(current_principal),
) -> PipelineStatus:
    """Worker pool health, stream depth and unacked message count."""
    bus = await get_bus()
    pool = get_pool()
    return PipelineStatus(
        running=pool.running,
        bus=bus.name,
        worker_count=get_settings().worker_count,
        stream_depth=await bus.depth(),
        pending_messages=await bus.pending_count(),
        stats=dict(pool.stats),
    )


@router.post("/pipeline/recover")
async def recover_stale(
    min_idle_ms: int = Query(0, ge=0, description="Claim entries idle at least this long"),
    principal: Principal = Depends(require_roles(Role.SUPERVISOR)),
) -> dict:
    """Force a recovery sweep instead of waiting for the periodic one.

    Claims messages whose consumer stopped acking and runs them again, which
    is how a killed worker's in-flight events get finished.
    """
    bus = await get_bus()
    pool = get_pool()
    claimed = await bus.claim_stale("manual-recovery", min_idle_ms, get_settings().batch_size)
    if claimed:
        await pool.handle(claimed, "manual-recovery")
    return {"claimed": len(claimed), "pending_after": await bus.pending_count()}


@router.post("/events/{event_id}/replay")
async def replay_event(
    event_id: str,
    session: Session = Depends(get_session),
    principal: Principal = Depends(require_roles(Role.SUPERVISOR)),
) -> dict:
    """Re-publish a dead-lettered event after the underlying fault is fixed."""
    event = session.get(TelemetryEvent, event_id)
    if event is None:
        raise HTTPException(404, "event not found")
    if event.status is not EventStatus.DEAD_LETTER:
        raise HTTPException(409, f"event is {event.status.value}, not dead-lettered")

    event.status = EventStatus.RECEIVED
    event.attempts = 0
    event.last_error = None
    session.commit()

    bus = await get_bus()
    stream_id = await bus.publish(
        {
            "event_key": event.event_key,
            "event_id": event.id,
            "sensor_tag": event.sensor_tag,
            "asset_code": event.asset_code,
            "metric": event.metric,
            "value": event.value,
            "quality": event.quality,
            "observed_at": event.observed_at.isoformat(),
            "received_at": event.received_at.isoformat(),
            "source": "replay",
        }
    )
    event.stream_id = stream_id
    session.commit()
    return {"event_id": event.id, "republished_as": stream_id}


@router.get("/stuck-events")
def stuck_events(
    limit: int = Query(50, ge=1, le=500),
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> dict:
    """Events accepted but never completed - the delivery-completion shortfall."""
    rows = session.scalars(
        select(TelemetryEvent)
        .where(TelemetryEvent.status.in_((EventStatus.RECEIVED, EventStatus.FAILED)))
        .order_by(TelemetryEvent.received_at)
        .limit(limit)
    ).all()
    return {
        "count": len(rows),
        "events": [
            {
                "id": event.id,
                "sensor_tag": event.sensor_tag,
                "status": event.status.value,
                "attempts": event.attempts,
                "last_error": event.last_error,
                "received_at": event.received_at.isoformat(),
            }
            for event in rows
        ],
    }
