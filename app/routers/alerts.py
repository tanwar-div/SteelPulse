"""Alert queue and operator actions."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app import alerts as alert_service
from app.auth import Principal, current_principal, require_roles
from app.db import get_session
from app.models import Alert, AlertStatus, Role, Severity
from app.schemas import AlertAction, AlertOut

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])


def _to_out(alert: Alert) -> AlertOut:
    return AlertOut(
        id=alert.id,
        asset_code=alert.asset_code,
        sensor_tag=alert.sensor_tag,
        metric=alert.metric,
        rule_kind=alert.rule_kind.value,
        severity=alert.severity.value,
        status=alert.status.value,
        observed_value=alert.observed_value,
        threshold=alert.threshold,
        message=alert.message,
        occurrences=alert.occurrences,
        detection_latency_ms=alert.detection_latency_ms,
        opened_at=alert.opened_at,
        acknowledged_at=alert.acknowledged_at,
        acknowledged_by=alert.acknowledged_by,
        resolved_at=alert.resolved_at,
        sla_due_at=alert.sla_due_at,
    )


@router.get("", response_model=list[AlertOut])
def list_alerts(
    status: AlertStatus | None = None,
    severity: Severity | None = None,
    asset_code: str | None = None,
    open_only: bool = Query(False, description="Only alerts nobody has resolved yet"),
    limit: int = Query(50, ge=1, le=500),
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> list[AlertOut]:
    stmt = select(Alert).order_by(Alert.opened_at.desc()).limit(limit)
    if open_only:
        stmt = stmt.where(Alert.status.in_((AlertStatus.OPEN, AlertStatus.ACKNOWLEDGED)))
    if status is not None:
        stmt = stmt.where(Alert.status == status)
    if severity is not None:
        stmt = stmt.where(Alert.severity == severity)
    if asset_code:
        stmt = stmt.where(Alert.asset_code == asset_code)
    return [_to_out(alert) for alert in session.scalars(stmt).all()]


@router.get("/{alert_id}", response_model=AlertOut)
def get_alert(
    alert_id: str,
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> AlertOut:
    alert = session.get(Alert, alert_id)
    if alert is None:
        raise HTTPException(404, "alert not found")
    return _to_out(alert)


@router.post("/{alert_id}/acknowledge", response_model=AlertOut)
def acknowledge(
    alert_id: str,
    session: Session = Depends(get_session),
    principal: Principal = Depends(
        require_roles(Role.OPERATOR, Role.TECHNICIAN, Role.SUPERVISOR)
    ),
) -> AlertOut:
    """Take ownership of an alert, which stops its acknowledgement SLA clock."""
    alert = session.get(Alert, alert_id)
    if alert is None:
        raise HTTPException(404, "alert not found")
    if alert.status is not AlertStatus.OPEN:
        raise HTTPException(409, f"alert is already {alert.status.value}")

    alert_service.acknowledge(session, alert, actor=principal.username, actor_role=principal.role)
    session.commit()
    return _to_out(alert)


@router.post("/{alert_id}/resolve", response_model=AlertOut)
def resolve(
    alert_id: str,
    body: AlertAction,
    session: Session = Depends(get_session),
    principal: Principal = Depends(
        require_roles(Role.TECHNICIAN, Role.SUPERVISOR)
    ),
) -> AlertOut:
    alert = session.get(Alert, alert_id)
    if alert is None:
        raise HTTPException(404, "alert not found")
    if alert.status is AlertStatus.RESOLVED:
        raise HTTPException(409, "alert is already resolved")

    alert_service.resolve(
        session, alert, actor=principal.username, actor_role=principal.role, note=body.note
    )
    session.commit()
    return _to_out(alert)
