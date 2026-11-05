"""SLA budgets and breach detection for alerts and work orders."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import Alert, AlertStatus, Severity, WorkOrder, WorkOrderStatus


def alert_budget_minutes(severity: Severity) -> float:
    settings = get_settings()
    return {
        Severity.CRITICAL: settings.sla_minutes_ack_critical,
        Severity.HIGH: settings.sla_minutes_ack_high,
        Severity.MEDIUM: settings.sla_minutes_ack_medium,
    }.get(severity, settings.sla_minutes_ack_medium)


def alert_due_at(severity: Severity, opened_at: datetime | None = None) -> datetime:
    """Deadline for acknowledging an alert of this severity."""
    base = opened_at or datetime.now(UTC)
    if base.tzinfo is None:
        base = base.replace(tzinfo=UTC)
    return base + timedelta(minutes=alert_budget_minutes(severity))


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def alert_breaches(session: Session, now: datetime | None = None) -> list[dict]:
    """Open alerts nobody has acknowledged inside their budget."""
    now = now or datetime.now(UTC)
    rows = session.scalars(
        select(Alert)
        .where(Alert.status == AlertStatus.OPEN, Alert.sla_due_at.is_not(None))
        .order_by(Alert.sla_due_at)
    ).all()

    out = []
    for alert in rows:
        deadline = _aware(alert.sla_due_at)
        if deadline >= now:
            continue
        out.append(
            {
                "kind": "alert_acknowledgement",
                "id": alert.id,
                "asset_code": alert.asset_code,
                "metric": alert.metric,
                "severity": alert.severity.value,
                "message": alert.message,
                "due_at": deadline.isoformat(),
                "minutes_overdue": round((now - deadline).total_seconds() / 60, 2),
            }
        )
    return out


def work_order_breaches(session: Session, now: datetime | None = None) -> list[dict]:
    """Jobs still open past their completion deadline."""
    now = now or datetime.now(UTC)
    rows = session.scalars(
        select(WorkOrder)
        .where(
            WorkOrder.status.not_in((WorkOrderStatus.COMPLETED, WorkOrderStatus.CANCELLED)),
            WorkOrder.sla_due_at.is_not(None),
        )
        .order_by(WorkOrder.sla_due_at)
    ).all()

    out = []
    for order in rows:
        deadline = _aware(order.sla_due_at)
        if deadline >= now:
            continue
        out.append(
            {
                "kind": "work_order_completion",
                "id": order.id,
                "number": order.number,
                "asset_code": order.asset_code,
                "severity": order.severity.value,
                "status": order.status.value,
                "title": order.title,
                "due_at": deadline.isoformat(),
                "hours_overdue": round((now - deadline).total_seconds() / 3600, 2),
            }
        )
    return out


def breaches(session: Session, now: datetime | None = None) -> list[dict]:
    return alert_breaches(session, now) + work_order_breaches(session, now)
