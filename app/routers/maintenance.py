"""Work order queue and lifecycle."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app import audit
from app import workorders as wo
from app.auth import Principal, current_principal, require_roles
from app.db import get_session
from app.models import Role, Severity, WorkOrder, WorkOrderStatus
from app.schemas import AuditEventOut, WorkOrderAssign, WorkOrderClose, WorkOrderOut

router = APIRouter(prefix="/api/v1/work-orders", tags=["maintenance"])


def _to_out(order: WorkOrder) -> WorkOrderOut:
    return WorkOrderOut(
        id=order.id,
        number=order.number,
        asset_code=order.asset_code,
        alert_id=order.alert_id,
        severity=order.severity.value,
        status=order.status.value,
        title=order.title,
        part_number=order.part_number,
        parts_reserved=order.parts_reserved,
        assigned_to=order.assigned_to,
        created_at=order.created_at,
        completed_at=order.completed_at,
        sla_due_at=order.sla_due_at,
        notes=order.notes,
    )


@router.get("", response_model=list[WorkOrderOut])
def list_work_orders(
    status: WorkOrderStatus | None = None,
    severity: Severity | None = None,
    asset_code: str | None = None,
    open_only: bool = Query(False),
    limit: int = Query(50, ge=1, le=500),
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> list[WorkOrderOut]:
    stmt = select(WorkOrder).order_by(WorkOrder.created_at.desc()).limit(limit)
    if open_only:
        stmt = stmt.where(
            WorkOrder.status.not_in((WorkOrderStatus.COMPLETED, WorkOrderStatus.CANCELLED))
        )
    if status is not None:
        stmt = stmt.where(WorkOrder.status == status)
    if severity is not None:
        stmt = stmt.where(WorkOrder.severity == severity)
    if asset_code:
        stmt = stmt.where(WorkOrder.asset_code == asset_code)
    return [_to_out(order) for order in session.scalars(stmt).all()]


@router.get("/{work_order_id}", response_model=WorkOrderOut)
def get_work_order(
    work_order_id: str,
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> WorkOrderOut:
    order = session.get(WorkOrder, work_order_id)
    if order is None:
        raise HTTPException(404, "work order not found")
    return _to_out(order)


@router.get("/{work_order_id}/audit", response_model=list[AuditEventOut])
def work_order_audit(
    work_order_id: str,
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> list[AuditEventOut]:
    if session.get(WorkOrder, work_order_id) is None:
        raise HTTPException(404, "work order not found")
    return [
        AuditEventOut(
            id=e.id,
            subject_type=e.subject_type,
            subject_id=e.subject_id,
            action=e.action,
            actor=e.actor,
            actor_role=e.actor_role.value,
            detail=e.detail,
            created_at=e.created_at,
        )
        for e in audit.trail(session, "work_order", work_order_id)
    ]


@router.post("/{work_order_id}/assign", response_model=WorkOrderOut)
def assign(
    work_order_id: str,
    body: WorkOrderAssign,
    session: Session = Depends(get_session),
    principal: Principal = Depends(require_roles(Role.PLANNER, Role.SUPERVISOR)),
) -> WorkOrderOut:
    order = session.get(WorkOrder, work_order_id)
    if order is None:
        raise HTTPException(404, "work order not found")
    if order.status in (WorkOrderStatus.COMPLETED, WorkOrderStatus.CANCELLED):
        raise HTTPException(409, f"work order is {order.status.value}")

    order.assigned_to = body.assigned_to
    if order.status is WorkOrderStatus.CREATED:
        order.status = WorkOrderStatus.SCHEDULED
    audit.record(
        session,
        action="work_order.assigned",
        actor=principal.username,
        actor_role=principal.role,
        subject_type="work_order",
        subject_id=order.id,
        detail={"assigned_to": body.assigned_to},
    )
    session.commit()
    return _to_out(order)


@router.post("/{work_order_id}/start", response_model=WorkOrderOut)
def start(
    work_order_id: str,
    session: Session = Depends(get_session),
    principal: Principal = Depends(require_roles(Role.TECHNICIAN, Role.SUPERVISOR)),
) -> WorkOrderOut:
    order = session.get(WorkOrder, work_order_id)
    if order is None:
        raise HTTPException(404, "work order not found")
    if order.status is WorkOrderStatus.AWAITING_PARTS:
        raise HTTPException(409, "parts for this job are not available yet")
    if order.status in (WorkOrderStatus.COMPLETED, WorkOrderStatus.CANCELLED):
        raise HTTPException(409, f"work order is {order.status.value}")

    order.status = WorkOrderStatus.IN_PROGRESS
    audit.record(
        session,
        action="work_order.started",
        actor=principal.username,
        actor_role=principal.role,
        subject_type="work_order",
        subject_id=order.id,
        detail={},
    )
    session.commit()
    return _to_out(order)


@router.post("/{work_order_id}/complete", response_model=WorkOrderOut)
def complete(
    work_order_id: str,
    body: WorkOrderClose,
    session: Session = Depends(get_session),
    principal: Principal = Depends(require_roles(Role.TECHNICIAN, Role.SUPERVISOR)),
) -> WorkOrderOut:
    """Close the job out, consume its parts and resolve the alert behind it."""
    order = session.get(WorkOrder, work_order_id)
    if order is None:
        raise HTTPException(404, "work order not found")
    if order.status is WorkOrderStatus.COMPLETED:
        raise HTTPException(409, "work order is already completed")

    wo.complete(
        session, order, actor=principal.username, actor_role=principal.role, notes=body.notes
    )
    session.commit()
    return _to_out(order)


@router.post("/{work_order_id}/cancel", response_model=WorkOrderOut)
def cancel(
    work_order_id: str,
    body: WorkOrderClose,
    session: Session = Depends(get_session),
    principal: Principal = Depends(require_roles(Role.SUPERVISOR)),
) -> WorkOrderOut:
    order = session.get(WorkOrder, work_order_id)
    if order is None:
        raise HTTPException(404, "work order not found")
    if order.status in (WorkOrderStatus.COMPLETED, WorkOrderStatus.CANCELLED):
        raise HTTPException(409, f"work order is already {order.status.value}")

    wo.cancel(
        session, order, actor=principal.username, actor_role=principal.role, reason=body.notes
    )
    session.commit()
    return _to_out(order)
