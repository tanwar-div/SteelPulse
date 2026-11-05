"""Maintenance work orders.

A work order is created from an alert, and its identity is the maintenance
need - asset plus rule - not the alert or the event that surfaced it. A sensor
breaching the same rule a thousand times describes one job, so a thousand
events produce one work order. That identity is claimed before the insert and
recorded in the processed-key table, so retries, concurrent workers and
replayed events all converge on the same order.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from app import audit, inventory
from app.config import get_settings
from app.idempotency import get_store, work_order_key
from app.models import (
    Alert,
    Part,
    ProcessedKey,
    Role,
    Severity,
    WorkOrder,
    WorkOrderStatus,
    utcnow,
)
from app.sequences import next_number

log = logging.getLogger(__name__)

SYSTEM_ACTOR = "maintenance.worker"

PARTS_PER_JOB = 1.0


def sla_due(severity: Severity):
    settings = get_settings()
    hours = {
        Severity.CRITICAL: settings.sla_hours_work_order_critical,
        Severity.HIGH: settings.sla_hours_work_order_high,
    }.get(severity, settings.sla_hours_work_order_normal)
    return utcnow() + timedelta(hours=hours)


def find_open(session: Session, dedupe_key: str) -> WorkOrder | None:
    """An existing order for this need that has not been closed out."""
    return session.scalar(
        select(WorkOrder).where(
            WorkOrder.dedupe_key == dedupe_key,
            WorkOrder.status.not_in((WorkOrderStatus.COMPLETED, WorkOrderStatus.CANCELLED)),
        )
    )


def generate(
    session: Session,
    *,
    asset_code: str,
    rule_id: str,
    severity: Severity,
    title: str,
    alert_id: str | None = None,
    part_number: str | None = None,
    actor: str = SYSTEM_ACTOR,
    actor_role: Role = Role.SYSTEM,
) -> tuple[WorkOrder | None, bool]:
    """Create the work order for a maintenance need, exactly once.

    Returns (work_order, created). ``created`` is False when an order for this
    need already existed, whether from a previous event, a concurrent worker
    or a replayed message.
    """
    key = work_order_key(asset_code, rule_id)

    # Durable ledger first: it outlives the process and the Redis cache, so a
    # worker that created an order and died before committing cannot create a
    # second one after restart.
    durable = session.get(ProcessedKey, key)
    if durable is not None and durable.result_ref:
        existing = session.get(WorkOrder, durable.result_ref)
        if existing is not None and existing.status not in (
            WorkOrderStatus.COMPLETED,
            WorkOrderStatus.CANCELLED,
        ):
            return existing, False
        # The previous order for this need is closed, so the need may recur.
        session.delete(durable)
        session.flush()
        get_store().release(key)

    open_order = find_open(session, key)
    if open_order is not None:
        return open_order, False

    claim = get_store().claim(key)
    if not claim.acquired:
        if claim.existing_ref and claim.existing_ref != "pending":
            return session.get(WorkOrder, claim.existing_ref), False
        # Another worker is mid-create; it will finish the job.
        return None, False

    order = WorkOrder(
        number=next_number(session, "work_order", "WO"),
        dedupe_key=key,
        asset_code=asset_code,
        alert_id=alert_id,
        severity=severity,
        status=WorkOrderStatus.CREATED,
        title=title,
        part_number=part_number,
        sla_due_at=sla_due(severity),
    )
    session.add(order)
    session.flush()

    if part_number:
        _secure_parts(session, order, part_number)

    session.merge(ProcessedKey(key=key, scope="work_order", result_ref=order.id))
    get_store().complete(key, order.id)

    audit.record(
        session,
        action="work_order.generated",
        actor=actor,
        actor_role=actor_role,
        subject_type="work_order",
        subject_id=order.id,
        detail={
            "asset_code": asset_code,
            "rule_id": rule_id,
            "severity": severity.value,
            "alert_id": alert_id,
            "dedupe_key": key,
        },
    )
    log.info("work order %s generated for %s (%s)", order.number, asset_code, severity.value)
    return order, True


def _secure_parts(session: Session, order: WorkOrder, part_number: str) -> None:
    """Reserve the part the job needs, and reorder if that empties the bin."""
    if inventory.reserve(session, part_number, PARTS_PER_JOB, order.number):
        order.parts_reserved = True
        order.status = WorkOrderStatus.SCHEDULED
    else:
        order.parts_reserved = False
        order.status = WorkOrderStatus.AWAITING_PARTS
        audit.record(
            session,
            action="work_order.awaiting_parts",
            actor=SYSTEM_ACTOR,
            actor_role=Role.SYSTEM,
            subject_type="work_order",
            subject_id=order.id,
            detail={"part_number": part_number},
        )

    part = session.get(Part, part_number)
    if part is not None and inventory.needs_replenishment(part):
        inventory.maybe_replenish(session, part_number)


def complete(
    session: Session, order: WorkOrder, *, actor: str, actor_role: Role, notes: str | None = None
) -> WorkOrder:
    """Close a job out, consuming any reserved parts."""
    if order.status is WorkOrderStatus.COMPLETED:
        return order

    if order.parts_reserved and order.part_number:
        inventory.issue(session, order.part_number, PARTS_PER_JOB, order.number)
        order.parts_reserved = False

    order.status = WorkOrderStatus.COMPLETED
    order.completed_at = utcnow()
    order.notes = notes
    order.sla_due_at = None

    # Closing the order also closes the alert that raised it.
    if order.alert_id:
        from app.models import AlertStatus

        alert = session.get(Alert, order.alert_id)
        if alert is not None and alert.status is not AlertStatus.RESOLVED:
            alert.status = AlertStatus.RESOLVED
            alert.resolved_at = utcnow()
            alert.sla_due_at = None

    audit.record(
        session,
        action="work_order.completed",
        actor=actor,
        actor_role=actor_role,
        subject_type="work_order",
        subject_id=order.id,
        detail={"notes": notes},
    )
    return order


def cancel(
    session: Session, order: WorkOrder, *, actor: str, actor_role: Role, reason: str | None = None
) -> WorkOrder:
    """Cancel a job, returning any reserved parts to available stock."""
    if order.status in (WorkOrderStatus.COMPLETED, WorkOrderStatus.CANCELLED):
        return order

    if order.parts_reserved and order.part_number:
        inventory.release(session, order.part_number, PARTS_PER_JOB, order.number)
        order.parts_reserved = False

    order.status = WorkOrderStatus.CANCELLED
    order.sla_due_at = None
    order.notes = reason
    audit.record(
        session,
        action="work_order.cancelled",
        actor=actor,
        actor_role=actor_role,
        subject_type="work_order",
        subject_id=order.id,
        detail={"reason": reason},
    )
    return order
