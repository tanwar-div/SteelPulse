"""Parts inventory and supplier replenishment.

Stock movements are an append-only ledger; the on-hand and reserved figures on
the part are the running totals. Replenishment is idempotent per reorder
cycle: while an order for a part is still open, re-running the check finds the
same key and raises nothing new. Once that order is received the cycle
advances and the part can be ordered again.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app import audit
from app.config import get_settings
from app.idempotency import get_store, replenishment_key
from app.models import (
    Part,
    ProcessedKey,
    ReplenishmentOrder,
    ReplenishmentStatus,
    Role,
    StockMovement,
    Supplier,
    utcnow,
)
from app.sequences import next_number

log = logging.getLogger(__name__)

SYSTEM_ACTOR = "inventory.worker"


def _ledger(session: Session, part_number: str, kind: str, quantity: float, reference: str | None):
    session.add(
        StockMovement(
            part_number=part_number, kind=kind, quantity=quantity, reference=reference
        )
    )


def reserve(session: Session, part_number: str, quantity: float, reference: str) -> bool:
    """Reserve stock for a work order.

    Returns False and reserves nothing when the available quantity will not
    cover the request - the caller then parks the work order on parts.
    """
    part = session.get(Part, part_number)
    if part is None:
        log.warning("reservation for unknown part %s", part_number)
        return False
    if part.available < quantity:
        return False

    part.reserved += quantity
    _ledger(session, part_number, "reserve", quantity, reference)
    return True


def release(session: Session, part_number: str, quantity: float, reference: str) -> None:
    """Give reserved stock back, e.g. when a work order is cancelled."""
    part = session.get(Part, part_number)
    if part is None:
        return
    part.reserved = max(0.0, part.reserved - quantity)
    _ledger(session, part_number, "release", quantity, reference)


def issue(session: Session, part_number: str, quantity: float, reference: str) -> None:
    """Consume reserved stock when the job is done."""
    part = session.get(Part, part_number)
    if part is None:
        return
    part.reserved = max(0.0, part.reserved - quantity)
    part.on_hand = max(0.0, part.on_hand - quantity)
    _ledger(session, part_number, "issue", quantity, reference)


def current_cycle(session: Session, part_number: str) -> str:
    """How many replenishment cycles for this part have already closed.

    Used as the idempotency discriminator, so one open order per part per
    cycle is the invariant.
    """
    closed = (
        session.scalar(
            select(func.count())
            .select_from(ReplenishmentOrder)
            .where(
                ReplenishmentOrder.part_number == part_number,
                ReplenishmentOrder.status.in_(
                    (ReplenishmentStatus.RECEIVED, ReplenishmentStatus.CANCELLED)
                ),
            )
        )
        or 0
    )
    return str(closed)


def needs_replenishment(part: Part) -> bool:
    return part.available <= part.reorder_point


def maybe_replenish(session: Session, part_number: str) -> ReplenishmentOrder | None:
    """Raise a replenishment order if the part has fallen to its reorder point.

    Idempotent: concurrent callers, retries and replays converge on the single
    order for the part's current cycle.
    """
    if not get_settings().auto_replenish:
        return None

    part = session.get(Part, part_number)
    if part is None or not needs_replenishment(part):
        return None

    key = replenishment_key(part_number, current_cycle(session, part_number))

    durable = session.get(ProcessedKey, key)
    if durable is not None and durable.result_ref:
        return session.get(ReplenishmentOrder, durable.result_ref)

    existing = session.scalar(
        select(ReplenishmentOrder).where(ReplenishmentOrder.dedupe_key == key)
    )
    if existing is not None:
        return existing

    claim = get_store().claim(key)
    if not claim.acquired:
        if claim.existing_ref and claim.existing_ref != "pending":
            return session.get(ReplenishmentOrder, claim.existing_ref)
        return None

    supplier = (
        session.scalar(select(Supplier).where(Supplier.code == part.supplier_code))
        if part.supplier_code
        else None
    )
    quantity = part.reorder_quantity or max(1.0, part.reorder_point)
    order = ReplenishmentOrder(
        number=next_number(session, "replenishment", "REP"),
        dedupe_key=key,
        part_number=part_number,
        supplier_code=part.supplier_code,
        quantity=quantity,
        unit_cost=part.unit_cost,
        status=ReplenishmentStatus.DRAFT,
        expected_at=utcnow() + timedelta(days=supplier.lead_time_days if supplier else 7),
    )
    session.add(order)
    session.flush()

    session.merge(ProcessedKey(key=key, scope="replenishment", result_ref=order.id))
    get_store().complete(key, order.id)

    audit.record(
        session,
        action="replenishment.raised",
        actor=SYSTEM_ACTOR,
        actor_role=Role.SYSTEM,
        subject_type="replenishment_order",
        subject_id=order.id,
        detail={
            "part_number": part_number,
            "quantity": quantity,
            "available": part.available,
            "reorder_point": part.reorder_point,
            "supplier": part.supplier_code,
        },
    )
    log.info("replenishment %s raised for %s qty %.1f", order.number, part_number, quantity)
    return order


def receive(session: Session, order: ReplenishmentOrder, *, actor: str, actor_role: Role) -> None:
    """Book a delivery in, which also closes the part's reorder cycle."""
    if order.status is ReplenishmentStatus.RECEIVED:
        return
    part = session.get(Part, order.part_number)
    if part is not None:
        part.on_hand += order.quantity
        _ledger(session, order.part_number, "receive", order.quantity, order.number)

    order.status = ReplenishmentStatus.RECEIVED
    order.received_at = utcnow()
    audit.record(
        session,
        action="replenishment.received",
        actor=actor,
        actor_role=actor_role,
        subject_type="replenishment_order",
        subject_id=order.id,
        detail={"part_number": order.part_number, "quantity": order.quantity},
    )
