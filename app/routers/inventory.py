"""Parts inventory and supplier replenishment."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from app import inventory as inventory_service
from app.auth import Principal, current_principal, require_roles
from app.db import get_session
from app.models import Part, ReplenishmentOrder, ReplenishmentStatus, Role
from app.schemas import PartOut, ReplenishmentOut

router = APIRouter(prefix="/api/v1/inventory", tags=["inventory"])


def _part_out(part: Part) -> PartOut:
    return PartOut(
        part_number=part.part_number,
        description=part.description,
        unit_cost=part.unit_cost,
        supplier_code=part.supplier_code,
        on_hand=part.on_hand,
        reserved=part.reserved,
        available=round(part.available, 3),
        reorder_point=part.reorder_point,
        reorder_quantity=part.reorder_quantity,
        below_reorder_point=inventory_service.needs_replenishment(part),
    )


def _order_out(order: ReplenishmentOrder) -> ReplenishmentOut:
    return ReplenishmentOut(
        id=order.id,
        number=order.number,
        part_number=order.part_number,
        supplier_code=order.supplier_code,
        quantity=order.quantity,
        unit_cost=order.unit_cost,
        status=order.status.value,
        created_at=order.created_at,
        expected_at=order.expected_at,
        received_at=order.received_at,
    )


@router.get("/parts", response_model=list[PartOut])
def list_parts(
    short_only: bool = Query(False, description="Only parts at or below the reorder point"),
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> list[PartOut]:
    parts = list(session.scalars(select(Part).order_by(Part.part_number)).all())
    if short_only:
        parts = [p for p in parts if inventory_service.needs_replenishment(p)]
    return [_part_out(part) for part in parts]


@router.get("/parts/{part_number}", response_model=PartOut)
def get_part(
    part_number: str,
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> PartOut:
    part = session.get(Part, part_number)
    if part is None:
        raise HTTPException(404, "part not found")
    return _part_out(part)


@router.get("/replenishments", response_model=list[ReplenishmentOut])
def list_replenishments(
    status: ReplenishmentStatus | None = None,
    open_only: bool = Query(False),
    limit: int = Query(50, ge=1, le=500),
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> list[ReplenishmentOut]:
    stmt = (
        select(ReplenishmentOrder).order_by(ReplenishmentOrder.created_at.desc()).limit(limit)
    )
    if open_only:
        stmt = stmt.where(
            ReplenishmentOrder.status.in_(
                (ReplenishmentStatus.DRAFT, ReplenishmentStatus.ORDERED)
            )
        )
    if status is not None:
        stmt = stmt.where(ReplenishmentOrder.status == status)
    return [_order_out(order) for order in session.scalars(stmt).all()]


@router.post("/parts/{part_number}/replenish", response_model=ReplenishmentOut)
def replenish(
    part_number: str,
    session: Session = Depends(get_session),
    principal: Principal = Depends(require_roles(Role.PLANNER, Role.SUPERVISOR)),
) -> ReplenishmentOut:
    """Raise a replenishment order for a part that has hit its reorder point.

    Idempotent: while an order for the part is open, repeated calls return
    that same order rather than stacking duplicates on the supplier.
    """
    part = session.get(Part, part_number)
    if part is None:
        raise HTTPException(404, "part not found")
    if not inventory_service.needs_replenishment(part):
        raise HTTPException(
            409,
            f"{part_number} is above its reorder point "
            f"(available {part.available:.1f} > {part.reorder_point:.1f})",
        )

    order = inventory_service.maybe_replenish(session, part_number)
    if order is None:
        raise HTTPException(409, "a replenishment for this part is already being raised")
    session.commit()
    return _order_out(order)


@router.post("/replenishments/{order_id}/order", response_model=ReplenishmentOut)
def send_to_supplier(
    order_id: str,
    session: Session = Depends(get_session),
    principal: Principal = Depends(require_roles(Role.PLANNER, Role.SUPERVISOR)),
) -> ReplenishmentOut:
    order = session.get(ReplenishmentOrder, order_id)
    if order is None:
        raise HTTPException(404, "replenishment order not found")
    if order.status is not ReplenishmentStatus.DRAFT:
        raise HTTPException(409, f"order is already {order.status.value}")

    order.status = ReplenishmentStatus.ORDERED
    session.commit()
    return _order_out(order)


@router.post("/replenishments/{order_id}/receive", response_model=ReplenishmentOut)
def receive(
    order_id: str,
    session: Session = Depends(get_session),
    principal: Principal = Depends(require_roles(Role.PLANNER, Role.SUPERVISOR)),
) -> ReplenishmentOut:
    """Book a delivery in, which also closes the part's reorder cycle."""
    order = session.get(ReplenishmentOrder, order_id)
    if order is None:
        raise HTTPException(404, "replenishment order not found")
    if order.status is ReplenishmentStatus.RECEIVED:
        raise HTTPException(409, "order is already received")

    inventory_service.receive(
        session, order, actor=principal.username, actor_role=principal.role
    )
    session.commit()
    return _order_out(order)


@router.get("/suppliers")
def list_suppliers(
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> list[dict]:
    """Suppliers with the count of orders currently outstanding with each."""
    from sqlalchemy import func

    from app.models import Supplier

    outstanding = dict(
        session.execute(
            select(ReplenishmentOrder.supplier_code, func.count())
            .where(
                ReplenishmentOrder.status.in_(
                    (ReplenishmentStatus.DRAFT, ReplenishmentStatus.ORDERED)
                )
            )
            .group_by(ReplenishmentOrder.supplier_code)
        ).all()
    )

    return [
        {
            "code": supplier.code,
            "name": supplier.name,
            "lead_time_days": supplier.lead_time_days,
            "open_orders": outstanding.get(supplier.code, 0),
        }
        for supplier in session.scalars(select(Supplier).order_by(Supplier.code)).all()
    ]
