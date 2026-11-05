"""Parts reservation and supplier replenishment."""

from __future__ import annotations

from conftest import build_plant

from app import inventory
from app.models import (
    Part,
    ReplenishmentOrder,
    ReplenishmentStatus,
    Role,
    StockMovement,
)

PART = "BRG-6208"


def test_reserving_reduces_availability_not_stock(session):
    build_plant(session, on_hand=10.0)
    assert inventory.reserve(session, PART, 3.0, "WO-1") is True

    part = session.get(Part, PART)
    assert part.on_hand == 10.0
    assert part.reserved == 3.0
    assert part.available == 7.0


def test_a_reservation_beyond_stock_is_refused(session):
    build_plant(session, on_hand=2.0)
    assert inventory.reserve(session, PART, 5.0, "WO-1") is False
    assert session.get(Part, PART).reserved == 0.0


def test_reserving_an_unknown_part_is_refused(session):
    build_plant(session)
    assert inventory.reserve(session, "NO-SUCH-PART", 1.0, "WO-1") is False


def test_releasing_gives_the_reservation_back(session):
    build_plant(session, on_hand=10.0)
    inventory.reserve(session, PART, 3.0, "WO-1")
    inventory.release(session, PART, 3.0, "WO-1")

    part = session.get(Part, PART)
    assert part.reserved == 0.0
    assert part.on_hand == 10.0


def test_issuing_consumes_the_stock(session):
    build_plant(session, on_hand=10.0)
    inventory.reserve(session, PART, 2.0, "WO-1")
    inventory.issue(session, PART, 2.0, "WO-1")

    part = session.get(Part, PART)
    assert part.on_hand == 8.0
    assert part.reserved == 0.0


def test_every_move_is_written_to_the_ledger(session):
    build_plant(session, on_hand=10.0)
    inventory.reserve(session, PART, 2.0, "WO-1")
    inventory.issue(session, PART, 2.0, "WO-1")
    session.commit()

    kinds = [m.kind for m in session.query(StockMovement).order_by(StockMovement.created_at)]
    assert kinds == ["reserve", "issue"]


def test_a_part_above_its_reorder_point_is_not_reordered(session):
    build_plant(session, on_hand=10.0, reorder_point=2.0)
    assert inventory.maybe_replenish(session, PART) is None
    assert session.query(ReplenishmentOrder).count() == 0


def test_hitting_the_reorder_point_raises_an_order(session):
    build_plant(session, on_hand=2.0, reorder_point=2.0)
    order = inventory.maybe_replenish(session, PART)

    assert order is not None
    assert order.part_number == PART
    assert order.quantity == 10.0
    assert order.supplier_code == "SUP-BEAR"
    assert order.status is ReplenishmentStatus.DRAFT
    assert order.expected_at is not None


def test_repeated_checks_do_not_stack_orders_on_the_supplier(session):
    build_plant(session, on_hand=1.0, reorder_point=2.0)
    first = inventory.maybe_replenish(session, PART)
    session.commit()

    for _ in range(25):
        again = inventory.maybe_replenish(session, PART)
        assert again is not None
        assert again.id == first.id

    assert session.query(ReplenishmentOrder).count() == 1


def test_a_delivery_closes_the_cycle_and_restocks(session):
    build_plant(session, on_hand=1.0, reorder_point=2.0)
    order = inventory.maybe_replenish(session, PART)
    session.commit()

    inventory.receive(session, order, actor="planner", actor_role=Role.PLANNER)
    session.commit()

    part = session.get(Part, PART)
    assert part.on_hand == 11.0
    assert order.status is ReplenishmentStatus.RECEIVED
    assert order.received_at is not None


def test_a_part_can_be_reordered_in_the_next_cycle(session):
    """Idempotency is per cycle, not forever - stock runs down again."""
    build_plant(session, on_hand=1.0, reorder_point=2.0)
    first = inventory.maybe_replenish(session, PART)
    session.commit()
    inventory.receive(session, first, actor="planner", actor_role=Role.PLANNER)
    session.commit()

    part = session.get(Part, PART)
    part.on_hand = 1.0
    session.commit()

    second = inventory.maybe_replenish(session, PART)
    assert second is not None
    assert second.id != first.id
    assert session.query(ReplenishmentOrder).count() == 2


def test_receiving_twice_does_not_double_the_stock(session):
    build_plant(session, on_hand=1.0, reorder_point=2.0)
    order = inventory.maybe_replenish(session, PART)
    session.commit()

    inventory.receive(session, order, actor="planner", actor_role=Role.PLANNER)
    inventory.receive(session, order, actor="planner", actor_role=Role.PLANNER)
    session.commit()

    assert session.get(Part, PART).on_hand == 11.0


def test_reservations_count_towards_the_reorder_point(session):
    """Stock promised to a job is not stock you have."""
    build_plant(session, on_hand=6.0, reorder_point=4.0)
    assert inventory.maybe_replenish(session, PART) is None

    inventory.reserve(session, PART, 3.0, "WO-1")
    session.commit()
    assert inventory.maybe_replenish(session, PART) is not None
