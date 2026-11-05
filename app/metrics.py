"""Operational KPI aggregation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app import sla
from app.models import (
    Alert,
    AlertStatus,
    EventStatus,
    Part,
    ReplenishmentOrder,
    ReplenishmentStatus,
    Severity,
    TelemetryEvent,
    WorkOrder,
    WorkOrderStatus,
)


def percentile(values: list[float], pct: float) -> float:
    """Nearest-rank percentile, which is what an SLA is measured against."""
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(1, min(len(ordered), int(round(pct / 100.0 * len(ordered) + 0.5))))
    return round(ordered[rank - 1], 3)


def _count(session: Session, model, *where) -> int:
    stmt = select(func.count()).select_from(model)
    for clause in where:
        stmt = stmt.where(clause)
    return session.scalar(stmt) or 0


def event_metrics(session: Session, window_minutes: float = 60.0) -> dict:
    """Ingestion health: completion, throughput and latency."""
    total = _count(session, TelemetryEvent)
    by_status = {
        status.value: _count(session, TelemetryEvent, TelemetryEvent.status == status)
        for status in EventStatus
    }
    processed = by_status[EventStatus.PROCESSED.value]
    dead = by_status[EventStatus.DEAD_LETTER.value]

    latencies = [
        value
        for (value,) in session.execute(
            select(TelemetryEvent.latency_ms).where(TelemetryEvent.latency_ms > 0)
        ).all()
    ]

    span = session.execute(
        select(func.min(TelemetryEvent.received_at), func.max(TelemetryEvent.received_at))
    ).one()
    throughput = 0.0
    if span[0] is not None and span[1] is not None:
        first, last = span
        seconds = (last - first).total_seconds()
        throughput = round(total / seconds, 2) if seconds > 0 else float(total)

    cutoff = datetime.now(UTC) - timedelta(minutes=window_minutes)
    recent = _count(session, TelemetryEvent, TelemetryEvent.received_at >= cutoff)

    return {
        "events_total": total,
        "events_by_status": by_status,
        "delivery_completion_pct": round(processed / total * 100, 4) if total else 100.0,
        "dead_lettered": dead,
        "backlog": total - processed - dead,
        "events_last_window": recent,
        "ingest_throughput_per_sec": throughput,
        "event_latency_ms_p50": percentile(latencies, 50),
        "event_latency_ms_p95": percentile(latencies, 95),
        "event_latency_ms_p99": percentile(latencies, 99),
    }


def alert_metrics(session: Session) -> dict:
    by_status = {
        status.value: _count(session, Alert, Alert.status == status) for status in AlertStatus
    }
    by_severity = {
        severity.value: _count(
            session,
            Alert,
            Alert.severity == severity,
            Alert.status.in_((AlertStatus.OPEN, AlertStatus.ACKNOWLEDGED)),
        )
        for severity in Severity
    }
    latencies = [
        value
        for (value,) in session.execute(
            select(Alert.detection_latency_ms).where(Alert.detection_latency_ms > 0)
        ).all()
    ]
    return {
        "alerts_total": sum(by_status.values()),
        "alerts_by_status": by_status,
        "open_by_severity": by_severity,
        "alert_latency_ms_p50": percentile(latencies, 50),
        "alert_latency_ms_p95": percentile(latencies, 95),
        "alert_latency_ms_p99": percentile(latencies, 99),
    }


def maintenance_metrics(session: Session) -> dict:
    by_status = {
        status.value: _count(session, WorkOrder, WorkOrder.status == status)
        for status in WorkOrderStatus
    }
    total = sum(by_status.values())
    completed = by_status[WorkOrderStatus.COMPLETED.value]

    durations = [
        (completed_at - created_at).total_seconds() / 3600
        for created_at, completed_at in session.execute(
            select(WorkOrder.created_at, WorkOrder.completed_at).where(
                WorkOrder.completed_at.is_not(None)
            )
        ).all()
        if completed_at is not None
    ]

    return {
        "work_orders_total": total,
        "work_orders_by_status": by_status,
        "open_work_orders": total - completed - by_status[WorkOrderStatus.CANCELLED.value],
        "awaiting_parts": by_status[WorkOrderStatus.AWAITING_PARTS.value],
        "completion_rate_pct": round(completed / total * 100, 2) if total else 0.0,
        "mean_time_to_repair_hours": round(sum(durations) / len(durations), 3)
        if durations
        else 0.0,
    }


def inventory_metrics(session: Session) -> dict:
    parts = list(session.scalars(select(Part)).all())
    below = [p for p in parts if p.available <= p.reorder_point]
    open_orders = _count(
        session,
        ReplenishmentOrder,
        ReplenishmentOrder.status.in_(
            (ReplenishmentStatus.DRAFT, ReplenishmentStatus.ORDERED)
        ),
    )
    return {
        "parts_tracked": len(parts),
        "parts_below_reorder_point": len(below),
        "stock_value": round(sum(p.on_hand * p.unit_cost for p in parts), 2),
        "open_replenishment_orders": open_orders,
        "short_parts": [
            {
                "part_number": p.part_number,
                "available": round(p.available, 2),
                "reorder_point": p.reorder_point,
            }
            for p in sorted(below, key=lambda p: p.available)[:10]
        ],
    }


def compute_kpis(session: Session) -> dict:
    breaches = sla.breaches(session)
    return {
        **event_metrics(session),
        **alert_metrics(session),
        **maintenance_metrics(session),
        "inventory": inventory_metrics(session),
        "sla_breaches": len(breaches),
        "sla_breaches_by_kind": {
            kind: sum(1 for b in breaches if b["kind"] == kind)
            for kind in {b["kind"] for b in breaches}
        },
    }
