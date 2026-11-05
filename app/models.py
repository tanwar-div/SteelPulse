"""SQLAlchemy models: assets, telemetry events, alerts, work orders, inventory."""

from __future__ import annotations

import enum
import uuid
from datetime import UTC, datetime

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    return datetime.now(UTC)


def new_id() -> str:
    return uuid.uuid4().hex


class Base(DeclarativeBase):
    pass


class Severity(str, enum.Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class EventStatus(str, enum.Enum):
    """Lifecycle of one telemetry event through the pipeline."""

    RECEIVED = "received"
    PROCESSED = "processed"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


class AlertStatus(str, enum.Enum):
    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


class WorkOrderStatus(str, enum.Enum):
    CREATED = "created"
    AWAITING_PARTS = "awaiting_parts"
    SCHEDULED = "scheduled"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class ReplenishmentStatus(str, enum.Enum):
    DRAFT = "draft"
    ORDERED = "ordered"
    RECEIVED = "received"
    CANCELLED = "cancelled"


class RuleKind(str, enum.Enum):
    THRESHOLD_HIGH = "threshold_high"
    THRESHOLD_LOW = "threshold_low"
    RATE_OF_CHANGE = "rate_of_change"
    FLATLINE = "flatline"


class Role(str, enum.Enum):
    OPERATOR = "operator"
    TECHNICIAN = "technician"
    PLANNER = "planner"
    SUPERVISOR = "supervisor"
    SYSTEM = "system"


class Asset(Base):
    """A machine on the line."""

    __tablename__ = "assets"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    code: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(128))
    line: Mapped[str] = mapped_column(String(64), index=True)
    site: Mapped[str] = mapped_column(String(64), default="plant-1")
    criticality: Mapped[Severity] = mapped_column(
        Enum(Severity, native_enum=False), default=Severity.MEDIUM
    )
    commissioned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    sensors: Mapped[list[Sensor]] = relationship(
        back_populates="asset", cascade="all, delete-orphan"
    )
    rules: Mapped[list[Rule]] = relationship(back_populates="asset", cascade="all, delete-orphan")


class Sensor(Base):
    __tablename__ = "sensors"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    tag: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    asset_id: Mapped[str] = mapped_column(ForeignKey("assets.id"), index=True)
    metric: Mapped[str] = mapped_column(String(32), index=True)  # temperature_c, power_kw, ...
    unit: Mapped[str] = mapped_column(String(16))
    sample_hz: Mapped[float] = mapped_column(Float, default=1.0)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    asset: Mapped[Asset] = relationship(back_populates="sensors")


class Rule(Base):
    """A detection rule bound to one asset and metric."""

    __tablename__ = "rules"
    __table_args__ = (
        UniqueConstraint("asset_id", "metric", "kind", name="uq_rule_asset_metric_kind"),
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    asset_id: Mapped[str] = mapped_column(ForeignKey("assets.id"), index=True)
    metric: Mapped[str] = mapped_column(String(32), index=True)
    kind: Mapped[RuleKind] = mapped_column(Enum(RuleKind, native_enum=False))
    threshold: Mapped[float] = mapped_column(Float)
    severity: Mapped[Severity] = mapped_column(
        Enum(Severity, native_enum=False), default=Severity.HIGH
    )
    # Consecutive breaching samples required before the rule fires.
    consecutive_samples: Mapped[int] = mapped_column(Integer, default=3)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    part_number: Mapped[str | None] = mapped_column(String(32), nullable=True)
    description: Mapped[str] = mapped_column(String(256), default="")

    asset: Mapped[Asset] = relationship(back_populates="rules")


class TelemetryEvent(Base):
    """Persistent record of every event accepted by the ingestion API.

    This table is what makes event-delivery completion measurable: an event is
    complete when it reaches PROCESSED, and anything stuck in RECEIVED or
    parked in DEAD_LETTER is visible and recoverable.
    """

    __tablename__ = "telemetry_events"
    __table_args__ = (
        Index("ix_events_status_received", "status", "received_at"),
        Index("ix_events_sensor_time", "sensor_tag", "observed_at"),
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    # Deterministic identity of a reading: sensor + observation instant.
    event_key: Mapped[str] = mapped_column(String(80), unique=True, index=True)
    sensor_tag: Mapped[str] = mapped_column(String(64), index=True)
    asset_code: Mapped[str] = mapped_column(String(32), index=True)
    metric: Mapped[str] = mapped_column(String(32))
    value: Mapped[float] = mapped_column(Float)
    quality: Mapped[float] = mapped_column(Float, default=1.0)

    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    status: Mapped[EventStatus] = mapped_column(
        Enum(EventStatus, native_enum=False), default=EventStatus.RECEIVED, index=True
    )
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    stream_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Wall-clock milliseconds from acceptance to terminal state.
    latency_ms: Mapped[float] = mapped_column(Float, default=0.0)


class Alert(Base):
    __tablename__ = "alerts"
    __table_args__ = (Index("ix_alerts_status_opened", "status", "opened_at"),)

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    # asset + metric + rule kind, so a flapping sensor opens one alert, not many.
    dedupe_key: Mapped[str] = mapped_column(String(128), index=True)
    asset_code: Mapped[str] = mapped_column(String(32), index=True)
    sensor_tag: Mapped[str] = mapped_column(String(64))
    metric: Mapped[str] = mapped_column(String(32))
    rule_id: Mapped[str] = mapped_column(String(32))
    rule_kind: Mapped[RuleKind] = mapped_column(Enum(RuleKind, native_enum=False))
    severity: Mapped[Severity] = mapped_column(Enum(Severity, native_enum=False), index=True)
    status: Mapped[AlertStatus] = mapped_column(
        Enum(AlertStatus, native_enum=False), default=AlertStatus.OPEN, index=True
    )

    observed_value: Mapped[float] = mapped_column(Float)
    threshold: Mapped[float] = mapped_column(Float)
    message: Mapped[str] = mapped_column(String(256))
    occurrences: Mapped[int] = mapped_column(Integer, default=1)

    triggering_event_id: Mapped[str | None] = mapped_column(String(32), nullable=True)
    # Milliseconds from the triggering event's observation to this alert.
    detection_latency_ms: Mapped[float] = mapped_column(Float, default=0.0)

    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    acknowledged_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    sla_due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    work_orders: Mapped[list[WorkOrder]] = relationship(back_populates="alert")


class WorkOrder(Base):
    __tablename__ = "work_orders"
    __table_args__ = (
        # At most one OPEN order per maintenance need. A plain unique index
        # would be wrong: a machine that breaks, gets fixed, and breaks again
        # must be able to raise a second job for the same rule.
        Index(
            "uq_work_order_open_need",
            "dedupe_key",
            unique=True,
            sqlite_where=text("status NOT IN ('COMPLETED', 'CANCELLED')"),
            postgresql_where=text("status NOT IN ('COMPLETED', 'CANCELLED')"),
        ),
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    number: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    # Identity of the maintenance need, not of the request - the idempotency anchor.
    dedupe_key: Mapped[str] = mapped_column(String(128), index=True)

    asset_code: Mapped[str] = mapped_column(String(32), index=True)
    alert_id: Mapped[str | None] = mapped_column(ForeignKey("alerts.id"), nullable=True, index=True)
    severity: Mapped[Severity] = mapped_column(Enum(Severity, native_enum=False))
    status: Mapped[WorkOrderStatus] = mapped_column(
        Enum(WorkOrderStatus, native_enum=False), default=WorkOrderStatus.CREATED, index=True
    )
    title: Mapped[str] = mapped_column(String(256))
    part_number: Mapped[str | None] = mapped_column(String(32), nullable=True)
    parts_reserved: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    assigned_to: Mapped[str | None] = mapped_column(String(64), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    sla_due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    alert: Mapped[Alert | None] = relationship(back_populates="work_orders")


class Supplier(Base):
    __tablename__ = "suppliers"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    code: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(128))
    lead_time_days: Mapped[int] = mapped_column(Integer, default=7)


class Part(Base):
    __tablename__ = "parts"

    part_number: Mapped[str] = mapped_column(String(32), primary_key=True)
    description: Mapped[str] = mapped_column(String(256))
    unit_cost: Mapped[float] = mapped_column(Float, default=0.0)
    supplier_code: Mapped[str | None] = mapped_column(String(32), index=True, nullable=True)

    on_hand: Mapped[float] = mapped_column(Float, default=0.0)
    reserved: Mapped[float] = mapped_column(Float, default=0.0)
    reorder_point: Mapped[float] = mapped_column(Float, default=0.0)
    reorder_quantity: Mapped[float] = mapped_column(Float, default=0.0)

    @property
    def available(self) -> float:
        return self.on_hand - self.reserved


class StockMovement(Base):
    """Append-only inventory ledger."""

    __tablename__ = "stock_movements"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    part_number: Mapped[str] = mapped_column(String(32), index=True)
    kind: Mapped[str] = mapped_column(String(24))  # reserve | release | issue | receive
    quantity: Mapped[float] = mapped_column(Float)
    reference: Mapped[str | None] = mapped_column(String(64), index=True, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class ReplenishmentOrder(Base):
    __tablename__ = "replenishment_orders"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    number: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    dedupe_key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    part_number: Mapped[str] = mapped_column(String(32), index=True)
    supplier_code: Mapped[str | None] = mapped_column(String(32), nullable=True)
    quantity: Mapped[float] = mapped_column(Float)
    unit_cost: Mapped[float] = mapped_column(Float, default=0.0)
    status: Mapped[ReplenishmentStatus] = mapped_column(
        Enum(ReplenishmentStatus, native_enum=False), default=ReplenishmentStatus.DRAFT, index=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    expected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class EnergyRollup(Base):
    """Hourly energy consumption per asset, integrated from power telemetry."""

    __tablename__ = "energy_rollups"
    __table_args__ = (
        UniqueConstraint("asset_code", "hour_start", name="uq_energy_asset_hour"),
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    asset_code: Mapped[str] = mapped_column(String(32), index=True)
    hour_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    kwh: Mapped[float] = mapped_column(Float, default=0.0)
    sample_count: Mapped[int] = mapped_column(Integer, default=0)
    peak_kw: Mapped[float] = mapped_column(Float, default=0.0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class AuditEvent(Base):
    """Append-only operator action trail."""

    __tablename__ = "audit_events"

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    subject_type: Mapped[str] = mapped_column(String(32), index=True)
    subject_id: Mapped[str | None] = mapped_column(String(32), index=True, nullable=True)
    action: Mapped[str] = mapped_column(String(64), index=True)
    actor: Mapped[str] = mapped_column(String(64))
    actor_role: Mapped[Role] = mapped_column(Enum(Role, native_enum=False))
    detail: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, index=True
    )


class ProcessedKey(Base):
    """Durable idempotency ledger, mirrored in Redis for hot-path lookups."""

    __tablename__ = "processed_keys"

    key: Mapped[str] = mapped_column(String(160), primary_key=True)
    scope: Mapped[str] = mapped_column(String(32), index=True)
    result_ref: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Counter(Base):
    """Monotonic sequence source for human-facing document numbers."""

    __tablename__ = "counters"

    name: Mapped[str] = mapped_column(String(32), primary_key=True)
    value: Mapped[int] = mapped_column(Integer, default=0)
