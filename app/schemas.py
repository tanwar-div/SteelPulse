"""Request and response models for the REST API."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class ReadingIn(BaseModel):
    sensor_tag: str = Field(..., max_length=64)
    value: float
    observed_at: datetime
    metric: str | None = Field(None, max_length=32)
    quality: float = Field(1.0, ge=0.0, le=1.0)


class TelemetryBatch(BaseModel):
    """A batch of readings from one gateway."""

    source: str = Field("gateway", max_length=32)
    readings: list[ReadingIn] = Field(..., min_length=1, max_length=5000)


class IngestResponse(BaseModel):
    accepted: int
    duplicates: int
    rejected: int
    errors: list[dict] = Field(default_factory=list)
    ingest_ms: float


class EventOut(BaseModel):
    id: str
    event_key: str
    sensor_tag: str
    asset_code: str
    metric: str
    value: float
    quality: float
    status: str
    attempts: int
    last_error: str | None
    observed_at: datetime
    received_at: datetime
    processed_at: datetime | None
    latency_ms: float


class AlertOut(BaseModel):
    id: str
    asset_code: str
    sensor_tag: str
    metric: str
    rule_kind: str
    severity: str
    status: str
    observed_value: float
    threshold: float
    message: str
    occurrences: int
    detection_latency_ms: float
    opened_at: datetime
    acknowledged_at: datetime | None
    acknowledged_by: str | None
    resolved_at: datetime | None
    sla_due_at: datetime | None


class AlertAction(BaseModel):
    note: str | None = Field(None, max_length=1000)


class WorkOrderOut(BaseModel):
    id: str
    number: str
    asset_code: str
    alert_id: str | None
    severity: str
    status: str
    title: str
    part_number: str | None
    parts_reserved: bool
    assigned_to: str | None
    created_at: datetime
    completed_at: datetime | None
    sla_due_at: datetime | None
    notes: str | None


class WorkOrderAssign(BaseModel):
    assigned_to: str = Field(..., max_length=64)


class WorkOrderClose(BaseModel):
    notes: str | None = Field(None, max_length=2000)


class PartOut(BaseModel):
    part_number: str
    description: str
    unit_cost: float
    supplier_code: str | None
    on_hand: float
    reserved: float
    available: float
    reorder_point: float
    reorder_quantity: float
    below_reorder_point: bool


class ReplenishmentOut(BaseModel):
    id: str
    number: str
    part_number: str
    supplier_code: str | None
    quantity: float
    unit_cost: float
    status: str
    created_at: datetime
    expected_at: datetime | None
    received_at: datetime | None


class AssetOut(BaseModel):
    id: str
    code: str
    name: str
    line: str
    site: str
    criticality: str
    sensor_count: int
    rule_count: int
    open_alerts: int
    open_work_orders: int


class RuleOut(BaseModel):
    id: str
    asset_code: str
    metric: str
    kind: str
    threshold: float
    severity: str
    consecutive_samples: int
    enabled: bool
    part_number: str | None
    description: str


class AuditEventOut(BaseModel):
    id: str
    subject_type: str
    subject_id: str | None
    action: str
    actor: str
    actor_role: str
    detail: dict
    created_at: datetime


class PipelineStatus(BaseModel):
    running: bool
    bus: str
    worker_count: int
    stream_depth: int
    pending_messages: int
    stats: dict
