"""Alert lifecycle.

A detection does not always mean a new alert. A sensor breaching the same rule
repeatedly produces one alert whose occurrence count climbs, which is what
keeps an operator's queue readable during an actual fault. A new alert for the
same key is only opened once the previous one has been resolved and the
suppression window has passed.
"""

from __future__ import annotations

import logging
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from app import audit, sla
from app.config import get_settings
from app.models import Alert, AlertStatus, Role, Severity, utcnow
from app.rules import Detection

log = logging.getLogger(__name__)

SYSTEM_ACTOR = "detector.worker"


def dedupe_key(asset_code: str, metric: str, rule_id: str) -> str:
    return f"{asset_code}|{metric}|{rule_id}"


def find_active(session: Session, key: str) -> Alert | None:
    return session.scalar(
        select(Alert)
        .where(
            Alert.dedupe_key == key,
            Alert.status.in_((AlertStatus.OPEN, AlertStatus.ACKNOWLEDGED)),
        )
        .order_by(Alert.opened_at.desc())
    )


def raise_alert(
    session: Session,
    detection: Detection,
    *,
    event_id: str | None = None,
    detection_latency_ms: float = 0.0,
) -> tuple[Alert, bool]:
    """Open an alert for a detection, or fold it into the active one.

    Returns (alert, opened). ``opened`` is False when an active alert already
    covered this condition.
    """
    key = dedupe_key(detection.asset_code, detection.metric, detection.rule.id)
    active = find_active(session, key)

    if active is not None:
        active.occurrences += 1
        active.observed_value = detection.value
        # An escalating condition raises the alert's severity but never lowers it.
        if _rank(detection.severity) > _rank(active.severity):
            active.severity = detection.severity
            active.sla_due_at = sla.alert_due_at(detection.severity, active.opened_at)
        return active, False

    recent = session.scalar(
        select(Alert)
        .where(Alert.dedupe_key == key, Alert.status == AlertStatus.RESOLVED)
        .order_by(Alert.resolved_at.desc())
    )
    window = timedelta(seconds=get_settings().alert_suppression_seconds)
    if recent is not None and recent.resolved_at is not None:
        resolved_at = recent.resolved_at
        if resolved_at.tzinfo is None:
            resolved_at = resolved_at.replace(tzinfo=utcnow().tzinfo)
        if utcnow() - resolved_at < window:
            log.debug("suppressing re-alert for %s inside the quiet window", key)
            recent.occurrences += 1
            return recent, False

    alert = Alert(
        dedupe_key=key,
        asset_code=detection.asset_code,
        sensor_tag=detection.sensor_tag,
        metric=detection.metric,
        rule_id=detection.rule.id,
        rule_kind=detection.rule.kind,
        severity=detection.severity,
        status=AlertStatus.OPEN,
        observed_value=detection.value,
        threshold=detection.rule.threshold,
        message=detection.message,
        triggering_event_id=event_id,
        detection_latency_ms=round(detection_latency_ms, 3),
    )
    alert.sla_due_at = sla.alert_due_at(detection.severity, alert.opened_at)
    session.add(alert)
    session.flush()
    return alert, True


def acknowledge(session: Session, alert: Alert, *, actor: str, actor_role: Role) -> Alert:
    if alert.status is not AlertStatus.OPEN:
        return alert
    alert.status = AlertStatus.ACKNOWLEDGED
    alert.acknowledged_at = utcnow()
    alert.acknowledged_by = actor
    audit.record(
        session,
        action="alert.acknowledged",
        actor=actor,
        actor_role=actor_role,
        subject_type="alert",
        subject_id=alert.id,
        detail={"asset_code": alert.asset_code, "severity": alert.severity.value},
    )
    return alert


def resolve(
    session: Session, alert: Alert, *, actor: str, actor_role: Role, note: str | None = None
) -> Alert:
    if alert.status is AlertStatus.RESOLVED:
        return alert
    alert.status = AlertStatus.RESOLVED
    alert.resolved_at = utcnow()
    alert.sla_due_at = None
    audit.record(
        session,
        action="alert.resolved",
        actor=actor,
        actor_role=actor_role,
        subject_type="alert",
        subject_id=alert.id,
        detail={"note": note},
    )
    return alert


_RANK = {Severity.LOW: 0, Severity.MEDIUM: 1, Severity.HIGH: 2, Severity.CRITICAL: 3}


def _rank(severity: Severity) -> int:
    return _RANK.get(severity, 0)
