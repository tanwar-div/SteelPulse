"""Alert deduplication, escalation and suppression."""

from __future__ import annotations

from datetime import timedelta

from conftest import build_plant

from app import alerts as alert_service
from app.models import Alert, AlertStatus, Role, RuleKind, Severity, utcnow
from app.rules import Detection, RuleSpec


def _detection(severity=Severity.HIGH, value=14.0, rule_id="rule-1"):
    spec = RuleSpec(
        id=rule_id,
        asset_code="MILL-01",
        metric="vibration_mm_s",
        kind=RuleKind.THRESHOLD_HIGH,
        threshold=11.0,
        severity=severity,
        consecutive_samples=2,
        part_number="BRG-6208",
        description="Mill vibration above alarm band",
    )
    return Detection(
        rule=spec,
        sensor_tag="MILL-01.vibration_mm_s",
        asset_code="MILL-01",
        metric="vibration_mm_s",
        value=value,
        message="vibration above limit",
    )


def test_the_first_detection_opens_an_alert(session):
    build_plant(session)
    alert, opened = alert_service.raise_alert(session, _detection())

    assert opened is True
    assert alert.status is AlertStatus.OPEN
    assert alert.occurrences == 1
    assert alert.sla_due_at is not None


def test_repeat_detections_fold_into_the_same_alert(session):
    """A flapping sensor must not flood the operator's queue."""
    build_plant(session)
    first, _ = alert_service.raise_alert(session, _detection())
    for _ in range(9):
        again, opened = alert_service.raise_alert(session, _detection())
        assert opened is False
        assert again.id == first.id

    session.commit()
    assert session.query(Alert).count() == 1
    assert session.get(Alert, first.id).occurrences == 10


def test_the_alert_tracks_the_latest_value(session):
    build_plant(session)
    alert, _ = alert_service.raise_alert(session, _detection(value=12.0))
    alert_service.raise_alert(session, _detection(value=19.0))
    assert alert.observed_value == 19.0


def test_a_worsening_condition_escalates_the_alert(session):
    build_plant(session)
    alert, _ = alert_service.raise_alert(session, _detection(severity=Severity.HIGH))
    alert_service.raise_alert(session, _detection(severity=Severity.CRITICAL))

    assert alert.severity is Severity.CRITICAL


def test_severity_never_walks_back_down(session):
    build_plant(session)
    alert, _ = alert_service.raise_alert(session, _detection(severity=Severity.CRITICAL))
    alert_service.raise_alert(session, _detection(severity=Severity.LOW))

    assert alert.severity is Severity.CRITICAL


def test_different_rules_raise_separate_alerts(session):
    build_plant(session)
    alert_service.raise_alert(session, _detection(rule_id="rule-1"))
    alert_service.raise_alert(session, _detection(rule_id="rule-2"))
    session.commit()

    assert session.query(Alert).count() == 2


def test_an_acknowledged_alert_still_absorbs_repeats(session):
    build_plant(session)
    alert, _ = alert_service.raise_alert(session, _detection())
    alert_service.acknowledge(session, alert, actor="op", actor_role=Role.OPERATOR)

    again, opened = alert_service.raise_alert(session, _detection())
    assert opened is False
    assert again.id == alert.id


def test_acknowledging_stops_the_sla_clock(session):
    build_plant(session)
    alert, _ = alert_service.raise_alert(session, _detection())
    alert_service.acknowledge(session, alert, actor="line.operator", actor_role=Role.OPERATOR)

    assert alert.status is AlertStatus.ACKNOWLEDGED
    assert alert.acknowledged_by == "line.operator"
    assert alert.acknowledged_at is not None


def test_a_resolved_alert_is_not_reopened_inside_the_quiet_window(session):
    """Suppression stops one fault re-alerting the moment it is closed."""
    build_plant(session)
    alert, _ = alert_service.raise_alert(session, _detection())
    alert_service.resolve(session, alert, actor="tech", actor_role=Role.TECHNICIAN)
    session.commit()

    again, opened = alert_service.raise_alert(session, _detection())
    assert opened is False
    assert again.id == alert.id
    assert session.query(Alert).count() == 1


def test_the_condition_can_alert_again_once_the_window_passes(session):
    build_plant(session)
    alert, _ = alert_service.raise_alert(session, _detection())
    alert_service.resolve(session, alert, actor="tech", actor_role=Role.TECHNICIAN)
    alert.resolved_at = utcnow() - timedelta(hours=2)
    session.commit()

    _again, opened = alert_service.raise_alert(session, _detection())
    session.commit()
    assert opened is True
    assert session.query(Alert).count() == 2


def test_resolving_clears_the_sla_deadline(session):
    build_plant(session)
    alert, _ = alert_service.raise_alert(session, _detection())
    alert_service.resolve(session, alert, actor="tech", actor_role=Role.TECHNICIAN)

    assert alert.status is AlertStatus.RESOLVED
    assert alert.sla_due_at is None
