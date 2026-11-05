"""SLA budgets and breach reporting."""

from __future__ import annotations

from datetime import timedelta

from conftest import build_plant

from app import sla
from app import workorders as wo
from app.models import Alert, AlertStatus, Role, RuleKind, Severity, utcnow


def _alert(session, severity=Severity.CRITICAL, status=AlertStatus.OPEN, overdue_minutes=0):
    alert = Alert(
        dedupe_key=f"MILL-01|vibration_mm_s|{severity.value}",
        asset_code="MILL-01",
        sensor_tag="MILL-01.vibration_mm_s",
        metric="vibration_mm_s",
        rule_id="rule-1",
        rule_kind=RuleKind.THRESHOLD_HIGH,
        severity=severity,
        status=status,
        observed_value=14.0,
        threshold=11.0,
        message="vibration above limit",
    )
    alert.sla_due_at = utcnow() - timedelta(minutes=overdue_minutes) if overdue_minutes else (
        utcnow() + timedelta(hours=1)
    )
    session.add(alert)
    session.commit()
    return alert


def test_a_critical_alert_gets_the_tightest_budget():
    critical = sla.alert_budget_minutes(Severity.CRITICAL)
    high = sla.alert_budget_minutes(Severity.HIGH)
    medium = sla.alert_budget_minutes(Severity.MEDIUM)
    assert critical < high < medium


def test_an_alert_inside_its_budget_is_not_a_breach(session):
    build_plant(session)
    _alert(session)
    assert sla.alert_breaches(session) == []


def test_an_unacknowledged_alert_past_its_deadline_breaches(session):
    build_plant(session)
    _alert(session, overdue_minutes=45)

    rows = sla.alert_breaches(session)
    assert len(rows) == 1
    assert rows[0]["kind"] == "alert_acknowledgement"
    assert rows[0]["minutes_overdue"] >= 45


def test_an_acknowledged_alert_is_not_a_breach(session):
    """Somebody owns it, so the acknowledgement clock no longer applies."""
    build_plant(session)
    _alert(session, status=AlertStatus.ACKNOWLEDGED, overdue_minutes=45)
    assert sla.alert_breaches(session) == []


def test_a_work_order_inside_its_budget_is_not_a_breach(plant):
    wo.generate(
        plant,
        asset_code="MILL-01",
        rule_id="rule-1",
        severity=Severity.CRITICAL,
        title="bearing",
    )
    plant.commit()
    assert sla.work_order_breaches(plant) == []


def test_an_overdue_work_order_breaches(plant):
    order, _ = wo.generate(
        plant,
        asset_code="MILL-01",
        rule_id="rule-1",
        severity=Severity.CRITICAL,
        title="bearing",
    )
    order.sla_due_at = utcnow() - timedelta(hours=3)
    plant.commit()

    rows = sla.work_order_breaches(plant)
    assert len(rows) == 1
    assert rows[0]["kind"] == "work_order_completion"
    assert rows[0]["hours_overdue"] >= 3


def test_a_completed_work_order_is_not_a_breach(plant):
    order, _ = wo.generate(
        plant,
        asset_code="MILL-01",
        rule_id="rule-1",
        severity=Severity.CRITICAL,
        title="bearing",
    )
    order.sla_due_at = utcnow() - timedelta(hours=3)
    wo.complete(plant, order, actor="tech", actor_role=Role.TECHNICIAN)
    plant.commit()

    assert sla.work_order_breaches(plant) == []


def test_a_critical_job_is_due_sooner_than_a_routine_one():
    critical = sla.alert_due_at(Severity.CRITICAL)
    medium = sla.alert_due_at(Severity.MEDIUM)
    assert critical < medium


def test_breaches_combines_both_kinds(plant):
    _alert(plant, overdue_minutes=45)
    order, _ = wo.generate(
        plant,
        asset_code="MILL-01",
        rule_id="rule-2",
        severity=Severity.CRITICAL,
        title="bearing",
    )
    order.sla_due_at = utcnow() - timedelta(hours=3)
    plant.commit()

    kinds = {row["kind"] for row in sla.breaches(plant)}
    assert kinds == {"alert_acknowledgement", "work_order_completion"}
