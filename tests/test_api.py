"""REST API surface and role-based access control."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from conftest import build_plant
from fastapi.testclient import TestClient

from app.db import session_scope
from app.main import app
from app.rules import get_rule_engine

GATEWAY = {"X-API-Key": "gateway-key"}
OPERATOR = {"X-API-Key": "operator-key"}
TECH = {"X-API-Key": "tech-key"}
PLANNER = {"X-API-Key": "planner-key"}
SUPERVISOR = {"X-API-Key": "supervisor-key"}


@pytest.fixture
def client():
    with session_scope() as session:
        build_plant(session, consecutive=2)
        get_rule_engine().load(session)
    with TestClient(app) as test_client:
        yield test_client


def batch(values, metric="vibration_mm_s", start_second=0):
    """Readings one second apart, ending now - the way a gateway sends them."""
    now = datetime.now(UTC)
    return {
        "source": "test",
        "readings": [
            {
                "sensor_tag": f"MILL-01.{metric}",
                "metric": metric,
                "value": float(value),
                "observed_at": (
                    now + timedelta(seconds=start_second + index)
                ).isoformat(),
                "quality": 1.0,
            }
            for index, value in enumerate(values)
        ],
    }


def test_health_reports_dependencies(client):
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert body["event_bus"] in ("memory", "redis")


def test_ingestion_requires_a_key(client):
    assert client.post("/api/v1/telemetry", json=batch([4.0])).status_code == 401


def test_an_operator_role_cannot_do_planner_work(client):
    response = client.post("/api/v1/inventory/parts/BRG-6208/replenish", headers=OPERATOR)
    assert response.status_code == 403


def test_telemetry_is_accepted(client):
    response = client.post("/api/v1/telemetry", json=batch([4.0, 4.5]), headers=GATEWAY)
    assert response.status_code == 202
    body = response.json()
    assert body["accepted"] == 2
    assert body["duplicates"] == 0


def test_resending_telemetry_is_reported_as_duplicate(client):
    payload = batch([4.0, 4.5])
    client.post("/api/v1/telemetry", json=payload, headers=GATEWAY)
    second = client.post("/api/v1/telemetry", json=payload, headers=GATEWAY).json()

    assert second["accepted"] == 0
    assert second["duplicates"] == 2
    assert len(client.get("/api/v1/telemetry/events", headers=OPERATOR).json()) == 2


def test_an_unknown_sensor_is_reported_not_silently_dropped(client):
    payload = {
        "source": "test",
        "readings": [
            {
                "sensor_tag": "GHOST-1.vibration_mm_s",
                "value": 4.0,
                "observed_at": datetime.now(UTC).isoformat(),
            }
        ],
    }
    body = client.post("/api/v1/telemetry", json=payload, headers=GATEWAY).json()
    assert body["rejected"] == 1
    assert body["errors"][0]["error"] == "unknown sensor tag"


def test_an_empty_batch_is_rejected_by_validation(client):
    response = client.post(
        "/api/v1/telemetry", json={"source": "test", "readings": []}, headers=GATEWAY
    )
    assert response.status_code == 422


def test_a_fault_produces_an_alert_and_a_work_order(client):
    """The pipeline runs under the live app, so this is the full path."""
    client.post("/api/v1/telemetry", json=batch([14.0, 14.5, 15.0]), headers=GATEWAY)

    alerts = _poll(client, "/api/v1/alerts?open_only=true", OPERATOR)
    assert len(alerts) == 1
    assert alerts[0]["severity"] == "critical"
    assert alerts[0]["asset_code"] == "MILL-01"

    orders = _poll(client, "/api/v1/work-orders?open_only=true", TECH)
    assert len(orders) == 1
    assert orders[0]["part_number"] == "BRG-6208"


def _poll(client, path, headers, attempts: int = 60):
    """Give the worker pool a moment to drain what was just ingested."""
    import time

    for _ in range(attempts):
        body = client.get(path, headers=headers).json()
        if body:
            return body
        time.sleep(0.05)
    return client.get(path, headers=headers).json()


def test_an_operator_can_acknowledge_an_alert(client):
    client.post("/api/v1/telemetry", json=batch([14.0, 14.5, 15.0]), headers=GATEWAY)
    alert = _poll(client, "/api/v1/alerts?open_only=true", OPERATOR)[0]

    acked = client.post(f"/api/v1/alerts/{alert['id']}/acknowledge", headers=OPERATOR)
    assert acked.status_code == 200
    assert acked.json()["acknowledged_by"] == "line.operator"


def test_an_alert_cannot_be_acknowledged_twice(client):
    client.post("/api/v1/telemetry", json=batch([14.0, 14.5, 15.0]), headers=GATEWAY)
    alert = _poll(client, "/api/v1/alerts?open_only=true", OPERATOR)[0]

    client.post(f"/api/v1/alerts/{alert['id']}/acknowledge", headers=OPERATOR)
    again = client.post(f"/api/v1/alerts/{alert['id']}/acknowledge", headers=OPERATOR)
    assert again.status_code == 409


def test_an_operator_may_not_resolve_an_alert(client):
    client.post("/api/v1/telemetry", json=batch([14.0, 14.5, 15.0]), headers=GATEWAY)
    alert = _poll(client, "/api/v1/alerts?open_only=true", OPERATOR)[0]

    response = client.post(
        f"/api/v1/alerts/{alert['id']}/resolve", json={"note": "done"}, headers=OPERATOR
    )
    assert response.status_code == 403


def test_completing_a_job_resolves_its_alert(client):
    client.post("/api/v1/telemetry", json=batch([14.0, 14.5, 15.0]), headers=GATEWAY)
    order = _poll(client, "/api/v1/work-orders?open_only=true", TECH)[0]

    client.post(f"/api/v1/work-orders/{order['id']}/start", headers=TECH)
    completed = client.post(
        f"/api/v1/work-orders/{order['id']}/complete",
        json={"notes": "bearing replaced"},
        headers=TECH,
    )
    assert completed.status_code == 200
    assert completed.json()["status"] == "completed"

    alert = client.get(f"/api/v1/alerts/{order['alert_id']}", headers=OPERATOR).json()
    assert alert["status"] == "resolved"


def test_completing_a_job_consumes_its_part(client):
    before = client.get("/api/v1/inventory/parts/BRG-6208", headers=PLANNER).json()
    client.post("/api/v1/telemetry", json=batch([14.0, 14.5, 15.0]), headers=GATEWAY)
    order = _poll(client, "/api/v1/work-orders?open_only=true", TECH)[0]

    client.post(
        f"/api/v1/work-orders/{order['id']}/complete", json={"notes": "done"}, headers=TECH
    )
    after = client.get("/api/v1/inventory/parts/BRG-6208", headers=PLANNER).json()

    assert after["on_hand"] == before["on_hand"] - 1
    assert after["reserved"] == before["reserved"]


def test_cancelling_a_job_returns_its_part(client):
    client.post("/api/v1/telemetry", json=batch([14.0, 14.5, 15.0]), headers=GATEWAY)
    order = _poll(client, "/api/v1/work-orders?open_only=true", TECH)[0]
    reserved = client.get("/api/v1/inventory/parts/BRG-6208", headers=PLANNER).json()["reserved"]
    assert reserved == 1

    client.post(
        f"/api/v1/work-orders/{order['id']}/cancel",
        json={"notes": "false alarm"},
        headers=SUPERVISOR,
    )
    after = client.get("/api/v1/inventory/parts/BRG-6208", headers=PLANNER).json()
    assert after["reserved"] == 0


def test_only_a_supervisor_cancels_a_job(client):
    client.post("/api/v1/telemetry", json=batch([14.0, 14.5, 15.0]), headers=GATEWAY)
    order = _poll(client, "/api/v1/work-orders?open_only=true", TECH)[0]

    response = client.post(
        f"/api/v1/work-orders/{order['id']}/cancel", json={"notes": "nope"}, headers=TECH
    )
    assert response.status_code == 403


def test_a_job_has_an_audit_trail(client):
    client.post("/api/v1/telemetry", json=batch([14.0, 14.5, 15.0]), headers=GATEWAY)
    order = _poll(client, "/api/v1/work-orders?open_only=true", TECH)[0]
    client.post(
        f"/api/v1/work-orders/{order['id']}/complete", json={"notes": "done"}, headers=TECH
    )

    trail = client.get(f"/api/v1/work-orders/{order['id']}/audit", headers=SUPERVISOR).json()
    actions = [event["action"] for event in trail]
    assert actions[0] == "work_order.generated"
    assert "work_order.completed" in actions


def test_an_unknown_work_order_is_404(client):
    assert client.get("/api/v1/work-orders/nope", headers=TECH).status_code == 404


def test_replenishment_is_refused_above_the_reorder_point(client):
    response = client.post("/api/v1/inventory/parts/BRG-6208/replenish", headers=PLANNER)
    assert response.status_code == 409


def test_the_asset_register_summarises_each_machine(client):
    assets = client.get("/api/v1/assets", headers=OPERATOR).json()
    assert len(assets) == 1
    assert assets[0]["code"] == "MILL-01"
    assert assets[0]["sensor_count"] == 3
    assert assets[0]["rule_count"] == 2


def test_rules_can_be_listed_and_reloaded(client):
    rules = client.get("/api/v1/assets/MILL-01/rules", headers=OPERATOR).json()
    assert {r["metric"] for r in rules} == {"vibration_mm_s", "temperature_c"}

    reloaded = client.post("/api/v1/assets/rules/reload", headers=SUPERVISOR)
    assert reloaded.status_code == 200
    assert reloaded.json()["rules_loaded"] == 2


def test_only_a_supervisor_reloads_rules(client):
    assert client.post("/api/v1/assets/rules/reload", headers=OPERATOR).status_code == 403


def test_kpis_report_the_plant(client):
    client.post("/api/v1/telemetry", json=batch([4.0, 4.5]), headers=GATEWAY)
    _poll(client, "/api/v1/telemetry/events", OPERATOR)

    kpis = client.get("/api/v1/dashboard/kpis", headers=SUPERVISOR).json()
    assert kpis["events_total"] == 2
    assert kpis["delivery_completion_pct"] >= 0
    assert "inventory" in kpis


def test_energy_is_reported_from_power_telemetry(client):
    client.post(
        "/api/v1/telemetry", json=batch([3600.0, 3600.0], metric="power_kw"), headers=GATEWAY
    )

    import time

    for _ in range(60):
        report = client.get("/api/v1/dashboard/energy?days=7", headers=SUPERVISOR).json()
        if report["by_asset"]:
            break
        time.sleep(0.05)

    assert report["total_kwh"] > 0
    assert report["by_asset"][0]["asset_code"] == "MILL-01"
    assert report["estimated_cost"] > 0


def test_sla_endpoint_reports_a_clean_plant(client):
    body = client.get("/api/v1/dashboard/sla-breaches", headers=SUPERVISOR).json()
    assert body == {"count": 0, "breaches": []}


def test_pipeline_status_shows_the_workers(client):
    body = client.get("/api/v1/ops/pipeline", headers=SUPERVISOR).json()
    assert body["running"] is True
    assert body["worker_count"] >= 1


def test_the_dead_letter_queue_starts_empty(client):
    assert client.get("/api/v1/telemetry/events/dead-letter", headers=SUPERVISOR).json() == []


def test_stuck_events_are_visible(client):
    body = client.get("/api/v1/ops/stuck-events", headers=SUPERVISOR).json()
    assert "count" in body and "events" in body


def test_recovery_can_be_forced(client):
    body = client.post("/api/v1/ops/pipeline/recover", headers=SUPERVISOR).json()
    assert body["claimed"] >= 0


def test_only_a_supervisor_forces_recovery(client):
    assert client.post("/api/v1/ops/pipeline/recover", headers=OPERATOR).status_code == 403


def test_the_dashboard_page_is_served(client):
    response = client.get("/dashboard")
    assert response.status_code == 200
    assert "SteelPulse" in response.text
