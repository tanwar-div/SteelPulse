"""Asset register, sensors and detection rules."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.auth import Principal, current_principal, require_roles
from app.db import get_session
from app.models import Alert, AlertStatus, Asset, Role, Rule, Sensor, WorkOrder, WorkOrderStatus
from app.rules import get_rule_engine
from app.schemas import AssetOut, RuleOut

router = APIRouter(prefix="/api/v1/assets", tags=["assets"])


@router.get("", response_model=list[AssetOut])
def list_assets(
    line: str | None = None,
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> list[AssetOut]:
    stmt = select(Asset).order_by(Asset.code)
    if line:
        stmt = stmt.where(Asset.line == line)
    assets = list(session.scalars(stmt).all())

    sensor_counts = dict(
        session.execute(select(Sensor.asset_id, func.count()).group_by(Sensor.asset_id)).all()
    )
    rule_counts = dict(
        session.execute(select(Rule.asset_id, func.count()).group_by(Rule.asset_id)).all()
    )
    open_alerts = dict(
        session.execute(
            select(Alert.asset_code, func.count())
            .where(Alert.status.in_((AlertStatus.OPEN, AlertStatus.ACKNOWLEDGED)))
            .group_by(Alert.asset_code)
        ).all()
    )
    open_orders = dict(
        session.execute(
            select(WorkOrder.asset_code, func.count())
            .where(
                WorkOrder.status.not_in(
                    (WorkOrderStatus.COMPLETED, WorkOrderStatus.CANCELLED)
                )
            )
            .group_by(WorkOrder.asset_code)
        ).all()
    )

    return [
        AssetOut(
            id=asset.id,
            code=asset.code,
            name=asset.name,
            line=asset.line,
            site=asset.site,
            criticality=asset.criticality.value,
            sensor_count=sensor_counts.get(asset.id, 0),
            rule_count=rule_counts.get(asset.id, 0),
            open_alerts=open_alerts.get(asset.code, 0),
            open_work_orders=open_orders.get(asset.code, 0),
        )
        for asset in assets
    ]


@router.get("/{asset_code}/sensors")
def list_sensors(
    asset_code: str,
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> list[dict]:
    asset = session.scalar(select(Asset).where(Asset.code == asset_code))
    if asset is None:
        raise HTTPException(404, "asset not found")
    return [
        {
            "tag": sensor.tag,
            "metric": sensor.metric,
            "unit": sensor.unit,
            "sample_hz": sensor.sample_hz,
            "last_seen_at": sensor.last_seen_at.isoformat() if sensor.last_seen_at else None,
        }
        for sensor in asset.sensors
    ]


@router.get("/{asset_code}/rules", response_model=list[RuleOut])
def list_rules(
    asset_code: str,
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> list[RuleOut]:
    asset = session.scalar(select(Asset).where(Asset.code == asset_code))
    if asset is None:
        raise HTTPException(404, "asset not found")
    return [
        RuleOut(
            id=rule.id,
            asset_code=asset.code,
            metric=rule.metric,
            kind=rule.kind.value,
            threshold=rule.threshold,
            severity=rule.severity.value,
            consecutive_samples=rule.consecutive_samples,
            enabled=rule.enabled,
            part_number=rule.part_number,
            description=rule.description,
        )
        for rule in asset.rules
    ]


@router.post("/rules/reload")
def reload_rules(
    session: Session = Depends(get_session),
    principal: Principal = Depends(require_roles(Role.SUPERVISOR)),
) -> dict:
    """Re-read rule configuration into the detection engine.

    Also clears the per-sensor evaluation windows, so changed thresholds are
    judged from fresh history rather than against streaks built under the old
    configuration.
    """
    count = get_rule_engine().reload(session)
    return {"rules_loaded": count}
