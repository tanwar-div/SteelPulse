"""Operational KPIs, SLA monitoring and energy reporting."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app import energy, sla
from app.auth import Principal, current_principal
from app.db import get_session
from app.metrics import compute_kpis

router = APIRouter(prefix="/api/v1/dashboard", tags=["dashboard"])


@router.get("/kpis")
def kpis(
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> dict:
    """Live operational picture: ingestion, alerts, maintenance, inventory."""
    return compute_kpis(session)


@router.get("/sla-breaches")
def sla_breaches(
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> dict:
    """Alerts unacknowledged and work orders unfinished past their deadline."""
    rows = sla.breaches(session)
    return {"count": len(rows), "breaches": rows}


@router.get("/energy")
def energy_report(
    days: int = Query(7, ge=1, le=90),
    asset_code: str | None = None,
    unit_cost_per_kwh: float = Query(0.12, ge=0.0),
    session: Session = Depends(get_session),
    principal: Principal = Depends(current_principal),
) -> dict:
    """Energy consumption per asset and line, integrated from power telemetry."""
    until = datetime.now(UTC)
    return energy.report(
        session,
        since=until - timedelta(days=days),
        until=until,
        asset_code=asset_code,
        unit_cost_per_kwh=unit_cost_per_kwh,
    )
