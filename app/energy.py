"""Energy consumption accounting.

Power telemetry arrives in kW, which is a rate, so consumption is the integral
over time. Readings land in hourly buckets per asset; each sample contributes
its power over the sampling interval, and the bucket carries a running kWh
total alongside the peak demand that drives demand charges.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Asset, EnergyRollup, utcnow

POWER_METRIC = "power_kw"
DEFAULT_SAMPLE_SECONDS = 1.0


def hour_bucket(moment: datetime) -> datetime:
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def accumulate(
    session: Session,
    *,
    asset_code: str,
    observed_at: datetime,
    kw: float,
    sample_seconds: float = DEFAULT_SAMPLE_SECONDS,
) -> EnergyRollup:
    """Fold one power reading into its hourly bucket."""
    bucket = hour_bucket(observed_at)
    rollup = session.scalar(
        select(EnergyRollup).where(
            EnergyRollup.asset_code == asset_code, EnergyRollup.hour_start == bucket
        )
    )
    if rollup is None:
        rollup = EnergyRollup(asset_code=asset_code, hour_start=bucket)
        session.add(rollup)
        session.flush()

    rollup.kwh += kw * (sample_seconds / 3600.0)
    rollup.sample_count += 1
    rollup.peak_kw = max(rollup.peak_kw, kw)
    rollup.updated_at = utcnow()
    return rollup


def report(
    session: Session,
    *,
    since: datetime | None = None,
    until: datetime | None = None,
    asset_code: str | None = None,
    unit_cost_per_kwh: float = 0.12,
) -> dict:
    """Energy consumption per asset and per line for a window."""
    until = until or utcnow()
    since = since or (until - timedelta(days=7))

    stmt = (
        select(
            EnergyRollup.asset_code,
            func.sum(EnergyRollup.kwh),
            func.max(EnergyRollup.peak_kw),
            func.sum(EnergyRollup.sample_count),
        )
        .where(EnergyRollup.hour_start >= since, EnergyRollup.hour_start <= until)
        .group_by(EnergyRollup.asset_code)
        .order_by(func.sum(EnergyRollup.kwh).desc())
    )
    if asset_code:
        stmt = stmt.where(EnergyRollup.asset_code == asset_code)

    lines = {
        code: line
        for code, line in session.execute(select(Asset.code, Asset.line)).all()
    }

    by_asset = []
    by_line: dict[str, float] = {}
    total_kwh = 0.0
    for code, kwh, peak, samples in session.execute(stmt).all():
        kwh = float(kwh or 0.0)
        total_kwh += kwh
        line = lines.get(code, "unassigned")
        by_line[line] = by_line.get(line, 0.0) + kwh
        by_asset.append(
            {
                "asset_code": code,
                "line": line,
                "kwh": round(kwh, 3),
                "peak_kw": round(float(peak or 0.0), 3),
                "samples": int(samples or 0),
                "estimated_cost": round(kwh * unit_cost_per_kwh, 2),
            }
        )

    hourly = [
        {
            "hour_start": hour.isoformat() if hasattr(hour, "isoformat") else str(hour),
            "kwh": round(float(kwh or 0.0), 3),
        }
        for hour, kwh in session.execute(
            select(EnergyRollup.hour_start, func.sum(EnergyRollup.kwh))
            .where(EnergyRollup.hour_start >= since, EnergyRollup.hour_start <= until)
            .group_by(EnergyRollup.hour_start)
            .order_by(EnergyRollup.hour_start)
        ).all()
    ]

    return {
        "window_start": since.isoformat(),
        "window_end": until.isoformat(),
        "unit_cost_per_kwh": unit_cost_per_kwh,
        "total_kwh": round(total_kwh, 3),
        "estimated_cost": round(total_kwh * unit_cost_per_kwh, 2),
        "by_asset": by_asset,
        "by_line": {
            line: round(value, 3)
            for line, value in sorted(by_line.items(), key=lambda kv: -kv[1])
        },
        "hourly": hourly,
    }
