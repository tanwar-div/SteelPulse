"""Test fixtures: an isolated database, an in-process bus, and a seeded plant."""

from __future__ import annotations

import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ["DATABASE_URL"] = "sqlite+pysqlite:///./test_steelpulse.db"
os.environ["DATABASE_FALLBACK_URL"] = "sqlite+pysqlite:///./test_steelpulse.db"
# Hermetic by construction: the suite must behave the same whether or not the
# developer happens to have Redis or a broker running locally.
os.environ["REDIS_URL"] = "redis://127.0.0.1:1/0"
os.environ["BUS_BACKEND"] = "memory"
os.environ["WORKER_COUNT"] = "2"
os.environ["BLOCK_MS"] = "10"
os.environ["RETRY_BASE_DELAY"] = "0.001"
os.environ["ALERT_SUPPRESSION_SECONDS"] = "300"

import pytest  # noqa: E402

from app.bus import MemoryBus, set_bus  # noqa: E402
from app.db import reset_db, session_scope  # noqa: E402
from app.idempotency import get_store  # noqa: E402
from app.models import (  # noqa: E402
    Asset,
    Part,
    Rule,
    RuleKind,
    Sensor,
    Severity,
    Supplier,
)
from app.pipeline.ingest import Reading  # noqa: E402
from app.rules import get_rule_engine, reset_rule_engine  # noqa: E402

BASE_TIME = datetime(2026, 4, 1, 9, 0, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def clean_state():
    reset_db()
    get_store().clear()
    reset_rule_engine()
    set_bus(MemoryBus())
    yield
    set_bus(None)
    reset_rule_engine()


@pytest.fixture
def bus():
    instance = MemoryBus()
    set_bus(instance)
    return instance


@pytest.fixture
def session():
    with session_scope() as handle:
        yield handle


@pytest.fixture
def plant(session):
    """A one-asset plant: a mill with vibration and power, and two rules."""
    build_plant(session)
    get_rule_engine().load(session)
    return session


def build_plant(
    session,
    *,
    asset_code="MILL-01",
    vibration_limit=11.0,
    consecutive=2,
    part_number="BRG-6208",
    on_hand=5.0,
    reorder_point=2.0,
):
    session.add(Supplier(code="SUP-BEAR", name="Nordic Bearing Works", lead_time_days=5))
    session.add(
        Part(
            part_number=part_number,
            description="Deep groove ball bearing",
            unit_cost=42.5,
            supplier_code="SUP-BEAR",
            on_hand=on_hand,
            reorder_point=reorder_point,
            reorder_quantity=10.0,
        )
    )

    asset = Asset(
        code=asset_code, name="Roughing mill", line="hot-mill", criticality=Severity.CRITICAL
    )
    session.add(asset)
    session.flush()

    for metric, unit in (("vibration_mm_s", "mm/s"), ("power_kw", "kW"), ("temperature_c", "degC")):
        session.add(
            Sensor(tag=f"{asset_code}.{metric}", asset_id=asset.id, metric=metric, unit=unit)
        )

    session.add(
        Rule(
            asset_id=asset.id,
            metric="vibration_mm_s",
            kind=RuleKind.THRESHOLD_HIGH,
            threshold=vibration_limit,
            severity=Severity.CRITICAL,
            consecutive_samples=consecutive,
            part_number=part_number,
            description="Mill vibration above alarm band",
        )
    )
    session.add(
        Rule(
            asset_id=asset.id,
            metric="temperature_c",
            kind=RuleKind.THRESHOLD_HIGH,
            threshold=95.0,
            severity=Severity.MEDIUM,
            consecutive_samples=1,
            part_number=None,
            description="Bearing housing hot",
        )
    )
    session.commit()
    return asset


def readings(
    values,
    *,
    sensor_tag="MILL-01.vibration_mm_s",
    metric="vibration_mm_s",
    start=BASE_TIME,
    step_seconds=1,
    quality=1.0,
):
    """Build a run of readings one second apart."""
    return [
        Reading(
            sensor_tag=sensor_tag,
            metric=metric,
            value=float(value),
            observed_at=start + timedelta(seconds=index * step_seconds),
            quality=quality,
        )
        for index, value in enumerate(values)
    ]
