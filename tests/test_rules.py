"""Rule-based anomaly detection."""

from __future__ import annotations

from conftest import build_plant
from sqlalchemy import select

from app.models import Asset, Rule, RuleKind, Severity
from app.rules import get_rule_engine


def _engine(session):
    engine = get_rule_engine()
    engine.load(session)
    return engine


def _feed(engine, values, metric="vibration_mm_s", quality=1.0):
    detections = []
    for value in values:
        detections.append(
            engine.evaluate(
                sensor_tag=f"MILL-01.{metric}",
                asset_code="MILL-01",
                metric=metric,
                value=float(value),
                quality=quality,
            )
        )
    return detections


def test_a_single_breach_does_not_fire(session):
    build_plant(session, consecutive=3)
    engine = _engine(session)
    assert _feed(engine, [14.0]) == [[]]


def test_firing_needs_the_configured_consecutive_breaches(session):
    build_plant(session, consecutive=3)
    engine = _engine(session)
    results = _feed(engine, [14.0, 14.2, 14.5])
    assert results[0] == [] and results[1] == []
    assert len(results[2]) == 1
    assert results[2][0].severity is Severity.CRITICAL


def test_a_normal_reading_resets_the_streak(session):
    build_plant(session, consecutive=3)
    engine = _engine(session)
    results = _feed(engine, [14.0, 14.2, 4.0, 14.0, 14.1])
    assert all(r == [] for r in results)


def test_readings_inside_the_limit_never_fire(session):
    build_plant(session, consecutive=2)
    engine = _engine(session)
    assert all(r == [] for r in _feed(engine, [4.0, 5.5, 6.1, 3.9]))


def test_a_bad_quality_reading_is_not_evidence(session):
    """A gateway-flagged bad read must not raise an alert or build a streak."""
    build_plant(session, consecutive=2)
    engine = _engine(session)
    assert _feed(engine, [99.0, 99.0], quality=0.1) == [[], []]


def test_threshold_low_fires_below_the_limit(session):
    asset = build_plant(session)
    session.add(
        Rule(
            asset_id=asset.id,
            metric="pressure_bar",
            kind=RuleKind.THRESHOLD_LOW,
            threshold=120.0,
            severity=Severity.CRITICAL,
            consecutive_samples=1,
            description="pressure collapsed",
        )
    )
    session.commit()
    engine = _engine(session)

    assert _feed(engine, [160.0], metric="pressure_bar") == [[]]
    assert len(_feed(engine, [90.0], metric="pressure_bar")[0]) == 1


def test_rate_of_change_fires_on_a_step(session):
    asset = build_plant(session)
    session.add(
        Rule(
            asset_id=asset.id,
            metric="current_a",
            kind=RuleKind.RATE_OF_CHANGE,
            threshold=50.0,
            severity=Severity.HIGH,
            consecutive_samples=1,
            description="step change",
        )
    )
    session.commit()
    engine = _engine(session)

    results = _feed(engine, [100.0, 110.0, 200.0], metric="current_a")
    assert results[0] == []  # no previous sample to compare against
    assert results[1] == []  # a 10-unit move is inside the limit
    assert len(results[2]) == 1


def test_flatline_fires_on_a_stuck_sensor(session):
    asset = build_plant(session)
    session.add(
        Rule(
            asset_id=asset.id,
            metric="flow_lpm",
            kind=RuleKind.FLATLINE,
            threshold=0.05,
            severity=Severity.HIGH,
            consecutive_samples=1,
            description="sensor stuck",
        )
    )
    session.commit()
    engine = _engine(session)

    results = _feed(engine, [42.0] * 12, metric="flow_lpm")
    assert all(r == [] for r in results[:11])
    assert len(results[11]) == 1
    assert "flat" in results[11][0].message


def test_a_moving_signal_is_not_a_flatline(session):
    asset = build_plant(session)
    session.add(
        Rule(
            asset_id=asset.id,
            metric="flow_lpm",
            kind=RuleKind.FLATLINE,
            threshold=0.05,
            severity=Severity.HIGH,
            consecutive_samples=1,
            description="sensor stuck",
        )
    )
    session.commit()
    engine = _engine(session)

    values = [42.0 + (i % 3) * 0.5 for i in range(20)]
    assert all(r == [] for r in _feed(engine, values, metric="flow_lpm"))


def test_disabled_rules_are_not_loaded(session):
    build_plant(session, consecutive=1)
    rule = session.scalars(
        select(Rule).where(Rule.metric == "vibration_mm_s")
    ).one()
    rule.enabled = False
    session.commit()

    engine = _engine(session)
    assert engine.rules_for("MILL-01", "vibration_mm_s") == []


def test_a_sensor_with_no_rules_is_cheap_to_evaluate(session):
    build_plant(session)
    engine = _engine(session)
    assert engine.evaluate(
        sensor_tag="MILL-01.power_kw", asset_code="MILL-01", metric="power_kw", value=99999.0
    ) == []


def test_reload_picks_up_a_changed_threshold(session):
    build_plant(session, vibration_limit=11.0, consecutive=1)
    engine = _engine(session)
    assert len(_feed(engine, [12.0])[0]) == 1

    rule = session.scalars(
        select(Rule).where(Rule.metric == "vibration_mm_s")
    ).one()
    rule.threshold = 20.0
    session.commit()
    engine.reload(session)

    assert _feed(engine, [12.0]) == [[]]


def test_rules_are_scoped_to_their_own_asset(session):
    build_plant(session, consecutive=1)
    other = Asset(code="MILL-02", name="Second mill", line="hot-mill")
    session.add(other)
    session.commit()
    engine = _engine(session)

    assert engine.evaluate(
        sensor_tag="MILL-02.vibration_mm_s",
        asset_code="MILL-02",
        metric="vibration_mm_s",
        value=50.0,
    ) == []
