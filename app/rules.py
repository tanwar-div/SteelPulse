"""Rule-based anomaly detection.

Rules are evaluated against a short in-memory window per sensor, so detection
costs no database round trip on the hot path. Four kinds are supported:

    threshold_high   value above the limit for N consecutive samples
    threshold_low    value below the limit for N consecutive samples
    rate_of_change   absolute change between samples beyond the limit
    flatline         a sensor that has stopped moving, which usually means the
                     sensor has failed rather than the process being stable

Requiring N consecutive samples is what keeps a single noisy reading from
raising an alert; the window is bounded so memory stays flat under load.
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from threading import Lock

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import get_settings
from app.models import Rule, RuleKind, Severity

log = logging.getLogger(__name__)

WINDOW_SIZE = 32


@dataclass(frozen=True)
class RuleSpec:
    """A rule flattened out of the database for hot-path evaluation."""

    id: str
    asset_code: str
    metric: str
    kind: RuleKind
    threshold: float
    severity: Severity
    consecutive_samples: int
    part_number: str | None
    description: str


@dataclass
class Detection:
    rule: RuleSpec
    sensor_tag: str
    asset_code: str
    metric: str
    value: float
    message: str

    @property
    def severity(self) -> Severity:
        return self.rule.severity


class _SensorWindow:
    __slots__ = ("values", "breach_streaks")

    def __init__(self) -> None:
        self.values: deque[float] = deque(maxlen=WINDOW_SIZE)
        self.breach_streaks: dict[str, int] = defaultdict(int)


class RuleEngine:
    """Evaluates telemetry against the configured rules.

    Rules are loaded once and cached by asset code; call ``reload`` after
    changing rule configuration.
    """

    def __init__(self) -> None:
        self._rules: dict[tuple[str, str], list[RuleSpec]] = {}
        self._windows: dict[str, _SensorWindow] = {}
        self._lock = Lock()
        self._loaded = False

    def load(self, session: Session) -> int:
        """Load enabled rules, keyed by (asset code, metric)."""
        from app.models import Asset

        rows = session.execute(
            select(Rule, Asset.code).join(Asset, Asset.id == Rule.asset_id).where(Rule.enabled)
        ).all()

        table: dict[tuple[str, str], list[RuleSpec]] = defaultdict(list)
        for rule, asset_code in rows:
            table[(asset_code, rule.metric)].append(
                RuleSpec(
                    id=rule.id,
                    asset_code=asset_code,
                    metric=rule.metric,
                    kind=rule.kind,
                    threshold=rule.threshold,
                    severity=rule.severity,
                    consecutive_samples=max(1, rule.consecutive_samples),
                    part_number=rule.part_number,
                    description=rule.description,
                )
            )

        with self._lock:
            self._rules = dict(table)
            self._loaded = True
        log.info("rule engine loaded %d rules over %d asset/metric pairs", len(rows), len(table))
        return len(rows)

    def reload(self, session: Session) -> int:
        self.clear_windows()
        return self.load(session)

    @property
    def loaded(self) -> bool:
        return self._loaded

    def clear_windows(self) -> None:
        with self._lock:
            self._windows.clear()

    def rules_for(self, asset_code: str, metric: str) -> list[RuleSpec]:
        return self._rules.get((asset_code, metric), [])

    def evaluate(
        self, *, sensor_tag: str, asset_code: str, metric: str, value: float, quality: float = 1.0
    ) -> list[Detection]:
        """Score one reading against every rule bound to its asset and metric."""
        specs = self.rules_for(asset_code, metric)
        if not specs:
            return []

        # A reading the gateway already flagged as untrustworthy is not evidence
        # of a process problem, so it updates no streaks and raises nothing.
        if quality < 0.5:
            return []

        with self._lock:
            window = self._windows.get(sensor_tag)
            if window is None:
                window = self._windows[sensor_tag] = _SensorWindow()
            previous = window.values[-1] if window.values else None
            window.values.append(value)
            history = list(window.values)
            streaks = window.breach_streaks

            detections: list[Detection] = []
            for spec in specs:
                breached, message = self._test(spec, value, previous, history)
                if breached:
                    streaks[spec.id] += 1
                    if streaks[spec.id] >= spec.consecutive_samples:
                        detections.append(
                            Detection(
                                rule=spec,
                                sensor_tag=sensor_tag,
                                asset_code=asset_code,
                                metric=metric,
                                value=value,
                                message=message,
                            )
                        )
                else:
                    streaks[spec.id] = 0
        return detections

    def _test(
        self, spec: RuleSpec, value: float, previous: float | None, history: list[float]
    ) -> tuple[bool, str]:
        if spec.kind is RuleKind.THRESHOLD_HIGH:
            if value > spec.threshold:
                return True, f"{spec.metric} {value:.2f} above limit {spec.threshold:.2f}"
            return False, ""

        if spec.kind is RuleKind.THRESHOLD_LOW:
            if value < spec.threshold:
                return True, f"{spec.metric} {value:.2f} below limit {spec.threshold:.2f}"
            return False, ""

        if spec.kind is RuleKind.RATE_OF_CHANGE:
            if previous is None:
                return False, ""
            delta = abs(value - previous)
            if delta > spec.threshold:
                return True, (
                    f"{spec.metric} moved {delta:.2f} in one sample "
                    f"(limit {spec.threshold:.2f})"
                )
            return False, ""

        if spec.kind is RuleKind.FLATLINE:
            needed = get_settings().flatline_samples
            if len(history) < needed:
                return False, ""
            recent = history[-needed:]
            if max(recent) - min(recent) <= spec.threshold:
                return True, f"{spec.metric} flat across {needed} samples - suspect sensor fault"
            return False, ""

        return False, ""


_engine: RuleEngine | None = None


def get_rule_engine() -> RuleEngine:
    global _engine
    if _engine is None:
        _engine = RuleEngine()
    return _engine


def reset_rule_engine() -> None:
    """Test helper - drops the cached engine and its windows."""
    global _engine
    _engine = None
