#!/usr/bin/env python3
"""Simulated IoT telemetry source.

Generates readings for every seeded sensor at a target rate, with each metric
following a plausible profile for its equipment: process noise around a
setpoint, slow drift, and injected faults - a bearing degrading, a hydraulic
seal letting go, a thermocouple sticking - so the detection rules have
something real to find.

Used as a library by the load harness, and runnable on its own to push
telemetry at a live API:

    python scripts/simulate_sensors.py --rate 250 --seconds 60 --post
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Metric profile: setpoint, noise sigma, hard floor, hard ceiling
PROFILES = {
    "temperature_c": (0.0, 1.0, -50.0, 2000.0),
    "vibration_mm_s": (4.5, 0.6, 0.0, 60.0),
    "pressure_bar": (160.0, 4.0, 0.0, 400.0),
    "flow_lpm": (220.0, 8.0, 0.0, 900.0),
    "current_a": (240.0, 9.0, 0.0, 900.0),
    "power_kw": (0.0, 0.0, 0.0, 10000.0),
}

# Per-asset setpoints where the generic profile is not representative.
ASSET_SETPOINTS = {
    ("FURN-01", "temperature_c"): 1180.0,
    ("FURN-01", "power_kw"): 3600.0,
    ("FURN-01", "pressure_bar"): 1.2,
    ("MILL-01", "temperature_c"): 72.0,
    ("MILL-01", "power_kw"): 2400.0,
    ("HYD-01", "temperature_c"): 54.0,
    ("HYD-01", "pressure_bar"): 168.0,
    ("HYD-01", "flow_lpm"): 72.0,
    ("COOL-01", "temperature_c"): 38.0,
    ("COOL-01", "flow_lpm"): 240.0,
    ("COOL-01", "power_kw"): 180.0,
    ("DRV-01", "temperature_c"): 62.0,
    ("DRV-01", "current_a"): 245.0,
    ("DRV-01", "power_kw"): 140.0,
}

NOISE = {
    "temperature_c": 2.5,
    "vibration_mm_s": 0.6,
    "pressure_bar": 3.0,
    "flow_lpm": 6.0,
    "current_a": 8.0,
    "power_kw": 45.0,
}

FAULTS = ("bearing_wear", "pressure_loss", "overheat", "sensor_stuck", "flow_starvation")


@dataclass
class SensorSim:
    tag: str
    asset_code: str
    metric: str
    setpoint: float
    sigma: float
    phase: float = 0.0
    fault: str | None = None
    fault_progress: float = 0.0
    stuck_value: float | None = None
    samples: int = 0

    def next_value(self) -> tuple[float, float]:
        """Produce the next reading and its quality flag."""
        self.samples += 1
        self.phase += 0.015

        if self.fault == "sensor_stuck" and self.stuck_value is not None:
            return self.stuck_value, 1.0

        # Shift-length cycle plus gaussian process noise.
        base = self.setpoint * (1 + 0.012 * math.sin(self.phase))
        value = random.gauss(base, self.sigma)

        if self.fault:
            value += self._fault_offset()

        low, high = PROFILES.get(self.metric, (0, 0, -1e9, 1e9))[2:]
        value = max(low, min(high, value))

        # Occasional bad read from the gateway, which must not raise an alert.
        quality = 0.2 if random.random() < 0.0008 else 1.0
        return round(value, 3), quality

    def _fault_offset(self) -> float:
        """How far the fault has pushed this metric from its setpoint."""
        self.fault_progress = min(1.0, self.fault_progress + 0.02)
        scale = self.fault_progress

        if self.fault == "bearing_wear" and self.metric == "vibration_mm_s":
            return scale * 11.0
        if self.fault == "bearing_wear" and self.metric == "temperature_c":
            return scale * 32.0
        if self.fault == "pressure_loss" and self.metric == "pressure_bar":
            return -scale * 70.0
        if self.fault == "overheat" and self.metric == "temperature_c":
            return scale * (160.0 if self.setpoint > 500 else 28.0)
        if self.fault == "flow_starvation" and self.metric == "flow_lpm":
            return -scale * 90.0
        if self.fault == "overheat" and self.metric == "current_a":
            return scale * 85.0
        return 0.0

    def inject(self, fault: str) -> None:
        self.fault = fault
        self.fault_progress = 0.0
        if fault == "sensor_stuck":
            self.stuck_value = round(self.setpoint, 3)

    def clear_fault(self) -> None:
        self.fault = None
        self.fault_progress = 0.0
        self.stuck_value = None


@dataclass
class PlantSimulator:
    """Drives every sensor in the plant and injects faults over time."""

    sensors: list[SensorSim] = field(default_factory=list)
    fault_probability: float = 0.0015
    rng: random.Random = field(default_factory=random.Random)

    @classmethod
    def from_database(cls, fault_probability: float = 0.0015, seed: int | None = None):
        from sqlalchemy import select

        from app.db import session_scope
        from app.models import Asset, Sensor

        sims: list[SensorSim] = []
        with session_scope() as session:
            rows = session.execute(
                select(Sensor.tag, Asset.code, Sensor.metric).join(
                    Asset, Asset.id == Sensor.asset_id
                )
            ).all()

        for tag, asset_code, metric in rows:
            setpoint = ASSET_SETPOINTS.get(
                (asset_code, metric), PROFILES.get(metric, (10.0,))[0]
            )
            sims.append(
                SensorSim(
                    tag=tag,
                    asset_code=asset_code,
                    metric=metric,
                    setpoint=setpoint,
                    sigma=NOISE.get(metric, 1.0),
                )
            )
        return cls(sensors=sims, fault_probability=fault_probability, rng=random.Random(seed))

    def tick(self, observed_at: datetime) -> list[dict]:
        """One sample from every sensor."""
        self._maybe_inject()
        batch = []
        for sensor in self.sensors:
            value, quality = sensor.next_value()
            batch.append(
                {
                    "sensor_tag": sensor.tag,
                    "metric": sensor.metric,
                    "value": value,
                    "quality": quality,
                    "observed_at": observed_at.isoformat(),
                }
            )
        return batch

    def _maybe_inject(self) -> None:
        if self.rng.random() >= self.fault_probability:
            return
        healthy = [s for s in self.sensors if s.fault is None]
        if not healthy:
            return
        sensor = self.rng.choice(healthy)
        fault = self.rng.choice(FAULTS)
        sensor.inject(fault)

    def active_faults(self) -> dict[str, str]:
        return {s.tag: s.fault for s in self.sensors if s.fault}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=int, default=250, help="target readings per second")
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--seed", type=int, default=4071)
    parser.add_argument("--out", default=None, help="write readings to a JSONL file")
    parser.add_argument(
        "--post", action="store_true", help="POST batches to a running API instead"
    )
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--api-key", default="gateway-key")
    args = parser.parse_args()

    random.seed(args.seed)
    simulator = PlantSimulator.from_database(seed=args.seed)
    if not simulator.sensors:
        print("no sensors found - run scripts/seed_plant.py first")
        return 1

    per_tick = len(simulator.sensors)
    ticks = max(1, args.rate * args.seconds // per_tick)
    interval = timedelta(seconds=per_tick / args.rate)
    start = datetime.now(UTC) - interval * ticks

    print(
        f"{per_tick} sensors, {ticks} ticks -> {ticks * per_tick} readings "
        f"at a nominal {args.rate}/s"
    )

    if args.post:
        return _post(simulator, ticks, start, interval, args)

    out = Path(args.out or "data/telemetry.jsonl")
    out.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with out.open("w") as handle:
        for index in range(ticks):
            for reading in simulator.tick(start + interval * index):
                handle.write(json.dumps(reading) + "\n")
                written += 1
    print(f"wrote {written} readings to {out}")
    print(f"faults injected: {simulator.active_faults()}")
    return 0


def _post(simulator, ticks, start, interval, args) -> int:
    import urllib.error
    import urllib.request

    sent = accepted = 0
    for index in range(ticks):
        batch = {"source": "simulator", "readings": simulator.tick(start + interval * index)}
        request = urllib.request.Request(
            f"{args.url}/api/v1/telemetry",
            data=json.dumps(batch).encode(),
            headers={"content-type": "application/json", "X-API-Key": args.api_key},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = json.loads(response.read())
                accepted += body.get("accepted", 0)
        except urllib.error.URLError as exc:
            print(f"post failed: {exc}")
            return 1
        sent += len(batch["readings"])
    print(f"posted {sent} readings, {accepted} accepted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
