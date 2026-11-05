#!/usr/bin/env python3
"""Seed a plant: assets, sensors, detection rules, parts and suppliers.

Models a small steel finishing works - a reheat furnace, a rolling mill, a
cooling line and the hydraulics and drives that serve them. Each asset carries
the sensors you would actually instrument it with, the rules that matter for
that equipment, and the spare part a technician would fit.

    python scripts/seed_plant.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.db import reset_db, session_scope  # noqa: E402
from app.models import (  # noqa: E402
    Asset,
    Part,
    Rule,
    RuleKind,
    Sensor,
    Severity,
    Supplier,
)

SUPPLIERS = [
    ("SUP-BEAR", "Nordic Bearing Works", 5),
    ("SUP-HYD", "Hydratech Components", 10),
    ("SUP-ELEC", "Voltaire Drives", 14),
    ("SUP-REFR", "Refracta Linings", 21),
]

PARTS = [
    ("BRG-6208", "Deep groove ball bearing 6208", 42.50, "SUP-BEAR", 24, 6, 24),
    ("SEAL-H40", "Hydraulic rod seal 40mm", 18.75, "SUP-HYD", 30, 8, 30),
    ("FLT-H10", "Hydraulic filter element 10um", 64.00, "SUP-HYD", 12, 4, 12),
    ("VFD-FAN", "VFD cooling fan assembly", 135.00, "SUP-ELEC", 6, 2, 6),
    ("THC-K", "Type K thermocouple probe", 88.00, "SUP-ELEC", 10, 3, 10),
    ("REF-BRK", "Refractory brick set", 310.00, "SUP-REFR", 4, 2, 6),
]

# asset code, name, line, criticality, [(metric, unit, hz)], [rules]
ASSETS = [
    (
        "FURN-01",
        "Reheat furnace 1",
        "hot-mill",
        Severity.CRITICAL,
        [("temperature_c", "degC", 1.0), ("power_kw", "kW", 1.0), ("pressure_bar", "bar", 1.0)],
        [
            ("temperature_c", RuleKind.THRESHOLD_HIGH, 1280.0, Severity.CRITICAL, 3, "REF-BRK",
             "Furnace over temperature - refractory at risk"),
            ("temperature_c", RuleKind.FLATLINE, 0.05, Severity.HIGH, 1, "THC-K",
             "Furnace thermocouple not responding"),
            ("power_kw", RuleKind.THRESHOLD_HIGH, 4200.0, Severity.MEDIUM, 5, None,
             "Furnace drawing above its demand band"),
        ],
    ),
    (
        "MILL-01",
        "Roughing mill stand",
        "hot-mill",
        Severity.CRITICAL,
        [("vibration_mm_s", "mm/s", 2.0), ("temperature_c", "degC", 1.0), ("power_kw", "kW", 1.0)],
        [
            ("vibration_mm_s", RuleKind.THRESHOLD_HIGH, 11.0, Severity.CRITICAL, 3, "BRG-6208",
             "Mill stand vibration above ISO alarm band - bearing wear"),
            ("vibration_mm_s", RuleKind.RATE_OF_CHANGE, 6.0, Severity.HIGH, 2, "BRG-6208",
             "Step change in mill vibration - suspect impact damage"),
            ("temperature_c", RuleKind.THRESHOLD_HIGH, 95.0, Severity.HIGH, 4, "BRG-6208",
             "Mill bearing housing running hot"),
        ],
    ),
    (
        "HYD-01",
        "Mill hydraulic power unit",
        "hot-mill",
        Severity.HIGH,
        [("pressure_bar", "bar", 2.0), ("temperature_c", "degC", 1.0), ("flow_lpm", "L/min", 1.0)],
        [
            ("pressure_bar", RuleKind.THRESHOLD_LOW, 120.0, Severity.CRITICAL, 3, "SEAL-H40",
             "Hydraulic pressure collapsed - seal or pump failure"),
            ("temperature_c", RuleKind.THRESHOLD_HIGH, 70.0, Severity.HIGH, 5, "FLT-H10",
             "Hydraulic oil overheating - filter restriction"),
            ("flow_lpm", RuleKind.THRESHOLD_LOW, 40.0, Severity.MEDIUM, 4, "FLT-H10",
             "Hydraulic flow below demand"),
        ],
    ),
    (
        "COOL-01",
        "Runout cooling line",
        "finishing",
        Severity.HIGH,
        [("flow_lpm", "L/min", 1.0), ("temperature_c", "degC", 1.0), ("power_kw", "kW", 1.0)],
        [
            ("flow_lpm", RuleKind.THRESHOLD_LOW, 180.0, Severity.HIGH, 3, None,
             "Cooling water flow below setpoint - product quality risk"),
            ("flow_lpm", RuleKind.FLATLINE, 0.1, Severity.MEDIUM, 1, None,
             "Cooling flow meter not responding"),
        ],
    ),
    (
        "DRV-01",
        "Finishing line drive",
        "finishing",
        Severity.MEDIUM,
        [("current_a", "A", 2.0), ("temperature_c", "degC", 1.0), ("power_kw", "kW", 1.0)],
        [
            ("current_a", RuleKind.THRESHOLD_HIGH, 310.0, Severity.HIGH, 4, "VFD-FAN",
             "Drive current above continuous rating"),
            ("temperature_c", RuleKind.THRESHOLD_HIGH, 80.0, Severity.MEDIUM, 5, "VFD-FAN",
             "Drive cabinet overheating - check cooling fan"),
        ],
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keep", action="store_true", help="add to existing data")
    args = parser.parse_args()

    if not args.keep:
        reset_db()

    with session_scope() as session:
        for code, name, lead in SUPPLIERS:
            session.add(Supplier(code=code, name=name, lead_time_days=lead))

        for number, description, cost, supplier, on_hand, point, qty in PARTS:
            session.add(
                Part(
                    part_number=number,
                    description=description,
                    unit_cost=cost,
                    supplier_code=supplier,
                    on_hand=float(on_hand),
                    reorder_point=float(point),
                    reorder_quantity=float(qty),
                )
            )

        sensor_total = rule_total = 0
        for code, name, line, criticality, sensors, rules in ASSETS:
            asset = Asset(code=code, name=name, line=line, criticality=criticality)
            session.add(asset)
            session.flush()

            for metric, unit, hz in sensors:
                session.add(
                    Sensor(
                        tag=f"{code}.{metric}",
                        asset_id=asset.id,
                        metric=metric,
                        unit=unit,
                        sample_hz=hz,
                    )
                )
                sensor_total += 1

            for metric, kind, threshold, severity, samples, part, description in rules:
                session.add(
                    Rule(
                        asset_id=asset.id,
                        metric=metric,
                        kind=kind,
                        threshold=threshold,
                        severity=severity,
                        consecutive_samples=samples,
                        part_number=part,
                        description=description,
                    )
                )
                rule_total += 1

    print(
        f"seeded {len(ASSETS)} assets, {sensor_total} sensors, {rule_total} rules, "
        f"{len(PARTS)} parts, {len(SUPPLIERS)} suppliers"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
