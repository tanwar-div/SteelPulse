#!/usr/bin/env python3
"""Replay attack on the pipeline: prove no duplicate maintenance orders.

Three paths are replayed concurrently against events that already produced
work orders:

 1. re-ingest the identical telemetry, which must be recognised as a duplicate
    reading rather than taken a second time;
 2. re-publish the event and let the workers process it again, as an
    at-least-once bus does on its own after a consumer timeout;
 3. crash recovery - the processed-key row written by the original run is the
    only trace of the work order that an at-least-once redelivery can consult,
    so generation is re-driven directly to prove it converges on the existing
    order rather than opening a second one.

A correct run creates no additional telemetry events and no additional work
orders, and every replay resolves to the original order number.

    python scripts/replay_test.py --attempts 5000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select  # noqa: E402

from app import workorders as wo  # noqa: E402
from app.bus import Message, current_bus, get_bus  # noqa: E402
from app.db import get_engine, session_scope  # noqa: E402
from app.idempotency import get_store  # noqa: E402
from app.models import (  # noqa: E402
    Alert,
    ReplenishmentOrder,
    Role,
    TelemetryEvent,
    WorkOrder,
)
from app.pipeline.ingest import Reading, ingest_batch  # noqa: E402
from app.pipeline.processor import process_message  # noqa: E402
from app.rules import get_rule_engine  # noqa: E402


def snapshot() -> dict:
    with session_scope() as session:
        return {
            "events": session.query(TelemetryEvent).count(),
            "alerts": session.query(Alert).count(),
            "work_orders": session.query(WorkOrder).count(),
            "replenishments": session.query(ReplenishmentOrder).count(),
        }


def build_pool(limit: int) -> list[dict]:
    """Work orders that came from a rule, with the event that triggered them."""
    with session_scope() as session:
        rows = session.execute(
            select(
                WorkOrder.id,
                WorkOrder.number,
                WorkOrder.asset_code,
                WorkOrder.dedupe_key,
                WorkOrder.severity,
                WorkOrder.title,
                WorkOrder.part_number,
                Alert.rule_id,
                Alert.id,
                Alert.triggering_event_id,
            )
            .join(Alert, Alert.id == WorkOrder.alert_id)
            .limit(limit)
        ).all()

        pool = []
        for (
            order_id,
            number,
            asset_code,
            _dedupe_key,
            severity,
            title,
            part_number,
            rule_id,
            alert_id,
            event_id,
        ) in rows:
            event = session.get(TelemetryEvent, event_id) if event_id else None
            pool.append(
                {
                    "work_order_id": order_id,
                    "number": number,
                    "asset_code": asset_code,
                    "severity": severity,
                    "title": title,
                    "part_number": part_number,
                    "rule_id": rule_id,
                    "alert_id": alert_id,
                    "event": None
                    if event is None
                    else {
                        "event_key": event.event_key,
                        "event_id": event.id,
                        "sensor_tag": event.sensor_tag,
                        "asset_code": event.asset_code,
                        "metric": event.metric,
                        "value": event.value,
                        "quality": event.quality,
                        "observed_at": event.observed_at.isoformat(),
                        "received_at": event.received_at.isoformat(),
                        "source": "replay",
                    },
                }
            )
        return pool


async def replay(pool: list[dict], attempts: int, concurrency: int) -> dict:
    semaphore = asyncio.Semaphore(concurrency)
    bus = await get_bus()
    engine = get_rule_engine()
    with session_scope() as session:
        if not engine.loaded:
            engine.load(session)

    stats = {
        "ingest_replays": 0,
        "ingest_duplicates_detected": 0,
        "ingest_new_events": 0,
        "message_replays": 0,
        "message_replays_skipped": 0,
        "generation_replays": 0,
        "work_order_numbers_stable": 0,
        "work_order_numbers_changed": 0,
    }
    lock = asyncio.Lock()

    async def one(index: int) -> None:
        item = pool[index % len(pool)]
        async with semaphore:
            event = item["event"]

            if event is not None:
                # Path 1: the gateway re-sends a reading it already delivered.
                with session_scope() as session:
                    result = await ingest_batch(
                        session,
                        [
                            Reading(
                                sensor_tag=event["sensor_tag"],
                                metric=event["metric"],
                                value=event["value"],
                                observed_at=datetime.fromisoformat(event["observed_at"]),
                                quality=event["quality"],
                            )
                        ],
                        bus,
                        source="replay",
                    )
                async with lock:
                    stats["ingest_replays"] += 1
                    stats["ingest_duplicates_detected"] += result.duplicates
                    stats["ingest_new_events"] += result.accepted

                # Path 2: the bus redelivers the message after a consumer timeout.
                with session_scope() as session:
                    outcome = await process_message(
                        session, Message(id=f"replay-{index}", payload=event), engine
                    )
                async with lock:
                    stats["message_replays"] += 1
                    if outcome.skipped:
                        stats["message_replays_skipped"] += 1

            # Path 3: re-drive generation for the same maintenance need.
            with session_scope() as session:
                order, created = wo.generate(
                    session,
                    asset_code=item["asset_code"],
                    rule_id=item["rule_id"],
                    severity=item["severity"],
                    title=item["title"],
                    alert_id=item["alert_id"],
                    part_number=item["part_number"],
                    actor="replay.harness",
                    actor_role=Role.SYSTEM,
                )
                number = order.number if order is not None else None
            async with lock:
                stats["generation_replays"] += 1
                if number == item["number"] and not created:
                    stats["work_order_numbers_stable"] += 1
                else:
                    stats["work_order_numbers_changed"] += 1

    await asyncio.gather(*(one(i) for i in range(attempts)))
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-a", "--attempts", type=int, default=5000)
    parser.add_argument("-c", "--concurrency", type=int, default=32)
    parser.add_argument("--pool", type=int, default=50, help="distinct work orders to replay")
    parser.add_argument("--seed", type=int, default=11)
    parser.add_argument("-o", "--report", default="data/replay_report.json")
    args = parser.parse_args()

    pool = build_pool(args.pool * 4)
    if not pool:
        print("no work orders to replay - run scripts/run_loadtest.py first")
        return 1
    random.Random(args.seed).shuffle(pool)
    pool = pool[: args.pool]

    before = snapshot()
    print(f"replaying {args.attempts} attempts across {len(pool)} work orders")
    print(f"before: {before}")

    stats = asyncio.run(replay(pool, args.attempts, args.concurrency))
    after = snapshot()
    print(f"after:  {after}")

    delta = {key: after[key] - before[key] for key in before}
    passed = (
        delta["work_orders"] == 0
        and delta["events"] == 0
        and stats["ingest_new_events"] == 0
        and stats["work_order_numbers_changed"] == 0
    )

    report = {
        "attempts": args.attempts,
        "distinct_work_orders_replayed": len(pool),
        "environment": {
            "event_bus": getattr(current_bus(), "name", "unknown"),
            "database": get_engine().dialect.name,
            "idempotency_backend": get_store().backend,
        },
        "before": before,
        "after": after,
        "delta": delta,
        "stats": stats,
        "passed": passed,
    }
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).write_text(json.dumps(report, indent=2))

    print("\n=== replay / idempotency result ===")
    print(f"replay attempts              {args.attempts}")
    print(f"duplicate readings suppressed {stats['ingest_duplicates_detected']}")
    print(f"new events created           {stats['ingest_new_events']}  (expected 0)")
    print(f"message redeliveries skipped {stats['message_replays_skipped']}")
    print(f"generation replays           {stats['generation_replays']}")
    print(f"work order numbers unchanged {stats['work_order_numbers_stable']}")
    print(f"work order numbers changed   {stats['work_order_numbers_changed']}  (expected 0)")
    print(f"net new work orders          {delta['work_orders']}  (expected 0)")
    print(f"\nVERDICT: {'PASS - no duplicate maintenance orders' if passed else 'FAIL'}")
    print(f"report written to {args.report}")
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
