#!/usr/bin/env python3
"""Drive the pipeline at a target event rate and report what it did.

Runs the ingestion API path and the worker pool in one process against the
configured database and bus, feeding simulated telemetry at a fixed rate.
Reports sustained throughput, alert latency percentiles and event-delivery
completion - the share of accepted events that reached a terminal processed
state rather than being lost, stuck or dead-lettered.

    python scripts/seed_plant.py
    python scripts/run_loadtest.py --rate 250 --seconds 60
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select  # noqa: E402

from app.bus import get_bus  # noqa: E402
from app.config import get_settings  # noqa: E402
from app.db import get_engine, init_db, session_scope  # noqa: E402
from app.idempotency import get_store  # noqa: E402
from app.metrics import compute_kpis, percentile  # noqa: E402
from app.models import Alert, EventStatus, TelemetryEvent, WorkOrder  # noqa: E402
from app.pipeline.ingest import Reading, ingest_batch  # noqa: E402
from app.pipeline.workers import WorkerPool  # noqa: E402
from app.rules import get_rule_engine  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from simulate_sensors import PlantSimulator  # noqa: E402


async def feed(
    simulator: PlantSimulator, rate: int, seconds: int, bus, batch_readings: int
) -> dict:
    """Publish telemetry at the target rate, in batches, with real pacing."""
    per_tick = len(simulator.sensors)
    total_target = rate * seconds
    ticks = max(1, total_target // per_tick)
    tick_interval = per_tick / rate

    stats = {"sent": 0, "accepted": 0, "duplicates": 0, "rejected": 0, "ingest_ms": []}
    observed = datetime.now(UTC)
    started = time.perf_counter()

    pending: list[dict] = []
    for index in range(ticks):
        pending.extend(simulator.tick(observed + timedelta(seconds=index * tick_interval)))

        if len(pending) >= batch_readings or index == ticks - 1:
            readings = [
                Reading(
                    sensor_tag=item["sensor_tag"],
                    metric=item["metric"],
                    value=item["value"],
                    observed_at=datetime.fromisoformat(item["observed_at"]),
                    quality=item["quality"],
                )
                for item in pending
            ]
            batch_started = time.perf_counter()
            with session_scope() as session:
                result = await ingest_batch(session, readings, bus, source="loadtest")
            stats["ingest_ms"].append((time.perf_counter() - batch_started) * 1000)
            stats["sent"] += len(readings)
            stats["accepted"] += result.accepted
            stats["duplicates"] += result.duplicates
            stats["rejected"] += result.rejected
            pending = []

        # Pace to the target rate rather than running flat out.
        target_elapsed = (index + 1) * tick_interval
        drift = target_elapsed - (time.perf_counter() - started)
        if drift > 0:
            await asyncio.sleep(drift)

    stats["feed_seconds"] = time.perf_counter() - started
    return stats


async def drain(bus, pool: WorkerPool, timeout: float = 60.0) -> float:
    """Wait for the workers to clear the backlog, or give up after timeout."""
    started = time.perf_counter()
    while time.perf_counter() - started < timeout:
        depth = await bus.depth()
        pending = await bus.pending_count()
        if depth == 0 and pending == 0:
            break
        await asyncio.sleep(0.2)
    return time.perf_counter() - started


async def run(args) -> dict:
    settings = get_settings()
    bus = await get_bus()

    # Create any missing tables before querying them, so an unseeded database
    # reports what to do rather than raising "no such table" from the ORM.
    init_db()

    with session_scope() as session:
        rules = get_rule_engine().reload(session)

    if not rules:
        raise SystemExit(
            f"no rules in {get_engine().dialect.name} - run scripts/seed_plant.py first"
        )

    simulator = PlantSimulator.from_database(
        fault_probability=args.fault_probability, seed=args.seed
    )
    if not simulator.sensors:
        raise SystemExit("no sensors found - run scripts/seed_plant.py first")

    pool = WorkerPool(bus=bus, engine=get_rule_engine())
    await pool.start()

    print(
        f"SteelPulse load test: target {args.rate} events/s for {args.seconds}s "
        f"({len(simulator.sensors)} sensors, {rules} rules, db={get_engine().dialect.name}, "
        f"bus={bus.name}, workers={settings.worker_count})"
    )

    feed_stats = await feed(simulator, args.rate, args.seconds, bus, args.batch)
    print(
        f"  fed {feed_stats['sent']} readings in {feed_stats['feed_seconds']:.1f}s "
        f"({feed_stats['sent'] / feed_stats['feed_seconds']:.0f}/s)"
    )

    drain_seconds = await drain(bus, pool, timeout=args.drain_timeout)
    print(f"  drained in {drain_seconds:.1f}s")
    await pool.stop()

    with session_scope() as session:
        kpis = compute_kpis(session)
        alert_latencies = [
            value
            for (value,) in session.execute(
                select(Alert.detection_latency_ms).where(Alert.detection_latency_ms > 0)
            ).all()
        ]
        accepted = session.query(TelemetryEvent).count()
        processed = (
            session.query(TelemetryEvent)
            .filter(TelemetryEvent.status == EventStatus.PROCESSED)
            .count()
        )
        work_orders = session.query(WorkOrder).count()

    wall = feed_stats["feed_seconds"] + drain_seconds
    return {
        "target_rate_per_sec": args.rate,
        "duration_seconds": args.seconds,
        # a throughput number means nothing without the stack it ran on
        "environment": {
            "event_bus": bus.name,
            "database": get_engine().dialect.name,
            "idempotency_backend": get_store().backend,
            "workers": settings.worker_count,
            "batch_size": settings.batch_size,
            "sensors": len(simulator.sensors),
            "rules": rules,
            "fault_probability": args.fault_probability,
            "seed": args.seed,
        },
        "ingest": {
            "sent": feed_stats["sent"],
            "accepted": feed_stats["accepted"],
            "duplicates": feed_stats["duplicates"],
            "rejected": feed_stats["rejected"],
            "feed_seconds": round(feed_stats["feed_seconds"], 3),
            "sustained_rate_per_sec": round(
                feed_stats["sent"] / feed_stats["feed_seconds"], 1
            ),
            "batch_ingest_ms_p95": percentile(feed_stats["ingest_ms"], 95),
        },
        "delivery": {
            "accepted_events": accepted,
            "processed_events": processed,
            "completion_pct": round(processed / accepted * 100, 4) if accepted else 100.0,
            "dead_lettered": kpis["dead_lettered"],
            "backlog": kpis["backlog"],
            "drain_seconds": round(drain_seconds, 3),
            "end_to_end_rate_per_sec": round(processed / wall, 1) if wall else 0.0,
        },
        "alerting": {
            "alerts_opened": kpis["alerts_total"],
            "work_orders_created": work_orders,
            "alert_latency_ms_p50": percentile(alert_latencies, 50),
            "alert_latency_ms_p95": percentile(alert_latencies, 95),
            "alert_latency_ms_p99": percentile(alert_latencies, 99),
        },
        "event_latency": {
            "p50": kpis["event_latency_ms_p50"],
            "p95": kpis["event_latency_ms_p95"],
            "p99": kpis["event_latency_ms_p99"],
        },
        "worker_stats": dict(pool.stats),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=int, default=250, help="target events per second")
    parser.add_argument("--seconds", type=int, default=60)
    parser.add_argument("--batch", type=int, default=250, help="readings per ingest call")
    parser.add_argument("--fault-probability", type=float, default=0.0015)
    parser.add_argument("--drain-timeout", type=float, default=120.0)
    parser.add_argument("--seed", type=int, default=4071)
    parser.add_argument("-o", "--report", default="data/loadtest_report.json")
    args = parser.parse_args()

    report = asyncio.run(run(args))
    Path(args.report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report).write_text(json.dumps(report, indent=2))

    ingest, delivery, alerting = report["ingest"], report["delivery"], report["alerting"]
    print("\n=== SteelPulse load test ===")
    print(f"target rate               {report['target_rate_per_sec']} events/s")
    print(f"sustained ingest rate     {ingest['sustained_rate_per_sec']} events/s")
    print(f"events accepted           {ingest['accepted']}")
    print(f"delivery completion       {delivery['completion_pct']:.4f}%")
    print(f"dead-lettered / backlog   {delivery['dead_lettered']} / {delivery['backlog']}")
    print(
        f"alert latency p50/p95/p99 {alerting['alert_latency_ms_p50']:.1f} / "
        f"{alerting['alert_latency_ms_p95']:.1f} / {alerting['alert_latency_ms_p99']:.1f} ms"
    )
    print(
        f"alerts / work orders      {alerting['alerts_opened']} / "
        f"{alerting['work_orders_created']}"
    )
    print(f"\nreport written to {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
