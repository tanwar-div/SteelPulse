# SteelPulse

**Industrial operations & supply chain automation platform**

SteelPulse is an event-driven platform for a plant floor. Sensor telemetry
arrives from gateways, gets scored against detection rules, and turns into the
operational work that follows from it: alerts somebody owns, maintenance jobs
with the right spare reserved, replenishment orders to the supplier when that
empties a bin, and energy accounting across the lines.

Two constraints shape the codebase:

- **No event is silently lost.** Every accepted reading becomes a durable row
  before it is published, and stays visible until it reaches a terminal state.
  Delivery completion is a number you can query, not a hope.
- **No fault produces two work orders.** A machine breaching the same rule
  10,000 times describes one job to do. Idempotency is keyed on the
  maintenance need, not the message.

**Stack:** Python · FastAPI · asyncio · Redis Streams · PostgreSQL · Docker

---

## Pipeline

```
gateway        ingest API              event bus              worker pool
  │     persist + publish      Redis Streams / in-proc     consumer group
  ▼            │                       │                         │
readings ──────┴──────▶ telemetry_events ──▶ stream ──▶ detect ──┬──▶ alert
               (durable before published)                        ├──▶ work order ──▶ reserve part
                                                                 │                   └──▶ replenish
                                                                 └──▶ energy rollup
```

Acceptance is deliberately thin — validate, insert, append, return — so a
burst of sensor traffic is bounded by an insert rather than by the slowest
consumer. Everything downstream happens in the worker pool.

### Delivery guarantees

The event row is the record of record. A reading is persisted *before* it is
published, because an event on the bus that has no durable row would be lost
with no way to notice. From there it moves `received → processed`, or parks in
`failed` / `dead_letter` where it stays queryable and replayable.

Redis Streams consumer groups give at-least-once delivery: a message stays in
the group's pending list until acked, so a worker that dies mid-message leaves
its entry behind rather than dropping it. The recovery loop claims entries
whose consumer has gone quiet (`XAUTOCLAIM`) and runs them again. An
in-process bus implements the same semantics — pending list, explicit ack,
stale claim — so the pipeline, the tests and the load scripts behave
identically with no broker running.

At-least-once only works if processing is safe to repeat, so it is: a message
whose event is already `processed` is acked and dropped.

### Anomaly detection

Four rule kinds, evaluated against a bounded in-memory window per sensor so
detection costs no database round trip on the hot path:

| Kind | Fires when |
|---|---|
| `threshold_high` / `threshold_low` | value past the limit for N consecutive samples |
| `rate_of_change` | absolute step between samples beyond the limit |
| `flatline` | the signal stops moving — usually a dead sensor, not a stable process |

Requiring N consecutive samples is what stops one noisy reading raising an
alert. A reading the gateway flagged as bad quality is not evidence of
anything, so it updates no streaks and raises nothing.

Alerts deduplicate on asset + metric + rule: a flapping sensor increments one
alert's occurrence count instead of flooding the queue, and a worsening
condition escalates that alert's severity but never walks it back down.

### Idempotent work-order generation

The idempotency key is `asset + rule` — the maintenance need itself, not the
alert and not the event. Three layers enforce it:

1. **Durable processed-key ledger**, consulted first. It outlives the process
   and the Redis cache, so a worker that created an order and died before
   committing cannot create a second one after restart.
2. **Redis claim**, which serialises workers racing the same fault.
3. **A partial unique index** on the open orders — `UNIQUE(dedupe_key) WHERE
   status NOT IN ('COMPLETED','CANCELLED')`. A plain unique index would be
   wrong: a machine that breaks, gets fixed, and breaks again must be able to
   raise a second job for the same rule. The database enforces *at most one
   open job per need*, which is the actual invariant.

Replenishment is idempotent the same way, per reorder cycle: while an order
for a part is open, re-running the check returns that order; once it is
received the cycle advances and the part can be ordered again.

### Supply chain

A work order reserves the spare it needs. Reserved stock counts against
availability — stock promised to a job is not stock you have — so a
reservation can push a part to its reorder point and raise a supplier order on
the spot. A job with no stock parks in `awaiting_parts` rather than being
dispatched to a technician who would arrive empty-handed. Completing the job
issues the part; cancelling it returns the reservation. Every move is written
to an append-only stock ledger.

### Operations

- **RBAC** — `operator`, `technician`, `planner`, `supervisor`, and a `system`
  role for gateways. An operator can acknowledge an alert but not resolve it;
  only a supervisor cancels a job or forces a recovery sweep.
- **SLA monitoring** — acknowledgement budgets per alert severity, completion
  budgets per work-order severity, both reported as breaches.
- **Energy reporting** — power telemetry is a rate, so consumption is
  integrated over time into hourly buckets per asset, carrying kWh, peak kW
  for demand charges, and cost.
- **Fault recovery** — periodic and on-demand claiming of stale messages, a
  dead-letter queue, and single-event replay once the underlying fault is fixed.
- **Audit trail** — append-only, with actor and role, on every operator action.

---

## Quickstart

```bash
make install      # venv + dependencies
make seed         # a 5-asset plant: furnace, mill, hydraulics, cooling, drive
make test         # 124 tests
make dev          # http://localhost:8000
```

Or with Postgres and Redis:

```bash
docker compose up --build
```

With neither reachable, the app falls back to a SQLite file and the in-process
bus, so everything runs with no services. `GET /health` and the startup log
report which backends are actually in use.

### Dashboard

`http://localhost:8000/dashboard?key=supervisor-key` — ingest rate, delivery
completion, alert latency, open alerts and jobs, short parts, SLA breaches and
energy by asset, refreshing every 10s. Interactive API docs at `/docs`.

---

## API

| Method | Path | Role | Purpose |
|---|---|---|---|
| `POST` | `/api/v1/telemetry` | system, operator | Ingest a batch of readings |
| `GET` | `/api/v1/telemetry/events` | any | Inspect events and their status |
| `GET` | `/api/v1/telemetry/events/dead-letter` | any | Events the pipeline gave up on |
| `GET` | `/api/v1/alerts` | any | Alert queue |
| `POST` | `/api/v1/alerts/{id}/acknowledge` | operator, technician | Take ownership |
| `POST` | `/api/v1/alerts/{id}/resolve` | technician, supervisor | Close an alert |
| `GET` | `/api/v1/work-orders` | any | Maintenance queue |
| `POST` | `/api/v1/work-orders/{id}/assign` | planner, supervisor | Assign a technician |
| `POST` | `/api/v1/work-orders/{id}/start` | technician | Begin work |
| `POST` | `/api/v1/work-orders/{id}/complete` | technician | Close out, issue parts |
| `POST` | `/api/v1/work-orders/{id}/cancel` | supervisor | Cancel, release parts |
| `GET` | `/api/v1/work-orders/{id}/audit` | any | Append-only trail |
| `GET` | `/api/v1/inventory/parts` | any | Stock, with `short_only` |
| `POST` | `/api/v1/inventory/parts/{pn}/replenish` | planner, supervisor | Raise a supplier order |
| `POST` | `/api/v1/inventory/replenishments/{id}/receive` | planner | Book a delivery in |
| `GET` | `/api/v1/assets` | any | Asset register with live counts |
| `POST` | `/api/v1/assets/rules/reload` | supervisor | Re-read rule configuration |
| `GET` | `/api/v1/dashboard/kpis` | any | Operational KPIs |
| `GET` | `/api/v1/dashboard/sla-breaches` | any | Past-deadline alerts and jobs |
| `GET` | `/api/v1/dashboard/energy` | any | Energy by asset and line |
| `GET` | `/api/v1/ops/pipeline` | any | Worker pool health, stream depth |
| `POST` | `/api/v1/ops/pipeline/recover` | supervisor | Force a recovery sweep |
| `POST` | `/api/v1/ops/events/{id}/replay` | supervisor | Replay a dead-lettered event |
| `GET` | `/health` | public | Liveness and dependency report |

Authentication is an `X-API-Key` header; keys map to a username and one role
via `STEELPULSE_API_KEYS` (`key:username:role`, comma separated).

```bash
curl -X POST localhost:8000/api/v1/telemetry \
  -H 'X-API-Key: gateway-key' -H 'content-type: application/json' \
  -d '{
    "source": "gateway-1",
    "readings": [
      {"sensor_tag": "MILL-01.vibration_mm_s", "metric": "vibration_mm_s",
       "value": 13.8, "observed_at": "2026-04-01T09:00:00Z", "quality": 1.0}
    ]
  }'
```

```json
{"accepted": 1, "duplicates": 0, "rejected": 0, "errors": [], "ingest_ms": 9.8}
```

Send the same batch again and every reading comes back as a duplicate, with
nothing re-processed and no second work order.

---

## Measuring the platform

The repository ships the harnesses that produce its own numbers.

```bash
make seed                                        # plant master data
python scripts/simulate_sensors.py --rate 250 --seconds 60 --post
make loadtest                                    # throughput, latency, completion
make replay                                      # 5,000 replay attempts
```

`simulate_sensors.py` drives every seeded sensor with a plausible profile for
its equipment — process noise around a setpoint, a shift-length cycle, the
occasional bad read — and injects real faults over time: a bearing degrading,
a hydraulic seal letting go, a thermocouple sticking. The detection rules then
have something genuine to find.

`run_loadtest.py` feeds telemetry at a fixed rate with real pacing, drains the
backlog, and reports sustained throughput, alert-latency percentiles and
event-delivery completion — the share of accepted events that reached a
terminal processed state rather than being lost, stuck or dead-lettered. It
writes `data/loadtest_report.json`.

`replay_test.py` attacks idempotency from three directions concurrently: the
gateway re-sending readings it already delivered, the bus redelivering
messages after a consumer timeout, and generation re-driven directly against
the processed-key ledger. A passing run creates no additional events and no
additional work orders.

Tune the run with `--rate`, `--seconds`, `--fault-probability`, `WORKER_COUNT`
and `BATCH_SIZE`.

---

## Layout

```
app/
  main.py              FastAPI app, lifespan, worker pool startup
  config.py            environment-driven settings
  models.py            assets, sensors, rules, events, alerts, work orders, inventory
  bus.py               Redis Streams consumer group + in-process equivalent
  rules.py             rule engine with per-sensor evaluation windows
  alerts.py            alert dedupe, escalation, suppression
  workorders.py        idempotent generation, parts, completion
  inventory.py         reservations, stock ledger, cycle-keyed replenishment
  energy.py            power integration and reporting
  idempotency.py       keys and the claim store
  retry.py             backoff with jitter, transient vs permanent errors
  sla.py               budgets and breach detection
  metrics.py           KPI aggregation
  auth.py              RBAC
  audit.py             append-only action trail
  pipeline/
    ingest.py          batch acceptance, dedupe, publish
    processor.py       per-message detect / alert / raise work / account energy
    workers.py         consumer pool and recovery loop
  routers/             telemetry, alerts, maintenance, inventory, assets, dashboard, ops
scripts/               seed, sensor simulator, load test, replay test
tests/                 124 tests
static/dashboard.html  operations dashboard
```

## Configuration

Every setting is an environment variable; `.env.example` carries the full set.
The ones worth knowing:

| Variable | Default | Effect |
|---|---|---|
| `DATABASE_URL` | local Postgres | falls back to `DATABASE_FALLBACK_URL` (SQLite) if unreachable |
| `REDIS_URL` | `redis://localhost:6379/0` | falls back to the in-process bus if unreachable |
| `WORKER_COUNT` | `4` | consumers in the pool |
| `BATCH_SIZE` | `256` | messages read per consumer poll |
| `MAX_DELIVERY_ATTEMPTS` | `5` | redeliveries before an event is dead-lettered |
| `STALE_CLAIM_MS` | `30000` | idle time before another worker claims a message |
| `ALERT_SUPPRESSION_SECONDS` | `300` | quiet window after an alert is resolved |
| `AUTO_WORK_ORDER_SEVERITIES` | `critical,high` | severities that become jobs on their own |
| `SLA_MINUTES_ACK_CRITICAL` | `15` | acknowledgement budget for a critical alert |

## Deployment

`docker-compose.yml` runs the API alongside PostgreSQL and Redis. Scale the
pipeline horizontally by running more API replicas or raising `WORKER_COUNT`:
the Redis consumer group distributes messages across every consumer, and the
idempotency keys mean adding workers cannot produce duplicate work orders.
