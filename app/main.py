"""SteelPulse FastAPI application."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from app import __version__
from app.bus import get_bus
from app.config import get_settings
from app.db import get_engine, init_db, session_scope
from app.idempotency import get_store
from app.pipeline.workers import get_pool
from app.routers import alerts, assets, dashboard, inventory, maintenance, ops, telemetry
from app.rules import get_rule_engine

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s %(message)s"
)
log = logging.getLogger("steelpulse")


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    init_db()

    with session_scope() as session:
        rule_count = get_rule_engine().load(session)

    bus = await get_bus()
    pool = get_pool()
    await pool.start()

    log.info(
        "SteelPulse %s ready (db=%s, bus=%s, idempotency=%s, rules=%d, workers=%d)",
        __version__,
        get_engine().dialect.name,
        bus.name,
        get_store().backend,
        rule_count,
        settings.worker_count,
    )
    yield

    await pool.stop()
    await bus.close()


app = FastAPI(
    title="SteelPulse",
    version=__version__,
    summary="Industrial operations and supply chain automation platform",
    description=(
        "Event-driven plant automation: IoT telemetry ingestion, rule-based "
        "anomaly detection, idempotent maintenance work orders, inventory "
        "replenishment and supply-chain operations, with SLA monitoring, "
        "energy reporting and fault recovery."
    ),
    lifespan=lifespan,
)

app.include_router(telemetry.router)
app.include_router(alerts.router)
app.include_router(maintenance.router)
app.include_router(inventory.router)
app.include_router(assets.router)
app.include_router(dashboard.router)
app.include_router(ops.router)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse("/dashboard")


@app.get("/dashboard", include_in_schema=False)
def operations_dashboard() -> FileResponse:
    return FileResponse("static/dashboard.html")


@app.get("/health", tags=["ops"])
async def health() -> dict:
    """Liveness and dependency report."""
    settings = get_settings()
    bus = await get_bus()
    return {
        "status": "ok",
        "version": __version__,
        "env": settings.env,
        "database": get_engine().dialect.name,
        "event_bus": bus.name,
        "idempotency_backend": get_store().backend,
        "workers_running": get_pool().running,
    }
