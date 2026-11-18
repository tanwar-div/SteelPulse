"""Runtime configuration, loaded from the environment."""

import os
from functools import lru_cache


def _flag(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


class Settings:
    """Application settings.

    PostgreSQL and Redis are the deployment targets. Both degrade to a local
    fallback (SQLite file / in-process event bus) so the test suite and the
    load scripts run with no external services.
    """

    def __init__(self) -> None:
        self.app_name = "SteelPulse"
        self.env = os.getenv("STEELPULSE_ENV", "local")

        self.database_url = os.getenv(
            "DATABASE_URL",
            "postgresql+psycopg://steelpulse:steelpulse@localhost:5432/steelpulse",
        )
        self.database_fallback_url = os.getenv(
            "DATABASE_FALLBACK_URL", "sqlite+pysqlite:///./steelpulse.db"
        )
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.rabbitmq_url = os.getenv(
            "RABBITMQ_URL", "amqp://steelpulse:steelpulse@localhost:5672/"
        )

        # Event bus
        # rabbitmq | redis | memory | auto (tries each in that order)
        self.bus_backend = os.getenv("BUS_BACKEND", "auto").strip().lower()
        self.telemetry_stream = os.getenv("TELEMETRY_STREAM", "steelpulse.telemetry")
        self.consumer_group = os.getenv("CONSUMER_GROUP", "detectors")

        # AMQP topology. The queue is the consumer group's equivalent: every
        # worker consumes the one queue, so the broker round-robins between them.
        self.rabbitmq_exchange = os.getenv("RABBITMQ_EXCHANGE", "steelpulse.telemetry")
        self.rabbitmq_queue = os.getenv("RABBITMQ_QUEUE", "steelpulse.telemetry.detectors")
        self.rabbitmq_prefetch = int(os.getenv("RABBITMQ_PREFETCH", "512"))
        self.rabbitmq_connect_timeout = float(os.getenv("RABBITMQ_CONNECT_TIMEOUT", "3"))
        self.worker_count = int(os.getenv("WORKER_COUNT", "4"))
        self.batch_size = int(os.getenv("BATCH_SIZE", "256"))
        self.block_ms = int(os.getenv("BLOCK_MS", "50"))
        self.stream_maxlen = int(os.getenv("STREAM_MAXLEN", "1000000"))

        # Delivery guarantees
        self.max_delivery_attempts = int(os.getenv("MAX_DELIVERY_ATTEMPTS", "5"))
        self.stale_claim_ms = int(os.getenv("STALE_CLAIM_MS", "30000"))
        self.retry_base_delay = float(os.getenv("RETRY_BASE_DELAY", "0.05"))

        # Anomaly detection
        self.flatline_samples = int(os.getenv("FLATLINE_SAMPLES", "12"))
        self.alert_suppression_seconds = float(os.getenv("ALERT_SUPPRESSION_SECONDS", "300"))
        self.heartbeat_timeout_seconds = float(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "120"))

        # Maintenance and inventory
        self.auto_work_order_severities = tuple(
            s.strip()
            for s in os.getenv("AUTO_WORK_ORDER_SEVERITIES", "critical,high").split(",")
            if s.strip()
        )
        self.auto_replenish = _flag("AUTO_REPLENISH", "true")

        # SLA budgets, minutes
        self.sla_minutes_ack_critical = float(os.getenv("SLA_MINUTES_ACK_CRITICAL", "15"))
        self.sla_minutes_ack_high = float(os.getenv("SLA_MINUTES_ACK_HIGH", "60"))
        self.sla_minutes_ack_medium = float(os.getenv("SLA_MINUTES_ACK_MEDIUM", "240"))
        self.sla_hours_work_order_critical = float(os.getenv("SLA_HOURS_WO_CRITICAL", "8"))
        self.sla_hours_work_order_high = float(os.getenv("SLA_HOURS_WO_HIGH", "24"))
        self.sla_hours_work_order_normal = float(os.getenv("SLA_HOURS_WO_NORMAL", "72"))

        self.auth_disabled = _flag("AUTH_DISABLED")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
