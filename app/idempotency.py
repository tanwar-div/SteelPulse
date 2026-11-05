"""Idempotency primitives.

Every stage that creates a durable record - a work order, a replenishment
order - is keyed. The first claim on a key wins and proceeds; every replay
gets the stored result reference back instead of creating a second record.

Redis is the hot-path store; a thread-safe in-process store takes over when
Redis is unreachable. The processed-key table backs both and is what survives
a restart or a cache flush.
"""

from __future__ import annotations

import hashlib
import logging
import threading
from dataclasses import dataclass

from app.config import get_settings

log = logging.getLogger(__name__)

KEY_TTL_SECONDS = 60 * 60 * 24 * 30


def event_key(sensor_tag: str, observed_at_iso: str) -> str:
    """Identity of a reading: one sensor, one instant."""
    return f"{sensor_tag}@{observed_at_iso}"


def work_order_key(asset_code: str, rule_id: str) -> str:
    """Identity of a maintenance need.

    Deliberately not derived from the alert or the event: a sensor that keeps
    breaching the same rule describes one job to do, however many events and
    alert updates it produces.
    """
    basis = f"{asset_code.strip().upper()}|{rule_id}"
    return "wo:" + hashlib.sha256(basis.encode()).hexdigest()[:40]


def replenishment_key(part_number: str, cycle: str) -> str:
    """Identity of a replenishment need for one part in one reorder cycle."""
    basis = f"{part_number.strip().upper()}|{cycle}"
    return "rep:" + hashlib.sha256(basis.encode()).hexdigest()[:40]


@dataclass
class Claim:
    key: str
    acquired: bool
    existing_ref: str | None = None


class _MemoryStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: dict[str, str] = {}

    def claim(self, key: str, value: str) -> tuple[bool, str | None]:
        with self._lock:
            if key in self._data:
                return False, self._data[key]
            self._data[key] = value
            return True, None

    def set(self, key: str, value: str) -> None:
        with self._lock:
            self._data[key] = value

    def get(self, key: str) -> str | None:
        with self._lock:
            return self._data.get(key)

    def delete(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


class IdempotencyStore:
    """Claim-once store backed by Redis, falling back to process memory."""

    def __init__(self) -> None:
        self._memory = _MemoryStore()
        self._redis = self._connect()

    @property
    def backend(self) -> str:
        return "redis" if self._redis is not None else "memory"

    def _connect(self):
        try:
            import redis
        except ImportError:
            return None
        try:
            client = redis.Redis.from_url(get_settings().redis_url, decode_responses=True)
            client.ping()
            log.info("idempotency store: redis")
            return client
        except Exception:  # pragma: no cover - depends on local services
            log.info("idempotency store: in-process memory (redis unreachable)")
            return None

    def claim(self, key: str, value: str = "pending") -> Claim:
        """Atomically take ownership of a key. acquired=False means replay."""
        if self._redis is not None:
            acquired = bool(self._redis.set(key, value, nx=True, ex=KEY_TTL_SECONDS))
            return Claim(key, acquired, None if acquired else self._redis.get(key))
        acquired, existing = self._memory.claim(key, value)
        return Claim(key, acquired, existing)

    def complete(self, key: str, result_ref: str) -> None:
        if self._redis is not None:
            self._redis.set(key, result_ref, ex=KEY_TTL_SECONDS)
        else:
            self._memory.set(key, result_ref)

    def release(self, key: str) -> None:
        """Drop a claim so a failed attempt can be retried."""
        if self._redis is not None:
            self._redis.delete(key)
        else:
            self._memory.delete(key)

    def get(self, key: str) -> str | None:
        if self._redis is not None:
            return self._redis.get(key)
        return self._memory.get(key)

    def clear(self) -> None:
        """Test helper - wipes only the in-process store."""
        self._memory.clear()


_store: IdempotencyStore | None = None


def get_store() -> IdempotencyStore:
    global _store
    if _store is None:
        _store = IdempotencyStore()
    return _store
