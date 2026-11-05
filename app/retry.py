"""Retry helper with exponential backoff and full jitter."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

from app.config import get_settings

log = logging.getLogger(__name__)

T = TypeVar("T")


class TransientError(RuntimeError):
    """The operation failed in a way a retry can plausibly fix."""


class PermanentError(RuntimeError):
    """Retrying cannot help; the caller routes to dead-letter handling."""


async def with_retries(
    operation: Callable[[int], Awaitable[T]],
    *,
    max_retries: int | None = None,
    base_delay: float | None = None,
    label: str = "operation",
) -> tuple[T, int]:
    """Run ``operation(attempt)`` until it succeeds or attempts are exhausted.

    Returns (result, attempts). Only TransientError is retried; PermanentError
    propagates on the first occurrence.
    """
    settings = get_settings()
    max_retries = settings.max_delivery_attempts - 1 if max_retries is None else max_retries
    base_delay = settings.retry_base_delay if base_delay is None else base_delay

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 2):
        try:
            return await operation(attempt), attempt
        except PermanentError:
            raise
        except TransientError as exc:
            last_error = exc
            if attempt > max_retries:
                break
            delay = random.uniform(0, base_delay * (2 ** (attempt - 1)))
            log.debug("%s attempt %d failed (%s), retrying in %.3fs", label, attempt, exc, delay)
            await asyncio.sleep(delay)

    raise PermanentError(f"{label} failed after {max_retries + 1} attempts: {last_error}")
