"""Monotonic document numbering backed by a counter row."""

from __future__ import annotations

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.models import Counter


def next_number(session: Session, name: str, prefix: str, width: int = 6) -> str:
    """Allocate the next number in a sequence.

    The increment happens in the database rather than in Python, so two
    workers allocating at the same moment cannot land on the same number.
    """
    if session.get(Counter, name) is None:
        session.add(Counter(name=name, value=0))
        session.flush()

    session.execute(update(Counter).where(Counter.name == name).values(value=Counter.value + 1))
    value = session.execute(select(Counter.value).where(Counter.name == name)).scalar_one()
    return f"{prefix}-{value:0{width}d}"
