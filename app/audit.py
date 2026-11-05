"""Append-only action trail."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AuditEvent, Role


def record(
    session: Session,
    *,
    action: str,
    actor: str,
    actor_role: Role,
    subject_type: str,
    subject_id: str | None = None,
    detail: dict | None = None,
) -> AuditEvent:
    event = AuditEvent(
        subject_type=subject_type,
        subject_id=subject_id,
        action=action,
        actor=actor,
        actor_role=actor_role,
        detail=detail or {},
    )
    session.add(event)
    return event


def trail(session: Session, subject_type: str, subject_id: str) -> list[AuditEvent]:
    return list(
        session.scalars(
            select(AuditEvent)
            .where(AuditEvent.subject_type == subject_type, AuditEvent.subject_id == subject_id)
            .order_by(AuditEvent.created_at, AuditEvent.id)
        ).all()
    )
