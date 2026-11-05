"""Role-based access control.

Callers present an API key; each key maps to a principal with one role.
Endpoints declare the roles they accept via the ``require_roles`` dependency.
Keys come from STEELPULSE_API_KEYS as ``key:username:role`` triples separated
by commas, with a local default set.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from fastapi import Depends, Header, HTTPException, status

from app.config import get_settings
from app.models import Role

DEFAULT_KEYS = (
    "gateway-key:edge.gateway:system",
    "operator-key:line.operator:operator",
    "tech-key:maint.tech:technician",
    "planner-key:supply.planner:planner",
    "supervisor-key:plant.supervisor:supervisor",
)


@dataclass(frozen=True)
class Principal:
    username: str
    role: Role

    def has_any(self, roles: tuple[Role, ...]) -> bool:
        return self.role in roles


def _load_keys() -> dict[str, Principal]:
    raw = os.getenv("STEELPULSE_API_KEYS")
    entries = raw.split(",") if raw else list(DEFAULT_KEYS)
    table: dict[str, Principal] = {}
    for entry in entries:
        parts = entry.strip().split(":")
        if len(parts) != 3:
            continue
        key, username, role = parts
        try:
            table[key] = Principal(username, Role(role))
        except ValueError:
            continue
    return table


_KEYS = _load_keys()

ANONYMOUS = Principal("anonymous", Role.SUPERVISOR)


def current_principal(x_api_key: str | None = Header(default=None)) -> Principal:
    """Resolve the caller from its API key."""
    if get_settings().auth_disabled:
        return ANONYMOUS
    principal = _KEYS.get(x_api_key or "")
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing or invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    return principal


def require_roles(*roles: Role):
    """Dependency factory restricting an endpoint to the given roles."""
    allowed = tuple(roles)

    def dependency(principal: Principal = Depends(current_principal)) -> Principal:
        if not principal.has_any(allowed):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"role '{principal.role.value}' may not perform this action; "
                f"requires one of {[r.value for r in allowed]}",
            )
        return principal

    return dependency
