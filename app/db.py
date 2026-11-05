"""Database engine and session management."""

import logging
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from app.config import get_settings
from app.models import Base

log = logging.getLogger(__name__)

_engine = None
_SessionFactory: sessionmaker | None = None


def _tune_sqlite(engine) -> None:
    """WAL plus a generous busy timeout, so the worker pool does not trip over
    itself on the local fallback database."""

    @event.listens_for(engine, "connect")
    def _set_pragmas(dbapi_connection, _record):  # pragma: no cover - driver hook
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.close()


def _build_engine():
    settings = get_settings()
    for url in (settings.database_url, settings.database_fallback_url):
        kwargs: dict = {"pool_pre_ping": True, "future": True}
        if url.startswith("sqlite"):
            kwargs = {"connect_args": {"check_same_thread": False}, "future": True}
        try:
            engine = create_engine(url, **kwargs)
            if engine.dialect.name == "sqlite":
                _tune_sqlite(engine)
            with engine.connect():
                pass
        except Exception as exc:  # pragma: no cover - depends on local services
            log.warning(
                "database unavailable (%s: %s), trying next",
                exc.__class__.__name__,
                str(exc).splitlines()[0][:120],
            )
            continue
        log.info("database connected: %s", engine.dialect.name)
        return engine
    raise RuntimeError("no database reachable")


def get_engine():
    global _engine, _SessionFactory
    if _engine is None:
        _engine = _build_engine()
        _SessionFactory = sessionmaker(bind=_engine, expire_on_commit=False, future=True)
    return _engine


def init_db() -> None:
    """Create tables that do not exist yet."""
    Base.metadata.create_all(bind=get_engine())


def reset_db() -> None:
    """Drop and recreate every table. Used by tests and the seed script."""
    engine = get_engine()
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def get_session() -> Iterator[Session]:
    """FastAPI dependency yielding a request-scoped session."""
    get_engine()
    assert _SessionFactory is not None
    session = _SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def session_scope() -> Iterator[Session]:
    """Context manager with the same commit/rollback semantics as get_session."""
    get_engine()
    assert _SessionFactory is not None
    session = _SessionFactory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
