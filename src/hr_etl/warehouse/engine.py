"""SQLAlchemy engine/session factory for the PostgreSQL warehouse."""

from __future__ import annotations

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from hr_etl.models.db_models import Base


def create_db_engine(
    dsn: str,
    echo: bool = False,
    pool_size: int = 10,
    max_overflow: int = 20,
    pool_recycle: int = 1800,
) -> Engine:
    """Create a SQLAlchemy engine with a tuned connection pool.

    - ``pool_pre_ping`` drops dead connections before use (resilient to DB restarts).
    - ``pool_size`` / ``max_overflow`` size the pool for concurrent workers under load.
    - ``pool_recycle`` refreshes connections periodically to avoid stale sockets.

    SQLite (used in tests) ignores pool sizing, so these args are only applied for
    non-sqlite DSNs.
    """
    kwargs: dict[str, object] = {"echo": echo, "pool_pre_ping": True, "future": True}
    if not dsn.startswith("sqlite"):
        kwargs.update(
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_recycle=pool_recycle,
        )
    return create_engine(dsn, **kwargs)


def init_schema(engine: Engine) -> None:
    """Create warehouse tables and apply idempotent SQL migrations.

    ``create_all`` builds the ORM tables (persons, gold_persons, duplicate_groups, ...).
    The SQL migrations then add what ``create_all`` cannot express — the ``pg_trgm``
    extension, the GIN trigram index on ``norm_name`` and the historical ``norm_name``
    backfill. Migrations are idempotent and skipped on non-Postgres backends, so unit
    tests on SQLite are unaffected.
    """
    Base.metadata.create_all(engine)
    # Imported lazily to avoid a circular import (migrate imports logging only, but keep
    # engine import-light for the ORM-only test paths).
    from hr_etl.warehouse.migrate import run_migrations

    run_migrations(engine)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Return a configured session factory."""
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)
