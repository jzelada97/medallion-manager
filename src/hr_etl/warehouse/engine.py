"""SQLAlchemy engine/session factory for the PostgreSQL warehouse."""

from __future__ import annotations

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from hr_etl.models.db_models import Base


def create_db_engine(dsn: str, echo: bool = False) -> Engine:
    """Create a SQLAlchemy engine with a sane connection pool."""
    return create_engine(dsn, echo=echo, pool_pre_ping=True, future=True)


def init_schema(engine: Engine) -> None:
    """Create warehouse tables if they do not exist."""
    Base.metadata.create_all(engine)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Return a configured session factory."""
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)
