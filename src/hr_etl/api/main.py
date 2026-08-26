"""FastAPI application factory for the read-only query API."""

from __future__ import annotations

from fastapi import FastAPI

from hr_etl.api.routes import build_router
from hr_etl.config import get_settings
from hr_etl.logging_conf import configure_logging
from hr_etl.warehouse.engine import create_db_engine, init_schema, make_session_factory


def create_app(session_factory=None) -> FastAPI:
    """Create the FastAPI app. A session_factory can be injected for tests."""
    settings = get_settings()
    configure_logging(settings.log_level)

    if session_factory is None:
        engine = create_db_engine(settings.postgres_dsn)
        init_schema(engine)
        session_factory = make_session_factory(engine)

    app = FastAPI(title="HR Insights API", version="0.1.0")
    app.include_router(build_router(session_factory))
    return app
