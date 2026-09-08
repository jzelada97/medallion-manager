"""FastAPI application factory for the read-only query API."""

from __future__ import annotations

from fastapi import FastAPI

from hr_etl.api.routes import build_router
from hr_etl.config import get_settings
from hr_etl.logging_conf import configure_logging
from hr_etl.warehouse.engine import create_db_engine, init_schema, make_session_factory


def _make_mongo_count(settings):
    """Return a zero-arg callable that counts Bronze (raw) docs in MongoDB.

    Kept lazy and defensive: a failure to reach Mongo must never break the API,
    it just means the Bronze count shows as unavailable in the dashboard.
    """
    try:
        from pymongo import MongoClient

        client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=2000)
        collection = client[settings.mongo_db][settings.mongo_raw_collection]

        def _count() -> int:
            return collection.count_documents({})

        return _count
    except Exception:  # noqa: BLE001 - Bronze count is optional/best-effort
        return None


def create_app(session_factory=None, mongo_count=None) -> FastAPI:
    """Create the FastAPI app. A session_factory can be injected for tests."""
    settings = get_settings()
    configure_logging(settings.log_level)

    if session_factory is None:
        engine = create_db_engine(settings.postgres_dsn)
        init_schema(engine)
        session_factory = make_session_factory(engine)

    if mongo_count is None:
        mongo_count = _make_mongo_count(settings)

    app = FastAPI(title="HR Insights API", version="0.1.0")
    app.include_router(build_router(session_factory, mongo_count=mongo_count))
    return app
