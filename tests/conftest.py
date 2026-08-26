"""Shared pytest fixtures."""

from __future__ import annotations

import mongomock
import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from hr_etl.models.db_models import Base
from hr_etl.warehouse.engine import make_session_factory


@pytest.fixture()
def mongo_collection():
    """An in-memory mongomock collection."""
    client = mongomock.MongoClient()
    return client["hr_lake"]["raw_messages"]


@pytest.fixture()
def sqlite_session_factory():
    """A SQLAlchemy session factory backed by shared in-memory SQLite.

    StaticPool keeps a single connection so every session sees the same
    in-memory database (otherwise each connection gets its own empty DB).
    """
    engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return make_session_factory(engine)


@pytest.fixture()
def personal_fragment() -> dict:
    return {
        "Name": "Ana",
        "Lastname": "Gil",
        "Sex": "F",
        "Telfnumber": "600111222",
        "Passport": "X1234567",
        "E-Mail": "ana@example.com",
    }


@pytest.fixture()
def bank_fragment() -> dict:
    return {"Passport": "X1234567", "IBAN": "ES9121000418450200051332", "Salary": "1.500,75"}


@pytest.fixture()
def location_fragment() -> dict:
    return {"Fullname": "Ana Gil", "City": "Madrid", "Address": "Calle Mayor 1"}
