"""Integration tests for PersonRepository against a REAL PostgreSQL.

These validate that the idempotent upsert behaves correctly on Postgres (not just
SQLite), which is what the warehouse uses in production (issue #6).

The test connects using TEST_POSTGRES_DSN (defaults to the docker-compose Postgres
on localhost:5432). If no Postgres is reachable, the whole module is skipped so it
never breaks the normal unit-test run.

Run it with the stack up:
    docker compose up -d postgres
    set TEST_POSTGRES_DSN=postgresql+psycopg2://hr_user:changeme@localhost:5432/hr_warehouse
    pytest -m integration
"""

from __future__ import annotations

import os
import uuid

import pytest

pytest.importorskip("psycopg2", reason="psycopg2 not installed")

from sqlalchemy import text  # noqa: E402

from hr_etl.models.db_models import Base, PersonRow  # noqa: E402
from hr_etl.models.person import Person  # noqa: E402
from hr_etl.warehouse.engine import (  # noqa: E402
    create_db_engine,
    init_schema,
    make_session_factory,
)
from hr_etl.warehouse.person_repo import PersonRepository  # noqa: E402

DSN = os.getenv(
    "TEST_POSTGRES_DSN",
    "postgresql+psycopg2://hr_user:changeme@localhost:5432/hr_warehouse",
)


def _postgres_available() -> bool:
    try:
        engine = create_db_engine(DSN)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:
        return False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _postgres_available(), reason="no PostgreSQL reachable at TEST_POSTGRES_DSN"),
]


@pytest.fixture()
def pg_session_factory():
    engine = create_db_engine(DSN)
    # Start from a clean schema so tests don't depend on pre-existing data
    # (e.g. seed data inserted manually into the same Postgres).
    Base.metadata.drop_all(engine)
    init_schema(engine)
    factory = make_session_factory(engine)
    yield factory
    Base.metadata.drop_all(engine)
    engine.dispose()


def test_insert_and_idempotent_upsert_on_real_postgres(pg_session_factory):
    repo = PersonRepository(pg_session_factory)
    key = f"passport:{uuid.uuid4().hex[:8]}"

    # first fragment: personal data
    pid1 = repo.upsert(Person(match_key=key, name="Ana", passport="X1", full_name="ana gil"))
    assert pid1 > 0

    # second fragment (same person): adds bank data, must NOT overwrite name
    pid2 = repo.upsert(Person(match_key=key, name="NO_WIN", iban="ES1", salary=1500.75))
    assert pid1 == pid2  # same row, no duplicate
    assert repo.count() == 1

    session = pg_session_factory()
    try:
        row = session.query(PersonRow).filter_by(match_key=key).one()
        assert row.name == "Ana"        # preserved
        assert row.iban == "ES1"        # filled
        assert row.salary == 1500.75    # filled
    finally:
        session.close()


def test_reprocessing_same_fragment_is_idempotent(pg_session_factory):
    repo = PersonRepository(pg_session_factory)
    key = f"passport:{uuid.uuid4().hex[:8]}"
    person = Person(match_key=key, name="Bea", passport="X2")

    repo.upsert(person)
    repo.upsert(person)  # same fragment again
    repo.upsert(person)

    assert repo.count() == 1  # never duplicates


def test_native_upsert_on_conflict(pg_session_factory):
    """INSERT ... ON CONFLICT keeps existing data and only fills gaps (atomic)."""
    repo = PersonRepository(pg_session_factory)
    key = f"passport:{uuid.uuid4().hex[:8]}"

    repo.upsert_native(Person(match_key=key, name="Ana", passport="X1"))
    # second fragment: must NOT overwrite name, must fill iban
    repo.upsert_native(Person(match_key=key, name="NO_WIN", iban="ES1", salary=1500.75))

    assert repo.count() == 1
    session = pg_session_factory()
    try:
        row = session.query(PersonRow).filter_by(match_key=key).one()
        assert row.name == "Ana"       # preserved via COALESCE
        assert row.iban == "ES1"       # filled
        assert row.salary == 1500.75
    finally:
        session.close()


def test_native_batch_upsert(pg_session_factory):
    """Batch upsert persists many rows atomically in one statement."""
    repo = PersonRepository(pg_session_factory)
    prefix = uuid.uuid4().hex[:6]
    people = [Person(match_key=f"passport:{prefix}-{i}", name=f"P{i}") for i in range(5)]

    sent = repo.upsert_many_native(people)
    assert sent == 5
    assert repo.count() == 5

    # reprocessing the same batch is idempotent (no duplicates)
    repo.upsert_many_native(people)
    assert repo.count() == 5
