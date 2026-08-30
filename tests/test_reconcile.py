"""Integration tests for the fuzzy batch reconciliation (processing/reconcile).

Reconciliation runs its detection in SQL using the ``pg_trgm`` extension (fuzzy name
similarity) and groups probable duplicates into ``duplicate_groups``. These are
integration tests against a REAL PostgreSQL — the SQL uses Postgres-specific features
(pg_trgm's ``similarity()``, ``split_part``, ``regexp_replace``) that SQLite lacks.

Connects via TEST_POSTGRES_DSN (defaults to the docker-compose Postgres on
localhost:5432). If no Postgres is reachable, the module is skipped.

Run it with the stack up:
    docker compose up -d postgres
    set TEST_POSTGRES_DSN=postgresql+psycopg2://hr_user:changeme@localhost:5432/hr_warehouse
    pytest -m integration
"""

from __future__ import annotations

import os

import pytest

pytest.importorskip("psycopg2", reason="psycopg2 not installed")

from sqlalchemy import text  # noqa: E402

from hr_etl.models.db_models import Base, DuplicateGroup, PersonRow  # noqa: E402
from hr_etl.processing.reconcile import run_reconciliation  # noqa: E402
from hr_etl.warehouse.engine import (  # noqa: E402
    create_db_engine,
    init_schema,
    make_session_factory,
)

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
    pytest.mark.skipif(
        not _postgres_available(), reason="no PostgreSQL reachable at TEST_POSTGRES_DSN"
    ),
]


@pytest.fixture()
def pg_session_factory():
    engine = create_db_engine(DSN)
    Base.metadata.drop_all(engine)
    init_schema(engine)
    factory = make_session_factory(engine)
    yield factory
    Base.metadata.drop_all(engine)
    engine.dispose()


def _add_person(session, match_key: str, full_name: str | None, **kwargs) -> PersonRow:
    row = PersonRow(match_key=match_key, full_name=full_name, **kwargs)
    session.add(row)
    session.flush()
    return row


def _groups(session) -> dict[int, list[DuplicateGroup]]:
    """Return current memberships bundled by group_id."""
    out: dict[int, list[DuplicateGroup]] = {}
    for m in session.query(DuplicateGroup).all():
        out.setdefault(m.group_id, []).append(m)
    return out


# ----------------------------------------------------------------------
# Fuzzy name similarity groups near-identical names (typos, extra surname)
# ----------------------------------------------------------------------


def test_fuzzy_groups_typo_surname(pg_session_factory):
    session = pg_session_factory()
    try:
        # One-letter difference at the end: exact matching would miss this.
        a = _add_person(session, "passport:X1", "jean leclerc")
        b = _add_person(session, "name:jean leclercq", "jean leclercq")
        session.commit()

        n = run_reconciliation(session, similarity_threshold=0.6)

        assert n >= 2  # both land in a group
        groups = _groups(session)
        # exactly one group containing both persons
        assert any({m.person_id for m in members} == {a.id, b.id} for members in groups.values())
    finally:
        session.close()


def test_group_anchored_to_min_id(pg_session_factory):
    session = pg_session_factory()
    try:
        a = _add_person(session, "passport:X1", "maria lopez")
        b = _add_person(session, "name:maria lopezz", "maria lopezz")
        session.commit()

        run_reconciliation(session, similarity_threshold=0.6)

        groups = _groups(session)
        assert len(groups) == 1
        gid = next(iter(groups))
        # group_id is the smallest person id (canonical anchor)
        assert gid == min(a.id, b.id)
    finally:
        session.close()


def test_same_city_marked_in_reason(pg_session_factory):
    session = pg_session_factory()
    try:
        _add_person(session, "passport:X1", "ana gil", city="Madrid")
        _add_person(session, "name:ana gill", "ana gill", city="Madrid")
        session.commit()

        run_reconciliation(session, similarity_threshold=0.6)

        reasons = [m.reason for m in session.query(DuplicateGroup).all()]
        assert any("same city" in r for r in reasons)
    finally:
        session.close()


# ----------------------------------------------------------------------
# Non-duplicates and edge cases
# ----------------------------------------------------------------------


def test_distinct_names_no_group(pg_session_factory):
    session = pg_session_factory()
    try:
        # Different first-word blocks -> never compared; clearly not duplicates.
        _add_person(session, "passport:X1", "ana gil")
        _add_person(session, "name:pedro ramirez", "pedro ramirez")
        session.commit()

        assert run_reconciliation(session, similarity_threshold=0.85) == 0
    finally:
        session.close()


def test_high_threshold_rejects_weak_similarity(pg_session_factory):
    session = pg_session_factory()
    try:
        # Same block ("carlos") but the surnames are quite different.
        _add_person(session, "passport:X1", "carlos mendez")
        _add_person(session, "name:carlos villanueva", "carlos villanueva")
        session.commit()

        # A strict threshold should not group them.
        assert run_reconciliation(session, similarity_threshold=0.9) == 0
    finally:
        session.close()


def test_rebuild_clears_previous(pg_session_factory):
    session = pg_session_factory()
    try:
        session.add(DuplicateGroup(group_id=1, person_id=1, confidence=0.9, reason="stale"))
        session.commit()

        # No persons this run -> stale rows cleared, zero written.
        assert run_reconciliation(session) == 0
        assert session.query(DuplicateGroup).count() == 0
    finally:
        session.close()


def test_empty_warehouse(pg_session_factory):
    session = pg_session_factory()
    try:
        assert run_reconciliation(session) == 0
    finally:
        session.close()
