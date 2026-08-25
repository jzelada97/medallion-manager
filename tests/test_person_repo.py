"""Tests for the PersonRepository upsert logic using in-memory SQLite."""

from __future__ import annotations

import pytest

from hr_etl.models.person import Person
from hr_etl.warehouse.person_repo import PersonRepository


def test_insert_new_person(sqlite_session_factory):
    repo = PersonRepository(sqlite_session_factory)
    pid = repo.upsert(Person(match_key="passport:x1", name="Ana", passport="X1"))
    assert pid > 0
    assert repo.count() == 1


def test_upsert_fills_blanks_only(sqlite_session_factory):
    repo = PersonRepository(sqlite_session_factory)
    repo.upsert(Person(match_key="passport:x1", name="Ana", passport="X1"))
    # second fragment adds iban but must NOT overwrite existing name
    repo.upsert(Person(match_key="passport:x1", name="SHOULD_NOT_WIN", iban="ES1"))

    assert repo.count() == 1
    session = sqlite_session_factory()
    try:
        from hr_etl.models.db_models import PersonRow

        row = session.query(PersonRow).filter_by(match_key="passport:x1").one()
        assert row.name == "Ana"  # preserved
        assert row.iban == "ES1"  # filled
    finally:
        session.close()


def test_upsert_requires_match_key(sqlite_session_factory):
    repo = PersonRepository(sqlite_session_factory)
    with pytest.raises(ValueError):
        repo.upsert(Person(match_key=""))
