"""Targeted unit tests closing coverage gaps identified by the coverage report.

These cover branches that were not exercised elsewhere without needing live infra:
- clean_salary on a value that cannot be coerced (ValueError branch)
- Person.filled_fields / merge behaviour
- native upsert input validation (ValueError without match_key)
- empty batch upsert returns 0
- consumer decode edge already covered; here we cover pipeline persist path errors
"""

from __future__ import annotations

import pytest

from hr_etl.models.person import Person
from hr_etl.processing.normalizer import clean_salary
from hr_etl.warehouse.person_repo import PersonRepository, _non_empty_values


def test_clean_salary_uncoercible_returns_none():
    # These survive the regex filter but are NOT valid floats -> ValueError branch.
    assert clean_salary("1-2") is None  # "1-2" -> float() raises (except branch)
    # These hit the sentinel guard (no except): still None
    assert clean_salary("-") is None
    assert clean_salary(".") is None


def test_clean_salary_multiple_dots_keeps_last_as_decimal():
    # "1.234.567,89" style -> 1234567.89
    assert clean_salary("1.234.567,89") == 1234567.89


def test_person_filled_fields_and_merge():
    p = Person(match_key="k", name="Ana", email="a@b.c")
    assert p.filled_fields() == 2  # name + email (match_key excluded)

    other = Person(match_key="k", name="NO_WIN", city="madrid")
    merged = p.merge(other)
    assert merged.name == "Ana"  # existing kept
    assert merged.city == "madrid"  # gap filled


def test_non_empty_values_helper():
    p = Person(match_key="k", name="Ana", email="", city=None, job="dev")
    vals = _non_empty_values(p)
    assert vals == {"name": "Ana", "job": "dev"}  # empties/None dropped, match_key not included


def test_native_upsert_requires_match_key(sqlite_session_factory):
    repo = PersonRepository(sqlite_session_factory)
    with pytest.raises(ValueError):
        repo.upsert_native(Person(match_key=""))
    with pytest.raises(ValueError):
        repo.upsert_many_native([Person(match_key="")])


def test_native_batch_empty_returns_zero(sqlite_session_factory):
    repo = PersonRepository(sqlite_session_factory)
    assert repo.upsert_many_native([]) == 0


def test_upsert_rollback_on_error():
    """A failing session must trigger rollback and re-raise (error path)."""

    class BoomSession:
        def execute(self, *_a, **_k):
            raise RuntimeError("db down")

        def rollback(self):
            self.rolled_back = True

        def close(self):
            self.closed = True

    boom = BoomSession()
    repo = PersonRepository(lambda: boom)
    with pytest.raises(RuntimeError):
        repo.upsert(Person(match_key="k", name="Ana"))
    assert getattr(boom, "rolled_back", False) is True
    assert getattr(boom, "closed", False) is True
