"""Idempotent upsert of consolidated Person records into PostgreSQL."""

from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from hr_etl.logging_conf import get_logger
from hr_etl.models.db_models import PersonRow
from hr_etl.models.person import Person

logger = get_logger(__name__)

_FIELDS = (
    "passport", "full_name", "name", "lastname", "sex", "phone", "email",
    "city", "address", "company", "company_address", "company_phone",
    "company_email", "job", "iban", "salary", "ipv4",
)


def _non_empty_values(person: Person) -> dict[str, object]:
    """Return the person fields that carry a real (non-empty) value."""
    return {
        field: getattr(person, field)
        for field in _FIELDS
        if getattr(person, field) not in (None, "")
    }


class PersonRepository:
    """Repository handling idempotent upserts keyed by ``match_key``."""

    def __init__(self, session_factory) -> None:
        self._session_factory = session_factory

    def upsert(self, person: Person) -> int:
        """Insert or merge a Person by match_key. Returns the row id.

        Existing non-empty columns are preserved; only blanks are filled in,
        making repeated processing of fragments idempotent.
        """
        if not person.match_key:
            raise ValueError("Person.match_key is required for upsert")

        session: Session = self._session_factory()
        try:
            row = session.execute(
                select(PersonRow).where(PersonRow.match_key == person.match_key)
            ).scalar_one_or_none()

            if row is None:
                row = PersonRow(match_key=person.match_key)
                for field in _FIELDS:
                    setattr(row, field, getattr(person, field))
                session.add(row)
            else:
                for field in _FIELDS:
                    new_value = getattr(person, field)
                    current = getattr(row, field)
                    if new_value not in (None, "") and current in (None, ""):
                        setattr(row, field, new_value)

            session.commit()
            logger.debug("upserted person match_key=%s id=%s", person.match_key, row.id)
            return row.id
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def upsert_native(self, person: Person) -> None:
        """Idempotent upsert using PostgreSQL ``INSERT ... ON CONFLICT``.

        Performs the whole upsert in a single atomic statement (no SELECT + write
        race window). Only fills columns that are currently NULL, preserving
        existing non-empty data via COALESCE(existing, new).

        Requires PostgreSQL. For SQLite/tests use :meth:`upsert`.
        """
        if not person.match_key:
            raise ValueError("Person.match_key is required for upsert")
        self.upsert_many_native([person])

    def upsert_many_native(self, persons: Iterable[Person]) -> int:
        """Batch idempotent upsert on PostgreSQL in a single transaction.

        Returns the number of rows sent. The whole batch commits atomically:
        either all rows are persisted or none (safe to reprocess from the lake).
        """
        rows: list[dict[str, object]] = []
        for person in persons:
            if not person.match_key:
                raise ValueError("Person.match_key is required for upsert")
            non_empty = _non_empty_values(person)
            # Every row must carry the SAME set of keys for a multi-row INSERT,
            # so we fill absent fields with None (missing -> NULL). COALESCE on
            # conflict keeps existing values, so NULLs never overwrite good data.
            payload = {field: non_empty.get(field) for field in _FIELDS}
            payload["match_key"] = person.match_key
            rows.append(payload)

        if not rows:
            return 0

        session: Session = self._session_factory()
        try:
            stmt = pg_insert(PersonRow).values(rows)
            # On conflict of match_key, keep existing non-null values and only
            # fill gaps with the incoming value: COALESCE(existing, incoming).
            # COALESCE(existing, incoming): keep existing value unless it is NULL,
            # so we only fill gaps and never overwrite good data (idempotent).
            update_cols = {
                field: func.coalesce(PersonRow.__table__.c[field], stmt.excluded[field])
                for field in _FIELDS
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=["match_key"],
                set_=update_cols,
            )
            session.execute(stmt)
            session.commit()
            logger.debug("native upsert batch size=%d", len(rows))
            return len(rows)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def count(self) -> int:
        """Return the number of consolidated persons stored."""
        session: Session = self._session_factory()
        try:
            return session.query(PersonRow).count()
        finally:
            session.close()
