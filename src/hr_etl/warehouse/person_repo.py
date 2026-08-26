"""Idempotent upsert of consolidated Person records into PostgreSQL."""

from __future__ import annotations

from sqlalchemy import select
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

    def count(self) -> int:
        """Return the number of consolidated persons stored."""
        session: Session = self._session_factory()
        try:
            return session.query(PersonRow).count()
        finally:
            session.close()
