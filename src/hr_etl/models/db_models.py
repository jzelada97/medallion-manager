"""SQLAlchemy table definitions for the PostgreSQL warehouse."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, Integer, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Declarative base for warehouse tables."""


class PersonRow(Base):
    """Consolidated person row in the warehouse."""

    __tablename__ = "persons"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    match_key: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    passport: Mapped[str | None] = mapped_column(String(64), index=True, nullable=True)
    full_name: Mapped[str | None] = mapped_column(String(255), index=True, nullable=True)
    name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lastname: Mapped[str | None] = mapped_column(String(128), nullable=True)
    sex: Mapped[str | None] = mapped_column(String(16), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    city: Mapped[str | None] = mapped_column(String(128), nullable=True)
    address: Mapped[str | None] = mapped_column(String(255), nullable=True)
    company: Mapped[str | None] = mapped_column(String(255), nullable=True)
    company_address: Mapped[str | None] = mapped_column(String(255), nullable=True)
    company_phone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    company_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    job: Mapped[str | None] = mapped_column(String(128), nullable=True)
    iban: Mapped[str | None] = mapped_column(String(64), nullable=True)
    salary: Mapped[float | None] = mapped_column(Float, nullable=True)
    ipv4: Mapped[str | None] = mapped_column(String(64), nullable=True)
    norm_name: Mapped[str | None] = mapped_column(String(255), index=True, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class MatchCandidate(Base):
    """Possible duplicate pair detected by batch reconciliation."""

    __tablename__ = "match_candidates"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    person_id_a: Mapped[int] = mapped_column(index=True)
    person_id_b: Mapped[int] = mapped_column(index=True)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    reason: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class FragmentLog(Base):
    """Audit trail: one row per fragment that contributed to a consolidated person.

    Stored at consolidation time so that a SPLIT/UNMERGE can recover the original
    fragments without querying MongoDB (which may be unavailable or slow).
    """

    __tablename__ = "fragment_log"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    person_id: Mapped[int] = mapped_column(Integer, index=True)
    match_key: Mapped[str] = mapped_column(String(255), index=True)
    fragment_type: Mapped[str] = mapped_column(String(32))
    payload: Mapped[str] = mapped_column(Text)  # JSON-serialized raw message
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class PersonReview(Base):
    """Queue of persons flagged as probable duplicates, pending human review.

    Status values:
    - ``pending``  : awaiting review
    - ``same``     : confirmed same person → merge
    - ``distinct`` : confirmed different → split back into original fragments
    """

    __tablename__ = "person_reviews"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    person_id_a: Mapped[int] = mapped_column(Integer, index=True)
    person_id_b: Mapped[int] = mapped_column(Integer, index=True)
    status: Mapped[str] = mapped_column(String(16), default="pending", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
