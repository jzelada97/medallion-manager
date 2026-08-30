"""SQLAlchemy table definitions for the PostgreSQL warehouse."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, String, func
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
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class MatchCandidate(Base):
    """Possible duplicate pair detected by batch reconciliation.

    Stores pairs of person records that *might* be the same individual,
    along with a confidence score and the reason for the match hypothesis.
    These are NOT confirmed merges — they require review or a higher-confidence pass.
    """

    __tablename__ = "match_candidates"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    person_id_a: Mapped[int] = mapped_column(index=True)
    person_id_b: Mapped[int] = mapped_column(index=True)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    reason: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class DuplicateGroup(Base):
    """A member of a probable-duplicate GROUP detected by fuzzy reconciliation.

    Unlike ``match_candidates`` (which stores binary pairs), this table models
    duplicates as *groups*: several person records that likely refer to the same
    individual share the same ``group_id``. One row per member.

    ``group_id`` is the smallest person id in the group (a stable canonical
    representative). ``confidence`` is the fuzzy-name similarity that put this member
    in the group; ``reason`` explains the signal (e.g. name similarity + same city).

    Populated by ``processing/reconcile.py`` via a full rebuild each run. Never
    auto-merged — these are review candidates.
    """

    __tablename__ = "duplicate_groups"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    group_id: Mapped[int] = mapped_column(index=True)
    person_id: Mapped[int] = mapped_column(index=True)
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    reason: Mapped[str] = mapped_column(String(255), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
