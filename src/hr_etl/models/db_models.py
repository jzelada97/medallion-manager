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
    # Persisted normalized name (lowercase, no accents, titles stripped from both ends,
    # whitespace collapsed). Materialized so the reconciliation job never recomputes the
    # heavy regex over millions of rows; kept in sync by the warehouse writer + a one-off
    # backfill. Indexed (btree + GIN trigram) for the fuzzy-name reconciliation.
    norm_name: Mapped[str | None] = mapped_column(String(255), index=True, nullable=True)
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


class GoldPerson(Base):
    """Gold-layer person: a Silver person whose record is "complete enough".

    Gold = the curated, high-quality subset of Silver. A person qualifies when its
    record clears a completeness threshold (>= 80% of the tracked data fields filled)
    AND the five business-critical fields are present (full_name, passport, email,
    city, company). The Gold stats tables (gold_*) are computed over THIS table, not
    over all of Silver, so "Gold" means quality, not raw volume.

    Rebuilt in full by warehouse/gold_layer.py. Mirrors the person columns plus a
    stored completeness score for transparency.
    """

    __tablename__ = "gold_persons"

    id: Mapped[int] = mapped_column(primary_key=True)  # same id as persons.id
    match_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    passport: Mapped[str | None] = mapped_column(String(64), nullable=True)
    full_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
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
    completeness: Mapped[float] = mapped_column(Float, default=0.0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class PersonReview(Base):
    """A persistent HUMAN decision about a person record in the duplicate-review flow.

    The duplicate-consolidation flow surfaces ambiguous same-name candidates that the
    automatic rules refuse to merge. A reviewer resolves each case with one of three
    verdicts, recorded here so the decision SURVIVES a full reprocess from the lake and
    the 30-min ``duplicate_groups`` rebuild:

    * ``merged``   — this record was manually merged into another. ``survivor_match_key``
      points at the survivor. (The physical loser row is deleted by the merge; this row
      is the audit trace so a reprocess can re-merge / keep it out of review.)
    * ``approved`` — a reviewer confirmed this is the canonical, valid person. It is
      force-promoted to Gold even if its name repeats in Silver, and it leaves the
      review queue.
    * ``distinct`` — a reviewer confirmed this is a DIFFERENT real person that merely
      shares a name (a legitimate homonym). It leaves the review queue and no longer
      blocks its same-name peers from Gold.

    Keyed by ``match_key`` (the deterministic, content-derived business key) rather than
    ``persons.id`` on purpose: ``persons.id`` is an autoincrement surrogate that would be
    reassigned by a truncate + reload, whereas ``match_key`` is reproduced identically
    from the same raw fragment. So the decision stays anchored across reprocessing.
    """

    __tablename__ = "person_reviews"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    # Stable, content-derived key of the reviewed person (survives a reprocess).
    match_key: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    # 'merged' | 'approved' | 'distinct'
    status: Mapped[str] = mapped_column(String(32), index=True)
    # For 'merged': the match_key of the survivor this record was folded into.
    survivor_match_key: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    # Optional free-text note (who/why). No PII beyond the key already stored.
    note: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    # server_default so raw-SQL INSERTs (the merge-decision trace in consolidate_merge)
    # populate it too; onupdate keeps it fresh on ORM updates (the review endpoints).
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
