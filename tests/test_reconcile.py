"""Integration tests for the batch reconciliation (processing/reconcile).

Reconciliation searches SILVER (``persons``) for probable-duplicate persons and stores
them as groups in ``duplicate_groups`` for HUMAN REVIEW — never auto-merged. The ONLY
signal is the NAME (identical / typo / containment); NO field corroboration is required,
because real split persons share no field but the name. The only negative guard is
passport NON-CONTRADICTION (different non-null passports = different people). Single-word
names are excluded. See the reconcile.py module docstring for the full rationale.

Integration tests against a REAL PostgreSQL (pg_trgm). Connects via TEST_POSTGRES_DSN
(defaults to the docker-compose Postgres on localhost:5432). Skipped if none reachable.

Run it with the stack up:
    docker compose up -d postgres
    $env:TEST_POSTGRES_DSN="postgresql+psycopg2://hr_user:changeme@localhost:5432/hr_warehouse"
    pytest tests/test_reconcile.py -m integration
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
    out: dict[int, list[DuplicateGroup]] = {}
    for m in session.query(DuplicateGroup).all():
        out.setdefault(m.group_id, []).append(m)
    return out


_ALLOWED_REASONS = {"exact_name", "fuzzy_name", "name_containment"}


# ----------------------------------------------------------------------
# Positive: names relate -> grouped, NO corroboration needed
# ----------------------------------------------------------------------


def test_typo_groups_without_corroboration(pg_session_factory):
    """Near-identical surnames (typo) group on the NAME ALONE — the split-person case
    where the two halves share NO field but the name (one from Personal, one from
    Location). No email/phone/company in common, yet they are candidates."""
    session = pg_session_factory()
    try:
        # disjoint data on purpose: one passport-side, one address-side, nothing shared.
        a = _add_person(session, "passport:X1", "jean leclerc", passport="X1", email="jl@x.com")
        b = _add_person(
            session,
            "name:jean leclercq",
            "jean leclercq",
            city="Paris",
            address="1 St",
            company="Acme",
        )
        session.commit()

        n = run_reconciliation(session, similarity_threshold=0.6)

        assert n >= 2
        groups = _groups(session)
        assert any({m.person_id for m in members} == {a.id, b.id} for members in groups.values())
        reasons = {m.reason for m in session.query(DuplicateGroup).all()}
        assert reasons <= _ALLOWED_REASONS
        assert "fuzzy_name" in reasons
    finally:
        session.close()


def test_containment_groups_disjoint_data(pg_session_factory):
    """Real split-person case (maite): "maite rodriguez" (Personal, passport) ⊆
    "maite rodriguez sanchez" (Location, address). They share NO field but the name;
    containment surfaces them for review anyway."""
    session = pg_session_factory()
    try:
        a = _add_person(session, "passport:M", "maite rodriguez", passport="PT99", email="m@x.com")
        b = _add_person(
            session,
            "name:maite rodriguez sanchez",
            "maite rodriguez sanchez",
            city="Lisboa",
            company="ACME",
            address="R 5",
        )
        session.commit()

        n = run_reconciliation(session)

        assert n >= 2
        groups = _groups(session)
        assert any({m.person_id for m in members} == {a.id, b.id} for members in groups.values())
        reasons = {m.reason for m in session.query(DuplicateGroup).all()}
        assert any("name_containment" in r for r in reasons)
    finally:
        session.close()


def test_identical_repeated_name_groups(pg_session_factory):
    """Identical repeated names ARE candidates for review (ambiguous homonyms a human
    decides). They group as exact_name — as long as passports do not contradict."""
    session = pg_session_factory()
    try:
        # neither has a passport -> non-contradiction holds -> grouped for review
        a = _add_person(session, "name:luisa jilani", "luisa jilani", city="Paris")
        b = _add_person(session, "k:luisa2", "luisa jilani", company="Globex")
        session.commit()

        n = run_reconciliation(session)

        assert n >= 2
        groups = _groups(session)
        assert any({m.person_id for m in members} == {a.id, b.id} for members in groups.values())
        reasons = {m.reason for m in session.query(DuplicateGroup).all()}
        assert "exact_name" in reasons
    finally:
        session.close()


def test_group_anchored_to_min_id(pg_session_factory):
    session = pg_session_factory()
    try:
        a = _add_person(session, "k:1", "maria lopez", city="A")
        b = _add_person(session, "k:2", "maria lopezz", city="B")
        session.commit()

        run_reconciliation(session, similarity_threshold=0.6)

        groups = _groups(session)
        assert len(groups) == 1
        assert next(iter(groups)) == min(a.id, b.id)
    finally:
        session.close()


# ----------------------------------------------------------------------
# Negative: the only guards — passport contradiction and single-word names
# ----------------------------------------------------------------------


def test_contradicting_passports_no_group(pg_session_factory):
    """Different non-null passports = provably different people; identical/similar names
    must NOT group them (the "many jose luis, each with own passport" case)."""
    session = pg_session_factory()
    try:
        _add_person(session, "k:1", "jose luis", passport="AAA111", city="Madrid")
        _add_person(session, "k:2", "jose luis", passport="BBB222", city="Sevilla")
        session.commit()

        assert run_reconciliation(session) == 0
    finally:
        session.close()


def test_typo_different_passports_still_grouped_for_review(pg_session_factory):
    """Passport non-contradiction is enforced only on the IDENTICAL-name branch (the
    homonym flood). For a TYPO pair across DISTINCT names, grouping is resolved on the
    name graph (for scale — no person×person product), so a typo pair is surfaced for
    review even with different passports. A human decides. This is a deliberate,
    documented trade-off: enforcing passport non-contradiction pairwise here would require
    expanding common name blocks to person×person pairs (measured 113M rows -> DiskFull)."""
    session = pg_session_factory()
    try:
        a = _add_person(session, "k:1", "carlos mendez", passport="AA1")
        b = _add_person(session, "k:2", "carlos mendezz", passport="BB2")
        session.commit()

        n = run_reconciliation(session, similarity_threshold=0.6)

        assert n >= 2
        groups = _groups(session)
        assert any({m.person_id for m in members} == {a.id, b.id} for members in groups.values())
    finally:
        session.close()


def test_common_namebase_bucket_pruned(pg_session_factory):
    """A common name-base (many persons sharing given-name + first-surname) is homonymy,
    not duplication: above the frequency guard it is NOT grouped, so the review pane stays
    useful. Here we seed > threshold persons named "juan ignacio" (and variants); none
    should group."""
    from hr_etl.processing.reconcile import _MAX_CONTAIN_BUCKET

    session = pg_session_factory()
    try:
        # threshold+2 identical "juan ignacio" (no passport, no contradiction) — still
        # pruned because the key2 bucket is too big to be a credible duplicate set.
        for i in range(_MAX_CONTAIN_BUCKET + 2):
            _add_person(session, f"k:ji-{i}", "juan ignacio", city=f"C{i}")
        # plus a containment variant that would have joined the huge bucket
        _add_person(session, "k:jig", "juan ignacio gomez", city="X")
        session.commit()

        assert run_reconciliation(session, similarity_threshold=0.6) == 0
    finally:
        session.close()


def test_small_namebase_still_groups(pg_session_factory):
    """A small name-base (few persons) is still grouped — the guard only prunes the big,
    noisy buckets, not the credible small ones (maite/lucio-like)."""
    session = pg_session_factory()
    try:
        a = _add_person(session, "k:1", "octavio ponce", city="Roma")
        b = _add_person(session, "k:2", "octavio ponce gimenez", city="Milano")
        session.commit()

        n = run_reconciliation(session)

        assert n >= 2
        groups = _groups(session)
        assert any({m.person_id for m in members} == {a.id, b.id} for members in groups.values())
    finally:
        session.close()


def test_single_word_name_never_groups(pg_session_factory):
    session = pg_session_factory()
    try:
        _add_person(session, "k:1", "madonna")
        _add_person(session, "k:2", "madonna")
        _add_person(session, "k:3", "Dr Madonna")  # normalizes to single word too
        session.commit()

        assert run_reconciliation(session, similarity_threshold=0.6) == 0
        assert session.query(DuplicateGroup).count() == 0
    finally:
        session.close()


def test_distinct_names_no_group(pg_session_factory):
    session = pg_session_factory()
    try:
        _add_person(session, "k:1", "ana gil")
        _add_person(session, "k:2", "pedro ramirez")
        session.commit()

        assert run_reconciliation(session, similarity_threshold=0.85) == 0
    finally:
        session.close()


def test_high_threshold_rejects_weak_similarity(pg_session_factory):
    session = pg_session_factory()
    try:
        _add_person(session, "k:1", "carlos mendez")
        _add_person(session, "k:2", "carlos villanueva")
        session.commit()

        assert run_reconciliation(session, similarity_threshold=0.9) == 0
    finally:
        session.close()


def test_rebuild_clears_previous(pg_session_factory):
    session = pg_session_factory()
    try:
        session.add(DuplicateGroup(group_id=1, person_id=1, confidence=0.9, reason="stale"))
        session.commit()

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


def test_idempotent_two_runs_same_groups(pg_session_factory):
    session = pg_session_factory()
    try:
        _add_person(session, "k:1", "maria lopez")
        _add_person(session, "k:2", "maria lopezz")
        session.commit()

        n1 = run_reconciliation(session, similarity_threshold=0.6)
        snap1 = {(m.group_id, m.person_id, m.reason) for m in session.query(DuplicateGroup).all()}
        n2 = run_reconciliation(session, similarity_threshold=0.6)
        snap2 = {(m.group_id, m.person_id, m.reason) for m in session.query(DuplicateGroup).all()}

        assert n1 == n2
        assert snap1 == snap2
    finally:
        session.close()


# ----------------------------------------------------------------------
# Parity + PII / schema checks
# ----------------------------------------------------------------------


def test_norm_name_python_sql_parity(pg_session_factory):
    from hr_etl.processing.normalizer import compute_norm_name
    from hr_etl.processing.sql_norm import norm_sql

    samples = [
        "William Weiss",
        "  William   Weiss  ",
        "María López",
        "Dr Juan Perez",
        "Juan Perez MD",
        "Sr. Octavio Ponce Gimenez",
        "JEAN LECLERCQ",
        "José Ángel Núñez",
    ]
    session = pg_session_factory()
    try:
        expr = norm_sql(":val")
        for raw in samples:
            sql_value = session.execute(text(f"SELECT {expr} AS n"), {"val": raw}).scalar_one()
            assert sql_value == compute_norm_name(raw), f"parity mismatch for {raw!r}"
    finally:
        session.close()


def test_reason_has_no_pii_only_fixed_labels(pg_session_factory):
    session = pg_session_factory()
    try:
        _add_person(session, "k:1", "jean leclerc", city="Paris")
        _add_person(session, "k:2", "jean leclercq", city="Paris")
        _add_person(session, "k:3", "octavio ponce", city="Roma")
        _add_person(session, "k:4", "octavio ponce gimenez", city="Roma")
        session.commit()

        run_reconciliation(session, similarity_threshold=0.6)

        reasons = {m.reason for m in session.query(DuplicateGroup).all()}
        assert reasons, "expected at least one group"
        assert reasons <= _ALLOWED_REASONS
        pii = ["leclerc", "leclercq", "octavio", "ponce", "gimenez", "paris", "roma"]
        for r in reasons:
            low = r.lower()
            for p in pii:
                assert p not in low, f"PII {p!r} leaked into reason {r!r}"
            assert not any(ch.isdigit() for ch in r)
    finally:
        session.close()


def test_duplicate_groups_schema_has_no_pii_columns(pg_session_factory):
    _ = pg_session_factory()
    engine = create_db_engine(DSN)
    try:
        with engine.connect() as conn:
            cols = {
                r[0]
                for r in conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = 'duplicate_groups'"
                    )
                ).all()
            }
        assert cols == {"id", "group_id", "person_id", "confidence", "reason", "created_at"}
    finally:
        engine.dispose()


def test_logs_contain_no_pii(pg_session_factory, caplog):
    import logging

    session = pg_session_factory()
    try:
        _add_person(session, "k:1", "jean leclerc", city="Paris")
        _add_person(session, "k:2", "jean leclercq", city="Paris")
        session.commit()

        with caplog.at_level(logging.DEBUG):
            run_reconciliation(session, similarity_threshold=0.6)

        blob = "\n".join(rec.getMessage() for rec in caplog.records).lower()
        for pii in ("leclerc", "leclercq", "jean", "paris"):
            assert pii not in blob, f"PII {pii!r} leaked into logs"
        assert "reconciliation complete" in blob
    finally:
        session.close()
